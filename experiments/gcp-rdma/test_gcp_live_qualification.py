import copy
import errno
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import gcp_live_qualification as live


def owner_evidence(tier='synchronous'):
    launch = {'listen': '10.0.0.2:9400', 'launch_nonce': 'fresh-nonce',
              'durability': {'tier': tier, 'max_unsynced_age_ms': None, 'max_pending_completions': 8},
              'node': {'partition': {'partition': 0, 'inventory': {'database': [1] * 16}}}}
    receipt = {'partition': 0, 'database': [1] * 16, 'address': launch['listen'],
               'launch_nonce': launch['launch_nonce'], 'pid': 42, 'boot_id': 'boot',
               'started_unix_ns': 123, 'durability': dict(launch['durability'])}
    return launch, receipt


def evidence():
    route = {'transport': 'rdma', 'address': '10.1.0.2', 'service': 4420,
             'host_nqn': 'nqn.2014-08.org.nvmexpress:uuid:11111111-1111-4111-8111-111111111111',
             'host_id': [17, 17, 17, 17, 17, 17, 65, 17, 129, 17, 17, 17, 17, 17, 17, 17],
             'namespace_identity': {'namespace_path': '/dev/nvme1n1', 'nqn': 'nqn.example',
                                    'nsid': 1, 'bytes': 4096, 'controller_instance': 1,
                                    'block_size': 4096, 'uuid': [1] * 16}}
    first_writer = copy.deepcopy(route)
    first_writer['writer'] = {'epoch': 1, 'file_index': 0}
    next_writer = copy.deepcopy(route)
    next_writer['writer'] = {'epoch': 1, 'file_index': 1}
    next_writer['namespace_identity'].update(namespace_path='/dev/nvme2n1',
                                             controller_instance=2, uuid=[2] * 16)
    def sample(**extra):
        return {'completed_write_ops': 0, 'completed_write_bytes': 0,
                'completed_read_ops': 0, 'completed_read_bytes': 0,
                'recovery_read_ops': 0, 'recovery_read_bytes': 0,
                'errors': 0, 'unknown_writes': 0,
                'route_evidence_dropped': 0, 'fence_evidence_dropped': 0,
                'routes': [copy.deepcopy(route)], 'fences': [], **extra}
    proof = {'schema_version': 1, 'kind': 'live_nvmeof_qualification', 'expected_route': 'rdma',
             'target_implementation': 'kernel', 'durability_tier': 'synchronous',
             'scope': 'verified_kernel_namespace_not_physical_media_performance',
             'checks': {'scalar_write_delete': True, 'mixed_batch': True,
                        'ordered_batch_missing_exists': True, 'checkpoint_iteration': True,
                        'flush_reopen': True, 'abrupt_tail_recovery': True,
                        'fence_drain': True, 'cache_miss_hit': True},
             'member_transition': {'wal_file_bytes': 1048576, 'completed_write_bytes': 1114112,
                                   'completed_write_ops': 32, 'writes_acknowledged': 32,
                                   'writer_routes': [first_writer, next_writer],
                                   'before_key_read': True, 'after_key_read': True,
                                   'early_key_read': True, 'post_transition_delete': True},
             'start': sample(), 'before_close': sample(completed_write_ops=1, completed_write_bytes=4096),
             'recovery': sample(),
             'after_miss': sample(completed_read_ops=1, completed_read_bytes=1024),
             'after_hit': sample(completed_read_ops=1, completed_read_bytes=1024),
             'abrupt_child': {'schema_version': 1, 'kind': 'acknowledged_unrooted_mixed_batch',
                              'exit_model': 'process_exit_without_db_drop_or_checkpoint',
                              'start': sample(), 'end': sample(completed_write_ops=1, completed_write_bytes=1024)},
             'abrupt_recovery': sample(recovery_read_ops=1, recovery_read_bytes=4096,
                 fences=[{'partition': 0, 'authority_epoch': 3,
                          'target': {'fixture_only': False, 'disable_drain_successes': 1, 'errors': 0}}]),
             'target_shutdown': [{'partition': 0, 'endpoint': '10.0.0.2:9400', 'epoch': 3, 'database': [1] * 16,
                                  'stats': {'fixture_only': False, 'owned_namespace_count': 0, 'errors': 0}}]}
    def counters(source, peer):
        row = {'source': source, 'peer': peer, 'transport': 'rdma', 'interface': 'eth1',
               'device': 'irdma0', 'port': '1', 'port_identity': [1, 123],
               'counter_samples': {
                   'irdma_ipv4': {'unit': 'octets',
                                  'paths': ['/sys/class/infiniband/irdma0/ports/1/hw_counters/ip4OutOctets',
                                            '/sys/class/infiniband/irdma0/ports/1/hw_counters/ip4InOctets'],
                                  'tx': 10, 'rx': 20}},
               'unavailable_counter_kinds': ['ib_pma', 'irdma_sends']}
        after = copy.deepcopy(row)
        after['counter_samples']['irdma_ipv4'].update(tx=11, rx=21)
        return {'before': row, 'after': after}
    return proof, counters('10.1.0.1', '10.1.0.2'), counters('10.1.0.2', '10.1.0.1')


class QualificationTest(unittest.TestCase):
    def admit(self, proof, coordinator, owner, *, archive_binary=False):
        launch, receipt = owner_evidence()
        return live.admit(proof, 'rdma', '10.1.0.2', '10.1.0.1', '10.0.0.2', coordinator, owner,
                          target='kernel', tier='synchronous', owner_launch=launch, owner_receipt=receipt,
                          archive_binary=archive_binary)

    def test_archive_proof_requires_no_payload_fallback(self):
        proof, coordinator, owner = evidence()
        with self.assertRaisesRegex(ValueError, 'checks incomplete'):
            self.admit(proof, coordinator, owner, archive_binary=True)
        proof['checks']['payload_rpc_fallback'] = False
        for sample in (proof['before_close'], proof['recovery'], proof['after_hit'],
                       proof['abrupt_child']['end'], proof['abrupt_recovery']):
            sample['payload_rpc_requests'] = 0
        self.admit(proof, coordinator, owner, archive_binary=True)
        proof['before_close']['payload_rpc_requests'] = 1
        with self.assertRaisesRegex(ValueError, 'live errors'):
            self.admit(proof, coordinator, owner, archive_binary=True)

    def test_owner_tier_and_launch_receipt_are_required(self):
        proof, coordinator, owner = evidence()
        launch, receipt = owner_evidence()
        for tier in ('native', 'synchronous'):
            p = copy.deepcopy(proof)
            p['durability_tier'] = tier
            l, r = owner_evidence(tier)
            result = live.admit(p, 'rdma', '10.1.0.2', '10.1.0.1', '10.0.0.2', coordinator, owner,
                                target='kernel', tier=tier, owner_launch=l, owner_receipt=r)
            self.assertEqual(result['owner_receipt'], r)
        bad = [None, {}, {**receipt, 'launch_nonce': 'stale'},
               {**receipt, 'database': [2] * 16}, {**receipt, 'address': '10.0.0.3:9400'},
               {**receipt, 'durability': {'tier': 'native'}},
               {**receipt, 'durability': {**receipt['durability'], 'max_pending_completions': 9}},
               {**receipt, 'pid': 0}, {**receipt, 'started_unix_ns': 0}]
        for value in bad:
            with self.subTest(receipt=value), self.assertRaises(ValueError):
                live.admit(proof, 'rdma', '10.1.0.2', '10.1.0.1', '10.0.0.2', coordinator, owner,
                           target='kernel', tier='synchronous', owner_launch=launch, owner_receipt=value)
        for tier in (None, 'native', 'invalid'):
            p = copy.deepcopy(proof)
            if tier is None:
                del p['durability_tier']
            else:
                p['durability_tier'] = tier
            with self.subTest(tier=tier), self.assertRaises(ValueError):
                self.admit(p, coordinator, owner)

    def test_running_owner_rejects_stale_process_receipt(self):
        launch, receipt = owner_evidence()
        class Process:
            pid = 42
            def poll(self):
                return None
        with patch.object(live.cloud_campaign, 'read_owner_receipt', return_value=receipt), \
             patch.object(live.Path, 'read_text', return_value='boot'):
            self.assertEqual(live._running_owner_receipt(launch, 'synchronous', Process(), 100), receipt)
            with self.assertRaises(ValueError):
                live._running_owner_receipt(launch, 'synchronous', Process(), 124)
            Process.pid = 43
            with self.assertRaises(ValueError):
                live._running_owner_receipt(launch, 'synchronous', Process(), 100)

    def test_spdk_sampler_waits_for_namespace_publication(self):
        flags = os.O_RDWR | os.O_DIRECT
        snapshot = {
            'backing_fds': [{'name': 'wal0', 'filename': '/wal', 'available': True,
                             'mode': 0o100600, 'device': 8, 'inode': 9, 'bytes': 4096,
                             'descriptors': [{'fd': 7, 'flags': flags}]}],
            'bdevs': [{'name': 'wal0', 'num_blocks': 1, 'block_size': 4096,
                       'driver_specific': {'aio': {'filename': '/wal', 'readonly': False}}}],
            'subsystems': [{'nqn': 'nqn.example', 'namespaces': []}],
        }
        self.assertFalse(live._published_spdk_snapshot(snapshot))
        snapshot['subsystems'][0]['namespaces'] = [
            {'bdev_name': 'wal0', 'nsid': 1, 'uuid': '01010101-0101-0101-0101-010101010101'}]
        self.assertTrue(live._published_spdk_snapshot(snapshot))
        snapshot['backing_fds'][0]['descriptors'][0]['flags'] = os.O_RDWR
        self.assertFalse(live._published_spdk_snapshot(snapshot))
    def test_spdk_selection_skips_unrelated_published_namespace(self):
        proof, _, _ = evidence()
        root = Path('/data')
        filename = '/data/wal'
        daemon = {'pid': 10, 'process_start_ticks': 20, 'executable': '/bin/nvmf_tgt',
                  'executable_device': 1, 'executable_inode': 2, 'binary_sha256': 'a' * 64,
                  'rpc_socket': '/run/rpc.sock', 'rpc_device': 3, 'rpc_inode': 4}
        route = proof['member_transition']['writer_routes'][0]
        route['spdk'] = {'rpc_socket': daemon['rpc_socket'], 'pid': 10, 'process_start_ticks': 20,
                         'bdev_name': 'wal0', 'image_path': filename}
        route['writer']['file_identity'] = {'path': filename, 'device': 8, 'inode': 9,
                                             'bytes': 4096, 'block_size': 4096}
        good = {'daemon': daemon,
                'backing_fds': [{'name': 'wal0', 'filename': filename, 'available': True,
                                 'mode': 0o100600, 'device': 8, 'inode': 9, 'bytes': 4096,
                                 'descriptors': [{'fd': 7, 'flags': os.O_RDWR | os.O_DIRECT}]}],
                'bdevs': [{'name': 'wal0', 'num_blocks': 1, 'block_size': 4096,
                           'driver_specific': {'aio': {'filename': filename, 'readonly': False}}}],
                'subsystems': [{'nqn': 'nqn.example', 'namespaces': [{
                    'bdev_name': 'wal0', 'nsid': 1, 'uuid': '01010101-0101-0101-0101-010101010101'}]}]}
        unrelated = copy.deepcopy(good)
        unrelated['subsystems'][0]['namespaces'][0]['uuid'] = '02020202-0202-0202-0202-020202020202'
        self.assertTrue(live._published_spdk_snapshot(unrelated))
        self.assertIs(live._select_spdk_observation([unrelated, good], daemon, proof, root, 'a' * 64), good)
        with self.assertRaisesRegex(ValueError, 'no SPDK observation matches'):
            live._select_spdk_observation([unrelated], daemon, proof, root, 'a' * 64)



    def test_physical_kernel_proof_is_accepted(self):
        proof, coordinator, owner = evidence()
        admitted = self.admit(proof, coordinator, owner)
        self.assertEqual(admitted['proof'], proof)
        self.assertEqual(admitted['counter_evidence']['common_advancing_families'], ['irdma_ipv4'])

    def test_member_transition_evidence_is_mandatory_and_bounded(self):
        proof, coordinator, owner = evidence()
        mutations = [
            lambda p: p.pop('member_transition'),
            lambda p: p['member_transition'].pop('completed_write_bytes'),
            lambda p: p['member_transition'].update(wal_file_bytes=0),
            lambda p: p['member_transition'].update(wal_file_bytes=1024 * 1024 * 1024 + 1),
            lambda p: p['member_transition'].update(wal_file_bytes=True),
            lambda p: p['member_transition'].update(completed_write_bytes=0),
            lambda p: p['member_transition'].update(completed_write_bytes=1048576),
            lambda p: p['member_transition'].update(completed_write_bytes=2 * 1024 * 1024 * 1024 + 1),
            lambda p: p['member_transition'].update(completed_write_bytes=True),
            lambda p: p['member_transition'].update(completed_write_ops=0),
            lambda p: p['member_transition'].update(completed_write_ops=True),
            lambda p: p['member_transition'].update(completed_write_ops=2 * 1024 * 1024 * 1024 + 1),
            lambda p: p['member_transition'].update(writes_acknowledged=0),
            lambda p: p['member_transition'].update(writes_acknowledged=65537),
            lambda p: p['member_transition'].update(writes_acknowledged=True),
            lambda p: p['member_transition'].update(before_key_read=False),
            lambda p: p['member_transition'].update(after_key_read=False),
            lambda p: p['member_transition'].update(early_key_read=False),
            lambda p: p['member_transition'].update(post_transition_delete=False),
            lambda p: p['member_transition'].update(writer_routes=[]),
            lambda p: p['member_transition'].update(writer_routes=p['member_transition']['writer_routes'] * 513),
        ]
        for mutate in mutations:
            p = copy.deepcopy(proof)
            mutate(p)
            with self.subTest(mutate=mutate), self.assertRaisesRegex(ValueError, 'member_transition'):
                self.admit(p, coordinator, owner)

    def test_member_transition_requires_consistent_authenticated_owner_members(self):
        proof, coordinator, owner = evidence()
        mutations = [
            lambda r: r[1]['writer'].update(file_index=0),
            lambda r: r[1]['writer'].update(epoch=2, file_index=0),
            lambda r: r[1]['writer'].update(file_index=True),
            lambda r: r[1]['writer'].update(epoch=2),
            lambda r: r[1].update(writer=None),
            lambda r: r[1]['namespace_identity'].update(uuid=copy.deepcopy(r[0]['namespace_identity']['uuid'])),
            lambda r: r[1].update(address='10.1.0.3'),
            lambda r: r[1].update(host_nqn='nqn.example.other'),
            lambda r: r[1]['namespace_identity'].update(namespace_path='/tmp/fake'),
        ]
        for mutate in mutations:
            p = copy.deepcopy(proof)
            mutate(p['member_transition']['writer_routes'])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.admit(p, coordinator, owner)


    def test_file_fixture_missing_fence_wrong_route_or_missing_counters_fail(self):
        proof, coordinator, owner = evidence()
        mutations = [
            lambda p, c, o: p.update(kind='live_nvmeof_file_component_qualification'),
            lambda p, c, o: p['abrupt_recovery'].update(fences=[]),
            lambda p, c, o: p['abrupt_recovery'].update(recovery_read_ops=0),
            lambda p, c, o: p['after_hit']['routes'][0].update(address='127.0.0.1'),
            lambda p, c, o: p['before_close'].update(unknown_writes=1),
            lambda p, c, o: p['target_shutdown'][0]['stats'].update(owned_namespace_count=1),
            lambda p, c, o: o['after']['counter_samples']['irdma_ipv4'].update(rx=20),
            lambda p, c, o: c['after'].update(device='other'),
        ]
        for mutate in mutations:
            p, c, o = copy.deepcopy((proof, coordinator, owner))
            mutate(p, c, o)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.admit(p, c, o)
        with self.assertRaises(ValueError):
            self.admit(proof, None, owner)

    def test_counter_identity_availability_and_values_are_strict(self):
        proof, coordinator, owner = evidence()
        mutations = [
            lambda c: c['after'].update(port='2'),
            lambda c: c['after'].update(port_identity=[1, 124]),
            lambda c: c['after'].update(transport='tcp'),
            lambda c: c['after']['counter_samples']['irdma_ipv4'].update(unit='4_octets'),
            lambda c: c['after']['counter_samples']['irdma_ipv4']['paths'].__setitem__(0, '/other'),
            lambda c: c['after']['unavailable_counter_kinds'].append('irdma_ipv4'),
            lambda c: c['after']['counter_samples'].clear(),
            lambda c: c['after']['counter_samples']['irdma_ipv4'].update(tx=0),
            lambda c: c['after']['counter_samples']['irdma_ipv4'].update(rx=19),
            lambda c: c['before']['counter_samples']['irdma_ipv4'].update(tx=-1),
            lambda c: c['after']['counter_samples']['irdma_ipv4'].update(tx=True),
        ]
        for mutate in mutations:
            c = copy.deepcopy(coordinator)
            mutate(c)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.admit(proof, c, owner)

    def test_valid_availability_change_rejected_despite_common_advancing_family(self):
        proof, coordinator, owner = evidence()
        before = coordinator['before']
        before['unavailable_counter_kinds'].remove('irdma_sends')
        before['counter_samples']['irdma_sends'] = {
            'unit': 'operations',
            'paths': ['/sys/class/infiniband/irdma0/ports/1/hw_counters/OutRdmaSends',
                      '/sys/class/infiniband/irdma0/ports/1/hw_counters/InRdmaSends'],
            'tx': 4, 'rx': 5}
        with self.assertRaisesRegex(ValueError, 'availability changed'):
            self.admit(proof, coordinator, owner)

    def test_missing_counters_and_disjoint_advancing_families_cannot_admit(self):
        proof, coordinator, owner = evidence()
        empty = copy.deepcopy(coordinator)
        for snapshot in empty.values():
            snapshot['counter_samples'] = {}
            snapshot['unavailable_counter_kinds'] = ['ib_pma', 'irdma_ipv4', 'irdma_sends']
        with self.assertRaises(ValueError):
            self.admit(proof, empty, owner)
        for snapshot in owner.values():
            sample = snapshot['counter_samples'].pop('irdma_ipv4')
            sample.update(unit='operations', paths=[
                '/sys/class/infiniband/irdma0/ports/1/hw_counters/OutRdmaSends',
                '/sys/class/infiniband/irdma0/ports/1/hw_counters/InRdmaSends'])
            snapshot['counter_samples']['irdma_sends'] = sample
            snapshot['unavailable_counter_kinds'] = ['ib_pma', 'irdma_ipv4']
        with self.assertRaises(ValueError):
            self.admit(proof, coordinator, owner)
        # Both families are present on both hosts, but each host advances only
        # a different family: supported is not the same as common advancing.
        proof, coordinator, owner = evidence()
        for counters in (coordinator, owner):
            for snapshot in counters.values():
                snapshot['unavailable_counter_kinds'].remove('irdma_sends')
                snapshot['counter_samples']['irdma_sends'] = {
                    'unit': 'operations',
                    'paths': ['/sys/class/infiniband/irdma0/ports/1/hw_counters/OutRdmaSends',
                              '/sys/class/infiniband/irdma0/ports/1/hw_counters/InRdmaSends'],
                    'tx': 4, 'rx': 5}
        owner['after']['counter_samples']['irdma_ipv4'].update(tx=10, rx=20)
        owner['after']['counter_samples']['irdma_sends'].update(tx=5, rx=6)
        with self.assertRaises(ValueError):
            self.admit(proof, coordinator, owner)


class PartialEvidenceTest(unittest.TestCase):
    def test_kernel_snapshot_is_bounded_immutable_and_non_admitted(self):
        class Store:
            def __init__(self):
                self.objects = {}

            def put(self, key, data, *, if_generation_match, metadata):
                if if_generation_match != 0 or key in self.objects:
                    raise ValueError('object exists')
                self.objects[key] = (bytes(data), dict(metadata))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'result').mkdir()
            proof = root / 'result/qualification.json'
            proof.write_bytes(b'kernel proof')
            log = root / 'qualification.log'
            log.write_bytes(b'a' * (1024 * 1024) + b'tail')
            (root / 'tls').mkdir()
            (root / 'tls/key.pem').write_bytes(b'private key must not be exported')
            (root / 'capsule.json').write_bytes(b'launch secret must not be exported')
            store = Store()
            identity = {'source_sha256': 'a' * 64}
            files = ('qualification.log', 'result/qualification.json',
                     'result/qualification-progress.json')
            live._export_partial(root, store, {'run_id': 'run-1'}, identity, 'host-1', 123,
                                 'rdma', 'kernel-complete', files, float('inf'), tier='native')
            prefix = 'runs/run-1/qualification/live-rdma-native/partial-host-1/kernel-complete/'
            self.assertEqual(set(store.objects), {prefix + 'qualification.log',
                                                  prefix + 'result-qualification.json'})
            data, metadata = store.objects[prefix + 'qualification.log']
            self.assertEqual(len(data), 1024 * 1024)
            self.assertTrue(data.endswith(b'tail'))
            self.assertEqual(metadata['truncated'], 'true')
            self.assertEqual(metadata['original_size'], str(1024 * 1024 + 4))
            self.assertEqual(metadata['status'], 'partial_not_admitted')
            self.assertEqual(metadata['instance_id'], '123')
            self.assertEqual(metadata['source_sha256'], 'a' * 64)
            proof.write_bytes(b'changed')
            self.assertEqual(store.objects[prefix + 'result-qualification.json'][0], b'kernel proof')
            with self.assertRaisesRegex(ValueError, 'object exists'):
                live._export_partial(root, store, {'run_id': 'run-1'}, identity, 'host-1', 123,
                                     'rdma', 'kernel-complete', files, float('inf'), tier='native')

    def test_partial_export_budget_blocks_uploads_and_preserves_failure(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'first').write_bytes(b'first')
            (root / 'second').write_bytes(b'second')
            clock = [100]
            class Store:
                def __init__(self):
                    self.attempts = []
                def put(self, key, data, **kwargs):
                    self.attempts.append((key, bytes(data)))
                    clock[0] += 40
                    raise OSError(errno.EIO, 'upload failed')
            store = Store()
            with patch.object(live.time, 'time', side_effect=lambda: clock[0]):
                with self.assertRaises(TimeoutError):
                    live._export_partial(root, store, {'run_id': 'run-1'}, {}, 'host', 1,
                                         'rdma', 'failure', ('first', 'second'), 164, tier='native')
                self.assertEqual(store.attempts, [])
                failure = RuntimeError('qualification failed')
                live._export_partial(root, store, {'run_id': 'run-1'}, {}, 'host', 1,
                                     'rdma', 'failure', ('first', 'second'), 180, failure, tier='native')
            self.assertEqual(len(store.attempts), 1)
            self.assertEqual(store.attempts[0][1], b'first')
            self.assertTrue(any('upload failed' in note for note in failure.__notes__))
            self.assertTrue(any('budget' in note for note in failure.__notes__))

    def test_failure_export_stat_error_preserves_qualification_failure(self):
        with TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / 'run-1' / 'live-rdma-native'
            original = RuntimeError('kernel qualification failed')
            is_dir = Path.is_dir
            def fail_stat(path):
                if path == root:
                    raise OSError(errno.EIO, 'stat failed')
                return is_dir(path)
            def fail_run(*args):
                root.mkdir(parents=True)
                raise original
            with patch.object(live, 'Path', return_value=base), \
                    patch.object(live.bootstrap, '_remaining', return_value=100), \
                    patch.object(live, '_run', side_effect=fail_run), \
                    patch.object(Path, 'is_dir', fail_stat):
                with self.assertRaises(RuntimeError) as caught:
                    live.run(None, {'run_id': 'run-1'}, 1000, None, 'host-1',
                             123, {}, None, None, None, 'rdma', 'native')
            self.assertIs(caught.exception, original)
            self.assertTrue(any('stat failed' in note for note in original.__notes__))


class PhysicalRouteTest(unittest.TestCase):
    def test_unknown_counter_filename_is_diagnostic_not_admission_evidence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            port = root / 'irdma0' / 'ports' / '1'
            for name, data in (('gid_attrs/ndevs/1', 'eth1'), ('state', '4: ACTIVE'),
                               ('hw_counters/provider_private_count', '100')):
                path = port / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(data)
            def route(source, peer):
                replies = [json.dumps([{'dev': 'eth1', 'prefsrc': source}]),
                           json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': source}]}])]
                with patch.object(live.bootstrap, '_call', side_effect=replies):
                    return live._route(peer, source, 'rdma', 'irdma0', 1)
            with patch.object(live, 'Path', return_value=root):
                before = route('10.1.0.1', '10.1.0.2')
                (port / 'hw_counters/provider_private_count').write_text('200')
                after = route('10.1.0.1', '10.1.0.2')
                self.assertIn('provider_private_count', before['counter_files']['hw_counters'])
                self.assertEqual(before['counter_samples'], {})
                owner_before = route('10.1.0.2', '10.1.0.1')
                owner_after = route('10.1.0.2', '10.1.0.1')
                proof, _, _ = evidence()
                launch, receipt = owner_evidence()
                with self.assertRaises(ValueError):
                    live.admit(proof, 'rdma', '10.1.0.2', '10.1.0.1', '10.0.0.2',
                               {'before': before, 'after': after},
                               {'before': owner_before, 'after': owner_after},
                               target='kernel', tier='synchronous', owner_launch=launch, owner_receipt=receipt)

    def test_native_counters_discovery_units_and_failures(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            port = root / 'irdma0' / 'ports' / '1'
            def put(name, value):
                path = port / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(value)
                return path
            put('gid_attrs/ndevs/1', 'eth1')
            put('state', '4: ACTIVE')
            tx = put('hw_counters/ip4OutOctets', '100')
            rx = put('hw_counters/ip4InOctets', '200')
            put('hw_counters/OutRdmaSends', '8')
            put('hw_counters/InRdmaSends', '9')
            def route():
                replies = [json.dumps([{'dev': 'eth1', 'prefsrc': '10.1.0.1'}]),
                           json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.1'}]}])]
                with patch.object(live.bootstrap, '_call', side_effect=replies):
                    return live._route('10.1.0.2', '10.1.0.1', 'rdma', 'irdma0', 1)
            with patch.object(live, 'Path', return_value=root):
                before = route()
                self.assertEqual(before['port'], '1')
                self.assertEqual(before['port_identity'], [port.stat().st_dev, port.stat().st_ino])
                self.assertIn('ib_pma', before['unavailable_counter_kinds'])
                self.assertEqual(before['counter_samples']['irdma_ipv4']['unit'], 'octets')
                self.assertEqual(before['counter_samples']['irdma_sends']['unit'], 'operations')
                tx.write_text('110')
                rx.write_text('210')
                after = route()
                self.assertEqual(live._delta(before, after, '10.1.0.1', '10.1.0.2', 'rdma'),
                                 {'irdma_ipv4'})
                pma_tx = put('counters/port_xmit_data', '30')
                pma_rx = put('counters/port_rcv_data', '40')
                all_families = route()['counter_samples']
                self.assertEqual(all_families['ib_pma']['unit'], '4_octets')
                self.assertEqual(all_families['irdma_ipv4']['unit'], 'octets')
                self.assertEqual(all_families['irdma_sends']['unit'], 'operations')
                pma_tx.unlink()
                pma_rx.unlink()
                tx.write_text('not-a-number')
                with self.assertRaises(ValueError):
                    route()
                tx.write_text('110')
                read_text = Path.read_text
                for failure in (errno.EACCES, errno.EIO):
                    def fail(path, *args, **kwargs):
                        if path == tx:
                            raise OSError(failure, 'counter read failed')
                        return read_text(path, *args, **kwargs)
                    with self.subTest(errno=failure), \
                            patch.object(Path, 'read_text', fail), self.assertRaises(OSError):
                        route()
                rx.unlink()
                with self.assertRaises((ValueError, OSError)):
                    route()
                tx.unlink()
                put('hw_counters/OutRdmaSends', '8').unlink()
                put('hw_counters/InRdmaSends', '9').unlink()
                empty = route()
                self.assertEqual(empty['counter_samples'], {})
                self.assertEqual(set(empty['unavailable_counter_kinds']),
                                 {'ib_pma', 'irdma_ipv4', 'irdma_sends'})

    def test_unmapped_gid_slot_is_skipped_but_other_read_errors_are_fatal(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            port = root / 'irdma0' / 'ports' / '1'
            names = port / 'gid_attrs' / 'ndevs'
            files = {'gid_attrs/ndevs/0': '', 'gid_attrs/ndevs/1': 'eth1',
                     'state': '4: ACTIVE', 'counters/port_xmit_data': '256',
                     'counters/port_rcv_data': '128'}
            for name, text in files.items():
                path = port / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            unused, assigned = names / '0', names / '1'
            read_text, iterdir = Path.read_text, Path.iterdir
            failure = errno.EINVAL

            def read(path, *args, **kwargs):
                if path == unused:
                    raise OSError(failure, 'unavailable GID slot')
                return read_text(path, *args, **kwargs)

            def entries(path):
                return iter((unused, assigned)) if path == names else iterdir(path)

            def route():
                replies = [json.dumps([{'dev': 'eth1', 'prefsrc': '10.1.0.1'}]),
                           json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.1'}]}])]
                with patch.object(live.bootstrap, '_call', side_effect=replies):
                    return live._route('10.1.0.2', '10.1.0.1', 'rdma', 'irdma0', 1)

            with patch.object(live, 'Path', return_value=root), \
                    patch.object(Path, 'read_text', read), patch.object(Path, 'iterdir', entries):
                self.assertEqual(route()['device'], 'irdma0')
                assigned.write_text('another-interface')
                with self.assertRaises(ValueError):
                    route()
                assigned.write_text('eth1')
                for failure in (errno.EIO, errno.EPERM, errno.ENOENT, errno.ENODEV, errno.ENODATA):
                    with self.subTest(errno=failure), self.assertRaises(OSError):
                        route()


DROPPED, PROVISIONED = 'aa' * 16, 'bb' * 16
LIVE_CONFIG = {'live_nvme': {'host_id': [1] * 16}}
FIRST = live.cloud_campaign.effective_live_owner(LIVE_CONFIG['live_nvme'], DROPPED, 0)
SECOND = live.cloud_campaign.effective_live_owner(LIVE_CONFIG['live_nvme'], PROVISIONED, 0)


def scoped_route(route, identity):
    route.update(host_nqn=identity['host_nqn'], host_id=list(identity['host_id']))
    route['namespace_identity']['nqn'] = identity['subsystem_scope'] + 'sub-00000000-0000-0000-0000-000000000001'
    return route


def owner_host_evidence(tier='synchronous'):
    """Owner-host (launch `host`) qualification: schema 2 with drop -> reprovision."""
    proof, coordinator, owner = evidence()
    proof['schema_version'] = 2
    proof['checks']['target_reuse'] = True
    for sample in (proof['before_close'], proof['after_hit'], proof['abrupt_child']['end'],
                   proof['abrupt_recovery'], proof['member_transition']):
        for route in sample.get('routes', sample.get('writer_routes')):
            scoped_route(route, FIRST)
    before = [copy.deepcopy(proof['member_transition']['writer_routes'][0])]
    after = [scoped_route(copy.deepcopy(before[0]), SECOND)]
    proof['target_reuse'] = {
        'dropped_database': DROPPED, 'database': PROVISIONED,
        'dropped_initiators': [{'partition': 0, 'database': DROPPED,
                                'host_nqn': FIRST['host_nqn'], 'host_id': FIRST['host_id']}],
        'initiators': [{'partition': 0, 'database': PROVISIONED,
                        'host_nqn': SECOND['host_nqn'], 'host_id': SECOND['host_id']}],
        'routes_before_drop': before, 'routes_after_reprovision': after}
    # The shutdown runs on the reprovisioned database's first fencing generation.
    proof['target_shutdown'][0].update(database=list(bytes.fromhex(PROVISIONED)), epoch=1)
    launch, receipt = owner_evidence(tier)
    launch.pop('node')
    launch.update(partition=0, host={'root': '/data/node-0/storage'})
    receipt.pop('database')
    return proof, coordinator, owner, launch, receipt


class OwnerHostQualificationTest(unittest.TestCase):
    def admit(self, proof, coordinator, owner, launch, receipt, target='kernel'):
        return live.admit(proof, 'rdma', '10.1.0.2', '10.1.0.1', '10.0.0.2', coordinator, owner,
                          target=target, tier='synchronous', owner_launch=launch, owner_receipt=receipt,
                          remote_config=LIVE_CONFIG)

    def test_owner_host_proof_with_target_reuse_is_accepted(self):
        proof, coordinator, owner, launch, receipt = owner_host_evidence()
        self.assertEqual(self.admit(proof, coordinator, owner, launch, receipt)['proof'], proof)

    def test_owner_host_proof_requires_scoped_identities_and_slot_reuse(self):
        mutations = [
            ('schema 1', lambda p, l, r: p.update(schema_version=1)),
            ('reuse check', lambda p, l, r: p['checks'].pop('target_reuse')),
            ('reuse evidence', lambda p, l, r: p.pop('target_reuse')),
            ('same identity', lambda p, l, r: p['target_reuse'].update(
                database=DROPPED, initiators=p['target_reuse']['dropped_initiators'])),
            ('zero identity', lambda p, l, r: p['target_reuse'].update(database='0' * 32)),
            ('initiator nqn', lambda p, l, r: p['target_reuse']['initiators'][0].update(
                host_nqn=SECOND['host_nqn'].replace('-p00000000-', '-p00000001-'))),
            ('initiator id', lambda p, l, r: p['target_reuse']['initiators'][0].update(host_id=[5] * 16)),
            ('no route after', lambda p, l, r: p['target_reuse'].update(routes_after_reprovision=[])),
            ('old initiator after', lambda p, l, r: scoped_route(
                p['target_reuse']['routes_after_reprovision'][0], FIRST)),
            ('other slot after', lambda p, l, r: p['target_reuse']['routes_after_reprovision'][0].update(
                service=4421)),
            ('new initiator before', lambda p, l, r: scoped_route(
                p['before_close']['routes'][0], SECOND)),
            ('unscoped subsystem', lambda p, l, r: p['after_hit']['routes'][0]['namespace_identity'].update(
                nqn='nqn.2026-01.local.tidehunter:live-1')),
            ('configured initiator', lambda p, l, r: p['abrupt_recovery']['routes'][0].update(
                host_nqn='nqn.2014-08.org.nvmexpress:uuid:11111111-1111-4111-8111-111111111111')),
            ('shutdown of dropped', lambda p, l, r: p['target_shutdown'][0].update(
                database=list(bytes.fromhex(DROPPED)))),
            ('shutdown generation', lambda p, l, r: p['target_shutdown'][0].update(epoch=0)),
            ('receipt database', lambda p, l, r: r.update(database=[1] * 16)),
            ('receipt legacy history', lambda p, l, r: r.update(history_persistence='on_disk')),
            ('typed shutdown count', lambda p, l, r: p['target_shutdown'][0]['stats'].update(errors=False)),
        ]
        for name, mutate in mutations:
            proof, coordinator, owner, launch, receipt = owner_host_evidence()
            mutate(proof, launch, receipt)
            with self.subTest(name), self.assertRaises(ValueError):
                self.admit(proof, coordinator, owner, launch, receipt)

    def test_launch_shape_selects_proof_schema(self):
        archive, coordinator, owner = evidence()
        _, _, _, launch, receipt = owner_host_evidence()
        with self.assertRaisesRegex(ValueError, 'receipt missing or mismatched'):
            self.admit(archive, coordinator, owner, launch, receipt)
        proof, coordinator, owner, _, _ = owner_host_evidence()
        archive_launch, archive_receipt = owner_evidence()
        with self.assertRaisesRegex(ValueError, 'receipt missing or mismatched'):
            self.admit(proof, coordinator, owner, archive_launch, archive_receipt)
        # Owner hosts serve kernel targets only.
        proof, coordinator, owner, launch, receipt = owner_host_evidence()
        proof.update(target_implementation='spdk',
                     scope='verified_spdk_namespace_not_physical_media_performance')
        with self.assertRaisesRegex(ValueError, 'receipt missing or mismatched'):
            self.admit(proof, coordinator, owner, launch, receipt, target='spdk')

    def test_prepared_owner_launch_matches_admitted_slot(self):
        node = Path('/run/q/node-0')
        pool = {'address': '10.1.0.2', 'transport': 'rdma', 'public_port_base': 1000,
                'public_service_base': 4420, 'commissioning_port_base': 1001,
                'commissioning_service_base': 4421}
        host = {'listen': '10.0.0.2:9400', 'ready_file': str(node / 'ready'), 'stop_file': str(node / 'stop'),
                'partition': 0, 'host': {'root': str(node / 'storage'), 'max_databases': 1,
                                         'live_target_pool': pool}}
        self.assertIsNone(live._admit_owner_launch(host, node, '10.0.0.2', '10.1.0.2', 'rdma', False))
        for mutate in (lambda l: l['host'].update(max_databases=2),
                       lambda l: l['host']['live_target_pool'].update(public_service_base=4422),
                       lambda l: l['host']['live_target_pool'].update(transport='tcp'),
                       lambda l: l['host'].update(live_target={}),
                       lambda l: l.update(partition=1),
                       lambda l: l.update(node={}),
                       lambda l: l['host'].update(root=str(node / 'other')),
                       lambda l: l.update(stop_file='/elsewhere')):
            launch = copy.deepcopy(host)
            mutate(launch)
            with self.subTest(launch=launch), self.assertRaises(ValueError):
                live._admit_owner_launch(launch, node, '10.0.0.2', '10.1.0.2', 'rdma', False)
        with self.assertRaises(ValueError):
            live._admit_owner_launch(host, node, '10.0.0.2', '10.1.0.2', 'rdma', True)
        target = {'address': '10.1.0.2', 'transport': 'rdma', 'service': 4420, 'port_id': 1000,
                  'commissioning_service': 4421, 'commissioning_port_id': 1001,
                  'state_dir': str(node / 'live-nvmeof')}
        archive = {key: host[key] for key in ('listen', 'ready_file', 'stop_file')}
        archive['node'] = {'root': str(node / 'storage'), 'live_target': target}
        self.assertEqual(live._admit_owner_launch(archive, node, '10.0.0.2', '10.1.0.2', 'rdma', True), target)
        with self.assertRaises(ValueError):
            live._admit_owner_launch(archive, node, '10.0.0.2', '10.1.0.2', 'rdma', False)
        archive['node']['live_target'] = dict(target, commissioning_service=4422)
        with self.assertRaises(ValueError):
            live._admit_owner_launch(archive, node, '10.0.0.2', '10.1.0.2', 'rdma', True)


if __name__ == '__main__':
    unittest.main()
