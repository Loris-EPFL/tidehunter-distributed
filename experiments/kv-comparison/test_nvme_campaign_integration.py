"""Pure controller and matrix boundary tests; no NVMe hardware is exercised."""
import copy
import hashlib
import json
import itertools
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cloud_campaign
import cloud_controller as controller


HERE = Path(__file__).resolve().parent


class CampaignModeTests(unittest.TestCase):
    def test_focused_matrix_matches_read_sources_and_excludes_nvme_writes(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        jobs = cloud_campaign.make_matrix(spec)
        self.assertEqual(len(jobs), 125)
        reads = [job for job in jobs if job['cohort'] == 'sealed_read']
        self.assertTrue(reads)
        for size in (64, 256, 1024, 8192, 65536):
            sources = {job['sealed_dataset']['source_id'] for job in reads if job['value_size'] == size}
            self.assertEqual(len(sources), 1)
            self.assertEqual({job['backend'] for job in reads if job['value_size'] == size}, set(spec['backends']))
        self.assertFalse(any(job['backend'] == 'tidehunter_nvme_tcp' and
                             job['cohort'] == 'native_write_regression' for job in jobs))

    def test_selected_backend_order_cannot_reenter_shared_tcp_source(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        for order in itertools.permutations(spec['backends']):
            with self.subTest(order=order):
                jobs = cloud_campaign.make_matrix(dict(spec, backends=list(order)))
                sources = {}
                for job in jobs:
                    if job['cohort'] == 'sealed_read':
                        sources.setdefault(job['sealed_dataset']['source_id'], []).append(job)
                for block in sources.values():
                    indices = [index for index, job in enumerate(block)
                               if job['backend'] in ('tidehunter_tcp', 'tidehunter_nvme_tcp')]
                    contiguous = block[min(indices):max(indices) + 1]
                    self.assertEqual({job['backend'] for job in contiguous},
                                     {'tidehunter_tcp', 'tidehunter_nvme_tcp'})
                    first_visits = list(dict.fromkeys(job['backend'] for job in contiguous))
                    self.assertEqual(first_visits,
                                     [backend for backend in order
                                      if backend in ('tidehunter_tcp', 'tidehunter_nvme_tcp')])

    def test_nvme_backend_is_rejected_outside_focused_mode(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        spec['mode'] = 'standard'
        with self.assertRaises(ValueError):
            cloud_campaign.make_matrix(spec)


class HostCpuIntervalTests(unittest.TestCase):
    def test_kernel_work_is_host_scoped_and_reboots_are_unavailable(self):
        def snapshot(boot, counters):
            return {'nvme': {'host_identity': {'boot_id': boot}}, 'host_cpu_hz': 100,
                    'host_cpu_stat': 'cpu ' + ' '.join(map(str, counters))}
        start = [snapshot(str(i), [0] * 8) for i in range(6)]
        end = [snapshot(str(i), [100, 0, 200, 300, 0, 50, 25, 0]) for i in range(6)]
        end[3]['nvme']['host_identity']['boot_id'] = 'rebooted'
        result = controller.host_cpu_interval(start, end)
        self.assertEqual(result['hosts'][0]['system_seconds'], 2.75)
        self.assertFalse(result['hosts'][3]['available'])
        self.assertIn('rebooted', result['hosts'][3]['reason'])
        self.assertIn('not attribution', result['scope'])


class ControllerBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.c = controller.Controller.__new__(controller.Controller)
        self.c.config = {'prefix': 'run', 'repository_root': str(self.root),
                         'fleet': {'nodes': [{'address': f'10.0.0.{i + 1}'} for i in range(5)]}}
        self.c.root = self.root
        self.c.clients = {'s3': Mock()}
        self.c.active_nvme = None
        self.c.active_tide = None
        self.c.tide_cleanup_forbidden = False
        self.c.shared_tide = None
        self.c.remaining = Mock(return_value=300)
        self.c.cleanup_deadline = Mock(return_value=100)
        self.c.cleanup_timeout = Mock(return_value=60)
        self.c.request = Mock(return_value={'complete': True})

    def mirror_fixtures(self, base, uuid, manifest):
        self.c.config['fleet']['data_root'] = str(base / 'fleet')
        roots, targets = [], []
        for index in range(5):
            root = base / f'mirror-{index}'
            root.mkdir()
            path = root / 'manifest.json'
            path.write_text(json.dumps(manifest))
            remote = base / 'fleet' / 'cells' / 'source-case' / f'node-{index}' / 'nvmeof' / 'export'
            roots.append(str(root))
            targets.append({'target': {'transport': 'tcp', 'export_root': str(remote), 'exports': [{
                'export_uuid': uuid, 'manifest': str(remote / 'manifest.json'),
                'manifest_sha256': controller.digest(path),
                'target_receipt': {'selected_transport': 'tcp', 'target_cache': 'buffered_file_page_cache',
                                   'target_cache_verified': True}}]}})
        return roots, targets

    def test_staging_requires_exact_object_and_preserves_verified_bytes(self):
        raw = b'{\"catalog\":[{}],\"nqn\":\"test\"}'
        checksum = hashlib.sha256(raw).hexdigest()
        self.c.config.update(bucket='bucket', account='account')
        self.c.config['fleet']['data_root'] = str(self.root / 'fleet')
        uuid = [1] * 16
        work = self.root / 'fleet/cells/case/node-0/nvmeof'
        key = f'run/results/cells/case/storage/0/nvme-manifests/{bytes(uuid).hex()}.json'
        entry = {'manifest_path': str(work / 'export/partition/manifest.json'),
                 'export_uuid': uuid, 'manifest_key': key, 'manifest_sha256': checksum,
                 'manifest_bytes': len(raw)}
        exports = [{'work_dir': str(work), 'ready': {'exports': [entry]}}]
        s3 = self.c.clients['s3']
        s3.head_object.return_value = {'ContentLength': len(raw), 'Metadata': {'sha256': checksum}}
        s3.download_file.side_effect = lambda bucket, obj, dest, **kw: Path(dest).write_bytes(raw)
        real_path = Path
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        with patch.object(controller, 'Path', side_effect=mapped_path):
            mirrors = self.c.stage_nvme_manifests('case', exports)
        self.assertEqual((real_path(mirrors[0]) / 'partition/manifest.json').read_bytes(), raw)
        s3.head_object.assert_called_once_with(Bucket='bucket', Key=key, ExpectedBucketOwner='account')
        self.assertEqual(s3.download_file.call_args.args[:2], ('bucket', key))
        self.assertEqual(s3.download_file.call_args.kwargs['ExtraArgs'], {'ExpectedBucketOwner': 'account'})

    def test_owner_batch_requires_every_exact_owner_to_confirm(self):
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 3}
        with self.assertRaises(controller.InfrastructureError):
            self.c.owner_batch('nvme_owner_stop', 'case')
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))

    def test_guest_owner_cleanup_accepts_only_exact_s3_proof_location(self):
        import nvme_campaign_owner
        config = {'prefix': 'run', 'spec': {'mode': 'nvmeof-focused'},
                  'fleet': {'data_root': '/unused'}}
        request = {'index': 2, 'action': 'nvme_owner_target_cleanup', 'case_id': 'case',
                   'generation': 'gen', 'host_disconnected_key': 'run/results/cells/case/storage/2/nvme-disconnect/gen.json'}
        clients = {'s3': Mock()}
        proof = {'phase': 'disconnected'}
        with patch.object(controller, 'verify_local_identity'), \
             patch.object(controller, 'guarded', return_value=self.root), \
             patch.object(controller, 'read_s3_json', return_value=proof) as read_proof, \
             patch.object(nvme_campaign_owner, 'execute', return_value={'complete': True}) as execute:
            self.assertEqual(controller.guest_operation(config, request, clients), {'complete': True})
            read_proof.assert_called_once_with(clients['s3'], config, request['host_disconnected_key'])
            self.assertIs(execute.call_args.args[1]['host_disconnected'], proof)
            execute.reset_mock()
            read_proof.reset_mock()
            forged = dict(request, host_disconnected_key='run/results/cells/case/storage/1/nvme-disconnect/gen.json',
                          host_disconnected=proof)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, forged, clients)
            read_proof.assert_not_called()
            execute.assert_not_called()
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(dict(config, spec={'mode': 'standard'}), request, clients)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, dict(request, index=-1), clients)
            execute.assert_not_called()

    def test_setup_addresses_are_taken_from_owned_fleet(self):
        self.c.owner_batch('nvme_owner_target_setup', 'case', generation='generation')
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['address'], self.c.config['fleet']['nodes'][index]['address'])
            self.assertEqual(request['generation'], 'generation')

    def test_shared_tcp_source_reuses_prepared_owner_dataset(self):
        options = {'external_remote_config': {'read_lanes': 1}, 'evidence': 'old'}
        self.c.shared_tide = {'identity': ('source', 'tcp'), 'options': copy.deepcopy(options)}
        job = {'backend': 'tidehunter_nvme_tcp', 'sealed_dataset': {'source_id': 'source'}, 'read_lanes': 4}
        self.c.prepare_tide = Mock(side_effect=AssertionError('must reuse prepared dataset'))
        actual = self.c.prepared_tide_options(job, self.root / 'case')
        self.assertEqual(actual['external_remote_config']['read_lanes'], 4)
        self.assertEqual(actual['evidence'], str(self.root / 'case' / 'read-evidence'))
        self.assertEqual(self.c.shared_tide['options'], options)
        self.c.prepare_tide.assert_not_called()

    def test_outstanding_namespaces_prevent_source_replacement_and_owner_stop(self):
        self.c.shared_tide = {'identity': ('old', 'tcp')}
        self.c.active_nvme = {'generation': 'g', 'case': 'old-case', 'hosts': []}
        job = {'backend': 'tidehunter_tcp', 'sealed_dataset': {'source_id': 'new'}}
        with self.assertRaises(controller.SafetyError):
            self.c.prepared_tide_options(job, self.root)
        self.c.active_tide = ({'case_id': 'old-case'}, self.root, [0])
        with self.assertRaises((controller.SafetyError, controller.InfrastructureError)):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_unconfirmed_client_quiescence_forbids_owner_cleanup(self):
        self.c.active_tide = ({'case_id': 'case'}, self.root, [0])
        self.c.tide_cleanup_forbidden = True
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_undrained_or_incomplete_host_set_is_quarantined(self):
        for drained, count in ((False, 5), (True, 4)):
            with self.subTest(drained=drained, hosts=count):
                active = {'generation': 'g', 'case': 'case',
                          'hosts': [{'index': i, 'state': f'/state/{i}'} for i in range(count)]}
                self.c.active_nvme = active
                with patch.object(controller, 'command') as command:
                    with self.assertRaises(controller.InfrastructureError):
                        self.c.disconnect_nvme(drained=drained)
                command.assert_not_called()
                self.c.request.assert_not_called()
                self.assertIs(self.c.active_nvme, active)

    def test_cleanup_uses_exact_per_owner_proof_and_retains_unknown_outcome(self):
        real_path = Path
        control = self.root / 'control' / 'run'
        control.mkdir(parents=True)
        hosts = []
        for index in range(5):
            host = {'index': index, 'state': str(control / f'state-{index}'),
                    'mirror': str(control / f'mirror-{index}'),
                    'ready_path': str(control / f'ready-{index}.json'),
                    'target_path': str(control / f'target-{index}.json')}
            real_path(host['state']).mkdir()
            real_path(host['mirror']).mkdir()
            real_path(host['ready_path']).write_text('{}')
            real_path(host['target_path']).write_text('{}')
            hosts.append(host)
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        active = {'generation': 'g', 'case': 'case', 'case_directory': str(self.root), 'hosts': hosts}
        self.c.active_nvme = active
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 2}
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', return_value=json.dumps({'phase': 'disconnected'})), \
             patch.object(controller, 's3_json') as upload:
            with self.assertRaises(controller.InfrastructureError):
                self.c.disconnect_nvme(drained=True)
        self.assertIs(self.c.active_nvme, active)
        self.assertEqual(upload.call_count, 5)
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['host_disconnected_key'], f'run/results/cells/case/storage/{index}/nvme-disconnect/g.json')
            self.assertIs(request['readers_drained'], True)
            self.assertEqual(request['action'], 'nvme_owner_target_cleanup')
        for host in hosts:
            for field in ('state', 'ready_path', 'target_path'):
                self.assertEqual(real_path(host[field]).exists(), host['index'] == 2)
            self.assertTrue(real_path(host['mirror']).exists())

    def test_reader_bindings_require_exact_export_uuid_set(self):
        real_path = Path
        manifest = {'identity': 'manifest', 'catalog': [{}], 'nqn': 'test-nqn'}
        roots, targets = self.mirror_fixtures(self.root, [1, 2], manifest)
        self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                              'mirror_roots': roots, 'options': {'external_remote_config': {
                                  'read_memory_allowance_bytes': 256 * 1024**2,
                                  'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
        self.c.owner_batch = Mock(return_value=targets)
        def mapped_path(value):
            path = real_path(value)
            if str(path) == '/var/lib/tidehunter-kv-control':
                return self.root / 'control'
            return path
        job = {'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp'}
        result = {'phase': 'connected', 'transport': 'tcp',
                  'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test-only', 'transport': 'tcp'}],
                  'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}
        def command(argv, *, timeout):
            if argv[:2] == ['nvme', 'id-ctrl']:
                return json.dumps({'cntlid': 7})
            return json.dumps(result)
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', side_effect=command):
            readers = self.c.connect_nvme(job, self.root)
        self.assertEqual(len(readers), 5)
        self.assertTrue(all(row['binding'] == {'namespace_path': '/dev/test-only', 'transport': 'tcp'}
                            and row['manifest'] == manifest for row in readers))
        self.assertEqual(len(self.c.active_nvme['hosts']), 5)
        self.assertTrue(all(host['controllers'] == [{'export_uuid': [1, 2], 'ctrl_id': 7}]
                            for host in self.c.active_nvme['hosts']))

    def test_missing_or_extra_reader_binding_is_rejected_and_quarantined(self):
        real_path = Path
        for bindings in ([], [{'export_uuid': [1], 'namespace_path': '/dev/test', 'transport': 'tcp'},
                              {'export_uuid': [2], 'namespace_path': '/dev/other', 'transport': 'tcp'}]):
            with self.subTest(bindings=bindings), tempfile.TemporaryDirectory() as directory:
                base = real_path(directory)
                self.c.active_nvme = None
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = real_path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                result = {'phase': 'connected', 'transport': 'tcp', 'bindings': bindings,
                          'exports': [{'export_uuid': [1], 'host_receipt': {'controller': 'nvme0'}}]}
                def command(argv, *, timeout):
                    if argv[:2] == ['nvme', 'id-ctrl']:
                        return json.dumps({'cntlid': 7})
                    return json.dumps(result)
                with patch.object(controller, 'Path', side_effect=mapped_path), \
                     patch.object(controller, 'command', side_effect=command):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp'}, base)
                self.assertEqual(len(self.c.active_nvme['hosts']), 1)

    def test_target_policy_and_transport_mismatch_prevent_host_connection(self):
        for field, value in (('selected_transport', 'rdma'), ('target_cache', 'direct'),
                             ('target_cache_verified', False)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                targets[0]['target']['exports'][0]['target_receipt'][field] = value
                self.c.active_nvme = None
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = Path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                with patch.object(controller, 'command') as command, \
                     patch.object(controller, 'dump'), \
                     patch.object(controller, 'Path', side_effect=mapped_path):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp'}, base)
                command.assert_not_called()
                self.assertEqual(self.c.active_nvme['hosts'], [])
                self.assertIs(self.c.active_nvme['targets'], targets)


if __name__ == '__main__':
    unittest.main()
