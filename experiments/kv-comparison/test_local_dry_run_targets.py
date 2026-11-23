"""Contract tests only; no hardware or target processes are started."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import errno
import tempfile
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import MagicMock, patch
import uuid

import cloud_campaign as campaign
import nvme_campaign_owner as owner
import local_dry_run_targets as targets


class TargetsContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'exports').mkdir()
        (self.root / 'ready.json').write_text('{}')

    def sealed(self, **kwargs):
        options = dict(target='kernel', transport='rdma', address='10.99.0.1',
                       base_port_id=1234, base_service=4420)
        options.update(kwargs)
        return targets.SealedTargets(self.root, self.root / 'ready.json',
                                     self.root / 'exports', **options)

    def test_fallback_only_for_rdma_specific_unavailability(self):
        self.assertEqual(targets.select_transport({'transport': 'rdma', 'missing': []}), 'rdma')
        self.assertEqual(targets.select_transport({'transport': 'rdma',
                                                   'missing': ['active_rdma_port']}), 'tcp')
        with self.assertRaises(targets.TargetError):
            targets.select_transport({'transport': 'rdma', 'missing': ['nvme_cli']})
        with self.assertRaises(ValueError):
            targets.select_transport({'transport': 'tcp', 'missing': []})

    def test_spdk_tcp_is_not_falsely_admitted(self):
        with self.assertRaises(targets.TargetError):
            self.sealed(target='spdk', transport='tcp')

    def test_fixture_paths_must_not_escape(self):
        with self.assertRaises(ValueError):
            targets.SealedTargets(self.root, '/etc/passwd', self.root / 'exports',
                target='kernel', transport='rdma', address='10.99.0.1',
                base_port_id=1, base_service=1)

    def test_sealed_cleanup_requires_drain_and_exact_order(self):
        resource = self.sealed()
        with patch.object(targets, '_run', side_effect=[
            {'phase': 'disconnected'}, {'phase': 'cleaned'}]) as run:
            with self.assertRaises(ValueError):
                resource.cleanup(readers_drained=False)
            result = resource.cleanup(readers_drained=True)
        self.assertEqual(result['target']['phase'], 'cleaned')
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[0].args[1][0], 'host-disconnect')
        self.assertEqual(run.call_args_list[1].args[1][0], 'target-cleanup')
        self.assertTrue(resource.disconnect_receipt.exists())
        self.assertTrue(resource.cleanup_receipt.exists())

    def test_failed_disconnect_cannot_cleanup_target(self):
        resource = self.sealed()
        with patch.object(targets, '_run', return_value={'phase': 'partial'}) as run:
            with self.assertRaises(targets.TargetError):
                resource.cleanup(readers_drained=True)
            self.assertEqual(run.call_count, 1)

    def test_target_cleanup_failure_is_resumable(self):
        resource = self.sealed()
        disconnected = {'phase': 'disconnected', 'campaign_id': 'owned'}
        with patch.object(targets, '_run', side_effect=[
            disconnected, targets.TargetError('target cleanup failed'),
            disconnected, {'phase': 'cleaned'}]) as run:
            with self.assertRaisesRegex(targets.TargetError, 'target cleanup failed'):
                resource.cleanup(readers_drained=True)
            result = resource.cleanup(readers_drained=True)
        self.assertEqual(result['target']['phase'], 'cleaned')
        self.assertEqual(run.call_count, 4)
        self.assertTrue(resource.cleanup_receipt.exists())

    def test_retry_refuses_changed_disconnect_proof(self):
        resource = self.sealed()
        with patch.object(targets, '_run', side_effect=[
            {'phase': 'disconnected', 'campaign_id': 'one'},
            targets.TargetError('target cleanup failed'),
            {'phase': 'disconnected', 'campaign_id': 'other'}]) as run:
            with self.assertRaises(targets.TargetError):
                resource.cleanup(readers_drained=True)
            with self.assertRaisesRegex(targets.TargetError, 'existing receipt differs'):
                resource.cleanup(readers_drained=True)
        self.assertEqual(run.call_count, 3)

    def test_failed_setup_without_host_proof_reports_retained_state(self):
        resource = self.sealed()
        resource.target_state.mkdir()
        with patch.object(targets, '_run', side_effect=[
            targets.TargetError('setup failed'),
            {'kind': 'campaign-target', 'campaign_id': 'owned'}]):
            with self.assertRaisesRegex(targets.TargetError,
                                       'setup failed.*fatal cleanup blocker.*disconnect proof'):
                resource.setup()

    def test_failed_connect_recovers_only_matching_campaign(self):
        resource = self.sealed()
        resource.target_state.mkdir()
        resource.host_state.mkdir()
        with patch.object(targets, '_run', side_effect=[
            targets.TargetError('connect failed'),
            {'kind': 'campaign-target', 'campaign_id': 'owned'},
            {'kind': 'campaign-host', 'campaign_id': 'owned'},
            {'phase': 'disconnected'}, {'phase': 'cleaned'}]) as run:
            with self.assertRaisesRegex(targets.TargetError, 'connect failed'):
                resource.connect()
        self.assertEqual(run.call_count, 5)
        self.assertTrue(resource.cleanup_receipt.exists())

    def test_sealed_geometry_required(self):
        resource = self.sealed()
        with patch.object(targets, '_run', return_value={
            'phase': 'connected', 'bindings': [{}],
            'exports': [{'host_receipt': {'queue_geometry': {'observed':
                {'queue_count': 2, 'sqsize': 31}}}}]}):
            with self.assertRaises(targets.TargetError):
                resource.connect()
        self.assertFalse(resource.host_receipt.exists())

    def test_live_stop_requires_proof_and_drain(self):
        daemon = targets.LiveSpdkDaemon(self.root, 0, '/spdk/bin/nvmf_tgt', 1234567890)
        with patch.object(targets, '_run') as run:
            with self.assertRaises(ValueError):
                daemon.stop('/etc/passwd', initiator_drained=True)
            with self.assertRaises(ValueError):
                daemon.stop(self.root / 'proof.json', initiator_drained=False)
            run.assert_not_called()

    def test_live_start_rejects_cpu_shortage_before_launch(self):
        daemon = targets.LiveSpdkDaemon(self.root, 4, '/spdk/bin/nvmf_tgt', 1234567890)
        with patch.object(targets.os, 'sched_getaffinity', return_value=set(range(8))), \
                patch.object(targets, '_run') as run:
            with self.assertRaisesRegex(targets.TargetError, 'distinct CPUs'):
                daemon.start()
            run.assert_not_called()

    def test_live_socket_rejects_unowned_paths(self):
        for invalid in ('/run/tidehunter-spdk-live/../rpc.sock',
                        '/run/tidehunter-spdk-live/' + 'A' * 32 + '/rpc.sock',
                        '/tmp/' + 'a' * 32 + '/rpc.sock'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                targets.LiveSpdkDaemon(self.root, 0, '/spdk/bin/nvmf_tgt',
                                       1234567890, socket=invalid)

    def test_live_geometry_rejects_unsafe_controller(self):
        with self.assertRaises(ValueError):
            targets.record_live_geometry(self.root, '../nvme0', nqn='nqn.x',
                                         address='10.99.0.1', service=4420, transport='rdma')

    def test_kernel_qualification_rejects_invalid_transport(self):
        with self.assertRaises(ValueError):
            targets.qualify_kernel_live(self.root, '/binary', transport='other')

    def test_kernel_qualification_passes_tcp_to_worker(self):
        with patch.object(targets.subprocess, 'run') as run:
            run.return_value.returncode = 0
            run.return_value.stdout = '{"complete": true}'
            self.assertTrue(targets.qualify_kernel_live(self.root, '/binary', transport='tcp')['complete'])
            self.assertEqual(run.call_args.args[0][-3], 'tcp')

    def test_kernel_worker_rejects_untrusted_output_before_creating_artifacts(self):
        self.root.chmod(0o777)
        before = set(self.root.iterdir())
        with patch.object(targets.os, 'geteuid', return_value=0):
            with self.assertRaisesRegex(ValueError, 'untrusted directory'):
                targets._kernel_live_worker(self.root, self.root / 'missing-binary',
                                            'rdma', 120, 0)
        self.assertEqual(set(self.root.iterdir()), before)
    def test_kernel_qualification_requires_prepared_durability(self):
        for tier, wrong_tier in (('native', 'synchronous'), ('synchronous', 'native')):
            with self.subTest(tier=tier):
                targets._admit_kernel_durability({'live_nvme': {'leased_durability': tier}}, tier)
                for remote in ({}, {'live_nvme': {'leased_durability': wrong_tier}}):
                    with self.assertRaisesRegex(targets.TargetError, 'durability differs'):
                        targets._admit_kernel_durability(remote, tier)




class OwnedControllerTeardownTests(unittest.TestCase):
    HOST = 'nqn.2014-08.org.nvmexpress:uuid:run'

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sysfs = Path(self.tmp.name) / 'nvme'
        self.sysfs.mkdir()

    def controller(self, name, hostnqn=HOST, service=4420, transport='tcp', subsystem='nqn.sub'):
        path = self.sysfs / name
        path.mkdir()
        for attr, value in (('transport', transport), ('subsysnqn', subsystem), ('hostnqn', hostnqn),
                            ('hostid', 'id'), ('queue_count', '2'), ('sqsize', '31'),
                            ('address', f'traddr=127.0.0.1,trsvcid={service}')):
            (path / attr).write_text(value + '\n')
        (path / 'delete_controller').write_text('')
        return path

    def delete(self, deadline):
        return targets._delete_owned_controllers(self.sysfs, 'tcp', '127.0.0.1', (4420, 4421),
                                                 self.HOST, deadline)

    def test_public_match_requires_both_nqn_scopes_and_only_public_service(self):
        seed = {'host_id': [1] * 16}
        effective = campaign.effective_live_owner(seed, 'aa' * 16, 0)
        nqn = effective['host_nqn']
        subsystem = effective['subsystem_scope'] + 'sub-00000000-0000-0000-0000-000000000001'
        self.controller('nvme0', hostnqn=nqn, subsystem=subsystem)
        self.controller('nvme1', hostnqn=nqn, subsystem=subsystem.replace('aa' * 16, 'bb' * 16))
        self.controller('nvme2', hostnqn=nqn, subsystem=subsystem.replace('p00000000', 'p00000001'))
        self.controller('nvme3', hostnqn=self.HOST, subsystem=subsystem)
        self.controller('nvme4', hostnqn=nqn, service=4421, subsystem=subsystem)
        matches = targets._owned_controllers(self.sysfs, 'tcp', '127.0.0.1', (4420,), targets._kernel_public_identity)
        self.assertEqual([row['controller'] for row in matches], ['nvme0'])

    def test_private_identity_comes_from_the_persisted_target_identity(self):
        db = 'aa' * 16
        root = Path(self.tmp.name) / 'storage'
        directory = root / ('db-' + db) / 'live-target'
        sealed = directory / 'sealed'
        sealed.mkdir(parents=True)
        scope = campaign.effective_live_owner({'host_id': [1] * 16}, db, 0)['subsystem_scope']
        private = scope + 'commission'
        stat = sealed.stat()

        def write_identity(registry=bytes(range(1, 17)), device=None, inode=None):
            raw = (registry + (stat.st_dev if device is None else device).to_bytes(8, 'little')
                   + (stat.st_ino if inode is None else inode).to_bytes(8, 'little'))
            (directory / 'target-id').write_bytes(raw)

        def identities():
            return targets._kernel_commissioning_identities(root, '10.2.0.2', 'rdma', 4420, 1000)

        with patch.object(owner, '_trusted_dir'), patch.object(
                owner, '_read_bytes', side_effect=lambda file, limit: file.read_bytes()[:limit + 1]):
            write_identity()
            self.assertEqual(identities(), {private: scope})
            write_identity(device=stat.st_dev + 1)
            with self.assertRaisesRegex(targets.TargetError, 'identity changed'):
                identities()
            write_identity(registry=bytes(16))
            with self.assertRaisesRegex(targets.TargetError,
                                        'invalid commissioning target identity'):
                identities()
            (directory / 'target-id').unlink()
            self.assertEqual(identities(), {})

    def test_kernel_reuse_requires_shutdown_of_exact_reprovisioned_database(self):
        remote = {'live_nvme': {'host_id': [1] * 16}}
        old, new = 'aa' * 16, 'bb' * 16
        address, service, endpoint = '127.0.0.1', 4420, '127.0.0.1:9400'

        def evidence(database):
            owner = campaign.effective_live_owner(remote['live_nvme'], database, 0)
            initiator = {key: owner[key] for key in ('host_id', 'host_nqn')}
            route = dict(initiator, address=address, service=service, transport='tcp',
                         namespace_identity={'controller_instance': 3, 'nqn': owner['subsystem_scope']
                             + 'sub-00000000-0000-0000-0000-000000000001'})
            return dict(initiator, database=database, partition=0), route

        first, before = evidence(old)
        second, after = evidence(new)
        shutdown = {'database': list(bytes.fromhex(new)), 'partition': 0, 'epoch': 1, 'endpoint': endpoint,
                    'stats': {'errors': 0, 'owned_namespace_count': 0, 'fixture_only': False}}
        proof = {'target_reuse': {'dropped_database': old, 'database': new,
                                'dropped_initiators': [first], 'initiators': [second],
                                'routes_before_drop': [before], 'routes_after_reprovision': [after]},
                 'member_transition': {'writer_routes': [before]}, 'target_shutdown': [shutdown]}

        def admit():
            return targets._admit_kernel_reuse(proof, remote, address, service, 'tcp', endpoint)

        self.assertEqual([step for step, _, _ in admit()[1]],
                         ['routes_before_drop', 'writer_routes', 'routes_after_reprovision'])
        shutdown['database'] = list(bytes.fromhex(old))
        with self.assertRaisesRegex(targets.TargetError, 'clean reprovisioned database'):
            admit()
        shutdown['database'] = list(bytes.fromhex(new))
        after['host_id'] = remote['live_nvme']['host_id']
        with self.assertRaisesRegex(targets.TargetError, 'route differs'):
            admit()

    def test_deletes_only_this_runs_controllers_and_waits_for_removal(self):
        owned = self.controller('nvme0')
        self.controller('nvme1', hostnqn='nqn.other')
        self.controller('nvme2', service=9999)
        self.controller('nvme3', transport='rdma')
        writes = []

        def kernel_delete(fd, data, timeout):
            writes.append((targets.os.readlink(f'/proc/self/fd/{fd}'), data))
            targets.os.close(fd)
            for child in owned.iterdir():
                child.unlink()
            owned.rmdir()

        with patch.object(targets, '_write_fd_bounded', side_effect=kernel_delete):
            self.assertEqual(self.delete(targets.time.monotonic() + 5), ['nvme0'])
        self.assertEqual(writes, [(str(owned / 'delete_controller'), b'1')])
        self.assertEqual(sorted(p.name for p in self.sysfs.iterdir()), ['nvme1', 'nvme2', 'nvme3'])

    def test_controller_that_survives_delete_fails_at_the_deadline(self):
        owned = self.controller('nvme0')
        with self.assertRaisesRegex(targets.TargetError, 'remain after delete'):
            self.delete(targets.time.monotonic() + .2)
        self.assertEqual((owned / 'delete_controller').read_text(), '1')

    def test_replacement_or_changed_identity_after_enumeration_is_never_deleted(self):
        real = targets._owned_controllers

        def replace(path):
            path.rename(Path(self.tmp.name) / 'enumerated')
            return self.controller('nvme0')

        def rename_host(path):
            (path / 'hostnqn').write_text('nqn.foreign\n')
            return path

        for change in (replace, rename_host):
            with self.subTest(change=change.__name__):
                original = self.controller('nvme0')
                changed = []

                def enumerate_then_change(*args):
                    rows = real(*args)
                    if not changed:
                        changed.append(change(original))
                    return rows

                with patch.object(targets, '_owned_controllers', side_effect=enumerate_then_change):
                    with self.assertRaises(targets.TargetError) if change is replace else nullcontext():
                        self.delete(targets.time.monotonic() + .2)
                self.assertEqual((changed[0] / 'delete_controller').read_text(), '')
                for path in (self.sysfs / 'nvme0', Path(self.tmp.name) / 'enumerated'):
                    if path.exists():
                        self.assertEqual((path / 'delete_controller').read_text(), '')
                        for child in path.iterdir():
                            child.unlink()
                        path.rmdir()

    def test_replacement_after_identity_check_deletes_only_the_pinned_object(self):
        original = self.controller('nvme0')
        enumerated = Path(self.tmp.name) / 'enumerated'
        real_fstat = targets.os.fstat

        def fstat_then_replace(fd):
            result = real_fstat(fd)
            if not enumerated.exists():
                original.rename(enumerated)
                self.controller('nvme0')
            return result

        with patch.object(targets.os, 'fstat', side_effect=fstat_then_replace):
            with self.assertRaisesRegex(targets.TargetError, 'remain after delete'):
                self.delete(targets.time.monotonic() + .2)
        self.assertEqual((enumerated / 'delete_controller').read_text(), '1')
        self.assertEqual((self.sysfs / 'nvme0' / 'delete_controller').read_text(), '')

    def test_no_owned_controller_is_a_no_op(self):
        self.controller('nvme0', hostnqn='nqn.other')
        with patch.object(targets, '_write_fd_bounded') as write:
            self.assertEqual(self.delete(targets.time.monotonic()), [])
        write.assert_not_called()

    def test_blocking_sysfs_write_is_bounded(self):
        reader, writer = targets.os.pipe()
        self.addCleanup(targets.os.close, reader)
        targets.os.set_blocking(writer, False)
        try:
            while True:
                targets.os.write(writer, b'x' * 65536)
        except BlockingIOError:
            pass
        targets.os.set_blocking(writer, True)
        with self.assertRaisesRegex(targets.TargetError, 'did not return'):
            targets._write_fd_bounded(writer, b'1', .2)
        # Drain the pipe; the helper thread then writes and closes its descriptor.
        targets.os.set_blocking(reader, False)
        deadline = targets.time.monotonic() + 5
        while targets.time.monotonic() < deadline:
            try:
                if not targets.os.read(reader, 1 << 20):
                    break
            except BlockingIOError:
                targets.time.sleep(.01)


class FailureEvidenceTests(unittest.TestCase):
    def test_nvme_state_reports_host_and_target_objects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'nvme/nvme0').mkdir(parents=True)
            (root / 'nvme/nvme0/state').write_text('live\n')
            (root / 'block/nvme0n2').mkdir(parents=True)
            (root / 'block/nvme0n2/nsid').write_text('2\n')
            namespace = root / 'nvmet/subsystems/nqn.sub/namespaces/2'
            namespace.mkdir(parents=True)
            (namespace / 'enable').write_text('1\n')
            (root / 'nvmet/ports/7/subsystems/nqn.sub').mkdir(parents=True)
            state = targets._nvme_state(root / 'nvme', root / 'block', root / 'nvmet')
        self.assertEqual(state['controllers']['nvme0']['state'], 'live')
        self.assertIn('FileNotFoundError', state['controllers']['nvme0']['subsysnqn'])
        self.assertEqual(state['disks']['nvme0n2']['nsid'], '2')
        self.assertEqual(state['target_namespaces']['nqn.sub/2']['enable'], '1')
        self.assertEqual(state['target_ports'], {'7': ['nqn.sub']})

    def test_process_threads_lists_every_thread_state(self):
        state = targets._process_threads(targets.os.getpid())
        self.assertEqual(state['task_count'], len(state['threads']))
        self.assertTrue(all(row['state'] and 'comm' in row and 'stack' in row for row in state['threads']))


class KernelPortsContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.sysctls = self.root / "ipv4"
        self.sysctls.mkdir()
        (self.sysctls / "ip_local_port_range").write_text("20001 20003\n")
        (self.sysctls / "ip_local_reserved_ports").write_text("20004,20007-20009\n")
        self.configfs = self.root / "nvmet"
        (self.configfs / "ports").mkdir(parents=True)

    def allocate(self, evidence="run", address="10.99.0.2"):
        return targets._kernel_live_ports(
            address, commissioning_address="127.0.0.1", control_address="127.0.0.1",
            state=self.state, evidence=self.root / evidence, deadline=time.monotonic() + 5,
            sysctls=self.sysctls, configfs=self.configfs)

    @staticmethod
    def socket_probe():
        probe = MagicMock()
        probe.__enter__.return_value = probe
        probe.getsockname.return_value = ("127.0.0.1", 42000)
        return probe

    def test_pair_excludes_automatic_reserved_and_unlinked_configfs_ports(self):
        for port_id, service in ((20011, 20020), (9, 20013)):
            port = self.configfs / "ports" / str(port_id)
            port.mkdir()
            (port / "addr_trsvcid").write_text(f"{service}\n")
        sockets = [self.socket_probe() for _ in range(3)]
        with patch.object(targets.socket, "socket", side_effect=sockets):
            control, service, commissioning, claim = self.allocate()
        self.assertEqual((control, service, commissioning), (42000, 20014, 20015))
        self.assertNotIn(service, range(20001, 20004))
        self.assertNotIn(commissioning, range(20001, 20004))
        self.assertEqual(claim.read_text(), f"{self.root / 'run'}\n")
        sockets[0].bind.assert_called_once_with(("127.0.0.1", 0))
        sockets[1].bind.assert_called_once_with(("10.99.0.2", service))
        sockets[2].bind.assert_called_once_with(("127.0.0.1", commissioning))
        for probe in sockets:
            probe.__exit__.assert_called_once()

    def test_concurrent_allocations_keep_distinct_pairs_until_cleanup(self):
        barrier = threading.Barrier(2, timeout=5)

        def concurrent(name):
            barrier.wait()
            return self.allocate(name)

        with patch.object(targets.socket, "socket", side_effect=self.socket_probe):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(concurrent, ("first", "second")))
            pairs = sorted((service, commissioning) for _, service, commissioning, _ in results)
            self.assertEqual(pairs, [(20010, 20011), (20012, 20013)])
            self.assertEqual({claim.read_text().strip() for _, _, _, claim in results},
                             {str(self.root / "first"), str(self.root / "second")})
            min(results)[-1].unlink()
            self.assertEqual(self.allocate("after-cleanup")[1:3], pairs[0])

    def test_detects_commissioning_address_conflict_without_claiming_pair(self):
        sockets = [self.socket_probe() for _ in range(5)]
        sockets[2].bind.side_effect = OSError(errno.EADDRINUSE, "port busy")
        with patch.object(targets.socket, "socket", side_effect=sockets):
            self.assertEqual(self.allocate()[1:3], (20012, 20013))
        sockets[2].bind.assert_called_once_with(("127.0.0.1", 20011))
        self.assertFalse((self.state / "20010.claim").exists())
        for probe in sockets:
            probe.__exit__.assert_called_once()

    def test_fails_closed_if_every_pool_pair_overlaps_automatic_range(self):
        (self.sysctls / "ip_local_port_range").write_text("20000 65535\n")
        probe = self.socket_probe()
        with patch.object(targets.socket, "socket", return_value=probe):
            with self.assertRaisesRegex(targets.TargetError, "no free adjacent non-ephemeral"):
                self.allocate()
        probe.bind.assert_called_once_with(("127.0.0.1", 0))
        self.assertEqual(list(self.state.glob("*.claim")), [])


class KernelGeometryAdmissionTests(unittest.TestCase):
    """The client records each controller it connected; the harness admits those records exactly."""

    GEOMETRY = {"io_queues": 1, "queue_size": 32, "poll_queues": 0}

    def setUp(self):
        self.live = {"host_id": [1] * 16, "address": "10.99.0.2", "service": 4420}
        self.first = campaign.effective_live_owner(self.live, "aa" * 16, 0)
        self.second = campaign.effective_live_owner(self.live, "bb" * 16, 0)

    def route(self, owner, instance, *, queue_count=2, sqsize=31, inode=100):
        return {"host_nqn": owner["host_nqn"], "host_id": owner["host_id"], "address": owner["address"],
                "service": owner["service"], "transport": "tcp",
                "namespace_identity": {
                    "controller_instance": instance,
                    "nqn": owner["subsystem_scope"] + "sub-00000000-0000-0000-0000-000000000001",
                    "controller": {"queue_count": queue_count, "sqsize": sqsize,
                                   "sysfs_device": 22, "sysfs_inode": inode}}}

    def lifecycles(self, first=None, second=None):
        return [("writer_routes", [self.route(self.first, 0)] if first is None else first, self.first),
                ("routes_after_reprovision", [self.route(self.second, 3, inode=103)] if second is None else second,
                 self.second)]

    def admit(self, lifecycles, record=None):
        record = {} if record is None else record
        targets._admit_kernel_geometry(record, lifecycles, self.GEOMETRY)
        return record

    def refuse(self, lifecycles, pattern):
        with self.assertRaisesRegex(targets.TargetError, pattern):
            self.admit(lifecycles)

    def test_configured_geometry_is_the_one_the_client_connects_with(self):
        self.assertEqual(targets._kernel_live_prepare_input(
            "g", "e", "tcp", "a", 1, 1, "t")["live_nvmeof"]["queue_geometry"], targets.KERNEL_QUEUE_GEOMETRY)
        self.assertEqual(targets.KERNEL_QUEUE_GEOMETRY, self.GEOMETRY)

    def test_both_lifecycles_are_admitted_with_their_records(self):
        record = self.admit(self.lifecycles())
        self.assertEqual([(row["step"], row["controller"], row["queue_count"], row["sqsize"])
                          for row in record["controller_records"]],
                         [("writer_routes", "nvme0", 2, 31), ("routes_after_reprovision", "nvme3", 2, 31)])

    def test_missing_route_record_is_refused(self):
        self.refuse(self.lifecycles(second=[]), "no route record at routes_after_reprovision")

    def test_missing_controller_record_is_refused(self):
        route = self.route(self.first, 0)
        del route["namespace_identity"]["controller"]
        self.refuse(self.lifecycles(first=[route]), "controller record missing at writer_routes")

    def test_incomplete_or_mistyped_controller_record_is_refused(self):
        for change in ({"sysfs_inode": None}, {"queue_count": "2"}, {"extra": 1}):
            with self.subTest(change=change):
                route = self.route(self.first, 0)
                route["namespace_identity"]["controller"].update(change)
                self.refuse(self.lifecycles(first=[route]), "controller record missing")

    def test_wrong_queue_count_is_refused(self):
        self.refuse(self.lifecycles(first=[self.route(self.first, 0, queue_count=5)]),
                    "nvme0 has queue_count=5, sqsize=31; expected 2, 31")

    def test_wrong_sqsize_is_refused(self):
        self.refuse(self.lifecycles(second=[self.route(self.second, 3, sqsize=127, inode=103)]),
                    "nvme3 has queue_count=2, sqsize=127")

    def test_wrong_subsystem_is_refused(self):
        route = self.route(self.first, 0)
        route["namespace_identity"]["nqn"] = self.second["subsystem_scope"] + "sub-00000000-0000-0000-0000-000000000001"
        self.refuse(self.lifecycles(first=[route]), "names another initiator or subsystem")

    def test_wrong_initiator_is_refused(self):
        self.refuse(self.lifecycles(second=[self.route(self.first, 3, inode=103)]),
                    "names another initiator or subsystem")

    def test_one_controller_serves_several_routes_of_one_lifecycle(self):
        members = [self.route(self.first, 0) for _ in range(3)]
        record = self.admit(self.lifecycles(first=members))
        self.assertEqual([row["controller"] for row in record["controller_records"]][:3], ["nvme0"] * 3)

    def test_one_pair_under_two_subsystems_is_refused(self):
        other = self.route(self.first, 0)
        other["namespace_identity"]["nqn"] = (
            self.first["subsystem_scope"] + "sub-00000000-0000-0000-0000-000000000002")
        self.refuse(self.lifecycles(first=[self.route(self.first, 0), other]), "disagree on instance, subsystem")

    def test_one_pair_under_two_instances_is_refused(self):
        self.refuse(self.lifecycles(first=[self.route(self.first, 0), self.route(self.first, 1)]),
                    "controller nvme1 has routes that disagree")

    def test_a_reused_instance_name_under_another_pair_is_admitted(self):
        self.admit(self.lifecycles(first=[self.route(self.first, 0), self.route(self.first, 0, inode=101)]))

    def test_distinct_controllers_sharing_the_sysfs_device_are_admitted(self):
        self.admit(self.lifecycles(first=[self.route(self.first, 0), self.route(self.first, 1, inode=101)]))

    def test_a_bad_controller_instance_is_refused(self):
        for instance in (-1, "0", True, None):
            with self.subTest(instance=instance):
                self.refuse(self.lifecycles(first=[self.route(self.first, instance)]), "controller record missing")

    def test_a_reused_controller_name_across_lifecycles_is_admitted(self):
        self.admit(self.lifecycles(second=[self.route(self.second, 0, inode=104)]))


if __name__ == '__main__':
    unittest.main()
