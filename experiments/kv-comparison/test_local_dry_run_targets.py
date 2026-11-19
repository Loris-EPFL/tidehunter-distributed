"""Contract tests only; no hardware or target processes are started."""
import errno
import tempfile
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

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
            targets.qualify_kernel_live(self.root, '/binary', transport='other',
                                        history_persistence='on_disk')

    def test_kernel_qualification_requires_durable_history(self):
        with self.assertRaisesRegex(ValueError, 'history persistence'):
            targets.qualify_kernel_live(self.root, '/binary', history_persistence='memory_only')

    def test_kernel_qualification_passes_history_to_worker(self):
        for history in ('on_disk', 'wal_replay'):
            with self.subTest(history=history), patch.object(targets.subprocess, 'run') as run:
                run.return_value.returncode = 0
                run.return_value.stdout = '{"complete": true}'
                self.assertTrue(targets.qualify_kernel_live(
                    self.root, '/binary', transport='tcp', history_persistence=history)['complete'])
                self.assertEqual(run.call_args.args[0][-1], history)

    def test_kernel_qualification_requires_prepared_leased_per_owner_route(self):
        for tier in ('native', 'synchronous'):
            live = {'write_mode': 'leased', 'export_mode': 'per_owner', 'leased_durability': tier}
            target = {'export_mode': 'per_owner'}
            with self.subTest(tier=tier):
                targets._admit_kernel_leased_route({'live_nvme': live}, target, tier)
            for field, value in (('write_mode', 'reserved'), ('export_mode', 'on_demand'),
                                 ('leased_durability', None)):
                with self.subTest(tier=tier, field=field), self.assertRaisesRegex(
                        targets.TargetError, 'not leased per-owner'):
                    targets._admit_kernel_leased_route({'live_nvme': live | {field: value}}, target, tier)
            with self.subTest(tier=tier, field='target'), self.assertRaisesRegex(
                    targets.TargetError, 'not leased per-owner'):
                targets._admit_kernel_leased_route({'live_nvme': live}, {'export_mode': 'on_demand'}, tier)

    def test_kernel_worker_rejects_untrusted_output_before_creating_artifacts(self):
        self.root.chmod(0o777)
        before = set(self.root.iterdir())
        with patch.object(targets.os, 'geteuid', return_value=0):
            with self.assertRaisesRegex(ValueError, 'untrusted directory'):
                targets._kernel_live_worker(self.root, self.root / 'missing-binary',
                                            'rdma', 120, 0, history_persistence='on_disk')
        self.assertEqual(set(self.root.iterdir()), before)


class KernelPortsContractTests(unittest.TestCase):
    def sockets(self):
        sockets = [MagicMock() for _ in range(3)]
        for candidate in sockets:
            candidate.__enter__.return_value = candidate
        sockets[0].getsockname.return_value = ("127.0.0.1", 41000)
        sockets[1].getsockname.return_value = ("10.99.0.2", 42000)
        return sockets

    def test_consecutive_pair_and_distinct_control_are_bind_checked(self):
        sockets = self.sockets()
        with patch.object(targets.socket, "socket", side_effect=sockets):
            control, service, commissioning = targets._kernel_live_ports(
                "10.99.0.2", commissioning_address="127.0.0.1", control_address="127.0.0.1")
        self.assertEqual((control, service, commissioning), (41000, 42000, 42001))
        self.assertNotIn(control, (service, commissioning))
        sockets[0].bind.assert_called_once_with(("127.0.0.1", 0))
        sockets[1].bind.assert_called_once_with(("10.99.0.2", 0))
        sockets[2].bind.assert_called_once_with(("127.0.0.1", service + 1))
        for candidate in sockets:
            candidate.__exit__.assert_called_once()

    def test_pair_collision_exhausts_bounded_attempts_and_closes_sockets(self):
        sockets = self.sockets()
        sockets[2].bind.side_effect = OSError(errno.EADDRINUSE, "port busy")
        with patch.object(targets.socket, "socket", side_effect=sockets * 32) as factory:
            with self.assertRaisesRegex(targets.TargetError, "after 32 attempts"):
                targets._kernel_live_ports(
                    "10.99.0.2", commissioning_address="127.0.0.1", control_address="127.0.0.1")
        self.assertEqual(factory.call_count, 96)
        for candidate in sockets:
            self.assertEqual(candidate.__exit__.call_count, 32)

    def test_loopback_only_commissioning_listener_is_detected(self):
        real_socket = targets.socket.socket
        with real_socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            busy_port = listener.getsockname()[1]
            sockets = self.sockets()
            sockets[0].getsockname.return_value = ("127.0.0.1", 1)
            sockets[1].getsockname.return_value = ("10.99.0.2", busy_port - 1)
            commissioning_sockets = []

            def candidates():
                for _ in range(32):
                    yield sockets[0]
                    yield sockets[1]
                    candidate = real_socket()
                    commissioning_sockets.append(candidate)
                    yield candidate

            with patch.object(targets.socket, "socket", side_effect=candidates()) as factory:
                with self.assertRaisesRegex(targets.TargetError, "after 32 attempts"):
                    targets._kernel_live_ports(
                        "10.99.0.2", commissioning_address="127.0.0.1",
                        control_address="127.0.0.1")
            self.assertEqual(factory.call_count, 96)
            self.assertTrue(all(candidate.fileno() == -1 for candidate in commissioning_sockets))


if __name__ == '__main__':
    unittest.main()
