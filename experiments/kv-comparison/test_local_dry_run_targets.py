"""Contract tests only; no hardware or target processes are started."""
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

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

    def test_kernel_worker_rejects_untrusted_output_before_creating_artifacts(self):
        self.root.chmod(0o777)
        before = set(self.root.iterdir())
        with patch.object(targets.os, 'geteuid', return_value=0):
            with self.assertRaisesRegex(ValueError, 'untrusted directory'):
                targets._kernel_live_worker(self.root, self.root / 'missing-binary',
                                            'rdma', 120, 0)
        self.assertEqual(set(self.root.iterdir()), before)


if __name__ == '__main__':
    unittest.main()
