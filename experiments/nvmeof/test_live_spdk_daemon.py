import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import live_spdk_daemon as daemon


class LiveDaemonTests(unittest.TestCase):
    def test_requires_initiator_drain_before_reading_proof(self):
        with mock.patch.object(daemon.campaign, 'trusted_json') as proof:
            with self.assertRaisesRegex(ValueError, 'initiator drain'):
                daemon.stop('/state', '/proof', False)
        proof.assert_not_called()

    def test_rejects_incomplete_owner_proof_before_touching_daemon(self):
        with mock.patch.object(daemon.campaign, 'trusted_json', return_value={}), \
             mock.patch.object(daemon.campaign, 'campaign_state') as state:
            with self.assertRaisesRegex(ValueError, 'owner namespace drain'):
                daemon.stop('/state', '/proof', True)
        state.assert_not_called()

    def test_live_never_accepts_old_read_only_artifact(self):
        with mock.patch.object(daemon.spdk_artifacts, 'admit_prefix',
                               return_value={'target_scope': 'sealed_read_only_file_aio_direct'}), \
             mock.patch.object(daemon.time, 'time', return_value=100):
            with self.assertRaisesRegex(ValueError, 'reviewed writable'):
                daemon.start('/state', '/run/tidehunter-spdk-live/' + 'a' * 32 + '/rpc.sock',
                             '/opt/spdk/bin/nvmf_tgt', 200)

    def test_replaced_service_is_rejected_before_rpc(self):
        state = {'pid': 123}
        with mock.patch.object(daemon.spdk, 'unit_status',
                               return_value={'ActiveState': 'active', 'MainPID': '124'}), \
             mock.patch.object(daemon.spdk, 'verify_process') as identity:
            with self.assertRaisesRegex(ValueError, 'identity or state'):
                daemon.verify(state)
        identity.assert_not_called()

    def test_rpc_peer_must_be_the_owned_root_daemon(self):
        state = {'pid': 123, 'rpc_socket': '/run/test/rpc.sock'}
        channel = mock.MagicMock()
        channel.__enter__.return_value = channel
        channel.getsockopt.return_value = daemon.struct.pack('3i', 124, 0, 0)
        with mock.patch.object(daemon.spdk, 'unit_status',
                               return_value={'ActiveState': 'active', 'MainPID': '123'}), \
             mock.patch.object(daemon.spdk, 'verify_process'), \
             mock.patch.object(daemon.socket, 'socket', return_value=channel):
            with self.assertRaisesRegex(ValueError, 'RPC peer'):
                daemon.verify(state)

    def test_backing_evidence_reports_real_inode_and_descriptor_flags(self):
        with tempfile.NamedTemporaryFile() as wal:
            row = daemon.backing_fds(os.getpid(), [{'name': 'wal',
                'driver_specific': {'aio': {'filename': wal.name}}}])[0]
            info = os.stat(wal.name)
            self.assertTrue(row['available'])
            self.assertEqual((row['device'], row['inode']), (info.st_dev, info.st_ino))
            self.assertTrue(any(item['fd'] == wal.fileno() and
                                item['flags'] & os.O_ACCMODE == os.O_RDWR
                                for item in row['descriptors']))

    def test_only_exact_graceful_systemd_exit_is_admitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            info = Path(temporary).stat()
            state = {'unit_name': 'owned', 'pid': 123, 'process_start_ticks': 456,
                     'state_directory_identity': [info.st_dev, info.st_ino]}
            valid = {'version': 1, 'unit_name': 'owned', 'pid': 123, 'process_start_ticks': 456,
                     'service_result': 'success', 'exit_code': 'exited', 'exit_status': '0'}
            def open_directory(path):
                return os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            with mock.patch.object(daemon.manage, 'trusted_directory', side_effect=open_directory):
                for changes in ({'unit_name': 'other'}, {'pid': 124}, {'process_start_ticks': 457},
                                {'service_result': 'timeout'}, {'exit_code': 'killed'},
                                {'exit_status': '9'}, {'exit_status': None}):
                    with self.subTest(changes=changes), \
                         mock.patch.object(daemon, '_read_private_json', return_value={**valid, **changes}):
                        with self.assertRaisesRegex(ValueError, 'did not prove graceful'):
                            daemon.admit_exit(temporary, state)
                with mock.patch.object(daemon, '_read_private_json', return_value=valid):
                    self.assertEqual(daemon.admit_exit(temporary, state), valid)

    def test_exit_recorder_is_bound_and_exclusive_without_state_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            info = path.stat()
            state = {'version': 1, 'kind': 'live-spdk-daemon', 'unit_name': 'owned',
                     'pid': 123, 'process_start_ticks': 456,
                     'state_directory_identity': [info.st_dev, info.st_ino]}
            def open_directory(path):
                return os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            environment = {'SERVICE_RESULT': 'timeout', 'EXIT_CODE': 'killed', 'EXIT_STATUS': '9'}
            with mock.patch.object(daemon.manage, 'trusted_directory', side_effect=open_directory), \
                 mock.patch.object(daemon, '_read_private_json', return_value=state), \
                 mock.patch.dict(os.environ, environment, clear=True):
                result = daemon.record_exit(str(path), 'owned')
                self.assertEqual((result['service_result'], result['exit_code'], result['exit_status']),
                                 ('timeout', 'killed', '9'))
                with self.assertRaises(FileExistsError):
                    daemon.record_exit(str(path), 'owned')
                with self.assertRaisesRegex(ValueError, 'identity is not bound'):
                    daemon.record_exit(str(path), 'other')

    def test_forced_exit_after_resource_cleanup_is_quarantined(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            info = path.stat()
            state = {'phase': 'ready', 'rpc_socket': '/run/test/rpc.sock',
                     'unit_name': 'owned', 'pid': 123, 'process_start_ticks': 456,
                     'state_directory_identity': [info.st_dev, info.st_ino]}
            directory = mock.Mock()
            receipt = {'version': 1, 'unit_name': 'owned', 'pid': 123, 'process_start_ticks': 456,
                       'service_result': 'timeout', 'exit_code': 'killed', 'exit_status': '9'}
            def open_directory(name):
                return os.open(name, os.O_RDONLY | os.O_DIRECTORY)
            with mock.patch.object(daemon.campaign, 'trusted_json',
                                   return_value={'complete': True, 'target': {'owned_namespace_count': 0}}), \
                 mock.patch.object(daemon.campaign, 'campaign_state', return_value=(directory, state)), \
                 mock.patch.object(daemon.manage, 'trusted_directory', side_effect=open_directory), \
                 mock.patch.object(daemon, '_read_private_json', return_value=receipt), \
                 mock.patch.object(daemon, 'verify'), \
                 mock.patch.object(daemon.spdk, 'rpc', side_effect=[[], []]), \
                 mock.patch.object(daemon.spdk, 'capture_journal'), \
                 mock.patch.object(daemon.manage, 'save'), \
                 mock.patch.object(daemon.spdk, 'stop_owned') as cleanup:
                with self.assertRaisesRegex(ValueError, 'did not prove graceful'):
                    daemon.stop(temporary, '/proof', True)
            cleanup.assert_called_once()
            self.assertEqual(state['phase'], 'quarantined')
            self.assertIn('timeout', state['cleanup_error'])

    def test_remaining_bdev_quarantines_without_stopping_daemon(self):
        state = {'phase': 'ready', 'rpc_socket': '/run/test/rpc.sock'}
        directory = mock.Mock()
        with mock.patch.object(daemon.campaign, 'trusted_json',
                               return_value={'complete': True, 'target': {'owned_namespace_count': 0}}), \
             mock.patch.object(daemon.campaign, 'campaign_state', return_value=(directory, state)), \
             mock.patch.object(daemon, 'verify'), \
             mock.patch.object(daemon.spdk, 'rpc', side_effect=[[{'name': 'wal'}], []]), \
             mock.patch.object(daemon.spdk, 'capture_journal'), \
             mock.patch.object(daemon.manage, 'save') as save, \
             mock.patch.object(daemon.spdk, 'stop_owned') as stop:
            with self.assertRaisesRegex(ValueError, 'still owns'):
                daemon.stop('/state', '/proof', True)
        self.assertEqual(state['phase'], 'quarantined')
        self.assertIn('still owns', state['cleanup_error'])
        save.assert_called()
        stop.assert_not_called()
