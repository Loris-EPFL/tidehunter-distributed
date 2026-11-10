import tempfile
import socket
from pathlib import Path
import unittest
from unittest.mock import patch

import local_dry_run_valkey as dry


class ValkeyLifecycleTests(unittest.TestCase):
    def test_directory_requires_fresh_absolute_path(self):
        with self.assertRaises(ValueError):
            dry._directory('relative')
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent) / 'fixture'
            self.assertEqual(dry._directory(target), target.resolve())
            with self.assertRaises(FileExistsError):
                dry._directory(target)

    def test_preflight_rejects_live_listener_and_accepts_reusable_time_wait(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            endpoint = listener.getsockname()
            with self.assertRaises(OSError):
                dry._free_ports([endpoint])
            with socket.create_connection(endpoint, timeout=5) as client:
                server, _ = listener.accept()
                server.close()
                self.assertEqual(client.recv(1), b'')
        dry._free_ports([endpoint])

    def test_service_command_uses_only_supplied_root_wrapper_and_restores(self):
        calls = []
        def command(argv, timeout=30):
            calls.append((argv, timeout))
            return 'ok'
        with patch.object(dry.valkey_service, 'command', command):
            with dry._root_service():
                self.assertEqual(dry.valkey_service.command(['systemctl', 'stop', 'owned'], timeout=7), 'ok')
            self.assertIs(dry.valkey_service.command, command)
        self.assertEqual(calls, [(dry.local_dry_run_targets.ROOT + ['systemctl', 'stop', 'owned'], 7)])

    def test_close_attempts_every_exact_unit_and_retains_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = dry._Processes.__new__(dry._Processes)
            fixture.directory = Path(directory)
            fixture.closed = False
            a = dry._Unit({'unit': 'owned-a'})
            b = dry._Unit({'unit': 'owned-b'})
            a.pid, b.pid = 1, 2
            fixture.processes = [('a', a), ('b', b)]
            calls = []
            def stop(config):
                unit = config['native_valkey']['unit']
                calls.append(unit)
                if unit == 'owned-b':
                    raise RuntimeError('identity mismatch')
                return [{'unit': unit}]
            with patch.object(dry.valkey_service, 'stop', side_effect=stop), \
                 patch.object(dry.valkey_service, 'status', return_value={'LoadState': 'not-found'}):
                with self.assertRaisesRegex(RuntimeError, 'identity mismatch'):
                    fixture.close()
            self.assertEqual(calls, ['owned-b', 'owned-a'])
            self.assertTrue((Path(directory) / 'cleanup.json').is_file())
            self.assertFalse(fixture.closed)

    def test_ambiguous_start_does_not_certify_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = dry._Processes.__new__(dry._Processes)
            fixture.directory = Path(directory)
            fixture.closed = False
            fixture.processes = [('server', dry._Unit({'unit': 'owned'}))]
            with patch.object(dry.valkey_service, 'stop', return_value=[]), \
                 patch.object(dry.valkey_service, 'status', return_value={'LoadState': 'not-found'}):
                with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
                    fixture.close()

    def test_monitor_selection_is_exact_admitted_cgroups(self):
        fixture = dry.Cluster.__new__(dry.Cluster)
        fixture.processes = [('node-0', object()), ('node-1', object())]
        fixture.controls = ['one', 'two']
        with patch.object(dry, '_owned_endpoint', side_effect=[{'cgroup': '/system.slice/a.service'},
                                                               {'cgroup': '/system.slice/b.service'}]):
            self.assertEqual(fixture.monitor_processes(), [
                {'role': 'valkey0', 'cgroup': '/system.slice/a.service'},
                {'role': 'valkey1', 'cgroup': '/system.slice/b.service'}])

    def test_invalid_native_scale_fails_before_creating_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent) / 'fixture'
            for fraction in (0, -1, float('nan'), 1.1):
                with self.assertRaises(ValueError):
                    dry.run_native_controls(target, parent, fraction=fraction)
            self.assertFalse(target.exists())

    def test_native_failures_continue_and_keep_full_key_ranges(self):
        spec = dry.valkey_reproduction.load()
        original_ranges = {name: dict(profile['key_ranges']) for name, profile in spec['profiles'].items()}
        plans = []
        def fail_plan(scaled, *args, **kwargs):
            plans.append((scaled['requests_per_test'], {
                name: dict(profile['key_ranges']) for name, profile in scaled['profiles'].items()}))
            raise RuntimeError('planned failure')
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent) / 'controls'
            with patch.object(dry.valkey_cloud, 'tools', return_value={}), \
                 patch.object(dry, '_free_ports'), \
                 patch.object(dry.valkey_reproduction, 'plan', side_effect=fail_plan):
                report = dry.run_native_controls(target, parent, fraction=0.001)
            self.assertFalse(report['complete'])
            self.assertEqual(len(report['cases']), 4)
            self.assertEqual(len(plans), 4)
            for requests, ranges in plans:
                self.assertEqual(requests, 10000)
                self.assertEqual(ranges, original_ranges)
            for case in report['cases']:
                self.assertIn('planned failure', case['error'])
                self.assertTrue(case['owned_server_stopped'])
                self.assertTrue((target / (case['profile'] + '-' + case['transport']) / 'receipt.json').is_file())


if __name__ == '__main__':
    unittest.main()
