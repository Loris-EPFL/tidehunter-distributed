import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nvme_campaign_owner as owner


class OwnerLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.node = self.base / 'mnt/tidehunter-kv-comparison/run/cells/case/node-0'
        (self.node / 'storage').mkdir(parents=True)
        (self.node / 'launch.yaml').write_text('listen: 10.0.0.1:9400\n')
        (self.node / 'launch.yaml').chmod(0o600)
        self.config = {'prefix': 'run', 'repository_root': '/repository', 'binary': '/binary',
                       'deadline_utc': '2099-01-01T00:00:00Z', 'fdb_library_dir': '/lib/fdb', 'fleet': {
                           'run_id': 'run', 'data_root': '/mnt/tidehunter-kv-comparison/run',
                           'nodes': [{'address': '10.0.0.1'}], 'server_cpus': 4, 'server_memory_mib': 2048}}
        self.calls = []
        self.status = {'ActiveState': 'active', 'SubState': 'running', 'MainPID': '123'}
        self.stopped = {'ActiveState': 'active', 'SubState': 'exited', 'MainPID': '0',
                        'Result': 'success', 'ExecMainStatus': '0'}
        class RootOwnedOS:
            def __getattr__(self, name):
                return getattr(os, name)

            @staticmethod
            def fstat(fd):
                st = os.fstat(fd)
                return SimpleNamespace(st_mode=st.st_mode, st_uid=0, st_size=st.st_size)

        def mapped_path(value):
            path = Path(value)
            if str(path).startswith('/mnt/tidehunter-kv-comparison'):
                return self.base / str(path).lstrip('/')
            return path
        for p in (patch.object(owner, 'Path', side_effect=mapped_path),
                  patch.object(owner, '_trusted_dir'), patch.object(owner, 'os', RootOwnedOS())):
            p.start()
            self.addCleanup(p.stop)

    def command(self, argv, *, timeout):
        self.calls.append(([str(x) for x in argv], timeout))
        return '{}'

    def run_action(self, action, **kwargs):
        return owner.execute(self.config, {'action': action, 'case_id': 'case', 'index': 0,
                                         'timeout': 10, **kwargs}, command=self.command,
                             validate_launch=lambda *args: None, unit_status=lambda name: self.status,
                             unit_name=lambda *args: 'owned-unit')

    def prove_stop(self):
        work = self.node / 'nvmeof'
        work.mkdir(mode=0o700, exist_ok=True)
        (work / 'stopped.json').write_text(json.dumps({'unit': 'owned-unit', 'status': self.stopped}))
        (work / 'stopped.json').chmod(0o600)
        self.status = self.stopped

    def test_graceful_stop_preserves_database_without_forced_stop(self):
        statuses = iter([self.status, self.stopped])
        result = owner.execute(self.config, {'action': 'nvme_owner_stop', 'case_id': 'case', 'index': 0, 'timeout': 10},
                               command=self.command, validate_launch=lambda *args: None,
                               unit_status=lambda name: next(statuses), unit_name=lambda *args: 'owned-unit')
        self.assertTrue(result['complete'])
        self.assertTrue((self.node / 'storage').is_dir())
        self.assertTrue((self.node / 'stop').exists())
        self.assertEqual(self.calls, [])
        self.assertEqual(json.loads((self.node / 'nvmeof/stopped.json').read_text())['status'], self.stopped)

    def test_failed_drain_never_forces_stop_or_records_success(self):
        failed = dict(self.stopped, Result='exit-code', ExecMainStatus='1')
        statuses = iter([self.status, failed])
        result = owner.execute(self.config, {'action': 'nvme_owner_stop', 'case_id': 'case', 'index': 0, 'timeout': 10},
                               command=self.command, validate_launch=lambda *args: None,
                               unit_status=lambda name: next(statuses), unit_name=lambda *args: 'owned-unit')
        self.assertFalse(result['complete'])
        self.assertTrue(result['stop_requested'])
        self.assertFalse((self.node / 'nvmeof/stopped.json').exists())
        self.assertEqual(self.calls, [])

    def test_export_uses_supplied_dataset_and_exact_owned_paths(self):
        self.prove_stop()
        def export(argv, *, timeout, env):
            self.assertEqual(env['FDB_CLIENT_LIB_DIR'], '/lib/fdb')
            self.calls.append(([str(x) for x in argv], timeout))
            destination = Path(argv[-1])
            destination.mkdir()
            manifest = destination / 'partition-0-file-0' / 'manifest.json'
            manifest.parent.mkdir()
            manifest.write_text('{\"nsid\":1}')
            (destination / 'ready.json').write_text(json.dumps({'members': 3, 'exports': [{'manifest_path': str(manifest)}]}))
            manifest.chmod(0o600)
            (destination / 'ready.json').chmod(0o600)
            return ''
        self.command = export
        prepared = {'keys': 3, 'entropy': 'random', 'database': [1], 'partitions': [0]}
        result = self.run_action('nvme_owner_export', prepared=prepared)
        self.assertTrue(result['complete'])
        self.assertEqual(result['ready']['members'], 3)
        entry = result['ready']['exports'][0]
        self.assertNotIn('manifest_data', entry)
        self.assertEqual(entry['manifest_sha256'], hashlib.sha256(b'{\"nsid\":1}').hexdigest())
        self.assertEqual(owner.read_export_manifest(self.config, 'case', 0, entry['manifest_path'],
                         validate_launch=lambda *args: None), b'{\"nsid\":1}')
        with self.assertRaises(ValueError):
            owner.read_export_manifest(self.config, 'case', 0, str(self.node / 'other/manifest.json'),
                                       validate_launch=lambda *args: None)
        args, timeout = self.calls[0]
        self.assertEqual(args, ['/binary', 'export-nvmeof-dataset', str(self.node / 'launch.yaml'),
                                str(self.node / 'nvmeof/prepared.json'), str(self.node / 'nvmeof/export')])
        self.assertGreater(timeout, 0)
        self.assertEqual(json.loads((self.node / 'nvmeof/prepared.json').read_text()), prepared)

    def test_cleanup_rejects_missing_drain_before_any_side_effect(self):
        result = self.run_action('nvme_owner_target_cleanup', generation='case1', readers_drained=False, host_disconnected={})
        self.assertFalse(result['complete'])
        self.assertEqual(self.calls, [])
        self.assertFalse((self.node / 'nvmeof/host-disconnected-case1.json').exists())

    def cleanup_export_fixture(self, phase='cleaned'):
        work = self.node / 'nvmeof'
        export = work / 'export'
        work.mkdir(mode=0o700)
        export.mkdir(mode=0o700)
        (export / 'ready.json').write_text('{\"exports\":[{}]}')
        (export / 'ready.json').chmod(0o600)
        (work / 'target-state-g1').mkdir()
        identity = {'uuid': 'uuid', 'nqn': 'nqn', 'hostnqn': 'host', 'hostid': 'id',
                    'address': '10.0.0.1', 'service': 4420}
        target = {'version': 1, 'kind': 'campaign-target', 'phase': phase, 'readers_drained': True,
                  'campaign_id': 'campaign', 'export_root': str(export),
                  'exports': [dict(identity, status='cleaned')]}
        proof = {'version': 1, 'kind': 'campaign-host', 'phase': 'disconnected', 'readers_drained': True,
                 'campaign_id': 'campaign', 'exports': [dict(identity, status='disconnected')]}
        (work / 'host-disconnected-g1.json').write_text(json.dumps(proof))
        (work / 'host-disconnected-g1.json').chmod(0o600)
        self.command = lambda argv, *, timeout: json.dumps(target)
        return export, work

    def test_cleanup_export_requires_matching_drain_and_keeps_receipts(self):
        export, work = self.cleanup_export_fixture()
        with patch.object(owner, '_remove_export') as remove:
            result = self.run_action('nvme_owner_cleanup_export')
        self.assertTrue(result['complete'])
        self.assertEqual(result['export_removed'], str(export))
        self.assertEqual(remove.call_args.args[0], export)
        self.assertTrue((work / 'target-state-g1').exists())
        self.assertTrue((work / 'host-disconnected-g1.json').exists())

    def test_cleanup_receipt_does_not_duplicate_catalog(self):
        _, work = self.cleanup_export_fixture()
        initial_command = self.command
        def status(argv, *, timeout):
            result = json.loads(initial_command(argv, timeout=timeout))
            result['exports'][0]['manifest_data'] = {'catalog': [{'value': 'x' * 128}] * 13000}
            return json.dumps(result)
        self.command = status
        with patch.object(owner, '_remove_export'):
            result = self.run_action('nvme_owner_cleanup_export')
        self.assertTrue(result['complete'])
        self.assertLess(len(json.dumps(result)), 4096)
        target = result['targets'][0]
        self.assertEqual(target['campaign_id'], 'campaign')
        self.assertEqual(target['target_state_path'], str(work / 'target-state-g1/state.json'))
        self.assertEqual(target['host_disconnect_path'], str(work / 'host-disconnected-g1.json'))
        self.assertEqual(target['exports'][0]['uuid'], 'uuid')

    def test_cleanup_export_refuses_uncleaned_or_missing_disconnect_proof(self):
        export, work = self.cleanup_export_fixture(phase='published')
        with patch.object(owner, '_remove_export') as remove:
            result = self.run_action('nvme_owner_cleanup_export')
            self.assertFalse(result['complete'])
            remove.assert_not_called()
            self.command = lambda argv, *, timeout: json.dumps({'phase': 'cleaned'})
            (work / 'host-disconnected-g1.json').unlink()
            result = self.run_action('nvme_owner_cleanup_export')
            self.assertFalse(result['complete'])
            remove.assert_not_called()
        self.assertTrue(export.exists())

    def test_cleanup_empty_export_requires_explicit_no_target_attestation(self):
        export, work = self.cleanup_export_fixture()
        (work / 'target-state-g1').rmdir()
        (export / 'ready.json').write_text('{\"exports\":[]}')
        with patch.object(owner, '_remove_export') as remove:
            result = self.run_action('nvme_owner_cleanup_export')
            self.assertFalse(result['complete'])
            remove.assert_not_called()
            result = self.run_action('nvme_owner_cleanup_export', no_targets_published=True)
            self.assertTrue(result['complete'])
            self.assertTrue(result['no_targets_published'])
            self.assertEqual(remove.call_args.args[0], export)

    def test_no_target_attestation_cannot_override_existing_state(self):
        self.cleanup_export_fixture(phase='published')
        with patch.object(owner, '_remove_export') as remove:
            result = self.run_action('nvme_owner_cleanup_export', no_targets_published=True)
            self.assertFalse(result['complete'])
            remove.assert_not_called()

    def test_export_cleanup_retries_after_ready_removed(self):
        export, work = self.cleanup_export_fixture()
        (work / 'target-state-g1').rmdir()
        (export / 'z-image').write_text('image')
        (export / 'z-image').chmod(0o600)
        original_scandir = os.scandir
        original_unlink = Path.unlink
        fail = [True]

        class Entry:
            def __init__(self, entry):
                self.entry = entry
                self.name = entry.name

            def __getattr__(self, name):
                return getattr(self.entry, name)

            def stat(self, **kwargs):
                st = self.entry.stat(**kwargs)
                return SimpleNamespace(st_mode=st.st_mode, st_uid=0)

        class Scan:
            def __init__(self, path):
                with original_scandir(path) as entries:
                    self.entries = iter([Entry(e) for e in sorted(entries, key=lambda e: e.name)])

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.entries)

            def close(self):
                pass

        def unlink(path, *args, **kwargs):
            if path.name == 'z-image' and fail[0]:
                fail[0] = False
                raise OSError('injected partial deletion')
            return original_unlink(path, *args, **kwargs)

        with patch.object(owner.os, 'scandir', side_effect=Scan), \
             patch.object(Path, 'unlink', autospec=True, side_effect=unlink):
            first = self.run_action('nvme_owner_cleanup_export', no_targets_published=True)
            self.assertFalse(first['complete'])
            self.assertTrue(first['export_cleanup_attempted'])
            self.assertFalse((export / 'ready.json').exists())
            self.assertTrue((work / 'export-cleanup-ready.json').exists())
            second = self.run_action('nvme_owner_cleanup_export', no_targets_published=True)
            self.assertTrue(second['complete'])
            self.assertFalse(export.exists())

    def test_partial_target_setup_is_reported_not_cleaned(self):
        self.prove_stop()
        self.status = {'ActiveState': 'active', 'SubState': 'running', 'MainPID': '123'}
        export = self.node / 'nvmeof/export'
        export.mkdir()
        (export / 'ready.json').write_text('{\"exports\":[]}')
        (export / 'ready.json').chmod(0o600)
        def setup(argv, *, timeout):
            self.calls.append(([str(x) for x in argv], timeout))
            if 'target-setup' in argv:
                (self.node / 'nvmeof/target-state-case1').mkdir()
                raise RuntimeError('setup failed after first export')
            return json.dumps({'phase': 'creating', 'exports': [{'status': 'published'}]})
        self.command = setup
        result = self.run_action('nvme_owner_target_setup', generation='case1', address='10.0.0.1',
                                 base_port_id=1, base_service=4420, transport='tcp')
        self.assertFalse(result['complete'])
        self.assertEqual(result['target']['exports'][0]['status'], 'published')
        self.assertTrue((self.node / 'nvmeof/target-state-case1').exists())
        self.assertFalse(any('target-cleanup' in args for args, _ in self.calls))

    def test_restart_refuses_live_target(self):
        self.prove_stop()
        (self.node / 'nvmeof/target-state-case1').mkdir()
        self.command = lambda argv, *, timeout: json.dumps({'phase': 'published'})
        result = self.run_action('nvme_owner_restart', runtime_seconds=3600)
        self.assertFalse(result['complete'])
        self.assertIn('readers drain', result['error'])
        self.assertTrue((self.node / 'nvmeof/stopped.json').exists())

    def test_restart_uses_runtime_not_action_timeout(self):
        self.prove_stop()
        (self.node / 'stop').touch()
        def start(argv, *, timeout):
            self.calls.append(([str(x) for x in argv], timeout))
            if argv[0] == 'systemd-run':
                self.status = {'ActiveState': 'active', 'SubState': 'running', 'MainPID': '456'}
                (self.node / 'ready').write_text('10.0.0.1:9400')
            return ''
        self.command = start
        result = self.run_action('nvme_owner_restart', runtime_seconds=3600)
        self.assertTrue(result['complete'])
        args = self.calls[-1][0]
        self.assertIn('--property=RuntimeMaxSec=3600', args)
        self.assertFalse((self.node / 'stop').exists())

    def test_restart_after_stopped_transient_unit_is_unloaded(self):
        self.prove_stop()
        def start(argv, *, timeout):
            self.calls.append(([str(x) for x in argv], timeout))
            if argv[:2] == ['systemctl', 'stop']:
                self.status = {'LoadState': 'not-found'}
            elif argv[0] == 'systemd-run':
                self.status = {'ActiveState': 'active', 'SubState': 'running', 'MainPID': '456'}
                (self.node / 'ready').write_text('10.0.0.1:9400')
            return ''
        self.command = start
        result = self.run_action('nvme_owner_restart', runtime_seconds=3600)
        self.assertTrue(result['complete'])
        self.assertFalse(any(args[:2] == ['systemctl', 'reset-failed'] for args, _ in self.calls))
        self.assertTrue(any(args[0] == 'systemd-run' for args, _ in self.calls))

    def test_rejects_case_traversal(self):
        with self.assertRaises(ValueError):
            owner.execute(self.config, {'action': 'nvme_owner_export', 'case_id': '../other', 'index': 0, 'timeout': 10},
                          command=self.command, validate_launch=lambda *args: None,
                          unit_status=lambda name: self.status, unit_name=lambda *args: 'owned-unit')

    def test_rejects_generation_traversal_on_non_target_actions(self):
        for action in ('nvme_owner_export', 'nvme_owner_restart', 'nvme_owner_cleanup_export'):
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.run_action(action, generation='x/../../outside')
        self.assertEqual(self.calls, [])
        self.assertFalse((self.node / 'nvmeof').exists())


if __name__ == '__main__':
    unittest.main()
