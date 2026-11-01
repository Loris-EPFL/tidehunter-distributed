import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nvme_campaign_trace as trace

class TrustedFile:
    """Use real file bytes with explicitly simulated trusted stat metadata."""
    def __init__(self, path):
        self.path = path
        self.name = path.name

    def __fspath__(self):
        return os.fspath(self.path)

    def __lt__(self, other):
        return self.name < other.name

    def lstat(self):
        st = self.path.lstat()
        return SimpleNamespace(st_mode=st.st_mode & ~0o022, st_uid=0, st_nlink=st.st_nlink, st_size=st.st_size)


class TrustedDirectory:
    def __init__(self, path):
        self.path = path

    def exists(self):
        return self.path.exists()

    def iterdir(self):
        return (TrustedFile(path) for path in self.path.iterdir())



class TraceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.work = self.base / 'mnt/tidehunter-kv-comparison/run/cells/case/node-0/nvmeof'
        self.work.mkdir(parents=True)
        self.config = {'prefix': 'run', 'bucket': 'bucket', 'bucket_region': 'us-east-1',
                       'account': '123', 'repository_root': '/repo',
                       'fleet': {'run_id': 'run', 'data_root': '/mnt/tidehunter-kv-comparison/run'}}
        self.request = {'action': 'nvme_trace_start', 'case_id': 'case', 'generation': 'g', 'index': 0,
                        'timeout': 30, 'duration_seconds': 60, 'controllers': []}
        self.item = {'export_uuid': [1] * 16, 'uuid': '01010101-0101-0101-0101-010101010101',
                     'ctrl_id': 3, 'manifest': '/manifest', 'image': '/image', 'device': {},
                     'block_device': 'nvme0n1', 'directory': 'export-000'}
        self.calls = []
        real_path = Path
        def mapped(value):
            p = real_path(value)
            return self.base / str(p).lstrip('/') if str(p).startswith('/mnt/tidehunter-kv-comparison') else p
        for p in (patch.object(trace, 'Path', side_effect=mapped),
                  patch.object(trace.owner, '_trusted_dir'),
                  patch.object(trace.owner, '_read_json', side_effect=lambda p: json.loads(real_path(p).read_text()))):
            p.start()
            self.addCleanup(p.stop)

    def command(self, argv, *, timeout):
        self.calls.append([str(x) for x in argv])
        if '--start' in argv:
            directory = Path(argv[-1])
            directory.mkdir()
            (directory / 'trace.txt').write_text('raw')
            return json.dumps({'available': True, 'pid': 12, 'start_monotonic_ns': 20})
        (Path(argv[-1]) / 'state.json').write_text(json.dumps({'kind': 'trace', 'phase': 'cleaned'}))
        return json.dumps({'available': True, 'overrun_or_dropped': 3, 'stages': {}})

    def run_action(self, exports):
        with patch.object(trace, '_exports', return_value=exports), \
             patch.object(trace, '_upload', return_value=([{'key': 'raw'}], [])), \
             patch.object(trace, '_upload_binding', return_value=([{'key': 'binding'}], [])):
            return trace.execute(self.config, self.request, {}, command=self.command)

    def test_gate_start_and_stop_preserve_loss_and_upload(self):
        started = self.run_action([self.item])
        self.assertTrue(started['complete'])
        self.assertIn('--start', self.calls[0])
        self.assertIn('nvme0n1', self.calls[0])
        self.request['action'] = 'nvme_trace_stop'
        stopped = self.run_action([self.item])
        self.assertTrue(stopped['complete'])
        self.assertTrue(stopped['cleanup_complete'])
        self.assertTrue(stopped['upload_complete'])
        self.assertEqual(stopped['traces'][0]['summary']['overrun_or_dropped'], 3)
        self.assertEqual(stopped['traces'][0]['files'], [{'key': 'raw'}])
        self.assertIn('--stop', self.calls[1])

    def test_exact_owner_prefix_and_denied_upload_is_incomplete(self):
        self.assertTrue(self.run_action([self.item])['complete'])
        self.request['action'] = 'nvme_trace_stop'
        real_upload = trace._upload
        def upload(directory, *args):
            return real_upload(TrustedDirectory(directory), *args)
        with patch.object(trace, '_exports', return_value=[self.item]), \
             patch.object(trace, '_upload', side_effect=upload), \
             patch.object(trace.subprocess, 'run'):
            result = trace.execute(self.config, self.request, {}, command=self.command)
        self.assertTrue(result['cleanup_complete'])
        self.assertTrue(result['upload_complete'])
        self.assertTrue(result['complete'])
        keys = [entry['key'] for entry in result['traces'][0]['files'] + result['binding_files']]
        prefix = 'run/results/cells/case/storage/0/nvmeof/trace-g/'
        self.assertIn(prefix + 'export-000/trace.txt', keys)
        self.assertIn(prefix + 'bindings.json', keys)
        self.assertTrue(all(key.startswith(prefix) for key in keys))
        with patch.object(trace, '_exports', return_value=[self.item]), \
             patch.object(trace, '_upload', side_effect=upload), \
             patch.object(trace.subprocess, 'run', side_effect=PermissionError('denied')):
            denied = trace.execute(self.config, self.request, {}, command=self.command)
        self.assertTrue(denied['cleanup_complete'])
        self.assertFalse(denied['upload_complete'])
        self.assertFalse(denied['complete'])
        self.assertFalse(denied['traces'][0]['files'])
        self.assertTrue(denied['traces'][0]['upload_errors'])

    def test_partial_start_stops_attempted_collectors(self):
        item2 = dict(self.item, directory='export-001', ctrl_id=4)
        original = self.command
        def failure(argv, *, timeout):
            if '--start' in argv and argv[-1].name == 'export-001':
                argv[-1].mkdir()
                raise RuntimeError('startup failed')
            return original(argv, timeout=timeout)
        self.command = failure
        result = self.run_action([self.item, item2])
        self.assertFalse(result['complete'])
        self.assertEqual(sum('--stop' in call for call in self.calls), 2)
        self.assertTrue(result['errors'])

    def test_empty_exports_and_mismatched_binding(self):
        self.assertTrue(self.run_action([])['complete'])
        self.request['action'] = 'nvme_trace_stop'
        empty = self.run_action([])
        self.assertTrue(empty['complete'])
        self.assertTrue(empty['cleanup_complete'])
        mismatch = self.run_action([self.item])
        self.assertFalse(mismatch['complete'])
        self.assertFalse(mismatch['cleanup_complete'])
        self.assertFalse(self.calls)

    def test_unavailable_start_with_confirmed_exit_is_safe_to_clean(self):
        def unsupported(argv, *, timeout):
            return json.dumps({'available': False, 'reason': 'collector exited before tracing started'})
        self.command = unsupported
        started = self.run_action([self.item])
        self.assertFalse(started['complete'])
        self.assertTrue(started['cleanup_complete'])
        self.request['action'] = 'nvme_trace_stop'
        stopped = self.run_action([self.item])
        self.assertFalse(stopped['complete'])
        self.assertTrue(stopped['cleanup_complete'])
        self.assertEqual(stopped['traces'][0]['cleanup'], 'exited_before_start')

    def test_unknown_collector_state_requires_quarantine(self):
        def unknown(argv, *, timeout):
            if '--start' in argv:
                raise TimeoutError('startup outcome unknown')
            raise AssertionError('no collector state to stop')
        self.command = unknown
        result = self.run_action([self.item])
        self.assertFalse(result['complete'])
        self.assertFalse(result['cleanup_complete'])

    def test_unavailable_summary_can_still_prove_cleanup(self):
        self.assertTrue(self.run_action([self.item])['complete'])
        self.request['action'] = 'nvme_trace_stop'
        def unavailable(argv, *, timeout):
            (Path(argv[-1]) / 'state.json').write_text(json.dumps({'kind': 'trace', 'phase': 'cleaned'}))
            return json.dumps({'available': False, 'reason': 'tracefs unavailable'})
        self.command = unavailable
        result = self.run_action([self.item])
        self.assertFalse(result['complete'])
        self.assertTrue(result['cleanup_complete'])

    def test_foreign_controller_rejected(self):
        export = self.work / 'export'
        target = self.work / 'target-state-g'
        export.mkdir(); target.mkdir()
        (export / 'ready.json').write_text('{"exports": []}')
        (target / 'state.json').write_text(json.dumps({'kind': 'campaign-target', 'phase': 'published',
                                                       'export_root': str(export), 'exports': []}))
        with self.assertRaises(ValueError):
            trace._exports(self.work, 'g', [{'export_uuid': [1] * 16, 'ctrl_id': 1}])

    def test_stalled_s3_worker_times_out_and_reports_loss(self):
        directory = self.work / 'trace-g/export-000'
        directory.mkdir(parents=True)
        (directory / 'trace.txt').write_bytes(b'raw')
        directory = TrustedDirectory(directory)
        def stalled(argv, **kwargs):
            self.assertEqual(kwargs['timeout'], 0.25)
            raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
        with patch.object(trace.subprocess, 'run', side_effect=stalled):
            files, errors = trace._upload(directory, self.config, {'s3': object()}, 'run/results/exact', lambda: 0.25)
        self.assertEqual(files, [])
        self.assertEqual(len(errors), 1)
        self.assertIn('TimeoutExpired', errors[0])


@unittest.skipUnless(os.geteuid() == 0, 'trusted owner fixtures require root')
class ExportBindingTests(unittest.TestCase):
    def setUp(self):
        # /run has trusted root-owned ancestors, unlike the world-writable /tmp.
        self.tmp = tempfile.TemporaryDirectory(dir='/run')
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        export = self.work / 'export'
        image_dir = export / 'part-0'
        image_dir.mkdir(parents=True)
        self.manifest = image_dir / 'manifest.json'
        self.image = image_dir / 'image.wal'
        self.image.write_bytes(b'0' * 512)
        self.data = {'version': 1, 'export_uuid': [1] * 16, 'nqn': 'nqn.2026-10.org.tidehunter:test',
                     'nsid': 1, 'image_len': 512, 'block_size': 512}
        self.manifest.write_text(json.dumps(self.data))
        ready = {'exports': [{'manifest_path': str(self.manifest),
                              **{key: self.data[key] for key in
                                 ('export_uuid', 'nqn', 'nsid', 'image_len', 'block_size')}}]}
        (export / 'ready.json').write_text(json.dumps(ready))
        state_dir = self.work / 'target-state-g'
        state_dir.mkdir()
        self.state_path = state_dir / 'state.json'
        self.target = {'manifest': str(self.manifest),
                       'manifest_sha256': hashlib.sha256(self.manifest.read_bytes()).hexdigest(),
                       'export_uuid': self.data['export_uuid'], 'nqn': self.data['nqn'], 'nsid': self.data['nsid'],
                       'uuid': '01010101-0101-0101-0101-010101010101', 'status': 'published',
                       'state': str(state_dir / 'export-000')}
        self.state = {'kind': 'campaign-target', 'phase': 'published',
                      'export_root': str(export), 'exports': [self.target]}
        self.state_path.write_text(json.dumps(self.state))
        self.controllers = [{'export_uuid': self.data['export_uuid'], 'ctrl_id': 7}]

    def test_published_export_resolves_exact_immutable_image(self):
        device = {'mount_device': '259:1', 'device': '259:0',
                  'sysfs_path': '/sys/devices/block/nvme0n1', 'partition': True}
        with patch.object(trace.cold_device_trace, 'resolve_device', return_value=device) as resolve:
            result = trace._exports(self.work, 'g', self.controllers)
        resolve.assert_called_once_with(self.image)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['manifest'], str(self.manifest))
        self.assertEqual(result[0]['manifest_sha256'], self.target['manifest_sha256'])
        self.assertEqual(result[0]['image'], str(self.image))
        self.assertEqual(result[0]['device'], device)
        self.assertEqual(result[0]['block_device'], 'nvme0n1')
        self.assertEqual(result[0]['ctrl_id'], 7)

    def test_tampered_target_manifest_is_rejected_before_device_resolution(self):
        self.target['manifest_sha256'] = '0' * 64
        self.state_path.write_text(json.dumps(self.state))
        with patch.object(trace.cold_device_trace, 'resolve_device') as resolve:
            with self.assertRaisesRegex(ValueError, 'target state and immutable export disagree'):
                trace._exports(self.work, 'g', self.controllers)
        resolve.assert_not_called()

    def test_equivalent_json_with_changed_bytes_is_rejected(self):
        self.manifest.write_text(json.dumps(self.data, indent=2))
        with patch.object(trace.cold_device_trace, 'resolve_device') as resolve:
            with self.assertRaisesRegex(ValueError, 'target state and immutable export disagree'):
                trace._exports(self.work, 'g', self.controllers)
        resolve.assert_not_called()


if __name__ == '__main__':
    unittest.main()
