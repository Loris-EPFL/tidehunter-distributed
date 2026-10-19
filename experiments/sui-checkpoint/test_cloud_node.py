"""Local-only ownership/deadline regression tests; no AWS or real storage launch."""
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import cloud_node


# A bounded fixture executable exercises real process-group cleanup. Its hash is
# explicitly supplied by these tests; production only accepts the frozen binary.
CHILD = '''
import json
from pathlib import Path
import signal
import sys
import time
launch = json.loads(Path(sys.argv[2]).read_text())
root = Path(launch['node']['root'])
root.mkdir()
print('fixture stdout', flush=True)
print('fixture stderr', file=sys.stderr, flush=True)
if launch.get('test_mode') == 'fail':
    raise SystemExit(17)
if launch.get('test_mode') == 'ignore_term':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(launch['ready_file']).write_text('ready')
end = time.monotonic() + 10
while time.monotonic() < end:
    if Path(launch['stop_file']).exists() and launch.get('test_mode') != 'ignore_term':
        break
    time.sleep(0.01)
'''


class CloudNodeContract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name) / 'owned'
        patcher = patch.object(cloud_node, 'OWNED_BASE', self.base)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.run_id = 'th-0123456789abcdef'
        self.root = self.base / self.run_id / 'checkpoint-cells/r04-tcp/storage'
        self.root.mkdir(parents=True)
        self.binary = self.root / 'storage-node'
        self.binary.write_text('#!' + sys.executable + '\n' + CHILD)
        self.binary.chmod(0o700)
        tls = self.root / 'tls'
        tls.mkdir()
        for name in ('ca.der', 'node.der', 'node.pk8'):
            (tls / name).write_bytes(b'fixture identity; never logged')
        self.config_path = self.root / 'node-config.json'
        self.cfg = dict(run_id=self.run_id, cell_id='r04-tcp', transport='tcp', root=str(self.root),
            deadline_utc=datetime.fromtimestamp(time.time() + 60, timezone.utc).isoformat(),
            storage_binary_sha256=cloud_node.sha256(self.binary),
            build_manifest_sha256='b' * 64, config_sha256='c' * 64,
            checkpoint_digest='EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6',
            commands=[], ready_files=[], stop_files=[],
            resource_path=str(self.root / 'resources.json'),
            ready_path=str(self.root / 'ready'), stop_path=str(self.root / 'stop'))
        self.add_namespace()

    def add_namespace(self, mode='normal'):
        directory = self.root / f"namespace-{len(self.cfg['commands'])}" / 'node-0'
        directory.mkdir(parents=True)
        launch = directory / 'launch.yaml'
        value = dict(node=dict(root=str(directory / 'storage')),
            tls=dict(transport='tcp', ca=str(self.root / 'tls/ca.der'), cert=str(self.root / 'tls/node.der'),
                     key=str(self.root / 'tls/node.pk8')),
            ready_file=str(directory / 'ready'), stop_file=str(directory / 'stop'), test_mode=mode)
        launch.write_text(json.dumps(value))
        self.cfg['commands'].append([str(self.binary), '--experimental-storage-node', str(launch)])
        self.cfg['ready_files'].append(value['ready_file'])
        self.cfg['stop_files'].append(value['stop_file'])

    def validate(self):
        self.config_path.write_text(json.dumps(self.cfg))
        return cloud_node.validate_config(self.config_path)

    def owner(self):
        return cloud_node.NodeOwner(self.validate())

    def test_valid_fresh_payload(self):
        cfg = self.validate()
        self.assertEqual(len(cfg['namespaces']), 1)
        self.assertEqual(cfg['storage_binary_sha256'], cloud_node.sha256(self.binary))
        self.assertEqual(cfg['cell_id'], 'r04-tcp')

    def test_direct_identity_accepts_only_exact_direct_launch(self):
        old_root = self.root
        self.root = old_root.parent.parent / 'r04-efa-direct/storage'
        self.root.parent.mkdir()
        old_root.rename(self.root)
        self.config_path = self.root / 'node-config.json'
        self.binary = self.root / 'storage-node'
        self.cfg = json.loads(json.dumps(self.cfg).replace(str(old_root), str(self.root))
                              .replace('"r04-tcp"', '"r04-efa-direct"').replace('"tcp"', '"efa-direct"'))
        launch = Path(self.cfg['commands'][0][2])
        value = json.loads(launch.read_text().replace(str(old_root), str(self.root)))
        value['tls']['transport'] = 'efa-direct'
        launch.write_text(json.dumps(value))
        self.assertEqual(self.validate()['transport'], 'efa-direct')
        for fallback in ('efa', 'tcp', 'fabric-tcp'):
            value['tls']['transport'] = fallback
            launch.write_text(json.dumps(value))
            with self.subTest(fallback=fallback), self.assertRaisesRegex(ValueError, 'transport differs'):
                self.validate()

    def test_requires_exact_owned_root(self):
        self.cfg['root'] = str(self.root.parent)
        with self.assertRaises(ValueError):
            self.validate()


    def test_rejects_another_cell_or_transport(self):
        for field, value in (('cell_id', 'r02-tcp'), ('transport', 'efa')):
            with self.subTest(field=field):
                original = self.cfg[field]
                self.cfg[field] = value
                with self.assertRaises(ValueError):
                    self.validate()
                self.cfg[field] = original

    def test_rejects_out_of_bounds_or_malformed_cells(self):
        for cell_id in ('r00-tcp', 'r05-tcp', 'r99-tcp', 'r4-tcp', 'r004-tcp',
                        'r04-tcp-extra', 'r04-tcp\n', None, 4):
            with self.subTest(cell_id=cell_id):
                self.cfg['cell_id'] = cell_id
                with self.assertRaisesRegex(ValueError, 'invalid matrix cell'):
                    self.validate()

    def test_rejects_transport_fallback_in_launch(self):
        launch = Path(self.cfg['commands'][0][2])
        value = json.loads(launch.read_text())
        value['tls']['transport'] = 'fabric-tcp'
        launch.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'transport differs'):
            self.validate()
    def test_rejects_outside_output(self):
        self.cfg['resource_path'] = str(self.base / 'someone-else/resources.json')
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_symlinked_launch_ancestor(self):
        directory = Path(self.cfg['commands'][0][2]).parent
        elsewhere = self.root / 'other'
        directory.rename(elsewhere)
        directory.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_output_aliasing_binary(self):
        self.cfg['resource_path'] = str(self.binary)
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_reused_storage_and_ready(self):
        launch = Path(self.cfg['commands'][0][2])
        for path in (launch.parent / 'storage', Path(self.cfg['ready_files'][0])):
            with self.subTest(path=path):
                path.mkdir()
                with self.assertRaises(ValueError):
                    self.validate()
                path.rmdir()

    def test_rejects_launch_stop_mismatch(self):
        self.cfg['stop_files'][0] = str(self.root / 'different-stop')
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_unsafe_commands_and_wrong_binary_hash(self):
        original = list(self.cfg['commands'][0])
        for command in ([str(self.binary), '--help'], ['/bin/sh', '-c', 'true'],
                        original + ['--another-option']):
            with self.subTest(command=command):
                self.cfg['commands'][0] = command
                with self.assertRaises(ValueError):
                    self.validate()
        self.cfg['commands'][0] = original
        self.cfg['storage_binary_sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_more_than_sixteen_namespaces(self):
        self.cfg['commands'] *= 17
        self.cfg['ready_files'] *= 17
        self.cfg['stop_files'] *= 17
        with self.assertRaises(ValueError):
            self.validate()

    def test_rejects_expired_unzoned_and_unbounded_deadlines(self):
        now = time.time()
        values = [datetime.fromtimestamp(now + delta, timezone.utc).isoformat()
                  for delta in (-1, 2, 3700)]
        values.append(datetime.fromtimestamp(now + 60).isoformat())
        for value in values:
            with self.subTest(deadline=value):
                self.cfg['deadline_utc'] = value
                with self.assertRaises(ValueError):
                    self.validate()

    def test_duplicate_owner_cannot_overwrite_live_receipt(self):
        owner = self.owner()
        receipt = Path(owner.cfg['receipt_path'])
        resource = Path(owner.cfg['resource_path'])
        receipt.write_text('another owner receipt')
        resource.write_text('another owner samples')
        self.assertEqual(owner.run(), 1)
        self.assertEqual(receipt.read_text(), 'another owner receipt')
        self.assertEqual(resource.read_text(), 'another owner samples')
        self.assertEqual(owner.children, [])

    def test_expiry_during_preflight_prevents_start(self):
        owner = self.owner()
        owner.cfg['deadline_monotonic'] = time.monotonic()
        with patch.object(cloud_node.subprocess, 'Popen') as start:
            self.assertEqual(owner.run(), 1)
        start.assert_not_called()
        result = json.loads(Path(owner.cfg['resource_path']).read_text())
        self.assertFalse(result['complete'])
        self.assertIn('deadline', result['failure'])

    def test_child_failure_stops_other_owned_group_not_unrelated_process(self):
        self.add_namespace('fail')
        owner = self.owner()
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'],
                                     start_new_session=True)
        try:
            self.assertEqual(owner.run(), 1)
            self.assertIsNone(unrelated.poll())
            self.assertTrue(owner.cleanup_complete)
            self.assertTrue(all(record['reaped'] for record in owner.children))
            self.assertTrue(any(record['exit_code'] == 17 for record in owner.children))
            result = json.loads(Path(owner.cfg['resource_path']).read_text())
            self.assertFalse(result['complete'])
            self.assertFalse(result['all_namespaces_healthy'])
            self.assertTrue(all(row['exited'] for row in result['samples'][-1]['processes']))
            for record in owner.children:
                self.assertTrue(Path(record['stdout']).is_file())
                self.assertTrue(Path(record['stderr']).is_file())
        finally:
            unrelated.terminate()
            unrelated.wait(timeout=2)

    def test_live_samples_and_normal_controller_shutdown(self):
        owner = self.owner()
        live = []
        errors = []

        def controller():
            try:
                end = time.monotonic() + 3
                while time.monotonic() < end:
                    if Path(owner.cfg['ready_path']).is_file():
                        live.append(json.loads(Path(owner.cfg['resource_path']).read_text()))
                        Path(owner.cfg['stop_path']).touch()
                        return
                    time.sleep(0.01)
                errors.append('node readiness timed out')
                owner.received_signal = signal.SIGTERM
            except Exception as error:
                errors.append(repr(error))
                owner.received_signal = signal.SIGTERM

        controller_thread = threading.Thread(target=controller)
        controller_thread.start()
        try:
            self.assertEqual(owner.run(), 0)
        finally:
            controller_thread.join(timeout=4)
        self.assertFalse(controller_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(live[0]['healthy'])
        self.assertFalse(live[0]['complete'])
        process = live[0]['samples'][-1]['processes'][0]
        self.assertFalse(process['exited'])
        self.assertEqual(process['unavailable'], {})
        for key in ('VmRSS', 'VmSwap', 'io', 'cpu', 'starttime_ticks'):
            self.assertIn(key, process)
        result = json.loads(Path(owner.cfg['resource_path']).read_text())
        self.assertTrue(result['complete'])
        self.assertTrue(result['all_namespaces_healthy'])
        self.assertFalse(result['healthy'])
        self.assertEqual(result['stop_reason'], 'controller_stop')
        final_process = result['samples'][-1]['processes'][0]
        self.assertTrue(final_process['exited'])
        self.assertNotIn('io', final_process)
        self.assertEqual(final_process['unavailable']['process'], 'exited_and_reaped')
        self.assertLessEqual(len(result['samples']), cloud_node.MAX_SAMPLES)

    def test_final_receipt_failure_cannot_publish_complete_resources(self):
        owner = self.owner()
        real_atomic_json = cloud_node.atomic_json

        def publish(path, value):
            if path == owner.cfg['receipt_path'] and value['complete']:
                raise OSError('injected final receipt failure')
            real_atomic_json(path, value)
            if path == owner.cfg['ready_path']:
                Path(owner.cfg['stop_path']).touch()

        with patch.object(cloud_node, 'atomic_json', side_effect=publish):
            self.assertEqual(owner.run(), 1)
        self.assertTrue(owner.cleanup_complete)
        self.assertIn('final evidence write failed', owner.failure)
        self.assertFalse(json.loads(Path(owner.cfg['resource_path']).read_text())['complete'])
        self.assertFalse(json.loads(Path(owner.cfg['receipt_path']).read_text())['complete'])

    def test_watchdog_kills_even_without_sampler_progress(self):
        owner = self.owner()
        with patch.object(owner, 'remaining', side_effect=[2.0, 0.5]), \
                patch.object(owner.watchdog_stop, 'wait') as pause, \
                patch.object(owner, 'signal_groups') as stop:
            owner.watchdog()
        self.assertTrue(owner.watchdog_expired)
        stop.assert_called_once_with(signal.SIGKILL)
        pause.assert_called_once_with(cloud_node.POLL_SECONDS)

    def test_cleanup_kills_term_ignoring_child_with_deadline_reserve(self):
        self.add_namespace('ignore_term')
        owner = self.owner()
        try:
            owner.launch()
        finally:
            owner.cfg['deadline_unix'] = time.time() + 0.5
            owner.cfg['deadline_monotonic'] = time.monotonic() + 0.5
            owner.cleanup()
        self.assertTrue(owner.cleanup_complete)
        self.assertEqual(owner.children[-1]['exit_code'], -signal.SIGKILL)

    def test_reaped_pid_never_reads_replacement_proc_counters(self):
        child = subprocess.Popen([sys.executable, '-c', 'pass'], start_new_session=True)
        child.wait(timeout=2)
        record = dict(index=0, child=child, reaped=True, starttime_ticks=1)
        with patch.object(cloud_node, 'proc_stat') as read_stat:
            sample = cloud_node.sample_process(record)
        read_stat.assert_not_called()
        self.assertTrue(sample['exited'])
        self.assertNotIn('VmRSS', sample)
        self.assertNotIn('io', sample)


if __name__ == '__main__':
    unittest.main()
