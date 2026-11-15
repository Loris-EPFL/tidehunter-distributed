import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import cloud_controller as controller
import owner_ftrace as sampler


def stat(name, ticks=10, started=100):
    fields = ['S'] + ['0'] * 19
    fields[11], fields[12], fields[19] = str(ticks), '0', str(started)
    return '1 (' + name + ') ' + ' '.join(fields)


def snapshot(ticks=10, started=100, other=False):
    threads = {'1': {'name': 'wal-write-owner', 'cpu_ticks': ticks, 'start_ticks': 100}}
    if other:
        threads['2'] = {'name': 'new-thread', 'cpu_ticks': 0, 'start_ticks': 200}
    return {'process_start_ticks': started, 'threads': threads}


class ThreadCpuTests(unittest.TestCase):
    def test_stat_names_and_truncated_thread_classes(self):
        parsed = sampler.parse_thread_stat(stat('a name (with) spaces', 17, 200))
        self.assertEqual(parsed, {'name': 'a name (with) spaces', 'cpu_ticks': 17, 'start_ticks': 200})
        for name, expected in (('wal-write-owner', 'wal_write_owner'), ('value-wal-tcp', 'value_wal_tcp'),
                               ('value-wal-prepare-1', 'value_wal_prepare'), ('value-wal-prepa', 'value_wal_prepare'),
                               ('wal-live-read-0', 'wal_live_read'), ('tokio-worker', 'other')):
            self.assertEqual(sampler.thread_class(name), expected)
        with self.assertRaises(ValueError):
            sampler.parse_thread_stat('broken')

    def test_task_exit_during_scan_does_not_discard_stable_threads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / '42'
            for tid in ('17', '18'):
                task = root / 'task' / tid
                task.mkdir(parents=True)
                (task / 'stat').write_text(stat('worker'))
            (root / 'stat').write_text(stat('process'))
            real_read = Path.read_text
            def read(path, *args, **kwargs):
                if path == root / 'task/18/stat':
                    raise ProcessLookupError('task exited')
                return real_read(path, *args, **kwargs)
            with patch.object(Path, 'read_text', read):
                self.assertEqual(set(sampler.thread_snapshot(42, Path(tmp))['threads']), {'17'})

    def test_thread_population_and_proc_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / '42'
            task = root / 'task/17'
            task.mkdir(parents=True)
            (root / 'stat').write_text(stat('process', started=90))
            (task / 'stat').write_text(stat('value-wal-tcp', ticks=20))
            result = sampler.thread_snapshot(42, Path(tmp))
            self.assertEqual(result['process_start_ticks'], 90)
            self.assertEqual(result['threads']['17']['cpu_ticks'], 20)
            with patch.object(sampler, 'THREAD_MAX_THREADS', 0), self.assertRaises(ValueError):
                sampler.thread_snapshot(42, Path(tmp))

    def test_group_percentages_and_busiest_other_thread(self):
        totals = {1: {'name': 'value-wal-prepa', 'cpu_ticks': 50},
                  2: {'name': 'value-wal-prepa', 'cpu_ticks': 100},
                  3: {'name': 'other-one', 'cpu_ticks': 90},
                  4: {'name': 'other-two', 'cpu_ticks': 10}}
        result = sampler.thread_summary(totals, set(), 2, 100)
        self.assertEqual(result['value_wal_prepare']['max_busy_pct'], 50)
        self.assertEqual(result['value_wal_prepare']['mean_busy_pct'], 37.5)
        self.assertEqual(result['other']['busiest_name'], 'other-one')
        self.assertEqual(result['other']['mean_busy_pct'], 25)
        self.assertFalse(result['wal_write_owner']['available'])
        result = sampler.thread_summary(totals, {'value_wal_prepare'}, 2, 100)
        self.assertIsNone(result['value_wal_prepare']['max_busy_pct'])
        self.assertTrue(result['other']['available'])

    def sample_pair(self, first, last, **patches):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'thread-cpu.stop').touch()
            with patch.object(sampler, 'thread_snapshot', side_effect=[first, last]), \
                    patch.object(sampler.time, 'monotonic', side_effect=[0, 1]), \
                    patch.object(sampler.os, 'sysconf', return_value=100):
                if patches:
                    with patch.multiple(sampler, **patches):
                        sampler.sample_threads(root, 42)
                else:
                    sampler.sample_threads(root, 42)
            return json.loads((root / 'thread-cpu.json').read_text())

    def test_sample_final_partial_and_churning_class_unavailable(self):
        result = self.sample_pair(snapshot(10), snapshot(60, other=True))
        self.assertTrue(result['available'])
        self.assertEqual(result['summary']['wal_write_owner']['max_busy_pct'], 50)
        self.assertFalse(result['summary']['other']['available'])
        self.assertEqual(len(result['samples']), 1)

    def test_counter_reset_process_reuse_and_size_bound_fail_closed(self):
        for last in (snapshot(9), snapshot(20, started=101)):
            self.assertFalse(self.sample_pair(snapshot(), last)['available'])
        result = self.sample_pair(snapshot(), snapshot(20), CPU_MAX_BYTES=10)
        self.assertFalse(result['available'])
        self.assertTrue(result['truncated'])
        self.assertFalse(sampler.start_threads('/unused', True)['available'])

    def test_sampler_survives_start_operation_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = ('import json,sys,owner_ftrace; '
                      'print(json.dumps(owner_ftrace.start_threads(sys.argv[1], int(sys.argv[2]), 5)))')
            result = subprocess.run([sys.executable, '-B', '-c', script, tmp, str(os.getpid())],
                                    capture_output=True, text=True, timeout=10, check=True,
                                    cwd=Path(__file__).resolve().parent)
            state = json.loads(result.stdout)
            self.assertEqual(state['directory'], tmp)
            artifact = sampler.stop_threads(state)
            self.assertTrue(artifact['available'], artifact)
            self.assertEqual(artifact['pid'], os.getpid())
            self.assertTrue(artifact['summary']['other']['available'])

    def test_deadline_expiry_is_not_a_successful_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            sampler.sample_threads(tmp, os.getpid(), duration_seconds=0.01)
            artifact = json.loads((Path(tmp) / 'thread-cpu.json').read_text())
            self.assertFalse(artifact['available'])
            self.assertTrue(artifact['truncated'])

    def test_guest_start_stop_bind_exact_workload_units_and_persist_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = {'prefix': 'run', 'deadline_utc': '2030-01-01T00:00:00Z',
                      'fleet': {'data_root': tmp, 'nodes': [{}]}}
            for index, role in ((-1, 'client'), (0, 'storage-0')):
                state = {'directory': tmp}
                artifact = {'available': True, 'pid': 42, 'samples': [], 'summary': {}}
                with patch.object(controller, 'verify_local_identity', return_value={}), \
                        patch.object(controller, 'guarded', side_effect=lambda value, *a, **k: Path(value)), \
                        patch.object(controller, 'unit_status', return_value={'MainPID': '42'}) as status, \
                        patch.object(sampler, 'start_threads', return_value=state) as start, \
                        patch.object(sampler, 'stop_threads', return_value=artifact) as stop:
                    request = {'index': index, 'case_id': 'case', 'action': 'thread_cpu_start'}
                    self.assertEqual(controller.guest_operation(config, request, {}), state)
                    self.assertEqual(status.call_args.args[0], controller.unit_name(config, 'case', role))
                    self.assertEqual(start.call_args.args[1], 42)
                    request['action'] = 'thread_cpu_stop'
                    self.assertEqual(controller.guest_operation(config, request, {}), artifact)
                    stop.assert_called_once_with(state)
            artifact = controller.store_thread_cpu(tmp, [{'available': True}, {'available': False}, {'inactive': True}])
            self.assertTrue(artifact['coordinator']['available'])
            self.assertEqual(len(artifact['owners']), 1)
            self.assertEqual(json.loads((Path(tmp) / 'thread-cpu.json').read_text()), artifact)
