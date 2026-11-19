import io
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import owner_ftrace


class OwnerFtraceTests(unittest.TestCase):
    def test_cpu_stat_excludes_guest_and_rejects_counter_resets(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'stat'
            path.write_text('cpu 1 2 3 4 5 6 7 8\ncpu0 10 2 3 4 5 6 7 8 9 10\nintr 9\n')
            self.assertEqual(owner_ftrace.cpu_ticks(path), {'cpu0': [10, 2, 3, 4, 5, 6, 7, 8]})
        with self.assertRaises(ValueError):
            owner_ftrace.cpu_deltas({'cpu0': [1] * 8}, {'cpu0': [0] * 8})
        with self.assertRaises(ValueError):
            owner_ftrace.cpu_deltas({'cpu0': [0] * 8}, {'cpu1': [1] * 8})

    def test_cpu_sampling_deltas_summary_and_bound(self):
        before = {'cpu0': [0] * 8, 'cpu1': [0] * 8}
        after = {'cpu0': [90, 0, 0, 5, 0, 0, 5, 0], 'cpu1': [40, 0, 0, 50, 0, 0, 10, 0]}
        for bound in (owner_ftrace.CPU_MAX_BYTES, 4096):
            with self.subTest(bound=bound), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                (root / 'owner-cpu.stop').touch()
                with patch.object(owner_ftrace, 'cpu_ticks', side_effect=[before, after]), \
                        patch.object(owner_ftrace.time, 'monotonic', side_effect=[0, 1]), \
                        patch.object(owner_ftrace, 'CPU_MAX_BYTES', bound):
                    owner_ftrace.sample_cpu(root)
                data = json.loads((root / 'owner-cpu.json').read_text())
                if bound == 4096:
                    self.assertFalse(data['available'])
                    self.assertTrue(data['truncated'])
                    self.assertNotIn('summary', data)
                else:
                    self.assertTrue(data['available'])
                    self.assertEqual(data['samples'][0]['elapsed_seconds'], 1)
                    self.assertEqual(data['samples'][0]['cpus']['cpu0']['ticks'], after['cpu0'])
                    self.assertEqual(data['summary'], {'max_busy_pct': 95, 'mean_busy_pct': 72.5,
                                                       'max_softirq_pct': 10, 'cpus_above_90_busy': 1})

    def test_cpu_sampling_unavailable_is_not_zero(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.object(owner_ftrace, 'cpu_ticks', side_effect=OSError('denied')):
                owner_ftrace.sample_cpu(root)
            data = json.loads((Path(root) / 'owner-cpu.json').read_text())
            self.assertFalse(data['available'])
            self.assertNotIn('summary', data)
            self.assertIn('denied', data['reason'])
        self.assertFalse(owner_ftrace.stop_cpu(None)['available'])

    def test_cpu_sampler_process_stops_and_persists_bounded_artifact(self):
        with tempfile.TemporaryDirectory() as root:
            script = ('import json, owner_ftrace, sys; '
                      'print(json.dumps(owner_ftrace.start_cpu(sys.argv[1], duration_seconds=5)))')
            started = subprocess.run([sys.executable, '-B', '-c', script, root],
                                     check=True, capture_output=True, text=True, timeout=5,
                                     cwd=Path(__file__).resolve().parent)
            state = json.loads(started.stdout)
            self.assertIn('directory', state)
            data = owner_ftrace.stop_cpu(state)
            self.assertIn('samples', data)
            self.assertLess((Path(root) / 'owner-cpu.json').stat().st_size, owner_ftrace.CPU_MAX_BYTES)
        self.assertFalse(owner_ftrace.start_cpu('/unused', duration_seconds=0)['available'])
        self.assertFalse(owner_ftrace.start_cpu('/unused', duration_seconds=7201)['available'])

    def test_cpu_sampler_deadline_marks_incomplete_window_unavailable(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.object(owner_ftrace, 'cpu_ticks', side_effect=[
                    {'cpu0': [0] * 8}, {'cpu0': [1] * 8}]), \
                    patch.object(owner_ftrace.time, 'monotonic', side_effect=[0, 1, 1]):
                owner_ftrace.sample_cpu(root, duration_seconds=1)
            data = json.loads((Path(root) / 'owner-cpu.json').read_text())
            self.assertFalse(data['available'])
            self.assertTrue(data['truncated'])
            self.assertIn('deadline expired', data['reason'])

    def test_cgroup_memory_samples_timestamp_counter_changes_and_peak(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            cgroup = root / 'workload.service'
            cgroup.mkdir()
            (cgroup / 'memory.current').write_text('120\n')
            (cgroup / 'memory.max').write_text('200\n')
            (cgroup / 'memory.events').write_text('max 0\noom 0\n')
            (cgroup / 'memory.peak').write_text('125\n')
            elapsed = 0.0

            def tick(interval):
                nonlocal elapsed
                elapsed += interval
                if elapsed >= 1:
                    (cgroup / 'memory.current').write_text('150\n')
                    (cgroup / 'memory.events').write_text('max 3\noom 0\n')
                    (cgroup / 'memory.peak').write_text('199\n')
                if elapsed >= 1.5:
                    (root / 'cgroup-memory.stop').touch()

            with patch.object(owner_ftrace.time, 'monotonic', side_effect=lambda: elapsed), \
                 patch.object(owner_ftrace.time, 'monotonic_ns', side_effect=lambda: int(elapsed * 1e9)), \
                 patch.object(owner_ftrace.time, 'time_ns', side_effect=lambda: 10**18 + int(elapsed * 1e9)), \
                 patch.object(owner_ftrace.time, 'sleep', side_effect=tick):
                owner_ftrace.sample_memory(root, cgroup, duration_seconds=5)
            data = json.loads((root / 'cgroup-memory.json').read_text())
            self.assertTrue(data['available'])
            self.assertEqual(data['cgroup'], str(cgroup))
            self.assertEqual(data['sampled_peak_bytes'], 150)
            self.assertEqual(data['kernel_peak_bytes'], 199)
            samples = data['samples']
            self.assertEqual([row['memory.events']['max'] for row in samples], [0, 3, 3])
            self.assertEqual([row['memory.current'] for row in samples], [120, 150, 150])
            self.assertEqual([row['memory.max'] for row in samples], [200] * 3)
            self.assertEqual([row['observed_monotonic_ns'] for row in samples], [0, 10**9, 15 * 10**8])
            self.assertEqual([row['observed_unix_ns'] - 10**18 for row in samples], [0, 10**9, 15 * 10**8])

    def test_cgroup_memory_missing_required_counters_is_not_zero(self):
        with tempfile.TemporaryDirectory() as root:
            owner_ftrace.sample_memory(root, Path(root) / 'missing.service', duration_seconds=1)
            data = json.loads((Path(root) / 'cgroup-memory.json').read_text())
            self.assertFalse(data['available'])
            self.assertEqual(data['samples'], [])
            self.assertIsNone(data['sampled_peak_bytes'])
            self.assertIn('memory.events', data['reason'])

    def test_missing_events_and_failed_start_remove_instance(self):
        with tempfile.TemporaryDirectory() as root:
            tracefs = Path(root)
            (tracefs / 'instances').mkdir()
            # A fake tracefs cannot create control files. Inject the kernel error.
            with patch.object(Path, 'write_text', side_effect=OSError('denied')):
                instance = owner_ftrace.start('case', tracefs)
            state = owner_ftrace.START_STATES.pop(str(instance))
            self.assertFalse(instance.exists())
            self.assertEqual(state['events_missing'], list(owner_ftrace.EVENTS))
            self.assertTrue(state['errors'])

    def test_capture_budget_counts_escapes_and_keeps_only_complete_lines(self):
        first = 'worker [001] .... 1.000001: nvmet_req_init: arg="é' + chr(92) + chr(0) + '"\n'
        second = 'worker [001] .... 2.000002: nvmet_req_complete: value=1\n'
        budget = len(json.dumps(first + second, ensure_ascii=True)) - 3
        output = io.BytesIO()
        with patch.object(owner_ftrace, 'TRACE_MAX_SERIALIZED_BYTES', budget):
            state = owner_ftrace._capture_trace(io.BytesIO((first + second).encode()), output)
        self.assertEqual(output.getvalue().decode(), first)
        self.assertTrue(state['truncated'])
        self.assertLessEqual(len(json.dumps(output.getvalue().decode())) - 2, budget)
        self.assertEqual(state['trace_coverage']['event_count'], 1)
        self.assertEqual(state['trace_coverage']['duration_ns'], 0)

    def test_capture_coverage_uses_event_extrema_and_ignores_headers(self):
        text = ('# a trace header\n'
                'worker [001] .... 3.250000: nvmet_req_init: qid=1\n'
                'worker [002] .... 3.125000: ext4_sync_file_enter: ino=2\n'
                'worker [001] .... 3.500000: nvmet_req_complete: qid=1\n')
        output = io.BytesIO()
        state = owner_ftrace._capture_trace(io.BytesIO(text.encode()), output)
        self.assertFalse(state['truncated'])
        self.assertEqual(output.getvalue().decode(), text)
        self.assertEqual(state['trace_coverage'], {
            'event_count': 3, 'first_timestamp_ns': 3125000000, 'last_timestamp_ns': 3500000000,
            'duration_ns': 375000000, 'scope': 'captured_complete_trace_events', 'clock': 'mono'})

    def test_capture_partial_final_event_does_not_inflate_coverage(self):
        first = b'worker [001] .... 1.000001: nvmet_req_init: qid=1\n'
        partial = b'worker [001] .... 2.000002: nvmet_req_complete: qid='
        output = io.BytesIO()
        state = owner_ftrace._capture_trace(io.BytesIO(first + partial), output)
        self.assertEqual(output.getvalue(), first)
        self.assertTrue(state['truncated'])
        self.assertEqual(state['trace_coverage']['event_count'], 1)
        self.assertEqual(state['trace_coverage']['last_timestamp_ns'], 1000001000)

    def test_empty_trace_and_replaced_utf8_preserve_the_serialized_bound(self):
        empty = owner_ftrace._capture_trace(io.BytesIO(b'# no events\n'), io.BytesIO())
        self.assertEqual(empty['trace_coverage']['event_count'], 0)
        self.assertIsNone(empty['trace_coverage']['duration_ns'])
        self.assertFalse(empty['truncated'])
        raw = b'worker [001] .... 1.000001: nvmet_req_init: arg=\xff\n'
        output = io.BytesIO()
        state = owner_ftrace._capture_trace(io.BytesIO(raw), output)
        text = output.getvalue().decode('utf-8')
        self.assertIn('\ufffd', text)
        self.assertLessEqual(len(json.dumps(text)) - 2, owner_ftrace.TRACE_MAX_SERIALIZED_BYTES)
        self.assertEqual(state['trace_coverage']['event_count'], 1)

    def test_stop_cap_stats_and_removal_even_on_error(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            instance = root / 'instance'
            instance.mkdir()
            (instance / 'tracing_on').touch()
            first = b'worker [001] .... 1.000001: nvmet_req_init: qid=1\n'
            second = b'worker [001] .... 1.000003: nvmet_req_complete: qid=1\n'
            third = b'worker [001] .... 2.000000: nvmet_req_init: qid=2\n'
            (instance / 'trace').write_bytes(first + second + third)
            stats = instance / 'per_cpu/cpu0'
            stats.mkdir(parents=True)
            (stats / 'stats').write_text('overrun: 7\n')
            def remove(path):
                import shutil
                shutil.rmtree(path)
            budget = len(json.dumps((first + second).decode())) - 2
            with patch.object(owner_ftrace, 'TRACE_MAX_SERIALIZED_BYTES', budget), patch.object(Path, 'rmdir', remove):
                state = owner_ftrace.stop(instance, root / 'out')
            self.assertFalse(instance.exists())
            self.assertTrue(state['truncated'])
            self.assertEqual(state['buffer_overruns'], 7)
            self.assertEqual((root / 'out/owner-ftrace.txt').read_bytes(), first + second)
            self.assertEqual(state['trace_coverage'], {
                'event_count': 2, 'first_timestamp_ns': 1000001000, 'last_timestamp_ns': 1000003000,
                'duration_ns': 2000, 'scope': 'captured_complete_trace_events', 'clock': 'mono'})
            self.assertEqual(json.loads((root / 'out/owner-ftrace.json').read_text()), state)

    def test_output_error_still_removes_instance(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            instance = root / 'instance'
            instance.mkdir()
            output = root / 'not-a-directory'
            output.touch()
            with patch.object(Path, 'write_text', side_effect=OSError('denied')):
                state = owner_ftrace.stop(instance, output)
            self.assertFalse(instance.exists())
            self.assertTrue(any(e.startswith('output:') for e in state['errors']))
            self.assertTrue(any(e.startswith('metadata:') for e in state['errors']))

    def test_probe_reports_available_events(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            event = root / 'events' / owner_ftrace.EVENTS[0] / 'enable'
            event.parent.mkdir(parents=True)
            event.touch()
            self.assertEqual(owner_ftrace.probe(root)['events_enabled'], [owner_ftrace.EVENTS[0]])
