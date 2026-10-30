import json
import os
import signal
from pathlib import Path
import tempfile
import unittest
import shutil
from unittest import mock

import cold_device_trace as trace


def event(timestamp, kind, sector, count, *, rwbs='R', device='259,0', error=0):
    payload = f'{device} {rwbs} '
    if kind == 'issue':
        payload += f'{count * 512} () '
    else:
        payload += '() '
    suffix = '[worker]' if kind == 'issue' else f'[{error}]'
    return f'worker-1 [000] .... {timestamp}: block_rq_{kind}: {payload}{sector} + {count} {suffix}'


class ParserTests(unittest.TestCase):
    def parse(self, *events, **kwargs):
        return trace.parse_trace('\n'.join(events), '259:0', **kwargs)

    def test_issue_complete_and_write_ignored(self):
        result = self.parse(event('1.000001', 'issue', 8, 4),
                            event('1.000004', 'complete', 8, 4),
                            event('1.000005', 'issue', 40, 4, rwbs='W'), lost=0)
        self.assertEqual((result['read_issues'], result['read_completions'], result['candidate_matched_reads']), (1, 1, 1))
        self.assertIsNone(result['matched_reads'])
        self.assertEqual(result['candidate_intervals_ns']['sum'], 3000)
        self.assertEqual(len(result['raw_events']), 3)

    def test_nearest_rank_percentiles_and_population_statistics(self):
        result = self.parse(event('1.000000000', 'issue', 1, 1),
                            event('1.000000001', 'complete', 1, 1),
                            event('2.000000000', 'issue', 1, 1),
                            event('2.000000009', 'complete', 1, 1), lost=0)
        dist = result['candidate_intervals_ns']
        self.assertEqual(result['candidate_raw_intervals_ns'], [1, 9])
        self.assertEqual((dist['p50'], dist['p95'], dist['p99'], dist['p999']), (1, 9, 9, 9))
        self.assertEqual((dist['mean'], dist['population_stddev']), (5, 4))
        self.assertTrue(dist['tail_warnings'])
    def test_timestamp_resolution_reflects_mixed_printed_precision(self):
        result = self.parse(event('1.000001', 'issue', 8, 4),
                            event('1.000002999', 'complete', 8, 4), lost=0)
        self.assertEqual(result['trace_clock'], 'mono')
        self.assertEqual(result['observed_timestamp_resolution_ns'], 1000)
        precise = self.parse(event('1.000000001', 'issue', 8, 4),
                             event('1.000000002', 'complete', 8, 4), lost=0)
        self.assertEqual(precise['observed_timestamp_resolution_ns'], 1)
        self.assertIsNone(self.parse(lost=0)['observed_timestamp_resolution_ns'])

    def test_boundary_straddling_candidate_is_not_a_proven_match(self):
        # An older same-range request may have been issued before capture and
        # completed here; the newer request may complete after capture.
        result = self.parse(event('1.000001', 'issue', 8, 4),
                            event('1.000002', 'complete', 8, 4), lost=0)
        self.assertEqual(result['candidate_matched_reads'], 1)
        self.assertIsNone(result['matched_reads'])
        self.assertIsNone(result['intervals_ns'])


    def test_overlaps_and_split_are_never_matched(self):
        result = self.parse(event('1.000001', 'issue', 8, 8),
                            event('1.000002', 'issue', 12, 8),
                            event('1.000003', 'complete', 8, 4),
                            event('1.000004', 'complete', 8, 8),
                            event('1.000005', 'complete', 12, 8))
        self.assertEqual(result['candidate_matched_reads'], 0)
        self.assertEqual(result['overlaps'], 1)
        self.assertEqual(result['split_or_partial_completions'], 1)
        self.assertEqual(result['unmatched_completions'], 3)
        self.assertIsNone(result['lost'])

    def test_duplicate_key_cannot_resume_matching_after_completion(self):
        result = self.parse(event('1.1', 'issue', 8, 4),
                            event('1.2', 'issue', 8, 4),
                            event('1.3', 'complete', 8, 4),
                            event('1.4', 'issue', 8, 4),
                            event('1.5', 'complete', 8, 4), lost=0)
        self.assertEqual(result['candidate_matched_reads'], 0)
        self.assertEqual(result['candidate_intervals_ns']['count'], 0)
        self.assertEqual(result['unmatched_completions'], 2)

    def test_loss_or_unknown_loss_suppresses_clean_looking_pair(self):
        for lost in (1, None):
            with self.subTest(lost=lost):
                result = self.parse(event('1.1', 'issue', 8, 4),
                                    event('1.2', 'complete', 8, 4), lost=lost)
                self.assertEqual((result['read_issues'], result['read_completions']), (1, 1))
                self.assertEqual(result['candidate_matched_reads'], 0)
                self.assertEqual(result['candidate_raw_intervals_ns'], [])
                self.assertEqual(len(result['raw_events']), 2)

    def test_requeue_loss_errors_and_bad_format_retained(self):
        result = self.parse(event('1.1', 'issue', 1, 8), event('1.2', 'requeue', 1, 8),
                            event('1.3', 'complete', 1, 8, error=-5),
                            'bad block_rq_issue: malformed', lost=4, truncated=True)
        self.assertEqual(result['requeues'], 1)
        self.assertEqual(result['completion_errors'], 1)
        self.assertEqual(result['malformed_events'], 1)
        self.assertEqual(result['lost'], 4)
        self.assertTrue(result['truncated'])
        self.assertEqual(result['candidate_matched_reads'], 0)

    def test_bounds_event_count_and_outstanding(self):
        with mock.patch.object(trace, 'MAX_EVENTS', 2), mock.patch.object(trace, 'MAX_OUTSTANDING', 1):
            result = self.parse(event('1.1', 'issue', 1, 1), event('1.2', 'issue', 3, 1),
                                event('1.3', 'complete', 1, 1))
        self.assertTrue(result['truncated'])
        self.assertEqual(result['outstanding_overflow'], 1)
        self.assertEqual(len(result['raw_events']), 2)


class IdentityParserTests(unittest.TestCase):
    def test_bpftrace_attachment_marker_stdout_and_stderr(self):
        marker = b'__BPFTRACE_NOTIFY_PROBES_ATTACHED\n'
        self.assertTrue(trace.attachment_marker_seen(marker, b''))
        self.assertTrue(trace.attachment_marker_seen(b'', marker))
        self.assertFalse(trace.attachment_marker_seen(b'', b'other diagnostic'))

    def test_partial_completion_and_boundary_filter(self):
        rows = ['I 110', 'P 120', 'C 110 130', 'C 90 140', '@events: 4']
        result = trace.parse_identity(rows, 100, 150)
        self.assertEqual(result['raw_intervals_ns'], [20])
        self.assertEqual(result['partial_completions'], 1)
        self.assertEqual(result['unmatched_completions'], 1)
        self.assertFalse(result['qualified'])
        self.assertEqual(result['lost'], 0)

    def test_lost_events_never_export_matched_intervals(self):
        for rows, diagnostics in [(['I 110', 'C 110 130', '@events: 3'], ''),
                                  (['I 110', 'C 110 130'], ''),
                                  (['I 110', 'C 110 130', '@events: 2'], 'lost events')]:
            with self.subTest(rows=rows, diagnostics=diagnostics):
                result = trace.parse_identity(rows, 100, 150, diagnostics=diagnostics)
                self.assertEqual(result['matched_reads'], 0)
                self.assertFalse(result['qualified'])

    def test_complete_identity_capture_and_anomalies(self):
        good = trace.parse_identity(['I 110', 'C 110 130', '@events: 2'], 100, 150)
        self.assertTrue(good['qualified'])
        self.assertEqual(good['matched_reads'], 1)
        for kind in ('R', 'M', 'E', 'O', 'D'):
            result = trace.parse_identity(['I 110', f'{kind} 120', 'C 110 130', '@events: 3'], 100, 150)
            self.assertFalse(result['qualified'])

    def test_end_straddling_issue_is_retained_and_unqualified(self):
        result = trace.parse_identity(['I 110', 'I 120', 'C 110 130', 'C 120 160', '@events: 4'], 100, 150)
        self.assertEqual(result['matched_reads'], 1)
        self.assertEqual(result['unmatched_issues'], 1)
        self.assertFalse(result['qualified'])


class CollectorTests(unittest.TestCase):
    def test_partition_maps_to_parent_and_stacked_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            disk = root / 'devices' / 'disk'
            partition = disk / 'disk1'
            (disk / 'slaves').mkdir(parents=True)
            (partition / 'slaves').mkdir(parents=True)
            (disk / 'dev').write_text('259:0')
            (partition / 'partition').write_text('1')
            sysblock = root / 'sysblock'
            sysblock.mkdir()
            st = root.stat().st_dev
            (sysblock / f'{os.major(st)}:{os.minor(st)}').symlink_to(partition)
            mapped = trace.resolve_device(root, sysblock)
            self.assertEqual(mapped['device'], '259:0')
            self.assertTrue(mapped['partition'])
            (disk / 'slaves' / 'stacked').mkdir()
            with self.assertRaises(ValueError):
                trace.resolve_device(root, sysblock)

    def test_startup_ring_admission_is_bounded_before_creation(self):
        bound = trace.startup_ring_bound('quiet trace_buf_size=1M', '0-3,8', 4096)
        self.assertEqual(bound['possible_cpus'], 5)
        self.assertLess(bound['conservative_startup_ring_bound_bytes'], trace.MAX_STARTUP_RING_BYTES)
        for cmdline, cpus in [('quiet', '0-3'), ('trace_buf_size=1G', '0-255'),
                              ('trace_buf_size=1M trace_buf_size=2M', '0-3'),
                              ('trace_buf_size=1M', '0-256')]:
            with self.subTest(cmdline=cmdline, cpus=cpus), self.assertRaises(ValueError):
                trace.startup_ring_bound(cmdline, cpus, 4096)

    def test_denied_is_unavailable_and_never_touches_global_tracing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / 'state'
            state.mkdir()
            trace_root = root / 'trace'
            trace_root.mkdir()
            global_control = trace_root / 'tracing_on'
            global_control.write_text('1')
            with mock.patch.object(trace, 'TRACE_ROOT', trace_root), \
                 mock.patch.object(trace, 'identity_capture',
                     side_effect=lambda *_args: trace.identity_unavailable(state, 'permission denied', returncode=1)), \
                 mock.patch.object(trace, 'resolve_device', return_value={'device': '259:0'}):
                trace.worker(state, root, 1, 'tidehunter-' + 'a' * 32)
            result = json.loads((state / 'result.json').read_text())
            self.assertFalse(result['available'])
            self.assertNotIn('read_issues', result)
            self.assertFalse(result['identity_attempt']['available'])
            self.assertEqual(result['identity_attempt']['returncode'], 1)
            self.assertEqual(result['identity_attempt']['backend'], 'bpftrace-request-identity')
            self.assertEqual(global_control.read_text(), '1')
            self.assertEqual(json.loads((state / 'status.json').read_text()), result)

    def test_worker_deadline_collects_and_removes_only_owned_instance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / 'state'
            state.mkdir()
            trace_root = root / 'trace'
            (trace_root / 'instances').mkdir(parents=True)
            instance = trace_root / 'instances' / ('tidehunter-' + 'b' * 32)
            mkdir, write_text, rmdir = Path.mkdir, Path.write_text, Path.rmdir

            def fake_mkdir(path, *args, **kwargs):
                mkdir(path, *args, **kwargs)
                if path == instance:
                    (path / 'per_cpu' / 'cpu0').mkdir(parents=True)
                    (path / 'per_cpu' / 'cpu0' / 'stats').write_text(
                        'overrun: 0\ncommit overrun: 0\ndropped events: 0\n')
                    (path / 'buffer_total_size_kb').write_text('64')
                    (path / 'trace_pipe').write_text('')
                    for name in ('block_rq_issue', 'block_rq_complete', 'block_rq_requeue'):
                        directory = path / 'events' / 'block' / name
                        directory.mkdir(parents=True)
                        (directory / 'format').write_text('dev; sector; nr_sector; rwbs[8];')

            def fake_write(path, data, *args, **kwargs):
                result = write_text(path, data, *args, **kwargs)
                if path == instance / 'tracing_on' and data == '1':
                    write_text(instance / 'trace', '\n'.join([
                        event('1.000001', 'issue', 8, 4),
                        event('1.000004', 'complete', 8, 4)]))
                return result

            def fake_rmdir(path):
                if path == instance:
                    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                    shutil.rmtree(path)
                else:
                    rmdir(path)
            close = os.close
            def fake_close(fd):
                signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                close(fd)


            with mock.patch.object(trace, 'TRACE_ROOT', trace_root), \
                 mock.patch.object(trace, 'resolve_device', return_value={'device': '259:0'}), \
                 mock.patch.object(trace, 'identity_capture', return_value=None), \
                 mock.patch.object(trace, 'startup_ring_bound', return_value={'possible_cpus': 1}), \
                 mock.patch.object(trace.os, 'cpu_count', return_value=1), \
                 mock.patch.object(Path, 'mkdir', fake_mkdir), \
                 mock.patch.object(Path, 'write_text', fake_write), \
                 mock.patch.object(trace.os, 'close', fake_close), \
                 mock.patch.object(trace, 'drain_trace', side_effect=lambda fd, instance, state, deadline, stop_requested:
                                   ((instance / 'trace').read_bytes(), False, trace.time.monotonic_ns())), \
                 mock.patch.object(Path, 'rmdir', fake_rmdir):
                trace.worker(state, root, 10, instance.name)
            result = json.loads((state / 'result.json').read_text())
            self.assertFalse(result['qualified'])
            self.assertIsNone(result['matched_reads'])
            self.assertEqual(result['candidate_matched_reads'], 1)
            self.assertFalse(instance.exists())

    def test_state_directory_is_exclusive_and_stop_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / 'owned'
            state.mkdir()
            with self.assertRaises(FileExistsError):
                trace.start(temp, state, 1)
            status = {'available': True, 'state_dir': str(state)}
            result = {'available': False, 'reason': 'denied'}
            trace.save(state / 'owner.json', {'state_dir': str(state), 'instance': 'tidehunter-' + 'a' * 32})
            trace.save(state / 'status.json', status)
            trace.save(state / 'result.json', result)
            self.assertEqual(trace.stop(state), result)
            self.assertEqual(trace.stop(state), result)

    def test_nonblocking_drain_stops_and_marks_byte_cap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'stop').touch()
            (root / 'tracing_on').write_text('1')
            read_fd, write_fd = os.pipe2(os.O_NONBLOCK)
            try:
                os.write(write_fd, b'abcdefghij\n')
                with mock.patch.object(trace, 'MAX_BYTES', 4):
                    data, truncated, stopped = trace.drain_trace(read_fd, root, root, trace.time.monotonic() + 1)
                self.assertEqual(data, b'abcd')
                self.assertTrue(truncated)
                self.assertIsNotNone(stopped)
                self.assertEqual((root / 'tracing_on').read_text(), '0')
            finally:
                os.close(read_fd)
                os.close(write_fd)

    def test_spawn_failure_has_idempotent_stop(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / 'owned'
            with mock.patch.object(trace.subprocess, 'Popen', side_effect=OSError('denied')):
                result = trace.start(temp, state, 1)
            self.assertFalse(result['available'])
            self.assertEqual(trace.stop(state), result)
            self.assertEqual(trace.stop(state), result)

    def test_stop_before_startup_status_requests_shutdown(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / 'owned'
            state.mkdir()
            trace.save(state / 'owner.json', {'state_dir': str(state), 'instance': 'tidehunter-' + 'a' * 32})
            with mock.patch.object(trace.time, 'monotonic', side_effect=[0, 16]):
                result = trace.stop(state)
            self.assertFalse(result['available'])
            self.assertTrue((state / 'stop').exists())


if __name__ == '__main__':
    unittest.main()
