"""Deterministic tests of captured-record conversion and workload accounting."""
import csv
from pathlib import Path
import tempfile
import unittest

import trace_summary as trace
from capture_run import validate


def row(**values):
    result = {key: 0 for key in ('event seq ns thread db call op ks key key_len value_len content group position count outcome reference epoch version member aux').split()}
    result['data'] = '00' * 32
    result.update(values)
    return result


DATABASES = {1: {'backend': 'remote', 'namespace': 'live/checkpoints',
                 'keyspaces': {0: 'objects', 1: 'never_touched'},
                 'remote_identity': '01' * 16}}


class TraceSummaryTests(unittest.TestCase):
    def test_conversion_preserves_reference_and_application_key(self):
        records = [row(event=13, seq=1, ns=1, db=0, op=5, ks=0, key=99,
                       key_len=32, value_len=256, reference=1234, version=4, member=2,
                       aux=1, data='01' * 16 + '00' * 16),
                   row(event=12, seq=2, ns=2, db=1, ks=0, key_len=32,
                       value_len=256, reference=1234, version=4, member=2, outcome=1)]
        converted = list(trace.evaluation_events(records, DATABASES))
        self.assertEqual(converted, [(1, ('publish_put', 0, 99, 32, 256, 1234)),
                                     (1, ('get', 0, 99, 32, 256, 1234))])
        with tempfile.TemporaryDirectory() as directory:
            result = trace.convert(records, DATABASES, Path(directory))
            with Path(result['1']['path']).open() as stream:
                actual = list(csv.reader(stream, delimiter='\t'))
            self.assertEqual(tuple(actual[0]), trace.EVAL_HEADER)
            self.assertEqual(actual[1], ['publish_put', '0', '99', '32', '256', '1234'])

    def test_scan_and_delete_are_distinct(self):
        records = [row(event=13, db=1, op=6, key=8, key_len=4, reference=77,
                       aux=1), row(event=12, db=1, key=8, key_len=4, reference=77, aux=1)]
        self.assertEqual([event[1][0] for event in trace.evaluation_events(records, DATABASES)],
                         ['publish_delete', 'scan_get'])

    def test_missing_reference_key_and_shape_conflict_fail(self):
        with self.assertRaisesRegex(ValueError, 'no application-key'):
            list(trace.evaluation_events([row(event=12, db=1, reference=1)], DATABASES))
        with self.assertRaisesRegex(ValueError, 'collision'):
            trace.reference_catalog([row(event=11, reference=1, db=1, value_len=4),
                                     row(event=11, reference=1, db=1, value_len=8)], DATABASES)

    def test_zero_is_a_valid_key_and_reference_id(self):
        records = [row(event=12, db=1, key=0, position=1, reference=0)]
        self.assertEqual(list(trace.evaluation_events(records, DATABASES)),
                         [(1, ('get', 0, 0, 0, 0, 0))])

    def test_nearest_rank_and_reuse_distance(self):
        self.assertEqual(trace.percentile([4, 1, 3, 2], .5), 2)
        reuse = trace.Reuse(5)
        for key, ns in [(1, 1), (2, 2), (2, 3), (1, 8), (1, 9)]:
            reuse.observe(key, ns)
        self.assertEqual(reuse.operations, [0, 2, 0])
        self.assertEqual(reuse.distinct, [0, 1, 0])
        self.assertEqual(reuse.times, [1, 7, 1])

    def test_completed_put_then_read_and_untouched_tables(self):
        records = [row(event=1, db=1, call=1, op=5, ns=1, count=1),
                   row(event=3, db=1, call=1, op=5, ns=2, key=9, key_len=4,
                       value_len=16, outcome=1),
                   row(event=2, db=1, call=1, op=5, ns=3, outcome=1),
                   row(event=1, db=1, call=2, op=1, ns=4, count=1),
                   row(event=3, db=1, call=2, op=1, ns=5, key=9, key_len=4,
                       value_len=16, outcome=1),
                   row(event=2, db=1, call=2, op=1, ns=6, outcome=1)]
        summaries, writes = trace.summarize(records, DATABASES)
        result = summaries['1']
        self.assertEqual(result['read_after_write']['share'], 1)
        self.assertEqual(result['untouched_keyspaces'], ['never_touched'])
        self.assertEqual(result['api_read_calls'], 1)
        self.assertEqual(result['keyspaces']['0']['read_value_bytes']['p99'], 16)
        populations = result['keyspaces']['0']['api_latency_ns_by_operation_outcome']
        self.assertEqual(set(populations), {'get:found', 'put:ok'})
        self.assertEqual(populations['get:found']['p50'], 2)
        self.assertEqual(writes, [{'db': 1, 'ns': 3, 'bytes': 20}])

    def test_empty_scan_still_touches_its_keyspace(self):
        records = [row(event=6, db=1, call=10, ks=1),
                   row(event=1, db=1, call=11, op=8, ks=1, ns=1, count=1),
                   row(event=2, db=1, call=11, op=8, ks=1, ns=2, outcome=1),
                   row(event=9, db=1, call=10, ks=1, ns=3, outcome=1)]
        summaries, _ = trace.summarize(records, DATABASES)
        self.assertEqual(summaries['1']['untouched_keyspaces'], ['objects'])
        self.assertEqual(summaries['1']['keyspaces']['1']['api_calls'], {'scan_next': 1})

    def test_invalid_capture_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / 'final.status').write_text('accepted\t1\nwritten\t1\ndropped_full\t1\ndropped_closed\t0\nactive\t0\n')
            with self.assertRaisesRegex(ValueError, 'invalid capture'):
                validate(path)


class TraceMetricTests(unittest.TestCase):
    def test_byte_histograms_are_not_latencies(self):
        from trace_metrics import storage_histograms
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'metrics.prom'
            path.write_text('rocksdb_get_bytes_bucket{cf_name="x",le="+Inf"} 4\n'
                            'rocksdb_get_latency_seconds_bucket{cf_name="x",le="0.001"} 4\n'
                            'rocksdb_get_latency_seconds_bucket{cf_name="x",le="+Inf"} 4\n'
                            'db_op_bucket{ks="x",op="get",le="5"} 4\n'
                            'db_op_bucket{ks="x",op="get",le="+Inf"} 4\n')
            result = storage_histograms(path)
        self.assertEqual(len(result), 2)
        self.assertEqual({row['unit'] for row in result.values()}, {'seconds', 'microseconds'})

    def test_histogram_quantiles_are_bucket_interpolations(self):
        from trace_metrics import histogram_quantile
        buckets = [(1, 2), (2, 10), (float('inf'), 10)]
        self.assertEqual(histogram_quantile(buckets, .5), 1.375)
        self.assertEqual(histogram_quantile(buckets, .99), 1.9875)

    def test_checkpoint_windows_keep_zero_byte_intervals(self):
        from trace_metrics import checkpoint_windows
        log = ('2026-01-01T00:00:01.000000Z INFO execute_checkpoint{seq=7}: executing checkpoint\n'
               '2026-01-01T00:00:01.100000Z INFO execute_checkpoint{seq=7}: Finished syncing and executing checkpoint 7\n'
               '2026-01-01T00:00:02.000000Z INFO execute_checkpoint{seq=8}: executing checkpoint\n')
        anchor = 1767225600 * 1_000_000_000
        writes = [{'db': 1, 'ns': 1_500_000_000, 'bytes': 32}]
        result = checkpoint_windows(log, writes, anchor, 3_000_000_000)
        self.assertEqual([row['checkpoint_start_label'] for row in result], [-1, 7, 8])
        self.assertEqual([row['write_bytes'] for row in result], [0, 32, 0])
        self.assertEqual(result[1]['bytes_per_second'], 32)


if __name__ == '__main__':
    unittest.main()
