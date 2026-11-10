import csv
import json
from pathlib import Path
import tempfile
import unittest

import fair_native


def job(case_id, group, backend, concurrency, repeat=0, mode=None, size=1024, **options):
    return {'case_id': case_id, 'group': group, 'backend': backend,
            'value_size': size, 'concurrency': concurrency, 'repeat': repeat,
            'cohort': 'primary_rf1', 'keys': 4096, 'mix': {'put': 100},
            'measurement_operations': 4096, 'warmup_operations': 512,
            'distribution': 'uniform', 'offered_ops_per_second': None,
            'native_durability': mode if backend == 'native' else 'value_wal_barrier',
            'durability_tier': mode if backend == 'native' else 'synchronous', **options}


def case(item, status='completed', p50=10000):
    return {'case_id': item['case_id'], 'job': item, 'status': status,
            'metrics': {'offered_calls': 10, 'unsuccessful_calls': 0,
                        'success_calls_per_second': 100,
                        'measurement_timeline': {'api_time_sum_ns': 30, 'duration_ns': 10},
                        'populations': [{'operation': 'put', 'outcome': 'success', 'count': 10,
                                         'api_mean_ns': p50 + 1000,
                                         'api_ns': {'p50': p50, 'p99': p50 * 2}}]}}


class FairNativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.export = self.root / 'export'
        self.report = self.root / 'report'
        self.export.mkdir()
        self.report.mkdir()

    def publish(self, cases):
        (self.export / 'matrix.json').write_text(json.dumps({'jobs': [c['job'] for c in cases]}))
        (self.report / 'comparison.json').write_text(json.dumps({'cases': cases}))

    def assert_table_captions(self, text, expected_count):
        lines = text.splitlines()
        headers = [index for index, line in enumerate(lines)
                   if line.startswith('| Arm: topology; network; durability; routing; mode')]
        self.assertEqual(len(headers), expected_count)
        for index in headers:
            caption = lines[index - 1]
            self.assertTrue(caption.startswith('Caption: '))
            for phrase in (
                'Writer counts are matched within each column',
                'one or five Db instances on one SSD with no network hop',
                'five owner instances and SSDs with a network hop',
                'fsync on every registered Value-WAL file and the directory after every put',
                'same calls shared by callers in one run',
                'one covering fdatasync of one owner file per group',
                'directory synced once per file',
                'Page-cache acknowledgements have no barrier',
                '16x64 cells per instance',
                'append-only owner history plus a coordinator index',
                'Native routes by key hash; owners route by operation ID',
            ):
                with self.subTest(phrase=phrase, table=index):
                    self.assertIn(phrase, caption)

    def test_tables_show_repetitions_topology_and_matching_concurrency(self):
        cases = [case(job('n1', 'G14N', 'native', 1, mode='value_wal_barrier')),
                 case(job('n5', 'G14S', 'native', 5, mode='value_wal_group_barrier', native_shards=5)),
                 case(job('n5r1', 'G14S', 'native', 5, repeat=1, mode='value_wal_group_barrier', native_shards=5), 'failed'),
                 case(job('o1', 'G14O1', 'tidehunter_tcp', 1, owner_count=1)),
                 case(job('o5', 'G14O5', 'tidehunter_verbs_native', 16, owner_count=5)),
                 case(job('profile', 'G14P', 'native', 16, mode='value_wal_barrier'))]
        cases[2]['error'] = 'failed run'
        metrics = self.export / 'cases' / 'n5' / 'result'
        metrics.mkdir(parents=True)
        (metrics / 'metrics.prom').write_text(
            'kv_research_native_group_barrier_runs_total{shard="1"} 2\n'
            'kv_research_native_group_barrier_callers_total{shard="1"} 4\n'
            'kv_research_native_group_barrier_errors_total{shard="1"} 0\n')
        self.publish(cases)
        rows = {row['case_id']: row for row in fair_native.generate(self.export, self.report, self.root / 'out')}
        self.assertEqual(rows['n5']['native_shards'], 5)
        self.assertIsNone(rows['n5']['group_stats_per_shard']['0'])
        self.assertEqual(rows['n5']['group_stats_per_shard']['1']['runs'], 2)
        self.assertIsNone(rows['n5']['group_stats_per_shard']['4'])
        self.assertEqual(rows['o1']['callers_per_ssd_derived'], 1)
        self.assertEqual(rows['o5']['callers_per_ssd_derived'], 3.2)
        self.assertEqual(rows['n5']['mean_inflight_calls_derived'], 3)
        self.assertEqual(rows['profile']['measurement_class'], 'profiled')
        self.assertEqual(rows['n5r1']['error'], 'failed run')
        with (self.root / 'out' / 'fair_native.csv').open(newline='') as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), len(rows))
        text = (self.root / 'out' / 'fair_native.md').read_text()
        self.assertIn('1 writer', text)
        self.assertIn('5 writers', text)
        self.assertIn('16 writers', text)
        self.assertIn('r1 n5r1 failed', text)
        self.assertIn('n=10 mean=11.0 µs', text)
        self.assertIn('operation-ID routing differ', text)
        self.assertIn('no ratios or rankings', text)
        self.assertIn('group stats by shard={"0": null, "1":', text)
        self.assertEqual(text.count('## 1024 bytes'), 2)
        self.assert_table_captions(text, 2)

    def test_workload_and_cohort_separate_tables(self):
        native = job('native', 'G14N', 'native', 5, mode='value_wal_barrier')
        owner = job('owner', 'G14O1', 'tidehunter_tcp', 5, owner_count=1)
        for key, changed in [('cohort', 'other'), ('keys', 8), ('value_size', 8192),
                             ('put_key_policy', 'new')]:
            with self.subTest(key=key):
                self.publish([case(native | {key: changed}), case(owner)])
                fair_native.generate(self.export, self.report, self.root / 'out')
                text = (self.root / 'out' / 'fair_native.md').read_text()
                self.assertEqual(text.count('\n## '), 2)
                self.assert_table_captions(text, 2)

    def test_strict_matrix_and_ambiguous_population_rejected(self):
        item = job('a', 'G14N', 'native', 1, mode='page_cache')
        one = case(item)
        self.publish([one])
        (self.report / 'comparison.json').write_text(json.dumps({'cases': []}))
        with self.assertRaisesRegex(ValueError, 'do not match'):
            fair_native.generate(self.export, self.report, self.root / 'out')
        one['metrics']['populations'].append(one['metrics']['populations'][0])
        self.publish([one])
        with self.assertRaisesRegex(ValueError, 'ambiguous success'):
            fair_native.generate(self.export, self.report, self.root / 'out')

    def test_metrics_are_per_shard_and_missing_is_not_zero(self):
        path = self.root / 'metrics.prom'
        self.assertEqual(fair_native.group_stats(path), {})
        path.write_text(
            'kv_research_native_group_barrier_runs_total{shard="0"} 4\n'
            'kv_research_native_group_barrier_callers_total{shard="0"} 10\n'
            'kv_research_native_group_barrier_errors_total{shard="0"} 0\n'
            'kv_research_native_group_barrier_group_size_bucket{le="1",shard="0"} 1\n'
            'kv_research_native_group_barrier_group_size_bucket{le="4",shard="0"} 4\n'
            'kv_research_native_group_barrier_group_size_bucket{le="+Inf",shard="0"} 4\n'
            'kv_research_native_group_barrier_runs_total{shard="1"} 2\n')
        stats = fair_native.group_stats(path)
        self.assertEqual(stats['0']['mean_size_derived'], 2.5)
        self.assertEqual(stats['0']['max_size_upper_bound_derived'], 4)
        self.assertIsNone(stats['1']['callers'])
        self.assertNotIn('2', stats)
        path.write_text('kv_research_native_group_barrier_runs_total 5\n'
                        'kv_research_native_group_barrier_errors_total 1\n'
                        'kv_research_native_group_barrier_callers_total 10\n')
        self.assertEqual(fair_native.group_stats(path)['0']['mean_size_derived'], 2.5)

    def test_invalid_histograms_and_counters(self):
        path = self.root / 'metrics.prom'
        path.write_text('kv_research_native_group_barrier_group_size_bucket{le="1",shard="2"} 2\n'
                        'kv_research_native_group_barrier_group_size_bucket{le="+Inf",shard="2"} 1\n')
        with self.assertRaisesRegex(ValueError, 'cumulative'):
            fair_native.group_stats(path)
        path.write_text('kv_research_native_group_barrier_runs_total 1\n'
                        'kv_research_native_group_barrier_errors_total 2\n')
        with self.assertRaisesRegex(ValueError, 'errors exceed runs'):
            fair_native.group_stats(path)


if __name__ == '__main__':
    unittest.main()
