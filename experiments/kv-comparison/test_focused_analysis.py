import csv
import json
from pathlib import Path
import tempfile
import unittest

import focused_analysis


class FocusedAnalysisTests(unittest.TestCase):
    def test_spread_preserves_independent_repetitions(self):
        spread = focused_analysis.summarize_spread([9, 1, 5])
        self.assertEqual((spread['min'], spread['median'], spread['max']), (1, 5, 9))
        self.assertEqual(focused_analysis.summarize_spread([4])['median'], 4)
        empty = focused_analysis.summarize_spread([])
        self.assertTrue(all(empty[key] is None for key in ('min', 'median', 'max')))

    def test_ratio_missing_and_zero_denominator(self):
        self.assertEqual(focused_analysis.ratio(9, 3), 3)
        self.assertIsNone(focused_analysis.ratio(None, 3))
        self.assertIsNone(focused_analysis.ratio(3, None))
        self.assertIsNone(focused_analysis.ratio(3, 0))

    def test_qualified_history_supersedes_provisional_media_only(self):
        case = {
            'history_prewarm_qualification': {'status': 'qualified', 'reason_codes': []},
            'media_qualification': {'status': 'unqualified', 'reason_codes': ['history_baseline_compared_in_report']},
            'focused_evidence': {'media_evidence': {'status': 'unqualified',
                'reason_codes': ['history_baseline_compared_in_report']}},
        }
        self.assertEqual(focused_analysis.verdict(case), ('qualified', []))
        case['target_io_qualification'] = {'qualified': False, 'reasons': ['target_unverified']}
        case['sealed_read_accounting'] = {'qualified': False, 'reasons': ['counter_inconsistent']}
        self.assertEqual(focused_analysis.verdict(case),
                         ('unqualified', ['counter_inconsistent', 'target_unverified']))

    def test_floor_tables_and_independent_log_recomputation(self):
        net = {'tool': 'ib_send_lat', 'mode': 'polled', 'interface': 'rdma0',
               'provider': None, 'endpoint': None, 'sizes': [8, 1024, 4096, 65536]}
        ssd = {'block_bytes': 4096, 'region_bytes': 100000, 'state': 'fresh',
               'age_seconds': 0, 'runtime_seconds': 10}
        def job(case_id, kind, params):
            return {'case_id': case_id, 'case_kind': kind, 'group': 'G11N' if kind == 'net_floor' else 'G11S',
                    'backend': 'tidehunter_verbs_native', 'repeat': 0, 'value_size': 65536,
                    'net' if kind == 'net_floor' else 'ssd': params}
        jobs = [job('net-ok', 'net_floor', net), job('net-failed', 'net_floor', net),
                job('ssd-ok', 'ssd_floor', ssd), job('ssd-not-run', 'ssd_floor', ssd),
                job('ssd-missing', 'ssd_floor', ssd)]
        cases = [{'case_id': j['case_id'], 'job': j, 'status': status}
                 for j, status in zip(jobs, ('completed', 'failed', 'completed', 'not_run', 'completed'))]
        cases[0]['focused_evidence'] = {'net_floor_result': {'runs': [
            {'size_bytes': 8, 'status': 'completed', 'latency_semantics': 'half_round_trip',
             'latency_us': {'p50': 2, 'avg': 3, 'p99': 5, 'p999': 6, 'min': 1, 'max': 7}},
            {'size_bytes': 1024, 'status': 'completed', 'latency_semantics': 'round_trip',
             'latency_us': {'p50': 4}},
            {'size_bytes': 4096, 'status': 'failed', 'latency_us': {'p50': 999}}]}}
        cases[1]['focused_evidence'] = cases[0]['focused_evidence']
        cases[2]['focused_evidence'] = {'ssd_floor_result': {
            'qualification': {'qualified': True}, 'lat_log': {'path': 'ssd_floor_lat.log'},
            'elapsed_seconds': 2, 'read_ios': 8, 'latency_ns': {'p99': 30000, 'p999': 40000}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report = root / 'export', root / 'report'
            export.mkdir()
            report.mkdir()
            log_path = export / 'cases' / 'ssd-ok' / 'ssd_floor_lat.log'
            log_path.parent.mkdir(parents=True)
            log_path.write_text(''.join(f'{t}, {lat}, 0, 4096, {offset}\n' for t, lat, offset in [
                (100, 1000, 0), (300, 1200, 10000), (2600, 1100, 20000),
                (2700, 1000, 30000), (5100, 10000, 40000), (5300, 11000, 50000),
                (7600, 12000, 60000), (7800, 10000, 70000)]))
            (export / 'matrix.json').write_text(json.dumps({'jobs': jobs}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            output = root / 'output'
            tables = focused_analysis.generate(export, report, output)['tables']
            network = {(r['case_id'], r['size_bytes']): r for r in tables['net_floor']}
            self.assertEqual(len(network), 8)
            self.assertEqual(network['net-ok', 8]['round_trip_p50_us'], 4)
            self.assertEqual(network['net-ok', 1024]['round_trip_p50_us'], 4)
            self.assertIsNone(network['net-ok', 4096]['p50_us'])
            self.assertIsNone(network['net-failed', 8]['p50_us'])
            row = next(r for r in tables['ssd_floor'] if r['case_id'] == 'ssd-ok')
            raw = [list(map(int, line.split(','))) for line in log_path.read_text().splitlines()]
            values = sorted(x[1] for x in raw)
            self.assertEqual(row['p50_us'], values[(len(values) + 1) // 2 - 1] / 1000)
            self.assertEqual(row['fast_share'], sum(x[1] < 5000 for x in raw) / len(raw))
            self.assertEqual(row['fast_share_by_time_quarter'], [1, 1, 0, 0])
            self.assertEqual(row['fast_share_by_offset_decile'], [1, 1, 1, 1, 0, 0, 0, 0, None, None])
            self.assertEqual(row['iops'], 4)
            self.assertEqual(row['p99_us'], 30)
            self.assertEqual(row['age_seconds'], 0)
            common = {'case_ids', 'case_id', 'status', 'repeat', 'verdict', 'reason_codes'}
            net_fields = common | {'tool', 'mode', 'interface', 'provider', 'endpoint', 'size_bytes',
                                   'latency_semantics', 'min_us', 'p50_us', 'avg_us', 'p99_us',
                                   'p999_us', 'max_us', 'round_trip_p50_us'}
            ssd_fields = common | {'block_bytes', 'region_bytes', 'state', 'age_seconds', 'n',
                                   'mean_us', 'p50_us', 'p99_us', 'p999_us', 'iops', 'fast_share',
                                   'fast_mean_us', 'slow_mean_us', 'fast_share_by_time_quarter',
                                   'fast_share_by_offset_decile'}
            self.assertEqual(set(network['net-ok', 8]), net_fields)
            self.assertEqual(set(row), ssd_fields)
            for case_id in ('ssd-not-run', 'ssd-missing'):
                missing = next(r for r in tables['ssd_floor'] if r['case_id'] == case_id)
                self.assertIsNone(missing['p50_us'])
                self.assertEqual(missing['verdict'], 'missing')
            coverage = {r['case_kind']: r for r in tables['coverage']}
            self.assertEqual((coverage['net_floor']['planned'], coverage['net_floor']['failed']), (2, 1))
            self.assertEqual((coverage['ssd_floor']['planned'], coverage['ssd_floor']['not_run']), (3, 1))
            for name, fields in (('net_floor', net_fields), ('ssd_floor', ssd_fields)):
                with (output / (name + '.csv')).open(newline='') as stream:
                    self.assertEqual(set(csv.DictReader(stream).fieldnames), fields)
                markdown_header = (output / (name + '.md')).read_text().splitlines()[5]
                self.assertEqual(set(markdown_header.strip(' |').split(' | ')), fields)

    def test_ssd_unimodal_and_empty_log(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'lat.log'
            path.write_text('0, 1000, 0, 4096, 0, 0\n1, 1100, 0, 4096, 4096, 0\n')
            self.assertIsNone(focused_analysis.ssd_log_metrics(path, 10000, 10)['fast_share'])
            path.write_text('')
            result = focused_analysis.ssd_log_metrics(path, 10000, 10)
            self.assertEqual(result['n'], 0)
            self.assertIsNone(result['p50_us'])

    def test_real_local_dry_run_when_present(self):
        root = Path(__file__).resolve().parents[3] / 'checkpoint-work' / 'spdk-matched-storage-20261105T004442Z' / 'local-focused-quiet-verbs-group-20261107' / 'results'
        if not (root / 'matrix.json').is_file() or not (root / 'comparison.json').is_file():
            self.skipTest('local focused dry-run report not present')
        with tempfile.TemporaryDirectory() as directory:
            focused_analysis.generate(root, root, Path(directory))
            summary = json.loads((Path(directory) / 'focused-summary.json').read_text())
            expected = {row['case_id'] for row in json.loads((root / 'comparison.json').read_text())['cases']}
            actual = {case_id for rows in summary['tables'].values() for row in rows for case_id in row['case_ids']}
            self.assertTrue(expected <= actual)


if __name__ == '__main__':
    unittest.main()
