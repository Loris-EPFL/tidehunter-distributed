import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_campaign
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

    def fixture(self, root):
        jobs = cloud_campaign.make_focused_matrix('recommended')
        chosen = [next(j for j in jobs if j['case_kind'] == 'media_read'
                       and j['backend'] == 'tidehunter_verbs_native'
                       and j['value_size'] == 1024 and j['repeat'] == repeat
                       and j['media']['expected'] == 'cold'
                       and j['media']['history'] == 'normal') for repeat in (0, 1)]
        rows = []
        for job, p50 in zip(chosen, (100, 900)):
            rows.append({'case_id': job['case_id'], 'job': job, 'status': 'completed',
                         'metrics': {'success_calls_per_second': 42,
                                     'populations': [{'operation': 'read', 'outcome': 'success',
                                                      'api_ns': {'p50': p50, 'p99': p50 * 2, 'p999': p50 * 3}},
                                                     {'operation': 'read', 'outcome': 'failed',
                                                      'api_ns': {'p50': 999999}}]},
                         'focused_evidence': {'media_evidence': {'status': 'unqualified',
                             'reason_codes': ['missing_devices'], 'read_ops_per_success': None}},
                         'media_qualification': {'status': 'unqualified', 'reason_codes': ['missing_devices']}})
        missing = next(j for j in jobs if j['case_kind'] == 'fio_floor')
        rows.append({'case_id': missing['case_id'], 'job': missing, 'status': 'not_run'})
        failed = next(j for j in jobs if j['case_kind'] == 'block_trace')
        rows.append({'case_id': failed['case_id'], 'job': failed, 'status': 'failed', 'error': 'synthetic failure'})
        export, report = root / 'export', root / 'report'
        export.mkdir()
        report.mkdir()
        (export / 'matrix.json').write_text(json.dumps({'jobs': [row['job'] for row in rows]}))
        (report / 'comparison.json').write_text(json.dumps({'schema_version': 1, 'cases': rows,
            'planned_cases': len(rows), 'completed_cases': 2, 'complete': False}))
        return export, report, rows

    def test_outputs_keep_unqualified_and_missing_cases_and_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report, cases = self.fixture(root)
            first, second = root / 'first', root / 'second'
            focused_analysis.generate(export, report, first)
            for path, key in ((export / 'matrix.json', 'jobs'), (report / 'comparison.json', 'cases')):
                data = json.loads(path.read_text())
                data[key].reverse()
                path.write_text(json.dumps(data))
            focused_analysis.generate(export, report, second)
            summary = json.loads((first / 'focused-summary.json').read_text())
            self.assertIsInstance(summary['tables'], dict)
            self.assertTrue(summary['tables'])
            all_rows = [row for table in summary['tables'].values() for row in table]
            self.assertTrue(all(isinstance(row.get('case_ids'), list) for row in all_rows))
            serialized = json.dumps(summary)
            for case in cases:
                self.assertIn(case['case_id'], serialized)
            self.assertIn('missing_devices', serialized)
            self.assertIn('unqualified', serialized)
            self.assertIn('synthetic failure', serialized)
            self.assertIn('not_run', serialized)
            for name in summary['tables']:
                csv_path = first / (name + '.csv')
                self.assertTrue(csv_path.is_file(), name)
                with csv_path.open(newline='') as stream:
                    self.assertIsNotNone(csv.DictReader(stream).fieldnames)
                self.assertTrue((first / (name + '.md')).is_file())
            self.assertEqual({p.name: p.read_bytes() for p in first.iterdir() if p.is_file()},
                             {p.name: p.read_bytes() for p in second.iterdir() if p.is_file()})
            media = {row['case_id']: row for row in summary['tables']['media_read']}
            for case, expected in zip(cases[:2], (0.1, 0.9)):
                row = media[case['case_id']]
                self.assertEqual(row['repeat'], case['job']['repeat'])
                self.assertEqual(row['api_p50_us'], expected)
                self.assertEqual(row['verdict'], 'unqualified')
                self.assertIn('missing_devices', row['reason_codes'])
                self.assertIsNone(row['read_ops_per_success'])
            fio = summary['tables']['fio_floor'][0]
            self.assertEqual(fio['status'], 'not_run')
            self.assertIsNone(fio['iops'])
            self.assertEqual(fio['verdict'], 'missing')

    def test_partial_and_failed_admitted_samples_are_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report, cases = self.fixture(root)
            data = json.loads((report / 'comparison.json').read_text())
            data['cases'][0]['status'] = 'failed'
            (report / 'comparison.json').write_text(json.dumps(data))
            summary = focused_analysis.generate(export, report, root / 'output')
            self.assertFalse(summary['complete'])
            self.assertEqual(summary['completed_cases'], 1)
            row = next(r for r in summary['tables']['media_read'] if r['case_id'] == cases[0]['case_id'])
            self.assertEqual(row['status'], 'failed')
            self.assertIsNone(row['api_p50_us'])
            self.assertIsNone(row['successful_calls_per_second'])

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

    def test_coverage_combines_accounting_and_stream_qualification(self):
        jobs = cloud_campaign.make_focused_matrix('recommended')
        knee = next(j for j in jobs if j['case_kind'] == 'sealed_knee'
                    and j['backend'] == 'tidehunter_nvme_rdma')
        interference = next(j for j in jobs if j['case_kind'] == 'interference'
                            and j['backend'] == 'tidehunter_verbs_native')
        rows = [
            {'case_id': knee['case_id'], 'job': knee, 'status': 'completed',
             'sealed_read_accounting': {'qualified': False, 'reasons': ['sealed_read_counter_inconsistent']}},
            {'case_id': interference['case_id'], 'job': interference, 'status': 'completed',
             'interference_route_qualification': {'qualified': True, 'reasons': []},
             'focused_evidence': {'interference_result': {'qualification': {
                 'qualified': False, 'reasons': ['stream_overlap_below_threshold']}}}},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report = root / 'export', root / 'report'
            export.mkdir()
            report.mkdir()
            (export / 'matrix.json').write_text(json.dumps({'jobs': [r['job'] for r in rows]}))
            (report / 'comparison.json').write_text(json.dumps({'cases': rows}))
            tables = focused_analysis.generate(export, report, root / 'output')['tables']
            coverage = {r['case_kind']: r for r in tables['coverage']}
            for kind, reason in [('sealed_knee', 'sealed_read_counter_inconsistent'),
                                 ('interference', 'stream_overlap_below_threshold')]:
                self.assertEqual(coverage[kind]['unqualified'], 1)
                self.assertEqual(coverage[kind]['reason_counts'], {reason: 1})
            self.assertEqual(tables['sealed_knee'][0]['verdict'], 'unqualified')
            self.assertEqual(tables['interference'][0]['verdict'], 'unqualified')

    def test_sustained_complete_bucket_selection(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('device')
                   if j['group'] == 'G10W' and j['measurement_operations'] == 200000)
        case = {'case_id': job['case_id'], 'job': job, 'status': 'completed'}
        second = 1000000000
        scenarios = [
            (5 * second + second // 2, 5, second // 2),
            (5 * second, 5, 0),
            (4 * second + second // 2, 4, second // 2),
            (second // 2, 0, second // 2),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for end, complete_count, partial_duration in scenarios:
                with self.subTest(end=end):
                    samples = [(0, i * second + second // 2, 1)
                               for i in range(complete_count)]
                    samples.append((0, end, 1))
                    with patch.object(focused_analysis, 'device_samples', return_value=samples):
                        tables = focused_analysis.device_generate(root, root, root / 'out',
                                                                  [case], None)['tables']
                    seconds = tables['sustained_seconds']
                    seconds.sort(key=lambda row: row['second'])
                    summary = tables['sustained_summary'][0]
                    self.assertEqual(seconds[-1]['duration_ns'], partial_duration)
                    self.assertEqual(seconds[-1]['bucket_status'], 'partial')
                    self.assertEqual(seconds[-1]['put_count'], 1)
                    self.assertIsNone(seconds[-1]['puts_per_second'])
                    self.assertEqual(sum(s['bucket_status'] == 'complete' for s in seconds),
                                     complete_count)
                    self.assertEqual(summary['first_second'], seconds[0] if complete_count else None)
                    self.assertEqual(summary['last_five_seconds'],
                                     seconds[complete_count - 5:complete_count]
                                     if complete_count >= 5 else [])
                    self.assertEqual(summary['stall_regime'], 'insufficient duration or samples')

    def test_device_samples_spreads_and_sustained(self):
        jobs = cloud_campaign.make_focused_matrix('device')
        reads = [next(j for j in jobs if j['group'] == 'G10R'
                      and j['backend'] == 'tidehunter_verbs_native'
                      and j['value_size'] == 1024 and j['concurrency'] == 1
                      and j['repeat'] == repeat) for repeat in (0, 1, 2)]
        sustained = next(j for j in jobs if j['group'] == 'G10W'
                         and j['measurement_operations'] == 200000)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report = root / 'export', root / 'report'
            export.mkdir()
            report.mkdir()
            cases = []
            for index, job in enumerate(reads + [sustained]):
                is_read = job['group'] == 'G10R'
                verdict = 'unqualified' if index == 1 else 'qualified'
                case = {'case_id': job['case_id'], 'job': job, 'status': 'completed',
                        'focused_evidence': {'media_evidence': {'status': verdict,
                            'reason_codes': ['cold_not_proven'] if index == 1 else [],
                            'read_ops_per_success': 1}} if is_read else {}}
                if is_read and index == 0:
                    case['focused_evidence']['media_evidence'].update(
                        history_loads_before=3, history_loads_after=5)
                cases.append(case)
                result = export / 'cases' / job['case_id'] / 'result'
                result.mkdir(parents=True)
                rows = [(0, 1000, (index + 1) * 1000), (0, 2000, (index + 1) * 2000)]
                if not is_read:
                    rows = [(0, 100000000, 100000000),
                            (1000000000, 1200000000, 200000000)]
                with (result / 'samples.csv').open('w', newline='') as stream:
                    writer = csv.writer(stream)
                    writer.writerow(['phase', 'operation', 'outcome', 'dispatched_ns', 'completed_ns', 'api_ns'])
                    for dispatch, completion, api in rows:
                        writer.writerow(['measurement', 'read' if is_read else 'put', 'success',
                                         dispatch, completion, api])
            (export / 'matrix.json').write_text(json.dumps({'jobs': reads + [sustained]}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            tables = focused_analysis.generate(export, report, root / 'output')['tables']
            rows = {r['repeat']: r for r in tables['device_reads']}
            self.assertEqual([rows[x]['api_p50_us'] for x in (0, 1, 2)], [1, 2, 3])
            self.assertEqual(rows[1]['verdict'], 'unqualified')
            spread = next(r for r in tables['device_read_spread'] if r['metric'] == 'api_p50_us')
            self.assertEqual((spread['min'], spread['median'], spread['max']), (1, 2, 3))
            decision = next(r for r in tables['decision'] if r['route'] == 'owner verbs')
            self.assertEqual(decision['read_1024_c1_api_p50_us'], 2)
            self.assertNotIn(reads[1]['case_id'], decision['case_ids'])
            history = decision['read_1024_c1_history']
            self.assertEqual(history['0']['delta'], 2)
            self.assertEqual(history['0']['status'], 'index or history pages read')
            self.assertEqual(history['2']['status'], 'missing')
            self.assertEqual(rows[2]['history_load_status'], 'missing')
            seconds = tables['sustained_seconds']
            self.assertEqual([r['put_count'] for r in seconds], [1, 1])
            self.assertEqual([r['puts_per_second'] for r in seconds], [1, None])
            summary = tables['sustained_summary'][0]
            self.assertAlmostEqual(summary['slow_worker_time_share'], 2 / 3)
            self.assertEqual(summary['seconds_slow_count_multiple_of_concurrency'], .5)
            self.assertFalse(tables['g1_comparison'])
            old = next(j for j in cloud_campaign.make_focused_matrix('recommended')
                       if j['group'] == 'G1' and j['backend'] == reads[0]['backend']
                       and j['value_size'] == 1024 and j['repeat'] == 0
                       and j['sealed_dataset']['tier'] == 'repeat_read')
            baseline = root / 'baseline'
            baseline.mkdir()
            (baseline / 'comparison.json').write_text(json.dumps({'cases': [{
                'case_id': old['case_id'], 'job': old, 'status': 'completed',
                'media_qualification': {'status': 'qualified'},
                'metrics': {'populations': [{'operation': 'read', 'outcome': 'success',
                                             'api_ns': {'p50': 500}}]}}]}))
            compared = focused_analysis.generate(export, report, root / 'compared', baseline)['tables']
            self.assertEqual(compared['g1_comparison'][0]['p50_ratio'], 2)

    def test_unqualified_puts_do_not_drive_decisions_or_ratios(self):
        jobs = cloud_campaign.make_focused_matrix('device')
        selected = [next(j for j in jobs if j['group'] == 'G10W'
                         and j['backend'] == 'tidehunter_tcp'
                         and j['durability_tier'] == tier and j['value_size'] == 1024
                         and j['concurrency'] == 1 and j['repeat'] == rep
                         and j['measurement_operations'] == 4096)
                    for rep in (0, 1) for tier in ('synchronous', 'native')]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report = root / 'export', root / 'report'
            export.mkdir()
            report.mkdir()
            cases = []
            for index, job in enumerate(selected):
                qualified = index in (1, 2)
                cases.append({'case_id': job['case_id'], 'job': job, 'status': 'completed',
                              'media_qualification': {'status': 'qualified' if qualified else 'unqualified',
                                                      'reason_codes': [] if qualified else ['bad_evidence']}})
                result = export / 'cases' / job['case_id'] / 'result'
                result.mkdir(parents=True)
                (result / 'samples.csv').write_text(
                    'phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                    'measurement,put,success,0,1000,1000\n')
            (export / 'matrix.json').write_text(json.dumps({'jobs': selected}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            tables = focused_analysis.generate(export, report, root / 'output')['tables']
            self.assertEqual(len(tables['unqualified']), 2)
            self.assertEqual(len(tables['device_puts']), 4)
            for row in tables['put_durability_ratio']:
                self.assertIsNone(row['p50_ratio'])
                self.assertIsNone(row['p99_ratio'])
            decision = next(row for row in tables['decision'] if row['route'] == 'owner TCP')
            self.assertNotIn(selected[0]['case_id'], decision['case_ids'])
            self.assertNotIn(selected[3]['case_id'], decision['case_ids'])
            self.assertIn(selected[1]['case_id'], decision['case_ids'])
            self.assertIn(selected[2]['case_id'], decision['case_ids'])

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
