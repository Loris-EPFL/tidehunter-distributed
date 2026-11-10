import json
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_campaign as cloud


class FocusedReportTests(unittest.TestCase):
    def test_fio_uses_dedicated_evidence_without_kv_samples(self):
        job = next(job for job in cloud.make_focused_matrix('recommended') if job['case_kind'] == 'fio_floor')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case = root / 'cases' / job['case_id']
            case.mkdir(parents=True)
            (root / 'matrix.json').write_text(json.dumps({'jobs': [job]}))
            (case / 'receipt.json').write_text(json.dumps({'job': job, 'status': 'completed'}))
            evidence = {'fio_result': {'case_id': job['case_id'], 'read_ios': 42},
                        'phase_timing': {'case_id': job['case_id'], 'spans': []}}
            with patch.object(cloud.analyze, 'admit_focused_artifacts', return_value=evidence):
                result = cloud.report(root)
            row = result['cases'][0]
            self.assertEqual(row['status'], 'completed')
            self.assertEqual(row['focused_evidence'], evidence)
            self.assertFalse((case / 'result/samples.csv').exists())
            self.assertTrue((root / 'comparison.csv').exists())

    def test_completed_focused_evidence_error_is_fatal(self):
        job = next(job for job in cloud.make_focused_matrix('recommended') if job['case_kind'] == 'fio_floor')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case = root / 'cases' / job['case_id']
            case.mkdir(parents=True)
            (root / 'matrix.json').write_text(json.dumps({'jobs': [job]}))
            (case / 'receipt.json').write_text(json.dumps({'job': job, 'status': 'completed'}))
            with patch.object(cloud.analyze, 'admit_focused_artifacts', side_effect=ValueError('artifact hash mismatch')):
                with self.assertRaisesRegex(ValueError, 'artifact hash mismatch'):
                    cloud.report(root)

    def test_duplicate_focused_id_fails_before_reading_receipts(self):
        job = cloud.make_focused_matrix('recommended')[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'matrix.json').write_text(json.dumps({'jobs': [job, job]}))
            with self.assertRaisesRegex(ValueError, 'duplicate focused case ID'):
                cloud.report(root)
    def test_device_export_coverage_and_no_duplicate_history_baselines(self):
        jobs = cloud.make_focused_matrix('device')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'matrix.json').write_text(json.dumps({
                'spec': {'mode': 'focused', 'selection': 'device', 'direct_control_arm': True},
                'jobs': jobs}))
            result = cloud.report(root)
            self.assertEqual(result['planned_cases'], 120)
            self.assertEqual(result['focused_coverage']['group'], {'G10R': 60, 'G10W': 60})
            self.assertEqual(result['focused_coverage']['case_kind'], {'media_read': 60, 'remote_put': 60})



class HistoryComparisonTests(unittest.TestCase):
    def fixture(self, root):
        jobs = cloud.make_focused_matrix('recommended')
        prewarm = next(j for j in jobs if j.get('media', {}).get('history') == 'prewarm')
        baseline = next(j for j in jobs if j.get('case_kind') == 'media_read'
                        and j['backend'] == 'tidehunter_verbs_native' and j['repeat'] == prewarm['repeat']
                        and j['value_size'] == prewarm['value_size']
                        and j['media']['expected'] == 'cold' and j['media']['history'] == 'normal')
        directory = root / 'cases' / prewarm['case_id']
        directory.mkdir(parents=True)
        gate = {'gate_released': True, 'completion_gate_observed': True}
        for side, count in (('before', 100), ('end', 190)):
            name = 'measurement-' + side + '.json'
            raw = json.dumps({'hosts': [{}, {'diskstats': f'8 0 dev {count} 0 0 0'}]}).encode()
            (directory / name).write_bytes(raw)
            gate[side], gate[side + '_sha256'] = name, hashlib.sha256(raw).hexdigest()
        evidence = {'expected': 'cold', 'status': 'unqualified',
                    'reason_codes': ['history_baseline_compared_in_report'], 'measurement_drained': True,
                    'successful_reads': 100, 'read_ops_per_success': 0.9,
                    'owner_devices': [{'owner_index': 0, 'major': 8, 'minor': 0,
                                       'read_ops_before': 100, 'read_ops_after': 190}],
                    'history_loads_before': 10, 'history_loads_after': 20}
        warm = {'case_id': prewarm['case_id'], 'job': prewarm, 'status': 'completed',
                'measurement_evidence': gate, 'focused_evidence': {'media_evidence': evidence}}
        control = {'case_id': baseline['case_id'], 'job': baseline, 'status': 'completed',
                   'focused_evidence': {'media_evidence': {'status': 'qualified', 'measurement_drained': True,
                                                          'history_loads_before': 100, 'history_loads_after': 200}}}
        return warm, control

    def test_boundaries_and_unavailable_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            warm, baseline = self.fixture(root)
            verdict = cloud.qualify_history_prewarm(warm, baseline, root)
            self.assertEqual(verdict['status'], 'qualified')
            self.assertEqual((verdict['history_delta_ratio'], verdict['history_ratio_max']), (0.1, 0.1))
            bad = copy.deepcopy(warm)
            bad['focused_evidence']['media_evidence']['history_loads_after'] = 21
            self.assertIn('history_delta_above_threshold',
                          cloud.qualify_history_prewarm(bad, baseline, root)['reason_codes'])
            bad['focused_evidence']['media_evidence']['history_loads_after'] = 9
            self.assertIn('history_counter_unavailable',
                          cloud.qualify_history_prewarm(bad, baseline, root)['reason_codes'])
            bad = copy.deepcopy(warm)
            bad['focused_evidence']['media_evidence']['read_ops_per_success'] = 0.89
            self.assertIn('cold_ratio_below_threshold',
                          cloud.qualify_history_prewarm(bad, baseline, root)['reason_codes'])
            self.assertIn('history_baseline_missing',
                          cloud.qualify_history_prewarm(warm, None, root)['reason_codes'])
            unqualified = copy.deepcopy(baseline)
            unqualified['focused_evidence']['media_evidence'].update(status='unqualified', measurement_drained=False)
            result = cloud.qualify_history_prewarm(warm, unqualified, root)
            self.assertEqual(result['status'], 'unqualified')
            self.assertIn('history_baseline_unqualified', result['reason_codes'])
            baseline['focused_evidence']['media_evidence']['history_loads_after'] = 100
            verdict = cloud.qualify_history_prewarm(warm, baseline, root)
            self.assertIn('history_baseline_zero', verdict['reason_codes'])
            self.assertNotIn('history_delta_above_threshold', verdict['reason_codes'])
            self.assertIsNone(verdict['history_delta_ratio'])

    def test_report_matches_repeat_and_preserves_bound_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            warm, baseline = self.fixture(root)
            jobs = [baseline['job'], warm['job']]
            (root / 'matrix.json').write_text(json.dumps({'jobs': jobs}))
            for row in (warm, baseline):
                case = root / 'cases' / row['case_id']
                case.mkdir(parents=True, exist_ok=True)
                (case / 'receipt.json').write_text(json.dumps({'job': row['job'], 'status': 'completed'}))
            original = copy.deepcopy(warm['focused_evidence']['media_evidence'])
            rows = {row['case_id']: row for row in (warm, baseline)}
            with patch.object(cloud, 'summarize_case', side_effect=lambda path: copy.deepcopy(rows[path.name])):
                result = cloud.report(root)
            row = next(row for row in result['cases'] if row['case_id'] == warm['case_id'])
            self.assertEqual(row['history_prewarm_qualification']['status'], 'qualified')
            self.assertEqual(row['focused_evidence']['media_evidence'], original)
            self.assertIn('0.1', (root / 'comparison.csv').read_text())


if __name__ == '__main__':
    unittest.main()
