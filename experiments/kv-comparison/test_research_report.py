import csv
import json
from pathlib import Path
import statistics
import tempfile
import unittest

import research_report
import analyze


class RepetitionEvidence(unittest.TestCase):
    def test_fewer_than_five_runs_does_not_invent_confidence(self):
        result = research_report.interval([100.0, 150.0], statistics.mean)
        self.assertEqual(result['estimate'], 125.0)
        self.assertEqual(result['status'], 'insufficient_independent_repetitions')
        self.assertIsNone(result['interval'])
        self.assertIsNone(result['confidence'])

    def test_confidence_resamples_run_units_with_declared_seed(self):
        result = research_report.interval([100.0, 120.0, 80.0, 110.0, 90.0], statistics.mean)
        self.assertEqual(result['status'], 'estimated')
        self.assertEqual(result['estimate'], 100.0)
        self.assertEqual(result['confidence'], .95)
        self.assertLessEqual(result['interval'][0], result['estimate'])
        self.assertGreaterEqual(result['interval'][1], result['estimate'])
        self.assertGreater(result['interval'][1] - result['interval'][0], 0)

    def test_invalid_numeric_evidence_is_rejected(self):
        for values in ([], [float('nan')], [float('inf')], [-1.0]):
            with self.assertRaises(ValueError):
                research_report.interval(values, statistics.mean)

    def write_run(self, root, name, identity):
        run = root / name
        (run / 'result').mkdir(parents=True)
        (run / 'config.json').write_text(json.dumps({'namespace': identity}))
        (run / 'receipt.json').write_text(json.dumps({'started_at_unix': 1000, 'binary_sha256': 'binary'}))
        fields = ['phase','operation','outcome','scheduled_ns','dispatched_ns','dispatch_observed',
                  'completed_ns','api_ns','response_ns','scheduling_ns','logical_keys','bytes','error_kind']
        with (run / 'result/samples.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for number in range(20):
                writer.writerow(dict(zip(fields, ['measurement','read','success',number*1000,
                    number*1000+10,'true',number*1000+110,100,110,10,1,256,''])))
        return run

    def test_copied_artifacts_cannot_be_independent_repetitions(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            first = self.write_run(root, 'first', 'same-run')
            copied = self.write_run(root, 'copy', 'same-run')
            accounting = [{'achieved_success_calls_per_second': 100.0}] * 2
            with self.assertRaisesRegex(ValueError, 'duplicated'):
                research_report.repetition_uncertainty([first, copied], accounting)

    def test_small_tail_population_is_not_repaired_by_repetition_count(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            runs = [self.write_run(root, str(number), 'run-' + str(number)) for number in range(5)]
            result = research_report.repetition_uncertainty(runs, [
                {'achieved_success_calls_per_second': 100.0 + number} for number in range(5)])
            tails = [row for row in result['latency_between_run_variation'] if row['percentile'] == .99]
            self.assertTrue(all(row['status'] == 'insufficient_tail_population' for row in tails))
            self.assertTrue(all(row['interval'] is None for row in tails))
            self.assertEqual(result['throughput_mean_calls_per_second']['status'], 'estimated')


class CoverageEvidence(unittest.TestCase):
    def test_failure_is_retained_without_fabricated_latency(self):
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder) / 'failed'
            run.mkdir()
            (run / 'receipt.json').write_text(json.dumps({'status': 'failed', 'error': 'readiness deadline'}))
            result = research_report.make_report([run])
            self.assertFalse(result['complete'])
            self.assertEqual(result['coverage'][0]['classification'], 'failed_or_incomplete')
            self.assertEqual(result['coverage'][0]['error'], 'readiness deadline')
            self.assertEqual(list(research_report.rows(result)), [])
            self.assertIn('failed_or_incomplete', research_report.markdown(result, []))

    def test_qualification_does_not_become_performance(self):
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder) / 'qualification'
            run.mkdir()
            (run / 'receipt.json').write_text(json.dumps({'status': 'completed', 'claim_boundary': 'API qualification only'}))
            result = research_report.make_report([run])
            self.assertTrue(result['complete'])
            self.assertEqual(result['coverage'][0]['classification'], 'qualification_only')
            self.assertEqual(list(research_report.rows(result)), [])

    def test_duplicate_directories_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder)
            (run / 'receipt.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                research_report.discover([run, run])
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                analyze.analyze([run, run])


class RateSweepEvidence(unittest.TestCase):
    def cohort(self, rate, throughput, fraction=1.0, p99=50000, samples=10000, rf=1):
        return {
            'cohort': str(rate) + '-' + str(rf), 'runs': ['independent-' + str(n) for n in range(5)],
            'boundary': {'backend': {'name': 'tidehunter_tcp', 'acknowledgement': 'barrier', 'replication_factor': rf},
                         'workload': {'execution': {'mode': 'open_loop', 'offered_ops_per_second': rate}}},
            'per_run_accounting': [{'success_calls': int(10000*fraction), 'offered_calls': 10000} for _ in range(5)],
            'uncertainty': {
                'throughput_mean_calls_per_second': {'estimate': throughput},
                'latency_between_run_variation': [{
                    'operation': 'read', 'outcome': 'success', 'metric': 'response_ns', 'percentile': .99,
                    'observed_run_percentiles': [p99] * 5, 'sample_counts': [samples] * 5, 'status': 'estimated'}]}}

    def test_overloaded_and_slow_rates_cannot_win_the_objective(self):
        report = {'cohorts': [self.cohort(1000, 900), self.cohort(2000, 1500, fraction=.98),
                              self.cohort(3000, 2000, p99=200000)]}
        objective = research_report.slo_frontier(report, 100, .999, 'read')
        group = next(iter(objective['groups'].values()))
        self.assertEqual(group['maximum_observed_admissible_success_calls_per_second'], 900)
        self.assertEqual([row['passes_all_observed_runs'] for row in group['rates']], [True, False, False])

    def test_replicated_contract_and_small_tail_are_not_silently_merged(self):
        report = {'cohorts': [self.cohort(1000, 900, samples=20), self.cohort(1000, 800, rf=3)]}
        objective = research_report.slo_frontier(report, 100, .999, 'read')
        self.assertEqual(len(objective['groups']), 2)
        maxima = [group['maximum_observed_admissible_success_calls_per_second'] for group in objective['groups'].values()]
        self.assertIn(None, maxima)
        self.assertIn(800, maxima)
        for bound in (0, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                research_report.slo_frontier(report, bound, .999, 'read')


if __name__ == '__main__':
    unittest.main()
