"""One protected push means one explicitly selected paid suite, never two fleets."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

SPEC = importlib.util.spec_from_file_location('aws_suite', Path(__file__).with_name('suite.py'))
suite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite)
ROOT = Path(__file__).resolve().parents[2]


class SuiteTests(unittest.TestCase):
    def test_committed_default_and_manual_override_are_strict(self):
        self.assertEqual(suite.selection(), 'kv-comparison')
        for name in suite.SUITES:
            self.assertEqual(suite.selection(override=name, event='workflow_dispatch'), name)
            with self.assertRaises(ValueError):
                suite.selection(override=name, event='push')
        with self.assertRaises(ValueError):
            suite.selection(override='all', event='workflow_dispatch')
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / 'suite.json'
            for value in ({'schema_version': 1, 'suite': 'all'}, {'schema_version': True, 'suite': 'kv-comparison'},
                          {'schema_version': 1, 'suite': 'kv-comparison', 'other': 'sui-transport'}):
                config.write_text(json.dumps(value))
                with self.subTest(value=value), self.assertRaises(ValueError):
                    suite.selection(config)

    def environment(self):
        return dict(GITHUB_REPOSITORY='Loris-EPFL/tidehunter-distributed', GITHUB_REF='refs/heads/bench/aws-efa',
                    GITHUB_EVENT_NAME='push', GITHUB_SHA='a' * 40, AWS_BENCH_ENABLED='true',
                    AWS_BENCH_ROLE_ARN='role', AWS_BENCH_BUCKET='bucket', AWS_BENCH_BUCKET_REGION='us-east-1',
                    AWS_BENCH_GUEST_BOUNDARY_ARN='boundary', AWS_BENCH_WATCHDOG_FUNCTION='watchdog',
                    AWS_BENCH_WATCHDOG_RULE='rule')

    def test_kv_needs_no_sui_checkout_or_fixture_identity_but_keeps_watchdog_and_trust(self):
        environment = self.environment()
        with patch.object(suite, 'load_budget', return_value={'sha256': 'policy'}):
            self.assertTrue(suite.settings(environment)['enabled'])
            for key in ('AWS_BENCH_WATCHDOG_FUNCTION', 'AWS_BENCH_GUEST_BOUNDARY_ARN'):
                bad = environment.copy()
                del bad[key]
                with self.subTest(key=key), self.assertRaises(ValueError):
                    suite.settings(bad)
            with self.assertRaises(ValueError):
                suite.settings(environment | {'GITHUB_REF': 'refs/heads/main'})
            self.assertFalse(suite.settings(environment | {'AWS_BENCH_ENABLED': 'false'})['enabled'])

    def test_sui_override_preserves_existing_strict_preparation_settings(self):
        environment = self.environment() | {'GITHUB_EVENT_NAME': 'workflow_dispatch', 'AWS_BENCH_SUITE': 'sui-transport'}
        with self.assertRaisesRegex(ValueError, 'AWS_BENCH_INPUT_ROLE_ARN'):
            suite.settings(environment)

    def test_workflow_has_one_paid_job_and_credential_free_current_source_kv_build(self):
        workflow = yaml.load((ROOT / '.github/workflows/aws-efa-benchmarks.yaml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(workflow['on']['push']['branches'], ['bench/aws-efa'])
        self.assertEqual(workflow['on']['workflow_dispatch']['inputs']['suite']['options'], ['configured', *suite.SUITES])
        self.assertEqual(workflow['concurrency']['cancel-in-progress'], 'false')
        jobs = workflow['jobs']
        self.assertIn("outputs.suite == 'sui-transport'", jobs['prepare']['if'])
        self.assertIn("outputs.suite == 'kv-comparison'", jobs['prepare-kv']['if'])
        self.assertEqual(jobs['prepare-kv']['permissions'], {'contents': 'read'})
        self.assertEqual(jobs['prepare-kv']['environment'], 'rdma-bench-inputs')
        self.assertEqual(jobs['benchmark']['environment'], 'rdma-bench')
        self.assertEqual(jobs['benchmark']['needs'], ['selection', 'prepare', 'prepare-kv'])
        kv_steps = jobs['prepare-kv']['steps']
        self.assertEqual(kv_steps[0]['with']['ref'], '${{ github.sha }}')
        self.assertFalse(any('configure-aws-credentials' in step.get('uses', '') for step in kv_steps))
        self.assertFalse(any(step.get('with', {}).get('repository', '').startswith('Loris-EPFL/sui') for step in kv_steps))
        self.assertTrue(any('ci_prepare.py build' in step.get('run', '') for step in kv_steps))
        steps = jobs['benchmark']['steps']
        restore = next(index for index, step in enumerate(steps) if 'ci_prepare.py restore' in step.get('run', ''))
        credentials = next(index for index, step in enumerate(steps) if 'configure-aws-credentials' in step.get('uses', ''))
        self.assertLess(restore, credentials)
        campaigns = [(job, step) for job in jobs.values() for step in job['steps']
                     if step.get('id') == 'campaign']
        self.assertEqual(len(campaigns), 1)
        job, campaign = campaigns[0]
        self.assertGreaterEqual(int(campaign['timeout-minutes']) * 60, 10800 + 1800)
        self.assertGreater(int(job['timeout-minutes']), int(campaign['timeout-minutes']) + 15)
        self.assertIn('--inputs', campaign['run'])
        self.assertTrue(any(step.get('if', '').startswith('always()') and 'ci_campaign.py cleanup' in step.get('run', '') for step in steps))
        cleanup = next(index for index, step in enumerate(steps) if 'ci_campaign.py cleanup' in step.get('run', ''))
        refresh = steps[cleanup - 1]
        self.assertIn('configure-aws-credentials@', refresh['uses'])
        self.assertEqual(refresh['if'], steps[cleanup]['if'])
        self.assertTrue(refresh['if'].startswith('always()'))
        self.assertEqual(refresh['with']['role-duration-seconds'], '3600')
        self.assertEqual(refresh['with']['unset-current-credentials'], 'true')
        self.assertEqual(steps[credentials]['with']['role-duration-seconds'], '14400')
        local = yaml.load((ROOT / '.github/workflows/kv-comparison.yaml').read_text(), Loader=yaml.BaseLoader)
        self.assertNotIn('push', local['on'])


if __name__ == '__main__':
    unittest.main()
