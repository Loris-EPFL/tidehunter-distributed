"""Offline KV admission/atomic-lifecycle regressions; no real AWS or services."""
import copy
from datetime import datetime, timedelta, timezone
import fnmatch
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import ci_campaign as ci

# Reuse the maintained service simulation rather than a second allocator model.
_spec = importlib.util.spec_from_file_location('kv_ci_legacy_fixtures', ci.OLD / 'test_ci_campaign.py')
legacy = importlib.util.module_from_spec(_spec)
with patch.dict(sys.modules, {'ci_campaign': ci.core}):
    _spec.loader.exec_module(legacy)
RUN = 'th-0123456789abcdef'
ACCOUNT = '354775971805'
ENV = dict(AWS_BENCH_BUCKET='tidehunter-ci-354775971805-us-east-1', AWS_BENCH_BUCKET_REGION='us-east-1',
    AWS_BENCH_GUEST_BOUNDARY_ARN=f'arn:aws:iam::{ACCOUNT}:policy/guest-boundary',
    AWS_BENCH_WATCHDOG_FUNCTION='janitor', AWS_BENCH_WATCHDOG_RULE='janitor')


class ConfigTests(unittest.TestCase):
    def config(self, mutate=None):
        value = ci.core.read_json(ci.HERE / 'ci-config.json')
        if mutate:
            mutate(value)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'cfg.json'
            ci.core.dump(path, value)
            return ci.load_config(path, ENV)

    def test_all_eight_full_matrix_and_three_hour_lifetime(self):
        cfg = self.config()
        self.assertEqual(len(ci.cloud_campaign.make_matrix(cfg['spec'])), 684)
        self.assertEqual(cfg['spec']['backends'], list(ci.cloud_campaign.BACKENDS))
        self.assertEqual((cfg['ttl_seconds'], cfg['export_reserve_seconds']), (10800, 600))

    def test_export_bound_covers_all_planned_csv_rows_and_monitor_snapshot(self):
        cfg = self.config()
        jobs = ci.cloud_campaign.make_matrix(cfg['spec'])
        offered = sum(job['measurement_operations'] for job in jobs if job['offered_ops_per_second'] is not None)
        rows = sum(job['keys'] + job['warmup_operations'] + job['measurement_operations'] for job in jobs)
        self.assertEqual(offered, 11712000)
        self.assertEqual(rows, 21520896)
        # Eight u64 columns, bounded/escaped error text and fixed labels fit
        # within 1KiB per CSV row. Keep the full 4GiB observer archive as well.
        raw_allowance = rows * 1024 + 4 * 1024**3
        self.assertLess(raw_allowance, cfg['raw_export_max_gib'] * 1024**3)
        self.assertGreaterEqual(cfg['cost_assumptions']['result_gib'], cfg['raw_export_max_gib'])

    def test_rejects_short_raw_bound_and_underreserved_s3_storage(self):
        with self.assertRaisesRegex(ValueError, 'raw export bound'):
            self.config(lambda cfg: cfg.update(raw_export_max_gib=8))
        with self.assertRaisesRegex(ValueError, 'shared cost reservation'):
            self.config(lambda cfg: cfg['cost_assumptions'].update(result_gib=8))

    def test_rejects_independent_budget_larger_ttl_and_unapproved_hardware(self):
        for key, value in [('weekly_budget_usd', '1000'), ('ttl_seconds', 10801),
                           ('instance_types', ['t3.micro']), ('branch', 'main')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.config(lambda cfg: cfg.update({key: value}))

    def test_rejects_shortened_or_incomplete_matrix(self):
        original = ci.core.read_json
        for replacement in ({'repeats': 1}, {'backends': ['native']}, {'export_reserve_seconds': 1}):
            def read(path):
                value = original(path)
                return value | replacement if Path(path).name == 'cloud-campaign.json' else value
            with self.subTest(replacement=replacement), patch.object(ci.core, 'read_json', side_effect=read):
                with self.assertRaises(ValueError):
                    self.config()

    def test_real_sui_loader_still_refuses_three_hour_config(self):
        old = ci.core.read_json(ci.OLD / 'ci-config.json')
        old['ttl_seconds'] = 10800
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'cfg.json'
            ci.core.dump(path, old)
            with self.assertRaises(ValueError):
                ci.core.load_config(path, ENV)

    def test_prepared_source_failure_precedes_any_aws_client(self):
        with patch.object(ci.ci_prepare, 'admit_inputs', side_effect=ValueError('stale source')), \
                patch.object(ci.core, 'AWS') as aws:
            with self.assertRaisesRegex(ValueError, 'stale source'):
                ci.admit_inputs(Path('/unavailable'), Path('/unused'))
            aws.assert_not_called()


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.clock = legacy.Clock()
        self.cfg = ci.load_config(ci.HERE / 'ci-config.json', ENV)
        self.cfg['regions'] = ['us-east-1', 'us-east-2']
        self.budget = self.output / 'budget.json'
        ci.core.dump(self.budget, {'weekly_budget_usd': '100'})
        self.cfg['budget_path'] = str(self.budget)
        self.addCleanup(patch.stopall)
        patch.object(legacy, 'ACCOUNT', ACCOUNT).start()
        legacy.Ledger.reservations, legacy.Ledger.settlements, legacy.Ledger.policies = [], {}, {}
        legacy.Ledger.available_usd = '100'
        patch('ci_budget.BudgetLedger', legacy.Ledger).start()
        self.aws = legacy.AWS(self.clock)
        self.campaign = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock)
        patch.object(self.campaign, 'network', return_value='sg-owned').start()
        self.bundle = self.output / 'bundle.tar.gz'
        self.bundle.write_bytes(b'sealed runtime fixture')
        self.admission = dict(bindings=dict(source_commit='a' * 40, source_sha256='b' * 64,
            binary_sha256='c' * 64, binary_blake2b512='d' * 128),
            inputs={'runner_bundle': dict(path=str(self.bundle), sha256=ci.core.sha(self.bundle), bytes=self.bundle.stat().st_size)},
            bootstrap_sha256='e' * 64, atomic_start_sha256='f' * 64)
        self.candidates = [dict(region=region, az=region + 'a', instance_type='g6.8xlarge', ami='ami-reviewed',
            subnet='subnet-owned', vpc='vpc-owned', root_device='/dev/sda1', root_gib=32,
            maximum_estimate_usd=str(36 + i * 18), host_hourly_usd=str(2 + i), price={})
            for i, region in enumerate(self.cfg['regions'])]
        self.preflight = dict(candidates=self.candidates, boundary=dict(arn=self.cfg['guest_boundary_arn'],
            version='v2', sha256='a' * 64), ancillary_prices=dict(storage_usd_per_gib_month='0.023',
            request_usd='0.000005', transfer_usd_per_gib='0.09'))
        def upload(path, key):
            if key == RUN + '/input/bundle.tar.gz':
                key = RUN + '/inputs/bundle.tar.gz'
            raw = Path(path).read_bytes()
            self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=raw,
                ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
            return dict(key=key, bytes=len(raw), sha256=ci.core.sha(path))
        patch.object(self.campaign, 'upload', side_effect=upload).start()

    def allocate(self):
        return self.campaign.allocate(self.admission, self.preflight)

    def configure_fixture(self, instances, candidate, plan):
        for item in self.aws.ec2[candidate['region']].instances:
            item['NetworkInterfaces'] = [{'InterfaceType': 'efa'}]
        group = dict(GroupName=RUN, Strategy='cluster', State='available', Tags=ci.core.aws_tags(
            ci.core.ownership(RUN) | {'ExpiresAt': self.campaign.journal.data['expires_at']}))
        ec2 = self.aws.ec2[candidate['region']]
        placement_groups = [group]
        patch.object(ec2, 'describe_placement_groups',
                     side_effect=lambda **params: {'PlacementGroups': copy.deepcopy(placement_groups)}).start()
        def delete_placement_group(**params):
            self.assertEqual(params, {'GroupName': RUN})
            self.assertTrue(all(item['State']['Name'] == 'terminated' for item in ec2.instances))
            placement_groups.clear()
            return {}
        patch.object(ec2, 'delete_placement_group', side_effect=delete_placement_group, create=True).start()
        patch.object(self.campaign, 'bind_profiles', side_effect=lambda current, *args: current).start()
        def mutate(operation, region, params):
            self.assertEqual(operation, 'create_tags')
            for item in self.aws.ec2[region].instances:
                if item['InstanceId'] in params['Resources']:
                    tags = ci.core.tag_map(item) | {t['Key']: t['Value'] for t in params['Tags']}
                    item['Tags'] = ci.core.aws_tags(tags)
            return {}
        patch.object(self.campaign, 'mutate', side_effect=mutate).start()
        return self.campaign.configure(instances, candidate, plan)

    def test_pending_is_durable_before_any_runinstances_and_ready_uses_cas(self):
        ec2 = self.aws.ec2['us-east-1']
        original = ec2.run_instances
        def launch(**request):
            pending = json.loads(self.aws.s3.objects[RUN + '/inputs/controller.json'])
            self.assertEqual(pending['state'], 'pending')
            self.assertEqual(pending['source_sha256'], self.admission['bindings']['source_sha256'])
            self.assertEqual(request['Placement']['GroupName'], RUN)
            self.assertEqual(request['NetworkInterfaces'][0]['InterfaceType'], 'efa')
            return original(**request)
        patch.object(ec2, 'run_instances', side_effect=launch).start()
        instances, candidate, plan = self.allocate()
        pending_etag = self.campaign.journal.data['activation']['pending_etag']
        cfg = self.configure_fixture(instances, candidate, plan)
        envelope = json.loads(self.aws.s3.objects[RUN + '/inputs/controller.json'])
        self.assertEqual(envelope['state'], 'ready')
        self.assertEqual(ci.cloud_bootstrap.activation_config(plan['bootstrap'], envelope), cfg)
        self.assertNotEqual(self.aws.s3.etags[RUN + '/inputs/controller.json'], pending_etag)
        self.assertEqual(len({h['instance_id'] for h in [cfg['fleet']['coordinator'], *cfg['fleet']['nodes']]}), 6)
        self.assertEqual(legacy.Ledger.reservations[-1][2]['ttl_seconds'], 10800)

    def test_unknown_empty_launch_never_falls_back_and_cleanup_stays_incomplete(self):
        self.aws.ec2['us-east-1'].outcomes = ['unknown-empty']
        with self.assertRaisesRegex(RuntimeError, 'outcome unknown'):
            self.allocate()
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])
        restarted = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock)
        with self.assertRaisesRegex(RuntimeError, 'never allocate again'):
            restarted.allocate(self.admission, self.preflight)
        self.assertFalse(restarted.cleanup()['cleanup_complete'])

    def test_unknown_six_is_reconciled_and_exact_owned_cleanup_remains_complete(self):
        self.aws.ec2['us-east-1'].outcomes = ['unknown-six']
        instances, _, _ = self.allocate()
        self.assertEqual(len(instances), 6)
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_capacity_fallback_cannot_extend_absolute_deadline(self):
        self.aws.ec2['us-east-1'].outcomes = ['capacity']
        started = self.clock.time()
        _, candidate, plan = self.allocate()
        self.assertEqual(candidate['region'], 'us-east-2')
        expiry = self.campaign.journal.data['expires_at']
        self.assertEqual(plan['bootstrap']['deadline_utc'], expiry)
        self.assertEqual(ci.core.parsed(expiry).timestamp(), started + 10800)
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_expired_pending_cannot_render_or_allocate_again(self):
        self.allocate()
        plan = copy.deepcopy(self.campaign.journal.data['plan']['bootstrap'])
        plan['deadline_utc'] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError, 'future'):
            ci.cloud_bootstrap.render_userdata(plan)
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)

    def assert_failed_activation_cleans_fleet(self, failure):
        patch.object(self.campaign, 'configure', side_effect=failure).start()
        retrieve = patch.object(self.campaign, 'retrieve', return_value={}).start()
        settle = patch.object(self.campaign, 'settle').start()
        with self.assertRaises(type(failure)):
            self.campaign.run(self.admission, self.preflight)
        self.assertEqual(len(self.aws.ec2['us-east-1'].instances), 6)
        self.assertTrue(all(i['State']['Name'] == 'terminated' for i in self.aws.ec2['us-east-1'].instances))
        retrieve.assert_called_once()
        self.assertTrue(settle.call_args.args[0]['cleanup_complete'])

    def test_activation_failure_always_terminates_all_six(self):
        self.assert_failed_activation_cleans_fleet(ValueError('activation CAS rejected'))

    def test_github_cancellation_always_terminates_all_six(self):
        self.assert_failed_activation_cleans_fleet(InterruptedError('GitHub cancel'))

    def test_one_termination_failure_does_not_skip_remaining_hosts(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        ec2.fail_termination.add(ec2.instances[0]['InstanceId'])
        cleanup = self.campaign.cleanup()
        self.assertFalse(cleanup['cleanup_complete'])
        self.assertEqual(len(ec2.terminations), 6)
        self.assertEqual(sum(i['State']['Name'] == 'terminated' for i in ec2.instances), 5)

    def test_lost_pending_cas_never_overwrites_an_existing_ready_record(self):
        _, _, plan = self.allocate()
        key = RUN + '/inputs/controller.json'
        self.aws.s3.objects[key] = b'{"state":"ready"}'
        before = self.aws.s3.objects[key]
        with self.assertRaisesRegex(ValueError, 'never overwrite ready'):
            self.campaign.pending_activation(plan['bootstrap'])
        self.assertEqual(self.aws.s3.objects[key], before)

    def test_ready_cas_rejects_concurrent_activation_change_then_cleanup_works(self):
        instances, candidate, plan = self.allocate()
        self.aws.s3.etags[RUN + '/inputs/controller.json'] = 'concurrent-change'
        with self.assertRaises(legacy.ServiceError):
            self.configure_fixture(instances, candidate, plan)
        cleanup = self.campaign.cleanup()
        self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
        self.assertTrue(any(item['kind'] == 'placement-group' and item['verified_absent']
                            for item in cleanup['auxiliary']))

    def test_storage_can_head_own_receipt_but_not_another_owner(self):
        instances, candidate, _ = self.allocate()
        policy = ci.guest_policy(self.cfg, RUN, 'storage-2', region=candidate['region'],
            expires_at=self.campaign.journal.data['expires_at'], instances=instances)
        def allowed(action, key):
            arn = f"arn:aws:s3:::{self.cfg['bucket']}/{key}"
            return any(action in statement['Action'] and any(fnmatch.fnmatchcase(arn, resource)
                for resource in ([statement['Resource']] if isinstance(statement['Resource'], str) else statement['Resource']))
                for statement in policy['Statement'])
        own = RUN + '/results/cells/startup/storage/2/bootstrap.json'
        self.assertTrue(allowed('s3:GetObject', own))
        self.assertTrue(allowed('s3:PutObject', own))
        self.assertFalse(allowed('s3:GetObject', own.replace('/2/', '/3/')))
        self.assertFalse(allowed('s3:PutObject', RUN + '/inputs/controller.json'))


class ResultAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.cfg = ci.load_config(ci.HERE / 'ci-config.json', ENV)
        self.campaign = object.__new__(ci.Campaign)
        self.campaign.cfg, self.campaign.run_id, self.campaign.output = self.cfg, RUN, self.output
        self.raw = self.output / 'raw'
        self.raw.mkdir()
        self.jobs = ci.cloud_campaign.make_matrix(self.cfg['spec'])
        ci.core.dump(self.raw / 'matrix.json', dict(schema_version=1, spec=self.cfg['spec'], jobs=self.jobs))
        ci.core.dump(self.raw / 'observability/receipt.json', dict(complete=False, attempted=True))

    def export(self, **changes):
        base = self.output / 's3' / RUN / 'results'
        base.mkdir(parents=True)
        archive = base / 'controller.tar.gz'
        records = ci.cloud_controller.archive_results(self.raw, archive)
        receipt = dict(status='incomplete', files=records, bytes=archive.stat().st_size,
            sha256=ci.core.sha(archive), monitoring_complete=False)
        receipt.update(changes)
        ci.core.dump(base / 'receipt.json', receipt)
        return archive

    def test_all_684_unrun_cases_remain_incomplete_with_monitor_failure_separate(self):
        self.export()
        with self.assertRaisesRegex(RuntimeError, 'incomplete campaign'):
            self.campaign.validate_results({'spec': self.cfg['spec']})
        result = ci.core.read_json(self.output / 'campaign-validation.json')
        self.assertEqual((result['planned_cases'], result['completed_cases']), (684, 0))
        self.assertFalse(result['benchmark_complete'])
        self.assertFalse(result['monitoring_complete'])
        self.assertFalse(result['complete'])
        report = ci.core.read_json(self.output / 'validated-results/comparison.json')
        self.assertEqual({case['case_id'] for case in report['cases']}, {job['case_id'] for job in self.jobs})

    def test_monitor_failure_cannot_turn_successful_benchmark_green(self):
        self.export(status='completed', monitoring_complete=True)
        with patch.object(ci.cloud_campaign, 'report', return_value=dict(complete=True, completed_cases=684)):
            with self.assertRaisesRegex(RuntimeError, 'incomplete campaign'):
                self.campaign.validate_results({'spec': self.cfg['spec']})
        outcome = ci.core.read_json(self.output / 'campaign-validation.json')
        self.assertTrue(outcome['benchmark_complete'])
        self.assertFalse(outcome['monitoring_complete'])
        self.assertFalse(outcome['complete'])

    def test_narrowed_export_matrix_rejected_even_with_terminal_success_claim(self):
        ci.core.dump(self.raw / 'matrix.json', dict(schema_version=1, spec=self.cfg['spec'], jobs=self.jobs[:-1]))
        self.export(status='completed', monitoring_complete=True)
        with self.assertRaisesRegex(ValueError, '684'):
            self.campaign.validate_results({'spec': self.cfg['spec']})

    def test_changed_raw_archive_rejected_before_reporting(self):
        archive = self.export()
        with archive.open('ab') as stream:
            stream.write(b'changed')
        with patch.object(ci.cloud_campaign, 'report') as report:
            with self.assertRaisesRegex(ValueError, 'durable export'):
                self.campaign.validate_results({'spec': self.cfg['spec']})
            report.assert_not_called()


class FailureFinalizationTests(unittest.TestCase):
    def test_retrieval_failure_still_settles_cleanup_evidence(self):
        campaign = Mock()
        campaign.cleanup.return_value = {'cleanup_complete': False}
        campaign.retrieve.side_effect = OSError('raw download unavailable')
        with self.assertRaises(OSError):
            ci.cleanup_campaign(campaign)
        campaign.settle.assert_called_once_with({'cleanup_complete': False})

    def test_cleanup_failure_is_not_success(self):
        campaign = Mock()
        campaign.cleanup.return_value = {'cleanup_complete': False}
        self.assertFalse(ci.cleanup_campaign(campaign))
        campaign.retrieve.assert_called_once()
        campaign.settle.assert_called_once()


if __name__ == '__main__':
    unittest.main()
