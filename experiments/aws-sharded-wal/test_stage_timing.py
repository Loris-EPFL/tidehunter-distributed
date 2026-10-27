"""No-cloud stage campaign consumer-boundary and owner-cleanup regressions."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

import analyze_cloud as audit
import control
import guest
import prepare_bundle
import stage_timing as profile
import transport_floor
from test_transport_floor import floor_files
from test_scalar_transport import save, table
from test_control import FakeMcp


def configuration():
    run = 'th-' + 'a' * 16
    identities = {label: dict(source_tree_sha256=('b' if label == 'baseline' else 'c') * 64,
        source_manifest_sha256='d' * 64, source_archive_sha256='e' * 64,
        local_binary_sha256='f' * 64, local_floor_binary_sha256='1' * 64,
        protocol=version) for label, version in profile.PROTOCOLS.items()}
    identities['baseline'].update(source_tree_sha256=profile.BASELINE_SOURCE_SHA256,
        local_binary_sha256=profile.BASELINE_BINARY_SHA256,
        local_floor_binary_sha256=transport_floor.CURRENT_FLOOR_BINARY_SHA256)
    return dict(campaign='stage-timing', run_id=run, account=control.ACCOUNT, region='us-east-1',
        instance_type='c6id.32xlarge', availability_zone='us-east-1a', placement_group=run,
        placement_strategy='cluster', ttl_seconds=1500, export_reserve_seconds=300,
        checkpoint_scope='full', repetitions=3, terminate_on_finish=True, efa=True,
        source_identities=identities, coordinator_instance_id='i-' + '0' * 16 + '1',
        nodes=[dict(partition=p, instance_id='i-' + f'{p + 2:017x}', private_ip=f'10.0.0.{p + 2}') for p in range(5)],
        bucket=control.ACCOUNT + '-' + run, s3_prefix=run, run_root='/mnt/tidehunter/' + run,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(seconds=1470)).isoformat())


def inventory(cfg):
    tags = control.aws_tags(control.ownership(cfg['run_id']))
    ids = [cfg['coordinator_instance_id']] + [n['instance_id'] for n in cfg['nodes']]
    instances = [dict(InstanceId=instance, InstanceType=cfg['instance_type'], Tags=copy.deepcopy(tags),
        Placement={'AvailabilityZone': cfg['availability_zone'], 'GroupName': cfg['placement_group']},
        NetworkInterfaces=[{'InterfaceType': 'efa'}], PrivateIpAddress=f'10.0.0.{index + 1}',
        State={'Name': 'running'}, ClientToken=cfg['run_id'] + ('-controller' if index == 0 else '-node'),
        LaunchTime=datetime.now(timezone.utc).isoformat()) for index, instance in enumerate(ids)]
    groups = [dict(GroupName=cfg['placement_group'], Strategy='cluster', State='available', Tags=tags)]
    return instances, groups


def stages(enabled=True, transport='tcp', samples=200, value=1024, writes=False):
    result = dict(requests=samples if enabled else 0, control_queue_ns=0, control_queue_samples=0,
        control_execution_ns=0, control_execution_samples=0, append_inventory_residence_ns=0,
        storage_write={key: 0 for key in profile.STORAGE_FIELDS},
        client={'ram_payload_verify_ns': samples if enabled else 0,
                'ram_payload_verify_calls': samples if enabled else 0,
                'ram_payload_verified_bytes': samples * value if enabled else 0},
        polling=dict(progress_sweeps=samples if enabled else 0, request_first_poll_samples=samples if enabled else 0,
                     idle_wait_calls=0, idle_wait_ns=0, scheduler_yields=0))
    for owner in ('transport', 'client_transport'):
        result[owner] = {key: dict(ns=samples if enabled else 0, calls=samples if enabled else 0,
                                 bytes=samples * value if enabled else 0, errors=0) for key in profile.WORK_FIELDS}
        result[owner].update({key: 0 for key in profile.PROVIDER_FIELDS})
        if enabled and transport == 'efa':
            result[owner].update(cq_timed_calls=samples, send_post_calls=samples, receive_copy_calls=samples)
    if writes:
        result.update(control_queue_samples=1, control_execution_samples=1)
        result['storage_write'].update(append_calls=1, appended_groups=1, appended_receipts=1,
            append_total_ns=5, append_wal_ns=2, persist_calls=1, durable_publications=1,
            persisted_receipts=1, file_persist_calls=1, file_persist_successes=1,
            persist_total_ns=5, persist_wal_file_ns=3)
    return result


class StageTimingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_targeted_matrix_controls_and_historical_plan_stay_exact(self):
        cases = profile.plan()
        self.assertEqual(len(cases), 52)
        self.assertEqual(len(profile.expected_cases()), 68)
        self.assertEqual(len({s['case_id'] for s in cases}), 52)
        self.assertEqual(sum(len(s['modes']) for s in cases if s['workload'] in ('core', 'scalar')), 36)
        self.assertEqual(sum(len(s['modes']) for s in cases if s['diagnostics']), 12)
        self.assertEqual(sum(bool(s.get('floor')) for s in cases), 16)
        self.assertFalse(any(s.get('recovery') for s in cases))
        for s in cases:
            if s['workload'] in ('core', 'scalar'):
                self.assertEqual(s['reads'], 4096)
                self.assertEqual(s['batch'] * s['batches'], 10240 if s['workload'] == 'core' else 320)
                self.assertFalse(s['diagnostics'])
                self.assertIn(s['native_control_id'], profile.expected_cases())
            if s['diagnostics']:
                self.assertTrue(s['cold'])
            if s.get('floor') and s['build'] == 'baseline':
                self.assertEqual((s['samples'], s['floor_repetitions'], s['floor_values'], s['idle_us'], s['profile']),
                                 (1000, 3, [1024, 8192], 0, False))
                self.assertIn('--transport', profile.flags(s, 'tcp', prepare=True))
                self.assertNotIn('--tcp-polling', profile.flags(s, 'tcp', prepare=True))
                self.assertEqual(profile.floor_flags(s)[-4:], ['--warmup', '64', '--shards', '5'])
        self.assertEqual(len(transport_floor.expected_cases()), 216)
        self.assertEqual(transport_floor.CURRENT_SOURCE_SHA256,
                         '4facb657c3b84720991cd02f18a97b3a53934e3edd1c8b49a5c139616d582239')
        with self.assertRaises(ValueError):
            profile.plan('core')

    def test_actual_placement_group_type_zone_and_ownership_are_required(self):
        cfg = configuration()
        fleet, groups = inventory(cfg)
        profile.validate_placement(cfg, fleet, groups)
        for field, value in [('GroupName', 'foreign'), ('Strategy', 'spread'), ('State', 'pending'), ('Tags', [])]:
            changed = copy.deepcopy(groups)
            changed[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                profile.validate_placement(cfg, fleet, changed)
        for change in ({'InstanceType': 'g6.16xlarge'}, {'Placement': {'AvailabilityZone': 'us-east-1b', 'GroupName': cfg['run_id']}},
                       {'Placement': {'AvailabilityZone': 'us-east-1a', 'GroupName': 'elsewhere'}}, {'NetworkInterfaces': []}):
            changed = copy.deepcopy(fleet)
            changed[-1].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                profile.validate_placement(cfg, changed, groups)
        save(self.root / 'campaign-config.json', cfg)
        (self.root / 'owned-instances.json').write_text(json.dumps(fleet))
        (self.root / 'placement-groups.json').write_text(json.dumps(groups))
        audit.validate_progress_inventory(self.root)
        groups[0]['Strategy'] = 'spread'
        (self.root / 'placement-groups.json').write_text(json.dumps(groups))
        with self.assertRaises(ValueError):
            audit.validate_progress_inventory(self.root)

    def test_control_preflight_and_guest_admission_reject_ignored_placement(self):
        cfg = configuration()
        fleet, groups = inventory(cfg)
        plan = cfg | {'az': cfg['availability_zone'], 'prefix': cfg['run_id']}
        actual, _ = control.fleet_configuration(plan, fleet, groups)
        self.assertEqual(actual['placement_strategy'], 'cluster')
        self.assertEqual(actual['export_reserve_seconds'], 300)
        guest.validate_cfg(actual)
        with self.assertRaises(ValueError):
            control.fleet_configuration(plan, fleet)
        campaign = object.__new__(guest.Campaign)
        campaign.cfg, campaign.results = cfg, self.root
        campaign.ids = [i['InstanceId'] for i in fleet]
        campaign.ec2 = Mock()
        campaign.ec2.describe_instances.return_value = {'Reservations': [{'OwnerId': cfg['account'], 'Instances': fleet}]}
        campaign.ec2.describe_placement_groups.return_value = {'PlacementGroups': groups}
        campaign.check_owned_instances()
        groups[0]['Strategy'] = 'spread'
        with self.assertRaises(ValueError):
            campaign.check_owned_instances()
        campaign.check_owned_instances(record=False)
        fleet[0]['Tags'] = []
        with self.assertRaisesRegex(ValueError, 'foreign'):
            campaign.check_owned_instances(record=False)

    def test_captured_durable_prefix_can_sync_multiple_wal_files_once(self):
        measured = stages(writes=True)
        measured['storage_write'].update(
            appended_groups=2, append_calls=2, appended_receipts=2, persisted_receipts=2,
            file_persist_calls=2, file_persist_successes=2,
            namespace_persist_calls=1, namespace_persist_successes=1)
        profile.validate_stages(measured, 'tcp', True, writes=True)
        measured['storage_write'].update(namespace_persist_calls=2, namespace_persist_successes=2)
        with self.assertRaises(ValueError):
            profile.validate_stages(measured, 'tcp', True, writes=True)
        measured['storage_write'].update(namespace_persist_calls=1, namespace_persist_successes=1)
        measured['storage_write']['file_persist_successes'] = 3
        with self.assertRaises(ValueError):
            profile.validate_stages(measured, 'tcp', True, writes=True)

    def test_stage_fields_counts_and_nested_scope_are_fail_closed(self):
        for transport in ('tcp', 'efa'):
            good = stages(transport=transport, writes=True)
            profile.validate_stages(good, transport, True, writes=True)
            for owner, field in [('transport', 'tls_process'), ('client_transport', 'cq_timed_calls'),
                                 ('storage_write', 'persist_wal_file_ns'), ('client', 'ram_payload_verify_calls')]:
                bad = copy.deepcopy(good)
                del bad[owner][field]
                with self.subTest(transport=transport, field=field), self.assertRaises(ValueError):
                    profile.validate_stages(bad, transport, True, writes=True)
            for owner, field, value in [('storage_write', 'persist_calls', 0), ('storage_write', 'append_calls', True),
                                       ('storage_write', 'append_wal_ns', 6), ('storage_write', 'namespace_persist_calls', 2),
                                       ('transport', 'cq_dispatch_errors', 1), ('transport', 'send_post_errors', 9999)]:
                bad = copy.deepcopy(good)
                bad[owner][field] = value
                with self.subTest(field=field), self.assertRaises(ValueError):
                    profile.validate_stages(bad, transport, True, writes=True)
        disabled = stages(enabled=False)
        profile.validate_stages(disabled, 'tcp', False)
        disabled['client']['ram_payload_verify_calls'] = 1
        with self.assertRaises(ValueError):
            profile.validate_stages(disabled, 'tcp', False)
        idle = stages()
        with self.assertRaisesRegex(ValueError, 'durable write'):
            profile.validate_stages(idle, 'tcp', True, writes=True)
        for transport in ('tcp', 'efa'):
            idle = stages(enabled=False, transport=transport)
            self.assertEqual(profile.validate_stages(idle, transport, True)['requests'], 0)
            idle['requests'] = 1
            with self.assertRaisesRegex(ValueError, 'frame encode/decode'):
                profile.validate_stages(idle, transport, True)

    def make_floor(self, spec):
        path = self.root / spec['case_id']
        path.mkdir()
        floor_files(path, spec)
        if spec['build'] == 'current':
            for value in spec['floor_values']:
                for rep in range(spec['floor_repetitions']):
                    evidence = path / f'evidence-v{value}-r{rep}.yaml'
                    docs = transport_floor.yaml_one(evidence.read_text())
                    docs[2] = [stages(spec['profile'], spec['transport'], (spec['samples'] + 4 - p) // 5, value)
                               for p in range(5)]
                    evidence.write_text(json.dumps(docs))
        return path

    def test_floor_exact_payload_samples_and_stage_fields(self):
        spec = next(s for s in profile.plan() if s.get('floor') and s['build'] == 'current' and s['profile'])
        path = self.make_floor(spec)
        metrics = profile.read_floor_metrics(path, spec)
        self.assertEqual(metrics['floor', 'ram-v1024-r0', 'samples'], 1000)
        evidence = path / 'evidence-v1024-r0.yaml'
        original = transport_floor.yaml_one(evidence.read_text())
        for field, value in [('ram_payload_verify_calls', 199), ('ram_payload_verified_bytes', 1024),
                             ('ram_payload_verify_ns', -1)]:
            changed = copy.deepcopy(original)
            changed[2][0]['client'][field] = value
            evidence.write_text(json.dumps(changed))
            with self.subTest(field=field), self.assertRaises(ValueError):
                profile.read_floor_metrics(path, spec)
        evidence.write_text(json.dumps(original))
        summaries = audit.cluster_ram.read_csv(path / 'summary.csv')
        table(path / 'summary.csv', summaries[:-1])
        with self.assertRaises(ValueError):
            profile.read_floor_metrics(path, spec)

    def test_read_diagnostic_sample_count_and_order(self):
        spec = next(s for s in profile.plan() if s['diagnostics'])
        for mode, filename, reopened in [('tcp', 'remote_read_profiles.csv', 'reopened-cache-cleared'),
                                         ('native-sync', 'native_read_profiles.csv', 'reopened-warm')]:
            rows = [dict(state=state, sample=sample) for state in ('warm', reopened, 'cache-evicted') for sample in range(256)]
            fields = ('index_ns', 'wal_and_decode_ns', 'whole_call_ns', 'dispatch_ns', 'wait_ns',
                      'actor_queue_ns', 'actor_service_ns', 'actor_return_ns', 'request_encode_ns',
                      'exchange_ns', 'response_decode_ns', 'record_verify_ns', 'server_handler_ns',
                      'server_inventory_wait_ns', 'server_authority_lookup_ns', 'server_record_read_ns', 'inline_read_ns')
            rows = [row | {field: 1 for field in fields} for row in rows]
            table(self.root / filename, rows)
            profile.validate_read_samples(self.root, spec, mode)
            table(self.root / filename, rows[:-1])
            with self.assertRaises(ValueError):
                profile.validate_read_samples(self.root, spec, mode)
            rows[0]['sample'] = 1
            table(self.root / filename, rows)
            with self.assertRaises(ValueError):
                profile.validate_read_samples(self.root, spec, mode)

    def test_summaries_include_denominators_and_never_invent_zero_latency(self):
        spec = next(s for s in profile.plan() if s.get('floor') and s['build'] == 'current')
        name = spec['case_id'] + '-floor'
        metrics = {('floor_profile', 'ram-v1024-r0', 'partition0.client.ram_payload_verify_ns'): 0,
                   ('floor_profile', 'ram-v1024-r0', 'partition0.client.ram_payload_verify_calls'): 0}
        runs = {name: (profile.expected_cases()[name], metrics)}
        report = profile.summarize_stages(runs)
        self.assertEqual(report[0]['samples'], 0)
        self.assertIsNone(report[0]['mean_ns'])
        del metrics['floor_profile', 'ram-v1024-r0', 'partition0.client.ram_payload_verify_calls']
        with self.assertRaises(ValueError):
            profile.summarize_stages(runs)

    def test_current_only_summary_preserves_main_shapes_and_excludes_qualification(self):
        runs = {}
        expected = profile.expected_cases()
        for spec in profile.plan():
            if spec.get('floor') or spec['diagnostics']:
                continue
            if spec['workload'] != 'qual' and (spec['transport'] != 'tcp' or spec['value'] != 1024):
                continue
            for mode in spec['modes']:
                name = spec['case_id'] + '-' + mode
                value = (10 if spec['batch'] == 1 else 100) * (spec['rep'] + 1)
                if mode == 'native-sync':
                    value /= 2
                runs[name] = (expected[name], {('summary', 'point_get', 'p50_ns'): value})
        summary = profile.summarize(profile.compare(runs))
        self.assertEqual(
            {(row['batch_size'], row['window'], row['backend']): row['new_median'] for row in summary},
            {(1, 1, 'tcp'): 20, (1, 1, 'native-sync'): 10,
             (32, 32, 'tcp'): 200, (32, 32, 'native-sync'): 100})
        self.assertTrue(all(row['new_repetitions'] == 3 and row['new_over_old'] is None for row in summary))

    def test_profile_summary_never_pools_scalar_and_window_shapes(self):
        runs = {}
        for batch, window, total in ((1, 1, 1000), (32, 32, 9000)):
            name = f'diag-{batch}-{window}'
            info = ('stage-diag-efa', 'current', 8192, batch, window, 0, 'tcp', name)
            metrics = {('profile', 'warm', 'whole_call_ns_sum'): total,
                       ('profile', 'warm', 'whole_call_ns_n'): 10,
                       ('profile', 'warm', 'whole_call_ns_p50'): total / 10}
            runs[name] = (info, metrics)
        summary = audit.summarize_profiles(runs)
        self.assertEqual(
            {(row['batch_size'], row['window']):
             (row['diagnostic_samples'], row['metrics']['whole_call_ns']['mean_ns']) for row in summary},
            {(1, 1): (10, 100), (32, 32): (10, 900)})

    def test_recheck_summary_uses_timed_samples_not_api_rows(self):
        info = ('read-recheck', 'current', 8192, 1, 1, 0, 'tcp', 'diag')
        metrics = {
            ('profile', 'warm', 'whole_call_ns_sum'): 1000,
            ('profile', 'warm', 'whole_call_ns_n'): 10,
            ('profile', 'warm', 'whole_call_ns_p50'): 100,
            ('profile', 'warm', 'server_response_recheck_wait_ns_sum'): 80,
            ('profile', 'warm', 'server_response_recheck_wait_ns_n'): 10,
            ('profile', 'warm', 'server_response_recheck_wait_ns_p50'): 8,
            ('profile', 'warm', 'server_response_recheck_samples_sum'): 2,
        }
        runs = {'diag': (info, metrics)}
        result = audit.summarize_profiles(runs)[0]['metrics']['server_response_recheck_wait_ns']
        self.assertEqual(result['mean_ns'], 40)
        self.assertEqual(result['samples'], 2)
        self.assertEqual(result['denominator_field'], 'server_response_recheck_samples_sum')
        stages = {row['field']: row for row in profile.summarize_stages(runs)}
        self.assertEqual(stages['server_response_recheck_wait_ns_sum']['mean_ns'], 40)
        metrics['profile', 'warm', 'server_response_recheck_samples_sum'] = 0
        metrics['profile', 'warm', 'server_response_recheck_wait_ns_sum'] = 0
        result = audit.summarize_profiles(runs)[0]['metrics']['server_response_recheck_wait_ns']
        self.assertIsNone(result['mean_ns'])
        self.assertIsNone(result['fraction_of_summed_whole'])
        del metrics['profile', 'warm', 'server_response_recheck_samples_sum']
        with self.assertRaises(ValueError):
            audit.summarize_profiles(runs)
        # Frozen profiles without the new timer remain valid, without invented zeros.
        for key in list(metrics):
            if key[2].startswith('server_response_recheck_'):
                del metrics[key]
        self.assertNotIn('server_response_recheck_wait_ns', audit.summarize_profiles(runs)[0]['metrics'])

    def test_output_and_authority_residence_use_event_denominators(self):
        info = ('read-recheck', 'current', 8192, 1, 1, 0, 'tcp', 'diag')
        metrics = {
            ('node_profile', 'read', 'partition0.requests'): 100,
            ('node_profile', 'read', 'partition0.read_response_recheck_wait_ns'): 60,
            ('node_profile', 'read', 'partition0.read_response_recheck_ns'): 12,
            ('node_profile', 'read', 'partition0.read_response_recheck_samples'): 3,
            ('node_profile', 'read', 'partition0.transport.output_queue_ns'): 80,
            ('node_profile', 'read', 'partition0.transport.output_queue_samples'): 4,
            ('node_profile', 'read', 'partition0.transport.output_submit_ns'): 24,
            ('node_profile', 'read', 'partition0.transport.output_submit_samples'): 2,
            ('node_profile', 'read', 'partition0.transport.output_retire_ns'): 0,
            ('node_profile', 'read', 'partition0.transport.output_retire_samples'): 0,
        }
        runs = {'diag': (info, metrics)}
        result = {row['field']: row for row in profile.summarize_stages(runs)}
        self.assertEqual(result['partition0.read_response_recheck_wait_ns']['mean_ns'], 20)
        self.assertEqual(result['partition0.read_response_recheck_ns']['mean_ns'], 4)
        self.assertEqual(result['partition0.transport.output_queue_ns']['mean_ns'], 20)
        self.assertEqual(result['partition0.transport.output_submit_ns']['mean_ns'], 12)
        self.assertIsNone(result['partition0.transport.output_retire_ns']['mean_ns'])
        del metrics['node_profile', 'read', 'partition0.transport.output_queue_samples']
        with self.assertRaises(ValueError):
            profile.summarize_stages(runs)

    def test_unavailable_timers_are_not_reported_as_zero_cost(self):
        expected = profile.expected_cases()
        wal = next(s for s in profile.plan() if s['diagnostics'] and s['transport'] == 'tcp')
        floor = next(s for s in profile.plan() if s.get('floor') and s['build'] == 'current')
        wal_name, floor_name = wal['case_id'] + '-tcp', floor['case_id'] + '-floor'
        runs = {
            wal_name: (expected[wal_name], {
                ('exchange_profile', 'warm', 'request_send_ns_sum'): 0,
                ('exchange_profile', 'warm', 'request_send_ns_n'): 256}),
            floor_name: (expected[floor_name], {
                ('floor_profile', 'ram-v1024-r0', 'partition0.handler_ns'): 0,
                ('floor_profile', 'ram-v1024-r0', 'partition0.requests'): 200,
                ('floor_profile', 'ram-v1024-r0', 'partition0.client.exchange_ns'): 20000,
                ('floor_profile', 'ram-v1024-r0', 'partition0.client.requests'): 200})}
        measured = {row['field']: row for row in profile.summarize_stages(runs)}
        self.assertIsNone(measured['request_send_ns_sum']['mean_ns'])
        self.assertEqual(measured['request_send_ns_sum']['samples'], 256)
        self.assertIsNone(measured['partition0.handler_ns']['mean_ns'])
        self.assertEqual(measured['partition0.client.exchange_ns']['mean_ns'], 100)
        self.assertEqual(measured['partition0.client.exchange_ns']['availability'], 'measured')

    def test_export_failure_and_bad_placement_still_terminate_only_owned_fleet(self):
        cfg = configuration()
        fleet, groups = inventory(cfg)
        groups[0]['Strategy'] = 'spread'
        campaign = object.__new__(guest.Campaign)
        campaign.cfg, campaign.root, campaign.results = cfg, self.root, self.root
        campaign.ids = [i['InstanceId'] for i in fleet]
        campaign.state = {'complete': False, 'failures': [], 'completed_cases': []}
        campaign.active_payload, campaign.cloud_builds = None, {}
        campaign.ec2 = Mock()
        campaign.ec2.describe_instances.return_value = {'Reservations': [{'OwnerId': cfg['account'], 'Instances': fleet}]}
        campaign.ec2.describe_placement_groups.return_value = {'PlacementGroups': groups}
        with patch.object(campaign, 'progress', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                campaign.execute()
        campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)
        self.assertTrue(any('placement' in message for message in campaign.state['failures']))
        campaign.ec2.reset_mock()
        fleet[0]['Tags'] = []
        with patch.object(campaign, 'progress', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(ValueError, 'foreign'):
                campaign.execute()
        campaign.ec2.terminate_instances.assert_not_called()


    def test_current_phase_resource_boundaries_are_required_and_include_control(self):
        spec = next(s for s in profile.plan() if s['build'] == 'current' and s['workload'] == 'core')
        boundaries = ['opened', 'loaded', 'warm-scalar-before', 'warm-scalar-after',
                      'warm-window-before', 'point_get_window', 'warm-window-after',
                      'warm-batched-before', 'warm-batched-after', 'cache_evicted_get_window']
        rows = [dict(stage=stage, role='native', partition='', pid=7, rss_bytes=100,
                     peak_rss_bytes=100, user_cpu_ns=index * 10, system_cpu_ns=0)
                for index, stage in enumerate(boundaries)]
        path = self.root / 'read_resources.csv'
        table(path, rows)
        args = (path, 'native-sync', spec, {'pid': 7}, {'ram_bytes': 10000}, {},
                {'point_get_window': {'phase_wall_ns': 100}})
        metrics = profile.resource_metrics(*args)
        self.assertEqual(metrics['resource', 'warm-window', 'cpu_ns_between_before_after_including_control'], 20)
        table(path, rows[:-1])
        with self.assertRaises(ValueError):
            profile.resource_metrics(*args)


class SourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.payload = self.root / 'payload'
        self.payload.mkdir()
        self.manifests = {}
        for label, protocol in profile.PROTOCOLS.items():
            directory = self.payload / label
            directory.mkdir()
            source = f'const PROTOCOL: u32 = {protocol};\n'.encode()
            files = {'tidehunter/src/distributed/tcp.rs': hashlib.sha256(source).hexdigest()}
            (directory / 'sharded_wal_bench').write_bytes((label + '-bench').encode())
            (directory / 'sharded_wal_transport_bench').write_bytes((label + '-floor').encode())
            manifest = dict(source_sha256=files,
                source_tree_sha256=hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                binary_sha256=prepare_bundle.sha(directory / 'sharded_wal_bench'),
                floor_binary_sha256=prepare_bundle.sha(directory / 'sharded_wal_transport_bench'),
                features='experimental-fabric,test-utils')
            save(directory / 'source-manifest.json', manifest)
            with tarfile.open(directory / 'source.tar.gz', 'w:gz') as archive:
                member = tarfile.TarInfo('tidehunter/src/distributed/tcp.rs')
                member.size = len(source)
                archive.addfile(member, io.BytesIO(source))
            self.manifests[label] = manifest
        for module, key, value in [(profile, 'BASELINE_SOURCE_SHA256', self.manifests['baseline']['source_tree_sha256']),
                                    (profile, 'BASELINE_BINARY_SHA256', self.manifests['baseline']['binary_sha256']),
                                    (transport_floor, 'CURRENT_FLOOR_BINARY_SHA256', self.manifests['baseline']['floor_binary_sha256'])]:
            patcher = patch.object(module, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def bundle(self):
        bundle = self.root / 'bundle.tar.gz'
        with tarfile.open(bundle, 'w:gz') as archive:
            archive.add(self.payload, arcname='bundle')
        save(self.root / 'upload-manifest.json', dict(archive=bundle.name,
            sha256=prepare_bundle.sha(bundle), bytes=bundle.stat().st_size))
        return bundle

    def test_plan_derives_identity_from_uploaded_manifests_and_protocol_bytes(self):
        bundle = self.bundle()
        run_dir = self.root / 'run'
        plan = control.init(run_dir, bundle, campaign='stage-timing', ttl_seconds=1500)
        self.assertEqual(plan['placement_strategy'], 'cluster')
        self.assertEqual(plan['placement_group'], plan['run_id'])
        self.assertEqual(plan['source_identities']['baseline']['protocol'], 5)
        self.assertEqual(plan['source_identities']['current']['protocol'], 6)
        self.assertEqual(plan['source_identities']['current']['source_tree_sha256'], self.manifests['current']['source_tree_sha256'])
        control.Control(run_dir)
        for kwargs in ({'ttl_seconds': 3000}, {'ttl_seconds': 1500, 'efa_instance_type': 'g6.16xlarge'},
                       {'ttl_seconds': 1500, 'checkpoint_scope': 'recovery'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                control.init(self.root / 'invalid', bundle, campaign='stage-timing', **kwargs)
        with self.assertRaises(ValueError):
            profile.source_identity(self.payload / 'baseline', 'current')
        binary = self.payload / 'current/sharded_wal_bench'
        binary.write_bytes(b'not-the-bound-build')
        with self.assertRaises(ValueError):
            profile.bundle_identities(self.bundle())

    def test_uncertain_launch_reconciles_same_owned_fleet_and_cluster_request(self):
        run_dir = self.root / 'run'
        plan = control.init(run_dir, self.bundle(), campaign='stage-timing', ttl_seconds=1500)
        fake = FakeMcp()
        runner = control.Control(run_dir, fake)
        runner.state['resources'].update(security_group='sg-fixture', uploaded_bundle=True,
            bundle_url='https://example.invalid/bundle', bundle_upload={'key': plan['prefix'] + '/bundle.tar.gz'})
        with patch.object(runner, 'presigned', return_value='https://example.invalid/bundle'), \
             patch.object(control, 'user_data', return_value='fixture'):
            with self.assertRaises(TimeoutError):
                runner.launch()
            runner.launch()
        launches = [params for _, operation, params in fake.calls if operation == 'RunInstances']
        self.assertEqual(len(launches), 2)
        self.assertTrue(all(params['Placement']['GroupName'] == plan['run_id'] for params in launches))
        self.assertEqual(len(fake.fleet), 6)
        runner.terminate()
        self.assertTrue(all(i['State']['Name'] == 'terminated' for i in fake.fleet))


if __name__ == '__main__':
    unittest.main()
