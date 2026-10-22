"""Synthetic consumer-boundary evidence; never depends on ignored campaign logs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_cloud as audit
import cluster_ram
import scalar_transport
import transport_floor as profile
from test_scalar_transport import Evidence as ScalarEvidence, recovery_stats, save, table

SHAPE = '''key_spaces:
- inner:
    id: 0
    key_indexing: !Fixed 8
    mutexes: 16
    key_type: !Uniform {cells_per_mutex: 64}
    config: {value_cache_size: 0}
'''


class Evidence(ScalarEvidence):
    def __init__(self, root):
        super().__init__(root)
        self.cfg['campaign'] = 'transport-floor'
        self.cfg['availability_zone'] = 'us-east-1a'
        save(root / 'campaign-config.json', self.cfg)
        self.pins = []
        for label in ('baseline', 'current'):
            files = {'tidehunter/examples/sharded_wal_bench.rs': ('1' if label == 'baseline' else '2') * 64}
            if label == 'current':
                files['tidehunter/examples/sharded_wal_transport_bench.rs'] = '3' * 64
            source = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            pin = patch.object(profile, label.upper() + '_SOURCE_SHA256', source)
            pin.start()
            self.pins.append(pin)
            build = self.builds[label]
            build.update(source_tree_sha256=source, source_archive_sha256='4' * 64,
                         local_binary_sha256=profile.BASELINE_BINARY_SHA256 if label == 'baseline' else profile.CURRENT_BINARY_SHA256)
            build['command'] = ['cargo', '+1.90.0', 'build', '--release', '--locked', '--manifest-path',
                                self.base + f'/build-source/{label}/Cargo.toml', '-p', 'tidehunter',
                                '--features', 'experimental-fabric,test-utils', '--example', 'sharded_wal_bench']
            manifest = dict(source_sha256=files, source_tree_sha256=source, binary_sha256=build['local_binary_sha256'])
            if label == 'current':
                manifest.update(floor_binary_sha256=profile.CURRENT_FLOOR_BINARY_SHA256,
                                recovery_binary_sha256=profile.CURRENT_RECOVERY_BINARY_SHA256)
                build.update(local_floor_binary_sha256=profile.CURRENT_FLOOR_BINARY_SHA256,
                             local_recovery_binary_sha256=profile.CURRENT_RECOVERY_BINARY_SHA256)
            save(root / f'source-manifest-{label}.json', manifest)
            build['source_manifest_sha256'] = hashlib.sha256((root / f'source-manifest-{label}.json').read_bytes()).hexdigest()
            if label == 'current':
                build.update(floor_binary_sha256='5' * 64,
                             floor_command=build['command'][:-1] + ['sharded_wal_transport_bench'])
                save(root / 'build-floor-current.command.json', dict(command=build['floor_command'], exit_code=0))
                save(root / 'capability-floor-current.command.json', dict(exit_code=0,
                     command=[self.base + '/cloud-bin/current/sharded_wal_transport_bench', '--help'],
                     binary_sha256=build['floor_binary_sha256']))
                (root / 'capability-floor-current.stdout.log').write_text('--remote-config --polling --profile --idle-us')
            save(root / f'cloud-build-{label}.json', build)
            save(root / f'build-{label}.command.json', dict(command=build['command'], exit_code=0))
        pair = audit.read_json(root / 'cloud-build-pair-verified.json')
        pair['prepared_configuration_sha256']['shape.yaml'] = '6' * 64
        save(root / 'cloud-build-pair-verified.json', pair)

    def close(self):
        for pin in reversed(self.pins):
            pin.stop()

    def node_members(self, spec, partition):
        members = super().node_members(spec, partition)
        launch = dict(node=dict(partition={'partition': partition}, shape_yaml=SHAPE,
                               read_workers=spec['workers'], wal_member_read=spec['member'],
                               read_cache_bytes=spec['shard_cache'], read_memory_allowance_bytes=spec['shard_allowance']),
                      tls={'transport': spec['transport']}, limits={})
        if spec['build'] == 'current':
            launch['node']['read_resident_first'] = spec['resident_first']
            launch['limits']['polling'] = spec['polling']
        import yaml
        members['launch.yaml'] = yaml.safe_dump(launch)
        return members

    def prepare(self, spec):
        super().prepare(spec)
        case = spec['case_id']
        path = self.root / (case + '-prepare.command.json')
        receipt = audit.read_json(path)
        receipt['command'] = [self.binary(spec['build']), '--prepare-external-case', self.base + '/cases/' + case,
                              '--guest-case-root', self.base + '/cases/' + case, '--remote-endpoints', self.endpoints]
        receipt['command'] += profile.flags(spec, 'tcp', prepare=True)
        save(path, receipt)
        remote = dict(read_lanes=spec['lanes'], read_cache_bytes=spec['cache'], read_memory_allowance_bytes=spec['read_allowance'],
                      tls={'transport': spec['transport']}, storage={'partitions': list(range(5))},
                      endpoints=[dict(partition=n['partition'], address=n['private_ip'] + ':7700') for n in self.cfg['nodes']])
        (self.root / (case + '-remote.yaml')).write_text(json.dumps(remote))
        if spec['build'] == 'current':
            (self.root / (case + '-shape.yaml')).write_text(SHAPE)

    def floor(self, spec):
        self.prepare(spec)
        name = spec['case_id'] + '-floor'
        path = self.root / name
        path.mkdir()
        floor_files(path, spec)
        command = [self.base + '/cloud-bin/current/sharded_wal_transport_bench', '--output',
                   self.base + '/results/' + name, '--remote-config', self.base + '/cases/' + spec['case_id'] + '/remote.yaml']
        command += profile.floor_flags(spec)
        self.process(name, spec, command)
        process = audit.read_json(self.root / (name + '-process.command.json'))
        save(self.root / (name + '-process.command.json'), process | {'binary_sha256': self.builds['current']['floor_binary_sha256']})
        self.controller(name)
        return name

    def benchmark(self, spec, mode='tcp'):
        name = super().benchmark(spec, mode)
        path = self.root / name
        summary = cluster_ram.read_csv(path / 'summary.csv')
        samples = cluster_ram.read_csv(path / 'samples.csv')
        phases = {'point_get_missing', 'exists', 'exists_missing', 'checkpoint_capture', 'post_flush_get'}
        phases |= ({'cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'} if spec['cold']
                   else {'point_get_hot', 'point_get_many_hot'})
        existing = {r['phase'] for r in summary}
        for phase in phases - existing:
            summary.append(summary[0] | {'phase': phase})
            samples.append(samples[0] | {'phase': phase})
        read_phases = phases - {'checkpoint_capture', 'post_flush_get'} | {'point_get_many'}
        for row in summary:
            if row['phase'] in read_phases:
                row.update(logical_operations=spec['reads'], instrumented_phase_ops_per_sec=spec['reads'] * 1e6)
            phase = row['phase']
            row['sample_unit'] = ('operation' if phase.endswith('_get_window') else 'complete_window' if phase.endswith('_window')
                                  else 'complete_get_many' if 'get_many' in phase else 'whole_phase')
        for row in samples:
            if row['phase'] in read_phases:
                row['logical_operations'] = spec['reads']
        table(path / 'summary.csv', summary)
        table(path / 'samples.csv', samples)
        rows = cluster_ram.read_csv(path / 'read_path_evidence.csv')
        for row in rows:
            for key in ('resident_read_attempts', 'resident_read_successes', 'resident_read_fallbacks',
                        'resident_read_unsupported', 'resident_read_partial', 'resident_read_deferred',
                        'async_read_submissions'):
                row[key] = 0
            if row['role'] == 'storage' and row['boundary'] == 'after':
                row['resident_read_attempts' if spec['resident_first'] else 'async_read_submissions'] = 1
        table(path / 'read_path_evidence.csv', rows)
        return name

    def recovery(self, spec):
        name = super().recovery(spec)
        # The new baseline is the former current and contains lifetime counters.
        if spec['build'] == 'baseline':
            for action in ('seed', 'reopen'):
                path = self.root / name / action / 'recovery.json'
                row = audit.read_json(path)
                row['recovery_stats'] = recovery_stats(spec | {'build': 'current'}, action == 'reopen')
                save(path, row)
        return name


def floor_files(path, spec):
    (path / 'COMPLETE').write_text('verified\n')
    metadata = dict(transport=spec['transport'], polling=spec['polling'].title(), instrumented=str(spec['profile']).lower(),
                    topology='external_nodes', shards=5, timed_scope='authenticated_exchange_decode_and_full_payload_validation',
                    wall_scope='measured_requests_and_offered_idle_including_scheduler_delay',
                    wal_io='checked_zero_member_reads', physical_network_latency='not_isolated', external_cleanup='operator_owned', argv='[]')
    if spec['transport'] in ('efa-native', 'fabric-tcp-native'):
        metadata['timed_scope'] = 'trusted_native_exchange_decode_and_full_payload_validation'
    (path / 'metadata.txt').write_text(''.join(f'{k}={v}\n' for k, v in metadata.items()))
    samples, summary = [], []
    for rep in range(spec['floor_repetitions']):
        for value in spec['floor_values']:
            n = spec['samples']
            samples.extend(dict(value_bytes=value, repetition=rep, sample=i, partition_index=i % 5, api_ns=100 + i) for i in range(n))
            wall = sum(100 + i for i in range(n)) + n * spec['idle_us'] * 1000 + 1000
            summary.append(dict(transport=spec['transport'], polling=spec['polling'].title(), instrumented=str(spec['profile']).lower(),
                                shards=5, value_bytes=value, repetition=rep, idle_us=spec['idle_us'], samples=n, wall_ns=wall,
                                p50_ns=100 + (n * 50 + 99) // 100 - 1, p95_ns=100 + (n * 95 + 99) // 100 - 1,
                                p99_ns=100 + (n * 99 + 99) // 100 - 1, max_ns=99 + n, phase_ops_per_sec=n * 1e9 / wall))
            counters = [dict(wal_reads=0, wal_read_bytes=0, resident_read_attempts=0, async_read_submissions=0) for _ in range(5)]
            profiles = [dict(requests=(n + 4 - p) // 5, polling=dict(progress_sweeps=1000, request_first_poll_samples=(n + 4 - p) // 5,
                idle_wait_calls=0, idle_wait_ns=0, scheduler_yields=0, busy_spin_hints=1000)) for p in range(5)]
            if spec['transport'] in ('efa-native', 'fabric-tcp-native'):
                for row in profiles:
                    for owner in ('transport', 'client_transport'):
                        row[owner] = {name: dict(ns=0, calls=0, bytes=0, errors=0)
                                      for name in ('tls_read', 'tls_write', 'tls_process')}
            save(path / f'evidence-v{value}-r{rep}.yaml', [counters, counters, profiles])
    table(path / 'samples.csv', samples)
    table(path / 'summary.csv', summary)


class PlanAndComparisonTests(unittest.TestCase):
    def test_bounded_scopes_and_no_synthetic_native_controls(self):
        for scope, specs, apis in [('full', 164, 216), ('core', 148, 200), ('recovery', 20, 20)]:
            with self.subTest(scope=scope):
                selected = profile.plan(scope)
                self.assertEqual(len(selected), specs)
                self.assertEqual(len(profile.expected_cases(scope)), apis)
                self.assertEqual(len({s['case_id'] for s in selected}), specs)
        expected = profile.expected_cases()
        for spec in profile.main_cases():
            if spec.get('floor'):
                self.assertEqual(spec['floor_values'], [1024, 8192])
                self.assertEqual(spec['floor_repetitions'], 3)
                self.assertEqual((spec['samples'], spec['profile']), (256, True) if spec['idle_us'] else (1000, False))
                continue
            self.assertIn(spec['native_control_id'], expected)
            if (spec['polling'], spec['resident_first']) != ('adaptive', False):
                self.assertEqual(spec['modes'], ['tcp'])
                self.assertEqual(spec['build'], 'current')
        with self.assertRaises(ValueError):
            profile.plan('capacity')
        self.assertEqual(profile.BASELINE_SOURCE_SHA256, scalar_transport.CURRENT_SOURCE_SHA256)
        self.assertNotEqual(profile.BASELINE_BINARY_SHA256, scalar_transport.BASELINE_BINARY_SHA256)

    def test_ratios_use_measured_matching_control_and_never_pool_transports(self):
        spec = next(s for s in profile.main_cases() if s['workload'] == 'scalar' and s['polling'] == 'busy' and s['resident_first'])
        expected = profile.expected_cases()
        name, control = spec['case_id'] + '-tcp', spec['native_control_id']
        metric = ('summary', 'point_get', 'p50_ns')
        runs = {name: (expected[name], {metric: 30}), control: (expected[control], {metric: 3})}
        wrong_control = control.replace('-tcp-', '-efa-')
        runs[wrong_control] = expected[wrong_control], {metric: 1000}
        row = next(r for r in profile.compare(runs) if r['backend'] == 'tcp')
        self.assertEqual(row['new_over_native_barrier'], 10)
        self.assertEqual(row['native_control_ids'], [control])
        self.assertEqual((row['polling'], row['resident_first']), ('busy', True))
        del runs[control]
        row = next(r for r in profile.compare(runs) if r['backend'] == 'tcp')
        self.assertIsNone(row['new_over_native_barrier'])
        self.assertEqual(row['native_control_ids'], [])



class CloudEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.evidence = Evidence(self.root)
        self.addCleanup(self.evidence.close)

    def read(self, name):
        return audit.read_case(self.root, name, profile.expected_cases()[name], 'transport-floor')

    def inventory(self, **config):
        cfg = self.evidence.cfg | {'availability_zone': 'us-east-1b'} | config
        save(self.root / 'campaign-config.json', cfg)
        nodes = {n['instance_id']: n['private_ip'] for n in cfg['nodes']}
        tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': cfg['run_id']}
        instances = [
            {'InstanceId': instance, 'InstanceType': cfg['instance_type'],
             'Placement': {'AvailabilityZone': cfg['availability_zone']},
             'NetworkInterfaces': [{'InterfaceType': 'efa'}],
             'Tags': [{'Key': key, 'Value': value} for key, value in tags.items()],
             'PrivateIpAddress': nodes.get(instance, '172.31.0.1')}
            for instance in [cfg['coordinator_instance_id'], *nodes]]
        save(self.root / 'owned-instances.json', instances)
        return instances

    def test_inventory_accepts_complete_selected_reviewed_hardware(self):
        for instance_type in ('g6.16xlarge', 'c6id.32xlarge'):
            with self.subTest(instance_type=instance_type):
                self.inventory(instance_type=instance_type)
                audit.validate_progress_inventory(self.root)

    def test_inventory_rejects_one_other_reviewed_type(self):
        for selected_type, other_type in (('g6.16xlarge', 'c6id.32xlarge'),
                                           ('c6id.32xlarge', 'g6.16xlarge')):
            for index in range(6):
                with self.subTest(instance_type=selected_type, instance=index):
                    instances = self.inventory(instance_type=selected_type)
                    instances[index]['InstanceType'] = other_type
                    save(self.root / 'owned-instances.json', instances)
                    with self.assertRaisesRegex(ValueError, 'ownership/type differs'):
                        audit.validate_progress_inventory(self.root)

    def test_inventory_rejects_unreviewed_selected_hardware(self):
        self.inventory(instance_type='c6id.16xlarge')
        with self.assertRaisesRegex(ValueError, 'reviewed real-EFA hardware/zone'):
            audit.validate_progress_inventory(self.root)

    def test_inventory_requires_every_host_in_the_planned_zone(self):
        for instance_type in ('g6.16xlarge', 'c6id.32xlarge'):
            with self.subTest(instance_type=instance_type):
                instances = self.inventory(instance_type=instance_type)
                instances[-1]['Placement']['AvailabilityZone'] = 'us-east-1a'
                save(self.root / 'owned-instances.json', instances)
                with self.assertRaisesRegex(ValueError, 'reviewed real-EFA hardware/zone'):
                    audit.validate_progress_inventory(self.root)

    def test_inventory_keeps_efa_and_ownership_gates_for_selected_c6id(self):
        for field, value in (('NetworkInterfaces', [{'InterfaceType': 'interface'}]),
                             ('Tags', []), ('PrivateIpAddress', '172.31.0.254')):
            with self.subTest(field=field):
                instances = self.inventory(instance_type='c6id.32xlarge')
                instances[-1][field] = value
                save(self.root / 'owned-instances.json', instances)
                with self.assertRaises(ValueError):
                    audit.validate_progress_inventory(self.root)

    def test_older_inventory_profiles_reject_selected_c6id(self):
        for campaign_name in ('checkpoint-async', 'cluster-ram', 'scalar-transport'):
            with self.subTest(campaign=campaign_name):
                self.inventory(campaign=campaign_name, instance_type='g6.16xlarge',
                               availability_zone='us-east-1a')
                audit.validate_progress_inventory(self.root)
                self.inventory(campaign=campaign_name, instance_type='c6id.32xlarge',
                               availability_zone='us-east-1a')
                with self.assertRaisesRegex(ValueError, 'reviewed real-EFA hardware/zone'):
                    audit.validate_progress_inventory(self.root)

    def test_campaign_hardware_policy_keeps_qualification_mandatory(self):
        for campaign_profile, campaign_name, repetitions in (
                (profile, 'transport-floor', 3), (scalar_transport, 'scalar-transport', 3),
                (cluster_ram, 'cluster-ram', 5), (audit.checkpoint_async, 'checkpoint-async', 5)):
            for instance_type in ('g6.16xlarge', 'c6id.32xlarge', 'c6id.16xlarge'):
                with self.subTest(campaign=campaign_name, instance_type=instance_type):
                    cfg = self.evidence.cfg | dict(campaign=campaign_name,
                        instance_type=instance_type, repetitions=repetitions)
                    save(self.root / 'campaign-config.json', cfg)
                    reviewed = instance_type == 'g6.16xlarge' or (
                        campaign_profile is profile and instance_type == 'c6id.32xlarge')
                    error = 'hardware qualification receipt/evidence mismatch' if reviewed else 'hardware/repetition/lifetime'
                    with self.assertRaisesRegex(ValueError, error):
                        audit.validate_checkpoint_campaign(self.root, {}, [], {'passed': False},
                                                           profile=campaign_profile)

    def test_all_floor_policies_parse_six_measurements_and_raw_counters(self):
        for spec in [s for s in profile.main_cases() if s.get('floor')]:
            with self.subTest(transport=spec['transport'], polling=spec['polling'], idle=spec['idle_us']):
                name = self.evidence.floor(spec)
                metrics = self.read(name)
                self.assertEqual(len({phase for kind, phase, _ in metrics if kind == 'floor'}), 6)
                self.assertEqual(metrics['floor_io', 'ram-v1024-r2', 'partition4.after.wal_reads'], 0)
                self.assertGreater(metrics['floor', 'ram-v8192-r0', 'p99_ns'], 0)
                info = profile.expected_cases()[name]
                rows = [r for r in profile.compare({name: (info, metrics)}) if r['kind'] == 'floor']
                self.assertEqual({r['value_bytes'] for r in rows}, {1024, 8192})
                self.assertTrue(all(r['new_repetitions'] == 3 and r['phase'] == 'ram_response' for r in rows))

    def test_native_ram_rejects_authentication_mislabel_and_observed_tls_work(self):
        spec = profile.subsuite_floor_spec('efa-native', True) | dict(idle_us=0)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            floor_files(directory, spec)
            metrics = profile.read_floor_metrics(directory, spec)
            self.assertEqual(metrics['floor', 'ram-v8192-r0', 'p50_ns'], 227)
            metadata = directory / 'metadata.txt'
            original = metadata.read_text()
            metadata.write_text(original.replace(
                'trusted_native_exchange_decode', 'authenticated_exchange_decode'))
            with self.assertRaises(ValueError):
                profile.read_floor_metrics(directory, spec)
            metadata.write_text(original)
            path = directory / 'evidence-v8192-r0.yaml'
            original = profile.yaml_one(path.read_text())
            for owner in ('transport', 'client_transport'):
                changed = copy.deepcopy(original)
                changed[2][0][owner]['tls_process']['calls'] = 1
                save(path, changed)
                with self.subTest(owner=owner), self.assertRaises(ValueError):
                    profile.read_floor_metrics(directory, spec)
            save(path, original)

    def test_ram_control_checks_interval_io_not_prior_wal_activity(self):
        spec = next(s for s in profile.main_cases() if s.get('floor'))
        name = self.evidence.floor(spec)
        directory = self.root / name
        for path in directory.glob('evidence-*.yaml'):
            evidence = profile.yaml_one(path.read_text())
            for boundary in evidence[:2]:
                for partition in boundary:
                    partition['wal_reads'] = 17
                    partition['wal_read_bytes'] = 4096
            save(path, evidence)
        metrics = profile.read_floor_metrics(directory, spec)
        self.assertEqual(metrics['floor_io', 'ram-v1024-r0', 'partition0.after.wal_reads'], 17)
        path = directory / 'evidence-v1024-r0.yaml'
        evidence = profile.yaml_one(path.read_text())
        evidence[1][0]['wal_reads'] += 1
        save(path, evidence)
        with self.assertRaisesRegex(ValueError, 'measured interval'):
            profile.read_floor_metrics(directory, spec)

    def test_wal_resident_activity_and_read_phase_counts_are_not_optional(self):
        for resident in (False, True):
            spec = next(s for s in profile.main_cases() if s['workload'] == 'scalar' and s['build'] == 'current'
                        and s['polling'] == 'adaptive' and s['resident_first'] == resident)
            name = self.evidence.benchmark(spec)
            metrics = self.read(name)
            key = 'storage-resident_read_attempts' if resident else 'storage-async_read_submissions'
            self.assertEqual(metrics['read_path', 'point_get', key], 5)
            path = self.root / name / 'read_path_evidence.csv'
            original = cluster_ram.read_csv(path)
            changed = copy.deepcopy(original)
            for row in changed:
                row['resident_read_attempts' if resident else 'async_read_submissions'] = '0'
            table(path, changed)
            with self.assertRaisesRegex(ValueError, 'selected resident/async'):
                self.read(name)
            table(path, original)
            path = self.root / name / 'summary.csv'
            summary = cluster_ram.read_csv(path)
            table(path, [r for r in summary if r['phase'] != 'exists_missing'])
            with self.assertRaises(ValueError):
                self.read(name)

    def test_floor_raw_samples_summary_and_authenticated_policy_tampering(self):
        spec = next(s for s in profile.main_cases() if s.get('floor') and s['profile'] and s['polling'] == 'busy')
        name = self.evidence.floor(spec)
        self.read(name)
        for filename, field, value in [('samples.csv', 'partition_index', '4'), ('samples.csv', 'sample', '1'),
                                       ('summary.csv', 'p99_ns', '1'), ('summary.csv', 'samples', '255'),
                                       ('summary.csv', 'polling', 'Adaptive'), ('summary.csv', 'instrumented', 'false'),
                                       ('summary.csv', 'phase_ops_per_sec', 'nan')]:
            path = self.root / name / filename
            original = cluster_ram.read_csv(path)
            rows = copy.deepcopy(original)
            rows[0][field] = value
            table(path, rows)
            with self.subTest(file=filename, field=field), self.assertRaises(ValueError):
                self.read(name)
            table(path, original)
        path = self.root / name / 'evidence-v1024-r0.yaml'
        original = profile.yaml_one(path.read_text())
        for section, field, value in [(0, 'wal_reads', 1), (1, 'wal_read_bytes', 1), (1, 'wal_reads', True)]:
            changed = copy.deepcopy(original)
            changed[section][0][field] = value
            save(path, changed)
            with self.subTest(section=section, field=field), self.assertRaises(ValueError):
                self.read(name)
        changed = copy.deepcopy(original)
        changed[2][0]['polling']['scheduler_yields'] = 1
        save(path, changed)
        with self.assertRaises(ValueError):
            self.read(name)
        save(path, original)
        metadata = self.root / name / 'metadata.txt'
        metadata.write_text(metadata.read_text().replace('polling=Busy', 'polling=Adaptive'))
        with self.assertRaises(ValueError):
            self.read(name)

    def test_floor_missing_repetition_or_payload_completion_fails(self):
        spec = next(s for s in profile.main_cases() if s.get('floor'))
        name = self.evidence.floor(spec)
        self.read(name)
        path = self.root / name / 'summary.csv'
        original = cluster_ram.read_csv(path)
        table(path, original[:-1])
        with self.assertRaisesRegex(ValueError, 'missing floor'):
            self.read(name)
        table(path, original)
        (self.root / name / 'COMPLETE').unlink()
        with self.assertRaisesRegex(ValueError, 'COMPLETE'):
            self.read(name)

    def test_source_process_and_launch_identity_chain_rejects_tampering(self):
        spec = next(s for s in profile.main_cases() if s.get('floor'))
        name = self.evidence.floor(spec)
        self.read(name)
        for filename, key in [('cloud-build-current.json', 'source_tree_sha256'),
                              ('cloud-build-current.json', 'floor_binary_sha256'),
                              ('source-manifest-current.json', 'binary_sha256'),
                              ('cloud-build-current.json', 'local_binary_sha256'),
                              ('cloud-build-current.json', 'local_floor_binary_sha256'),
                              ('cloud-build-current.json', 'local_recovery_binary_sha256'),
                              (name + '-process.command.json', 'binary_sha256'),
                              (spec['case_id'] + '-prepare.command.json', 'binary_sha256')]:
            path = self.root / filename
            original = audit.read_json(path)
            save(path, original | {key: '9' * 64})
            with self.subTest(file=filename), self.assertRaises(ValueError):
                self.read(name)
            save(path, original)
        members = self.evidence.node_members(spec, 0)
        start = json.loads(members['start.json'])
        self.evidence.archive(spec, 0, members | {'start.json': json.dumps(start | {'binary_sha256': '9' * 64})})
        with self.assertRaisesRegex(ValueError, 'node executable'):
            self.read(name)
        self.evidence.archive(spec, 0, members)
        changed = profile.yaml_one(members['launch.yaml'])
        changed['limits']['polling'] = 'busy'
        self.evidence.archive(spec, 0, members | {'launch.yaml': json.dumps(changed)})
        with self.assertRaisesRegex(ValueError, 'polling/resident'):
            self.read(name)
        self.evidence.archive(spec, 0, members)
        (self.root / (spec['case_id'] + '-shape.yaml')).write_text('key_spaces: [changed]\n')
        with self.assertRaisesRegex(ValueError, 'shape differs'):
            self.read(name)

    def test_recovery_public_service_format_and_fresh_lifetime_for_both_builds(self):
        for spec in profile.recovery_cases():
            with self.subTest(build=spec['build'], transport=spec['transport'], restart=spec['restart'], tail=spec['tail_batches']):
                name = self.evidence.recovery(spec)
                service = audit.read_json(self.root / name / 'service-restore.json')
                self.assertNotIn('transport', service)
                metrics = self.read(name)
                self.assertEqual(metrics['recovery', 'reopen', 'stats_created_connections'], 10)
                self.assertEqual(metrics['recovery', 'reopen', 'stats_tail_batches'], spec['tail_batches'])
                path = self.root / name / 'reopen/recovery.json'
                row = audit.read_json(path)
                save(path, row | {'recovery_stats': row['recovery_stats'] | {'created_connections': 0, 'reused_connections': 10}})
                with self.assertRaisesRegex(ValueError, 'fresh transports'):
                    self.read(name)

    def test_archive_digest_and_immutable_scope_rejected(self):
        spec = next(s for s in profile.main_cases() if s.get('floor'))
        name = self.evidence.floor(spec)
        self.read(name)
        receipt = self.root / 'nodes' / spec['case_id'] / 'node-0.tar.gz.receipt.json'
        original = audit.read_json(receipt)
        save(receipt, original | {'sha256': '0' * 64})
        with self.assertRaisesRegex(ValueError, 'scoped receipt'):
            self.read(name)
        save(receipt, original)
        save(self.root / 'campaign-config.json', self.evidence.cfg | {'checkpoint_scope': 'recovery'})
        with self.assertRaises(ValueError):
            self.read(name)
        with self.assertRaises(ValueError):
            audit.node_evidence(self.root, spec['case_id'], 0)

    def test_yaml_duplicate_keys_and_unsafe_tags_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            profile.yaml_one('wal_reads: 0\nwal_reads: 1\n')
        with self.assertRaises(ValueError):
            profile.yaml_one('!!python/object/apply:os.system [false]')
        self.assertEqual(profile.yaml_one('index: !Fixed 8\ntype: !Uniform {cells_per_mutex: 64}\n'),
                         {'index': ('Fixed', 8), 'type': ('Uniform', {'cells_per_mutex': 64})})


if __name__ == '__main__':
    unittest.main()
