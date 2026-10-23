"""Consumer-facing workload matching and cache/device classification; no cloud calls."""
import copy
import csv
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import checkpoint_kv as kv
from guest import benchmark_case_identity, kv_case_commands


class MatchedReads(unittest.TestCase):
    def config(self):
        return dict(run_root='/frozen', source_archive_sha256='a' * 64,
                    binaries={'storage-node': 'b' * 64},
                    region='fixture-region', availability_zone='fixture-zone',
                    instance_type='fixture-host', placement_group='fixture-cluster',
                    nodes=[dict(partition=p, private_ip=f'10.0.1.{p + 1}') for p in range(5)])

    def resident_limits(self):
        return dict(source_bytes=4 * 1024**2, source_regions=512,
            destination_bytes=20 * 1024**2, destination_slots=640, max_read_bytes=32768,
            max_handles=4096, require_device_read=False)

    def specimen(self, batches=320):
        return dict(shape=[8192, 32, 32], repetition=0, transport='tcp',
                    cached=False, cold=False, diagnostics=False, authority_resident_batches=None,
                    workload_batches=batches, workload_reads=8192, wal_layout='small',
                    shard_cache_bytes=0, preparation_chunk_batches=16)

    def observed(self, root, batches=320, resident=False, mode='tcp'):
        cfg, spec = self.config(), self.specimen(batches)
        if resident:
            spec.update(transport='efa-native', resident_reads=self.resident_limits())
        prepare, commands = kv_case_commands(cfg, spec)
        directory = Path(root) / Path(commands[mode][commands[mode].index('--output') + 1]).name
        directory.mkdir()
        metadata = (
            f'load_batches={batches}\nmixed_batches=64\npoint_reads=8192\n'
            'read_query_algorithm=mix64-v1\nread_query_seed=0\n'
            'read_query_order=mix(sample)%loaded_keys\nread_prepared_state=post-load-before-any-read\n'
            'wal_fragment_bytes=1048576\nwal_file_bytes=134217728\n'
            'wal_group_max_batches=32\nwal_group_encoded_bytes=1048568\none_sided_wal_reads=false\n'
            'target_os=linux\ntarget_arch=x86_64\nread_cpu_affinity=0-7\nread_numa_affinity=0\n'
            'read_placement_policy=kernel-scheduled-inherited-affinity\n'
            'build_features=experimental-fabric:true,test-utils:true\ntopology=explicit_external_tcp_nodes\n'
            'native_read_cache_bytes=0\n'
            'registered_hot_policy={"enabled":false,"hot_keys":32,"scalar_reads":512,"group_width":32,"group_calls":16,"read_lanes":2,"max_warm_passes":5,"required_steady_passes":2,"scalar_order":"mix64-v1(sample)%32","group_order":"unique-prefix-keys-0-through-31","preparation":"post-load-before-other-reads;exact-scalar-then-group-sequence"}\n'
            'rma_config={"source_bytes":0,"source_regions":0,"destination_bytes":0,"destination_slots":0,"max_read_bytes":0,"max_handles":0,"require_device_read":false}\n')
        if resident:
            fields = dict(line.split('=', 1) for line in metadata.splitlines())
            fields['one_sided_wal_reads'] = str(mode == 'tcp').lower()
            if mode == 'tcp':
                fields['rma_config'] = json.dumps(self.resident_limits())
            warm = json.loads(fields['registered_hot_policy'])
            warm['enabled'] = mode == 'tcp'
            warm['preparation'] = 'post-load-before-other-reads;fixed-passes;exact-scalar-then-group-sequence'
            fields['registered_hot_policy'] = json.dumps(warm)
            fields['bounded_hot_policy'] = json.dumps(self.bounded_policy(8192))
            fields['read_prepared_state'] = 'post-load-after-bounded-hot32-controls'
            if mode == 'native-sync':
                fields['topology'] = 'native_local'
            metadata = ''.join(f'{key}={value}\n' for key, value in fields.items())
        (directory / 'campaign.txt').write_text(metadata)
        (directory.parent / (directory.name + '-process.command.json')).write_text(json.dumps(
            dict(exit_code=0, binary_sha256='b' * 64, command=commands[mode])))
        case_id = directory.name.removesuffix('-' + mode)
        (directory.parent / (case_id + '-prepare.command.json')).write_text(json.dumps(
            dict(exit_code=0, binary_sha256='b' * 64, command=prepare)))
        remote, _ = kv.kv_case_policy(cfg, spec)
        (directory.parent / (case_id + '-remote.yaml')).write_text(json.dumps(remote))
        with (directory / 'summary.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=['phase', 'logical_operations', 'value_bytes', 'batch_size', 'samples'])
            writer.writeheader()
            writer.writerow(dict(phase='load_write_window', logical_operations=batches * 32,
                                 value_bytes=8192, batch_size=32, samples=batches // 32))
            writer.writerow(dict(phase='point_get', logical_operations=8192, value_bytes=8192,
                                 batch_size=32, samples=8192))
        return directory, spec, cfg

    def test_historical_and_current_working_sets_have_distinct_commands_and_ids(self):
        cases = kv.plan(dict(scope='targeted', repetitions=1, diagnostics=False), ['tcp'])
        controls = [case for case in cases if case['shape'] == [8192, 32, 32]
                    and not case['cached'] and not case['cold'] and not case['shard_cache_bytes']
                    and case['preparation_chunk_batches'] == 16 and case['wal_layout'] == 'small']
        self.assertEqual({case['workload_batches'] for case in controls}, {320, 3200})
        names = set()
        for case in controls:
            prepare, commands = kv_case_commands(self.config(), case)
            names.add(commands['tcp'][commands['tcp'].index('--output') + 1])
            self.assertEqual(prepare[prepare.index('--batches') + 1], str(case['workload_batches']))
        self.assertEqual(len(names), 2)
        for batches in (320, 3200):
            identity, _ = benchmark_case_identity('current', [8192, 32, 32], 0,
                                                  storage=True, workload_batches=batches)
            self.assertTrue(identity.startswith(f'load{batches}-'))

    def test_actual_cardinality_mismatch_blocks_ratios(self):
        with tempfile.TemporaryDirectory() as root:
            left = kv.comparison_identity(*self.observed(root, 320)[:2], 'tcp', self.config())
            right = kv.comparison_identity(*self.observed(root, 3200)[:2], 'tcp', self.config())
            self.assertEqual(left['workload']['loaded_payload_bytes'], 80 * 1024**2)
            self.assertEqual(right['workload']['loaded_payload_bytes'], 800 * 1024**2)
            matched = kv.match_comparison(left, right)
            self.assertFalse(matched['ratios_allowed'])
            self.assertIn('workload', matched['mismatched_fields'])

    def test_source_binary_provenance_is_not_an_equality_requirement_for_code_ab(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            left = kv.comparison_identity(directory, spec, 'tcp', cfg)
            right = copy.deepcopy(left)
            right['implementation'] = dict(binary_sha256='c' * 64, source_archive_sha256='d' * 64)
            matched = kv.match_comparison(left, right)
            self.assertTrue(matched['comparable'])
            self.assertEqual(matched['provenance']['candidate'], right['implementation'])
            right['transport']['name'] = 'fabric-tcp-native'
            self.assertFalse(kv.match_comparison(left, right)['comparable'])
            self.assertTrue(kv.match_comparison(left, right, compared=('implementation', 'transport'))['comparable'])

    def test_each_matching_control_is_required(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            left = kv.comparison_identity(directory, spec, 'tcp', cfg)
            for section in ('workload', 'policy', 'preparation', 'clocks', 'transport', 'environment'):
                with self.subTest(section=section):
                    right = copy.deepcopy(left)
                    right[section]['changed'] = 1
                    self.assertFalse(kv.match_comparison(left, right)['comparable'])

    def test_missing_historical_metadata_is_unqualified_not_current_default(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            path = directory / 'campaign.txt'
            path.write_text(path.read_text().replace('read_query_seed=0\n', ''))
            row = kv.comparison_identity(directory, spec, 'tcp', cfg)
            self.assertFalse(row['qualified'])
            self.assertIn('read_query_seed', row['missing'])
            self.assertFalse(kv.match_comparison(row, row)['comparable'])

    def test_observed_summary_must_prove_load_and_read_count(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            path = directory / 'summary.csv'
            path.write_text(path.read_text().replace('10240', '1024'))
            with self.assertRaisesRegex(ValueError, 'actual loaded/read'):
                kv.comparison_identity(directory, spec, 'tcp', cfg)

    def test_shard_hot_set_cannot_be_swallowed_by_coordinator_cache(self):
        spec = self.specimen() | dict(shape=[8192, 1, 1], shard_cache_bytes=8 * 1024**2)
        prepare, commands = kv_case_commands(self.config(), spec)
        self.assertEqual(prepare[prepare.index('--tcp-read-cache-bytes') + 1], '0')
        self.assertEqual(prepare[prepare.index('--tcp-shard-cache-bytes') + 1], str(8 * 1024**2))
        self.assertEqual(commands['tcp'][commands['tcp'].index('--cache-probe-keys') + 1], '256')
        self.assertNotIn('--transport-diagnostics', commands['tcp'])

    def test_diagnostic_twin_uses_same_workload_and_preparation(self):
        clean = self.specimen()
        diagnostic = clean | dict(diagnostics=True, repetition=1)
        for key in ('--batches', '--reads', '--mixed', '--values', '--batch-sizes', '--windows', '--read-window'):
            left, right = [kv_case_commands(self.config(), row)[1]['tcp'] for row in (clean, diagnostic)]
            self.assertEqual(left[left.index(key) + 1], right[right.index(key) + 1])
        self.assertNotIn('--transport-diagnostics', left)
        self.assertIn('--transport-diagnostics', right)

    def test_targeted_planner_has_full_oracle_twins_for_both_bulk_footprints(self):
        planned = kv.plan(dict(scope='targeted', repetitions=1, diagnostics=True), ['tcp'])
        diagnostics = [row for row in planned if row['diagnostics']]
        bulk = [row for row in diagnostics if row['shape'] == [8192, 32, 32]]
        self.assertEqual({row['workload_batches'] for row in bulk}, {320, 3200})
        controls = ('shape', 'transport', 'cold', 'cached', 'shard_cache_bytes', 'wal_layout',
                    'preparation_chunk_batches', 'workload_batches', 'workload_reads')
        for twin in diagnostics:
            self.assertTrue(any(not row['diagnostics'] and
                all(row.get(key) == twin.get(key) for key in controls) for row in planned))

    def test_async_and_mixed_cache_are_not_misclassified(self):
        owners = {'0': dict(cache_hits=0, wal_reads=64, resident_read_successes=0, async_read_submissions=64)}
        row = kv.classify_read_population(dict(cache_hits=0, cache_misses=64), owners, 0)
        self.assertFalse(row['physical_nvme'])
        self.assertFalse(row['physical_io_observed'])
        row = kv.classify_read_population(dict(cache_hits=1, cache_misses=63), owners, 4096)
        self.assertEqual(row['population'], 'mixed_coordinator_cache')
        self.assertFalse(row['hit_only'])
        self.assertFalse(row['advisory_eviction_proves_cold'])
        row = kv.classify_read_population(dict(cache_hits=0, cache_misses=64),
                                          {'0': dict(cache_hits=64, wal_reads=0)}, 0)
        self.assertEqual(row['population'], 'shard_cache_hit_only')
        row = kv.classify_read_population(None, None, None)
        self.assertEqual(row['population'], 'unqualified_missing_counters')

    def test_resident_opt_in_preserves_matrix_size_and_disables_cold_handles(self):
        config = self.resident_limits()
        options = dict(scope='targeted', repetitions=1, diagnostics=False)
        clean = kv.plan(options, ['efa-native'])
        resident = kv.plan(options | dict(resident_reads=config), ['efa-native'])
        self.assertEqual(len(clean), len(resident))
        self.assertTrue(all('resident_reads' not in row for row in resident if row['cold']))
        spec = next(row for row in resident if not row['cold'] and not row['cached'])
        prepare, commands = kv_case_commands(self.config(), spec)
        self.assertIn('--rma-max-handles', prepare)
        self.assertEqual(prepare[prepare.index('--rma-source-bytes') + 1], str(4 * 1024**2))
        self.assertNotIn('--rma-source-bytes', commands['native-sync'])
        # The client consumes the source-bound prepared YAML; no ignored CLI owner override.
        self.assertNotIn('--rma-source-bytes', commands['tcp'])
        remote, owners = kv.kv_case_policy(self.config(), spec)
        self.assertEqual(remote['tls']['resident_reads'], config)
        self.assertTrue(all(owner['node']['resident_reads'] == config for owner in owners.values()))
        with self.assertRaises(ValueError):
            kv_case_commands(self.config(), spec | dict(cold=True))

    def resident_snapshots(self, root, acquisitions=0):
        counters = ('message_read_requests', 'grants_issued', 'grant_acquisitions', 'eligible_reads', 'rma_posts',
                    'rma_completions', 'rma_hits', 'rma_errors', 'fallback_missing',
                    'fallback_oversize', 'fallback_capacity', 'fallback_unsupported', 'fallback_stale')
        gauges = ('source_bytes_live', 'source_regions_live', 'source_bytes_quarantined',
                  'destination_slots_live', 'handles_live', 'grants_live')
        rows = []
        for boundary in ('before', 'after'):
            owners = []
            for partition in range(5):
                stats = dict.fromkeys(counters + gauges, 0)
                if boundary == 'after':
                    stats.update(eligible_reads=1, rma_posts=1, rma_completions=1,
                                 rma_hits=1, grant_acquisitions=acquisitions)
                owners.append([partition, stats])
            rows.append(dict(phase='point_get_hot', boundary=boundary,
                scope='coordinator_local_per_owner_connection_counters', owners=owners))
        path = Path(root) / 'resident_reads.jsonl'
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        return path, rows

    def registered_control_fixture(self, root, reads=32):
        """Protocol fixture, not performance evidence: scalar and unique32 groups."""
        directory = Path(root)
        scalar = min(512, max(32, reads))
        group_calls = (scalar + 31) // 32
        spec = self.specimen() | dict(transport='efa-native', workload_reads=reads,
                                     resident_reads=self.resident_limits())
        policy = dict(enabled=True, hot_keys=32, scalar_reads=scalar, group_width=32,
            group_calls=group_calls, read_lanes=2, max_warm_passes=5, required_steady_passes=2,
            scalar_order='mix64-v1(sample)%32', group_order='unique-prefix-keys-0-through-31',
            preparation='post-load-before-other-reads;fixed-passes;exact-scalar-then-group-sequence')
        initial = [[owner, lane, dict.fromkeys(kv.REGISTERED_COUNTERS, 0)]
                   for owner in range(5) for lane in range(2)]
        for _, _, counters in initial[:2]:
            counters['grant_acquisitions'] = 32
        def advance(snapshot, changes):
            result = copy.deepcopy(snapshot)
            for lane, groups, elements in changes:
                counters = result[lane][2]
                counters['eligible_reads'] += groups
                counters['rma_hits'] += groups
                counters['rma_posts'] += elements
                counters['rma_completions'] += elements
            return result
        cursor = 0
        def sequence(snapshot, widths):
            nonlocal cursor
            changes = []
            for width in widths:
                changes.append((cursor % 2, 1, width))
                cursor += 1
            return advance(snapshot, changes)
        snapshots = [initial]
        for _ in range(5):
            snapshots.append(sequence(snapshots[-1], [1] * scalar + [32] * group_calls))
        scalar_after = sequence(snapshots[-1], [1] * scalar)
        group_after = sequence(scalar_after, [32] * group_calls)
        def counts(groups, elements):
            return dict.fromkeys(kv.REGISTERED_COUNTERS, 0) | dict(
                eligible_reads=groups, rma_hits=groups, rma_posts=elements, rma_completions=elements)
        identity = dict(backend='trusted_experimental_efa_native_authority', transport='efa-native',
            value_bytes=8192, batch_size=32, window=32, repetition=0)
        def record(phase, observation):
            return identity | dict(phase=phase,
                scope='actual_read_lane_counters;eligible_and_hits_are_groups;posts_and_completions_are_elements',
                source_occupancy_available=False, observation=observation)
        records = [record('registered_hot_setup', dict(kind='preparation', policy=policy,
            status='ready', setup_wall_ns=1000, steady_passes=5,
            expected_active_lanes=[[0, 0], [0, 1]], covered_lanes=[[0, 0], [0, 1]],
            lane_snapshots=snapshots,
            pass_populations=[counts(scalar + group_calls, scalar + 32 * group_calls) for _ in range(5)]))]
        for phase, before, after, groups, elements in (
                ('cache_hot_get', snapshots[-1], scalar_after, scalar, scalar),
                ('cache_hot_get_many', scalar_after, group_after, group_calls, 32 * group_calls)):
            records.append(record(phase, dict(kind='measurement', policy=policy,
                population='registered_hit_only', measurement_phase=phase,
                expected_elements=elements, expected_groups=groups, before=before, after=after,
                counters=counts(groups, elements))))
        path = directory / 'registered_controls.jsonl'
        path.write_text(''.join(json.dumps(row) + '\n' for row in records))
        aggregate_rows = []
        for row in records[1:]:
            for boundary in ('before', 'after'):
                owners = []
                for owner in range(5):
                    counters = {field: sum(lane[2][field] for lane in row['observation'][boundary]
                                          if lane[0] == owner) for field in kv.REGISTERED_COUNTERS}
                    counters.update(grants_issued=0, source_bytes_live=0, source_regions_live=0,
                        source_bytes_quarantined=0, destination_slots_live=0,
                        handles_live=64 if owner == 0 else 0, grants_live=0)
                    owners.append([owner, counters])
                aggregate_rows.append(dict(phase=row['phase'], boundary=boundary, owners=owners,
                    scope='coordinator_local_per_owner_connection_counters'))
        (directory / 'resident_reads.jsonl').write_text(''.join(
            json.dumps(row) + '\n' for row in aggregate_rows))
        self.bounded_control_fixture(directory, spec)
        return spec, path, records

    def bounded_policy(self, reads):
        scalar = min(512, max(32, reads))
        return dict(enabled=True, hot_keys=32, scalar_reads=scalar, group_width=32,
            group_calls=(scalar + 31) // 32, warm_passes=5,
            scalar_order='mix64-v1(sample)%32', group_order='unique-prefix-keys-0-through-31',
            preparation='post-load-before-other-reads;fixed-passes;exact-scalar-then-group-sequence')

    def bounded_control_fixture(self, root, spec, native=False):
        """Consumer protocol fixture; never a measured I/O or latency result."""
        directory = Path(root)
        policy = self.bounded_policy(spec['workload_reads'])
        identity = dict(backend='native_value_wal_barrier' if native else 'trusted_experimental_efa_native_authority',
            transport='tcp' if native else 'efa-native', value_bytes=8192, batch_size=32, window=32, repetition=0)
        def record(phase, observation):
            return identity | dict(phase=phase, policy=policy,
                scope='verified_API_values;RMA_counters_only_in_registered_controls', observation=observation)
        warm_elements = policy['scalar_reads'] + 32 * policy['group_calls']
        records = [record('bounded_hot_setup', dict(kind='preparation', completed_passes=5,
            verified_elements=5 * warm_elements, setup_wall_ns=1000))]
        summary_rows = [identity | dict(phase='bounded_hot_setup', samples=1,
            logical_operations=5 * warm_elements, phase_wall_ns=1000)]
        physical_rows = []
        for phase, samples, elements in (
                ('cache_hot_get', policy['scalar_reads'], policy['scalar_reads']),
                ('cache_hot_get_many', policy['group_calls'], 32 * policy['group_calls'])):
            records.append(record(phase, dict(kind='measurement', measurement_phase=phase,
                samples=samples, verified_elements=elements, physical_io_available=True,
                population='native_API_hotset_unclassified' if native else 'registered_hit_only')))
            summary_rows.append(identity | dict(phase=phase, samples=samples, logical_operations=elements,
                                                phase_wall_ns=1000))
            for owner in ([0] if native else range(5)):
                physical_rows.append({key: value for key, value in identity.items() if key != 'transport'} |
                    dict(phase=phase, partition=owner, pid=1000 + owner, physical_read_bytes=0,
                         minor_faults=0, major_faults=0, file_bytes=1024**3,
                         resident_bytes_before=1024**2, resident_bytes_after=1024**2))
        path = directory / 'bounded_hot_controls.jsonl'
        path.write_text(''.join(json.dumps(row) + '\n' for row in records))
        summary = directory / 'summary.csv'
        existing = []
        if summary.is_file():
            with summary.open() as stream:
                existing = list(csv.DictReader(stream))
        with summary.open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=[*identity, 'phase', 'samples',
                'logical_operations', 'phase_wall_ns'])
            writer.writeheader()
            writer.writerows(existing + summary_rows)
        with (directory / 'read_io.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=physical_rows[0].keys())
            writer.writeheader()
            writer.writerows(physical_rows)
        return path, records

    def test_registered_hit_and_acquisition_populations_are_distinct(self):
        with tempfile.TemporaryDirectory() as root:
            self.resident_snapshots(root)
            row = kv.resident_read_evidence(root)['phases']['point_get_hot']
            self.assertTrue(row['registered_hit_only'])
            self.assertEqual(row['totals']['rma_completions'], 5)
            self.assertFalse(row['source_occupancy_available'])
            self.assertFalse(row['physical_nvme'])
            self.resident_snapshots(root, acquisitions=1)
            row = kv.resident_read_evidence(root)['phases']['point_get_hot']
            self.assertFalse(row['registered_hit_only'])
            self.assertEqual(row['totals']['grant_acquisitions'], 5)

    def test_missing_or_reset_registered_counters_are_not_zero_hits(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertFalse(kv.resident_read_evidence(root)['available'])
            path, rows = self.resident_snapshots(root)
            rows[0]['owners'][0][1]['rma_posts'] = 2
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            with self.assertRaisesRegex(ValueError, 'reset'):
                kv.resident_read_evidence(root)

    def test_authority_metadata_history_is_not_physical_io_and_missing_is_unavailable(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            rows = []
            for boundary in ('before', 'after'):
                after = boundary == 'after'
                rows.append(dict(phase='point_get', role='storage', partition='0', boundary=boundary,
                    cache_hits=0, cache_misses=int(after), wal_reads=int(after),
                    resident_read_successes=int(after), async_read_submissions=0,
                    authority_available='true', authority_resident_hits=3 * int(after),
                    authority_resident_misses=int(after), authority_contention=0,
                    authority_history_loads=int(after), authority_entries=32,
                    authority_capacity=64, authority_table_bytes=4096))
            path = directory / 'read_path_evidence.csv'
            def capture(observed):
                with path.open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(observed[0]))
                    writer.writeheader()
                    writer.writerows(observed)
            capture(rows)
            (directory / 'read_io.csv').write_text('phase,partition,physical_read_bytes\n')
            evidence = kv.read_service_evidence(directory, 'tcp', [0])['point_get']
            self.assertTrue(evidence['owner_authority_available'])
            self.assertEqual(evidence['owner_authority_history_loads'], 1)
            self.assertFalse(evidence['authority_history_loads_prove_device_io'])
            self.assertEqual(evidence['owners']['0']['authority']['entries'], dict(before=32, after=32))
            capture([{key: value for key, value in row.items() if not key.startswith('authority_')}
                     for row in rows])
            missing = kv.read_service_evidence(directory, 'tcp', [0])['point_get']
            self.assertFalse(missing['owner_authority_available'])
            self.assertIsNone(missing['owner_authority_history_loads'])
            self.assertIsNone(missing['owners']['0']['authority'])

    def test_partial_nested_identity_cannot_assert_its_own_qualification(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            complete = kv.comparison_identity(directory, spec, 'tcp', cfg)
            incomplete = copy.deepcopy(complete)
            del incomplete['workload']['loaded_keys']
            result = kv.match_comparison(incomplete, incomplete)
            self.assertFalse(result['comparable'])
            self.assertIn('workload.loaded_keys', result['missing']['reference'])

    def test_detailed_clock_population_cannot_be_called_clean(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            for name in ('read_profiles.csv', 'native_read_profiles.csv',
                         'remote_read_profiles.csv', 'exchange_profiles.csv'):
                (directory / name).write_text('whole_call_ns\n')
            (directory / 'node_profiles.yaml').write_text('')
            kv.require_clean_clocks(directory)
            (directory / 'remote_read_profiles.csv').write_text('whole_call_ns\n100\n')
            with self.assertRaisesRegex(ValueError, 'detailed clocks'):
                kv.require_clean_clocks(directory)

    def test_mixed_transport_plan_keeps_tcp_tls_rpc_controls(self):
        options = dict(scope='targeted', repetitions=1, diagnostics=False)
        transports = ['tcp', 'efa', 'efa-direct', 'efa-native']
        rpc = kv.plan(options, transports)
        resident = kv.plan(options | dict(resident_reads=self.resident_limits()), transports)
        self.assertEqual(len(rpc), len(resident))
        for before, after in zip(rpc, resident, strict=True):
            if before['transport'] != 'efa-native' or before['cold']:
                self.assertEqual(before, after)
                prepare, _ = kv_case_commands(self.config(), after)
                self.assertFalse(any(argument.startswith('--rma-') for argument in prepare))
            else:
                self.assertEqual(after['resident_reads'], self.resident_limits())
                prepare, _ = kv_case_commands(self.config(), after)
                self.assertTrue(Path(prepare[prepare.index('--prepare-external-case') + 1]).name.startswith('rma'))

    def test_registered_probe_requires_real_posts_completions_and_verified_hits(self):
        spec = self.specimen() | dict(transport='efa-native', workload_reads=32,
                                     resident_reads=self.resident_limits())
        empty = dict(available=True, phases={'point_get_hot': dict(totals=dict(
            message_read_requests=4096, rma_posts=0, rma_completions=0, rma_hits=0))})
        with self.assertRaises(ValueError):
            kv.require_registered_probe(spec, 'tcp', empty)
        with tempfile.TemporaryDirectory() as root:
            spec, _, _ = self.registered_control_fixture(root)
            proof = kv.require_registered_probe(spec, 'tcp', kv.resident_read_evidence(root))
            self.assertTrue(proof['qualified'])
            grouped = proof['phases']['cache_hot_get_many']['totals']
            self.assertEqual(grouped['rma_hits'], 1)
            self.assertEqual(grouped['eligible_reads'], 1)
            self.assertEqual(grouped['rma_posts'], 32)
            self.assertEqual(grouped['message_read_requests'], 0)
        self.assertIsNone(kv.require_registered_probe(spec, 'native-sync', empty))

    def test_complete_clean_auxiliary_pair_validates_every_owner_archive(self):
        with tempfile.TemporaryDirectory() as root:
            tcp, spec, cfg = self.observed(root)
            root = Path(root)
            case_id = tcp.name.removesuffix('-tcp')
            _, commands = kv_case_commands(cfg, spec)
            native = root / (case_id + '-native-sync')
            native.mkdir()
            (native / 'campaign.txt').write_text((tcp / 'campaign.txt').read_text().replace(
                'topology=explicit_external_tcp_nodes',
                'topology=one_shared_local_ssd_with_optional_five_child_nodes'))
            (native / 'summary.csv').write_text((tcp / 'summary.csv').read_text())
            (root / (native.name + '-process.command.json')).write_text(json.dumps(dict(
                exit_code=0, binary_sha256='b' * 64, command=commands['native-sync'])))
            read_fields = ('phase', 'role', 'partition', 'boundary', 'cache_hits', 'cache_misses',
                           'wal_reads', 'resident_read_successes', 'async_read_submissions')
            for directory in (native, tcp):
                (directory / 'COMPLETE').touch()
                for name in ('read_profiles.csv', 'native_read_profiles.csv',
                             'remote_read_profiles.csv', 'exchange_profiles.csv'):
                    (directory / name).write_text('whole_call_ns\n')
                (directory / 'node_profiles.yaml').write_text('')
                (directory / 'checkpoint_diagnostics.jsonl').write_text('')
                (directory / 'write_windows.csv').write_text('phase,sample\n')
                (directory / 'read_io.csv').write_text('phase,partition,physical_read_bytes\n')
                (root / (directory.name + '-process.stderr.log')).write_text('')
                with (directory / 'read_path_evidence.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=read_fields)
                    writer.writeheader()
                    if directory == tcp:
                        for boundary in ('before', 'after'):
                            after = boundary == 'after'
                            for partition in range(5):
                                reads = (1640 if partition == 0 else 1638) * int(after)
                                writer.writerow(dict(phase='point_get', role='storage', partition=partition,
                                    boundary=boundary, cache_hits=0, cache_misses=reads, wal_reads=reads,
                                    resident_read_successes=reads, async_read_submissions=0))
                            writer.writerow(dict(phase='point_get', role='frontend', partition='',
                                boundary=boundary, cache_hits=0, cache_misses=8192 * int(after),
                                wal_reads=0, resident_read_successes=0, async_read_submissions=0))
            node_root = root / 'nodes' / case_id
            node_root.mkdir(parents=True)
            for partition in range(5):
                with tarfile.open(node_root / f'node-{partition}.tar.gz', 'w:gz') as archive:
                    member = tarfile.TarInfo('node.stderr.log')
                    member.size = 0
                    archive.addfile(member, io.BytesIO(b''))
            _, variant = benchmark_case_identity('current', spec['shape'], spec['repetition'],
                storage=True, workload_batches=spec['workload_batches'])
            cfg = cfg | dict(kv_suite=dict(scope='targeted', repetitions=1, diagnostics=False),
                transports=['tcp'], binaries={'storage-node': 'b' * 64, 'transport-floor': 'c' * 64})
            state = dict(plan=[spec], binary_sha256='b' * 64, floor_binary_sha256='c' * 64,
                completed_floor_cases=[], completed_cases=[
                    dict(case_id=case_id, variant=variant, backend=mode, build='current',
                         transport='local' if mode == 'native-sync' else 'tcp')
                    for mode in ('native-sync', 'tcp')])
            # Isolate one admitted pair, but execute every applicable auxiliary
            # validator and all five real archive parsers without mocking them.
            with patch.object(kv, 'plan', return_value=[spec]):
                result = kv.validate_auxiliary(root, state, cfg)
            self.assertEqual(len(result['read_service']), 2)
            self.assertTrue(all(row['comparison_identity']['qualified'] for row in result['read_service']))
            owners = [row for row in result['fabric_local_diagnostics'] if row['role'] == 'storage']
            self.assertEqual({row['partition'] for row in owners}, set(range(5)))
            self.assertTrue(all(row['population'] == 'clean' for row in owners))

    def test_comparison_rejects_modified_or_extended_observed_deployment_policy(self):
        for mutation in ('budget', 'unknown'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                directory, spec, cfg = self.observed(root)
                path = directory.parent / (directory.name.removesuffix('-tcp') + '-remote.yaml')
                deployment = json.loads(path.read_text())
                if mutation == 'budget':
                    deployment['read_memory_allowance_bytes'] //= 2
                else:
                    deployment['unadmitted'] = True
                path.write_text(json.dumps(deployment))
                with self.assertRaisesRegex(ValueError, 'effective KV policy'):
                    kv.comparison_identity(directory, spec, 'tcp', cfg)

    def test_rpc_messages_prevent_registered_hit_only_misclassification(self):
        with tempfile.TemporaryDirectory() as root:
            path, rows = self.resident_snapshots(root)
            rows[1]['owners'][0][1]['message_read_requests'] = 2
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            phase = kv.resident_read_evidence(root)['phases']['point_get_hot']
            self.assertEqual(phase['totals']['message_read_requests'], 2)
            self.assertFalse(phase['registered_hit_only'])

    def test_registered_group_counts_groups_separately_from_completed_elements(self):
        counters = dict.fromkeys(kv.REGISTERED_COUNTERS, 0) | dict(
            eligible_reads=1, rma_hits=1, rma_posts=32, rma_completions=32)
        self.assertTrue(kv.registered_only_hits(counters, elements=32))
        self.assertFalse(kv.registered_only_hits(counters, elements=32, groups=32))
        self.assertFalse(kv.registered_only_hits(dict.fromkeys(kv.REGISTERED_COUNTERS, 0), elements=32))
        for field in ('message_read_requests', 'grant_acquisitions', 'rma_errors',
                      'fallback_missing', 'fallback_oversize', 'fallback_capacity',
                      'fallback_unsupported', 'fallback_stale'):
            with self.subTest(field=field):
                self.assertFalse(kv.registered_only_hits(counters | {field: 1}, elements=32))
        self.assertFalse(kv.registered_only_hits(counters | dict(rma_completions=31), elements=32))

    def test_aggregate_hits_cannot_substitute_for_actual_lane_coverage(self):
        with tempfile.TemporaryDirectory() as root:
            _, path, records = self.registered_control_fixture(root)
            for pass_index, snapshot in enumerate(records[0]['observation']['lane_snapshots'][1:], 1):
                for field, per_pass in (('eligible_reads', 33), ('rma_hits', 33),
                                        ('rma_posts', 64), ('rma_completions', 64)):
                    snapshot[0][2][field] = pass_index * per_pass
                    snapshot[1][2][field] = 0
            path.write_text(''.join(json.dumps(row) + '\n' for row in records))
            with self.assertRaises(ValueError):
                kv.registered_control_evidence(root)

    def test_registered_control_rpc_is_mixed_even_with_completed_verified_rma(self):
        with tempfile.TemporaryDirectory() as root:
            spec, path, records = self.registered_control_fixture(root)
            observation = records[2]['observation']
            observation['after'][0][2]['message_read_requests'] += 1
            observation['counters']['message_read_requests'] += 1
            observation['population'] = 'mixed_or_invalid'
            observation['measurement_phase'] = 'cache_hot_get_many_mixed'
            path.write_text(''.join(json.dumps(row) + '\n' for row in records))
            summary = Path(root) / 'summary.csv'
            summary.write_text(summary.read_text().replace('cache_hot_get_many,', 'cache_hot_get_many_mixed,'))
            evidence = kv.registered_control_evidence(root)
            self.assertFalse(evidence['qualified'])
            self.assertEqual(evidence['phases']['cache_hot_get_many']['totals']['rma_completions'], 32)
            with self.assertRaises(ValueError):
                kv.require_registered_probe(spec, 'tcp', kv.resident_read_evidence(root))

    def test_registered_latency_cardinality_must_match_its_counter_population(self):
        with tempfile.TemporaryDirectory() as root:
            _, _, _ = self.registered_control_fixture(root)
            summary = Path(root) / 'summary.csv'
            with summary.open() as stream:
                rows = list(csv.DictReader(stream))
            next(row for row in rows if row['phase'] == 'cache_hot_get')['samples'] = 8192
            with summary.open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            self.assertFalse(kv.registered_control_evidence(root)['qualified'])

    def test_registered_owner_and_lane_counter_populations_must_agree(self):
        with tempfile.TemporaryDirectory() as root:
            spec, _, _ = self.registered_control_fixture(root)
            path = Path(root) / 'resident_reads.jsonl'
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            for row in rows:
                for field in kv.REGISTERED_COUNTERS:
                    row['owners'][1][1][field] = row['owners'][0][1][field]
                    row['owners'][0][1][field] = 0
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            with self.assertRaises(ValueError):
                kv.require_registered_probe(spec, 'tcp', kv.resident_read_evidence(root))

    def test_legacy_disabled_rma_proof_does_not_require_an_unused_new_hot_policy(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            current = kv.comparison_identity(directory, spec, 'tcp', cfg)
            metadata = directory / 'campaign.txt'
            metadata.write_text(''.join(line + '\n' for line in metadata.read_text().splitlines()
                                        if not line.startswith('registered_hot_policy=')))
            legacy = kv.comparison_identity(directory, spec, 'tcp', cfg)
            self.assertTrue(legacy['qualified'])
            self.assertEqual(legacy['preparation']['registered_hot_policy'], dict(enabled=False))
            self.assertEqual(legacy['preparation'], current['preparation'])
            self.assertTrue(kv.match_comparison(legacy, current)['comparable'])
            metadata.write_text(''.join(line + '\n' for line in metadata.read_text().splitlines()
                                        if not line.startswith('rma_config=')))
            missing = kv.comparison_identity(directory, spec, 'tcp', cfg)
            self.assertFalse(missing['qualified'])
            self.assertIn('rma_config', missing['missing'])

    def test_active_rma_without_observed_preparation_policy_remains_unqualified(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root, resident=True)
            metadata = directory / 'campaign.txt'
            metadata.write_text(''.join(line + '\n' for line in metadata.read_text().splitlines()
                                        if not line.startswith('registered_hot_policy=')))
            identity = kv.comparison_identity(directory, spec, 'tcp', cfg)
            self.assertFalse(identity['qualified'])
            self.assertIn('registered_hot_policy', identity['missing'])

    def test_registered_measurement_requires_complete_typed_lane_evidence(self):
        for mutation in ('missing_lane', 'missing_counter', 'wrong_scope'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                _, path, records = self.registered_control_fixture(root)
                observation = records[2]['observation']
                if mutation == 'missing_lane':
                    observation['before'].pop()
                    observation['after'].pop()
                elif mutation == 'missing_counter':
                    del observation['after'][0][2]['fallback_stale']
                else:
                    records[2]['scope'] = 'inferred_lane_coverage'
                path.write_text(''.join(json.dumps(row) + '\n' for row in records))
                with self.assertRaises(ValueError):
                    kv.registered_control_evidence(root)

    def test_rma_planner_adds_same_control_to_native_without_native_rma_flags(self):
        cfg = self.config()
        for resident in (False, True):
            spec = self.specimen()
            if resident:
                spec.update(transport='efa-native', resident_reads=self.resident_limits())
            _, commands = kv_case_commands(cfg, spec)
            for mode, command in commands.items():
                self.assertEqual('--bounded-hot-pass' in command, resident)
                if resident:
                    self.assertEqual(command[command.index('--bounded-hot-pass') + 1], 'true')
                if mode == 'native-sync':
                    self.assertFalse(any(flag.startswith('--rma-') for flag in command))
            self.assertEqual(commands['tcp'][commands['tcp'].index('--batches') + 1],
                             commands['native-sync'][commands['native-sync'].index('--batches') + 1])

    def paired_hot_identities(self, root, remote_batches=320):
        native, native_spec, cfg = self.observed(root, resident=True, mode='native-sync')
        self.bounded_control_fixture(native, native_spec, native=True)
        remote, remote_spec, _ = self.observed(root, remote_batches, resident=True)
        self.registered_control_fixture(remote, reads=8192)
        return (kv.comparison_identity(native, native_spec, 'native-sync', cfg),
                kv.comparison_identity(remote, remote_spec, 'tcp', cfg))

    def test_native_and_rma_have_matching_real_bounded_populations_not_legacy_ratios(self):
        with tempfile.TemporaryDirectory() as root:
            native, remote = self.paired_hot_identities(root)
            self.assertTrue(native['qualified'])
            self.assertTrue(remote['qualified'])
            self.assertEqual(native['bounded_hot']['policy'], remote['bounded_hot']['policy'])
            for identity in (native, remote):
                self.assertEqual(identity['bounded_hot']['preparation']['completed_passes'], 5)
                self.assertEqual(identity['bounded_hot']['phases']['cache_hot_get']['samples'], 512)
                self.assertEqual(identity['bounded_hot']['phases']['cache_hot_get_many']['samples'], 16)
                self.assertTrue(identity['bounded_hot']['service_qualified'])
            self.assertFalse(kv.match_comparison(native, remote,
                compared=('implementation', 'transport'))['ratios_allowed'])
            matched = kv.match_bounded_hot(native, remote)
            self.assertTrue(matched['ratios_allowed'])
            self.assertEqual(matched['population'], 'bounded_hot32')
            self.assertEqual(matched['provenance']['reference']['actual_runtime_policy']['registration_profile']['destination_slots'], 0)
            self.assertEqual(matched['provenance']['candidate']['actual_runtime_policy']['registration_profile']['destination_slots'], 640)
            self.assertFalse(kv.match_bounded_hot(native, remote, compared=('implementation',))['ratios_allowed'])

    def test_bounded_native_remote_match_keeps_working_sets_caches_and_preparation_strict(self):
        with tempfile.TemporaryDirectory() as root:
            native, remote = self.paired_hot_identities(root, remote_batches=3200)
            self.assertFalse(kv.match_bounded_hot(native, remote)['ratios_allowed'])
        with tempfile.TemporaryDirectory() as root:
            native, remote = self.paired_hot_identities(root)
            for section, field, value in (
                    ('workload', 'read_seed', '99'),
                    ('policy', 'coordinator_cache_bytes', 8192),
                    ('preparation', 'chunk_batches', 1)):
                changed = copy.deepcopy(remote)
                changed[section][field] = value
                self.assertFalse(kv.match_bounded_hot(native, changed)['ratios_allowed'])
            changed = copy.deepcopy(remote)
            changed['bounded_hot']['policy']['warm_passes'] = 3
            self.assertFalse(kv.match_bounded_hot(native, changed)['ratios_allowed'])
            changed = copy.deepcopy(remote)
            changed['bounded_hot']['service_qualified'] = False
            self.assertFalse(kv.match_bounded_hot(native, changed)['ratios_allowed'])

    def test_native_legacy_or_missing_control_cannot_be_used_as_bounded_denominator(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root, mode='native-sync')
            legacy = kv.comparison_identity(directory, spec, 'native-sync', cfg)
            self.assertFalse(kv.match_bounded_hot(legacy, legacy)['ratios_allowed'])
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root, resident=True, mode='native-sync')
            missing = kv.comparison_identity(directory, spec, 'native-sync', cfg)
            self.assertFalse(missing['qualified'])
            self.assertIn('bounded_hot_preparation_or_population_not_qualified', missing['missing'])

    def test_native_hot_control_reports_real_physical_evidence_not_rma_counters(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root, resident=True, mode='native-sync')
            path, records = self.bounded_control_fixture(directory, spec, native=True)
            evidence = kv.bounded_control_evidence(directory, 'native-sync')
            self.assertTrue(evidence['service_qualified'])
            self.assertEqual(evidence['phases']['cache_hot_get']['charged_physical_read_bytes'], 0)
            self.assertFalse(kv.resident_read_evidence(directory)['available'])
            self.assertNotIn('counters', records[1]['observation'])
            records[1]['observation']['population'] = 'registered_hit_only'
            path.write_text(''.join(json.dumps(row) + '\n' for row in records))
            with self.assertRaises(ValueError):
                kv.bounded_control_evidence(directory, 'native-sync')

    def test_missing_or_positive_native_physical_io_never_becomes_zero_io_ram_proof(self):
        for physical_state in ('missing', 'positive'):
            with self.subTest(physical_state=physical_state), tempfile.TemporaryDirectory() as root:
                directory, spec, _ = self.observed(root, resident=True, mode='native-sync')
                path, records = self.bounded_control_fixture(directory, spec, native=True)
                io_path = directory / 'read_io.csv'
                if physical_state == 'missing':
                    io_path.unlink()
                    for row in records[1:]:
                        row['observation']['physical_io_available'] = False
                    path.write_text(''.join(json.dumps(row) + '\n' for row in records))
                else:
                    with io_path.open() as stream:
                        rows = list(csv.DictReader(stream))
                    rows[0]['physical_read_bytes'] = 4096
                    with io_path.open('w') as stream:
                        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                        writer.writeheader()
                        writer.writerows(rows)
                evidence = kv.bounded_control_evidence(directory, 'native-sync')
                self.assertFalse(evidence['service_qualified'])
                observed = evidence['phases']['cache_hot_get']['charged_physical_read_bytes']
                self.assertEqual(observed, None if physical_state == 'missing' else 4096)

    def test_bounded_control_requires_its_fixed_actual_warm_pass_count(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, _ = self.observed(root, resident=True, mode='native-sync')
            path, records = self.bounded_control_fixture(directory, spec, native=True)
            records[0]['observation']['completed_passes'] = 2
            path.write_text(''.join(json.dumps(row) + '\n' for row in records))
            with self.assertRaises(ValueError):
                kv.bounded_control_evidence(directory, 'native-sync')

    def test_bounded_same_role_comparison_keeps_full_registration_budget_matching(self):
        with tempfile.TemporaryDirectory() as root:
            _, remote = self.paired_hot_identities(root)
            changed = copy.deepcopy(remote)
            changed['policy']['registration_profile']['source_bytes'] *= 2
            self.assertFalse(kv.match_bounded_hot(remote, changed)['ratios_allowed'])

    def test_explicit_registered_arm_holds_source_load_flags_and_common_samples_fixed(self):
        with tempfile.TemporaryDirectory() as root:
            _, registered = self.paired_hot_identities(root)
            control = copy.deepcopy(registered)
            control['policy']['registration_profile'] = kv.resident_read_config()
            control['transport']['one_sided'] = 'false'
            control['preparation'].update(registered_hot_policy=dict(enabled=False),
                registered_hot_state='not_applicable', registered_hot_active_lanes=[],
                registered_hot_owner_elements={})
            self.assertFalse(kv.match_comparison(control, registered)['ratios_allowed'])
            self.assertTrue(kv.match_registered_arm(control, registered)['ratios_allowed'])
            for section, field, value in (
                    ('workload', 'loaded_batches', 3200),
                    ('implementation', 'binary_sha256', 'f' * 64),
                    ('policy', 'shard_cache_bytes', 8 * 1024**2),
                    ('clocks', 'detailed', True)):
                changed = copy.deepcopy(registered)
                changed[section][field] = value
                self.assertFalse(kv.match_registered_arm(control, changed)['ratios_allowed'])
            changed = copy.deepcopy(registered)
            changed['preparation']['bounded_hot_policy']['scalar_reads'] = 32768
            self.assertFalse(kv.match_registered_arm(control, changed)['ratios_allowed'])


    def test_physical_cold_matcher_requires_every_probe_and_same_source_load_flags(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            remote = kv.comparison_identity(directory, spec, 'tcp', cfg)
            remote['workload'].update(advisory_eviction=True, cold_single_probes=1024)
            remote['physical_cold'] = dict(physical_cold_api_latency_qualified=True, samples=1024)
            native = copy.deepcopy(remote)
            native['transport']['name'] = 'native'
            native['policy'].update(max_pending_read_bytes=None, authority_resident_batches=None)
            self.assertTrue(kv.match_physical_cold(native, remote)['ratios_allowed'])
            for section, field, value in (
                    ('workload', 'cold_single_probes', 32),
                    ('workload', 'loaded_batches', 3200),
                    ('implementation', 'source_archive_sha256', 'f' * 64),
                    ('physical_cold', 'physical_cold_api_latency_qualified', False),
                    ('clocks', 'detailed', True)):
                changed = copy.deepcopy(remote)
                changed[section][field] = value
                self.assertFalse(kv.match_physical_cold(native, changed)['ratios_allowed'])


    def test_group_count_metadata_cannot_enable_an_unadmitted_physical_population(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            metadata = directory / 'campaign.txt'
            original = metadata.read_text()
            metadata.write_text(original + 'cold_group_probes=0\n')
            self.assertTrue(kv.comparison_identity(directory, spec, 'tcp', cfg)['qualified'])
            metadata.write_text(original + 'cold_group_probes=1024\n')
            with self.assertRaisesRegex(ValueError, 'grouped probe count'):
                kv.comparison_identity(directory, spec, 'tcp', cfg)

    def test_physical_group_matcher_requires_complete_group_proof_and_same_load_count_source(self):
        with tempfile.TemporaryDirectory() as root:
            directory, spec, cfg = self.observed(root)
            remote = kv.comparison_identity(directory, spec, 'tcp', cfg)
            remote['workload'].update(advisory_eviction=True, cold_group_probes=1024)
            remote['physical_cold_group'] = dict(physical_cold_group_api_latency_qualified=True,
                                                  samples=1024, members_per_call=32)
            native = copy.deepcopy(remote)
            native['transport']['name'] = 'native'
            native['policy'].update(max_pending_read_bytes=None, authority_resident_batches=None)
            result = kv.match_physical_cold_group(native, remote)
            self.assertTrue(result['ratios_allowed'])
            self.assertEqual(result['population'], 'all_probes_physical_cold_complete32_API')
            self.assertIn('not_per_member', result['scope'])
            self.assertFalse(kv.match_physical_cold(native, remote)['ratios_allowed'])
            for section, field, value in (
                    ('workload', 'cold_group_probes', 16),
                    ('workload', 'loaded_batches', 3200),
                    ('implementation', 'source_archive_sha256', 'f' * 64),
                    ('physical_cold_group', 'physical_cold_group_api_latency_qualified', False),
                    ('clocks', 'detailed', True)):
                changed = copy.deepcopy(remote)
                changed[section][field] = value
                self.assertFalse(kv.match_physical_cold_group(native, changed)['ratios_allowed'])

    def test_rpc_bounded_tiers_require_actual_service_counters_and_zero_interval_io(self):
        expected = dict(coordinator='coordinator_cache_hit_only', owner='shard_cache_hit_only',
                        pagecache='remote_pagecache_service_observed_zero_accounted_IO')
        for population, label in expected.items():
            with self.subTest(population=population), tempfile.TemporaryDirectory() as root:
                spec = self.specimen()
                path, records = self.bounded_control_fixture(root, spec)
                for row in records[1:]:
                    row['observation']['population'] = 'remote_API_hotset_unclassified'
                path.write_text(''.join(json.dumps(row) + '\n' for row in records))
                fields = ['phase', 'role', 'partition', 'boundary', 'cache_hits', 'cache_misses',
                          'wal_reads', 'resident_read_successes', 'async_read_submissions']
                with (Path(root) / 'read_path_evidence.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    for phase in ('cache_hot_get', 'cache_hot_get_many'):
                        for boundary in ('before', 'after'):
                            count = 512 if boundary == 'after' else 0
                            writer.writerow(dict(phase=phase, role='frontend', partition=0, boundary=boundary,
                                cache_hits=count if population == 'coordinator' else 0,
                                cache_misses=0 if population == 'coordinator' else count,
                                wal_reads=0, resident_read_successes=0, async_read_submissions=0))
                            for owner in range(5):
                                active = count if owner == 0 else 0
                                writer.writerow(dict(phase=phase, role='storage', partition=owner, boundary=boundary,
                                    cache_hits=active if population == 'owner' else 0, cache_misses=0,
                                    wal_reads=active if population == 'pagecache' else 0,
                                    resident_read_successes=active if population == 'pagecache' else 0,
                                    async_read_submissions=0))
                evidence = kv.bounded_control_evidence(root, 'tcp')
                self.assertTrue(evidence['service_qualified'])
                self.assertEqual(evidence['phases']['cache_hot_get']['population'], label)
                with (Path(root) / 'read_io.csv').open() as stream:
                    rows = list(csv.DictReader(stream))
                rows[0]['physical_read_bytes'] = 4096
                with (Path(root) / 'read_io.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)
                self.assertFalse(kv.bounded_control_evidence(root, 'tcp')['service_qualified'])


if __name__ == '__main__':
    unittest.main()
