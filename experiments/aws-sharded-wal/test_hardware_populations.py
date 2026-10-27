"""Deterministic, offline hardware campaign contract; no allocation or processes."""
import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest

import checkpoint_kv as kv
from checkpoint_matrix import matrix_plan, cell_resident_reads, check_cell
from guest import kv_case_commands, kv_case_policy


class HardwarePopulations(unittest.TestCase):
    def config(self):
        cfg = json.loads(Path(__file__).with_name('ci-config.json').read_text())
        cfg.update(run_root='/frozen', nodes=[dict(partition=p, private_ip=f'10.0.1.{p + 1}')
                                            for p in range(5)])
        return cfg

    def test_exact_population_counts_and_reversed_clean_order(self):
        cfg = self.config()
        cases = kv.plan(cfg['kv_suite'], cfg['transports'])
        self.assertEqual(len(cases), 93)
        self.assertEqual(sum(not c['diagnostics'] for c in cases), 88)
        self.assertEqual(sum(c['diagnostics'] for c in cases), 5)
        for repeat in (0, 1):
            clean = [c for c in cases if c['repetition'] == repeat]
            ordered = list(dict.fromkeys(c['transport'] for c in clean))
            self.assertEqual(ordered, cfg['transports'] if repeat == 0 else cfg['transports'][::-1])
            for transport in cfg['transports']:
                selected = [c for c in clean if c['transport'] == transport]
                for value in (256, 8192):
                    scalar = [c for c in selected if c['shape'] == [value, 1, 1]]
                    self.assertTrue(any(c['cached'] for c in scalar))
                    self.assertTrue(any(c['shard_cache_bytes'] == 8 * 1024**2 for c in scalar))
                    self.assertTrue(any(c['cold'] and not c['cached'] and c['shard_cache_bytes'] == 0
                                        and 'resident_reads' not in c for c in scalar))
                batches = [c for c in selected if c['shape'] == [8192, 32, 32]]
                self.assertEqual({c['workload_batches'] for c in batches}, {320, 3200})
                for load in (320, 3200):
                    arms = [c for c in batches if c['workload_batches'] == load]
                    self.assertTrue(any('resident_reads' not in c for c in arms))
                    self.assertEqual(sum('resident_reads' in c for c in arms), int(transport == 'efa-native'))

    def test_exact_commands_native_never_receives_rma_flags_and_sampling_is_hashed(self):
        cfg = self.config()
        paths = set()
        for spec in kv.plan(cfg['kv_suite'], cfg['transports']):
            prepare, commands = kv_case_commands(cfg, spec)
            remote, owners, owner_transports = kv_case_policy(cfg, spec)
            self.assertEqual(len(owner_transports), len(cfg['nodes']))
            self.assertEqual(remote['read_cache_bytes'], 8 * 1024**2 if spec['cached'] else 0)
            self.assertEqual({o['node']['read_cache_bytes'] for o in owners.values()}, {spec['shard_cache_bytes']})
            self.assertEqual(remote['tls']['resident_reads']['require_device_read'], 'resident_reads' in spec)
            for mode, command in commands.items():
                self.assertEqual(command[command.index('--bounded-hot-reads') + 1], '32768')
                self.assertEqual('--bounded-hot-pass' in command, not spec['cold'])
                if spec['cold']:
                    self.assertEqual(command[command.index('--cold-single-probes') + 1], '1024')
                    self.assertEqual(command[command.index('--cold-group-probes') + 1], '1024')
                else:
                    self.assertNotIn('--cold-single-probes', command)
                    self.assertNotIn('--cold-group-probes', command)
                self.assertFalse(any(arg.startswith('--rma-') for arg in command))
                output = command[command.index('--output') + 1]
                self.assertIn('hotreads32768-', output)
                self.assertNotIn(output, paths)
                paths.add(output)
            if 'resident_reads' in spec:
                self.assertIn('--rma-require-device-read', prepare)
                self.assertEqual(prepare[prepare.index('--rma-require-device-read') + 1], 'true')
            else:
                self.assertFalse(any(arg.startswith('--rma-') for arg in prepare))

    def test_explicit_samples_do_not_change_legacy_policy(self):
        legacy = kv.bounded_hot_policy(8192, 2, True)
        self.assertEqual(legacy['scalar_reads'], 512)
        self.assertEqual(legacy['group_calls'], 16)
        self.assertNotIn('bounded_hot_reads', legacy)
        explicit = kv.bounded_hot_policy(8192, 2, True, 32768)
        self.assertEqual(explicit['scalar_reads'], 32768)
        self.assertEqual(explicit['group_calls'], 1024)
        self.assertEqual(explicit['bounded_hot_reads'], 32768)
        self.assertEqual(explicit['warm_passes'], 5)
        for bad in (True, '32768', 0, 31, 131073, 32768.0):
            options = self.config()['kv_suite'] | dict(bounded_hot_reads=bad)
            with self.assertRaises(ValueError):
                kv.validate_options(options)

    def test_cold_probe_count_is_strict_and_disabled_legacy_identity_is_unchanged(self):
        for bad in (True, '1024', -1, 4097, 1024.0):
            with self.assertRaises(ValueError):
                kv.validate_options(dict(repetitions=1, diagnostics=False, cold_single_probes=bad))
        self.assertEqual(kv.plan(dict(repetitions=1, diagnostics=False), ['tcp']),
                         kv.plan(dict(repetitions=1, diagnostics=False, cold_single_probes=0), ['tcp']))
        cfg = self.config()
        spec = next(case for case in kv.plan(cfg['kv_suite'], cfg['transports']) if case['cold'])
        command = kv_case_commands(cfg, spec)[1]['tcp']
        self.assertIn('coldprobes1024-', command[command.index('--output') + 1])
        changed = spec | dict(cold_single_probes=32)
        self.assertNotEqual(command, kv_case_commands(cfg, changed)[1]['tcp'])

    def test_group_probe_option_is_strict_independent_and_preserves_zero_legacy_default(self):
        legacy = dict(repetitions=1, diagnostics=False)
        self.assertEqual(kv.plan(legacy, ['tcp']), kv.plan(legacy | dict(cold_group_probes=0), ['tcp']))
        for invalid in (True, '1024', -1, 4097, 1024.0):
            with self.assertRaises(ValueError):
                kv.validate_options(legacy | dict(cold_group_probes=invalid))
        grouped = kv.plan(legacy | dict(cold_group_probes=7), ['tcp'])
        selected = next(case for case in grouped if case['cold'])
        self.assertEqual(selected['cold_group_probes'], 7)
        self.assertNotIn('cold_single_probes', selected)
        for case in grouped:
            self.assertEqual('cold_group_probes' in case, case['cold'])
        commands = kv_case_commands(self.config(), selected)[1]
        for command in commands.values():
            self.assertEqual(command[command.index('--cold-group-probes') + 1], '7')
            self.assertNotIn('--cold-single-probes', command)
            self.assertIn('coldgroups7-', command[command.index('--output') + 1])
        with self.assertRaises(ValueError):
            kv_case_commands(self.config(), selected | dict(cold=False))

    def test_sui_fixture_has_off_and_on_cells_and_explicit_cache_schedules(self):
        cfg = self.config()
        cells = matrix_plan(cfg)
        self.assertEqual(len(cells), 12)
        self.assertEqual(sum(c['read_profile_limit'] == 0 for c in cells), 10)
        for cell in cells:
            check_cell(cell['cell_id'], cell['transport'])
            observed = cell_resident_reads(cfg, cell['transport'], cell)
            self.assertEqual(observed['require_device_read'], cell['cell_id'].endswith('-rma'))
            self.assertEqual(cell['shard_cache_mib'], 8)
            self.assertEqual(cell['coordinator_cache_mib'], 8 if cell['repetition'] == 2 else 0)
        for repeat in (1, 2, 3):
            native = [c for c in cells if c['repetition'] == repeat and c['transport'] == 'efa-native']
            self.assertEqual(len(native), 2)
            self.assertEqual(sum('resident_reads' in c for c in native), 1)
        damaged = copy.deepcopy(cfg)
        damaged['coordinator_cache_mib_by_repetition'] = [8, 8, 8]
        with self.assertRaises(ValueError):
            matrix_plan(damaged)

    def test_real_write_and_reopen_population_missing_is_rejected(self):
        cfg = self.config()
        spec = kv.plan(cfg['kv_suite'], cfg['transports'])[0]
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'summary.csv'
            with path.open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=['phase', 'logical_operations'])
                writer.writeheader()
                writer.writerow(dict(phase='point_get', logical_operations=4096))
            with self.assertRaisesRegex(ValueError, 'WAL/read/barrier/reopen'):
                kv.operation_population_evidence(root, spec, 'tcp')

    def test_cold_labels_require_positive_interval_io_and_exact_process_owners(self):
        cfg = self.config()
        spec = next(c for c in kv.plan(cfg['kv_suite'], cfg['transports']) if c['cold'])
        fields = ['backend', 'value_bytes', 'batch_size', 'window', 'repetition', 'phase',
                  'partition', 'pid', 'physical_read_bytes', 'file_bytes',
                  'resident_bytes_before', 'resident_bytes_after']
        with tempfile.TemporaryDirectory() as root:
            def emit(mode, charged, omit=False):
                backend = 'native_value_wal_barrier' if mode == 'native-sync' else 'experimental_tcp_authority'
                with (Path(root) / 'read_io.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    for phase in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'):
                        for owner in ([0] if mode == 'native-sync' else range(4 if omit else 5)):
                            writer.writerow(dict(backend=backend, value_bytes=256, batch_size=1, window=1,
                                repetition=0, phase=phase, partition=owner, pid=100 + owner,
                                physical_read_bytes=charged, file_bytes=4096,
                                resident_bytes_before=0, resident_bytes_after=4096))
            for mode in ('native-sync', 'tcp'):
                emit(mode, 0)
                with self.assertRaisesRegex(ValueError, 'positive accounted physical IO'):
                    kv.cold_interval_evidence(root, spec, mode)
                emit(mode, 4096)
                result = kv.cold_interval_evidence(root, spec, mode)
                self.assertTrue(result['physical_io_observed'])
                self.assertFalse(result['physical_nvme_latency'])
            emit('tcp', 4096, omit=True)
            with self.assertRaises(ValueError):
                kv.cold_interval_evidence(root, spec, 'tcp')




class PhysicalColdPopulations(unittest.TestCase):
    def fixture(self, root, mode='tcp'):
        spec = dict(shape=[256, 1, 1], transport='tcp', cold=True,
                    workload_batches=320, cold_single_probes=2)
        rows = []
        for index in range(2):
            mixed = ((index ^ (index >> 30)) * 0xbf58476d1ce4e5b9) & (2**64 - 1)
            mixed = ((mixed ^ (mixed >> 27)) * 0x94d049bb133111eb) & (2**64 - 1)
            selected = (mixed ^ (mixed >> 31)) % 320
            owners = [0] if mode == 'native-sync' else list(range(5))
            owner = 0 if mode == 'native-sync' else index
            rows.append(dict(backend='native_value_wal_barrier' if mode == 'native-sync' else 'experimental_tcp_authority',
                transport='tcp', value_bytes=256, batch_size=1, window=1, repetition=0,
                phase='physical_cold_get', requested_probes=2, probe_index=index,
                key_index=selected, expected_owner=owner, api_elapsed_ns=1000 + index,
                preparation_wall_ns=10000, value_verified=True,
                identity_qualified=True,
                expected_owner_accounted_process_read_bytes_delta=4096,
                positive_expected_owner_accounted_process_read=True,
                positive_any_owner_accounted_process_read=True,
                observations=[dict(partition=p, pid=100 + p, process_start_ticks=1,
                    process_boot_id=[p] * 16,
                    accounted_process_read_bytes_before=0,
                    accounted_process_read_bytes_after=4096 if p == owner else 0,
                    accounted_process_read_bytes_delta=4096 if p == owner else 0,
                    minor_fault_delta=0, major_fault_delta=0, file_bytes=8192,
                    resident_bytes_before=0, resident_bytes_after=4096) for p in owners]))
        with (Path(root) / 'summary.csv').open('w') as stream:
            identity = {key: rows[0][key] for key in
                        ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition', 'phase')}
            writer = csv.DictWriter(stream, fieldnames=[*identity, 'samples', 'logical_operations',
                                                       'measured_api_ns', 'phase_wall_ns'])
            writer.writeheader()
            writer.writerow(identity | dict(samples=2, logical_operations=2,
                                             measured_api_ns=2001, phase_wall_ns=30000))
        return spec, rows

    def save(self, root, rows):
        (Path(root) / 'cold_probes.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))

    def test_all_samples_require_expected_owner_io_not_other_owner_or_aggregate_io(self):
        for mode in ('native-sync', 'tcp'):
            with tempfile.TemporaryDirectory() as root:
                spec, rows = self.fixture(root, mode)
                self.save(root, rows)
                evidence = kv.physical_cold_probe_evidence(root, spec, mode)
                self.assertTrue(evidence['qualified'])
                self.assertEqual(evidence['all_sample_latency_ns']['p99'], 1001)
                damaged = copy.deepcopy(rows)
                owner = damaged[1]['expected_owner']
                observation = next(row for row in damaged[1]['observations'] if row['partition'] == owner)
                observation.update(accounted_process_read_bytes_after=0, accounted_process_read_bytes_delta=0)
                damaged[1].update(expected_owner_accounted_process_read_bytes_delta=0,
                                  positive_expected_owner_accounted_process_read=False,
                                  positive_any_owner_accounted_process_read=False)
                if mode == 'tcp':
                    other = damaged[1]['observations'][0]
                    other.update(accounted_process_read_bytes_after=4096, accounted_process_read_bytes_delta=4096)
                    damaged[1]['positive_any_owner_accounted_process_read'] = True
                self.save(root, damaged)
                evidence = kv.physical_cold_probe_evidence(root, spec, mode)
                self.assertFalse(evidence['qualified'])
                self.assertEqual(len(evidence['probes']), 2)
                self.assertEqual(evidence['unqualified_probes'][0]['probe_index'], 1)
                self.assertEqual(evidence['all_sample_latency_ns']['p99'], 1001)

    def test_scalar_before_after_identity_flag_is_required_despite_valid_after_snapshot(self):
        for flag in (None, False):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as root:
                spec, rows = self.fixture(root, 'native-sync')
                rows[1]['identity_qualified'] = flag
                self.save(root, rows)
                result = kv.physical_cold_probe_evidence(root, spec, 'native-sync')
                self.assertFalse(result['qualified'])
                self.assertEqual(result['probes'][1]['expected_owner_accounted_process_read_bytes_delta'], 4096)

    def test_unavailable_physical_counters_remain_null_and_unqualified(self):
        with tempfile.TemporaryDirectory() as root:
            spec, rows = self.fixture(root, 'native-sync')
            observation = rows[1]['observations'][0]
            observation.update(accounted_process_read_bytes_before=None,
                               accounted_process_read_bytes_after=None,
                               accounted_process_read_bytes_delta=None)
            rows[1].update(expected_owner_accounted_process_read_bytes_delta=None,
                           positive_expected_owner_accounted_process_read=False,
                           positive_any_owner_accounted_process_read=False)
            self.save(root, rows)
            evidence = kv.physical_cold_probe_evidence(root, spec, 'native-sync')
            self.assertFalse(evidence['qualified'])
            self.assertIsNone(evidence['probes'][1]['observations'][0]['accounted_process_read_bytes_delta'])

    def test_missing_or_changed_identity_and_reordered_probes_do_not_qualify(self):
        with tempfile.TemporaryDirectory() as root:
            spec, rows = self.fixture(root)
            for identity in (None, 0, 999):
                damaged = copy.deepcopy(rows)
                damaged[1]['observations'][0]['process_start_ticks'] = identity
                self.save(root, damaged)
                self.assertFalse(kv.physical_cold_probe_evidence(root, spec, 'tcp')['qualified'])
            damaged = copy.deepcopy(rows)
            damaged[1]['observations'][0]['process_boot_id'] = [255] * 16
            self.save(root, damaged)
            self.assertFalse(kv.physical_cold_probe_evidence(root, spec, 'tcp')['qualified'])
            damaged = copy.deepcopy(rows)
            damaged[1]['probe_index'] = 0
            self.save(root, damaged)
            with self.assertRaises(ValueError):
                kv.physical_cold_probe_evidence(root, spec, 'tcp')


class GroupedPhysicalColdPopulations(unittest.TestCase):
    def fixture(self, root, native=False):
        spec = dict(shape=[256, 1, 1], transport='tcp', cold=True, workload_batches=32, cold_group_probes=2)
        owners = [0] if native else list(range(5))
        rows = []
        for index in range(2):
            mixed = ((index ^ (index >> 30)) * 0xbf58476d1ce4e5b9) & (2**64 - 1)
            mixed = ((mixed ^ (mixed >> 27)) * 0x94d049bb133111eb) & (2**64 - 1)
            start = (mixed ^ (mixed >> 31)) % 32
            indices = [(start + member) % 32 for member in range(32)]
            members = [owners[key % len(owners)] for key in indices]
            rows.append(dict(backend='native_value_wal_barrier' if native else 'experimental_tcp_authority',
                transport='tcp', value_bytes=256, batch_size=1, window=1, repetition=0,
                phase='physical_cold_get_many', requested_probes=2, probe_index=index,
                start_key_index=start, key_indices=indices, member_owners=members,
                expected_owners=sorted(set(members)), member_count=32, returned_member_count=32,
                api_elapsed_ns=1000 + index, preparation_wall_ns=10000, values_verified=True,
                identity_qualified=True,
                observations=[dict(partition=owner, pid=100 + owner, process_start_ticks=1,
                    process_boot_id=[owner] * 16, accounted_process_read_bytes_before=0,
                    accounted_process_read_bytes_after=4096, accounted_process_read_bytes_delta=4096,
                    minor_fault_delta=0, major_fault_delta=0, file_bytes=8192,
                    resident_bytes_before=0, resident_bytes_after=4096) for owner in owners]))
        summary = {key: rows[0][key] for key in
                   ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition', 'phase')}
        summary.update(samples=2, logical_operations=64, measured_api_ns=2001, phase_wall_ns=30000)
        self.save(root, rows, summary)
        return spec, rows, summary

    def save(self, root, rows, summary, *, update_claims=True):
        if update_claims:
            for row in rows:
                observed = {entry['partition']: entry for entry in row['observations']}
                present = set(row['expected_owners']) <= set(observed)
                positive = all((observed[owner]['accounted_process_read_bytes_delta'] or 0) > 0
                               for owner in set(row['expected_owners']) & set(observed))
                row.update(expected_owners_observed=present, expected_owner_reads_positive=positive,
                           probe_qualified=row['values_verified'] and present and positive
                                           and row.get('identity_qualified') is True)
                row.setdefault('api_error', None)
        (Path(root) / 'cold_group_probes.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
        with (Path(root) / 'summary.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=summary.keys())
            writer.writeheader()
            writer.writerow(summary)

    def test_native_and_remote_complete_calls_keep_wrap_order_and_sample_units(self):
        for native in (False, True):
            with self.subTest(native=native), tempfile.TemporaryDirectory() as root:
                spec, rows, _ = self.fixture(root, native)
                mode = 'native-sync' if native else 'tcp'
                result = kv.physical_cold_group_probe_evidence(root, spec, mode)
                self.assertTrue(result['qualified'])
                self.assertEqual(result['samples'], 2)
                self.assertEqual(result['members_per_call'], 32)
                self.assertEqual(result['verified_member_count'], 64)
                self.assertEqual(result['all_sample_complete_call_latency_ns']['p99'], 1001)
                self.assertGreater(rows[1]['key_indices'][0], rows[1]['key_indices'][-1])
                self.assertNotIn('per_member_latency_ns', result)

    def test_every_expected_owner_must_have_io_missing_owner_and_zero_are_not_filtered(self):
        for missing in (False, True):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                if missing:
                    rows[1]['observations'].pop()
                else:
                    rows[1]['observations'][0].update(accounted_process_read_bytes_after=0,
                                                      accounted_process_read_bytes_delta=0)
                self.save(root, rows, summary)
                result = kv.physical_cold_group_probe_evidence(root, spec, 'tcp')
                self.assertFalse(result['qualified'])
                self.assertEqual(len(result['probes']), 2)
                self.assertEqual(result['unqualified_probes'][0]['probe_index'], 1)
                self.assertEqual(result['all_sample_complete_call_latency_ns']['p99'], 1001)

    def test_missing_returned_member_and_failed_value_verification_remain_unqualified(self):
        for field, value in (('returned_member_count', 31), ('values_verified', False)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                rows[1][field] = value
                self.save(root, rows, summary)
                result = kv.physical_cold_group_probe_evidence(root, spec, 'tcp')
                self.assertFalse(result['qualified'])
                self.assertIsNone(result['verified_member_count'])
                self.assertEqual(len(result['probes']), 2)

    def test_identity_unavailable_or_changed_cannot_qualify_grouped_io(self):
        for field, value in (('process_boot_id', None), ('pid', 999),
                             ('process_start_ticks', 0), ('process_start_ticks', 999)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                rows[1]['observations'][0][field] = value
                self.save(root, rows, summary)
                self.assertFalse(kv.physical_cold_group_probe_evidence(root, spec, 'tcp')['qualified'])

    def test_unknown_group_counter_remains_null_not_zero_or_an_all_owner_claim(self):
        with tempfile.TemporaryDirectory() as root:
            spec, rows, summary = self.fixture(root)
            rows[1]['observations'][0].update(accounted_process_read_bytes_before=None,
                accounted_process_read_bytes_after=None, accounted_process_read_bytes_delta=None)
            self.save(root, rows, summary)
            result = kv.physical_cold_group_probe_evidence(root, spec, 'tcp')
            self.assertFalse(result['qualified'])
            self.assertEqual(len(result['probes']), 2)
            self.assertIsNone(result['probes'][1]['observations'][0]['accounted_process_read_bytes_delta'])

    def test_foreign_key_order_member_owner_count_and_raw_arithmetic_fail_closed(self):
        for mutation in ('wrong_start', 'wrong_key', 'missing_key', 'missing_member_owner',
                         'wrong_owner_set', 'member_count', 'probe_count', 'row_count', 'delta'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                if mutation == 'wrong_start':
                    rows[1]['start_key_index'] = 0
                elif mutation == 'wrong_key':
                    rows[1]['key_indices'].reverse()
                elif mutation == 'missing_key':
                    rows[1]['key_indices'].pop()
                elif mutation == 'missing_member_owner':
                    rows[1]['member_owners'].pop()
                elif mutation == 'wrong_owner_set':
                    rows[1]['expected_owners'].pop()
                elif mutation == 'member_count':
                    rows[1]['member_count'] = 31
                elif mutation == 'probe_count':
                    rows[1]['requested_probes'] = 3
                elif mutation == 'row_count':
                    rows.pop()
                else:
                    rows[1]['observations'][0]['accounted_process_read_bytes_delta'] = 1
                self.save(root, rows, summary)
                with self.assertRaises(ValueError):
                    kv.physical_cold_group_probe_evidence(root, spec, 'tcp')

    def test_group_before_after_identity_flag_is_required_despite_valid_after_snapshot(self):
        for flag in (None, False):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                rows[1]['identity_qualified'] = flag
                self.save(root, rows, summary)
                result = kv.physical_cold_group_probe_evidence(root, spec, 'tcp')
                self.assertFalse(result['qualified'])
                self.assertTrue(result['probes'][1]['expected_owner_reads_positive'])

    def test_group_producer_claims_cannot_disagree_with_process_evidence(self):
        for field in ('expected_owners_observed', 'expected_owner_reads_positive', 'probe_qualified'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                rows[1][field] = False
                self.save(root, rows, summary, update_claims=False)
                with self.assertRaises(ValueError):
                    kv.physical_cold_group_probe_evidence(root, spec, 'tcp')

    def test_group_api_error_preserves_failed_call_and_every_sample(self):
        with tempfile.TemporaryDirectory() as root:
            spec, rows, summary = self.fixture(root)
            rows[1].update(api_error='actual get_many failed', values_verified=False, returned_member_count=0)
            self.save(root, rows, summary)
            result = kv.physical_cold_group_probe_evidence(root, spec, 'tcp')
            self.assertFalse(result['qualified'])
            self.assertEqual(len(result['probes']), 2)
            self.assertEqual(result['probes'][1]['api_error'], 'actual get_many failed')
            self.assertIsNone(result['verified_member_count'])

    def test_group_summary_cannot_substitute_member_counts_scalar_latencies_or_exclude_preparation(self):
        for field, value in (('samples', 64), ('logical_operations', 2),
                             ('measured_api_ns', 1000), ('phase_wall_ns', 2001)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                spec, rows, summary = self.fixture(root)
                summary[field] = value
                self.save(root, rows, summary)
                with self.assertRaises(ValueError):
                    kv.physical_cold_group_probe_evidence(root, spec, 'tcp')


class WritePopulations(unittest.TestCase):
    def test_write_intervals_retain_actual_roles_unknowns_and_no_per_key_io(self):
        spec = dict(shape=[256, 1, 1], transport='tcp', bounded_hot_reads=32768)
        base = dict(backend='experimental_tcp_authority', value_bytes=256, batch_size=1, window=1,
                    repetition=0, phase='load_write_window', role='controller', partition='', pid=100,
                    process_start_ticks=1, accounted_process_read_bytes_before=0,
                    accounted_process_read_bytes_after=0, accounted_process_read_bytes_delta=0,
                    accounted_process_write_bytes_before=4096, accounted_process_write_bytes_after=8192,
                    accounted_process_write_bytes_delta=4096)
        phases = ('load_write_window', 'mixed_write_window', 'checkpoint_intervening_write',
                  'delete_window', 'maintenance_flush_index')
        with tempfile.TemporaryDirectory() as root:
            def emit(missing=False, unknown=False, corrupt=False):
                with (Path(root) / 'write_io.csv').open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=base.keys())
                    writer.writeheader()
                    for phase in phases:
                        writer.writerow(base | dict(phase=phase))
                        for owner in range(4 if missing else 5):
                            row = base | dict(phase=phase, role='storage', partition=owner, pid=200 + owner)
                            if unknown and owner == 0:
                                row.update(accounted_process_write_bytes_before='',
                                           accounted_process_write_bytes_after='',
                                           accounted_process_write_bytes_delta='')
                            if corrupt and owner == 0:
                                row['accounted_process_write_bytes_delta'] = 100
                            writer.writerow(row)
            emit()
            evidence = kv.write_interval_evidence(root, spec, 'tcp')
            self.assertEqual(evidence['phases']['load_write_window']['accounted_process_write_bytes'], 6 * 4096)
            self.assertEqual(len(evidence['phases']['load_write_window']['processes']), 6)
            emit(unknown=True)
            evidence = kv.write_interval_evidence(root, spec, 'tcp')
            self.assertIsNone(evidence['phases']['load_write_window']['accounted_process_write_bytes'])
            emit(missing=True)
            with self.assertRaises(ValueError):
                kv.write_interval_evidence(root, spec, 'tcp')
            emit(corrupt=True)
            with self.assertRaises(ValueError):
                kv.write_interval_evidence(root, spec, 'tcp')


if __name__ == '__main__':
    unittest.main()
