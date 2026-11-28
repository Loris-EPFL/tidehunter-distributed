"""Synthetic mechanism cell tests: table invariants, spec admission, verification."""
import json
from pathlib import Path
import tempfile
import unittest

import stage2_mechanism as mechanism

ALLOWED_VALUES = (64, 256, 512, 1024, 4096, 8192, 16_384, 65_536)


def context(receipts=('r0.json', 'r1.json')):
    return dict(transport={'kind': 'tcp'},
                remote={'read_cache_bytes': 0, 'storage': {}, 'endpoints': [],
                        'tls': {'transport': {'kind': 'tcp'}}},
                receipts=list(receipts))


def write_run(directory, *, status='completed', completed=1000, errors=0,
              phase_counts=None, distributions=None, role='measure',
              read_rpc=10, open_kind='create', recovery=None, live=None,
              max_pending_read_bytes=64 << 20, flight_metadata_bytes=6_408_864,
              device_reads=None, measurement=True, key_bytes=32):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'summary.json').write_text(json.dumps(dict(
        status=status, durability={}, started_unix_seconds=0.0,
        finished_unix_seconds=1.0, execution={},
        backend={'configuration': {
            'client': {'max_pending_read_bytes': max_pending_read_bytes},
            'key_bytes': key_bytes}},
        phase_counts=phase_counts or {'realized_mix': {}},
        distributions=distributions or {}, limitations=[])))
    (directory / 'manifest.json').write_text(json.dumps(dict(
        status=status, completed_operations=completed,
        errors_or_wrong_values=errors)))
    live_lines = [f'tidehunter_nvmeof_live_{name} {value}'
                  for name, value in (live or {}).items()]
    (directory / 'live.prom').write_text('\n'.join(live_lines) + '\n')
    if measurement:
        nvme = dict(available=device_reads is not None,
                    counters={} if device_reads is None
                    else {'completed_read_ops': device_reads})
        (directory / 'measurement-read-evidence.json').write_text(json.dumps(dict(
            start=dict(coordinator=dict(
                           read_rpc_count=read_rpc,
                           read_singleflight_metadata_bytes=flight_metadata_bytes),
                       coordinator_open=dict(kind=open_kind,
                                             recovery=recovery or dict(
                                                 tail_batches=3, tail_bytes=4096,
                                                 below_cut_skipped=0, fencing_ns=10))),
            deltas=dict(coordinator=dict(available=True,
                                         counters=dict(read_rpc_count=read_rpc))),
            live_nvmeof=nvme)))
    (directory / 'read-evidence.json').write_text(json.dumps(dict(
        coordinator=dict(read_rpc_count=read_rpc, kind=open_kind),
        coordinator_open=dict(kind=open_kind))))
    return directory


class CellTable(unittest.TestCase):
    def test_identifiers_are_unique_and_tiered(self):
        cells = mechanism.cells()
        identifiers = [cell['id'] for cell in cells]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertTrue(all(cell['synthetic'] for cell in cells))
        self.assertEqual({cell['tier'] for cell in cells},
                         {'native', 'synchronous'})

    def test_batches_and_values_are_admitted_by_the_frozen_binary(self):
        for cell in mechanism.cells():
            for run in cell['runs']:
                with self.subTest(cell=cell['id'], role=run['role']):
                    self.assertGreaterEqual(run['batch'], 2)
                    self.assertLessEqual(run['batch'], 1024)
                    self.assertIn(run['value'], ALLOWED_VALUES)

    def test_m13_batch_crosses_the_covering_flush_threshold(self):
        cell = mechanism.find('M13-batch132-synchronous')
        self.assertEqual(cell['runs'][0]['batch'] * cell['runs'][0]['value'],
                         2 * 65_536)
        self.assertGreater(mechanism.ENCODED_132KB,
                           mechanism.FUA_THRESHOLD + 64 * 1024)
        # Within 1% of the plan's 132,462 B mainnet batch and above the threshold.
        self.assertLess(abs(mechanism.ENCODED_132KB - 132_462) / 132_462, 0.01)

    def test_m6_covers_every_plan_width_once(self):
        widths = {run['batch'] for cell in mechanism.cells()
                  if cell['family'] == 'M6' for run in cell['runs']
                  if cell['id'].startswith(('M6-w4', 'M6-w8', 'M6-w26', 'M6-page'))}
        self.assertEqual(widths, {4, 8, 26, 256})
        self.assertTrue(any(cell['id'] == 'M6-w1-c16'
                            and cell['runs'][0]['mix']['read'] == 100
                            for cell in mechanism.cells()))

    def test_only_retirement_cells_request_relocation(self):
        for cell in mechanism.cells():
            measure = [run for run in cell['runs'] if run['role'] == 'measure']
            requested = [run.get('relocation') for run in measure]
            if cell['family'] in ('M9', 'S1-6'):
                self.assertTrue(all(requested), cell['id'])
            else:
                self.assertFalse(any(requested), cell['id'])
            for run in cell['runs']:
                spec = mechanism.spec(cell, run, context(), path='/c', evidence='/e')
                self.assertEqual(spec['phases']['relocation'],
                                 run.get('relocation', False), cell['id'])

    def test_a_relocation_cell_declares_one_shape_for_every_run(self):
        # The coordinator's reopen rejects a changed shape, so the measure and
        # verify specs of one cell must declare the same keyspaces; only the
        # relocation phase (and the marker write) may differ.
        for cell in mechanism.cells():
            relocation = any(run.get('relocation') for run in cell['runs'])
            shapes = set()
            phases = []
            for run in cell['runs']:
                spec = mechanism.spec(cell, run, context(), path='/c', evidence='/e')
                self.assertEqual(
                    bool(spec['backend']['options'].get('relocation_marker', False)),
                    relocation, cell['id'])
                shapes.add(spec['backend']['options'].get('relocation_marker', False))
                phases.append(bool(spec['phases']['relocation']))
            self.assertEqual(len(shapes), 1, cell['id'])
            if relocation:
                self.assertTrue(any(phases), cell['id'])
                if len(cell['runs']) > 1:
                    # A reopen declares the same shape but writes no marker.
                    self.assertIn(False, phases, cell['id'])

    def test_verify_runs_are_read_only_and_explicit(self):
        for cell in mechanism.cells():
            verify = [run for run in cell['runs'] if run['role'] == 'verify']
            for run in verify:
                self.assertTrue(run['verify'])
                self.assertFalse(run['preload'])
                self.assertEqual(run['mix']['put'], 0)
                self.assertEqual(run['mix']['put_batch'], 0)
                self.assertEqual(run['mix']['delete'], 0)

    def test_the_snapshot_cut_outlives_the_snapshot_poll_and_byte_cadence(self):
        """The cut must be certain at the first 60 s poll, not a byte-volume race.

        `S1-2S-snapshot-native/attempt-3` ran the 3,500,000 row and completed
        3,565,536 operations in 101.1 s (35.3k put/s), yet no control region was
        stored: 14.3 GiB of value bytes crosses the production 8 GiB cadence at
        ~59 s, within a second of the first 60 s poll, and the next poll is at
        120 s, past the run. The cells now force `index.snapshot_written_bytes`
        to 256 MiB, an eighth of the production value, and keep 3,500,000
        operations so the run lasts 101 s at the measured rate.
        """
        for tier in mechanism.TIERS:
            cell = mechanism.find(f'S1-2S-snapshot-{tier}')
            run = cell['runs'][0]
            self.assertEqual(run['measurement'], mechanism.SNAPSHOT_MEASUREMENT)
            self.assertEqual(cell['index_overrides'],
                             dict(snapshot_written_bytes=mechanism.SNAPSHOT_ROOT_BYTES))
            self.assertEqual(mechanism.SNAPSHOT_ROOT_BYTES, 256 << 20)
            # 3,500,000 x 4,096 B at the measured 35.3k put/s is 101 s, so the
            # first poll at 60 s runs over data.
            self.assertGreater(mechanism.SNAPSHOT_MEASUREMENT * run['value'],
                               8 << 30)
            self.assertLessEqual(mechanism.SNAPSHOT_MEASUREMENT * run['value'],
                                 16 << 30)
            self.assertGreaterEqual(run['deadline'], 900)
            publication = mechanism.find(f'S1-2S-publication-{tier}')
            self.assertEqual(publication['runs'][0]['measurement'], 1_000_000)
            self.assertIsNone(publication['index_overrides'])

    def test_unknown_cell_fails(self):
        with self.assertRaisesRegex(ValueError, 'unknown mechanism cell'):
            mechanism.find('M99')


class RowSelection(unittest.TestCase):
    def test_a_refused_mutation_is_not_an_error_row(self):
        rows = [dict(phase='measurement', outcome='success', error_kind=''),
                dict(phase='measurement', outcome='unknown_mutation',
                     error_kind='coordinator lease lapsed'),
                dict(phase='measurement', outcome='error', error_kind='boom')]
        self.assertEqual([row['outcome'] for row in mechanism.refused(rows)],
                         ['unknown_mutation', 'error'])
        self.assertEqual([row['outcome'] for row in mechanism.failures(rows)],
                         ['error'])
        self.assertEqual(mechanism.acknowledged(rows), 1)


class Specs(unittest.TestCase):
    def test_spec_binds_the_deployment_and_a_stable_namespace(self):
        cell = mechanism.find('S1-2S-publication-native')
        run = cell['runs'][0]
        spec = mechanism.spec(cell, run, context(),
                              path='/owned/coordinator', evidence='/owned/evidence')
        options = spec['backend']['options']
        self.assertEqual(spec['backend']['kind'], 'tidehunter')
        self.assertEqual(options['path'], '/owned/coordinator')
        self.assertEqual(options['evidence'], '/owned/evidence')
        self.assertEqual(options['durability_tier'], 'native')
        self.assertEqual(options['external_owner_durability_receipts'],
                         ['r0.json', 'r1.json'])
        self.assertEqual(spec['key_namespace_hex'], mechanism.unit_namespace(cell['unit']))
        self.assertEqual(len(spec['key_namespace_hex']), 32)
        self.assertEqual(spec['phases']['measurement_operations'], run['measurement'])
        self.assertEqual(spec['limits']['deadline_seconds'], run['deadline'])
        self.assertNotIn('index', options['external_remote_config'],
                         'a publication cell uses the prepared deployment as written')

    def test_a_snapshot_spec_forces_only_the_root_cadence(self):
        cell = mechanism.find('S1-2S-snapshot-synchronous')
        base = context()
        spec = mechanism.spec(cell, cell['runs'][0], base,
                              path='/owned/coordinator', evidence='/owned/evidence')
        remote = spec['backend']['options']['external_remote_config']
        self.assertEqual(remote['index'], dict(snapshot_written_bytes=256 << 20))
        self.assertEqual(remote['read_cache_bytes'], 0)
        self.assertEqual(remote['storage'], {})
        self.assertNotIn('index', base['remote'],
                         'the prepared deployment is not mutated in place')

    def test_limit_covers_dataset_and_operations(self):
        for cell in mechanism.cells():
            for run in cell['runs']:
                spec = mechanism.spec(cell, run, context(),
                                      path='/c', evidence='/e')
                phases = spec['phases']
                operations = (run['keys'] if phases['preload'] or
                              phases['verify_existing_dataset'] else 0) \
                    + phases['warmup_operations'] + phases['measurement_operations']
                self.assertGreaterEqual(spec['limits']['max_operations'], operations)
                self.assertGreaterEqual(spec['limits']['max_bytes'],
                                        run['keys'] * run['value'])
                self.assertEqual(spec['execution']['mode'], 'closed_loop')
                self.assertGreaterEqual(spec['execution']['queue_capacity'],
                                        spec['execution']['concurrency'])

    def test_namespace_labels_fit_the_binary_limit(self):
        for cell in mechanism.cells():
            for run in cell['runs']:
                spec = mechanism.spec(cell, run, context(), path='/c', evidence='/e')
                self.assertLessEqual(len(spec['namespace']), 64)
                self.assertLessEqual(len(spec['case_id']), 160)

    def test_templates_are_written(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = mechanism.write(directory, ['M13-batch132-synchronous'])
            self.assertEqual(len(paths), 1)
            value = json.loads(paths[0].read_text())
            self.assertEqual(value['cell']['id'], 'M13-batch132-synchronous')
            self.assertEqual(value['template']['backend']['options']['durability_tier'],
                             'synchronous')


class Verification(unittest.TestCase):
    def _m6_run(self, cell_id, *, calls, read_rpc, device_reads, **extra):
        cell = mechanism.find(cell_id)
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, completed=calls, read_rpc=read_rpc,
                      device_reads=device_reads,
                      phase_counts={'realized_mix': {
                          'measurement.get_many': {'offered': calls, 'fraction': 1.0}}},
                      distributions={'measurement.get_many.api.success':
                                     {'count': calls, 'p50_upper_bound_ns': 10,
                                      'p99_upper_bound_ns': 20}},
                      **extra)
            return cell, mechanism.verify(cell, directory)

    def test_m6_reports_latency_and_owner_reads(self):
        # 26 keys x 16 concurrent x 25,999 B per member fits the 60.7 MB credit,
        # so two owners give exactly two RPCs per call and the bound is asserted.
        cell, record = self._m6_run('M6-w26-c16', calls=20_000,
                                    read_rpc=40_000, device_reads=520_000)
        self.assertTrue(record['ok'], record)
        checks = {check['check']: check for check in record['checks']}
        self.assertEqual(checks['owner read RPCs per call at most one per owner']
                         ['detail']['per_call'], 2.0)
        self.assertNotIn('not_applicable',
                         checks['owner read RPCs per call at most one per owner']['detail'])
        self.assertTrue(checks['device reads per measured key at most 1.05']['ok'])
        self.assertEqual(checks['device reads per measured key at most 1.05']
                         ['detail']['per_key'], 1.0)
        self.assertTrue(checks['native contention counter is not claimed']['ok'])

    def test_m6_page256_wave_over_credit_is_not_applicable(self):
        # 256 keys x 16 concurrent x 25,999 B = 106.5 MB is 1.75x the 60.7 MB
        # credit, so the dispatch splits each owner group and 92,017 RPCs for
        # 20,000 calls is not a violation of the per-owner statement.
        cell, record = self._m6_run(
            'M6-page256-c16', calls=20_000, read_rpc=92_017,
            device_reads=4_956_160)
        checks = {check['check']: check for check in record['checks']}
        rpc = checks['owner read RPCs per call at most one per owner']
        self.assertTrue(rpc['ok'], rpc)
        self.assertTrue(rpc['detail']['not_applicable'], rpc)
        self.assertEqual(rpc['detail']['per_call'], 4.60085)
        self.assertGreater(rpc['detail']['wave_ratio'], 1.75)
        self.assertLess(rpc['detail']['wave_ratio'], 1.76)
        self.assertEqual(rpc['detail']['member_frame_bytes'], 8_236)
        self.assertEqual(rpc['detail']['member_charge_bytes'], 25_999)
        self.assertEqual(rpc['detail']['credit_bytes'], 60_700_000)
        self.assertTrue(checks['device reads per measured key at most 1.05']['ok'],
                        checks)
        self.assertAlmostEqual(
            checks['device reads per measured key at most 1.05']['detail']['per_key'],
            0.968)
        self.assertTrue(record['ok'], record)

    def test_m6_device_read_ratio_above_limit_fails(self):
        cell, record = self._m6_run(
            'M6-page256-c16', calls=20_000, read_rpc=92_017,
            device_reads=6_144_000)
        checks = {check['check']: check for check in record['checks']}
        self.assertFalse(checks['device reads per measured key at most 1.05']['ok'])
        self.assertAlmostEqual(
            checks['device reads per measured key at most 1.05']['detail']['per_key'],
            1.2)
        self.assertFalse(record['ok'])

    def test_m6_requires_the_measurement_evidence(self):
        # read-evidence.json is cumulative through preload and warmup, so it
        # cannot stand in for the measurement-window device-read delta.
        cell, record = self._m6_run(
            'M6-w26-c16', calls=20_000, read_rpc=40_000,
            device_reads=None, measurement=False)
        checks = {check['check']: check for check in record['checks']}
        device = checks['device reads per measured key at most 1.05']
        self.assertFalse(device['ok'])
        self.assertIn('measurement-read-evidence.json', device['detail']['reason'])
        self.assertFalse(record['ok'])

    def test_m13_requires_a_covering_flush(self):
        cell = mechanism.find('M13-batch132-synchronous')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, live={'leased_frames_total': 1000,
                                       'leased_flush_covered_frames_total': 40,
                                       'fua_writes_total': 10})
            record = mechanism.verify(cell, directory)
        self.assertTrue(record['ok'], record)
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, live={'leased_frames_total': 1000,
                                       'leased_flush_covered_frames_total': 0,
                                       'fua_writes_total': 1000})
            failed = mechanism.verify(cell, directory)
        self.assertFalse(failed['ok'])

    def test_m9_needs_two_owner_rolls_and_a_retirement(self):
        cell = mechanism.find('M9-roll2-native')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory)
            retired = dict(reached=True, note='owner WAL file retired by the relocation pass',
                           observations=[dict(gone={'h1': [], 'h2': []}),
                                         dict(gone={'h1': ['/s/epoch_1/wal_0'], 'h2': []})])
            record = mechanism.verify(cell, directory, observations=dict(
                role='measure', owner_rolls={'h1': {'max_file_id': 3}},
                retirement=retired, retire_requests=0))
        self.assertTrue(record['ok'], record)
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory)
            record = mechanism.verify(cell, directory, observations=dict(
                role='measure', owner_rolls={'h1': {'max_file_id': 3}},
                retirement=dict(reached=True, observations=[dict(gone={'h1': []})]),
                retire_requests=4))
        self.assertFalse(record['ok'])
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory)
            record = mechanism.verify(cell, directory, observations=dict(
                role='measure', owner_rolls={'h1': {'max_file_id': 0}},
                retirement=retired))
        self.assertFalse(record['ok'])

    def test_verify_run_requires_reopen_and_a_clean_audit(self):
        cell = mechanism.find('S1-2S-publication-native')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, open_kind='reopen')
            record = mechanism.verify(cell, directory, observations=dict(
                role='verify', gate=dict(reached=True, label='x'),
                kill=dict(exit_code=-9)))
        checks = {check['check']: check['ok'] for check in record['checks']}
        self.assertTrue(checks['coordinator reopened the existing database'])
        self.assertTrue(checks['full dataset verified, no torn value'])
        self.assertTrue(checks['coordinator was killed, not shut down'])

    def test_the_owner_restart_verdict_leaves_the_failure_to_the_crash_part(self):
        """The verify run cannot carry the crash evidence, so it must not ask."""
        cell = mechanism.find('S1-3S-owner-native')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, open_kind='reopen')
            record = mechanism.verify(cell, directory, observations=dict(
                role='verify', gate=dict(reached=True, label='200 acknowledged writes'),
                kill=dict(pids=[18130]),
                restart=dict(previous=[18130], current=[18175])))
        names = [check['check'] for check in record['checks']]
        self.assertTrue(record['ok'], record)
        self.assertNotIn('coordinator recorded a typed write failure', names)
        self.assertIn('owner was killed with SIGKILL', names)
        self.assertIn('coordinator reopened the existing database', names)

    def test_wrong_value_fails_the_audit(self):
        cell = mechanism.find('S1-3S-owner-native')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, open_kind='reopen', errors=1)
            record = mechanism.verify(cell, directory, observations=dict(
                role='verify', restart=dict(previous=[1], current=[2])))
        self.assertFalse(record['ok'])

    def test_missing_artifact_is_reported_not_guessed(self):
        cell = mechanism.find('M6-w1-c16')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'missing artifact'):
                mechanism.verify(cell, directory)


class Stages(unittest.TestCase):
    """F23: the S1 fault cells are stage-1 entries; M8 is a derived view."""

    def test_every_s1_cell_is_recorded_under_stage_one(self):
        s1 = [cell for cell in mechanism.cells() if cell['id'].startswith('S1-')]
        self.assertEqual(len(s1), 18)
        self.assertEqual({cell['stage'] for cell in s1}, {'stage1'})
        self.assertEqual({cell['stage'] for cell in mechanism.cells()
                          if cell['id'].startswith('M')},
                         {'stage2'})

    def test_the_m8_aliases_are_the_s1_5s_cells(self):
        aliases = mechanism.aliases()
        self.assertEqual(len(aliases), 8)
        self.assertEqual(set(aliases.values()),
                         {cell['id'] for cell in mechanism.cells()
                          if cell['family'] == 'S1-5'})
        for alias, cell_id in aliases.items():
            self.assertEqual(mechanism.find(cell_id)['stage'], 'stage1')
            self.assertTrue(alias.startswith('M8-'))
        self.assertEqual(aliases['M8-stale-synchronous'], 'S1-5S-stale-synchronous')

    def test_the_m8_view_carries_the_recorded_s1_5s_entry(self):
        ledger = type('Ledger', (), dict(entries={}))()
        cell_id = 'S1-5S-stale-native'
        ledger.entries[('stage1', cell_id)] = dict(
            id=cell_id, stage='stage1', unit=cell_id, status='passed',
            attempts=1, config_hash='h', result_paths=[dict(path='/x')])
        rows = mechanism.alias_entries(ledger)
        row = next(row for row in rows if row['id'] == 'M8-stale-native')
        self.assertEqual((row['stage'], row['alias_of']), ('stage2', cell_id))
        self.assertEqual((row['status'], row['attempts']), ('passed', 1))
        self.assertEqual(row['result_paths'], [dict(path='/x')])
        # A cell that never ran is absent, never invented.
        self.assertEqual([row['id'] for row in rows], ['M8-stale-native'])


class Gates(unittest.TestCase):
    """The mechanism spec carries the binary-side measurement handshake."""

    def test_the_spec_declares_two_distinct_pairs_under_the_evidence(self):
        cell = mechanism.find('M6-w1-c16')
        spec = mechanism.spec(cell, cell['runs'][0], context(),
                              path='/c', evidence='/e')
        begin, end = spec['measurement_gate'], spec['measurement_completion_gate']
        self.assertEqual(begin['ready_file'], '/e/measurement.ready')
        self.assertEqual(begin['release_file'], '/e/measurement.release')
        self.assertEqual(end['ready_file'], '/e/measurement-complete.ready')
        self.assertEqual(len({begin['ready_file'], begin['release_file'],
                              end['ready_file'], end['release_file']}), 4)
        self.assertEqual(begin['timeout_seconds'], mechanism.MEASUREMENT_GATE_SECONDS)

    def test_preflight_names_a_missing_manifest_key(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / 'samples.csv').write_text(
                'phase,operation,outcome,api_ns,completed_ns,bytes\n'
                'measurement,read,success,1,1,0\n')
            (output / 'manifest.json').write_text('{"status": "completed"}')
            (output / 'summary.json').write_text(
                '{"phase_counts": {"realized_mix": {}}}')
            problems = mechanism.preflight(output)
        self.assertEqual(problems, ['manifest.json lacks key(s): '
                                    'errors_or_wrong_values, completed_operations'])


if __name__ == '__main__':
    unittest.main()
