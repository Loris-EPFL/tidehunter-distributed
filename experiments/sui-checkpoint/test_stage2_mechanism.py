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
              read_rpc=10, open_kind='create', recovery=None, live=None):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'summary.json').write_text(json.dumps(dict(
        status=status, durability={}, started_unix_seconds=0.0,
        finished_unix_seconds=1.0, backend={}, execution={},
        phase_counts=phase_counts or {'realized_mix': {}},
        distributions=distributions or {}, limitations=[])))
    (directory / 'manifest.json').write_text(json.dumps(dict(
        status=status, completed_operations=completed,
        errors_or_wrong_values=errors)))
    live_lines = [f'tidehunter_nvmeof_live_{name} {value}'
                  for name, value in (live or {}).items()]
    (directory / 'live.prom').write_text('\n'.join(live_lines) + '\n')
    (directory / 'measurement-read-evidence.json').write_text(json.dumps(dict(
        start=dict(coordinator=dict(read_rpc_count=read_rpc),
                   coordinator_open=dict(kind=open_kind,
                                         recovery=recovery or dict(
                                             tail_batches=3, tail_bytes=4096,
                                             below_cut_skipped=0, fencing_ns=10))),
        deltas=dict(coordinator=dict(available=True,
                                     counters=dict(read_rpc_count=read_rpc))))))
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
        for tier in mechanism.TIERS:
            cell = mechanism.find(f'S1-2S-snapshot-{tier}')
            run = cell['runs'][0]
            self.assertEqual(run['measurement'], mechanism.SNAPSHOT_MEASUREMENT)
            # The first snapshot check is 60 s after open and the root trigger
            # is 8 GiB of value bytes; the run must pass both.
            self.assertGreater(run['measurement'] * run['value'], 8 << 30)
            self.assertLessEqual(run['measurement'] * run['value'], 16 << 30)
            self.assertGreaterEqual(run['deadline'], 900)
            publication = mechanism.find(f'S1-2S-publication-{tier}')
            self.assertEqual(publication['runs'][0]['measurement'], 1_000_000)

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
    def test_m6_reports_latency_and_owner_reads(self):
        cell = mechanism.find('M6-w26-c16')
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory, completed=20_000, read_rpc=40_000,
                      phase_counts={'realized_mix': {
                          'measurement.get_many': {'offered': 20_000, 'fraction': 1.0}}},
                      distributions={'measurement.get_many.api.success':
                                     {'count': 20_000, 'p50_upper_bound_ns': 10,
                                      'p99_upper_bound_ns': 20}})
            record = mechanism.verify(cell, directory)
        self.assertTrue(record['ok'], record)
        checks = {check['check']: check for check in record['checks']}
        self.assertEqual(checks['owner read RPCs per call at most one per owner']
                         ['detail']['per_call'], 2.0)
        self.assertTrue(checks['native contention counter is not claimed']['ok'])

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
            record = mechanism.verify(cell, directory, observations=dict(
                role='measure', owner_rolls={'h1': {'max_file_id': 3}},
                retire_requests=4))
        self.assertTrue(record['ok'], record)
        with tempfile.TemporaryDirectory() as directory:
            write_run(directory)
            record = mechanism.verify(cell, directory, observations=dict(
                role='measure', owner_rolls={'h1': {'max_file_id': 0}},
                retire_requests=0))
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
