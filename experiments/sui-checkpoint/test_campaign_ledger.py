"""Resume ledger: atomic writes, bucket merge, artifact gating and planning."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import campaign_ledger as ledger


def cell(cell_id, *, stage='stage3', unit=None, config_hash='h'):
    return dict(id=cell_id, stage=stage, unit=unit or cell_id,
                config_hash=config_hash)


class AtomicWrite(unittest.TestCase):
    def test_failed_rename_keeps_the_previous_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ledger.json'
            value = ledger.Ledger(path)
            value.begin(stage='s', cell_id='a', config_hash='h')
            value.save()
            before = path.read_bytes()
            value.begin(stage='s', cell_id='b', config_hash='h')
            with mock.patch.object(ledger.os, 'replace', side_effect=OSError('crash')):
                with self.assertRaises(OSError):
                    value.save()
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ['ledger.json'])
            self.assertEqual(ledger.Ledger(path).load().entries[('s', 'a')]['status'],
                             'failed')

    def test_running_entry_is_interrupted_on_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ledger.json'
            value = ledger.Ledger(path)
            value.begin(stage='s', cell_id='a', config_hash='h')
            value.save()
            loaded = ledger.Ledger(path).load()
            self.assertEqual(loaded.status('s', 'a'), 'failed')
            self.assertEqual(loaded.entries[('s', 'a')]['failure'], 'interrupted')


class FakeStore:
    def __init__(self, objects=None, conflict_once=False, uploaded_sha=None):
        self.objects = dict(objects or {})
        self.generation = {key: 1 for key in self.objects}
        self.conflict_once = conflict_once
        self.uploaded_sha = uploaded_sha or {}

    def get(self, key):
        if key not in self.objects:
            raise KeyError(key)
        return self.objects[key], self.generation[key]

    def put(self, key, data, generation):
        if self.conflict_once:
            self.conflict_once = False
            self.objects[key] = ledger.Ledger().to_bytes()
            self.generation[key] = generation + 1
            raise ledger.PreconditionFailed(key)
        if self.generation.get(key, 0) != generation:
            raise ledger.PreconditionFailed(key)
        self.objects[key] = data
        self.generation[key] = generation + 1
        return self.generation[key]

    def head(self, key):
        return dict(size=len(self.objects[key]),
                    sha256=self.uploaded_sha.get(key, ledger.sha256_bytes(self.objects[key])))


class BucketCopy(unittest.TestCase):
    def test_conflict_merges_the_concurrent_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote = ledger.Ledger()
            remote.begin(stage='s', cell_id='remote', config_hash='h')
            store = FakeStore({'ledger.json': remote.to_bytes()}, conflict_once=True)
            mine = ledger.Ledger(Path(tmp) / 'l.json')
            mine.begin(stage='s', cell_id='mine', config_hash='h')
            mine.save_store(store, 'ledger.json')
            stored = ledger.Ledger.from_bytes(store.objects['ledger.json'])
            self.assertEqual(set(stored.entries), {('s', 'remote'), ('s', 'mine')})

    def test_upload_must_match_the_verified_local_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'result.json'
            path.write_text('result\n')
            digest = ledger.sha256_file(path)
            entry = ledger.Ledger(Path(tmp) / 'l.json').begin(
                stage='s', cell_id='a', config_hash='h')
            artifact = dict(path=str(path), key='r', size=path.stat().st_size,
                            sha256=digest)
            with self.assertRaises(ValueError):
                ledger.Ledger().mark_passed(
                    entry, [artifact],
                    store=FakeStore({'r': b'other'}, uploaded_sha={'r': '0' * 64}))
            self.assertNotEqual(entry['status'], 'passed')
            ledger.Ledger().mark_passed(entry, [artifact], store=FakeStore({'r': path.read_bytes()},
                                                                          uploaded_sha={'r': digest}))
            self.assertEqual(entry['status'], 'passed')


class ArtifactGate(unittest.TestCase):
    def test_missing_or_changed_artifact_never_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'summary.json'
            path.write_text('{}\n')
            entry = ledger.Ledger(Path(tmp) / 'l.json').begin(
                stage='s', cell_id='a', config_hash='h')
            good = dict(path=str(path), size=path.stat().st_size,
                        sha256=ledger.sha256_file(path))
            for bad in (dict(good, size=good['size'] + 1),
                        dict(good, sha256='0' * 64),
                        dict(good, path=str(Path(tmp) / 'missing'))):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    ledger.Ledger().mark_passed(entry, [bad])
                self.assertNotEqual(entry['status'], 'passed')
            ledger.Ledger().mark_passed(entry, [good])
            self.assertEqual(entry['status'], 'passed')


class NotReached(unittest.TestCase):
    """A point the replay cannot produce is labelled, not failed."""

    def test_a_not_reached_cell_is_recorded_and_runs_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ledger.json'
            record = ledger.Ledger(path)
            entry = record.begin(stage='stage1', cell_id='S1-4-during-flush-synchronous',
                                 config_hash='h')
            record.not_reached(entry, 'not reached: no flush in replay')
            record.save()
            loaded = ledger.Ledger(path)
            loaded.load()
            self.assertEqual(loaded.status('stage1', 'S1-4-during-flush-synchronous'),
                             'not_reached')
            self.assertIn('no flush in replay',
                          loaded.entries[('stage1', 'S1-4-during-flush-synchronous')]['failure'])
            planned = ledger.plan([dict(id='S1-4-during-flush-synchronous', stage='stage1',
                                        config_hash='h')], loaded)
            self.assertEqual([row['id'] for row in planned],
                             ['S1-4-during-flush-synchronous'])

    def test_the_status_is_part_of_the_admitted_set(self):
        self.assertIn('not_reached', ledger.STATUSES)


class Planning(unittest.TestCase):
    def ledger(self, statuses, hashes=None):
        value = ledger.Ledger()
        hashes = hashes or {}
        for cell_id, status in statuses.items():
            entry = value.begin(stage='stage3', cell_id=cell_id,
                                config_hash=hashes.get(cell_id, 'h'))
            entry['status'] = status
        value.reconcile()
        return value

    def test_resume_skips_passed_and_reruns_everything_else(self):
        cells = [cell('a'), cell('b'), cell('c'), cell('d')]
        value = self.ledger({'a': 'passed', 'b': 'failed', 'c': 'running',
                             'd': 'passed'}, {'d': 'old'})
        self.assertEqual([row['id'] for row in ledger.plan(cells, value)],
                         ['b', 'c', 'd'])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, only=['a'])], [])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, only=['d'])], ['d'])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, from_='c')],
                         ['c', 'd'])
        with self.assertRaises(ValueError):
            ledger.plan(cells, value, only=['missing'])

    def test_unit_members_run_together(self):
        cells = [cell('a1', unit='a'), cell('a2', unit='a'), cell('b')]
        value = self.ledger({'a1': 'passed', 'a2': 'failed'})
        self.assertEqual([row['id'] for row in ledger.plan(cells, value)], ['a1', 'a2', 'b'])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, only=['a1'])],
                         ['a1', 'a2'])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, from_='a2')],
                         ['a1', 'a2', 'b'])

    def test_config_hash_changes_with_any_binding(self):
        base = dict(spec='s', knobs={'c': 1}, engine_rev='e', sui_rev='u',
                    binary_ids={'kv': 'b'})
        value = ledger.config_hash(**base)
        self.assertEqual(value, ledger.config_hash(**base))
        for field, changed in (('spec', 's2'), ('engine_rev', 'e2'),
                               ('sui_rev', 'u2'), ('binary_ids', {'kv': 'b2'})):
            with self.subTest(field=field):
                self.assertNotEqual(value, ledger.config_hash(**(base | {field: changed})))


class ReviewedCells(unittest.TestCase):
    """The reviewed manual_sui table fixes ids, geometry and criteria (F8, F25)."""

    def test_every_id_is_unique_and_reaches_its_row(self):
        rows = ledger.MANUAL_SUI_CELLS
        ids = [row['id'] for row in rows]
        self.assertEqual(len(ids), len(set(ids)))
        for row in rows:
            self.assertIn(row['stage'], ledger.REVIEWED_STAGES)
            self.assertIn(row['tier'], ('native', 'synchronous'))
            self.assertEqual(ledger.cell(row['id']), row)
        self.assertIsNone(ledger.cell('not-a-reviewed-cell'))

    def test_the_stage_one_lifecycle_and_cache_rows_exist_per_tier(self):
        ids = {row['id'] for row in ledger.MANUAL_SUI_CELLS}
        for required in ('S1-0', 'S1-1-native', 'S1-1-synchronous',
                         'S1-7-cache0-native', 'S1-7-cache128-native',
                         'S1-7-cache1000-native', 'S1-7-cache1000-synchronous',
                         'S1-7-blob1024-native', 'S1-8-native', 'S1-8-synchronous',
                         'S1-4-mid-frame-native', 'S1-4-after-adoption-synchronous',
                         'S1-4-during-flush-synchronous'):
            self.assertIn(required, ids)
        self.assertNotIn('S1-4-during-flush-native', ids,
                         'a native flush edge is unreachable')

    def test_the_production_geometry_is_the_default_and_the_mechanism_rows_force_theirs(self):
        for row in ledger.MANUAL_SUI_CELLS:
            expected_fragment = 64 if row['id'].startswith(('M3-', 'M4-')) else 1024
            self.assertEqual(row['fragment_mib'], expected_fragment, row['id'])
            self.assertEqual(row['coordinator_cache_mib'], 0, row['id'])
        for row in ledger.MANUAL_SUI_CELLS:
            if row['id'].startswith('M3-'):
                self.assertEqual((row['max_maps'], row['max_index_maps']), (3, 3))
            else:
                self.assertEqual((row['max_maps'], row['max_index_maps']), (None, None))
        hold = ledger.cell('M5-hold')
        self.assertEqual(hold['hold_seconds'], 1800)
        self.assertIn('hold', hold['requires'])

    def test_run_arguments_round_trip(self):
        self.assertEqual(ledger.run_arguments(ledger.cell('S1-0')),
                         ['--checkpoints', '4'])
        self.assertEqual(ledger.run_arguments(ledger.cell('S1-1-native')),
                         ['--lifecycle-checks', '--tail-checkpoints', '64'])
        self.assertEqual(ledger.run_arguments(ledger.cell('M1-c4-native')),
                         ['--driver-concurrency', '4'])

    def test_a_reviewed_stage_is_the_only_filter(self):
        self.assertEqual({row['stage'] for row in ledger.cells('stage1')}, {'stage1'})
        self.assertEqual({row['stage'] for row in ledger.cells('stage2')}, {'stage2'})
        with self.assertRaises(ValueError):
            ledger.cells('stage3')


class TerminalInvalid(unittest.TestCase):
    """A second `not_reached` attempt is terminal (F22)."""

    def test_an_invalid_cell_is_never_planned_again(self):
        cells = [cell('a'), cell('b')]
        value = ledger.Ledger()
        entry = value.begin(stage='stage3', cell_id='a', config_hash='h')
        value.invalidate(entry, 'not reached on attempt 2: no flush edge')
        self.assertEqual([row['id'] for row in ledger.plan(cells, value)], ['b'])
        self.assertEqual([row['id'] for row in ledger.plan(cells, value, only=['a'])], [])

    def test_a_pending_not_reached_cell_still_runs_once_more(self):
        cells = [cell('a')]
        value = ledger.Ledger()
        entry = value.begin(stage='stage3', cell_id='a', config_hash='h')
        value.not_reached(entry, 'no flush edge')
        self.assertEqual([row['id'] for row in ledger.plan(cells, value)], ['a'])


if __name__ == '__main__':
    unittest.main()
