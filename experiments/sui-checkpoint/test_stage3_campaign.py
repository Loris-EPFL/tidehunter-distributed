"""Stage-3 driver resume: skip passed, rerun failed/interrupted/stale, fresh DB."""
import json
import os
from pathlib import Path
import stat
import tempfile
import textwrap
import unittest

import campaign_ledger as ledger_module
import stage3_campaign


FAKE = textwrap.dedent("""\
    #!/usr/bin/env python3
    import argparse, json, os
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument('--config')
    parser.add_argument('--output')
    args = parser.parse_args()
    case = Path(args.config).stem
    log = Path(os.environ['CALL_LOG'])
    with log.open('a') as stream:
        stream.write(case + '\\n')
    if case in os.environ.get('FAIL_CASES', '').split(','):
        raise SystemExit(2)
    out = Path(args.output)
    out.mkdir(parents=True)
    (out / 'summary.json').write_text(json.dumps({'case': case, 'complete': True}))
    (out / 'samples.csv').write_text('sample\\n')
    """)


class Driver(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'campaign'
        spec_dir = self.root / 'stage3' / 'spec'
        spec_dir.mkdir(parents=True)
        self.binary = Path(self.tmp.name) / 'kv'
        self.binary.write_text(FAKE)
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        self.log = Path(self.tmp.name) / 'calls.txt'
        self.log.write_text('')
        self.old = dict(os.environ)
        os.environ['CALL_LOG'] = str(self.log)
        self.addCleanup(self.restore)
        self.cells = []
        for name in ('S3-A1-c2-native-cache0', 'S3-A2-c2-native-cache0',
                     'S3-A3-c2-native-cache0'):
            path = spec_dir / f'{name}.json'
            path.write_text(json.dumps({'case_id': name}))
            self.cells.append(dict(id=name, stage='stage3', unit=name, path=str(path),
                                   config_hash=ledger_module.config_hash(
                                       spec=ledger_module.sha256_file(path),
                                       knobs={}, engine_rev='e', sui_rev='s',
                                       binary_ids={'kv': 'b'})))

    def restore(self):
        os.environ.clear()
        os.environ.update(self.old)

    def calls(self):
        return self.log.read_text().splitlines()

    def run_campaign(self, *, ledger=None, cells=None, stop=True):
        ledger = ledger or ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json')
        return stage3_campaign.run_campaign(
            cells or self.cells, binary=self.binary, root=self.root, ledger=ledger,
            stop_on_failure=stop)

    def test_failed_cell_is_retried_and_passed_cells_are_skipped(self):
        os.environ['FAIL_CASES'] = self.cells[1]['id']
        results, planned = self.run_campaign()
        self.assertEqual([row['id'] for row in planned],
                         [cell['id'] for cell in self.cells])
        self.assertEqual([row['status'] for row in results], ['passed', 'failed'])
        saved = ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load()
        self.assertEqual(saved.status('stage3', self.cells[0]['id']), 'passed')
        self.assertEqual(saved.status('stage3', self.cells[1]['id']), 'failed')
        self.assertIsNone(saved.status('stage3', self.cells[2]['id']))
        del os.environ['FAIL_CASES']
        before = len(self.calls())
        results, planned = self.run_campaign(ledger=saved)
        self.assertEqual([row['id'] for row in planned],
                         [self.cells[1]['id'], self.cells[2]['id']])
        self.assertEqual(len(self.calls()) - before, 2)
        self.assertTrue(all(row['status'] == 'passed' for row in results))

    def test_interrupted_cell_reruns_alone(self):
        first, _ = self.run_campaign()
        saved = ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load()
        # Simulate a crash after the first cell's next attempt began.
        saved.begin(stage='stage3', cell_id=self.cells[1]['id'], config_hash='new')
        saved.save()
        before = len(self.calls())
        results, planned = self.run_campaign(
            ledger=ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load())
        self.assertEqual([row['id'] for row in planned], [self.cells[1]['id']])
        self.assertEqual(len(self.calls()) - before, 1)
        self.assertEqual(results[0]['status'], 'passed')

    def test_changed_config_hash_marks_a_passed_cell_stale(self):
        self.run_campaign()
        changed = [dict(self.cells[0]), dict(self.cells[1]), dict(self.cells[2])]
        changed[1]['config_hash'] = 'changed'
        before = len(self.calls())
        results, planned = self.run_campaign(
            ledger=ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load(),
            cells=changed)
        self.assertEqual([row['id'] for row in planned], [self.cells[1]['id']])
        self.assertEqual(len(self.calls()) - before, 1)

    def test_only_and_from_select_cells(self):
        before = len(self.calls())
        _, planned = self.run_campaign(cells=self.cells)
        self.assertEqual(len(self.calls()) - before, 3)
        saved = ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load()
        _, planned = stage3_campaign.run_campaign(
            self.cells, binary=self.binary, root=self.root, ledger=saved,
            from_=self.cells[2]['id'])
        self.assertEqual(planned, [])

    def test_previous_database_and_output_are_archived_not_reused(self):
        database = self.root / 'stage3' / 'db' / self.cells[0]['id']
        output = self.root / 'stage3' / 'out' / self.cells[0]['id']
        database.mkdir(parents=True)
        output.mkdir(parents=True)
        (database / 'leftover').write_text('old')
        (output / 'leftover').write_text('old')
        self.run_campaign(cells=self.cells[:1])
        stale = list((self.root / 'stage3' / 'stale').rglob('leftover'))
        self.assertEqual(len(stale), 2)
        self.assertFalse((database / 'leftover').exists())
        self.assertFalse((output / 'leftover').exists())
        saved = ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load()
        self.assertEqual(saved.entries[('stage3', self.cells[0]['id'])]['start_from'],
                         'fresh')

    def test_missing_summary_never_passes(self):
        self.binary.write_text(self.binary.read_text().replace(
            "(out / 'summary.json').write_text(json.dumps({'case': case, 'complete': True}))",
            'pass'))
        results, _ = self.run_campaign(cells=self.cells[:1])
        self.assertEqual(results[0]['status'], 'failed')
        saved = ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json').load()
        self.assertEqual(saved.status('stage3', self.cells[0]['id']), 'failed')


if __name__ == '__main__':
    unittest.main()
