"""Stage-3 spec set: 72 cells at the admitted 256 B and 8,192 B sizes.

Every case declares its coordinator value cache in its `-cacheN` tag. The 70
grid cells and the native baseline declare 0; the two reference cells (one per
tier, read-only C=16) declare 1,000.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import stage3_specs


class StageThreeSpecs(unittest.TestCase):
    def test_case_list_matches_the_plan(self):
        cases = stage3_specs.cells()
        self.assertEqual(len(cases), 72)
        self.assertEqual(len({case['case_id'] for case in cases}), 72)
        counts = {}
        for case in cases:
            family = case['case_id'].split('-')[1][:2]
            counts[family] = counts.get(family, 0) + 1
        # A1 carries the two cache-on reference cells on top of its eight.
        self.assertEqual(counts, {'A1': 10, 'A2': 8, 'A3': 8, 'A4': 6, 'A5': 6,
                                  'A6': 6, 'A7': 4, 'A8': 4, 'A9': 4, 'B1': 8, 'B2': 8})

    def test_every_non_reference_spec_declares_the_value_cache_off(self):
        reference = []
        for case in stage3_specs.cells():
            self.assertEqual(stage3_specs.declared_cache(case['case_id']), case['cache'])
            if case['reference']:
                reference.append(case)
            else:
                self.assertEqual(case['cache'], 0, case['case_id'])
                self.assertIn('-cache0', case['case_id'])
        self.assertEqual(len(reference), 2)
        for case in reference:
            self.assertEqual(case['cache'], 1000)
            self.assertIn('-cache1000', case['case_id'])
            self.assertEqual((case['arm'], case['mix'], case['size'],
                              case['concurrency']), ('A', 'read', 8192, 16))
        self.assertEqual({case['tier'] for case in reference},
                         {'native', 'synchronous'})

    def test_only_admitted_sizes_and_group_widths(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = stage3_specs.write(tmp)
            self.assertEqual(len(paths), 72)
            for path in paths:
                document = json.loads(path.read_text())
                self.assertIn(document['dataset']['value_size'], (256, 8192))
                self.assertEqual(document['dataset']['keys'], 65536)
                self.assertEqual(document['seed'], 1775)
                self.assertEqual(document['limits']['max_operations'], 3_000_000)
                self.assertEqual(document['namespace'], document['case_id'])
                cache = stage3_specs.declared_cache(document['case_id'])
                self.assertIn(f'-cache{cache}', document['namespace'])
                if '-cache1000' not in document['case_id']:
                    self.assertEqual(cache, 0)
                # The cache is declared in the label; the engine document keeps
                # only fields kv-research admits (no unknown cache key).
                self.assertEqual(set(document), {
                    'schema_version', 'case_id', 'backend', 'seed', 'namespace',
                    'dataset', 'workload', 'execution', 'phases', 'diagnostics',
                    'qualification_only', 'source_revision', 'limits'})
                mix = document['workload']['mix']
                if 'put_batch' in path.name or 'get_many' in path.name or 'A7' in path.name or 'A8' in path.name:
                    self.assertEqual(document['workload']['batch_size'], 26)
                    self.assertTrue(mix['put_batch'] or mix['get_many'])
                if path.name.startswith('S3-B'):
                    self.assertEqual(document['backend']['kind'], 'native')
                else:
                    self.assertEqual(document['backend']['options']['owners'], 2)
                    self.assertIn(document['backend']['options']['durability_tier'],
                                  ('native', 'synchronous'))
                self.assertIn(document['workload']['distribution'],
                              ('uniform', {'zipf': {'exponent': 1.0}}))

    def test_every_document_passes_runconfig_admission(self):
        binary = os.environ.get('KV_RESEARCH_BIN')
        if not binary:
            self.skipTest('set KV_RESEARCH_BIN to validate RunConfig admission')
        with tempfile.TemporaryDirectory() as tmp:
            for path in stage3_specs.write(tmp):
                subprocess.run([binary, '--config', str(path), '--validate-config',
                                '--output', str(Path(tmp) / 'admission')],
                               check=True, stdout=subprocess.DEVNULL, timeout=60)


if __name__ == '__main__':
    unittest.main()
