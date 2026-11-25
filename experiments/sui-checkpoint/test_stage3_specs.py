"""Stage-3 spec set: 70 cells at the admitted 256 B and 8,192 B sizes."""
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
        self.assertEqual(len(cases), 70)
        self.assertEqual(len({case['case_id'] for case in cases}), 70)
        counts = {}
        for case in cases:
            family = case['case_id'].split('-')[1][:2]
            counts[family] = counts.get(family, 0) + 1
        self.assertEqual(counts, {'A1': 8, 'A2': 8, 'A3': 8, 'A4': 6, 'A5': 6,
                                  'A6': 6, 'A7': 4, 'A8': 4, 'A9': 4, 'B1': 8, 'B2': 8})

    def test_only_admitted_sizes_and_group_widths(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = stage3_specs.write(tmp)
            self.assertEqual(len(paths), 70)
            for path in paths:
                document = json.loads(path.read_text())
                self.assertIn(document['dataset']['value_size'], (256, 8192))
                self.assertEqual(document['dataset']['keys'], 65536)
                self.assertEqual(document['seed'], 1775)
                self.assertEqual(document['limits']['max_operations'], 3_000_000)
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
