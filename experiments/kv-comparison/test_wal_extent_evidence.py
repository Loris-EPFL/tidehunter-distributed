import copy
import unittest
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import cold_device_trace as trace
import wal_extent_evidence as wal


class OwnedWalEvidence(unittest.TestCase):
    def evidence(self):
        return {'available': True, 'selection_truncated': False,
                'files': [{'identity': [1, 2, 8192, 3, 4], 'excluded': [], 'truncated': False,
                           'extents': [{'sector': 100, 'sectors': 16}]}]}

    def parsed(self, *lines):
        return trace.parse_identity([*lines, '@events: ' + str(len(lines))], 1, 1000)

    def test_unrelated_and_boundary_overlapping_device_reads_are_excluded(self):
        events = self.parsed('I 10', 'S 10 100 4096', 'C 10 20',
                             'I 30', 'S 30 200 4096', 'C 30 60',
                             'I 70', 'S 70 115 1024', 'C 70 100')
        before = self.evidence()
        result = wal.classify(events, before, copy.deepcopy(before))
        self.assertTrue(result['qualified'])
        self.assertEqual((result['read_ops'], result['read_bytes'], result['unattributed_requests']), (1, 4096, 2))
        self.assertEqual(result['raw_intervals_ns'], [10])

    def test_extent_or_file_replacement_invalidates_the_population(self):
        events = self.parsed('I 10', 'S 10 100 4096', 'C 10 20')
        before = self.evidence()
        for key, value in [('identity', [1, 9, 8192, 3, 4]), ('extents', [{'sector': 200, 'sectors': 16}])]:
            after = copy.deepcopy(before)
            after['files'][0][key] = value
            with self.subTest(key=key):
                result = wal.classify(events, before, after)
                self.assertFalse(result['available'])
                self.assertNotIn('read_ops', result)

    def test_old_trace_without_geometry_is_unknown_not_zero_owned_io(self):
        events = self.parsed('I 10', 'C 10 20')
        result = wal.classify(events, self.evidence(), self.evidence())
        self.assertFalse(result['available'])
        self.assertNotIn('read_ops', result)

    def test_lost_or_erroneous_request_trace_cannot_be_attributed(self):
        for lines in [('I 10', 'S 10 100 4096', 'E 20'),
                      ('I 10', 'S 10 100 4096', 'C 10 20', '@events: 4')]:
            with self.subTest(lines=lines):
                result = wal.classify(self.parsed(*lines), self.evidence(), self.evidence())
                self.assertFalse(result['qualified'])

    def test_equal_start_timestamps_do_not_merge_distinct_requests(self):
        events = self.parsed('I 10', 'S 10 100 4096', 'I 10', 'S 10 200 4096', 'C 10 20', 'C 10 30')
        result = wal.classify(events, self.evidence(), self.evidence())
        self.assertFalse(result['qualified'])
        self.assertEqual(result['ambiguous_start_timestamps'], 1)
        self.assertEqual(result['unattributed_requests'], 2)




class ExtentSourceConfinement(unittest.TestCase):
    def test_root_and_intermediate_symlinks_cannot_escape_owned_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            real = root / 'real'
            real.mkdir()
            (real / 'child').mkdir()
            (root / 'link').symlink_to(real, target_is_directory=True)
            for path in (root / 'link', root / 'link/child'):
                with self.subTest(path=path), self.assertRaises(OSError):
                    wal.open_directory(path)

    def test_fifo_replacement_is_rejected_without_waiting_for_a_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            fifo = Path(temporary) / 'wal_0000000000000000'
            os.mkfifo(fifo)
            program = '''import sys
import wal_extent_evidence as wal
try:
    wal.file_extents(sys.argv[1], 0)
except ValueError:
    sys.exit(0)
sys.exit(7)
'''
            result = subprocess.run([sys.executable, '-B', '-c', program, str(fifo)],
                                    capture_output=True, timeout=2,
                                    cwd=Path(wal.__file__).resolve().parent)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
if __name__ == '__main__':
    unittest.main()
