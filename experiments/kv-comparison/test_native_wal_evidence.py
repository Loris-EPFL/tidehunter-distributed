import hashlib
import tempfile
from pathlib import Path
import unittest

import cloud_controller as controller


class NativeWalEvidenceTests(unittest.TestCase):
    def test_counter_delta_excludes_preload_and_retains_raw_boundary_exports(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'result').mkdir()
            source = root / 'result/live.prom'
            before = ('# TYPE wal_written_bytes_type counter\n'
                      'wal_written_bytes_type{kind="Record",ks="default"} 100000\n'
                      'wal_written_bytes_type{kind="Index",ks="default"} 1024\n')
            source.write_text(before)
            start = controller.native_wal_snapshot(root, 'start')
            self.assertTrue(start['available'])
            self.assertEqual(start['sha256'], hashlib.sha256(before.encode()).hexdigest())
            self.assertEqual((root / start['path']).read_text(), before)
            source.write_text(before.replace('100000', '102048').replace('1024\n', '1152\n'))
            end = controller.native_wal_snapshot(root, 'end')
            evidence = controller.native_wal_interval(start, end)
            self.assertTrue(evidence['available'])
            self.assertEqual(evidence['logical_wal_bytes'], 2048 + 128)
            self.assertNotEqual(evidence['logical_wal_bytes'], sum(end['series'].values()))

    def test_missing_reset_or_changed_series_are_unavailable_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(controller.native_wal_snapshot(tmp, 'start')['available'])
        start = {'available': True, 'series': {'record': 100}}
        for end in ({'available': False}, {'available': True, 'series': {'record': 99}},
                    {'available': True, 'series': {'record': 101, 'index': 1}}):
            self.assertFalse(controller.native_wal_interval(start, end)['available'])
        self.assertFalse(controller.native_wal_interval(None, start)['available'])

    def test_invalid_counter_values_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'result').mkdir()
            source = root / 'result/live.prom'
            for value in ('NaN', '-1', '1.5', '9007199254740993'):
                source.write_text('wal_written_bytes_type{kind="Record"} ' + value + '\n')
                self.assertFalse(controller.native_wal_snapshot(root, 'start')['available'])
            source.write_text('wal_written_bytes_type{kind="Record"} 1e3\n')
            self.assertTrue(controller.native_wal_snapshot(root, 'start')['available'])
            source.write_text('wal_written_bytes_type{kind="Record"} 10\n' * 2)
            self.assertFalse(controller.native_wal_snapshot(root, 'start')['available'])
