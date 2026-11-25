"""Coordinator disk inventory tests; no workload, filesystem root or cargo run."""
import json
import os
import tempfile
import unittest
from pathlib import Path

import disk_collector as disk


class DiskInventory(unittest.TestCase):
    def fixture(self, root):
        database = root / 'live/db'
        database.mkdir(parents=True)
        (database / 'index_0000000000000000').write_bytes(b'x' * 4096)
        (database / 'index_0000000000000001').write_bytes(b'x' * 8192)
        (database / 'wal_0000000000000000').write_bytes(b'x' * 1024)
        (database / 'shape.json').write_text('{}')
        return database

    def test_classes_and_totals_cover_only_wal_families(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            collection = disk.sample(root, ['live/db'], fragment_bytes=4096, now=7)
            self.assertEqual(collection['sampled_unix_ns'], 7)
            row = collection['databases']['live/db']
            self.assertEqual(row['index_wal_files'], 2)
            self.assertEqual(row['classes']['index_wal']['files'], 2)
            self.assertEqual(row['classes']['index_wal']['apparent_bytes'], 12288)
            self.assertEqual(row['classes']['replay_wal']['files'], 1)
            self.assertEqual(row['classes']['other']['files'], 1)
            self.assertEqual(row['total']['files'], 4)
            self.assertEqual(row['total']['apparent_bytes'], 4096 + 8192 + 1024 + 2)
            self.assertEqual(row['classes']['index_wal']['apparent_fragment_slots'], 3)
            self.assertIn('free_bytes', collection['free'])

    def test_fragment_slots_are_derived_bounds_not_engine_records(self):
        self.assertEqual(disk._file_fragment_slots(3 * 4096, 2 * 4096, 4096), (3, 2))
        self.assertEqual(disk._file_fragment_slots(1, 0, 4096), (1, 0))
        self.assertEqual(disk._file_fragment_slots(4096, 1 << 30, 4096), (1, 1))
        self.assertEqual(disk._file_fragment_slots(4096, 0, None), (0, 0))

    def test_unsafe_or_missing_namespace_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            for namespace in ('/absolute', '../escape', 'live/absent'):
                with self.subTest(namespace=namespace), self.assertRaises(ValueError):
                    disk.sample(root, [namespace], fragment_bytes=4096)

    def test_symlinked_wal_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = self.fixture(root)
            (database / 'index_0000000000000002').symlink_to(database / 'shape.json')
            with self.assertRaisesRegex(ValueError, 'symlink'):
                disk.sample(root, ['live/db'], fragment_bytes=4096)

    def test_budget_compares_the_larger_observation_with_free_space(self):
        collection = dict(databases={'a': dict(total=dict(apparent_bytes=100, allocated_bytes=150))},
                          free=dict(free_bytes=1000))
        result = disk.budget(collection, limit=0.1)
        self.assertEqual(result['projected_peak_bytes'], 150)
        self.assertFalse(result['within_limit'])
        collection['databases']['a']['total']['allocated_bytes'] = 100
        self.assertTrue(disk.budget(collection, limit=0.1)['within_limit'])
        collection['free']['free_bytes'] = 0
        with self.assertRaisesRegex(ValueError, 'no free space'):
            disk.budget(collection)

    def test_cli_json_contains_budget_and_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            output = root / 'disk.json'
            import subprocess, sys
            subprocess.run([sys.executable, str(Path(__file__).with_name('disk_collector.py')),
                            '--root', str(root), '--namespace', 'live/db',
                            '--fragment-mib', '1', '--output', str(output)],
                           check=True, capture_output=True)
            data = json.loads(output.read_text())
            self.assertEqual(data['fragment_bytes'], disk.MIB)
            self.assertEqual(data['databases']['live/db']['index_wal_files'], 2)
            self.assertIn('within_limit', data['budget'])


if __name__ == '__main__':
    unittest.main()
