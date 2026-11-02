import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gcp_campaign


class FakeStore:
    def __init__(self, objects):
        self.objects = objects
        self.downloads = []

    def list(self, prefix):
        for key, data in self.objects:
            yield {'name': key, 'generation': '7', 'size': str(len(data)),
                   'metadata': {'sha256': hashlib.sha256(data).hexdigest()}}

    def download(self, key, path, *, generation, max_bytes):
        self.downloads.append((key, generation, max_bytes))
        data = dict(self.objects)[key]
        if len(data) > max_bytes:
            raise ValueError('download exceeds bound')
        Path(path).write_bytes(data)


class ResultCollectionTests(unittest.TestCase):
    prefix = 'run/results/'

    def test_full_matrix_receipts_are_collected_without_truncation(self):
        objects = [(self.prefix + f'cells/{case}/storage/{host}/receipt.json',
                    f'raw observation {case} {host}'.encode())
                   for case in range(273) for host in range(5)]
        store = FakeStore(objects)
        with tempfile.TemporaryDirectory() as tmp:
            inventory = gcp_campaign.collect_results(store, 'run', tmp)
            self.assertEqual(len(inventory), len(objects))
            self.assertEqual(json.loads(Path(tmp, 'result-inventory.json').read_bytes()), inventory)
            for key, data in objects:
                self.assertEqual(Path(tmp, 'results', key[len(self.prefix):]).read_bytes(), data)
            self.assertTrue(all(generation == '7' for _, generation, _ in store.downloads))

    def test_count_and_byte_limits_preserve_completed_evidence(self):
        objects = [(self.prefix + 'first', b'abcd'), (self.prefix + 'second', b'efghi')]
        for limits, message in [({'MAX_RESULT_OBJECTS': 1}, 'count'),
                                ({'MAX_RESULT_OBJECT_BYTES': 4}, 'object size'),
                                ({'MAX_RESULT_TOTAL_BYTES': 8}, 'aggregate size')]:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as tmp, \
                    mock.patch.multiple(gcp_campaign, **limits):
                store = FakeStore(objects)
                with self.assertRaisesRegex(ValueError, message):
                    gcp_campaign.collect_results(store, 'run', tmp)
                self.assertEqual(Path(tmp, 'results/first').read_bytes(), b'abcd')
                self.assertFalse(Path(tmp, 'results/second').exists())
                inventory = json.loads(Path(tmp, 'result-inventory.json').read_bytes())
                self.assertEqual([item['key'] for item in inventory], [objects[0][0]])
                self.assertEqual(len(store.downloads), 1)

    def test_exact_limits_are_inclusive_and_download_uses_remaining_budget(self):
        store = FakeStore([(self.prefix + 'first', b'abcd'), (self.prefix + 'second', b'efghi')])
        with tempfile.TemporaryDirectory() as tmp, mock.patch.multiple(
                gcp_campaign, MAX_RESULT_OBJECTS=2, MAX_RESULT_OBJECT_BYTES=5,
                MAX_RESULT_TOTAL_BYTES=9):
            inventory = gcp_campaign.collect_results(store, 'run', tmp)
            self.assertEqual(len(inventory), 2)
            self.assertEqual([entry[2] for entry in store.downloads], [5, 5])

    def test_listing_failure_still_writes_partial_inventory(self):
        class BrokenStore(FakeStore):
            def list(self, prefix):
                yield from super().list(prefix)
                raise RuntimeError('listing failed')
        store = BrokenStore([(self.prefix + 'first', b'complete')])
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'listing failed'):
                gcp_campaign.collect_results(store, 'run', tmp)
            self.assertEqual(Path(tmp, 'results/first').read_bytes(), b'complete')
            self.assertEqual(len(json.loads(Path(tmp, 'result-inventory.json').read_bytes())), 1)


if __name__ == '__main__':
    unittest.main()
