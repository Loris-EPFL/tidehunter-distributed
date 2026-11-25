"""Conditional GCS ledger writes over the delegated ObjectStore interface."""
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'sui-checkpoint'))
import campaign_ledger as ledger_module
import gcs_ledger
import manual_campaign as manual
from gcp_api import ApiError, ObjectStore


def digest(data):
    return hashlib.sha256(data).hexdigest()


class FakeObjectStore(ObjectStore):
    """In-memory storage JSON API: generations, 412 preconditions, metadata."""

    def __init__(self):
        self.objects = {}
        self.calls = []
        self.conflict_once = set()
        self.receipt_size_delta = 0

    def head(self, key):
        if key not in self.objects:
            raise ApiError(404, 'missing')
        data, generation = self.objects[key]
        return dict(size=str(len(data)), generation=str(generation),
                    metadata={'sha256': digest(data)})

    def get(self, key, *, generation=None, max_bytes=256 * 1024 * 1024):
        if key not in self.objects:
            raise ApiError(404, 'missing')
        data, current = self.objects[key]
        if generation is not None and int(generation) != current:
            raise ApiError(404, 'generation changed')
        if len(data) > max_bytes:
            raise ValueError('object exceeds max_bytes')
        return data

    def put(self, key, data, *, content_type='application/octet-stream',
            if_generation_match=None, metadata=None):
        data = bytes(data)
        self.calls.append((key, if_generation_match))
        current = self.objects.get(key, (b'', 0))[1]
        if key in self.conflict_once:
            self.conflict_once.discard(key)
            concurrent = ledger_module.Ledger()
            concurrent.begin(stage='s', cell_id='concurrent', config_hash='h')
            self.objects[key] = (concurrent.to_bytes(), current + 1)
            raise ApiError(412, 'precondition failed')
        if if_generation_match != current:
            raise ApiError(412, 'precondition failed')
        generation = current + 1
        self.objects[key] = (data, generation)
        return dict(size=str(len(data) + self.receipt_size_delta),
                    generation=str(generation),
                    metadata={'sha256': digest(data)})


class ConditionalLedger(unittest.TestCase):
    def ledger(self, cell='mine'):
        value = ledger_module.Ledger()
        value.begin(stage='s', cell_id=cell, config_hash='h')
        return value

    def test_create_uses_generation_zero_and_reads_back(self):
        store = FakeObjectStore()
        value = self.ledger()
        gcs_ledger.save_ledger(store, value, 'runs/r/campaign-ledger.json')
        self.assertEqual(store.calls, [('runs/r/campaign-ledger.json', 0)])
        stored, generation = gcs_ledger.ObjectStoreLedger(store).get(
            'runs/r/campaign-ledger.json')
        self.assertEqual(generation, 1)
        self.assertIn(('s', 'mine'), ledger_module.Ledger.from_bytes(stored).entries)
        self.assertEqual(gcs_ledger.ObjectStoreLedger(store).head(
            'runs/r/campaign-ledger.json')['generation'], 1)

    def test_conflict_merges_and_retries_with_the_read_generation(self):
        store = FakeObjectStore()
        store.conflict_once.add('ledger.json')
        value = self.ledger()
        value.save_store(gcs_ledger.ObjectStoreLedger(store), 'ledger.json')
        self.assertEqual([call[1] for call in store.calls], [0, 1])
        stored, generation = gcs_ledger.ObjectStoreLedger(store).get('ledger.json')
        entries = ledger_module.Ledger.from_bytes(stored).entries
        self.assertEqual(set(entries), {('s', 'mine'), ('s', 'concurrent')})
        self.assertEqual(generation, 2)

    def test_missing_object_is_a_key_error(self):
        store = FakeObjectStore()
        for method in (gcs_ledger.ObjectStoreLedger(store).get,
                       gcs_ledger.ObjectStoreLedger(store).head):
            with self.subTest(method=method), self.assertRaises(KeyError):
                method('absent')

    def test_upload_receipt_mismatch_never_marks_the_ledger_current(self):
        store = FakeObjectStore()
        store.receipt_size_delta = 1
        value = self.ledger()
        with self.assertRaisesRegex(ValueError, 'size differs'):
            gcs_ledger.save_ledger(store, value, 'ledger.json')
        self.assertEqual(set(value.entries), {('s', 'mine')})

    def test_publish_ledger_skips_plain_fakes_and_uses_real_stores(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = ledger_module.Ledger(Path(tmp) / 'local.json')
            local.begin(stage='s', cell_id='local', config_hash='h')
            self.assertEqual(manual.publish_ledger(object(), local, 'r'), local.path)
            store = FakeObjectStore()
            manual.publish_ledger(store, local, 'r')
            self.assertIn('runs/r/campaign-ledger.json', store.objects)


if __name__ == '__main__':
    unittest.main()
