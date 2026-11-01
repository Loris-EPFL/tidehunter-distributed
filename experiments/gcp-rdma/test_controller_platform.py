"""Provider-neutral object identity and physical-fleet admission boundaries."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_controller as controller


class StoreError(RuntimeError):
    def __init__(self, status):
        self.status = status


class VersionedStore:
    def __init__(self):
        self.values = {}
        self.generation = 0
        self.lose_next_ack = False

    def head(self, key):
        if key not in self.values:
            raise StoreError(404)
        raw, generation, metadata = self.values[key]
        return {'size': str(len(raw)), 'generation': str(generation), 'metadata': metadata}

    def put(self, key, raw, *, content_type, metadata, if_generation_match):
        previous = int(self.head(key)['generation']) if key in self.values else 0
        if int(if_generation_match) != previous:
            raise StoreError(412)
        self.generation += 1
        self.values[key] = (raw, self.generation, metadata)
        if self.lose_next_ack:
            self.lose_next_ack = False
            raise TimeoutError('accepted write; reply lost')
        return self.head(key)

    def get(self, key, *, max_bytes, generation=None):
        raw, actual, _ = self.values[key]
        if generation is not None and str(actual) != str(generation):
            raise StoreError(412)
        if len(raw) > max_bytes:
            raise ValueError('oversized object')
        return raw

    def download(self, key, path, *, generation):
        Path(path).write_bytes(self.get(key, generation=generation, max_bytes=1024))


class ControllerObjects(unittest.TestCase):
    def setUp(self):
        self.store = VersionedStore()
        self.clients = {'objects': self.store}
        self.config = {'prefix': 'th-' + 'a' * 16, 'bucket': 'example-private-bucket'}
        self.key = self.config['prefix'] + '/results/operation.json'

    def test_lost_ack_and_duplicate_do_not_overwrite_immutable_result(self):
        self.store.lose_next_ack = True
        first = controller.persist_json(self.clients, self.config, self.key, {'value': 7})
        duplicate = controller.persist_json(self.clients, self.config, self.key, {'value': 7})
        self.assertEqual(first['generation'], duplicate['generation'])
        with self.assertRaises(StoreError):
            controller.persist_json(self.clients, self.config, self.key, {'value': 9})
        self.assertEqual(controller.read_json(self.clients, self.config, self.key), {'value': 7})

    def test_explicit_progress_replacement_preserves_generation_bound_bytes(self):
        controller.persist_json(self.clients, self.config, self.key, {'case': 1}, replace=True)
        result = controller.persist_json(self.clients, self.config, self.key, {'case': 2}, replace=True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'download.json'
            controller.download_object(self.clients, self.config, self.key, path)
            self.assertEqual(json.loads(path.read_text()), {'case': 2})
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), result['sha256'])
        for foreign in ('other/results/x', self.config['prefix'] + '/../foreign'):
            with self.assertRaises(controller.SafetyError):
                controller.persist_json(self.clients, self.config, foreign, {'case': 3})

    def test_generation_bound_download_rejects_corrupted_bytes(self):
        controller.persist_json(self.clients, self.config, self.key, {'value': 7})
        raw, generation, metadata = self.store.values[self.key]
        self.store.values[self.key] = (raw.replace(b'7', b'8'), generation, metadata)
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(controller.InfrastructureError):
            controller.download_object(self.clients, self.config, self.key, Path(directory) / 'value')


class PhysicalFleet(unittest.TestCase):
    def test_google_devices_require_six_independent_boots_and_serials(self):
        proofs = [{'qualification_only': False, 'independent_nvme_admitted': True,
                   'host_identity': {'boot_id': 'boot-' + str(index)},
                   'block_devices': {'blockdevices': [{'model': 'Google Local SSD', 'serial': 'ssd-' + str(index)}]}}
                  for index in range(6)]
        self.assertTrue(controller.validate_physical_proofs(proofs, provider='gcp'))
        for field in ('boot', 'serial'):
            changed = copy.deepcopy(proofs)
            if field == 'boot':
                changed[5]['host_identity'] = changed[0]['host_identity']
            else:
                changed[5]['block_devices'] = changed[0]['block_devices']
            with self.assertRaises(controller.SafetyError):
                controller.validate_physical_proofs(changed, provider='gcp')
        with self.assertRaises(controller.SafetyError):
            controller.validate_physical_proofs(proofs)


if __name__ == '__main__':
    unittest.main()
