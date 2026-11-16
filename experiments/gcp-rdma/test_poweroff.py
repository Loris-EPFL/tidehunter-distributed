import hashlib
import json
import unittest
from unittest import mock

import gcp_api
import gcp_poweroff as poweroff


class Store:
    def __init__(self):
        self.items = {}
        self.generation = 0

    def put(self, key, data, **kwargs):
        if kwargs.get('if_generation_match') == 0 and key in self.items:
            raise gcp_api.ApiError(412, 'exists')
        self.generation += 1
        meta = {'name': key, 'generation': str(self.generation), 'size': str(len(data)),
                'metadata': {'sha256': hashlib.sha256(data).hexdigest()}}
        self.items[key] = (meta, data)
        return meta

    def head(self, key):
        if key not in self.items:
            raise gcp_api.ApiError(404, 'missing')
        return self.items[key][0]

    def get(self, key, *, generation, max_bytes):
        meta, data = self.items[key]
        if meta['generation'] != str(generation) or len(data) > max_bytes:
            raise ValueError('generation or length mismatch')
        return data


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


class PoweroffTests(unittest.TestCase):
    def setUp(self):
        self.config = {'run_id': 'bench-test', 'source_sha256': 'a' * 64,
                       'fleet': [{'id': '1'}, {'id': '2'}],
                       'spec': {'suite': 'kv'}, 'package': {'binary_blake2b512': 'b' * 128}}
        self.host = {'id': '1', 'index': 0}
        self.owner = {'id': '2', 'index': 1}
        self.base = 'runs/bench-test/poweroff/'
        self.store = Store()
        self.result = {'exit_code': 0, 'operation_id': 'campaign'}
        self.response = 'runs/bench-test/mailbox/1/responses/campaign.json'
        self.store.put(self.response, encode(self.result))
        self.store.put(self.base + 'kv-started.json', encode({'run_id': 'bench-test',
                       'source_sha256': 'a' * 64, 'suite': 'kv'}))
        prefix = 'bench-test/results/'
        archive = b'archive'
        self.store.put(prefix + 'controller.tar.gz', archive)
        inventory = b'[]'
        meta = self.store.put(prefix + 'controller-inventory.json', inventory)
        receipt = {'schema_version': 2, 'status': 'completed', 'source_sha256': 'a' * 64,
                   'binary_blake2b512': 'b' * 128, 'monitoring_complete': True, 'key': prefix + 'controller.tar.gz',
                   'bytes': len(archive), 'sha256': hashlib.sha256(archive).hexdigest(),
                   'inventory': {'key': prefix + 'controller-inventory.json', 'bytes': len(inventory),
                                 'sha256': hashlib.sha256(inventory).hexdigest(), 'generation': meta['generation']}}
        self.store.put(prefix + 'receipt.json', encode(receipt))

    def terminal(self):
        with mock.patch.object(poweroff.time, 'time', return_value=1000):
            poweroff.publish_terminal(self.config, self.host, self.store, self.result, self.response)

    def ack(self, **extra):
        self.store.put(self.base + 'operator.json', encode({'schema_version': 1, 'run_id': 'bench-test',
                       'source_sha256': 'a' * 64, 'action': 'operator_has_taken_over', **extra}))

    def test_absent_terminal_and_grace(self):
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=2000), 'no_terminal')
        self.terminal()
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=1599), 'grace')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=1600), 'poweroff_no_acknowledgement')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=1600, active=True), 'active_requests')

    def test_ack_hard_cap_and_extension(self):
        self.terminal()
        self.ack()
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=4599), 'operator_takeover')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=4600), 'poweroff_hard_cap')
        self.ack(extend_until_utc='1970-01-01T02:00:00Z')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=7000), 'operator_takeover')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=7200), 'poweroff_hard_cap')
        self.ack(extend_until_utc='invalid')
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=9000), 'unknown')

    def test_failed_and_ambiguous_result(self):
        self.result['exit_code'] = 1
        self.store.put(self.response, encode(self.result))
        self.terminal()
        terminal = json.loads(self.store.get(self.base + 'terminal.json', generation=self.store.head(self.base + 'terminal.json')['generation'], max_bytes=100000))
        self.assertEqual(terminal['status'], 'failed')
        self.assertEqual(terminal['owners']['2']['status'], 'exported')
        for code in (124, -9):
            self.store.items.pop(self.base + 'terminal.json')
            self.result['exit_code'] = code
            self.terminal()
            self.assertNotIn(self.base + 'terminal.json', self.store.items)
            self.result['exit_code'] = 1
            self.terminal()

    def test_missing_or_changed_evidence_defers(self):
        self.store.items.pop('bench-test/results/controller.tar.gz')
        with self.assertRaises(gcp_api.ApiError):
            self.terminal()
        self.store.put('bench-test/results/controller.tar.gz', b'archive')
        self.terminal()
        self.store.put('bench-test/results/controller.tar.gz', b'changed')
        with self.assertRaises(ValueError):
            poweroff.evaluate(self.config, self.owner, self.store, now=9000)

    def test_failed_owner_monitoring_export_prevents_poweroff(self):
        key = 'bench-test/results/receipt.json'
        receipt = json.loads(self.store.items[key][1])
        receipt['status'] = 'incomplete'
        receipt['monitoring_complete'] = False
        self.store.put(key, encode(receipt))
        self.result['exit_code'] = 1
        self.store.put(self.response, encode(self.result))
        with self.assertRaises(ValueError):
            self.terminal()
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=9000), 'no_terminal')

    def test_failed_before_suite_and_missing_success_marker(self):
        self.store.items.pop(self.base + 'kv-started.json')
        with self.assertRaises(ValueError):
            self.terminal()
        self.result['exit_code'] = 1
        self.store.put(self.response, encode(self.result))
        self.terminal()
        self.assertEqual(poweroff.evaluate(self.config, self.owner, self.store, now=1600), 'poweroff_no_acknowledgement')


    def test_unknown_storage_error_fails_closed(self):
        self.terminal()
        original = self.store.head
        def head(key):
            if key == self.base + 'operator.json':
                raise gcp_api.ApiError(403, 'denied')
            return original(key)
        with mock.patch.object(self.store, 'head', side_effect=head):
            with self.assertRaises(gcp_api.ApiError):
                poweroff.evaluate(self.config, self.owner, self.store, now=9000)

    def test_bad_ack_and_invalid_times(self):
        self.terminal()
        self.store.put(self.base + 'operator.json', encode({'schema_version': 1, 'run_id': 'wrong',
                       'source_sha256': 'a' * 64, 'action': 'operator_has_taken_over'}))
        with self.assertRaises(ValueError):
            poweroff.evaluate(self.config, self.owner, self.store, now=9000)
        self.assertEqual(poweroff.decision(terminal_epoch=1001, now=1000, grace=600, hard_cap=3600, acknowledgement=None), 'unknown')
        self.assertEqual(poweroff.decision(terminal_epoch=1000, now=9000, grace=3600, hard_cap=600, acknowledgement=None), 'unknown')
        self.assertEqual(poweroff.decision(terminal_epoch=1000, now=9000, grace=600, hard_cap=3600, acknowledgement={}), 'unknown')


if __name__ == '__main__':
    unittest.main()
