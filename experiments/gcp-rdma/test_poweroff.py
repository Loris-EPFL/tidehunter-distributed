import datetime
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
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

    def list(self, prefix):
        return [meta for key, (meta, _data) in sorted(self.items.items())
                if key.startswith(prefix)]


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


class IdleStopTests(unittest.TestCase):
    def setUp(self):
        self.config = {'run_id': 'bench-test', 'source_sha256': 'a' * 64,
                       'fleet': [{'id': '1'}, {'id': '2'}],
                       'idle_stop_seconds': 60}
        self.coordinator = {'id': '1', 'index': 0}
        self.owner = {'id': '2', 'index': 1}
        self.store = Store()

    def test_idle_decision_uses_the_injected_clock(self):
        self.assertEqual(poweroff.idle_decision(now=1000, last_activity=950,
                                                 idle_seconds=60), 'idle_wait')
        self.assertEqual(poweroff.idle_decision(now=1010, last_activity=950,
                                                 idle_seconds=60), 'idle_poweroff')
        self.assertEqual(poweroff.idle_decision(now=1010, last_activity=950,
                                                 idle_seconds=60, active=True),
                         'active_requests')
        self.assertEqual(poweroff.idle_decision(now=1010, last_activity=950,
                                                 idle_seconds=60, driver=True),
                         'driver_active')
        for change in ({'now': 'x'}, {'last_activity': 1001}, {'idle_seconds': 0}):
            values = dict(now=1000, last_activity=950, idle_seconds=60)
            values.update(change)
            self.assertEqual(poweroff.idle_decision(**values), 'unknown')

    def test_coordinator_publishes_while_it_is_not_idle(self):
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.coordinator, self.store, active=True,
            last_activity=950, now=1000), 'active_requests')
        beat = poweroff.heartbeat_epoch(self.config, self.store)
        self.assertEqual(beat, 1000)
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.coordinator, self.store, last_activity=950,
            now=1009), 'idle_wait')
        self.assertEqual(poweroff.heartbeat_epoch(self.config, self.store), 1009)
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.coordinator, self.store, last_activity=950,
            now=1010), 'idle_poweroff')
        self.assertEqual(poweroff.heartbeat_epoch(self.config, self.store), 1009)

    def test_owner_follows_a_fresh_or_stale_coordinator_heartbeat(self):
        poweroff.publish_heartbeat(self.config, self.coordinator, self.store, 950)
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, last_activity=900, now=1000),
            'idle_wait')
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, active=True,
            last_activity=900, now=1100), 'active_requests')
        # Owners follow the heartbeat. A detached owner process group cannot
        # keep them up after the coordinator is gone.
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, driver=True,
            last_activity=900, now=1100), 'idle_poweroff')
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, last_activity=900, now=1100),
            'idle_poweroff')

    def test_owner_without_a_heartbeat_starts_from_its_own_activity(self):
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, last_activity=950, now=1000),
            'idle_wait')
        self.assertEqual(poweroff.evaluate_idle(
            self.config, self.owner, self.store, last_activity=900, now=1000),
            'idle_poweroff')

    def test_an_invalid_heartbeat_is_not_trusted(self):
        poweroff.publish_heartbeat(self.config, self.coordinator, self.store, 950)
        key = poweroff.heartbeat_key(self.config)
        self.store.put(key, encode({'schema_version': 1, 'run_id': 'other',
                                    'source_sha256': 'a' * 64, 'coordinator_id': '1',
                                    'observed_epoch': 950}))
        with self.assertRaises(ValueError):
            poweroff.evaluate_idle(self.config, self.owner, self.store,
                                   last_activity=900, now=1000)

    def test_the_idle_window_is_bounded(self):
        for value in (59, 3601, '60', None):
            config = dict(self.config, idle_stop_seconds=value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                poweroff.idle_window(config)
        self.assertEqual(poweroff.idle_window({'idle_stop_seconds': 60}), 60)
        self.assertEqual(poweroff.idle_window({}), poweroff.gcp_runtime.IDLE_STOP_SECONDS)

    def test_only_a_coordinator_driver_script_counts(self):
        import os
        import tempfile
        config = {'run_id': 'run-x', 'package_root': '/mnt/tidehunter/run-x/package',
                  'guest_roots': ['/mnt/tidehunter/sui-run-x']}
        with tempfile.TemporaryDirectory() as root:
            owner = os.path.join(root, '10')
            other = os.path.join(root, '11')
            driver = os.path.join(root, '12')
            workload = os.path.join(root, '13')
            for path in (owner, other, driver, workload):
                os.mkdir(path)
            with open(os.path.join(owner, 'cmdline'), 'wb') as stream:
                stream.write(b'/mnt/tidehunter/sui-run-x/storage-node\0--db-root\0/x\0')
            with open(os.path.join(other, 'cmdline'), 'wb') as stream:
                stream.write(b'/usr/bin/python3\0/rehearsal/work/worker.py\0')
            with open(os.path.join(workload, 'cmdline'), 'wb') as stream:
                stream.write(b'/mnt/tidehunter/run-x/package/runtime/bin/kv-research\0')
            self.assertFalse(poweroff.driver_process_active(config, proc_root=root))
            with open(os.path.join(driver, 'cmdline'), 'wb') as stream:
                stream.write(b'/usr/bin/python3\0/mnt/tidehunter/run-x/package/source/run.py\0')
            self.assertTrue(poweroff.driver_process_active(config, proc_root=root))
            os.unlink(os.path.join(driver, 'cmdline'))
            self.assertFalse(poweroff.driver_process_active(config, proc_root=root))
        self.assertTrue(poweroff.driver_process_active({}))

    def test_the_guard_poweroff_action_is_one_acpi_stop(self):
        with mock.patch('subprocess.run') as run:
            poweroff.poweroff('poweroff_idle')
        run.assert_called_once_with(['/usr/bin/systemctl', 'poweroff'], check=True, timeout=30)


def _stamp(store, key, epoch):
    meta, _data = store.items[key]
    meta['timeCreated'] = datetime.datetime.fromtimestamp(
        epoch, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


class IdleGuardTests(unittest.TestCase):
    def setUp(self):
        self.config = {'run_id': 'bench-test', 'source_sha256': 'a' * 64,
                       'fleet': [{'id': '1', 'name': 'c', 'zone': 'z'},
                                 {'id': '2', 'name': 'o', 'zone': 'z'}],
                       'bucket': 'bucket', 'project_id': 'p', 'project_number': '1',
                       'deadline_utc': '2030-01-01T00:00:00Z',
                       'idle_stop_seconds': 60, 'package_root': '/package'}
        self.host = {'id': '1', 'name': 'c', 'zone': 'z'}
        self.store = Store()

    def state(self, root, pending=(), modified=0):
        directory = Path(root) / self.config['run_id']
        directory.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(directory / 'mailbox-state.sqlite3')
        database.execute('CREATE TABLE requests (key TEXT PRIMARY KEY, status TEXT NOT NULL)')
        for key in pending:
            database.execute('INSERT INTO requests VALUES (?, ?)', (key, 'pending'))
        database.commit()
        database.close()
        os.utime(directory / 'mailbox-state.sqlite3', (modified, modified))
        return root

    def guard(self, state_root):
        action, clock = [], [0]
        api = mock.Mock()
        api.objects.return_value = self.store
        result = poweroff.serve_idle(
            self.config, host=self.host, api=api, state_root=state_root,
            clock=lambda: clock[0],
            sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
            poweroff_action=lambda reason: action.append((clock[0], reason)))
        return result, action

    def test_the_guard_stops_an_idle_coordinator(self):
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(poweroff.gcp_runtime, 'validate_runtime'), \
                mock.patch.object(poweroff.gcp_runtime, '_deadline', return_value=10000):
            result, action = self.guard(root)
        self.assertEqual(result, 'poweroff_idle')
        self.assertEqual(action, [(60, 'poweroff_idle')])
        beat = poweroff.heartbeat_epoch(self.config, self.store)
        self.assertEqual(beat, 30)

    def test_a_long_request_holds_the_guard_until_its_timeout(self):
        key = 'runs/bench-test/mailbox/1/requests/op.json'
        self.store.put(key, encode({'timeout_seconds': 120}))
        _stamp(self.store, key, 0)
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(poweroff.gcp_runtime, 'validate_runtime'), \
                mock.patch.object(poweroff.gcp_runtime, '_deadline', return_value=10000):
            self.state(root, [key], modified=0)
            result, action = self.guard(root)
        self.assertEqual(result, 'poweroff_idle')
        self.assertGreaterEqual(action[0][0], 120)

    def test_a_completed_row_does_not_hold_the_guard(self):
        key = 'runs/bench-test/mailbox/1/requests/op.json'
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(poweroff.gcp_runtime, 'validate_runtime'), \
                mock.patch.object(poweroff.gcp_runtime, '_deadline', return_value=10000):
            self.state(root, [], modified=0)
            result, action = self.guard(root)
        self.assertEqual(action, [(60, 'poweroff_idle')])

    def test_a_hung_request_expires_at_its_timeout(self):
        key = 'runs/bench-test/mailbox/1/requests/op.json'
        self.store.put(key, encode({'timeout_seconds': 120}))
        _stamp(self.store, key, 900)
        with tempfile.TemporaryDirectory() as root:
            self.state(root, [key], modified=900)
            last, active_until = poweroff.mailbox_activity(
                self.config, self.host, self.store, now=1100, state_root=root)
        self.assertEqual(last, 900)
        self.assertEqual(active_until, 1020)

    def test_a_pending_row_without_an_object_gets_a_short_bound(self):
        key = 'runs/bench-test/mailbox/1/requests/op.json'
        with tempfile.TemporaryDirectory() as root:
            self.state(root, [key], modified=900)
            last, active_until = poweroff.mailbox_activity(
                self.config, self.host, self.store, now=1000, state_root=root)
        self.assertEqual(last, 900)
        self.assertEqual(active_until, 900 + poweroff.POLL_SECONDS)


if __name__ == '__main__':
    unittest.main()
