"""Offline evidence gates and opt-in local real-systemd lifecycle regressions."""
from concurrent.futures import ThreadPoolExecutor
import copy
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import Mock, patch

import analyze_cloud as analysis
import checkpoint_async as profile
import guest


class CheckpointEvidenceTests(unittest.TestCase):
    def test_unplanned_case_is_rejected_before_preparation(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'campaign': 'checkpoint-async'}
        campaign.qualification_passed = True
        spec = profile.main_cases()[0] | {'reads': 512}
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            campaign.run_case(spec['build'], (1024, 32, 32), 0, storage=True, progress_case=spec)

    def test_recovery_scope_cannot_admit_a_performance_case(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'campaign': 'checkpoint-async', 'checkpoint_scope': 'recovery'}
        campaign.qualification_passed = True
        spec = profile.main_cases()[0]
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            campaign.run_case(spec['build'], (1024, 32, 32), 0, progress_case=spec)

    def test_recovery_followup_cannot_bypass_hardware_qualification(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'campaign': 'checkpoint-async', 'checkpoint_scope': 'recovery'}
        campaign.qualification_passed = False
        spec = profile.recovery_cases()[0]
        with self.assertRaisesRegex(ValueError, 'hardware qualification'):
            campaign.run_case(spec['build'], (8192, 32, 32), 0, progress_case=spec)

    def test_comparison_keeps_repetition_pairs_and_transport_controls_separate(self):
        runs = {}
        for spec in profile.main_cases():
            if not spec['family'].startswith('core'):
                continue
            name = spec['case_id'] + '-native-sync'
            runs[name] = (profile.expected_cases()[name], {('summary', 'point_get', 'p50_ns'): 10 + spec['rep'] + (2 if spec['build'] == 'current' else 0)})
        rows = analysis.compare_checkpoint(runs)
        current = [r for r in rows if r['build'] == 'current']
        self.assertEqual(len(current), 4)
        for row in current:
            self.assertEqual(row['new_repetitions'], 5)
            self.assertEqual(row['paired_deltas'], dict.fromkeys(range(5), 2))
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            bad = object.__new__(guest.Campaign)
            bad.cfg = {'campaign': 'checkpoint-async'}
            bad.run_case('current', (8192, 32, 32), 6, progress_case=profile.main_cases()[0] | {'rep': 6})

    def recovery(self, tail=128):
        spec = next(s for s in profile.recovery_cases() if s['build'] == 'current' and s['tail_batches'] == tail)
        row = dict(schema=1, action='reopen', value_bytes=8192, batch_size=32,
                   checkpoint_batches=64, tail_batches=tail, key_count=2048, verified_keys=2048,
                   abrupt_exit=False, exact_retry_verified=True, open_ns=100, checkpoint_create_ns=0,
                   tail_append_ns=0, verification_ns=10,
                   recovery_stats=dict(checkpoint_loaded=True, tail_batches=tail, tail_bytes=tail * 100,
                       checkpoint_load_ns=1, proof_validation_ns=1, tail_replay_ns=1,
                       checkpoint_commit_ns=1, connection_setup_ns=1, fencing_ns=1))
        return spec, row

    def test_recovery_requires_full_contents_retry_and_checkpoint_adoption(self):
        spec, row = self.recovery()
        profile.validate_recovery_json(row, spec, 'reopen')
        for field, value in [('verified_keys', 2047), ('exact_retry_verified', False),
                             ('abrupt_exit', True), ('open_ns', float('nan'))]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                profile.validate_recovery_json(row | {field: value}, spec, 'reopen')
        for field, value in [('checkpoint_loaded', False), ('tail_batches', 127), ('tail_bytes', 0)]:
            bad = copy.deepcopy(row)
            bad['recovery_stats'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                profile.validate_recovery_json(bad, spec, 'reopen')

    def test_restart_proof_rejects_stale_pid_readiness_and_extended_ttl(self):
        spec = next(s for s in profile.recovery_cases() if s['restart'] == 'full')
        cfg = {'deadline_utc': 'original-expiry'}
        row = dict(case_id=spec['case_id'], partition=0, deadline_utc='original-expiry',
                   data_preserved=True, old_pid=100, new_pid=101, ready='172.31.0.1:7700',
                   ready_removed_before_start=True, node_restart_ns=100)
        profile.validate_restart(row, spec, cfg, 0, 101)
        for key, value in [('old_pid', 101), ('ready', ''), ('deadline_utc', 'extended'),
                           ('ready_removed_before_start', False), ('data_preserved', False)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                profile.validate_restart(row | {key: value}, spec, cfg, 0, 101)

    def node_fixture(self, root):
        cfg = dict(campaign='checkpoint-async', account='123456789012', region='us-east-1',
                   run_id='th-0123456789abcdef', deadline_utc='original-expiry',
                   nodes=[dict(instance_id='i-node', partition=0)])
        (root / 'bootstrap.json').write_text(json.dumps({'identity': {'instanceId': 'i-node', 'accountId': cfg['account']}}))
        (root / 'node-control').mkdir()
        payload = dict(config=cfg, case_id='recovery-test', action='restart', build='current',
                       binary_sha256='a' * 64, node_archives={'0': {'sha256': 'b' * 64}})
        return cfg, payload

    def test_terminal_stop_cannot_be_undone_by_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, payload = self.node_fixture(root)
            marker = root / 'node-control/recovery-test.stop-requested'
            marker.touch()
            with patch.object(guest, 'validate_cfg', return_value=(root, [])), patch.object(guest.boto3, 'Session'), patch.object(guest.subprocess, 'run') as execute:
                with self.assertRaisesRegex(ValueError, 'terminal stop'):
                    guest.node_action(payload)
                execute.assert_not_called()
                self.assertTrue(marker.exists())

    def test_restart_cannot_reconfigure_or_extend_original_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, payload = self.node_fixture(root)
            state = root / 'node-control/recovery-test.json'
            original = dict(binary_sha256='a' * 64, archive_sha256='b' * 64, deadline_utc='original-expiry')
            state.write_text(json.dumps(original))
            payload['config']['deadline_utc'] = 'extended-expiry'
            with patch.object(guest, 'validate_cfg', return_value=(root, [])), patch.object(guest.boto3, 'Session'), patch.object(guest.subprocess, 'run') as execute:
                with self.assertRaisesRegex(ValueError, 'original source/config/deadline'):
                    guest.node_action(payload)
                execute.assert_not_called()
                self.assertEqual(json.loads(state.read_text()), original)

    def path_rows(self, spec):
        phases = ['point_get', 'point_get_many', 'point_get_window', 'point_get_hot',
                  'point_get_many_hot', 'point_get_missing', 'exists', 'exists_missing', 'post_flush_get']
        rows = []
        for phase in phases:
            for boundary in ('before', 'after'):
                for role, partition in [('frontend', '')] + [('storage', str(p)) for p in range(5)]:
                    after = boundary == 'after'
                    budget = spec['cache'] if role == 'frontend' else spec['shard_cache']
                    hot = phase.endswith('_hot')
                    rows.append(dict(backend='experimental_tcp_authority', value_bytes=8192,
                        batch_size=32, window=32, repetition=0, phase=phase, boundary=boundary,
                        role=role, partition=partition, cache_budget_bytes=budget,
                        cache_retained_bytes=8192 if budget else 0, cache_metadata_bytes=64 if budget else 0,
                        cache_entries=1 if budget else 0, cache_hits=int(after and hot and bool(budget)),
                        cache_misses=int(after and not hot), cache_evictions=int(after and not hot and bool(budget)),
                        wal_reads=int(after and not hot and role == 'storage'),
                        wal_read_bytes=8192 * int(after and not hot and role == 'storage'),
                        inflight_requests=0, inflight_bytes=0, inflight_reads=0,
                        peak_inflight_requests=1, peak_inflight_bytes=16384,
                        peak_inflight_reads=1, peak_device_reads=1))
        return rows

    def test_pressure_rejects_cache_fitting_missing_wal_and_unbounded_budgets(self):
        spec = next(s for s in profile.main_cases() if s['cache'] and s['shard_cache'] and s['transport'] == 'tcp')
        rows = self.path_rows(spec)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'read_path_evidence.csv'
            def consume(values):
                with path.open('w') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(values[0]))
                    writer.writeheader()
                    writer.writerows(values)
                return profile.read_path_metrics(path, spec, 'experimental_tcp_authority')
            self.assertTrue(consume(rows))
            for field, value in [('cache_evictions', 0), ('cache_misses', 0), ('wal_reads', 0),
                                 ('cache_retained_bytes', 9999999), ('cache_budget_bytes', 0),
                                 ('peak_inflight_requests', 33), ('peak_inflight_bytes', 64 * 1024 * 1024 + 1),
                                 ('peak_device_reads', 129)]:
                bad = copy.deepcopy(rows)
                for selected in bad:
                    if selected['phase'] == 'point_get' and selected['boundary'] == 'after' and selected['role'] == 'storage':
                        selected[field] = value
                with self.subTest(field=field), self.assertRaises(ValueError):
                    consume(bad)
            with self.assertRaises(ValueError):
                consume(rows[:-1])
            with self.assertRaises(ValueError):
                consume(rows + [rows[0]])
            unbalanced = copy.deepcopy(rows)
            for row in unbalanced:
                if row['role'] == 'storage' and row['partition'] != '0':
                    for field in ('cache_misses', 'cache_evictions', 'wal_reads', 'wal_read_bytes'):
                        row[field] = 0
            self.assertEqual(
                consume(unbalanced)['read_path', 'point_get', 'storage-0-wal_reads'], 1)


@unittest.skipUnless(os.environ.get('TH_TEST_SYSTEMD') == '1',
                     'set TH_TEST_SYSTEMD=1 for the local real-systemd lifecycle exercise')
class SystemdRestartTests(unittest.TestCase):
    def setUp(self):
        if os.geteuid() != 0:
            # Use the real user manager on unprivileged laptops. Only select the
            # bus; process signals, collection and deadlines remain real.
            run = subprocess.run
            def user_manager(command, *args, **kwargs):
                if command[0] in ('systemctl', 'systemd-run'):
                    command = [command[0], '--user', *command[1:]]
                return run(command, *args, **kwargs)
            manager = patch('subprocess.run', side_effect=user_manager)
            manager.start()
            self.addCleanup(manager.stop)
        self.assertTrue(Path('/run/systemd/system').is_dir(), 'systemd must be the local system manager')
        temporary = tempfile.TemporaryDirectory(prefix='th-restart-test-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.unit = 'th-restart-test-' + uuid.uuid4().hex
        self.pid = 0
        self.addCleanup(self.stop_fixture)
        self.script = self.root / 'process.py'
        self.script.write_text(
            'import os, signal, sys, time\n'
            'from pathlib import Path\n'
            'root = Path(__file__).parent\n'
            'if sys.argv[1] == "stop":\n'
            '    (root / "deactivating").touch()\n'
            '    while not (root / "release").exists():\n'
            '        time.sleep(0.02)\n'
            'else:\n'
            '    def graceful(*args):\n'
            '        (root / "graceful").touch()\n'
            '        sys.exit(0)\n'
            '    signal.signal(signal.SIGTERM, graceful)\n'
            '    (root / "ready").write_text(str(os.getpid()))\n'
            '    while True:\n'
            '        time.sleep(1)\n')

    def wait_file(self, name):
        limit = time.monotonic() + 5
        path = self.root / name
        while not path.exists() or not path.read_text().strip() and name == 'ready':
            if time.monotonic() >= limit:
                self.fail(f'fixture did not reach {name}')
            time.sleep(0.02)
        return path

    def start_fixture(self, delayed_stop=False):
        (self.root / 'ready').unlink(missing_ok=True)
        command = ['systemd-run', '--quiet', '--no-ask-password', '--collect',
                   '--unit', self.unit, '--property=RuntimeMaxSec=30',
                   '--property=TimeoutStopSec=10', '--property=KillMode=control-group']
        if delayed_stop:
            command.append(f'--property=ExecStopPost={sys.executable} {self.script} stop')
        subprocess.run(command + [sys.executable, str(self.script), 'serve'], check=True, timeout=10)
        self.pid = int(self.wait_file('ready').read_text())
        self.assertTrue(Path(f'/proc/{self.pid}').exists())
        return self.pid

    def stop_fixture(self):
        (self.root / 'release').touch()
        # The successful crash case is already collected; cleanup also handles
        # assertions made while a fixture is still running or deactivating.
        stopped = subprocess.run(['systemctl', '--no-ask-password', 'stop', self.unit],
                                 capture_output=True, text=True, timeout=15)
        try:
            guest.wait_node_exit(self.unit, self.pid, timeout=5)
        except Exception:
            stopped.check_returncode()
            raise

    def test_crash_collected_unit_and_restart_same_name(self):
        with subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']) as unrelated:
            try:
                old_pid = self.start_fixture()
                self.assertEqual(guest.crash_node_service(self.unit), old_pid)
                self.assertFalse(Path(f'/proc/{old_pid}').exists())
                self.assertFalse((self.root / 'graceful').exists())
                # Deterministically exercise the already-collected state, not
                # just whichever side of the kill/GC race this machine reaches.
                guest.wait_node_exit(self.unit, old_pid, timeout=2)
                new_pid = self.start_fixture()
                self.assertNotEqual(new_pid, old_pid)
                self.assertEqual((self.root / 'ready').read_text(), str(new_pid))
                self.assertIsNone(unrelated.poll(), 'crash must not kill an unrelated process')
            finally:
                unrelated.kill()
                unrelated.wait(timeout=5)

    def test_crash_waits_for_deactivation_and_has_a_deadline(self):
        old_pid = self.start_fixture(delayed_stop=True)
        with ThreadPoolExecutor(max_workers=1) as executor:
            crashed = executor.submit(guest.crash_node_service, self.unit)
            try:
                self.wait_file('deactivating')
                self.assertFalse(Path(f'/proc/{old_pid}').exists())
                self.assertFalse(crashed.done(), 'PID death alone cannot permit unit-name reuse')
                with self.assertRaisesRegex(TimeoutError, 'exit/collection deadline'):
                    guest.wait_node_exit(self.unit, old_pid, timeout=0.2)
                self.assertFalse(crashed.done())
            finally:
                (self.root / 'release').touch()
            self.assertEqual(crashed.result(timeout=10), old_pid)
        self.assertFalse((self.root / 'graceful').exists())


if __name__ == '__main__':
    unittest.main()
