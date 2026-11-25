"""Operator driver tests: parsing, gates, receipts and the dry-run CLI."""
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import fault_cells

ROOT = Path(__file__).resolve().parents[2]


class FakeRemote:
    """The injected `gcloud compute ssh` boundary, in memory."""

    def __init__(self, *, files=None, outputs=None):
        self.files = dict(files or {})
        self.outputs = dict(outputs or {})
        self.calls = []

    def run(self, host, command, *, stdin=None, check=True):
        self.calls.append((host, command, stdin))
        value = self.outputs.get((host, command.split(' --root')[0]))
        if value is None:
            value = b'{}'
        return 0, value, b''

    def shell(self, host, command, *, stdin=None, check=True):
        return self.run(host, command, stdin=stdin, check=check)

    def exists(self, host, path):
        return str(path) in self.files

    def read_bytes(self, host, path):
        if str(path) not in self.files:
            raise fault_cells.CellError(f'{host}: missing {path}')
        return self.files[str(path)]


class FakeChild:
    def __init__(self, *, alive=True, rows=(), exit_code=None):
        self_rows = list(rows)
        self.alive_ = alive
        self.exit = exit_code
        self.signals = []

    def alive(self):
        return self.alive_

    def rows(self):
        return self.rows_

    def signal(self, value):
        self.signals.append(value)

    def stop(self):
        pass


def sample_rows(count, *, outcome='success', operation='put', phase='measurement'):
    lines = ['phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,'
             'completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,'
             'error_kind,caller_sample_id,released_ns,stream,source_checkpoint,'
             'source_tx,repetition,db,table,batch_phase,parent_batch,members']
    for index in range(count):
        error = '' if outcome == 'success' else 'coordinator lease lapsed'
        lines.append(f'{phase},{operation},{outcome},{index},0,true,{index},0,0,0,1,4096,'
                     f'{error},0,{index},,,,,,,,,')
    return '\n'.join(lines) + '\n'


class SampleParsing(unittest.TestCase):
    def test_rows_and_acknowledged_count_only_successes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.csv.tmp'
            path.write_text(sample_rows(3) +
                            sample_rows(2, outcome='error').split('\n', 1)[1])
            parsed = fault_cells.rows(path)
            self.assertEqual(len(parsed), 5)
            self.assertEqual(fault_cells.acknowledged(parsed), 3)
            self.assertEqual(len(fault_cells.failures(parsed)), 2)
            self.assertEqual(fault_cells.failures(parsed)[0]['error_kind'],
                             'coordinator lease lapsed')

    def test_missing_file_is_empty_not_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(fault_cells.rows(Path(directory) / 'absent.csv'), [])

    def test_a_short_row_is_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.csv'
            path.write_text('a,b\n1,2\n3\n')
            self.assertEqual(fault_cells.rows(path), [dict(a='1', b='2')])


class TypedErrors(unittest.TestCase):
    def test_lease_and_fence_classes_are_typed(self):
        for text in ('coordinator lease lapsed',
                     'coordinator lease held by another incarnation',
                     'stale coordinator lease authority',
                     'sealed by write protect',
                     'Permission denied (os error 13)'):
            with self.subTest(text=text):
                self.assertTrue(fault_cells.CellDriver.typed(text))

    def test_an_unrelated_error_is_not_typed(self):
        for text in ('', 'connection refused', 'value mismatch'):
            with self.subTest(text=text):
                self.assertFalse(fault_cells.CellDriver.typed(text))


class Gates(unittest.TestCase):
    def driver(self):
        return fault_cells.CellDriver.__new__(fault_cells.CellDriver)

    def test_write_gate_waits_for_the_count(self):
        driver = self.driver()
        with tempfile.TemporaryDirectory() as directory:
            child = fault_cells.Coordinator.__new__(fault_cells.Coordinator)
            child.output = Path(directory)
            (child.output / 'samples.csv.tmp').write_text(sample_rows(5))
            child.alive = lambda: True
            gate = driver.wait_for(child, lambda: dict(
                reached=fault_cells.acknowledged(child.rows()) >= 5,
                acknowledged=fault_cells.acknowledged(child.rows())),
                timeout=1, label='5 writes', poll=0.01)
        self.assertTrue(gate['reached'])
        self.assertEqual(gate['observations'][-1]['acknowledged'], 5)

    def test_a_gate_that_never_fires_reports_the_reason(self):
        driver = self.driver()

        class Idle:
            def alive(self):
                return True

        gate = driver.wait_for(Idle(), lambda: dict(reached=False), timeout=0.05,
                               label='index growth', poll=0.01)
        self.assertFalse(gate['reached'])
        self.assertIn('not observed', gate['reason'])

    def test_a_dead_coordinator_ends_the_gate(self):
        driver = self.driver()

        class Dead:
            def alive(self):
                return False

        gate = driver.wait_for(Dead(), lambda: dict(reached=False), timeout=5,
                               label='writes', poll=0.01)
        self.assertFalse(gate['reached'])
        self.assertIn('exited before', gate['reason'])


class OwnerWAL(unittest.TestCase):
    def cluster(self, views):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.guest_root = Path('/guest')
        cluster.owners = ['h1', 'h2']
        cluster.remote = FakeRemote()
        iterator = iter(views)
        cluster.wal = lambda host: next(iterator)
        return cluster

    def test_unlink_cut_needs_only_a_disappearance(self):
        before = dict(host='h1', files={'/g/wal_0000000000000000': dict(size=1, mtime=1.0)},
                      roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        gone = dict(host='h1', files={},
                    roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        with patch.object(fault_cells.time, 'sleep', lambda _: None):
            cluster = self.cluster([before, gone])
            driver = fault_cells.CellDriver(cluster, fault_cells.mechanism.find('S1-6S-unlink-native'))
            gate = driver.gate_retirement('h1', 'unlink', timeout=1)
        self.assertTrue(gate['reached'])
        self.assertIn('after the unlink', gate['note'])

    def test_dir_sync_cut_needs_the_root_rewrite(self):
        before = dict(host='h1', files={'/g/wal_0000000000000000': dict(size=1, mtime=1.0)},
                      roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        unlinked = dict(host='h1', files={},
                        roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        synced = dict(host='h1', files={},
                      roots={'/g/partition-wal-root-v1': dict(size=2, mtime=2.0)})
        with patch.object(fault_cells.time, 'sleep', lambda _: None):
            driver = fault_cells.CellDriver(
                self.cluster([before, unlinked, unlinked, synced]),
                fault_cells.mechanism.find('S1-6S-dir-sync-native'))
            gate = driver.gate_retirement('h1', 'dir-sync', timeout=1)
        self.assertTrue(gate['reached'])
        self.assertIn('directory sync', gate['note'])


class OwnerRolls(unittest.TestCase):
    def test_the_highest_wal_file_id_proves_the_roll(self):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.wal = lambda host: dict(files={
            '/g/db-x/wal_0000000000000000': dict(size=1, mtime=1.0),
            '/g/db-x/wal_0000000000000002': dict(size=1, mtime=1.0),
        })
        row = cluster.rolls('h1')
        self.assertEqual(row['max_file_id'], 2)
        self.assertEqual(row['files'], 2)

    def test_no_wal_file_reports_no_roll(self):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.wal = lambda host: dict(files={})
        self.assertIsNone(cluster.rolls('h1')['max_file_id'])


class Receipts(unittest.TestCase):
    def test_receipts_are_copied_in_partition_order(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory)
            (config / 'nodes.json').write_text(json.dumps([
                dict(namespace='ns', partition=1, host='h2', ready='/guest/node-1/ready'),
                dict(namespace='other', partition=0, host='h9', ready='/guest/other/ready'),
                dict(namespace='ns', partition=0, host='h1', ready='/guest/node-0/ready'),
            ]))
            remote = FakeRemote(files={
                '/guest/node-0/ready.durability.json': b'{"partition": 0}',
                '/guest/node-1/ready.durability.json': b'{"partition": 1}',
            })
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.config_dir, cluster.remote = config, remote
            cluster.guest_root, cluster.owners = Path('/guest'), ['h1', 'h2']
            rows = cluster.receipts('ns')
        self.assertEqual([row['partition'] for row in rows], [0, 1])
        self.assertEqual(rows[0]['receipt'], {'partition': 0})

    def test_missing_receipt_coverage_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory)
            (config / 'nodes.json').write_text(json.dumps([
                dict(namespace='ns', partition=0, host='h1', ready='/guest/node-0/ready')]))
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.config_dir = config
            cluster.remote = FakeRemote(files={
                '/guest/node-0/ready.durability.json': b'{}'})
            cluster.guest_root, cluster.owners = Path('/guest'), ['h1', 'h2']
            with self.assertRaisesRegex(fault_cells.CellError, 'do not cover'):
                cluster.receipts('ns')


class CrashChecks(unittest.TestCase):
    def test_a_reached_gate_and_a_kill_pass(self):
        cell = fault_cells.mechanism.find('S1-2S-publication-native')
        checks = fault_cells.crash_checks(cell, dict(
            gate=dict(reached=True, label='200 acknowledged writes'),
            kill=dict(exit_code=-9)))
        self.assertTrue(all(check['ok'] for check in checks), checks)

    def test_a_missed_gate_fails(self):
        cell = fault_cells.mechanism.find('S1-2S-snapshot-native')
        checks = fault_cells.crash_checks(cell, dict(
            gate=dict(reached=False, reason='index growth not observed'),
            kill=dict(exit_code=-9)))
        self.assertFalse(all(check['ok'] for check in checks))

    def test_an_owner_kill_needs_the_typed_failure(self):
        cell = fault_cells.mechanism.find('S1-6S-unlink-native')
        checks = fault_cells.crash_checks(cell, dict(
            gate=dict(reached=True, cut='unlink', note='after the unlink'),
            kill=dict(pids=[10]),
            failure=dict(gate=dict(reached=True, errors=1))))
        self.assertTrue(all(check['ok'] for check in checks), checks)
        missing = fault_cells.crash_checks(cell, dict(
            gate=dict(reached=True, cut='unlink'), kill=dict(pids=[10]),
            failure=dict(gate=dict(reached=False, errors=0))))
        self.assertFalse(all(check['ok'] for check in missing))


class FenceVerdict(unittest.TestCase):
    def records(self, fence):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        driver.cell = fault_cells.mechanism.find('S1-5S-stale-native')
        driver.fence = fence
        driver.parts = []
        return fault_cells.verdict_parts(driver, [])[0]

    def test_one_writer_with_a_typed_refusal_passes(self):
        record = self.records(dict(case='stale', tier='native', writers=1,
                                   typed_refusal=True,
                                   refusal_text='coordinator lease lapsed',
                                   no_success_after_refusal=True,
                                   first_error_row=40, last_success_row=39,
                                   fence_reject_ms=12.5))
        self.assertTrue(record['ok'], record)

    def test_two_writers_fail(self):
        record = self.records(dict(case='stale', tier='native', writers=2,
                                   typed_refusal=True, refusal_text='lease',
                                   no_success_after_refusal=True,
                                   fence_reject_ms=1.0))
        self.assertFalse(record['ok'])

    def test_an_untyped_refusal_fails(self):
        record = self.records(dict(case='stale', tier='native', writers=1,
                                   typed_refusal=False, refusal_text='boom',
                                   no_success_after_refusal=True,
                                   fence_reject_ms=1.0))
        self.assertFalse(record['ok'])

    def test_a_sealed_read_needs_both_halves(self):
        record = self.records(dict(case='sealed', tier='native', writers=1,
                                   typed_refusal=True, refusal_text='lease lapsed',
                                   sealed_partitions=1, sealed_read_ok=True,
                                   sealed_read_ms=2.0, sealed_write_refused=False,
                                   no_success_after_refusal=True,
                                   fence_reject_ms=3.0,
                                   write_protect_apply_ms=900.0))
        self.assertFalse(record['ok'])


class DryRun(unittest.TestCase):
    def test_the_plan_names_every_run_without_touching_a_host(self):
        import contextlib
        argv = ['fault_cells.py', '--cell', 'M13-batch132-synchronous',
                '--build', str(ROOT), '--output', '/tmp/mech-out',
                '--guest-root', '/var/tmp/guest', '--coordinator', 'H0',
                '--owners', 'H1,H2', '--zone', 'us-central1-a', '--dry-run']
        stream = io.StringIO()
        with patch.object(sys, 'argv', argv), redirect_stdout(stream):
            fault_cells.main()
        plan = json.loads(stream.getvalue())
        self.assertEqual(plan['cell'], 'M13-batch132-synchronous')
        self.assertTrue(plan['synthetic'])
        self.assertEqual(plan['runs'][0]['batch'], 2)
        self.assertEqual(plan['runs'][0]['value'], 65_536)
        self.assertEqual(plan['tier'], 'synchronous')


if __name__ == '__main__':
    unittest.main()
