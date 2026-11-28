"""Operator driver tests: parsing, gates, receipts and the dry-run CLI."""
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import yaml

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

    def test_a_refused_mutation_is_an_unknown_outcome_not_an_error(self):
        """The engine records `unknown_mutation` for a refused put; it is not `error`."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.csv.tmp'
            path.write_text(sample_rows(3) +
                            sample_rows(2, outcome='unknown_mutation').split('\n', 1)[1])
            parsed = fault_cells.rows(path)
            self.assertEqual(fault_cells.acknowledged(parsed), 3)
            self.assertEqual(fault_cells.failures(parsed), [])
            self.assertEqual(len(fault_cells.refused(parsed)), 2)
            self.assertTrue(fault_cells.CellDriver.typed(
                fault_cells.refused(parsed)[0]['error_kind']))

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
                     'stale or sealed request authority',
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


class SnapshotGate(unittest.TestCase):
    def driver(self):
        return fault_cells.CellDriver.__new__(fault_cells.CellDriver)

    def tree(self, root):
        run = Path(root) / 'measure' / 'run'
        run.mkdir(parents=True)
        (run / 'samples.csv.tmp').write_text(sample_rows(fault_cells.GATE_WRITES))
        region = Path(root) / 'measure' / 'coordinator' / 'cr'
        region.parent.mkdir(parents=True)
        return run, region

    def child(self, run):
        return types.SimpleNamespace(
            output=run, alive=lambda: True,
            rows=lambda: fault_cells.rows(run / 'samples.csv.tmp'))

    def test_the_gate_reaches_on_a_stored_control_region(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, region = self.tree(tmp)
            region.write_text('region v1')
            calls = []

            def sleep(_):
                calls.append(1)
                if len(calls) == 1:
                    region.write_text('region v2 with the stored root')
            with patch.object(fault_cells.time, 'sleep', sleep):
                gate = self.driver().gate_snapshot(self.child(run), timeout=1)
        self.assertTrue(gate['reached'], gate)
        self.assertIn('cr', gate['observations'][-1]['changed'])

    def test_creating_the_region_at_open_is_not_a_store(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, region = self.tree(tmp)
            calls = []

            def sleep(_):
                calls.append(1)
                if len(calls) == 1:
                    region.write_text('created at open')
            with patch.object(fault_cells.time, 'sleep', sleep):
                gate = self.driver().gate_snapshot(self.child(run), timeout=0.3)
        self.assertFalse(gate['reached'], gate)

    def test_a_dead_coordinator_ends_the_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = self.tree(tmp)
            child = types.SimpleNamespace(output=run, alive=lambda: False,
                                          rows=lambda: [])
            gate = self.driver().gate_snapshot(child, timeout=1)
        self.assertFalse(gate['reached'])
        self.assertIn('exited before', gate['reason'])

    def test_the_gate_records_the_coordinator_wal_bytes(self):
        """The next run must answer how many value bytes the engine had published.

        `S1-2S-snapshot-native/attempt-3` missed the 8 GiB cadence at its first
        60 s poll and retained nothing about the written volume. The gate now
        carries the WAL's allocated bytes (a `set_len` preallocation allocates
        no blocks, so allocated bytes are the written volume) and their delta
        from the baseline.
        """
        with tempfile.TemporaryDirectory() as tmp:
            run, region = self.tree(tmp)
            region.write_text('region v1')
            wal = run.parent / 'coordinator' / 'wal_0000000000000000'
            wal.write_bytes(b'x' * 8192)
            before = wal.stat().st_blocks * 512
            calls = []

            def sleep(_):
                calls.append(1)
                if len(calls) == 1:
                    with wal.open('ab') as handle:
                        handle.write(b'y' * 8192)
                    region.write_text('region v2 with the stored root')
            with patch.object(fault_cells.time, 'sleep', sleep):
                gate = self.driver().gate_snapshot(self.child(run), timeout=1)
        self.assertTrue(gate['reached'], gate)
        self.assertEqual(gate['coordinator_wal_footprint_baseline'], before)
        self.assertGreater(gate['observations'][-1]['coordinator_wal_footprint_bytes'],
                           before)
        self.assertGreater(gate['observations'][-1]['coordinator_wal_footprint_delta'], 0)
        self.assertIn('not the engine index_value_bytes', gate['wal_measure'])

    def test_a_coordinator_without_a_wal_reports_no_volume(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, region = self.tree(tmp)
            region.write_text('region v1')
            driver = self.driver()
            self.assertIsNone(driver._coordinator_wal_footprint(self.child(run)))
            gate = driver.gate_snapshot(self.child(run), timeout=0.1)
        self.assertIsNone(gate['coordinator_wal_footprint_baseline'])
        self.assertIsNone(gate['observations'][-1]['coordinator_wal_footprint_bytes'])
        self.assertIsNone(gate['observations'][-1]['coordinator_wal_footprint_delta'])

    def test_a_missed_gate_is_written_before_the_cell_raises(self):
        """The next missed cut must ship its probe series.

        `crash()` raises before it appends a part, and the failure upload walks
        files only, so S1-2S-snapshot's two fleet-A failures shipped no gate at
        all. The record now lands at `<measure>/gate.json` first.
        """
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = self.tree(tmp)
            path = Path(run)

            class Process:
                returncode = None
                pid = 7

            child = types.SimpleNamespace(output=path, log=path / 'run.log',
                                          process=Process(), started_unix_ns=1,
                                          stop_record=None, stop=lambda: None)
            driver = self.driver()
            driver.cell = fault_cells.mechanism.find('S1-2S-snapshot-native')
            driver.open = lambda run, name: child
            driver.gate_writes = lambda child, count: dict(reached=True)
            driver.gate_snapshot = lambda child, timeout=None: dict(
                reached=False, reason='snapshot control-region store not observed',
                observations=[dict(seconds=60.0,
                                   coordinator_wal_footprint_bytes=9 << 30)])
            driver.kill_coordinator = lambda child: {}
            with self.assertRaisesRegex(fault_cells.CellError, 'gate missed'):
                driver.crash()
            written = json.loads((path.parent / 'gate.json').read_text())
        self.assertFalse(written['reached'])
        self.assertEqual(written['observations'][0]['coordinator_wal_footprint_bytes'],
                         9 << 30)

    def test_a_missed_write_gate_persists_itself_without_referring_to_itself(self):
        """The write gate is the record when it misses; it must not nest itself.

        `gate = writes; gate['writes'] = writes` made the persisted record a
        cycle (`ValueError: Circular reference detected`) and hid the CellError.
        """
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = self.tree(tmp)
            path = Path(run)

            class Process:
                returncode = None
                pid = 7

            child = types.SimpleNamespace(output=path, log=path / 'run.log',
                                          process=Process(), started_unix_ns=1,
                                          stop_record=None, stop=lambda: None)
            driver = self.driver()
            driver.cell = fault_cells.mechanism.find('S1-2S-snapshot-native')
            driver.open = lambda run, name: child
            driver.gate_writes = lambda child, count: dict(
                reached=False, reason='200 acknowledged writes not observed',
                observations=[dict(acknowledged=12)])
            driver.kill_coordinator = lambda child: {}
            with self.assertRaisesRegex(fault_cells.CellError, 'gate missed'):
                driver.crash()
            written = json.loads((path.parent / 'gate.json').read_text())
        self.assertEqual(written['reason'], '200 acknowledged writes not observed')
        self.assertEqual(written['observations'], [dict(acknowledged=12)])
        self.assertNotIn('writes', written)


class FenceExclusivity(unittest.TestCase):
    """S1-5S: the stale writer cannot write after the new authority's fence.

    `fixtures/s1-5s-contend-attempt3.json` is the real fleet-A contend receipt
    record (no start clocks, no fence instant) and
    `fixtures/s1-5s-contend-a-excerpt.csv` is the real stale writer's row
    excerpt. `fixtures/s1-5s-contend-attempt3.json:new_writer` holds the real new
    writer's counts and its first success at 1.624 s after its own start.
    """

    def fixture(self, name):
        return json.loads((ROOT / 'experiments/gcp-rdma/fixtures' / name).read_text())

    def driver(self, fence, parts=()):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        driver.cell = dict(id='S1-5S-contend-native')
        driver.fence = fence
        driver.parts = list(parts)
        return driver

    def test_the_old_contend_record_names_the_missing_proof(self):
        """No start clocks means no fence instant, which must fail named."""
        real = self.fixture('s1-5s-contend-attempt3.json')
        fence = dict(real['fence'], writers=2,
                     refusal_kind='coordinator-self-fence',
                     stale_success_after_fence=None,
                     second_measurement_rows=real['new_writer']['measurement_rows'],
                     exclusivity_reason=('the stale writer\'s rows or the fence instant '
                                         'are missing, so no write after the fence can '
                                         'be ruled out'))
        record = fault_cells.fence_record(self.driver(fence))
        self.assertFalse(record['ok'])
        named = {check['check']: check for check in record['checks']}
        self.assertFalse(named['no stale-writer write completed after the fence']['ok'])
        self.assertFalse(named['exclusive writes are proven']['ok'])
        self.assertIn('missing',
                      named['exclusive writes are proven']['detail']['reason'])
        self.assertFalse(record['fence']['audit_available'])
        self.assertIn('recorded in the fence record', record['fence']['audit_note'])
        self.assertFalse(record['fence']['exclusive'])
        named_checks = {check['check']: check for check in record['checks']}
        self.assertFalse(named_checks[
            'the owners fenced before the new authority\'s first accepted write']['ok'])
        self.assertIn('no owner-side fence snapshot',
                      named_checks['the owners fenced before the new authority\'s '
                                   'first accepted write']['detail']['reason'])

    def test_the_real_stale_rows_are_judged_against_the_new_writers_fence(self):
        rows_ = fault_cells.rows(ROOT / 'experiments/gcp-rdma/fixtures' / 's1-5s-contend-a-excerpt.csv')
        real = self.fixture('s1-5s-contend-attempt3.json')['new_writer']
        child = types.SimpleNamespace(started_unix_ns=1_000_000_000,
                                      output=Path('/nonexistent'))
        # The real excerpt keeps every early non-success row and drops the
        # successes (the receipt's last_success_row is 89,156 of the full file),
        # so it can never prove the property on its own: the verdict must read
        # the live rows and both start clocks.
        measurement = [row for row in rows_ if row['phase'] == 'measurement']
        self.assertTrue(measurement)
        self.assertEqual({row['outcome'] for row in measurement} - {'success'},
                         {row['outcome'] for row in measurement})
        self.assertIsNone(fault_cells.CellDriver._last_success_ns(child, rows_))
        self.assertEqual(fault_cells.CellDriver._stale_after_fence(child, rows_, 1), 0)
        self.assertIsNone(fault_cells.CellDriver._stale_after_fence(child, rows_, None),
                          'no fence instant is never read as zero')
        # The counting itself: two successes at +1 s and +3 s, a fence between
        # them finds the later one.
        late = [dict(phase='measurement', outcome='success', completed_ns='1000000000'),
                dict(phase='measurement', outcome='success', completed_ns='3000000000')]
        self.assertEqual(fault_cells.CellDriver._stale_after_fence(
            child, late, 1_000_000_000 + 3_500_000_000), 0)
        self.assertEqual(fault_cells.CellDriver._stale_after_fence(
            child, late, 1_000_000_000 + 2_000_000_000), 1)
        self.assertIsNone(fault_cells.CellDriver._stale_after_fence(
            child, [dict(phase='measurement', outcome='success')], 1))
        second = types.SimpleNamespace(started_unix_ns=5_000_000_000,
                                       output=Path('/nonexistent'))
        self.assertEqual(real['first_success_completed_ns'], 1_624_106_620)
        self.assertGreater(real['measurement_successes'], 0)

    def test_recorded_clocks_and_no_stale_success_is_a_pass(self):
        real = self.fixture('s1-5s-contend-attempt3.json')
        fence = dict(real['fence'], refusal_kind='owner-row', exclusive=True,
                     writers=1, stale_success_after_fence=0,
                     audit_available=True, audit_integrity_ok=True,
                     second_measurement_rows=real['new_writer']['measurement_rows'],
                     owner_fence=dict(evidence_available=True,
                                      fence_counters_increased=True,
                                      no_stale_growth=True,
                                      stale_generations=['h0:/db-1/epoch_3'],
                                      stale_growth=dict(new_files=[], grown=[]),
                                      scraped_unix_ns=5, reason=None),
                     fence_unix_ns=1_000_000_000_000,
                     first_started_unix_ns=999_000_000_000,
                     second_started_unix_ns=999_500_000_000,
                     fence_reject_ms=12.5, exclusivity_reason=None)
        record = fault_cells.fence_record(self.driver(fence))
        self.assertTrue(record['ok'], record)

    def test_a_stale_success_after_the_fence_fails_named(self):
        real = self.fixture('s1-5s-contend-attempt3.json')
        fence = dict(real['fence'], refusal_kind='owner-row', exclusive=False,
                     writers=2, stale_success_after_fence=3,
                     second_measurement_rows=1,
                     fence_unix_ns=1, first_started_unix_ns=0,
                     second_started_unix_ns=0, fence_reject_ms=1.0,
                     exclusivity_reason='3 stale-writer write(s) completed after the fence')
        record = fault_cells.fence_record(self.driver(fence))
        self.assertFalse(record['ok'])
        detail = [c for c in record['checks']
                  if c['check'] == 'no stale-writer write completed after the fence'][0]
        self.assertEqual(detail['detail']['stale_success_after_fence'], 3)

    def test_a_self_fence_is_accepted_with_a_bounded_latency(self):
        fence = dict(case='lapse', refusal_kind='coordinator-self-fence',
                     refusal_text='coordinator lease lapsed on owner partition 1',
                     first_refusal_row=None, exclusive=True, writers=1,
                     stale_success_after_fence=0, sealed_partitions=1,
                     audit_available=True, audit_integrity_ok=True,
                     owner_fence=dict(evidence_available=True,
                                      fence_counters_increased=True,
                                      no_stale_growth=True,
                                      stale_generations=['h0:/db-1/epoch_3'],
                                      stale_growth=dict(new_files=[], grown=[]),
                                      scraped_unix_ns=5, reason=None),
                     write_protect_apply_ms=7097.3, fence_reject_ms=None,
                     fence_reject_window_ms=dict(lower_ms=0.0, upper_ms=420.0),
                     fence_unix_ns=1, first_started_unix_ns=0)
        record = fault_cells.fence_record(self.driver(fence))
        self.assertTrue(record['ok'], record)
        latency = [c for c in record['checks']
                   if c['check'] == 'fence latency measured or bounded'][0]
        self.assertTrue(latency['ok'])
        self.assertEqual(latency['detail']['kind'], 'coordinator-self-fence')
        self.assertEqual(latency['detail']['window'], dict(lower_ms=0.0, upper_ms=420.0))


class OwnerWAL(unittest.TestCase):
    def cluster(self, views):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.guest_root = Path('/guest')
        cluster.owners = ['h1', 'h2']
        cluster.remote = FakeRemote()
        iterator = iter(views)
        cluster.wal = lambda host: next(iterator)
        return cluster

    def test_the_listing_reads_owner_files_under_the_epoch_directories(self):
        """The real listing script, executed over a real nested owner tree.

        The engine writes `wal_*`, `partition-wal-root-v1` and
        `partition-seal-v5` under `db-<hex>/epoch_<hex>`, not at the database
        root. The script is run by `sh`, not answered from canned output.
        """
        class ShellRemote(FakeRemote):
            def run(self, host, command, *, stdin=None, check=True):
                self.calls.append((host, command, stdin))
                done = subprocess.run(command, shell=True, capture_output=True)
                return done.returncode, done.stdout, done.stderr

        with tempfile.TemporaryDirectory() as root:
            epoch = Path(root) / ('db-' + 'a' * 32) / 'epoch_0000000000000001'
            epoch.mkdir(parents=True)
            wal = epoch / 'wal_0000000000000000'
            wal.write_bytes(b'x' * 11)
            durable = epoch / 'partition-wal-root-v1'
            durable.write_bytes(b'r' * 5)
            (epoch / 'partition-seal-v5').write_bytes(b's')
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.remote = ShellRemote()
            cluster.owner_root = lambda host: Path(root)
            view = cluster.wal('h1')
            self.assertEqual(set(view['files']), {str(wal)})
            self.assertEqual(set(view['roots']), {str(durable)})
            self.assertEqual(cluster.rolls('h1')['max_file_id'], 0)
            seals = cluster.seal_markers('h1')
            self.assertEqual(seals['databases'], 1)
            self.assertEqual(seals['sealed'], 1)

    def test_the_listing_exits_zero_while_a_root_is_mid_rename(self):
        """The durable root is replaced by rename; a missing root is not a failure."""
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.remote = FakeRemote()
        cluster.owner_root = lambda host: Path('/guest')
        self.assertEqual(cluster.wal('h1')['files'], {})
        command = cluster.remote.calls[-1][1]
        self.assertIn('done; exit 0', command)

    def test_a_root_line_is_kept_out_of_the_wal_files(self):
        class Rows(FakeRemote):
            def run(self, host, command, *, stdin=None, check=True):
                return 0, (b'/g/db-x/wal_0000000000000000 4096 1.5\n'
                           b'/g/db-x/partition-wal-root-v1 512 2.5\n'), b''

        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.remote = Rows()
        cluster.owner_root = lambda host: Path('/guest')
        view = cluster.wal('h1')
        self.assertEqual(view['files'], {
            '/g/db-x/wal_0000000000000000': dict(size=4096, mtime=1.5)})
        self.assertEqual(view['roots'], {
            '/g/db-x/partition-wal-root-v1': dict(size=512, mtime=2.5)})

    def test_a_transport_failure_still_reports_its_cause(self):
        class Failing(FakeRemote):
            def run(self, host, command, *, stdin=None, check=True):
                return 1, b'', b'connection reset by peer'

        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.remote = Failing()
        cluster.owner_root = lambda host: Path('/guest')
        with self.assertRaisesRegex(fault_cells.CellError,
                                    'connection reset by peer'):
            cluster.wal('h1')

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

    def test_dir_sync_baselines_the_root_after_the_first_store(self):
        no_root = dict(host='h1', files={'/g/wal_0000000000000000': dict(size=1, mtime=1.0)},
                       roots={})
        first = dict(host='h1', files={'/g/wal_0000000000000000': dict(size=1, mtime=1.0)},
                     roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        gone = dict(host='h1', files={},
                    roots={'/g/partition-wal-root-v1': dict(size=1, mtime=1.0)})
        synced = dict(host='h1', files={},
                      roots={'/g/partition-wal-root-v1': dict(size=2, mtime=2.0)})
        with patch.object(fault_cells.time, 'sleep', lambda _: None):
            driver = fault_cells.CellDriver(
                self.cluster([no_root, first, gone, synced]),
                fault_cells.mechanism.find('S1-6S-dir-sync-native'))
            gate = driver.gate_retirement('h1', 'dir-sync', timeout=1)
        self.assertTrue(gate['reached'], gate)

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
    """One receipt per partition, bound to this launch and this process."""

    def prepared(self, directory, *, tier='native', pids=(101, 102), nonce='n'):
        """A deployment of namespace `ns` with a launch file per owner."""
        config = Path(directory)
        nodes = []
        for partition, host in enumerate(('h1', 'h2')):
            launch = config / 'ns' / f'node-{partition}' / 'launch.yaml'
            launch.parent.mkdir(parents=True)
            launch.write_text(yaml.safe_dump(dict(
                partition=partition, listen=f'10.0.0.{partition + 1}:9400',
                launch_nonce=f'{nonce}-{partition}', durability=dict(tier=tier))))
            nodes.append(dict(namespace='ns', partition=partition, host=host,
                              launch=str(launch), ready=f'/guest/ns/node-{partition}/ready'))
        (config / 'nodes.json').write_text(json.dumps(nodes))
        (config / 'config.json').write_text(json.dumps(
            {'databases': {'ns': {'read_cache_bytes': 0,
                                  'tls': {'transport': {'kind': 'tcp'}}}}}))
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.config_dir, cluster.remote = config, FakeRemote()
        cluster.guest_root, cluster.owners = Path('/guest'), ['h1', 'h2']
        cluster.tier = tier
        cluster.namespaces = {
            'h1': [dict(partition=0, pid=pids[0], process_group=pids[0], stopped=False)],
            'h2': [dict(partition=1, pid=pids[1], process_group=pids[1], stopped=False)]}
        cluster.bind('ns')
        return cluster

    def receipt(self, partition, *, pid, nonce='n', tier='native',
                address=None):
        return dict(partition=partition, address=address or f'10.0.0.{partition + 1}:9400',
                    launch_nonce=f'{nonce}-{partition}', pid=pid, boot_id='b',
                    started_unix_ns=1, durability=dict(tier=tier))

    def seed(self, cluster, *, pids=(101, 102), **changes):
        files = {}
        for partition in (0, 1):
            body = self.receipt(partition, pid=pids[partition], **changes)
            files[f'/guest/ns/node-{partition}/ready.durability.json'] = \
                json.dumps(body).encode()
        cluster.remote.files.update(files)

    def test_receipts_are_copied_in_partition_order(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster)
            rows = cluster.receipts('ns')
        self.assertEqual([row['partition'] for row in rows], [0, 1])
        self.assertEqual([row['pid'] for row in rows], [101, 102])
        self.assertEqual(rows[0]['receipt']['launch_nonce'], 'n-0')

    def test_the_context_serializes_receipt_paths(self):
        """`external_owner_durability_receipts` is a list of path strings."""
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster)
            rows = cluster.receipts('ns')
            context = cluster.context('ns', rows)
            expected = [str(Path(directory) / 'receipts/node-0.json'),
                        str(Path(directory) / 'receipts/node-1.json')]
        self.assertEqual(context['receipts'], expected)
        self.assertTrue(all(isinstance(path, str) for path in context['receipts']))

    def test_a_receipt_from_another_incarnation_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster, pids=(9999, 102))
            with self.assertRaisesRegex(fault_cells.CellError, 'stale or foreign'):
                cluster.receipts('ns')

    def test_a_receipt_from_another_launch_nonce_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster, nonce='old')
            with self.assertRaisesRegex(fault_cells.CellError, 'launch nonce'):
                cluster.receipts('ns')

    def test_a_receipt_of_another_tier_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster, tier='synchronous')
            with self.assertRaisesRegex(fault_cells.CellError, 'tier synchronous'):
                cluster.receipts('ns')

    def test_a_receipt_of_another_partition_or_endpoint_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            self.seed(cluster)
            body = self.receipt(0, pid=101, address='10.9.9.9:9400')
            cluster.remote.files['/guest/ns/node-0/ready.durability.json'] = \
                json.dumps(body).encode()
            with self.assertRaisesRegex(fault_cells.CellError, 'address'):
                cluster.receipts('ns')
            body = self.receipt(1, pid=101)
            cluster.remote.files['/guest/ns/node-0/ready.durability.json'] = \
                json.dumps(body).encode()
            with self.assertRaisesRegex(fault_cells.CellError, 'partition 1 != 0'):
                cluster.receipts('ns')

    def test_missing_receipt_coverage_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.prepared(directory)
            cluster.owners = ['h1']
            self.seed(cluster)
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
                                   refusal_kind='owner-row',
                                   refusal_text='coordinator lease lapsed',
                                   no_success_after_refusal=True,
                                   first_error_row=40, last_success_row=39,
                                   first_refusal_row=40,
                                   stale_success_after_fence=0, exclusive=True,
                                   second_measurement_rows=200,
                                   audit_available=True, audit_integrity_ok=True,
owner_fence=dict(evidence_available=True, fence_counters_increased=True,
                                       no_stale_growth=True, stale_generations=['h0:/db-1/epoch_3'],
                                       stale_growth=dict(new_files=[], grown=[]),
                                       scraped_unix_ns=5, reason=None),
                                   first_started_unix_ns=1, second_started_unix_ns=2,
                                   fence_unix_ns=3,
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


class EvidenceLayout(unittest.TestCase):
    def test_every_cell_file_uses_the_one_campaign_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'M6-w1-c16'
            run = output / 'measure' / 'run'
            run.mkdir(parents=True)
            (output / 'receipt.json').write_text('{"ok": true}')
            (output / 'measure' / 'spec.json').write_text('{}')
            for name in ('manifest.json', 'samples.csv', 'live.prom',
                         'measurement-read-evidence.json'):
                (run / name).write_text('x')
            parts = [dict(name='measure', output=str(run), log=str(output / 'measure.log'))]
            rows = fault_cells.cell_artifacts(output, parts)
            names = {row['name'] for row in rows}
            self.assertEqual(names, {'receipt.json', 'measure/spec.json',
                                     'measure/run/manifest.json', 'measure/run/samples.csv',
                                     'measure/run/live.prom',
                                     'measure/run/measurement-read-evidence.json'})
            evidence = {fault_cells.campaign_manifest.evidence_key(
                'p', 'stage2', 'M6-w1-c16', row['name']) for row in rows}
            self.assertIn('p/stage2/evidence/M6-w1-c16/attempt-1/measure/run/manifest.json',
                          evidence)
            self.assertIn('p/stage2/raw/M6-w1-c16/attempt-1/measure/run/samples.csv',
                          evidence)
            self.assertIn('p/stage2/raw/M6-w1-c16/attempt-1/measure/run/live.prom',
                          evidence)

    def test_a_pass_needs_the_upload_or_the_rehearsal_escape(self):
        argv = ['fault_cells.py', '--cell', 'S1-2S-publication-native',
                '--build', str(ROOT), '--output', '/tmp/mech-out',
                '--guest-root', '/var/tmp/guest', '--coordinator', 'H0',
                '--owners', 'H1,H2', '--zone', 'us-central1-a',
                '--fixture', '/tmp/fixture', '--schema', '/tmp/schema']
        with patch.object(sys, 'argv', argv), self.assertRaises(SystemExit) as raised:
            fault_cells.main()
        self.assertIn('--allow-local-ledger', str(raised.exception))


class ReopenPath(unittest.TestCase):
    """The reopen of a killed coordinator: one launch, on the killed root."""

    def driver(self, root, cell='S1-2S-publication-native'):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.output = Path(root)
        cluster.binary = Path('/bin/true')
        cluster.owners = ['H1', 'H2']
        cluster.remote = None
        cluster.receipts = lambda namespace: []
        cluster.database = lambda: 'live/checkpoints'
        cluster.context = lambda namespace, receipts: dict(
            transport={'kind': 'tcp'}, remote={'read_cache_bytes': 0},
            receipts=list(receipts))
        cluster.local = lambda name: cluster.output / name
        cluster.root = lambda name: cluster.local(name) / 'coordinator'
        cluster.evidence = lambda name: cluster.local(name) / 'evidence'
        return fault_cells.CellDriver(cluster, fault_cells.mechanism.find(cell))

    def test_the_verify_run_reopens_the_killed_root_exactly_once(self):
        class Child:
            def __init__(self, *, binary, spec, directory, name='run', hold_completion=False):
                self.output = Path(directory) / name
                self.output.mkdir(parents=True, exist_ok=False)
                (self.output / 'manifest.json').write_text('{}')
                self.log = Path(directory) / f'{name}.log'
                self.process = type('P', (), dict(pid=4242, returncode=0))()
                self.started_unix_ns = 1

            def wait(self, timeout):
                return 0

            def stop(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            driver = self.driver(tmp)
            with patch.object(fault_cells, 'Coordinator', Child):
                part = driver.after_restart('verify', driver.cell['runs'][1], {})
            spec = json.loads((Path(tmp) / 'verify' / 'spec.json').read_text())
            self.assertEqual(spec['backend']['options']['path'],
                             str(Path(tmp) / 'measure' / 'coordinator'))
            self.assertEqual(part['output'], str(Path(tmp) / 'verify' / 'run'))

    def test_open_serializes_receipt_paths_not_rows(self):
        """The spec carries path strings; a raw row is a map and fails the open."""
        with tempfile.TemporaryDirectory() as tmp:
            driver = self.driver(tmp)
            driver.cluster.receipts = lambda namespace: [
                dict(partition=0, path='/g/receipts/node-0.json', receipt={}),
                dict(partition=1, path='/g/receipts/node-1.json', receipt={})]
            driver.cluster.context = lambda namespace, receipts: dict(
                transport={'kind': 'tcp'}, remote={'read_cache_bytes': 0},
                receipts=[row['path'] if isinstance(row, dict) else str(row)
                          for row in receipts])

            class Child:
                def __init__(self, *, binary, spec, directory, name='run', hold_completion=False):
                    self.output = Path(directory) / name
                    self.output.mkdir(parents=True, exist_ok=True)

            with patch.object(fault_cells, 'Coordinator', Child):
                driver.open(driver.cell['runs'][0], 'measure')
            spec = json.loads((Path(tmp) / 'measure' / 'spec.json').read_text())
        self.assertEqual(
            spec['backend']['options']['external_owner_durability_receipts'],
            ['/g/receipts/node-0.json', '/g/receipts/node-1.json'])

    def test_the_verify_verdict_sees_the_kill_of_the_measured_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            driver = self.driver(tmp)
            gate = dict(reached=True, label='200 acknowledged writes')
            kill = dict(exit_code=-9)
            captured = {}

            def after(name, run, observations):
                captured.update(observations)
                return dict(name=name, role=run['role'], verdict=True,
                            output=str(Path(tmp) / name), log='', pid=1,
                            started_unix_ns=1, finished_unix_ns=2, exit_code=0,
                            observations=observations)

            child = types.SimpleNamespace(
                process=types.SimpleNamespace(pid=1, returncode=-9),
                output=Path(tmp) / 'measure' / 'run',
                log=Path(tmp) / 'measure.log', started_unix_ns=1,
                rows=lambda: [], alive=lambda: True,
                kill=lambda: -9, stop=lambda: None)
            with patch.object(driver, 'open', return_value=child), \
                    patch.object(driver, 'gate_writes', return_value=gate), \
                    patch.object(driver, 'kill_coordinator', return_value=kill), \
                    patch.object(driver, 'after_restart', side_effect=after):
                driver.crash()
            self.assertEqual(captured['gate'], gate)
            self.assertEqual(captured['kill'], kill)


class FailureUpload(unittest.TestCase):
    """A wall-failing cell registers no part, so its logs must still travel."""

    def tree(self, root):
        output = Path(root) / 'S1-2S-publication-native'
        run = output / 'measure' / 'run' / 'experiment'
        run.mkdir(parents=True)
        (output / 'measure' / 'run.log').write_text(
            'nvmet is not on mounted configfs\n')
        (run / 'replay.log').write_text(
            'ProvisionDatabase: nvmet is not on mounted configfs\n')
        (output / 'measure' / 'coordinator').mkdir()
        (output / 'measure' / 'coordinator' / 'db').write_text('private')
        (output / 'measure' / 'tls').mkdir()
        (output / 'measure' / 'tls' / 'key.pem').write_text('private')
        config = Path(root) / 'S1-2S-publication-native-external-config'
        config.mkdir()
        (config / 'config.json').write_text('{}')
        (config / 'host-bench-0.tar.gz').write_text('rebuildable')
        return output, config

    def test_failure_artifacts_take_the_logs_and_skip_private_trees(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, config = self.tree(tmp)
            rows, skipped = fault_cells.failure_artifacts(output, config)
            names = {row['name'] for row in rows}
            self.assertIn('measure/run.log', names)
            self.assertIn('measure/run/experiment/replay.log', names)
            self.assertIn('external/config.json', names)
            self.assertNotIn('measure/coordinator/db', names)
            self.assertNotIn('measure/tls/key.pem', names)
            self.assertNotIn('external/host-bench-0.tar.gz', names)
            self.assertEqual(skipped, [dict(
                path=str(config / 'host-bench-0.tar.gz'),
                reason='rebuildable owner archive')])

    def test_failure_artifacts_bound_one_oversized_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, config = self.tree(tmp)
            (config / 'host-bench-0.tar.gz').unlink()
            big = output / 'measure' / 'huge.log'
            big.write_bytes(b'')
            os.truncate(big, fault_cells.FAILURE_ARTIFACT_LIMIT + 1)
            rows, skipped = fault_cells.failure_artifacts(output, config)
            self.assertNotIn('measure/huge.log', {row['name'] for row in rows})
            self.assertEqual(skipped, [dict(path=str(big),
                                            reason='above the failure upload bound')])

    def test_an_oversized_sample_file_still_ships_a_bounded_excerpt(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'out'
            run = output / 'measure' / 'run'
            run.mkdir(parents=True)
            samples = run / 'samples.csv'
            samples.write_text(sample_rows(4) +
                               sample_rows(3, outcome='unknown_mutation').split('\n', 1)[1])
            with patch.object(fault_cells, 'FAILURE_ARTIFACT_LIMIT', 64):
                rows, skipped = fault_cells.failure_artifacts(output, Path(tmp) / 'cfg')
            names = {row['name'] for row in rows}
            self.assertIn('measure/run/samples.excerpt.csv', names)
            excerpt = (run / 'samples.excerpt.csv').read_text()
            self.assertIn('unknown_mutation', excerpt)
            self.assertIn(dict(path=str(samples),
                               reason='above the failure upload bound'), skipped)

    def test_a_failed_attempt_uploads_evidence_and_a_failed_ledger(self):
        from test_gcs_ledger import FakeObjectStore
        with tempfile.TemporaryDirectory() as tmp:
            output, config = self.tree(tmp)
            rows, skipped = fault_cells.failure_artifacts(output, config)
            ledger = fault_cells.ledger_module.Ledger(Path(tmp) / 'campaign-ledger.json')
            entry = ledger.begin(stage='stage1', cell_id='S1-2S-publication-native',
                                 config_hash='hash', unit='S1-2S-publication-native')
            entry['failure_skipped'] = skipped
            store = FakeObjectStore()
            fault_cells.campaign_upload.Evidence(
                store, 'campaign-hybrid-sui-2026-11-25').persist_failure(
                    entry, rows, RuntimeError('wall'))
            keys = {row['key'] for row in entry['failure_evidence']['uploaded']}
            base = ('campaign-hybrid-sui-2026-11-25/stage1/evidence/'
                    'S1-2S-publication-native/attempt-1/')
            self.assertIn(base + 'measure/run.log', keys)
            self.assertIn(base + 'measure/run/experiment/replay.log', keys)
            payload = store.objects[
                'campaign-hybrid-sui-2026-11-25/campaign-ledger.json'][0]
            stored = fault_cells.ledger_module.Ledger.from_bytes(payload)
            self.assertEqual(stored.status('stage1', 'S1-2S-publication-native'),
                             'failed')
            self.assertIn('wall', stored.entries[
                ('stage1', 'S1-2S-publication-native')]['failure'])


class UploadedPass(unittest.TestCase):
    def cell_tree(self, root):
        output = Path(root) / 'M6-w1-c16'
        run = output / 'measure' / 'run'
        run.mkdir(parents=True)
        (output / 'receipt.json').write_text('{"ok": true}')
        (output / 'measure' / 'spec.json').write_text('{}')
        for name in ('manifest.json', 'summary.json', 'samples.csv',
                     'measurement-read-evidence.json', 'live.prom'):
            (run / name).write_text(name)
        parts = [dict(name='measure', output=str(run), log=str(output / 'measure.log'))]
        return output, fault_cells.cell_artifacts(output, parts)

    def entry(self, root):
        ledger = fault_cells.ledger_module.Ledger(Path(root) / 'campaign-ledger.json')
        entry = ledger.begin(stage='stage2', cell_id='M6-w1-c16',
                             config_hash='hash', unit='M6-w1-c16')
        ledger.save()
        return ledger, entry

    def test_a_cell_passes_only_after_every_object_verifies(self):
        from test_gcs_ledger import FakeObjectStore
        with tempfile.TemporaryDirectory() as tmp:
            output, artifacts = self.cell_tree(tmp)
            ledger, entry = self.entry(tmp)
            store = FakeObjectStore()
            receipt = fault_cells.campaign_upload.Evidence(
                store, 'campaign-hybrid-sui-2026-11-25').persist(
                    ledger, entry, artifacts)
            self.assertEqual(ledger.status('stage2', 'M6-w1-c16'), 'passed')
            keys = {row['key'] for row in receipt['evidence']}
            self.assertIn('campaign-hybrid-sui-2026-11-25/stage2/evidence/M6-w1-c16/'
                          'attempt-1/receipt.json', keys)
            self.assertIn('campaign-hybrid-sui-2026-11-25/stage2/raw/M6-w1-c16/'
                          'attempt-1/measure/run/samples.csv', keys)
            stored = ledger.to_bytes()
            self.assertEqual(store.objects[
                'campaign-hybrid-sui-2026-11-25/campaign-ledger.json'][0], stored)

    def test_a_corrupt_upload_never_passes_the_cell(self):
        from test_gcs_ledger import FakeObjectStore
        with tempfile.TemporaryDirectory() as tmp:
            output, artifacts = self.cell_tree(tmp)
            ledger, entry = self.entry(tmp)
            store = FakeObjectStore()
            store.receipt_size_delta = 1
            with self.assertRaises(ValueError):
                fault_cells.campaign_upload.Evidence(
                    store, 'campaign-hybrid-sui-2026-11-25').persist(
                        ledger, entry, artifacts)
            self.assertEqual(ledger.status('stage2', 'M6-w1-c16'), 'running')


class LedgerClose(unittest.TestCase):
    """F2/F13: a clean cell closes its ledger entry; the exit code is returned."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.build = self.base / 'build'
        self.build.mkdir()
        (self.build / 'build-manifest.json').write_text(json.dumps(dict(
            protocol=9, source_archive_sha256='a' * 64,
            binaries={'storage-node': '0' * 64})))
        self.fixture = self.base / 'fixture'
        (self.fixture / 'experiment').mkdir(parents=True)
        (self.fixture / 'experiment' / 'results.json').write_text('{}')
        self.schema = self.base / 'schema'
        self.schema.mkdir()
        (self.schema / 'shape.yaml').write_text('key_spaces: []\n')
        self.output = self.base / 'out' / 'M6-w1-c16'
        self.ledger = self.base / 'out' / 'ledger.json'

    def argv(self, *extra):
        return ['fault_cells.py', '--cell', 'M6-w1-c16',
                '--build', str(self.build), '--fixture', str(self.fixture),
                '--schema', str(self.schema), '--output', str(self.output),
                '--guest-root', '/mnt/tidehunter/guest', '--coordinator', 'H0',
                '--owners', 'H1,H2', '--ledger', str(self.ledger),
                '--allow-local-ledger', '--on-coordinator', *extra]

    def run_main(self, *, verdict=True, failure=None, extra=()):
        created = []

        class Cluster:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.failure = failure
                self.stops = []
                created.append(self)

            def admit(self):
                pass

            def prepare(self):
                if self.failure is not None:
                    raise self.failure

            def bind(self, namespace):
                self.namespace = namespace

            def install(self):
                pass

            def start(self):
                pass

            def stop(self):
                pass

            def database(self):
                return 'live/checkpoints'

        class Driver:
            def __init__(self, cluster, cell):
                self.cluster, self.cell = cluster, cell

            def measure(self):
                run = self.cell['runs'][0]
                out = self.cluster.output / 'measure' / 'run'
                out.mkdir(parents=True)
                (out / 'manifest.json').write_text(json.dumps(dict(
                    status='completed', errors_or_wrong_values=0,
                    completed_operations=1)))
                (out / 'summary.json').write_text(json.dumps(dict(
                    phase_counts=dict(realized_mix={}))))
                (out / 'samples.csv').write_text(
                    'phase,operation,outcome,api_ns,completed_ns,bytes\n'
                    'measurement,put,success,1,1,4096\n')
                return [dict(name='measure', role=run['role'], run=run, verdict=True,
                             output=str(out), log='', pid=1, started_unix_ns=1,
                             finished_unix_ns=2, exit_code=0, observations={})]

        with patch.object(fault_cells, 'Cluster', Cluster), \
                patch.object(fault_cells, 'CellDriver', Driver), \
                patch.object(fault_cells, 'verdict_parts',
                             lambda driver, parts: [dict(ok=verdict, checks=[])]), \
                patch.object(fault_cells.mailbox_exec, 'open_executor',
                             return_value=None), \
                patch.object(sys, 'argv', self.argv(*extra)), \
                redirect_stdout(io.StringIO()):
            try:
                return fault_cells.main(), created
            except Exception as error:
                return error, created

    def status(self):
        return (fault_cells.ledger_module.Ledger(self.ledger).load()
                .status('stage2', 'M6-w1-c16'))

    def test_a_clean_cell_passes_and_returns_zero(self):
        code, created = self.run_main()
        self.assertEqual(code, 0)
        self.assertEqual(self.status(), 'passed')
        self.assertEqual(created[0].output, self.output)
        self.assertTrue((self.output / 'receipt.json').is_file())

    def test_a_failed_verdict_records_failed_and_returns_one(self):
        code, _ = self.run_main(verdict=False)
        self.assertEqual(code, 1)
        self.assertEqual(self.status(), 'failed')

    def test_a_raised_work_error_records_failed_and_is_reraised(self):
        error, _ = self.run_main(failure=fault_cells.CellError('gate missed'))
        self.assertIsInstance(error, fault_cells.CellError)
        self.assertEqual(self.status(), 'failed')
        entry = fault_cells.ledger_module.Ledger(self.ledger).load().entries[
            ('stage2', 'M6-w1-c16')]
        self.assertEqual(entry['attempts'], 1)
        self.assertIn('gate missed', entry['failure'])

    def test_a_retry_writes_a_fresh_output_and_config_dir(self):
        ledger = fault_cells.ledger_module.Ledger(self.ledger)
        entry = ledger.begin(stage='stage2', cell_id='M6-w1-c16',
                             config_hash='stale', unit='M6-w1-c16')
        ledger.fail(entry, 'previous attempt')
        ledger.save()
        code, created = self.run_main(extra=('--resume',))
        self.assertEqual(code, 0)
        self.assertEqual(created[0].output,
                         self.output.with_name(self.output.name + '-attempt-2'))
        self.assertEqual(created[0].config_dir.name,
                         self.output.name + '-external-attempt-2')
        self.assertTrue((created[0].output / 'receipt.json').is_file())
        self.assertEqual(self.status(), 'passed')


class StaleFenceMeasurement(unittest.TestCase):
    """F3: the stale case measures its fence latency, like the lapse case."""

    class Child:
        def __init__(self, output):
            self.output = Path(output)
            self.signals = []
            self.started_unix_ns = 1_000_000_000

        def signal(self, value):
            self.signals.append(value)

        def stop(self):
            pass

        def rows(self):
            return [dict(phase='measurement', outcome='success', error_kind='',
                         completed_ns='0'),
                    dict(phase='measurement', outcome='success', error_kind='',
                         completed_ns='1'),
                    dict(phase='measurement', outcome='unknown_mutation',
                         error_kind='coordinator lease lapsed',
                         completed_ns='50000000')]

    class Second:
        def __init__(self, started_unix_ns=1_000_000_000):
            self.stopped = False
            self.started_unix_ns = started_unix_ns
            self.output = Path('/nonexistent')

        def rows(self):
            return [dict(phase='measurement', outcome='success', error_kind='',
                         completed_ns='10000000')]

        def stop(self):
            self.stopped = True

    def test_the_stale_case_records_the_resume_to_refusal_latency(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'fence-a'
            (root / 'coordinator').mkdir(parents=True)
            child = self.Child(root / 'run')
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.output = Path(tmp)
            cluster.owners = ['H1', 'H2']
            cluster.local = lambda name: cluster.output / name
            cluster.root = lambda name: cluster.local(name) / 'coordinator'
            seals = dict(n=0)
            def marker(host):
                seals['n'] += 1
                return dict(sealed=0 if seals['n'] == 1 else 1)
            cluster.seal_markers = marker
            cluster.generation_wal = lambda host: dict(generations={
                '/db-1/epoch_3': {'/db-1/epoch_3/wal_0000000000000000':
                                  dict(size=4096, mtime=1.0)}})
            driver = fault_cells.CellDriver(
                cluster, fault_cells.mechanism.find('S1-5S-stale-native'))
            second = self.Second()
            gate = dict(reached=True, errors=1, unix_ns=1_050_000_000)
            with patch.object(fault_cells, 'now_ns', return_value=1_000_000_000), \
                    patch.object(driver, 'launch', return_value=second), \
                    patch.object(driver, 'gate_writes', return_value=dict(reached=True)), \
                    patch.object(driver, 'stop_on_failure',
                                 return_value=dict(gate=gate, exit_code=1)):
                with patch.object(driver, 'fence_audit', return_value=dict(
                        audit_available=True, audit_integrity_ok=True,
                     owner_fence=dict(evidence_available=True,
                                      fence_counters_increased=True,
                                      no_stale_growth=True,
                                      stale_generations=['h0:/db-1/epoch_3'],
                                      stale_growth=dict(new_files=[], grown=[]),
                                      scraped_unix_ns=5, reason=None),
                        audit_read_ms=1.0, audit_text='verify ok')):
                    result = driver._contend(child, 'stale')
        self.assertEqual(result['resumed_unix_ns'], 1_000_000_000)
        # The fence is the new writer's first completed write at +10 ms on its
        # own clock; the stale writer's refusal completed at +50 ms.
        self.assertEqual(result['fence_unix_ns'], 1_010_000_000)
        self.assertEqual(result['fence_reject_ms'], 40.0)
        self.assertEqual(result['refusal_kind'], 'owner-row')
        self.assertEqual(result['stale_success_after_fence'], 0)
        self.assertTrue(result['stale_rows_exclusive'], result['stale_rows_reason'])
        self.assertTrue(result['stale_write_refused'])
        self.assertTrue(result['typed_refusal'])
        self.assertTrue(second.stopped)
        fence = dict(result, case='stale', tier='native', owner_fence=dict(
            evidence_available=True, fence_counters_increased=True,
            no_stale_growth=True, stale_generations=['h0:/db-1/epoch_3'],
            stale_growth=dict(new_files=[], grown=[]), scraped_unix_ns=5, reason=None))
        record = fault_cells.fence_record(
            types.SimpleNamespace(cell=driver.cell, parts=[dict(role='verify')],
                                  fence=fence))
        self.assertTrue(record['ok'], record)
        latency = [check for check in record['checks']
                   if check['check'] == 'fence latency measured or bounded'][0]
        self.assertEqual(latency['detail']['kind'], 'owner-row')
        self.assertEqual(latency['detail']['exact_ms'], 40.0)

    def test_a_refusal_is_the_first_non_success_measurement_row(self):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        rows_ = [dict(phase='preload', outcome='success', error_kind=''),
                 dict(phase='measurement', outcome='success', error_kind=''),
                 dict(phase='measurement', outcome='unknown_mutation',
                      error_kind='backend_transport_failure; mutation outcome '
                                 'unknown: coordinator lease lapsed'),
                 dict(phase='measurement', outcome='unknown_mutation',
                      error_kind='coordinator lease lapsed')]
        refusal = driver._refusal(rows_)
        self.assertEqual(refusal['first'], 2)
        self.assertEqual(refusal['last_success'], 1)
        self.assertTrue(refusal['typed'])
        self.assertTrue(refusal['no_success_after'])

    def test_an_untyped_row_takes_the_causal_line_from_the_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / 'run.log'
            log.write_text('kv-research: measurement end boundary failed: write core '
                           'requires recovery; first causal failure: owner=Some(0) '
                           'stage=live_group: coordinator lease lapsed on owner '
                           'partition 0\n')
            driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
            child = types.SimpleNamespace(log=log)
            rows_ = [dict(phase='measurement', outcome='unknown_mutation',
                          error_kind='Connection refused', completed_ns='5')]
            refusal = driver._refusal(rows_)
            self.assertFalse(refusal['typed'])
            logged = driver._log_refusal(child)
        self.assertIn('coordinator lease lapsed', logged)

    def test_a_success_after_a_refusal_is_a_second_writer(self):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        rows_ = [dict(phase='measurement', outcome='unknown_mutation',
                      error_kind='coordinator lease lapsed'),
                 dict(phase='measurement', outcome='success', error_kind='')]
        refusal = driver._refusal(rows_)
        self.assertFalse(refusal['no_success_after'])

    def test_a_contend_cell_reports_the_fence_latency_with_its_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'fence-a'
            (root / 'coordinator').mkdir(parents=True)
            child = self.Child(root / 'run')
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.output = Path(tmp)
            cluster.owners = ['H1', 'H2']
            cluster.local = lambda name: cluster.output / name
            cluster.root = lambda name: cluster.local(name) / 'coordinator'
            seals = dict(n=0)
            def marker(host):
                seals['n'] += 1
                return dict(sealed=0 if seals['n'] == 1 else 1)
            cluster.seal_markers = marker
            cluster.generation_wal = lambda host: dict(generations={
                '/db-1/epoch_3': {'/db-1/epoch_3/wal_0000000000000000':
                                  dict(size=4096, mtime=1.0)}})
            driver = fault_cells.CellDriver(
                cluster, fault_cells.mechanism.find('S1-5S-contend-native'))
            with patch.object(fault_cells, 'now_ns', return_value=1_000_000_000), \
                    patch.object(driver, 'launch', return_value=self.Second()), \
                    patch.object(driver, 'gate_writes', return_value=dict(reached=True)), \
                    patch.object(driver, 'stop_on_failure',
                                 return_value=dict(gate=dict(reached=True, errors=1),
                                                   exit_code=1)):
                with patch.object(driver, 'fence_audit', return_value=dict(
                        audit_available=True, audit_integrity_ok=True,
                     owner_fence=dict(evidence_available=True,
                                      fence_counters_increased=True,
                                      no_stale_growth=True,
                                      stale_generations=['h0:/db-1/epoch_3'],
                                      stale_growth=dict(new_files=[], grown=[]),
                                      scraped_unix_ns=5, reason=None),
                        audit_read_ms=1.0, audit_text='verify ok')):
                    result = driver._contend(child, 'contend')
        # A contend cell reports the same quantity as a stale cell, and says
        # which kind of refusal it measured.
        self.assertEqual(result['fence_unix_ns'], 1_010_000_000)
        self.assertEqual(result['fence_reject_ms'], 40.0)
        self.assertEqual(result['refusal_kind'], 'owner-row')
        self.assertTrue(result['stale_rows_exclusive'], result['stale_rows_reason'])
        fence = dict(result, case='contend', tier='native', owner_fence=dict(
            evidence_available=True, fence_counters_increased=True,
            no_stale_growth=True, stale_generations=['h0:/db-1/epoch_3'],
            stale_growth=dict(new_files=[], grown=[]), scraped_unix_ns=5, reason=None))
        record = fault_cells.fence_record(
            types.SimpleNamespace(cell=driver.cell, parts=[dict(role='verify')],
                                  fence=fence))
        self.assertTrue(record['ok'], record)
        latency = [check for check in record['checks']
                   if check['check'] == 'fence latency measured or bounded'][0]
        self.assertEqual(latency['detail']['kind'], 'owner-row')


class FenceAuditRun(unittest.TestCase):
    """The audit the fence verdict requires is a kv-research verify run.

    `fixtures/s1-5s-contend-attempt3.json` carries the real cell's numbers so the
    audit is asserted beside the evidence it must accompany.
    """

    def driver(self, tmp, *, exit_code=0, wrong=0, fail=None):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        driver.cell = fault_cells.mechanism.find('S1-5S-contend-native')
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.output = Path(tmp)
        cluster.local = lambda name: cluster.output / name
        cluster.root = lambda name: cluster.local(name) / 'coordinator'
        driver.cluster = cluster

        class Reader:
            output = Path(tmp) / 'fence-verify' / 'run'

            def wait(self, timeout):
                return exit_code

            def stop(self):
                pass

        Reader.output.mkdir(parents=True, exist_ok=True)
        (Reader.output / 'manifest.json').write_text(
            json.dumps(dict(errors_or_wrong_values=wrong)))
        driver.launch = (lambda run, name, path=None: (_ for _ in ()).throw(ValueError(fail))
                         if fail else Reader())
        return driver

    def test_a_clean_verify_run_is_the_audit_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = self.driver(tmp).fence_audit(
                types.SimpleNamespace(output=Path(tmp) / 'fence-a' / 'run'))
        self.assertTrue(audit['audit_available'])
        self.assertTrue(audit['audit_integrity_ok'])
        self.assertNotIn('audit_stale_writes_after_fence', audit,
                         'a deterministic readback is not adoption evidence')
        self.assertIn('verify_existing_dataset exit 0', audit['audit_text'])
        self.assertIsNotNone(audit['audit_read_ms'])

    def test_a_wrong_value_or_a_failed_read_is_a_dirty_audit(self):
        for kwargs in (dict(wrong=2), dict(exit_code=1)):
            with self.subTest(**kwargs), tempfile.TemporaryDirectory() as tmp:
                audit = self.driver(tmp, **kwargs).fence_audit(
                    types.SimpleNamespace(output=Path(tmp) / 'fence-a' / 'run'))
            self.assertTrue(audit['audit_available'])
            self.assertFalse(audit['audit_integrity_ok'])

    def test_an_unopenable_audit_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = self.driver(tmp, fail='no dataset at that root').fence_audit(
                types.SimpleNamespace(output=Path(tmp) / 'fence-a' / 'run'))
        self.assertFalse(audit['audit_available'])
        self.assertIsNone(audit['audit_integrity_ok'])
        self.assertIn('did not open', audit['audit_text'])


class AttemptPaths(unittest.TestCase):
    """F20: a retry never reuses the previous attempt's tree."""

    def test_the_first_attempt_keeps_the_requested_paths(self):
        output, config = Path('/c/out/S1-5S-stale-native'), Path('/c/out/x-external')
        self.assertEqual(fault_cells.attempt_paths(output, config, 1), (output, config))

    def test_a_retry_appends_the_attempt_to_both_paths(self):
        output, config = Path('/c/out/S1-5S-stale-native'), Path('/c/out/x-external')
        self.assertEqual(fault_cells.attempt_paths(output, config, 3),
                         (Path('/c/out/S1-5S-stale-native-attempt-3'),
                          Path('/c/out/x-external-attempt-3')))


class ZoneBinding(unittest.TestCase):
    """F1: --zone is an SSH hint, never a mailbox-mode requirement."""

    def test_the_zone_comes_from_the_fleet_state_when_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / 'fleet.json'
            state.write_text(json.dumps({'fleet': [dict(index=0, name='H0',
                                                       zone='us-central1-a')]}))
            self.assertEqual(fault_cells.fleet_zone(state), 'us-central1-a')
            state.write_text(json.dumps({'instances': [
                dict(name='H0', fleet=dict(index=0, zone='us-central1-b'))]}))
            self.assertEqual(fault_cells.fleet_zone(state), 'us-central1-b')
            state.write_text(json.dumps({'instances': []}))
            self.assertIsNone(fault_cells.fleet_zone(state))

    def test_a_dry_run_without_a_zone_parses(self):
        argv = ['fault_cells.py', '--cell', 'S1-5S-stale-native',
                '--build', str(ROOT), '--output', '/tmp/mech-out',
                '--guest-root', '/var/tmp/guest', '--coordinator', 'H0',
                '--owners', 'H1,H2', '--dry-run']
        stream = io.StringIO()
        with patch.object(sys, 'argv', argv), redirect_stdout(stream):
            self.assertEqual(fault_cells.main(), 0)
        self.assertIsNone(json.loads(stream.getvalue())['zone'])


class CellList(unittest.TestCase):
    """The operator list is the code's cell table, split by stage."""

    def listed(self, argv):
        stream = io.StringIO()
        with patch.object(sys, 'argv', argv), redirect_stdout(stream):
            self.assertEqual(fault_cells.main(), 0)
        return json.loads(stream.getvalue())

    def test_stage_one_lists_the_eighteen_reviewed_fault_cells(self):
        value = self.listed(['fault_cells.py', '--list', 'stage1'])
        self.assertEqual(value['count'], 18)
        self.assertEqual(
            {family: sum(cell['family'] == family for cell in value['cells'])
             for family in ('S1-2', 'S1-3', 'S1-5', 'S1-6')},
            {'S1-2': 4, 'S1-3': 2, 'S1-5': 8, 'S1-6': 4})
        self.assertTrue(all(cell['stage'] == 'stage1' for cell in value['cells']))

    def test_stage_two_lists_only_its_mechanism_cells(self):
        value = self.listed(['fault_cells.py', '--list=stage2'])
        self.assertEqual(value['count'], 12)
        self.assertEqual({cell['family'] for cell in value['cells']},
                         {'M6', 'M9', 'M13'})
        self.assertNotIn('M8', {cell['id'].split('-')[0] for cell in value['cells']})


class OwnerStopGuard(unittest.TestCase):
    """A guest root is never removed under a live owner of an earlier cell."""

    class Remote:
        def __init__(self, *, alive, survive=False):
            self.alive, self.survive = set(alive), survive
            self.calls = []

        def shell(self, host, command, *, stdin=None, check=True):
            self.calls.append((host, command, stdin))
            if command.startswith("sh -c 'for f in"):
                return 0, ''.join(f'{pid} /guest/owner-{pid}.pid\n'
                                  for pid in sorted(self.alive)).encode(), b''
            if command.startswith('kill -TERM'):
                if not self.survive:
                    self.alive.clear()
                return 0, b'', b''
            if command.startswith('kill -KILL'):
                if not self.survive:
                    self.alive.clear()
                return 0, b'', b''
            return 0, b'', b''

        def exists(self, host, path):
            match = re.fullmatch(r'/proc/([0-9]+)', str(path))
            if match:
                return int(match.group(1)) in self.alive
            return False

        def read_bytes(self, host, path):
            if str(path).endswith('/cmdline'):
                return b'/guest/storage-node\0--experimental-storage-node\0'
            raise fault_cells.CellError(f'{host}: missing {path}')

    def cluster(self, remote):
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.remote, cluster.guest_root = remote, Path('/guest')
        cluster.owners, cluster.stops = ['h1', 'h2'], []
        return cluster

    def test_a_live_previous_owner_is_stopped_before_the_root_is_removed(self):
        remote = self.Remote(alive=[11])
        cluster = self.cluster(remote)
        with patch.object(fault_cells, 'OWNER_STOP_GRACE_SECONDS', 0.05), \
                patch.object(fault_cells.time, 'sleep', lambda _: None):
            records = cluster._stop_previous_owners('h1')
        self.assertFalse(records[0]['forced'])
        self.assertFalse(records[0]['still_alive'])
        self.assertEqual(records[0]['grace_seconds'], 0.05)
        self.assertTrue(any(call[1].startswith('kill -TERM -11')
                            for call in remote.calls))
        self.assertEqual(cluster.stops, records)

    def test_an_owner_that_survives_the_grace_fails_the_cell(self):
        remote = self.Remote(alive=[11], survive=True)
        cluster = self.cluster(remote)
        with patch.object(fault_cells, 'OWNER_STOP_GRACE_SECONDS', 0.05), \
                patch.object(fault_cells.time, 'sleep', lambda _: None):
            with self.assertRaisesRegex(fault_cells.CellError, 'still alive'):
                cluster._stop_previous_owners('h1')
        self.assertTrue(any(call[1].startswith('kill -KILL -11')
                            for call in remote.calls))
        self.assertTrue(cluster.stops[0]['forced'])

    def test_a_recycled_pid_is_not_an_owner(self):
        class Recycled(self.Remote):
            def read_bytes(self, host, path):
                if str(path).endswith('/cmdline'):
                    return b'python3\0train.py\0'
                raise AssertionError(path)
        cluster = self.cluster(Recycled(alive=[11]))
        self.assertEqual(cluster.live_previous_owners('h1'), [])


class StopVerdict(unittest.TestCase):
    def cell(self):
        return fault_cells.mechanism.find('M6-w1-c16')

    def test_a_clean_coordinator_stop_passes(self):
        record = fault_cells.stop_verdict(self.cell(), [
            dict(process='coordinator', part='measure', forced=False,
                 still_alive=False, exit_code=0, duration_seconds=0.2)])
        self.assertTrue(record['ok'], record)
        self.assertEqual(record['role'], 'stop')

    def test_a_forced_kill_is_never_a_clean_stop(self):
        record = fault_cells.stop_verdict(self.cell(), [
            dict(process='coordinator', part='measure', forced=True,
                 still_alive=False, exit_code=-9, duration_seconds=30.1)])
        self.assertFalse(record['ok'])
        self.assertFalse(record['checks'][0]['ok'])

    def test_an_owner_stop_is_part_of_the_cell_verdict(self):
        driver = fault_cells.CellDriver.__new__(fault_cells.CellDriver)
        driver.cluster = types.SimpleNamespace(stops=[
            dict(host='h1', partition=0, pid=1, signal='SIGTERM',
                 grace_seconds=90, duration_seconds=1.0, forced=False,
                 still_alive=False, exit_code=None)])
        parts = [dict(name='measure', stop=dict(
            process='coordinator', signal=None, grace_seconds=30,
            duration_seconds=0.1, forced=False, still_alive=False, exit_code=0))]
        records = fault_cells.stop_records(driver, parts)
        self.assertEqual([row.get('process') for row in records],
                         ['coordinator', None])
        self.assertEqual([row['part'] for row in records], ['measure', 'owners'])
        self.assertTrue(fault_cells.stop_verdict(self.cell(), records)['ok'])


class Preflight(unittest.TestCase):
    def tree(self, root, *, header=None, manifest=None):
        out = Path(root)
        out.mkdir(parents=True, exist_ok=True)
        (out / 'samples.csv').write_text(
            (header or 'phase,operation,outcome,api_ns,completed_ns,bytes')
            + '\nmeasurement,put,success,1,1,4096\n')
        (out / 'manifest.json').write_text(json.dumps(
            manifest or dict(status='completed', errors_or_wrong_values=0,
                             completed_operations=1)))
        (out / 'summary.json').write_text(json.dumps(
            dict(phase_counts=dict(realized_mix={}))))
        return out

    def test_a_complete_run_has_no_problems(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(fault_cells.mechanism.preflight(self.tree(tmp)), [])

    def test_a_renamed_column_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self.tree(tmp, header='phase,operation,result,api_ns,'
                                        'completed_ns,bytes')
            problems = fault_cells.mechanism.preflight(out)
        self.assertEqual(problems, ['samples.csv lacks column(s): outcome'])

    def test_a_renamed_manifest_key_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self.tree(tmp, manifest=dict(status='completed',
                                               errors_or_wrong_values=0))
            with self.assertRaisesRegex(fault_cells.CellError,
                                        'manifest.json lacks key\\(s\\): '
                                        'completed_operations'):
                fault_cells.preflight(out)

    def test_the_gate_counter_and_the_verdict_share_one_parser(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self.tree(tmp)
            parsed = fault_cells.rows(out / 'samples.csv')
        self.assertEqual(fault_cells.acknowledged(parsed), 1)
        self.assertEqual(fault_cells.failures(parsed), [])


class MeasurementGates(unittest.TestCase):
    """The mechanism RunConfig declares the engine's own handshake."""

    def test_every_run_declares_both_gate_pairs(self):
        cell = fault_cells.mechanism.find('M6-w1-c16')
        spec = fault_cells.mechanism.spec(
            cell, cell['runs'][0], {'transport': {'kind': 'tcp'},
                                    'remote': {'read_cache_bytes': 0},
                                    'receipts': []},
            path='/c', evidence='/e/evidence')
        self.assertEqual(spec['measurement_gate']['ready_file'],
                         '/e/evidence/measurement.ready')
        self.assertEqual(spec['measurement_gate']['release_file'],
                         '/e/evidence/measurement.release')
        self.assertEqual(spec['measurement_completion_gate']['ready_file'],
                         '/e/evidence/measurement-complete.ready')
        self.assertNotEqual(spec['measurement_gate']['release_file'],
                            spec['measurement_completion_gate']['release_file'])

    def test_the_driver_answers_ready_with_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = Path(tmp) / 'spec.json'
            spec.write_text(json.dumps(dict(
                measurement_gate=dict(ready_file=str(Path(tmp) / 'm.ready'),
                                      release_file=str(Path(tmp) / 'm.release')),
                measurement_completion_gate=dict(
                    ready_file=str(Path(tmp) / 'c.ready'),
                    release_file=str(Path(tmp) / 'c.release')))))
            gates = fault_cells.mechanism.LocalGates(spec)
            (Path(tmp) / 'm.release').write_text('stale')
            gates.prepare()
            self.assertFalse((Path(tmp) / 'm.release').exists())
            (Path(tmp) / 'm.ready').write_text('ready')
            gates.serve()
            self.assertTrue((Path(tmp) / 'm.release').exists())
            self.assertFalse((Path(tmp) / 'c.release').exists())


class OwnerCellStop(unittest.TestCase):
    """The engine's stop marker is the owner's first close path."""

    class Remote:
        def __init__(self):
            self.alive = {11}
            self.calls = []

        def shell(self, host, command, *, stdin=None, check=True):
            self.calls.append((host, command, stdin))
            if command.startswith('touch /guest/stop'):
                self.alive.clear()
            return 0, b'', b''

        def exists(self, host, path):
            return str(path) == '/proc/11' and 11 in self.alive

    def test_a_marker_close_records_no_signal_and_a_duration(self):
        remote = self.Remote()
        cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
        cluster.remote, cluster.owners, cluster.stops = remote, ['h1'], []
        cluster.cleanup_root, cluster.cleanups = None, []
        cluster.output = Path(tempfile.mkdtemp()) / 'out'
        cluster.namespaces = {'h1': [dict(partition=0, pid=11, process_group=11,
                                          stopped=False, stop='/guest/stop')]}
        records = cluster.stop()
        self.assertEqual(records[0]['signal'], 'stop-file')
        self.assertFalse(records[0]['forced'])
        self.assertFalse(records[0]['still_alive'])
        self.assertGreaterEqual(records[0]['duration_seconds'], 0.0)
        self.assertFalse(any(call[1].startswith('kill -')
                             for call in remote.calls))
        self.assertEqual(cluster.stops, records)
        # A failed cell raises before its verdict; the stop record is already
        # on disk next to the cleanup, so its failure upload carries both.
        persisted = json.loads((cluster.output / 'owner-stops.json').read_text())
        self.assertEqual(persisted, records)
        self.assertTrue((cluster.output / 'owner-cleanup.json').is_file())
