"""Operator driver tests: parsing, gates, receipts and the dry-run CLI."""
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
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
            def __init__(self, *, binary, spec, directory, name='run'):
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
                (out / 'manifest.json').write_text('{}')
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

        def signal(self, value):
            self.signals.append(value)

        def stop(self):
            pass

        def rows(self):
            return [dict(phase='measurement', outcome='success', error_kind=''),
                    dict(phase='measurement', outcome='success', error_kind=''),
                    dict(phase='measurement', outcome='error',
                         error_kind='coordinator lease lapsed')]

    class Second:
        def __init__(self):
            self.stopped = False

        def rows(self):
            return [dict(phase='measurement', outcome='success', error_kind='')]

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
            driver = fault_cells.CellDriver(
                cluster, fault_cells.mechanism.find('S1-5S-stale-native'))
            second = self.Second()
            gate = dict(reached=True, errors=1, unix_ns=1_050_000_000)
            with patch.object(fault_cells, 'now_ns', return_value=1_000_000_000), \
                    patch.object(driver, 'launch', return_value=second), \
                    patch.object(driver, 'gate_writes', return_value=dict(reached=True)), \
                    patch.object(driver, 'stop_on_failure',
                                 return_value=dict(gate=gate, exit_code=1)):
                result = driver._contend(child, 'stale')
        self.assertEqual(result['resumed_unix_ns'], 1_000_000_000)
        self.assertEqual(result['fence_reject_ms'], 50.0)
        self.assertTrue(result['stale_write_refused'])
        self.assertTrue(result['typed_refusal'])
        self.assertEqual(result['writers'], 1)
        self.assertTrue(second.stopped)
        record = fault_cells.fence_record(
            types.SimpleNamespace(cell=driver.cell,
                                  fence=dict(result, case='stale', tier='native')))
        self.assertTrue(record['ok'], record)

    def test_a_contend_cell_is_not_a_stale_latency_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'fence-a'
            (root / 'coordinator').mkdir(parents=True)
            child = self.Child(root / 'run')
            cluster = fault_cells.Cluster.__new__(fault_cells.Cluster)
            cluster.output = Path(tmp)
            cluster.owners = ['H1', 'H2']
            cluster.local = lambda name: cluster.output / name
            cluster.root = lambda name: cluster.local(name) / 'coordinator'
            driver = fault_cells.CellDriver(
                cluster, fault_cells.mechanism.find('S1-5S-contend-native'))
            with patch.object(fault_cells, 'now_ns', return_value=1_000_000_000), \
                    patch.object(driver, 'launch', return_value=self.Second()), \
                    patch.object(driver, 'gate_writes', return_value=dict(reached=True)), \
                    patch.object(driver, 'stop_on_failure',
                                 return_value=dict(gate=dict(reached=True, errors=1),
                                                   exit_code=1)):
                result = driver._contend(child, 'contend')
        self.assertNotIn('fence_reject_ms', result)


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
