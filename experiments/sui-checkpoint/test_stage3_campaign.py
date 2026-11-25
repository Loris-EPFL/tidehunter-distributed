"""Stage-3 driver: local arm, host arm, owner lifecycle, upload-before-pass."""
import csv
import json
import os
from pathlib import Path
import re
import shlex
import socket
import stat
import sys
import tempfile
import threading
import textwrap
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'gcp-rdma'))
import campaign_ledger as ledger_module
import gcs_ledger
import manual_sui
import stage3_campaign
import stage3_specs
from gcp_api import ApiError, ObjectStore
from test_stage3_specs import preparation


FAKE = textwrap.dedent("""\
    #!/usr/bin/env python3
    import argparse, json, os
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument('--config')
    parser.add_argument('--output')
    parser.add_argument('--validate-config', action='store_true')
    args = parser.parse_args()
    case = Path(args.config).stem
    log = Path(os.environ['CALL_LOG'])
    with log.open('a') as stream:
        stream.write(case + '\\n')
    if case in os.environ.get('FAIL_CASES', '').split(','):
        raise SystemExit(2)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'summary.json').write_text(json.dumps({'case': case, 'complete': True}))
    rows = ['phase,completed_ns,bytes', 'preload,0,8192']
    rows += [f'measurement,{index * 1000000},8192' for index in range(5)]
    (out / 'samples.csv').write_text('\\n'.join(rows) + '\\n')
    """)


MEASUREMENT_CSV = ('phase,completed_ns,bytes\npreload,0,8192\n' + ''.join(
    f'measurement,{index * 1000000},8192\n' for index in range(5))).encode()


class FakeObjectStore(ObjectStore):
    """In-memory storage JSON API with generations and uploads."""

    def __init__(self):
        self.objects = {}
        self.calls = []
        self.uploads = []
        self.corrupt = set()

    def head(self, key):
        if key not in self.objects:
            raise ApiError(404, 'missing')
        data, generation = self.objects[key]
        digest = 'corrupt' if key in self.corrupt else ledger_module.sha256_bytes(data)
        return dict(size=str(len(data)), generation=str(generation),
                    metadata={'sha256': digest})

    def get(self, key, *, generation=None, max_bytes=256 * 1024 * 1024):
        if key not in self.objects:
            raise ApiError(404, 'missing')
        data, current = self.objects[key]
        if generation is not None and int(generation) != current:
            raise ApiError(404, 'generation changed')
        return data

    def put(self, key, data, *, content_type='application/octet-stream',
            if_generation_match=None, metadata=None):
        data = bytes(data)
        self.calls.append((key, if_generation_match))
        current = self.objects.get(key, (b'', 0))[1]
        if if_generation_match is not None and int(if_generation_match) != current:
            raise ApiError(412, 'precondition failed')
        generation = current + 1
        self.objects[key] = (data, generation)
        return dict(size=str(len(data)), generation=str(generation),
                    metadata={'sha256': ledger_module.sha256_bytes(data)})

    def upload(self, key, path, *, content_type='application/octet-stream',
               if_generation_match=None, metadata=None, deadline_check=None):
        data = Path(path).read_bytes()
        self.uploads.append(key)
        return self.put(key, data, if_generation_match=if_generation_match,
                        metadata=metadata)


class FakeRemote:
    """The gcloud SSH callback with an in-memory file system per host."""

    def __init__(self):
        self.by_host = {}
        self.calls = []
        self.owner_ready = {}
        self.fail_cases = set()

    def files(self, host):
        return self.by_host.setdefault(host, {})

    def commands(self, host=None):
        return [call for call in self.calls if host is None or call[0] == host]

    def sample_payload(self):
        """One /proc sample whose device counters advance per call."""
        self.samples_served = getattr(self, 'samples_served', 0) + 1
        sectors = 16 * self.samples_served
        stat = ('4242 (kv-research) S '
                + ' '.join(['0'] * 11 + ['100', '100'] + ['0'] * 6 + ['5'] + ['0'] * 30))
        return ('@@vmstat\npswpin 1\npswpout 2\n@@blk\n'
                f'sda 10 0 8 0 20 0 {sectors} 0 0 0 0\n'
                f'@@4242\n{stat}\n'
                'VmRSS:     1024 kB\nVmHWM:     2048 kB\nVmSwap:       0 kB\n'
                'rchar: 10\nwchar: 20\nsyscr: 30\nsyscw: 40\n'
                'read_bytes: 50\nwrite_bytes: 60\ncancelled_write_bytes: 0\n')

    def run(self, host, command, *, stdin=None, check=True):
        self.calls.append((host, command))
        tokens = shlex.split(command)
        files = self.files(host)
        if '@@' in command:
            return 0, self.sample_payload().encode(), b''
        if tokens[:1] == ['pgrep']:
            return 0, b'4242\n', b''
        if tokens[:2] == ['rm', '-rf']:
            for target in tokens[2:]:
                if target == '&&':
                    break
                for key in [key for key in files
                            if key == target or key.startswith(target + '/')]:
                    del files[key]
            return 0, b'', b''
        if tokens[:1] == ['if']:
            if 'mv' in tokens:
                index = tokens.index('mv')
                source, target = (tokens[index + 1].rstrip(';'),
                                  tokens[index + 2].rstrip(';'))
                for key in [key for key in files if key.startswith(source + '/')]:
                    files[target + key[len(source):]] = files.pop(key)
            return 0, b'', b''
        if tokens[:2] in (['mkdir', '-p'], ['chmod', '+x']) or tokens[:1] == ['kill']:
            return 0, b'', b''
        if tokens[:3] == ['tar', '-xzf', '-']:
            return 0, b'', b''
        if tokens[:2] == ['cat', '>']:
            files[tokens[2]] = stdin or b''
            return 0, b'', b''
        if tokens[:2] == ['tar', '-czf']:
            directory = tokens[tokens.index('-C') + 1]
            return 0, self.archive(directory, files), b''
        case = Path(tokens[tokens.index('--config') + 1]).stem
        output = tokens[tokens.index('--output') + 1]
        if case in self.fail_cases:
            return 2, b'', b'boom'
        files[f'{output}/summary.json'] = json.dumps({'case': case}).encode()
        files[f'{output}/samples.csv'] = MEASUREMENT_CSV
        config = Path(tokens[tokens.index('--config') + 1])
        database = config.parent.parent / 'db' / config.stem
        files[f'{database}/evidence/read.json'] = b'{"read": 1}\n'
        return 0, b'', b''

    def shell(self, host, command, *, stdin=None, check=True):
        if 'setsid' in command:
            self.calls.append((host, command))
            pidfile = re.search(r'echo \$\$ > (\S+);', command).group(1)
            files = self.files(host)
            files[pidfile] = b'4242\n'
            files.update(self.owner_ready.get(host, {}))
            return 0, b'', b''
        code, out, err = self.run(host, command, stdin=stdin, check=False)
        if check and code != 0:
            raise manual_sui.LaunchError(f'{host}: command failed ({code})')
        return code, out, err

    def read_bytes(self, host, path):
        try:
            return self.files(host)[str(path)]
        except KeyError:
            raise manual_sui.LaunchError(f'{host}: cannot read {path}') from None

    def exists(self, host, path):
        path = str(path)
        files = self.files(host)
        return path in files or any(key.startswith(path + '/') for key in files)

    def archive(self, directory, files):
        import io
        import tarfile
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w:gz') as handle:
            for key, data in files.items():
                if not key.startswith(directory.rstrip('/') + '/'):
                    continue
                info = tarfile.TarInfo(key[len(directory.rstrip('/')) + 1:])
                info.size = len(data)
                handle.addfile(info, io.BytesIO(data))
        return buffer.getvalue()


class Driver(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'campaign'
        spec_dir = self.root / 'stage3' / 'spec'
        spec_dir.mkdir(parents=True)
        self.binary = self.base / 'kv'
        self.binary.write_text(FAKE)
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        self.log = self.base / 'calls.txt'
        self.log.write_text('')
        self.old = dict(os.environ)
        os.environ['CALL_LOG'] = str(self.log)
        self.addCleanup(self.restore)
        self.cells = []
        for name, arm, tier in (('S3-A1-c2-native-cache0', 'A', 'native'),
                                ('S3-A2-c2-native-cache0', 'A', 'synchronous'),
                                ('S3-B1-c1-native-cache0', 'B', 'native')):
            path = spec_dir / f'{name}.json'
            path.write_text(json.dumps({'case_id': name}))
            self.cells.append(dict(id=name, stage='stage3', unit=name, arm=arm,
                                   tier=tier, path=str(path),
                                   config_hash=ledger_module.config_hash(
                                       spec=ledger_module.sha256_file(path),
                                       knobs={}, engine_rev='e', sui_rev='s',
                                       binary_ids={'kv': 'b'})))

    def restore(self):
        os.environ.clear()
        os.environ.update(self.old)

    def calls(self):
        return self.log.read_text().splitlines()

    def local(self, mode='rehearsal', remote=None):
        return stage3_campaign.LocalRunner(
            binary=self.binary, data_root=self.root,
            stale_root=self.root / 'stage3' / 'stale', mode=mode, remote=remote)

    def ledger(self):
        return ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json')

    def run_campaign(self, *, ledger=None, cells=None, stop=True, remote=None,
                     owners=None, bucket=None):
        return stage3_campaign.run_campaign(
            cells or self.cells, local=self.local(), root=self.root,
            ledger=ledger or self.ledger(), remote=remote, owners=owners,
            bucket=bucket, stop_on_failure=stop, allow_local_ledger=bucket is None)

    # ---- local arm -----------------------------------------------------
    def test_failed_cell_is_retried_and_passed_cells_are_skipped(self):
        os.environ['FAIL_CASES'] = self.cells[1]['id']
        results, planned = self.run_campaign()
        self.assertEqual([row['id'] for row in planned],
                         [cell['id'] for cell in self.cells])
        self.assertEqual([row['status'] for row in results], ['passed', 'failed'])
        saved = self.ledger().load()
        self.assertEqual(saved.status('stage3', self.cells[0]['id']), 'passed')
        self.assertEqual(saved.status('stage3', self.cells[1]['id']), 'failed')
        self.assertIsNone(saved.status('stage3', self.cells[2]['id']))
        del os.environ['FAIL_CASES']
        before = len(self.calls())
        results, planned = self.run_campaign(ledger=saved)
        self.assertEqual([row['id'] for row in planned],
                         [self.cells[1]['id'], self.cells[2]['id']])
        self.assertEqual(len(self.calls()) - before, 2)
        self.assertTrue(all(row['status'] == 'passed' for row in results))

    def test_interrupted_cell_reruns_alone(self):
        self.run_campaign()
        saved = self.ledger().load()
        saved.begin(stage='stage3', cell_id=self.cells[1]['id'], config_hash='new')
        saved.save()
        before = len(self.calls())
        results, planned = self.run_campaign(ledger=self.ledger().load())
        self.assertEqual([row['id'] for row in planned], [self.cells[1]['id']])
        self.assertEqual(len(self.calls()) - before, 1)
        self.assertEqual(results[0]['status'], 'passed')

    def test_changed_config_hash_marks_a_passed_cell_stale(self):
        self.run_campaign()
        changed = [dict(cell) for cell in self.cells]
        changed[1]['config_hash'] = 'changed'
        before = len(self.calls())
        results, planned = self.run_campaign(ledger=self.ledger().load(), cells=changed)
        self.assertEqual([row['id'] for row in planned], [self.cells[1]['id']])
        self.assertEqual(len(self.calls()) - before, 1)

    def test_only_and_from_select_cells(self):
        before = len(self.calls())
        self.run_campaign()
        self.assertEqual(len(self.calls()) - before, 3)
        saved = self.ledger().load()
        _, planned = stage3_campaign.run_campaign(
            self.cells, local=self.local(), root=self.root, ledger=saved,
            from_=self.cells[2]['id'], allow_local_ledger=True)
        self.assertEqual(planned, [])

    def test_previous_database_and_output_are_archived_not_reused(self):
        database = self.root / 'stage3' / 'db' / self.cells[0]['id']
        output = self.root / 'stage3' / 'out' / self.cells[0]['id']
        database.mkdir(parents=True)
        output.mkdir(parents=True)
        (database / 'leftover').write_text('old')
        (output / 'leftover').write_text('old')
        self.run_campaign(cells=self.cells[:1])
        stale = list((self.root / 'stage3' / 'stale').rglob('leftover'))
        self.assertEqual(len(stale), 2)
        self.assertFalse((database / 'leftover').exists())
        self.assertFalse((output / 'leftover').exists())
        saved = self.ledger().load()
        self.assertEqual(saved.entries[('stage3', self.cells[0]['id'])]['start_from'],
                         'fresh')

    def test_missing_summary_never_passes(self):
        self.binary.write_text(self.binary.read_text().replace(
            "(out / 'summary.json').write_text(json.dumps({'case': case, 'complete': True}))",
            'pass'))
        results, _ = self.run_campaign(cells=self.cells[:1])
        self.assertEqual(results[0]['status'], 'failed')
        saved = self.ledger().load()
        self.assertEqual(saved.status('stage3', self.cells[0]['id']), 'failed')

    def test_results_are_labelled_with_the_mode(self):
        results, _ = self.run_campaign(cells=self.cells[:1])
        self.assertEqual(results[0]['mode'], 'rehearsal')
        record = json.loads((self.root / 'stage3' / 'out' / self.cells[0]['id']
                             / 'run.json').read_text())
        self.assertEqual(record['mode'], 'rehearsal')
        self.assertEqual(record['execution'], 'local')

    # ---- host arm ------------------------------------------------------
    def test_arm_b_runs_on_the_owner_host_and_collects_the_output(self):
        remote = FakeRemote()
        native_root = Path('/mnt/tidehunter/campaign')
        runner = stage3_campaign.HostRunner(
            remote=remote, host='H1', binary=native_root / 'storage-node',
            data_root=native_root, mode='cluster')
        results, planned = self.run_campaign(remote=runner, cells=[self.cells[2]])
        self.assertEqual(results[0]['status'], 'passed')
        self.assertTrue(remote.commands('H1'))
        self.assertTrue(any('--config' in command for _, command in remote.calls))
        output = self.root / 'stage3' / 'out' / self.cells[2]['id']
        self.assertTrue((output / 'summary.json').is_file())
        self.assertTrue((output / 'samples.csv').is_file())
        self.assertTrue((output / 'evidence' / 'read.json').is_file())
        record = json.loads((output / 'run.json').read_text())
        self.assertEqual((record['execution'], record['host']), ('host', 'H1'))
        self.assertEqual(record['local_output'], str(output))

    def test_a_host_cell_that_exceeds_its_timeout_is_killed(self):
        class Hanging(FakeRemote):
            def shell(self, host, command, *, stdin=None, check=True):
                if '--config' in command and '--output' in command:
                    self.calls.append((host, command))
                    threading.Event().wait(2)
                    return 0, b'', b''
                if command.startswith('pgrep'):
                    self.calls.append((host, command))
                    return 0, b'4242\n', b''
                return super().shell(host, command, stdin=stdin, check=check)

        remote = Hanging()
        runner = stage3_campaign.HostRunner(
            remote=remote, host='H1', binary=Path('/mnt/c/storage-node'),
            data_root=Path('/mnt/c'), mode='cluster')
        with self.assertRaises(stage3_campaign.CellFailure) as raised:
            runner.run(self.cells[2], output=self.base / 'out', attempt=1, timeout=0.2)
        self.assertEqual(raised.exception.record['exit_code'], None)
        self.assertIn('kill -KILL 4242', [command for _, command in remote.calls])

    def test_unsafe_archive_member_is_refused(self):
        import io
        import tarfile
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w:gz') as handle:
            info = tarfile.TarInfo('../escape')
            info.size = 1
            handle.addfile(info, io.BytesIO(b'x'))
        with self.assertRaisesRegex(ValueError, 'unsafe archive member'):
            stage3_campaign.extract_archive(buffer.getvalue(),
                                            self.base / 'target')

    def test_schedule_runs_arm_a_before_arm_b_one_start_per_tier(self):
        cells = [dict(id='b-sync', arm='B', tier='synchronous'),
                 dict(id='a-sync', arm='A', tier='synchronous'),
                 dict(id='b-native', arm='B', tier='native'),
                 dict(id='a-native', arm='A', tier='native')]
        self.assertEqual([cell['id'] for cell in stage3_campaign.schedule(cells)],
                         ['a-native', 'a-sync', 'b-native', 'b-sync'])

    # ---- owner lifecycle -----------------------------------------------
    def external(self):
        """Both tiers prepared with the one stage guest root, as on the cluster."""
        preparations = {}
        for tier in stage3_specs.TIERS:
            directory = preparation(self.base / tier, tier,
                                    guest=self.base / 'guest')
            preparations[tier] = stage3_specs.load_external(directory, tier=tier)
        return preparations

    def seed_owner_files(self, remote, preparation_, tier):
        """The next owner incarnation publishes its own ready and receipt files."""
        for record in preparation_['manifest']['hosts']:
            ready = record['ready_files'][preparation_['index']]
            receipt = dict(partition=record['partition'],
                           durability=dict(tier=tier), launch_nonce='nonce-' + tier)
            remote.owner_ready[record['host']] = {
                ready: b'',
                ready + '.durability.json': json.dumps(receipt).encode()}

    def test_owners_start_the_tier_and_copy_the_receipts(self):
        remote = FakeRemote()
        preparations = self.external()
        owners = stage3_campaign.Owners(remote, preparations, root=self.root,
                                        guest_root=self.base / 'guest')
        self.seed_owner_files(remote, preparations['native'], 'native')
        receipts = owners.start('native')
        self.assertEqual([row['partition'] for row in receipts], [0, 1])
        for partition in (0, 1):
            path = stage3_specs.receipt_path(self.root, 'native', partition)
            self.assertEqual(json.loads(path.read_text())['launch_nonce'],
                             'nonce-native')
        self.assertTrue(any('--experimental-storage-node' in command
                            for _, command in remote.calls))
        # A tier switch stops the running set before installing the next one.
        self.seed_owner_files(remote, preparations['synchronous'], 'synchronous')
        owners.start('synchronous')
        self.assertEqual(json.loads(stage3_specs.receipt_path(
            self.root, 'synchronous', 0).read_text())['launch_nonce'], 'nonce-synchronous')
        self.assertTrue(any(command.startswith('kill -TERM') for _, command in remote.calls))
        self.assertEqual(owners.active, 'synchronous')
        self.assertEqual(owners.stop(), [])
        self.assertIsNone(owners.active)

    def test_arm_b_stops_the_owner_set_first(self):
        remote = FakeRemote()
        preparations = self.external()
        self.seed_owner_files(remote, preparations['native'], 'native')
        owners = stage3_campaign.Owners(remote, preparations, root=self.root,
                                        guest_root=self.base / 'guest')
        runner = stage3_campaign.HostRunner(
            remote=remote, host='H1', binary=Path('/mnt/storage-node'),
            data_root=Path('/mnt/campaign'), mode='cluster')
        results, planned = self.run_campaign(
            cells=[self.cells[0], self.cells[2]], remote=runner, owners=owners)
        self.assertEqual([row['id'] for row in planned],
                         [self.cells[0]['id'], self.cells[2]['id']])
        reasons = [row['status'] for row in results if row['id'] != 'owner-processes']
        self.assertEqual(reasons, ['passed', 'passed'])
        self.assertTrue(any(command.startswith('kill -TERM')
                            for _, command in remote.calls))

    # ---- upload before pass --------------------------------------------
    def bucket(self, store):
        return stage3_campaign.Bucket(store, 'campaign-hybrid-sui-2026-11-25')

    def test_passed_requires_the_verified_upload(self):
        store = FakeObjectStore()
        results, _ = self.run_campaign(cells=self.cells[:1], bucket=self.bucket(store))
        self.assertEqual(results[0]['status'], 'passed')
        case = self.cells[0]['id']
        self.assertIn(f'campaign-hybrid-sui-2026-11-25/stage3/evidence/{case}/'
                      'attempt-1/summary.json', store.objects)
        self.assertIn(f'campaign-hybrid-sui-2026-11-25/stage3/evidence/{case}/'
                      'attempt-1/run.json', store.objects)
        self.assertIn(f'campaign-hybrid-sui-2026-11-25/stage3/raw/{case}/'
                      'attempt-1/samples.csv', store.objects)
        self.assertIn('campaign-hybrid-sui-2026-11-25/campaign-ledger.json', store.objects)
        saved = ledger_module.Ledger.from_bytes(
            store.objects['campaign-hybrid-sui-2026-11-25/campaign-ledger.json'][0])
        self.assertEqual(saved.status('stage3', case), 'passed')

    def test_an_upload_that_does_not_verify_never_passes(self):
        store = FakeObjectStore()
        store.corrupt.add('campaign-hybrid-sui-2026-11-25/stage3/evidence/'
                          f'{self.cells[0]["id"]}/attempt-1/summary.json')
        results, _ = self.run_campaign(cells=self.cells[:1], bucket=self.bucket(store))
        self.assertEqual(results[0]['status'], 'failed')
        saved = self.ledger().load()
        self.assertEqual(saved.status('stage3', self.cells[0]['id']), 'failed')

    def test_resume_merges_the_campaign_ledger_from_the_bucket(self):
        store = FakeObjectStore()
        bucket = self.bucket(store)
        passed = ledger_module.Ledger()
        entry = passed.begin(stage='stage3', cell_id=self.cells[0]['id'],
                             config_hash=self.cells[0]['config_hash'])
        output = self.root / 'stage3' / 'out' / self.cells[0]['id']
        output.mkdir(parents=True)
        (output / 'summary.json').write_text('{}')
        passed.mark_passed(entry, stage3_campaign.artifacts(output))
        store.put(bucket.key(), passed.to_bytes(), if_generation_match=0)
        merged = bucket.merge(ledger_module.Ledger(self.root / 'stage3' / 'run-ledger.json'))
        before = len(self.calls())
        results, planned = self.run_campaign(ledger=merged, cells=self.cells[:2])
        self.assertEqual([row['id'] for row in planned], [self.cells[1]['id']])
        self.assertEqual(len(self.calls()) - before, 1)

    def test_ledger_copy_is_written_before_and_after_every_cell(self):
        store = FakeObjectStore()
        self.run_campaign(cells=self.cells[:1], bucket=self.bucket(store))
        writes = [call for call in store.calls if call[0].endswith('campaign-ledger.json')]
        self.assertGreaterEqual(len(writes), 2)
        self.assertEqual(writes[0][1], 0)
        self.assertEqual(writes[-1][1], len(writes) - 1)


class Phases(Driver):
    """Phase A (probe), phase B (calibration) and the resource rows."""

    def test_the_probe_runs_on_the_store_host_and_projects_the_grid(self):
        remote = FakeRemote()
        owners = stage3_campaign.Owners(remote, self.external(), root=self.root,
                                       guest_root=self.base / 'guest')
        spec = stage3_specs.write_probe(self.root, native_root=Path('/mnt/campaign'))
        runner = stage3_campaign.HostRunner(
            remote=remote, host='H1', binary=Path('/mnt/campaign/storage-node'),
            data_root=Path('/mnt/campaign'), mode='cluster')
        identity = stage3_campaign.entry_identity(
            spec, knobs=dict(phase='probe'), engine_rev='e', sui_rev='s',
            binary_ids={'kv': 'b'})
        measurements = stage3_campaign.run_probe(
            path=spec, identity=identity, runner=runner, owners=owners,
            root=self.root, ledger=self.ledger(), bucket=None, timeout=60,
            allow_local_ledger=True)
        self.assertEqual(measurements['measurement_calls'], 5)
        self.assertAlmostEqual(measurements['measurement_window_seconds'], 0.004)
        self.assertAlmostEqual(measurements['calls_per_second'], 1250.0)
        self.assertEqual(measurements['cell'], stage3_specs.PROBE_CASE)
        saved = self.ledger().load()
        self.assertEqual(saved.status('stage3', stage3_specs.PROBE_CASE), 'passed')
        # The probe installed the native archive so arm B has its binary.
        self.assertEqual(owners.installed, 'native')
        self.assertIsNone(owners.active)
        document = stage3_campaign.projection(
            measurements, stage3_specs.operation_counts(50_000), counts_source='test')
        self.assertEqual(document['grid_measurement_operations'], 50_000)
        self.assertAlmostEqual(document['projected_grid_cell_seconds'],
                               measurements['preload_seconds'] + 55_000 / 1250.0)
        path = stage3_campaign.write_projection(self.root, None, document)
        self.assertEqual(json.loads(path.read_text())['counts_source'], 'test')

    def test_calibration_freezes_one_count_per_family(self):
        remote = FakeRemote()
        preparations = self.external()
        owners = stage3_campaign.Owners(remote, preparations, root=self.root,
                                       guest_root=self.base / 'guest')
        self.seed_owner_files(remote, preparations['native'], 'native')
        counts = stage3_campaign.calibrate(
            root=self.root, runner=self.local(remote=remote), owners=owners,
            ledger=self.ledger(), bucket=None, external=preparations,
            identity_for=lambda path, knobs: stage3_campaign.entry_identity(
                path, knobs=knobs, engine_rev='e', sui_rev='s',
                binary_ids={'kv': 'b'}),
            timeout=60, allow_local_ledger=True)
        self.assertEqual(counts, {'read': 50_000, 'mixed': 50_000, 'write': 50_000,
                                  'get_many': 50_000, 'put_batch': 50_000})
        frozen = json.loads((self.root / 'stage3' / 'calibration.json').read_text())
        self.assertEqual(frozen['counts'], counts)
        self.assertEqual(frozen['pilot_operations'],
                         stage3_specs.CALIBRATION_OPERATIONS)
        saved = self.ledger().load()
        for family in ('read', 'mixed', 'write'):
            self.assertEqual(saved.status('stage3', f'S3-CAL-{family}-c16-native'),
                             'passed')
        # The frozen file drives the generated specs, and a different set is refused.
        spec_root = self.base / 'specroot'
        paths = stage3_specs.write(spec_root, counts=counts)
        self.assertEqual(json.loads(paths[0].read_text())['phases']
                         ['measurement_operations'], 50_000)
        with self.assertRaisesRegex(ValueError, 'differs from these settings'):
            stage3_specs.write(spec_root, counts=stage3_specs.operation_counts(60_000))

    def test_a_calibration_file_is_the_frozen_count_source(self):
        path = self.root / 'stage3' / 'calibration.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dict(counts=dict(
            read=21_000, mixed=22_000, write=23_000, get_many=24_000,
            put_batch=25_000))))
        counts = stage3_specs.read_calibration(path)
        paths = stage3_specs.write(self.base / 'specroot', counts=counts)
        by_family = {}
        for spec in paths:
            document = json.loads(spec.read_text())
            by_family[document['case_id']] = document['phases']['measurement_operations']
        self.assertEqual(by_family['S3-A1-c2-native-cache0'], 21_000)
        self.assertEqual(by_family['S3-A2-c2-native-cache0'], 22_000)
        self.assertEqual(by_family['S3-A3-c2-native-cache0'], 23_000)
        self.assertEqual(by_family['S3-A7-c4-native-cache0'], 24_000)
        self.assertEqual(by_family['S3-A8-c4-native-cache0'], 25_000)
        self.assertEqual(by_family['S3-B1-c1-native-cache0'], 21_000)
        self.assertEqual(by_family['S3-B2-c1-native-cache0'], 23_000)

    def test_resource_rows_are_written_for_the_local_arm(self):
        self.run_campaign(cells=self.cells[:1])
        output = self.root / 'stage3' / 'out' / self.cells[0]['id']
        with (output / 'resources.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertTrue(rows)
        self.assertIn('coordinator', {row['role'] for row in rows})
        summary = json.loads((output / 'resources.json').read_text())
        self.assertEqual(summary['payload_bytes'], 5 * 8192)
        self.assertIsNone(summary['error'])
        self.assertEqual(summary['label'], stage3_campaign.cell_label(self.cells[0]))
        self.assertIn(socket.gethostname(), summary['hosts'])

    def test_resource_rows_come_from_the_store_host(self):
        remote = FakeRemote()
        runner = stage3_campaign.HostRunner(
            remote=remote, host='H1', binary=Path('/mnt/campaign/storage-node'),
            data_root=Path('/mnt/campaign'), mode='cluster')
        self.run_campaign(remote=runner, cells=[self.cells[2]])
        output = self.root / 'stage3' / 'out' / self.cells[2]['id']
        with (output / 'resources.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        roles = {(row['host'], row['role']) for row in rows}
        self.assertIn(('H1', 'store'), roles)
        self.assertIn(('H1', 'host'), roles)
        store = [row for row in rows if row['role'] == 'store'][0]
        self.assertEqual(store['pid'], '4242')
        self.assertEqual(store['hwm_bytes'], str(2048 * 1024))
        summary = json.loads((output / 'resources.json').read_text())
        self.assertEqual(summary['payload_bytes'], 5 * 8192)
        self.assertEqual(summary['device_write_per_payload'],
                         summary['device_write_bytes'] / (5 * 8192))
        host = summary['hosts']['H1']
        self.assertEqual(host['samples'], len([row for row in rows
                                              if row['role'] == 'host']))
        self.assertGreater(host['device_write_bytes'], 0)

    def test_a_shortened_probe_window_never_projects(self):
        document = stage3_campaign.projection(
            dict(measurement_calls=0, measurement_window_seconds=0.0,
                 calls_per_second=None, preload_seconds=1.0, cell='p'),
            stage3_specs.operation_counts(20_000), counts_source='test')
        self.assertIn('error', document)
        self.assertNotIn('projected_grid_cell_seconds', document)


class CommandLine(unittest.TestCase):
    def run_main(self, *arguments):
        old = sys.argv
        sys.argv = ['stage3_campaign.py', *arguments]
        try:
            return stage3_campaign.main()
        finally:
            sys.argv = old

    def test_cluster_mode_names_every_missing_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as raised:
                self.run_main('--root', tmp, '--binary', 'kv')
        message = str(raised.exception)
        for flag in ('--external', '--guest-root', '--owners', '--zone',
                     '--native-root', '--bucket-prefix'):
            self.assertIn(flag, message)

    def test_rehearsal_mode_refuses_cluster_bindings(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as raised:
                self.run_main('--root', tmp, '--binary', 'kv', '--mode', 'rehearsal',
                              '--external', f'native={tmp}')
        self.assertIn('rehearsal mode', str(raised.exception))


class BucketPrefix(unittest.TestCase):
    def test_only_the_reviewed_bucket_and_identity_are_accepted(self):
        with self.assertRaises(SystemExit):
            stage3_campaign.bucket_store('gs://other-bucket/campaign', 'bench-guest@mysten-research.iam.gserviceaccount.com', 60)
        with self.assertRaises(SystemExit):
            stage3_campaign.bucket_store('gs://mysten-research-bench-artifacts', 'bench-guest@mysten-research.iam.gserviceaccount.com', 60)
        with self.assertRaises(SystemExit):
            stage3_campaign.bucket_store('gs://mysten-research-bench-artifacts/campaign', 'loris.tran@epfl.ch', 60)


if __name__ == '__main__':
    unittest.main()
