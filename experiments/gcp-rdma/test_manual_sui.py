"""The three-host Sui launcher: shape, gcloud argv and the stage handshake.

The launcher runs against a fake gcloud transport that executes each command
locally, a fake five-binary build, and a fake run.py that imports the real
`validate_resources` contract. Owners are real processes with real /proc
sampling; no cloud account, no engine and no nvmet are required.
"""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest

import gcp_api
import gcs_ledger
import host_exec
import manual_sui
from test_gcs_ledger import FakeObjectStore

HERE = Path(__file__).resolve().parent
CHECKPOINT = HERE.parent / 'sui-checkpoint'
EXPERIMENTS = HERE.parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class LocalGcloud:
    """Executes the `--command` of each gcloud argv on this host.

    Two logical hosts share one filesystem, so the single absolute guest root
    the manifest requires is translated to a per-host directory before
    execution. The launcher still records the manifest root.
    """

    def __init__(self, root, guest_root):
        self.root, self.guest_root = Path(root), Path(guest_root)
        self.calls = []

    def __call__(self, argv, stdin=None):
        self.calls.append(argv)
        command = next((arg.split('=', 1)[1] for arg in argv
                        if arg.startswith('--command=')), None)
        if command is None:
            raise AssertionError(f'not a gcloud ssh command: {argv}')
        host = argv[3]
        translated = command.replace(str(self.guest_root),
                                     str(self.root / 'hosts' / host / 'guest'))
        completed = subprocess.run(['bash', '-c', translated], input=stdin,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   timeout=120)
        return completed.returncode, completed.stdout, completed.stderr


class FakeBuild:
    def __init__(self, root):
        self.root = root
        self.binaries = root / 'binaries'
        self.binaries.mkdir(parents=True)
        sources = root / 'sources/tidehunter-sharded-wal/experiments'
        scripts = sources / 'sui-checkpoint'
        scripts.mkdir(parents=True)
        for name in ('build.py', 'deployment.py', 'prepare_external.py',
                     'checkpoint_support.py', 'disk_collector.py'):
            shutil.copy2(CHECKPOINT / name, scripts / name)
        shutil.copy2(EXPERIMENTS / 'resident_config.py', sources / 'resident_config.py')
        shutil.copy2(EXPERIMENTS / 'portable_transport.py', sources / 'portable_transport.py')
        # Keep the real validator importable for the fake runner.
        shutil.copy2(CHECKPOINT / 'run.py', scripts / 'run_contract.py')
        for name in ('sui-node-native', 'sui-node-remote', 'state-audit', 'rocksdb-audit'):
            (self.binaries / name).write_text('fake\n')
        owner = self.binaries / 'storage-node'
        owner.write_text(textwrap.dedent("""\
            #!/usr/bin/env python3
            import pathlib, sys, time, yaml
            launch = pathlib.Path(sys.argv[2])
            buffer = bytearray(1 << 20)
            (launch.parent / 'ready').write_text('ready')
            while True:
                time.sleep(0.05)
            """))
        owner.chmod(0o755)
        manifest = {'protocol': 9, 'fabric': True,
                    'source_archive_sha256': 'a' * 64,
                    'source_manifest_sha256': 'b' * 64,
                    'binaries': {path.name: digest(path) for path in self.binaries.iterdir()}}
        (root / 'build-manifest.json').write_text(json.dumps(manifest))
        (scripts / 'run.py').write_text(textwrap.dedent("""\
            import argparse, hashlib, json, time
            from pathlib import Path
            import run_contract

            parser = argparse.ArgumentParser()
            for name in ('--build', '--fixture', '--output', '--external-config',
                         '--external-resources', '--external-stage-dir', '--durability-tier'):
                parser.add_argument(name)
            parser.add_argument('--owners', type=int)
            parser.add_argument('--deadline', type=int)
            parser.add_argument('--fragment-mib', type=int)
            parser.add_argument('--disk-sample-seconds', type=int)
            args, _ = parser.parse_known_args()
            config = Path(args.external_config)
            resources = Path(args.external_resources)
            stage = Path(args.external_stage_dir)
            build_root = Path(args.build)
            Path(args.output).mkdir(parents=True, exist_ok=True)
            sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            binding = dict(config_sha256=sha(config),
                           build_manifest_sha256=sha(build_root / 'build-manifest.json'),
                           checkpoint_digest='EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6')
            stage.mkdir(parents=True, exist_ok=True)
            def publish(name):
                row = dict(binding, stage=name, unix_ns=time.time_ns())
                for file in (name + '.json', 'stage.json'):
                    temporary = stage / (file + '.tmp')
                    temporary.write_text(json.dumps(row))
                    temporary.replace(stage / file)
                return row
            start = publish('remote-starting')
            time.sleep(1.5)
            finish = publish('remote-finished')
            publish('resources-required')
            deadline = time.time() + 30
            while not (stage / 'resources-ready').exists():
                if time.time() > deadline:
                    raise SystemExit(3)
                time.sleep(0.1)
            manifest = json.loads((config.parent / 'external-manifest.json').read_text())
            run_contract.validate_resources(json.loads(resources.read_text()),
                                            manifest, binding, [start, finish])
            (Path(args.output) / 'experiment').mkdir(exist_ok=True)
            (Path(args.output) / 'experiment/results.json').write_text(
                json.dumps(dict(complete=True)))
            (Path(args.output) / 'receipt.json').write_text(json.dumps(dict(complete=True)))
            (Path(args.output) / 'evidence.tar.gz').write_bytes(b'evidence')
            """))

    def fixture(self, root):
        fixture = root / 'fixture'
        fixture.mkdir()
        (fixture / 'experiment').mkdir()
        (fixture / 'experiment/results.json').write_text('{"frozen": true}\n')
        (fixture / 'distributed.yaml').write_text(
            f'db-path: {fixture}/distributed/full_node_db/94970e38de08\n')
        return fixture

    def schema(self, root):
        schema = root / 'native-schema/live/perpetual'
        schema.mkdir(parents=True)
        (schema / 'manifest.json').write_text(
            json.dumps({'namespace': 'live/perpetual', 'backend': 'native'}))
        (schema / 'shape.json').write_text('{}')
        (schema / 'shape.yaml').write_text('key_spaces: []\n')
        return root / 'native-schema'


class LauncherShape(unittest.TestCase):
    def test_one_coordinator_and_two_distinct_owners(self):
        self.assertEqual(manual_sui.plan('h0', ['h1', 'h2']),
                         {'coordinator': 'h0', 'owners': ['h1', 'h2']})
        for owners in (['h1'], ['h1', 'h2', 'h3']):
            with self.subTest(owners=owners), self.assertRaises(ValueError):
                manual_sui.plan('h0', owners)
        for coordinator, owners in (('h1', ['h1', 'h2']), ('h0', ['h1', 'h1']),
                                    ('', ['h1', 'h2'])):
            with self.subTest(coordinator=coordinator, owners=owners), \
                    self.assertRaises(ValueError):
                manual_sui.plan(coordinator, owners)

    def test_gcloud_argv_names_the_host_and_zone(self):
        remote = host_exec.GcloudSsh('us-central1-a')
        self.assertEqual(remote.argv('owner-1', 'true'), [
            'gcloud', 'compute', 'ssh', 'owner-1', '--zone', 'us-central1-a',
            '--quiet', '--command=true'])


class LauncherHandshake(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.build = FakeBuild(self.root / 'build')
        self.fixture = self.build.fixture(self.root)
        self.schema = self.build.schema(self.root)
        self.gcloud = LocalGcloud(self.root, self.root / 'guest')
        self.saved = manual_sui.proc_sampler.SAMPLE_SECONDS
        manual_sui.proc_sampler.SAMPLE_SECONDS = 0.2
        self.addCleanup(setattr, manual_sui.proc_sampler, 'SAMPLE_SECONDS', self.saved)

    def launcher(self, directory='', **overrides):
        base = self.root / directory if directory else self.root
        values = dict(
            remote=host_exec.GcloudSsh('us-central1-a', runner=self.gcloud),
            build_root=self.build.root, fixture=self.fixture,
            output=base / 'run', guest_root=self.root / 'guest',
            stage_dir=base / 'stages', config_dir=base / 'external-config',
            resources=base / 'resources.json', tier='native', owners=2,
            deadline=900, transport='efa', live_transport='rdma', schema=self.schema)
        values.update(overrides)
        return manual_sui.SuiLauncher(**values)

    def test_resume_skips_a_passed_cell_and_reruns_a_stale_one(self):
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        hosts = manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3'])
        first = self.launcher(ledger=ledger, allow_local_ledger=True).run(hosts)
        self.assertTrue(first['complete'])
        self.assertEqual(ledger.status('stage1', 'sui-replay'), 'passed')
        self.assertFalse(first['ledger']['uploaded'])
        calls = len(self.gcloud.calls)
        skipped = self.launcher(ledger=ledger, resume=True,
                                allow_local_ledger=True).run(hosts)
        self.assertTrue(skipped['skipped'])
        self.assertEqual(len(self.gcloud.calls), calls)
        # A changed knob hashes differently and reruns in a fresh output.
        stale = self.launcher('stale', ledger=ledger, resume=True, fragment_mib=16,
                              allow_local_ledger=True).run(hosts)
        self.assertTrue(stale['complete'])
        self.assertNotIn('skipped', stale)
        self.assertEqual(ledger.entries[('stage1', 'sui-replay')]['attempts'], 2)

    def test_a_local_pass_needs_the_explicit_rehearsal_escape(self):
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        hosts = manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3'])
        with self.assertRaises(ValueError):
            self.launcher(ledger=ledger).run(hosts)
        self.assertEqual(ledger.status('stage1', 'sui-replay'), 'failed')

    def test_handshake_satisfies_the_runner_resource_contract(self):
        receipt = self.launcher().run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertTrue(receipt['complete'])
        self.assertEqual(receipt['owners'], 2)
        self.assertTrue((self.root / 'stages/resources-ready').is_file())
        resources = json.loads((self.root / 'resources.json').read_text())
        self.assertEqual(set(resources['hosts']), {'10.0.0.2', '10.0.0.3'})
        for host in resources['hosts'].values():
            self.assertTrue(host['complete'] and host['cleanup_complete'])
            self.assertTrue(host['all_namespaces_healthy'])
            self.assertTrue(host['namespaces'])
            self.assertTrue(all(row['healthy'] for row in host['samples']
                                if row['unix_ns'] >= min(
                                    r['unix_ns'] for r in host['samples'])))

    def test_prepare_names_the_two_owner_hosts_and_three_external_flags(self):
        launcher = self.launcher()
        launcher.run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        prepare = next(command for command in launcher.local_commands
                       if 'prepare_external' in ' '.join(command))
        self.assertIn('--hosts 10.0.0.2,10.0.0.3', ' '.join(prepare))
        self.assertIn('--owners 2', ' '.join(prepare))
        run = next(command for command in launcher.local_commands
                   if '/run.py' in ' '.join(command))
        text = ' '.join(run)
        self.assertIn('--external-config', text)
        self.assertIn('--external-resources', text)
        self.assertIn('--external-stage-dir', text)
        self.assertIn('--durability-tier native', text)

    def test_run_command_repeats_the_shared_knobs_once_and_matches_the_config(self):
        launcher = self.launcher(coordinator_cache_mib=0, shard_cache_mib=0)
        hosts = manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3'])
        launcher.run(hosts)
        command = next(command for command in launcher.local_commands
                       if '/run.py' in ' '.join(command))
        for flag in ('--coordinator-cache-mib', '--shard-cache-mib'):
            self.assertEqual(command.count(flag), 1, flag)
        argv = launcher.run_command()
        config = json.loads((launcher.config_dir / 'config.json').read_text())
        manifest = json.loads((launcher.config_dir / 'external-manifest.json').read_text())
        read_cache = {row['read_cache_bytes'] for row in config['databases'].values()}
        self.assertEqual(read_cache, {int(argv[argv.index('--coordinator-cache-mib') + 1]) * 1024 * 1024})
        self.assertEqual(manifest['shard_cache_bytes'],
                         int(argv[argv.index('--shard-cache-mib') + 1]) * 1024 * 1024)
        self.assertEqual(read_cache, {0})
        self.assertEqual(manifest['shard_cache_bytes'], 0)

    def test_profiling_and_resident_reads_reach_both_owners_and_the_runner(self):
        policy = self.root / 'resident-reads.json'
        policy.write_text(json.dumps(dict(
            source_bytes=0, source_regions=0, destination_bytes=0,
            destination_slots=0, max_read_bytes=0, max_handles=0,
            require_device_read=False)))
        launcher = self.launcher(write_profile_limit=5, resident_reads=policy)
        hosts = manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3'])
        launcher.run(hosts)
        prepare = next(command for command in launcher.local_commands
                       if 'prepare_external' in ' '.join(command))
        runner = next(command for command in launcher.local_commands
                      if '/run.py' in ' '.join(command))
        for command in (prepare, runner):
            text = ' '.join(command)
            self.assertEqual(text.count('--write-profile-limit'), 1)
            self.assertIn('--write-profile-limit 5', text)
            self.assertEqual(text.count('--resident-reads'), 1)
            self.assertIn(f'--resident-reads {policy.resolve()}', text)
        config = json.loads((launcher.config_dir / 'config.json').read_text())
        manifest = json.loads((launcher.config_dir / 'external-manifest.json').read_text())
        self.assertEqual({row['write_profile_limit'] for row in config['databases'].values()}, {5})
        self.assertEqual(manifest['write_profile_limit'], 5)

    def test_bucket_prefix_passes_only_after_a_verified_upload(self):
        store = FakeObjectStore()
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        launcher = self.launcher(ledger=ledger, store=store,
                                 bucket='mysten-research-bench-artifacts',
                                 prefix='campaign-hybrid-sui-2026-11-25')
        receipt = launcher.run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(ledger.status('stage1', 'sui-replay'), 'passed')
        prefix = 'campaign-hybrid-sui-2026-11-25'
        ledger_key = f'{prefix}/campaign-ledger.json'
        self.assertIn(ledger_key, store.objects)
        uploaded = {key for key in store.objects if key != ledger_key}
        self.assertEqual(len(uploaded), len(receipt['upload']['evidence']))
        for key in uploaded:
            self.assertTrue(key.startswith(
                f'{prefix}/stage1/evidence/sui-replay/attempt-1/'), key)
        for row in receipt['upload']['evidence']:
            data, generation = store.objects[row['key']]
            self.assertEqual(hashlib.sha256(data).hexdigest(), row['sha256'])
            self.assertEqual(row['generation'], generation)
        stored, generation = gcs_ledger.ObjectStoreLedger(store).get(ledger_key)
        self.assertEqual(generation, receipt['upload']['ledger']['generation'])
        self.assertEqual(stored, ledger.to_bytes())
        mirror = json.loads((self.root / 'run/upload-receipt.json').read_text())
        self.assertEqual(mirror['ledger']['sha256'], receipt['upload']['ledger']['sha256'])
        self.assertTrue(all(Path(row['path']).is_file() for row in mirror['evidence']))

    def test_failed_upload_never_passes_the_cell(self):
        store = FakeObjectStore()
        store.put = lambda *args, **kwargs: (_ for _ in ()).throw(
            gcp_api.ApiError(503, 'unavailable'))
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        launcher = self.launcher(ledger=ledger, store=store,
                                 bucket='mysten-research-bench-artifacts',
                                 prefix='campaign-hybrid-sui-2026-11-25')
        with self.assertRaises(gcp_api.ApiError):
            launcher.run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(ledger.status('stage1', 'sui-replay'), 'failed')
        self.assertIn('unavailable', ledger.entries[('stage1', 'sui-replay')]['failure'])
        self.assertFalse((self.root / 'run/upload-receipt.json').exists())

    def test_existing_stage_marker_is_refused(self):
        stage = self.root / 'stages'
        stage.mkdir()
        (stage / 'remote-starting.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'fresh'):
            self.launcher().run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))


if __name__ == '__main__':
    unittest.main()
