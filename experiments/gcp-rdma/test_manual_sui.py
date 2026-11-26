"""The three-host Sui launcher: shape, gcloud argv and the stage handshake.

The launcher runs against a fake gcloud transport that executes each command
locally, a fake five-binary build, and a fake run.py that imports the real
`validate_resources` contract. Owners are real processes with real /proc
sampling; no cloud account, no engine and no nvmet are required.
"""
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import time
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
                json.dumps(dict(complete=True, audit=dict(all_equal=True))))
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


INJECTOR_RECORD = {
    'schema': 1, 'kind': 'w1_controller_loss', 'ok': True, 'reason': None,
    'readback_equal': True, 'fail_stop_count': 0, 'reconnect_seconds': 0.2,
    'live_delta': {'available': True, 'errors': 0, 'namespace_connects': 1,
                   'by_counter': {'resume_attempts_total': 2,
                                  'resume_rewrites_total': 1,
                                  'resume_frames_total': 7,
                                  'resume_refused_total': 0}},
}


class MailboxDouble(host_exec.HostExecution):
    """A mailbox-shaped executor: one recorded request per host command.

    Shell commands run locally against a per-host guest root, and the
    controller-loss request is answered with a canned injector record, so no
    sysfs, no JSON-RPC and no engine are needed.
    """

    def __init__(self, root, guest_root, *, record=None, code=0, fleet=None):
        self.root, self.guest_root = Path(root), Path(guest_root)
        self.fleet = fleet
        self.record = dict(INJECTOR_RECORD if record is None else record)
        self.code = code
        self.requests = []

    def execute(self, host, command, *, stdin=None):
        self.requests.append(dict(host=host, command=command))
        if 'fault_inject.py' in command:
            words = shlex.split(command)
            output = Path(words[words.index('--output') + 1])
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(self.record))
            return self.code, json.dumps(dict(ok=self.record.get('ok'))).encode(), b''
        translated = command.replace(str(self.guest_root),
                                     str(self.root / 'hosts' / host / 'guest'))
        completed = subprocess.run(['bash', '-c', translated], input=stdin,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   timeout=120)
        return completed.returncode, completed.stdout, completed.stderr


FLEET = [{'index': index, 'role': 'coordinator' if index == 0 else 'owner',
          'name': 'bench-' + 'a' * 16 + '-' + str(index), 'id': str(500 + index),
          'zone': 'us-central1-a', 'internal_ip': '10.128.0.' + str(4 + index),
          'rdma_ip': '10.20.0.' + str(21 + index)} for index in range(3)]


class FleetAddresses(unittest.TestCase):
    """The control and live-target addresses of one fleet state."""

    def test_tcp_control_uses_the_default_subnet_and_rdma_uses_the_fabric(self):
        control, live = manual_sui.fleet_hosts(
            FLEET, [FLEET[1]['name'], FLEET[2]['name']],
            transport='tcp', live_transport='rdma')
        self.assertEqual(control, ['10.128.0.5', '10.128.0.6'])
        self.assertEqual(live, ['10.20.0.22', '10.20.0.23'])

    def test_a_verbs_control_fabric_binds_the_irdma_address(self):
        control, live = manual_sui.fleet_hosts(
            FLEET, [FLEET[1]['name'], FLEET[2]['name']],
            transport='efa', live_transport='rdma')
        self.assertEqual(control, ['10.20.0.22', '10.20.0.23'])
        self.assertEqual(live, control)

    def test_a_tcp_live_transport_stays_on_the_default_subnet(self):
        control, live = manual_sui.fleet_hosts(
            FLEET, [FLEET[1]['name']], transport='tcp', live_transport='tcp')
        self.assertEqual(control, ['10.128.0.5'])
        self.assertEqual(live, control)

    def test_a_rehearsal_keeps_the_addresses_it_was_given(self):
        self.assertEqual(manual_sui.fleet_hosts(None, ['10.0.0.2'], transport='tcp',
                                                live_transport='rdma'),
                         (['10.0.0.2'], ['10.0.0.2']))

    def test_an_unknown_host_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'not a host of this fleet'):
            manual_sui.fleet_hosts(FLEET, ['owner-x'], transport='tcp',
                                   live_transport='rdma')


class ControllerLossAddresses(unittest.TestCase):
    """The injector must read the config the replay actually runs."""

    def fixture(self, directory, name, value=True):
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        body = 'json-rpc-address: 127.0.0.1:34765\nmetrics-address: 127.0.0.1:43985\n'
        (root / f'{name}.yaml').write_text(body if value else 'metrics-address: x:1\n')

    def test_the_campaign_fixture_has_only_the_distributed_template(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory, 'distributed')
            self.assertFalse((Path(directory) / 'remote.yaml').exists())
            self.assertEqual(manual_sui.controller_loss_addresses(directory),
                             ('127.0.0.1:34765', '127.0.0.1:43985'))

    def test_generated_remote_config_wins_when_present(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory, 'distributed')
            self.fixture(directory, 'remote')
            self.assertEqual(manual_sui.controller_loss_addresses(directory),
                             ('127.0.0.1:34765', '127.0.0.1:43985'))

    def test_a_fixture_without_a_config_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'no remote or distributed config'):
                manual_sui.controller_loss_addresses(directory)


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
        text = ' '.join(prepare)
        self.assertIn('--hosts 10.0.0.2,10.0.0.3', text)
        self.assertIn('--live-hosts 10.0.0.2,10.0.0.3', text)
        self.assertIn('--port 9400', text)
        self.assertIn('--owners 2', text)
        run = next(command for command in launcher.local_commands
                   if '/run.py' in ' '.join(command))
        text = ' '.join(run)
        self.assertIn('--external-config', text)
        self.assertIn('--external-resources', text)
        self.assertIn('--external-stage-dir', text)
        self.assertIn('--durability-tier native', text)

    def test_the_prepared_launch_listens_on_the_control_port_and_exports_the_fabric(self):
        launcher = self.launcher()
        launcher.run(manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        launches = sorted(launcher.config_dir.glob('*/node-*/launch.yaml'))
        self.assertEqual([path.parent.name for path in launches], ['node-0', 'node-1'])
        # The owner control port is 9400 and each partition adds its index.
        self.assertIn('listen: 10.0.0.2:9400', launches[0].read_text())
        self.assertIn('listen: 10.0.0.3:9401', launches[1].read_text())
        self.assertNotIn('listen: coordinator', launches[0].read_text())

    def test_a_fleet_state_resolves_the_launch_files_to_the_guest_addresses(self):
        remote = MailboxDouble(self.root, self.root / 'guest', fleet=FLEET)
        # The campaign control transport is TCP on the default subnet; the live
        # NVMe-oF target is exported on the falcon-rdma subnet.
        launcher = self.launcher(remote=remote, transport='tcp')
        launcher.run(manual_sui.plan(FLEET[0]['name'],
                                     [FLEET[1]['name'], FLEET[2]['name']]))
        prepare = next(command for command in launcher.local_commands
                       if 'prepare_external' in ' '.join(command))
        text = ' '.join(prepare)
        self.assertIn('--hosts 10.128.0.5,10.128.0.6', text)
        self.assertIn('--live-hosts 10.20.0.22,10.20.0.23', text)
        launches = sorted(launcher.config_dir.glob('*/node-*/launch.yaml'))
        self.assertEqual([path.parent.name for path in launches], ['node-0', 'node-1'])
        self.assertIn('listen: 10.128.0.5:9400', launches[0].read_text())
        self.assertIn('listen: 10.128.0.6:9401', launches[1].read_text())
        self.assertIn('address: 10.20.0.22', launches[0].read_text())
        self.assertIn('address: 10.20.0.23', launches[1].read_text())

    def test_a_verbs_cell_listens_on_the_irdma_address(self):
        remote = MailboxDouble(self.root, self.root / 'guest', fleet=FLEET)
        launcher = self.launcher(remote=remote)
        launcher.run(manual_sui.plan(FLEET[0]['name'],
                                     [FLEET[1]['name'], FLEET[2]['name']]))
        launches = sorted(launcher.config_dir.glob('*/node-*/launch.yaml'))
        self.assertIn('listen: 10.20.0.22:9400', launches[0].read_text())

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


class ControllerLossCells(unittest.TestCase):
    def test_cell_ids_follow_the_plan_and_refuse_a_mismatch(self):
        self.assertEqual(manual_sui.controller_loss_cell_id(
            'stage1', 'mid-frame', 'native'), 'S1-4-mid-frame-native')
        self.assertEqual(manual_sui.controller_loss_cell_id(
            'stage2', 'during-flush', 'synchronous'), 'M7-during-flush-synchronous')
        self.assertEqual(manual_sui.controller_loss_cell_id(
            'stage2', 'after-adoption', 'native', 'M7-after-adoption-native'),
            'M7-after-adoption-native')
        with self.assertRaises(ValueError):
            manual_sui.controller_loss_cell_id('stage2', 'mid-frame', 'native',
                                               'S1-4-mid-frame-native')
        with self.assertRaises(ValueError):
            manual_sui.controller_loss_cell('stage3', 'mid-frame', 'native')

    def test_live_window_matches_the_prepared_owner_pool(self):
        self.assertEqual(manual_sui.live_window(0), (4420, 4435))
        self.assertEqual(manual_sui.live_window(1), (4484, 4499))
        with self.assertRaises(ValueError):
            manual_sui.live_window(-1)

    def test_the_gate_fires_only_on_its_own_counter_edge(self):
        clock = [0.0]
        state = dict(watermark=2, writes=10, adoptions=0, flushes=0)
        def metrics(url):
            return ('tidehunter_nvmeof_live_completed_write_ops_total %d\n'
                    'tidehunter_nvmeof_live_adoption_async_exchanges_total %d\n'
                    'tidehunter_nvmeof_live_adoption_sync_exchanges_total 0\n'
                    'tidehunter_nvmeof_live_leased_flushes_total %d\n'
                    'tidehunter_nvmeof_live_leased_flush_covered_frames_total 0\n'
                    % (state['writes'], state['adoptions'], state['flushes']))
        cases = {'mid-frame': 'writes', 'after-adoption': 'adoptions',
                 'during-flush': 'flushes'}
        for point, knob in cases.items():
            with self.subTest(point=point):
                state.update(watermark=2, writes=10, adoptions=0, flushes=0)
                gate = manual_sui.ControllerLoss(
                    point=point, host='h0', address='10.0.0.2',
                    service=4420, service_max=4435, transport='rdma',
                    metrics_url='http://127.0.0.1:9000/metrics',
                    rpc_address='127.0.0.1:9000', output=Path('/nonexistent'),
                    now=lambda: clock[0], latest=lambda address: state['watermark'],
                    metrics=metrics)
                clock[0] = 0.0
                self.assertFalse(gate.poll(None))
                clock[0] = 1.0
                self.assertFalse(gate.poll(None))
                state[knob] += 1
                clock[0] = 2.0
                fired, reason = gate.fires(gate.samples[-2], gate.sample())
                self.assertTrue(fired, reason)

    def test_the_gate_fails_closed_without_a_completed_checkpoint(self):
        gate = manual_sui.ControllerLoss(
            point='mid-frame', host='h0', address='10.0.0.2', service=4420,
            service_max=4435, transport='rdma', metrics_url='',
            rpc_address='127.0.0.1:9000', output=Path('/nonexistent'),
            now=lambda: 0.0, latest=lambda address: 0,
            metrics=lambda url: '')
        fired, reason = gate.fires(None, gate.sample())
        self.assertFalse(fired)
        self.assertIn('no completed checkpoint', reason)

    def test_a_missing_counter_fails_closed(self):
        gate = manual_sui.ControllerLoss(
            point='during-flush', host='h0', address='10.0.0.2', service=4420,
            service_max=4435, transport='rdma', metrics_url='http://x/metrics',
            rpc_address='127.0.0.1:9000', output=Path('/nonexistent'),
            now=lambda: 0.0, latest=lambda address: 5,
            metrics=lambda url: 'tidehunter_nvmeof_live_errors_total 0\n')
        previous = gate.sample()
        current = gate.sample()
        fired, reason = gate.fires(previous, current)
        self.assertFalse(fired)
        self.assertIn('absent from the scrape', reason)

    def test_duplicate_counters_are_refused(self):
        text = ('tidehunter_nvmeof_live_completed_write_ops_total 1\n'
                'tidehunter_nvmeof_live_completed_write_ops_total 2\n')
        with self.assertRaises(ValueError):
            manual_sui.live_sample(text, ('completed_write_ops_total',))
        self.assertEqual(manual_sui.live_sample(
            'tidehunter_nvmeof_live_completed_write_ops_total 7\n',
            ('completed_write_ops_total',)), {'completed_write_ops_total': 7.0})


class ControllerLossInjection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.build = FakeBuild(self.root / 'build')
        self.fixture = self.build.fixture(self.root)
        self.schema = self.build.schema(self.root)
        self.saved = manual_sui.proc_sampler.SAMPLE_SECONDS
        manual_sui.proc_sampler.SAMPLE_SECONDS = 0.2
        self.addCleanup(setattr, manual_sui.proc_sampler, 'SAMPLE_SECONDS', self.saved)

    def gate(self, remote, point='mid-frame', output=None, **overrides):
        state = dict(watermark=0, writes=0, adoptions=0, flushes=0)
        def latest(address):
            state['watermark'] += 1
            return state['watermark']
        def metrics(url):
            for knob in ('writes', 'adoptions', 'flushes'):
                state[knob] += 1
            return ('tidehunter_nvmeof_live_completed_write_ops_total %d\n'
                    'tidehunter_nvmeof_live_adoption_async_exchanges_total %d\n'
                    'tidehunter_nvmeof_live_adoption_sync_exchanges_total 0\n'
                    'tidehunter_nvmeof_live_leased_flushes_total %d\n'
                    'tidehunter_nvmeof_live_leased_flush_covered_frames_total 0\n'
                    % (state['writes'], state['adoptions'], state['flushes']))
        values = dict(
            point=point, host='coordinator', address='10.0.0.2',
            service=4420, service_max=4435, transport='rdma',
            metrics_url='http://127.0.0.1:9000/metrics',
            rpc_address='127.0.0.1:9000',
            output=Path(output or self.root / 'run'), gate_timeout=30.0,
            probe_seconds=0.2, now=time.monotonic, latest=latest, metrics=metrics)
        values.update(overrides)
        return manual_sui.ControllerLoss(**values)

    def launcher(self, remote, gate, output=None, **overrides):
        values = dict(
            remote=remote,
            build_root=self.build.root, fixture=self.fixture,
            output=Path(output or self.root / 'run'), guest_root=self.root / 'guest',
            stage_dir=self.root / 'stages', config_dir=self.root / 'external-config',
            resources=self.root / 'resources.json', tier='native', owners=2,
            deadline=900, transport='efa', live_transport='rdma', schema=self.schema,
            controller_loss=gate)
        values.update(overrides)
        return manual_sui.SuiLauncher(**values)

    def test_an_observed_point_injects_and_passes_the_cell(self):
        remote = MailboxDouble(self.root, self.root / 'guest')
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        gate = self.gate(remote)
        receipt = self.launcher(remote, gate, ledger=ledger, cell_id='S1-4-mid-frame-native',
                                allow_local_ledger=True).run(
            manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertTrue(receipt['complete'])
        self.assertEqual(receipt['controller_loss']['point'], 'mid-frame')
        self.assertTrue(receipt['controller_loss']['injector_ok'])
        self.assertEqual(ledger.status('stage1', 'S1-4-mid-frame-native'), 'passed')
        injector = [row for row in remote.requests if 'fault_inject.py' in row['command']]
        self.assertEqual(len(injector), 1)
        self.assertEqual(injector[0]['host'], 'coordinator')
        words = shlex.split(injector[0]['command'])
        self.assertIn('--service-max', words)
        self.assertEqual(words[words.index('--service-max') + 1], '4435')
        self.assertEqual(words[words.index('--address') + 1], '10.0.0.2')
        self.assertNotIn('--nqn', words)
        evidence = json.loads((self.root / 'run/controller-loss/evidence.json').read_text())
        self.assertTrue(evidence['injected'])
        self.assertGreaterEqual(len(evidence['samples']), 2)
        self.assertEqual(evidence['injector']['ok'], True)
        self.assertTrue((self.root / 'run/controller-loss/mid-frame.json').is_file())
        names = {row['name'] for row in self.launcher(remote, gate).artifacts()}
        self.assertIn('controller-loss/evidence.json', names)
        self.assertIn('controller-loss/mid-frame.json', names)

    def test_an_unobserved_mid_frame_point_fails_the_cell_and_keeps_the_series(self):
        remote = MailboxDouble(self.root, self.root / 'guest')
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        gate = self.gate(remote, gate_timeout=0.2,
                         metrics=lambda url: 'tidehunter_nvmeof_live_completed_write_ops_total 0\n',
                         latest=lambda address: 3)
        with self.assertRaisesRegex(manual_sui.LaunchError, 'was not observed'):
            self.launcher(remote, gate, ledger=ledger,
                          cell_id='S1-4-mid-frame-native',
                          allow_local_ledger=True).run(
                manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(ledger.status('stage1', 'S1-4-mid-frame-native'), 'failed')
        self.assertIn('was not observed',
                      ledger.entries[('stage1', 'S1-4-mid-frame-native')]['failure'])
        evidence = json.loads((self.root / 'run/controller-loss/evidence.json').read_text())
        self.assertFalse(evidence['injected'])
        self.assertTrue(evidence['samples'])
        self.assertTrue(all(not row['counters'].get('completed_write_ops_total')
                            for row in evidence['samples']))
        self.assertFalse([row for row in remote.requests if 'fault_inject.py' in row['command']])

    def test_a_synchronous_flush_point_that_never_fires_is_not_reached(self):
        remote = MailboxDouble(self.root, self.root / 'guest')
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        gate = self.gate(remote, point='during-flush', gate_timeout=0.2,
                         metrics=lambda url: 'tidehunter_nvmeof_live_leased_flushes_total 0\n',
                         latest=lambda address: 3)
        receipt = self.launcher(remote, gate, ledger=ledger, tier='synchronous',
                                cell_id='S1-4-during-flush-synchronous',
                                allow_local_ledger=True).run(
            manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertTrue(receipt['not_reached'])
        self.assertFalse(receipt['complete'])
        self.assertIn('no flush in replay', receipt['reason'])
        self.assertEqual(ledger.status('stage1', 'S1-4-during-flush-synchronous'),
                         'not_reached')
        self.assertFalse([row for row in remote.requests if 'fault_inject.py' in row['command']])

    def test_the_native_flush_point_is_refused_at_admission(self):
        with self.assertRaisesRegex(ValueError, 'during-flush is unreachable at the native tier'):
            manual_sui.controller_loss_admission('during-flush', 'native')
        manual_sui.controller_loss_admission('during-flush', 'synchronous')
        manual_sui.controller_loss_admission('mid-frame', 'native')
        remote = MailboxDouble(self.root, self.root / 'guest')
        gate = self.gate(remote, point='during-flush')
        with self.assertRaisesRegex(ValueError, 'native tier'):
            self.launcher(remote, gate, allow_local_ledger=True).run(
                manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertFalse(remote.requests)

    def test_the_evidence_carries_the_four_resume_deltas(self):
        remote = MailboxDouble(self.root, self.root / 'guest')
        gate = self.gate(remote)
        receipt = self.launcher(remote, gate, cell_id='S1-4-mid-frame-native',
                                allow_local_ledger=True).run(
            manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(receipt['controller_loss']['point'], 'mid-frame')
        evidence = json.loads((self.root / 'run/controller-loss/evidence.json').read_text())
        self.assertEqual(evidence['resume'], {'resume_attempts_total': 2,
                                              'resume_rewrites_total': 1,
                                              'resume_frames_total': 7,
                                              'resume_refused_total': 0})
        missing = dict(INJECTOR_RECORD, live_delta={'available': True, 'errors': 0,
                                                    'namespace_connects': 1})
        failures = manual_sui.controller_loss_verdict(
            dict(point='mid-frame', injected=True, exit_code=0,
                 injector=missing, resume={}))
        self.assertTrue(any('write-resume counters' in item for item in failures))

    def test_a_failed_injector_record_fails_the_cell(self):
        record = dict(INJECTOR_RECORD, ok=False, reason='owner controller did not reconnect',
                      live_delta={'available': True, 'errors': 0, 'namespace_connects': 0})
        remote = MailboxDouble(self.root, self.root / 'guest', record=record, code=1)
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        with self.assertRaisesRegex(manual_sui.LaunchError, 'did not reconnect'):
            self.launcher(remote, self.gate(remote), ledger=ledger,
                          cell_id='S1-4-mid-frame-native',
                          allow_local_ledger=True).run(
                manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(ledger.status('stage1', 'S1-4-mid-frame-native'), 'failed')

    def test_a_new_connection_is_required_after_the_loss(self):
        record = dict(INJECTOR_RECORD,
                      live_delta={'available': True, 'errors': 0, 'namespace_connects': 0})
        remote = MailboxDouble(self.root, self.root / 'guest', record=record)
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        with self.assertRaisesRegex(manual_sui.LaunchError, 'no new verified controller'):
            self.launcher(remote, self.gate(remote), ledger=ledger,
                          cell_id='S1-4-mid-frame-native',
                          allow_local_ledger=True).run(
                manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3']))
        self.assertEqual(ledger.status('stage1', 'S1-4-mid-frame-native'), 'failed')

    def test_a_changed_point_invalidates_a_passed_cell(self):
        remote = MailboxDouble(self.root, self.root / 'guest')
        ledger = manual_sui.ledger_module.Ledger(self.root / 'ledger.json')
        hosts = manual_sui.plan('coordinator', ['10.0.0.2', '10.0.0.3'])
        self.launcher(remote, self.gate(remote), ledger=ledger,
                      cell_id='S1-4-mid-frame-native',
                      allow_local_ledger=True).run(hosts)
        skipped = self.launcher(remote, self.gate(remote), ledger=ledger, resume=True,
                                cell_id='S1-4-mid-frame-native',
                                allow_local_ledger=True).run(hosts)
        self.assertTrue(skipped['skipped'])
        # A different injection target hashes differently, so the same cell
        # reruns in a fresh output instead of being skipped.
        changed = self.launcher(remote, self.gate(
                                    remote,
                                    nqn='nqn.2014-08.org.nvmexpress:uuid:'
                                        '00000000-0000-0000-0000-000000000001',
                                    output=self.root / 'run2'),
                                output=self.root / 'run2', ledger=ledger, resume=True,
                                stage_dir=self.root / 'stages2',
                                config_dir=self.root / 'external-config2',
                                resources=self.root / 'resources2.json',
                                cell_id='S1-4-mid-frame-native',
                                allow_local_ledger=True).run(hosts)
        self.assertNotIn('skipped', changed)
        self.assertEqual(ledger.entries[('stage1', 'S1-4-mid-frame-native')]['attempts'], 2)


if __name__ == '__main__':
    unittest.main()
