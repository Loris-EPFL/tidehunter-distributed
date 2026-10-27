"""Offline regression gates for real-Sui admission, evidence, and teardown."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import sys
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import transport_for_case, select_transports
import sui_checkpoint as supervisor
import guest as kv_guest
from ci_campaign import fresh_archive


def encoded(value):
    return (json.dumps(value, indent=2) + '\n').encode()

def frozen_runner_sources(root):
    source_root = Path(supervisor.__file__).resolve().parents[2]
    records = {str(path.relative_to(source_root)): dict(sha256=supervisor.sha(path))
               for path in (source_root / 'experiments').rglob('*.py')
               if path.is_file() and not path.is_symlink()}
    frozen = root / 'frozen-source'
    for relative in supervisor.runner_helper_origins(records).values():
        target = frozen / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_root / relative, target)
    return frozen, records



def example_config():
    cfg = supervisor.schema()['config']
    cfg['deadline_utc'] = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
        cfg[name]['sha256'] = 'a' * 64
    for name in ('build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256'):
        cfg[name] = 'a' * 64
    cfg['binaries'] = {name: 'b' * 64 for name in supervisor.BINARIES}
    return cfg


def resident_profile():
    return supervisor.resident_read_config(dict(source_bytes=1024**2, source_regions=32,
        destination_bytes=1024**2, destination_slots=4, max_read_bytes=65536,
        max_handles=128, require_device_read=True))

def traffic(identity, start=100, end=200):
    def snapshot(stamp, value):
        return dict(identity=identity, unix_ns=stamp, devices={
            'rdmap0s1': dict(driver='efa', counters={
                'ports/1/hw_counters/send_bytes': value,
                'ports/1/hw_counters/recv_bytes': value})})
    return supervisor.efa_proof(snapshot(1, start), snapshot(2, end), identity)



def completed_node(transport='tcp', preflight_identity=False):
    cfg = example_config()
    common = {name: cfg[name] for name in ('run_id', 'account', 'region', 'instance_type', 'deadline_utc', 'bucket', 's3_prefix')}
    common.update(transport=transport, cell_id='r01-' + transport)
    root = cfg['run_root'] + '/checkpoint-cells/' + common['cell_id'] + '/storage'
    canonical = transport_for_case(transport)
    cloud = dict(run_id=cfg['run_id'], cell_id=common['cell_id'], transport=transport,
        client_transport=canonical, owner_transport=canonical, deadline_utc=cfg['deadline_utc'], root=root,
        storage_binary_sha256=cfg['binaries']['storage-node'], build_manifest_sha256=cfg['build_manifest_sha256'],
        config_sha256='c' * 64, checkpoint_digest=supervisor.CHECKPOINT,
        resource_path=root + '/resources.json', ready_path=root + '/ready', stop_path=root + '/stop',
        commands=[[root + '/storage-node', '--experimental-storage-node', root + f'/namespace-{i}/node-0/launch.yaml'] for i in range(2)],
        ready_files=[root + f'/namespace-{i}/node-0/ready' for i in range(2)],
        stop_files=[root + f'/namespace-{i}/node-0/stop' for i in range(2)])
    if preflight_identity and transport in ('efa', 'efa-direct'):
        cloud['fabric_preflight_transport'] = transport
    expected = dict(campaign=common, cloud=cloud, partition=0, instance_id=cfg['nodes'][0]['instance_id'])
    namespaces = [dict(index=i, command=command, pid=100 + i, process_group=100 + i,
        starttime_ticks=1000 + i, ready=cloud['ready_files'][i], stop=cloud['stop_files'][i],
        reaped=True, exit_code=0, stdout=root + f'/cloud-node-logs/namespace-{i:02d}.stdout.log',
        stderr=root + f'/cloud-node-logs/namespace-{i:02d}.stderr.log') for i, command in enumerate(cloud['commands'])]
    receipt = {key: cloud[key] for key in ('run_id', 'root', 'deadline_utc', 'storage_binary_sha256',
        'build_manifest_sha256', 'config_sha256', 'checkpoint_digest', 'cell_id', 'transport',
        'client_transport', 'owner_transport')}
    receipt.update(node_config_sha256=hashlib.sha256(encoded(cloud)).hexdigest(), schema=1, owner_pid=99,
        started_unix_ns=100, finished_unix_ns=500, started_unix=100 / 1e9, finished_unix=500 / 1e9,
        ready=True, complete=True, failure=None, cleanup_complete=True, stop_reason='controller_stop',
        expected_namespace_count=2, namespaces=namespaces)
    resources = copy.deepcopy(receipt)
    resources.update(all_namespaces_healthy=True, samples=[dict(unix_ns=200, processes=[
        dict(index=i, pid=100 + i, starttime_ticks=1000 + i, VmRSS=4096, VmSwap=0, io={'read_bytes': 0}) for i in range(2)])])
    status = dict(Id='sui-storage-' + cfg['run_id'] + '-' + cloud['cell_id'] + '.service', LoadState='loaded', ActiveState='active',
        SubState='exited', Result='success', ExecMainCode='1', ExecMainStatus='0', MainPID='0', ExecMainPID='99')
    files = {name: b'owned process log\n' for name in supervisor.required_node_files(expected)}
    files.update({
        'node-config.json': encoded(cloud), 'supervisor-node.json': encoded(expected),
        'cloud-node-receipt.json': encoded(receipt), 'resources.json': encoded(resources),
        'systemd-status.json': encoded(status),
        'bootstrap.json': encoded(dict(run_id=cfg['run_id'], identity=dict(instanceId=expected['instance_id'],
            accountId=cfg['account'], instanceType=cfg['instance_type'], region=cfg['region']))),
        'preflight/platform.json': encoded(dict(uname=['Linux'], os_release='Ubuntu')),
        'preflight/storage-node.ldd.txt': b'libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6\n',
    })
    if transport in ('efa', 'efa-direct'):
        files['preflight/efa-provider.txt'] = (
            f'provider: efa\n    fabric: {transport}\n    type: FI_EP_RDM\n').encode()
        proof = traffic(supervisor.efa_identity(expected))
        files['efa-proof.json'] = encoded(proof)
        files['efa-before.json'] = encoded(proof['before'])
        files['efa-after.json'] = encoded(proof['after'])
    return expected, resources, files


def archive_at(path, files, extra=()):
    with tarfile.open(path, 'w:gz') as archive:
        for name, content in [*files.items(), *extra]:
            if isinstance(content, tarfile.TarInfo):
                archive.addfile(content)
            else:
                member = tarfile.TarInfo(name)
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
    return path


class RunnerHelperAdmissionTests(unittest.TestCase):
    def sources(self, root):
        engine = root / 'source'
        records, helpers = {}, []
        current = Path(supervisor.__file__).resolve().parent
        for relative, content in (
                ('experiments/aws-sharded-wal/sui_checkpoint.py', (current / 'sui_checkpoint.py').read_bytes()),
                ('experiments/aws-sharded-wal/guest.py', (current / 'guest.py').read_bytes()),
                ('experiments/resident_config.py', (current.parent / 'resident_config.py').read_bytes()),
                ('experiments/portable_transport.py', (current.parent / 'portable_transport.py').read_bytes()),
                ('experiments/aws-sharded-wal/test_not_a_runtime_helper.py', b'not installed\n')):
            path = engine / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            records[relative] = dict(sha256=supervisor.sha(path))
            if not path.name.startswith('test_'):
                helpers.append(path)
        return records, helpers

    def install(self, root, helpers):
        archive = root / 'bundle.tar.gz'
        fresh_archive(archive, [(path, 'runner/' + path.name) for path in helpers])
        supervisor.safe_extract(archive, root / 'installed', max_bytes=512 << 20)
        return root / 'installed/runner'

    def test_real_flattened_archive_binds_shared_helper_to_its_canonical_origin(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records, helpers = self.sources(root)
            installed = self.install(root, helpers)
            self.assertNotIn('experiments/aws-sharded-wal/resident_config.py', records)
            self.assertEqual(supervisor.sha(installed / 'resident_config.py'),
                             records['experiments/resident_config.py']['sha256'])
            supervisor.validate_runner_helpers(installed, records)
            self.assertEqual({path.name for path in installed.iterdir()},
                             {'sui_checkpoint.py', 'guest.py', 'resident_config.py', 'portable_transport.py'})

    def test_actual_archives_reject_missing_unknown_changed_and_symlink_helpers(self):
        for change in ('missing_shared', 'missing_runtime', 'unknown', 'changed_shared', 'changed_runtime', 'symlink'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                records, helpers = self.sources(root)
                shared = next(path for path in helpers if path.name == 'resident_config.py')
                guest = next(path for path in helpers if path.name == 'guest.py')
                if change == 'missing_shared':
                    helpers.remove(shared)
                elif change == 'missing_runtime':
                    helpers.remove(guest)
                elif change == 'unknown':
                    unknown = root / 'unknown.py'
                    unknown.write_text('unbound helper\n')
                    helpers.append(unknown)
                elif change == 'changed_shared':
                    shared.write_text('changed shared helper\n')
                elif change == 'changed_runtime':
                    guest.write_text('changed runtime helper\n')
                else:
                    original = root / 'original-guest.py'
                    guest.rename(original)
                    guest.symlink_to(original)
                with self.assertRaises(ValueError):
                    installed = self.install(root, helpers)
                    supervisor.validate_runner_helpers(installed, records)

    def test_equal_bytes_under_noncanonical_or_ambiguous_source_names_are_not_a_fallback(self):
        for destination in ('experiments/aws-sharded-wal/resident_config.py',
                            'experiments/other/resident_config.py'):
            with self.subTest(destination=destination), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                records, helpers = self.sources(root)
                installed = self.install(root, helpers)
                records[destination] = records.pop('experiments/resident_config.py')
                with self.assertRaises(ValueError):
                    supervisor.validate_runner_helpers(installed, records)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records, helpers = self.sources(root)
            installed = self.install(root, helpers)
            records['experiments/aws-sharded-wal/resident_config.py'] = dict(
                records['experiments/resident_config.py'])
            with self.assertRaisesRegex(ValueError, 'ambiguous canonical'):
                supervisor.validate_runner_helpers(installed, records)

    def test_missing_link_or_malformed_canonical_hash_record_remains_fail_closed(self):
        for entry in (None, {}, {'link': 'somewhere.py'}, {'sha256': None}, {'sha256': 1},
                      {'sha256': 'f' * 64}, {'sha256': 'not-a-hash'}):
            with self.subTest(entry=entry), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                records, helpers = self.sources(root)
                installed = self.install(root, helpers)
                if entry is None:
                    del records['experiments/resident_config.py']
                else:
                    records['experiments/resident_config.py'] = entry
                with self.assertRaises(ValueError):
                    supervisor.validate_runner_helpers(installed, records)

    def test_controller_rejects_installed_symlink_even_when_target_bytes_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records, helpers = self.sources(root)
            installed = self.install(root, helpers)
            helper = installed / 'resident_config.py'
            target = root / 'same-bytes.py'
            helper.rename(target)
            helper.symlink_to(target)
            with self.assertRaises(ValueError):
                supervisor.validate_runner_helpers(installed, records)


    def test_canonical_assembler_validates_sources_and_excludes_tests_before_copying(self):
        for change in (None, 'missing', 'changed', 'symlink', 'extra'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                records, _ = self.sources(root)
                source = root / 'source'
                dependency = source / 'experiments/resident_config.py'
                if change == 'missing':
                    dependency.unlink()
                elif change == 'changed':
                    dependency.write_text('changed dependency\n')
                elif change == 'symlink':
                    target = root / 'original.py'
                    dependency.rename(target)
                    dependency.symlink_to(target)
                elif change == 'extra':
                    (source / 'experiments/aws-sharded-wal/unbound.py').write_text('unbound\n')
                destination = root / 'runner'
                if change is None:
                    supervisor.assemble_runner(source, destination, records)
                    supervisor.validate_runner_helpers(destination, records)
                    self.assertEqual({path.name for path in destination.iterdir()},
                                     {'sui_checkpoint.py', 'guest.py', 'resident_config.py', 'portable_transport.py'})
                else:
                    with self.assertRaises(ValueError):
                        supervisor.assemble_runner(source, destination, records)
                    self.assertFalse(destination.exists())

    def test_actual_assembled_worker_entrypoint_runs_without_repository_python_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen, records = frozen_runner_sources(root)
            runner = supervisor.assemble_runner(frozen, root / 'runner', records)
            poison = root / 'not-a-python-home'
            poison.mkdir()
            with patch.dict(supervisor.os.environ, PYTHONPATH=str(poison), PYTHONHOME=str(poison)):
                receipt = supervisor.validate_runner_entrypoint(runner, records)
            self.assertEqual(receipt['exit_code'], 0)
            self.assertTrue(receipt['isolated_python_environment'])
            self.assertEqual(len(receipt['stdout_sha256']), 64)

    def test_preallocation_import_probe_rejects_dependency_only_available_from_repository_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen, records = frozen_runner_sources(root)
            relative = 'experiments/aws-sharded-wal/guest.py'
            guest = frozen / relative
            guest.write_bytes(guest.read_bytes() + b'\nimport worker_dependency_only_on_repository_path\n')
            records[relative]['sha256'] = supervisor.sha(guest)
            runner = supervisor.assemble_runner(frozen, root / 'runner', records)
            poison = root / 'repository-path'
            poison.mkdir()
            (poison / 'worker_dependency_only_on_repository_path.py').write_text('present_only_outside_bundle = True\n')
            with patch.dict(supervisor.os.environ, PYTHONPATH=str(poison)):
                with self.assertRaisesRegex(ValueError, 'worker_dependency_only_on_repository_path'):
                    supervisor.validate_runner_entrypoint(runner, records)

    def test_missing_or_changed_installed_dependency_cannot_be_masked_by_repository_imports(self):
        for change in ('missing', 'changed'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                frozen, records = frozen_runner_sources(root)
                runner = supervisor.assemble_runner(frozen, root / 'runner', records)
                dependency = runner / 'resident_config.py'
                if change == 'missing':
                    dependency.unlink()
                else:
                    dependency.write_text('changed installed dependency\n')
                with patch.dict(supervisor.os.environ, PYTHONPATH=str(frozen / 'experiments')):
                    with self.assertRaises(ValueError):
                        supervisor.validate_runner_entrypoint(runner, records)


class EvidenceTests(unittest.TestCase):
    def validate(self, files, expected, resources, extra=()):
        with tempfile.TemporaryDirectory() as temporary:
            archive = archive_at(Path(temporary) / 'evidence.tar.gz', files, extra)
            supervisor.validate_node_archive(archive, expected, resources)

    def test_complete_tcp_and_efa_evidence_pass(self):
        for transport in ('tcp', 'efa', 'efa-direct'):
            with self.subTest(transport=transport):
                expected, resources, files = completed_node(transport)
                self.validate(files, expected, resources)
    def test_direct_archive_rejects_regular_fabric_and_missing_hardware_proof(self):
        expected, resources, files = completed_node('efa-direct')
        for name in ('efa-proof.json', 'efa-before.json', 'efa-after.json', 'preflight/efa-provider.txt'):
            changed = files.copy()
            del changed[name]
            with self.subTest(missing=name), self.assertRaises(ValueError):
                self.validate(changed, expected, resources)
        for fabric in ('efa', 'tcp', 'efa;ofi_rxd'):
            changed = files | {'preflight/efa-provider.txt':
                f'provider: efa\n fabric: {fabric}\n type: FI_EP_RDM\n'.encode()}
            with self.subTest(fabric=fabric), self.assertRaises(ValueError):
                self.validate(changed, expected, resources)
        expected, resources, files = completed_node('efa')
        files['preflight/efa-provider.txt'] = b'provider: efa\n type: FI_EP_RDM\n'
        self.validate(files, expected, resources)
        files['preflight/efa-provider.txt'] = b'provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n'
        with self.assertRaises(ValueError):
            self.validate(files, expected, resources)
    def test_new_regular_archives_require_exact_fabric_but_retained_evidence_is_valid(self):
        expected, resources, files = completed_node('efa', preflight_identity=True)
        self.validate(files, expected, resources)
        files['preflight/efa-provider.txt'] = b'provider: efa\n type: FI_EP_RDM\n'
        with self.assertRaises(ValueError):
            self.validate(files, expected, resources)


    def test_preflight_negotiates_exact_fabric_and_never_falls_back(self):
        import resource
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / 'fi_info'
            executable.write_text('#!' + sys.executable + '\nimport sys\n'
                'args = sys.argv[1:]\n'
                'assert args[args.index("-p")+1] == "efa"\n'
                'assert args[args.index("-t")+1] == "FI_EP_RDM"\n'
                'fabric = args[args.index("-f")+1]\n'
                'print("provider: efa\\n fabric: " + fabric + "\\n type: FI_EP_RDM")\n')
            executable.chmod(0o700)
            with patch.object(supervisor.shutil, 'which', return_value=str(executable)), patch.object(resource, 'setrlimit'):
                for transport in ('efa', 'efa-direct', 'efa-native'):
                    output = root / transport
                    output.mkdir()
                    supervisor.fabric_preflight(output, transport)
                    supervisor.validate_provider((output / 'efa-provider.txt').read_text(), transport)
                executable.write_text('#!' + sys.executable + '\nprint("provider: efa\\n fabric: efa\\n type: FI_EP_RDM")\n')
                with self.assertRaises(ValueError):
                    supervisor.fabric_preflight(root / 'efa-direct', 'efa-direct')
                with self.assertRaises(ValueError):
                    supervisor.fabric_preflight(root / 'efa-native', 'efa-native')
                executable.write_text('#!' + sys.executable + '\nprint("provider: efa\\n fabric: efa-direct\\n type: FI_EP_RDM")\n')
                with self.assertRaises(ValueError):
                    supervisor.fabric_preflight(root / 'efa', 'efa')
                executable.write_text('#!' + sys.executable + '\nraise SystemExit(42)\n')
                with self.assertRaises(RuntimeError):
                    supervisor.fabric_preflight(root / 'efa-direct', 'efa-direct')


    def test_every_log_config_and_receipt_is_mandatory(self):
        expected, resources, files = completed_node()
        for name in files:
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, 'missing mandatory'):
                self.validate({key: value for key, value in files.items() if key != name}, expected, resources)

    def test_receipt_cannot_omit_its_source_or_owner_identity(self):
        for name in ('run_id', 'node_config_sha256', 'owner_pid', 'finished_unix_ns'):
            expected, resources, files = completed_node()
            receipt = json.loads(files['cloud-node-receipt.json'])
            del receipt[name]
            files['cloud-node-receipt.json'] = encoded(receipt)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'fields are incomplete'):
                self.validate(files, expected, resources)

    def test_stale_incomplete_receipt_cannot_qualify_complete_resources(self):
        expected, resources, files = completed_node()
        receipt = json.loads(files['cloud-node-receipt.json'])
        receipt.update(complete=False, cleanup_complete=False, finished_unix_ns=None)
        files['cloud-node-receipt.json'] = encoded(receipt)
        with self.assertRaisesRegex(ValueError, 'receipt does not prove'):
            self.validate(files, expected, resources)

    def test_hash_valid_archive_with_different_resources_is_rejected(self):
        expected, resources, files = completed_node()
        changed = copy.deepcopy(resources)
        changed['samples'][0]['processes'][0]['VmRSS'] += 4096
        files['resources.json'] = encoded(changed)
        with self.assertRaisesRegex(ValueError, 'archived resources differ'):
            self.validate(files, expected, resources)

    def test_node_config_hash_and_source_identity_are_both_bound(self):
        for field in ('config_sha256', 'storage_binary_sha256', 'run_id'):
            expected, resources, files = completed_node()
            cloud = json.loads(files['node-config.json'])
            cloud[field] = 'changed'
            files['node-config.json'] = encoded(cloud)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'node-config/source'):
                self.validate(files, expected, resources)

    def test_pid_reuse_and_namespace_command_changes_are_rejected(self):
        expected, resources, files = completed_node()
        resources['samples'][0]['processes'][0]['starttime_ticks'] += 1
        files['resources.json'] = encoded(resources)
        with self.assertRaisesRegex(ValueError, 'PID/starttime'):
            self.validate(files, expected, resources)
        expected, resources, files = completed_node()
        resources['namespaces'][0]['command'][2] = '/foreign/launch.yaml'
        receipt = json.loads(files['cloud-node-receipt.json'])
        receipt['namespaces'] = copy.deepcopy(resources['namespaces'])
        files['resources.json'], files['cloud-node-receipt.json'] = encoded(resources), encoded(receipt)
        with self.assertRaisesRegex(ValueError, 'namespace command/process'):
            self.validate(files, expected, resources)

    def test_sampler_exit_failure_or_collected_unit_is_not_success(self):
        for change in ({'ExecMainStatus': '1'}, {'LoadState': 'not-found'}, {'ExecMainPID': '999'}, {'forced_stop': True}):
            expected, resources, files = completed_node()
            status = json.loads(files['systemd-status.json']) | change
            files['systemd-status.json'] = encoded(status)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'successful exit'):
                self.validate(files, expected, resources)

    def test_duplicate_traversal_private_file_and_symlink_rejected(self):
        expected, resources, files = completed_node()
        link = tarfile.TarInfo('untrusted-link')
        link.type, link.linkname = tarfile.SYMTYPE, '/etc/passwd'
        for extra in ([('resources.json', files['resources.json'])], [('../escape', b'x')],
                      [('tls/authority.pk8', b'private')], [('untrusted-link', link)]):
            with self.subTest(extra=extra[0][0]), self.assertRaises(ValueError):
                self.validate(files, expected, resources, extra)

    def test_relative_source_symlink_allowed_but_escape_forbidden(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            link = tarfile.TarInfo('sources/alias')
            link.type, link.linkname = tarfile.SYMTYPE, 'file'
            archive_at(root / 'safe.tar.gz', {'sources/file': b'owned'}, [('sources/alias', link)])
            supervisor.safe_extract(root / 'safe.tar.gz', root / 'safe', symlinks=True)
            self.assertEqual((root / 'safe/sources/alias').read_bytes(), b'owned')
            link.linkname = '../../escape'
            archive_at(root / 'bad.tar.gz', {'sources/file': b'owned'}, [('sources/alias', link)])
            with self.assertRaises(ValueError):
                supervisor.safe_extract(root / 'bad.tar.gz', root / 'bad', symlinks=True)

    def collector(self, root, expected, resources, files):
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.root = root
        campaign.cell_root = root
        campaign.cell = dict(cell_id=expected['cloud']['cell_id'], transport=expected['cloud']['transport'], node_archives={})
        campaign.results = root / 'results'
        campaign.results.mkdir()
        campaign.cfg = expected['campaign']
        campaign.s3 = Mock()
        campaign.ssm_one = Mock()
        campaign.collected_final = set()
        node = dict(partition=0, instance_id=expected['instance_id'], private_ip='10.0.0.11')
        payload = root / 'payload-0'
        payload.mkdir()
        (payload / 'supervisor-node.json').write_bytes(encoded(expected))
        source = root / 'objects'
        source.mkdir()
        for name in supervisor.completion_files(campaign.cell['transport']).values():
            (source / name).write_bytes(files[name])
        archive_at(source / 'evidence.tar.gz', files)
        prefix = expected['campaign']['s3_prefix'] + '/results/cells/' + campaign.cell['cell_id'] + '/storage/0/final/'
        receipts = {key: dict(bucket=campaign.cfg['bucket'], key=prefix + name,
                    sha256=supervisor.sha(source / name)) for key, name in
                    (supervisor.completion_files(campaign.cell['transport']) | {'evidence': 'evidence.tar.gz'}).items()}
        reply = dict(run_id=campaign.cfg['run_id'], cell_id=campaign.cell['cell_id'], instance_id=node['instance_id'], partition=0,
                     receipts=receipts, errors=[])
        campaign.object_json = Mock(return_value=reply)
        def local_download(s3, cfg, key, digest, target):
            path = source / key.rsplit('/', 1)[1]
            self.assertEqual(supervisor.sha(path), digest)
            shutil.copy2(path, target)
        return campaign, node, reply, local_download

    def test_collector_rejects_missing_resources_even_when_stale_file_exists(self):
        with tempfile.TemporaryDirectory() as temporary:
            expected, resources, files = completed_node()
            campaign, node, reply, download = self.collector(Path(temporary), expected, resources, files)
            stale = campaign.results / 'storage/0/final'
            stale.mkdir(parents=True)
            (stale / 'resources.json').write_bytes(encoded(resources))
            del reply['receipts']['resources']
            with patch.object(supervisor, 'download', side_effect=download), self.assertRaisesRegex(RuntimeError, 'receipt is incomplete'):
                campaign.collect_node(node, final=True, logs=True)
            self.assertFalse(campaign.collected_final)

    def test_collector_does_not_accept_digest_valid_archive_without_namespace_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            expected, resources, files = completed_node()
            del files['cloud-node-logs/namespace-00.stderr.log']
            campaign, node, reply, download = self.collector(Path(temporary), expected, resources, files)
            with patch.object(supervisor, 'download', side_effect=download), self.assertRaisesRegex(ValueError, 'missing mandatory'):
                campaign.collect_node(node, final=True, logs=True)
            self.assertFalse(campaign.collected_final)

    def test_final_collection_cannot_replace_the_committed_handshake_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            expected, resources, files = completed_node()
            campaign, node, reply, download = self.collector(Path(temporary), expected, resources, files)
            committed = copy.deepcopy(resources)
            committed['samples'][0]['processes'][0]['VmRSS'] += 1
            (campaign.results / 'resources.json').write_bytes(encoded({'hosts': {node['private_ip']: committed}}))
            with patch.object(supervisor, 'download', side_effect=download), self.assertRaisesRegex(ValueError, 'handshake snapshot'):
                campaign.collect_node(node, final=True, logs=True)
            self.assertFalse(campaign.collected_final)

    def test_collector_marks_complete_only_after_validating_archive_and_handshake(self):
        with tempfile.TemporaryDirectory() as temporary:
            expected, resources, files = completed_node()
            campaign, node, reply, download = self.collector(Path(temporary), expected, resources, files)
            (campaign.results / 'resources.json').write_bytes(encoded({'hosts': {node['private_ip']: resources}}))
            with patch.object(supervisor, 'download', side_effect=download):
                campaign.collect_node(node, final=True, logs=True)
            self.assertEqual(campaign.collected_final, {0})


class PreparedResidentPolicyTests(unittest.TestCase):
    def prepare_campaign(self, root, profile, transport='efa-native', source_change=None, captured_uploads=None):
        """Execute the real preparation CLI; only the AWS upload is replaced."""
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.cfg = example_config()
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.cfg.update(transports=[transport], repetitions=1, read_profile_limits=[0],
                            write_profile_limits=[0], native_write_profile_limits=[0])
        if profile is not None:
            campaign.cfg['kv_suite'] = dict(resident_reads=profile)
        campaign.cell = supervisor.matrix_plan(campaign.cfg)[0]
        campaign.root, campaign.cell_root = root, root / 'cell'
        campaign.cell_root.mkdir()
        campaign.results = root / 'results'
        campaign.results.mkdir()
        campaign.fixture = root / 'fixture'
        campaign.fixture.mkdir()
        (campaign.fixture / 'distributed.yaml').write_text('db-path: distributed/store\n')
        campaign.build = root / 'build'
        (campaign.build / 'binaries').mkdir(parents=True)
        binary = campaign.build / 'binaries/storage-node'
        binary.write_bytes(b'fixture executable bytes; preparation hashes and archives, never executes this file')
        campaign.cfg['binaries']['storage-node'] = supervisor.sha(binary)
        frozen, records = frozen_runner_sources(root)
        supervisor.write_json(campaign.build / 'source-manifest.json',
                              {'tidehunter-sharded-wal': {'files': records}})
        campaign.cfg['source_manifest_sha256'] = supervisor.sha(campaign.build / 'source-manifest.json')
        campaign.support = frozen / 'experiments/aws-sharded-wal'
        if source_change == 'missing_shared':
            (frozen / 'experiments/resident_config.py').unlink()
        elif source_change == 'changed_shared':
            (frozen / 'experiments/resident_config.py').write_text('changed dependency\n')
        manifest = dict(protocol=9, fabric=True, binaries=campaign.cfg['binaries'],
                        source_archive_sha256=campaign.cfg['source_archive_sha256'],
                        source_manifest_sha256=campaign.cfg['source_manifest_sha256'])
        supervisor.write_json(campaign.build / 'build-manifest.json', manifest)
        supervisor.write_json(campaign.build / 'plan.json', dict(protocol=manifest['protocol']))
        campaign.cfg['build_manifest_sha256'] = supervisor.sha(campaign.build / 'build-manifest.json')
        campaign.verification = root / 'verification'
        schema = campaign.verification / 'experiment/native-schema/perpetual'
        schema.mkdir(parents=True)
        supervisor.write_json(schema / 'manifest.json', dict(namespace='perpetual', backend='native'))
        supervisor.write_json(schema / 'shape.json', {})
        (schema / 'shape.yaml').write_text('{}\n')
        campaign.scripts = Path(__file__).resolve().parents[1] / 'sui-checkpoint'
        campaign.nodes = copy.deepcopy(campaign.cfg['nodes'])
        campaign.state, campaign.s3 = dict(uploads=[]), object()
        commands, uploads = [], captured_uploads if captured_uploads is not None else []
        def execute(name, command, timeout):
            commands.append([str(part) for part in command])
            subprocess.run(command, check=True, capture_output=True, text=True, timeout=timeout)
        def capture(s3, path, cfg, key):
            uploads.append(Path(path))
            return dict(bucket=cfg['bucket'], key=key, sha256=supervisor.sha(path))
        campaign.run_command = execute
        with patch.object(supervisor, 'upload', side_effect=capture):
            campaign.prepare()
        return campaign, commands, uploads

    def test_cloud_prepare_passes_real_registered_profile_to_existing_producer(self):
        import yaml
        with tempfile.TemporaryDirectory() as temporary:
            profile = resident_profile()
            campaign, commands, uploads = self.prepare_campaign(Path(temporary), profile)
            command = commands[0]
            policy = Path(command[command.index('--resident-reads') + 1])
            self.assertEqual(policy, campaign.cell_root / 'resident-reads.json')
            self.assertEqual(supervisor.read_json(policy), profile)
            self.assertEqual(supervisor.read_json(campaign.results / 'resident-reads.json'), profile)
            self.assertEqual(campaign.cell['resident_reads'], profile)
            self.assertEqual(campaign.external['resident_reads'], profile)
            prepared = supervisor.read_json(campaign.deployment / 'config.json')
            for database in prepared['databases'].values():
                self.assertEqual(database['tls']['transport'], transport_for_case(campaign.cell['transport']))
            self.assertEqual(campaign.external['owner_transports'], select_transports(
                transport_for_case(campaign.cell['transport']), None, 5))
            self.assertEqual(len(uploads), 5)
            for archive in uploads:
                with tarfile.open(archive) as stream:
                    launches = [member for member in stream.getmembers() if member.name.endswith('/launch.yaml')]
                    self.assertEqual(len(launches), 1)
                    with stream.extractfile(launches[0]) as source:
                        launch = yaml.safe_load(source)
                    self.assertEqual(launch['tls']['transport'],
                                     transport_for_case(campaign.cell['transport']))
                    self.assertEqual(launch['tls']['cert'], '')

    def test_tcp_storage_artifacts_contain_the_same_complete_source_bound_worker_runner(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            campaign, _, archives = self.prepare_campaign(root, None, 'tcp')
            records = supervisor.read_json(campaign.build / 'source-manifest.json')[
                'tidehunter-sharded-wal']['files']
            self.assertEqual(len(archives), 5)
            for index, archive in enumerate(archives):
                installed = root / f'installed-worker-{index}'
                supervisor.safe_extract(archive, installed)
                runner = installed / 'runner'
                supervisor.validate_runner_helpers(runner, records)
                self.assertEqual(supervisor.sha(runner / 'resident_config.py'),
                                 records['experiments/resident_config.py']['sha256'])
                self.assertEqual(supervisor.sha(runner / 'portable_transport.py'),
                                 records['experiments/portable_transport.py']['sha256'])
                self.assertFalse(any(path.name.startswith('test_') for path in runner.iterdir()))
                self.assertEqual(supervisor.sha(installed / 'cloud_node.py'),
                                 records['experiments/sui-checkpoint/cloud_node.py']['sha256'])
                if index == 0:
                    self.assertEqual(supervisor.validate_runner_entrypoint(runner, records)['exit_code'], 0)

    def test_storage_dependency_source_failures_are_rejected_before_any_payload_upload(self):
        for change in ('missing_shared', 'changed_shared'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                uploads = []
                with self.assertRaises(ValueError):
                    self.prepare_campaign(Path(temporary), None, 'tcp',
                                          source_change=change, captured_uploads=uploads)
                self.assertEqual(uploads, [])

    def test_actual_coordinator_and_manifest_policy_cannot_be_silently_omitted_or_changed(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign, _, _ = self.prepare_campaign(Path(temporary), resident_profile())
            prepared = supervisor.read_json(campaign.deployment / 'config.json')
            namespace = next(iter(prepared['databases']))
            for field in ('coordinator-missing', 'coordinator-changed', 'manifest-missing',
                          'manifest-changed', 'cell-missing'):
                config, external, cell = copy.deepcopy((prepared, campaign.external, campaign.cell))
                if field == 'coordinator-missing':
                    config['databases'][namespace]['tls'].pop('resident_reads')
                elif field == 'coordinator-changed':
                    config['databases'][namespace]['tls']['resident_reads']['max_handles'] += 1
                elif field == 'manifest-missing':
                    external.pop('resident_reads')
                elif field == 'manifest-changed':
                    external['resident_reads']['max_handles'] += 1
                else:
                    cell.pop('resident_reads')
                with self.subTest(field=field), self.assertRaises(ValueError):
                    supervisor.validate_prepared_cell(campaign.cfg, cell, config, external)

    def test_missing_and_disabled_native_profile_keep_rpc_deployment_and_old_metadata(self):
        for profile in (None, supervisor.resident_read_config()):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as temporary:
                campaign, commands, _ = self.prepare_campaign(Path(temporary), profile)
                self.assertNotIn('--resident-reads', commands[0])
                self.assertFalse((campaign.cell_root / 'resident-reads.json').exists())
                self.assertNotIn('resident_reads', campaign.cell)
                prepared = supervisor.read_json(campaign.deployment / 'config.json')
                self.assertTrue(all('resident_reads' not in database['tls']
                                    for database in prepared['databases'].values()))
                self.assertEqual(campaign.external['resident_reads'], supervisor.resident_read_config())

    def test_enabled_profile_does_not_change_actual_tcp_tls_deployment(self):
        import yaml
        with tempfile.TemporaryDirectory() as temporary:
            campaign, commands, uploads = self.prepare_campaign(Path(temporary), resident_profile(), 'tcp')
            self.assertNotIn('--resident-reads', commands[0])
            self.assertNotIn('resident_reads', campaign.cell)
            prepared = supervisor.read_json(campaign.deployment / 'config.json')
            database = next(iter(prepared['databases'].values()))
            self.assertEqual(database['tls']['transport'], transport_for_case('tcp'))
            self.assertNotIn('resident_reads', database['tls'])
            self.assertTrue(all(database['tls'][name] for name in ('ca', 'cert', 'key')))
            for archive in uploads:
                with tarfile.open(archive) as stream:
                    names = stream.getnames()
                    self.assertIn('tls/ca.der', names)
                    launch_name = next(name for name in names if name.endswith('/launch.yaml'))
                    with stream.extractfile(launch_name) as source:
                        launch = yaml.safe_load(source)
                    self.assertNotIn('resident_reads', launch['node'])
                    self.assertEqual(launch['tls']['transport'], transport_for_case('tcp'))
            self.assertEqual(campaign.external['resident_reads'], supervisor.resident_read_config())
            changed = copy.deepcopy(prepared)
            next(iter(changed['databases'].values()))['tls']['resident_reads'] = resident_profile()
            with self.assertRaises(ValueError):
                supervisor.validate_prepared_cell(campaign.cfg, campaign.cell, changed, campaign.external)


class LifecycleTests(unittest.TestCase):
    def test_admission_requires_exact_owned_inventory_and_input_scope(self):
        cfg = example_config()
        root, ids = supervisor.validate_cfg(cfg)
        self.assertEqual(root, Path(cfg['run_root']))
        self.assertEqual(len(ids), 6)
        for field, value in [('terminate_on_finish', False), ('s3_prefix', 'foreign'),
                             ('checkpoint_digest', 'other'), ('transports', ['fabric-tcp']),
                             ('verification_bundle', {'key': 'foreign/inputs/local.tar.gz', 'sha256': 'a' * 64})]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                supervisor.validate_cfg(cfg | {field: value})
        bad_nodes = copy.deepcopy(cfg['nodes'])
        bad_nodes[0]['instance_id'] = cfg['coordinator_instance_id']
        with self.assertRaises(ValueError):
            supervisor.validate_cfg(cfg | {'nodes': bad_nodes})

    def test_full_matrix_deadline_accepts_exact_two_hours_and_preserves_export_window(self):
        cfg = example_config()
        self.assertEqual(cfg['repetitions'], 4)
        self.assertEqual(cfg['read_profile_limits'], [0, 4096, 4096, 0])
        now = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
        for left in (3600, 7200, cfg['export_reserve_seconds'] + 180.000001):
            candidate = cfg | {'deadline_utc': (now + timedelta(seconds=left)).isoformat()}
            with self.subTest(left=left), patch.object(kv_guest.time, 'time', return_value=now.timestamp()):
                root, ids = supervisor.validate_cfg(candidate)
                self.assertEqual(root, Path(cfg['run_root']))
                self.assertEqual(len(ids), 6)
        for left in (7200.000001, cfg['export_reserve_seconds'] + 180,
                     cfg['export_reserve_seconds'], 0, -1):
            candidate = cfg | {'deadline_utc': (now + timedelta(seconds=left)).isoformat()}
            with self.subTest(left=left), patch.object(kv_guest.time, 'time', return_value=now.timestamp()):
                with self.assertRaises(ValueError):
                    supervisor.validate_cfg(candidate)

    def test_kv_guest_admission_accepts_old_and_two_hour_absolute_deadlines(self):
        cfg = example_config() | {'campaign': 'overlap', 'repetitions': 2}
        now = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
        for left in (1800, 3600, 7200):
            candidate = cfg | {'deadline_utc': (now + timedelta(seconds=left)).isoformat()}
            with self.subTest(left=left), patch.object(kv_guest.time, 'time', return_value=now.timestamp()):
                root, ids = kv_guest.validate_cfg(candidate)
                self.assertEqual(root, Path(cfg['run_root']))
                self.assertEqual(len(ids), 6)
        for left in (7200.000001, 0, -1):
            candidate = cfg | {'deadline_utc': (now + timedelta(seconds=left)).isoformat()}
            with self.subTest(left=left), patch.object(kv_guest.time, 'time', return_value=now.timestamp()):
                with self.assertRaises(ValueError):
                    kv_guest.validate_cfg(candidate)

    def test_malformed_deadline_or_export_reserve_is_not_admitted(self):
        cfg = example_config()
        for value in ('not-a-deadline', '2026-10-22T12:30:00', '2026-10-22T12:30:00+01:00'):
            with self.subTest(deadline=value), self.assertRaises(ValueError):
                supervisor.validate_cfg(cfg | {'deadline_utc': value})
        for reserve in (299, 901, 600.0, True, None, '600'):
            with self.subTest(reserve=reserve), self.assertRaises(ValueError):
                supervisor.validate_cfg(cfg | {'export_reserve_seconds': reserve})

    def test_replay_deadline_default_and_one_hour_maximum_do_not_follow_fleet_lifetime(self):
        cfg = example_config()
        self.assertEqual(cfg['replay_deadline_seconds'], 900)
        without_replay = {name: value for name, value in cfg.items() if name != 'replay_deadline_seconds'}
        supervisor.validate_cfg(without_replay)
        supervisor.validate_cfg(cfg | {'replay_deadline_seconds': 3600})
        for seconds in (29, 3600.000001, 7200):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                supervisor.validate_cfg(cfg | {'replay_deadline_seconds': seconds})

    def test_campaign_work_deadline_preserves_original_absolute_expiry(self):
        cfg = example_config()
        original = cfg['deadline_utc']
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(supervisor, 'validate_cfg', return_value=(Path(temporary), ['i-1'])), \
                    patch.object(supervisor.boto3, 'Session'):
                campaign = supervisor.SuiCampaign(cfg)
            try:
                self.assertEqual(cfg['deadline_utc'], original)
                self.assertEqual(
                    (datetime.fromisoformat(original) - datetime.fromisoformat(campaign.deadline)).total_seconds(),
                    cfg['export_reserve_seconds'])
            finally:
                campaign.lock.close()

    def test_bootstrap_deadline_cannot_be_extended_even_by_a_fractional_second(self):
        cfg = example_config()
        record = dict(run_id=cfg['run_id'], deadline_utc=cfg['deadline_utc'],
                      identity=dict(instanceId=cfg['coordinator_instance_id'], accountId=cfg['account'],
                                    instanceType=cfg['instance_type'], region=cfg['region']))
        for seconds in (.000001, .5, 1):
            later = (datetime.fromisoformat(cfg['deadline_utc']) + timedelta(seconds=seconds)).isoformat()
            with self.subTest(seconds=seconds), patch.object(supervisor, 'read_json', return_value=record), \
                    patch.object(supervisor.subprocess, 'run') as command:
                with self.assertRaisesRegex(ValueError, 'exceeds original bootstrap TTL'):
                    supervisor.bootstrap_gate(Path(cfg['run_root']), cfg | dict(deadline_utc=later),
                                              cfg['coordinator_instance_id'])
                command.assert_not_called()

    def campaign(self, root):
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.root, campaign.results = root, root / 'results'
        campaign.campaign_results = campaign.results
        campaign.cfg = example_config()
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.ids = [campaign.cfg['coordinator_instance_id'], *(node['instance_id'] for node in campaign.cfg['nodes'])]
        campaign.nodes = campaign.cfg['nodes']
        campaign.state = dict(complete=False, failures=[])
        campaign.attempted_nodes, campaign.collected_final = set(), set()
        campaign.child = None
        campaign.ec2 = Mock()
        for name in ('inventory', 'probe_export', 'progress', 'inputs', 'prepare', 'admit_node',
                     'install_node', 'collect_node', 'export', 'check_owned_instances'):
            setattr(campaign, name, Mock())
        def parallel(action, function, nodes=None):
            for node in campaign.nodes if nodes is None else nodes:
                function(node)
        campaign.parallel_nodes = parallel
        campaign.replay = Mock()
        campaign.run_matrix = Mock(side_effect=lambda: campaign.state.update(complete=True))
        return campaign

    def execute_offline(self, campaign):
        with patch.object(supervisor, 'evidence', return_value={}), patch.object(supervisor.signal, 'signal'), patch.object(supervisor.signal, 'setitimer'):
            campaign.execute()

    def test_input_admission_failure_still_exports_and_terminates_all_six(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.inputs.side_effect = ValueError('local replay not verified')
            with self.assertRaisesRegex(RuntimeError, 'campaign incomplete'):
                self.execute_offline(campaign)
            campaign.export.assert_called_once()
            campaign.replay.assert_not_called()
            campaign.check_owned_instances.assert_called_once_with(record=False)
            campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)
            self.assertFalse(campaign.state['complete'])

    def test_kv_failure_cannot_preserve_sui_success_or_skip_fleet_termination(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.cfg['kv_suite'] = {'repetitions': 2, 'diagnostics': True}
            with patch.object(supervisor, 'run_kv_suite', side_effect=ValueError('cold IO evidence missing')):
                with self.assertRaisesRegex(RuntimeError, 'campaign incomplete'):
                    self.execute_offline(campaign)
            self.assertFalse(campaign.state['complete'])
            self.assertIn('cold IO evidence missing', campaign.state['failures'][0])
            campaign.export.assert_called_once()
            campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)

    def test_export_failure_cannot_bypass_owned_fleet_termination(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.export.side_effect = OSError('export unavailable')
            with self.assertRaisesRegex(OSError, 'export unavailable'):
                self.execute_offline(campaign)
            campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)

    def test_replay_cleanup_failure_still_exports_nodes_and_terminates(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.attempted_nodes = {0}
            campaign.stop_process = Mock(side_effect=OSError('replay cleanup unavailable'))
            with self.assertRaisesRegex(RuntimeError, 'campaign incomplete'):
                self.execute_offline(campaign)
            campaign.collect_node.assert_called_once_with(campaign.nodes[0], final=True, logs=True)
            campaign.export.assert_called_once()
            campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)
            self.assertIn('replay cleanup:', campaign.state['failures'][0])

    def test_foreign_inventory_blocks_termination(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.check_owned_instances.side_effect = ValueError('refusing foreign instance')
            with self.assertRaisesRegex(ValueError, 'foreign instance'):
                self.execute_offline(campaign)
            campaign.export.assert_called_once()
            campaign.ec2.terminate_instances.assert_not_called()

    def test_final_archive_admission_failure_clears_success_before_export(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.attempted_nodes = {0}
            def collect(node, final=False, logs=False):
                if logs:
                    raise ValueError('missing namespace log')
            campaign.collect_node.side_effect = collect
            with self.assertRaisesRegex(RuntimeError, 'campaign incomplete'):
                self.execute_offline(campaign)
            self.assertFalse(campaign.state['complete'])
            campaign.export.assert_called_once()
            campaign.ec2.terminate_instances.assert_called_once_with(InstanceIds=campaign.ids)

    def test_ssm_client_disables_sdk_retries_and_unknown_send_is_not_repeated(self):
        with tempfile.TemporaryDirectory() as temporary:
            cfg = example_config()
            root = Path(temporary)
            session = Mock()
            with patch.object(supervisor, 'validate_cfg', return_value=(root, ['i-1'])), patch.object(supervisor.boto3, 'Session', return_value=session):
                campaign = supervisor.SuiCampaign(cfg)
            try:
                ssm_calls = [call for call in session.client.call_args_list if call.args == ('ssm',)]
                self.assertEqual(len(ssm_calls), 1)
                self.assertEqual(ssm_calls[0].kwargs['config'].retries['total_max_attempts'], 1)
                campaign.ssm = Mock()
                campaign.ssm.send_command.side_effect = TimeoutError('HTTP acknowledgement lost')
                with self.assertRaisesRegex(TimeoutError, 'acknowledgement lost'):
                    campaign.ssm_one(cfg['nodes'][0], 'start', 'owned-command')
                campaign.ssm.send_command.assert_called_once()
                campaign.ssm.get_command_invocation.assert_not_called()
            finally:
                campaign.lock.close()


if __name__ == '__main__':
    unittest.main()
