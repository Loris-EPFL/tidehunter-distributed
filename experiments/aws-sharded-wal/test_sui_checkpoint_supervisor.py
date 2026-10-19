"""Offline regression gates for real-Sui admission, evidence, and teardown."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
import sys
from unittest.mock import Mock, patch

import sui_checkpoint as supervisor


def encoded(value):
    return (json.dumps(value, indent=2) + '\n').encode()


def example_config():
    cfg = supervisor.schema()['config']
    cfg['deadline_utc'] = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
        cfg[name]['sha256'] = 'a' * 64
    for name in ('build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256'):
        cfg[name] = 'a' * 64
    cfg['binaries'] = {name: 'b' * 64 for name in supervisor.BINARIES}
    return cfg

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
    cloud = dict(run_id=cfg['run_id'], cell_id=common['cell_id'], transport=transport, deadline_utc=cfg['deadline_utc'], root=root,
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
        'build_manifest_sha256', 'config_sha256', 'checkpoint_digest', 'cell_id', 'transport')}
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
                for transport in ('efa', 'efa-direct'):
                    output = root / transport
                    output.mkdir()
                    supervisor.fabric_preflight(output, transport)
                    supervisor.validate_provider((output / 'efa-provider.txt').read_text(), transport)
                executable.write_text('#!' + sys.executable + '\nprint("provider: efa\\n fabric: efa\\n type: FI_EP_RDM")\n')
                with self.assertRaises(ValueError):
                    supervisor.fabric_preflight(root / 'efa-direct', 'efa-direct')
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

    def test_four_repetition_deadline_is_bounded_to_one_hour(self):
        cfg = example_config()
        self.assertEqual(cfg['repetitions'], 4)
        self.assertEqual(cfg['read_profile_limits'], [0, 4096, 4096, 0])
        with patch.object(supervisor, 'remaining', return_value=3600):
            supervisor.validate_cfg(cfg)
        for left in (3600.001, cfg['export_reserve_seconds'] + 180, 0):
            with self.subTest(left=left), patch.object(supervisor, 'remaining', return_value=left):
                with self.assertRaisesRegex(ValueError, 'bounded export reserve'):
                    supervisor.validate_cfg(cfg)

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
