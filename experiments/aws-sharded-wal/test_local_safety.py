"""No-cloud regression gates for destructive deployment boundaries."""
from datetime import datetime, timedelta, timezone
import copy
import csv
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch


def module(name):
    path = Path(__file__).with_name(name + '.py')
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


guest = module('guest')
bootstrap = module('bootstrap')
analysis = module('analyze_cloud')


class SafetyTests(unittest.TestCase):
    def config(self):
        return {'run_id': 'th-0123456789abcdef', 'account': '354775971805',
                'run_root': '/mnt/tidehunter/th-0123456789abcdef',
                's3_prefix': 'th-0123456789abcdef',
                'availability_zone': 'us-east-1a',
                'coordinator_instance_id': 'i-00000000000000001',
                'deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
                'nodes': [{'partition': p, 'private_ip': f'172.31.32.{p+10}',
                           'instance_id': f'i-{p+2:017x}'} for p in range(5)]}

    def test_account_path_partition_and_lifetime_scope(self):
        cfg = self.config()
        root, ids = guest.validate_cfg(cfg)
        self.assertEqual(len(ids), 6)
        for key, value in [('run_root', '/mnt/other'), ('s3_prefix', 'unrelated'),
                           ('campaign', 'unknown'),
                           ('capacity_repetitions', 3), ('chunk_control_repetitions', 4),
                           ('deadline_utc', (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat())]:
            with self.assertRaises(ValueError):
                guest.validate_cfg(cfg | {key: value})
        nodes = cfg['nodes'].copy()
        nodes[-1] = nodes[0]
        with self.assertRaises(ValueError):
            guest.validate_cfg(cfg | {'nodes': nodes})

    def test_scalar_campaign_keeps_fixed_hardware_scope_and_termination(self):
        cfg = self.config() | dict(campaign='scalar-transport', efa=True,
                                  instance_type='g6.16xlarge', region='us-east-1',
                                  repetitions=3, export_reserve_seconds=600,
                                  terminate_on_finish=True)
        for scope in ('full', 'core', 'recovery'):
            guest.validate_cfg(cfg | {'checkpoint_scope': scope})
        for field, value in [('checkpoint_scope', 'capacity'), ('repetitions', 5),
                             ('efa', False), ('instance_type', 'c6id.32xlarge'),
                             ('export_reserve_seconds', 300), ('terminate_on_finish', False)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                guest.validate_cfg(cfg | {field: value})

    def test_source_bound_process_rejects_changed_binary_before_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'binary'
            binary.write_bytes(b'changed executable')
            with patch.object(guest.subprocess, 'Popen') as launch:
                with self.assertRaisesRegex(ValueError, 'source-bound'):
                    guest.run_recorded([str(binary)], root / 'process', self.config()['deadline_utc'],
                                       expected_binary_sha256='0' * 64)
            launch.assert_not_called()

    def test_scalar_completion_rejects_missing_case_before_success(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = self.config() | {'campaign': 'scalar-transport', 'checkpoint_scope': 'core'}
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.state = {'completed_cases': []}
        campaign.run_case = Mock()
        with tempfile.TemporaryDirectory() as temporary:
            campaign.results = Path(temporary)
            with patch('analyze_cloud.validate_qualification', return_value={
                    'passed': True, 'errors': {}}):
                with self.assertRaisesRegex(ValueError, 'completion identities'):
                    campaign.execute_progress()

    def test_scalar_completion_rejects_invalid_evidence_after_all_cases_finish(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = self.config() | {'campaign': 'scalar-transport', 'checkpoint_scope': 'core'}
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.state = {'completed_cases': []}

        def finish(label, shape, rep, storage, progress_case):
            campaign.state['completed_cases'].extend(
                {'case_id': progress_case['case_id'], 'backend': mode}
                for mode in progress_case['modes'])

        campaign.run_case = finish
        with tempfile.TemporaryDirectory() as temporary:
            campaign.results = Path(temporary)
            with patch('analyze_cloud.validate_qualification', return_value={'passed': True, 'errors': {}}), \
                 patch.object(guest.scalar_transport, 'read_cloud_case', side_effect=ValueError('missing node evidence')):
                with self.assertRaisesRegex(ValueError, 'missing node evidence'):
                    campaign.execute_progress()

    def test_floor_campaign_keeps_fixed_scope_hardware_and_termination(self):
        cfg = self.config() | dict(campaign='transport-floor', efa=True,
                                  instance_type='g6.16xlarge', region='us-east-1',
                                  repetitions=3, export_reserve_seconds=600,
                                  terminate_on_finish=True)
        for instance_type in ('g6.16xlarge', 'c6id.32xlarge'):
            for scope in ('full', 'core', 'recovery'):
                with self.subTest(instance_type=instance_type, scope=scope):
                    guest.validate_cfg(cfg | {'instance_type': instance_type, 'checkpoint_scope': scope})
        for field, value in [('checkpoint_scope', 'capacity'), ('repetitions', 5),
                             ('efa', False), ('instance_type', 'c6id.16xlarge'),
                             ('region', 'us-west-2'), ('export_reserve_seconds', 300),
                             ('availability_zone', ''), ('availability_zone', 'us-west-2a'),
                             ('terminate_on_finish', False)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                guest.validate_cfg(cfg | {field: value})

    def test_checkpoint_and_cluster_ram_keep_g6_only(self):
        for campaign_name in ('checkpoint-async', 'cluster-ram'):
            cfg = self.config() | dict(campaign=campaign_name, efa=True,
                                      instance_type='g6.16xlarge', region='us-east-1',
                                      repetitions=5, export_reserve_seconds=600)
            with self.subTest(campaign=campaign_name):
                guest.validate_cfg(cfg)
                with self.assertRaisesRegex(ValueError, 'reviewed hardware'):
                    guest.validate_cfg(cfg | {'instance_type': 'c6id.32xlarge'})

    def test_floor_admission_binds_selected_type_and_zone_but_teardown_does_not(self):
        for selected_type in ('g6.16xlarge', 'c6id.32xlarge'):
            with self.subTest(instance_type=selected_type), tempfile.TemporaryDirectory() as temporary:
                campaign = object.__new__(guest.Campaign)
                campaign.cfg = self.config() | dict(campaign='transport-floor',
                    instance_type=selected_type, availability_zone='us-east-1b')
                campaign.ids = [campaign.cfg['coordinator_instance_id']] + [
                    node['instance_id'] for node in campaign.cfg['nodes']]
                campaign.results = Path(temporary)
                tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark',
                        'RunId': campaign.cfg['run_id']}
                fleet = [{'InstanceId': iid, 'InstanceType': selected_type,
                          'Placement': {'AvailabilityZone': campaign.cfg['availability_zone']},
                          'Tags': [{'Key': key, 'Value': value} for key, value in tags.items()]}
                         for iid in campaign.ids]
                campaign.ec2 = Mock()
                campaign.ec2.describe_instances.return_value = {
                    'Reservations': [{'OwnerId': campaign.cfg['account'], 'Instances': fleet}]}
                campaign.check_owned_instances()
                for field, value in (
                        ('InstanceType', 'g6.16xlarge' if selected_type == 'c6id.32xlarge' else 'c6id.32xlarge'),
                        ('Placement', {'AvailabilityZone': 'us-east-1a'})):
                    original = fleet[-1][field]
                    fleet[-1][field] = value
                    with self.subTest(field=field), self.assertRaises(ValueError):
                        campaign.check_owned_instances()
                    campaign.check_owned_instances(record=False)
                    fleet[-1][field] = original
                fleet[-1]['Tags'] = []
                with self.assertRaisesRegex(ValueError, 'foreign instance'):
                    campaign.check_owned_instances(record=False)

    def test_floor_completion_requires_every_floor_and_final_evidence_audit(self):
        for defect in ('missing-floor', 'invalid-floor', 'duplicate-floor'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                campaign = object.__new__(guest.Campaign)
                campaign.cfg = self.config() | {'campaign': 'transport-floor', 'checkpoint_scope': 'core'}
                campaign.deadline = campaign.cfg['deadline_utc']
                campaign.results = Path(temporary)
                campaign.state = {'completed_cases': []}

                def finish(label, shape, rep, storage, progress_case):
                    if defect == 'missing-floor' and progress_case.get('floor'):
                        return
                    rows = [{'case_id': progress_case['case_id'], 'backend': mode}
                            for mode in progress_case['modes']]
                    campaign.state['completed_cases'].extend(rows)
                    if defect == 'duplicate-floor' and progress_case.get('floor'):
                        campaign.state['completed_cases'].extend(rows)

                def audit(root, name, spec, mode):
                    if spec.get('floor'):
                        raise ValueError('floor evidence rejected')

                campaign.run_case = finish
                with patch('analyze_cloud.validate_qualification', return_value={'passed': True, 'errors': {}}), \
                     patch.object(guest.transport_floor, 'read_cloud_case', side_effect=audit) as reader:
                    expected = 'floor evidence rejected' if defect == 'invalid-floor' else 'completion identities'
                    with self.assertRaisesRegex(ValueError, expected):
                        campaign.execute_progress()
                    if defect != 'invalid-floor':
                        reader.assert_not_called()

    def test_floor_uses_prepared_main_nodes_and_hash_bound_external_client(self):
        for defect in (None, 'missing-shape', 'client-failure'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                campaign = object.__new__(guest.Campaign)
                campaign.cfg = self.config() | {'campaign': 'transport-floor', 'checkpoint_scope': 'core', 'efa': True}
                campaign.root = Path(temporary)
                campaign.results = campaign.root / 'results'
                campaign.results.mkdir()
                campaign.deadline = campaign.cfg['deadline_utc']
                campaign.cloud_builds = {'current': {'binary_sha256': 'main-hash', 'floor_binary_sha256': 'floor-hash'}}
                campaign.state = {'completed_cases': []}
                campaign.qualification_passed = True
                campaign.progress = Mock()
                campaign.ssm_nodes = Mock()
                campaign.collect_node_results = Mock()
                campaign.controller_evidence = Mock(return_value={})
                campaign.s3 = None
                campaign.active_payload = None
                spec = next(item for item in guest.transport_floor.main_cases('core')
                            if item.get('floor') and item['transport'] == 'efa' and item['polling'] == 'busy')
                commands = []

                def run(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
                    commands.append(command)
                    if '--prepare-external-case' in command:
                        self.assertEqual(Path(command[0]).name, 'sharded_wal_bench')
                        self.assertEqual(expected_binary_sha256, 'main-hash')
                        self.assertEqual(command[command.index('--transport') + 1], 'efa')
                        self.assertEqual(command[command.index('--tcp-polling') + 1], 'busy')
                        root = Path(command[command.index('--prepare-external-case') + 1])
                        names = ['remote.yaml', 'tls/ca.der']
                        if defect != 'missing-shape':
                            names.append('shape.yaml')
                        for p in range(5):
                            names += [f'node-{p}/launch.yaml', f'tls/tidehunter-node-{p}.der',
                                      f'tls/tidehunter-node-{p}.pk8']
                        for name in names:
                            path = root / name
                            path.parent.mkdir(parents=True, exist_ok=True)
                            path.write_text('prepared fixture\n')
                    else:
                        self.assertEqual(Path(command[0]).name, 'sharded_wal_transport_bench')
                        self.assertEqual(expected_binary_sha256, 'floor-hash')
                        self.assertEqual(command[command.index('--transport') + 1], 'efa')
                        self.assertEqual(command[command.index('--polling') + 1], 'busy')
                        self.assertNotIn('--backend', command)
                        remote = Path(command[command.index('--remote-config') + 1])
                        self.assertTrue(remote.with_name('shape.yaml').is_file())
                        if defect == 'client-failure':
                            raise RuntimeError('floor process failed')
                        output = Path(command[command.index('--output') + 1])
                        output.mkdir()
                        (output / 'COMPLETE').write_text('verified payload\n')

                with patch.object(guest, 'run_recorded', side_effect=run), \
                     patch.object(guest, 'upload', return_value={'sha256': 'archive-hash'}):
                    args = (spec['build'], (spec['value'], spec['batch'], spec['window']), spec['rep'])
                    if defect:
                        with self.assertRaises(FileNotFoundError if defect == 'missing-shape' else RuntimeError):
                            campaign.run_case(*args, storage=True, progress_case=spec)
                    else:
                        campaign.run_case(*args, storage=True, progress_case=spec)
                if defect == 'missing-shape':
                    campaign.ssm_nodes.assert_not_called()
                    campaign.collect_node_results.assert_not_called()
                else:
                    payloads = [call.args[0] for call in campaign.ssm_nodes.call_args_list]
                    self.assertEqual([p['action'] for p in payloads], ['start', 'stop'])
                    self.assertTrue(all(p['build'] == 'current' and p['binary_sha256'] == 'main-hash'
                                        for p in payloads))
                    campaign.collect_node_results.assert_called_once_with(spec['case_id'])
                    self.assertIsNone(campaign.active_payload)
                self.assertEqual(len(campaign.state['completed_cases']), 0 if defect else 1)
                if not defect:
                    self.assertEqual(campaign.state['completed_cases'][0]['transport'], 'efa')
                    self.assertEqual(campaign.state['completed_cases'][0]['backend'], 'floor')

    def capacity_case(self, root):
        # Exact relevant serde YAML copied from the frozen generator's actual
        # group-capacity/group-1m-r0 configs. The batch cap must not change.
        settings = ('batch_limits:\n  max_operations: 32\n  max_encoded_bytes: 327680\n'
                    'group_limits:\n  max_batches: 32\n  max_encoded_bytes: 1048568\n'
                    'fragment_bytes: 1048576\nwal_file_bytes: 134217728\n')
        paths = [root / 'remote.yaml'] + [root / f'node-{p}/launch.yaml' for p in range(5)]
        for index, path in enumerate(paths):
            path.parent.mkdir(parents=True, exist_ok=True)
            indent = '  ' if index == 0 else '    '
            prefix = 'storage:\n' if index == 0 else 'node:\n  partition:\n'
            path.write_text(prefix + ''.join(indent + line + '\n' for line in settings.splitlines()))
        return paths

    def test_capacity_transform_changes_all_six_configs_only_at_expected_fields(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.capacity_case(root)
            originals = [p.read_text() for p in paths]
            receipt = guest.prepare_capacity_4m(root)
            self.assertEqual(receipt['aggregate_value_mapping_allowance_bytes'], 60 * 1024 * 1024)
            self.assertFalse(receipt['native_comparison'])
            for path, original in zip(paths, originals):
                expected = original.replace('fragment_bytes: 1048576', 'fragment_bytes: 4194304')
                expected = expected.replace('max_encoded_bytes: 1048568', 'max_encoded_bytes: 4194296')
                self.assertEqual(path.read_text(), expected)
                self.assertIn('max_encoded_bytes: 327680', path.read_text())

    def test_capacity_transform_validates_entire_set_before_any_write(self):
        for invalid in ['    fragment_bytes: 1048576\n', '']:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                paths = self.capacity_case(root)
                last = paths[-1]
                text = last.read_text()
                last.write_text(text + invalid if invalid else text.replace('max_encoded_bytes: 1048568', 'max_encoded_bytes: 999'))
                before = [p.read_bytes() for p in paths]
                with self.assertRaises(ValueError):
                    guest.prepare_capacity_4m(root)
                self.assertEqual([p.read_bytes() for p in paths], before)

    def test_efa_hardware_requires_an_exact_blank_drive_inventory(self):
        for count in (2, 4):
            drives = [{'path': f'/dev/nvme{n}n1', 'type': 'disk',
                       'model': 'Amazon EC2 NVMe Instance Storage', 'fstype': None,
                       'mountpoints': [None]} for n in range(1, count + 1)]
            self.assertEqual(bootstrap.blank_instance_disk(list(reversed(drives)), count), '/dev/nvme1n1')
            for wrong in [drives[:-1], drives + [drives[0]], [drives[0]] * count,
                          drives[:-1] + [drives[-1] | {'fstype': 'ext4'}],
                          drives[:-1] + [drives[-1] | {'mountpoints': ['/mnt/research']}]]:
                with self.subTest(count=count, devices=wrong), self.assertRaises(ValueError):
                    bootstrap.blank_instance_disk(wrong, count)

    def test_efa_matrix_covers_both_sizes_scalar_batched_and_cold(self):
        cases = guest.efa_cases()
        self.assertEqual(len(cases) * 2, 68)
        efa_warm = [(shape, rep) for label, shape, rep, options in cases
                    if options.get('transport') == 'efa' and not options.get('cold')]
        self.assertEqual(len(efa_warm), 12)
        self.assertEqual({shape for shape, _ in efa_warm},
                         {(1024, 1, 1), (1024, 32, 32), (8192, 1, 1), (8192, 32, 32)})
        self.assertEqual(sum(options.get('cold', False) for *_, options in cases), 6)
        self.assertTrue(all(label == 'current' for label, _, _, options in cases if options.get('transport') == 'efa'))

    def test_never_format_ebs_used_or_ambiguous_device(self):
        blank = {'path': '/dev/nvme1n1', 'type': 'disk',
                 'model': 'Amazon EC2 NVMe Instance Storage', 'fstype': None,
                 'mountpoints': [None]}
        self.assertEqual(bootstrap.blank_instance_disk([blank]), '/dev/nvme1n1')
        for devices in [[blank | {'model': 'Amazon Elastic Block Store'}],
                        [blank | {'fstype': 'ext4'}], [blank | {'children': [{}]}],
                        [blank | {'mountpoints': ['/mnt/research']}], [blank, blank]]:
            with self.assertRaises(ValueError):
                bootstrap.blank_instance_disk(devices)

    def test_reject_escape_symlink_and_expanded_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, kind, size in [('../outside', tarfile.REGTYPE, 1),
                                     ('link', tarfile.SYMTYPE, 0),
                                     ('large', tarfile.REGTYPE, 32)]:
                archive = root / 'input.tar'
                with tarfile.open(archive, 'w') as stream:
                    item = tarfile.TarInfo(name)
                    item.type = kind
                    item.size = size
                    item.linkname = '/etc/passwd' if kind == tarfile.SYMTYPE else ''
                    stream.addfile(item, io.BytesIO(b'x' * size) if size else None)
                with self.assertRaises(ValueError):
                    guest.safe_extract(archive, root / 'output', max_bytes=16)
            self.assertFalse((root.parent / 'outside').exists())

    def test_cloud_termination_ownership_refuses_foreign_tags(self):
        cfg = self.config()
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = cfg
        _, campaign.ids = guest.validate_cfg(cfg)
        expected = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': cfg['run_id']}
        values = [{'InstanceId': iid, 'InstanceType': 'i4i.xlarge',
                   'Tags': [{'Key': k, 'Value': v} for k, v in expected.items()]} for iid in campaign.ids]
        class Ec2:
            def describe_instances(self, **kwargs):
                return {'Reservations': [{'OwnerId': cfg['account'], 'Instances': values}]}
        campaign.ec2 = Ec2()
        with tempfile.TemporaryDirectory() as temporary:
            campaign.results = Path(temporary)
            campaign.check_owned_instances()
            values[0]['Tags'][-1]['Value'] = 'other-user'
            for record in (True, False):
                with self.subTest(record=record), self.assertRaises(ValueError):
                    campaign.check_owned_instances(record=record)

    def test_export_preparation_failure_still_terminates_owned_fleet(self):
        for failure_stage, campaign_name in (
                (stage, name) for stage in ('progress', 'evidence', 'archive', 'disk-full', 'wrong-hardware')
                for name in ('scalar-transport', 'transport-floor')):
            with self.subTest(stage=failure_stage, campaign=campaign_name), tempfile.TemporaryDirectory() as temporary:
                campaign = object.__new__(guest.Campaign)
                campaign.cfg = self.config() | {'campaign': campaign_name, 'instance_type': 'g6.16xlarge'}
                campaign.ids = [campaign.cfg['coordinator_instance_id']] + [
                    node['instance_id'] for node in campaign.cfg['nodes']]
                campaign.root = campaign.results = Path(temporary)
                campaign.state = {'complete': False, 'failures': [], 'completed_cases': []}
                campaign.active_payload = None
                campaign.cloud_builds = {}
                tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark',
                        'RunId': campaign.cfg['run_id']}
                fleet = [{'InstanceId': iid,
                          'InstanceType': 'i4i.xlarge' if failure_stage == 'wrong-hardware' else 'g6.16xlarge',
                          'Placement': {'AvailabilityZone': 'us-east-1a'}, 'State': 'running',
                          'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}
                         for iid in campaign.ids]
                class Ec2:
                    def describe_instances(self, **kwargs):
                        return {'Reservations': [{'OwnerId': campaign.cfg['account'], 'Instances': fleet}]}

                    def terminate_instances(self, InstanceIds):
                        for instance in fleet:
                            if instance['InstanceId'] in InstanceIds:
                                instance['State'] = 'terminated'
                campaign.ec2 = Ec2()
                def progress(stage):
                    if stage.startswith('exporting-') and failure_stage == 'progress':
                        raise OSError('export preparation failed')
                with patch.object(campaign, 'probe_export'), patch.object(campaign, 'build_pair'), \
                     patch.object(campaign, 'execute_progress'), patch.object(campaign, 'progress', side_effect=progress), \
                     patch.object(guest, 'evidence', side_effect=OSError('export preparation failed')
                                  if failure_stage == 'evidence' else None, return_value={}), \
                     patch.object(guest.tarfile, 'open', side_effect=OSError('export preparation failed')), \
                     patch.object(guest, 'write_json', wraps=guest.write_json,
                                  side_effect=OSError('export preparation failed')
                                  if failure_stage == 'disk-full' else None):
                    with self.assertRaisesRegex(OSError, 'export preparation failed'):
                        campaign.execute()
                self.assertTrue(all(i['State'] == 'terminated' for i in fleet))
                if failure_stage == 'wrong-hardware':
                    self.assertFalse(campaign.state['complete'])
                    self.assertTrue(any('wrong hardware' in failure for failure in campaign.state['failures']))

    def test_final_export_transient_retries_stop_at_fixed_deadline(self):
        error = guest.EndpointConnectionError(endpoint_url='https://s3.invalid')
        with patch.object(guest, 'remaining', side_effect=[100, 80, 70, 29]), \
             patch.object(guest, 'upload', side_effect=error) as upload, \
             patch.object(guest.time, 'sleep') as sleep:
            with self.assertRaises(TimeoutError):
                guest.upload_final(None, None, {'deadline_utc': 'unchanged'}, 'key')
            self.assertEqual(upload.call_count, 2)
            sleep.assert_called_once_with(3)

    def test_final_export_permanent_failure_does_not_retry(self):
        with patch.object(guest, 'remaining', return_value=500), \
             patch.object(guest, 'upload', side_effect=guest.S3UploadFailedError('AccessDenied')) as upload, \
             patch.object(guest.time, 'sleep') as sleep:
            with self.assertRaises(guest.S3UploadFailedError):
                guest.upload_final(None, None, {'deadline_utc': 'unchanged'}, 'key')
            self.assertEqual(upload.call_count, 1)
            sleep.assert_not_called()

    def test_export_probe_checks_downloaded_bytes(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = self.config() | {'bucket': 'scoped-test-bucket'}
        class S3:
            def get_object(self, **kwargs):
                return {'Body': io.BytesIO(b'wrong object body')}
        campaign.s3 = S3()
        with tempfile.TemporaryDirectory() as temporary, patch.object(guest, 'upload', return_value={}):
            campaign.results = Path(temporary)
            with self.assertRaisesRegex(ValueError, 'read-back probe failed'):
                campaign.probe_export()
            self.assertFalse((campaign.results / 'export-probe-receipt.json').exists())

    def build_campaign(self, root, scalar=False, floor=False):
        campaign = object.__new__(guest.Campaign)
        campaign.root = root
        campaign.bundle = root / 'bundle'
        campaign.results = root / 'results'
        campaign.results.mkdir()
        campaign.cfg = self.config()
        if scalar or floor:
            campaign.cfg['campaign'] = 'transport-floor' if floor else 'scalar-transport'
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.cloud_builds = {}
        campaign.s3 = None
        campaign.progress = Mock()
        campaign.install_cloud_binary = Mock()
        files = {}
        for label in ['baseline', 'current']:
            directory = campaign.bundle / label
            directory.mkdir(parents=True)
            original = {'Cargo.toml': b'[workspace]\n', 'Cargo.lock': b'# locked\n',
                        'src.rs': label.encode()}
            if scalar or floor:
                original['tidehunter/examples/sharded_wal_recovery_bench.rs'] = (label + '-own-recovery').encode()
            if floor and label == 'current':
                original['tidehunter/examples/sharded_wal_transport_bench.rs'] = b'current-own-floor'
            with tarfile.open(directory / 'source.tar.gz', 'w:gz') as archive:
                for name, contents in original.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(contents)
                    member.mtime = 0  # Same normalized timestamps as deployed archives.
                    archive.addfile(member, io.BytesIO(contents))
            sources = {name: guest.hashlib.sha256(value).hexdigest() for name, value in original.items()}
            archived_binary = directory / 'sharded_wal_bench'
            archived_binary.write_bytes(('local-' + label).encode())
            manifest = {'source_sha256': sources,
                        'source_tree_sha256': guest.hashlib.sha256(
                            json.dumps(sources, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                        'binary_sha256': guest.sha(archived_binary)}
            if scalar or floor:
                manifest['features'] = 'experimental-fabric,test-utils'
            if floor:
                for kind in (('recovery', 'floor') if label == 'current' else ('recovery',)):
                    name = 'sharded_wal_' + ('transport' if kind == 'floor' else kind) + '_bench'
                    auxiliary = directory / name
                    auxiliary.write_text('local-' + label + '-' + kind)
                    manifest[kind + '_binary_sha256'] = guest.sha(auxiliary)
                    files[f'{label}/{name}'] = guest.sha(auxiliary)
            (directory / 'source-manifest.json').write_text(json.dumps(manifest))
            for name in ['source.tar.gz', 'source-manifest.json', 'sharded_wal_bench']:
                files[f'{label}/{name}'] = guest.sha(directory / name)
        (campaign.bundle / 'bundle-manifest.json').write_text(json.dumps({'files_sha256': files}))
        return campaign

    def fake_build_runner(self, targets, *, identical=False, missing_capability=False):
        def run(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
            output = Path(output)
            if '--manifest-path' in command:
                source = Path(command[command.index('--manifest-path') + 1]).parent
                self.assertEqual(source, cwd)
                self.assertEqual((source / 'src.rs').stat().st_mtime, 0)
                target = Path(env['CARGO_TARGET_DIR'])
                targets.append(target)
                binary = target / 'release/examples/sharded_wal_bench'
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b'identical' if identical else (source / 'src.rs').read_bytes())
            elif '--help' in command:
                text = '--remote-config'
                if 'current' in Path(command[0]).parts and not missing_capability:
                    text += ' --prepare-external-case --tcp-preparation-chunk-batches --cold-read-pass --transport-diagnostics'
                output.with_suffix('.stdout.log').write_text(text)
            elif '--prepare-external-case' in command:
                root = Path(command[command.index('--prepare-external-case') + 1])
                for name in ['remote.yaml'] + [f'node-{p}/launch.yaml' for p in range(5)]:
                    file = root / name
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.write_text('prepared: true\n')
            else:
                self.fail('unexpected build-gate subprocess')
        return run

    def test_archived_sources_build_in_fresh_independent_targets_before_distribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary))
            targets = []
            with patch.object(guest, 'run_recorded', side_effect=self.fake_build_runner(targets)), \
                 patch.object(guest.subprocess, 'check_output', return_value='rustc pinned\n'), \
                 patch.object(guest, 'upload', return_value={'verified': True}) as upload:
                campaign.build_pair()
            self.assertEqual(targets, [campaign.root / 'build-target/baseline', campaign.root / 'build-target/current'])
            self.assertEqual(upload.call_count, 2)
            self.assertEqual(campaign.install_cloud_binary.call_count, 2)
            proof = json.loads((campaign.results / 'cloud-build-pair-verified.json').read_text())
            self.assertEqual(len(proof['prepared_configuration_sha256']), 6)
            self.assertTrue(proof['distinct_binaries'])

    def test_scalar_recovery_uses_each_manifest_bound_example_not_overlay(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary), scalar=True)
            overlay = campaign.bundle / 'recovery-overlay/sharded_wal_recovery_bench.rs'
            overlay.parent.mkdir()
            overlay.write_bytes(b'incompatible current-only overlay')
            observed = []

            def run(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
                if command[0] == 'ldd':
                    return
                target = Path(env['CARGO_TARGET_DIR']) / 'release/examples'
                target.mkdir(parents=True, exist_ok=True)
                example = command[command.index('--example') + 1]
                if example == 'sharded_wal_recovery_bench':
                    frozen = Path(cwd) / 'tidehunter/examples/sharded_wal_recovery_bench.rs'
                    self.assertEqual(frozen.read_text(), Path(cwd).name + '-own-recovery')
                    self.assertIn('checkpoint_recovery_stats', command)
                    observed.append(Path(cwd).name)
                (target / example).write_text(Path(cwd).name + example)

            real_sha = guest.sha
            manifests = {label: json.loads((campaign.bundle / label / 'source-manifest.json').read_text())
                         for label in ('baseline', 'current')}
            with patch.multiple(guest.scalar_transport,
                                BASELINE_SOURCE_SHA256=manifests['baseline']['source_tree_sha256'],
                                BASELINE_BINARY_SHA256=manifests['baseline']['binary_sha256'],
                                CURRENT_SOURCE_SHA256=manifests['current']['source_tree_sha256']), \
                 patch.object(guest, 'run_recorded', side_effect=run), \
                 patch.object(guest.subprocess, 'check_output', return_value='pinned\n'), \
                 patch.object(guest, 'sha', side_effect=lambda path: 'fabric-hash'
                              if str(path).startswith('/opt/amazon/efa/') else real_sha(path)), \
                 patch.object(campaign, 'verify_cloud_pair'), \
                 patch.object(guest, 'upload', return_value={'verified': True}):
                campaign.build_pair()
            self.assertEqual(observed, ['baseline', 'current'])
            for label in observed:
                receipt = campaign.cloud_builds[label]
                manifest = json.loads((campaign.bundle / label / 'source-manifest.json').read_text())
                self.assertEqual(receipt['recovery_overlay_sha256'],
                                 manifest['source_sha256']['tidehunter/examples/sharded_wal_recovery_bench.rs'])

    def test_floor_build_is_current_only_manifest_bound_and_not_installed_on_nodes(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary), floor=True)
            observed = []

            def run(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
                if command[0] == 'ldd':
                    return
                target = Path(env['CARGO_TARGET_DIR'])
                example = command[command.index('--example') + 1]
                self.assertEqual(target, campaign.root / 'build-target' / Path(cwd).name)
                self.assertEqual(command[command.index('--features') + 1], 'experimental-fabric,test-utils')
                if example != 'sharded_wal_bench':
                    source = Path(cwd) / 'tidehunter/examples' / (example + '.rs')
                    self.assertEqual(source.read_text(), Path(cwd).name + (
                        '-own-floor' if example == 'sharded_wal_transport_bench' else '-own-recovery'))
                observed.append((Path(cwd).name, example))
                binary = target / 'release/examples' / example
                binary.parent.mkdir(parents=True, exist_ok=True)
                binary.write_text(Path(cwd).name + example)

            manifests = {label: json.loads((campaign.bundle / label / 'source-manifest.json').read_text())
                         for label in ('baseline', 'current')}
            real_sha = guest.sha
            with patch.multiple(guest.transport_floor,
                                BASELINE_SOURCE_SHA256=manifests['baseline']['source_tree_sha256'],
                                BASELINE_BINARY_SHA256=manifests['baseline']['binary_sha256'],
                                CURRENT_SOURCE_SHA256=manifests['current']['source_tree_sha256']), \
                 patch.object(guest, 'run_recorded', side_effect=run), \
                 patch.object(guest.subprocess, 'check_output', return_value='pinned\n'), \
                 patch.object(guest, 'sha', side_effect=lambda path: 'fabric-hash'
                              if str(path).startswith('/opt/amazon/efa/') else real_sha(path)), \
                 patch.object(campaign, 'verify_cloud_pair'), \
                 patch.object(guest, 'upload', return_value={'verified': True}) as upload:
                campaign.build_pair()
            self.assertEqual(observed, [
                ('baseline', 'sharded_wal_bench'), ('baseline', 'sharded_wal_recovery_bench'),
                ('current', 'sharded_wal_bench'), ('current', 'sharded_wal_recovery_bench'),
                ('current', 'sharded_wal_transport_bench')])
            self.assertNotIn('floor_binary_sha256', campaign.cloud_builds['baseline'])
            current = campaign.cloud_builds['current']
            self.assertEqual(current['floor_binary_sha256'],
                             real_sha(campaign.root / 'cloud-bin/current/sharded_wal_transport_bench'))
            self.assertEqual(current['floor_command'][-1], 'sharded_wal_transport_bench')
            self.assertEqual(current['local_floor_binary_sha256'], manifests['current']['floor_binary_sha256'])
            for label in ('baseline', 'current'):
                self.assertEqual(campaign.cloud_builds[label]['local_recovery_binary_sha256'],
                                 manifests[label]['recovery_binary_sha256'])
                self.assertEqual(real_sha(campaign.results / f'source-manifest-{label}.json'),
                                 campaign.cloud_builds[label]['source_manifest_sha256'])
            self.assertEqual([Path(call.args[1]).name for call in upload.call_args_list],
                             ['sharded_wal_bench', 'sharded_wal_bench'])
            self.assertEqual(campaign.install_cloud_binary.call_count, 2)

    def test_source_archive_rejects_unbound_duplicate_missing_and_changed_files(self):
        for defect in ('unbound', 'duplicate', 'missing', 'changed'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                campaign = self.build_campaign(Path(temporary), scalar=True)
                directory = campaign.bundle / 'baseline'
                manifest = json.loads((directory / 'source-manifest.json').read_text())
                archive = directory / 'source.tar.gz'
                if defect == 'missing':
                    with tarfile.open(archive, 'w:gz'):
                        pass
                else:
                    # Keep every manifest entry bound; only the additional file,
                    # duplicate member or altered actual bytes must cause failure.
                    with tarfile.open(archive) as stream:
                        contents = [(member.name, stream.extractfile(member).read())
                                    for member in stream.getmembers()]
                    if defect == 'unbound':
                        contents.append(('.cargo/config.toml', b'[build]\nrustflags=["--cfg=wrong"]\n'))
                    elif defect == 'duplicate':
                        contents.append(contents[0])
                    else:
                        contents[0] = (contents[0][0], b'changed source')
                    with tarfile.open(archive, 'w:gz') as stream:
                        for name, body in contents:
                            member = tarfile.TarInfo(name)
                            member.size = len(body)
                            stream.addfile(member, io.BytesIO(body))
                with self.assertRaises(ValueError):
                    guest.prepare_bundle.validate_source_archive(archive, manifest)

    def test_source_tree_claim_cannot_hide_a_changed_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary), scalar=True)
            directory = campaign.bundle / 'baseline'
            manifest = json.loads((directory / 'source-manifest.json').read_text())
            manifest['source_sha256']['src.rs'] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'tree hash'):
                guest.prepare_bundle.validate_source_archive(directory / 'source.tar.gz', manifest)

    def test_identical_cloud_outputs_abort_before_any_distribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary))
            with patch.object(guest, 'run_recorded', side_effect=self.fake_build_runner([], identical=True)), \
                 patch.object(guest.subprocess, 'check_output', return_value='rustc pinned\n'), \
                 patch.object(guest, 'upload') as upload:
                with self.assertRaisesRegex(ValueError, 'identical cloud binaries'):
                    campaign.build_pair()
            upload.assert_not_called()
            campaign.install_cloud_binary.assert_not_called()

    def test_missing_candidate_capability_aborts_before_any_distribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary))
            with patch.object(guest, 'run_recorded', side_effect=self.fake_build_runner([], missing_capability=True)), \
                 patch.object(guest.subprocess, 'check_output', return_value='rustc pinned\n'), \
                 patch.object(guest, 'upload') as upload:
                with self.assertRaisesRegex(ValueError, 'lacks required benchmark capabilities'):
                    campaign.build_pair()
            upload.assert_not_called()
            campaign.install_cloud_binary.assert_not_called()

    def test_floor_capabilities_and_shape_are_required_before_admission(self):
        for missing in (None, '--tcp-polling', '--tcp-resident-first', '--polling', 'shape.yaml'):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as temporary:
                campaign = self.build_campaign(Path(temporary), floor=True)
                for label in ('baseline', 'current'):
                    directory = campaign.root / 'cloud-bin' / label
                    directory.mkdir(parents=True)
                    binary = directory / 'sharded_wal_bench'
                    binary.write_text(label)
                    campaign.cloud_builds[label] = {'source_tree_sha256': label,
                                                    'binary_sha256': guest.sha(binary)}
                floor = campaign.root / 'cloud-bin/current/sharded_wal_transport_bench'
                floor.write_text('floor')
                campaign.cloud_builds['current']['floor_binary_sha256'] = guest.sha(floor)

                def run(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
                    self.assertEqual(guest.sha(command[0]), expected_binary_sha256)
                    if '--help' in command:
                        if Path(command[0]).name == 'sharded_wal_transport_bench':
                            flags = ('--output --remote-config --transport --polling --values '
                                     '--samples --repetitions --idle-us --profile').split()
                        else:
                            flags = ('--remote-config --prepare-external-case --tcp-preparation-chunk-batches '
                                     '--transport --native-read-workers --read-resource-evidence --read-window '
                                     '--native-read-cache-bytes --tcp-read-lanes --tcp-server-read-workers '
                                     '--tcp-wal-member-read --cold-read-pass --hot-read-pass --tcp-shard-cache-bytes '
                                     '--read-memory-allowance-bytes --shard-read-memory-allowance-bytes '
                                     '--cache-probe-keys --concurrent-mixed').split()
                            if 'current' in Path(command[0]).parts:
                                flags += ['--tcp-polling', '--tcp-resident-first']
                        Path(output).with_suffix('.stdout.log').write_text(' '.join(f for f in flags if f != missing))
                    else:
                        root = Path(command[command.index('--prepare-external-case') + 1])
                        for name in ['remote.yaml', 'shape.yaml'] + [f'node-{p}/launch.yaml' for p in range(5)]:
                            if name != missing:
                                path = root / name
                                path.parent.mkdir(parents=True, exist_ok=True)
                                path.write_text('prepared\n')

                with patch.object(guest, 'run_recorded', side_effect=run):
                    if missing:
                        with self.assertRaises(FileNotFoundError if missing == 'shape.yaml' else ValueError):
                            campaign.verify_cloud_pair()
                    else:
                        campaign.verify_cloud_pair()
                        receipt = json.loads((campaign.results / 'cloud-build-pair-verified.json').read_text())
                        self.assertIn('shape.yaml', receipt['prepared_configuration_sha256'])

    def test_floor_node_receipt_checks_actual_main_process_binary(self):
        for changed in (False, True):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                cfg = self.config() | {'campaign': 'transport-floor', 'region': 'us-east-1'}
                (root / 'bootstrap.json').write_text(json.dumps({'identity': {
                    'instanceId': cfg['nodes'][0]['instance_id'], 'accountId': cfg['account']}}))
                case_id = 'floor-owned-case'
                node_dir = root / 'cases' / case_id / 'node-0'
                payload = {'config': cfg, 'case_id': case_id, 'action': 'start', 'build': 'current',
                           'binary_sha256': 'main-hash',
                           'node_archives': {'0': {'sha256': 'archive-hash', 'key': 'owned-key'}}}

                def extract(*args):
                    node_dir.mkdir()
                    (node_dir / 'ready').write_text('authenticated-ready')

                def digest(path):
                    return 'changed' if changed and str(path).startswith('/proc/') else 'main-hash'

                with patch.object(guest, 'validate_cfg', return_value=(root, [])), \
                     patch.object(guest.boto3, 'Session'), patch.object(guest, 'download'), \
                     patch.object(guest, 'safe_extract', side_effect=extract), \
                     patch.object(guest, 'sha', side_effect=digest), \
                     patch.object(guest, 'evidence', return_value={}), \
                     patch.object(guest.subprocess, 'run'), \
                     patch.object(guest.subprocess, 'check_output', return_value=str(guest.os.getpid())), \
                     patch.object(guest.subprocess, 'Popen') as sampler:
                    if changed:
                        with self.assertRaisesRegex(ValueError, 'ready process executable'):
                            guest.node_action(payload)
                        self.assertFalse((node_dir / 'start.json').exists())
                        sampler.assert_not_called()
                    else:
                        guest.node_action(payload)
                        receipt = json.loads((node_dir / 'start.json').read_text())
                        self.assertEqual(receipt['build'], 'current')
                        self.assertEqual(receipt['binary_sha256'], 'main-hash')
                        sampler.assert_called_once()

    def test_floor_rejects_archived_auxiliary_substitution_before_building(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary), floor=True)
            auxiliary = campaign.bundle / 'baseline/sharded_wal_recovery_bench'
            auxiliary.write_text('substituted auxiliary')
            bundle_manifest = campaign.bundle / 'bundle-manifest.json'
            manifest = json.loads(bundle_manifest.read_text())
            manifest['files_sha256']['baseline/sharded_wal_recovery_bench'] = guest.sha(auxiliary)
            bundle_manifest.write_text(json.dumps(manifest))
            source_manifest = json.loads((campaign.bundle / 'baseline/source-manifest.json').read_text())
            with patch.object(guest.transport_floor, 'BASELINE_SOURCE_SHA256',
                              source_manifest['source_tree_sha256']), \
                 patch.object(guest, 'run_recorded') as run, \
                 patch.object(guest, 'upload') as upload:
                with self.assertRaisesRegex(ValueError, 'archived auxiliary executable'):
                    campaign.build_pair()
            run.assert_not_called()
            upload.assert_not_called()
            campaign.install_cloud_binary.assert_not_called()


    def test_read_campaign_requires_frozen_test_utils_before_distribution(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.build_campaign(Path(temporary))
            campaign.cfg['campaign'] = 'read-path'
            with patch.object(guest, 'run_recorded', side_effect=self.fake_build_runner([])), \
                 patch.object(guest.subprocess, 'check_output', return_value='rustc pinned\n'), \
                 patch.object(guest, 'upload') as upload:
                with self.assertRaisesRegex(ValueError, 'requires test-utils'):
                    campaign.build_pair()
            upload.assert_not_called()

    def test_read_campaign_matrix_matches_independent_result_validator(self):
        analyzer = module('analyze_cloud')
        specs = guest.read_path_cases()
        self.assertEqual(len(specs), 16)
        actual = set()
        for label, (value, batch, window), rep, options in specs:
            prefix = 'diagnostic' if options.get('diagnostics') else ('cold' if options.get('cold') else 'storage')
            for mode in ['native-sync', 'tcp']:
                actual.add(f'{prefix}-{label}-v{value}-b{batch}-w{window}-r{rep}-{mode}')
        self.assertEqual(actual, set(analyzer.expected_cases('read-path')))
        self.assertEqual(len(actual), 32)
        self.assertEqual(sum('-current-' in c and c.startswith('cold-') for c in actual), 6)

    def test_cached_cases_reject_cold_or_unmatched_campaign_before_start(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'campaign': 'efa-cache', 'efa': True}
        for changes in [{'cold': True}, {'diagnostics': True}, {'capacity': True},
                        {'chunk_control': True}, {'storage': False}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                campaign.run_case('current', (8192, 32, 32), 0,
                                  **({'storage': True, 'cached': True} | changes))
        campaign.cfg['campaign'] = 'efa'
        with self.assertRaises(ValueError):
            campaign.run_case('current', (8192, 32, 32), 0, storage=True, cached=True)
        with self.assertRaises(ValueError):
            guest.validate_cfg(self.config() | {'campaign': 'efa-cache', 'efa': False})

    def test_cache_ratios_use_current_control_and_matching_native_cache(self):
        runs = {}
        for family, build, mode, latency in [
                ('storage-efa', 'baseline', 'tcp', 400),
                ('storage-efa', 'baseline', 'native-sync', 20),
                ('storage-efa', 'current', 'tcp', 100),
                ('storage-efa', 'current', 'native-sync', 10),
                ('cache-efa', 'current', 'tcp', 3),
                ('cache-efa', 'current', 'native-sync', 2)]:
            name = f'{family}-{build}-{mode}'
            info = (family, build, 8192, 32, 32, 0, mode, name)
            runs[name] = (info, {('summary', 'point_get', 'p50_ns'): latency})
        row = next(row for row in analysis.compare(runs)
                   if row['family'] == 'cache-efa' and row['backend'] == 'tcp')
        self.assertEqual(row['comparison'], 'read_cache')
        self.assertEqual(row['new_over_old'], 0.03)
        self.assertEqual(row['old_over_native_barrier'], 10)
        self.assertEqual(row['new_over_native_barrier'], 1.5)


class ProgressCampaignTests(unittest.TestCase):
    def test_fixed_focused_plan_preserves_repetitions_and_native_identity(self):
        progress = guest.efa_progress
        qualification, main = progress.qualification_cases(), progress.main_cases()
        self.assertEqual((len(qualification) * 2, len(qualification) * 5), (12, 30))
        self.assertEqual((len(main) * 2, len(main) * 5), (224, 560))
        self.assertEqual(len({c['case_id'] for c in qualification + main}), 118)
        self.assertEqual({(c['value'], c['lanes']) for c in qualification},
                         {(v, lanes) for v in (1024, 8192) for lanes in (1, 2, 4)})
        self.assertTrue(all(c['cache'] == 0 and c['workers'] == 1 and c['read_window'] == 32
                            and c['native_workers'] == 32 and c['cold'] for c in qualification))
        core = [c for c in main if c['family'].startswith('core-')]
        for value in (1024, 8192):
            for build in ('baseline', 'current'):
                for transport in ('tcp', 'efa'):
                    selected = [c for c in core if (c['value'], c['build'], c['transport'], c['cache'])
                                == (value, build, transport, 0)]
                    self.assertEqual({(c['batch'], c['window'], c['rep']) for c in selected},
                                     {(b, b, rep) for b in (1, 32) for rep in range(3)})
        cold = [c for c in main if c['family'].startswith('cold-')]
        self.assertEqual({(c['lanes'], c['workers'], c['read_window']) for c in cold},
                         {(1, 1, 1), (4, 1, 32), (4, 4, 32)})
        # Repeated exact native controls remain identifiable; serial and window controls never alias.
        same = [c for c in cold if c['value'] == 1024 and c['rep'] == 0]
        self.assertEqual(len({c['native_control_id'] for c in same}), 2)
        orders = [[(c['build'], c['transport'], c['cache']) for c in core
                   if c['value'] == 1024 and c['batch'] == 1 and c['rep'] == rep] for rep in range(3)]
        self.assertEqual(len({tuple(order) for order in orders}), 3)

    def sensor(self, traffic=1):
        return {'meminfo': 'MemTotal: 1000 kB\nMemAvailable: 500 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n',
                'vmstat': 'pswpin 0\npswpout 0\n',
                'efa_counters': {'/sys/class/infiniband/rdmap0s1/ports/1/hw_counters/send_bytes': str(traffic)}}

    def test_hardware_gate_rejects_missing_reset_zero_and_swap_evidence(self):
        before, after = self.sensor(), self.sensor(100)
        self.assertEqual(guest.efa_progress.hardware_interval(before, after, True)['efa_traffic_bytes'], 99)
        bad = [after | {'efa_counters': {}}, self.sensor(1), self.sensor(0),
               after | {'vmstat': 'pswpin 1\npswpout 0\n'},
               after | {'meminfo': after['meminfo'].replace('SwapTotal: 0', 'SwapTotal: 1')},
               after | {'meminfo': after['meminfo'].replace('MemAvailable: 500', 'MemAvailable: 0')},
               after | {'meminfo': after['meminfo'].replace('MemTotal: 1000', 'MemTotal: 2000')}]
        for evidence in bad:
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                guest.efa_progress.hardware_interval(before, evidence, True)
        with self.assertRaises(KeyError):
            guest.efa_progress.hardware_interval({}, after, True)

    def resource_rows(self):
        return [dict(stage=stage, role=role, partition=partition, pid=pid, rss_bytes=100,
                     peak_rss_bytes=120, user_cpu_ns=ordinal * 100, system_cpu_ns=ordinal * 20)
                for ordinal, stage in enumerate(('opened', 'loaded', 'point_get_window',
                                                  'cache_evicted_get_window'))
                for role, partition, pid in [('frontend', '', 42)] +
                [('storage', str(p), p + 100) for p in range(5)]]

    def resource_result(self, rows):
        spec = guest.efa_progress.qualification_cases()[0]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'resources.csv'
            with path.open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=['stage', 'role', 'partition', 'pid',
                    'rss_bytes', 'peak_rss_bytes', 'user_cpu_ns', 'system_cpu_ns'])
                writer.writeheader()
                writer.writerows(rows)
            return guest.efa_progress.resource_metrics(path, 'tcp', spec, {'pid': 42},
                {'ram_bytes': 1000}, {p: {'ram_bytes': 1000, 'pid': p + 100} for p in range(5)},
                {'point_get_window': {'phase_wall_ns': 1000}})

    def test_complete_resource_inventory_aggregates_without_synthetic_provider_rss(self):
        result = self.resource_result(self.resource_rows())
        self.assertEqual(result['resource', 'opened', 'aggregate_rss_bytes'], 600)
        self.assertEqual(result['resource', 'point_get_window', 'cpu_ns_since_previous_snapshot'], 720)
        self.assertEqual(result['resource', 'point_get_window', 'cpu_ns_per_window_wall_ns_upper_bound'], .72)

    def test_reported_peak_rss_drop_preserves_observed_peak_and_cpu_guards(self):
        rows = self.resource_rows()
        rows[6]['peak_rss_bytes'] = 300
        rows[12]['peak_rss_bytes'] = 110
        result = self.resource_result(rows)
        self.assertEqual(result['resource', 'loaded', 'sum_observed_process_peak_rss_bytes'], 900)
        self.assertEqual(result['resource', 'point_get_window', 'sum_observed_process_peak_rss_bytes'], 900)
        self.assertEqual(result['resource', 'cache_evicted_get_window', 'sum_observed_process_peak_rss_bytes'], 900)
        for field in ('user_cpu_ns', 'system_cpu_ns'):
            invalid = copy.deepcopy(rows)
            invalid[12][field] = 0
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.resource_result(invalid)

    def test_resource_gate_rejects_missing_duplicate_wrong_pid_and_ram_violation(self):
        rows = self.resource_rows()
        for invalid in [rows[:-1], rows + rows[:1]]:
            with self.assertRaises(ValueError):
                self.resource_result(invalid)
        for change in [{'pid': 999}, {'rss_bytes': 900, 'peak_rss_bytes': 900},
                       {'role': 'provider'}, {'user_cpu_ns': -1}, {'rss_bytes': 121},
                       {'system_cpu_ns': 1}]:
            invalid = copy.deepcopy(rows)
            invalid[-1].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.resource_result(invalid)

    def test_full_qualification_missing_archive_fails_closed_with_structured_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            receipt = analysis.validate_qualification(Path(temporary))
        self.assertFalse(receipt['passed'])
        self.assertEqual(receipt['case_ids'], [])
        self.assertEqual(len(receipt['errors']), 13)
        self.assertIn('inventory', receipt['errors'])

    def test_broad_admission_rejects_unqualified_and_unplanned_cases_before_side_effects(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'campaign': 'efa-progress', 'efa': True}
        spec = guest.efa_progress.main_cases()[0]
        args = (spec['build'], (spec['value'], spec['batch'], spec['window']), spec['rep'])
        with self.assertRaisesRegex(ValueError, 'full hardware qualification'):
            campaign.run_case(*args, storage=True, progress_case=spec)
        campaign.qualification_passed = True
        with self.assertRaisesRegex(ValueError, 'unplanned'):
            campaign.run_case(*args, storage=True, progress_case=spec | {'read_window': 256})
        with self.assertRaisesRegex(ValueError, 'explicit planned case'):
            campaign.run_case(*args, storage=True)

    def test_controller_sensor_rejects_wrong_actual_instance_or_account(self):
        campaign = object.__new__(guest.Campaign)
        campaign.cfg = {'coordinator_instance_id': 'i-controller', 'account': '123456789012'}
        with tempfile.TemporaryDirectory() as temporary:
            campaign.root = Path(temporary)
            for identity in [{'instanceId': 'i-foreign', 'accountId': '123456789012'},
                             {'instanceId': 'i-controller', 'accountId': '999999999999'}]:
                (campaign.root / 'bootstrap.json').write_text(json.dumps({'identity': identity}))
                with self.assertRaisesRegex(ValueError, 'actual controller identity'):
                    campaign.controller_evidence()

    def test_progress_ratios_match_native_window_and_do_not_invent_old_window(self):
        runs = {}
        specs = [c for c in guest.efa_progress.main_cases() if c['family'].startswith('core-')
                 and c['transport'] == 'efa' and c['value'] == 1024 and c['batch'] == 32 and c['rep'] == 0]
        expected = analysis.expected_cases('efa-progress')
        for spec in specs:
            for mode in ('native-sync', 'tcp'):
                value = 10 if mode == 'native-sync' else 100
                if spec['cache']:
                    value /= 2
                metrics = {('summary', 'point_get', 'p50_ns'): value}
                if spec['build'] == 'current':
                    metrics['summary', 'point_get_window', 'phase_wall_ns'] = value * 2
                name = spec['case_id'] + '-' + mode
                runs[name] = (expected[name], metrics)
        rows = analysis.compare_progress(runs)
        window = next(r for r in rows if r['build'] == 'current' and not r['cache_bytes']
                      and r['backend'] == 'tcp' and r['phase'] == 'point_get_window')
        self.assertIsNone(window['old_median'])
        self.assertIsNone(window['new_over_old'])
        self.assertEqual(window['new_over_native_barrier'], 10)
        cached = next(r for r in rows if r['cache_bytes'] and r['backend'] == 'tcp'
                      and r['phase'] == 'point_get')
        self.assertEqual(cached['new_over_old'], .5)
        self.assertEqual(cached['new_over_native_barrier'], 10)
        self.assertEqual(cached['effective_native_concurrency'], 32)
        self.assertEqual(cached['comparison'], 'matched_cache_budget')

    def qualification_archive(self, root):
        progress = guest.efa_progress
        cfg = {'campaign': 'efa-progress', 'efa': True, 'run_id': 'th-0123456789abcdef',
               'run_root': '/mnt/tidehunter/th-0123456789abcdef', 'instance_type': 'c6id.32xlarge',
               'coordinator_instance_id': 'i-00000000000000001',
               'nodes': [{'partition': p, 'instance_id': f'i-{p+2:017x}',
                          'private_ip': f'172.31.32.{p+10}'} for p in range(5)]}
        def save(path, value):
            path.write_text(json.dumps(value))
        def table(path, rows):
            with path.open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        def sensor(instance, after=False, partition=None):
            result = self.sensor(100 if after else 1)
            result.update(instance_id=instance, utc=f'2026-10-08T00:00:0{int(after)}+00:00',
                findmnt=json.dumps({'filesystems': [{'source': '/dev/nvme1n1'}]}),
                lsblk=json.dumps({'blockdevices': [{'path': '/dev/nvme1n1', 'type': 'disk',
                                                    'model': 'Amazon EC2 NVMe Instance Storage'}]}),
                diskstats=f'259 1 nvme1n1 0 0 {100 if after else 1} 0 0 0 1 0 0 1 0\n')
            if partition is not None:
                result['partition'] = partition
            return result
        save(root / 'campaign-config.json', cfg)
        tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': cfg['run_id']}
        save(root / 'owned-instances.json', [{'InstanceId': iid, 'InstanceType': cfg['instance_type'],
             'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}
             for iid in [cfg['coordinator_instance_id']] + [n['instance_id'] for n in cfg['nodes']]])
        save(root / 'cloud-build-current.json', {'target_directory': cfg['run_root'] + '/build-target/current',
                                                'source_tree_sha256': 'a' * 64})
        binary = cfg['run_root'] + '/cloud-bin/current/sharded_wal_bench'
        for spec in progress.qualification_cases():
            case = spec['case_id']
            save(root / (case + '-plan.json'), spec)
            case_root = cfg['run_root'] + '/cases/' + case
            shape = {'--values': spec['value'], '--batch-sizes': 32, '--windows': 32,
                     '--batches': 64, '--mixed': 16, '--reads': 256, '--repetitions': 1}
            prepare = shape | {'--prepare-external-case': case_root, '--guest-case-root': case_root,
                '--remote-endpoints': ','.join(n['private_ip'] + ':7700' for n in cfg['nodes']),
                '--tcp-read-cache-bytes': 0, '--transport': 'efa', '--tcp-read-lanes': spec['lanes'],
                '--tcp-server-read-workers': 1, '--tcp-wal-member-read': 'mapped'}
            def command(opts):
                return [binary] + [str(item) for pair in opts.items() for item in pair]
            save(root / (case + '-prepare.command.json'), {'exit_code': 0, 'command': command(prepare)})
            (root / (case + '-remote.yaml')).write_text(f'read_lanes: {spec["lanes"]}\nread_cache_bytes: 0\n')
            node_root = root / 'nodes' / case
            node_root.mkdir(parents=True)
            for node in cfg['nodes']:
                p = node['partition']
                archive = node_root / f'node-{p}.tar.gz'
                members = {
                    'before.json': json.dumps(sensor(node['instance_id'], partition=p)),
                    'after.json': json.dumps(sensor(node['instance_id'], True, p)),
                    'efa-provider.txt': 'prov_name: efa\n',
                    'launch.yaml': 'read_workers: 1\nwal_member_read: mapped\n',
                    'process-status.txt': f'Pid: {p+100}\nVmRSS: 100 kB\nVmHWM: 200 kB\nVmSwap: 0 kB\n'}
                with tarfile.open(archive, 'w:gz') as stream:
                    for name, text in members.items():
                        member = tarfile.TarInfo(name)
                        data = text.encode()
                        member.size = len(data)
                        stream.addfile(member, io.BytesIO(data))
                save(archive.with_name(archive.name + '.receipt.json'), {'sha256': guest.sha(archive),
                     'bytes': archive.stat().st_size, 'key': f'test/results/nodes/{case}/node-{p}.tar.gz'})
            for mode in ('native-sync', 'tcp'):
                name = case + '-' + mode
                path = root / name
                path.mkdir()
                (path / 'COMPLETE').write_text('verified\n')
                opts = shape | {'--backend': mode, '--output': cfg['run_root'] + '/results/' + name,
                    '--cold-read-pass': 'true', '--read-window': 32, '--read-resource-evidence': 'true'}
                if mode == 'native-sync':
                    opts.update({'--native-read-cache-bytes': 0, '--native-read-workers': 32})
                else:
                    opts.update({'--remote-config': case_root + '/remote.yaml', '--tcp-read-cache-bytes': 0,
                                 '--transport': 'efa', '--tcp-preparation-chunk-batches': 16})
                save(root / (name + '-process.command.json'), {'exit_code': 0, 'command': command(opts),
                                                              'pid': 42, 'peak_sampled_main_process_rss_kib': 100})
                for stage in ('before', 'after'):
                    save(root / (name + '-' + stage + '.json'), sensor(cfg['coordinator_instance_id'], stage == 'after'))
                workers = 32 if mode == 'native-sync' else 1
                (path / 'campaign.txt').write_text(f'native_read_workers={workers}\n'
                    f'native_read_effective_concurrency={workers}\nread_window=32\nread_resource_evidence=true\n'
                    'native_read_cache_bytes=0\n')
                dimensions = dict(backend='experimental_efa_authority' if mode == 'tcp' else 'native_value_wal_barrier',
                                  durability='barrier', value_bytes=spec['value'], batch_size=32, window=32, repetition=0)
                counts = {phase: 1 for phase in ('open', 'point_get_missing', 'exists', 'exists_missing',
                    'checkpoint_capture', 'checkpoint_intervening_write', 'checkpoint_get', 'delete_window',
                    'full_scan', 'maintenance_flush_index', 'post_flush_get', 'shutdown', 'reopen')}
                counts.update(load_write_window=2048, mixed_write_window=512, mixed_get=512,
                              reopen_get_audit=2048, point_get=256, point_get_many=256, point_get_window=256,
                              cache_evicted_get=256, cache_evicted_get_many=256, cache_evicted_get_window=256)
                table(path / 'summary.csv', [dimensions | dict(phase=phase, samples=1, logical_operations=count,
                      measured_api_ns=100, phase_wall_ns=1000) for phase, count in counts.items()])
                table(path / 'samples.csv', [dimensions | dict(phase=phase, logical_operations=count, latency_ns=100)
                                            for phase, count in counts.items()])
                table(path / 'stages.csv', [dimensions | dict(phase='point_get', counter='cache_hits', delta=0)])
                table(path / 'remote_read_profiles.csv', [dimensions | dict(state='cache-evicted', whole_call_ns=100)])
                table(path / 'native_read_profiles.csv', [dimensions | dict(state='cache-evicted', value_cache_hit='false')])
                table(path / 'exchange_profiles.csv', [dimensions | dict(state='cache-evicted', elapsed_ns=100)])
                (path / 'node_profiles.yaml').write_text('[]\n')
                parts = range(5) if mode == 'tcp' else [0]
                table(path / 'read_io.csv', [dimensions | dict(phase=phase, partition=p,
                    pid=p+100 if mode == 'tcp' else 42, physical_read_bytes=4096, minor_faults=0, major_faults=1,
                    file_bytes=4096, resident_bytes_before=0, resident_bytes_after=4096)
                    for phase in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window',
                                  'cache_evicted_diagnostics') for p in parts])
                resources = self.resource_rows()
                if mode == 'native-sync':
                    resources = [row | {'role': 'native'} for row in resources if row['role'] == 'frontend']
                table(path / 'read_resources.csv', resources)

    def test_legacy_cold_archive_does_not_inherit_progress_requirements(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.qualification_archive(root)
            current = guest.efa_progress.qualification_cases()[0]['case_id']
            case = 'cold-current-v8192-b32-w32-r0'
            target = root / 'nodes' / case
            target.mkdir()
            archive = target / 'node-0.tar.gz'
            with tarfile.open(root / 'nodes' / current / archive.name) as source, \
                    tarfile.open(archive, 'w:gz') as destination:
                for member in source.getmembers():
                    data = source.extractfile(member).read()
                    if member.name in ('before.json', 'after.json'):
                        evidence = json.loads(data)
                        for key in ('instance_id', 'partition', 'meminfo', 'efa_counters'):
                            evidence.pop(key, None)
                        data = json.dumps(evidence).encode()
                    member.size = len(data)
                    destination.addfile(member, io.BytesIO(data))
            archive.with_name(archive.name + '.receipt.json').write_text(json.dumps({
                'sha256': guest.sha(archive), 'bytes': archive.stat().st_size,
                'key': f'test/results/nodes/{case}/node-0.tar.gz'}))
            result = analysis.node_evidence(root, case, 0)
            self.assertEqual(result['read_bytes'], 99 * 512)
            self.assertEqual(result['VmRSS_kib'], 100)
            self.assertNotIn('ram_bytes', result)

    def test_complete_qualification_audits_real_archive_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.qualification_archive(root)
            receipt = analysis.validate_qualification(root)
            self.assertTrue(receipt['passed'], receipt['errors'])
            self.assertEqual(len(receipt['case_ids']), 12)
            name = guest.efa_progress.qualification_cases()[0]['case_id'] + '-tcp'
            for filename, before, after in [('read_io.csv', ',4096,0,4096', ',4096,4096,4096'),
                                             ('stages.csv', 'cache_hits,0', 'cache_hits,1'),
                                             ('read_resources.csv', 'frontend,,42', 'frontend,,999')]:
                path = root / name / filename
                original = path.read_text()
                self.assertIn(before, original)
                path.write_text(original.replace(before, after))
                invalid = analysis.validate_qualification(root)
                self.assertFalse(invalid['passed'])
                self.assertIn(name, invalid['errors'])
                path.write_text(original)


if __name__ == '__main__':
    unittest.main()
