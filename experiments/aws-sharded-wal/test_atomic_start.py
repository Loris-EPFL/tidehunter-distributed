"""Offline six-guest startup exercise; no real AWS, IMDS, poweroff or benchmark."""
import configparser
import copy
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import atomic_start as atomic
import bootstrap as boot
import control
import sui_checkpoint as supervisor


def utc(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


class Clock:
    def __init__(self):
        self.now = datetime.now(timezone.utc).timestamp()
        self.elapsed = 0

    def time(self):
        return self.now + self.elapsed

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds


class Metadata:
    def __init__(self, fleet, role, identity):
        self.fleet, self.role, self.identity = fleet, role, identity
        self.role_at, self.profile_at = 2, 4
        self.config_at = 8
        self.hash_override = None
        self.foreign_identity = False
        self.credentials_reads = []

    def get(self, path):
        fleet, plan, elapsed = self.fleet, self.fleet.plan, self.fleet.clock.elapsed
        if path == 'dynamic/instance-identity/document':
            return json.dumps(self.identity | ({'accountId': '999999999999'} if self.foreign_identity else {}))
        if path.startswith('meta-data/tags/instance/'):
            name = path.rsplit('/', 1)[-1]
            if name == 'Role':
                return self.role if elapsed >= self.role_at else 'bootstrap'
            if name == 'ControllerConfigSha256':
                if self.role != 'controller':
                    raise AssertionError('storage tried to read controller configuration')
                if elapsed < self.config_at:
                    return None
                return self.hash_override or hashlib.sha256(fleet.config_raw).hexdigest()
            return dict(Project='TideHunter', ManagedBy='sharded-wal-benchmark', RunId=plan['run_id'])[name]
        role = self.role if elapsed >= self.profile_at else 'storage-0'
        name = atomic.instance_profile(plan, role)['role_name']
        if path == 'meta-data/iam/security-credentials/':
            return name
        if path == 'meta-data/iam/info':
            return json.dumps({'InstanceProfileArn': atomic.instance_profile(plan, role)['arn']})
        if path.startswith('meta-data/iam/security-credentials/'):
            if elapsed < self.role_at or elapsed < self.profile_at or path.rsplit('/', 1)[-1] != name:
                raise AssertionError('credential material fetched before final role/profile convergence')
            self.credentials_reads.append(elapsed)
            return json.dumps(dict(Code='Success', AccessKeyId=name, SecretAccessKey='local-test-only',
                                   Token=self.identity['instanceId'], Expiration=utc(fleet.clock.now + 7200)))
        raise AssertionError('unexpected metadata: ' + path)


class S3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, fleet):
        self.fleet, self.objects, self.uploads = fleet, {}, []
        self.missing_reads, self.fail_put = 1, False

    def put_object(self, **kwargs):
        if self.fail_put:
            raise OSError('export unavailable')
        assert kwargs['ExpectedBucketOwner'] == self.fleet.plan['account']
        self.objects[kwargs['Key']] = kwargs['Body']

    def get_object(self, **kwargs):
        assert kwargs['Key'] == self.fleet.plan['prefix'] + '/inputs/controller-config.json'
        if self.missing_reads:
            self.missing_reads -= 1
            raise self.exceptions.NoSuchKey()
        return {'Body': io.BytesIO(self.fleet.config_raw)}

    def download_file(self, bucket, key, destination, **kwargs):
        assert key == self.fleet.plan['prefix'] + '/input/bundle.tar.gz'
        Path(destination).write_bytes(self.fleet.archive)

    def upload_file(self, source, bucket, key, **kwargs):
        self.uploads.append(key)
        self.objects[key] = Path(source).read_bytes()


class EC2:
    def __init__(self, fleet):
        self.fleet, self.terminated, self.reads = fleet, [], []

    def describe_instances(self, **kwargs):
        self.reads.append(kwargs)
        instances = self.fleet.instances
        if 'InstanceIds' in kwargs:
            instances = [item for item in instances if item['InstanceId'] in kwargs['InstanceIds']]
        return {'Reservations': [{'OwnerId': self.fleet.plan['account'], 'Instances': copy.deepcopy(instances)}]}

    def terminate_instances(self, **kwargs):
        self.terminated.append(kwargs['InstanceIds'])
        for item in self.fleet.instances:
            if item['InstanceId'] in kwargs['InstanceIds']:
                item['State']['Name'] = 'terminated'
        return {}


class SSM:
    class exceptions:
        class InvocationDoesNotExist(Exception):
            pass

    def __init__(self, fleet):
        self.fleet, self.sent, self.proofs = fleet, [], {}
        self.never_online, self.fail_send = False, False

    def describe_instance_information(self, **kwargs):
        ids = kwargs['Filters'][0]['Values']
        return {'InstanceInformationList': [{'InstanceId': instance, 'PingStatus': 'Online'}
                for instance in ids if instance in self.fleet.started_storage and not self.never_online]}

    def send_command(self, **kwargs):
        self.sent.append(kwargs)
        if self.fail_send:
            raise TimeoutError('unknown SendCommand acknowledgement')
        args = shlex.split(kwargs['Parameters']['commands'][0].split('\n', 1)[1])
        assert args[2] == 'verify-ready'
        flags = dict(zip(args[3::2], args[4::2]))
        for instance in kwargs['InstanceIds']:
            # Execute the real storage admission code, not a canned SSM Success.
            guest = self.fleet.guests[instance]
            try:
                proof = guest.verify_ready(json.loads(flags['--expected']), flags['--deadline'],
                    flags['--bundle-sha256'], int(flags['--wait-seconds']))
                self.proofs[instance] = {'Status': 'Success', 'StandardOutputContent': json.dumps(proof)}
            except Exception as error:
                self.proofs[instance] = {'Status': 'Failed', 'StandardErrorContent': str(error)}
        return {'Command': {'CommandId': 'local-bootstrap-admission'}}

    def get_command_invocation(self, **kwargs):
        return self.proofs[kwargs['InstanceId']]


class Fleet:
    """Fake infrastructure around the production Guest.execute entrypoint."""
    def __init__(self, directory, ttl_seconds=1800, export_reserve_seconds=300):
        self.directory, self.clock = Path(directory), Clock()
        self.cfg = supervisor.schema()['config']
        self.cfg.update(deadline_utc=utc(self.clock.now + ttl_seconds),
                        export_reserve_seconds=export_reserve_seconds)
        for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            self.cfg[name]['sha256'] = 'a' * 64
        for name in ('build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256'):
            self.cfg[name] = 'b' * 64
        self.cfg['binaries'] = {name: 'c' * 64 for name in supervisor.BINARIES}
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
            for name in ('atomic_start.py', 'sui_checkpoint.py'):
                archive.add(Path(atomic.__file__).parent / name, arcname='runner/' + name)
        self.archive = buffer.getvalue()
        self.plan = dict(run_id=self.cfg['run_id'], account=self.cfg['account'], region=self.cfg['region'],
            bucket=self.cfg['bucket'], prefix=self.cfg['s3_prefix'], ttl_seconds=ttl_seconds,
            instance_type=self.cfg['instance_type'], placement_group=self.cfg['placement_group'],
            az=self.cfg['availability_zone'], export_reserve_seconds=export_reserve_seconds,
            protected_instances=['i-' + 'f' * 17],
            bundle_sha256=hashlib.sha256(self.archive).hexdigest(), bundle_bytes=len(self.archive),
            atomic_bindings={name: copy.deepcopy(self.cfg[name]) for name in atomic.BINDINGS},
            atomic_client_token=self.cfg['run_id'] + '-atomic')
        self.config_raw = json.dumps(self.cfg).encode()
        self.s3, self.ec2, self.ssm = S3(self), EC2(self), SSM(self)
        self.instances, self.guests, self.metadata = [], {}, {}
        self.started_storage, self.handoffs, self.poweroffs, self.sdk_creations = set(), [], [], []
        self.fail_handoff, self.bad_sts = False, False
        hosts = [('controller', self.cfg['coordinator_instance_id'], self.cfg['coordinator_private_ip'])]
        hosts += [(f"storage-{node['partition']}", node['instance_id'], node['private_ip']) for node in self.cfg['nodes']]
        for role, instance, ip in hosts:
            root = self.directory / instance
            root.mkdir()
            identity = dict(instanceId=instance, privateIp=ip, accountId=self.plan['account'], region=self.plan['region'],
                            instanceType=self.plan['instance_type'], pendingTime=utc(self.clock.now))
            record = dict(run_id=self.plan['run_id'], identity=identity, deadline_utc=self.cfg['deadline_utc'],
                          shutdown_behavior_required='terminate', device='/dev/nvme1n1')
            (root / 'bootstrap.json').write_text(json.dumps(record))
            (root / 'atomic-plan.json').write_text(json.dumps(self.plan))
            metadata = Metadata(self, role, identity)
            self.metadata[instance] = metadata
            self.instances.append(dict(InstanceId=instance, InstanceType=self.plan['instance_type'],
                ClientToken=self.plan['atomic_client_token'], PrivateIpAddress=ip, State={'Name': 'running'},
                Placement=dict(GroupName=self.plan['placement_group'], AvailabilityZone=self.plan['az']),
                IamInstanceProfile={'Arn': f"arn:aws:iam::{self.plan['account']}:instance-profile/{self.plan['run_id']}-{role}"},
                Tags=[{'Key': key, 'Value': value} for key, value in dict(Project='TideHunter',
                    ManagedBy='sharded-wal-benchmark', RunId=self.plan['run_id'], Role=role).items()]))
            self.guests[instance] = atomic.Guest(self.plan, root=root, metadata=metadata,
                services=self.services, clock=self.clock, command=lambda args, _id=instance, **kw: self.command(_id, args, **kw),
                validator=lambda root, cfg: supervisor.validate_cfg(cfg))
        self.controller = self.guests[self.cfg['coordinator_instance_id']]

    def services(self, plan, credentials):
        instance = credentials['Token']
        self.sdk_creations.append(instance)
        metadata = self.metadata[instance]
        assert metadata.credentials_reads
        assert self.clock.elapsed >= metadata.role_at and self.clock.elapsed >= metadata.profile_at
        name = credentials['AccessKeyId']
        arn = f"arn:aws:sts::{plan['account']}:assumed-role/{name}/{instance}"
        caller = dict(Account=plan['account'], Arn='foreign-role' if self.bad_sts else arn)
        return dict(sts=SimpleNamespace(get_caller_identity=lambda: caller), s3=self.s3, ssm=self.ssm, ec2=self.ec2)

    def command(self, instance, args, **kwargs):
        if args[0] == '/usr/sbin/poweroff':
            self.poweroffs.append(instance)
        elif args[0] == 'systemctl':
            pass
        else:
            assert args[2] == 'controller'
            cfg = json.loads(Path(args[4]).read_text())
            supervisor.validate_cfg(cfg)
            assert len(self.ssm.proofs) == 5 and all(item['Status'] == 'Success' for item in self.ssm.proofs.values())
            assert self.metadata[instance].role == 'controller'
            self.handoffs.append((instance, cfg, kwargs['timeout']))
            kwargs['stdout'].write(b'local frozen-controller handoff boundary reached\n')
            if self.fail_handoff:
                raise subprocess.CalledProcessError(1, args)
        return SimpleNamespace(returncode=0)

    def storage(self):
        for node in self.cfg['nodes']:
            instance = node['instance_id']
            # Storage-0 initially shares the bootstrap profile. Final Role still
            # gates its credential use; other nodes must also await replacement.
            if node['partition'] == 0:
                self.metadata[instance].profile_at = 0
            self.guests[instance].execute()
            self.started_storage.add(instance)


class BootstrapDeadlineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.units = self.root / 'units'
        self.units.mkdir()
        self.mount = self.root / 'mount'
        self.launched = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
        self.args = SimpleNamespace(run_id='th-0123456789abcdef', account='123456789012',
                                    instance_type='i4i.xlarge', ttl_seconds=7200)
        self.identity = dict(accountId=self.args.account, instanceType=self.args.instance_type,
                             instanceId='i-' + '1' * 17, pendingTime=self.launched.isoformat())
        self.command = self.enter_patch(patch.object(boot.subprocess, 'run'))
        self.devices = self.enter_patch(patch.object(boot.subprocess, 'check_output', side_effect=[
            json.dumps({'blockdevices': [dict(path='/dev/nvme1n1', type='disk',
                model='Amazon EC2 NVMe Instance Storage', fstype=None, mountpoints=[])]}),
            json.dumps({'signatures': []}),
        ]))
        self.enter_patch(patch.object(boot.os, 'geteuid', return_value=0))
        self.metadata = self.enter_patch(patch.object(boot, 'identity', return_value=self.identity))
        self.enter_patch(patch.object(boot, 'Path', side_effect=lambda path:
            self.units if path == '/etc/systemd/system' else self.mount if path == '/mnt/tidehunter' else Path(path)))
        self.clock = self.enter_patch(patch.object(boot, 'datetime', wraps=datetime))
        self.clock.now.return_value = self.launched
        self.enter_patch(patch('builtins.print'))

    def enter_patch(self, patcher):
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def test_two_hour_boot_arms_persistent_absolute_timer(self):
        boot.bootstrap(self.args)
        record = json.loads((self.mount / self.args.run_id / 'bootstrap.json').read_text())
        deadline = datetime.fromisoformat(record['deadline_utc'])
        self.assertEqual(deadline.timestamp(), self.launched.timestamp() + 7200)
        self.assertEqual(record['shutdown_behavior_required'], 'terminate')
        name = 'sharded-wal-expire-' + self.args.run_id
        timer = configparser.ConfigParser()
        timer.read(self.units / (name + '.timer'))
        self.assertEqual(datetime.strptime(timer['Timer']['OnCalendar'], '%Y-%m-%d %H:%M:%S UTC')
                         .replace(tzinfo=timezone.utc), deadline)
        self.assertTrue(timer['Timer'].getboolean('Persistent'))
        self.assertEqual(timer['Timer']['Unit'], name + '.service')
        service = configparser.ConfigParser()
        service.read(self.units / (name + '.service'))
        self.assertEqual(service['Service']['ExecStart'], '/usr/sbin/poweroff')
        self.assertEqual([call.args[0] for call in self.command.call_args_list[:3]], [
            ['systemctl', 'daemon-reload'],
            ['systemctl', 'enable', '--now', name + '.timer'],
            ['systemctl', 'is-active', '--quiet', name + '.timer']])

    def test_rebootstrap_after_reboot_does_not_reset_launch_expiry(self):
        boot.bootstrap(self.args)
        record = (self.mount / self.args.run_id / 'bootstrap.json').read_bytes()
        timer = self.units / ('sharded-wal-expire-' + self.args.run_id + '.timer')
        original_timer = timer.read_bytes()
        self.clock.now.return_value = datetime.fromtimestamp(self.launched.timestamp() + 1800, timezone.utc)
        self.command.reset_mock()
        with self.assertRaises(FileExistsError):
            boot.bootstrap(self.args)
        self.command.assert_not_called()
        self.assertEqual(timer.read_bytes(), original_timer)
        self.assertEqual((self.mount / self.args.run_id / 'bootstrap.json').read_bytes(), record)

    def test_malformed_or_excess_lifetimes_fail_before_metadata_or_timer_creation(self):
        for value in (599, 7201, 7200.001, 7200.0, True, None, '7200'):
            with self.subTest(lifetime=value), self.assertRaises(ValueError):
                boot.bootstrap(SimpleNamespace(**(vars(self.args) | {'ttl_seconds': value})))
        self.metadata.assert_not_called()
        self.command.assert_not_called()
        self.assertEqual(list(self.units.iterdir()), [])

    def test_expired_original_launch_powers_off_without_rearming(self):
        self.clock.now.return_value = datetime.fromtimestamp(self.launched.timestamp() + 7200, timezone.utc)
        with self.assertRaises(ValueError):
            boot.bootstrap(self.args)
        self.command.assert_called_once_with(['/usr/sbin/poweroff'], check=True)
        self.devices.assert_not_called()
        self.assertEqual(list(self.units.iterdir()), [])


class AtomicStartupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fleet = Fleet(self.temporary.name)

    def assert_owned_cleanup(self):
        self.assertEqual(self.fleet.ec2.terminated, [sorted(self.fleet.guests)])
        self.assertTrue(all(item not in self.fleet.plan['protected_instances'] for item in self.fleet.ec2.terminated[0]))

    def test_lifecycle_contract_accepts_old_lifetimes_and_exact_two_hours(self):
        for lifetime in (600, 1800, 3600, 7200):
            with self.subTest(lifetime=lifetime):
                contract = atomic.launch_contract(self.fleet.plan | {'ttl_seconds': lifetime})
                self.assertEqual(contract['ttl_seconds'], lifetime)
        for lifetime in (599, 7201, 7200.000001, 7200.0, True, None, '7200'):
            with self.subTest(lifetime=lifetime), self.assertRaises(ValueError):
                atomic.launch_contract(self.fleet.plan | {'ttl_seconds': lifetime})

    def test_two_hour_fleet_handoff_preserves_absolute_expiry_reserve_and_exact_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(directory, ttl_seconds=7200, export_reserve_seconds=600)
            original = fleet.cfg['deadline_utc']
            with patch.object(supervisor, 'remaining', side_effect=lambda deadline:
                              atomic.stamp(deadline) - fleet.clock.time()):
                fleet.storage()
                status = fleet.controller.execute()
            self.assertEqual(status['stage'], 'campaign-returned')
            self.assertEqual(status['deadline_utc'], original)
            self.assertEqual(fleet.ec2.terminated, [sorted(fleet.guests)])
            self.assertEqual(len(fleet.handoffs), 1)
            self.assertEqual(fleet.handoffs[0][1]['deadline_utc'], original)
            self.assertEqual(fleet.handoffs[0][1]['export_reserve_seconds'], 600)
            self.assertEqual(fleet.handoffs[0][2], atomic.stamp(original) - fleet.clock.time() - 30)
            self.assertEqual(len(fleet.ssm.proofs), 5)
            fleet.clock.elapsed += 600
            rebooted = atomic.Guest(fleet.plan, root=fleet.controller.root,
                                   clock=fleet.clock, command=fleet.controller.command)
            rebooted.bootstrap()
            self.assertEqual(rebooted.deadline, atomic.stamp(original))
            self.assertEqual(rebooted.startup_deadline, atomic.stamp(original) - 600 - 180)
            with self.assertRaises(FileExistsError):
                fleet.controller.execute()
            self.assertEqual(len(fleet.handoffs), 1)

    def test_two_hour_export_failure_still_terminates_exact_owned_fleet(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(directory, ttl_seconds=7200, export_reserve_seconds=600)
            fleet.s3.fail_put = True
            with self.assertRaises(OSError):
                fleet.controller.execute()
            self.assertEqual(fleet.handoffs, [])
            self.assertEqual(fleet.ec2.terminated, [sorted(fleet.guests)])

    def test_six_guest_eventual_role_config_handoff_export_and_cleanup(self):
        fleet = self.fleet
        fleet.storage()
        self.assertEqual(fleet.handoffs, [])
        self.assertEqual(fleet.ec2.reads, [])  # Storage never needs EC2 or controller SSM.
        status = fleet.controller.execute()
        self.assertEqual(status['stage'], 'campaign-returned')
        self.assertEqual(status['cleanup'], 'termination-requested')
        self.assertEqual(len(status['bootstrap']), 5)
        self.assertEqual(len(fleet.handoffs), 1)
        self.assertEqual(fleet.handoffs[0][1], fleet.cfg)
        self.assertEqual(len(fleet.sdk_creations), 6)
        self.assertGreaterEqual(fleet.clock.elapsed, 9)  # Tag then missing S3 config arrival.
        for i in range(5):
            key = fleet.plan['prefix'] + f'/results/cells/startup/storage/{i}/startup.json'
            proof = json.loads(fleet.s3.objects[key])
            self.assertEqual(proof['role'], f'storage-{i}')
            self.assertEqual(proof['stage'], 'bundle-ready')
        self.assertIn(fleet.plan['prefix'] + '/results/controller-autostart.log', fleet.s3.uploads)
        self.assert_owned_cleanup()

    def test_shared_checked_profiles_keep_six_guest_atomic_handoff(self):
        fleet = self.fleet
        profiles = {}
        for role in atomic.ROLES:
            name = 'ci-controller' if role == 'controller' else 'ci-storage'
            profiles[role] = dict(name=name, role_name=name + '-role',
                arn=f"arn:aws:iam::{fleet.plan['account']}:instance-profile/trusted/{name}")
        fleet.plan['instance_profiles'] = profiles
        for item in fleet.instances:
            role = next(tag['Value'] for tag in item['Tags'] if tag['Key'] == 'Role')
            item['IamInstanceProfile']['Arn'] = profiles[role]['arn']
        for guest in fleet.guests.values():
            guest.plan = atomic.launch_contract(fleet.plan)
        for metadata in fleet.metadata.values():
            metadata.profile_at = metadata.role_at
        fleet.storage()
        status = fleet.controller.execute()
        self.assertEqual(status['credential_role'], 'ci-controller-role')
        self.assertEqual(len(fleet.handoffs), 1)
        self.assertEqual(len(fleet.sdk_creations), 6)
        self.assert_owned_cleanup()

    def test_shared_profile_map_rejects_missing_roles_or_foreign_account(self):
        profiles = {role: dict(name='ci-profile', role_name='ci-role',
            arn=f"arn:aws:iam::{self.fleet.plan['account']}:instance-profile/ci-profile")
            for role in atomic.ROLES}
        with self.assertRaisesRegex(ValueError, 'six-role'):
            atomic.launch_contract(self.fleet.plan | {'instance_profiles': {}})
        profiles['controller']['arn'] = 'arn:aws:iam::999999999999:instance-profile/ci-profile'
        with self.assertRaisesRegex(ValueError, 'trusted instance profile'):
            atomic.launch_contract(self.fleet.plan | {'instance_profiles': profiles})

    def test_final_permissions_may_propagate_without_extending_original_ttl(self):
        fleet = self.fleet
        original = fleet.s3.put_object
        denied = 2
        class AccessDenied(Exception):
            response = {'Error': {'Code': 'AccessDenied'}}
        def eventually_allowed(**params):
            nonlocal denied
            if denied:
                denied -= 1
                raise AccessDenied()
            return original(**params)
        deadline = fleet.cfg['deadline_utc']
        with patch.object(fleet.s3, 'put_object', side_effect=eventually_allowed):
            fleet.storage()
            status = fleet.controller.execute()
        self.assertEqual(status['stage'], 'campaign-returned')
        self.assertEqual(fleet.cfg['deadline_utc'], deadline)
        self.assert_owned_cleanup()

    def test_storage_never_fetches_controller_config_or_launches(self):
        self.fleet.storage()
        self.assertEqual(self.fleet.handoffs, [])
        self.assertEqual(self.fleet.ssm.sent, [])
        self.assertEqual(self.fleet.ec2.terminated, [])
        self.assertEqual(self.fleet.s3.missing_reads, 1)

    def test_config_hash_mismatch_exports_failure_and_terminates_owned_fleet(self):
        self.fleet.metadata[self.fleet.cfg['coordinator_instance_id']].hash_override = '0' * 64
        with self.assertRaisesRegex(ValueError, 'config hash differs'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.handoffs, [])
        status = json.loads(self.fleet.s3.objects[self.fleet.plan['prefix'] + '/results/startup.json'])
        self.assertEqual(status['stage'], 'startup-failed')
        self.assertIn('config hash differs', status['error'])
        self.assert_owned_cleanup()

    def test_hash_valid_malformed_config_cannot_supply_cleanup_targets(self):
        self.fleet.config_raw = b'{"nodes": [{"instance_id": "i-fffffffffffffffff"}]}'
        with self.assertRaisesRegex(ValueError, 'immutable binding'):
            self.fleet.controller.execute()
        self.assert_owned_cleanup()
        self.assertEqual(self.fleet.ssm.sent, [])

    def test_config_foreign_node_is_rejected_before_ssm(self):
        cfg = copy.deepcopy(self.fleet.cfg)
        cfg['nodes'][0]['instance_id'] = 'i-' + 'e' * 17
        self.fleet.config_raw = json.dumps(cfg).encode()
        with self.assertRaisesRegex(ValueError, 'foreign instance/IP'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.ssm.sent, [])
        self.assert_owned_cleanup()

    def test_source_binding_and_coordinator_identity_cannot_be_rewritten(self):
        for field, value in [('source_archive_sha256', 'd' * 64),
                             ('coordinator_instance_id', 'i-' + 'e' * 17),
                             ('coordinator_private_ip', '10.0.0.99')]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                fleet = Fleet(directory)
                fleet.config_raw = json.dumps(fleet.cfg | {field: value}).encode()
                with self.assertRaisesRegex(ValueError, 'immutable binding differs: ' + field):
                    fleet.controller.execute()
                self.assertEqual(fleet.handoffs, [])
                self.assertEqual(fleet.ec2.terminated, [sorted(fleet.guests)])

    def test_foreign_inventory_is_never_terminated(self):
        self.fleet.instances[1]['Tags'][0]['Value'] = 'ForeignProject'
        with self.assertRaisesRegex(ValueError, 'foreign instance'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.ec2.terminated, [])
        self.assertEqual(self.fleet.ssm.sent, [])
        self.assertTrue(self.fleet.poweroffs)

    def test_missing_role_times_out_without_ever_acquiring_credentials(self):
        metadata = self.fleet.metadata[self.fleet.cfg['coordinator_instance_id']]
        metadata.role_at = 10000
        with self.assertRaisesRegex(TimeoutError, 'role/profile readiness'):
            self.fleet.controller.execute()
        self.assertEqual(metadata.credentials_reads, [])
        self.assertEqual(self.fleet.sdk_creations, [])
        self.assertEqual(self.fleet.ec2.terminated, [])
        self.assertEqual(self.fleet.poweroffs, [self.fleet.cfg['coordinator_instance_id']])
        self.assertEqual(self.fleet.clock.elapsed, 600)

    def test_missing_final_profile_never_borrows_bootstrap_credentials(self):
        metadata = self.fleet.metadata[self.fleet.cfg['coordinator_instance_id']]
        metadata.profile_at = 10000
        with self.assertRaisesRegex(TimeoutError, 'role/profile readiness'):
            self.fleet.controller.execute()
        self.assertEqual(metadata.credentials_reads, [])
        self.assertEqual(self.fleet.sdk_creations, [])
        self.assertEqual(self.fleet.clock.elapsed, 600)
        self.assertEqual(self.fleet.poweroffs, [self.fleet.cfg['coordinator_instance_id']])

    def test_config_timeout_cleans_up_without_untrusted_config(self):
        self.fleet.metadata[self.fleet.cfg['coordinator_instance_id']].config_at = 10000
        with self.assertRaisesRegex(TimeoutError, 'config publication deadline'):
            self.fleet.controller.execute()
        self.assert_owned_cleanup()

    def test_ssm_readiness_timeout_and_unknown_send_do_not_launch(self):
        self.fleet.ssm.never_online = True
        with self.assertRaisesRegex(TimeoutError, 'SSM readiness deadline'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.handoffs, [])
        self.assert_owned_cleanup()
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(directory)
            fleet.storage()
            fleet.ssm.fail_send = True
            with self.assertRaisesRegex(TimeoutError, 'unknown SendCommand'):
                fleet.controller.execute()
            self.assertEqual(len(fleet.ssm.sent), 1)
            self.assertEqual(fleet.handoffs, [])
            self.assertEqual(fleet.ec2.terminated, [sorted(fleet.guests)])

    def test_invalid_ready_proof_blocks_handoff(self):
        self.fleet.storage()
        node = self.fleet.guests[self.fleet.cfg['nodes'][0]['instance_id']]
        (node.root / 'bundle-ready').write_text('wrong-source')
        with self.assertRaisesRegex(RuntimeError, 'bootstrap gate failed'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.handoffs, [])
        self.assert_owned_cleanup()

    def test_supervisor_failure_retains_log_and_terminates(self):
        self.fleet.storage()
        self.fleet.fail_handoff = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.fleet.controller.execute()
        self.assertEqual(len(self.fleet.handoffs), 1)
        self.assertIn(self.fleet.plan['prefix'] + '/results/controller-autostart.log', self.fleet.s3.uploads)
        self.assert_owned_cleanup()

    def test_export_failure_cannot_suppress_termination(self):
        self.fleet.s3.fail_put = True
        with self.assertRaisesRegex(OSError, 'export unavailable'):
            self.fleet.controller.execute()
        self.assert_owned_cleanup()

    def test_sdk_export_timeout_preserves_error_and_terminates_owned_fleet(self):
        from botocore.exceptions import ReadTimeoutError
        timeout = ReadTimeoutError(endpoint_url='https://s3.invalid/startup.json')
        with patch.object(self.fleet.s3, 'put_object', side_effect=timeout):
            with self.assertRaises(ReadTimeoutError):
                self.fleet.controller.execute()
        self.assert_owned_cleanup()

    def test_bootstrap_deadline_cannot_be_extended(self):
        path = self.fleet.controller.root / 'bootstrap.json'
        record = json.loads(path.read_text())
        record['deadline_utc'] = utc(self.fleet.clock.now + 1801)
        path.write_text(json.dumps(record))
        with self.assertRaisesRegex(ValueError, 'immutable launch deadline'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.sdk_creations, [])
        self.assertEqual(self.fleet.poweroffs, [])

    def test_config_deadline_cannot_extend_bootstrap(self):
        self.fleet.config_raw = json.dumps(self.fleet.cfg | {'deadline_utc': utc(self.fleet.clock.now + 1801)}).encode()
        with self.assertRaisesRegex(ValueError, 'extends bootstrap'):
            self.fleet.controller.execute()
        self.assert_owned_cleanup()

    def test_wrong_sts_identity_never_uses_s3_or_ec2(self):
        self.fleet.bad_sts = True
        with self.assertRaisesRegex(ValueError, 'SDK caller'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.s3.objects, {})
        self.assertEqual(self.fleet.ec2.reads, [])
        self.assertEqual(self.fleet.handoffs, [])

    def test_live_foreign_identity_never_acquires_credentials(self):
        self.fleet.metadata[self.fleet.cfg['coordinator_instance_id']].foreign_identity = True
        with self.assertRaisesRegex(ValueError, 'live metadata'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.sdk_creations, [])
        self.assertEqual(self.fleet.poweroffs, [])

    def test_protected_bootstrap_never_terminates_or_powers_off(self):
        self.fleet.controller.plan['protected_instances'].append(self.fleet.cfg['coordinator_instance_id'])
        with self.assertRaisesRegex(ValueError, 'bootstrap identity'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.sdk_creations, [])
        self.assertEqual(self.fleet.poweroffs, [])
        self.assertEqual(self.fleet.ec2.terminated, [])

    def test_late_handoff_preserves_export_and_original_campaign_reserve(self):
        self.fleet.storage()
        self.fleet.clock.elapsed = 901
        with self.assertRaisesRegex(TimeoutError, 'insufficient original campaign lifetime'):
            self.fleet.controller.execute()
        self.assertEqual(self.fleet.handoffs, [])
        self.assert_owned_cleanup()

    def test_reentry_never_runs_another_campaign_or_extends_timer(self):
        self.fleet.storage()
        self.fleet.controller.execute()
        with self.assertRaises(FileExistsError):
            self.fleet.controller.execute()
        self.assertEqual(len(self.fleet.handoffs), 1)
        self.assert_owned_cleanup()

    def test_atomic_user_data_fits_ec2_limit_without_changing_other_campaigns(self):
        plan = copy.deepcopy(self.fleet.plan) | {'atomic_controller': True, 'efa': True}
        # Repeated dummy hashes compress much more than a real launch payload.
        bindings = plan['atomic_bindings']
        for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            bindings[name]['sha256'] = hashlib.sha256(name.encode()).hexdigest()
        for name in ('build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256'):
            bindings[name] = hashlib.sha256(name.encode()).hexdigest()
        bindings['binaries'] = {name: hashlib.sha256(name.encode()).hexdigest()
                                for name in bindings['binaries']}
        plan['bucket'] = 'tidehunter-ci-354775971805-us-east-1'
        plan['atomic_client_token'] = plan['run_id'] + '-' + hashlib.sha256(b'ci-candidate-token').hexdigest()[:24]
        plan['ownership_tags'] = dict(Repository='Loris-EPFL/tidehunter-distributed',
            BenchmarkBranch='bench/aws-efa', ExpiresAt='2026-10-19T13:18:31.767261+00:00')
        plan['instance_profiles'] = {}
        for role in atomic.ROLES:
            name = f"tidehunter-ci-{plan['run_id']}-{role}"
            plan['instance_profiles'][role] = dict(name=name, role_name=name,
                arn=f"arn:aws:iam::{plan['account']}:instance-profile/{name}")
        for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            bindings[name].update(bucket=plan['bucket'],
                budget_config_path='/home/runner/work/tidehunter-distributed/tidehunter-distributed/engine/experiments/aws-sharded-wal/budget.json',
                budget_config_sha256=hashlib.sha256(b'budget').hexdigest(),
                config_sha256=hashlib.sha256(b'config').hexdigest(),
                budget_config_source='recorded-policy')
        bindings.update(transports=['tcp', 'efa', 'efa-direct'], repetitions=2,
            read_profile_limits=[0, 128], write_profile_limits=[0, 128],
            native_write_profile_limits=[0, 128],
            kv_suite=dict(scope='full', repetitions=1, diagnostics=True))
        script = control.user_data(plan)
        self.assertLessEqual(len(script.encode()), 16384)
        # The shell is parsed by bash itself; no cloud commands are executed.
        subprocess.run(['bash', '-n'], input=script, text=True, check=True)
        with patch.object(atomic, 'launch_contract', side_effect=AssertionError('non-atomic changed')):
            legacy = control.user_data(plan | {'atomic_controller': False})
        subprocess.run(['bash', '-n'], input=legacy, text=True, check=True)


if __name__ == '__main__':
    unittest.main()
