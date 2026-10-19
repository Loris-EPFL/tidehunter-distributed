#!/usr/bin/env python3
"""Guest-only atomic fleet dispatch; the frozen supervisor owns all measurements."""
import argparse
from datetime import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import time
import traceback
import urllib.error
import urllib.request

BINDINGS = ('build_bundle', 'fixture_bundle', 'verification_bundle',
            'build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256',
            'binaries', 'transports', 'repetitions', 'read_profile_limits', 'write_profile_limits',
            'native_write_profile_limits', 'kv_suite',
            'checkpoint_digest', 'checkpoint_limit')
ROLES = {'controller', *(f'storage-{i}' for i in range(5))}


def instance_profile(plan, role):
    """Shared CI profiles are exact immutable identities, never suffix matches."""
    if role not in ROLES:
        raise ValueError('invalid instance role')
    if 'instance_profiles' not in plan:
        name = plan['run_id'] + '-' + role
        return dict(name=name, role_name=name,
                    arn=f"arn:aws:iam::{plan['account']}:instance-profile/{name}")
    profiles = plan['instance_profiles']
    if not isinstance(profiles, dict) or set(profiles) != ROLES:
        raise ValueError('exact six-role instance profile map required')
    for item in profiles.values():
        if (not isinstance(item, dict) or set(item) != {'name', 'role_name', 'arn'}
                or any(not re.fullmatch(r'[\w+=,.@-]{1,128}', item.get(key, ''))
                       for key in ('name', 'role_name'))
                or not re.fullmatch(
                    rf"arn:aws:iam::{plan['account']}:instance-profile/(?:[\w+=,.@-]+/)*"
                    + re.escape(item['name']), item.get('arn', ''))):
            raise ValueError('invalid trusted instance profile identity')
    return dict(profiles[role])


def stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError('absolute UTC deadline required')
    return parsed.timestamp()


def launch_contract(plan):
    """Copy only immutable startup inputs, never credentials or mutable fleet IDs."""
    names = ('run_id', 'account', 'region', 'bucket', 'prefix', 'ttl_seconds',
             'instance_type', 'placement_group', 'az', 'export_reserve_seconds',
             'protected_instances', 'bundle_sha256', 'bundle_bytes', 'atomic_bindings')
    result = {name: plan[name] for name in names}
    result['atomic_client_token'] = plan.get('atomic_client_token', plan['run_id'] + '-atomic')
    if (not re.fullmatch(r'th-[a-f0-9]{16}', result['run_id'])
            or not re.fullmatch(r'[0-9]{12}', result['account'])
            or result['prefix'] != result['run_id'] or result['placement_group'] != result['run_id']
            or not 600 <= result['ttl_seconds'] <= 3600
            or not 300 <= result['export_reserve_seconds'] <= 900
            or not re.fullmatch(r'[a-f0-9]{64}', result['bundle_sha256'])
            or not 0 < result['bundle_bytes'] <= 512 * 1024 * 1024
            or set(result['atomic_bindings']) != set(BINDINGS)):
        raise ValueError('invalid immutable atomic startup contract')
    if 'instance_profiles' in plan:
        instance_profile(plan, 'controller')
        result['instance_profiles'] = plan['instance_profiles']
    if 'ownership_tags' in plan:
        tags = plan['ownership_tags']
        if (not isinstance(tags, dict)
                or set(tags) != {'Repository', 'BenchmarkBranch', 'ExpiresAt'}
                or tags['Repository'] != 'Loris-EPFL/tidehunter-distributed'
                or tags['BenchmarkBranch'] != 'bench/aws-efa'):
            raise ValueError('invalid CI ownership identity')
        stamp(tags['ExpiresAt'])
        result['ownership_tags'] = dict(tags)
    return result


class Metadata:
    """Every request uses a fresh, bounded IMDSv2 token; never falls back to v1."""
    def get(self, path):
        base = 'http://169.254.169.254/latest/'
        request = urllib.request.Request(base + 'api/token', method='PUT',
            headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
        with urllib.request.urlopen(request, timeout=3) as response:
            token = response.read(4096).decode()
        request = urllib.request.Request(base + path, headers={'X-aws-ec2-metadata-token': token})
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise ValueError('oversized metadata')
                return raw.decode().strip()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None  # Tags and replacement profiles are eventually visible.
            raise


def aws_services(plan, credentials):
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=plan['region'],
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['Token'])
    config = Config(connect_timeout=3, read_timeout=10,
                    retries={'total_max_attempts': 1, 'mode': 'standard'})
    return {name: session.client(name, config=config) for name in ('sts', 's3', 'ssm', 'ec2')}


def validate_supervisor(root, cfg):
    # Import only the hash-verified bootstrap copy, not a deployment-side script.
    sys.path.insert(0, str(root / 'runner'))
    spec = importlib.util.spec_from_file_location('atomic_frozen_supervisor', root / 'runner/sui_checkpoint.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.validate_cfg(cfg)


class Guest:
    def __init__(self, plan, *, root=None, metadata=None, services=aws_services,
                 clock=time, command=subprocess.run, validator=validate_supervisor):
        self.plan = launch_contract(plan)
        self.root = Path(root) if root is not None else Path('/mnt/tidehunter') / plan['run_id']
        self.metadata = metadata or Metadata()
        self.services_factory, self.clock, self.command, self.validator = services, clock, command, validator
        self.clients, self.role, self.identity = None, None, None
        self.owned_bootstrap = False
        self.status = {'run_id': plan['run_id'], 'stage': 'waiting-role', 'error': None}
        self.tags = dict(Project='TideHunter', ManagedBy='sharded-wal-benchmark',
                         RunId=plan['run_id'], **self.plan.get('ownership_tags', {}))
        self.log = self.root / 'atomic-start.log'

    def publish(self):
        self.status['updated_unix'] = self.clock.time()
        raw = (json.dumps(self.status, sort_keys=True) + '\n').encode()
        (self.root / 'atomic-start.json').write_bytes(raw)
        with self.log.open('ab') as log:
            log.write(raw)
        if self.clients:
            end = self.clock.monotonic() + 60
            while True:
                try:
                    self.clients['s3'].put_object(Bucket=self.plan['bucket'], Key=self.output_prefix() + '/startup.json',
                        Body=raw, ServerSideEncryption='AES256', ExpectedBucketOwner=self.plan['account'])
                    break
                except Exception as error:
                    code = (getattr(error, 'response', None) or {}).get('Error', {}).get('Code')
                    if code not in ('AccessDenied', 'AccessDeniedException'):
                        raise
                    self.pause(end, 'final guest IAM permission propagation deadline')

    def output_prefix(self):
        prefix = self.plan['prefix'] + '/results'
        return prefix if self.role == 'controller' else prefix + '/cells/startup/storage/' + self.role[-1]

    def pause(self, end, reason):
        if self.clock.monotonic() >= end or self.clock.time() >= self.startup_deadline:
            raise TimeoutError(reason)
        self.clock.sleep(min(1, end - self.clock.monotonic(), self.startup_deadline - self.clock.time()))

    def bootstrap(self):
        record = json.loads((self.root / 'bootstrap.json').read_text())
        self.identity = identity = record['identity']
        p = self.plan
        if (record['run_id'] != p['run_id'] or identity['accountId'] != p['account']
                or identity['region'] != p['region'] or identity['instanceType'] != p['instance_type']
                or identity['instanceId'] in p['protected_instances']
                or not re.fullmatch(r'i-[a-f0-9]{17}', identity['instanceId'])
                or stamp(record['deadline_utc']) != stamp(identity['pendingTime']) + p['ttl_seconds']
                or record.get('shutdown_behavior_required') != 'terminate'):
            raise ValueError('bootstrap identity or immutable launch deadline differs')
        self.owned_bootstrap = True
        self.deadline = stamp(record['deadline_utc'])
        self.startup_deadline = self.deadline - p['export_reserve_seconds'] - 180
        self.startup_end = self.clock.monotonic() + min(600, self.startup_deadline - self.clock.time())
        self.status.update(instance_id=identity['instanceId'], deadline_utc=record['deadline_utc'])
        self.command(['systemctl', 'is-active', '--quiet', 'sharded-wal-expire-' + p['run_id'] + '.timer'],
                     check=True, timeout=10)

    def metadata_role(self):
        actual = self.metadata.get('dynamic/instance-identity/document')
        if actual is None:
            return None
        if json.loads(actual) != self.identity:
            raise ValueError('live metadata differs from bootstrapped identity')
        for key, expected in self.tags.items():
            observed = self.metadata.get('meta-data/tags/instance/' + key)
            if observed is None:
                return None
            if observed != expected:
                raise ValueError('foreign metadata ownership tag: ' + key)
        role = self.metadata.get('meta-data/tags/instance/Role')
        if role in (None, 'bootstrap'):
            return None
        if role not in ROLES:
            raise ValueError('invalid final role tag')
        profile = instance_profile(self.plan, role)
        # This listing does not fetch credential material. Profile and role must
        # both have converged before the credentials endpoint or boto3 is touched.
        observed = self.metadata.get('meta-data/iam/security-credentials/')
        info = self.metadata.get('meta-data/iam/info')
        if (observed != profile['role_name'] or not info
                or json.loads(info).get('InstanceProfileArn') != profile['arn']):
            return None
        return role

    def settle_role(self):
        while True:
            try:
                role = self.metadata_role()
            except (urllib.error.URLError, TimeoutError):
                role = None
            if role:
                self.role = role
                break
            self.pause(self.startup_end, 'final role/profile readiness deadline')
        if self.clock.time() >= self.startup_deadline or self.clock.monotonic() >= self.startup_end:
            raise TimeoutError('no startup reserve after profile assignment')
        name = instance_profile(self.plan, self.role)['role_name']
        while True:
            if self.metadata_role() != self.role:
                raise ValueError('role changed before credential acquisition')
            raw = self.metadata.get('meta-data/iam/security-credentials/' + name)
            credentials = json.loads(raw) if raw else {}
            if credentials.get('Code') == 'Success':
                break
            self.pause(self.startup_end, 'final role credentials readiness deadline')
        if stamp(credentials['Expiration']) < self.deadline:
            raise ValueError('final role credentials do not cover immutable lifetime')
        if self.metadata_role() != self.role:
            raise ValueError('role changed during credential acquisition')
        clients = self.services_factory(self.plan, credentials)
        caller = clients['sts'].get_caller_identity()
        expected = f"arn:aws:sts::{self.plan['account']}:assumed-role/{name}/{self.identity['instanceId']}"
        if caller.get('Account') != self.plan['account'] or caller.get('Arn') != expected:
            raise ValueError('SDK caller is not the exact final instance role')
        self.clients = clients
        self.status.update(role=self.role, credential_role=name, stage='role-ready')
        self.publish()

    def bundle(self):
        p = self.plan
        archive = self.root / 'bundle.tar.gz'
        self.clients['s3'].download_file(p['bucket'], p['prefix'] + '/input/bundle.tar.gz', str(archive),
                                       ExtraArgs={'ExpectedBucketOwner': p['account']})
        with archive.open('rb') as stream:
            if archive.stat().st_size != p['bundle_bytes'] or hashlib.file_digest(stream, 'sha256').hexdigest() != p['bundle_sha256']:
                raise ValueError('bootstrap bundle hash/length differs')
        with tarfile.open(archive) as stream:
            members = stream.getmembers()
            if sum(item.size for item in members) > 512 * 1024 * 1024:
                raise ValueError('expanded bootstrap bundle bound')
            names = set()
            for item in members:
                path = Path(item.name)
                if (path.is_absolute() or '..' in path.parts or not path.parts or path.parts[0] != 'runner'
                        or not (item.isfile() or item.isdir()) or str(path) in names):
                    raise ValueError('unsafe bootstrap archive member')
                names.add(str(path))
            if not {'runner/atomic_start.py', 'runner/sui_checkpoint.py'} <= names:
                raise ValueError('missing tracked startup/supervisor scripts')
            stream.extractall(self.root, members=members, filter='data')
        if (self.root / 'runner/atomic_start.py').read_bytes() != Path(__file__).read_bytes():
            raise ValueError('bootstrap dispatcher differs from embedded launch source')
        if self.metadata_role() != self.role:
            raise ValueError('final role changed before bundle readiness')
        # Restart the managed-node agent after profile replacement, not the TTL.
        for service in ('amazon-ssm-agent.service', 'snap.amazon-ssm-agent.amazon-ssm-agent.service'):
            found = self.command(['systemctl', 'cat', service], check=False, capture_output=True, timeout=10)
            if found.returncode == 0:
                self.command(['systemctl', 'restart', service], check=True, timeout=30)
                break
        else:
            raise ValueError('managed-node SSM service is absent')
        if self.clock.time() >= self.startup_deadline:
            raise TimeoutError('bundle installation exhausted startup reserve')
        proof = dict(self.status, bundle_sha256=p['bundle_sha256'], stage='bundle-ready')
        (self.root / 'bundle-ready.json').write_text(json.dumps(proof))
        (self.root / 'bundle-ready').write_text(p['bundle_sha256'])
        self.status['stage'] = 'bundle-ready'
        self.publish()

    def inventory(self, ids=None, admission=False):
        p = self.plan
        args = {'InstanceIds': ids} if ids is not None else {'Filters': [
            *[{'Name': 'tag:' + key, 'Values': [value]} for key, value in self.tags.items()],
            {'Name': 'client-token', 'Values': [p['atomic_client_token']]}]}
        response = self.clients['ec2'].describe_instances(**args)
        if response.get('NextToken'):
            raise ValueError('unexpected paginated six-host inventory')
        instances = []
        for reservation in response['Reservations']:
            if reservation['OwnerId'] != p['account']:
                raise ValueError('foreign reservation owner')
            for item in reservation['Instances']:
                tags = {tag['Key']: tag['Value'] for tag in item.get('Tags', [])}
                if (any(tags.get(key) != value for key, value in self.tags.items())
                        or item['ClientToken'] != p['atomic_client_token']
                        or item['InstanceId'] in p['protected_instances']
                        or item['InstanceType'] != p['instance_type']
                        or item['Placement'].get('GroupName') != p['placement_group']
                        or item['Placement'].get('AvailabilityZone') != p['az']
                        or tags.get('Role') not in ROLES | {'bootstrap'}):
                    raise ValueError('foreign instance cannot be admitted or terminated')
                instances.append(item)
        found = [item['InstanceId'] for item in instances]
        if (not 1 <= len(found) <= 6 or len(set(found)) != len(found)
                or self.identity['instanceId'] not in found
                or (ids is not None and set(found) != set(ids))):
            raise ValueError('exact owned fleet inventory differs')
        if admission:
            roles = {next(tag['Value'] for tag in item['Tags'] if tag['Key'] == 'Role'): item for item in instances}
            if len(instances) != 6 or set(roles) != ROLES:
                raise ValueError('six final fleet roles required')
            if roles['controller']['InstanceId'] != self.identity['instanceId']:
                raise ValueError('foreign controller instance')
            for role, item in roles.items():
                arn = instance_profile(p, role)['arn']
                if item.get('IamInstanceProfile', {}).get('Arn') != arn or item['State']['Name'] != 'running':
                    raise ValueError('final fleet profile/state differs')
        return instances

    def configuration(self):
        p = self.plan
        self.status['stage'] = 'waiting-controller-config'
        self.publish()
        while True:
            digest = self.metadata.get('meta-data/tags/instance/ControllerConfigSha256')
            if digest:
                if not re.fullmatch(r'[a-f0-9]{64}', digest):
                    raise ValueError('invalid controller config SHA tag')
                try:
                    response = self.clients['s3'].get_object(Bucket=p['bucket'],
                        Key=p['prefix'] + '/inputs/controller-config.json', ExpectedBucketOwner=p['account'])
                except self.clients['s3'].exceptions.NoSuchKey:
                    pass
                else:
                    with response['Body'] as body:
                        raw = body.read(1024 * 1024 + 1)
                    if len(raw) > 1024 * 1024 or hashlib.sha256(raw).hexdigest() != digest:
                        raise ValueError('controller config hash differs')
                    cfg = json.loads(raw)
                    break
            self.pause(self.startup_end, 'controller config publication deadline')
        for name, expected in dict(run_id=p['run_id'], account=p['account'], region=p['region'],
                bucket=p['bucket'], s3_prefix=p['prefix'], instance_type=p['instance_type'],
                run_root='/mnt/tidehunter/' + p['run_id'], placement_group=p['placement_group'],
                availability_zone=p['az'], export_reserve_seconds=p['export_reserve_seconds'],
                coordinator_instance_id=self.identity['instanceId'], coordinator_private_ip=self.identity['privateIp'],
                **p['atomic_bindings']).items():
            if cfg.get(name) != expected:
                raise ValueError('controller config immutable binding differs: ' + name)
        if stamp(cfg['deadline_utc']) > self.deadline:
            raise ValueError('controller config extends bootstrap deadline')
        self.validator(self.root, cfg)
        fleet = self.inventory(admission=True)
        expected = {'controller': (cfg['coordinator_instance_id'], cfg['coordinator_private_ip'])}
        expected.update({f"storage-{node['partition']}": (node['instance_id'], node['private_ip']) for node in cfg['nodes']})
        for item in fleet:
            role = next(tag['Value'] for tag in item['Tags'] if tag['Key'] == 'Role')
            if expected[role] != (item['InstanceId'], item['PrivateIpAddress']):
                raise ValueError('controller config names a foreign instance/IP')
        self.startup_deadline = min(self.startup_deadline, stamp(cfg['deadline_utc']) - p['export_reserve_seconds'] - 180)
        self.status['controller_config_sha256'] = digest
        (self.root / 'sui-config.json').write_bytes(raw)
        return cfg

    def storage_ready(self, cfg):
        ssm = self.clients['ssm']
        ids = sorted(node['instance_id'] for node in cfg['nodes'])
        while True:
            information = ssm.describe_instance_information(Filters=[{'Key': 'InstanceIds', 'Values': ids}], MaxResults=50)
            online = {item['InstanceId'] for item in information['InstanceInformationList'] if item['PingStatus'] == 'Online'}
            if not online <= set(ids):
                raise ValueError('SSM returned foreign instance')
            if online == set(ids):
                break
            self.pause(self.startup_end, 'exact storage SSM readiness deadline')
        seconds = int(min(self.startup_end - self.clock.monotonic(), self.startup_deadline - self.clock.time()))
        if seconds < 5:
            raise TimeoutError('bootstrap gate lacks startup reserve')
        expected = {node['instance_id']: f"storage-{node['partition']}" for node in cfg['nodes']}
        args = [sys.executable, str(self.root / 'runner/atomic_start.py'), 'verify-ready',
                '--plan', str(self.root / 'atomic-plan.json'), '--expected', json.dumps(expected),
                '--deadline', cfg['deadline_utc'], '--bundle-sha256', self.plan['bundle_sha256'],
                '--wait-seconds', str(seconds)]
        reply = ssm.send_command(InstanceIds=ids, DocumentName='AWS-RunShellScript',
            Parameters={'commands': ['set -eu\n' + shlex.join(args)], 'executionTimeout': [str(seconds)]},
            TimeoutSeconds=30, Comment=self.plan['run_id'] + ' exact atomic bootstrap admission')
        command_id = reply['Command']['CommandId']
        self.status['bootstrap_command'] = command_id
        self.publish()
        completed = {}
        while len(completed) < 5:
            for instance in ids:
                if instance in completed:
                    continue
                try:
                    reply = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
                except ssm.exceptions.InvocationDoesNotExist:
                    continue
                if reply['Status'] in ('Pending', 'Delayed', 'InProgress'):
                    continue
                if reply['Status'] != 'Success':
                    raise RuntimeError('storage bootstrap gate failed: ' + instance + ' ' + str(reply))
                proof = json.loads(reply['StandardOutputContent'])
                if (proof['instance_id'] != instance or proof['role'] != expected[instance]
                        or proof['bundle_sha256'] != self.plan['bundle_sha256']
                        or stamp(proof['deadline_utc']) < stamp(cfg['deadline_utc'])):
                    raise ValueError('storage bootstrap proof differs')
                completed[instance] = proof
            if len(completed) < 5:
                self.pause(self.startup_end, 'five-host bootstrap gate deadline')
        self.status['bootstrap'] = completed

    def handoff(self, cfg):
        if self.metadata_role() != 'controller':
            raise ValueError('only the final controller role may launch')
        seconds = stamp(cfg['deadline_utc']) - self.clock.time() - 30
        if seconds < cfg['export_reserve_seconds'] + 600:
            raise TimeoutError('insufficient original campaign lifetime')
        self.status['stage'] = 'starting-frozen-campaign'
        self.publish()
        env = {key: value for key, value in os.environ.items() if not key.startswith('AWS_')}
        env.update(AWS_SHARED_CREDENTIALS_FILE='/dev/null', AWS_CONFIG_FILE='/dev/null',
                   AWS_EC2_METADATA_DISABLED='false', AWS_DEFAULT_REGION=self.plan['region'])
        with (self.root / 'controller-autostart.log').open('xb') as output:
            self.command([sys.executable, str(self.root / 'runner/sui_checkpoint.py'), 'controller',
                          '--config', str(self.root / 'sui-config.json')],
                         stdout=output, stderr=subprocess.STDOUT, check=True, timeout=seconds, env=env)
        self.status['stage'] = 'campaign-returned'

    def cleanup(self):
        # Never obtain termination targets from config, including hash-valid bad JSON.
        fleet = self.inventory()
        ids = sorted(item['InstanceId'] for item in fleet)
        self.inventory(ids=ids)
        self.status['cleanup_instance_ids'] = ids
        self.status['cleanup'] = 'requesting-termination'
        try:
            self.publish()
        finally:
            # S3 export failure must never suppress the independently scoped
            # termination request.
            self.clients['ec2'].terminate_instances(InstanceIds=ids)
            self.status['cleanup'] = 'termination-requested'

    def poweroff_owned(self):
        # Even self-shutdown needs trusted ownership, not a malformed config or
        # a transplanted bootstrap record on a protected/foreign host.
        if not self.owned_bootstrap:
            return
        try:
            raw = self.metadata.get('dynamic/instance-identity/document')
            owned = raw and json.loads(raw) == self.identity and all(
                self.metadata.get('meta-data/tags/instance/' + key) == value
                for key, value in self.tags.items())
        except (urllib.error.URLError, TimeoutError, ValueError):
            owned = False
        if owned:
            self.command(['/usr/sbin/poweroff'], check=True, timeout=10)

    def execute(self):
        lock = (self.root / '.atomic-start.lock').open('a')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Creation-only disallows a second invocation after process exit.
            with (self.root / 'atomic-started').open('x'):
                pass
        except BaseException:
            lock.close()
            raise
        error = None
        try:
            self.bootstrap()
            self.publish()
            self.settle_role()
            self.bundle()
            if self.role == 'controller':
                cfg = self.configuration()
                self.status['stage'] = 'waiting-storage-readiness'
                self.publish()
                self.storage_ready(cfg)
                self.handoff(cfg)
        except BaseException as caught:
            error = caught
            self.status.update(stage='startup-failed', error=traceback.format_exc())
        finally:
            try:
                self.publish()
                if self.clients:
                    for path in (self.log, self.root / 'controller-autostart.log'):
                        if path.is_file():
                            self.clients['s3'].upload_file(str(path), self.plan['bucket'],
                                self.output_prefix() + '/' + path.name,
                                ExtraArgs={'ServerSideEncryption': 'AES256', 'ExpectedBucketOwner': self.plan['account']})
            except BaseException as caught:
                error = error or caught
            finally:
                try:
                    if self.clients and self.role == 'controller':
                        self.cleanup()
                except BaseException as caught:
                    self.status['cleanup_error'] = traceback.format_exc()
                    error = error or caught
                finally:
                    try:
                        self.publish()
                    except BaseException as caught:
                        error = error or caught
                    lock.close()
                    if error:
                        # If ownership is unavailable, leave the independently
                        # armed immutable timer as the only self-shutdown path.
                        self.poweroff_owned()
        if error:
            raise error
        return self.status

    def verify_ready(self, expected, deadline, bundle_sha256, wait_seconds):
        self.bootstrap()
        end = self.clock.monotonic() + min(600, max(0, wait_seconds))
        while not (self.root / 'bundle-ready').is_file():
            self.pause(end, 'storage bundle readiness deadline')
        role = self.metadata_role()
        proof = json.loads((self.root / 'bundle-ready.json').read_text())
        instance = self.identity['instanceId']
        if (role not in ROLES - {'controller'} or expected.get(instance) != role
                or proof['instance_id'] != instance or proof['role'] != role
                or proof['credential_role'] != instance_profile(self.plan, role)['role_name']
                or proof['bundle_sha256'] != bundle_sha256 or self.plan['bundle_sha256'] != bundle_sha256
                or (self.root / 'bundle-ready').read_text() != bundle_sha256
                or stamp(deadline) > self.deadline or proof['deadline_utc'] != self.status['deadline_utc']):
            raise ValueError('storage ready marker/role/deadline binding differs')
        return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'verify-ready'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--expected')
    parser.add_argument('--deadline')
    parser.add_argument('--bundle-sha256')
    parser.add_argument('--wait-seconds', type=int, default=60)
    args = parser.parse_args()
    guest = Guest(json.loads(args.plan.read_text()))
    if args.action == 'start':
        guest.execute()
    else:
        print(json.dumps(guest.verify_ready(json.loads(args.expected), args.deadline,
                                           args.bundle_sha256, args.wait_seconds)))


if __name__ == '__main__':
    main()
