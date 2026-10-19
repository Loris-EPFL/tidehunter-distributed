#!/usr/bin/env python3
"""Explicit, bounded benchmark phases over the installed AWS MCP adapter.

`init` and `plan` are local only. All AWS APIs use aws_mcp.api/tool; HTTP bodies
use MCP-issued presigned URLs. No direct local boto3 calls or CLI fallback.
Unknown mutation outcomes remain journaled; deterministic names/client tokens
allow reconciliation without creating another fleet. Run one phase at a time.
"""
import argparse
import base64
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import lzma
import os
from pathlib import Path
import re
import secrets
import shlex
import sys
import subprocess
import urllib.request
import stage_timing

HERE = Path(__file__).resolve().parent
ACCOUNT = '354775971805'
REGION = 'us-east-1'
OTHER_INSTANCES = ['i-0d01435030bb7bbac', 'i-048dd1dcd14bf7815']
SSM_CORE = 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
MAX_ARCHIVE = 4 << 30


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def durable_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.pending')
    with temporary.open('wb') as stream:
        stream.write(encoded(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def error_code(error):
    return (getattr(error, 'response', None) or {}).get('Error', {}).get('Code')


def ownership(run_id):
    return {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': run_id}


def aws_tags(tags):
    return [{'Key': key, 'Value': value} for key, value in tags.items()]


def check_tags(resource, wanted):
    actual = {item['Key']: item['Value'] for item in resource.get('Tags', resource.get('TagSet', []))}
    if any(actual.get(key) != value for key, value in wanted.items()):
        raise ValueError('resource does not carry all three exact ownership tags')


def init(directory, bundle, placement=False, campaign='overlap',
         az='us-east-1a', subnet='subnet-038ee7d3ae5be11d6', ttl_seconds=3600,
         efa_instance_type='c6id.32xlarge', checkpoint_scope='full'):
    if campaign not in ['overlap', 'read-path', 'efa', 'efa-cache', 'efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing']:
        raise ValueError('unknown benchmark campaign')
    if not re.fullmatch(r'us-east-1[a-f]', az) or not re.fullmatch(r'subnet-[a-f0-9]{17}', subnet):
        raise ValueError('invalid benchmark zone or subnet')
    scopes = (stage_timing.SCOPES if campaign == 'stage-timing' else ('full', 'core', 'recovery') if campaign in ('scalar-transport', 'transport-floor') else
              ('full', 'core', 'capacity', 'recovery') if campaign == 'cluster-ram' else ('full', 'recovery'))
    if checkpoint_scope not in scopes or (checkpoint_scope != 'full' and campaign not in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')):
        raise ValueError('selected scope is not supported by this campaign')
    lifetimes = (1500,) if campaign == 'stage-timing' else (2400, 3000, 3600) if checkpoint_scope == 'recovery' else (3000, 3600)
    if type(ttl_seconds) is not int or ttl_seconds not in lifetimes:
        raise ValueError('unreviewed fleet lifetime for the selected campaign scope')
    efa = campaign in ['efa', 'efa-cache', 'efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing']
    if (efa_instance_type not in ('c6id.32xlarge', 'g6.16xlarge')
            or (not efa and efa_instance_type != 'c6id.32xlarge')):
        raise ValueError('unsupported EFA hardware for this campaign')
    if (campaign in ('checkpoint-async', 'cluster-ram', 'scalar-transport')
            and (efa_instance_type != 'g6.16xlarge' or az != 'us-east-1a')):
        raise ValueError('campaign requires reviewed G6 hardware and its planned zone')
    if campaign == 'stage-timing' and efa_instance_type != 'c6id.32xlarge':
        raise ValueError('stage timing requires six C6id hosts with cluster placement')
    directory, bundle = Path(directory).resolve(), Path(bundle).resolve()
    manifest = json.loads(bundle.with_name('upload-manifest.json').read_text())
    if manifest['archive'] != bundle.name or digest(bundle) != manifest['sha256'] or bundle.stat().st_size != manifest['bytes']:
        raise ValueError('bundle differs from its source-bound upload manifest')
    if not 0 < manifest['bytes'] <= 512 << 20:
        raise ValueError('bundle exceeds 512 MiB local/guest bound')
    run = 'th-' + secrets.token_hex(8)
    plan = {'schema': 1, 'campaign': campaign, 'run_id': run, 'created_at': now(), 'account': ACCOUNT,
            'region': REGION, 'profile': 'tidehunter', 'ttl_seconds': ttl_seconds,
            'checkpoint_scope': checkpoint_scope,
            'export_reserve_seconds': 600, 'reserve_usd': 5,
            'estimated_hourly_usd': 6 * (.343 + .005 + 32 * .08 / 730),
            'instance_type': 'i4i.xlarge', 'instance_count': 6,
            'ami': 'ami-0045d7fc2ad003464', 'subnet': subnet,
            'vpc': 'vpc-04d440613282b7bd0', 'az': az,
            'placement_group': run if placement else None,
            'bucket': ACCOUNT + '-' + run, 'prefix': run,
            'bundle': str(bundle), 'bundle_sha256': manifest['sha256'], 'bundle_bytes': manifest['bytes'],
            'bootstrap_sha256': digest(HERE / 'bootstrap.py'),
            'tags': ownership(run), 'protected_instances': OTHER_INSTANCES,
            'roles': {role: run + '-' + role for role in ('controller', 'node')}}
    if efa:
        # AWS Pricing GetProducts, Linux/shared us-east-1, verified 2026-10-12.
        hourly = 6.4512 if efa_instance_type == 'c6id.32xlarge' else 3.3968
        plan.update(instance_type=efa_instance_type, reserve_usd=42 * ttl_seconds // 3600,
                    estimated_hourly_usd=6 * (hourly + .005 + 32 * .08 / 730),
                    placement_group=run, efa=True,
                    placement_strategy='spread' if campaign == 'transport-floor' or efa_instance_type == 'g6.16xlarge' else 'cluster')
    if campaign == 'stage-timing':
        plan.update(export_reserve_seconds=300, reserve_usd=18,
                    source_identities=stage_timing.bundle_identities(bundle))
    directory.mkdir(mode=0o700)
    durable_json(directory / 'plan.json', plan)
    durable_json(directory / 'state.json', {'plan_sha256': hashlib.sha256(encoded(plan)).hexdigest(),
                                         'intents': {}, 'resources': {}, 'events': []})
    return plan


class Control:
    def __init__(self, directory, adapter=None):
        self.directory = Path(directory).resolve()
        self.plan = json.loads((self.directory / 'plan.json').read_text())
        self.state = json.loads((self.directory / 'state.json').read_text())
        self.adapter = adapter
        p = self.plan
        if self.state['plan_sha256'] != hashlib.sha256(encoded(p)).hexdigest():
            raise ValueError('immutable plan has changed')
        if not re.fullmatch(r'th-[a-f0-9]{16}', p['run_id']) or p['tags'] != ownership(p['run_id']):
            raise ValueError('invalid ownership identity')
        lifetimes = (1500,) if p['campaign'] == 'stage-timing' else (2400, 3000, 3600) if p.get('checkpoint_scope') == 'recovery' and p['campaign'] in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor') else (3000, 3600)
        if (p['account'] != ACCOUNT or p['region'] != REGION or p['instance_count'] != 6
                or p['ttl_seconds'] not in lifetimes):
            raise ValueError('unsupported run scope')
        if p['campaign'] == 'stage-timing':
            stage_timing.validate_configuration(p | {'availability_zone': p['az'], 'terminate_on_finish': True})

    def save(self):
        durable_json(self.directory / 'state.json', self.state)

    def intent(self, name, params):
        fingerprint = hashlib.sha256(encoded(params)).hexdigest()
        previous = self.state['intents'].get(name)
        if previous and previous['sha256'] != fingerprint:
            raise ValueError('attempted to change immutable mutation: ' + name)
        if not previous:
            self.state['intents'][name] = {'sha256': fingerprint, 'at': now()}
            self.save()

    def remember(self, name, value):
        self.state['resources'][name] = value
        self.save()
        return value

    def mcp(self):
        if self.adapter is None:
            path = HERE / 'mcp_adapter.py'
            spec = importlib.util.spec_from_file_location('sharded_wal_aws_mcp', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.adapter = module
        return self.adapter

    def api(self, service, operation, **params):
        return self.mcp().api(service, operation, params, self.plan['region'], self.plan['profile'])

    def mutate(self, name, service, operation, **params):
        self.intent(name, {'service': service, 'operation': operation, 'params': params})
        result = self.api(service, operation, **params)
        self.state['events'].append({'name': name, 'completed_at': now()})
        self.save()
        return result

    def optional(self, service, operation, missing, **params):
        try:
            return self.api(service, operation, **params)
        except Exception as error:
            if error_code(error) in missing:
                return None
            raise

    def identity(self):
        if self.api('sts', 'GetCallerIdentity')['Account'] != self.plan['account']:
            raise ValueError('AWS account differs from immutable plan')

    def filters(self):
        return [{'Name': 'tag:' + key, 'Values': [value]} for key, value in self.plan['tags'].items()]

    def instances(self):
        data = self.api('ec2', 'DescribeInstances', Filters=self.filters())
        result = [i for r in data.get('Reservations', []) for i in r['Instances']]
        for item in result:
            check_tags(item, self.plan['tags'])
            if item['InstanceId'] in self.plan['protected_instances']:
                raise ValueError('protected instance in owned fleet')
        if len(result) > 6:
            raise ValueError('more than six owned instances; operator reconciliation required')
        return result

    def protected(self):
        data = self.api('ec2', 'DescribeInstances', InstanceIds=self.plan['protected_instances'])
        return {i['InstanceId']: i['State']['Name'] for r in data['Reservations'] for i in r['Instances']}

    def provision(self):
        self.identity()
        p = self.plan
        if not self.state.get('protected_before'):
            self.state['protected_before'] = self.protected()
            self.save()
        bucket = p['bucket']
        present = self.optional('s3', 'HeadBucket', {'404', 'NoSuchBucket', 'NotFound'},
                                Bucket=bucket, ExpectedBucketOwner=p['account'])
        if present is None:
            self.mutate('create-bucket', 's3', 'CreateBucket', Bucket=bucket)
        elif 'create-bucket' not in self.state['intents']:
            raise ValueError('bucket already exists without this run create intent')
        self.remember('bucket', bucket)
        self.mutate('tag-bucket', 's3', 'PutBucketTagging', Bucket=bucket, ExpectedBucketOwner=p['account'],
                    Tagging={'TagSet': aws_tags(p['tags'])})
        self.mutate('private-bucket', 's3', 'PutPublicAccessBlock', Bucket=bucket, ExpectedBucketOwner=p['account'],
                    PublicAccessBlockConfiguration={k: True for k in ['BlockPublicAcls', 'IgnorePublicAcls', 'BlockPublicPolicy', 'RestrictPublicBuckets']})
        self.mutate('encrypt-bucket', 's3', 'PutBucketEncryption', Bucket=bucket, ExpectedBucketOwner=p['account'],
                    ServerSideEncryptionConfiguration={'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})
        self.mutate('owner-bucket', 's3', 'PutBucketOwnershipControls', Bucket=bucket, ExpectedBucketOwner=p['account'],
                    OwnershipControls={'Rules': [{'ObjectOwnership': 'BucketOwnerEnforced'}]})
        bucket_policy = {'Version': '2012-10-17', 'Statement': [{'Effect': 'Deny', 'Principal': '*', 'Action': 's3:*',
            'Resource': [f'arn:aws:s3:::{bucket}', f'arn:aws:s3:::{bucket}/*'], 'Condition': {'Bool': {'aws:SecureTransport': 'false'}}}]}
        self.mutate('tls-bucket', 's3', 'PutBucketPolicy', Bucket=bucket, ExpectedBucketOwner=p['account'], Policy=json.dumps(bucket_policy))
        groups = self.api('ec2', 'DescribeSecurityGroups', Filters=[{'Name': 'group-name', 'Values': [p['run_id']]}, {'Name': 'vpc-id', 'Values': [p['vpc']]}])['SecurityGroups']
        if not groups:
            result = self.mutate('create-sg', 'ec2', 'CreateSecurityGroup', GroupName=p['run_id'],
                Description='Scoped sharded WAL benchmark', VpcId=p['vpc'],
                TagSpecifications=[{'ResourceType': 'security-group', 'Tags': aws_tags(p['tags'])}])
            group = {'GroupId': result['GroupId'], 'Tags': aws_tags(p['tags'])}
        elif len(groups) == 1:
            group = groups[0]
            check_tags(group, p['tags'])
        else:
            raise ValueError('ambiguous security group')
        sg = self.remember('security_group', group['GroupId'])
        current = self.api('ec2', 'DescribeSecurityGroups', GroupIds=[sg])['SecurityGroups'][0]
        check_tags(current, p['tags'])
        self_rule = ({'IpProtocol': '-1', 'UserIdGroupPairs': [{'GroupId': sg}]} if p.get('efa') else
                     {'IpProtocol': 'tcp', 'FromPort': 7700, 'ToPort': 7700, 'UserIdGroupPairs': [{'GroupId': sg}]})
        for kind, permissions in [('Ingress', [self_rule]), ('Egress', [self_rule] + [
                {'IpProtocol': 'tcp', 'FromPort': port, 'ToPort': port, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]} for port in (80, 443)])]:
            # New groups have all-egress. Revoke that one rule; never touch another SG.
            if kind == 'Egress':
                for item in current.get('IpPermissionsEgress', []):
                    if (item['IpProtocol'] == '-1' and not item.get('UserIdGroupPairs')
                            and any(r.get('CidrIp') == '0.0.0.0/0' for r in item.get('IpRanges', []))):
                        try:
                            self.mutate('revoke-default-egress', 'ec2', 'RevokeSecurityGroupEgress', GroupId=sg, IpPermissions=[item])
                        except Exception as error:
                            if error_code(error) != 'InvalidPermission.NotFound':
                                raise
            for number, permission in enumerate(permissions):
                try:
                    self.mutate('allow-' + kind + str(number), 'ec2', 'AuthorizeSecurityGroup' + kind, GroupId=sg, IpPermissions=[permission])
                except Exception as error:
                    if error_code(error) != 'InvalidPermission.Duplicate':
                        raise
        trust = {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Principal': {'Service': 'ec2.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]}
        for kind, role in p['roles'].items():
            data = self.optional('iam', 'GetRole', {'NoSuchEntity'}, RoleName=role)
            if data is None:
                data = self.mutate('create-role-' + kind, 'iam', 'CreateRole', RoleName=role,
                    AssumeRolePolicyDocument=json.dumps(trust), Tags=aws_tags(p['tags']))
            check_tags(data['Role'], p['tags'])
            self.remember('role-' + kind, role)
            self.mutate('ssm-' + kind, 'iam', 'AttachRolePolicy', RoleName=role, PolicyArn=SSM_CORE)
            policy = role_policy(p, kind)
            durable_json(self.directory / ('policy-' + kind + '.json'), policy)
            self.mutate('base-policy-' + kind, 'iam', 'PutRolePolicy', RoleName=role,
                        PolicyName='run-scope', PolicyDocument=json.dumps(policy))
            data = self.optional('iam', 'GetInstanceProfile', {'NoSuchEntity'}, InstanceProfileName=role)
            if data is None:
                data = self.mutate('profile-' + kind, 'iam', 'CreateInstanceProfile', InstanceProfileName=role, Tags=aws_tags(p['tags']))
            check_tags(data['InstanceProfile'], p['tags'])
            roles = data['InstanceProfile'].get('Roles', [])
            if roles and [r['RoleName'] for r in roles] != [role]:
                raise ValueError('unexpected instance profile membership')
            if not roles:
                self.mutate('profile-role-' + kind, 'iam', 'AddRoleToInstanceProfile', InstanceProfileName=role, RoleName=role)
            self.remember('profile-' + kind, role)
        if p['placement_group']:
            data = self.optional('ec2', 'DescribePlacementGroups', {'InvalidPlacementGroup.Unknown'}, GroupNames=[p['placement_group']])
            if not data or not data['PlacementGroups']:
                self.mutate('placement', 'ec2', 'CreatePlacementGroup', GroupName=p['placement_group'],
                            Strategy=p.get('placement_strategy', 'cluster'),
                            TagSpecifications=[{'ResourceType': 'placement-group', 'Tags': aws_tags(p['tags'])}])
            else:
                check_tags(data['PlacementGroups'][0], p['tags'])
                if p['campaign'] == 'stage-timing' and (len(data['PlacementGroups']) != 1
                        or data['PlacementGroups'][0].get('Strategy') != 'cluster'):
                    raise ValueError('existing placement group is not the planned cluster strategy')
            self.remember('placement_group', p['placement_group'])
        return self.state['resources']

    def presigned(self, key, operation, params=None):
        p = self.plan
        if not key.startswith(p['prefix'] + '/'):
            raise ValueError('S3 key outside this run')
        request = {'operation': operation, 'bucket': p['bucket'], 'key': key,
                   'region': p['region'], 'expires_in': 900, 's3_params': params or {}}
        response = self.mcp().tool('aws___get_presigned_url', {'requests': [request]}, p['region'], p['profile'])
        items = response.get('result')
        if not isinstance(items, list) or len(items) != 1:
            raise ValueError('MCP did not return one presigned object request')
        url = items[0].get('url') or items[0].get('presigned_url')
        if not isinstance(url, str) or not url.startswith('https://'):
            raise ValueError('invalid presigned URL')
        return url

    def upload(self):
        p = self.plan
        archive = Path(p['bundle'])
        if archive.stat().st_size != p['bundle_bytes'] or digest(archive) != p['bundle_sha256']:
            raise ValueError('immutable bundle changed')
        key = p['prefix'] + '/input/bundle.tar.gz'
        checksum = base64.b64encode(bytes.fromhex(p['bundle_sha256'])).decode()
        self.intent('upload-bundle', {'key': key, 'sha256': p['bundle_sha256']})
        existing = self.optional('s3', 'HeadObject', {'404', 'NoSuchKey', 'NotFound'}, Bucket=p['bucket'], Key=key,
                                 ExpectedBucketOwner=p['account'], ChecksumMode='ENABLED')
        if not existing or existing.get('ContentLength') != p['bundle_bytes'] or existing.get('ChecksumSHA256') != checksum:
            url = self.presigned(key, 'upload', {'ContentType': 'application/gzip', 'ServerSideEncryption': 'AES256', 'ChecksumSHA256': checksum})
            with archive.open('rb') as body:
                request = urllib.request.Request(url, data=body, method='PUT', headers={
                    'Content-Length': str(p['bundle_bytes']), 'Content-Type': 'application/gzip',
                    'x-amz-server-side-encryption': 'AES256', 'x-amz-checksum-sha256': checksum})
                try:
                    with urllib.request.urlopen(request, timeout=45) as response:
                        response.read(1024)
                except Exception:
                    raise RuntimeError('S3 upload transport failed; outcome unknown; rerun upload to reconcile HEAD') from None
        observed = self.api('s3', 'HeadObject', Bucket=p['bucket'], Key=key, ExpectedBucketOwner=p['account'], ChecksumMode='ENABLED')
        if observed.get('ContentLength') != p['bundle_bytes'] or observed.get('ChecksumSHA256') != checksum:
            raise ValueError('S3 checksum/length differs from immutable bundle')
        return self.remember('uploaded_bundle', {'key': key, 'sha256': p['bundle_sha256'], 'bytes': p['bundle_bytes']})

    def launch(self):
        self.identity()
        p = self.plan
        if not self.state['resources'].get('uploaded_bundle'):
            raise ValueError('upload and verify the source bundle before launch')
        if digest(HERE / 'bootstrap.py') != p['bootstrap_sha256']:
            raise ValueError('bootstrap changed since immutable plan')
        userdata = user_data(p)
        if len(userdata.encode()) > 16384:
            raise ValueError('EC2 user data exceeds 16 KiB')
        existing = self.instances()
        if any(i.get('ClientToken') not in (p['run_id'] + '-node', p['run_id'] + '-controller') for i in existing):
            raise ValueError('unexpected owned instance token; reconcile before launching')
        for kind, count in [('node', 5), ('controller', 1)]:
            token = p['run_id'] + '-' + kind
            found = [i for i in existing if i.get('ClientToken') == token]
            if found:
                if len(found) != count:
                    raise ValueError('partial fleet must be reconciled; no replacement launch')
                continue
            params = {'ImageId': p['ami'], 'InstanceType': p['instance_type'], 'MinCount': count, 'MaxCount': count,
                'ClientToken': token, 'IamInstanceProfile': {'Name': p['roles'][kind]},
                'NetworkInterfaces': [{'DeviceIndex': 0, 'SubnetId': p['subnet'], 'Groups': [self.state['resources']['security_group']],
                                       'AssociatePublicIpAddress': True, 'DeleteOnTermination': True}],
                'Placement': {'AvailabilityZone': p['az']},
                'BlockDeviceMappings': [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 32, 'VolumeType': 'gp3',
                    'Encrypted': True, 'DeleteOnTermination': True}}],
                'MetadataOptions': {'HttpTokens': 'required', 'HttpEndpoint': 'enabled', 'HttpPutResponseHopLimit': 1},
                'InstanceInitiatedShutdownBehavior': 'terminate',
                'TagSpecifications': [{'ResourceType': rt, 'Tags': aws_tags(p['tags'] | {'Name': token, 'Role': kind})}
                                      for rt in ('instance', 'volume')], 'UserData': userdata}
            if p.get('efa'):
                params['NetworkInterfaces'][0].update(InterfaceType='efa', NetworkCardIndex=0)
            if p['placement_group']:
                params['Placement']['GroupName'] = p['placement_group']
            old = self.state['intents'].get('launch-' + kind)
            if old and (datetime.now(timezone.utc) - datetime.fromisoformat(old['at'])).total_seconds() > p['ttl_seconds']:
                raise ValueError('old uncertain launch must be inspected; never reuse its token after deadline')
            self.mutate('launch-' + kind, 'ec2', 'RunInstances', **params)
        return self.status()

    def status(self):
        instances = self.instances()
        summaries = [{'id': i['InstanceId'], 'state': i['State']['Name'], 'private_ip': i.get('PrivateIpAddress'),
                      'role': next((t['Value'] for t in i.get('Tags', []) if t['Key'] == 'Role'), None),
                      'launch_time': str(i['LaunchTime']), 'volumes': [b['Ebs']['VolumeId'] for b in i.get('BlockDeviceMappings', []) if 'Ebs' in b]}
                     for i in instances]
        self.remember('instances', summaries)
        return {'run_id': self.plan['run_id'], 'instances': summaries, 'protected_now': self.protected(),
                'protected_before': self.state.get('protected_before')}

    def readiness(self):
        """Submit one bounded read-only SSM probe to the six exact owned guests."""
        instances = self.instances()
        if len(instances) != 6 or any(i['State']['Name'] != 'running' for i in instances):
            raise ValueError('readiness requires the six running owned instances')
        ids = sorted(i['InstanceId'] for i in instances)
        attempt = 1 + self.state.get('readiness_attempt', 0)
        self.state['readiness_attempt'] = attempt
        self.state['resources'].pop('readiness_verified', None)
        self.save()
        p = self.plan
        result = self.mutate('readiness-' + str(attempt), 'ssm', 'SendCommand', InstanceIds=ids,
            DocumentName='AWS-RunShellScript', Parameters={'commands': [readiness_command(p)], 'executionTimeout': ['30']},
            TimeoutSeconds=60, Comment=p['run_id'] + '-readiness-' + str(attempt),
            OutputS3BucketName=p['bucket'], OutputS3KeyPrefix=p['prefix'] + '/ssm/readiness')
        probe = {'command_id': result['Command']['CommandId'], 'instance_ids': ids, 'requested_at': now()}
        return self.remember('readiness_command', probe)

    def readiness_status(self):
        probe = self.state['resources'].get('readiness_command')
        if not probe:
            raise ValueError('submit a readiness probe first')
        self.state['resources'].pop('readiness_verified', None)
        self.save()
        replies, pending = [], []
        for instance in probe['instance_ids']:
            result = self.optional('ssm', 'GetCommandInvocation', {'InvocationDoesNotExist'},
                                  CommandId=probe['command_id'], InstanceId=instance)
            if result is None or result.get('Status') in ('Pending', 'InProgress', 'Delayed'):
                pending.append(instance)
                continue
            if result.get('Status') != 'Success' or result.get('ResponseCode') != 0:
                self.state['resources'].pop('readiness_verified', None)
                self.save()
                return {'ready': False, 'instance_id': instance, 'status': result.get('Status'),
                        'probe_command_id': probe['command_id'], 'retry': 'Run readiness again after bootstrap has completed.'}
            reply = json.loads(result['StandardOutputContent'])
            if reply['instance_id'] != instance or reply['run_id'] != self.plan['run_id'] or reply['bundle_sha256'] != self.plan['bundle_sha256']:
                raise ValueError('guest readiness identity or bundle differs from immutable run')
            if reply['account'] != self.plan['account'] or reply['mount_target'] != '/mnt/tidehunter' or reply['model'] != 'Amazon EC2 NVMe Instance Storage':
                raise ValueError('guest readiness hardware/account differs from required topology')
            deadline = datetime.fromisoformat(reply['deadline_utc'].replace('Z', '+00:00'))
            if (deadline - datetime.now(timezone.utc)).total_seconds() < 1200:
                raise ValueError('guest has insufficient original lifetime remaining')
            replies.append(reply)
        if pending:
            return {'ready': False, 'pending': pending, 'probe_command_id': probe['command_id']}
        if len(replies) != 6:
            raise ValueError('incomplete readiness set')
        return self.remember('readiness_verified', {'ready': True, 'checked_at': now(),
            'command_id': probe['command_id'], 'instance_ids': probe['instance_ids'], 'guests': replies})

    def start(self):
        if 'ssh-start-controller' in self.state['intents']:
            raise ValueError('SSH start already requested; reconcile progress rather than resending')
        p = self.plan
        instances = self.instances()
        groups = self.api('ec2', 'DescribePlacementGroups', GroupNames=[p['placement_group']])['PlacementGroups'] if p['campaign'] == 'stage-timing' else None
        cfg, controller = fleet_configuration(p, instances, groups)
        nodes = cfg['nodes']
        verified = self.state['resources'].get('readiness_verified')
        if not verified or sorted(i['InstanceId'] for i in instances) != verified['instance_ids']:
            raise ValueError('verify readiness of all six actual guests before requesting controller start')
        if (datetime.now(timezone.utc) - datetime.fromisoformat(verified['checked_at'])).total_seconds() > 300:
            raise ValueError('readiness proof is older than five minutes; run a fresh read-only probe')
        # Conservative bound precedes guest pendingTime-derived timer; never refresh TTL.
        deadline = datetime.fromisoformat(cfg['deadline_utc'])
        deadline = min(deadline, *(datetime.fromisoformat(g['deadline_utc'].replace('Z', '+00:00')) for g in verified['guests']))
        remaining = int((deadline - datetime.now(timezone.utc)).total_seconds())
        if remaining < 1200:
            raise ValueError('insufficient immutable lifetime for build/workload/export')
        policy = command_policy(p, [node['instance_id'] for node in nodes])
        durable_json(self.directory / 'policy-controller-command.json', policy)
        self.mutate('command-policy', 'iam', 'PutRolePolicy', RoleName=p['roles']['controller'],
                    PolicyName='exact-node-command', PolicyDocument=json.dumps(policy))
        cfg['deadline_utc'] = deadline.isoformat()
        durable_json(self.directory / 'controller.json', cfg)
        command = start_command(cfg, remaining)
        key = 'start-controller'
        if key in self.state['intents']:
            # SendCommand has no idempotency token. Do not resend an uncertain request.
            return {'start_outcome': 'already requested; inspect command/progress',
                    'command_id': self.state['resources'].get('controller_command_id')}
        result = self.mutate(key, 'ssm', 'SendCommand', InstanceIds=[controller['InstanceId']],
            DocumentName='AWS-RunShellScript', Parameters={'commands': [command], 'executionTimeout': ['60']},
            TimeoutSeconds=60, Comment=p['run_id'] + '-start-controller',
            OutputS3BucketName=p['bucket'], OutputS3KeyPrefix=p['prefix'] + '/ssm/start')
        return self.remember('controller_command_id', result['Command']['CommandId'])

    def ssh_readiness_start(self, cidr):
        """Use the permitted EIC path; never expand the operator's IAM permissions."""
        cidr = exact_public_ipv4(cidr)
        if any(name in self.state['intents'] for name in ('start-controller', 'ssh-start-controller')):
            raise ValueError('a start was already requested; reconcile progress rather than resending')
        self.identity()
        p = self.plan
        instances = self.instances()
        groups = self.api('ec2', 'DescribePlacementGroups', GroupNames=[p['placement_group']])['PlacementGroups'] if p['campaign'] == 'stage-timing' else None
        cfg, controller = fleet_configuration(p, instances, groups)
        durable_json(self.directory / 'controller.json', cfg)
        public_ip = str(ipaddress.IPv4Address(controller['PublicIpAddress']))
        group_id = self.state['resources']['security_group']
        group = self.api('ec2', 'DescribeSecurityGroups', GroupIds=[group_id])['SecurityGroups'][0]
        check_tags(group, p['tags'])
        if group['GroupName'] != p['run_id'] or group['VpcId'] != p['vpc']:
            raise ValueError('SSH ingress group differs from owned run')
        role = self.api('iam', 'GetRole', RoleName=p['roles']['controller'])['Role']
        check_tags(role, p['tags'])
        policy = command_policy(p, [n['instance_id'] for n in cfg['nodes']])
        durable_json(self.directory / 'policy-controller-command.json', policy)
        self.mutate('command-policy', 'iam', 'PutRolePolicy', RoleName=p['roles']['controller'],
                    PolicyName='exact-node-command', PolicyDocument=json.dumps(policy))
        console = self.api('ec2', 'GetConsoleOutput', InstanceId=controller['InstanceId'], Latest=True)
        if console.get('InstanceId') != controller['InstanceId']:
            raise ValueError('console response belongs to a different instance')
        host_key = console_ed25519_key(console['Output'])
        directory = self.directory / 'ssh'
        directory.mkdir(mode=0o700, exist_ok=True)
        os.chmod(directory, 0o700)
        (directory / (controller['InstanceId'] + '.console.log')).write_text(console['Output'])
        known_hosts = directory / 'known_hosts'
        line = public_ip + ' ' + host_key + '\n'
        if known_hosts.exists() and known_hosts.read_text() != line:
            raise ValueError('pinned controller host identity changed')
        known_hosts.write_text(line)
        key = directory / 'ephemeral'
        if key.exists() != key.with_suffix('.pub').exists():
            raise ValueError('incomplete run-local ephemeral key pair')
        if not key.exists():
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', p['run_id'], '-f', str(key)],
                           check=True, timeout=15, capture_output=True)
        if key.stat().st_mode & 0o077:
            raise ValueError('ephemeral private key permissions are too broad')
        public_key = key.with_suffix('.pub').read_text().strip()
        if not public_key.startswith('ssh-ed25519 '):
            raise ValueError('unexpected run-local key type')
        script = ssh_start_script(p, cfg)
        (directory / 'controller-readiness-start.sh').write_text(script)
        permission = {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': cidr}]}
        # Record the exact rule before authorizing: a lost response still permits
        # safe revocation. No other ingress rule is ever changed by this phase.
        self.remember('ssh_ingress', {'group_id': group_id, 'cidr': cidr, 'closed': False})
        try:
            try:
                self.mutate('ssh-ingress', 'ec2', 'AuthorizeSecurityGroupIngress', GroupId=group_id, IpPermissions=[permission])
            except Exception as error:
                if error_code(error) != 'InvalidPermission.Duplicate':
                    raise
            self.intent('ssh-start-controller', {'instance_id': controller['InstanceId'],
                'script_sha256': hashlib.sha256(script.encode()).hexdigest(), 'cidr': cidr})
            result = self.api('ec2-instance-connect', 'SendSSHPublicKey', InstanceId=controller['InstanceId'],
                InstanceOSUser='ubuntu', AvailabilityZone=p['az'], SSHPublicKey=public_key)
            if not result.get('Success'):
                raise ValueError('EC2 Instance Connect did not authorize the ephemeral key')
            command = ssh_command(directory, public_ip)
            try:
                result = subprocess.run(command, input=script, text=True, capture_output=True, timeout=180)
            except subprocess.TimeoutExpired as error:
                for name, value in [('stdout', error.stdout), ('stderr', error.stderr)]:
                    (directory / ('start.' + name + '.log')).write_bytes(value.encode() if isinstance(value, str) else value or b'')
                raise RuntimeError('SSH start response timed out; do not resend; inspect S3 progress') from None
            (directory / 'start.stdout.log').write_text(result.stdout)
            (directory / 'start.stderr.log').write_text(result.stderr)
            if result.returncode:
                raise RuntimeError('SSH start failed or is uncertain; inspect run-local logs and S3 progress')
            proof = json.loads(result.stdout)
            if proof.get('started') is not True or proof.get('deadline_utc') != cfg['deadline_utc']:
                raise ValueError('controller did not return its fixed-deadline start receipt')
            if {r['instance_id'] for r in proof['ready']['guests']} != {i['InstanceId'] for i in instances}:
                raise ValueError('returned readiness receipt has different fleet members')
            return self.remember('ssh_controller_start', proof)
        finally:
            # Cost/scope safety must survive a lost SSH or EIC response. If this
            # MCP call fails, the persisted exact rule is resumable via ssh-close.
            self.ssh_close()

    def ssh_close(self):
        ingress = self.state['resources'].get('ssh_ingress')
        if ingress and not ingress.get('closed'):
            self.identity()
            if ingress['group_id'] != self.state['resources']['security_group']:
                raise ValueError('SSH cleanup group differs from owned run')
            cidr = exact_public_ipv4(ingress['cidr'])
            groups = self.optional('ec2', 'DescribeSecurityGroups', {'InvalidGroup.NotFound'}, GroupIds=[ingress['group_id']])
            if groups:
                group = groups['SecurityGroups'][0]
                check_tags(group, self.plan['tags'])
                if group['GroupName'] != self.plan['run_id'] or group['VpcId'] != self.plan['vpc']:
                    raise ValueError('refusing to revoke ingress on another group')
                try:
                    self.mutate('ssh-revoke', 'ec2', 'RevokeSecurityGroupIngress', GroupId=ingress['group_id'],
                        IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': cidr}]}])
                except Exception as error:
                    if error_code(error) != 'InvalidPermission.NotFound':
                        raise
            self.remember('ssh_ingress', ingress | {'closed': True, 'closed_at': now()})
        for name in ('ephemeral', 'ephemeral.pub'):
            (self.directory / 'ssh' / name).unlink(missing_ok=True)
        return {'ssh_ingress_closed': not ingress or self.state['resources']['ssh_ingress']['closed']}

    def command_status(self):
        p = self.plan
        command = self.state['resources'].get('controller_command_id')
        if not command:
            intent = self.state['intents'].get('start-controller')
            if not intent:
                return {'start_outcome': 'not requested'}
            commands = self.api('ssm', 'ListCommands', Filters=[{'key': 'InvokedAfter', 'value': intent['at']}]).get('Commands', [])
            found = [x for x in commands if x.get('Comment') == p['run_id'] + '-start-controller']
            if len(found) != 1:
                return {'start_outcome': 'unknown', 'matching_commands': len(found)}
            command = self.remember('controller_command_id', found[0]['CommandId'])
        # SSM output only contains launch status; no TLS private material or signed URLs.
        return self.api('ssm', 'ListCommandInvocations', CommandId=command, Details=True)

    def download(self, key, target, limit):
        p = self.plan
        info = self.api('s3', 'HeadObject', Bucket=p['bucket'], Key=key, ExpectedBucketOwner=p['account'])
        size = info['ContentLength']
        if not 0 <= size <= limit:
            raise ValueError('S3 object exceeds bounded download')
        url = self.presigned(key, 'download', {'ExpectedBucketOwner': p['account']})
        target = Path(target)
        temporary = target.with_suffix(target.suffix + '.partial')
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'x-amz-expected-bucket-owner': p['account']}), timeout=45) as response, temporary.open('wb') as out:
                received = 0
                while received < size:
                    chunk = response.read(min(1 << 20, size - received))
                    if not chunk:
                        raise EOFError('incomplete object')
                    out.write(chunk)
                    received += len(chunk)
                if response.read(1):
                    raise ValueError('object length changed')
                out.flush()
                os.fsync(out.fileno())
            os.replace(temporary, target)
        except Exception:
            raise RuntimeError('S3 download incomplete; retry the same phase; URLs withheld') from None
        finally:
            temporary.unlink(missing_ok=True)
        return target

    def progress(self):
        # Progress is a mutable snapshot. A preceding HEAD can describe a prior
        # version by the time the signed GET runs; bound this one response instead.
        p = self.plan
        url = self.presigned(p['prefix'] + '/results/progress.json', 'download',
                             {'ExpectedBucketOwner': p['account']})
        try:
            request = urllib.request.Request(url, headers={'x-amz-expected-bucket-owner': p['account']})
            with urllib.request.urlopen(request, timeout=45) as response:
                body = response.read((2 << 20) + 1)
        except Exception:
            raise RuntimeError('S3 progress snapshot unavailable; retry polling; URLs withheld') from None
        if len(body) > 2 << 20:
            raise ValueError('progress snapshot exceeds 2 MiB')
        snapshot = json.loads(body)
        if not isinstance(snapshot, dict) or snapshot.get('run_id') != p['run_id']:
            raise ValueError('progress snapshot belongs to a different run')
        durable_json(self.directory / 'progress.json', snapshot)
        return snapshot

    def retrieve(self):
        p = self.plan
        receipt_path = self.download(p['prefix'] + '/results/receipt.json', self.directory / 'receipt.json', 1 << 20)
        receipt = json.loads(receipt_path.read_text())
        if receipt['bucket'] != p['bucket'] or receipt['key'] != p['prefix'] + '/results/controller.tar.gz':
            raise ValueError('result receipt points outside the expected archive')
        if not re.fullmatch('[a-f0-9]{64}', receipt['sha256']) or not 0 < receipt['bytes'] <= MAX_ARCHIVE:
            raise ValueError('invalid bounded archive receipt')
        target = self.directory / 'controller.tar.gz'
        if not target.exists() or target.stat().st_size != receipt['bytes'] or digest(target) != receipt['sha256']:
            self.download(receipt['key'], target, MAX_ARCHIVE)
        if target.stat().st_size != receipt['bytes'] or digest(target) != receipt['sha256']:
            raise ValueError('downloaded result archive failed actual-byte validation')
        self.remember('verified_results', receipt | {'local_archive': str(target), 'verified_at': now()})
        return receipt

    def terminate(self):
        self.identity()
        instances = self.instances()
        ids = sorted(i['InstanceId'] for i in instances if i['State']['Name'] not in ('terminated', 'shutting-down'))
        if ids:
            self.mutate('terminate-' + hashlib.sha256(encoded(ids)).hexdigest()[:12], 'ec2', 'TerminateInstances', InstanceIds=ids)
        return self.status()

    def cleanup(self, discard=False):
        self.identity()
        p = self.plan
        instances = self.instances()
        if any(i['State']['Name'] != 'terminated' for i in instances):
            raise ValueError('terminate first, then verify all exact instances terminated before cleanup')
        volumes = self.api('ec2', 'DescribeVolumes', Filters=self.filters()).get('Volumes', [])
        if volumes:
            raise ValueError('owned EBS volumes still exist; inspect DeleteOnTermination before cleanup')
        if not self.state['resources'].get('verified_results') and not discard:
            raise ValueError('retrieve verified results first, or explicitly --discard-results for a failed run')
        if self.state['resources'].get('verified_results'):
            verified = self.state['resources']['verified_results']
            path = Path(verified['local_archive'])
            if not path.exists() or path.stat().st_size != verified['bytes'] or digest(path) != verified['sha256']:
                raise ValueError('verified local result archive is missing or changed')
        for kind, role in p['roles'].items():
            profile = self.optional('iam', 'GetInstanceProfile', {'NoSuchEntity'}, InstanceProfileName=role)
            if profile:
                check_tags(profile['InstanceProfile'], p['tags'])
                for item in profile['InstanceProfile'].get('Roles', []):
                    if item['RoleName'] != role:
                        raise ValueError('unexpected profile role during cleanup')
                    self.mutate('detach-profile-' + kind, 'iam', 'RemoveRoleFromInstanceProfile', InstanceProfileName=role, RoleName=role)
                self.mutate('delete-profile-' + kind, 'iam', 'DeleteInstanceProfile', InstanceProfileName=role)
            data = self.optional('iam', 'GetRole', {'NoSuchEntity'}, RoleName=role)
            if data:
                check_tags(data['Role'], p['tags'])
                for name in self.api('iam', 'ListRolePolicies', RoleName=role)['PolicyNames']:
                    if name not in ('run-scope', 'exact-node-command'):
                        raise ValueError('unexpected policy on owned role')
                    self.mutate('delete-inline-' + kind + '-' + name, 'iam', 'DeleteRolePolicy', RoleName=role, PolicyName=name)
                for policy in self.api('iam', 'ListAttachedRolePolicies', RoleName=role)['AttachedPolicies']:
                    if policy['PolicyArn'] != SSM_CORE:
                        raise ValueError('unexpected managed policy on owned role')
                    self.mutate('detach-ssm-' + kind, 'iam', 'DetachRolePolicy', RoleName=role, PolicyArn=SSM_CORE)
                self.mutate('delete-role-' + kind, 'iam', 'DeleteRole', RoleName=role)
        groups = self.api('ec2', 'DescribeSecurityGroups', Filters=self.filters()).get('SecurityGroups', [])
        for group in groups:
            check_tags(group, p['tags'])
            if group['GroupName'] != p['run_id'] or group['VpcId'] != p['vpc']:
                raise ValueError('unexpected owned group')
            self.mutate('delete-sg', 'ec2', 'DeleteSecurityGroup', GroupId=group['GroupId'])
        if p['placement_group']:
            data = self.optional('ec2', 'DescribePlacementGroups', {'InvalidPlacementGroup.Unknown'}, GroupNames=[p['placement_group']])
            if data and data['PlacementGroups']:
                check_tags(data['PlacementGroups'][0], p['tags'])
                self.mutate('delete-placement', 'ec2', 'DeletePlacementGroup', GroupName=p['placement_group'])
        bucket = self.optional('s3', 'HeadBucket', {'404', 'NoSuchBucket', 'NotFound'}, Bucket=p['bucket'], ExpectedBucketOwner=p['account'])
        if bucket is not None:
            tags = self.api('s3', 'GetBucketTagging', Bucket=p['bucket'], ExpectedBucketOwner=p['account'])
            check_tags(tags, p['tags'])
            uploads = self.api('s3', 'ListMultipartUploads', Bucket=p['bucket'], ExpectedBucketOwner=p['account']).get('Uploads', [])
            for upload in uploads:
                if not upload['Key'].startswith(p['prefix'] + '/'):
                    raise ValueError('unexpected multipart upload outside owned prefix')
                self.mutate('abort-' + upload['UploadId'], 's3', 'AbortMultipartUpload', Bucket=p['bucket'], Key=upload['Key'], UploadId=upload['UploadId'], ExpectedBucketOwner=p['account'])
            # MCP can cap a paginated result. Re-listing after deletion handles
            # bounded partial pages safely without manual continuation tokens.
            for page_pass in range(32):
                objects = self.api('s3', 'ListObjectsV2', Bucket=p['bucket'], ExpectedBucketOwner=p['account']).get('Contents', [])
                if not objects:
                    break
                if any(not item['Key'].startswith(p['prefix'] + '/') for item in objects):
                    raise ValueError('unexpected object outside owned prefix')
                for start in range(0, len(objects), 1000):
                    keys = [{'Key': item['Key']} for item in objects[start:start + 1000]]
                    result = self.mutate('delete-objects-' + hashlib.sha256(encoded(keys)).hexdigest()[:16], 's3', 'DeleteObjects',
                        Bucket=p['bucket'], ExpectedBucketOwner=p['account'], Delete={'Objects': keys, 'Quiet': True})
                    if result.get('Errors'):
                        raise ValueError('some owned S3 objects failed deletion')
            else:
                raise ValueError('bounded S3 cleanup pass limit reached; inspect exact bucket before retry')
            self.mutate('delete-bucket', 's3', 'DeleteBucket', Bucket=p['bucket'], ExpectedBucketOwner=p['account'])
        protected_after = self.protected()
        report = {'completed_at': now(), 'terminated_instances': [i['InstanceId'] for i in instances], 'owned_volumes': [],
                  'protected_before': self.state.get('protected_before'), 'protected_after': protected_after,
                  'protected_states_unchanged': self.state.get('protected_before') == protected_after}
        durable_json(self.directory / 'cleanup-receipt.json', report)
        return report


def role_policy(p, kind):
    """Narrow the reviewed Autopilot baseline to this run; no PassRole/KMS needed."""
    bucket, prefix = p['bucket'], p['prefix']
    statements = [
        {'Effect': 'Allow', 'Action': ['s3:ListBucket', 's3:ListBucketMultipartUploads'], 'Resource': f'arn:aws:s3:::{bucket}',
         'Condition': {'StringLike': {'s3:prefix': [prefix, prefix + '/*']}}},
        {'Effect': 'Allow', 'Action': ['s3:GetObject', 's3:PutObject', 's3:AbortMultipartUpload', 's3:ListMultipartUploadParts'],
         'Resource': f'arn:aws:s3:::{bucket}/{prefix}/*'}]
    if kind == 'controller':
        statements += [{'Effect': 'Allow', 'Action': ['ec2:DescribeInstances', 'ssm:GetCommandInvocation'], 'Resource': '*'},
            {'Effect': 'Allow', 'Action': 'ec2:TerminateInstances', 'Resource': f"arn:aws:ec2:{p['region']}:{p['account']}:instance/*",
             'Condition': {'StringEquals': {'ec2:ResourceTag/' + k: v for k, v in p['tags'].items()}}}]
        if p.get('atomic_controller') is True:
            statements[2]['Action'] += ['ec2:DescribeInstanceAttribute', 'ec2:DescribePlacementGroups',
                                        'ssm:DescribeInstanceInformation']
        elif p.get('campaign') == 'stage-timing':
            statements.append({'Effect': 'Allow', 'Action': 'ec2:DescribePlacementGroups', 'Resource': '*'})
    return {'Version': '2012-10-17', 'Statement': statements}


def command_policy(p, ids):
    if len(ids) != 5 or len(set(ids)) != 5 or any(not re.fullmatch(r'i-[a-f0-9]{17}', value) for value in ids):
        raise ValueError('exactly five distinct storage instance IDs required')
    if any(value in p['protected_instances'] for value in ids):
        raise ValueError('protected instance in command targets')
    resources = [f"arn:aws:ec2:{p['region']}:{p['account']}:instance/{value}" for value in ids]
    return {'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Allow', 'Action': 'ssm:SendCommand', 'Resource': resources,
         'Condition': {'StringEquals': {'ssm:resourceTag/' + k: v for k, v in p['tags'].items()}}},
        {'Effect': 'Allow', 'Action': 'ssm:SendCommand', 'Resource': f"arn:aws:ssm:{p['region']}::document/AWS-RunShellScript"}]}


def efa_install_script(p):
    # Pinned official installer SHA256 from the AWS EFA verification table.
    # EFA uses the AMI's in-tree kernel module; the provider check must succeed.
    return f'''timeout 300 apt-get install -y pkg-config libclang-dev rdma-core
curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 300 --retry 2 -fsS https://efa-installer.amazonaws.com/aws-efa-installer-1.50.0.tar.gz -o /root/efa.tar.gz
printf '%s  %s\n' fa6dff8593d866866c13cb4640d9059835cd4efa427971f100ab40c97bef2841 /root/efa.tar.gz | sha256sum -c -
tar -xzf /root/efa.tar.gz -C /root
cd /root/aws-efa-installer
timeout 600 ./efa_installer.sh -y --skip-kmod --skip-mpi
modprobe efa
printf '%s\n' /opt/amazon/efa/lib > /etc/ld.so.conf.d/tidehunter-efa.conf
ldconfig
ulimit -l unlimited
FI_EFA_USE_DEVICE_RDMA=1 /opt/amazon/efa/bin/fi_info -p efa -t FI_EP_RDM -v > /mnt/tidehunter/{p['run_id']}/efa-provider.txt
/opt/amazon/efa/bin/fi_info --version > /mnt/tidehunter/{p['run_id']}/efa-version.txt
cd /root
'''


def user_data(p):
    bootstrap = base64.b64encode((HERE / 'bootstrap.py').read_bytes()).decode()
    efa_setup = efa_install_script(p) if p.get('efa') else ''
    # The first command after set -eu arms shutdown before apt, S3, Rust, or MCP.
    setup = f'''import boto3, hashlib, pathlib, tarfile
root = pathlib.Path('/mnt/tidehunter/{p['run_id']}')
archive = root / 'bundle.tar.gz'
s3 = boto3.client('s3', region_name={p['region']!r})
s3.download_file({p['bucket']!r}, {p['prefix'] + '/input/bundle.tar.gz'!r}, str(archive), ExtraArgs={{'ExpectedBucketOwner': {p['account']!r}}})
with archive.open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == {p['bundle_sha256']!r}, 'bundle checksum'
assert archive.stat().st_size == {p['bundle_bytes']}, 'bundle length'
with tarfile.open(archive) as stream:
    members = stream.getmembers()
    assert sum(m.size for m in members) <= 512 * 1024 * 1024, 'expanded bundle bound'
    for m in members:
        path = pathlib.Path(m.name)
        assert not path.is_absolute() and '..' not in path.parts and (m.isfile() or m.isdir()), 'unsafe archive member'
    stream.extractall(root, members=members, filter='data')
(root / 'bundle-ready').write_text({p['bundle_sha256']!r})
'''
    bootstrap_command = f'printf %s {shlex.quote(bootstrap)} | base64 -d > /root/sharded-wal-bootstrap.py'
    if p.get('atomic_controller') is True:
        from atomic_start import launch_contract
        # LZMA and Base85 keep the complete source/metadata capsule within the
        # checked EC2 wire limit. Boto3 owns API base64 encoding, so the result
        # remains an ordinary shell script rather than pre-encoded user data.
        capsule = base64.b85encode(lzma.compress(encoded({
            'bootstrap': (HERE / 'bootstrap.py').read_text(),
            'atomic': (HERE / 'atomic_start.py').read_text(),
            'plan': launch_contract(p),
        }))).decode()
        bootstrap_command = '/usr/bin/python3 -c ' + shlex.quote(
            "import base64,json,lzma,pathlib;"
            f"data=json.loads(lzma.decompress(base64.b85decode({capsule!r})));"
            "pathlib.Path('/root/sharded-wal-bootstrap.py').write_text(data['bootstrap']);"
            "pathlib.Path('/root/sharded-wal-atomic-start.py').write_text(data['atomic']);"
            "pathlib.Path('/root/sharded-wal-atomic-plan.json').write_text(json.dumps(data['plan']))")
        setup = f'''import pathlib, subprocess
root = pathlib.Path('/mnt/tidehunter/{p['run_id']}')
plan = root / 'atomic-plan.json'
with plan.open('xb') as output:
    output.write(pathlib.Path('/root/sharded-wal-atomic-plan.json').read_bytes())
subprocess.run(['/usr/bin/python3', '/root/sharded-wal-atomic-start.py', 'start', '--plan', str(plan)], check=True)
'''
    script = f'''#!/bin/bash
set -eu
/usr/sbin/shutdown -h +{p['ttl_seconds'] // 60}
umask 077
{bootstrap_command}
/usr/bin/python3 /root/sharded-wal-bootstrap.py --account {p['account']} --run-id {p['run_id']} --ttl-seconds {p['ttl_seconds']} --instance-type {p['instance_type']}
export DEBIAN_FRONTEND=noninteractive
timeout 600 apt-get update -o Acquire::Retries=2
timeout 900 apt-get install -y python3-boto3 python3-yaml build-essential openssl curl ca-certificates
{efa_setup}
if systemctl cat snap.amazon-ssm-agent.amazon-ssm-agent.service >/dev/null 2>&1; then
  systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service
elif systemctl cat amazon-ssm-agent.service >/dev/null 2>&1; then
  systemctl enable --now amazon-ssm-agent.service
else
  curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 120 -fsS https://s3.us-east-1.amazonaws.com/amazon-ssm-us-east-1/latest/debian_amd64/amazon-ssm-agent.deb -o /root/amazon-ssm-agent.deb
  timeout 120 dpkg -i /root/amazon-ssm-agent.deb
  systemctl enable --now amazon-ssm-agent.service
fi
/usr/bin/python3 - <<'SHARDED_WAL_SETUP'
{setup}SHARDED_WAL_SETUP
'''
    if p.get('atomic_controller') is True and len(script.encode()) > 16384:
        raise ValueError('atomic user data exceeds EC2 16 KiB limit')
    return script


def exact_public_ipv4(cidr):
    if not isinstance(cidr, str) or not cidr.endswith('/32'):
        raise ValueError('SSH requires the exact current public laptop IPv4 /32')
    network = ipaddress.ip_network(cidr, strict=True)
    if not isinstance(network, ipaddress.IPv4Network) or network.prefixlen != 32 or not network.network_address.is_global:
        raise ValueError('SSH requires one public IPv4 address, never a subnet or IPv6 range')
    return str(network)


def console_ed25519_key(output):
    """MCP GetConsoleOutput already decodes Output; never decode it a second time."""
    if not isinstance(output, str):
        raise ValueError('console output is not decoded text')
    blocks = re.findall(r'-----BEGIN SSH HOST KEY KEYS-----(.*?)-----END SSH HOST KEY KEYS-----', output, re.S)
    keys = {match for block in blocks for match in re.findall(r'^ssh-ed25519 ([A-Za-z0-9+/]+={0,2})(?: .*)?$', block, re.M)}
    if len(keys) != 1:
        raise ValueError('authenticated console must contain one unambiguous ED25519 host key')
    key = keys.pop()
    decoded = base64.b64decode(key, validate=True)
    if len(decoded) != 51 or decoded[:19] != b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20':
        raise ValueError('console ED25519 host key has invalid wire encoding')
    return 'ssh-ed25519 ' + key


def fleet_configuration(p, instances, placement_groups=None):
    if len(instances) != 6 or any(i['State']['Name'] != 'running' for i in instances):
        raise ValueError('all six exact owned instances must be running')
    if p.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing'):
        selected_type = p['instance_type'] if p['campaign'] in ('transport-floor', 'stage-timing') else 'g6.16xlarge'
        if selected_type not in ('c6id.32xlarge', 'g6.16xlarge') or any(
                i.get('InstanceType') != selected_type
                or i.get('Placement', {}).get('AvailabilityZone') != p['az']
                for i in instances):
            raise ValueError('transport fleet differs from required hardware/placement')
    nodes = sorted([i for i in instances if i.get('ClientToken') == p['run_id'] + '-node'], key=lambda i: i['InstanceId'])
    controllers = [i for i in instances if i.get('ClientToken') == p['run_id'] + '-controller']
    if len(nodes) != 5 or len(controllers) != 1:
        raise ValueError('fleet roles differ from immutable plan')
    deadline = min(datetime.fromisoformat(str(i['LaunchTime']).replace('Z', '+00:00')) for i in instances) + timedelta(seconds=p['ttl_seconds'] - 30)
    if (deadline - datetime.now(timezone.utc)).total_seconds() <= 1200:
        raise ValueError('insufficient original lifetime for readiness/build/workload/export')
    cfg = {'run_id': p['run_id'], 'account': p['account'], 'region': p['region'], 'bucket': p['bucket'],
           'availability_zone': p['az'],
           's3_prefix': p['prefix'], 'run_root': '/mnt/tidehunter/' + p['run_id'],
           'coordinator_instance_id': controllers[0]['InstanceId'],
           'nodes': [{'partition': n, 'instance_id': i['InstanceId'], 'private_ip': i['PrivateIpAddress']} for n, i in enumerate(nodes)],
           'deadline_utc': deadline.isoformat(), 'export_reserve_seconds': 600,
           'repetitions': 3 if p.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') else 5 if p.get('campaign') in ('checkpoint-async', 'cluster-ram') else 2,
           'checkpoint_scope': p.get('checkpoint_scope', 'full'),
           'storage_repetitions': 3, 'capacity_repetitions': 0, 'chunk_control_repetitions': 3, 'terminate_on_finish': True,
           'campaign': p.get('campaign', 'overlap'), 'instance_type': p['instance_type'], 'efa': p.get('efa', False)}
    if p['campaign'] == 'stage-timing':
        cfg.update(export_reserve_seconds=300, ttl_seconds=1500, placement_group=p['placement_group'],
                   placement_strategy=p['placement_strategy'], source_identities=p['source_identities'])
        stage_timing.validate_placement(cfg, instances, placement_groups or [])
    return cfg, controllers[0]


def ssh_command(directory, public_ip):
    return ['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'HostKeyAlgorithms=ssh-ed25519', '-o', 'UserKnownHostsFile=' + str((directory / 'known_hosts').resolve()),
            '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
            '-i', str((directory / 'ephemeral').resolve()), 'ubuntu@' + public_ip, 'sudo /bin/bash -s']


def ssh_start_script(p, cfg):
    payload = {'cfg': cfg, 'probe': readiness_command(p), 'start': start_command(cfg, 0), 'bundle_sha256': p['bundle_sha256']}
    remote = r"""import json,base64,subprocess,time,datetime,pathlib,boto3
from botocore.config import Config
sdk_config=Config(connect_timeout=5,read_timeout=15,retries={'total_max_attempts':2,'mode':'standard'})
payload=json.loads(base64.b64decode(PAYLOAD))
cfg=payload['cfg']
root=pathlib.Path(cfg['run_root'])
assert not (root/'controller-start-once').exists(), 'controller already requested'
ssm=boto3.client('ssm',region_name=cfg['region'],config=sdk_config)
ids=[i['instance_id'] for i in cfg['nodes']]
command=ssm.send_command(InstanceIds=ids,DocumentName='AWS-RunShellScript',Parameters={'commands':[payload['probe']],'executionTimeout':['30']},TimeoutSeconds=60,Comment=cfg['run_id']+'-guest-readiness',OutputS3BucketName=cfg['bucket'],OutputS3KeyPrefix=cfg['s3_prefix']+'/ssm/guest-readiness')['Command']['CommandId']
replies=[json.loads(subprocess.check_output(['bash','-c',payload['probe']],text=True,timeout=30))]
limit=time.monotonic()+100
pending=set(ids)
while pending:
    assert time.monotonic()<limit, 'readiness deadline'
    for instance in list(pending):
        assert time.monotonic()<limit, 'readiness deadline'
        try:
            result=ssm.get_command_invocation(CommandId=command,InstanceId=instance)
        except ssm.exceptions.InvocationDoesNotExist:
            continue
        if result['Status'] in ['Pending','InProgress','Delayed']:
            continue
        assert result['Status']=='Success' and result['ResponseCode']==0, 'node readiness failed'
        reply=json.loads(result['StandardOutputContent'])
        assert reply['instance_id']==instance
        replies.append(reply)
        pending.remove(instance)
    if pending: time.sleep(1)
assert len(replies)==6 and {r['instance_id'] for r in replies}==set(ids+[cfg['coordinator_instance_id']])
for r in replies:
    assert r['run_id']==cfg['run_id'] and r['account']==cfg['account'] and r['bundle_sha256']==payload['bundle_sha256']
    assert r['mount_target']=='/mnt/tidehunter' and r['model']=='Amazon EC2 NVMe Instance Storage'
    assert datetime.datetime.fromisoformat(r['deadline_utc'].replace('Z','+00:00'))>=datetime.datetime.fromisoformat(cfg['deadline_utc'])
assert datetime.datetime.fromisoformat(cfg['deadline_utc']).timestamp()-time.time()>1200
proof={'ready':True,'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'command_id':command,'instance_ids':sorted(ids+[cfg['coordinator_instance_id']]),'guests':replies}
(root/'ssh-readiness.json').write_text(json.dumps(proof))
s3=boto3.client('s3',region_name=cfg['region'],config=sdk_config)
s3.put_object(Bucket=cfg['bucket'],Key=cfg['s3_prefix']+'/results/readiness.json',Body=json.dumps(proof).encode(),ServerSideEncryption='AES256',ExpectedBucketOwner=cfg['account'])
left=int(datetime.datetime.fromisoformat(cfg['deadline_utc']).timestamp()-time.time())
assert left>1200 and payload['start'].count('RuntimeMaxSec=0 ')==1
start=payload['start'].replace('RuntimeMaxSec=0 ', 'RuntimeMaxSec='+str(left)+' ')
subprocess.run(['bash','-c',start],check=True,timeout=30)
print(json.dumps({'ready':proof,'started':True,'deadline_utc':cfg['deadline_utc']}))
""".replace('PAYLOAD', repr(base64.b64encode(encoded(payload)).decode()))
    return "/usr/bin/python3 - <<'SHARDED_WAL_FALLBACK'\n" + remote + "SHARDED_WAL_FALLBACK\n"


def readiness_command(p):
    probe = f"""import json, pathlib, subprocess, re
root = pathlib.Path('/mnt/tidehunter/{p['run_id']}')
record = json.loads((root / 'bootstrap.json').read_text())
assert record['run_id'] == {p['run_id']!r}, 'run identity'
assert record['identity']['accountId'] == {p['account']!r}, 'account'
assert record['identity']['instanceType'] == {p['instance_type']!r}, 'instance type'
assert (root / 'bundle-ready').read_text() == {p['bundle_sha256']!r}, 'bundle checksum receipt'
assert (root / 'bundle/runner/guest.py').is_file(), 'guest entry point'
if {p.get('efa', False)!r}:
    assert list(pathlib.Path('/sys/class/infiniband').iterdir()), 'EFA device missing'
    assert re.search(r'^\\s*prov_name: efa\\s*$', (root / 'efa-provider.txt').read_text(), re.M), 'EFA provider probe failed'
    assert (root / 'efa-version.txt').is_file(), 'EFA runtime version missing'
mount = json.loads(subprocess.check_output(['findmnt', '--json', '--target', '/mnt/tidehunter', '--output', 'SOURCE,TARGET,FSTYPE']))['filesystems']
assert len(mount) == 1 and mount[0]['target'] == '/mnt/tidehunter' and mount[0]['source'] == record['device'], 'dedicated mount'
model = subprocess.check_output(['lsblk', '--nodeps', '--noheadings', '--output', 'MODEL', record['device']], text=True).strip()
assert model == 'Amazon EC2 NVMe Instance Storage', 'local NVMe model'
subprocess.run(['systemctl', 'is-active', '--quiet', 'sharded-wal-expire-{p['run_id']}.timer'], check=True)
print(json.dumps(dict(instance_id=record['identity']['instanceId'], run_id=record['run_id'], account=record['identity']['accountId'], deadline_utc=record['deadline_utc'], bundle_sha256={p['bundle_sha256']!r}, mount_target=mount[0]['target'], device=record['device'], model=model)))
"""
    return "/usr/bin/python3 - <<'SHARDED_WAL_READINESS'\n" + probe + "SHARDED_WAL_READINESS\n"


def start_command(cfg, remaining):
    root = cfg['run_root']
    payload = base64.b64encode(encoded(cfg)).decode()
    # Unit exists across SSM retries; the mkdir guard makes an attempted rerun fail closed.
    script = f'''#!/bin/bash
set -eu
umask 077
mkdir {root}/controller-start-once
export CARGO_HOME=/root/.cargo
export RUSTUP_HOME=/root/.rustup
export PATH=/root/.cargo/bin:$PATH
export PKG_CONFIG_PATH=/opt/amazon/efa/lib/pkgconfig
export FI_EFA_USE_DEVICE_RDMA=1
export FI_EFA_ENABLE_SHM_TRANSFER=0
if ! /root/.cargo/bin/rustup toolchain list 2>/dev/null | grep -q '^1.90.0'; then
  curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 120 -sSf https://sh.rustup.rs -o {root}/rustup-init.sh
  timeout 600 sh {root}/rustup-init.sh -y --profile minimal --default-toolchain 1.90.0
fi
exec /usr/bin/python3 {root}/bundle/runner/guest.py controller --config {root}/controller.json
'''
    script64 = base64.b64encode(script.encode()).decode()
    unit = 'sharded-wal-controller-' + cfg['run_id']
    return f'''set -eu
umask 077
test -f {root}/bundle-ready
test -f {root}/bootstrap.json
printf %s {shlex.quote(payload)} | base64 -d > {root}/controller.json
printf %s {shlex.quote(script64)} | base64 -d > {root}/start-controller.sh
chmod 700 {root}/start-controller.sh
systemd-run --unit={unit} --property=RuntimeMaxSec={remaining} --property=LimitMEMLOCK=infinity --property=TimeoutStopSec=20 --property=KillMode=control-group --property=StandardOutput=append:{root}/controller-service.log --property=StandardError=append:{root}/controller-service.log {root}/start-controller.sh
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['init', 'plan', 'provision', 'upload', 'launch', 'status', 'readiness', 'readiness-status', 'start', 'ssh-readiness-start', 'ssh-close', 'command-status', 'progress', 'retrieve', 'terminate', 'cleanup'])
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--placement-group', action='store_true')
    parser.add_argument('--campaign', choices=['overlap', 'read-path', 'efa', 'efa-cache', 'efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'], default='overlap')
    parser.add_argument('--checkpoint-scope', choices=['full', 'core', 'capacity', 'recovery'], default='full',
                        help='All scopes retain qualification; core supports RAM/transport and capacity supports RAM only')
    parser.add_argument('--az', default='us-east-1a', help='Zone for a new immutable run')
    parser.add_argument('--subnet', default='subnet-038ee7d3ae5be11d6', help='Verified subnet in the chosen zone and benchmark VPC')
    parser.add_argument('--ttl-seconds', type=int, choices=[1500, 2400, 3000, 3600], default=3600,
                        help='Create-only lifetime; 1500 seconds is stage-timing only; 2400 is recovery-only')
    parser.add_argument('--efa-instance-type', choices=['c6id.32xlarge', 'g6.16xlarge'],
                        default='c6id.32xlarge', help='Create-only EFA hardware; G6 and transport-floor use spread placement')
    parser.add_argument('--discard-results', action='store_true')
    parser.add_argument('--ssh-cidr', help='Exact current public laptop IPv4/32, only for explicit EIC fallback')
    args = parser.parse_args()
    try:
        if args.phase == 'init':
            if not args.bundle:
                parser.error('init requires --bundle')
            result = init(args.run_dir, args.bundle, args.placement_group, args.campaign,
                          args.az, args.subnet, args.ttl_seconds, args.efa_instance_type, args.checkpoint_scope)
        else:
            with (args.run_dir / '.control.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                control = Control(args.run_dir)
                if args.phase == 'plan':
                    result = control.plan
                elif args.phase == 'ssh-readiness-start':
                    result = control.ssh_readiness_start(args.ssh_cidr)
                elif args.phase == 'cleanup':
                    result = control.cleanup(args.discard_results)
                else:
                    result = getattr(control, args.phase.replace('-', '_'))()
        print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception as error:
        # Never print exception text: HTTP/MCP errors can contain signed URLs.
        print(json.dumps({'error_type': type(error).__name__, 'aws_error_code': error_code(error),
                          'phase': args.phase, 'message': 'Phase failed; persisted intent may have unknown outcome. Inspect scoped status before retry.'}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
