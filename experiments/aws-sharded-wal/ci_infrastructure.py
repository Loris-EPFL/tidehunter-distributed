#!/usr/bin/env python3
"""Generate reviewed IAM baselines and a local CloudFormation foundation; never deploy."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import zlib

HERE = Path(__file__).resolve().parent
REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'
SUBJECT = 'repo:Loris-EPFL@91122257/tidehunter-distributed@1378911199:environment:rdma-bench'


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def policy_actions(value):
    """Read action names from generated policy documents, not inferred SDK names."""
    found = set()
    if isinstance(value, dict):
        if value.get('Version') == '2012-10-17' and isinstance(value.get('Statement'), list):
            for statement in value['Statement']:
                if statement.get('Effect') != 'Allow' or 'NotAction' in statement:
                    raise ValueError('baseline must contain explicit allow actions')
                actions = statement.get('Action', [])
                for action in [actions] if isinstance(actions, str) else actions:
                    if not re.fullmatch(r'[A-Za-z0-9-]+:[A-Za-z0-9]+', action):
                        raise ValueError('refuse wildcard or malformed generated IAM action')
                    found.add(action)
        else:
            for child in value.values():
                found.update(policy_actions(child))
    elif isinstance(value, list):
        for child in value:
            found.update(policy_actions(child))
    return found


def compact_policy(statements):
    """Merge only identical resource/condition grants, preserving their union."""
    grouped = {}
    for statement in statements:
        key = json.dumps({k: v for k, v in statement.items() if k != 'Action'}, sort_keys=True)
        group = grouped.setdefault(key, {k: v for k, v in statement.items() if k != 'Action'} | {'Action': []})
        actions = statement['Action']
        group['Action'].extend([actions] if isinstance(actions, str) else actions)
    for group in grouped.values():
        group['Action'] = sorted(set(group['Action']))
    return {'Version': '2012-10-17', 'Statement': list(grouped.values())}


def baseline_commands(account, region):
    common = ['uvx', 'iam-policy-autopilot==0.3.0', 'generate-policies']
    groups = {
        'launcher': ['ci_campaign.py', 'ci_budget.py', 'ci_watchdog.py'],
        'controller': ['atomic_start.py', 'sui_checkpoint.py', 'guest.py', 'checkpoint_kv.py'],
        # The storage guest only retrieves its immutable bootstrap and emits proof;
        # its policy is narrowed further below despite the shared dispatcher source.
        'storage': ['atomic_start.py'],
        'watchdog': ['ci_watchdog.py'],
    }
    return {name: common + [str((HERE / path).resolve()) for path in paths]
            + ['--account', account, '--region', region, '--service-hints', 'ec2', 's3', 'ssm', 'sts', 'iam', 'lambda', 'events', 'pricing', 'ce', '--pretty']
            for name, paths in groups.items()}


def scoped_policy(actions, role, *, account, region, bucket, stack, regions, instance_types):
    """Refine an Autopilot baseline with the explicit deployment ownership boundary.

    Action names come only from generated output. Resource/condition refinement is
    separate from baseline generation and must be reviewed before deployment.
    """
    owner = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark',
             'Repository': REPOSITORY, 'BenchmarkBranch': BRANCH}
    statements = []
    region_condition = {'StringEquals': {'aws:RequestedRegion': regions}}
    owned = {'StringEquals': {'ec2:ResourceTag/' + k: v for k, v in owner.items()}}
    request = {'StringEquals': {'aws:RequestTag/' + k: v for k, v in owner.items()},
               'Null': {'aws:RequestTag/RunId': 'false', 'aws:RequestTag/ExpiresAt': 'false'}}
    node_prefix = f'arn:aws:s3:::{bucket}/${{aws:PrincipalTag/RunId}}/*'
    if role in ('controller', 'storage'):
        owned['StringEquals']['ec2:ResourceTag/RunId'] = '${aws:PrincipalTag/RunId}'
    for action in sorted(actions):
        service, method = action.split(':', 1)
        if role == 'input' and action not in ('s3:GetObject', 'sts:GetCallerIdentity'):
            continue
        # The deployed bucket uses SSE-S3 and no Object Lambda, ACL or Object
        # Lock APIs. Autopilot includes these conditional API variants; retain
        # the raw baseline for review, but do not grant inapplicable variants.
        if service in ('kms', 's3-object-lambda'):
            continue
        if service == 's3' and method not in (
                'GetObject', 'PutObject', 'AbortMultipartUpload', 'ListMultipartUploadParts',
                'ListBucket', 'ListBucketMultipartUploads', 'GetBucketLocation',
                'GetBucketTagging', 'GetBucketVersioning'):
            continue
        resource, condition = '*', None
        if role == 'storage' and service not in ('s3', 'sts'):
            continue  # SSM agent uses its separate managed instance-core policy.
        if service == 's3':
            if method in ('ListBucket', 'ListBucketMultipartUploads', 'GetBucketLocation', 'GetBucketTagging', 'GetBucketVersioning'):
                resource = f'arn:aws:s3:::{bucket}'
                if role in ('controller', 'storage') and method.startswith('List'):
                    condition = {'StringLike': {'s3:prefix': ['${aws:PrincipalTag/RunId}', '${aws:PrincipalTag/RunId}/*']}}
            else:
                resource = node_prefix if role in ('controller', 'storage') else f'arn:aws:s3:::{bucket}/*'
                if role == 'input':
                    resource = f'arn:aws:s3:::{bucket}/fixtures/sui-genesis-1775/fixture.tar.gz'
                if role == 'watchdog':
                    resource = f'arn:aws:s3:::{bucket}/ci/watchdog/*'
                if role == 'storage' and method not in ('GetObject', 'PutObject', 'AbortMultipartUpload', 'ListMultipartUploadParts'):
                    continue
        elif service == 'ec2':
            condition = region_condition
            if not method.startswith(('Describe', 'Get')):
                resource = f'arn:aws:ec2:*:{account}:*'
                condition = {'StringEquals': dict(owned['StringEquals'], **region_condition['StringEquals'])}
                if method == 'RunInstances':
                    if role != 'launcher':
                        raise ValueError('only the launcher may allocate compute')
                    for resources, extra in [
                        ([f'arn:aws:ec2:*::image/*', f'arn:aws:ec2:*:{account}:subnet/*', f'arn:aws:ec2:*:{account}:security-group/*', f'arn:aws:ec2:*:{account}:placement-group/*'], region_condition),
                        ([f'arn:aws:ec2:*:{account}:instance/*', f'arn:aws:ec2:*:{account}:volume/*', f'arn:aws:ec2:*:{account}:network-interface/*'],
                         {'StringEquals': dict(request['StringEquals'], **region_condition['StringEquals']), 'Null': request['Null'],
                          'StringEqualsIfExists': {'ec2:InstanceType': instance_types}}),
                    ]:
                        statements.append({'Effect': 'Allow', 'Action': action, 'Resource': resources, 'Condition': extra})
                    continue
                if method in ('CreateSecurityGroup', 'CreatePlacementGroup'):
                    condition = {'StringEquals': dict(request['StringEquals'], **region_condition['StringEquals']), 'Null': request['Null']}
                    if method == 'CreateSecurityGroup':
                        # Run-time creation needs its existing VPC as well as the
                        # new, request-tagged SG. No right to modify the VPC.
                        statements.append({'Effect': 'Allow', 'Action': action,
                                           'Resource': f'arn:aws:ec2:*:{account}:vpc/*', 'Condition': region_condition})
                elif method == 'CreateTags':
                    statements.append({'Effect': 'Allow', 'Action': action, 'Resource': resource,
                        'Condition': {'StringEquals': {'ec2:CreateAction': ['RunInstances', 'CreateSecurityGroup', 'CreatePlacementGroup'], **request['StringEquals'], **region_condition['StringEquals']}}})
        elif service == 'iam':
            if role in ('controller', 'storage'):
                continue
            profile = f'arn:aws:iam::{account}:instance-profile/tidehunter-ci-th-*'
            guest_role = f'arn:aws:iam::{account}:role/tidehunter-ci-th-*'
            boundary = f'arn:aws:iam::{account}:policy/{stack}-guest-boundary'
            if method == 'PassRole':
                if role != 'launcher':
                    raise ValueError('only the launcher may pass a guest role')
                resource = guest_role
                condition = {'StringEquals': {'iam:PassedToService': 'ec2.amazonaws.com'}}
            elif method in ('ListRoles', 'ListInstanceProfiles'):
                resource = '*'
            elif method in ('GetPolicy', 'GetPolicyVersion'):
                resource = boundary
            elif method in ('CreateRole', 'CreateInstanceProfile', 'TagRole', 'TagInstanceProfile'):
                if role != 'launcher':
                    raise ValueError('janitor must not create or retag IAM identities')
                resource = profile if 'InstanceProfile' in method else guest_role
                condition = {'StringEquals': dict(request['StringEquals']), 'Null': request['Null']}
                if method == 'CreateRole':
                    condition['StringEquals']['iam:PermissionsBoundary'] = boundary
            elif method in ('GetRole', 'GetRolePolicy', 'GetInstanceProfile', 'ListRolePolicies',
                            'ListAttachedRolePolicies', 'ListInstanceProfilesForRole'):
                resource = profile if method == 'GetInstanceProfile' else guest_role
            elif method in ('PutRolePolicy', 'AttachRolePolicy', 'DetachRolePolicy',
                            'DeleteRolePolicy', 'DeleteRole', 'AddRoleToInstanceProfile',
                            'RemoveRoleFromInstanceProfile', 'DeleteInstanceProfile'):
                if role == 'watchdog' and method in ('PutRolePolicy', 'AttachRolePolicy', 'AddRoleToInstanceProfile'):
                    raise ValueError('janitor must not grant IAM permissions')
                resource = profile if 'InstanceProfile' in method else guest_role
                condition = {'StringEquals': {'aws:ResourceTag/' + k: v for k, v in owner.items()}}
                if method in ('AttachRolePolicy', 'DetachRolePolicy'):
                    condition['ArnEquals'] = {'iam:PolicyARN': 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'}
            else:
                raise ValueError('unreviewed IAM operation: ' + action)
        elif service == 'ssm':
            if method == 'SendCommand':
                statements.extend([
                    {'Effect': 'Allow', 'Action': action, 'Resource': f'arn:aws:ssm:*::document/AWS-RunShellScript', 'Condition': region_condition},
                    {'Effect': 'Allow', 'Action': action, 'Resource': f'arn:aws:ec2:*:{account}:instance/*',
                     'Condition': {'StringEquals': {'ssm:resourceTag/' + k: v for k, v in owner.items()}
                                   | ({'ssm:resourceTag/RunId': '${aws:PrincipalTag/RunId}'} if role in ('controller', 'storage') else {})}},
                ])
                continue
            if method == 'GetParameter':
                resource = 'arn:aws:ssm:*::parameter/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id'
            elif not method.startswith(('Describe', 'Get', 'List')):
                raise ValueError('unreviewed SSM mutation: ' + action)
        elif action == 'aws-portal:ViewBilling':
            # Preserve the analyzer's explicit read-only billing dependency.
            resource = '*'
        elif service == 'lambda':
            if method not in ('GetFunctionConfiguration', 'GetPolicy', 'InvokeFunction'):
                raise ValueError('unreviewed Lambda mutation: ' + action)
            resource = f'arn:aws:lambda:{region}:{account}:function:{stack}-watchdog'
        elif service == 'events':
            if method not in ('DescribeRule', 'ListTargetsByRule'):
                raise ValueError('unreviewed EventBridge mutation: ' + action)
            resource = f'arn:aws:events:{region}:{account}:rule/{stack}-expiry'
        elif service in ('sts', 'pricing', 'ce'):
            if not method.startswith(('Get', 'List', 'Describe')):
                raise ValueError('unreviewed accounting mutation: ' + action)
        else:
            raise ValueError('unreviewed generated service permission: ' + action)
        row = {'Effect': 'Allow', 'Action': action, 'Resource': resource}
        if condition:
            row['Condition'] = condition
        statements.append(row)
    if not statements:
        raise ValueError('empty generated permissions for ' + role)
    return compact_policy(statements)


def template(args, policies):
    config = json.loads(args.config.read_text())
    account, stack, region = args.account, args.stack, args.region
    tags = [{'Key': k, 'Value': v} for k, v in {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark',
            'Repository': REPOSITORY, 'BenchmarkBranch': BRANCH, 'RunId': 'shared-infrastructure'}.items()]
    resources = {}
    def add(name, kind, properties, why, must=()):
        resources[name] = {'Type': kind, 'Properties': properties, 'Metadata': {
            'com.aws.cloudformation.Context': {'why': why, 'must': list(must)}}}
    add('Results', 'AWS::S3::Bucket', {'BucketName': args.bucket,
        'PublicAccessBlockConfiguration': {k: True for k in ('BlockPublicAcls', 'IgnorePublicAcls', 'BlockPublicPolicy', 'RestrictPublicBuckets')},
        'OwnershipControls': {'Rules': [{'ObjectOwnership': 'BucketOwnerEnforced'}]},
        'BucketEncryption': {'ServerSideEncryptionConfiguration': [{'ServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]},
        'VersioningConfiguration': {'Status': 'Enabled'},
        'LifecycleConfiguration': {'Rules': [
            {'Id': 'expire-run-evidence', 'Status': 'Enabled', 'Prefix': 'th-', 'ExpirationInDays': 30, 'NoncurrentVersionExpiration': {'NoncurrentDays': 7}, 'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1}},
            {'Id': 'expire-old-ledger-versions', 'Status': 'Enabled', 'Prefix': 'ci-control/budget/', 'NoncurrentVersionExpiration': {'NoncurrentDays': 30}},
            {'Id': 'expire-old-report-versions', 'Status': 'Enabled', 'Prefix': 'ci-control/reports/', 'NoncurrentVersionExpiration': {'NoncurrentDays': 30}},
            {'Id': 'expire-watchdog-receipts', 'Status': 'Enabled', 'Prefix': 'ci/watchdog/', 'ExpirationInDays': 30, 'NoncurrentVersionExpiration': {'NoncurrentDays': 7}},
        ]}, 'Tags': tags}, 'Private retained fixture, exact run evidence and never-expiring active cost ledger.', ['Retain fixtures and current ledger; only temporary run evidence expires.'])
    resources['Results'].update(DeletionPolicy='Retain', UpdateReplacePolicy='Retain')
    add('ResultsTls', 'AWS::S3::BucketPolicy', {'Bucket': {'Ref': 'Results'}, 'PolicyDocument': {
        'Version': '2012-10-17', 'Statement': [{'Effect': 'Deny', 'Principal': '*', 'Action': 's3:*',
            'Resource': [f'arn:aws:s3:::{args.bucket}', f'arn:aws:s3:::{args.bucket}/*'],
            'Condition': {'Bool': {'aws:SecureTransport': 'false'}}},
            {'Sid': 'OnlyCiPublishesAggregate', 'Effect': 'Deny', 'Principal': '*', 'Action': 's3:PutObject',
             'Resource': f'arn:aws:s3:::{args.bucket}/ci-control/reports/*',
             'Condition': {'ArnNotEquals': {'aws:PrincipalArn': {'Fn::GetAtt': ['LauncherRole', 'Arn']}}}}]}},
        'Reject non-TLS storage access and human/guest edits to generated accounting.',
        ['Only the exact CI accounting identity publishes aggregate content; administrators can change the policy.'])
    add('GitHubOidc', 'AWS::IAM::OIDCProvider', {'Url': 'https://token.actions.githubusercontent.com', 'ClientIdList': ['sts.amazonaws.com'], 'Tags': tags}, 'Short-lived GitHub credentials; no stored access keys.', ['Use exact repository/environment subject and deployment branch restrictions.'])
    ssm_core = json.loads((args.output / 'ssm-core.baseline.json').read_text())
    provenance = json.loads((args.output / 'ssm-core.provenance.json').read_text())
    if provenance['version'] != 'v2' or provenance['sha256'] != hashlib.sha256(json.dumps(ssm_core, sort_keys=True).encode()).hexdigest():
        raise ValueError('SSM managed policy changed; review its baseline before rendering')
    core_actions = policy_actions(ssm_core)
    if any(action.split(':', 1)[0] not in ('ssm', 'ssmmessages', 'ec2messages') for action in core_actions):
        raise ValueError('unreviewed service in SSM managed policy')
    boundary = dict(policies['controller'])
    boundary['Statement'] = list(boundary['Statement'])
    for original in ssm_core['Statement']:
        actions = original['Action'] if isinstance(original['Action'], list) else [original['Action']]
        for action in actions:
            row = dict(original, Action=[action])
            if action in ('ssm:GetParameter', 'ssm:GetParameters'):
                row['Resource'] = 'arn:aws:ssm:*::parameter/aws/service/*'
            elif action in ('ssm:GetDocument', 'ssm:DescribeDocument'):
                row['Resource'] = 'arn:aws:ssm:*::document/*'
            boundary['Statement'].append(row)
    boundary = compact_policy(boundary['Statement'])
    add('GuestBoundary', 'AWS::IAM::ManagedPolicy', {
        'ManagedPolicyName': stack + '-guest-boundary', 'PolicyDocument': boundary},
        'Fixed upper bound for temporary guest roles; run identity comes from trusted role tags.',
        ['Guest roles cannot alter this boundary; run inputs and partition writes are narrowed by their inline policy.'])
    for role in ('launcher', 'input', 'watchdog'):
        logical = role.title() + 'Role'
        if role in ('launcher', 'input'):
            trust = {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Principal': {'Federated': {'Ref': 'GitHubOidc'}},
                'Action': 'sts:AssumeRoleWithWebIdentity', 'Condition': {'StringEquals': {
                    'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
                    'token.actions.githubusercontent.com:sub': args.oidc_subject + ('-inputs' if role == 'input' else '')}}}]}
        else:
            trust = {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow',
                'Principal': {'Service': 'lambda.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]}
        props = {'RoleName': stack + '-' + role, 'AssumeRolePolicyDocument': trust,
                 'MaxSessionDuration': 7200, 'Policies': [{'PolicyName': 'generated-and-owned-scope', 'PolicyDocument': policies[role]}], 'Tags': tags}
        if role == 'watchdog':
            props['ManagedPolicyArns'] = ['arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole']
        add(logical, 'AWS::IAM::Role', props, 'Generated and scoped actions; guest creation requires the fixed boundary.',
            ['Only temporary tidehunter-ci-th-* guest identities may be managed.'])
    watchdog_source = (HERE / 'ci_watchdog.py').read_bytes()
    source_hash = hashlib.sha256(watchdog_source).hexdigest()
    payload = base64.b64encode(zlib.compress(watchdog_source, 9)).decode('ascii')
    # Keep the inline template below CloudFormation's 51,200-byte body limit.
    # This is the exact reviewed source, decoded once during Lambda cold start.
    watchdog = "import base64,zlib\nexec(compile(zlib.decompress(base64.b64decode(" + repr(payload) + ")), 'ci_watchdog.py', 'exec'))\n"
    add('Watchdog', 'AWS::Lambda::Function', {'FunctionName': stack + '-watchdog', 'Runtime': 'python3.12', 'Handler': 'index.lambda_handler',
        'Role': {'Fn::GetAtt': ['WatchdogRole', 'Arn']}, 'Timeout': 180, 'MemorySize': 256, 'ReservedConcurrentExecutions': 1,
        'Code': {'ZipFile': watchdog}, 'Environment': {'Variables': {'AWS_BENCH_WATCHDOG_VERSION': __import__('ci_watchdog').VERSION, 'AWS_BENCH_ACCOUNT': account,
            'AWS_BENCH_REGIONS': ','.join(config['regions']), 'AWS_BENCH_REPOSITORY': REPOSITORY, 'AWS_BENCH_BRANCH': BRANCH,
            'AWS_BENCH_RESULTS_BUCKET': args.bucket, 'AWS_BENCH_RESULTS_REGION': region,
            'AWS_BENCH_GUEST_BOUNDARY_ARN': {'Ref': 'GuestBoundary'}}}, 'Tags': tags},
        'Independent recurring expiry cleanup survives GitHub and guest failure.', ['Never delete foreign or unexpired resources; retain results.'])
    resources['Watchdog']['Metadata']['SourceSha256'] = source_hash
    add('WatchdogLogs', 'AWS::Logs::LogGroup', {'LogGroupName': '/aws/lambda/' + stack + '-watchdog', 'RetentionInDays': 14, 'Tags': tags}, 'Bounded structured cleanup logs.')
    resources['Watchdog']['DependsOn'] = ['WatchdogLogs']
    add('ExpiryRule', 'AWS::Events::Rule', {'Name': stack + '-expiry', 'ScheduleExpression': 'rate(1 minute)', 'State': 'ENABLED',
        'Targets': [{'Id': 'expiry', 'Arn': {'Fn::GetAtt': ['Watchdog', 'Arn']}, 'Input': '{}'}], 'Tags': tags},
        'Retry discovery every minute, independent of campaign journal availability.')
    add('AllowExpiry', 'AWS::Lambda::Permission', {'FunctionName': {'Ref': 'Watchdog'}, 'Action': 'lambda:InvokeFunction',
        'Principal': 'events.amazonaws.com', 'SourceArn': {'Fn::GetAtt': ['ExpiryRule', 'Arn']}, 'SourceAccount': account}, 'Only the owned rule may invoke scheduled cleanup.')
    add('WatchdogErrors', 'AWS::CloudWatch::Alarm', {'AlarmName': stack + '-cleanup-errors', 'Namespace': 'AWS/Lambda', 'MetricName': 'Errors',
        'Dimensions': [{'Name': 'FunctionName', 'Value': {'Ref': 'Watchdog'}}], 'Statistic': 'Sum', 'Period': 60, 'EvaluationPeriods': 1,
        'Threshold': 0, 'ComparisonOperator': 'GreaterThanThreshold', 'TreatMissingData': 'notBreaching', 'Tags': tags}, 'Surface incomplete cleanup for investigation; periodic retries remain armed.')
    return {'AWSTemplateFormatVersion': '2010-09-09',
        'Description': 'Ephemeral Tidehunter benchmark access, retained evidence and independent expiry; no persistent EC2 fleet.',
        'Metadata': {'AWSToolsMetrics': {'AWSAgentToolkit': 'aws-cloudformation@3'},
                     'com.aws.cloudformation.Context': {'ref': [{'at': 'docs/distributed/CLOUD_AND_CI_TESTING.md', 'has': 'activation, ownership, cost and failure contract'}]}},
        'Resources': resources,
        'Outputs': {name: {'Value': value, 'Description': description} for name, value, description in [
            ('RoleArn', {'Fn::GetAtt': ['LauncherRole', 'Arn']}, 'AWS_BENCH_ROLE_ARN'),
            ('InputRoleArn', {'Fn::GetAtt': ['InputRole', 'Arn']}, 'AWS_BENCH_INPUT_ROLE_ARN'),
            ('Bucket', {'Ref': 'Results'}, 'AWS_BENCH_BUCKET'), ('BucketRegion', {'Ref': 'AWS::Region'}, 'AWS_BENCH_BUCKET_REGION'),
            ('GuestBoundaryArn', {'Ref': 'GuestBoundary'}, 'AWS_BENCH_GUEST_BOUNDARY_ARN'),
            ('WatchdogFunction', {'Ref': 'Watchdog'}, 'AWS_BENCH_WATCHDOG_FUNCTION'), ('WatchdogRule', {'Ref': 'ExpiryRule'}, 'AWS_BENCH_WATCHDOG_RULE'),
        ]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('commands', 'generate', 'render'))
    parser.add_argument('--account', required=True)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--stack', default='tidehunter-aws-benchmarks')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--oidc-subject', default=SUBJECT)
    parser.add_argument('--config', type=Path, default=HERE / 'ci-config.json')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', help='Optional admin profile for read-only managed-policy discovery; never used by CI')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]{12}', args.account) or not re.fullmatch(r'[a-z][a-z0-9-]{2,40}', args.stack):
        raise ValueError('invalid expected account or stack name')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', args.bucket):
        raise ValueError('invalid dedicated bucket name')
    if args.oidc_subject not in (SUBJECT, 'repo:' + REPOSITORY + ':environment:rdma-bench'):
        raise ValueError('exact trusted GitHub environment subject required')
    commands = baseline_commands(args.account, args.region)
    args.output.mkdir(parents=True, exist_ok=True)
    save(args.output / 'generation-commands.json', commands)
    if args.action == 'commands':
        import shlex
        for role, command in commands.items():
            print(role + ': ' + shlex.join(command))
        return
    if args.action == 'generate':
        sources = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
                   for command in commands.values() for name in command if name.endswith('.py')}
        for role, command in commands.items():
            with (args.output / (role + '.baseline.json')).open('x') as stdout, (args.output / (role + '.generation.log')).open('x') as stderr:
                subprocess.run(command, stdout=stdout, stderr=stderr, check=True, timeout=300)
        aws = ['aws'] + (['--profile', args.profile] if args.profile else []) + ['--region', args.region, '--cli-connect-timeout', '5', '--cli-read-timeout', '15']
        policy = 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
        version = subprocess.check_output(aws + ['iam', 'get-policy', '--policy-arn', policy,
            '--query', 'Policy.DefaultVersionId', '--output', 'text'], text=True, timeout=30).strip()
        document = json.loads(subprocess.check_output(aws + ['iam', 'get-policy-version', '--policy-arn', policy,
            '--version-id', version, '--query', 'PolicyVersion.Document', '--output', 'json'], text=True, timeout=30))
        save(args.output / 'ssm-core.baseline.json', document)
        save(args.output / 'ssm-core.provenance.json', {'arn': policy, 'version': version,
            'sha256': hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()})
        if any(hashlib.sha256(Path(name).read_bytes()).hexdigest() != digest for name, digest in sources.items()):
            raise ValueError('policy sources changed during analysis; regenerate into a fresh output directory')
        save(args.output / 'source-hashes.json', sources)
        return
    sources = json.loads((args.output / 'source-hashes.json').read_text())
    if any(hashlib.sha256(Path(name).read_bytes()).hexdigest() != digest for name, digest in sources.items()):
        raise ValueError('runtime policy sources changed; regenerate baselines before rendering')
    config = json.loads(args.config.read_text())
    policies = {}
    for role in commands:
        source = args.output / (role + '.baseline.json')
        actions = policy_actions(json.loads(source.read_text()))
        if role == 'controller':
            # Managed S3 transfers also abort/list multipart uploads on failure.
            # Their explicit SDK calls are visible in the launcher baseline.
            actions |= {action for action in policy_actions(json.loads((args.output / 'launcher.baseline.json').read_text()))
                        if action.startswith('s3:')}
        policies[role] = scoped_policy(actions, role, account=args.account, region=args.region, bucket=args.bucket,
                                      stack=args.stack, regions=config['regions'], instance_types=config['instance_types'])
        save(args.output / (role + '.scoped.json'), policies[role])
    policies['input'] = scoped_policy(policy_actions(json.loads((args.output / 'storage.baseline.json').read_text())),
        'input', account=args.account, region=args.region, bucket=args.bucket, stack=args.stack,
        regions=config['regions'], instance_types=config['instance_types'])
    save(args.output / 'input.scoped.json', policies['input'])
    rendered = template(args, policies)
    payload = json.dumps(rendered, separators=(',', ':')) + '\n'
    if len(payload.encode()) > 51200:
        raise ValueError('inline CloudFormation template exceeds 51,200 bytes')
    for resource in rendered['Resources'].values():
        props = resource['Properties']
        if resource['Type'] == 'AWS::IAM::ManagedPolicy' and len(json.dumps(props['PolicyDocument'], separators=(',', ':'))) > 6144:
            raise ValueError('guest permissions boundary exceeds IAM managed-policy quota')
        if resource['Type'] == 'AWS::IAM::Role' and sum(len(json.dumps(item['PolicyDocument'], separators=(',', ':')))
                for item in props.get('Policies', [])) > 10240:
            raise ValueError('role inline policies exceed IAM quota')
    path = args.output / 'foundation.json'
    path.write_text(payload)
    save(args.output / 'render-receipt.json', {'template_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'bytes': path.stat().st_size, 'deployed': False,
        'policy_generation': 'iam-policy-autopilot==0.3.0 baseline; explicit resource/ownership refinement requires review'})
    print(path)


if __name__ == '__main__':
    main()
