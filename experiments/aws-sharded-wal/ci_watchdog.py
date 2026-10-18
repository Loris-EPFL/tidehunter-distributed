"""Independent, fail-closed expiry janitor for the scheduled benchmark Lambda.

Deploy ci_watchdog.lambda_handler on an ENABLED EventBridge rate(1 minute)
rule, independently of runners. No journal, local disk, S3 read, or fleet-size
assumption is required: tags and AWS ownership are the cleanup authority.
"""
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import re
import time

VERSION = '2'
REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'
PROJECT = 'TideHunter'
MANAGED_BY = 'sharded-wal-benchmark'
HANDLER = 'ci_watchdog.lambda_handler'
HANDLERS = (HANDLER, 'index.lambda_handler')
SCHEDULE = 'rate(1 minute)'
LOG = logging.getLogger(__name__)
_RUN_ID = re.compile(r'th-[a-f0-9]{16}\Z')
_ACCOUNT = re.compile(r'[0-9]{12}\Z')
_REGION = re.compile(r'[a-z]{2}(?:-[a-z]+)+-[0-9]+\Z')
_EXPIRY = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)\Z')
_IAM_NAME = re.compile(r'tidehunter-ci-(th-[a-f0-9]{16})-(controller|storage-[0-4])\Z')
_SSM_POLICY = 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
_MISSING = object()


class IncompleteCleanup(RuntimeError):
    """Carries a nonsecret receipt; the next scheduled invocation must retry."""

    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__('benchmark cleanup incomplete; inspect structured cleanup receipt')


class _Limit(RuntimeError):
    pass


def _error_code(error):
    # Never log arbitrary exception text, response bodies, credentials or tags.
    response = getattr(error, 'response', {})
    code = response.get('Error', {}).get('Code', type(error).__name__)
    return re.sub(r'[^A-Za-z0-9_.-]', '_', str(code))[:100]


def _utc(now):
    now = datetime.now(timezone.utc) if now is None else now
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('now must be a timezone-aware datetime')
    return now.astimezone(timezone.utc)


def _config(account, regions, repository, branch, resource_prefix='', guest_boundary_arn=''):
    regions = list(regions)
    if not isinstance(account, str) or not _ACCOUNT.fullmatch(account):
        raise ValueError('AWS_BENCH_ACCOUNT must be a twelve-digit account ID')
    if not regions or len(regions) > 16 or len(set(regions)) != len(regions):
        raise ValueError('configure between one and sixteen unique approved regions')
    if any(not isinstance(region, str) or not _REGION.fullmatch(region) for region in regions):
        raise ValueError('invalid approved region')
    if repository != REPOSITORY or branch != BRANCH:
        raise ValueError('janitor is restricted to the approved repository and benchmark branch')
    if not isinstance(resource_prefix, str) or len(resource_prefix) > 128:
        raise ValueError('invalid resource naming prefix')
    if guest_boundary_arn and not re.fullmatch(
            r'arn:aws:iam::' + account + r':policy/[A-Za-z0-9+=,.@_/-]+', guest_boundary_arn):
        raise ValueError('guest boundary must be an exact same-account managed policy ARN')
    return dict(version=VERSION, account=account, regions=regions, repository=repository,
                branch=branch, project=PROJECT, managed_by=MANAGED_BY,
                resource_prefix=resource_prefix, guest_boundary_arn=guest_boundary_arn)


def _environment():
    if os.environ.get('AWS_BENCH_WATCHDOG_VERSION') != VERSION:
        raise ValueError('AWS_BENCH_WATCHDOG_VERSION does not match deployed janitor')
    if not os.environ.get('AWS_BENCH_GUEST_BOUNDARY_ARN'):
        raise ValueError('AWS_BENCH_GUEST_BOUNDARY_ARN is required')
    return _config(os.environ['AWS_BENCH_ACCOUNT'],
                   os.environ['AWS_BENCH_REGIONS'].split(','),
                   os.environ['AWS_BENCH_REPOSITORY'],
                   os.environ['AWS_BENCH_BRANCH'],
                   os.environ.get('AWS_BENCH_RESOURCE_PREFIX', ''),
                   os.environ['AWS_BENCH_GUEST_BOUNDARY_ARN'])


def _filters(config):
    return [{'Name': 'tag:' + key, 'Values': [value]} for key, value in (
        ('Project', PROJECT), ('ManagedBy', MANAGED_BY),
        ('Repository', config['repository']), ('BenchmarkBranch', config['branch']))]


class _OwnedReaper:
    def __init__(self, config, now, receipt, *, max_api_calls,
                 max_pages, proof_attempts, proof_delay, sleep, deadline, monotonic):
        self.config, self.now, self.receipt = config, now, receipt
        self.max_api_calls, self.max_pages = max_api_calls, max_pages
        self.proof_attempts, self.proof_delay, self.sleep = proof_attempts, proof_delay, sleep
        self.deadline, self.monotonic = deadline, monotonic
        self.calls = 0

    def issue(self, action, resource, code):
        self.receipt['errors'].append(dict(action=action, resource=resource, code=code))

    def take(self):
        if self.monotonic() >= self.deadline:
            raise _Limit('DeadlineRetryNextInvocation')
        if self.calls >= self.max_api_calls:
            raise _Limit('APICallLimitRetryNextInvocation')
        self.calls += 1

    def call(self, action, resource, function, *, absent=()):
        self.take()
        try:
            return function()
        except Exception as error:
            code = _error_code(error)
            if code in absent:
                return {'_absent': True}
            self.issue(action, resource, code)
            return _MISSING

    def pages(self, operation, **kwargs):
        """Bound real SDK calls; opaque/cyclic/missing continuation fails closed."""
        collections = {
            'describe_instances': 'Reservations',
            'describe_network_interfaces': 'NetworkInterfaces',
            'describe_volumes': 'Volumes',
            'describe_security_groups': 'SecurityGroups',
            'list_roles': 'Roles',
            'list_instance_profiles': 'InstanceProfiles',
            'list_instance_profiles_for_role': 'InstanceProfiles',
            'list_role_policies': 'PolicyNames',
            'list_attached_role_policies': 'AttachedPolicies',
        }
        iam = operation.startswith('list_')
        token, seen = None, set()
        try:
            for _ in range(self.max_pages):
                params = dict(kwargs, **({'MaxItems': 100} if iam else {'MaxResults': 100}))
                if token is not None:
                    params['Marker' if iam else 'NextToken'] = token
                self.take()
                page = self.page(operation, params)
                if not isinstance(page, dict) or not isinstance(page.get(collections[operation]), list):
                    self.issue(operation, '*', 'MalformedPaginationPage')
                    return
                if iam:
                    if not isinstance(page.get('IsTruncated'), bool):
                        self.issue(operation, '*', 'MissingOrMalformedTruncationFlag')
                        return
                    next_token = page.get('Marker') if page['IsTruncated'] else None
                    if page['IsTruncated'] and (not isinstance(next_token, str) or not next_token):
                        self.issue(operation, '*', 'MissingOrMalformedContinuationToken')
                        return
                else:
                    next_token = page.get('NextToken')
                    if next_token is not None and (not isinstance(next_token, str) or not next_token):
                        self.issue(operation, '*', 'MissingOrMalformedContinuationToken')
                        return
                if next_token is not None and next_token in seen:
                    self.issue(operation, '*', 'RepeatedContinuationToken')
                    return
                yield page
                if next_token is None:
                    return
                seen.add(next_token)
                token = next_token
            self.issue(operation, '*', 'PageLimitReached')
        except _Limit:
            raise
        except Exception as error:
            self.issue(operation, '*', _error_code(error))

    def eligible(self, item, resource, *, owner=None, require_owner=False):
        tags = {}
        duplicates = False
        raw_tags = item.get('Tags', [])
        if not isinstance(raw_tags, list):
            self.issue('ownership', resource, 'MalformedTags')
            return False
        for tag in raw_tags:
            if not isinstance(tag, dict) or not isinstance(tag.get('Key'), str):
                self.issue('ownership', resource, 'MalformedTags')
                return False
            key = tag['Key']
            duplicates |= key in tags
            tags[key] = tag.get('Value')
        expected = dict(Project=PROJECT, ManagedBy=MANAGED_BY,
                        Repository=self.config['repository'], BenchmarkBranch=self.config['branch'])
        if any(tags.get(key) != value for key, value in expected.items()):
            self.receipt['preserved'] += 1
            return False
        if duplicates:
            self.issue('ownership', resource, 'DuplicateTags')
            return False
        if require_owner and owner != self.config['account']:
            self.issue('ownership', resource, 'WrongOrMissingAccountOwner')
            return False
        if not isinstance(tags.get('RunId'), str) or not _RUN_ID.fullmatch(tags['RunId']):
            self.issue('ownership', resource, 'MalformedRunId')
            return False
        prefix = self.config['resource_prefix']
        if prefix and (not isinstance(tags.get('Name'), str) or not tags['Name'].startswith(prefix)):
            self.issue('ownership', resource, 'WrongResourcePrefix')
            return False
        expiry = tags.get('ExpiresAt')
        if not isinstance(expiry, str) or not _EXPIRY.fullmatch(expiry):
            self.issue('expiry', resource, 'MalformedAbsoluteUTCExpiry')
            return False
        try:
            expiry = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
        except ValueError:
            self.issue('expiry', resource, 'MalformedAbsoluteUTCExpiry')
            return False
        if expiry > self.now:
            self.receipt['preserved'] += 1
            return False
        return True


class _RegionReaper(_OwnedReaper):
    def __init__(self, ec2, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ec2 = ec2

    def page(self, operation, params):
        # Explicit calls are required by source-based IAM permission generation.
        if operation == 'describe_instances':
            return self.ec2.describe_instances(**params)
        if operation == 'describe_network_interfaces':
            return self.ec2.describe_network_interfaces(**params)
        if operation == 'describe_volumes':
            return self.ec2.describe_volumes(**params)
        if operation == 'describe_security_groups':
            return self.ec2.describe_security_groups(**params)
        raise ValueError('unsupported EC2 scan')

    def instances(self):
        candidates = {}
        for page in self.pages('describe_instances', Filters=_filters(self.config)):
            for reservation in page.get('Reservations', []):
                for item in reservation.get('Instances', []):
                    resource = item.get('InstanceId')
                    if not resource:
                        self.issue('describe_instances', '*', 'MissingInstanceId')
                        continue
                    if self.eligible(item, resource, owner=reservation.get('OwnerId'), require_owner=True):
                        candidates[resource] = (item, reservation.get('OwnerId'))
        # Independent IDs: a lost response or one denied instance never blocks the others.
        for resource, snapshot in candidates.items():
            found = self.instance(resource)
            if found is _MISSING:
                continue
            if found is None:
                # Lookup absence can be EC2 propagation, not termination. The
                # verified scan snapshot still authorizes an expiry termination.
                item, owner = snapshot
            else:
                item, owner = found
                if not self.eligible(item, resource, owner=owner, require_owner=True):
                    continue
            state = item.get('State', {}).get('Name')
            if state not in ('pending', 'running', 'stopping', 'stopped', 'shutting-down', 'terminated'):
                self.issue('terminate_instances', resource, 'UnknownInstanceState')
                continue
            if state not in ('shutting-down', 'terminated'):
                self.call('terminate_instances', resource,
                          lambda: self.ec2.terminate_instances(InstanceIds=[resource]))
            # A successful request or shutting-down state is NOT terminal-state proof.
            for attempt in range(self.proof_attempts):
                current = self.instance(resource)
                if current is not None and current is not _MISSING:
                    item, owner = current
                    if owner != self.config['account']:
                        self.issue('termination_proof', resource, 'WrongOrMissingAccountOwner')
                        break
                    if item.get('State', {}).get('Name') == 'terminated':
                        self.receipt['terminated'].append(dict(id=resource, proof='terminated'))
                        break
                if attempt + 1 < self.proof_attempts:
                    self.sleep(self.proof_delay)
            else:
                # NotFound and empty reservations intentionally land here too.
                self.issue('termination_proof', resource, 'NotTerminalRetryNextInvocation')

    def instance(self, resource):
        response = self.call('describe_instances', resource,
                             lambda: self.ec2.describe_instances(InstanceIds=[resource]),
                             absent=('InvalidInstanceID.NotFound',))
        if response is _MISSING:
            return _MISSING
        if response.get('_absent'):
            return None
        found = [(item, reservation.get('OwnerId'))
                 for reservation in response.get('Reservations', [])
                 for item in reservation.get('Instances', [])]
        if not found:
            return None
        if len(found) != 1 or found[0][0].get('InstanceId') != resource:
            self.issue('describe_instances', resource, 'UnexpectedInstanceIdentity')
            return _MISSING
        return found[0]

    def auxiliary(self, kind, operation, collection, id_key):
        candidates = []
        for page in self.pages(operation, Filters=_filters(self.config)):
            for item in page.get(collection, []):
                resource = item.get(id_key)
                if not resource:
                    self.issue(operation, '*', 'MissingResourceIdentity')
                    continue
                require_owner = kind in ('eni', 'security-group')
                if not self.eligible(item, resource, owner=item.get('OwnerId'), require_owner=require_owner):
                    continue
                candidates.append(resource)
        # Finish pagination before deleting items from the scanned collection.
        for resource in dict.fromkeys(candidates):
            self.remove(kind, resource)

    def fetch(self, kind, resource):
        if kind == 'eni':
            response = self.call('describe_network_interfaces', resource,
                                 lambda: self.ec2.describe_network_interfaces(NetworkInterfaceIds=[resource]),
                                 absent=('InvalidNetworkInterfaceID.NotFound',))
            collection, id_key = 'NetworkInterfaces', 'NetworkInterfaceId'
        elif kind == 'volume':
            response = self.call('describe_volumes', resource,
                                 lambda: self.ec2.describe_volumes(VolumeIds=[resource]),
                                 absent=('InvalidVolume.NotFound',))
            collection, id_key = 'Volumes', 'VolumeId'
        elif kind == 'security-group':
            response = self.call('describe_security_groups', resource,
                                 lambda: self.ec2.describe_security_groups(GroupIds=[resource]),
                                 absent=('InvalidGroup.NotFound',))
            collection, id_key = 'SecurityGroups', 'GroupId'
        else:
            response = self.call('describe_placement_groups', resource,
                                 lambda: self.ec2.describe_placement_groups(GroupNames=[resource]),
                                 absent=('InvalidPlacementGroup.Unknown',))
            collection, id_key = 'PlacementGroups', 'GroupName'
        if response is _MISSING:
            return _MISSING
        items = response.get(collection, [])
        if response.get('_absent') or not items:
            return None
        if len(items) != 1 or items[0].get(id_key) != resource:
            self.issue('refresh_' + kind, resource, 'UnexpectedResourceIdentity')
            return _MISSING
        return items[0]

    def detached(self, kind, item, resource):
        if kind == 'eni':
            return item.get('Status') == 'available' and not item.get('Attachment') and not item.get('RequesterManaged', False)
        if kind == 'volume':
            return item.get('State') == 'available' and not item.get('Attachments')
        if kind == 'security-group':
            before = len(self.receipt['errors'])
            # Deliberately no ownership filters: foreign users of an owned SG protect it.
            for page in self.pages('describe_network_interfaces', Filters=[{'Name': 'group-id', 'Values': [resource]}]):
                if page.get('NetworkInterfaces'):
                    return False
            return len(self.receipt['errors']) == before
        before = len(self.receipt['errors'])
        for page in self.pages('describe_instances', Filters=[{'Name': 'placement-group-name', 'Values': [resource]}]):
            for reservation in page.get('Reservations', []):
                if any(instance.get('State', {}).get('Name') != 'terminated'
                       for instance in reservation.get('Instances', [])):
                    return False
        return len(self.receipt['errors']) == before and item.get('State') == 'available'

    def remove(self, kind, resource):
        item = self.fetch(kind, resource)
        if item is _MISSING:
            return
        if item is None:
            self.receipt['deleted'].append(dict(kind=kind, id=resource, proof='absent'))
            return
        if not self.eligible(item, resource, owner=item.get('OwnerId'), require_owner=kind in ('eni', 'security-group')):
            return
        if not self.detached(kind, item, resource):
            self.issue('delete_' + kind, resource, 'AttachedOrNotReadyRetryNextInvocation')
            return
        if kind == 'eni':
            self.call('delete_network_interface', resource,
                      lambda: self.ec2.delete_network_interface(NetworkInterfaceId=resource))
        elif kind == 'volume':
            self.call('delete_volume', resource, lambda: self.ec2.delete_volume(VolumeId=resource))
        elif kind == 'security-group':
            self.call('delete_security_group', resource,
                      lambda: self.ec2.delete_security_group(GroupId=resource))
        else:
            self.call('delete_placement_group', resource,
                      lambda: self.ec2.delete_placement_group(GroupName=resource))
        for attempt in range(self.proof_attempts):
            current = self.fetch(kind, resource)
            if current is None:
                self.receipt['deleted'].append(dict(kind=kind, id=resource, proof='absent'))
                return
            if attempt + 1 < self.proof_attempts:
                self.sleep(self.proof_delay)
        self.issue('deletion_proof', resource, 'StillPresentRetryNextInvocation')

    def run(self):
        try:
            self.instances()
            self.auxiliary('eni', 'describe_network_interfaces', 'NetworkInterfaces', 'NetworkInterfaceId')
            self.auxiliary('volume', 'describe_volumes', 'Volumes', 'VolumeId')
            self.auxiliary('security-group', 'describe_security_groups', 'SecurityGroups', 'GroupId')
            # EC2 DescribePlacementGroups has no pagination token or paginator.
            response = self.call('describe_placement_groups', '*',
                                 lambda: self.ec2.describe_placement_groups(Filters=_filters(self.config)))
            if response is not _MISSING:
                for item in response.get('PlacementGroups', []):
                    resource = item.get('GroupName')
                    if resource and self.eligible(item, resource):
                        self.remove('placement-group', resource)
        except _Limit as error:
            self.issue('regional_cleanup', '*', str(error))
        except Exception as error:
            # Isolate malformed service responses too; all other regions still run.
            self.issue('regional_cleanup', '*', _error_code(error))
        self.receipt['api_calls'] = self.calls
        self.receipt['complete'] = not self.receipt['errors']

    def active_iam_runs(self):
        """Fresh, unfiltered account scan: foreign users of owned profiles count."""
        active = set()
        try:
            for page in self.pages('describe_instances', Filters=[]):
                for reservation in page.get('Reservations', []):
                    if reservation.get('OwnerId') != self.config['account']:
                        self.issue('iam_compute_scan', '*', 'WrongOrMissingAccountOwner')
                        continue
                    for item in reservation.get('Instances', []):
                        state = item.get('State', {}).get('Name')
                        if state == 'terminated':
                            continue
                        if state not in ('pending', 'running', 'stopping', 'stopped', 'shutting-down'):
                            self.issue('iam_compute_scan', item.get('InstanceId', '*'), 'UnknownInstanceState')
                        raw = item.get('Tags', [])
                        if (not isinstance(raw, list)
                                or any(not isinstance(tag, dict) or not isinstance(tag.get('Key'), str) for tag in raw)
                                or len({tag['Key'] for tag in raw}) != len(raw)):
                            self.issue('iam_compute_scan', item.get('InstanceId', '*'), 'MalformedTags')
                            continue
                        values = {tag['Key']: tag.get('Value') for tag in raw}
                        run_id = values.get('RunId')
                        if isinstance(run_id, str) and _RUN_ID.fullmatch(run_id):
                            active.add(run_id)
                        profile = item.get('IamInstanceProfile', {}).get('Arn', '')
                        name = profile.rsplit('/', 1)[-1]
                        match = _IAM_NAME.fullmatch(name)
                        if match:
                            active.add(match.group(1))
        except _Limit as error:
            self.issue('iam_compute_scan', '*', str(error))
        except Exception as error:
            self.issue('iam_compute_scan', '*', _error_code(error))
        self.receipt['active_iam_runs'] = sorted(active)
        self.receipt['api_calls'] = self.calls
        self.receipt['complete'] = not self.receipt['errors']
        return active


class _IAMReaper(_OwnedReaper):
    """Reuse bounded SDK calls and exact-tag expiry validation."""

    def __init__(self, iam, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iam = iam
        # IAM's namespace, not an EC2 Name tag, limits these resources.
        self.config = dict(self.config, resource_prefix='')

    def page(self, operation, params):
        if operation == 'list_roles':
            return self.iam.list_roles(**params)
        if operation == 'list_instance_profiles':
            return self.iam.list_instance_profiles(**params)
        if operation == 'list_instance_profiles_for_role':
            return self.iam.list_instance_profiles_for_role(**params)
        if operation == 'list_role_policies':
            return self.iam.list_role_policies(**params)
        if operation == 'list_attached_role_policies':
            return self.iam.list_attached_role_policies(**params)
        raise ValueError('unsupported IAM scan')

    def get(self, kind, name):
        if kind == 'role':
            response = self.call('get_role', name, lambda: self.iam.get_role(RoleName=name),
                                 absent=('NoSuchEntity',))
            key = 'Role'
        else:
            response = self.call('get_instance_profile', name,
                                 lambda: self.iam.get_instance_profile(InstanceProfileName=name),
                                 absent=('NoSuchEntity',))
            key = 'InstanceProfile'
        if response is _MISSING:
            return _MISSING
        if response.get('_absent'):
            return None
        item = response.get(key)
        if not isinstance(item, dict):
            self.issue('get_' + kind, name, 'MalformedIAMResponse')
            return _MISSING
        return item

    def valid(self, kind, item, name, run_id):
        prefix = 'role/' if kind == 'role' else 'instance-profile/'
        name_key = 'RoleName' if kind == 'role' else 'InstanceProfileName'
        if (item.get(name_key) != name
                or item.get('Arn') != 'arn:aws:iam::' + self.config['account'] + ':' + prefix + name):
            self.issue('iam_ownership', name, 'WrongIAMAccountPathOrName')
            return False
        if not self.eligible(item, name):
            return False
        values = {tag['Key']: tag.get('Value') for tag in item['Tags']}
        if values['RunId'] != run_id:
            self.issue('iam_ownership', name, 'RunIdNameMismatch')
            return False
        if kind == 'role' and item.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn') != self.config['guest_boundary_arn']:
            self.issue('iam_ownership', name, 'WrongOrMissingGuestBoundary')
            return False
        return True

    def role_profiles(self, name):
        profiles = []
        for page in self.pages('list_instance_profiles_for_role', RoleName=name):
            profiles.extend(page.get('InstanceProfiles', []))
        return profiles

    def clean(self, name, active):
        match = _IAM_NAME.fullmatch(name)
        run_id = match.group(1)
        if run_id in active:
            self.issue('iam_cleanup', name, 'NonterminalComputeProtectsRun')
            return
        role, profile = self.get('role', name), self.get('profile', name)
        if role is _MISSING or profile is _MISSING:
            return
        if role is not None and not self.valid('role', role, name, run_id):
            return
        if profile is not None and not self.valid('profile', profile, name, run_id):
            return
        members = profile.get('Roles', []) if profile is not None else []
        if (not isinstance(members, list) or len(members) > 1
                or any(member.get('RoleName') != name or role is None
                       or member.get('Arn') != role.get('Arn') for member in members)):
            self.issue('iam_cleanup', name, 'ForeignOrUnverifiedProfileMembership')
            return
        inline, attached = [], []
        before = len(self.receipt['errors'])
        if role is not None:
            profiles = self.role_profiles(name)
            if any(item.get('InstanceProfileName') != name or profile is None
                   or item.get('Arn') != profile.get('Arn') for item in profiles):
                self.issue('iam_cleanup', name, 'ForeignOrUnverifiedRoleProfileMembership')
            for page in self.pages('list_role_policies', RoleName=name):
                inline.extend(page.get('PolicyNames', []))
            for page in self.pages('list_attached_role_policies', RoleName=name):
                attached.extend(item.get('PolicyArn') for item in page.get('AttachedPolicies', []))
            if set(inline) - {'run-scope'} or set(attached) - {_SSM_POLICY}:
                self.issue('iam_cleanup', name, 'UnexpectedRolePolicies')
        if len(self.receipt['errors']) != before:
            return
        if profile is not None:
            if members:
                self.call('remove_role_from_instance_profile', name,
                          lambda: self.iam.remove_role_from_instance_profile(InstanceProfileName=name, RoleName=name))
                current = self.get('profile', name)
                if current is _MISSING or (current is not None and current.get('Roles')):
                    self.issue('iam_cleanup', name, 'ProfileMembershipRemovalUnconfirmed')
                    return
            self.call('delete_instance_profile', name,
                      lambda: self.iam.delete_instance_profile(InstanceProfileName=name))
            if self.get('profile', name) is not None:
                self.issue('iam_cleanup', name, 'ProfileDeletionUnconfirmed')
                return
            self.receipt['deleted'].append(dict(kind='instance-profile', id=name, proof='NoSuchEntity'))
        if role is not None:
            before = len(self.receipt['errors'])
            if self.role_profiles(name) or len(self.receipt['errors']) != before:
                self.issue('iam_cleanup', name, 'RoleStillHasProfiles')
                return
            for policy in inline:
                self.call('delete_role_policy', name,
                          lambda policy=policy: self.iam.delete_role_policy(RoleName=name, PolicyName=policy))
            for policy in attached:
                self.call('detach_role_policy', name,
                          lambda policy=policy: self.iam.detach_role_policy(RoleName=name, PolicyArn=policy))
            self.call('delete_role', name, lambda: self.iam.delete_role(RoleName=name))
            if self.get('role', name) is not None:
                self.issue('iam_cleanup', name, 'RoleDeletionUnconfirmed')
                return
            self.receipt['deleted'].append(dict(kind='role', id=name, proof='NoSuchEntity'))

    def run_iam(self, active):
        try:
            names = set()
            for page in self.pages('list_roles'):
                names.update(item['RoleName'] for item in page.get('Roles', [])
                             if isinstance(item.get('RoleName'), str) and _IAM_NAME.fullmatch(item['RoleName']))
            for page in self.pages('list_instance_profiles'):
                names.update(item['InstanceProfileName'] for item in page.get('InstanceProfiles', [])
                             if isinstance(item.get('InstanceProfileName'), str) and _IAM_NAME.fullmatch(item['InstanceProfileName']))
            for name in sorted(names):
                try:
                    self.clean(name, active)
                except _Limit:
                    raise
                except Exception as error:
                    self.issue('iam_cleanup', name, _error_code(error))
        except _Limit as error:
            self.issue('iam_cleanup', '*', str(error))
        except Exception as error:
            self.issue('iam_cleanup', '*', _error_code(error))
        self.receipt['api_calls'] = self.calls
        self.receipt['complete'] = not self.receipt['errors']


def reap(clients_by_region, *, account, repository=REPOSITORY, branch=BRANCH,
         now=None, sts_client=None, resource_prefix='', max_api_calls=256,
         max_pages=32, proof_attempts=2, proof_delay=0.25, sleep=time.sleep,
         deadline=None, monotonic=time.monotonic, iam_client=None, guest_boundary_arn=''):
    """Reap expired resources, returning complete=False on any unresolved anomaly.

    clients_by_region maps EVERY approved region to an EC2 client. sts_client is
    required, injectable, and validates the actual credential account before any
    mutation (volumes/placement groups do not expose OwnerId). Each regional scan
    and API retry is finite. Callers must not equate a termination request with
    cleanup completion. Lambda raises IncompleteCleanup for periodic retry.

    Supplying iam_client enables expired six-name per-run IAM cleanup. It
    requires guest_boundary_arn and a fresh, complete cross-region scan proving
    no nonterminal instances remain for that run (including foreign profile
    users). Any compute uncertainty prevents all IAM membership/policy changes.
    """
    config = _config(account, clients_by_region, repository, branch, resource_prefix, guest_boundary_arn)
    if iam_client is not None and not guest_boundary_arn:
        raise ValueError('IAM cleanup requires the approved guest boundary ARN')
    now = _utc(now)
    deadline = monotonic() + 120 if deadline is None else deadline
    if not isinstance(deadline, (int, float)) or not float('-inf') < deadline < float('inf'):
        raise ValueError('deadline must be a finite monotonic timestamp')
    if (not isinstance(max_api_calls, int) or not 1 <= max_api_calls <= 1000
            or not isinstance(max_pages, int) or not 1 <= max_pages <= 100
            or not isinstance(proof_attempts, int) or not 1 <= proof_attempts <= 5
            or not isinstance(proof_delay, (int, float)) or not 0 <= proof_delay <= 2):
        raise ValueError('invalid bounded cleanup limits')
    if sts_client is None or sts_client.get_caller_identity().get('Account') != account:
        raise ValueError('actual AWS credential account does not match configured account')
    receipt = dict(schema=1, version=VERSION, at=now.isoformat(), account=account,
                   repository=repository, branch=branch, regions={}, complete=True)
    # Start every approved region independently: an impaired region must not
    # consume the entire Lambda window before another region even gets scanned.
    reapers = []
    for region, ec2 in clients_by_region.items():
        regional = dict(terminated=[], deleted=[], errors=[], preserved=0)
        receipt['regions'][region] = regional
        reapers.append(_RegionReaper(ec2, config, now, regional, max_api_calls=max_api_calls,
                                   max_pages=max_pages, proof_attempts=proof_attempts,
                                   proof_delay=proof_delay, sleep=sleep,
                                   deadline=deadline, monotonic=monotonic))
    with ThreadPoolExecutor(max_workers=len(reapers)) as executor:
        for future in [executor.submit(reaper.run) for reaper in reapers]:
            future.result()
    if iam_client is not None:
        iam_receipt = dict(deleted=[], errors=[], preserved=0, api_calls=0, complete=False)
        receipt['iam'] = iam_receipt
        with ThreadPoolExecutor(max_workers=len(reapers)) as executor:
            scans = [executor.submit(reaper.active_iam_runs) for reaper in reapers]
            active = set().union(*(future.result() for future in scans))
        if any(regional['errors'] for regional in receipt['regions'].values()):
            iam_receipt['errors'].append(dict(action='iam_cleanup', resource='*',
                                             code='ComputeScanUncertainRetryNextInvocation'))
        else:
            _IAMReaper(iam_client, config, now, iam_receipt, max_api_calls=max_api_calls,
                       max_pages=max_pages, proof_attempts=proof_attempts,
                       proof_delay=proof_delay, sleep=sleep, deadline=deadline,
                       monotonic=monotonic).run_iam(active)
    receipt['complete'] = all(regional['complete'] for regional in receipt['regions'].values())
    if 'iam' in receipt:
        receipt['complete'] &= receipt['iam']['complete']
    return receipt


def verify_readiness(function_config, rule, targets, *, account, regions,
                     guest_boundary_arn, repository=REPOSITORY, branch=BRANCH):
    """Validate control-plane configuration; validate_readiness also invokes health."""
    if not guest_boundary_arn:
        raise ValueError('readiness requires the approved guest boundary ARN')
    config = _config(account, regions, repository, branch, guest_boundary_arn=guest_boundary_arn)
    env = function_config.get('Environment', {}).get('Variables', {})
    arn = function_config.get('FunctionArn', '')
    if (len(arn.split(':')) != 7 or arn.split(':')[2] != 'lambda'
            or arn.split(':')[4] != account):
        raise ValueError('watchdog function must be an unqualified ARN in the approved account')
    if (function_config.get('Handler') not in HANDLERS
            or function_config.get('State') != 'Active'
            or function_config.get('LastUpdateStatus') != 'Successful'
            or not function_config.get('Runtime', '').startswith('python3.')
            or function_config.get('Timeout', 0) < 60):
        raise ValueError('watchdog Lambda must be active, successfully deployed, Python, with timeout >=60s')
    expected = dict(AWS_BENCH_WATCHDOG_VERSION=VERSION, AWS_BENCH_ACCOUNT=account,
                    AWS_BENCH_REPOSITORY=repository, AWS_BENCH_BRANCH=branch,
                    AWS_BENCH_GUEST_BOUNDARY_ARN=guest_boundary_arn)
    if any(env.get(key) != value for key, value in expected.items()):
        raise ValueError('watchdog ownership/version environment does not match campaign')
    if set(env.get('AWS_BENCH_REGIONS', '').split(',')) != set(config['regions']):
        raise ValueError('watchdog must cover exactly the approved campaign regions')
    # Reuse the same validation for optional naming restrictions.
    config = _config(account, env['AWS_BENCH_REGIONS'].split(','), repository, branch,
                     env.get('AWS_BENCH_RESOURCE_PREFIX', ''), guest_boundary_arn)
    if config['resource_prefix'] not in ('', 't', 'th', 'th-'):
        raise ValueError('watchdog naming prefix must cover all th-<RunId> campaign names')
    rule_arn = rule.get('Arn', '')
    if (rule.get('State') != 'ENABLED' or rule.get('ScheduleExpression') != SCHEDULE
            or len(rule_arn.split(':')) < 6 or rule_arn.split(':')[4] != account):
        raise ValueError('watchdog requires an enabled account-owned rate(1 minute) rule')
    if len(targets) != 1 or targets[0].get('Arn') != arn:
        raise ValueError('watchdog rule must have exactly the expected unqualified Lambda target')
    target = targets[0]
    if target.get('InputPath') or target.get('InputTransformer'):
        raise ValueError('watchdog scheduled target must not transform its event')
    if 'Input' in target and json.loads(target['Input']) != {}:
        raise ValueError('watchdog scheduled target must invoke cleanup, not health')
    return dict(config=config, function_arn=arn, rule_arn=rule_arn)


def _verify_eventbridge_permission(policy, verified):
    statements = json.loads(policy).get('Statement', [])
    if isinstance(statements, dict):
        statements = [statements]
    if any(statement.get('Effect') == 'Deny' for statement in statements):
        raise ValueError('watchdog resource policy contains a deny requiring operator review')
    for statement in statements:
        condition = statement.get('Condition', {})
        source_arn = condition.get('ArnLike', condition.get('ArnEquals', {})).get('AWS:SourceArn')
        source_account = condition.get('StringEquals', {}).get('AWS:SourceAccount')
        if (statement.get('Effect') == 'Allow'
                and statement.get('Principal') == {'Service': 'events.amazonaws.com'}
                and statement.get('Action') == 'lambda:InvokeFunction'
                and statement.get('Resource') == verified['function_arn']
                and source_arn == verified['rule_arn']
                and source_account == verified['config']['account']):
            return
    raise ValueError('watchdog lacks exact account/rule-scoped EventBridge invocation permission')


def validate_readiness(lambda_client, events_client, *, function_name, rule_name,
                       account, regions, guest_boundary_arn, repository=REPOSITORY, branch=BRANCH):
    function = lambda_client.get_function_configuration(FunctionName=function_name)
    rule = events_client.describe_rule(Name=rule_name)
    targets = []
    token, seen = None, set()
    for _ in range(10):
        params = dict(Rule=rule_name, Limit=100)
        if token is not None:
            params['NextToken'] = token
        page = events_client.list_targets_by_rule(**params)
        if not isinstance(page, dict) or not isinstance(page.get('Targets'), list):
            raise ValueError('watchdog rule returned malformed targets')
        next_token = page.get('NextToken')
        if next_token is not None:
            if not isinstance(next_token, str) or not next_token or next_token in seen:
                raise ValueError('watchdog rule returned invalid or repeated continuation')
            seen.add(next_token)
        targets.extend(page['Targets'])
        if next_token is None:
            break
        token = next_token
    else:
        raise ValueError('watchdog rule target pagination exceeded its bound')
    verified = verify_readiness(function, rule, targets, account=account, regions=regions,
                                repository=repository, branch=branch, guest_boundary_arn=guest_boundary_arn)
    policy = lambda_client.get_policy(FunctionName=verified['function_arn'])
    _verify_eventbridge_permission(policy['Policy'], verified)
    response = lambda_client.invoke(FunctionName=verified['function_arn'],
                                    InvocationType='RequestResponse',
                                    Payload=b'{"action":"health"}')
    with response['Payload'] as payload:
        health = json.load(payload)
    if (response.get('FunctionError') or response.get('StatusCode') != 200
            or health != dict(schema=1, healthy=True, **verified['config'])):
        raise ValueError('deployed watchdog did not attest expected runtime version/config/account')
    return verified


def lambda_handler(event, context):
    """Health is read-only; every ordinary scheduled invocation reaps all regions."""
    import boto3
    from botocore.config import Config

    config = _environment()
    sdk = Config(connect_timeout=2, read_timeout=4,
                 retries={'total_max_attempts': 2, 'mode': 'standard'})
    sts = boto3.client('sts', config=sdk)
    if context is not None:
        arn = getattr(context, 'invoked_function_arn', '')
        if len(arn.split(':')) < 7 or arn.split(':')[4] != config['account']:
            raise ValueError('Lambda invocation account does not match janitor account')
    if isinstance(event, dict) and event.get('action') == 'health':
        if sts.get_caller_identity().get('Account') != config['account']:
            raise ValueError('actual AWS credential account does not match configured account')
        return dict(schema=1, healthy=True, **config)
    remaining = getattr(context, 'get_remaining_time_in_millis', lambda: 180000)() / 1000
    deadline = time.monotonic() + max(0, remaining - 20)
    clients = {region: boto3.client('ec2', region_name=region, config=sdk)
               for region in config['regions']}
    iam = boto3.client('iam', config=sdk)
    receipt = reap(clients, account=config['account'], repository=config['repository'],
                   branch=config['branch'], sts_client=sts,
                   resource_prefix=config['resource_prefix'], deadline=deadline,
                   iam_client=iam, guest_boundary_arn=config['guest_boundary_arn'])
    bucket = os.environ.get('AWS_BENCH_RESULTS_BUCKET')
    if bucket:
        try:
            export_sdk = Config(connect_timeout=1, read_timeout=2,
                                retries={'total_max_attempts': 1, 'mode': 'standard'})
            s3 = boto3.client('s3', region_name=os.environ.get('AWS_BENCH_RESULTS_REGION'), config=export_sdk)
            # Only append a janitor receipt. Never read/delete fixtures, results or ledger.
            request_id = getattr(context, 'aws_request_id', 'manual')
            if not re.fullmatch(r'[A-Za-z0-9-]{1,100}', request_id):
                raise ValueError('invalid Lambda request ID')
            key = 'ci/watchdog/' + receipt['at'].replace(':', '-') + '-' + request_id + '.json'
            s3.put_object(Bucket=bucket, Key=key, ExpectedBucketOwner=config['account'],
                          ServerSideEncryption='AES256', ContentType='application/json',
                          Body=json.dumps(receipt, sort_keys=True).encode())
            receipt['export'] = dict(bucket=bucket, key=key)
        except Exception as error:
            # All termination already ran. Export can never veto compute cleanup.
            receipt['export_error'] = _error_code(error)
            receipt['complete'] = False
    LOG.log(logging.INFO if receipt['complete'] else logging.ERROR,
            json.dumps(dict(event='benchmark_cleanup', receipt=receipt), sort_keys=True))
    if not receipt['complete']:
        raise IncompleteCleanup(receipt)
    return receipt
