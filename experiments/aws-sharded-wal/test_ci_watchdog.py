"""Offline behavior tests for the independent expiry janitor (no AWS access)."""
import copy
from datetime import datetime, timedelta, timezone
import io
import json
import os
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import ci_watchdog as watchdog

ACCOUNT = '354775971805'
FOREIGN_ACCOUNT = '999999999999'
NOW = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)
BOUNDARY = f'arn:aws:iam::{ACCOUNT}:policy/tidehunter-ci-guest-boundary'
REGION = 'us-east-1'
RUN = 'th-0123456789abcdef'


def tags(**changes):
    values = dict(Project=watchdog.PROJECT, ManagedBy=watchdog.MANAGED_BY,
                  Repository=watchdog.REPOSITORY, BenchmarkBranch=watchdog.BRANCH,
                  RunId=RUN, ExpiresAt=(NOW - timedelta(seconds=1)).isoformat(), Name=RUN)
    values.update(changes)
    return [dict(Key=key, Value=value) for key, value in values.items() if value is not None]


def instance(resource, *, owner=ACCOUNT, state='running', **changes):
    return dict(InstanceId=resource, OwnerId=owner, State={'Name': state}, Tags=tags(**changes))


def eni(resource, *, attached=False, owner=ACCOUNT, **changes):
    return dict(NetworkInterfaceId=resource, OwnerId=owner, Tags=tags(**changes),
                Status='in-use' if attached else 'available',
                Attachment={'InstanceId': 'i-foreign'} if attached else {}, Groups=[])


def volume(resource, *, attached=False, **changes):
    return dict(VolumeId=resource, Tags=tags(**changes), State='in-use' if attached else 'available',
                Attachments=[{'InstanceId': 'i-foreign'}] if attached else [])


def group(resource, *, owner=ACCOUNT, **changes):
    return dict(GroupId=resource, OwnerId=owner, Tags=tags(**changes))


def placement(resource, **changes):
    return dict(GroupName=resource, State='available', Tags=tags(**changes))


class Fault(Exception):
    def __init__(self, code='AccessDenied'):
        self.response = {'Error': {'Code': code, 'Message': 'must-not-log-sensitive-message'}}
        super().__init__('must-not-log-sensitive-message')


class STS:
    def __init__(self, account=ACCOUNT):
        self.account = account

    def get_caller_identity(self):
        return {'Account': self.account}




class EC2:
    """Emulates state transitions, pagination, and independently failing AWS calls."""
    collections = dict(describe_instances='Reservations', describe_network_interfaces='NetworkInterfaces',
                       describe_volumes='Volumes', describe_security_groups='SecurityGroups',
                       describe_placement_groups='PlacementGroups')

    def __init__(self, instances=(), enis=(), volumes=(), groups=(), placements=()):
        self.data = {
            'describe_instances': {item['InstanceId']: copy.deepcopy(item) for item in instances},
            'describe_network_interfaces': {item['NetworkInterfaceId']: copy.deepcopy(item) for item in enis},
            'describe_volumes': {item['VolumeId']: copy.deepcopy(item) for item in volumes},
            'describe_security_groups': {item['GroupId']: copy.deepcopy(item) for item in groups},
            'describe_placement_groups': {item['GroupName']: copy.deepcopy(item) for item in placements},
        }
        self.calls, self.terminations, self.deletions = [], [], []
        self.page_size = 1
        self.page_faults, self.faults, self.stuck, self.lost, self.keep = set(), set(), set(), set(), set()
        self.before_refresh = {}
        self.scan_requests = []

    def scan(self, operation, kwargs):
        assert kwargs['MaxResults'] == 100
        self.scan_requests.append((operation, copy.deepcopy(kwargs)))
        index = int(kwargs.get('NextToken', 'ec2:0').split(':')[1])
        self.calls.append((operation + ':page', index))
        if (operation, index) in self.page_faults:
            raise Fault('ThrottlingException')
        records = self.records(operation, kwargs)
        start = index * self.page_size
        page = self.response(operation, records[start:start + self.page_size])
        if start + self.page_size < len(records):
            page['NextToken'] = 'ec2:' + str(index + 1)
        return page

    def records(self, operation, kwargs):
        records = list(self.data[operation].values())
        for condition in kwargs.get('Filters', []):
            if condition['Name'] == 'group-id':
                records = [item for item in records if any(
                    group['GroupId'] in condition['Values'] for group in item.get('Groups', []))]
            if condition['Name'] == 'placement-group-name':
                records = [item for item in records if item.get('Placement', {}).get('GroupName') in condition['Values']]
        # Ownership filters deliberately return a superset: local validation is mandatory.
        return records

    def response(self, operation, records):
        if operation == 'describe_instances':
            return {'Reservations': [dict(OwnerId=item.get('OwnerId'), Instances=[copy.deepcopy(item)]) for item in records]}
        return {self.collections[operation]: copy.deepcopy(records)}

    def read(self, operation, resource):
        self.calls.append((operation, resource))
        if (operation, resource) in self.faults:
            raise Fault()
        callback = self.before_refresh.pop((operation, resource), None)
        if callback:
            callback(self.data[operation][resource])
        return self.response(operation, [self.data[operation][resource]] if resource in self.data[operation] else [])

    def describe_instances(self, **kwargs):
        if 'InstanceIds' not in kwargs:
            return self.scan('describe_instances', kwargs)
        return self.read('describe_instances', kwargs['InstanceIds'][0])

    def describe_network_interfaces(self, **kwargs):
        if 'NetworkInterfaceIds' not in kwargs:
            return self.scan('describe_network_interfaces', kwargs)
        return self.read('describe_network_interfaces', kwargs['NetworkInterfaceIds'][0])

    def describe_volumes(self, **kwargs):
        if 'VolumeIds' not in kwargs:
            return self.scan('describe_volumes', kwargs)
        return self.read('describe_volumes', kwargs['VolumeIds'][0])

    def describe_security_groups(self, **kwargs):
        if 'GroupIds' not in kwargs:
            return self.scan('describe_security_groups', kwargs)
        return self.read('describe_security_groups', kwargs['GroupIds'][0])

    def describe_placement_groups(self, **kwargs):
        if 'GroupNames' in kwargs:
            return self.read('describe_placement_groups', kwargs['GroupNames'][0])
        self.calls.append(('describe_placement_groups', '*'))
        if ('describe_placement_groups', '*') in self.faults:
            raise Fault()
        return self.response('describe_placement_groups', self.records('describe_placement_groups', kwargs))

    def terminate_instances(self, *, InstanceIds):
        resource, = InstanceIds
        self.calls.append(('terminate_instances', resource))
        if ('terminate_instances', resource) in self.faults:
            raise Fault()
        self.terminations.append(resource)
        self.data['describe_instances'][resource]['State']['Name'] = 'shutting-down' if resource in self.stuck else 'terminated'
        if resource in self.lost:
            raise Fault('ReadTimeoutError')
        return {'TerminatingInstances': [{'InstanceId': resource, 'CurrentState': {'Name': 'shutting-down'}}]}

    def delete(self, operation, resource):
        self.calls.append((operation, resource))
        if (operation, resource) in self.faults:
            raise Fault()
        self.deletions.append((operation, resource))
        if resource not in self.keep:
            del self.data[operation.replace('delete_', 'describe_')][resource]
        return {}

    def delete_network_interface(self, *, NetworkInterfaceId):
        return self.delete('delete_network_interfaces', NetworkInterfaceId)

    def delete_volume(self, *, VolumeId):
        return self.delete('delete_volumes', VolumeId)

    def delete_security_group(self, *, GroupId):
        return self.delete('delete_security_groups', GroupId)

    def delete_placement_group(self, *, GroupName):
        return self.delete('delete_placement_groups', GroupName)


def iam_name(role='controller', run_id=RUN):
    return 'tidehunter-ci-' + run_id + '-' + role


def iam_role(role='controller', *, run_id=RUN, boundary=BOUNDARY, **changes):
    name = iam_name(role, run_id)
    return dict(RoleName=name, Arn=f'arn:aws:iam::{ACCOUNT}:role/{name}',
                PermissionsBoundary={'PermissionsBoundaryArn': boundary},
                Tags=tags(Name=None, RunId=run_id, **changes),
                inline=['run-scope'], attached=[watchdog._SSM_POLICY])


def iam_profile(role='controller', *, run_id=RUN, member=None, **changes):
    name = iam_name(role, run_id)
    return dict(InstanceProfileName=name, Arn=f'arn:aws:iam::{ACCOUNT}:instance-profile/{name}',
                Tags=tags(Name=None, RunId=run_id, **changes),
                Roles=[] if member is None else [copy.deepcopy(member)])


class IAM:
    def __init__(self, roles=(), profiles=()):
        self.roles = {item['RoleName']: copy.deepcopy(item) for item in roles}
        self.profiles = {item['InstanceProfileName']: copy.deepcopy(item) for item in profiles}
        self.calls, self.mutations, self.faults, self.page_faults, self.keep = [], [], set(), set(), set()
        self.scan_requests = []

    def check(self, operation, name):
        self.calls.append((operation, name))
        if (operation, name) in self.faults:
            raise Fault()

    def get_role(self, *, RoleName):
        self.check('get_role', RoleName)
        if RoleName not in self.roles:
            raise Fault('NoSuchEntity')
        return {'Role': copy.deepcopy(self.roles[RoleName])}

    def get_instance_profile(self, *, InstanceProfileName):
        self.check('get_instance_profile', InstanceProfileName)
        if InstanceProfileName not in self.profiles:
            raise Fault('NoSuchEntity')
        return {'InstanceProfile': copy.deepcopy(self.profiles[InstanceProfileName])}

    def list_page(self, operation, kwargs):
        assert kwargs['MaxItems'] == 100
        self.scan_requests.append((operation, copy.deepcopy(kwargs)))
        name = kwargs.get('RoleName', '*')
        self.check(operation, name)
        index = int(kwargs.get('Marker', 'iam:0').split(':')[1])
        if (operation, index) in self.page_faults:
            raise Fault('Throttling')
        if operation == 'list_roles':
            key, records = 'Roles', list(self.roles.values())
        elif operation == 'list_instance_profiles':
            key, records = 'InstanceProfiles', list(self.profiles.values())
        elif operation == 'list_instance_profiles_for_role':
            key = 'InstanceProfiles'
            records = [item for item in self.profiles.values()
                       if any(role['RoleName'] == name for role in item['Roles'])]
        elif operation == 'list_role_policies':
            key, records = 'PolicyNames', self.roles[name]['inline']
        elif operation == 'list_attached_role_policies':
            key = 'AttachedPolicies'
            records = [{'PolicyArn': arn} for arn in self.roles[name]['attached']]
        else:
            raise AssertionError(operation)
        return {key: copy.deepcopy(records[index:index + 1]), 'IsTruncated': index + 1 < len(records),
                'Marker': 'iam:' + str(index + 1)}

    def list_roles(self, **kwargs):
        return self.list_page('list_roles', kwargs)

    def list_instance_profiles(self, **kwargs):
        return self.list_page('list_instance_profiles', kwargs)

    def list_instance_profiles_for_role(self, **kwargs):
        return self.list_page('list_instance_profiles_for_role', kwargs)

    def list_role_policies(self, **kwargs):
        return self.list_page('list_role_policies', kwargs)

    def list_attached_role_policies(self, **kwargs):
        return self.list_page('list_attached_role_policies', kwargs)

    def remove_role_from_instance_profile(self, *, InstanceProfileName, RoleName):
        self.check('remove_role_from_instance_profile', InstanceProfileName)
        self.mutations.append(('remove', InstanceProfileName))
        self.profiles[InstanceProfileName]['Roles'] = []
        return {}

    def delete_instance_profile(self, *, InstanceProfileName):
        self.check('delete_instance_profile', InstanceProfileName)
        if self.profiles[InstanceProfileName]['Roles']:
            raise Fault('DeleteConflict')
        self.mutations.append(('profile', InstanceProfileName))
        if InstanceProfileName not in self.keep:
            del self.profiles[InstanceProfileName]
        return {}

    def delete_role_policy(self, *, RoleName, PolicyName):
        self.check('delete_role_policy', RoleName)
        self.mutations.append(('inline', RoleName))
        self.roles[RoleName]['inline'].remove(PolicyName)
        return {}

    def detach_role_policy(self, *, RoleName, PolicyArn):
        self.check('detach_role_policy', RoleName)
        self.mutations.append(('managed', RoleName))
        self.roles[RoleName]['attached'].remove(PolicyArn)
        return {}

    def delete_role(self, *, RoleName):
        self.check('delete_role', RoleName)
        if (self.roles[RoleName]['inline'] or self.roles[RoleName]['attached']
                or any(any(role['RoleName'] == RoleName for role in item['Roles'])
                       for item in self.profiles.values())):
            raise Fault('DeleteConflict')
        self.mutations.append(('role', RoleName))
        if RoleName not in self.keep:
            del self.roles[RoleName]
        return {}


def reap(ec2, **kwargs):
    return watchdog.reap({REGION: ec2}, account=ACCOUNT, sts_client=STS(), now=NOW,
                         proof_delay=0, **kwargs)


def regional(receipt):
    return receipt['regions'][REGION]


class ReapTests(unittest.TestCase):
    def test_expired_only_boundary_and_foreign_resources_preserved(self):
        ec2 = EC2(instances=[instance('i-expired'), instance('i-boundary', ExpiresAt=NOW.isoformat()),
                             instance('i-active', ExpiresAt=(NOW + timedelta(seconds=1)).isoformat()),
                             instance('i-foreign', Repository='somebody/else'),
                             instance('i-otherbranch', BenchmarkBranch='main'),
                             instance('i-otherproject', Project='Other'),
                             instance('i-othermanager', ManagedBy='Other')])
        receipt = reap(ec2)
        self.assertTrue(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-expired', 'i-boundary'])
        self.assertEqual({entry['proof'] for entry in regional(receipt)['terminated']}, {'terminated'})

    def test_malformed_ownership_expiry_and_wrong_account_fail_closed(self):
        bad = [instance('i-bad-run', RunId='th-not-a-run'), instance('i-missing-run', RunId=None),
               instance('i-naive', ExpiresAt='2026-01-01T00:00:00'),
               instance('i-offset', ExpiresAt='2026-01-01T00:00:00+01:00'),
               instance('i-date', ExpiresAt='yesterday'), instance('i-bad-date', ExpiresAt='2026-02-31T00:00:00Z'),
               instance('i-no-expiry', ExpiresAt=None), instance('i-owner', owner=FOREIGN_ACCOUNT),
               instance('i-no-owner', owner=None)]
        duplicate = instance('i-duplicate')
        duplicate['Tags'].append({'Key': 'RunId', 'Value': RUN})
        bad.append(duplicate)
        malformed = instance('i-malformed')
        malformed['Tags'] = None
        bad.append(malformed)
        ec2 = EC2(instances=bad + [instance('i-good')])
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-good'])
        self.assertEqual(len(regional(receipt)['errors']), len(bad))
        self.assertNotIn('must-not-log-sensitive-message', json.dumps(receipt))

    def test_name_prefix_is_an_additional_restriction(self):
        ec2 = EC2(instances=[instance('i-good', Name='approved-a'), instance('i-other', Name='other')])
        self.assertFalse(reap(ec2, resource_prefix='approved-')['complete'])
        self.assertEqual(ec2.terminations, ['i-good'])

    def test_direct_ec2_pages_preserve_opaque_tokens_and_delete_every_auxiliary(self):
        ec2 = EC2(instances=[instance('i-one'), instance('i-two')],
                  enis=[eni('eni-one'), eni('eni-two')],
                  volumes=[volume('vol-one'), volume('vol-two')],
                  groups=[group('sg-one'), group('sg-two')])
        receipt = reap(ec2)
        self.assertTrue(receipt['complete'])
        self.assertEqual(set(ec2.terminations), {'i-one', 'i-two'})
        self.assertEqual(len(regional(receipt)['deleted']), 6)
        for operation in ('describe_instances', 'describe_network_interfaces',
                          'describe_volumes', 'describe_security_groups'):
            requests = [params for name, params in ec2.scan_requests if name == operation
                        and any(item['Name'].startswith('tag:') for item in params['Filters'])]
            self.assertEqual(len(requests), 2)
            self.assertNotIn('NextToken', requests[0])
            self.assertEqual(requests[1]['NextToken'], 'ec2:1')
            self.assertTrue(all(params['MaxResults'] == 100 and 'Marker' not in params
                                for params in requests))

    def test_malformed_or_cyclic_ec2_continuation_never_authorizes_iam_cleanup(self):
        for mode in ('cycle', 'non-string', 'empty', 'missing-collection'):
            with self.subTest(mode=mode):
                class BadEC2(EC2):
                    def scan(inner, operation, kwargs):
                        page = super().scan(operation, kwargs)
                        if operation == 'describe_instances':
                            if mode == 'cycle':
                                page['NextToken'] = 'ec2:1'
                            elif mode == 'non-string':
                                page['NextToken'] = 7
                            elif mode == 'empty':
                                page['NextToken'] = ''
                            else:
                                del page['Reservations']
                        return page

                ec2 = BadEC2(instances=[instance('i-one'), instance('i-two')])
                iam = IAM([iam_role()])
                receipt = reap(ec2, iam_client=iam, guest_boundary_arn=BOUNDARY)
                self.assertFalse(receipt['complete'])
                self.assertEqual(iam.mutations, [])
                self.assertTrue(any(error['code'] in ('RepeatedContinuationToken',
                                                      'MissingOrMalformedContinuationToken',
                                                      'MalformedPaginationPage')
                                    for error in regional(receipt)['errors']))

    def test_pagination_finds_partial_fleets_in_every_region_without_journal(self):
        first = EC2(instances=[instance('i-one'), instance('i-two')])
        second = EC2(instances=[instance('i-three')])
        receipt = watchdog.reap({REGION: first, 'eu-west-1': second}, account=ACCOUNT,
                                sts_client=STS(), now=NOW, proof_delay=0)
        self.assertTrue(receipt['complete'])
        self.assertEqual(first.terminations, ['i-one', 'i-two'])
        self.assertEqual(second.terminations, ['i-three'])
        self.assertIn(('describe_instances:page', 1), first.calls)

    def test_page_failure_cleans_discovered_hosts_and_other_regions(self):
        first = EC2(instances=[instance('i-one'), instance('i-unseen')])
        first.page_faults.add(('describe_instances', 1))
        second = EC2(instances=[instance('i-other-region')])
        receipt = watchdog.reap({REGION: first, 'eu-west-1': second}, account=ACCOUNT,
                                sts_client=STS(), now=NOW, proof_delay=0)
        self.assertFalse(receipt['complete'])
        self.assertEqual(first.terminations, ['i-one'])
        self.assertEqual(second.terminations, ['i-other-region'])

    def test_api_failure_does_not_skip_independent_host_or_auxiliary(self):
        ec2 = EC2(instances=[instance('i-denied'), instance('i-good')], volumes=[volume('vol-orphan')])
        ec2.faults.add(('terminate_instances', 'i-denied'))
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-good'])
        self.assertIn(('delete_volumes', 'vol-orphan'), ec2.deletions)
        self.assertTrue(any(error['resource'] == 'i-denied' for error in regional(receipt)['errors']))

    def test_lost_termination_response_is_reconciled_by_terminal_proof(self):
        ec2 = EC2(instances=[instance('i-lost'), instance('i-good')])
        ec2.lost.add('i-lost')
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])  # Response anomaly is surfaced, not hidden.
        self.assertEqual({entry['id'] for entry in regional(receipt)['terminated']}, {'i-lost', 'i-good'})
        self.assertTrue(reap(ec2)['complete'])  # Periodic retry has no unresolved work.

    def test_shutting_down_request_is_not_terminal_proof(self):
        ec2 = EC2(instances=[instance('i-stuck'), instance('i-good')])
        ec2.stuck.add('i-stuck')
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual([entry['id'] for entry in regional(receipt)['terminated']], ['i-good'])
        self.assertTrue(any(error['code'] == 'NotTerminalRetryNextInvocation' for error in regional(receipt)['errors']))

    def test_missing_instance_lookup_still_requests_termination_but_is_not_proof(self):
        for mode in ('empty', 'not-found'):
            with self.subTest(mode=mode):
                class Missing(EC2):
                    def describe_instances(inner, **kwargs):
                        if 'InstanceIds' not in kwargs:
                            return super().describe_instances(**kwargs)
                        if mode == 'not-found':
                            raise Fault('InvalidInstanceID.NotFound')
                        return {'Reservations': []}

                ec2 = Missing(instances=[instance('i-propagating')])
                receipt = reap(ec2)
                self.assertEqual(ec2.terminations, ['i-propagating'])
                self.assertFalse(receipt['complete'])
                self.assertEqual(regional(receipt)['terminated'], [])
                self.assertTrue(any(error['code'] == 'NotTerminalRetryNextInvocation'
                                    for error in regional(receipt)['errors']))

    def test_transient_missing_lookup_then_verified_terminal_state_completes(self):
        class Propagating(EC2):
            missing = True

            def describe_instances(inner, **kwargs):
                if 'InstanceIds' not in kwargs:
                    return super().describe_instances(**kwargs)
                if inner.missing:
                    inner.missing = False
                    return {'Reservations': []}
                return super().describe_instances(**kwargs)

        ec2 = Propagating(instances=[instance('i-propagating')])
        receipt = reap(ec2)
        self.assertTrue(receipt['complete'])
        self.assertEqual(regional(receipt)['terminated'], [{'id': 'i-propagating', 'proof': 'terminated'}])

    def test_refresh_protects_resources_that_are_now_unexpired(self):
        ec2 = EC2(instances=[instance('i-extended')], volumes=[volume('vol-extended')])
        future = (NOW + timedelta(seconds=1)).isoformat()
        ec2.before_refresh[('describe_instances', 'i-extended')] = lambda item: item.update(Tags=tags(ExpiresAt=future))
        ec2.before_refresh[('describe_volumes', 'vol-extended')] = lambda item: item.update(Tags=tags(ExpiresAt=future))
        self.assertTrue(reap(ec2)['complete'])
        self.assertEqual(ec2.terminations, [])
        self.assertEqual(ec2.deletions, [])

    def test_detached_only_auxiliary_cleanup_and_foreign_users_protect_dependencies(self):
        attached = eni('eni-foreign-user', attached=True, Repository='foreign/repo')
        attached['Groups'] = [{'GroupId': 'sg-in-use'}]
        pg_user = instance('i-active-foreign', Repository='foreign/repo')
        pg_user['Placement'] = {'GroupName': 'pg-in-use'}
        ec2 = EC2(instances=[pg_user, instance('i-expired')],
                  enis=[eni('eni-free'), eni('eni-attached', attached=True), attached,
                        eni('eni-foreign', owner=FOREIGN_ACCOUNT)],
                  volumes=[volume('vol-free'), volume('vol-attached', attached=True),
                           volume('vol-active', ExpiresAt=(NOW + timedelta(seconds=1)).isoformat())],
                  groups=[group('sg-free'), group('sg-in-use'), group('sg-foreign', owner=FOREIGN_ACCOUNT)],
                  placements=[placement('pg-free'), placement('pg-in-use')])
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-expired'])
        self.assertEqual(set(ec2.deletions), {('delete_network_interfaces', 'eni-free'),
                                             ('delete_volumes', 'vol-free'),
                                             ('delete_security_groups', 'sg-free'),
                                             ('delete_placement_groups', 'pg-free')})
        first_delete = next(index for index, entry in enumerate(ec2.calls) if entry[0].startswith('delete_'))
        terminal_read = max(index for index, entry in enumerate(ec2.calls[:first_delete])
                            if entry == ('describe_instances', 'i-expired'))
        self.assertLess(terminal_read, first_delete)

    def test_auxiliary_api_failure_continues_other_resources_and_proves_deletion(self):
        ec2 = EC2(volumes=[volume('vol-denied'), volume('vol-good')], groups=[group('sg-good')])
        ec2.faults.add(('delete_volumes', 'vol-denied'))
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual({entry['id'] for entry in regional(receipt)['deleted']}, {'vol-good', 'sg-good'})
        self.assertTrue(all(entry['proof'] == 'absent' for entry in regional(receipt)['deleted']))

    def test_successful_delete_without_absence_is_incomplete(self):
        ec2 = EC2(volumes=[volume('vol-stuck')])
        ec2.keep.add('vol-stuck')
        receipt = reap(ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(regional(receipt)['deleted'], [])
        self.assertTrue(any(error['code'] == 'StillPresentRetryNextInvocation' for error in regional(receipt)['errors']))

    def test_page_bound_is_explicit_and_does_not_claim_full_scan(self):
        ec2 = EC2(instances=[instance('i-one'), instance('i-two')])
        receipt = reap(ec2, max_pages=1)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-one'])
        self.assertNotIn(('describe_instances:page', 1), ec2.calls)

    def test_call_bound_cannot_skip_an_independent_region(self):
        first = EC2(instances=[instance('i-one'), instance('i-two')])
        second = EC2(instances=[])
        receipt = watchdog.reap({REGION: first, 'eu-west-1': second}, account=ACCOUNT,
                                sts_client=STS(), now=NOW, max_api_calls=1, proof_delay=0)
        self.assertFalse(receipt['complete'])
        self.assertEqual(regional(receipt)['api_calls'], 1)
        self.assertTrue(second.calls)

    def test_expired_deadline_surfaces_retry_for_every_region_without_mutation(self):
        first, second = EC2(instances=[instance('i-one')]), EC2(instances=[instance('i-two')])
        receipt = watchdog.reap({REGION: first, 'eu-west-1': second}, account=ACCOUNT,
                                sts_client=STS(), now=NOW, deadline=100, monotonic=lambda: 100)
        self.assertFalse(receipt['complete'])
        self.assertEqual(first.calls + second.calls, [])
        for result in receipt['regions'].values():
            self.assertEqual(result['errors'][0]['code'], 'DeadlineRetryNextInvocation')

    def test_slow_region_cannot_prevent_another_region_starting(self):
        other_started = threading.Event()

        class Slow(EC2):
            def describe_instances(inner, **kwargs):
                if 'InstanceIds' not in kwargs and not other_started.wait(timeout=2):
                    raise Fault('OtherRegionWasNotStarted')
                return super().describe_instances(**kwargs)

        class Other(EC2):
            def describe_instances(inner, **kwargs):
                other_started.set()
                return super().describe_instances(**kwargs)

        slow, other = Slow(instances=[instance('i-slow')]), Other(instances=[instance('i-other')])
        receipt = watchdog.reap({REGION: slow, 'eu-west-1': other}, account=ACCOUNT,
                                sts_client=STS(), now=NOW, proof_delay=0)
        self.assertTrue(receipt['complete'])
        self.assertEqual(slow.terminations, ['i-slow'])
        self.assertEqual(other.terminations, ['i-other'])

    def test_actual_credential_account_must_match_before_any_ec2_call(self):
        ec2 = EC2(instances=[instance('i-protected')])
        with self.assertRaises(ValueError):
            watchdog.reap({REGION: ec2}, account=ACCOUNT, now=NOW, sts_client=STS(FOREIGN_ACCOUNT))
        with self.assertRaises(ValueError):
            watchdog.reap({REGION: ec2}, account=ACCOUNT, now=NOW)
        self.assertEqual(ec2.calls, [])

    def test_invalid_campaign_or_clock_is_rejected_before_mutations(self):
        ec2 = EC2(instances=[instance('i-protected')])
        with self.assertRaises(ValueError):
            reap(ec2, branch='main')
        with self.assertRaises(ValueError):
            watchdog.reap({REGION: ec2}, account=ACCOUNT, sts_client=STS(), now=NOW.replace(tzinfo=None))
        self.assertEqual(ec2.calls, [])


class IAMCleanupTests(unittest.TestCase):
    def reap(self, iam, ec2=None, **kwargs):
        return reap(EC2() if ec2 is None else ec2, iam_client=iam,
                    guest_boundary_arn=BOUNDARY, **kwargs)

    def test_complete_expired_run_profiles_roles_and_policies_removed_with_pagination(self):
        kinds = ['controller'] + ['storage-' + str(index) for index in range(5)]
        roles = [iam_role(kind) for kind in kinds]
        profiles = [iam_profile(kind, member=item) for kind, item in zip(kinds, roles)]
        iam = IAM(roles, profiles)
        ec2 = EC2(instances=[instance('i-expired')])
        receipt = self.reap(iam, ec2)
        self.assertTrue(receipt['complete'])
        self.assertEqual(ec2.terminations, ['i-expired'])
        self.assertEqual(iam.roles, {})
        self.assertEqual(iam.profiles, {})
        self.assertEqual(len(receipt['iam']['deleted']), 12)
        self.assertEqual(sum(operation == 'list_roles' for operation, _ in iam.calls), 6)
        self.assertEqual(sum(operation == 'list_instance_profiles' for operation, _ in iam.calls), 6)
        for name in map(iam_name, kinds):
            self.assertLess(iam.mutations.index(('remove', name)), iam.mutations.index(('profile', name)))
            self.assertLess(iam.mutations.index(('profile', name)), iam.mutations.index(('inline', name)))
            self.assertLess(iam.mutations.index(('managed', name)), iam.mutations.index(('role', name)))

    def test_partial_creation_role_only_and_empty_profile_only_do_not_need_runner_disk(self):
        iam = IAM(roles=[iam_role()], profiles=[iam_profile('storage-0')])
        receipt = self.reap(iam)
        self.assertTrue(receipt['complete'])
        self.assertEqual(iam.roles, {})
        self.assertEqual(iam.profiles, {})
        self.assertIn(('role', iam_name()), iam.mutations)
        self.assertIn(('profile', iam_name('storage-0')), iam.mutations)

    def test_shared_foreign_namespace_tags_owner_expiry_and_boundary_are_preserved(self):
        foreign = iam_role('storage-0', Repository='someone/else')
        unexpired = iam_role('storage-1', ExpiresAt=(NOW + timedelta(seconds=1)).isoformat())
        malformed = iam_role('storage-2', ExpiresAt='tomorrow')
        boundary = iam_role('storage-3', boundary=BOUNDARY + '-other')
        wrong_account = iam_role('storage-4')
        wrong_account['Arn'] = wrong_account['Arn'].replace(ACCOUNT, FOREIGN_ACCOUNT)
        shared = iam_role()
        shared['RoleName'] = 'tidehunter-ci-watchdog'
        shared['Arn'] = f'arn:aws:iam::{ACCOUNT}:role/tidehunter-ci-watchdog'
        wrong_prefix = iam_role()
        wrong_prefix['RoleName'] = 'other-' + RUN + '-controller'
        wrong_prefix['Arn'] = 'arn:aws:iam::' + ACCOUNT + ':role/' + wrong_prefix['RoleName']
        iam = IAM([foreign, unexpired, malformed, boundary, wrong_account, shared, wrong_prefix])
        receipt = self.reap(iam)
        self.assertFalse(receipt['complete'])
        self.assertEqual(iam.mutations, [])
        self.assertEqual(len(iam.roles), 7)
        self.assertFalse(any(name == shared['RoleName'] and operation == 'get_role' for operation, name in iam.calls))

    def test_runid_tag_must_match_exact_role_namespace(self):
        role = iam_role()
        role['Tags'] = tags(Name=None, RunId='th-ffffffffffffffff')
        iam = IAM([role])
        receipt = self.reap(iam)
        self.assertFalse(receipt['complete'])
        self.assertEqual(iam.mutations, [])
        self.assertTrue(any(error['code'] == 'RunIdNameMismatch' for error in receipt['iam']['errors']))

    def test_any_nonterminal_owned_run_host_protects_iam_even_when_unexpired(self):
        role = iam_role()
        iam = IAM([role], [iam_profile(member=role)])
        ec2 = EC2(instances=[instance('i-active', ExpiresAt=(NOW + timedelta(seconds=1)).isoformat())])
        receipt = self.reap(iam, ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, [])
        self.assertEqual(iam.mutations, [])
        self.assertIn(RUN, regional(receipt)['active_iam_runs'])

    def test_foreign_host_using_exact_profile_protects_iam_without_being_terminated(self):
        role = iam_role()
        profile = iam_profile(member=role)
        iam = IAM([role], [profile])
        host = instance('i-foreign', Repository='someone/else', RunId='foreign-run')
        host['IamInstanceProfile'] = {'Arn': profile['Arn']}
        ec2 = EC2(instances=[host])
        receipt = self.reap(iam, ec2)
        self.assertFalse(receipt['complete'])
        self.assertEqual(ec2.terminations, [])
        self.assertEqual(iam.mutations, [])

    def test_active_other_run_does_not_block_independent_orphan_cleanup(self):
        other = 'th-ffffffffffffffff'
        iam = IAM([iam_role()])
        ec2 = EC2(instances=[instance('i-other', RunId=other,
                                     ExpiresAt=(NOW + timedelta(seconds=1)).isoformat())])
        receipt = self.reap(iam, ec2)
        self.assertTrue(receipt['complete'])
        self.assertEqual(iam.roles, {})
        self.assertEqual(ec2.terminations, [])

    def test_direct_iam_pages_forward_markers_and_ignore_final_nontruncated_marker(self):
        iam = IAM([iam_role(), iam_role('storage-0')],
                  [iam_profile(), iam_profile('storage-0')])
        receipt = self.reap(iam)
        self.assertTrue(receipt['complete'])
        self.assertEqual(iam.roles, {})
        self.assertEqual(iam.profiles, {})
        for operation in ('list_roles', 'list_instance_profiles'):
            requests = [params for name, params in iam.scan_requests if name == operation]
            self.assertEqual(len(requests), 2)
            self.assertNotIn('Marker', requests[0])
            self.assertEqual(requests[1]['Marker'], 'iam:1')
            self.assertTrue(all(params['MaxItems'] == 100 and 'NextToken' not in params
                                for params in requests))

    def test_invalid_iam_policy_continuation_preserves_all_identity_permissions(self):
        for mode in ('cycle', 'missing-marker', 'non-string-marker',
                     'missing-flag', 'non-boolean-flag'):
            with self.subTest(mode=mode):
                class BadIAM(IAM):
                    def list_role_policies(inner, **kwargs):
                        page = super().list_role_policies(**kwargs)
                        page.update(IsTruncated=True, Marker='iam:1')
                        if mode == 'missing-marker':
                            del page['Marker']
                        elif mode == 'non-string-marker':
                            page['Marker'] = 7
                        elif mode == 'missing-flag':
                            del page['IsTruncated']
                        elif mode == 'non-boolean-flag':
                            page['IsTruncated'] = 'true'
                        return page

                role = iam_role()
                iam = BadIAM([role], [iam_profile(member=role)])
                receipt = self.reap(iam)
                self.assertFalse(receipt['complete'])
                self.assertEqual(iam.mutations, [])
                self.assertTrue(any(error['code'] in ('RepeatedContinuationToken',
                                                      'MissingOrMalformedContinuationToken',
                                                      'MissingOrMalformedTruncationFlag')
                                    for error in receipt['iam']['errors']))

    def test_empty_truncated_iam_page_continues_to_real_policy(self):
        class EmptyFirst(IAM):
            def list_role_policies(inner, **kwargs):
                page = super().list_role_policies(**kwargs)
                if 'Marker' not in kwargs:
                    return {'PolicyNames': [], 'IsTruncated': True, 'Marker': 'iam:0'}
                return page

        iam = EmptyFirst([iam_role()])
        receipt = self.reap(iam)
        self.assertTrue(receipt['complete'])
        self.assertEqual(iam.roles, {})
        requests = [params for name, params in iam.scan_requests if name == 'list_role_policies']
        self.assertEqual([params.get('Marker') for params in requests], [None, 'iam:0'])
        self.assertTrue(all(params['RoleName'] == iam_name() for params in requests))

    def test_iam_page_limit_does_not_overfetch_or_remove_permissions(self):
        class Truncated(IAM):
            def list_role_policies(inner, **kwargs):
                page = super().list_role_policies(**kwargs)
                page.update(IsTruncated=True, Marker='iam:1')
                return page

        iam = Truncated([iam_role()])
        receipt = self.reap(iam, max_pages=1)
        self.assertFalse(receipt['complete'])
        self.assertEqual(iam.mutations, [])
        self.assertEqual(sum(name == 'list_role_policies' for name, _ in iam.calls), 1)
        self.assertTrue(any(error['code'] == 'PageLimitReached' for error in receipt['iam']['errors']))

    def test_explicit_iam_page_calls_share_the_existing_api_budget(self):
        iam = IAM([iam_role()])
        receipt = self.reap(iam, max_api_calls=8)
        self.assertFalse(receipt['complete'])
        self.assertEqual(receipt['iam']['api_calls'], 8)
        self.assertEqual(len(iam.calls), 8)
        self.assertEqual(iam.mutations, [])
        self.assertTrue(any(error['code'] == 'APICallLimitRetryNextInvocation'
                            for error in receipt['iam']['errors']))

    def test_any_region_scan_uncertainty_prevents_every_iam_mutation(self):
        iam = IAM([iam_role()])
        first, uncertain = EC2(), EC2()
        uncertain.page_faults.add(('describe_instances', 0))
        receipt = watchdog.reap({REGION: first, 'eu-west-1': uncertain}, account=ACCOUNT,
                                sts_client=STS(), iam_client=iam, guest_boundary_arn=BOUNDARY,
                                now=NOW, proof_delay=0)
        self.assertFalse(receipt['complete'])
        self.assertEqual(iam.mutations, [])
        self.assertEqual(receipt['iam']['errors'][0]['code'], 'ComputeScanUncertainRetryNextInvocation')

    def test_foreign_or_malformed_profile_tags_protect_role_as_well(self):
        for change in ({'Repository': 'someone/else'}, {'ExpiresAt': None},
                       {'ExpiresAt': '2026-01-01'}, {'RunId': 'th-ffffffffffffffff'}):
            with self.subTest(change=change):
                role = iam_role()
                profile = iam_profile(member=role)
                profile['Tags'] = tags(Name=None, **change)
                iam = IAM([role], [profile])
                self.reap(iam)
                self.assertEqual(iam.mutations, [])

    def test_foreign_profile_membership_or_unexpected_policy_blocks_all_role_mutations(self):
        for mode in ('foreign-profile', 'foreign-member', 'extra-inline', 'extra-managed'):
            with self.subTest(mode=mode):
                role = iam_role()
                profile = iam_profile(member=role)
                if mode == 'foreign-profile':
                    profile['InstanceProfileName'] = 'foreign-profile'
                    profile['Arn'] = f'arn:aws:iam::{ACCOUNT}:instance-profile/foreign-profile'
                elif mode == 'foreign-member':
                    profile['Roles'][0]['RoleName'] = 'foreign-role'
                    profile['Roles'][0]['Arn'] = f'arn:aws:iam::{ACCOUNT}:role/foreign-role'
                elif mode == 'extra-inline':
                    role['inline'].append('someone-elses-policy')
                else:
                    role['attached'].append('arn:aws:iam::aws:policy/AdministratorAccess')
                iam = IAM([role], [profile])
                receipt = self.reap(iam)
                self.assertFalse(receipt['complete'])
                self.assertEqual(iam.mutations, [])

    def test_one_iam_api_failure_cannot_skip_independent_identity(self):
        iam = IAM([iam_role(), iam_role('storage-0')])
        iam.faults.add(('delete_role_policy', iam_name()))
        receipt = self.reap(iam)
        self.assertFalse(receipt['complete'])
        self.assertIn(iam_name(), iam.roles)
        self.assertNotIn(iam_name('storage-0'), iam.roles)

    def test_iam_success_response_without_absence_is_incomplete(self):
        iam = IAM(profiles=[iam_profile()])
        iam.keep.add(iam_name())
        receipt = self.reap(iam)
        self.assertFalse(receipt['complete'])
        self.assertEqual(receipt['iam']['deleted'], [])
        self.assertTrue(any(error['code'] == 'ProfileDeletionUnconfirmed' for error in receipt['iam']['errors']))

    def test_missing_or_foreign_boundary_prevents_cleanup_before_aws_calls(self):
        iam = IAM([iam_role()])
        for boundary in ('', f'arn:aws:iam::{FOREIGN_ACCOUNT}:policy/foreign'):
            with self.subTest(boundary=boundary):
                with self.assertRaises(ValueError):
                    reap(EC2(), iam_client=iam, guest_boundary_arn=boundary)
        self.assertEqual(iam.calls, [])


def environment():
    return dict(AWS_BENCH_ACCOUNT=ACCOUNT, AWS_BENCH_REGIONS=REGION,
                AWS_BENCH_REPOSITORY=watchdog.REPOSITORY, AWS_BENCH_BRANCH=watchdog.BRANCH,
                AWS_BENCH_WATCHDOG_VERSION=watchdog.VERSION,
                AWS_BENCH_GUEST_BOUNDARY_ARN=BOUNDARY)


def function_configuration():
    return dict(FunctionArn=f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:janitor',
                Handler=watchdog.HANDLER, State='Active', LastUpdateStatus='Successful',
                Runtime='python3.12', Timeout=180,
                Environment={'Variables': environment()})


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.function = function_configuration()
        self.rule = dict(Arn=f'arn:aws:events:{REGION}:{ACCOUNT}:rule/janitor',
                         State='ENABLED', ScheduleExpression=watchdog.SCHEDULE)
        self.targets = [{'Id': 'janitor', 'Arn': self.function['FunctionArn']}]

    def validate(self):
        return watchdog.verify_readiness(self.function, self.rule, self.targets,
                                         account=ACCOUNT, regions=[REGION], guest_boundary_arn=BOUNDARY)

    def test_active_exact_owned_version_schedule_and_target_accepted(self):
        self.assertEqual(self.validate()['function_arn'], self.function['FunctionArn'])

    def test_inline_cloudformation_handler_is_accepted(self):
        self.function['Handler'] = 'index.lambda_handler'
        self.assertEqual(self.validate()['function_arn'], self.function['FunctionArn'])

    def test_incompatible_resource_prefix_cannot_admit_real_campaign(self):
        for prefix in ('other-', 'tidehunter-ci-', RUN):
            with self.subTest(prefix=prefix):
                self.function['Environment']['Variables']['AWS_BENCH_RESOURCE_PREFIX'] = prefix
                with self.assertRaises(ValueError):
                    self.validate()
        self.function['Environment']['Variables']['AWS_BENCH_RESOURCE_PREFIX'] = 'th-'
        self.assertEqual(self.validate()['config']['resource_prefix'], 'th-')

    def test_wrong_guest_boundary_cannot_admit_campaign(self):
        self.function['Environment']['Variables']['AWS_BENCH_GUEST_BOUNDARY_ARN'] = BOUNDARY + '-other'
        with self.assertRaises(ValueError):
            self.validate()

    def test_bad_version_disabled_rule_or_foreign_target_rejected(self):
        cases = [lambda: self.function['Environment']['Variables'].update(AWS_BENCH_WATCHDOG_VERSION='old'),
                 lambda: self.function['Environment']['Variables'].update(AWS_BENCH_REGIONS='eu-west-1'),
                 lambda: self.function.update(State='Pending'),
                 lambda: self.function.update(Timeout=30),
                 lambda: self.rule.update(State='DISABLED'),
                 lambda: self.rule.update(ScheduleExpression='rate(5 minutes)'),
                 lambda: self.targets[0].update(Arn='arn:aws:lambda:us-east-1:999999999999:function:other'),
                 lambda: self.targets[0].update(Input='{"action":"health"}')]
        for change in cases:
            with self.subTest(change=change):
                self.setUp()
                change()
                with self.assertRaises(ValueError):
                    self.validate()

    def permission(self):
        return json.dumps({'Statement': [{
            'Effect': 'Allow', 'Principal': {'Service': 'events.amazonaws.com'},
            'Action': 'lambda:InvokeFunction', 'Resource': self.function['FunctionArn'],
            'Condition': {'ArnLike': {'AWS:SourceArn': self.rule['Arn']},
                          'StringEquals': {'AWS:SourceAccount': ACCOUNT}},
        }]})

    def test_eventbridge_permission_requires_exact_rule_and_account(self):
        verified = self.validate()
        watchdog._verify_eventbridge_permission(self.permission(), verified)
        missing = json.loads(self.permission())
        missing['Statement'][0]['Condition']['ArnLike']['AWS:SourceArn'] = '*'
        with self.assertRaises(ValueError):
            watchdog._verify_eventbridge_permission(json.dumps(missing), verified)
        missing = json.loads(self.permission())
        del missing['Statement'][0]['Condition']['StringEquals']
        with self.assertRaises(ValueError):
            watchdog._verify_eventbridge_permission(json.dumps(missing), verified)

    def test_direct_rule_target_pages_forward_tokens_and_bound_unknown_state(self):
        verified = self.validate()
        health = dict(schema=1, healthy=True, **verified['config'])
        lambda_client = SimpleNamespace(
            get_function_configuration=lambda **kw: self.function,
            get_policy=lambda **kw: {'Policy': self.permission()},
            invoke=lambda **kw: {'StatusCode': 200, 'Payload': io.BytesIO(json.dumps(health).encode())})
        arguments = dict(function_name='janitor', rule_name='janitor', account=ACCOUNT,
                         regions=[REGION], guest_boundary_arn=BOUNDARY)
        outer = self

        class Events:
            def __init__(inner, mode):
                inner.mode, inner.requests = mode, []

            def describe_rule(inner, **kwargs):
                return outer.rule

            def list_targets_by_rule(inner, **kwargs):
                inner.requests.append(kwargs)
                if inner.mode == 'good':
                    return ({'Targets': [], 'NextToken': 'opaque:event:next'}
                            if 'NextToken' not in kwargs else {'Targets': outer.targets})
                if inner.mode == 'cycle':
                    return {'Targets': [], 'NextToken': 'same'}
                if inner.mode == 'malformed':
                    return {'Targets': [], 'NextToken': 5}
                return {'Targets': [], 'NextToken': 'token:' + str(len(inner.requests))}

        events = Events('good')
        self.assertEqual(watchdog.validate_readiness(lambda_client, events, **arguments), verified)
        self.assertEqual([params.get('NextToken') for params in events.requests],
                         [None, 'opaque:event:next'])
        self.assertTrue(all(params['Rule'] == 'janitor' and params['Limit'] == 100
                            for params in events.requests))
        for mode, calls in (('cycle', 2), ('malformed', 1), ('limit', 10)):
            with self.subTest(mode=mode):
                events = Events(mode)
                with self.assertRaises(ValueError):
                    watchdog.validate_readiness(lambda_client, events, **arguments)
                self.assertEqual(len(events.requests), calls)

    def test_runtime_health_is_required_not_just_self_declared_environment(self):
        verified = self.validate()
        health = dict(schema=1, healthy=True, **verified['config'])
        lambda_client = SimpleNamespace(
            get_function_configuration=lambda **kw: self.function,
            get_policy=lambda **kw: {'Policy': self.permission()},
            invoke=lambda **kw: {'StatusCode': 200, 'Payload': io.BytesIO(json.dumps(health).encode())})
        targets = self.targets

        class Events:
            def describe_rule(inner, **kwargs):
                return self.rule

            def list_targets_by_rule(inner, **kwargs):
                return {'Targets': targets}

        arguments = dict(function_name='janitor', rule_name='janitor', account=ACCOUNT,
                         regions=[REGION], guest_boundary_arn=BOUNDARY)
        self.assertEqual(watchdog.validate_readiness(lambda_client, Events(), **arguments), verified)
        health['version'] = 'old'
        with self.assertRaises(ValueError):
            watchdog.validate_readiness(lambda_client, Events(), **arguments)


class HandlerTests(unittest.TestCase):
    def modules(self, ec2, *, fail_export=False):
        self.created, self.uploads = [], []
        outer = self

        class S3:
            def put_object(self, **kwargs):
                outer.uploads.append(kwargs)
                if fail_export:
                    raise Fault('AccessDenied')
                return {}

        def client(service, **kwargs):
            self.created.append((service, kwargs))
            return {'sts': STS(), 'ec2': ec2, 's3': S3(), 'iam': IAM()}[service]

        return {'boto3': SimpleNamespace(client=client),
                'botocore': SimpleNamespace(),
                'botocore.config': SimpleNamespace(Config=lambda **kwargs: kwargs)}

    def context(self):
        return SimpleNamespace(invoked_function_arn=function_configuration()['FunctionArn'],
                               aws_request_id='01234567-abcd-1234-abcd-123456789012')

    def test_health_never_creates_compute_clients_or_mutates(self):
        ec2 = EC2(instances=[instance('i-expired')])
        with patch.dict(os.environ, environment(), clear=True), patch.dict(sys.modules, self.modules(ec2)):
            health = watchdog.lambda_handler({'action': 'health'}, self.context())
        self.assertTrue(health['healthy'])
        self.assertEqual([service for service, _ in self.created], ['sts'])
        self.assertEqual(ec2.calls, [])

    def test_optional_receipt_export_failure_cannot_suppress_termination(self):
        ec2 = EC2(instances=[instance('i-expired')])
        env = environment() | {'AWS_BENCH_RESULTS_BUCKET': 'known-results', 'AWS_BENCH_RESULTS_REGION': REGION}
        with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, self.modules(ec2, fail_export=True)), \
                patch.object(watchdog, '_utc', return_value=NOW), patch.object(watchdog.LOG, 'log') as logger:
            with self.assertRaises(watchdog.IncompleteCleanup) as caught:
                watchdog.lambda_handler({}, self.context())
        self.assertEqual(ec2.terminations, ['i-expired'])
        self.assertEqual(caught.exception.receipt['export_error'], 'AccessDenied')
        self.assertEqual(self.uploads[0]['ExpectedBucketOwner'], ACCOUNT)
        self.assertEqual(self.uploads[0]['ServerSideEncryption'], 'AES256')
        self.assertTrue(self.uploads[0]['Key'].startswith('ci/watchdog/'))
        self.assertNotIn('must-not-log-sensitive-message', logger.call_args.args[1])

    def test_periodic_handler_surfaces_incomplete_terminal_state(self):
        ec2 = EC2(instances=[instance('i-stuck')])
        ec2.stuck.add('i-stuck')
        with patch.dict(os.environ, environment(), clear=True), patch.dict(sys.modules, self.modules(ec2)), \
                patch.object(watchdog, '_utc', return_value=NOW), patch.object(watchdog.LOG, 'log'):
            with self.assertRaises(watchdog.IncompleteCleanup) as caught:
                watchdog.lambda_handler({}, self.context())
        self.assertFalse(caught.exception.receipt['complete'])

    def test_unknown_deployed_version_fails_before_any_compute_client(self):
        ec2 = EC2(instances=[instance('i-expired')])
        env = environment() | {'AWS_BENCH_WATCHDOG_VERSION': 'old'}
        with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, self.modules(ec2)):
            with self.assertRaises(ValueError):
                watchdog.lambda_handler({}, self.context())
        self.assertEqual(self.created, [])


if __name__ == '__main__':
    unittest.main()
