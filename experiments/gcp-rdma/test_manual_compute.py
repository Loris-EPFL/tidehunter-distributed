import asyncio
import contextlib
import copy
import datetime as dt
import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import gcp_manual_compute
import mailbox_exec
from gcp_campaign import GUEST_SERVICE_ACCOUNT
from gcp_manual_compute import Compute, ComputeError


class Store:
    """One bucket: enough of ObjectStore for the release path."""

    def __init__(self):
        self.objects = {}
        self.next_generation = 1

    def put(self, key, data, *, if_generation_match=None, **kwargs):
        if if_generation_match == 0 and key in self.objects:
            raise ValueError('generation mismatch')
        generation = str(self.next_generation)
        self.next_generation += 1
        self.objects[key] = (generation, bytes(data))
        return {'generation': generation, 'size': len(data),
                'metadata': {'sha256': hashlib.sha256(bytes(data)).hexdigest()}}

    def head(self, key):
        if key not in self.objects:
            raise KeyError(key)
        generation, data = self.objects[key]
        return {'name': key, 'generation': generation, 'size': len(data),
                'metadata': {'sha256': hashlib.sha256(data).hexdigest()}}

    def get(self, key, *, generation=None, max_bytes=None):
        return self.objects[key][1]

    def upload(self, key, path, **kwargs):
        return self.put(key, Path(path).read_bytes(), **kwargs)


def flag(argv, name):
    return next(arg[len(name) + 1:] for arg in argv if arg.startswith(name + '='))


def dictionary(value):
    if value.startswith('^'):
        _, delimiter, value = value.split('^', 2)
    else:
        delimiter = ','
    return dict(item.split('=', 1) for item in value.split(delimiter))


class FakeMcp:
    def __init__(self, state, config, path):
        self.state, self.config, self.path = state, config, path
        self.resources = {'policy': {}, 'instance': {}, 'disk': {}}
        self.calls = []
        self.mutations = []
        self.denied_guest = False
        self.lost = None
        self.suppress = None
        self.operation_polls_before_done = 0
        self.instance_lists_before_visible = 0
        self.operation_name = None
        self.operation_target = None
        self.blank_deletes = False

    async def __call__(self, argv):
        self.calls.append(argv)
        if self.denied_guest and any(x.startswith('--impersonate-service-account=') for x in argv):
            return {'text': 'ERROR: permission denied'}
        if argv[:3] == ['compute', 'operations', 'describe']:
            assert argv[3] == self.operation_name
            status = 'PENDING' if self.operation_polls_before_done else 'DONE'
            self.operation_polls_before_done = max(0, self.operation_polls_before_done - 1)
            return {'text': json.dumps({'name': self.operation_name, 'status': status,
                'zone': 'zones/us-central1-a', 'targetLink': self.operation_target, 'targetId': '200'})}
        if argv[:3] == ['beta', 'compute', 'resource-policies']:
            kind, verb, pos = 'policy', argv[3], 4
            if verb == 'create':
                pos += 1
        elif argv[:3] in (['compute', 'instances', 'list'], ['compute', 'instances', 'describe'],
                          ['compute', 'instances', 'delete'], ['compute', 'instances', 'create']):
            kind, verb, pos = 'instance', argv[2], 3
        elif argv[:3] in (['compute', 'disks', 'list'], ['compute', 'disks', 'describe'],
                          ['compute', 'disks', 'delete']):
            kind, verb, pos = 'disk', argv[2], 3
        else:
            raise AssertionError(argv)
        if verb == 'list':
            name = next(x.split('=', 2)[2] for x in argv if x.startswith('--filter=name='))
            if kind == 'instance' and self.operation_name and self.instance_lists_before_visible:
                self.instance_lists_before_visible -= 1
                return {'text': '[]'}
            obj = self.resources[kind].get(name)
            return {'text': json.dumps([obj] if obj else [])}
        name = argv[pos]
        if verb == 'describe':
            obj = self.resources[kind].get(name)
            return {'text': json.dumps(obj)} if obj else {'text': 'ERROR: not found'}
        persisted = json.loads(Path(self.path).read_text())
        if verb == 'create':
            rec = persisted['policy'] if kind == 'policy' else persisted['instances'][-1]
            assert rec['name'] == name and rec['uncertain'] is True
            self.mutations.append((kind, verb, name))
            if self.suppress != kind:
                if kind == 'policy':
                    self.resources[kind][name] = {'name': name, 'id': '100', 'region': 'regions/' + flag(argv, '--region'),
                        'description': flag(argv, '--description'),
                        'groupPlacementPolicy': {'collocation': flag(argv, '--collocation').upper(),
                                                 'maxDistance': int(flag(argv, '--max-distance'))}}
                else:
                    disk = dictionary(flag(argv, '--create-disk'))
                    metadata = dictionary(flag(argv, '--metadata'))
                    nics = []
                    for arg in argv:
                        if arg.startswith('--network-interface='):
                            parts = arg.split('=', 1)[1].split(',')
                            nic = dict(part.split('=', 1) for part in parts if '=' in part)
                            nics.append({'subnetwork': nic['subnet'], 'nicType': nic['nic-type'],
                                         'accessConfigs': [] if 'no-address' in parts else [{'natIP': '203.0.113.1'}]})
                    nics[0]['networkIP'] = '10.0.0.2'
                    nics[1]['networkIP'] = '10.1.0.2'
                    source = 'projects/mysten-research/zones/us-central1-a/disks/' + disk['name']
                    vm = {'name': name, 'id': '200', 'zone': 'zones/' + flag(argv, '--zone'), 'status': 'RUNNING',
                          'machineType': 'zones/us-central1-a/machineTypes/' + flag(argv, '--machine-type'),
                          'metadata': {'items': [{'key': k, 'value': v} for k, v in metadata.items()]},
                          'labels': dictionary(flag(argv, '--labels')),
                          'networkInterfaces': nics,
                          'resourcePolicies': ['projects/mysten-research/regions/us-central1/resourcePolicies/' + flag(argv, '--resource-policies')],
                          'serviceAccounts': [{'email': flag(argv, '--service-account'),
                                               'scopes': ['https://www.googleapis.com/auth/' + flag(argv, '--scopes')]}],
                          'scheduling': {'onHostMaintenance': flag(argv, '--maintenance-policy'),
                                         'automaticRestart': '--no-restart-on-failure' not in argv,
                                         'terminationTime': flag(argv, '--termination-time'),
                                         'instanceTerminationAction': flag(argv, '--instance-termination-action')},
                          'disks': [{'boot': disk['boot'] == 'yes', 'autoDelete': disk['auto-delete'] == 'yes',
                                     'interface': disk['interface'], 'mode': 'READ_WRITE', 'source': source}]}
                    self.resources[kind][name] = vm
                    self.resources['disk'][name] = {'name': disk['name'], 'id': '300', 'zone': 'zones/us-central1-a',
                        'description': disk['description'], 'sourceImage': disk['image'],
                        'type': 'zones/us-central1-a/diskTypes/' + disk['type'],
                        'sizeGb': disk['size'].removesuffix('GB'), 'users': [name]}
                    self.operation_name = 'operation-123'
                    self.operation_target = 'projects/mysten-research/zones/us-central1-a/instances/' + name
            if self.lost == kind:
                raise TimeoutError('connection lost')
            return {'text': json.dumps({'name': self.operation_name}) if kind == 'instance' else '{}'}
        rec = persisted['policy'] if kind == 'policy' else next(r for r in persisted['instances'] if r['name'] == name)
        assert rec['delete_requested' if kind != 'disk' else 'disk_delete_requested'] is True
        self.mutations.append((kind, verb, name))
        self.resources[kind].pop(name, None)
        if kind == 'instance':
            self.resources['disk'].pop(name, None)
        return {'text': '' if self.blank_deletes else '{}'}


class ManualComputeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        sleeper = mock.patch('gcp_manual_compute.asyncio.sleep', new_callable=mock.AsyncMock)
        sleeper.start()
        self.addCleanup(sleeper.stop)
        self.path = Path(self.temp.name) / 'state.json'
        self.state = {'run_id': 'bench-0123456789abcdef', 'source_sha256': 'a' * 64, 'owner_nonce': 'b' * 32,
                      'deadline': (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).isoformat(), 'instances': []}
        self.config = {'project_id': 'mysten-research', 'service_account': GUEST_SERVICE_ACCOUNT,
                       'machine_type': 'z4m-highmem-16', 'max_distance': 3,
                       'image': 'projects/test/global/images/image', 'candidates': [{
                           'zone': 'us-central1-a',
                           'ordinary_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/default',
                           'rdma_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/rdma'}]}
        self.api = FakeMcp(self.state, self.config, self.path)
        self.compute = Compute(self.config, self.state, self.path, self.api)

    async def make_vm(self):
        await self.compute.create_policy()
        return await self.compute.create_vm(0, self.config['image'], self.state['deadline'], 'startup')

    async def _assert_failed_intent_is_restored(self, kind, previous):
        await self.make_vm()
        if kind == 'policy':
            rec, key = self.state['policy'], 'delete_requested'
            self.state['instances'][0]['deleted'] = True
            operation = self.compute._cleanup_policy
        else:
            rec = self.state['instances'][0]
            key = 'disk_delete_requested' if kind == 'disk' else 'delete_requested'
            operation = self.compute._cleanup_disk if kind == 'disk' else self.compute._cleanup_instance
            if kind == 'disk':
                self.api.resources['disk'][rec['name']]['users'] = []
        if previous == 'absent':
            rec.pop(key, None)
        else:
            rec[key] = previous
        original_save = self.compute._save
        failures = [1]
        def save():
            if rec.get(key) is True and failures[0]:
                failures[0] -= 1
                raise RuntimeError('state save failed')
            original_save()
        self.api.mutations.clear()
        with mock.patch.object(self.compute, '_save', side_effect=save):
            with self.assertRaisesRegex(RuntimeError, 'state save failed'):
                await operation(rec)
            if previous == 'absent':
                self.assertNotIn(key, rec)
            else:
                self.assertIs(rec[key], previous)
            self.assertFalse(any(item[1] == 'delete' for item in self.api.mutations))
            await operation(rec)
        self.assertEqual([item[:2] for item in self.api.mutations], [(kind, 'delete')])

    async def test_instance_failed_intent_is_restored(self):
        await self._assert_failed_intent_is_restored('instance', 'absent')

    async def test_disk_failed_intent_is_restored(self):
        await self._assert_failed_intent_is_restored('disk', False)

    async def test_policy_failed_intent_is_restored(self):
        await self._assert_failed_intent_is_restored('policy', 'absent')

    async def test_guest_disk_read_timeout_retries_identical_observation(self):
        fleet = await self.make_vm()
        original = self.api.__call__
        calls = []
        async def execute(argv):
            if argv[:3] == ['compute', 'disks', 'describe'] and any(
                    a.startswith('--impersonate-service-account=') for a in argv):
                calls.append(list(argv))
                if len(calls) == 1:
                    raise subprocess.TimeoutExpired(argv, 30)
            return await original(argv)
        self.compute.execute = execute
        self.assertEqual(await self.compute.admit_guests(), [fleet])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])

    async def test_read_timeouts_are_bounded_by_attempts_and_deadline(self):
        calls = []
        async def timeout(argv):
            calls.append(list(argv))
            raise TimeoutError('read timed out')
        self.compute.execute = timeout
        with self.assertRaisesRegex(ComputeError, 'timed out'):
            await self.compute._exists('disk', self.state['run_id'] + '-0')
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(call == calls[0] for call in calls))
        calls.clear()
        async def expire(argv):
            calls.append(list(argv))
            self.state['deadline'] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
            raise TimeoutError('read timed out')
        self.compute.execute = expire
        with self.assertRaisesRegex(ComputeError, 'deadline exhausted'):
            await self.compute._exists('disk', self.state['run_id'] + '-0')
        self.assertEqual(len(calls), 1)

    async def test_invalid_and_denied_observations_are_not_retried(self):
        name = self.state['run_id'] + '-0'
        for response in ({'text': 'ERROR: permission denied'},
                         {'text': json.dumps({'name': 'foreign', 'id': '123'})},
                         {'text': 'not json'}):
            calls = []
            async def execute(argv):
                calls.append(argv)
                return response
            self.compute.execute = execute
            with self.assertRaises(ComputeError):
                await self.compute._describe('disk', name, guest=True)
            self.assertEqual(len(calls), 1)

    async def test_read_transport_crash_is_retried_but_mutation_is_not(self):
        name = self.state['run_id'] + '-0'
        crash = {'exit_code': 1, 'stdout': '', 'stderr': "ERROR: gcloud crashed (ConnectionError): ('Connection aborted.', "
                                                          "RemoteDisconnected('Remote end closed connection without response'))"}
        calls = []
        async def flaky(argv):
            calls.append(list(argv))
            return crash if len(calls) == 1 else {'text': '[]'}
        self.compute.execute = flaky
        self.assertFalse(await self.compute._exists('disk', name))
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        calls.clear()
        async def broken(argv):
            calls.append(list(argv))
            return crash
        self.compute.execute = broken
        with self.assertRaisesRegex(ComputeError, 'read transport failed'):
            await self.compute._exists('disk', name)
        self.assertEqual(len(calls), 3)
        calls.clear()
        with self.assertRaises(ComputeError):
            await self.compute._call(['compute', 'disks', 'delete', name, '--zone=us-central1-a'])
        self.assertEqual(len(calls), 1)

    async def test_guest_disk_source_mismatch_is_not_retried(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        self.api.resources['disk'][name]['sourceImage'] = 'projects/foreign/global/images/wrong'
        original = self.api.__call__
        calls = []
        async def execute(argv):
            if argv[:3] == ['compute', 'disks', 'describe'] and any(
                    a.startswith('--impersonate-service-account=') for a in argv):
                calls.append(list(argv))
            return await original(argv)
        self.compute.execute = execute
        with self.assertRaises(ComputeError):
            await self.compute.admit_guests()
        self.assertEqual(len(calls), 1)
        self.assertFalse(self.state['guest_admitted'])

    async def test_create_guest_admission_cleanup_and_persistence(self):
        fleet = await self.make_vm()
        self.assertEqual(fleet['id'], '200')
        self.assertEqual(fleet['rdma_ip'], '10.1.0.2')
        self.assertEqual((await self.compute.admit_guests())[0], fleet)
        self.api.blank_deletes = True
        result = await self.compute.cleanup()
        self.assertIn('TOCTOU', result['toctou'])
        self.assertEqual(self.state['cleanup_errors'], [])
        self.assertTrue(self.state['instances'][0]['disk_deleted'])
        self.assertTrue(self.state['policy']['deleted'])

    async def test_compact_owned_creation_and_cleanup_boundaries(self):
        self.state['host_count'] = 2
        self.compute = Compute(self.config, self.state, self.path, self.api)
        await self.make_vm()
        await self.compute.create_vm(1, self.config['image'], self.state['deadline'], 'startup')
        with self.assertRaises(ValueError):
            await self.compute.create_vm(2, self.config['image'], self.state['deadline'], 'startup')
        foreign = {'name': self.state['run_id'] + '-2', 'zone': 'us-central1-a'}
        with self.assertRaises(ComputeError):
            await self.compute._cleanup_instance(foreign)
        with self.assertRaises(ComputeError):
            await self.compute._cleanup_disk(foreign)
        await self.compute.cleanup()
        names = {self.state['run_id'] + '-0', self.state['run_id'] + '-1'}
        self.assertEqual({name for kind, action, name in self.api.mutations
                          if kind == 'instance' and action == 'create'}, names)
        self.assertEqual({name for kind, action, name in self.api.mutations
                          if kind == 'instance' and action == 'delete'}, names)

    async def test_cleanup_deletes_already_powered_off_guest(self):
        await self.make_vm()
        rec = self.state['instances'][0]
        self.api.resources['instance'][rec['name']]['status'] = 'TERMINATED'
        await self.compute.cleanup()
        self.assertNotIn(rec['name'], self.api.resources['instance'])
        self.assertTrue(rec['deleted'])
        self.assertTrue(rec['disk_deleted'])

    async def test_mcp_delete_notice_requires_and_accepts_verified_absence(self):
        await self.make_vm()
        original = self.api.__call__

        async def with_notice(argv):
            result = await original(argv)
            if 'delete' in argv:
                return {'text': '[]\n\nSTDERR:\nDeleted [owned resource].\n',
                        'details': {'provider': 'native'}}
            return result

        self.compute.execute = with_notice
        await self.compute.cleanup()
        self.assertEqual(self.api.resources, {'policy': {}, 'instance': {}, 'disk': {}})
        self.assertTrue(self.state['instances'][0]['deleted'])
        self.assertTrue(self.state['policy']['deleted'])
        self.assertEqual(len(self.state['delete_reply_errors']), 2)
        persisted = json.loads(self.path.read_text())
        self.assertEqual(persisted['delete_reply_errors'], self.state['delete_reply_errors'])
        self.assertEqual(sum(m[1] == 'delete' for m in self.api.mutations), 2)

    async def test_malformed_delete_reply_cannot_replace_absence_or_replay(self):
        await self.make_vm()
        original = self.api.__call__
        deletes = 0

        async def unknown(argv):
            nonlocal deletes
            if argv[:3] == ['compute', 'instances', 'delete']:
                deletes += 1
                return {'text': 'unconfirmed outcome', 'details': {'provider': 'native'}}
            if deletes and argv[:3] == ['compute', 'instances', 'list']:
                return {'text': 'ERROR: observation unavailable'}
            return await original(argv)

        self.compute.execute = unknown
        rec = self.state['instances'][0]
        for _ in range(2):
            with self.assertRaises(ComputeError):
                await self.compute._cleanup_instance(rec)
            self.assertFalse(rec.get('deleted', False))
        self.assertTrue(rec['delete_requested'])
        self.assertIn(rec['name'], self.api.resources['instance'])
        self.assertEqual(deletes, 1)

    async def test_lost_delete_reply_waits_for_provider_absence(self):
        await self.make_vm()
        old_vm = copy.deepcopy(next(iter(self.api.resources['instance'].values())))
        original = self.api.__call__
        deleting, pending_views = False, 2

        async def delayed(argv):
            nonlocal deleting, pending_views
            if argv[:3] == ['compute', 'instances', 'delete']:
                await original(argv)
                deleting = True
                raise TimeoutError('delete reply lost after submission')
            if deleting and pending_views and argv[:3] == ['compute', 'instances', 'list']:
                pending_views -= 1
                return {'text': json.dumps([old_vm])}
            return await original(argv)

        self.compute.execute = delayed
        with mock.patch('gcp_manual_compute.asyncio.sleep', new=mock.AsyncMock()):
            await self.compute.cleanup()
        self.assertEqual(self.state['cleanup_errors'], [])
        self.assertEqual(self.api.resources, {'policy': {}, 'instance': {}, 'disk': {}})
        self.assertEqual(sum(m[0:2] == ('instance', 'delete') for m in self.api.mutations), 1)

    async def test_slow_delete_does_not_starve_other_owned_instances(self):
        await self.make_vm()
        await self.compute.create_vm(1, self.config['image'], self.state['deadline'], 'startup')
        entered, release, fast_deleted = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = self.api.__call__
        slow = self.state['run_id'] + '-1'
        fast = self.state['run_id'] + '-0'

        async def delayed(argv):
            if argv[:3] == ['compute', 'instances', 'delete'] and argv[3] == slow:
                entered.set()
                await release.wait()
            result = await original(argv)
            if argv[:3] == ['compute', 'instances', 'delete'] and argv[3] == fast:
                fast_deleted.set()
            return result

        self.compute.execute = delayed
        task = asyncio.create_task(self.compute.cleanup())
        await entered.wait()
        await asyncio.wait_for(fast_deleted.wait(), timeout=2)
        self.assertIn(('instance', 'delete', fast), self.api.mutations)
        self.assertFalse(self.state['instances'][1].get('deleted', False))
        release.set()
        await task
        self.assertEqual(self.api.resources, {'policy': {}, 'instance': {}, 'disk': {}})
    async def test_unknown_delete_observation_fails_closed_without_replay(self):
        await self.make_vm()
        original = self.api.__call__
        deletes = 0
        async def execute(argv):
            nonlocal deletes
            if argv[:3] == ['compute', 'instances', 'delete']:
                deletes += 1
                raise TimeoutError('unknown mutation outcome')
            if deletes and argv[:3] == ['compute', 'instances', 'list']:
                raise TimeoutError('unknown observation outcome')
            return await original(argv)
        self.compute.execute = execute
        rec = self.state['instances'][0]
        for _ in range(2):
            with self.assertRaises(ComputeError):
                await self.compute._cleanup_instance(rec)
            self.assertFalse(rec.get('deleted', False))
        self.assertEqual(deletes, 1)


    async def test_lost_delete_reply_never_marks_present_instance_absent(self):
        await self.make_vm()
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.api.__call__
        lost = False

        async def delayed(argv):
            nonlocal lost
            if argv[:3] == ['compute', 'instances', 'delete']:
                lost = True
                raise TimeoutError('reply lost')
            if lost and argv[:3] == ['compute', 'instances', 'list']:
                entered.set()
                await release.wait()
            return await original(argv)

        self.compute.execute = delayed
        task = asyncio.create_task(self.compute.cleanup())
        await entered.wait()
        self.assertTrue(self.state['instances'][0]['delete_requested'])
        self.assertFalse(self.state['instances'][0].get('deleted', False))
        self.assertIn(self.state['instances'][0]['name'], self.api.resources['instance'])
        task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_guest_permission_failure_never_admits(self):
        await self.make_vm()
        self.api.denied_guest = True
        with self.assertRaises(ComputeError):
            await self.compute.admit_guests()
        self.assertFalse(self.state['guest_admitted'])

    async def test_guest_rejects_each_launch_spec_mismatch(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        vm = self.api.resources['instance'][name]
        disk = self.api.resources['disk'][name]
        changes = [
            (vm['scheduling'], 'onHostMaintenance', 'MIGRATE'),
            (vm['scheduling'], 'automaticRestart', True),
            (vm['serviceAccounts'][0], 'scopes', []),
            (next(item for item in vm['metadata']['items'] if item['key'] == 'startup-script'), 'value', 'changed'),
            (next(item for item in vm['metadata']['items'] if item['key'] == 'tidehunter-run-id'), 'value', 'other'),
            (vm['disks'][0], 'interface', 'SCSI'),
            (vm['disks'][0], 'mode', 'READ_ONLY'),
            (disk, 'sourceImage', 'projects/test/global/images/other'),
            (disk, 'type', 'zones/us-central1-a/diskTypes/pd-standard'),
            (disk, 'sizeGb', '81'),
        ]
        for obj, key, wrong in changes:
            with self.subTest(key=key):
                original = copy.deepcopy(obj[key])
                obj[key] = wrong
                try:
                    with self.assertRaises(ComputeError):
                        await self.compute.admit_guests()
                    self.assertFalse(self.state['guest_admitted'])
                finally:
                    obj[key] = original

    async def test_orchestrator_rejects_wrong_boot_image(self):
        await self.compute.create_policy()
        original = self.api.__call__
        async def wrong_image(argv):
            result = await original(argv)
            if argv[:3] == ['compute', 'instances', 'create']:
                self.api.resources['disk'][argv[3]]['sourceImage'] = 'projects/test/global/images/other'
            return result
        self.compute.execute = wrong_image
        with self.assertRaisesRegex(ComputeError, 'launch specification'):
            await self.compute.create_vm(0, self.config['image'], self.state['deadline'], 'startup')

    async def test_stale_instance_and_disk_ids_refuse_deletion(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        self.api.resources['instance'][name]['id'] = '201'
        self.api.resources['disk'][name]['id'] = '301'
        self.api.resources['disk'][name]['users'] = []
        with self.assertRaisesRegex(ComputeError, 'cleanup incomplete'):
            await self.compute.cleanup()
        self.assertFalse(any(m[1] == 'delete' and m[0] in ('instance', 'disk') for m in self.api.mutations))
        self.assertEqual(len(self.state['cleanup_errors']), 3)

    async def test_foreign_metadata_and_disk_description_refuse_deletion(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        self.api.resources['instance'][name]['labels']['bench-run'] = 'foreign'
        self.api.resources['disk'][name]['description'] = 'foreign'
        self.api.resources['disk'][name]['users'] = []
        with self.assertRaises(ComputeError):
            await self.compute.cleanup()
        self.assertFalse(any(m[1] == 'delete' and m[0] in ('instance', 'disk') for m in self.api.mutations))

    async def test_lost_create_reconciles_owned_resource_without_retry(self):
        self.api.lost = 'instance'
        fleet = await self.make_vm()
        self.assertEqual(fleet['id'], '200')
        self.assertEqual(sum(m[:2] == ('instance', 'create') for m in self.api.mutations), 1)
        self.assertFalse(self.state['instances'][0]['uncertain'])

    async def test_async_operation_and_visibility_are_waited_for(self):
        self.api.operation_polls_before_done = 2
        self.api.instance_lists_before_visible = 2
        fleet = await self.make_vm()
        self.assertEqual(fleet['id'], '200')
        self.assertEqual(self.state['instances'][0]['operation_name'], 'operation-123')
        self.assertTrue(self.state['instances'][0]['operation_done'])
        self.assertEqual(sum(args[:3] == ['compute', 'operations', 'describe'] for args in self.api.calls), 3)
        self.assertEqual(sum(m[:2] == ('instance', 'create') for m in self.api.mutations), 1)

    async def test_pending_operation_retained_during_cleanup(self):
        self.api.operation_polls_before_done = 10
        with mock.patch('gcp_manual_compute.time') as clock:
            clock.monotonic.side_effect = [0, 181]
            with self.assertRaises(ComputeError):
                await self.make_vm()
        rec = self.state['instances'][0]
        self.assertTrue(rec['uncertain'])
        self.assertEqual(rec['operation_name'], 'operation-123')
        self.assertFalse(rec.get('operation_done', False))
        persisted = json.loads(self.path.read_text())['instances'][0]
        self.assertEqual(persisted['operation_name'], rec['operation_name'])
        self.assertTrue(persisted['uncertain'])
        mutations = list(self.api.mutations)
        with mock.patch('gcp_manual_compute.time') as clock:
            clock.monotonic.side_effect = [0, 0, 0, 181] + [181] * 20
            with self.assertRaises(ComputeError):
                await self.compute.cleanup()
        self.assertFalse(any(m[1] == 'delete' for m in self.api.mutations))
        self.assertEqual(self.api.mutations, mutations)
        self.assertFalse(rec.get('deleted', False))
        self.assertFalse(rec.get('delete_requested', False))
        self.assertTrue(self.state['cleanup_errors'])

    async def test_absent_list_does_not_resolve_unknown_create(self):
        await self.compute.create_policy()
        self.api.lost = self.api.suppress = 'instance'
        with mock.patch('gcp_manual_compute.time') as clock:
            clock.monotonic.side_effect = [0, 181]
            with self.assertRaises(ComputeError):
                await self.compute.create_vm(0, self.config['image'], self.state['deadline'], 'startup')
        with self.assertRaisesRegex(ComputeError, 'unresolved'):
            await self.compute.cleanup()
        self.assertTrue(self.state['instances'][0]['uncertain'])
        self.assertEqual(sum(m[:2] == ('instance', 'create') for m in self.api.mutations), 1)

    async def test_timed_out_create_is_reconciled_without_another_mutation(self):
        original = self.api.__call__
        async def timeout_after_create(argv):
            result = await original(argv)
            if argv[:5] == ['beta', 'compute', 'resource-policies', 'create', 'group-placement']:
                await asyncio.Event().wait()
            return result
        self.compute.execute = timeout_after_create
        with mock.patch('gcp_manual_compute.MCP_CALL_SECONDS', 0.001):
            policy = await self.compute.create_policy()
        self.assertEqual(policy['id'], '100')
        self.assertFalse(policy['uncertain'])
        self.assertEqual(sum(m[:2] == ('policy', 'create') for m in self.api.mutations), 1)

    async def test_timed_out_cleanup_records_all_resource_errors(self):
        await self.make_vm()
        original = self.api.__call__
        async def blocked_lists(argv):
            if 'list' in argv:
                await asyncio.Event().wait()
            return await original(argv)
        self.compute.execute = blocked_lists
        with mock.patch('gcp_manual_compute.MCP_CALL_SECONDS', 0.001):
            with self.assertRaisesRegex(ComputeError, 'cleanup incomplete'):
                await self.compute.cleanup()
        self.assertGreaterEqual(len(self.state['cleanup_errors']), 2)
        self.assertIsNone(self.compute._cleanup_until)

    async def test_phase_timeout_clipping_and_expired_cleanup_window(self):
        self.state['deadline'] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=3)).isoformat()
        captured = []
        async def capture(awaitable, timeout):
            captured.append(timeout)
            return await awaitable
        with mock.patch('gcp_manual_compute.asyncio.wait_for', side_effect=capture):
            await self.compute._call(['compute', 'instances', 'list', '--filter=name=missing', '--zones=us-central1-a'])
        self.assertGreater(captured[0], 0)
        self.assertLessEqual(captured[0], 3)
        self.state['deadline'] = '2000-01-01T00:00:00Z'
        self.compute._cleanup_until = __import__('time').monotonic() + 10
        try:
            with mock.patch('gcp_manual_compute.asyncio.wait_for', side_effect=capture):
                await self.compute._call(['compute', 'instances', 'list', '--filter=name=missing', '--zones=us-central1-a'])
            self.assertGreater(captured[-1], 0)
            self.assertLessEqual(captured[-1], 10)
        finally:
            self.compute._cleanup_until = None

    async def test_delete_uses_longer_timeout_without_absence_retries(self):
        await self.make_vm()
        self.compute._save_delete_intent(self.state['instances'][0], 'delete_requested')
        captured = []
        async def elapsed_command(awaitable, timeout):
            captured.append(timeout)
            # Model a 60-second delete without sleeping in the test.
            if len(captured) == 1:
                self.assertGreater(timeout, 60)
            return await awaitable
        before = len(self.api.calls)
        with mock.patch('gcp_manual_compute.asyncio.wait_for', side_effect=elapsed_command):
            await self.compute._delete('instance', self.state['instances'][0]['name'])
        self.assertEqual(captured, [300, 30])
        self.assertEqual([argv[2] for argv in self.api.calls[before:]], ['delete', 'list'])
        self.assertNotIn('delete_reply_errors', self.state)

    async def test_delete_timeout_keeps_absence_verification(self):
        await self.make_vm()
        self.compute._save_delete_intent(self.state['instances'][0], 'delete_requested')
        name = self.state['instances'][0]['name']
        original = self.api.__call__
        async def delayed_delete(argv):
            result = await original(argv)
            if argv[:3] == ['compute', 'instances', 'delete']:
                await asyncio.Event().wait()
            return result
        self.compute.execute = delayed_delete
        with mock.patch('gcp_manual_compute.DELETE_CALL_SECONDS', 0.001):
            await self.compute._delete('instance', name)
        self.assertIn('timed out', self.state['delete_reply_errors']['instance:' + name])
        self.assertEqual(self.api.calls[-1][:3], ['compute', 'instances', 'list'])

    async def test_non_delete_call_keeps_thirty_second_bound(self):
        captured = []
        async def timed_out(awaitable, timeout):
            captured.append(timeout)
            awaitable.close()
            raise TimeoutError()
        with mock.patch('gcp_manual_compute.asyncio.wait_for', side_effect=timed_out):
            with self.assertRaisesRegex(ComputeError, 'timed out'):
                await self.compute._call(['compute', 'instances', 'describe', 'missing'])
        self.assertEqual(captured, [30])


    async def test_unknown_policy_is_not_assumed_absent(self):
        self.api.lost = self.api.suppress = 'policy'
        with self.assertRaises(ComputeError):
            await self.compute.create_policy()
        with self.assertRaisesRegex(ComputeError, 'unresolved'):
            await self.compute.cleanup()
        self.assertTrue(self.state['policy']['uncertain'])

    async def test_deadline_and_boot_disk_safety(self):
        await self.compute.create_policy()
        with self.assertRaises(ValueError):
            await self.compute.create_vm(0, self.config['image'], '2099-01-01T00:00:00Z', 'startup')
        self.state['deadline'] = '2000-01-01T00:00:00Z'
        with self.assertRaisesRegex(ComputeError, 'deadline'):
            await self.compute.create_vm(0, self.config['image'], self.state['deadline'], 'startup')
        self.assertFalse(any(m[0] == 'instance' for m in self.api.mutations))

    async def test_autodelete_and_expiry_are_required(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        self.api.resources['instance'][name]['disks'][0]['autoDelete'] = False
        with self.assertRaises(ComputeError):
            await self.compute.admit_guests()
        with self.assertRaises(ComputeError):
            await self.compute.cleanup()
        self.assertFalse(any(m[:2] == ('instance', 'delete') for m in self.api.mutations))

    async def test_policy_stale_id_refuses_delete(self):
        await self.compute.create_policy()
        self.api.resources['policy'][self.state['policy']['name']]['id'] = '101'
        with self.assertRaisesRegex(ComputeError, 'replaced'):
            await self.compute.cleanup()
        self.assertFalse(any(m[:2] == ('policy', 'delete') for m in self.api.mutations))

    async def test_structured_collision_never_adopts_or_deletes(self):
        original = self.api.__call__
        async def collision(argv):
            if argv[:5] == ['beta', 'compute', 'resource-policies', 'create', 'group-placement']:
                return {'status_code': 409}
            return await original(argv)
        self.compute.execute = collision
        with self.assertRaisesRegex(ComputeError, 'collision'):
            await self.compute.create_policy()
        with self.assertRaisesRegex(ComputeError, 'conflicting'):
            await self.compute.cleanup()
        self.assertTrue(self.state['policy']['conflict'])

    async def test_owner_nonce_and_expiry_are_enforced(self):
        await self.make_vm()
        name = self.state['instances'][0]['name']
        vm = self.api.resources['instance'][name]
        vm['scheduling']['instanceTerminationAction'] = 'STOP'
        with self.assertRaisesRegex(ComputeError, 'expiry'):
            await self.compute.admit_guests()
        vm['scheduling']['instanceTerminationAction'] = 'DELETE'
        next(x for x in vm['metadata']['items'] if x['key'] == 'bench-owner')['value'] = 'c' * 32
        with self.assertRaises(ComputeError):
            await self.compute.cleanup()
        self.assertFalse(any(m[:2] == ('instance', 'delete') for m in self.api.mutations))

    async def test_error_text_is_not_absence(self):
        await self.compute.create_policy()
        original = self.api.__call__
        async def bad_list(argv):
            if 'list' in argv:
                return {'text': 'ERROR: 404 not found (or permission denied)'}
            return await original(argv)
        self.compute.execute = bad_list
        with self.assertRaises(ComputeError):
            await self.compute.cleanup()
        self.assertFalse(any(m[1] == 'delete' for m in self.api.mutations))


CAMPAIGN = 'campaign-hybrid-sui-2026-11-25'
SOURCE = 'c' * 64
IMAGE = 'projects/mysten-research/global/images/bench-img-3012611b9ce24218-image'


class OperatorFleetTests(unittest.TestCase):
    """The two- and three-host operator CLI, with the reviewed request bodies."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_path = Path(self.temp.name) / 'fleet.json'
        self.config = {'version': 1, 'project_id': 'mysten-research', 'bucket': 'mysten-research-bench-artifacts',
                       'service_account': GUEST_SERVICE_ACCOUNT, 'machine_type': 'z4m-highmem-16',
                       'image': IMAGE, 'image_qualification': None, 'max_distance': 3,
                       'qualification_profile': 'focused', 'candidates': [{
                           'zone': 'us-central1-a',
                           'ordinary_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/default',
                           'rdma_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/falcon-rdma-us-central1-a'}]}
        self.config_path = Path(self.temp.name) / 'config.json'
        self.config_path.write_text(json.dumps(self.config))
        self.api = FakeMcp(None, self.config, self.state_path)

    def cli(self, action, *extra, state=None):
        return [action, *extra, '--config', str(self.config_path),
                '--state', str(self.state_path if state is None else state)]

    def create(self, *extra):
        return gcp_manual_compute.main(self.cli(
            'create', '--campaign', CAMPAIGN, '--hosts', '3', '--hours', '5',
            '--source-sha256', SOURCE, *extra))

    def output(self, argv):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            code = gcp_manual_compute.main(argv)
        self.assertEqual(code, 0)
        return json.loads(stream.getvalue())

    def live(self, state):
        api = FakeMcp(state, self.config, self.state_path)
        return api

    def test_dry_run_prints_the_three_bodies_and_the_policy(self):
        report = self.output(self.cli(
            'create', '--campaign', CAMPAIGN, '--hosts', '3', '--hours', '5',
            '--source-sha256', SOURCE, '--dry-run'))
        run_id = 'bench-' + __import__('hashlib').sha256(
            ('tidehunter-campaign:' + CAMPAIGN).encode()).hexdigest()[:16]
        self.assertEqual(report['run_id'], run_id)
        self.assertEqual(report['policy']['name'], run_id + '-placement')
        self.assertEqual(report['policy']['groupPlacementPolicy'],
                         {'collocation': 'COLLOCATED', 'maxDistance': 3})
        self.assertEqual([body['name'] for body in report['instances']],
                         [run_id + '-' + str(index) for index in range(3)])
        for index, body in enumerate(report['instances']):
            self.assertEqual(body['machineType'], 'zones/us-central1-a/machineTypes/z4m-highmem-16')
            self.assertEqual([nic['nicType'] for nic in body['networkInterfaces']], ['GVNIC', 'IRDMA'])
            self.assertEqual([nic['subnetwork'] for nic in body['networkInterfaces']],
                             [self.config['candidates'][0]['ordinary_subnet'],
                              self.config['candidates'][0]['rdma_subnet']])
            self.assertTrue(all('accessConfigs' not in nic for nic in body['networkInterfaces']))
            disk = body['disks'][0]
            self.assertEqual((disk['boot'], disk['autoDelete'], disk['interface']), (True, True, 'NVME'))
            self.assertEqual(disk['initializeParams']['diskSizeGb'], '80')
            self.assertEqual(disk['initializeParams']['diskType'], 'zones/us-central1-a/diskTypes/hyperdisk-balanced')
            self.assertEqual(disk['initializeParams']['sourceImage'], IMAGE)
            self.assertEqual(body['labels']['campaign'], CAMPAIGN)
            self.assertEqual(body['resourcePolicies'], ['regions/us-central1/resourcePolicies/' + run_id + '-placement'])
            self.assertEqual(body['scheduling']['instanceTerminationAction'], 'DELETE')
            self.assertEqual(body['scheduling']['automaticRestart'], False)
            self.assertNotEqual(body['scheduling']['terminationTime'], '')
        self.assertFalse(self.state_path.exists())

    def test_dry_run_refuses_a_two_host_fleet_size(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            gcp_manual_compute.main(self.cli(
                'create', '--campaign', CAMPAIGN, '--hosts', '4', '--hours', '5',
                '--source-sha256', SOURCE, '--dry-run'))

    def test_launch_command_carries_every_reviewed_body_field(self):
        state = gcp_manual_compute.new_state(
            self.config, campaign=CAMPAIGN, run_id=gcp_manual_compute.fleet_run_id(CAMPAIGN),
            deadline=gcp_manual_compute.fleet_deadline(5), hosts=3,
            source_sha256=SOURCE, owner_nonce='b' * 32)
        state['policy'] = {'name': state['run_id'] + '-placement'}
        for index in range(3):
            argv, body = gcp_manual_compute.launch_spec(
                self.config, self.config['candidates'][0], state, index, IMAGE,
                gcp_manual_compute.startup_script(self.config, state), 'd' * 32,
                state['policy']['name'])
            self.assertEqual(argv[3], body['name'])
            disk, params = body['disks'][0], body['disks'][0]['initializeParams']
            self.assertEqual(dictionary(flag(argv, '--create-disk')), {
                'name': params['diskName'], 'description': params['description'],
                'image': params['sourceImage'], 'size': params['diskSizeGb'] + 'GB',
                'type': 'hyperdisk-balanced', 'interface': 'NVME', 'boot': 'yes', 'auto-delete': 'yes'})
            self.assertEqual(dictionary(flag(argv, '--labels')), body['labels'])
            self.assertEqual(dictionary(flag(argv, '--metadata')),
                             {item['key']: item['value'] for item in body['metadata']['items']})
            self.assertEqual([arg for arg in argv if arg.startswith('--network-interface=')],
                             ['--network-interface=nic-type=GVNIC,subnet=' + body['networkInterfaces'][0]['subnetwork'] + ',no-address',
                              '--network-interface=nic-type=IRDMA,subnet=' + body['networkInterfaces'][1]['subnetwork'] + ',no-address'])
            self.assertIn('--termination-time=' + body['scheduling']['terminationTime'], argv)
            self.assertIn('--instance-termination-action=DELETE', argv)
            self.assertIn('--no-restart-on-failure', argv)
            self.assertNotIn('--maintenance-policy=STOP', argv)
            self.assertEqual(flag(argv, '--machine-type'), 'z4m-highmem-16')
            self.assertEqual(flag(argv, '--resource-policies'), state['policy']['name'])
        with self.assertRaises(ValueError):
            gcp_manual_compute.launch_spec(self.config, self.config['candidates'][0], state, 3, IMAGE,
                                           gcp_manual_compute.startup_script(self.config, state), 'd' * 32,
                state['policy']['name'])

    def test_three_host_create_verify_and_delete(self):
        calls = []
        holder = {}

        class Recorded(FakeMcp):
            async def __call__(self, argv):
                calls.append(argv)
                return await super().__call__(argv)

        async def transport(argv):
            if holder.get('api') is None:
                holder['api'] = Recorded(None, self.config, self.state_path)
            return await holder['api'](argv)

        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               return_value={'path': IMAGE, 'id': '1937849582956621267'}), \
             mock.patch.object(gcp_manual_compute, 'gcloud', new=transport):
            report = self.output(self.cli(
                'create', '--campaign', CAMPAIGN, '--hosts', '3', '--hours', '5',
                '--source-sha256', SOURCE))
        self.assertEqual(len(report['hosts']), 3)
        self.assertEqual([host['role'] for host in report['hosts']], ['coordinator', 'owner', 'owner'])
        state = json.loads(self.state_path.read_text())
        self.assertEqual(state['campaign'], CAMPAIGN)
        self.assertEqual(state['host_count'], 3)
        self.assertEqual(state['policy']['name'], state['run_id'] + '-placement')
        self.assertEqual(len(state['instances']), 3)
        self.assertEqual([verb for kind, verb, name in holder['api'].mutations if kind == 'instance'],
                         ['create', 'create', 'create'])
        for argv in calls:
            if argv[:3] == ['compute', 'instances', 'create']:
                self.assertEqual(dictionary(flag(argv, '--labels'))['campaign'], CAMPAIGN)
                self.assertIn('--termination-time=' + state['deadline'], argv)
                self.assertIn('--instance-termination-action=DELETE', argv)
        async def replay(argv):
            return await holder['api'](argv)
        with mock.patch.object(gcp_manual_compute, 'gcloud', new=replay):
            verified = self.output(self.cli('verify'))
            self.assertEqual(verified['placement']['collocation'], 'COLLOCATED')
            self.assertEqual(verified['placement']['max_distance'], 3)
            self.assertEqual(len(verified['hosts']), 3)
            self.assertEqual([host['name'] for host in verified['hosts']],
                             [state['run_id'] + '-' + str(index) for index in range(3)])
            deletion = self.output(self.cli('delete'))
        self.assertEqual(deletion['cleanup']['errors'], [])
        self.assertEqual(len(holder['api'].resources['instance']), 0)
        self.assertEqual(len(holder['api'].resources['disk']), 0)
        self.assertEqual(len(holder['api'].resources['policy']), 0)
        state = json.loads(self.state_path.read_text())
        self.assertTrue(all(item['deleted'] and item['disk_deleted'] for item in state['instances']))
        self.assertTrue(state['policy']['deleted'])

    def test_the_fleet_boots_the_identity_bound_gate_only(self):
        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               return_value={'path': IMAGE, 'id': '1'}), \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__):
            self.create()
        state = json.loads(self.state_path.read_text())
        gate = gcp_manual_compute.startup_script(self.config, state)
        self.assertIn('/qualification/', gate)
        self.assertIn('gate.json', gate)
        self.assertNotIn('echo "tidehunter campaign host', gate)
        self.assertNotIn('workers_released', state)
        for record in state['instances']:
            self.assertEqual(record['startup_sha256'], hashlib.sha256(gate.encode()).hexdigest())

    def test_create_with_a_package_publishes_and_releases_every_gate(self):
        package = Path(self.temp.name) / 'package'
        package.mkdir()
        spec = Path(self.temp.name) / 'spec.json'
        spec.write_text(json.dumps({'suite': 'sui', 'mode': 'sui', 'owners': 2,
                                    'backends': ['native']}))
        store = Store()
        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               return_value={'path': IMAGE, 'id': '1'}), \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__), \
             mock.patch.object(gcp_manual_compute.gcp_api, 'GoogleApi') as api, \
             mock.patch.object(mailbox_exec, 'publish') as publish, \
             mock.patch.object(mailbox_exec, 'release') as release:
            api.return_value.objects.return_value = store
            report = self.output(self.cli(
                'create', '--campaign', CAMPAIGN, '--hosts', '3', '--hours', '5',
                '--source-sha256', SOURCE, '--package', str(package), '--spec', str(spec),
                '--revision', 'b' * 40, '--guest-root', '/mnt/tidehunter/sui-run'))
        state = json.loads(self.state_path.read_text())
        self.assertEqual(report['workers_released'],
                         [state['run_id'] + '-' + str(index) for index in range(3)])
        self.assertEqual(state['workers_released'], report['workers_released'])
        publish.assert_called_once()
        published = publish.call_args
        self.assertEqual(published.args[2]['run_id'], state['run_id'])
        self.assertEqual(published.kwargs['guest_roots'], ['/mnt/tidehunter/sui-run'])
        self.assertEqual([call.args[2]['name'] for call in release.call_args_list],
                         report['workers_released'])
        for call in release.call_args_list:
            self.assertIn('TIDEHUNTER_BOOTSTRAP', call.args[3])
            self.assertIn(state['run_id'], call.args[3])
            self.assertIn('gcp_worker.py', call.args[3])

    def test_a_qualification_fleet_is_never_released_here(self):
        ready = {'status': 'READY', 'id': '1937849582956621267',
                 'selfLink': 'https://www.googleapis.com/compute/v1/' + IMAGE, 'diskSizeGb': '80'}
        package = Path(self.temp.name) / 'package'
        package.mkdir()
        spec = Path(self.temp.name) / 'spec.json'
        spec.write_text(json.dumps({'suite': 'sui', 'mode': 'sui', 'owners': 1,
                                    'backends': ['native']}))
        with mock.patch('gcp_api.GoogleApi') as api, \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__), \
             mock.patch.object(mailbox_exec, 'release') as release:
            api.return_value.request.return_value = ready
            report = self.output(self.cli(
                'create', '--campaign', CAMPAIGN + '-qualify', '--hosts', '2', '--hours', '1',
                '--role', 'qualification', '--image', IMAGE, '--source-sha256', SOURCE,
                '--package', str(package), '--spec', str(spec), '--revision', 'b' * 40))
        self.assertIsNone(report['workers_released'])
        release.assert_not_called()

    def test_ready_refuses_an_unreleased_fleet_and_waits_for_every_worker(self):
        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               return_value={'path': IMAGE, 'id': '1'}), \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__):
            self.create()
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(gcp_manual_compute.main(self.cli('ready')), 1)
        state = json.loads(self.state_path.read_text())
        state['workers_released'] = [record['name'] for record in state['instances']]
        self.state_path.write_text(json.dumps(state))
        receipt = {'complete': True, 'run_id': state['run_id'], 'source_sha256': SOURCE,
                   'deadline_utc': state['deadline'], 'manifest_sha256': 'e' * 64,
                   'storage': {'device': '/dev/nvme0n1'}}
        with mock.patch.object(gcp_manual_compute.gcp_api, 'GoogleApi'), \
             mock.patch.object(mailbox_exec.MailboxExec, 'from_state') as from_state:
            from_state.return_value.wait_workers.return_value = {
                record['fleet']['name']: dict(receipt, instance_name=record['fleet']['name'],
                                              instance_id=record['fleet']['id'])
                for record in state['instances']}
            report = self.output(self.cli('ready', '--require-manifest-sha256', 'e' * 64))
        from_state.return_value.wait_workers.assert_called_once_with(require='e' * 64)
        self.assertEqual(report['hosts'], sorted(record['fleet']['name'] for record in state['instances']))

    def test_package_flags_are_validated_before_any_call(self):
        for extra, message in (((['--package', '/tmp/p'],), '--spec'),
                               ((['--worker-seconds', '5'],), 'ready only')):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    gcp_manual_compute.parse(self.cli('create', '--campaign', CAMPAIGN,
                                                      '--hosts', '3', '--hours', '5',
                                                      '--source-sha256', SOURCE, *extra[0]))

    def test_recorded_fleet_refuses_a_different_campaign_or_size(self):
        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               return_value={'path': IMAGE, 'id': '1'}), \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__):
            self.output(self.cli('create', '--campaign', CAMPAIGN, '--hosts', '3', '--hours', '5',
                                 '--source-sha256', SOURCE))
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(self.create('--hosts', '2'), 1)
                self.assertEqual(gcp_manual_compute.main(self.cli('delete', '--campaign', 'campaign-other')), 1)
        self.assertEqual(len(json.loads(self.state_path.read_text())['instances']), 3)

    def test_qualification_role_admits_a_ready_image_only(self):
        ready = {'status': 'READY', 'id': '1937849582956621267',
                 'selfLink': 'https://www.googleapis.com/compute/v1/' + IMAGE, 'diskSizeGb': '80'}
        with mock.patch('gcp_api.GoogleApi') as api, \
             mock.patch.object(gcp_manual_compute, 'admit_image') as receipt_admission, \
             mock.patch.object(gcp_manual_compute, 'gcloud',
                               new=FakeMcp(None, self.config, self.state_path).__call__):
            api.return_value.request.return_value = ready
            report = self.output(self.cli(
                'create', '--campaign', CAMPAIGN + '-qualify',
                '--hosts', '2', '--hours', '1', '--role', 'qualification', '--image', IMAGE,
                '--source-sha256', SOURCE))
            receipt_admission.assert_not_called()
        self.assertEqual(report['role'], 'qualification')
        self.assertEqual(len(report['hosts']), 2)
        self.assertEqual(json.loads(self.state_path.read_text())['role'], 'qualification')
        with mock.patch('gcp_api.GoogleApi') as api:
            api.return_value.request.return_value = dict(ready, status='PENDING')
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(gcp_manual_compute.main(self.cli(
                    'create', '--campaign', CAMPAIGN + '-other',
                    '--hosts', '2', '--hours', '1', '--role', 'qualification', '--image', IMAGE,
                    '--source-sha256', SOURCE, state=Path(self.temp.name) / 'second.json')), 1)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            gcp_manual_compute.main(self.cli(
                'create', '--campaign', CAMPAIGN + '-third', '--hosts', '3', '--hours', '1',
                '--role', 'qualification', '--image', IMAGE, '--source-sha256', SOURCE,
                state=Path(self.temp.name) / 'third.json'))
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            gcp_manual_compute.main(self.cli(
                'create', '--campaign', CAMPAIGN + '-fourth', '--hosts', '2', '--hours', '1',
                '--role', 'qualification', '--source-sha256', SOURCE,
                state=Path(self.temp.name) / 'fourth.json'))

    def test_campaign_role_requires_the_receipt(self):
        with mock.patch.object(gcp_manual_compute, 'admit_image',
                               side_effect=ValueError('image qualification is not configured')), \
             contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.create(), 1)
        self.assertFalse(self.state_path.exists())

    def test_qualification_role_is_limited_to_two_hosts(self):
        with self.assertRaises(ValueError):
            gcp_manual_compute.new_state(
                self.config, campaign=CAMPAIGN, run_id=gcp_manual_compute.fleet_run_id(CAMPAIGN),
                deadline=gcp_manual_compute.fleet_deadline(1), hosts=3,
                source_sha256=SOURCE, owner_nonce='a' * 32, role='qualification')

    def test_fleet_identity_and_deadline_are_derived(self):
        run_id = gcp_manual_compute.fleet_run_id(CAMPAIGN)
        self.assertRegex(run_id, r'^bench-[0-9a-f]{16}$')
        self.assertEqual(run_id, gcp_manual_compute.fleet_run_id(CAMPAIGN))
        self.assertNotEqual(run_id, gcp_manual_compute.fleet_run_id('campaign-other'))
        with self.assertRaises(ValueError):
            gcp_manual_compute.fleet_run_id('Campaign Upper')
        with self.assertRaises(ValueError):
            gcp_manual_compute.fleet_run_id(None)
        now = dt.datetime(2026, 11, 26, 12, 0, 0, 500000, tzinfo=dt.timezone.utc)
        self.assertEqual(gcp_manual_compute.fleet_deadline(5, now=now), '2026-11-26T17:00:00Z')
        for hours in (0, 7):
            with self.assertRaises(ValueError):
                gcp_manual_compute.fleet_deadline(hours, now=now)
        state = gcp_manual_compute.new_state(
            self.config, campaign=CAMPAIGN, run_id=run_id,
            deadline=gcp_manual_compute.fleet_deadline(2, now=now), hosts=2,
            source_sha256=SOURCE, owner_nonce='a' * 32)
        self.assertEqual((state['host_count'], state['campaign'], state['instances']), (2, CAMPAIGN, []))
        with self.assertRaises(ValueError):
            gcp_manual_compute.new_state(self.config, campaign=CAMPAIGN, run_id=run_id,
                                         deadline=gcp_manual_compute.fleet_deadline(2, now=now), hosts=4,
                                         source_sha256=SOURCE, owner_nonce='a' * 32)

    def test_campaign_additions_never_replace_reviewed_keys(self):
        state = gcp_manual_compute.new_state(
            self.config, campaign=CAMPAIGN, run_id=gcp_manual_compute.fleet_run_id(CAMPAIGN),
            deadline=gcp_manual_compute.fleet_deadline(5), hosts=3,
            source_sha256=SOURCE, owner_nonce='b' * 32)
        state['policy'] = {'name': state['run_id'] + '-placement'}
        for extra in ({'extra_labels': {'bench-run': 'other'}},
                      {'extra_metadata': {'bench-expiry': 'other'}},
                      {'extra_labels': {'': 'empty'}}):
            with self.assertRaises(ValueError):
                gcp_manual_compute.instance_body(self.config, self.config['candidates'][0], state,
                                                 0, 'startup', 'd' * 32, **extra)
        body = gcp_manual_compute.instance_body(
            self.config, self.config['candidates'][0], state, 0, 'startup', 'd' * 32,
            extra_labels={'campaign': CAMPAIGN})
        self.assertEqual(body['labels']['campaign'], CAMPAIGN)
        self.assertEqual(body['labels']['bench-run'], state['run_id'])
