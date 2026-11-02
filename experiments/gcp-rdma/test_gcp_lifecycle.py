import datetime as dt
import hashlib
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import gcp_budget
from gcp_api import ApiError
from gcp_campaign import StateFile, save, cleanup, instance_body, mutation, owned_instance, has_delete_expiry, collect_results, coordinator_timeout


class Store:
    def __init__(self):
        self.data = {}
        self.generation = 0
    def head(self, key):
        if key not in self.data:
            raise ApiError(404, 'missing')
        value, generation = self.data[key]
        return {'generation': str(generation), 'size': str(len(value)), 'metadata': {'sha256': hashlib.sha256(value).hexdigest()}, 'name': key}
    def get(self, key, *, generation=None, max_bytes=999999):
        value, current = self.data[key]
        if generation is not None and str(generation) != str(current):
            raise ApiError(404, 'generation missing')
        return value
    def put(self, key, data, *, content_type=None, if_generation_match=None, metadata=None):
        old = self.data.get(key)
        if if_generation_match is not None and str(if_generation_match) != str(old[1] if old else 0):
            raise ApiError(412, 'CAS')
        self.generation += 1
        self.data[key] = (data, self.generation)
        return self.head(key)
    def upload(self, key, path, **kwargs):
        return self.put(key, Path(path).read_bytes(), **kwargs)
    def list(self, prefix):
        return [self.head(k) for k in self.data if k.startswith(prefix)]
    def download(self, key, path, *, generation=None, max_bytes=None):
        Path(path).write_bytes(self.get(key, generation=generation))
        return self.head(key)


class FakeApi:
    def __init__(self, objects=None):
        self.resources = objects or {}
        for path, vm in list(self.resources.items()):
            if '/instances/' in path:
                disk_path = path.replace('/instances/', '/disks/')
                self.resources[disk_path] = {'name': vm['name'], 'id': '90042', 'users': [path],
                    'description': vm['disks'][0]['initializeParams']['description']}
        self.calls = []
        self.lost = False
    def request(self, method, path, body=None, query=None):
        self.calls.append((method, path, query))
        if method == 'GET':
            if path not in self.resources:
                raise ApiError(404, 'missing')
            return self.resources[path]
        if method == 'DELETE':
            if '/instances/' in path:
                self.resources.pop(path.replace('/instances/', '/disks/'), None)
            self.resources.pop(path, None)
            return {'status': 'DONE'}
        if not self.lost:
            self.lost = True
            raise ApiError(None, 'response lost')
        return {'status': 'DONE', 'targetId': '42'}


class FleetApi:
    """Fake Compute operations complete only after a separate operation GET."""
    def __init__(self, failure=None):
        self.store = Store()
        self.resources = {}
        self.operations = {}
        self.calls = []
        self.next_id = 100
        self.failure = failure
        self.failed = False

    def objects(self, bucket):
        return self.store

    def request(self, method, path, body=None, query=None):
        self.calls.append((method, path))
        if method == 'GET':
            if path in self.operations:
                return self.operations[path]
            if path not in self.resources:
                raise ApiError(404, 'missing')
            return self.resources[path]
        if method == 'POST':
            scope = path.removeprefix('beta/').rsplit('/', 1)[0]
            target = path + '/' + body['name']
            self.next_id += 1
            operation = {'status': 'DONE', 'targetId': str(self.next_id)}
            if ('/instances' in path and body['name'].endswith('-2') and
                    'us-central1-a' in path and self.failure and not self.failed):
                self.failed = True
                if self.failure.startswith('SYNC:'):
                    _, status, reason = self.failure.split(':')
                    raise ApiError(int(status), json.dumps({'error': {'errors': [{'reason': reason}]}}))
                operation['error'] = {'errors': [{'code': self.failure}]}
            else:
                resource = json.loads(json.dumps(body))
                resource['id'] = str(self.next_id)
                if '/instances' in path:
                    zone = path.split('/')[1]
                    disk_path = f'zones/{zone}/disks/{body["name"]}'
                    resource['disks'][0]['source'] = 'projects/mysten-research/' + disk_path
                    self.resources[disk_path] = {'name': body['name'], 'id': str(90000 + self.next_id),
                        'description': body['disks'][0]['initializeParams']['description']}
                    for index, nic in enumerate(resource['networkInterfaces']):
                        nic['networkIP'] = f'10.{index}.{self.next_id // 255}.{self.next_id % 255}'
                self.resources[target] = resource
            op_name = 'op-' + str(self.next_id)
            self.operations[scope + '/operations/' + op_name] = operation
            return {'status': 'PENDING', 'name': op_name}
        if method == 'DELETE':
            if '/instances/' in path:
                self.resources.pop(path.replace('/instances/', '/disks/'), None)
            self.resources.pop(path, None)
            self.next_id += 1
            scope = path.removeprefix('beta/').rsplit('/', 2)[0]
            op_name = 'op-' + str(self.next_id)
            self.operations[scope + '/operations/' + op_name] = {'status': 'DONE'}
            return {'status': 'PENDING', 'name': op_name}
        raise AssertionError(method)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.state = {'run_id': 'bench-0123456789abcdef', 'source_sha256': 'a' * 64, 'deadline': '2099-01-01T00:00:00Z', 'instances': []}
        self.config = {'machine_type': 'z4m-highmem-16', 'image': 'projects/p/global/images/i', 'service_account': 'bench-guest@mysten-research.iam.gserviceaccount.com'}
        self.candidate = {'zone': 'us-central1-a', 'ordinary_subnet': 'o', 'rdma_subnet': 'r'}
        self.state['policy'] = {'name': self.state['run_id'] + '-placement', 'region': 'us-central1'}

    def vm(self):
        body = instance_body(self.config, self.candidate, self.state, 0, 'startup', 'a' * 32)
        body['id'] = '42'
        body['disks'][0]['source'] = 'projects/mysten-research/zones/us-central1-a/disks/' + body['name']
        return body

    def test_delete_expiry_and_identity_checked(self):
        vm = self.vm()
        rec = {'name': vm['name'], 'id': '42'}
        self.assertTrue(owned_instance(vm, self.state, rec))
        self.assertTrue(has_delete_expiry(vm, self.state['deadline']))
        vm['scheduling']['instanceTerminationAction'] = 'STOP'
        self.assertTrue(owned_instance(vm, self.state, rec))
        self.assertFalse(has_delete_expiry(vm, self.state['deadline']))
        vm['scheduling']['instanceTerminationAction'] = 'DELETE'
        vm['id'] = '43'
        self.assertFalse(owned_instance(vm, self.state, rec))

    def test_h4d_storage_and_expiry_payload_and_shape_admission(self):
        import gcp_campaign
        now = dt.datetime.now(dt.timezone.utc)
        deadline = (now + dt.timedelta(hours=1)).isoformat()
        config = {**self.config,
                  'version': 1, 'project_id': 'mysten-research', 'project_number': '1',
                  'bucket': 'bucket', 'max_distance': 3,
                  'candidates': [{**self.candidate, 'network_profile': 'falcon'}]}
        budget = {'weekly_limit_cents': 10000, 'run_upper_bound_cents': 9000,
                  'max_duration_seconds': 7200, 'quote_description': 'bounded quote'}
        gcp_campaign.validate(config, budget, self.state['run_id'], self.state['source_sha256'], deadline)
        h4d = {**config, 'machine_type': 'h4d-highmem-192-lssd'}
        with self.assertRaises(ValueError):
            gcp_campaign.validate(h4d, budget, self.state['run_id'], self.state['source_sha256'], deadline)
        body = instance_body(h4d, self.candidate, {**self.state, 'deadline': deadline}, 0, 'startup', 'a' * 32)
        self.assertTrue(body['machineType'].endswith('/h4d-highmem-192-lssd'))
        self.assertEqual([n['nicType'] for n in body['networkInterfaces']], ['GVNIC', 'IRDMA'])
        self.assertEqual(len(body['disks']), 1)
        self.assertEqual(body['disks'][0]['initializeParams']['diskType'],
                         'zones/us-central1-a/diskTypes/hyperdisk-balanced')
        self.assertTrue(has_delete_expiry(body, deadline))
        for index in range(6):
            vm = instance_body(config, self.candidate, {**self.state, 'deadline': deadline}, index, 'startup', 'a' * 32)
            self.assertEqual(vm['name'], f'bench-0123456789abcdef-{index}')
            self.assertEqual(vm['disks'][0]['initializeParams']['diskName'], vm['name'])
            self.assertEqual(vm['disks'][0]['initializeParams']['diskSizeGb'], '80')
            self.assertEqual(vm['serviceAccounts'][0]['email'], self.config['service_account'])
            self.assertTrue(vm['disks'][0]['autoDelete'])
        with self.assertRaises(ValueError):
            instance_body({**config, 'service_account': 'other@example.invalid'}, self.candidate, self.state, 0, 'startup', 'a' * 32)
        with self.assertRaises(ValueError):
            gcp_campaign.validate({**config, 'service_account': 'other@example.invalid'}, budget, self.state['run_id'], self.state['source_sha256'], deadline)
        with self.assertRaises(ValueError):
            gcp_campaign.validate(config, budget, 'th-0123456789abcdef', self.state['source_sha256'], deadline)
        with self.assertRaises(ValueError):
            gcp_campaign.validate(config, {**budget, 'max_duration_seconds': 6 * 3600}, self.state['run_id'], self.state['source_sha256'], deadline)
        with self.assertRaises(ValueError):
            gcp_campaign.validate(config, {**budget, 'max_duration_seconds': 4 * 3600}, self.state['run_id'], self.state['source_sha256'], (now + dt.timedelta(hours=5)).isoformat())
        with self.assertRaises(ValueError):
            gcp_campaign.validate(config, {**budget, 'max_duration_seconds': 14400, 'run_upper_bound_cents': 7889}, self.state['run_id'], self.state['source_sha256'], deadline)
        gcp_campaign.validate(config, {**budget, 'max_duration_seconds': 14400, 'run_upper_bound_cents': 7890}, self.state['run_id'], self.state['source_sha256'], deadline)
        with self.assertRaises(ValueError):
            gcp_campaign.validate({**config, 'candidates': [{**self.candidate, 'zone': 'europe-west4-a', 'network_profile': 'falcon'}]}, budget, self.state['run_id'], self.state['source_sha256'], deadline)
        for shape in ('h4d-standard-192', 'h4d-standard-192-lssd', 'h4d-highmem-192', 'unknown'):
            config['machine_type'] = shape
            with self.subTest(shape=shape):
                with self.assertRaises(ValueError):
                    gcp_campaign.validate(config, budget, self.state['run_id'], self.state['source_sha256'], deadline)
                with self.assertRaisesRegex(ValueError, 'local SSD'):
                    instance_body(config, self.candidate, self.state, 0, 'startup', 'a' * 32)
        config['machine_type'] = 'z4m-highmem-16'
        for distance in (True, 0, 4, None):
            config['max_distance'] = distance
            with self.assertRaisesRegex(ValueError, 'max_distance'):
                gcp_campaign.validate(config, budget, self.state['run_id'], self.state['source_sha256'], deadline)
        config['max_distance'] = 3
        with self.assertRaisesRegex(ValueError, 'deadline'):
            gcp_campaign.validate(config, budget, self.state['run_id'], self.state['source_sha256'],
                                  (now - dt.timedelta(seconds=1)).isoformat())

    def test_cleanup_removes_owned_vm_with_rejected_expiry(self):
        vm = self.vm()
        vm['scheduling']['instanceTerminationAction'] = 'STOP'
        rec = {'name': vm['name'], 'id': '42', 'zone': 'us-central1-a', 'disk_token': 'a' * 32}
        self.state['instances'] = [rec]; self.state.pop('policy')
        api = FakeApi({'zones/us-central1-a/instances/' + vm['name']: vm})
        with tempfile.TemporaryDirectory() as tmp:
            cleanup(api, self.state, Path(tmp) / 'state.json')
        self.assertTrue(rec['deleted'])

    def test_run_rejects_missing_cleanup_or_guest_permissions_before_insert(self):
        import gcp_campaign
        class PermissionApi:
            def __init__(self, missing, guest=False):
                self.missing = missing
                self.guest = guest
                self.mutations = []
            def project_permissions(self, requested):
                return [p for p in requested if p != self.missing or self.guest != (p == 'compute.instances.get')]
            def service_account_permissions(self, account, requested):
                return requested
            def bucket_permissions(self, bucket, requested):
                return [p for p in requested if not (self.guest and p == self.missing)]
            def impersonated(self, account):
                return PermissionApi(self.missing, guest=True)
            def request(self, method, path, *args, **kwargs):
                self.mutations.append((method, path))
                raise AssertionError('preflight must fail before compute requests')

        for missing in ('compute.instances.delete', 'storage.objects.create'):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as tmp:
                api = PermissionApi(missing)
                now = dt.datetime.now(dt.timezone.utc)
                deadline = (now + dt.timedelta(hours=1)).isoformat()
                config = {**self.config, 'version': 1, 'project_id': 'mysten-research',
                          'project_number': '1', 'bucket': 'bucket', 'max_distance': 3,
                          'candidates': [{**self.candidate, 'network_profile': 'falcon'}]}
                budget = {'weekly_limit_cents': 10000, 'run_upper_bound_cents': 9000,
                          'max_duration_seconds': 7200, 'quote_description': 'bounded quote'}
                receipt = {'source_sha256': self.state['source_sha256']}
                for name, value in (('config', config), ('budget', budget), ('receipt', receipt), ('spec', {})):
                    Path(tmp, name + '.json').write_text(json.dumps(value))
                args = ['run', '--config', tmp + '/config.json', '--budget', tmp + '/budget.json',
                        '--run-id', self.state['run_id'], '--source-sha', self.state['source_sha256'],
                        '--package-receipt', tmp + '/receipt.json', '--spec', tmp + '/spec.json',
                        '--deadline', deadline, '--state', tmp + '/state.json', '--output-dir', tmp, '--activate']
                with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=api):
                    with self.assertRaisesRegex(ValueError, 'missing permissions'):
                        gcp_campaign.main(args)
                self.assertEqual(api.mutations, [])
                self.assertFalse(Path(tmp, 'state.json').exists())

    def test_preflight_rejects_subnet_without_private_google_access(self):
        import gcp_campaign
        class ImageApi:
            def request(self, method, path, body=None):
                if method == 'POST':
                    return {'permissions': ['compute.images.useReadOnly']}
                return {'status': 'READY', 'diskSizeGb': '80'}
        class Operator:
            token_provider = staticmethod(lambda: 'token')
            opener = None
            def __init__(self):
                self.calls = []
            def request(self, method, path, body=None):
                self.calls.append((method, path))
                if '/subnetworks/' in path:
                    return {'privateIpGoogleAccess': False}
                return {}
        api = Operator()
        subnet = 'projects/mysten-research/regions/us-central1/subnetworks/'
        config = {'image': 'projects/image-project/global/images/pinned',
                  'project_id': 'mysten-research', 'machine_type': 'z4m-highmem-16',
                  'candidates': [{'zone': 'us-central1-a', 'ordinary_subnet': subnet + 'ordinary',
                                  'rdma_subnet': subnet + 'rdma', 'network_profile': 'falcon'}]}
        with mock.patch.object(gcp_campaign, 'admit_permissions'), \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_image'), \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_abi'), \
             mock.patch.object(gcp_campaign, 'GoogleApi', return_value=ImageApi()):
            with self.assertRaisesRegex(ValueError, 'Private Google Access'):
                gcp_campaign.preflight(api, config, {}, {})
        self.assertTrue(all(method == 'GET' for method, _ in api.calls))

    def test_preflight_falcon_profile_default_and_explicit_mtu(self):
        import gcp_campaign
        class ImageApi:
            def request(self, method, path, body=None):
                return {'permissions': ['compute.images.useReadOnly']} if method == 'POST' else {'status': 'READY', 'diskSizeGb': '80'}
        class Operator:
            token_provider = staticmethod(lambda: 'token')
            opener = None
            def __init__(self, network):
                self.network = network
            def request(self, method, path, body=None):
                if path.endswith('/subnetworks/ordinary'):
                    return {'privateIpGoogleAccess': True}
                if path.endswith('/subnetworks/rdma'):
                    return {'network': 'projects/mysten-research/global/networks/falcon'}
                if path == 'global/networks/falcon':
                    return self.network
                return {}
            def objects(self, bucket):
                return Store()
        prefix = 'projects/mysten-research/regions/europe-west4/subnetworks/'
        profile = 'europe-west4-b-vpc-falcon'
        config = {'image': 'projects/image-project/global/images/pinned', 'bucket': 'bucket',
                  'project_id': 'mysten-research', 'machine_type': 'h4d-highmem-192-lssd',
                  'candidates': [{'zone': 'europe-west4-b', 'ordinary_subnet': prefix + 'ordinary',
                                  'rdma_subnet': prefix + 'rdma', 'network_profile': profile}]}
        network = {'networkProfile': 'projects/mysten-research/global/networkProfiles/' + profile}
        with mock.patch.object(gcp_campaign, 'admit_permissions'), \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_image'), \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_abi'), \
             mock.patch.object(gcp_campaign, 'GoogleApi', return_value=ImageApi()):
            for value in ({}, {'mtu': 8896}):
                gcp_campaign.preflight(Operator({**network, **value}), config, {}, {})
            for value in ({'mtu': 1460}, {'mtu': None}, {'mtu': '8896'},
                          {'networkProfile': 'europe-west4-b-vpc-roce'},
                          {'networkProfile': 'us-central1-a-vpc-falcon'}):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'Falcon profile'):
                    gcp_campaign.preflight(Operator({**network, **value}), config, {}, {})

    def test_foreign_instance_not_deleted(self):
        vm = self.vm(); vm['id'] = '43'
        rec = {'name': vm['name'], 'id': '42', 'zone': 'us-central1-a', 'disk_token': 'a' * 32}
        self.state['instances'] = [rec]; self.state.pop('policy')
        api = FakeApi({'zones/us-central1-a/instances/' + vm['name']: vm})
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'refusing to delete foreign'):
                cleanup(api, self.state, Path(tmp) / 'state.json')
        self.assertFalse(any(c[0] == 'DELETE' for c in api.calls))

    def test_cleanup_deletes_owned_and_checks_absence(self):
        vm = self.vm(); rec = {'name': vm['name'], 'id': '42', 'zone': 'us-central1-a', 'disk_token': 'a' * 32}
        self.state['instances'] = [rec]; self.state.pop('policy')
        api = FakeApi({'zones/us-central1-a/instances/' + vm['name']: vm})
        with tempfile.TemporaryDirectory() as tmp:
            cleanup(api, self.state, Path(tmp) / 'state.json')
        self.assertTrue(rec['deleted'])
        self.assertEqual(api.calls[-1][0], 'GET')

    def test_lost_mutation_reuses_request_id(self):
        api = FakeApi()
        result = mutation(api, 'POST', 'zones/z/instances', {}, 'fixed-id', 'zones/z')
        self.assertEqual(result['targetId'], '42')
        self.assertEqual([c[2]['requestId'] for c in api.calls], ['fixed-id', 'fixed-id'])

    def test_ambiguous_create_reconciled_before_delete(self):
        vm = self.vm()
        rec = {'name': vm['name'], 'zone': 'us-central1-a', 'uncertain': True, 'disk_token': 'a' * 32,
               'create_path': 'zones/us-central1-a/instances', 'create_body': vm,
               'create_request_id': 'same-create', 'scope': 'zones/us-central1-a'}
        self.state['instances'] = [rec]; self.state.pop('policy')
        api = FakeApi({'zones/us-central1-a/instances/' + vm['name']: vm})
        with tempfile.TemporaryDirectory() as tmp:
            cleanup(api, self.state, Path(tmp) / 'state.json')
        self.assertEqual(rec['id'], '42')
        self.assertTrue(rec['deleted'])
        self.assertEqual([c[2]['requestId'] for c in api.calls if c[0] == 'POST'], ['same-create', 'same-create'])

    def test_expired_ambiguous_creation_is_never_reissued(self):
        self.state['deadline'] = '2000-01-01T00:00:00Z'
        self.state['instances'] = [{'name': 'bench-0123456789abcdef-0', 'zone': 'us-central1-a', 'uncertain': True}]
        self.state.pop('policy')
        api = FakeApi()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, 'operator reconciliation'):
                cleanup(api, self.state, Path(tmp) / 'state.json')
        self.assertEqual(api.calls, [('GET', 'zones/us-central1-a/disks/bench-0123456789abcdef-0', None)])

    def test_reservation_idempotence_and_limit(self):
        store = Store(); now = dt.datetime(2026, 1, 5, tzinfo=dt.timezone.utc)
        first = gcp_budget.reserve(store, 'run', 100, 150, now)
        second = gcp_budget.reserve(store, 'run', 100, 150, now)
        self.assertEqual(first, second)
        self.assertEqual(first['lifetime'], 100)
        self.assertEqual(len(json.loads(store.get(gcp_budget.KEY))['runs']), 1)
        with self.assertRaisesRegex(ValueError, 'weekly budget'):
            gcp_budget.reserve(store, 'other', 51, 150, now)
        with self.assertRaisesRegex(ValueError, 'different estimate'):
            gcp_budget.reserve(store, 'run', 101, 150, now)

    def test_state_recovery_and_cas_reject_stale_writer(self):
        store = Store()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'state.json'
            statefile = StateFile(path, store, self.state['run_id'])
            save(statefile, self.state)
            recovered, state = StateFile.recover(Path(tmp) / 'recovered.json', store, self.state['run_id'])
            self.assertEqual(state, self.state)
            state['marker'] = 'recovered'
            save(recovered, state)
            with self.assertRaises(ApiError) as exc:
                save(statefile, self.state)
            self.assertEqual(exc.exception.status, 412)

    def test_lost_state_write_ack_still_allows_owned_cleanup(self):
        class LostAckStore(Store):
            lose = False
            def put(self, key, data, **kwargs):
                result = super().put(key, data, **kwargs)
                if self.lose:
                    self.lose = False
                    raise ApiError(None, 'lost response')
                return result

        store = LostAckStore()
        vm = self.vm()
        rec = {'name': vm['name'], 'id': '42', 'zone': 'us-central1-a', 'disk_token': 'a' * 32}
        self.state['instances'] = [rec]; self.state.pop('policy')
        api = FakeApi({'zones/us-central1-a/instances/' + vm['name']: vm})
        with tempfile.TemporaryDirectory() as tmp:
            statefile = StateFile(Path(tmp) / 'state.json', store, self.state['run_id'])
            save(statefile, self.state)
            store.lose = True
            cleanup(api, self.state, statefile)
            self.assertTrue(rec['deleted'])
            self.assertTrue(any(call[0] == 'DELETE' for call in api.calls))

    def test_full_work_budget_leaves_export_and_receipt_time(self):
        now = dt.datetime(2098, 12, 31, 23, 0, tzinfo=dt.timezone.utc)
        expiry = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
        timeout = coordinator_timeout(expiry.isoformat(), 600, now)
        campaign_end = expiry - dt.timedelta(seconds=120)
        work_end = campaign_end - dt.timedelta(seconds=600)
        clock = work_end
        self.assertLess(clock, now + dt.timedelta(seconds=timeout))
        clock += dt.timedelta(seconds=600)
        self.assertLess(clock, now + dt.timedelta(seconds=timeout))
        self.assertLess(now + dt.timedelta(seconds=timeout), expiry)

    def test_preflight_bounds_image_disk_before_allocation(self):
        import gcp_campaign
        class ImageApi:
            def __init__(self, size):
                self.size = size
            def request(self, method, path, body=None):
                return {'permissions': ['compute.images.useReadOnly']} if method == 'POST' else {'status': 'READY', 'diskSizeGb': self.size}
        class Operator:
            token_provider = staticmethod(lambda: 'token')
            opener = None
            def objects(self, bucket):
                return Store()
        config = {'image': 'projects/image-project/global/images/pinned', 'bucket': 'bucket', 'candidates': []}
        with mock.patch.object(gcp_campaign, 'admit_permissions'), \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_image') as image_admit, \
             mock.patch.object(gcp_campaign.gcp_image, 'admit_abi'), \
             mock.patch.object(gcp_campaign, 'reserve') as reservation:
            for size in ('81', '0', '-1', None):
                with self.subTest(size=size), mock.patch.object(gcp_campaign, 'GoogleApi', return_value=ImageApi(size)):
                    with self.assertRaisesRegex(ValueError, 'diskSizeGb'):
                        gcp_campaign.preflight(Operator(), config, {}, {})
            image_admit.assert_not_called()
            reservation.assert_not_called()
            with mock.patch.object(gcp_campaign, 'GoogleApi', return_value=ImageApi('80')):
                gcp_campaign.preflight(Operator(), config, {}, {})

    def test_existing_boot_disk_rejected_before_first_instance_create(self):
        import gcp_campaign
        import gcp_bootstrap
        api = FleetApi()
        state = dict(self.state)
        state.pop('policy')
        state['instances'] = []
        name = state['run_id'] + '-0'
        disk_path = 'zones/us-central1-a/disks/' + name
        api.resources[disk_path] = {'name': name, 'id': '123', 'description': 'foreign'}
        config = {**self.config, 'bucket': 'test-bucket', 'max_distance': 3, 'candidates': [self.candidate]}
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp, 'package.tar')
            archive.write_bytes(b'sealed')
            receipt = {'archive_path': str(archive), 'sha256': hashlib.sha256(b'sealed').hexdigest(),
                       'source_sha256': state['source_sha256'], 'source_revision': 'b' * 40}
            with mock.patch.object(gcp_campaign.time, 'sleep'), \
                 mock.patch.object(gcp_bootstrap, 'startup_script', return_value='startup'):
                with self.assertRaisesRegex(RuntimeError, 'boot disk name already exists'):
                    gcp_campaign.launch(api, config, {'run_upper_bound_cents': 10, 'weekly_limit_cents': 100},
                                        state, Path(tmp, 'state'), receipt, {}, tmp)
        self.assertIn(disk_path, api.resources)
        self.assertFalse(any(method == 'POST' and path.endswith('/instances') for method, path in api.calls))


    def test_full_campaign_async_fleet_fallback_and_failure_cleanup(self):
        import gcp_campaign
        import gcp_bootstrap
        import gcp_runtime
        for failure in (None, 'ZONE_RESOURCE_POOL_EXHAUSTED', 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                        'SYNC:403:quotaExceeded', 'SYNC:503:zoneResourcePoolExhaustedWithDetails',
                        'INVALID_PARAMETER', 'SYNC:403:forbidden'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                api = FleetApi(failure)
                state = dict(self.state)
                state.pop('policy')
                state['instances'] = []
                config = {**self.config, 'bucket': 'test-bucket', 'max_distance': 3,
                          'candidates': [self.candidate, {**self.candidate, 'zone': 'us-central1-b'}]}
                budget = {'run_upper_bound_cents': 10, 'weekly_limit_cents': 100}
                archive = Path(tmp) / 'package.tar'
                archive.write_bytes(b'sealed')
                receipt = {'archive_path': str(archive), 'sha256': hashlib.sha256(b'sealed').hexdigest(),
                           'source_sha256': state['source_sha256'], 'source_revision': 'b' * 40}
                spec = {'suite': 'kv', 'export_reserve_seconds': 600}
                observed_fleet = []

                def build(config, fleet, receipt, spec, deadline, **kwargs):
                    observed_fleet.extend(fleet)
                    return {'package_root': '/mnt/tidehunter/test/package'}

                class Mailbox:
                    def __init__(self, *args):
                        pass
                    def submit(self, index, argv, **kwargs):
                        return {'operation': kwargs['operation_id']}
                    def wait(self, reference, **kwargs):
                        for name in ('controller.tar.gz', 'controller-inventory.json', 'receipt.json'):
                            api.store.put(state['run_id'] + '/results/' + name, b'export', if_generation_match=0)
                        return {'exit_code': 0, 'operation_id': reference['operation']}

                with mock.patch.object(gcp_campaign.time, 'sleep'), \
                     mock.patch.object(gcp_bootstrap, 'startup_script', return_value='startup'), \
                     mock.patch.object(gcp_runtime, 'build_runtime_config', side_effect=build), \
                     mock.patch.object(gcp_runtime, 'RemoteMailbox', Mailbox):
                    if failure in ('INVALID_PARAMETER', 'SYNC:403:forbidden'):
                        with self.assertRaises(RuntimeError):
                            gcp_campaign.run_campaign(api, config, budget, state, Path(tmp) / 'state.json', receipt, spec, tmp)
                    else:
                        gcp_campaign.run_campaign(api, config, budget, state, Path(tmp) / 'state.json', receipt, spec, tmp)
                        self.assertEqual(len(observed_fleet), 6)
                        self.assertEqual(observed_fleet[0]['role'], 'coordinator')
                        self.assertTrue(all(host['role'] == 'owner' for host in observed_fleet[1:]))
                        self.assertTrue(Path(tmp, 'campaign-receipt.json').exists())
                        self.assertTrue(Path(tmp, 'results/controller.tar.gz').exists())
                self.assertEqual(api.resources, {})
                self.assertTrue(any(method == 'GET' and '/operations/' in path for method, path in api.calls))
                if failure not in (None, 'INVALID_PARAMETER', 'SYNC:403:forbidden', 'SYNC:503:zoneResourcePoolExhaustedWithDetails'):
                    policies = [i for i, call in enumerate(api.calls)
                                if call == ('POST', 'beta/regions/us-central1/resourcePolicies')]
                    self.assertEqual(len(policies), 2)
                    next_zone = policies[1]
                    deletions = [i for i, (method, path) in enumerate(api.calls)
                                 if method == 'DELETE' and 'us-central1-a/instances' in path]
                    self.assertEqual(len(deletions), 2)
                    self.assertLess(max(deletions), next_zone)
                    policy_deletes = [i for i, (method, path) in enumerate(api.calls)
                                      if method == 'DELETE' and '/resourcePolicies/' in path]
                    self.assertLess(policy_deletes[0], next_zone)
                if failure in ('INVALID_PARAMETER', 'SYNC:403:forbidden'):
                    self.assertFalse(any(method == 'POST' and 'us-central1-b' in path for method, path in api.calls))

    def test_result_inventory_generation_and_hash(self):
        store = Store(); store.put('bench-0123456789abcdef/results/raw.json', b'{}', if_generation_match=0)
        with tempfile.TemporaryDirectory() as tmp:
            inventory = collect_results(store, 'bench-0123456789abcdef', tmp)
            self.assertEqual(inventory[0]['sha256'], hashlib.sha256(b'{}').hexdigest())
            self.assertEqual(Path(tmp, 'results/raw.json').read_bytes(), b'{}')


if __name__ == '__main__':
    unittest.main()
