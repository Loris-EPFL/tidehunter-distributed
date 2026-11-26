import json
from unittest import IsolatedAsyncioTestCase, TestCase

import gcp_manual_preflight as preflight


class Guest:
    def __init__(self, missing=()):
        self.missing = set(missing)

    def bucket_permissions(self, bucket, permissions):
        return [p for p in permissions if p not in self.missing]


class User:
    def __init__(self, missing=(), guest_missing=()):
        self.missing = set(missing)
        self.guest = Guest(guest_missing)
        self.delegated = False

    def project_permissions(self, permissions):
        return [p for p in permissions if p not in self.missing]

    def service_account_permissions(self, account, permissions):
        return [p for p in permissions if p not in self.missing]

    def impersonated(self, account):
        self.delegated = True
        return self.guest


CONFIG = {'project_id': 'mysten-research', 'bucket': 'mysten-research-bench-artifacts',
          'service_account': 'bench-guest@mysten-research.iam.gserviceaccount.com'}


class PermissionAdmissionTest(TestCase):
    def test_permission_admission_does_not_claim_future_guest_access(self):
        api = User()
        evidence = preflight.permission_preflight(CONFIG, api=api)
        self.assertTrue(api.delegated)
        self.assertEqual(evidence['guest_resource_permissions'], 'not established by this preflight')
        self.assertIn('compute.instances.create', evidence['user_project_permissions'])

    def test_management_or_delegation_denial_fails_before_impersonation(self):
        for permission in ('compute.instances.create', 'iam.serviceAccounts.actAs'):
            with self.subTest(permission=permission):
                api = User(missing=[permission])
                with self.assertRaisesRegex(ValueError, permission):
                    preflight.permission_preflight(CONFIG, api=api)
                self.assertFalse(api.delegated)

    def test_guest_bucket_denial_is_not_passed(self):
        api = User(guest_missing=['storage.objects.delete'])
        with self.assertRaisesRegex(ValueError, 'storage.objects.delete'):
            preflight.permission_preflight(CONFIG, api=api)


class InfrastructureAdmissionTest(IsolatedAsyncioTestCase):
    def setUp(self):
        base = 'projects/mysten-research'
        self.config = {**CONFIG, 'machine_type': 'z4m-highmem-16', 'max_distance': 3,
                       'candidates': [{'zone': 'us-central1-a',
                           'ordinary_subnet': base + '/regions/us-central1/subnetworks/default',
                           'rdma_subnet': base + '/regions/us-central1/subnetworks/falcon-rdma-us-central1-a',
                           'network_profile': base + '/global/networkProfiles/us-central1-a-vpc-falcon'}]}
        self.image_path = base + '/global/images/example'
        self.resources = {
            ('images', 'example'): {'name': 'example', 'status': 'READY', 'id': '123', 'diskSizeGb': '80', 'selfLink': 'https://compute/' + self.image_path},
            ('machine-types', 'z4m-highmem-16'): {'name': 'z4m-highmem-16', 'selfLink': 'https://compute/' + base + '/zones/us-central1-a/machineTypes/z4m-highmem-16'},
            ('subnets', 'default'): {'name': 'default', 'selfLink': 'https://compute/' + self.config['candidates'][0]['ordinary_subnet'], 'privateIpGoogleAccess': True},
            ('subnets', 'falcon-rdma-us-central1-a'): {'name': 'falcon-rdma-us-central1-a', 'selfLink': 'https://compute/' + self.config['candidates'][0]['rdma_subnet'], 'network': 'https://compute/' + base + '/global/networks/falcon'},
            ('networks', 'falcon'): {'name': 'falcon', 'selfLink': 'https://compute/' + base + '/global/networks/falcon', 'networkProfile': 'https://compute/' + self.config['candidates'][0]['network_profile'], 'mtu': 8896},
            ('network-profiles', 'us-central1-a-vpc-falcon'): {'name': 'us-central1-a-vpc-falcon', 'selfLink': 'https://compute/' + self.config['candidates'][0]['network_profile'], 'location': {'name': 'us-central1-a'}, 'profileType': {'networkType': 'RDMA', 'rdmaSubtype': 'FALCON'}, 'features': {'interfaceTypes': ['IRDMA']}},
        }

    async def execute(self, argv):
        position = argv.index('describe')
        kind, name = argv[position - 1], argv[position + 1]
        return {'text': json.dumps(self.resources[(kind, name)])}

    async def test_exact_network_and_image_are_admitted(self):
        image = await preflight.infrastructure_preflight(self.config, self.image_path, self.execute)
        self.assertEqual(image['id'], '123')

    async def test_a_cold_token_refresh_has_a_two_minute_bound(self):
        # A cold refresh exceeded thirty seconds at 03:00Z and aborted a
        # requalification before any host existed; the bound is 120 s.
        self.assertEqual(preflight.GCLOUD_CALL_SECONDS, 120)
        bounds = []

        async def slow(argv):
            bounds.append(preflight.GCLOUD_CALL_SECONDS)
            return await self.execute(argv)

        await preflight.infrastructure_preflight(self.config, self.image_path, slow)
        self.assertTrue(bounds and all(value == 120 for value in bounds))

    async def test_missing_private_google_access_fails_closed(self):
        self.resources[('subnets', 'default')]['privateIpGoogleAccess'] = False
        with self.assertRaisesRegex(ValueError, 'private Google'):
            await preflight.infrastructure_preflight(self.config, self.image_path, self.execute)

    async def test_wrong_fabric_fails_closed(self):
        self.resources[('network-profiles', 'us-central1-a-vpc-falcon')]['features']['interfaceTypes'] = ['GVNIC']
        with self.assertRaisesRegex(ValueError, 'IRDMA'):
            await preflight.infrastructure_preflight(self.config, self.image_path, self.execute)

    async def test_guest_image_read_denial_blocks_admission(self):
        async def denied(argv):
            if '--impersonate-service-account=' + preflight.GUEST_SERVICE_ACCOUNT in argv:
                raise PermissionError('guest image read denied')
            return await self.execute(argv)

        with self.assertRaisesRegex(PermissionError, 'guest image read denied'):
            await preflight.infrastructure_preflight(self.config, self.image_path, denied)

    async def test_guest_image_replacement_blocks_admission(self):
        async def replaced(argv):
            response = await self.execute(argv)
            if '--impersonate-service-account=' + preflight.GUEST_SERVICE_ACCOUNT in argv:
                image = json.loads(response['text'])
                image['id'] = '456'
                return {'text': json.dumps(image)}
            return response

        with self.assertRaisesRegex(ValueError, 'guest image identity'):
            await preflight.infrastructure_preflight(self.config, self.image_path, replaced)
