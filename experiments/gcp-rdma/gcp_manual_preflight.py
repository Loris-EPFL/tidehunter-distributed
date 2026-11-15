"""Explicit read-only permission admission for the manual GCP lifecycle.

The mounted gcloud MCP exposes no testIamPermissions command. This is the only
local REST control-plane fallback: it performs permission tests, not resource
mutations. The guest impersonation token is used only within this short preflight.
"""
import asyncio
import datetime
import re

from gcp_manual_image import _json

from gcp_api import GoogleApi
from gcp_campaign import GUEST_SERVICE_ACCOUNT, require_permissions

USER_PROJECT_PERMISSIONS = (
    'compute.instances.create', 'compute.instances.get', 'compute.instances.list',
    'compute.instances.delete', 'compute.instances.stop', 'compute.instances.setMetadata',
    'compute.instances.setLabels', 'compute.instances.setServiceAccount',
    'compute.instances.getSerialPortOutput', 'compute.disks.create',
    'compute.disks.get', 'compute.disks.list', 'compute.disks.delete', 'compute.disks.useReadOnly',
    'compute.images.create', 'compute.images.get', 'compute.images.list',
    'compute.images.delete', 'compute.images.useReadOnly', 'compute.machineTypes.get',
    'compute.networks.get', 'compute.networkProfiles.get',
    'compute.subnetworks.get', 'compute.subnetworks.use',
    'compute.resourcePolicies.create', 'compute.resourcePolicies.get',
    'compute.resourcePolicies.list', 'compute.resourcePolicies.use',
    'compute.resourcePolicies.delete', 'compute.zoneOperations.get',
    'compute.regionOperations.get', 'compute.globalOperations.get',
)
SERVICE_ACCOUNT_PERMISSIONS = ('iam.serviceAccounts.actAs', 'iam.serviceAccounts.getAccessToken')
BUCKET_PERMISSIONS = ('storage.objects.get', 'storage.objects.list',
                      'storage.objects.create', 'storage.objects.delete')


def permission_preflight(config, *, api=None):
    if (config.get('project_id') != 'mysten-research'
            or config.get('bucket') != 'mysten-research-bench-artifacts'
            or config.get('service_account') != GUEST_SERVICE_ACCOUNT):
        raise ValueError('manual preflight requires the reviewed project, bucket and guest')
    api = api or GoogleApi(config['project_id'])
    require_permissions('user project', USER_PROJECT_PERMISSIONS,
                        api.project_permissions(USER_PROJECT_PERMISSIONS))
    require_permissions('user guest delegation', SERVICE_ACCOUNT_PERMISSIONS,
                        api.service_account_permissions(GUEST_SERVICE_ACCOUNT, SERVICE_ACCOUNT_PERMISSIONS))
    guest = api.impersonated(GUEST_SERVICE_ACCOUNT)
    require_permissions('guest bucket', BUCKET_PERMISSIONS,
                        guest.bucket_permissions(config['bucket'], BUCKET_PERMISSIONS))
    return {'observed_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'project': config['project_id'], 'guest_service_account': GUEST_SERVICE_ACCOUNT,
            'user_project_permissions': list(USER_PROJECT_PERMISSIONS),
            'service_account_permissions': list(SERVICE_ACCOUNT_PERMISSIONS),
            'guest_bucket_permissions': list(BUCKET_PERMISSIONS),
            'guest_resource_permissions': 'not established by this preflight'}


async def infrastructure_preflight(config, base_image, execute):
    """Read the exact reviewed image, machine, subnets and Falcon profile."""
    if (config.get('project_id') != 'mysten-research'
            or config.get('machine_type', 'z4m-highmem-16') not in {'z4m-highmem-16', 'z4m-highmem-32'}
            or config.get('max_distance') != 3):
        raise ValueError('unreviewed infrastructure configuration')
    candidate = {'zone': 'us-central1-a',
                 'ordinary_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/default',
                 'rdma_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/falcon-rdma-us-central1-a',
                 'network_profile': 'projects/mysten-research/global/networkProfiles/us-central1-a-vpc-falcon'}
    if config.get('candidates') != [candidate]:
        raise ValueError('exact reviewed subnet and zone configuration required')
    if not isinstance(base_image, str) or not re.fullmatch(
            r'projects/mysten-research/global/images/[a-z][a-z0-9-]{0,62}', base_image):
        raise ValueError('exact custom base image required')

    async def describe(arguments, expected_name):
        response = await asyncio.wait_for(execute(arguments + [
            '--project=mysten-research', '--quiet', '--format=json']), timeout=30)
        value = _json(response)
        if not isinstance(value, dict) or value.get('name') != expected_name:
            raise ValueError('resource description name mismatch')
        return value

    image_name = base_image.rsplit('/', 1)[-1]
    image = await describe(['compute', 'images', 'describe', image_name], image_name)
    if (image.get('status') != 'READY'
            or not str(image.get('id', '')).isdigit()
            or not str(image.get('selfLink', '')).endswith('/' + base_image)
            or not str(image.get('diskSizeGb', '')).isdigit()
            or not 0 < int(image['diskSizeGb']) <= 80):
        raise ValueError('base image identity, size or READY status mismatch')
    guest_image = await describe(
        ['compute', 'images', 'describe', image_name,
         '--impersonate-service-account=' + GUEST_SERVICE_ACCOUNT, '--verbosity=error'], image_name)
    if any(guest_image.get(key) != image[key]
           for key in ('id', 'status', 'selfLink', 'diskSizeGb')):
        raise ValueError('guest image identity differs from the reviewed base image')
    image['guest_image_read'] = {
        'service_account': GUEST_SERVICE_ACCOUNT, 'path': base_image, 'id': image['id']}
    machine_type = config.get('machine_type', 'z4m-highmem-16')
    machine = await describe(['compute', 'machine-types', 'describe', machine_type,
                              '--zone=us-central1-a'], machine_type)
    if not str(machine.get('selfLink', '')).endswith(
            '/projects/mysten-research/zones/us-central1-a/machineTypes/' + machine_type):
        raise ValueError('machine type identity mismatch')
    ordinary = await describe(['compute', 'networks', 'subnets', 'describe', 'default',
                               '--region=us-central1'], 'default')
    rdma = await describe(['compute', 'networks', 'subnets', 'describe', 'falcon-rdma-us-central1-a',
                           '--region=us-central1'], 'falcon-rdma-us-central1-a')
    if (not str(ordinary.get('selfLink', '')).endswith('/' + candidate['ordinary_subnet'])
            or ordinary.get('privateIpGoogleAccess') is not True
            or not str(rdma.get('selfLink', '')).endswith('/' + candidate['rdma_subnet'])):
        raise ValueError('subnet identity or private Google access mismatch')
    network_path = rdma.get('network', '')
    network_name = str(network_path).rsplit('/', 1)[-1]
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', network_name) or not str(network_path).endswith(
            '/projects/mysten-research/global/networks/' + network_name):
        raise ValueError('RDMA network identity mismatch')
    network = await describe(['compute', 'networks', 'describe', network_name], network_name)
    if (not str(network.get('selfLink', '')).endswith('/projects/mysten-research/global/networks/' + network_name)
            or not str(network.get('networkProfile', '')).endswith('/' + candidate['network_profile'])
            or ('mtu' in network and network['mtu'] != 8896)):
        raise ValueError('RDMA network profile or MTU mismatch')
    profile = await describe(['compute', 'network-profiles', 'describe', 'us-central1-a-vpc-falcon'],
                             'us-central1-a-vpc-falcon')
    if (not str(profile.get('selfLink', '')).endswith('/' + candidate['network_profile'])
            or profile.get('location', {}).get('name') != 'us-central1-a'
            or profile.get('profileType', {}).get('networkType') != 'RDMA'
            or profile.get('profileType', {}).get('rdmaSubtype') != 'FALCON'
            or 'IRDMA' not in profile.get('features', {}).get('interfaceTypes', [])):
        raise ValueError('zonal Falcon IRDMA profile mismatch')
    return image
