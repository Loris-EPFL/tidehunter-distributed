#!/usr/bin/env python3
"""GCP benchmark lifecycle. No cloud mutation occurs without explicit activation."""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

from gcp_api import ApiError, GoogleApi
from gcp_budget import reserve
import gcp_image
import gcp_lease
from gcp_runtime import GUEST_PYTHON, LOCAL_SSD_COUNTS

BOOT_DISK_GIB = 80
GUEST_SERVICE_ACCOUNT = 'bench-guest@mysten-research.iam.gserviceaccount.com'
MAX_DURATION_SECONDS = 4 * 60 * 60


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def parse_time(value):
    result = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('deadline must include UTC offset')
    return result.astimezone(dt.timezone.utc)


def validate(config, budget, run_id, source, deadline):
    if config.get('version') != 1 or config.get('project_id') != 'mysten-research':
        raise ValueError('config requires version 1 and project_id mysten-research')
    for key in ('bucket', 'service_account', 'image', 'project_number'):
        if not config.get(key):
            raise ValueError('config missing ' + key)
    if config['service_account'] != GUEST_SERVICE_ACCOUNT:
        raise ValueError('config service_account must be ' + GUEST_SERVICE_ACCOUNT)
    if config.get('machine_type') != 'z4m-highmem-16':
        raise ValueError('protected campaign requires z4m-highmem-16')
    if type(config.get('max_distance')) is not int or config['max_distance'] not in (1, 2, 3):
        raise ValueError('max_distance must be 1, 2 or 3')
    if not config.get('candidates'):
        raise ValueError('config missing explicit zonal candidates')
    for candidate in config['candidates']:
        for key in ('zone', 'ordinary_subnet', 'rdma_subnet', 'network_profile'):
            if not candidate.get(key):
                raise ValueError('candidate missing ' + key)
        if not re.fullmatch(r'[a-z]+(?:-[a-z0-9]+)+-\w', candidate['zone']):
            raise ValueError('invalid candidate zone')
        if candidate['zone'].rsplit('-', 1)[0] != 'us-central1':
            raise ValueError('protected campaign price is reviewed only for us-central1')
        if candidate['ordinary_subnet'] == candidate['rdma_subnet']:
            raise ValueError('ordinary and Falcon subnets must differ')
    if not re.fullmatch(r'bench-[0-9a-f]{16}', run_id):
        raise ValueError('run_id must match bench-[0-9a-f]{16}')
    if not re.fullmatch(r'[0-9a-f]{64}', source):
        raise ValueError('source_sha256 must be a full SHA256')
    if parse_time(deadline) <= dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=15):
        raise ValueError('deadline must reserve at least 15 minutes for launch and export')
    for key in ('weekly_limit_cents', 'run_upper_bound_cents', 'max_duration_seconds'):
        if type(budget.get(key)) is not int or budget[key] <= 0:
            raise ValueError('budget missing positive integer ' + key)
    if budget['max_duration_seconds'] > MAX_DURATION_SECONDS:
        raise ValueError('quoted maximum duration exceeds 4-hour safety limit')
    # Reviewed six-host US rate: $3.287109973 per host-hour, rounded up to cents.
    numerator = 6 * budget['max_duration_seconds'] * 3287109973 * 100
    denominator = 3600 * 1000000000
    base_cents = (numerator + denominator - 1) // denominator
    if budget['run_upper_bound_cents'] < base_cents:
        raise ValueError(f'run quote below six-host compute base: {base_cents} cents')
    if (parse_time(deadline) - dt.datetime.now(dt.timezone.utc)).total_seconds() > budget['max_duration_seconds']:
        raise ValueError('deadline exceeds quoted maximum duration')
    if not budget.get('quote_description'):
        raise ValueError('budget missing explicit quote_description including all expected charges')


def admit():
    if os.environ.get('GITHUB_REF') != 'refs/heads/bench/gcp-rdma' or os.environ.get('GITHUB_ACTIONS') != 'true':
        raise ValueError('allocation requires trusted GitHub bench/gcp-rdma branch')


class StateFile:
    def __init__(self, path, store, run_id, generation=0):
        self.path = Path(path)
        self.store = store
        self.key = 'runs/' + run_id + '/lifecycle/state.json'
        self.generation = generation

    def persist(self, state):
        data = json_bytes(state)
        try:
            meta = self.store.put(self.key, data, content_type='application/json', if_generation_match=self.generation)
        except ApiError as exc:
            if exc.status is not None and exc.status < 500:
                raise
            try:
                meta = self.store.head(self.key)
                observed = self.store.get(self.key, generation=meta['generation'])
            except Exception:
                raise exc
            if observed != data:
                raise exc
        self.generation = meta['generation']
        save(self.path, state)

    @classmethod
    def recover(cls, path, store, run_id):
        key = 'runs/' + run_id + '/lifecycle/state.json'
        meta = store.head(key)
        state = json.loads(store.get(key, generation=meta['generation']))
        return cls(path, store, run_id, meta['generation']), state


def save(path, state):
    if isinstance(path, StateFile):
        path.persist(state)
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_bytes(json_bytes(state))
    os.replace(temp, path)


def read_optional(api, path):
    try:
        return api.request('GET', path)
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise


class OperationFailure(RuntimeError):
    def __init__(self, error):
        self.error = error
        super().__init__('Compute operation failed: ' + json.dumps(error, sort_keys=True))


class MutationRejected(ApiError):
    """A definitive response to the first mutation request, not a poll failure."""


def capacity_failure(exc):
    allowed = {'ZONE_RESOURCE_POOL_EXHAUSTED', 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
               'QUOTA_EXCEEDED', 'RESOURCE_POOL_EXHAUSTED'}
    if isinstance(exc, OperationFailure):
        errors = exc.error.get('errors', [])
        return isinstance(errors, list) and bool(errors) and all(isinstance(item, dict) and item.get('code') in allowed for item in errors)
    if isinstance(exc, MutationRejected) and exc.status in (403, 503):
        try:
            errors = json.loads(exc.message)['error']['errors']
        except (ValueError, KeyError, TypeError):
            return False
        def reason(item):
            return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', str(item.get('reason', ''))).upper()
        return isinstance(errors, list) and bool(errors) and all(isinstance(item, dict) and reason(item) in allowed for item in errors)
    return False


def poll(api, operation, scope, timeout=600):
    end = time.monotonic() + timeout
    while operation.get('status') != 'DONE':
        if not operation.get('name') or time.monotonic() >= end:
            raise RuntimeError('operation unresolved; manual reconciliation required')
        time.sleep(2)
        operation = api.request('GET', scope + '/operations/' + operation['name'])
    if operation.get('error'):
        raise OperationFailure(operation['error'])
    return operation


def mutation(api, method, path, body, request_id, scope):
    # A lost response must be retried with the same request ID, never a fresh one.
    try:
        op = api.request(method, path, body, {'requestId': request_id})
    except ApiError as exc:
        if exc.status is not None and 400 <= exc.status < 500 and exc.status != 429:
            raise MutationRejected(exc.status, exc.message) from exc
        op = api.request(method, path, body, {'requestId': request_id})
    return poll(api, op, scope)


def require_permissions(label, requested, granted):
    missing = sorted(set(requested) - set(granted))
    if missing:
        raise ValueError(label + ' missing permissions: ' + ', '.join(missing))


def admit_permissions(api, config):
    project = ('compute.instances.create', 'compute.instances.get', 'compute.instances.delete',
               'compute.instances.setMetadata', 'compute.instances.setLabels', 'compute.instances.setServiceAccount',
               'compute.disks.create', 'compute.disks.get', 'compute.disks.delete', 'compute.machineTypes.get', 'compute.networks.get',
               'compute.subnetworks.get', 'compute.subnetworks.use',
               'compute.resourcePolicies.create', 'compute.resourcePolicies.get',
               'compute.resourcePolicies.use', 'compute.resourcePolicies.delete',
               'compute.zoneOperations.get', 'compute.regionOperations.get')
    storage = ('storage.objects.get', 'storage.objects.list', 'storage.objects.create', 'storage.objects.delete')
    require_permissions('orchestrator project', project, api.project_permissions(project))
    service_account = ('iam.serviceAccounts.actAs', 'iam.serviceAccounts.getAccessToken')
    require_permissions('orchestrator VM service account', service_account,
                        api.service_account_permissions(config['service_account'], service_account))
    require_permissions('orchestrator bucket', storage, api.bucket_permissions(config['bucket'], storage))
    guest = api.impersonated(config['service_account'])
    require_permissions('guest project', ('compute.instances.get',),
                        guest.project_permissions(('compute.instances.get',)))
    require_permissions('guest bucket', storage, guest.bucket_permissions(config['bucket'], storage))


def preflight(api, config, receipt, spec):
    admit_permissions(api, config)
    image = config['image'].split('/')
    if len(image) != 5 or image[0] != 'projects' or image[2:4] != ['global', 'images'] or not image[1] or not image[4]:
        raise ValueError('image must be a pinned projects/PROJECT/global/images/IMAGE path, not a family')
    image_api = GoogleApi(image[1], token_provider=api.token_provider, opener=api.opener)
    image_permissions = ('compute.images.useReadOnly',)
    granted = image_api.request('POST', 'global/images/' + image[4] + '/testIamPermissions',
                                {'permissions': list(image_permissions)}).get('permissions', [])
    require_permissions('orchestrator image', image_permissions, granted)
    image_info = image_api.request('GET', 'global/images/' + image[4])
    size = image_info.get('diskSizeGb')
    if not isinstance(size, str) or not size.isascii() or not size.isdecimal() or not 0 < int(size) <= BOOT_DISK_GIB:
        raise ValueError('image diskSizeGb must be positive and no larger than 80 GiB')
    qualification = gcp_image.admit_image(api, config, image_info)
    gcp_image.admit_abi(qualification, receipt.get('abi'))
    if spec.get('suite') in ('sui', 'both'):
        gcp_image.admit_abi(qualification, receipt.get('sui_abi'))
    if image_info.get('status') != 'READY':
        raise ValueError('configured image is not READY')
    for c in config['candidates']:
        z = c['zone']
        region = z.rsplit('-', 1)[0]
        api.request('GET', f'zones/{z}/machineTypes/{config["machine_type"]}')
        for field in ('ordinary_subnet', 'rdma_subnet'):
            subnet = c[field]
            if not subnet.startswith('projects/'):
                raise ValueError(field + ' must be a full projects/.../regions/.../subnetworks/... path')
            pieces = subnet.split('/')
            if len(pieces) != 6 or pieces[0] != 'projects' or pieces[2] != 'regions' or pieces[3] != region or pieces[4] != 'subnetworks':
                raise ValueError(field + ' must identify a subnet in candidate region')
            if pieces[1] != config['project_id']:
                raise ValueError(field + ' must belong to configured project')
            found = api.request('GET', '/'.join(pieces[2:]))
            if field == 'ordinary_subnet' and found.get('privateIpGoogleAccess') is not True:
                raise ValueError('ordinary_subnet must have Private Google Access enabled for private-only VMs')
            if field == 'rdma_subnet':
                network = found.get('network', '').split('/')[-1]
                net = api.request('GET', 'global/networks/' + network)
                profile = net.get('networkProfile', '').split('/')[-1]
                # An omitted MTU uses the RDMA profile's documented 8896 default.
                # https://docs.cloud.google.com/vpc/docs/rdma-network-profiles#specifications
                if (profile != z + '-vpc-falcon' or profile != c['network_profile'].split('/')[-1]
                    or ('mtu' in net and (type(net['mtu']) is not int or net['mtu'] != 8896))):
                    raise ValueError('RDMA subnet network must have the zonal Falcon profile and MTU 8896')
    # Non-mutating list verifies access to the configured existing bucket.
    next(iter(api.objects(config['bucket']).list('runs/__preflight__/')), None)


def instance_body(config, c, state, i, startup, disk_token):
    if not isinstance(config.get('machine_type'), str) or config['machine_type'] not in LOCAL_SSD_COUNTS:
        raise ValueError('machine type must have an admitted local SSD layout')
    if config.get('service_account') != GUEST_SERVICE_ACCOUNT:
        raise ValueError('config service_account must be ' + GUEST_SERVICE_ACCOUNT)
    if not re.fullmatch(r'bench-[0-9a-f]{16}', state['run_id']) or i not in range(6):
        raise ValueError('invalid fleet instance identity')
    zone, run_id, source, deadline = c['zone'], state['run_id'], state['source_sha256'], state['deadline']
    name = f'{run_id}-{i}'
    if not re.fullmatch(r'[0-9a-f]{32}', disk_token):
        raise ValueError('invalid boot disk creation token')
    disk_description = json.dumps({'run': run_id, 'source': source, 'expiry': deadline, 'token': disk_token}, sort_keys=True)
    return {'name': name, 'machineType': f'zones/{zone}/machineTypes/{config["machine_type"]}',
            'networkInterfaces': [{'subnetwork': c['ordinary_subnet'], 'nicType': 'GVNIC'}, {'subnetwork': c['rdma_subnet'], 'nicType': 'IRDMA'}],
            # Both admitted shapes automatically attach their fixed Local SSDs.
            # https://docs.cloud.google.com/compute/docs/disks/add-local-ssd
            'disks': [{'boot': True, 'autoDelete': True, 'interface': 'NVME', 'initializeParams': {'diskName': name, 'sourceImage': config['image'], 'diskType': f'zones/{zone}/diskTypes/hyperdisk-balanced', 'diskSizeGb': str(BOOT_DISK_GIB), 'description': disk_description}}],
            'serviceAccounts': [{'email': config['service_account'], 'scopes': ['https://www.googleapis.com/auth/cloud-platform']}],
            'resourcePolicies': [f'regions/{zone.rsplit("-", 1)[0]}/resourcePolicies/{state["policy"]["name"]}'],
            'labels': {'bench-run': run_id, 'bench-source': source[:63]},
            'metadata': {'items': [{'key': 'startup-script', 'value': startup}, {'key': 'bench-run', 'value': run_id}, {'key': 'bench-source', 'value': source}, {'key': 'bench-expiry', 'value': deadline}, {'key': 'tidehunter-run-id', 'value': run_id}, {'key': 'tidehunter-source-sha256', 'value': source}, {'key': 'tidehunter-deadline-utc', 'value': deadline}]},
            'scheduling': {'onHostMaintenance': 'TERMINATE', 'automaticRestart': False, 'terminationTime': deadline, 'instanceTerminationAction': 'DELETE'}}


def owned_instance(obj, state, record):
    items = {x['key']: x['value'] for x in obj.get('metadata', {}).get('items', [])}
    return (str(obj.get('id')) == str(record.get('id')) and obj.get('name') == record['name']
            and obj.get('labels', {}).get('bench-run') == state['run_id']
            and obj.get('labels', {}).get('bench-source') == state['source_sha256'][:63]
            and items.get('bench-source') == state['source_sha256'] and items.get('bench-run') == state['run_id']
            and items.get('bench-expiry') == state['deadline']
            and [x.get('email') for x in obj.get('serviceAccounts', [])] == [GUEST_SERVICE_ACCOUNT])


def has_delete_expiry(obj, deadline):
    scheduling = obj.get('scheduling', {})
    try:
        return (scheduling.get('instanceTerminationAction') == 'DELETE'
                and parse_time(scheduling.get('terminationTime', '')) == parse_time(deadline))
    except (ValueError, TypeError):
        return False


def reconcile_create(api, state, record, state_path):
    if not record.get('uncertain'):
        return
    if parse_time(state['deadline']) <= dt.datetime.now(dt.timezone.utc):
        raise RuntimeError('ambiguous create after expiry requires operator reconciliation')
    try:
        op = mutation(api, 'POST', record['create_path'], record['create_body'], record['create_request_id'], record['scope'])
        record['id'] = str(op['targetId'])
    except OperationFailure:
        pass
    record['uncertain'] = False
    save(state_path, state)

def owned_disk(disk, state, record):
    token = record.get('disk_token')
    if not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{32}', token):
        return False
    description = json.dumps({'run': state['run_id'], 'source': state['source_sha256'],
                              'expiry': state['deadline'], 'token': token}, sort_keys=True)
    return (disk.get('name') == record['name'] and str(disk.get('id', '')).isdecimal()
            and disk.get('description') == description)


def capture_boot_disk(api, state, record, vm, state_path):
    disks = [disk for disk in vm.get('disks', []) if disk.get('boot') is True]
    expected = f'zones/{record["zone"]}/disks/{record["name"]}'
    source = disks[0].get('source', '') if len(disks) == 1 else ''
    disk_resource = 'projects/mysten-research/' + expected
    if (len(disks) != 1 or disks[0].get('autoDelete') is not True
            or not (source == disk_resource or source.endswith('/' + disk_resource))):
        raise RuntimeError('boot disk identity or autoDelete mismatch')
    disk = api.request('GET', expected)
    if not owned_disk(disk, state, record):
        raise RuntimeError('boot disk identity unavailable')
    if record.get('disk_id') and str(disk['id']) != record['disk_id']:
        raise RuntimeError('boot disk identity changed')
    record['disk_id'] = str(disk['id'])
    save(state_path, state)


def cleanup_disk(api, state, record, state_path):
    path = f'zones/{record["zone"]}/disks/{record["name"]}'
    disk = read_optional(api, path)
    if disk is None:
        return
    if (not owned_disk(disk, state, record)
            or (record.get('disk_id') and str(disk['id']) != record['disk_id'])):
        raise RuntimeError('refusing to delete foreign or unidentified boot disk ' + record['name'])
    if disk.get('users'):
        raise RuntimeError('boot disk still attached: ' + record['name'])
    record['disk_id'] = str(disk['id'])
    record.setdefault('disk_delete_request_id', str(uuid.uuid4()))
    save(state_path, state)
    mutation(api, 'DELETE', path, None, record['disk_delete_request_id'], f'zones/{record["zone"]}')
    if read_optional(api, path) is not None:
        raise RuntimeError('boot disk deletion not verified')




def cleanup(api, state, state_path):
    errors = []
    for record in reversed(state.get('instances', [])):
        if (record.get('name') not in {state['run_id'] + '-' + str(i) for i in range(6)}
                or not re.fullmatch(r'us-central1-[a-z]', record.get('zone', ''))):
            errors.append('invalid instance record identity')
            continue
        path = f'zones/{record["zone"]}/instances/{record["name"]}'
        try:
            reconcile_create(api, state, record, state_path)
            obj = read_optional(api, path)
            if obj is not None:
                if not record.get('id') or not owned_instance(obj, state, record):
                    raise RuntimeError('refusing to delete foreign or unidentified instance ' + record['name'])
                try:
                    capture_boot_disk(api, state, record, obj, state_path)
                except Exception as exc:
                    errors.append(str(exc))
                record.setdefault('delete_request_id', str(uuid.uuid4()))
                save(state_path, state)
                mutation(api, 'DELETE', path, None, record['delete_request_id'], f'zones/{record["zone"]}')
                if read_optional(api, path) is not None:
                    raise RuntimeError('instance deletion not verified')
                record['deleted'] = True
                save(state_path, state)
            elif record.get('uncertain'):
                raise RuntimeError('ambiguous create unresolved; absence alone is not proof')
        except Exception as exc:
            errors.append(str(exc))
        try:
            cleanup_disk(api, state, record, state_path)
        except Exception as exc:
            errors.append(str(exc))
    policy = state.get('policy')
    if policy:
        try:
            if policy.get('name') != state['run_id'] + '-placement' or policy.get('region') != 'us-central1':
                raise RuntimeError('invalid placement policy record identity')
            reconcile_create(api, state, policy, state_path)
            path = f'regions/{policy["region"]}/resourcePolicies/{policy["name"]}'
            obj = read_optional(api, 'beta/' + path)
            if obj is not None:
                if not policy.get('id') or str(obj.get('id')) != str(policy['id']) or obj.get('description') != policy['description']:
                    raise RuntimeError('refusing to delete foreign or unidentified placement policy')
                policy.setdefault('delete_request_id', str(uuid.uuid4()))
                save(state_path, state)
                mutation(api, 'DELETE', 'beta/' + path, None, policy['delete_request_id'], f'regions/{policy["region"]}')
                if read_optional(api, 'beta/' + path) is not None:
                    raise RuntimeError('placement policy deletion not verified')
            elif policy.get('uncertain'):
                raise RuntimeError('ambiguous placement creation unresolved')
        except Exception as exc:
            errors.append(str(exc))
    state['cleanup_errors'] = errors
    save(state_path, state)
    if errors:
        raise RuntimeError('cleanup incomplete: ' + '; '.join(errors))


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def upload_file(store, path, digest, key):
    if file_sha256(path) != digest:
        raise ValueError('package SHA256 mismatch: ' + str(path))
    meta = store.upload(key, path, if_generation_match=0)
    if meta.get('metadata', {}).get('sha256') != digest:
        raise ValueError('package changed during upload')
    return str(meta['generation'])


def launch(api, config, budget, state, state_path, receipt, spec, output):
    from gcp_bootstrap import startup_script
    from gcp_runtime import build_runtime_config, RemoteMailbox
    store = api.objects(config['bucket'])
    prefix = 'runs/' + state['run_id'] + '/'
    report = reserve(store, state['run_id'], budget['run_upper_bound_cents'], budget['weekly_limit_cents'])
    Path(output, 'budget-report.json').write_bytes(json_bytes(report))
    if receipt.get('source_sha256') != state['source_sha256'] or not re.fullmatch(r'[0-9a-f]{40}', receipt.get('source_revision', '')):
        raise ValueError('sealed package source identity mismatch')
    receipt['object_key'] = prefix + 'package.tar'
    receipt['generation'] = upload_file(store, receipt['archive_path'], receipt['sha256'], receipt['object_key'])
    if receipt.get('sui_archive_path'):
        receipt['sui_object_key'] = prefix + 'sui-package.tar'
        receipt['sui_generation'] = upload_file(store, receipt['sui_archive_path'], receipt['sui_sha256'], receipt['sui_object_key'])
    if receipt.get('verification_archive_path'):
        key = prefix + 'sui-verification.tar'
        generation = upload_file(store, receipt['verification_archive_path'], receipt['verification_sha256'], key)
        spec['sui_verification'] = {'key': key, 'generation': generation, 'sha256': receipt['verification_sha256']}
    store.put(prefix + 'package.json', json_bytes(receipt), content_type='application/json', if_generation_match=0)
    startup = startup_script(config['bucket'], state['run_id'], state['source_sha256'], state['deadline'])
    fleet = []
    for candidate in config['candidates']:
        if parse_time(state['deadline']) <= dt.datetime.now(dt.timezone.utc):
            raise RuntimeError('campaign deadline reached before policy creation')
        zone = candidate['zone']; region = zone.rsplit('-', 1)[0]
        policy_name = state['run_id'] + '-placement'
        policy = {'name': policy_name, 'region': region, 'description': json.dumps({'run': state['run_id'], 'source': state['source_sha256'], 'expiry': state['deadline']}, sort_keys=True), 'uncertain': True}
        policy_body = {'name': policy_name, 'description': policy['description'], 'groupPlacementPolicy': {'collocation': 'COLLOCATED', 'maxDistance': config['max_distance']}}
        policy.update(create_path=f'beta/regions/{region}/resourcePolicies', create_body=policy_body, create_request_id=str(uuid.uuid4()), scope=f'regions/{region}')
        state['policy'] = policy; state['instances'] = []; save(state_path, state)
        active = policy
        creating = True
        try:
            policy_op = mutation(api, 'POST', policy['create_path'], policy_body, policy['create_request_id'], policy['scope'])
            creating = False
            policy['id'] = str(policy_op['targetId']); policy['uncertain'] = False; save(state_path, state)
            actual = api.request('GET', f'beta/regions/{region}/resourcePolicies/{policy_name}')
            if str(actual.get('id')) != policy['id'] or actual.get('description') != policy['description'] or actual.get('groupPlacementPolicy', {}).get('maxDistance') != config['max_distance']:
                raise RuntimeError('placement identity or maxDistance verification failed')
            for i in range(6):
                if parse_time(state['deadline']) <= dt.datetime.now(dt.timezone.utc):
                    raise RuntimeError('campaign deadline reached before instance creation')
                name = f'{state["run_id"]}-{i}'
                if read_optional(api, f'zones/{zone}/disks/{name}') is not None:
                    raise RuntimeError('boot disk name already exists: ' + name)
                rec = {'name': name, 'zone': zone, 'uncertain': True, 'disk_token': uuid.uuid4().hex}
                rec.update(create_path=f'zones/{zone}/instances', create_body=instance_body(config, candidate, state, i, startup, rec['disk_token']), create_request_id=str(uuid.uuid4()), scope=f'zones/{zone}')
                state['instances'].append(rec); active = rec; save(state_path, state)
                creating = True
                op = mutation(api, 'POST', rec['create_path'], rec['create_body'], rec['create_request_id'], rec['scope'])
                creating = False
                rec['id'] = str(op['targetId']); rec['uncertain'] = False; save(state_path, state)
                vm = api.request('GET', f'zones/{zone}/instances/{rec["name"]}')
                if not owned_instance(vm, state, rec) or not has_delete_expiry(vm, state['deadline']):
                    raise RuntimeError('instance identity or DELETE expiry verification failed')
                capture_boot_disk(api, state, rec, vm, state_path)
                nics = vm['networkInterfaces']
                fleet.append({'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': rec['name'], 'id': rec['id'], 'zone': zone, 'internal_ip': nics[0]['networkIP'], 'rdma_ip': nics[1]['networkIP']})
            break
        except Exception as exc:
            is_capacity = creating and capacity_failure(exc)
            if isinstance(exc, OperationFailure) or is_capacity or (creating and isinstance(exc, MutationRejected) and exc.status is not None and 400 <= exc.status < 500 and exc.status != 429):
                active['uncertain'] = False
                save(state_path, state)
            cleanup(api, state, state_path)
            if not is_capacity:
                raise
            fleet = []
    if len(fleet) != 6:
        raise RuntimeError('no candidate had sufficient capacity')
    campaign_deadline = (parse_time(state['deadline']) - dt.timedelta(seconds=120)).isoformat()
    runtime = build_runtime_config(config, fleet, receipt, spec, state['deadline'], run_id=state['run_id'], campaign_deadline_utc=campaign_deadline)
    store.put(prefix + 'runtime.json', json_bytes(runtime), content_type='application/json', if_generation_match=0)
    return fleet, runtime, receipt


# A full 273-case campaign can produce many individual owner RPC receipts.
# Allow over 360 objects per case as well as the suite-level archives.
MAX_RESULT_OBJECTS = 100_000
MAX_RESULT_OBJECT_BYTES = 32 * 1024**3
MAX_RESULT_TOTAL_BYTES = 256 * 1024**3


def collect_results(store, run_id, output):
    prefix = run_id + '/results/'
    inventory = []
    total = 0
    try:
        for meta in store.list(prefix):
            if len(inventory) >= MAX_RESULT_OBJECTS:
                raise ValueError('result object count exceeds bound')
            key = meta['name']
            if not key.startswith(prefix):
                raise ValueError('result object outside prefix')
            relative = key[len(prefix):]
            parts = relative.split('/')
            if not relative or any(p in ('', '.', '..') for p in parts):
                raise ValueError('unsafe result object name')
            if not re.fullmatch(r'[0-9a-f]{64}', meta.get('metadata', {}).get('sha256', '')):
                raise ValueError('result object lacks SHA256 metadata')
            size = int(meta['size'])
            if size < 0 or size > MAX_RESULT_OBJECT_BYTES:
                raise ValueError('result object size exceeds bound')
            if size > MAX_RESULT_TOTAL_BYTES - total:
                raise ValueError('result aggregate size exceeds bound')
            target = Path(output, 'results', *parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            store.download(key, target, generation=meta['generation'],
                           max_bytes=min(MAX_RESULT_OBJECT_BYTES, MAX_RESULT_TOTAL_BYTES - total))
            if target.stat().st_size != size:
                raise ValueError('result object size mismatch')
            digest = file_sha256(target)
            if digest != meta['metadata']['sha256']:
                raise ValueError('result checksum mismatch')
            inventory.append({'key': key, 'generation': meta['generation'], 'size': size, 'sha256': digest})
            total += size
    finally:
        Path(output, 'result-inventory.json').write_bytes(json_bytes(inventory))
    return inventory


def coordinator_timeout(deadline, export_reserve_seconds, now):
    # Leave a minute for the terminal receipt after the campaign's own deadline.
    available = int((parse_time(deadline) - now).total_seconds()) - 60
    if available <= export_reserve_seconds + 60:
        raise RuntimeError('insufficient time remains for workload and export')
    return available


def run_campaign(api, config, budget, state, state_path, receipt, spec, output, lease=None):
    from gcp_runtime import RemoteMailbox
    workload_error = None
    try:
        fleet, runtime, _ = launch(api, config, budget, state, state_path, receipt, spec, output)
        root = runtime['package_root']
        runtime_path = f'/mnt/tidehunter/{state["run_id"]}/runtime.json'
        remaining = coordinator_timeout(state['deadline'], spec.get('export_reserve_seconds', 600), dt.datetime.now(dt.timezone.utc))
        mailbox = RemoteMailbox(api, config['bucket'], state['run_id'], fleet, state['source_sha256'], state['deadline'])
        reference = mailbox.submit(0, [GUEST_PYTHON, root + '/source/experiments/gcp-rdma/gcp_runtime.py', 'execute', runtime_path], operation_id='campaign', timeout_seconds=remaining, cwd=root)
        terminal = mailbox.wait(reference, timeout_seconds=remaining)
        Path(output, 'campaign-receipt.json').write_bytes(json_bytes(terminal))
        if terminal.get('exit_code') != 0:
            raise RuntimeError('coordinator campaign failed; see campaign-receipt.json')
    except BaseException as exc:
        workload_error = exc
    try:
        inventory = collect_results(api.objects(config['bucket']), state['run_id'], output)
        if workload_error is None:
            required = set()
            if spec.get('suite') in ('kv', 'both'):
                required.update(('controller.tar.gz', 'controller-inventory.json', 'receipt.json'))
            if spec.get('suite') in ('sui', 'both'):
                required.add('sui-results.tar.gz')
            if not required or not required.issubset({x['key'].removeprefix(state['run_id'] + '/results/') for x in inventory}):
                workload_error = RuntimeError('coordinator returned success without complete suite exports')
    except Exception as exc:
        if workload_error is None:
            workload_error = exc
        else:
            Path(output, 'export-error.txt').write_text(str(exc))
    finally:
        cleanup(api, state, state_path)
        if lease is not None:
            state['cleanup_verified'] = True
            save(state_path, state)
            gcp_lease.release(api.objects(config['bucket']), *lease)
    if workload_error is not None:
        raise workload_error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('plan', 'preflight', 'run', 'cleanup', 'recover-cleanup'))
    parser.add_argument('--terminal-workflow', action='store_true')
    for flag in ('config', 'run-id', 'output-dir'):
        parser.add_argument('--' + flag, required=True)
    for flag in ('budget', 'source-sha', 'package-receipt', 'spec', 'deadline', 'state'):
        parser.add_argument('--' + flag)
    parser.add_argument('--activate', action='store_true')
    args = parser.parse_args(argv)
    config = json.loads(Path(args.config).read_text())
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    if args.command in ('cleanup', 'recover-cleanup'):
        if config.get('project_id') != 'mysten-research' or not config.get('bucket') or not re.fullmatch(r'bench-[0-9a-f]{16}', args.run_id):
            raise ValueError('recovery requires configured project, bucket, and exact run ID')
        api = GoogleApi(config['project_id'])
        try:
            state_path, state = StateFile.recover(args.state or output / 'state.json', api.objects(config['bucket']), args.run_id)
        except ApiError as exc:
            if exc.status != 404:
                raise
            Path(output, 'cleanup-receipt.json').write_bytes(json_bytes({'run_id': args.run_id, 'result': 'no persisted fleet state'}))
            return
        if state.get('run_id') != args.run_id or state.get('project_id') != config['project_id']:
            raise ValueError('persisted state identity mismatch')
        if args.command == 'cleanup' and (state['source_sha256'], state['deadline']) != (args.source_sha, args.deadline):
            raise ValueError('cleanup arguments do not match persisted identity')
        store = api.objects(config['bucket'])
        current = gcp_lease.read(store)
        if state.get('cleanup_verified') is True and (current is None or current[0].get('run_id') != args.run_id):
            Path(output, 'cleanup-receipt.json').write_bytes(json_bytes({'run_id': args.run_id, 'result': 'owned resources absent'}))
            return
        if parse_time(state['deadline']) > dt.datetime.now(dt.timezone.utc):
            trusted = (args.terminal_workflow and os.environ.get('GITHUB_ACTIONS') == 'true'
                       and os.environ.get('GITHUB_REF') == 'refs/heads/bench/gcp-rdma'
                       and os.environ.get('GITHUB_JOB') == 'cleanup')
            run = os.environ.get('GITHUB_RUN_ID', '')
            attempt = os.environ.get('GITHUB_RUN_ATTEMPT', '')
            derived = 'bench-' + hashlib.sha256((run + ':' + attempt).encode()).hexdigest()[:16]
            if not trusted or not run.isdecimal() or not attempt.isdecimal() or derived != args.run_id:
                raise RuntimeError('early recovery requires the trusted terminal cleanup job for this exact workflow run')
        if current is None and not state.get('instances') and not state.get('policy'):
            Path(output, 'cleanup-receipt.json').write_bytes(json_bytes({'run_id': args.run_id, 'result': 'lease was never acquired; no fleet created'}))
            return
        lease = gcp_lease.owned(store, state)
        cleanup(api, state, state_path)
        state['cleanup_verified'] = True
        save(state_path, state)
        gcp_lease.release(store, *lease)
        Path(output, 'cleanup-receipt.json').write_bytes(json_bytes({'run_id': args.run_id, 'result': 'owned resources absent'}))
        return
    for field in ('budget', 'source_sha', 'package_receipt', 'spec', 'deadline', 'state'):
        if not getattr(args, field):
            parser.error('--' + field.replace('_', '-') + ' is required for ' + args.command)
    budget = json.loads(Path(args.budget).read_text())
    validate(config, budget, args.run_id, args.source_sha, args.deadline)
    receipt = json.loads(Path(args.package_receipt).read_text())
    spec = json.loads(Path(args.spec).read_text())
    if receipt.get('source_sha256') != args.source_sha:
        raise ValueError('source identity differs from package receipt')
    plan = {'project_id': config['project_id'], 'run_id': args.run_id, 'source_sha256': args.source_sha, 'deadline': args.deadline, 'hosts': 6, 'machine_type': config['machine_type'], 'candidates': config['candidates'], 'upper_bound_cents': budget['run_upper_bound_cents'], 'estimate_note': budget['quote_description']}
    Path(output, 'plan.json').write_bytes(json_bytes(plan))
    if args.command == 'plan':
        return
    api = GoogleApi(config['project_id'])
    preflight(api, config, receipt, spec)
    if args.command == 'preflight':
        return
    if not args.activate:
        raise ValueError('run requires explicit --activate')
    admit()
    if os.environ.get('GITHUB_EVENT_NAME') not in ('workflow_dispatch', 'push'):
        raise ValueError('allocation requires a trusted push or workflow_dispatch')
    if receipt.get('source_revision') != os.environ.get('GITHUB_SHA'):
        raise ValueError('package revision differs from trusted workflow commit')
    if Path(args.state).exists():
        raise ValueError('refusing to overwrite existing run state')
    state_path = StateFile(args.state, api.objects(config['bucket']), args.run_id)
    state = {'version': 1, 'project_id': config['project_id'], 'run_id': args.run_id, 'source_sha256': args.source_sha, 'deadline': args.deadline, 'instances': []}
    save(state_path, state)
    lease = gcp_lease.acquire(api.objects(config['bucket']), state)
    run_campaign(api, config, budget, state, state_path, receipt, spec, output, lease)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'gcp campaign: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
