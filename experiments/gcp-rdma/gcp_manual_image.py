"""Source-bound capture and recovery of a manual campaign's owned image.

Compute image deletion is name-based: the provider exposes no atomic numeric-ID
precondition. The ID and ownership are checked immediately before deletion,
but a concurrent malicious replacement between describe and delete remains a
TOCTOU risk. An exclusive lease and unpredictable names are required.
"""
import asyncio
import json
import re
import time
import uuid

import gcp_campaign


def _json(result):
    if isinstance(result, dict):
        code = result.get('exit_code')
        if code is not None and code != 0:
            raise RuntimeError('gcloud command failed')
        text = result.get('stdout', result.get('text'))
    else:
        text = result
    if not isinstance(text, str):
        raise RuntimeError('gcloud response lacks JSON output')
    try:
        return json.loads(text)
    except ValueError as exc:
        raise RuntimeError('gcloud outcome is unknown: expected JSON') from exc


def _scope(config):
    if config.get('project_id') != 'mysten-research':
        raise ValueError('exact reviewed project required')
    return ['--project=mysten-research', '--quiet', '--format=json']


async def _list(execute, config, kind, name, zone=None):
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', name):
        raise ValueError('invalid exact resource name')
    args = ['compute', kind, 'list', '--filter=name=' + name, '--limit=2']
    if kind == 'images':
        args.append('--no-standard-images')
    if zone:
        args.append('--zones=' + zone)
    values = _json(await asyncio.wait_for(execute(args + _scope(config)), timeout=30))
    if not isinstance(values, list) or len(values) > 1:
        raise RuntimeError('resource list is ambiguous')
    if values and (not isinstance(values[0], dict) or values[0].get('name') != name):
        raise RuntimeError('resource list did not return the exact name')
    if values and zone and not str(values[0].get('zone', '')).endswith('/' + zone):
        raise RuntimeError('resource list returned an unexpected zone')
    return values[0] if values else None


def _valid_instance(instance, bake_state, vm):
    if instance is None or not gcp_campaign.owned_instance(instance, bake_state, vm):
        return False
    metadata = {item.get('key'): item.get('value') for item in instance.get('metadata', {}).get('items', [])}
    return metadata.get('bench-owner') == bake_state.get('owner_nonce')


def _valid_disk(disk, bake_state, vm):
    return (disk is not None and gcp_campaign.owned_disk(disk, bake_state, vm)
            and str(disk.get('id')) == str(vm.get('disk_id')))


def _valid_image(image, state, record):
    return (isinstance(image, dict) and image.get('name') == record['name']
            and str(image.get('id', '')).isdigit() and int(image['id']) > 0
            and image.get('description') == record['description']
            and image.get('labels', {}).get('bench-run') == state['run_id']
            and image.get('labels', {}).get('bench-source') == state['source_sha256'][:63]
            and image.get('labels', {}).get('bench-owner') == state.get('owner_nonce')
            and str(image.get('sourceDiskId', '')) == record['source_disk_id']
            and str(image.get('sourceDisk', '')).endswith('/zones/' + record['zone'] + '/disks/' + record['disk_name'])
            and str(image.get('selfLink', '')).endswith('/projects/mysten-research/global/images/' + record['name']))


def _limit(deadline):
    timestamp = gcp_campaign.parse_time(deadline).timestamp()
    if not time.time() < timestamp:
        raise TimeoutError('bake deadline expired')
    return timestamp


async def capture_image(config, state, state_path, execute, bake_state, vm_record):
    """Stop the admitted baker, capture its disk, and persist the exact image ID."""
    _scope(config)
    if (not re.fullmatch(r'bench-[0-9a-f]{16}', state.get('run_id', ''))
            or not re.fullmatch(r'[0-9a-f]{64}', state.get('source_sha256', ''))
            or not re.fullmatch(r'[0-9a-f]{32}', state.get('owner_nonce', ''))
            or state.get('source_sha256') != bake_state.get('source_sha256')
            or vm_record not in bake_state.get('instances', [])
            or vm_record.get('zone') != 'us-central1-a'
            or not re.fullmatch(r'[1-9][0-9]*', str(vm_record.get('id', '')))
            or not re.fullmatch(r'[1-9][0-9]*', str(vm_record.get('disk_id', '')))
            or state.get('image')):
        raise ValueError('exact admitted baker and unused parent image identity required')
    deadline = _limit(bake_state['deadline'])
    name, zone = vm_record['name'], vm_record['zone']
    vm = await _list(execute, config, 'instances', name, zone)
    disk = await _list(execute, config, 'disks', name, zone)
    if not _valid_instance(vm, bake_state, vm_record) or not _valid_disk(disk, bake_state, vm_record):
        raise RuntimeError('baker or boot disk identity changed')
    if not gcp_campaign.has_delete_expiry(vm, bake_state['deadline']):
        raise RuntimeError('baker DELETE deadline not verified')
    state['image_stop'] = {'vm_id': str(vm_record['id']), 'zone': zone, 'name': name, 'requested': True}
    gcp_campaign.save(state_path, state)
    # A lost result is not success. Poll exact VM state; never retry this mutation.
    remaining = deadline - time.time()
    if remaining <= 0:
        raise TimeoutError('bake deadline expired before stop request')
    try:
        await asyncio.wait_for(execute(['compute', 'instances', 'stop', name, '--zone=' + zone,
                                        '--discard-local-ssd=True', '--async'] + _scope(config)),
                               timeout=remaining)
    except Exception:
        pass
    while time.time() < deadline:
        vm = await _list(execute, config, 'instances', name, zone)
        if not _valid_instance(vm, bake_state, vm_record):
            raise RuntimeError('baker identity changed while stopping')
        if vm.get('status') == 'TERMINATED':
            break
        await asyncio.sleep(min(2, max(0, deadline - time.time())))
    else:
        raise TimeoutError('baker stop did not complete before expiry')
    disk = await _list(execute, config, 'disks', name, zone)
    if not _valid_disk(disk, bake_state, vm_record):
        raise RuntimeError('baker disk identity changed after stop')
    image_name = state['run_id'] + '-image'
    if await _list(execute, config, 'images', image_name) is not None:
        raise RuntimeError('image name already exists')
    description = json.dumps({'run': state['run_id'], 'source': state['source_sha256'],
                              'baker_id': str(vm_record['id']), 'disk_id': str(vm_record['disk_id']),
                              'owner': state['owner_nonce'], 'token': uuid.uuid4().hex}, sort_keys=True)
    record = {'name': image_name, 'description': description, 'zone': zone, 'disk_name': name,
              'source_disk_id': str(vm_record['disk_id']), 'uncertain': True}
    state['image'] = record
    gcp_campaign.save(state_path, state)
    args = ['compute', 'images', 'create', image_name, '--source-disk=' + name,
            '--source-disk-zone=' + zone, '--source-disk-project=mysten-research',
            '--description=' + description,
            '--labels=bench-run=' + state['run_id'] + ',bench-source=' + state['source_sha256'][:63] + ',bench-owner=' + state['owner_nonce'],
            '--guest-os-features=GVNIC,IDPF,UEFI_COMPATIBLE', '--storage-location=us-central1']
    remaining = deadline - time.time()
    if remaining <= 0:
        record['uncertain'] = False
        record['not_created'] = True
        gcp_campaign.save(state_path, state)
        raise TimeoutError('bake deadline expired before image creation')
    response = None
    try:
        response = await asyncio.wait_for(execute(args + _scope(config)), timeout=remaining)
    except Exception:
        pass
    if isinstance(response, dict):
        error = response.get('error')
        if (response.get('status') == 409 or response.get('status_code') == 409
                or isinstance(error, dict) and error.get('code') == 409):
            record['conflict'] = True
            gcp_campaign.save(state_path, state)
            raise RuntimeError('image creation collided with an existing name')
    while time.time() < deadline:
        image = await _list(execute, config, 'images', image_name)
        if image is not None:
            if not _valid_image(image, state, record):
                raise RuntimeError('image name occupied by foreign or mismatched resource')
            record['id'] = str(image['id'])
            record['uncertain'] = False
            gcp_campaign.save(state_path, state)
            if image.get('status') == 'READY':
                return {'path': 'projects/mysten-research/global/images/' + image_name,
                        'id': record['id']}
        await asyncio.sleep(min(2, max(0, deadline - time.time())))
    raise TimeoutError('image did not become READY before baker deadline')


async def cleanup_image(config, state, state_path, execute):
    """Delete only the exact image proven to belong to this run, then verify absence."""
    record = state.get('image')
    if not record:
        return
    if (record.get('name') != state.get('run_id', '') + '-image'
            or not re.fullmatch(r'bench-[0-9a-f]{16}', state.get('run_id', ''))
            or not re.fullmatch(r'[0-9a-f]{64}', state.get('source_sha256', ''))
            or not re.fullmatch(r'[0-9a-f]{32}', state.get('owner_nonce', ''))):
        raise RuntimeError('invalid image state identity')
    if record.get('conflict'):
        raise RuntimeError('refusing cleanup of conflicting image name')
    image = await _list(execute, config, 'images', record['name'])
    if image is None:
        if record.get('uncertain'):
            raise RuntimeError('uncertain image creation: absence cannot prove operation finished')
        record['deleted'] = True
        gcp_campaign.save(state_path, state)
        return
    if not _valid_image(image, state, record):
        raise RuntimeError('refusing to delete an unowned image')
    if record.get('id') and str(image['id']) != str(record['id']):
        raise RuntimeError('refusing to delete a replacement image')
    record['id'] = str(image['id'])
    record['uncertain'] = False
    present, previous = 'delete_requested' in record, record.get('delete_requested')
    record['delete_requested'] = True
    try:
        gcp_campaign.save(state_path, state)
    except BaseException:
        if present:
            record['delete_requested'] = previous
        else:
            record.pop('delete_requested', None)
        raise
    try:
        await asyncio.wait_for(execute(['compute', 'images', 'delete', record['name']] + _scope(config)), timeout=30)
    except Exception:
        pass
    remaining = await _list(execute, config, 'images', record['name'])
    if remaining is not None:
        if str(remaining.get('id')) != record['id']:
            raise RuntimeError('image name was replaced during deletion')
        raise RuntimeError('image deletion was not verified')
    record['deleted'] = True
    gcp_campaign.save(state_path, state)
