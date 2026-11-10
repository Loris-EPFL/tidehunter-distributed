"""Fail-closed guest poweroff after terminal evidence is durable in GCS."""
import hashlib
import json
import re
import time

import gcp_api
import gcp_runtime

GRACE_SECONDS = 600
HARD_CAP_SECONDS = 3600
POLL_SECONDS = 30


def _read(store, key, limit=4 * 1024 * 1024):
    meta = store.head(key)
    generation = str(meta.get('generation', ''))
    digest = meta.get('metadata', {}).get('sha256')
    size = int(meta.get('size', -1))
    if (meta.get('name') != key or not re.fullmatch(r'[1-9][0-9]*', generation)
            or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)
            or size < 0 or size > limit):
        raise ValueError('invalid object identity')
    data = store.get(key, generation=generation, max_bytes=limit)
    if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
        raise ValueError('object checksum mismatch')
    return data, {'key': key, 'generation': generation, 'sha256': digest, 'bytes': size}


def _optional(store, key):
    try:
        return _read(store, key)
    except gcp_api.ApiError as exc:
        if exc.status == 404:
            return None
        raise


def _bound(store, key, size, digest, generation=None):
    meta = store.head(key)
    actual_generation = str(meta.get('generation', ''))
    if (meta.get('name') != key or not re.fullmatch(r'[1-9][0-9]*', actual_generation)
            or type(size) is not int or size <= 0 or int(meta.get('size', -1)) != size
            or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)
            or meta.get('metadata', {}).get('sha256') != digest
            or (generation is not None and str(generation) != actual_generation)):
        raise ValueError('export generation or SHA-256 mismatch')
    return {'key': key, 'generation': actual_generation, 'sha256': digest, 'bytes': size}


def publish_terminal(config, host, store, result, response_key):
    """Publish only after the controller returned and each started suite exported."""
    if host['index'] != 0 or result['exit_code'] == 124 or result['exit_code'] < 0:
        return
    base = 'runs/' + config['run_id'] + '/poweroff/'
    if _optional(store, base + 'terminal.json') is not None:
        return
    response, response_ref = _read(store, response_key)
    if json.loads(response) != result:
        raise ValueError('campaign response mismatch')
    refs = [response_ref]
    owner_suites = {}
    suites = config['spec'].get('suite', 'kv')
    for suite in ('kv', 'sui'):
        if suite not in (('kv', 'sui') if suites == 'both' else (suites,)):
            continue
        started = _optional(store, base + suite + '-started.json')
        if started is None:
            if result['exit_code'] == 0:
                raise ValueError('successful suite missing start record')
            continue
        marker, marker_ref = started
        if json.loads(marker) != {'run_id': config['run_id'], 'source_sha256': config['source_sha256'], 'suite': suite}:
            raise ValueError('suite start identity mismatch')
        refs.append(marker_ref)
        prefix = config['run_id'] + '/results/'
        if suite == 'kv':
            raw, receipt_ref = _read(store, prefix + 'receipt.json')
            receipt = json.loads(raw)
            inventory = receipt.get('inventory')
            if (receipt.get('schema_version') != 2 or receipt.get('status') not in ('completed', 'incomplete')
                    or receipt.get('monitoring_complete') is not True
                    or receipt.get('source_sha256') != config['source_sha256']
                    or receipt.get('binary_blake2b512') != config['package']['binary_blake2b512']
                    or receipt.get('key') != prefix + 'controller.tar.gz'
                    or not isinstance(inventory, dict)
                    or inventory.get('key') != prefix + 'controller-inventory.json'):
                raise ValueError('controller export is not terminal')
            refs.extend([receipt_ref, _bound(store, receipt['key'], receipt.get('bytes'), receipt.get('sha256')),
                         _bound(store, inventory['key'], inventory.get('bytes'), inventory.get('sha256'), inventory.get('generation'))])
            owner_suites['kv'] = [receipt_ref['key'], receipt['key'], inventory['key']]
        else:
            if result['exit_code'] != 0:
                raise ValueError('failed Sui run has no complete owner export proof')
            key = prefix + 'sui-results.tar.gz'
            meta = store.head(key)
            refs.append(_bound(store, key, int(meta.get('size', -1)), meta.get('metadata', {}).get('sha256')))
            owner_suites['sui'] = [key]
    owners = {str(owner['id']): {'status': 'exported' if owner_suites else 'not_required',
                                  'suites': dict(owner_suites)} for owner in config['fleet'][1:]}
    terminal = {'schema_version': 1, 'run_id': config['run_id'], 'source_sha256': config['source_sha256'],
                'coordinator_id': str(host['id']), 'status': 'completed' if result['exit_code'] == 0 else 'failed',
                'terminal_epoch': time.time(), 'exports': refs, 'owners': owners}
    gcp_runtime._put_once(store, base + 'terminal.json', gcp_runtime._encode(terminal))


def decision(*, terminal_epoch, now, grace, hard_cap, acknowledgement, active=False):
    if (type(terminal_epoch) not in (int, float) or not 0 < terminal_epoch <= now
            or type(grace) is not int or type(hard_cap) is not int or grace < 0 or hard_cap < grace):
        return 'unknown'
    if active:
        return 'active_requests'
    if now < terminal_epoch + grace:
        return 'grace'
    if acknowledgement is None:
        return 'poweroff_no_acknowledgement'
    if not isinstance(acknowledgement, dict) or acknowledgement.get('action') != 'operator_has_taken_over':
        return 'unknown'
    cap = terminal_epoch + hard_cap
    if 'extend_until_utc' in acknowledgement:
        try:
            cap = max(cap, gcp_runtime._deadline(acknowledgement['extend_until_utc']))
        except (ValueError, TypeError, gcp_runtime.RuntimeSafetyError):
            return 'unknown'
    return 'poweroff_hard_cap' if now >= cap else 'operator_takeover'


def evaluate(config, host, store, *, active=False, now=None):
    now = time.time() if now is None else now
    base = 'runs/' + config['run_id'] + '/poweroff/'
    record = _optional(store, base + 'terminal.json')
    if record is None:
        log_decision(config, host, store, now, 'no_terminal')
        return 'no_terminal'
    raw, terminal_ref = record
    terminal = json.loads(raw)
    if (terminal.get('schema_version') != 1 or terminal.get('run_id') != config['run_id']
            or terminal.get('source_sha256') != config['source_sha256']
            or terminal.get('coordinator_id') != str(config['fleet'][0]['id'])
            or terminal.get('status') not in ('completed', 'failed')
            or not isinstance(terminal.get('owners'), dict)
            or set(terminal['owners']) != {str(owner['id']) for owner in config['fleet'][1:]}
            or not isinstance(terminal.get('exports'), list) or not terminal['exports']):
        raise ValueError('invalid terminal evidence')
    for ref in terminal['exports']:
        _bound(store, ref['key'], ref['bytes'], ref['sha256'], ref['generation'])
    if str(host['id']) != str(config['fleet'][0]['id']):
        owner = terminal['owners'][str(host['id'])]
        if (not isinstance(owner, dict) or owner.get('status') not in ('exported', 'not_required')
                or not isinstance(owner.get('suites'), dict)
                or (owner['status'] == 'not_required') != (not owner['suites'])):
            raise ValueError('invalid owner export provenance')
        export_keys = {ref['key'] for ref in terminal['exports']}
        for suite, keys in owner['suites'].items():
            if (suite not in ('kv', 'sui') or not isinstance(keys, list) or not keys
                    or any(key not in export_keys for key in keys)):
                raise ValueError('owner export evidence not bound')
    ack_record = _optional(store, base + 'operator.json')
    ack = None
    if ack_record is not None:
        ack = json.loads(ack_record[0])
        if (not isinstance(ack, dict) or ack.get('schema_version') != 1
                or ack.get('run_id') != config['run_id'] or ack.get('source_sha256') != config['source_sha256']):
            raise ValueError('invalid operator acknowledgement')
    grace = config.get('poweroff_grace_seconds', GRACE_SECONDS)
    hard_cap = config.get('poweroff_hard_cap_seconds', HARD_CAP_SECONDS)
    outcome = decision(terminal_epoch=terminal.get('terminal_epoch'), now=now, grace=grace,
                       hard_cap=hard_cap, acknowledgement=ack, active=active)
    log = {'schema_version': 1, 'run_id': config['run_id'], 'instance_id': str(host['id']),
           'observed_epoch': now, 'decision': outcome, 'terminal': terminal_ref,
           'grace_seconds': grace, 'hard_cap_seconds': hard_cap,
           'acknowledgement': ack_record[1] if ack_record else None}
    log_decision(config, host, store, now, outcome, log)
    return outcome


def log_decision(config, host, store, now, outcome, details=None):
    record = details or {'schema_version': 1, 'run_id': config['run_id'],
                         'instance_id': str(host['id']), 'observed_epoch': now,
                         'decision': outcome}
    key = ('runs/' + config['run_id'] + '/poweroff/decisions/' + str(host['id'])
           + '/' + str(time.time_ns()) + '.json')
    store.put(key, gcp_runtime._encode(record), content_type='application/json', if_generation_match=0)
