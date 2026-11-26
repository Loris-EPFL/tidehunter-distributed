"""Fail-closed guest poweroff: terminal evidence, idle, and the deadline."""
import datetime
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import gcp_api
import gcp_runtime

GRACE_SECONDS = 600
HARD_CAP_SECONDS = 3600
POLL_SECONDS = 30
STATE_ROOT = '/var/lib/tidehunter-gcp-control'


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
    started = _optional(store, base + 'kv-started.json')
    if started is None:
        if result['exit_code'] == 0:
            raise ValueError('successful suite missing start record')
    else:
        marker, marker_ref = started
        if json.loads(marker) != {'run_id': config['run_id'], 'source_sha256': config['source_sha256'], 'suite': 'kv'}:
            raise ValueError('suite start identity mismatch')
        refs.append(marker_ref)
        prefix = config['run_id'] + '/results/'
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


def idle_window(config):
    value = config.get('idle_stop_seconds', gcp_runtime.IDLE_STOP_SECONDS)
    if (type(value) is not int
            or not gcp_runtime.IDLE_MIN_SECONDS <= value <= gcp_runtime.IDLE_MAX_SECONDS):
        raise ValueError('invalid idle stop window')
    return value


def idle_decision(*, now, last_activity, idle_seconds, active=False, driver=False):
    """Decide one guest's idle outcome from an injected clock.

    A request that is still running (`active`) or a campaign driver process
    (`driver`) keeps the guest up for its whole duration. The mailbox request's
    own timeout bounds a hung request, so this decision cannot wait forever.
    """
    if (type(now) not in (int, float) or type(last_activity) not in (int, float)
            or type(idle_seconds) is not int or idle_seconds <= 0
            or last_activity > now):
        return 'unknown'
    if active:
        return 'active_requests'
    if driver:
        return 'driver_active'
    return 'idle_poweroff' if now - last_activity >= idle_seconds else 'idle_wait'


def heartbeat_key(config):
    return 'runs/' + config['run_id'] + '/poweroff/heartbeat.json'


def publish_heartbeat(config, host, store, now):
    """Record that the coordinator is alive and must not have its owners stop."""
    if str(host.get('id')) != str(config['fleet'][0]['id']):
        return None
    record = {'schema_version': 1, 'run_id': config['run_id'],
              'source_sha256': config['source_sha256'],
              'coordinator_id': str(host['id']), 'observed_epoch': now}
    store.put(heartbeat_key(config), gcp_runtime._encode(record),
              content_type='application/json')
    return record


def heartbeat_epoch(config, store):
    """The coordinator's last heartbeat instant, or None when never published."""
    record = _optional(store, heartbeat_key(config))
    if record is None:
        return None
    value = json.loads(record[0])
    if (not isinstance(value, dict) or value.get('schema_version') != 1
            or value.get('run_id') != config['run_id']
            or value.get('source_sha256') != config['source_sha256']
            or value.get('coordinator_id') != str(config['fleet'][0]['id'])
            or type(value.get('observed_epoch')) not in (int, float)
            or value['observed_epoch'] <= 0):
        raise ValueError('invalid coordinator heartbeat')
    return value['observed_epoch']


def is_coordinator(config, host):
    return str(host.get('id')) == str(config['fleet'][0]['id'])


def driver_process_active(config, *, proc_root='/proc'):
    """True when a coordinator driver script still runs outside the request.

    Only the coordinator scans. Owner process groups start detached (`setsid`)
    and outlive the mailbox request that started them; if they counted, an
    owner whose driver crashed would never stop. Owners follow the coordinator
    heartbeat instead. The coordinator counts only admitted driver scripts
    under `<package_root>/source/`; a leftover workload binary does not keep an
    abandoned fleet up. When the package root is unknown, the guard cannot
    prove absence and keeps the guest up.
    """
    package_root = config.get('package_root')
    if not isinstance(package_root, str) or not package_root:
        return True
    marker = (package_root.rstrip('/') + '/source/').encode()
    me = os.getpid()
    with os.scandir(proc_root) as entries:
        for entry in entries:
            if not entry.name.isdigit() or int(entry.name) == me:
                continue
            try:
                with open(os.path.join(entry.path, 'cmdline'), 'rb') as stream:
                    raw = stream.read(64 * 1024)
            except OSError:
                continue
            if not raw:
                continue
            arguments = [argument for argument in raw.split(b'\0') if argument]
            # The worker and this guard are long-lived services, not drivers.
            if any(argument.endswith(b'/gcp_worker.py')
                   or argument.endswith(b'/gcp_poweroff.py') for argument in arguments):
                continue
            if any(marker in argument for argument in arguments):
                return True
    return False


def evaluate_idle(config, host, store, *, active=False, driver=False,
                  last_activity, now=None):
    """Return the idle outcome and publish the coordinator heartbeat.

    The coordinator publishes its heartbeat while it is not ready to stop. The
    owners stop when that heartbeat is stale, because the owner process groups
    are detached and only the coordinator knows whether more work is coming.
    Owners ignore `driver`: a detached owner process group is not evidence that
    more work will arrive, and counting it would prevent every owner stop.
    """
    now = time.time() if now is None else now
    idle_seconds = idle_window(config)
    if is_coordinator(config, host):
        outcome = idle_decision(now=now, last_activity=last_activity,
                                idle_seconds=idle_seconds, active=active, driver=driver)
        if outcome in ('active_requests', 'driver_active', 'idle_wait'):
            publish_heartbeat(config, host, store, now)
        log_decision(config, host, store, now, outcome, {
            'schema_version': 1, 'run_id': config['run_id'],
            'instance_id': str(host['id']), 'observed_epoch': now,
            'decision': outcome, 'idle_stop_seconds': idle_seconds,
            'last_activity_epoch': last_activity, 'coordinator_heartbeat_epoch': now})
        return outcome
    beat = heartbeat_epoch(config, store)
    if beat is not None and now - beat < idle_seconds:
        outcome = 'active_requests' if active else 'idle_wait'
    elif active:
        outcome = 'active_requests'
    elif beat is None and now - last_activity < idle_seconds:
        outcome = 'idle_wait'
    else:
        outcome = 'idle_poweroff'
    log_decision(config, host, store, now, outcome, {
        'schema_version': 1, 'run_id': config['run_id'],
        'instance_id': str(host['id']), 'observed_epoch': now,
        'decision': outcome, 'idle_stop_seconds': idle_seconds,
        'last_activity_epoch': last_activity, 'coordinator_heartbeat_epoch': beat})
    return outcome


def _metadata(path):
    request = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path,
                                     headers={'Metadata-Flavor': 'Google'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            raise ValueError('untrusted metadata server')
        return response.read(4096).decode().strip()


def identify(config):
    """Resolve this guest from the metadata server against the runtime fleet."""
    project = _metadata('project/project-id')
    number = _metadata('project/numeric-project-id')
    if project != config['project_id'] or number != str(config['project_number']):
        raise ValueError('metadata project differs from the runtime')
    instance_id = _metadata('instance/id')
    name = _metadata('instance/name')
    zone = _metadata('instance/zone').rsplit('/', 1)[-1]
    matches = [item for item in config['fleet'] if str(item['id']) == str(instance_id)]
    if (len(matches) != 1 or matches[0].get('name') != name
            or matches[0].get('zone') != zone):
        raise ValueError('metadata instance differs from the runtime fleet')
    return matches[0]


def _epoch(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed.timestamp()


def local_mailbox_state(config, *, state_root=STATE_ROOT):
    """Read the worker's SQLite state without taking its lock.

    The file mtime is the last request-state write, so it is the local activity
    instant. Rows in `pending` are started or waiting requests; each is bounded
    by the request object's own timeout.
    """
    path = Path(state_root) / config['run_id'] / 'mailbox-state.sqlite3'
    try:
        modified = path.stat().st_mtime
    except OSError:
        return None, []
    pending = []
    try:
        database = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True, timeout=0)
        try:
            pending = [row[0] for row in database.execute(
                "SELECT key FROM requests WHERE status='pending'")]
        finally:
            database.close()
    except sqlite3.Error:
        pass
    return modified, pending


def mailbox_activity(config, host, store, *, now, state_root=STATE_ROOT):
    """Return the last local activity time and the active request deadline.

    A request without a response is active until its admitted timeout expires,
    so a hung request is bounded by its own timeout. The guard reads only the
    pending rows and their request objects, not the whole mailbox.
    """
    last_activity, pending = local_mailbox_state(config, state_root=state_root)
    active_until = None
    for key in pending:
        created = None
        timeout = None
        try:
            meta = store.head(key)
            raw = store.get(key, generation=str(meta['generation']), max_bytes=4 * 1024 * 1024)
            request = json.loads(raw)
            timeout = request.get('timeout_seconds')
            created = _epoch(meta.get('timeCreated')) or _epoch(meta.get('updated'))
        except Exception:
            pass
        if created is None:
            created = last_activity if last_activity is not None else now
        if type(timeout) is not int or timeout < 1:
            timeout = POLL_SECONDS
        until = created + timeout
        active_until = until if active_until is None else max(active_until, until)
    return last_activity, active_until


def poweroff(reason):
    """Stop this guest. The ACPI poweroff leaves the VM TERMINATED."""
    print(f'poweroff: {reason}', file=sys.stderr, flush=True)
    subprocess.run(['/usr/bin/systemctl', 'poweroff'], check=True, timeout=30)


def needs_delete_key(config, host):
    return ('runs/' + config['run_id'] + '/poweroff/needs-delete/'
            + str(host['id']) + '.json')


def publish_needs_delete(config, host, store, reason, now=None):
    """Mark a stopped guest: its boot disk bills until `delete` frees it.

    A guest poweroff clears the scheduled termination time, so the VM and its
    disk have no automatic delete backstop. The marker is advisory and a
    restarting guard may overwrite it, so it is not an immutable object.
    """
    record = {'schema_version': 1, 'run_id': config['run_id'],
              'source_sha256': config['source_sha256'],
              'instance_id': str(host['id']), 'instance_name': host.get('name'),
              'reason': reason, 'observed_epoch': time.time() if now is None else now}
    store.put(needs_delete_key(config, host), gcp_runtime._encode(record),
              content_type='application/json')
    return record


def _poweroff_marked(config, host, store, reason, action):
    """Write the disk-billing marker, then stop even if the write failed."""
    try:
        publish_needs_delete(config, host, store, reason)
    except Exception as exc:
        print(f'needs-delete marker deferred: {type(exc).__name__}: {exc}',
              file=sys.stderr, flush=True)
    action(reason)


def serve_idle(config, *, host=None, api=None, poll_seconds=None, poweroff_action=None,
               clock=time.time, sleep=time.sleep, state_root=STATE_ROOT,
               mode='campaign'):
    """Guest poweroff guard: terminal evidence, idle window, absolute deadline.

    This runs as its own process, not inside the mailbox worker. It survives a
    worker crash and keeps `gcp_worker.py` out of the image recipe hash. It
    reads the mailbox state from the bucket instead of the worker's memory.

    `mode='fleet'` is the qualification fleet's guard. It is started by
    `gcp_qualify_bootstrap` from the same admitted runtime the worker uses, so
    the full validation below still applies; the mode records that this fleet
    has no campaign terminal receipt and must stop on the idle window.
    """
    if mode not in ('campaign', 'fleet'):
        raise ValueError('invalid idle guard mode')
    gcp_runtime.validate_runtime(config)
    host = host or identify(config)
    api = api or gcp_api.GoogleApi(config['project_id'], token_provider=gcp_api.metadata_token)
    store = api.objects(config['bucket'])
    deadline = gcp_runtime._deadline(config['deadline_utc'])
    poll = POLL_SECONDS if poll_seconds is None else poll_seconds
    action = poweroff_action or poweroff
    started = clock()
    while True:
        now = clock()
        if now >= deadline:
            log_decision(config, host, store, now, 'poweroff_deadline')
            _poweroff_marked(config, host, store, 'poweroff_deadline', action)
            return 'poweroff_deadline'
        try:
            newest, active_until = mailbox_activity(
                config, host, store, now=now, state_root=state_root)
        except Exception as exc:
            print(f'idle activity deferred: {type(exc).__name__}: {exc}',
                  file=sys.stderr, flush=True)
            log_decision(config, host, store, now, 'unknown')
            sleep(poll)
            continue
        last_activity = started if newest is None else max(started, newest)
        active = active_until is not None and now < active_until
        terminal = 'unknown'
        try:
            terminal = evaluate(config, host, store, active=active)
        except Exception as exc:
            print(f'poweroff deferred: {type(exc).__name__}: {exc}',
                  file=sys.stderr, flush=True)
            log_decision(config, host, store, now, 'unknown')
        if terminal in ('poweroff_no_acknowledgement', 'poweroff_hard_cap'):
            _poweroff_marked(config, host, store, terminal, action)
            return terminal
        if terminal != 'operator_takeover':
            try:
                driver = (driver_process_active(config)
                          if is_coordinator(config, host) else False)
                idle = evaluate_idle(config, host, store, active=active, driver=driver,
                                     last_activity=last_activity, now=now)
            except Exception as exc:
                print(f'idle poweroff deferred: {type(exc).__name__}: {exc}',
                      file=sys.stderr, flush=True)
                log_decision(config, host, store, now, 'unknown')
            else:
                if idle == 'idle_poweroff':
                    _poweroff_marked(config, host, store, 'poweroff_idle', action)
                    return 'poweroff_idle'
        sleep(poll)


if __name__ == '__main__':
    argv = sys.argv[1:]
    if argv[:1] != ['serve-idle'] or len(argv) not in (2, 4):
        raise SystemExit('usage: gcp_poweroff.py serve-idle RUNTIME_CONFIG '
                         '[--mode campaign|fleet]')
    mode = 'campaign'
    if len(argv) == 4:
        if argv[2] != '--mode' or argv[3] not in ('campaign', 'fleet'):
            raise SystemExit('usage: gcp_poweroff.py serve-idle RUNTIME_CONFIG '
                             '[--mode campaign|fleet]')
        mode = argv[3]
    with open(argv[1], encoding='utf-8') as stream:
        serve_idle(json.load(stream), mode=mode)
