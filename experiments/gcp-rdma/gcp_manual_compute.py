"""Owned, fail-closed Compute lifecycle through the supplied async command transport.

Deletes are by name: even an immediately preceding ID/ownership GET cannot
prevent replacement between that GET and the delete (a residual TOCTOU race).
An uncertain create is never retried and an empty list cannot resolve it.

The module also carries the operator CLI for the reviewed two- and three-host
fleets. ``create --dry-run`` prints the exact request bodies without a call.

Every host boots one identity-bound gate script and nothing else. Once the
fleet identity is admitted, ``create --package`` publishes the sealed worker
package and the runtime, then releases each gate with
``gcp_bootstrap.startup_script``; the guest installs the package read-only and
starts the mailbox worker. ``ready`` waits for every worker to publish its
readiness object before any campaign driver runs.
"""
import argparse
import asyncio
import datetime as dt
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import gcp_api
import gcp_bootstrap
import gcp_budget
import gcp_lease
import gcp_manual_preflight
import mailbox_exec
from gcp_campaign import (BOOT_DISK_GIB, GUEST_SERVICE_ACCOUNT, campaign_mark,
                          campaign_identity, has_delete_expiry, instance_body,
                          owned_disk, owned_instance, parse_time, save)

ZONE = 'us-central1-a'
REGION = 'us-central1'
PROJECT = 'mysten-research'
MCP_CALL_SECONDS = 30
# The operator CLI runs one gcloud process per call; the absolute deadline and
# the per-call wait below still bound it.
GCLOUD_SECONDS = 600
FLEET_HOSTS = (2, 3)
FLEET_ROLES = ('campaign', 'qualification')
MAX_FLEET_HOURS = 6
def startup_script(config, state):
    """The identity-bound gate every host boots with; it runs no workload yet."""
    return mailbox_exec.gate_script(config['bucket'], state)
IMAGE_PATH = re.compile(r'projects/[a-z0-9-]+/global/images/[a-z0-9-]+')
# F7 attempts 4/5: at most 146.140965 s from delete to observed absence.
# Allow 153.859035 s margin; the whole cleanup remains bounded by 600 s.
DELETE_CALL_SECONDS = 300
READ_ATTEMPTS = 3
# Local gcloud transport failures before any response: retrying an idempotent
# read is safe. Denials, invalid replies and every mutation stay fail-closed.
TRANSPORT_FAILURE = re.compile(
    r'gcloud crashed \((?:ConnectionError|SSLError|ProtocolError|ReadTimeout|TransportError)\)'
    r'|Connection aborted|RemoteDisconnected|Connection reset by peer'
    r'|Temporary failure in name resolution|Max retries exceeded')
CLEANUP_SECONDS = 600
CREATE_ABSENCE_ROUNDS = 3
CREATE_ABSENCE_SECONDS = 30
TOCTOU = 'Name-based delete has a residual describe/delete TOCTOU race.'
DEFINITIVE_CREATE_REJECTIONS = frozenset({
    'QUOTA_EXCEEDED', 'ZONE_RESOURCE_POOL_EXHAUSTED',
    'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
})


class ComputeError(RuntimeError):
    pass


class CreateConflict(ComputeError):
    pass


class CommandFailed(ComputeError):
    def __init__(self, response):
        self.response = response
        super().__init__('gcloud command failed: ' + str(response))


class CreateRejected(ComputeError):
    def __init__(self, code):
        self.code = code
        super().__init__('instance create rejected: ' + code)


def _definitive_create_rejection(response):
    """Accept only machine codes in an error JSON object, never error prose."""
    value = response
    if isinstance(value, dict) and 'error' not in value:
        value = value.get('stdout') or value.get('text') or value.get('stderr')
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except (ValueError, UnicodeError):
            return None
    error = value.get('error') if isinstance(value, dict) else None
    if not isinstance(error, dict):
        return None
    codes = [error['code']] if isinstance(error.get('code'), str) else []
    entries = error.get('errors', [])
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get('code'), str):
            return None
        codes.append(entry['code'])
    return codes[0] if codes and all(code in DEFINITIVE_CREATE_REJECTIONS for code in codes) else None


def _id(value):
    value = str(value) if value is not None else ''
    if not value.isascii() or not value.isdecimal() or int(value) <= 0:
        raise ComputeError('missing or invalid numeric resource ID')
    return value


def _end(value, suffix):
    return isinstance(value, str) and (value == suffix or value.endswith('/' + suffix))


def _transport_failure(response):
    """A nonzero gcloud exit whose diagnostic names a local transport crash."""
    if not isinstance(response, dict) or response.get('exit_code', 0) in (0, None):
        return False
    diagnostic = ' '.join(str(response.get(k, '')) for k in ('stderr', 'stdout', 'text'))
    return bool(TRANSPORT_FAILURE.search(diagnostic))


def _json_response(response):
    """Accept MCP text or a structured stdout envelope, never error prose."""
    if isinstance(response, dict):
        error = response.get('error')
        if response.get('status') == 409 or response.get('status_code') == 409 or (
                isinstance(error, dict) and error.get('code') == 409):
            raise CreateConflict('resource name collision')
    if isinstance(response, dict):
        if response.get('isError') or response.get('exit_code', 0) not in (0, None):
            diagnostic = ' '.join(str(response.get(k, '')) for k in ('stderr', 'stdout', 'text'))
            if re.search(r'(?i)\b(?:HTTP\s*409|code[=: ]+409|already exists|alreadyexists)\b', diagnostic):
                raise CreateConflict('resource name collision')
            raise CommandFailed(response)
        if 'stdout' in response:
            response = response['stdout']
        elif 'text' in response:
            response = response['text']
        elif 'content' in response:
            content = response['content']
            if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
                raise ComputeError('unrecognized MCP content')
            response = content[0].get('text')
        else:
            return response
    if isinstance(response, bytes):
        response = response.decode('utf-8')
    if isinstance(response, str):
        try:
            value = json.loads(response)
        except (ValueError, TypeError) as exc:
            if re.search(r'(?i)\b(?:HTTP\s*409|code[=: ]+409|already exists|alreadyexists)\b', response):
                raise CreateConflict('resource name collision') from exc
            raise ComputeError('gcloud did not return JSON') from exc
        if isinstance(value, dict) and isinstance(value.get('error'), dict) and value['error'].get('code') == 409:
            raise CreateConflict('resource name collision')
        return value
    if isinstance(response, list):
        return response
    raise ComputeError('unrecognized gcloud response')


def _dict_flag(values):
    delimiter = 'TIDEHUNTER-DICT-DELIMITER'
    while any(delimiter in str(value) for value in values.values()):
        delimiter += 'X'
    return '^' + delimiter + '^' + delimiter.join(k + '=' + str(v) for k, v in values.items())


def _control_object(name, run_id):
    """One of the run prefix's control objects that a re-create must not inherit.

    The run id is derived from the campaign, so every stage fleet writes the
    same `runs/<run>/` keys. These objects name one attempt's instance ids and
    deadline; a new attempt must start without them.
    """
    prefix = 'runs/' + run_id + '/'
    if not isinstance(name, str) or not name.startswith(prefix):
        return False
    leaf = name[len(prefix):]
    if leaf in ('package.tar', 'package.json', 'runtime.json') or leaf.startswith('transfer/'):
        return True
    parts = leaf.split('/')
    return (len(parts) == 3 and parts[0] == 'qualification'
            and parts[2] in ('worker-ready.json', 'gate.json'))


def fleet_run_id(campaign):
    """Return the stable run identity of one campaign fleet.

    The name is derived, never chosen, so a retry reuses the same host and disk
    names and the cleanup proof can filter by the exact prefix.
    """
    campaign = campaign_identity(campaign)
    if campaign is None:
        raise ValueError('the operator fleet requires a campaign id')
    return 'bench-' + hashlib.sha256(('tidehunter-campaign:' + campaign).encode()).hexdigest()[:16]


def fleet_deadline(hours, now=None):
    """Return the absolute UTC deadline of one fleet, at most six hours ahead."""
    if type(hours) is not int or not 1 <= hours <= MAX_FLEET_HOURS:
        raise ValueError('fleet duration must be one to ' + str(MAX_FLEET_HOURS) + ' whole hours')
    start = now or dt.datetime.now(dt.timezone.utc)
    return (start + dt.timedelta(hours=hours)).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def new_state(config, *, campaign, run_id, deadline, hosts, source_sha256, owner_nonce,
              role='campaign'):
    """Build the persisted fleet identity before any cloud mutation."""
    campaign = campaign_identity(campaign)
    if campaign is None:
        raise ValueError('the operator fleet requires a campaign id')
    if role not in FLEET_ROLES:
        raise ValueError('the fleet role must be campaign or qualification')
    if type(hosts) is not int or hosts not in FLEET_HOSTS:
        raise ValueError('the operator fleet must have two or three hosts')
    if role == 'qualification' and hosts != 2:
        raise ValueError('the qualification fleet must have exactly two hosts')
    if not isinstance(run_id, str) or not re.fullmatch(r'bench-[0-9a-f]{16}', run_id):
        raise ValueError('the fleet run id must match bench-[0-9a-f]{16}')
    if not isinstance(source_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', source_sha256):
        raise ValueError('the fleet source sha256 must be a full lowercase SHA256')
    if not isinstance(owner_nonce, str) or not re.fullmatch(r'[0-9a-f]{32}', owner_nonce):
        raise ValueError('the fleet owner nonce must be 128 random bits')
    parse_time(deadline)
    return {'run_id': run_id, 'campaign': campaign, 'role': role, 'source_sha256': source_sha256,
            'owner_nonce': owner_nonce, 'deadline': deadline, 'host_count': hosts,
            'instances': []}


def launch_spec(config, candidate, state, index, image, startup, disk_token, policy):
    """Return the reviewed insert body and the exact gcloud command for one host.

    Both come from this function so the printed body and the executed command
    cannot describe different resources.
    """
    if not isinstance(image, str) or not IMAGE_PATH.fullmatch(image):
        raise ValueError('a pinned image path is required')
    if type(index) is not int or not 0 <= index < state.get('host_count', 0):
        raise ValueError('invalid VM index')
    if not isinstance(startup, str) or not startup:
        raise ValueError('a startup script is required')
    if parse_time(state['deadline']) <= dt.datetime.now(dt.timezone.utc):
        raise ComputeError('absolute deadline already passed')
    labels, metadata = campaign_mark(state)
    metadata['bench-owner'] = state['owner_nonce']
    body = instance_body(dict(config, image=image), candidate, state, index, startup, disk_token,
                         extra_labels=labels, extra_metadata=metadata)
    disk, params = body['disks'][0], body['disks'][0]['initializeParams']
    command = ['compute', 'instances', 'create', body['name'], '--zone=' + ZONE,
               '--machine-type=' + body['machineType'].rsplit('/', 1)[-1],
               '--create-disk=' + _dict_flag({
                   'name': params['diskName'], 'description': params['description'],
                   'image': params['sourceImage'], 'size': params['diskSizeGb'] + 'GB',
                   'type': params['diskType'].rsplit('/', 1)[-1], 'interface': disk['interface'],
                   'boot': 'yes' if disk['boot'] else 'no',
                   'auto-delete': 'yes' if disk['autoDelete'] else 'no'})]
    for nic in body['networkInterfaces']:
        command.append('--network-interface=nic-type=' + nic['nicType'] + ',subnet=' + nic['subnetwork']
                       + ('' if nic.get('accessConfigs') else ',no-address'))
    command.append('--resource-policies=' + body['resourcePolicies'][0].rsplit('/', 1)[-1])
    account = body['serviceAccounts'][0]
    command.extend(['--service-account=' + account['email'],
                    '--scopes=' + account['scopes'][0].rsplit('/', 1)[-1],
                    '--labels=' + ','.join(key + '=' + value for key, value in body['labels'].items()),
                    '--metadata=' + _dict_flag({item['key']: item['value'] for item in body['metadata']['items']})])
    scheduling = body['scheduling']
    command.extend(['--termination-time=' + scheduling['terminationTime'],
                    '--instance-termination-action=' + scheduling['instanceTerminationAction'],
                    '--maintenance-policy=' + scheduling['onHostMaintenance']])
    if not scheduling['automaticRestart']:
        command.append('--no-restart-on-failure')
    command.append('--async')
    if body['resourcePolicies'] != ['regions/' + REGION + '/resourcePolicies/' + policy]:
        raise ComputeError('launch body does not carry the reviewed placement policy')
    return command, body


def policy_body(state):
    """Return the reviewed placement-policy body of one fleet."""
    description = json.dumps({'run': state['run_id'], 'source': state['source_sha256'],
                              'expiry': state['deadline'], 'owner': state['owner_nonce']}, sort_keys=True)
    return {'name': state['run_id'] + '-placement', 'description': description,
            'groupPlacementPolicy': {'collocation': 'COLLOCATED', 'maxDistance': 3}}


def admit_ready_image(config, image, api=None):
    """Admit one READY custom image of the configured project and its size."""
    import gcp_api
    if (not isinstance(image, str) or not IMAGE_PATH.fullmatch(image)
            or not image.startswith('projects/' + config.get('project_id', '') + '/global/images/')):
        raise ValueError('image must be a pinned image of the configured project')
    api = api or gcp_api.campaign_client(config)
    info = api.request('GET', 'global/images/' + image.rsplit('/', 1)[-1])
    if not isinstance(info, dict) or info.get('status') != 'READY' or not str(info.get('id', '')).isdigit():
        raise ValueError('configured image is not READY with a numeric ID')
    if not str(info.get('selfLink', '')).endswith('/' + image):
        raise ValueError('configured image identity mismatch')
    size = str(info.get('diskSizeGb', ''))
    if not size.isascii() or not size.isdecimal() or not 0 < int(size) <= BOOT_DISK_GIB:
        raise ValueError('configured image boot disk exceeds the reviewed size')
    return info


def package_source_sha256(package_dir, explicit=None):
    """The source SHA the fleet binds: the sealed package receipt's own value.

    A fleet created with the build bundle's SHA instead binds every host to an
    identity no guest gate can match, and each worker exits before its first
    cell. Both values are 64-hex, so only the receipt can tell them apart.
    """
    receipt_path = Path(package_dir) / 'package-receipt.json'
    try:
        receipt = json.loads(receipt_path.read_text())
    except (OSError, ValueError) as exc:
        raise ValueError(f'{receipt_path}: package receipt unavailable') from exc
    source = receipt.get('source_sha256') if isinstance(receipt, dict) else None
    if not isinstance(source, str) or re.fullmatch(r'[0-9a-f]{64}', source) is None:
        raise ValueError('the package receipt carries no source SHA-256')
    if explicit is not None and explicit != source:
        raise ValueError(
            f'--source-sha256 {explicit} is not the package receipt source {source}: '
            'the guest gate binds the package receipt source SHA-256')
    return source


def admit_image(config, api=None):
    """Refuse to create a campaign fleet unless the image carries a current receipt."""
    import gcp_api
    import gcp_image
    image = config.get('image', '')
    api = api or gcp_api.campaign_client(config)
    info = admit_ready_image(config, image, api=api)
    qualification = gcp_image.admit_image(api, config, info)
    gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'])
    return {'path': image, 'id': str(info['id'])}


class Compute:
    def __init__(self, config, state, state_path, execute):
        self.config, self.state, self.state_path, self.execute = config, state, state_path, execute
        self._cleanup_until = None
        if (config.get('project_id') != PROJECT or config.get('service_account') != GUEST_SERVICE_ACCOUNT
                or config.get('machine_type', 'z4m-highmem-16') not in {'z4m-highmem-16', 'z4m-highmem-32'} or config.get('max_distance') != 3):
            raise ValueError('unreviewed Compute configuration')
        if not re.fullmatch(r'bench-[0-9a-f]{16}', state.get('run_id', '')):
            raise ValueError('invalid run identity')
        if not re.fullmatch(r'[0-9a-f]{64}', state.get('source_sha256', '')):
            raise ValueError('invalid source identity')
        if not re.fullmatch(r'[0-9a-f]{32}', state.get('owner_nonce', '')):
            raise ValueError('independent 128-bit owner nonce required')
        campaign_identity(state.get('campaign'))
        if state.get('role', 'campaign') not in FLEET_ROLES:
            raise ValueError('invalid persisted fleet role')
        from gcp_runtime import owner_host_counts
        count = state.get('host_count', 6)
        if type(count) is not int or count - 1 not in owner_host_counts():
            raise ValueError('invalid persisted fleet host count')
        self.host_count = count
        state['host_count'] = count
        parse_time(state['deadline'])
        candidates = [c for c in config.get('candidates', []) if c.get('zone') == ZONE]
        if len(candidates) != 1:
            raise ValueError('exactly one us-central1-a candidate is required')
        self.candidate = candidates[0]
        for field in ('ordinary_subnet', 'rdma_subnet'):
            if not re.fullmatch(r'projects/mysten-research/regions/us-central1/subnetworks/[a-z0-9-]+', self.candidate.get(field, '')):
                raise ValueError('invalid candidate subnet')
        if self.candidate['ordinary_subnet'] == self.candidate['rdma_subnet']:
            raise ValueError('separate subnets required')
        state.setdefault('instances', [])

    def _save(self):
        save(self.state_path, self.state)

    def _save_delete_intent(self, record, key):
        present, previous = key in record, record.get(key)
        record[key] = True
        try:
            self._save()
        except BaseException:
            if present:
                record[key] = previous
            else:
                record.pop(key, None)
            raise

    def _live(self):
        if parse_time(self.state['deadline']) <= dt.datetime.now(dt.timezone.utc):
            raise ComputeError('absolute deadline already passed')

    async def _call(self, argv, *, guest=False, read_only=False, deadline=None, allow_empty=False, creation=False, timeout=None):
        args = argv + ['--project=' + PROJECT, '--quiet', '--format=json', '--verbosity=error']
        if guest:
            args.append('--impersonate-service-account=' + GUEST_SERVICE_ACCOUNT)
        for attempt in range(READ_ATTEMPTS if read_only else 1):
            if self._cleanup_until is None:
                remaining = (parse_time(self.state['deadline']) - dt.datetime.now(dt.timezone.utc)).total_seconds()
            else:
                remaining = self._cleanup_until - time.monotonic()
            if deadline is not None:
                remaining = min(remaining, deadline - time.monotonic())
            if remaining <= 0:
                raise ComputeError('MCP call deadline exhausted')
            try:
                response = await asyncio.wait_for(self.execute(args), timeout=min(MCP_CALL_SECONDS if timeout is None else timeout, remaining))
            except (TimeoutError, subprocess.TimeoutExpired) as exc:
                if not read_only or attempt + 1 == READ_ATTEMPTS:
                    raise ComputeError('MCP call timed out; outcome may be unknown') from exc
                continue
            if read_only and _transport_failure(response):
                if attempt + 1 == READ_ATTEMPTS:
                    raise ComputeError('read transport failed after bounded retries')
                # A dropped connection needs time to recover; stay inside the deadline.
                await asyncio.sleep(min(2.0 * (attempt + 1), max(remaining - 1.0, 0.0)))
                continue
            if creation:
                rejection = _definitive_create_rejection(response)
                if rejection is not None:
                    raise CreateRejected(rejection)
            if allow_empty and response in ({'text': ''}, {'stdout': '', 'exit_code': 0}):
                return {}
            return _json_response(response)

    def _resource(self, kind):
        if kind == 'policy':
            return ['beta', 'compute', 'resource-policies'], '--region=' + REGION
        return ['compute', 'instances' if kind == 'instance' else 'disks'], '--zone=' + ZONE

    async def _describe(self, kind, name, *, guest=False, deadline=None):
        prefix, location = self._resource(kind)
        result = await self._call(prefix + ['describe', name, location], guest=guest, read_only=True, deadline=deadline)
        if not isinstance(result, dict) or result.get('name') != name:
            raise ComputeError('invalid ' + kind + ' description')
        _id(result.get('id'))
        return result

    async def _exists(self, kind, name, *, deadline=None):
        prefix, _ = self._resource(kind)
        location = [] if kind == 'policy' else ['--zones=' + ZONE]
        rows = await self._call(prefix + ['list', '--filter=name=' + name] + location, read_only=True, deadline=deadline)
        if not isinstance(rows, list) or any(not isinstance(r, dict) or not isinstance(r.get('name'), str) for r in rows):
            raise ComputeError('invalid ' + kind + ' list response')
        # Full result with no limit: exact names can exist in several regions.
        location_key = 'region' if kind == 'policy' else 'zone'
        target = REGION if kind == 'policy' else ZONE
        matches = []
        for row in rows:
            if row['name'] != name:
                raise ComputeError('unexpected resource in filtered list')
            if not isinstance(row.get(location_key), str):
                raise ComputeError('list response lacks resource location')
            if _end(row[location_key], target):
                matches.append(row)
        if len(matches) > 1:
            raise ComputeError('duplicate resource identity')
        return bool(matches)

    async def _delete(self, kind, name):
        prefix, location = self._resource(kind)
        error = None
        try:
            flags = ['--no-graceful-shutdown'] if kind == 'instance' else []
            await self._call(prefix + ['delete', name, location] + flags,
                             allow_empty=True, timeout=DELETE_CALL_SECONDS)
        except ComputeError as exc:
            # MCP may combine JSON with stderr even when deletion took effect.
            # Preserve uncertainty; exact scoped absence, not this reply, proves cleanup.
            self.state.setdefault('delete_reply_errors', {})[kind + ':' + name] = str(exc)
            self._save()
            error = exc
        await self._observe_absence(kind, name, error)

    async def _observe_absence(self, kind, name, error=None):
        deadline = self._cleanup_until if self._cleanup_until is not None else time.monotonic() + CLEANUP_SECONDS
        while await self._exists(kind, name, deadline=deadline):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ComputeError(kind + ' deletion not verified: ' + str(error or 'still present'))
            await asyncio.sleep(min(2, remaining))

    def _policy_owned(self, obj, rec, *, require_id=True):
        group = obj.get('groupPlacementPolicy', {})
        description = json.dumps({'run': self.state['run_id'], 'source': self.state['source_sha256'],
                                  'expiry': self.state['deadline'], 'owner': self.state['owner_nonce']}, sort_keys=True)
        return (obj.get('name') == rec['name'] and _end(obj.get('region'), REGION)
                and rec.get('description') == description and obj.get('description') == description
                and group.get('collocation') == 'COLLOCATED' and group.get('maxDistance') == 3
                and (not require_id or _id(obj.get('id')) == _id(rec.get('id'))))

    async def create_policy(self):
        self._live()
        if self.state.get('policy'):
            raise ComputeError('policy already recorded; never retry a create')
        name = self.state['run_id'] + '-placement'
        if await self._exists('policy', name):
            raise ComputeError('placement policy name already exists')
        self._live()
        rec = {'name': name, 'region': REGION, 'description': json.dumps(
            {'run': self.state['run_id'], 'source': self.state['source_sha256'],
             'expiry': self.state['deadline'], 'owner': self.state['owner_nonce']}, sort_keys=True), 'uncertain': True}
        self.state['policy'] = rec
        self._save()
        command = ['beta', 'compute', 'resource-policies', 'create', 'group-placement', name,
                   '--region=' + REGION, '--collocation=collocated', '--max-distance=3',
                   '--description=' + rec['description']]
        error = None
        try:
            await self._call(command)
        except CreateConflict:
            rec['conflict'] = True
            self._save()
            raise
        except Exception as exc:
            error = exc
        try:
            obj = await self._describe('policy', name)
            if not self._policy_owned(obj, rec, require_id=False):
                raise ComputeError('placement ownership or maxDistance mismatch')
            rec['id'] = _id(obj['id'])
            rec['uncertain'] = False
            self._save()
            return rec
        except Exception as exc:
            raise ComputeError('placement create unresolved: ' + str(error or exc)) from exc

    def _vm_owned(self, obj, rec, *, require_id=True):
        candidate = dict(rec)
        if not require_id:
            candidate['id'] = _id(obj.get('id'))
        items = obj.get('metadata', {}).get('items', [])
        owners = [item.get('value') for item in items if item.get('key') == 'bench-owner']
        return (owners == [self.state['owner_nonce']] and owned_instance(obj, self.state, candidate)
                and _end(obj.get('zone'), ZONE))

    async def _boot(self, rec, vm, *, guest=False, verify_spec=True):
        disks = [d for d in vm.get('disks', []) if d.get('boot') is True]
        path = 'projects/' + PROJECT + '/zones/' + ZONE + '/disks/' + rec['name']
        if len(disks) != 1 or disks[0].get('autoDelete') is not True or not _end(disks[0].get('source'), path):
            raise ComputeError('boot disk identity or autoDelete mismatch')
        disk = await self._describe('disk', rec['name'], guest=guest)
        if not _end(disk.get('zone'), ZONE) or not owned_disk(disk, self.state, rec):
            raise ComputeError('boot disk ownership mismatch')
        if verify_spec and (
                disks[0].get('interface') != 'NVME' or disks[0].get('mode') != 'READ_WRITE'
                or not _end(disk.get('sourceImage'), rec.get('image', ''))
                or not _end(disk.get('type'), 'zones/' + ZONE + '/diskTypes/hyperdisk-balanced')
                or str(disk.get('sizeGb')) != str(BOOT_DISK_GIB)):
            raise ComputeError('boot disk launch specification mismatch')
        disk_id = _id(disk['id'])
        if rec.get('disk_id') and _id(rec['disk_id']) != disk_id:
            raise ComputeError('boot disk ID changed')
        if not guest:
            rec['disk_id'] = disk_id
            self._save()
        elif disk_id != _id(rec.get('disk_id')):
            raise ComputeError('guest disk ID mismatch')
        return disk

    async def _resolve_failed_create(self, rec):
        """Resolve a synchronous CLI failure by scoped observations, not its prose."""
        target = 'projects/' + PROJECT + '/zones/' + ZONE + '/instances/' + rec['name']
        target_url = 'https://www.googleapis.com/compute/v1/' + target
        end = time.monotonic() + CREATE_ABSENCE_SECONDS
        for round_index in range(CREATE_ABSENCE_ROUNDS):
            try:
                description = await self._call(
                    ['compute', 'instances', 'describe', rec['name'], '--zone=' + ZONE],
                    read_only=True, deadline=end)
            except CommandFailed as exc:
                # gcloud emits describe-notFound as prose even with --format=json.
                # Require the exact queried resource; arbitrary 404 text is not proof.
                try:
                    result = _json_response({'stdout': exc.response.get('stdout', '')})
                except ComputeError:
                    result = None
                error = result.get('error') if isinstance(result, dict) else None
                diagnostic = str(exc.response.get('stderr', ''))
                if not (isinstance(error, dict) and error.get('code') == 404
                        or re.search(r"The resource '" + re.escape(target) + r"' was not found\b", diagnostic)):
                    raise ComputeError('instance absence query failed') from exc
            else:
                error = description.get('error') if isinstance(description, dict) else None
                if not isinstance(error, dict) or error.get('code') != 404:
                    raise ComputeError('instance absence not confirmed')
            operations = await self._call(
                ['compute', 'operations', 'list', '--zones=' + ZONE,
                 '--filter=targetLink=' + target_url + ' AND operationType=insert'],
                read_only=True, deadline=end)
            if not isinstance(operations, list) or len(operations) > 1:
                raise ComputeError('ambiguous create operation list')
            if operations:
                operation = operations[0]
                if (not isinstance(operation, dict)
                        or not re.fullmatch(r'operation-[a-z0-9-]+', str(operation.get('name', '')))
                        or not _end(operation.get('zone'), ZONE)
                        or not _end(operation.get('targetLink'), target)
                        or operation.get('operationType') != 'insert'):
                    raise ComputeError('invalid create operation list')
                rec['operation_name'] = operation['name']
                self._save()
                return
            if round_index + 1 < CREATE_ABSENCE_ROUNDS:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise ComputeError('create absence verification deadline exhausted')
                await asyncio.sleep(min(CREATE_ABSENCE_SECONDS / CREATE_ABSENCE_ROUNDS, remaining))
        # Every round confirmed both absence and no insert. Normal cleanup must
        # still verify VM/disk/policy absence before releasing the lease.
        rec['uncertain'] = False
        rec['create_absence_verified'] = True
        self._save()

    async def _operation_done(self, rec):
        name = rec['operation_name']
        end = time.monotonic() + 180
        while True:
            operation = await self._call(['compute', 'operations', 'describe', name, '--zone=' + ZONE], read_only=True, deadline=end)
            expected = 'projects/' + PROJECT + '/zones/' + ZONE + '/instances/' + rec['name']
            if (not isinstance(operation, dict) or operation.get('name') != name
                    or not _end(operation.get('zone'), ZONE)
                    or not _end(operation.get('targetLink'), expected)
                    or operation.get('status') not in ('PENDING', 'RUNNING', 'DONE')):
                raise ComputeError('invalid or mismatched create operation')
            if operation['status'] == 'DONE':
                rec['operation_done'] = True
                if operation.get('error'):
                    rec['operation_error'] = operation['error']
                    rec['uncertain'] = False
                    self._save()
                    raise ComputeError('instance create operation failed')
                if operation.get('targetId') is not None:
                    rec['operation_target_id'] = _id(operation['targetId'])
                self._save()
                return
            if time.monotonic() >= end or parse_time(self.state['deadline']) <= dt.datetime.now(dt.timezone.utc):
                raise ComputeError('instance create operation still unresolved')
            await asyncio.sleep(2)

    async def _ready_vm(self, rec):
        end = time.monotonic() + 180
        while True:
            self._live()
            if await self._exists('instance', rec['name'], deadline=end):
                vm = await self._describe('instance', rec['name'], deadline=end)
                if not self._vm_owned(vm, rec, require_id=False):
                    raise ComputeError('instance ownership mismatch')
                if rec.get('operation_target_id') and _id(vm['id']) != rec['operation_target_id']:
                    raise ComputeError('operation target ID mismatch')
                self._live()
                if vm.get('status') == 'RUNNING':
                    return vm
                if vm.get('status') not in ('PROVISIONING', 'STAGING'):
                    raise ComputeError('unexpected instance status')
            if time.monotonic() >= end or parse_time(self.state['deadline']) <= dt.datetime.now(dt.timezone.utc):
                raise ComputeError('instance readiness unresolved')
            await asyncio.sleep(2)

    def _fleet(self, vm, rec):
        if not self._vm_owned(vm, rec) or not has_delete_expiry(vm, self.state['deadline']):
            raise ComputeError('instance identity or DELETE expiry mismatch')
        spec = rec.get('launch_spec', {})
        if (spec.get('deadline') != self.state['deadline'] or spec.get('policy') != self.state['policy']['name']
                or spec.get('machine_type', 'z4m-highmem-16') != self.config.get('machine_type', 'z4m-highmem-16')):
            raise ComputeError('persisted launch specification mismatch')
        scheduling = vm.get('scheduling', {})
        if scheduling.get('onHostMaintenance') != 'TERMINATE' or scheduling.get('automaticRestart') is not False:
            raise ComputeError('instance maintenance or restart mismatch')
        accounts = vm.get('serviceAccounts', [])
        if len(accounts) != 1 or accounts[0].get('scopes') != ['https://www.googleapis.com/auth/cloud-platform']:
            raise ComputeError('instance service account scopes mismatch')
        items = vm.get('metadata', {}).get('items', [])
        expected = {'tidehunter-run-id': self.state['run_id'],
                    'tidehunter-source-sha256': self.state['source_sha256'],
                    'tidehunter-deadline-utc': self.state['deadline']}
        for key, value in expected.items():
            if [item.get('value') for item in items if item.get('key') == key] != [value]:
                raise ComputeError('instance startup metadata mismatch')
        scripts = [item.get('value') for item in items if item.get('key') == 'startup-script']
        if (len(scripts) != 1 or not isinstance(scripts[0], str)
                or hashlib.sha256(scripts[0].encode('utf-8')).hexdigest() != rec.get('startup_sha256')):
            raise ComputeError('startup script identity mismatch')
        nics = vm.get('networkInterfaces', [])
        if len(nics) != 2:
            raise ComputeError('expected two network interfaces')
        ips = []
        for nic, field, nic_type in zip(nics, ('ordinary_subnet', 'rdma_subnet'), ('GVNIC', 'IRDMA')):
            if (not _end(nic.get('subnetwork'), spec.get(field, '')) or nic.get('nicType') != nic_type
                    or nic.get('accessConfigs') or nic.get('ipv6AccessConfigs')):
                raise ComputeError('network interface identity mismatch')
            try:
                ip = ipaddress.ip_address(nic.get('networkIP', ''))
                if ip.version != 4 or not ip.is_private or ip.is_loopback or ip.is_link_local:
                    raise ValueError()
            except ValueError as exc:
                raise ComputeError('invalid private network address') from exc
            ips.append(str(ip))
        if ips[0] == ips[1]:
            raise ComputeError('network addresses must be distinct')
        if not _end(vm.get('machineType'), 'zones/' + ZONE + '/machineTypes/' + spec['machine_type']):
            raise ComputeError('machine type mismatch')
        expected_policy = 'projects/' + PROJECT + '/regions/' + REGION + '/resourcePolicies/' + spec['policy']
        if len(vm.get('resourcePolicies', [])) != 1 or not _end(vm['resourcePolicies'][0], expected_policy):
            raise ComputeError('placement policy attachment mismatch')
        index = int(rec['name'].rsplit('-', 1)[1])
        return {'index': index, 'role': 'coordinator' if index == 0 else 'owner', 'name': rec['name'],
                'id': _id(rec['id']), 'zone': ZONE, 'internal_ip': ips[0], 'rdma_ip': ips[1]}

    def _requested_identity(self, *, source_sha256, deadline):
        """The persisted identity fields this command asks to change."""
        changes = {}
        if deadline is not None and parse_time(deadline) != parse_time(self.state['deadline']):
            changes['deadline'] = deadline
        if source_sha256 is not None and source_sha256 != self.state.get('source_sha256'):
            changes['source_sha256'] = source_sha256
        return changes

    async def _run_prefix_present(self):
        """Every instance or boot disk name this run owns, present or not."""
        names = [self.state['run_id'] + '-' + str(index) for index in range(self.host_count)]
        present = []
        for name in names:
            for kind in ('instance', 'disk'):
                if await self._exists(kind, name):
                    present.append(kind + ':' + name)
        return present

    async def purge_expired_lease(self, store):
        """Drop one dead attempt's expired lease once its fleet is verifiably gone."""
        current = gcp_lease.read(store, self.state['run_id'])
        if current is None or not gcp_lease.expired(current[0]):
            return None
        return gcp_lease.purge(store, self.state['run_id'], absent=True)

    async def sweep_prefix(self, *, source_sha256=None, deadline=None):
        """Remove one dead attempt's state before this fleet takes the run prefix.

        The run id is derived from the campaign, so every re-create writes the
        same `runs/<run>/qualification/<host>/worker-ready.json` and `gate.json`
        keys, the same `package.tar`, and the same placement policy name. The
        worker publishes readiness with `_put_once`, so a stale object makes the
        new worker exit before its first poll, and a guest gate that names the
        previous instance waits for a gate that never arrives. Delete these
        control objects and the policy only when no instance or boot disk of the
        run exists; a live fleet is never touched, and evidence under the
        campaign prefix is never listed.
        """
        if self._cleanup_until is not None:
            raise ComputeError('cleanup already in progress')
        changes = self._requested_identity(source_sha256=source_sha256, deadline=deadline)
        self._cleanup_until = time.monotonic() + CLEANUP_SECONDS
        try:
            present = await self._run_prefix_present()
            if present:
                if changes:
                    raise ComputeError(
                        'a recorded instance or disk still exists; a new fleet identity '
                        'needs a verifiably gone fleet: ' + ', '.join(present))
                return {'present': present, 'removed': None}
            removed = {'policy': None, 'objects': [], 'lease': None}
            policy_name = self.state['run_id'] + '-placement'
            if await self._exists('policy', policy_name):
                # Persist the intent first: a retried sweep repeats a recorded
                # delete instead of an unrecorded name-based one.
                self.state['policy'] = {'name': policy_name, 'region': REGION,
                                        'delete_requested': True}
                self._save()
                await self._delete('policy', policy_name)
                removed['policy'] = policy_name
            store = gcp_api.campaign_client(self.config).objects(self.config['bucket'])
            for item in store.list('runs/' + self.state['run_id'] + '/'):
                name = item.get('name')
                if not _control_object(name, self.state['run_id']):
                    continue
                try:
                    store.delete(name, if_generation_match=item['generation'])
                except gcp_api.ApiError as exc:
                    if exc.status != 404:
                        raise
                removed['objects'].append({'object': name,
                                           'generation': str(item['generation'])})
            lease = await self.purge_expired_lease(store)
            removed['lease'] = self.state['run_id'] if lease is not None else None
            self.state['instances'] = []
            for key in ('policy', 'workers_released', 'guest_admitted', 'cleanup_errors'):
                self.state.pop(key, None)
            if 'deadline' in changes:
                self.state['deadline'] = changes['deadline']
            if 'source_sha256' in changes:
                self.state['source_sha256'] = changes['source_sha256']
            self.state['prefix_purge'] = {
                'at_utc': dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
                          .isoformat().replace('+00:00', 'Z'),
                'no_instance_or_disk': True, 'identity_changes': sorted(changes),
                **removed}
            self._save()
            return {'present': [], 'removed': removed}
        finally:
            self._cleanup_until = None

    async def create_vm(self, index, image, deadline, startup):
        self._live()
        if type(index) is not int or index not in range(self.host_count) or not isinstance(startup, str) or not startup:
            raise ValueError('invalid VM index or startup')
        if parse_time(deadline) != parse_time(self.state['deadline']):
            raise ValueError('VM deadline must equal persisted absolute deadline')
        if not isinstance(image, str) or not IMAGE_PATH.fullmatch(image):
            raise ValueError('a pinned image path is required')
        policy = self.state.get('policy')
        if not policy or policy.get('uncertain') or not policy.get('id'):
            raise ComputeError('placement must be created and verified first')
        actual_policy = await self._describe('policy', policy['name'])
        if not self._policy_owned(actual_policy, policy):
            raise ComputeError('placement identity changed')
        name = self.state['run_id'] + '-' + str(index)
        if any(r.get('name') == name for r in self.state['instances']) or len(self.state['instances']) >= self.host_count:
            raise ComputeError('instance already recorded or fleet limit reached')
        if await self._exists('instance', name) or await self._exists('disk', name):
            raise ComputeError('instance or disk name already exists')
        self._live()
        rec = {'name': name, 'zone': ZONE, 'disk_token': uuid.uuid4().hex, 'uncertain': True,
               'image': image, 'startup_sha256': hashlib.sha256(startup.encode('utf-8')).hexdigest(),
               'launch_spec': {'deadline': self.state['deadline'], 'policy': policy['name'],
                               'machine_type': self.config['machine_type'],
                               'ordinary_subnet': self.candidate['ordinary_subnet'],
                               'rdma_subnet': self.candidate['rdma_subnet']}}
        self.state['instances'].append(rec)
        self._save()
        argv, _body = launch_spec(self.config, self.candidate, self.state, index, image,
                                  startup, rec['disk_token'], policy['name'])
        error = None
        response = None
        try:
            response = await self._call(argv, creation=True)
        except CreateConflict:
            rec['conflict'] = True
            self._save()
            raise
        except CreateRejected as exc:
            rec['uncertain'] = False
            rec['create_rejection'] = exc.code
            self._save()
            raise
        except CommandFailed as exc:
            error = exc
            if type(exc.response.get('exit_code')) is not int or exc.response['exit_code'] == 0:
                raise  # An untyped transport failure is not a synchronous rejection.
            # A failed CLI can still carry the accepted operation on stdout.
            try:
                response = _json_response({'stdout': exc.response.get('stdout', '')})
            except ComputeError:
                response = None
            if not (isinstance(response, dict) and isinstance(response.get('name'), str)
                    or isinstance(response, list) and len(response) == 1
                    and isinstance(response[0], dict) and isinstance(response[0].get('name'), str)):
                await self._resolve_failed_create(rec)
                if not rec.get('operation_name'):
                    raise ComputeError('instance create failed; absence verified') from exc
        except Exception as exc:
            error = exc
        if isinstance(response, list) and len(response) == 1:
            response = response[0]
        if isinstance(response, dict) and isinstance(response.get('name'), str):
            operation_name = response['name']
            if not re.fullmatch(r'operation-[a-z0-9-]+', operation_name):
                raise ComputeError('invalid create operation name')
            rec['operation_name'] = operation_name
            self._save()
        if rec.get('operation_name'):
            await self._operation_done(rec)
        try:
            vm = await self._ready_vm(rec)
            rec['id'] = _id(vm['id'])
            rec['uncertain'] = False
            self._save()
            fleet = self._fleet(vm, rec)
            await self._boot(rec, vm)
            rec['fleet'] = fleet
            self._save()
            return fleet
        except Exception as exc:
            raise ComputeError('instance create or verification failed: ' + str(error or exc)) from exc

    async def admit_guests(self):
        self.state['guest_admitted'] = False
        self._save()
        policy = self.state.get('policy')
        if not policy or policy.get('uncertain'):
            raise ComputeError('placement not resolved')
        obj = await self._describe('policy', policy['name'], guest=True)
        if not self._policy_owned(obj, policy):
            raise ComputeError('guest placement identity mismatch')
        if not self.state['instances']:
            raise ComputeError('no guests to admit')
        fleet = []
        for rec in self.state['instances']:
            if rec.get('uncertain'):
                raise ComputeError('guest create unresolved')
            vm = await self._describe('instance', rec['name'], guest=True)
            actual = self._fleet(vm, rec)
            if actual != rec.get('fleet'):
                raise ComputeError('guest VM or IP identity changed')
            await self._boot(rec, vm, guest=True)
            fleet.append(actual)
        self.state['guest_admitted'] = True
        self._save()
        return fleet

    async def _cleanup_instance(self, rec):
        if rec.get('name') not in {self.state['run_id'] + '-' + str(i) for i in range(self.host_count)} or rec.get('zone') != ZONE:
            raise ComputeError('invalid instance record')
        if rec.get('conflict'):
            raise ComputeError('refusing conflicting instance name')
        if rec.get('operation_name') and not rec.get('operation_done'):
            await self._operation_done(rec)
        exists = await self._exists('instance', rec['name'])
        if exists:
            vm = await self._describe('instance', rec['name'])
            if not rec.get('id'):
                if not rec.get('uncertain') or not self._vm_owned(vm, rec, require_id=False):
                    raise ComputeError('unidentified instance')
                rec['id'] = _id(vm['id'])
                rec['uncertain'] = False
                self._save()
            if not self._vm_owned(vm, rec):
                raise ComputeError('refusing foreign or replaced instance')
            # Refuse auto-delete when its boot disk cannot be identified exactly.
            await self._boot(rec, vm, verify_spec=False)
            if rec.get('delete_requested'):
                await self._observe_absence('instance', rec['name'])
            else:
                self._save_delete_intent(rec, 'delete_requested')
                await self._delete('instance', rec['name'])
            rec['deleted'] = True
            self._save()
        elif rec.get('uncertain'):
            raise ComputeError('ambiguous instance create unresolved despite absent list')
        else:
            rec['deleted'] = True
            self._save()

    async def _cleanup_disk(self, rec):
        if rec.get('name') not in {self.state['run_id'] + '-' + str(i) for i in range(self.host_count)} or rec.get('zone') != ZONE:
            raise ComputeError('invalid disk record')
        if rec.get('uncertain'):
            raise ComputeError('cannot delete disk while instance create is unresolved')
        exists = await self._exists('disk', rec['name'])
        if not exists:
            if rec.get('uncertain'):
                raise ComputeError('ambiguous disk create unresolved despite absent list')
            rec['disk_deleted'] = True
            self._save()
            return
        disk = await self._describe('disk', rec['name'])
        if not _end(disk.get('zone'), ZONE) or not owned_disk(disk, self.state, rec):
            raise ComputeError('refusing foreign disk')
        if rec.get('disk_id') and _id(rec['disk_id']) != _id(disk['id']):
            raise ComputeError('refusing replaced disk')
        if disk.get('users'):
            raise ComputeError('disk remains attached')
        rec['disk_id'] = _id(disk['id'])
        if rec.get('disk_delete_requested'):
            await self._observe_absence('disk', rec['name'])
        else:
            self._save_delete_intent(rec, 'disk_delete_requested')
            await self._delete('disk', rec['name'])
        rec['disk_deleted'] = True
        self._save()

    async def _cleanup_policy(self, rec):
        if rec.get('name') != self.state['run_id'] + '-placement' or rec.get('region') != REGION:
            raise ComputeError('invalid placement record')
        if rec.get('conflict'):
            raise ComputeError('refusing conflicting placement name')
        if any(not instance.get('deleted') or instance.get('uncertain')
               for instance in self.state.get('instances', [])):
            raise ComputeError('placement retained while instance cleanup is unresolved')
        if not await self._exists('policy', rec['name']):
            if rec.get('uncertain'):
                raise ComputeError('ambiguous placement create unresolved despite absent list')
            rec['deleted'] = True
            self._save()
            return
        policy = await self._describe('policy', rec['name'])
        if not rec.get('id'):
            if not rec.get('uncertain') or not self._policy_owned(policy, rec, require_id=False):
                raise ComputeError('unidentified placement policy')
            rec['id'] = _id(policy['id'])
            rec['uncertain'] = False
            self._save()
        if not self._policy_owned(policy, rec):
            raise ComputeError('refusing foreign or replaced placement policy')
        if rec.get('delete_requested'):
            await self._observe_absence('policy', rec['name'])
        else:
            self._save_delete_intent(rec, 'delete_requested')
            await self._delete('policy', rec['name'])
        rec['deleted'] = True
        self._save()

    async def cleanup(self):
        if self._cleanup_until is not None:
            raise ComputeError('cleanup already in progress')
        self._cleanup_until = time.monotonic() + CLEANUP_SECONDS
        try:
            return await self._cleanup()
        finally:
            self._cleanup_until = None

    async def _cleanup(self):
        errors = []
        async def cleanup_record(rec):
            failures = []
            for label, operation in (('instance', self._cleanup_instance), ('disk', self._cleanup_disk)):
                try:
                    await operation(rec)
                except Exception as exc:
                    failures.append(label + ' ' + str(rec.get('name')) + ': ' + str(exc))
            return failures

        # At most the persisted fleet size is admitted. Saves are synchronous on this event
        # loop, so each persisted intent completes before any callback yields.
        # A slow delete must not prevent the other owned VMs from being issued.
        records = list(reversed(self.state.get('instances', [])))
        if len(records) > self.host_count:
            raise ComputeError('fleet limit exceeded during cleanup')
        for failures in await asyncio.gather(*(cleanup_record(rec) for rec in records)):
            errors.extend(failures)
        if self.state.get('policy'):
            try:
                await self._cleanup_policy(self.state['policy'])
            except Exception as exc:
                errors.append('policy: ' + str(exc))
        self.state['cleanup_errors'] = errors
        self.state['cleanup_toctou'] = TOCTOU
        self._save()
        if errors:
            raise ComputeError('cleanup incomplete: ' + '; '.join(errors) + ' ' + TOCTOU)
        return {'errors': errors, 'toctou': TOCTOU}


async def gcloud(argv):
    """Run one reviewed gcloud argument list and return its bounded result."""
    process = await asyncio.create_subprocess_exec(
        'gcloud', *argv, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=GCLOUD_SECONDS)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    return {'exit_code': process.returncode,
            'stdout': stdout.decode('utf-8', 'replace'),
            'stderr': stderr.decode('utf-8', 'replace')}


def _recorded_fleet(config, args, *, rebind=False):
    state = json.loads(args.state.read_text())
    args.campaign = args.campaign or state.get('campaign')
    args.hosts = args.hosts or state.get('host_count')
    args.role = args.role or state.get('role', 'campaign')
    if state.get('campaign') != args.campaign or state.get('host_count') != args.hosts:
        raise ValueError('recorded fleet campaign or host count differs from this command')
    if state.get('role', 'campaign') != args.role:
        raise ValueError('recorded fleet role differs from this command')
    if args.source_sha256 is not None and state.get('source_sha256') != args.source_sha256 and not rebind:
        raise ValueError('recorded fleet source differs from this command')
    if args.deadline is not None and parse_time(state['deadline']) != parse_time(args.deadline) and not rebind:
        raise ValueError('recorded fleet deadline differs from this command')
    Compute(config, state, args.state, gcloud)
    return state


async def create(config, args):
    """Create the placement policy and every owned host of one reviewed fleet."""
    if args.package is not None:
        # Derive the source from the package the guests will verify, and refuse
        # a differing explicit value before any cloud call creates a host. A
        # campaign package must also carry the pinned nvmet module: without it
        # S1-0 cannot pass, so refuse before a host is rented.
        args.source_sha256 = package_source_sha256(args.package, args.source_sha256)
        if (args.role or 'campaign') == 'campaign':
            mailbox_exec.admit_package_nvmet_fix(Path(args.package))
    if args.state.exists():
        state = _recorded_fleet(config, args, rebind=True)
    else:
        state = new_state(config, campaign=args.campaign, run_id=args.run_id,
                          deadline=args.deadline, hosts=args.hosts,
                          source_sha256=args.source_sha256, owner_nonce=uuid.uuid4().hex,
                          role=args.role)
        Compute(config, state, args.state, gcloud)
    image_path = args.image or config.get('image')
    if args.dry_run:
        policy = policy_body(state)
        preview = dict(state, policy={'name': policy['name']})
        bodies = [launch_spec(config, candidate(config), preview, index, image_path,
                              startup_script(config, state), '0' * 32, policy['name'])[1]
                  for index in range(state['host_count'])]
        print(json.dumps({'run_id': state['run_id'], 'campaign': state['campaign'],
                          'role': state.get('role', 'campaign'), 'host_count': state['host_count'],
                          'deadline': state['deadline'], 'policy': policy,
                          'instances': bodies}, sort_keys=True, indent=2))
        return 0

    if args.role == 'qualification':
        image = {'path': image_path, 'id': str(admit_ready_image(config, image_path)['id'])}
    else:
        if args.image is not None and args.image != config.get('image'):
            raise ValueError('--image must equal the admitted config image')
        image = admit_image(config)
    # The checklist's Falcon profile and MTU admission belongs to this path, not
    # only to the qualification flow.
    await gcp_manual_preflight.infrastructure_preflight(config, image['path'], gcloud)
    compute = Compute(config, state, args.state, gcloud)
    # The run id is derived from the campaign, so a re-create meets its own
    # previous attempt: stale readiness and gate objects, a reserved placement
    # policy name, and (after an expiry) an old deadline. Clear them only when
    # no instance or boot disk of the run exists.
    swept = await compute.sweep_prefix(source_sha256=args.source_sha256, deadline=args.deadline)
    compute = Compute(config, state, args.state, gcloud)
    budget = None
    if not state.get('instances'):
        # The conservative campaign cap is checked before the first rented host.
        budget = gcp_budget.admit_campaign(_window_hours(state), state['host_count'])
    gate = startup_script(config, state)
    policy = policy_body(state)
    if not state.get('policy'):
        save(args.state, state)
        await compute.create_policy()
    fleet = []
    for index in range(state['host_count']):
        record = next((item for item in state['instances'] if item.get('name') == state['run_id'] + '-' + str(index)), None)
        if record is not None and record.get('fleet') and not record.get('uncertain'):
            fleet.append(record['fleet'])
            continue
        fleet.append(await compute.create_vm(index, image['path'], state['deadline'], gate))
    released = None
    try:
        if await compute.admit_guests() != fleet:
            raise ComputeError('guest fleet identity changed before admission')
        if args.role == 'campaign' and args.package is not None:
            released = _release_workers(config, state, args, fleet, args.state)
    except BaseException:
        # The hosts exist and bill. A publication that raises must not leave
        # them waiting on a closed gate until the DELETE deadline.
        await _abandon_fleet(compute, state, args.state)
        raise
    print(json.dumps({'run_id': state['run_id'], 'campaign': state['campaign'],
                      'role': state.get('role', 'campaign'), 'image': image, 'policy': policy['name'],
                      'hosts': fleet, 'workers_released': released, 'control_purge': swept,
                      'budget': budget}, sort_keys=True, indent=2))
    return 0


def _window_hours(state):
    """The billed window of a fleet as whole hours, rounded up."""
    remaining = (parse_time(state['deadline'])
                 - dt.datetime.now(dt.timezone.utc)).total_seconds()
    return max(1, math.ceil(remaining / 3600 - 1e-6))


async def _abandon_fleet(compute, state, path):
    """Delete a fleet whose create failed after its hosts existed."""
    try:
        await compute.cleanup()
    except Exception as exc:
        state.setdefault('cleanup_errors', []).append('abandoned create: ' + str(exc))
        save(path, state)
        print('error: fleet cleanup incomplete: ' + str(exc), file=sys.stderr)


def _release_workers(config, state, args, fleet, path):
    """Publish the sealed package and runtime, then open every host's gate.

    The gates were released only after the guest and image identities were
    admitted, and the objects exist before them, so a host cannot start a
    worker against a missing or differing package.
    """
    store = gcp_api.campaign_client(config).objects(config['bucket'])
    spec = json.loads(Path(args.spec).read_text())
    mailbox_exec.publish(store, config, state, Path(args.package).resolve(strict=True),
                         args.revision, spec, fleet,
                         guest_roots=[str(item) for item in args.guest_root] or None)
    released = []
    for host in fleet:
        mailbox_exec.release(store, state, host,
                             gcp_bootstrap.startup_script(config['bucket'], state['run_id'],
                                                          state['source_sha256'], state['deadline']))
        released.append(host['name'])
    state['workers_released'] = released
    save(path, state)
    return released


async def ready(config, args):
    """Wait for every guest worker to publish its readiness object."""
    state = _recorded_fleet(config, args)
    if not state.get('workers_released'):
        raise ComputeError('no gate was released for this fleet; create it with --package')
    remote = mailbox_exec.MailboxExec.from_state(
        args.state, gcp_api.campaign_client(config), bucket=config['bucket'],
        timeout_seconds=args.worker_seconds)
    prepared = remote.wait_workers(require=args.require_manifest_sha256)
    print(json.dumps({'run_id': state['run_id'], 'hosts': sorted(prepared),
                      'storage': {name: item.get('storage') for name, item in prepared.items()}},
                     sort_keys=True, indent=2))
    return 0


async def verify(config, args):
    """Read back the owned placement policy, hosts and boot disks; change nothing."""
    state = _recorded_fleet(config, args)
    compute = Compute(config, state, args.state, gcloud)
    policy = state.get('policy')
    if not policy or policy.get('uncertain'):
        raise ComputeError('no verified placement policy is recorded')
    actual = await compute._describe('policy', policy['name'])
    if not compute._policy_owned(actual, policy):
        raise ComputeError('placement identity changed')
    hosts = []
    for record in state['instances']:
        vm = await compute._describe('instance', record['name'])
        if not compute._vm_owned(vm, record):
            raise ComputeError('instance ownership mismatch for ' + record['name'])
        disk = await compute._boot(dict(record), vm)
        hosts.append({**compute._fleet(vm, record), 'disk_id': _id(disk['id']),
                      'disk_size_gb': disk.get('sizeGb'), 'status': vm.get('status')})
    if len(hosts) != state['host_count']:
        raise ComputeError('recorded host count differs from the fleet size')
    print(json.dumps({'run_id': state['run_id'], 'campaign': state['campaign'],
                      'placement': {'name': actual['name'], 'id': _id(actual['id']),
                                    'collocation': actual['groupPlacementPolicy']['collocation'],
                                    'max_distance': actual['groupPlacementPolicy']['maxDistance']},
                      'hosts': hosts}, sort_keys=True, indent=2))
    return 0


async def delete(config, args):
    """Delete the recorded hosts, their boot disks and the placement policy."""
    state = _recorded_fleet(config, args)
    compute = Compute(config, state, args.state, gcloud)
    result = await compute.cleanup()
    # Cleanup just proved that no instance or boot disk of the run exists, so an
    # expired lease of the same run is dead and must not block the next fleet.
    store = gcp_api.campaign_client(config).objects(config['bucket'])
    lease = await compute.purge_expired_lease(store)
    print(json.dumps({'run_id': state['run_id'], 'campaign': state['campaign'],
                      'cleanup': result, 'hosts': len(state['instances']),
                      'lease_purged': bool(lease)}, sort_keys=True, indent=2))
    return 0


def candidate(config):
    candidates = [item for item in config.get('candidates', []) if item.get('zone') == ZONE]
    if len(candidates) != 1:
        raise ValueError('exactly one us-central1-a candidate is required')
    return candidates[0]


def parse(argv=None):
    parser = argparse.ArgumentParser(description='Owned two- or three-host campaign fleet.')
    parser.add_argument('action', choices=('create', 'ready', 'verify', 'delete'))
    parser.add_argument('--config', type=Path, default=Path(__file__).with_name('config.json'))
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--campaign')
    parser.add_argument('--hosts', type=int, choices=FLEET_HOSTS)
    parser.add_argument('--hours', type=int)
    parser.add_argument('--deadline')
    parser.add_argument('--image')
    parser.add_argument('--role', choices=FLEET_ROLES)
    parser.add_argument('--source-sha256')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--package', type=Path,
                        help='sealed package directory (prepare.py output) to publish')
    parser.add_argument('--spec', type=Path,
                        help='reviewed campaign specification the package was built for')
    parser.add_argument('--revision', help='full source revision the package was sealed at')
    parser.add_argument('--guest-root', action='append', default=[],
                        help='guest directory the campaign owns; repeatable')
    parser.add_argument('--worker-seconds', type=int, default=900,
                        help='bound on the worker readiness wait')
    parser.add_argument('--require-manifest-sha256',
                        help='require every worker to report this sealed manifest')
    args = parser.parse_args(argv)
    if args.dry_run and args.action != 'create':
        parser.error('--dry-run applies to create only')
    if args.package is not None and (args.spec is None or args.revision is None):
        parser.error('--package needs --spec and --revision')
    if args.action == 'ready' and not args.state.exists():
        parser.error('ready needs the recorded fleet state')
    if args.action != 'ready' and (args.worker_seconds != 900
                                   or args.require_manifest_sha256 is not None):
        parser.error('--worker-seconds and --require-manifest-sha256 apply to ready only')
    if args.action == 'create':
        if args.campaign is None:
            parser.error('create requires --campaign')
        if args.hosts is None:
            parser.error('create requires --hosts')
        if (args.deadline is None) == (args.hours is None):
            parser.error('create requires exactly one of --hours or --deadline')
        if not args.state.exists():
            args.role = args.role or 'campaign'
            if args.role == 'qualification' and args.hosts != 2:
                parser.error('a qualification fleet has exactly two hosts')
            if args.role == 'qualification' and args.image is None:
                parser.error('a qualification fleet requires --image')
        args.run_id = fleet_run_id(args.campaign)
        args.deadline = args.deadline or fleet_deadline(args.hours)
        if parse_time(args.deadline) <= dt.datetime.now(dt.timezone.utc):
            parser.error('the fleet deadline must be in the future')
        if (args.source_sha256 is None and not args.state.exists()
                and args.package is None):
            parser.error('create requires --source-sha256 or --package')
    return args


def main(argv=None):
    args = parse(argv)
    config = json.loads(args.config.read_text())
    action = {'create': create, 'ready': ready, 'verify': verify,
              'delete': delete}[args.action]
    try:
        return asyncio.run(action(config, args))
    except (ComputeError, gcp_api.ApiError, ValueError, TimeoutError, OSError) as exc:
        print('error: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

