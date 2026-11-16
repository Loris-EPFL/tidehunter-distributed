"""Owned, fail-closed Compute lifecycle through the supplied async command transport.

Deletes are by name: even an immediately preceding ID/ownership GET cannot
prevent replacement between that GET and the delete (a residual TOCTOU race).
An uncertain create is never retried and an empty list cannot resolve it.
"""
import asyncio
import datetime as dt
import hashlib
import ipaddress
import json
import re
import subprocess
import time
import uuid

from gcp_campaign import (BOOT_DISK_GIB, GUEST_SERVICE_ACCOUNT, has_delete_expiry,
                          owned_disk, owned_instance, parse_time, save)

ZONE = 'us-central1-a'
REGION = 'us-central1'
PROJECT = 'mysten-research'
MCP_CALL_SECONDS = 30
READ_ATTEMPTS = 3
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
        from gcp_runtime import owner_host_counts
        count = state.get('host_count', 6)
        if type(count) is not int or count - 1 not in owner_host_counts():
            raise ValueError('invalid persisted fleet host count')
        self.host_count = count
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

    async def _call(self, argv, *, guest=False, read_only=False, deadline=None, allow_empty=False, creation=False):
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
                response = await asyncio.wait_for(self.execute(args), timeout=min(MCP_CALL_SECONDS, remaining))
            except (TimeoutError, subprocess.TimeoutExpired) as exc:
                if not read_only or attempt + 1 == READ_ATTEMPTS:
                    raise ComputeError('MCP call timed out; outcome may be unknown') from exc
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
            await self._call(prefix + ['delete', name, location] + flags, allow_empty=True)
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

    async def create_vm(self, index, image, deadline, startup):
        self._live()
        if type(index) is not int or index not in range(self.host_count) or not isinstance(startup, str) or not startup:
            raise ValueError('invalid VM index or startup')
        if parse_time(deadline) != parse_time(self.state['deadline']):
            raise ValueError('VM deadline must equal persisted absolute deadline')
        if not isinstance(image, str) or not re.fullmatch(r'projects/[a-z0-9-]+/global/images/[a-z0-9-]+', image):
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
        description = json.dumps({'run': self.state['run_id'], 'source': self.state['source_sha256'],
                                  'expiry': self.state['deadline'], 'token': rec['disk_token']}, sort_keys=True)
        disk = _dict_flag({'name': name, 'description': description, 'image': image, 'size': str(BOOT_DISK_GIB) + 'GB',
                           'type': 'hyperdisk-balanced', 'interface': 'NVME', 'boot': 'yes', 'auto-delete': 'yes'})
        metadata = _dict_flag({'startup-script': startup, 'bench-run': self.state['run_id'],
                               'bench-source': self.state['source_sha256'], 'bench-expiry': self.state['deadline'],
                               'bench-owner': self.state['owner_nonce'],
                               'tidehunter-run-id': self.state['run_id'], 'tidehunter-source-sha256': self.state['source_sha256'],
                               'tidehunter-deadline-utc': self.state['deadline']})
        argv = ['compute', 'instances', 'create', name, '--zone=' + ZONE,
                '--machine-type=' + self.config['machine_type'], '--create-disk=' + disk,
                '--network-interface=nic-type=GVNIC,subnet=' + self.candidate['ordinary_subnet'] + ',no-address',
                '--network-interface=nic-type=IRDMA,subnet=' + self.candidate['rdma_subnet'] + ',no-address',
                '--resource-policies=' + policy['name'], '--service-account=' + GUEST_SERVICE_ACCOUNT,
                '--scopes=cloud-platform', '--labels=bench-run=' + self.state['run_id'] + ',bench-source=' + self.state['source_sha256'][:63],
                '--metadata=' + metadata, '--termination-time=' + deadline, '--instance-termination-action=DELETE',
                '--maintenance-policy=TERMINATE', '--no-restart-on-failure', '--async']
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
