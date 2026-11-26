"""Per-run leases in one CAS admission registry; expiry never permits takeover.

The registry is the atomic cap/budget/quota reservation boundary. Entries are
keyed by run ID and have stable owner tokens, independent of sibling mutations.
The legacy singleton remains readable and is never rewritten by admission.
"""
import datetime as dt
import json
import math
import re
import uuid

from gcp_api import ApiError

KEY = 'lifecycle/active-run.json'
REGISTRY_KEY = 'lifecycle/active-runs.json'
DEFAULT_MAX_RUNS = 3


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def _read(store, key):
    for _ in range(30):
        try:
            meta = store.head(key)
        except ApiError as exc:
            if exc.status == 404:
                return None
            raise
        try:
            return json.loads(store.get(key, generation=meta['generation'])), str(meta['generation'])
        except ApiError as exc:
            if exc.status != 404:
                raise
            # A sibling CAS can replace an unversioned object's generation
            # between HEAD and GET. Read the new generation, never unpinned data.
    raise RuntimeError('lease read contention exceeded retry bound')


def _registry(store):
    current = _read(store, REGISTRY_KEY)
    if current is None:
        return {'version': 2, 'runs': {}}, 0
    registry, generation = current
    if registry.get('version') != 2 or not isinstance(registry.get('runs'), dict):
        raise RuntimeError('invalid active run registry')
    return registry, generation


def read(store, run_id=None):
    legacy = _read(store, KEY)
    if legacy is not None and (run_id is None or legacy[0].get('run_id') == run_id):
        return legacy
    runs = _registry(store)[0]['runs']
    if run_id is None:
        if len(runs) > 1:
            raise RuntimeError('exact run ID required for concurrent leases')
        run_id = next(iter(runs), None)
    lease = runs.get(run_id)
    return (lease, lease['token']) if lease is not None else None


def reservation(state):
    """Recover the old manual parent's quote and full admitted fleet shape."""
    if 'reservation' in state:
        result = dict(state['reservation'])
        if 'instances' not in result:
            # Older registry entries omitted VM count. Each admitted Z4M VM has
            # at least one SSD, so one VM per SSD is a conservative upper bound.
            result['instances'] = state.get('children', {}).get('fleet', {}).get(
                'host_count', result.get('local_ssds'))
    else:
        budget = state.get('budget', {})
        fleet = state.get('children', {}).get('fleet', {})
        compute = budget.get('compute_cents')
        # Old manual state predates a machine_type field. Its exact compute quote
        # identifies the reviewed six-hour shape, including two-host fleets.
        shapes = []
        for cpus, rate in ((16, 3287109973), (32, 6574219945)):
            for hosts in (2, 6):
                cents = (rate * hosts * 21600 * 100 + 3600 * 10**9 - 1) // (3600 * 10**9)
                if cents == compute and fleet.get('host_count', hosts) == hosts:
                    shapes.append((cpus, hosts))
        if len(shapes) != 1:
            raise RuntimeError('legacy active lease has no unambiguous fleet reservation')
        cpus, hosts = shapes[0]
        result = {'cents': budget.get('ceiling_cents'), 'region': 'us-central1',
                  'vcpus': cpus * hosts, 'local_ssds': hosts * (cpus // 16),
                  'local_ssd_gib': 14000 * hosts * (cpus // 16), 'instances': hosts}
    if (not isinstance(result, dict) or result.get('region') != 'us-central1'
            or any(type(result.get(k)) is not int or result[k] <= 0
                   for k in ('cents', 'vcpus', 'local_ssds', 'local_ssd_gib', 'instances'))):
        raise ValueError('invalid active fleet reservation')
    return dict(result)


def _legacy_reservation(store, legacy):
    state = _read(store, legacy['state_key'])
    if state is None:
        raise RuntimeError('legacy active lease state missing; recover cleanup first')
    state = state[0]
    if any(state.get(k) != legacy.get(k) for k in
           ('run_id', 'project_id', 'source_sha256', 'deadline')):
        raise RuntimeError('legacy active lease state identity mismatch')
    return reservation(state)


def acquire(store, state, *, max_runs=DEFAULT_MAX_RUNS, ceiling_cents=None, quotas=None):
    if type(max_runs) is not int or max_runs <= 0:
        raise ValueError('max_concurrent_runs must be a positive integer')
    if not re.fullmatch(r'bench-[0-9a-f]{16}', state['run_id']):
        raise ValueError('invalid lease run ID')
    lease = {'version': 2, 'run_id': state['run_id'], 'project_id': state['project_id'],
             'source_sha256': state['source_sha256'], 'deadline': state['deadline'],
             'state_key': 'runs/' + state['run_id'] + '/lifecycle/state.json',
             'token': uuid.uuid4().hex}
    if ceiling_cents is not None:
        if type(ceiling_cents) is not int or ceiling_cents <= 0:
            raise ValueError('active reservation ceiling must be positive integer cents')
        lease['reservation'] = reservation(state)
    for _ in range(30):
        registry, generation = _registry(store)
        runs = registry['runs']
        legacy = _read(store, KEY)
        if state['run_id'] in runs or (legacy and legacy[0].get('run_id') == state['run_id']):
            raise RuntimeError('another campaign holds this run lease')
        if len(runs) + bool(legacy) >= max_runs:
            raise RuntimeError('another campaign holds the active lease capacity')
        if ceiling_cents is not None:
            reservations = [reservation(item) for item in runs.values()]
            if legacy is not None:
                reservations.append(_legacy_reservation(store, legacy[0]))
            reservations.append(lease['reservation'])
            if sum(item['cents'] for item in reservations) > ceiling_cents:
                raise ValueError('aggregate active run budget ceiling exceeded')
            if quotas is not None:
                check_quota(quotas, reservations)
        runs[state['run_id']] = lease
        try:
            store.put(REGISTRY_KEY, _encode(registry), content_type='application/json',
                      if_generation_match=generation)
            return lease, lease['token']
        except ApiError as exc:
            if exc.status == 412:
                continue
            if exc.status is None or exc.status >= 500:
                current = read(store, state['run_id'])
                if current == (lease, lease['token']):
                    return current
            raise
    raise RuntimeError('lease CAS contention exceeded retry bound')


def check_quota(quotas, reservations):
    """Reserve full fleets on top of observed usage, without guessing overlap.

    This can over-reserve already allocated active VMs, but also covers unrelated
    regional usage and active fleets still waiting for package upload.
    """
    # The reviewed regional API reports CPUS, LOCAL_SSD_TOTAL_GB and INSTANCES.
    # Cloud Quotas family visibility is not required: that API is denied here.
    # For immediate VM-create errors, only structured Compute JSON codes
    # QUOTA_EXCEEDED, ZONE_RESOURCE_POOL_EXHAUSTED and
    # ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS prove no create
    # (gcp_manual_compute.CreateRejected). A well-formed DONE operation error is
    # already a terminal provider result for every code. Both paths still verify
    # absence before release; immediate unknown errors/timeouts retain the lease.
    required = (('CPUS', 'vcpus'), ('LOCAL_SSD_TOTAL_GB', 'local_ssd_gib'),
                ('INSTANCES', 'instances'))
    optional = (('Z4M_CPUS', 'vcpus'),
                ('LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY', 'local_ssd_gib'))
    # Both launchers create private-only NICs, so no IN_USE_ADDRESSES is reserved.
    # Family quotas absent from this API remain enforced by VM creation.
    for metric, field in (*required, *optional):
        rows = [row for row in quotas if row.get('metric') == metric]
        if not rows and (metric, field) in optional:
            continue
        if len(rows) != 1:
            raise ValueError('regional quota missing or ambiguous: ' + metric)
        row = rows[0]
        limit, usage = float(row['limit']), float(row['usage'])
        if not all(math.isfinite(value) and value >= 0 for value in (limit, usage)):
            raise ValueError('invalid regional quota: ' + metric)
        total = sum(item[field] for item in reservations)
        if usage + total > limit:
            raise ValueError('regional quota exceeded: ' + metric)


def owned(store, state):
    current = read(store, state['run_id'])
    if current is None:
        raise RuntimeError('active campaign lease identity mismatch or missing')
    lease, generation = current
    expected = {'run_id': state['run_id'], 'project_id': state['project_id'],
                'source_sha256': state['source_sha256'], 'deadline': state['deadline'],
                'state_key': 'runs/' + state['run_id'] + '/lifecycle/state.json'}
    if any(lease.get(k) != v for k, v in expected.items()) or not lease.get('token'):
        raise RuntimeError('active campaign lease identity mismatch')
    return lease, generation


def expiry(lease):
    """The absolute deadline the lease itself carries."""
    value = lease.get('deadline') if isinstance(lease, dict) else None
    if not isinstance(value, str):
        raise RuntimeError('invalid active lease deadline')
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise RuntimeError('invalid active lease deadline') from exc
    if parsed.tzinfo is None:
        raise RuntimeError('active lease deadline lacks a UTC offset')
    return parsed.astimezone(dt.timezone.utc)


def expired(lease, now=None):
    return expiry(lease) <= (now or dt.datetime.now(dt.timezone.utc))


def purge(store, run_id, *, absent, now=None):
    """Remove one expired lease whose instances are verifiably absent.

    A dead run (a lost workstation or a credential outage) leaves its registry
    entry behind and blocks the same run id forever. `absent` is the caller's
    verified result: no instance and no boot disk of the run exists. An
    unexpired lease is never purged, because its fleet may still be billing.
    """
    if absent is not True:
        raise RuntimeError('lease instances are not verifiably absent')
    current = read(store, run_id)
    if current is None:
        return None
    lease, _token = current
    if lease.get('version') != 2:
        raise RuntimeError('a legacy lease needs an explicit release')
    if not expired(lease, now):
        raise RuntimeError('active lease has not expired')
    for _ in range(30):
        registry, generation = _registry(store)
        if registry['runs'].get(run_id) != lease:
            if read(store, run_id) is None:
                return lease
            raise RuntimeError('active campaign lease changed before purge')
        del registry['runs'][run_id]
        try:
            store.put(REGISTRY_KEY, _encode(registry), content_type='application/json',
                      if_generation_match=generation)
            return lease
        except ApiError as exc:
            if exc.status == 412:
                continue
            if exc.status is None or exc.status >= 500:
                if read(store, run_id) is None:
                    return lease
            raise
    raise RuntimeError('lease purge CAS contention exceeded retry bound')


def release(store, lease, generation):
    if lease.get('version') == 1:
        current = _read(store, KEY)
        if current != (lease, str(generation)):
            raise RuntimeError('active campaign lease changed before release')
        try:
            store.delete(KEY, if_generation_match=generation)
        except ApiError as exc:
            if exc.status is not None and exc.status < 500:
                raise
            if _read(store, KEY) == current:
                raise
        return
    for _ in range(30):
        registry, registry_generation = _registry(store)
        if (registry['runs'].get(lease['run_id']) != lease or generation != lease['token']):
            raise RuntimeError('active campaign lease changed before release')
        del registry['runs'][lease['run_id']]
        try:
            store.put(REGISTRY_KEY, _encode(registry), content_type='application/json',
                      if_generation_match=registry_generation)
            return
        except ApiError as exc:
            if exc.status == 412:
                continue
            if exc.status is None or exc.status >= 500:
                if read(store, lease['run_id']) is None:
                    return
            raise
    raise RuntimeError('lease release CAS contention exceeded retry bound')
