"""Deterministic checkpoint/Async campaign and fail-closed evidence helpers."""
import csv
import math
from pathlib import Path

import efa_progress

BASELINE_SOURCE_SHA256 = '97f18475bb15b8ad7ae20ac799fd2a39d5399ccdbc7d3ce8c88d5950730ec5b1'
BASELINE_BINARY_SHA256 = 'a7f2d3fe0fbe41df070527e810ae736c5e3f1a8ac7d0fe0654fedfcde25e74ab'


def case(family, build, value, rep, transport, cache=0, shard_cache=0):
    spec = efa_progress.case(family, build, value, 32, 32, rep, transport,
                             cache=cache, lanes=4 if build == 'baseline' else 1,
                             workers=1 if build == 'baseline' else 2,
                             member='mapped' if build == 'baseline' else 'async', cold=family != 'pressure')
    spec.update(family=f'{family}-f{cache}-s{shard_cache}-{transport}',
                shard_cache=shard_cache, native_workers=32,
                reads=256 if family == 'qual' else 8192,
                modes=['tcp'] if family in ('qual', 'pressure') else ['native-sync', 'tcp'])
    spec['case_id'] = f"{spec['family']}-{build}-v{value}-b32-w32-r{rep}"
    # Paired native controls are independent executions, but equivalent transport
    # copies are correlated controls, not ten independent repetitions.
    spec['native_control_id'] = f'{build}-v{value}-r{rep}-cache0-workers32-window32'
    return spec


def qualification_cases():
    return [case('qual', build, value, 0, 'efa')
            for value in (1024, 8192) for build in ('baseline', 'current')]


def main_cases():
    result = []
    for rep in range(5):
        builds = ('baseline', 'current') if rep % 2 == 0 else ('current', 'baseline')
        for value in (1024, 8192):
            for transport in ('tcp', 'efa'):
                result.extend(case('core', build, value, rep, transport) for build in builds)
        for transport in ('tcp', 'efa'):
            for frontend, shard in ((131072, 0), (0, 65536), (131072, 65536)):
                result.append(case('pressure', 'current', 8192, rep, transport, frontend, shard))
    return result


def recovery_cases():
    result = []
    for rep in range(5):
        for tail in (0, 128):
            for restart in ('surviving', 'full'):
                for build in (('baseline', 'current') if rep % 2 == 0 else ('current', 'baseline')):
                    spec = case('recovery', build, 8192, rep, 'efa')
                    spec.update(case_id=f'recovery-{restart}-t{tail}-efa-{build}-r{rep}',
                                family=f'recovery-{restart}-t{tail}-efa', recovery=True,
                                restart=restart, checkpoint_batches=64, tail_batches=tail,
                                modes=['recovery'], cold=False)
                    result.append(spec)
    return result


def plan(scope='full'):
    if scope not in ('full', 'recovery'):
        raise ValueError('unknown checkpoint campaign scope')
    return qualification_cases() + (main_cases() if scope == 'full' else []) + recovery_cases()


def expected_cases(scope='full'):
    return {c['case_id'] + '-' + mode:
            (c['family'], c['build'], c['value'], c['batch'], c['window'], c['rep'], mode, c['case_id'])
            for c in plan(scope) for mode in c['modes']}


def positive(value, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (not allow_zero and value == 0):
        raise ValueError('invalid numeric evidence')
    return value


def validate_threads(document, pid):
    if document.get('pid') != pid or document.get('ticks_per_second', 0) <= 0 or not document.get('threads'):
        raise ValueError('missing per-thread process identity/CPU evidence')
    for key, row in document['threads'].items():
        if key != f"{row['tid']}:{row['start_ticks']}" or row['tid'] <= 0 or row['samples'] < 1:
            raise ValueError('invalid per-thread identity')
        for name in ('user_ticks', 'system_ticks', 'voluntary_context_switches', 'nonvoluntary_context_switches'):
            first, last = row['first'][name], row['last'][name]
            positive(first, True)
            positive(last, True)
            if last < first:
                raise ValueError('thread counters decreased')
    return {name: sum(row['last'][name] - row['first'][name] for row in document['threads'].values())
            for name in ('user_ticks', 'system_ticks', 'voluntary_context_switches', 'nonvoluntary_context_switches')}


def validate_restart(receipt, spec, cfg, partition, final_pid):
    if (receipt.get('case_id') != spec['case_id'] or receipt.get('partition') != partition
            or receipt.get('deadline_utc') != cfg['deadline_utc'] or receipt.get('data_preserved') is not True
            or receipt.get('new_pid') != final_pid or receipt.get('old_pid', 0) <= 0
            or receipt['old_pid'] == receipt['new_pid'] or not receipt.get('ready')
            or receipt.get('ready_removed_before_start') is not True):
        raise ValueError('restart lacks replacement PID/readiness/data/TTL proof')
    positive(receipt['node_restart_ns'])


def validate_recovery_json(row, spec, action):
    expected = dict(schema=1, action=action, value_bytes=8192, batch_size=32,
                    checkpoint_batches=64, tail_batches=spec['tail_batches'], key_count=2048,
                    verified_keys=2048 if action == 'reopen' else 0,
                    abrupt_exit=action == 'seed', exact_retry_verified=action == 'reopen')
    if any(row.get(k) != v or type(row.get(k)) is not type(v) for k, v in expected.items()):
        raise ValueError('recovery correctness/dimensions/exit proof differs')
    for key in ('open_ns', 'checkpoint_create_ns', 'tail_append_ns', 'verification_ns'):
        positive(row[key], key not in (('open_ns', 'verification_ns') if action == 'reopen' else ('open_ns', 'checkpoint_create_ns', 'tail_append_ns')))
    if spec['build'] == 'current' and action == 'reopen':
        stats = row['recovery_stats']
        if stats.get('checkpoint_loaded') is not True or stats.get('tail_batches') != spec['tail_batches']:
            raise ValueError('current reopen did not adopt checkpoint/exact tail')
        for key in ('checkpoint_load_ns', 'proof_validation_ns', 'tail_replay_ns', 'checkpoint_commit_ns',
                    'connection_setup_ns', 'fencing_ns', 'tail_bytes'):
            positive(stats[key], True)
        if (stats['tail_bytes'] > 0) != bool(spec['tail_batches']):
            raise ValueError('tail replay bytes differ from empty/nonempty tail')
    elif spec['build'] == 'baseline' and 'recovery_stats' in row:
        raise ValueError('baseline must not claim current-only recovery statistics')


def read_path_metrics(path, spec, backend):
    """Validate per-phase, per-tier accounting, not success markers or hot labels."""
    phases = {'point_get', 'point_get_many', 'point_get_window'}
    phases |= ({'cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'}
               if spec['cold'] else {'point_get_hot', 'point_get_many_hot'})
    audited_phases = phases | {'point_get_missing', 'exists', 'exists_missing', 'post_flush_get'}
    roles = {('frontend', '')} | {('storage', str(p)) for p in range(5)}
    inventory, result = {}, {}
    with Path(path).open() as stream:
        for row in csv.DictReader(stream):
            phase, boundary, role = row['phase'], row['boundary'], (row['role'], row['partition'])
            if (row['backend'], int(row['value_bytes']), int(row['batch_size']), int(row['window']), int(row['repetition'])) != (backend, spec['value'], 32, 32, 0):
                raise ValueError('read path dimensions differ')
            if phase not in audited_phases or boundary not in ('before', 'after') or role not in roles:
                raise ValueError('unplanned read-path evidence')
            key = (phase, boundary, role)
            if key in inventory:
                raise ValueError('duplicate read-path evidence')
            values = {k: int(v) for k, v in row.items() if k not in
                      ('backend', 'value_bytes', 'batch_size', 'window', 'repetition', 'phase', 'boundary', 'role', 'partition')}
            if min(values.values()) < 0:
                raise ValueError('negative read-path counter')
            budget = spec['cache'] if role[0] == 'frontend' else spec['shard_cache']
            if values['cache_budget_bytes'] != budget or values['cache_retained_bytes'] > budget or values['cache_metadata_bytes'] > values['cache_retained_bytes']:
                raise ValueError('finite cache accounting exceeds selected budget')
            if budget == 0 and any(values[k] != 0 for k in ('cache_retained_bytes', 'cache_metadata_bytes', 'cache_entries', 'cache_hits', 'cache_evictions')):
                raise ValueError('disabled tier retained/hit/evicted cache entries')
            if role[0] == 'storage':
                for current, peak, limit in (
                        ('inflight_requests', 'peak_inflight_requests', 32),
                        ('inflight_bytes', 'peak_inflight_bytes', 64 * 1024 * 1024),
                        ('inflight_reads', 'peak_inflight_reads', 128)):
                    if not values[current] <= values[peak] <= limit:
                        raise ValueError('node admission exceeds configured bound')
                if values['peak_device_reads'] > 128:
                    raise ValueError('device admission exceeds configured bound')
            inventory[key] = values
    if set(inventory) != {(p, b, r) for p in audited_phases for b in ('before', 'after') for r in roles}:
        raise ValueError('missing per-phase read-path inventory')
    for phase in sorted(phases):
        totals = {role: dict.fromkeys(('cache_misses', 'cache_evictions', 'wal_reads', 'wal_read_bytes'), 0)
                  for role in ('frontend', 'storage')}
        for role in sorted(roles):
            before, after = (inventory[phase, b, role] for b in ('before', 'after'))
            deltas = {k: after[k] - before[k] for k in ('cache_hits', 'cache_misses', 'cache_evictions', 'wal_reads', 'wal_read_bytes')}
            if min(deltas.values()) < 0:
                raise ValueError('read-path counters decreased')
            for key in totals[role[0]]:
                totals[role[0]][key] += deltas[key]
            for key, value in deltas.items():
                result['read_path', phase, '-'.join(role) + '-' + key] = value
        # Hot passes can fit in the finite cache, and arbitrary placement can put
        # all requested members on one shard. Require misses by tier, not shard.
        if phase in ('point_get', 'point_get_window'):
            for role, budget in (('frontend', spec['cache']), ('storage', spec['shard_cache'])):
                if budget and any(totals[role][key] <= 0 for key in ('cache_misses', 'cache_evictions')):
                    raise ValueError('pressure tier lacks real misses/evictions')
        if not phase.endswith('_hot') and any(totals['storage'][key] <= 0 for key in ('wal_reads', 'wal_read_bytes')):
            raise ValueError('read phase lacks actual WAL reads')
    return result
