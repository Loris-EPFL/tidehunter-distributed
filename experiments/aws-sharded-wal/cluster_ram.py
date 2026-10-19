"""Cluster RAM/progress identities and local evidence validation; no AWS calls."""
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import checkpoint_async
import efa_progress

MIB = 1024 * 1024
BASELINE_SOURCE_SHA256 = '84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0'
BASELINE_BINARY_SHA256 = '081a77b68a54b3f165dbadaaade8d2a87d28398d5c6723eeab522a34436bfb4c'
SCOPES = ('full', 'core', 'capacity', 'recovery')


def case(workload, build, value, rep, transport, batch=32, window=32, cache=0, shard_cache=0):
    spec = efa_progress.case('qual' if workload == 'qual' else workload, build, value, batch,
                             window, rep, transport, cache=cache, lanes=1, workers=2,
                             member='async', cold=workload in ('qual', 'core', 'diag'))
    spec.update(workload=workload, shard_cache=shard_cache, native_workers=32,
                read_allowance=512 * MIB, shard_allowance=512 * MIB,
                cache_probe_keys=4096 if workload == 'capacity' else 0,
                concurrent_mixed=workload == 'concurrent',
                reads=256 if workload == 'qual' else 4096,
                modes=['tcp'] if workload in ('qual', 'capacity', 'pressure', 'large-config') else ['native-sync', 'tcp'])
    if batch == 1:
        spec.update(batches=320, mixed=64)
    if workload == 'large-config':
        spec.update(read_allowance=2 * 1024 * MIB, shard_allowance=2 * 1024 * MIB,
                    cache_probe_keys=256, batches=64, mixed=16, reads=512)
    spec['family'] = f'ram-{workload}-f{cache}-s{shard_cache}-{transport}'
    spec['case_id'] = f"{spec['family']}-{build}-v{value}-b{batch}-w{window}-r{rep}"
    spec['native_control_id'] = f'{build}-v{value}-b{batch}-w{window}-r{rep}-cache{cache}-workers32'
    return spec


def qualification_cases():
    return [case('qual', build, value, 0, 'efa')
            for value in (1024, 8192) for build in ('baseline', 'current')]


def main_cases(scope='full', repetitions=5, transports=('tcp', 'efa')):
    if scope not in SCOPES or not 1 <= repetitions <= 5:
        raise ValueError('invalid bounded RAM campaign scope/repetitions')
    result = []
    for rep in range(repetitions):
        builds = ('baseline', 'current') if rep % 2 == 0 else ('current', 'baseline')
        if scope in ('full', 'core'):
            for value in (1024, 8192):
                for transport in transports:
                    for batch, window, workload in ((32, 32, 'core'), (1, 1, 'scalar')):
                        result.extend(case(workload, build, value, rep, transport, batch, window) for build in builds)
        if scope in ('full', 'capacity'):
            for transport in transports:
                for build in builds:
                    result.append(case('equal-cache', build, 8192, rep, transport, cache=8 * MIB))
                for frontend, shard in ((2 * MIB, 0), (8 * MIB, 0), (128 * MIB, 0),
                                        (0, 16 * MIB), (8 * MIB, 16 * MIB)):
                    result.append(case('capacity', 'current', 8192, rep, transport,
                                       cache=frontend, shard_cache=shard))
                result.append(case('pressure', 'current', 8192, rep, transport,
                                   cache=128 * 1024, shard_cache=64 * 1024))
                result.append(case('concurrent', 'current', 8192, rep, transport))
    if scope in ('full', 'core'):
        for transport in transports:
            for value in (1024, 8192):
                result.append(case('diag', 'current', value, 0, transport))
    if scope in ('full', 'capacity'):
        for transport in transports:
            result.append(case('large-config', 'current', 8192, 0, transport,
                               cache=1024 * MIB, shard_cache=512 * MIB))
    return result


def recovery_cases():
    result = []
    for old in checkpoint_async.recovery_cases():
        spec = case('recovery', old['build'], 8192, old['rep'], 'efa')
        spec.update(recovery=True, restart=old['restart'], checkpoint_batches=64,
                    tail_batches=old['tail_batches'], modes=['recovery'], cold=False)
        spec['family'] = f"ram-recovery-{spec['restart']}-t{spec['tail_batches']}-efa"
        spec['case_id'] = f"{spec['family']}-{spec['build']}-r{spec['rep']}"
        result.append(spec)
    return result


def plan(scope='full'):
    return qualification_cases() + main_cases(scope) + (recovery_cases() if scope in ('full', 'recovery') else [])


def expected_cases(scope='full'):
    return {spec['case_id'] + '-' + mode:
            (spec['family'], spec['build'], spec['value'], spec['batch'], spec['window'], spec['rep'], mode, spec['case_id'])
            for spec in plan(scope) for mode in spec['modes']}


def flags(spec, mode, external=False, prepare=False):
    result = ['--values', str(spec['value']), '--batch-sizes', str(spec['batch']),
              '--windows', str(spec['window']), '--batches', str(spec['batches']),
              '--mixed', str(spec['mixed']), '--reads', str(spec['reads']), '--repetitions', '1',
              '--transport', spec['transport'], '--tcp-read-cache-bytes', str(spec['cache']),
              '--native-read-cache-bytes', str(spec['cache'] if mode == 'native-sync' else 0),
              '--native-read-workers', str(spec['native_workers']), '--read-window', str(spec['read_window']),
              '--read-resource-evidence', 'true', '--cold-read-pass', str(spec['cold']).lower(),
              '--hot-read-pass', str(not spec['cold']).lower(),
              '--transport-diagnostics', str(spec['diagnostics']).lower()]
    if mode == 'tcp' or prepare:
        result += ['--tcp-read-lanes', str(spec['lanes'])]
        if not external or prepare:
            result += ['--tcp-server-read-workers', str(spec['workers']),
                       '--tcp-wal-member-read', spec['member'],
                       '--tcp-shard-cache-bytes', str(spec['shard_cache'])]
    if spec['build'] == 'current':
        result += ['--read-memory-allowance-bytes', str(spec['read_allowance']),
                   '--cache-probe-keys', str(spec['cache_probe_keys']),
                   '--concurrent-mixed', str(spec['concurrent_mixed']).lower()]
        if (mode == 'tcp' and not external) or prepare:
            result += ['--shard-read-memory-allowance-bytes', str(spec['shard_allowance'])]
    if prepare:
        result += ['--backend', 'tcp']
    return result


def validate_recovery_json(row, spec, action):
    # Both revisions include checkpoint recovery; baseline statistics must not
    # be discarded just because the previous campaign used an older baseline.
    checkpoint_async.validate_recovery_json(row, spec | {'build': 'current'}, action)
    if spec['build'] == 'current' and action == 'reopen':
        stats = row['recovery_stats']
        for key in ('tls_config_ns', 'read_lane_setup_ns', 'connect_worker_ns',
                    'status_worker_ns', 'tls_worker_ns'):
            checkpoint_async.positive(stats[key], True)
        if spec['transport'] == 'tcp':
            if stats['fabric_worker_ns'] is not None:
                raise ValueError('TCP must not invent fabric setup time')
        else:
            checkpoint_async.positive(stats['fabric_worker_ns'], True)


def number(text):
    value = float(text)
    if not math.isfinite(value) or value < 0:
        raise ValueError('invalid nonnegative numeric evidence')
    return value


def read_csv(path):
    with Path(path).open() as stream:
        rows = list(csv.DictReader(stream))
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f'truncated CSV: {path}')
    return rows


def read_metrics(path, spec, mode):
    path = Path(path)
    if not (path / 'COMPLETE').is_file():
        raise ValueError('missing complete value/reopen audit')
    backend = ('native_value_wal_barrier' if mode == 'native-sync' else
               {'tcp': 'experimental_tcp_authority', 'efa': 'experimental_efa_authority',
                'fabric-tcp': 'experimental_fabric_tcp_authority'}[spec['transport']])
    expected = (backend, spec['value'], spec['batch'], spec['window'], 0)
    def dimensions(row):
        actual = (row['backend'], int(row['value_bytes']), int(row['batch_size']), int(row['window']), int(row['repetition']))
        if actual != expected:
            raise ValueError('CSV shape differs from planned case')
    metrics, summaries, samples = {}, {}, defaultdict(list)
    for row in read_csv(path / 'samples.csv'):
        dimensions(row)
        records = samples[row['phase']]
        if int(row['sample']) != len(records):
            raise ValueError('sample sequence differs')
        records.append((int(row['latency_ns']), int(row['logical_operations'])))
        if min(records[-1]) < 0:
            raise ValueError('negative sample')
    metadata = {'backend', 'durability', 'value_bytes', 'batch_size', 'window', 'repetition', 'phase',
                'transport', 'sample_unit', 'rate_scope'}
    for row in read_csv(path / 'summary.csv'):
        dimensions(row)
        phase = row['phase']
        if phase in summaries or phase not in samples:
            raise ValueError('duplicate or missing phase')
        summaries[phase] = row
        records = samples[phase]
        if (int(row['samples']), int(row['logical_operations']), int(row['measured_api_ns'])) != (
                len(records), sum(r[1] for r in records), sum(r[0] for r in records)):
            raise ValueError('sample counts/operations/time disagree with summary')
        ordered = sorted(r[0] for r in records)
        for pct, minimum in ((50, 1), (95, 20), (99, 100)):
            observed = row[f'p{pct}_ns']
            if observed == '':
                if len(ordered) >= minimum:
                    raise ValueError('sufficient-sample percentile is missing')
            elif spec['build'] == 'current' and len(ordered) < minimum:
                raise ValueError('insufficient-sample percentile must be absent')
            elif int(observed) != ordered[max(0, (len(ordered) * pct + 99) // 100 - 1)]:
                raise ValueError('percentile differs from raw samples')
        rate = int(row['logical_operations']) * 1e9 / max(1, int(row['phase_wall_ns']))
        if not math.isclose(float(row['instrumented_phase_ops_per_sec']), rate, rel_tol=1e-8):
            raise ValueError('throughput differs from operation count/wall')
        if spec['build'] == 'current':
            if row['transport'] != spec['transport']:
                raise ValueError('paired transport identity differs')
            mixed = phase.startswith(('mixed_', 'concurrent_mixed_'))
            if row['rate_scope'] != ('shared_read_write_wall' if mixed else 'phase_wall'):
                raise ValueError('mixed rate lacks explicit shared-wall scope')
        for key, value in row.items():
            if key not in metadata and value != '':
                metrics['summary', phase, key] = number(value)
        if phase in ('full_scan', 'cache_probe_scan'):
            # The benchmark verifies the full returned vector before dropping it.
            # This is caller-held logical payload, not unique RSS or cache charge.
            metrics['caller_result', phase, 'logical_value_bytes_at_return'] = int(row['logical_operations']) * spec['value']
    if set(samples) != set(summaries):
        raise ValueError('summary omitted samples')
    required = {'open', 'load_write_window', 'point_get', 'point_get_many', 'point_get_window',
                'full_scan', 'maintenance_flush_index', 'shutdown', 'reopen', 'reopen_get_audit',
                'checkpoint_get', 'delete_window'}
    required |= {'concurrent_mixed_get', 'concurrent_mixed_write_window'} if spec['concurrent_mixed'] else {'mixed_get', 'mixed_write_window'}
    if spec['cache_probe_keys']:
        required |= {'cache_fill', 'cache_hot_before_scan', 'cache_probe_scan', 'cache_hot_after_scan'}
    if not required <= summaries.keys():
        raise ValueError('required workload absent')
    for phase, count in [('load_write_window', spec['batches'] * spec['batch']),
                         ('point_get', spec['reads']), ('point_get_window', spec['reads']),
                         ('reopen_get_audit', spec['batches'] * spec['batch'])]:
        if int(summaries[phase]['logical_operations']) != count:
            raise ValueError('workload operation count differs')
    for row in read_csv(path / 'stages.csv'):
        dimensions(row)
        identity = ('stage', row['phase'], row['counter'])
        if identity in metrics:
            raise ValueError('duplicate stage counter')
        metrics[identity] = number(row['delta'])
    if spec['cold']:
        physical = defaultdict(dict)
        for row in read_csv(path / 'read_io.csv'):
            dimensions(row)
            partition = int(row['partition'])
            if partition in physical[row['phase']]:
                raise ValueError('duplicate physical-read process evidence')
            physical[row['phase']][partition] = number(row['physical_read_bytes'])
        for phase in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'):
            if set(physical[phase]) != (set(range(5)) if mode == 'tcp' else {0}) or sum(physical[phase].values()) <= 0:
                raise ValueError('evicted phase lacks physical read evidence')
            metrics['physical_io', phase, 'read_bytes'] = sum(physical[phase].values())
    if mode == 'tcp':
        metrics.update(read_cache_metrics(path, spec, dimensions))
        if spec['build'] == 'current':
            windows = defaultdict(list)
            for row in read_csv(path / 'write_windows.csv'):
                dimensions(row)
                phase = row['phase']
                if int(row['sample']) != len(windows[phase]) or row['transport'] != spec['transport']:
                    raise ValueError('write-window identity differs')
                windows[phase].append(row)
                parents = sum(number(row[k]) for k in ('preparation_admission_ns', 'storage_wait_ns', 'publication_ns', 'dispatch_ns', 'checkpoint_ns'))
                if parents > number(row['api_ns']):
                    raise ValueError('exclusive write parent spans exceed API wall time')
                if number(row['foreground_checkpoint_ns']) > number(row['checkpoint_ns']):
                    raise ValueError('foreground checkpoint subset exceeds checkpoint total')
            for phase in ('load_write_window', 'delete_window', 'checkpoint_intervening_write',
                          'concurrent_mixed_write_window' if spec['concurrent_mixed'] else 'mixed_write_window'):
                if len(windows[phase]) != len(samples[phase]):
                    raise ValueError('missing per-window timing')
            for phase, rows in windows.items():
                for key in ('checkpoint_count', 'checkpoint_ns', 'foreground_checkpoint_count', 'foreground_checkpoint_ns'):
                    metrics['write_windows', phase, key] = sum(number(row[key]) for row in rows)
            if spec['value'] == 8192 and spec['batch'] == 32 and spec['batches'] == 320:
                if metrics['write_windows', 'load_write_window', 'foreground_checkpoint_count'] < 1:
                    raise ValueError('80MiB load did not record its automatic checkpoint')
    for filename, kind in (('remote_read_profiles.csv', 'profile'),
                           ('exchange_profiles.csv', 'exchange_profile'),
                           ('native_read_profiles.csv', 'native_profile')):
        grouped = defaultdict(list)
        for row in read_csv(path / filename):
            dimensions(row)
            if kind == 'native_profile':
                if row['value_cache_hit'] not in ('true', 'false'):
                    raise ValueError('invalid native cache hit evidence')
                identity = (kind, row['state'], 'value_cache_hit_count')
                metrics[identity] = metrics.get(identity, 0) + (row['value_cache_hit'] == 'true')
            for key, value in row.items():
                if key.endswith('_ns'):
                    grouped[row['state'], key].append(number(value))
        for (state, key), values in grouped.items():
            metrics[kind, state, key + '_sum'] = sum(values)
            metrics[kind, state, key + '_n'] = len(values)
            metrics[kind, state, key + '_p50'] = statistics.median(values)
            metrics[kind, state, key + '_mean'] = statistics.mean(values)
    return metrics


def read_cache_metrics(path, spec, dimensions):
    snapshots, metrics = {}, {}
    roles = {('frontend', '')} | {('storage', str(n)) for n in range(5)}
    counters = ('cache_hits', 'cache_misses', 'cache_evictions', 'wal_reads', 'wal_read_bytes')
    if spec['build'] == 'current':
        counters += ('cache_bypass_disabled', 'cache_bypass_eviction_limit', 'cache_bypass_scan',
                     'cache_bypass_generation', 'cache_admitted_charge_bytes', 'cache_served_accounted_bytes')
    for row in read_csv(path / 'read_path_evidence.csv'):
        dimensions(row)
        role = row['role'], row['partition']
        identity = row['phase'], row['boundary'], role
        if identity in snapshots or role not in roles or row['boundary'] not in ('before', 'after'):
            raise ValueError('duplicate/invalid cache snapshot')
        budget = spec['cache'] if role[0] == 'frontend' else spec['shard_cache']
        if int(row['cache_budget_bytes']) != budget or not 0 <= int(row['cache_metadata_bytes']) <= int(row['cache_retained_bytes']) <= budget:
            raise ValueError('cache budget/ownership evidence differs')
        if role[0] == 'storage':
            for current, peak, limit in (('inflight_requests', 'peak_inflight_requests', 32),
                                         ('inflight_bytes', 'peak_inflight_bytes', 64 * MIB),
                                         ('inflight_reads', 'peak_inflight_reads', 128)):
                if not 0 <= int(row[current]) <= int(row[peak]) <= limit:
                    raise ValueError('shard credit bound exceeded')
            if 'idle_buffer_bytes' in row:
                idle = int(row['idle_buffer_bytes'])
                if not 0 <= idle <= 64 * MIB - int(row['inflight_bytes']):
                    raise ValueError('idle buffers exceeded the existing pending-byte budget')
                if not 0 <= int(row['idle_buffers']) <= 128:
                    raise ValueError('idle buffer count exceeded its bound')
        snapshots[identity] = row
    phases = {identity[0] for identity in snapshots}
    if not {'point_get', 'point_get_window'} <= phases:
        raise ValueError('missing cache tier evidence')
    for phase in phases:
        for role in roles:
            before, after = (snapshots[phase, boundary, role] for boundary in ('before', 'after'))
            names = counters
            if role[0] == 'storage' and 'idle_buffer_bytes' in before:
                names += ('buffer_reuses', 'buffer_allocations')
            for key in names:
                delta = int(after[key]) - int(before[key])
                if delta < 0:
                    raise ValueError('cache counter regressed')
                name = role[0] + '-' + key
                identity = 'read_path', phase, name
                metrics[identity] = metrics.get(identity, 0) + delta
    if spec['cache_probe_keys'] and spec['build'] == 'current':
        if metrics['read_path', 'cache_probe_scan', 'storage-cache_admitted_charge_bytes'] != 0:
            raise ValueError('scan polluted shard point cache')
        if metrics['read_path', 'cache_probe_scan', 'frontend-cache_admitted_charge_bytes'] != 0:
            raise ValueError('scan polluted coordinator point cache')
    if spec['workload'] == 'pressure':
        for role in ('frontend', 'storage'):
            if metrics['read_path', 'point_get', role + '-cache_misses'] <= 0:
                raise ValueError('pressure case lacks misses')
    return metrics


def read_cloud_case(root, name, spec, mode):
    """Apply the same workload validator plus exact remote ownership/provenance."""
    from pathlib import PurePosixPath
    import re
    import analyze_cloud as audit
    root = Path(root)
    cfg = audit.read_json(root / 'campaign-config.json')
    base = PurePosixPath(cfg['run_root'])
    case_root = base / 'cases' / spec['case_id']
    binary = str(base / 'cloud-bin' / spec['build'] / 'sharded_wal_bench')
    if audit.read_json(root / (spec['case_id'] + '-plan.json')) != spec:
        raise ValueError('archived RAM case differs from immutable plan')
    process = audit.read_json(root / (name + '-process.command.json'))
    expected = [binary, '--output', str(base / 'results' / name), '--backend', mode]
    expected += flags(spec, mode, external=mode == 'tcp')
    if mode == 'tcp':
        expected += ['--remote-config', str(case_root / 'remote.yaml')]
    if (process.get('exit_code') != 0 or process['command'][0] != binary or
            efa_progress.command_options(process['command']) != efa_progress.command_options(expected)):
        raise ValueError('executed RAM command differs from exact workload/source')
    preparation = audit.read_json(root / (spec['case_id'] + '-prepare.command.json'))
    endpoints = ','.join(n['private_ip'] + ':7700' for n in sorted(cfg['nodes'], key=lambda n: n['partition']))
    expected = [binary, '--prepare-external-case', str(case_root), '--guest-case-root', str(case_root),
                '--remote-endpoints', endpoints] + flags(spec, 'tcp', prepare=True)
    if (preparation.get('exit_code') != 0 or preparation['command'][0] != binary or
            efa_progress.command_options(preparation['command']) != efa_progress.command_options(expected)):
        raise ValueError('prepared RAM node/coordinator controls differ')
    remote = (root / (spec['case_id'] + '-remote.yaml')).read_text()
    settings = [('read_lanes', spec['lanes']), ('read_cache_bytes', spec['cache'])]
    if spec['build'] == 'current':
        settings.append(('read_memory_allowance_bytes', spec['read_allowance']))
    for key, value in settings:
        if re.findall(r'^\s*' + key + r':\s*(\S+)\s*$', remote, re.M) != [str(value)]:
            raise ValueError('actual remote RAM policy differs')
    before, after = [audit.read_json(root / (name + '-' + stage + '.json')) for stage in ('before', 'after')]
    if any(e.get('instance_id') != cfg['coordinator_instance_id'] for e in (before, after)):
        raise ValueError('controller instance differs')
    hardware = efa_progress.hardware_interval(before, after, mode == 'tcp' and spec['transport'] == 'efa')
    peak = number(process['peak_sampled_main_process_rss_kib']) * 1024
    if not 0 < peak <= hardware['ram_bytes'] * 4 / 5:
        raise ValueError('controller RSS exceeds hardware allowance')
    nodes = {partition: audit.node_evidence(root, spec['case_id'], partition) for partition in range(5)}
    for partition, node in nodes.items():
        expected_id = next(n['instance_id'] for n in cfg['nodes'] if n['partition'] == partition)
        if node['instance_id'] != expected_id:
            raise ValueError('storage instance differs')
    result = read_metrics(root / name, spec, mode)
    if spec['cold']:
        for row in read_csv(root / name / 'read_io.csv'):
            pid = nodes[int(row['partition'])]['pid'] if mode == 'tcp' else process['pid']
            if int(row['pid']) != pid:
                raise ValueError('physical reads belong to another process')
    summaries = {row['phase']: row for row in read_csv(root / name / 'summary.csv')}
    result.update(efa_progress.resource_metrics(root / name / 'read_resources.csv', mode, spec,
                                               process, hardware, nodes, summaries))
    for key, value in audit.io_delta(before, after).items():
        if isinstance(value, (int, float)):
            result['hardware_io', 'whole_api_controller', key] = value
    for key, value in checkpoint_async.validate_threads(
            audit.read_json(root / (name + '-process.threads.json')), process['pid']).items():
        result['threads', 'sampled_controller_lifetimes', key] = value
    return result
