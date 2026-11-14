#!/usr/bin/env python3
"""Report an NVMe-oF data path selection (compact or core), retaining each case and repetition."""
import argparse
import csv
import json
from pathlib import Path

import analyze
from cloud_campaign import NVMEOF_PATH_SELECTIONS
import focused_analysis as focused
import leased_ab

NA = 'not available'
MISSING = 'missing'
STATEMENTS = (
    'The reference arm is the `native` backend of this source tree with the original default configuration.',
    'History persistence is reported per arm. `wal_replay` recovers from the owner WAL; `memory_only` has no restart with data, WAL file retirement or owner-RPC read.')
PAGE_CACHE = 'A put is acknowledged from the page cache of the owner. Its latency does not include the SSD. This is the contract of the original engine.'
DEVICE_WINDOW = 'Device counters cover the window between the two controller snapshots on the whole SSD. The window is longer than the measured calls and includes other writes (run 12, first launch: about 1 GiB in each 1 KiB put case on both routes; the data file system is made with default `mkfs.ext4`). `device_write_payload_ratio` is therefore an upper bound, not a write amplification.'
SETTINGS = ('group', 'arm', 'backend', 'route', 'write mode', 'acknowledgement contract', 'target I/O', 'live export mode', 'inline reads', 'read lanes', 'I/O queues', 'queue size', 'poll queues', 'owner fragment bytes', 'owner WAL file bytes', 'mapped fragments', 'history persistence', 'coordinator cache bytes', 'cache procedure', 'keys', 'key distribution', 'value bytes', 'concurrency', 'repetitions', 'cases', 'differs from engine default')
REMOTE_DEFAULTS = dict(owner_wal_geometry='native_default', history_persistence='wal_replay', coordinator_cache_bytes=8388608, read_lanes=2)
LIVE_DEFAULTS = dict(live_export_mode='on_demand', live_write_mode='reserved', live_inline_reads=True, live_io_queues=1, live_io_queue_size=32, live_poll_queues=0, target_io='buffered', live_lease_ahead=True, live_polled_writes=False)
GEOMETRY = {'native_default': (134217728, 10737418240, 16), 'research': (4194304, 1073741824, 3), 'sui_harness': (16777216, 268435456, 3)}
# The arm columns identify a row without the settings table: several arms share operation, size and concurrency.
ARM = ('route', 'tier', 'history_persistence', 'lease_ahead', 'polled_writes', 'media_expected', 'keys', 'read_lanes', 'io_queues', 'queue_size', 'poll_queues', 'inline_reads', 'diagnostic')
RESULTS = ('case_id', 'group', 'backend', 'operation', 'value_size', 'concurrency', 'repetition', 'contract', *ARM, 'status', 'p50_us', 'p99_us', 'p999_us', 'mean_us', 'calls_per_second', 'mean_inflight_calls', 'native_p50_ratio', 'calls_ge_5ms', 'time_ge_5ms_share', 'device_write_payload_ratio', 'history_waits', 'read_ops_per_success', 'device_bytes_per_read', 'media_status', 'reason_codes', 'cache_label', 'grant_misses', 'metadata_requests', 'namespace_connects', 'Control_mean_us', 'NamespaceSetup_mean_us', 'ControllerConnect_mean_us', 'coordinator_hits', 'coordinator_misses', 'owner_wal_file_count')
PROFILE_COUNTERS = {'lease_reserve_ns': 'lease_profile_samples',
                    'lease_namespace_sync_ns': 'lease_profile_samples',
                    'lease_publish_ns': 'lease_profile_samples',
                    'adopt_owner_ns': 'adopt_profile_samples',
                    'adopt_close_scan_ns': 'adopt_profile_samples',
                    'adopt_close_scan_bytes': 'adopt_profile_samples'}
RESULTS += tuple(name + '_mean' for name in PROFILE_COUNTERS) + (
    'lease_profile_samples', 'adopt_profile_samples', 'extent_preleases', 'extent_switch_waits')
RESULTS += ('first_call_us', 'contract_class')
STALL_FIELDS = ('case_id', 'first_sample', 'calls', 'max_api_us', 'label', 'switch_us', 'first_measured_call')
STAGE_FIELDS = ('case_id', 'stage_pair', 'mean_us', 'p50_us', 'share')


def load(path):
    return leased_ab._load(Path(path))


def contract(job):
    tier = job.get('native_durability' if job.get('backend') == 'native' else 'durability_tier', MISSING)
    return 'page_cache' if tier in ('native', 'page_cache') else 'durable' if tier in ('synchronous', 'value_wal_barrier') else tier


def contract_class(job):
    return {'durable': 'S', 'page_cache': 'P'}.get(contract(job))


def operation(job):
    return next((k for k, v in job.get('mix', {}).items() if v), NA)


def live(job):
    return job.get('backend') == 'tidehunter_nvme_rdma' and job.get('nvmeof_route') == 'live'


def settings(jobs):
    arms = {}
    errors = []
    for job in jobs:
        # A repetition has its own data set identity; the canary flag is on one repetition only.
        identity = {k: v for k, v in job.items() if k not in ('case_id', 'repeat', 'require_media_qualified') and 'seed' not in k}
        if isinstance(identity.get('sealed_dataset'), dict):
            identity['sealed_dataset'] = {k: v for k, v in identity['sealed_dataset'].items() if k not in ('data_id', 'source_id')}
        key = json.dumps(identity, sort_keys=True)
        arms.setdefault(key, []).append(job)
    rows = []
    for members in arms.values():
        j = members[0]
        native = j.get('backend') == 'native'
        floor = j.get('group') == 'G18F'
        defaults = dict(native_wal_geometry='native_default', native_shards=1) if native else REMOTE_DEFAULTS | (LIVE_DEFAULTS if live(j) else {})
        required = (set(defaults) - {'live_lease_ahead', 'live_polled_writes'}) | {'backend', 'keys', 'distribution', 'value_size', 'concurrency', 'repeat', 'native_durability' if native else 'durability_tier'}
        if not floor:
            for member in members:
                errors.extend(member.get('case_id', '?') + ': ' + k for k in sorted(required - member.keys()))
        get = lambda k: j.get(k, LIVE_DEFAULTS[k] if k in ('live_lease_ahead', 'live_polled_writes') else MISSING)
        geometry = GEOMETRY.get(get('native_wal_geometry' if native else 'owner_wal_geometry'), (MISSING,) * 3)
        if not native and 'owner_wal_file_bytes' in j:
            geometry = (geometry[0], j['owner_wal_file_bytes'], geometry[2])
        differences = '; '.join(k + '=' + (str(get(k)).lower() if isinstance(get(k), bool) else str(get(k))) for k, default in defaults.items() if get(k) != default)
        if not native and 'owner_wal_file_bytes' in j and geometry[1] != GEOMETRY['native_default'][1]:
            differences = '; '.join(filter(None, (differences, 'owner_wal_file_bytes=' + str(geometry[1]))))
        cache = '; '.join(k + '=' + str(j[k]).lower() for k in ('drop_os_caches', 'evict_mapped_wal_before_measurement', 'warmup_key_sweep') if k in j)
        if 'media' in j:
            cache += '; media.expected=' + str(j['media'].get('expected', MISSING))
        required = sorted(str(m.get('repeat', MISSING)) for m in members if m.get('require_media_qualified'))
        if required:
            cache += '; require_media_qualified on repetition ' + ', '.join(required)
        values = (get('group'), j.get('case_id', str(len(rows))), get('backend'), 'native' if native else 'live NVMe-oF' if live(j) else 'owner RPC', get('live_write_mode') if live(j) else 'not applicable', contract(j), get('target_io') if live(j) else 'not applicable', get('live_export_mode') if live(j) else 'not applicable', get('live_inline_reads') if live(j) else 'not applicable', get('read_lanes') if not native else 'not applicable', get('live_io_queues') if live(j) else 'not applicable', get('live_io_queue_size') if live(j) else 'not applicable', get('live_poll_queues') if live(j) else 'not applicable', *geometry, get('history_persistence') if not native else 'not applicable', get('coordinator_cache_bytes') if not native else 'not applicable', cache, get('keys'), get('distribution'), get('value_size'), get('concurrency'), ', '.join(str(x) for x in sorted({m.get('repeat', MISSING) for m in members}, key=str)), len(members), differences)
        rows.append(dict(zip(SETTINGS, values)))
        rows[-1]['arm'] = str(j.get('group', 'unknown')) + '-arm-' + str(len(rows))
        if floor:
            # A floor case runs a network or SSD tool on the hosts. It starts no store.
            tool = j.get('net') or j.get('ssd') or j.get('ssd_write') or {}
            label = ' '.join(str(tool[k]) for k in ('tool', 'mode', 'block_bytes') if k in tool)
            rows[-1] = dict.fromkeys(SETTINGS, 'not applicable') | {k: rows[-1][k] for k in ('group', 'arm', 'repetitions', 'cases')} | {'backend': 'none', 'route': 'floor: ' + str(j.get('case_kind', MISSING)) + (' ' + label if label else '')}
    return rows, errors


def nonnegative(value):
    return value if isinstance(value, (int, float)) and value >= 0 else None


def find_values(data, key):
    if isinstance(data, dict):
        if key in data:
            yield data[key]
        for value in data.values():
            yield from find_values(value, key)
    elif isinstance(data, list):
        for value in data:
            yield from find_values(value, key)


def first(data, *keys):
    for key in keys:
        for value in find_values(data, key):
            if value is not None:
                return value
    return None


def first_measured_sample(samples):
    """The first API call is the observed dispatch with the earliest start."""
    return min((sample for sample in samples if sample['dispatch_observed'] == 'true'),
               key=lambda sample: sample['dispatched_ns'], default=None)


def result_row(case, export):
    j = case['job']; op = operation(j)
    directory = export / 'cases' / case['case_id'] / 'result'
    row = dict.fromkeys(RESULTS)
    native = j['backend'] == 'native'
    row.update(case_id=case['case_id'], group=j['group'], backend=j['backend'], operation=op, value_size=j.get('value_size'), concurrency=j.get('concurrency'), repetition=j.get('repeat'), contract=contract(j), status=case.get('status', 'not_run'),
               route='native' if native else 'live NVMe-oF' if live(j) else 'owner RPC', tier=j.get('native_durability' if native else 'durability_tier'),
               media_expected=j.get('media', {}).get('expected'), keys=j.get('keys'), read_lanes=None if native else j.get('read_lanes'),
               io_queues=j.get('live_io_queues'), queue_size=j.get('live_io_queue_size'), poll_queues=j.get('live_poll_queues'), inline_reads=j.get('live_inline_reads'),
               diagnostic='sustained' if j.get('measurement_operations') == 200000 else 'history diagnostics' if j.get('owner_history_diagnostics') else None)
    row['contract_class'] = contract_class(j)
    row.update(history_persistence=None if native else j.get('history_persistence', 'on_disk'),
               lease_ahead=j.get('live_lease_ahead', True) if live(j) else None,
               polled_writes=j.get('live_polled_writes', False) if live(j) else None)
    samples = list(analyze.read_samples(directory / 'samples.csv')) if (directory / 'samples.csv').is_file() and case.get('status') == 'completed' else []
    first_call = first_measured_sample(samples)
    row['first_call_us'] = first_call['api_ns'] / 1000 if first_call is not None else None
    good = [s['api_ns'] for s in samples if s['operation'] == op and s['outcome'] in ('ok', 'success') and nonnegative(s['api_ns']) is not None]
    if good:
        span = max(s['completed_ns'] for s in samples) - min(s['scheduled_ns'] for s in samples)
        for name, q in (('p50', .5), ('p99', .99), ('p999', .999)):
            row[name + '_us'] = analyze.quantile(good, q) / 1000
        row.update(mean_us=sum(good) / len(good) / 1000, calls_per_second=len(good) * 1e9 / span if span > 0 else None, mean_inflight_calls=sum(good) / span if span > 0 else None)
        slow = [x for x in good if x >= 5000000]
        row.update(calls_ge_5ms=len(slow), time_ge_5ms_share=sum(slow) / sum(good) if sum(good) else None)
    boundary = load(directory / 'measurement-read-evidence.json')
    counters = (boundary.get('live_nvmeof') or {}).get('counters') or {}
    for name in ('grant_misses', 'metadata_requests', 'namespace_connects'):
        row[name] = nonnegative(counters.get(name))
    for name, denominator in PROFILE_COUNTERS.items():
        value, count = nonnegative(counters.get(name)), nonnegative(counters.get(denominator))
        row[name + '_mean'] = value / count if value is not None and count else None
    for name in ('lease_profile_samples', 'adopt_profile_samples', 'extent_preleases', 'extent_switch_waits'):
        row[name] = nonnegative(counters.get(name))
    for title, stage in (('Control', 'control'), ('NamespaceSetup', 'namespace_setup'), ('ControllerConnect', 'controller_connect')):
        row[title + '_mean_us'] = leased_ab._mean_stage(counters, stage) if nonnegative(counters.get(stage + '_ns')) is not None and (nonnegative(counters.get(stage + '_samples')) or 0) > 0 else None
    coordinator = ((boundary.get('deltas') or {}).get('coordinator') or {}).get('counters') or {}
    for target, names in (('coordinator_hits', ('hits', 'value_cache_hits', 'read_profile_cache_hits')), ('coordinator_misses', ('misses', 'value_cache_misses', 'read_profile_cache_misses'))):
        row[target] = nonnegative(first(coordinator, *names))
    media = load(directory.parent / 'media_evidence.json') or load(directory / 'media_evidence.json') or case.get('focused_evidence', {}).get('media_evidence', {})
    for name in ('read_ops_per_success', 'device_bytes_per_read'):
        row[name] = nonnegative(media.get(name))
    qualification = media.get('qualification') or media
    row.update(media_status=qualification.get('status'), reason_codes=qualification.get('reason_codes', qualification.get('reasons')))
    if j.get('media', {}).get('expected') == 'cold' and row['media_status'] != 'qualified':
        row['cache_label'] = 'partly cached'
    elif j['group'] == 'G18M':
        row['cache_label'] = 'cache case'
    devices = load(directory / 'device_counters.json') or load(directory.parent / 'device_counters.json')
    before, after = devices.get('before'), devices.get('after')
    if before is not None and after is not None and good and j.get('value_size'):
        a = list(find_values(after, 'write_sectors')); b = list(find_values(before, 'write_sectors'))
        if a and len(a) == len(b):
            delta = nonnegative(sum(a) - sum(b))
            if delta is not None:
                row['device_write_payload_ratio'] = 512 * delta / (len(good) * j['value_size'] * (j.get('batch_size', 32) if op == 'put_batch' else 1))
    history = load(directory / 'owner-history-maintenance.json') or load(directory.parent / 'owner-history-maintenance.json')
    if not history:
        history = [load(path) for path in sorted((directory.parent / 'owner-durability').glob('*.history-maintenance.json'))]
    # One record per owner, one entry per epoch: report the total.
    waits = [value for value in find_values(history, 'waits') if nonnegative(value) is not None]
    row['history_waits'] = sum(waits) if waits else None
    # The end observation of owner 0 lists the WAL files beneath its storage root.
    observation = load(directory.parent / 'live-observation' / 'end-0.json')
    images = observation.get('images')
    if isinstance(images, list) and (observation.get('discovery') or {}).get('images_available') is True:
        row['owner_wal_file_count'] = len(images)
    return row


def stall_rows(case, export):
    """Attribute slow raw calls using only the coordinator monotonic clock."""
    directory = Path(export) / 'cases' / case['case_id'] / 'result'
    evidence = load(directory / 'write-timeline.json')
    timeline = evidence.get('timeline') or {}
    if not timeline or not (directory / 'samples.csv').is_file():
        return []
    samples = list(analyze.read_samples(directory / 'samples.csv'))
    samples_by_id = {}
    for sample in samples:
        sample_id = sample.get('caller_sample_id')
        if sample_id is not None:
            if sample_id in samples_by_id:
                raise ValueError('duplicate caller sample ID in raw samples')
            samples_by_id[sample_id] = sample
    records = {}
    for record in timeline.get('records', []):
        sample_id = record['caller_sample_id']
        if sample_id in records:
            raise ValueError('duplicate caller sample ID in timeline')
        records[sample_id] = record
    first_call = first_measured_sample(samples)
    first_record = records.get(first_call.get('caller_sample_id'), {}) if first_call is not None else {}
    first_batch = first_record.get('batch')
    first_group_batches = set()
    if first_batch is not None:
        for group in timeline.get('live_groups', []):
            batches = {(batch['epoch'], batch['sequence']) for batch in group['batches']}
            if (first_batch['epoch'], first_batch['sequence']) in batches:
                first_group_batches.update(batches)
    generations = {}
    for event in timeline.get('checkpoint_events', []):
        if event.get('generation') is not None:
            generations.setdefault(event['generation'], []).append(event)
    intervals = []
    for events in generations.values():
        events = sorted(events, key=lambda event: event['elapsed_ns'])
        intervals.extend(zip(events, events[1:]))
    rows = []
    for sample in samples:
        sample_id = sample.get('caller_sample_id')
        record = records.get(sample_id, {})
        if (sample.get('api_ns') or 0) < 5_000_000:
            continue
        events = record.get('events', [])
        label, switch_us = 'unattributed', None
        starts = [event['elapsed_ns'] for event in events
                  if event.get('stage') == 'live_extent_switch_started']
        ends = [event['elapsed_ns'] for event in events
                if event.get('stage') == 'live_extent_switch_completed']
        if starts:
            label = 'extent_switch'
            if len(starts) == len(ends) == 1 and ends[0] >= starts[0]:
                switch_us = (ends[0] - starts[0]) / 1000
        else:
            starts = [event['elapsed_ns'] for event in events if event.get('stage') == 'caller_start']
            ends = [event['elapsed_ns'] for event in events if event.get('stage') == 'caller_finished']
            if starts and ends:
                overlaps = [(max(0, min(max(ends), b['elapsed_ns']) -
                                  max(min(starts), a['elapsed_ns'])), a['stage'])
                            for a, b in intervals]
                overlap, stage = max(overlaps, default=(0, ''))
                if overlap:
                    label = 'checkpoint:' + stage
        rows.append(dict(case_id=case['case_id'], first_sample=sample_id,
                         calls=1, max_api_us=sample['api_ns'] / 1000,
                         label=label, switch_us=switch_us,
                         first_measured_call=sample is first_call or (
                             record.get('batch') is not None and
                             (record['batch']['epoch'], record['batch']['sequence']) in first_group_batches)))
    return rows


def reference_key(row, job):
    reading = row['operation'] in ('read', 'get_many')
    return (row['operation'], row['value_size'], row['concurrency'], row['repetition'], row['contract'] if not reading else job.get('media', {}).get('expected'), job.get('batch_size') if row['operation'] in ('get_many', 'put_batch') else None, job['keys'])


def write_csv(path, fields, rows):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows({k: NA if row.get(k) is None else row[k] for k in fields} for row in rows)


def floors(cases, export):
    rows = []
    for case in cases:
        j = case['job']; evidence = case.get('focused_evidence', {})
        row = focused.floor_base(case)
        kind = j.get('case_kind')
        if kind == 'net_floor':
            job = j['net']; runs = evidence.get('net_floor_result', {}).get('runs', [])
            row.update(tool=job.get('tool'), mode=job.get('mode'))
            for size in job['sizes']:
                run = next((r for r in runs if r.get('size_bytes') == size and r.get('status') == 'completed'), {})
                rows.append(row | {'size_bytes': size, **{k + '_us': run.get('latency_us', {}).get(k) for k in ('p50', 'p99', 'p999', 'avg')}})
        elif kind == 'ssd_floor':
            job = j['ssd']; e = evidence.get('ssd_floor_result', {}); log = e.get('lat_log', {})
            metrics = focused.ssd_log_metrics(export / 'cases' / case['case_id'] / log['path'], job['region_bytes'], job['runtime_seconds']) if log.get('path') and (export / 'cases' / case['case_id'] / log['path']).is_file() else {}
            rows.append(row | {'size_bytes': job['block_bytes'], **metrics, **{k + '_us': focused.latency(e, 'latency', k) for k in ('p99', 'p999')}})
        else:
            job = j.get('ssd_write', {}); e = evidence.get('ssd_write_floor_result', {}); log = e.get('lat_log', {})
            available = log.get('path') and (export / 'cases' / case['case_id'] / log['path']).is_file()
            rows.append(row | {'mode': job.get('mode'), 'size_bytes': job.get('block_bytes'), **{k + '_us': nonnegative(e.get('total', {}).get(k)) / 1000 if available and nonnegative(e.get('total', {}).get(k)) is not None else None for k in ('mean', 'p50', 'p99', 'p999')}})
    return rows


def stage_rows(case):
    """Decompose complete captured calls into adjacent coordinator intervals."""
    case = Path(case)
    timeline = load(case / 'result' / 'write-timeline.json').get('timeline') or {}
    calls = []
    pairs = {}
    total_ns = 0
    for record in timeline.get('records', []):
        events = record.get('events', [])
        if not events or events[0]['stage'] != 'caller_start' or events[-1]['stage'] != 'caller_finished':
            continue
        durations = {}
        for start, end in zip(events, events[1:]):
            elapsed = end['elapsed_ns'] - start['elapsed_ns']
            if elapsed < 0:
                raise ValueError('write timeline events are not in clock order')
            pair = start['stage'] + ' -> ' + end['stage']
            pairs.setdefault(pair, None)
            durations[pair] = durations.get(pair, 0) + elapsed
        calls.append(durations)
        total_ns += events[-1]['elapsed_ns'] - events[0]['elapsed_ns']
    rows = []
    for pair in pairs:
        values = [call.get(pair, 0) for call in calls]
        total = sum(values)
        rows.append(dict(case_id=case.name, stage_pair=pair,
                         mean_us=total / len(calls) / 1000,
                         p50_us=analyze.quantile(values, .5) / 1000,
                         share=total / total_ns if total_ns else None))
    return rows


def generate(export=None, report=None, output=None, matrix=None, settings_only=False):
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    source = Path(matrix) if matrix else Path(export) / 'matrix.json'
    data = json.loads(source.read_text())
    jobs = data if isinstance(data, list) else data['jobs']
    selection = jobs[0].get('selection', NVMEOF_PATH_SELECTIONS[0]) if jobs else NVMEOF_PATH_SELECTIONS[0]
    for crossing in ('nvmeof_contracts_crossing', 'nvmeof_contracts_crossing2'):
        if any(j.get('selection') == crossing for j in jobs):
            selection = crossing
    if isinstance(data, dict):
        selection = data.get('spec', {}).get('selection', selection)
    if selection not in NVMEOF_PATH_SELECTIONS:
        raise ValueError('export selection is not a reviewed NVMe-oF data path selection')
    if any(j.get('group') not in ('G18F', 'G18N', 'G18W', 'G18R', 'G18M', 'G18B', 'G18X', 'G19W', 'G19R', 'G19N', 'G19X', 'G20W', 'G20R', 'G20N', 'G21W', 'G21R', 'G21N', 'G22W') for j in jobs):
        raise ValueError('unexpected case group')
    setting_rows, errors = settings(jobs)
    records = [data]
    for root in (source.parent, Path(export) if export else source.parent):
        for name in ('receipt.json', 'startup.json', 'package-receipt.json', 'state.json', 'source.json'):
            records.append(load(root / name))
    lines = [f'# NVMe-oF data path — {selection}', '', '## Run identity', '']
    identities = {'fleet id': ('fleet_id', 'run_id'), 'machine type': ('machine_type', 'instance_type'), 'kernel release': ('kernel_release',), 'nvmet module identity': ('module_sha256',), 'binary SHA-256': ('binary_sha256',), 'source tree SHA-256': ('source_tree_sha256', 'source_sha256'), 'matrix SHA-256': ('matrix_sha256',)}
    lines.extend('- ' + label + ': ' + str(first(records, *keys) or NA) for label, keys in identities.items())
    lines.extend(['', *STATEMENTS, '', 'Matrix input accepts a job list or a wrapper with a `jobs` list.', '',
                  'The source tree retains the value-cache refill correctness fix and lock/shutdown ordering fixes. The buffered file read does not zero its buffer, as in the original engine. WAL and index formats are unchanged.',
                  'The live route preallocates WAL files; the original engine uses sparse files. The synchronous tier is outside the paper page-cache contract.', ''])
    for label, acknowledgement in (('S — synced', 'durable'), ('P — page cache', 'page_cache'),
                                    ('not applicable', 'not applicable'), ('unknown', MISSING)):
        members = [row for row in setting_rows if row['acknowledgement contract'] == acknowledgement]
        if members:
            leased_ab._table(lines, 'Settings — ' + label, SETTINGS, members)
    write_csv(output / 'settings.csv', SETTINGS, setting_rows)
    (output / 'nvmeof_path.md').write_text('\n'.join(lines))
    if errors:
        raise ValueError('missing required settings: ' + '; '.join(errors))
    if settings_only:
        return setting_rows
    export = Path(export); cases = load(Path(report) / 'comparison.json')['cases']
    planned = {j['case_id']: j for j in jobs}
    if len(planned) != len(jobs) or len(cases) != len(jobs) or len({c['case_id'] for c in cases}) != len(cases) or any(c.get('job') != planned.get(c.get('case_id')) for c in cases):
        raise ValueError('report cases do not match export matrix')
    rows = [result_row(c, export) for c in cases if c['job']['group'] != 'G18F']
    references = {}
    for row in rows:
        if row['backend'] == 'native' and row['status'] == 'completed' and row['p50_us'] is not None:
            references.setdefault(reference_key(row, planned[row['case_id']]), []).append(row)
    for row in rows:
        refs = references.get(reference_key(row, planned[row['case_id']]), [])
        if len(refs) == 1 and refs[0]['p50_us'] > 0 and row['p50_us'] is not None:
            row['native_p50_ratio'] = row['p50_us'] / refs[0]['p50_us']
    lines.extend(['One row per case. Quantiles use exact nearest rank. Repetitions are not pooled.', '', PAGE_CACHE, '', DEVICE_WINDOW, ''])
    tables = {'puts': [r for r in rows if r['group'] in ('G18N', 'G18W') and r['operation'] in ('put', 'put_batch')], 'reads': [r for r in rows if r['group'] in ('G18N', 'G18R', 'G18M') and r['operation'] in ('read', 'get_many')], 'batches': [r for r in rows if r['group'] == 'G18B'], 'many_files': [r for r in rows if r['group'] == 'G18X'], 'floors': floors([c for c in cases if c['job']['group'] == 'G18F'], export)}
    tables['puts'] += [r for r in rows if r['group'] in ('G19N', 'G19W', 'G20N', 'G20W', 'G21N', 'G21W', 'G22W') and r['operation'] in ('put', 'put_batch')]
    tables['reads'] += [r for r in rows if r['group'] in ('G19N', 'G19R', 'G20N', 'G20R', 'G21N', 'G21R') and r['operation'] in ('read', 'get_many')]
    tables['many_files'] += [r for r in rows if r['group'] == 'G19X']
    write_csv(output / 'stalls.csv', STALL_FIELDS,
              [row for case in cases for row in stall_rows(case, export)])
    write_csv(output / 'stages.csv', STAGE_FIELDS,
              [row for case in cases for row in stage_rows(export / 'cases' / case['case_id'])])
    lines.append('Stages use complete captured coordinator calls. Missing adjacent pairs contribute zero per call; their shares use the mean captured call duration.')
    for name, entries in tables.items():
        fields = tuple(dict.fromkeys(k for r in entries for k in r)) if name == 'floors' and entries else RESULTS
        write_csv(output / (name + '.csv'), fields, entries)
        if name == 'floors':
            leased_ab._table(lines, name.title(), fields, entries)
        else:
            for label in ('S', 'P', None):
                members = [row for row in entries if row['contract_class'] == label]
                if members:
                    leased_ab._table(lines, name.replace('_', ' ').title() + ' — class ' + (label or 'unknown'),
                                     fields, members)
    (output / 'nvmeof_path.md').write_text('\n'.join(lines))
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('export', 'report', 'matrix'):
        parser.add_argument('--' + name)
    parser.add_argument('--output', required=True)
    parser.add_argument('--settings-only', action='store_true')
    args = parser.parse_args(argv)
    if (args.settings_only and not args.matrix) or (not args.settings_only and (not args.export or not args.report)):
        parser.error('settings-only needs --matrix; results need --export and --report')
    try:
        generate(**vars(args))
    except (ValueError, KeyError) as error:
        parser.exit(1, str(error) + '\n')


if __name__ == '__main__':
    main()
