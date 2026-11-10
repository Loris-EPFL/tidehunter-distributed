#!/usr/bin/env python3
"""Render the performance plan from its template and archived benchmark CSVs."""
import csv
import hashlib
import json
import re
import statistics
from collections import defaultdict
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
WORK = REPO.parent / 'checkpoint-work'
RUN8 = WORK / 'focused-launch-20261109-unified2' / 'analysis' / 'output'
RUN7 = WORK / 'focused-launch-20261109-g10b' / 'analysis-final-v2' / 'output'
CROSS = REPO / 'docs' / 'research' / 'supervisor-brief-2026-11-10'
TEMPLATE = HERE / 'PLAN.template.md'
OUTPUT = HERE.parent / 'PERFORMANCE_PLAN_2026-11-10.md'
APPENDIX = HERE.parent / 'PERFORMANCE_PLAN_2026-11-10-APPENDIX.md'
CAMPAIGN = HERE.parent / 'FOCUSED_CAMPAIGN_2026-11-07.md'
SUI_TRACE = WORK / 'sui-kv-trace-20261107/captures-corrected/remote/events.tsv'
SUI_REPORT = WORK / 'sui-kv-trace-20261107/REPORT.md'
HISTORICAL = WORK / 'gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/metrics-all-populations.csv'
inputs = {TEMPLATE, Path(__file__).resolve()}
cache = {}

CONSTANTS = {
    'date': '10 November 2026', 'run8': '8', 'run7': '7',
    'kib1': '1 KiB', 'kib8': '8 KiB', 'kib64': '64 KiB', 'c1': 'c1', 'c16': 'c16',
    'batch32': '32', 'percent13': '13%', 'duration45': '45 minutes', 'vm6': '6 VMs',
    'vmrate': '$3.29 per VM-hour', 'vmhours': '13.17 VM-hours', 'vmcost': '$43.3',
    'launchcost': '$15', 'reservation': '$145', 'weeklycap': '$290', 'week': '46',
    'fillcases': '50 cases', 'fillreps': '2 repetitions', 'fillhours': '1.4 fleet hours',
    'modes4': 'four', 'sizes3': 'three', 'reps2': 'two', 'reps3': 'three',
    'profreps1': 'one', 'workers1': 'one', 'workers16': 'sixteen', 'host0': 'zero',
    'kib128': '128 KiB',
    'completed8': '265', 'total8': '270',
    'firstsecond': 'first second', 'lastfiveseconds': 'last five complete one-second buckets',
}


def load(path):
    if path not in cache:
        inputs.add(path)
        with path.open(newline='', encoding='utf-8') as f:
            cache[path] = list(csv.DictReader(f))
    return cache[path]


def source(name):
    if name.startswith('run7/'):
        return RUN7 / name.removeprefix('run7/')
    if name.startswith('cross/'):
        return CROSS / name.removeprefix('cross/')
    return RUN8 / name


def fmt(value, kind):
    if value in (None, '', 'missing', 'not computed', 'n/c'):
        return 'not measured'
    try:
        number = float(value)
    except (ValueError, TypeError):
        return str(value)
    if kind == 'us':
        return f'{number:.1f}'
    if kind == 'share':
        percent = number * 100
        return f'{percent:.1f}%' if abs(percent) < 10 else f'{percent:.0f}%'
    if kind == 'ratio':
        return f'{number:.2f}'
    if kind == 'int':
        return f'{number:.0f}'
    return str(value)


case_index = {}


def row_id(value):
    if not value or value in ('missing', 'not measured'):
        return 'not measured'
    ids = json.loads(value) if isinstance(value, str) and value.startswith('[') else value
    if isinstance(ids, str):
        ids = [ids]
    ids = sorted(set(ids))
    key = 'R' + hashlib.sha256('\n'.join(ids).encode()).hexdigest()[:8]
    if key in case_index and case_index[key] != ids:
        raise ValueError(f'case ID collision: {key}')
    case_index[key] = ids
    return key


def cell(value):
    value = str(value if value not in (None, '', 'missing') else 'not measured')
    return value.replace('|', r'\|').replace('\n', ' ')


def table(columns, rows):
    return '\n'.join(['| ' + ' | '.join(map(cell, columns)) + ' |',
                      '| ' + ' | '.join('---' for _ in columns) + ' |'] +
                     ['| ' + ' | '.join(cell(v) for v in row) + ' |' for row in rows])


def readable(value, column):
    if column in ('case_id', 'case_ids'):
        return row_id(value)
    if isinstance(value, str) and value[:1] in ('[', '{'):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return ', '.join(f'{k}: {v}' for k, v in parsed.items()) or 'none'
        return ', '.join(map(str, parsed)) or 'none'
    if column.endswith('_us'):
        return fmt(value, 'us')
    if column in ('p50_ratio', 'p99_ratio', 'device_reads_per_call'):
        return fmt(value, 'ratio')
    return value


def fields(rows, columns):
    return table(['ID' if c in ('case_id', 'case_ids') else c for c in columns],
                 ([readable(r.get(c, 'not measured'), c) for c in columns] for r in rows))


def size_label(value):
    return f'{int(value) // 1024} KiB' if int(value) % 1024 == 0 else f'{value} B'


def operation_table(rows):
    out = []
    for r in sorted(rows, key=lambda r: (int(r['size_bytes']), int(r['concurrency']), r['route'], r['shape'], r['durability'], r['cache_state'], r['key_policy'])):
        counts = json.loads(r['n'])
        verdicts = json.loads(r['verdicts'])
        verdict = ', '.join(f'{v} {verdicts.count(v)}/{len(verdicts)}' for v in dict.fromkeys(verdicts)) or 'not measured'
        if r['route'] == 'native' and r['shape'] == 'read' and r['size_bytes'] == '8192' and r['cache_state'] == 'cold':
            verdict = 'passes device-count gate; memory-like latency'
        contract = r['durability']
        if r['key_policy'] != 'overwrite':
            contract += '; ' + r['key_policy']
        diagnostics = json.loads(r['diagnostics'])
        profile = ('write timeline' if 'write_timeline_limit' in diagnostics else
                   'write profile' if 'write_profile_limit' in diagnostics else
                   'read profile' if 'read_profile_limit' in diagnostics else 'unprofiled')
        contract += '; ' + profile
        span = json.loads(r['p50_range_us']) if r['p50_range_us'].startswith('{') else {}
        p50range = (f"{fmt(span['min'], 'us')} to {fmt(span['max'], 'us')}"
                    if span.get('min') is not None and span.get('max') is not None else 'not measured')
        out.append([row_id(r['case_ids']), r['route'], r['shape'], size_label(r['size_bytes']),
                    r['concurrency'], contract, r['cache_state'],
                    ' + '.join(str(v) for v in counts.values()), r['repetition_count'],
                    *[fmt(r[k], 'us') for k in ('mean_us', 'p50_us', 'p99_us', 'p999_us')],
                    p50range, fmt(r['device_reads_per_call'], 'ratio'), verdict])
    return table(['ID', 'Route', 'Shape', 'Size', 'c', 'Contract', 'Cache', 'n', 'Reps',
                  'Mean µs', 'p50 µs', 'p99 µs', 'p99.9 µs', 'p50 range µs', 'Reads/call', 'Verdict'], out)




def operations(kind):
    rows = load(RUN8 / 'ops_summary.csv')
    def selected(r):
        if kind == 'reads':
            return r['shape'] == 'read' and r['diagnostics'] == '{}'
        if kind == 'batch_delete':
            return r['shape'] in ('put_batch', 'delete') and r['diagnostics'] == '{}'
        if kind == 'reference':
            return (r['key_policy'] == 'overwrite'
                    and r['shape'] in ('read', 'put') and r['size_bytes'] in ('1024', '65536')
                    and r['concurrency'] in ('1', '16')
                    and ((r['shape'] == 'read' and (r['diagnostics'] == '{}' or 'read_profile' in r['diagnostics']))
                         or (r['shape'] == 'put' and r['diagnostics'] == '{}'
                             and r['durability'] in ('synchronous', 'value_wal_barrier'))))
        return (r['shape'] == 'put' and r['diagnostics'] == '{}'
                and ((r['durability'] in ('synchronous', 'value_wal_barrier')) == (kind == 'durable')))
    note = ('Run 8; fleet bench-0a8a6af114a64287. Counts are per repetition; timings are medians '
            'of repetition statistics, not pooled calls. IDs resolve in the case appendix. ')
    if kind == 'reads':
        note += 'Unprofiled distributed read copies were not run; see profiled decomposition. '
    return note + 'Each row is one population; p50 range spans repetitions.\n\n' + operation_table([r for r in rows if selected(r)])


def decomposition():
    comps = load(RUN8 / 'component_summary.csv')
    ops = load(RUN8 / 'ops_summary.csv')
    bycase = defaultdict(list)
    for r in comps:
        bycase[r['case_id']].append(r)
    specifications = [
        ('Durable put 1 KiB c1', 'put', '1024', '1', 'synchronous', '', ['owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel DIRECT', 'hybrid SPDK']),
        ('Native barrier put 1 KiB c1', 'put', '1024', '1', 'value_wal_barrier', '', ['native']),
        ('Durable put 64 KiB c1', 'put', '65536', '1', 'synchronous', '', ['owner verbs', 'owner TCP', 'native']),
        ('Durable put 1 KiB c16', 'put', '1024', '16', 'synchronous', '', ['owner verbs', 'owner TCP', 'native']),
        ('Page-cache put 1 KiB c1', 'put', '1024', '1', 'native', '', ['owner verbs', 'native']),
        ('Delete 1 KiB c1', 'delete', '1024', '1', 'synchronous', '', ['owner verbs', 'owner TCP', 'native']),
        ('Batch 32 × 8 KiB c1', 'put_batch', '8192', '1', 'synchronous', '', ['owner verbs', 'owner TCP']),
        ('Resident read 1 KiB c1', 'read', '1024', '1', 'synchronous', 'memory', ['owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel buffered']),
        ('Resident read 64 KiB c1', 'read', '65536', '1', 'synchronous', 'memory', ['owner verbs', 'owner TCP', 'hybrid kernel buffered']),
        ('Cold read 1 KiB c1', 'read', '1024', '1', 'synchronous', 'cold', ['owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel DIRECT', 'hybrid SPDK']),
        ('Cold read 64 KiB c1', 'read', '65536', '1', 'synchronous', 'cold', ['owner verbs', 'owner TCP', 'hybrid kernel DIRECT', 'hybrid SPDK']),
    ]
    output = ['Run 8; fleet bench-0a8a6af114a64287; one profiled repetition per route. Cells show mean µs (share of API mean). A dash means not computed or not measured; rows with no computed values are omitted. Components can overlap. Owner synchronous waits for fdatasync; native barrier syncs WAL and directory. Read state is given in the title.']
    for title, shape, size, concurrency, durability, state, routes in specifications:
        chosen = {}
        for route in routes:
            expected = ('page_cache' if durability == 'native' else 'value_wal_barrier') if route == 'native' else durability
            matches = [r for r in ops if r['route'] == route and r['shape'] == shape
                       and r['size_bytes'] == size and r['concurrency'] == concurrency
                       and r['durability'] == expected and r['key_policy'] == 'overwrite'
                       and (not state or r['cache_state'] == state) and r['diagnostics'] != '{}']
            matches.sort(key=lambda r: ('write_profile' in r['diagnostics'], r['case_ids']))
            if matches:
                chosen[route] = matches[0]
        component_names = list(dict.fromkeys(x['component'] for r in chosen.values()
                                             for x in bycase[json.loads(r['case_ids'])[0]]))
        rows = []
        dropped = []
        for name in component_names:
            values, kinds = [], set()
            for route in routes:
                r = chosen.get(route)
                parts = [x for x in bycase[json.loads(r['case_ids'])[0]] if x['component'] == name] if r else []
                if not parts or parts[0]['mean_us'] in ('', 'missing', 'not computed'):
                    values.append('—')
                    continue
                x = parts[0]
                kinds.add(x['exactness'].replace('_', ' '))
                share = fmt(x['share_of_api_mean'], 'share')
                values.append(f"{fmt(x['mean_us'], 'us')} ({share})")
            if all(v == '—' for v in values):
                dropped.append(name)
                continue
            label = name
            if 'native' in routes:
                if name == 'client coordinator':
                    label = ('Library work (WAL append plus index update)' if routes == ['native']
                             else name + ' (native: WAL append and index update)')
                elif name == 'durable wait and publication':
                    label = ('Barrier file sync' if routes == ['native']
                             else name + ' (native: barrier file sync)')
            rows.append([label, ', '.join(sorted(kinds))] + values)
        for label, key in [('TOTAL (API mean µs)', 'mean_us'), ('API p50 µs', 'p50_us'), ('ID', 'case_ids')]:
            rows.append([label, ''] + [(row_id(chosen[r][key]) if key == 'case_ids' else fmt(chosen[r][key], 'us'))
                                        if r in chosen else '—' for r in routes])
        output.append('#### ' + title + '\n\n' + table(['Component', 'Kind'] + routes, rows))
        if dropped:
            output.append('Not computed for all routes: ' + '; '.join(dropped) + '.')
    return '\n\n'.join(output)


def spread(values):
    values = [float(v) for v in values if v not in ('', 'missing', 'not computed')]
    if not values:
        return 'not measured'
    return f'{statistics.median(values):.1f} ({min(values):.1f}–{max(values):.1f})'


def network_floor():
    groups = defaultdict(lambda: defaultdict(list))
    for r in load(RUN8 / 'net_floor.csv'):
        if r['status'] == 'completed' and r['round_trip_p50_us'] not in ('', 'missing', 'not computed'):
            groups[tuple(r[k] for k in ('tool', 'mode', 'interface'))][r['size_bytes']].append(r)
    sizes = ('8', '1024', '4096', '65536')
    out = []
    for key, bysize in sorted(groups.items()):
        ids = [r['case_id'] for group in bysize.values() for r in group]
        out.append([*key, *[spread([r['round_trip_p50_us'] for r in bysize.get(size, [])])
                             for size in sizes], row_id(ids)])
    return ('RTT p50 µs: median (min–max) across repetitions. fi_pingpong reports averages, not p50, and is omitted.\n\n'
            + table(['Tool', 'Mode', 'Interface', '8 B', '1 KiB', '4 KiB', '64 KiB', 'ID'], out))


def network_repetitions():
    rows = []
    for r in load(RUN8 / 'net_floor.csv'):
        if r['status'] == 'completed':
            rows.append([r['tool'], r['mode'], r['interface'], size_label(r['size_bytes']),
                         r['repeat'], r['latency_semantics'], fmt(r['p50_us'], 'us'),
                         fmt(r['round_trip_p50_us'], 'us'), row_id(r['case_id'])])
    return table(['Tool', 'Mode', 'Interface', 'Size', 'Repetition', 'p50 semantics',
                  'p50 µs', 'RTT p50 µs', 'Row ID'], rows)


def ssd_floor(write=False):
    groups = defaultdict(list)
    if write:
        for r in load(RUN8 / 'ssd_write_floor.csv'):
            groups[tuple(r[k] for k in ('mode', 'block_bytes', 'region_bytes', 'status'))].append(r)
        rows = []
        for (mode, size, region, status), group in sorted(groups.items(), key=lambda item: (int(item[0][1]), item[0][0], item[0][2], item[0][3])):
            stats = []
            for prefix in ('write_', 'sync_', ''):
                if status != 'completed':
                    stats.append('not measured')
                elif prefix == 'sync_' and mode != 'fdatasync':
                    stats.append('none')
                else:
                    stats.append(' / '.join(spread([r[prefix + metric + '_us'] for r in group])
                                            for metric in ('mean', 'p50', 'p99')))
            rows.append([mode, size_label(size), size_label(region), status, len(group), *stats,
                         row_id([r['case_id'] for r in group])])
        return ('Each step shows mean / p50 / p99 µs, each median (min–max) across repetitions. '
                'Direct 1 KiB writes failed; they are not timings.\n\n' +
                table(['Mode', 'Block', 'Region', 'Status', 'Reps', 'Write', 'Sync', 'Total', 'ID'], rows))
    for r in load(RUN8 / 'ssd_floor.csv'):
        if r['status'] == 'completed' and r['block_bytes'] in ('4096', '65536'):
            groups[(r['region_bytes'], r['state'], r['block_bytes'])].append(r)
    keys = sorted({(region, state) for region, state, _ in groups})
    rows = []
    for region, state in keys:
        cells, ids = [], []
        for size in ('4096', '65536'):
            group = groups.get((region, state, size), [])
            cells.append(' / '.join(spread([r[metric + '_us'] for r in group])
                                    for metric in ('mean', 'p50', 'p99')) if group else 'not measured')
            ids.extend(r['case_id'] for r in group)
        rows.append([size_label(region), state, *cells, row_id(ids)])
    return ('Each cell is mean / p50 / p99 µs, each median (min–max) across repetitions.\n\n'
            + table(['Region', 'State', '4 KiB', '64 KiB', 'ID'], rows))


def run7_spread(filename, metrics):
    groups = {}
    for r in load(RUN7 / filename):
        if r['metric'] not in metrics:
            continue
        key = tuple(r[k] for k in ('route', 'size_bytes', 'concurrency', 'durability',
                                    'cache_state', 'tier', 'case_ids'))
        groups.setdefault(key, {})[r['metric']] = r
    rows = []
    for key, group in sorted(groups.items()):
        route, size, concurrency, durability, state, tier, cases = key
        vals = []
        for metric in sorted(metrics):
            r = group.get(metric)
            if not r:
                vals.append('not measured')
                continue
            kind = 'us' if metric.endswith('_us') else 'ratio' if metric == 'read_ops_per_success' else 'int'
            vals.append(f"{fmt(r['median'], kind)} ({fmt(r['min'], kind)}–{fmt(r['max'], kind)})")
        rows.append([row_id(cases), route, size_label(size), concurrency, durability, state, tier, *vals])
    return table(['ID', 'Route', 'Size', 'c', 'Durability', 'Cache', 'Tier'] +
                 [m + ' median (min–max)' for m in sorted(metrics)], rows)


def sustained():
    rows = []
    for r in load(RUN7 / 'sustained_summary.csv'):
        first = json.loads(r['first_second'])
        last = json.loads(r['last_five_seconds'])
        last_rates = ', '.join(fmt(second['puts_per_second'], 'int') for second in last)
        rows.append([row_id(r['case_id']), r['route'], r['durability'], r['repeat'], r['success_count'],
                     fmt(r['api_p999_us'], 'us'), fmt(first.get('puts_per_second'), 'int'), last_rates])
    return table(['ID', 'Route', 'Durability', 'Repeat', 'n', 'p99.9 µs', 'First second puts/s',
                  'Last five complete one-second buckets puts/s'], rows)


def sustained_summary():
    groups = defaultdict(list)
    for r in load(RUN7 / 'sustained_summary.csv'):
        groups[(r['route'], r['durability'])].append(r)
    rows = []
    for (route, tier), group in sorted(groups.items()):
        first = [json.loads(r['first_second'])['puts_per_second'] for r in group]
        late = [statistics.mean(s['puts_per_second'] for s in json.loads(r['last_five_seconds'])) for r in group]
        rows.append([row_id([r['case_id'] for r in group]), route, tier, len(group),
                     spread([r['api_p999_us'] for r in group]),
                     spread(first), spread(late)])
    return table(['ID', 'Route', 'Tier', 'Reps', 'p99.9 µs median (min–max)',
                  'First second puts/s median (min–max)', 'Last five complete one-second buckets mean puts/s median (min–max)'], rows)


def cross_store():
    sources = {
        'cassandra': (REPO / 'benchmarks/kv-research/src/backends/cassandra.rs',
                      r'"(CQL write acknowledged at \{selected_consistency:\?\};[^"]+)"'),
        'foundationdb': (REPO / 'benchmarks/kv-research/src/backends/foundationdb.rs',
                         r'"(successful FDB transaction commit[^"]+)"'),
        'garnet': (REPO / 'benchmarks/kv-research/src/backends/industry.rs',
                   r'"(SET/DEL reply after AOF commit;[^"]+)"'),
    }
    full_contract = {}
    for label, (path, pattern) in sources.items():
        inputs.add(path)
        match = re.search(pattern, path.read_text(encoding='utf-8'))
        if not match:
            raise ValueError(f'full contract missing from {path}')
        full_contract[label] = match.group(1).replace('{selected_consistency:?}', 'Quorum')
    specs = {('point-mixed', 'read'), ('point-mixed', 'put'), ('read-concurrent', 'read'),
             ('write-concurrent', 'put'), ('read-many32', 'get_many')}
    rows = [r for r in load(CROSS / 'populations.csv') if r['status'] == 'completed'
            and r['outcome'] == 'success' and r['cohort'] == 'primary_rf1'
            and r['value_bytes'] == '1024' and (r['profile'], r['operation']) in specs
            and r['durability_tier'] != 'native'
            and (r['label'] != 'native' or r['native_durability'] in ('', 'value_wal_barrier'))]
    groups = defaultdict(dict)
    for r in rows:
        key = (r['label'], r['acknowledgement'])
        population = (r['profile'], r['operation'])
        if population in groups[key]:
            raise ValueError(f'ambiguous cross-store population {key} {population}')
        groups[key][population] = r
    populations = [('point-mixed', 'read'), ('point-mixed', 'put'),
                   ('read-concurrent', 'read'), ('write-concurrent', 'put'),
                   ('read-many32', 'get_many')]
    out = []
    for key, group in sorted(groups.items()):
        values = []
        for population in populations:
            r = group.get(population)
            values.append('not measured' if not r else
                          f"{row_id(r['case_id'])}: {fmt(r['api_p50_us'], 'us')} / {fmt(r['api_p99_us'], 'us')}; runs {r['median_runs'] or '1'}")
        label, contract = key
        if label in full_contract:
            replacement = full_contract[label]
            if not replacement.startswith(contract):
                raise ValueError(f'contract does not match source for {label}')
            contract = replacement
        out.append([label, '<br>'.join(textwrap.wrap(contract, width=72)), *values])
    return table(['Store', 'Contract', 'point-mixed read c1', 'point-mixed put c1',
                  'read-concurrent c16', 'write-concurrent c16', 'read-many32 c16'], out)
def fair_plan():
    inputs.add(CAMPAIGN)
    text = CAMPAIGN.read_text(encoding='utf-8')
    return text.split('## Fair native comparison and 64 KiB write profiles (planned, not run)\n\n', 1)[1].split('\nRun the standalone analysis on a finished result tree:', 1)[0].strip()

def fair_counts():
    match = re.search(r'`fair_native` selects these groups \((\d+) cases\).*?append floors \((\d+) cases\)', fair_plan(), re.S)
    if not match:
        raise ValueError('Missing campaign selection counts')
    return f'fair_native {match[1]}, parity {match[2]}'



def spike_summary():
    old = [r for r in load(HISTORICAL) if r['case_id'].startswith('0183-native-65536-')
           and r['operation'] == 'put' and r['outcome'] == 'success' and r['timing'] == 'api']
    focused = load(RUN8 / 'ops_summary.csv')
    barrier = next(r for r in focused if 'G12K-r0-fc2fd2a25da56cb25569' in r['case_ids'])
    page = next(r for r in focused if 'G12N-r1-71d612a2e157e9bae6ba' in r['case_ids'])
    matrix = [r for r in load(CROSS / 'populations.csv') if r['case_id'].startswith('0490-native-65536-')
              and r['operation'] == 'put' and r['outcome'] == 'success']
    if len(old) != 1 or len(matrix) != 1:
        raise ValueError('Missing historical spike source')
    barrier_max = max(float(r['p50_us']) for r in json.loads(barrier['repetitions']))
    page_min = min(float(r['p50_us']) for r in json.loads(page['repetitions']))
    return (f"Across three fleets, Native per-put file-and-directory barrier c16 64 KiB put p50 was "
            f"{float(old[0]['p50']) / 1000:.1f} to {barrier_max:.1f} µs; page-cache acknowledgement "
            f"(not per-put durable) was {page_min:.1f} to {float(matrix[0]['api_p50_us']):.1f} µs in two fleets. "
            "The main body of the sampled distribution shifts, not only a few tail calls; the page-cache jump is unexplained.")


def sui_summary():
    inputs.add(SUI_TRACE)
    inputs.add(SUI_REPORT)
    ends, groups, starts = {}, {}, {}
    with SUI_TRACE.open(newline='') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            if r['event'] == '1':
                starts[r['call']] = r['op']
            elif r['event'] == '2':
                ends[r['call']] = r['outcome']
            elif r['event'] == '5':
                groups[r['group']] = (r['outcome'], r['call'])
    total = large = 0
    with SUI_TRACE.open(newline='') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            op = r['op']
            if r['event'] in ('3', '8'):
                if op in ('5', '6') and ends.get(r['call']) != '1':
                    continue
            elif r['event'] == '4':
                group = groups.get(r['group'])
                if not group or group[0] != '1' or starts.get(group[1]) in ('5', '6'):
                    continue
            else:
                continue
            if op == '5' or (op not in ('2', '4', '6') and r['outcome'] == '1'):
                total += 1
                large += int(r['value_len']) >= 65536
    match = re.search(r'\|remote\|checkpoints\|full_checkpoint_content_v2\|[^\n]*\|(\d+); (\d+)/(\d+)/(\d+)\|', SUI_REPORT.read_text())
    if not match:
        raise ValueError('Missing full-checkpoint value distribution')
    p50, p95 = int(match.group(2)), int(match.group(3))
    return (f"In one private checkpoint replay, {large} of {total:,} value-bearing successful puts and found reads "
            f"were at least 64 KiB ({100 * large / total:.4f}%); this is not a mainnet rate. "
            f"Many nonempty table medians are about 0.1 to 10 KiB, though smaller values occur; full-checkpoint contents had p50 {p50:,} and p95 {p95:,} bytes, "
            "approximately 8 to 12 KiB.")


def engine_guard():
    data = load(HERE / 'engine-guard-initial.csv')
    groups = defaultdict(list)
    for row in data:
        groups[(row['mode'], row['bytes'], row['arm'])].append(row)
    rows = []
    for size in ('1024', '64'):
        for mode in ('write', 'get', 'mixed'):
            cells = []
            for arm in ('baseline', 'baseline-forklock', 'fork'):
                samples = groups[(mode, size, arm)]
                if len(samples) != 2 or {r['repeat'] for r in samples} != {'1', '2'}:
                    raise ValueError(f'Incomplete engine guard: {mode}, {size}, {arm}')
                values = [int(r['target_ops_s']) for r in samples]
                if any(float(r['firefox_max_percent']) <= 30 or r['status'] != 'invalid: competing process' for r in samples):
                    raise ValueError(f'Unexpected engine guard status: {mode}, {size}, {arm}')
                cells.append(f'{sum(values) // 2:,} ({min(values):,}–{max(values):,})')
            rows.append([mode, size, *cells, 'undecided: invalid'])
    return table(['Mode', 'Bytes', 'Upstream own lock', 'Upstream fork lock', 'Fork', 'Verdict'], rows)


def per_node():
    source_rows = load(CROSS / 'populations.csv')
    specs = [('native', 1, 'per-put file and directory barrier'),
             ('th_verbs', 5, 'synchronous covering sync'),
             ('valkey_rdma', 5, 'AOF always')]
    rows = []
    for label, nodes, contract in specs:
        matches = [r for r in source_rows if r['label'] == label and r['cohort'] == 'primary_rf1'
                   and r['profile'] == 'write-concurrent' and r['value_bytes'] == '65536'
                   and r['operation'] == 'put' and r['outcome'] == 'success']
        if len(matches) != 1:
            raise ValueError(f'ambiguous per-node population {label}: {len(matches)}')
        r = matches[0]
        rate = float(r['case_success_calls_per_second'])
        rows.append([label, contract, nodes, f'{16 / nodes:.1f}', f'{rate:.1f}',
                     f'{rate / nodes:.1f}', f'{rate / nodes / 16:.1f}',
                     fmt(r['api_p50_us'], 'us'), fmt(r['api_mean_us'], 'us')])
    return table(['Store', 'Acknowledgement', 'Storage nodes', '16/nodes (assumed)', 'Puts/s',
                  'aggregate puts/s / node count', 'Payload MiB/s/node', 'p50 µs', 'Mean µs'], rows)




def headline():
    routes = [('native', 'native', 'native'),
              ('owner verbs', 'owner verbs', 'owner verbs'),
              ('owner TCP', 'owner TCP', 'owner TCP'),
              ('verbs mTLS', 'verbs mTLS', 'verbs mTLS'),
              ('hybrid kernel', 'hybrid kernel DIRECT', 'hybrid kernel buffered'),
              ('hybrid SPDK', 'hybrid SPDK', 'hybrid SPDK')]
    source_rows = [r for r in load(RUN8 / 'ops_summary.csv') if r['size_bytes'] == '1024'
                   and r['concurrency'] == '1' and r['key_policy'] == 'overwrite']
    rows = []
    for label, write_route, memory_route in routes:
        cells = []
        for kind in ('durable', 'page', 'memory', 'cold'):
            matches = []
            for r in source_rows:
                if kind in ('durable', 'page'):
                    target = ('value_wal_barrier' if write_route == 'native' else 'synchronous') if kind == 'durable' else ('page_cache' if write_route == 'native' else 'native')
                    ok = r['route'] == write_route and r['shape'] == 'put' and r['durability'] == target and r['diagnostics'] == '{}'
                else:
                    route = memory_route if kind == 'memory' else write_route
                    ok = r['route'] == route and r['shape'] == 'read' and r['cache_state'] == kind
                if ok:
                    matches.append(r)
            if len(matches) > 1:
                raise ValueError(f'ambiguous headline: {label} {kind}')
            if not matches:
                cells.append('not measured')
            else:
                r = matches[0]
                cells.append(f"{fmt(r['mean_us'], 'us')} / {fmt(r['p50_us'], 'us')}")
        rows.append([label] + cells)
    return ('Run 8, fleet bench-0a8a6af114a64287, 1 KiB c1. Cells show mean / p50 µs. '
            'Durable puts use owner synchronous or native barrier; page-cache puts use owner native tier or native page_cache. '
            'Reads use the stated cache state; distributed read copies are profiled. Repetitions and full contracts appear in the data tables.\n\n'
            + table(['Route', 'Durable put', 'Page-cache put', 'Cache read', 'Cold read'], rows))


def regression():
    groups = defaultdict(dict)
    combos = {('1024', '1'), ('1024', '16'), ('65536', '1')}
    for r in load(RUN8 / 'ops_summary.csv'):
        if (r['key_policy'] != 'overwrite' or (r['size_bytes'], r['concurrency']) not in combos
                or r['shape'] not in ('read', 'put')):
            continue
        if r['shape'] == 'put':
            if r['diagnostics'] != '{}':
                continue
            if r['durability'] in ('synchronous', 'value_wal_barrier'):
                column = 'durable put'
            elif r['durability'] in ('native', 'page_cache'):
                column = 'page-cache put'
            else:
                continue
        else:
            if r['cache_state'] not in ('memory', 'cold') or (r['diagnostics'] != '{}' and 'read_profile' not in r['diagnostics']):
                continue
            column = r['cache_state'] + ' read'
        key = (r['route'], r['size_bytes'], r['concurrency'])
        previous = groups[key].get(column)
        if previous is None or (previous['diagnostics'] != '{}' and r['diagnostics'] == '{}'):
            groups[key][column] = r
    syncs = {}
    for r in load(RUN8 / 'sync_step.csv'):
        if ((r['size_bytes'], r['concurrency']) in combos
                and (r['step'] == 'file + directory fsync'
                     or (r['route'] != 'native' and r['step'] == 'file fdatasync'
                         and r['route'] != 'owner0 overwrite floor'))):
            syncs[(r['route'], r['size_bytes'], r['concurrency'])] = r
    def operation(r):
        if not r:
            return 'not measured'
        return (f"{r['repetition_count']} rep; "
                + ' / '.join(fmt(r[k], 'us') for k in ('mean_us', 'p50_us', 'p99_us')))
    rows = []
    for (route, size, concurrency), columns in sorted(groups.items()):
        sync = syncs.get((route, size, concurrency))
        sync_cell = ('not measured' if not sync else
                     f"1 rep; {sync['step']}; " +
                     ' / '.join(fmt(sync[k], 'us') for k in ('mean_us', 'p50_us', 'p99_us')))
        order = ('durable put', 'page-cache put', 'memory read', 'cold read')
        ids = [row_id(columns[k]['case_ids']) if k in columns else '—' for k in order]
        ids.append(row_id(sync['case_id']) if sync else '—')
        rows.append([route, size_label(size), concurrency,
                     *[operation(columns.get(k)) for k in order],
                     sync_cell, ' / '.join(ids)])
    for size in ('4096', '65536'):
        group = [r for r in load(RUN8 / 'ssd_floor.csv') if r['status'] == 'completed'
                 and r['block_bytes'] == size and r['region_bytes'] == '2147483648' and r['state'] == 'fresh']
        if group:
            text = f"{len(group)} rep; " + ' / '.join(
                fmt(statistics.median(float(r[k]) for r in group), 'us') for k in ('mean_us', 'p50_us', 'p99_us'))
            rows.append(['SSD direct QD1, fresh 2 GiB', size_label(size), '1', 'not measured',
                         'not measured', 'not measured', text, 'not measured',
                         '— / — / — / ' + row_id([r['case_id'] for r in group]) + ' / —'])
    for tool, modes, interface in [('ib_send_lat', ('polled', 'event'), 'rdma0'),
                                   ('tcp_rtt', ('block', 'busy_poll'), 'eth0')]:
        for mode in modes:
            group = [r for r in load(RUN8 / 'net_floor.csv') if r['status'] == 'completed'
                     and r['tool'] == tool and r['mode'] == mode and r['interface'] == interface
                     and r['size_bytes'] == '1024' and r['round_trip_p50_us'] not in ('', 'missing')]
            if group:
                text = (f"{len(group)} rep; RTT p50 "
                        f"{fmt(statistics.median(float(r['round_trip_p50_us']) for r in group), 'us')} µs")
                rows.append([f'{tool} {mode} {interface}', '1 KiB', '1', 'not measured', 'not measured',
                             text, 'not measured', 'not measured',
                             '— / — / ' + row_id([r['case_id'] for r in group]) + ' / — / —'])
    for mode in ('buffered', 'fdatasync', 'direct'):
        for size in ('1024', '65536'):
            group = [r for r in load(RUN8 / 'ssd_write_floor.csv') if r['mode'] == mode
                     and r['block_bytes'] == size and r['status'] == 'completed']
            if group:
                text = f"{len(group)} rep; " + ' / '.join(
                    fmt(statistics.median(float(r[k]) for r in group), 'us') for k in ('mean_us', 'p50_us', 'p99_us'))
                durable = text if mode == 'fdatasync' else 'not measured'
                page = text if mode == 'buffered' else 'not measured'
                direct = 'direct write, no sync; ' + text if mode == 'direct' else 'not measured'
                ident = row_id([r['case_id'] for r in group])
                ids = ([ident, '—', '—', '—', '—'] if mode == 'fdatasync' else
                       ['—', ident, '—', '—', '—'] if mode == 'buffered' else
                       ['—', '—', '—', '—', ident])
                rows.append([f'SSD write {mode}', size_label(size), '1', durable, page,
                             'not measured', 'not measured', direct, ' / '.join(ids)])
    return ('Run 8; fleet bench-0a8a6af114a64287. Each populated cell gives repetitions '
            'and mean / p50 / p99 in µs. Row IDs follow the five value columns in order; — means no population. '
            'Durable puts use owner synchronous or native barrier; '
            'page-cache puts use owner native tier or native page_cache. Read state is in the column; '
            'profiled reads include profiling cost. Floor and sync cells state their own contract.\n\n' +
            table(['Route', 'Size', 'c', 'Durable put', 'Page-cache put', 'Memory read', 'Cold read', 'Sync step', 'Row IDs'], rows))


def ranked_basis():
    comps = load(RUN8 / 'component_summary.csv')
    sync = load(RUN8 / 'sync_step.csv')
    def sync_mean(route, step):
        matches = [r for r in sync if r['route'] == route and r['step'] == step
                   and r['size_bytes'] == '1024' and r['concurrency'] == '1']
        if len(matches) != 1:
            raise ValueError(f'ambiguous sync step: {route}, {step}')
        return fmt(matches[0]['mean_us'], 'us')
    def component(index):
        r = comps[index]
        return f"{fmt(r['mean_us'], 'us')} µs ({fmt(r['share_of_api_mean'], 'share')})"
    stall_data = json.loads(load(RUN8 / 'stall.csv')[0]['overwrite'])
    batch = [r for r in load(RUN8 / 'ops_summary.csv')
             if r['shape'] == 'put_batch' and r['size_bytes'] == '8192'
             and r['concurrency'] == '1' and r['diagnostics'] == '{}'
             and r['route'] in ('native', 'owner verbs') and r['key_policy'] == 'overwrite'
             and r['durability'] in ('synchronous', 'value_wal_barrier')]
    batch_basis = '; '.join(f"{r['route']} p50 {fmt(r['p50_us'], 'us')} µs" for r in batch) or 'not measured'
    rows = [
        ['0', 'Hardware parity', f"native sync {sync_mean('native', 'file + directory fsync')} µs; verbs {sync_mean('owner verbs', 'file fdatasync')} µs",
         'Sync steps; SSD count and group-commit contracts differ', f'Defined: equal writers c1/c5/c16, adapter group barrier, five Native instances on one SSD, one owner and 64 KiB profiles; {fair_counts()}', 'Explicit launch authorization'],
        ['1', 'Completion and network', f"read network {component(209)}; put {component(1)}",
         'Network component', 'Busy-poll A/B', 'Public configuration'],
        ['2', 'Owner persist and hand-offs', f"persist {component(0)}",
         'Software and hand-off intervals', 'Same-call stage timers', 'Durability contract'],
        ['3', 'Size-dependent software', f"64 KiB coordinator {component(266)}; owner {component(265)}",
         'Coordinator and owner components', 'Time digest and owner stages', 'Wire or persisted format'],
        ['4', 'c16 pipeline waits', f"waiting and coalescing {component(222)}",
         'Measured wait', 'Trace same-call pipeline stages', 'Durability contract'],
        ['5', 'Put stall', f"timeline overwrite mean spacing {fmt(stall_data['mean_spacing_puts'], 'int')} puts; fit intercept {fmt(stall_data['duration_fit']['intercept_us'], 'us')} µs",
         'Observed stall', 'Correlate checkpoint phases', 'Persisted format'],
        ['6', 'Batch writes', batch_basis,
         'Software and hand-offs', 'Time batch stages', 'Wire or persisted format'],
        ['7', 'Hybrid read residual', f"resident residual {component(67)}",
         'Derived residual', 'Time dispatch and wake-up', 'Public queue geometry'],
    ]
    return table(['Rank', 'Item', 'Measured basis', 'Accounting reference', 'Next step', 'Ask before'], rows)


def tier_ratios():
    groups = defaultdict(list)
    for r in load(RUN7 / 'put_durability_ratio.csv'):
        groups[(r['route'], r['size_bytes'], r['concurrency'])].append(r)
    rows = []
    for (route, size, concurrency), group in sorted(groups.items()):
        ratios = [float(r['p50_ratio']) for r in group if r['p50_ratio'] not in ('', 'missing')]
        ids = sum((json.loads(r['case_ids']) for r in group), [])
        vals = ('not measured' if not ratios else
                f'{statistics.median(ratios):.2f} ({min(ratios):.2f}–{max(ratios):.2f})')
        rows.append([row_id(ids), route, size_label(size), concurrency, len(group), vals])
    return ('Tier p50 ratios are context across separate populations, not same-call fsync cost.\n\n' +
            table(['ID', 'Route', 'Size', 'c', 'Reps', 'Synchronous / page-cache p50 ratio median (min–max)'], rows))


def owner_stages():
    names = ['handler_ns', 'control_queue_ns', 'appender_queue_ns', 'flusher_queue_ns',
             'append_wal_ns', 'append_validation_ns', 'append_descriptor_preparation_ns',
             'append_persist_ns', 'wal_file_persist_ns']
    cases = [('owner verbs', '1024'), ('owner TCP', '1024'), ('owner verbs', '65536')]
    rows = []
    for route, size in cases:
        matches = [r for r in load(RUN8 / 'ops_summary.csv')
                   if r['route'] == route and r['size_bytes'] == size
                   and r['shape'] == 'put' and r['concurrency'] == '1'
                   and r['durability'] == 'synchronous' and r['group'] == 'G12D'
                   and r['key_policy'] == 'overwrite' and 'write_timeline_limit' in r['diagnostics']]
        if len(matches) != 1:
            raise ValueError(f'ambiguous owner timeline: {route} {size}')
        case = json.loads(matches[0]['case_ids'])[0]
        path = RUN8.parent / 'report-root' / 'cases' / case / 'result' / 'write-timeline.json'
        inputs.add(path)
        records = json.loads(path.read_text())['timeline']['owner_records']
        if not records:
            raise ValueError(f'empty owner timeline: {case}')
        derived = {
            'unassigned owner time (append decode, group build, other handler work)': lambda r: r['handler_ns'] - r['flusher_queue_ns'] - r['append_wal_ns'] - r['append_persist_ns'],
            'persist minus fdatasync': lambda r: r['append_persist_ns'] - r['wal_file_persist_ns'],
        }
        for name in names + list(derived):
            values = sorted((r[name] if name in names else derived[name](r)) / 1000 for r in records)
            rows.append([row_id(case), route, size_label(size), len(values), name,
                         fmt(statistics.mean(values), 'us'), fmt(values[(len(values)-1)//2], 'us')])
    return table(['ID', 'Route', 'Size', 'n', 'Stage', 'Mean µs', 'p50 µs'], rows)


def write_profile():
    case = 'G12D-r0-0ce5f8eaa8fde37f6566'
    path = RUN8.parent / 'report-root' / 'cases' / case / 'result' / 'write-diagnostics.json'
    inputs.add(path)
    stages = json.loads(path.read_text(encoding='utf-8'))['profile']['stages']
    entries = [
        ('Connection handler', 'connection_handler'),
        ('Control queue', 'connection_control_queue'),
        ('Request decode', 'connection_request_decode'),
        ('Append decode', 'connection_append_decode'),
        ('Group build', 'connection_append_group_build'),
        ('Connection WAL append', 'connection_append_wal'),
        ('Storage append', 'storage_append_total'),
        ('Connection persist', 'connection_append_persist'),
        ('Storage persist', 'storage_persist_total'),
        ('WAL file sync', 'storage_persist_wal_file'),
        ('Append publication', 'storage_append_publication'),
        ('Persist publication', 'storage_persist_publication'),
        ('Persist serialization lock wait', 'storage_persist_serialization_lock_wait'),
        ('Append state lock wait', 'storage_append_state_lock_wait'),
        ('Persist state lock wait', 'storage_persist_state_lock_wait'),
        ('Append validation', 'storage_append_validation'),
        ('Persist validation', 'storage_persist_validation'),
        ('Client request encode', 'client_request_encode'),
        ('Client reply decode', 'client_response_decode'),
    ]
    rows = []
    for label, duration in entries:
        count = 'connection_control_queue_samples' if duration == 'connection_control_queue' else 'sampled_requests'
        n = stages['write_profile_' + count]
        mean = stages['write_profile_' + duration + '_ns'] / n / 1000 if n else None
        rows.append([label, n, fmt(mean, 'us')])
    return (f"Run 8; fleet bench-0a8a6af114a64287; owner verbs synchronous 1 KiB c1; "
            f"one separate profiled repetition, {row_id(case)}. Nested stages and populations overlap; do not add them.\n\n" +
            table(['Stage', 'n', 'Mean µs'], rows))


def build_tables():
    return {
        'durable': lambda: operations('durable'), 'page_cache': lambda: operations('page_cache'),
        'batch_delete': lambda: operations('batch_delete'), 'reads': lambda: operations('reads'),
        'reference': regression, 'headline': headline, 'headline_throughput': sustained_summary, 'ranked_basis': ranked_basis,
        'operations_appendix': lambda: operation_table(load(RUN8 / 'ops_summary.csv')),
        'case_ids': lambda: table(['ID', 'Case IDs'],
                                  ([key, ', '.join(ids)] for key, ids in sorted(case_index.items()))),
        'decompositions': decomposition, 'network_floor': network_floor,
        'ssd_read_floor': lambda: ssd_floor(False),
        'ssd_write_floor': lambda: ssd_floor(True),
        'sync_step': lambda: fields(load(RUN8 / 'sync_step.csv'), ['case_id', 'route', 'size_bytes', 'concurrency', 'durability', 'step', 'n', 'calls', 'mean_us', 'p50_us', 'p99_us']),
        'owner_stages': owner_stages,
        'write_profile': write_profile,
        'stall': stall,
        'run7_reads': lambda: run7_spread('device_read_spread.csv', {'api_p50_us', 'api_p99_us', 'successful_calls_per_second', 'read_ops_per_success'}),
        'run7_puts': lambda: run7_spread('device_put_spread.csv', {'api_p50_us', 'api_p99_us', 'api_p999_us', 'successful_calls_per_second'}) + '\n\n' + tier_ratios(),
        'run7_sustained': sustained_summary, 'cross_store': cross_store, 'per_node': per_node, 'fair_plan': fair_plan, 'fair_counts': fair_counts, 'spike': spike_summary, 'sui': sui_summary, 'engine_guard': engine_guard,
        'failures': lambda: fields(load(RUN8 / 'failed_cases.csv'), ['case_id', 'case_kind', 'repeat', 'error']),
        'unqualified': lambda: fields(load(RUN8 / 'unqualified.csv'), ['case_id', 'route', 'size_bytes', 'concurrency', 'verdict', 'reason_codes']),
    }


def stall():
    rows = []
    for row in load(RUN8 / 'stall.csv'):
        for policy in ('overwrite', 'new'):
            data = json.loads(row[policy])
            fit = data['duration_fit']
            phases = data['phase_counts']
            rows.append([row_id(data['case_id']), row['mode'], policy, data['completion_count'],
                         data['count'], fmt(data['mean_spacing_puts'], 'int'), fmt(data['median_spacing_puts'], 'int'),
                         fmt(fit['intercept_us'], 'us'), fmt(fit['slope_us_per_put'], 'ratio'),
                         ', '.join(f'{k}: {v}' for k, v in phases.items()) if phases else 'not measured'])
    return table(['case_id', 'diagnostic', 'key policy', 'completions', 'stalls',
                  'mean spacing puts', 'median spacing puts', 'fit intercept µs',
                  'fit slope µs/put', 'longest interval phase counts'], rows)


def main():
    template = TEMPLATE.read_text(encoding='utf-8')
    builders = build_tables()
    rendered_tables = {}
    def replace(match):
        key = match.group(1).strip()
        if key.startswith('table:'):
            name = key[6:]
            if name not in builders:
                raise ValueError(f'unknown table placeholder: {key}')
            if name not in rendered_tables:
                rendered_tables[name] = builders[name]()
            return rendered_tables[name]
        if key.startswith('const:'):
            return CONSTANTS[key[6:]]
        if key.startswith('csv:'):
            parts = key.split(':')
            if len(parts) not in (4, 5):
                raise ValueError(f'invalid CSV placeholder: {key}')
            _, filename, row, column = parts[:4]
            if '/' in filename and not filename.startswith(('run7/', 'cross/')):
                raise ValueError(f'unknown CSV source: {filename}')
            value = load(source(filename))[int(row)][column]
            if len(parts) == 5:
                if parts[4] not in ('us', 'share', 'ratio', 'int'):
                    raise ValueError(f'unknown formatter: {parts[4]}')
                value = fmt(value, parts[4])
            return cell(value)
        raise ValueError(f'unknown placeholder: {key}')
    rendered = re.sub(r'\{\{(.*?)\}\}', replace, template, flags=re.DOTALL)
    rendered = re.sub(r'\n{3,}', '\n\n', rendered)
    rendered = re.sub(r'\n\n(?=#{1,6} )', '\n', rendered)
    rendered = re.sub(r'(?m)^(#{1,6} [^\n]+)\n\n', r'\1\n', rendered)
    rendered = re.sub(r'\n\n(?=\*\*(?:Do|Verify|Ask before):)', '<br>\n', rendered)
    rendered = rendered.replace('\n\nNot computed for all routes:', '\nNot computed for all routes:')
    if '{{' in rendered or '}}' in rendered:
        raise ValueError('unmatched placeholder delimiter')
    OUTPUT.write_text(rendered, encoding='utf-8')
    full_operations = builders['operations_appendix']()
    appendix = ('# Performance plan appendix\n\nRun 8, fleet `bench-0a8a6af114a64287`; '
                'the operation table lists contracts and repetitions in each row. '
                'Values are medians of per-repetition statistics, not pooled calls. Native uses one process and one SSD; Tidehunter uses five owners. Native barriers sync every registered Value-WAL file and the directory per put; owners acknowledge after a covering sync. The 64 KiB c16 Native barrier result is consistent with a one-barrier-per-put model [INFERENCE], but its cause is not isolated. Native cold reads use mapped recent fragments and positional reads for older fragments; the 8 KiB case passes the device-count gate but has memory-like latency. Window counters do not prove individual NAND access.\n\n'
                '## Full operation table\n\n' + full_operations +
                '\n\n## Run 7 sustained puts by repetition\n\n'
                'Fleet `bench-6e40ca83d74146cf`; route and durability are in each row.\n\n' + sustained() +
                '\n\n## Network floor repetitions\n\n'
                'Run 8, fleet `bench-0a8a6af114a64287`; `ib_send_lat` and `ib_write_lat` record half round trip and are doubled for RTT; `ib_read_lat` records round trip. Missing p50 is not zero.\n\n' + network_repetitions() +
                '\n\n## Case IDs by row ID\n\n' + builders['case_ids']() + '\n')
    APPENDIX.write_text(appendix, encoding='utf-8')
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(inputs)}
    hashes[str(OUTPUT)] = hashlib.sha256(OUTPUT.read_bytes()).hexdigest()
    hashes[str(APPENDIX)] = hashlib.sha256(APPENDIX.read_bytes()).hexdigest()
    (HERE / 'inputs.json').write_text(json.dumps(hashes, indent=2, sort_keys=True) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
