"""Shared loaders and the number language of the 2026-11-10 brief (carried-forward number language).

main.tex quotes every measured number as \\N{key}. make_tables.py scans the
TeX sources, evaluates each key against populations.csv, device-reads.csv and
previous-populations.csv, and writes numbers.tex. An unknown or ambiguous key
stops the build, so prose cannot drift from the data.

Key grammar (fields separated by ':'; optional '|format' suffix, Python format spec):
  <metric>:<label>:<cohort>:<profile>:<op>:<size>        metric: p50 p95 p99 p999 mean max cps mibs resp50 resp99
  twin<metric>:<label>:<cohort>:<profile>:<op>:<size>    same, from the native-durability twin execution
  ratio:<metric>:<A>/<B>:<cohort>:<profile>:<op>:<size>  A divided by B
  dev:<label>:<cohort>:<profile>:<op>:<size>             block-device reads per call (owner hosts; coordinator for native)
  devb|devus|initb|initus|initops: same fields           device bytes per call; mean block-layer us per device read;
                                                         initiator bytes per call; initiator us per read; initiator reads per call
  tlcorr:<w1|w2|w3>  tlbucket:<w1|w2|w3>_competing<0|8>   authority-thread waits of the traced write case (r; mean us)
  S:<name>                                               named statistic from STATS
  curve:<profile>:<op>:<calls|case_calls|runs_min|runs_max>  uniform point counts or external run-count limits
"""
import csv
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
SIZES = (64, 256, 1024, 8192, 65536)
LABELS = {
    'native': 'Native', 'th_tcp': 'TH TCP', 'th_verbs': 'TH verbs', 'th_kernel': 'TH NVMe-oF kernel',
    'th_spdk': 'TH NVMe-oF SPDK', 'valkey_tcp': 'Valkey TCP', 'valkey_rdma': 'Valkey RDMA',
    'tikv': 'TiKV', 'cassandra': 'Cassandra', 'foundationdb': 'FoundationDB', 'kvrocks': 'Kvrocks',
    'garnet': 'Garnet',
}
ORDER = list(LABELS)
COLORS = dict(zip(ORDER, ['#111111', '#0072b2', '#56b4e9', '#d55e00', '#cc79a7', '#009e73', '#006442',
                          '#e69f00', '#882255', '#44aa99', '#aa4499', '#999933']))
MARKERS = dict(zip(ORDER, ['o', 's', 'D', '^', 'v', 's', 'D', 'P', 'X', 'h', 'p', '*']))
METRICS = {'p50': 'api_p50_us', 'p95': 'api_p95_us', 'p99': 'api_p99_us', 'p999': 'api_p999_us',
           'mean': 'api_mean_us', 'max': 'api_max_us', 'cps': 'case_success_calls_per_second',
           'mibs': 'case_success_value_mib_per_second', 'resp50': 'response_p50_us', 'resp99': 'response_p99_us'}
FLOAT_COLUMNS = ['observed_calls', 'planned_calls', 'successful_calls', 'unsuccessful_calls', 'elapsed_seconds',
                 'case_success_calls_per_second', 'case_success_value_mib_per_second', 'population_calls',
                 'api_mean_us', 'api_p50_us', 'api_p95_us', 'api_p99_us', 'api_p999_us', 'api_max_us',
                 'response_p50_us', 'response_p99_us', 'raw_api_p50_us']
DEVICE_COLUMNS = ['coordinator_reads', 'coordinator_bytes', 'coordinator_ms', 'initiator_reads',
                  'initiator_bytes', 'initiator_ms', 'storage_reads', 'storage_bytes', 'storage_ms',
                  'coordinator_writes', 'coordinator_write_bytes', 'coordinator_write_ms', 'coordinator_flushes',
                  'storage_writes', 'storage_write_bytes', 'storage_write_ms', 'storage_flushes',
                  'dropped_os_caches']


def read_csv(name):
    with open(HERE / name, newline='') as handle:
        return list(csv.DictReader(handle))


def convert(row):
    for column in FLOAT_COLUMNS:
        row[column] = float(row[column]) if row.get(column, '') != '' else None
    row['size'] = int(row['value_bytes']) if row['value_bytes'] else None
    row['conc'] = int(row['concurrency']) if row['concurrency'] else None
    return row


class Data:
    def __init__(self):
        everything = [convert(r) for r in read_csv('populations.csv')]
        self.all_rows = everything
        self.rows = [r for r in everything if r['status'] == 'completed' and r['operation']]
        self.previous = [convert(r) for r in read_csv('previous-populations.csv')
                         if r['status'] == 'completed' and r['operation']]
        self.device = {r['case_id']: {k: int(v) for k, v in r.items() if k in DEVICE_COLUMNS}
                       for r in read_csv('device-reads.csv')}
        self.timeline = {(r['kind'], r['name']): {k: (float(v) if k in ('n', 'mean_us', 'median_us', 'p99_us') else v)
                                                  for k, v in r.items()} for r in read_csv('write-timeline-spans.csv')}
        self.stage = {(r['case'], r['group'], r['field']): r for r in read_csv('stage-evidence.csv')}
        self.readmany = {r['case_id'][:4]: {k: (float(v) if k != 'case_id' else v) for k, v in r.items()}
                         for r in read_csv('readmany-trace.csv')}

    # ---- selection -------------------------------------------------------------------------
    def select(self, label=None, cohort=None, profile=None, op=None, size=None, outcome='success',
               twin=False, rows=None):
        out = []
        for r in (self.rows if rows is None else rows):
            if label is not None and r['label'] != label:
                continue
            if cohort is not None and r['cohort'] != cohort:
                continue
            if profile is not None and r['profile'] != profile:
                continue
            if op is not None and r['operation'] != op:
                continue
            if size is not None and r['size'] != int(size):
                continue
            if outcome is not None and r['outcome'] != outcome:
                continue
            if (r.get('durability_tier') == 'native') != twin:
                continue
            if r['label'] == 'native' and r['native_durability'] not in ('value_wal_barrier', ''):
                continue
            out.append(r)
        return out

    def pop(self, label, cohort, profile, op, size, outcome='success', twin=False, rows=None):
        found = self.select(label, cohort, profile, op, size, outcome, twin, rows)
        if len(found) != 1:
            raise KeyError(f'{len(found)} populations for {(label, cohort, profile, op, size, outcome, twin)}')
        return found[0]

    def maybe(self, *args, **kwargs):
        found = self.select(*args, **kwargs)
        return found[0] if len(found) == 1 else None

    # ---- device evidence -------------------------------------------------------------------
    def device_per_call(self, row):
        """Per-call device figures. Owner hosts, except native where the local SSD is the coordinator's."""
        d = self.device[row['case_id']]
        calls = row['successful_calls']
        prefix = 'coordinator' if row['label'] == 'native' else 'storage'
        reads, nbytes, ms = d[prefix + '_reads'], d[prefix + '_bytes'], d[prefix + '_ms']
        return {'dev': reads / calls, 'devb': nbytes / calls, 'devus': (ms * 1000.0 / reads) if reads else 0.0,
                'initops': d['initiator_reads'] / calls, 'initb': d['initiator_bytes'] / calls,
                'initus': (d['initiator_ms'] * 1000.0 / d['initiator_reads']) if d['initiator_reads'] else 0.0,
                # writes: owner hosts (the coordinator's SSD for native), all hosts, and device flush requests
                'wr': d[prefix + '_writes'] / calls, 'wrk': d[prefix + '_write_bytes'] / 1024.0 / calls,
                'wrtk': (d['coordinator_write_bytes'] + d['storage_write_bytes']) / 1024.0 / calls,
                'fl': (d['coordinator_flushes'] + d['storage_flushes']) / calls}

    # ---- matched pairs -----------------------------------------------------------------------
    def pairs(self, a, b, rows=None):
        """Populations of label a and label b with the same cohort, profile, size, concurrency, op, keys, rate."""
        def key(r):
            return (r['cohort'], r['profile'], r['size'], r['conc'], r['operation'], r['keys'],
                    r['offered_ops_per_second'])
        left = {key(r): r for r in self.select(a, rows=rows)}
        right = {key(r): r for r in self.select(b, rows=rows)}
        return [(k, left[k], right[k]) for k in left if k in right]


def auto_format(value):
    if value < 10:
        return f'{value:.2f}'
    if value < 100:
        return f'{value:.1f}'
    return f'{value:,.0f}'


# ---- named statistics ------------------------------------------------------------------------
def build_stats(data):
    stats = {}
    # planned and observed coverage
    status = [r for r in data.all_rows if r['operation'] == '' or True]
    cases = {}
    for r in data.all_rows:
        cases[r['case_id']] = r['status']
    stats['cases_planned'] = len(cases)
    stats['cases_completed'] = sum(1 for s in cases.values() if s == 'completed')
    stats['cases_notrun'] = sum(1 for s in cases.values() if s == 'not_run')
    stats['cases_failed'] = sum(1 for s in cases.values() if s == 'failed')
    stats['cases_flagged'] = len({r['case_id'] for r in data.all_rows if r['flag_spdk_accounting']})
    # replicate evidence: the sealed read cases ran twice, once per durability tier of the dataset build
    twins = []
    sync = {}
    for r in data.all_rows:
        if r['status'] == 'completed' and r['operation'] == 'read' and r['outcome'] == 'success' \
                and r['cohort'] == 'sealed_read':
            key = (r['backend'], r['nvmeof_target'], r['profile'], r['size'])
            if r['durability_tier'] == 'native':
                twins.append((key, r))
            else:
                sync[key] = r
    ratios50 = [t['api_p50_us'] / sync[k]['api_p50_us'] for k, t in twins if k in sync]
    ratios99 = [t['api_p99_us'] / sync[k]['api_p99_us'] for k, t in twins if k in sync]
    stats.update(twin_pairs=len(ratios50), twin_p50_lo=min(ratios50), twin_p50_hi=max(ratios50),
                 twin_p50_med=statistics.median(ratios50), twin_p99_lo=min(ratios99), twin_p99_hi=max(ratios99),
                 twin_p99_med=statistics.median(ratios99))
    # RDMA against TCP, matched populations
    def tally(a, b, name, mismatch=None):
        pairs = data.pairs(a, b)
        rows_ = []
        for k, left, right in pairs:
            ra, rb = left, right  # left = a (TCP side for the first argument)
            rows_.append((k, ra, rb))
        return rows_
    th = data.pairs('th_tcp', 'th_verbs')
    stats['th_pairs'] = len(th)
    slower50 = [(k, t, v) for k, t, v in th if v['api_p50_us'] > t['api_p50_us']]
    mism = lambda t, v: abs(data.device_per_call(v)['dev'] - data.device_per_call(t)['dev']) > 0.1
    stats['th_p50_slower'] = len(slower50)
    stats['th_p50_slower_mismatch'] = sum(1 for k, t, v in slower50 if mism(t, v))
    stats['th_p50_slower_tie'] = sum(1 for k, t, v in slower50 if not mism(t, v))
    stats['th_p50_slower_tie_max'] = max([v['api_p50_us'] / t['api_p50_us'] for k, t, v in slower50
                                          if not mism(t, v)] or [1.0])
    stats['th_mismatched'] = sum(1 for k, t, v in th if mism(t, v))
    matched = [(k, t, v) for k, t, v in th if not mism(t, v)]
    stats['th_matched'] = len(matched)
    stats['th_matched_p50_faster'] = sum(1 for k, t, v in matched if v['api_p50_us'] < t['api_p50_us'])
    stats['th_matched_p50_slower'] = sum(1 for k, t, v in matched if v['api_p50_us'] > t['api_p50_us'])
    stats['th_matched_p99_slower'] = sum(1 for k, t, v in matched if v['api_p99_us'] > t['api_p99_us'])
    stats['th_matched_p999_slower'] = sum(1 for k, t, v in matched if v['api_p999_us'] > t['api_p999_us'])
    stats['th_matched_cps_slower'] = sum(1 for k, t, v in matched if v['case_success_calls_per_second']
                                         < t['case_success_calls_per_second'])
    repeat = [(k, t, v) for k, t, v in th if k[0] == 'sealed_read' and k[1].startswith('repeat_read')
              and 'baseline' in k[1]]
    stats['th_repeat_pairs'] = len(repeat)
    # Media state of the matched read pairs: memory means at most 0.06 device reads per call on both sides, SSD
    # means at least 0.5 on both sides. Ratios are verbs over TCP.
    def device_dev(row):
        return data.device_per_call(row)['dev']
    reads = [(k, t, v) for k, t, v in matched if k[4] == 'read']
    memory = [(k, t, v) for k, t, v in reads if max(device_dev(t), device_dev(v)) <= 0.06]
    ssd = [(k, t, v) for k, t, v in reads if min(device_dev(t), device_dev(v)) >= 0.5]
    mem_ratios = [v['api_p50_us'] / t['api_p50_us'] for k, t, v in memory]
    stats['th_mem_pairs'] = len(memory)
    stats['th_mem_ratio_med'] = statistics.median(mem_ratios)
    stats['th_mem_lower'] = (1.0 - statistics.median(mem_ratios)) * 100.0
    stats['th_mem_ratio_lo'], stats['th_mem_ratio_hi'] = min(mem_ratios), max(mem_ratios)
    big = [v['api_p50_us'] / t['api_p50_us'] for k, t, v in memory if k[2] == 65536]
    stats['th_mem_64k_lo'], stats['th_mem_64k_hi'] = min(big), max(big)
    stats['th_ssd_pairs'] = len(ssd)
    stats['th_repeat_faster'] = sum(1 for k, t, v in repeat if v['api_p50_us'] < t['api_p50_us'])
    ratios = [v['api_p50_us'] / t['api_p50_us'] for k, t, v in repeat]
    stats['th_repeat_ratio_lo'] = min(ratios)
    stats['th_repeat_ratio_hi'] = max(ratios)
    vk = data.pairs('valkey_tcp', 'valkey_rdma')
    stats['vk_pairs'] = len(vk)
    stats['vk_p50_slower'] = sum(1 for k, t, v in vk if v['api_p50_us'] > t['api_p50_us'])
    stats['vk_p99_slower'] = sum(1 for k, t, v in vk if v['api_p99_us'] > t['api_p99_us'])
    stats['vk_p999_slower'] = sum(1 for k, t, v in vk if v['api_p999_us'] > t['api_p999_us'])
    # previous campaign
    def jk(r):
        return (r['backend'], r['nvmeof_target'], r['cohort'], r['profile'], r['value_bytes'], r['concurrency'],
                r['operation'], r['outcome'], r['keys'], r['offered_ops_per_second'])
    old = {jk(r): r for r in data.previous if r['outcome'] == 'success'}
    new = {jk(r): r for r in data.select()}
    common = [k for k in new if k in old]
    stats['xr_common'] = len(common)
    for label in ORDER:
        vals = sorted(new[k]['raw_api_p50_us'] / old[k]['api_p50_us'] for k in common if new[k]['label'] == label)
        if vals:
            stats[f'xr_n:{label}'] = len(vals)
            stats[f'xr_med:{label}'] = statistics.median(vals)
            stats[f'xr_p10:{label}'] = vals[len(vals) // 10]
            stats[f'xr_p90:{label}'] = vals[(9 * len(vals)) // 10]
    spdk = data.pop('th_spdk', 'sealed_read', 'first_touch-scalar-c1-baseline', 'read', 64)
    per_call = data.device_per_call(spdk)
    stats['spdk_fabric_us'] = per_call['initus'] - per_call['devus']
    speedups = []
    for label in ('th_tcp', 'th_verbs', 'th_kernel', 'th_spdk'):
        for size in (256, 8192):
            hit = data.maybe(label, 'coordinator_cache_configured', 'coordinator-cache', 'read', size)
            remote = data.maybe(label, 'primary_rf1', 'point-mixed', 'read', size)
            if hit and remote:
                speedups.append(remote['api_p50_us'] / hit['api_p50_us'])
    stats['hit_speedup_min'], stats['hit_speedup_max'], stats['hit_speedup_n'] = min(speedups), max(speedups), len(speedups)
    # External-store values are medians over runs; the spread table gives how far the runs differ.
    spread = {r['store']: r for r in read_csv('external-spread.csv')}
    stats['ext_runs_min'] = min(int(r['median_runs']) for r in data.rows if r['median_runs'])
    stats['ext_runs_max'] = max(int(r['median_runs']) for r in data.rows if r['median_runs'])
    stats['ext_pops'] = sum(1 for r in data.rows if r['median_runs'])
    stats['ext_spread_n'] = int(spread['all']['populations'])
    stats['ext_spread_med'] = float(spread['all']['spread_median'])
    stats['ext_spread_p90'] = float(spread['all']['spread_p90'])
    stats['ext_spread_max'] = float(spread['all']['spread_max'])
    stats.update(timeline_stats(data))
    return stats


def timeline_stats(data):
    """Groups of the contiguous write-call intervals (instrumented verbs write case, concurrency 16, 1 KiB)."""
    def interval(*names):
        return sum(data.timeline[('interval', n)]['mean_us'] for n in names)
    groups = {
        'tl_entry': interval('caller_start>gate_acquired', 'gate_acquired>validation_start',
                             'validation_start>admission_wait_started', 'admission_wait_started>admission_acquired',
                             'admission_acquired>owner_enqueue_started'),
        'tl_wait_owner': interval('owner_enqueue_started>owner_dequeued'),
        'tl_prepare': interval('owner_dequeued>prepared', 'prepared>admitted'),
        'tl_wait_append': interval('admitted>append_enqueue_started'),
        'tl_dispatch': interval('append_enqueue_started>worker_dequeued', 'worker_dequeued>tx_queued'),
        'tl_exchange': interval('tx_queued>completion_harvested'),
        'tl_wait_durable': interval('completion_harvested>durable_observed'),
        'tl_publish': interval('durable_observed>published'),
        'tl_reply': interval('published>caller_reply_queued', 'caller_reply_queued>caller_finished'),
    }
    span = sum(groups.values())
    groups['tl_span'] = span
    groups['tl_waits'] = groups['tl_wait_owner'] + groups['tl_wait_append'] + groups['tl_wait_durable']
    groups['tl_waits_pct'] = 100.0 * groups['tl_waits'] / span
    groups['tl_exchange_pct'] = 100.0 * groups['tl_exchange'] / span
    groups['tl_walpersist'] = data.timeline[('owner', 'wal_file_persist_ns')]['mean_us']
    groups['tl_walpersist_pct'] = 100.0 * groups['tl_walpersist'] / span
    for name in ('tl_entry', 'tl_wait_owner', 'tl_prepare', 'tl_wait_append', 'tl_dispatch', 'tl_exchange',
                 'tl_wait_durable', 'tl_publish', 'tl_reply'):
        groups[name + '_pct'] = 100.0 * groups[name] / span
    return groups


def evaluate(key, data, stats):
    """Evaluate one \\N key; return the formatted string."""
    spec = None
    if '|' in key:
        key, spec = key.split('|', 1)
    kind, *args = key.split(':')
    if kind == 'curve':
        profile, op, field = args
        rows = data.select(cohort='primary_rf1', profile=profile, op=op)
        if field in ('calls', 'case_calls'):
            column = 'population_calls' if field == 'calls' else 'planned_calls'
            values = {int(row[column]) for row in rows}
            assert len(values) == 1, (key, values)
            return format(values.pop(), spec or ',d')
        runs = [int(row['median_runs']) for row in rows if row['median_runs']]
        value = {'runs_min': min(runs), 'runs_max': max(runs)}[field]
        return format(value, spec or 'd')
    if kind == 'S':
        value = stats[':'.join(args)]
        if isinstance(value, int):
            return format(value, spec or 'd')
        return format(value, spec or '.3f')
    if kind == 'stage':
        case, group, field = args
        return format(float(data.stage[(case, group, field)]['mean_us']), spec or '.1f')
    if kind == 'stagecount':
        case, group, field = args
        return format(int(data.stage[(case, group, field)]['count']), spec or ',d')
    if kind == 'tl':
        return format(data.timeline[('interval', args[0])]['mean_us'], spec or '.1f')
    if kind == 'tlowner':
        return format(data.timeline[('owner', args[0])]['mean_us'], spec or '.1f')
    if kind == 'tlcorr':  # Pearson r of a wait against its competing-command count: tlcorr:w1|w2|w3
        return format(data.timeline[('corr', args[0])]['mean_us'], spec or '.2f')
    if kind == 'tlbucket':  # mean wait (us) at 0 or 8 competing commands: tlbucket:w2_competing8
        return format(data.timeline[('bucket', args[0])]['mean_us'], spec or '.0f')
    if kind == 'rm':
        case, column = args
        value = data.readmany[case][column]
        return format(value, spec or ('.2f' if column == 'groups_per_call' else '.0f'))
    if kind == 'rmpct':
        case, column = args
        row = data.readmany[case]
        return format(100.0 * row[column] / row['total_us'], spec or '.0f')
    if kind == 'rmcoord':
        row = data.readmany[args[0]]
        return format(100.0 * (row['total_us'] - row['flight_us']) / row['total_us'], spec or '.0f')
    if kind == 'more':  # percent by which A exceeds B: more:<metric>:<A>/<B>:<cohort>:<profile>:<op>:<size>
        metric, pair, cohort, profile, op, size = args
        a, b = pair.split('/')
        va = data.pop(a, cohort, profile, op, size)[METRICS[metric]]
        vb = data.pop(b, cohort, profile, op, size)[METRICS[metric]]
        return format((va / vb - 1.0) * 100.0, spec or '.0f')
    if kind in ('rej', 'succ'):  # offered-load case totals: rej:<label>:<rate>
        label, rate = args
        row = data.select(label, 'primary_rf1', f'offered-{rate}', outcome=None)[0]
        return format(int(row['unsuccessful_calls' if kind == 'rej' else 'successful_calls']), spec or ',d')
    if kind == 'hit':  # hit ratio (percent) for a mean latency goal, hit cost 0.8 us: hit:<goal>:<label>:<cohort>:<profile>:<op>:<size>
        goal, label, cohort, profile, op, size = args
        miss = data.pop(label, cohort, profile, op, size)['api_p50_us']
        return format(100.0 * (miss - float(goal)) / (miss - 0.8), spec or '.0f')
    if kind == 'old':  # same population in the previous campaign: old:<metric>:<label>:<cohort>:<profile>:<op>:<size>
        metric, label, cohort, profile, op, size = args
        row = data.pop(label, cohort, profile, op, size, rows=data.previous)
        value = row[METRICS[metric]]
        return format(value, spec) if spec else auto_format(value)
    if kind == 'xratio':  # A over B for different cohorts or profiles of two labels
        metric, la, ca, pa, lb, cb, pb, op, size = args
        va = data.pop(la, ca, pa, op, size)[METRICS[metric]]
        vb = data.pop(lb, cb, pb, op, size)[METRICS[metric]]
        return format(va / vb, spec or '.2f')
    twin = False
    if kind.startswith('twin'):
        kind, twin = kind[4:], True
    if kind in METRICS:
        label, cohort, profile, op, size = args
        row = data.pop(label, cohort, profile, op, size, twin=twin)
        value = row[METRICS[kind]]
        return format(value, spec) if spec else auto_format(value)
    if kind in ('p50ms', 'p99ms', 'p999ms', 'maxms', 'meanms'):
        label, cohort, profile, op, size = args
        value = data.pop(label, cohort, profile, op, size, twin=twin)[METRICS[kind[:-2]]] / 1000.0
        return format(value, spec or '.0f')
    if kind == 'gibs':
        label, cohort, profile, op, size = args
        value = data.pop(label, cohort, profile, op, size)['case_success_value_mib_per_second'] / 1024.0
        return format(value, spec or '.1f')
    if kind == 'el':
        label, cohort, profile, op, size = args
        return format(data.pop(label, cohort, profile, op, size)['elapsed_seconds'], spec or '.2f')
    if kind in ('ratio', 'lower'):
        metric, pair, cohort, profile, op, size = args
        a, b = pair.split('/')
        va = data.pop(a, cohort, profile, op, size)[METRICS[metric]]
        vb = data.pop(b, cohort, profile, op, size)[METRICS[metric]]
        if kind == 'lower':
            return format((1.0 - va / vb) * 100.0, spec or '.0f')
        return format(va / vb, spec or '.2f')
    if kind in ('dev', 'devb', 'devus', 'initb', 'initus', 'initops', 'amp', 'clientus', 'wr', 'wrk', 'wrtk', 'fl'):
        label, cohort, profile, op, size = args
        row = data.pop(label, cohort, profile, op, size)
        per_call = data.device_per_call(row)
        if kind == 'amp':
            return format(per_call['initb'] / row['size'], spec or '.1f')
        if kind == 'clientus':  # mean API time minus the initiator's mean NVMe command time
            return format(row['api_mean_us'] - per_call['initus'], spec or '.1f')
        default = {'dev': '.3f', 'initops': '.3f', 'wr': '.2f', 'fl': '.2f', 'wrk': '.1f', 'wrtk': '.1f'}.get(
            kind, ',.0f' if kind in ('devb', 'initb') else '.1f')
        return format(per_call[kind], spec or default)
    raise KeyError(f'unknown key kind {kind!r} in {key!r}')


# ---- Focused fleets: never combine these with Data.rows ---------------------------------------
# C:<series>:<component-token> and D:<series>:<field> select one named case/population.
# Q:<csv-stem>:<zero-based-row>:<field> selects a cell of a hashed copied/extracted table.
# R:<reads|puts>:<route-token>:<bytes>:<c>:<tier>:<metric>:<min|median|max>
# reports run 7 per-repetition summaries, never pooled calls.
import json

COMPONENTS = {
    'persist': 'owner persist (file sync, includes software)',
    'network': 'network (wire, both stacks, TLS, dispatch)',
    'coordinator': 'client coordinator', 'owner': 'owner software',
    'durable': 'durable wait and publication', 'coalescing': 'waiting and coalescing',
    'queue': 'client queueing and wake-up', 'residual': 'residual',
    'messaging': 'client messaging (encode, decode, verify)', 'device': 'ssd device',
    'combined': 'device + fabric + stack (submit_to_cq, not separable)',
    'exchange': 'exchange (owner and network not separable)',
}
SHORT = {'owner verbs': 'Verbs', 'owner TCP': 'TCP', 'verbs mTLS': 'Verbs mTLS',
         'hybrid kernel DIRECT': 'Kernel direct', 'hybrid kernel buffered': 'Kernel buffered',
         'hybrid SPDK': 'SPDK', 'native': 'Native'}
ROUTES7 = {'v': 'owner verbs', 't': 'owner TCP', 'k': 'hybrid kernel DIRECT', 's': 'hybrid SPDK'}


class Focused:
    def __init__(self):
        self.ops = read_csv('run8-ops_summary.csv')
        self.components = read_csv('run8-component_summary.csv')
        self.series = {}
        self.tables = {}
        for shape, size, c, prefix in [('put', 1024, 1, 'p1'), ('put', 65536, 1, 'p64'),
                                       ('put', 1024, 16, 'p16'), ('delete', 1024, 1, 'del'),
                                       ('put_batch', 8192, 1, 'batch8')]:
            for token, route in [('v', 'owner verbs'), ('t', 'owner TCP'), ('m', 'verbs mTLS'),
                                 ('k', 'hybrid kernel DIRECT'), ('s', 'hybrid SPDK'), ('n', 'native')]:
                found = [r for r in self.ops if r['group'] == 'G12D' and r['route'] == route
                         and r['shape'] == shape and int(r['size_bytes']) == size
                         and int(r['concurrency']) == c and r['key_policy'] == 'overwrite'
                         and r['durability'] == ('value_wal_barrier' if route == 'native' else 'synchronous')
                         and ('write_profile_limit' if route == 'native' else 'write_timeline_limit')
                         in json.loads(r['diagnostics'])]
                if found:
                    self.series[prefix + token] = self.unique(found, prefix + token)
        for state, prefix in [('memory', 'mem'), ('cold', 'ssd')]:
            for size, st in [(1024, '1'), (65536, '64')]:
                for token, route in [('v', 'owner verbs'), ('t', 'owner TCP'), ('m', 'verbs mTLS'),
                                     ('k', 'hybrid kernel buffered' if state == 'memory' else 'hybrid kernel DIRECT'),
                                     ('s', 'hybrid SPDK'), ('n', 'native')]:
                    found = [r for r in self.ops if r['group'] == ('G12N' if route == 'native' else 'G12D')
                             and r['route'] == route and r['shape'] == 'read'
                             and int(r['size_bytes']) == size and r['concurrency'] == '1'
                             and r['cache_state'] == state]
                    if found:
                        self.series[prefix + st + token] = self.unique(found, prefix + st + token)
        for alias, group, route, shape, size in [
            ('batch1n', 'G12N', 'native', 'put_batch', 1024),
            ('batch8n', 'G12N', 'native', 'put_batch', 8192),
            ('batch1v', 'G12W', 'owner verbs', 'put_batch', 1024),
            ('ubatch8v', 'G12W', 'owner verbs', 'put_batch', 8192),
            ('uput1v', 'G12W', 'owner verbs', 'put', 1024),
            ('uput1m', 'G12W', 'verbs mTLS', 'put', 1024)]:
            found = [r for r in self.ops if r['group'] == group and r['route'] == route
                     and r['shape'] == shape and r['size_bytes'] == str(size)
                     and r['concurrency'] == '1' and r['key_policy'] == 'overwrite'
                     and r['durability'] == ('value_wal_barrier' if route == 'native' else 'synchronous')
                     and json.loads(r['diagnostics']) == {}]
            self.series[alias] = self.unique(found, alias)
        for i, row in enumerate(self.ops):
            self.series[f'op{i:03d}'] = row

    @staticmethod
    def unique(found, key):
        if len(found) != 1:
            raise KeyError(f'{len(found)} rows for {key}')
        return found[0]

    def comp(self, series, token):
        component = COMPONENTS[token]
        case_ids = json.loads(self.series[series]['case_ids'])
        if len(case_ids) != 1:
            return None  # Whole-call native read statistics do not provide a profiler.
        found = [r for r in self.components if r['case_id'] == case_ids[0]
                 and r['component'] == component]
        return self.unique(found, (series, token)) if found else None

    def table(self, name):
        if name not in self.tables:
            self.tables[name] = read_csv(name + '.csv')
        return self.tables[name]

    def run7(self, kind, route, size, c, tier, metric):
        name = 'run7-device_' + ('read' if kind == 'reads' else 'put') + '_spread'
        found = [r for r in self.table(name) if r['route'] == ROUTES7[route]
                 and r['size_bytes'] == size and r['concurrency'] == c
                 and r['durability'] == tier and r['metric'] == metric]
        return self.unique(found, (kind, route, size, c, tier, metric))


FOCUSED = None
legacy_evaluate = evaluate


def evaluate(key, data, stats):
    global FOCUSED
    if FOCUSED is None:
        FOCUSED = Focused()
    bare, _, spec = key.partition('|')
    kind, *args = bare.split(':')
    if kind == 'B':
        table, label, field = args
        value = FOCUSED.unique([r for r in FOCUSED.table(table) if r.get('label', r.get('key')) == label],
                               (table, label))[field]
        return format(float(value), spec or '.1f').replace(',', '{,}') if value != '' else ''
    if kind == 'J':
        name, = args
        native16 = data.pop('native', 'primary_rf1', 'write-concurrent', 'put', 65536)
        native1 = data.pop('native', 'primary_rf1', 'point-mixed', 'put', 65536)
        if name == 'native_mean_ratio':
            value = native16['api_mean_us'] / native1['api_mean_us']
        elif name == 'native_reciprocal':
            value = 1e6 / native1['api_mean_us']
        elif name == 'native_reciprocal_pct':
            value = native16['case_success_calls_per_second'] * native1['api_mean_us'] / 1e4
        elif name in ('pagecache64', 'pagecache8', 'pagecache1', 'pagecache_c1_64', 'pagecache_jump', 'pagecache_ms'):
            def page(profile, cohort, size):
                return FOCUSED.unique([r for r in data.rows if r['label'] == 'native' and r['profile'] == profile
                                      and r['cohort'] == cohort and r['size'] == size and r['operation'] == 'put'],
                                      (profile, cohort, size))
            pc64 = page('write-concurrent', 'native_async_write', 65536)
            pc8 = page('write-concurrent', 'native_async_write', 8192)
            pc1 = page('write-concurrent', 'native_async_write', 1024)
            value = {'pagecache64': pc64['api_p50_us'], 'pagecache8': pc8['api_p50_us'],
                     'pagecache1': pc1['api_p50_us'], 'pagecache_jump': pc64['api_p50_us'] / pc8['api_p50_us'],
                     'pagecache_ms': pc1['elapsed_seconds'] * 1000,
                     'pagecache_c1_64': page('point-mixed', 'native_pagecache', 65536)['api_p50_us']}[name]
        elif name == 'owner64_software_pct':
            value = 100 * (float(FOCUSED.comp('p64v', 'coordinator')['mean_us'])
                           + float(FOCUSED.comp('p64v', 'owner')['mean_us'])) / float(FOCUSED.series['p64v']['mean_us'])
        elif name == 'pagecache1_cps':
            value = FOCUSED.unique([r for r in data.rows if r['label'] == 'native' and r['cohort'] == 'native_async_write'
                                  and r['profile'] == 'write-concurrent' and r['size'] == 1024 and r['operation'] == 'put'], name)['case_success_calls_per_second']
        else:
            raise KeyError(name)
        return format(value, spec or '.1f').replace(',', '{,}')
    if kind == 'C':
        row = FOCUSED.comp(*args)
        value = row['mean_us'] if row is not None else 'missing'
    elif kind == 'D':
        series, field = args
        value = FOCUSED.series[series][field]
        if field == 'n':
            return '/'.join(str(x) for x in json.loads(value).values())
    elif kind == 'Q':
        table, index, field = args
        value = FOCUSED.table(table)[int(index)][field]
    elif kind == 'A':
        selection, field, statistic = args
        found = [r for r in FOCUSED.table('owner-stages') if r['selection'] == selection and r['field'] == field]
        value = FOCUSED.unique(found, (selection, field))[statistic]
    elif kind == 'P':
        field, statistic = args
        found = [r for r in FOCUSED.table('owner-profile') if r['field'] == field]
        value = FOCUSED.unique(found, field)[statistic]
    elif kind == 'Y':
        route, size, concurrency, statistic = args
        route = {'n': 'native', 'v': 'owner verbs', 't': 'owner TCP', 'f': 'owner0 overwrite floor'}[route]
        step = 'file + directory fsync' if route == 'native' else 'file fdatasync'
        found = [r for r in FOCUSED.table('run8-sync_step') if r['route'] == route
                 and r['size_bytes'] == size and r['concurrency'] == concurrency
                 and r['step'] == step and '-r0-' in r['case_id']]
        value = FOCUSED.unique(found, (route, size, concurrency, step))[statistic]
    elif kind == 'E':
        name, = args
        if name == 'profile_persist_gap':
            profile = {r['field']: float(r['mean_us']) for r in FOCUSED.table('owner-profile')}
            value = profile['connection_append_persist_ns'] - profile['storage_persist_total_ns']
        elif name == 'native_read_floor_gap':
            floor = FOCUSED.unique([r for r in FOCUSED.table('run8-ssd_floor')
                                    if r['block_bytes'] == '65536' and r['region_bytes'] == '2147483648'
                                    and r['state'] == 'fresh' and r['repeat'] == '0'], name)
            value = float(FOCUSED.series['ssd64n']['p50_us']) - float(floor['p50_us'])
        elif name in ('run7_put1_gap', 'run7_put64_gap'):
            size = '1024' if name == 'run7_put1_gap' else '65536'
            value = (float(FOCUSED.run7('puts', 'v', size, '1', 'synchronous', 'api_p50_us')['median']) -
                     float(FOCUSED.run7('puts', 'v', size, '1', 'native', 'api_p50_us')['median']))
        else:
            raise KeyError(name)
    elif kind == 'F':
        size, region, state, repeat, statistic = args
        found = [r for r in FOCUSED.table('run8-ssd_floor') if r['block_bytes'] == size
                 and r['region_bytes'] == region and r['state'] == state and r['repeat'] == repeat]
        value = FOCUSED.unique(found, (size, region, state, repeat))[statistic]
    elif kind == 'W':
        series, = args
        value = 100 * sum(float(FOCUSED.comp(series, t)['mean_us']) for t in
                          ['durable', 'coalescing', 'queue']) / float(FOCUSED.series[series]['mean_us'])
    elif kind == 'R':
        *selection, field = args
        value = FOCUSED.run7(*selection)[field]
    else:
        return legacy_evaluate(key, data, stats)
    if value in ('missing', '', None):
        return 'n/c'
    return format(float(value), spec or '.1f').replace(',', '{,}')
