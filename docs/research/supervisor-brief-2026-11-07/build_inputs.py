#!/usr/bin/env python3
"""Build the trimmed input tables of the 2026-11-07 brief from the retrieved campaign export.

Reads the strict report of the export (regenerated with the fixed report code) and
the per-case measurement gates of the export, and writes small tables next to this file:

* populations.csv          this run, one row per case and operation population
* previous-populations.csv previous campaign, same columns where available
* device-reads.csv         block-device counters between the measurement gates

The export is large (12 GB) and is not kept in the repository. The tables
and inputs.json (source hashes) are the inputs of plot_final.py and make_tables.py.
"""
import bisect
import csv
import hashlib
import json
import statistics
from pathlib import Path

import numpy

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
WORK = ROOT / 'checkpoint-work/spdk-matched-storage-20261105T004442Z'
CAMPAIGN = WORK / 'gcp-full-matrix-campaign-20261106T213227Z-direct'
REPORT = WORK / 'analysis/gcp-full-matrix-2026-11-07/report-fixed'
CASES = CAMPAIGN / 'extract/cases'
PREVIOUS = WORK / 'gcp-queue-bounded-campaign-20261106T032027Z/final-retrieval/extract/comparison.csv'

KEEP = [
    'case_id', 'status', 'backend', 'durability_tier', 'native_durability', 'nvmeof_target',
    'cohort', 'profile', 'value_bytes', 'keys', 'concurrency', 'offered_ops_per_second',
    'observed_calls', 'planned_calls', 'successful_calls', 'unsuccessful_calls', 'elapsed_seconds',
    'case_success_calls_per_second', 'case_success_value_mib_per_second', 'operation', 'outcome',
    'population_calls', 'api_mean_us', 'api_p50_us', 'api_p95_us', 'api_p99_us', 'api_p999_us',
    'api_max_us', 'response_p50_us', 'response_p99_us', 'replication_factor', 'acknowledgement',
]
LABEL = {
    'native': 'native', 'tidehunter_tcp': 'th_tcp', 'tidehunter_verbs_native': 'th_verbs',
    'tidehunter_nvme_rdma': 'th_kernel', 'tidehunter_spdk_live_rdma': 'th_spdk',
    'tidehunter_spdk_rdma': 'th_spdk', 'valkey_tcp': 'valkey_tcp', 'valkey_rdma': 'valkey_rdma',
    'tikv': 'tikv', 'cassandra': 'cassandra', 'foundationdb': 'foundationdb',
    'kvrocks': 'kvrocks', 'garnet': 'garnet',
}
EXTERNAL = ('valkey_tcp', 'valkey_rdma', 'tikv', 'cassandra', 'foundationdb', 'kvrocks', 'garnet')
# User decision: every other store's value is the median over all GCP runs that completed the population.
MEDIANS = WORK / 'analysis/gcp-full-matrix-2026-11-07/fixes/external-store-medians.csv'
# Population column -> column of the medians table (case-level values repeat on each operation row).
MEDIAN_COLUMNS = {
    'api_mean_us': 'median_api_mean_us', 'api_p50_us': 'median_api_p50_us',
    'api_p95_us': 'median_api_p95_us', 'api_p99_us': 'median_api_p99_us',
    'api_p999_us': 'median_api_p999_us', 'api_max_us': 'median_api_max_us',
    'response_p50_us': 'median_response_p50_us', 'response_p99_us': 'median_response_p99_us',
    'case_success_calls_per_second': 'median_throughput_ops_per_second',
    'case_success_value_mib_per_second': 'median_case_success_value_mib_per_second',
    'successful_calls': 'median_successful_operations',
    'unsuccessful_calls': 'median_unsuccessful_operations',
}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def read_rows(path):
    with open(path, newline='') as handle:
        return list(csv.DictReader(handle))


def number_text(value):
    """Canonical text of a numeric cell, so that '1000' and '1000.0' join."""
    if value == '':
        return ''
    number = float(value)
    return str(int(number)) if number == int(number) else repr(number)


def identity(row, store):
    return (store, row['cohort'], row['profile'], number_text(row['value_bytes']), number_text(row['keys']),
            number_text(row['concurrency']), number_text(row['offered_ops_per_second']), row['operation'],
            row['outcome'])


def load_medians():
    table = {}
    for row in read_rows(MEDIANS):
        key = identity(row, row['store'])
        if key in table:
            raise ValueError(f'duplicate median identity {key}')
        table[key] = row
    return table


def population_rows(path, flagged, medians=None):
    """Rows of one report. With `medians`, completed external-store rows take the cross-run medians."""
    out = []
    for row in read_rows(path):
        slim = {key: row.get(key, '') for key in KEEP}
        slim['acknowledgement'] = slim['acknowledgement'][:120]
        slim['label'] = LABEL[row['backend']]
        slim['flag_spdk_accounting'] = '1' if row['case_id'] in flagged else ''
        slim['median_runs'] = ''
        slim['raw_api_p50_us'] = row['api_p50_us']
        if medians is not None and row['backend'] in EXTERNAL and row['status'] == 'completed' and row['api_p50_us']:
            median = medians[identity(row, row['backend'])]
            for column, source in MEDIAN_COLUMNS.items():
                if row[column] != '' and median[source] == '':
                    raise ValueError(f'no median of {column} for {median["population_id"]}')
                slim[column] = median[source] if row[column] != '' else ''
            slim['median_runs'] = median['n_runs']
        out.append(slim)
    return out


def percentile_nearest(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def write_spread(medians):
    """Fastest-to-slowest p50 ratio across runs of the same external-store population (n_runs >= 2)."""
    rows = []
    groups = {'all': []}
    for row in medians.values():
        if row['n_runs'] and int(row['n_runs']) >= 2 and row['p50_max_over_min']:
            ratio = float(row['p50_max_over_min'])
            groups['all'].append(ratio)
            groups.setdefault(row['store'], []).append(ratio)
    for store, ratios in groups.items():
        rows.append({'store': store, 'populations': len(ratios), 'spread_median': statistics.median(ratios),
                     'spread_p90': percentile_nearest(ratios, 0.9), 'spread_max': max(ratios)})
    write_rows(HERE / 'external-spread.csv', rows,
               ['store', 'populations', 'spread_median', 'spread_p90', 'spread_max'])



def write_rows(path, rows, columns):
    with open(path, 'w', newline='') as handle:
        writer = csv.DictWriter(handle, columns, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def diskstats(text):
    """Map device name to (reads completed, sectors read, ms reading, writes completed, sectors written,
    ms writing, flush requests completed) from /proc/diskstats. Flushes are 0 on kernels without the field."""
    stats = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 14:
            flushes = int(parts[18]) if len(parts) >= 20 else 0
            stats[parts[2]] = (int(parts[3]), int(parts[5]), int(parts[6]), int(parts[7]), int(parts[9]),
                               int(parts[10]), flushes)
    return stats


def initiator_namespace(name):
    """Remote namespaces on the coordinator: nvmeXn1 with X >= 2, never a path or partition device."""
    return (name.startswith('nvme') and name.endswith('n1') and 'c' not in name[4:]
            and name not in ('nvme0n1', 'nvme1n1'))


def device_row(case_id):
    directory = CASES / case_id
    before = json.load(open(directory / 'measurement-before.json'))
    end = json.load(open(directory / 'measurement-end.json'))
    row = {'case_id': case_id}
    storage = [0] * 7
    for index, (host_before, host_end) in enumerate(zip(before['hosts'], end['hosts'])):
        # The Local SSD is /dev/nvme0n1 (ext4, benchmark data) on all six hosts; checked per case.
        local = [d['name'] for d in host_before['nvme']['block_devices']['blockdevices']
                 if 'local-nvme' in str(d.get('serial', ''))]
        if local != ['/dev/nvme0n1']:
            raise ValueError(f'{case_id}: host {index} local SSD is {local}')
        a, b = diskstats(host_before['diskstats']), diskstats(host_end['diskstats'])
        delta = [y - x for x, y in zip(a['nvme0n1'], b['nvme0n1'])]
        if index == 0:
            row.update(coordinator_reads=delta[0], coordinator_bytes=delta[1] * 512, coordinator_ms=delta[2],
                       coordinator_writes=delta[3], coordinator_write_bytes=delta[4] * 512,
                       coordinator_write_ms=delta[5], coordinator_flushes=delta[6])
            initiator = [0, 0, 0]
            for name in b:
                if initiator_namespace(name) and name in a:
                    initiator = [i + y - x for i, x, y in zip(initiator, a[name], b[name])]
            row.update(initiator_reads=initiator[0], initiator_bytes=initiator[1] * 512, initiator_ms=initiator[2])
        else:
            storage = [s + d for s, d in zip(storage, delta)]
    row.update(storage_reads=storage[0], storage_bytes=storage[1] * 512, storage_ms=storage[2],
               storage_writes=storage[3], storage_write_bytes=storage[4] * 512, storage_write_ms=storage[5],
               storage_flushes=storage[6],
               dropped_os_caches=int(any(h.get('drop_os_caches') for h in before['hosts'])))
    return row


def case_dir(prefix):
    found = sorted(p for p in CASES.iterdir() if p.name.startswith(prefix + '-'))
    if len(found) != 1:
        raise ValueError(f'{len(found)} case directories for prefix {prefix}')
    return found[0]


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def write_timeline():
    """Contiguous intervals between consecutive timeline events of the instrumented verbs write case.

    The intervals of one call sum exactly to its caller span. Owner record fields use owner-local clocks.
    """
    case = case_dir('0491')
    timeline = json.load(open(case / 'result/write-timeline.json'))['timeline']
    records = []
    for record in timeline['records']:
        seen, events = set(), []
        for event in record['events']:
            if event['stage'] not in seen:
                seen.add(event['stage'])
                events.append((event['stage'], event['elapsed_ns']))
        records.append(events)
    order = [stage for stage, _ in records[0]]
    if any([stage for stage, _ in events] != order for events in records):
        raise ValueError('timeline records have different event orders')
    rows = []
    for first, second in zip(order, order[1:]):
        spans = [(dict(events)[second] - dict(events)[first]) / 1000.0 for events in records]
        rows.append({'kind': 'interval', 'name': f'{first}>{second}', 'n': len(spans),
                     'mean_us': sum(spans) / len(spans), 'median_us': percentile(spans, .5),
                     'p99_us': percentile(spans, .99)})
    owner = timeline['owner_records']
    for name in ('handler_ns', 'append_persist_ns', 'wal_file_persist_ns', 'append_wal_ns', 'control_queue_ns'):
        values = [r[name] / 1000.0 for r in owner if r.get(name) is not None]
        rows.append({'kind': 'owner', 'name': name, 'n': len(values), 'mean_us': sum(values) / len(values),
                     'median_us': percentile(values, .5), 'p99_us': percentile(values, .99)})
    batches = [r['batch_count'] for r in owner]
    rows.append({'kind': 'owner', 'name': 'batches_per_owner_record', 'n': len(batches),
                 'mean_us': sum(batches) / len(batches), 'median_us': percentile(batches, .5),
                 'p99_us': percentile(batches, .99)})
    # Waits at the coordinator authority thread. W1: owner_enqueue_started>owner_dequeued against the number of
    # commands queued ahead at its start. W2: admitted>append_enqueue_started and W3:
    # completion_harvested>durable_observed, each against the number of owner_dequeued events inside the
    # interval (commands the thread took meanwhile). Pearson r and the mean wait at 0 and 8 competing events.
    stamps = [dict(events) for events in records]
    arrivals = sorted(s['owner_enqueue_started'] for s in stamps)
    dequeues = sorted(s['owner_dequeued'] for s in stamps)
    waits = {'w1': ([], []), 'w2': ([], []), 'w3': ([], [])}
    for s in stamps:
        start = s['owner_enqueue_started']
        waits['w1'][0].append((s['owner_dequeued'] - start) / 1000.0)
        waits['w1'][1].append(bisect.bisect_right(arrivals, start) - bisect.bisect_right(dequeues, start) - 1)
        for name, begin, end in (('w2', 'admitted', 'append_enqueue_started'),
                                 ('w3', 'completion_harvested', 'durable_observed')):
            waits[name][0].append((s[end] - s[begin]) / 1000.0)
            waits[name][1].append(bisect.bisect_right(dequeues, s[end]) - bisect.bisect_right(dequeues, s[begin]))
    for name, (spans, counts) in waits.items():
        rows.append({'kind': 'corr', 'name': name, 'n': len(spans), 'mean_us': float(numpy.corrcoef(spans, counts)[0, 1]),
                     'median_us': 'nan', 'p99_us': 'nan'})
        for level in (0, 8):
            chosen = [span for span, count in zip(spans, counts) if count == level]
            rows.append({'kind': 'bucket', 'name': f'{name}_competing{level}', 'n': len(chosen),
                         'mean_us': sum(chosen) / len(chosen), 'median_us': percentile(chosen, .5),
                         'p99_us': percentile(chosen, .99)})
    write_rows(HERE / 'write-timeline-spans.csv', rows, ['kind', 'name', 'n', 'mean_us', 'median_us', 'p99_us'])
    return case.name


def readmany_traces():
    """Where the time of a traced 32-key call goes, on the path of its last-delivered owner flight."""
    rows = []
    for prefix in ('0474', '0483', '0093', '0095'):
        case = case_dir(prefix)
        profile = json.load(open(case / 'result/read-diagnostics.json'))['profile']
        parts = []
        for call in profile['caller_correlation']['trace']['calls']:
            events = {e['name']: e['elapsed_ns'] for e in call['events']}
            if 'api_started' not in events or 'api_finished' not in events:
                continue
            groups = []
            for group in call.get('groups', []):
                named = {}
                for event in group['events']:
                    named.setdefault(event['name'], event['elapsed_ns'])
                if all(n in named for n in ('admission_started', 'admitted', 'transport_request_submitted',
                                            'flight_completed', 'request_result_delivered')):
                    groups.append(named)
            if not groups:
                continue
            last = max(groups, key=lambda g: g['request_result_delivered'])
            first_start = min(g['admission_started'] for g in groups)
            parts.append([
                events['api_finished'] - events['api_started'], first_start - events['api_started'],
                last['admission_started'] - first_start, last['admitted'] - last['admission_started'],
                last['transport_request_submitted'] - last['admitted'],
                last['flight_completed'] - last['transport_request_submitted'],
                last['request_result_delivered'] - last['flight_completed'],
                events['api_finished'] - last['request_result_delivered'], len(call.get('groups', []))])
        means = [sum(p[i] for p in parts) / len(parts) for i in range(9)]
        rows.append({'case_id': case.name, 'n_calls': len(parts), 'total_us': means[0] / 1000.0,
                     'preflight_us': means[1] / 1000.0, 'group_gap_us': means[2] / 1000.0,
                     'admit_us': means[3] / 1000.0, 'submit_wait_us': means[4] / 1000.0,
                     'flight_us': means[5] / 1000.0, 'deliver_us': means[6] / 1000.0,
                     'post_us': means[7] / 1000.0, 'groups_per_call': means[8]})
    write_rows(HERE / 'readmany-trace.csv', rows, list(rows[0]))


STAGE_SELECTION = {
    '0282': [('profile', 'write_profile_client_exchange_ns'), ('pre_completion_gate', 'write_preparation_admission_ns'),
             ('pre_completion_gate', 'write_durable_receipt_publication_blocked_ns'),
             ('profile', 'write_profile_storage_persist_total_ns'),
             ('profile', 'write_profile_storage_persist_wal_file_ns')],
    '0447': [('profile', 'write_profile_client_exchange_ns'), ('profile', 'write_profile_storage_persist_total_ns'),
             ('profile', 'write_profile_storage_persist_wal_file_ns')],
    '0346': [('live_nvmeof', n) for n in ('control_ns', 'persistence_ns', 'frontier_file_ns', 'frontier_namespace_ns',
                                          'wal_persistence_ns', 'cq_ns')],
    '0347': [('live_nvmeof', n) for n in ('control_ns', 'persistence_ns', 'frontier_file_ns', 'frontier_namespace_ns',
                                          'wal_persistence_ns', 'cq_ns')],
    '0011': [('profile', 'exchange'), ('profile', 'server_handler')],
    '0041': [('profile', 'exchange'), ('profile', 'server_handler')],
    '0474': [('profile', 'exchange'), ('profile', 'server_handler')],
    '0483': [('profile', 'exchange'), ('profile', 'server_handler')],
}


def history_loads(node):
    """Sum every `history_loads` counter below one measurement-evidence snapshot."""
    if isinstance(node, dict):
        return sum(v if key == 'history_loads' else history_loads(v) for key, v in node.items())
    if isinstance(node, list):
        return sum(history_loads(v) for v in node)
    return 0


def stage_evidence():
    """Selected stage means from the stage report (bottlenecks/stage-breakdown.csv); each was derived from
    result/read-diagnostics.json or result/write-diagnostics.json as sum_ns / count."""
    source = WORK / 'analysis/gcp-full-matrix-2026-11-07/bottlenecks/stage-breakdown.csv'
    table = read_rows(source)
    rows = []
    for prefix, fields in STAGE_SELECTION.items():
        for group, field in fields:
            hits = [r for r in table if r['case_id'].startswith(prefix + '-') and r['group'] == group
                    and r['field'] == field]
            if len(hits) != 1:
                raise ValueError(f'{len(hits)} stage rows for {prefix} {group} {field}')
            rows.append({'case': prefix, 'group': group, 'field': field, 'count': hits[0]['count'],
                         'mean_us': hits[0]['mean_us'], 'denominator': hits[0]['denominator']})
    # raw counters: publication-blocked batches against durable batches (write cases 0282 and 0447)
    for prefix in ('0282', '0447'):
        case = case_dir(prefix)
        stages = json.load(open(case / 'result/write-diagnostics.json'))['pre_completion_gate']['stages']
        for field in ('write_durable_receipt_publication_blocked_batches', 'write_durable_batches'):
            rows.append({'case': prefix, 'group': 'pre_completion_gate', 'field': field,
                         'count': stages[field], 'mean_us': '', 'denominator': 'batches'})
    # Authority history loads in the measured interval of the cold 64 KiB cases (owner RPC and NVMe-oF).
    for prefix in ('0283', '0448', '0348', '0349'):
        evidence = json.load(open(case_dir(prefix) / 'result/measurement-read-evidence.json'))
        start, end = (history_loads(evidence[side]) for side in ('start', 'end'))
        rows.append({'case': prefix, 'group': 'authority', 'field': 'history_loads',
                     'count': end - start, 'mean_us': '', 'denominator': 'measured interval'})
    write_rows(HERE / 'stage-evidence.csv', rows, ['case', 'group', 'field', 'count', 'mean_us', 'denominator'])


def main():
    medians = load_medians()
    current = population_rows(REPORT / 'comparison.csv', set(), medians)
    previous = population_rows(PREVIOUS, set())
    write_spread(medians)
    columns = KEEP + ['label', 'flag_spdk_accounting', 'median_runs', 'raw_api_p50_us']
    write_rows(HERE / 'populations.csv', current, columns)
    write_rows(HERE / 'previous-populations.csv', previous, columns)
    cases = sorted({r['case_id'] for r in current if r['status'] == 'completed'})
    devices = [device_row(case) for case in cases]
    write_rows(HERE / 'device-reads.csv', devices, list(devices[0]))
    timeline_case = write_timeline()
    readmany_traces()
    stage_evidence()
    outputs = ('populations.csv', 'previous-populations.csv', 'device-reads.csv', 'write-timeline-spans.csv',
               'readmany-trace.csv', 'stage-evidence.csv', 'external-spread.csv')
    manifest = {
        'campaign': 'gcp-full-matrix-campaign-20261106T213227Z', 'fleet_run_id': 'bench-0d99e8256fff4891',
        'sources': {str(p.relative_to(ROOT)): sha256(p) for p in (REPORT / 'comparison.csv', PREVIOUS, MEDIANS)},
        'outputs': {name: sha256(HERE / name) for name in outputs}, 'timeline_case': timeline_case,
        'cases_with_device_rows': len(devices),
    }
    (HERE / 'inputs.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(len(current), 'population rows;', len(previous), 'previous rows;', len(devices), 'device rows')


if __name__ == '__main__':
    main()
