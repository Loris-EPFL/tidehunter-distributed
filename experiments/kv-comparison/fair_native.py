#!/usr/bin/env python3
"""Report Native and owner puts from one focused campaign.

Run: python3 fair_native.py --export EXPORT --report REPORT --output OUTPUT
"""
import argparse
import csv
import json
import math
import re
from pathlib import Path

import focused_analysis

GROUPS = ('G14N', 'G14S', 'G14O1', 'G14O5', 'G14P')
FIELDS = ('case_id', 'status', 'error', 'group', 'measurement_class', 'size_bytes',
          'repeat', 'cohort', 'route_or_mode', 'concurrency', 'owner_count',
          'native_shards', 'topology', 'network', 'durability', 'routing',
          'callers_per_ssd_derived', 'n', 'offered_calls', 'unsuccessful_calls',
          'api_mean_us', 'api_p50_us', 'api_p99_us', 'successful_calls_per_second',
          'mean_inflight_calls_derived', 'group_stats_per_shard')


def group_stats(path):
    """Return cumulative barrier statistics separately for each metric shard."""
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return {}
    prefix = 'kv_research_native_group_barrier_'
    shards = {}
    seen = set()
    for line in lines:
        match = re.fullmatch(r'([^\s{]+)(?:\{([^}]*)\})?\s+(\S+)(?:\s+\S+)?', line)
        if not match or not match.group(1).startswith(prefix):
            continue
        name, raw_labels, raw_value = match.groups()
        labels = {}
        if raw_labels:
            for label in raw_labels.split(','):
                parsed = re.fullmatch(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"\s*', label)
                if not parsed or parsed.group(1) in labels:
                    raise ValueError('invalid group barrier labels')
                labels[parsed.group(1)] = parsed.group(2)
        shard = labels.pop('shard', '0')
        bound = labels.pop('le', None)
        if labels:
            raise ValueError('unexpected group barrier labels')
        metric = name[len(prefix):]
        key = (shard, metric, bound)
        if key in seen:
            raise ValueError('duplicate group barrier metric')
        seen.add(key)
        try:
            value = float(raw_value)
        except ValueError as exc:
            raise ValueError('invalid group barrier metric') from exc
        if not math.isfinite(value) or value < 0:
            raise ValueError('invalid group barrier metric')
        data = shards.setdefault(shard, {'buckets': []})
        if metric == 'group_size_bucket':
            if bound is None:
                raise ValueError('group histogram bucket has no bound')
            try:
                upper = float(bound)
            except ValueError as exc:
                raise ValueError('invalid group histogram bound') from exc
            if math.isnan(upper) or upper < 0:
                raise ValueError('invalid group histogram bound')
            data['buckets'].append((upper, value))
        elif bound is not None:
            raise ValueError('unexpected histogram bound')
        elif metric in ('runs_total', 'callers_total', 'errors_total'):
            data[metric] = value
    result = {}
    for shard, data in sorted(shards.items()):
        runs, callers, errors = (data.get(name) for name in ('runs_total', 'callers_total', 'errors_total'))
        successful = runs - errors if runs is not None and errors is not None else None
        if successful is not None and successful < 0:
            raise ValueError('group barrier errors exceed runs')
        buckets = sorted(data['buckets'])
        if len({bound for bound, _ in buckets}) != len(buckets) or any(
                left[1] > right[1] for left, right in zip(buckets, buckets[1:])):
            raise ValueError('invalid cumulative group histogram')
        upper = None
        if buckets and math.isinf(buckets[-1][0]) and buckets[-1][1] > 0:
            upper = next((bound for bound, count in buckets if count == buckets[-1][1]), None)
            if upper is not None and not math.isfinite(upper):
                upper = None
        result[shard] = {'runs': runs, 'callers': callers, 'errors': errors,
                         'mean_size_derived': callers / successful if callers is not None and successful else None,
                         'max_size_upper_bound_derived': upper}
    return result


def _workload(job):
    return tuple(json.dumps(job.get(key), sort_keys=True) for key in
                 ('keys', 'mix', 'measurement_operations', 'warmup_operations',
                  'distribution', 'offered_ops_per_second', 'put_key_policy', 'diagnostics'))


def _row(export, case):
    job = case['job']
    native = job['backend'] == 'native'
    concurrency = job['concurrency']
    shards = job.get('native_shards') or 1 if native else None
    owners = None if native else (job.get('owner_count') or 5)
    mode = job.get('native_durability') if native else job['backend']
    row = dict.fromkeys(FIELDS)
    row.update(case_id=case['case_id'], status=case.get('status', 'not_run'),
               error=case.get('error'), group=job['group'],
               measurement_class='profiled' if job['group'] == 'G14P' else 'unprofiled',
               size_bytes=job['value_size'], repeat=job.get('repeat'), cohort=job.get('cohort'),
               route_or_mode=mode, concurrency=concurrency, owner_count=owners, native_shards=shards,
               topology=f'{shards} Db, one process, one SSD' if native else f'{owners} active owners and SSDs, coordinator',
               network='in-process' if native else job['backend'],
               durability=job.get('native_durability') if native else job.get('durability_tier'),
               routing='key hash to Db' if native and shards > 1 else
                       'single Db' if native else 'operation ID hash to owner',
               callers_per_ssd_derived=concurrency if native else concurrency / owners)
    if row['status'] != 'completed':
        return row
    metrics = case.get('metrics') or {}
    pop = focused_analysis.population(case, 'put')
    row.update(n=pop.get('count'), offered_calls=metrics.get('offered_calls'),
               unsuccessful_calls=metrics.get('unsuccessful_calls'),
               api_mean_us=pop['api_mean_ns'] / 1000 if pop.get('api_mean_ns') is not None else None,
               api_p50_us=focused_analysis.latency(pop, 'api', 'p50'),
               api_p99_us=focused_analysis.latency(pop, 'api', 'p99'),
               successful_calls_per_second=metrics.get('success_calls_per_second'))
    timeline = metrics.get('measurement_timeline') or {}
    inflight = timeline.get('mean_observed_api_concurrency')
    if inflight is None and timeline.get('api_time_sum_ns') is not None and timeline.get('duration_ns'):
        inflight = timeline['api_time_sum_ns'] / timeline['duration_ns']
    row['mean_inflight_calls_derived'] = inflight
    if native and mode == 'value_wal_group_barrier':
        stats = group_stats(export / 'cases' / case['case_id'] / 'result' / 'metrics.prom')
        expected = {str(index) for index in range(shards)}
        if set(stats) - expected:
            raise ValueError('group barrier metric has an unexpected shard')
        row['group_stats_per_shard'] = {index: stats.get(index) for index in sorted(expected)}
    return row


def _cell(rows):
    if not rows:
        return '—'
    def value(item):
        return 'n/a' if item is None else str(item)
    return '<br>'.join(
        f"r{value(row['repeat'])} {row['case_id']} {row['status']}"
        + (f" n={value(row['n'])} mean={value(row['api_mean_us'])} µs"
           f" p50={value(row['api_p50_us'])} µs p99={value(row['api_p99_us'])} µs"
           f" rate={value(row['successful_calls_per_second'])}/s"
           f" in-flight={value(row['mean_inflight_calls_derived'])}"
           f" offered={value(row['offered_calls'])} unsuccessful={value(row['unsuccessful_calls'])}"
           if row['status'] == 'completed' else f" error={value(row['error'])}")
        + (f" group stats by shard={json.dumps(row['group_stats_per_shard'], sort_keys=True)}"
           if row['group_stats_per_shard'] is not None else '')
        for row in sorted(rows, key=lambda item: (item['repeat'] is None, item['repeat'], item['case_id'])))


def generate(export, report, output):
    export, report, output = map(Path, (export, report, output))
    jobs = json.loads((export / 'matrix.json').read_text())['jobs']
    cases = json.loads((report / 'comparison.json').read_text())['cases']
    planned = {job['case_id']: job for job in jobs}
    if (len(planned) != len(jobs) or len(cases) != len(planned)
            or len({case['case_id'] for case in cases}) != len(cases)
            or any(case.get('job') != planned.get(case.get('case_id')) for case in cases)):
        raise ValueError('report cases do not match export matrix')
    selected = [case for case in cases if case['job'].get('group') in GROUPS]
    rows = [_row(export, case) for case in selected]
    rows.sort(key=lambda row: (row['size_bytes'], row['measurement_class'], row['route_or_mode'],
                               row['concurrency'], row['repeat'], row['case_id']))
    output.mkdir(parents=True, exist_ok=True)
    with (output / 'fair_native.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows({**row, 'group_stats_per_shard': json.dumps(row['group_stats_per_shard'], sort_keys=True)
                          if row['group_stats_per_shard'] is not None else None} for row in rows)
    lines = ['# Fair Native comparison', '',
             'One export is one fleet. Each cell lists independent repetitions; no samples are pooled. '
             'Only rows in the same table and column share size, total concurrency, cohort, workload and fleet. '
             'Profiled and unprofiled cases are separate; no ratios or rankings are implied. '
             'API latency is for successful puts; rate covers the whole case and in-flight calls include dispatched outcomes. '
             'Callers per SSD for owners is a derived even-spread average, not measured placement. '
             'Native key-hash routing and owner operation-ID routing differ. Native uses one SSD even with five Db instances; '
             'owner arms use one or five SSDs and a network hop. Native barriers sync registered files and the directory; '
             'owners sync an owner file. Native per-put barriers do not share syncs, grouped barriers do. '
             'Page-cache acknowledgements are not durable. These topology, routing, network, index and sync differences '
             'remain unmatched even at equal concurrency.', '',
             'Group barrier statistics in the CSV are cumulative per shard over the whole process, including warmup. '
             'Runs include errors; mean group size uses successful runs. Maximum group size is a histogram upper bound, '
             'not an exact maximum. Missing evidence is not zero.', '']
    case_by_id = {case['case_id']: case for case in selected}
    partitions = {}
    for row in rows:
        job = case_by_id[row['case_id']]['job']
        key = (row['size_bytes'], row['measurement_class'], json.dumps(row['cohort'], sort_keys=True), _workload(job))
        partitions.setdefault(key, []).append(row)
    for (size, measurement_class, cohort, workload), subset in sorted(partitions.items()):
        lines += [f'## {size} bytes — {measurement_class} — cohort {cohort}', '',
                  f'Workload identity: `{json.dumps(workload)}`', '']
        arms = {}
        for row in subset:
            arm = (row['topology'], row['network'], row['durability'], row['routing'], row['route_or_mode'])
            arms.setdefault(arm, []).append(row)
        table = []
        for arm, items in sorted(arms.items()):
            label = '; '.join(str(part) for part in arm)
            table.append([label] + [_cell([row for row in items if row['concurrency'] == c]) for c in (1, 5, 16)])
        lines.append(
            'Caption: Writer counts are matched within each column, but other dimensions are unmatched: '
            'Native uses one or five Db instances on one SSD with no network hop; owner arms use one or '
            'five owner instances and SSDs with a network hop. Native per-put durability calls fsync on '
            'every registered Value-WAL file and the directory after every put; Native grouped durability '
            'makes the same calls shared by callers in one run. Owner synchronous durability uses one '
            'covering fdatasync of one owner file per group, with the directory synced once per file. '
            'Page-cache acknowledgements have no barrier. Each Native Db has a large index table of '
            '16x64 cells per instance; owners use append-only owner history plus a coordinator index. '
            'Native routes by key hash; owners route by operation ID.'
        )
        lines += focused_analysis._md_table(('Arm: topology; network; durability; routing; mode', '1 writer', '5 writers', '16 writers'), table)
        lines.append('')
        for row in subset:
            if row['group_stats_per_shard'] is not None:
                evidence = json.dumps(row['group_stats_per_shard'], sort_keys=True)
                lines.append(f"- {row['case_id']} group barrier statistics by shard: `{evidence}`")
        lines.append('')
    (output / 'fair_native.md').write_text('\n'.join(lines) + '\n')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', required=True)
    parser.add_argument('--report', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    generate(args.export, args.report, args.output)


if __name__ == '__main__':
    main()
