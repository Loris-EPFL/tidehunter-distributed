#!/usr/bin/env python3
"""Decision tables from admitted focused evidence, without pooling raw samples.

Read components use sampled coordinator resolution, messaging and reader stages;
device and handler differences retain their population and overlap caveats.
Write components use coordinator timeline endpoint intervals: front end,
owner queue, prepare, coalescing, exchange, publication and reply. Owner handler
and persist are inclusive envelope annotations; network is a duration difference.
Persist is not claimed to be physical SSD service. Missing measurements stay null.
"""
import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
import math


def device_samples(root, case, operation):
    path = root / 'cases' / case['case_id'] / 'result' / 'samples.csv'
    if case.get('status') != 'completed' or not path.is_file():
        return []
    with path.open(newline='') as stream:
        return [(int(r['dispatched_ns']), int(r['completed_ns']), int(r['api_ns']))
                for r in csv.DictReader(stream)
                if r['phase'] == 'measurement' and r['operation'] == operation
                and r['outcome'] in ('success', 'ok')]


def device_quantile(values, quantile):
    return sorted(values)[math.ceil(len(values) * quantile) - 1] / 1000 if values else None


def device_metrics(samples, case, operation):
    values = [r[2] for r in samples]
    result = {'success_count': len(values) if samples else None}
    for name, q in (('p50', .5), ('p99', .99), ('p999', .999)):
        value = device_quantile(values, q)
        result['api_' + name + '_us'] = value
        reported = latency(population(case, operation), 'api', name)
        if value is not None and reported is not None and value != reported:
            raise ValueError('raw quantile differs from comparison: ' + case['case_id'])
    count = population(case, operation).get('count')
    if samples and count is not None and count != len(samples):
        raise ValueError('raw population differs from comparison: ' + case['case_id'])
    span = max((r[1] for r in samples), default=0) - min((r[0] for r in samples), default=0)
    result['successful_calls_per_second'] = len(samples) * 1e9 / span if span > 0 else None
    return result


def device_contract(job):
    return {'route': route(job), 'size_bytes': job.get('value_size'),
            'concurrency': job.get('concurrency'), 'durability': job.get('durability_tier'),
            'coordinator_cache_bytes': job.get('coordinator_cache_bytes'),
            'cache_state': job.get('sealed_dataset', {}).get('page_cache_policy', 'normal'),
            'tier': job.get('sealed_dataset', {}).get('tier'),
            'key_order': job.get('key_order', job.get('distribution')),
            'dataset_keys': job.get('keys')}


def device_generate(export, report, output, cases, baseline_root):
    tables = defaultdict(list)
    read_rows, put_rows = [], []
    for c in cases:
        j = c['job']
        op = 'read' if j.get('group') == 'G10R' else 'put'
        samples = device_samples(export, c, op)
        v, reasons = verdict(c)
        if op == 'put' and v == 'missing' and c.get('status') == 'completed' and samples:
            v, reasons = 'qualified', []
        row = {**base(c), **device_contract(j), **device_metrics(samples, c, op),
               'group': j.get('group'), 'case_kind': j.get('case_kind'),
               'verdict': v, 'reason_codes': reasons}
        if op == 'read':
            evidence = c.get('focused_evidence', {}).get('media_evidence', {})
            path = export / 'cases' / c['case_id'] / 'media_evidence.json'
            if path.is_file():
                evidence = json.loads(path.read_text())
            before, after = evidence.get('history_loads_before'), evidence.get('history_loads_after')
            delta = after - before if before is not None and after is not None else None
            history_status = ('not applicable' if not row['route'].startswith('owner ') else
                              'missing' if delta is None else
                              'index or history pages read' if delta else 'no history loads')
            row.update(read_ops_per_success=evidence.get('read_ops_per_success'),
                       history_load_delta=delta, history_load_status=history_status)
            read_rows.append(row)
        elif j.get('measurement_operations') == 200000:
            buckets = defaultdict(list)
            span = 0
            if samples:
                start = min(x[0] for x in samples)
                span = max(x[1] for x in samples) - start
                for dispatch, completion, ns in samples:
                    buckets[(completion - start) // 1000000000].append(ns)
            seconds = []
            for second in range(max(buckets, default=-1) + 1):
                values = buckets[second]
                duration_ns = min(1000000000, max(0, span - second * 1000000000))
                complete = duration_ns == 1000000000
                seconds.append({**row, 'second': second, 'put_count': len(values),
                                'duration_ns': duration_ns,
                                'bucket_status': 'complete' if complete else 'partial',
                                'puts_per_second': len(values) if complete else None,
                                'p50_us': device_quantile(values, .5),
                                'p99_us': device_quantile(values, .99),
                                'max_us': max(values) / 1000 if values else None,
                                'slow_put_count': sum(x > 100000000 for x in values)})
            tables['sustained_seconds'].extend(seconds)
            total = sum(x[2] for x in samples)
            slow = sum(x[2] for x in samples if x[2] > 100000000)
            complete_seconds = [s for s in seconds if s['bucket_status'] == 'complete']
            last_five = complete_seconds[-5:] if len(complete_seconds) >= 5 else []
            last_rate = statistics.mean(s['puts_per_second'] for s in last_five) if last_five else None
            first_rate = complete_seconds[0]['puts_per_second'] if complete_seconds else None
            share = ratio(slow, total)
            if len(complete_seconds) < 20 or last_rate is None or not first_rate or share is None:
                regime = 'insufficient duration or samples'
            elif last_rate <= first_rate * .5 and share >= .4:
                regime = '[INFERENCE] throughput decline and slow worker time resemble G4; cause unexplained'
            else:
                regime = 'G4-like throughput decline and slow worker time not both observed'
            tables['sustained_summary'].append({**row, 'first_second': complete_seconds[0] if complete_seconds else None,
                'last_five_seconds': last_five, 'slow_worker_time_share': ratio(slow, total),
                'seconds_slow_count_multiple_of_concurrency': ratio(
                    sum(s['slow_put_count'] % j['concurrency'] == 0 for s in seconds), len(seconds)),
                'stall_regime': regime})
        else:
            put_rows.append(row)
        tables['device_cases'].append(row)
    tables['device_reads'] = read_rows
    tables['device_puts'] = put_rows
    for name, rows in (('device_read_spread', read_rows), ('device_put_spread', put_rows)):
        groups = defaultdict(list)
        for r in rows:
            groups[(r['route'], r['durability'], r['size_bytes'], r['concurrency'])].append(r)
        for group in groups.values():
            fields = ['api_p50_us', 'api_p99_us', 'api_p999_us', 'successful_calls_per_second']
            if name == 'device_read_spread':
                fields += ['read_ops_per_success', 'history_load_delta']
            for metric in fields:
                good = [r for r in group if r['verdict'] == 'qualified' and r['status'] == 'completed']
                tables[name].append({**device_contract(cases_by_id(cases, group[0]['case_id'])['job']),
                    'case_ids': [r['case_id'] for r in group], 'metric': metric,
                    'repetitions': {str(r['repeat']): r[metric] for r in good},
                    **summarize_spread([r[metric] for r in good])})
    for r in put_rows:
        if r['durability'] != 'synchronous':
            continue
        match = next((x for x in put_rows if x['durability'] == 'native' and
                      all(x[k] == r[k] for k in ('route', 'size_bytes', 'concurrency', 'repeat'))), None)
        qualified = (match is not None and r['status'] == match['status'] == 'completed'
                     and r['verdict'] == match['verdict'] == 'qualified')
        tables['put_durability_ratio'].append({**r, 'case_ids': r['case_ids'] + (match['case_ids'] if match else []),
            'native_case_id': match['case_id'] if match else None,
            'native_verdict': match['verdict'] if match else 'missing',
            'native_reason_codes': match['reason_codes'] if match else ['artifact_missing'],
            'p50_ratio': ratio(r['api_p50_us'], match['api_p50_us']) if qualified else None,
            'p99_ratio': ratio(r['api_p99_us'], match['api_p99_us']) if qualified else None,
            'contract': 'synchronous: covering fsync; native: page-cache append'})
    ratio_groups = defaultdict(list)
    for row in tables['put_durability_ratio']:
        ratio_groups[(row['route'], row['size_bytes'], row['concurrency'])].append(row)
    for group in ratio_groups.values():
        for metric in ('p50_ratio', 'p99_ratio'):
            tables['put_durability_ratio_spread'].append({
                **device_contract(cases_by_id(cases, group[0]['case_id'])['job']),
                'case_ids': sorted({x for row in group for x in row['case_ids']}),
                'metric': metric, 'contract': 'synchronous: covering fsync; native: page-cache append',
                'repetitions': {str(row['repeat']): row[metric] for row in group},
                **summarize_spread([row[metric] for row in group])})
    if baseline_root:
        baseline = json.loads((Path(baseline_root) / 'comparison.json').read_text())['cases']
        for r in read_rows:
            if r['concurrency'] != 1:
                continue
            hybrid = r['route'].startswith('hybrid')
            match = next((c for c in baseline if c['job'].get('group') == 'G1' and
                          route(c['job']) == r['route'] and c['job'].get('value_size') == r['size_bytes'] and
                          c['job'].get('concurrency') == 1 and c['job'].get('repeat') == r['repeat'] and
                          c['job'].get('sealed_dataset', {}).get('tier') == ('first_touch' if hybrid else 'repeat_read')), None)
            if not match:
                continue
            bv, br = verdict(match)
            old = metrics(match)['api_p50_us']
            tables['g1_comparison'].append({**r, 'case_ids': r['case_ids'] + [match['case_id']],
                'baseline_case_id': match['case_id'], 'baseline_verdict': bv, 'baseline_reason_codes': br,
                'baseline_p50_us': old, 'p50_ratio': ratio(r['api_p50_us'], old) if r['verdict'] == bv == 'qualified' else None,
                'baseline_contract': '4096 keys; ascending sweep; ' + ('cold gate' if hybrid else 'resident repeat_read'),
                'interpretation': '[INFERENCE] SSD extra cost' if not hybrid else 'different key order and dataset size'})
        groups = defaultdict(list)
        for r in tables['g1_comparison']:
            groups[(r['route'], r['size_bytes'])].append(r)
        for group in groups.values():
            tables['g1_spread'].append({**device_contract(cases_by_id(cases, group[0]['case_id'])['job']),
                'case_ids': sorted({x for r in group for x in r['case_ids']}),
                'repetitions': {str(r['repeat']): r['p50_ratio'] for r in group},
                **summarize_spread([r['p50_ratio'] for r in group])})
    for design in ('owner verbs', 'owner TCP', 'hybrid kernel DIRECT', 'hybrid SPDK'):
        row = {'route': design, 'case_ids': [], 'read_contract': 'cold gate; random with replacement; coordinator cache 0; qualified only',
               'write_contract': 'synchronous: covering fsync; native: page-cache append; 4096 keys'}
        for size, concurrency in ((1024, 1), (8192, 1), (65536, 1), (1024, 16), (65536, 16)):
            matched = [r for r in read_rows if r['route'] == design and r['size_bytes'] == size and r['concurrency'] == concurrency and r['verdict'] == 'qualified' and r['status'] == 'completed']
            row['case_ids'].extend(r['case_id'] for r in matched)
            for metric in ('api_p50_us', 'api_p99_us'):
                row[f'read_{size}_c{concurrency}_{metric}'] = summarize_spread([r[metric] for r in matched])['median']
            row[f'read_{size}_c{concurrency}_dataset_keys'] = matched[0]['dataset_keys'] if matched else None
            row[f'read_{size}_c{concurrency}_history'] = {
                str(r['repeat']): {'case_id': r['case_id'], 'delta': r['history_load_delta'],
                                   'status': r['history_load_status'], 'verdict': r['verdict']}
                for r in matched}
            row['case_ids'] = sorted(set(row['case_ids']) | {item['case_id'] for item in row[f'read_{size}_c{concurrency}_history'].values()})
        for tier in ('synchronous', 'native'):
            for size in (1024, 65536):
                for concurrency in (1, 16):
                    prefix = f'put_{tier}_{size}_c{concurrency}'
                    if design.startswith('hybrid'):
                        row[prefix] = 'owner verbs write path'
                    else:
                        matched = [r for r in put_rows if r['route'] == design and r['durability'] == tier and r['size_bytes'] == size and r['concurrency'] == concurrency and r['status'] == 'completed' and r['verdict'] == 'qualified']
                        row['case_ids'].extend(r['case_id'] for r in matched)
                        for metric in ('api_p50_us', 'api_p99_us'):
                            row[prefix + '_' + metric] = summarize_spread([r[metric] for r in matched])['median']
        tables['decision'].append(row)
    coverage = defaultdict(list)
    for r in tables['device_cases']:
        coverage[(r['group'], r['case_kind'], r['route'], r['verdict'])].append(r)
        if r['status'] == 'failed':
            tables['failures'].append(r)
        if r['verdict'] == 'unqualified':
            tables['unqualified'].append(r)
    for (group, kind, design, v), rows in sorted(coverage.items()):
        tables['coverage'].append({'group': group, 'case_kind': kind, 'route': design,
            'verdict': v, 'count': len(rows), 'case_ids': [r['case_id'] for r in rows],
            'reason_codes': sorted({x for r in rows for x in r['reason_codes']})})
    for name in ('g1_comparison', 'g1_spread', 'failures', 'unqualified', 'sustained_seconds', 'sustained_summary'):
        tables[name]
    output.mkdir(parents=True, exist_ok=True)
    sections = []
    for name, rows in tables.items():
        rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
        fields = list(dict.fromkeys(k for row in rows for k in row)) or ['case_ids']
        def text(value):
            if value is None: return ''
            if isinstance(value, (dict, list)): return json.dumps(value, sort_keys=True, separators=(',', ':'))
            return str(value)
        with (output / (name + '.csv')).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows({k: text(r.get(k)) for k in fields} for r in rows)
        lines = ['# ' + name.replace('_', ' ').title(), '', '| ' + ' | '.join(fields) + ' |',
                 '| ' + ' | '.join('---' for _ in fields) + ' |']
        lines.extend('| ' + ' | '.join(text(r.get(k)).replace('|', '\\|').replace('\n', ' ') for k in fields) + ' |' for r in rows)
        content = '\n'.join(lines) + '\n'
        (output / (name + '.md')).write_text(content)
        sections.append(content)
    summary = {'schema_version': 1, 'planned_cases': len(cases),
               'completed_cases': sum(c.get('status') == 'completed' for c in cases),
               'complete': all(c.get('status') == 'completed' for c in cases), 'tables': dict(tables)}
    (output / 'focused-summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + '\n')
    preface = ('# Device campaign report\n\nSuccess-only exact nearest-rank API quantiles use measurement samples in each case. '
               'Repetitions are not pooled. Min, median and max are descriptive spreads, not confidence intervals. '
               'The p99 and p99.9 from 2,048 or 4,096 calls are descriptive; see success_count for each case. '
               'Cold qualification requires device evidence. '
               'A non-zero history load delta means index or history pages were read. '
               'Missing values are empty. Unqualified cases do not drive the decision table.\n\n'
               '## Surprises and open questions\n\nThe sustained stall regime remains unexplained. '
               'For the G4 comparison, the descriptive flag requires at least 20 complete seconds, '
               'a last-five complete-second mean rate at most half the first complete-second rate, and at least 40% slow worker time. '
               'This flag does not prove that all workers stall together. The multiple-of-concurrency '
               'fraction includes seconds with zero slow puts and partial buckets. '
               'Do not infer a cause from these tables.\n\n')
    (output / 'REPORT.md').write_text(preface + '\n'.join(sections))
    return summary


def cases_by_id(cases, case_id):
    return next(c for c in cases if c['case_id'] == case_id)


def summarize_spread(values):
    values = [x for x in values if x is not None]
    return {'min': min(values), 'median': statistics.median(values), 'max': max(values)} if values else {'min': None, 'median': None, 'max': None}


def ratio(a, b):
    return a / b if a is not None and b not in (None, 0) else None


def route(job):
    backend = job.get('backend')
    if backend == 'tidehunter_verbs_native':
        return 'owner verbs warm-history' if job.get('media', {}).get('history') == 'prewarm' else 'owner verbs'
    if backend == 'tidehunter_tcp':
        return 'owner TCP'
    if backend == 'tidehunter_nvme_rdma':
        return 'hybrid SPDK' if job.get('nvmeof_target') == 'spdk' else 'hybrid kernel DIRECT' if job.get('target_io') == 'direct' else 'hybrid kernel buffered'
    return backend


def population(case, operation, stream=None):
    if case.get('status') != 'completed':
        return {}
    matches = [p for p in case.get('metrics', {}).get('populations', []) if p.get('operation') == operation and p.get('outcome') in ('success', 'ok') and (stream is None or p.get('stream') == stream)]
    if len(matches) > 1:
        raise ValueError('ambiguous success population: ' + case['case_id'])
    return matches[0] if matches else {}


def latency(pop, kind, stat):
    value = pop.get(kind + '_ns', {}).get(stat)
    return value / 1000 if value is not None else None


def base(case):
    j = case['job']
    return {'case_ids': [case['case_id']], 'case_id': case['case_id'], 'status': case.get('status'), 'route': route(j), 'repeat': j.get('repeat'), 'size_bytes': j.get('value_size'), 'concurrency': j.get('concurrency'), 'tier': j.get('sealed_dataset', {}).get('tier'), 'cache_state': j.get('sealed_dataset', {}).get('target_cache'), 'page_cache_policy': j.get('sealed_dataset', {}).get('page_cache_policy'), 'coordinator_cache_bytes': j.get('coordinator_cache_bytes'), 'durability': j.get('durability_tier')}


def floor_base(case):
    return {'case_ids': [case['case_id']], 'case_id': case['case_id'],
            'status': case.get('status'), 'repeat': case['job'].get('repeat')}


def verdict(case):
    evidence = case.get('focused_evidence', {})
    history = case.get('history_prewarm_qualification')
    qualifications = [history, case.get('interference_route_qualification'),
                      case.get('sealed_read_accounting'), case.get('target_io_qualification'),
                      case.get('hybrid_put_qualification')]
    if history is None:
        qualifications.append(case.get('media_qualification'))
    for name in ('media_evidence', 'fio_result', 'ssd_floor_result', 'block_trace_evidence', 'interference_result'):
        e = evidence.get(name)
        if e is not None and not (name == 'media_evidence' and history is not None):
            q = e.get('qualification', e)
            qualifications.append(q)
            if name in ('fio_result', 'ssd_floor_result'):
                qualifications.append(q.get('queue_depth'))
    statuses, reasons = [], set()
    for q in qualifications:
        if not isinstance(q, dict):
            continue
        status = q.get('status')
        if status not in ('qualified', 'unqualified'):
            status = 'qualified' if q.get('qualified') is True else 'unqualified' if q.get('qualified') is False else None
        if status:
            statuses.append(status)
            if status == 'unqualified':
                reasons.update(q.get('reason_codes', q.get('reasons', [])))
    if 'unqualified' in statuses:
        return 'unqualified', sorted(reasons)
    if 'qualified' in statuses:
        return 'qualified', []
    return 'missing', ['artifact_missing']


def ssd_log_metrics(path, region_bytes, runtime_seconds):
    """Use the optimal two-cluster split in log space and its geometric gap.

    Time quarters cover the requested runtime from fio's zero timestamp; offset
    deciles cover the requested region. Empty buckets have no share.
    """
    samples = []
    with path.open(newline='') as stream:
        for row in csv.reader(stream):
            time_ms, latency_ns, direction, block_bytes, offset = map(int, row[:5])
            if latency_ns <= 0 or direction != 0 or not 0 <= offset < region_bytes:
                raise ValueError('invalid SSD latency log row')
            samples.append((time_ms, latency_ns, offset))
    if not samples:
        return {'n': 0, 'mean_us': None, 'p50_us': None, 'fast_share': None,
                'fast_mean_us': None, 'slow_mean_us': None,
                'fast_share_by_time_quarter': None, 'fast_share_by_offset_decile': None}
    ordered = sorted(samples, key=lambda x: x[1])
    origin = math.log(ordered[0][1])
    logs = [math.log(s[1]) - origin for s in ordered]
    total, total_sq = sum(logs), sum(x * x for x in logs)
    left = left_sq = 0.0
    best, split = None, None
    for i in range(1, len(logs)):
        x = logs[i - 1]
        left += x
        left_sq += x * x
        if ordered[i - 1][1] == ordered[i][1]:
            continue
        right = total - left
        cost = left_sq - left * left / i + (total_sq - left_sq) - right * right / (len(logs) - i)
        if best is None or cost < best:
            best, split = cost, i
    values = [s[1] for s in ordered]
    result = {'n': len(values), 'mean_us': statistics.mean(values) / 1000,
              'p50_us': device_quantile(values, .5), 'fast_share': None,
              'fast_mean_us': None, 'slow_mean_us': None,
              'fast_share_by_time_quarter': None, 'fast_share_by_offset_decile': None}
    if split is None:
        return result
    fast_mean = statistics.mean(values[:split])
    slow_mean = statistics.mean(values[split:])
    if slow_mean / fast_mean < 1.3:
        return result
    # A geometric midpoint separates the clusters in log space.
    threshold = math.sqrt(values[split - 1]) * math.sqrt(values[split])
    counts = [[0, 0] for _ in range(4)]
    offsets = [[0, 0] for _ in range(10)]
    for time_ms, latency_ns, offset in samples:
        quarter = min(3, max(0, time_ms * 4 // (runtime_seconds * 1000)))
        decile = min(9, offset * 10 // region_bytes)
        for bucket in (counts[quarter], offsets[decile]):
            bucket[1] += 1
            bucket[0] += latency_ns < threshold
    result.update(fast_share=split / len(values), fast_mean_us=fast_mean / 1000,
                  slow_mean_us=slow_mean / 1000,
                  fast_share_by_time_quarter=[ratio(*b) for b in counts],
                  fast_share_by_offset_decile=[ratio(*b) for b in offsets])
    return result


def metrics(case, operation='read', stream=None):
    p = population(case, operation, stream)
    return {'api_p50_us': latency(p, 'api', 'p50'), 'api_p99_us': latency(p, 'api', 'p99'), 'api_p999_us': latency(p, 'api', 'p999'), 'response_p99_us': latency(p, 'response', 'p99'), 'success_count': p.get('count'), 'successful_calls_per_second': case.get('metrics', {}).get('success_calls_per_second') if case.get('status') == 'completed' else None}


def stages_route(job):
    if job.get('backend') == 'tidehunter_verbs':
        return 'verbs mTLS'
    return route(job)


def stages_contract(job):
    operation = next((name for name, weight in job.get('mix', {}).items() if weight), None)
    return {'route': stages_route(job), 'case_kind': job.get('case_kind'),
            'group': job.get('group'), 'shape': operation, 'size_bytes': job.get('value_size'),
            'concurrency': job.get('concurrency'), 'durability': job.get('durability_tier'),
            'native_durability': job.get('native_durability') if job.get('backend') == 'native' else None,
            'key_policy': job.get('put_key_policy', 'overwrite'),
            'cache_state': job.get('media', {}).get('expected', 'memory' if operation == 'read' else None),
            'key_order': job.get('key_order', job.get('distribution')),
            'dataset_keys': job.get('keys'), 'target_io': job.get('target_io'),
            'diagnostics': job.get('diagnostics', {})}


def stages_ops(export, cases):
    groups = defaultdict(list)
    for case in cases:
        job = case['job']
        if not job.get('group', '').startswith('G12'):
            continue
        contract = stages_contract(job)
        operation = contract['shape']
        samples = device_samples(export, case, operation)
        values = [s[2] for s in samples]
        v, reasons = verdict(case)
        if job['case_kind'] == 'hybrid_put' and not isinstance(case.get('hybrid_put_qualification'), dict):
            v, reasons = 'missing', ['artifact_missing']
        if operation != 'read' and job['case_kind'] != 'hybrid_put' and v == 'missing' and case.get('status') == 'completed' and samples:
            v, reasons = 'qualified', []
        if operation == 'read' and job['case_kind'] == 'native_op' and case.get('status') == 'completed' and samples:
            v, reasons = 'qualified', []
        measured = {'case_id': case['case_id'], 'repeat': job.get('repeat'),
                    'status': case.get('status'), 'verdict': v, 'reason_codes': reasons,
                    'n': len(values) if samples else None,
                    'mean_us': statistics.mean(values) / 1000 if values else None,
                    **{name + '_us': device_quantile(values, q) for name, q in
                       (('p50', .5), ('p99', .99), ('p999', .999))},
                    'throughput': device_metrics(samples, case, operation)['successful_calls_per_second'],
                    'device_reads_per_call': case.get('focused_evidence', {}).get('media_evidence', {}).get('read_ops_per_success')}
        groups[json.dumps(contract, sort_keys=True)].append(measured)
    rows = []
    for key, repetitions in sorted(groups.items()):
        contract = json.loads(key)
        eligible = [r for r in repetitions if r['status'] == 'completed' and r['verdict'] == 'qualified']
        row = {**contract, 'case_ids': sorted(r['case_id'] for r in repetitions),
               'repetitions': sorted(repetitions, key=lambda r: (r['repeat'], r['case_id'])),
               'n': {str(r['repeat']): r['n'] for r in repetitions},
               'repetition_count': len(repetitions), 'qualified_repetitions': len(eligible),
               'verdicts': [r['verdict'] for r in repetitions]}
        for name in ('mean_us', 'p50_us', 'p99_us', 'p999_us', 'throughput', 'device_reads_per_call'):
            values = [r[name] for r in eligible if r[name] is not None]
            row[name] = statistics.median(values) if values else None
        row['p50_range_us'] = ('range n/a (1 repetition)' if len(repetitions) == 1 else
                               summarize_spread([r['p50_us'] for r in eligible]))
        rows.append(row)
    return rows


def stages_headline_match(r, design, size, concurrency, label):
    return ((r['route'] == design or (design == 'hybrid kernel DIRECT' and label == 'cache_read'
                                     and r['route'] == 'hybrid kernel buffered'))
            and r['size_bytes'] == size and r['concurrency'] == concurrency
            and r['key_policy'] == 'overwrite'
            and r['shape'] == ('read' if label.endswith('read') else 'put')
            and (r['cache_state'] == ('memory' if label == 'cache_read' else 'cold')
                 if label.endswith('read') else
                 r['durability'] == (('page_cache' if design == 'native' else 'native')
                                     if label == 'page_cache_write' else
                                     'value_wal_barrier' if design == 'native' else 'synchronous'))
            and r['group'] != 'G12S')


def stages_apples(rows):
    routes = ('native', 'owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel DIRECT', 'hybrid SPDK')
    output = []
    for design in routes:
        for size in (1024, 8192, 65536):
            for concurrency in (1, 16):
                result = {'route': design, 'size_bytes': size, 'concurrency': concurrency, 'case_ids': []}
                for label in ('cache_read', 'disk_read', 'durable_write', 'page_cache_write'):
                    candidates = [r for r in rows if stages_headline_match(r, design, size, concurrency, label)]
                    # Do not let an unrun or unqualified baseline hide a measured copy.
                    candidates = [r for r in candidates if r['qualified_repetitions'] and r['p50_us'] is not None]
                    candidates.sort(key=lambda r: (bool(r['diagnostics']), r['group']))
                    selected = candidates[0] if candidates else None
                    result[label] = ({**{k: selected[k] for k in ('p50_us', 'p99_us', 'p999_us', 'p50_range_us', 'n', 'verdicts', 'case_ids', 'diagnostics', 'durability', 'cache_state', 'target_io')},
                                      'source': 'profiled copy' if selected['diagnostics'] else 'unprofiled baseline'}
                                     if selected else 'not measured')
                    if selected:
                        result['case_ids'].extend(selected['case_ids'])
                output.append(result)
    return output



def stages_decompositions(export, cases, floor_tables):
    """Aggregate exact adjacent intervals; only individual intervals are exact_interval.

    An aggregate_of_exact_intervals row summarizes multiple retained calls.
    """
    import decomposition

    write_rows, read_rows, stalls = [], [], []
    for case in cases:
        job = case['job']
        if not job.get('group', '').startswith('G12') or case.get('status') != 'completed':
            continue
        diagnostics = job.get('diagnostics', {})
        if not diagnostics and job.get('case_kind') != 'native_media_read':
            if job.get('backend') == 'native' and job.get('mix', {}).get('put_batch'):
                write_rows.append({**floor_base(case), **stages_contract(job), 'stage': 'native_put_batch',
                                   'exactness': 'sampled_mean', 'mean_us': None, 'count': 0,
                                   'selection': 'unsupported: native batch writes are not profiled'})
            continue
        directory = export / 'cases' / case['case_id']
        result = directory / 'result'
        contract = {**floor_base(case), **stages_contract(job)}
        sample_path = result / 'samples.csv'
        if sample_path.is_file():
            with sample_path.open(newline='') as stream:
                measured_samples = [r for r in csv.DictReader(stream) if r['phase'] == 'measurement'
                                    and r['operation'] == contract['shape']]
                samples = [r for r in measured_samples if r['outcome'] in ('success', 'ok')]
        else:
            measured_samples = samples = []
        def load(path):
            return json.loads(path.read_text()) if path.is_file() else None
        envelope = load(result / 'write-timeline.json') or {}
        timeline = envelope.get('timeline') or {}
        intervals = decomposition.write_timeline(timeline)
        if diagnostics.get('write_timeline_limit'):
            by_id = {str(s['caller_sample_id']): int(s['api_ns']) / 1000 for s in samples if s.get('caller_sample_id')}
            joined = [r for r in intervals if str(r['caller_sample_id']) in by_id]
            matched_ids = {str(r['caller_sample_id']) for r in joined}
            by_id = {key: value for key, value in by_id.items() if key in matched_ids}
            api = statistics.mean(by_id.values()) if by_id else None
            grouped = defaultdict(list)
            per_call = defaultdict(float)
            for row in joined:
                grouped[(row['start_stage'], row['end_stage'])].append(row)
                per_call[str(row['caller_sample_id'])] += row['mean_us']
            for (start, end), entries in sorted(grouped.items()):
                values = [entry['mean_us'] for entry in entries]
                contribution = sum(values) / len(by_id) if by_id else None
                write_rows.append({**contract, 'stage': start + ' → ' + end, 'exactness': 'aggregate_of_exact_intervals',
                    'count': len(values), 'mean_us': statistics.mean(values),
                    'p50_us': device_quantile([entry['end_ns'] - entry['start_ns'] for entry in entries], .5),
                    'share_of_api_mean': ratio(contribution, api),
                    'intervals': [{'caller_sample_id': r['caller_sample_id'], 'start_ns': r['start_ns'],
                                   'end_ns': r['end_ns'], 'duration_us': r['mean_us'],
                                   'exactness': 'exact_interval'} for r in entries],
                    'selection': 'retained coordinator call events', 'overlap': 'adjacent coordinator events',
                    'dropped_calls': timeline.get('dropped_calls'), 'dropped_events': timeline.get('dropped_events')})
            for row in decomposition.owner_timeline(timeline):
                write_rows.append({**contract, **row})
            residuals = [by_id[key] - total for key, total in per_call.items()]
            write_rows.append({**contract, 'stage': 'unattributed', 'exactness': 'residual',
                'count': len(residuals), 'mean_us': statistics.mean(residuals) if residuals else None,
                'p50_us': device_quantile([value * 1000 for value in residuals], .5),
                'selection': 'matched retained calls only', 'overlap': 'API minus adjacent coordinator intervals'})
        if diagnostics.get('write_profile_limit'):
            profile = (load(result / 'write-diagnostics.json') or {}).get('profile')
            profile_rows = decomposition.write_profile(profile) if profile else []
            for row in profile_rows:
                write_rows.append({**contract, **row})
            if profile and profile.get('runtime_observation_mode') == 'runtime_selected_native_scalar_write_stages':
                selected = profile.get('selected_operations')
                leaves = [r for r in profile_rows if r['stage'] in ('wal_append', 'index_apply')]
                persistence = profile.get('value_wal_persistence', {})
                valid = (selected and selected == len(measured_samples) == len(samples)
                         and len(leaves) == 2 and all(r['count'] == selected and r['mean_us'] is not None for r in leaves))
                if (persistence.get('status') not in ('sampled', 'not_applicable') or
                        (persistence.get('status') == 'sampled' and persistence.get('file_ns') is None)):
                    valid = False
                barrier = persistence['file_ns'] / selected / 1000 if valid and persistence.get('status') == 'sampled' else 0
                api_mean = statistics.mean(int(s['api_ns']) for s in samples) / 1000 if valid else None
                write_rows.append({**contract, 'stage': 'unattributed', 'exactness': 'derived_duration_difference',
                    'count': selected if valid else 0,
                    'mean_us': api_mean - sum(r['mean_us'] for r in leaves) - barrier if valid else None,
                    'selection': ('full measured population; API mean minus sampled native leaves' if valid else
                                  'not computed: selection differs from the measured population or a leaf is unavailable'),
                    'overlap': 'same-case duration means; barrier file time per selected operation'})
        if diagnostics.get('read_profile_limit') or job.get('case_kind') == 'native_media_read':
            profile = (load(result / 'read-diagnostics.json') or {}).get('profile')
            profile_rows = decomposition.read_profile(profile) if profile else []
            for row in profile_rows:
                read_rows.append({**contract, **row})
            stages = {r['stage']: r for r in profile_rows}
            if job.get('backend') in ('tidehunter_verbs_native', 'tidehunter_verbs', 'tidehunter_tcp'):
                exchange, handler = stages.get('exchange', {}), stages.get('server_handler', {})
                selected = profile.get('selected_operations') if profile else None
                valid = (selected and selected == exchange.get('count') == handler.get('count')
                         and exchange.get('mean_us') is not None and handler.get('mean_us') is not None)
                read_rows.append({**contract, 'stage': 'exchange minus server_handler (wire, both stacks, TLS, dispatch)',
                    'mean_us': exchange['mean_us'] - handler['mean_us'] if valid else None,
                    'count': selected if valid else 0, 'exactness': 'derived_mean_difference',
                    'selection': 'equal counts; population identity not proven; includes client messaging plus wire and stacks'
                                 if valid else 'not computed: unequal or missing counts',
                    'overlap': 'exchange minus handler includes nested encode, decode, verification, wire and stacks'})
            evidence = load(result / 'measurement-read-evidence.json')
            readers = decomposition.reader_stages(evidence) if evidence else []
            for row in readers:
                read_rows.append({**contract, **row})
            before, after = load(directory / 'measurement-before.json'), load(directory / 'measurement-end.json')
            media = case.get('focused_evidence', {}).get('media_evidence', {})
            devices = media.get('owner_devices', []) or media.get('client_devices', [])
            device = None
            if before and after and devices:
                device = decomposition.kernel_device(before, after, devices)
                read_rows.append({**contract, 'stage': 'client_device' if media.get('client_devices') else 'owner_block_layer',
                                  **device})
                if device.get('count') and len(samples):
                    ops_per_read = device['count'] / len(samples)
                    avg_kib = device['bytes'] / device['count'] / 1024
                    read_rows.append({**contract, 'stage': 'device time per device op (upper bound, includes non-API reads)',
                        'mean_us': device['mean_us'], 'count': device['count'], 'exactness': 'device_counter',
                        'selection': f'annotation only; ops per read {ops_per_read:.3f}; average {avg_kib:.3f} KiB per device read',
                        'overlap': 'not additive; block-layer counters may include non-API reads'})
            start, end = load(directory / 'spdk-target-start.json'), load(directory / 'spdk-target-end.json')
            if start and end:
                device = decomposition.spdk_bdev(start, end)
                read_rows.append({**contract, 'stage': 'spdk_bdev', **device})
            if job.get('backend') == 'tidehunter_nvme_rdma':
                submit = next((r for r in readers if r['stage'] == 'submit_to_cq'), {})
                valid = (device and device.get('mean_us') is not None and submit.get('mean_us') is not None
                         and len(samples) and (start and end or device['count'] / len(samples) <= 1.05))
                read_rows.append({**contract, 'stage': 'fabric and stack (reader submit_to_cq minus device service)',
                    'mean_us': submit['mean_us'] - device['mean_us'] * device['count'] / len(samples) if valid else None,
                    'count': submit.get('count') if valid else 0, 'exactness': 'derived_mean_difference',
                    'selection': 'same measurement window; populations may differ' if valid else 'not computed: missing or contaminated device evidence',
                    'overlap': 'mean difference; includes NVMe-oF stack and target path'})
            population_complete = (bool(samples) and len(samples) == len(measured_samples)
                                   and len(samples) == job.get('measurement_operations'))
            measured_count = len(samples) if population_complete else 0
            api_mean = statistics.mean(int(row['api_ns']) for row in samples) / 1000 if population_complete else None
            residual_route = ('native' if job.get('backend') == 'native' else
                              'hybrid' if job.get('backend') == 'tidehunter_nvme_rdma' else 'owner')
            for row in decomposition.read_residuals(profile, readers, device, api_mean, measured_count,
                                                    residual_route, job.get('media', {}).get('expected') == 'cold'):
                read_rows.append({**contract, **row})
        if job.get('group') == 'G12S':
            stalls.append({**contract, **decomposition.stall(samples, timeline)})
    for kind in ('net_floor', 'ssd_floor'):
        for floor in floor_tables.get(kind, []):
            if floor.get('status') == 'completed' and floor.get('p50_us') is not None:
                annotation = {**floor, 'stage': kind, 'mean_us': floor.get('mean_us', floor.get('avg_us')), 'exactness': 'floor_derived',
                              'selection': 'separate floor case; not a component measurement',
                              'overlap': 'bound only; not additive'}
                write_rows.append(annotation)
                read_rows.append(annotation)
    paired = {}
    for row in stalls:
        mode = 'timeline' if row['diagnostics'].get('write_timeline_limit') else 'profile'
        pair = paired.setdefault(mode, {'case_ids': [], 'mode': mode, 'overwrite': 'not measured', 'new': 'not measured'})
        pair['case_ids'].extend(row['case_ids'])
        pair[row['key_policy']] = row
    return write_rows, read_rows, list(paired.values())


def stages_transport(ops, writes, reads, floors):
    """Keep each case and diagnostic population separate; missing is never zero."""
    indexed = defaultdict(dict)
    for row in writes + reads:
        if row.get('case_id') and row.get('stage'):
            indexed[row['case_id']][row['stage']] = row
    result = []
    for op in ops:
        for repetition in op['repetitions']:
            cid, operation = repetition['case_id'], op['shape']
            stages = indexed[cid]
            read = operation == 'read'
            hybrid = op['route'].startswith('hybrid')
            if read and hybrid:
                service_name = 'spdk_bdev' if op['route'] == 'hybrid SPDK' else 'owner_block_layer'
                derived_name = 'fabric and stack (reader submit_to_cq minus device service)'
            elif read:
                service_name = 'server_handler'
                derived_name = 'exchange minus server_handler (wire, both stacks, TLS, dispatch)'
            else:
                service_name = 'owner_handler_ns'
                derived_name = 'exchange minus owner handler (wire, both network stacks, TLS, dispatch)'
            service, derived = stages.get(service_name, {}), stages.get(derived_name, {})
            api = repetition['mean_us'] if repetition['status'] == 'completed' and repetition['verdict'] == 'qualified' else None
            service_value = service.get('mean_us') if api is not None else None
            transport = derived.get('mean_us') if api is not None else None
            floor_tool = ('tcp_rtt' if op['route'] == 'owner TCP' else
                          'ib_read_lat' if read and hybrid else
                          'ib_send_lat' if op['route'] in ('owner verbs', 'verbs mTLS') or hybrid else None)
            candidates = [r for r in floors if floor_tool and r.get('tool') == floor_tool
                          and r.get('size_bytes') == op['size_bytes'] and r.get('repeat') == repetition['repeat']
                          and r.get('verdict') == 'qualified' and r.get('round_trip_p50_us') is not None
                          and ((floor_tool == 'tcp_rtt' and r.get('interface') == 'eth0' and r.get('mode') == 'block')
                               or (floor_tool != 'tcp_rtt' and r.get('mode') == 'polled'))]
            floor = candidates[0] if candidates else {}
            missing = []
            if api is None: missing.append('qualified API samples')
            if service_value is None: missing.append(service_name)
            if transport is None: missing.append(derived_name)
            result.append({**{k: op[k] for k in ('route', 'case_kind', 'group', 'shape', 'size_bytes',
                'concurrency', 'durability', 'native_durability', 'key_policy', 'cache_state', 'target_io', 'diagnostics')},
                'case_ids': [cid], 'case_id': cid, 'repeat': repetition['repeat'],
                'api_mean_us': api, 'api_exactness': 'sampled_mean' if api is not None else 'not computed',
                'service_us': service_value, 'service_source': service_name,
                'service_exactness': service.get('exactness') if service_value is not None else 'not computed',
                'transport_us': transport, 'transport_exactness': derived.get('exactness') if transport is not None else 'not computed',
                'transport_definition': ('client messaging plus wire, stacks, TLS and dispatch' if read and not hybrid
                                         else 'fabric, stacks and target path' if read else 'wire, stacks, TLS and dispatch'),
                'share_of_api_mean': ratio(transport, api), 'floor_p50_us': floor.get('round_trip_p50_us'),
                'floor_exactness': 'floor_derived' if floor else 'not computed', 'floor_case_id': floor.get('case_id'),
                'floor_tool': floor.get('tool'), 'floor_interface': floor.get('interface'),
                'interpretation': derived.get('selection'), 'status': 'not computed: ' + ', '.join(missing) if missing else 'computed'})
    return result

COMPONENT_NAMES = ('client coordinator', 'client messaging (encode, decode, verify)',
                   'client queueing and wake-up', 'network (wire, both stacks, TLS, dispatch)',
                   'owner software', 'ssd device', 'waiting and coalescing',
                   'durable wait and publication', 'residual')
OWNER_PERSIST = 'owner persist (file sync, includes software)'
EXCHANGE_UNSEPARATED = 'exchange (owner and network not separable)'
DEVICE_UNSEPARATED = 'device + fabric + stack (submit_to_cq, not separable)'


def component_names(shape):
    return tuple(OWNER_PERSIST if shape != 'read' and name == 'ssd device' else name
                 for name in COMPONENT_NAMES) + (EXCHANGE_UNSEPARATED, DEVICE_UNSEPARATED)


def _write_component_intervals(export, case_id):
    """Sum same-call endpoint intervals; incomplete or unjoined calls are excluded."""
    directory = export / 'cases' / case_id / 'result'
    path, samples_path = directory / 'write-timeline.json', directory / 'samples.csv'
    if not path.is_file() or not samples_path.is_file():
        return {}
    timeline = json.loads(path.read_text()).get('timeline') or {}
    with samples_path.open(newline='') as stream:
        ids = {str(row['caller_sample_id']) for row in csv.DictReader(stream)
               if row.get('phase') == 'measurement' and row.get('outcome') in ('success', 'ok')
               and row.get('caller_sample_id')}
    groups = {'client coordinator': (('caller_start', 'owner_enqueue_started'),
                                     ('owner_dequeued', 'admitted'), ('published', 'caller_finished')),
              'client queueing and wake-up': (('owner_enqueue_started', 'owner_dequeued'),),
              'waiting and coalescing': (('admitted', 'tx_queued'),),
              'durable wait and publication': (('completion_harvested', 'published'),)}
    collected = defaultdict(list)
    for record in timeline.get('records', []):
        if str(record.get('caller_sample_id')) not in ids:
            continue
        stamps = defaultdict(list)
        for event in record.get('events', []):
            stamps[event['stage']].append(event['elapsed_ns'])
        for component, pairs in groups.items():
            if all(len(stamps[a]) == len(stamps[b]) == 1 and stamps[b][0] >= stamps[a][0] for a, b in pairs):
                collected[component].append(sum(stamps[b][0] - stamps[a][0] for a, b in pairs) / 1000)
    return {name: {'mean_us': statistics.mean(values), 'count': len(values),
                   'exactness': 'aggregate_of_exact_intervals',
                   'selection': 'same-call endpoints; incomplete and unjoined calls excluded',
                   'source_stages': [a + ' → ' + b for a, b in groups[name]]}
            for name, values in collected.items()}


def _component_floors(row, floors):
    """Return explicitly sized matching floor observations, without pooling repeats."""
    name, route, size, repeat = row['component'], row['route'], row['size_bytes'], row['repeat']
    if name == 'network (wire, both stacks, TLS, dispatch)':
        tool = ('tcp_rtt' if route == 'owner TCP' else 'ib_read_lat' if route.startswith('hybrid') and row['shape'] == 'read'
                else 'ib_send_lat' if route != 'native' else None)
        candidates = [r for r in floors.get('net_floor', []) if r.get('tool') == tool and r.get('repeat') == repeat
                      and r.get('verdict') == 'qualified' and r.get('round_trip_p50_us') is not None
                      and ((tool == 'tcp_rtt' and r.get('interface') == 'eth0' and r.get('mode') == 'block')
                           or (tool != 'tcp_rtt' and r.get('mode') in ('polled', 'event')))]
        size_key, value_key = 'size_bytes', 'round_trip_p50_us'
    elif name == 'ssd device' and row['shape'] == 'read':
        candidates = [r for r in floors.get('ssd_floor', []) if r.get('repeat') == repeat
                      and r.get('state') == 'fresh' and r.get('region_bytes') == 67108864
                      and r.get('verdict') == 'qualified' and r.get('p50_us') is not None]
        size_key, value_key = 'block_bytes', 'p50_us'
    elif (row['shape'] != 'read' and (name == OWNER_PERSIST or
          (route == 'native' and name == 'durable wait and publication'))):
        mode = 'fdatasync' if row.get('durability') in ('synchronous', 'value_wal_barrier') else 'buffered'
        candidates = [r for r in floors.get('ssd_write_floor', []) if r.get('mode') == mode
                      and r.get('repeat') == repeat and r.get('verdict') == 'qualified'
                      and r.get('p50_us') is not None]
        size_key, value_key = 'block_bytes', 'p50_us'
    else:
        return []
    if not candidates:
        return []
    distance = min(abs(r[size_key] - size) for r in candidates)
    return [{'case_id': r['case_id'], 'size_bytes': r[size_key], 'mode': r.get('mode') or r.get('state'),
             'p50_us': r[value_key], 'exactness': 'floor_derived'}
            for r in candidates if abs(r[size_key] - size) == distance]


def stages_component_summary(export, cases, ops, writes, reads, floors):
    """Keep component populations and overlapping annotations explicit."""
    indexed = defaultdict(dict)
    for row in writes + reads:
        if row.get('case_id') and row.get('stage'):
            indexed[row['case_id']][row['stage']] = row
    jobs = {c['case_id']: c['job'] for c in cases}
    output = []
    for op in ops:
        for rep in op['repetitions']:
            cid = rep['case_id']
            job = jobs[cid]
            stages = indexed[cid]
            api = rep['mean_us'] if rep['status'] == 'completed' and rep['verdict'] == 'qualified' else None
            components = {name: {'mean_us': None, 'exactness': 'not computed',
                                  'selection': 'not computed: component evidence unavailable',
                                  'population_n': None, 'source_stages': []} for name in component_names(op['shape'])}
            def add(name, stage, label=None):
                if stage:
                    components[name] = {'mean_us': stage.get('mean_us'),
                                        'exactness': stage.get('exactness') if stage.get('mean_us') is not None else 'not computed',
                                        'population_n': stage.get('count'), 'selection': stage.get('selection'),
                                        'source_stages': stage.get('source_stages', [label or stage.get('stage')])}
            def hide(names, reason):
                for name in names:
                    components[name] = {'mean_us': None, 'exactness': 'not computed', 'population_n': None,
                                        'selection': reason, 'source_stages': []}
            def sum_stages(name, names, raw=None, covered=True):
                selected = [stages.get(stage, {}) for stage in names]
                denominator = rep.get('n')
                values = []
                if not denominator or not covered:
                    return
                for stage_name, stage in zip(names, selected):
                    count = stage.get('count')
                    original = (raw or {}).get(stage_name, {})
                    if (count == 0 and original.get('sum_ns') is None
                            and original.get('unavailable_reason') == 'no observations for this stage'):
                        values.append(0)
                    elif (stage.get('mean_us') is not None and type(count) is int and 0 < count <= denominator):
                        values.append(stage['mean_us'] * count / denominator)
                    else:
                        return
                components[name] = {'mean_us': sum(values), 'exactness': 'sampled_mean', 'population_n': denominator,
                    'source_stages': list(names), 'selection': 'full selected window; per-call stage totals; populations may overlap'}
            if op['shape'] == 'read':
                profile_path = export / 'cases' / cid / 'result' / 'read-diagnostics.json'
                profile = (json.loads(profile_path.read_text()).get('profile') or {}) if profile_path.is_file() else {}
                resolution = profile.get('coordinator_resolution') or {}
                full_api = (bool(rep.get('n')) and rep['n'] == job.get('measurement_operations')
                            and resolution.get('selected') == rep['n'] and profile.get('coordinator_resolution_valid') is True
                            and resolution.get('errors') == resolution.get('member_errors') == 0)
                full_remote = (bool(rep.get('n')) and rep['n'] == job.get('measurement_operations')
                               and profile.get('selected_operations') == profile.get('completed_operations') == rep['n']
                               and (profile.get('counters') or {}).get('read_profile_errors') == 0
                               and not profile.get('finalization_error'))
                sum_stages('client coordinator', ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture',
                    'route_preparation', 'pin_preparation', 'batch_preparation', 'postread_cache_fill', 'result_materialization'),
                    resolution.get('stages'), full_api)
                batch_stage = (resolution.get('stages') or {}).get('batch_wait', {})
                if full_api and batch_stage.get('count') == 0 and batch_stage.get('unavailable_reason') == 'no observations for this stage':
                    add('waiting and coalescing', {'stage': 'batch_wait', 'mean_us': 0, 'count': rep['n'],
                        'exactness': 'sampled_mean', 'selection': 'full API resolution window; no batch wait observations'})
                elif full_api and batch_stage.get('sum_ns') is not None:
                    add('waiting and coalescing', {'stage': 'batch_wait',
                        'mean_us': batch_stage['sum_ns'] / rep['n'] / 1000, 'count': rep['n'],
                        'exactness': 'sampled_mean', 'selection': 'full API resolution window; total per measured call'})
                hybrid = job.get('backend') == 'tidehunter_nvme_rdma'
                device = stages.get('spdk_bdev') or stages.get('client_device') or stages.get('owner_block_layer')
                ops_per_read = device['count'] / rep['n'] if device and rep.get('n') else None
                block_device = bool(device and device.get('stage') != 'spdk_bdev')
                contaminated = block_device and ops_per_read is not None and ops_per_read > 1.05
                cache_hit = (not contaminated and (job.get('media', {}).get('expected') == 'memory'
                             or (block_device and ops_per_read is not None and ops_per_read <= 0.06)))
                partial_owner = (not hybrid and job.get('backend') != 'native' and block_device
                                 and not cache_hit and ops_per_read is not None and ops_per_read < 0.5)
                guard = (f'not computed: device counters include non-API reads (ops per read {ops_per_read:.3f})'
                         if contaminated else
                         f'not computed: partially page-cache served (ops per read {ops_per_read:.3f})'
                         if partial_owner else None)
                if guard and device.get('count'):
                    kib = device.get('bytes')
                    detail = (f"; device time per device op (upper bound, includes non-API reads): "
                              f"{device['mean_us']:.3f} us; ops per read {ops_per_read:.3f}; "
                              f"average {kib / device['count'] / 1024:.3f} KiB per device read"
                              if kib is not None and device.get('mean_us') is not None else '')
                    guard += detail
                device_per_call = (device['mean_us'] * device['count'] / rep['n']
                                   if device and device.get('mean_us') is not None and rep.get('n')
                                   and not guard and not cache_hit else None)
                if (job.get('backend') == 'native' and device_per_call is not None
                        and api is not None and device_per_call > api):
                    guard = 'not computed: device time exceeds API mean; populations differ'
                    device_per_call = None
                raw_stages = profile.get('stages') or {}
                if not hybrid:
                    required = ('inline', 'exchange', 'server_handler', 'request_encode', 'response_decode', 'record_verify')
                    covered = (full_api and full_remote and all(raw_stages.get(name, {}).get('count') == rep['n']
                               and raw_stages[name].get('sum_ns') is not None for name in required))
                    sum_stages('client messaging (encode, decode, verify)', ('request_encode', 'response_decode', 'record_verify'),
                               raw_stages, covered)
                    if covered:
                        per_call = lambda name: raw_stages[name]['sum_ns'] / rep['n'] / 1000
                        queue = per_call('inline') - per_call('exchange')
                        network = per_call('exchange') - per_call('server_handler') - sum(
                            per_call(name) for name in ('request_encode', 'response_decode', 'record_verify'))
                        add('client queueing and wake-up', {'stage': 'inline minus exchange', 'mean_us': queue,
                            'count': rep['n'], 'exactness': 'derived_mean_difference',
                            'selection': 'full selected reads; inline minus nested exchange; populations not individually joined'})
                        add('network (wire, both stacks, TLS, dispatch)', {'stage': 'exchange minus handler and messaging',
                            'mean_us': network, 'count': rep['n'], 'exactness': 'derived_mean_difference',
                            'source_stages': ['exchange', 'server_handler', 'request_encode', 'response_decode', 'record_verify'],
                            'selection': ('negative: populations differ; ' if network < 0 else '') +
                                'full selected reads; differences of nested stage means; population identity not proven'})
                else:
                    reader_names = ('admission', 'alignment', 'queue', 'submit_to_cq', 'extraction', 'verification')
                    reader_covered = (full_api and full_remote and all(stages.get(name, {}).get('count') == rep['n']
                                      and stages[name].get('mean_us') is not None for name in reader_names))
                    sum_stages('client queueing and wake-up', ('admission', 'alignment', 'queue'), covered=reader_covered)
                    sum_stages('client messaging (encode, decode, verify)', ('extraction', 'verification'), covered=reader_covered)
                    if reader_covered:
                        submit = stages['submit_to_cq']['mean_us'] * stages['submit_to_cq']['count'] / rep['n']
                        if contaminated or (device_per_call is None and ops_per_read is None and not cache_hit):
                            add(DEVICE_UNSEPARATED, {'stage': 'submit_to_cq', 'mean_us': submit,
                                'count': rep['n'], 'exactness': 'sampled_mean',
                                'selection': 'device, fabric and stack not separable; full selected reads'})
                        else:
                            network = submit - (device_per_call or 0)
                            add('network (wire, both stacks, TLS, dispatch)', {'stage': 'submit_to_cq minus device per read',
                                'mean_us': network, 'count': rep['n'], 'exactness': 'derived_mean_difference',
                                'source_stages': ['submit_to_cq', device['stage'] if device else 'no device operations'],
                                'selection': ('negative: populations differ; ' if network < 0 else '') +
                                    'full selected reads; window reader total minus device total per read'})
                add('owner software', stages.get('server_handler') if not hybrid and cache_hit else
                    stages.get('owner server software') if job.get('media', {}).get('expected') == 'cold'
                    else stages.get('server_handler') if not hybrid else None)
                if device_per_call is not None:
                    add('ssd device', {**device, 'mean_us': device_per_call,
                                       'selection': 'measurement-window total device time per measured API read; populations may differ'})
                elif guard:
                    hide(('ssd device',), guard)
                elif cache_hit:
                    hide(('ssd device',), 'not applicable (cache hit)')
                add('residual', stages.get('client other') or stages.get('native cold software'))
                split_missing = (not hybrid and job.get('backend') != 'native'
                                 and any(components[name]['mean_us'] is None for name in
                                     ('client messaging (encode, decode, verify)', 'client queueing and wake-up',
                                      'network (wire, both stacks, TLS, dispatch)', 'owner software')))
                if hybrid:
                    split_missing = any(components[name]['mean_us'] is None for name in
                        ('client messaging (encode, decode, verify)', 'client queueing and wake-up')) or (
                        components['network (wire, both stacks, TLS, dispatch)']['mean_us'] is None
                        and components[DEVICE_UNSEPARATED]['mean_us'] is None)
                elif guard:
                    split_missing = True
                if split_missing and full_api and full_remote:
                    whole = [(raw_stages[name]['sum_ns'] / rep['n'] / 1000, name)
                             for name in ('inline', 'exchange') if raw_stages.get(name, {}).get('count') == rep['n']
                             and raw_stages[name].get('sum_ns') is not None]
                    if whole:
                        duration, source = max(whole)
                        hide(('client messaging (encode, decode, verify)', 'client queueing and wake-up',
                              'network (wire, both stacks, TLS, dispatch)', 'owner software', 'ssd device'),
                             'not measured: included in the whole ' + source + ' interval')
                        if guard:
                            hide(('ssd device',), guard)
                        add(EXCHANGE_UNSEPARATED, {'stage': source, 'mean_us': duration, 'count': rep['n'],
                            'exactness': 'sampled_mean', 'selection': 'full selected window; whole ' + source +
                            ' interval; owner and network not separable'})
                        coordinator = components['client coordinator']['mean_us']
                        waiting = components['waiting and coalescing']['mean_us']
                        if api is not None and coordinator is not None and waiting is not None:
                            add('residual', {'stage': 'API minus coordinator, waiting and ' + source,
                                'mean_us': api - coordinator - waiting - duration, 'count': rep['n'],
                                'exactness': 'derived_mean_difference',
                                'selection': 'full selected window; remaining API time outside whole ' + source})
            else:
                for name, value in _write_component_intervals(export, cid).items():
                    add(name, value)
                add('network (wire, both stacks, TLS, dispatch)',
                    stages.get('exchange minus owner handler (wire, both network stacks, TLS, dispatch)'))
                handler, persist = stages.get('owner_handler_ns', {}), stages.get('owner_append_persist_ns', {})
                if (handler.get('mean_us') is not None and persist.get('mean_us') is not None
                        and handler.get('count') == persist.get('count')):
                    add('owner software', {'mean_us': handler['mean_us'] - persist['mean_us'],
                        'count': handler['count'], 'exactness': 'derived_mean_difference',
                        'source_stages': ['owner_handler_ns', 'owner_append_persist_ns'],
                        'selection': 'equal envelope counts; population identity not proven; excludes inclusive persistence'})
                add(OWNER_PERSIST, {**persist, 'selection': 'owner append_persist; inclusive of software, not physical SSD service'} if persist else None)
                if op['route'] != 'native' and job.get('diagnostics', {}).get('write_timeline_limit'):
                    network = stages.get('exchange minus owner handler (wire, both network stacks, TLS, dispatch)', {})
                    separated = all(s.get('mean_us') is not None and s.get('count') == rep.get('n')
                                    for s in (handler, persist, network))
                    if not separated:
                        hide(('network (wire, both stacks, TLS, dispatch)', 'owner software', OWNER_PERSIST),
                             'not measured: owner envelopes are shared or unavailable')
                        exchange = stages.get('tx_queued → completion_harvested', {})
                        if exchange.get('mean_us') is not None and exchange.get('count') == rep.get('n'):
                            add(EXCHANGE_UNSEPARATED, {**exchange,
                                'selection': 'whole coordinator exchange; owner and network not separable; '
                                             f"unshared owner-join calls: {network.get('count', 0)}"})
                if op['route'] == 'native':
                    complete = (rep.get('n') == job.get('measurement_operations') and rep.get('n')
                                and all(stages.get(name, {}).get('count') == rep['n'] for name in ('wal_append', 'index_apply')))
                    sum_stages('client coordinator', ('wal_append', 'index_apply'), covered=complete)
                    barrier = stages.get('barrier_file', {})
                    if complete and barrier.get('mean_us') is not None:
                        add('durable wait and publication', {**barrier,
                            'mean_us': barrier['mean_us'] * barrier['count'] / rep['n'],
                            'selection': 'barrier file total per measured API call'})
                add('residual', stages.get('unattributed'))
            for name, value in components.items():
                row = {**{k: op[k] for k in ('route', 'shape', 'size_bytes', 'concurrency', 'durability',
                       'native_durability', 'key_policy', 'cache_state', 'target_io', 'diagnostics', 'group')},
                       'case_ids': [cid], 'case_id': cid, 'repeat': rep['repeat'], 'verdict': rep['verdict'],
                       'component': name, 'api_mean_us': api, **value}
                if api is None:
                    row.update(mean_us=None, exactness='not computed', selection='not computed: case not qualified or no API samples')
                if row['mean_us'] is not None and row['mean_us'] < 0:
                    row.update(mean_us=None, exactness='not computed',
                               selection='not computed: negative difference; populations differ')
                row['share_of_api_mean'] = ratio(row['mean_us'], api)
                matching = _component_floors(row, floors)
                row.update(floor=matching, floor_size_bytes=[f['size_bytes'] for f in matching],
                           floor_exactness='floor_derived' if matching else 'not computed',
                           excess_over_floor_us=[{'case_id': f['case_id'], 'size_bytes': f['size_bytes'],
                               'mode': f['mode'], 'mean_minus_floor_p50_us': row['mean_us'] - f['p50_us']}
                               for f in matching] if row['mean_us'] is not None else [])
                output.append(row)
    return sorted(output, key=lambda r: (r['case_id'], r['mean_us'] is None,
                                          -(r['mean_us'] or 0), r['component']))


def stages_component_compare(rows):
    """Select one case per route, preferring more observed components, then profiling."""
    route_names = ('owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel', 'hybrid SPDK', 'native')
    groups = defaultdict(list)
    for row in rows:
        tier = (row['cache_state'] if row['shape'] == 'read' else
                'durable' if row['durability'] in ('synchronous', 'value_wal_barrier') else
                'page_cache' if row['durability'] in ('native', 'page_cache') else row['durability'])
        workload = 'sustained' if row['group'] == 'G12S' else 'standard'
        key = (row['shape'], row['size_bytes'], row['concurrency'], tier, row['key_policy'], workload)
        groups[key].append(row)
    output = []
    for key, members in sorted(groups.items(), key=lambda item: str(item[0])):
        base = dict(zip(('action', 'size_bytes', 'concurrency', 'tier', 'key_policy', 'workload'), key))
        selected_cases = {}
        for route in route_names:
            cases = defaultdict(list)
            for item in members:
                if (item['route'] == route or
                        (route == 'hybrid kernel' and item['route'].startswith('hybrid kernel '))):
                    cases[item['case_id']].append(item)
            ranked = sorted(cases.values(), key=lambda group: (
                -sum(item['mean_us'] is not None for item in group),
                not bool(group[0]['diagnostics']), group[0]['repeat'], group[0]['case_id']))
            selected_cases[route] = {item['component']: item for item in ranked[0]} if ranked else {}
        maximum = {}
        for component in component_names(key[0]):
            row = {**base, 'component': component, 'case_ids': []}
            for route in route_names:
                chosen = selected_cases[route].get(component)
                if chosen and chosen['mean_us'] is None:
                    chosen = None
                row[route] = ({**{k: chosen[k] for k in ('mean_us', 'share_of_api_mean', 'exactness', 'case_id',
                              'repeat', 'source_stages', 'selection', 'floor', 'excess_over_floor_us')},
                              'population_n': chosen.get('population_n')} if chosen else 'not measured')
                if chosen:
                    row['case_ids'].append(chosen['case_id'])
                    if route not in maximum or chosen['mean_us'] > maximum[route][1]:
                        maximum[route] = (component, chosen['mean_us'], chosen['case_id'])
            output.append(row)
        output.append({**base, 'component': 'largest components', 'case_ids': sorted({v[2] for v in maximum.values()}),
                       **{route: {'component': maximum[route][0], 'mean_us': maximum[route][1],
                                  'case_id': maximum[route][2]} if route in maximum else 'not measured'
                          for route in route_names}})
    return output


def _md_value(value):
    if value is None:
        return 'missing'
    if isinstance(value, float):
        return f'{value:.3g}'
    return str(value).replace('|', '\\|').replace('\n', ' ')


def _md_table(headers, rows):
    return ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join('---' for _ in headers) + ' |'] + [
        '| ' + ' | '.join(_md_value(value) for value in row) + ' |' for row in rows]


def where_time_goes(rows):
    """Summarize each qualified case without combining repetitions or profile populations."""
    groups = defaultdict(list)
    for row in rows:
        if row['api_mean_us'] is not None:
            groups[row['case_id']].append(row)
    labels = {'aggregate_of_exact_intervals': 'sampled interval', 'sampled_mean': 'sampled',
              'derived_duration_difference': 'derived duration', 'derived_mean_difference': 'derived mean',
              'device_counter': 'device counter', 'residual': 'residual', 'floor_derived': 'floor'}
    by_action = defaultdict(list)
    for cid, group in groups.items():
        first = group[0]
        ranked = sorted((r for r in group if r['mean_us'] is not None),
                        key=lambda r: (-r['mean_us'], r['component']))
        top = []
        for row in ranked[:3]:
            share = row.get('share_of_api_mean')
            share_text = _md_value(100 * share) + '%' if share is not None else 'missing share'
            flag = ' [negative: populations differ]' if 'negative: populations differ' in (row.get('selection') or '') else ''
            sample_count = f", n={row['population_n']}" if row.get('population_n') is not None else ''
            top.append(f"{row['component']}: {_md_value(row['mean_us'])} us ({share_text}, "
                       f"{labels.get(row['exactness'], row['exactness'])}{sample_count}){flag}")
        top.extend(['not measured'] * (3 - len(top)))
        excesses = [(entry['mean_minus_floor_p50_us'], row['component'], entry)
                    for row in group for entry in row['excess_over_floor_us']]
        if excesses:
            value, component, floor = max(excesses, key=lambda item: item[0])
            excess = (f"{component}: {_md_value(value)} us "
                      f"({floor['mode']} {floor['size_bytes']}B floor, {floor['case_id']})")
        else:
            excess = 'not measured'
        device_note = next((r['selection'] for r in group if r['component'] == 'ssd device'
                            and ('non-API reads' in (r.get('selection') or '')
                                 or 'partially page-cache served' in (r.get('selection') or ''))), '')
        by_action[first['shape']].append((first['size_bytes'], first['route'], first['concurrency'], cid,
            [first['route'], first['size_bytes'], first['concurrency'],
             first.get('cache_state') or first.get('durability'), first['key_policy'], cid,
             first['api_mean_us'], *top, excess, device_note]))
    lines = ['# Where Time Goes', '',
             'Each row is one qualified case, not a pooled repetition. Components can have different sampled '
             'populations and can overlap; floor excess compares a component mean with a separate floor p50. '
             'Device counters cover the whole gate-to-gate window and include background writeback; '
             'device-counter per-op comparisons are not valid for coherence. Owner persist includes software.', '']
    for action, entries in sorted(by_action.items()):
        lines.extend(['## ' + str(action), ''])
        lines.extend(_md_table(['route', 'size bytes', 'c', 'tier / cache', 'key policy', 'case',
                                'API mean us', 'largest', 'second', 'third', 'largest mean minus floor p50', 'device annotation (not additive)'],
                               [item[-1] for item in sorted(entries, key=lambda item: item[:4])]))
        lines.append('')
    return lines


def _headline_cell(cell, repetitions=()):
    if isinstance(cell, str):
        return cell
    spread = cell['p50_range_us']
    if isinstance(spread, dict):
        span = f"range {_md_value(spread['min'])}-{_md_value(spread['max'])}"
    else:
        span = spread
    counts = [rep['n'] for rep in repetitions if rep['status'] == 'completed'
              and rep['verdict'] == 'qualified' and rep['n'] is not None]
    count_text = f'{len(counts)}x{counts[0]}' if counts and len(set(counts)) == 1 else '+'.join(map(str, counts))
    state = cell.get('cache_state') or cell.get('durability') or 'unspecified'
    return (f"p50 {_md_value(cell['p50_us'])} us ({span}, n={count_text}; qualified; "
            f"{cell['source']}; {state}; target I/O {cell.get('target_io') or 'not applicable'})")


def stages_markdown(name, rows, tables):
    """Render concise stage tables without changing CSV or summary evidence."""
    if name == 'apples_to_apples':
        labels = ('cache_read', 'disk_read', 'durable_write', 'page_cache_write')
        rendered, dropped = [], 0
        for row in rows:
            cells = []
            for label in labels:
                cell = row[label]
                if cell == 'not measured':
                    matching = [r for r in tables['ops_summary'] if stages_headline_match(
                        r, row['route'], row['size_bytes'], row['concurrency'], label)]
                    reasons = sorted({reason for r in matching for rep in r['repetitions']
                                      if rep['verdict'] == 'unqualified' for reason in rep['reason_codes']})
                    cell = 'unqualified: ' + ', '.join(reasons) if reasons else cell
                source = next((r for r in tables['ops_summary'] if isinstance(cell, dict)
                               and r['case_ids'] == cell['case_ids']), {})
                cells.append(_headline_cell(cell, source.get('repetitions', ())))
            if all(cell == 'not measured' for cell in cells):
                dropped += 1
                continue
            rendered.append([row['route'], row['size_bytes'], row['concurrency'], *cells])
        return ['# Apples To Apples', '', *_md_table(
            ['route', 'size bytes', 'c', 'cache read', 'disk read', 'durable write', 'page-cache write'], rendered),
            '', f'{dropped} rows with no measured cells omitted.']
    if name in ('decomposition_write', 'decomposition_read'):
        keys = ('route', 'shape', 'size_bytes', 'concurrency', 'durability', 'key_policy', 'cache_state')
        groups = defaultdict(list)
        for row in rows:
            groups[tuple(row.get(key) for key in keys)].append(row)
        lines = ['# ' + name.replace('_', ' ').title(), '',
                 'Stage annotations can overlap and are not additive. exact_interval describes one adjacent '
                 'event pair; aggregate_of_exact_intervals summarizes retained calls. '
                 'derived_duration_difference subtracts locally measured durations, not cross-host timestamps; '
                 'owner handler time is assumed to lie inside the coordinator exchange. '
                 'derived_mean_difference can compare populations whose identity is not proven. '
                 'Exchange minus server handler includes client messaging plus wire and stacks, not wire alone. '
                 'Device counters cover the whole gate-to-gate window and include background writeback; '
                 'device-counter per-op comparisons are not valid for coherence.', '']
        leaves = ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture', 'route_preparation',
                  'pin_preparation', 'batch_preparation', 'batch_wait', 'postread_cache_fill', 'result_materialization')
        devices = ('owner_block_layer', 'client_device', 'spdk_bdev')
        for key, group in sorted(groups.items(), key=lambda item: tuple(str(x) for x in item[0])):
            measured = [row for row in group if (row.get('count') or 0) > 0 and row.get('mean_us') is not None]
            annotations = [row for row in measured if row.get('stage', '').startswith(('client_transport_', 'client_socket_'))]
            group = [row for row in measured if not row.get('stage', '').startswith(('client_transport_', 'client_socket_'))]
            if not measured:
                continue
            lines.extend(['## ' + ' / '.join(_md_value(value) for value in key),
                          'Cases: ' + ', '.join(sorted({cid for row in measured for cid in row.get('case_ids', [])})),
                          'Selection: ' + '; '.join(sorted({str(row['selection']) for row in measured if row.get('selection')})), ''])
            parents = {row.get('stage'): row.get('parent') for row in group if row.get('parent')}
            event_starts = {row['stage']: min(x['start_ns'] for x in row['intervals'])
                            for row in group if row.get('intervals')}
            def ancestry(stage):
                path, seen = [stage], {stage}
                while path[0] in parents and parents[path[0]] not in seen:
                    parent = parents[path[0]]
                    seen.add(parent)
                    path.insert(0, parent)
                return tuple(path)
            def order(row):
                stage = row.get('stage', '')
                if stage == 'unattributed': return (4, 0, stage)
                if stage in devices or row.get('exactness') in ('device_counter', 'floor_derived'): return (3, 0, stage)
                if name == 'decomposition_read':
                    if stage in leaves: return (0, leaves.index(stage), stage)
                    return (1, ancestry(stage))
                path = ancestry(stage)
                root = path[0]
                return (0, event_starts[root], path) if root in event_starts else (1, 0, path)
            body = []
            for row in sorted(group, key=order):
                stage = ('↳ ' * max(0, len(ancestry(row.get('stage', ''))) - 1)) + str(row.get('stage', ''))
                body.append([stage, row.get('exactness'), row.get('count'), row.get('mean_us'),
                             row.get('p50_us'), row.get('share_of_api_mean')])
            lines.extend(_md_table(['stage', 'exactness', 'n', 'mean us', 'p50 us', 'share of API mean'], body))
            if annotations:
                lines.extend(['', 'Transport and socket annotations: top 10 by mean (overlapping, not additive).', ''])
                lines.extend(_md_table(['stage', 'mean us', 'n'],
                    [[row['stage'], row['mean_us'], row['count']] for row in
                     sorted(annotations, key=lambda r: (-r['mean_us'], r['stage']))[:10]]))
            lines.append('')
        return lines
    if name in ('component_summary', 'component_compare'):
        lines = ['# ' + name.replace('_', ' ').title(), '',
                 'Components are independently sampled or exact timeline intervals; overlapping annotations are not additive. '
                 'Owner persist includes software and is not physical SSD service. '
                 'Network means include both stacks, TLS and dispatch, not wire alone. '
                 'Owner-read network subtracts nested encode, decode, verification and handler from exchange; '
                 'queueing is inline minus exchange. Hybrid-read submit-to-CQ is split into device and fabric, '
                 'and extraction and verification are counted once as client messaging. '
                 'These splits require the stated full-coverage conditions. '
                 'For c16 writes owner joins exclude shared envelopes; network estimates use unshared calls only, '
                 'and n gives the calls used. When the split lacks full-call coverage the whole exchange is shown instead. '
                 'Floor excess compares a component mean with a separate floor p50; each floor size is shown. '
                 'Write persist is compared with fdatasync total (write plus sync) for durable tiers and buffered write for native tiers. '
                 'Get-many batch reads are not measured in this run. '
                 'Each route column selects one case with the most measured components; ties prefer profiling, then earlier repetition. '
                 'Device counters cover the whole gate-to-gate window and include background writeback; '
                 'device-counter per-op comparisons are not valid for coherence.', '']
        if name == 'component_summary':
            groups = defaultdict(list)
            for row in rows:
                groups[row['case_id']].append(row)
            omitted = 0
            for cid, group in sorted(groups.items()):
                if all(row['mean_us'] is None for row in group):
                    omitted += 1
                    continue
                first = group[0]
                lines.extend([f"## {cid}: {first['route']} / {first['shape']} / {first['size_bytes']} bytes / c{first['concurrency']} / "
                              f"{first.get('cache_state') or first.get('durability')} / {first['key_policy']}",
                              f"API mean: {_md_value(first['api_mean_us'])} us", ''])
                body = []
                for row in sorted(group, key=lambda r: (r['mean_us'] is None, -(r['mean_us'] or 0), r['component'])):
                    floors = ', '.join(f"{f['mode']} {f['size_bytes']}B {_md_value(f['p50_us'])}us"
                                       for f in row['floor']) or 'not measured'
                    excess = ', '.join(f"{e['size_bytes']}B {_md_value(e['mean_minus_floor_p50_us'])}us"
                                       for e in row['excess_over_floor_us']) or 'not computed'
                    body.append([row['component'], row['mean_us'], row.get('population_n'), row['share_of_api_mean'], row['exactness'],
                                 ', '.join(str(x) for x in row['source_stages']), row.get('selection'), floors, excess])
                lines.extend(_md_table(['component', 'mean us', 'n', 'share', 'exactness', 'source stages', 'selection / condition',
                                        'floor p50', 'mean minus floor p50'], body))
                lines.append('')
            lines.append(f'{omitted} groups with no measured components omitted.')
            return lines
        routes = ('owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel', 'hybrid SPDK', 'native')
        groups = defaultdict(list)
        for row in rows:
            groups[tuple(row[k] for k in ('action', 'size_bytes', 'concurrency', 'tier', 'key_policy', 'workload'))].append(row)
        omitted = 0
        for key, group in sorted(groups.items(), key=lambda item: str(item[0])):
            if all(isinstance(row[route], str) for row in group for route in routes):
                omitted += 1
                continue
            lines.extend(['## ' + ' / '.join(_md_value(value) for value in key), ''])
            body = []
            for row in sorted((r for r in group if r['component'] != 'largest components'),
                              key=lambda r: component_names(key[0]).index(r['component'])):
                cells = []
                for route in routes:
                    cell = row[route]
                    cells.append('not measured' if isinstance(cell, str) else
                                 f"{_md_value(cell['mean_us'])} us / {_md_value(cell['share_of_api_mean'])} "
                                 f"({cell['exactness']}, n={_md_value(cell.get('population_n'))}, {cell['case_id']})" +
                                 (' [negative: populations differ]' if 'negative: populations differ' in (cell.get('selection') or '') else ''))
                body.append([row['component'], *cells])
            lines.extend(_md_table(['component', *routes], body))
            largest = next((r for r in group if r['component'] == 'largest components'), {})
            for route in routes:
                value = largest.get(route, 'not measured')
                lines.append(f"Largest components — {route}: " + ('not measured' if isinstance(value, str) else
                             f"{value['component']} {_md_value(value['mean_us'])} us ({value['case_id']})"))
            lines.append('')
        lines.append(f'{omitted} groups with no measured components omitted.')
        return lines
    if name == 'stall':
        body = []
        for pair in rows:
            for policy in ('overwrite', 'new'):
                row = pair.get(policy)
                if not isinstance(row, dict):
                    continue
                fit = row.get('duration_fit')
                fit_text = (f"{_md_value(fit['intercept_us'])} us + {_md_value(fit['slope_us_per_put'])} us/put"
                            if fit else 'missing')
                phases = row.get('phase_counts', {})
                observed = [item for item in row.get('stalls', [])
                            if item.get('longest_phase') is not None and item.get('longest_phase_us') is not None]
                longest = max(observed, key=lambda item: item['longest_phase_us'])['longest_phase'] if observed else 'missing'
                body.append([row.get('case_id'), policy, row.get('completion_count'), row.get('count'),
                             row.get('mean_spacing_puts'), fit_text, longest,
                             ', '.join(f'{phase}: {count}' for phase, count in sorted(phases.items())) or 'missing'])
        return ['# Stall', '', *_md_table(['case', 'key policy', 'completions', 'stalls', 'mean spacing puts',
                                           'duration fit', 'longest-interval phase', 'phase counts'], body)]
    raise ValueError('unknown stages markdown table')


def generate(export, report, output, baseline_root=None):
    export, report, output = map(Path, (export, report, output))
    matrix = json.loads((export / 'matrix.json').read_text())
    data = json.loads((report / 'comparison.json').read_text())
    cases = data['cases']
    planned = {j['case_id']: j for j in matrix['jobs']}
    if len(planned) != len(matrix['jobs']) or len(cases) != len(planned) or any(c.get('job') != planned.get(c.get('case_id')) for c in cases) or len({c['case_id'] for c in cases}) != len(cases):
        raise ValueError('report cases do not match export matrix')
    if any(j.get('group') in ('G10R', 'G10W') for j in matrix['jobs']):
        return device_generate(export, report, output, cases, baseline_root)
    tables = defaultdict(list)
    by_kind = defaultdict(list)
    for c in cases:
        by_kind[c['job'].get('case_kind')].append(c)
    import fleet_tools
    tables.update(fleet_tools.analysis_rows(cases))
    for c in by_kind['media_read']:
        j = c['job']; e = c.get('focused_evidence', {}).get('media_evidence', {}); v, reasons = verdict(c)
        row = {**base(c), **metrics(c), 'expected': j.get('media', {}).get('expected'), 'history': j.get('media', {}).get('history'), 'verdict': v, 'reason_codes': reasons, 'read_ops_per_success': e.get('read_ops_per_success')}
        tables['media_read'].append(row)
        if row['history'] == 'prewarm':
            h = c.get('history_prewarm_qualification') or {}
            tables['warm_history'].append({**row, 'baseline_case_id': h.get('baseline_case_id'), 'case_ids': sorted(set(row['case_ids'] + ([h['baseline_case_id']] if h.get('baseline_case_id') else []))), 'prewarm_history_load_delta': h.get('prewarm_history_load_delta'), 'baseline_history_load_delta': h.get('baseline_history_load_delta'), 'history_delta_ratio': h.get('history_delta_ratio')})
    media = tables['media_read']
    for cold in [r for r in media if r['expected'] == 'cold' and r['history'] == 'normal']:
        matches = [r for r in media if r['expected'] == 'memory' and (r['route'], r['size_bytes'], r['repeat']) == (cold['route'], cold['size_bytes'], cold['repeat'])]
        resident = matches[0] if matches else {}
        tables['cold_resident'].append({'case_ids': cold['case_ids'] + resident.get('case_ids', []), 'route': cold['route'], 'size_bytes': cold['size_bytes'], 'repeat': cold['repeat'], 'cold_case_id': cold['case_id'], 'resident_case_id': resident.get('case_id'), 'cold_p50_us': cold['api_p50_us'], 'resident_p50_us': resident.get('api_p50_us'), 'cold_verdict': cold['verdict'], 'resident_verdict': resident.get('verdict', 'missing'), 'cold_over_resident_p50': ratio(cold['api_p50_us'], resident.get('api_p50_us'))})
    grouped = defaultdict(dict)
    for r in media:
        if r['expected'] == 'cold' and r['route'] in ('hybrid kernel buffered', 'hybrid kernel DIRECT', 'hybrid SPDK'):
            grouped[(r['size_bytes'], r['repeat'])][r['route']] = r
    for (size, rep), group in sorted(grouped.items()):
        row = {'size_bytes': size, 'repeat': rep, 'case_ids': sorted(r['case_id'] for r in group.values())}
        for short, name in [('buffered', 'hybrid kernel buffered'), ('direct', 'hybrid kernel DIRECT'), ('spdk', 'hybrid SPDK')]:
            r = group.get(name, {}); row.update({short + '_case_id': r.get('case_id'), short + '_p50_us': r.get('api_p50_us'), short + '_p99_us': r.get('api_p99_us'), short + '_p999_us': r.get('api_p999_us'), short + '_verdict': r.get('verdict', 'missing')})
        for name in ('direct', 'spdk'):
            for stat in ('p50', 'p99', 'p999'):
                row[name + '_over_buffered_' + stat] = ratio(row[name + '_' + stat + '_us'], row['buffered_' + stat + '_us'])
        tables['cold_targets'].append(row)
    for size in sorted({r['size_bytes'] for r in tables['cold_targets']}):
        rows = [r for r in tables['cold_targets'] if r['size_bytes'] == size]
        fields = [name + '_' + stat + '_us' for name in ('buffered', 'direct', 'spdk') for stat in ('p50', 'p99', 'p999')]
        fields += [name + '_over_buffered_' + stat for name in ('direct', 'spdk') for stat in ('p50', 'p99', 'p999')]
        for field in fields:
            tables['cold_target_spread'].append({'size_bytes': size, 'metric': field, 'case_ids': sorted({i for r in rows for i in r['case_ids']}), 'repetitions': {str(r['repeat']): r[field] for r in rows}, **summarize_spread([r[field] for r in rows])})
    for c in by_kind['sealed_knee']:
        v, reasons = verdict(c)
        tables['sealed_knee'].append({**base(c), **metrics(c), 'verdict': v, 'reason_codes': reasons})
    knee = tables['sealed_knee']
    groups = defaultdict(dict)
    for r in knee:
        groups[(r['route'], r['tier'], r['repeat'])][r['concurrency']] = r
    growth = []
    for (r, tier, repeat), group in sorted(groups.items()):
        a, b = group.get(1, {}), group.get(16, {})
        growth.append({'route': r, 'tier': tier, 'repeat': repeat, 'case_ids': a.get('case_ids', []) + b.get('case_ids', []), 'c1_case_id': a.get('case_id'), 'c16_case_id': b.get('case_id'), 'c1_p50_us': a.get('api_p50_us'), 'c16_p50_us': b.get('api_p50_us'), 'growth_factor': ratio(b.get('api_p50_us'), a.get('api_p50_us'))})
    tables['knee_growth'] = growth
    for r, tier in sorted({(x['route'], x['tier']) for x in growth}):
        rows = [x for x in growth if x['route'] == r and x['tier'] == tier]
        tables['knee_spread'].append({'route': r, 'tier': tier, 'case_ids': sorted({i for x in rows for i in x['case_ids']}), 'repetitions': {str(x['repeat']): x['growth_factor'] for x in rows}, **summarize_spread([x['growth_factor'] for x in rows])})
    for c in by_kind['interference']:
        e = c.get('focused_evidence', {}).get('interference_result', {}); v, reasons = verdict(c); accounting = c.get('sealed_read_accounting') or c.get('interference_route_qualification') or {}; row = {**base(c), 'verdict': v, 'reason_codes': reasons, 'accounting_gap': accounting.get('accounting_gap')}
        for stream in ('read', 'put'):
            for key in ('offered', 'admitted', 'completed', 'successful', 'rejected', 'failed'):
                row[stream + '_' + key] = e.get(stream, {}).get(key)
            p = population(c, stream, stream)
            for key in ('p50', 'p99'):
                row[stream + '_api_' + key + '_us'] = latency(p, 'api', key)
            row[stream + '_response_p99_us'] = latency(p, 'response', 'p99')
        match = [k for k in knee if (k['route'], k['repeat'], k['tier'], k['concurrency'], k['size_bytes']) == (row['route'], row['repeat'], 'repeat_read', 16, row['size_bytes'])]
        baseline = match[0] if match else {}
        row.update(baseline_case_id=baseline.get('case_id'), baseline_status=baseline.get('status'), baseline_description='G5 c16 repeat_read, put-free' if baseline else 'none', baseline_read_p50_us=baseline.get('api_p50_us'), baseline_read_p99_us=baseline.get('api_p99_us'))
        row['case_ids'] += baseline.get('case_ids', [])
        tables['interference'].append(row)
    for c in by_kind['fio_floor']:
        e = c.get('focused_evidence', {}).get('fio_result', {}) if c.get('status') == 'completed' else {}; j = c['job'].get('fio', {}); v, reasons = verdict(c); elapsed = e.get('elapsed_seconds')
        tables['fio_floor'].append({**base(c), 'location': j.get('location'), 'block_bytes': j.get('block_bytes'), 'requested_depth': j.get('queue_depth'), 'effective_depth': e.get('effective_depth'), 'iops': ratio(e.get('read_ios'), elapsed), 'bandwidth_bytes_per_second': ratio(e.get('read_bytes'), elapsed), 'p50_us': latency(e, 'latency', 'p50'), 'p99_us': latency(e, 'latency', 'p99'), 'p999_us': latency(e, 'latency', 'p999'), 'region_bytes': e.get('region_bytes'), 'target_buffered_io': e.get('target_buffered_io'), 'verdict': v, 'reason_codes': reasons})
    for c in by_kind['net_floor']:
        job = c['job']['net']
        evidence = c.get('focused_evidence', {}).get('net_floor_result', {}) if c.get('status') == 'completed' else {}
        runs = {run['size_bytes']: run for run in evidence.get('runs', [])}
        for size in job['sizes']:
            run = runs.get(size, {})
            completed = run.get('status') == 'completed'
            values = run.get('latency_us', {}) if completed else {}
            semantics = run.get('latency_semantics') if completed else None
            v = 'qualified' if completed else 'missing'
            row = {**floor_base(c), 'tool': job['tool'], 'mode': job['mode'],
                   'interface': job['interface'], 'provider': job['provider'],
                   'endpoint': job['endpoint'], 'size_bytes': size, 'latency_semantics': semantics,
                   **{key + '_us': values.get(key) for key in ('min', 'p50', 'avg', 'p99', 'p999', 'max')},
                   'verdict': v, 'reason_codes': [] if completed else ['artifact_missing']}
            row['round_trip_p50_us'] = (row['p50_us'] * (2 if semantics == 'half_round_trip' else 1)
                                        if row['p50_us'] is not None and semantics in ('half_round_trip', 'round_trip') else None)
            tables['net_floor'].append(row)
    for c in by_kind['ssd_floor']:
        job = c['job']['ssd']
        evidence = c.get('focused_evidence', {}).get('ssd_floor_result', {}) if c.get('status') == 'completed' else {}
        v, reasons = verdict(c)
        result = {'n': None, 'mean_us': None, 'p50_us': None, 'fast_share': None,
                  'fast_mean_us': None, 'slow_mean_us': None, 'fast_share_by_time_quarter': None,
                  'fast_share_by_offset_decile': None}
        log = evidence.get('lat_log', {})
        if log.get('path'):
            path = export / 'cases' / c['case_id'] / log['path']
            if path.is_file():
                result = ssd_log_metrics(path, job['region_bytes'], job['runtime_seconds'])
            else:
                v, reasons = 'missing', ['artifact_missing']
        elif c.get('status') == 'completed':
            v, reasons = 'missing', ['artifact_missing']
        elapsed = evidence.get('elapsed_seconds')
        tables['ssd_floor'].append({**floor_base(c), 'block_bytes': job['block_bytes'],
            'region_bytes': job['region_bytes'], 'state': job['state'],
            'age_seconds': job['age_seconds'], **result,
            'p99_us': latency(evidence, 'latency', 'p99'), 'p999_us': latency(evidence, 'latency', 'p999'),
            'iops': ratio(evidence.get('read_ios'), elapsed), 'verdict': v, 'reason_codes': reasons})
    for c in by_kind['ssd_write_floor']:
        job = c['job']['ssd_write']
        evidence = c.get('focused_evidence', {}).get('ssd_write_floor_result', {}) if c.get('status') == 'completed' else {}
        log = evidence.get('lat_log', {})
        available = bool(evidence and log.get('path') and (export / 'cases' / c['case_id'] / log['path']).is_file())
        series = evidence.get('total', {}) if available else {}
        tables['ssd_write_floor'].append({**floor_base(c), 'mode': job['mode'], 'block_bytes': job['block_bytes'],
            'region_bytes': job['region_bytes'], 'n': evidence.get('logged_operations') if available else None,
            'total_operations': evidence.get('total_operations') if available else None,
            'dropped_operations': evidence.get('dropped_operations') if available else None,
            'sampling': evidence.get('sampling') if available else None,
            'summary_population': evidence.get('summary_population') if available else None,
            **{name + '_us': series.get(name) / 1000 if series.get(name) is not None else None
               for name in ('mean', 'p50', 'p99', 'p999')},
            **{series_name + '_' + stat + '_us': evidence[series_name][stat] / 1000
               if available and (evidence.get(series_name) or {}).get(stat) is not None else None
               for series_name in ('write', 'sync') for stat in ('mean', 'p50', 'p99', 'p999')},
            'write': evidence.get('write') if available else None, 'sync': evidence.get('sync') if available else None,
            'series': 'total (write plus fdatasync)' if job['mode'] == 'fdatasync' else 'total write',
            'verdict': 'qualified' if available else 'missing', 'reason_codes': [] if available else ['artifact_missing']})
    for row in tables.get('ssd_write_floor', []):
        peers = [r for r in tables['ssd_write_floor'] if r['mode'] == row['mode'] and r['block_bytes'] == row['block_bytes']]
        row['p50_range_us'] = summarize_spread([r['p50_us'] for r in peers if r['verdict'] == 'qualified'])
    for c in by_kind['block_trace']:
        e = c.get('focused_evidence', {}).get('block_trace_evidence', {}); v, reasons = verdict(c)
        tables['block_trace'].append({**base(c), 'scope': c['job'].get('trace', {}).get('scope'), 'available': e.get('available'), 'verdict': v, 'reason_codes': reasons, 'lost_events': e.get('lost_events'), 'roles': (sorted({f['role'] for f in e.get('files', [])}) or ['unknown']) if e else None})
    for c in by_kind['native_anchor']:
        op = 'put' if c['job'].get('mix', {}).get('put') else 'read'
        tables['anchors'].append({**base(c), 'operation': op, **metrics(c, op)})
    for r in knee:
        if r['route'] in ('owner verbs', 'owner TCP', 'hybrid kernel buffered', 'hybrid SPDK') and r['concurrency'] in (1, 16) and r['size_bytes'] == 1024:
            tables['hybrid_versus_owner'].append(dict(r))
    coverage = defaultdict(list)
    for c in cases:
        coverage[(c['job'].get('case_kind'), route(c['job']))].append(c)
        if c.get('status') == 'failed':
            tables['failed_cases'].append({**base(c), 'case_kind': c['job'].get('case_kind'), 'error': c.get('error')})
    for (kind, r), rows in sorted(coverage.items()):
        reasons = defaultdict(int); unqualified = 0
        for c in rows:
            v, why = verdict(c)
            if v == 'unqualified':
                unqualified += 1
                for reason in why: reasons[reason] += 1
        tables['coverage'].append({'case_kind': kind, 'route': r, 'case_ids': sorted(c['case_id'] for c in rows), 'planned': len(rows), 'completed': sum(c.get('status') == 'completed' for c in rows), 'failed': sum(c.get('status') == 'failed' for c in rows), 'not_run': sum(c.get('status') == 'not_run' for c in rows), 'unqualified': unqualified, 'reason_counts': dict(sorted(reasons.items()))})
    names = ['media_read', 'cold_resident', 'cold_targets', 'cold_target_spread', 'warm_history', 'interference', 'sealed_knee', 'knee_growth', 'knee_spread', 'fio_floor', 'block_trace', 'anchors', 'coverage', 'failed_cases', 'hybrid_versus_owner']
    if any(by_kind[kind] for kind in fleet_tools.KINDS):
        names.extend(fleet_tools.KINDS)
    if by_kind['net_floor'] or by_kind['ssd_floor']:
        names.extend(('net_floor', 'ssd_floor'))
    if by_kind['ssd_write_floor']:
        names.append('ssd_write_floor')
    if any(c['job'].get('group', '').startswith('G12') for c in cases):
        tables['ops_summary'] = stages_ops(export, cases)
        tables['apples_to_apples'] = stages_apples(tables['ops_summary'])
        tables['decomposition_write'], tables['decomposition_read'], tables['stall'] = stages_decompositions(export, cases, tables)
        tables['transport_share'] = stages_transport(tables['ops_summary'], tables['decomposition_write'],
                                                     tables['decomposition_read'], tables.get('net_floor', []))
        tables['component_summary'] = stages_component_summary(export, cases, tables['ops_summary'],
            tables['decomposition_write'], tables['decomposition_read'], tables)
        tables['component_compare'] = stages_component_compare(tables['component_summary'])
        import sync_step
        tables['sync_step'] = sync_step.rows(export, cases, tables.get('ssd_write_floor', []), stages_route)
        names.append('sync_step')
        for c in cases:
            if not c['job'].get('group', '').startswith('G12'):
                continue
            v, reasons = verdict(c)
            if v == 'unqualified':
                tables['unqualified'].append({**floor_base(c), **stages_contract(c['job']),
                                              'verdict': v, 'reason_codes': reasons})
        names.append('unqualified')
        names.extend(('ops_summary', 'apples_to_apples', 'decomposition_write', 'decomposition_read', 'stall',
                      'transport_share', 'component_summary', 'component_compare'))
    output.mkdir(parents=True, exist_ok=True)
    for name in names:
        rows = tables[name]
        if name not in ('component_summary', 'component_compare'):
            rows.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=True))
        fields = list(dict.fromkeys(k for row in rows for k in row)) or ['case_ids']
        def text(value):
            if value is None: return 'missing'
            if isinstance(value, (list, dict)): return json.dumps(value, sort_keys=True, separators=(',', ':'))
            return str(value)
        with (output / (name + '.csv')).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader()
            for row in rows: writer.writerow({k: text(row.get(k)) for k in fields})
        if name == 'sync_step':
            lines = ['# Sync Step', '',
                     'Native means are total time divided by selected operations; percentiles are unavailable. '
                     'Owner percentiles use nearest rank over retained owner records with each field. '
                     'Append persistence is inclusive, not a second system call. '
                     'The overwrite floor is a separate population, not a WAL append reference. '
                     'No independently sampled populations are subtracted.', '']
            lines.extend(_md_table(fields, [[f"{row[key]:.3f}" if key in ('mean_us', 'p50_us', 'p99_us')
                                            and row.get(key) is not None else row.get(key)
                                            for key in fields] for row in rows]))
            (output / 'sync_step.md').write_text('\n'.join(lines) + '\n')
            continue
        if name in ('apples_to_apples', 'decomposition_write', 'decomposition_read', 'stall',
                    'component_summary', 'component_compare'):
            (output / (name + '.md')).write_text('\n'.join(stages_markdown(name, rows, tables)) + '\n')
            continue
        lines = ['# ' + name.replace('_', ' ').title(), '', f"Campaign complete: {all(c.get('status') == 'completed' for c in cases)}; completed {sum(c.get('status') == 'completed' for c in cases)}/{len(cases)}.", 'Success-only completed cases; repetitions are not pooled. Missing is not zero.', '', '| ' + ' | '.join(fields) + ' |', '| ' + ' | '.join('---' for _ in fields) + ' |']
        for row in rows:
            lines.append('| ' + ' | '.join(text(row.get(k)).replace('|', '\\|').replace('\n', ' ') for k in fields) + ' |')
        (output / (name + '.md')).write_text('\n'.join(lines) + '\n')
    if 'component_summary' in tables:
        (output / 'where_time_goes.md').write_text('\n'.join(where_time_goes(tables['component_summary'])) + '\n')
    summary = {'schema_version': 1, 'complete': all(c.get('status') == 'completed' for c in cases), 'planned_cases': len(cases), 'completed_cases': sum(c.get('status') == 'completed' for c in cases), 'failed_cases': sum(c.get('status') == 'failed' for c in cases), 'not_run_cases': sum(c.get('status') == 'not_run' for c in cases), 'interpretation': ['Each quantile is a strict-report success-only population, not a pooled sample.', 'Failed cases are excluded from comparison metrics even when their raw samples were admitted.', 'Repetition spreads are descriptive, not confidence intervals or significance claims.', 'Target cache and durability contracts differ; do not rank unmatched states.', 'Media evidence is a cohort proxy, not proof for each read.', 'A missing artifact or baseline is not zero.'], 'tables': {name: tables[name] for name in names}}
    (output / 'focused-summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + '\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', required=True); parser.add_argument('--report', required=True); parser.add_argument('--output', required=True)
    parser.add_argument('--baseline-root')
    args = parser.parse_args()
    generate(args.export, args.report, args.output, args.baseline_root)


if __name__ == '__main__':
    main()
