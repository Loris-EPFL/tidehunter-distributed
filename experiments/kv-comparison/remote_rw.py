#!/usr/bin/env python3
"""Analyze one remote-read/write export without pooling independent cases."""
import argparse
import csv
import json
import math
from pathlib import Path

import cloud_campaign
import focused_analysis

GROUPS = {'G14N', 'G14O1', 'G15C', 'G15M'}
FLOORS = {'G13W', 'G11S', 'G15F', 'G11N'}
FIELDS = ('case_id', 'arm', 'transport', 'group', 'size_bytes', 'readers', 'writers',
          'repetition', 'n', 'mean_us', 'p50_us', 'p99_us', 'calls_per_second',
          'mean_inflight_calls', 'offered_calls', 'unsuccessful_calls', 'floor_contract', 'status', 'error',
          'read_lanes', 'native_shards', 'durability_tier', 'native_durability')
CAPTION = ('Caption: Unmatched dimensions: durable mechanics and sync scope (Native page cache is not durable; '
           'Native barrier fsyncs registered files and directory per call, grouped barrier shares calls; owner '
           'synchronous uses covering fdatasync on an owner file); network in the data path (Native in-process, '
           'owner RPC); index (Native Db index versus coordinator index and owner history); routing (Native key '
           'hash versus owner operation ID); instances and SSD count (Native one process and one SSD versus '
           'coordinator plus one owner and one owner SSD). Floors are separate populations, not components.')
WRITE_FIELDS = ('value_size', 'concurrency', 'repeat', 'keys', 'distribution', 'measurement_operations',
                'warmup_operations', 'mix', 'offered_ops_per_second', 'put_key_policy', 'cohort', 'diagnostics')


def _arm(job):
    if job.get('backend') == 'native':
        return 'native', 'in-process'
    if job.get('backend') == 'tidehunter_tcp':
        return 'owner', 'tcp'
    if job.get('backend') == 'tidehunter_verbs_native':
        return 'owner', 'verbs'
    return job.get('backend', 'unknown'), 'unknown'


def _media(case, export):
    evidence = (case.get('focused_evidence') or {}).get('media_evidence') or {}
    path = export / 'cases' / case['case_id'] / 'media_evidence.json'
    if path.is_file():
        evidence = json.loads(path.read_text())
    return evidence


def _case_row(case, export):
    job = case['job']
    group = job['group']
    read = group in ('G15C', 'G15M')
    arm, transport = _arm(job)
    if group == 'G14N':
        arm = 'native ' + str(job.get('native_durability'))
    status = case.get('status', 'not_run')
    evidence = _media(case, export) if group == 'G15C' else {}
    device = evidence.get('read_ops_per_success')
    if group == 'G15C' and status == 'completed' and (not isinstance(device, (int, float)) or
            not math.isfinite(device) or device < .9 or evidence.get('status') != 'qualified'):
        status = 'unqualified'
    row = dict.fromkeys(FIELDS)
    row.update(case_id=case['case_id'], arm=arm, transport=transport, group=group,
               size_bytes=job.get('value_size'), readers=job.get('concurrency') if read else None,
               writers=None if read else job.get('concurrency'), repetition=job.get('repeat'),
               status=status, error=case.get('error'), read_lanes=job.get('read_lanes'),
               native_shards=job.get('native_shards'), durability_tier=job.get('durability_tier'),
               native_durability=job.get('native_durability'))
    if status == 'completed':
        pop = focused_analysis.population(case, 'read' if read else 'put')
        metrics = case.get('metrics') or {}
        mean = pop.get('api_mean_ns')
        timeline = metrics.get('measurement_timeline') or {}
        inflight = timeline.get('mean_observed_api_concurrency')
        if inflight is None and timeline.get('api_time_sum_ns') is not None and timeline.get('duration_ns'):
            inflight = timeline['api_time_sum_ns'] / timeline['duration_ns']
        row.update(n=pop.get('count'), mean_us=mean / 1000 if mean is not None else None,
                   p50_us=focused_analysis.latency(pop, 'api', 'p50'),
                   p99_us=focused_analysis.latency(pop, 'api', 'p99'),
                   calls_per_second=metrics.get('success_calls_per_second'),
                   mean_inflight_calls=inflight, offered_calls=metrics.get('offered_calls'),
                   unsuccessful_calls=metrics.get('unsuccessful_calls'))
    return row, device, evidence.get('reason_codes', [])


def _floor_rows(case, export):
    job = case['job']; group = job['group']; status = case.get('status', 'not_run')
    evidence = case.get('focused_evidence') or {}
    base = dict.fromkeys(FIELDS)
    base.update(case_id=case['case_id'], group=group, repetition=job.get('repeat'), status=status,
                error=case.get('error'), read_lanes=job.get('read_lanes'),
                native_shards=job.get('native_shards'), durability_tier=job.get('durability_tier'),
                native_durability=job.get('native_durability'))
    if group == 'G11N':
        spec = job.get('net') or {}; result = evidence.get('net_floor_result') or {}
        runs = {run.get('size_bytes'): run for run in result.get('runs', [])}
        rows = []
        for size in spec.get('sizes', []):
            run = runs.get(size) or {}; ok = status == 'completed' and run.get('status') == 'completed'
            values = run.get('latency_us') or {} if ok else {}
            row = dict(base, arm=spec.get('tool'), transport=spec.get('interface'), size_bytes=size,
                       status=status if status != 'completed' else ('completed' if ok else 'missing'),
                       floor_contract=json.dumps({'spec': spec, 'latency_semantics': run.get('latency_semantics')}, sort_keys=True),
                       mean_us=values.get('avg'), p50_us=values.get('p50'), p99_us=values.get('p99'))
            rows.append((row, {'spec': spec, 'latency_semantics': run.get('latency_semantics')}))
        return rows
    spec = job.get('ssd_write' if group == 'G13W' else 'ssd') or {}
    data = evidence.get('ssd_write_floor_result' if group == 'G13W' else 'ssd_floor_result') or {}
    row = dict(base, arm=('client' if group == 'G15F' or spec.get('host') == 'client' or spec.get('host_index') == -1 else 'owner 0'),
               transport='local', size_bytes=spec.get('block_bytes'), floor_contract=json.dumps(spec, sort_keys=True))
    if status == 'completed' and not data:
        row['status'] = 'missing'
    if data and status == 'completed':
        if group == 'G13W':
            series = data.get('total') or {}
            row.update(n=data.get('logged_operations'), **{k + '_us': series.get(k) / 1000 if series.get(k) is not None else None for k in ('mean', 'p50', 'p99')})
            if data.get('elapsed_seconds'):
                row['calls_per_second'] = data.get('total_operations', 0) / data['elapsed_seconds']
        else:
            row.update(n=data.get('read_ios'), p50_us=focused_analysis.latency(data, 'latency', 'p50'),
                       p99_us=focused_analysis.latency(data, 'latency', 'p99'))
            if data.get('elapsed_seconds') and data.get('read_ios') is not None:
                row['calls_per_second'] = data['read_ios'] / data['elapsed_seconds']
            log = (data.get('lat_log') or {}).get('path')
            path = export / 'cases' / case['case_id'] / log if log else None
            if path and path.is_file():
                stats = focused_analysis.ssd_log_metrics(path, spec['region_bytes'], spec['runtime_seconds'])
                row.update(n=stats.get('n'), mean_us=stats.get('mean_us'), p50_us=stats.get('p50_us'))
            else:
                row['status'] = 'missing'
            verdict, reasons = focused_analysis.verdict(case)
            if verdict == 'unqualified':
                row['status'] = 'unqualified'
                row['error'] = ', '.join(reasons)
    return [(row, {'spec': spec, 'qualification': data.get('qualification'), 'sampling': data.get('sampling'),
                    'summary_population': data.get('summary_population')})]


def _equal(a, b, fields):
    return all(a.get(field) == b.get(field) for field in fields)




def _table(lines, title, headers, data, read_caption=False):
    caption = CAPTION + (' Owner read lanes: 1 at 1 and 5 readers, 4 at 16 readers; Native reads are in-process (0 lanes).'
                         if read_caption else '')
    lines.extend(['## ' + title, '', caption])
    lines.extend(focused_analysis._md_table(headers, data))
    lines.append('')


def _display(value):
    return '—' if value is None else str(value).replace('|', '\\|').replace('\n', ' ')


def generate(export, report, output):
    export, report, output = map(Path, (export, report, output))
    matrix = json.loads((export / 'matrix.json').read_text())
    jobs = matrix['jobs']
    cases = json.loads((report / 'comparison.json').read_text())['cases']
    planned = {j['case_id']: j for j in jobs}
    if (len(planned) != len(jobs) or len(cases) != len(jobs) or
            len({c['case_id'] for c in cases}) != len(cases) or
            any(c.get('job') != planned.get(c.get('case_id')) for c in cases)):
        raise ValueError('report cases do not match export matrix')
    cases = [c for c in cases if c['job'].get('group') in GROUPS | FLOORS]
    rows, cold, floors = [], [], []
    for case in cases:
        if case['job']['group'] in FLOORS:
            floors.extend(_floor_rows(case, export))
        else:
            row, device, reasons = _case_row(case, export)
            rows.append(row)
            if row['group'] == 'G15C':
                cold.append((row, device, reasons))
    by_id = {c['case_id']: c for c in cases}
    lines = ['# Remote read and write parity', '',
             'One export represents one fleet; each case and repetition is independent, never pooled. '
             'Latency is successful API calls, nearest-rank percentiles as supplied by analyze.py; '
             'whole-case rate includes time outside API calls. Cold qualification uses device reads per '
             'successful call >= 0.9 and the receipt verdict; this proxy is not proof of physical access. '
             'No significance or general ranking is implied.', '']
    if (matrix.get('spec') or {}).get('selection') == 'remote_rw_compact':
        violations = cloud_campaign.parity_violations(jobs)
        lines.extend(['Full matrix parity: ' + ('passed' if not violations else
                     f'{len(violations)} violations: ' + '; '.join(map(str, violations))), ''])
    for group, title in [('G14N', 'Native write parity'), ('G14O1', 'Owner write parity'),
                         ('G15C', 'Cold read parity (qualified only)'), ('G15M', 'Warm read parity')]:
        subset = [r for r in rows if r['group'] == group]
        table = [[_display(r.get(k)) for k in FIELDS] for r in subset]
        _table(lines, title, FIELDS, table, read_caption=group in ('G15C', 'G15M'))
    _table(lines, 'Cold device qualification', ('case_id', 'arm', 'transport', 'size_bytes', 'readers', 'repetition', 'status', 'device reads per call', 'reasons'),
           [[_display(r['case_id']), _display(r['arm']), _display(r['transport']), _display(r['size_bytes']),
             _display(r['readers']), _display(r['repetition']), _display(r['status']), _display(device),
             _display(', '.join(reasons))] for r, device, reasons in cold], read_caption=True)
    for group, title in [('G13W', 'Append floors'), ('G11S', 'Owner 0 SSD read floors'),
                         ('G15F', 'Client SSD read floors'), ('G11N', 'Network floors')]:
        subset = [(r, meta) for r, meta in floors if r['group'] == group]
        if not subset:
            lines.extend([f'{title}: not run (not present in matrix).', ''])
        _table(lines, title, FIELDS + ('floor contract and sampling',),
               [[*[_display(r.get(k)) for k in FIELDS], _display(json.dumps(meta, sort_keys=True))] for r, meta in subset])
    ratios = []; matched = missing = 0
    for owner in (r for r in rows if r['group'] in ('G14O1', 'G15C', 'G15M') and r['transport'] != 'in-process'):
        job = by_id[owner['case_id']]['job']; read = owner['group'] != 'G14O1'
        peers = [r for r in rows if r['group'] == (owner['group'] if read else 'G14N') and
                 r['transport'] == 'in-process' and r['size_bytes'] == owner['size_bytes'] and
                 r['repetition'] == owner['repetition'] and (r['readers'] if read else r['writers']) == (owner['readers'] if read else owner['writers'])]
        if not peers:
            missing += 1
            ratios.append([owner['case_id'], '—', 'missing partner', '—', 'no same size, concurrency and repetition'])
            continue
        for native in peers:
            njob = by_id[native['case_id']]['job']
            group_jobs = [by_id[r['case_id']]['job'] for r in rows
                          if r['group'] in ((owner['group'],) if read else ('G14N', 'G14O1'))
                          and r['size_bytes'] == owner['size_bytes']
                          and (r['readers'] if read else r['writers']) == (owner['readers'] if read else owner['writers'])
                          and r['repetition'] == owner['repetition']]
            violations = cloud_campaign.parity_violations(group_jobs, complete=False)
            if not read and (not _equal(job, njob, WRITE_FIELDS) or
                             (njob.get('native_shards') or 1) != 1):
                violations = [*violations, 'write contract mismatch']
            if violations:
                missing += 1
                ratios.append([owner['case_id'], native['case_id'], 'unmatched', '—', _display('; '.join(map(str, violations)))])
                continue
            matched += 1
            eligible = owner['status'] == native['status'] == 'completed'
            ratio = owner['mean_us'] / native['mean_us'] if eligible and owner['mean_us'] is not None and native['mean_us'] else None
            ratios.append([owner['case_id'], native['case_id'], 'matched' if eligible else 'not available', _display(ratio), 'owner mean / Native mean'])
    failed = sum(c.get('status') == 'failed' for c in cases)
    lines.extend([f'Parity gate: {matched} matched pairs; {missing} missing or unmatched partners; {failed} failed cases.', ''])
    _table(lines, 'Matched mean ratios', ('owner case', 'Native case', 'parity', 'ratio', 'detail'), ratios)
    decomposition = []
    for r in rows:
        if r['transport'] not in ('tcp', 'verbs'):
            continue
        network = [f for f, meta in floors if f['group'] == 'G11N' and f['size_bytes'] == r['size_bytes'] and f['repetition'] == r['repetition'] and (meta['spec'].get('tool') == ('tcp_rtt' if r['transport'] == 'tcp' else 'ib_send_lat'))]
        disk_group = 'G11S' if r['readers'] is not None else 'G13W'
        disk = [f for f, meta in floors if f['group'] == disk_group and f['arm'] == 'owner 0' and f['size_bytes'] == r['size_bytes'] and f['repetition'] == r['repetition']]
        def describe(items):
            return '; '.join(f"{x['case_id']} mean={_display(x['mean_us'])} p50={_display(x['p50_us'])} status={x['status']} contract={x['floor_contract']}" for x in items) or 'missing'
        decomposition.append([r['case_id'], _display(r['mean_us']), describe(network), describe(disk), 'not computed: separate populations'])
    _table(lines, '[INFERENCE] Owner mean and separate floors (µs)', ('case_id', 'API mean', 'network floor', 'owner SSD floor', 'software remainder'), decomposition)
    output.mkdir(parents=True, exist_ok=True)
    (output / 'remote_rw.md').write_text('\n'.join(lines) + '\n')
    families = {'latency_vs_size': rows, 'latency_vs_concurrency': rows,
                'throughput_vs_concurrency': rows, 'cold_vs_warm': [r for r in rows if r['group'] in ('G15C', 'G15M')],
                'floors': [r for r, _ in floors], 'parity_matrix': rows}
    for name, entries in families.items():
        with (output / (name + '.csv')).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader(); writer.writerows(entries)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', required=True); parser.add_argument('--report', required=True); parser.add_argument('--output', required=True)
    args = parser.parse_args()
    generate(args.export, args.report, args.output)


if __name__ == '__main__':
    main()
