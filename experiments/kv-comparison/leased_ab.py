#!/usr/bin/env python3
"""Report compact leased A/B evidence without pooling repetitions."""
import argparse
import csv
import json
from pathlib import Path

import analyze
import focused_analysis

INLINE = ('live_inline', 'live_inline_fallback_drain', 'live_inline_fallback_busy',
          'live_inline_fallback_grant', 'live_inline_fallback_credit',
          'live_inline_fallback_paused', 'live_inline_fallback_coalesced')
LEASED = ('leased_frames', 'leased_skip_markers', 'fua_writes', 'lease_exchanges', 'adoption_async_exchanges',
          'adoption_sync_exchanges', 'adoption_history_full_settles', 'leased_failed_writes',
          'leased_short_writes', 'leased_timed_out_writes')
FIELDS = ('case_id', 'group', 'arm', 'operation', 'diagnostic', 'tier', 'size_bytes', 'concurrency', 'repetition',
          'cache_contract', 'acknowledgement_contract', 'preload_write_mode', 'preload_durability_tier',
          'status', 'error', 'offered', 'successful',
          'rejected', 'mean_us', 'p50_us', 'p99_us', 'p999_us', 'calls_ge_5ms', 'time_ge_5ms_share',
          'calls_per_second', 'mean_inflight_calls')
COUNTERS = ('case_id', 'tier', 'size_bytes', 'concurrency', 'repetition', 'cache_contract',
            'acknowledgement_contract', 'status', 'frames_per_put') + LEASED + (
                'adoption_async_mean_us', 'adoption_settle_samples', 'adoption_settle_mean_us',
                'adoption_settle_total_ms')
READS = FIELDS + INLINE


def _load(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def _arm(job):
    if job['backend'] == 'tidehunter_verbs_native':
        return 'owner RPC'
    if job['backend'] != 'tidehunter_nvme_rdma' or job.get('nvmeof_route') != 'live':
        return 'unknown'
    if job['group'] == 'G17W':
        return job.get('live_write_mode', 'unknown')
    return 'inline on' if job.get('live_inline_reads') is True else 'inline off' if job.get('live_inline_reads') is False else 'unknown'


def _mean_stage(counters, stage):
    samples, total = counters.get(stage + '_samples'), counters.get(stage + '_ns')
    if samples is None or total is None or not samples:
        return 'not available'
    return total / samples / 1000


def _row(case, export):
    job = case['job']; arm = _arm(job); tier = job.get('durability_tier')
    directory = export / 'cases' / case['case_id'] / 'result'
    live_route = job['backend'] == 'tidehunter_nvme_rdma' and job.get('nvmeof_route') == 'live'
    mode = job.get('live_write_mode')
    live_ack = {
        ('leased', 'native'): 'leased extent: page-cache write',
        ('leased', 'synchronous'): 'synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous',
        ('reserved', 'native'): 'reserved frame: owner validated page-cache write',
        ('reserved', 'synchronous'): 'reserved frame: owner WAL and frontier root synchronized',
    }.get((mode, tier))
    if job['group'] == 'G17R':
        cache = ('target page cache through kernel NVMe-oF' if live_route else
                 'owner WAL through its page cache' if job['backend'] == 'tidehunter_verbs_native' else 'not available')
        ack = 'not applicable to read'
    else:
        cache = json.dumps({key: job.get(key) for key in ('coordinator_cache_bytes', 'drop_os_caches', 'warmup_key_sweep', 'target_io')}, sort_keys=True)
        ack = (live_ack if live_route else
               'owner root synchronized' if job['backend'] == 'tidehunter_verbs_native' and tier == 'synchronous' else
               'owner page-cache write' if job['backend'] == 'tidehunter_verbs_native' and tier == 'native' else 'not available')
    row = dict.fromkeys(FIELDS)
    operation = next((name for name, amount in job.get('mix', {}).items() if amount), 'put' if job['group'] == 'G17W' else 'read')
    row.update(case_id=case['case_id'], group=job['group'], arm=arm, operation=operation,
               diagnostic=bool(job.get('diagnostics')), tier=tier,
               size_bytes=job.get('value_size'), concurrency=job.get('concurrency'), repetition=job.get('repeat'),
               cache_contract=cache, acknowledgement_contract=ack,
               preload_write_mode=(mode if live_route else 'owner RPC') if job['group'] == 'G17R' else 'not applicable',
               preload_durability_tier=tier if job['group'] == 'G17R' else 'not applicable',
               status=case.get('status', 'not_run'), error=case.get('error'))
    boundary = _load(directory / 'measurement-read-evidence.json')
    counters = ((boundary.get('live_nvmeof') or {}).get('counters') or
                ((_load(directory / 'write-diagnostics.json').get('profile') or {}).get('stages') or {}).get('live_nvmeof') or {})
    read_counters = (((boundary.get('deltas') or {}).get('coordinator') or {}).get('counters') or
                     ((_load(directory / 'read-diagnostics.json').get('profile') or {}).get('counters') or {}))
    inline = {name: read_counters.get('read_profile_' + name) for name in INLINE}
    reasons = []
    if arm == 'unknown':
        reasons.append('unknown arm')
    if row['status'] == 'completed':
        manifest = _load(directory / 'manifest.json')
        configuration = (manifest.get('backend_info') or {}).get('configuration') or {}
        live = configuration.get('live_write') or {}
        if not manifest:
            reasons.append('executed manifest unavailable')
        if job['backend'] == 'tidehunter_nvme_rdma':
            if live.get('write_mode') != job.get('live_write_mode'):
                reasons.append('executed live write mode differs or unavailable')
            observed_ack = live.get('acknowledgement_contract')
            # Archived per-frame-FUA binaries used this narrower, truthful label.
            archived_fua = (mode == 'leased' and tier == 'synchronous'
                            and job.get('live_leased_sync', 'per_frame_fua') == 'per_frame_fua'
                            and observed_ack == 'leased extent: FUA data sync; owner frontier root asynchronous')
            if live_ack is None or (observed_ack != live_ack and not archived_fua):
                reasons.append('executed acknowledgement contract differs or unavailable')
            elif job['group'] != 'G17R':
                row['acknowledgement_contract'] = observed_ack
            if job['group'] == 'G17R' and live.get('inline_reads') is not job.get('live_inline_reads'):
                reasons.append('executed inline setting differs or unavailable')
        elif job['backend'] == 'tidehunter_verbs_native':
            observed_ack = (manifest.get('backend_info') or {}).get('acknowledgement')
            owner_ack = {
                'native': 'RF1 complete batch and all earlier batches appended to the configured owner Value-WAL (page cache); background fsync, explicit flush; no replication',
                'synchronous': 'RF1 complete Value-WAL batch persisted (covering fsync complete) on the configured owner; no replication',
            }.get(tier)
            if observed_ack is not None and observed_ack != owner_ack:
                reasons.append('executed owner acknowledgement contract differs')
        samples = list(analyze.read_samples(directory / 'samples.csv'))
        if not samples:
            raise ValueError('completed case has no measurement samples')
        good = [s['api_ns'] for s in samples if s['operation'] == operation and s['outcome'] in ('ok', 'success') and s['api_ns'] is not None]
        metrics = case.get('metrics') or {}
        elapsed_ns = max(s['completed_ns'] for s in samples) - min(s['scheduled_ns'] for s in samples)
        if elapsed_ns <= 0:
            raise ValueError('measurement exposure must be positive')
        row.update(offered=len(samples), successful=len(good), rejected=sum(s['outcome'] == 'rejected' for s in samples),
                   mean_us=sum(good) / len(good) / 1000 if good else None,
                   p50_us=analyze.quantile(good, .5) / 1000 if good else None,
                   p99_us=analyze.quantile(good, .99) / 1000 if good else None,
                   p999_us=analyze.quantile(good, .999) / 1000 if good else None,
                   calls_per_second=metrics.get('success_calls_per_second'),
                   mean_inflight_calls=sum(good) / elapsed_ns if good else None)
        if job['group'] == 'G17W' and good:
            long_calls = [value for value in good if value >= 5_000_000]
            total = sum(good)
            row['calls_ge_5ms'] = len(long_calls)
            row['time_ge_5ms_share'] = f"{sum(long_calls) / total:.3f}" if total else 'not available'
        if arm == 'leased':
            for name in ('reserve_exchanges', 'complete_exchanges'):
                if counters.get(name) != 0:
                    reasons.append(name + ' is nonzero or unavailable')
            frames, skips, fua = (counters.get(k) for k in ('leased_frames', 'leased_skip_markers', 'fua_writes'))
            if tier == 'synchronous' and (frames is None or skips is None or fua != frames + skips):
                reasons.append('FUA writes differ from leased frames plus skip markers or unavailable')
            if tier == 'native' and fua != 0:
                reasons.append('native FUA writes nonzero or unavailable')
            for name in ('leased_failed_writes', 'leased_short_writes', 'leased_timed_out_writes'):
                if counters.get(name) != 0:
                    reasons.append(name + ' is nonzero or unavailable')
        if arm == 'inline off' and inline['live_inline'] != 0:
            reasons.append('inline admissions nonzero or unavailable')
    if reasons:
        row['status'] = 'failed'; row['error'] = '; '.join(reasons)
    counter_row = {k: row.get(k) for k in COUNTERS}
    counter_row.update({k: counters.get(k) for k in LEASED})
    counter_row['frames_per_put'] = counters.get('leased_frames') / row['successful'] if counters.get('leased_frames') is not None and row['successful'] else None
    for stage in ('adoption_async', 'adoption_settle'):
        counter_row[stage + '_mean_us'] = _mean_stage(counters, stage)
    settle_samples = counters.get('adoption_settle_samples')
    settle_total = counters.get('adoption_settle_ns')
    counter_row['adoption_settle_samples'] = settle_samples
    counter_row['adoption_settle_total_ms'] = (
        settle_total / 1_000_000 if settle_samples and settle_total is not None else 'not available')
    return row | inline, counter_row


def _table(lines, title, fields, rows):
    lines.extend(['## ' + title, ''])
    lines.extend(focused_analysis._md_table(fields, [[str(row.get(k)).replace('|', '\\|').replace('\n', ' ') if row.get(k) is not None else 'not available' for k in fields] for row in rows]))
    lines.append('')

def _side_by_side(rows, arms):
    cells = {}
    for row in rows:
        key = (row['tier'], row['size_bytes'], row['concurrency'], row['repetition'], row['operation'],
               row['case_id'] if row.get('diagnostic') else '')
        cells.setdefault(key, {})[row['arm']] = row
    fields = ('tier', 'size_bytes', 'concurrency', 'repetition', 'operation', 'diagnostic_case') + arms
    result = []
    for key, cell in sorted(cells.items()):
        item = dict(zip(fields[:6], key))
        for arm in arms:
            r = cell.get(arm)
            if r is not None:
                item[arm] = (f"{r['case_id']}: status={r['status']}; mean/p50/p99/p99.9 us="
                             f"{r['mean_us']}/{r['p50_us']}/{r['p99_us']}/{r['p999_us']}; "
                             f"calls/s={r['calls_per_second']}; mean in-flight={r['mean_inflight_calls']}; "
                             f"calls >=5ms={r['calls_ge_5ms']}; time share >=5ms={r['time_ge_5ms_share']}; "
                             f"offered/success/rejected={r['offered']}/{r['successful']}/{r['rejected']}; "
                             f"cache={r['cache_contract']}; ack={r['acknowledgement_contract']}; "
                             f"preload mode/tier={r['preload_write_mode']}/{r['preload_durability_tier']}")
        result.append(item)
    return fields, result


def generate(export, report, output):
    export, report, output = map(Path, (export, report, output))
    matrix = json.loads((export / 'matrix.json').read_text()); jobs = matrix['jobs']
    if matrix.get('spec', {}).get('selection') != 'leased_ab_compact':
        raise ValueError('export selection is not leased_ab_compact')
    cases = json.loads((report / 'comparison.json').read_text())['cases']
    planned = {job['case_id']: job for job in jobs}
    if (len(planned) != len(jobs) or len(cases) != len(jobs) or len({c['case_id'] for c in cases}) != len(cases)
            or any(c.get('job') != planned.get(c.get('case_id')) for c in cases)):
        raise ValueError('report cases do not match export matrix')
    if any(j.get('group') not in ('G17W', 'G17R') for j in jobs):
        raise ValueError('unexpected case group')
    pairs = [_row(case, export) for case in cases]
    writes = [r for r, _ in pairs if r['group'] == 'G17W']
    reads = [r for r, _ in pairs if r['group'] == 'G17R']
    counters = [c for r, c in pairs if r['group'] == 'G17W' and r['arm'] == 'leased']
    regression = [r for r in writes if r['arm'] == 'owner RPC' and r['tier'] == 'native' and r['size_bytes'] == 1024 and r['concurrency'] == 1]
    lines = ['# Compact leased A/B', '', 'One row per case population and repetition; successful raw API samples use exact nearest-rank quantiles. Rates cover the case; mean in-flight calls use successful API time divided by the raw measurement span (first scheduled arrival to last completion). No ranking across acknowledgement or cache contracts.', '',
             'Synchronous owner RPC acknowledges after the owner root; synchronous leased acknowledges after the FUA write while owner root adoption is asynchronous. Native acknowledges at the target page-cache write.', '',
             'Each 4096-put case contains about four owner reference-history spills on either route; compare the mean, the long-call columns and the settle columns, not only percentiles.', '']
    for title, data, arms in [('Write arms side by side', writes, ('owner RPC', 'reserved', 'leased')),
                              ('Read arms side by side', reads, ('owner RPC', 'inline off', 'inline on'))]:
        fields, matched = _side_by_side(data, arms)
        _table(lines, title, fields, matched)
    for title, fields, rows in [('Writes', FIELDS, writes), ('Leased counters', COUNTERS, counters), ('Reads', READS, reads), ('Owner RPC regression', FIELDS, regression)]:
        _table(lines, title, fields, rows)
    lines.extend(['Run-8 reference: owner verbs native, five-owner fleet, 1 KiB concurrency 1; mean 78.2, p50 74.4, p99 127.4, p99.9 317.0 microseconds. Context from another fleet, not a matched comparison.', ''])
    output.mkdir(parents=True, exist_ok=True)
    (output / 'leased_ab.md').write_text('\n'.join(lines))
    for name, fields, rows in [('leased_writes', FIELDS, writes), ('leased_counters', COUNTERS, counters), ('leased_reads', READS, reads), ('leased_regression', FIELDS, regression)]:
        with (output / (name + '.csv')).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore'); writer.writeheader(); writer.writerows(rows)
    return [r for r, _ in pairs]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', required=True); parser.add_argument('--report', required=True); parser.add_argument('--output', required=True)
    args = parser.parse_args(); generate(args.export, args.report, args.output)


if __name__ == '__main__':
    main()
