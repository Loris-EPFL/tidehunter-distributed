#!/usr/bin/env python3
"""Create clearly labelled test data, never experimental evidence.

Uses reviewed jobs, but no analyzer/test fixture helpers. No cloud or benchmark runs.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--exercise-statuses', action='store_true', help='include invented failed, not-run and unqualified cases')
    args = parser.parse_args()
    root = args.output.resolve()
    if 'SYNTHETIC' not in root.name:
        parser.error('output directory basename must contain SYNTHETIC')
    if root.exists():
        parser.error('use a new output directory; never overwrite an export')
    sys.path.insert(0, str(args.repo / 'experiments/kv-comparison'))
    import cloud_campaign
    jobs = cloud_campaign.make_focused_matrix('remote_rw_compact')
    if len(jobs) != 342:
        raise ValueError('expected the reviewed 342-job matrix (including client SSD floors)')
    root.mkdir(parents=True)
    export, report = root / 'export', root / 'report'
    export.mkdir(); report.mkdir()
    (root / 'SYNTHETIC').write_text('SYNTHETIC: deterministic invented latencies. Not measured.\n')
    cases = []
    for job in jobs:
        group = job['group']; size = job['value_size']; c = job['concurrency']; rep = job['repeat']
        case = dict(case_id=job['case_id'], job=job, status='completed', focused_evidence={})
        folder = export / 'cases' / job['case_id']; folder.mkdir(parents=True)
        if group in ('G14N', 'G14O1', 'G15C', 'G15M', 'G14P'):
            owner = job['backend'] != 'native'
            base = (110 if group == 'G15C' else 4 if group == 'G15M' else
                    {'page_cache': 3, 'value_wal_barrier': 150, 'value_wal_group_barrier': 85}.get(job.get('native_durability'), 100))
            if owner:
                base = (125 if group == 'G15C' else 16 if group == 'G15M' else 175)
                base *= 1.15 if job['backend'] == 'tidehunter_tcp' else 1
            base *= (1 + size / 180000) * (1 + .035 * (c - 1)) * (1 + .07 * rep)
            # Four invented samples expanded with equal multiplicity; exact nearest-rank statistics.
            values = [round(base * f * 1000) for f in (.72, .9, 1.04, 1.8)]
            n = job['measurement_operations']
            mean = sum(values) / len(values); inflight = c * .78
            case['metrics'] = dict(success_calls_per_second=inflight * 1e9 / mean,
                                   offered_calls=n, unsuccessful_calls=0,
                                   measurement_timeline={'mean_observed_api_concurrency': inflight},
                                   populations=[dict(operation='read' if group in ('G15C', 'G15M') else 'put',
                                                     outcome='success', count=n, api_mean_ns=mean,
                                                     api_ns={'p50': values[1], 'p99': values[-1]})])
            if group == 'G15C':
                media = dict(status='qualified', read_ops_per_success=1.02 + .03 * rep, reason_codes=[])
                case['focused_evidence']['media_evidence'] = media
                (folder / 'media_evidence.json').write_text(json.dumps(media))
        elif group == 'G13W':
            spec = job['ssd_write']; size = spec['block_bytes']
            base = (75 + size / 2000) * (1.1 if spec['host'] == 'owner0' else 1) * (1 + .04 * rep)
            if spec['mode'] == 'fdatasync': base *= .8
            result = dict(total={'mean': base * 1000, 'p50': base * 900, 'p99': base * 1800},
                          logged_operations=4096, total_operations=4096, elapsed_seconds=1)
            case['focused_evidence']['ssd_write_floor_result'] = result
        elif group in ('G11S', 'G15F'):
            spec = job['ssd']; size = spec['block_bytes']
            base = (70 + size / 2200) * (1.06 if group == 'G11S' else 1) * (1 + .03 * rep)
            values = [round(base * f * 1000) for f in (.85, .95, 1.05, 1.15)]
            (folder / 'SYNTHETIC-lat.log').write_text(''.join(
                f'{i * 1000},{value},0,{size},{i * size}\n' for i, value in enumerate(values)))
            case['focused_evidence']['ssd_floor_result'] = dict(
                read_ios=4, elapsed_seconds=6, latency_ns={'p50': values[1], 'p99': values[-1]},
                lat_log={'path': 'SYNTHETIC-lat.log'}, qualification={'status': 'qualified'})
        elif group == 'G11N':
            spec = job['net']; tool = spec['tool']
            base = 15 if tool == 'tcp_rtt' else 3
            semantics = 'half_round_trip' if tool in ('ib_send_lat', 'ib_write_lat') else 'round_trip'
            case['focused_evidence']['net_floor_result'] = {'runs': [dict(
                size_bytes=s, status='completed', latency_semantics=semantics,
                latency_us={'avg': (base + s / 10000) * (1 + .03 * rep),
                            'p50': (base + s / 10000) * .9 * (1 + .03 * rep),
                            'p99': (base + s / 10000) * 1.8 * (1 + .03 * rep)}) for s in spec['sizes']]}
        if args.exercise_statuses:
            if group == 'G15C' and job['backend'] == 'native' and size == 1024 and c == 16 and rep == 1:
                media = dict(status='unqualified', read_ops_per_success=.3, reason_codes=['SYNTHETIC-device-ratio-low'])
                case['focused_evidence']['media_evidence'] = media
                (folder / 'media_evidence.json').write_text(json.dumps(media))
            if group == 'G15M' and job['backend'] == 'tidehunter_tcp' and size == 65536 and c == 5 and rep == 1:
                case.update(status='failed', error='SYNTHETIC failure')
            if group == 'G14O1' and job['backend'] == 'tidehunter_tcp' and size == 8192 and c == 16 and rep == 1:
                case.update(status='not_run', error='SYNTHETIC not-run control')
            if group == 'G11N' and job['net']['tool'] == 'ib_send_lat' and job['net']['mode'] == 'polled' and rep == 1:
                case.update(status='failed', error='SYNTHETIC floor failure', focused_evidence={})
        cases.append(case)
    matrix = json.dumps({'selection': 'remote_rw_compact', 'synthetic': True, 'jobs': jobs}, indent=2)
    (export / 'matrix.json').write_text(matrix + '\n')
    (report / 'comparison.json').write_text(json.dumps({'synthetic': True, 'cases': cases}, indent=2) + '\n')
    provenance = dict(synthetic=True, run_id='SYNTHETIC-run-9', package_sha256='SYNTHETIC-not-a-package',
                      source_revision='SYNTHETIC-not-a-revision', source_dirty=True,
                      launch_time='SYNTHETIC-not-launched', vm_names=['SYNTHETIC-coordinator', 'SYNTHETIC-owner0'],
                      selection='remote_rw_compact', matrix_sha256=hashlib.sha256(matrix.encode()).hexdigest())
    (root / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    print(f'SYNTHETIC only: {len(jobs)} jobs; export={export}; report={report}')


if __name__ == '__main__':
    main()
