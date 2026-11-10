#!/usr/bin/env python3
"""Extract per-run curve evidence and topology. Never pool calls or edit sources."""
import csv
import hashlib
import json
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
FULL = ROOT / 'checkpoint-work/spdk-matched-storage-20261105T004442Z/analysis/gcp-full-matrix-2026-11-07/report-fixed'
SOURCES = {
    'nov03': 'checkpoint-work/gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/input/comparison.json',
    'nov04-0657': 'checkpoint-work/gcp-manual-combined-prep-20261104/manual-live-nvmeof-native-counters-20261104T065739Z/controller-expanded/comparison.csv',
    'nov04-1427': 'checkpoint-work/gcp-manual-combined-prep-20261104/manual-live-nvmeof-read-observation-20261104T142730Z/canonical-terminal/expanded/comparison.csv',
    'nov04-1812': 'checkpoint-work/gcp-manual-nvme-diag-prep-20261104/manual-live-nvmeof-delete-observed-20261104T181245Z/canonical-terminal/expanded/comparison.csv',
    'queue': 'checkpoint-work/spdk-matched-storage-20261105T004442Z/gcp-queue-bounded-campaign-20261106T032027Z/final-retrieval/extract/comparison.csv',
    'final': str(FULL.relative_to(ROOT) / 'comparison.csv'),
}
PROFILES = {'write-concurrent': 'put', 'read-concurrent': 'read', 'read-many32': 'get_many', 'point-mixed': None}
METRICS = ('api_p50_us', 'api_p99_us', 'response_p99_us', 'api_mean_us', 'case_success_calls_per_second')


def rows(path):
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = json.loads((HERE / 'inputs.json').read_text())

    def source(path):
        manifest['sources'][str(path)] = sha(path)
        return path

    def output(name, records):
        path = HERE / name
        with path.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
        manifest['outputs'][name] = sha(path)

    source(HERE / 'number-baseline.json')
    populations = rows(HERE / 'populations.csv')
    external = {'valkey_tcp', 'valkey_rdma', 'garnet', 'kvrocks', 'cassandra', 'tikv', 'foundationdb'}
    reports = {}
    for run, relative in SOURCES.items():
        path = source(ROOT / relative)
        if path.suffix == '.csv':
            reports[run] = rows(path)
        else:
            flat = []
            for case in json.loads(path.read_text())['cases']:
                if case['status'] != 'completed' or not case.get('metrics'):
                    continue
                job, metrics = case['job'], case['metrics']
                for pop in metrics['populations']:
                    if pop['outcome'] != 'success':
                        continue
                    flat.append(dict(case_id=case['case_id'], status=case['status'], backend=job['backend'],
                        cohort=job['cohort'], profile=job['profile'], value_bytes=job['value_size'],
                        keys=job['keys'], concurrency=job['concurrency'], offered_ops_per_second=job.get('offered_ops_per_second'),
                        operation=pop['operation'], outcome=pop['outcome'], population_calls=pop['count'],
                        observed_topology=case['backend']['topology'],
                        elapsed_seconds=metrics['elapsed_seconds'], api_mean_us=pop['api_mean_ns'] / 1000,
                        api_p50_us=pop['api_ns']['p50'] / 1000, api_p99_us=pop['api_ns']['p99'] / 1000,
                        response_p99_us=pop['response_ns']['p99'] / 1000,
                        case_success_calls_per_second=metrics['success_calls_per_second']))
            reports[run] = flat
    final_json = json.loads(source(FULL / 'comparison.json').read_text())
    cases = {c['case_id']: c for c in final_json['cases']}
    selected = [r for r in populations if r['status'] == 'completed' and r['outcome'] == 'success'
                and r['profile'] in PROFILES and r['operation'] in ('put', 'read', 'get_many')
                and ((r['cohort'] == 'primary_rf1' and r['durability_tier'] != 'native')
                     or (r['label'] == 'native' and r['cohort'] in ('native_async_write', 'native_pagecache') and r['operation'] == 'put'))]
    curve_runs = []
    for row in selected:
        available = SOURCES if row['label'] in external else ['final']
        for run in available:
            def equal(candidate):
                return (candidate['status'] == 'completed' and candidate['outcome'] == 'success'
                    and all(str(candidate[k]) == str(row[k]) for k in
                            ('backend', 'cohort', 'profile', 'value_bytes', 'keys', 'concurrency', 'operation'))
                    and float(candidate.get('offered_ops_per_second') or 0) == float(row['offered_ops_per_second'] or 0))
            found = [r for r in reports[run] if equal(r)]
            assert len(found) <= 1, (run, row['case_id'])
            if not found:
                continue
            sample = found[0]
            # The replacement panel is success-only. These closed-loop read cases have no unsuccessful calls.
            same_case = [r for r in reports[run] if r['case_id'] == sample['case_id'] and r['outcome'] == 'success']
            elapsed = float(sample['elapsed_seconds'])
            api_sum_us = sum(float(r['population_calls']) * float(r['api_mean_us']) for r in same_case)
            occupancy = api_sum_us / 1e6 / elapsed
            method = 'same-run sum(success population count * API mean) / elapsed; no products of medians'
            if run == 'final':
                timeline = cases[sample['case_id']]['metrics'].get('measurement_timeline', {})
                if timeline.get('mean_observed_api_concurrency') is not None:
                    observed = timeline['mean_observed_api_concurrency']
                    assert abs(occupancy - observed) < 1e-9, sample['case_id']
                    occupancy = observed
                    method = 'measurement_timeline.mean_observed_api_concurrency (agrees with success API sum / elapsed)'
            curve_runs.append(dict(label=row['label'], cohort=row['cohort'], profile=row['profile'], operation=row['operation'],
                size=int(row['value_bytes']), run=run, case_id=sample['case_id'], source=SOURCES[run],
                occupancy_method=method, mean_inflight_calls=occupancy,
                elapsed_seconds=elapsed, population_calls=int(sample['population_calls']),
                **{metric: float(sample[metric]) for metric in METRICS}))
    output('curve-runs.csv', curve_runs)
    topology = []
    for row in selected:
        if row['profile'] != 'write-concurrent' or row['value_bytes'] != '65536' or row['cohort'] != 'primary_rf1':
            continue
        path = source(FULL / 'cases' / row['case_id'] / 'config.json')
        options = json.loads(path.read_text())['backend']['options']
        label = row['label']
        count, evidence = None, ''
        if label == 'native':
            count, evidence = 1, 'one native database path; NATIVE.md one-process one-SSD reference'
        elif label.startswith('th_'):
            count, evidence = len(options['external_owner_durability_receipts']), 'external_owner_durability_receipts'
        elif 'nodes' in options:
            count, evidence = len(options['nodes']), 'backend.options.nodes (distinct storage addresses)'
        elif label == 'tikv':
            stores = cases[row['case_id']]['backend']['topology']['store_metadata']['stores']
            count = len({s['store']['address'].split(':')[0] for s in stores})
            evidence = 'config PD endpoint plus observed topology.store_metadata.stores, not PD count'
        elif label == 'foundationdb':
            processes = cases[row['case_id']]['backend']['topology']['cluster_status']['cluster']['processes'].values()
            count = len({p['address'].split(':')[0] for p in processes if any(r['role'] == 'storage' for r in p.get('roles', []))})
            evidence = 'config cluster_file plus observed status storage roles; transaction/log node excluded'
        topology.append(dict(label=label, case_id=row['case_id'], storage_nodes=count or '', source=str(path), evidence=evidence))
    output('storage-topology.csv', topology)
    pernode = []
    for top in topology:
        label = top['label']
        row = next(r for r in selected if r['case_id'] == top['case_id'])
        mixed = next(r for r in selected if r['label'] == label and r['cohort'] == 'primary_rf1' and r['profile'] == 'point-mixed' and r['operation'] == 'put' and r['value_bytes'] == '65536')
        node_count = top['storage_nodes']
        rate = float(row['case_success_calls_per_second'])
        inflight = statistics.median(r['mean_inflight_calls'] for r in curve_runs if r['label'] == label and r['cohort'] == 'primary_rf1' and r['profile'] == 'write-concurrent' and r['size'] == 65536)
        pernode.append(dict(label=label, case_id=row['case_id'], storage_nodes=node_count,
            callers_per_node=16 / node_count if node_count else '', puts_s=rate,
            puts_s_node=rate / node_count if node_count else '', payload_mibs_node=rate / node_count / 16 if node_count else '',
            p50_us=row['api_p50_us'], mean_us=row['api_mean_us'], c1_mixed_p50_us=mixed['api_p50_us'], mean_inflight_calls=inflight))
    output('per-node-64k.csv', pernode)
    spread = []
    for label in ('th_verbs', 'valkey_rdma'):
        row = next(r for r in topology if r['label'] == label)
        before = json.loads(source(FULL / 'cases' / row['case_id'] / 'measurement-before.json').read_text())
        after = json.loads(source(FULL / 'cases' / row['case_id'] / 'measurement-end.json').read_text())
        def writes(host):
            line = next(line for line in host['diskstats'].splitlines() if line.split()[2] == 'nvme0n1')
            return int(line.split()[7])
        # Host array order is coordinator 0, then storage hosts 1..5.
        assert len(before['hosts']) == len(after['hosts']) == 6
        counts = [writes(b) - writes(a) for a, b in zip(before['hosts'][1:], after['hosts'][1:])]
        spread.append(dict(label=label, case_id=row['case_id'], minimum=min(counts), maximum=max(counts), ratio=max(counts)/min(counts), counts=json.dumps(counts)))
    output('device-write-spread.csv', spread)
    paper = source(ROOT / 'papers/parsed/Tidehunter-2026-v2.md').read_text()
    paper_facts = []
    for key, pattern, multiplier in [('write_ops_s', r'achieves (830)K ops/sec', 1000),
                                     ('uniform_get_ops_s', r'achieves (392)K ops/s', 1000),
                                     ('skew_get_ops_s', r'Tidehunter.s (3\.98)M ops/s', 1000000)]:
        match = re.search(pattern, paper)
        assert match, key
        paper_facts.append(dict(key=key, value=float(match[1])*multiplier, source='papers/parsed/Tidehunter-2026-v2.md', matched_text=match[0]))
    output('paper-context.csv', paper_facts)
    garnet = next(r for r in selected if r['label'] == 'garnet' and r['profile'] == 'write-concurrent'
                  and r['value_bytes'] == '64' and r['operation'] == 'put')
    sample_path = source(FULL / 'cases' / garnet['case_id'] / 'result/samples.csv')
    stalls_ms = [int(r['api_ns']) / 1e6 for r in rows(sample_path)
                 if r['phase'] == 'measurement' and r['operation'] == 'put'
                 and r['outcome'] == 'success' and int(r['api_ns']) >= 10_000_000]
    output('chart-anomalies.csv', [dict(label='garnet', case_id=garnet['case_id'],
        source=str(sample_path), selection='successful measurement puts with API >= 10 ms',
        stall_count=len(stalls_ms), stall_min_ms=min(stalls_ms), stall_max_ms=max(stalls_ms))])
    historical = []
    for sample in curve_runs:
        if sample['label'] not in external:
            continue
        original = next(r for r in reports[sample['run']] if r['case_id'] == sample['case_id']
                        and r['operation'] == sample['operation'] and r['outcome'] == 'success')
        observed = original['observed_topology']
        observed = json.loads(observed) if isinstance(observed, str) else observed
        label = sample['label']
        if label == 'foundationdb':
            processes = observed['cluster_status']['cluster']['processes'].values()
            node_count = len({p.get('machine_id', p['address'].split(':')[0]) for p in processes
                              if any(role['role'] == 'storage' for role in p.get('roles', []))})
            kind = 'storage-role hosts; separate transaction/log host excluded'
        elif label == 'tikv':
            node_count = observed['store_metadata']['count']
            kind = 'TiKV stores; PD endpoint excluded'
        elif label == 'cassandra':
            node_count = observed['contact_node_count']
            kind = 'contact nodes; Nov03 ring not independently established'
        else:
            node_count = observed['primary_count']
            kind = 'configured or observed logical primaries'
        historical.append(dict(label=label, run=sample['run'], case_id=sample['case_id'],
            operation=sample['operation'], storage_node_count=node_count, evidence_kind=kind,
            source=sample['source'], ssd_mapping=('not established' if sample['run'] in ('nov03', 'queue')
                                               else 'deployment mounts attested by historical audit; not NAND proof')))
    output('historical-topology.csv', historical)
    (HERE / 'inputs.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Extracted', len(curve_runs), 'same-run curve populations and', len(pernode), 'topology rows.')


if __name__ == '__main__':
    main()
