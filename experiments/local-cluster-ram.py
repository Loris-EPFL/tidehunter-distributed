#!/usr/bin/env python3
"""Run and audit the named RAM/progress campaign on five local processes. No AWS calls."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / 'aws-sharded-wal'))
import cluster_ram
import local_compare

import portable_transport



def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def comparison(runs):
    grouped = {}
    for spec, mode, metrics in runs:
        for (kind, phase, metric), value in metrics.items():
            key = (spec['family'], spec['build'], spec['value'], spec['batch'], spec['window'], mode, kind, phase, metric)
            grouped.setdefault(key, {})[spec['rep']] = value
    rows = []
    for key, values in sorted(grouped.items()):
        family, build, value, batch, window, mode, kind, phase, metric = key
        old = grouped.get((family, 'baseline', value, batch, window, mode, kind, phase, metric), {}) if build == 'current' else {}
        native = grouped.get((family, build, value, batch, window, 'native-sync', kind, phase, metric), {})
        median = statistics.median(values.values())
        previous = statistics.median(old.values()) if old else None
        control = statistics.median(native.values()) if native else None
        paired = {str(rep): values[rep] / old[rep] for rep in values.keys() & old.keys() if old[rep]}
        rows.append(dict(family=family, build=build, value_bytes=value, batch_size=batch, window=window,
                         backend=mode, kind=kind, phase=phase, metric=metric, repetitions=len(values),
                         median=median, baseline_median=previous,
                         candidate_over_baseline=median / previous if previous else None,
                         native_median=control if mode == 'tcp' else None,
                         distributed_over_native=median / control if mode == 'tcp' and control else None,
                         paired_ratios=json.dumps(paired, sort_keys=True)))
    return rows


def write_comparison(path, runs):
    rows = comparison(runs)
    if not rows:
        return
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--baseline-manifest', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-source-sha256', default=cluster_ram.BASELINE_SOURCE_SHA256)
    parser.add_argument('--baseline-binary-sha256', default=cluster_ram.BASELINE_BINARY_SHA256)
    parser.add_argument('--suite', choices=('core', 'capacity', 'all'), default='core')
    parser.add_argument('--transport', choices=('tcp', 'fabric-tcp', 'fabric-tcp-native'), default='tcp')
    parser.add_argument('--values', default='1024,8192')
    parser.add_argument('--repetitions', type=int, default=1)
    parser.add_argument('--reads', type=int, default=1024)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 3 or not 256 <= args.reads <= 8192:
        raise ValueError('local repetitions/reads exceed bounds')
    values = [int(value) for value in args.values.split(',')]
    if not values or len(set(values)) != len(values) or any(value not in (1024, 8192) for value in values):
        raise ValueError('values must be a unique subset of 1024,8192')
    if bool(args.baseline) != bool(args.baseline_manifest):
        raise ValueError('baseline executable and manifest must be supplied together')
    timeout = shutil.which('timeout')
    if timeout is None:
        raise RuntimeError('GNU timeout required for bounded process ownership')
    binaries, manifests = {}, {}
    for label, binary, manifest in [('current', args.candidate, args.candidate_manifest),
                                    ('baseline', args.baseline, args.baseline_manifest)]:
        if binary is None:
            continue
        binaries[label] = binary.resolve(strict=True)
        manifests[label] = json.loads(manifest.read_text())
        if digest(binary) != manifests[label]['binary_sha256']:
            raise ValueError('binary differs from frozen source manifest')
    if 'baseline' in manifests and (manifests['baseline']['source_tree_sha256'] != args.baseline_source_sha256
                                    or manifests['baseline']['binary_sha256'] != args.baseline_binary_sha256):
        raise ValueError('baseline does not match the explicitly pinned source and binary')
    if 'baseline' in manifests and manifests['baseline']['source_tree_sha256'] == manifests['current']['source_tree_sha256']:
        raise ValueError('comparison requires distinct source trees')
    scope = 'full' if args.suite == 'all' else args.suite
    cases = [spec for spec in cluster_ram.main_cases(scope, args.repetitions, (args.transport,))
             if spec['value'] in values and spec['build'] in binaries]
    if not cases:
        raise ValueError('selected suite has no cases')
    for spec in cases:
        spec['reads'] = args.reads
    args.output.mkdir()
    requested_configs = args.output / 'requested-configs'
    requested_configs.mkdir()
    transport_configs = {}
    for spec in cases:
        if spec['build'] == 'current':
            path = requested_configs / spec['case_id'] / 'transport.json'
            path.parent.mkdir()
            portable_transport.write_transport_config(
                path, portable_transport.transport_for_case(args.transport)
            )
            transport_configs[spec['case_id']] = path
    plan = dict(schema=1, campaign='cluster-ram-local', transport=args.transport,
                transport_configs={case_id: str(path) for case_id, path in transport_configs.items()},
                source_manifests=manifests, cases=cases,
                topology='five loopback storage processes; one shared SSD and RAM pool; no hardware RDMA',
                case_deadline_seconds=600,
                cache_accounting='explicit budgets include fixed metadata; native cache nominal values only; shared allocations may be charged twice',
                acceptance='verified values/reopen, real evicted I/O, bounded credits, scan no-fill, exclusive window timing; not EFA or multi-NVMe speed proof')
    (args.output / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    runs, resources = [], []
    for spec in cases:
        binary = binaries[spec['build']]
        if digest(binary) != manifests[spec['build']]['binary_sha256']:
            raise ValueError('binary changed during campaign')
        modes = spec['modes'] if spec['rep'] % 2 == 0 else list(reversed(spec['modes']))
        for mode in modes:
            output = args.output / (spec['case_id'] + '-' + mode)
            command = [timeout, '--signal=TERM', '--kill-after=5s', '600s', str(binary),
                       '--output', str(output), '--backend', mode]
            command += cluster_ram.flags(
                spec, mode,
                transport_config=transport_configs.get(spec['case_id'])
            )
            local_compare.run(command, output)
            record = json.loads(output.with_suffix('.resources.json').read_text())
            if record['remaining_sampled_pids']:
                raise ValueError('benchmark left sampled owned processes running')
            resources.append(dict(case=output.name, **record))
            runs.append((spec, mode, cluster_ram.read_metrics(output, spec, mode)))
            write_comparison(args.output / 'comparison.csv', runs)
            (args.output / 'resource-summary.json').write_text(json.dumps(resources, indent=2) + '\n')
    for label, binary in binaries.items():
        if digest(binary) != manifests[label]['binary_sha256']:
            raise ValueError('binary changed before campaign completion')
    (args.output / 'COMPLETE').write_text('all requested local RAM/progress cases and evidence checks passed\n')
    print(json.dumps(dict(complete=True, cases=len(runs), topology=plan['topology'],
                         swap_delta_pages={key: sum(r['swap_delta_pages'][key] for r in resources) for key in ('pswpin', 'pswpout')},
                         peak_sampled_tree_rss_kib=max(r['peak_sampled_process_tree_rss_kib'] for r in resources))))


if __name__ == '__main__':
    main()
