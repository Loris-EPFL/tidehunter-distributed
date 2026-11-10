#!/usr/bin/env python3
"""Recompute value sizes and put quantiles from retained raw files, not reports.

No imports from trace_summary or the brief's number/plotting pipeline.
"""
import csv
import hashlib
import io
import json
import math
import re
import tarfile
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
TRACE = ROOT / 'checkpoint-work/sui-kv-trace-20261107/captures-corrected/remote'
FULL = ROOT / 'checkpoint-work/spdk-matched-storage-20261105T004442Z/analysis/gcp-full-matrix-2026-11-07/report-fixed'
R8 = ROOT / 'checkpoint-work/focused-launch-20261109-unified2/analysis/report-root'
R7 = ROOT / 'checkpoint-work/focused-launch-20261109-g10b/analysis-final/report-root'
N3 = ROOT / 'checkpoint-work/gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/metrics-all-populations.csv'
N3_ARCHIVE = ROOT / 'checkpoint-work/gcp-manual-run-b35a62e24b47faaf/priority-exports/nofile-recovery-2/controller.tar.gz'
QUEUE_ARCHIVE = ROOT / 'checkpoint-work/spdk-matched-storage-20261105T004442Z/gcp-queue-bounded-campaign-20261106T032027Z/final-retrieval/terminal-controller.tar.gz'
SIZES = (64, 256, 1024, 8192, 65536)


def rows(path, delimiter=','):
    with path.open(newline='') as stream:
        yield from csv.DictReader(stream, delimiter=delimiter)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def quantile(values, q):
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)] if values else ''


def trace_sizes():
    metadata, sources = {}, [TRACE / 'events.tsv']
    for path in sorted(TRACE.glob('db-*.tsv')):
        sources.append(path)
        fields, spaces = {}, {}
        for line in path.read_text().splitlines():
            cells = line.split('\t')
            if cells[0] == 'keyspace':
                spaces[int(cells[1])] = bytes.fromhex(cells[2]).decode()
            else:
                fields[cells[0]] = cells[1]
        namespace = bytes.fromhex(fields['path_hex']).decode().split('/full_node_db/', 1)[1].split('/', 1)[1]
        for ks, table in spaces.items():
            metadata[int(fields['db']), ks] = (namespace, table)
    starts, ends, batches = {}, {}, {}
    for row in rows(TRACE / 'events.tsv', '\t'):
        event, call = int(row['event']), int(row['call'])
        if event == 1:
            starts[call] = int(row['op'])
        elif event == 2:
            ends[call] = int(row['outcome'])
        elif event == 5:
            batches[int(row['group'])] = (int(row['outcome']), call)
    assert starts.keys() <= ends.keys(), 'incomplete trace calls'
    populations = defaultdict(list)
    deletes, large_calls = 0, set()
    for row in rows(TRACE / 'events.tsv', '\t'):
        event, op, call = int(row['event']), int(row['op']), int(row['call'])
        if event in (3, 8):
            if op in (5, 6) and ends.get(call) != 1:
                continue
        elif event == 4:
            outcome, parent = batches.get(int(row['group']), (0, 0))
            if outcome != 1 or starts.get(parent) in (5, 6):
                continue
            call = parent
        else:
            continue
        namespace, table = metadata[int(row['db']), int(row['ks'])]
        if op == 6:
            deletes += 1
            continue
        if op == 5:
            operation = 'put'
        elif int(row['outcome']) == 1 and op not in (2, 4):
            operation = 'read'
        else:
            continue
        value = int(row['value_len'])
        populations[namespace, table, operation].append(value)
        if value >= 65536:
            large_calls.add(call)
    output = []
    # Retain every populated table and explicit empty rows for the six displayed tables.
    shown = [('live/checkpoints', table) for table in ('checkpoint_content', 'full_checkpoint_content_v2', 'certified_checkpoints')]
    shown += [('live/store/perpetual', table) for table in ('effects', 'objects', 'transactions')]
    keys = set(populations) | {(ns, table, op) for ns, table in shown for op in ('read', 'put')}
    for operation in ('read', 'put'):
        populations['ALL', 'ALL', operation] = [value for (ns, table, op), values in populations.items()
                                                if op == operation and ns != 'ALL' for value in values]
        keys.add(('ALL', 'ALL', operation))
    for namespace, table, operation in sorted(keys):
        values = populations[namespace, table, operation]
        row = dict(label=namespace.replace('/', '_') + '_' + table + '_' + operation, namespace=namespace, table=table, operation=operation,
                   n=len(values), p50=quantile(values, .5), p90=quantile(values, .9),
                   p99=quantile(values, .99), maximum=max(values) if values else '',
                   ge8=sum(value >= 8192 for value in values), ge64=sum(value >= 65536 for value in values))
        for threshold in ('ge8', 'ge64'):
            row[threshold + '_pct'] = 100 * row[threshold] / len(values) if values else ''
        for size in SIZES:
            row[f'exact_{size}'] = values.count(size)
        output.append(row)
    all_values = populations['ALL', 'ALL', 'read'] + populations['ALL', 'ALL', 'put']
    summary = [dict(label='value_ops', value=len(all_values)), dict(label='api_calls', value=len(ends)),
               dict(label='deletes', value=deletes), dict(label='ge64_value_ops', value=sum(v >= 65536 for v in all_values)),
               dict(label='ge64_value_pct', value=100 * sum(v >= 65536 for v in all_values) / len(all_values)),
               dict(label='ge64_api_calls', value=len(large_calls)),
               dict(label='ge64_api_pct', value=100 * len(large_calls) / len(ends))]
    medians = [quantile(populations[ns, table, op], .5) / 1024
               for ns, table in shown for op in ('read', 'put') if populations[ns, table, op]]
    nearest_decade = lambda value: 10 ** round(math.log10(value))
    summary += [dict(label='typical_low_kib', value=nearest_decade(min(medians))),
                dict(label='typical_high_kib', value=nearest_decade(max(medians))),
                dict(label='full_low_kib', value=math.floor(quantile(populations['live/checkpoints', 'full_checkpoint_content_v2', 'read'], .5) / 1024)),
                dict(label='full_high_kib', value=math.ceil(quantile(populations['live/checkpoints', 'full_checkpoint_content_v2', 'read'], .9) / 1024))]
    return output, summary, sources


def spike_samples():
    selected, sources = [], [FULL / 'comparison.csv', R8 / 'comparison.csv', R7 / 'comparison.csv', N3, HERE / 'curve-runs.csv']
    campaigns = [('M', FULL / 'comparison.csv'), ('R8', R8 / 'comparison.csv'), ('R7', R7 / 'comparison.csv'), ('N3', N3),
                 ('O', ROOT / 'checkpoint-work/gcp-manual-combined-prep-20261104/manual-live-nvmeof-read-observation-20261104T142730Z/canonical-terminal/expanded/comparison.csv'),
                 ('D', ROOT / 'checkpoint-work/gcp-manual-nvme-diag-prep-20261104/manual-live-nvmeof-delete-observed-20261104T181245Z/canonical-terminal/expanded/comparison.csv'),
                 ('Q', QUEUE_ARCHIVE.parent / 'extract/comparison.csv')]
    for run, source in campaigns:
        sources.append(source)
        for row in rows(source):
            if row['operation'] != 'put' or row['outcome'] != 'success' or row['concurrency'] != '16' or row['value_bytes'] != '65536':
                continue
            if run == 'N3' and row['timing'] != 'api':
                continue
            if run in ('O', 'D', 'Q') and (row['cohort'], row['profile']) != ('primary_rf1', 'write-concurrent'):
                continue
            backend = row['backend']
            if backend == 'native' and run in ('M', 'R8', 'N3'):
                mode = 'P' if row['cohort'] in ('native_async_write', 'native_pagecache') or row.get('native_durability') == 'page_cache' else 'B'
            elif backend == 'tidehunter_verbs_native' and (run in ('N3', 'O', 'D', 'Q') or row.get('durability_tier') == 'synchronous'):
                mode = 'S'
            else:
                continue
            selected.append((run, mode, source, row['case_id']))
    for row in rows(HERE / 'curve-runs.csv'):
        if row['label'] == 'valkey_rdma' and row['profile'] == 'write-concurrent' and row['size'] == '65536':
            selected.append((row['run'], 'V', ROOT / row['source'], row['case_id']))
    archives = {}
    result = []
    try:
        for run, mode, source, case in selected:
            sample = source.parent / 'cases' / case / 'result/samples.csv'
            member = ''
            if sample.exists():
                with sample.open(newline='') as stream:
                    data = list(csv.DictReader(stream))
            else:
                archive = N3_ARCHIVE if run in ('N3', 'nov03') else QUEUE_ARCHIVE if run in ('Q', 'queue') else None
                if archive is None:
                    raise FileNotFoundError(f'No raw samples for {run} {case}: {sample}')
                if archive not in archives:
                    archives[archive] = tarfile.open(archive, 'r:gz')
                    sources.append(archive)
                member = f'cases/{case}/result/samples.csv'
                with archives[archive].extractfile(member) as binary:
                    data = list(csv.DictReader(io.TextIOWrapper(binary)))
                sample = archive
            sources.append(sample)
            measured = [r for r in data if r['phase'] == 'measurement' and r['operation'] == 'put' and r['outcome'] == 'success']
            values = [int(r['api_ns']) / 1000 for r in measured]
            ordered = sorted(measured, key=lambda r: int(r['dispatched_ns']))
            median = quantile(values, .5)
            result.append(dict(label=run + '_' + case, run=run, mode=mode, case_id=case,
                               source=str(sample.relative_to(ROOT)), archive_member=member, n=len(values),
                               p10_us=quantile(values, .1), p50_us=median, p99_us=quantile(values, .99),
                               above_twice_pct=100 * sum(v > 2 * median for v in values) / len(values),
                               **{f'q{i+1}_p50_us': quantile([int(r['api_ns']) / 1000 for r in ordered[i*len(ordered)//4:(i+1)*len(ordered)//4]], .5) for i in range(4)}))
    finally:
        for archive in archives.values():
            archive.close()
    summary = []
    for mode in ('B', 'P'):
        values = [r['p50_us'] for r in result if r['mode'] == mode]
        summary.extend([dict(label=mode + '_minimum', value=min(values)), dict(label=mode + '_maximum', value=max(values))])
    return result, summary, sources


def context():
    paper = ROOT / 'papers/parsed/Tidehunter-2026-v2.md'
    protocol = ROOT / 'sui/crates/sui-protocol-config/src/lib.rs'
    result = []
    for name in ('max_tx_size_bytes', 'max_serialized_tx_effects_size_bytes', 'max_move_object_size', 'max_move_package_size', 'max_checkpoint_size_bytes'):
        line, match = next((i, re.search(r'\b' + name + r': Some\((\d+)((?: \* 1024)+)\)', text))
                           for i, text in enumerate(protocol.read_text().splitlines(), 1)
                           if re.search(r'\b' + name + r': Some\((\d+)((?: \* 1024)+)\)', text))
        value = int(match[1]) * 1024 ** match[2].count('1024')
        result.append(dict(label=name, value=value, kib=value / 1024, mib=value / 1048576, source=str(protocol.relative_to(ROOT)), line=line))
    patterns = {'dedup_low': r'from (\d+) KB to', 'dedup_high': r'to (\d+) KB',
                'photo_low': r'around (\d+)-', 'photo_high': r'around \d+-(\d+) KB',
                'sui_low_kb': r'typically (\d+)KB', 'sui_high_mb': r'typically \d+KB[–-](\d+)MB'}
    for name, pattern in patterns.items():
        line, match = next((i, re.search(pattern, text)) for i, text in enumerate(paper.read_text().splitlines(), 1) if re.search(pattern, text))
        result.append(dict(label=name, value=int(match[1]), kib='', mib='', source=str(paper.relative_to(ROOT)), line=line))
    paper_lines = paper.read_text().splitlines()
    evaluation_line = next(i for i, text in enumerate(paper_lines, 1) if text.startswith('• **Value size**:'))
    for value, unit in re.findall(r'(\d+) (KB|bytes)', paper_lines[evaluation_line - 1]):
        result.append(dict(label='eval_' + value + unit, value=int(value), kib='', mib='',
                           source=str(paper.relative_to(ROOT)), line=evaluation_line))
    # Configuration labels visually verified in the parsed Figure 1 image at line 23.
    for value in (256, 512):
        result.append(dict(label='figure_' + str(value), value=value, kib='', mib='',
                           source=str(paper.relative_to(ROOT)), line=23))
    return result, [paper, protocol]


def fair_native_plan():
    """Read-only matrix generation; this does not launch or run any case."""
    import sys
    sys.path.insert(0, str(ROOT / 'tidehunter-sharded-wal/experiments/kv-comparison'))
    import cloud_campaign
    fair = cloud_campaign.make_focused_matrix('fair_native')
    parity = cloud_campaign.make_focused_matrix('parity')
    source = 'tidehunter-sharded-wal/experiments/kv-comparison/cloud_campaign.py'
    records = []
    selections = [(group, [job for job in fair if job['group'] == group])
                  for group in ('G14N', 'G14S', 'G14O1', 'G14O5', 'G14P')]
    selections += [('fair_native', fair), ('parity', parity),
                   ('G13W', [job for job in parity if job['group'] == 'G13W'])]
    selections += [('frozen_' + selection, cloud_campaign.make_focused_matrix(selection))
                   for selection in ('recommended', 'full', 'device', 'floors', 'stages', 'unified')]
    for label, jobs in selections:
        writers = sorted({job['concurrency'] for job in jobs})
        records.append(dict(label=label, cases=len(jobs),
                            writer0=writers[0], writer1=writers[1] if len(writers) > 1 else '',
                            writer2=writers[2] if len(writers) > 2 else '',
                            matrix_sha256=hashlib.sha256(json.dumps(jobs, sort_keys=True).encode()).hexdigest(),
                            source=source, method='cloud_campaign.make_focused_matrix; read-only generation'))
    return records, [ROOT / source, ROOT / 'tidehunter-sharded-wal/docs/distributed/FOCUSED_CAMPAIGN_2026-11-07.md']


def main():
    manifest = json.loads((HERE / 'inputs.json').read_text())
    size_rows, size_summary, sources = trace_sizes()
    sources += [ROOT / 'tidehunter-sharded-wal' / path for path in (
        'experiments/sui-checkpoint/trace_summary.py', 'benchmarks/kv-research/src/runner.rs',
        'benchmarks/kv-research/src/backends/native.rs', 'tidehunter/src/db.rs', 'tidehunter/src/crc.rs')]
    spike_rows, spike_summary, spike_sources = spike_samples()
    context_rows, context_sources = context()
    plan_rows, plan_sources = fair_native_plan()
    outputs = {'sui-value-sizes.csv': size_rows, 'size-summary.csv': size_summary,
               'spike-cases.csv': spike_rows, 'spike-summary.csv': spike_summary, 'size-context.csv': context_rows,
               'fair-native-plan.csv': plan_rows}
    for name, records in outputs.items():
        path = HERE / name
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
        manifest['outputs'][name] = sha(path)
    local_csv = HERE / 'local-structure-test.csv'
    manifest['outputs'][local_csv.name] = sha(local_csv)
    local_records = list(rows(local_csv))
    report_paths = {Path(row['report_path']) for row in local_records}
    sources += [local_csv] + [path for path in report_paths if path.exists()]
    manifest['local_structure_transcription'] = {
        'source': 'local://report-spike.md, Subsequent IEEE CRC and missing controls',
        'selection': 'IEEE CRC32 only; seven requested variants; each run retained separately',
        'evidence': 'transcribed from the audit report; raw samples not retained',
        'rows': len(local_records)}
    for source in set(sources + spike_sources + context_sources + plan_sources):
        manifest['sources'][str(source)] = sha(source)
    manifest['size_spike_rules'] = {'trace': 'one corrected remote capture; successful put members; found payload reads including scan rows; no exists or missing sizes; scalar batches deduplicated',
                                  'quantiles': 'nearest rank, no trimming; successful measurement put samples; quarters by dispatch order',
                                  'typical_range': 'nearest-decade range of nonempty medians of the six displayed tables; full-content interval is read p50/p90 rounded outwards to KiB',
                                  'spike_modes': 'B: per-put Native barrier, one SSD; P: Native page cache, no per-put foreground sync guarantee; S: verbs synchronous covering sync, five owners; V: historical RDMA AOF-always; caches not controlled'}
    (HERE / 'inputs.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'size_summary': size_summary, 'spike_cases': len(spike_rows)}, indent=2))


if __name__ == '__main__':
    main()
