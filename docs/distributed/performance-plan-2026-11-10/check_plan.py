#!/usr/bin/env python3
"""Independently check displayed values against raw successful samples and owner records."""
import csv
import json
import math
import re
import io
import tarfile
import statistics
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLAN = HERE.parent / 'PERFORMANCE_PLAN_2026-11-10.md'
ANALYSIS = HERE.parents[3] / 'checkpoint-work/focused-launch-20261109-unified2/analysis'


def tables(text):
    lines = text.splitlines()
    result = []
    for index, line in enumerate(lines[:-1]):
        if not line.startswith('|') or not re.match(r'^\|[ :|-]+\|$', lines[index + 1]):
            continue
        header = [x.strip() for x in line.strip('|').split('|')]
        rows = []
        for item in lines[index + 2:]:
            if not item.startswith('|'):
                break
            cells = [x.strip() for x in item.strip('|').split('|')]
            if len(cells) == len(header):
                rows.append(dict(zip(header, cells)))
        result.append((header, rows))
    return result


def close(value, shown, label):
    try:
        actual = float(shown)
    except ValueError as exc:
        raise SystemExit(f'{label}: non-numeric rendered value {shown!r}') from exc
    if abs(actual - value) > 0.05000001:
        raise SystemExit(f'{label}: raw {value} differs from rendered {shown}')


def main():
    text = PLAN.read_text() + '\n' + (HERE.parent / 'PERFORMANCE_PLAN_2026-11-10-APPENDIX.md').read_text()
    if '{{' in text or '}}' in text:
        raise SystemExit('Unresolved placeholder')
    found = tables(text)
    appendix = {}
    for header, rows in found:
        if 'ID' in header and 'Case IDs' in header:
            for row in rows:
                appendix[row['ID']] = re.findall(r'G\d+[A-Z]-r\d+-[0-9a-f]+', row['Case IDs'])
    if not appendix:
        raise SystemExit('Case ID appendix missing')
    operation_rows = [r for h, rows in found if {'ID', 'Shape', 'p50 µs'} <= set(h) for r in rows]
    owner_rows = [r for h, rows in found if {'ID', 'Stage', 'Mean µs', 'p50 µs'} <= set(h) for r in rows]
    checked = 0
    seen = set()
    for row in operation_rows:
        cases = appendix.get(row['ID'], [])
        if len(cases) != 1 or cases[0] in seen:
            continue
        case = cases[0]
        path = ANALYSIS / 'report-root/cases' / case / 'result/samples.csv'
        if not path.is_file():
            continue
        try:
            float(row['p50 µs'])
        except ValueError:
            continue
        with path.open(newline='') as samples:
            values = sorted(int(sample['api_ns']) for sample in csv.DictReader(samples)
                            if sample['phase'] == 'measurement' and sample['outcome'] == 'success'
                            and sample['api_ns'])
        if not values:
            continue
        value = values[math.ceil(len(values) * .5) - 1] / 1000
        close(value, row['p50 µs'], case)
        seen.add(case)
        checked += 1
        print(f'{checked:02d} {row["ID"]} {case} raw p50 {value:.3f} µs; shown {row["p50 µs"]} µs')
        if checked == 27:
            break
    if checked < 27:
        raise SystemExit(f'Only {checked} raw operation checks; need 27')
    targets = [('G12D-r0-08a9a20ff91943a18559', 'handler_ns'),
               ('G12D-r0-946e08ec43aceab58b29', 'append_persist_ns'),
               ('G12D-r0-9eaa39f00ad063fef4cf', 'append_wal_ns')]
    for case, stage in targets:
        matching = [r for r in owner_rows if case in appendix.get(r['ID'], []) and r['Stage'] == stage]
        if not matching:
            raise SystemExit(f'Owner stage missing: {case} {stage}')
        path = ANALYSIS / 'report-root/cases' / case / 'result/write-timeline.json'
        records = json.loads(path.read_text())['timeline']['owner_records']
        value = sum(r[stage] for r in records) / len(records) / 1000
        close(value, matching[0]['Mean µs'], f'{case} {stage}')
        checked += 1
        print(f'{checked:02d} {matching[0]["ID"]} {case} {stage} raw mean {value:.3f} µs; shown {matching[0]["Mean µs"]} µs')
        residual = sum(r['handler_ns'] - r['flusher_queue_ns'] - r['append_wal_ns'] - r['append_persist_ns']
                       for r in records) / len(records) / 1000
        rem = [r for r in owner_rows if case in appendix.get(r['ID'], [])
               and r['Stage'] == 'unassigned owner time (append decode, group build, other handler work)']
        if not rem:
            raise SystemExit(f'Owner remainder missing: {case}')
        close(residual, rem[0]['Mean µs'], f'{case} remainder')
        checked += 1
        print(f'{checked:02d} {case} unassigned raw mean {residual:.3f} µs; shown {rem[0]["Mean µs"]} µs')
    profile_case = 'G12D-r0-0ce5f8eaa8fde37f6566'
    profile = json.loads((ANALYSIS / 'report-root/cases' / profile_case / 'result/write-diagnostics.json').read_text())['profile']['stages']
    profile_rows = [r for h, rows in found if h == ['Stage', 'n', 'Mean µs'] for r in rows]
    for label, field, count in [('Connection handler', 'connection_handler_ns', 'sampled_requests'),
                                ('Control queue', 'connection_control_queue_ns', 'connection_control_queue_samples'),
                                ('Storage persist', 'storage_persist_total_ns', 'sampled_requests')]:
        matches = [r for r in profile_rows if r['Stage'] == label]
        if len(matches) != 1:
            raise SystemExit(f'Profile stage missing or ambiguous: {label}')
        value = profile['write_profile_' + field] / profile['write_profile_' + count] / 1000
        close(value, matches[0]['Mean µs'], label)
        checked += 1
        print(f'{checked:02d} {profile_case} {label} raw mean {value:.3f} µs; shown {matches[0]["Mean µs"]} µs')
    sustained_rows = [r for h, rows in found if 'Last five complete one-second buckets puts/s' in h for r in rows]
    if len(sustained_rows) != 12:
        raise SystemExit(f'Expected 12 sustained cases, got {len(sustained_rows)}')
    groups = defaultdict(list)
    for row in sustained_rows:
        cases = appendix[row['ID']]
        if len(cases) != 1:
            raise SystemExit(f'Expected one case: {row["ID"]}')
        path = HERE.parents[3] / 'checkpoint-work/focused-launch-20261109-g10b/analysis-final/report-root/cases' / cases[0] / 'result/samples.csv'
        with path.open(newline='') as f:
            calls = [(int(r['dispatched_ns']), int(r['completed_ns'])) for r in csv.DictReader(f)
                     if r['phase'] == 'measurement' and r['operation'] == 'put' and r['outcome'] == 'success']
        start = min(d for d, _ in calls)
        end = max(c for _, c in calls)
        count = (end - start) // 1_000_000_000
        buckets = [0] * count
        for _, completed in calls:
            index = (completed - start) // 1_000_000_000
            if index < count:
                buckets[index] += 1
        expected = buckets[-5:]
        shown = [int(v.strip()) for v in row['Last five complete one-second buckets puts/s'].split(',')]
        if len(expected) != 5 or shown != expected or int(row['First second puts/s']) != buckets[0]:
            raise SystemExit(f'Raw sustained buckets differ for {cases[0]}')
        groups[(row['Route'], row['Durability'])].append(statistics.mean(expected))
        checked += 1
    summaries = [r for h, rows in found if 'Last five complete one-second buckets mean puts/s median (min–max)' in h for r in rows]
    if len(summaries) != 8:
        raise SystemExit(f'Expected two sets of four sustained summaries, got {len(summaries)}')
    for row in summaries:
        values = groups[(row['Route'], row['Tier'])]
        shown = row['Last five complete one-second buckets mean puts/s median (min–max)']
        expected = f'{statistics.median(values):.1f} ({min(values):.1f}–{max(values):.1f})'
        if shown != expected:
            raise SystemExit(f'Sustained summary differs: {shown} != {expected}')
        checked += 1
    print(f'{checked:02d} complete-bucket vectors and summaries checked against raw calls')
    root = HERE.parents[3]
    work = root / 'checkpoint-work'
    full = work / 'spdk-matched-storage-20261105T004442Z/analysis/gcp-full-matrix-2026-11-07/report-fixed/cases'
    native_case = '0379-native-65536-write-concurrent-primary_rf1-r0'
    owner_case = '0255-tidehunter_verbs_native-65536-write-concurrent-primary_rf1-r0'
    valkey_case = '0181-valkey_rdma-65536-write-concurrent-primary_rf1-r0'
    old_case = '0030-valkey_rdma-65536-write-concurrent-primary_rf1-r0'
    queue_case = '0131-valkey_rdma-65536-write-concurrent-primary_rf1-r0'
    queue_archive = work / 'spdk-matched-storage-20261105T004442Z/gcp-queue-bounded-campaign-20261106T032027Z/final-retrieval/terminal-controller.tar.gz'
    initial_archive = work / f'gcp-manual-run-b35a62e24b47faaf/results/case-checkpoints/{old_case}.tar.gz'
    runs = {
        'native': [(full / native_case / 'result/samples.csv', None)],
        'th_verbs': [(full / owner_case / 'result/samples.csv', None)],
        'valkey_rdma': [
            (initial_archive, 'result/samples.csv'),
            (work / f'gcp-manual-combined-prep-20261104/manual-live-nvmeof-read-observation-20261104T142730Z/canonical-terminal/expanded/cases/{old_case}/result/samples.csv', None),
            (work / f'gcp-manual-nvme-diag-prep-20261104/manual-live-nvmeof-delete-observed-20261104T181245Z/canonical-terminal/expanded/cases/{old_case}/result/samples.csv', None),
            (queue_archive, f'cases/{queue_case}/result/samples.csv'),
            (full / valkey_case / 'result/samples.csv', None),
        ],
    }
    def raw_metrics(path, member):
        if member:
            with tarfile.open(path) as archive:
                stream = archive.extractfile(member)
                if stream is None:
                    raise SystemExit(f'Missing archived samples: {path} {member}')
                with io.TextIOWrapper(stream, encoding='utf-8', newline='') as f:
                    calls = [r for r in csv.DictReader(f) if r['phase'] == 'measurement']
        else:
            with path.open(newline='') as f:
                calls = [r for r in csv.DictReader(f) if r['phase'] == 'measurement']
        successful = [r for r in calls if r['operation'] == 'put' and r['outcome'] == 'success']
        if len(successful) != 4096 or len(calls) != len(successful):
            raise SystemExit(f'Unexpected raw population in {path}: {len(successful)}/{len(calls)}')
        elapsed_ns = max(int(r['completed_ns']) for r in calls) - min(int(r['scheduled_ns']) for r in calls)
        times = sorted(int(r['api_ns']) for r in successful)
        return len(successful) * 1e9 / elapsed_ns, times[math.ceil(len(times) * .5) - 1] / 1000, statistics.mean(times) / 1000
    configs = {label: json.loads((full / case / 'config.json').read_text())['backend']['options']
               for label, case in [('native', native_case), ('th_verbs', owner_case), ('valkey_rdma', valkey_case)]}
    node_counts = {'native': 1, 'th_verbs': len(configs['th_verbs']['external_owner_durability_receipts']),
                   'valkey_rdma': len(configs['valkey_rdma']['nodes'])}
    node_rows = [r for h, rows in found if 'aggregate puts/s / node count' in h for r in rows]
    if len(node_rows) != len(runs):
        raise SystemExit('Expected three per-node rows')
    for row in node_rows:
        label = row['Store']
        metrics = [raw_metrics(path, member) for path, member in runs[label]]
        rate, p50, mean = (statistics.median(values) for values in zip(*metrics))
        nodes = node_counts[label]
        for column, value in [('Storage nodes', nodes), ('16/nodes (assumed)', 16 / nodes),
                              ('Puts/s', rate), ('aggregate puts/s / node count', rate / nodes),
                              ('Payload MiB/s/node', rate / nodes / 16),
                              ('p50 µs', p50), ('Mean µs', mean)]:
            close(value, row[column], f'{label} {column}')
            checked += 1
    print(f'{checked:02d} per-node values checked against seven raw case sample files')
    campaign = (HERE.parent / 'FOCUSED_CAMPAIGN_2026-11-07.md').read_text()
    planned = campaign.split('## Fair native comparison and 64 KiB write profiles (planned, not run)\n\n', 1)[1].split('\nRun the standalone analysis on a finished result tree:', 1)[0]
    for header, rows in tables(planned):
        matches = [shown for h, shown in found if h == header and shown == rows]
        if len(matches) != 1:
            raise SystemExit(f'Planned comparison table differs: {header}')
        checked += len(rows)
    match = re.search(r'`fair_native` selects these groups \((\d+) cases\).*?adds the (\d+) G13W\s+append floors \((\d+) cases\)', planned, re.S)
    if not match:
        raise SystemExit('Missing fair selection counts')
    fair, floors, parity = map(int, match.groups())
    arm_rows = next(rows for h, rows in tables(planned) if h == ['Group', 'Cases', 'Arm and writer counts'])
    if sum(int(r['Cases']) for r in arm_rows) != fair or fair + floors != parity:
        raise SystemExit('Fair selection count mismatch')
    if f'fair_native {fair}, parity {parity}' not in text:
        raise SystemExit('Rank zero selection counts differ')
    checked += 3
    samples = [
        (work / 'gcp-manual-run-b35a62e24b47faaf/results/case-checkpoints/0183-native-65536-write-concurrent-primary_rf1-r0.tar.gz', 'result/samples.csv'),
        (ANALYSIS / 'report-root/cases/G12K-r0-fc2fd2a25da56cb25569/result/samples.csv', None),
        (ANALYSIS / 'report-root/cases/G12N-r1-71d612a2e157e9bae6ba/result/samples.csv', None),
        (full / '0490-native-65536-write-concurrent-native_async_write-r0/result/samples.csv', None),
    ]
    spike_p50 = [raw_metrics(path, member)[1] for path, member in samples]
    spike = re.search(r'barrier c16 64 KiB put p50 was ([0-9.]+) to ([0-9.]+) µs; page-cache acknowledgement .*? was ([0-9.]+) to ([0-9.]+) µs', text)
    if not spike:
        raise SystemExit('Spike paragraph missing')
    for expected, shown in zip(spike_p50, spike.groups()):
        close(expected, shown, 'raw spike p50')
        checked += 1
    trace = work / 'sui-kv-trace-20261107/captures-corrected/remote/events.tsv'
    outcomes, group_outcomes, starts = {}, {}, {}
    with trace.open(newline='') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            if r['event'] == '1':
                starts[r['call']] = r['op']
            elif r['event'] == '2':
                outcomes[r['call']] = r['outcome']
            elif r['event'] == '5':
                group_outcomes[r['group']] = (r['outcome'], r['call'])
    total = large = 0
    with trace.open(newline='') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            op, event = r['op'], r['event']
            if event in ('3', '8'):
                if op in ('5', '6') and outcomes.get(r['call']) != '1':
                    continue
            elif event == '4':
                group = group_outcomes.get(r['group'])
                if group is None or group[0] != '1' or starts.get(group[1]) in ('5', '6'):
                    continue
            else:
                continue
            if op == '5' or (op not in ('2', '4', '6') and r['outcome'] == '1'):
                total += 1
                large += int(r['value_len']) >= 65536
    value_match = re.search(r'In one private checkpoint replay, (\d+) of ([\d,]+) value-bearing successful puts and found reads were at least 64 KiB \(([0-9.]+)%\)', text)
    if not value_match or int(value_match[1]) != large or int(value_match[2].replace(',', '')) != total:
        raise SystemExit('Sui value population mismatch')
    if abs(float(value_match[3]) - 100 * large / total) > .000050001:
        raise SystemExit('Sui large-value percentage mismatch')
    report = (work / 'sui-kv-trace-20261107/REPORT.md').read_text()
    value_row = next(line for line in report.splitlines() if line.startswith('|remote|checkpoints|full_checkpoint_content_v2|'))
    if '8,885 and p95 11,466 bytes' not in text or '8885/11466/11466' not in value_row:
        raise SystemExit('Sui checkpoint quantiles mismatch')
    medians = []
    for line in report.splitlines():
        if line.startswith('|remote|'):
            medians.extend(int(m.group(1)) for m in re.finditer(r'\b\d+; (\d+)/\d+/\d+', line))
    if sum(102 <= n <= 10240 for n in medians) < 5:
        raise SystemExit('Insufficient table medians in the stated approximate range')
    if not (8 * 1024 <= 8885 <= 12 * 1024 and 8 * 1024 <= 11466 <= 12 * 1024):
        raise SystemExit('Full-checkpoint quantiles outside approximate range')
    checked += 2
    checked += 5
    print(f'{checked:02d} planned cases, spike p50 and Sui value counts checked')
    with (HERE / 'engine-guard-initial.csv').open(newline='') as f:
        guard = list(csv.DictReader(f))
    guard_rows = [r for h, rows in found if h == ['Mode', 'Bytes', 'Upstream own lock', 'Upstream fork lock', 'Fork', 'Verdict'] for r in rows]
    if len(guard) != 36 or len(guard_rows) != 6:
        raise SystemExit('Engine guard case count mismatch')
    arm_columns = [('baseline', 'Upstream own lock'), ('baseline-forklock', 'Upstream fork lock'), ('fork', 'Fork')]
    by_case = defaultdict(list)
    for r in guard:
        if r['status'] != 'invalid: competing process' or float(r['firefox_max_percent']) <= 30:
            raise SystemExit('Engine guard interference verdict mismatch')
        by_case[(r['mode'], r['bytes'], r['arm'])].append(r)
        checked += 1
    for row in guard_rows:
        if row['Verdict'] != 'undecided: invalid':
            raise SystemExit('Engine guard row verdict mismatch')
        checked += 1
        for arm, col in arm_columns:
            samples = by_case[(row['Mode'], row['Bytes'], arm)]
            if len(samples) != 2 or {r['repeat'] for r in samples} != {'1', '2'}:
                raise SystemExit('Engine guard repetition mismatch')
            values = [int(r['target_ops_s']) for r in samples]
            shown = re.fullmatch(r'([\d,]+) \(([\d,]+)–([\d,]+)\)', row[col])
            if not shown:
                raise SystemExit('Engine guard summary malformed')
            for actual, expected in zip(shown.groups(), (sum(values) / 2, min(values), max(values))):
                if int(actual.replace(',', '')) != expected:
                    raise SystemExit('Engine guard throughput mismatch')
                checked += 1
    for mode, size in [('get', '64'), ('mixed', '64'), ('get', '1024')]:
        means = [statistics.mean(int(r['target_ops_s']) for r in by_case[(mode, size, arm)]) for arm, _ in arm_columns]
        if max(means) / min(means) >= 1.02:
            raise SystemExit('Engine guard cross-arm observation mismatch')
        checked += 1
    write64 = [statistics.mean(int(r['target_ops_s']) for r in by_case[('write', '64', arm)]) for arm in ('baseline', 'fork')]
    write64.append(next(int(r['target_ops_s']) for r in by_case[('write', '64', 'baseline-forklock')] if r['repeat'] == '2'))
    if max(write64) / min(write64) >= 1.02:
        raise SystemExit('Engine guard 64-byte write observation mismatch')
    write1024 = [int(r['target_ops_s']) for r in by_case[('write', '1024', 'baseline')]]
    if max(write1024) / min(write1024) <= 2:
        raise SystemExit('Engine guard 1 KiB write spread mismatch')
    checked += 2
    print(f'{checked:02d} checks including engine guard CSV consistency')
    raw_root = Path('/tmp/perf/results')
    raw_checked = 0
    for r in guard:
        name = f"{r['arm']}-{r['mode']}-{r['bytes']}-r{r['repeat']}.json"
        original = raw_root / 'first-attempts' / name
        path = original if original.is_file() else raw_root / name
        if not path.is_file():
            continue
        source = json.loads(path.read_text())
        expected_identity = (r['arm'], r['mode'], int(r['bytes']), int(r['repeat']))
        if (source['tree'], source['mode'], source['value_size_bytes'], source['repetition']) != expected_identity:
            raise SystemExit(f'Engine guard raw identity mismatch: {path}')
        rate = source['phases']['mixed']['operations_per_second_reported_rounded']
        firefox = [sample['cpu_percent_one_core'] for sample in source['competing_processes']
                   if 'firefox' in sample['command'].lower()]
        if (round(rate) != int(r['target_ops_s']) or not firefox
                or max(firefox) != float(r['firefox_max_percent']) or source['status'] != r['status']):
            raise SystemExit(f'Engine guard raw evidence mismatch: {path}')
        raw_checked += 1
    print(f'PASS: {checked} checks; engine guard: {raw_checked}/36 initial raw JSON records checked')


if __name__ == '__main__':
    main()
