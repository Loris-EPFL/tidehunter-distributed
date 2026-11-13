#!/usr/bin/env python3
"""Check contract tables against original CSVs and the final PDF; no builder imports."""
import csv
import hashlib
import json
import math
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_stage_values():
    derived = json.loads((HERE / 'contract-stage-metrics.json').read_text())
    raw = {name: Path(path) for name, path in derived['sources'].items()}
    timeline = json.loads(raw['write-timeline.json'].read_text())['timeline']
    records = timeline['records']
    with raw['samples.csv'].open(newline='') as stream:
        samples = [r for r in csv.DictReader(stream) if r['phase'] == 'measurement']
    assert len(records) == len(samples) == 4096
    assert all(r['completed'] and r['error'] is None for r in records)
    total_ns = sum(r['events'][-1]['elapsed_ns'] - r['events'][0]['elapsed_ns'] for r in records)
    expected = dict(samples=len(records), mean_total_us=total_ns / len(records) / 1000)
    intervals = {
        'queued': ('owner_enqueue_started', 'owner_dequeued'),
        'preparation': ('owner_dequeued', 'prepared'),
        'append_wait': ('admitted', 'append_enqueue_started'),
        'live_io': ('live_io_started', 'live_io_completed'),
    }
    selected_ns = 0
    for name, (start, end) in intervals.items():
        durations = []
        for record in records:
            pair, = [(a, b) for a, b in zip(record['events'], record['events'][1:])
                     if (a['stage'], b['stage']) == (start, end)]
            durations.append(pair[1]['elapsed_ns'] - pair[0]['elapsed_ns'])
        value = sum(durations)
        selected_ns += value
        expected[name + '_us'] = value / len(records) / 1000
        expected[name + '_share'] = 100 * value / total_ns
    expected.update(other_us=(total_ns - selected_ns) / len(records) / 1000,
                    other_share=100 * (total_ns - selected_ns) / total_ns, total_share=100)
    counters = json.loads(raw['measurement-read-evidence.json'].read_text())['live_nvmeof']['counters']
    writes = counters['leased_write_samples']
    assert writes == 258
    groups, skips = counters['leased_frames'], counters['leased_skip_markers']
    assert groups == 256 and skips == 2 and writes == groups + skips
    assert timeline['dropped_live_groups'] == 0 and len(timeline['live_groups']) == groups
    batch_ids = [(b['epoch'], b['sequence']) for g in timeline['live_groups'] for b in g['batches']]
    assert len(batch_ids) == len(set(batch_ids)) == len(records)
    assert set(batch_ids) == {(r['batch']['epoch'], r['batch']['sequence']) for r in records}
    wall = max(int(r['completed_ns']) for r in samples) - min(int(r['scheduled_ns']) for r in samples)
    write_ns, wal_bytes = counters['leased_write_ns'], counters['leased_bytes']
    expected.update(groups=groups, io_samples=writes, skip_markers=skips,
                    puts_per_group=len(samples) / groups, wall_ns=wall,
                    group_period_ms=wall / groups / 1e6, mean_leased_write_ms=write_ns / writes / 1e6,
                    group_mib=wal_bytes / groups / 1048576,
                    leased_mib_s=wal_bytes / 1048576 * 1e9 / wall,
                    payload_mib_s=sum(int(r['bytes']) for r in samples) / 1048576 * 1e9 / wall,
                    active_write_gib_s=wal_bytes / 1073741824 * 1e9 / write_ns,
                    write_busy_pct=100 * write_ns / wall)
    with (raw['samples.csv'].parents[4] / 'output/puts.csv').open(newline='') as stream:
        metadata, = [r for r in csv.DictReader(stream) if r['case_id'] == derived['case_id']]
    expected['callers'] = int(metadata['concurrency'])
    expected['serial_preparation_ms'] = expected['callers'] * expected['preparation_us'] / 1000
    for name, value in expected.items():
        assert math.isclose(derived[name], value, rel_tol=1e-12, abs_tol=1e-12), (name, derived[name], value)
    assert math.isclose(sum(expected[key + '_share'] for key in (*intervals, 'other')), 100)
    return dict(values=len(expected), samples=len(records), groups=groups, io_samples=writes,
                mean_total_us=expected['mean_total_us'], other_share=expected['other_share'])


def check_benchmark_plan():
    plan = json.loads((HERE / 'contract-benchmark-plan.json').read_text())
    source = Path(plan['source_path']).read_text()
    assert plan['source_excerpt'] in source and plan['matrix_evidence'] in source
    correction = plan['call_count_correction']
    assert correction in source
    assert plan['run_a_put_calls'] == int(re.search(r'([\d,]+) measured calls', correction)[1].replace(',', '')) == 4096
    read_counts = tuple(int(n.replace(',', '')) for n in
                        re.search(r'memory reads ([\d,]+); 1 KiB device reads ([\d,]+); 64 KiB device reads ([\d,]+)', correction).groups())
    assert tuple(plan[k] for k in ('run_a_memory_read_calls', 'run_a_device_1k_calls',
                                  'run_a_device_64k_calls')) == read_counts == (8192, 2048, 4096)
    rows = {int(i): int(count) for i, count in
            re.findall(r'^\| (\d+) \| G21[WRN] \| (\d+) \|', plan['source_excerpt'], re.M)}
    assert sum(rows.values()) == plan['run_a_cases'] == 34
    assert plan['run_a_w'] == sum(rows[i] for i in range(1, 6)) == 16
    assert plan['run_a_r'] == rows[6] + rows[7] == 8
    assert plan['run_a_n'] == sum(rows[i] for i in (8, 9, 10)) == 10
    assert plan['run_a_vms'] == 2 and plan['run_a_sui_cells'] == 4
    assert plan['run_b_vms'] == 6 and plan['run_b_storage_hosts'] == 5
    assert plan['run_a_leased_puts'] == rows[1] + rows[2]
    assert (plan['run_a_sustained'], plan['run_a_timelines'], plan['run_a_rpc']) == (rows[3], rows[4], rows[5])
    assert plan['matrix_sha256'] == 'cad4e58cc1e01a4977f7b7e3e0c87ed6ff00cfb15d498959472a511a8bab0208'
    assert plan['matrix_sha256'] in (HERE / 'acknowledgement-contracts.tex').read_text()
    return dict(run_a_cases=plan['run_a_cases'], matrix_sha256=plan['matrix_sha256'],
                status='planned, not run')


def check_contracts(pdf_text):
    manifest = json.loads((HERE / 'contract-inputs.json').read_text())
    for filename, wanted in manifest['sources'].items():
        assert digest(Path(filename)) == wanted, filename
    for filename, wanted in manifest['outputs'].items():
        assert digest(HERE / filename) == wanted, filename
    with (HERE / 'contract-values.csv').open(newline='') as stream:
        values = list(csv.DictReader(stream))
    assert len({r['label'] for r in values}) == len(values)
    stage_checks = check_stage_values()
    plan_checks = check_benchmark_plan()
    originals = {}
    for value in values:
        path = Path(value['source'])
        if path not in originals:
            if path.suffix == '.json':
                originals[path] = json.loads(path.read_text())
            else:
                with path.open(newline='') as stream:
                    originals[path] = list(csv.DictReader(stream))
        source = originals[path]
        if isinstance(source, list):
            source = [r for r in source if r['case_id'] == value['case_id']
                      and r.get('operation', '') == value['operation']]
            assert len(source) == 1, value
            source = source[0]
        assert str(source[value['column']]) == value['value'], value
    definitions = dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{((?:[^{}]|\{,\})*)\}',
                                  (HERE / 'numbers.tex').read_text()))
    indexed = {r['label']: r for r in values}
    checked = []
    for filename in ('contract-results.tex', 'tab-contract-a.tex', 'tab-contract-b-controls.tex',
                     'tab-contract-indicative.tex', 'contract-stages.tex', 'tab-contract-stages.tex',
                     'acknowledgement-contracts.tex'):
        source = (HERE / filename).read_text()
        for key in re.findall(r'\\N\{(B:contract-values:[^{}]+)\}', source):
            bare, _, fmt = key.partition('|')
            label = bare.split(':')[2]
            shown = format(float(indexed[label]['value']), fmt or '.1f')
            assert definitions[key].replace('{,}', ',') == shown, (key, shown)
            assert shown in pdf_text, (filename, key, shown)
            checked.append(dict(key=key, source=indexed[label]['source'],
                                column=indexed[label]['column'], case_id=indexed[label]['case_id']))
    paired = (HERE / 'tab-contract-a.tex').read_text()
    for line in paired.splitlines():
        if not line.startswith(('combined &', 'run13 &')):
            continue
        cells = line.split(' & ')
        run, kib, callers, rep = cells[:4]
        keys = re.findall(r'\\N\{B:contract-values:([^{}:]+):value\|[^{}]+\}', line)
        assert len(keys) == 6
        members = []
        for key in (keys[0], keys[3]):
            entry = indexed[key]
            records = originals[Path(entry['source'])]
            row, = [r for r in records if r['case_id'] == entry['case_id']]
            assert row['contract'] == 'page_cache'
            assert (int(row['value_size']) // 1024, row['concurrency'], row['repetition']) == (int(kib), callers, rep)
            assert ('combined-launch' in entry['source']) == (run == 'combined')
            members.append(row)
        assert members[0]['backend'] == 'tidehunter_nvme_rdma'
        assert members[0]['history_persistence'] == 'wal_replay'
        assert members[0]['lease_ahead'] == 'True' and members[0]['polled_writes'] == 'False'
        assert members[1]['backend'] == 'native'
    indicative = (HERE / 'tab-contract-indicative.tex').read_text()
    assert len(re.findall(r'^(?:put|read[^&]*) &', indicative, re.M)) == 11
    assert indicative.count('not measured') == 1
    missing, = [s for s in indicative.splitlines() if 'not measured' in s]
    assert missing.startswith('read: from device & 64 & 16 & not measured')
    memory_lines = [s for s in indicative.splitlines() if s.startswith('read') and 'from memory' in s]
    assert len(memory_lines) == 3
    historical_key = None
    for line in memory_lines:
        keys = re.findall(r'\\N\{B:contract-values:([^{}:]+):value\|[^{}]+\}', line)
        assert len(keys) == 2
        entry = indexed[keys[0]]
        row, = [r for r in originals[Path(entry['source'])] if r['case_id'] == entry['case_id']]
        assert row['media_expected'] == 'memory'
        assert (row['value_size'], row['concurrency'], row['contract']) == ('1024', '1', 'durable')
        if 'owner RPC' in line:
            assert row['backend'] == 'tidehunter_verbs_native'
        else:
            assert row['backend'] == 'tidehunter_nvme_rdma'
            assert row['poll_queues'] == ('1' if 'polled' in line else '0')
        assert historical_key in (None, keys[1])
        historical_key = keys[1]
    normalized = ' '.join(pdf_text.split())
    for phrase in ('Acknowledgement contracts', 'page-cache acknowledgement, one owner',
                   'No ranking follows', 'device sync not verified', 'No Sui replay result on hardware'):
        assert phrase in normalized, phrase
    return dict(pass_check=True, numeric_occurrences=len(checked), source_linked_values=len(values),
                sources=len(manifest['sources']), claims=checked, stages=stage_checks, plan=plan_checks,
                scope='retained A/B labels map to P/S; measured stage intervals; planned S/P/W benchmark map')


if __name__ == '__main__':
    text = subprocess.check_output(['pdftotext', '-layout', str(HERE / 'main.pdf'), '-'], text=True)
    result = check_contracts(text)
    print('PASS:', result['source_linked_values'], 'source-linked values;', result['numeric_occurrences'], 'PDF claims')
