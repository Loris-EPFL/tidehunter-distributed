#!/usr/bin/env python3
"""Independent document acceptance. Stdlib only; no pipeline imports."""
import csv
import hashlib
import json
import math
import re
import statistics
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
SIZES = (64, 256, 1024, 8192, 65536)
LABELS = ('native', 'th_tcp', 'th_verbs', 'valkey_tcp', 'valkey_rdma', 'garnet',
          'kvrocks', 'cassandra', 'tikv', 'foundationdb', 'th_kernel', 'th_spdk')
SPECS = (('write-c16', 'write-concurrent', 'put', 'Scalar puts, c16.'),
         ('read-c16', 'read-concurrent', 'read', 'Single-value reads, primary RF1'),
         ('read32-c16', 'read-many32', 'get_many', '32-key reads, primary RF1'),
         ('point-read-c1', 'point-mixed', 'read', 'Point reads at c1 within a mixed stream'),
         ('point-put-c1', 'point-mixed', 'put', 'Point puts at c1 within a mixed stream'))


def rows(name):
    with (HERE / name).open(newline='') as handle:
        return list(csv.DictReader(handle))


def one(values, key):
    assert len(values) == 1, (key, len(values))
    return values[0]


def check_curves(pdf_pages=None):
    populations, run_rows = rows('populations.csv'), rows('curve-runs.csv')
    evidence = json.loads((HERE / 'evidence.json').read_text())
    assert 'Native resident-map reads' not in json.dumps(evidence)
    if pdf_pages is None:
        text = subprocess.check_output(['pdftotext', '-layout', str(HERE / 'main.pdf'), '-'], text=True)
        pdf_pages = [p for p in text.split('\f') if p.strip()]
    pages = [' '.join(p.split()) for p in pdf_pages]
    all_text = ' '.join(pages)
    definitions = dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{((?:[^{}]|\{,\})*)\}', (HERE / 'numbers.tex').read_text()))

    def select(label, cohort, profile, operation, size):
        return one([r for r in populations if r['label'] == label and r['cohort'] == cohort
                    and r['profile'] == profile and r['operation'] == operation and r['outcome'] == 'success'
                    and r['status'] == 'completed' and r['value_bytes'] == str(size) and r['durability_tier'] != 'native'],
                   (label, cohort, profile, operation, size))

    # Check each extracted contributor against its original report, not only the pipeline output.
    originals, raw_checks = {}, []
    for sample in run_rows:
        source = ROOT / sample['source']
        if source not in originals:
            if source.suffix == '.csv':
                with source.open(newline='') as handle:
                    originals[source] = list(csv.DictReader(handle))
            else:
                originals[source] = json.loads(source.read_text())['cases']
        report = originals[source]
        if source.suffix == '.csv':
            original = one([r for r in report if r['case_id'] == sample['case_id'] and r['operation'] == sample['operation']
                            and r['outcome'] == 'success'], sample['case_id'])
            expected_metrics = {k: float(original[k]) for k in ('api_p50_us', 'api_p99_us', 'response_p99_us', 'api_mean_us', 'case_success_calls_per_second')}
            same_case = [r for r in report if r['case_id'] == sample['case_id'] and r['outcome'] == 'success']
            occupancy = sum(float(r['population_calls']) * float(r['api_mean_us']) for r in same_case) / 1e6 / float(original['elapsed_seconds'])
        else:
            case = one([r for r in report if r['case_id'] == sample['case_id']], sample['case_id'])
            metrics = case['metrics']
            pop = one([r for r in metrics['populations'] if r['operation'] == sample['operation'] and r['outcome'] == 'success'], sample['case_id'])
            expected_metrics = dict(api_p50_us=pop['api_ns']['p50']/1000, api_p99_us=pop['api_ns']['p99']/1000,
                                    response_p99_us=pop['response_ns']['p99']/1000, api_mean_us=pop['api_mean_ns']/1000,
                                    case_success_calls_per_second=metrics['success_calls_per_second'])
            occupancy = sum(r['count'] * r['api_mean_ns'] for r in metrics['populations'] if r['outcome'] == 'success') / 1e9 / metrics['elapsed_seconds']
        for metric, value in expected_metrics.items():
            assert math.isclose(value, float(sample[metric]), rel_tol=1e-12), (sample['case_id'], metric)
        assert math.isclose(occupancy, float(sample['mean_inflight_calls']), rel_tol=1e-12)
        raw_checks.append(dict(run=sample['run'], case_id=sample['case_id'], operation=sample['operation'], mean_inflight_calls=occupancy))

    results, claims = {}, []
    for stem, profile, operation, caption in SPECS:
        name = f'curves-{stem}.pdf'
        figure = evidence['figures'][name]
        assert figure['run'] == 'cross-store-context' and 'historical' in figure['fleet']
        assert figure['sha256'] == hashlib.sha256((HERE / 'figures' / name).read_bytes()).hexdigest()
        page_number = one([i+1 for i,p in enumerate(pages) if caption in p], caption)
        fourth = 'api_mean_us' if profile == 'point-mixed' else 'case_success_calls_per_second' if operation == 'put' else 'mean_inflight_calls'
        metrics = ('api_p50_us', 'api_p99_us', 'response_p99_us', fourth)
        assert figure['metrics'] == list(metrics)
        expected = {}
        for label in LABELS + (('native_pagecache',) if operation == 'put' else ()):
            cohort = ('native_pagecache' if profile == 'point-mixed' else 'native_async_write') if label == 'native_pagecache' else 'primary_rf1'
            for size in SIZES:
                row = select('native' if label == 'native_pagecache' else label, cohort, profile, operation, size)
                contributors = [r for r in run_rows if r['label'] == row['label'] and r['cohort'] == cohort and r['profile'] == profile
                                and r['operation'] == operation and r['size'] == str(size)]
                assert len(contributors) == int(row['median_runs'] or 1)
                for metric in metrics:
                    if label == 'native_pagecache' and metric == 'case_success_calls_per_second':
                        continue
                    value = statistics.median(float(r[metric]) for r in contributors)
                    if metric != 'mean_inflight_calls':
                        assert math.isclose(value, float(row[metric]), rel_tol=1e-12)
                    expected[label, size, metric] = (row, contributors, value)
        assert len(figure['points']) == len(expected)
        seen = set()
        for point in figure['points']:
            key = point['label'], point['size'], point['metric']
            assert key not in seen
            seen.add(key)
            row, contributors, value = expected[key]
            assert point['case_id'] == row['case_id'] and point['cohort'] == row['cohort']
            assert point['median_runs'] == row['median_runs'] and point['repetition_count'] == len(contributors)
            assert math.isclose(point['value'], value, rel_tol=1e-12)
            assert len(point['per_run']) == len(contributors)
            for contributor in point['per_run']:
                sample = one([r for r in contributors if r['run'] == contributor['run']], contributor)
                assert contributor['case_id'] == sample['case_id'] and contributor['value'] == float(sample[point['metric']])
        assert seen == set(expected)
        assert not figure['missing_points']
        assert all(len(t['values']) >= 3 for t in figure['y_ticks'])
        results[name] = dict(values_checked=len(expected), page=page_number, fourth_panel=fourth,
                             native_pagecache_points=sum(k[0] == 'native_pagecache' for k in expected),
                             hollow_write_markers=['cassandra', 'foundationdb', 'tikv'] if operation == 'put' else [])

    # Every curve caption/reading number: evaluate directly from CSV and locate it on its figure page.
    tex = (HERE / 'cross-store-curves.tex').read_text()
    for index, (stem, profile, operation, _) in enumerate(SPECS):
        section = tex.split('\\begin{figure}[H]')[index+1]
        page = results[f'curves-{stem}.pdf']['page']
        for key in re.findall(r'\\N\{([^{}]+)\}', section):
            bare, _, fmt = key.partition('|')
            kind, *args = bare.split(':')
            if kind == 'curve':
                p, op, field = args
                selected = [select(l, 'primary_rf1', p, op, s) for l in LABELS for s in SIZES]
                value = one(list({int(r['population_calls' if field == 'calls' else 'planned_calls']) for r in selected}), key)
                displayed = format(value, fmt or ',d')
            else:
                label, cohort, p, op, size = args
                row = select(label, cohort, p, op, size)
                metric = {'p50':'api_p50_us','p99':'api_p99_us','p99ms':'api_p99_us'}[kind]
                value = float(row[metric]) / (1000 if kind.endswith('ms') else 1)
                displayed = format(value, fmt) if fmt else (f'{value:.2f}' if value < 10 else f'{value:.1f}' if value < 100 else f'{value:,.0f}')
            assert definitions[key].replace('{,}', ',') == displayed, key
            assert displayed in pages[page-1], (key, page, displayed)
            claims.append(dict(key=key, source_value=value, pdf_value=displayed, page=page, pass_check=True))

    # Independently recompute every new per-node cell and verify its printed number key.
    topology = {r['label']: r for r in rows('storage-topology.csv')}
    pernode_checks = []
    for table in rows('per-node-64k.csv'):
        label = table['label']
        row = select(label, 'primary_rf1', 'write-concurrent', 'put', 65536)
        mixed = select(label, 'primary_rf1', 'point-mixed', 'put', 65536)
        options = json.loads(Path(topology[label]['source']).read_text())['backend']['options']
        if label == 'native':
            nodes = 1
        elif label.startswith('th_'):
            nodes = len(options['external_owner_durability_receipts'])
        elif 'nodes' in options:
            nodes = len({address.split(':')[0] for address in options['nodes']})
        else:
            # The one final JSON is already a recorded hashed input of curve-runs.csv.
            final_source = ROOT / one([r for r in run_rows if r['label'] == label and r['run'] == 'final' and r['case_id'] == row['case_id']], label)['source']
            full_json = json.loads(final_source.with_suffix('.json').read_text())
            backend = one([c for c in full_json['cases'] if c['case_id'] == row['case_id']], label)['backend']['topology']
            if label == 'tikv':
                nodes = len({s['store']['address'].split(':')[0] for s in backend['store_metadata']['stores']})
            else:
                nodes = len({p['address'].split(':')[0] for p in backend['cluster_status']['cluster']['processes'].values()
                             if any(role['role'] == 'storage' for role in p.get('roles', []))})
        rate = float(row['case_success_calls_per_second'])
        inflight = statistics.median(float(r['mean_inflight_calls']) for r in run_rows if r['label'] == label and r['cohort'] == 'primary_rf1'
                                    and r['profile'] == 'write-concurrent' and r['size'] == '65536')
        wanted = dict(storage_nodes=nodes, callers_per_node=16/nodes, puts_s=rate, puts_s_node=rate/nodes,
                      payload_mibs_node=rate/nodes/16, p50_us=float(row['api_p50_us']), mean_us=float(row['api_mean_us']),
                      c1_mixed_p50_us=float(mixed['api_p50_us']), mean_inflight_calls=inflight)
        display = {'native': 'Native', 'th_tcp': 'TH TCP', 'th_verbs': 'TH verbs',
                   'th_kernel': 'TH NVMe-oF kernel', 'th_spdk': 'TH NVMe-oF SPDK',
                   'valkey_tcp': 'Valkey TCP', 'valkey_rdma': 'Valkey RDMA',
                   'garnet': 'Garnet', 'kvrocks': 'Kvrocks', 'cassandra': 'Cassandra',
                   'tikv': 'TiKV', 'foundationdb': 'FoundationDB'}[label]
        contract = ('barrier' if label == 'native' else 'sync' if label.startswith('th_') else
                    'AOF' if label.startswith('valkey') else 'commit' if label == 'garnet' else
                    'WAL' if label == 'kvrocks' else 'unverified')
        row_label = display + '/' + contract
        table_page = one([p for p in pdf_pages if 'Even spread assumed for callers' in ' '.join(p.split())], 'per-node page')
        printed_row = one([line.strip() for line in table_page.splitlines() if line.strip().startswith(row_label + ' ')], row_label)
        printed_cells = printed_row[len(row_label):].split()
        assert len(printed_cells) == len(wanted), printed_row
        for column, (field, value) in enumerate(wanted.items()):
            assert math.isclose(float(table[field]), value, rel_tol=1e-12), (label, field)
            fmt = '.0f' if field == 'storage_nodes' else ',.0f' if field in ('puts_s','puts_s_node') else '.2f' if field == 'mean_inflight_calls' else '.1f'
            key = f'B:per-node-64k:{label}:{field}|{fmt}'
            assert definitions[key].replace('{,}', ',') == format(value, fmt)
            assert printed_cells[column] == format(value, fmt), (label, field, printed_cells)
            pernode_checks.append(dict(label=label, field=field, value=value, key=key))
    assert evidence['per_node_table']['rows'] == rows('per-node-64k.csv')
    # Device spread must exclude hosts[0], the coordinator, and use only storage hosts[1:].
    device_checks = []
    for spread in rows('device-write-spread.csv'):
        directory = Path(topology[spread['label']]['source']).parent
        before = json.loads((directory / 'measurement-before.json').read_text())
        after = json.loads((directory / 'measurement-end.json').read_text())
        assert len(before['hosts']) == len(after['hosts']) == 6
        counts = []
        for index in range(1, 6):
            def completed(host):
                return int(one([line.split() for line in host['diskstats'].splitlines()
                                if line.split()[2] == 'nvme0n1'], index)[7])
            counts.append(completed(after['hosts'][index]) - completed(before['hosts'][index]))
        assert json.loads(spread['counts']) == counts
        assert float(spread['minimum']) == min(counts) and float(spread['maximum']) == max(counts)
        assert float(spread['ratio']) == max(counts) / min(counts)
        device_checks.append(dict(label=spread['label'], storage_host_indices=list(range(1, 6)), counts=counts))
    for anomaly in rows('chart-anomalies.csv'):
        with Path(anomaly['source']).open(newline='') as handle:
            stalls = [int(r['api_ns']) / 1e6 for r in csv.DictReader(handle)
                      if r['phase'] == 'measurement' and r['operation'] == 'put'
                      and r['outcome'] == 'success' and int(r['api_ns']) >= 10_000_000]
        assert len(stalls) == int(anomaly['stall_count'])
        assert min(stalls) == float(anomaly['stall_min_ms']) and max(stalls) == float(anomaly['stall_max_ms'])
    historical_checks = []
    for record in rows('historical-topology.csv'):
        report = originals[ROOT / record['source']]
        if record['source'].endswith('.json'):
            original = one([c for c in report if c['case_id'] == record['case_id']], record['case_id'])
            topology_value = original['backend']['topology']
        else:
            original = one([r for r in report if r['case_id'] == record['case_id']
                            and r['operation'] == record['operation'] and r['outcome'] == 'success'], record['case_id'])
            topology_value = json.loads(original['observed_topology'])
        if record['label'] == 'foundationdb':
            count = len({p.get('machine_id', p['address'].split(':')[0])
                         for p in topology_value['cluster_status']['cluster']['processes'].values()
                         if any(role['role'] == 'storage' for role in p.get('roles', []))})
        elif record['label'] == 'tikv':
            count = topology_value['store_metadata']['count']
        elif record['label'] == 'cassandra':
            count = topology_value['contact_node_count']
        else:
            count = topology_value['primary_count']
        assert int(record['storage_node_count']) == count == 5
        historical_checks.append(dict(run=record['run'], case_id=record['case_id'],
                                      operation=record['operation'], count=count, evidence_kind=record['evidence_kind']))
    assert len(historical_checks) == sum(r['label'] != 'native' and not r['label'].startswith('th_') for r in run_rows)

    # Every measured number introduced in the explanation, not just its table.
    explanation = (HERE / 'cross-store-explanation.tex').read_text() + (HERE / 'cross-store-paper.tex').read_text()
    components, operations = rows('run8-component_summary.csv'), rows('run8-ops_summary.csv')
    # Select the timeline population by its explicit shape/contract/instrument, not a guessed case id.
    selected_ops = {}
    for alias, size, concurrency in [('p16v', 1024, 16), ('p64v', 65536, 1)]:
        selected_ops[alias] = one([r for r in operations if r['group'] == 'G12D' and r['route'] == 'owner verbs' and r['shape'] == 'put'
                                   and r['size_bytes'] == str(size) and r['concurrency'] == str(concurrency)
                                   and r['durability'] == 'synchronous' and r['key_policy'] == 'overwrite'
                                   and 'write_timeline_limit' in json.loads(r['diagnostics'])], alias)
    selected_ops['uput1v'] = one([r for r in operations if r['group'] == 'G12W' and r['route'] == 'owner verbs'
                                 and r['shape'] == 'put' and r['size_bytes'] == '1024' and r['concurrency'] == '1'
                                 and r['durability'] == 'synchronous' and r['key_policy'] == 'overwrite'
                                 and json.loads(r['diagnostics']) == {}], 'uput1v')
    names = {'coordinator': 'client coordinator', 'owner': 'owner software',
             'durable': 'durable wait and publication', 'coalescing': 'waiting and coalescing',
             'queue': 'client queueing and wake-up'}
    def component(alias, name):
        ident = one(json.loads(selected_ops[alias]['case_ids']), alias)
        return float(one([r for r in components if r['case_id'] == ident and r['component'] == names[name]],
                         (alias, name))['mean_us'])
    native16 = select('native', 'primary_rf1', 'write-concurrent', 'put', 65536)
    native1 = select('native', 'primary_rf1', 'point-mixed', 'put', 65536)
    pc64 = select('native', 'native_async_write', 'write-concurrent', 'put', 65536)
    pc8 = select('native', 'native_async_write', 'write-concurrent', 'put', 8192)
    pc1 = select('native', 'native_async_write', 'write-concurrent', 'put', 1024)
    waits = sum(component('p16v', name) for name in ('durable', 'coalescing', 'queue'))
    derived = {'native_mean_ratio': float(native16['api_mean_us']) / float(native1['api_mean_us']),
               'native_reciprocal': 1e6 / float(native1['api_mean_us']),
               'native_reciprocal_pct': float(native16['case_success_calls_per_second']) * float(native1['api_mean_us']) / 1e4,
               'pagecache64': float(pc64['api_p50_us']), 'pagecache8': float(pc8['api_p50_us']),
               'pagecache_c1_64': float(select('native', 'native_pagecache', 'point-mixed', 'put', 65536)['api_p50_us']),
               'pagecache_jump': float(pc64['api_p50_us']) / float(pc8['api_p50_us']),
               'pagecache1_cps': float(pc1['case_success_calls_per_second']), 'pagecache_ms': float(pc1['elapsed_seconds']) * 1000,
               'owner64_software_pct': 100 * (component('p64v', 'coordinator') + component('p64v', 'owner')) / float(selected_ops['p64v']['mean_us'])}
    for key in set(re.findall(r'\\N\{([^{}]+)\}', explanation)):
        bare, _, fmt = key.partition('|')
        kind, *args = bare.split(':')
        if kind == 'B':
            table, label, field = args
            value = float(one([r for r in rows(table + '.csv') if r.get('label', r.get('key')) == label], key)[field])
        elif kind == 'J':
            value = derived[args[0]]
        elif kind == 'W':
            value = 100 * waits / float(selected_ops['p16v']['mean_us'])
        elif kind == 'C':
            value = component(*args)
        elif kind == 'D':
            alias, field = args
            value = float(selected_ops[alias][field])
        else:
            label, cohort, profile, op, size = args
            row = select(label, cohort, profile, op, size)
            field = {'p50': 'api_p50_us', 'mean': 'api_mean_us', 'cps': 'case_success_calls_per_second', 'p99ms': 'api_p99_us'}[kind]
            value = float(row[field]) / (1000 if kind.endswith('ms') else 1)
        shown = format(value, fmt or '.1f')
        assert definitions[key].replace('{,}', ',') == shown, (key, shown)
        assert shown in all_text, (key, shown)
        claims.append(dict(key=key, source_value=value, pdf_value=shown, pass_check=True))
    paper = (ROOT / 'papers/parsed/Tidehunter-2026-v2.md').read_text()
    for fact in rows('paper-context.csv'):
        assert fact['matched_text'] in paper
        captured = float(re.search(r'(\d+(?:\.\d+)?)', fact['matched_text'])[1])
        assert float(fact['value']) == captured * (1e6 if 'M ops/s' in fact['matched_text'] else 1e3)
    return dict(figures=results, independent_plotted_value_checks=sum(r['values_checked'] for r in results.values()),
                evidence_completeness='All plotted points and per-run contributors independently checked against source reports',
                number_keys_checked=len({r['key'] for r in claims}), hand_checked_claims=claims,
                raw_contributor_checks=raw_checks, per_node_table_checks=pernode_checks, device_spread_checks=device_checks,
                historical_topology_checks=historical_checks,
                mixed_rate_decision='Mixed c1 panel four now shows operation API mean; no duplicated whole-case rate.',
                contradictions=['Native page-cache occupancy <=1.26 only through 8 KiB; 64 KiB is 8.40.',
                                'Device write-count max/min is 1.36 for Valkey and 1.07 for owner verbs, not within about 25%.',
                                'Native reciprocal c1 mean is 6396.03 puts/s, rounds to 6396 not 6397.',
                                'Serialized-service model does not imply nonoverlapping API calls.',
                                'Run-8 software shares suggest possible limits, not proof that SSD is not limiting.',
                                'Paper size summary applies to the main evaluation; supplied Figure 1 facts include additional small sizes.',
                                'FoundationDB has five storage-role hosts plus a separate transaction/log host; storage-node count is not total persistence-device count.'])


if __name__ == '__main__':
    result = check_curves()
    print(json.dumps(result['figures'], indent=2))
    print('PASS:', result['independent_plotted_value_checks'], 'curve values;', len(result['raw_contributor_checks']),
          'source contributor checks;', len(result['per_node_table_checks']), 'per-node cells.')
