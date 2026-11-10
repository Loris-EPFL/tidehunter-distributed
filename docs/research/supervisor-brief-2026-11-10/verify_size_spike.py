"""Independent checks of the added raw trace and raw sample evidence.

Does not import extraction, table-generation, number or plotting modules.
"""
import csv
import io
import math
import re
import tarfile
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]


def read_csv(path, delimiter=','):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream, delimiter=delimiter))


def rank(values, percentile):
    if not values:
        return ''
    return sorted(values)[max(0, (len(values) * percentile + 99) // 100 - 1)]


def check_sizes_spike(pages, definitions):
    trace = ROOT / 'checkpoint-work/sui-kv-trace-20261107/captures-corrected/remote'
    names = {}
    for path in trace.glob('db-*.tsv'):
        lines = [line.split('\t') for line in path.read_text().splitlines()]
        identity = dict((r[0], r[1]) for r in lines if r[0] != 'keyspace')
        namespace = bytes.fromhex(identity['path_hex']).decode().split('/full_node_db/')[1].split('/', 1)[1]
        names.update({(identity['db'], r[1]): (namespace, bytes.fromhex(r[2]).decode()) for r in lines if r[0] == 'keyspace'})
    # Index only completions and scalar-call kinds, then stream the raw observations again.
    kinds, completed, batch = {}, {}, {}
    with (trace / 'events.tsv').open(newline='') as stream:
        for event in csv.DictReader(stream, delimiter='\t'):
            if event['event'] == '1':
                kinds[event['call']] = event['op']
            if event['event'] == '2':
                completed[event['call']] = event['outcome']
            if event['event'] == '5':
                batch[event['group']] = event['call'], event['outcome']
    assert kinds.keys() <= completed.keys()
    values, large_calls = defaultdict(list), set()
    deletes = 0
    with (trace / 'events.tsv').open(newline='') as stream:
        for event in csv.DictReader(stream, delimiter='\t'):
            call, op = event['call'], event['op']
            if event['event'] == '4':
                if event['group'] not in batch:
                    continue
                call, outcome = batch[event['group']]
                if outcome != '1' or kinds[call] in ('5', '6'):
                    continue
            elif event['event'] in ('3', '8'):
                if op in ('5', '6') and completed.get(call) != '1':
                    continue
            else:
                continue
            if op == '6':
                deletes += 1
                continue
            if op != '5' and (event['outcome'] != '1' or op in ('2', '4')):
                continue
            operation = 'put' if op == '5' else 'read'
            namespace, table = names[event['db'], event['ks']]
            length = int(event['value_len'])
            values[namespace, table, operation].append(length)
            if length >= 65536:
                large_calls.add(call)
    sizes = read_csv(HERE / 'sui-value-sizes.csv')
    assert len({r['label'] for r in sizes}) == len(sizes)
    for row in sizes:
        population = (sum((items for (ns, table, op), items in values.items() if op == row['operation']), [])
                      if row['namespace'] == 'ALL' else values[row['namespace'], row['table'], row['operation']])
        expected = {'n': len(population), 'p50': rank(population, 50), 'p90': rank(population, 90),
                    'p99': rank(population, 99), 'maximum': max(population) if population else ''}
        for cutoff, name in [(8192, 'ge8'), (65536, 'ge64')]:
            expected[name] = sum(value >= cutoff for value in population)
            expected[name + '_pct'] = 100 * expected[name] / len(population) if population else ''
        expected.update({f'exact_{size}': population.count(size) for size in (64, 256, 1024, 8192, 65536)})
        for name, value in expected.items():
            assert row[name] == '' if value == '' else math.isclose(float(row[name]), value, rel_tol=1e-12), (row['label'], name, value)
    assert set(values) <= {(r['namespace'], r['table'], r['operation']) for r in sizes}
    totals = {r['operation']: r for r in sizes if r['namespace'] == 'ALL'}
    assert int(totals['read']['n']) == 15899 and int(totals['put']['n']) == 218513
    assert int(totals['read']['ge8']) == 1105 and int(totals['put']['ge8']) == 1116
    assert int(totals['read']['ge64']) == 2 and int(totals['put']['ge64']) == 6
    summary = {r['label']: float(r['value']) for r in read_csv(HERE / 'size-summary.csv')}
    count = sum(len(items) for items in values.values())
    large = sum(sum(value >= 65536 for value in items) for items in values.values())
    for label, value in {'value_ops': count, 'api_calls': len(completed), 'deletes': deletes,
                         'ge64_value_ops': large, 'ge64_value_pct': 100 * large / count,
                         'ge64_api_calls': len(large_calls), 'ge64_api_pct': 100 * len(large_calls) / len(completed)}.items():
        assert summary[label] == value
    selected_tables = {'checkpoint_content', 'full_checkpoint_content_v2', 'certified_checkpoints', 'effects', 'objects', 'transactions'}
    medians = [rank(v, 50)/1024 for (ns, table, op), v in values.items() if table in selected_tables and v]
    assert summary['typical_low_kib'] == 10 ** round(math.log10(min(medians)))
    assert summary['typical_high_kib'] == 10 ** round(math.log10(max(medians)))
    full = values['live/checkpoints', 'full_checkpoint_content_v2', 'read']
    assert summary['full_low_kib'] == math.floor(rank(full, 50)/1024)
    assert summary['full_high_kib'] == math.ceil(rank(full, 90)/1024)
    cases = read_csv(HERE / 'spike-cases.csv')
    assert Counter(r['mode'] for r in cases) == {'B': 6, 'P': 3, 'S': 12, 'V': 5}
    assert len({(r['source'], r['archive_member']) for r in cases}) == len(cases)
    archives = {}
    try:
        for case in cases:
            source = ROOT / case['source']
            if case['archive_member']:
                if source not in archives:
                    archives[source] = tarfile.open(source)
                with archives[source].extractfile(case['archive_member']) as stream:
                    raw = list(csv.DictReader(io.TextIOWrapper(stream)))
            else:
                raw = read_csv(source)
            selected = sorted((r for r in raw if (r['phase'], r['operation'], r['outcome']) == ('measurement', 'put', 'success')),
                              key=lambda r: int(r['dispatched_ns']))
            times = [int(r['api_ns']) / 1000 for r in selected]
            median = rank(times, 50)
            expected = {'n': len(times), 'p10_us': rank(times, 10), 'p50_us': median, 'p99_us': rank(times, 99),
                        'above_twice_pct': 100 * sum(v > 2 * median for v in times) / len(times)}
            for i in range(4):
                expected[f'q{i+1}_p50_us'] = rank(times[i*len(times)//4:(i+1)*len(times)//4], 50)
            for key, value in expected.items():
                assert float(case[key]) == value, (case['label'], key)
        assert all(int(r['n']) == 4096 for r in cases)
        for row in read_csv(HERE / 'spike-summary.csv'):
            mode, statistic = row['label'].split('_')
            measured = [float(case['p50_us']) for case in cases if case['mode'] == mode]
            assert float(row['value']) == (min(measured) if statistic == 'minimum' else max(measured))
    finally:
        for archive in archives.values():
            archive.close()
    context = read_csv(HERE / 'size-context.csv')
    for row in context:
        line = (ROOT / row['source']).read_text().splitlines()[int(row['line']) - 1]
        if row['label'].startswith('max_'):
            expression = re.search(row['label'] + r': Some\(([0-9 *]+)\)', line)[1]
            value = math.prod(int(term.strip()) for term in expression.split('*'))
            assert float(row['value']) == value
            assert float(row['kib']) == value/1024 and float(row['mib']) == value/1048576
        elif row['label'].startswith('figure_'):
            assert line.startswith('![](data:image/jpeg;base64,') and int(row['value']) in (256, 512)
        else:
            assert str(int(float(row['value']))) in line
    # Check every added B key, including generated table cells, against its raw-checked CSV.
    local_fair = check_local_fair()
    text = '\n'.join((HERE / name).read_text() for name in
                     ('cross-store-sizes.tex', 'cross-store-spike.tex', 'tab-sui-sizes.tex', 'tab-spike.tex',
                      'tab-local-structure.tex', 'cross-store-fair-native.tex', 'tab-fair-native.tex'))
    key_checks = []
    for key in set(re.findall(r'\\N\{(B:[^{}]+)\}', text)):
        bare, _, spec = key.partition('|')
        _, table, label, field = bare.split(':')
        row, = [r for r in read_csv(HERE / (table + '.csv')) if r['label'] == label]
        expected = format(float(row[field]), spec or '.1f')
        assert definitions[key].replace('{,}', ',') == expected, key
        assert expected in ' '.join(pages), (key, expected)
        key_checks.append(key)
    return {'trace_rows_checked': len(sizes), 'spike_populations_checked': len(cases),
            'modes': dict(Counter(r['mode'] for r in cases)), 'new_B_keys_checked': sorted(key_checks),
            'value_observations': count, 'large_value_observations': large,
            'large_value_percent': summary['ge64_value_pct'], 'large_call_percent': summary['ge64_api_pct'],
            'local_structure_and_plan': local_fair,
            'scope': 'one corrected remote private-chain capture including initialization; sizes only; no persistence/cache qualification or mainnet frequency'}


def check_local_fair():
    """Check audit transcriptions, not unavailable laptop samples; generate no runs."""
    import hashlib
    import json
    import sys
    local = read_csv(HERE / 'local-structure-test.csv')
    assert len(local) == len({r['label'] for r in local}) == 40
    assert {r['variant'] for r in local} == {
        'Fresh', 'MAP_POPULATE', 'Write-prefault', 'pwrite', 'Anonymous', 'Data sync', 'Grouped data sync'}
    audit_files = {}
    for row in local:
        cells = [cell.strip() for cell in row['report_row'].strip('|').split('|')]
        assert cells[:3] == [row['variant'], row['kib'], row['concurrency']]
        run = int(row['run'])
        values = [value.strip() for value in cells[2 + run].split('/')]
        assert [row['p50_us'], row['p99_us'], row['mean_us'], row['puts_s']] == values
        assert float(row['p50_ms']) == float(row['p50_us']) / 1000
        assert 'Subsequent IEEE CRC and missing controls' in row['provenance']
        assert f'IEEE run {run}' in row['provenance']
        assert 'raw samples not retained' in row['provenance']
        source = Path(row['report_path'])
        if source.exists():
            if source not in audit_files:
                audit_files[source] = source.read_text().splitlines()
            lines = audit_files[source]
            index = int(row['report_line']) - 1
            assert lines[index] == row['report_row']
            assert any(line.startswith('## Subsequent IEEE CRC and missing controls') for line in lines[:index])
    sys.path.insert(0, str(ROOT / 'tidehunter-sharded-wal/experiments/kv-comparison'))
    import cloud_campaign
    fair = cloud_campaign.make_focused_matrix('fair_native')
    parity = cloud_campaign.make_focused_matrix('parity')
    generated = {group: [job for job in fair if job['group'] == group]
                 for group in ('G14N', 'G14S', 'G14O1', 'G14O5', 'G14P')}
    generated.update(fair_native=fair, parity=parity, G13W=[j for j in parity if j['group'] == 'G13W'])
    for selection in ('recommended', 'full', 'device', 'floors', 'stages', 'unified'):
        generated['frozen_' + selection] = cloud_campaign.make_focused_matrix(selection)
    plan = read_csv(HERE / 'fair-native-plan.csv')
    assert {r['label'] for r in plan} == set(generated)
    for row in plan:
        jobs = generated[row['label']]
        assert int(row['cases']) == len(jobs)
        writers = sorted({job['concurrency'] for job in jobs})
        for i in range(3):
            assert row['writer' + str(i)] == (str(writers[i]) if i < len(writers) else '')
        assert row['matrix_sha256'] == hashlib.sha256(json.dumps(jobs, sort_keys=True).encode()).hexdigest()
    campaign = (ROOT / 'tidehunter-sharded-wal/docs/distributed/FOCUSED_CAMPAIGN_2026-11-07.md').read_text()
    for group, jobs in generated.items():
        if group.startswith('G14'):
            assert re.search(r'\| ' + group + r' \| ' + str(len(jobs)) + r' \|', campaign)
    return {'transcribed_ieee_rows': len(local), 'raw_local_samples': 'not retained; transcription checked against audit report',
            'matrix_counts': {label: len(jobs) for label, jobs in generated.items()},
            'status': 'planned, not run; read-only make_focused_matrix calls; no launch'}
