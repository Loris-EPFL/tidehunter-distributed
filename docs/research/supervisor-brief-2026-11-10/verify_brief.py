#!/usr/bin/env python3
"""Independent acceptance checks. Does not import any brief pipeline module.

Read source CSVs directly. Match PDF table rows to source cases and columns.
Recompute owner extracts from raw JSON when present. Never run repository suites.
"""
import csv
import hashlib
import json
import math
import re
import statistics
import subprocess
import xml.etree.ElementTree as ET
import unicodedata
from pathlib import Path

from verify_curves import check_curves
from verify_size_spike import check_sizes_spike
from verify_contracts import check_contracts
HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[3]
RUN8 = WORKSPACE / 'checkpoint-work/focused-launch-20261109-unified2/analysis'
RUN7 = WORKSPACE / 'checkpoint-work/focused-launch-20261109-g10b/analysis-final-v2'
OLD = HERE.parent / 'supervisor-brief-2026-11-07'


def csv_rows(path):
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def unique(rows, **selection):
    chosen = [r for r in rows if all(r[k] == str(v) for k, v in selection.items())]
    assert len(chosen) == 1, (selection, len(chosen))
    return chosen[0]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = json.loads((HERE / 'inputs.json').read_text())
    source_checks = {}
    for name, wanted in manifest['outputs'].items():
        assert digest(HERE / name) == wanted, name
    for name, wanted in manifest['sources'].items():
        path = Path(name)
        if path.exists():
            assert digest(path) == wanted, name
            source_checks[name] = 'hash verified'
        else:
            source_checks[name] = 'original unavailable; copied hash checked'
    # Prefer originals, but allow the versioned CSV-only rebuild documented in README.
    op_path = RUN8 / 'output/ops_summary.csv'
    component_path = RUN8 / 'output/component_summary.csv'
    op_path = op_path if op_path.exists() else HERE / 'run8-ops_summary.csv'
    component_path = component_path if component_path.exists() else HERE / 'run8-component_summary.csv'
    operations, components = csv_rows(op_path), csv_rows(component_path)
    text = subprocess.check_output(['pdftotext', '-layout', str(HERE / 'main.pdf'), '-'], text=True)
    (HERE / 'main.txt').write_text(text)
    pages = [p for p in text.split('\f') if p.strip()]
    assert len(pages) <= 56, len(pages)
    def page_for(heading):
        pattern = re.compile(r'(?:\d+(?:\.\d+)*|[A-Z])\s+' + re.escape(heading))
        found = [i+1 for i,p in enumerate(pages) if any(pattern.fullmatch(line.strip())
                 for line in unicodedata.normalize('NFKC', p).splitlines())]
        assert len(found) == 1, (heading, found)
        return found[0]
    # Historical claims retain their selectors; only their layout positions can move.
    legacy_headings = {3: 'Where the time goes: durable writes', 4: 'Value size and concurrency',
                       5: 'Owner hand-offs and nested annotations', 8: 'Serving-node memory',
                       9: 'SSD-served reads and native cold qualification',
                       11: 'Timed sync steps inside each system', 13: 'Put stall',
                       15: 'Run 7 puts and durability cost'}
    legacy_page = lambda n: page_for(legacy_headings[n])
    assert 'Scope and findings' in pages[0] and 'Cross-store results stay separate' in pages[0]
    assert any('Reads' in p and 'case-level SSD-service qualification' in p for p in pages)
    normalized = ' '.join(text.split())
    section_headings = ('Topology, contracts and the Native result', 'Cross-store curves',
                        'Agreement with the Tidehunter paper', 'Where 64 KiB values occur')
    section_positions = [normalized.index(heading) for heading in section_headings]
    assert section_positions == sorted(section_positions)
    log = (HERE / 'main.log').read_text()
    bad = [line for line in log.splitlines() if re.search(
        r'Overfull|Underfull|undefined reference|LaTeX Warning|Missing number|Missing character', line)]
    assert not bad, bad
    definitions = dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{((?:[^{}]|\{,\})*)\}', (HERE / 'numbers.tex').read_text()))
    baseline = json.loads((HERE / 'number-baseline.json').read_text())
    assert set(baseline) <= set(definitions)
    changed_keys = {key: {'old': value, 'new': definitions[key]} for key, value in baseline.items() if definitions[key] != value}
    assert len(changed_keys) == 12
    assert all(re.fullmatch(r'Q:run7-sustained-extract:\d+:late_puts_s\|\.0f', key) for key in changed_keys)
    sustained_checks = []
    extracts = csv_rows(HERE / 'run7-sustained-extract.csv')
    for row in csv_rows(RUN7 / 'output/sustained_summary.csv'):
        complete = json.loads(row['last_five_seconds'])
        assert len(complete) == 5
        assert all(bucket['bucket_status'] == 'complete' and bucket['duration_ns'] == 1000000000 for bucket in complete)
        late = statistics.fmean(bucket['put_count'] for bucket in complete)
        extracted = unique(extracts, case_id=row['case_id'])
        assert float(extracted['late_puts_s']) == late
        assert float(extracted['average_puts_s']) == float(row['successful_calls_per_second'])
        assert late < float(extracted['first_puts_s'])
        sustained_checks.append(dict(case_id=row['case_id'], late_puts_s=late, complete_bucket_indices=[b['second'] for b in complete]))
    network_checks = []
    for row in csv_rows(HERE / 'run8-net_floor.csv'):
        if row['tool'] in ('ib_send_lat', 'ib_write_lat', 'ib_read_lat') and row['status'] == 'completed':
            factor = 2 if row['tool'] in ('ib_send_lat', 'ib_write_lat') else 1
            assert row['latency_semantics'] == ('half_round_trip' if factor == 2 else 'round_trip')
            assert math.isclose(float(row['round_trip_p50_us']), factor * float(row['p50_us']), abs_tol=1e-12)
            network_checks.append(dict(case_id=row['case_id'], tool=row['tool'], multiplier=factor))
    assert 'Output written on main.pdf' in log
    assert (HERE / 'main.pdf').stat().st_size > 0

    checks = []
    number_pattern = re.compile(r'(?<![A-Za-z0-9])\d+(?:[,.]\d+)*(?![A-Za-z0-9])')

    def table_claim(page_number, row_label, column, source_value, source_file, case_id, metric, decimals=1):
        page_number = legacy_page(page_number)
        candidates = [line for line in pages[page_number - 1].splitlines()
                      if re.match(re.escape(row_label) + r'(?:a)?\s', line.strip())]
        assert len(candidates) == 1, (page_number, row_label, candidates)
        row = candidates[0].strip()[len(row_label):]
        cells = number_pattern.findall(row)
        actual = cells[column].replace(',', '')
        expected = f'{float(source_value):.{decimals}f}'
        assert actual == expected, (page_number, row_label, column, actual, expected)
        checks.append(dict(claim=f'{row_label}; column {column + 1}; {metric}',
                           source=str(source_file), case_id=case_id, source_value=float(source_value),
                           pdf_value=actual, page=page_number, pass_check=True))

    def operation(case):
        selected = [r for r in operations if case in json.loads(r['case_ids'])]
        assert len(selected) == 1
        assert len(json.loads(selected[0]['case_ids'])) == 1
        return selected[0]

    cases1 = ['G12D-r0-08a9a20ff91943a18559', 'G12D-r0-946e08ec43aceab58b29',
              'G12D-r0-5ac5e1c809e1545074bf', 'G12D-r0-ca7078fc585acc3331ce',
              'G12D-r0-f154e627c9d924039b33', 'G12D-r0-be14bc607d9e72aafdea']
    for j, case in enumerate(cases1):
        row = operation(case)
        for label, metric in [('TOTAL: same-case API mean', 'mean_us'), ('API p50', 'p50_us')]:
            table_claim(3, label, j, row[metric], op_path, case, metric)
    for component, label, cases in [
        ('network (wire, both stacks, TLS, dispatch)', 'Network (stacks, TLS, dispatch included)', cases1[:5]),
        ('owner persist (file sync, includes software)', 'Owner append-persist (includes software)', cases1[:5]),
        ('client coordinator', 'Coordinator / native library insert', cases1)]:
        for j, case in enumerate(cases):
            source = unique(components, case_id=case, component=component)
            table_claim(3, label, j, source['mean_us'], component_path, case, component)
    for page_number, cases in [(4, ['G12D-r0-9eaa39f00ad063fef4cf', 'G12D-r0-e44182bb7585ecb016b5',
                                    'G12D-r0-2d96d2b8eeab0e23aab1']),
                               (8, ['G12D-r0-85bb4645de9ec6ac2c6b', 'G12D-r0-f9afdb4474380265a761',
                                    'G12D-r0-c43d911a13fcb571c6dc', 'G12D-r0-2d105120fe757255aaa3']),
                               (9, ['G12D-r0-321ba7273072eec9f5f4', 'G12D-r0-f3e5e748b856e9aafc6a',
                                    'G12D-r0-a2dfb7244367ba447ecf', 'G12D-r0-6c4d9d94da989fe10037',
                                    'G12D-r0-fdfd5c02439e7284baef'])]:
        page_number = legacy_page(page_number)
        # Pages 4, 8 and 9 contain two component tables. Select the first one.
        first_table_end = pages[page_number - 1].index('API p50')
        prefix = pages[page_number - 1][:first_table_end]
        for label, metric in [('TOTAL: same-case API mean', 'mean_us')]:
            line = next(l for l in prefix.splitlines() if l.strip().startswith(label))
            values = number_pattern.findall(line[len(line) - len(line.lstrip()):][len(label):])
            for j, case in enumerate(cases):
                source = operation(case)
                shown = values[j]
                assert shown == f"{float(source[metric]):.1f}", (case, shown)
                checks.append(dict(claim=f'{label}; column {j + 1}', source=str(op_path),
                                   case_id=case, metric=metric, source_value=float(source[metric]),
                                   pdf_value=shown, page=page_number, pass_check=True))

    sync_path = RUN8 / 'output/sync_step.csv'
    sync_path = sync_path if sync_path.exists() else HERE / 'run8-sync_step.csv'
    sync_rows = csv_rows(sync_path)
    for route, size, concurrency in [('owner TCP', 1024, 1), ('owner verbs', 1024, 1),
                                      ('native', 1024, 1), ('owner TCP', 65536, 1),
                                      ('owner verbs', 65536, 1), ('native', 65536, 1)]:
        step = 'file + directory fsync' if route == 'native' else 'file fdatasync'
        source = unique(sync_rows, route=route, size_bytes=size, concurrency=concurrency, step=step)
        label = f'{route}, {size // 1024} KiB c{concurrency}'
        for column, metric in [(1, 'mean_us'), (2, 'p50_us'), (3, 'p99_us')]:
            if source[metric] != 'missing':
                table_claim(11, label, column, source[metric], sync_path, source['case_id'], metric)
        if route == 'native':
            assert source['p50_us'] == source['p99_us'] == 'missing'

    raw_checks = []
    stages = csv_rows(HERE / 'owner-stages.csv')
    for case in sorted({r['case_id'] for r in stages}):
        raw_path = RUN8 / 'report-root/cases' / case / 'result/write-timeline.json'
        if not raw_path.exists():
            raw_checks.append(dict(case_id=case, status='raw source unavailable; extract hash verified'))
            continue
        records = json.loads(raw_path.read_text())['timeline']['owner_records']
        assert len(records) == 4096
        for target in [r for r in stages if r['case_id'] == case]:
            field = target['field']
            if field == 'handler_remainder_ns':
                values = [r['handler_ns'] - r['flusher_queue_ns'] - r['append_wal_ns'] - r['append_persist_ns'] for r in records]
            elif field == 'persist_software_ns':
                values = [r['append_persist_ns'] - r['wal_file_persist_ns'] for r in records]
            else:
                values = [r[field] for r in records]
            mean = statistics.fmean(values) / 1000
            quantile = sorted(values)[math.ceil(.5 * len(values)) - 1] / 1000
            assert math.isclose(mean, float(target['mean_us']), abs_tol=1e-10)
            assert quantile == float(target['p50_us'])
            raw_checks.append(dict(case_id=case, field=field, n=len(values), mean_us=mean, p50_us=quantile,
                                   status='independently recomputed from raw owner clocks'))
    profile_path = RUN8 / 'report-root/cases/G12D-r0-0ce5f8eaa8fde37f6566/result/write-diagnostics.json'
    profile_rows = csv_rows(HERE / 'owner-profile.csv')
    if profile_path.exists():
        original = json.loads(profile_path.read_text())['profile']['stages']
        for r in profile_rows:
            if r['field'] not in ('codec_sum_ns', 'locks_publication_sum_ns'):
                assert int(r['total_ns']) == original['write_profile_' + r['field']]
            elif r['field'] == 'codec_sum_ns':
                assert int(r['total_ns']) == sum(original['write_profile_' + field] for field in [
                    'connection_request_decode_ns', 'connection_append_decode_ns',
                    'connection_append_group_build_ns', 'connection_response_encode_ns'])
            else:
                assert int(r['total_ns']) == sum(original['write_profile_' + field] for field in [
                    'storage_persist_publication_ns', 'storage_persist_serialization_lock_wait_ns',
                    'storage_persist_state_lock_wait_ns'])
            expected_n = original['write_profile_connection_control_queue_samples'] if r['field'] == 'connection_control_queue_ns' else original['write_profile_sampled_requests']
            assert int(r['n']) == expected_n
            assert math.isclose(float(r['mean_us']), int(r['total_ns']) / expected_n / 1000, abs_tol=1e-12)
    for r in profile_rows:
        table_claim(5, r['field'], 1, r['mean_us'], profile_path, r['case_id'], r['field'], decimals=2)
    derived = {}
    profile_values = {r['field']: float(r['mean_us']) for r in profile_rows}
    derived['profile_persist_gap'] = (5, profile_values['connection_append_persist_ns'] -
                                     profile_values['storage_persist_total_ns'])
    native_cold = unique(operations, group='G12N', route='native', shape='read', size_bytes=65536,
                         concurrency=1, cache_state='cold')
    read_floor = unique(csv_rows(HERE / 'run8-ssd_floor.csv'), block_bytes=65536,
                        region_bytes=2147483648, state='fresh', repeat=0)
    derived['native_read_floor_gap'] = (9, float(native_cold['p50_us']) - float(read_floor['p50_us']))
    put_spread = csv_rows(HERE / 'run7-device_put_spread.csv')
    for name, size in [('run7_put1_gap', 1024), ('run7_put64_gap', 65536)]:
        sync = unique(put_spread, route='owner verbs', size_bytes=size, concurrency=1,
                      durability='synchronous', metric='api_p50_us')
        page_cache = unique(put_spread, route='owner verbs', size_bytes=size, concurrency=1,
                            durability='native', metric='api_p50_us')
        derived[name] = (15, float(sync['median']) - float(page_cache['median']))
    for name, (page, value) in derived.items():
        page = legacy_page(page)
        displayed = f'{value:.0f}'
        assert re.search(r'(?<![0-9.])' + displayed + r'\s*µs', pages[page - 1]), (name, page, displayed)
        checks.append(dict(claim=name + '; descriptive comparison, not a component', source_value=value,
                           pdf_value=displayed, page=page, pass_check=True,
                           source='owner-profile.csv' if name.startswith('profile') else
                           'run8-ops_summary.csv + run8-ssd_floor.csv' if name.startswith('native') else
                           'run7-device_put_spread.csv'))
    stall_spacing_checks = []
    stall_fits = csv_rows(HERE / 'run8-stall-fits.csv')
    for source in csv_rows(HERE / 'run8-stall.csv'):
        for policy in ['overwrite', 'new']:
            raw = json.loads(source[policy])
            extracted = unique(stall_fits, case_id=raw['case_id'])
            mean = statistics.fmean(raw['spacing_puts'])
            median = statistics.median(raw['spacing_puts'])
            assert math.isclose(mean, raw['mean_spacing_puts'], abs_tol=1e-10)
            assert median == raw['median_spacing_puts']
            assert float(extracted['spacing']) == raw['mean_spacing_puts']
            assert float(extracted['median_spacing']) == raw['median_spacing_puts']
            for column, statistic, value in [(2, 'mean spacing', mean), (3, 'median spacing', median)]:
                table_claim(13, source['mode'] + ', ' + policy, column, value,
                            HERE / 'run8-stall.csv', raw['case_id'], statistic, decimals=0)
            stall_spacing_checks.append(dict(case_id=raw['case_id'], mean_spacing_puts=mean,
                                             median_spacing_puts=median, status='raw arrays and PDF checked'))
    assert len(checks) >= 25

    unchanged = {}
    for name in ['tab-onekib-1.tex', 'tab-onekib-2.tex', 'tab-stores.tex', 'tab-hitratio.tex']:
        assert (HERE / name).read_bytes() == (OLD / name).read_bytes(), name
        unchanged[name] = 'byte-identical generated fragment'
    old_evidence = json.loads((OLD / 'evidence.json').read_text())
    evidence = json.loads((HERE / 'evidence.json').read_text())
    for name in ['scoreboard.pdf', 'others.pdf', 'coherence.pdf']:
        assert evidence['figures'][name]['points'] == old_evidence['figures'][name]['points']
        unchanged[name] = 'all source cases and plotted values identical'
    for name, figure in evidence['figures'].items():
        assert digest(HERE / 'figures' / name) == figure['sha256']
        for point in figure['points']:
            assert any(k in point for k in ['case_id', 'case_ids', 'a', 'tcp']), (name, point)
    structured = subprocess.check_output(['mutool', 'draw', '-F', 'stext', '-o', '-',
                                          str(HERE / 'main.pdf')], text=True, stderr=subprocess.DEVNULL)
    figure_font_sizes = {}
    layout_rows = []
    for page in ET.fromstring(structured).findall('page'):
        height = float(page.attrib['height'])
        body_top, body_bottom = 54.0, height - 54.0
        boxes = [list(map(float, block.attrib['bbox'].split())) for block in page.findall('block')]
        bottoms = [min(box[3], body_bottom) for box in boxes if box[1] < body_bottom and box[3] > body_top]
        last = max(bottoms, default=body_top)
        layout_rows.append(dict(page=int(page.attrib['id'].replace('page', '')),
                                unused_lower_body_fraction=(body_bottom-last)/(body_bottom-body_top)))
        sizes = [float(font.attrib['size']) for font in page.iter('font')
                 if 'DejaVu' in font.attrib['name']]
        if sizes:
            figure_font_sizes[page.attrib['id']] = min(sizes)
    rwc_evidence = json.loads((HERE / 'rwc/generated/figure-evidence.json').read_text()) if 'rwc' in manifest else {'figures': {}}
    rwc10_evidence = json.loads((HERE / manifest['rwc10']['manifest']).parent.joinpath('figure-evidence.json').read_text()) if 'rwc10' in manifest else {'figures': {}}
    assert len(evidence['figures']) == 14
    assert len(figure_font_sizes) == len(evidence['figures']) + len(rwc_evidence['figures']) + len(rwc10_evidence['figures'])
    assert min(figure_font_sizes.values()) >= 7, figure_font_sizes
    for figure in evidence['figures'].values():
        assert figure['text_size']['printed_min_pt'] >= 7
    source_main = (HERE / 'main.tex').read_text()
    assert not re.search(r'\\N\{D:op\d+', source_main), 'Prose must use semantic operation selectors'
    appendix = (HERE / 'appendix-tables.tex').read_text()
    row_ids = re.findall(r'^([0-9]{3})/', appendix, re.M)
    assert len(row_ids) == len(set(row_ids)) == len(operations) == 128
    assert set(row_ids) == {f'{i:03d}' for i in range(128)}

    curve_checks = check_curves(pages)
    size_spike_checks = check_sizes_spike(pages, definitions)
    contract_checks = check_contracts(text)
    curve_pages = [figure['page'] for figure in curve_checks['figures'].values()]
    renders = sorted({1, 2, 8, 9, 10, 11, 12, 13} | set(curve_pages)
                     | {i+1 for i, page in enumerate(pages) if any(term in page for term in
                        ('Topology, contracts and the Native result', 'Where Valkey is ahead',
                         'Acknowledgement contracts', 'page-cache acknowledgement,',
                         'Separate Class B controls', 'Order of magnitude only',
                         'Agreement with the Tidehunter paper', 'Unexplained features',
                         'points are reproducible', 'Where 64 KiB values occur',
                         'Local structure test', 'Fair Native comparison', 'Two-VM remote reads and writes',
                         'Run 9: cold qualification', 'Run 9: floors and provenance',
                         'Protocol allowance is not', 'Exact chart sizes versus', 'large-value stress point',
                         'unchanged scoreboard', 'Other stores, unchanged', 'RDMA against TCP',
                         'Provenance and limits', 'Durable scalar puts', 'case-level SSD-service qualification'))})
    (HERE / 'rendered').mkdir(exist_ok=True)
    current_renders = {f'page-{page:02d}.png' for page in renders}
    for stale in (HERE / 'rendered').glob('page-*.png'):
        if stale.name not in current_renders:
            stale.unlink()
    for page in renders:
        subprocess.run(['pdftoppm', '-f', str(page), '-l', str(page), '-r', '110', '-png',
                        '-singlefile', str(HERE / 'main.pdf'), str(HERE / f'rendered/page-{page:02d}')],
                       check=True, capture_output=True)
    prior = json.loads((HERE / 'validation.json').read_text()) if (HERE / 'validation.json').exists() else {}
    visual = prior.get('figures', {}).get('visual_inspection', {})
    if visual.get('pdf_sha256') != digest(HERE / 'main.pdf'):
        visual = {}  # A prior PDF's visual inspection is not evidence for this PDF.
    validation = {
        'phase': '10 November supervisor brief: final acceptance',
        'acknowledgement_contracts': contract_checks,
        'inputs': {'outputs_hashed': len(manifest['outputs']), 'sources': source_checks,
                   'copied_full_matrix': 'seven byte-identical CSV inputs; unavailable export not required',
                   'run8_operation_rows': len(operations), 'appendix_rows': len(row_ids)},
        'figures': {'count': len(evidence['figures']) + len(rwc_evidence['figures']) + len(rwc10_evidence['figures']),
                    'files': list(evidence['figures']) + list(rwc_evidence['figures']) + list(rwc10_evidence['figures']),
                    'point_case_ids': 'checked for every plotted point', 'carried_forward': unchanged,
                    'printed_font_minima_pt': figure_font_sizes,
                    'minimum_figure_font_pt': min(figure_font_sizes.values()),
                    'accessibility': 'Blue/neutral/vermilion scoreboard. Stall policies use blue circles/solid fit and vermilion triangles/dashed fit. Other plots use named rows, marker shapes, hatches or named multi-component legends; red/green is not the sole distinction.',
                    'layout': {'definition': 'Unused lower body below the last content text block, excluding 54 pt margins and page number; not white pixel area or space inside plots.',
                               'pages_over_40_percent_blank': [r for r in layout_rows if r['unused_lower_body_fraction'] > .4]},
                    'rendered_pages': renders, 'visual_inspection': visual},
        'numbers': {'resolved_keys': (HERE / 'numbers.tex').read_text().count('\\expandafter\\gdef'),
                    'independent_pdf_checks': len(checks),
                    'changed_keys': changed_keys,
                    'unchanged_existing_keys': len(baseline) - len(changed_keys),
                    'added_keys': sorted(set(definitions) - set(baseline)),
                    'hand_typed_outside_csv': [
                        {'values': [], 'source': 'Unsupported about-13-percent shorthand removed; explicit fleet/workload example now uses CSV number keys.'},
                        {'values': ['3 to 10 s', '0.5 to 2 s', 'about 15 KiB/put', '13 to 15 ops/s', '41 to 45 KiB'],
                         'source': 'brief specification device-counter correction; REPORT-unified.md and focused campaign correction'},
                        {'values': [], 'source': 'Removed mmap-only and fixed-fault-window claims; recent mapped and historical positional reads stated separately.'},
                        {'values': ['0.8 us'], 'source': 'unchanged 7 November hit-ratio model approximation'},
                        {'values': ['48', '512', '4096', 'two cases'], 'source': 'planned G13W configuration and requested 64 KiB profile extension, not results'},
                        {'values': 'Dates, value sizes, concurrency, sample counts, repetition counts, key counts, VM counts, RF1 and algorithm bit width',
                         'source': 'configuration labels and source-case metadata, not hand-entered latency estimates'}]},
        'cross_store_curves': curve_checks,
        'size_spike': size_spike_checks,
        'hand_checked_claims': checks,
        'stage_evidence_checks': {'raw_owner_fields': raw_checks, 'profile': 'totals and denominators independently checked against write-diagnostics.json when present',
                                  'stall_spacing': stall_spacing_checks,
                                  'nesting': 'control_queue includes appender and flusher waits; validation/descriptor nested in append_wal; no sum or TOTAL of annotations'},
        'code_checks': ['codec.rs PayloadHasher and record_digest hash full encoded payload with BLAKE2b-256',
                        'tcp/control.rs handler starts after appender dequeue; flusher wait added to control_queue',
                        'tcp/control.rs append_wal wraps try_append_group, including validation',
                        'tcp/diagnostics.rs queue nesting and separate append_decode/group_build timers',
                        'tcp/control.rs:1197 closes handler before completion dispatch; reactor.rs:2693 request decode and :3515 response encode are outside owner handler',
                        'wal/mod.rs recent fragments use mapped reads; older fragments use FileReader positional reads; no native read-path edit'],
        'latexmk': {'success': True, 'command': 'latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex',
                    'pages': len(pages), 'appendix_pages': len(pages) - page_for('Durable scalar puts') + 1, 'log_check': 'No Overfull, Underfull, undefined reference, Missing number, Missing character or LaTeX Warning lines.',
                    'section_order': list(section_headings),
                    'pdf_sha256': digest(HERE / 'main.pdf')},
        'source_defects': [
            {'file': str(RUN8 / 'REPORT-unified.md'), 'issue': 'Sync-step narrative compares owner p50 with native means. Brief compares means and shows owner tails separately.'},
            {'file': str(RUN8 / 'REPORT-unified.md'), 'issue': 'Native file-sync value is under owner persist in the narrative component table; component_summary.csv places it under durable wait. Brief follows the CSV.'},
            {'file': str(RUN8 / 'REPORT-unified.md'), 'issue': 'Native 1 KiB memory headline is explicitly a run-6 value, not a run-8 measurement; not imported.'},
            {'file': str(RUN8 / 'output/cold_resident.csv'), 'issue': 'Two owner c16 cold cases join c1 resident controls; concurrency is missing from the table. Brief uses exact selectors, not those pairs.'},
            {'file': str(RUN8 / 'output/cold_resident.csv'), 'issue': 'mTLS route is labelled tidehunter_verbs, unlike ops_summary.csv verbs mTLS.'},
            {'file': str(RUN8 / 'output/failed_cases.csv'), 'issue': 'Outer size_bytes=65536 does not identify failed direct helper block=1024; ssd_write_floor.csv supplies actual block size.'},
            {'file': str(RUN7.parent / 'analysis-final/APPLES-TO-APPLES.md'), 'issue': 'Subtracts run-6 memory times from run-7 SSD times across fleets; not carried forward.'},
            {'file': str(RUN7.parent / 'analysis-final/REPORT-run7.md'), 'issue': 'Some component context and findings subtract run-6 timings across fleets; not carried forward.'},
            {'file': str(RUN7 / 'output/sustained_summary.csv'), 'issue': 'Corrected complete buckets agree in range with the report headline; the earlier brief included a partial final bucket and was biased low.'},
            {'file': 'docs/distributed/FOCUSED_CAMPAIGN_2026-11-07.md', 'issue': 'Historical Deferred paragraph predates successful direct-kernel fleet runs; later launch history supersedes it.'}],
        'limits': ['No native read or native batch-write profiler',
                   'No batch reads in focused runs 7 and 8; full matrix has read-many32',
                   'No unprofiled TCP/hybrid batch controls or hybrid/mTLS deletes',
                   'Profiled copies have one repetition; no confidence interval or significance claim',
                   'Run-9 G14P adds 64 KiB verbs c16 only; c1 TCP/verbs decode/digest attribution remains open',
                   'Run-9 G13W completed; independent floors do not identify the old owner sync-step gap',
                   'Put stall phase, hybrid residual and constant non-API kernel reads remain unattributed',
                   'Device counters are window-wide; some read components are derived means with unproved population identity',
                   'No saturation, per-call CPU attribution, physical NAND or failover result; run 10 records process/cgroup CPU evidence; Sui capture adds sizes only, not replay performance'],
        'coherence_corrections': {'sustained_complete_bucket_checks': sustained_checks, 'network_semantics_checks': network_checks,
                                  'wording_contradictions': curve_checks['contradictions'],
                                  'unverified': ['Native 64 KiB root cause, queue depth, page-fault/allocation/journal attribution',
                                                 'Actual group sizes and fsync counts for Valkey and owner',
                                                 'Historical hardware/SSD-map parity, Nov03 Cassandra ring and some Nov03 execution-fleet identities',
                                                 'Valkey pair-level same-fleet transport provenance',
                                                 'Cassandra, TiKV and FoundationDB device persistence',
                                                 'Power-loss survival and Garnet stall cause',
                                                 'Paper-matched sustained throughput; paper client count']},
        'not_done': ['No infrastructure launched by document generation; selected second-attempt run-9 and run-10 results imported; aborted attempts excluded',
                     'No repository test suite run; only document build and requested acceptance checks',
                     'Original full-matrix extraction not rerun: 12 GB export unavailable; versioned inputs preserved']}
    (HERE / 'validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    print(f'PASS: {len(pages)} pages; {len(checks)} existing PDF number checks; '
          f'{curve_checks["independent_plotted_value_checks"]} curve values; '
          f'{curve_checks["number_keys_checked"]} curve number keys; '
          f'{len(raw_checks)} raw owner-stage checks; {len(evidence["figures"])} vector figures.')


if __name__ == '__main__':
    main()
