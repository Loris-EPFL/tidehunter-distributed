#!/usr/bin/env python3
"""Check contract tables against original CSVs and the final PDF; no builder imports."""
import csv
import hashlib
import json
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_contracts(pdf_text):
    manifest = json.loads((HERE / 'contract-inputs.json').read_text())
    for filename, wanted in manifest['sources'].items():
        assert digest(Path(filename)) == wanted, filename
    for filename, wanted in manifest['outputs'].items():
        assert digest(HERE / filename) == wanted, filename
    with (HERE / 'contract-values.csv').open(newline='') as stream:
        values = list(csv.DictReader(stream))
    assert len({r['label'] for r in values}) == len(values)
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
                     'tab-contract-indicative.tex'):
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
                sources=len(manifest['sources']), claims=checked,
                scope='same-fleet Class A pairs; separate B controls; one explicitly non-ranking historical scale table')


if __name__ == '__main__':
    text = subprocess.check_output(['pdftotext', '-layout', str(HERE / 'main.pdf'), '-'], text=True)
    result = check_contracts(text)
    print('PASS:', result['source_linked_values'], 'source-linked values;', result['numeric_occurrences'], 'PDF claims')
