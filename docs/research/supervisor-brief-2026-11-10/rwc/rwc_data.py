"""Small presentation-only helpers for the remote_rw.py output contract."""
import csv
import json
from pathlib import Path

FAMILIES = ('latency_vs_size', 'latency_vs_concurrency', 'throughput_vs_concurrency',
            'cold_vs_warm', 'floors', 'parity_matrix')
UNMATCHED = ('Unmatched dimensions: sync/grouping, RPC/security, index/routing, and host SSD '
             '(Native coordinator SSD versus owner SSD). Read lanes: Native 0, owner 1 at c1/c5 and 4 at c16; '
             'c5 owner reads are lane-limited, not a transport ranking. Floors are separate populations.')
ARMS = ('native page_cache', 'native value_wal_barrier', 'native value_wal_group_barrier', 'tcp', 'verbs')
LABELS = {'native page_cache': 'Native page cache', 'native value_wal_barrier': 'Native per-put barrier',
          'native value_wal_group_barrier': 'Native grouped barrier', 'tcp': 'Owner TCP', 'verbs': 'Owner verbs',
          'native': 'Native'}


def load(path):
    result = {}
    for family in FAMILIES:
        with (Path(path) / (family + '.csv')).open(newline='') as f:
            result[family] = list(csv.DictReader(f))
    return result


def arm(row):
    return row['arm'] if row['transport'] == 'in-process' else row['transport']


def concurrency(row):
    return int(row['readers'] or row['writers'])


def key(row):
    return row['group'], row['size_bytes'], row['readers'] or row['writers'], row['repetition']


def pairs(rows):
    """Structural pairs after remote_rw.py's job-level parity gate; no CSV-level parity claim."""
    out = []
    for owner in rows:
        if owner['transport'] not in ('tcp', 'verbs'):
            continue
        native_group = 'G14N' if owner['group'] == 'G14O1' else owner['group']
        peers = [r for r in rows if r['transport'] == 'in-process' and
                 (native_group, *key(owner)[1:]) == key(r)]
        expected = ('native page_cache', 'native value_wal_barrier', 'native value_wal_group_barrier') if native_group == 'G14N' else ('native',)
        for label in expected:
            found = [r for r in peers if r['arm'] == label]
            if len(found) > 1:
                raise ValueError('duplicate Native partner')
            native = found[0] if found else None
            status = ('missing partner' if native is None else
                      'failed' if 'failed' in (owner['status'], native['status']) else
                      'unqualified cold' if 'unqualified' in (owner['status'], native['status']) else
                      'matched' if owner['status'] == native['status'] == 'completed' else 'not available')
            out.append(dict(owner=owner, native=native, native_arm=label, status=status))
    return out


def floor_spec(row):
    contract = json.loads(row['floor_contract'])
    return contract.get('spec', contract)


def floor_factor(row):
    if row['group'] != 'G11N' or row['status'] != 'completed':
        return 1
    semantics = json.loads(row['floor_contract']).get('latency_semantics')
    if semantics == 'half_round_trip': return 2
    if semantics == 'round_trip': return 1
    raise ValueError(f'unknown network timing semantics: {semantics!r}')


def selected_floor(row):
    """Declared controls, not minima: retain repetitions, no picking fast points."""
    spec = floor_spec(row)
    if row['group'] == 'G13W':
        return spec['mode'] in ('append_pwrite', 'append_mmap_fsync')
    if row['group'] in ('G11S', 'G15F'):
        return spec['region_bytes'] == 2147483648 and spec['state'] == 'fresh'
    return ((spec['tool'] == 'tcp_rtt' and spec['interface'] == 'eth0' and spec['mode'] == 'block') or
            (spec['tool'] == 'ib_send_lat' and spec['mode'] == 'polled'))


def availability_counts(rows, group, repetition):
    pair_rows = [p for p in pairs(rows) if p['owner']['group'] == group and p['owner']['repetition'] == repetition]
    groups = ('G14N', 'G14O1') if group == 'G14O1' else (group,)
    cases = [r for r in rows if r['group'] in groups and r['repetition'] == repetition]
    return [sum(p['status'] == 'matched' for p in pair_rows),
            sum(p['status'] == 'missing partner' for p in pair_rows),
            sum(r['status'] == 'failed' for r in cases),
            sum(r['status'] == 'unqualified' for r in cases),
            sum(r['status'] not in ('completed', 'failed', 'unqualified') for r in cases)]
