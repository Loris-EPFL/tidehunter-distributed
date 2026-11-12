"""Presentation contracts for the reviewed fabric_ab_compact CSVs; no engine code."""
import csv
import hashlib
import math
import json
from pathlib import Path

FAMILIES = ('ab_warm', 'ab_pairs', 'cold_fabric', 'cold_pairs', 'parity_matrix')
WARM = ('tcp-mtls-adaptive', 'tcp-trusted-adaptive', 'tcp-trusted-busy', 'verbs-adaptive', 'verbs-busy')
COLD = ('owner-tcp-trusted', 'owner-verbs', 'nvmeof-rdma-kernel', 'nvmeof-rdma-spdk', 'nvmeof-tcp-kernel')
CELLS = ((1024, 1), (1024, 16), (65536, 1), (65536, 16))
COMPARISONS = ('TLS cost', 'transport adaptive', 'transport busy', 'parking TCP', 'parking verbs')
PAIR_ARMS = dict(zip(COMPARISONS, ((WARM[0], WARM[1]), (WARM[1], WARM[3]), (WARM[2], WARM[4]), (WARM[1], WARM[2]), (WARM[3], WARM[4]))))
CODES = dict(zip(WARM + COLD, ('TM-A', 'TT-A', 'TT-B', 'V-A', 'V-B', 'O-T', 'O-V', 'K-R', 'S-R', 'K-T')))
LABELS = dict(zip(WARM + COLD, ('TCP mTLS\nadaptive', 'TCP trusted\nadaptive', 'TCP trusted\nBusy spins', 'verbs trusted\nadaptive', 'verbs trusted\nBusy spins', 'Owner TCP trusted\nadaptive', 'Owner verbs trusted\nadaptive', 'Kernel NVMe/RDMA\ndirect; trusted control', 'SPDK NVMe/RDMA\nfile AIO; trusted control', 'Kernel NVMe/TCP\ndirect; trusted control')))
PAIR_LABELS = ('TLS: TM-A / TT-A', 'Transport A: TT-A / V-A', 'Transport Busy: TT-B / V-B', 'Parking TCP: TT-A / TT-B', 'Parking verbs: V-A / V-B')
COLORS = ('#0072B2', '#56B4E9', '#E69F00', '#D55E00', '#009E73')
FLOOR_LABELS = ('TCP blocking', 'TCP user spin', 'TCP SO_BUSY_POLL', 'RDMA SEND polled', 'RDMA SEND event', 'RDMA READ polled', 'verbs;ofi_rxm')
CONTRACT = ('Coordinator cache off; owner RPC: mTLS only TM-A, otherwise trusted/no TLS.\n'
            'Busy = user-space spinning on owner AND coordinator read driver, not kernel SO_BUSY_POLL.\n'
            'Lanes: 1 at c1, 4 at c16; r0/r1 separate; no pooled samples or confidence intervals.')
TEX_CONTRACT = ('Coordinator cache off; lanes one at c1, four at c16; r0/r1 separate. '
                'Busy spins owner and coordinator read driver, not kernel busy poll. '
                'Unmatched dimensions: TLS/security, polling, RPC/direct I/O; NVMe control is verbs trusted Adaptive.')
DEFAULT_FLOORS = Path('/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-rwcompact2/analysis2/floors.csv')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tex(value):
    return ''.join({'&': r'\&', '%': r'\%', '_': r'\_', '#': r'\#', '$': r'\$', '{': r'\{', '}': r'\}', '\\': r'\textbackslash{}'}.get(c, c) for c in str(value))


def load(output):
    data = {}
    for name in FAMILIES:
        with (Path(output) / (name + '.csv')).open(newline='') as stream:
            data[name] = list(csv.DictReader(stream))
    return data


def one(rows, arm, size, readers, rep, field='arm'):
    found = [(i, r) for i, r in enumerate(rows) if r[field] == arm and
             (r['size_bytes'], r['readers'], r['repetition']) == (str(size), str(readers), str(rep))]
    if len(found) != 1:
        raise ValueError(('missing or duplicate row', arm, size, readers, rep, len(found)))
    return found[0]


def number_value(row, field):
    value = row.get(field)
    if value in ('', None, 'not recorded'):
        return None
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('nonfinite CSV metric')
    return value


def selected_floor(row):
    if row['group'] != 'G11N' or row['size_bytes'] != '1024':
        return None
    contract = json.loads(row['floor_contract']); spec = contract['spec']
    tool, mode = spec['tool'], spec['mode']
    label = None
    if tool == 'tcp_rtt' and spec['interface'] == 'eth0':
        label = {'block': FLOOR_LABELS[0], 'spin': FLOOR_LABELS[1], 'busy_poll': FLOOR_LABELS[2]}.get(mode)
    elif tool == 'ib_send_lat':
        label = {'polled': FLOOR_LABELS[3], 'event': FLOOR_LABELS[4]}.get(mode)
    elif tool == 'ib_read_lat' and mode == 'polled':
        label = FLOOR_LABELS[5]
    elif tool == 'fi_pingpong' and spec['provider'] == 'verbs;ofi_rxm' and spec['endpoint'] == 'rdm':
        label = FLOOR_LABELS[6]
    if label is None:
        return None
    semantics = contract.get('latency_semantics')
    factor = 2 if semantics == 'half_round_trip' else 1 if semantics == 'round_trip' else None
    if row['status'] == 'completed' and factor is None:
        raise ValueError('unknown floor round-trip semantics')
    return label, factor
