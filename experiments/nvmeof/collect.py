#!/usr/bin/env python3
"""Collect independent target block and CPU intervals; never infer per-request latency."""
import argparse
import json
import os
from pathlib import Path
import re
import time

BLOCK_FIELDS = ('reads_completed', 'reads_merged', 'read_sectors', 'read_ms',
                'writes_completed', 'writes_merged', 'write_sectors', 'write_ms',
                'in_flight', 'io_ms', 'weighted_io_ms', 'discards_completed',
                'discards_merged', 'discard_sectors', 'discard_ms', 'flushes_completed', 'flush_ms')
CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal', 'guest', 'guest_nice')


def block_snapshot(name, root=Path('/sys/class/block')):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or name in ('.', '..'):
        raise ValueError('invalid sysfs block device name')
    start = time.monotonic_ns()
    values = [int(x) for x in (root / name / 'stat').read_text().split()]
    end = time.monotonic_ns()
    if len(values) < 11 or any(x < 0 for x in values):
        raise ValueError('invalid block stat')
    return {'start_ns': start, 'end_ns': end, 'values': dict(zip(BLOCK_FIELDS, values))}


def cpu_snapshot(path=Path('/proc/stat')):
    start = time.monotonic_ns()
    line = path.read_text().splitlines()[0].split()
    end = time.monotonic_ns()
    if not line or line[0] != 'cpu':
        raise ValueError('invalid aggregate CPU stat')
    return {'start_ns': start, 'end_ns': end, 'values': dict(zip(CPU_FIELDS, map(int, line[1:])))}


def delta(first, last):
    result = {}
    for key, value in first['values'].items():
        if key == 'in_flight' or key not in last['values']:
            continue
        end = last['values'][key]
        if end < value:
            raise ValueError('counter reset: ' + key)
        result[key] = end - value
    return {'start_monotonic_ns': first['start_ns'], 'end_monotonic_ns': last['end_ns'],
            'first_read_end_ns': first['end_ns'], 'last_read_start_ns': last['start_ns'], 'delta': result}


def collect(device, seconds):
    if not 0 < seconds <= 3600:
        raise ValueError('interval must be in (0, 3600] seconds')
    # These are deliberately separate clocks and non-atomic snapshots.
    b0 = block_snapshot(device); c0 = cpu_snapshot()
    time.sleep(seconds)
    b1 = block_snapshot(device); c1 = cpu_snapshot()
    block = delta(b0, b1)
    if 'read_sectors' in block['delta']:
        block['delta']['read_bytes'] = block['delta']['read_sectors'] * 512
    if 'write_sectors' in block['delta']:
        block['delta']['write_bytes'] = block['delta']['write_sectors'] * 512
    return {'version': 1, 'block_device': device, 'block': block, 'cpu': delta(c0, c1),
            'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
            'qualification': 'sysfs device-level aggregate; 512-byte accounting sectors; not per-request NAND latency or network time',
            'target_cache': {'warm_hits': None, 'storage_misses': None,
                             'denominator': None,
                             'reason': 'device-wide counters cannot attribute page-cache hits or misses to exports'},
            'trace': {'available': False, 'reason': 'per-request nvmet/file/block trace correlation is not collected by this interval sampler'}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--block-device', required=True, help='explicit /sys/class/block name on target')
    parser.add_argument('--seconds', type=float, required=True)
    parser.add_argument('--output', required=True, help='new JSON output path')
    args = parser.parse_args()
    result = collect(args.block_device, args.seconds)
    with open(args.output, 'x') as f:
        json.dump(result, f, sort_keys=True, indent=2); f.write('\n')


if __name__ == '__main__':
    main()
