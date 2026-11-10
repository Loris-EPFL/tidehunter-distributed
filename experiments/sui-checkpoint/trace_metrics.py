"""Node-owned histogram evidence and temporal checkpoint-start write windows."""
from collections import Counter, defaultdict
from datetime import datetime
import json
import math
import re

from analyze import metrics
from checkpoint_support import EXECUTION


def checkpoint_windows(log, writes, anchor, cutoff):
    """Label time windows by the latest start log, not causal checkpoint ownership."""
    boundaries = []
    clean = re.sub(r'\x1b\[[0-9;]*m', '', log)
    for line in clean.splitlines():
        match = EXECUTION.fullmatch(line)
        if match:
            moment = datetime.fromisoformat(match[1])
            # Integer conversion avoids floating-point rounding of nanoseconds.
            seconds = int(moment.timestamp())
            ns = seconds * 1_000_000_000 + moment.microsecond * 1000 - anchor
            boundaries.append((ns, int(match[2])))
    if not boundaries:
        raise ValueError('checkpoint execution timestamps absent')
    boundaries.sort()
    intervals = []
    ordered = sorted(writes, key=lambda row: row['ns'])
    position, previous, previous_label = 0, 0, -1
    for end, label in [*boundaries, (cutoff, -2)]:
        volume, count = Counter(), Counter()
        while position < len(ordered) and ordered[position]['ns'] < end:
            item = ordered[position]
            volume[item['db']] += item['bytes']
            count[item['db']] += 1
            position += 1
        if end > previous:
            intervals.append({'checkpoint_start_label': previous_label,
                'start_ns': previous, 'end_ns': end, 'seconds': (end - previous) / 1e9,
                'write_bytes': sum(volume.values()), 'write_operations': sum(count.values()),
                'bytes_per_second': sum(volume.values()) * 1e9 / (end - previous),
                'bytes_by_db': {str(db): size for db, size in volume.items()}})
        previous, previous_label = end, label
    return intervals


def histogram_quantile(buckets, fraction):
    """Prometheus linear bucket interpolation, not an exact sample quantile."""
    buckets = sorted(buckets)
    total = buckets[-1][1] if buckets else 0
    if not total:
        return None
    target, lower, previous = total * fraction, 0, 0
    for upper, count in buckets:
        if count >= target:
            if math.isinf(upper):
                return lower
            return lower + (upper - lower) * (target - previous) / (count - previous) if count > previous else upper
        lower, previous = upper, count
    return None


def storage_histograms(path):
    groups = defaultdict(list)
    for metric, count in metrics(path).items():
        if not (metric.startswith('db_op_bucket{') or
                (metric.startswith('rocksdb_') and '_latency_seconds_bucket{' in metric)):
            continue
        match = re.search(r'le="([^"]+)"', metric)
        if not match:
            continue
        identity = re.sub(r',?le="[^"]+",?', ',', metric).replace('{,', '{').replace(',}', '}')
        groups[identity].append((float(match[1]), count))
    return {name: {'n': sorted(buckets)[-1][1], 'unit': 'microseconds' if name.startswith('db_op_') else 'seconds',
                   'p50_bucket_interpolated': histogram_quantile(buckets, .5),
                   'p99_bucket_interpolated': histogram_quantile(buckets, .99)}
            for name, buckets in groups.items() if sorted(buckets)[-1][1]}


def replay_performance(root):
    result = json.loads((root / 'experiment/results.json').read_text())
    return {mode: {'wall_seconds_including_startup': result[mode]['elapsed_seconds_including_startup'],
                   'completed_checkpoints': result[mode]['checkpoint_limit'] + 1,
                   'checkpoints_per_second_including_startup': (result[mode]['checkpoint_limit'] + 1) / result[mode]['elapsed_seconds_including_startup'],
                   'storage_latency_histograms': storage_histograms(root / 'experiment' / f'{mode}-foreground.prom')}
            for mode in ('native', 'remote')}
