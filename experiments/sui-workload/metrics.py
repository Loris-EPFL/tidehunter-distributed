"""Small shared statistical helpers."""
import numpy as np

def percentiles(values):
    a = np.asarray(values, dtype=float)
    if not len(a):
        return ['', '', '', '', '', '', '']
    return [len(a), *np.percentile(a, [50, 90, 99, 99.9]).tolist(), a.max(), a.mean()]

def complete_component_buckets(checkpoints, seconds, components):
    """Return full UTC-aligned buckets, including empty buckets."""
    if not checkpoints:
        return []
    width = seconds * 1000
    first = int(checkpoints[0]['timestamp_ms'])
    last = int(checkpoints[-1]['timestamp_ms'])
    first_full = (first + width - 1) // width
    end_full = last // width
    buckets = {}
    for checkpoint in checkpoints:
        bucket = int(checkpoint['timestamp_ms']) // width
        if first_full <= bucket < end_full:
            values = buckets.setdefault(bucket, [0] * len(components))
            for i, name in enumerate(components):
                values[i] += int(checkpoint[name])
    result = []
    for bucket in range(first_full, end_full):
        values = [v / seconds for v in buckets.get(bucket, [0] * len(components))]
        result.append((bucket * width, values))
    return result
