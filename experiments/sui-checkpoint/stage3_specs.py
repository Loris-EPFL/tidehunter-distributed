#!/usr/bin/env python3
"""Generate the store-neutral stage-3 RunConfig set (campaign plan section 9).

The sizes are the harness-admitted values closest to the study's 332 B p50
object and 8,885 B checkpoint contents: 256 B and 8,192 B. Arm A is the
distributed two-owner grid; arm B is the native single-node equal-load
reference. Every file is one `kv-research` RunConfig that another store runs by
changing only `backend`.

Value cache: every cell declares its coordinator value-cache entry count in its
`case_id`/`namespace` as the trailing `-cacheN` tag. The campaign runs the 70
grid cells and the native baseline with the cache off (`-cache0`), and adds one
read-only reference pair at C=16 with 1,000 entries (`-cache1000`). The engine
document itself cannot carry the count: kv-research builds both backends with
`KeySpaceConfig::default()`, whose `value_cache_size` is 0, and both option
structs deny unknown fields. The tag therefore states the intended setting
without pretending the engine applied it; `-cache1000` is a labelled reference
that can only be executed once kv-research exposes the value-cache option.

Usage: `stage3_specs.py --root DIR [--validate BINARY]`.
"""
import argparse
import json
from pathlib import Path
import re

KEYS = 65536
SEED = 1775
BATCH = 26
SIZES = (256, 8192)
TIERS = ('native', 'synchronous')
CALLERS = (2, 4, 8, 16)
MEASUREMENT_OPERATIONS = 200_000
WARMUP_FRACTION = 0.10
GRID_CACHE_ENTRIES = 0
REFERENCE_CACHE_ENTRIES = 1_000
MIXES = {
    'read': dict(read=100, put=0, delete=0, missing_read=0, get_many=0, put_batch=0),
    'mixed': dict(read=70, put=30, delete=0, missing_read=0, get_many=0, put_batch=0),
    'write': dict(read=0, put=100, delete=0, missing_read=0, get_many=0, put_batch=0),
    'get_many': dict(read=0, put=0, delete=0, missing_read=0, get_many=100, put_batch=0),
    'put_batch': dict(read=0, put=0, delete=0, missing_read=0, get_many=0, put_batch=100),
}


def declared_cache(case_id):
    """The value-cache entry count a case id states, e.g. `-cache0`."""
    match = re.search(r'-cache([0-9]+)$', case_id)
    if match is None:
        raise ValueError(f'case id does not declare a value cache: {case_id}')
    return int(match.group(1))


def _cells(arm, family, mix, size, distribution, tiers, callers, *, batch=None,
           cache=GRID_CACHE_ENTRIES, reference=False):
    suffix = '' if family in ('A1', 'A2', 'A3', 'A7', 'A8', 'B1', 'B2') else '-' + mix
    for tier in tiers:
        for concurrency in callers:
            case = f'S3-{family}{suffix}-c{concurrency}-{tier}-cache{cache}'
            yield dict(case_id=case, arm=arm, mix=mix, size=size,
                       distribution=distribution, tier=tier,
                       concurrency=concurrency, batch=batch, cache=cache,
                       reference=reference)


def cells():
    """The 72 campaign cells: the 70-cell grid at cache 0 plus a cache-on pair."""
    rows = []
    for family, mix in (('A1', 'read'), ('A2', 'mixed'), ('A3', 'write')):
        rows += list(_cells('A', family, mix, 8192, 'uniform', TIERS, CALLERS))
    for family, mix in (('A4', 'read'), ('A4', 'mixed'), ('A4', 'write')):
        rows += list(_cells('A', family, mix, 256, 'uniform', TIERS, (16,)))
    for family, mix in (('A5', 'read'), ('A5', 'mixed'), ('A5', 'write')):
        rows += list(_cells('A', family, mix, 8192, 'zipf', TIERS, (16,)))
    for family, mix in (('A6', 'read'), ('A6', 'mixed'), ('A6', 'write')):
        rows += list(_cells('A', family, mix, 256, 'zipf', TIERS, (16,)))
    for concurrency in (4, 16):
        rows += list(_cells('A', 'A7', 'get_many', 8192, 'uniform', TIERS,
                            (concurrency,), batch=BATCH))
        rows += list(_cells('A', 'A8', 'put_batch', 8192, 'uniform', TIERS,
                            (concurrency,), batch=BATCH))
    for family, mix in (('A9', 'get_many'), ('A9', 'put_batch')):
        rows += list(_cells('A', family, mix, 8192, 'zipf', TIERS, (16,), batch=BATCH))
    for family, mix in (('B1', 'read'), ('B2', 'write')):
        for tier in TIERS:
            for callers in (1, 2, 4, 8):
                rows.append(dict(case_id=f'S3-{family}-c{callers}-{tier}-cache0',
                                 arm='B', mix=mix, size=8192, distribution='uniform',
                                 tier=tier, concurrency=callers, batch=None,
                                 cache=GRID_CACHE_ENTRIES, reference=False))
    rows += list(_cells('A', 'A1', 'read', 8192, 'uniform', TIERS, (16,),
                        cache=REFERENCE_CACHE_ENTRIES, reference=True))
    return rows


def backend(case, root):
    directory = Path(root) / 'stage3' / 'db' / case['case_id']
    if case['arm'] == 'B':
        return dict(kind='native', options=dict(
            path=str(directory / 'native'),
            durability='value_wal_barrier' if case['tier'] == 'synchronous'
            else 'page_cache'))
    return dict(kind='tidehunter', options=dict(
        transport=dict(kind='tcp-trusted'),
        path=str(directory / 'coordinator'),
        evidence=str(Path(root) / 'stage3' / 'evidence' / case['case_id']),
        owners=2, key_count=KEYS, durability_tier=case['tier'],
        local_geometry=dict(fragment_bytes=4 << 20, wal_file_bytes=64 << 20),
        index=dict(frag_size=8 << 20, wal_file_size=32 << 20)))


def run_config(case, root, measurement_operations=MEASUREMENT_OPERATIONS):
    distribution = ('uniform' if case['distribution'] == 'uniform'
                    else dict(zipf=dict(exponent=1.0)))
    workload = dict(kind='synthetic', distribution=distribution,
                    mix=MIXES[case['mix']], batch_size=case['batch'] or BATCH,
                    put_key_policy='overwrite')
    return dict(
        schema_version=1, case_id=case['case_id'], backend=backend(case, root),
        seed=SEED, namespace=case['case_id'],
        dataset=dict(keys=KEYS, value_size=case['size'], entropy=1.0),
        workload=workload,
        execution=dict(mode='closed_loop', concurrency=case['concurrency'],
                       queue_capacity=64, request_timeout_ms=30000),
        phases=dict(preload=True,
                    warmup_operations=int(measurement_operations * WARMUP_FRACTION),
                    aging_operations=0,
                    measurement_operations=measurement_operations),
        diagnostics=dict(write_profile_limit=0, read_profile_limit=0),
        qualification_only=False, source_revision=None,
        limits=dict(max_keys=KEYS, max_bytes=16 << 30, max_operations=3_000_000,
                    deadline_seconds=600, shutdown_grace_seconds=30))


def write(root, measurement_operations=MEASUREMENT_OPERATIONS):
    root = Path(root)
    output = root / 'stage3' / 'spec'
    output.mkdir(parents=True, exist_ok=True)
    written = []
    for case in cells():
        path = output / f'{case["case_id"]}.json'
        path.write_text(json.dumps(run_config(case, root, measurement_operations),
                                   indent=2, sort_keys=True) + '\n')
        written.append(path)
    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True,
                        help='campaign directory that owns stage3/{db,evidence,spec}')
    parser.add_argument('--measurement-operations', type=int, default=MEASUREMENT_OPERATIONS)
    parser.add_argument('--validate', type=Path,
                        help='kv-research binary; validates every generated RunConfig')
    args = parser.parse_args()
    paths = write(args.root, args.measurement_operations)
    if args.validate:
        import subprocess
        for path in paths:
            subprocess.run([str(args.validate), '--config', str(path),
                            '--validate-config', '--output', str(args.root / 'stage3' / 'admission')],
                           check=True, stdout=subprocess.DEVNULL, timeout=60)
    print(json.dumps(dict(cells=len(paths), sizes=list(SIZES))))


if __name__ == '__main__':
    main()
