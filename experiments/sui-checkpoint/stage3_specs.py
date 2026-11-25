#!/usr/bin/env python3
"""Generate the stage-3 RunConfig set (campaign plan sections 0j and 9).

The sizes are the harness-admitted values closest to the study's 332 B p50
object and 8,885 B checkpoint contents: 256 B and 8,192 B. Arm A is the
distributed two-owner grid; arm B is the native single-node equal-load
reference. Every file is one `kv-research` RunConfig that another store runs by
changing only `backend`.

Cluster mode (default): every distributed cell carries the external deployment
that `prepare_external.py` produced for its acknowledgement tier
(`external_remote_config` with live NVMe-oF, plus one owner durability receipt
per owner). The control transport is the preparation's mTLS TCP; the data path
is RDMA NVMe-oF. Every distributed cell also declares the coordinator read cache
off, so the cell measures owner reads. One preparation per tier is reused by
every cell of that tier: the owner launch files, including the durability tier
and the NVMe initiator identities, are fixed in the preparation, and kv-research
verifies each owner receipt against the cell's tier.

Rehearsal mode (`--rehearsal`): every cell runs the Linux `test-utils` trusted
TCP ordinary-file fixture in one process on one host. The driver labels the
result `rehearsal`; it is not a campaign measurement.

Value cache: every cell declares its coordinator value-cache entry count in its
`case_id`/`namespace` as the trailing `-cacheN` tag and passes the same count as
`backend.options.value_cache_size`. kv-research applies it to the keyspace
configuration of both the distributed and the native backend; zero disables the
cache. The campaign runs the 70 grid cells and the native baseline with the
cache off (`-cache0`), and adds one read-only reference pair at C=16 with 1,000
entries (`-cache1000`).

Measurement sizing (plan section 9.4): `phases.measurement_operations` is one
value per operation family (`read`, `mixed`, `write`, `get_many`, `put_batch`),
frozen from the three phase-B calibration cells. Without a frozen calibration
every family uses `MEASUREMENT_OPERATIONS`, the pre-calibration default. The
counts enter every spec, so a spec written from a different calibration is
refused instead of silently replaced.

The phase-A probe (`probe_case`) is a separate arm-B synchronous write cell. It
is not one of the 72 campaign cells; it is run once before the grid so the
watch sees the real per-write barrier rate on the store host.

Usage: `stage3_specs.py --root DIR [--rehearsal | --external TIER=DIR ...]
[--native-root DIR] [--calibration FILE] [--validate BINARY]`.
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
OWNERS = 2
MEASUREMENT_OPERATIONS = 200_000
WARMUP_FRACTION = 0.10
GRID_CACHE_ENTRIES = 0
REFERENCE_CACHE_ENTRIES = 1_000
CONTROL_TRANSPORT = {'kind': 'tcp'}
LIVE_TRANSPORT = 'rdma'
FAMILIES = ('read', 'mixed', 'write', 'get_many', 'put_batch')
# A grouped call counts once per call, so it inherits the rate of its class.
CALIBRATED_BY = {'get_many': 'read', 'put_batch': 'write'}
FLOOR = 20_000
GROUPED_FLOOR = 5_000
CEILING = 3_000_000
WINDOW_SECONDS = 40
CALIBRATION_OPERATIONS = 20_000
PROBE_CASE = 'S3-P1-armB-c8-synchronous-probe'
PROBE_OPERATIONS = 5_000
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


def operation_counts(default=MEASUREMENT_OPERATIONS, overrides=None):
    """One measurement operation count per family; `overrides` must be complete."""
    counts = {family: default for family in FAMILIES}
    for family, count in (overrides or {}).items():
        if family not in FAMILIES:
            raise ValueError(f'unknown operation family: {family}')
        # Rehearsal runs may use small windows; the campaign floors live in
        # `calibration_counts` and in `read_calibration`.
        if type(count) is not int or not 1 <= count <= CEILING:
            raise ValueError(f'{family} measurement operations out of range: {count}')
        counts[family] = count
    return counts


def count_for(case, counts):
    return counts[case['mix']]


def calibration_counts(rates):
    """Plan section 9.4: 40 s of calls per family, bounded, from the measured rates."""
    frozen = {}
    for family in FAMILIES:
        source = CALIBRATED_BY.get(family, family)
        rate = rates.get(source)
        if not isinstance(rate, (int, float)) or not rate > 0:
            raise ValueError(f'{family} has no measured rate from its calibration cell')
        count = round(WINDOW_SECONDS * rate)
        frozen[family] = min(CEILING, max(FLOOR, count))
    return frozen


def read_calibration(path):
    """The frozen family counts of one phase-B calibration file."""
    document = json.loads(Path(path).read_text())
    counts = document.get('counts')
    if not isinstance(counts, dict) or set(counts) != set(FAMILIES):
        raise ValueError(f'calibration file must name every family: {path}')
    for family, count in counts.items():
        if type(count) is not int or not GROUPED_FLOOR <= count <= CEILING:
            raise ValueError(f'{family} measurement operations out of range: {count}')
    return counts


def receipt_path(root, tier, partition):
    """Where the driver copies the owner-published durability receipt."""
    return (Path(root) / 'stage3' / 'owner-durability' / tier /
            f'node-{partition}.durability.json')


def load_external(directory, *, tier, database=None):
    """One `prepare_external.py` output, checked against the stage-3 shape."""
    directory = Path(directory)
    manifest_path = directory / 'external-manifest.json'
    config_path = directory / 'config.json'
    if not manifest_path.is_file() or not config_path.is_file():
        raise ValueError(
            f'external preparation lacks config.json/external-manifest.json: {directory}')
    manifest = json.loads(manifest_path.read_text())
    config = json.loads(config_path.read_text())
    if manifest.get('schema') != 1 or manifest.get('protocol') != 9:
        raise ValueError(f'external preparation protocol differs: {directory}')
    if manifest.get('owners') != OWNERS:
        raise ValueError(f'external preparation needs exactly {OWNERS} owners: {directory}')
    if manifest.get('transport') != CONTROL_TRANSPORT:
        raise ValueError(
            f'external preparation control transport must be mTLS TCP: {directory}')
    if manifest.get('live_transport') != LIVE_TRANSPORT:
        raise ValueError(f'external preparation must use the {LIVE_TRANSPORT} data path')
    if manifest.get('durability_tier') != tier:
        raise ValueError(f'external preparation tier {manifest.get("durability_tier")!r} '
                         f'is not {tier!r}: {directory}')
    databases = config.get('databases') or {}
    name = database if database is not None else next(iter(databases), None)
    if name not in databases:
        raise ValueError(f'external preparation has no database {name!r}; '
                         f'it has {sorted(databases)}')
    document = databases[name]
    partitions = sorted(document.get('storage', {}).get('partitions') or [])
    endpoints = sorted((endpoint.get('partition'), endpoint.get('address') or '')
                       for endpoint in document.get('endpoints') or [])
    if (partitions != list(range(OWNERS))
            or [partition for partition, _ in endpoints] != list(range(OWNERS))
            or any(not address for _, address in endpoints)
            or len({address for _, address in endpoints}) != OWNERS):
        raise ValueError('external database must name one distinct endpoint per owner partition')
    if document.get('tls', {}).get('transport') != CONTROL_TRANSPORT:
        raise ValueError('external database must use the mTLS TCP control transport')
    if document.get('live_nvme', {}).get('leased_durability') != tier:
        raise ValueError('external database durability tier differs from its preparation')
    if document.get('read_cache_bytes') != 0:
        raise ValueError('external database must be prepared with the coordinator read cache off')
    hosts = manifest.get('hosts') or []
    if [record.get('partition') for record in hosts] != list(range(OWNERS)):
        raise ValueError('external manifest must list one record per owner partition')
    for record in hosts:
        if (len(record.get('commands') or []) != len(databases)
                or len(record.get('ready_files') or []) != len(databases)):
            raise ValueError('external manifest commands/ready files do not cover every database')
    return dict(tier=tier, directory=directory, manifest=manifest, config=config,
                database=name, index=list(databases).index(name), document=document)


def external_map(items, *, database=None):
    """Load one preparation per acknowledgement tier: `{tier: preparation}`."""
    preparations = {}
    for tier, directory in items:
        if tier not in TIERS:
            raise ValueError(f'unknown acknowledgement tier: {tier}')
        if tier in preparations:
            raise ValueError(f'duplicate external preparation for tier {tier}')
        preparations[tier] = load_external(directory, tier=tier, database=database)
    missing = [tier for tier in TIERS if tier not in preparations]
    if missing:
        raise ValueError('one external preparation per tier is required; missing '
                         + ', '.join(missing))
    return preparations


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


def probe_case():
    """The phase-A arm-B synchronous write probe: one full preload, 5,000 calls."""
    return dict(case_id=PROBE_CASE, arm='B', mix='write', size=8192,
                distribution='uniform', tier='synchronous', concurrency=8,
                batch=None, cache=GRID_CACHE_ENTRIES, reference=False, probe=True)


def store_root(case, root, native_root=None):
    """The absolute root of the host that runs the cell's store."""
    if native_root is not None and case['arm'] == 'B':
        return Path(native_root)
    return Path(root)


def backend(case, root, *, native_root=None, external=None):
    store = store_root(case, root, native_root)
    directory = store / 'stage3' / 'db' / case['case_id']
    if case['arm'] == 'B':
        return dict(kind='native', options=dict(
            path=str(directory / 'native'),
            value_cache_size=case['cache'],
            durability='value_wal_barrier' if case['tier'] == 'synchronous'
            else 'page_cache'))
    if external is None:
        return dict(kind='tidehunter', options=dict(
            transport=dict(kind='tcp-trusted'),
            path=str(directory / 'coordinator'),
            evidence=str(directory / 'evidence'),
            owners=OWNERS, key_count=KEYS, durability_tier=case['tier'],
            value_cache_size=case['cache'],
            local_geometry=dict(fragment_bytes=4 << 20, wal_file_bytes=64 << 20),
            index=dict(frag_size=8 << 20, wal_file_size=32 << 20)))
    prepared = external[case['tier']]
    return dict(kind='tidehunter', options=dict(
        transport=dict(CONTROL_TRANSPORT),
        path=str(directory / 'coordinator'),
        evidence=str(directory / 'evidence'),
        owners=OWNERS, key_count=KEYS, durability_tier=case['tier'],
        value_cache_size=case['cache'],
        coordinator_read_cache_bytes=0,
        external_remote_config=prepared['document'],
        external_owner_durability_receipts=[
            str(receipt_path(root, case['tier'], partition))
            for partition in range(OWNERS)]))


def run_config(case, root, *, native_root=None, external=None,
               measurement_operations=MEASUREMENT_OPERATIONS):
    distribution = ('uniform' if case['distribution'] == 'uniform'
                    else dict(zipf=dict(exponent=1.0)))
    workload = dict(kind='synthetic', distribution=distribution,
                    mix=MIXES[case['mix']], batch_size=case['batch'] or BATCH,
                    put_key_policy='overwrite')
    return dict(
        schema_version=1, case_id=case['case_id'],
        backend=backend(case, root, native_root=native_root, external=external),
        seed=SEED, namespace=f'stage3-{case["case_id"]}',
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


def write_one(path, document):
    if Path(path).exists():
        if json.loads(Path(path).read_text()) != document:
            raise ValueError('the generated spec differs from these settings; '
                             f'use a fresh root or delete {path}')
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(document, indent=2, sort_keys=True) + '\n')
    return Path(path)


def write(root, measurement_operations=MEASUREMENT_OPERATIONS, *, counts=None,
          native_root=None, external=None):
    """Write every spec; a spec whose settings differ is never overwritten."""
    root = Path(root)
    if not root.is_absolute():
        raise ValueError(f'the campaign root must be absolute: {root}')
    if native_root is not None and not Path(native_root).is_absolute():
        raise ValueError(f'the native store root must be absolute: {native_root}')
    counts = operation_counts(measurement_operations, counts)
    output = root / 'stage3' / 'spec'
    output.mkdir(parents=True, exist_ok=True)
    written = []
    for case in cells():
        written.append(write_one(
            output / f'{case["case_id"]}.json',
            run_config(case, root, native_root=native_root, external=external,
                       measurement_operations=count_for(case, counts))))
    return written


def write_probe(root, *, native_root=None, external=None):
    """Write the phase-A probe spec; it carries no calibrated count."""
    root = Path(root)
    if not root.is_absolute():
        raise ValueError(f'the campaign root must be absolute: {root}')
    case = probe_case()
    return write_one(
        root / 'stage3' / 'probe' / 'spec' / f'{case["case_id"]}.json',
        run_config(case, root, native_root=native_root, external=external,
                   measurement_operations=PROBE_OPERATIONS))


def external_items(values):
    """Parse repeated `TIER=DIR` values into `[(tier, Path)]`."""
    items = []
    for value in values:
        tier, separator, directory = value.partition('=')
        if not separator or not directory:
            raise ValueError(f'--external needs TIER=DIR, e.g. native=/path: {value}')
        items.append((tier.strip(), Path(directory)))
    return items


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True,
                        help='campaign directory that owns stage3/{db,spec,out}')
    parser.add_argument('--measurement-operations', type=int, default=MEASUREMENT_OPERATIONS,
                        help='one count for every family when no calibration is given')
    parser.add_argument('--calibration', type=Path,
                        help='frozen phase-B calibration file with the family counts')
    parser.add_argument('--rehearsal', action='store_true',
                        help='emit the single-host trusted TCP ordinary-file fixture')
    parser.add_argument('--external', action='append', default=[], metavar='TIER=DIR',
                        help='prepare_external.py output per tier, repeated')
    parser.add_argument('--external-database', help='config.json database key every cell uses')
    parser.add_argument('--native-root', type=Path,
                        help='absolute store root of the owner host (arm B)')
    parser.add_argument('--validate', type=Path,
                        help='kv-research binary; validates every generated RunConfig')
    args = parser.parse_args()
    if args.rehearsal and args.external:
        raise SystemExit('--rehearsal runs the local fixture and takes no --external')
    if not args.rehearsal and not args.external:
        raise SystemExit('the cluster set needs --external TIER=DIR for both tiers; '
                         'pass --rehearsal for the labelled single-host fixture')
    external = (external_map(external_items(args.external), database=args.external_database)
                if args.external else None)
    counts = read_calibration(args.calibration) if args.calibration else None
    paths = write(args.root, args.measurement_operations, counts=counts,
                  native_root=args.native_root, external=external)
    probe = (None if args.rehearsal else
             write_probe(args.root, native_root=args.native_root, external=external))
    if args.validate:
        import subprocess
        for path in paths:
            subprocess.run([str(args.validate), '--config', str(path),
                            '--validate-config', '--output', str(args.root / 'stage3' / 'admission')],
                           check=True, stdout=subprocess.DEVNULL, timeout=60)
    print(json.dumps(dict(cells=len(paths), sizes=list(SIZES),
                          probe=str(probe) if probe else None,
                          measurement_operations=counts or args.measurement_operations,
                          mode='rehearsal' if external is None else 'cluster')))


if __name__ == '__main__':
    main()
