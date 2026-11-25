#!/usr/bin/env python3
"""Synthetic mechanism cells for campaign stage 2 (M6, M9, M13) and the
stage-1 fault substitutes (S1-2S, S1-3S, S1-5S/M8, S1-6S).

The frozen `storage-node` binary is the `kv-research` executable, so a cell is
a `RunConfig` document over the same external NVMe-oF deployment the Sui stages
use (`prepare_external.py` -> `config.json`). The coordinator of a cell is this
process; each owner is the `--experimental-storage-node` mode of the same
binary. Nothing here opens a device, an SSH session or a cloud resource: the
operator driver `experiments/gcp-rdma/fault_cells.py` owns the lifecycle.

Two limits decide the shape of this table, and both are properties of the
frozen binary, not of the plan:

* `dataset.value_size` is one of 64, 256, 512, 1024, 4096, 8192, 16384, 65536,
  so the plan's 132,462 B mainnet value cannot be written literally. M13 uses a
  two-member `put_batch` of 65,536 B values, whose encoded frame is 131,296 B:
  the same 132 KB class, 1,166 B below the plan's figure and 65,760 B above the
  64 KiB `leased_flush_min_frame_bytes` threshold it is meant to cross.
* the coordinator's native registry (`flush_pending`, `large_table_contention`,
  `flush_count`, `snapshot_written_bytes`) is created by the engine with a
  throwaway `Registry`, so a `kv-research` cell exports only the
  `tidehunter_nvmeof_live_*` family. A cell therefore never claims a native
  counter; M6 reports latency, owner read RPCs and live device reads, and says
  so in its evidence.

Every synthetic cell is labelled `synthetic` and its plan family, so a table
built from this file cannot be mistaken for a Sui-adapter result.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

KEYS = 65_536
SEED = 1775
OWNERS = 2
TIERS = ('native', 'synchronous')
UNIFORM = 'uniform'
# 2 x (65,536 + KEY_BYTES(32) + 32) + 96 = 131,296 B: the plan's mainnet batch is
# 132,462 B and the covering-flush threshold (`leased_flush_min_frame_bytes`)
# is 65,536 B. 65,536 B is the largest legal `dataset.value_size`, so a
# two-member batch is the closest frame the frozen binary can build.
BATCH_132KB = 2
VALUE_132KB = 65_536
ENCODED_132KB = (VALUE_132KB + 32 + 32) * BATCH_132KB + 96
FUA_THRESHOLD = 64 << 10
# Synthetic cells reuse one namespace per unit so a restart, a second
# coordinator or a verification pass addresses the same physical keys.
UNIT_PREFIX = 'mechanism'


def unit_namespace(unit):
    """32 lowercase hex digits, the binary's `key_namespace_hex` form."""
    return hashlib.sha256(f'{UNIT_PREFIX}:{unit}'.encode()).hexdigest()[:32]


def _mix(read=0, put=0, delete=0, missing_read=0, get_many=0, put_batch=0):
    return dict(read=read, put=put, delete=delete, missing_read=missing_read,
                get_many=get_many, put_batch=put_batch)


def _run(role, *, mix, value=8192, batch=2, concurrency=4, keys=KEYS,
         preload=True, verify=False, warmup=0, measurement=20_000, deadline=900):
    """One `kv-research` invocation inside a cell unit."""
    if batch < 2:
        raise ValueError('a run needs batch_size >= 2; width 1 is a point get')
    return dict(role=role, mix=mix, value=value, batch=batch,
                concurrency=concurrency, keys=keys, preload=preload,
                verify=verify, warmup=warmup, measurement=measurement,
                deadline=deadline)


def read_run(reads, *, value=8192, verify=False, deadline=300):
    """A read-only run over an existing dataset.

    `verify=True` reads every dataset key and compares the deterministic payload,
    which is the only read-only form that cannot pass on an absent key.
    """
    return _run('verify', mix=_mix(read=100), value=value, preload=False,
                verify=verify, measurement=reads, deadline=deadline)


def _verify_run(reads, *, value=8192):
    """The read-only full-dataset audit that follows every crash or restart.

    `value` must equal the written cell's value: the payload is a deterministic
    function of (key, value size), so a different size would fail every key.
    """
    return _run('verify', mix=_mix(read=100), value=value, preload=False,
                verify=True, measurement=reads)


def cells():
    """The synthetic cell table, in plan order."""
    rows = []

    # M6: multi_get width. A width of 1 is a point get (the binary refuses a
    # batch of one). The plan names two keyspaces (objects, executed_effects);
    # a synthetic cell has one keyspace, so the union of the plan's widths is
    # run once instead of once per keyspace.
    for width in (1, 4, 8, 26):
        mix = _mix(read=100) if width == 1 else _mix(get_many=100)
        rows.append(dict(
            id=f'M6-w{width}-c16', stage='stage2', unit=f'M6-w{width}-c16',
            family='M6', synthetic=True, tier='native',
            plan=dict(widths=[1, 8, 26], keyspaces=['objects', 'executed_effects'],
                      cells='15'),
            note='width 1 is a point get; one synthetic keyspace covers both plan keyspaces',
            runs=[_run('measure', mix=mix, batch=max(width, 2), concurrency=16,
                       warmup=2_000)]))
    rows.append(dict(
        id='M6-page256-c16', stage='stage2', unit='M6-page256-c16', family='M6',
        synthetic=True, tier='native',
        plan=dict(widths=[256], keyspaces=['objects', 'executed_effects'], cells='15'),
        note='one 256-key engine page',
        runs=[_run('measure', mix=_mix(get_many=100), batch=256, concurrency=16,
                   warmup=2_000)]))
    for concurrency in (1, 2, 4, 8):
        rows.append(dict(
            id=f'M6-w26-c{concurrency}', stage='stage2',
            unit=f'M6-w26-c{concurrency}', family='M6', synthetic=True,
            tier='native',
            plan=dict(widths=[26], keyspaces=['objects', 'executed_effects'], cells='15'),
            note='concurrency sweep at width 26; c16 is M6-w26-c16',
            runs=[_run('measure', mix=_mix(get_many=100), batch=26,
                       concurrency=concurrency, warmup=2_000)]))

    # M9: forced owner file roll. 16 MiB owner extents, 256 MiB owner WAL files
    # (`prepare_external.py --owner-wal harness`). At 16 KiB values, the preload
    # already writes about 512 MiB per owner, so one cell crosses several file
    # rolls; the plan's "two roll cycles x 2 tiers" is one cell per tier because
    # a second cell would only repeat the same preload.
    for tier in TIERS:
        rows.append(dict(
            id=f'M9-roll2-{tier}', stage='stage2', unit=f'M9-roll2-{tier}',
            family='M9', synthetic=True, tier=tier,
            plan=dict(cells='4', cycles='2 per cell'),
            note='owner WAL rolls by written volume; the coordinator index keeps '
                 'its engine geometry because TH_* is a Sui-adapter knob',
            runs=[_run('measure', mix=_mix(put=100), value=16_384,
                       concurrency=4, measurement=32_768, deadline=1_800)]))

    # M13: 132 KB Synchronous batch. The covering flush is visible as
    # `leased_flush_covered_frames_total` on the coordinator's live family.
    rows.append(dict(
        id='M13-batch132-synchronous', stage='stage2',
        unit='M13-batch132-synchronous', family='M13', synthetic=True,
        tier='synchronous',
        plan=dict(cells='1', batch_bytes=132_462),
        note=f'put_batch {BATCH_132KB} x {VALUE_132KB} B = {ENCODED_132KB} B encoded; '
             'concurrency 16 keeps frames overlapping so the group flush can cover them',
        runs=[_run('measure', mix=_mix(put_batch=100), value=VALUE_132KB,
                   batch=BATCH_132KB, concurrency=16, measurement=20_000)]))

    # S1-2S / S1-3S / S1-6S: write, inject, restart, verify. The verify run is
    # the plan's audit: it reads every key and compares the deterministic
    # payload, and it must open the coordinator as a reopen, not a create.
    for tier in TIERS:
        for cut in ('publication', 'snapshot'):
            unit = f'S1-2S-{cut}-{tier}'
            rows.append(dict(
                id=unit, stage='stage1S', unit=unit, family='S1-2',
                synthetic=True, tier=tier,
                plan=dict(cut=cut, tiers='native, synchronous', cells='4'),
                note='kill -9 the coordinator at the gated cut, reopen, full audit',
                runs=[_run('measure', mix=_mix(put=100), value=4096,
                           concurrency=4, measurement=1_000_000, deadline=600),
                      _verify_run(4_096, value=4096)]))
        unit = f'S1-3S-owner-{tier}'
        rows.append(dict(
            id=unit, stage='stage1S', unit=unit, family='S1-3',
            synthetic=True, tier=tier,
            plan=dict(cut='owner', tiers='native, synchronous', cells='2'),
            note='kill -9 one owner, restart it as a new incarnation, reopen, full audit',
            runs=[_run('measure', mix=_mix(put=100), value=4096,
                       concurrency=4, measurement=1_000_000, deadline=600),
                  _verify_run(4_096, value=4096)]))
        for cut in ('unlink', 'dir-sync'):
            unit = f'S1-6S-{cut}-{tier}'
            rows.append(dict(
                id=unit, stage='stage1S', unit=unit, family='S1-6',
                synthetic=True, tier=tier,
                plan=dict(cut=cut, tiers='native, synchronous', cells='4'),
                note='kill -9 the owner at the observed retirement cut, restart, full audit',
                runs=[_run('measure', mix=_mix(put=100), value=8192,
                           concurrency=4, measurement=65_536, deadline=1_200),
                      _verify_run(4_096)]))
        for case in ('contend', 'lapse', 'stale', 'sealed'):
            unit = f'S1-5S-{case}-{tier}'
            rows.append(dict(
                id=unit, stage='stage1S', unit=unit, family='S1-5', case=case,
                synthetic=True, tier=tier,
                plan=dict(cases=['contend', 'lapse', 'stale', 'sealed'],
                          tiers='native, synchronous', cells='8', also='M8'),
                note='two coordinators on one owner database; lease lapse, stale '
                     'write, sealed read, fence latency',
                runs=[_run('measure', mix=_mix(put=100), value=4096,
                           concurrency=4, measurement=1_000_000, deadline=600)]))
    return rows


def find(cell_id):
    for cell in cells():
        if cell['id'] == cell_id:
            return cell
    raise ValueError('unknown mechanism cell: ' + cell_id)


def _key_distribution(cell):
    return UNIFORM


def spec(cell, run, context, *, path, evidence):
    """The RunConfig document of one run of one cell.

    `context` is the prepared external deployment: `remote` is the per-database
    document of `config.json`, `transport` its canonical transport object and
    `receipts` the copied `ready.durability.json` paths in partition order.
    """
    mix = run['mix']
    workload = dict(kind='synthetic', distribution=_key_distribution(cell), mix=mix,
                    batch_size=run['batch'], put_key_policy='overwrite')
    phases = dict(preload=run['preload'],
                  verify_existing_dataset=run['verify'],
                  warmup_operations=run['warmup'], aging_operations=0,
                  measurement_operations=run['measurement'])
    operations = (run['keys'] if run['preload'] or run['verify'] else 0) \
        + run['warmup'] + run['measurement']
    options = dict(
        transport=context['transport'], path=str(path), evidence=str(evidence),
        owners=OWNERS, key_count=run['keys'],
        external_remote_config=context['remote'],
        external_owner_durability_receipts=list(context['receipts']),
        durability_tier=cell['tier'], value_cache_size=0,
        coordinator_read_cache_bytes=context['remote']['read_cache_bytes'])
    return dict(
        schema_version=1, case_id=f"{cell['id']}-{run['role']}",
        backend=dict(kind='tidehunter', options=options),
        seed=SEED, namespace=f"mech-{cell['id']}-{run['role']}"[:64],
        key_namespace_hex=unit_namespace(cell['unit']),
        dataset=dict(keys=run['keys'], value_size=run['value'], entropy=1.0),
        workload=workload,
        execution=dict(mode='closed_loop', concurrency=run['concurrency'],
                       queue_capacity=64, request_timeout_ms=30_000),
        phases=phases,
        diagnostics=dict(write_profile_limit=0, read_profile_limit=0),
        qualification_only=False,
        source_revision=None,
        limits=dict(max_keys=run['keys'], max_bytes=16 << 30,
                    max_operations=max(operations, 1),
                    deadline_seconds=run['deadline'], shutdown_grace_seconds=30))


def write(root, cell_ids=None):
    """Write one `spec` directory per selected cell unit; no deployment needed."""
    root = Path(root)
    output = root / 'mechanism' / 'spec'
    output.mkdir(parents=True, exist_ok=True)
    written = []
    selected = cells() if not cell_ids else [find(name) for name in cell_ids]
    for cell in selected:
        for index, run in enumerate(cell['runs']):
            path = output / f"{cell['id']}.{index}.{run['role']}.json"
            path.write_text(json.dumps(
                dict(cell=cell, run=run, template=spec(
                    cell, run, dict(transport={'kind': 'tcp'},
                                    remote={'read_cache_bytes': 0},
                                    receipts=[]),
                    path='<coordinator>', evidence='<evidence>')),
                indent=2, sort_keys=True) + '\n')
            written.append(path)
    return written


# ---- verification ---------------------------------------------------------

def live(counters):
    """Decode the aggregate live NVMe-oF family from a prometheus text dump."""
    values = {}
    for line in counters.splitlines():
        if not line or line.startswith('#'):
            continue
        name, _, raw = line.rpartition(' ')
        if not name.startswith('tidehunter_nvmeof_live_'):
            continue
        values[name[len('tidehunter_nvmeof_live_'):]] = float(raw)
    return values


def _read(path):
    path = Path(path)
    if not path.is_file():
        raise ValueError(f'missing artifact: {path}')
    return path.read_text()


def _json(path):
    return json.loads(_read(path))


def _summary(output):
    return _json(Path(output) / 'summary.json')


def _manifest(output):
    return _json(Path(output) / 'manifest.json')


def _counts(output):
    """`{phase.operation: offered}` from the run summary's realized mix."""
    phase = _summary(output)['phase_counts']
    mix = phase.get('realized_mix')
    if mix is None:
        raise ValueError('summary lacks phase_counts.realized_mix')
    return {key: row['offered'] for key, row in mix.items()}


def evidence(output):
    """The measurement read evidence: open record and coordinator read delta.

    `measurement-read-evidence.json` carries the measurement boundary start/end
    snapshots; `read-evidence.json` is the close-time fallback. A missing
    diagnostics counter is null, never zero.
    """
    output = Path(output)
    measured = output / 'measurement-read-evidence.json'
    if measured.is_file():
        value = _json(measured)
        start = value.get('start') or {}
        deltas = (value.get('deltas') or {}).get('coordinator') or {}
        opened = start.get('coordinator_open')
        rpc = (deltas.get('counters') or {}).get('read_rpc_count')
        return dict(source=str(measured), open=opened, read_rpc_count=rpc,
                    available=bool(deltas.get('available', True)))
    closed = output / 'read-evidence.json'
    if closed.is_file():
        value = _json(closed)
        coordinator = value.get('coordinator') or {}
        return dict(source=str(closed), open=value.get('coordinator_open'),
                    read_rpc_count=coordinator.get('read_rpc_count'),
                    available=coordinator.get('read_rpc_count') is not None)
    return dict(source=None, open=None, read_rpc_count=None, available=False)


def _distribution(output, phase, operation, metric, outcome='success'):
    key = f'{phase}.{operation}.{metric}.{outcome}'
    row = _summary(output)['distributions'].get(key)
    if row is None:
        raise ValueError(f'summary lacks the distribution {key}')
    return row


def _sample_rows(output, phase='measurement', operation=None, outcome='success'):
    path = Path(output) / 'samples.csv'
    if not path.is_file():
        return []
    lines = path.read_text(errors='replace').splitlines()
    if not lines:
        return []
    header = lines[0].split(',')
    result = []
    for line in lines[1:]:
        values = line.split(',')
        if len(values) != len(header):
            continue
        row = dict(zip(header, values))
        if row.get('phase') != phase or row.get('outcome') != outcome:
            continue
        if operation is not None and row.get('operation') != operation:
            continue
        result.append(row)
    return result


def _quantiles(output, operation, *, field='api_ns'):
    """Exact per-sample quantiles; `summary.json` holds coarse histogram bounds."""
    values = sorted(int(row[field]) for row in _sample_rows(output, operation=operation)
                    if row.get(field))
    if not values:
        return None

    def pick(fraction):
        return values[min(len(values) - 1, int(fraction * len(values)))]

    return dict(count=len(values), p50_ns=pick(0.5), p99_ns=pick(0.99),
                p999_ns=pick(0.999),
                source='samples.csv api_ns; the summary histogram is an upper bound')


def _check(name, ok, detail):
    return dict(check=name, ok=bool(ok), detail=detail)


def verify(cell, output, *, observations=None):
    """Deterministic checks for one run of one cell.

    `output` is the `kv-research --output` directory of one run; `observations`
    carries the operator driver's own evidence (owner disks, WAL file ids, the
    owner event log, kill stamps) and is required only where a check needs it.
    """
    observations = observations or {}
    output = Path(output)
    role = observations.get('role', 'measure')
    manifest = _manifest(output)
    summary = _summary(output)
    stats = live(_read(output / 'live.prom'))
    counts = _counts(output)
    checks = [_check('run completed', manifest['status'] == 'completed',
                     manifest['status'])]
    errors = manifest['errors_or_wrong_values']

    if cell['family'] == 'M6':
        calls = sum(int(value) for key, value in counts.items()
                    if key in ('measurement.get_many', 'measurement.read'))
        read = evidence(output)
        rpc = read['read_rpc_count']
        checks.append(_check('no wrong value or read error', errors == 0, errors))
        checks.append(_check('every read call answered',
                             calls > 0 and manifest['completed_operations'] >= calls,
                             dict(calls=calls,
                                  completed=manifest['completed_operations'])))
        if rpc is not None and calls:
            checks.append(_check('owner read RPCs per call at most one per owner',
                                 rpc / calls <= OWNERS,
                                 dict(read_rpc_count=rpc, calls=calls,
                                      per_call=rpc / calls, owners=OWNERS)))
        operation = 'get_many' if cell['id'].startswith(('M6-w4', 'M6-w8', 'M6-w26',
                                                         'M6-page')) else 'read'
        latency = _distribution(output, 'measurement', operation, 'api')
        exact = _quantiles(output, operation) or {}
        checks.append(_check('api latency recorded', latency['count'] > 0,
                             dict(count=latency['count'],
                                  histogram_p50_upper_bound_ns=latency.get('p50_upper_bound_ns'),
                                  histogram_p99_upper_bound_ns=latency.get('p99_upper_bound_ns'),
                                  samples=exact)))
        checks.append(_check('native contention counter is not claimed', True,
                             'kv-research creates the engine registry privately; '
                             'large_table_contention is Sui-tier only'))

    if cell['family'] == 'M9':
        rolls = observations.get('owner_rolls') or {}
        highest = max((row.get('max_file_id') or 0) for row in rolls.values()) \
            if rolls else 0
        checks.append(_check('two owner WAL file rolls observed', highest >= 2,
                             dict(highest_file_id=highest, rolls=rolls)))
        checks.append(_check('no wrong value or write error', errors == 0, errors))
        checks.append(_check('retirement pass reached the owners',
                             (observations.get('retire_requests') or 0) > 0,
                             observations.get('retire_requests')))

    if cell['family'] == 'M13':
        frames = stats.get('leased_frames_total', 0.0)
        covered = stats.get('leased_flush_covered_frames_total', 0.0)
        fua = stats.get('fua_writes_total', 0.0)
        fraction = (fua / frames) if frames else None
        checks.append(_check('live leased frames recorded', frames > 0, frames))
        checks.append(_check('a covering flush crossed the 64 KiB threshold',
                             covered > 0,
                             dict(covered_frames=covered,
                                  threshold_bytes=FUA_THRESHOLD,
                                  encoded_batch_bytes=ENCODED_132KB)))
        checks.append(_check('enumerated batch is above the flush threshold',
                             ENCODED_132KB > FUA_THRESHOLD,
                             dict(encoded=ENCODED_132KB, threshold=FUA_THRESHOLD)))
        checks.append(_check('most frames avoid FUA',
                             fraction is not None and fraction < 0.5,
                             dict(fua_writes=fua, frames=frames,
                                  fua_fraction=fraction)))
        checks.append(_check('no write error', errors == 0, errors))

    if role == 'verify':
        read = evidence(output)
        opened = read.get('open') or {}
        recovery = opened.get('recovery') or {}
        checks.append(_check('coordinator reopened the existing database',
                             opened.get('kind') == 'reopen', opened.get('kind')))
        checks.append(_check('full dataset verified, no torn value',
                             errors == 0 and manifest['completed_operations'] > 0,
                             dict(errors=errors,
                                  completed=manifest['completed_operations'])))
        checks.append(_check('recovery evidence retained',
                             isinstance(recovery.get('tail_batches'), int),
                             dict(tail_batches=recovery.get('tail_batches'),
                                  tail_bytes=recovery.get('tail_bytes'),
                                  below_cut_skipped=recovery.get('below_cut_skipped'),
                                  fencing_ns=recovery.get('fencing_ns'))))
        restart = observations.get('restart')
        if restart is not None:
            checks.append(_check('owner restarted as a new incarnation',
                                 bool(restart.get('current')) and
                                 not set(restart['current']) & set(restart['previous']),
                                 restart))

    if cell['family'] == 'S1-2':
        gate = observations.get('gate') or {}
        checks.append(_check('cut gate reached before the kill', gate.get('reached') is True,
                             gate.get('label')))
        kill = observations.get('kill') or {}
        checks.append(_check('coordinator was killed, not shut down',
                             kill.get('exit_code') not in (0, None),
                             kill.get('exit_code')))

    if cell['family'] in ('S1-3', 'S1-6'):
        gate = observations.get('gate') or {}
        if cell['family'] == 'S1-6':
            checks.append(_check('retirement cut observed on the owner',
                                 gate.get('reached') is True,
                                 dict(cut=gate.get('cut'), note=gate.get('note'))))
        kill = observations.get('kill') or {}
        checks.append(_check('owner was killed with SIGKILL',
                             bool(kill.get('pids')) and 'still_alive' not in kill,
                             kill.get('pids')))
        failure = observations.get('failure') or {}
        found = failure.get('gate') or {}
        checks.append(_check('coordinator recorded a typed write failure',
                             found.get('reached') is True, found.get('errors')))

    return dict(cell=cell['id'], role=role, tier=cell['tier'], synthetic=True,
                ok=all(check['ok'] for check in checks), checks=checks,
                live={key: stats.get(key) for key in
                      ('leased_frames_total', 'leased_flushes_total',
                       'leased_flush_covered_frames_total', 'fua_writes_total',
                       'completed_read_ops_total', 'errors_total')},
                metrics={'completed_operations': manifest.get('completed_operations'),
                         'errors_or_wrong_values': errors})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true', help='print the cell table')
    parser.add_argument('--generate', type=Path,
                        help='write one spec template per run under ROOT/mechanism/spec')
    parser.add_argument('--cell', action='append', default=[],
                        help='restrict --generate or --verify to this cell id')
    parser.add_argument('--verify', type=Path,
                        help='verify one run output directory')
    parser.add_argument('--role', default='measure')
    args = parser.parse_args()
    if args.list:
        for cell in cells():
            print(json.dumps({key: cell[key] for key in
                              ('id', 'stage', 'family', 'tier', 'synthetic', 'note')},
                             sort_keys=True))
        return
    if args.generate:
        for path in write(args.generate, args.cell or None):
            print(path)
        return
    if args.verify:
        if len(args.cell) != 1:
            raise SystemExit('--verify needs exactly one --cell')
        cell = find(args.cell[0])
        record = verify(cell, args.verify, observations={'role': args.role})
        print(json.dumps(record, indent=2, sort_keys=True))
        raise SystemExit(0 if record['ok'] else 1)
    parser.print_help()


if __name__ == '__main__':
    main()
