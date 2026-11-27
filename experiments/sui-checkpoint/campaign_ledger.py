#!/usr/bin/env python3
"""Resumable campaign ledger shared by the Sui and stage-3 runners.

One entry per cell unit:
  id, stage, unit, config_hash, status, attempts, started_unix_ns,
  finished_unix_ns, result_paths, start_from, failure, stale, updated_ns.

The local file is written atomically (temporary file, fsync, rename, directory
fsync). A bucket copy is written with a generation precondition and merged by
`updated_ns`, so a controller and a worker cannot erase each other's entries.
A cell is `passed` only after `mark_passed` has verified every artifact's size
and SHA-256 locally and, when a store is supplied, the uploaded object.
`running` at load time means the process died mid-cell; `reconcile` turns it
into `failed` so `plan` reruns it.

`not_reached` is a cell whose injection point the replay cannot produce (for
example a synchronous flush edge that never appears); it is not a correctness
result and it runs once more. A second `not_reached` attempt is `invalid`: the
cell is terminal and `plan` never reruns it.

`plan` is the resume decision:
  * `passed` with the current config hash is skipped;
  * `invalid` is terminal and skipped;
  * `failed`, `not_reached`, `pending`, stale and interrupted cells run again;
  * all members of a unit run together when any member must run;
  * `only` and `from_` narrow or shift the plan without breaking unit grouping.

`MANUAL_SUI_CELLS` is the reviewed stage-1/2 table of the `manual_sui.py`
cells. One row fixes the cell id, its stage and tier, the run.py flags the cell
needs, the production geometry (1024 MiB fragments, coordinator read cache off)
unless the plan's synthetic mechanism row forces another value (M3 and M4 use
64 MiB fragments), and the criteria the cell must prove.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import time

STATUSES = ('pending', 'running', 'passed', 'failed', 'invalid', 'not_reached')
PASSED = 'passed'
INVALID = 'invalid'

# The campaign production geometry: coordinator read cache off in every cell and
# a 1024 MiB coordinator index fragment. A cell whose plan row forces another
# value declares it in its table row (M3 and M4 are the 64 MiB rows).
DEFAULT_FRAGMENT_MIB = 1024
DEFAULT_COORDINATOR_CACHE_MIB = 0
REVIEWED_STAGES = ('stage1', 'stage2')


def _cell(cell_id, stage, tier, *, requires=(), run=(), fragment_mib=None,
          coordinator_cache_mib=None, max_maps=None, max_index_maps=None,
          hold_seconds=0):
    """One reviewed cell row; `run` is a tuple of `(flag[, value])` tuples."""
    flags = {}
    for item in run:
        if len(item) > 2:
            raise ValueError(f'cell {cell_id}: a run flag takes a name and at most one value')
        flags[item[0]] = item[1] if len(item) == 2 else None
    return dict(
        id=cell_id, stage=stage, tier=tier, requires=tuple(requires), run=flags,
        fragment_mib=(DEFAULT_FRAGMENT_MIB if fragment_mib is None else fragment_mib),
        coordinator_cache_mib=(DEFAULT_COORDINATOR_CACHE_MIB
                               if coordinator_cache_mib is None
                               else coordinator_cache_mib),
        max_maps=max_maps, max_index_maps=max_index_maps, hold_seconds=hold_seconds)


def _manual_sui_cells():
    rows = [
        # ---- stage 1: correctness -------------------------------------------
        # The admission cell runs on both tiers: the synchronous one is the
        # first FUA write on this image. It replays the full fixture: a short
        # run exits at its bound before the last scrape can observe it, and
        # its final metrics would predate the replayed checkpoints.
        _cell('S1-0-native', 'stage1', 'native', requires=('admission',)),
        _cell('S1-0-synchronous', 'stage1', 'synchronous', requires=('admission',)),
        _cell('S1-1-native', 'stage1', 'native', requires=('lifecycle',),
              run=(('--lifecycle-checks',), ('--tail-checkpoints', '64'))),
        _cell('S1-1-synchronous', 'stage1', 'synchronous', requires=('lifecycle',),
              run=(('--lifecycle-checks',), ('--tail-checkpoints', '64'))),
        _cell('S1-7-cache0-native', 'stage1', 'native', requires=('cache',),
              run=(('--value-cache-size', '0'),)),
        _cell('S1-7-cache128-native', 'stage1', 'native', requires=('cache',),
              run=(('--default-value-cache-size', '128'),)),
        _cell('S1-7-cache1000-native', 'stage1', 'native', requires=('cache',)),
        _cell('S1-7-cache1000-synchronous', 'stage1', 'synchronous',
              requires=('cache',)),
        _cell('S1-7-blob1024-native', 'stage1', 'native', requires=('cache',),
              run=(('--index-blob-cache-mib', '1024'),
                   ('--index-blob-cache-min-bytes', '0'))),
        _cell('S1-8-native', 'stage1', 'native', requires=('lifecycle',),
              run=(('--lifecycle-checks',), ('--tail-checkpoints', '64'))),
        _cell('S1-8-synchronous', 'stage1', 'synchronous', requires=('lifecycle',),
              run=(('--lifecycle-checks',), ('--tail-checkpoints', '64'))),
        # ---- stage 2: mechanism and replay ----------------------------------
        _cell('M1-c1-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '1'),)),
        _cell('M1-c2-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '2'),)),
        _cell('M1-c4-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '4'),)),
        _cell('M1-c8-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '8'),)),
        _cell('M1-c16-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '16'),)),
        _cell('M1-c4-synchronous', 'stage2', 'synchronous',
              requires=('write_batches',), run=(('--driver-concurrency', '4'),)),
        _cell('M1-paced-c4-native', 'stage2', 'native', requires=('write_batches',),
              run=(('--driver-concurrency', '4'), ('--pacing-ms', '245'))),
        _cell('M2-cache1000-native', 'stage2', 'native', requires=('value_cache',)),
        _cell('M2-cache128-native', 'stage2', 'native', requires=('value_cache',),
              run=(('--default-value-cache-size', '128'),)),
        _cell('M2-cache0-native', 'stage2', 'native', requires=('value_cache',),
              run=(('--value-cache-size', '0'),)),
        _cell('M2-cache1000-synchronous', 'stage2', 'synchronous',
              requires=('value_cache',)),
        _cell('M3-native', 'stage2', 'native', fragment_mib=64,
              max_maps=3, max_index_maps=3,
              run=(('--max-dirty-keys', '16'), ('--max-maps', '3'),
                   ('--max-index-maps', '3'))),
        _cell('M3-synchronous', 'stage2', 'synchronous', fragment_mib=64,
              max_maps=3, max_index_maps=3,
              run=(('--max-dirty-keys', '16'), ('--max-maps', '3'),
                   ('--max-index-maps', '3'))),
        _cell('M4-native', 'stage2', 'native', fragment_mib=64,
              run=(('--index-blob-cache-mib', '1024'),
                   ('--index-blob-cache-min-bytes', '0'))),
        _cell('M4-synchronous', 'stage2', 'synchronous', fragment_mib=64,
              run=(('--index-blob-cache-mib', '1024'),
                   ('--index-blob-cache-min-bytes', '0'))),
        _cell('M5-hold', 'stage2', 'native', requires=('hold',), hold_seconds=1800,
              run=(('--hold-seconds', '1800'),)),
    ]
    # S1-4 and M7 are one launcher flag (--controller-loss) and a derived cell
    # id. A Native `during-flush` point is unreachable and is not a cell.
    for point in ('mid-frame', 'after-adoption'):
        for tier in ('native', 'synchronous'):
            rows.append(_cell(f'S1-4-{point}-{tier}', 'stage1', tier))
            rows.append(_cell(f'M7-{point}-{tier}', 'stage2', tier))
    rows.append(_cell('S1-4-during-flush-synchronous', 'stage1', 'synchronous'))
    rows.append(_cell('M7-during-flush-synchronous', 'stage2', 'synchronous'))
    return tuple(rows)


MANUAL_SUI_CELLS = _manual_sui_cells()
_BY_ID = {row['id']: row for row in MANUAL_SUI_CELLS}


def cell(cell_id):
    """The reviewed row of one manual_sui cell, or None for a rehearsal id."""
    return _BY_ID.get(cell_id)


def cells(stage=None):
    """Reviewed cells in plan order, optionally only one stage."""
    if stage is not None and stage not in REVIEWED_STAGES:
        raise ValueError('a reviewed manual_sui stage is ' + ' or '.join(REVIEWED_STAGES))
    return [row for row in MANUAL_SUI_CELLS
            if stage is None or row['stage'] == stage]


def run_arguments(row):
    """The reviewed run.py argv of one cell, as the operator must pass it."""
    argv = []
    for flag, value in row['run'].items():
        argv.append(flag)
        if value is not None:
            argv.append(value)
    return argv



class PreconditionFailed(RuntimeError):
    """The bucket object changed since it was read."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def config_hash(*, spec, knobs, engine_rev, sui_rev, binary_ids):
    """Identity of everything that can change a cell's behaviour."""
    return sha256_bytes(canonical(dict(schema=1, spec=spec, knobs=knobs,
                                       engine_rev=engine_rev, sui_rev=sui_rev,
                                       binary_ids=binary_ids)))


class Ledger:
    def __init__(self, path=None):
        self.path = Path(path) if path else None
        self.entries = {}

    # ---- serialization -------------------------------------------------
    def to_bytes(self):
        return (json.dumps(dict(schema=1, entries=sorted(
            self.entries.values(), key=lambda e: (e['stage'], e['id']))),
            indent=2, sort_keys=True) + '\n').encode()

    @classmethod
    def from_bytes(cls, data):
        value = cls()
        for entry in json.loads(data)['entries']:
            value.entries[(entry['stage'], entry['id'])] = entry
        value.reconcile()
        return value

    def load(self):
        if self.path and self.path.exists():
            self.entries = Ledger.from_bytes(self.path.read_bytes()).entries
        return self

    def reconcile(self):
        """A `running` entry is an interrupted attempt, never a success."""
        for entry in self.entries.values():
            if entry['status'] == 'running':
                entry['status'] = 'failed'
                entry['failure'] = entry.get('failure') or 'interrupted'
                entry['finished_unix_ns'] = entry.get('finished_unix_ns') or time.time_ns()
        return self

    def save(self):
        if self.path is None:
            raise ValueError('ledger has no local path')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_bytes()
        descriptor, temporary = tempfile.mkstemp(
            prefix=self.path.name + '.', suffix='.tmp', dir=self.path.parent)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return self.path

    # ---- bucket copy ---------------------------------------------------
    def merge(self, remote):
        """Keep the newer entry per cell; local wins on an exact tie."""
        merged = Ledger()
        keys = set(self.entries) | set(remote.entries)
        for key in keys:
            mine, theirs = self.entries.get(key), remote.entries.get(key)
            if mine is None:
                merged.entries[key] = theirs
            elif theirs is None or mine['updated_ns'] >= theirs['updated_ns']:
                merged.entries[key] = mine
            else:
                merged.entries[key] = theirs
        return merged

    def save_store(self, store, key, attempts=3):
        """Generation-preconditioned write; retries merge the concurrent copy."""
        merged = self
        for _ in range(attempts):
            try:
                current, generation = store.get(key)
            except KeyError:
                current, generation = None, 0
            if current:
                merged = merged.merge(Ledger.from_bytes(current))
            try:
                generation = store.put(key, merged.to_bytes(), generation)
            except PreconditionFailed:
                continue
            self.entries = merged.entries
            return generation
        raise PreconditionFailed(f'ledger write conflicted {attempts} times: {key}')

    # ---- state transitions ---------------------------------------------
    def begin(self, *, stage, cell_id, config_hash, unit=None, start_from='fresh'):
        key = (stage, cell_id)
        previous = self.entries.get(key)
        stale = bool(previous and previous['status'] == PASSED
                     and previous.get('config_hash') != config_hash)
        entry = dict(
            id=cell_id, stage=stage, unit=unit or cell_id, config_hash=config_hash,
            status='running', attempts=(previous['attempts'] if previous else 0) + 1,
            started_unix_ns=time.time_ns(), finished_unix_ns=None,
            result_paths=[], start_from=start_from, failure=None, stale=stale,
            host=socket.gethostname(), updated_ns=time.time_ns())
        self.entries[key] = entry
        return entry

    def fail(self, entry, reason):
        entry.update(status='failed', failure=str(reason),
                     finished_unix_ns=time.time_ns(), updated_ns=time.time_ns())
        return entry

    def invalidate(self, entry, reason):
        entry.update(status='invalid', failure=str(reason),
                     finished_unix_ns=time.time_ns(), updated_ns=time.time_ns())
        return entry

    def not_reached(self, entry, reason):
        """Record a cell the replay could not produce; not a failure."""
        entry.update(status='not_reached', failure=str(reason),
                     finished_unix_ns=time.time_ns(), updated_ns=time.time_ns())
        return entry

    def mark_passed(self, entry, artifacts, *, store=None):
        """Verify local and uploaded artifacts, then and only then pass."""
        verified = []
        for artifact in artifacts:
            path = Path(artifact['path'])
            if not path.is_file():
                raise ValueError(f'missing result artifact: {path}')
            size = path.stat().st_size
            digest = sha256_file(path)
            if size != artifact['size'] or digest != artifact['sha256']:
                raise ValueError(f'result artifact differs: {path}')
            if store is not None:
                head = store.head(artifact['key'])
                if (int(head.get('size', -1)) != size
                        or head.get('sha256') != digest):
                    raise ValueError(f'uploaded artifact differs: {artifact["key"]}')
            verified.append(dict(path=str(path), key=artifact.get('key'),
                                 size=size, sha256=digest))
        entry.update(status=PASSED, failure=None, result_paths=verified,
                     finished_unix_ns=time.time_ns(), updated_ns=time.time_ns())
        return entry

    def status(self, stage, cell_id):
        return self.entries.get((stage, cell_id), {}).get('status')


def plan(cells, ledger, *, only=None, from_=None):
    """Cells to run, in plan order, with units kept together."""
    order = list(cells)
    allowed = {cell['id'] for cell in order}
    if only:
        requested = set(only)
        if not requested <= allowed:
            raise ValueError('unknown --only cell: ' + ', '.join(sorted(requested - allowed)))
        allowed = requested
    if from_:
        if from_ not in {cell['id'] for cell in order}:
            raise ValueError(f'unknown --from cell: {from_}')
        start = min(index for index, cell in enumerate(order) if cell['id'] == from_)
        allowed &= {cell['id'] for cell in order[start:]}
    selected_units = {cell.get('unit', cell['id']) for cell in order
                      if cell['id'] in allowed}
    run_units = set()
    for cell in order:
        unit = cell.get('unit', cell['id'])
        if unit not in selected_units:
            continue
        entry = ledger.entries.get((cell['stage'], cell['id']))
        if entry and entry['status'] == INVALID:
            continue
        if (entry and entry['status'] == PASSED
                and entry.get('config_hash') == cell['config_hash']):
            continue
        run_units.add(unit)
    return [cell for cell in order if cell.get('unit', cell['id']) in run_units]
