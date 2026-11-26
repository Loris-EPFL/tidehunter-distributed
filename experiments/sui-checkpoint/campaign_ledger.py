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
result and it runs again like any other unfinished cell.

`plan` is the resume decision:
  * `passed` with the current config hash is skipped;
  * `failed`, `invalid`, `not_reached`, `pending`, stale and interrupted cells run again;
  * all members of a unit run together when any member must run;
  * `only` and `from_` narrow or shift the plan without breaking unit grouping.
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
        if (entry and entry['status'] == PASSED
                and entry.get('config_hash') == cell['config_hash']):
            continue
        run_units.add(unit)
    return [cell for cell in order if cell.get('unit', cell['id']) in run_units]
