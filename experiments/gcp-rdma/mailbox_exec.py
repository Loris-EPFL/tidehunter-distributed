#!/usr/bin/env python3
"""The campaign host executor: one GCS mailbox request per host command.

The three-host Sui campaign has no SSH path: the guests carry no external
address and IAP is not enabled, so `gcloud compute ssh` cannot reach them. This
module is the `host_exec.HostExecution` implementation the drivers inject
instead. It speaks the worker protocol that the earlier P13, P16 and Q0 fleets
already used: one immutable JSON request under
`runs/<run>/mailbox/<instance-id>/requests/<operation-id>.json`, executed at
most once by `gcp_worker.py`, answered by one immutable receipt under
`.../responses/<operation-id>.json`.

Commands run as one `/bin/sh -c` script because the drivers issue shell: archive
extraction with stdin, `setsid` process groups, `kill -TERM -<pgid>`, `/proc`
sampling and the disk collector's stdin. Bytes that do not fit a receipt travel
as bucket blobs bound to the same operation ID: `.../transfer/<id>/<op>/stdin`
is downloaded and SHA-256 verified before the command starts, and
`.../transfer/<id>/<op>/stdout` carries a stdout larger than the receipt bound.

Every wait is bounded by the fleet's own absolute deadline, a refused request
raises `MailboxRefused` instead of hanging, and a missing receipt raises
`MailboxTimeout` because the outcome is unknown. `GcloudSsh` remains only for an
explicit local rehearsal, where a reachable host exists.

The campaign orchestrator runs on the coordinator host, not on the workstation.
`manual_sui.py`, `stage3_campaign.py` and `fault_cells.py` all start their
coordinator work as a local child (`run.py`, stage-3 arm A, the fault
coordinator) because it must reach the owners; that child is correct only on
H0. `run_driver` submits the whole orchestrator as one bounded request, its
admitted script being `<package>/source/experiments/...`, and the driver builds
its executor from the guest metadata identity (`--guest-token`) so the owner
requests are written from H0. The workstation only stages files
(`stage_tree`/`send_file`) and reads the terminal receipt, so nothing depends on
a route or on the 1 MiB inline bound.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tarfile
import tempfile
import time
import uuid

import gcp_api
import gcp_runtime
import host_exec
import manual_campaign

SHELL = '/bin/sh'
MAX_SCRIPT = 64 * 1024
MAX_BLOB = 16 * 1024 * 1024 * 1024
# One poll window; a live command may need hours, so only the run deadline ends
# the wait. Each window re-reads the receipt key and nothing else.
WAIT_SECONDS = 120
READY_SECONDS = 900
# The build bundle (1.1 GB), the fixture (752 MB) and the schema are the same
# bytes for every cell of a fleet, so they are staged under a digest-addressed
# key once per run and the guest records what it already holds.
STAGE_ROOT = '.staged'
# The pinned campaign nvmet module (experiments/nvmeof/kernel_fix.MODULE_SHA).
NVMET_FIX_MODULE_SHA256 = '5ba7b96c3b232e7fc0d1200b674d6210324d87ed406f8b51f4b323609ccc233a'


class MailboxError(host_exec.HostCommandError):
    """A mailbox operation produced no verified result."""


class MailboxTimeout(MailboxError):
    """No terminal receipt before the deadline; the command outcome is unknown."""


class MailboxRefused(MailboxError):
    """The guest refused the request before running it; nothing took effect."""


class MailboxUnknownHost(MailboxError):
    """The host is not one of this fleet's admitted guests."""


def upload_identical(store, key, path, sha256):
    """Upload one file and accept an existing object that holds the same bytes.

    A re-create under the same deterministic run ID publishes the same package
    again. The first attempt's object already exists, so an unconditional
    `ifGenerationMatch=0` fails 412 before any host starts; identical bytes are
    the same admitted input, not a conflict.
    """
    path = Path(path)
    try:
        return store.upload(key, path, content_type='application/octet-stream',
                            if_generation_match=0)
    except Exception as exc:
        if getattr(exc, 'status', None) != 412:
            raise
    head = store.head(key)
    if ((head.get('metadata') or {}).get('sha256') != sha256
            or int(head.get('size', -1)) != path.stat().st_size):
        raise MailboxError(f'{key}: immutable blob already holds other bytes')
    return head


def tree_digest(path):
    """One content digest of a directory tree, independent of mtimes.

    A stat-only signature would restage the same 1.85 GB after a copy; hashing
    the bytes costs a few seconds on a workstation and is the only value that
    proves the guest holds this tree.
    """
    root = Path(path)
    digest = hashlib.sha256()
    for entry in sorted(root.rglob('*'), key=lambda item: item.relative_to(root).as_posix()):
        name = entry.relative_to(root).as_posix()
        if entry.is_symlink():
            digest.update(b'L\0' + name.encode() + b'\0' + os.readlink(entry).encode() + b'\0')
        elif entry.is_dir():
            digest.update(b'D\0' + name.encode() + b'\0')
        elif entry.is_file():
            digest.update(b'F\0' + name.encode() + b'\0'
                          + str(entry.stat().st_size).encode() + b'\0')
            with entry.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
        else:
            raise ValueError(f'{entry}: unsupported entry in a campaign tree')
    return digest.hexdigest()


def admit_nvmet_fix(receipt):
    """The campaign package must carry the pinned nvmet kernel module.

    The guests bind the stock CIQ module otherwise, and a short `IOCB_NOWAIT`
    read on the buffered target can return `-EREMOTEIO`, so the S1-0 admission
    cells cannot pass.
    A package is either resealed with `prepare.py --nvmet-fix-prefix
    checkpoint-work/benchmark-results/pinned-artifacts/nvmet-fix` or it is not
    a campaign package.
    """
    fix = receipt.get('nvmet_fix') if isinstance(receipt, dict) else None
    module = fix.get('module_sha256') if isinstance(fix, dict) else None
    if not isinstance(module, str) or not module.startswith(NVMET_FIX_MODULE_SHA256[:16]):
        raise ValueError(
            'campaign package carries no pinned nvmet fix; reseal it with '
            'prepare.py --nvmet-fix-prefix '
            'checkpoint-work/benchmark-results/pinned-artifacts/nvmet-fix')
    return fix


def admit_package_nvmet_fix(package_dir):
    """Read one sealed receipt and require the pinned module before any host."""
    receipt = json.loads((Path(package_dir) / 'package-receipt.json').read_text())
    return admit_nvmet_fix(receipt)


def replace_object(store, key, data, *, content_type='application/json'):
    """Publish mutable state: reuse identical bytes, else replace with a CAS write.

    The run ID is deterministic, so a second create finds the previous attempt's
    `runtime.json` and per-host `gate.json`. Identical bytes are reused. A
    changed source or script replaces the object, because a fresh guest that
    reads the stale gate aborts its bootstrap and never starts a worker.
    """
    try:
        return store.put(key, data, content_type=content_type, if_generation_match=0)
    except Exception as exc:
        if getattr(exc, 'status', None) != 412:
            raise
    head = store.head(key)
    if store.get(key, generation=head['generation']) == data:
        return head
    return store.put(key, data, content_type=content_type,
                     if_generation_match=head['generation'])


def sui_spec(owners, backends):
    """The runtime specification that admits the worker for the Sui campaign.

    The Sui drivers own their cell list and transport admission; the worker only
    needs an admitted suite, owner count and backend selection.
    """
    import workflow_input
    if type(owners) is not int or not 1 <= owners <= 16:
        raise ValueError('invalid owner count')
    if (not isinstance(backends, (list, tuple)) or not backends
            or any(not isinstance(backend, str) or backend not in workflow_input.BACKENDS
                   for backend in backends)
            or len(set(backends)) != len(backends)):
        raise ValueError('invalid backend selection')
    return {'suite': 'sui', 'mode': 'sui', 'owners': owners, 'backends': list(backends)}


class MailboxExec(host_exec.HostExecution):
    """Run commands and move files on the campaign guests through the bucket."""

    def __init__(self, api, *, bucket, run_id, fleet, source_sha256, deadline_utc,
                 timeout_seconds=None, poll_seconds=1.0):
        if not isinstance(fleet, (list, tuple)) or not 2 <= len(fleet) <= 6:
            raise ValueError('invalid campaign fleet')
        self.mailbox = gcp_runtime.RemoteMailbox(api, bucket, run_id, fleet, source_sha256, deadline_utc)
        self.store = self.mailbox.store
        self.run_id = self.mailbox.run_id
        self.fleet = self.mailbox.fleet
        self.source_sha256 = self.mailbox.source_sha256
        self.deadline_utc = deadline_utc
        self.deadline = self.mailbox.deadline
        self.poll_seconds = poll_seconds
        if timeout_seconds is not None and (type(timeout_seconds) is not int or timeout_seconds < 1):
            raise ValueError('positive integer command timeout required')
        self.timeout_seconds = timeout_seconds
        self.hosts = {}
        for host in self.fleet:
            # A driver may name a host by its fleet name or by either admitted
            # NIC address: the control fabric of a verbs cell binds the RDMA
            # address, and TCP binds the default-subnet address.
            for value in (host['name'], host['internal_ip'], host['rdma_ip']):
                if value in self.hosts:
                    raise ValueError(f'ambiguous campaign host {value}')
                self.hosts[value] = host['index']
        self.operations = 0

    # ---- seam ----------------------------------------------------------
    def execute(self, host, command, *, stdin=None):
        reference = self.submit(host, command, stdin=stdin)
        return self.finish(host, reference)

    def shell(self, host, command, *, stdin=None, check=True):
        # The command is already the admitted shell script; `sh -lc` would add
        # a login profile the guest does not have.
        return self.run(host, command, stdin=stdin, check=check)

    def write_bytes(self, host, path, data):
        path = Path(path)
        return self.run(host, f'mkdir -p {shlex.quote(str(path.parent))} && '
                              f'cat > {shlex.quote(str(path))}', stdin=data)

    def put_file(self, host, path, data):
        return self.write_bytes(host, path, data)

    def get_file(self, host, path):
        return self.read_bytes(host, path)

    # ---- requests ------------------------------------------------------
    def next_operation(self):
        self.operations += 1
        return f'cmd-{self.operations:06d}-{uuid.uuid4().hex[:12]}'

    def index_of(self, host):
        if not isinstance(host, str) or host not in self.hosts:
            raise MailboxUnknownHost(f'{host}: not a guest of this campaign fleet')
        return self.hosts[host]

    def transfer_key(self, index, operation_id, leaf):
        return f'runs/{self.run_id}/transfer/{self.fleet[index]["id"]}/{operation_id}/{leaf}'

    def submit(self, host, command, *, operation_id=None, stdin=None, stdin_blob=None,
               timeout_seconds=None):
        """Publish one immutable request and return its reference.

        Every request names its stdout blob, so a chatty command (the campaign
        orchestrator, `tar -czf -`) spills to the bucket instead of failing at
        the receipt bound.
        """
        index = self.index_of(host)
        if (not isinstance(command, str) or not command or '\x00' in command
                or len(command.encode('utf-8', 'surrogatepass')) > MAX_SCRIPT):
            raise ValueError('invalid campaign command')
        if stdin is not None and stdin_blob is not None:
            raise ValueError('one command takes one stdin')
        operation_id = operation_id or self.next_operation()
        timeout = self.command_timeout(timeout_seconds)
        blob = stdin_blob
        if stdin is not None:
            data = bytes(stdin)
            if len(data) > MAX_BLOB:
                raise ValueError('command stdin exceeds the mailbox bound')
            key = self.transfer_key(index, operation_id, 'stdin')
            self.put_blob(key, data)
            blob = {'object': key, 'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
        if blob is not None and blob.get('object') != self.transfer_key(index, operation_id, 'stdin'):
            raise ValueError('stdin blob does not belong to this operation')
        return self.mailbox.submit(
            index, [SHELL, '-c', command], operation_id=operation_id, timeout_seconds=timeout,
            stdin=blob, stdout_object=self.transfer_key(index, operation_id, 'stdout'))

    def finish(self, host, reference):
        """Read one terminal receipt and return (exit code, stdout, stderr)."""
        receipt = self.wait(reference)
        if receipt.get('rejected'):
            raise MailboxRefused(f'{host}: {reference["operation_id"]} refused: '
                                 f'{receipt["stderr"][-2000:]}')
        return (receipt['exit_code'], self.receipt_bytes(receipt, 'stdout'),
                self.receipt_bytes(receipt, 'stderr'))

    def run_once(self, host, command, *, operation_id, stdin=None, timeout_seconds=None):
        """Run one command under a caller-chosen immutable operation ID."""
        reference = self.submit(host, command, operation_id=operation_id, stdin=stdin,
                                timeout_seconds=timeout_seconds)
        return self.finish(host, reference)

    def command_timeout(self, timeout_seconds=None):
        limit = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError('positive integer command timeout required')
        remaining = max(1, int(self.deadline - time.time()) - 5)
        return max(1, min(limit, remaining)) if limit is not None else remaining

    def wait(self, reference):
        host = reference['instance_id']
        while True:
            remaining = self.deadline - time.time()
            if remaining <= 0:
                raise MailboxTimeout(f'{host}: no terminal receipt for '
                                     f'{reference["operation_id"]} before the deadline')
            try:
                return self.mailbox.wait(reference, timeout_seconds=min(WAIT_SECONDS, remaining))
            except TimeoutError as exc:
                if time.time() >= self.deadline:
                    raise MailboxTimeout(
                        f'{host}: no terminal receipt for {reference["operation_id"]} '
                        'before the deadline; operation outcome unknown') from exc

    # ---- bucket blobs --------------------------------------------------
    def put_blob(self, key, data):
        try:
            return self.store.put(key, data, content_type='application/octet-stream',
                                  if_generation_match=0)
        except Exception as exc:
            if getattr(exc, 'status', None) != 412:
                raise
            existing = self.store.get(key, max_bytes=len(data))
            if existing != data:
                raise MailboxError(f'{key}: immutable blob already holds other bytes') from exc
            return self.store.head(key)

    def get_blob(self, blob):
        meta = self.store.head(blob['object'])
        data = self.store.get(blob['object'], generation=meta['generation'],
                              max_bytes=blob['size'])
        if len(data) != blob['size'] or hashlib.sha256(data).hexdigest() != blob['sha256']:
            raise MailboxError(f'{blob["object"]}: blob differs from its receipt digest')
        return data

    def receipt_bytes(self, receipt, field):
        blob = receipt.get(field + '_object')
        if blob is None:
            return receipt[field].encode()
        if (not isinstance(blob, dict) or set(blob) != {'object', 'sha256', 'size'}
                or not isinstance(blob['sha256'], str)
                or re.fullmatch(r'[0-9a-f]{64}', blob['sha256']) is None
                or type(blob['size']) is not int or not 0 <= blob['size'] <= MAX_BLOB):
            raise MailboxError(f'{field}: invalid receipt blob')
        return self.get_blob(blob)

    # ---- campaign files ------------------------------------------------
    def upload_file(self, key, path):
        """Stream one local file to the bucket and return its admitted blob."""
        path = Path(path)
        size = path.stat().st_size
        if not 0 <= size <= MAX_BLOB:
            raise ValueError('campaign file exceeds the mailbox bound')
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        meta = upload_identical(self.store, key, path, digest.hexdigest())
        if (int(meta.get('size', -1)) != size
                or (meta.get('metadata') or {}).get('sha256') != digest.hexdigest()):
            raise MailboxError(f'{key}: uploaded blob receipt mismatch')
        return {'object': key, 'sha256': digest.hexdigest(), 'size': size}

    def send_local(self, host, command, local, *, operation_id=None):
        """Run one guest command whose stdin is a local file, through the bucket."""
        index = self.index_of(host)
        operation_id = operation_id or self.next_operation()
        blob = self.upload_file(self.transfer_key(index, operation_id, 'stdin'), local)
        reference = self.mailbox.submit(
            index, [SHELL, '-c', command], operation_id=operation_id,
            timeout_seconds=self.command_timeout(None), stdin=blob,
            stdout_object=self.transfer_key(index, operation_id, 'stdout'))
        return self.finish(host, reference)

    def send_file(self, host, path, local, *, operation_id=None, mode=None):
        """Copy one local file to a guest path through the bucket.

        A streamed copy creates the destination with the writer's umask, so a
        caller that stages an executable must name its mode.
        """
        destination = Path(path)
        command = (f'mkdir -p {shlex.quote(str(destination.parent))} && '
                   f'cat > {shlex.quote(str(destination))}')
        if mode is not None:
            command += f' && chmod {mode:o} {shlex.quote(str(destination))}'
        return self.send_local(
            host, command, local, operation_id=operation_id)

    def staging_key(self, name, digest):
        """The one bucket key of a tree under this run, addressed by its digest."""
        return f'runs/{self.run_id}/staging/{name}/{digest}.tar.gz'

    def _stage_archive(self, key, local_dir, digest):
        """Publish one tree under its digest key; never upload the same bytes twice."""
        try:
            head = self.store.head(key)
        except Exception as exc:
            if getattr(exc, 'status', None) != 404:
                raise
        else:
            if (head.get('metadata') or {}).get('tree_sha256') == digest:
                return head
            raise MailboxError(f'{key}: staged tree object holds another digest')
        handle, name = tempfile.mkstemp(suffix='.tar.gz')
        os.close(handle)
        archive = Path(name)
        try:
            with tarfile.open(archive, 'w:gz', compresslevel=1) as bundle:
                for entry in sorted(Path(local_dir).iterdir()):
                    bundle.add(entry, arcname=entry.name, recursive=True)
            try:
                return self.store.upload(
                    key, archive, content_type='application/gzip', if_generation_match=0,
                    metadata={'tree_sha256': digest})
            except Exception as exc:
                if getattr(exc, 'status', None) != 412:
                    raise
                head = self.store.head(key)
                if (head.get('metadata') or {}).get('tree_sha256') != digest:
                    raise MailboxError(f'{key}: staged tree object holds another digest') from exc
                return head
        finally:
            archive.unlink()

    def _discard_transfer(self, key):
        """Drop the per-operation copy; the digest-addressed tree stays."""
        try:
            head = self.store.head(key)
            self.store.delete(key, if_generation_match=head['generation'])
        except Exception as exc:
            if getattr(exc, 'status', None) == 404:
                return
            print(f'{key}: transfer copy not removed: {exc}', file=sys.stderr)

    def stage_tree(self, host, local_dir, guest_dir, *, operation_id=None):
        """Copy one local directory to a guest directory once per run.

        The build bundle (1.1 GB), the fixture (752 MB) and the schema are the
        same bytes for every cell of a fleet. The first cell uploads the tree
        under a digest-addressed key and the guest records that digest in a
        `.staged` marker; every later cell transfers nothing. A guest that does
        not hold the tree receives one server-side copy of the staged object,
        not a second upload from the workstation.
        """
        local_dir = Path(local_dir)
        if not local_dir.is_dir():
            raise ValueError(f'{local_dir}: not a campaign directory')
        index = self.index_of(host)
        destination = Path(guest_dir)
        marker = destination.parent / STAGE_ROOT / destination.name
        digest = tree_digest(local_dir)
        code, out, _err = self.execute(
            host, f'cat {shlex.quote(str(marker))} 2>/dev/null || true')
        if code == 0 and out.decode('utf-8', 'replace').strip() == digest:
            return (0, b'', b'')
        key = self.staging_key(destination.name, digest)
        self._stage_archive(key, local_dir, digest)
        staged = self.store.head(key)
        operation_id = operation_id or self.next_operation()
        blob = {'object': self.transfer_key(index, operation_id, 'stdin'),
                'sha256': (staged.get('metadata') or {}).get('sha256'),
                'size': int(staged.get('size', -1))}
        if not isinstance(blob['sha256'], str) or blob['size'] < 0:
            raise MailboxError(f'{key}: staged tree receipt is incomplete')
        copied = self.store.copy(key, blob['object'], if_generation_match=0)
        if str(copied.get('size', -1)) != str(blob['size']):
            raise MailboxError(f'{blob["object"]}: staged copy size mismatch')
        reference = self.mailbox.submit(
            index, [SHELL, '-c',
                    f'mkdir -p {shlex.quote(str(destination))} {shlex.quote(str(marker.parent))} && '
                    f'tar -xzf - -C {shlex.quote(str(destination))} && '
                    f'printf %s {digest} > {shlex.quote(str(marker))}'],
            operation_id=operation_id, timeout_seconds=self.command_timeout(None),
            stdin=blob, stdout_object=self.transfer_key(index, operation_id, 'stdout'))
        result = self.finish(host, reference)
        self._discard_transfer(blob['object'])
        return result

    def run_driver(self, host, script, argv=(), *, timeout_seconds=None, operation_id=None):
        """Run one admitted orchestrator script on the coordinator host.

        `manual_sui.py`, `stage3_campaign.py` and `fault_cells.py` are written
        for the coordinator host: their `run.py`, arm A and fault coordinator
        are local children that must reach the owners, so the whole orchestrator
        runs there as one bounded request. Its stdout spills to the bucket.
        """
        if not isinstance(script, str) or not script.startswith('/'):
            raise ValueError('an absolute admitted script is required')
        command = 'exec ' + ' '.join(
            [shlex.quote(gcp_runtime.GUEST_PYTHON), shlex.quote(script)]
            + [shlex.quote(str(item)) for item in argv])
        code, out, err = self.finish(host, self.submit(
            host, command, operation_id=operation_id, timeout_seconds=timeout_seconds))
        return code, out, err

    # ---- boot ----------------------------------------------------------
    def wait_workers(self, timeout_seconds=READY_SECONDS, *, require=None):
        """Wait for every guest's worker-ready object and return its receipts."""
        end = min(self.deadline, time.time() + timeout_seconds)
        ready = {}
        while len(ready) < len(self.fleet):
            for host in self.fleet:
                if host['name'] in ready:
                    continue
                key = f'runs/{self.run_id}/qualification/{host["name"]}/worker-ready.json'
                try:
                    meta = self.store.head(key)
                    receipt = json.loads(self.store.get(key, generation=meta['generation'],
                                                       max_bytes=1024 * 1024))
                except Exception as exc:
                    if getattr(exc, 'status', None) != 404:
                        raise
                    continue
                if (not isinstance(receipt, dict) or receipt.get('complete') is not True
                        or receipt.get('run_id') != self.run_id
                        or receipt.get('source_sha256') != self.source_sha256
                        or receipt.get('deadline_utc') != self.deadline_utc
                        or receipt.get('instance_id') != str(host['id'])
                        or receipt.get('instance_name') != host['name']):
                    raise MailboxError(f'{host["name"]}: worker readiness identity mismatch')
                ready[host['name']] = receipt
            if len(ready) == len(self.fleet):
                break
            if time.time() >= end:
                missing = sorted(host['name'] for host in self.fleet if host['name'] not in ready)
                raise MailboxTimeout('worker readiness missing on ' + ', '.join(missing))
            time.sleep(self.poll_seconds)
        if require is not None:
            for name, receipt in ready.items():
                if receipt.get('manifest_sha256') != require:
                    raise MailboxError(f'{name}: worker manifest differs from the sealed package')
        return ready

    @classmethod
    def from_state(cls, path, api, *, bucket=None, timeout_seconds=None):
        """Build the executor from a `gcp_manual_compute.py` fleet state file."""
        state = json.loads(Path(path).read_text())
        fleet = state.get('fleet') or [record['fleet'] for record in state.get('instances', [])
                                       if record.get('fleet')]
        if len(fleet) != state.get('host_count'):
            raise ValueError('fleet state does not describe every host')
        bucket = bucket or state.get('bucket')
        if not isinstance(bucket, str) or not bucket:
            raise ValueError('fleet state names no artifact bucket')
        return cls(api, bucket=bucket, run_id=state['run_id'], fleet=fleet,
                   source_sha256=state['source_sha256'], deadline_utc=state['deadline'],
                   timeout_seconds=timeout_seconds)


def publish(store, config, state, package_dir, revision, spec, fleet, *, guest_roots=None):
    """Stage the sealed package and the runtime the guests' bootstrap waits for.

    The Sui campaign's package is the focused package plus the Sui archives, so
    it is admitted under the focused profile while the runtime carries the Sui
    specification. Both objects land where `gcp_bootstrap.startup_script` waits
    for them.
    """
    admission = dict(spec, mode='focused') if spec.get('suite') == 'sui' else spec
    receipt, _source = manual_campaign._admit_package(Path(package_dir).resolve(strict=True),
                                                      revision, admission)
    if admission.get('suite') == 'sui':
        admit_nvmet_fix(receipt)
    prefix = 'runs/' + state['run_id'] + '/'
    key = prefix + 'package.tar'
    package = dict(receipt, object_key=key)
    package['generation'] = str(upload_identical(store, key, receipt['archive_path'],
                                                 receipt['sha256'])['generation'])
    replace_object(store, prefix + 'package.json', gcp_runtime._encode(package))
    runtime = gcp_runtime.build_runtime_config(
        dict(config, run_id=state['run_id']), fleet, package, spec, state['deadline'],
        run_id=state['run_id'], guest_roots=guest_roots)
    # A re-create under the same run ID may carry a different source; the
    # runtime must then replace the previous copy, and identical bytes reuse it.
    meta = replace_object(store, prefix + 'runtime.json', gcp_runtime._encode(runtime))
    return runtime, {'object': prefix + 'runtime.json',
                     'generation': str(meta['generation'])}


def release(store, state, host, script):
    """Release one guest's bootstrap gate with its exact immutable script."""
    if not isinstance(script, str) or not script:
        raise ValueError('a startup script is required')
    gate = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
            'instance_id': str(host['id']), 'deadline_utc': state['deadline'],
            'script': script, 'sha256': hashlib.sha256(script.encode()).hexdigest()}
    replace_object(store, 'runs/' + state['run_id'] + '/qualification/' + host['name']
                   + '/gate.json', gcp_runtime._encode(gate))
    return gate


def gate_script(bucket, state):
    """The identity-bound startup script a fleet boots with before admission."""
    return manual_campaign._gate(bucket, state)


def with_flags(argv, extra):
    """Add driver flags ahead of a `--` passthrough, never behind it."""
    result = list(argv)
    if '--' in result:
        index = result.index('--')
        return result[:index] + list(extra) + result[index:]
    return result + list(extra)


def coordinator_of(state_path):
    """The coordinator host name of one recorded fleet (index 0)."""
    state = json.loads(Path(state_path).read_text())
    fleet = state.get('fleet') or [record['fleet'] for record in state.get('instances', [])
                                   if record.get('fleet')]
    if not fleet or fleet[0].get('index') != 0:
        raise ValueError('fleet state names no coordinator host')
    return fleet[0]['name']


def dispatch(args, *, coordinator, script_relative, trees=(), files=(), values=None):
    """Stage this driver's inputs and run it on the coordinator host.

    `trees` and `files` are `(guest name, local path)` pairs staged under
    `runs/<run>` inside the guest root; `values` maps a flag to the guest name
    its value becomes. The coordinator runs the admitted package copy of this
    script, with the guest metadata identity, so it writes the owner requests
    itself.
    """
    remote = open_executor(args)
    state_path = Path(args.fleet_state)
    state = json.loads(state_path.read_text())
    campaign = Path('/mnt/tidehunter') / state['run_id'] / 'campaign'
    for name, local in trees:
        remote.stage_tree(coordinator, local, campaign / name)
    for name, local in files:
        remote.send_file(coordinator, campaign / name, local)
    remote.send_file(coordinator, campaign / 'fleet-state.json', state_path)
    remote.send_file(coordinator, campaign / 'config.json', Path(args.fleet_config).resolve())
    argv = rewrite([item for item in sys.argv[1:] if item != '--on-coordinator'],
                   {flag: str(campaign / name) for flag, name in (values or {}).items()})
    argv = with_flags(argv, ['--on-coordinator', '--guest-token',
                            '--fleet-state', str(campaign / 'fleet-state.json'),
                            '--fleet-config', str(campaign / 'config.json')])
    script = f'/mnt/tidehunter/{state["run_id"]}/package/source/{script_relative}'
    code, out, err = remote.run_driver(coordinator, script, argv,
                                       timeout_seconds=getattr(args, 'coordinator_seconds', None))
    sys.stdout.write(out.decode('utf-8', 'replace'))
    sys.stderr.write(err.decode('utf-8', 'replace'))
    return code


def rewrite(argv, values):
    """Replace the values of named flags in an existing argument vector.

    A flag absent from `argv` keeps its own default on the guest, so a caller
    maps only the paths that exist on the workstation.
    """
    result = [str(item) for item in argv]
    for index, item in enumerate(result[:-1]):
        if item == '--':
            break
        if item in values:
            result[index + 1] = str(values[item])
    return result


def load_config(path=None):
    """The campaign configuration that names the project and the bucket."""
    return json.loads(Path(path or Path(__file__).with_name('config.json')).read_text())


def open_executor(args):
    """The one constructor swap the drivers make: mailbox, or SSH for a rehearsal.

    The caller supplies `args.gcloud_ssh`, `args.zone`, `args.fleet_state`,
    `args.fleet_config` and `args.guest_token`; `args.iap` and
    `args.tunnel_through_iap` are accepted because the two SSH flags have
    different names in the existing drivers. `--guest-token` selects the guest
    metadata identity, which is what an orchestrator running on the coordinator
    uses to write requests for the owners.
    """
    if args.gcloud_ssh:
        if not args.zone:
            raise SystemExit('--gcloud-ssh needs --zone')
        return host_exec.GcloudSsh(args.zone, iap=getattr(args, 'tunnel_through_iap', False)
                                   or getattr(args, 'iap', False))
    if args.fleet_state is None:
        raise SystemExit('a campaign run needs --fleet-state, or --gcloud-ssh for a rehearsal')
    config = load_config(args.fleet_config)
    guest = bool(getattr(args, 'guest_token', False))
    if not guest and gcp_api.rehearsal_endpoint() is None:
        # Every advertised stage is paid. Refuse before the first request when
        # the workstation or the delegated bench-guest identity cannot mint.
        receipt = gcp_api.require_paid_credentials(
            config, Path(sys.argv[0]).stem + '-dispatch')
        gcp_api.record_credential_receipt(
            Path(args.fleet_state).with_name('credentials-gate.json'), receipt)
    # The workstation cannot read the campaign bucket as its own user. Every
    # laptop-side client delegates to the campaign service account; the
    # coordinator uses its own metadata identity.
    api = gcp_api.campaign_client(config, guest=guest)
    return MailboxExec.from_state(args.fleet_state, api, bucket=config['bucket'])


__all__ = ['MailboxError', 'MailboxTimeout', 'MailboxRefused', 'MailboxUnknownHost',
           'MailboxExec', 'sui_spec', 'publish', 'release', 'gate_script', 'load_config',
           'open_executor', 'rewrite', 'with_flags', 'coordinator_of', 'dispatch',
           'tree_digest', 'admit_nvmet_fix', 'admit_package_nvmet_fix']
