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
"""
import hashlib
import json
from pathlib import Path
import re
import shlex
import time

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


class MailboxError(host_exec.HostCommandError):
    """A mailbox operation produced no verified result."""


class MailboxTimeout(MailboxError):
    """No terminal receipt before the deadline; the command outcome is unknown."""


class MailboxRefused(MailboxError):
    """The guest refused the request before running it; nothing took effect."""


class MailboxUnknownHost(MailboxError):
    """The host is not one of this fleet's admitted guests."""


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
            for value in (host['name'], host['internal_ip']):
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
        return f'cmd-{self.operations:06d}-{hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:12]}'

    def index_of(self, host):
        if not isinstance(host, str) or host not in self.hosts:
            raise MailboxUnknownHost(f'{host}: not a guest of this campaign fleet')
        return self.hosts[host]

    def transfer_key(self, index, operation_id, leaf):
        return f'runs/{self.run_id}/transfer/{self.fleet[index]["id"]}/{operation_id}/{leaf}'

    def submit(self, host, command, *, operation_id=None, stdin=None, timeout_seconds=None):
        """Publish one immutable request and return its reference."""
        index = self.index_of(host)
        if (not isinstance(command, str) or not command or '\x00' in command
                or len(command.encode('utf-8', 'surrogatepass')) > MAX_SCRIPT):
            raise ValueError('invalid campaign command')
        operation_id = operation_id or self.next_operation()
        timeout = self.command_timeout(timeout_seconds)
        blob = None
        if stdin is not None:
            data = bytes(stdin)
            if len(data) > MAX_BLOB:
                raise ValueError('command stdin exceeds the mailbox bound')
            key = self.transfer_key(index, operation_id, 'stdin')
            self.put_blob(key, data)
            blob = {'object': key, 'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
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
        return cls(api, bucket=bucket or state['bucket'], run_id=state['run_id'], fleet=fleet,
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
    package, _preparation = manual_campaign._stage(store, config['bucket'], state, receipt)
    runtime = gcp_runtime.build_runtime_config(
        dict(config, run_id=state['run_id']), fleet, package, spec, state['deadline'],
        run_id=state['run_id'], guest_roots=guest_roots)
    # _put_once makes a retried publish compare bytes instead of failing 412.
    meta = gcp_runtime._put_once(store, 'runs/' + state['run_id'] + '/runtime.json',
                                 gcp_runtime._encode(runtime))
    return runtime, {'object': 'runs/' + state['run_id'] + '/runtime.json',
                     'generation': str(meta['generation'])}


def release(store, state, host, script):
    """Release one guest's bootstrap gate with its exact immutable script."""
    if not isinstance(script, str) or not script:
        raise ValueError('a startup script is required')
    gate = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
            'instance_id': str(host['id']), 'deadline_utc': state['deadline'],
            'script': script, 'sha256': hashlib.sha256(script.encode()).hexdigest()}
    gcp_runtime._put_once(
        store, 'runs/' + state['run_id'] + '/qualification/' + host['name'] + '/gate.json',
        gcp_runtime._encode(gate))
    return gate


def gate_script(bucket, state):
    """The identity-bound startup script a fleet boots with before admission."""
    return manual_campaign._gate(bucket, state)


def load_config(path=None):
    """The campaign configuration that names the project and the bucket."""
    return json.loads(Path(path or Path(__file__).with_name('config.json')).read_text())


def open_executor(args):
    """The one constructor swap the drivers make: mailbox, or SSH for a rehearsal.

    The caller supplies `args.gcloud_ssh`, `args.zone`, `args.fleet_state` and
    `args.fleet_config`; `args.iap` and `args.tunnel_through_iap` are accepted
    because the two SSH flags have different names in the existing drivers.
    """
    if args.gcloud_ssh:
        if not args.zone:
            raise SystemExit('--gcloud-ssh needs --zone')
        return host_exec.GcloudSsh(args.zone, iap=getattr(args, 'tunnel_through_iap', False)
                                   or getattr(args, 'iap', False))
    if args.fleet_state is None:
        raise SystemExit('a campaign run needs --fleet-state, or --gcloud-ssh for a rehearsal')
    config = load_config(args.fleet_config)
    api = gcp_api.GoogleApi(config['project_id'])
    return MailboxExec.from_state(args.fleet_state, api, bucket=config['bucket'])


__all__ = ['MailboxError', 'MailboxTimeout', 'MailboxRefused', 'MailboxUnknownHost',
           'MailboxExec', 'sui_spec', 'publish', 'release', 'gate_script', 'load_config',
           'open_executor']
