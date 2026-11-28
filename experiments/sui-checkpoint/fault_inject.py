#!/usr/bin/env python3
"""W1 controller-loss injector for stage S1-4/M7.

Deletes one verified host NVMe-oF controller with sysfs `delete_controller`
while the owner process stays alive. Then it proves that checkpoint writes
continue, that a previously read checkpoint summary reads back byte-identical,
and that the aggregate live counters advanced. The injector never disconnects
globally and never signals an owner process.

It runs on the initiator, because the controller object being deleted is the
local one: the coordinator host holds one controller per owner target. The
active write controller is named either exactly (`--nqn`) or by the
coordinator's newest connection intent inside the owner's live service window
(`--intents`); the service window alone is never enough, because several
databases share it. The engine publishes one registry per database under the
coordinator root (`<namespace>/live-nvme-connections`), so `--intents` names
that root and every registry beneath it is searched. Before the delete the
injector proves the selected controller is the one serving writes: it is
`live`, a process holds a descriptor on its namespace, and that namespace's
write sectors advance. After the delete a new controller inode for the same
NQN and a completed write-resume episode are required when a metrics endpoint
is given; the resume counters are polled until they advance or `--timeout`
expires, because the episode follows the write path's own break.

Exit status is nonzero when any verification fails; the JSON record is written
in both outcomes so the campaign retains the failed case.
"""
import argparse
import copy
import hashlib
import importlib.util
import ipaddress
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True
import checkpoint_support as checkpoint

ENGINE = Path(__file__).resolve().parents[2]
NVMEOF_MANAGE = ENGINE / 'experiments/nvmeof/manage.py'


def _load_manage():
    spec = importlib.util.spec_from_file_location('campaign_nvmeof_manage', NVMEOF_MANAGE)
    if spec is None or spec.loader is None:
        raise ValueError('nvmeof manage helper is absent')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


manage = _load_manage()

# The W1 episode that follows a loss starts on the write path's own failure, not
# on the new controller: a scrape taken right after the reconnect can still read
# zero. The wait is an evidence poll, never a fixed sleep, and its bound is the
# injector's own `--timeout`.
RESUME_POLL_CADENCE = 0.05
RESUME_POLL_OBSERVATIONS = 4


class InjectionError(ValueError):
    """A precondition or verification failed; evidence is still retained."""


def validate_spec(args):
    if args.nqn is not None:
        if not isinstance(args.nqn, str) or not manage.NQN.fullmatch(args.nqn):
            raise ValueError('invalid target NQN')
    elif not (type(args.service_max) is int and args.service <= args.service_max <= 65535):
        raise ValueError('an intent target needs a bounded service window')
    if args.nqn is None and not args.intents:
        raise ValueError('an exact NQN (--nqn) or the coordinator database root '
                         '(--intents) is required: one service window holds every database')
    if args.transport not in ('tcp', 'rdma'):
        raise ValueError('transport must be tcp or rdma')
    if not 1 <= args.service <= 65535:
        raise ValueError('service out of range')
    if args.timeout <= 0:
        raise ValueError('timeout must be positive')
    if args.preflight_timeout <= 0:
        raise ValueError('preflight timeout must be positive')
    if any(pid <= 0 for pid in args.owner_pid):
        raise ValueError('owner PIDs must be positive')


def select_controller(inventory, *, nqn, transport, address, service):
    """Select exactly one matching controller; ambiguity fails closed."""
    matches = [(name, identity) for name, identity in inventory.items()
               if identity['nqn'] == nqn and identity['transport'] == transport
               and manage.controller_matches(identity, nqn, transport, address, service)]
    if len(matches) != 1:
        raise ValueError(f'expected one matching controller, found {len(matches)}')
    return matches[0]


INTENT_DIRECTORY = 'live-nvme-connections'


def intent_directories(root):
    """Every coordinator live-connection registry under one database root.

    The engine opens one database per namespace and publishes each database's
    intent in its own `<namespace>/live-nvme-connections` directory: the Sui
    replay registers `live/checkpoints`, `live/store/epoch_0` and
    `live/store/perpetual` under the single `--db-root` the campaign prepares,
    and the epoch directory rolls during the replay. A path that is itself a
    registry, or one that names intent files directly, is used as given so an
    operator or a rehearsal can still point at exactly one directory.
    """
    root = Path(root)
    if not root.is_dir():
        raise ValueError(f'connection intent root is absent: {root}')
    if root.name == INTENT_DIRECTORY or any(root.glob('*.json')):
        return [root]
    found = sorted(path for path in root.rglob(INTENT_DIRECTORY) if path.is_dir())
    if not found:
        raise ValueError(f'no {INTENT_DIRECTORY} registry under the coordinator '
                         f'database root: {root}')
    return found


def newest_write_intent(database_root, *, transport, address, service, service_max):
    """Select the coordinator's newest connection intent of one owner window.

    The engine publishes one `live-nvme-connections/<nonce>.json` intent per
    controller it connects, and deletes it when that controller is retired, so
    the newest intent inside the owner's service window is the controller the
    write path is using. Every database of the coordinator publishes into its
    own registry, and the owner window covers all of them, so the search spans
    all registries beneath the database root. Filenames are random nonces:
    recency is the file's modification time, and the whole candidate list is
    retained as evidence.
    """
    if not (type(service) is int and type(service_max) is int and 1 <= service <= service_max <= 65535):
        raise ValueError('invalid service window')
    directories = intent_directories(database_root)
    matches = []
    for directory in directories:
        for path in sorted(directory.glob('*.json')):
            try:
                intent = json.loads(path.read_text())
            except (OSError, ValueError) as error:
                raise ValueError(f'unreadable connection intent {path.name}: {error}') from error
            target = intent.get('target') if isinstance(intent, dict) else None
            if not isinstance(target, dict):
                raise ValueError(f'connection intent {path.name} carries no target')
            if target.get('transport') != transport or target.get('address') != address:
                continue
            port = target.get('service')
            if not (isinstance(port, int) and service <= port <= service_max):
                continue
            nqn_value = target.get('nqn')
            if not (isinstance(nqn_value, str) and manage.NQN.fullmatch(nqn_value)):
                raise ValueError(f'connection intent {path.name} names no valid NQN')
            matches.append((path.stat().st_mtime_ns, path.name, nqn_value))
    if not matches:
        raise ValueError('no connection intent inside the owner service window')
    matches.sort()
    return matches[-1][2], [row[2] for row in matches], [str(path) for path in directories]


NAMESPACE = re.compile(r'nvme[0-9]+n[0-9]+')
# `/sys/block/<ns>/stat` fields 0-3 are reads, 4-7 writes; field 6 is the
# sectors written count.
SECTORS_WRITTEN_FIELD = 6


def controller_namespaces(controller, *, nvme_root=None, block_root=None):
    """The namespace block devices one host controller serves, from sysfs.

    A fabrics controller's namespace device links to that controller; a native
    multipath group links to its subsystem, which is accepted only when its
    `subsysnqn` equals the controller's.
    """
    nvme_root = Path(nvme_root or manage.SYS_NVME)
    block_root = Path(block_root or manage.SYS_BLOCK)
    root = nvme_root / controller
    controller_path = root.resolve(strict=True)
    subsysnqn = (root / 'subsysnqn').read_text().strip()
    if not subsysnqn:
        raise ValueError('controller has an empty subsystem NQN')
    names = []
    for entry in sorted(block_root.iterdir()):
        if not NAMESPACE.fullmatch(entry.name):
            continue
        try:
            device = (entry / 'device').resolve(strict=True)
        except OSError:
            continue
        if device == controller_path:
            names.append(entry.name)
            continue
        if device.name.startswith('nvme-subsys'):
            try:
                if (device / 'subsysnqn').read_text().strip() == subsysnqn:
                    names.append(entry.name)
            except OSError:
                continue
    return names


def namespace_holders(devices, *, proc_root=Path('/proc'), dev_root=Path('/dev')):
    """Processes holding an open descriptor on one of `devices`."""
    targets = {str(dev_root / name) for name in devices}
    holders = []
    for process in sorted(proc_root.iterdir()):
        if not process.name.isdigit():
            continue
        try:
            descriptors = list((process / 'fd').iterdir())
        except OSError:
            continue
        open_fds = 0
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except OSError:
                continue
            if target in targets:
                open_fds += 1
        if open_fds:
            holders.append(dict(pid=int(process.name), descriptors=open_fds))
    return holders


def sectors_written(device, *, block_root=None):
    """The sectors written counter of one namespace block device."""
    block_root = Path(block_root or manage.SYS_BLOCK)
    fields = (block_root / device / 'stat').read_text().split()
    if len(fields) <= SECTORS_WRITTEN_FIELD:
        raise ValueError(f'namespace {device} stat has no sectors written field')
    return int(fields[SECTORS_WRITTEN_FIELD])


def wait_sectors_advance(devices, *, timeout, block_root=None,
                         clock=time.monotonic, sleep=time.sleep):
    """Wait until one namespace's write sectors advance; bounded by `timeout`.

    Returns `(advanced, rows)` where each row keeps the first and last observed
    counter of one device, so the evidence names the write path that was live
    when the controller was deleted.
    """
    before = {device: sectors_written(device, block_root=block_root) for device in devices}
    start = clock()
    rows = []
    while True:
        now = {device: sectors_written(device, block_root=block_root) for device in devices}
        advanced = any(now[device] > before[device] for device in devices)
        rows.append(dict(seconds=clock() - start,
                         sectors={device: dict(before=before[device], now=now[device])
                                  for device in devices}))
        if advanced:
            return True, rows
        if clock() - start >= timeout:
            return False, rows
        sleep(min(.05, max(0, timeout - (clock() - start))))


def hostnqn(controller):
    value = (manage.SYS_NVME / controller / 'hostnqn').read_text().strip()
    if not value:
        raise ValueError('controller host NQN is empty')
    return value


def controller_state(controller):
    """The controller's kernel state, or None when its sysfs entry vanished."""
    try:
        return (manage.SYS_NVME / controller / 'state').read_text().strip()
    except FileNotFoundError:
        return None


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def read_checkpoint(address, sequence):
    return checkpoint.rpc(address, 'sui_getCheckpoint', [str(sequence)])


def latest_checkpoint(address):
    return int(checkpoint.rpc(address, 'sui_getLatestCheckpointSequenceNumber'))


def scrape_metrics(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read(32 << 20).decode()


def live_counters(text):
    from analyze import live_nvmeof_stats, parse_metrics
    return live_nvmeof_stats(parse_metrics(text))


def matching_controllers(inventory, *, nqn, transport, address, service):
    return [identity for identity in inventory.values()
            if identity['nqn'] == nqn and identity['transport'] == transport
            and manage.controller_matches(identity, nqn, transport, address, service)]


def wait_for_reconnect(*, nqn, transport, address, service, removed_inode, timeout, snapshot=None, clock=time.monotonic, sleep=time.sleep):
    """Accept a new inode or a matching controller after observed absence.

    sysfs inode numbers can be reused. A different inode proves a new object;
    otherwise the identity must disappear completely and then reappear.
    """
    snapshot = snapshot or manage.controller_snapshot
    start = clock()
    observations = []
    absent = False
    while True:
        inventory = snapshot()
        matches = matching_controllers(inventory, nqn=nqn, transport=transport,
                                       address=address, service=service)
        fresh = any(identity['inode'] != removed_inode for identity in matches)
        present = fresh or (absent and bool(matches))
        if not matches:
            absent = True
        observations.append(dict(seconds=clock() - start, controllers=sorted(inventory),
                                 matching=len(matches), present=present))
        if present:
            return clock() - start, observations
        if clock() - start >= timeout:
            raise InjectionError('owner controller did not reconnect before the deadline')
        sleep(min(.05, max(0, timeout - (clock() - start))))


def wait_for_progress(address, before, *, timeout, clock=time.monotonic, sleep=time.sleep, rpc=latest_checkpoint):
    start = clock()
    observations = []
    while True:
        current = rpc(address)
        observations.append(dict(seconds=clock() - start, checkpoint=current))
        if current > before:
            return current, clock() - start, observations
        if clock() - start >= timeout:
            raise InjectionError('checkpoint writes did not advance after reconnect')
        sleep(min(.25, max(0, timeout - (clock() - start))))


def wait_for_resume(url, before, *, timeout, metrics=scrape_metrics, clock=time.monotonic,
                    sleep=time.sleep, cadence=RESUME_POLL_CADENCE):
    """Poll the live counters until the W1 episode that follows the loss appears.

    A one-shot scrape right after the reconnect races the engine's resume: the
    episode starts when the leased write path observes its own break, not when
    the controller reappears, so the first sample can still read zero. The wait
    is bounded by the injector's existing `--timeout`, and the sample it stops
    on is the record's after pair either way. `exhausted` says whether the
    counters advanced before that bound; it is evidence, not an extra verdict.
    """
    counters = before.get('counters', {})
    base_attempts = counters.get('resume_attempts_total') or 0
    base_frames = counters.get('resume_frames_total') or 0
    start = clock()
    samples = 0
    observations = []
    while True:
        sample = live_counters(metrics(url))
        counters = sample.get('counters', {})
        attempts = counters.get('resume_attempts_total') or 0
        frames = counters.get('resume_frames_total') or 0
        samples += 1
        observations.append(dict(seconds=clock() - start, resume_attempts=attempts,
                                 resume_frames=frames))
        del observations[:-RESUME_POLL_OBSERVATIONS]
        advanced = attempts > base_attempts and frames > base_frames
        if advanced or clock() - start >= timeout:
            return sample, dict(samples=samples, elapsed_seconds=clock() - start,
                                exhausted=not advanced, observations=observations)
        sleep(min(cadence, max(0, timeout - (clock() - start))))


def process_state(pids):
    rows = []
    for pid in pids:
        try:
            status = Path(f'/proc/{pid}/status').read_text().splitlines()
            state = next((line.split()[1] for line in status if line.startswith('State:')), None)
            rows.append(dict(pid=pid, alive=True, state=state))
        except (FileNotFoundError, ProcessLookupError):
            rows.append(dict(pid=pid, alive=False, state=None))
    return rows


def run(args, *, snapshot=None, delete=None,
        latest=latest_checkpoint, metrics=scrape_metrics, clock=time.monotonic,
        sleep=time.sleep, read_checkpoint=read_checkpoint,
        namespaces=None, holders=None, sectors=None, write=None):
    """Inject one controller loss and return the retained record.

    `write` persists the record at each milestone (target selected, preflight
    passed, delete attempted, delete returned, reconnect seen). A run that dies
    on an RPC after the delete therefore still leaves the evidence that the
    delete happened; without it a fallback record cannot say whether anything
    was injected (`main`).
    """
    snapshot = snapshot or manage.controller_snapshot
    delete = delete or manage.delete_controller
    namespaces = namespaces or controller_namespaces
    holders = holders or namespace_holders
    sectors = sectors or wait_sectors_advance
    def persist():
        # A copy per milestone: the caller keeps what was true at that point, not
        # a live dict the next milestone mutates.
        write(copy.deepcopy(record))

    write = write or (lambda record: None)
    validate_spec(args)
    address = str(ipaddress.ip_address(args.address))
    record = dict(schema=1, kind='w1_controller_loss', injection='sysfs delete_controller',
        created_unix_ns=time.time_ns(), target=dict(nqn=args.nqn, transport=args.transport,
            address=address, service=args.service), ok=False, reason=None, deleted=False)
    record['processes_before'] = process_state(args.owner_pid)
    if any(not row['alive'] for row in record['processes_before']):
        record['reason'] = 'an owned process was not alive before injection'
        return record
    inventory = snapshot()
    candidates = None
    selected_nqn = args.nqn
    if selected_nqn is None:
        selected_nqn, candidates, directories = newest_write_intent(
            args.intents, transport=args.transport, address=address,
            service=args.service, service_max=args.service_max)
        record['target']['intents'] = str(args.intents)
        record['target']['intent_directories'] = directories
        record['target']['candidates'] = candidates
    controller, identity = select_controller(inventory, nqn=selected_nqn,
        transport=args.transport, address=address, service=args.service)
    deleted_nqn = identity['nqn']
    deleted_hostnqn = hostnqn(controller)
    record['target'].update(controller=controller, inode=identity['inode'],
        hostnqn=deleted_hostnqn, nqn=deleted_nqn, transport=args.transport,
        address=address, service=args.service, service_max=args.service_max)
    persist()
    # The deleted controller must be the one serving the coordinator's writes:
    # it is live, a process holds its namespace open, and the namespace's write
    # sectors advance. A stale read-only epoch fails these before any delete.
    try:
        state = controller_state(controller)
        devices = namespaces(controller)
        holding = holders(devices) if devices else []
        advanced, sector_rows = (
            sectors(devices, timeout=args.preflight_timeout) if devices else (False, []))
    except (OSError, ValueError) as error:
        record['reason'] = 'controller write-path preflight failed: ' + str(error)
        record['finished_unix_ns'] = time.time_ns()
        return record
    record['preflight'] = dict(state=state, devices=devices, holders=holding,
        sectors_advanced=advanced, sectors=sector_rows)
    if state != 'live':
        record['reason'] = f'the selected controller is not live: {state}'
        record['finished_unix_ns'] = time.time_ns()
        return record
    if not devices:
        record['reason'] = 'the selected controller serves no namespace block device'
        record['finished_unix_ns'] = time.time_ns()
        return record
    if not holding:
        record['reason'] = 'no process holds a descriptor on the selected controller namespace'
        record['finished_unix_ns'] = time.time_ns()
        return record
    if not advanced:
        record['reason'] = ('the selected controller namespace write sectors did not advance '
                            f'in {args.preflight_timeout} s')
        record['finished_unix_ns'] = time.time_ns()
        return record
    persist()
    before_sequence = latest(args.sui_rpc)
    if before_sequence < 0:
        raise ValueError('negative checkpoint sequence')
    if before_sequence == 0:
        record['reason'] = 'no completed checkpoint before injection'
        return record
    before_checkpoint = read_checkpoint(args.sui_rpc, before_sequence)
    record['checkpoint_before'] = before_sequence
    record['checkpoint_before_sha256'] = hashlib.sha256(
        canonical(before_checkpoint).encode()).hexdigest()
    before_metrics = live_counters(metrics(args.metrics_url)) if args.metrics_url else None
    if before_metrics is not None:
        record['live_before'] = before_metrics
    started = clock()
    record['delete_attempted_unix_ns'] = time.time_ns()
    persist()
    try:
        delete(controller, identity, {'hostnqn': deleted_hostnqn})
    except (OSError, ValueError) as error:
        record['reason'] = 'controller delete was refused or failed: ' + str(error)
        record['deleted'] = False
        record['finished_unix_ns'] = time.time_ns()
        return record
    record['deleted'] = True
    record['deleted_unix_ns'] = time.time_ns()
    persist()
    record['outage_started_seconds'] = started
    try:
        reconnect, reconnect_observations = wait_for_reconnect(nqn=deleted_nqn,
            transport=args.transport, address=address, service=args.service,
            removed_inode=identity['inode'], timeout=args.timeout,
            snapshot=snapshot, clock=clock, sleep=sleep)
        record['reconnect_seconds'] = reconnect
        record['reconnect_observations'] = reconnect_observations
        record['reconnected'] = True
        record['reconnected_unix_ns'] = time.time_ns()
        persist()
        after_sequence, progress, progress_observations = wait_for_progress(
            args.sui_rpc, before_sequence, timeout=args.timeout, clock=clock,
            sleep=sleep, rpc=lambda address: latest(address))
        record['checkpoint_after'] = after_sequence
        record['write_progress_seconds'] = progress
        record['progress_observations'] = progress_observations
        readback = read_checkpoint(args.sui_rpc, before_sequence)
        record['readback_equal'] = canonical(readback) == canonical(before_checkpoint)
        record['readback_sha256'] = hashlib.sha256(
            canonical(readback).encode()).hexdigest()
        new_checkpoint = read_checkpoint(args.sui_rpc, after_sequence)
        record['new_checkpoint_sequence'] = after_sequence
        record['new_checkpoint_read'] = bool(canonical(new_checkpoint))
        if args.metrics_url and before_metrics is not None:
            after_metrics, record['resume_poll'] = wait_for_resume(
                args.metrics_url, before_metrics, timeout=args.timeout,
                metrics=metrics, clock=clock, sleep=sleep)
        else:
            after_metrics = live_counters(metrics(args.metrics_url)) if args.metrics_url else None
        if before_metrics is not None and after_metrics is not None:
            from analyze import live_nvmeof_deltas
            record['live_after'] = after_metrics
            record['live_delta'] = live_nvmeof_deltas(
                {f'tidehunter_nvmeof_live_{k}': v for k, v in before_metrics['counters'].items()},
                {f'tidehunter_nvmeof_live_{k}': v for k, v in after_metrics['counters'].items()})
        # The reconnect a controller loss requires is a write resume, not just a
        # new connection: the engine must have rewritten the unadopted suffix.
        delta = record.get('live_delta') or {}
        record['resume'] = {name: delta.get(name) for name in (
            'resume_attempts', 'resume_rewrites', 'resume_frames', 'resume_refused')}
        new_name, new_identity = select_controller(snapshot(), nqn=deleted_nqn,
            transport=args.transport, address=address, service=args.service)
        record['new_controller'] = dict(controller=new_name, inode=new_identity['inode'])
        record['processes_after'] = process_state(args.owner_pid)
        failures = []
        if new_identity['inode'] == identity['inode']:
            failures.append('the controller inode did not change after the loss')
        if not record['readback_equal']:
            failures.append('checkpoint summary readback differs')
        if any(not row['alive'] for row in record['processes_after']):
            failures.append('an owned process fail-stopped')
        if args.metrics_url and record.get('live_delta', {}).get('available') is False:
            failures.append('aggregate live counters are not a usable before/after pair')
        if args.metrics_url and record.get('live_delta', {}).get('available') and (
                record['live_delta'].get('errors') or 0) > 0:
            failures.append('live data-plane errors increased during the injection')
        if args.metrics_url and delta.get('available'):
            if (delta.get('resume_attempts') or 0) < 1:
                failures.append('no write-resume episode followed the controller loss')
            if (delta.get('resume_frames') or 0) < 1:
                failures.append('no frame was rewritten after the controller loss')
            if (delta.get('namespace_connects') or 0) < 1:
                failures.append('no new verified controller connection followed the loss')
        record['fail_stop_count'] = sum(1 for row in record['processes_after'] if not row['alive'])
        record['duplicate_count'] = None
        record['duplicate_observation'] = 'unavailable: no host-side duplicate counter; engine reconciliation is inspected from its own logs'
        record['reason'] = '; '.join(failures) if failures else None
        record['ok'] = not failures
    except InjectionError as error:
        record['reason'] = str(error)
        record['processes_after'] = process_state(args.owner_pid)
        record['fail_stop_count'] = sum(1 for row in record['processes_after'] if not row['alive'])
        record['duplicate_count'] = None
        record['duplicate_observation'] = 'unavailable: no host-side duplicate counter; engine reconciliation is inspected from its own logs'
        record['ok'] = False
    finally:
        record['finished_unix_ns'] = time.time_ns()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nqn', help='exact owner target NQN to lose')
    parser.add_argument('--intents',
                        help="the coordinator's database root, the prepared "
                             '`--db-root`; its registries under '
                             '`<namespace>/live-nvme-connections` are searched and the '
                             'newest intent inside the owner service window names the '
                             'active write controller. One exact registry directory is '
                             'also accepted')
    parser.add_argument('--service-max', type=int,
                        help='upper bound of the owner target service window; '
                             'required when --nqn is absent')
    parser.add_argument('--transport', choices=('tcp', 'rdma'), default='rdma')
    parser.add_argument('--address', required=True)
    parser.add_argument('--service', type=int, default=4420)
    parser.add_argument('--sui-rpc', required=True, help='Sui JSON-RPC host:port')
    parser.add_argument('--metrics-url', help='Sui metrics endpoint used for aggregate live counter evidence')
    parser.add_argument('--owner-pid', type=int, action='append', default=[],
                        help='owned process expected to stay alive; repeat per process')
    parser.add_argument('--timeout', type=float, default=30.0)
    parser.add_argument('--preflight-timeout', type=float, default=10.0,
                        help='bound on observing the selected namespace write advance')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()

    def persist(record):
        """Replace the record atomically, so a killed run leaves a whole file."""
        temporary = args.output.with_name(args.output.name + '.tmp')
        temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
        os.replace(temporary, args.output)

    try:
        record = run(args, write=persist)
    except (OSError, ValueError) as error:
        # `run` persists each milestone, so the last one survives an exception
        # on any later RPC: a fallback record still says whether the controller
        # was selected and deleted. Only a run that died before its first
        # milestone leaves nothing, and then the record says exactly that.
        try:
            record = json.loads(args.output.read_text())
            if not isinstance(record, dict):
                raise ValueError('the persisted record is not an object')
        except (OSError, ValueError):
            record = dict(schema=1, kind='w1_controller_loss', deleted=False,
                          created_unix_ns=time.time_ns())
        record.update(ok=False, reason=str(error))
    persist(record)
    print(json.dumps(dict(ok=record.get('ok'), reason=record.get('reason'),
                          output=str(args.output)), sort_keys=True))
    if not record.get('ok'):
        sys.exit(1)


if __name__ == '__main__':
    main()
