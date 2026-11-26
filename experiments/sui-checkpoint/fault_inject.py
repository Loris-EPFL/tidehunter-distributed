#!/usr/bin/env python3
"""W1 controller-loss injector for stage S1-4/M7.

Deletes one verified host NVMe-oF controller with sysfs `delete_controller`
while the owner process stays alive. Then it proves that checkpoint writes
continue, that a previously read checkpoint summary reads back byte-identical,
and that the aggregate live counters advanced. The injector never disconnects
globally and never signals an owner process.

It runs on the initiator, because the controller object being deleted is the
local one: the coordinator host holds one controller per owner target. The
target is either one exact NQN (`--nqn`) or the owner's contiguous live service
window (`--service` to `--service-max`); the window form keeps the lowest NQN
and records every candidate.

Exit status is nonzero when any verification fails; the JSON record is written
in both outcomes so the campaign retains the failed case.
"""
import argparse
import hashlib
import importlib.util
import ipaddress
import json
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


class InjectionError(ValueError):
    """A precondition or verification failed; evidence is still retained."""


def validate_spec(args):
    if args.nqn is not None:
        if not isinstance(args.nqn, str) or not manage.NQN.fullmatch(args.nqn):
            raise ValueError('invalid target NQN')
    elif not (type(args.service_max) is int and args.service <= args.service_max <= 65535):
        raise ValueError('an exact NQN or a bounded service window is required')
    if args.transport not in ('tcp', 'rdma'):
        raise ValueError('transport must be tcp or rdma')
    if not 1 <= args.service <= 65535:
        raise ValueError('service out of range')
    if args.timeout <= 0:
        raise ValueError('timeout must be positive')
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


def select_scoped(inventory, *, transport, address, service, service_max):
    """Select the lowest-NQN controller inside one owner's live service window.

    A live owner host publishes one target per database partition inside a
    contiguous service window (`deployment.prepare` sets the base as
    `4420 + 64 * partition`), so the window names the owner host while one
    controller is still needed. The lowest NQN makes that choice explicit and
    the whole candidate list is retained as evidence.
    """
    if not (type(service) is int and type(service_max) is int and 1 <= service <= service_max <= 65535):
        raise ValueError('invalid service window')
    matches = []
    for name, identity in inventory.items():
        if identity['transport'] != transport:
            continue
        fields = dict(item.split('=', 1) for item in identity['address'].split(',') if '=' in item)
        if fields.get('traddr') != address or not fields.get('trsvcid', '').isdigit():
            continue
        if service <= int(fields['trsvcid']) <= service_max:
            matches.append((identity['nqn'], name, identity))
    if not matches:
        raise ValueError('no controller inside the owner service window')
    matches.sort(key=lambda row: (row[0], row[1]))
    nqn, name, identity = matches[0]
    return name, identity, sorted(row[0] for row in matches)


def hostnqn(controller):
    value = (manage.SYS_NVME / controller / 'hostnqn').read_text().strip()
    if not value:
        raise ValueError('controller host NQN is empty')
    return value


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
        sleep=time.sleep, read_checkpoint=read_checkpoint):
    """Inject one controller loss and return the retained record."""
    snapshot = snapshot or manage.controller_snapshot
    delete = delete or manage.delete_controller
    validate_spec(args)
    address = str(ipaddress.ip_address(args.address))
    record = dict(schema=1, kind='w1_controller_loss', injection='sysfs delete_controller',
        created_unix_ns=time.time_ns(), target=dict(nqn=args.nqn, transport=args.transport,
            address=address, service=args.service), ok=False, reason=None)
    record['processes_before'] = process_state(args.owner_pid)
    if any(not row['alive'] for row in record['processes_before']):
        record['reason'] = 'an owned process was not alive before injection'
        return record
    inventory = snapshot()
    candidates = None
    if args.nqn is None:
        controller, identity, candidates = select_scoped(
            inventory, transport=args.transport, address=address,
            service=args.service, service_max=args.service_max)
    else:
        controller, identity = select_controller(inventory, nqn=args.nqn,
            transport=args.transport, address=address, service=args.service)
    deleted_nqn = identity['nqn']
    deleted_hostnqn = hostnqn(controller)
    record['target'].update(controller=controller, inode=identity['inode'],
        hostnqn=deleted_hostnqn, nqn=deleted_nqn, transport=args.transport,
        address=address, service=args.service, service_max=args.service_max)
    if candidates is not None:
        record['target']['candidates'] = candidates
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
    try:
        delete(controller, identity, {'hostnqn': deleted_hostnqn})
    except (OSError, ValueError) as error:
        record['reason'] = 'controller delete was refused or failed: ' + str(error)
        record['ok'] = False
        record['finished_unix_ns'] = time.time_ns()
        return record
    record['outage_started_seconds'] = started
    try:
        reconnect, reconnect_observations = wait_for_reconnect(nqn=deleted_nqn,
            transport=args.transport, address=address, service=args.service,
            removed_inode=identity['inode'], timeout=args.timeout,
            snapshot=snapshot, clock=clock, sleep=sleep)
        record['reconnect_seconds'] = reconnect
        record['reconnect_observations'] = reconnect_observations
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
        after_metrics = live_counters(metrics(args.metrics_url)) if args.metrics_url else None
        if before_metrics is not None and after_metrics is not None:
            from analyze import live_nvmeof_deltas
            record['live_after'] = after_metrics
            record['live_delta'] = live_nvmeof_deltas(
                {f'tidehunter_nvmeof_live_{k}': v for k, v in before_metrics['counters'].items()},
                {f'tidehunter_nvmeof_live_{k}': v for k, v in after_metrics['counters'].items()})
        record['processes_after'] = process_state(args.owner_pid)
        failures = []
        if not record['readback_equal']:
            failures.append('checkpoint summary readback differs')
        if any(not row['alive'] for row in record['processes_after']):
            failures.append('an owned process fail-stopped')
        if args.metrics_url and record.get('live_delta', {}).get('available') is False:
            failures.append('aggregate live counters are not a usable before/after pair')
        if args.metrics_url and record.get('live_delta', {}).get('available') and (
                record['live_delta'].get('errors') or 0) > 0:
            failures.append('live data-plane errors increased during the injection')
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
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        record = run(args)
    except (OSError, ValueError) as error:
        record = dict(schema=1, kind='w1_controller_loss', ok=False, reason=str(error),
                      created_unix_ns=time.time_ns())
    args.output.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    print(json.dumps(dict(ok=record.get('ok'), reason=record.get('reason'),
                          output=str(args.output)), sort_keys=True))
    if not record.get('ok'):
        sys.exit(1)


if __name__ == '__main__':
    main()
