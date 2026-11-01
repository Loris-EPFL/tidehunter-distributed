#!/usr/bin/env python3
"""Capture bounded, isolated nvmet and block tracefs events on the target."""
import argparse
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

import manage

TRACEFS = Path('/sys/kernel/tracing')
EVENTS = ('nvmet/nvmet_req_init', 'nvmet/nvmet_req_complete',
          'block/block_rq_issue', 'block/block_rq_complete')
LINE = re.compile(r'\s(\d+\.\d+):\s+(nvmet_req_init|nvmet_req_complete|block_rq_issue|block_rq_complete):\s+(.*)$')
NVMET = re.compile(r'nvmet(\d+):.*?qid=(\d+),\s*cmdid=(\d+)')
BLOCK = re.compile(r'^(\d+),(\d+)\s+.*?\s(\d+)\s*\+\s*(\d+)')
START_TIMEOUT = 10
STOP_TIMEOUT = 15
POLL_INTERVAL = 0.05


def stop_requested(directory):
    try:
        os.stat('stop', dir_fd=directory.fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def request_stop(directory):
    try:
        fd = os.open('stop', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory.fd)
    except FileExistsError:
        return
    os.close(fd)


def wait_capture(directory, seconds):
    deadline = time.monotonic() + seconds
    while not stop_requested(directory):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL, remaining))


def summarize(lines, ctrl_id, block_dev):
    pending = {'nvmet': {}, 'block': {}}
    stats = {k: {'count': 0, 'sum_ns': 0, 'max_ns': 0, 'unmatched_start': 0,
                 'unmatched_completion': 0, 'ambiguous': 0} for k in pending}
    for line in lines:
        match = LINE.search(line)
        if not match:
            continue
        try:
            timestamp = int(Decimal(match.group(1)) * 1000000000)
        except InvalidOperation:
            continue
        event, payload = match.group(2), match.group(3)
        if event.startswith('nvmet_'):
            row = NVMET.search(payload)
            if not row or int(row.group(1)) != ctrl_id or int(row.group(2)) == 0:
                continue
            kind = 'nvmet'; key = tuple(map(int, row.groups())); start = event == 'nvmet_req_init'
        else:
            row = BLOCK.search(payload)
            if not row or tuple(map(int, row.groups()[:2])) != block_dev:
                continue
            kind = 'block'; key = tuple(map(int, row.groups())); start = event == 'block_rq_issue'
        outstanding = pending[kind]; result = stats[kind]
        if start:
            if key in outstanding:
                outstanding[key] = None
                result['ambiguous'] += 1
            else:
                outstanding[key] = timestamp
        elif key not in outstanding:
            result['unmatched_completion'] += 1
        else:
            earlier = outstanding.pop(key)
            if earlier is None or timestamp < earlier:
                result['ambiguous'] += 1
                continue
            elapsed = timestamp - earlier
            result['count'] += 1; result['sum_ns'] += elapsed
            result['max_ns'] = max(result['max_ns'], elapsed)
    for kind, entries in pending.items():
        stats[kind]['unmatched_start'] = len(entries)
    return stats


def cleanup_instance(directory, state):
    name = state.get('instance')
    if not isinstance(name, str) or not re.fullmatch(r'tidehunter-[0-9a-f]{32}', name):
        raise ValueError('invalid owned trace instance name')
    instance = TRACEFS / 'instances' / name
    if instance.exists():
        if state.get('instance_identity') != manage.object_identity(instance):
            raise ValueError('trace instance identity missing or replaced; refusing cleanup')
        (instance / 'tracing_on').write_text('0')
        instance.rmdir()
    state['phase'] = 'cleaned'
    manage.save(directory, state)


def recover(path):
    directory, state = manage.load_state(path, 'trace')
    if state.get('phase') == 'cleaned':
        return state
    pid = state.get('pid')
    if type(pid) is not int or pid <= 0:
        raise ValueError('invalid trace owner pid')
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise ValueError('trace owner PID still exists; stop it before recovery')
    cleanup_instance(directory, state)
    return state


def interrupted(signum, frame):
    raise InterruptedError('trace interrupted by signal ' + str(signum))


def capture(args):
    if os.geteuid() != 0:
        raise ValueError('root/tracefs permissions required')
    if not 0 < args.seconds <= 3600 or not 64 <= args.buffer_kb <= 4096:
        raise ValueError('seconds must be in (0,3600], buffer-kb in [64,4096] per CPU')
    if not 1 <= args.ctrl_id <= 65535:
        raise ValueError('controller ID must be in [1,65535]')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.block_device) or args.block_device in ('.', '..'):
        raise ValueError('invalid block device')
    block_dev = tuple(map(int, (Path('/sys/class/block') / args.block_device / 'dev').read_text().strip().split(':')))
    if len(block_dev) != 2:
        raise ValueError('invalid block device number')
    instances = TRACEFS / 'instances'
    if not instances.is_dir():
        raise ValueError('tracefs instances unavailable; mount tracefs and grant access')
    for event in EVENTS:
        if not (TRACEFS / 'events' / event / 'format').is_file():
            raise ValueError('required tracepoint unavailable: ' + event)
    output = Path(args.output)
    name = 'tidehunter-' + uuid.uuid4().hex
    state = {'version': 1, 'kind': 'trace', 'phase': 'creating', 'instance': name,
             'pid': os.getpid(), 'created_at_unix_ns': time.time_ns()}
    directory = manage.new_state(output, state)
    instance = instances / name
    previous = signal.signal(signal.SIGTERM, interrupted)
    created = False
    try:
        instance.mkdir()
        created = True
        state['instance_identity'] = manage.object_identity(instance)
        state['phase'] = 'configured'
        manage.save(directory, state)
        (instance / 'tracing_on').write_text('0')
        (instance / 'buffer_size_kb').write_text(str(args.buffer_kb))
        clocks = (instance / 'trace_clock').read_text().replace('[', '').replace(']', '').split()
        if 'mono' not in clocks:
            raise ValueError('tracefs mono clock unavailable')
        (instance / 'trace_clock').write_text('mono')
        for event in EVENTS:
            (output / (event.replace('/', '_') + '.format')).write_text((TRACEFS / 'events' / event / 'format').read_text())
            (instance / 'events' / event / 'enable').write_text('1')
        (instance / 'trace').write_text('')
        started = time.monotonic_ns()
        (instance / 'tracing_on').write_text('1')
        state['phase'] = 'running'
        state['start_monotonic_ns'] = started
        manage.save(directory, state)
        wait_capture(directory, args.seconds)
        (instance / 'tracing_on').write_text('0')
        stopped = time.monotonic_ns()
        # The kernel ring is bounded per CPU. Refuse excessive capture rather than
        # truncating it and silently treating an incomplete trace as complete.
        with (instance / 'trace').open('rb') as f:
            raw = f.read(64 * 1024 * 1024 + 1)
        if len(raw) > 64 * 1024 * 1024:
            raise ValueError('trace exceeds 64 MiB output limit; reduce buffer/interval')
        (output / 'trace.txt').write_bytes(raw)
        cpu_stats = {}
        for stat in (instance / 'per_cpu').glob('cpu*/stats'):
            cpu_stats[stat.parent.name] = stat.read_text()
        (output / 'per_cpu_stats.json').write_text(json.dumps(cpu_stats, indent=2) + '\n')
        if not cpu_stats:
            raise ValueError('tracefs per-CPU loss statistics unavailable; raw trace retained')
        loss = []
        for text in cpu_stats.values():
            values = re.findall(r'^(?:overrun|dropped events):\s*(\d+)', text, re.M)
            if len(values) != 2:
                raise ValueError('incomplete tracefs per-CPU loss statistics; raw trace retained')
            loss.extend(map(int, values))
        overruns = sum(loss)
        result = {'version': 1, 'available': True, 'trace_clock': 'mono',
                  'start_monotonic_ns': started, 'end_monotonic_ns': stopped,
                  'controller_id': args.ctrl_id, 'block_device': args.block_device,
                  'block_major_minor': list(block_dev), 'overrun_or_dropped': overruns,
                  'stages': summarize(raw.decode('utf-8', errors='replace').splitlines(), args.ctrl_id, block_dev),
                  'qualification': 'nvmet ctrl_id/qid/cid pairs are target-side request lifetime, not NAND or network latency; ctrl_id can collide between subsystems. Block dev/sector/length pairs can alias, merge or split. No cross-layer request correlation is claimed. Unmatched, ambiguous and lost events invalidate completeness; isolate the target and inspect raw trace.'}
        (output / 'summary.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
        state['result'] = result
        manage.save(directory, state)
        return result
    except Exception as error:
        state['error'] = f'{type(error).__name__}: {error}'[:500]
        manage.save(directory, state)
        raise
    finally:
        try:
            if created:
                if 'instance_identity' not in state:
                    state['instance_identity'] = manage.object_identity(instance)
                    manage.save(directory, state)
                cleanup_instance(directory, state)
        finally:
            signal.signal(signal.SIGTERM, previous)


def start(args):
    command = [sys.executable, str(Path(__file__).resolve()), '--seconds', str(args.seconds),
               '--buffer-kb', str(args.buffer_kb), '--ctrl-id', str(args.ctrl_id),
               '--block-device', args.block_device, '--output', args.output]
    process = subprocess.Popen(command, start_new_session=True, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
    deadline = time.monotonic() + START_TIMEOUT
    directory = None
    while time.monotonic() < deadline:
        try:
            current, state = manage.load_state(args.output, 'trace')
        except FileNotFoundError:
            state = None
        if state is not None and state.get('pid') == process.pid:
            directory = current
            if state.get('phase') == 'running':
                return {'available': True, 'output': args.output, 'pid': process.pid,
                        'start_monotonic_ns': state['start_monotonic_ns']}
            if state.get('phase') == 'cleaned':
                if 'result' in state and 'error' not in state:
                    return state['result']
                return {'available': False, 'output': args.output,
                        'reason': state.get('error', 'capture ended before startup was observed')}
        if process.poll() is not None:
            return {'available': False, 'output': args.output,
                    'reason': (state or {}).get('error', 'collector exited before tracing started')}
        time.sleep(POLL_INTERVAL)
    if directory is not None:
        request_stop(directory)
    else:
        process.terminate()
    return {'available': False, 'output': args.output, 'reason': 'collector startup timed out'}


def stop(path):
    directory, state = manage.load_state(path, 'trace')
    if state.get('phase') != 'cleaned':
        request_stop(directory)
    deadline = time.monotonic() + STOP_TIMEOUT
    while True:
        _, state = manage.load_state(path, 'trace')
        if state.get('phase') == 'cleaned':
            if 'result' in state and 'error' not in state:
                return state['result']
            return {'available': False, 'output': path,
                    'reason': state.get('error', 'capture ended without a summary')}
        pid = state.get('pid')
        if type(pid) is not int or pid <= 0:
            raise ValueError('invalid trace owner pid')
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return {'available': False, 'output': path,
                    'reason': 'collector exited before cleanup; use --recover'}
        if time.monotonic() >= deadline:
            return {'available': False, 'output': path, 'reason': 'collector stop timed out'}
        time.sleep(POLL_INTERVAL)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--seconds', type=float)
    p.add_argument('--buffer-kb', type=int, default=256)
    p.add_argument('--ctrl-id', type=int, help='target controller ID')
    p.add_argument('--block-device', help='target block device sysfs name')
    p.add_argument('--output', help='new absolute output directory')
    modes = p.add_mutually_exclusive_group()
    modes.add_argument('--start', action='store_true', help='start a bounded background capture')
    modes.add_argument('--stop', help='stop and collect an owned background capture')
    modes.add_argument('--recover', help='owned output directory of interrupted trace')
    args = p.parse_args()
    if args.recover or args.stop:
        if any(x is not None for x in (args.seconds, args.ctrl_id, args.block_device, args.output)):
            p.error('--stop and --recover cannot be combined with capture arguments')
        result = recover(args.recover) if args.recover else stop(args.stop)
    else:
        if any(x is None for x in (args.seconds, args.ctrl_id, args.block_device, args.output)):
            p.error('capture requires --seconds, --ctrl-id, --block-device and --output')
        result = start(args) if args.start else capture(args)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
