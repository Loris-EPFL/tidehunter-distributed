"""Bounded owner tracing. Only the owned tracefs instance is changed."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

EVENTS = ('jbd2/jbd2_start_commit', 'jbd2/jbd2_end_commit', 'jbd2/jbd2_run_stats',
          'writeback/balance_dirty_pages', 'ext4/ext4_fallocate_enter',
          'ext4/ext4_fallocate_exit', 'ext4/ext4_sync_file_enter',
          'ext4/ext4_sync_file_exit', 'nvmet/nvmet_req_init', 'nvmet/nvmet_req_complete')
MAX_BYTES = 64 * 1024**2
START_STATES = {}
CPU_MAX_BYTES = 16 * 1024**2
CPU_MAX_SAMPLES = 7200
CPU_MAX_CPUS = 256


def cpu_ticks(path=Path('/proc/stat')):
    """First eight fields exclude guest ticks already counted in user/nice."""
    result = {}
    for line in Path(path).read_text().splitlines():
        fields = line.split()
        if fields and re.fullmatch(r'cpu[0-9]+', fields[0]):
            ticks = [int(value) for value in fields[1:9]]
            if len(ticks) != 8 or any(value < 0 for value in ticks):
                raise ValueError('invalid per-CPU stat fields')
            result[fields[0]] = ticks
    if not result or len(result) > CPU_MAX_CPUS:
        raise ValueError('per-CPU stat population unavailable or exceeds bound')
    return result


def cpu_deltas(before, after):
    if before.keys() != after.keys():
        raise ValueError('CPU population changed during measurement')
    result = {}
    for cpu, ticks in before.items():
        delta = [last - first for first, last in zip(ticks, after[cpu])]
        if any(value < 0 for value in delta):
            raise ValueError('per-CPU stat counter decreased')
        total = sum(delta)
        result[cpu] = {'ticks': delta, 'busy_pct': 100 * (total - delta[3] - delta[4]) / total
                       if total else None, 'softirq_pct': 100 * delta[6] / total if total else None}
    return result


def sample_cpu(directory, stat_path=Path('/proc/stat'), duration_seconds=CPU_MAX_SAMPLES):
    """One-second deltas plus the final partial interval, within the fleet deadline."""
    directory = Path(directory)
    artifact = {'available': False, 'source': '/proc/stat', 'interval_seconds': 1,
                'scope': 'owner trace start to stop; includes the final partial interval',
                'samples': [], 'truncated': False}
    output = directory / 'owner-cpu.json'
    try:
        before = cpu_ticks(stat_path)
        previous = time.monotonic()
        expires = previous + duration_seconds
        totals = {cpu: [0] * 8 for cpu in before}
        (directory / 'owner-cpu.ready').touch()
        size = 0
        for index in range(CPU_MAX_SAMPLES):
            deadline = min(previous + 1, expires)
            while not (directory / 'owner-cpu.stop').exists() and time.monotonic() < deadline:
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            now = time.monotonic()
            after = cpu_ticks(stat_path)
            deltas = cpu_deltas(before, after)
            sample = {'elapsed_seconds': now - previous, 'cpus': deltas}
            size += len(json.dumps(sample)) + 2
            if size > CPU_MAX_BYTES - 4096:
                artifact['truncated'] = True
                break
            artifact['samples'].append(sample)
            for cpu, data in deltas.items():
                totals[cpu] = [a + b for a, b in zip(totals[cpu], data['ticks'])]
            before, previous = after, now
            if (directory / 'owner-cpu.stop').exists():
                break
            if now >= expires:
                artifact.update(truncated=True, reason='owner CPU sampling deadline expired')
                break
        else:
            artifact['truncated'] = True
        per_cpu = cpu_deltas({cpu: [0] * 8 for cpu in totals}, totals)
        busy = [data['busy_pct'] for data in per_cpu.values()]
        softirq = [data['softirq_pct'] for data in per_cpu.values()]
        artifact['available'] = bool(busy) and all(v is not None for v in busy) and not artifact['truncated']
        if artifact['available']:
            artifact['summary'] = {'max_busy_pct': max(busy), 'mean_busy_pct': sum(busy) / len(busy),
                                   'max_softirq_pct': max(softirq),
                                   'cpus_above_90_busy': sum(value > 90 for value in busy)}
    except (OSError, ValueError) as error:
        artifact['reason'] = str(error)
    temporary = output.with_suffix('.tmp')
    temporary.write_text(json.dumps(artifact) + '\n')
    temporary.replace(output)


def _start_sampler(directory, kind, duration_seconds, args=()):
    directory = Path(directory)
    if not 0 < duration_seconds <= CPU_MAX_SAMPLES:
        return {'available': False, 'reason': 'CPU sampling deadline unavailable or expired'}
    try:
        directory.mkdir(parents=True, exist_ok=True)
        for name in (kind + '.json', kind + '.ready', kind + '.stop'):
            (directory / name).unlink(missing_ok=True)
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--sample-' + kind,
                                    str(directory), str(duration_seconds), *map(str, args)],
                                   start_new_session=True, close_fds=True,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as error:
        return {'available': False, 'reason': str(error)}
    for attempt in range(100):
        if (directory / (kind + '.ready')).exists():
            return {'directory': str(directory)}
        if process.poll() is not None:
            return {'available': False, 'reason': 'owner CPU sampler failed to start'}
        time.sleep(0.02)
    process.terminate()
    process.wait(timeout=3)
    return {'available': False, 'reason': 'owner CPU sampler startup timed out'}


def _stop_sampler(state, kind):
    if not state or 'directory' not in state:
        return state or {'available': False, 'reason': 'owner CPU sampling was not started'}
    directory = Path(state['directory'])
    try:
        (directory / (kind + '.stop')).touch()
        for attempt in range(500):
            path = directory / (kind + '.json')
            if path.exists():
                if path.stat().st_size > CPU_MAX_BYTES:
                    return {'available': False, 'reason': 'owner CPU artifact exceeds bound'}
                return json.loads(path.read_text())
            time.sleep(0.02)
    except (OSError, ValueError) as error:
        return {'available': False, 'reason': str(error)}
    return {'available': False, 'reason': 'owner CPU sampler stop timed out'}


def start_cpu(directory, duration_seconds=CPU_MAX_SAMPLES):
    return _start_sampler(directory, 'owner-cpu', duration_seconds)


def stop_cpu(state):
    return _stop_sampler(state, 'owner-cpu')


THREAD_CLASSES = ('wal_write_owner', 'value_wal_tcp', 'value_wal_prepare', 'wal_live_read', 'other')
THREAD_MAX_THREADS = 4096


def thread_class(name):
    if name == 'wal-write-owner':
        return 'wal_write_owner'
    if name == 'value-wal-tcp':
        return 'value_wal_tcp'
    # Linux comm is truncated to 15 bytes, including value-wal-prepare-*.
    if name.startswith('value-wal-prepa'):
        return 'value_wal_prepare'
    if name.startswith('wal-live-read-'):
        return 'wal_live_read'
    return 'other'


def parse_thread_stat(text):
    opening, closing = text.find('('), text.rfind(')')
    fields = text[closing + 1:].split()
    if opening < 0 or closing <= opening or len(fields) < 20:
        raise ValueError('invalid task stat fields')
    ticks, started = int(fields[11]) + int(fields[12]), int(fields[19])
    if ticks < 0 or started < 0:
        raise ValueError('invalid task stat counters')
    return {'name': text[opening + 1:closing], 'cpu_ticks': ticks, 'start_ticks': started}


def thread_snapshot(pid, proc=Path('/proc')):
    root = Path(proc) / str(pid)
    process = parse_thread_stat((root / 'stat').read_text())
    threads = {}
    for index, task in enumerate((root / 'task').iterdir()):
        if not task.name.isdigit():
            continue
        if index >= THREAD_MAX_THREADS:
            raise ValueError('task population exceeds bound')
        try:
            threads[task.name] = parse_thread_stat((task / 'stat').read_text())
        except (FileNotFoundError, ProcessLookupError):
            continue  # A task can exit between readdir and stat; the process identity is checked separately.
    if not threads:
        raise ValueError('task population unavailable')
    return {'process_start_ticks': process['start_ticks'], 'threads': threads}


def thread_summary(totals, incomplete, elapsed, ticks_per_second):
    summary = {}
    for group in THREAD_CLASSES:
        members = [row for row in totals.values() if thread_class(row['name']) == group]
        available = bool(members) and group not in incomplete and elapsed > 0
        values = [100 * row['cpu_ticks'] / ticks_per_second / elapsed for row in members] if available else []
        summary[group] = {'available': available, 'thread_count': len(members),
                          'max_busy_pct': max(values) if values else None,
                          'mean_busy_pct': sum(values) / len(values) if values else None}
        if group == 'other' and values:
            summary[group]['busiest_name'] = members[values.index(max(values))]['name']
        if not available:
            summary[group]['reason'] = 'thread population changed' if group in incomplete else 'no observed threads'
    return summary


def sample_threads(directory, pid, duration_seconds=CPU_MAX_SAMPLES, proc=Path('/proc')):
    """One logical CPU is 100%; missing/churning classes remain unavailable."""
    directory = Path(directory)
    artifact = {'available': False, 'source': '/proc/PID/task/*/stat', 'pid': pid,
                'interval_seconds': 1, 'samples': [], 'truncated': False,
                'scope': 'sampler-ready before measurement release to completion-observed stop; includes boundary RPC overlap and final partial interval'}
    try:
        hz = os.sysconf('SC_CLK_TCK')
        before = thread_snapshot(pid, proc)
        previous = started = time.monotonic()
        expires = started + duration_seconds
        totals = {(tid, row['start_ticks']): dict(row, cpu_ticks=0)
                  for tid, row in before['threads'].items()}
        incomplete = set()
        size = 0
        (directory / 'thread-cpu.ready').touch()
        for index in range(CPU_MAX_SAMPLES):
            deadline = min(previous + 1, expires)
            while not (directory / 'thread-cpu.stop').exists() and time.monotonic() < deadline:
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            after = thread_snapshot(pid, proc)
            now = time.monotonic()
            elapsed = now - previous
            if after['process_start_ticks'] != before['process_start_ticks'] or elapsed <= 0:
                raise ValueError('sampled process identity or clock changed')
            rows = []
            for tid in before['threads'].keys() | after['threads'].keys():
                first, last = before['threads'].get(tid), after['threads'].get(tid)
                if first is None or last is None or first['start_ticks'] != last['start_ticks'] or first['name'] != last['name']:
                    incomplete.update(thread_class(row['name']) for row in (first, last) if row is not None)
                    if last is not None:
                        totals.setdefault((tid, last['start_ticks']), dict(last, cpu_ticks=0))
                        if len(totals) > THREAD_MAX_THREADS:
                            raise ValueError('observed task identities exceed bound')
                    continue
                delta = last['cpu_ticks'] - first['cpu_ticks']
                if delta < 0:
                    raise ValueError('task CPU counter decreased')
                totals[(tid, last['start_ticks'])]['cpu_ticks'] += delta
                rows.append({'tid': tid, 'name': last['name'], 'start_ticks': last['start_ticks'],
                             'cpu_ticks': delta, 'busy_pct': 100 * delta / hz / elapsed})
            sample = {'elapsed_seconds': elapsed, 'threads': rows}
            size += len(json.dumps(sample)) + 2
            if size > CPU_MAX_BYTES - 65536:
                artifact['truncated'] = True
                break
            artifact['samples'].append(sample)
            before, previous = after, now
            if (directory / 'thread-cpu.stop').exists():
                break
            if now >= expires:
                artifact.update(truncated=True, reason='thread CPU sampling deadline expired')
                break
        else:
            artifact['truncated'] = True
        artifact.update(available=not artifact['truncated'], elapsed_seconds=previous - started,
                        ticks_per_second=hz)
        if artifact['available']:
            artifact['summary'] = thread_summary(totals, incomplete, previous - started, hz)
    except (OSError, ValueError) as error:
        artifact['reason'] = str(error)
    output = directory / 'thread-cpu.json'
    temporary = output.with_suffix('.tmp')
    temporary.write_text(json.dumps(artifact) + '\n')
    temporary.replace(output)


def start_threads(directory, pid, duration_seconds=CPU_MAX_SAMPLES):
    if type(pid) is not int or pid <= 0:
        return {'available': False, 'reason': 'workload PID unavailable'}
    return _start_sampler(directory, 'thread-cpu', duration_seconds, (pid,))


def stop_threads(state):
    return _stop_sampler(state, 'thread-cpu')

def probe(tracefs=Path('/sys/kernel/tracing')):
    tracefs = Path(tracefs)
    return {'events_enabled': [e for e in EVENTS if (tracefs / 'events' / e / 'enable').exists()],
            'events_missing': [e for e in EVENTS if not (tracefs / 'events' / e / 'enable').exists()]}


def _remove(instance, errors):
    try:
        instance.rmdir()
    except OSError as error:
        errors.append('remove: ' + str(error))


def start(case_id, tracefs=Path('/sys/kernel/tracing'), cpu_directory=None, duration_seconds=CPU_MAX_SAMPLES):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case_id):
        raise ValueError('invalid owner trace case identity')
    instance = Path(tracefs) / 'instances' / ('th-' + case_id)
    state = probe(tracefs)
    state['errors'] = []
    state['events_enabled'] = []
    try:
        instance.mkdir()
        for name, value in (('trace_clock', 'mono'), ('buffer_size_kb', '16384')):
            try:
                (instance / name).write_text(value)
            except OSError as error:
                state['errors'].append(name + ': ' + str(error))
        for event in EVENTS:
            if event in state['events_missing']:
                continue
            try:
                (instance / 'events' / event / 'enable').write_text('1')
                state['events_enabled'].append(event)
            except OSError as error:
                state['errors'].append(event + ': ' + str(error))
        (instance / 'tracing_on').write_text('1')
    except OSError as error:
        state['errors'].append('start: ' + str(error))
        _remove(instance, state['errors'])
    # State lives outside tracefs, which does not allow normal files.
    START_STATES[str(instance)] = state
    if cpu_directory is not None:
        state['cpu_sampling'] = start_cpu(cpu_directory, duration_seconds)
    return instance


def stop(instance, output_dir, state=None):
    instance, output_dir = Path(instance), Path(output_dir)
    state = dict(state or {'events_enabled': [], 'events_missing': list(EVENTS), 'errors': []})
    state.update(truncated=False, buffer_overruns=0)
    cpu = stop_cpu(state.pop('cpu_sampling', None))
    state['cpu_summary'] = {key: value for key, value in cpu.items() if key != 'samples'}
    try:
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / 'owner-cpu.json').write_text(json.dumps(cpu) + '\n')
        except OSError as error:
            state['errors'].append('output: ' + str(error))
        try:
            (instance / 'tracing_on').write_text('0')
        except OSError as error:
            state['errors'].append('stop: ' + str(error))
        try:
            with (instance / 'trace').open('rb') as source, (output_dir / 'owner-ftrace.txt').open('wb') as target:
                remaining = MAX_BYTES
                while remaining:
                    block = source.read(min(1024**2, remaining))
                    if not block:
                        break
                    target.write(block)
                    remaining -= len(block)
                state['truncated'] = bool(source.read(1))
        except OSError as error:
            state['errors'].append('trace: ' + str(error))
        for stats in instance.glob('per_cpu/cpu*/stats'):
            try:
                for line in stats.read_text().splitlines():
                    if line.split(':', 1)[0].strip() == 'overrun':
                        state['buffer_overruns'] += int(line.split(':', 1)[1])
            except (OSError, ValueError) as error:
                state['errors'].append('stats: ' + str(error))
    finally:
        _remove(instance, state['errors'])
        try:
            (output_dir / 'owner-ftrace.json').write_text(json.dumps(state, indent=2) + '\n')
        except OSError as error:
            state['errors'].append('metadata: ' + str(error))
    return state


if __name__ == '__main__':
    if len(sys.argv) == 4 and sys.argv[1] == '--sample-owner-cpu':
        sample_cpu(sys.argv[2], duration_seconds=float(sys.argv[3]))
    elif len(sys.argv) == 5 and sys.argv[1] == '--sample-thread-cpu':
        sample_threads(sys.argv[2], int(sys.argv[4]), duration_seconds=float(sys.argv[3]))
    else:
        raise SystemExit('usage: owner_ftrace.py --sample-owner-cpu DIRECTORY DURATION_SECONDS | --sample-thread-cpu DIRECTORY DURATION_SECONDS PID')
