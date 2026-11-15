"""Bounded owner tracing. Only the owned tracefs instance is changed."""
import json
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


def start_cpu(directory, duration_seconds=CPU_MAX_SAMPLES):
    directory = Path(directory)
    if not 0 < duration_seconds <= CPU_MAX_SAMPLES:
        return {'available': False, 'reason': 'owner CPU sampling deadline unavailable or expired'}
    try:
        directory.mkdir(parents=True, exist_ok=True)
        for name in ('owner-cpu.json', 'owner-cpu.ready', 'owner-cpu.stop'):
            (directory / name).unlink(missing_ok=True)
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--sample-cpu',
                                    str(directory), str(duration_seconds)],
                                   start_new_session=True, close_fds=True,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as error:
        return {'available': False, 'reason': str(error)}
    for attempt in range(100):
        if (directory / 'owner-cpu.ready').exists():
            return {'directory': str(directory)}
        if process.poll() is not None:
            return {'available': False, 'reason': 'owner CPU sampler failed to start'}
        time.sleep(0.02)
    process.terminate()
    process.wait(timeout=3)
    return {'available': False, 'reason': 'owner CPU sampler startup timed out'}


def stop_cpu(state):
    if not state or 'directory' not in state:
        return state or {'available': False, 'reason': 'owner CPU sampling was not started'}
    directory = Path(state['directory'])
    try:
        (directory / 'owner-cpu.stop').touch()
        for attempt in range(500):
            path = directory / 'owner-cpu.json'
            if path.exists():
                if path.stat().st_size > CPU_MAX_BYTES:
                    return {'available': False, 'reason': 'owner CPU artifact exceeds bound'}
                return json.loads(path.read_text())
            time.sleep(0.02)
    except (OSError, ValueError) as error:
        return {'available': False, 'reason': str(error)}
    return {'available': False, 'reason': 'owner CPU sampler stop timed out'}


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
    if len(sys.argv) != 4 or sys.argv[1] != '--sample-cpu':
        raise SystemExit('usage: owner_ftrace.py --sample-cpu DIRECTORY DURATION_SECONDS')
    sample_cpu(sys.argv[2], duration_seconds=float(sys.argv[3]))
