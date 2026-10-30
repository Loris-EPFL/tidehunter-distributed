"""Bounded, device-wide block trace evidence; never a NAND or application latency metric.

The detached worker owns one tracefs instance and exits at its deadline even if
its caller disappears. No global tracing controls are changed.
"""
import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import re
import secrets
import selectors
import shutil
import select
import signal
import subprocess
import sys
import time

TRACE_ROOT = Path('/sys/kernel/tracing')
SYS_BLOCK = Path('/sys/dev/block')
MAX_DURATION = 3600
MAX_CPUS = 256
BUFFER_KB = 64  # per CPU, at most 16 MiB across MAX_CPUS
MAX_STARTUP_RING_BYTES = 512 * 1024 * 1024
MAX_BYTES = 8 * 1024 * 1024
MAX_EVENTS = 20000
MAX_OUTSTANDING = 2048
EVENT = re.compile(r'\s(\d+)\.(\d+):\s+block_rq_(issue|complete|requeue):\s+(\d+),(\d+)\s+(\S+)\s+(?:\d+\s+)?(?:\([^)]*\)\s+)?(\d+)\s+\+\s+(\d+)(?:\s|$)')


def save(path, obj):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as stream:
        json.dump(obj, stream, sort_keys=True)
        stream.write('\n')
    os.replace(temporary, path)


def startup_ring_bound(cmdline, possible_cpus, page_size):
    """Bound initial per-CPU trace rings before creating an instance."""
    params = [part.split('=', 1)[1] for part in cmdline.split() if part.startswith('trace_buf_size=')]
    if len(params) != 1:
        raise ValueError('explicit unique trace_buf_size boot parameter required for startup ring bound')
    match = re.fullmatch(r'([0-9]+)([kKmMgG]?)', params[0])
    if not match:
        raise ValueError('unsupported trace_buf_size boot parameter')
    units = {'': 1, 'k': 1024, 'm': 1024**2, 'g': 1024**3}
    size = max(4096, int(match.group(1)) * units[match.group(2).lower()])
    cpus = set()
    for part in possible_cpus.strip().split(','):
        match = re.fullmatch(r'(\d+)(?:-(\d+))?', part)
        if not match:
            raise ValueError('invalid possible CPU list')
        first, last = int(match.group(1)), int(match.group(2) or match.group(1))
        if last < first or last - first >= MAX_CPUS or last >= 1_000_000:
            raise ValueError('possible CPU range exceeds bound')
        cpus.update(range(first, last + 1))
        if len(cpus) > MAX_CPUS:
            raise ValueError('possible CPU count exceeds bound')
    if not cpus or page_size <= 0 or page_size > 1024 * 1024:
        raise ValueError('invalid possible CPU or page size')
    # Account for page rounding and spare pages conservatively for each ring.
    bound = 2 * ((size + page_size - 1) // page_size + 2) * page_size * len(cpus)
    if bound > MAX_STARTUP_RING_BYTES:
        raise ValueError('initial trace ring allocation exceeds startup budget')
    return {'boot_trace_buf_size_bytes': size, 'possible_cpus': len(cpus),
            'conservative_startup_ring_bound_bytes': bound}


def resolve_device(wal_path, sys_block=SYS_BLOCK):
    path = Path(wal_path).resolve(strict=True)
    stat = path.stat()
    mount_dev = f'{os.major(stat.st_dev)}:{os.minor(stat.st_dev)}'
    node = (sys_block / mount_dev).resolve(strict=True)
    original = node
    if (node / 'partition').exists():
        node = node.parent
    if not (node / 'dev').is_file():
        raise ValueError('mount is not backed by a directly resolvable block device')
    partition_slaves = original / 'slaves'
    if any((node / 'slaves').iterdir()) or (original != node and partition_slaves.exists() and any(partition_slaves.iterdir())):
        raise ValueError('stacked block devices are not supported')
    device = (node / 'dev').read_text().strip()
    if not re.fullmatch(r'\d+:\d+', device):
        raise ValueError('invalid backing device identity')
    major, minor = map(int, device.split(':'))
    if major >= 4096 or minor >= 1048576:
        raise ValueError('device identity outside kernel dev_t encoding')
    return {'mount_device': mount_dev, 'device': device, 'sysfs_path': str(node),
            'partition': original != node}


def parse_trace(text, device, *, truncated=False, lost=None):
    """Parse bounded trace text; overlapping requests cannot be correlated safely."""
    outstanding = {}
    raw = []
    issues = completions = matches = unmatched = overlaps = malformed = overflow = splits = requeues = errors = 0
    intervals = []
    events = 0
    resolution_ns = None
    # A lost event could be a duplicate issue anywhere in the capture, and
    # aggregate loss counters cannot locate it. Unknown loss is equally unsafe.
    # Once ambiguity appears, never resume correlation within this capture.
    correlation_ambiguous = lost != 0
    for line in text.splitlines():
        if not any('block_rq_' + kind + ':' in line for kind in ('issue', 'complete', 'requeue')):
            continue
        events += 1
        if events > MAX_EVENTS:
            truncated = True
            break
        raw.append(line[:2048])
        if len(line) > 2048:
            truncated = True
        m = EVENT.search(line)
        if not m:
            correlation_ambiguous = True
            malformed += 1
            continue
        sec, fraction, kind, major, minor, rwbs, sector, length = m.groups()
        if f'{int(major)}:{int(minor)}' != device:
            correlation_ambiguous = True
            malformed += 1
            continue
        if not rwbs.startswith('R'):
            continue
        sector, length = int(sector), int(length)
        if length <= 0 or len(fraction) > 9:
            correlation_ambiguous = True
            malformed += 1
            continue
        resolution = 10 ** (9 - len(fraction))
        resolution_ns = max(resolution_ns or 0, resolution)
        timestamp = int(sec) * 1_000_000_000 + int(fraction.ljust(9, '0'))
        key = (sector, length)
        if kind == 'issue':
            issues += 1
            conflicting = [k for k in outstanding if k[0] < sector + length and sector < k[0] + k[1]]
            if conflicting:
                overlaps += 1
                correlation_ambiguous = True
                for other in conflicting:
                    outstanding[other] = None
                if key not in outstanding:
                    if len(outstanding) >= MAX_OUTSTANDING:
                        overflow += 1
                        correlation_ambiguous = True
                    else:
                        outstanding[key] = None
            elif len(outstanding) >= MAX_OUTSTANDING:
                overflow += 1
                correlation_ambiguous = True
            else:
                outstanding[key] = None if correlation_ambiguous else timestamp
        elif kind == 'requeue':
            requeues += 1
            correlation_ambiguous = True
            for other in outstanding:
                if other[0] < sector + length and sector < other[0] + other[1]:
                    outstanding[other] = None
        else:
            completions += 1
            error = re.search(r'\[(-?\d+)\]\s*$', line)
            if not error:
                malformed += 1
                correlation_ambiguous = True
            elif int(error.group(1)) != 0:
                errors += 1
                correlation_ambiguous = True
            if key not in outstanding:
                unmatched += 1
                correlation_ambiguous = True
                # Partial/split completions invalidate any intersecting issue.
                intersecting = False
                for other in outstanding:
                    if other[0] < sector + length and sector < other[0] + other[1]:
                        outstanding[other] = None
                        intersecting = True
                if intersecting:
                    splits += 1
                continue
            start = outstanding.pop(key)
            if start is None or timestamp < start:
                unmatched += 1
                correlation_ambiguous = True
            elif not correlation_ambiguous:
                matches += 1
                intervals.append(timestamp - start)
            else:
                unmatched += 1
    intervals.sort()
    n = len(intervals)
    total = sum(intervals)
    mean = total / n if n else None
    distribution = {'count': n, 'sum': total, 'max': intervals[-1] if n else None,
                    'mean': mean,
                    'population_stddev': math.sqrt(sum((x - mean) ** 2 for x in intervals) / n) if n else None,
                    'tail_warnings': []}
    for name, numerator, minimum in (('p50', 50, 2), ('p95', 95, 20),
                                     ('p99', 99, 100), ('p999', 999, 1000)):
        denominator = 1000 if name == 'p999' else 100
        distribution[name] = intervals[(numerator * n + denominator - 1) // denominator - 1] if n else None
        if n < minimum:
            distribution['tail_warnings'].append(f'{name}: fewer than {minimum} observations')
    return {'read_issues': issues, 'read_completions': completions,
            'matched_reads': None, 'intervals_ns': None, 'raw_intervals_ns': None,
            'correlation_note': 'sector/length candidates only; boundary in-flight requests are unknown',
            'candidate_matched_reads': matches, 'candidate_intervals_ns': distribution,
            'candidate_raw_intervals_ns': intervals,
            'trace_clock': 'mono', 'observed_timestamp_resolution_ns': resolution_ns,
            'unmatched_completions': unmatched, 'unmatched_issues': len(outstanding),
            'overlaps': overlaps, 'split_or_partial_completions': splits, 'requeues': requeues,
            'completion_errors': errors,
            'malformed_events': malformed, 'outstanding_overflow': overflow,
            'lost': lost, 'truncated': truncated, 'raw_events': raw}


def lost_events(instance):
    total = 0
    cpus = list((instance / 'per_cpu').glob('cpu[0-9]*'))
    if not cpus or len(cpus) > MAX_CPUS:
        raise ValueError('trace per-CPU statistics unavailable or exceed limit')
    for cpu in cpus:
        values = {}
        for line in (cpu / 'stats').read_text().splitlines():
            if ':' in line:
                key, value = line.split(':', 1)
                values[key.strip()] = value.strip()
        for key in ('overrun', 'commit overrun', 'dropped events'):
            if key not in values or not values[key].isdigit():
                raise ValueError('trace loss counters unavailable')
            total += int(values[key])
    return total


def drain_trace(fd, instance, state, deadline, stop_requested=lambda: False):
    """Drain the private trace pipe continuously, including the final stopped buffer."""
    poller = select.poll()
    poller.register(fd, select.POLLIN | select.POLLERR | select.POLLHUP)
    data = bytearray()
    lines = 0
    truncated = False
    stopped_ns = None
    while True:
        if stopped_ns is None and (stop_requested() or time.monotonic() >= deadline or (state / 'stop').exists()):
            (instance / 'tracing_on').write_text('0')
            stopped_ns = time.monotonic_ns()
        timeout_ms = 0 if stopped_ns is not None else min(100, max(0, math.ceil((deadline - time.monotonic()) * 1000)))
        ready = poller.poll(timeout_ms)
        if not ready:
            if stopped_ns is not None:
                break
            continue
        try:
            chunk = os.read(fd, min(65536, MAX_BYTES - len(data) + 1))
        except BlockingIOError:
            if stopped_ns is not None:
                break
            continue
        if not chunk:
            if stopped_ns is not None:
                break
            raise OSError('trace pipe closed unexpectedly')
        remaining = MAX_BYTES - len(data)
        if len(chunk) > remaining:
            data.extend(chunk[:remaining])
            truncated = True
        else:
            data.extend(chunk)
        lines += chunk.count(b'\n')
        if lines > MAX_EVENTS or truncated:
            truncated = True
            if stopped_ns is None:
                (instance / 'tracing_on').write_text('0')
                stopped_ns = time.monotonic_ns()
            break
    return bytes(data), truncated, stopped_ns


def identity_program(device):
    major, minor = map(int, device.split(':'))
    # Request pointers are map keys only; no address is emitted.
    return r'''
rawtracepoint:block_rq_issue {
  $r = (struct request *)arg0;
  if ($r != 0 && $r->q != 0 && $r->q->disk != 0 &&
      $r->q->disk->major == MAJOR && $r->q->disk->first_minor == MINOR &&
      ($r->cmd_flags & 255) == 0) {
    $t = nsecs(monotonic); @events = count(); printf("I %llu\n", $t);
    if (@pending[$r]) {
      @events = count(); printf("D %llu\n", $t); delete(@pending[$r]);
    } else {
      @pending[$r] = $t;
      if (!@pending[$r]) { @events = count(); printf("O %llu\n", $t); }
    }
  }
}
rawtracepoint:block_rq_requeue {
  $r = (struct request *)arg0;
  if ($r != 0 && $r->q != 0 && $r->q->disk != 0 &&
      $r->q->disk->major == MAJOR && $r->q->disk->first_minor == MINOR &&
      ($r->cmd_flags & 255) == 0) {
    @events = count(); printf("R %llu\n", nsecs(monotonic));
    if (@pending[$r]) { delete(@pending[$r]); }
  }
}
rawtracepoint:block_rq_complete {
  $r = (struct request *)arg0;
  if ($r != 0 && $r->q != 0 && $r->q->disk != 0 &&
      $r->q->disk->major == MAJOR && $r->q->disk->first_minor == MINOR &&
      ($r->cmd_flags & 255) == 0) {
    $t = nsecs(monotonic); $start = @pending[$r]; $bytes = (uint64)arg2;
    if (!$start) {
      @events = count(); printf("M %llu\n", $t);
    } else if (arg1 || !$bytes || !$r->__data_len || $bytes > $r->__data_len) {
      @events = count(); printf("E %llu\n", $t); delete(@pending[$r]);
    } else if ($bytes == $r->__data_len) {
      @events = count(); printf("C %llu %llu\n", $start, $t); delete(@pending[$r]);
    } else {
      @events = count(); printf("P %llu\n", $t);
    }
  }
}
END { clear(@pending); }
'''.replace('MAJOR', str(major)).replace('MINOR', str(minor))


def parse_identity(lines, started, stopped, *, truncated=False, diagnostics=''):
    issues = completions = partials = requeues = misses = errors = overflows = duplicates = 0
    intervals, raw = [], []
    emitted = 0
    expected = None
    malformed = 0
    for line in lines:
        if line.startswith('@events:'):
            try:
                expected = int(line.split(':', 1)[1].strip())
            except ValueError:
                malformed += 1
            continue
        if not line:
            continue
        parts = line.split()
        if not parts or parts[0] not in ('I', 'C', 'P', 'R', 'M', 'E', 'O', 'D'):
            malformed += 1
            continue
        emitted += 1
        if len(parts) != (3 if parts[0] == 'C' else 2) or not all(x.isdigit() for x in parts[1:]):
            malformed += 1
            continue
        kind, end = parts[0], int(parts[-1])
        if not started <= end <= stopped:
            continue
        raw.append(line)
        if kind == 'I':
            issues += 1
        elif kind == 'C':
            completions += 1
            begin = int(parts[1])
            if started <= begin <= end:
                intervals.append(end - begin)
            else:
                misses += 1
        elif kind == 'P':
            partials += 1
            completions += 1
        elif kind == 'M':
            misses += 1
            completions += 1
        elif kind == 'E':
            errors += 1
            completions += 1
        elif kind == 'R':
            requeues += 1
        elif kind == 'O':
            overflows += 1
        else:
            duplicates += 1
    lost = None if expected is None or diagnostics else max(0, expected - emitted)
    if expected is not None and emitted > expected:
        malformed += 1
    if lost != 0 or truncated or malformed:
        intervals = []
    intervals.sort()
    n = len(intervals)
    unmatched_issues = max(0, issues - n)
    total = sum(intervals)
    mean = total / n if n else None
    dist = {'count': n, 'sum': total, 'max': intervals[-1] if n else None,
            'mean': mean, 'population_stddev': math.sqrt(sum((x - mean) ** 2 for x in intervals) / n) if n else None,
            'tail_warnings': []}
    for name, numerator, minimum in (('p50', 50, 2), ('p95', 95, 20), ('p99', 99, 100), ('p999', 999, 1000)):
        denominator = 1000 if name == 'p999' else 100
        dist[name] = intervals[(numerator * n + denominator - 1) // denominator - 1] if n else None
        if n < minimum:
            dist['tail_warnings'].append(f'{name}: fewer than {minimum} observations')
    qualified = bool(issues and completions and n and lost == 0 and not truncated and
                     not (malformed or requeues or misses or errors or overflows or duplicates or unmatched_issues))
    return {'backend': 'bpftrace-request-identity', 'read_issues': issues, 'read_completions': completions,
            'matched_reads': n, 'intervals_ns': dist, 'raw_intervals_ns': intervals, 'raw_events': raw,
            'partial_completions': partials, 'requeues': requeues, 'unmatched_completions': misses,
            'unmatched_issues': unmatched_issues,
            'completion_errors': errors, 'outstanding_overflow': overflows, 'overlaps': duplicates,
            'malformed_events': malformed, 'lost': lost, 'truncated': truncated, 'qualified': qualified,
            'trace_clock': 'mono', 'timestamp_unit_ns': 1, 'observed_timestamp_resolution_ns': None}


def attachment_marker_seen(stdout, stderr):
    marker = b'__BPFTRACE_NOTIFY_PROBES_ATTACHED'
    return marker in stdout or marker in stderr


def identity_unavailable(state, reason, **details):
    save(state / 'identity-unavailable.json',
         {'available': False, 'backend': 'bpftrace-request-identity',
          'reason': reason, **details})
    return None


def identity_capture(state, mapping, deadline, stop_requested):
    binary = shutil.which('bpftrace')
    if not binary or not Path('/sys/kernel/btf/vmlinux').is_file():
        return identity_unavailable(state, 'bpftrace binary or kernel BTF unavailable')
    # Enforce the possible-CPU bound even when some CPUs are currently offline.
    cpus = startup_ring_bound('trace_buf_size=4K',
                              Path('/sys/devices/system/cpu/possible').read_text(),
                              os.sysconf('SC_PAGE_SIZE'))['possible_cpus']
    env = {k: v for k, v in os.environ.items() if not k.startswith('BPFTRACE_')}
    env.update(BPFTRACE_MAX_MAP_KEYS=str(MAX_OUTSTANDING), BPFTRACE_PERF_RB_PAGES='8',
               __BPFTRACE_NOTIFY_PROBES_ATTACHED='1')
    parent_pid = os.getpid()
    def child_death_signal():
        if ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent_pid:
            os._exit(127)
    try:
        process = subprocess.Popen([binary, '-q', '-k', '-B', 'line', '-e', identity_program(mapping['device'])],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   env=env, close_fds=True, preexec_fn=child_death_signal)
    except (OSError, subprocess.SubprocessError) as error:
        return identity_unavailable(state, 'bpftrace could not start', error=str(error)[:500])
    selector = selectors.DefaultSelector()
    data, errors = bytearray(), bytearray()
    started = stopped = None
    truncated = False
    shutdown_deadline = None
    marker = b'__BPFTRACE_NOTIFY_PROBES_ATTACHED'
    try:
        for stream in (process.stdout, process.stderr):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
        while selector.get_map():
            now = time.monotonic()
            if stopped is None and (now >= deadline or stop_requested() or (state / 'stop').exists() or truncated):
                stopped = time.monotonic_ns()
                shutdown_deadline = now + 5
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
            if shutdown_deadline is not None and now >= shutdown_deadline and process.poll() is None:
                process.kill()
                truncated = True
                shutdown_deadline = None
            for key, _ in selector.select(0.1):
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                target, limit = (data, MAX_BYTES) if key.fileobj is process.stdout else (errors, 256 * 1024)
                remaining = limit - len(target)
                target.extend(chunk[:max(0, remaining)])
                if len(chunk) > remaining or data.count(b'\n') > MAX_EVENTS + 8:
                    truncated = True
                if started is None and attachment_marker_seen(data, errors) and not truncated and stopped is None:
                    started = time.monotonic_ns()
                    save(state / 'status.json', {'available': True, 'state_dir': str(state),
                                                 'device': mapping, 'backend': 'bpftrace-request-identity',
                                                 'started_monotonic_ns': started})
        process.wait()
        if started is None:
            return identity_unavailable(
                state, 'bpftrace did not confirm probe attachment',
                returncode=process.returncode,
                stderr=bytes(errors).replace(marker, b'').decode('utf-8', errors='replace')[:4096])
        if stopped is None:
            stopped = time.monotonic_ns()
        if process.returncode != 0:
            return {'available': False, 'state_dir': str(state), 'reason': 'bpftrace terminated unsuccessfully'}
        diagnostics = bytes(errors).replace(marker, b'').strip()
        parsed = parse_identity(bytes(data).replace(marker, b'').decode('utf-8', errors='replace').splitlines(),
                                started, stopped, truncated=truncated, diagnostics=diagnostics)
        return {'available': True, 'state_dir': str(state), 'device': mapping,
                'started_monotonic_ns': started, 'stopped_monotonic_ns': stopped,
                'scope': 'device-wide request-identity block issue-to-completion; not application or NAND service',
                'possible_cpus': cpus, 'perf_ring_pages_per_cpu': 8, **parsed}
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()
        process.stderr.close()


def worker(state, wal_path, duration, instance_name):
    instance = TRACE_ROOT / 'instances' / instance_name
    created = False
    fd = None
    result = {'available': False, 'reason': 'collector did not start'}
    deadline = time.monotonic() + duration
    terminated = False
    def terminate(_signum, _frame):
        nonlocal terminated
        terminated = True
    previous_handler = signal.signal(signal.SIGTERM, terminate)
    try:
        if not re.fullmatch(r'tidehunter-[0-9a-f]{32}', instance_name):
            raise ValueError('invalid owned instance name')
        mapping = resolve_device(wal_path)
        identity = identity_capture(state, mapping, deadline, lambda: terminated)
        if identity is not None:
            result = identity
            return
        startup_bound = startup_ring_bound(Path('/proc/cmdline').read_text(),
                                           Path('/sys/devices/system/cpu/possible').read_text(),
                                           os.sysconf('SC_PAGE_SIZE'))
        instance.mkdir()
        created = True
        (instance / 'tracing_on').write_text('0')
        (instance / 'buffer_size_kb').write_text(str(BUFFER_KB))
        if int((instance / 'buffer_total_size_kb').read_text().strip()) > BUFFER_KB * MAX_CPUS:
            raise ValueError('trace ring exceeds total byte limit')
        (instance / 'trace_clock').write_text('mono')
        encoded = (int(mapping['device'].split(':')[0]) << 20) | int(mapping['device'].split(':')[1])
        for name in ('block_rq_issue', 'block_rq_complete', 'block_rq_requeue'):
            event = instance / 'events' / 'block' / name
            fmt = (event / 'format').read_text()
            if any(not re.search(r'\b' + field + r'(?:\[\d+\])?\s*;', fmt) for field in ('dev', 'sector', 'nr_sector', 'rwbs')):
                raise ValueError('unsupported block tracepoint format')
            (event / 'filter').write_text(f'dev == {encoded}')
            (event / 'enable').write_text('1')
        (instance / 'trace').write_text('')
        fd = os.open(instance / 'trace_pipe', os.O_RDONLY | os.O_NONBLOCK)
        if terminated or time.monotonic() >= deadline or (state / 'stop').exists():
            raise ValueError('collector deadline elapsed before tracing started')
        started = time.monotonic_ns()
        (instance / 'tracing_on').write_text('1')
        save(state / 'status.json', {'available': True, 'state_dir': str(state), 'device': mapping,
                                    'started_monotonic_ns': started})
        data, truncated, stopped = drain_trace(fd, instance, state, deadline, lambda: terminated)
        lost = lost_events(instance)
        parsed = parse_trace(data.decode('utf-8', errors='replace'), mapping['device'],
                             truncated=truncated, lost=lost)
        result = {'available': True, 'state_dir': str(state), 'device': mapping,
                  'startup_ring_bound': startup_bound,
                  'started_monotonic_ns': started, 'stopped_monotonic_ns': stopped,
                  'scope': 'device-wide block events; candidate sector/length correlation lacks request identity and known boundary quiescence; not application or NAND service', **parsed}
        result['qualified'] = False
        result['qualification_reason'] = 'request identity and boundary in-flight state unavailable'
    except (OSError, ValueError) as error:
        result = {'available': False, 'state_dir': str(state), 'reason': f'{type(error).__name__}: {error}'[:500]}
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError as error:
                result['cleanup_error'] = str(error)[:500]
                result['available'] = False
                result['qualified'] = False
        if created:
            try:
                (instance / 'tracing_on').write_text('0')
            except OSError as error:
                result['cleanup_error'] = str(error)[:500]
                result['available'] = False
                result['qualified'] = False
            try:
                instance.rmdir()
            except OSError as error:
                result['cleanup_error'] = str(error)[:500]
                result['available'] = False
                result['qualified'] = False
        try:
            identity_error = state / 'identity-unavailable.json'
            if identity_error.is_file():
                result['identity_attempt'] = json.loads(identity_error.read_text())
        except (OSError, ValueError) as error:
            result['identity_attempt'] = {
                'available': False, 'reason': 'identity failure details unavailable',
                'error': str(error)[:500]}
        save(state / 'result.json', result)
        if not (state / 'status.json').exists():
            save(state / 'status.json', result)
        signal.signal(signal.SIGTERM, previous_handler)


def start(wal_path, state_dir, duration_seconds):
    if not 0 < duration_seconds <= MAX_DURATION:
        raise ValueError('duration must be in (0, 3600] seconds')
    state = Path(state_dir).absolute()
    state.mkdir(mode=0o700)  # exclusive ownership: never adopt an existing directory
    instance_name = 'tidehunter-' + secrets.token_hex(16)
    save(state / 'owner.json', {'state_dir': str(state), 'instance': instance_name})
    try:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '_worker', '--wal-path', str(wal_path),
                          '--state-dir', str(state), '--duration-seconds', str(duration_seconds),
                          '--instance-name', instance_name], start_new_session=True,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         close_fds=True)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if (state / 'status.json').exists():
                return json.loads((state / 'status.json').read_text())
            time.sleep(0.05)
        (state / 'stop').touch()
        return {'available': False, 'state_dir': str(state), 'reason': 'collector startup timed out'}
    except OSError as error:
        result = {'available': False, 'state_dir': str(state), 'reason': str(error)[:500]}
        save(state / 'result.json', result)
        save(state / 'status.json', result)
        return result


def stop(state_dir):
    state = Path(state_dir).absolute()
    owner = json.loads((state / 'owner.json').read_text())
    if owner.get('state_dir') != str(state) or not re.fullmatch(r'tidehunter-[0-9a-f]{32}', owner.get('instance', '')):
        raise ValueError('invalid collector state directory')
    if not (state / 'result.json').exists():
        (state / 'stop').touch()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if (state / 'result.json').exists():
            return json.loads((state / 'result.json').read_text())
        time.sleep(0.05)
    return {'available': False, 'state_dir': str(state), 'reason': 'collector stop timed out'}


def main():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest='command', required=True)
    for name in ('start', '_worker'):
        p = subs.add_parser(name)
        p.add_argument('--wal-path', required=True)
        p.add_argument('--state-dir', required=True)
        p.add_argument('--duration-seconds', type=float, required=True)
        if name == '_worker':
            p.add_argument('--instance-name', required=True)
    p = subs.add_parser('stop')
    p.add_argument('--state-dir', required=True)
    args = parser.parse_args()
    try:
        if args.command == '_worker':
            worker(Path(args.state_dir), args.wal_path, args.duration_seconds, args.instance_name)
            return
        result = start(args.wal_path, args.state_dir, args.duration_seconds) if args.command == 'start' else stop(args.state_dir)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        result = {'available': False, 'reason': f'{type(error).__name__}: {error}'[:500]}
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
