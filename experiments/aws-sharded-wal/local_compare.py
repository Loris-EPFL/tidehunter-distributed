#!/usr/bin/env python3
"""Interleave preserved baseline/candidate benchmarks; no AWS operations."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time


def swap():
    return {k: int(v) for k, v in (line.split() for line in Path('/proc/vmstat').read_text().splitlines())
            if k in ('pswpin', 'pswpout')}


def cgroup_memory():
    """Collector's inherited cgroup, not isolated or additive benchmark RSS."""
    membership = Path('/proc/self/cgroup').read_text()
    unified = next((line.split(':', 2)[2] for line in membership.splitlines()
                    if line.startswith('0::')), None)
    result = {'membership': membership, 'scope': 'collector_inherited_group_including_other_members'}
    if unified is None:
        return result | {'unavailable': 'no unified cgroup-v2 membership'}
    root = Path('/sys/fs/cgroup') / unified.lstrip('/')
    for name in ('memory.current', 'memory.max', 'memory.stat', 'memory.events',
                 'memory.swap.current', 'memory.swap.max'):
        try:
            result[name] = (root / name).read_text()
        except OSError as error:
            result[name] = {'unavailable_errno': error.errno}
    return result


def host_snapshot():
    return {name: Path('/proc', name).read_text() for name in ('meminfo', 'stat')} | {
        'cgroup_memory': cgroup_memory(),
        'scope': 'shared_host_including_other_processes; stat includes host context switches'}


def tree(pid):
    todo, found = [pid], {}
    while todo:
        item = todo.pop()
        if item in found:
            continue
        try:
            status = dict(line.split(':', 1) for line in Path(f'/proc/{item}/status').read_text().splitlines())
            found[item] = int(status.get('VmRSS', '0 kB').split()[0])
            for tasks in Path(f'/proc/{item}/task').glob('*/children'):
                todo.extend(int(x) for x in tasks.read_text().split())
        except (OSError, ProcessLookupError):
            continue
    return found


def run(command, log):
    before = swap()
    host_before = host_snapshot()
    peaks = {}
    aggregate, process_count = 0, 0
    started = time.monotonic()
    stamp = datetime.now(timezone.utc).isoformat()
    with log.with_suffix('.stdout.log').open('w') as out, log.with_suffix('.stderr.log').open('w') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err)
        while child.poll() is None:
            sample = tree(child.pid)
            aggregate = max(aggregate, sum(sample.values()))
            process_count = max(process_count, len(sample))
            for pid, rss in sample.items():
                peaks[pid] = max(peaks.get(pid, 0), rss)
            time.sleep(.05)
        code = child.wait()
    after = swap()
    host_after = host_snapshot()
    record = {'command': command, 'started_utc': stamp, 'elapsed_seconds': time.monotonic() - started,
              'exit_code': code, 'sampling_seconds': .05,
              'peak_sampled_process_tree_rss_kib': aggregate, 'max_sampled_process_count': process_count,
              'per_process_peak_rss_kib': peaks, 'swap_delta_pages': {k: after[k] - before[k] for k in before},
              'host_before': host_before, 'host_after': host_after,
              'remaining_sampled_pids': [p for p in peaks if Path(f'/proc/{p}').exists()],
              'note': 'RSS is sampled, sums shared pages per process; swap is system-wide. Not a dedicated host.'}
    log.with_suffix('.resources.json').write_text(json.dumps(record, indent=2) + '\n')
    if code:
        raise RuntimeError(f'benchmark failed with exit {code}: {log}')
    print(json.dumps({k: v for k, v in record.items()
                      if k not in ('per_process_peak_rss_kib', 'host_before', 'host_after')}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--windows', default='1,8,32')
    parser.add_argument('--backend', default='all')
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 5:
        raise ValueError('bounded 1..5 repetitions only')
    args.output.mkdir()
    binaries = {name: getattr(args, name).resolve() for name in ['baseline', 'candidate']}
    hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
    (args.output / 'binary-hashes.json').write_text(json.dumps(hashes, indent=2) + '\n')
    for rep in range(args.repetitions):
        order = ['baseline', 'candidate'] if rep % 2 == 0 else ['candidate', 'baseline']
        for name in order:
            if hashlib.sha256(binaries[name].read_bytes()).hexdigest() != hashes[name]:
                raise ValueError('binary changed during campaign')
            case = args.output / f'{name}-r{rep}'
            command = [str(binaries[name]), '--output', str(case), '--backend', args.backend,
                       '--repetitions', '1', '--windows', args.windows]
            run(command, case)
            if not (case / 'COMPLETE').exists():
                raise RuntimeError('missing full correctness/reopen completion receipt')
    (args.output / 'COMPLETE').write_text('all selected baseline/candidate runs passed\n')


if __name__ == '__main__':
    main()
