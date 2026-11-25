#!/usr/bin/env python3
"""One `/proc` sampler for every campaign driver.

`manual_sui.py` samples each owner process group during a Sui replay;
`stage3_campaign.py` samples a cell's process and the hosts that ran or owned
it. Both read the same `@@`-delimited blocks and write the same evidence: one
host row (swap and device bytes) and one row per watched process (CPU, RSS,
VmHWM, swap, io counters). `parse_proc` returns `utime` and `stime` (fields 14
and 15 of the same `/proc/<pid>/stat` line) as ticks and as `cpu_seconds`.
"""
import csv
import json
from pathlib import Path
import os
import socket
import threading
import time

SAMPLE_SECONDS = 2.0
MAX_SAMPLES = 20000
IO_COUNTERS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes',
               'cancelled_write_bytes')

def parse_proc(stat, status, io):
    """One live process sample; a missing stream is an exited process."""
    if not stat or not status or not io:
        return None
    try:
        fields = stat.rsplit(b')', 1)[1].split()
        starttime = int(fields[19])
        # Field 14 is utime and field 15 is stime; after the `)` split the
        # state is element 0, so field n is element n - 3.
        utime, stime = int(fields[11]), int(fields[12])
        values = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in
                  status.decode().splitlines() if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))}
        counters = {line.split(':')[0]: int(line.split()[1]) for line in
                    io.decode().splitlines()}
    except (IndexError, ValueError, UnicodeError):
        return None
    if set(values) != {'VmRSS', 'VmHWM', 'VmSwap'} or not set(IO_COUNTERS) <= set(counters):
        return None
    return dict(starttime_ticks=starttime, utime_ticks=utime, stime_ticks=stime,
                cpu_seconds=(utime + stime) / CLOCK_TICKS, **values,
                io={key: counters[key] for key in IO_COUNTERS})


RESOURCE_COLUMNS = ('case', 'host', 'role', 'pid', 'unix_ns', 'exited', 'cpu_seconds',
                    'rss_bytes', 'hwm_bytes', 'swap_bytes', 'rchar', 'wchar', 'syscr',
                    'syscw', 'read_bytes', 'write_bytes', 'swap_in', 'swap_out',
                    'device_read_bytes', 'device_write_bytes')
CLOCK_TICKS = os.sysconf('SC_CLK_TCK')
SECTOR_BYTES = 512


def proc_blocks(text):
    """`@@name`-delimited blocks, the format manual_sui's sampler emits."""
    blocks, current = {}, None
    for line in text.splitlines():
        if line.startswith('@@'):
            current = line[2:].strip()
            blocks[current] = []
        elif current is not None:
            blocks[current].append(line)
    return blocks


def sample_script(pids):
    script = ['echo @@vmstat', 'cat /proc/vmstat',
              'echo @@blk', 'for d in /sys/block/*; do echo "$(basename $d) $(cat $d/stat)"; done']
    for pid in pids:
        script.extend([f'echo @@{pid}', f'cat /proc/{pid}/stat 2>/dev/null',
                       f'cat /proc/{pid}/status 2>/dev/null',
                       f'cat /proc/{pid}/io 2>/dev/null'])
    # A process that exited mid-cell is a normal sample, not a sampling failure.
    script.append('true')
    return '; '.join(script)


def block_rows(blocks, roles, host, unix_ns):
    """One sample: a host row plus one row per watched process."""
    rows = []
    vmstat = dict(line.split() for line in blocks.get('vmstat', []) if len(line.split()) == 2)
    read_bytes = write_bytes = 0
    for line in blocks.get('blk', []):
        # `<device> <18 stat fields>`: sectors read is field 3, written is 7.
        fields = line.split()
        if len(fields) < 8:
            continue
        read_bytes += int(fields[3]) * SECTOR_BYTES
        write_bytes += int(fields[7]) * SECTOR_BYTES
    rows.append(dict(case=None, host=host, role='host', pid='', unix_ns=unix_ns,
                     exited=False, cpu_seconds=None, rss_bytes=None, hwm_bytes=None,
                     swap_bytes=None, rchar=None, wchar=None, syscr=None, syscw=None,
                     read_bytes=None, write_bytes=None,
                     swap_in=int(vmstat.get('pswpin', 0)),
                     swap_out=int(vmstat.get('pswpout', 0)),
                     device_read_bytes=read_bytes, device_write_bytes=write_bytes))
    for pid, role in roles.items():
        raw = blocks.get(str(pid), [])
        stat = next((line for line in raw if ') ' in line), '')
        status = [line for line in raw if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))]
        counters = [line for line in raw if line.split(':')[0] in IO_COUNTERS]
        entry = parse_proc((stat + '\n').encode(),
                           ('\n'.join(status) + '\n').encode(),
                           ('\n'.join(counters) + '\n').encode())
        row = dict(case=None, host=host, role=role, pid=pid, unix_ns=unix_ns,
                   exited=entry is None,
                   cpu_seconds=entry['cpu_seconds'] if entry else None,
                   swap_in=None, swap_out=None, device_read_bytes=None,
                   device_write_bytes=None)
        if entry is None:
            row.update(rss_bytes=None, hwm_bytes=None, swap_bytes=None, rchar=None,
                       wchar=None, syscr=None, syscw=None, read_bytes=None, write_bytes=None)
        else:
            row.update(rss_bytes=entry['VmRSS'], hwm_bytes=entry['VmHWM'],
                       swap_bytes=entry['VmSwap'], rchar=entry['io']['rchar'],
                       wchar=entry['io']['wchar'], syscr=entry['io']['syscr'],
                       syscw=entry['io']['syscw'], read_bytes=entry['io']['read_bytes'],
                       write_bytes=entry['io']['write_bytes'])
        rows.append(row)
    return rows


class Sampler:
    """Per-host `/proc` samples during one cell, in manual_sui's sample format."""

    def __init__(self, remote=None, *, interval=None):
        self.remote = remote
        self.interval = SAMPLE_SECONDS if interval is None else interval
        self.local = {}
        self.hosts = {}
        self.samples = []
        self.error = None
        self._stop = threading.Event()
        self._thread = None

    def watch_local(self, pid, role):
        self.local[int(pid)] = role

    def watch_host(self, host, pid, role):
        self.hosts.setdefault(host, {})[int(pid)] = role

    def _sample(self):
        stamp = time.time_ns()
        if self.local:
            blocks = proc_blocks(_read_local(list(self.local)))
            self.samples.extend(block_rows(blocks, self.local, socket.gethostname(), stamp))
        for host, roles in self.hosts.items():
            if self.remote is None:
                self.error = f'{host}: no transport for host sampling'
                continue
            code, out, err = self.remote.shell(
                host, sample_script(list(roles)), check=False)
            if code != 0 and not out.strip():
                self.error = f'{host}: sampling failed: {err.decode(errors="replace")[-500:]}'
                continue
            self.samples.extend(block_rows(proc_blocks(out.decode()), roles, host, stamp))

    def start(self):
        self._sample()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def _loop(self):
        while not self._stop.wait(self.interval):
            try:
                self._sample()
            except BaseException as error:  # a dead transport is not a healthy sample
                self.error = str(error)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        try:
            self._sample()
        except BaseException as error:
            self.error = str(error)
        return self

    # ---- evidence ------------------------------------------------------
    def write(self, output, *, case, mode, label):
        """`resources.csv` per sample and a compact `resources.json` per cell."""
        output = Path(output)
        output.mkdir(parents=True, exist_ok=True)
        path = output / 'resources.csv'
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=RESOURCE_COLUMNS)
            writer.writeheader()
            for row in self.samples:
                writer.writerow(dict(row, case=case))
        hosts = {}
        for row in self.samples:
            entry = hosts.setdefault(row['host'], dict(
                samples=0, device_read_bytes=None, device_write_bytes=None,
                swap_in=None, swap_out=None, processes={}))
            if row['role'] == 'host':
                entry['samples'] += 1
                entry['device_read_bytes'] = row['device_read_bytes']
                entry['device_write_bytes'] = row['device_write_bytes']
                entry['swap_in'] = row['swap_in']
                entry['swap_out'] = row['swap_out']
                if 'first_device_read_bytes' not in entry:
                    entry['first_device_read_bytes'] = row['device_read_bytes']
                    entry['first_device_write_bytes'] = row['device_write_bytes']
                    entry['first_swap_in'] = row['swap_in']
                    entry['first_swap_out'] = row['swap_out']
            else:
                process = entry['processes'].setdefault(row['role'], dict(
                    pid=row['pid'], cpu_seconds=None, rss_bytes=None, hwm_bytes=None,
                    swap_bytes=None, exited=False))
                process.update(pid=row['pid'], exited=row['exited'])
                for name in ('cpu_seconds', 'rss_bytes', 'hwm_bytes', 'swap_bytes'):
                    if row[name] is not None:
                        process[name] = row[name]
        for entry in hosts.values():
            entry['device_read_bytes'] = (entry.pop('device_read_bytes', 0) or 0) - (
                entry.pop('first_device_read_bytes', 0) or 0)
            entry['device_write_bytes'] = (entry['device_write_bytes'] or 0) - (
                entry.pop('first_device_write_bytes', 0) or 0)
            entry['swap_in'] = (entry['swap_in'] or 0) - (entry.pop('first_swap_in', 0) or 0)
            entry['swap_out'] = (entry['swap_out'] or 0) - (entry.pop('first_swap_out', 0) or 0)
        totals = sample_totals(output / 'samples.csv')
        device_write = sum(entry['device_write_bytes'] for entry in hosts.values())
        value = dict(case=case, mode=mode, label=label, interval_seconds=self.interval,
                     hosts=hosts, error=self.error, payload_bytes=totals['payload_bytes'],
                     device_write_bytes=device_write,
                     device_write_per_payload=(device_write / totals['payload_bytes']
                                               if totals['payload_bytes'] else None),
                     device_bytes_source=('summed /sys/block/*/stat deltas over the '
                                          'hosts that ran or owned the cell'),
                     path=str(path))
        (output / 'resources.json').write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
        return value



def _read_local(pids):
    """The same `@@` blocks, read from this host without a shell."""
    lines = ['@@vmstat'] + Path('/proc/vmstat').read_text().splitlines()
    lines += ['@@blk']
    for device in sorted(Path('/sys/block').glob('*')):
        try:
            lines.append(f'{device.name} {(device / "stat").read_text().strip()}')
        except OSError:
            continue
    for pid in pids:
        lines.append(f'@@{pid}')
        for name in ('stat', 'status', 'io'):
            try:
                lines.extend(Path(f'/proc/{pid}/{name}').read_text().splitlines())
            except OSError:
                continue
    return '\n'.join(lines)


def sample_host(remote, host, namespaces):
    """One owner-host sample in the `@@vmstat`/`@@diskstats` block format."""
    script = ['echo @@vmstat; cat /proc/vmstat', 'echo @@diskstats; cat /proc/diskstats']
    for namespace in namespaces:
        pid = namespace['pid']
        script.extend([
            f'echo @@{pid}', f'cat /proc/{pid}/stat 2>/dev/null',
            f'cat /proc/{pid}/status 2>/dev/null', f'cat /proc/{pid}/io 2>/dev/null'])
    code, out, err = remote.shell(host, '; '.join(script), check=False)
    stamp = time.time_ns()
    if code != 0:
        return dict(unix_ns=stamp, healthy=False,
                    reason=err.decode(errors='replace'))
    blocks, current = {}, None
    for line in out.decode().splitlines():
        if line.startswith('@@'):
            current = line[2:].strip()
            blocks[current] = []
        elif current is not None:
            blocks[current].append(line)
    processes, healthy = [], True
    for namespace in namespaces:
        pid = namespace['pid']
        raw = blocks.get(str(pid), [])
        stat = next((line for line in raw if ') ' in line), '')
        status = [line for line in raw if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))]
        io = [line for line in raw if line.split(':')[0] in IO_COUNTERS]
        entry = parse_proc((stat + '\n').encode(),
                           ('\n'.join(status) + '\n').encode(),
                           ('\n'.join(io) + '\n').encode())
        if entry is None:
            healthy = False
            processes.append(dict(pid=pid, index=namespace['index'], exited=True))
            continue
        if namespace.setdefault('starttime_ticks', entry['starttime_ticks']) != entry['starttime_ticks']:
            healthy = False
        processes.append(dict(pid=pid, index=namespace['index'], exited=False,
                              unavailable={}, **entry))
    vmstat = dict(line.split() for line in blocks.get('vmstat', []))
    swap = ({key: int(vmstat[key]) for key in ('pswpin', 'pswpout')}
            if vmstat else None)
    return dict(unix_ns=stamp, healthy=healthy, processes=processes, swap=swap,
                diskstats='\n'.join(blocks.get('diskstats', [])))


def host_evidence(hosts, namespaces_by_host, samples_by_host, binding, *,
                  binary_sha256, guest_root):
    """Per-host resource evidence for `run.py --external-resources`.

    `hosts` is the external manifest's host list; every host carries its owner
    namespaces, the per-sample rows, and the first and last swap and diskstats
    snapshots the runner's contract reads.
    """
    rows = {}
    for record in hosts:
        host = record['host']
        namespaces = namespaces_by_host.get(host, [])
        samples = samples_by_host.get(host, [])
        with_swap = [row for row in samples if row.get('swap')]
        before = with_swap[0] if with_swap else None
        after = with_swap[-1] if with_swap else None
        healthy = [row for row in samples if row['healthy']]
        rows[host] = dict(binding,
            storage_binary_sha256=binary_sha256,
            root=str(guest_root),
            complete=(len(namespaces) == len(record['commands'])
                      and all(n.get('stopped') for n in namespaces)),
            cleanup_complete=True,
            all_namespaces_healthy=bool(healthy) and all(
                len(row['processes']) == len(namespaces) for row in healthy),
            failure=None, samples_dropped=0,
            namespaces=[dict(index=n['index'], command=n['command'], pid=n['pid'],
                             process_group=n['process_group'],
                             starttime_ticks=n.get('starttime_ticks', 0),
                             reaped=bool(n.get('stopped')))
                        for n in namespaces],
            swap_before=before['swap'] if before else None,
            diskstats_before=before['diskstats'] if before else None,
            swap_after=after['swap'] if after else None,
            diskstats_after=after['diskstats'] if after else None,
            samples=[dict(unix_ns=row['unix_ns'], healthy=row['healthy'],
                          processes=row.get('processes', [])) for row in samples])
    return dict(binding, hosts=rows)


def sample_totals(path):
    """Measurement-phase calls, window and payload bytes of one samples.csv."""
    calls, payload, first, last = 0, 0, None, None
    path = Path(path)
    if path.is_file():
        with path.open(newline='') as stream:
            for row in csv.DictReader(stream):
                if row.get('phase') != 'measurement':
                    continue
                calls += 1
                try:
                    payload += int(row.get('bytes') or 0)
                    completed = int(row['completed_ns'])
                except (KeyError, ValueError):
                    continue
                first = completed if first is None else min(first, completed)
                last = completed if last is None else max(last, completed)
    window = ((last - first) / 1e9) if first is not None and last is not None else 0.0
    return dict(calls=calls, window_seconds=window, payload_bytes=payload,
                calls_per_second=(calls / window) if window > 0 else None)
