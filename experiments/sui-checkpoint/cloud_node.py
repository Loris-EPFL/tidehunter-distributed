#!/usr/bin/env python3
"""Own one fresh checkpoint matrix cell payload, never provision or discover devices.

Run with --config node-config.json. The controller supplies the absolute deadline,
cell/transport identity, source hashes, storage-node argv and ready/stop files. Only a controller
stop after readiness is successful; signals, expiry and any early child exit fail.
Atomic resources.json snapshots are live evidence (complete=false), then final
cleanup evidence. Host swap/disk counters are explicitly host-wide observations,
not storage-node attribution; per-process /proc counters remain separate.
"""
import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time

import yaml

OWNED_BASE = Path('/mnt/tidehunter')
MAX_NAMESPACES = 16
MAX_SECONDS = 7200
MAX_SAMPLES = 2048
SAMPLE_SECONDS = 2.0
POLL_SECONDS = 0.1
CLEANUP_RESERVE = 5.0
KILL_RESERVE = 1.0


def bounded_text(path, limit):
    with path.open('r') as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError('input exceeds bounded size')
    return value


def owned_path(value, owner, kind):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('invalid owned path')
    path = Path(value)
    if (not path.is_absolute() or str(path) != value or '..' in path.parts
            or path == owner or owner not in path.parents):
        raise ValueError('path is not within exact owned root')
    for component in (path, *path.parents):
        if component.is_symlink():
            raise ValueError('owned paths must not traverse symlinks')
    if kind == 'file' and not path.is_file():
        raise ValueError('required owned file is missing')
    if kind == 'directory' and not path.is_dir():
        raise ValueError('required owned directory is missing')
    if kind == 'absent' and path.exists():
        raise ValueError('owned output or process root is not fresh')
    return path


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_config(config_path):
    cfg = json.loads(bounded_text(config_path, 128 * 1024))
    if not isinstance(cfg, dict) or not re.fullmatch(r'th-[0-9a-f]{16}', cfg.get('run_id', '')):
        raise ValueError('invalid owned run ID')
    owner = OWNED_BASE / cfg['run_id']
    transport = cfg.get('transport')
    if (transport not in ('tcp', 'efa', 'efa-direct', 'efa-native') or not isinstance(cfg.get('cell_id'), str)
            or not re.fullmatch(r'r0[1-4]-' + transport + (r'(?:-rma)?' if transport == 'efa-native' else ''), cfg['cell_id'])):
        raise ValueError('invalid matrix cell/transport identity')
    root = owner / 'checkpoint-cells' / cfg['cell_id'] / 'storage'
    if cfg.get('root') != str(root):
        raise ValueError('root must be the exact owned matrix cell storage directory')
    owned_path(str(root), owner, 'directory')
    owned_path(str(config_path), owner, 'file')
    if not isinstance(cfg.get('deadline_utc'), str):
        raise ValueError('absolute UTC deadline is required')
    deadline = datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00'))
    if deadline.utcoffset() != timezone.utc.utcoffset(deadline):
        raise ValueError('deadline must explicitly use UTC')
    now = time.time()
    monotonic_now = time.monotonic()
    left = deadline.timestamp() - now
    if not CLEANUP_RESERVE < left <= MAX_SECONDS:
        raise ValueError('deadline expired, lacks cleanup reserve, or exceeds two hours')
    for key in ('storage_binary_sha256', 'build_manifest_sha256', 'config_sha256'):
        if not isinstance(cfg.get(key), str) or not re.fullmatch(r'[0-9a-f]{64}', cfg[key]):
            raise ValueError('source/config hashes must be lowercase SHA256')
    if (not isinstance(cfg.get('checkpoint_digest'), str)
            or not re.fullmatch(r'[1-9A-HJ-NP-Za-km-z]{32,64}', cfg['checkpoint_digest'])):
        raise ValueError('checkpoint digest must be base58')
    commands = cfg.get('commands')
    if not isinstance(commands, list) or not 1 <= len(commands) <= MAX_NAMESPACES:
        raise ValueError('commands must cover 1..16 namespaces')
    for key in ('ready_files', 'stop_files'):
        if not isinstance(cfg.get(key), list) or len(cfg[key]) != len(commands):
            raise ValueError('each command requires matching ready/stop files')
    binary = owned_path(str(root / 'storage-node'), root, 'file')
    if not os.access(binary, os.X_OK):
        raise ValueError('storage binary is not executable')
    inputs = {config_path, binary}
    outputs = [owned_path(cfg.get(key), owner, 'absent')
               for key in ('resource_path', 'ready_path', 'stop_path')]
    receipt = owned_path(str(root / 'cloud-node-receipt.json'), root, 'absent')
    logs = owned_path(str(root / 'cloud-node-logs'), root, 'absent')
    temporaries = [owned_path(str(path.with_name(path.name + '.cloud-node-tmp')), owner, 'absent')
                   for path in (*outputs[:2], receipt)]
    outputs.extend((receipt, logs))
    outputs.extend(temporaries)
    namespaces, storage_roots, launches = [], [], []
    for index, command in enumerate(commands):
        if (not isinstance(command, list) or len(command) != 3
                or command[:2] != [str(binary), '--experimental-storage-node']):
            raise ValueError('only current storage-node launch commands are allowed')
        launch = owned_path(command[2], root, 'file')
        if launch.name != 'launch.yaml' or launch in launches:
            raise ValueError('each namespace requires a distinct launch.yaml')
        launches.append(launch)
        try:
            document = yaml.safe_load(bounded_text(launch, 2 * 1024 * 1024))
        except yaml.YAMLError:
            raise ValueError('invalid storage launch YAML') from None
        if not isinstance(document, dict) or not isinstance(document.get('node'), dict):
            raise ValueError('missing storage node configuration')
        data = owned_path(document['node'].get('root'), root, 'absent')
        ready = owned_path(cfg['ready_files'][index], root, 'absent')
        stop = owned_path(cfg['stop_files'][index], root, 'absent')
        if (data != launch.parent / 'storage' or ready != launch.parent / 'ready'
                or stop != launch.parent / 'stop' or document.get('ready_file') != str(ready)
                or document.get('stop_file') != str(stop)):
            raise ValueError('namespace storage/readiness/stop paths differ from launch')
        tls = document.get('tls')
        if not isinstance(tls, dict) or tls.get('transport') != transport:
            raise ValueError('storage TLS transport differs from matrix cell')
        if transport == 'efa-native':
            if any(tls.get(key) != '' for key in ('ca', 'cert', 'key')):
                raise ValueError('native transport requires explicit empty credentials')
        else:
            for key in ('ca', 'cert', 'key'):
                inputs.add(owned_path(tls.get(key), root, 'file'))
        inputs.add(launch)
        outputs.extend((ready, stop))
        storage_roots.append(data)
        namespaces.append(dict(index=index, command=command, launch=str(launch),
                               storage_root=str(data), ready=str(ready), stop=str(stop)))
    if len(set(outputs + storage_roots)) != len(outputs + storage_roots):
        raise ValueError('owned paths must not alias')
    for output in outputs + storage_roots:
        for other in inputs | set(outputs + storage_roots):
            if output == other and other in inputs:
                raise ValueError('output aliases an input')
            if output in other.parents or other in output.parents:
                raise ValueError('owned inputs, outputs and process roots must not overlap')
        if not output.parent.is_dir():
            raise ValueError('owned output parent directory is missing')
    observed = sha256(binary)
    if observed != cfg['storage_binary_sha256']:
        raise ValueError('storage binary does not match source-bound hash')
    # Monotonic time also bounds the run if the wall clock steps backwards.
    cfg.update(deadline_unix=deadline.timestamp(), deadline_monotonic=monotonic_now + left,
               namespaces=namespaces, node_config_sha256=sha256(config_path),
               receipt_path=str(receipt), logs_path=str(logs))
    return cfg


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.cloud-node-tmp')
    # Exclusive creation rejects stale files and symlinks rather than following them.
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, separators=(',', ':'), allow_nan=False)
            stream.write('\n')
        if path.is_symlink():
            raise ValueError('owned output became a symlink')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def exit_status(child):
    """Observe without reaping: reserve the leader PID until its group is killed."""
    if child.returncode is not None:
        return child.returncode
    info = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    if info is None:
        return None
    return info.si_status if info.si_code == os.CLD_EXITED else -info.si_status


def proc_stat(pid):
    text = bounded_text(Path(f'/proc/{pid}/stat'), 64 * 1024)
    fields = text[text.rindex(')') + 2:].split()
    return dict(state=fields[0], process_group=int(fields[2]), session=int(fields[3]),
                utime_ticks=int(fields[11]), stime_ticks=int(fields[12]),
                starttime_ticks=int(fields[19]))


def sample_process(record):
    child = record['child']
    row = dict(index=record['index'], pid=child.pid, unix_ns=time.time_ns(),
               starttime_ticks=record.get('starttime_ticks'), exited=False, unavailable={})
    code = exit_status(child)
    if code is not None:
        row.update(exited=True, exit_code=code)
    if record['reaped']:
        row['unavailable']['process'] = 'exited_and_reaped'
        return row
    try:
        observed = proc_stat(child.pid)
        if observed['starttime_ticks'] != record.get('starttime_ticks'):
            row['unavailable']['process'] = 'pid_identity_changed'
            return row
        row.update(cpu={key: observed[key] for key in ('utime_ticks', 'stime_ticks')},
                   state=observed['state'])
    except (OSError, ValueError, IndexError) as error:
        row['unavailable']['stat'] = type(error).__name__
        return row
    for name in ('status', 'io'):
        try:
            lines = bounded_text(Path(f'/proc/{child.pid}/{name}'), 64 * 1024).splitlines()
            if name == 'io':
                row['io'] = {line.split(':')[0]: int(line.split()[1]) for line in lines}
            else:
                row.update({line.split(':')[0]: int(line.split()[1]) * 1024 for line in lines
                            if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))})
                for key in ('VmRSS', 'VmHWM', 'VmSwap'):
                    if key not in row:
                        row['unavailable'][key] = 'not_reported'
        except (OSError, ValueError, IndexError) as error:
            row['unavailable'][name] = type(error).__name__
    return row


def host_sample():
    row = dict(unix_ns=time.time_ns(), scope='host-wide; not attributable to owned processes',
               swap_pages=None, diskstats=None, unavailable={})
    try:
        vmstat = dict(line.split() for line in bounded_text(Path('/proc/vmstat'), 256 * 1024).splitlines())
        row['swap_pages'] = {key: int(vmstat[key]) for key in ('pswpin', 'pswpout')}
    except (OSError, ValueError, KeyError) as error:
        row['unavailable']['swap_pages'] = type(error).__name__
    try:
        row['diskstats'] = bounded_text(Path('/proc/diskstats'), 1024 * 1024)
    except (OSError, ValueError) as error:
        row['unavailable']['diskstats'] = type(error).__name__
    return row


class NodeOwner:
    def __init__(self, cfg):
        self.cfg = cfg
        self.children = []
        self.samples = deque(maxlen=MAX_SAMPLES)
        self.total_samples = 0
        self.lock = threading.Lock()
        self.watchdog_stop = threading.Event()
        self.watchdog_expired = False
        self.received_signal = None
        self.failure = None
        self.claimed = False
        self.ready = False
        self.stopping = False
        self.stop_reason = None
        self.cleanup_complete = False
        self.started_ns = time.time_ns()
        self.finished_ns = None
        self.before = None
        self.after = None
        self.identities = {key: cfg[key] for key in ('run_id', 'root', 'deadline_utc',
            'storage_binary_sha256', 'build_manifest_sha256', 'config_sha256',
            'checkpoint_digest', 'node_config_sha256', 'cell_id', 'transport')}

    def remaining(self):
        return min(self.cfg['deadline_unix'] - time.time(),
                   self.cfg['deadline_monotonic'] - time.monotonic())

    def on_signal(self, signum, _frame):
        self.received_signal = signum

    def fail(self, reason):
        if self.failure is None:
            self.failure = reason

    def signal_groups(self, signum):
        with self.lock:
            for record in self.children:
                if not record['reaped']:
                    try:
                        os.killpg(record['child'].pid, signum)
                    except ProcessLookupError:
                        pass
                    except OSError as error:
                        self.fail('owned group signal failed: ' + type(error).__name__)

    def watchdog(self):
        # Independent of snapshot disk writes; reserve time to deliver SIGKILL
        # before expiry even if the sampler is blocked. Unreaped leader PIDs
        # prevent signalling a recycled unrelated process group.
        while not self.watchdog_stop.is_set():
            left = self.remaining() - KILL_RESERVE
            if left <= 0:
                self.watchdog_expired = True
                self.signal_groups(signal.SIGKILL)
                return
            self.watchdog_stop.wait(min(POLL_SECONDS, left))

    def check_running(self):
        if self.received_signal is not None:
            raise RuntimeError('received signal ' + str(self.received_signal))
        if self.watchdog_expired or self.remaining() <= CLEANUP_RESERVE:
            raise TimeoutError('absolute deadline cleanup reserve reached')
        for record in self.children:
            code = exit_status(record['child'])
            if code is not None:
                raise RuntimeError(f"namespace {record['index']} exited early with status {code}")
            for key in ('ready', 'stop'):
                path = Path(record[key])
                if path.is_symlink():
                    raise RuntimeError('namespace control path became a symlink')
            if Path(record['stop']).exists():
                raise RuntimeError('namespace stop was requested outside owner shutdown')
            if self.ready and not Path(record['ready']).is_file():
                raise RuntimeError('namespace readiness disappeared')
        stop = Path(self.cfg['stop_path'])
        if stop.is_symlink() or (stop.exists() and not stop.is_file()):
            raise RuntimeError('invalid owner stop path')
        if stop.exists():
            if not self.ready:
                raise RuntimeError('controller stopped before namespace readiness')
            return False
        return True

    def record_sample(self):
        row = dict(unix_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                   processes=[sample_process(record) for record in self.children])
        row['healthy'] = (self.ready and not self.stopping and len(self.children) == len(self.cfg['namespaces'])
                          and all(not proc['exited'] and 'process' not in proc['unavailable']
                                  and 'stat' not in proc['unavailable'] for proc in row['processes']))
        self.samples.append(row)
        self.total_samples += 1
        self.after = host_sample()
        if not self.stopping and any(proc['exited'] for proc in row['processes']):
            raise RuntimeError('namespace exited while sampling')

    def receipt(self):
        return dict(self.identities, schema=1, owner_pid=os.getpid(),
                    started_unix_ns=self.started_ns, finished_unix_ns=self.finished_ns,
                    started_unix=self.started_ns / 1e9,
                    finished_unix=None if self.finished_ns is None else self.finished_ns / 1e9,
                    ready=self.ready, stop_reason=self.stop_reason, failure=self.failure,
                    complete=self.finished_ns is not None and self.failure is None and self.cleanup_complete,
                    cleanup_complete=self.cleanup_complete,
                    expected_namespace_count=len(self.cfg['namespaces']),
                    namespaces=[{key: value for key, value in record.items() if key != 'child'}
                                for record in self.children])

    def publish(self):
        receipt = self.receipt()
        document = dict(receipt)
        document.update(samples=list(self.samples), sample_interval_seconds=SAMPLE_SECONDS,
                        sample_limit=MAX_SAMPLES, samples_dropped=self.total_samples - len(self.samples),
                        clock_ticks_per_second=os.sysconf('SC_CLK_TCK'),
                        healthy=bool(self.samples and self.samples[-1]['healthy'] and self.failure is None),
                        all_namespaces_healthy=self.ready and self.failure is None,
                        host_counter_scope='host-wide; no device ownership or per-workload attribution',
                        host_before=self.before, host_after=self.after,
                        swap_before=self.before['swap_pages'], swap_after=self.after['swap_pages'],
                        diskstats_before=self.before['diskstats'], diskstats_after=self.after['diskstats'])
        # Resources is the completion commit marker: a complete snapshot must
        # never become visible before its matching final receipt is written.
        atomic_json(self.cfg['receipt_path'], receipt)
        atomic_json(self.cfg['resource_path'], document)

    def launch(self):
        root = Path(self.cfg['root'])
        # Claim the payload once, before launching anything. A stale receipt or
        # logs directory rejects any attempt to silently reuse a process root.
        fd = os.open(self.cfg['receipt_path'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        self.claimed = True
        Path(self.cfg['logs_path']).mkdir(mode=0o700)
        self.before = host_sample()
        self.after = self.before
        self.publish()
        ready_end = time.monotonic() + min(30, self.remaining() - CLEANUP_RESERVE)
        for namespace in self.cfg['namespaces']:
            self.check_running()
            stdout_path = Path(self.cfg['logs_path']) / f"namespace-{namespace['index']:02d}.stdout.log"
            stderr_path = stdout_path.with_name(stdout_path.name.replace('.stdout.', '.stderr.'))
            with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
                child = subprocess.Popen(namespace['command'], stdin=subprocess.DEVNULL,
                    stdout=stdout, stderr=stderr, cwd=root, start_new_session=True, close_fds=True)
            record = dict(namespace, child=child, pid=child.pid, process_group=child.pid,
                          started_unix_ns=time.time_ns(), reaped=False,
                          stdout=str(stdout_path), stderr=str(stderr_path))
            with self.lock:
                self.children.append(record)
            if self.watchdog_expired or self.remaining() <= KILL_RESERVE:
                self.signal_groups(signal.SIGKILL)
            # Every successful Popen is registered before any fallible /proc IO.
            identity = proc_stat(child.pid)
            if identity['process_group'] != child.pid or identity['session'] != child.pid:
                raise RuntimeError('child did not acquire its own process group/session')
            record['starttime_ticks'] = identity['starttime_ticks']
            self.check_running()
        next_sample = 0.0
        while True:
            self.check_running()
            now = time.monotonic()
            if now >= ready_end:
                raise TimeoutError('storage readiness exceeded 30 seconds or absolute expiry')
            if all(Path(record['ready']).is_file() for record in self.children):
                self.ready = True
                self.check_running()
                self.record_sample()
                self.publish()
                atomic_json(self.cfg['ready_path'], self.receipt())
                return
            if now >= next_sample:
                self.record_sample()
                self.publish()
                next_sample = time.monotonic() + SAMPLE_SECONDS
            time.sleep(min(POLL_SECONDS, max(0, ready_end - now)))

    def cleanup(self):
        if self.stop_reason == 'controller_stop':
            for record in self.children:
                if exit_status(record['child']) is not None:
                    self.fail('namespace exited before requested shutdown')
        self.stopping = True
        # Stop-file handling is advisory; TERM/KILL always target only our
        # unreaped child-led groups, including descendants of an exited leader.
        for record in self.children:
            try:
                path = Path(record['stop'])
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                os.close(fd)
            except FileExistsError:
                pass
            except OSError as error:
                self.fail('namespace stop-file write failed: ' + type(error).__name__)
        self.signal_groups(signal.SIGTERM)
        end = time.monotonic() + min(3, max(0, self.remaining() - KILL_RESERVE))
        while time.monotonic() < end:
            if all(exit_status(record['child']) is not None for record in self.children):
                break
            time.sleep(min(POLL_SECONDS, max(0, end - time.monotonic())))
        # Kill even when all leaders exited: same-group descendants may remain.
        self.signal_groups(signal.SIGKILL)
        reap_end = time.monotonic() + min(0.5, max(0, self.remaining()))
        for record in self.children:
            child = record['child']
            try:
                code = child.wait(timeout=max(0, reap_end - time.monotonic()))
                with self.lock:
                    record.update(reaped=True, exit_code=code)
            except subprocess.TimeoutExpired:
                self.fail('owned child failed to exit after SIGKILL')
        self.cleanup_complete = all(record['reaped'] for record in self.children)

    def run(self):
        old_signals = {number: signal.signal(number, self.on_signal)
                       for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
        guard = threading.Thread(target=self.watchdog, name='checkpoint-deadline', daemon=True)
        guard.start()
        try:
            self.launch()
            next_sample = time.monotonic() + SAMPLE_SECONDS
            while self.check_running():
                if time.monotonic() >= next_sample:
                    self.record_sample()
                    self.publish()
                    next_sample = time.monotonic() + SAMPLE_SECONDS
                time.sleep(POLL_SECONDS)
            self.stop_reason = 'controller_stop'
            self.record_sample()
        except Exception as error:
            self.fail(str(error) if isinstance(error, (RuntimeError, TimeoutError)) else type(error).__name__)
            self.stop_reason = 'failure'
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.fail('cleanup failed: ' + type(error).__name__)
                self.signal_groups(signal.SIGKILL)
            if self.received_signal is not None:
                self.fail('received signal ' + str(self.received_signal))
            if self.watchdog_expired or self.remaining() <= 0:
                self.fail('absolute deadline reached')
            self.finished_ns = time.time_ns()
            try:
                if self.claimed:
                    if self.before is None:
                        self.before = host_sample()
                    self.record_sample()
                    self.publish()
            except Exception as error:
                self.fail('final evidence write failed: ' + type(error).__name__)
            finally:
                self.watchdog_stop.set()
                guard.join(timeout=POLL_SECONDS * 2)
                for number, handler in old_signals.items():
                    signal.signal(number, handler)
        return 0 if self.failure is None and self.cleanup_complete else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        cfg = validate_config(args.config.absolute())
    except Exception as error:
        # Parser errors may quote YAML/TLS content; never log untrusted text.
        print(json.dumps(dict(complete=False, failure='node config rejected', error=type(error).__name__)))
        return 2
    owner = NodeOwner(cfg)
    code = owner.run()
    print(json.dumps(dict(run_id=cfg['run_id'], complete=code == 0, failure=owner.failure)))
    return code


if __name__ == '__main__':
    raise SystemExit(main())
