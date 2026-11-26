#!/usr/bin/env python3
"""Replay one frozen real Sui chain through NativeDb and RF1, then audit every row.

The owner count is explicit (`--owners`, default 2). One coordinator and two
owners is the campaign shape; the coordinator has no per-partition counter.
"""
import argparse
import ctypes
import fcntl
import math
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
sys.dont_write_bytecode = True
import tarfile
import time
import urllib.request
import yaml
import build
import campaign_ledger
import deployment
import disk_collector as disk
import checkpoint_support as checkpoint


def read_json(path):
    return json.loads(path.read_text())


def sample(child):
    """Sample a reserved owned PID; unavailable counters never stand in for IO."""
    if not isinstance(child, build.OwnedProcess):
        raise TypeError('sampling requires an owned process')
    pid = child.pid
    code = child.poll()
    if code is not None:
        return dict(pid=pid, exited=True, exit_code=code,
                    unavailable={'process': 'exited_and_reaped' if child.reaped else 'exited'})
    source = 'status'
    try:
        status = Path(f'/proc/{pid}/status').read_text().splitlines()
        source = 'io'
        io = Path(f'/proc/{pid}/io').read_text().splitlines()
        return dict(pid=pid, **{line.split(':')[0]: int(line.split()[1])*1024 for line in status
            if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))},
            io={line.split(':')[0]: int(line.split()[1]) for line in io})
    except (FileNotFoundError, ProcessLookupError, PermissionError) as error:
        code = child.poll()
        if code is None and isinstance(error, PermissionError):
            # Core dumping revokes ptrace-gated /proc/io access before waitid
            # reports exit. Re-read status only on this failure path: an earlier
            # status read may predate the fatal signal.
            try:
                status = Path(f'/proc/{pid}/status').read_text().splitlines()
            except (FileNotFoundError, ProcessLookupError):
                status = []
            code = child.poll()
            if code is None and any(line.split() == ['CoreDumping:', '1'] for line in status):
                return dict(pid=pid, exited=False, core_dumping=True,
                            unavailable={source: type(error).__name__})
        if code is None:
            raise
        return dict(pid=pid, exited=True, exit_code=code,
                    unavailable={source: type(error).__name__})


def host_sample():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    return dict(swap_pages={k: int(vm[k]) for k in ('pswpin', 'pswpout')},
                diskstats=Path('/proc/diskstats').read_text(), meminfo=Path('/proc/meminfo').read_text())


class Owned:
    def __init__(self, root, env):
        self.root, self.env, self.children = root, env, []
        self.libc = ctypes.CDLL(None, use_errno=True)

    def start(self, name, command, extra=None):
        with (self.root / f'{name}.log').open('xb') as log:
            return self.spawn(command, log, subprocess.STDOUT, extra)

    def spawn(self, command, stdout, stderr, extra=None):
        parent = os.getpid()
        def parent_death():
            if self.libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0 or os.getppid() != parent:
                os._exit(125)
        child = build.OwnedProcess(list(map(str, command)), env=self.env | (extra or {}),
                                   stdout=stdout, stderr=stderr, preexec_fn=parent_death)
        self.children.append(child)
        return child

    def stop(self, child):
        if child not in self.children:
            raise ValueError('process is not owned by this runner')
        build.stop_group(child, grace=10)
        self.children.remove(child)

    def close(self):
        for child in reversed(self.children.copy()):
            self.stop(child)


def live_metric_namespaces(schema, epoch):
    """Exports fix population independently of scrapes; old epoch stores are closed."""
    if type(epoch) is not int or epoch < 0:
        raise ValueError('certified checkpoint lacks a valid epoch')
    import re
    epochs = {name for name in schema if re.search(r'/epoch_[0-9]+$', name)}
    live = {name for name in schema if name not in epochs or name.endswith(f'/epoch_{epoch}')}
    if not live or (epochs and not live.intersection(epochs)):
        raise ValueError('selected schema lacks the certified live epoch namespace')
    return live


def wait_for_final_metrics(child, shards, address, namespaces, *, remote, end, destination):
    """Certify one post-certified scrape with observed aggregate progress.

    The engine no longer publishes per-owner generation counters. A scalar
    coordinator registry covers every audited live namespace, and a remote cut
    additionally requires an aggregate `tidehunter_nvmeof_live_*` counter to
    advance after certification.
    """
    from analyze import (metric_cut, metric_cut_progressed, parse_metrics,
                         validate_final_metric_cut, MetricCutUnavailable)
    baseline = None
    unavailable_observations = 0
    observations = []
    start = time.monotonic()
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('final metrics freshness exceeded original replay deadline')
        for process in (child, *shards):
            state = sample(process)
            if state.get('exited') or state.get('core_dumping'):
                status = f"exited with code {state['exit_code']}" if state.get('exited') else 'is core-dumping'
                raise RuntimeError(f"owned process {state['pid']} {status} during final metrics freshness; see {destination.parent}/*.log")
        with urllib.request.urlopen(f'http://{address}/metrics', timeout=min(1, remaining)) as response:
            text = response.read(32 << 20).decode()
        # Retain the last real response even on schema/deadline/freshness failure.
        destination.write_text(text)
        if time.monotonic() >= end:
            raise TimeoutError('final metrics scrape exceeded original replay deadline')
        try:
            current = metric_cut(parse_metrics(text), namespaces, remote=remote)
        except MetricCutUnavailable as unavailable:
            unavailable_observations += 1
            observations.append(dict(seconds=time.monotonic() - start, unavailable=True,
                                     reason=str(unavailable)))
            time.sleep(min(.1, max(0, end - time.monotonic())))
            continue
        observations.append(dict(seconds=time.monotonic() - start,
            live_available=current['live']['available'],
            keyspaces=current['coordinator']['keyspaces']))
        if baseline is None:
            baseline = current
        elif metric_cut_progressed(baseline, current, remote=remote):
            cut = dict(status='post-certified-scrape',
                required_namespaces=sorted(namespaces), baseline=baseline, final=current,
                observations=observations, seconds=time.monotonic() - start,
                unavailable_observations=unavailable_observations,
                scope='after certified RPC and index catchup; excluded from foreground elapsed; '
                      'aggregate live counters and coordinator histograms, not quiescent physical IO')
            validate_final_metric_cut(parse_metrics(text), namespaces, cut, remote=remote)
            if time.monotonic() >= end:
                raise TimeoutError('final metrics qualification exceeded original replay deadline')
            return text, cut
        time.sleep(min(.1, max(0, end - time.monotonic())))


def hold_node(owned, child, shards, *, address, seconds, disk_sample,
              interval_seconds, destination, poll_seconds=1.0):
    """M5: keep the certified measured node resident and sample its disk.

    The node is past its final metric cut, so it serves no new checkpoint; the
    hold observes the on-disk coordinator geometry while the database stays
    open. A process that exits, or a scrape that fails, fails the cell. The
    snapshot and relocation triggers the plan names are not exposed by the
    engine: the control-region snapshot runs on a one-hour timer or a write
    threshold, and relocation is requested by the Sui pruner after retained
    epochs. Both are recorded, never claimed.
    """
    if not callable(disk_sample):
        raise ValueError('a hold needs disk sampling')
    start = time.monotonic()
    samples, next_sample = [], start
    end = start + seconds
    while True:
        now = time.monotonic()
        if now >= end:
            break
        for process in (child, *shards):
            state = sample(process)
            if state.get('exited') or state.get('core_dumping'):
                status = (f"exited with code {state['exit_code']}"
                          if state.get('exited') else 'is core-dumping')
                raise RuntimeError(f'owned process {state["pid"]} {status} during the hold')
        if now >= next_sample:
            try:
                samples.append(disk_sample())
            except (OSError, ValueError) as error:
                samples.append(dict(unavailable=str(error)))
            next_sample = now + interval_seconds
        time.sleep(min(poll_seconds, max(0.0, end - time.monotonic())))
    try:
        final = disk_sample()
    except (OSError, ValueError) as error:
        final = dict(unavailable=str(error))
    samples.append(final)
    metrics = None
    metrics_error = None
    for _ in range(60):
        try:
            with urllib.request.urlopen(f'http://{address}/metrics', timeout=5) as response:
                metrics = response.read(32 << 20).decode()
            destination.write_text(metrics)
            break
        except OSError as error:
            metrics_error = str(error)
            time.sleep(0.5)
    return dict(
        requested_seconds=seconds, seconds=time.monotonic() - start,
        interval_seconds=interval_seconds, disk_samples=samples, final=final,
        final_metrics=str(destination) if metrics is not None else None,
        final_metrics_available=metrics is not None,
        final_metrics_reason=metrics_error,
        triggers=dict(
            snapshot=dict(
                triggered=False,
                reason='the control-region snapshot runs on a 3600 s timer or a '
                       'written-byte threshold; the engine exposes no external trigger'),
            relocation=dict(
                triggered=False,
                reason='relocation is requested by the Sui pruner after retained '
                       'epochs; the frozen fixture is epoch 0, so this cell cannot '
                       'reach one')),
        scope='the measured remote node stays open after its final metric cut; '
              'disk samples cover the hold window and are not a quiescent measurement')


def replay(owned, binary, root, mode, tip, digest, extra, shards, deadline,
           disk_sample=None, disk_interval_seconds=10, bound=None,
           short_run=False, certified_epoch=None, hold_seconds=0):
    if bound is None:
        bound = tip
    if short_run and certified_epoch is None:
        raise ValueError('a short replay needs the producer-certified epoch')
    config = checkpoint.configuration(root, mode)
    start = time.monotonic()
    child = owned.start(mode, [binary, '--config-path', root / f'{mode}.yaml',
                              '--run-with-range-checkpoint', replay_bound(bound)], extra)
    interval = 0.05 if short_run else 0.25
    log_path = owned.root / f'{mode}.log'
    progress, samples, disk_samples, final_count = [], [], [], 0
    next_disk = time.monotonic() if disk_sample else float('inf')
    metrics = ''
    def take_disk_sample():
        if not disk_sample:
            return
        try:
            disk_samples.append(disk_sample())
        except (OSError, ValueError) as error:
            disk_samples.append(dict(unavailable=str(error)))
    with (owned.root / f'{mode}-samples.jsonl').open('x') as stream:
        try:
            while time.monotonic()-start < deadline:
                if time.monotonic() >= next_disk:
                    take_disk_sample()
                    next_disk = time.monotonic() + disk_interval_seconds
                row = dict(seconds=time.monotonic()-start, node=sample(child), shards=[sample(p) for p in shards], host=host_sample())
                samples.append(row)
                stream.write(json.dumps(row)+'\n')
                stream.flush()
                failed = next((p for p in [row['node'], *row['shards']]
                               if p.get('exited') or p.get('core_dumping')), None)
                if failed is not None:
                    if short_run and failed.get('exited') and failed.get('exit_code') == 0:
                        require_short_watermark(metrics, tip, mode=mode)
                        progress.append(dict(seconds=time.monotonic()-start,
                                             completed_checkpoint=tip, exited_at_bound=True))
                        break
                    state = f"exited with code {failed['exit_code']}" if failed.get('exited') else 'is core-dumping'
                    raise RuntimeError(f"{mode}: owned process {failed['pid']} {state} during replay; see {owned.root}/*.log")
                try:
                    with urllib.request.urlopen(f"http://{config['metrics-address']}/metrics", timeout=1) as response:
                        metrics = response.read(32 << 20).decode()
                    completed = checkpoint.completed_checkpoint(metrics)
                except (OSError, ValueError):
                    time.sleep(interval)
                    continue
                progress.append(dict(seconds=time.monotonic()-start, completed_checkpoint=completed))
                if completed > tip:
                    raise RuntimeError('replay passed frozen tip')
                final_count = final_count+1 if completed == tip else 0
                if final_count == 2 and not short_run:
                    break
                time.sleep(interval)
            else:
                raise TimeoutError(f'{mode}: foreground deadline exceeded')
        except BaseException as error:
            if metrics:
                try:
                    (owned.root / f'{mode}-foreground.prom').write_text(metrics)
                except OSError as retention_error:
                    error.add_note(f'foreground metrics retention failed: {retention_error}')
            raise
    elapsed = time.monotonic()-start
    (owned.root / f'{mode}-foreground.prom').write_text(metrics)
    schema = deployment.schemas(Path(extra['SUI_SHARDED_WAL_SCHEMA_OUTPUT']),
                                'sharded-wal' if mode == 'remote' else 'native')
    if short_run:
        # The node exits 0 after the inclusive bound. Its RPC and final-metrics
        # path are gone, so keep the last live scrape and use the producer's
        # certified checkpoint for the epoch and digest identity.
        (owned.root / f'{mode}-final.prom').write_text(metrics)
        catchup, certified, freshness = short_run_records(
            tip=tip, digest=digest, certified_epoch=int(certified_epoch),
            namespaces=live_metric_namespaces(schema, int(certified_epoch)))
    else:
        metrics, catchup = checkpoint.wait_for_rpc_indexes(child, config['metrics-address'], tip, metrics)
        certified = checkpoint.rpc(config['json-rpc-address'], 'sui_getCheckpoint', [str(tip)])
        if certified['digest'] != digest or int(certified['sequenceNumber']) != tip:
            raise RuntimeError('completed certified checkpoint digest mismatch')
        namespaces = live_metric_namespaces(schema, int(certified['epoch']))
        metrics, freshness = wait_for_final_metrics(child, shards, config['metrics-address'], namespaces,
            remote=mode == 'remote', end=start + deadline, destination=owned.root / f'{mode}-final.prom')
    # SIGTERM only after committed indexer frontiers; audit subsequently reopens all stores.
    held = None
    if hold_seconds:
        if short_run:
            raise ValueError('a hold needs the measured node alive; a short replay '
                             'exits at its bound')
        held = hold_node(owned, child, shards, address=config['metrics-address'],
                         seconds=hold_seconds, disk_sample=disk_sample,
                         interval_seconds=disk_interval_seconds,
                         destination=owned.root / f'{mode}-hold.prom')
    owned.stop(child)
    if child.returncode not in (0, -signal.SIGTERM):
        raise RuntimeError(f'{mode}: shutdown required forced kill or failed')
    final_disk = None
    if disk_sample:
        take_disk_sample()
        final_disk = disk_samples[-1] if disk_samples else None
        if final_disk is not None and 'databases' in final_disk:
            try:
                final_disk = dict(final_disk, budget=disk.budget(final_disk))
            except ValueError as error:
                final_disk = dict(final_disk, budget=dict(available=False, reason=str(error)))
    return dict(elapsed_seconds_including_startup=elapsed, completed_checkpoint_digest=digest,
        completed_checkpoint_epoch=int(certified['epoch']),
        checkpoint_limit=tip, completed_window=checkpoint.completion_window(progress), progress=progress,
        phase_timing=checkpoint.replay_phases((owned.root / f'{mode}.log').read_text(), elapsed),
        rpc_index_catchup=catchup, final_metrics_freshness=freshness, exit_code=child.returncode,
        bound=bound, short_run=short_run, final_metrics_source=(
            'last_live_scrape' if short_run else 'post-certified live scrape'),
        certified_source=('producer' if short_run else 'measured node'),
        peak_sampled_rss_bytes=max(sum(p.get('VmRSS', 0) for p in [r['node'], *r['shards']]) for r in samples),
        process_io_final=samples[-1], host_before=samples[0]['host'], host_after=samples[-1]['host'],
        disk_samples=disk_samples if disk_sample else None, disk_final=final_disk,
        hold=held)


def command_json(command, destination, env, deadline=600):
    owned = Owned(destination.parent, env)
    try:
        with destination.open('xb') as stdout, destination.with_suffix('.stderr').open('xb') as stderr:
            child = owned.spawn(command, stdout, stderr)
            code = child.wait(timeout=deadline)
            if code:
                raise subprocess.CalledProcessError(code, command)
    finally:
        owned.close()
    return read_json(destination)


def audit(root, output, bins, schemas, native_root, remote_root, config, env, label=''):
    from checkpoint_support import local_rocksdb_paths, compare_local_store
    result = dict(databases={}, rocksdb={}, all_equal=True)
    for index, namespace in enumerate(schemas):
        shape = output / 'native-schema' / namespace / 'shape.json'
        reports = []
        for mode, db_root in [('native', native_root), ('remote', remote_root)]:
            args = [bins / 'state-audit', mode, db_root / namespace, shape]
            if mode == 'remote':
                args += [config, namespace]
            reports.append(command_json(args, output / f'audit{label}-{index}-{mode}.json', env))
        if reports[0]['tables'] != reports[1]['tables']:
            result['all_equal'] = False
        result['databases'][namespace] = dict(equal=reports[0]['tables']==reports[1]['tables'], native=reports[0], remote=reports[1])
    audited_roots = [Path(namespace).relative_to('live') for namespace in schemas]
    inventories = [local_rocksdb_paths(path / 'live', audited_roots) for path in (native_root, remote_root)]
    if set(inventories[0]) != set(inventories[1]) or 'epochs' not in inventories[0]:
        raise ValueError('RocksDB coverage differs or committee store absent')
    for index, name in enumerate(sorted(inventories[0])):
        reports = [command_json([bins / 'rocksdb-audit', stores[name]], output / f'rocks{label}-{index}-{mode}.json', env)
                   for mode, stores in zip(('native','remote'), inventories)]
        comparison = compare_local_store(name, *reports)
        result['rocksdb'][name] = comparison
        result['all_equal'] &= comparison['equal']
    build.save(output / f'audit{label}.json', result)
    if not result['all_equal']:
        raise RuntimeError('exact final full-state audit differs')
    return result


def publish_stage(directory, stage, binding):
    directory.mkdir(parents=True, exist_ok=True)
    row = dict(binding, stage=stage, unix_ns=time.time_ns())
    if (directory/f'{stage}.json').exists():
        raise ValueError('external stage directory has stale run markers')
    for name in (f'{stage}.json', 'stage.json'):
        temporary = directory/(name+'.tmp')
        with temporary.open('x') as stream:
            stream.write(json.dumps(row, sort_keys=True)+'\n')
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(directory/name)
    descriptor = os.open(directory, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return row


def wait_resources(directory, binding, timeout=120):
    deadline = time.monotonic()+min(timeout, 120)
    path = directory/'resources-ready'
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('controller resource evidence deadline exceeded')
        time.sleep(.1)
    marker = read_json(path)
    required = read_json(directory/'resources-required.json')
    if (any(marker.get(key) != value for key, value in binding.items())
            or type(marker.get('unix_ns')) is not int or marker['unix_ns'] < required['unix_ns']):
        raise ValueError('controller resources-ready binding/timestamp differs')


def nonnegative_integer(value):
    return type(value) is int and value >= 0


def validate_resources(resources, manifest, binding, stages):
    if (any(resources.get(key) != value for key, value in binding.items())
            or set(resources.get('hosts', {})) != {host['host'] for host in manifest['hosts']}
            or manifest.get('owners') != len(manifest.get('hosts', []))):
        raise ValueError('external resource evidence does not cover this replay host inventory')
    start, finish = stages[0]['unix_ns'], stages[1]['unix_ns']
    for expected in manifest['hosts']:
        host = resources['hosts'][expected['host']]
        if (any(host.get(key) != value for key, value in binding.items())
                or host.get('storage_binary_sha256') != manifest['storage_binary_sha256']
                or host.get('root') != expected['guest_root']
                or any(host.get(key) is not True for key in ('complete', 'cleanup_complete', 'all_namespaces_healthy'))
                or host.get('failure') or host.get('samples_dropped') != 0):
            raise ValueError('external host identity, lifecycle or retained samples differ')
        namespaces = host.get('namespaces', [])
        if (len(namespaces) != len(expected['commands'])
                or {tuple(n.get('command', [])) for n in namespaces} != {tuple(c) for c in expected['commands']}
                or any(n.get('reaped') is not True for n in namespaces)):
            raise ValueError('external owned namespace coverage differs')
        pids = {n.get('pid'): n for n in namespaces}
        if (len(pids) != len(namespaces) or any(not nonnegative_integer(pid) or pid == 0
                or n.get('process_group') != pid or not nonnegative_integer(n.get('starttime_ticks'))
                for pid, n in pids.items())):
            raise ValueError('external owned process identities are invalid')
        for suffix in ('before', 'after'):
            swap = host.get('swap_'+suffix)
            disk = host.get('diskstats_'+suffix)
            if not isinstance(swap, dict) or any(not nonnegative_integer(swap.get(k)) for k in ('pswpin', 'pswpout')):
                raise ValueError('external host swap counters are unavailable')
            if not isinstance(disk, str) or not disk.strip():
                raise ValueError('external diskstats are unavailable')
            for line in disk.splitlines():
                fields = line.split()
                if len(fields) < 14 or not all(v.isdecimal() for v in fields[:2]+fields[3:]):
                    raise ValueError('external diskstats contain placeholders')
        samples = host.get('samples', [])
        healthy = []
        previous_ns = -1
        for row in samples:
            stamp = row.get('unix_ns')
            if not nonnegative_integer(stamp) or stamp <= previous_ns:
                raise ValueError('external sample timestamps are invalid')
            previous_ns = stamp
            if row.get('healthy') is not True:
                if start <= stamp <= finish:
                    raise ValueError('external owned process failed during remote replay')
                continue
            processes = row.get('processes', [])
            if len(processes) != len(pids) or {p.get('pid') for p in processes} != set(pids):
                raise ValueError('external sample lacks complete owned PID coverage')
            for process in processes:
                owner = pids[process['pid']]
                if (process.get('exited') is not False or process.get('unavailable') != {}
                        or process.get('starttime_ticks') != owner['starttime_ticks']
                        or process.get('index') != owner['index']
                        or any(not nonnegative_integer(process.get(k)) for k in ('VmRSS', 'VmHWM', 'VmSwap'))
                        or any(not nonnegative_integer(process.get('io', {}).get(k)) for k in
                               ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes', 'cancelled_write_bytes'))):
                    raise ValueError('external live process physical IO/RSS/swap is unavailable')
            healthy.append(stamp)
        if len(healthy) < 2 or healthy[0] > start or healthy[-1] < finish:
            raise ValueError('external healthy samples do not span remote replay')


def is_fabric_transport(value):
    return deployment.canonical_transport(value)['kind'] == 'ofi'


def validate_profiles(counters, databases, read_limit, write_limit):
    for name, stats in counters.items():
        if read_limit:
            if stats['read_rpcs'] == 0:
                if any(stats[key] != 0 for key in (
                        'read_profile_selected', 'read_profile_completed',
                        'read_profile_errors', 'read_profile_wire_requests')):
                    raise ValueError('zero read-RPC population contains profile observations')
            elif (not 0 < stats['read_profile_selected'] <= read_limit
                    or not 0 < stats['read_profile_completed'] <= read_limit
                    or stats['read_profile_wire_requests'] <= 0
                    or stats['read_profile_errors'] != 0
                    or stats.get('read_profile_server_available') != 1
                    or stats.get('read_profile_transport_progress_available') != 1):
                raise ValueError('bounded read profile lacks successful RPC/server/transport observations')
            if (is_fabric_transport(databases[name]['tls']['transport']) and stats['read_rpcs'] > 0
                    and (stats.get('read_profile_transport_cq_available') != 1
                         or (stats.get('read_profile_transport_cq_timed_calls') or 0) <= 0)):
                raise ValueError('fabric completion-queue profiling is unavailable')
        expected = len(databases[name]['endpoints'])
        if stats['write_profile_expected_connections'] != expected:
            raise ValueError('write profiling does not cover the configured append owners')
        if not write_limit:
            if any(stats[key] != 0 for key in (
                    'write_profile_available', 'write_profile_captured_connections',
                    'write_profile_sampled_requests')):
                raise ValueError('clean population contains write profiling work')
            continue
        if (stats['write_profile_available'] != 1
                or stats['write_profile_captured_connections'] != expected
                or not 0 < stats['write_profile_sampled_requests'] <= write_limit * expected
                or any((stats.get('write_profile_storage_' + field) or 0) <= 0
                       for field in ('append_calls', 'persist_calls', 'file_persist_calls'))
                or any(stats.get('write_profile_storage_' + field) != 0
                       for field in ('append_errors', 'persist_errors'))):
            raise ValueError(f'bounded write profile lacks complete successful append/persistence coverage: {name}')
        if (is_fabric_transport(databases[name]['tls']['transport'])
                and any((stats.get(prefix + 'cq_timed_calls') or 0) <= 0
                        for prefix in ('write_profile_client_transport_', 'write_profile_transport_'))):
            raise ValueError('fabric write completion-queue profiling is unavailable')


def validate_native_profiles(counters, limit):
    for stats in counters.values():
        # Availability describes backend capability, not whether sampling is on.
        if stats.get('native_profile_limit') != limit or stats.get('native_profile_available') != 1:
            raise ValueError('native persistence profiling differs from requested population')
        calls = stats.get('native_profile_barrier_calls')
        if (not isinstance(calls, (int, float)) or not 0 <= calls <= limit
                or stats.get('native_profile_barrier_errors') != 0
                or stats.get('native_profile_file_calls', -1) < calls
                or stats.get('native_profile_file_successes') != stats.get('native_profile_file_calls')
                or stats.get('native_profile_namespace_calls') != calls
                or stats.get('native_profile_namespace_successes') != calls):
            raise ValueError('native persistence profile is incomplete or contains failed barriers')


def validate_checkpoint_diagnostics(counters, write_limit):
    from analyze import CHECKPOINT_DIAGNOSTIC_PHASES, CHECKPOINT_HISTORY_COUNTERS
    enabled = int(write_limit > 0)
    def count(value):
        return (not isinstance(value, bool) and isinstance(value, (int, float))
                and math.isfinite(value) and value >= 0 and value == int(value))
    for stats in counters.values():
        if stats.get('write_checkpoint_diagnostics_available') != enabled:
            raise ValueError('checkpoint diagnostic availability differs from selected population')
        records = stats.get('write_checkpoint_diagnostic_records')
        dropped = stats.get('write_checkpoint_diagnostic_dropped_records')
        if not count(records) or not count(dropped) or dropped > records or (not enabled and records):
            raise ValueError('checkpoint diagnostic record/drop cardinality is invalid')
        for phase in CHECKPOINT_DIAGNOSTIC_PHASES:
            prefix = 'write_checkpoint_diagnostic_' + phase
            samples, total = stats.get(prefix + '_count'), stats.get(prefix + '_ns')
            if (not count(samples) or samples > records or (not enabled and samples)
                    or (enabled and (not count(total) or (samples == 0 and total != 0)))
                    or (not enabled and total not in (None, 0))):
                raise ValueError('checkpoint diagnostic phase population/span is invalid: ' + phase)
        history_count = stats.get('write_checkpoint_diagnostic_history_io_checkpoint_count')
        if (not count(history_count) or history_count > records or (not enabled and history_count)):
            raise ValueError('checkpoint history IO population is invalid')
        for counter in CHECKPOINT_HISTORY_COUNTERS:
            total = stats.get('write_checkpoint_diagnostic_' + counter)
            if ((enabled and (not count(total) or (history_count == 0 and total != 0)))
                    or (not enabled and total not in (None, 0))):
                raise ValueError('checkpoint history IO counter unavailable or malformed: ' + counter)


def external_index_rows_error(database):
    """The hybrid index rows must come from TH_* so both measured nodes agree.

    `SUI_SHARDED_WAL_CONFIG` may state an `index` section, but that document
    reaches the hybrid coordinator only; the native node always builds its rows
    from the env. A stated section would therefore size the two measured nodes
    differently, and the campaign would compare unlike caches.
    """
    if 'index' in database:
        return ('SUI_SHARDED_WAL_CONFIG states an index section; the campaign sets '
                'index rows through TH_* so the native node and the hybrid coordinator '
                'take the same values')
    return None


def external_durability_error(config, tier):
    """Return why an external deployment does not state `tier`, or None."""
    if tier not in ('native', 'synchronous'):
        raise ValueError('durability tier must be native or synchronous')
    for name, database in config['databases'].items():
        stated = (database.get('live_nvme') or {}).get('leased_durability')
        if stated is not None and stated != tier:
            return (f'TH_SYNC_WRITES={tier} requires live_nvme.leased_durability '
                    f'{tier!r}, but the external config states {stated!r} for {name}')
    return None


def frozen_mutex_count(fixture):
    """The mutex count the frozen producer's checkpoint keyspace declares.

    The producer database is frozen data: its `checkpoint_content` shape records
    one mutex per keyspace, and the engine refuses a reopen that declares another
    count because it would misroute existing cells. A replay must therefore
    declare exactly that count instead of inheriting a fork default.
    """
    shapes = sorted((Path(fixture) / 'producer').rglob('live/checkpoints/shape_v2.yaml'))
    if len(shapes) != 1:
        raise ValueError('the fixture must carry exactly one checkpoint keyspace shape')
    current = None
    for line in shapes[0].read_text().splitlines():
        named = re.fullmatch(r'\s+name: (\S+)', line)
        if named is not None:
            current = named.group(1)
            continue
        counted = re.fullmatch(r'\s+mutexes: (\d+)', line)
        if counted is not None and current == 'checkpoint_content':
            count = int(counted.group(1))
            if not 1 <= count <= 4096:
                raise ValueError('the frozen mutex count is outside 1..4096')
            return count
    raise ValueError('the frozen checkpoint shape declares no checkpoint_content mutex count')


INDEX_BLOB_CACHE_ENV = 'TH_INDEX_BLOB_CACHE_BYTES'
INDEX_BLOB_CACHE_MIN_ENV = 'TH_INDEX_BLOB_CACHE_MIN_BYTES'
# The frozen Sui row keeps this floor when no value is given; record it as the
# effective minimum instead of writing it back as an explicit 0.
ENGINE_INDEX_BLOB_CACHE_MIN_BYTES = 64 << 20
INDEX_BLOB_CACHE_ROWS = (INDEX_BLOB_CACHE_ENV, INDEX_BLOB_CACHE_MIN_ENV)
MANAGED_ENV = {
    'TH_SYNC_WRITES', 'TH_FRAG_SIZE', 'TH_MAX_MAPS', 'TH_MAX_INDEX_MAPS',
    'TH_DEFAULT_MUTEX_COUNT', 'TH_DEFAULT_MAX_DIRTY_KEYS',
    'TH_DEFAULT_VALUE_CACHE_SIZE', 'TH_VALUE_CACHE_SIZE',
    'TH_SNAPSHOT_WRITTEN_BYTES', 'TH_INDEX_BLOB_CACHE_MIN_BYTES',
    INDEX_BLOB_CACHE_ENV,
    'SUI_TH_READ_PROFILE_LIMIT', 'SUI_TH_NATIVE_WRITE_PROFILE_LIMIT',
    'SUI_CHECKPOINT_EXECUTION_MAX_CONCURRENCY',
    'SUI_CHECKPOINT_EXECUTION_PACING_MS',
}
ENGINE_PREFIXES = ('TH_', 'SUI_SHARDED_WAL_', 'SUI_DISTRIBUTED_',
                   'DISTRIBUTED_TIDEHUNTER_', 'TIDEHUNTER_', 'SUI_CHECKPOINT_')


def runtime_environment(os_environ, *, tier, fragment_mib, driver_concurrency, pacing_ms,
                        value_cache_size, default_value_cache_size, max_dirty_keys,
                        snapshot_written_bytes, max_maps, max_index_maps,
                        index_blob_cache_min_bytes, index_blob_cache_bytes,
                        read_profile_limit, native_write_profile_limit, mutex_count):
    """One explicit env for the replay; unknown inherited TH_* is refused.

    A knob this runner does not pin stays unset, so the engine's fork default is
    what the cell measures; `TH_MAX_MAPS` and `TH_MAX_INDEX_MAPS` are therefore
    written only when a cell sets them. The mutex count is different: the frozen
    fixture and native schema record one mutex per keyspace, and the engine
    refuses a reopen that declares another count because it would misroute
    existing cells. It is therefore pinned from the frozen layout
    (`frozen_mutex_count`), never inherited and never left to the fork default.
    """
    for name, value in (('max_maps', max_maps), ('max_index_maps', max_index_maps)):
        if value is not None and (type(value) is not int or not 3 <= value <= 32):
            raise ValueError(f'{name} must be 3..32 when set; the WAL mapper '
                             'panics below 3')
    unknown = {key for key in os_environ if key.startswith('TH_') and key not in MANAGED_ENV}
    if unknown:
        raise ValueError('unknown TH_ runner knob(s): ' + ', '.join(sorted(unknown)))
    env = {key: value for key, value in os_environ.items()
           if not key.startswith(ENGINE_PREFIXES)}
    env.update(RUST_LOG='info', TOKIO_WORKER_THREADS='4', USE_TIDEHUNTER='1',
               TH_SYNC_WRITES='true' if tier == 'synchronous' else 'false',
               TH_DEFAULT_MUTEX_COUNT=str(mutex_count),
               TH_FRAG_SIZE=str(fragment_mib*deployment.MIB),
               TH_DEFAULT_MAX_DIRTY_KEYS=str(max_dirty_keys),
               TH_DEFAULT_VALUE_CACHE_SIZE=str(default_value_cache_size),
               TH_SNAPSHOT_WRITTEN_BYTES=str(snapshot_written_bytes),
               SUI_TH_READ_PROFILE_LIMIT=str(read_profile_limit),
               SUI_TH_NATIVE_WRITE_PROFILE_LIMIT=str(native_write_profile_limit))
    if max_maps is not None:
        env['TH_MAX_MAPS'] = str(max_maps)
    if max_index_maps is not None:
        env['TH_MAX_INDEX_MAPS'] = str(max_index_maps)
    if driver_concurrency is not None:
        env['SUI_CHECKPOINT_EXECUTION_MAX_CONCURRENCY'] = str(driver_concurrency)
    if pacing_ms:
        env['SUI_CHECKPOINT_EXECUTION_PACING_MS'] = str(pacing_ms)
    if value_cache_size is not None:
        env['TH_VALUE_CACHE_SIZE'] = str(value_cache_size)
    if index_blob_cache_bytes:
        env[INDEX_BLOB_CACHE_ENV] = str(index_blob_cache_bytes)
    if index_blob_cache_min_bytes is not None:
        env[INDEX_BLOB_CACHE_MIN_ENV] = str(index_blob_cache_min_bytes)
    return env


PACING_ENV = 'SUI_CHECKPOINT_EXECUTION_PACING_MS'


def cell_admission(args):
    """Refuse a reviewed cell whose knobs differ from its reviewed row.

    A cell row fixes the tier, the geometry, the map and hold limits and the
    run.py flags the cell needs. Nothing here adds a knob: it refuses a run
    that would carry a different id than the one it measures.
    """
    row = campaign_ledger.cell(args.cell_id)
    if row is None:
        return None
    if args.durability_tier != row['tier']:
        raise ValueError(f"{row['id']} is a {row['tier']} cell, not "
                         f"{args.durability_tier}")
    for name, actual, declared in (
            ('--fragment-mib', args.fragment_mib, row['fragment_mib']),
            ('--coordinator-cache-mib', args.coordinator_cache_mib,
             row['coordinator_cache_mib']),
            ('--max-maps', args.max_maps, row['max_maps']),
            ('--max-index-maps', args.max_index_maps, row['max_index_maps']),
            ('--hold-seconds', args.hold_seconds, row['hold_seconds'])):
        if actual != declared:
            raise ValueError(f"{row['id']} requires {name} {declared!r}, not {actual!r}")
    for flag, value in row['run'].items():
        actual = getattr(args, flag[2:].replace('-', '_'))
        if value is None:
            expected = True
        elif isinstance(actual, bool):
            expected = value not in ('0', 'false', 'False')
        elif actual is None:
            expected = value
        else:
            expected = type(actual)(value)
        if actual != expected:
            raise ValueError(f"{row['id']} requires {flag} {value!r}, not {actual!r}")
    return row


def _mentions(path, needle):
    value = needle.encode()
    with Path(path).open('rb') as stream:
        tail = b''
        while chunk := stream.read(1 << 20):
            window = tail + chunk
            if value in window:
                return True
            tail = window[-len(value):]
    return False


def pacing_support(build_root):
    """True only when the frozen Sui executor reads the pacing hook.

    The hook lives in `checkpoint_executor/pacing.rs`, so scan that directory,
    not two fixed files. A built `sui-node-*` binary's strings are the final
    proof that the hook was compiled in.
    """
    root = Path(build_root) / 'sources/sui/crates/sui-core/src/checkpoints/checkpoint_executor'
    if root.is_dir():
        for path in root.rglob('*.rs'):
            try:
                if PACING_ENV in path.read_text():
                    return True
            except (OSError, UnicodeError):
                continue
    return any(_mentions(path, PACING_ENV)
               for path in sorted((Path(build_root) / 'binaries').glob('sui-node-*')))


def requested_checkpoint(limit, requested):
    """Highest replayed sequence for a requested checkpoint count.

    Sui's RunWithRange checkpoint bound is inclusive, so a request for N
    checkpoints starting at 0 ends at N-1. `None` keeps the frozen range.
    """
    if requested is None:
        return limit
    if type(requested) is not int or not 1 <= requested <= limit + 1:
        raise ValueError(f'checkpoint count must be 1..{limit + 1}')
    return requested - 1


def replay_bound(highest):
    """Sui's RunWithRange checkpoint bound is inclusive; pass the sequence."""
    return highest


def producer_command(binary, config_path):
    """The producer is a checkpoint source, never a bounded replay.

    A producer database already holds the frozen tip, so a RunWithRange equal
    to that tip is met at startup, before the executor's shutdown receiver
    exists; Sui then panics. The producer therefore gets no range at all.
    """
    return [binary, '--config-path', config_path]


def short_run_records(*, tip, digest, certified_epoch, namespaces):
    """JSON-safe short-run result records (namespace sets are sorted)."""
    catchup = dict(available=False, started=0, indexed=0, tip=tip,
                   reason='short replay: node stopped at the inclusive bound')
    certified = dict(sequenceNumber=tip, digest=digest,
                     epoch=int(certified_epoch), source='producer')
    freshness = dict(available=False, required_namespaces=sorted(namespaces),
                     source='last_live_scrape',
                     reason='short replay: node exited at the inclusive bound; '
                            'final metrics are the last live scrape and the state '
                            'audit reopens the stopped stores')
    return catchup, certified, freshness


def require_short_watermark(metrics, tip, *, mode='replay'):
    """A short run may exit 0 only after the last scrape saw the requested tip.

    This fork does not log every executed checkpoint, so the last successful
    scrape's `last_executed_checkpoint` gauge is the watermark of record.
    """
    try:
        watermark = checkpoint.completed_checkpoint(metrics)
    except ValueError:
        watermark = None
    if watermark != tip:
        raise RuntimeError(f'{mode}: short replay exited 0 after the last scrape saw '
                           f'checkpoint {watermark}, expected {tip}')
    return watermark


def replay_mode(source_tip, requested):
    """Full cells keep the node alive; short cells exit at the inclusive N-1."""
    tip = requested_checkpoint(source_tip, requested)
    short_run = tip != source_tip
    return dict(tip=tip, short_run=short_run,
                bound=tip if short_run else source_tip + 1)


def configure_range(config, *, mode, tip, short_run):
    """Producer: no range. Measured nodes: full stays alive, short exits at N-1."""
    if mode == 'producer':
        config.pop('run-with-range', None)
    else:
        bound = tip if short_run else tip + 1
        config['run-with-range'] = checkpoint.CheckpointBound(replay_bound(bound))
    return config


DURABLE_CUT_RECORD = 'durable-cut.v1'


def index_blob_cache_support(build_root):
    """True only when the frozen Sui turns both env rows into the index config.

    The native node and the hybrid coordinator both take their index rows from
    `thdb_config`, so the budget row and its companion floor row must exist
    there. A built `sui-node-*` binary's strings are the final proof that both
    rows were compiled in.
    """
    path = (Path(build_root)
            / 'sources/sui/crates/typed-store/src/tidehunter_util.rs')
    if path.is_file():
        try:
            text = path.read_text()
            if all(row in text for row in INDEX_BLOB_CACHE_ROWS):
                return True
        except (OSError, UnicodeError):
            pass
    return any(all(_mentions(binary, row) for row in INDEX_BLOB_CACHE_ROWS)
               for binary in sorted((Path(build_root) / 'binaries').glob('sui-node-*')))


def index_blob_cache_floor_error(min_bytes, budget_mib):
    """Why this floor cannot be used with this budget, or None.

    An unset floor keeps the engine's 64 MiB default and a budget of 0 keeps the
    cache off, so neither is compared. The engine refuses a floor above the
    budget when both rows are set; refuse it here, before a host is touched.
    """
    if min_bytes is None or not budget_mib:
        return None
    if min_bytes > budget_mib * deployment.MIB:
        return ('index blob cache floor exceeds its budget: '
                f'{min_bytes} > {budget_mib * deployment.MIB} bytes')
    return None


def index_blob_cache_error(build_root):
    """Why this build cannot size the index blob cache, or None."""
    if index_blob_cache_support(build_root):
        return None
    return ('the index blob cache needs ' + ' and '.join(INDEX_BLOB_CACHE_ROWS)
            + ' in the frozen Sui fork: '
            'crates/typed-store/src/tidehunter_util.rs::thdb_config builds the native Config '
            'and the hybrid coordinator Config from env rows and has no pair of rows for '
            'index_blob_cache_bytes and index_blob_cache_min_bytes, so the engine keeps its '
            'None default. An "index" section in SUI_SHARDED_WAL_CONFIG reaches the hybrid '
            'coordinator only, never the native node, so the two measured nodes cannot be set '
            'the same way. Rebuild Sui hybrid-tidy with both env rows, then repeat this cell.')


def durable_cut_record(path):
    """Parse one CRC-framed `durable-cut.v1` record (engine storage format)."""
    import zlib
    data = Path(path).read_bytes()
    if len(data) < 8:
        raise ValueError(f'durable-cut record is too short: {path}')
    length, checksum = int.from_bytes(data[:4], 'big'), int.from_bytes(data[4:8], 'big')
    payload = data[8:]
    if len(payload) != length:
        raise ValueError(f'durable-cut record has trailing bytes: {path}')
    if zlib.crc32(payload) & 0xffffffff != checksum:
        raise ValueError(f'durable-cut record CRC differs: {path}')
    value = yaml.safe_load(payload)
    if not isinstance(value, dict) or value.get('format') != 2:
        raise ValueError(f'durable-cut record format differs: {path}')
    return value


def durable_cut_paths(remote_root, namespaces):
    """Locate one record per audited namespace under the coordinator root."""
    paths = {}
    for namespace in namespaces:
        directory = Path(remote_root) / namespace
        direct = directory / DURABLE_CUT_RECORD
        if direct.is_file():
            paths[namespace] = direct
            continue
        matches = sorted(directory.rglob(DURABLE_CUT_RECORD)) if directory.is_dir() else []
        if len(matches) == 1:
            paths[namespace] = matches[0]
    return paths


def durable_cut_frontier(record):
    """The tracked cut of one namespace, or None when the tier is untracked."""
    live = record.get('live')
    if live is None:
        return None
    if not isinstance(live, dict) or set(live) != {'epoch', 'next'}:
        raise ValueError('durable-cut live frontier is malformed')
    return dict(epoch=int(live['epoch']), next=int(live['next']))


def durable_cut_comparison(before, after):
    """A clean `flush_durable` certifies the prefix; recovery must not extend it."""
    rows = {}
    for namespace in sorted(before):
        prior = durable_cut_frontier(before[namespace])
        later = durable_cut_frontier(after[namespace])
        if (prior is None) != (later is None):
            raise ValueError(f'durable-cut tracking changed across the reopen: {namespace}')
        if prior is None:
            rows[namespace] = dict(tracked=False)
            continue
        if later['next'] > prior['next']:
            raise ValueError(
                'durable cut extended after flush_durable for '
                f'{namespace}: next {prior["next"]} -> {later["next"]}')
        rows[namespace] = dict(tracked=True, before=prior, after=later)
    return dict(tracked=any(row['tracked'] for row in rows.values()), namespaces=rows,
                scope='the durable cut after the last flush_durable, before and after the '
                      'offline state audit and the clean reopen')


def exports_comparison(before, after):
    """A clean reopen adopts exposed namespaces; it must not export new ones."""
    reason = 'the aggregate tidehunter_nvmeof_live_* family is missing from one scrape'
    if not before['available'] or not after['available']:
        return dict(available=False, reason=reason)
    prior, later = before['counters'], after['counters']
    if set(prior) != set(later):
        raise ValueError('aggregate live NVMe-oF population changed across the reopen')
    exposed, previous = later.get('target_exposed_total'), prior.get('target_exposed_total')
    observed, fixtures = later.get('target_observed_partitions'), later.get('target_fixture_partitions')
    if exposed is None or previous is None or observed is None or fixtures is None:
        return dict(available=False, reason='live target exposure counters are absent')
    if fixtures and observed and fixtures >= observed:
        return dict(available=False,
                    reason='every observed partition is a fixture target; no kernel namespace is exported',
                    before=previous, after=exposed, observed_partitions=observed,
                    fixture_partitions=fixtures)
    if exposed < previous:
        raise ValueError('live target exposure counter regressed across the reopen')
    if exposed > previous:
        raise ValueError(f'a clean reopen created {exposed - previous} new export(s)')
    return dict(available=True, reason=None, before=previous, after=exposed,
                observed_partitions=observed, fixture_partitions=fixtures,
                scope='the pre-close scrape against the post-reopen scrape; the offline '
                      'state audit opens the same authority in between')


def audit_comparison(before, after):
    """The full-state audit must not change across the reopen."""
    def canonical(value):
        return json.dumps(value, sort_keys=True)
    rows = {}
    for namespace in before['databases']:
        rows[namespace] = dict(equal=(
            canonical(before['databases'][namespace]['native']['tables'])
            == canonical(after['databases'][namespace]['native']['tables'])
            and canonical(before['databases'][namespace]['remote']['tables'])
            == canonical(after['databases'][namespace]['remote']['tables'])))
    for name in before['rocksdb']:
        rows['rocksdb/' + name] = dict(equal=(
            canonical(before['rocksdb'][name]) == canonical(after['rocksdb'][name])))
    changed = sorted(name for name, row in rows.items() if not row['equal'])
    if changed:
        raise ValueError('the state audit differs after the reopen: ' + ', '.join(changed))
    return dict(equal=True, namespaces=rows)


def reopen(owned, *, binary, root, address, producer_address, tip, digest, bound,
           extra, tail_checkpoints):
    """Restart the measured node, certify the frozen tip, then stop it cleanly.

    The pinned Sui schedules from `highest_executed + 1`, so a node that already
    holds the tip re-executes nothing. The engine's open still replays the owner
    WAL tail above the durable cut, and the last K checkpoints are read back
    through the reopened RPC surface and compared with the producer.
    """
    start = time.monotonic()
    child = owned.start('reopen', [binary, '--config-path', root / 'remote.yaml',
                                  '--run-with-range-checkpoint', replay_bound(bound)], extra)
    try:
        checkpoint.wait_rpc(child, address)
        with urllib.request.urlopen(f'http://{address}/metrics', timeout=5) as response:
            metrics = response.read(32 << 20).decode()
        metrics, catchup = checkpoint.wait_for_rpc_indexes(child, address, tip, metrics)
        certified = checkpoint.rpc(address, 'sui_getCheckpoint', [str(tip)])
        first = max(0, tip - tail_checkpoints + 1)
        sample = []
        for sequence in range(first, tip + 1):
            node = checkpoint.rpc(address, 'sui_getCheckpoint', [str(sequence)])
            source = checkpoint.rpc(producer_address, 'sui_getCheckpoint', [str(sequence)])
            if (int(node['sequenceNumber']) != sequence or node['digest'] != source['digest']):
                raise RuntimeError(f'tail checkpoint {sequence} differs after the reopen')
            sample.append(dict(sequenceNumber=sequence, digest=node['digest']))
    finally:
        owned.stop(child)
    if child.returncode not in (0, -signal.SIGTERM):
        raise RuntimeError('the reopen required a forced kill or failed')
    (owned.root / 'reopen-final.prom').write_text(metrics)
    return dict(seconds=time.monotonic() - start, exit_code=child.returncode,
                certified=dict(sequenceNumber=int(certified['sequenceNumber']),
                               digest=certified['digest'], epoch=int(certified['epoch'])),
                rpc_index_catchup=catchup,
                tail=dict(window=[first, tip], count=len(sample), sample=sample,
                          source='read back through the reopened RPC and compared with '
                                 'the producer')), metrics


def lifecycle_checks(owned, *, bins, root, output, remote_root, namespaces,
                     address, producer_address, tip, digest, bound, extra,
                     schemas, native_root, config_path, env, tier, audit_before,
                     tail_checkpoints=64):
    """S1-1/S1-8: clean reopen, no new export, unchanged durable cut, same state."""
    from analyze import adapter_stats, live_nvmeof_stats, parse_metrics
    paths = durable_cut_paths(remote_root, namespaces)
    missing = sorted(set(namespaces) - set(paths))
    if missing:
        raise ValueError('durable-cut record not found for namespace(s): '
                         + ', '.join(missing) + f' under {remote_root}')
    before_records = {name: durable_cut_record(path) for name, path in paths.items()}
    before_live = live_nvmeof_stats(parse_metrics((output / 'remote-final.prom').read_text()))
    receipt, metrics = reopen(owned, binary=bins / 'sui-node-remote', root=root,
                             address=address, producer_address=producer_address, tip=tip,
                             digest=digest, bound=bound, extra=extra,
                             tail_checkpoints=tail_checkpoints)
    after_records = {name: durable_cut_record(path) for name, path in paths.items()}
    cut = durable_cut_comparison(before_records, after_records)
    if tier == 'native' and not cut['tracked']:
        raise ValueError('the native tier tracks a durable cut; none was recorded')
    exports = exports_comparison(before_live, live_nvmeof_stats(parse_metrics(metrics)))
    counters = adapter_stats(parse_metrics(metrics))
    if not counters:
        raise ValueError('the reopened node published no adapter counters')
    publishes = {name: int(stats.get('write_commit_calls', -1))
                 for name, stats in counters.items()}
    if set(publishes) != set(namespaces):
        raise ValueError('reopened adapter counters do not cover the audited namespaces')
    absent = sorted(name for name, value in publishes.items() if value < 0)
    if absent:
        raise ValueError('the reopened node lacks the write-commit counter for: '
                         + ', '.join(absent))
    if any(publishes.values()):
        raise ValueError('the reopen published a checkpoint instead of replaying the tail')
    audit_after = audit(root, output, bins, schemas, native_root, remote_root,
                        config_path, env, label='-reopen')
    return dict(
        status='clean-reopen-verified', tail_checkpoints=tail_checkpoints,
        reopen=receipt, no_new_export=exports, durable_cut=cut,
        tail_replay=dict(
            state_unchanged=True, publishes=publishes,
            sui_rescheduled_checkpoints=0,
            source='engine open replays the owner WAL tail above the durable cut; '
                   'the pinned Sui schedules from highest_executed + 1'),
        audit_reopen=audit_comparison(audit_before, audit_after))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--deadline', type=int, default=900)
    parser.add_argument('--port', type=int, default=24000)
    parser.add_argument('--coordinator-cache-mib', type=int,
                        default=campaign_ledger.DEFAULT_COORDINATOR_CACHE_MIB,
                        help='coordinator read cache in MiB; the campaign default is 0 (off)')
    parser.add_argument('--shard-cache-mib', type=int, default=0)
    parser.add_argument('--local-transport', choices=('tcp', *deployment.SOFTWARE_TRANSPORTS), default='tcp',
                        help='loopback correctness provider; never hardware EFA evidence')
    parser.add_argument('--resident-reads', type=Path,
                        help='bounded per-database registered-read JSON policy; requires trusted-native fabric')
    parser.add_argument('--owners', type=int, default=2,
                        help='distinct RF1 owner processes/hosts per database (default 2; no five-owner assumption)')
    parser.add_argument('--durability-tier', choices=('native', 'synchronous'), default='synchronous',
                        help='store acknowledgement tier; native is the Sui production default (db sync disabled)')
    parser.add_argument('--fragment-mib', type=int,
                        default=campaign_ledger.DEFAULT_FRAGMENT_MIB,
                        help='engine fragment size in MiB; the production campaign '
                             'geometry is 1024 and the synthetic M3/M4 rows use 64')
    parser.add_argument('--driver-concurrency', type=int, default=4,
                        help='Sui checkpoint-execution-max-concurrency; the fixture value is 4')
    parser.add_argument('--pacing-ms', type=int, default=0,
                        help='minimum milliseconds between checkpoints; 0 disables pacing')
    parser.add_argument('--checkpoints', type=int,
                        help='replay exactly N checkpoints 0..N-1 (default: the whole fixture range)')
    parser.add_argument('--value-cache-size', type=int,
                        help='TH_VALUE_CACHE_SIZE flattened per-keyspace override; unset keeps Sui values')
    parser.add_argument('--default-value-cache-size', type=int, default=1000,
                        help='TH_DEFAULT_VALUE_CACHE_SIZE per-cell default (Sui uses 1000)')
    parser.add_argument('--max-dirty-keys', type=int, default=1024,
                        help='TH_DEFAULT_MAX_DIRTY_KEYS base; Sui multiplies it per keyspace')
    parser.add_argument('--snapshot-written-bytes', type=int, default=8 << 30,
                        help='TH_SNAPSHOT_WRITTEN_BYTES native snapshot threshold')
    parser.add_argument('--max-maps', type=int, default=None,
                        help='TH_MAX_MAPS replay/value WAL maps; unset keeps the fork '
                             'default, and the engine panics below 3')
    parser.add_argument('--max-index-maps', type=int, default=None,
                        help='TH_MAX_INDEX_MAPS index WAL maps; unset keeps the fork '
                             'default, and the engine panics below 3')
    parser.add_argument('--index-blob-cache-min-bytes', type=int,
                        help='TH_INDEX_BLOB_CACHE_MIN_BYTES; omit to keep the engine 64 MiB '
                             'default, 0 admits every qualifying blob')
    parser.add_argument('--index-blob-cache-mib', type=int, default=0,
                        help='TH_INDEX_BLOB_CACHE_BYTES in MiB; 0 keeps the engine default (cache off). '
                             'The frozen Sui must read the row for both measured nodes')
    parser.add_argument('--lifecycle-checks', action='store_true',
                        help='after a full replay: clean reopen, no new export, unchanged durable '
                             'cut, tail replay idempotence and a second audit (S1-1/S1-8)')
    parser.add_argument('--tail-checkpoints', type=int, default=64,
                        help='last K checkpoints read back through the reopened node')
    parser.add_argument('--disk-sample-seconds', type=int, default=10,
                        help='coordinator disk inventory interval in seconds, 0 disables collection')
    parser.add_argument('--hold-seconds', type=int, default=0,
                        help='M5: keep the certified measured node resident this long, '
                             'sampling its disk, then take a final metrics scrape; needs '
                             'a positive --disk-sample-seconds')
    parser.add_argument('--read-profile-limit', type=int, default=0,
                        help='maximum profiled remote read RPCs per DB, 0 disables diagnostics')
    parser.add_argument('--write-profile-limit', type=int, default=0,
                        help='maximum profiled append RPCs per owning connection, 0 disables diagnostics')
    parser.add_argument('--native-write-profile-limit', type=int, default=0,
                        help='maximum profiled native durability barriers per DB, 0 disables diagnostics')
    parser.add_argument('--external-config', type=Path)
    parser.add_argument('--external-resources', type=Path, help='controller-owned final per-host resource evidence')
    parser.add_argument('--external-stage-dir', type=Path, help='controller handshake directory, required in external mode')
    parser.add_argument('--cell-id',
                        help='campaign cell identity; a reviewed id (see '
                             'campaign_ledger.MANUAL_SUI_CELLS) fixes the cell knobs '
                             'and the criteria it must prove')
    args = parser.parse_args()
    if any((args.external_config, args.external_resources, args.external_stage_dir)) and not all(
            (args.external_config, args.external_resources, args.external_stage_dir)):
        raise ValueError('external config, resources and stage directory must be supplied together')
    cell_admission(args)
    if not 30 <= args.deadline <= 3600 or not 1024 <= args.port <= 65000:
        raise ValueError('deadline/port outside bounds')
    if not 1 <= args.owners <= 16:
        raise ValueError('owner count must be 1..16')
    if not 1 <= args.fragment_mib <= 4096:
        raise ValueError('fragment size must be 1..4096 MiB')
    if not 0 <= args.disk_sample_seconds <= 600:
        raise ValueError('disk sample interval must be 0..600 seconds')
    if any(not 0 <= value <= 128 for value in (args.coordinator_cache_mib,args.shard_cache_mib)):
        raise ValueError('per-database cache bound is 0..128 MiB')
    if not 0 <= args.read_profile_limit <= 65536:
        raise ValueError('read profile limit must be 0..65536')
    if not 0 <= args.write_profile_limit <= 65536:
        raise ValueError('write profile limit must be 0..65536')
    if not 0 <= args.native_write_profile_limit <= 65536:
        raise ValueError('native write profile limit must be 0..65536')
    if not 1 <= args.driver_concurrency <= 64:
        raise ValueError('driver concurrency must be 1..64')
    if not 0 <= args.pacing_ms <= 10000:
        raise ValueError('pacing must be 0..10000 ms per checkpoint')
    if args.value_cache_size is not None and not 0 <= args.value_cache_size <= 1000000:
        raise ValueError('value cache size must be 0..1000000 entries')
    if not 0 <= args.default_value_cache_size <= 1000000:
        raise ValueError('default value cache size must be 0..1000000 entries')
    if not 1 <= args.max_dirty_keys <= 1000000:
        raise ValueError('max dirty keys must be 1..1000000')
    if args.max_maps is not None and not 3 <= args.max_maps <= 32:
        raise ValueError('max maps must be 3..32; the WAL mapper panics below 3')
    if args.max_index_maps is not None and not 3 <= args.max_index_maps <= 32:
        raise ValueError('max index maps must be 3..32; the WAL mapper panics below 3')
    if not 0 <= args.hold_seconds <= 86400:
        raise ValueError('hold seconds must be 0..86400')
    if args.hold_seconds and not args.disk_sample_seconds:
        raise ValueError('a hold needs disk sampling; give --disk-sample-seconds above 0')
    if not 0 <= args.snapshot_written_bytes <= 1 << 40:
        raise ValueError('snapshot written bytes must be 0..1 TiB')
    if (args.index_blob_cache_min_bytes is not None
            and not 0 <= args.index_blob_cache_min_bytes <= 1 << 40):
        raise ValueError('index blob cache minimum must be 0..1 TiB')
    if not 0 <= args.index_blob_cache_mib <= 4096:
        raise ValueError('index blob cache budget must be 0..4096 MiB')
    floor_error = index_blob_cache_floor_error(args.index_blob_cache_min_bytes,
                                               args.index_blob_cache_mib)
    if floor_error is not None:
        raise ValueError(floor_error)
    if not 1 <= args.tail_checkpoints <= 1024:
        raise ValueError('tail checkpoint sample must be 1..1024')
    if args.external_config and args.local_transport != 'tcp':
        raise ValueError('local transport cannot override an external deployment')
    resident = (deployment.resident_read_config(read_json(args.resident_reads.resolve(strict=True)))
                if args.resident_reads else None)
    build_root, fixture, root = args.build.resolve(strict=True), args.fixture.resolve(strict=True), args.output.resolve()
    manifest = read_json(build_root / 'build-manifest.json')
    if manifest['protocol'] != 9 or build.digest(build_root/'sources.tar.gz') != manifest['source_archive_sha256']:
        raise ValueError('wrong protocol or source archive binding')
    pacing_available = pacing_support(build_root)
    if args.pacing_ms and not pacing_available:
        raise ValueError('pacing requires SUI_CHECKPOINT_EXECUTION_PACING_MS support in the frozen '
                         'Sui fork; this fork has no pacing hook, so a paced run cannot be claimed')
    if args.index_blob_cache_mib or args.index_blob_cache_min_bytes is not None:
        blob_error = index_blob_cache_error(build_root)
        if blob_error is not None:
            raise ValueError(blob_error)
    if set(manifest['binaries']) != {'sui-node-native','sui-node-remote','storage-node','state-audit','rocksdb-audit'}:
        raise ValueError('build manifest lacks the exact required executable set')
    if args.local_transport in deployment.SOFTWARE_TRANSPORTS and not manifest['fabric']:
        raise ValueError('software fabric validation requires a fabric-enabled build')
    sources = read_json(build_root/'source-manifest.json')
    if build.digest(build_root/'source-manifest.json') != manifest['source_manifest_sha256']:
        raise ValueError('source manifest differs')
    build.verify_staged(build_root, sources)
    for path in Path(__file__).resolve().parent.glob('*.py'):
        relative = 'experiments/sui-checkpoint/'+path.name
        entry = sources['tidehunter-sharded-wal']['files'].get(relative, {})
        if build.digest(path) != entry.get('sha256'):
            raise ValueError('runner source differs from binary-bound source archive')
    bins = build_root/'binaries'
    for name, expected in manifest['binaries'].items():
        if build.digest(bins/name) != expected:
            raise ValueError(f'binary hash differs: {name}')
    prior = read_json(fixture/'experiment/results.json')
    if not prior.get('source_frozen') or prior.get('failure'):
        raise ValueError('fixture is not a completed frozen producer')
    source_tip, expected_digest = prior['checkpoint_limit'], prior['checkpoint_digest']
    if not 0 < source_tip < 5000:
        raise ValueError('fixture does not retain the complete genesis range')
    mode_range = replay_mode(source_tip, args.checkpoints)
    tip, short_run, bound = mode_range['tip'], mode_range['short_run'], mode_range['bound']
    if args.lifecycle_checks and short_run:
        raise ValueError('lifecycle checks need the whole frozen range; a short replay exits at '
                         'the bound, so a clean reopen cannot be exercised')
    replay_digest = expected_digest
    prefix_epoch = None
    # Lock original fixture DB; only the experiment lock file is created there.
    lock = (fixture/'experiment/.sharded-replay.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    inputs = [fixture/'experiment/results.json', *(fixture/f'{mode}.yaml' for mode in ('producer','native','distributed'))]
    inputs += [p for p in (fixture/'producer').rglob('*') if p.is_file() or p.is_symlink()]
    if any(p.is_symlink() for p in inputs):
        raise ValueError('fixture file symlinks are forbidden')
    fixture_files = {str(p.relative_to(fixture)):build.digest(p) for p in inputs}
    if (fixture/'fixture-manifest.json').exists() and read_json(fixture/'fixture-manifest.json')['files'] != fixture_files:
        raise ValueError('portable fixture inventory/hash mismatch')
    root.mkdir(mode=0o700)
    output=root/'experiment'
    output.mkdir()
    import shutil
    shutil.copytree(fixture/'producer', root/'producer')
    for relative, expected in fixture_files.items():
        if build.digest(fixture/relative) != expected or (relative.startswith('producer/') and build.digest(root/relative) != expected):
            raise ValueError('producer changed while copying the frozen fixture')
    configs={}
    for mode, fixture_mode in [('producer','producer'),('native','native'),('remote','distributed')]:
        config=checkpoint.configuration(fixture,fixture_mode)
        previous=Path(config['db-path'])
        relative=previous.relative_to(fixture/fixture_mode) if previous.is_absolute() else previous.relative_to(fixture_mode)
        config['db-path']=str(root/mode/relative)
        config['consensus-config']=None
        config['fullnode-sync-mode']='state-sync-only'
        configure_range(config, mode=mode, tip=tip, short_run=short_run)
        configs[mode]=config
        (root/f'{mode}.yaml').write_text(yaml.dump(config,Dumper=checkpoint.ExperimentDumper))
    native_root,remote_root=(Path(configs[m]['db-path']) for m in ('native','remote'))
    mutex_count = frozen_mutex_count(fixture)
    env = runtime_environment(os.environ,
        tier=args.durability_tier, fragment_mib=args.fragment_mib,
        driver_concurrency=args.driver_concurrency, pacing_ms=args.pacing_ms,
        value_cache_size=args.value_cache_size,
        default_value_cache_size=args.default_value_cache_size,
        max_dirty_keys=args.max_dirty_keys,
        snapshot_written_bytes=args.snapshot_written_bytes, max_maps=args.max_maps,
        max_index_maps=args.max_index_maps,
        index_blob_cache_min_bytes=args.index_blob_cache_min_bytes,
        index_blob_cache_bytes=args.index_blob_cache_mib * deployment.MIB,
        read_profile_limit=args.read_profile_limit,
        native_write_profile_limit=args.native_write_profile_limit,
        mutex_count=mutex_count)
    env = deployment.process_environment(env, transport='tcp' if args.external_config else args.local_transport)
    owned=Owned(output,env)
    producer_env = checkpoint.source_environment(prior)
    result=dict(schema=1,metrics_schema=3,protocol=9,backend='tidehunter-sharded-wal',cell_id=args.cell_id,
        durability=('synchronous durable whole-batch publication' if args.durability_tier == 'synchronous'
                    else 'native appended-frontier acknowledgement (RAM ack, Sui production tier)'),
        durability_tier=args.durability_tier,
        fixture=str(fixture),fixture_files=fixture_files,checkpoint_limit=tip,
        checkpoint_digest=replay_digest,source_checkpoint_digest=expected_digest,
        build_manifest_sha256=build.digest(build_root/'build-manifest.json'),source_archive_sha256=manifest['source_archive_sha256'],
        binaries=manifest['binaries'],configurations=configs,
        environment={k:v for k,v in env.items() if k.startswith(('RUST_LOG','TOKIO_','USE_TIDEHUNTER','TH_','SUI_TH_','SUI_CHECKPOINT_','LD_LIBRARY_PATH','FI_'))},
        knobs=dict(driver_concurrency=args.driver_concurrency, pacing_ms=args.pacing_ms,
                   pacing_available=pacing_available, checkpoints=args.checkpoints,
                   checkpoint_limit=tip, source_checkpoint_limit=source_tip,
                   fragment_mib=args.fragment_mib, mutex_count=mutex_count,
                   mutex_count_source='frozen fixture checkpoint_content layout',
                   value_cache_size=args.value_cache_size,
                   default_value_cache_size=args.default_value_cache_size,
                   max_dirty_keys=args.max_dirty_keys,
                   snapshot_written_bytes=args.snapshot_written_bytes,
                   max_maps=args.max_maps,
                   max_index_maps=args.max_index_maps,
                   index_blob_cache_min_bytes=args.index_blob_cache_min_bytes,
                   index_blob_cache_min_bytes_effective=(
                       args.index_blob_cache_min_bytes
                       if args.index_blob_cache_min_bytes is not None
                       else ENGINE_INDEX_BLOB_CACHE_MIN_BYTES),
                   index_blob_cache_min_bytes_source=(
                       INDEX_BLOB_CACHE_MIN_ENV
                       if args.index_blob_cache_min_bytes is not None
                       else 'engine default 64 MiB (row unset)'),
                   index_blob_cache_mib=args.index_blob_cache_mib,
                   index_blob_cache_bytes=args.index_blob_cache_mib * deployment.MIB,
                   index_blob_cache_source=(INDEX_BLOB_CACHE_ENV
                                            if args.index_blob_cache_mib
                                            else 'engine default off (row unset)'),
                   lifecycle_checks=args.lifecycle_checks,
                   tail_checkpoints=args.tail_checkpoints,
                   source='explicit runtime env; nothing is inherited from TH_*'),
        cache=dict(coordinator_bytes=args.coordinator_cache_mib*deployment.MIB,shard_bytes=args.shard_cache_mib*deployment.MIB),
        owners=args.owners,
        topology=f'{args.owners} physical owners per database; '+('external hosts' if args.external_config else 'loopback, one SSD/shared host memory'),
        local_transport=None if args.external_config else args.local_transport,
        read_profile_limit=args.read_profile_limit,write_profile_limit=args.write_profile_limit,
        native_write_profile_limit=args.native_write_profile_limit,
        started_unix=time.time())
    result['producer_environment'] = producer_env
    def interrupted(signum, frame):
        raise InterruptedError(f'runner received signal {signum}')
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    try:
        producer=owned.start('producer',producer_command(bins/'sui-node-native', root/'producer.yaml'), producer_env)
        address=configs['producer']['json-rpc-address']
        if int(checkpoint.wait_rpc(producer,address)) != source_tip:
            raise ValueError('producer tip differs')
        producer_tip = checkpoint.rpc(address,'sui_getCheckpoint',[str(source_tip)])
        if producer_tip['digest'] != expected_digest:
            raise ValueError('producer tip digest differs from the frozen fixture')
        result['source_range']=checkpoint.verify_source_range(address,source_tip)
        if result['source_range'] != prior['verified_source_range']:
            raise ValueError('complete source digest range differs')
        replay_digest = expected_digest
        if short_run:
            prefix = checkpoint.rpc(address,'sui_getCheckpoint',[str(tip)])
            replay_digest = prefix['digest']
            prefix_epoch = int(prefix['epoch'])
            result['checkpoint_digest'] = replay_digest
            result['source_range_prefix'] = checkpoint.verify_source_range(address, tip)
        result['source_checkpoint_limit'] = source_tip
        result['short_run'] = short_run
        result['bound'] = bound
        native_extra=dict(SUI_SHARDED_WAL_ROOT=str(native_root), SUI_SHARDED_WAL_SCHEMA_OUTPUT=str(output/'native-schema'))
        result['native']=replay(owned,bins/'sui-node-native',root,'native',tip,replay_digest,native_extra,[],args.deadline,
            bound=bound, short_run=short_run, certified_epoch=prefix_epoch)
        schema=deployment.schemas(output/'native-schema', 'native')
        shards=[]
        if args.external_config:
            config_path=args.external_config.resolve(strict=True)
            config=read_json(config_path)
            if Path(config['root']) != remote_root or set(config['databases']) != set(schema):
                raise ValueError('external namespace/root coverage differs')
            mismatch = external_durability_error(config, args.durability_tier)
            if mismatch is not None:
                raise ValueError(mismatch)
            for name, database in config['databases'].items():
                if deployment.is_software(database['tls']['transport']):
                    raise ValueError('external software fabric is loopback-only in the native engine')
                if (len(database['endpoints']) != args.owners
                        or len(set(e['address'].rsplit(':',1)[0] for e in database['endpoints'])) != args.owners):
                    raise ValueError(f'external mode requires {args.owners} distinct storage hosts per DB')
                if database['storage']['partitions'] != list(range(args.owners)):
                    raise ValueError('external storage partition population differs from the requested owners')
                if database['read_cache_bytes'] != result['cache']['coordinator_bytes']:
                    raise ValueError('external coordinator cache differs')
                index_error = external_index_rows_error(database)
                if index_error is not None:
                    raise ValueError(index_error)
                if (resident is not None and
                        deployment.resident_read_config(database['tls'].get('resident_reads')) != resident):
                    raise ValueError('external resident-read policy differs from requested population')
            external_manifest=read_json(config_path.parent/'external-manifest.json')
            if external_manifest.get('owners') != args.owners:
                raise ValueError('external manifest owner count differs from the requested owners')
            if (external_manifest['config_sha256'] != build.digest(config_path)
                    or external_manifest['build_manifest_sha256'] != result['build_manifest_sha256']
                    or external_manifest['shard_cache_bytes'] != result['cache']['shard_bytes']):
                raise ValueError('external storage source/config/cache binding differs')
            result['external_manifest']=external_manifest
        else:
            config_path=output/'deployment/config.json'
            config,nodes=deployment.prepare(config_path.parent,schema,remote_root,
                shape_yamls={name:(output/'native-schema'/name/'shape.yaml').read_text() for name in schema},
                hosts=['127.0.0.1']*args.owners, owners=args.owners,
                port=args.port,transport=args.local_transport,coordinator_cache=result['cache']['coordinator_bytes'],
                shard_cache=result['cache']['shard_bytes'],write_profile_limit=args.write_profile_limit,
                resident_reads=resident, owner_tier=args.durability_tier,
                live_transport='tcp')
            for index,node in enumerate(nodes):
                child=owned.start(f'storage-{index}',[bins/'storage-node','--experimental-storage-node',node['launch']])
                shards.append(child)
                end=time.monotonic()+30
                while not Path(node['ready']).exists():
                    if child.poll() is not None or time.monotonic()>end:
                        raise RuntimeError('storage process failed readiness')
                    time.sleep(.05)
        if any(database.get('write_profile_limit', 0) != args.write_profile_limit
               for database in config['databases'].values()):
            raise ValueError('prepared write profiling differs from requested population')
        result['deployment']=config
        result['resident_reads_by_namespace'] = {
            name: deployment.resident_read_config(database['tls'].get('resident_reads'))
            for name, database in config['databases'].items()}
        remote_extra=dict(SUI_SHARDED_WAL_CONFIG=str(config_path),SUI_SHARDED_WAL_SCHEMA_OUTPUT=str(output/'remote-schema'))
        if args.external_config:
            binding = dict(config_sha256=build.digest(config_path),
                           build_manifest_sha256=result['build_manifest_sha256'], checkpoint_digest=expected_digest)
            result['external_stages'] = [publish_stage(args.external_stage_dir, 'remote-starting', binding)]
        disk_sample = None
        if args.disk_sample_seconds:
            fragment_bytes = args.fragment_mib * deployment.MIB
            namespaces = sorted(schema)
            def disk_sample():
                return disk.sample(remote_root, namespaces, fragment_bytes=fragment_bytes)
        result['disk_geometry'] = dict(fragment_bytes=args.fragment_mib*deployment.MIB,
            production_fragment_bytes=1024*deployment.MIB,
            matches_production=args.fragment_mib == 1024)
        result['remote']=replay(owned,bins/'sui-node-remote',root,'remote',tip,replay_digest,remote_extra,shards,args.deadline,
            bound=bound, short_run=short_run, certified_epoch=prefix_epoch,
            disk_sample=disk_sample, disk_interval_seconds=args.disk_sample_seconds,
            hold_seconds=args.hold_seconds)
        result['hold'] = result['remote']['hold']
        result['live_transport'] = (
            (result.get('external_manifest') or {}).get('live_transport') or 'tcp')
        result['disk'] = dict(geometry=result['disk_geometry'],
            samples=result['remote']['disk_samples'], final=result['remote']['disk_final'])
        budget = (result['disk']['final'] or {}).get('budget') if result['disk']['final'] else None
        if budget and budget.get('within_limit') is False:
            raise RuntimeError('coordinator disk projection exceeds the configured free-space share')
        if args.external_config:
            result['external_stages'].append(publish_stage(args.external_stage_dir, 'remote-finished', binding))
        if deployment.schemas(output/'remote-schema', 'sharded-wal') != schema:
            raise ValueError('remote selected schema differs from native')
        from analyze import adapter_stats, checkpoint_sequence_cache_stats, metric_cut, metrics
        if not short_run:
            metric_cut(metrics(output/'remote-final.prom'),
                       result['remote']['final_metrics_freshness']['required_namespaces'], remote=True)
        counters = adapter_stats(metrics(output/'remote-final.prom'))
        result['adapter_metrics_available'] = bool(counters)
        required = {'read_calls','read_keys','read_rpcs','cache_hits','cache_misses',
                    'remote_timing_available','write_gate_wait_calls','scalar_read_calls','bulk_read_calls','write_commit_calls',
                    'scalar_read_ns_available','read_index_ns_available',
                    'read_submit_calls','read_detached_wait_calls','read_finish_calls',
                    'write_append_requests','write_peak_inflight_nodes','write_durable_batches',
                    'write_concurrent_admitted','write_concurrent_completed','write_concurrent_pending_peak',
                    'read_profile_limit','read_profile_selected','read_profile_completed','read_profile_errors',
                    'read_profile_inline_completed','read_profile_inline_ns','read_profile_actor_completed',
                    'read_profile_wire_requests','cache_inserts',
                    'write_checkpoint_ns','write_checkpoint_count',
                    'write_foreground_checkpoint_ns','write_foreground_checkpoint_count',
                    'write_durable_receipt_publication_blocked_ns','write_durable_receipt_publication_blocked_batches',
                    'write_durable_receipt_publication_waiting_batches','write_durable_receipt_publication_waiting_peak',
                    'write_profile_limit','write_profile_available','write_profile_expected_connections',
                    'write_profile_captured_connections','write_profile_sampled_requests'}
        if counters and (set(counters) != set(schema)
                or any(not required.issubset(stats) for stats in counters.values())
                or any(stats['remote_timing_available'] != 1
                       or stats['read_profile_limit'] != args.read_profile_limit
                       or stats['write_profile_limit'] != args.write_profile_limit
                       for stats in counters.values())):
            raise ValueError('API/key/RPC/cache/stage instrumentation lacks selected DB coverage')
        if counters and any(stats['scalar_read_ns_available'] != int(args.read_profile_limit > 0)
               or stats['read_index_ns_available'] != int(args.read_profile_limit > 0)
               for stats in counters.values()):
            raise ValueError('scalar/index diagnostic timing availability differs from selected population')
        if counters:
            validate_profiles(counters, config['databases'], args.read_profile_limit, args.write_profile_limit)
            validate_checkpoint_diagnostics(counters, args.write_profile_limit)
        native_counters = adapter_stats(metrics(output/'native-final.prom'))
        if native_counters and (set(native_counters) != set(schema) or any(
                stats.get('remote_timing_available') != 0 or 'write_gate_wait_calls' not in stats
                for stats in native_counters.values())):
            raise ValueError('native write-gate timing instrumentation is unavailable')
        if native_counters:
            validate_native_profiles(native_counters, args.native_write_profile_limit)
        for mode in ('native', 'remote'):
            if not checkpoint_sequence_cache_stats(metrics(output / f'{mode}-foreground.prom')):
                raise ValueError('checkpoint sequence-cache counters are unavailable')
        result['audit']=audit(root,output,bins,schema,native_root,remote_root,config_path,env)
        result['audit_scope'] = ('re-open of both stopped stores after a short bound; shards stayed live'
                                 if short_run else 're-open of both stopped stores; shards stayed live')
        if args.lifecycle_checks:
            result['lifecycle'] = lifecycle_checks(
                owned, bins=bins, root=root, output=output, remote_root=remote_root,
                namespaces=set(schema), address=configs['remote']['json-rpc-address'],
                producer_address=configs['producer']['json-rpc-address'],
                tip=tip, digest=replay_digest, bound=bound, extra=remote_extra,
                schemas=schema, native_root=native_root, config_path=config_path,
                env=env, tier=args.durability_tier,
                audit_before=result['audit'], tail_checkpoints=args.tail_checkpoints)
        if producer.poll() is not None or int(checkpoint.rpc(address,'sui_getLatestCheckpointSequenceNumber')) != source_tip or checkpoint.verify_source_range(address,source_tip)!=result['source_range']:
            raise RuntimeError('source changed across paired replays')
        if tip != source_tip and checkpoint.rpc(address,'sui_getCheckpoint',[str(tip)])['digest'] != replay_digest:
            raise RuntimeError('source checkpoint prefix changed across paired replays')
        if args.external_config:
            result['external_stages'].append(publish_stage(args.external_stage_dir, 'resources-required', binding))
            wait_resources(args.external_stage_dir, binding)
            resources = read_json(args.external_resources)
            validate_resources(resources, result['external_manifest'], binding, result['external_stages'])
            result['external_resources'] = resources
        for name, expected in manifest['binaries'].items():
            if build.digest(bins/name) != expected:
                raise ValueError('binary changed during replay/audit')
        from analyze import cell_checks, metrics as read_metrics
        final_by_mode = {mode: read_metrics(output / f'{mode}-final.prom')
                         for mode in ('native', 'remote')}
        checks = cell_checks(result, final_by_mode, sorted(schema),
                             checkpoint_count=tip + 1)
        result['cell_checks'] = checks
        if checks['failures']:
            raise RuntimeError('cell criteria failed: ' + '; '.join(checks['failures']))
        result['complete']=True
    except BaseException as error:
        result['failure']=str(error)
        raise
    finally:
        owned.close()
        result['finished_unix']=time.time()
        build.save(output/'results.json',result)
        # Complete evidence, without bulk DB/WAL files or private TLS material.
        with tarfile.open(root/'evidence.tar.gz','w:gz') as archive:
            for path in sorted(output.rglob('*')):
                if path.is_file() and not any(part in ('tls','storage') for part in path.relative_to(output).parts):
                    archive.add(path,arcname=str(path.relative_to(root)))
            for name in ('source-manifest.json','build-manifest.json','plan.json'):
                archive.add(build_root/name,arcname='provenance/'+name)
        build.save(root/'receipt.json',dict(complete=result.get('complete',False), evidence_sha256=build.digest(root/'evidence.tar.gz'),
            source_archive_sha256=manifest['source_archive_sha256'],build_manifest_sha256=result['build_manifest_sha256']))


if __name__=='__main__':
    main()
