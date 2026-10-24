#!/usr/bin/env python3
"""Replay one frozen real Sui chain through NativeDb and five-shard RF1, then audit every row."""
import argparse
import ctypes
import fcntl
import math
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
sys.dont_write_bytecode = True
import tarfile
import time
import urllib.request
import yaml
import build
import deployment
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


def replay(owned, binary, root, mode, tip, digest, extra, shards, deadline):
    config = checkpoint.configuration(root, mode)
    start = time.monotonic()
    child = owned.start(mode, [binary, '--config-path', root / f'{mode}.yaml',
                              '--run-with-range-checkpoint', tip+1], extra)
    progress, samples, final_count = [], [], 0
    metrics = ''
    with (owned.root / f'{mode}-samples.jsonl').open('x') as stream:
        try:
            while time.monotonic()-start < deadline:
                row = dict(seconds=time.monotonic()-start, node=sample(child), shards=[sample(p) for p in shards], host=host_sample())
                samples.append(row)
                stream.write(json.dumps(row)+'\n')
                stream.flush()
                failed = next((p for p in [row['node'], *row['shards']]
                               if p.get('exited') or p.get('core_dumping')), None)
                if failed is not None:
                    state = f"exited with code {failed['exit_code']}" if failed.get('exited') else 'is core-dumping'
                    raise RuntimeError(f"{mode}: owned process {failed['pid']} {state} during replay; see {owned.root}/*.log")
                try:
                    with urllib.request.urlopen(f"http://{config['metrics-address']}/metrics", timeout=1) as response:
                        metrics = response.read(32 << 20).decode()
                    completed = checkpoint.completed_checkpoint(metrics)
                except (OSError, ValueError):
                    time.sleep(.25)
                    continue
                progress.append(dict(seconds=time.monotonic()-start, completed_checkpoint=completed))
                if completed > tip:
                    raise RuntimeError('replay passed frozen tip')
                final_count = final_count+1 if completed == tip else 0
                if final_count == 2:
                    break
                time.sleep(.25)
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
    metrics, catchup = checkpoint.wait_for_rpc_indexes(child, config['metrics-address'], tip, metrics)
    certified = checkpoint.rpc(config['json-rpc-address'], 'sui_getCheckpoint', [str(tip)])
    if certified['digest'] != digest or int(certified['sequenceNumber']) != tip:
        raise RuntimeError('completed certified checkpoint digest mismatch')
    (owned.root / f'{mode}-final.prom').write_text(metrics)
    # SIGTERM only after committed indexer frontiers; audit subsequently reopens all stores.
    owned.stop(child)
    if child.returncode not in (0, -signal.SIGTERM):
        raise RuntimeError(f'{mode}: shutdown required forced kill or failed')
    return dict(elapsed_seconds_including_startup=elapsed, completed_checkpoint_digest=digest,
        checkpoint_limit=tip, completed_window=checkpoint.completion_window(progress), progress=progress,
        phase_timing=checkpoint.replay_phases((owned.root / f'{mode}.log').read_text(), elapsed),
        rpc_index_catchup=catchup, exit_code=child.returncode,
        peak_sampled_rss_bytes=max(sum(p.get('VmRSS', 0) for p in [r['node'], *r['shards']]) for r in samples),
        process_io_final=samples[-1], host_before=samples[0]['host'], host_after=samples[-1]['host'])


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


def audit(root, output, bins, schemas, native_root, remote_root, config, env):
    from checkpoint_support import local_rocksdb_paths, compare_local_store
    result = dict(databases={}, rocksdb={}, all_equal=True)
    for index, namespace in enumerate(schemas):
        shape = output / 'native-schema' / namespace / 'shape.json'
        reports = []
        for mode, db_root in [('native', native_root), ('remote', remote_root)]:
            args = [bins / 'state-audit', mode, db_root / namespace, shape]
            if mode == 'remote':
                args += [config, namespace]
            reports.append(command_json(args, output / f'audit-{index}-{mode}.json', env))
        if reports[0]['tables'] != reports[1]['tables']:
            result['all_equal'] = False
        result['databases'][namespace] = dict(equal=reports[0]['tables']==reports[1]['tables'], native=reports[0], remote=reports[1])
    audited_roots = [Path(namespace).relative_to('live') for namespace in schemas]
    inventories = [local_rocksdb_paths(path / 'live', audited_roots) for path in (native_root, remote_root)]
    if set(inventories[0]) != set(inventories[1]) or 'epochs' not in inventories[0]:
        raise ValueError('RocksDB coverage differs or committee store absent')
    for index, name in enumerate(sorted(inventories[0])):
        reports = [command_json([bins / 'rocksdb-audit', stores[name]], output / f'rocks-{index}-{mode}.json', env)
                   for mode, stores in zip(('native','remote'), inventories)]
        comparison = compare_local_store(name, *reports)
        result['rocksdb'][name] = comparison
        result['all_equal'] &= comparison['equal']
    build.save(output / 'audit.json', result)
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
            or set(resources.get('hosts', {})) != {host['host'] for host in manifest['hosts']}):
        raise ValueError('external resource evidence does not cover this five-host replay')
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
            if (databases[name]['tls']['transport'] != 'tcp' and stats['read_rpcs'] > 0
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
        if (databases[name]['tls']['transport'] != 'tcp'
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--deadline', type=int, default=900)
    parser.add_argument('--port', type=int, default=24000)
    parser.add_argument('--coordinator-cache-mib', type=int, default=8)
    parser.add_argument('--shard-cache-mib', type=int, default=0)
    parser.add_argument('--local-transport', choices=('tcp', *deployment.SOFTWARE_TRANSPORTS), default='tcp',
                        help='loopback correctness provider; never hardware EFA evidence')
    parser.add_argument('--resident-reads', type=Path,
                        help='bounded per-database registered-read JSON policy; requires trusted-native fabric')
    parser.add_argument('--read-profile-limit', type=int, default=0,
                        help='maximum profiled remote read RPCs per DB, 0 disables diagnostics')
    parser.add_argument('--write-profile-limit', type=int, default=0,
                        help='maximum profiled append RPCs per owning connection, 0 disables diagnostics')
    parser.add_argument('--native-write-profile-limit', type=int, default=0,
                        help='maximum profiled native durability barriers per DB, 0 disables diagnostics')
    parser.add_argument('--external-config', type=Path)
    parser.add_argument('--external-resources', type=Path, help='controller-owned final five-host resource evidence')
    parser.add_argument('--external-stage-dir', type=Path, help='controller handshake directory, required in external mode')
    args = parser.parse_args()
    if any((args.external_config, args.external_resources, args.external_stage_dir)) and not all(
            (args.external_config, args.external_resources, args.external_stage_dir)):
        raise ValueError('external config, resources and stage directory must be supplied together')
    if not 30 <= args.deadline <= 3600 or not 1024 <= args.port <= 65000:
        raise ValueError('deadline/port outside bounds')
    if any(not 0 <= value <= 128 for value in (args.coordinator_cache_mib,args.shard_cache_mib)):
        raise ValueError('per-database cache bound is 0..128 MiB')
    if not 0 <= args.read_profile_limit <= 65536:
        raise ValueError('read profile limit must be 0..65536')
    if not 0 <= args.write_profile_limit <= 65536:
        raise ValueError('write profile limit must be 0..65536')
    if not 0 <= args.native_write_profile_limit <= 65536:
        raise ValueError('native write profile limit must be 0..65536')
    if args.external_config and args.local_transport != 'tcp':
        raise ValueError('local transport cannot override an external deployment')
    resident = (deployment.resident_read_config(read_json(args.resident_reads.resolve(strict=True)))
                if args.resident_reads else None)
    build_root, fixture, root = args.build.resolve(strict=True), args.fixture.resolve(strict=True), args.output.resolve()
    manifest = read_json(build_root / 'build-manifest.json')
    if manifest['protocol'] != 9 or build.digest(build_root/'sources.tar.gz') != manifest['source_archive_sha256']:
        raise ValueError('wrong protocol or source archive binding')
    if set(manifest['binaries']) != {'sui-node-native','sui-node-remote','storage-node','state-audit','rocksdb-audit','transport-floor'}:
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
    tip, expected_digest = prior['checkpoint_limit'], prior['checkpoint_digest']
    if not 0 < tip < 5000:
        raise ValueError('fixture does not retain the complete genesis range')
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
        config['run-with-range']=checkpoint.CheckpointBound(tip+1)
        configs[mode]=config
        (root/f'{mode}.yaml').write_text(yaml.dump(config,Dumper=checkpoint.ExperimentDumper))
    native_root,remote_root=(Path(configs[m]['db-path']) for m in ('native','remote'))
    env={k:v for k,v in os.environ.items() if not k.startswith(('TH_', 'SUI_SHARDED_WAL_', 'SUI_DISTRIBUTED_', 'DISTRIBUTED_TIDEHUNTER_', 'TIDEHUNTER_'))}
    env.update(RUST_LOG='info', TOKIO_WORKER_THREADS='4', USE_TIDEHUNTER='1',
               TH_SYNC_WRITES='true', TH_FRAG_SIZE=str(8*deployment.MIB), TH_MAX_MAPS='3', TH_MAX_INDEX_MAPS='3',
               TH_DEFAULT_MUTEX_COUNT='1', TH_DEFAULT_MAX_DIRTY_KEYS='128', TH_DEFAULT_VALUE_CACHE_SIZE='1000',
               SUI_TH_READ_PROFILE_LIMIT=str(args.read_profile_limit),
               SUI_TH_NATIVE_WRITE_PROFILE_LIMIT=str(args.native_write_profile_limit))
    owned=Owned(output,env)
    producer_env = checkpoint.source_environment(prior)
    result=dict(schema=1,protocol=9,backend='tidehunter-sharded-wal',durability='synchronous durable whole-batch publication',
        fixture=str(fixture),fixture_files=fixture_files,checkpoint_limit=tip,checkpoint_digest=expected_digest,
        build_manifest_sha256=build.digest(build_root/'build-manifest.json'),source_archive_sha256=manifest['source_archive_sha256'],
        binaries=manifest['binaries'],configurations=configs,
        environment={k:v for k,v in env.items() if k.startswith(('RUST_LOG','TOKIO_','USE_TIDEHUNTER','TH_','SUI_TH_','LD_LIBRARY_PATH','FI_'))},
        cache=dict(coordinator_bytes=args.coordinator_cache_mib*deployment.MIB,shard_bytes=args.shard_cache_mib*deployment.MIB),
        topology='five physical shards per database; '+('external hosts' if args.external_config else 'loopback, one SSD/shared host memory'),
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
        producer=owned.start('producer',[bins/'sui-node-native','--config-path',root/'producer.yaml','--run-with-range-checkpoint',tip+1], producer_env)
        address=configs['producer']['json-rpc-address']
        if int(checkpoint.wait_rpc(producer,address)) != tip:
            raise ValueError('producer tip differs')
        result['source_range']=checkpoint.verify_source_range(address,tip)
        if result['source_range'] != prior['verified_source_range']:
            raise ValueError('complete source digest range differs')
        native_extra=dict(SUI_SHARDED_WAL_ROOT=str(native_root), SUI_SHARDED_WAL_SCHEMA_OUTPUT=str(output/'native-schema'))
        result['native']=replay(owned,bins/'sui-node-native',root,'native',tip,expected_digest,native_extra,[],args.deadline)
        schema=deployment.schemas(output/'native-schema', 'native')
        shards=[]
        if args.external_config:
            config_path=args.external_config.resolve(strict=True)
            config=read_json(config_path)
            if Path(config['root']) != remote_root or set(config['databases']) != set(schema):
                raise ValueError('external namespace/root coverage differs')
            for database in config['databases'].values():
                if database['tls']['transport'] in deployment.SOFTWARE_TRANSPORTS:
                    raise ValueError('external software fabric is loopback-only in the native engine')
                if len(database['endpoints']) != 5 or len(set(e['address'].rsplit(':',1)[0] for e in database['endpoints'])) != 5:
                    raise ValueError('external mode requires five distinct storage hosts per DB')
                if database['read_cache_bytes'] != result['cache']['coordinator_bytes']:
                    raise ValueError('external coordinator cache differs')
                if (resident is not None and
                        deployment.resident_read_config(database['tls'].get('resident_reads')) != resident):
                    raise ValueError('external resident-read policy differs from requested population')
            external_manifest=read_json(config_path.parent/'external-manifest.json')
            if (external_manifest['config_sha256'] != build.digest(config_path)
                    or external_manifest['build_manifest_sha256'] != result['build_manifest_sha256']
                    or external_manifest['shard_cache_bytes'] != result['cache']['shard_bytes']):
                raise ValueError('external storage source/config/cache binding differs')
            result['external_manifest']=external_manifest
        else:
            config_path=output/'deployment/config.json'
            config,nodes=deployment.prepare(config_path.parent,schema,remote_root,
                shape_yamls={name:(output/'native-schema'/name/'shape.yaml').read_text() for name in schema},hosts=['127.0.0.1']*5,
                port=args.port,transport=args.local_transport,coordinator_cache=result['cache']['coordinator_bytes'],
                shard_cache=result['cache']['shard_bytes'],write_profile_limit=args.write_profile_limit,
                resident_reads=resident)
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
        result['remote']=replay(owned,bins/'sui-node-remote',root,'remote',tip,expected_digest,remote_extra,shards,args.deadline)
        if args.external_config:
            result['external_stages'].append(publish_stage(args.external_stage_dir, 'remote-finished', binding))
        if deployment.schemas(output/'remote-schema', 'sharded-wal') != schema:
            raise ValueError('remote selected schema differs from native')
        from analyze import adapter_stats, checkpoint_sequence_cache_stats, metrics
        counters = adapter_stats(metrics(output/'remote-foreground.prom'))
        required = {'read_calls','read_keys','read_rpcs','cache_hits','cache_misses','shard_sample_available',
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
        if (set(counters) != set(schema) or any(not required.issubset(stats) for stats in counters.values())
                or any(stats['shard_sample_available'] != 1 or stats['remote_timing_available'] != 1
                       or stats['read_profile_limit'] != args.read_profile_limit
                       or stats['write_profile_limit'] != args.write_profile_limit for stats in counters.values())):
            raise ValueError('API/key/RPC/cache/stage instrumentation lacks selected DB coverage')
        if any(stats['scalar_read_ns_available'] != int(args.read_profile_limit > 0)
               or stats['read_index_ns_available'] != int(args.read_profile_limit > 0)
               for stats in counters.values()):
            raise ValueError('scalar/index diagnostic timing availability differs from selected population')
        validate_profiles(counters, config['databases'], args.read_profile_limit, args.write_profile_limit)
        validate_checkpoint_diagnostics(counters, args.write_profile_limit)
        native_counters = adapter_stats(metrics(output/'native-foreground.prom'))
        if set(native_counters) != set(schema) or any(
                stats.get('remote_timing_available') != 0 or 'write_gate_wait_calls' not in stats
                for stats in native_counters.values()):
            raise ValueError('native write-gate timing instrumentation is unavailable')
        validate_native_profiles(native_counters, args.native_write_profile_limit)
        for mode in ('native', 'remote'):
            if not checkpoint_sequence_cache_stats(metrics(output / f'{mode}-foreground.prom')):
                raise ValueError('checkpoint sequence-cache counters are unavailable')
        result['audit']=audit(root,output,bins,schema,native_root,remote_root,config_path,env)
        if producer.poll() is not None or int(checkpoint.rpc(address,'sui_getLatestCheckpointSequenceNumber')) != tip or checkpoint.verify_source_range(address,tip)!=result['source_range']:
            raise RuntimeError('source changed across paired replays')
        if args.external_config:
            result['external_stages'].append(publish_stage(args.external_stage_dir, 'resources-required', binding))
            wait_resources(args.external_stage_dir, binding)
            resources = read_json(args.external_resources)
            validate_resources(resources, result['external_manifest'], binding, result['external_stages'])
            result['external_resources'] = resources
        for name, expected in manifest['binaries'].items():
            if build.digest(bins/name) != expected:
                raise ValueError('binary changed during replay/audit')
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
