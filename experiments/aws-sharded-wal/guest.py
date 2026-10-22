#!/usr/bin/env python3
"""Bounded guest supervisor for the new fork only; EC2 control is scoped by exact IDs/tags.

Launched by the operator via AWS MCP/SSM. Guest SDK calls use its scoped instance
role. SSM command timeouts remain unknown outcomes; deterministic node actions
are idempotent and each case has a fresh namespace. No SSH is used.
"""
import argparse
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import tarfile
import time

import efa_progress
import checkpoint_async
import cluster_ram
import scalar_transport
import transport_floor
import stage_timing
import prepare_bundle
import local_compare
from checkpoint_matrix import HARDWARE_TRANSPORTS, TRANSPORTS

import boto3
from boto3.s3.transfer import TransferConfig
from boto3.exceptions import S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

SDK_CONFIG = Config(connect_timeout=5, read_timeout=15,
                    retries={'total_max_attempts': 2, 'mode': 'standard'})
TRANSFER = TransferConfig(max_concurrency=2, multipart_threshold=8 * 1024 * 1024,
                          multipart_chunksize=8 * 1024 * 1024)
CACHE_BYTES = 8 * 1024 * 1024


def benchmark_case_identity(label, shape, repetition, *, storage=False, capacity=False,
                            chunk_control=False, cold=False, diagnostics=False, transport='tcp',
                            cached=False, authority_resident_batches=None, shard_cache_bytes=0,
                            preparation_chunk_batches=16, wal_layout=None):
    """Canonical outer identity, shared by execution and closed evidence admission."""
    value, batch, window = shape
    prefix = 'chunk32' if chunk_control else ('capacity4m' if capacity else ('storage' if storage else 'warm'))
    if cold or diagnostics:
        prefix = 'diagnostic' if diagnostics else 'cold'
    if cached:
        prefix = 'cache'
    if transport in HARDWARE_TRANSPORTS:
        prefix += '-' + transport
    if shard_cache_bytes:
        prefix = f'shardcache{shard_cache_bytes}-' + prefix
    if preparation_chunk_batches != 16:
        prefix = f'preparation{preparation_chunk_batches}-' + prefix
    if wal_layout == 'sui':
        prefix = 'wal-sui-' + prefix
    if authority_resident_batches is not None:
        prefix = f'authority{authority_resident_batches}-' + prefix
    return f'{prefix}-{label}-v{value}-b{batch}-w{window}-r{repetition}', prefix


def kv_case_commands(cfg, spec):
    """Build the admitted KV workload once for execution and offline admission.

    Paths belong to the trusted guest configuration, never to captured receipts
    or the local archive extraction directory. Other campaigns retain their own
    command construction and CLI controls.
    """
    root = Path(cfg['run_root'])
    case_id, _ = benchmark_case_identity('current', spec['shape'], spec['repetition'],
        storage=True, cold=spec['cold'], diagnostics=spec['diagnostics'],
        transport=spec['transport'], cached=spec['cached'],
        authority_resident_batches=spec['authority_resident_batches'],
        shard_cache_bytes=spec.get('shard_cache_bytes', 0),
        preparation_chunk_batches=spec.get('preparation_chunk_batches', 16),
        wal_layout=spec.get('wal_layout'))
    case_root = root / 'cases' / case_id
    binary = str(root / 'cloud-bin/current/sharded_wal_bench')
    value, batch, window = spec['shape']
    batches = 512 if spec['authority_resident_batches'] is not None else 320
    if spec.get('workload_batches') is not None:
        batches = spec['workload_batches']
    reads = spec.get('workload_reads')
    if reads is None:
        reads = 4096
    cache_bytes = CACHE_BYTES if spec['cached'] else 0
    shape_args = ['--values', str(value), '--batch-sizes', str(batch), '--windows', str(window),
                  '--batches', str(batches), '--mixed', '64', '--reads', str(reads), '--repetitions', '1']
    if spec.get('wal_layout') is not None:
        shape_args += ['--wal-layout', spec['wal_layout']]
    endpoints = ','.join(node['private_ip'] + ':7700'
                         for node in sorted(cfg['nodes'], key=lambda node: node['partition']))
    prepare = [binary, '--prepare-external-case', str(case_root), '--guest-case-root', str(case_root),
               '--remote-endpoints', endpoints, '--tcp-read-cache-bytes', str(cache_bytes),
               '--transport', spec['transport'], '--tcp-server-read-workers', '2',
               '--tcp-wal-member-read', 'async', '--tcp-resident-first', 'true',
               '--tcp-polling', 'adaptive', '--tcp-shard-cache-bytes', str(spec.get('shard_cache_bytes', 0))]
    prepare += shape_args
    commands = {}
    for mode in ('native-sync', 'tcp'):
        output = root / 'checkpoint-results/kv' / (case_id + '-' + mode)
        command = [binary, '--output', str(output), '--backend', mode] + shape_args
        command += ['--read-resource-evidence', 'true']
        if spec.get('cache_hit_contention', False):
            command += ['--cache-hit-contention', 'true']
        if spec['cold']:
            command += ['--cold-read-pass', 'true']
        if spec['diagnostics']:
            command += ['--transport-diagnostics', 'true']
        command += ['--read-window', '32']
        if mode == 'native-sync':
            command += ['--native-read-cache-bytes', str(cache_bytes)]
        if not spec['cold']:
            command += ['--hot-read-pass', 'true']
        if mode == 'tcp':
            command += ['--remote-config', str(case_root / 'remote.yaml'),
                        '--tcp-read-cache-bytes', str(cache_bytes),
                        '--transport', spec['transport'], '--tcp-preparation-chunk-batches',
                        str(spec.get('preparation_chunk_batches', 16))]
        commands[mode] = command
    return prepare, commands


def kv_case_policy(cfg, spec):
    """Complete semantic admission policy for the source-bound KV deployment.

    Values follow sharded_wal_bench::BackendParameters, tcp_cluster's external
    constructor and the serialized runtime schemas/defaults. The authority-window
    transform changes only resident caps. These defaults are admission constants,
    not values learned from captured YAML; drift requires explicit readmission.
    """
    prepare, _ = kv_case_commands(cfg, spec)
    arguments = dict(zip(prepare[1::2], prepare[2::2], strict=True))
    lifetime = int(arguments['--batches']) + 2 * int(arguments['--mixed']) + 8
    resident = spec['authority_resident_batches']
    if resident is None:
        resident = lifetime
    sui = arguments.get('--wal-layout', 'small') == 'sui'
    geometry = dict(fragment_bytes=(16 if sui else 1) * 1024**2,
                    wal_file_bytes=(256 if sui else 128) * 1024**2,
                    group_limits=dict(max_batches=32,
                                      max_encoded_bytes=12 * 1024**2 if sui else 1024**2 - 8))
    partitions = sorted(node['partition'] for node in cfg['nodes'])
    inventory = dict(database=[42] * 16, epoch=1, segment_generation=1, partitions=partitions)
    batch_limits = dict(max_operations=32, max_encoded_bytes=320 * 1024)
    common_partition = geometry | dict(batch_limits=batch_limits, max_reference_metadata_bytes=16 * 1024**2)
    case_root = Path(arguments['--guest-case-root'])
    def tls(identity, server_name):
        if arguments['--transport'] == 'efa-native':
            return dict(transport='efa-native', ca='', cert='', key='', server_name=server_name)
        return dict(transport=arguments['--transport'], ca=str(case_root / 'tls/ca.der'),
                    cert=str(case_root / 'tls' / (identity + '.der')),
                    key=str(case_root / 'tls' / (identity + '.pk8')), server_name=server_name)
    remote = dict(
        storage=common_partition | dict(database=[42] * 16, partitions=partitions,
            max_batches_per_partition=resident, max_epochs=4),
        endpoints=[dict(partition=node['partition'], address=node['private_ip'] + ':7700',
                        server_name=f"tidehunter-node-{node['partition']}")
                   for node in sorted(cfg['nodes'], key=lambda node: node['partition'])],
        tls=tls('tidehunter-authority', 'unused-client-template'), timeout=dict(secs=10, nanos=0),
        max_admitted_batches=lifetime, max_index_bytes=128 * 1024**2,
        max_pending_batches=128, max_pending_bytes=64 * 1024**2, pipelined_writes=True,
        read_cache_bytes=int(arguments['--tcp-read-cache-bytes']),
        read_memory_allowance_bytes=512 * 1024**2, preparation_workers=2, scan_prefetch_pages=2,
        read_lanes=2, max_pending_reads=256, max_pending_read_bytes=64 * 1024**2, write_profile_limit=0)
    shape = dict(key_spaces=[dict(inner=dict(
        id=0, name='root', key_indexing=('Fixed', 8), mutexes=16,
        key_type=('Uniform', dict(cells_per_mutex=64)),
        config=dict(disable_unload=False, max_dirty_keys=None, l0_max_entries=None, bloom_filter=None,
                    value_cache_size=0, index_format='Header', unloaded_iterator=True), destroyed=False))])
    owners = {}
    for node in cfg['nodes']:
        partition = node['partition']
        node_root = case_root / f'node-{partition}'
        owners[partition] = dict(
            node=dict(root=str(node_root / 'storage'),
                partition=common_partition | dict(inventory=inventory, partition=partition, max_batches=resident),
                shape_yaml=shape, max_epochs=4,
                read_workers=int(arguments['--tcp-server-read-workers']),
                wal_member_read=arguments['--tcp-wal-member-read'],
                read_resident_first=arguments['--tcp-resident-first'] == 'true',
                read_cache_bytes=int(arguments['--tcp-shard-cache-bytes']),
                read_memory_allowance_bytes=512 * 1024**2, read_max_inflight_requests=32,
                read_max_inflight_bytes=64 * 1024**2, read_max_device_reads=128),
            tls=tls(f'tidehunter-node-{partition}', f'tidehunter-node-{partition}'),
            limits=dict(max_frame_bytes=16 * 1024**2, max_connections=16, max_reads=256,
                        recovery_page_batches=128, io_timeout=dict(secs=60, nanos=0),
                        polling=arguments['--tcp-polling']),
            listen=node['private_ip'] + ':7700', ready_file=str(node_root / 'ready'),
            stop_file=str(node_root / 'stop'), drop_first_append_response=False)
    return remote, owners


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.pending')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def remaining(deadline):
    return datetime.fromisoformat(deadline.replace('Z', '+00:00')).timestamp() - time.time()


def safe_extract(archive, destination, max_bytes=512 * 1024 * 1024):
    destination = Path(destination)
    with tarfile.open(archive) as stream:
        members = stream.getmembers()
        if sum(m.size for m in members) > max_bytes:
            raise ValueError('archive exceeds expanded-size budget')
        for m in members:
            path = Path(m.name)
            if path.is_absolute() or '..' in path.parts or not (m.isfile() or m.isdir()):
                raise ValueError('archive contains a link or unsafe path')
        stream.extractall(destination, members=members, filter='data')


def upload(s3, path, cfg, key):
    path = Path(path)
    digest = sha(path)
    s3.upload_file(str(path), cfg['bucket'], key,
                   ExtraArgs={'ExpectedBucketOwner': cfg['account'], 'ServerSideEncryption': 'AES256',
                              'Metadata': {'sha256': digest}}, Config=TRANSFER)
    observed = s3.head_object(Bucket=cfg['bucket'], Key=key, ExpectedBucketOwner=cfg['account'])
    if observed['ContentLength'] != path.stat().st_size or observed.get('Metadata', {}).get('sha256') != digest:
        raise ValueError('uploaded object receipt does not match local archive')
    return {'bucket': cfg['bucket'], 'key': key, 'sha256': digest, 'bytes': path.stat().st_size}


def upload_final(s3, path, cfg, key):
    """Preserve the export reserve for transient failures, never extend expiry."""
    while True:
        if remaining(cfg['deadline_utc']) <= 30:
            raise TimeoutError('final export reserve exhausted; cleanup must proceed')
        try:
            return upload(s3, path, cfg, key)
        except (ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError):
            pass
        except S3UploadFailedError as error:
            # Managed-transfer errors wrap their service exception. Retry only
            # identifiable transient conditions; denied/missing-key credentials
            # and other permanent failures do not consume the whole reserve.
            if not re.search(r'SlowDown|InternalError|ServiceUnavailable|RequestTimeout|timed out|Connection|EOF', str(error)):
                raise
        except ClientError as error:
            # This final export boundary also covers HEAD/service errors after
            # the SDK's own bounded retries. Permanent errors propagate intact.
            if error.response.get('Error', {}).get('Code') not in {
                    'SlowDown', 'InternalError', 'ServiceUnavailable', 'RequestTimeout', '500', '503'}:
                raise
        left = remaining(cfg['deadline_utc']) - 30
        if left <= 0:
            raise TimeoutError('final export retry deadline reached')
        time.sleep(min(3, left))


def download(s3, cfg, key, expected, target):
    s3.download_file(cfg['bucket'], key, str(target),
                     ExtraArgs={'ExpectedBucketOwner': cfg['account']}, Config=TRANSFER)
    if sha(target) != expected:
        raise ValueError('downloaded object differs from immutable SHA-256')


def validate_cfg(cfg):
    if cfg.get('campaign') == 'stage-timing':
        stage_timing.validate_configuration(cfg)
    if cfg.get('campaign', 'overlap') not in ['overlap', 'read-path', 'efa', 'efa-cache', 'efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing']:
        raise ValueError('unknown benchmark campaign')
    if cfg.get('campaign') in ['efa', 'efa-cache', 'efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'] and not cfg.get('efa'):
        raise ValueError('EFA campaigns require an EFA-enabled fleet')
    scope = cfg.get('checkpoint_scope', 'full')
    allowed_scopes = (stage_timing.SCOPES if cfg.get('campaign') == 'stage-timing' else transport_floor.SCOPES if cfg.get('campaign') == 'transport-floor' else
                      scalar_transport.SCOPES if cfg.get('campaign') == 'scalar-transport' else
                      cluster_ram.SCOPES if cfg.get('campaign') == 'cluster-ram' else ('full', 'recovery'))
    if scope not in allowed_scopes or (scope != 'full' and cfg.get('campaign') not in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')):
        raise ValueError('invalid checkpoint campaign scope')
    run_id = cfg['run_id']
    if not re.fullmatch(r'th-[a-f0-9]{16}', run_id) or not re.fullmatch(r'[0-9]{12}', cfg['account']):
        raise ValueError('invalid exact run/account identity')
    root = Path('/mnt/tidehunter') / run_id
    if Path(cfg['run_root']) != root or cfg['s3_prefix'] != run_id:
        raise ValueError('run paths and S3 prefix must match exact run ID')
    nodes = cfg['nodes']
    if sorted(n['partition'] for n in nodes) != list(range(5)):
        raise ValueError('exactly five unique partitions required')
    ids = [cfg['coordinator_instance_id']] + [n['instance_id'] for n in nodes]
    if len(set(ids)) != 6 or any(not re.fullmatch(r'i-[0-9a-f]{8,17}', i) for i in ids):
        raise ValueError('six distinct exact EC2 IDs required')
    for n in nodes:
        socket.inet_aton(n['private_ip'])
    if not 300 <= cfg.get('export_reserve_seconds', 600) <= 900:
        raise ValueError('export reserve must be 300..900 seconds')
    reviewed_types = ('g6.16xlarge', 'c6id.32xlarge') if cfg.get('campaign') == 'transport-floor' else ('g6.16xlarge',)
    if cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor') and (cfg.get('export_reserve_seconds') != 600 or
            cfg.get('region') != 'us-east-1' or cfg.get('instance_type') not in reviewed_types):
        raise ValueError('checkpoint profile requires reviewed hardware and fixed export reserve')
    if cfg.get('campaign') == 'transport-floor' and not re.fullmatch(r'us-east-1[a-f]', cfg.get('availability_zone', '')):
        raise ValueError('transport floor requires an explicit reviewed US East zone')
    if cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') and cfg.get('terminate_on_finish') is not True:
        raise ValueError('source-bound transport campaign requires owned-fleet termination')
    if cfg.get('repetitions', 2) not in ([3] if cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') else [5] if cfg.get('campaign') in ('checkpoint-async', 'cluster-ram') else [1, 2, 3]):
        raise ValueError('invalid profile repetition count')
    if cfg.get('storage_repetitions', 2) not in [0, 1, 2, 3]:
        raise ValueError('storage repetitions must be zero to three')
    if cfg.get('capacity_repetitions', 2) not in [0, 1, 2]:
        raise ValueError('capacity repetitions must be zero to two')
    if cfg.get('chunk_control_repetitions', 0) not in [0, 1, 2, 3]:
        raise ValueError('chunk controls must be zero to three')
    if not 0 < remaining(cfg['deadline_utc']) <= 7200:
        raise ValueError('fixed campaign deadline expired or exceeds two hours')
    return root, ids


def prepare_capacity_4m(case_root):
    """Change only the generated fragment/group caps, validating all files first.

    This deliberately recognizes the exact serde YAML emitted by the frozen
    generator. A schema/default change fails closed instead of partly changing
    the coordinator and nodes. Batch/frame/metadata limits remain unchanged.
    """
    case_root = Path(case_root)
    paths = [case_root / 'remote.yaml'] + [case_root / f'node-{p}/launch.yaml' for p in range(5)]
    updated = []
    for path in paths:
        text = path.read_text()
        for pattern, replacement in [
                (r'(?m)^( +fragment_bytes:) 1048576$', r'\g<1> 4194304'),
                (r'(?m)^( +group_limits:\n +max_batches: 32\n +max_encoded_bytes:) 1048568$',
                 r'\g<1> 4194296')]:
            text, count = re.subn(pattern, replacement, text)
            if count != 1:
                raise ValueError(f'capacity transform expected one original field in {path.name}')
        updated.append((path, text))
    for path, text in updated:
        path.write_text(text)
    return {'fragment_bytes': 4194304, 'max_group_encoded_bytes': 4194296,
            'aggregate_value_mapping_allowance_bytes': 60 * 1024 * 1024,
            'matched_matrix_value_mapping_allowance_bytes': 15 * 1024 * 1024,
            'variant': 'capacity4m', 'native_comparison': False}


def prepare_authority_window(case_root, resident_batches):
    """Bound resident authority without changing lifetime batch admission."""
    import yaml
    if type(resident_batches) is not int or not 32 <= resident_batches <= 512:
        raise ValueError('authority resident window must be 32..512 batches')
    root = Path(case_root)
    paths = [root / 'remote.yaml', *(root / f'node-{p}/launch.yaml' for p in range(5))]
    documents = [yaml.safe_load(path.read_text()) for path in paths]
    storage = documents[0]['storage']
    if storage['group_limits']['max_batches'] > resident_batches:
        raise ValueError('authority window cannot split an atomic append group')
    storage['max_batches_per_partition'] = resident_batches
    for document in documents[1:]:
        partition = document['node']['partition']
        if partition['group_limits']['max_batches'] > resident_batches:
            raise ValueError('node authority window cannot split a group')
        partition['max_batches'] = resident_batches
    for path, document in zip(paths, documents):
        path.write_text(yaml.safe_dump(document, sort_keys=False))
    return {'resident_batches': resident_batches,
            'scope': 'bounded nonresident-authority stress; not verified cold metadata pages'}


def evidence(path):
    result = {'utc': datetime.now(timezone.utc).isoformat()}
    for name in ['diskstats', 'vmstat', 'meminfo', 'stat']:
        result[name] = Path('/proc', name).read_text()
    result['collector_cgroup_memory'] = local_compare.cgroup_memory()
    result['findmnt'] = subprocess.check_output(['findmnt', '--json', '--target', str(path)], text=True)
    result['lsblk'] = subprocess.check_output(['lsblk', '--json', '--output', 'PATH,MODEL,TYPE,FSTYPE,MOUNTPOINTS'], text=True)
    result['efa_counters'] = {}
    for counters in sorted(Path('/sys/class/infiniband').glob('*/ports/*/hw_counters'))[:16]:
        for counter in sorted(counters.iterdir())[:128]:
            try:
                result['efa_counters'][str(counter)] = counter.read_text().strip()
            except OSError as error:
                result['efa_counters'][str(counter)] = {'error': str(error)}
    return result


def sample_threads(pid, samples):
    """Bounded lifetime counters; sampled by supervisor, outside API timers."""
    for task in Path(f'/proc/{pid}/task').glob('[0-9]*'):
        try:
            stat = (task / 'stat').read_text().rsplit(')', 1)[1].split()
            status = dict(line.split(':', 1) for line in (task / 'status').read_text().splitlines() if ':' in line)
            tid, start = int(task.name), int(stat[19])
            counters = dict(user_ticks=int(stat[11]), system_ticks=int(stat[12]),
                            voluntary_context_switches=int(status['voluntary_ctxt_switches']),
                            nonvoluntary_context_switches=int(status['nonvoluntary_ctxt_switches']))
            key = f'{tid}:{start}'
            if key not in samples:
                if len(samples) >= 4096:
                    raise ValueError('thread evidence inventory exceeds bound')
                samples[key] = dict(tid=tid, start_ticks=start, first=counters, samples=0)
            samples[key].update(last=counters, samples=samples[key]['samples'] + 1)
        except (FileNotFoundError, ProcessLookupError):
            continue


def thread_document(pid, samples):
    return dict(schema=1, pid=pid, ticks_per_second=os.sysconf('SC_CLK_TCK'), threads=samples,
                scope='sampled thread lifetimes, not benchmark phase CPU')


def monitor_threads(pid, output, deadline):
    samples = {}
    while Path(f'/proc/{pid}').exists() and remaining(deadline) > 0:
        sample_threads(pid, samples)
        write_json(output, thread_document(pid, samples))
        time.sleep(0.5)


def run_recorded(command, output, deadline, env=None, cwd=None, expected_binary_sha256=None):
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    state = {'command': command, 'started_utc': datetime.now(timezone.utc).isoformat()}
    if expected_binary_sha256 is not None:
        state['binary_sha256'] = sha(Path(command[0]))
        if state['binary_sha256'] != expected_binary_sha256:
            raise ValueError('process executable differs from source-bound build receipt')
    write_json(output.with_suffix('.command.json'), state)
    with output.with_suffix('.stdout.log').open('wb') as stdout, output.with_suffix('.stderr.log').open('wb') as stderr:
        start = time.monotonic()
        child = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env,
                                 cwd=cwd, start_new_session=True)
        state['pid'] = child.pid
        peak = 0
        threads = {}
        next_thread_sample = 0
        try:
            while child.poll() is None:
                if remaining(deadline) <= 0:
                    raise TimeoutError('fixed phase deadline reached')
                if time.monotonic() >= next_thread_sample:
                    sample_threads(child.pid, threads)
                    next_thread_sample = time.monotonic() + 0.5
                try:
                    status = Path(f'/proc/{child.pid}/status').read_text()
                    rss = int(re.search(r'^VmRSS:\s+(\d+)', status, re.M).group(1))
                    peak = max(peak, rss)
                except (FileNotFoundError, AttributeError):
                    pass
                time.sleep(0.05)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
            state.update(exit_code=child.returncode, elapsed_seconds=time.monotonic() - start,
                         peak_sampled_main_process_rss_kib=peak)
            write_json(output.with_suffix('.command.json'), state)
            write_json(output.with_suffix('.threads.json'), thread_document(child.pid, threads))
        if child.returncode:
            raise subprocess.CalledProcessError(child.returncode, command)


def wait_node_exit(unit, pid, timeout=20):
    """Wait for both process death and collection of the owned transient unit."""
    service = unit if unit.endswith('.service') else unit + '.service'
    limit = time.monotonic() + timeout
    while True:
        left = limit - time.monotonic()
        if left <= 0:
            raise TimeoutError(f'node exit/collection deadline: {service}, PID {pid}')
        # --collect also unloads failed units. An empty successful listing proves
        # collection; a failed systemctl/bus request is never proof of exit.
        loaded = subprocess.run(
            ['systemctl', 'list-units', '--all', '--no-legend', '--plain', '--no-pager', service],
            check=True, capture_output=True, text=True, timeout=min(5, left))
        if not loaded.stdout.strip() and not Path(f'/proc/{pid}').exists():
            return
        time.sleep(min(0.1, max(0, limit - time.monotonic())))


def crash_node_service(unit):
    pid = int(subprocess.check_output(
        ['systemctl', 'show', unit, '--property=MainPID', '--value'], text=True, timeout=5).strip())
    if pid <= 0 or not Path(f'/proc/{pid}').exists():
        raise ValueError('restart requires a live original server PID')
    subprocess.run(['systemctl', 'kill', '--signal=SIGKILL', '--kill-whom=all', unit],
                   check=True, timeout=15)
    # A separate stop races with --collect after SIGKILL. Wait instead, including
    # deactivation/GC, before reusing the exact unit name for the fresh process.
    wait_node_exit(unit, pid)
    return pid


def node_action(payload):
    cfg = payload['config']
    root, _ = validate_cfg(cfg)
    info = json.loads((root / 'bootstrap.json').read_text())
    node = next(n for n in cfg['nodes'] if n['instance_id'] == info['identity']['instanceId'])
    if info['identity']['accountId'] != cfg['account']:
        raise ValueError('guest account mismatch')
    case_id = payload['case_id']
    if not re.fullmatch(r'[a-z0-9-]{1,100}', case_id):
        raise ValueError('invalid case namespace')
    session = boto3.Session(region_name=cfg['region'])
    s3 = session.client('s3', config=SDK_CONFIG)
    control = root / 'node-control'
    control.mkdir(exist_ok=True)
    case = root / 'cases' / case_id
    unit = f"{cfg['run_id']}-{case_id}-p{node['partition']}"
    state_path = control / (case_id + '.json')
    stopped = control / (case_id + '.stop-requested')
    with (control / (case_id + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if payload['action'] in ('start', 'restart'):
            if stopped.exists():
                raise ValueError('case already has a terminal stop request')
            desired = {'binary_sha256': payload['binary_sha256'], 'archive_sha256': payload['node_archives'][str(node['partition'])]['sha256']}
            restarting = payload['action'] == 'restart'
            if cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                desired['deadline_utc'] = cfg['deadline_utc']
            node_dir = case / f"node-{node['partition']}"
            restart_started = time.monotonic_ns()
            if restarting:
                if cfg.get('campaign') not in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') or not state_path.exists():
                    raise ValueError('restart requires an existing checkpoint campaign case')
                if json.loads(state_path.read_text()) != desired:
                    raise ValueError('restart cannot change original source/config/deadline')
                if (node_dir / 'restart.json').exists() or (node_dir / 'stop').exists():
                    raise ValueError('restart already requested or node stopped')
                old_pid = crash_node_service(unit)
                (node_dir / 'ready').unlink(missing_ok=True)
                # Do not extract, initialize, remove data, or alter the absolute TTL.
                write_json(node_dir / 'restart.json', dict(case_id=case_id, partition=node['partition'],
                           old_pid=old_pid, deadline_utc=cfg['deadline_utc'], complete=False))
            if state_path.exists() and not restarting:
                if json.loads(state_path.read_text()) != desired:
                    raise ValueError('same namespace was used with different source/config')
                subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True)
                print('node_start_already_active=true')
                return
            if not restarting:
                staged = control / (case_id + '.tar.gz')
                artifact = payload['node_archives'][str(node['partition'])]
                download(s3, cfg, artifact['key'], artifact['sha256'], staged)
                case.parent.mkdir(exist_ok=True)
                case.mkdir(mode=0o700)
                safe_extract(staged, case, 2 * 1024 * 1024)
            binary = root / 'cloud-bin' / payload['build'] / 'sharded_wal_bench'
            if sha(binary) != payload['binary_sha256']:
                raise ValueError('node executable differs from campaign build')
            node_dir = case / f"node-{node['partition']}"
            if cfg.get('efa'):
                for name in ['efa-provider.txt', 'efa-version.txt']:
                    shutil.copy2(root / name, node_dir / name)
            if not restarting:
                write_json(node_dir / 'before.json', evidence(case) | {
                    'instance_id': info['identity']['instanceId'], 'partition': node['partition']})
            write_json(state_path, desired)
            if payload.get('require_efa_proof'):
                from checkpoint_matrix import efa_snapshot, validate_provider
                transport = payload.get('transport', 'efa')
                from sui_checkpoint import fabric_preflight
                fabric_preflight(node_dir, transport)
                provider = node_dir / 'efa-provider.txt'
                validate_provider(provider.read_text(), transport)
                identity = dict(run_id=cfg['run_id'], case_id=case_id,
                    instance_id=node['instance_id'], binary_sha256=payload['binary_sha256'])
                write_json(node_dir / 'efa-before.json', efa_snapshot(identity))
            command = ['systemd-run', '--quiet', '--collect', '--unit', unit,
                       '--property=RuntimeMaxSec=' + str(max(1, int(remaining(cfg['deadline_utc'])))),
                       '--property=LimitMEMLOCK=infinity', '--setenv=FI_EFA_USE_DEVICE_RDMA=1', '--setenv=FI_EFA_ENABLE_SHM_TRANSFER=0',
                       '--property=TimeoutStopSec=10', '--property=KillMode=control-group',
                       '--property=StandardOutput=append:' + str(node_dir / 'node.stdout.log'),
                       '--property=StandardError=append:' + str(node_dir / 'node.stderr.log'),
                       str(binary), '--experimental-storage-node', str(node_dir / 'launch.yaml')]
            subprocess.run(command, check=True, timeout=15)
            limit = time.monotonic() + min(30, remaining(cfg['deadline_utc']))
            while not (node_dir / 'ready').exists():
                subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True)
                if time.monotonic() >= limit:
                    raise TimeoutError('node readiness deadline')
                time.sleep(0.1)
            pid = int(subprocess.check_output(
                ['systemctl', 'show', unit, '--property=MainPID', '--value'], text=True).strip())
            if pid <= 0 or not Path(f'/proc/{pid}').exists():
                raise ValueError('ready file without live server process')
            ready = (node_dir / 'ready').read_text().strip()
            if not ready:
                raise ValueError('empty server readiness identity')
            if (cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') or cfg.get('kv_subsuite')) and sha(Path(f'/proc/{pid}/exe')) != payload['binary_sha256']:
                raise ValueError('ready process executable differs from source-bound binary')
            identity = ({'build': payload['build'], 'binary_sha256': payload['binary_sha256']}
                        if cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') or cfg.get('kv_subsuite') else {})
            if restarting:
                if pid == old_pid:
                    raise ValueError('restart reused original PID')
                write_json(node_dir / 'restart.json', dict(case_id=case_id, partition=node['partition'],
                           old_pid=old_pid, new_pid=pid, deadline_utc=cfg['deadline_utc'],
                           data_preserved=True, ready_removed_before_start=True, ready=ready,
                           node_restart_ns=time.monotonic_ns() - restart_started, complete=True, **identity))
            else:
                write_json(node_dir / 'start.json', dict(pid=pid, ready=ready, deadline_utc=cfg['deadline_utc'], **identity))
            if cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                subprocess.Popen(['python3', str(Path(__file__).resolve()), 'threads', '--pid', str(pid),
                                  '--output', str(node_dir / f'threads-{pid}.json'),
                                  '--deadline', cfg['deadline_utc']],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
            print(json.dumps({'ready': (node_dir / 'ready').read_text(), 'binary_sha256': sha(binary)}))
        elif payload['action'] == 'stop':
            stopped.touch(exist_ok=True)
            node_dir = case / f"node-{node['partition']}"
            if node_dir.exists():
                pid = subprocess.run(['systemctl', 'show', unit, '--property=MainPID', '--value'], capture_output=True, text=True)
                if pid.stdout.strip().isdigit() and int(pid.stdout.strip()) > 0:
                    try:
                        (node_dir / 'process-status.txt').write_text(Path('/proc', pid.stdout.strip(), 'status').read_text())
                        (node_dir / 'process-stat.txt').write_text(Path('/proc', pid.stdout.strip(), 'stat').read_text())
                    except FileNotFoundError:
                        pass
                (node_dir / 'stop').touch()
            subprocess.run(['systemctl', 'stop', unit], check=False, timeout=20)
            if subprocess.run(['systemctl', 'is-active', '--quiet', unit]).returncode == 0:
                raise ValueError('node service still running after stop')
            if payload.get('require_efa_proof') and (node_dir / 'efa-before.json').is_file():
                from checkpoint_matrix import efa_snapshot, efa_proof
                try:
                    before = json.loads((node_dir / 'efa-before.json').read_text())
                    after = efa_snapshot(before['identity'])
                    write_json(node_dir / 'efa-after.json', after)
                    proof = efa_proof(before, after, before['identity'])
                except (OSError, ValueError) as error:
                    # Export logs even for failed hardware proof. The controller
                    # rejects this explicit failure instead of admitting the case.
                    proof = {'complete': False, 'error': repr(error)}
                write_json(node_dir / 'efa-proof.json', proof)
            output = control / (case_id + '.result.tar.gz')
            if node_dir.exists():
                write_json(node_dir / 'after.json', evidence(case) | {
                    'instance_id': info['identity']['instanceId'], 'partition': node['partition']})
            with tarfile.open(output, 'w:gz') as archive:
                if node_dir.exists():
                    for item in sorted(node_dir.iterdir()):
                        if item.is_file() and item.name not in ['ready', 'stop']:
                            archive.add(item, arcname=item.name, recursive=False)
            key = f"{cfg['s3_prefix']}/results/nodes/{case_id}/node-{node['partition']}.tar.gz"
            receipt = upload(s3, output, cfg, key)
            receipt_path = control / (case_id + '.receipt.json')
            write_json(receipt_path, receipt)
            upload(s3, receipt_path, cfg, key + '.receipt.json')
            print(json.dumps(receipt))
        else:
            raise ValueError('unknown node action')


def read_path_cases():
    """Fixed 32 API cases: 24 warm comparisons, six cold and two diagnostics."""
    cases = []
    for rep in range(3):
        for shape in [(1024, 1, 1), (8192, 32, 32)]:
            for label in (['baseline', 'current'] if rep % 2 == 0 else ['current', 'baseline']):
                cases.append((label, shape, rep, {'storage': True}))
    for rep in range(3):
        cases.append(('current', (8192, 32, 32), rep, {'storage': True, 'cold': True}))
    cases.append(('current', (8192, 32, 32), 0,
                  {'storage': True, 'cold': True, 'diagnostics': True}))
    return cases


def efa_cases(include_cache=False):
    """68 API cases, or 116 with paired native/frontend cache measurements."""
    shapes = [(1024, 1, 1), (1024, 32, 32), (8192, 1, 1), (8192, 32, 32)]
    cases = [('baseline', shape, 0, {'storage': True}) for shape in shapes]
    for rep in range(3):
        for shape in shapes:
            for transport in (['tcp', 'efa'] if rep % 2 == 0 else ['efa', 'tcp']):
                cases.append(('current', shape, rep, {'storage': True, 'transport': transport}))
                if include_cache:
                    cases.append(('current', shape, rep,
                                  {'storage': True, 'transport': transport, 'cached': True}))
    for rep in range(2):
        for transport in ['tcp', 'efa']:
            cases.append(('current', (8192, 32, 32), rep, {'storage': True, 'cold': True, 'transport': transport}))
    for transport in ['tcp', 'efa']:
        cases.append(('current', (8192, 32, 32), 0, {'storage': True, 'cold': True, 'diagnostics': True, 'transport': transport}))
    return cases


class Campaign:
    def __init__(self, cfg):
        self.cfg = cfg
        self.root, self.ids = validate_cfg(cfg)
        self.bundle = self.root / 'bundle'
        self.results = self.root / 'results'
        self.results.mkdir()
        self.session = boto3.Session(region_name=cfg['region'])
        self.s3 = self.session.client('s3', config=SDK_CONFIG)
        self.ssm = self.session.client('ssm', config=SDK_CONFIG)
        self.ec2 = self.session.client('ec2', config=SDK_CONFIG)
        self.deadline = datetime.fromtimestamp(datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00')).timestamp() - cfg.get('export_reserve_seconds', 600), timezone.utc).isoformat()
        self.state = {'run_id': cfg['run_id'], 'complete': False, 'completed_cases': [], 'failures': []}
        self.cloud_builds = {}
        self.active_payload = None
        write_json(self.results / 'campaign-config.json', cfg)
        write_json(self.results / 'before.json', evidence(self.root))

    def progress(self, stage):
        self.state['stage'] = stage
        self.state['updated_utc'] = datetime.now(timezone.utc).isoformat()
        path = self.results / 'progress.json'
        write_json(path, self.state)
        try:
            upload(self.s3, path, self.cfg, f"{self.cfg['s3_prefix']}/results/progress.json")
        except Exception as error:
            # Live progress is advisory. Final evidence upload is mandatory and
            # has its own failure/cleanup boundary; never relaunch a case merely
            # because this out-of-band update was not observed.
            self.state.setdefault('progress_upload_errors', []).append(repr(error))
            write_json(path, self.state)

    def probe_export(self):
        path = self.results / 'export-permission-probe.json'
        write_json(path, {'run_id': self.cfg['run_id'], 'utc': datetime.now(timezone.utc).isoformat()})
        key = f"{self.cfg['s3_prefix']}/results/export-permission-probe.json"
        receipt = upload(self.s3, path, self.cfg, key)
        response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key,
                                      ExpectedBucketOwner=self.cfg['account'])
        with response['Body'] as body:
            actual = body.read(16 * 1024 + 1)
        if actual != path.read_bytes():
            raise ValueError('export read-back probe failed before cloud build')
        write_json(self.results / 'export-probe-receipt.json', receipt)

    def controller_evidence(self):
        identity = json.loads((self.root / 'bootstrap.json').read_text())['identity']
        if (identity['instanceId'] != self.cfg['coordinator_instance_id'] or
                identity['accountId'] != self.cfg['account']):
            raise ValueError('actual controller identity differs from owned inventory')
        return evidence(self.root) | {'instance_id': identity['instanceId']}

    def check_owned_instances(self, record=True):
        response = self.ec2.describe_instances(InstanceIds=self.ids)
        instances = [i for r in response['Reservations'] for i in r['Instances']]
        if {i['InstanceId'] for i in instances} != set(self.ids):
            raise ValueError('owned fleet inventory is incomplete')
        for r in response['Reservations']:
            if r['OwnerId'] != self.cfg['account']:
                raise ValueError('instance owner mismatch')
        expected = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': self.cfg['run_id']}
        for instance in instances:
            tags = {t['Key']: t['Value'] for t in instance.get('Tags', [])}
            if any(tags.get(k) != v for k, v in expected.items()):
                raise ValueError('refusing foreign instance')
            if record and instance['InstanceType'] != self.cfg.get('instance_type', 'i4i.xlarge'):
                raise ValueError('refusing wrong hardware for benchmark admission')
            if record and self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing'):
                zone = self.cfg['availability_zone'] if self.cfg.get('campaign') in ('transport-floor', 'stage-timing') else 'us-east-1a'
                if instance.get('Placement', {}).get('AvailabilityZone') != zone:
                    raise ValueError('source-bound transport campaign differs from planned placement')
        if record:
            if self.cfg.get('campaign') == 'stage-timing':
                groups = self.ec2.describe_placement_groups(GroupNames=[self.cfg['placement_group']])['PlacementGroups']
                stage_timing.validate_placement(self.cfg, instances, groups)
                write_json(self.results / 'placement-groups.json', groups)
            write_json(self.results / 'owned-instances.json', instances)

    def ssm_nodes(self, payload, *, timeout=180):
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        command = shlex.join(['python3', str(self.bundle / 'runner/guest.py'), 'node', '--payload-base64', encoded])
        nodes = [n['instance_id'] for n in self.cfg['nodes']]
        # If this reply is lost, outcome stays unknown. No replacement benchmark is
        # started; cleanup targets the same deterministic node units and hard TTL.
        reply = self.ssm.send_command(InstanceIds=nodes, DocumentName='AWS-RunShellScript',
                    Parameters={'commands': [command], 'executionTimeout': [str(timeout)]},
                    TimeoutSeconds=60, Comment=f"{self.cfg['run_id']} {payload['action']} {payload['case_id']}")
        command_id = reply['Command']['CommandId']
        record = self.results / f"ssm-{payload['case_id']}-{payload['action']}.json"
        results = {}
        limit = time.monotonic() + min(timeout + 30, max(1, remaining(self.cfg['deadline_utc'])))
        while len(results) < len(nodes):
            if time.monotonic() >= limit:
                raise TimeoutError('SSM command outcome unknown at fixed wait deadline')
            for instance in nodes:
                if instance in results:
                    continue
                try:
                    item = self.ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
                except self.ssm.exceptions.InvocationDoesNotExist:
                    continue
                if item['Status'] not in ['Pending', 'InProgress', 'Delayed']:
                    results[instance] = item
                    write_json(record, {'command_id': command_id, 'instances': results})
            if len(results) < len(nodes):
                time.sleep(1)
        if any(r['Status'] != 'Success' for r in results.values()):
            raise RuntimeError(f'SSM {payload["action"]} failed; inspect {record.name}')

    def build_pair(self):
        manifest = json.loads((self.bundle / 'bundle-manifest.json').read_text())
        for name, expected in manifest['files_sha256'].items():
            if sha(self.bundle / name) != expected:
                raise ValueError(f'bundle hash mismatch: {name}')
        # The guest starter supplies /root/.cargo/bin on PATH. Resolving through
        # PATH also permits this exact build gate to run as an unprivileged local
        # user without probing an inaccessible /root directory.
        cargo = shutil.which('cargo') or '/root/.cargo/bin/cargo'
        if self.cfg.get('efa'):
            for name in ['efa-provider.txt', 'efa-version.txt']:
                shutil.copy2(self.root / name, self.results / name)
        for label in ['baseline', 'current']:
            self.progress('building-' + label)
            # Archives intentionally normalize mtimes. A shared Cargo target can
            # therefore consider the other workspace's same-named crate fresh.
            # Only the download cache may be shared; build/fingerprint/output
            # directories must be independent and new for this exact source.
            target = self.root / 'build-target' / label
            target.mkdir(parents=True)
            environment = os.environ | {'CARGO_BUILD_JOBS': '2', 'CARGO_TARGET_DIR': str(target)}
            source = self.root / 'build-source' / label
            source.mkdir(parents=True)
            original = json.loads((self.bundle / label / 'source-manifest.json').read_text())
            if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing'):
                prepare_bundle.validate_source_archive(self.bundle / label / 'source.tar.gz', original)
                profile = transport_floor if self.cfg['campaign'] == 'transport-floor' else scalar_transport
                expected_source = (self.cfg['source_identities'][label]['source_tree_sha256']
                                   if self.cfg['campaign'] == 'stage-timing' else
                                   profile.BASELINE_SOURCE_SHA256 if label == 'baseline' else profile.CURRENT_SOURCE_SHA256)
                if self.cfg['campaign'] == 'stage-timing':
                    if stage_timing.source_identity(self.bundle / label, label) != self.cfg['source_identities'][label]:
                        raise ValueError('uploaded artifacts differ from immutable plan binding')
                    shutil.copy2(self.bundle / label / 'source.tar.gz', self.results / f'source-{label}.tar.gz')
                if original['source_tree_sha256'] != expected_source:
                    raise ValueError('source-bound transport source differs from campaign pin')
                if sha(self.bundle / label / 'sharded_wal_bench') != original['binary_sha256']:
                    raise ValueError('archived executable differs from source manifest')
            local_auxiliary = {}
            if self.cfg.get('campaign') in ('transport-floor', 'stage-timing'):
                shutil.copy2(self.bundle / label / 'source-manifest.json',
                             self.results / f'source-manifest-{label}.json')
                for kind, executable in [('recovery', 'sharded_wal_recovery_bench'),
                                         ('floor', 'sharded_wal_transport_bench')]:
                    key = kind + '_binary_sha256'
                    if key in original:
                        if sha(self.bundle / label / executable) != original[key]:
                            raise ValueError('archived auxiliary executable differs from source manifest')
                        local_auxiliary['local_' + key] = original[key]
            safe_extract(self.bundle / label / 'source.tar.gz', source)
            if (self.cfg.get('campaign') == 'efa-progress' and label == 'baseline' and
                    original['source_tree_sha256'] != efa_progress.BASELINE_SOURCE_SHA256):
                raise ValueError('progress baseline must be the preserved completed October 7 EFA source')
            checkpoint_profile = (stage_timing if self.cfg.get('campaign') == 'stage-timing' else transport_floor if self.cfg.get('campaign') == 'transport-floor' else scalar_transport if self.cfg.get('campaign') == 'scalar-transport' else
            cluster_ram if self.cfg.get('campaign') == 'cluster-ram' else checkpoint_async)
            if (self.cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') and label == 'baseline' and
                    (original['source_tree_sha256'] != checkpoint_profile.BASELINE_SOURCE_SHA256 or
                     original['binary_sha256'] != checkpoint_profile.BASELINE_BINARY_SHA256)):
                raise ValueError('baseline differs from the campaign-preserved source/binary pair')
            for name, expected in original['source_sha256'].items():
                if sha(source / name) != expected:
                    raise ValueError(f'{label} source differs: {name}')
            features = original.get('features', 'experimental-distributed')
            if self.cfg.get('efa') and label == 'current' and features != 'experimental-fabric,test-utils':
                raise ValueError('EFA campaign requires the frozen native fabric build')
            if self.cfg.get('campaign') in ('efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') and features != 'experimental-fabric,test-utils':
                raise ValueError('progress requires both preserved and current EFA builds')
            if features not in ['experimental-distributed', 'experimental-distributed,test-utils', 'experimental-fabric,test-utils']:
                raise ValueError('unsupported archived build feature set')
            if self.cfg.get('campaign') in ['read-path', 'efa', 'efa-cache'] and label == 'current' and 'test-utils' not in features:
                raise ValueError('cold-read campaign requires test-utils in the frozen manifest')
            command = [cargo, '+1.90.0', 'build', '--release', '--locked', '--manifest-path',
                       str(source / 'Cargo.toml'), '-p', 'tidehunter', '--features',
                       features, '--example', 'sharded_wal_bench']
            # Cargo reads .cargo/config from cwd, not from --manifest-path.
            # Preserve the recorded panic=abort and any future source config.
            run_recorded(command, self.results / ('build-' + label), self.deadline,
                         environment, cwd=source)
            if sha(source / 'Cargo.lock') != original['source_sha256']['Cargo.lock']:
                raise ValueError('cloud build changed Cargo.lock')
            binary_dir = self.root / 'cloud-bin' / label
            binary_dir.mkdir(parents=True)
            binary = binary_dir / 'sharded_wal_bench'
            shutil.copy2(target / 'release/examples/sharded_wal_bench', binary)
            receipt = {'label': label, 'binary_sha256': sha(binary),
                       'features': features,
                       'source_manifest_sha256': sha(self.bundle / label / 'source-manifest.json'),
                       'source_tree_sha256': original['source_tree_sha256'],
                       'command': command, 'target_directory': str(target),
                       'local_binary_sha256': original['binary_sha256'],
                       'compiler': subprocess.check_output([str(Path(cargo).with_name('rustc')), '+1.90.0', '--version'], text=True).strip()}
            receipt.update(local_auxiliary)
            if self.cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor'):
                overlay = self.bundle / 'recovery-overlay/sharded_wal_recovery_bench.rs'
                destination = source / 'tidehunter/examples/sharded_wal_recovery_bench.rs'
                if self.cfg.get('campaign') in ('cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                    # Each revision owns its recovery APIs and hash-bound example.
                    recovery_name = 'tidehunter/examples/sharded_wal_recovery_bench.rs'
                    if (not destination.is_file() or recovery_name not in original['source_sha256']
                            or sha(destination) != original['source_sha256'][recovery_name]):
                        raise ValueError('campaign requires manifest-bound per-revision recovery code')
                    overlay = destination
                else:
                    if destination.exists() and sha(destination) != sha(overlay):
                        raise ValueError('frozen recovery example differs from common overlay')
                    shutil.copy2(overlay, destination)
                recovery_command = [cargo, '+1.90.0', 'rustc', '--release', '--locked',
                                    '--manifest-path', str(source / 'Cargo.toml'), '-p', 'tidehunter',
                                    '--features', features, '--example', 'sharded_wal_recovery_bench',
                                    '--', '--check-cfg', 'cfg(checkpoint_recovery_stats)']
                recovery_stats = label == 'current' or self.cfg.get('campaign') in ('cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')
                if recovery_stats:
                    recovery_command += ['--cfg', 'checkpoint_recovery_stats']
                run_recorded(recovery_command, self.results / ('build-recovery-' + label),
                             self.deadline, environment, cwd=source)
                recovery_binary = binary_dir / 'sharded_wal_recovery_bench'
                shutil.copy2(target / 'release/examples/sharded_wal_recovery_bench', recovery_binary)
                for name, expected in original['source_sha256'].items():
                    if sha(source / name) != expected:
                        raise ValueError('recovery overlay mutated frozen engine: ' + name)
                receipt.update(source_archive_sha256=sha(self.bundle / label / 'source.tar.gz'),
                               lock_sha256=sha(source / 'Cargo.lock'), engine_source_unchanged=True,
                               recovery_overlay_sha256=sha(overlay),
                               recovery_binary_sha256=sha(recovery_binary), recovery_command=recovery_command,
                               recovery_cfg='checkpoint_recovery_stats' if recovery_stats else None)
                shutil.copy2(overlay, self.results / (f'sharded_wal_recovery_bench-{label}.rs'
                             if self.cfg.get('campaign') in ('cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') else 'sharded_wal_recovery_bench.rs'))
            if self.cfg.get('campaign') == 'stage-timing':
                receipt.update(source_archive_sha256=sha(self.bundle / label / 'source.tar.gz'),
                               lock_sha256=sha(source / 'Cargo.lock'), engine_source_unchanged=True)
            if self.cfg.get('campaign') == 'stage-timing' or (self.cfg.get('campaign') == 'transport-floor' and label == 'current'):
                floor_name = 'tidehunter/examples/sharded_wal_transport_bench.rs'
                if (floor_name not in original['source_sha256'] or
                        sha(source / floor_name) != original['source_sha256'][floor_name]):
                    raise ValueError('floor requires manifest-bound current transport benchmark')
                floor_command = [cargo, '+1.90.0', 'build', '--release', '--locked',
                                 '--manifest-path', str(source / 'Cargo.toml'), '-p', 'tidehunter',
                                 '--features', features, '--example', 'sharded_wal_transport_bench']
                run_recorded(floor_command, self.results / ('build-floor-' + label),
                             self.deadline, environment, cwd=source)
                floor_binary = binary_dir / 'sharded_wal_transport_bench'
                shutil.copy2(target / 'release/examples/sharded_wal_transport_bench', floor_binary)
                receipt.update(floor_binary_sha256=sha(floor_binary), floor_command=floor_command)
                for name, expected in original['source_sha256'].items():
                    if sha(source / name) != expected:
                        raise ValueError('floor build mutated frozen source: ' + name)
            write_json(self.results / ('cloud-build-' + label + '.json'), receipt)
            if 'experimental-fabric' in features:
                run_recorded(['ldd', str(binary)], self.results / ('linked-libraries-' + label), self.deadline)
                receipt['libfabric_version'] = subprocess.check_output(['pkg-config', '--modversion', 'libfabric'], text=True).strip()
                receipt['libfabric_sha256'] = sha(Path('/opt/amazon/efa/lib/libfabric.so.1').resolve())
                write_json(self.results / ('cloud-build-' + label + '.json'), receipt)
            self.cloud_builds[label] = receipt
        self.verify_cloud_pair()
        for label in ['baseline', 'current']:
            binary = self.root / 'cloud-bin' / label / 'sharded_wal_bench'
            artifact = upload(self.s3, binary, self.cfg, f"{self.cfg['s3_prefix']}/cloud-bin/{label}/sharded_wal_bench")
            self.install_cloud_binary(label, artifact)
            self.progress('installed-' + label)

    def verify_cloud_pair(self):
        """Reject stale/collapsed build outputs before distributing any binary."""
        baseline, current = (self.cloud_builds[label] for label in ['baseline', 'current'])
        if baseline['source_tree_sha256'] == current['source_tree_sha256']:
            raise ValueError('comparison requires distinct archived source trees')
        if baseline['binary_sha256'] == current['binary_sha256']:
            raise ValueError('distinct sources produced identical cloud binaries; refusing comparison')
        for label in ['baseline', 'current']:
            binary = self.root / 'cloud-bin' / label / 'sharded_wal_bench'
            if sha(binary) != self.cloud_builds[label]['binary_sha256']:
                raise ValueError('cloud binary changed after build receipt')
            output = self.results / ('capability-' + label)
            run_recorded([str(binary), '--help'], output, self.deadline,
                         expected_binary_sha256=self.cloud_builds[label]['binary_sha256']
                         if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') else None)
            help_text = output.with_suffix('.stdout.log').read_text()
            if '--remote-config' not in help_text or (label == 'current' and any(flag not in help_text for flag in ['--prepare-external-case', '--tcp-preparation-chunk-batches'])):
                raise ValueError(f'{label} cloud binary lacks required benchmark capabilities')
            if self.cfg.get('campaign') in ['read-path', 'efa', 'efa-cache'] and label == 'current' and any(
                    flag not in help_text for flag in ['--cold-read-pass', '--transport-diagnostics']):
                raise ValueError('current cloud binary lacks cold-read/diagnostic capabilities')
            if self.cfg.get('campaign') == 'efa-cache' and label == 'current' and any(
                    flag not in help_text for flag in ['--native-read-cache-bytes', '--hot-read-pass',
                                                       '--read-window', '--tcp-read-lanes']):
                raise ValueError('current cloud binary lacks cache/concurrent-read controls')
            if self.cfg.get('campaign') in ('efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                required = ['--transport', '--prepare-external-case', '--remote-config']
                if label == 'current' or self.cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                    required += ['--native-read-workers', '--read-resource-evidence', '--read-window',
                                 '--native-read-cache-bytes', '--tcp-read-lanes', '--tcp-server-read-workers',
                                 '--tcp-wal-member-read', '--cold-read-pass', '--hot-read-pass']
                if self.cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') and label == 'current':
                    required += ['--tcp-shard-cache-bytes']
                if self.cfg.get('campaign') in ('cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') and label == 'current':
                    required += ['--read-memory-allowance-bytes', '--shard-read-memory-allowance-bytes',
                                 '--cache-probe-keys', '--concurrent-mixed']
                if self.cfg.get('campaign') == 'stage-timing' or (self.cfg.get('campaign') == 'transport-floor' and label == 'current'):
                    required += ['--tcp-polling', '--tcp-resident-first']
                if any(flag not in help_text for flag in required):
                    raise ValueError(f'{label} lacks EFA progress capabilities')
        floor_labels = ('baseline', 'current') if self.cfg.get('campaign') == 'stage-timing' else ('current',) if self.cfg.get('campaign') == 'transport-floor' else ()
        for label in floor_labels:
            output = self.results / ('capability-floor-' + label)
            run_recorded([str(self.root / 'cloud-bin' / label / 'sharded_wal_transport_bench'), '--help'],
                         output, self.deadline,
                         expected_binary_sha256=self.cloud_builds[label]['floor_binary_sha256'])
            help_text = output.with_suffix('.stdout.log').read_text()
            if any(flag not in help_text for flag in [
                    '--output', '--remote-config', '--transport', '--polling', '--values',
                    '--samples', '--repetitions', '--idle-us', '--profile']):
                raise ValueError('floor binary lacks external transport capabilities')
        case_root = self.root / 'build-preflight' / 'external-case'
        case_root.parent.mkdir()
        endpoints = ','.join(n['private_ip'] + ':7700' for n in sorted(self.cfg['nodes'], key=lambda n: n['partition']))
        command = [str(self.root / 'cloud-bin/current/sharded_wal_bench'),
                   '--prepare-external-case', str(case_root), '--guest-case-root', str(case_root),
                   '--remote-endpoints', endpoints, '--values', '1024', '--batch-sizes', '1',
                   '--windows', '1', '--batches', '128', '--mixed', '32', '--reads', '512',
                   '--repetitions', '1']
        run_recorded(command, self.results / 'cloud-build-preparation-check', self.deadline,
                     expected_binary_sha256=self.cloud_builds['current']['binary_sha256']
                     if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') else None)
        files = ['remote.yaml'] + [f'node-{p}/launch.yaml' for p in range(5)]
        if self.cfg.get('campaign') in ('transport-floor', 'stage-timing'):
            files.append('shape.yaml')
        config_hashes = {name: sha(case_root / name) for name in files}
        write_json(self.results / 'cloud-build-pair-verified.json', {
            'binary_sha256': {label: self.cloud_builds[label]['binary_sha256'] for label in ['baseline', 'current']},
            'distinct_sources': True, 'distinct_binaries': True,
            'capability_checks': True, 'prepared_configuration_sha256': config_hashes,
            'prepare_command': command})

    def install_cloud_binary(self, label, artifact):
        # A one-shot bounded SSM install writes a new binary path; node services are
        # not running yet. Transfer uses each guest's instance profile.
        data = {'bucket': self.cfg['bucket'], 'account': self.cfg['account'],
                'region': self.cfg['region'], 'key': artifact['key'], 'sha256': artifact['sha256'],
                'target': str(self.root / 'cloud-bin' / label / 'sharded_wal_bench')}
        code = ('import boto3,hashlib,pathlib;d=' + repr(data) + ';'
                'p=pathlib.Path(d["target"]);p.parent.mkdir(parents=True,exist_ok=True);'
                's=boto3.client("s3",region_name=d["region"]);'
                's.download_file(d["bucket"],d["key"],str(p),ExtraArgs={"ExpectedBucketOwner":d["account"]});'
                'assert hashlib.sha256(p.read_bytes()).hexdigest()==d["sha256"];p.chmod(0o755)')
        command = shlex.join(['python3', '-c', code])
        reply = self.ssm.send_command(InstanceIds=[n['instance_id'] for n in self.cfg['nodes']],
                  DocumentName='AWS-RunShellScript', Parameters={'commands': [command], 'executionTimeout': ['120']},
                  TimeoutSeconds=60, Comment=f"{self.cfg['run_id']} install {label}")
        command_id = reply['Command']['CommandId']
        end = time.monotonic() + min(150, remaining(self.deadline))
        pending = {n['instance_id'] for n in self.cfg['nodes']}
        outcomes = {}
        while pending:
            if time.monotonic() >= end:
                raise TimeoutError('cloud binary installation outcome unknown')
            for instance in list(pending):
                try:
                    value = self.ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
                except self.ssm.exceptions.InvocationDoesNotExist:
                    continue
                if value['Status'] not in ['Pending', 'InProgress', 'Delayed']:
                    outcomes[instance] = value
                    pending.remove(instance)
            if pending:
                time.sleep(1)
        write_json(self.results / ('install-' + label + '.json'), outcomes)
        if any(x['Status'] != 'Success' for x in outcomes.values()):
            raise ValueError('cloud binary installation failed')

    def run_case(self, label, shape, rep, storage=False, capacity=False, chunk_control=False,
                 cold=False, diagnostics=False, transport='tcp', cached=False, progress_case=None,
                 resource_evidence=False, authority_resident_batches=None,
                 shard_cache_bytes=0, transport_floor_probe=False,
                 preparation_chunk_batches=16, workload_batches=None, workload_reads=None,
                 wal_layout=None, cache_hit_contention=False):
        if ((shard_cache_bytes or transport_floor_probe or preparation_chunk_batches != 16
                or workload_batches is not None or workload_reads is not None or wal_layout is not None
                or cache_hit_contention) and not self.cfg.get('kv_subsuite')
                or type(shard_cache_bytes) is not int or not 0 <= shard_cache_bytes <= 128 * 1024**2
                or type(transport_floor_probe) is not bool
                or type(cache_hit_contention) is not bool
                or (cache_hit_contention and (not cached or cold or diagnostics or shape[1] != 1))
                or type(preparation_chunk_batches) is not int or not 1 <= preparation_chunk_batches <= 32
                or any(value is not None and (type(value) is not int or not 1 <= value <= bound)
                       for value, bound in ((workload_batches, 4096), (workload_reads, 8192)))
                or wal_layout not in (None, 'small', 'sui')):
            raise ValueError('bounded cache, RAM-floor and workload controls require a KV subsuite')
        checkpoint = self.cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')
        ram = self.cfg.get('campaign') in ('cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')
        profile = (stage_timing if self.cfg.get('campaign') == 'stage-timing' else transport_floor if self.cfg.get('campaign') == 'transport-floor' else scalar_transport if self.cfg.get('campaign') == 'scalar-transport' else
        cluster_ram if ram else checkpoint_async if checkpoint else efa_progress)
        progress = self.cfg.get('campaign') in ('efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing')
        if progress_case is not None:
            planned = profile.plan(self.cfg.get('checkpoint_scope', 'full')) if checkpoint else profile.plan()
            if not progress or progress_case not in planned:
                raise ValueError('unplanned EFA progress case')
            spec = progress_case
            if (label, shape, rep) != (spec['build'], (spec['value'], spec['batch'], spec['window']), spec['rep']):
                raise ValueError('progress case identity differs from workload')
            cold, diagnostics, transport = spec['cold'], spec['diagnostics'], spec['transport']
            if not spec['qualification'] and not getattr(self, 'qualification_passed', False):
                raise ValueError('broader campaign requires full hardware qualification')
        elif progress:
            raise ValueError('progress requires an explicit planned case')
        if transport not in TRANSPORTS or (transport in HARDWARE_TRANSPORTS and
                ((label != 'current' and not progress) or not self.cfg.get('efa'))):
            raise ValueError('fabric cases require an EFA-enabled build and fleet')
        value, batch, window = shape
        if (cold or diagnostics) and ((label != 'current' and not checkpoint) or capacity or chunk_control):
            raise ValueError('read diagnostics require current source without layout controls')
        if cached and (self.cfg.get('campaign') != 'efa-cache' or label != 'current'
                       or not storage or cold or diagnostics or capacity or chunk_control):
            raise ValueError('cached cases require current warm storage without layout controls')
        if capacity and (storage or label != 'current' or shape != (8192, 32, 32)):
            raise ValueError('capacity supplement requires current TCP 8KiB/B32/W32')
        if chunk_control and (label != 'current' or not storage or capacity):
            raise ValueError('chunk control requires current storage shape')
        case_id, prefix = benchmark_case_identity(label, shape, rep,
            storage=storage, capacity=capacity, chunk_control=chunk_control, cold=cold,
            diagnostics=diagnostics, transport=transport, cached=cached,
            authority_resident_batches=authority_resident_batches, shard_cache_bytes=shard_cache_bytes,
            preparation_chunk_batches=preparation_chunk_batches, wal_layout=wal_layout)
        if progress:
            case_id, prefix = spec['case_id'], spec['family']
        self.progress('preparing-' + case_id)
        case_root = self.root / 'cases' / case_id
        case_root.parent.mkdir(exist_ok=True)
        kv_commands = None
        if self.cfg.get('kv_subsuite'):
            if (self.cfg.get('campaign') != 'efa-cache' or label != 'current' or not storage
                    or not resource_evidence or capacity or chunk_control or progress
                    or self.root != Path(self.cfg['run_root'])
                    or self.results != self.root / 'checkpoint-results/kv'):
                raise ValueError('KV execution differs from its fixed admitted runner')
            kv_spec = dict(shape=shape, repetition=rep, cold=cold, diagnostics=diagnostics,
                transport=transport, cached=cached, authority_resident_batches=authority_resident_batches,
                shard_cache_bytes=shard_cache_bytes, preparation_chunk_batches=preparation_chunk_batches,
                workload_batches=workload_batches, workload_reads=workload_reads, wal_layout=wal_layout,
                cache_hit_contention=cache_hit_contention)
            prepare, kv_commands = kv_case_commands(self.cfg, kv_spec)
        else:
            batches, mixed = (320, 64) if storage else (128, 32)
            reads = '4096' if self.cfg.get('campaign') in ['read-path', 'efa', 'efa-cache'] else '512'
            if authority_resident_batches is not None:
                batches = 512
            if workload_batches is not None:
                batches = workload_batches
            if workload_reads is not None:
                reads = str(workload_reads)
            cache_bytes = CACHE_BYTES if cached else 0
            if progress:
                batches, mixed, reads, cache_bytes = spec['batches'], spec['mixed'], str(spec['reads']), spec['cache']
            shape_args = ['--values', str(value), '--batch-sizes', str(batch), '--windows', str(window),
                          '--batches', str(batches), '--mixed', str(mixed), '--reads', reads, '--repetitions', '1']
            if wal_layout is not None:
                shape_args += ['--wal-layout', wal_layout]
            generator = self.root / 'cloud-bin' / (label if progress else 'current') / 'sharded_wal_bench'
            endpoints = ','.join(n['private_ip'] + ':7700' for n in sorted(self.cfg['nodes'], key=lambda n: n['partition']))
            prepare = [str(generator), '--prepare-external-case', str(case_root), '--guest-case-root', str(case_root),
                       '--remote-endpoints', endpoints, '--tcp-read-cache-bytes', str(cache_bytes)]
            if label == 'current' or progress:
                prepare += ['--transport', transport]
            if progress and (label == 'current' or checkpoint):
                prepare += ['--tcp-read-lanes', str(spec['lanes']),
                            '--tcp-server-read-workers', str(spec['workers']),
                            '--tcp-wal-member-read', spec['member']]
            if checkpoint and label == 'current':
                prepare += ['--tcp-shard-cache-bytes', str(spec['shard_cache'])]
            if ram:
                prepare = [str(generator), '--prepare-external-case', str(case_root),
                           '--guest-case-root', str(case_root), '--remote-endpoints', endpoints]
                prepare += profile.flags(spec, 'tcp', prepare=True)
            else:
                prepare += shape_args
        run_recorded(prepare, self.results / (case_id + '-prepare'), self.deadline,
                     expected_binary_sha256=self.cloud_builds[label]['binary_sha256']
                     if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') or self.cfg.get('kv_subsuite') else None)
        if capacity:
            write_json(self.results / (case_id + '-capacity.json'), prepare_capacity_4m(case_root))
        if authority_resident_batches is not None:
            write_json(self.results / (case_id + '-authority-window.json'),
                       prepare_authority_window(case_root, authority_resident_batches))
        shutil.copy2(case_root / 'remote.yaml', self.results / (case_id + '-remote.yaml'))
        if self.cfg.get('campaign') == 'stage-timing' or (self.cfg.get('campaign') == 'transport-floor' and label == 'current'):
            shutil.copy2(case_root / 'shape.yaml', self.results / (case_id + '-shape.yaml'))
        node_archives = {}
        for node in self.cfg['nodes']:
            part = node['partition']
            archive = self.root / (case_id + f'-node-{part}.tar.gz')
            with tarfile.open(archive, 'w:gz') as stream:
                names = [f'node-{part}/launch.yaml']
                if transport != 'efa-native':
                    names += ['tls/ca.der', f'tls/tidehunter-node-{part}.der', f'tls/tidehunter-node-{part}.pk8']
                for name in names:
                    stream.add(case_root / name, arcname=name, recursive=False)
            node_archives[str(part)] = upload(self.s3, archive, self.cfg,
                        f"{self.cfg['s3_prefix']}/case-artifacts/{case_id}/node-{part}.tar.gz")
        payload = {'config': self.cfg, 'case_id': case_id, 'action': 'start', 'build': label,
                   'binary_sha256': self.cloud_builds[label]['binary_sha256'], 'node_archives': node_archives}
        payload['transport'] = transport
        payload['require_efa_proof'] = bool(self.cfg.get('kv_subsuite') and transport in HARDWARE_TRANSPORTS)
        self.active_payload = payload
        self.ssm_nodes(payload)
        try:
            if checkpoint and spec.get('recovery'):
                self.run_recovery_actions(spec, case_root, payload)
                return
            # Rotate native/TCP ordering across rounds; both use this identical
            # controller hardware and its own NVMe. Storage services are idle
            # during native phases and separate roots prevent data interference.
            modes = ['tcp'] if (capacity or chunk_control) else (['native-sync', 'tcp'] if rep % 2 == 0 else ['tcp', 'native-sync'])
            if checkpoint:
                modes = spec['modes'] if rep % 2 == 0 else list(reversed(spec['modes']))
            for mode in modes:
                if remaining(self.deadline) < 90:
                    raise TimeoutError('API admission reserve exhausted')
                output = self.results / (case_id + '-' + mode)
                if kv_commands is not None:
                    command = kv_commands[mode]
                else:
                    command = [str(self.root / 'cloud-bin' / label / 'sharded_wal_bench'),
                               '--output', str(output), '--backend', mode] + shape_args
                    if resource_evidence:
                        command += ['--read-resource-evidence', 'true']
                    if cache_hit_contention:
                        command += ['--cache-hit-contention', 'true']
                    if cold:
                        command += ['--cold-read-pass', 'true']
                    if diagnostics:
                        command += ['--transport-diagnostics', 'true']
                    if self.cfg.get('campaign') == 'efa-cache' and label == 'current':
                        command += ['--read-window', '32']
                        if mode == 'native-sync':
                            command += ['--native-read-cache-bytes', str(cache_bytes)]
                        if not cold:
                            command += ['--hot-read-pass', 'true']
                    if progress:
                        write_json(self.results / (case_id + '-plan.json'), spec)
                        write_json(self.results / (case_id + '-' + mode + '-before.json'),
                                   self.controller_evidence())
                        if label == 'current' or checkpoint:
                            command += ['--read-window', str(spec['read_window']),
                                        '--read-resource-evidence', 'true']
                            if mode == 'native-sync':
                                command += ['--native-read-cache-bytes', str(cache_bytes),
                                            '--native-read-workers', str(spec['native_workers'])]
                            if not cold:
                                command += ['--hot-read-pass', 'true']
                    if mode == 'tcp':
                        command += ['--remote-config', str(case_root / 'remote.yaml'),
                                    '--tcp-read-cache-bytes', str(cache_bytes)]
                        if label == 'current' or progress:
                            command += ['--transport', transport]
                            command += ['--tcp-preparation-chunk-batches',
                                        '32' if chunk_control else '16']
                if ram and not spec.get('floor'):
                    command = [str(self.root / 'cloud-bin' / label / 'sharded_wal_bench'),
                               '--output', str(output), '--backend', mode]
                    command += profile.flags(spec, mode, external=mode == 'tcp')
                    if mode == 'tcp':
                        command += ['--remote-config', str(case_root / 'remote.yaml')]
                binary_hash = self.cloud_builds[label]['binary_sha256']
                if progress and spec.get('floor'):
                    command = [str(self.root / 'cloud-bin' / label / 'sharded_wal_transport_bench'),
                               '--output', str(output), '--remote-config', str(case_root / 'remote.yaml')]
                    command += profile.floor_flags(spec)
                    binary_hash = self.cloud_builds[label]['floor_binary_sha256']
                run_recorded(command, self.results / (case_id + '-' + mode + '-process'), self.deadline,
                             expected_binary_sha256=binary_hash
                             if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') or self.cfg.get('kv_subsuite') else None)
                if progress:
                    write_json(self.results / (case_id + '-' + mode + '-after.json'),
                               self.controller_evidence())
                if not (output / 'COMPLETE').is_file():
                    raise ValueError('benchmark omitted complete correctness/reopen receipt')
                self.state['completed_cases'].append({'case_id': case_id, 'backend': mode, 'build': label,
                                                      'variant': prefix, 'transport': transport if mode in ('tcp', 'floor') else 'local'})
                self.progress('completed-' + case_id + '-' + mode)
            if transport_floor_probe:
                for instrumented in (False, True):
                    suffix = '-transport-floor' + ('-profiled' if instrumented else '')
                    floor_output = self.results / (case_id + suffix)
                    floor_spec = transport_floor.subsuite_floor_spec(transport, instrumented)
                    floor_command = [str(self.root / 'cloud-bin' / label / 'sharded_wal_transport_bench'),
                        '--output', str(floor_output), '--remote-config', str(case_root / 'remote.yaml')]
                    floor_command += transport_floor.floor_flags(floor_spec)
                    run_recorded(floor_command, self.results / (case_id + suffix + '-process'),
                        self.deadline, expected_binary_sha256=self.state['floor_binary_sha256'])
                    if not (floor_output / 'COMPLETE').is_file():
                        raise ValueError('RAM transport floor omitted its completion receipt')
                    transport_floor.read_floor_metrics(floor_output, floor_spec)
                    self.state['completed_floor_cases'].append(dict(
                        case_id=case_id, population='diagnostic' if instrumented else 'clean',
                        output=floor_output.name, specification=floor_spec,
                        binary_sha256=self.state['floor_binary_sha256'],
                        files={path.name: sha(path) for path in sorted(floor_output.iterdir()) if path.is_file()},
                        process_record=case_id + suffix + '-process.command.json'))
                    self.progress('completed-' + case_id + suffix)
        finally:
            self.ssm_nodes(payload | {'action': 'stop'})
            self.collect_node_results(case_id)
            self.active_payload = None

    def collect_node_results(self, case_id):
        destination = self.results / 'nodes' / case_id
        destination.mkdir(parents=True, exist_ok=True)
        for node in self.cfg['nodes']:
            filename = f"node-{node['partition']}.tar.gz"
            key = f"{self.cfg['s3_prefix']}/results/nodes/{case_id}/{filename}"
            response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key + '.receipt.json',
                                          ExpectedBucketOwner=self.cfg['account'])
            with response['Body'] as body:
                encoded = body.read(64 * 1024 + 1)
            if len(encoded) > 64 * 1024:
                raise ValueError('node receipt exceeds bounded size')
            receipt = json.loads(encoded)
            if receipt['bucket'] != self.cfg['bucket'] or receipt['key'] != key or receipt['bytes'] > 16 * 1024 * 1024:
                raise ValueError('node result receipt has wrong scope or exceeds size budget')
            download(self.s3, self.cfg, key, receipt['sha256'], destination / filename)
            write_json(destination / (filename + '.receipt.json'), receipt)

    def run_recovery_actions(self, spec, case_root, payload):
        case = spec['case_id']
        name = case + '-recovery'
        output = self.results / name
        output.mkdir()
        write_json(self.results / (case + '-plan.json'), spec)
        write_json(self.results / (name + '-before.json'), self.controller_evidence())
        binary = self.root / 'cloud-bin' / spec['build'] / 'sharded_wal_recovery_bench'
        if sha(binary) != self.cloud_builds[spec['build']]['recovery_binary_sha256']:
            raise ValueError('recovery binary differs from overlay build receipt')
        restore_start = None
        for action in ('seed', 'reopen'):
            if remaining(self.deadline) < 60:
                raise TimeoutError('recovery admission deadline')
            if action == 'reopen':
                restore_start = time.monotonic_ns()
                if spec['restart'] == 'full':
                    self.ssm_nodes(payload | {'action': 'restart'})
                nodes_ready_ns = time.monotonic_ns() - restore_start
            command = [str(binary), '--action', action, '--remote-config', str(case_root / 'remote.yaml'),
                       '--output', str(output / action), '--value-bytes', '8192',
                       '--checkpoint-batches', '64', '--tail-batches', str(spec['tail_batches'])]
            run_recorded(command, self.results / (name + '-' + action + '-process'), self.deadline,
                         expected_binary_sha256=self.cloud_builds[spec['build']]['recovery_binary_sha256']
                         if self.cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing') else None)
            receipt = json.loads((output / action / 'recovery.json').read_text())
            profile = (stage_timing if self.cfg.get('campaign') == 'stage-timing' else transport_floor if self.cfg.get('campaign') == 'transport-floor' else scalar_transport if self.cfg.get('campaign') == 'scalar-transport' else
            cluster_ram if self.cfg.get('campaign') == 'cluster-ram' else checkpoint_async)
            profile.validate_recovery_json(receipt, spec, action)
            if not (output / action / 'COMPLETE').is_file():
                raise ValueError('missing recovery action completion receipt')
        write_json(output / 'service-restore.json', dict(
            scope='controller inclusive restart orchestration plus verified reopen process; not engine open latency',
            restart=spec['restart'], os_cache_evicted=False, nodes_ready_ns=nodes_ready_ns,
            controller_service_restore_ns=time.monotonic_ns() - restore_start))
        write_json(self.results / (name + '-after.json'), self.controller_evidence())
        (output / 'COMPLETE').write_text('seed exit and verified reopen completed\n')
        self.state['completed_cases'].append(dict(case_id=case, backend='recovery', build=spec['build'],
                                                  variant=spec['family'], transport=spec['transport']))
        self.progress('completed-' + name)

    def execute_progress(self):
        # The same offline validator is the admission authority and archive auditor.
        from analyze_cloud import validate_qualification
        profile = (stage_timing if self.cfg.get('campaign') == 'stage-timing' else transport_floor if self.cfg.get('campaign') == 'transport-floor' else scalar_transport if self.cfg.get('campaign') == 'scalar-transport' else
        cluster_ram if self.cfg.get('campaign') == 'cluster-ram' else
        checkpoint_async if self.cfg.get('campaign') == 'checkpoint-async' else efa_progress)
        self.qualification_passed = False
        receipt = {'schema': 1, 'passed': False, 'case_ids': [], 'errors': {}}
        write_json(self.results / 'qualification-receipt.json', receipt)
        try:
            for spec in profile.qualification_cases():
                if remaining(self.deadline) < 180:
                    raise TimeoutError('qualification admission reserve exhausted')
                self.run_case(spec['build'], (spec['value'], spec['batch'], spec['window']),
                              spec['rep'], storage=True, progress_case=spec)
            receipt = validate_qualification(self.results)
            write_json(self.results / 'qualification-receipt.json', receipt)
            if not receipt['passed']:
                raise ValueError('hardware qualification failed: ' + json.dumps(receipt['errors'], sort_keys=True))
            self.qualification_passed = True
            scope = self.cfg.get('checkpoint_scope', 'full')
            if profile in (cluster_ram, scalar_transport, transport_floor, stage_timing):
                cases = profile.main_cases(scope)
                if scope in ('full', 'recovery'):
                    cases += profile.recovery_cases()
            else:
                cases = [] if scope == 'recovery' else profile.main_cases()
                if profile is checkpoint_async:
                    cases += profile.recovery_cases()
            for spec in cases:
                if remaining(self.deadline) < 180:
                    raise TimeoutError('progress admission reserve exhausted')
                self.run_case(spec['build'], (spec['value'], spec['batch'], spec['window']),
                              spec['rep'], storage=True, progress_case=spec)
            if profile in (scalar_transport, transport_floor, stage_timing):
                expected = profile.expected_cases(scope)
                completed = [item['case_id'] + '-' + item['backend']
                             for item in self.state['completed_cases']]
                if len(completed) != len(expected) or set(completed) != set(expected):
                    raise ValueError('source-bound transport completion identities differ from exact plan')
                for spec in profile.plan(scope):
                    for mode in spec['modes']:
                        if remaining(self.deadline) <= 0:
                            raise TimeoutError('evidence validation reached fixed phase deadline')
                        profile.read_cloud_case(self.results, spec['case_id'] + '-' + mode, spec, mode)
        except BaseException as error:
            if not self.qualification_passed:
                receipt['errors']['execution'] = repr(error)
                write_json(self.results / 'qualification-receipt.json', receipt)
            raise

    def execute(self):
        try:
            self.check_owned_instances()
            self.probe_export()
            self.progress('verified-fleet-building')
            self.build_pair()
            if self.cfg.get('campaign') in ('efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
                self.execute_progress()
            elif self.cfg.get('campaign') in ['read-path', 'efa', 'efa-cache']:
                cases = (efa_cases(self.cfg.get('campaign') == 'efa-cache')
                         if self.cfg.get('efa') else read_path_cases())
                for label, shape, rep, options in cases:
                    if remaining(self.deadline) < 180:
                        raise TimeoutError('read-path admission reserve exhausted')
                    self.run_case(label, shape, rep, **options)
            else:
                for rep in range(self.cfg.get('repetitions', 2)):
                    for value in [1024, 8192]:
                        for batch in [1, 32]:
                            for window in [1, 8, 32]:
                                for label in (['baseline', 'current'] if rep % 2 == 0 else ['current', 'baseline']):
                                    if remaining(self.deadline) < 180:
                                        raise TimeoutError('benchmark admission reserve exhausted')
                                    self.run_case(label, (value, batch, window), rep)
                for rep in range(self.cfg.get('storage_repetitions', 2)):
                    for label in (['baseline', 'current'] if rep % 2 == 0 else ['current', 'baseline']):
                        if remaining(self.deadline) < 180:
                            raise TimeoutError('storage-case admission reserve exhausted')
                        self.run_case(label, (8192, 32, 32), rep, storage=True)
                for rep in range(self.cfg.get('capacity_repetitions', 2)):
                    if remaining(self.deadline) < 180:
                        raise TimeoutError('capacity-case admission reserve exhausted')
                    self.run_case('current', (8192, 32, 32), rep, capacity=True)
                for rep in range(self.cfg.get('chunk_control_repetitions', 0)):
                    if remaining(self.deadline) < 180:
                        raise TimeoutError('chunk-control admission reserve exhausted')
                    self.run_case('current', (8192, 32, 32), rep, storage=True, chunk_control=True)
            self.state['complete'] = True
        except BaseException as error:
            self.state['failures'].append(repr(error))
            if self.active_payload is not None:
                try:
                    self.ssm_nodes(self.active_payload | {'action': 'stop'})
                    self.collect_node_results(self.active_payload['case_id'])
                except BaseException as cleanup_error:
                    self.state['failures'].append('node cleanup: ' + repr(cleanup_error))
        finally:
            try:
                self.progress('exporting-complete' if self.state['complete'] else 'exporting-partial')
                write_json(self.results / 'after.json', evidence(self.root))
                write_json(self.results / 'campaign-receipt.json', self.state)
                if self.state['complete']:
                    (self.results / 'COMPLETE').write_text('all selected native/TCP cases and reopen audits passed\n')
                archive = self.root / 'results.tar.gz'
                with tarfile.open(archive, 'w:gz') as stream:
                    stream.add(self.results, arcname='results')
                receipt = upload_final(self.s3, archive, self.cfg, f"{self.cfg['s3_prefix']}/results/controller.tar.gz")
                receipt.update(complete=self.state['complete'], completed_cases=len(self.state['completed_cases']),
                               run_id=self.cfg['run_id'], cloud_builds=self.cloud_builds)
                write_json(self.root / 'result-receipt.json', receipt)
                upload_final(self.s3, self.root / 'result-receipt.json', self.cfg, f"{self.cfg['s3_prefix']}/results/receipt.json")
            finally:
                # Absolute guest shutdown timers remain armed even if S3 or the
                # final SDK request fails. The operator must verify termination
                # and root-volume deletion independently through AWS MCP.
                if self.cfg.get('terminate_on_finish', True):
                    # A full/broken evidence disk must not prevent owned teardown.
                    self.check_owned_instances(record=False)
                    self.ec2.terminate_instances(InstanceIds=self.ids)
        if not self.state['complete']:
            raise RuntimeError('campaign partial; uploaded evidence preserves failures')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    controller = modes.add_parser('controller')
    controller.add_argument('--config', type=Path, required=True)
    node = modes.add_parser('node')
    node.add_argument('--payload-base64', required=True)
    threads = modes.add_parser('threads')
    threads.add_argument('--pid', type=int, required=True)
    threads.add_argument('--output', type=Path, required=True)
    threads.add_argument('--deadline', required=True)
    args = parser.parse_args()
    if args.mode == 'controller':
        Campaign(json.loads(args.config.read_text())).execute()
    elif args.mode == 'threads':
        monitor_threads(args.pid, args.output, args.deadline)
    else:
        node_action(json.loads(base64.b64decode(args.payload_base64, validate=True)))


if __name__ == '__main__':
    main()
