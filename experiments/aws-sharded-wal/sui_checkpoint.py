#!/usr/bin/env python3
"""Deploy an already locally verified real Sui replay to an existing owned fleet.

The optional KV subsuite reuses the same locally built storage executable after
real Sui replay; no builds or provisioning occur here. Run ``schema`` for the
input contract, then ``controller --config FILE`` on the bootstrapped coordinator.
Install beside guest.py and its Python siblings; boto3, PyYAML, OpenSSL and libraries
must already be installed. All six bootstrap absolute poweroff timers must remain
armed and EC2 InstanceInitiatedShutdownBehavior must be terminate. The supervisor
always attempts exact-tag-checked six-host termination, including on export or
admission failure; the parent must independently verify termination through MCP.
"""
import argparse
import fcntl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import inspect
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import time

from guest import (Campaign, SDK_CONFIG, download, evidence, remaining, sha,
                   upload, upload_final, write_json)
import boto3
from botocore.config import Config
from checkpoint_matrix import (matrix_plan, check_cell, validate_provider, efa_snapshot,
                               efa_proof, validate_efa_proof, validate_matrix, validate_placement)
from checkpoint_kv import run_suite as run_kv_suite, validate_options as validate_kv_options

# SendCommand has no idempotency token. An ambiguous HTTP acknowledgement must
# never be replayed by botocore; exact-unit cleanup and TTL own that uncertainty.
SSM_CONFIG = SDK_CONFIG.merge(Config(retries={'total_max_attempts': 1, 'mode': 'standard'}))

BINARIES = {'sui-node-native', 'sui-node-remote', 'storage-node', 'state-audit', 'rocksdb-audit', 'transport-floor'}
HEX = r'[0-9a-f]{64}'
TIP = 1775
CHECKPOINT = 'EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6'


def schema():
    """Machine-readable launch contract, not an executable default plan."""
    return {
        'config': {
            'campaign': 'sui-checkpoint', 'run_id': 'th-0123456789abcdef',
            'run_root': '/mnt/tidehunter/th-0123456789abcdef',
            'account': '123456789012', 'region': 'us-east-1',
            'availability_zone': 'us-east-1a', 'placement_group': 'th-0123456789abcdef',
            'bucket': 'YOUR-OWNED-BUCKET', 's3_prefix': 'th-0123456789abcdef',
            'deadline_utc': 'ABSOLUTE-UTC-ISO8601', 'export_reserve_seconds': 600,
            'terminate_on_finish': True, 'instance_type': 'g6.8xlarge',
            'coordinator_instance_id': 'i-00000000000000000',
            'coordinator_private_ip': '10.0.0.10',
            'nodes': [{'partition': i, 'instance_id': f'i-{i+1:017x}',
                       'private_ip': f'10.0.0.{i+11}'} for i in range(5)],
            'transports': ['tcp', 'efa'], 'repetitions': 4, 'read_profile_limits': [0, 4096, 4096, 0],
            'write_profile_limits': [0, 128, 128, 0],
            'native_write_profile_limits': [0, 640, 640, 0],
            'port': 24000, 'coordinator_cache_mib': 8,
            'shard_cache_mib': 0, 'replay_deadline_seconds': 900,
            'kv_suite': None,
            'build_bundle': {'key': 'th-0123456789abcdef/inputs/build-bundle.tar.gz', 'sha256': 'SHA256'},
            'fixture_bundle': {'key': 'th-0123456789abcdef/inputs/fixture.tar.gz', 'sha256': 'SHA256'},
            'verification_bundle': {'key': 'th-0123456789abcdef/inputs/verification.tar.gz', 'sha256': 'SHA256'},
            'build_manifest_sha256': 'SHA256', 'source_archive_sha256': 'SHA256',
            'source_manifest_sha256': 'SHA256',
            'binaries': {name: 'SHA256' for name in sorted(BINARIES)},
            'checkpoint_digest': CHECKPOINT, 'checkpoint_limit': TIP,
        },
        'artifact_layouts': {
            'build_bundle': 'Unmodified build.py build-bundle.tar.gz: binaries/, sources.tar.gz, source-manifest.json, build-manifest.json, plan.json, portability.json and build logs.',
            'fixture_bundle': 'Unmodified fixture.py fixture.tar.gz: fixture/fixture-manifest.json, fixture/producer/**, fixture/{producer,native,distributed}.yaml, fixture/experiment/results.json.',
            'verification_bundle': 'verification/receipt.json, verification/evidence.tar.gz, verification/experiment/results.json, verification/experiment/audit.json, verification/experiment/native-schema/** copied from the successful same-build LOCAL run. Evidence archive byte-binds every supplied experiment file; no cloud result may substitute.',
        },
        'prerequisites': [
            'Use AWS MCP to supply six already-owned, bootstrapped hosts; never call this before local success.',
            'RunId/Project/ManagedBy tags, exact IDs/IPs/account/type, active absolute bootstrap TTL, terminate-on-OS-shutdown, one mounted local NVMe per host.',
            'Python >=3.12, boto3, PyYAML, openssl, ldd, systemd; install binary dependencies including required GLIBC/GLIBCXX/libfabric before invocation.',
            'Coordinator SDK permissions: exact six EC2 Describe/Terminate, shutdown-attribute reads and DescribePlacementGroups; scoped five-node SSM SendCommand/GetCommandInvocation; owned bucket/prefix Get/Put/Head.',
            'Storage SDK permissions: Get <prefix>/deploy/cells/*/storage-<partition>.tar.gz; Get/Put/Head <prefix>/results/cells/*/storage/<partition>/*. No other partition or foreign run access.',
            'Frozen source build must include this supervisor, cloud_node.py, and run.py durable external stage handshake.',
        ],
        'invocation': 'python3 /PATH/aws-sharded-wal/sui_checkpoint.py controller --config /mnt/tidehunter/<run_id>/sui-config.json',
        'outputs': '<prefix>/results/{controller.tar.gz,receipt.json,progress.json,cells/<cell_id>/storage/<partition>/{live,final}/...}; controller archive retains all cells, including partial failures.',
        'completion_admission': 'Each requested cell requires matching source/config/cell-bound final sampler resources and receipt, successful systemd owner exit, exact reaped namespace PID/starttime/commands and every stdout/stderr log. EFA stop/final receipts additionally require efa_proof; final archives include raw before/after counters. All final archives must match the runner resource handshake. Successful exact-unit release and absent old namespace processes are required before the next cell.',
        'matrix': 'transports is a distinct nonempty tcp/efa/efa-direct list; the default remains tcp/efa. repetitions is 1..4. Each cell has its own native control. Odd repetitions use list order, even repetitions reverse it. No requested cell may be omitted.',
        'profiling': 'read_profile_limits is required; write_profile_limits and native_write_profile_limits default to disabled. All are per-repetition integers 0..65536. [0,4096,4096,0] read, [0,128,128,0] remote write and [0,640,640,0] native write give clean/diagnostic/diagnostic/clean repetitions. Remote writes sample append RPCs per owning connection; native writes sample foreground durability barriers per DB. These are distinct populations.',
        'efa_proof': 'fi_info -p efa -f <transport> -t FI_EP_RDM must report exactly provider efa and requested fabric efa or efa-direct. Every hardware cell requires source/cell/host-bound before/after sysfs efa-driver send_bytes and recv_bytes, both positive on coordinator and all five storage hosts. No software, normal-EFA-for-Direct or TCP fallback; host-wide activity is not pure-wire timing.',
        'lifetime': 'deadline_utc must be timezone-aware UTC, <=1 hour away and <=every bootstrap deadline; measured work ends export_reserve_seconds earlier. No retries of unknown benchmark starts.',
    }


def safe_extract(archive, destination, *, symlinks=False, max_bytes=32 << 30):
    """Fresh destination; reject devices/hardlinks/alias writes, allow safe source links."""
    destination = Path(destination)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    with tarfile.open(archive) as stream:
        members = stream.getmembers()
        if sum(m.size for m in members) > max_bytes or len(members) > 500000:
            raise ValueError('archive expansion exceeds bounded budget')
        names, links = set(), set()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or '..' in path.parts or not path.parts or str(path) in names:
                raise ValueError('unsafe/duplicate archive path')
            names.add(str(path))
            if member.issym() and symlinks:
                target = PurePosixPath(member.linkname)
                if target.is_absolute():
                    raise ValueError('absolute source symlink')
                resolved = (destination / str(path.parent) / str(target)).resolve()
                if not resolved.is_relative_to(destination.resolve()):
                    raise ValueError('source symlink escapes archive')
                links.add(path)
            elif not (member.isfile() or member.isdir()):
                raise ValueError('archive contains forbidden link/device')
        for member in members:
            if any(parent in links for parent in PurePosixPath(member.name).parents):
                raise ValueError('archive writes through a symlink')
        stream.extractall(destination, members=members, filter='data')
    for path in destination.rglob('*'):
        if path.is_symlink() and not path.resolve().is_relative_to(destination.resolve()):
            raise ValueError('resolved source symlink escapes archive')


def read_json(path):
    return json.loads(Path(path).read_text())


def scoped_path(root, path):
    path = Path(path)
    if not path.is_absolute() or not path.resolve().is_relative_to(root.resolve()) or '..' in path.parts:
        raise ValueError('path outside exact owned root')
    return path


def validate_cfg(cfg):
    if cfg.get('campaign') != 'sui-checkpoint' or cfg.get('terminate_on_finish') is not True:
        raise ValueError('dedicated real-checkpoint campaign requires unconditional teardown')
    if not re.fullmatch(r'th-[0-9a-f]{16}', cfg['run_id']) or not re.fullmatch(r'[0-9]{12}', cfg['account']):
        raise ValueError('invalid run/account identity')
    root = Path('/mnt/tidehunter') / cfg['run_id']
    if str(root) != cfg['run_root'] or root.resolve() != root or cfg['s3_prefix'] != cfg['run_id']:
        raise ValueError('owned root/prefix must exactly match run ID without symlinks')
    if (cfg.get('placement_group') != cfg['run_id']
            or not re.fullmatch(re.escape(cfg['region']) + r'[a-z]', cfg.get('availability_zone', ''))):
        raise ValueError('exact run-owned cluster group and regional availability zone required')
    nodes = cfg['nodes']
    if len(nodes) != 5 or sorted(n['partition'] for n in nodes) != list(range(5)):
        raise ValueError('exactly five distinct partitions required')
    ids = [cfg['coordinator_instance_id'], *(n['instance_id'] for n in nodes)]
    ips = [cfg['coordinator_private_ip'], *(n['private_ip'] for n in nodes)]
    if len(set(ids)) != 6 or any(not re.fullmatch(r'i-[0-9a-f]{8,17}', value) for value in ids):
        raise ValueError('six distinct exact instance IDs required')
    if len(set(ips)) != 6 or any(not ipaddress.IPv4Address(value).is_private for value in ips):
        raise ValueError('six distinct private IPv4 addresses required')
    deadline = datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00'))
    if deadline.utcoffset() is None or deadline.utcoffset().total_seconds() != 0:
        raise ValueError('deadline must be absolute UTC')
    reserve = cfg.get('export_reserve_seconds', 600)
    if not 300 <= reserve <= 900 or not reserve + 180 < remaining(cfg['deadline_utc']) <= 3600:
        raise ValueError('deadline must preserve measured work and bounded export reserve')
    matrix_plan(cfg)
    validate_kv_options(cfg.get('kv_suite'))
    if not 1024 <= cfg.get('port', 24000) <= 64920:
        raise ValueError('port range cannot accommodate all namespaces')
    if any(not 0 <= cfg.get(name, default) <= 128 for name, default in [('coordinator_cache_mib', 8), ('shard_cache_mib', 0)]):
        raise ValueError('invalid per-database cache bound')
    if not 30 <= cfg.get('replay_deadline_seconds', 900) <= 3600:
        raise ValueError('invalid replay deadline')
    if cfg.get('checkpoint_limit') != TIP or cfg.get('checkpoint_digest') != CHECKPOINT:
        raise ValueError('campaign requires the frozen real genesis-through-1775 chain')
    if set(cfg.get('binaries', {})) != BINARIES:
        raise ValueError('exact six executable hashes required')
    for value in [cfg[name] for name in ('build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256')] + list(cfg['binaries'].values()):
        if not re.fullmatch(HEX, value):
            raise ValueError('invalid immutable digest')
    for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
        item = cfg[name]
        key = PurePosixPath(item['key'])
        if not item['key'].startswith(cfg['run_id'] + '/inputs/') or '..' in key.parts or not re.fullmatch(HEX, item['sha256']):
            raise ValueError('input object outside exact run prefix or missing SHA-256')
    return root, ids


def bootstrap_gate(root, cfg, instance_id):
    """Never format/mount here, and never extend the original absolute TTL."""
    record = read_json(root / 'bootstrap.json')
    identity = record['identity']
    if (record['run_id'] != cfg['run_id'] or identity['instanceId'] != instance_id
            or identity['accountId'] != cfg['account'] or identity['instanceType'] != cfg['instance_type']
            or identity['region'] != cfg['region']):
        raise ValueError('actual bootstrapped guest differs from exact inventory')
    if (datetime.fromisoformat(record['deadline_utc'].replace('Z', '+00:00'))
            < datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00'))):
        raise ValueError('campaign deadline exceeds original bootstrap TTL')
    unit = 'sharded-wal-expire-' + cfg['run_id']
    subprocess.run(['systemctl', 'is-active', '--quiet', unit + '.timer'], check=True, timeout=10)
    timer = Path('/etc/systemd/system') / (unit + '.timer')
    service = timer.with_suffix('.service')
    expected = datetime.fromisoformat(record['deadline_utc'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S UTC')
    if 'OnCalendar=' + expected not in timer.read_text() or 'ExecStart=/usr/sbin/poweroff' not in service.read_text():
        raise ValueError('absolute expiry unit changed')
    mount = json.loads(subprocess.check_output(['findmnt', '--json', '--target', str(root), '--output', 'SOURCE,TARGET'], timeout=10))['filesystems']
    if len(mount) != 1 or mount[0]['target'] != '/mnt/tidehunter' or mount[0]['source'] != record['device']:
        raise ValueError('owned root must use the one bootstrapped local NVMe filesystem')
    devices = json.loads(subprocess.check_output(['lsblk', '--json', '--output', 'PATH,MODEL,TYPE'], timeout=10))['blockdevices']
    selected = [d for d in devices if d['path'] == record['device']]
    if len(selected) != 1 or selected[0]['type'] != 'disk' or selected[0]['model'].strip() != 'Amazon EC2 NVMe Instance Storage':
        raise ValueError('benchmark root is not one local instance-store NVMe')
    return record


def preflight(binaries, output):
    output.mkdir(exist_ok=False)
    for binary in binaries:
        result = subprocess.run(['ldd', str(binary)], capture_output=True, text=True, timeout=30)
        text = result.stdout + result.stderr
        (output / (binary.name + '.ldd.txt')).write_text(text)
        if result.returncode or 'not found' in text:
            raise RuntimeError(f'dynamic dependency preflight failed: {binary.name}; install matching runtime before replay')
        if not os.access(binary, os.X_OK):
            raise ValueError('binary is not executable: ' + str(binary))
    (output / 'platform.json').write_text(json.dumps(dict(uname=list(os.uname()), os_release=Path('/etc/os-release').read_text())))


def fabric_preflight(output, transport='efa'):
    import resource
    if transport not in ('efa', 'efa-direct'):
        raise ValueError('hardware fabric preflight requires an EFA transport')
    resource.setrlimit(resource.RLIMIT_MEMLOCK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
    os.environ.update(FI_EFA_USE_DEVICE_RDMA='1', FI_EFA_ENABLE_SHM_TRANSFER='0')
    executable = shutil.which('fi_info') or '/opt/amazon/efa/bin/fi_info'
    result = subprocess.run([executable, '-p', 'efa', '-f', transport, '-t', 'FI_EP_RDM'],
                            capture_output=True, timeout=30)
    (output / 'efa-provider.txt').write_bytes(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f'{transport} requires an available hardware EFA RDM fabric; no transport fallback')
    validate_provider(result.stdout.decode(), transport)


def systemd_status(unit):
    properties = 'Id,LoadState,ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,MainPID,ExecMainPID'
    result = subprocess.run(['systemctl', 'show', unit, '--property=' + properties],
                            capture_output=True, text=True, timeout=10)
    status = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    if result.returncode:
        status['query_error'] = result.stderr
    return status


def completion_files(transport='tcp'):
    files = {'resources': 'resources.json', 'node_receipt': 'cloud-node-receipt.json',
             'node_config': 'node-config.json', 'systemd_status': 'systemd-status.json'}
    if transport in ('efa', 'efa-direct'):
        files['efa_proof'] = 'efa-proof.json'
    return files


def required_node_files(expected):
    names = {'bootstrap.json', 'supervisor-node.json', 'cloud-node-supervisor.log',
             'journal.log', 'preflight/platform.json', 'preflight/storage-node.ldd.txt'}
    names.update(completion_files(expected['cloud']['transport']).values())
    if expected['campaign']['transport'] in ('efa', 'efa-direct'):
        names.update(('preflight/efa-provider.txt', 'efa-before.json', 'efa-after.json'))
    for index in range(len(expected['cloud']['commands'])):
        names.update(f'cloud-node-logs/namespace-{index:02d}.{kind}.log' for kind in ('stdout', 'stderr'))
    return names


def validate_node_completion(cloud, config_digest, resources, receipt, status, expected):
    """A sampler JSON alone is not proof that its final publication succeeded."""
    if cloud != expected['cloud'] or config_digest != resources.get('node_config_sha256'):
        raise ValueError('archived node-config/source binding differs')
    required_receipt = {'schema', 'run_id', 'root', 'deadline_utc', 'storage_binary_sha256',
        'build_manifest_sha256', 'config_sha256', 'checkpoint_digest', 'node_config_sha256', 'cell_id', 'transport',
        'owner_pid', 'started_unix_ns', 'finished_unix_ns', 'started_unix', 'finished_unix',
        'ready', 'stop_reason', 'failure', 'complete', 'cleanup_complete',
        'expected_namespace_count', 'namespaces'}
    if not required_receipt.issubset(receipt):
        raise ValueError('node receipt identity/completion fields are incomplete')
    for key in ('run_id', 'root', 'deadline_utc', 'storage_binary_sha256',
                'build_manifest_sha256', 'config_sha256', 'checkpoint_digest', 'cell_id', 'transport'):
        if resources.get(key) != cloud[key]:
            raise ValueError('node completion identity differs: ' + key)
    if (receipt.get('complete') is not True or receipt.get('cleanup_complete') is not True
            or receipt.get('ready') is not True or receipt.get('failure') is not None
            or receipt.get('stop_reason') != 'controller_stop'):
        raise ValueError('node receipt does not prove successful final cleanup/publication')
    for key, value in receipt.items():
        if key not in resources or resources[key] != value:
            raise ValueError('node receipt and final resources disagree: ' + key)
    if (resources.get('all_namespaces_healthy') is not True
            or type(receipt.get('finished_unix_ns')) is not int
            or receipt['finished_unix_ns'] <= receipt.get('started_unix_ns', 0)
            or type(receipt.get('owner_pid')) is not int or receipt['owner_pid'] <= 0):
        raise ValueError('node completion timestamps/owner/health are incomplete')
    required_status = dict(Id='sui-storage-' + cloud['run_id'] + '-' + cloud['cell_id'] + '.service', LoadState='loaded',
        ActiveState='active', SubState='exited', Result='success', ExecMainCode='1',
        ExecMainStatus='0', MainPID='0', ExecMainPID=str(receipt['owner_pid']))
    if status != required_status:
        raise ValueError('systemd did not retain a successful exit of this sampler owner')
    namespaces = receipt.get('namespaces', [])
    if (len(namespaces) != len(cloud['commands'])
            or receipt.get('expected_namespace_count') != len(namespaces)):
        raise ValueError('node receipt misses namespace coverage')
    identities = {}
    for index, record in enumerate(namespaces):
        if (record.get('index') != index or record.get('command') != cloud['commands'][index]
                or record.get('ready') != cloud['ready_files'][index]
                or record.get('stop') != cloud['stop_files'][index]
                or record.get('reaped') is not True or record.get('exit_code') not in (0, -signal.SIGTERM)
                or type(record.get('pid')) is not int or record['pid'] <= 0
                or type(record.get('starttime_ticks')) is not int or record['starttime_ticks'] <= 0
                or record.get('process_group') != record['pid']):
            raise ValueError('node namespace command/process/cleanup identity differs')
        for kind in ('stdout', 'stderr'):
            wanted = str(Path(cloud['root']) / f'cloud-node-logs/namespace-{index:02d}.{kind}.log')
            if record.get(kind) != wanted:
                raise ValueError('namespace log identity differs')
        identities[index] = (record['pid'], record['starttime_ticks'])
    if len(set(identities.values())) != len(identities):
        raise ValueError('duplicate namespace process identity')
    for sample in resources.get('samples', []):
        for process in sample.get('processes', []):
            if identities.get(process.get('index')) != (process.get('pid'), process.get('starttime_ticks')):
                raise ValueError('sample PID/starttime differs from completed namespace')


def validate_node_archive(archive, expected, resources):
    """Validate exact public evidence inventory without extracting node archives."""
    required = required_node_files(expected)
    with tarfile.open(archive) as stream:
        files, names = {}, set()
        for member in stream:
            path = PurePosixPath(member.name)
            name = str(path)
            if (path.is_absolute() or '..' in path.parts or not path.parts
                    or member.name.rstrip('/') != name or name in names):
                raise ValueError('unsafe/duplicate node evidence archive path')
            names.add(name)
            if member.isdir() and name in ('preflight', 'cloud-node-logs'):
                continue
            if not member.isfile() or name not in required:
                raise ValueError('node evidence contains a link or unrelated/private file')
            files[name] = member
        if set(files) != required:
            raise ValueError('node evidence is missing mandatory logs/config/receipt files')
        def blob(name, limit=1024 * 1024):
            if files[name].size > limit:
                raise ValueError('node evidence JSON/preflight exceeds limit')
            with stream.extractfile(files[name]) as source:
                return source.read(limit + 1)
        def document(name, limit=1024 * 1024):
            return json.loads(blob(name, limit))
        if document('resources.json', 32 * 1024 * 1024) != resources:
            raise ValueError('archived resources differ from validated resource snapshot')
        if document('supervisor-node.json') != expected:
            raise ValueError('node supervisor deployment identity differs')
        bootstrap = document('bootstrap.json')
        identity = bootstrap['identity']
        if (bootstrap['run_id'] != expected['campaign']['run_id']
                or identity['instanceId'] != expected['instance_id']
                or any(identity[field] != expected['campaign'][key] for field, key in
                       [('accountId', 'account'), ('instanceType', 'instance_type'), ('region', 'region')])):
            raise ValueError('archived bootstrap belongs to another host')
        if b'not found' in blob('preflight/storage-node.ldd.txt'):
            raise ValueError('archived dynamic dependency preflight failed')
        if expected['cloud']['transport'] in ('efa', 'efa-direct'):
            transport = expected['cloud']['transport']
            declared = expected['cloud'].get('fabric_preflight_transport')
            if declared is not None and declared != transport:
                raise ValueError('archived preflight transport identity differs')
            validate_provider(blob('preflight/efa-provider.txt').decode(),
                              transport if transport == 'efa-direct' or declared is not None else None)
            proof = document('efa-proof.json')
            validate_efa_proof(proof, efa_identity(expected))
            if proof['before'] != document('efa-before.json') or proof['after'] != document('efa-after.json'):
                raise ValueError('archived EFA raw snapshots differ from proof')
        validate_node_completion(document('node-config.json'),
            hashlib.sha256(blob('node-config.json')).hexdigest(), resources,
            document('cloud-node-receipt.json'), document('systemd-status.json'), expected)

def efa_identity(config):
    cloud = config['cloud']
    return dict(run_id=cloud['run_id'], cell_id=cloud['cell_id'], instance_id=config['instance_id'],
                config_sha256=cloud['config_sha256'], build_manifest_sha256=cloud['build_manifest_sha256'])



def node_action(action, config):
    """Invoked only on the exact host with its own payload and leaf identity."""
    cfg, cloud = config['campaign'], config['cloud']
    root = Path('/mnt/tidehunter') / cfg['run_id']
    check_cell(cloud['cell_id'], cloud['transport'])
    if cfg['cell_id'] != cloud['cell_id'] or cfg['transport'] != cloud['transport']:
        raise ValueError('node campaign and sampler cell differ')
    node_root = root / 'checkpoint-cells' / cloud['cell_id'] / 'storage'
    if not re.fullmatch(r'th-[0-9a-f]{16}', cfg['run_id']) or cloud['root'] != str(node_root) or node_root.resolve() != node_root:
        raise ValueError('invalid node ownership root')
    bootstrap_gate(root, cfg, config['instance_id'])
    for name in ('resource_path', 'ready_path', 'stop_path'):
        scoped_path(node_root, cloud[name])
    unit = 'sui-storage-' + cfg['run_id'] + '-' + cloud['cell_id']
    evidence_errors = []
    if action == 'start':
        if sha(node_root / 'storage-node') != cloud['storage_binary_sha256']:
            raise ValueError('storage executable changed after deployment')
        preflight([node_root / 'storage-node'], node_root / 'preflight')
        if cfg['transport'] in ('efa', 'efa-direct'):
            fabric_preflight(node_root / 'preflight', cfg['transport'])
            write_json(node_root / 'efa-before.json', efa_snapshot(efa_identity(config)))
        write_json(node_root / 'node-config.json', cloud)
        seconds = int(remaining(cloud['deadline_utc']))
        if seconds < 60:
            raise TimeoutError('storage start lacks work reserve')
        fabric_args = (['--property=LimitMEMLOCK=infinity', '--setenv=FI_EFA_USE_DEVICE_RDMA=1',
                        '--setenv=FI_EFA_ENABLE_SHM_TRANSFER=0'] if cfg['transport'] in ('efa', 'efa-direct') else [])
        subprocess.run(['systemd-run', '--unit=' + unit, '--property=Type=exec',
            '--property=RemainAfterExit=yes', '--property=KillMode=control-group',
            '--property=TimeoutStopSec=15', '--property=RuntimeMaxSec=' + str(seconds),
            '--property=StandardOutput=append:' + str(node_root / 'cloud-node-supervisor.log'),
            '--property=StandardError=inherit', *fabric_args, sys.executable, str(node_root / 'cloud_node.py'),
            '--config', str(node_root / 'node-config.json')], check=True, timeout=15)
        end = time.monotonic() + min(90, max(1, remaining(cloud['deadline_utc'])))
        while not Path(cloud['ready_path']).exists():
            if time.monotonic() >= end:
                raise TimeoutError('storage namespaces failed readiness')
            if subprocess.run(['systemctl', 'is-active', '--quiet', unit], timeout=10).returncode:
                raise RuntimeError('storage sampler unit exited before readiness')
            time.sleep(.25)
    elif action in ('stop', 'final'):
        terminal = node_root / 'systemd-status.json'
        if not terminal.exists():
            Path(cloud['stop_path']).touch(exist_ok=True)
            end = time.monotonic() + min(40, max(1, remaining(cfg['deadline_utc']) - 45))
            while time.monotonic() < end:
                status = systemd_status(unit)
                if status.get('MainPID') == '0' and status.get('SubState') in ('exited', 'failed', 'dead'):
                    break
                time.sleep(.25)
            else:
                subprocess.run(['systemctl', 'stop', unit], check=False, timeout=20)
                status = systemd_status(unit) | {'forced_stop': True}
            # Retain the observed exit before RuntimeMaxSec or guest TTL can
            # unload the unit. Never overwrite a failed/unknown terminal result.
            write_json(terminal, status)
        if cfg['transport'] in ('efa', 'efa-direct') and not (node_root / 'efa-proof.json').exists():
            try:
                after = efa_snapshot(efa_identity(config))
                write_json(node_root / 'efa-after.json', after)
                write_json(node_root / 'efa-proof.json', efa_proof(
                    read_json(node_root / 'efa-before.json'), after, efa_identity(config)))
            except (OSError, ValueError, KeyError) as error:
                evidence_errors.append('EFA proof: ' + repr(error))
    elif action == 'release':
        subprocess.run(['systemctl', 'stop', unit], check=True, timeout=20)
        status = systemd_status(unit)
        if status.get('MainPID') != '0' or status.get('ActiveState') not in ('inactive', 'failed'):
            raise RuntimeError('previous storage unit has not fully stopped')
        for record in read_json(node_root / 'cloud-node-receipt.json')['namespaces']:
            proc = Path(f"/proc/{record['pid']}/stat")
            try:
                fields = proc.read_text().rsplit(')', 1)[1].split()
            except FileNotFoundError:
                continue
            if int(fields[19]) == record['starttime_ticks']:
                raise RuntimeError('previous namespace process still exists')
    elif action != 'live':
        raise ValueError('unknown node operation')
    if action in ('live', 'final', 'stop'):
        label = 'final' if action in ('final', 'stop') else 'live'
        target = node_root / ('export-' + label)
        target.mkdir(exist_ok=True)
        errors = evidence_errors
        resource = Path(cloud['resource_path'])
        if resource.is_file():
            # Read one immutable atomic publication, never copy a moving stream.
            write_json(target / 'resources.json', read_json(resource))
        else:
            errors.append('resource snapshot unavailable')
        session = boto3.Session(region_name=cfg['region'])
        s3 = session.client('s3', config=SDK_CONFIG)
        key = f"{cfg['s3_prefix']}/results/cells/{cloud['cell_id']}/storage/{config['partition']}/{label}"
        receipts = {}
        files = {'resources': 'resources.json'} if action == 'live' else completion_files(cloud['transport'])
        for name, filename in files.items():
            source = target / filename if name == 'resources' else node_root / filename
            if source.is_file() and not source.is_symlink():
                if source.parent != target:
                    shutil.copy2(source, target / filename)
                receipts[name] = upload_final(s3, target / filename, cfg, key + '/' + filename)
            else:
                errors.append('mandatory completion artifact unavailable: ' + filename)
        if action == 'final':
            journal = subprocess.run(['journalctl', '--no-pager', '-u', unit], capture_output=True, timeout=15)
            (target / 'journal.log').write_bytes(journal.stdout + journal.stderr)
            if journal.returncode:
                errors.append('systemd journal collection failed')
            archive = target / 'evidence.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                for name in sorted(required_node_files(config)):
                    if name == 'bootstrap.json':
                        path = root / name
                    elif name in set(completion_files(cloud['transport']).values()) | {'journal.log'}:
                        path = target / name
                    else:
                        path = node_root / name
                    if path.is_file() and not path.is_symlink():
                        stream.add(path, arcname=name, recursive=False)
                    else:
                        errors.append('mandatory node evidence unavailable: ' + name)
            receipts['evidence'] = upload_final(s3, archive, cfg, key + '/evidence.tar.gz')
        write_json(target / 'receipt.json', dict(run_id=cfg['run_id'], cell_id=cloud['cell_id'],
                   instance_id=config['instance_id'], partition=config['partition'], receipts=receipts, errors=errors))
        upload_final(s3, target / 'receipt.json', cfg, key + '/receipt.json')


class SuiCampaign(Campaign):
    """Reuse ownership/export primitives, never the old Campaign executor/builder."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.root, self.ids = validate_cfg(cfg)
        self.lock = (self.root / '.sui-checkpoint.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.results = self.root / 'checkpoint-results'
        self.campaign_results = self.results
        self.plan = matrix_plan(cfg)
        self.session = boto3.Session(region_name=cfg['region'])
        self.s3 = self.session.client('s3', config=SDK_CONFIG)
        self.ssm = self.session.client('ssm', config=SSM_CONFIG)
        self.ec2 = self.session.client('ec2', config=SDK_CONFIG)
        self.deadline = datetime.fromtimestamp(datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00')).timestamp() - cfg.get('export_reserve_seconds', 600), timezone.utc).isoformat()
        self.state = {'run_id': cfg['run_id'], 'campaign': 'sui-checkpoint', 'complete': False, 'failures': [], 'uploads': [], 'cells': []}
        self.nodes = sorted(cfg['nodes'], key=lambda n: n['partition'])
        self.attempted_nodes = set()
        self.collected_final = set()
        self.child = None

    def inventory(self):
        self.check_owned_instances()
        instances = read_json(self.results / 'owned-instances.json')
        groups = self.ec2.describe_placement_groups(GroupNames=[self.cfg['placement_group']])['PlacementGroups']
        write_json(self.results / 'placement-proof.json', dict(run_id=self.cfg['run_id'],
            build_manifest_sha256=self.cfg['build_manifest_sha256'], source_archive_sha256=self.cfg['source_archive_sha256'],
            availability_zone=self.cfg['availability_zone'], placement_group=self.cfg['placement_group'],
            instances=instances, groups=groups))
        validate_placement(self.cfg, instances, groups)
        ips = {self.cfg['coordinator_instance_id']: self.cfg['coordinator_private_ip']}
        ips.update({n['instance_id']: n['private_ip'] for n in self.nodes})
        for instance in instances:
            if instance.get('PrivateIpAddress') != ips[instance['InstanceId']] or instance['State']['Name'] != 'running':
                raise ValueError('owned fleet IP/state differs from exact plan')
            value = self.ec2.describe_instance_attribute(InstanceId=instance['InstanceId'], Attribute='instanceInitiatedShutdownBehavior')
            if value['InstanceInitiatedShutdownBehavior']['Value'] != 'terminate':
                raise ValueError('absolute poweroff backstop must terminate, not stop')
        write_json(self.results / 'bootstrap.json', bootstrap_gate(self.root, self.cfg, self.cfg['coordinator_instance_id']))

    def admit_node(self, node):
        common = {k: self.cfg[k] for k in ('run_id', 'account', 'region', 'instance_type', 'deadline_utc')}
        code = '\n'.join([
            'import json, subprocess, time', 'from pathlib import Path', 'from datetime import datetime',
            inspect.getsource(read_json), inspect.getsource(remaining), inspect.getsource(bootstrap_gate),
            'cfg=json.loads(' + repr(json.dumps(common)) + ')',
            'print(json.dumps(bootstrap_gate(Path(' + repr(str(self.root)) + '),cfg,' + repr(node['instance_id']) + ')))',
        ])
        self.ssm_one(node, 'bootstrap-gate', shlex.join(['python3', '-c', code]), timeout=90)

    def ssm_one(self, node, action, command, timeout=120, cleanup=False):
        limit = min(timeout + 15, max(1, remaining(self.cfg['deadline_utc'] if cleanup else self.deadline) - (30 if cleanup else 0)))
        if limit < 5:
            raise TimeoutError('SSM operation lacks absolute deadline reserve')
        reply = self.ssm.send_command(InstanceIds=[node['instance_id']], DocumentName='AWS-RunShellScript',
            Parameters={'commands': ['set -eu\n' + command], 'executionTimeout': [str(max(1, min(timeout, int(limit))))]},
            TimeoutSeconds=30, Comment=f"{self.cfg['run_id']} sui-{action}")
        command_id = reply['Command']['CommandId']
        write_json(self.results / f"ssm-{node['partition']}-{action}-{command_id}-sent.json", {'command_id': command_id, 'instance_id': node['instance_id']})
        end = time.monotonic() + limit
        while time.monotonic() < end:
            try:
                item = self.ssm.get_command_invocation(CommandId=command_id, InstanceId=node['instance_id'])
            except self.ssm.exceptions.InvocationDoesNotExist:
                time.sleep(.25)
                continue
            if item['Status'] not in ('Pending', 'InProgress', 'Delayed'):
                write_json(self.results / f"ssm-{node['partition']}-{action}-{command_id}.json", item)
                if item['Status'] != 'Success':
                    raise RuntimeError(f"storage {node['partition']} {action} SSM {item['Status']}")
                return
            time.sleep(.5)
        # Never replace a start with unknown outcome. The deterministic unit and
        # exact-node final stop plus bootstrap expiry own its cleanup.
        raise TimeoutError(f'SSM {action} outcome unknown at fixed deadline')

    def parallel_nodes(self, action, function, nodes=None):
        failures = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            work = {executor.submit(function, node): node for node in (self.nodes if nodes is None else nodes)}
            for future in as_completed(work):
                try:
                    future.result()
                except BaseException as error:
                    failures.append(f"partition {work[future]['partition']}: {error!r}")
        if failures:
            raise RuntimeError(action + ': ' + '; '.join(failures))

    def run_command(self, name, command, timeout=None):
        available = remaining(self.deadline)
        if available <= 0:
            raise TimeoutError('campaign work deadline reached')
        with (self.results / (name + '.log')).open('xb') as log:
            child = subprocess.Popen(list(map(str, command)), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = child.wait(timeout=min(available, timeout or available))
                if code:
                    raise subprocess.CalledProcessError(code, command)
            finally:
                self.stop_process(child)

    @staticmethod
    def stop_process(child):
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=5)

    def inputs(self):
        inputs = self.root / 'checkpoint-inputs'
        inputs.mkdir(mode=0o700)
        for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            archive = inputs / (name + '.tar.gz')
            item = self.cfg[name]
            download(self.s3, self.cfg, item['key'], item['sha256'], archive)
            safe_extract(archive, inputs / name)
        self.build = inputs / 'build_bundle'
        self.fixture = inputs / 'fixture_bundle/fixture'
        self.verification = inputs / 'verification_bundle/verification'
        manifest = read_json(self.build / 'build-manifest.json')
        if sha(self.build / 'build-manifest.json') != self.cfg['build_manifest_sha256']:
            raise ValueError('build manifest differs from MCP immutable plan')
        for name, key in [('sources.tar.gz', 'source_archive_sha256'), ('source-manifest.json', 'source_manifest_sha256')]:
            if sha(self.build / name) != self.cfg[key] or manifest[key] != self.cfg[key]:
                raise ValueError('source archive/manifest binding differs')
        if manifest['protocol'] != 8 or manifest['binaries'] != self.cfg['binaries']:
            raise ValueError('wrong wire protocol or executable inventory')
        if any(transport in ('efa', 'efa-direct') for transport in self.cfg['transports']) and not manifest.get('fabric'):
            raise ValueError('transport requires fabric-enabled frozen binaries')
        for name, expected in manifest['binaries'].items():
            if sha(self.build / 'binaries' / name) != expected:
                raise ValueError('executable SHA-256 mismatch: ' + name)
        commands = manifest['commands']
        if len(commands) != len(BINARIES) or {c['label'] for c in commands} != BINARIES:
            raise ValueError('build is not complete: exact six successful source-bound invocations required')
        for command in commands:
            name = command['label']
            if command['sha256'] != manifest['binaries'][name] or sha(self.build / f'build-{name}.log') != command['log_sha256']:
                raise ValueError('source-bound build command/log receipt differs')
            if sha(self.build / f'build-{name}.jsonl') != command['messages_sha256']:
                raise ValueError('source-bound Cargo artifact message receipt differs')
        source_unpack = inputs / 'source-unpack'
        safe_extract(self.build / 'sources.tar.gz', source_unpack, symlinks=True)
        (source_unpack / 'sources').rename(self.build / 'sources')
        source_unpack.rmdir()
        sources = read_json(self.build / 'source-manifest.json')
        owners = {'sui', 'tidehunter-sharded-wal'}
        if set(sources) != owners or {p.name for p in (self.build / 'sources').iterdir()} != owners:
            raise ValueError('frozen sources must contain only Sui and the active engine')
        checkpoint_support = 'experiments/sui-checkpoint/checkpoint_support.py'
        entry = sources['tidehunter-sharded-wal']['files'].get(checkpoint_support, {})
        path = self.build / 'sources/tidehunter-sharded-wal' / checkpoint_support
        if 'sha256' not in entry or path.is_symlink() or not path.is_file():
            raise ValueError('checkpoint support must be an ordinary hash-bound engine source')
        for owner in ('sui', 'tidehunter-sharded-wal'):
            base = self.build / 'sources' / owner
            entries = sources[owner]['files']
            actual = {str(p.relative_to(base)) for p in base.rglob('*') if p.is_file() or p.is_symlink()}
            if actual != set(entries):
                raise ValueError('source archive inventory differs: ' + owner)
            for name, entry in entries.items():
                path = scoped_path(base, base / name)
                if 'link' in entry:
                    if not path.is_symlink() or os.readlink(path) != entry['link']:
                        raise ValueError('source symlink binding differs')
                elif path.is_symlink() or sha(path) != entry['sha256']:
                    # tarfile's data filter intentionally strips unsafe write
                    # permissions. Archive and manifest hashes bind original modes.
                    raise ValueError('source content differs: ' + name)
        self.scripts = self.build / 'sources/tidehunter-sharded-wal/experiments/sui-checkpoint'
        self.support = self.build / 'sources/tidehunter-sharded-wal/experiments/aws-sharded-wal'
        # Admission binds even the already executing lifecycle helpers, not only
        # the scripts we are about to spawn from the immutable archive.
        for path in Path(__file__).resolve().parent.glob('*.py'):
            relative = 'experiments/aws-sharded-wal/' + path.name
            entry = sources['tidehunter-sharded-wal']['files'].get(relative)
            if entry is None or sha(path) != entry['sha256']:
                raise ValueError('installed supervisor/helper differs from frozen build: ' + path.name)
        self.verify_local()
        preflight([self.build / 'binaries' / name for name in sorted(BINARIES)], self.results / 'preflight')
        for transport in self.cfg['transports']:
            if transport in ('efa', 'efa-direct'):
                output = self.results / 'preflight'
                if transport == 'efa-direct':
                    output = output / transport
                    output.mkdir()
                fabric_preflight(output, transport)
        for name in ('build-manifest.json', 'source-manifest.json', 'plan.json', 'portability.json'):
            shutil.copy2(self.build / name, self.results / name)
        write_json(self.results / 'input-receipt.json', {key: self.cfg[key] for key in ('build_bundle', 'fixture_bundle', 'verification_bundle', 'build_manifest_sha256', 'source_archive_sha256', 'source_manifest_sha256', 'binaries')})

    def verify_local(self):
        fixture_manifest = read_json(self.fixture / 'fixture-manifest.json')
        expected = fixture_manifest['files']
        actual = {str(p.relative_to(self.fixture)) for p in self.fixture.rglob('*') if p.is_file()}
        if actual != set(expected) | {'fixture-manifest.json'}:
            raise ValueError('portable fixture inventory differs')
        for name, digest in expected.items():
            if sha(scoped_path(self.fixture, self.fixture / name)) != digest:
                raise ValueError('portable frozen fixture file differs')
        prior = read_json(self.fixture / 'experiment/results.json')
        if not prior.get('source_frozen') or prior.get('failure') or prior['checkpoint_limit'] != TIP or prior['checkpoint_digest'] != CHECKPOINT:
            raise ValueError('fixture is not the completed frozen real chain')
        if fixture_manifest['checkpoint_digest'] != CHECKPOINT or fixture_manifest['checkpoint_limit'] != TIP or fixture_manifest['source_range'] != prior['verified_source_range']:
            raise ValueError('fixture receipt/range differs')
        receipt = read_json(self.verification / 'receipt.json')
        result = read_json(self.verification / 'experiment/results.json')
        audit = read_json(self.verification / 'experiment/audit.json')
        if receipt.get('complete') is not True or result.get('complete') is not True or result.get('failure'):
            raise ValueError('cloud admission requires a successful complete LOCAL replay')
        if sha(self.verification / 'evidence.tar.gz') != receipt['evidence_sha256']:
            raise ValueError('local verification evidence archive differs')
        for record in (receipt, result):
            if record['build_manifest_sha256'] != self.cfg['build_manifest_sha256'] or record['source_archive_sha256'] != self.cfg['source_archive_sha256']:
                raise ValueError('local replay belongs to another build/source')
        if result['binaries'] != self.cfg['binaries'] or result['checkpoint_digest'] != CHECKPOINT or result['checkpoint_limit'] != TIP or result['source_range'] != prior['verified_source_range']:
            raise ValueError('local successful replay used another executable or chain')
        if 'loopback' not in result['topology'] or result.get('external_manifest'):
            raise ValueError('a cloud replay cannot satisfy the prerequisite local replay')
        if audit != result['audit'] or audit.get('all_equal') is not True or not audit.get('databases') or not audit.get('rocksdb'):
            raise ValueError('local full-state audit is incomplete')
        for mode in ('native', 'remote'):
            if result[mode]['completed_checkpoint_digest'] != CHECKPOINT:
                raise ValueError('local replay did not complete the exact certified tip')
        producer = {k: v for k, v in expected.items() if k.startswith('producer/')}
        if producer != {k: v for k, v in result['fixture_files'].items() if k.startswith('producer/')}:
            raise ValueError('local replay used different frozen producer bytes')
        unpack = self.verification / 'bound-evidence'
        safe_extract(self.verification / 'evidence.tar.gz', unpack)
        supplied = self.verification / 'experiment'
        allowed = {'results.json', 'audit.json'}
        files = [p for p in supplied.rglob('*') if p.is_file()]
        if not any('native-schema' in p.parts for p in files):
            raise ValueError('same-build native schema is absent')
        for path in files:
            relative = path.relative_to(supplied)
            if relative.parts[0] not in allowed | {'native-schema'}:
                raise ValueError('verification bundle has unrelated experiment input')
            bound = unpack / 'experiment' / relative
            if not bound.is_file() or sha(path) != sha(bound):
                raise ValueError('supplied schema/results are not bound to local evidence')
        wanted = {str(p.relative_to(unpack / 'experiment/native-schema')) for p in (unpack / 'experiment/native-schema').rglob('*') if p.is_file()}
        found = {str(p.relative_to(supplied / 'native-schema')) for p in (supplied / 'native-schema').rglob('*') if p.is_file()}
        if wanted != found:
            raise ValueError('native schema coverage differs from complete local replay')
        shutil.copy2(self.verification / 'receipt.json', self.results / 'local-verification-receipt.json')
        shutil.copy2(supplied / 'results.json', self.results / 'local-verification-results.json')

    def prepare(self):
        self.replay_root = self.cell_root / 'replay'
        # Portable fixture paths preserve the original native/remote DB suffix.
        import yaml
        config = yaml.safe_load((self.fixture / 'distributed.yaml').read_text())
        suffix = Path(config['db-path']).relative_to('distributed')
        db_root = self.replay_root / 'remote' / suffix
        self.deployment = self.cell_root / 'deployment'
        command = [sys.executable, self.scripts / 'prepare_external.py', '--schema', self.verification / 'experiment/native-schema',
            '--build', self.build, '--output', self.deployment, '--db-root', db_root,
            '--guest-root', self.cell_root / 'storage', '--hosts', ','.join(n['private_ip'] for n in self.nodes),
            '--transport', self.cell['transport'], '--port', str(self.cfg.get('port', 24000)),
            '--write-profile-limit', str(self.cell['write_profile_limit']),
            '--coordinator-cache-mib', str(self.cell['coordinator_cache_mib']), '--shard-cache-mib', str(self.cell['shard_cache_mib'])]
        self.run_command('prepare-external', command, 120)
        prepared = read_json(self.deployment / 'config.json')
        if not prepared['databases'] or any(
                database['tls']['transport'] != self.cell['transport']
                or database.get('write_profile_limit', 0) != self.cell['write_profile_limit']
                for database in prepared['databases'].values()):
            raise ValueError('prepared transport/profiling differs from matrix cell; no fallback')
        self.cell['namespace_ids'] = {name: database['storage']['database']
                                     for name, database in prepared['databases'].items()}
        self.external = read_json(self.deployment / 'external-manifest.json')
        if (self.external['build_manifest_sha256'] != self.cfg['build_manifest_sha256']
                or self.external['config_sha256'] != sha(self.deployment / 'config.json')
                or self.external['storage_binary_sha256'] != self.cfg['binaries']['storage-node']):
            raise ValueError('prepared deployment source binding differs')
        shutil.copy2(self.deployment / 'config.json', self.results / 'external-config.json')
        shutil.copy2(self.deployment / 'external-manifest.json', self.results / 'external-manifest.json')
        for host, node in zip(self.external['hosts'], self.nodes):
            if host['partition'] != node['partition'] or host['host'] != node['private_ip']:
                raise ValueError('host deployment inventory differs')
            original = Path(host['archive'])
            if sha(original) != host['archive_sha256']:
                raise ValueError('host payload digest differs')
            payload = self.cell_root / f"payload-{node['partition']}"
            safe_extract(original, payload)
            shutil.copy2(self.scripts / 'cloud_node.py', payload / 'cloud_node.py')
            support = payload / 'runner'
            support.mkdir()
            for path in self.support.glob('*.py'):
                shutil.copy2(path, support / path.name)
            cloud = {key: host[key] for key in ('commands', 'ready_files', 'stop_files')}
            cloud.update(run_id=self.cfg['run_id'], cell_id=self.cell['cell_id'], transport=self.cell['transport'], deadline_utc=self.deadline,
                storage_binary_sha256=self.cfg['binaries']['storage-node'], build_manifest_sha256=self.cfg['build_manifest_sha256'],
                config_sha256=self.external['config_sha256'], checkpoint_digest=CHECKPOINT,
                root=str(self.cell_root / 'storage'), resource_path=str(self.cell_root / 'storage/resources.json'),
                ready_path=str(self.cell_root / 'storage/ready'), stop_path=str(self.cell_root / 'storage/stop'))
            if self.cell['transport'] in ('efa', 'efa-direct'):
                cloud['fabric_preflight_transport'] = self.cell['transport']
            common = {k: self.cfg[k] for k in ('run_id', 'account', 'region', 'instance_type', 'deadline_utc', 'bucket', 's3_prefix')}
            common.update(cell_id=self.cell['cell_id'], transport=self.cell['transport'])
            write_json(payload / 'supervisor-node.json', dict(campaign=common, cloud=cloud,
                       instance_id=node['instance_id'], partition=node['partition']))
            archive = payload.with_suffix('.tar.gz')
            with tarfile.open(archive, 'w:gz') as stream:
                for path in sorted(payload.iterdir()):
                    stream.add(path, arcname=path.name)
            key = f"{self.cfg['s3_prefix']}/deploy/cells/{self.cell['cell_id']}/storage-{node['partition']}.tar.gz"
            receipt = upload(self.s3, archive, self.cfg, key)
            node['payload'] = receipt
            self.state['uploads'].append(receipt)

    def install_node(self, node):
        # This tiny bootstrap cannot import the frozen supervisor until its own
        # per-host object is verified and safely extracted. No foreign key exists
        # in this archive: only the selected leaf and public CA were packaged.
        payload = dict(bucket=self.cfg['bucket'], account=self.cfg['account'], region=self.cfg['region'],
            root=str(self.root), cell_root=str(self.cell_root), key=node['payload']['key'], sha256=node['payload']['sha256'])
        code = '\n'.join([
            'import boto3, hashlib, json, tarfile', 'from pathlib import Path, PurePosixPath',
            'from botocore.config import Config', inspect.getsource(safe_extract),
            'p=json.loads(' + repr(json.dumps(payload)) + ')',
            'root=Path(p["root"])',
            'assert root.resolve()==root and (root/"bootstrap.json").is_file()',
            'cell=Path(p["cell_root"]); cell.mkdir(parents=True,exist_ok=False)',
            'archive=cell/"host-payload.tar.gz"',
            'with archive.open("xb"): pass',
            's3=boto3.client("s3",region_name=p["region"],config=Config(connect_timeout=5,read_timeout=15,retries={"total_max_attempts":2}))',
            's3.download_file(p["bucket"],p["key"],str(archive),ExtraArgs={"ExpectedBucketOwner":p["account"]})',
            'with archive.open("rb") as f: actual=hashlib.file_digest(f,"sha256").hexdigest()',
            'if actual!=p["sha256"]: raise ValueError("host payload SHA-256 differs")',
            'safe_extract(archive,cell/"storage",max_bytes=4<<30)',
        ])
        self.attempted_nodes.add(node['partition'])
        self.ssm_one(node, 'install', shlex.join(['python3', '-c', code]), timeout=180)
        self.ssm_one(node, 'start', self.node_command('start'), timeout=120)

    def node_command(self, action):
        root = self.cell_root / 'storage'
        return shlex.join(['python3', str(root / 'runner/sui_checkpoint.py'), 'node', '--action', action,
                           '--config', str(root / 'supervisor-node.json')])

    def object_json(self, key, destination):
        response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
        with response['Body'] as body:
            raw = body.read(32 * 1024 * 1024 + 1)
        if len(raw) > 32 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != response.get('Metadata', {}).get('sha256'):
            raise ValueError('node JSON upload receipt differs or exceeds limit')
        destination.write_bytes(raw)
        return json.loads(raw)

    def collect_node(self, node, final=False, logs=False):
        # Keep full log compression/transfers outside the runner's 120s resource
        # handshake. The final sampler snapshot itself is already complete.
        action = 'final' if logs else 'stop' if final else 'live'
        self.ssm_one(node, action, self.node_command(action),
                     timeout=180 if logs else 65 if final else 30, cleanup=final)
        label = 'final' if final else 'live'
        local = self.results / 'storage' / str(node['partition']) / label
        local.mkdir(parents=True, exist_ok=True)
        prefix = f"{self.cfg['s3_prefix']}/results/cells/{self.cell['cell_id']}/storage/{node['partition']}/{label}"
        receipt = self.object_json(prefix + '/receipt.json', local / 'receipt.json')
        if receipt.get('cell_id') != self.cell['cell_id'] or receipt['run_id'] != self.cfg['run_id'] or receipt['instance_id'] != node['instance_id'] or receipt['partition'] != node['partition']:
            raise ValueError('node export receipt identity differs')
        filenames = completion_files(self.cell['transport']) if final else {'resources': 'resources.json'}
        if logs:
            filenames = filenames | {'evidence': 'evidence.tar.gz'}
        for name, item in receipt['receipts'].items():
            filename = filenames.get(name)
            if filename is None or item['key'] != prefix + '/' + filename or item['bucket'] != self.cfg['bucket']:
                raise ValueError('node export outside exact partition prefix')
            download(self.s3, self.cfg, item['key'], item['sha256'], local / filename)
        if receipt['errors'] or set(receipt['receipts']) != set(filenames):
            raise RuntimeError('node resource/evidence receipt is incomplete: ' + repr(receipt['errors']))
        if final:
            expected = read_json(self.cell_root / f"payload-{node['partition']}/supervisor-node.json")
            resources = read_json(local / 'resources.json')
            validate_node_completion(read_json(local / 'node-config.json'), sha(local / 'node-config.json'),
                resources, read_json(local / 'cloud-node-receipt.json'), read_json(local / 'systemd-status.json'), expected)
            if self.cell['transport'] == 'efa':
                validate_efa_proof(read_json(local / 'efa-proof.json'), efa_identity(expected))
            committed = self.results / 'resources.json'
            if committed.exists() and resources != read_json(committed)['hosts'][node['private_ip']]:
                raise ValueError('final resources differ from the runner handshake snapshot')
            if logs:
                validate_node_archive(local / 'evidence.tar.gz', expected, resources)
                self.collected_final.add(node['partition'])
                self.cell['node_archives'][node['instance_id']] = receipt['receipts']['evidence']

    def resources(self, stages):
        start, end = stages['remote-starting']['unix_ns'], stages['remote-finished']['unix_ns']
        if not start < end <= stages['resources-required']['unix_ns']:
            raise ValueError('remote stage timeline is not ordered')
        hosts = {}
        for node in self.nodes:
            record = read_json(self.results / 'storage' / str(node['partition']) / 'final/resources.json')
            if record.get('complete') is not True or record.get('failure') or not record.get('all_namespaces_healthy'):
                raise ValueError('storage host did not complete cleanly')
            for key, expected in [('run_id', self.cfg['run_id']), ('cell_id', self.cell['cell_id']), ('transport', self.cell['transport']), ('config_sha256', self.external['config_sha256']),
                                  ('build_manifest_sha256', self.cfg['build_manifest_sha256']), ('checkpoint_digest', CHECKPOINT),
                                  ('storage_binary_sha256', self.cfg['binaries']['storage-node'])]:
                if record.get(key) != expected:
                    raise ValueError('storage resource identity differs: ' + key)
            samples = record['samples']
            stamps = [sample['unix_ns'] for sample in samples]
            if not stamps or stamps != sorted(stamps) or stamps[0] > start or stamps[-1] < end:
                raise ValueError('storage samples do not bracket remote replay (native idle is insufficient)')
            during = [sample for sample in samples if start <= sample['unix_ns'] <= end]
            if not during or not all(sample.get('processes') for sample in during):
                raise ValueError('storage has no physical process observations during remote replay')
            for sample in during:
                if len(sample['processes']) != len(self.external['hosts'][node['partition']]['commands']):
                    raise ValueError('remote resource samples miss a selected namespace')
                for process in sample['processes']:
                    if process.get('exited') or not all(k in process for k in ('pid', 'VmRSS', 'VmSwap', 'io')):
                        raise ValueError('remote IO/RSS/swap observation unavailable')
            for key in ('swap_before', 'swap_after', 'diskstats_before', 'diskstats_after'):
                if record.get(key) is None:
                    raise ValueError('storage host physical evidence absent')
            hosts[node['private_ip']] = record
        return dict(config_sha256=self.external['config_sha256'], checkpoint_digest=CHECKPOINT,
                    build_manifest_sha256=self.cfg['build_manifest_sha256'], cell_id=self.cell['cell_id'], stages=stages, hosts=hosts)

    def replay(self):
        stages_dir = self.results / 'stages'
        stages_dir.mkdir()
        resources = self.results / 'resources.json'
        command = [sys.executable, self.scripts / 'run.py', '--build', self.build, '--fixture', self.fixture,
            '--output', self.replay_root, '--deadline', str(self.cfg.get('replay_deadline_seconds', 900)),
            '--external-config', self.deployment / 'config.json', '--external-resources', resources,
            '--external-stage-dir', stages_dir, '--port', str(self.cfg.get('port', 24000)),
            '--read-profile-limit', str(self.cell['read_profile_limit']),
            '--write-profile-limit', str(self.cell['write_profile_limit']),
            '--native-write-profile-limit', str(self.cell['native_write_profile_limit']),
            '--coordinator-cache-mib', str(self.cell['coordinator_cache_mib']), '--shard-cache-mib', str(self.cell['shard_cache_mib'])]
        write_json(self.results / 'replay-command.json', [str(value) for value in command])
        with (self.results / 'replay-supervisor.log').open('xb') as log:
            self.child = subprocess.Popen(list(map(str, command)), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        stages, next_collect, supplied = {}, time.monotonic(), False
        try:
            while self.child.poll() is None:
                if remaining(self.deadline) <= 0:
                    raise TimeoutError('absolute measured-work deadline reached')
                for name in ('remote-starting', 'remote-finished', 'resources-required'):
                    path = stages_dir / (name + '.json')
                    if path.exists() and name not in stages:
                        stage = read_json(path)
                        if (stage['stage'] != name or stage['config_sha256'] != self.external['config_sha256']
                                or stage['build_manifest_sha256'] != self.cfg['build_manifest_sha256']
                                or stage['checkpoint_digest'] != CHECKPOINT):
                            raise ValueError('runner stage source/config/checkpoint binding differs')
                        stages[name] = stage
                        self.state.update(stage=name, updated_utc=datetime.now(timezone.utc).isoformat())
                        write_json(self.results / 'progress.json', self.state)
                if 'resources-required' in stages and not supplied:
                    if set(stages) != {'remote-starting', 'remote-finished', 'resources-required'}:
                        raise ValueError('runner omitted durable remote stage markers')
                    self.parallel_nodes('final post-audit resources', lambda node: self.collect_node(node, final=True))
                    write_json(resources, self.resources(stages))
                    write_json(stages_dir / 'resources-ready', dict(config_sha256=self.external['config_sha256'],
                        build_manifest_sha256=self.cfg['build_manifest_sha256'], checkpoint_digest=CHECKPOINT,
                        unix_ns=time.time_ns()))
                    supplied = True
                elif not supplied and time.monotonic() >= next_collect:
                    self.parallel_nodes('live resources', self.collect_node)
                    next_collect = time.monotonic() + 15
                time.sleep(.2)
            if self.child.returncode:
                raise subprocess.CalledProcessError(self.child.returncode, command)
            if not supplied:
                raise ValueError('runner exited without post-audit physical resource handshake')
        finally:
            self.stop_process(self.child)
        self.run_command('verify-evidence', [sys.executable, self.scripts / 'analyze.py', self.replay_root, '--verify-only'], 90)
        result = read_json(self.replay_root / 'experiment/results.json')
        if result.get('complete') is not True or result.get('failure'):
            raise ValueError('real paired replay/full-state audit did not complete')
        self.state['performance_analysis'] = 'deferred until confirmed fleet termination'
        if result.get('binaries') != self.cfg['binaries'] or result.get('build_manifest_sha256') != self.cfg['build_manifest_sha256']:
            raise ValueError('paired result build differs from matrix cell')

    def retain_replay(self):
        # Never include DBs or deployment TLS keys; keep every cell's public logs.
        if hasattr(self, 'replay_root') and self.replay_root.exists():
            for name in ('evidence.tar.gz', 'receipt.json'):
                if (self.replay_root / name).is_file():
                    shutil.copy2(self.replay_root / name, self.results / ('replay-' + name))
            experiment = self.replay_root / 'experiment'
            if experiment.exists():
                raw = self.results / 'replay-logs'
                raw.mkdir(exist_ok=True)
                for path in experiment.rglob('*'):
                    relative = path.relative_to(experiment)
                    if (path.is_file() and not path.is_symlink()
                            and not any(part in ('tls', 'storage') for part in relative.parts)):
                        destination = raw / relative
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(path, destination)

    def run_matrix(self):
        for planned in self.plan:
            if remaining(self.deadline) < 60:
                raise TimeoutError('matrix cannot start another cell inside original work deadline')
            self.cell = dict(planned, complete=False, node_archives={}, efa={},
                build_manifest_sha256=self.cfg['build_manifest_sha256'], binaries=self.cfg['binaries'],
                fixture_bundle=self.cfg['fixture_bundle'], verification_bundle=self.cfg['verification_bundle'],
                source_archive_sha256=self.cfg['source_archive_sha256'], source_manifest_sha256=self.cfg['source_manifest_sha256'])
            self.state['cells'].append(self.cell)
            self.cell_root = self.root / 'checkpoint-cells' / self.cell['cell_id']
            self.cell_root.mkdir(parents=True, exist_ok=False)
            self.results = self.campaign_results / 'cells' / self.cell['cell_id']
            self.results.mkdir(parents=True, exist_ok=False)
            self.attempted_nodes, self.collected_final = set(), set()
            self.child = None
            self.prepare()
            self.cell['config_sha256'] = self.external['config_sha256']
            write_json(self.results / 'cell-receipt.json', self.cell)
            identity = dict(run_id=self.cfg['run_id'], cell_id=self.cell['cell_id'],
                instance_id=self.cfg['coordinator_instance_id'], config_sha256=self.external['config_sha256'],
                build_manifest_sha256=self.cfg['build_manifest_sha256'])
            if self.cell['transport'] in ('efa', 'efa-direct'):
                fabric_preflight(self.results, self.cell['transport'])
                write_json(self.results / 'efa-before.json', efa_snapshot(identity))
            self.parallel_nodes('deploy', self.install_node)
            self.parallel_nodes('initial resources', self.collect_node)
            self.replay()
            self.parallel_nodes('final cell logs/export', lambda node: self.collect_node(node, final=True, logs=True))
            if self.cell['transport'] in ('efa', 'efa-direct'):
                after = efa_snapshot(identity)
                write_json(self.results / 'efa-after.json', after)
                proof = efa_proof(read_json(self.results / 'efa-before.json'), after, identity)
                write_json(self.results / 'efa-proof.json', proof)
                self.cell['efa'][self.cfg['coordinator_instance_id']] = proof
                for node in self.nodes:
                    self.cell['efa'][node['instance_id']] = read_json(
                        self.results / 'storage' / str(node['partition']) / 'final/efa-proof.json')
            self.parallel_nodes('release previous cell', lambda node: self.ssm_one(
                node, 'release', self.node_command('release'), timeout=30))
            self.retain_replay()
            self.cell['replay_evidence_sha256'] = sha(self.results / 'replay-evidence.tar.gz')
            self.cell['resources_sha256'] = sha(self.results / 'resources.json')
            self.cell['complete'] = True
            write_json(self.results / 'cell-receipt.json', self.cell)
        validate_matrix(self.cfg, self.state['cells'])
        self.state['complete'] = True

    def export(self):
        if hasattr(self, 'cell') and self.cell['transport'] in ('efa', 'efa-direct') and (self.results / 'efa-before.json').exists() and not (self.results / 'efa-after.json').exists():
            try:
                before = read_json(self.results / 'efa-before.json')
                write_json(self.results / 'efa-after.json', efa_snapshot(before['identity']))
            except (OSError, ValueError) as error:
                self.state['failures'].append('partial coordinator EFA snapshot: ' + repr(error))
        self.retain_replay()
        if hasattr(self, 'cell'):
            write_json(self.results / 'cell-receipt.json', self.cell)
        self.results = self.campaign_results
        write_json(self.results / 'after.json', evidence(self.root))
        write_json(self.results / 'campaign-receipt.json', self.state)
        if self.state['complete']:
            validate_matrix(self.cfg, self.state['cells'])
            (self.results / 'COMPLETE').write_text('all requested Sui pairs, full-state audits, storage/EFA evidence and any configured KV subsuite complete; timing analysis deferred until termination\n')
        archive = self.root / 'checkpoint-results.tar.gz'
        with tarfile.open(archive, 'w:gz') as stream:
            stream.add(self.results, arcname='results')
        receipt = upload_final(self.s3, archive, self.cfg, f"{self.cfg['s3_prefix']}/results/controller.tar.gz")
        receipt.update(run_id=self.cfg['run_id'], complete=self.state['complete'],
                       build_manifest_sha256=self.cfg['build_manifest_sha256'], checkpoint_digest=CHECKPOINT,
                       teardown='exact owned six-host termination attempted unconditionally after export; original absolute shutdown timers remain armed; verify via MCP')
        path = self.root / 'checkpoint-result-receipt.json'
        write_json(path, receipt)
        upload_final(self.s3, path, self.cfg, f"{self.cfg['s3_prefix']}/results/receipt.json")

    def execute(self):
        try:
            self.results.mkdir(mode=0o700)
            def work_expired(signum, frame):
                raise TimeoutError('absolute work deadline reached; preserving export reserve')
            signal.signal(signal.SIGALRM, work_expired)
            signal.setitimer(signal.ITIMER_REAL, max(.001, remaining(self.deadline)))
            write_json(self.results / 'campaign-config.json', self.cfg)
            write_json(self.results / 'before.json', evidence(self.root))
            self.inventory()
            self.probe_export()
            self.parallel_nodes('five-host absolute TTL/NVMe admission', self.admit_node)
            self.progress('admitting-local-verified-inputs')
            self.inputs()
            self.run_matrix()
            if self.cfg.get('kv_suite') is not None:
                self.state['complete'] = False
                run_kv_suite(self)
                self.state['complete'] = True
        except BaseException as error:
            self.state['complete'] = False
            self.state['failures'].append(repr(error))
            if hasattr(self, 'cell') and not self.cell['complete']:
                self.cell['failure'] = repr(error)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            try:
                try:
                    self.stop_process(self.child)
                except BaseException as error:
                    self.state['complete'] = False
                    self.state['failures'].append('replay cleanup: ' + repr(error))
                # Attempt every host independently, including unknown starts.
                nodes = [n for n in self.nodes if n['partition'] in self.attempted_nodes and n['partition'] not in self.collected_final]
                try:
                    self.parallel_nodes('final node logs/export', lambda node: self.collect_node(node, final=True, logs=True), nodes)
                except BaseException as error:
                    self.state['complete'] = False
                    self.state['failures'].append('node cleanup/export: ' + repr(error))
                try:
                    self.progress('exporting-complete' if self.state['complete'] else 'exporting-partial')
                finally:
                    self.export()
            finally:
                # No disk write, export success, or benchmark completeness gates
                # this exact-ID/tag recheck and termination request.
                self.check_owned_instances(record=False)
                self.ec2.terminate_instances(InstanceIds=self.ids)
        if not self.state['complete']:
            raise RuntimeError('real checkpoint campaign incomplete; inspect exported failure evidence')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    modes.add_parser('schema')
    controller = modes.add_parser('controller')
    controller.add_argument('--config', type=Path, required=True)
    node = modes.add_parser('node')
    node.add_argument('--config', type=Path, required=True)
    node.add_argument('--action', choices=('start', 'live', 'stop', 'final', 'release'), required=True)
    args = parser.parse_args()
    if args.mode == 'schema':
        print(json.dumps(schema(), indent=2))
        return
    def interrupted(signum, frame):
        raise InterruptedError(f'checkpoint supervisor received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    if args.mode == 'controller':
        SuiCampaign(read_json(args.config)).execute()
    else:
        node_action(args.action, read_json(args.config))


if __name__ == '__main__':
    main()
