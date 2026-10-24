#!/usr/bin/env python3
"""Raw FI_READ provider floor inside the admitted KV fleet, never a WAL/RPC RTT.

The hardware entry point is run_registered_floor(suite). The guest subcommand is
SSM-only and owns one deterministic transient unit. Local software mode is an
explicit two-process correctness exercise, not hardware performance evidence.
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
import signal
import shutil
import socket
import subprocess
import tarfile
import time
import uuid

PAYLOADS = (256, 8192)
SAMPLES = 1000
WARMUP = 64
PROBE_MS = 30000
# Readiness, SSM dispatch, export, and exact-unit release must fit before the
# parent's work deadline, not merely before the fleet's hard poweroff deadline.
CASE_RESERVE = 180
CLAIM = 'provider_FI_READ_completion_and_client_API_validation_not_pure_wire_RTT'
SECURITY = 'bounded_trusted_test_tcp_no_authentication'


def document(path, value=None):
    path = Path(path)
    if value is None:
        return json.loads(path.read_text())
    temporary = path.with_suffix(path.suffix + '.pending')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def left(deadline):
    stamp = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
    if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
        raise ValueError('raw RMA requires an absolute UTC deadline')
    return stamp.timestamp() - time.time()


def budget(deadline, reserve, maximum):
    remaining = int(left(deadline) - reserve)
    if remaining < 1:
        raise TimeoutError('raw RMA lacks remaining campaign work/cleanup reserve')
    return min(maximum, remaining)


def client_deadline(work_deadline):
    # A wedged executable cannot consume the time needed for the finally stop,
    # even though it also has its own provider deadline. Never use the whole
    # supervisor work TTL as the child-process timeout.
    seconds = budget(work_deadline, 90, PROBE_MS // 1000 + 5)
    return datetime.fromtimestamp(time.time() + seconds, timezone.utc).isoformat()


def acquire_case_lock(lock, deadline):
    end = time.monotonic() + budget(deadline, 30, 30)
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= end:
                raise TimeoutError('raw RMA exact-case ownership lock deadline')
            time.sleep(.05)


def enabled(cfg):
    options = cfg.get('kv_suite') or {}
    resident = options.get('resident_reads')
    return bool(resident and any(resident.values()) and 'efa-native' in cfg['transports'])


def owned_port(cfg, payload_bytes):
    # Existing internal SG already permits fleet-to-fleet traffic. Do not change
    # ingress or reuse the normal KV 7700 or Sui's reserved namespace port range.
    # Each payload needs a fresh listener port; the prior case can leave TIME_WAIT.
    base = cfg.get('port', 24000)
    ports = [port for port in (17700, 27700, 37700, 47700, 57700)
             if not base <= port <= base + 615]
    return ports[PAYLOADS.index(payload_bytes)]


def command(binary, role, address, payload, domain, output, *, software=False,
            samples=SAMPLES, warmup=WARMUP, deadline_ms=PROBE_MS):
    return [str(binary), '--rma-probe', '--role', role,
            '--listen' if role == 'server' else '--connect', address,
            '--provider', 'tcp;ofi_rxm' if software else 'efa', '--domain', domain,
            '--software-tcp' if software else '--efa-direct', '--bytes', str(payload),
            '--samples', str(samples), '--warmup', str(warmup),
            '--deadline-ms', str(deadline_ms), '--polling', 'busy',
            '--detailed-steps', '--output', str(output)]


def nonnegative(value, label):
    if type(value) is not int or value < 0:
        raise ValueError('invalid raw RMA integer: ' + label)
    return value


def validate_reports(client, server, expected, *, software=False):
    """Consume complete producer JSON; never synthesize CQ/RPC/RSS clocks."""
    payload, count, warmup = (expected[key] for key in ('payload', 'samples', 'warmup'))
    fabric = 'tcp;ofi_rxm_explicit_software' if software else 'efa-direct'
    provider = 'tcp;ofi_rxm' if software else 'efa'
    common = dict(benchmark='raw_provider_fi_read_floor', claim=CLAIM,
        topology='two_process_control_tcp_and_provider_fabric', control_security=SECURITY,
        provider=provider, fabric=fabric, payload_bytes=payload,
        accepted_fi_reads=count + warmup, samples_requested=count, warmup_requested=warmup,
        warmup_completed=warmup, clean_samples_completed=count,
        registered_region_bytes=payload + 24, deadline_ms=expected['deadline_ms'], polling='busy_spin')
    for role, report in (('client', client), ('server', server)):
        for key, value in dict(common, role=role, domain=expected[role + '_domain']).items():
            if report.get(key) != value or type(report.get(key)) is not type(value):
                raise ValueError(f'raw RMA {role} mismatched {key}')
        if not isinstance(report.get('cpu_affinity'), str) or not re.fullmatch(r'[0-9,-]+', report['cpu_affinity']):
            raise ValueError('missing actual raw RMA CPU affinity')
        caps = report.get('capabilities', {})
        required = dict(provider=provider, domain=expected[role + '_domain'],
            read_mode='emulated' if software else 'native',
            efa_direct_requested=not software, software_provider_explicit=software)
        for key, value in required.items():
            if caps.get(key) != value or type(caps.get(key)) is not type(value):
                raise ValueError('raw RMA did not select the explicit provider capability: ' + key)
        if (caps.get('address_mode') not in ('virtual', 'offset') or
                nonnegative(caps.get('max_read_bytes'), 'max_read_bytes') < payload + 24):
            raise ValueError('raw RMA registration/read capability differs')
        details = report.get('detailed_steps_ns')
        if not isinstance(details, list) or not details:
            raise ValueError('raw RMA setup/drain evidence missing')
        steps = {}
        for row in details:
            if not isinstance(row, list) or len(row) != 2 or not isinstance(row[0], str) or row[0] in steps:
                raise ValueError('ambiguous raw RMA setup step')
            steps[row[0]] = nonnegative(row[1], row[0])
        needed = ('provider_source_setup_ns', 'drain_ack_retirement_ns') if role == 'server' else (
            'provider_endpoint_setup_ns', 'control_bootstrap_descriptor_ns', 'drain_ack_write_ns')
        if not set(needed) <= steps.keys():
            raise ValueError('raw RMA exact drain acknowledgement/retirement missing')
    if client['capabilities']['address_mode'] != server['capabilities']['address_mode']:
        raise ValueError('raw RMA peers disagree on MR addressing')
    if server.get('raw_samples') != []:
        raise ValueError('raw RMA target must not invent client samples')
    rows = client.get('raw_samples')
    if not isinstance(rows, list) or len(rows) != warmup + count:
        raise ValueError('raw RMA samples missing or excessive')
    cdfs = {}
    offset = 0
    for phase, size in (('warmup', warmup), ('clean', count)):
        population = rows[offset:offset + size]
        for index, row in enumerate(population):
            for key, value in dict(index=index, phase=phase, requested_payload_bytes=payload,
                                   actual_fi_read_bytes=payload + 24, valid=True).items():
                if row.get(key) != value or type(row.get(key)) is not type(value):
                    raise ValueError('raw RMA sample identity/payload validation differs')
            for key in ('post_would_block_retries', 'empty_completion_polls',
                        'completion_latency_ns', 'api_validation_latency_ns'):
                nonnegative(row.get(key), key)
            if row['api_validation_latency_ns'] < row['completion_latency_ns']:
                raise ValueError('raw RMA completion extends beyond API validation')
        cdfs[phase] = {key: sorted(row[key] for row in population)
                      for key in ('completion_latency_ns', 'api_validation_latency_ns')}
        offset += size
    return dict(claim=CLAIM, hardware=not software, payload_bytes=payload,
        actual_fi_read_bytes=payload + 24, setup_steps_ns={role: report['detailed_steps_ns']
        for role, report in (('client', client), ('server', server))}, latency_cdfs_ns=cdfs,
        drain_complete=True, scope='setup separate; warmup and clean distinct; no pure-wire, RPC, RSS or 5us claim')


def matching_pids(output):
    """Exact unique --output argument, including unknown-start acknowledgement."""
    result = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            args = (proc / 'cmdline').read_bytes().split(b'\0')
            if b'--output' in args and os.fsencode(str(output)) in args:
                result.append(int(proc.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return result


def listener_owned(pid, address):
    """Observe the listener without consuming the probe's single TCP accept."""
    ip, port = address.rsplit(':', 1)
    encoded = socket.inet_aton(ip)[::-1].hex().upper() + ':' + format(int(port), '04X')
    inodes = {line.split()[9] for line in Path('/proc/net/tcp').read_text().splitlines()[1:]
              if line.split()[1] == encoded and line.split()[3] == '0A'}
    for fd in Path(f'/proc/{pid}/fd').glob('*'):
        try:
            link = os.readlink(fd)
        except FileNotFoundError:
            continue
        if link.startswith('socket:[') and link[8:-1] in inodes:
            return True
    return False


def no_workload_units(run_id):
    patterns = [run_id + '-*.service', 'sui-storage-' + run_id + '-*.service',
                'raw-rma-' + run_id + '-*.service']
    result = subprocess.run(['systemctl', 'list-units', '--all', '--type=service',
        '--state=active,activating,deactivating', '--no-legend', '--plain', '--no-pager', *patterns],
        check=True, capture_output=True, text=True, timeout=5)
    if result.stdout.strip():
        raise RuntimeError('raw RMA cannot overlap owned normal/probe workload units: ' + result.stdout)


def select_domain(output):
    # fi_info's actual selected hardware domain is host-specific. Never assume
    # the controller and storage host share a domain name.
    from sui_checkpoint import fabric_preflight
    fabric_preflight(output, 'efa-native')
    domains = re.findall(r'^\s*domain:\s*(\S+)\s*$', (output / 'efa-provider.txt').read_text(), re.M)
    if not domains or any(not re.fullmatch(r'[A-Za-z0-9_.:-]+', domain) for domain in domains):
        raise ValueError('EFA direct preflight has no explicit hardware domain')
    return domains[0]


def validate_payload(payload):
    cfg = payload['config']
    if not re.fullmatch(r'th-[0-9a-f]{16}', cfg['run_id']):
        raise ValueError('raw RMA run ownership differs')
    if cfg['run_root'] != '/mnt/tidehunter/' + cfg['run_id']:
        raise ValueError('raw RMA root differs from original owned fleet namespace')
    if not re.fullmatch(r'raw-rma-[0-9a-f]{16}-v(?:256|8192)', payload['case_id']):
        raise ValueError('raw RMA requires a fresh exact case namespace')
    if payload['payload'] not in PAYLOADS or not payload['case_id'].endswith('-v' + str(payload['payload'])):
        raise ValueError('raw RMA payload/case binding differs')
    if payload['port'] != owned_port(cfg, payload['payload']) or payload['port'] == 7700:
        raise ValueError('raw RMA port is not the explicit unused owned port')
    if (payload['deadline_ms'] != PROBE_MS or payload['samples'] != SAMPLES or
            payload['warmup'] != WARMUP or payload['software'] is not False):
        raise ValueError('hardware campaign raw RMA policy differs')
    left(cfg['deadline_utc'])
    left(payload['work_deadline'])
    hard = datetime.fromisoformat(cfg['deadline_utc'].replace('Z', '+00:00')).timestamp()
    work = datetime.fromisoformat(payload['work_deadline'].replace('Z', '+00:00')).timestamp()
    if work > hard - cfg.get('export_reserve_seconds', 600):
        raise ValueError('raw RMA work exceeds original export reserve')
    if not re.fullmatch(r'[0-9a-f]{64}', payload['binary_sha256']) or not re.fullmatch(
            r'[0-9a-f]{64}', cfg['source_archive_sha256']):
        raise ValueError('raw RMA source/binary identity missing')
    if payload['binary_sha256'] != cfg['binaries']['transport-floor']:
        raise ValueError('raw RMA binary differs from admitted six-binary bundle')
    node = next(n for n in cfg['nodes'] if n['instance_id'] == payload['instance_id'])
    source_case = payload['case_id'].rsplit('-v', 1)[0] + '-binary'
    source_key = f"{cfg['s3_prefix']}/case-artifacts/{source_case}/node-{node['partition']}.tar.gz"
    if (payload['artifact']['key'] != source_key or
            not re.fullmatch(r'[0-9a-f]{64}', payload['artifact']['sha256'])):
        raise ValueError('raw RMA binary download is outside scoped admitted input')
    return node


def result_key(payload):
    node = next(n for n in payload['config']['nodes'] if n['instance_id'] == payload['instance_id'])
    return (f"{payload['config']['s3_prefix']}/results/nodes/{payload['case_id']}/"
            f"node-{node['partition']}.tar.gz")


def stop_owned(unit, output, deadline):
    """Stop/reap even when start's SSM reply or systemd-run ack was lost."""
    from guest import wait_node_exit
    pids = matching_pids(output)
    result = subprocess.run(['systemctl', 'stop', unit], capture_output=True, text=True,
                            timeout=budget(deadline, 5, 15))
    # A unit that never started may already be absent. A bus failure is not
    # absence: wait_node_exit requires a successful empty list-units query.
    for pid in pids or [0]:
        wait_node_exit(unit, pid, timeout=budget(deadline, 2, 15))
    if matching_pids(output):
        raise RuntimeError('raw RMA owned process survives exact-unit cleanup')
    return dict(cleanup_complete=True, unit=unit, observed_pids=pids,
                stop_exit_code=result.returncode, no_owned_processes=True, unit_collected=True)


def guest_action(payload):
    from guest import SDK_CONFIG, download, upload, write_json
    from sui_checkpoint import bootstrap_gate
    import boto3
    node = validate_payload(payload)
    cfg = payload['config']
    root = Path('/mnt/tidehunter') / cfg['run_id']
    bootstrap_gate(root, cfg, node['instance_id'])
    control = root / 'raw-rma-control'
    control.mkdir(exist_ok=True)
    case = control / payload['case_id']
    unit = 'raw-rma-' + cfg['run_id'] + '-' + payload['case_id']
    output = case / 'server.json'
    session = boto3.Session(region_name=cfg['region'])
    s3 = session.client('s3', config=SDK_CONFIG)
    stopped = control / (payload['case_id'] + '.stopped')
    with (control / (payload['case_id'] + '.lock')).open('a') as lock:
        acquire_case_lock(lock, payload['work_deadline'])
        if payload['action'] == 'start':
            budget(payload['work_deadline'], CASE_RESERVE, 1)
            if stopped.exists() or case.exists():
                raise ValueError('raw RMA namespace already started or terminally stopped; never retry unknown start')
            no_workload_units(cfg['run_id'])
            case.mkdir(mode=0o700)
            document(case / 'intent.json', payload)
            binary = case / 'transport-floor'
            staged = case / 'input.tar.gz'
            download(s3, cfg, payload['artifact']['key'], payload['artifact']['sha256'], staged)
            with tarfile.open(staged) as source:
                members = source.getmembers()
                if (len(members) != 1 or members[0].name != 'transport-floor' or
                        not members[0].isfile() or members[0].size > 256 * 1024 * 1024):
                    raise ValueError('raw RMA input is not the exact admitted single executable')
                with source.extractfile(members[0]) as incoming, binary.open('xb') as target:
                    shutil.copyfileobj(incoming, target)
            if digest(binary) != payload['binary_sha256']:
                raise ValueError('raw RMA target binary differs from source-bound bundle')
            binary.chmod(0o700)
            domain = select_domain(case)
            address = node['private_ip'] + ':' + str(payload['port'])
            with socket.socket() as reservation:
                reservation.bind((node['private_ip'], payload['port']))
            args = command(binary, 'server', address, payload['payload'], domain, output)
            document(case / 'command.json', dict(command=args, binary_sha256=digest(binary)))
            subprocess.run(['systemd-run', '--quiet', '--collect', '--unit=' + unit,
                '--property=Type=exec', '--property=RemainAfterExit=yes',
                '--property=RuntimeMaxSec=' + str(budget(payload['work_deadline'], 30, 90)),
                '--property=LimitMEMLOCK=infinity', '--property=TimeoutStopSec=10',
                '--property=KillMode=control-group', '--setenv=FI_EFA_USE_DEVICE_RDMA=1',
                '--setenv=FI_EFA_ENABLE_SHM_TRANSFER=0',
                '--property=StandardOutput=append:' + str(case / 'server.stdout.log'),
                '--property=StandardError=append:' + str(case / 'server.stderr.log'), *args],
                check=True, timeout=budget(payload['work_deadline'], 60, 15))
            end = time.monotonic() + budget(payload['work_deadline'], 60, 15)
            while time.monotonic() < end:
                pid_text = subprocess.check_output(['systemctl', 'show', unit, '--property=MainPID', '--value'],
                                                   text=True, timeout=5).strip()
                pid = int(pid_text or 0)
                if pid and listener_owned(pid, address):
                    if digest(Path(f'/proc/{pid}/exe')) != payload['binary_sha256']:
                        raise ValueError('raw RMA ready executable differs from admitted build')
                    ready = dict(case_id=payload['case_id'], run_id=cfg['run_id'],
                        instance_id=node['instance_id'], binary_sha256=payload['binary_sha256'],
                        source_archive_sha256=cfg['source_archive_sha256'], domain=domain, pid=pid,
                        address=address, unit=unit, ready=True,
                        readiness='owned live executable and TCP listener; no consuming readiness connection')
                    write_json(case / 'ready.json', ready)
                    upload(s3, case / 'ready.json', cfg, result_key(payload) + '.ready.json')
                    return
                subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True, timeout=5)
                time.sleep(.05)
            raise TimeoutError('raw RMA owned TCP listener did not become ready')
        if payload['action'] != 'stop':
            raise ValueError('unknown raw RMA guest action')
        # Persist BEFORE stopping. Delayed/unknown start dispatches acquire this
        # same lock and cannot resurrect a namespace after acknowledged cleanup.
        stopped.touch(exist_ok=True)
        case.mkdir(mode=0o700, exist_ok=True)
        cleanup_deadline = payload['work_deadline']
        # The client writes its drain ACK immediately before exiting. Allow the
        # target to consume it, retire the registration, publish JSON and exit
        # naturally before stopping its retained (RemainAfterExit) unit.
        from sui_checkpoint import systemd_status
        end = time.monotonic() + budget(cleanup_deadline, 30, 5)
        status = systemd_status(unit)
        while status.get('MainPID', '0') != '0' and time.monotonic() < end:
            time.sleep(.05)
            status = systemd_status(unit)
        cleanup = stop_owned(unit, output, cleanup_deadline)
        cleanup['terminal_status'] = status
        document(case / 'cleanup.json', cleanup)
        archive = control / (payload['case_id'] + '.tar.gz')
        with tarfile.open(archive, 'w:gz') as stream:
            for path in sorted(case.iterdir()):
                if path.is_file() and path.name not in ('transport-floor', 'input.tar.gz'):
                    stream.add(path, arcname=path.name, recursive=False)
        receipt = upload(s3, archive, cfg, result_key(payload))
        upload_path = control / (payload['case_id'] + '.receipt.json')
        document(upload_path, receipt)
        upload(s3, upload_path, cfg, result_key(payload) + '.receipt.json')


def s3_json(s3, cfg, key):
    with s3.get_object(Bucket=cfg['bucket'], Key=key, ExpectedBucketOwner=cfg['account'])['Body'] as body:
        data = body.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('raw RMA S3 JSON exceeds bounded size')
    return json.loads(data)


def archive_documents(path, required=None):
    result = {}
    with tarfile.open(path) as stream:
        names = set()
        for member in stream.getmembers():
            if (not member.isfile() or '/' in member.name or member.name in names or
                    member.size > 16 * 1024 * 1024):
                raise ValueError('raw RMA target archive has foreign/duplicate/unbounded entries')
            names.add(member.name)
            if member.name.endswith('.json'):
                with stream.extractfile(member) as body:
                    result[member.name] = json.load(body)
    if required is None:
        required = {'intent.json', 'ready.json', 'command.json', 'cleanup.json', 'server.json',
                    'server.stdout.log', 'server.stderr.log', 'efa-provider.txt'}
    if not required <= names:
        raise ValueError('raw RMA peer evidence is incomplete')
    return result


def validate_ready(ready, payload):
    node = validate_payload(payload)
    expected = dict(case_id=payload['case_id'], run_id=payload['config']['run_id'],
        instance_id=node['instance_id'], binary_sha256=payload['binary_sha256'],
        source_archive_sha256=payload['config']['source_archive_sha256'], ready=True,
        address=node['private_ip'] + ':' + str(payload['port']),
        unit='raw-rma-' + payload['config']['run_id'] + '-' + payload['case_id'])
    for key, value in expected.items():
        if ready.get(key) != value or type(ready.get(key)) is not type(value):
            raise ValueError('raw RMA readiness provenance differs: ' + key)
    if not isinstance(ready.get('domain'), str) or not ready['domain'] or nonnegative(ready.get('pid'), 'ready PID') == 0:
        raise ValueError('raw RMA live readiness identity missing')


def dispatch(suite, node, payload, action):
    encoded = base64.b64encode(json.dumps(dict(payload, action=action)).encode()).decode()
    script = suite.root / 'runner/raw_rma_floor.py'
    args = ['python3', str(script), 'guest', '--payload-base64', encoded]
    # Reuse the real exact-instance SSM bounded/unknown-ack handling. Cleanup
    # still uses the work deadline, preserving the outer fleet export reserve.
    budget(suite.deadline, 15, 1)
    suite.parent.ssm_one(node, payload['case_id'] + '-' + action, shlex.join(args),
                         timeout=budget(suite.deadline, 15, 45), cleanup=False)


def validate_cleanup(cleanup, unit):
    if (cleanup.get('cleanup_complete') is not True or cleanup.get('unit_collected') is not True or
            cleanup.get('no_owned_processes') is not True or cleanup.get('unit') != unit):
        raise ValueError('raw RMA target process cleanup is not proven')
    status = cleanup.get('terminal_status', {})
    if (status.get('MainPID') != '0' or status.get('ExecMainCode') != '1' or
            status.get('ExecMainStatus') != '0' or status.get('Result') != 'success'):
        raise ValueError('raw RMA target did not exit naturally after exact drain')


def validate_case(case, payload, client_domain):
    """Offline consumer shared with campaign admission; original paths stay bound."""
    cfg, case_id = payload['config'], payload['case_id']
    validate_payload(payload)
    captured = archive_documents(case / 'server.tar.gz')
    ready = document(case / 'ready.json')
    validate_ready(ready, payload)
    if captured['ready.json'] != ready or captured['intent.json'] != dict(payload, action='start'):
        raise ValueError('raw RMA target source/config/readiness differs')
    validate_cleanup(captured['cleanup.json'], ready['unit'])
    receipt = document(case / 'server-receipt.json')
    if (receipt.get('key') != result_key(payload) or
            receipt.get('sha256') != digest(case / 'server.tar.gz')):
        raise ValueError('raw RMA target retrieved S3 archive provenance differs')
    remote_case = Path('/mnt/tidehunter') / cfg['run_id'] / 'raw-rma-control' / case_id
    server_args = command(remote_case / 'transport-floor', 'server', ready['address'],
        payload['payload'], ready['domain'], remote_case / 'server.json')
    if captured['command.json'] != dict(command=server_args, binary_sha256=payload['binary_sha256']):
        raise ValueError('raw RMA target executable/command provenance differs')
    root = Path(cfg['run_root'])
    client_case = root / 'checkpoint-results/kv/raw-rma' / case_id
    client_args = command(root / 'checkpoint-inputs/build_bundle/binaries/transport-floor',
        'client', ready['address'], payload['payload'], client_domain, client_case / 'client.json')
    client_receipt = document(case / 'client-receipt.json')
    if (client_receipt.get('key') != f"{cfg['s3_prefix']}/case-artifacts/{case_id}/client-results.tar.gz" or
            client_receipt.get('sha256') != digest(case / 'client-retrieved.tar.gz')):
        raise ValueError('raw RMA client retrieved S3 archive provenance differs')
    client = archive_documents(case / 'client-retrieved.tar.gz', required={
        'client.json', 'client-process.command.json', 'client-process.stdout.log', 'client-process.stderr.log'})
    if (client['client.json'] != document(case / 'client.json') or
            client['client-process.command.json'] != document(case / 'client-process.command.json')):
        raise ValueError('raw RMA client retrieved S3 evidence differs')
    process = client['client-process.command.json']
    if (process.get('exit_code') != 0 or type(process.get('exit_code')) is not int or
            process.get('binary_sha256') != payload['binary_sha256'] or process.get('command') != client_args):
        raise ValueError('raw RMA client command/exit provenance differs')
    expected = dict(payload=payload['payload'], samples=payload['samples'], warmup=payload['warmup'],
        deadline_ms=payload['deadline_ms'], client_domain=client_domain, server_domain=ready['domain'])
    return expected, validate_reports(client['client.json'], captured['server.json'], expected)


def validate_receipt(root, state, cfg):
    """Fail closed on missing/foreign raw-floor populations in a registered run."""
    observed = state.get('raw_rma_floor')
    if not enabled(cfg):
        if observed is not None:
            raise ValueError('unexpected raw RMA population in ordinary TCP/TLS/RPC controls')
        return None
    folder = Path(root) / 'raw-rma'
    receipt = document(folder / 'receipt.json')
    if receipt != observed or receipt.get('complete') is not True or receipt.get('scope') != CLAIM:
        raise ValueError('registered profile lacks complete exact raw RMA floor receipt')
    if (receipt.get('source_archive_sha256') != cfg['source_archive_sha256'] or
            receipt.get('binary_sha256') != cfg['binaries']['transport-floor']):
        raise ValueError('raw RMA receipt is from another source/binary build')
    boot = document(folder / 'controller-bootstrap.json')
    identity = boot.get('identity', {})
    if (boot.get('run_id') != cfg['run_id'] or identity.get('instanceId') != cfg['coordinator_instance_id'] or
            identity.get('accountId') != cfg['account'] or identity.get('region') != cfg['region']):
        raise ValueError('raw RMA actual controller identity differs from existing fleet')
    records = receipt.get('cases', [])
    if len(records) != len(PAYLOADS) or len({row.get('case_id') for row in records}) != len(PAYLOADS):
        raise ValueError('raw RMA payload populations missing or duplicated')
    immutable = ('run_id', 'run_root', 'account', 'region', 'nodes', 'coordinator_instance_id',
        'binaries', 'source_archive_sha256', 'build_manifest_sha256', 'source_manifest_sha256',
        'deadline_utc', 'export_reserve_seconds', 's3_prefix', 'kv_suite', 'transports')
    target = min(cfg['nodes'], key=lambda item: item['partition'])
    for size, record in zip(PAYLOADS, records, strict=True):
        case_id = record['case_id']
        if not re.fullmatch(r'raw-rma-[0-9a-f]{16}-v' + str(size), case_id):
            raise ValueError('raw RMA payload/case order differs')
        case = folder / case_id
        payload = document(case / 'intent.json')
        if (payload.get('case_id') != case_id or payload.get('payload') != size or
                any(payload['config'].get(key) != cfg.get(key) for key in immutable)):
            raise ValueError('raw RMA original campaign identity differs')
        expected, result = validate_case(case, payload, record['expected']['client_domain'])
        admitted = dict(case_id=case_id, complete=True, cleanup_complete=True, expected=expected, result=result,
            controller_instance_id=cfg['coordinator_instance_id'], target_instance_id=target['instance_id'])
        if record != admitted or document(case / 'validated.json') != admitted:
            raise ValueError('raw RMA summary differs from actual client/server samples')
    return receipt


def export_client(suite, case, payload):
    """Use existing case-artifact scope, then retrieve/hash the actual log bundle."""
    from guest import upload, download
    archive = case / 'client-export.tar.gz'
    excluded = {'server.tar.gz', 'client-export.tar.gz', 'client-retrieved.tar.gz', 'client-receipt.json'}
    with tarfile.open(archive, 'w:gz') as stream:
        for path in sorted(case.iterdir()):
            if path.is_file() and path.name not in excluded:
                stream.add(path, arcname=path.name, recursive=False)
    cfg = payload['config']
    receipt = upload(suite.s3, archive, cfg,
        f"{cfg['s3_prefix']}/case-artifacts/{payload['case_id']}/client-results.tar.gz")
    document(case / 'client-receipt.json', receipt)
    download(suite.s3, cfg, receipt['key'], receipt['sha256'], case / 'client-retrieved.tar.gz')


def run_registered_floor(suite):
    """One serial hardware floor pair; called after install, before any KV case."""
    if not enabled(suite.parent.cfg):
        return
    from guest import download, run_recorded, upload
    from sui_checkpoint import bootstrap_gate
    cfg = suite.parent.cfg
    if suite.active_payload is not None:
        raise RuntimeError('normal KV node case still owns the fleet')
    output = suite.results / 'raw-rma'
    output.mkdir(exist_ok=False)
    document(output / 'controller-bootstrap.json',
             bootstrap_gate(suite.root, cfg, cfg['coordinator_instance_id']))
    state = dict(complete=False, cases=[], source_archive_sha256=cfg['source_archive_sha256'],
                 binary_sha256=cfg['binaries']['transport-floor'], scope=CLAIM)
    suite.state['raw_rma_floor'] = state
    binary = suite.parent.build / 'binaries/transport-floor'
    if digest(binary) != state['binary_sha256']:
        raise ValueError('raw RMA executable differs from admitted rebuilt bundle')
    node = min(cfg['nodes'], key=lambda item: item['partition'])
    with (suite.root / '.raw-rma-floor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        no_workload_units(cfg['run_id'])
        budget(suite.deadline, 2 * CASE_RESERVE + 30, 1)
        token = uuid.uuid4().hex[:16]
        staged = output / 'binary.tar.gz'
        with tarfile.open(staged, 'w:gz') as archive:
            archive.add(binary, arcname='transport-floor', recursive=False)
        artifact = upload(suite.s3, staged, cfg,
            f"{cfg['s3_prefix']}/case-artifacts/raw-rma-{token}-binary/node-{node['partition']}.tar.gz")
        try:
            for payload_bytes in PAYLOADS:
                budget(suite.deadline, CASE_RESERVE, 1)
                case_id = f'raw-rma-{token}-v{payload_bytes}'
                case = output / case_id
                case.mkdir()
                client_domain = select_domain(case)
                payload = dict(config=cfg, case_id=case_id, instance_id=node['instance_id'],
                    binary_sha256=state['binary_sha256'], artifact=artifact,
                    payload=payload_bytes, samples=SAMPLES, warmup=WARMUP,
                    deadline_ms=PROBE_MS, software=False, port=owned_port(cfg, payload_bytes), work_deadline=suite.deadline)
                document(case / 'intent.json', payload)
                prefix = result_key(payload)
                record = dict(case_id=case_id, complete=False)
                state['cases'].append(record)
                try:
                    # From this point a lost ACK has unknown ownership. Always
                    # stop this exact namespace in finally, never relaunch it.
                    dispatch(suite, node, payload, 'start')
                    ready = s3_json(suite.s3, cfg, prefix + '.ready.json')
                    validate_ready(ready, payload)
                    document(case / 'ready.json', ready)
                    args = command(binary, 'client', ready['address'], payload_bytes,
                                   client_domain, case / 'client.json')
                    run_recorded(args, case / 'client-process', client_deadline(suite.deadline),
                        env=os.environ | dict(FI_EFA_USE_DEVICE_RDMA='1', FI_EFA_ENABLE_SHM_TRANSFER='0'),
                        expected_binary_sha256=state['binary_sha256'])
                finally:
                    try:
                        dispatch(suite, node, payload, 'stop')
                        receipt = s3_json(suite.s3, cfg, prefix + '.receipt.json')
                        if receipt['key'] != prefix:
                            raise ValueError('raw RMA target export escaped owned case')
                        download(suite.s3, cfg, receipt['key'], receipt['sha256'], case / 'server.tar.gz')
                        document(case / 'server-receipt.json', receipt)
                    finally:
                        # Preserve failure logs too. Export cannot bypass the
                        # exact stop attempt or turn an unknown ACK into success.
                        export_client(suite, case, payload)
                if matching_pids(case / 'client.json'):
                    raise ValueError('raw RMA client process survives reaping')
                expected, result = validate_case(case, payload, client_domain)
                record.update(complete=True, cleanup_complete=True, expected=expected, result=result,
                    controller_instance_id=cfg['coordinator_instance_id'], target_instance_id=node['instance_id'])
                document(case / 'validated.json', record)
                no_workload_units(cfg['run_id'])
            state['complete'] = True
        finally:
            document(output / 'receipt.json', state)


def reap(child):
    if child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            child.wait(timeout=2)
            return
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=2)
    else:
        child.wait()


def local_software(binary, output, *, samples=SAMPLES, warmup=WARMUP, deadline_ms=PROBE_MS):
    """Run the REAL existing software FI_READ executable; no AWS/systemd/mock."""
    binary, output = Path(binary).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary_hash = digest(binary)
    results = []
    for payload in PAYLOADS:
        case = output / str(payload)
        case.mkdir()
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        address = '127.0.0.1:' + str(port)
        server_args = command(binary, 'server', address, payload, 'lo', case / 'server.json',
            software=True, samples=samples, warmup=warmup, deadline_ms=deadline_ms)
        client_args = command(binary, 'client', address, payload, 'lo', case / 'client.json',
            software=True, samples=samples, warmup=warmup, deadline_ms=deadline_ms)
        processes = []
        end = time.monotonic() + deadline_ms / 1000
        try:
            with (case / 'server.stdout.log').open('wb') as stdout, (case / 'server.stderr.log').open('wb') as stderr:
                server = subprocess.Popen(server_args, stdout=stdout, stderr=stderr, start_new_session=True)
                processes.append(server)
                while not listener_owned(server.pid, address):
                    if server.poll() is not None:
                        raise RuntimeError('real software FI_READ target exited before readiness')
                    if time.monotonic() >= end:
                        raise TimeoutError('real software FI_READ readiness deadline')
                    time.sleep(.01)
                if digest(Path(f'/proc/{server.pid}/exe')) != binary_hash:
                    raise ValueError('local raw RMA target executable identity differs')
                with (case / 'client.stdout.log').open('wb') as client_stdout, (case / 'client.stderr.log').open('wb') as client_stderr:
                    client = subprocess.Popen(client_args, stdout=client_stdout, stderr=client_stderr, start_new_session=True)
                    processes.append(client)
                    if client.wait(timeout=max(.001, end - time.monotonic())) != 0:
                        raise RuntimeError('real software FI_READ client failed; inspect stderr')
                if server.wait(timeout=max(.001, end - time.monotonic())) != 0:
                    raise RuntimeError('real software FI_READ target failed exact drain')
        finally:
            for child in reversed(processes):
                reap(child)
            document(case / 'processes.json', dict(binary_sha256=binary_hash,
                server_command=server_args, client_command=client_args,
                exits=[child.returncode for child in processes], cleanup_complete=all(
                    child.poll() is not None for child in processes)))
        expected = dict(payload=payload, samples=samples, warmup=warmup, deadline_ms=deadline_ms,
                        client_domain='lo', server_domain='lo')
        results.append(validate_reports(document(case / 'client.json'), document(case / 'server.json'), expected, software=True))
    document(output / 'receipt.json', dict(complete=True, hardware=False, binary_sha256=binary_hash, cases=results))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    guest = sub.add_parser('guest')
    guest.add_argument('--payload-base64', required=True)
    local = sub.add_parser('local-software')
    local.add_argument('--binary', required=True, type=Path)
    local.add_argument('--output', required=True, type=Path)
    local.add_argument('--samples', type=int, default=SAMPLES)
    local.add_argument('--warmup', type=int, default=WARMUP)
    local.add_argument('--deadline-ms', type=int, default=PROBE_MS)
    args = parser.parse_args()
    if args.action == 'guest':
        guest_action(json.loads(base64.b64decode(args.payload_base64, validate=True)))
    else:
        local_software(args.binary, args.output, samples=args.samples, warmup=args.warmup, deadline_ms=args.deadline_ms)


if __name__ == '__main__':
    main()
