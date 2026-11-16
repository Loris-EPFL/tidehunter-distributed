#!/usr/bin/env python3
"""Real two-process software FI_READ correctness floor, not hardware performance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import transport_for_case, validate_transport
from resident_config import software_fabric_environment

PAYLOADS = (256, 8192)
SAMPLES = 1000
WARMUP = 64
PROBE_MS = 30000
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


def fabric_config_for_case(label, domain):
    transport = transport_for_case(label)
    transport['fabric']['domain'] = domain
    return validate_transport(transport)['fabric']


def write_fabric_config(output, domain, *, software=False):
    label = 'fabric-tcp' if software else 'efa-direct'
    document(Path(output).with_suffix('.fabric.json'), fabric_config_for_case(label, domain))


def command(binary, role, address, payload, output, *, software=False,
            samples=SAMPLES, warmup=WARMUP, deadline_ms=PROBE_MS):
    config_path = Path(output).with_suffix('.fabric.json')
    require_device_read = not software
    return [str(binary), '--rma-probe', '--role', role,
            '--listen' if role == 'server' else '--connect', address,
            '--fabric-config', str(config_path), '--require-device-read', str(require_device_read).lower(),
            '--bytes', str(payload), '--samples', str(samples), '--warmup', str(warmup),
            '--deadline-ms', str(deadline_ms), '--polling', 'busy',
            '--detailed-steps', '--output', str(output)]


def nonnegative(value, label):
    if type(value) is not int or value < 0:
        raise ValueError('invalid raw RMA integer: ' + label)
    return value


def validate_reports(client, server, expected, *, software=False):
    """Consume complete producer JSON; never synthesize CQ/RPC/RSS clocks."""
    for report in (client, server):
        if not isinstance(report, dict) or report.get('benchmark') != 'raw_provider_fi_read_floor':
            raise ValueError('raw RMA benchmark identity differs')
    payload, count, warmup = (expected[key] for key in ('payload', 'samples', 'warmup'))
    fabric = 'software-tcp' if software else 'efa-direct'
    provider = 'tcp;ofi_rxm' if software else 'efa'
    common = dict(benchmark='raw_provider_fi_read_floor', claim=CLAIM,
        topology='two_process_control_tcp_and_provider_fabric', control_security=SECURITY,
        fabric=fabric, payload_bytes=payload,
        accepted_fi_reads=count + warmup, samples_requested=count, warmup_requested=warmup,
        warmup_completed=warmup, clean_samples_completed=count,
        registered_region_bytes=payload + 24, deadline_ms=expected['deadline_ms'], polling='busy_spin')
    for role, report in (('client', client), ('server', server)):
        actual_config = expected.get(role + '_fabric_config')
        if not isinstance(actual_config, dict):
            raise ValueError(f'raw RMA {role} request metadata is incomplete')
        caps = report.get('capabilities')
        if not isinstance(caps, dict):
            raise ValueError(f'raw RMA {role} capability metadata missing')
        if (report.get('fabric_config') != actual_config
                or caps.get('require_device_read') is not expected['require_device_read']):
            raise ValueError(f'raw RMA {role} request metadata differs')
        provider_info = report.get('provider_info')
        if not isinstance(provider_info, dict) or not provider_info:
            raise ValueError(f'raw RMA {role} actual provider metadata missing')
        required_info = ('backend', 'provider', 'fabric', 'domain', 'source_address', 'device',
                         'driver', 'bus_address', 'rma_capabilities', 'capability_evidence')
        if any(key not in provider_info for key in required_info):
            raise ValueError(f'raw RMA {role} provider metadata is incomplete')
        fabric_config = actual_config
        if (provider_info['backend'] != fabric_config['backend']
                or (fabric_config.get('domain') is not None
                    and provider_info['domain'] != fabric_config['domain'])
                or (fabric_config.get('source_address') is not None
                    and provider_info['source_address'] != fabric_config['source_address'])):
            raise ValueError(f'raw RMA {role} actual provider selection differs')
        if (provider_info['provider'] != provider
                or not isinstance(provider_info['provider'], str) or not provider_info['provider']
                or not isinstance(provider_info['fabric'], str) or not provider_info['fabric']
                or not isinstance(provider_info['capability_evidence'], str)
                or not provider_info['capability_evidence']):
            raise ValueError(f'raw RMA {role} lacks requested provider/capability evidence')
        rma = provider_info['rma_capabilities']
        read_mode = rma.get('read_mode') if isinstance(rma, dict) else None
        if (read_mode not in ('native', 'emulated', 'unknown')
                or (expected['require_device_read'] and read_mode != 'native')
                or (software and read_mode == 'native')
                or nonnegative(rma.get('max_read_bytes'), 'provider_info.max_read_bytes') < payload + 24):
            raise ValueError(f'raw RMA {role} provider metadata does not attest the requested read policy')
        for key, value in dict(common, role=role).items():
            if report.get(key) != value or type(report.get(key)) is not type(value):
                raise ValueError(f'raw RMA {role} mismatched {key}')
        if not isinstance(report.get('cpu_affinity'), str) or not re.fullmatch(r'[0-9,-]+', report['cpu_affinity']):
            raise ValueError('missing actual raw RMA CPU affinity')
        required = dict(read_mode=read_mode, fabric_config=actual_config,
                        provider_info=provider_info)
        for key, value in required.items():
            if caps.get(key) != value or type(caps.get(key)) is not type(value):
                raise ValueError('raw RMA did not select the explicit provider capability: ' + key)
        if (caps.get('address_mode') not in ('virtual', 'offset') or
                nonnegative(caps.get('max_read_bytes'), 'max_read_bytes') < payload + 24):
            raise ValueError('raw RMA registration/read capability differs')
        if (caps.get('read_mode') != read_mode
                or caps.get('max_read_bytes') != rma.get('max_read_bytes')
                or caps.get('address_mode') != rma.get('address_mode')):
            raise ValueError('raw RMA capability summary disagrees with selected provider metadata')
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
    environment = software_fabric_environment(os.environ, software=True)
    output.mkdir(parents=True, exist_ok=False)
    binary_hash = digest(binary)
    results = []
    for payload in PAYLOADS:
        case = output / str(payload)
        case.mkdir()
        for name in ('server.json', 'client.json'):
            write_fabric_config(case / name, 'lo', software=True)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        address = '127.0.0.1:' + str(port)
        server_args = command(binary, 'server', address, payload, case / 'server.json',
            software=True, samples=samples, warmup=warmup, deadline_ms=deadline_ms)
        client_args = command(binary, 'client', address, payload, case / 'client.json',
            software=True, samples=samples, warmup=warmup, deadline_ms=deadline_ms)
        processes = []
        end = time.monotonic() + deadline_ms / 1000
        try:
            with (case / 'server.stdout.log').open('wb') as stdout, (case / 'server.stderr.log').open('wb') as stderr:
                server = subprocess.Popen(server_args, stdout=stdout, stderr=stderr, start_new_session=True, env=environment)
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
                    client = subprocess.Popen(client_args, stdout=client_stdout, stderr=client_stderr, start_new_session=True, env=environment)
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
                environment={key: value for key, value in environment.items() if key.startswith('FI_')},
                exits=[child.returncode for child in processes], cleanup_complete=all(
                    child.poll() is not None for child in processes)))
        client_fabric = fabric_config_for_case('fabric-tcp', 'lo')
        server_fabric = fabric_config_for_case('fabric-tcp', 'lo')
        expected = dict(payload=payload, samples=samples, warmup=warmup, deadline_ms=deadline_ms,
                        client_domain='lo', server_domain='lo', client_fabric_config=client_fabric,
                        server_fabric_config=server_fabric, require_device_read=False)
        results.append(validate_reports(document(case / 'client.json'), document(case / 'server.json'), expected, software=True))
    document(output / 'receipt.json', dict(complete=True, hardware=False, binary_sha256=binary_hash,
        environment={key: value for key, value in environment.items() if key.startswith('FI_')}, cases=results))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    local = sub.add_parser('local-software')
    local.add_argument('--binary', required=True, type=Path)
    local.add_argument('--output', required=True, type=Path)
    local.add_argument('--samples', type=int, default=SAMPLES)
    local.add_argument('--warmup', type=int, default=WARMUP)
    local.add_argument('--deadline-ms', type=int, default=PROBE_MS)
    args = parser.parse_args()
    local_software(args.binary, args.output, samples=args.samples, warmup=args.warmup, deadline_ms=args.deadline_ms)


if __name__ == '__main__':
    main()
