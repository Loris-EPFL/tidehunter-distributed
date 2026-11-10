#!/usr/bin/env python3
"""Plan native Valkey reference experiments; qualify isolated local TCP only.

No cloud operations. Full plans retain ten million requests per test. The local
qualifier is explicitly not a performance run and owns every server it stops.
"""
import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import socket
import subprocess
import time

CONFIG = Path(__file__).with_name('valkey-public-reproduction.json')


def load():
    spec = json.loads(CONFIG.read_text())
    for field in ('clients', 'threads', 'requests_per_test', 'value_bytes', 'pipeline',
                  'keepalive', 'case_deadline_seconds', 'qualification_requests', 'qualification_keys'):
        if type(spec[field]) is not int or not 1 <= spec[field] <= 100_000_000:
            raise ValueError('invalid positive bounded parameter: ' + field)
    if spec['schema_version'] != 1 or spec['case_deadline_seconds'] > 3600:
        raise ValueError('unsupported schema or unbounded case deadline')
    return spec


def plan(spec, source, profile, host, ports, directory, transports, qualification=False,
         *, rdma_host=None, binary_directory=None):
    address = ipaddress.ip_address(host)
    rdma_host = host if rdma_host is None else rdma_host
    rdma_address = ipaddress.ip_address(rdma_host)
    binaries = source / 'src' if binary_directory is None else Path(binary_directory)
    if address.is_unspecified or address.is_multicast or not (address.is_private or address.is_loopback):
        raise ValueError('use an explicit private or loopback address')
    if rdma_address.is_unspecified or rdma_address.is_multicast or not (rdma_address.is_private or rdma_address.is_loopback):
        raise ValueError('use an explicit private RDMA address')
    if (len(set(transports)) != len(transports) or not transports
            or set(transports) - {'tcp', 'rdma'} or len(set(ports.values())) != 2
            or any(type(port) is not int or not 1 <= port <= 65535 for port in ports.values())):
        raise ValueError('invalid distinct transports or ports')
    selected = spec['profiles'][profile]
    requests = spec['qualification_requests'] if qualification else spec['requests_per_test']
    server = [str(binaries / 'valkey-server'), '--bind', host, '--port', str(ports['tcp']),
              '--protected-mode', 'no', '--daemonize', 'no', '--appendonly', 'no',
              '--maxmemory', spec['maxmemory'], '--maxmemory-policy', spec['maxmemory_policy'],
              '--save', selected['save']]
    if 'rdma' in transports:
        server += ['--rdma-bind', rdma_host, '--rdma-port', str(ports['rdma'])]
    cases = []
    bound = [] if qualification else ['timeout', '--signal=TERM', '--kill-after=10s',
                                      str(spec['case_deadline_seconds']) + 's']
    server_bound = [] if qualification else ['timeout', '--signal=TERM', '--kill-after=10s',
                                             str(2 * spec['case_deadline_seconds'] + 30) + 's']
    for transport in transports:
        keys = selected['key_ranges'][transport]
        if qualification and selected['prefill']:
            keys = spec['qualification_keys']
        common = [str(binaries / 'valkey-benchmark'), '-h', rdma_host if transport == 'rdma' else host, '-p', str(ports[transport]),
                  '-c', str(spec['clients']), '--threads', str(spec['threads']),
                  '-d', str(spec['value_bytes']), '-k', str(spec['keepalive']),
                  '-P', str(spec['pipeline']), '-r', str(keys)]
        if selected['seed'] is not None:
            common += ['--seed', str(selected['seed'])]
        if transport == 'rdma':
            common += ['--rdma']
        cases.append({'transport': transport, 'key_range': keys,
                      'server': server_bound + server + ['--dir', str(directory / transport)],
                      'prefill': bound + common + ['--sequential', '-n', str(keys), '-t', 'set']
                                 if selected['prefill'] else None,
                      'measurement': bound + common + ['-n', str(requests), '-t', spec['tests']]})
    return {'schema_version': 1, 'profile': profile, 'qualification_only': qualification,
            'interpretation': selected['interpretation'], 'reference': spec['reference'],
            'selected_version': spec['selected_version'], 'source_commit': spec['source_commit'],
            'requests_per_test': requests, 'deadline_seconds': 90 if qualification else spec['case_deadline_seconds'],
            'deadline_scope': 'each full measurement or prefill command; entire local qualification is bounded to 90 seconds',
            'server_deadline_seconds': 90 if qualification else 2 * spec['case_deadline_seconds'] + 30,
            'cases': cases, 'limits': spec['limits'],
            'lifecycle': 'fresh dedicated server/data directory per transport; never use an existing database'}


def invoke(argv, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('local qualification deadline expired')
    return subprocess.check_output(argv, text=True, stderr=subprocess.STDOUT, timeout=remaining)


def fields(text):
    return dict(line.split(':', 1) for line in text.splitlines() if ':' in line and not line.startswith('#'))


def qualify(spec, source, output, port):
    source = source.resolve(strict=True)
    observed = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True, timeout=10).strip()
    if observed != spec['source_commit']:
        raise ValueError('native checkout is not the pinned Valkey commit')
    tools = {name: source / 'src' / name for name in ('valkey-server', 'valkey-cli', 'valkey-benchmark')}
    identities = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in tools.items()}
    output.mkdir(parents=True, exist_ok=False)
    report = {'qualification_only': True, 'hardware_performance_evidence': False,
              'source_commit': observed, 'binary_sha256': identities, 'profiles': []}
    deadline = time.monotonic() + 90
    for profile in spec['profiles']:
        directory = output / profile
        directory.mkdir()
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', port))
        recipe = plan(spec, source, profile, '127.0.0.1', {'tcp': port, 'rdma': port + 1},
                      directory, ['tcp'], qualification=True)
        (directory / 'plan.json').write_text(json.dumps(recipe, indent=2))
        cli = [str(tools['valkey-cli']), '-h', '127.0.0.1', '-p', str(port), '--raw']
        process = None
        entry = {'profile': profile, 'complete': False}
        report['profiles'].append(entry)
        try:
            (directory / 'tcp').mkdir()
            with (directory / 'server.log').open('w') as log:
                process = subprocess.Popen(recipe['cases'][0]['server'], stdout=log, stderr=subprocess.STDOUT)
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError('owned Valkey server exited before readiness')
                    try:
                        info = fields(invoke(cli + ['INFO', 'server'], min(deadline, time.monotonic() + 1)))
                        if int(info.get('process_id', -1)) == process.pid:
                            break
                    except (subprocess.SubprocessError, ValueError):
                        pass
                    time.sleep(.05)
                else:
                    raise TimeoutError('owned Valkey server readiness expired')
                if process.poll() is not None:
                    raise RuntimeError('owned server exited during readiness')
                raw = invoke(cli + ['CONFIG', 'GET', 'appendonly', 'save', 'maxmemory', 'maxmemory-policy', 'cluster-enabled'], deadline)
                lines = raw.splitlines()
                settings = dict(zip(lines[::2], lines[1::2]))
                expected = {'appendonly': 'no', 'save': spec['profiles'][profile]['save'],
                            'maxmemory': str(20 * 1024**3), 'maxmemory-policy': spec['maxmemory_policy'],
                            'cluster-enabled': 'no'}
                if settings != expected or int(invoke(cli + ['DBSIZE'], deadline)) != 0:
                    raise RuntimeError('fresh standalone server configuration differs')
                case = recipe['cases'][0]
                if case['prefill']:
                    (directory / 'prefill.log').write_text(invoke(case['prefill'], deadline))
                    if int(invoke(cli + ['DBSIZE'], deadline)) != case['key_range']:
                        raise RuntimeError('sequential prefill did not populate the full key range')
                before = fields(invoke(cli + ['INFO', 'stats'], deadline))
                (directory / 'benchmark.log').write_text(invoke(case['measurement'], deadline))
                after = fields(invoke(cli + ['INFO', 'stats'], deadline))
                delta = {key: int(after[key]) - int(before[key]) for key in
                         ('keyspace_hits', 'keyspace_misses', 'evicted_keys', 'total_error_replies')}
                if delta['total_error_replies'] or delta['evicted_keys']:
                    raise RuntimeError('benchmark produced errors or evictions')
                if case['prefill'] and (delta['keyspace_misses'] or delta['keyspace_hits'] < recipe['requests_per_test']):
                    raise RuntimeError('matched GET population was not resident and present')
                entry.update(complete=True, effective_settings=settings, measured_server_stat_delta=delta)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                entry['owned_server_stopped'] = process.poll() is not None
            (output / 'qualification.json').write_text(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'qualify-local'))
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--profile', choices=('published-as-written', 'matched-ram'), default='matched-ram')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--tcp-port', type=int, default=16379)
    parser.add_argument('--rdma-port', type=int, default=16380)
    parser.add_argument('--transports', nargs='+', choices=('tcp', 'rdma'), default=['tcp', 'rdma'])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    spec = load()
    if args.action == 'qualify-local':
        result = qualify(spec, args.source, args.output, args.tcp_port)
    else:
        result = plan(spec, args.source, args.profile, args.host,
                      {'tcp': args.tcp_port, 'rdma': args.rdma_port}, args.output, args.transports)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
