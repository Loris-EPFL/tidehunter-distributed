"""Owned local Valkey fixtures and scaled native-client reference controls.

The caller owns the returned object and must call close(), including on failure.
All data and receipts remain in the supplied fresh absolute directory.
"""
import binascii
import ipaddress
from contextlib import contextmanager
import math
from pathlib import Path
import socket
import subprocess
import time
import uuid

import local
import local_dry_run_targets
import local_valkey_rdma
import valkey_cloud
import valkey_reproduction
import valkey_service


def _directory(path):
    path = Path(path)
    if not path.is_absolute():
        raise ValueError('fixture directory must be absolute')
    path.mkdir(parents=True, exist_ok=False, mode=0o700)
    return path.resolve(strict=True)


def _save(path, value):
    valkey_cloud.save(path, value)


def _free_ports(endpoints):
    # The bind preflight is not a reservation. Endpoint PID admission below is
    # required before any mutating command, including cluster creation.
    for host, port in endpoints:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))




@contextmanager
def _root_service():
    original = valkey_service.command
    def command(argv, timeout=30):
        return original(local_dry_run_targets.ROOT + list(argv), timeout=timeout)
    valkey_service.command = command
    try:
        yield
    finally:
        valkey_service.command = original


class _Unit:
    def __init__(self, service):
        self.service = service
        self.pid = None

    def poll(self):
        state = valkey_service.status(self.service['unit'])
        return None if state.get('ActiveState') == 'active' else state.get('ActiveState', 'unknown')


class _Processes:
    def __init__(self, directory, runtime):
        self.directory = directory
        self.runtime = Path(runtime).resolve(strict=True)
        self.metadata = valkey_cloud.tools(self.runtime)
        self.processes = []
        self.closed = False
        self.owner = uuid.uuid4().hex

    def spawn(self, argv, name):
        service = {'runtime': str(self.runtime), 'unit': 'tidehunter-dry-valkey-' + self.owner + '-' + name,
                   'args': argv, 'cpus': 2, 'memory_mib': 1536, 'runtime_seconds': 7200,
                   'source_commit': self.metadata['source_commit'],
                   'server_sha256': self.metadata['artifacts']['src/valkey-server']['sha256']}
        process = _Unit(service)
        self.processes.append((name, process))
        _save(self.directory / (name + '-intent.json'), service)
        with _root_service():
            evidence = valkey_service.start({'native_valkey': service})
        process.pid = evidence[0]['pid']
        _save(self.directory / (name + '-service.json'), evidence)
        return process

    def identity(self, process):
        evidence = valkey_service.evidence({'native_valkey': process.service})[0]
        if process.pid is None or evidence['pid'] != process.pid:
            raise RuntimeError('Valkey unit PID changed')
        return evidence

    def close(self):
        if self.closed:
            return
        failures, records = [], []
        for name, process in reversed(self.processes):
            try:
                with _root_service():
                    stopped = valkey_service.stop({'native_valkey': process.service})
                after = valkey_service.status(process.service['unit'])
                if process.pid is None and not stopped and after.get('LoadState') == 'not-found':
                    raise RuntimeError('ambiguous Valkey unit start; quiescence not certified')
                if after.get('LoadState') != 'not-found' and (
                        after.get('ActiveState') not in ('inactive', 'failed') or after.get('MainPID') != '0'):
                    raise RuntimeError('owned Valkey unit remains active')
                records.append({'name': name, 'pid': process.pid, 'unit': process.service['unit'],
                                'stopped': stopped, 'after': after})
            except Exception as error:
                failures.append({'name': name, 'pid': process.pid, 'error': repr(error)})
        _save(self.directory / 'cleanup.json', {'processes': records, 'failures': failures,
                                                 'data_retained': True})
        if failures:
            raise RuntimeError('Valkey cleanup failed: ' + repr(failures))
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        try:
            self.close()
        except Exception as cleanup:
            if error is None:
                raise
            error.add_note('Valkey cleanup failed: ' + repr(cleanup))
        return False


def _owned_endpoint(fixture, process, endpoint):
    host, port = endpoint.rsplit(':', 1)
    text = local.command([fixture.runtime / 'bin/valkey-cli', '--raw', '-h', host, '-p', port,
                          'INFO', 'server'], timeout=5)
    info = local.valkey_info(text)
    if int(info.get('process_id', '-1')) != process.pid:
        raise RuntimeError('Valkey endpoint belongs to another process')
    return fixture.identity(process)


class Cluster(_Processes):
    """Five transient services with AOF always and exact admitted cgroups."""
    def __init__(self, directory, runtime, backend, rdma_address='10.99.0.1', timeout=120):
        if backend not in ('valkey_tcp', 'valkey_rdma'):
            raise ValueError('unsupported Valkey backend')
        if not 1 <= timeout <= 300:
            raise ValueError('cluster timeout must be in [1, 300]')
        address = str(ipaddress.IPv4Address(rdma_address))
        if ipaddress.IPv4Address(address).is_loopback or ipaddress.IPv4Address(address).is_unspecified:
            raise ValueError('RDMA address must be non-loopback')
        root = _directory(directory)
        super().__init__(root, runtime)
        self.backend = backend
        # The owner token is created before any unit is attempted.
        self.controls = [f'127.0.0.{21+i}:{6379+i}' for i in range(5)]
        self.rdma_endpoints = [f'{address}:{6380+i}' for i in range(5)]
        self.backend_options = local_valkey_rdma.options_for_backend(backend, 5, self.rdma_endpoints)
        self.state = {'runtime': str(self.runtime), 'control_endpoints': self.controls,
                      'rdma_endpoints': self.rdma_endpoints}
        ports = [(f'127.0.0.{21+i}', 6379+i) for i in range(5)]
        ports += [(f'127.0.0.{21+i}', 16379+i) for i in range(5)]
        _free_ports(ports)
        try:
            for i in range(5):
                data = root / f'node-{i}' / 'data'
                data.mkdir(parents=True)
                argv = local.valkey_server_args(f'127.0.0.{21+i}', 6379+i, 16379+i, True, 1024)
                argv = [str(self.runtime / 'bin/valkey-server') if x == 'valkey-server'
                        else x.replace('/data', str(data)) for x in argv]
                argv += ['--rdma-bind', address, '--rdma-port', str(6380+i)]
                self.spawn(argv, f'node-{i}')
            deadline = time.monotonic() + timeout
            for i, (_, process) in enumerate(self.processes):
                while True:
                    try:
                        _owned_endpoint(self, process, self.controls[i])
                        break
                    except (OSError, subprocess.SubprocessError, RuntimeError, ValueError):
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise
                        time.sleep(.1)
            def control(endpoint, *args):
                self._admit_endpoint(endpoint, False)
                if '--cluster' in args:
                    self.monitor_processes()
                return local_valkey_rdma.run(self.state, False, endpoint, *args)
            local.initialize_valkey(control, self.controls, root, max(1, deadline - time.monotonic()))
            while True:
                try:
                    proof = self.probe()
                    break
                except (OSError, subprocess.SubprocessError, RuntimeError, ValueError, StopIteration):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(.2)
            _save(root / 'deployment.json', {'owner': self.owner, 'backend': backend,
                  'backend_options': self.backend_options, 'observed': proof,
                  'physical_hosts': 1, 'independent_disks': False})
        except BaseException as error:
            try:
                self.close()
            except Exception as cleanup:
                error.add_note('Valkey cleanup failed: ' + repr(cleanup))
            raise

    def _admit_endpoint(self, endpoint, rdma):
        index = self.controls.index(endpoint)
        process = self.processes[index][1]
        identity = _owned_endpoint(self, process, endpoint)
        if rdma:
            host, port = endpoint.rsplit(':', 1)
            info = local.valkey_info(local_valkey_rdma.run(
                self.state, True, endpoint, 'valkey-cli', '--raw', '-h', host, '-p', port, 'INFO', 'server'))
            if int(info.get('process_id', '-1')) != process.pid:
                raise RuntimeError('RDMA endpoint belongs to another process')
        return identity

    def monitor_processes(self):
        return [{'role': f'valkey{i}', 'cgroup': _owned_endpoint(self, process, self.controls[i])['cgroup']}
                for i, (_, process) in enumerate(self.processes)]

    def probe(self):
        identities = self.monitor_processes()
        rdma = self.backend == 'valkey_rdma'
        def execute(endpoint, *args):
            self._admit_endpoint(endpoint, rdma)
            return local_valkey_rdma.run(self.state, rdma, endpoint, *args)
        proof = local.valkey_evidence(execute, self.controls, True)
        directories = {endpoint: str(self.directory / f'node-{i}' / 'data')
                       for i, endpoint in enumerate(self.controls)}
        proof['coverage'] = local.admit_valkey(proof['nodes'], self.controls, True, data_dir=directories)
        for node in proof['nodes']:
            endpoint = node['endpoint']
            own = next(line.split() for line in node['cluster_nodes'].splitlines()
                       if 'myself' in line.split()[2].split(','))
            ranges = [tuple(map(int, token.split('-'))) if '-' in token else (int(token), int(token))
                      for token in own[8:] if not token.startswith('[')]
            key = next((f'dry-{self.owner}-{i}' for i in range(200000)
                        if any(a <= binascii.crc_hqx(f'dry-{self.owner}-{i}'.encode(), 0) % 16384 <= b
                               for a, b in ranges)), None)
            if key is None or local.valkey_cli(execute, endpoint, 'PING').strip() != 'PONG':
                raise RuntimeError('Valkey qualification PING or slot selection failed')
            for args, expected in ((('SET', key, 'qualified'), 'OK'), (('GET', key), 'qualified'),
                                   (('DEL', key), '1')):
                if local.valkey_cli(execute, endpoint, *args).strip() != expected:
                    raise RuntimeError('Valkey qualification command failed: ' + args[0])
        proof.update(runtime=identities, transport='RDMA-CM RC' if rdma else 'TCP',
                     qualification={'status': 'passed', 'operations': ['PING', 'SET', 'GET', 'DEL'],
                                    'endpoints': self.backend_options['nodes']})
        return proof


def start_cluster(directory, runtime, backend, *, rdma_address='10.99.0.1', timeout=120):
    return Cluster(directory, runtime, backend, rdma_address, timeout)


def run_native_controls(directory, runtime, *, rdma_address='10.99.0.1', fraction=0.001,
                        minimum=32, timeout=120):
    """Run both published profiles over TCP and RDMA, with a fresh server per case."""
    if not math.isfinite(fraction) or not 0 < fraction <= 1 or type(minimum) is not int or minimum < 1:
        raise ValueError('invalid operation scaling')
    if not 1 <= timeout <= 1800:
        raise ValueError('native case timeout must be in [1, 1800]')
    rdma_address = str(ipaddress.IPv4Address(rdma_address))
    if ipaddress.IPv4Address(rdma_address).is_loopback or ipaddress.IPv4Address(rdma_address).is_unspecified:
        raise ValueError('RDMA address must be non-loopback')
    root = _directory(directory)
    runtime = Path(runtime).resolve(strict=True)
    valkey_cloud.tools(runtime)
    spec = valkey_reproduction.load()
    spec['requests_per_test'] = min(spec['requests_per_test'], max(minimum, math.ceil(spec['requests_per_test'] * fraction)))
    spec['case_deadline_seconds'] = timeout
    report = {'complete': False, 'planned_cases': 4, 'cases': [], 'qualification_only': True,
              'fraction': fraction, 'minimum': minimum}
    try:
        for profile in ('published-as-written', 'matched-ram'):
            for transport in ('tcp', 'rdma'):
                name = profile + '-' + transport
                case_dir = _directory(root / name)
                fixture = _Processes(case_dir, runtime)
                entry = {'profile': profile, 'transport': transport, 'complete': False}
                report['cases'].append(entry)
                try:
                    _free_ports([('127.0.0.1', valkey_cloud.PORTS['tcp'])])
                    recipe = valkey_reproduction.plan(spec, runtime, profile, '127.0.0.1',
                              valkey_cloud.PORTS, case_dir, ['tcp', 'rdma'], rdma_host=rdma_address,
                              binary_directory=runtime / 'bin')
                    case = next(item for item in recipe['cases'] if item['transport'] == transport)
                    _save(case_dir / 'plan.json', recipe)
                    data = case_dir / transport
                    data.mkdir()
                    conf = case_dir / 'valkey.conf'
                    save_rule = spec['profiles'][profile]['save']
                    conf.write_text('\n'.join([
                        'bind 127.0.0.1', 'port ' + str(valkey_cloud.PORTS['tcp']),
                        'rdma-bind ' + rdma_address, 'rdma-port ' + str(valkey_cloud.PORTS['rdma']),
                        'protected-mode no', 'daemonize no', 'appendonly no', 'maxmemory 20gb',
                        'maxmemory-policy volatile-lru', 'save ' + (save_rule or '\"\"'),
                        'dir ' + str(data), 'loglevel warning', '']))
                    argv = [str(runtime / 'bin/valkey-server'), str(conf)]
                    process = fixture.spawn(argv, 'server')
                    deadline = time.monotonic() + min(timeout, 30)
                    endpoint = '127.0.0.1:' + str(valkey_cloud.PORTS['tcp'])
                    while True:
                        try:
                            _owned_endpoint(fixture, process, endpoint)
                            break
                        except (OSError, subprocess.SubprocessError, RuntimeError, ValueError):
                            if process.poll() is not None or time.monotonic() >= deadline:
                                raise
                            time.sleep(.1)
                    def snapshot():
                        identity = _owned_endpoint(fixture, process, endpoint)
                        state = {'runtime': str(runtime), 'address': '127.0.0.1'}
                        values = valkey_cloud.cli(state, 'CONFIG', 'GET', 'appendonly', 'save',
                                                  'maxmemory', 'maxmemory-policy', 'cluster-enabled').splitlines()
                        settings = dict(zip(values[::2], values[1::2]))
                        expected = {'appendonly': 'no', 'save': spec['profiles'][profile]['save'],
                                    'maxmemory': str(20 * 1024**3), 'maxmemory-policy': 'volatile-lru',
                                    'cluster-enabled': 'no'}
                        if settings != expected:
                            raise ValueError('native control configuration differs')
                        if transport == 'rdma':
                            info = local.valkey_info(local.command(
                                [runtime / 'bin/valkey-cli', '--rdma', '--raw', '-h', rdma_address,
                                 '-p', str(valkey_cloud.PORTS['rdma']), 'INFO', 'server'], timeout=5))
                            if int(info.get('process_id', '-1')) != process.pid:
                                raise RuntimeError('native RDMA endpoint belongs to another process')
                        return {'ownership': identity, 'dbsize': int(valkey_cloud.cli(state, 'DBSIZE')),
                                'stats': valkey_reproduction.fields(valkey_cloud.cli(state, 'INFO', 'stats')),
                                'persistence': valkey_reproduction.fields(valkey_cloud.cli(state, 'INFO', 'persistence')),
                                'config': settings}
                    initial = snapshot()
                    _save(case_dir / 'initial.json', initial)
                    if initial['dbsize'] != 0:
                        raise ValueError('native control server was not empty')
                    if case['prefill']:
                        valkey_cloud.execute(case['prefill'], case_dir / 'prefill.log', timeout + 15)
                    before = snapshot()
                    _save(case_dir / 'before.json', before)
                    if case['prefill'] and before['dbsize'] != case['key_range']:
                        raise ValueError('native matched RAM prefill incomplete')
                    valkey_cloud.execute(case['measurement'], case_dir / 'benchmark.log', timeout + 15)
                    after = snapshot()
                    _save(case_dir / 'after.json', after)
                    delta = {k: int(after['stats'][k]) - int(before['stats'][k]) for k in
                             ('keyspace_hits', 'keyspace_misses', 'evicted_keys', 'total_error_replies')}
                    if min(delta.values()) < 0 or delta['evicted_keys'] or delta['total_error_replies'] or \
                       delta['keyspace_hits'] + delta['keyspace_misses'] < recipe['requests_per_test'] or \
                       (case['prefill'] and delta['keyspace_misses']):
                        raise ValueError('native control server counter admission failed')
                    entry.update(measurements=valkey_cloud.statistics((case_dir / 'benchmark.log').read_text(),
                                 recipe['requests_per_test']), server_stats_delta=delta,
                                 requests_per_test=recipe['requests_per_test'], key_range=case['key_range'],
                                 native_benchmark_sha256=valkey_service.digest(case_dir / 'benchmark.log'), complete=True)
                except Exception as error:
                    entry['error'] = type(error).__name__ + ': ' + str(error)
                finally:
                    try:
                        fixture.close()
                        entry['owned_server_stopped'] = True
                    except Exception as error:
                        entry['cleanup_error'] = repr(error)
                        raise
                    finally:
                        _save(case_dir / 'receipt.json', entry)
                        _save(root / 'report.json', report)
        report['complete'] = len(report['cases']) == 4 and all(x['complete'] and x['owned_server_stopped'] for x in report['cases'])
        return report
    finally:
        _save(root / 'report.json', report)
