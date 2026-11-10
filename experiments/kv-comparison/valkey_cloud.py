"""Separate native Valkey reference controls; never pooled with per-operation API cases."""
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid

import valkey_reproduction as reference
import valkey_service as service

PORTS = {'tcp': 19080, 'rdma': 19081}


def save(path, value):
    temp = path.with_suffix('.pending')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


def tools(runtime):
    metadata = service.identity(runtime)
    for name in ('valkey-server', 'valkey-cli', 'valkey-benchmark'):
        if service.digest(runtime / 'bin' / name) != metadata['artifacts']['src/' + name]['sha256']:
            raise ValueError('native Valkey executable differs from frozen package')
    return metadata


def cli(state, *args):
    return subprocess.check_output([str(Path(state['runtime']) / 'bin/valkey-cli'), '-h', state['address'],
        '-p', str(PORTS['tcp']), '--raw', *args], text=True, stderr=subprocess.STDOUT, timeout=10)


def snapshot(state):
    if service.digest(state['config_path']) != state['config_sha256']:
        raise ValueError('owned Valkey configuration changed')
    ownership = service.evidence(state['deployment'])
    server = reference.fields(cli(state, 'INFO', 'server'))
    if int(server.get('process_id', -1)) != ownership[0]['pid']:
        raise ValueError('native control endpoint belongs to another process')
    settings = cli(state, 'CONFIG', 'GET', 'appendonly', 'save', 'maxmemory', 'maxmemory-policy', 'cluster-enabled').splitlines()
    settings = dict(zip(settings[::2], settings[1::2]))
    expected = {'appendonly': 'no', 'save': state['save'], 'maxmemory': str(20 * 1024**3),
                'maxmemory-policy': 'volatile-lru', 'cluster-enabled': 'no'}
    if settings != expected:
        raise ValueError('effective standalone Valkey configuration differs')
    return {'ownership': ownership, 'server': server, 'settings': settings, 'dbsize': int(cli(state, 'DBSIZE')),
            'stats': reference.fields(cli(state, 'INFO', 'stats')),
            'persistence': reference.fields(cli(state, 'INFO', 'persistence'))}


def guest(config, request):
    if request['index'] != 0:
        raise ValueError('native reference server belongs only to owner zero')
    profile, transport = request['profile'], request['transport']
    spec = reference.load()
    if profile not in spec['profiles'] or transport not in PORTS:
        raise ValueError('unknown native Valkey control')
    directory = Path(config['fleet']['data_root']) / ('valkey-native-' + profile + '-' + transport)
    state_path = directory / 'state.json'
    action = request['action']
    if action == 'valkey_native_start':
        runtime = Path(config['binary']).parent.parent
        metadata = tools(runtime)
        remaining = request['deadline_epoch'] - time.time()
        if not 30 <= remaining <= 7200:
            raise ValueError('native Valkey deadline is not bounded')
        host = config['fleet']['nodes'][0]
        # Plan validates both distinct network addresses before interpolating config.
        reference.plan(spec, runtime, profile, host['address'], PORTS, directory, ['tcp', 'rdma'],
                       rdma_host=host['rdma_address'], binary_directory=runtime / 'bin')
        directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        data = directory / 'data'
        data.mkdir(mode=0o700)
        conf = directory / 'valkey.conf'
        save_rule = spec['profiles'][profile]['save']
        text = '\n'.join([f"bind {host['address']}", f"port {PORTS['tcp']}",
            f"rdma-bind {host['rdma_address']}", f"rdma-port {PORTS['rdma']}",
            'protected-mode no', 'daemonize no', 'appendonly no', 'maxmemory 20gb',
            'maxmemory-policy volatile-lru', 'save ' + (save_rule or '""'),
            'dir ' + str(data), 'loglevel warning', 'logfile ' + str(directory / 'server.log'), ''])
        conf.write_text(text)
        deployment = {'native_valkey': {'runtime': str(runtime),
            'unit': 'tidehunter-native-valkey-' + uuid.uuid4().hex,
            'source_commit': metadata['source_commit'],
            'server_sha256': metadata['artifacts']['src/valkey-server']['sha256'],
            'args': [str(runtime / 'bin/valkey-server'), str(conf)], 'memory_mib': 24576, 'cpus': 8,
            'runtime_seconds': math.floor(remaining)}}
        state = {'runtime': str(runtime), 'address': host['address'], 'rdma_address': host['rdma_address'],
                 'config_path': str(conf), 'config_sha256': service.digest(conf), 'save': save_rule,
                 'deployment': deployment, 'started': False, 'stopped': False}
        save(state_path, state)  # Intent survives an ambiguous systemd response.
        service.start(deployment)
        state['started'] = True
        save(state_path, state)
        end = time.monotonic() + 20
        for _ in range(100):
            try:
                evidence = snapshot(state)
                if evidence['dbsize'] != 0:
                    raise ValueError('native control server was not empty')
                return evidence
            except subprocess.CalledProcessError:
                if time.monotonic() >= end:
                    raise
                time.sleep(.1)
        raise TimeoutError('native Valkey readiness deadline')
    state = json.loads(state_path.read_text())
    if action == 'valkey_native_snapshot':
        return snapshot(state)
    if action != 'valkey_native_stop':
        raise ValueError('unknown native Valkey operation')
    if not state['stopped']:
        if not state['started'] and service.status(state['deployment']['native_valkey']['unit']).get('LoadState') == 'not-found':
            raise RuntimeError('ambiguous queued native Valkey start; cannot certify quiescence')
        ownership = service.stop(state['deployment'])
        state['stopped'] = True
        save(state_path, state)
    else:
        ownership = []
    log = directory / 'server.log'
    tail = None
    if log.exists():
        with log.open('rb') as stream:
            stream.seek(max(0, log.stat().st_size - 16384))
            tail = stream.read(16384).decode(errors='replace')
    result = {'owned_server_stopped': True, 'ownership': ownership, 'server_log_tail': tail}
    try:
        result['journal_tail'] = service.command(['journalctl', '--unit=' + state['deployment']['native_valkey']['unit'],
            '--no-pager', '--output=cat', '--lines=40'], timeout=10)[-16384:]
    except (OSError, subprocess.SubprocessError) as error:
        result['journal_error'] = type(error).__name__ + ': ' + str(error)
    return result


def execute(argv, path, timeout):
    """Terminate the exact owned process group, including the outer timeout utility."""
    with path.open('w') as output:
        process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            raise
    if code:
        raise RuntimeError('native benchmark failed; retained output: ' + str(path))
    if path.stat().st_size > 16 * 1024**2:
        raise ValueError('native benchmark output exceeds bound')


def statistics(text, requests_per_test):
    """Keep native-client populations separate; preserve its printed millisecond units."""
    result = {}
    blocks = re.split(r'^======\s*(PING_INLINE|PING_MBULK|SET|GET)\s*======\s*$', text.replace('\r', '\n'), flags=re.MULTILINE)
    for operation, block in zip(blocks[1::2], blocks[2::2]):
        completed = re.search(r'^\s*([0-9]+) requests completed in ', block, flags=re.MULTILINE)
        if operation in result or not completed or int(completed.group(1)) != requests_per_test:
            raise ValueError('duplicate or incomplete native benchmark population')
        throughput = re.search(r'throughput summary:\s*([0-9.]+) requests per second', block)
        latency = re.search(r'latency summary \(msec\):\s*avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n\s*([0-9.\s]+)', block)
        if not throughput or not latency:
            raise ValueError('native benchmark omitted throughput or latency summary')
        numbers = latency.group(1).split()[:6]
        if len(numbers) != 6:
            raise ValueError('native latency summary has missing observations')
        result[operation] = {'calls_per_second': float(throughput.group(1)),
            'latency_ms': dict(zip(('mean', 'min', 'p50', 'p95', 'p99', 'max'), map(float, numbers)))}
    if set(result) != {'PING_INLINE', 'PING_MBULK', 'SET', 'GET'}:
        raise ValueError('native benchmark omitted a requested population')
    return result


def run(controller):
    profiles = controller.config['spec'].get('valkey_reproduction_profiles', [])
    if not profiles:
        return None
    spec = reference.load()
    runtime = Path(controller.config['binary']).parent.parent
    metadata = tools(runtime)
    root = controller.root / 'valkey-native'
    root.mkdir()
    report = {'complete': False, 'planned_cases': len(profiles) * 2, 'completed_cases': 0,
              'per_operation_api_suite': False, 'source_commit': metadata['source_commit'], 'cases': []}
    for profile in profiles:
        for transport in ('tcp', 'rdma'):
            name = profile + '-' + transport
            directory = root / name
            directory.mkdir()
            host = controller.config['fleet']['nodes'][0]
            recipe = reference.plan(spec, runtime, profile, host['address'], PORTS, directory, ['tcp', 'rdma'],
                rdma_host=host['rdma_address'], binary_directory=runtime / 'bin')
            case = next(c for c in recipe['cases'] if c['transport'] == transport)
            save(directory / 'plan.json', recipe)
            entry = {'profile': profile, 'transport': transport, 'complete': False,
                     'interpretation': recipe['interpretation'], 'requests_per_test': recipe['requests_per_test'],
                     'native_histograms_not_per_operation_samples': True,
                     'binary_sha256': {k: v['sha256'] for k, v in metadata['artifacts'].items()}}
            report['cases'].append(entry)
            request = {'profile': profile, 'transport': transport}
            failure = None
            try:
                lifetime = min(controller.remaining() - 60, 2 * spec['case_deadline_seconds'] + 60)
                controller.request(0, {'action': 'valkey_native_start', **request,
                    'deadline_epoch': time.time() + lifetime}, case=name, timeout=90)
                if case['prefill']:
                    execute(case['prefill'], directory / 'prefill.log', min(controller.remaining(), spec['case_deadline_seconds'] + 15))
                before = controller.request(0, {'action': 'valkey_native_snapshot', **request}, case=name, timeout=30)
                save(directory / 'before.json', before)
                if case['prefill'] and before['dbsize'] != case['key_range']:
                    raise ValueError('native matched RAM prefill is incomplete')
                execute(case['measurement'], directory / 'benchmark.log', min(controller.remaining(), spec['case_deadline_seconds'] + 15))
                after = controller.request(0, {'action': 'valkey_native_snapshot', **request}, case=name, timeout=30)
                save(directory / 'after.json', after)
                delta = {k: int(after['stats'][k]) - int(before['stats'][k]) for k in
                         ('keyspace_hits', 'keyspace_misses', 'evicted_keys', 'total_error_replies')}
                if min(delta.values()) < 0 or delta['evicted_keys'] or delta['total_error_replies']:
                    raise ValueError('native control reset counters or produced evictions/errors')
                if delta['keyspace_hits'] + delta['keyspace_misses'] < recipe['requests_per_test']:
                    raise ValueError('native GET population is incomplete')
                if case['prefill'] and delta['keyspace_misses']:
                    raise ValueError('matched RAM GET population contains misses')
                entry.update(server_stats_delta=delta, measurements=statistics((directory / 'benchmark.log').read_text(), recipe['requests_per_test']),
                             native_benchmark_sha256=service.digest(directory / 'benchmark.log'), complete=True)
            except BaseException as error:
                failure = error
                entry['error'] = type(error).__name__ + ': ' + str(error)
            finally:
                try:
                    cleanup = controller.request(0, {'action': 'valkey_native_stop', **request}, case=name, timeout=60, export=True)
                    save(directory / 'server-cleanup.json', cleanup)
                    if cleanup.get('owned_server_stopped') is not True:
                        raise RuntimeError('native Valkey server stop unconfirmed')
                    entry['owned_server_stopped'] = True
                except BaseException as cleanup_error:
                    entry['cleanup_error'] = type(cleanup_error).__name__ + ': ' + str(cleanup_error)
                    if failure is None:
                        failure = cleanup_error
                save(directory / 'receipt.json', entry)
                if entry['complete'] and entry.get('owned_server_stopped'):
                    report['completed_cases'] += 1
                save(root / 'report.json', report)
            if failure is not None:
                raise failure
    report['complete'] = report['completed_cases'] == report['planned_cases']
    save(root / 'report.json', report)
    return report
