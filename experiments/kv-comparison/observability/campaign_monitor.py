#!/usr/bin/env python3
"""Run-owned, cohosted campaign observation and hash-bound historical TSDB replay.

No AWS calls, service discovery, Compose project, or monitoring library dependency.
Every mutating API executes on the current machine. Cloud orchestration transports
host calls separately. A root-filesystem work directory is required; CPU/memory
caps and retention do not establish non-perturbation or a hard TSDB disk quota.
"""
import argparse
import base64
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
from functools import wraps
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parent
LABEL = 'org.tidehunter.campaign-observer'
DASHBOARD = 'kv-comparison-campaign'
DATASOURCE = 'kv-campaign-prometheus'
MAX_BYTES = 4 * 1024**3
MAX_MEMBERS = 100000
MAX_JSON = 16 * 1024**2
SCRAPE_SECONDS = 1
QUERY_PAGE_SECONDS = 300
QUERY_METRICS = ('kv_observer_snapshot_final', 'kv_observer_case_terminal', 'kv_observer_data_available', 'up',
                 'kv_case_success_calls_per_second', 'kv_case_api_quantile_seconds', 'kv_case_read_path_calls',
                 'kv_case_resource_interval_available', 'kv_case_resource_interval_elapsed_seconds',
                 'kv_case_resource_interval_cpu_seconds', 'kv_case_resource_interval_cpu_throttled_seconds',
                 'kv_case_resource_interval_cpu_periods', 'kv_case_resource_interval_cpu_throttled_periods',
                 'kv_case_resource_interval_io_read_bytes', 'kv_case_resource_interval_io_write_bytes',
                 'kv_case_resource_interval_memory_current_bytes', 'kv_case_resource_interval_memory_peak_bytes',
                 'kv_observer_cgroup_cpu_seconds_total', 'kv_observer_cgroup_cpu_throttled_seconds_total',
                 'kv_observer_cgroup_cpu_periods_total', 'kv_observer_cgroup_cpu_throttled_periods_total',
                 'kv_observer_cgroup_io_read_bytes_total', 'kv_observer_cgroup_io_write_bytes_total',
                 'kv_case_write_diagnostic_available', 'kv_case_write_diagnostic_operations',
                 'kv_case_write_diagnostic_stage_seconds', 'kv_case_write_diagnostic_stage_count')
CAPS = {'prometheus': ('1', '1g'), 'grafana': ('0.5', '768m'),
        'benchmark': ('0.25', '256m'), 'host': ('0.1', '128m'), 'node': ('0.1', '128m')}
TOKEN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$')
OPERATION_DEADLINE = ContextVar('campaign_observer_deadline', default=None)
STARTUP_RESOURCES = ContextVar('campaign_observer_startup_resources', default=None)


def startup_guard(function):
    """Rollback only containers newly attempted by this startup, including nesting."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        if STARTUP_RESOURCES.get() is not None:
            return function(*args, **kwargs)
        resources = []
        token = STARTUP_RESOURCES.set(resources)
        try:
            return function(*args, **kwargs)
        except BaseException as error:
            # The operation's deadline must not disable rollback. This separate
            # allowance is bounded, never prunes, and never touches prior services.
            deadline_token = OPERATION_DEADLINE.set(time.monotonic() + 10)
            try:
                for spec, service in reversed(resources):
                    try:
                        result = stop_services(spec, [service])
                        state, _ = state_for(spec)
                        atomic_json(state / ('startup-failure-' + service + '.json'),
                                    {'error_type': type(error).__name__, 'cleanup': result})
                    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                        # Preserve the original startup error even if Docker is
                        # unreachable; exact names and owner state remain on disk.
                        pass
            finally:
                OPERATION_DEADLINE.reset(deadline_token)
            raise
        finally:
            STARTUP_RESOURCES.reset(token)
    return wrapped


def preflight_ports(spec, endpoints):
    """Bind every exact address before starting anything; post-start checks close races."""
    sockets = []
    try:
        for address, port in endpoints:
            budget(spec, 5)
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockets.append(listener)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind((address, port))
                listener.listen(1)
            except OSError as error:
                raise RuntimeError(f'observer listen address is unavailable: {address}:{port}') from error
    finally:
        for listener in sockets:
            listener.close()


def collector_ports(spec, benchmark=True):
    return [('127.0.0.1', spec['ports'][service])
            for service in (('prometheus', 'grafana', 'benchmark') if benchmark else ('prometheus', 'grafana'))]


def assert_owned_running(spec, owner, containers):
    for expected in containers:
        detail = json.loads(docker(spec, 'inspect', expected['container_id'], maximum=5))[0]
        labels = detail.get('Config', {}).get('Labels', {})
        if (detail.get('Id') != expected['container_id']
                or labels.get(LABEL) != spec['run_id']
                or labels.get(LABEL + '.owner') != owner['token']
                or detail.get('Config', {}).get('Image') != expected['image']
                or detail.get('Image') != expected['image_id']
                or detail.get('State', {}).get('Running') is not True):
            raise RuntimeError('exact-owned observer container is not running with its pinned image: ' + expected['name'])


def assert_prometheus_identity(spec):
    state, owner = state_for(spec)
    response = http(spec, 'prometheus', '/api/v1/status/config')
    configuration = response.get('data', {}).get('yaml')
    if response.get('status') != 'success' or not isinstance(configuration, str):
        raise ValueError('Prometheus did not return its effective configuration')
    # Prometheus emits YAML; these generated string scalars have no escapes or
    # spaces. Accept its optional quoting without adding a YAML dependency.
    for key, value in (('campaign_observer_run_id', spec['run_id']),
                       ('campaign_observer_session', owner['identity'])):
        pattern = r'(?m)^[ \t]*' + key + r":[ \t]*([\"']?)" + re.escape(value) + r'\1[ \t]*$'
        if len(re.findall(pattern, configuration)) != 1:
            raise ValueError('Prometheus effective configuration belongs to another session')
    return {'session_identity': owner['identity'], 'effective_configuration_sha256':
            hashlib.sha256(configuration.encode()).hexdigest()}


def check_deadline():
    deadline = OPERATION_DEADLINE.get()
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError('bounded observer operation deadline reached')


@contextmanager
def operation_scope(seconds):
    if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('operation timeout must be finite and positive')
    previous = OPERATION_DEADLINE.get()
    token = OPERATION_DEADLINE.set(min(previous or float('inf'), time.monotonic() + seconds))
    try:
        yield
    finally:
        OPERATION_DEADLINE.reset(token)


def bounded(default_seconds):
    def decorate(function):
        @wraps(function)
        def wrapped(*args, timeout_seconds=default_seconds, **kwargs):
            with operation_scope(timeout_seconds):
                return function(*args, **kwargs)
        return wrapped
    return decorate


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            check_deadline()
            result.update(block)
    return result.hexdigest()


def atomic_json(path, value, mode=0o600, compact=False):
    path = Path(path)
    data = (json.dumps(value, sort_keys=True, indent=None if compact else 2,
                       separators=(',', ':') if compact else None, allow_nan=False) + '\n').encode()
    if len(data) > MAX_JSON:
        raise ValueError('JSON evidence exceeds size bound')
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JSON:
        raise ValueError('expected a bounded ordinary JSON file')
    return json.loads(path.read_text())


def absolute(value):
    if not isinstance(value, str) or not Path(value).is_absolute() or '..' in Path(value).parts:
        raise ValueError('expected explicit absolute path without traversal')
    path = Path(value)
    if path.resolve() != path:
        raise ValueError('symlinked or noncanonical path is not allowed')
    return path


def validate_spec(value):
    if not isinstance(value, dict):
        raise ValueError('session spec must be a JSON object')
    spec = dict(value)
    allowed = {'schema_version', 'run_id', 'state_dir', 'results_root', 'deadline_utc',
               'hosts', 'ports', 'allow_loopback_hosts'}
    if set(spec) - allowed or type(spec.get('schema_version')) is not int or spec['schema_version'] != 1:
        raise ValueError('invalid session spec schema')
    if not isinstance(spec.get('run_id'), str) or not re.fullmatch(r'th-[A-Za-z0-9][A-Za-z0-9_-]{0,78}', spec['run_id']):
        raise ValueError('run_id must be a safe th-* identity')
    state = absolute(spec['state_dir'])
    results = absolute(spec['results_root'])
    if state == results or state.is_relative_to(results):
        raise ValueError('state must be outside results root')
    if state == ROOT or state.is_relative_to(ROOT) or ROOT.is_relative_to(state):
        raise ValueError('mutable state must be outside repository source')
    source = ROOT
    while source.parent != source and source.name != 'tidehunter-sharded-wal':
        source = source.parent
    if source.name == 'tidehunter-sharded-wal' and state.is_relative_to(source):
        raise ValueError('mutable state must be outside repository source')
    local = spec.setdefault('allow_loopback_hosts', False)
    if not isinstance(local, bool):
        raise ValueError('allow_loopback_hosts must be boolean')
    hosts = spec['hosts']
    if not isinstance(hosts, list) or len(hosts) not in ((1, 6) if local else (6,)):
        raise ValueError('expected six cloud hosts or one/six explicit local loopback hosts')
    expected = {-1} if len(hosts) == 1 else {-1, 0, 1, 2, 3, 4}
    indexes, addresses = set(), set()
    for host in hosts:
        if not isinstance(host, dict) or set(host) != {'index', 'address', 'role'} or type(host['index']) is not int:
            raise ValueError('host fields must be index/address/role')
        index = host['index']
        ip = ipaddress.IPv4Address(host['address'])
        if str(ip) != host['address'] or ip.is_unspecified or ip.is_multicast or ip.is_link_local:
            raise ValueError('invalid explicit IPv4 target')
        if local and not ip.is_loopback or not local and (ip.is_loopback or not ip.is_private):
            raise ValueError('hosts must be approved private addresses (loopback only for local)')
        if index in indexes or str(ip) in addresses:
            raise ValueError('duplicate host index/address')
        if host['role'] != ('coordinator' if index == -1 else f'storage-{index}'):
            raise ValueError('host role/index mismatch')
        indexes.add(index)
        addresses.add(str(ip))
    if indexes != expected:
        raise ValueError('host indexes must identify coordinator and explicit fleet')
    ports = spec['ports']
    if set(ports) != {'prometheus', 'grafana', 'benchmark', 'node', 'host'}:
        raise ValueError('explicit five service ports required')
    if any(type(port) is not int or not 1024 <= port <= 65535 for port in ports.values()) or len(set(ports.values())) != 5:
        raise ValueError('ports must be distinct nonprivileged TCP ports')
    deadline = datetime.fromisoformat(spec['deadline_utc'].replace('Z', '+00:00'))
    if deadline.tzinfo is None:
        raise ValueError('deadline_utc must be timezone aware')
    if not local and state != Path('/var/lib/tidehunter-observation') / spec['run_id']:
        raise ValueError('cloud state must use run-owned root-volume work directory')
    return spec


def budget(spec, maximum=30, cleanup=False):
    check_deadline()
    operation = OPERATION_DEADLINE.get()
    if operation is not None:
        maximum = min(maximum, operation - time.monotonic())
    if cleanup:
        return maximum
    remaining = datetime.fromisoformat(spec['deadline_utc'].replace('Z', '+00:00')).timestamp() - time.time() - 3
    if remaining <= 0:
        raise TimeoutError('observer deadline reached (cleanup reserve retained)')
    return min(maximum, remaining)


def state_for(spec, create=False):
    spec = validate_spec(spec)
    state = Path(spec['state_dir'])
    identity = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    marker = state / 'owner.json'
    if not state.exists():
        if not create:
            raise ValueError('session state does not exist')
        state.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if state.parent.stat().st_dev != Path('/').stat().st_dev:
            raise ValueError('observer state must be on the root filesystem, not benchmark data SSD')
        state.mkdir(mode=0o700)
        atomic_json(marker, {'run_id': spec['run_id'], 'identity': identity, 'token': secrets.token_hex(16)})
        atomic_json(state / 'spec.json', spec)
    owner = load_json(marker)
    if owner.get('run_id') != spec['run_id'] or owner.get('identity') != identity:
        raise ValueError('state is not owned by this exact session spec')
    if state.stat().st_uid != os.getuid() or state.stat().st_mode & 0o077:
        raise ValueError('state root must be private and owned by current effective user')
    return state, owner


def receipt(started, complete=True, **fields):
    return {'schema_version': 1, 'complete': complete, 'status': 'complete' if complete else 'gap',
            'gaps': [], 'elapsed_seconds': time.monotonic() - started, **fields}


def docker(spec, *args, maximum=40, cleanup=False):
    result = subprocess.run(['docker', *map(str, args)], check=False, capture_output=True,
                            text=True, timeout=budget(spec, maximum, cleanup))
    if result.returncode:
        # Do not echo Docker arguments/environment (could carry a mounted secret).
        raise RuntimeError('Docker operation failed: ' + result.stderr[-1500:])
    if len(result.stdout) > MAX_JSON:
        raise ValueError('Docker response exceeds bound')
    return result.stdout.strip()


def container_name(spec, owner, service):
    return spec['run_id'] + '-obs-' + owner['token'][:10] + '-' + service


def images():
    pins = load_json(ROOT / 'images.lock.json')['images']
    result = {name: entry['image'] for name, entry in pins.items()}
    if any(not re.fullmatch(r'[A-Za-z0-9_./:-]+@sha256:[a-f0-9]{64}', image) for image in result.values()):
        raise ValueError('all observer images must be digest pinned')
    return result


def run_container(spec, state, owner, service, image, arguments, mounts=(), environment=None):
    name = container_name(spec, owner, service)
    cpu, memory = CAPS['node' if service.startswith('node-') else 'host' if service.startswith('host-') else service]
    command = ['run', '--pull', 'never', '--detach', '--name', name, '--label', LABEL + '=' + spec['run_id'],
               '--label', LABEL + '.owner=' + owner['token'], '--network', 'host',
               '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
               '--cpus', cpu, '--memory', memory, '--pids-limit', '256', '--restart', 'no',
               '--log-opt', 'max-size=5m', '--log-opt', 'max-file=2',
               '--user', f'{os.getuid()}:{os.getgid()}', '--tmpfs', '/tmp:size=64m,mode=1777']
    for source, target, readonly in mounts:
        if ',' in str(source) or ',' in target:
            raise ValueError('Docker mount path may not contain comma')
        command += ['--mount', f'type=bind,source={source},target={target}' + (',readonly' if readonly else '')]
    for key, value in (environment or {}).items():
        command += ['--env', key + '=' + str(value)]
    command += [image, *arguments]
    # All starts, especially offline replay, are registry-independent.
    docker(spec, 'image', 'inspect', image, maximum=5)
    if docker(spec, 'ps', '-a', '--filter', 'name=^/' + name + '$', '--format', '{{.ID}}', maximum=5):
        raise ValueError('refusing to reuse an existing observer container name')
    if STARTUP_RESOURCES.get() is not None:
        STARTUP_RESOURCES.get().append((spec, service))
    container_id = docker(spec, *command, maximum=60)
    detail = json.loads(docker(spec, 'inspect', name))[0]
    if detail['Config']['Labels'].get(LABEL + '.owner') != owner['token']:
        raise ValueError('started container ownership mismatch')
    image_detail = json.loads(docker(spec, 'image', 'inspect', image))[0]
    answer = {'name': name, 'container_id': container_id, 'image': image,
              'image_id': image_detail['Id'], 'repo_digests': image_detail.get('RepoDigests', []),
              'cpu_limit': cpu, 'memory_limit': memory, 'network': 'host'}
    atomic_json(state / ('container-' + service + '.json'), answer)
    assert_owned_running(spec, owner, [answer])
    return answer


class ResponseTooLarge(ValueError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise ValueError('unexpected redirect from fixed observer endpoint')


def http(spec, service, path, payload=None, method=None, authenticated=False, maximum=5):
    url = f'http://127.0.0.1:{spec["ports"][service]}' + path
    headers = {'Accept': 'application/json'}
    data = None
    if authenticated:
        state, _ = state_for(spec)
        password = (state / 'grafana_admin_password').read_text().strip()
        headers['Authorization'] = 'Basic ' + base64.b64encode(('researcher:' + password).encode()).decode()
    if payload is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(payload).encode()
    request = Request(url, data=data, headers=headers, method=method)
    with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=budget(spec, maximum)) as response:
        raw = response.read(MAX_JSON + 1)
        if len(raw) > MAX_JSON:
            raise ResponseTooLarge('observer endpoint returned oversized response')
        if not 200 <= response.status < 300:
            raise ValueError('observer endpoint returned invalid status')
    result = json.loads(raw) if raw else {}
    if not isinstance(result, dict):
        raise ValueError('observer endpoint did not return a JSON object')
    return result


def wait_http(spec, service, path, authenticated=False):
    until = time.monotonic() + budget(spec, 45)
    error_type = 'unavailable'
    while time.monotonic() < until:
        try:
            return http(spec, service, path, authenticated=authenticated, maximum=min(2, until - time.monotonic()))
        except (OSError, ValueError) as error:
            error_type = type(error).__name__
            time.sleep(min(0.2, max(0, until - time.monotonic())))
    raise TimeoutError(f'{service} readiness failed ({error_type})')


def query(spec, expression, timestamp=None, start=None, end=None):
    parameters = {'query': expression, 'timeout': '2s'}
    path = '/api/v1/query'
    if start is not None:
        path += '_range'
        parameters.update(start=start, end=end, step=max(1, (end - start) / 600))
    elif timestamp is not None:
        parameters['time'] = timestamp
    answer = http(spec, 'prometheus', path + '?' + urlencode(parameters), maximum=3)
    if answer.get('status') != 'success' or answer.get('error') or answer.get('warnings'):
        raise ValueError('Prometheus query failed or returned incomplete/warned evidence')
    return answer['data']


def host_for(spec, index):
    return next(host for host in spec['hosts'] if host['index'] == index)


def sources(state):
    directory = state / 'source'
    directory.mkdir(mode=0o755, exist_ok=True)
    identities = {}
    for name in ('campaign_exporter.py', 'exporter.py', 'campaign_monitor.py', 'images.lock.json'):
        source = ROOT / name
        if not source.is_file():
            raise ValueError('required campaign source missing: ' + name)
        identities[name] = digest(source)
        if (directory / name).exists():
            if digest(directory / name) != identities[name]:
                raise ValueError('frozen session source changed; require a fresh session')
        else:
            shutil.copyfile(source, directory / name)
            (directory / name).chmod(0o444)
    return identities


@startup_guard
@bounded(90)
def start_host(spec, index):
    started = time.monotonic()
    spec = validate_spec(spec)
    host = host_for(spec, index)
    preflight_ports(spec, [(host['address'], spec['ports']['node']), (host['address'], spec['ports']['host'])])
    state, owner = state_for(spec, create=True)
    identities = sources(state)
    selection = state / f'host-{index}'
    selection.mkdir(mode=0o755, exist_ok=True)
    atomic_json(selection / 'selection.json', {'job': None, 'processes': []}, 0o444)
    atomic_json(selection / 'exporter.json', {'schema_version': 1, 'mode': 'host', 'run_id': spec['run_id'],
                'selection': '/selection/selection.json', 'proc_root': '/host/proc', 'cgroup_root': '/host/cgroup'}, 0o444)
    pins = images()
    node_args = [f'--web.listen-address={host["address"]}:{spec["ports"]["node"]}', '--web.max-requests=2',
                 '--path.procfs=/host/proc', '--path.sysfs=/host/sys', '--collector.disable-defaults']
    node_args += ['--collector.' + name for name in ('cpu', 'meminfo', 'diskstats', 'netdev', 'stat',
                                                  'loadavg', 'uname', 'vmstat', 'pressure')]
    node = run_container(spec, state, owner, f'node-{index}', pins['prom/node-exporter'], node_args,
                         [('/proc', '/host/proc', True), ('/sys', '/host/sys', True)])
    custom = run_container(spec, state, owner, f'host-{index}', pins['library/python'],
                           ['python3', '-B', '/app/campaign_exporter.py', '--config', '/selection/exporter.json',
                            '--listen', f'{host["address"]}:{spec["ports"]["host"]}'],
                           [(state / 'source', '/app', True), (selection, '/selection', True),
                            ('/proc', '/host/proc', True), ('/sys/fs/cgroup', '/host/cgroup', True)])
    # Host private endpoints cannot be checked via collector loopback on cloud.
    for service, port in (('node', spec['ports']['node']), ('host', spec['ports']['host'])):
        wait_endpoint(spec, host['address'], port, '/metrics' if service == 'node' else '/healthz')
    assert_owned_running(spec, owner, [node, custom])
    answer = receipt(started, host=host, containers=[node, custom], source_sha256=identities,
                     cgroups='explicit selections only; unavailable until supplied', placement='cohost, not calibrated')
    atomic_json(state / f'host-{index}-receipt.json', answer)
    return answer


def wait_endpoint(spec, address, port, path):
    until = time.monotonic() + budget(spec, 30)
    while time.monotonic() < until:
        try:
            request = Request(f'http://{address}:{port}{path}')
            with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=min(2, budget(spec), max(0.01, until - time.monotonic()))) as response:
                if response.status == 200:
                    response.read(1)
                    return
        except OSError:
            pass
        time.sleep(min(0.1, max(0, until - time.monotonic())))
    raise TimeoutError('explicit exporter endpoint did not become ready')


def configure_collector(spec, state, offline=False, dashboard=None, time_range=None):
    config = state / 'config'
    config.mkdir(mode=0o755, exist_ok=True)
    for name in ('dashboards', 'provisioning', 'provisioning/dashboards', 'provisioning/datasources'):
        (config / name).mkdir(mode=0o755, exist_ok=True)
    if dashboard is None:
        dashboard = load_json(ROOT / 'dashboards/campaign.json')
    if dashboard.get('uid') != DASHBOARD:
        raise ValueError('campaign dashboard source UID mismatch')
    dashboard = json.loads(json.dumps(dashboard))
    if offline:
        dashboard['time'] = {'from': datetime.fromtimestamp(time_range['start'], timezone.utc).isoformat(),
                             'to': datetime.fromtimestamp(time_range['end'], timezone.utc).isoformat()}
        dashboard['refresh'] = ''
    atomic_json(config / 'dashboards/campaign.json', dashboard, 0o444)
    provider = {'apiVersion': 1, 'providers': [{'name': 'campaign', 'orgId': 1, 'type': 'file',
                'disableDeletion': True, 'editable': False, 'options': {'path': '/campaign-dashboards'}}]}
    atomic_json(config / 'provisioning/dashboards/campaign.yaml', provider, 0o444)
    datasource = {'apiVersion': 1, 'datasources': [{'name': 'Campaign Prometheus', 'uid': DATASOURCE,
                  'type': 'prometheus', 'access': 'proxy', 'url': f'http://127.0.0.1:{spec["ports"]["prometheus"]}',
                  'isDefault': True, 'editable': False, 'jsonData': {'httpMethod': 'POST', 'timeInterval': '1s'}}]}
    atomic_json(config / 'provisioning/datasources/campaign.yaml', datasource, 0o444)
    jobs = []
    if not offline:
        for name, port in (('campaign-host', 'node'), ('campaign-cgroup', 'host')):
            groups = [{'targets': [f'{host["address"]}:{spec["ports"][port]}'],
                       'labels': {'run_id': spec['run_id'], 'host_role': host['role'], 'host_index': str(host['index'])}}
                      for host in spec['hosts']]
            jobs.append({'job_name': name, 'scrape_interval': '5s', 'scrape_timeout': '2s', 'static_configs': groups})
        jobs.append({'job_name': 'campaign-benchmark', 'scrape_interval': '1s', 'scrape_timeout': '800ms',
                     'static_configs': [{'targets': [f'127.0.0.1:{spec["ports"]["benchmark"]}'],
                                         'labels': {'run_id': spec['run_id'], 'host_role': 'coordinator', 'host_index': '-1'}}]})
    _, owner = state_for(spec)
    prom = {'global': {'scrape_interval': '5s', 'evaluation_interval': '5s',
                      'external_labels': {'campaign_observer_run_id': spec['run_id'],
                                          'campaign_observer_session': owner['identity']}},
            'scrape_configs': jobs}
    atomic_json(config / 'prometheus.json', prom, 0o444)
    return prom


@bounded(120)
def start_ui(spec, state, owner, pins, retention='7d', retention_size='2GB'):
    preflight_ports(spec, collector_ports(spec, benchmark=False))
    if shutil.disk_usage(state).free < 6 * 1024**3:
        raise ValueError('observer needs at least 6GiB free root disk for head/WAL, snapshot and archive; retention is not a hard disk quota')
    for directory in ('prometheus', 'grafana'):
        (state / directory).mkdir(mode=0o755, exist_ok=True)
    password = state / 'grafana_admin_password'
    if password.exists():
        raise ValueError('fresh UI state required; stop and use a fresh session to restart')
    fd = os.open(password, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, 'w') as stream:
        stream.write(secrets.token_urlsafe(32) + '\n')
    prometheus = run_container(spec, state, owner, 'prometheus', pins['prom/prometheus'],
        ['--config.file=/config/prometheus.json', f'--web.listen-address=127.0.0.1:{spec["ports"]["prometheus"]}',
         '--web.enable-admin-api', '--storage.tsdb.path=/prometheus', f'--storage.tsdb.retention.time={retention}',
         f'--storage.tsdb.retention.size={retention_size}', '--query.timeout=5s', '--query.max-concurrency=4'],
        [(state / 'config', '/config', True), (state / 'prometheus', '/prometheus', False)])
    environment = {'GOMAXPROCS': '2', 'GF_SERVER_HTTP_ADDR': '127.0.0.1', 'GF_SERVER_HTTP_PORT': spec['ports']['grafana'],
        'GF_SERVER_ROOT_URL': f'http://127.0.0.1:{spec["ports"]["grafana"]}', 'GF_SECURITY_ADMIN_USER': 'researcher',
        'GF_SECURITY_ADMIN_PASSWORD__FILE': '/run/secrets/grafana_admin_password', 'GF_AUTH_ANONYMOUS_ENABLED': 'false',
        'GF_USERS_ALLOW_SIGN_UP': 'false', 'GF_ANALYTICS_REPORTING_ENABLED': 'false',
        'GF_ANALYTICS_CHECK_FOR_UPDATES': 'false', 'GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES': 'false',
        'GF_NEWS_NEWS_FEED_ENABLED': 'false', 'GF_PLUGINS_PREINSTALL_DISABLED': 'true',
        'GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH': '/campaign-dashboards/campaign.json'}
    grafana = run_container(spec, state, owner, 'grafana', pins['grafana/grafana'], [],
        [(state / 'grafana', '/var/lib/grafana', False),
         (state / 'config/provisioning', '/etc/grafana/provisioning', True),
         (state / 'config/dashboards', '/campaign-dashboards', True),
         (password, '/run/secrets/grafana_admin_password', True)], environment)
    build = wait_http(spec, 'prometheus', '/api/v1/status/buildinfo')
    ready = wait_http(spec, 'prometheus', '/api/v1/query?query=up&timeout=2s')
    if ready.get('status') != 'success' or ready.get('data', {}).get('resultType') != 'vector':
        raise ValueError('Prometheus real query engine is not ready')
    grafana_health = wait_http(spec, 'grafana', '/api/health')
    dashboard = wait_http(spec, 'grafana', '/api/dashboards/uid/' + DASHBOARD, True)
    if dashboard.get('dashboard', {}).get('uid') != DASHBOARD or grafana_health.get('database') != 'ok':
        raise ValueError('Grafana real database/dashboard is not ready')
    assert_owned_running(spec, owner, [prometheus, grafana])
    identity = assert_prometheus_identity(spec)
    atomic_json(state / 'ui-readiness.json', {'prometheus_build': build, 'query': ready,
        'grafana_health': grafana_health, 'dashboard_uid': DASHBOARD, 'prometheus_identity': identity})
    return [prometheus, grafana]


@startup_guard
@bounded(150)
def start_collector(spec):
    started = time.monotonic()
    spec = validate_spec(spec)
    budget(spec)
    if not Path(spec['results_root']).is_dir():
        raise ValueError('collector results_root must exist on coordinator')
    preflight_ports(spec, collector_ports(spec))
    state, owner = state_for(spec, create=True)
    identities = sources(state)
    targets = configure_collector(spec, state)
    selection = state / 'benchmark'
    selection.mkdir(mode=0o755, exist_ok=True)
    atomic_json(selection / 'selection.json', {'job': None, 'results': None, 'terminal': None}, 0o444)
    atomic_json(selection / 'exporter.json', {'schema_version': 1, 'mode': 'benchmark', 'run_id': spec['run_id'],
                'selection': '/selection/selection.json', 'stale_seconds': 15}, 0o444)
    pins = images()
    initial = receipt(started, complete=False, run_id=spec['run_id'],
        started_timestamp_seconds=time.time() - (time.monotonic() - started), containers=[],
        images=pins, targets=targets, source_sha256=identities,
        placement={'kind': 'controller-cohost', 'overhead_calibrated': False, 'state_dir': str(state),
                   'host_topology': 'logical-loopback-shared-kernel-and-disks' if spec.get('allow_loopback_hosts') else 'admitted-private-fleet',
                   'root_filesystem_device': state.stat().st_dev, 'root_free_bytes_at_start': shutil.disk_usage(state).free,
                   'retention_size_is_hard_disk_quota': False, 'retention_time': '7d', 'retention_size': '2GB'},
        expected_case_ids=[], seen_case_ids=[], selected_case_ids=[], terminal_observations=[],
        gaps=['collector startup did not finish'], **access(spec))
    atomic_json(state / 'session.json', initial)
    exporter = run_container(spec, state, owner, 'benchmark', pins['library/python'],
        ['python3', '-B', '/app/campaign_exporter.py', '--config', '/selection/exporter.json',
         '--listen', f'127.0.0.1:{spec["ports"]["benchmark"]}'],
        [(state / 'source', '/app', True), (selection, '/selection', True),
         (spec['results_root'], spec['results_root'], True)])
    wait_endpoint(spec, '127.0.0.1', spec['ports']['benchmark'], '/healthz')
    assert_owned_running(spec, owner, [exporter])
    containers = [exporter, *start_ui(spec, state, owner, pins)]
    assert_owned_running(spec, owner, containers)
    answer = {**initial, **receipt(started), 'containers': containers}
    atomic_json(state / 'session.json', answer)
    return answer


def validate_job(job):
    import campaign_exporter
    campaign_exporter.context('th-validation', job)
    if job is None:
        raise ValueError('case selection requires explicit job')


def result_path(spec, job, result_dir):
    path = absolute(str(result_dir))
    if not path.is_relative_to(Path(spec['results_root'])) or path.name != 'result' or path.parent.name != job['case_id']:
        raise ValueError('result must be results_root/.../CASE_ID/result for selected case')
    if not path.parent.is_dir():
        raise ValueError('case parent must exist before selection')
    return path


def select_case(spec, job, result_dir, processes=None):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, _ = state_for(spec)
    validate_job(job)
    path = result_path(spec, job, result_dir)
    session = load_json(state / 'session.json')
    previous = load_json(state / 'benchmark/selection.json').get('job')
    if previous and previous['case_id'] != job['case_id'] and previous['case_id'] not in session['seen_case_ids']:
        raise ValueError('previous case has no terminal Prometheus observation; refusing selection overwrite')
    atomic_json(state / 'benchmark/selection.json', {'job': job, 'results': str(path), 'terminal': None}, 0o444)
    if job['case_id'] not in session['expected_case_ids']:
        session['expected_case_ids'].append(job['case_id'])
        session['selected_case_ids'].append(job['case_id'])
    atomic_json(state / 'session.json', session)
    if processes is not None:
        select_host(spec, -1, job, processes)
    return receipt(started, case_id=job['case_id'], results=str(path), outside_api_timing=True)


def select_host(spec, index, job, processes):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, _ = state_for(spec)
    host_for(spec, index)
    if job is not None:
        validate_job(job)
    if not isinstance(processes, list) or len(processes) > 8:
        raise ValueError('at most eight explicit cgroups allowed')
    seen = set()
    for process in processes:
        if set(process) != {'role', 'cgroup'} or not TOKEN.fullmatch(process['role']):
            raise ValueError('invalid process metadata')
        path = PurePosixPath(process['cgroup'])
        if '..' in path.parts or not path.is_absolute() or str(path) != process['cgroup']:
            raise ValueError('cgroup must be a canonical absolute cgroupv2 path')
        if not (str(path).startswith('/system.slice/') or re.fullmatch(r'/docker/system.slice/docker-[a-f0-9]{12,64}\.scope', str(path))):
            raise ValueError('cgroup must be an explicitly admitted system.slice unit')
        if process['role'] in seen:
            raise ValueError('duplicate process role')
        seen.add(process['role'])
    atomic_json(state / f'host-{index}/selection.json', {'job': job, 'processes': processes}, 0o444)
    return receipt(started, host_index=index, processes=processes,
                   availability='explicit' if processes else 'unavailable: no admitted cgroups')


@bounded(2.5)
def finish_case(spec, job, result_dir, *, metrics=None, status='completed', write_profile_enabled=None,
                read_evidence=None, resource_evidence=None, write_diagnostics=None):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, _ = state_for(spec)
    validate_job(job)
    path = result_path(spec, job, result_dir)
    if status not in ('completed', 'failed'):
        raise ValueError('terminal status must be completed or failed')
    if write_profile_enabled is not None and type(write_profile_enabled) is not bool:
        raise ValueError('write profile evidence must be observed boolean or absent')
    if read_evidence is not None and not isinstance(read_evidence, dict):
        raise ValueError('read evidence must be observed counter object or absent')
    selected = load_json(state / 'benchmark/selection.json')
    if selected['job'] != job or selected['results'] != str(path):
        raise ValueError('terminal job/result must match current atomic selection')
    terminal = {'status': status, 'metrics': metrics}
    if write_profile_enabled is not None:
        terminal['write_profile_enabled'] = write_profile_enabled
    if read_evidence is not None:
        terminal['read_evidence'] = read_evidence
    if resource_evidence is not None:
        terminal['resource_evidence'] = resource_evidence
    if write_diagnostics is not None:
        terminal['write_diagnostics'] = write_diagnostics
    import campaign_exporter
    campaign_exporter.terminal_metrics(terminal, {})
    selected['terminal'] = terminal
    published_at = time.time()
    atomic_json(state / 'benchmark/selection.json', selected, 0o444)
    labels = '{run_id=' + json.dumps(spec['run_id']) + ',case_id=' + json.dumps(job['case_id']) + '}'
    metric = ('kv_observer_snapshot_final' if status == 'completed' else 'kv_observer_case_terminal') + labels
    status_labels = labels[:-1] + ',status=' + json.dumps(status) + '}'
    expression = (metric + ' == 1 and on(run_id,case_id) kv_observer_case_status' + status_labels
                  + ' == 1 and on(run_id,case_id) (timestamp(kv_observer_case_terminal' + labels
                  + ') >= ' + str(published_at) + ')')
    until = time.monotonic() + budget(spec, 2.5)
    observed, evidence, errors = False, None, []
    while time.monotonic() < until:
        try:
            answer = query(spec, expression)
            evidence = answer
            if answer.get('resultType') == 'vector' and len(answer['result']) == 1 and float(answer['result'][0]['value'][1]) == 1:
                observed = True
                break
        except (OSError, ValueError, TimeoutError) as error:
            errors.append(type(error).__name__)
        time.sleep(min(0.1, max(0, until - time.monotonic())))
    answer = receipt(started, complete=observed, case_id=job['case_id'], terminal_status=status,
        final_observed=observed, query=expression, evidence=evidence, outside_api_timing=True,
        profiling='observed_enabled' if write_profile_enabled else 'observed_disabled' if write_profile_enabled is False else 'unavailable',
        gaps=[] if observed else ['terminal snapshot not scraped before bounded observation deadline'], errors=errors[-3:])
    session = load_json(state / 'session.json')
    if observed and job['case_id'] not in session['seen_case_ids']:
        session['seen_case_ids'].append(job['case_id'])
    session['terminal_observations'].append(answer)
    atomic_json(state / 'session.json', session)
    return answer


def access(spec):
    state, _ = state_for(spec)
    replay_info = load_json(state / 'replay.json') if (state / 'replay.json').exists() else None
    suffix = '?' + urlencode({'var-run_id': spec['run_id']})
    if replay_info:
        recorded = replay_info['time_range']
        suffix = '?' + urlencode({'from': int(recorded['start'] * 1000), 'to': int(recorded['end'] * 1000), 'var-run_id': spec['run_id']})
    return {'prometheus_url': f'http://127.0.0.1:{spec["ports"]["prometheus"]}',
            'grafana_url': f'http://127.0.0.1:{spec["ports"]["grafana"]}/d/{DASHBOARD}' + suffix,
            'grafana_user': 'researcher', 'password_file': str(state / 'grafana_admin_password'),
            'access': 'loopback only; use private SSM port forwarding on cloud',
            'offline_original_timeline': bool(replay_info)}


def interpolate(expression, spec, case_id, duration):
    variables = {'run_id': spec['run_id'], 'case_id': case_id, 'requested_backend': '.*',
        'host_index': '.*', 'host_role': '.*', 'phase': 'measurement', 'device': '.*', 'instance': '.*',
        'operation': '.*', 'outcome': 'success', 'quantile': '.*', '__rate_interval': '20s', '__interval': '1s',
        '__interval_ms': '1000', '__range': str(max(1, math.ceil(duration))) + 's',
        '__range_s': str(max(1, math.ceil(duration))), '__range_ms': str(max(1000, math.ceil(duration * 1000)))}
    def replace(match):
        name = match.group(1) or match.group(2)
        if name not in variables:
            raise ValueError('unsupported dashboard variable: ' + name)
        return variables[name]
    rendered = re.sub(r'\$\{([A-Za-z_][A-Za-z_0-9]*)(?::[^}]+)?\}|\$([A-Za-z_][A-Za-z_0-9]*)', replace, expression)
    return rendered


@bounded(180)
def verify(spec):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, _ = state_for(spec)
    offline = (state / 'replay.json').exists()
    session = load_json(state / ('replay.json' if offline else 'session.json'))
    time_range = session['time_range'] if offline else {'start': session['started_timestamp_seconds'], 'end': time.time()}
    end, start = time_range['end'], time_range['start']
    seen = session.get('seen_case_ids', [])
    case_id = seen[-1] if seen else 'idle'
    evidence, gaps = {}, []
    if offline:
        gaps.extend('capture gap: ' + gap for gap in session.get('source_capture', {}).get('gaps', []))
    else:
        gaps.extend('terminal observation deadline missed: ' + item['case_id']
                    for item in session.get('terminal_observations', []) if not item.get('complete'))
    try:
        targets = http(spec, 'prometheus', '/api/v1/targets')['data']['activeTargets']
        evidence['targets'] = targets
        if offline and targets:
            gaps.append('offline Prometheus unexpectedly has live targets')
        if not offline:
            configuration = load_json(state / 'config/prometheus.json')
            admitted = {(job['job_name'], address): group['labels']
                        for job in configuration['scrape_configs'] for group in job['static_configs']
                        for address in group['targets']}
            actual = {(target.get('labels', {}).get('job'),
                       target.get('discoveredLabels', {}).get('__address__')) for target in targets}
            if actual != set(admitted) or len(targets) != len(admitted):
                gaps.append('active scrape targets differ from explicit admitted fleet')
            for target in targets:
                key = (target.get('labels', {}).get('job'), target.get('discoveredLabels', {}).get('__address__'))
                labels = target.get('labels', {})
                if target.get('health') != 'up' or any(labels.get(name) != value for name, value in admitted.get(key, {}).items()):
                    gaps.append('explicit target unhealthy or host labels differ: ' + str(key))
        captured_pages = session.get('query_evidence') if offline else None
        if offline and not captured_pages:
            # Legacy archives retain their original query scope and hashes, including
            # missing hashes/gaps. Never manufacture a replacement baseline.
            anchors = {}
            for metric in QUERY_METRICS[:7]:
                expression = metric + '{run_id=' + json.dumps(spec['run_id']) + '}'
                anchors[metric] = query(spec, expression + '[' + str(max(1, math.ceil(end - start))) + 's]', timestamp=end)
            actual_seen = terminal_cases(anchors['kv_observer_case_terminal'])
            hashes = {name: query_digest(value) for name, value in anchors.items()}
            evidence['historical_query_sha256'] = hashes
            if not session.get('query_sha256') or hashes != session['query_sha256']:
                gaps.append('historical samples differ from retained capture query evidence')
        else:
            pages, actual_seen = collect_query_evidence(spec, state, time_range, captured_pages)
            evidence['query_evidence'] = pages
            if offline and pages != captured_pages:
                gaps.append('historical samples differ from retained capture query evidence')
        missing = set(session.get('expected_case_ids', [])) - actual_seen
        if missing:
            gaps.append('expected terminal cases missing from TSDB: ' + ','.join(sorted(missing)))
        evidence['actual_seen_case_ids'] = sorted(case for case in actual_seen if case)
        source = http(spec, 'grafana', '/api/datasources/uid/' + DATASOURCE, authenticated=True)
        if source.get('type') != 'prometheus' or source.get('url') != f'http://127.0.0.1:{spec["ports"]["prometheus"]}':
            raise ValueError('Grafana datasource identity/URL mismatch')
        health = http(spec, 'grafana', '/api/datasources/uid/' + DATASOURCE + '/health', authenticated=True)
        if health.get('status') != 'OK':
            raise ValueError('authenticated Grafana datasource health failed')
        dashboard = http(spec, 'grafana', '/api/dashboards/uid/' + DASHBOARD, authenticated=True)['dashboard']
        if dashboard.get('uid') != DASHBOARD:
            raise ValueError('Grafana dashboard identity mismatch')
        queue, panels, count = list(dashboard.get('panels', [])), [], 0
        while queue:
            panel = queue.pop(0)
            count += 1
            if count + len(queue) > 128:
                raise ValueError('dashboard exceeds bounded panel count')
            queue.extend(panel.get('panels', []))
            requests = []
            for target in panel.get('targets', []):
                if target.get('hide') or not target.get('expr'):
                    continue
                if len(requests) >= 16 or len(target['expr']) > 16384:
                    raise ValueError('dashboard query exceeds bound')
                source = target.get('datasource', panel.get('datasource', {}))
                if source.get('type') != 'prometheus' or source.get('uid') != DATASOURCE:
                    raise ValueError('dashboard target references unexpected datasource')
                requests.append({'refId': target['refId'], 'expr': interpolate(target['expr'], spec, case_id, end - start),
                    'datasource': {'type': 'prometheus', 'uid': DATASOURCE}, 'intervalMs': 1000,
                    'maxDataPoints': 600, 'format': target.get('format', 'time_series'), 'instant': bool(target.get('instant')),
                    'range': not bool(target.get('instant'))})
            if not requests:
                continue
            if len({request['refId'] for request in requests}) != len(requests):
                raise ValueError('dashboard panel contains duplicate query reference IDs')
            result = http(spec, 'grafana', '/api/ds/query',
                          {'from': str(int(start * 1000)), 'to': str(int(end * 1000)), 'queries': requests}, authenticated=True)
            if not isinstance(result.get('results'), dict):
                raise ValueError('Grafana panel query has no results')
            points = {}
            for request in requests:
                if request['refId'] not in result['results']:
                    raise ValueError('Grafana omitted requested panel query reference')
                response = result['results'].get(request['refId'], {})
                if response.get('error') or response.get('status', 200) != 200:
                    raise ValueError('Grafana panel query failed')
                points[request['refId']] = sum(
                    sum(value is not None for value in column)
                    for frame in response.get('frames', [])
                    for field, column in zip(frame.get('schema', {}).get('fields', []),
                                             frame.get('data', {}).get('values', []))
                    if field.get('type') == 'number')
            panels.append({'id': panel.get('id'), 'queries': requests, 'points': points})
        if not panels:
            raise ValueError('Grafana dashboard has no executable panels')
        evidence['panels'] = panels
        evidence['datasource_health'] = health['status']
        anchor_expression = ('last_over_time(kv_observer_case_terminal{run_id=' + json.dumps(spec['run_id'])
                             + ',case_id=' + json.dumps(case_id) + '}[' + str(max(1, math.ceil(end - start))) + 's])')
        if seen:
            anchor = http(spec, 'grafana', '/api/ds/query',
                {'from': str(int(start * 1000)), 'to': str(int(end * 1000)), 'queries': [{
                    'refId': 'ObservedTerminal', 'expr': anchor_expression, 'instant': True, 'range': False,
                    'datasource': {'uid': DATASOURCE, 'type': 'prometheus'}, 'maxDataPoints': 1, 'intervalMs': 1000}]},
                authenticated=True)
            observed = anchor.get('results', {}).get('ObservedTerminal', {})
            values = [value for frame in observed.get('frames', [])
                      for field, column in zip(frame.get('schema', {}).get('fields', []), frame.get('data', {}).get('values', []))
                      if field.get('type') == 'number' for value in column if value is not None]
            if observed.get('error') or observed.get('status', 200) != 200 or values != [1]:
                raise ValueError('authenticated Grafana terminal-observation anchor missing or incorrect')
            evidence['grafana_terminal_anchor'] = anchor
    except (OSError, ValueError, KeyError, TimeoutError) as error:
        gaps.append(type(error).__name__ + ': ' + str(error)[:250])
    answer = receipt(started, complete=not gaps, gaps=gaps, time_range=time_range, evidence=evidence,
                     offline=offline, original_timestamps=True, visual_verification=False)
    atomic_json(state / 'verification.json', answer)
    return answer


def query_digest(value):
    normalized = dict(value)
    normalized['result'] = sorted(value.get('result', []), key=lambda row: json.dumps(row.get('metric', {}), sort_keys=True))
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def terminal_cases(value):
    return {row['metric'].get('case_id') for row in value.get('result', [])
            if any(float(sample) == 1 for _, sample in row.get('values', []))}


def collect_query_evidence(spec, state, time_range, captured=None):
    """Retain exact raw range vectors in bounded, hash-addressed (start,end] pages."""
    start, end = time_range['start'], time_range['end']
    lower = end - max(1, math.ceil(end - start))
    directory = state / 'query-evidence'
    directory.mkdir(mode=0o700, exist_ok=True)
    seen, pages = set(), []
    if captured is not None:
        validate_query_evidence(captured, time_range)
        plan = [(page['metric'], page['start'], page['end']) for page in captured['pages']]
    else:
        plan = []
        for metric in QUERY_METRICS:
            cursor = lower
            while cursor < end:
                boundary = min(end, cursor + QUERY_PAGE_SECONDS)
                plan.append((metric, cursor, boundary))
                cursor = boundary

    def retain(metric, left, right):
        check_deadline()
        expression = metric + '{run_id=' + json.dumps(spec['run_id']) + '}'
        try:
            # Whole-second page boundaries also work with older PromQL duration
            # parsers. Preserve the original fractional evaluation timestamp.
            duration = round(right - left)
            value = query(spec, expression + '[' + str(duration) + 's]', timestamp=right)
            if value.get('resultType') != 'matrix' or not isinstance(value.get('result'), list):
                raise ValueError('raw query evidence must be a range vector')
            value = dict(value, result=sorted(value['result'], key=lambda row: json.dumps(row.get('metric', {}), sort_keys=True)))
            if len((json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()) > MAX_JSON:
                raise ResponseTooLarge('normalized query page exceeds size bound')
            content_hash = query_digest(value)
            filename = content_hash + '.json'
            atomic_json(directory / filename, value, compact=True)
        except ResponseTooLarge:
            if captured is not None or right - left <= 1:
                raise
            middle = left + max(1, round(right - left) // 2)
            retain(metric, left, middle)
            retain(metric, middle, right)
            return
        if len(pages) >= MAX_MEMBERS - 1000:
            raise ValueError('query evidence page count exceeds archive reserve')
        pages.append({'metric': metric, 'start': left, 'end': right, 'query_sha256': content_hash,
                      'file': filename, 'sha256': digest(directory / filename),
                      'bytes': (directory / filename).stat().st_size})
        if metric == 'kv_observer_case_terminal':
            seen.update(terminal_cases(value))

    for metric, left, right in plan:
        retain(metric, left, right)
    result = {'schema_version': 2, 'page_interval': '(start,end]', 'pages': pages}
    validate_query_evidence(result, time_range)
    return result, seen


def validate_query_evidence(evidence, time_range):
    if (not isinstance(evidence, dict) or evidence.get('schema_version') != 2
            or evidence.get('page_interval') != '(start,end]' or not isinstance(evidence.get('pages'), list)
            or not 0 < len(evidence['pages']) < MAX_MEMBERS):
        raise ValueError('invalid bounded query evidence index')
    end = time_range['end']
    lower = end - max(1, math.ceil(end - time_range['start']))
    cursors = {metric: lower for metric in QUERY_METRICS}
    for page in evidence['pages']:
        if not isinstance(page, dict):
            raise ValueError('query page index row must be an object')
        metric = page.get('metric')
        if (metric not in cursors or page.get('start') != cursors[metric]
                or not isinstance(page.get('end'), (int, float)) or not math.isfinite(page['end'])
                or not page['start'] < page['end'] <= end
                or page['end'] - page['start'] > QUERY_PAGE_SECONDS
                or not re.fullmatch(r'[a-f0-9]{64}', str(page.get('query_sha256', '')))
                or page.get('file') != page['query_sha256'] + '.json'
                or not re.fullmatch(r'[a-f0-9]{64}', str(page.get('sha256', '')))
                or type(page.get('bytes')) is not int or not 0 < page['bytes'] <= MAX_JSON):
            raise ValueError('invalid query page identity, size or coverage')
        cursors[metric] = page['end']
    if any(cursor != end for cursor in cursors.values()):
        raise ValueError('query evidence has missing metric/time coverage')


def regular_files(root):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('archive root must be an existing ordinary directory')
    files, total = [], 0
    for directory, dirs, names in os.walk(root, followlinks=False):
        check_deadline()
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise ValueError('archive directories must not be symlinks')
        for name in names:
            path = Path(directory) / name
            if path.is_symlink() or not path.is_file():
                raise ValueError('archive requires ordinary files only')
            total += path.stat().st_size
            files.append(path)
            if len(files) > MAX_MEMBERS or total > MAX_BYTES:
                raise ValueError('archive exceeds validation bound (not a TSDB disk quota)')
    return sorted(files)


@bounded(300)
def export_session(spec, destination):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, _ = state_for(spec)
    destination = absolute(str(destination))
    sidecar = Path(str(destination) + '.receipt.json')
    if (destination.exists() or sidecar.exists() or sidecar.is_symlink()
            or not destination.parent.is_dir() or destination.is_relative_to(state)):
        raise ValueError('export requires a new destination and receipt outside observer state')
    session = load_json(state / 'session.json') if (state / 'session.json').exists() else {}
    time_range = {'start': session.get('started_timestamp_seconds', time.time()), 'end': time.time()}
    gaps, snapshot_name, complete = list(session.get('gaps', [])), None, False
    staging = Path(tempfile.mkdtemp(prefix='capture-', dir=state))
    try:
        try:
            verification = verify(spec, timeout_seconds=min(180, budget(spec, 180)))
            time_range = verification['time_range']
            if not verification['complete']:
                gaps.extend(verification['gaps'])
        except (OSError, ValueError, TimeoutError) as error:
            verification = {'complete': False, 'gaps': [type(error).__name__]}
            gaps.extend(verification['gaps'])
        try:
            response = http(spec, 'prometheus', '/api/v1/admin/tsdb/snapshot?skip_head=false', method='POST', maximum=30)
            snapshot_name = response.get('data', {}).get('name')
            if response.get('status') != 'success' or not isinstance(snapshot_name, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', snapshot_name):
                raise ValueError('Prometheus did not return successful snapshot identity')
            snapshot = state / 'prometheus/snapshots' / snapshot_name
            files = regular_files(snapshot)
            if not files or not any(path.name == 'meta.json' for path in files):
                raise ValueError('snapshot contains no actual TSDB blocks')
            metadata = [load_json(path) for path in files if path.name == 'meta.json']
            tsdb_range = {'start': min(item['minTime'] for item in metadata) / 1000,
                          'end_exclusive': max(item['maxTime'] for item in metadata) / 1000}
            snapshot_bytes = sum(path.stat().st_size for path in files)
            if shutil.disk_usage(destination.parent).free < snapshot_bytes + 128 * 1024**2:
                raise ValueError('insufficient archive destination disk reserve')
            shutil.copytree(snapshot, staging / 'snapshot', copy_function=os.link)
            complete = True
        except (OSError, ValueError, KeyError, TypeError, TimeoutError) as error:
            gaps.append('TSDB snapshot unavailable: ' + type(error).__name__ + ': ' + str(error)[:200])
        evidence = staging / 'evidence'
        evidence.mkdir()
        capture = {'schema_version': 1, 'run_id': spec['run_id'], 'time_range': time_range,
            'expected_case_ids': session.get('expected_case_ids', []), 'seen_case_ids': session.get('seen_case_ids', []),
            'snapshot': {'name': snapshot_name, 'skip_head': False, 'complete': complete,
                         'tsdb_time_range': tsdb_range if complete else None},
            'placement': session.get('placement'),
            'containers': [load_json(path) for path in sorted(state.glob('container-*.json'))[:16]],
            'source_sha256': session.get('source_sha256', {}), 'images': session.get('images', {}),
            'targets': session.get('targets', {}), 'terminal_observations': session.get('terminal_observations', []),
            'query_sha256': verification.get('evidence', {}).get('historical_query_sha256', {}),
            'query_evidence': verification.get('evidence', {}).get('query_evidence'),
            'complete': complete and not gaps, 'gaps': gaps, 'bounds': {'uncompressed_bytes': MAX_BYTES, 'members': MAX_MEMBERS},
            'raw_latency_csv': 'authoritative; not synthesized from TSDB', 'observer_overhead_calibrated': False}
        atomic_json(evidence / 'capture.json', capture)
        atomic_json(evidence / 'verification.json', verification)
        if capture['query_evidence']:
            # Only referenced pages enter the archive; prior verification runs may
            # have other end timestamps and must not accumulate in the receipt.
            page_directory = evidence / 'query-evidence'
            page_directory.mkdir()
            for page in capture['query_evidence']['pages']:
                source = state / 'query-evidence' / page['file']
                if digest(source) != page['sha256'] or source.stat().st_size != page['bytes']:
                    raise ValueError('query artifact changed before capture')
                target = page_directory / page['file']
                if not target.exists():
                    os.link(source, target)
        if (state / 'ui-readiness.json').exists():
            atomic_json(evidence / 'ui-readiness.json', load_json(state / 'ui-readiness.json'))
        if (state / 'config').exists():
            shutil.copytree(state / 'config', staging / 'config')
        if (state / 'source').exists():
            shutil.copytree(state / 'source', staging / 'source')
        files = regular_files(staging)
        manifest = {'schema_version': 1, 'files': {str(path.relative_to(staging)): {'sha256': digest(path), 'bytes': path.stat().st_size} for path in files}}
        atomic_json(staging / 'manifest.json', manifest)
        budget(spec, 30)
        fd, temporary = tempfile.mkstemp(prefix='.' + destination.name, dir=destination.parent)
        os.close(fd)
        try:
            with tarfile.open(temporary, 'w:gz', dereference=True) as archive:
                for path in [*files, staging / 'manifest.json']:
                    budget(spec, 30)
                    info = archive.gettarinfo(str(path), arcname=str(path.relative_to(staging)))
                    # Metadata also excludes workstation usernames and source ownership.
                    info.uid = info.gid = 0
                    info.uname = info.gname = ''
                    with path.open('rb') as stream:
                        archive.addfile(info, DeadlineReader(stream))
            if Path(temporary).stat().st_size > MAX_BYTES:
                raise ValueError('compressed archive exceeds bound')
            os.chmod(temporary, 0o600)
            archive_sha256 = digest(temporary)
            manifest_sha256 = digest(staging / 'manifest.json')
            # A concurrent writer cannot make us overwrite an older result.
            os.link(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        answer = receipt(started, complete=capture['complete'], gaps=gaps, destination=str(destination),
                         sha256=archive_sha256, manifest_sha256=manifest_sha256,
                         bytes=destination.stat().st_size, time_range=time_range, snapshot=capture['snapshot'])
        atomic_json(str(destination) + '.receipt.json', answer)
        atomic_json(state / 'export-receipt.json', answer)
        return answer
    finally:
        shutil.rmtree(staging)
        if snapshot_name and re.fullmatch(r'[A-Za-z0-9_-]{1,100}', snapshot_name):
            shutil.rmtree(state / 'prometheus/snapshots' / snapshot_name, ignore_errors=True)


def stop_services(spec, services):
    started = time.monotonic()
    spec = validate_spec(spec)
    state, owner = state_for(spec)
    removed, gaps = [], []
    for service in services:
        name = container_name(spec, owner, service)
        try:
            found = docker(spec, 'ps', '-a', '--filter', 'name=^/' + name + '$', '--format', '{{.ID}}', maximum=4, cleanup=True)
            if not found:
                continue
            detail = json.loads(docker(spec, 'inspect', name, maximum=4, cleanup=True))[0]
            labels = detail.get('Config', {}).get('Labels', {})
            if labels.get(LABEL) != spec['run_id'] or labels.get(LABEL + '.owner') != owner['token']:
                raise ValueError('refusing to remove container without exact session labels')
            docker(spec, 'rm', '--force', detail['Id'], maximum=8, cleanup=True)
            removed.append(name)
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            gaps.append(service + ': ' + type(error).__name__)
    answer = receipt(started, complete=not gaps, gaps=gaps, removed=removed, retained_state=str(state),
                     processes='all session services are exact-owned containers; no detached host processes')
    atomic_json(state / ('stop-' + services[0] + '.json'), answer)
    return answer


@bounded(30)
def stop_collector(spec):
    return stop_services(spec, ['grafana', 'prometheus', 'benchmark'])


@bounded(20)
def stop_host(spec, index):
    host_for(validate_spec(spec), index)
    return stop_services(spec, [f'host-{index}', f'node-{index}'])


def unpack_archive(archive_path, target):
    archive_path = absolute(str(archive_path))
    external = load_json(str(archive_path) + '.receipt.json')
    if archive_path.stat().st_size > MAX_BYTES or digest(archive_path) != external['sha256']:
        raise ValueError('archive SHA-256 does not match retained receipt')
    total, names = 0, set()
    with tarfile.open(archive_path, 'r:gz') as archive:
        for count, member in enumerate(archive, 1):
            check_deadline()
            path = PurePosixPath(member.name)
            if (count > MAX_MEMBERS or not member.isfile() or path.is_absolute() or '..' in path.parts
                    or str(path) != member.name or member.name in names or not path.parts
                    or path.parts[0] not in {'manifest.json', 'snapshot', 'evidence', 'config', 'source'}):
                raise ValueError('unsafe or excessive archive member')
            total += member.size
            if total > MAX_BYTES:
                raise ValueError('restored data exceeds validation bound')
            names.add(member.name)
            destination = target / member.name
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            stream = archive.extractfile(member)
            with destination.open('xb') as output:
                while True:
                    check_deadline()
                    block = stream.read(1024 * 1024)
                    if not block:
                        break
                    output.write(block)
            destination.chmod(0o644)
    manifest_path = target / 'manifest.json'
    if digest(manifest_path) != external['manifest_sha256']:
        raise ValueError('archive manifest SHA-256 does not match receipt')
    manifest = load_json(manifest_path)
    if names != set(manifest['files']) | {'manifest.json'}:
        raise ValueError('archive member set differs from manifest')
    for name, entry in manifest['files'].items():
        path = target / name
        if path.stat().st_size != entry['bytes'] or digest(path) != entry['sha256']:
            raise ValueError('archive member hash/size mismatch')
    capture = load_json(target / 'evidence/capture.json')
    if capture.get('query_evidence') is not None:
        validate_query_evidence(capture['query_evidence'], capture['time_range'])
        for page in capture['query_evidence']['pages']:
            path = target / 'evidence/query-evidence' / page['file']
            if (digest(path) != page['sha256'] or path.stat().st_size != page['bytes']
                    or query_digest(load_json(path)) != page['query_sha256']):
                raise ValueError('query page does not match captured evidence index')
    return capture


@startup_guard
@bounded(180)
def replay(archive, state_dir, ports=None, deadline_utc=None):
    started = time.monotonic()
    state = absolute(str(state_dir))
    if state.exists():
        raise ValueError('offline replay requires fresh nonexistent owned state')
    # Validate before allocating replay state; extraction never preserves original ownership.
    with tempfile.TemporaryDirectory(prefix='kv-replay-validate-', dir=state.parent) as temporary:
        staging = Path(temporary)
        capture = unpack_archive(archive, staging)
        if not capture['snapshot']['complete'] or not (staging / 'snapshot').is_dir():
            raise ValueError('archive declares no replayable TSDB snapshot')
        time_range = capture['time_range']
        if not (0 < time_range['start'] <= time_range['end'] <= time.time() + 60):
            raise ValueError('invalid historical capture time range')
        if deadline_utc is None:
            deadline_utc = datetime.fromtimestamp(time.time() + 1800, timezone.utc).isoformat()
        spec = {'schema_version': 1, 'run_id': capture['run_id'], 'state_dir': str(state),
                'results_root': str(staging), 'deadline_utc': deadline_utc,
                'hosts': [{'index': -1, 'address': '127.0.0.1', 'role': 'coordinator'}],
                'ports': ports or {'prometheus': 29090, 'grafana': 23000, 'benchmark': 29899, 'node': 29100, 'host': 29898},
                'allow_loopback_hosts': True}
        # Empty owned placeholder lives inside replay state; never mounted or scraped.
        spec['results_root'] = str(state / 'offline-results')
        state, owner = state_for(spec, create=True)
        (state / 'offline-results').mkdir(mode=0o700)
        shutil.copytree(staging / 'snapshot', state / 'prometheus')
        for path in regular_files(state / 'prometheus'):
            path.chmod(0o644)
        # Current UID owns copied blocks, so nonroot Prometheus can open/write them.
        dashboard = load_json(staging / 'config/dashboards/campaign.json')
        configure_collector(spec, state, offline=True, dashboard=dashboard, time_range=time_range)
        offline = {'time_range': time_range, 'seen_case_ids': capture['seen_case_ids'],
                   'expected_case_ids': capture['expected_case_ids'], 'archive_sha256': digest(archive),
                   'query_sha256': capture.get('query_sha256', {}),
                   'query_evidence': capture.get('query_evidence'),
                   'original_timestamps': True, 'live_scrape_targets': [], 'source_capture': capture}
        atomic_json(state / 'replay.json', offline)
        # Retention is relative to wall clock, not replay end; include historical age + 30d.
        retention_days = max(30, math.ceil((time.time() - time_range['start']) / 86400) + 30)
        pins = capture['images']
        if pins != images():
            # Use exact archived digest pins, not today's tags; validate their shape.
            if set(pins) != set(images()) or any(not re.fullmatch(r'[A-Za-z0-9_./:-]+@sha256:[a-f0-9]{64}', pin) for pin in pins.values()):
                raise ValueError('invalid captured image identity set')
        containers = start_ui(spec, state, owner, pins, retention=str(retention_days) + 'd', retention_size='8GB')
        result = verify(spec)
        assert_owned_running(spec, owner, containers)
        answer = receipt(started, complete=result['complete'], gaps=result['gaps'], spec=spec,
                         containers=containers, verification=result, time_range=time_range,
                         retention_days=retention_days, **access(spec))
        atomic_json(state / 'replay-receipt.json', answer)
        if not result['complete']:
            raise RuntimeError('offline replay verification failed; see private replay-receipt.json')
        return answer


class DeadlineReader:
    def __init__(self, stream):
        self.stream = stream

    def read(self, size=-1):
        check_deadline()
        return self.stream.read(size)


@bounded(900)
def pull_images(spec):
    """Explicit online preparation; no start/replay ever pulls implicitly."""
    spec = validate_spec(spec)
    resolved = {}
    for image in images().values():
        docker(spec, 'pull', image, maximum=240)
        resolved[image] = json.loads(docker(spec, 'image', 'inspect', image))[0]['Id']
    return {'complete': True, 'images': resolved, 'registry_access': 'explicit preparation only'}


@startup_guard
@bounded(300)
def start_local(spec):
    spec = validate_spec(spec)
    if not spec['allow_loopback_hosts']:
        raise ValueError('start is local loopback qualification only; cloud starts hosts individually')
    endpoints = collector_ports(spec)
    endpoints += [(host['address'], spec['ports'][service])
                  for host in spec['hosts'] for service in ('node', 'host')]
    preflight_ports(spec, endpoints)
    hosts = [start_host(spec, host['index']) for host in spec['hosts']]
    answer = start_collector(spec)
    answer['local_hosts'] = hosts
    return answer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', type=Path)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('start', 'start-collector', 'stop', 'stop-collector', 'verify', 'access', 'pull'):
        commands.add_parser(name)
    for name in ('start-host', 'stop-host', 'select-host'):
        command = commands.add_parser(name)
        command.add_argument('--index', type=int, required=True)
        if name == 'select-host':
            command.add_argument('--job', type=Path)
            command.add_argument('--processes', type=Path, required=True)
    for name in ('select-case', 'finish-case'):
        command = commands.add_parser(name)
        command.add_argument('--job', type=Path, required=True)
        command.add_argument('--result-dir', type=Path, required=True)
        if name == 'finish-case':
            command.add_argument('--metrics', type=Path)
            command.add_argument('--status', choices=('completed', 'failed'), default='completed')
            command.add_argument('--write-profile-enabled', choices=('true', 'false'))
            command.add_argument('--read-evidence', type=Path)
            command.add_argument('--resource-evidence', type=Path)
            command.add_argument('--write-diagnostics', type=Path)
    command = commands.add_parser('export')
    command.add_argument('--destination', type=Path, required=True)
    command = commands.add_parser('replay')
    command.add_argument('--archive', type=Path, required=True)
    command.add_argument('--state-dir', type=Path, required=True)
    command.add_argument('--ports', type=Path)
    command.add_argument('--deadline-utc')
    args = parser.parse_args()
    try:
        if args.command == 'replay':
            answer = replay(args.archive, args.state_dir, load_json(args.ports) if args.ports else None, args.deadline_utc)
        else:
            if args.spec is None:
                parser.error('--spec is required except for replay')
            spec = validate_spec(load_json(args.spec))
            if args.command == 'pull':
                answer = pull_images(spec)
            elif args.command == 'start':
                answer = start_local(spec)
            elif args.command == 'start-collector':
                answer = start_collector(spec)
            elif args.command == 'start-host':
                answer = start_host(spec, args.index)
            elif args.command in ('stop', 'stop-collector'):
                answer = stop_collector(spec)
                if args.command == 'stop' and spec.get('allow_loopback_hosts'):
                    answer['hosts'] = [stop_host(spec, host['index']) for host in spec['hosts']]
                    answer['complete'] = answer['complete'] and all(host['complete'] for host in answer['hosts'])
            elif args.command == 'stop-host':
                answer = stop_host(spec, args.index)
            elif args.command == 'select-case':
                answer = select_case(spec, load_json(args.job), args.result_dir)
            elif args.command == 'select-host':
                answer = select_host(spec, args.index, load_json(args.job) if args.job else None, load_json(args.processes))
            elif args.command == 'finish-case':
                answer = finish_case(spec, load_json(args.job), args.result_dir,
                    metrics=load_json(args.metrics) if args.metrics else None, status=args.status,
                    write_profile_enabled=None if args.write_profile_enabled is None else args.write_profile_enabled == 'true',
                    read_evidence=load_json(args.read_evidence) if args.read_evidence else None,
                    resource_evidence=load_json(args.resource_evidence) if args.resource_evidence else None,
                    write_diagnostics=load_json(args.write_diagnostics) if args.write_diagnostics else None)
            elif args.command == 'export':
                answer = export_session(spec, args.destination)
            elif args.command == 'verify':
                answer = verify(spec)
            else:
                answer = access(spec)
        print(json.dumps(answer, allow_nan=False))
        return 0 if answer.get('complete', True) else 1
    except (OSError, ValueError, KeyError, RuntimeError, TimeoutError, subprocess.SubprocessError) as error:
        print(json.dumps({'complete': False, 'status': 'error', 'error_type': type(error).__name__,
                          'error': str(error)[:400]}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
