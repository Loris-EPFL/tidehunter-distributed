"""GCP runtime identities and immutable, source-bound guest mailbox."""
import base64
import hashlib
import ipaddress
import json
import re
import time
from datetime import datetime, timezone
import sys
from pathlib import Path
from pathlib import PurePosixPath


class RuntimeSafetyError(RuntimeError):
    pass


def _deadline(value):
    if not isinstance(value, str):
        raise RuntimeSafetyError('absolute UTC deadline required')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise RuntimeSafetyError('invalid deadline') from exc
    if result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise RuntimeSafetyError('deadline must be UTC')
    return result.timestamp()


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise RuntimeSafetyError('exact SHA-256 required')
    return value


def _run(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', value):
        raise RuntimeSafetyError('invalid run ID')
    return value


def _fleet(fleet):
    if not isinstance(fleet, list) or len(fleet) != 6:
        raise RuntimeSafetyError('exact six-host fleet required')
    ids, names, addresses = set(), set(), set()
    for index, host in enumerate(fleet):
        if not isinstance(host, dict) or type(host.get('index')) is not int or host['index'] != index or host.get('role') != ('coordinator' if index == 0 else 'owner'):
            raise RuntimeSafetyError('invalid fleet position or role')
        if not re.fullmatch(r'[0-9]+', str(host.get('id', ''))) or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', str(host.get('name', ''))):
            raise RuntimeSafetyError('invalid GCE identity')
        if not all(isinstance(host.get(k), str) and host[k] for k in ('zone', 'internal_ip', 'rdma_ip')):
            raise RuntimeSafetyError('fleet network identity missing')
        if not re.fullmatch(r'[a-z]+(?:-[a-z0-9]+)+-[a-z]', host['zone']):
            raise RuntimeSafetyError('invalid GCE zone')
        try:
            ips = [ipaddress.IPv4Address(host[k]) for k in ('internal_ip', 'rdma_ip')]
        except ipaddress.AddressValueError as exc:
            raise RuntimeSafetyError('fleet requires IPv4 addresses') from exc
        if any(ip.is_loopback or ip.is_multicast or ip.is_unspecified for ip in ips):
            raise RuntimeSafetyError('invalid fleet interface address')
        addresses.update(map(str, ips))
        ids.add(str(host['id'])); names.add(host['name'])
    if len(ids) != 6 or len(names) != 6 or len(addresses) != 12 or len({h['zone'] for h in fleet}) != 1:
        raise RuntimeSafetyError('fleet identities must be distinct and zonal')
    return fleet


def validate_runtime(config):
    if not isinstance(config, dict):
        raise RuntimeSafetyError('runtime config must be an object')
    if isinstance(config.get('platform'), dict):
        return validate_controller_config(config)
    for name in ('project_id', 'bucket'):
        if not isinstance(config.get(name), str) or not config[name]:
            raise RuntimeSafetyError('missing ' + name)
    _run(config.get('run_id')); _sha(config.get('source_sha256')); _fleet(config.get('fleet'))
    if _deadline(config.get('deadline_utc')) <= time.time():
        raise RuntimeSafetyError('runtime has expired')
    if not time.time() < _deadline(config.get('campaign_deadline_utc')) <= _deadline(config['deadline_utc']) - 60:
        raise RuntimeSafetyError('campaign deadline must precede VM expiry by at least 60 seconds')
    if not re.fullmatch(r'[0-9]+', str(config.get('project_number', ''))):
        raise RuntimeSafetyError('numeric project number required')
    if not isinstance(config.get('package'), dict) or not isinstance(config.get('spec'), dict):
        raise RuntimeSafetyError('package and spec required')
    package = config['package']
    if (package.get('source_sha256') != config['source_sha256']
        or package.get('object_key') != 'runs/' + config['run_id'] + '/package.tar'
        or not str(package.get('generation', '')).isdigit()
        or not re.fullmatch(r'[0-9a-f]{40}', str(package.get('source_revision', '')))):
        raise RuntimeSafetyError('package source, revision or immutable object identity mismatch')
    _sha(package.get('sha256'))
    backends = config['spec'].get('backends', [])
    if not isinstance(backends, list) or not set(backends) <= {'native', 'tidehunter_tcp', 'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma', 'tikv', 'cassandra', 'foundationdb', 'valkey_tcp'}:
        raise RuntimeSafetyError('invalid GCP backend selection')
    if not isinstance(config.get('machine_type'), str) or not config['machine_type']:
        raise RuntimeSafetyError('machine type required')
    if config.get('package_root') != '/mnt/tidehunter/' + config['run_id'] + '/package':
        raise RuntimeSafetyError('invalid installed package root')
    return config


def build_runtime_config(launch_config, fleet, package_receipt, spec, deadline_utc, **optional):
    run_id = optional.pop('run_id', launch_config.get('run_id'))
    source_sha256 = package_receipt.get('source_sha256')
    campaign_deadline = optional.pop('campaign_deadline_utc', None)
    if campaign_deadline is None:
        campaign_deadline = datetime.fromtimestamp(_deadline(deadline_utc) - 120, timezone.utc).isoformat()
    if optional:
        raise RuntimeSafetyError('unknown runtime options: ' + ', '.join(sorted(optional)))
    config = {'project_id': launch_config['project_id'], 'project_number': str(launch_config['project_number']),
              'bucket': launch_config['bucket'], 'machine_type': launch_config['machine_type'],
              'run_id': run_id, 'fleet': fleet, 'source_sha256': source_sha256,
              'deadline_utc': deadline_utc, 'campaign_deadline_utc': campaign_deadline,
              'package': package_receipt, 'spec': spec,
              'package_root': '/mnt/tidehunter/' + str(run_id) + '/package'}
    return validate_runtime(config)


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def _object_get(store, key, generation=None, max_bytes=4 * 1024 * 1024):
    return store.get(key, generation=generation, max_bytes=max_bytes)


def _put_once(store, key, data):
    try:
        return store.put(key, data, content_type='application/json', if_generation_match=0)
    except Exception as exc:
        if getattr(exc, 'status', None) != 412:
            raise
        head = store.head(key)
        if _object_get(store, key, generation=head['generation']) != data:
            raise RuntimeSafetyError('immutable operation ID already has different contents') from exc
        return head


class RemoteMailbox:
    """Immutable requests and terminal receipts addressed to exact GCE instances."""
    def __init__(self, api, bucket, run_id, fleet, source_sha256, deadline_utc):
        self.store = api.objects(bucket)
        self.run_id = _run(run_id)
        self.fleet = _fleet(fleet)
        self.source_sha256 = _sha(source_sha256)
        self.deadline = _deadline(deadline_utc)
        self.deadline_utc = deadline_utc

    def submit(self, index, argv, *, operation_id, timeout_seconds, env=None, cwd=None):
        if type(index) is not int or index not in range(6):
            raise RuntimeSafetyError('invalid fleet index')
        if not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', operation_id):
            raise RuntimeSafetyError('invalid operation ID')
        if type(timeout_seconds) is not int or timeout_seconds < 1:
            raise RuntimeSafetyError('positive integer timeout required')
        if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or '\x00' in x for x in argv):
            raise RuntimeSafetyError('invalid argv')
        if env is not None and (not isinstance(env, dict) or any(not isinstance(k, str) or not isinstance(v, str) or '\x00' in k + v for k, v in env.items())):
            raise RuntimeSafetyError('invalid environment')
        if cwd is not None and (not isinstance(cwd, str) or not PurePosixPath(cwd).is_absolute() or '..' in PurePosixPath(cwd).parts):
            raise RuntimeSafetyError('invalid working directory')
        now = time.time()
        if now >= self.deadline:
            raise RuntimeSafetyError('run deadline reached')
        host = self.fleet[index]
        request = {'schema_version': 1, 'run_id': self.run_id, 'source_sha256': self.source_sha256,
                   'instance_id': str(host['id']), 'index': index, 'operation_id': operation_id,
                   'deadline_utc': self.deadline_utc, 'timeout_seconds': timeout_seconds,
                   'argv': argv, 'env': env or {}, 'cwd': cwd}
        raw = _encode(request)
        key = f'runs/{self.run_id}/mailbox/{host["id"]}/requests/{operation_id}.json'
        metadata = _put_once(self.store, key, raw)
        return {'key': key, 'generation': str(metadata['generation']), 'request_sha256': hashlib.sha256(raw).hexdigest(),
                'instance_id': str(host['id']), 'index': index, 'operation_id': operation_id}

    def wait(self, reference, *, timeout_seconds):
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise RuntimeSafetyError('positive wait timeout required')
        index = reference.get('index')
        if type(index) is not int or index not in range(6) or str(self.fleet[index]['id']) != reference.get('instance_id'):
            raise RuntimeSafetyError('reference host mismatch')
        op = reference.get('operation_id')
        if not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', op):
            raise RuntimeSafetyError('invalid operation reference')
        expected = f'runs/{self.run_id}/mailbox/{reference["instance_id"]}/requests/{op}.json'
        if reference.get('key') != expected:
            raise RuntimeSafetyError('reference key mismatch')
        request = _object_get(self.store, expected, generation=reference['generation'])
        if hashlib.sha256(request).hexdigest() != reference.get('request_sha256'):
            raise RuntimeSafetyError('request generation identity mismatch')
        key = expected.replace('/requests/', '/responses/')
        end = min(self.deadline, time.time() + timeout_seconds)
        while time.time() < end:
            try:
                meta = self.store.head(key)
                receipt = json.loads(_object_get(self.store, key, generation=meta['generation']))
            except Exception as exc:
                if getattr(exc, 'status', None) != 404:
                    raise
                time.sleep(min(0.5, max(0, end - time.time())))
                continue
            if (not isinstance(receipt, dict) or receipt.get('request_sha256') != reference['request_sha256']
                or receipt.get('instance_id') != reference['instance_id'] or receipt.get('operation_id') != op
                or receipt.get('source_sha256') != self.source_sha256 or type(receipt.get('exit_code')) is not int
                or not isinstance(receipt.get('stdout'), str) or not isinstance(receipt.get('stderr'), str)):
                raise RuntimeSafetyError('terminal receipt provenance mismatch')
            return {**receipt, 'object_key': key, 'generation': str(meta['generation'])}
        raise TimeoutError('terminal response missing before deadline; operation outcome unknown')


def validate_controller_config(config):
    import cloud_campaign
    import cloud_controller
    import cloud_layout
    p = config['platform']
    if p.get('name') != 'gcp':
        raise RuntimeSafetyError('invalid provider')
    validate_runtime(p['runtime'])
    runtime = p['runtime']
    if (config.get('account') != runtime['project_number'] or config.get('prefix') != runtime['run_id']
        or config.get('bucket') != runtime['bucket']):
        raise RuntimeSafetyError('project, bucket or run identity mismatch')
    if (config.get('source_sha256') != runtime['source_sha256'] or config.get('deadline_utc') != runtime['deadline_utc']
        or config.get('source_revision') != runtime['package']['source_revision']):
        raise RuntimeSafetyError('source revision or deadline mismatch')
    cloud_layout.validate_fleet(config['fleet'])
    hosts = [config['fleet']['coordinator'], *config['fleet']['nodes']]
    for admitted, actual in zip(runtime['fleet'], hosts):
        if (actual.get('instance_id') != admitted['name'] or actual.get('numeric_id') != str(admitted['id'])
            or actual.get('address') != admitted['internal_ip'] or actual.get('rdma_address') != admitted['rdma_ip']):
            raise RuntimeSafetyError('controller fleet differs from GCE identities')
    for name, width in (('source_sha256', 64), ('binary_sha256', 64), ('binary_blake2b512', 128), ('source_revision', 40)):
        if not isinstance(config.get(name), str) or not re.fullmatch('[0-9a-f]{%d}' % width, config[name]):
            raise RuntimeSafetyError('invalid ' + name)
    root = Path(runtime['package_root'])
    if (config.get('repository_root') != str(root / 'source') or config.get('binary') != str(root / 'runtime/bin/kv-research')
        or config.get('fdb_library_dir') != str(root / 'runtime/lib')):
        raise RuntimeSafetyError('package paths differ from sealed layout')
    allowed = {'native', 'tidehunter_tcp', 'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma',
               'tikv', 'cassandra', 'foundationdb', 'valkey_tcp'}
    if not set(config['spec']['backends']) <= allowed:
        raise RuntimeSafetyError('unsupported GCP backend or AWS EFA selected')
    jobs = cloud_campaign.make_matrix(config['spec'])
    cloud_controller.validate_read_tier_memory(jobs, config['fleet'])
    return config


def verify_local_identity(config, index):
    import gcp_worker
    host = gcp_worker.identify(config['platform']['runtime'])
    if host['index'] != index + 1:
        raise RuntimeSafetyError('local metadata does not match requested host')
    return {'project_id': config['platform']['runtime']['project_id'],
            'project_number': config['platform']['runtime']['project_number'],
            'instance_id': str(host['id']), 'name': host['name'], 'zone': host['zone']}


def make_clients(config, *, guest=False, api=None):
    import gcp_api
    r = config['platform']['runtime']
    api = api or gcp_api.GoogleApi(r['project_id'], token_provider=gcp_api.metadata_token if guest else None)
    return {'objects': api.objects(r['bucket']), 'platform': api,
            'mailbox': RemoteMailbox(api, r['bucket'], r['run_id'], r['fleet'], r['source_sha256'], r['deadline_utc'])}


def verify_fleet(config, clients, *, terminating=False, starting=False):
    r = config['platform']['runtime']
    observed = {}
    for host in r['fleet']:
        instance = clients['platform'].request('GET', f'beta/zones/{host["zone"]}/instances/{host["name"]}')
        meta = {item['key']: item.get('value') for item in instance.get('metadata', {}).get('items', [])}
        schedule = instance.get('scheduling', {})
        networks = instance.get('networkInterfaces', [])
        scratch = [disk for disk in instance.get('disks', []) if disk.get('type') == 'SCRATCH' and disk.get('interface') == 'NVME']
        policy = r['run_id'] + '-placement'
        allowed = ('RUNNING', 'PROVISIONING', 'STAGING') if starting else ('RUNNING',)
        if terminating:
            allowed = ('RUNNING', 'PROVISIONING', 'STAGING', 'STOPPING', 'TERMINATED')
        if (str(instance.get('id')) != str(host['id']) or instance.get('name') != host['name']
            or instance.get('status') not in allowed or instance.get('labels', {}).get('bench-run') != r['run_id']
            or instance.get('labels', {}).get('bench-source') != r['source_sha256'][:63]
            or meta.get('bench-source') != r['source_sha256'] or meta.get('bench-run') != r['run_id']
            or _deadline(meta.get('bench-expiry')) != _deadline(r['deadline_utc'])
            or schedule.get('instanceTerminationAction') != 'DELETE'
            or _deadline(schedule.get('terminationTime')) != _deadline(r['deadline_utc'])
            or instance.get('machineType', '').split('/')[-1] != r['machine_type']
            or len(networks) != 2 or networks[0].get('networkIP') != host['internal_ip']
            or networks[1].get('networkIP') != host['rdma_ip']
            or networks[0].get('nicType') != 'GVNIC' or networks[1].get('nicType') != 'IRDMA'
            or len(scratch) != 1 or not any(item.split('/')[-1] == policy for item in instance.get('resourcePolicies', []))):
            raise RuntimeSafetyError('live GCE instance differs from admitted identity, network or expiry')
        observed[host['name']] = {'status': instance['status'], 'id': str(instance['id']), 'name': host['name']}
    return observed


def request_remote(controller, index, request, *, case='startup', timeout=180, export=False):
    import cloud_controller
    if index == -1:
        controller.guard()
        return cloud_controller.guest_operation(controller.config, {**request, 'index': index}, controller.clients)
    available = controller.remaining(export=export)
    timeout = min(int(timeout), int(available))
    if timeout < 1:
        raise RuntimeSafetyError('no time for remote operation')
    request = {**request, 'index': index}
    if request.get('action') == 'layout':
        request['operation_timeout'] = timeout
        if request.get('operation') == 'probe':
            request['timeout'] = min(request['timeout'], timeout)
    with controller.sequence_lock:
        controller.sequence += 1
        number = controller.sequence
    op = f'kv-{number:08d}'
    key = f'{controller.config["prefix"]}/results/cells/{case}/storage/{index}/{number:06d}-{request["action"]}.json'
    payload = {'execution_schema_version': 1, 'deadline_epoch': min(_deadline(controller.config['deadline_utc']), time.time() + timeout),
               'request': request, 'key': key}
    encode = lambda value: base64.b64encode(json.dumps(value).encode()).decode()
    script = Path(controller.config['repository_root']) / 'experiments/kv-comparison/cloud_controller.py'
    argv = ['/usr/bin/python3', str(script), 'guest', '--config-base64', encode(controller.config), '--request-base64', encode(payload)]
    controller.guard()
    reference = controller.clients['mailbox'].submit(index + 1, argv, operation_id=op, timeout_seconds=timeout)
    receipt = controller.clients['mailbox'].wait(reference, timeout_seconds=timeout)
    if receipt['exit_code'] != 0:
        raise RuntimeSafetyError('guest RPC process failed: ' + receipt['stderr'])
    store = controller.clients['objects']
    head = store.head(key)
    result = json.loads(store.get(key, generation=head['generation'], max_bytes=32 * 1024 * 1024))
    if (result.get('request') != request or result.get('instance_id') != controller.ids[index + 1]
        or result.get('status') != 'completed' or 'result' not in result):
        raise RuntimeSafetyError('guest RPC failed or receipt identity mismatch: ' + str(result.get('error')))
    return result['result']


def terminate(config, clients):
    return {'external_lifecycle_owner': True, 'termination_requested': False,
            'reason': 'fleet lifecycle owner deletes instances after all selected suites'}


def monitoring_access(config, proofs=()):
    return {'provider': 'gcp', 'scope': 'private fleet only',
            'hosts': [{'name': h['name'], 'address': h['internal_ip']} for h in config['platform']['runtime']['fleet']]}


def admit_fabrics(config, index, clients, root):
    import subprocess
    requested = 'tidehunter_nvme_rdma' in config['spec']['backends']
    evidence = {'provider': 'gcp', 'nvme_rdma_requested': requested}
    if requested:
        for module in ('nvme-rdma', 'nvmet-rdma'):
            subprocess.run(['modprobe', module], check=True, timeout=30)
        links = subprocess.check_output(['rdma', 'link', 'show'], text=True, timeout=10)
        host = config['platform']['runtime']['fleet'][index + 1]
        interfaces = json.loads(subprocess.check_output(['ip', '-j', 'address', 'show'], text=True, timeout=10))
        names = [item.get('ifname') for item in interfaces
                 if any(address.get('local') == host['rdma_ip'] for address in item.get('addr_info', []))]
        if len(names) != 1 or not isinstance(names[0], str) or not re.search(r'\bnetdev\s+' + re.escape(names[0]) + r'\b', links):
            raise RuntimeSafetyError('RDMA device is not bound to the admitted IRDMA address')
        evidence.update(rdma_links=links, network_interface=names[0], address=host['rdma_ip'])
    return evidence


def bootstrap_controller(controller):
    import cloud_controller
    identity = verify_local_identity(controller.config, -1)
    controller.guard(starting=True)
    controller.persist('startup.json', {'status': 'starting', 'identity': identity,
                                         'config': controller.config})
    controller.package = cloud_controller.verify_package(controller.config)
    end = controller.clock() + min(600, controller.remaining())
    while controller.clock() < end:
        current = controller.guard(starting=True)
        if all(host['status'] == 'RUNNING' for host in current.values()):
            break
        time.sleep(2)
    else:
        raise RuntimeSafetyError('exact fleet did not reach running state')
    proofs = controller.all_hosts({'action': 'bootstrap'}, timeout=300)
    cloud_controller.validate_physical_proofs([proof['snapshot']['nvme'] for proof in proofs], provider='gcp')
    cloud_controller.dump(controller.root / 'bootstrap-proofs.json', proofs)
    controller.persist('startup.json', {'status': 'admitted', 'identity': identity,
                                        'config': controller.config, 'package': controller.package,
                                        'bootstrap_proofs_sha256': cloud_controller.digest(controller.root / 'bootstrap-proofs.json')})
    controller.start_monitoring()
    controller.observe('access_receipt', lambda: cloud_controller.dump(
        controller.root / 'observability/access.json', monitoring_access(controller.config, proofs)), retain_receipt=False)


def bootstrap_guest(config, index, clients):
    import cloud_controller
    import cloud_package
    import local
    import cloud_bootstrap
    import subprocess
    identity = verify_local_identity(config, index)
    verify_fleet(config, clients)
    package = cloud_package.verify_directory(Path(config['platform']['runtime']['package_root']), runtime=True, provider='gcp')
    if package['source_sha256'] != config['source_sha256'] or package['binary_sha256'] != config['binary_sha256']:
        raise RuntimeSafetyError('package identity differs from controller')
    unit = 'tidehunter-expire-' + config['prefix'] + '.timer'
    subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True, timeout=10)
    images = {name: json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True, timeout=30))
              for name, image in local.IMAGES.items()}
    fabrics = admit_fabrics(config, index, clients, Path(config['fleet']['data_root']))
    monitor_images = cloud_bootstrap.cache_monitor_images(config['repository_root'], 5 if index == -1 else index,
        datetime.fromtimestamp(_deadline(config['deadline_utc']), timezone.utc))
    return {'identity': identity, 'package': package, 'images': images, 'fabric_admission': fabrics,
            'snapshot': cloud_controller.snapshot(config), 'monitor_images': monitor_images}


def _controller_config(runtime):
    root = Path(runtime['package_root'])
    manifest = json.loads((root / 'manifest.json').read_text())
    package = runtime['package']
    if manifest['source_sha256'] != runtime['source_sha256'] or manifest['revision'] != package['source_revision']:
        raise RuntimeSafetyError('runtime manifest source mismatch')
    raw_hosts = runtime['fleet']
    def host(item):
        return {'instance_id': item['name'], 'numeric_id': str(item['id']), 'address': item['internal_ip'],
                'rdma_address': item['rdma_ip']}
    nodes = [{**host(item), 'index': i} for i, item in enumerate(raw_hosts[1:])]
    region = raw_hosts[0]['zone'].rsplit('-', 1)[0]
    fleet = {'schema_version': 1, 'run_id': runtime['run_id'], 'provider': 'gcp', 'replication_factor': 1,
             'qualification_only': False, 'data_root': '/mnt/tidehunter-kv-comparison/' + runtime['run_id'],
             'region': region, 'instance_type': runtime['machine_type'], 'coordinator': host(raw_hosts[0]), 'nodes': nodes,
             'server_cpus': 8, 'client_cpus': 8, 'server_memory_mib': 16384, 'client_memory_mib': 16384}
    extra = {'suite', 'sui_fixture', 'sui_verification', 'sui_transports'}
    spec = {k: v for k, v in runtime['spec'].items() if k not in extra}
    return {'fleet': fleet, 'bucket': runtime['bucket'], 'bucket_region': region, 'prefix': runtime['run_id'],
            'account': runtime['project_number'], 'source_revision': manifest['revision'],
            'source_sha256': runtime['source_sha256'], 'binary_sha256': manifest['binary_sha256'],
            'binary_blake2b512': manifest['binary_blake2b512'], 'repository_root': str(root / 'source'),
            'binary': str(root / 'runtime/bin/kv-research'), 'fdb_library_dir': str(root / 'runtime/lib'),
            'deadline_utc': runtime['deadline_utc'], 'spec': spec,
            'platform': {'name': 'gcp', 'runtime': runtime}}


def execute_campaign(runtime_config, *, api=None):
    import tarfile
    import gcp_api
    validate_runtime(runtime_config)
    runtime = runtime_config
    sys.path.insert(0, str(Path(runtime['package_root']) / 'source/experiments/kv-comparison'))
    import gcp_worker
    if gcp_worker.identify(runtime)['index'] != 0:
        raise RuntimeSafetyError('campaign may only run on the admitted coordinator')
    api = api or gcp_api.GoogleApi(runtime['project_id'], token_provider=gcp_api.metadata_token)
    mailbox = RemoteMailbox(api, runtime['bucket'], runtime['run_id'], runtime['fleet'], runtime['source_sha256'], runtime['deadline_utc'])
    suite = runtime['spec'].get('suite', 'kv')
    if suite not in ('kv', 'sui', 'both'):
        raise RuntimeSafetyError('unknown suite')
    results = {}
    if suite in ('kv', 'both'):
        import cloud_controller
        config = _controller_config(runtime)
        clients = make_clients(config, guest=True, api=api)
        report = cloud_controller.Controller(config, clients).run()
        results['kv'] = {key: report[key] for key in ('complete', 'planned_cases', 'completed_cases')}
    if suite in ('sui', 'both'):
        import gcp_sui
        root = Path('/mnt/tidehunter') / runtime['run_id'] / 'sui-results'
        try:
            sui_config = {**runtime, 'deadline_utc': runtime['campaign_deadline_utc']}
            results['sui'] = gcp_sui.run_suite(sui_config, mailbox, runtime['package'], runtime['spec']['sui_fixture'], output_root=root)
        finally:
            if root.exists():
                archive = root.parent / 'sui-results.tar.gz'
                with tarfile.open(archive, 'x:gz') as out:
                    for path in sorted(root.iterdir()):
                        if path.name != 'inputs':
                            out.add(path, arcname='sui-results/' + path.name)
                key = runtime['run_id'] + '/results/sui-results.tar.gz'
                receipt = api.objects(runtime['bucket']).upload(key, archive, if_generation_match=0)
                with archive.open('rb') as stream:
                    checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
                results['sui_archive'] = {'key': key, 'generation': str(receipt['generation']), 'sha256': checksum}
    return results


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != 'execute':
        raise SystemExit('usage: gcp_runtime.py execute RUNTIME_CONFIG')
    with open(sys.argv[2], encoding='utf-8') as stream:
        config = json.load(stream)
    print(json.dumps(execute_campaign(config), sort_keys=True))
