"""GCP runtime identities and immutable, source-bound guest mailbox."""
import copy
import base64
import hashlib
import ipaddress
import json
import os
import re
import resource
import time
from datetime import datetime, timezone
import sys
from pathlib import Path
from pathlib import PurePosixPath

from gcp_manual_storage import CredentialUnavailable

GUEST_PYTHON = '/opt/tidehunter-python/bin/python3'
GUEST_PATH = '/opt/tidehunter-python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'

# Google automatically attaches these NVMe Local SSDs for the machine type.
# https://docs.cloud.google.com/compute/docs/disks/local-ssd#performance
LOCAL_SSD_COUNTS = {'z4m-highmem-16': 1, 'z4m-highmem-32': 2, 'h4d-highmem-192-lssd': 10}
# Conservative MiB floors from the published VM RAM, keyed by the admitted SKU.
MACHINE_RAM_MIB = {'z4m-highmem-16': 122_000_000_000 // 2**20,
                   'z4m-highmem-32': 244_000_000_000 // 2**20,
                   'h4d-highmem-192-lssd': 1523712}
REPLAY_HOST_RESERVE_MIB = 8 * 1024  # Kernel, services, and host page cache.
REPLAY_OBSERVED_PEAK_MIB = 16 * 1024  # P13 peaked at its cap; the uncapped peak is unknown.
REPLAY_CLIENT_TARGET_MIB = 32 * 1024
# A guest stops itself after this much time with no mailbox request and no
# coordinator driver. The value is recorded in runtime.json, bounded, and can be
# shortened only for the free local rehearsal. Thirty minutes covers the 5-7
# minute input staging of one dispatch plus an operator pause; the incident this
# guard addresses idled for five hours.
IDLE_STOP_SECONDS = 1800
IDLE_MIN_SECONDS = 60
IDLE_MAX_SECONDS = 3600
# The one campaign data root per run. `gcp_worker.prepare_storage` formats the
# guest's local NVMe SSD and mounts it at `/mnt/tidehunter-kv-comparison/<run>`
# before the worker serves the first request; that file is frozen image code, so
# the same literal lives there too. Campaign trees, owner WAL backing files and
# cell databases live under this root; anything left on the boot disk fills the
# 80 GB persistent disk.
RUN_DATA_ROOT = '/mnt/tidehunter-kv-comparison'
# The package and the mailbox gate live on the boot disk; campaign data roots
# belong to the mounted SSD.
GUEST_ROOT_PREFIXES = ('/mnt/tidehunter/', RUN_DATA_ROOT + '/')
# Placement label hashed into every campaign cell identity. A cell that passed
# while its data sat on the boot disk has another identity, so it reruns.
DATA_DEVICE = 'local-ssd'
RUN_ID_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z')


def run_data_root(run_id):
    """The one directory this run owns on every guest's local NVMe SSD."""
    if (not isinstance(run_id, str) or not RUN_ID_PATTERN.match(run_id)
            or '..' in run_id):
        raise RuntimeSafetyError('invalid campaign run id')
    return Path(RUN_DATA_ROOT) / run_id


def fleet_run_id(fleet_state):
    """The run id a `gcp_manual_compute.py` fleet state names, or None."""
    if fleet_state is None:
        return None
    try:
        state = json.loads(Path(fleet_state).read_text())
    except (OSError, ValueError) as exc:
        raise RuntimeSafetyError('fleet state is not readable') from exc
    if not isinstance(state, dict):
        raise RuntimeSafetyError('fleet state is not an object')
    return state.get('run_id')


def admit_data_root_path(path, run_id, label):
    """Resolve one declared data path and prove it is inside the run's SSD root."""
    root = run_data_root(run_id)
    try:
        resolved = Path(os.path.abspath(str(path)))
        resolved.relative_to(root)
    except (OSError, ValueError):
        raise RuntimeSafetyError(
            f'{label} {path} is outside the run data root {root}: the campaign '
            'stores everything on the guest local NVMe SSD') from None
    return resolved


def replay_client_memory_mib(machine_type, requested=None):
    available = MACHINE_RAM_MIB.get(machine_type, 0) - REPLAY_HOST_RESERVE_MIB
    cap = min(REPLAY_CLIENT_TARGET_MIB, available) if requested is None else requested
    if type(cap) is not int or not REPLAY_OBSERVED_PEAK_MIB < cap <= available:
        raise RuntimeSafetyError(
            f'REPLAY_HOST_MEMORY_UNAVAILABLE: {machine_type} replay cap must exceed the '
            f'{REPLAY_OBSERVED_PEAK_MIB} MiB observed peak and fit the '
            f'{available} MiB available after host reserve')
    return cap


def admit_replay_memory(policy, jobs, source, machine_type):
    import replay_memory

    if (not isinstance(policy, dict) or not policy or set(policy) - {'cap_mib', 'workloads'}
            or ('cap_mib' in policy and type(policy['cap_mib']) is not int)):
        raise ValueError('replay_memory requires cap_mib and/or a workloads map')
    if not any(job.get('case_kind') == 'mainnet_replay' for job in jobs):
        raise ValueError('replay_memory requires a selected mainnet replay workload')
    cap = replay_client_memory_mib(machine_type, policy.get('cap_mib'))
    return cap, replay_memory.admit(policy.get('workloads', {}), jobs, source, cap * 1024**2)


def qualify_physical_trace(spec):
    import cloud_controller
    import cold_device_trace
    from gcp_image_prepare import require_kernel_btf

    if not any(cloud_controller.requests_cold_trace(job)
               for job in cloud_controller.controller_matrix(spec)):
        return None
    require_kernel_btf()
    return cold_device_trace.qualify_trace_ring()


class RuntimeSafetyError(RuntimeError):
    pass


def admit_guest_runtime():
    """Admit the interpreter and inherited hard limit used by guest operations."""
    if sys.executable != GUEST_PYTHON or sys.version_info < (3, 11):
        raise RuntimeSafetyError('guest orchestration requires ' + GUEST_PYTHON + ' (Python >= 3.11)')
    try:
        import yaml
        if yaml.__version__ != '6.0.2' or yaml.safe_load('x: 1') != {'x': 1}:
            raise ValueError('incorrect PyYAML version or behavior')
    except (ImportError, AttributeError, ValueError) as exc:
        raise RuntimeSafetyError('guest Python requires functional PyYAML 6.0.2') from exc
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 524288:
        raise RuntimeSafetyError('guest hard RLIMIT_NOFILE must permit 524288 descriptors')
    return {'executable': sys.executable, 'python': sys.version, 'yaml': yaml.__version__,
            'nofile_soft': soft, 'nofile_hard': hard}


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


def owner_host_counts():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
    from cloud_layout import OWNER_HOST_COUNTS
    return OWNER_HOST_COUNTS


def _fleet(fleet):
    if not isinstance(fleet, list) or len(fleet) - 1 not in owner_host_counts():
        raise RuntimeSafetyError('fleet must have one or five owner hosts')
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
    if len(ids) != len(fleet) or len(names) != len(fleet) or len(addresses) != 2 * len(fleet) or len({h['zone'] for h in fleet}) != 1:
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
    grace = config.get('poweroff_grace_seconds', 600)
    hard_cap = config.get('poweroff_hard_cap_seconds', 3600)
    if (type(grace) is not int or type(hard_cap) is not int or grace < 0
            or hard_cap < grace or hard_cap > 21600):
        raise RuntimeSafetyError('invalid guest poweroff timing')
    idle = config.get('idle_stop_seconds', IDLE_STOP_SECONDS)
    if type(idle) is not int or not IDLE_MIN_SECONDS <= idle <= IDLE_MAX_SECONDS:
        raise RuntimeSafetyError('invalid guest idle stop window')
    package = config['package']
    if (package.get('source_sha256') != config['source_sha256']
        or package.get('object_key') != 'runs/' + config['run_id'] + '/package.tar'
        or not str(package.get('generation', '')).isdigit()
        or not re.fullmatch(r'[0-9a-f]{40}', str(package.get('source_revision', '')))):
        raise RuntimeSafetyError('package source, revision or immutable object identity mismatch')
    _sha(package.get('sha256'))
    suite = config['spec'].get('suite')
    if suite not in ('kv', 'sui'):
        raise RuntimeSafetyError('invalid campaign suite')
    import workflow_input
    backends = config['spec'].get('backends', [])
    if (not isinstance(backends, list) or not backends
            or any(not isinstance(backend, str) or backend not in workflow_input.BACKENDS for backend in backends)
            or len(set(backends)) != len(backends)):
        raise RuntimeSafetyError('invalid GCP backend selection')
    sui = suite == 'sui'
    mode = config['spec'].get('mode', 'standard')
    admitted_modes = {'sui'} if sui else {'standard', 'standard-with-live-nvmeof', 'focused'}
    if mode not in admitted_modes:
        raise RuntimeSafetyError('invalid matrix mode')
    if sui:
        # The Sui campaign owns its transport admission in its own drivers; this
        # runtime only bounds the mailbox worker, so it must name the owner count.
        owners = config['spec'].get('owners')
        if type(owners) is not int or not 1 <= owners <= 16 or owners != len(config['fleet']) - 1:
            raise RuntimeSafetyError('Sui owner count differs from the fleet')
    if len(config['fleet']) == 2 and not sui:
        if mode != 'focused' or config['spec'].get('suite') != 'kv':
            raise RuntimeSafetyError('compact fleet requires a focused KV selection')
        forbidden = {'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'garnet', 'kvrocks'}
        if config['spec'].get('selection') != 'valkey_rdma_reads':
            forbidden.add('valkey_rdma')
        if set(backends) & forbidden:
            raise RuntimeSafetyError('backend requires five owner hosts')
        import workflow_input
        selection = config['spec'].get('selection')
        owners = config['spec'].get('owner_hosts', 5)
        if owners != 1 or (selection not in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5')
                and workflow_input.cloud_campaign.selection_owner_hosts(selection) != 1):
            raise RuntimeSafetyError('selection does not admit a compact fleet')
        import manual_campaign
        jobs = manual_campaign._reviewed_jobs(config['spec'])
        if any(workflow_input.cloud_campaign.job_owner_hosts(job) > 1 for job in jobs):
            raise RuntimeSafetyError('selection contains a multi-host owner job')
    if mode == 'focused':
        import manual_campaign
        manual_campaign._reviewed_jobs(config['spec'])
        if config['spec'].get('owner_hosts', 5) != len(config['fleet']) - 1:
            raise RuntimeSafetyError('reviewed selection and fleet size disagree')
    if mode == 'standard' and set(backends) & {'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'}:
        raise RuntimeSafetyError('NVMe backends require an NVMe-oF mode')
    if live_mode(config['spec']) and set(backends) & {'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'}:
        live = config.get('live_qualification')
        expected_fields = {'image', 'image_qualification', 'pair_hosts', 'rdma_device', 'service_account'}
        if not isinstance(live, dict) or set(live) != expected_fields:
            raise RuntimeSafetyError('generation-pinned live pair qualification is required')
        reference = live['image_qualification']
        if (not isinstance(reference, dict) or set(reference) != {'bucket', 'object', 'generation', 'sha256'}
                or reference.get('bucket') != config['bucket']
                or not re.fullmatch(r'runs/bench-[0-9a-f]{16}/qualification/image-receipt\.json', str(reference.get('object', '')))
                or not re.fullmatch(r'[1-9][0-9]*', str(reference.get('generation', '')))):
            raise RuntimeSafetyError('invalid live pair receipt reference')
        _sha(reference.get('sha256'))
        if (not re.fullmatch(r'projects/' + re.escape(config['project_id']) + r'/global/images/[a-z0-9-]+', str(live['image']))
                or not re.fullmatch(r'[A-Za-z0-9_]+', str(live['rdma_device']))
                or not isinstance(live['service_account'], str) or not live['service_account']):
            raise RuntimeSafetyError('invalid live qualification image or device identity')
        pair = live['pair_hosts']
        if not isinstance(pair, list) or len(pair) != 2:
            raise RuntimeSafetyError('live qualification requires two distinct guests')
        for host in pair:
            if (not isinstance(host, dict) or set(host) != {'name', 'id', 'zone', 'internal_ip', 'rdma_ip'}
                    or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', str(host['name']))
                    or not re.fullmatch(r'[1-9][0-9]*', str(host['id']))
                    or not re.fullmatch(r'[a-z]+(?:-[a-z0-9]+)+-[a-z]', str(host['zone']))):
                raise RuntimeSafetyError('invalid live pair guest identity')
            try:
                addresses = [ipaddress.IPv4Address(host[key]) for key in ('internal_ip', 'rdma_ip')]
            except (ValueError, TypeError) as exc:
                raise RuntimeSafetyError('invalid live pair addresses') from exc
            if any(ip.is_loopback or ip.is_multicast or ip.is_unspecified for ip in addresses):
                raise RuntimeSafetyError('invalid live pair addresses')
        pair_run = reference['object'].split('/')[1]
        if ({host['name'] for host in pair} != {pair_run + '-0', pair_run + '-1'}
                or str(pair[0]['id']) == str(pair[1]['id'])
                or pair[0]['zone'] != pair[1]['zone'] or pair[0]['zone'] != config['fleet'][0]['zone']
                or len({host[key] for host in pair for key in ('internal_ip', 'rdma_ip')}) != 4):
            raise RuntimeSafetyError('live pair guests and addresses must be distinct, bound to the receipt and zonal')
        if mode == 'focused' and any(
                any(str(pair[i][key]) != str(config['fleet'][i][key])
                    for key in ('name', 'id', 'zone', 'internal_ip', 'rdma_ip'))
                for i in range(2)):
            raise RuntimeSafetyError('focused live pair must match the campaign fleet')
    if not isinstance(config.get('machine_type'), str) or config['machine_type'] not in LOCAL_SSD_COUNTS:
        raise RuntimeSafetyError('machine type must have an admitted local SSD layout')
    policy = config.get('placement_policy')
    if policy is not None and (not isinstance(policy, dict)
            or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', str(policy.get('name', '')))
            or not re.fullmatch(r'[1-9][0-9]*', str(policy.get('id', '')))):
        raise RuntimeSafetyError('exact placement policy name and numeric ID required')
    if config.get('package_root') != '/mnt/tidehunter/' + config['run_id'] + '/package':
        raise RuntimeSafetyError('invalid installed package root')
    roots = config.get('guest_roots', [])
    package_root = config['package_root']
    if (not isinstance(roots, list) or len(roots) > 8 or len(set(roots)) != len(roots)
            or any(not isinstance(value, str)
                   or not value.startswith(GUEST_ROOT_PREFIXES)
                   or '..' in PurePosixPath(value).parts or value.endswith('/')
                   or value == package_root or value.startswith(package_root + '/')
                   for value in roots)):
        raise RuntimeSafetyError(
            'guest roots must be distinct directories under /mnt/tidehunter or '
            + RUN_DATA_ROOT)
    return config


def build_runtime_config(launch_config, fleet, package_receipt, spec, deadline_utc, **optional):
    run_id = optional.pop('run_id', launch_config.get('run_id'))
    source_sha256 = package_receipt.get('source_sha256')
    campaign_deadline = optional.pop('campaign_deadline_utc', None)
    policy = optional.pop('placement_policy', None)
    guest_roots = optional.pop('guest_roots', None)
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
    for key in ('poweroff_grace_seconds', 'poweroff_hard_cap_seconds'):
        if key in launch_config:
            config[key] = launch_config[key]
    # The guard runs on every guest, so its window is part of the immutable
    # runtime, not a worker default that could drift from the launch record.
    config['idle_stop_seconds'] = launch_config.get('idle_stop_seconds', IDLE_STOP_SECONDS)
    if 'live_qualification' in launch_config:
        config['live_qualification'] = launch_config['live_qualification']
    if policy is not None:
        config['placement_policy'] = dict(policy)
    if guest_roots is not None:
        config['guest_roots'] = list(guest_roots)
    return validate_runtime(config)


def live_mode(spec):
    from workflow_input import cloud_campaign
    return (spec.get('mode') == 'standard-with-live-nvmeof'
            or (spec.get('mode') == 'focused' and spec.get('selection') in cloud_campaign.FOCUSED_SELECTIONS))


def admit_package_profile(spec, package):
    expected = None
    if spec.get('mode') == 'focused':
        expected = 'focused_valkey' if spec.get('selection') == 'valkey_rdma_reads' else 'focused'
    if package.get('build_profile') != expected:
        raise RuntimeSafetyError('package build profile differs from campaign')


def admit_live_qualification(runtime):
    """Revalidate the immutable pair proof on the guest before live owner setup."""
    import gcp_api
    import gcp_image
    import manual_campaign
    validate_runtime(runtime)
    spec = runtime['spec']
    if (not live_mode(spec)
            or not set(spec['backends']) & {'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'}):
        raise RuntimeSafetyError('live qualification requested for a non-live campaign')
    binding = runtime['live_qualification']
    api = gcp_api.GoogleApi(runtime['project_id'], token_provider=gcp_api.metadata_token)
    image = api.request('GET', 'global/images/' + binding['image'].rsplit('/', 1)[-1])
    image_config = {'project_id': runtime['project_id'], 'bucket': runtime['bucket'],
                    'image': binding['image'], 'image_qualification': binding['image_qualification'],
                    'service_account': binding['service_account']}
    if spec.get('mode') == 'focused':
        image_config['qualification_profile'] = 'focused'
    qualification = gcp_image.admit_image(api, image_config, image)
    if qualification.get('runtime_sha256') != runtime['package'].get('manifest_sha256'):
        raise RuntimeSafetyError('live pair qualification does not bind this package manifest')
    gcp_image.admit_nvme_transports(qualification, spec['backends'], live=True)
    gcp_image.admit_spdk(qualification, spec)
    records, hosts = qualification['_records'], binding['pair_hosts']
    for index, record in enumerate(records):
        if (record.get('instance_id') != str(hosts[index]['id'])
                or record.get('peer_instance_id') != str(hosts[1 - index]['id'])
                or record.get('local_rdma') != hosts[index]['rdma_ip']
                or record.get('peer') != hosts[1 - index]['rdma_ip']):
            raise RuntimeSafetyError('live pair qualification host identity mismatch')
    pair_run_id = binding['image_qualification']['object'].split('/')[1]
    manual_campaign._admit_live_pair(records, hosts, spec, binding['rdma_device'], pair_run_id)


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

    def submit(self, index, argv, *, operation_id, timeout_seconds, env=None, cwd=None,
               stdin=None, stdout_object=None):
        if type(index) is not int or index not in range(len(self.fleet)):
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
        host = self.fleet[index]
        prefix = f'runs/{self.run_id}/transfer/{host["id"]}/{operation_id}/'
        if stdin is not None:
            if (not isinstance(stdin, dict) or set(stdin) != {'object', 'sha256', 'size'}
                    or stdin['object'] != prefix + 'stdin' or not _sha(str(stdin.get('sha256')))
                    or type(stdin['size']) is not int or stdin['size'] < 0):
                raise RuntimeSafetyError('invalid command stdin blob')
        if stdout_object is not None and stdout_object != prefix + 'stdout':
            raise RuntimeSafetyError('invalid command stdout blob')
        now = time.time()
        if now >= self.deadline:
            raise RuntimeSafetyError('run deadline reached')
        request = {'schema_version': 1, 'run_id': self.run_id, 'source_sha256': self.source_sha256,
                   'instance_id': str(host['id']), 'index': index, 'operation_id': operation_id,
                   'deadline_utc': self.deadline_utc, 'timeout_seconds': timeout_seconds,
                   'argv': argv, 'env': env or {}, 'cwd': cwd,
                   **({'stdin': stdin} if stdin is not None else {}),
                   **({'stdout_object': stdout_object} if stdout_object is not None else {})}
        raw = _encode(request)
        key = f'runs/{self.run_id}/mailbox/{host["id"]}/requests/{operation_id}.json'
        metadata = _put_once(self.store, key, raw)
        return {'key': key, 'generation': str(metadata['generation']), 'request_sha256': hashlib.sha256(raw).hexdigest(),
                'instance_id': str(host['id']), 'index': index, 'operation_id': operation_id}

    def wait(self, reference, *, timeout_seconds):
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise RuntimeSafetyError('positive wait timeout required')
        index = reference.get('index')
        if type(index) is not int or index not in range(len(self.fleet)) or str(self.fleet[index]['id']) != reference.get('instance_id'):
            raise RuntimeSafetyError('reference host mismatch')
        op = reference.get('operation_id')
        if not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', op):
            raise RuntimeSafetyError('invalid operation reference')
        expected = f'runs/{self.run_id}/mailbox/{reference["instance_id"]}/requests/{op}.json'
        if reference.get('key') != expected:
            raise RuntimeSafetyError('reference key mismatch')
        key = expected.replace('/requests/', '/responses/')
        end = min(self.deadline, time.time() + timeout_seconds)
        request_verified = False
        failures = 0
        backoff = 5
        while time.time() < end:
            try:
                if not request_verified:
                    request = _object_get(self.store, expected, generation=reference['generation'])
                    if hashlib.sha256(request).hexdigest() != reference.get('request_sha256'):
                        raise RuntimeSafetyError('request generation identity mismatch')
                    request_verified = True
                meta = self.store.head(key)
                receipt = json.loads(_object_get(self.store, key, generation=meta['generation']))
            except CredentialUnavailable:
                failures += 1
                print(f'credential attempt {failures} failed; re-authenticate gcloud', file=sys.stderr)
                time.sleep(min(backoff, max(0, end - time.time())))
                backoff = min(backoff * 2, 60)
                continue
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
    if len(runtime['fleet']) != len(hosts):
        raise RuntimeSafetyError('controller fleet size differs from GCE identities')
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
    from workflow_input import BACKENDS
    if not set(config['spec']['backends']) <= BACKENDS:
        raise RuntimeSafetyError('unsupported GCP backend selected')
    if runtime['spec'].get('mode') == 'focused':
        if json.dumps(config['spec'], sort_keys=True) != json.dumps(_controller_spec(runtime['spec']), sort_keys=True):
            raise RuntimeSafetyError('focused controller spec differs from reviewed runtime')
        jobs = (cloud_controller.controller_matrix(config['spec'])
                if runtime['spec']['selection'] == cloud_campaign.Q0_SELECTION
                else cloud_campaign.make_focused_matrix(runtime['spec']['selection']))
    else:
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
    if not isinstance(r.get('machine_type'), str) or r['machine_type'] not in LOCAL_SSD_COUNTS:
        raise RuntimeSafetyError('machine type must have an admitted local SSD layout')
    expected_ssds = LOCAL_SSD_COUNTS[r['machine_type']]
    placement = r.get('placement_policy')
    if placement is not None:
        region = r['fleet'][0]['zone'].rsplit('-', 1)[0]
        observed_policy = clients['platform'].request('GET', f'beta/regions/{region}/resourcePolicies/{placement["name"]}')
        group = observed_policy.get('groupPlacementPolicy', {})
        if (str(observed_policy.get('id')) != str(placement['id'])
                or group.get('collocation') != 'COLLOCATED'
                or group.get('maxDistance') not in (1, 2, 3)):
            raise RuntimeSafetyError('live compact placement policy differs from admitted identity')
    observed = {}
    for host in r['fleet']:
        instance = clients['platform'].request('GET', f'beta/zones/{host["zone"]}/instances/{host["name"]}')
        meta = {item['key']: item.get('value') for item in instance.get('metadata', {}).get('items', [])}
        schedule = instance.get('scheduling', {})
        networks = instance.get('networkInterfaces', [])
        scratch = [disk for disk in instance.get('disks', []) if disk.get('type') == 'SCRATCH']
        policy = placement['name'] if placement is not None else r['run_id'] + '-placement'
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
            or len(scratch) != expected_ssds or any(disk.get('interface') != 'NVME' for disk in scratch)
            or not any(item.split('/')[-1] == policy for item in instance.get('resourcePolicies', []))):
            raise RuntimeSafetyError('live GCE instance differs from admitted identity, network or expiry')
        observed[host['name']] = {'status': instance['status'], 'id': str(instance['id']), 'name': host['name'],
                                  'local_ssd_count': expected_ssds, 'selected_local_ssd': 'google-local-nvme-ssd-0',
                                  'unused_local_ssd_count': expected_ssds - 1}
    return observed


def admit_nvmet_fix(runtime, host, package):
    """Load the selected sealed candidate only after local and fleet admission."""
    candidate = package.get('nvmet_fix')
    if candidate is None:
        return None
    if package.get('manifest_sha256') != runtime['package']['manifest_sha256']:
        raise RuntimeSafetyError('nvmet fix manifest differs from selected package')
    import cloud_package
    root = Path(runtime['package_root'])
    loaded = cloud_package.nvmet_fix_module(root / 'source').install(root / 'nvmet-fix')
    guest = loaded.get('guest', {})
    if (any(loaded.get(key) != value for key, value in candidate.items())
            or guest.get('instance_name') != host['name']
            or guest.get('instance_id') != str(host['id'])
            or guest.get('run_id') != runtime['run_id']
            or loaded.get('loaded_nvmet') != {'srcversion': candidate['candidate_srcversion'],
                                              'build_id': candidate['candidate_build_id']}):
        raise RuntimeSafetyError('loaded nvmet identity differs from selected candidate or guest')
    return {'candidate': candidate, 'loaded': loaded}



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
    argv = [GUEST_PYTHON, str(script), 'guest', '--config-base64', encode(controller.config), '--request-base64', encode(payload)]
    controller.guard()
    reference = controller.clients['mailbox'].submit(index + 1, argv, operation_id=op, timeout_seconds=timeout)
    receipt = controller.clients['mailbox'].wait(reference, timeout_seconds=timeout)
    if receipt['exit_code'] != 0:
        raise RuntimeSafetyError('guest RPC process failed: ' + receipt['stderr'])
    store = controller.clients['objects']
    head = store.head(key)
    result = json.loads(store.get(key, generation=head['generation'], max_bytes=cloud_controller.MAX_JSON_BYTES))
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


def prepare_transport(config, preparation):
    """Bind generic verbs to each admitted host, not its management NIC."""
    result = copy.deepcopy(preparation)
    transport = result['transport']
    if transport.get('fabric', {}).get('backend') == 'verbs':
        fleet = config['fleet']
        transport['fabric']['source_address'] = fleet['coordinator']['rdma_address']
        result['owner_transports'] = []
        selected_hosts = fleet['nodes']
        if 'addresses' in result:
            hosts = {host['address']: host for host in fleet['nodes']}
            selected_hosts = [hosts[endpoint.rsplit(':', 1)[0]] for endpoint in result['addresses']]
        for host in selected_hosts:
            owner = copy.deepcopy(transport)
            owner['fabric']['source_address'] = host['rdma_address']
            result['owner_transports'].append(owner)
    return result


def admit_fabrics(config, index, clients, root):
    import shutil
    import subprocess
    backends = set(config['spec']['backends'])
    nvme_requested = 'tidehunter_nvme_rdma' in backends
    nvme_tcp_requested = 'tidehunter_nvme_tcp' in backends
    nvme_live = (live_mode(config['spec'])
                 and (nvme_requested or nvme_tcp_requested))
    valkey_requested = 'valkey_rdma' in backends
    requested = nvme_requested or valkey_requested or bool(
        backends & {'tidehunter_verbs', 'tidehunter_verbs_native'})
    evidence = {'provider': 'gcp', 'nvme_rdma_requested': nvme_requested,
                'nvme_tcp_requested': nvme_tcp_requested,
                'valkey_rdma_requested': valkey_requested, 'rdma_requested': requested}
    if nvme_requested or nvme_tcp_requested:
        if not shutil.which('nvme'):
            raise RuntimeSafetyError('NVMe-oF requires nvme-cli')
        modules = ['nvmet', 'nvme_fabrics']
        if nvme_requested:
            modules += ['rdma_cm', 'nvme_rdma', 'nvmet_rdma']
        if nvme_tcp_requested or nvme_live:
            modules += ['nvme_tcp', 'nvmet_tcp']
        for module in modules:
            subprocess.run(['modprobe', module], check=True, timeout=30)
        mounted = subprocess.run(['findmnt', '-rn', '-M', '/sys/kernel/config', '-o', 'FSTYPE'],
                                 capture_output=True, text=True, timeout=10)
        if mounted.returncode == 1:
            subprocess.run(['mount', '-t', 'configfs', 'configfs', '/sys/kernel/config'], check=True, timeout=30)
        elif mounted.returncode:
            raise RuntimeSafetyError('unable to inspect configfs mount')
        elif mounted.stdout.strip() != 'configfs':
            raise RuntimeSafetyError('NVMe target requires configfs at /sys/kernel/config')
        if not Path('/sys/kernel/config/nvmet').is_dir() or not Path('/dev/nvme-fabrics').is_char_device():
            raise RuntimeSafetyError('NVMe-oF target configfs or initiator device is unavailable')
    if requested:
        if valkey_requested or nvme_requested:
            subprocess.run(['modprobe', 'rdma_cm'], check=True, timeout=30)
            if not Path('/dev/infiniband/rdma_cm').is_char_device():
                raise RuntimeSafetyError('RDMA requires the RDMA-CM character device')
        links = subprocess.check_output(['rdma', 'link', 'show'], text=True, timeout=10)
        host = config['platform']['runtime']['fleet'][index + 1]
        interfaces = json.loads(subprocess.check_output(['ip', '-j', 'address', 'show'], text=True, timeout=10))
        names = [item.get('ifname') for item in interfaces
                 if any(address.get('local') == host['rdma_ip'] for address in item.get('addr_info', []))]
        if (len(names) != 1 or not isinstance(names[0], str)
                or not any(re.search(r'\bstate\s+ACTIVE\b', line)
                           and re.search(r'\bnetdev\s+' + re.escape(names[0]) + r'\b', line)
                           for line in links.splitlines())):
            raise RuntimeSafetyError('active RDMA device is not bound to the admitted IRDMA address')
        evidence.update(rdma_links=links, network_interface=names[0], address=host['rdma_ip'])
    return evidence


def bootstrap_controller(controller):
    import cloud_controller
    identity = verify_local_identity(controller.config, -1)
    fleet = controller.guard(starting=True)
    controller.persist('startup.json', {'status': 'starting', 'identity': identity,
                                         'config': controller.config, 'fleet_observations': fleet})
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
    cloud_controller.validate_physical_proofs(
        [proof['snapshot']['nvme'] for proof in proofs], provider='gcp',
        expected_instance_ids=[str(host['id']) for host in controller.config['platform']['runtime']['fleet']])
    cloud_controller.dump(controller.root / 'bootstrap-proofs.json', proofs)
    controller.persist('startup.json', {'status': 'admitted', 'identity': identity,
                                        'config': controller.config, 'package': controller.package,
                                        'fleet_observations': current,
                                        'bootstrap_proofs_sha256': cloud_controller.digest(controller.root / 'bootstrap-proofs.json')})
    controller.start_monitoring()
    controller.observe('access_receipt', lambda: cloud_controller.dump(
        controller.root / 'observability/access.json', monitoring_access(controller.config, proofs)), retain_receipt=False)


def admit_guest_unit(config, index):
    """Exercise the actual benchmark transient-unit constructor before any case."""
    import cloud_controller
    import subprocess
    import tempfile
    role = 'client' if index == -1 else 'storage-' + str(index)
    case = 'startup-runtime-admission'
    name = cloud_controller.unit_name(config, case, role)
    script = (
        "import json,resource,sys,yaml; "
        "print(json.dumps({'executable':sys.executable,'yaml':yaml.__version__,"
        "'yaml_check':yaml.safe_load('x: 1'),"
        "'nofile':list(resource.getrlimit(resource.RLIMIT_NOFILE))}))"
    )
    fleet = config['fleet']
    argv = cloud_controller.systemd_argv(
        config, case, role, fleet['client_cpus'] if index == -1 else fleet['server_cpus'],
        fleet['client_memory_mib'] if index == -1 else fleet['server_memory_mib'], 30,
        [GUEST_PYTHON, '-c', script])
    with tempfile.TemporaryDirectory(prefix='.runtime-admission-', dir=fleet['data_root']) as temporary:
        proof_path = Path(temporary) / 'guest-runtime.json'
        argv[1:1] = ['--property=StandardOutput=file:' + str(proof_path)]
        try:
            subprocess.run(argv, check=True, capture_output=True, text=True, timeout=15)
            deadline = time.monotonic() + 30
            while True:
                status = cloud_controller.unit_status(name)
                if status.get('ActiveState') == 'active' and status.get('SubState') == 'exited':
                    break
                if status.get('ActiveState') == 'failed' or status.get('Result') not in (None, '', 'success'):
                    raise RuntimeSafetyError('transient unit runtime admission failed: ' + repr(status))
                if time.monotonic() >= deadline:
                    raise RuntimeSafetyError('transient unit runtime admission timed out: ' + repr(status))
                time.sleep(0.1)
            if (status.get('ExecMainStatus') != '0' or status.get('LimitNOFILE') != '524288'
                    or status.get('LimitNOFILESoft') != '524288'):
                raise RuntimeSafetyError('transient unit exit or NOFILE properties do not match admission')
            if not proof_path.is_file() or proof_path.is_symlink() or proof_path.stat().st_size > 16384:
                raise RuntimeSafetyError('transient unit runtime proof is missing or invalid')
            proof = json.loads(proof_path.read_text())
            if (proof.get('executable') != GUEST_PYTHON or proof.get('yaml') != '6.0.2'
                    or proof.get('yaml_check') != {'x': 1} or proof.get('nofile') != [524288, 524288]):
                raise RuntimeSafetyError('transient unit guest Python or effective NOFILE admission failed')
            return {'process': proof, 'unit': status}
        finally:
            subprocess.run(['systemctl', 'stop', name], check=True, capture_output=True, timeout=10)


def bootstrap_guest(config, index, clients):
    import cloud_controller
    import cloud_package
    import local
    import cloud_bootstrap
    import subprocess
    identity = verify_local_identity(config, index)
    runtime_admission = admit_guest_runtime()
    fleet = verify_fleet(config, clients)
    package = cloud_package.verify_directory(Path(config['platform']['runtime']['package_root']), runtime=True, provider='gcp')
    if package['source_sha256'] != config['source_sha256'] or package['binary_sha256'] != config['binary_sha256']:
        raise RuntimeSafetyError('package identity differs from controller')
    admit_package_profile(config['spec'], package)
    nvmet_fix = admit_nvmet_fix(config['platform']['runtime'],
                                {'name': identity['name'], 'id': identity['instance_id']}, package)
    physical_trace = qualify_physical_trace(config['spec'])
    unit_admission = admit_guest_unit(config, index)
    unit = 'tidehunter-expire-' + config['prefix'] + '.timer'
    subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True, timeout=10)
    images = ({} if config['spec'].get('mode') == 'focused' else
              {name: json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True, timeout=30))
               for name, image in local.IMAGES.items()})
    fabrics = admit_fabrics(config, index, clients, Path(config['fleet']['data_root']))
    monitor_images = cloud_bootstrap.cache_monitor_images(config['repository_root'], 5 if index == -1 else index,
        datetime.fromtimestamp(_deadline(config['deadline_utc']), timezone.utc), pull=False)
    if not monitor_images['complete']:
        raise RuntimeSafetyError('prepared guest lacks cached monitoring images: ' + repr(monitor_images['gaps']))
    result = {'identity': identity, 'package': package, 'images': images, 'fabric_admission': fabrics,
              'guest_runtime': runtime_admission, 'guest_unit': unit_admission,
              'fleet_observations': fleet, 'snapshot': cloud_controller.snapshot(config), 'monitor_images': monitor_images}
    if physical_trace is not None:
        result['physical_trace_qualification'] = physical_trace
    if nvmet_fix is not None:
        result['nvmet_fix'] = nvmet_fix
    return result


def _controller_spec(spec):
    """Project the reviewed runtime spec to the controller spec.

    `reservation_hours` is launch-only: the launcher resolves it into
    `hard_deadline_seconds` and the guest expiry, which stay in the spec.
    Launcher projection and guest validation must use this one function.
    """
    return {k: v for k, v in spec.items()
            if k not in ('suite', 'reservation_hours')}


def _controller_config(runtime):
    root = Path(runtime['package_root'])
    manifest = json.loads((root / 'manifest.json').read_text())
    package = runtime['package']
    if manifest['source_sha256'] != runtime['source_sha256'] or manifest['revision'] != package['source_revision']:
        raise RuntimeSafetyError('runtime manifest source mismatch')
    if runtime['spec'].get('mode') == 'focused' and runtime['spec'].get('selection') == 'valkey_rdma_reads':
        import prepare
        prepare.admit_selected_features(runtime['spec'], manifest)
    elif runtime['spec'].get('mode') == 'focused':
        import cloud_package
        if manifest.get('build_profile') != 'focused' or manifest.get('features') != cloud_package.FOCUSED_FEATURES:
            raise RuntimeSafetyError('focused campaign requires exact TideHunter build features')
    required_features = set(runtime['spec']['backends']) & {'tikv', 'cassandra', 'foundationdb', 'garnet', 'kvrocks'}
    if not required_features <= set(manifest.get('features', [])):
        raise RuntimeSafetyError('packaged binary lacks selected industry backend features')
    required_artifacts = set()
    if live_mode(runtime['spec']):
        required_artifacts.add('source/experiments/kv-comparison/nvme_campaign_owner.py')
    if runtime['spec'].get('valkey_reproduction_profiles'):
        required_artifacts |= {'runtime/bin/valkey-benchmark', 'runtime/valkey-rdma.json',
                               'source/experiments/kv-comparison/valkey_cloud.py',
                               'source/experiments/kv-comparison/valkey_reproduction.py',
                               'source/experiments/kv-comparison/valkey-public-reproduction.json'}
    if not required_artifacts <= set(manifest.get('files', {})):
        raise RuntimeSafetyError('runtime omits selected leased or native Valkey artifacts')
    raw_hosts = runtime['fleet']
    def host(item):
        return {'instance_id': item['name'], 'numeric_id': str(item['id']), 'address': item['internal_ip'],
                'rdma_address': item['rdma_ip']}
    nodes = [{**host(item), 'index': i} for i, item in enumerate(raw_hosts[1:])]
    region = raw_hosts[0]['zone'].rsplit('-', 1)[0]
    cpus = int(runtime['machine_type'].rsplit('-', 1)[-1])
    fleet = {'schema_version': 1, 'run_id': runtime['run_id'], 'provider': 'gcp', 'replication_factor': 1,
             'qualification_only': False, 'data_root': RUN_DATA_ROOT + '/' + runtime['run_id'],
             'region': region, 'instance_type': runtime['machine_type'], 'coordinator': host(raw_hosts[0]), 'nodes': nodes,
             'server_cpus': cpus, 'client_cpus': cpus, 'server_memory_mib': 16384, 'client_memory_mib': 16384}
    if 'valkey_rdma' in runtime['spec']['backends']:
        fleet['valkey_rdma_package'] = True
    spec = _controller_spec(runtime['spec'])
    return {'fleet': fleet, 'bucket': runtime['bucket'], 'bucket_region': region, 'prefix': runtime['run_id'],
            'account': runtime['project_number'], 'source_revision': manifest['revision'],
            'source_sha256': runtime['source_sha256'], 'binary_sha256': manifest['binary_sha256'],
            'binary_blake2b512': manifest['binary_blake2b512'], 'repository_root': str(root / 'source'),
            'binary': str(root / 'runtime/bin/kv-research'), 'fdb_library_dir': str(root / 'runtime/lib'),
            'deadline_utc': runtime['deadline_utc'], 'spec': spec,
            'platform': {'name': 'gcp', 'runtime': runtime}}


def execute_campaign(runtime_config, *, api=None):
    import gcp_api
    validate_runtime(runtime_config)
    runtime = runtime_config
    sys.path.insert(0, str(Path(runtime['package_root']) / 'source/experiments/kv-comparison'))
    import gcp_worker
    if gcp_worker.identify(runtime)['index'] != 0:
        raise RuntimeSafetyError('campaign may only run on the admitted coordinator')
    api = api or gcp_api.GoogleApi(runtime['project_id'], token_provider=gcp_api.metadata_token)
    results = {}
    _put_once(api.objects(runtime['bucket']), 'runs/' + runtime['run_id'] + '/poweroff/kv-started.json',
              _encode({'run_id': runtime['run_id'], 'source_sha256': runtime['source_sha256'], 'suite': 'kv'}))
    import cloud_controller
    config = _controller_config(runtime)
    clients = make_clients(config, guest=True, api=api)
    report = cloud_controller.Controller(config, clients).run()
    results['kv'] = {key: report[key] for key in ('complete', 'planned_cases', 'completed_cases')}
    if runtime['spec'].get('valkey_reproduction_profiles'):
        results['kv']['native_valkey_reproduction'] = report['native_valkey_reproduction']
    return results


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != 'execute':
        raise SystemExit('usage: gcp_runtime.py execute RUNTIME_CONFIG')
    with open(sys.argv[2], encoding='utf-8') as stream:
        config = json.load(stream)
    print(json.dumps(execute_campaign(config), sort_keys=True))
