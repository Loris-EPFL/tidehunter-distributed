"""Protocol-v5 history and explicit TLS/trusted-native RAM transport controls.

Native controls are measured once per matching workload/transport/repetition;
policy variants reference those exact cases, never synthesized native artifacts.
"""
import hashlib
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path, PurePosixPath

import checkpoint_async
import cluster_ram
import efa_progress
import scalar_transport
from checkpoint_matrix import TRANSPORTS

BASELINE_SOURCE_SHA256 = '2e694cb0be9fe0738c217b75afb9fe0ba5d2eef170573bd95f092fa40e93dc57'
BASELINE_BINARY_SHA256 = '7f692391ec6b6acbc5c12c31cb64878764c0ef96d98d82e1b0dfb6a67594db2c'
CURRENT_SOURCE_SHA256 = '4facb657c3b84720991cd02f18a97b3a53934e3edd1c8b49a5c139616d582239'
CURRENT_BINARY_SHA256 = '032402ee15960d55267774826412d6de0b127a88627e1a549b9010f2331862b6'
CURRENT_FLOOR_BINARY_SHA256 = 'd2e6690120ac97e19b5dd2a4c1e61ff9e4c648db3da5d7750e0b00b2e25d8701'
CURRENT_RECOVERY_BINARY_SHA256 = '035a8b62d9838b254e1417e2744ea350bf6f479c2f1ae873e1931e09d3d41739'
SCOPES = ('full', 'core', 'recovery')
POLICIES = (('adaptive', False), ('busy', False), ('adaptive', True), ('busy', True))
FABRIC_VERSION = scalar_transport.FABRIC_VERSION


def case(workload, build, value, rep, transport, batch=32, window=32,
         polling='adaptive', resident_first=False):
    spec = cluster_ram.case(workload, build, value, rep, transport, batch, window)
    spec.update(polling=polling, resident_first=resident_first)
    spec['family'] = f'floor-{workload}-p{polling}-resident{int(resident_first)}-{transport}'
    spec['case_id'] = f"{spec['family']}-{build}-v{value}-b{batch}-w{window}-r{rep}"
    control_family = f'floor-{workload}-padaptive-resident0-{transport}'
    spec['native_control_id'] = f'{control_family}-{build}-v{value}-b{batch}-w{window}-r{rep}-native-sync'
    if (polling, resident_first) != POLICIES[0]:
        spec['modes'] = ['tcp']
    return spec


def qualification_cases():
    return [case('qual', build, value, 0, 'efa')
            for value in (1024, 8192) for build in ('baseline', 'current')]


def main_cases(scope='full'):
    if scope not in SCOPES:
        raise ValueError('unknown transport-floor scope')
    if scope == 'recovery':
        return []
    result = []
    for rep in range(3):
        builds = ('baseline', 'current') if rep % 2 == 0 else ('current', 'baseline')
        policies = POLICIES if rep % 2 == 0 else tuple(reversed(POLICIES))
        for value in (1024, 8192):
            for transport in ('tcp', 'efa'):
                for batch, window, workload in ((32, 32, 'core'), (1, 1, 'scalar')):
                    for polling, resident in policies:
                        for build in builds if (polling, resident) == POLICIES[0] else ('current',):
                            result.append(case(workload, build, value, rep, transport, batch, window, polling, resident))
    for transport in ('tcp', 'efa'):
        for value in (1024, 8192):
            for polling, resident in POLICIES:
                result.append(case('diag', 'current', value, 0, transport, polling=polling, resident_first=resident))
    for transport in ('tcp', 'efa'):
        for polling in ('adaptive', 'busy'):
            for idle in (0, 2000):
                spec = case('ram-idle2ms' if idle else 'ram-continuous', 'current', 8192, 0, transport, polling=polling)
                spec.update(floor=True, modes=['floor'], cold=False, samples=256 if idle else 1000,
                            idle_us=idle, profile=bool(idle), floor_values=[1024, 8192], floor_repetitions=3)
                spec['native_control_id'] = None
                result.append(spec)
    return result


def recovery_cases():
    result = []
    for old in scalar_transport.recovery_cases():
        spec = case('recovery', old['build'], 8192, 0, old['transport'])
        spec.update(recovery=True, restart=old['restart'], checkpoint_batches=64,
                    tail_batches=old['tail_batches'], modes=['recovery'], cold=False)
        spec['family'] = f"floor-recovery-{spec['restart']}-t{spec['tail_batches']}-{spec['transport']}"
        spec['case_id'] = f"{spec['family']}-{spec['build']}-r0"
        spec['native_control_id'] = None
        result.append(spec)
    return result


def plan(scope='full'):
    return qualification_cases() + main_cases(scope) + (recovery_cases() if scope != 'core' else [])


def expected_cases(scope='full'):
    return {spec['case_id'] + '-' + mode:
            (spec['family'], spec['build'], spec['value'], spec['batch'], spec['window'], spec['rep'], mode, spec['case_id'])
            for spec in plan(scope) for mode in spec['modes']}


def flags(spec, mode, external=False, prepare=False):
    result = cluster_ram.flags(spec | {'build': 'current'}, mode, external, prepare)
    # External node policy belongs to prepared servers, not an
    # ignored coordinator flag. The old binary predates these two options.
    if spec['build'] == 'current' and (prepare or not external):
        result += ['--tcp-polling', spec['polling'], '--tcp-resident-first', str(spec['resident_first']).lower()]
    return result


def subsuite_floor_spec(transport, instrumented):
    """Bounded independent RAM populations inside the existing full KV suite."""
    if transport not in TRANSPORTS or type(instrumented) is not bool:
        raise ValueError('invalid subsuite RAM control population')
    return dict(transport=transport, polling='adaptive', floor_values=[256, 8192],
                samples=256 if instrumented else 512,
                floor_repetitions=1 if instrumented else 3,
                idle_us=0, profile=instrumented)


def floor_flags(spec):
    return ['--transport', spec['transport'], '--polling', spec['polling'],
            '--values', ','.join(map(str, spec['floor_values'])), '--samples', str(spec['samples']),
            '--repetitions', str(spec['floor_repetitions']), '--idle-us', str(spec['idle_us']),
            '--profile', str(spec['profile']).lower(), '--warmup', '64', '--shards', '5']


def validate_build_identity(root, label):
    import analyze_cloud as audit
    root = Path(root)
    if label not in ('baseline', 'current'):
        raise ValueError('unplanned transport-floor build')
    source = BASELINE_SOURCE_SHA256 if label == 'baseline' else CURRENT_SOURCE_SHA256
    receipt = audit.read_json(root / f'cloud-build-{label}.json')
    hashes = ('binary_sha256', 'local_binary_sha256', 'source_manifest_sha256',
              'source_archive_sha256', 'libfabric_sha256', 'recovery_binary_sha256')
    if (not audit.is_sha256(source) or receipt.get('label') != label or receipt.get('source_tree_sha256') != source
            or receipt.get('features') != 'experimental-fabric,test-utils'
            or receipt.get('libfabric_version') != FABRIC_VERSION
            or any(not audit.is_sha256(receipt.get(key)) for key in hashes)):
        raise ValueError('transport-floor frozen source/binary/provider identity differs')
    if label == 'baseline' and receipt['local_binary_sha256'] != BASELINE_BINARY_SHA256:
        raise ValueError('baseline differs from preserved scalar transport executable')
    manifest_path = root / f'source-manifest-{label}.json'
    manifest = audit.read_json(manifest_path)
    files = manifest['source_sha256']
    if (not isinstance(files, dict) or not files or any(not audit.is_sha256(v) for v in files.values())
            or hashlib.sha256(manifest_path.read_bytes()).hexdigest() != receipt['source_manifest_sha256']
            or hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != source
            or manifest['source_tree_sha256'] != source or manifest['binary_sha256'] != receipt['local_binary_sha256']):
        raise ValueError('archived source manifest does not bind the frozen source/local binary')
    for kind in ('floor', 'recovery'):
        if kind + '_binary_sha256' in manifest and receipt.get('local_' + kind + '_binary_sha256') != manifest[kind + '_binary_sha256']:
            raise ValueError('local auxiliary binary differs from frozen manifest')
    if label == 'current':
        if (receipt['local_binary_sha256'] != CURRENT_BINARY_SHA256
                or manifest.get('floor_binary_sha256') != CURRENT_FLOOR_BINARY_SHA256
                or manifest.get('recovery_binary_sha256') != CURRENT_RECOVERY_BINARY_SHA256):
            raise ValueError('current local binaries differ from completed frozen build')
    pair = audit.read_json(root / 'cloud-build-pair-verified.json')
    if pair.get('binary_sha256', {}).get(label) != receipt['binary_sha256']:
        raise ValueError('executable differs from verified pair')
    if label == 'current':
        command = receipt['command'][:-1] + ['sharded_wal_transport_bench']
        built = audit.read_json(root / 'build-floor-current.command.json')
        if (not audit.is_sha256(receipt.get('floor_binary_sha256')) or receipt.get('floor_command') != command
                or built.get('command') != command or built.get('exit_code') != 0):
            raise ValueError('floor binary lacks exact successful source-bound build')
        capability = audit.read_json(root / 'capability-floor-current.command.json')
        floor_binary = str(PurePosixPath(receipt['target_directory']).parent.parent / 'cloud-bin/current/sharded_wal_transport_bench')
        help_text = (root / 'capability-floor-current.stdout.log').read_text()
        if (capability.get('exit_code') != 0 or capability.get('command') != [floor_binary, '--help']
                or capability.get('binary_sha256') != receipt['floor_binary_sha256']
                or any(flag not in help_text for flag in ('--remote-config', '--polling', '--profile', '--idle-us'))):
            raise ValueError('floor binary lacks successful hash-bound capability evidence')
    return receipt


def yaml_documents(text):
    # Import only when this campaign needs YAML, preserving historical dependency behavior.
    import yaml
    class UniqueLoader(yaml.SafeLoader):
        pass
    def mapping(loader, node):
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=True)
            if key in result:
                raise ValueError('duplicate YAML evidence key')
            result[key] = loader.construct_object(value_node, deep=True)
        return result
    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    # serde_yaml represents these benchmark KeyShape enum variants as tags.
    # Preserve their type in comparisons; do not enable arbitrary YAML objects.
    UniqueLoader.add_constructor('!Fixed', lambda loader, node: ('Fixed', int(loader.construct_scalar(node))))
    UniqueLoader.add_constructor('!Uniform', lambda loader, node: ('Uniform', mapping(loader, node)))
    try:
        return list(yaml.load_all(text, Loader=UniqueLoader))
    except yaml.YAMLError as error:
        raise ValueError('invalid YAML evidence') from error


def yaml_one(text):
    documents = yaml_documents(text)
    if len(documents) != 1:
        raise ValueError('expected one YAML evidence document')
    return documents[0]


def validate_node_policy(root, spec, launch_text, partition):
    launch = yaml_one(launch_text)
    if launch['node']['partition']['partition'] != partition:
        raise ValueError('launch partition differs')
    if launch['tls']['transport'] != spec['transport']:
        raise ValueError('launch transport differs')
    if (launch['node']['read_cache_bytes'] != spec['shard_cache']
            or launch['node']['read_memory_allowance_bytes'] != spec['shard_allowance']):
        raise ValueError('launched cache/read-memory policy differs')
    node_shape = yaml_one(launch['node']['shape_yaml'])
    spaces = node_shape['key_spaces']
    if not isinstance(spaces, list) or len(spaces) != 1:
        raise ValueError('node shape differs from single key-space benchmark')
    shape_fields = spaces[0]['inner']
    if (shape_fields['id'] != 0 or shape_fields['key_indexing'] != ('Fixed', 8)
            or shape_fields['mutexes'] != 16 or shape_fields['key_type'] != ('Uniform', {'cells_per_mutex': 64})
            or shape_fields['config']['value_cache_size'] != 0):
        raise ValueError('node shape differs from immutable benchmark geometry')
    if spec['build'] == 'current':
        if (launch['limits']['polling'] != spec['polling']
                or launch['node']['read_resident_first'] is not spec['resident_first']):
            raise ValueError('launched polling/resident policy differs')
        shape = yaml_one((Path(root) / (spec['case_id'] + '-shape.yaml')).read_text())
        if not isinstance(shape, dict) or not shape or node_shape != shape:
            raise ValueError('node shape differs from floor/coordinator shape')


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError('expected nonnegative integer evidence')
    return value


def flatten_counters(value, prefix=''):
    if not isinstance(value, dict):
        raise ValueError('expected structured counters')
    result = {}
    for key, item in value.items():
        name = prefix + key
        if isinstance(item, dict):
            result.update(flatten_counters(item, name + '.'))
        else:
            result[name] = integer(item)
    return result


def validate_polling(profile, spec):
    counters = flatten_counters(profile)
    if spec['transport'] in ('efa-native', 'fabric-tcp-native'):
        for owner in ('transport', 'client_transport'):
            for name in ('tls_read', 'tls_write', 'tls_process'):
                work = profile.get(owner, {}).get(name, {})
                if any(type(work.get(key)) is not int or work[key] != 0
                       for key in ('ns', 'calls', 'bytes', 'errors')):
                    raise ValueError('native RAM evidence lacks zero-TLS work')
    if spec['profile']:
        polling = profile['polling']
        if integer(polling['progress_sweeps']) == 0 or integer(polling['request_first_poll_samples']) == 0:
            raise ValueError('missing measured polling progress')
        if spec['polling'] == 'busy' and any(integer(polling[key]) != 0 for key in
                                            ('idle_wait_calls', 'idle_wait_ns', 'scheduler_yields')):
            raise ValueError('Busy established session blocked or yielded')
    return counters


def read_floor_metrics(path, spec):
    path = Path(path)
    if not (path / 'COMPLETE').is_file():
        raise ValueError('missing complete payload audit')
    metadata = {}
    for line in (path / 'metadata.txt').read_text().splitlines():
        key, value = line.split('=', 1)
        if key in metadata:
            raise ValueError('duplicate floor metadata')
        metadata[key] = value
    scope = ('trusted_native_exchange_decode_and_full_payload_validation'
             if spec['transport'] in ('efa-native', 'fabric-tcp-native') else
             'authenticated_exchange_decode_and_full_payload_validation')
    required = dict(transport=spec['transport'], polling=spec['polling'].title(), instrumented=str(spec['profile']).lower(),
                    topology='external_nodes', shards='5', timed_scope=scope,
                    wall_scope='measured_requests_and_offered_idle_including_scheduler_delay',
                    wal_io='checked_zero_member_reads', physical_network_latency='not_isolated', external_cleanup='operator_owned')
    if any(metadata.get(key) != value for key, value in required.items()):
        raise ValueError('floor security/policy/timing contract differs')
    expected = {(value, rep) for rep in range(spec['floor_repetitions']) for value in spec['floor_values']}
    samples = defaultdict(list)
    for row in cluster_ram.read_csv(path / 'samples.csv'):
        identity = int(row['value_bytes']), int(row['repetition'])
        index, ns = int(row['sample']), int(row['api_ns'])
        if identity not in expected or index != len(samples[identity]) or int(row['partition_index']) != index % 5 or ns <= 0:
            raise ValueError('floor sample dimensions/order/partition/time differ')
        samples[identity].append(ns)
    metrics, seen = {}, set()
    for row in cluster_ram.read_csv(path / 'summary.csv'):
        identity = int(row['value_bytes']), int(row['repetition'])
        if identity not in expected or identity in seen:
            raise ValueError('duplicate/unplanned floor summary')
        seen.add(identity)
        values = sorted(samples[identity])
        if (len(values) != spec['samples'] or int(row['samples']) != len(values)
                or row['transport'] != spec['transport'] or row['polling'] != spec['polling'].title()
                or row['instrumented'] != str(spec['profile']).lower() or int(row['shards']) != 5
                or int(row['idle_us']) != spec['idle_us']):
            raise ValueError('floor sample count/policy differs')
        for pct in (50, 95, 99):
            if int(row[f'p{pct}_ns']) != values[(len(values) * pct + 99) // 100 - 1]:
                raise ValueError('floor percentile differs from raw samples')
        wall = int(row['wall_ns'])
        if (int(row['max_ns']) != values[-1] or wall < sum(values) or wall <= 0
                or not math.isclose(cluster_ram.number(row['phase_ops_per_sec']), len(values) * 1e9 / wall, rel_tol=1e-8)):
            raise ValueError('floor summary wall/rate/max differs')
        phase = f'ram-v{identity[0]}-r{identity[1]}'
        for key in ('samples', 'wall_ns', 'p50_ns', 'p95_ns', 'p99_ns', 'max_ns', 'phase_ops_per_sec'):
            metrics['floor', phase, key] = cluster_ram.number(row[key])
        evidence = yaml_one((path / f'evidence-v{identity[0]}-r{identity[1]}.yaml').read_text())
        if not isinstance(evidence, list) or len(evidence) != 3 or any(not isinstance(v, list) or len(v) != 5 for v in evidence):
            raise ValueError('floor evidence requires five before/after/profiles')
        for partition, (before, after, profile) in enumerate(zip(*evidence)):
            for boundary, counters in (('before', before), ('after', after)):
                for key, value in flatten_counters(counters).items():
                    metrics['floor_io', phase, f'partition{partition}.{boundary}.{key}'] = value
            for key in ('wal_reads', 'wal_read_bytes'):
                if after[key] != before[key]:
                    raise ValueError('RAM floor accessed WAL members during its measured interval')
            if spec['profile'] and integer(profile['requests']) < (spec['samples'] + 4 - partition) // 5:
                raise ValueError('profile omitted measured RAM requests')
            for key, value in validate_polling(profile, spec).items():
                metrics['floor_profile', phase, f'partition{partition}.{key}'] = value
    if seen != expected or set(samples) != expected:
        raise ValueError('missing floor value/repetition measurements')
    if {p.name for p in path.glob('evidence-*.yaml')} != {f'evidence-v{v}-r{r}.yaml' for v, r in expected}:
        raise ValueError('unplanned floor evidence file')
    return metrics


def validate_recovery_json(row, spec, action):
    # Both sources have the scalar transport lifetime/setup statistics.
    scalar_transport.validate_recovery_json(row, spec | {'build': 'current'}, action)


def read_wal_metrics(path, spec, mode, expected_profiles=None):
    # The new baseline was the previous scalar campaign's current binary.
    parser_spec = spec | {'build': 'current'}
    metrics = cluster_ram.read_metrics(path, parser_spec, mode)
    scalar_transport.validate_lifecycle(metrics, parser_spec, mode)
    summaries = {row['phase']: row for row in cluster_ram.read_csv(Path(path) / 'summary.csv')}
    required = {'point_get_missing', 'exists', 'exists_missing', 'checkpoint_capture',
                'checkpoint_intervening_write', 'post_flush_get'}
    if spec['cold']:
        required |= {'cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'}
    else:
        required |= {'point_get_hot', 'point_get_many_hot'}
    if not required <= summaries.keys():
        raise ValueError('missing correctness/read-path phases')
    read_phases = {'point_get', 'point_get_many', 'point_get_window', 'point_get_missing', 'exists', 'exists_missing'}
    read_phases |= ({'cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'} if spec['cold']
                    else {'point_get_hot', 'point_get_many_hot'})
    if any(int(summaries[p]['logical_operations']) != spec['reads'] for p in read_phases):
        raise ValueError('read phase omitted planned full workload')
    for row in summaries.values():
        if row['durability'] != 'value_wal_file_and_directory_barrier' or row['transport'] != spec['transport']:
            raise ValueError('WAL durability/transport differs')
        if spec['build'] == 'current':
            phase = row['phase']
            unit = ('operation' if phase.endswith('_get_window') else 'complete_window' if phase.endswith('_window')
                    else 'complete_get_many' if 'get_many' in phase else 'whole_phase' if int(row['samples']) == 1 else 'operation')
            if row['sample_unit'] != unit:
                raise ValueError('sample unit mislabels per-operation or complete-window latency')
    if mode == 'tcp' and spec['build'] == 'current':
        snapshots = {}
        keys = ('resident_read_attempts', 'resident_read_successes', 'resident_read_fallbacks',
                'resident_read_unsupported', 'resident_read_partial', 'async_read_submissions')
        for row in cluster_ram.read_csv(Path(path) / 'read_path_evidence.csv'):
            if row['role'] == 'storage':
                snapshots[row['phase'], row['boundary'], row['partition']] = row
        for (phase, boundary, partition), before in snapshots.items():
            if boundary != 'before':
                continue
            after = snapshots[phase, 'after', partition]
            fields = keys + (('resident_read_deferred',) if 'resident_read_deferred' in before else ())
            deltas = {key: int(after[key]) - int(before[key]) for key in fields}
            if min(deltas.values()) < 0 or (not spec['resident_first'] and any(
                    value for key, value in deltas.items() if key != 'async_read_submissions')):
                raise ValueError('resident-read counters contradict requested policy')
            if deltas['resident_read_successes'] + deltas['resident_read_fallbacks'] > deltas['resident_read_attempts']:
                raise ValueError('resident read outcomes exceed attempts')
            for key, value in deltas.items():
                identity = 'read_path', phase, 'storage-' + key
                metrics[identity] = metrics.get(identity, 0) + value
        for phase in ('point_get', 'point_get_window'):
            policy_counters = (('resident_read_attempts', 'resident_read_deferred')
                               if spec['resident_first'] else ('async_read_submissions',))
            if sum(metrics.get(('read_path', phase, 'storage-' + key), 0) for key in policy_counters) <= 0:
                raise ValueError('missing actual selected resident/async read activity')
    if mode == 'tcp' and spec['diagnostics']:
        records = [r for r in yaml_documents((Path(path) / 'node_profiles.yaml').read_text()) if r is not None]
        if not records:
            raise ValueError('missing node diagnostics')
        seen = set()
        for row in records:
            if (row['backend'] != ('experimental_efa_authority' if spec['transport'] == 'efa' else 'experimental_tcp_authority')
                    or (row['value_bytes'], row['batch_size'], row['window'], row['repetition']) !=
                    (spec['value'], spec['batch'], spec['window'], 0) or row['phase'] in seen
                    or len(row['nodes']) != 5 or {p for p, _ in row['nodes']} != set(range(5))):
                raise ValueError('node diagnostic identity differs')
            seen.add(row['phase'])
            for partition, profile in row['nodes']:
                counters = (validate_polling(profile, spec | {'profile': True}) if spec['build'] == 'current'
                            else flatten_counters(profile))
                for key, value in counters.items():
                    metrics['node_profile', row['phase'], f'partition{partition}.{key}'] = value
        if expected_profiles is None:
            expected_profiles = {'load-write-diagnostics', 'warm-scalar-read-diagnostics',
                                 'warm-batched-read-diagnostics', 'reopened-read-diagnostics'}
            if spec['cold']:
                expected_profiles.add('cache-evicted-read-diagnostics')
        if seen != expected_profiles:
            raise ValueError('missing/unplanned diagnostic phase')
    return metrics


def read_cloud_case(root, name, spec, mode, profile=None):
    import analyze_cloud as audit
    profile = profile or __import__(__name__)
    campaign = 'transport-floor' if profile.__name__ == __name__ else 'stage-timing'
    root = Path(root)
    cfg = audit.read_json(root / 'campaign-config.json')
    if (cfg.get('campaign') != campaign or spec not in profile.plan(cfg.get('checkpoint_scope', 'full'))
            or mode not in spec['modes'] or name != spec['case_id'] + '-' + mode
            or audit.read_json(root / (spec['case_id'] + '-plan.json')) != spec):
        raise ValueError('unplanned transport-floor case/mode')
    receipt = profile.validate_build_identity(root, spec['build'])
    base = PurePosixPath(cfg['run_root'])
    case_root = base / 'cases' / spec['case_id']
    binary = str(base / 'cloud-bin' / spec['build'] / 'sharded_wal_bench')
    prep = audit.read_json(root / (spec['case_id'] + '-prepare.command.json'))
    endpoints = ','.join(n['private_ip'] + ':7700' for n in sorted(cfg['nodes'], key=lambda n: n['partition']))
    expected = [binary, '--prepare-external-case', str(case_root), '--guest-case-root', str(case_root),
                '--remote-endpoints', endpoints] + profile.flags(spec, 'tcp', prepare=True)
    validate_command(prep, expected, receipt['binary_sha256'])
    if mode == 'recovery':
        for action in ('seed', 'reopen'):
            process = audit.read_json(root / f'{name}-{action}-process.command.json')
            if process.get('binary_sha256') != receipt['recovery_binary_sha256']:
                raise ValueError('recovery executable differs from frozen source')
        return audit.read_recovery_case(root, name, spec, audit.transport_floor)
    process = audit.read_json(root / (name + '-process.command.json'))
    if mode == 'floor':
        binary = str(base / 'cloud-bin' / spec['build'] / 'sharded_wal_transport_bench')
        expected = [binary, '--output', str(base / 'results' / name), '--remote-config', str(case_root / 'remote.yaml')] + profile.floor_flags(spec)
        digest = receipt['floor_binary_sha256']
    else:
        expected = [binary, '--output', str(base / 'results' / name), '--backend', mode] + profile.flags(spec, mode, external=mode == 'tcp')
        if mode == 'tcp':
            expected += ['--remote-config', str(case_root / 'remote.yaml')]
        digest = receipt['binary_sha256']
    validate_command(process, expected, digest)
    remote = yaml_one((root / (spec['case_id'] + '-remote.yaml')).read_text())
    if (remote['read_lanes'] != spec['lanes'] or remote['read_cache_bytes'] != spec['cache']
            or remote['read_memory_allowance_bytes'] != spec['read_allowance']
            or remote['tls']['transport'] != spec['transport']
            or remote['storage']['partitions'] != list(range(5))
            or sorted((n['partition'], n['address']) for n in remote['endpoints']) !=
            sorted((n['partition'], n['private_ip'] + ':7700') for n in cfg['nodes'])):
        raise ValueError('remote topology/cache/transport differs')
    before, after = [audit.read_json(root / (name + '-' + stage + '.json')) for stage in ('before', 'after')]
    if any(e.get('instance_id') != cfg['coordinator_instance_id'] for e in (before, after)):
        raise ValueError('controller instance differs')
    hardware = efa_progress.hardware_interval(before, after, mode != 'native-sync' and spec['transport'] == 'efa')
    peak = cluster_ram.number(process['peak_sampled_main_process_rss_kib']) * 1024
    if not 0 < peak <= hardware['ram_bytes'] * 4 / 5:
        raise ValueError('controller RSS exceeds hardware allowance')
    nodes = {p: audit.node_evidence(root, spec['case_id'], p) for p in range(5)}
    for partition, node in nodes.items():
        if node['instance_id'] != next(n['instance_id'] for n in cfg['nodes'] if n['partition'] == partition):
            raise ValueError('storage instance differs')
    metrics = profile.read_floor_metrics(root / name, spec) if mode == 'floor' else profile.read_wal_metrics(root / name, spec, mode)
    if mode != 'floor':
        if spec['cold']:
            for row in cluster_ram.read_csv(root / name / 'read_io.csv'):
                pid = nodes[int(row['partition'])]['pid'] if mode == 'tcp' else process['pid']
                if int(row['pid']) != pid:
                    raise ValueError('physical reads belong to another process')
        summaries = {row['phase']: row for row in cluster_ram.read_csv(root / name / 'summary.csv')}
        resource_reader = getattr(profile, 'resource_metrics', efa_progress.resource_metrics)
        metrics.update(resource_reader(root / name / 'read_resources.csv', mode, spec, process, hardware, nodes, summaries))
    for key, value in audit.io_delta(before, after).items():
        if isinstance(value, (int, float)):
            metrics['hardware_io', 'whole_api_controller', key] = value
    for key, value in checkpoint_async.validate_threads(audit.read_json(root / (name + '-process.threads.json')), process['pid']).items():
        metrics['threads', 'sampled_controller_lifetimes', key] = value
    return metrics


def validate_command(receipt, expected, digest):
    if (receipt.get('exit_code') != 0 or receipt.get('binary_sha256') != digest
            or receipt['command'][0] != expected[0]
            or efa_progress.command_options(receipt['command']) != efa_progress.command_options(expected)):
        raise ValueError('executed command/binary differs from exact source-bound plan')


def compare(runs):
    import analyze_cloud as audit
    specs = {s['case_id']: s for s in plan()}
    grouped = defaultdict(dict)
    for info, metrics in runs.values():
        family, build, value, batch, window, rep, mode, case_id = info
        for (kind, phase, metric), number in metrics.items():
            measured_value, measured_rep = value, rep
            if specs[case_id].get('floor') and kind.startswith('floor'):
                measured_value, measured_rep = map(int, phase.removeprefix('ram-v').split('-r'))
                phase = 'ram_response'
            grouped[family, build, measured_value, batch, window, mode, kind, phase, metric][measured_rep] = (number, case_id)
    rows = []
    for key, repetitions in sorted(grouped.items()):
        family, build, value, batch, window, mode, kind, phase, metric = key
        spec = specs[next(iter(repetitions.values()))[1]]
        oldfamily = family.replace('-pbusy-', '-padaptive-').replace('-resident1-', '-resident0-')
        oldbuild = 'current' if spec.get('floor') else 'baseline'
        previous = (grouped.get((oldfamily, oldbuild, value, batch, window, mode, kind, phase, metric), {})
                    if build == 'current' and (not spec.get('floor') or spec['polling'] == 'busy') else {})
        values = {rep: entry[0] for rep, entry in repetitions.items()}
        oldvalues = {rep: entry[0] for rep, entry in previous.items()}
        native = {}
        native_ids = []
        for rep, (_, case_id) in repetitions.items():
            control = specs[case_id]['native_control_id']
            if mode == 'tcp' and not spec['qualification']:
                if control not in runs:
                    continue  # The missing expected case already makes analysis incomplete.
                native_ids.append(control)
                if (kind, phase, metric) in runs[control][1]:
                    native[rep] = runs[control][1][kind, phase, metric]
        old_native = {}
        for rep, (_, case_id) in previous.items():
            control = specs[case_id]['native_control_id']
            if mode == 'tcp' and control in runs and (kind, phase, metric) in runs[control][1]:
                old_native[rep] = runs[control][1][kind, phase, metric]
        new = statistics.median(values.values())
        old = statistics.median(oldvalues.values()) if oldvalues else None
        rows.append(dict(comparison='persistent_ram_floor_profiled' if spec.get('floor') and spec['profile'] else
                         'persistent_ram_floor' if spec.get('floor') else 'qualification' if spec['qualification'] else
                         'diagnostic_only' if spec['diagnostics'] else 'whole_implementation_and_config',
                         family=family, backend=mode, build=build, value_bytes=value, batch_size=batch, window=window,
                         kind=kind, phase=phase, metric=metric, transport=spec['transport'], polling=spec['polling'],
                         resident_first=spec['resident_first'], idle_us=spec.get('idle_us'),
                         instrumented=spec.get('profile', spec['diagnostics']),
                         old_label=oldbuild + '/adaptive/resident=false',
                         new_label=f"{build}/{spec['polling']}/resident={str(spec['resident_first']).lower()}",
                         old_median=old, new_median=new, new_over_old=audit.ratio(new, old),
                         old_repetitions=len(oldvalues), new_repetitions=len(values),
                         old_over_native_barrier=audit.ratio(old, statistics.median(old_native.values())) if old_native and kind == 'summary' else None,
                         new_over_native_barrier=audit.ratio(new, statistics.median(native.values())) if native and kind == 'summary' else None,
                         native_control_ids=native_ids, native_control_semantics='exact measured same-transport/workload/value/repetition Adaptive resident=false control; not pooled',
                         repetition_values=values, paired_deltas={r: values[r] - oldvalues[r] for r in values.keys() & oldvalues.keys()},
                         paired_ratios={r: audit.ratio(values[r], oldvalues[r]) for r in values.keys() & oldvalues.keys()}))
    return rows
