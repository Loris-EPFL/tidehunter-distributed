"""Source-bound protocol-v5/v6 cluster-placement stage timing campaign.

Local monotonic spans are nested/parallel diagnostics, not wire or one-way latency.
Historical transport-floor definitions and pins remain independent.
"""
import json
import re
import statistics
import tarfile
import tempfile
from collections import defaultdict
from pathlib import Path

import cluster_ram
import prepare_bundle
import transport_floor
from checkpoint_matrix import HARDWARE_TRANSPORTS

BASELINE_SOURCE_SHA256 = transport_floor.CURRENT_SOURCE_SHA256
BASELINE_BINARY_SHA256 = transport_floor.CURRENT_BINARY_SHA256
SCOPES = ('full',)
FABRIC_VERSION = transport_floor.FABRIC_VERSION
PROTOCOLS = {'baseline': 5, 'current': 6}


def case(workload, build, value, rep, transport, batch=32, window=32):
    spec = transport_floor.case(workload, build, value, rep, transport, batch, window,
                                polling='busy', resident_first=True)
    spec['family'] = f'stage-{workload}-{transport}'
    spec['case_id'] = f"{spec['family']}-{build}-v{value}-b{batch}-w{window}-r{rep}"
    spec['native_control_id'] = None
    return spec


def qualification_cases():
    return [case('qual', build, value, 0, 'efa')
            for value in (1024, 8192) for build in ('baseline', 'current')]


def main_cases(scope='full'):
    if scope not in SCOPES:
        raise ValueError('stage timing only supports the complete targeted matrix')
    result = []
    for rep in range(3):
        for value in (1024, 8192):
            for batch, window, workload in ((32, 32, 'core'), (1, 1, 'scalar')):
                for transport in ('tcp', 'efa'):
                    spec = case(workload, 'current', value, rep, transport, batch, window)
                    # One actually measured native control per exact shape/repetition;
                    # both remote transports reference it, without synthetic output.
                    spec['modes'] = ['native-sync', 'tcp'] if transport == 'tcp' else ['tcp']
                    spec['native_control_id'] = spec['case_id'].replace('-efa-', '-tcp-') + '-native-sync'
                    result.append(spec)
    for transport in ('tcp', 'efa'):
        for value in (1024, 8192):
            for batch, window in ((32, 32), (1, 1)):
                spec = case('diag', 'current', value, 0, transport, batch, window)
                spec['modes'] = ['native-sync', 'tcp'] if transport == 'tcp' else ['tcp']
                spec['native_control_id'] = spec['case_id'].replace('-efa-', '-tcp-') + '-native-sync'
                result.append(spec)
    # Match the previous spread campaign's continuous and idle controls exactly.
    # Current paired instrumentation controls isolate diagnostic overhead, not wire time.
    for transport in ('tcp', 'efa'):
        for polling in ('adaptive', 'busy'):
            for build, instrumented, idle in (('baseline', False, 0), ('current', False, 0),
                                               ('current', True, 0), ('current', True, 2000)):
                spec = case(f'ram-{polling}-profile{int(instrumented)}-idle{idle}', build, 8192, 0, transport)
                spec.update(floor=True, modes=['floor'], cold=False, samples=256 if idle else 1000,
                            idle_us=idle, profile=instrumented, floor_values=[1024, 8192],
                            floor_repetitions=3, polling=polling, resident_first=False)
                result.append(spec)
    return result


def recovery_cases():
    return []


def plan(scope='full'):
    return qualification_cases() + main_cases(scope)


def expected_cases(scope='full'):
    return {s['case_id'] + '-' + mode:
            (s['family'], s['build'], s['value'], s['batch'], s['window'], s['rep'], mode, s['case_id'])
            for s in plan(scope) for mode in s['modes']}


def flags(spec, mode, external=False, prepare=False, transport_config=None):
    # Both revisions implement Busy/resident-first and the memory policy flags.
    return transport_floor.flags(spec, mode, external, prepare, transport_config)


floor_flags = transport_floor.floor_flags


def source_identity(directory, label):
    directory = Path(directory)
    manifest_path = directory / 'source-manifest.json'
    manifest = json.loads(manifest_path.read_text())
    prepare_bundle.validate_source_archive(directory / 'source.tar.gz', manifest)
    expected_features = 'experimental-fabric,test-utils'
    if manifest.get('features') != expected_features:
        raise ValueError('stage campaign requires source-bound fabric build features')
    with tarfile.open(directory / 'source.tar.gz') as archive:
        protocol = archive.extractfile('tidehunter/src/distributed/tcp.rs').read().decode()
    if re.findall(r'^const PROTOCOL: u32 = (\d+);$', protocol, re.M) != [str(PROTOCOLS[label])]:
        raise ValueError('baseline-v5/current-v6 protocol binding differs')
    for executable, key in [('sharded_wal_bench', 'binary_sha256'),
                            ('sharded_wal_transport_bench', 'floor_binary_sha256')]:
        if prepare_bundle.sha(directory / executable) != manifest[key]:
            raise ValueError('stage binary differs from source-bound manifest')
    if label == 'baseline' and (manifest['source_tree_sha256'] != BASELINE_SOURCE_SHA256
            or manifest['binary_sha256'] != BASELINE_BINARY_SHA256
            or manifest['floor_binary_sha256'] != transport_floor.CURRENT_FLOOR_BINARY_SHA256):
        raise ValueError('baseline differs from preserved previous-current build')
    return dict(source_tree_sha256=manifest['source_tree_sha256'],
                source_manifest_sha256=prepare_bundle.sha(manifest_path),
                source_archive_sha256=prepare_bundle.sha(directory / 'source.tar.gz'),
                local_binary_sha256=manifest['binary_sha256'],
                local_floor_binary_sha256=manifest['floor_binary_sha256'], protocol=PROTOCOLS[label])


def bundle_identities(bundle):
    # The plan derives identity from the actual upload bytes, never user-supplied claims.
    with tempfile.TemporaryDirectory(prefix='stage-bind-') as directory:
        root = Path(directory)
        with tarfile.open(bundle) as archive:
            for label in PROTOCOLS:
                target = root / label
                target.mkdir()
                for name in ('source-manifest.json', 'source.tar.gz', 'sharded_wal_bench', 'sharded_wal_transport_bench'):
                    matches = [m for m in archive.getmembers() if m.name == f'bundle/{label}/{name}']
                    if len(matches) != 1 or not matches[0].isfile() or matches[0].size > 512 << 20:
                        raise ValueError('missing/unsafe/duplicate source-bound bundle artifact')
                    (target / name).write_bytes(archive.extractfile(matches[0]).read())
        identities = {label: source_identity(root / label, label) for label in PROTOCOLS}
    if identities['baseline']['source_tree_sha256'] == identities['current']['source_tree_sha256']:
        raise ValueError('stage current must be a distinct instrumentation source')
    return identities


def validate_configuration(cfg):
    if (cfg.get('campaign') != 'stage-timing' or cfg.get('instance_type') != 'c6id.32xlarge'
            or cfg.get('placement_strategy') != 'cluster' or cfg.get('placement_group') != cfg.get('run_id')
            or not re.fullmatch(r'us-east-1[a-f]', cfg.get('availability_zone', ''))
            or cfg.get('ttl_seconds') != 1500 or cfg.get('export_reserve_seconds') != 300
            or cfg.get('checkpoint_scope') != 'full' or cfg.get('terminate_on_finish') is not True):
        raise ValueError('stage campaign requires reviewed cluster placement and bounded lifetime')
    identities = cfg['source_identities']
    if set(identities) != set(PROTOCOLS):
        raise ValueError('missing source-bound build identities')
    for label, identity in identities.items():
        if identity.get('protocol') != PROTOCOLS[label] or any(
                not re.fullmatch('[a-f0-9]{64}', identity.get(key, '')) for key in (
                    'source_tree_sha256', 'source_manifest_sha256', 'source_archive_sha256',
                    'local_binary_sha256', 'local_floor_binary_sha256')):
            raise ValueError('unbound stage build identity')
    if (identities['baseline']['source_tree_sha256'] != BASELINE_SOURCE_SHA256
            or identities['baseline']['local_binary_sha256'] != BASELINE_BINARY_SHA256
            or identities['baseline']['local_floor_binary_sha256'] != transport_floor.CURRENT_FLOOR_BINARY_SHA256
            or identities['current']['source_tree_sha256'] == BASELINE_SOURCE_SHA256):
        raise ValueError('baseline/current source identity differs')


def validate_placement(cfg, instances, groups):
    validate_configuration(cfg)
    if len(groups) != 1:
        raise ValueError('placement group lookup is not exact')
    group = groups[0]
    expected_tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': cfg['run_id']}
    tags = {t['Key']: t['Value'] for t in group.get('Tags', [])}
    if (group.get('GroupName') != cfg['placement_group'] or group.get('Strategy') != 'cluster'
            or group.get('State') != 'available' or any(tags.get(k) != v for k, v in expected_tags.items())):
        raise ValueError('actual placement group strategy/ownership differs')
    if len(instances) != 6 or len({i['InstanceId'] for i in instances}) != 6:
        raise ValueError('placement requires six distinct hosts')
    for instance in instances:
        placement = instance.get('Placement', {})
        if (instance.get('InstanceType') != 'c6id.32xlarge'
                or placement.get('AvailabilityZone') != cfg['availability_zone']
                or placement.get('GroupName') != cfg['placement_group']
                or not any(n.get('InterfaceType') in ('efa', 'efa-only') for n in instance.get('NetworkInterfaces', []))):
            raise ValueError('actual instance placement/type/AZ/EFA differs')


def validate_build_identity(root, label):
    import analyze_cloud as audit
    root = Path(root)
    cfg = audit.read_json(root / 'campaign-config.json')
    validate_configuration(cfg)
    identity = cfg['source_identities'][label]
    receipt = audit.read_json(root / f'cloud-build-{label}.json')
    if any(receipt.get(key) != value for key, value in identity.items() if key != 'protocol'):
        raise ValueError('cloud receipt differs from immutable source-bound plan')
    manifest_path = root / f'source-manifest-{label}.json'
    manifest = audit.read_json(manifest_path)
    prepare_bundle.validate_source_archive(root / f'source-{label}.tar.gz', manifest)
    if (prepare_bundle.sha(manifest_path) != identity['source_manifest_sha256']
            or prepare_bundle.sha(root / f'source-{label}.tar.gz') != identity['source_archive_sha256']
            or manifest['source_tree_sha256'] != identity['source_tree_sha256']
            or manifest['binary_sha256'] != identity['local_binary_sha256']
            or manifest['floor_binary_sha256'] != identity['local_floor_binary_sha256']
            or receipt.get('libfabric_version') != FABRIC_VERSION
            or receipt.get('features') != 'experimental-fabric,test-utils'
            or not audit.is_sha256(receipt.get('libfabric_sha256'))):
        raise ValueError('archived manifest/source/provider differs from plan')
    with tarfile.open(root / f'source-{label}.tar.gz') as archive:
        protocol = archive.extractfile('tidehunter/src/distributed/tcp.rs').read().decode()
    if re.findall(r'^const PROTOCOL: u32 = (\d+);$', protocol, re.M) != [str(PROTOCOLS[label])]:
        raise ValueError('archived source protocol differs')
    pair = audit.read_json(root / 'cloud-build-pair-verified.json')
    if pair['binary_sha256'][label] != receipt['binary_sha256']:
        raise ValueError('build pair executable differs')
    for kind in ('binary_sha256', 'floor_binary_sha256'):
        if not audit.is_sha256(receipt.get(kind)):
            raise ValueError('missing cloud executable identity')
    command = receipt['command'][:-1] + ['sharded_wal_transport_bench']
    floor_build = audit.read_json(root / f'build-floor-{label}.command.json')
    capability = audit.read_json(root / f'capability-floor-{label}.command.json')
    binary = str(Path(receipt['target_directory']).parent.parent / 'cloud-bin' / label / 'sharded_wal_transport_bench')
    if (receipt.get('floor_command') != command or floor_build.get('command') != command
            or floor_build.get('exit_code') != 0 or capability.get('command') != [binary, '--help']
            or capability.get('exit_code') != 0 or capability.get('binary_sha256') != receipt['floor_binary_sha256']):
        raise ValueError('floor build/capability is not source-bound')
    help_text = (root / f'capability-floor-{label}.stdout.log').read_text()
    if any(flag not in help_text for flag in ('--remote-config', '--polling', '--profile', '--idle-us')):
        raise ValueError('floor capability output is missing required controls')
    if receipt.get('label') != label or not receipt.get('compiler') or receipt.get('engine_source_unchanged') is not True:
        raise ValueError('missing source-bound compiler/build completion evidence')
    return receipt


def validate_node_policy(root, spec, text, partition):
    transport_floor.validate_node_policy(root, spec | {'build': 'current'}, text, partition)


def read_cloud_case(root, name, spec, mode):
    return transport_floor.read_cloud_case(root, name, spec, mode, profile=__import__(__name__))


def read_floor_metrics(path, spec):
    metrics = transport_floor.read_floor_metrics(path, spec)
    if spec['build'] == 'current':
        for value in spec['floor_values']:
            for repetition in range(spec['floor_repetitions']):
                evidence = transport_floor.yaml_one((Path(path) / f'evidence-v{value}-r{repetition}.yaml').read_text())
                for partition, profile in enumerate(evidence[2]):
                    validate_stages(profile, spec['transport'], enabled=spec['profile'])
                    expected = (spec['samples'] + 4 - partition) // 5 if spec['profile'] else 0
                    client = profile['client']
                    if (client['ram_payload_verify_calls'] != expected
                            or client['ram_payload_verified_bytes'] != expected * value):
                        raise ValueError('RAM verification sample/byte count differs from measured partition workload')
                    if any(profile['storage_write'].values()):
                        raise ValueError('RAM floor performed storage write work')
    return metrics


def read_wal_metrics(path, spec, mode):
    if spec['build'] == 'current':
        metadata = (Path(path) / 'campaign.txt').read_text()
        required = dict(diagnostic_timing_schema='6', stage_times_are_nested_not_additive='true',
                        transport_io_time_includes_provider_work='true',
                        storage_persist_time_includes_file_lookup_and_sync='true')
        if any(re.findall(r'^' + key + r'=(.*)$', metadata, re.M) != [value] for key, value in required.items()):
            raise ValueError('missing current stage timing schema/scope contract')
    phases = {'load-write-diagnostics', 'warm-scalar-read-diagnostics', 'warm-profiled-read-diagnostics',
              'warm-batched-read-diagnostics', 'warm-window-read-diagnostics', 'reopened-read-diagnostics'}
    if spec['cold']:
        phases.add('cache-evicted-read-diagnostics')
        phases |= {phase + '-diagnostics' for phase in (
            'cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window')}
    metrics = transport_floor.read_wal_metrics(path, spec, mode, expected_profiles=phases)
    if spec['build'] == 'current' and spec['diagnostics']:
        validate_read_samples(path, spec, mode)
        if mode == 'tcp':
            for record in transport_floor.yaml_documents((Path(path) / 'node_profiles.yaml').read_text()):
                if record is None:
                    continue
                for _, profile in record['nodes']:
                    validate_stages(profile, spec['transport'], enabled=True,
                                    writes=record['phase'] == 'load-write-diagnostics')
    return metrics


def compare(runs):
    import analyze_cloud as audit
    specs = {s['case_id']: s for s in plan()}
    grouped = defaultdict(dict)
    for info, metrics in runs.values():
        family, build, value, batch, window, rep, mode, case_id = info
        for (kind, phase, metric), measured in metrics.items():
            measured_value, measured_rep = value, rep
            if specs[case_id].get('floor') and kind.startswith('floor'):
                measured_value, measured_rep = map(int, phase.removeprefix('ram-v').split('-r'))
                phase = 'ram_response'
            grouped[family, build, measured_value, batch, window, mode, kind, phase, metric][measured_rep] = (measured, case_id)
    rows = []
    for key, values in sorted(grouped.items()):
        family, build, value, batch, window, mode, kind, phase, metric = key
        spec = specs[next(iter(values.values()))[1]]
        baseline = grouped.get((family, 'baseline', value, batch, window, mode, kind, phase, metric), {}) if build == 'current' else {}
        old = statistics.median(v[0] for v in baseline.values()) if baseline else None
        new = statistics.median(v[0] for v in values.values())
        controls = [specs[case]['native_control_id'] for _, case in values.values()]
        native = [runs[c][1][kind, phase, metric] for c in controls if c in runs and (kind, phase, metric) in runs[c][1]]
        unprofiled = (grouped.get((family.replace('-profile1-', '-profile0-'), build,
                                  value, batch, window, mode, kind, phase, metric), {})
                      if spec.get('floor') and spec['profile'] and spec['idle_us'] == 0 else {})
        control = statistics.median(v[0] for v in unprofiled.values()) if unprofiled else None
        rows.append(dict(comparison='diagnostic_only' if spec.get('profile', spec['diagnostics']) else 'uninstrumented',
            family=family, backend=mode, build=build, value_bytes=value, batch_size=batch, window=window,
            kind=kind, phase=phase, metric=metric, transport=spec['transport'], polling=spec['polling'],
            resident_first=spec['resident_first'], old_median=old, new_median=new,
            new_over_old=audit.ratio(new, old), old_repetitions=len(baseline), new_repetitions=len(values),
            new_over_native_barrier=audit.ratio(new, statistics.median(native)) if native and kind == 'summary' else None,
            old_over_native_barrier=None, native_control_ids=[c for c in controls if c],
            uninstrumented_control_median=control, profiled_over_unprofiled=audit.ratio(new, control),
            repetition_values={r: v[0] for r, v in values.items()},
            timing_scope='local monotonic; nested/parallel spans not additive; no wire-only or one-way attribution'))
    return rows


def summarize(rows):
    """Keep exact main-case shapes even when no same-fleet old API case exists."""
    return [row for row in rows if row['kind'] == 'summary'
            and row['comparison'] == 'uninstrumented'
            and row['family'].startswith(('stage-core-', 'stage-scalar-'))]


WORK_FIELDS = ('frame_encode', 'frame_decode', 'plaintext_write', 'plaintext_read',
               'plaintext_buffer_resize', 'tls_write', 'tls_read', 'tls_process')
PROVIDER_FIELDS = tuple(
    f'{name}_{suffix}' for name in ('send_copy', 'receive_copy', 'send_post', 'receive_post')
    for suffix in (('ns', 'calls', 'bytes', 'would_block', 'errors') if name.endswith('post') else ('ns', 'calls', 'bytes'))
) + ('cq_call_ns', 'cq_timed_calls', 'cq_error_call_ns', 'cq_error_calls', 'cq_dispatch_ns',
     'cq_dispatch_calls', 'cq_dispatch_errors', 'cq_poll_gap_ns', 'cq_poll_gap_samples', 'cq_poll_gap_max_ns')
APPEND_STAGES = ('state_lock_wait', 'validation', 'wal', 'publication')
PERSIST_STAGES = ('serialization_lock_wait', 'state_lock_wait', 'validation', 'wal_file', 'namespace', 'publication')
STORAGE_FIELDS = (
    'append_calls', 'append_errors', 'append_retries', 'appended_groups', 'appended_receipts',
    'append_total_ns', *(f'append_{name}_ns' for name in APPEND_STAGES),
    'persist_calls', 'persist_errors', 'persist_noops', 'persisted_receipts', 'persist_total_ns',
    *(f'persist_{name}_ns' for name in PERSIST_STAGES), 'file_persist_calls', 'file_persist_successes',
    'namespace_persist_calls', 'namespace_persist_successes', 'durable_publications')


def validate_stages(profile, transport, enabled, writes=False):
    """Validate exact new-field presence, nonnegative integers, and count invariants."""
    flat = transport_floor.flatten_counters(profile)
    required = {'requests', 'control_queue_ns', 'control_queue_samples', 'control_execution_ns', 'control_execution_samples',
                'append_inventory_residence_ns', 'client.ram_payload_verify_ns',
                'client.ram_payload_verify_calls', 'client.ram_payload_verified_bytes'}
    required |= {'storage_write.' + field for field in STORAGE_FIELDS}
    for owner in ('transport', 'client_transport'):
        required |= {f'{owner}.{field}.{part}' for field in WORK_FIELDS for part in ('ns', 'calls', 'bytes', 'errors')}
        required |= {f'{owner}.{field}' for field in PROVIDER_FIELDS}
    missing = required - flat.keys()
    if missing:
        raise ValueError('missing protocol-v6 stage evidence: ' + ','.join(sorted(missing)))
    if not enabled and any(flat[field] for field in required):
        raise ValueError('uninstrumented control recorded opt-in stage work')
    if profile['control_queue_samples'] != profile['control_execution_samples']:
        raise ValueError('control admission/execution sample count differs')
    for timer, denominator in stage_denominators().items():
        if timer in required and timer in flat and denominator in flat and flat[denominator] == 0 and flat[timer]:
            raise ValueError('stage time lacks its call/sample denominator')
    for owner in ('transport', 'client_transport'):
        counters = profile[owner]
        for field in WORK_FIELDS:
            work = counters[field]
            if work['errors'] > work['calls'] or (work['calls'] == 0 and any(work.values())):
                raise ValueError('stage work sample/error/byte denominator differs')
        for prefix in ('send_post', 'receive_post'):
            if counters[prefix + '_errors'] + counters[prefix + '_would_block'] > counters[prefix + '_calls']:
                raise ValueError('provider post outcomes exceed call count')
        if counters['cq_dispatch_errors'] > counters['cq_dispatch_calls']:
            raise ValueError('CQ dispatch errors exceed sampled entries')
        if counters['cq_poll_gap_max_ns'] > counters['cq_poll_gap_ns']:
            raise ValueError('CQ gap maximum exceeds sampled sum')
        if counters['cq_poll_gap_samples'] == 0 and counters['cq_poll_gap_ns']:
            raise ValueError('CQ gap time lacks samples')
        if transport == 'tcp' and any(counters[field] for field in PROVIDER_FIELDS):
            raise ValueError('TCP evidence contains provider work')
        if transport == 'efa-native' and any(any(counters[field].values())
                                            for field in ('tls_write', 'tls_read', 'tls_process')):
            raise ValueError('trusted native transport recorded software TLS work')
        if enabled and profile['requests']:
            if not all(counters[field]['calls'] > 0 for field in ('frame_encode', 'frame_decode')):
                raise ValueError('missing measured frame encode/decode work')
            if transport in HARDWARE_TRANSPORTS and not all(counters[field] > 0 for field in ('cq_timed_calls', 'send_post_calls', 'receive_copy_calls')):
                raise ValueError('missing measured provider work')
    storage = profile['storage_write']
    if (storage['append_calls'] != storage['append_errors'] + storage['append_retries'] + storage['appended_groups']
            or storage['persist_calls'] != storage['persist_errors'] + storage['persist_noops'] + storage['durable_publications']):
        raise ValueError('storage call/outcome count differs')
    if (sum(storage[f'append_{stage}_ns'] for stage in APPEND_STAGES) > storage['append_total_ns']
            or sum(storage[f'persist_{stage}_ns'] for stage in PERSIST_STAGES) > storage['persist_total_ns']):
        raise ValueError('storage nested stage sum exceeds enclosing total')
    # One captured durable prefix can span several WAL files.
    bounds = (('file_persist_successes', 'file_persist_calls'),
              ('namespace_persist_calls', 'persist_calls'),
              ('namespace_persist_calls', 'file_persist_successes'),
              ('namespace_persist_successes', 'namespace_persist_calls'), ('durable_publications', 'file_persist_successes'))
    if any(storage[child] > storage[parent] for child, parent in bounds):
        raise ValueError('storage persistence count ordering differs')
    if writes and (not enabled or not all(storage[field] > 0 for field in
                  ('appended_groups', 'durable_publications', 'file_persist_calls'))
                  or profile['control_queue_samples'] == 0 or profile['control_execution_samples'] == 0):
        raise ValueError('load write diagnostics omit actual durable write stages')
    return flat


def validate_read_samples(path, spec, mode):
    filename = 'native_read_profiles.csv' if mode == 'native-sync' else 'remote_read_profiles.csv'
    records = cluster_ram.read_csv(Path(path) / filename)
    fields = (('index_ns', 'wal_and_decode_ns', 'whole_call_ns') if mode == 'native-sync' else
              ('index_ns', 'dispatch_ns', 'wait_ns', 'actor_queue_ns', 'actor_service_ns', 'actor_return_ns',
               'request_encode_ns', 'exchange_ns', 'response_decode_ns', 'record_verify_ns',
               'server_handler_ns', 'server_inventory_wait_ns', 'server_authority_lookup_ns',
               'server_record_read_ns', 'whole_call_ns', 'inline_read_ns'))
    states = defaultdict(list)
    for row in records:
        if any(field not in row for field in fields):
            raise ValueError('missing sampled read stage field')
        for field in fields:
            cluster_ram.number(row[field])
        states[row['state']].append(row)
    reopened = 'reopened-warm' if mode == 'native-sync' else 'reopened-cache-cleared'
    expected_states = {'warm', reopened} | ({'cache-evicted'} if spec['cold'] else set())
    if set(states) != expected_states:
        raise ValueError('missing/unplanned sampled read diagnostic states')
    expected = min(spec['reads'], 256)
    for rows in states.values():
        if len(rows) != expected:
            raise ValueError('diagnostic sample count differs from planned bounded workload')
        if any(int(row['sample']) != sample for sample, row in enumerate(rows)):
            raise ValueError('diagnostic sample sequence differs')


def stage_denominators():
    denominators = {
        'control_queue_ns': 'control_queue_samples', 'control_execution_ns': 'control_execution_samples',
        'request_decode_ns': 'requests', 'handler_ns': 'requests',
        'response_encode_ns': 'requests', 'response_send_ns': 'requests',
        'read_response_recheck_wait_ns': 'read_response_recheck_samples',
        'read_response_recheck_ns': 'read_response_recheck_samples',
        'client.server_response_recheck_wait_ns': 'client.server_response_recheck_samples',
        'client.server_response_recheck_ns': 'client.server_response_recheck_samples',
        'append_inventory_wait_ns': 'storage_write.append_calls',
        'append_inventory_residence_ns': 'storage_write.append_calls',
        'append_decode_ns': 'storage_write.append_calls', 'append_group_build_ns': 'storage_write.append_calls',
        'append_wal_ns': 'storage_write.append_calls', 'append_persist_ns': 'storage_write.persist_calls',
        'client.ram_payload_verify_ns': 'client.ram_payload_verify_calls',
        'client.exchange_ns': 'client.requests',
    }
    for field in ('request_encode_ns', 'response_decode_ns', 'record_verify_ns', 'server_handler_ns',
                  'server_inventory_wait_ns', 'server_authority_lookup_ns', 'server_record_read_ns'):
        denominators['client.' + field] = 'client.requests'
    for owner in ('socket', 'client.socket'):
        for timer, count in (('read_ns', 'read_calls'), ('write_ns', 'write_calls'),
                             ('timeout_setup_ns', 'timeout_sets'), ('fabric_drain_ns', 'fabric_drains')):
            denominators[f'{owner}.{timer}'] = f'{owner}.{count}'
    denominators['polling.request_first_poll_gap_ns'] = 'polling.request_first_poll_samples'
    denominators['polling.idle_wait_ns'] = 'polling.idle_wait_calls'
    for field in STORAGE_FIELDS:
        if field.endswith('_ns'):
            denominators['storage_write.' + field] = 'storage_write.' + ('append_calls' if field.startswith('append_') else 'persist_calls')
    for owner in ('transport', 'client_transport'):
        for stage in ('queue', 'submit', 'retire'):
            denominators[f'{owner}.output_{stage}_ns'] = f'{owner}.output_{stage}_samples'
        for field in WORK_FIELDS:
            denominators[f'{owner}.{field}.ns'] = f'{owner}.{field}.calls'
        for prefix in ('send_copy', 'receive_copy', 'send_post', 'receive_post', 'cq_dispatch'):
            denominators[f'{owner}.{prefix}_ns'] = f'{owner}.{prefix}_calls'
        denominators[f'{owner}.cq_call_ns'] = f'{owner}.cq_timed_calls'
        denominators[f'{owner}.cq_error_call_ns'] = f'{owner}.cq_error_calls'
        denominators[f'{owner}.cq_poll_gap_ns'] = f'{owner}.cq_poll_gap_samples'
    return denominators


def summarize_stages(runs):
    result = []
    denominators = stage_denominators()
    for name, (info, metrics) in sorted(runs.items()):
        for (kind, phase, metric), summed in sorted(metrics.items()):
            if info[1] != 'current' and kind in ('node_profile', 'floor_profile'):
                continue
            denominator = None
            if kind in ('node_profile', 'floor_profile') and '.' in metric:
                partition, field = metric.split('.', 1)
                if field in denominators:
                    denominator = partition + '.' + denominators[field]
            elif kind in ('profile', 'native_profile', 'exchange_profile') and metric.endswith('_ns_sum'):
                if kind == 'profile' and metric in (
                        'server_response_recheck_wait_ns_sum', 'server_response_recheck_ns_sum'):
                    denominator = 'server_response_recheck_samples_sum'
                else:
                    denominator = metric.removesuffix('_sum') + '_n'
            if denominator is not None:
                count = metrics.get((kind, phase, denominator))
                if count is None:
                    raise ValueError('missing stage sample denominator')
                unavailable = (
                    'not attributable per request under multiplexing' if kind == 'exchange_profile' else
                    'not measured for the RAM probe' if kind == 'floor_profile' and field in (
                        'handler_ns', 'client.record_verify_ns', 'client.server_handler_ns',
                        'client.server_inventory_wait_ns', 'client.server_authority_lookup_ns',
                        'client.server_record_read_ns') else None)
                result.append(dict(case=name, kind=kind, phase=phase, field=metric,
                    ns_sum=summed, denominator_field=denominator, samples=count,
                    mean_ns=summed / count if count and unavailable is None else None,
                    availability=unavailable or ('measured' if count else 'no observations'),
                    denominator_scope='raw rows or requests, not timed events' if unavailable else 'recorded field denominator',
                    scope='local monotonic call/residence; nested/parallel, not additive; connection counters include control traffic',
                    zero_sample_semantics='not observed; never interpreted as zero latency'))
    return result


def compare_saved_spread(runs, root):
    """Same-source workload control across campaigns, not causal placement isolation."""
    import analyze_cloud as audit
    root = Path(root)
    if not (root / 'COMPLETE').is_file() or audit.read_json(root / 'campaign-receipt.json').get('complete') is not True:
        raise ValueError('saved spread campaign is incomplete')
    if audit.read_json(root / 'campaign-config.json').get('campaign') != 'transport-floor':
        raise ValueError('saved spread control must be the historical transport-floor campaign')
    transport_floor.validate_build_identity(root, 'current')
    result = []
    for spec in main_cases():
        if spec['build'] != 'baseline' or not spec.get('floor'):
            continue
        previous = next(s for s in transport_floor.main_cases() if s.get('floor')
                        and s['transport'] == spec['transport'] and s['polling'] == spec['polling']
                        and not s['profile'] and s['idle_us'] == 0)
        for key in ('samples', 'floor_values', 'floor_repetitions', 'idle_us', 'profile'):
            if spec[key] != previous[key]:
                raise ValueError('historical floor workload dimensions differ')
        old = transport_floor.read_cloud_case(root, previous['case_id'] + '-floor', previous, 'floor')
        name = spec['case_id'] + '-floor'
        if name not in runs:
            raise ValueError('missing same-source cluster floor control')
        current = runs[name][1]
        for key, value in sorted(current.items()):
            if key[0] == 'floor':
                result.append(dict(cluster_case=name, historical_case=previous['case_id'] + '-floor',
                    phase=key[1], metric=key[2], cluster=value, historical=old[key],
                    cluster_over_historical=audit.ratio(value, old[key]),
                    source_tree_sha256=BASELINE_SOURCE_SHA256,
                    interpretation='same-code matched workload, cross-time historical spread versus cluster observation; not causal or wire-only'))
    return result


def resource_metrics(path, mode, spec, process, controller, nodes, summaries):
    import efa_progress
    stages = None
    if spec['build'] == 'current':
        stages = ['opened', 'loaded', 'warm-scalar-before', 'warm-scalar-after',
                  'warm-window-before', 'point_get_window', 'warm-window-after',
                  'warm-batched-before', 'warm-batched-after']
        if spec['cold']:
            stages.append('cache_evicted_get_window')
    metrics = efa_progress.resource_metrics(path, mode, spec, process, controller, nodes, summaries, stages=stages)
    if spec['build'] == 'current':
        # Window recording includes an intervening point_get_window snapshot.
        for phase, boundaries in (
                ('warm-scalar', ['warm-scalar-after']),
                ('warm-window', ['point_get_window', 'warm-window-after']),
                ('warm-batched', ['warm-batched-after'])):
            metrics['resource', phase, 'cpu_ns_between_before_after_including_control'] = sum(
                metrics['resource', boundary, 'cpu_ns_since_previous_snapshot'] for boundary in boundaries)
    return metrics
