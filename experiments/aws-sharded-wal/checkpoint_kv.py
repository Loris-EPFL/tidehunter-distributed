"""Source-bound KV measurements inside the Sui controller's existing fleet lifetime.

Reuse the tested storage-node lifecycle and real WAL benchmark. This module does
not provision, rebuild, extend deadlines, or terminate a second fleet. The outer
Sui supervisor owns unconditional export and termination, including failures.
"""
import json
import shutil
import csv
import tarfile
from itertools import islice
from pathlib import Path
import math
import copy

import transport_floor
import raw_rma_floor

from checkpoint_matrix import efa_snapshot, efa_proof, validate_efa_proof, validate_provider
from checkpoint_matrix import fabric_local_log, fabric_local_archive_log
from checkpoint_matrix import HARDWARE_TRANSPORTS, DIRECT_TRANSPORTS, TRANSPORT_BACKENDS
from guest import (Campaign, upload, write_json, sha, benchmark_case_identity,
                   kv_case_commands, kv_case_policy, resident_read_config)


def validate_options(options):
    if options is None:
        return
    if (not isinstance(options, dict)
            or not {'repetitions', 'diagnostics'} <= options.keys()
            or options.keys() - {'repetitions', 'diagnostics', 'scope', 'resident_reads', 'bounded_hot_reads', 'cold_single_probes', 'cold_group_probes'}
            or type(options['repetitions']) is not int or not 1 <= options['repetitions'] <= 3
            or type(options['diagnostics']) is not bool
            or options.get('scope', 'full') not in ('full', 'targeted', 'hardware-populations')):
        raise ValueError('kv_suite requires repetitions 1..3, boolean diagnostics, and full/targeted/hardware-populations scope')
    if 'resident_reads' in options:
        resident_read_config(options['resident_reads'])
    count = options.get('bounded_hot_reads')
    if count is not None and (type(count) is not int or not 32 <= count <= 131072):
        raise ValueError('bounded_hot_reads requires an integer 32..131072')
    probes = options.get('cold_single_probes', 0)
    if type(probes) is not int or not 0 <= probes <= 4096:
        raise ValueError('cold_single_probes requires an integer 0..4096')
    groups = options.get('cold_group_probes', 0)
    if type(groups) is not int or not 0 <= groups <= 4096:
        raise ValueError('cold_group_probes requires an integer 0..4096')
    if options.get('scope') == 'hardware-populations':
        profile = resident_read_config(options.get('resident_reads'))
        if (count != 32768 or probes != 1024 or groups != 1024 or options['repetitions'] != 2 or not options['diagnostics']
                or not profile['destination_slots'] or not profile['require_device_read']):
            raise ValueError('hardware-populations requires 32768 hot samples, 1024 scalar and grouped physical cold probes, two clean repeats, diagnostics and device-qualified resident limits')


def resident_plan(cases, options):
    if options.get('bounded_hot_reads') is not None:
        cases = [case | dict(bounded_hot_reads=options['bounded_hot_reads']) for case in cases]
    if options.get('cold_single_probes'):
        cases = [case | dict(cold_single_probes=options['cold_single_probes']) if case['cold'] else case for case in cases]
    if options.get('cold_group_probes'):
        cases = [case | dict(cold_group_probes=options['cold_group_probes']) if case['cold'] else case for case in cases]
    if 'resident_reads' not in options:
        return cases
    config = resident_read_config(options['resident_reads'])
    if config == resident_read_config():
        return cases
    native = ('efa-native', 'fabric-tcp-native')
    if not any(case['transport'] in native for case in cases):
        raise ValueError('resident_reads requires a trusted-native case; RPC controls remain unchanged')
    # Preserve matrix size and the TCP/TLS RPC controls. Only explicit
    # trusted-native cases acquire the distinct RMA-bound case identity.
    return [case | dict(resident_reads=config)
            if not case['cold'] and case['transport'] in native else case for case in cases]


def hardware_population_plan(options, transports):
    """Named populations, not a cache × shape × transport Cartesian product."""
    if set(transports) != {'tcp', 'efa', 'efa-direct', 'efa-native'} or len(transports) != 4:
        raise ValueError('hardware populations require all four remote transport controls')
    profile = resident_read_config(options['resident_reads'])
    shapes = [(256, 1, 1, 320), (8192, 1, 1, 320),
              (8192, 32, 32, 320), (8192, 32, 32, 3200)]
    def case(shape, transport, repeat, population, diagnostic=False):
        value, batch, window, batches = shape
        result = dict(shape=[value, batch, window], repetition=repeat, transport=transport,
            cold=population == 'physical_io_interval', cached=population == 'coordinator_ram',
            diagnostics=diagnostic, authority_resident_batches=None,
            shard_cache_bytes=8 * 1024**2 if population == 'owner_encoded_ram' else 0,
            preparation_chunk_batches=16, wal_layout='small', workload_batches=batches,
            workload_reads=8192 if batch == 32 else 4096, transport_floor=False,
            bounded_hot_reads=options['bounded_hot_reads'])
        if result['cold']:
            result['cold_single_probes'] = options['cold_single_probes']
            result['cold_group_probes'] = options['cold_group_probes']
        if population == 'registered_ram':
            result['resident_reads'] = dict(profile)
        return result
    cases = []
    for repeat in range(options['repetitions']):
        for transport in transports if repeat % 2 == 0 else transports[::-1]:
            cases.extend(case(shape, transport, repeat, 'pagecache_control') for shape in shapes)
            if transport == 'efa-native':
                cases.extend(case(shape, transport, repeat, 'registered_ram') for shape in shapes)
            for population in ('coordinator_ram', 'owner_encoded_ram', 'physical_io_interval'):
                cases.extend(case(shape, transport, repeat, population) for shape in shapes[:2])
    # Clocks are fresh processes, not samples pooled with either clean repetition.
    repeat = options['repetitions']
    cases.extend(case(shapes[1], transport, repeat, 'pagecache_control', True) for transport in transports)
    cases.append(case(shapes[1], 'efa-native', repeat, 'registered_ram', True))
    return cases


def plan(options, transports):
    validate_options(options)
    if options is None:
        return []
    if options.get('scope') == 'hardware-populations':
        return hardware_population_plan(options, transports)
    if options.get('scope', 'full') == 'targeted':
        cases = []
        for repeat in range(options['repetitions']):
            for transport in (transports if repeat % 2 == 0 else transports[::-1]):
                for value, batch, window, cached, shard_cache, cold, chunk, layout in (
                        (256, 1, 1, False, 0, False, 16, 'small'),
                        (256, 1, 1, False, 8 * 1024**2, False, 16, 'small'),
                        (256, 32, 32, False, 0, False, 16, 'small'),
                        (8192, 1, 1, False, 0, False, 16, 'small'),
                        (8192, 1, 1, False, 8 * 1024**2, False, 16, 'small'),
                        (8192, 1, 1, True, 0, False, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 32, 'small'),
                        (8192, 32, 32, False, 8 * 1024**2, False, 16, 'small'),
                        (8192, 32, 32, False, 0, True, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 32, 'sui')):
                    cases.append(dict(shape=[value, batch, window], repetition=repeat,
                        transport=transport, cold=cold, cached=cached,
                        diagnostics=False,
                        authority_resident_batches=None, shard_cache_bytes=shard_cache,
                        preparation_chunk_batches=chunk, wal_layout=layout,
                        workload_batches=3200 if batch == 32 else 320,
                        workload_reads=8192 if batch == 32 else 4096,
                        transport_floor=bool(repeat == 0 and value == 256 and batch == 1 and not shard_cache)))
                # Preserve the smaller historical working set as its own
                # explicit case, never a denominator for the enlarged case.
                cases.append(dict(shape=[8192, 32, 32], repetition=repeat,
                    transport=transport, cold=False, cached=False, diagnostics=False,
                    authority_resident_batches=None, shard_cache_bytes=0,
                    preparation_chunk_batches=16, wal_layout='small',
                    workload_batches=320, workload_reads=8192, transport_floor=False))
        if options['diagnostics']:
            for transport in transports:
                # Fresh case/repetition: stage clocks must not contaminate the
                # ordinary no-cache control used for cache comparisons.
                cases.append(dict(shape=[8192, 1, 1], repetition=options['repetitions'],
                    transport=transport, cold=False, cached=False, diagnostics=True,
                    authority_resident_batches=None, shard_cache_bytes=0,
                    preparation_chunk_batches=16, wal_layout='small',
                    workload_batches=320, workload_reads=4096, transport_floor=False))
                for batches in (320, 3200):
                    cases.append(dict(shape=[8192, 32, 32], repetition=options['repetitions'],
                        transport=transport, cold=False, cached=False, diagnostics=True,
                        authority_resident_batches=None, shard_cache_bytes=0,
                        preparation_chunk_batches=16, wal_layout='small',
                        workload_batches=batches, workload_reads=8192, transport_floor=False))
        return resident_plan(cases, options)
    cases = []
    shapes = [(256, 1, 1), (256, 32, 32), (8192, 1, 1), (8192, 32, 32)]
    for repeat in range(options['repetitions']):
        order = transports if repeat % 2 == 0 else transports[::-1]
        for transport in order:
            for shape in shapes:
                for family in ('warm', 'cache', 'cold'):
                    cases.append(dict(shape=list(shape), repetition=repeat, transport=transport,
                                      cold=family == 'cold', cached=family == 'cache', diagnostics=False,
                                      authority_resident_batches=None,
                                      workload_batches=3200 if shape == (8192, 32, 32) and family == 'warm' else None,
                                      cache_hit_contention=shape[1] == 1 and family == 'cache',
                                      transport_floor=repeat == 0 and shape == (256, 1, 1) and family == 'warm'))
            cases.append(dict(shape=[8192, 1, 1], repetition=repeat, transport=transport,
                              cold=True, cached=False, diagnostics=False,
                              authority_resident_batches=32))
    if options['diagnostics']:
        for transport in transports:
            for shape in shapes:
                cases.append(dict(shape=list(shape), repetition=options['repetitions'], transport=transport,
                                  cold=False, cached=False, diagnostics=True,
                                  workload_batches=3200 if shape == (8192, 32, 32) else None,
                                  authority_resident_batches=None))
    # Prioritize the same bounded diagnostic cases; preserve every clean case's order.
    return resident_plan([case for case in cases if case['diagnostics']] + [
        case for case in cases if not case['diagnostics']], options)


def comparison_identity(directory, spec, mode, cfg):
    """Bind observed cardinality/policy to successful source-bound commands.

    Missing historical metadata stays missing: planner defaults are not evidence.
    """
    directory = Path(directory)
    metadata = dict(line.split('=', 1) for line in
                    (directory / 'campaign.txt').read_text().splitlines() if '=' in line)
    process = json.loads((directory.parent / (directory.name + '-process.command.json')).read_text())
    prepare, commands = kv_case_commands(cfg, spec)
    remote, owners = kv_case_policy(cfg, spec)
    required = ('load_batches', 'mixed_batches', 'point_reads', 'read_query_algorithm',
                'read_query_seed', 'read_query_order', 'read_prepared_state',
                'wal_fragment_bytes', 'wal_file_bytes', 'wal_group_max_batches',
                'wal_group_encoded_bytes', 'one_sided_wal_reads', 'rma_config',
                'target_os', 'target_arch', 'read_cpu_affinity', 'read_numa_affinity',
                'read_placement_policy', 'build_features', 'topology')
    missing = [field for field in required if metadata.get(field) in (None, '', 'unavailable')]
    if spec.get('cold_group_probes') and metadata.get('cold_group_probes') in (None, '', 'unavailable'):
        missing.append('cold_group_probes')
    elif 'cold_group_probes' in metadata and metadata['cold_group_probes'] != str(spec.get('cold_group_probes') or 0):
        raise ValueError('observed cold grouped probe count differs from the admitted command')
    expected_bounded = ('--bounded-hot-reads' in commands[mode] or (
        '--bounded-hot-pass' in commands[mode]
        and commands[mode][commands[mode].index('--bounded-hot-pass') + 1] == 'true'))
    if expected_bounded:
        if metadata.get('bounded_hot_policy') in (None, '', 'unavailable'):
            missing.append('bounded_hot_policy')
        if mode == 'native-sync' and metadata.get('native_read_cache_bytes') in (None, '', 'unavailable'):
            missing.append('native_read_cache_bytes')
    if (mode == 'tcp' and resident_read_config(spec.get('resident_reads'))['destination_slots']
            and not spec['cached'] and metadata.get('registered_hot_policy') in (None, '', 'unavailable')):
        missing.append('registered_hot_policy')
    for field in ('region', 'availability_zone', 'instance_type', 'placement_group'):
        if not cfg.get(field):
            missing.append('environment.' + field)
    source = cfg.get('source_archive_sha256')
    binary = process.get('binary_sha256')
    for field, digest in (('source_archive_sha256', source), ('binary_sha256', binary)):
        if (not isinstance(digest, str) or len(digest) != 64
                or any(char not in '0123456789abcdef' for char in digest)):
            missing.append(field)
    if process.get('exit_code') != 0 or process.get('command') != commands[mode]:
        missing.append('successful_exact_command_binding')
    if binary != cfg.get('binaries', {}).get('storage-node'):
        missing.append('admitted_binary_binding')
    case_id = directory.name.removesuffix('-' + mode)
    preparation_path = directory.parent / (case_id + '-prepare.command.json')
    deployment_path = directory.parent / (case_id + '-remote.yaml')
    if not preparation_path.is_file() or not deployment_path.is_file():
        missing.append('observed_preparation_and_deployment_binding')
    else:
        preparation = json.loads(preparation_path.read_text())
        if (preparation.get('exit_code') != 0 or preparation.get('command') != prepare
                or preparation.get('binary_sha256') != binary):
            missing.append('successful_exact_preparation_binding')
        deployment = transport_floor.yaml_documents(deployment_path.read_text())
        if len(deployment) != 1:
            raise ValueError('ambiguous comparison deployment')
        require_kv_policy(deployment[0], remote)
    identity = dict(schema=1, qualified=not missing, missing=missing,
        implementation=dict(binary_sha256=binary, source_archive_sha256=source),
        transport=dict(name=spec['transport'] if mode == 'tcp' else 'native',
            security='unauthenticated-native' if spec['transport'] in
            ('efa-native', 'fabric-tcp-native') and mode == 'tcp' else
            'tls' if mode == 'tcp' else 'local',
            one_sided=metadata.get('one_sided_wal_reads'), build_features=metadata.get('build_features')),
        clocks=dict(detailed=spec['diagnostics']))
    if missing:
        return identity
    observed_rma = resident_read_config(json.loads(metadata['rma_config']))
    if observed_rma != (resident_read_config(spec.get('resident_reads')) if mode == 'tcp' else resident_read_config()):
        raise ValueError('observed resident limits differ from admitted configuration')
    value, batch, window = spec['shape']
    batches = int(metadata['load_batches'])
    reads = int(metadata['point_reads'])
    if (batches != int(prepare[prepare.index('--batches') + 1])
            or reads != int(commands[mode][commands[mode].index('--reads') + 1])
            or int(metadata['mixed_batches']) != 64):
        raise ValueError('observed workload cardinality differs from admitted command')
    warm_policy = registered_hot_policy(reads, remote['read_lanes'],
        mode == 'tcp' and observed_rma['destination_slots'] != 0 and remote['read_cache_bytes'] == 0,
        spec.get('bounded_hot_reads'))
    reported_warm_policy = metadata.get('registered_hot_policy')
    if warm_policy['enabled']:
        require_kv_policy(json.loads(reported_warm_policy), warm_policy)
    elif reported_warm_policy not in (None, '', 'unavailable'):
        inactive = json.loads(reported_warm_policy)
        if not isinstance(inactive, dict) or inactive.get('enabled') is not False:
            raise ValueError('inactive registered preparation contradicts effective configuration')
    if not warm_policy['enabled']:
        warm_policy = dict(enabled=False)
    controls = registered_control_evidence(directory, [node['partition'] for node in cfg['nodes']])
    if observed_rma == resident_read_config() and controls.get('available'):
        raise ValueError('registered controls contradict the effective disabled resident configuration')
    if warm_policy['enabled'] and not controls.get('qualified'):
        identity['qualified'] = False
        identity['missing'].append('registered_hot_preparation_or_population_not_qualified')
    common_policy = bounded_hot_policy(reads, remote['read_lanes'], expected_bounded, spec.get('bounded_hot_reads'))
    reported_common = metadata.get('bounded_hot_policy')
    if expected_bounded:
        require_kv_policy(json.loads(reported_common), common_policy)
    elif reported_common not in (None, '', 'unavailable'):
        inactive = json.loads(reported_common)
        if not isinstance(inactive, dict) or inactive.get('enabled') is not False:
            raise ValueError('bounded preparation contradicts the source-bound command')
    common = bounded_control_evidence(directory, mode, [node['partition'] for node in cfg['nodes']])
    if expected_bounded:
        if not common.get('qualified'):
            identity['qualified'] = False
            identity['missing'].append('bounded_hot_preparation_or_population_not_qualified')
        elif common.get('policy') != common_policy:
            raise ValueError('bounded control differs from the source-bound preparation policy')
        expected_case = ['native_value_wal_barrier' if mode == 'native-sync' else TRANSPORT_BACKENDS[spec['transport']],
                         'tcp' if mode == 'native-sync' else spec['transport'], *spec['shape'], 0]
        if common.get('available') and common.get('identity') != expected_case:
            raise ValueError('bounded control case differs from its source-bound identity')
        if mode == 'native-sync' and metadata['native_read_cache_bytes'] != str(remote['read_cache_bytes']):
            raise ValueError('native bounded cache budget differs from its admitted command')
    elif common.get('available'):
        raise ValueError('bounded controls contradict the disabled source-bound command')
    if warm_policy['enabled'] and controls.get('qualified'):
        require_registered_probe(spec, mode, resident_read_evidence(
            directory, [node['partition'] for node in cfg['nodes']]))
    with (directory / 'summary.csv').open() as stream:
        observed = list(csv.DictReader(stream))
    load = [row for row in observed if row['phase'] == 'load_write_window']
    scalar = [row for row in observed if row['phase'] == 'point_get']
    if (len(load) != 1 or len(scalar) != 1
            or int(load[0]['logical_operations']) != batches * batch
            or int(load[0]['value_bytes']) != value
            or int(load[0]['batch_size']) != batch
            or int(scalar[0]['samples']) != reads):
        raise ValueError('actual loaded/read population differs from command identity')
    identity['workload'] = dict(value_bytes=value, batch_size=batch, write_window=window,
        loaded_batches=batches, loaded_keys=batches * batch, loaded_payload_bytes=batches * batch * value,
        mixed_batches=int(metadata['mixed_batches']), read_count=reads,
        read_order=metadata['read_query_order'], read_seed=metadata['read_query_seed'],
        read_algorithm=metadata['read_query_algorithm'], prepared_state=metadata['read_prepared_state'],
        hot_read_pass=not spec['cold'], advisory_eviction=spec['cold'],
        read_window=32, get_many_widths=[4, 32],
        hot_set_keys=256 if spec.get('shard_cache_bytes', 0) and not spec['cached'] else 0)
    identity['workload']['requested_owner_cache_bytes'] = spec.get('shard_cache_bytes', 0)
    if spec.get('cold_single_probes') is not None:
        identity['workload']['cold_single_probes'] = spec['cold_single_probes']
    if spec.get('cold_group_probes') is not None:
        identity['workload']['cold_group_probes'] = spec['cold_group_probes']
    identity['policy'] = dict(
        coordinator_cache_bytes=remote['read_cache_bytes'],
        shard_cache_bytes=spec.get('shard_cache_bytes', 0) if mode == 'tcp' else 0,
        coordinator_allowance=remote['read_memory_allowance_bytes'],
        shard_allowances=sorted(owner['node']['read_memory_allowance_bytes'] for owner in owners.values()) if mode == 'tcp' else [],
        read_lanes=remote['read_lanes'] if mode == 'tcp' else 0,
        max_pending_reads=remote['max_pending_reads'] if mode == 'tcp' else 1,
        max_pending_read_bytes=remote['max_pending_read_bytes'] if mode == 'tcp' else None,
        member_read='async' if mode == 'tcp' else 'native-file-reader',
        resident_first=mode == 'tcp', polling='adaptive' if mode == 'tcp' else 'not_applicable',
        read_workers=2 if mode == 'tcp' else 1,
        authority_resident_batches=remote['storage']['max_batches_per_partition'] if mode == 'tcp' else None,
        registration_profile=observed_rma)
    identity['preparation'] = dict(chunk_batches=spec.get('preparation_chunk_batches', 16),
        workers=remote['preparation_workers'], scan_prefetch_pages=remote['scan_prefetch_pages'],
        layout={key: int(metadata[key]) for key in required[7:11]},
        registered_hot_policy=warm_policy,
        registered_hot_state=('qualified_all_lane_hit_only' if controls.get('qualified') else 'mixed_or_invalid')
            if warm_policy['enabled'] else 'not_applicable')
    identity['preparation']['bounded_hot_policy'] = common_policy if expected_bounded else dict(enabled=False)
    identity['preparation']['bounded_hot_state'] = (
        'verified_API_population' if common.get('qualified') else 'mixed_or_invalid') if expected_bounded else 'not_applicable'
    identity['bounded_hot'] = common
    identity['preparation']['registered_hot_active_lanes'] = (
        controls.get('preparation', {}).get('active_lanes', []) if warm_policy['enabled'] else [])
    identity['preparation']['registered_hot_owner_elements'] = {
        phase: {str(owner): sum(lane['counters']['rma_posts'] for lane in observation['lanes']
                    if lane['owner'] == owner) for owner in sorted({lane['owner'] for lane in observation['lanes']})}
        for phase, observation in controls.get('phases', {}).items()
    } if warm_policy['enabled'] else {}
    if warm_policy['enabled']:
        identity['registered_control_observations'] = controls
    identity['environment'] = dict(
        region=cfg['region'], availability_zone=cfg['availability_zone'], instance_type=cfg['instance_type'],
        placement_strategy='cluster', topology=metadata['topology'],
        target_os=metadata['target_os'], target_arch=metadata['target_arch'],
        cpu_affinity=metadata['read_cpu_affinity'], numa_affinity=metadata['read_numa_affinity'],
        placement_policy=metadata['read_placement_policy'],
        placement_scope='coordinator_allowed_masks_not_per_thread_pinned_core_proof')
    if spec.get('cold_single_probes'):
        physical = physical_cold_probe_evidence(directory, spec, mode, [node['partition'] for node in cfg['nodes']])
        identity['physical_cold'] = {key: value for key, value in physical.items() if key != 'probes'}
    if spec.get('cold_group_probes'):
        physical = physical_cold_group_probe_evidence(directory, spec, mode, [node['partition'] for node in cfg['nodes']])
        identity['physical_cold_group'] = {key: value for key, value in physical.items() if key != 'probes'}
    return identity


def match_comparison(reference, candidate, *, compared=('implementation',)):
    """Require matching controls, but retain provenance for intentional A/B axes."""
    if not set(compared) <= {'implementation', 'transport', 'clocks'}:
        raise ValueError('comparison axes must be implementation, transport, or clocks')
    required = dict(
        implementation={'binary_sha256', 'source_archive_sha256'},
        transport={'name', 'security', 'one_sided', 'build_features'}, clocks={'detailed'},
        environment={'region', 'availability_zone', 'instance_type', 'placement_strategy',
            'topology', 'target_os', 'target_arch', 'cpu_affinity', 'numa_affinity',
            'placement_policy', 'placement_scope'},
        workload={'value_bytes', 'batch_size', 'write_window', 'loaded_batches', 'loaded_keys',
            'loaded_payload_bytes', 'mixed_batches', 'read_count', 'read_order', 'read_seed',
            'read_algorithm', 'prepared_state', 'hot_read_pass', 'advisory_eviction',
            'read_window', 'get_many_widths', 'hot_set_keys'},
        policy={'coordinator_cache_bytes', 'shard_cache_bytes', 'coordinator_allowance',
            'shard_allowances', 'read_lanes', 'max_pending_reads', 'max_pending_read_bytes',
            'member_read', 'resident_first', 'polling', 'read_workers',
            'authority_resident_batches', 'registration_profile'},
        preparation={'chunk_batches', 'workers', 'scan_prefetch_pages', 'layout',
                     'registered_hot_policy', 'registered_hot_state',
                     'registered_hot_active_lanes', 'registered_hot_owner_elements',
                     'bounded_hot_policy', 'bounded_hot_state'})
    fields, missing = set(required), {}
    for label, row in (('reference', reference), ('candidate', candidate)):
        absent = list(row.get('missing', []))
        transport = row.get('transport') if isinstance(row.get('transport'), dict) else {}
        if row.get('schema') != 1:
            absent.append('schema')
        for section, names in required.items():
            values = row.get(section)
            if not isinstance(values, dict):
                absent.append(section)
                continue
            for name in names:
                native_none = (section == 'policy' and transport.get('name') == 'native'
                               and name in ('max_pending_read_bytes', 'authority_resident_batches'))
                if name not in values or (values[name] is None and not native_none) or values.get(name) == '':
                    absent.append(section + '.' + name)
        for section, child, names in (
                ('preparation', 'layout', {'wal_fragment_bytes', 'wal_file_bytes',
                                         'wal_group_max_batches', 'wal_group_encoded_bytes'}),
                ('policy', 'registration_profile', set(resident_read_config()))):
            parent = row.get(section) if isinstance(row.get(section), dict) else {}
            values = parent.get(child, {})
            if (not isinstance(values, dict) or not names <= set(values)
                    or any(values.get(name) is None or values.get(name) == '' for name in names)):
                absent.append(section + '.' + child)
        missing[label] = sorted(set(absent))
    if any(missing.values()) or not reference.get('qualified') or not candidate.get('qualified'):
        return dict(comparable=False, reason='unqualified_metadata', missing=missing, ratios_allowed=False)
    mismatches = sorted(field for field in fields - set(compared)
                        if reference[field] != candidate[field])
    return dict(comparable=not mismatches, reason='matched' if not mismatches else 'control_mismatch',
        mismatched_fields=mismatches, compared=list(compared), ratios_allowed=not mismatches,
        provenance={label: row['implementation'] for label, row in
                    (('reference', reference), ('candidate', candidate))})


def match_bounded_hot(reference, candidate, *, compared=('implementation', 'transport')):
    """Explicit Native/remote32-key comparison; the full-oracle matcher stays strict."""
    if not set(compared) <= {'implementation', 'transport', 'clocks'}:
        raise ValueError('comparison axes must be implementation, transport, or clocks')
    for label, row in (('reference', reference), ('candidate', candidate)):
        if not match_comparison(row, row)['comparable']:
            return dict(comparable=False, ratios_allowed=False, reason='unqualified_metadata', role=label)
        bounded = row.get('bounded_hot', {})
        if (not isinstance(bounded, dict) or not bounded.get('qualified') or not bounded.get('service_qualified')
                or not isinstance(bounded.get('policy'), dict)
                or set(bounded.get('phases', {})) != {'cache_hot_get', 'cache_hot_get_many'}
                or any(not phase.get('qualified') or not phase.get('service_qualified')
                       for phase in bounded['phases'].values())):
            return dict(comparable=False, ratios_allowed=False, reason='bounded_service_unqualified', role=label)
    native = [row['transport']['name'] == 'native' for row in (reference, candidate)]
    if native[0] == native[1]:
        result = match_comparison(reference, candidate, compared=compared)
        result['population'] = 'bounded_hot32'
        return result
    if not {'implementation', 'transport'} <= set(compared):
        return dict(comparable=False, ratios_allowed=False, reason='name_native_remote_comparison_axes')
    def controls(row):
        return dict(workload=row['workload'], bounded_policy=row['bounded_hot']['policy'],
            preparation={field: row['preparation'][field] for field in
                ('chunk_batches', 'workers', 'scan_prefetch_pages', 'layout', 'bounded_hot_state')},
            policy={field: row['policy'][field] for field in
                ('coordinator_cache_bytes', 'coordinator_allowance')},
            environment={field: value for field, value in row['environment'].items() if field != 'topology'})
    left, right = controls(reference), controls(candidate)
    mismatches = [field for field in left if left[field] != right[field]]
    if 'clocks' not in compared and reference['clocks'] != candidate['clocks']:
        mismatches.append('clocks')
    # Native has no remote lanes, owner pools, or registered destinations. These
    # actual role-specific profiles are the explicitly named implementation/
    # transport axis, not invented equal budgets. Same-role comparisons above
    # still require their exact full policies to match.
    return dict(comparable=not mismatches, ratios_allowed=not mismatches,
        reason='matched_bounded32' if not mismatches else 'control_mismatch',
        population='bounded_hot32', mismatched_fields=sorted(mismatches), compared=list(compared),
        provenance={label: dict(implementation=row['implementation'], transport=row['transport'],
            actual_runtime_policy=row['policy'], topology=row['environment']['topology'],
            populations=row['bounded_hot']['phases'])
            for label, row in (('reference', reference), ('candidate', candidate))},
        scope='only_cache_hot_get_and_cache_hot_get_many;not_legacy_oracle_ratios')


def match_physical_cold(reference, candidate, *, compared=()):
    """Matched scalar cold API latencies; every probe qualifies independently."""
    return _match_physical_population(reference, candidate, compared=compared,
        evidence_field='physical_cold', qualification_field='physical_cold_api_latency_qualified',
        population='all_probes_physical_cold_API')


def match_physical_cold_group(reference, candidate, *, compared=()):
    """Complete 32-member calls, never a scalar-derived per-member device metric."""
    result = _match_physical_population(reference, candidate, compared=compared,
        evidence_field='physical_cold_group', qualification_field='physical_cold_group_api_latency_qualified',
        population='all_probes_physical_cold_complete32_API')
    result['scope'] = 'complete_get_many32_call_with_positive_process_IO_on_each_expected_owner;not_per_member_device_service_or_device_IOPS'
    return result


def _match_physical_population(reference, candidate, *, compared, evidence_field, qualification_field, population):
    if compared:
        raise ValueError('physical-cold comparison holds source and clocks fixed')
    rows = (reference, candidate)
    if any(not match_comparison(row, row)['comparable'] for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='unqualified_metadata')
    if any(not row.get(evidence_field, {}).get(qualification_field) for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='not_every_physical_probe_qualified')
    if any(not row['workload']['advisory_eviction'] or row['policy']['registration_profile']['destination_slots']
           for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='requires_RMA_off_physical_cold_controls')
    native = [row['transport']['name'] == 'native' for row in rows]
    if native[0] == native[1]:
        result = match_comparison(reference, candidate, compared=('transport',))
    else:
        def controls(row):
            return dict(implementation=row['implementation'], workload=row['workload'], clocks=row['clocks'],
                preparation=row['preparation'],
                policy={field: row['policy'][field] for field in
                        ('coordinator_cache_bytes', 'coordinator_allowance')},
                environment={key: value for key, value in row['environment'].items() if key != 'topology'})
        left, right = controls(reference), controls(candidate)
        mismatch = [field for field in left if left[field] != right[field]]
        result = dict(comparable=not mismatch, ratios_allowed=not mismatch,
                      reason='matched_physical_cold_API' if not mismatch else 'control_mismatch',
                      mismatched_fields=mismatch)
    result.update(population=population,
        provenance={label: dict(implementation=row['implementation'], transport=row['transport'],
                               actual_runtime_policy=row['policy'], **{evidence_field: row[evidence_field]})
                    for label, row in zip(('reference', 'candidate'), rows)},
        scope='normal_API_latency_with_positive_expected_owner_IO_each_probe;not_bare_device_latency_or_IOPS')
    return result


def match_registered_arm(reference, candidate, *, compared=()):
    """Explicit same-provider OFF/ON axis; do not relax ordinary source matching."""
    if compared:
        raise ValueError('registered-arm comparison holds source, transport and clocks fixed')
    rows = (reference, candidate)
    if any(not match_comparison(row, row)['comparable'] for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='unqualified_metadata')
    if any(row['transport']['name'] != 'efa-native' for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='same_efa_native_provider_required')
    enabled = [row['policy']['registration_profile']['destination_slots'] != 0 for row in rows]
    if enabled[0] == enabled[1]:
        return dict(comparable=False, ratios_allowed=False, reason='requires_distinct_off_on_arms')
    if any(not row.get('bounded_hot', {}).get('service_qualified') for row in rows):
        return dict(comparable=False, ratios_allowed=False, reason='bounded_service_unqualified')
    normalized = []
    for row in rows:
        value = copy.deepcopy(row)
        value['transport']['one_sided'] = 'explicit_registration_axis'
        value['policy']['registration_profile'] = resident_read_config()
        for field in ('registered_hot_policy', 'registered_hot_state',
                      'registered_hot_active_lanes', 'registered_hot_owner_elements'):
            value['preparation'][field] = 'explicit_registration_axis'
        normalized.append(value)
    result = match_comparison(*normalized, compared=())
    result.update(population='bounded_hot32_registered_off_on',
        scope='only_cache_hot_get_and_cache_hot_get_many;full_oracle_may_mix_RPC_RMA',
        provenance={label: dict(implementation=row['implementation'], transport=row['transport'],
            actual_runtime_policy=row['policy'], preparation=row['preparation'])
            for label, row in zip(('reference', 'candidate'), rows)})
    return result


def classify_read_population(frontend, owners, physical_read_bytes, residency=None):
    """Async submissions and advisory eviction never establish physical NVMe."""
    if frontend is None or owners is None:
        return dict(population='unqualified_missing_counters', hit_only=False, physical_nvme=False)
    hits, misses = frontend.get('cache_hits', 0), frontend.get('cache_misses', 0)
    shard_hits = sum(owner.get('cache_hits', 0) for owner in owners.values())
    wal_reads = sum(owner.get('wal_reads', 0) for owner in owners.values())
    resident = sum(owner.get('resident_read_successes', 0) for owner in owners.values())
    if hits and not misses:
        population = 'coordinator_cache_hit_only'
    elif hits:
        population = 'mixed_coordinator_cache'
    elif shard_hits and shard_hits == misses and not wal_reads:
        population = 'shard_cache_hit_only'
    elif shard_hits:
        population = 'mixed_shard_cache'
    elif resident:
        population = 'page_cache_resident_observed'
    else:
        population = 'cache_state_not_established'
    return dict(population=population, hit_only=population.endswith('hit_only'),
        physical_nvme=False, charged_physical_read_bytes=physical_read_bytes,
        physical_io_observed=physical_read_bytes is not None and physical_read_bytes > 0,
        residency=residency, advisory_eviction_proves_cold=False,
        async_submissions_prove_device_io=False)


def bounded_hot_policy(reads, read_lanes, enabled, bounded_hot_reads=None):
    registered = registered_hot_policy(reads, read_lanes, enabled, bounded_hot_reads)
    return {field: value for field, value in registered.items()
            if field not in ('read_lanes', 'max_warm_passes', 'required_steady_passes')} | dict(
                warm_passes=registered['max_warm_passes'])


def bounded_control_evidence(directory, mode, owner_partitions=range(5)):
    """Verify the common bounded API population without inventing native RMA counters."""
    directory = Path(directory)
    path = directory / 'bounded_hot_controls.jsonl'
    if not path.is_file() or not path.stat().st_size:
        return dict(available=False, qualified=False, reason='bounded hot control evidence unavailable')
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    phases, identities = {}, set()
    for row in rows:
        if (row.get('scope') != 'verified_API_values;RMA_counters_only_in_registered_controls'
                or row['phase'] in phases):
            raise ValueError('bounded control scope or phase population differs')
        phases[row['phase']] = row
        identities.add(tuple(row[field] for field in
            ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition')))
    if (len(identities) != 1
            or set(phases) != {'bounded_hot_setup', 'cache_hot_get', 'cache_hot_get_many'}):
        raise ValueError('bounded control case/phase population differs')
    identity = next(iter(identities))
    native = mode == 'native-sync'
    expected_backend = 'native_value_wal_barrier' if native else TRANSPORT_BACKENDS.get(identity[1])
    if identity[0] != expected_backend:
        raise ValueError('bounded control backend differs')
    policy = phases['bounded_hot_setup']['policy']
    if (not isinstance(policy, dict) or type(policy.get('warm_passes')) is not int
            or policy['warm_passes'] not in range(3, 18, 2)):
        raise ValueError('bounded control warm-pass policy differs')
    require_kv_policy(policy, bounded_hot_policy(
        policy['scalar_reads'], (policy['warm_passes'] - 1) // 2, True, policy.get('bounded_hot_reads')))
    setup = phases['bounded_hot_setup']['observation']
    warm_elements = policy['scalar_reads'] + policy['group_calls'] * policy['group_width']
    if (setup.get('kind') != 'preparation'
            or type(setup.get('completed_passes')) is not int
            or setup['completed_passes'] != policy['warm_passes']
            or type(setup.get('verified_elements')) is not int
            or setup['verified_elements'] != policy['warm_passes'] * warm_elements
            or type(setup.get('setup_wall_ns')) is not int or setup['setup_wall_ns'] <= 0):
        raise ValueError('bounded control omitted its exact completed preparation')
    with (directory / 'summary.csv').open() as stream:
        summary = list(csv.DictReader(stream))
    setup_timing = [row for row in summary if row.get('phase') == 'bounded_hot_setup']
    if (len(setup_timing) != 1 or setup_timing[0].get('samples') != '1'
            or setup_timing[0].get('logical_operations') != str(setup['verified_elements'])
            or setup_timing[0].get('phase_wall_ns') != str(setup['setup_wall_ns'])
            or any(setup_timing[0].get(field) != str(value) for field, value in zip(
                ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition'), identity, strict=True))):
        raise ValueError('bounded control preparation timing/cardinality differs')
    io_path = directory / 'read_io.csv'
    if io_path.is_file():
        with io_path.open() as stream:
            physical_rows = list(csv.DictReader(stream))
    else:
        physical_rows = []
    metadata_path = directory / 'campaign.txt'
    metadata = dict(line.split('=', 1) for line in metadata_path.read_text().splitlines() if '=' in line) if metadata_path.is_file() else {}
    expected_owners = {0} if native else set(owner_partitions)
    registered = None
    results, reasons = {}, []
    for phase, samples, elements in (
            ('cache_hot_get', policy['scalar_reads'], policy['scalar_reads']),
            ('cache_hot_get_many', policy['group_calls'], policy['group_calls'] * policy['group_width'])):
        row = phases[phase]
        require_kv_policy(row['policy'], policy)
        observation = row['observation']
        if (observation.get('kind') != 'measurement'
                or type(observation.get('samples')) is not int or observation['samples'] != samples
                or type(observation.get('verified_elements')) is not int or observation['verified_elements'] != elements
                or type(observation.get('physical_io_available')) is not bool):
            raise ValueError('bounded control measurement cardinality differs')
        measured_phase = observation['measurement_phase']
        # Cache budgets are not hit evidence: consume the measured service interval.
        service = (read_service_evidence(directory, mode, owner_partitions).get(measured_phase)
                   if not native and (directory / 'read_path_evidence.csv').is_file() else None)
        timing = [row for row in summary if row.get('phase') == measured_phase]
        timing_matches = (len(timing) == 1 and timing[0].get('samples') == str(samples)
            and timing[0].get('logical_operations') == str(elements)
            and all(timing[0].get(field) == str(value) for field, value in zip(
                ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition'), identity, strict=True)))
        physical = {}
        for entry in (row for row in physical_rows if row.get('phase') == measured_phase):
            if any(entry.get(field) != str(value) for field, value in zip(
                    ('backend', 'value_bytes', 'batch_size', 'window', 'repetition'),
                    (identity[0], *identity[2:]), strict=True)):
                raise ValueError('bounded physical-I/O case identity differs')
            owner = int(entry['partition'])
            values = {field: int(entry[field]) for field in
                ('pid', 'physical_read_bytes', 'minor_faults', 'major_faults',
                 'file_bytes', 'resident_bytes_before', 'resident_bytes_after')}
            if owner in physical or min(values.values()) < 0 or values['pid'] == 0:
                raise ValueError('bounded physical-I/O population differs')
            physical[owner] = values
        if observation['physical_io_available'] != bool(physical):
            raise ValueError('bounded physical-I/O availability differs from actual rows')
        if physical and set(physical) != expected_owners:
            raise ValueError('bounded physical-I/O lacks its exact owner population')
        charged = sum(row['physical_read_bytes'] for row in physical.values()) if physical else None
        population = observation['population']
        actual_population = 'unclassified_missing_service_evidence'
        service_qualified = False
        if native:
            if population != 'native_API_hotset_unclassified' or measured_phase != phase:
                raise ValueError('native bounded control cannot claim registered-hit service')
            cache_bytes = metadata.get('native_read_cache_bytes')
            if (cache_bytes == '0' and charged == 0
                    and all(row['file_bytes'] > 0 and row['major_faults'] == 0
                            and row['resident_bytes_before'] > 0 and row['resident_bytes_after'] > 0
                            for row in physical.values())):
                actual_population = 'native_buffered_hotset_zero_accounted_IO_with_residency'
                service_qualified = True
        elif population in ('registered_hit_only', 'mixed_or_invalid'):
            if registered is None:
                registered = registered_control_evidence(directory, expected_owners)
            observation_rma = registered.get('phases', {}).get(phase, {})
            if (registered.get('qualified') and observation_rma.get('qualified')
                    and observation_rma.get('measurement_phase') == measured_phase
                    and registered.get('identity') == list(identity)):
                actual_population = 'registered_hit_only'
                service_qualified = True
        elif population != 'remote_API_hotset_unclassified' or measured_phase != phase:
            raise ValueError('unknown bounded service population')
        elif service is not None and charged == 0:
            classification = service['classification']['population']
            frontend = service['frontend'] or {}
            owners = list(service['owners'].values())
            complete = frontend.get('cache_hits', 0) + frontend.get('cache_misses', 0) == elements
            owner_hits = sum(owner['cache_hits'] for owner in owners)
            wal_reads = sum(owner['wal_reads'] for owner in owners)
            if (complete and classification == 'coordinator_cache_hit_only'
                    and frontend.get('cache_hits') == elements and not owner_hits and not wal_reads):
                actual_population, service_qualified = classification, True
            elif (complete and classification == 'shard_cache_hit_only'
                    and owner_hits == elements and not wal_reads):
                actual_population, service_qualified = classification, True
            elif (complete and classification == 'page_cache_resident_observed'
                    and not owner_hits and wal_reads == elements):
                actual_population = 'remote_pagecache_service_observed_zero_accounted_IO'
                service_qualified = True
        if not timing_matches:
            reasons.append(phase + '_latency_population_mismatch')
        results[phase] = dict(qualified=timing_matches, samples=samples, verified_elements=elements,
            measurement_phase=measured_phase, population=actual_population,
            service_qualified=service_qualified, physical_by_owner={str(owner): value for owner, value in physical.items()},
            charged_physical_read_bytes=charged, physical_nvme=False)
    return dict(available=True, qualified=not reasons, reasons=reasons, policy=policy,
        identity=list(identity), preparation=setup, phases=results,
        service_qualified=all(row['service_qualified'] for row in results.values()),
        scope='same_bounded32_API_workload;Native_and_RPC_are_not_RMA_counter_populations')


REGISTERED_COUNTERS = (
    'message_read_requests', 'grant_acquisitions', 'eligible_reads', 'rma_posts',
    'rma_completions', 'rma_hits', 'rma_errors', 'fallback_missing',
    'fallback_oversize', 'fallback_capacity', 'fallback_unsupported', 'fallback_stale')


def registered_hot_policy(reads, read_lanes, enabled, bounded_hot_reads=None):
    if bounded_hot_reads is not None and (type(bounded_hot_reads) is not int or not 32 <= bounded_hot_reads <= 131072):
        raise ValueError('bounded_hot_reads requires an integer 32..131072')
    scalar = bounded_hot_reads if bounded_hot_reads is not None else min(512, max(32, reads))
    return (dict(bounded_hot_reads=bounded_hot_reads) if bounded_hot_reads is not None else {}) | dict(
        enabled=enabled, hot_keys=32, scalar_reads=scalar, group_width=32,
        group_calls=(scalar + 31) // 32, read_lanes=read_lanes,
        max_warm_passes=2 * read_lanes + 1, required_steady_passes=read_lanes,
        scalar_order='mix64-v1(sample)%32', group_order='unique-prefix-keys-0-through-31',
        preparation='post-load-before-other-reads;fixed-passes;exact-scalar-then-group-sequence')


def registered_only_hits(counters, elements=None, groups=None):
    """Groups are not FI_READ elements: a verified 32-member group is one hit."""
    if any(type(counters.get(field)) is not int or counters[field] < 0 for field in REGISTERED_COUNTERS):
        return False
    no_rpc = ('message_read_requests', 'grant_acquisitions', 'rma_errors',
              'fallback_missing', 'fallback_oversize', 'fallback_capacity',
              'fallback_unsupported', 'fallback_stale')
    return (all(counters[field] == 0 for field in no_rpc)
        and 0 < counters['eligible_reads'] == counters['rma_hits'] <= counters['rma_posts']
        and counters['rma_posts'] == counters['rma_completions']
        and (elements is None or counters['rma_posts'] == elements)
        and (groups is None or counters['eligible_reads'] == groups))


def registered_lane_population(before, after):
    """Recompute actual lane deltas; aggregate counts never establish lane coverage."""
    def indexed(rows):
        result = {}
        for row in rows:
            if (not isinstance(row, list) or len(row) != 3
                    or any(type(value) is not int or value < 0 for value in row[:2])
                    or not isinstance(row[2], dict)):
                raise ValueError('invalid registered owner/lane snapshot')
            identity = tuple(row[:2])
            if identity in result:
                raise ValueError('duplicate registered owner/lane snapshot')
            if any(type(row[2].get(field)) is not int or row[2][field] < 0 for field in REGISTERED_COUNTERS):
                raise ValueError('registered lane counter population is unavailable')
            result[identity] = row[2]
        return result
    left, right = indexed(before), indexed(after)
    if not left or set(left) != set(right):
        raise ValueError('registered lane population changed')
    total = dict.fromkeys(REGISTERED_COUNTERS, 0)
    lanes = []
    for identity in sorted(left):
        delta = {field: right[identity][field] - left[identity][field] for field in REGISTERED_COUNTERS}
        if any(value < 0 for value in delta.values()):
            raise ValueError('registered lane counter reset')
        if not delta['rma_hits'] <= delta['rma_completions'] <= delta['rma_posts']:
            raise ValueError('registered lane group/element population is inconsistent')
        for field, value in delta.items():
            total[field] += value
        lanes.append(dict(owner=identity[0], lane=identity[1], counters=delta))
    return dict(totals=total, lanes=lanes)


def registered_control_evidence(directory, owner_partitions=range(5)):
    """Qualify both bounded timing populations using their raw lane evidence."""
    path = Path(directory) / 'registered_controls.jsonl'
    if not path.is_file():
        return dict(available=False, qualified=False, reason='registered control evidence unavailable')
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if not rows:
        return dict(available=False, qualified=False, reason='registered controls not requested')
    phases = {}
    identities = set()
    for row in rows:
        if (row.get('scope') != 'actual_read_lane_counters;eligible_and_hits_are_groups;posts_and_completions_are_elements'
                or row.get('source_occupancy_available') is not False):
            raise ValueError('registered control lane scope/source occupancy claim differs')
        phase = row['phase']
        if phase in phases:
            raise ValueError('duplicate registered control phase')
        identities.add(tuple(row[field] for field in
            ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition')))
        phases[phase] = row['observation']
    if len(identities) != 1 or 'registered_hot_setup' not in phases:
        raise ValueError('registered control case/preparation identity differs')
    setup = phases['registered_hot_setup']
    if setup.get('kind') == 'unavailable':
        return dict(available=True, qualified=False, reason=setup['reason'], policy=setup['policy'],
                    identity=list(next(iter(identities))), phases={})
    if setup.get('kind') != 'preparation':
        raise ValueError('registered control lacks its preparation observation')
    policy = setup['policy']
    if (not isinstance(policy, dict) or type(policy.get('read_lanes')) is not int
            or not 1 <= policy['read_lanes'] <= 8 or type(policy.get('scalar_reads')) is not int
            or policy != registered_hot_policy(policy['scalar_reads'], policy['read_lanes'], True, policy.get('bounded_hot_reads'))):
        raise ValueError('registered hot preparation policy differs')
    require_kv_policy(policy, registered_hot_policy(policy['scalar_reads'], policy['read_lanes'], True, policy.get('bounded_hot_reads')))
    snapshots = setup['lane_snapshots']
    if len(snapshots) != policy['max_warm_passes'] + 1:
        raise ValueError('registered preparation pass bound differs')
    owners = set(owner_partitions)
    expected_population = {(owner, lane) for owner in owners for lane in range(policy['read_lanes'])}
    if {(row[0], row[1]) for row in snapshots[0]} != expected_population:
        raise ValueError('registered preparation lacks the configured owner/lane population')
    if len(setup['pass_populations']) != len(snapshots) - 1:
        raise ValueError('registered preparation omitted pass counter populations')
    active, covered, steady = set(), set(), 0
    warm_elements = policy['scalar_reads'] + policy['group_calls'] * policy['group_width']
    for index, (before, after) in enumerate(zip(snapshots, snapshots[1:])):
        population = registered_lane_population(before, after)
        reported = setup['pass_populations'][index]
        if (set(reported) != set(REGISTERED_COUNTERS)
                or any(type(reported[field]) is not int for field in REGISTERED_COUNTERS)
                or reported != population['totals']):
            raise ValueError('registered preparation counters differ from raw lane evidence')
        for lane in population['lanes']:
            counters = lane['counters']
            if counters['message_read_requests'] or counters['rma_posts']:
                active.add(lane['owner'])
            if counters['rma_hits']:
                covered.add((lane['owner'], lane['lane']))
        expected = {(owner, lane) for owner in active for lane in range(policy['read_lanes'])}
        steady = steady + 1 if expected and expected <= covered and registered_only_hits(
            population['totals'], warm_elements,
            policy['scalar_reads'] + policy['group_calls'] * len(active)) else 0
    expected = {(owner, lane) for owner in active for lane in range(policy['read_lanes'])}
    ready = bool(expected) and expected <= covered and steady >= policy['required_steady_passes']
    if (setup['steady_passes'] != steady
            or setup['expected_active_lanes'] != [list(lane) for lane in sorted(expected)]
            or setup['covered_lanes'] != [list(lane) for lane in sorted(covered)]
            or setup['status'] != ('ready' if ready else 'mixed_or_invalid')
            or type(setup.get('setup_wall_ns')) is not int or setup['setup_wall_ns'] <= 0):
        raise ValueError('registered preparation lane/steady-state claim differs from evidence')
    summary_path = Path(directory) / 'summary.csv'
    with summary_path.open() as stream:
        summary = list(csv.DictReader(stream))
    identity = next(iter(identities))
    results = {}
    reasons = [] if ready else ['preparation_not_all_lane_steady']
    for phase, elements, groups in (
            ('cache_hot_get', policy['scalar_reads'], policy['scalar_reads']),
            ('cache_hot_get_many', policy['group_calls'] * policy['group_width'], policy['group_calls'] * len(active))):
        observation = phases.get(phase)
        if not observation:
            reasons.append(phase + '_missing')
            continue
        if (observation.get('kind') != 'measurement' or observation['policy'] != policy
                or observation['expected_elements'] != elements or observation['expected_groups'] != groups):
            raise ValueError('registered measurement population/policy differs')
        require_kv_policy(observation['policy'], policy)
        population = registered_lane_population(observation['before'], observation['after'])
        if {(lane['owner'], lane['lane']) for lane in population['lanes']} != expected_population:
            raise ValueError('registered measurement lacks the configured owner/lane population')
        if (any(type(observation['counters'].get(field)) is not int for field in REGISTERED_COUNTERS)
                or observation['counters'] != population['totals']):
            raise ValueError('registered measurement counters differ from raw lane evidence')
        pure = ready and registered_only_hits(population['totals'], elements, groups)
        claimed = observation.get('population') == 'registered_hit_only'
        measurement_phase = phase if pure else phase + '_mixed'
        if claimed != pure or observation.get('measurement_phase') != measurement_phase:
            reasons.append(phase + '_producer_claim_invalid')
        if not pure:
            reasons.append(phase + '_not_hit_only')
        timing = [row for row in summary if row.get('phase') == observation.get('measurement_phase')]
        timing_matches = (len(timing) == 1
            and timing[0].get('logical_operations') == str(elements)
            and timing[0].get('samples') == str(
                policy['group_calls'] if phase == 'cache_hot_get_many' else policy['scalar_reads'])
            and all(timing[0].get(field) == str(value) for field, value in zip(
                ('backend', 'transport', 'value_bytes', 'batch_size', 'window', 'repetition'), identity, strict=True)))
        if not timing_matches:
            reasons.append(phase + '_latency_population_mismatch')
        qualified = (pure and claimed and observation.get('measurement_phase') == phase and timing_matches)
        results[phase] = dict(qualified=qualified, elements=elements, groups=groups,
            measurement_phase=observation.get('measurement_phase'), counters_hit_only=pure, **population)
    if set(phases) != {'registered_hot_setup', 'cache_hot_get', 'cache_hot_get_many'}:
        raise ValueError('registered control has unexpected phases')
    return dict(available=True, qualified=not reasons, reasons=reasons, policy=policy,
        identity=list(next(iter(identities))), preparation=dict(
            qualified=ready, passes=len(snapshots) - 1, steady_passes=steady,
            setup_wall_ns=setup['setup_wall_ns'], active_lanes=[list(lane) for lane in sorted(expected)]),
        phases=results, scope='bounded32;all_eligible_groups_verified;zero_RPC_acquisition_fallback_errors')


def resident_read_evidence(directory, owner_partitions=range(5)):
    """Actual FI_READ/grant populations, not server-RPC or physical-device clocks."""
    path = Path(directory) / 'resident_reads.jsonl'
    if not path.is_file():
        return dict(available=False, reason='resident-read counters unavailable in this source', phases={})
    counters = ('message_read_requests', 'grants_issued', 'grant_acquisitions', 'eligible_reads', 'rma_posts',
                'rma_completions', 'rma_hits', 'rma_errors', 'fallback_missing',
                'fallback_oversize', 'fallback_capacity', 'fallback_unsupported', 'fallback_stale')
    gauges = ('source_bytes_live', 'source_regions_live', 'source_bytes_quarantined',
              'destination_slots_live', 'handles_live', 'grants_live')
    expected = set(owner_partitions)
    phases = {}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        if (row.get('scope') != 'coordinator_local_per_owner_connection_counters'
                or row.get('boundary') not in ('before', 'after')):
            raise ValueError('resident-read snapshot scope/boundary differs')
        owners = row['owners']
        if (len(owners) != len(expected) or {owner for owner, _ in owners} != expected
                or any(type(value) is not int or value < 0 for _, stats in owners
                       for field in counters + gauges for value in [stats.get(field)])):
            raise ValueError('resident-read snapshot lacks exact owner/counter population')
        pair = phases.setdefault(row['phase'], {})
        if row['boundary'] in pair:
            raise ValueError('duplicate resident-read boundary')
        pair[row['boundary']] = dict(owners)
    result = {}
    for phase, pair in phases.items():
        if set(pair) != {'before', 'after'}:
            raise ValueError('resident-read phase lacks both boundaries')
        owners = {}
        for owner in expected:
            before, after = pair['before'][owner], pair['after'][owner]
            delta = {field: after[field] - before[field] for field in counters}
            if any(value < 0 for value in delta.values()):
                raise ValueError('resident-read counter reset')
            if not delta['rma_hits'] <= delta['rma_completions'] <= delta['rma_posts']:
                raise ValueError('resident-read hit/completion/post population is inconsistent')
            owners[str(owner)] = dict(counters=delta,
                occupancy={field: dict(before=before[field], after=after[field]) for field in gauges})
        total = {field: sum(owner['counters'][field] for owner in owners.values()) for field in counters}
        hit_only = registered_only_hits(total)
        result[phase] = dict(owners=owners, totals=total, registered_hit_only=hit_only,
            population_scope='resident_eligible_attempts_not_all_API_reads',
            source_occupancy_available=False, physical_nvme=False,
            server_handler_timing='not_applicable_to_registered_hits')
    return dict(available=bool(result), phases=result,
                registered_controls=registered_control_evidence(directory, expected),
                scope='coordinator-local per-owner connection deltas, not remote source occupancy')


def require_registered_probe(spec, mode, evidence):
    """Require real all-lane prepared scalar AND unique-32-key hit-only populations."""
    config = resident_read_config(spec.get('resident_reads'))
    if mode != 'tcp' or not config['destination_slots'] or spec['cold'] or spec['cached']:
        return None
    control = evidence.get('registered_controls', {})
    if not evidence.get('available') or not control.get('qualified'):
        raise ValueError('bounded registered probe is mixed/invalid or lacks actual all-lane hit-only evidence')
    expected_policy = registered_hot_policy(spec.get('workload_reads') or 4096, 2, True, spec.get('bounded_hot_reads'))
    require_kv_policy(control['policy'], expected_policy)
    expected_identity = [TRANSPORT_BACKENDS[spec['transport']], spec['transport'], *spec['shape'], 0]
    if control.get('identity') != expected_identity:
        raise ValueError('registered control case identity differs')
    for phase in ('cache_hot_get', 'cache_hot_get_many'):
        aggregate = evidence.get('phases', {}).get(phase)
        lane = control['phases'][phase]
        if (not aggregate or not aggregate['registered_hit_only']
                or any(aggregate['totals'].get(field) != lane['totals'][field] for field in REGISTERED_COUNTERS)):
            raise ValueError('registered lane and owner aggregates disagree or are not hit-only')
        per_owner = {}
        for row in lane['lanes']:
            owner = per_owner.setdefault(str(row['owner']), dict.fromkeys(REGISTERED_COUNTERS, 0))
            for field in REGISTERED_COUNTERS:
                owner[field] += row['counters'][field]
        if (set(per_owner) != set(aggregate['owners'])
                or any(aggregate['owners'][owner]['counters'].get(field) != counters[field]
                       for owner, counters in per_owner.items() for field in REGISTERED_COUNTERS)):
            raise ValueError('registered per-owner lane and aggregate counter populations disagree')
    return dict(qualified=True, policy=control['policy'], preparation=control['preparation'],
                phases=control['phases'], scope='bounded32_registered_hit_only_not_uniform_or_hardware_latency')


def validate_contention(path, spec, mode):
    """Recompute the opt-in hit-only population from raw per-caller API samples."""
    # Each outer case starts a fresh --repetitions 1 process; its internal index is zero.
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if len(rows) != 2:
        raise ValueError('cache contention requires separate one/eight-caller populations')
    backend = 'native_value_wal_barrier' if mode == 'native-sync' else TRANSPORT_BACKENDS[spec['transport']]
    for row, callers in zip(rows, (1, 8)):
        if (row.get('api') != 'distributed::sui::Db::get'
                or row.get('backend') != backend
                or any(type(row.get(key)) is not int for key in
                       ('value_bytes', 'batch_size', 'window', 'repetition'))
                or [row.get(k) for k in ('value_bytes', 'batch_size', 'window')] != spec['shape']
                or row.get('repetition') != 0
                or (mode == 'tcp' and row.get('transport') != spec['transport'])):
            raise ValueError('cache contention API/shape/transport identity differs')
        observed = row['observation']
        samples = observed['get_api_intervals_ns_by_caller']
        writes = observed['write_api_intervals_ns']
        wall = observed['phase_wall_ns']
        if (observed['callers'] != callers or observed['reads_per_caller'] != 1024
                or observed['concurrent_durable_writes'] != 32
                or any(type(observed[key]) is not int for key in
                       ('callers', 'reads_per_caller', 'concurrent_durable_writes'))
                or len(samples) != callers or any(len(values) != 1024 for values in samples)
                or len(writes) != 32 or type(wall) is not int or wall <= 0):
            raise ValueError('cache contention raw sample population/time differs')
        for intervals in [*samples, writes]:
            previous_end = 0
            for interval in intervals:
                if (not isinstance(interval, list) or len(interval) != 2
                        or any(type(ns) is not int for ns in interval)
                        or not previous_end <= interval[0] < interval[1] <= wall):
                    raise ValueError('cache contention API intervals are invalid or unordered')
                previous_end = interval[1]
        overlaps = [sum(start < writes[index // 32][1] and writes[index // 32][0] < end
                        for index, (start, end) in enumerate(intervals)) for intervals in samples]
        if any(count == 0 for count in overlaps) or overlaps != observed.get('overlapping_reads_by_caller'):
            raise ValueError('cache contention has no verified read/write API overlap for each caller')
        available = mode == 'tcp'
        if observed['remote_cache_counter_available'] is not available:
            raise ValueError('cache contention cache-counter availability differs')
        expected = (callers * 1024, 0, 0) if available else (None, None, None)
        if available and any(type(observed.get(key)) is not int for key in
                             ('remote_hits', 'remote_misses', 'remote_read_rpcs')):
            raise ValueError('cache contention counters must be nonnegative integers')
        if tuple(observed.get(name) for name in ('remote_hits', 'remote_misses', 'remote_read_rpcs')) != expected:
            raise ValueError('cache contention lacks verified hit-only, zero-RPC service')
    return rows


def require_clean_clocks(directory):
    """Reject clock-bearing records instead of relabeling mixed runs as clean."""
    directory = Path(directory)
    for name in ('read_profiles.csv', 'native_read_profiles.csv',
                 'remote_read_profiles.csv', 'exchange_profiles.csv'):
        with (directory / name).open() as stream:
            if next(csv.DictReader(stream), None) is not None:
                raise ValueError('clean population contains detailed clocks: ' + name)
    if any(row is not None for row in transport_floor.yaml_documents(
            (directory / 'node_profiles.yaml').read_text())):
        raise ValueError('clean population contains node diagnostic clocks')


def validate_checkpoint_records(directory, spec, mode):
    directory = Path(directory)
    rows = [json.loads(line) for line in (directory / 'checkpoint_diagnostics.jsonl').read_text().splitlines()]
    with (directory / 'write_windows.csv').open() as stream:
        windows = list(csv.DictReader(stream))
    if mode != 'tcp' or not spec['diagnostics']:
        if rows:
            raise ValueError('clean/native population contains checkpoint diagnostic captures')
        return
    if len(rows) != len(windows):
        raise ValueError('diagnostic checkpoint windows are missing or duplicated')
    for row, window in zip(rows, windows):
        if (row.get('diagnostic_available') is not True or row.get('phase') != window['phase']
                or row.get('sample') != int(window['sample'])
                or row.get('backend') != window['backend'] or row.get('transport') != spec['transport']
                or any(row.get(key) != int(window[key]) for key in
                       ('value_bytes', 'batch_size', 'window', 'repetition'))):
            raise ValueError('checkpoint diagnostics lost their exact timed-window identity')
        snapshot = row['snapshot']
        before, after = row['cursor_before'], snapshot['next_cursor']
        records = snapshot['records']
        dropped = snapshot['dropped_records']
        if (type(before) is not int or type(after) is not int or not 0 <= before <= after <= 2**64 - 1
                or type(dropped) is not int or dropped != max(0, after - 128)
                or not isinstance(records, list) or len(records) > 128
                or type(row.get('record_count')) is not int):
            raise ValueError('invalid bounded checkpoint diagnostic cursor/cardinality')
        # The producer starts at cursor zero and evicts exactly one oldest entry
        # on each append to its full 128-entry ring. dropped is a global count,
        # not the number lost since this particular window's before cursor.
        retained_start = max(before, dropped)
        retained_count = after - retained_start
        if (not 0 <= retained_count <= 128 or len(records) != retained_count
                or row['record_count'] != retained_count):
            raise ValueError('checkpoint diagnostics omitted their exact retained suffix')
        for ordinal, record in enumerate(records):
            cursor = record['cursor']
            if type(cursor) is not int or cursor != retained_start + ordinal:
                raise ValueError('checkpoint diagnostic record outside its contiguous retained suffix')
            if record['trigger'] is None:
                if record.get('outcome') is not None or record.get('pressure_wait_ns') is not None:
                    raise ValueError('checkpoint completion must not invent a pressure population')
            elif (record['trigger'] not in ('Tail', 'KnownIdentities', 'PartitionWindow', 'IndexReservation')
                    or record.get('outcome') not in ('AlreadyAvailable', 'AfterDrain', 'AfterExistingCapture',
                                                   'AfterNewCapture', 'CapacityError', 'ProgressError')
                    or type(record.get('pressure_wait_ns')) is not int):
                raise ValueError('unknown or unavailable checkpoint pressure reason/outcome')
            for key, value in record.items():
                if key in ('trigger', 'outcome', 'requested_cut', 'captured_cut'):
                    continue
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError('checkpoint diagnostic span/counter must be nonnegative or unavailable')
            for key in ('requested_cut', 'captured_cut'):
                cut = record.get(key)
                if cut is None:
                    continue
                if (not isinstance(cut, dict) or set(cut) != {'batch', 'operation_index'}
                        or type(cut['operation_index']) is not int or not 0 <= cut['operation_index'] <= 2**32 - 1
                        or not isinstance(cut['batch'], dict) or set(cut['batch']) != {'epoch', 'sequence'}
                        or any(type(value) is not int or not 0 <= value <= 2**64 - 1
                               for value in cut['batch'].values())):
                    raise ValueError('checkpoint diagnostic logical cut is malformed')


def read_service_evidence(directory, mode, owner_partitions=range(5)):
    """Keep page-cache evidence, queued reads and charged device IO distinct."""
    directory = Path(directory)
    expected_owners = {str(partition) for partition in owner_partitions}
    if mode == 'tcp' and not expected_owners:
        raise ValueError('remote read-service evidence needs its configured owner population')
    with (directory / 'read_path_evidence.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    boundaries = {}
    for row in rows:
        identity = (row['phase'], row['role'], row['partition'])
        pair = boundaries.setdefault(identity, {})
        if row['boundary'] in pair or row['boundary'] not in ('before', 'after'):
            raise ValueError('duplicate or invalid read-service evidence boundary')
        pair[row['boundary']] = row
    phases = {}
    fields = ('cache_hits', 'cache_misses', 'wal_reads', 'resident_read_successes', 'async_read_submissions')
    deferred_available = bool(rows) and 'resident_read_deferred' in rows[0]
    if deferred_available:
        fields += ('resident_read_deferred',)
    for (phase, role, partition), pair in boundaries.items():
        if set(pair) != {'before', 'after'}:
            raise ValueError('read-service evidence lacks complete interval')
        delta = {}
        for field in fields:
            before, after = int(pair['before'][field]), int(pair['after'][field])
            if before < 0 or after < before:
                raise ValueError('read-service counter reset or negative evidence')
            delta[field] = after - before
        delta['authority'] = None
        availability = tuple(pair[boundary].get('authority_available') for boundary in ('before', 'after'))
        if any(value not in (None, 'true', 'false') for value in availability):
            raise ValueError('invalid authority-cache availability')
        if availability == ('true', 'true'):
            authority = {}
            for name in ('resident_hits', 'resident_misses', 'contention', 'history_loads',
                         'entries', 'capacity', 'table_bytes'):
                before, after = (int(pair[boundary]['authority_' + name]) for boundary in ('before', 'after'))
                if min(before, after) < 0:
                    raise ValueError('negative authority-cache observation')
                if name in ('entries', 'capacity', 'table_bytes'):
                    authority[name] = dict(before=before, after=after)
                elif after < before:
                    raise ValueError('authority-cache counter reset')
                else:
                    authority[name] = after - before
            delta['authority'] = authority
        phases.setdefault(phase, dict(owners={}, frontend=None, physical={}))
        if role == 'frontend':
            phases[phase]['frontend'] = delta
        elif role == 'storage':
            phases[phase]['owners'][partition] = delta
        else:
            raise ValueError('unknown read-service evidence role')
    with (directory / 'read_io.csv').open() as stream:
        for row in csv.DictReader(stream):
            evidence = phases.setdefault(row['phase'], dict(owners={}, frontend=None, physical={}))
            partition, value = row['partition'], int(row['physical_read_bytes'])
            if value < 0 or partition in evidence['physical']:
                raise ValueError('invalid/duplicate charged physical-read evidence')
            evidence['physical'][partition] = value
            if all(field in row for field in ('file_bytes', 'resident_bytes_before', 'resident_bytes_after')):
                evidence.setdefault('residency', {})[partition] = {
                    field: int(row[field]) for field in
                    ('file_bytes', 'resident_bytes_before', 'resident_bytes_after')}
    for phase, evidence in phases.items():
        physical = evidence['physical']
        if physical and set(physical) != (expected_owners if mode == 'tcp' else {'0'}):
            raise ValueError('physical-read interval lacks the exact owner process population')
        charged = sum(physical.values()) if physical else None
        owners = evidence['owners']
        if owners and (mode != 'tcp' or set(owners) != expected_owners):
            raise ValueError('read-service counters lack the exact configured owner population')
        owner_available = mode == 'tcp' and set(owners) == expected_owners
        evidence['owner_counters_available'] = owner_available
        authority_available = owner_available and all(owner.get('authority') is not None for owner in owners.values())
        evidence['owner_authority_available'] = authority_available
        evidence['owner_authority_history_loads'] = (
            sum(owner['authority']['history_loads'] for owner in owners.values()) if authority_available else None)
        evidence['authority_history_loads_prove_device_io'] = False
        evidence['charged_physical_read_bytes'] = charged
        evidence['physical_io_scope'] = (
            'positive_process_interval_IO_not_per_value_device_latency' if charged else
            'zero_charged_process_reads_in_this_interval' if charged == 0 else 'unavailable')
        evidence['owner_resident_read_successes'] = (
            sum(owner['resident_read_successes'] for owner in owners.values()) if owner_available else None)
        evidence['owner_read_service_scope'] = (
            'unavailable_owner_counters' if not owner_available else
            'resident_RAM_pagecache_service_observed_not_device_latency'
            if evidence['owner_resident_read_successes'] else 'cache_state_not_established')
        evidence['owner_async_submissions'] = (
            sum(owner['async_read_submissions'] for owner in owners.values()) if owner_available else None)
        evidence['owner_resident_read_deferred'] = (
            sum(owner['resident_read_deferred'] for owner in owners.values())
            if owner_available and deferred_available else None)
        evidence['async_submissions_prove_device_io'] = False
        evidence['classification'] = classify_read_population(
            evidence['frontend'], owners if owner_available else None, charged,
            evidence.get('residency'))
        evidence['api'] = phase
    return phases


def completed_case_ids(planned, completed):
    """Bind each complete backend pair to one distinct planned outer case."""
    if len(completed) != 2 * len(planned):
        raise ValueError('KV case missing its exact native/distributed population')
    case_ids, seen = [], set()
    for ordinal, spec in enumerate(planned):
        case_id, variant = benchmark_case_identity('current', spec['shape'], spec['repetition'],
            storage=True, cold=spec['cold'], diagnostics=spec['diagnostics'],
            transport=spec['transport'], cached=spec['cached'],
            authority_resident_batches=spec['authority_resident_batches'],
            shard_cache_bytes=spec.get('shard_cache_bytes', 0),
            preparation_chunk_batches=spec.get('preparation_chunk_batches', 16),
            wal_layout=spec.get('wal_layout'), workload_batches=spec.get('workload_batches'),
            resident_reads=spec.get('resident_reads'), bounded_hot_reads=spec.get('bounded_hot_reads'),
            cold_single_probes=spec.get('cold_single_probes'), cold_group_probes=spec.get('cold_group_probes'))
        pair = completed[2 * ordinal:2 * ordinal + 2]
        if (case_id in seen or {row['backend'] for row in pair} != {'native-sync', 'tcp'}
                or any(row.get('case_id') != case_id or row.get('build') != 'current'
                       or row.get('variant') != variant
                       or row.get('transport') != (spec['transport'] if row['backend'] == 'tcp' else 'local')
                       for row in pair)):
            raise ValueError('KV completed pair differs from its planned outer case identity')
        seen.add(case_id)
        case_ids.append(case_id)
    return case_ids


def require_kv_policy(observed, expected):
    """Require the entire admitted semantic schema, including nested value types."""
    if type(observed) is not type(expected):
        raise ValueError('effective KV policy has a foreign value type')
    if isinstance(expected, dict):
        if observed.keys() != expected.keys():
            raise ValueError('effective KV policy has missing or unadmitted fields')
        for key, value in expected.items():
            require_kv_policy(observed[key], value)
    elif isinstance(expected, (list, tuple)):
        if len(observed) != len(expected):
            raise ValueError('effective KV policy has a foreign population')
        for actual, value in zip(observed, expected, strict=True):
            require_kv_policy(actual, value)
    elif observed != expected:
        raise ValueError('effective KV policy differs from admission')


def validate_diagnostic_evidence(directory, spec, mode, owner_partitions):
    """Admit the producer's separate clock populations, never clean latency rows."""
    directory = Path(directory)
    native = mode == 'native-sync'
    backend = 'native_value_wal_barrier' if native else TRANSPORT_BACKENDS[spec['transport']]
    identity = dict(zip(('value_bytes', 'batch_size', 'window'), spec['shape']))
    identity.update(backend=backend, repetition=0)
    # These producers have no transport column: the exact backend label binds it.
    def same_case(row, csv_row=False):
        return isinstance(row, dict) and all(
            row.get(key) == (str(value) if csv_row else value)
            and (csv_row or not isinstance(value, int) or type(row.get(key)) is int)
            for key, value in identity.items())

    native_fields = ('index_ns', 'wal_and_decode_ns', 'whole_call_ns')
    remote_fields = (
        'index_ns', 'dispatch_ns', 'wait_ns', 'actor_queue_ns', 'actor_service_ns',
        'actor_return_ns', 'request_encode_ns', 'exchange_ns', 'response_decode_ns',
        'record_verify_ns', 'server_handler_ns', 'server_inventory_wait_ns',
        'server_authority_lookup_ns', 'server_record_read_ns', 'cache_hits', 'cache_misses',
        'whole_call_ns', 'inline_read_ns', 'server_response_recheck_wait_ns',
        'server_response_recheck_ns', 'server_response_recheck_samples',
        'server_response_recheck_deferrals', 'wire_read_requests')
    count = spec.get('workload_reads') or 4096
    reopened = 'reopened-warm' if native else 'reopened-cache-cleared'
    states = ['warm'] + (['cache-evicted'] if spec['cold'] else []) + [reopened]
    population = [(state, sample) for state in states
                  for sample in range(min(count, 256) if state == 'cache-evicted' else count)]
    for filename, applicable, fields in (
            ('native_read_profiles.csv', native, native_fields),
            ('remote_read_profiles.csv', not native, remote_fields)):
        with (directory / filename).open() as stream:
            reader = csv.DictReader(stream)
            required = {*identity, 'state', 'sample', *fields}
            if filename == 'native_read_profiles.csv':
                required.add('value_cache_hit')
            else:
                required.add('server_timing_available')
            if (reader.fieldnames is None or len(set(reader.fieldnames)) != len(reader.fieldnames)
                    or not required <= set(reader.fieldnames)):
                raise ValueError('diagnostic read profile schema is missing or ambiguous')
            rows = list(islice(reader, len(population) + 1))
        if not applicable:
            if rows:
                raise ValueError('diagnostic profile contains a foreign backend population')
            continue
        if (len(rows) != len(population)
                or [row['state'] for row in rows] != [state for state, _ in population]):
            raise ValueError('diagnostic read profile state/population differs')
        for index, row in enumerate(rows):
            if (None in row or not same_case(row, True)
                    or row['sample'] != str(population[index][1])):
                raise ValueError('diagnostic read profile case/sample identity differs')
            if any(not isinstance(row.get(field), str) or not row[field].isascii()
                   or not row[field].isdigit() for field in fields):
                raise ValueError('diagnostic read clocks/counters must be nonnegative integers')
            values = {field: int(row[field]) for field in fields}
            whole = values['whole_call_ns']
            # Nested spans overlap: bound individually, never add them as a budget.
            if whole <= 0 or any(value > whole for field, value in values.items() if field.endswith('_ns')):
                raise ValueError('diagnostic read whole-call interval is invalid')
            if native:
                if row['value_cache_hit'] not in ('true', 'false'):
                    raise ValueError('native diagnostic cache-hit flag is invalid')
            elif row['state'] != reopened and values['cache_hits'] + values['cache_misses'] != 1:
                raise ValueError('remote diagnostic lacks one lookup observation')
            if not native and row.get('server_timing_available') not in ('true', 'false'):
                raise ValueError('remote diagnostic server timing availability is missing')
            if not native and (row['server_timing_available'] == 'true') != (values['server_response_recheck_samples'] > 0):
                raise ValueError('server timing availability lacks its actual sample population')
            if not native and row['server_timing_available'] == 'false':
                if any(values[field] for field in ('server_handler_ns', 'server_record_read_ns',
                                                   'server_response_recheck_samples')):
                    raise ValueError('unavailable server timing cannot contain observed spans')
        for state in states:
            selected = [row for row in rows if row['state'] == state]
            measured = ('index_ns', 'wal_and_decode_ns') if native else ('index_ns',)
            if not native and any(int(row['wire_read_requests']) for row in selected):
                measured += ('exchange_ns',)
            if not native and any(row['server_timing_available'] == 'true' for row in selected):
                measured += ('server_record_read_ns',)
            if any(sum(int(row[field]) for row in selected) <= 0 for field in measured):
                raise ValueError('diagnostic read state lacks its measured clock population')

    records = [row for row in transport_floor.yaml_documents(
        (directory / 'node_profiles.yaml').read_text()) if row is not None]
    phases = [
        'load-write-diagnostics', 'warm-profiled-read-diagnostics', 'warm-window-read-diagnostics',
        'warm-batched-read-diagnostics']
    if spec['cold']:
        phases += ['cache_evicted_get-diagnostics', 'cache_evicted_get_many-diagnostics',
                   'cache_evicted_get_window-diagnostics', 'cache-evicted-read-diagnostics']
    phases += ['reopened-read-diagnostics']
    if native:
        if records:
            raise ValueError('native diagnostic unexpectedly contains remote node profiles')
        return
    partitions = set(owner_partitions)
    if (len(owner_partitions) != 5 or len(partitions) != 5
            or any(type(partition) is not int for partition in owner_partitions)):
        raise ValueError('diagnostic requires exactly five configured owner partitions')
    if (len(records) != len(phases) or any(not isinstance(row, dict) for row in records)
            or [row.get('phase') for row in records] != phases):
        raise ValueError('diagnostic node profile phase population differs')
    registered = resident_read_evidence(directory, owner_partitions)['phases'] if spec.get('resident_reads') else {}
    registered_phases = {
        'warm-profiled-read-diagnostics': 'point_get',
        'warm-window-read-diagnostics': 'point_get_window',
        'warm-batched-read-diagnostics': 'point_get_many',
        'reopened-read-diagnostics': 'reopened_read_diagnostics'}
    for row in records:
        nodes = row.get('nodes')
        if (not same_case(row) or not isinstance(nodes, list) or len(nodes) != 5
                or any(not isinstance(node, list) or len(node) != 2
                       or type(node[0]) is not int for node in nodes)
                or {node[0] for node in nodes} != partitions):
            raise ValueError('diagnostic node profile case/owner identity differs')
        counters = [transport_floor.flatten_counters(profile) for _, profile in nodes]
        if any('requests' not in profile or 'client.requests' not in profile for profile in counters):
            raise ValueError('diagnostic owner request populations are missing')
        if any(profile['requests'] != profile['client.requests'] for profile in counters):
            raise ValueError('diagnostic owner/client request populations differ')
        # Bounded profiled reads can legitimately select only some batch owners.
        # Require every active owner's clocks, not merely a positive fleet sum.
        active = [profile for profile in counters if profile['client.requests'] > 0]
        registered_hits = registered.get(registered_phases.get(row['phase']), {}).get('totals', {}).get('rma_hits', 0)
        if row['phase'] == 'load-write-diagnostics' and len(active) != len(counters):
            raise ValueError('diagnostic load did not exercise every owner')
        def positive(*keys):
            if not active and registered_hits:
                # FI_READ completion has no remote message handler. Keep those
                # clocks not applicable rather than require a fictitious RPC.
                return
            for key in keys:
                if (any(key not in profile for profile in counters) or not active
                        or any(profile[key] <= 0 for profile in active)):
                    raise ValueError('diagnostic node profile lacks positive timing population: ' + key)
        positive('requests', 'request_decode_ns', 'handler_ns', 'response_encode_ns',
                 'response_send_ns', 'client.requests', 'client.exchange_ns',
                 'polling.progress_sweeps', 'polling.request_first_poll_samples')
        if row['phase'] == 'load-write-diagnostics':
            positive('storage_write.append_calls', 'storage_write.persist_calls',
                     'append_wal_ns', 'append_persist_ns')
        else:
            positive('read_response_recheck_samples', 'read_response_recheck_ns')
            # Ordinary Read replies lack ReadProfiled's returned server spans.
            if row['phase'] in ('warm-profiled-read-diagnostics',
                                'cache-evicted-read-diagnostics', 'reopened-read-diagnostics'):
                positive('client.server_handler_ns', 'client.server_record_read_ns')
        for _, profile in nodes:
            if spec['transport'] in ('efa-native', 'fabric-tcp-native'):
                # Reuse the native zero-TLS contract without imposing another polling policy.
                transport_floor.validate_polling(profile, dict(transport=spec['transport'], profile=False))
        if spec['transport'] != 'tcp':
            for owner in ('transport', 'client_transport'):
                positive(*(owner + '.' + key for key in
                           ('cq_polls', 'cq_completions', 'cq_call_ns', 'cq_timed_calls',
                            'cq_dispatch_ns', 'cq_dispatch_calls')))
        # Async submission is not a physical IO counter; partial/resident paths and
        # multiplexing compatibility zeros are intentionally not positive clocks.


def physical_cold_probe_evidence(directory, spec, mode, owner_partitions=range(5)):
    """Qualify every actual single-probe interval, never a filtered cold subset."""
    path = Path(directory) / 'cold_probes.jsonl'
    count = spec.get('cold_single_probes', 0)
    if not count:
        if path.is_file() and path.stat().st_size:
            raise ValueError('physical cold probes contradict their disabled command')
        return dict(available=False, qualified=False, probes=[])
    if not spec['cold'] or not path.is_file():
        raise ValueError('physical cold probes omitted their admitted population')
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if len(rows) != count:
        raise ValueError('physical cold probe sample count differs')
    native = mode == 'native-sync'
    expected = {0} if native else set(owner_partitions)
    case = dict(backend='native_value_wal_barrier' if native else TRANSPORT_BACKENDS[spec['transport']],
                transport='tcp' if native else spec['transport'], value_bytes=spec['shape'][0],
                batch_size=spec['shape'][1], window=spec['shape'][2], repetition=0,
                phase='physical_cold_get', requested_probes=count)
    identities, reasons = {}, []
    for index, row in enumerate(rows):
        if any(type(row.get(key)) is not type(value) or row[key] != value for key, value in case.items()):
            raise ValueError('physical cold probe case/command identity differs')
        if type(row.get('probe_index')) is not int or row['probe_index'] != index:
            raise ValueError('physical cold probe order/duplicates differ')
        mixed = index
        mixed = ((mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9) & (2**64 - 1)
        mixed = ((mixed ^ (mixed >> 27)) * 0x94d049bb133111eb) & (2**64 - 1)
        key_index = (mixed ^ (mixed >> 31)) % (spec['workload_batches'] * spec['shape'][1])
        if type(row.get('key_index')) is not int or row['key_index'] != key_index:
            raise ValueError('physical cold probe key selection differs')
        if any(type(row.get(field)) is not int or row[field] <= 0 for field in
               ('api_elapsed_ns', 'preparation_wall_ns')):
            raise ValueError('physical cold probe omitted actual API/preparation timing')
        local_reasons = []
        if row.get('identity_qualified') is not True:
            local_reasons.append('before_after_process_identity_unqualified')
        if row.get('value_verified') is not True:
            local_reasons.append('value_not_verified')
        observations = row.get('observations')
        if not isinstance(observations, list):
            raise ValueError('physical cold probe process observations missing')
        owners = {}
        for observation in observations:
            owner = observation.get('partition')
            if type(owner) is not int or owner not in expected or owner in owners:
                raise ValueError('physical cold probe actual owner population differs')
            owners[owner] = observation
            boot = observation.get('process_boot_id')
            boot_valid = (isinstance(boot, list) and len(boot) == 16
                          and all(type(byte) is int and 0 <= byte <= 255 for byte in boot))
            identity = (observation.get('pid'), observation.get('process_start_ticks'),
                        tuple(boot) if boot_valid else None)
            if not boot_valid or any(type(value) is not int or value <= 0 for value in identity[:2]):
                local_reasons.append('process_identity_unavailable')
            elif owner in identities and identities[owner] != identity:
                local_reasons.append('process_identity_changed')
            else:
                identities[owner] = identity
            before, after, delta = (observation.get('accounted_process_read_bytes_' + suffix)
                                    for suffix in ('before', 'after', 'delta'))
            if any(value is not None and (type(value) is not int or value < 0) for value in (before, after, delta)):
                raise ValueError('physical cold probe process counters invalid')
            actual = after - before if before is not None and after is not None and after >= before else None
            if delta != actual:
                raise ValueError('physical cold probe process delta differs from raw counters')
            for field in ('minor_fault_delta', 'major_fault_delta', 'file_bytes',
                          'resident_bytes_before', 'resident_bytes_after'):
                value = observation.get(field)
                if value is None:
                    local_reasons.append('residency_or_fault_evidence_unavailable')
                elif type(value) is not int or value < 0:
                    raise ValueError('physical cold probe residency/fault evidence invalid')
            if not observation.get('file_bytes'):
                local_reasons.append('WAL_file_population_empty')
        if set(owners) != expected:
            local_reasons.append('exact_process_population_missing')
        expected_owner = row.get('expected_owner')
        if type(expected_owner) is not int or expected_owner not in expected:
            local_reasons.append('expected_owner_unavailable')
        delta = owners.get(expected_owner, {}).get('accounted_process_read_bytes_delta')
        positive = delta is not None and delta > 0
        any_positive = any((owner.get('accounted_process_read_bytes_delta') or 0) > 0 for owner in owners.values())
        if (row.get('expected_owner_accounted_process_read_bytes_delta') != delta
                or type(row.get('positive_expected_owner_accounted_process_read')) is not bool
                or row['positive_expected_owner_accounted_process_read'] != positive
                or type(row.get('positive_any_owner_accounted_process_read')) is not bool
                or row['positive_any_owner_accounted_process_read'] != any_positive):
            raise ValueError('physical cold probe producer claim differs from actual process counters')
        if not positive:
            local_reasons.append('expected_owner_has_no_positive_physical_read')
        if local_reasons:
            reasons.append(dict(probe_index=index, reasons=sorted(set(local_reasons))))
    with (Path(directory) / 'summary.csv').open() as stream:
        summary = [row for row in csv.DictReader(stream) if row['phase'] == 'physical_cold_get']
    elapsed = sum(row['api_elapsed_ns'] for row in rows)
    preparation = sum(row['preparation_wall_ns'] for row in rows)
    if (len(summary) != 1 or summary[0].get('samples') != str(count)
            or summary[0].get('logical_operations') != str(count)
            or any(summary[0].get(key) != str(value) for key, value in case.items() if key != 'requested_probes')
            or int(summary[0]['measured_api_ns']) != elapsed
            or int(summary[0]['phase_wall_ns']) < elapsed + preparation):
        raise ValueError('physical cold probe API/phase timing population differs')
    ordered = sorted(row['api_elapsed_ns'] for row in rows)
    return dict(available=True, qualified=not reasons, physical_cold_api_latency_qualified=not reasons,
        samples=count, probes=rows, unqualified_probes=reasons,
        all_sample_latency_ns={name: ordered[math.ceil(count * quantile) - 1]
                               for name, quantile in (('p50', .5), ('p95', .95), ('p99', .99))},
        quantile_method='nearest_rank_over_ALL_unfiltered_samples',
        measured_api_ns=elapsed, preparation_wall_ns=preparation,
        scope='each expected owner process interval positive; normal get includes lookup/verification/fabric; not bare NVMe device latency, per-key physical bytes or device IOPS')


def physical_cold_group_probe_evidence(directory, spec, mode, owner_partitions=range(5)):
    """Every complete call and expected owner qualifies, or the population does not."""
    path = Path(directory) / 'cold_group_probes.jsonl'
    count = spec.get('cold_group_probes', 0)
    if not count:
        if path.is_file() and path.stat().st_size:
            raise ValueError('physical cold grouped probes contradict their disabled command')
        return dict(available=False, qualified=False, probes=[])
    batches = spec.get('workload_batches') or (512 if spec.get('authority_resident_batches') is not None else 320)
    loaded = batches * spec['shape'][1]
    if (type(count) is not int or not 1 <= count <= 4096 or not spec['cold']
            or loaded < 32 or not path.is_file()):
        raise ValueError('physical cold grouped probes require an admitted complete32 population')
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if len(rows) != count:
        raise ValueError('physical cold grouped probe sample count differs')
    native = mode == 'native-sync'
    expected = {0} if native else set(owner_partitions)
    case = dict(backend='native_value_wal_barrier' if native else TRANSPORT_BACKENDS[spec['transport']],
        transport='tcp' if native else spec['transport'], value_bytes=spec['shape'][0],
        batch_size=spec['shape'][1], window=spec['shape'][2], repetition=0,
        phase='physical_cold_get_many', requested_probes=count)
    identities, reasons = {}, []
    for index, row in enumerate(rows):
        if any(type(row.get(key)) is not type(value) or row[key] != value for key, value in case.items()):
            raise ValueError('physical cold grouped probe case/command identity differs')
        if type(row.get('probe_index')) is not int or row['probe_index'] != index:
            raise ValueError('physical cold grouped probe order/duplicates differ')
        mixed = ((index ^ (index >> 30)) * 0xbf58476d1ce4e5b9) & (2**64 - 1)
        mixed = ((mixed ^ (mixed >> 27)) * 0x94d049bb133111eb) & (2**64 - 1)
        start = (mixed ^ (mixed >> 31)) % loaded
        keys = row.get('key_indices')
        if (type(row.get('start_key_index')) is not int or row['start_key_index'] != start
                or not isinstance(keys, list) or len(keys) != 32
                or any(type(key) is not int for key in keys)
                or keys != [(start + member) % loaded for member in range(32)]):
            raise ValueError('physical cold grouped probe exact32 key selection/order differs')
        if type(row.get('member_count')) is not int or row['member_count'] != 32:
            raise ValueError('physical cold grouped probe member count differs')
        member_owners, expected_owners = row.get('member_owners'), row.get('expected_owners')
        if (not isinstance(member_owners, list) or len(member_owners) != 32
                or any(type(owner) is not int or owner not in expected for owner in member_owners)
                or not isinstance(expected_owners, list)
                or any(type(owner) is not int for owner in expected_owners)
                or expected_owners != sorted(set(member_owners))):
            raise ValueError('physical cold grouped probe authoritative member/owner population differs')
        if any(type(row.get(field)) is not int or row[field] <= 0 for field in
               ('api_elapsed_ns', 'preparation_wall_ns')):
            raise ValueError('physical cold grouped probe omitted actual complete-call/preparation timing')
        if type(row.get('values_verified')) is not bool:
            raise ValueError('physical cold grouped probe full-value verification flag invalid')
        local_reasons = [] if row['values_verified'] else ['full32_member_values_not_verified']
        if row.get('identity_qualified') is not True:
            local_reasons.append('before_after_process_identity_unqualified')
        returned = row.get('returned_member_count')
        if type(returned) is not int or returned < 0:
            raise ValueError('physical cold grouped probe returned-member count invalid')
        if returned != 32:
            local_reasons.append('returned_member_count_not32')
        observations = row.get('observations')
        if not isinstance(observations, list):
            raise ValueError('physical cold grouped probe process observations missing')
        owners = {}
        for observation in observations:
            owner = observation.get('partition')
            if type(owner) is not int or owner not in expected or owner in owners:
                raise ValueError('physical cold grouped probe actual owner population differs')
            owners[owner] = observation
            boot = observation.get('process_boot_id')
            boot_valid = (isinstance(boot, list) and len(boot) == 16
                          and all(type(byte) is int and 0 <= byte <= 255 for byte in boot))
            identity = (observation.get('pid'), observation.get('process_start_ticks'),
                        tuple(boot) if boot_valid else None)
            if not boot_valid or any(type(value) is not int or value <= 0 for value in identity[:2]):
                local_reasons.append('process_identity_unavailable')
            elif owner in identities and identities[owner] != identity:
                local_reasons.append('process_identity_changed')
            else:
                identities[owner] = identity
            before, after, delta = (observation.get('accounted_process_read_bytes_' + suffix)
                                    for suffix in ('before', 'after', 'delta'))
            if any(value is not None and (type(value) is not int or value < 0) for value in (before, after, delta)):
                raise ValueError('physical cold grouped probe process counters invalid')
            actual = after - before if before is not None and after is not None and after >= before else None
            if delta != actual:
                raise ValueError('physical cold grouped probe process delta differs from raw counters')
            for field in ('minor_fault_delta', 'major_fault_delta', 'file_bytes',
                          'resident_bytes_before', 'resident_bytes_after'):
                value = observation.get(field)
                if value is None:
                    local_reasons.append('residency_or_fault_evidence_unavailable')
                elif type(value) is not int or value < 0:
                    raise ValueError('physical cold grouped probe residency/fault evidence invalid')
            if not observation.get('file_bytes'):
                local_reasons.append('WAL_file_population_empty')
        if set(owners) != expected:
            local_reasons.append('exact_process_population_missing')
        observed_expected = set(expected_owners) <= set(owners)
        observed_positive = all((owners[owner].get('accounted_process_read_bytes_delta') or 0) > 0
                                for owner in set(expected_owners) & set(owners))
        claims = dict(expected_owners_observed=observed_expected,
                      expected_owner_reads_positive=observed_positive,
                      probe_qualified=row['values_verified'] and observed_expected and observed_positive
                                      and row.get('identity_qualified') is True)
        if any(type(row.get(field)) is not bool or row[field] != value for field, value in claims.items()):
            raise ValueError('physical cold grouped probe producer qualification differs from actual observations')
        api_error = row.get('api_error')
        if api_error is not None:
            if not isinstance(api_error, str):
                raise ValueError('physical cold grouped probe API error field invalid')
            local_reasons.append('API_returned_error')
        for owner in expected_owners:
            delta = owners.get(owner, {}).get('accounted_process_read_bytes_delta')
            if delta is None or delta <= 0:
                local_reasons.append(f'expected_owner_{owner}_has_no_positive_physical_read')
        if local_reasons:
            reasons.append(dict(probe_index=index, reasons=sorted(set(local_reasons))))
    with (Path(directory) / 'summary.csv').open() as stream:
        summary = [row for row in csv.DictReader(stream) if row['phase'] == case['phase']]
    elapsed = sum(row['api_elapsed_ns'] for row in rows)
    preparation = sum(row['preparation_wall_ns'] for row in rows)
    if (len(summary) != 1 or summary[0].get('samples') != str(count)
            or summary[0].get('logical_operations') != str(count * 32)
            or any(summary[0].get(key) != str(value) for key, value in case.items() if key != 'requested_probes')
            or int(summary[0]['measured_api_ns']) != elapsed
            or int(summary[0]['phase_wall_ns']) < elapsed + preparation):
        raise ValueError('physical cold grouped probe complete-call/phase timing population differs')
    ordered = sorted(row['api_elapsed_ns'] for row in rows)
    return dict(available=True, qualified=not reasons, physical_cold_group_api_latency_qualified=not reasons,
        samples=count, members_per_call=32, verified_member_count=count * 32 if not any(
            not row['values_verified'] or row['returned_member_count'] != 32 for row in rows) else None,
        probes=rows, unqualified_probes=reasons,
        all_sample_complete_call_latency_ns={name: ordered[math.ceil(count * quantile) - 1]
            for name, quantile in (('p50', .5), ('p95', .95), ('p99', .99))},
        quantile_method='nearest_rank_over_ALL_unfiltered_complete32_calls',
        measured_api_ns=elapsed, preparation_wall_ns=preparation,
        scope='one normal get_many32 API call; positive process interval IO on EVERY expected owner; not per-member physical service, bare device latency or device IOPS')


def cold_interval_evidence(directory, spec, mode, owner_partitions=range(5)):
    if not spec['cold']:
        return None
    expected = {0} if mode == 'native-sync' else set(owner_partitions)
    backend = 'native_value_wal_barrier' if mode == 'native-sync' else TRANSPORT_BACKENDS[spec['transport']]
    phases = {name: {} for name in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window')}
    with (Path(directory) / 'read_io.csv').open() as stream:
        for row in csv.DictReader(stream):
            if row['phase'] not in phases:
                continue
            identity = dict(backend=backend, value_bytes=spec['shape'][0],
                            batch_size=spec['shape'][1], window=spec['shape'][2], repetition=0)
            if any(row.get(key) != str(value) for key, value in identity.items()):
                raise ValueError('cold interval case identity differs')
            owner = int(row['partition'])
            values = {field: int(row[field]) for field in
                ('pid', 'physical_read_bytes', 'file_bytes', 'resident_bytes_before', 'resident_bytes_after')}
            if owner not in expected or owner in phases[row['phase']] or min(values.values()) < 0 or values['pid'] == 0:
                raise ValueError('cold interval process/owner population differs')
            phases[row['phase']][owner] = values
    for owners in phases.values():
        if set(owners) != expected or sum(row['physical_read_bytes'] for row in owners.values()) <= 0:
            raise ValueError('cold interval lacks exact processes and positive accounted physical IO')
    return dict(phases=phases, physical_io_observed=True, physical_nvme_latency=False,
        scope='positive accounted process interval IO; not per-key device service or saturation')


def write_interval_evidence(directory, spec, mode, owner_partitions=range(5)):
    path = Path(directory) / 'write_io.csv'
    if not path.is_file():
        if spec.get('bounded_hot_reads') is not None:
            raise ValueError('hardware population omitted actual write process IO')
        return dict(available=False, phases={})
    expected = ({('native', '')} if mode == 'native-sync' else
                {('controller', ''), *(('storage', str(owner)) for owner in owner_partitions)})
    backend = 'native_value_wal_barrier' if mode == 'native-sync' else TRANSPORT_BACKENDS[spec['transport']]
    phases, identities = {}, {}
    with path.open() as stream:
        for row in csv.DictReader(stream):
            case = dict(backend=backend, value_bytes=spec['shape'][0], batch_size=spec['shape'][1],
                        window=spec['shape'][2], repetition=0)
            if any(row.get(key) != str(value) for key, value in case.items()):
                raise ValueError('write interval case identity differs')
            owner = (row['role'], row['partition'])
            identity = (int(row['pid']), int(row['process_start_ticks']) if row['process_start_ticks'] else None)
            values = phases.setdefault(row['phase'], {})
            if (owner not in expected or owner in values or identity[0] <= 0
                    or (owner in identities and identities[owner] != identity)):
                raise ValueError('write interval process/owner population differs')
            identities[owner] = identity
            counters = {}
            for kind in ('read', 'write'):
                prefix = 'accounted_process_' + kind + '_bytes_'
                before, after, delta = (int(row[prefix + key]) if row[prefix + key] else None
                                        for key in ('before', 'after', 'delta'))
                if (any(value is not None and value < 0 for value in (before, after, delta))
                        or (delta is not None and (before is None or after is None or delta != after - before))):
                    raise ValueError('write interval process counter/delta differs')
                counters[kind] = dict(before=before, after=after, delta=delta)
            values[owner] = dict(pid=identity[0], process_start_ticks=identity[1], counters=counters)
    required = {'load_write_window', 'mixed_write_window', 'checkpoint_intervening_write',
                'delete_window', 'maintenance_flush_index'}
    if spec.get('bounded_hot_reads') is not None and not required <= phases.keys():
        raise ValueError('write interval omitted append/mixed/delete/barrier phases')
    results = {}
    for phase, owners in phases.items():
        if set(owners) != expected:
            raise ValueError('write interval lacks exact controller/native/storage processes')
        deltas = [owner['counters']['write']['delta'] for owner in owners.values()]
        results[phase] = dict(processes=[dict(role=role, partition=partition, **row)
                                         for (role, partition), row in owners.items()],
            accounted_process_write_bytes=sum(deltas) if all(value is not None for value in deltas) else None)
    return dict(available=bool(results), phases=results,
        scope='accounted process interval bytes; controller and storage roles separate; buffered writeback is not per-key device latency or RAM-only durability')


def operation_population_evidence(directory, spec, mode):
    """Retain complete API timings; no bytes/key division invents physical latency."""
    directory = Path(directory)
    with (directory / 'summary.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    phases = {}
    for row in rows:
        if row['phase'] in phases:
            raise ValueError('duplicate operation timing population')
        phases[row['phase']] = row
    required = {'load_write_window', 'mixed_get', 'mixed_write_window', 'delete_window',
                'maintenance_flush_index', 'checkpoint_capture', 'checkpoint_intervening_write',
                'checkpoint_get', 'reopen', 'reopen_get_audit', 'point_get', 'point_get_many'}
    if spec.get('bounded_hot_reads') is not None and not required <= phases.keys():
        raise ValueError('hardware population omitted actual WAL/read/barrier/reopen timings')
    if spec.get('bounded_hot_reads') is not None:
        batch = spec['shape'][1]
        for phase, count in (('load_write_window', spec['workload_batches'] * batch),
                             ('mixed_get', 64 * batch), ('mixed_write_window', 64 * batch),
                             ('reopen_get_audit', spec['workload_batches'] * batch)):
            if int(phases[phase]['logical_operations']) != count:
                raise ValueError('operation population cardinality differs: ' + phase)
    return dict(mode=mode, timings=phases,
        write_io_available=(directory / 'write_io.csv').is_file(),
        scope='complete API/window timers; writes remain actual WAL appends/persistence; process IO is interval evidence, never per-key device latency')


def validate_auxiliary(root, state, cfg):
    """Fail closed on added controls; never relax the original paired-case count."""
    root = Path(root)
    raw_rma_floor.validate_receipt(root, state, cfg)
    expected_plan = plan(cfg['kv_suite'], cfg['transports'])
    completed = state.get('completed_cases', [])
    floors = state.get('completed_floor_cases', [])
    if (state.get('plan') != expected_plan
            or state.get('binary_sha256') != cfg['binaries']['storage-node']
            or state.get('floor_binary_sha256') != cfg['binaries']['transport-floor']):
        raise ValueError('auxiliary controls differ from the immutable full KV plan/build')
    expected_floors, contention, read_service, fabric = [], [], [], []
    for spec, case_id in zip(expected_plan, completed_case_ids(expected_plan, completed), strict=True):
        for mode in ('native-sync', 'tcp'):
            directory = root / (case_id + '-' + mode)
            if not (directory / 'COMPLETE').is_file():
                raise ValueError('KV case omitted its original complete value/reopen audit')
            if spec['diagnostics']:
                validate_diagnostic_evidence(directory, spec, mode,
                    [node['partition'] for node in cfg['nodes']])
            else:
                require_clean_clocks(directory)
            validate_checkpoint_records(directory, spec, mode)
            resident = resident_read_evidence(directory,
                owner_partitions=[node['partition'] for node in cfg['nodes']])
            registered_probe = require_registered_probe(spec, mode, resident)
            read_service.append(dict(case_id=case_id, mode=mode, diagnostic=spec['diagnostics'],
                comparison_identity=comparison_identity(directory, spec, mode, cfg),
                resident_reads=resident, registered_probe=registered_probe,
                operations=operation_population_evidence(directory, spec, mode),
                write_intervals=write_interval_evidence(directory, spec, mode,
                    [node['partition'] for node in cfg['nodes']]),
                cold_intervals=cold_interval_evidence(directory, spec, mode,
                    [node['partition'] for node in cfg['nodes']]),
                physical_cold_probes=physical_cold_probe_evidence(directory, spec, mode,
                    [node['partition'] for node in cfg['nodes']]),
                physical_cold_group_probes=physical_cold_group_probe_evidence(directory, spec, mode,
                    [node['partition'] for node in cfg['nodes']]),
                phases=read_service_evidence(directory, mode,
                    owner_partitions=[node['partition'] for node in cfg['nodes']])))
            direct_diagnostic = spec['transport'] in DIRECT_TRANSPORTS and spec['diagnostics'] and mode == 'tcp'
            # Clean advisory-eviction runs keep physical/residency brackets, not
            # a second instrumented population disguised as clean execution.
            log = root / (case_id + '-' + mode + '-process.stderr.log')
            evidence = fabric_local_log(log, required=direct_diagnostic,
                                        forbidden=not direct_diagnostic)
            fabric.append(dict(case_id=case_id, role=mode,
                population='diagnostic' if spec['diagnostics'] else 'clean',
                evidence=evidence))
        # These five logs span the whole case. The first clean scalar case also
        # runs the later diagnostic RAM control; its node logs are mixed, never clean.
        node_diagnostic = spec['diagnostics'] or spec.get('transport_floor', False)
        for partition in range(5):
            archive_path = root / 'nodes' / case_id / f'node-{partition}.tar.gz'
            with tarfile.open(archive_path, 'r:gz') as archive:
                evidence = fabric_local_archive_log(archive, 'node.stderr.log',
                    required=spec['transport'] in DIRECT_TRANSPORTS and node_diagnostic,
                    forbidden=spec['transport'] not in DIRECT_TRANSPORTS or not node_diagnostic)
            fabric.append(dict(case_id=case_id, role='storage', partition=partition,
                population='mixed_clean_KV_and_diagnostic_RAM' if spec.get('transport_floor', False)
                           # The optional RAM control is distinct from clean KV.
                           else 'diagnostic' if spec['diagnostics'] else 'clean',
                archive=str(archive_path), evidence=evidence))
        if spec.get('cache_hit_contention', False):
            for mode in ('native-sync', 'tcp'):
                output = root / (case_id + '-' + mode)
                if not (output / 'COMPLETE').is_file():
                    raise ValueError('cache contention omitted full-case completion/audit')
                contention.append(dict(case_id=case_id, mode=mode,
                    observations=validate_contention(output / 'cache_contention.jsonl', spec, mode)))
        if spec.get('transport_floor', False):
            for instrumented in (False, True):
                suffix = '-transport-floor' + ('-profiled' if instrumented else '')
                expected_floors.append((case_id, suffix, spec['transport'], instrumented))
    if len(floors) != len(expected_floors):
        raise ValueError('RAM floor control population is incomplete')
    metrics = []
    base = Path(cfg['run_root'])
    for observed, (case_id, suffix, transport, instrumented) in zip(floors, expected_floors):
        spec = transport_floor.subsuite_floor_spec(transport, instrumented)
        output = case_id + suffix
        process_name = output + '-process.command.json'
        if (observed.get('case_id') != case_id or observed.get('output') != output
                or observed.get('population') != ('diagnostic' if instrumented else 'clean')
                or observed.get('specification') != spec
                or observed.get('binary_sha256') != cfg['binaries']['transport-floor']
                or observed.get('process_record') != process_name):
            raise ValueError('RAM floor source/case/population identity differs')
        directory = root / output
        files = {path.name: sha(path) for path in sorted(directory.iterdir()) if path.is_file()}
        if files != observed.get('files'):
            raise ValueError('RAM floor raw evidence inventory/hash differs')
        process = json.loads((root / process_name).read_text())
        command = [str(base / 'cloud-bin/current/sharded_wal_transport_bench'),
                   '--output', str(base / 'checkpoint-results/kv' / output),
                   '--remote-config', str(base / 'cases' / case_id / 'remote.yaml')]
        command += transport_floor.floor_flags(spec)
        if (process.get('exit_code') != 0 or process.get('binary_sha256') != cfg['binaries']['transport-floor']
                or not math.isfinite(process.get('elapsed_seconds', float('nan')))
                or process['elapsed_seconds'] <= 0 or process.get('command') != command):
            raise ValueError('RAM floor process command/build/completion differs')
        values = transport_floor.read_floor_metrics(directory, spec)
        fabric.append(dict(case_id=case_id, role='ram-control', population=observed['population'],
            evidence=fabric_local_log(root / (output + '-process.stderr.log'),
                required=transport in DIRECT_TRANSPORTS and instrumented,
                forbidden=transport not in DIRECT_TRANSPORTS or not instrumented)))
        metrics.append(dict(case_id=case_id, population=observed['population'], specification=spec,
            metrics=[dict(kind=kind, phase=phase, counter=counter, value=value)
                     for (kind, phase, counter), value in sorted(values.items())]))
    return dict(ram_controls=metrics, cache_contention=contention, read_service=read_service,
                fabric_local_diagnostics=fabric,
                scope='RAM exchange includes transport/software/payload validation; TLS only in authenticated modes; not pure wire or device IO')


class CheckpointKvCampaign(Campaign):
    def __init__(self, parent):
        # Deliberately do not call the generic executor/constructor: its output
        # root, two-source rebuild and fleet termination belong to another run.
        self.cfg = dict(parent.cfg, campaign='efa-cache', efa=any(
                            transport in HARDWARE_TRANSPORTS for transport in parent.cfg['transports']),
                        repetitions=parent.cfg['kv_suite']['repetitions'], kv_subsuite=True)
        self.root, self.ids = parent.root, parent.ids
        self.bundle = self.root  # The admitted atomic bootstrap installs root/runner/*.py.
        self.results = parent.campaign_results / 'kv'
        self.results.mkdir()
        self.session, self.s3, self.ssm, self.ec2 = parent.session, parent.s3, parent.ssm, parent.ec2
        self.deadline = parent.deadline
        self.state = dict(complete=False, completed_cases=[], completed_floor_cases=[], failures=[], plan=plan(
            parent.cfg['kv_suite'], parent.cfg['transports']),
            source_archive_sha256=parent.cfg['source_archive_sha256'],
            binary_sha256=parent.cfg['binaries']['storage-node'],
            floor_binary_sha256=parent.cfg['binaries']['transport-floor'],
            cold_scope='value caches cleared and WAL residency/physical IO checked per phase; not metadata-cold or per-key device timing',
            authority_scope='32-batch resident window stress; no claim every descriptor or metadata file page is cold')
        self.cloud_builds = {'current': {'binary_sha256': self.state['binary_sha256']}}
        self.active_payload = None
        self.parent = parent

    def progress(self, stage):
        self.state['stage'] = stage
        write_json(self.results / 'progress.json', self.state)
        self.parent.state['kv_suite'] = self.state
        self.parent.progress('kv-' + stage)

    def install(self):
        source = self.parent.build / 'binaries/storage-node'
        if sha(source) != self.state['binary_sha256']:
            raise ValueError('KV binary differs from admitted Sui storage executable')
        target = self.root / 'cloud-bin/current/sharded_wal_bench'
        target.parent.mkdir(parents=True, exist_ok=False)
        shutil.copy2(source, target)
        artifact = upload(self.s3, source, self.cfg, f"{self.cfg['s3_prefix']}/inputs/kv-storage-node")
        self.install_cloud_binary('current', artifact)
        floor_source = self.parent.build / 'binaries/transport-floor'
        if sha(floor_source) != self.state['floor_binary_sha256']:
            raise ValueError('transport-floor binary differs from admitted build')
        shutil.copy2(floor_source, target.with_name('sharded_wal_transport_bench'))
        write_json(self.results / 'binary-receipt.json', artifact)

    def verify_node_hardware(self, case_id, transport):
        for node in self.cfg['nodes']:
            archive = self.results / 'nodes' / case_id / f"node-{node['partition']}.tar.gz"
            with tarfile.open(archive) as stream:
                member = stream.getmember('efa-proof.json')
                if not member.isfile() or member.size > 1024 * 1024:
                    raise ValueError('KV EFA proof missing or too large')
                with stream.extractfile(member) as source:
                    proof = json.load(source)
                member = stream.getmember('efa-provider.txt')
                if not member.isfile() or member.size > 1024 * 1024:
                    raise ValueError('KV hardware provider evidence missing or too large')
                with stream.extractfile(member) as source:
                    validate_provider(source.read().decode(), transport)
            identity = dict(run_id=self.cfg['run_id'], case_id=case_id,
                            instance_id=node['instance_id'], binary_sha256=self.state['binary_sha256'])
            validate_efa_proof(proof, identity)

    def run_planned_case(self, ordinal, spec):
        identity = dict(run_id=self.cfg['run_id'], case_ordinal=ordinal,
                        instance_id=self.cfg['coordinator_instance_id'],
                        binary_sha256=self.state['binary_sha256'], specification=spec)
        before = None
        if spec['transport'] in HARDWARE_TRANSPORTS:
            from sui_checkpoint import fabric_preflight
            output = self.results / f"preflight-{ordinal}-{spec['transport']}"
            output.mkdir()
            fabric_preflight(output, spec['transport'])
            before = efa_snapshot(identity)
            write_json(self.results / f'efa-{ordinal}-before.json', before)
        completed_before = len(self.state['completed_cases'])
        self.run_case('current', tuple(spec['shape']), spec['repetition'], storage=True,
                      cold=spec['cold'], diagnostics=spec['diagnostics'], transport=spec['transport'],
                      cached=spec['cached'], resource_evidence=True,
                      authority_resident_batches=spec['authority_resident_batches'],
                      shard_cache_bytes=spec.get('shard_cache_bytes', 0),
                      transport_floor_probe=spec.get('transport_floor', False),
                      preparation_chunk_batches=spec.get('preparation_chunk_batches', 16),
                      workload_batches=spec.get('workload_batches'),
                      cache_hit_contention=spec.get('cache_hit_contention', False),
                      workload_reads=spec.get('workload_reads'), wal_layout=spec.get('wal_layout'),
                      resident_reads=spec.get('resident_reads'), bounded_hot_reads=spec.get('bounded_hot_reads'),
                      cold_single_probes=spec.get('cold_single_probes'), cold_group_probes=spec.get('cold_group_probes'))
        completed = self.state['completed_cases'][completed_before:]
        case_id = completed_case_ids([spec], completed)[0]
        if before is not None:
            after = efa_snapshot(identity)
            write_json(self.results / f'efa-{ordinal}-after.json', after)
            write_json(self.results / f'efa-{ordinal}-proof.json', efa_proof(before, after, identity))
            self.verify_node_hardware(case_id, spec['transport'])

    def run(self):
        try:
            write_json(self.results / 'plan.json', self.state)
            self.install()
            raw_rma_floor.run_registered_floor(self)
            for ordinal, spec in enumerate(self.state['plan']):
                self.run_planned_case(ordinal, spec)
            if len(self.state['completed_cases']) != 2 * len(self.state['plan']):
                raise ValueError('KV suite incomplete')
            validate_auxiliary(self.results, self.state, self.cfg)
            self.state['complete'] = True
        except BaseException as error:
            self.state['failures'].append(repr(error))
            raise
        finally:
            if self.active_payload is not None:
                try:
                    self.ssm_nodes(self.active_payload | {'action': 'stop'})
                    self.collect_node_results(self.active_payload['case_id'])
                except BaseException as error:
                    self.state['failures'].append('KV cleanup/export: ' + repr(error))
                    self.state['complete'] = False
            self.parent.state['kv_suite'] = self.state
            write_json(self.results / 'receipt.json', self.state)


def run_suite(parent):
    suite = CheckpointKvCampaign(parent)
    parent.state['kv_suite'] = suite.state
    suite.run()
    if not suite.state['complete']:
        raise RuntimeError('KV subsuite incomplete; outer supervisor must export and terminate')


def planning_summary(cfg, *, sui_cell_seconds=155, fixed_seconds=600, kv_pair_seconds=None):
    """Sensitivity estimate, not an admission promise or a sum of hang deadlines."""
    from checkpoint_matrix import matrix_plan
    cases = plan(cfg['kv_suite'], cfg['transports'])
    cells = matrix_plan(cfg)
    work = cfg['ttl_seconds'] - cfg['export_reserve_seconds']
    raw_reserve = 390 if raw_rma_floor.enabled(cfg) else 0
    remaining = work - fixed_seconds - raw_reserve - len(cells) * sui_cell_seconds
    return dict(kv_pairs=len(cases), kv_processes=2 * len(cases),
        clean_kv_pairs=sum(not case['diagnostics'] for case in cases),
        diagnostic_kv_pairs=sum(case['diagnostics'] for case in cases),
        sui_cells=len(cells), sui_replays=2 * len(cells),
        raw_rma_payloads=[256, 8192] if raw_reserve else [],
        bounded_hot_policy=bounded_hot_policy(8192, 2, True, cfg['kv_suite'].get('bounded_hot_reads')),
        physical_cold_probes_per_process=cfg['kv_suite'].get('cold_single_probes', 0),
        physical_cold_probes_total=2 * sum(case.get('cold_single_probes', 0) for case in cases),
        physical_cold_group_probes_per_process=cfg['kv_suite'].get('cold_group_probes', 0),
        physical_cold_group_probes_total=2 * sum(case.get('cold_group_probes', 0) for case in cases),
        physical_cold_group_members_total=64 * sum(case.get('cold_group_probes', 0) for case in cases),
        ttl_seconds=cfg['ttl_seconds'], export_reserve_seconds=cfg['export_reserve_seconds'],
        measured_work_seconds=work, per_sui_replay_deadline_seconds=cfg['replay_deadline_seconds'],
        assumptions=dict(sui_cell_seconds=sui_cell_seconds, fixed_setup_seconds=fixed_seconds,
                         raw_rma_reserve_seconds=raw_reserve, kv_pair_seconds=kv_pair_seconds),
        maximum_mean_kv_pair_seconds=remaining / len(cases) if cases else None,
        estimated_work_seconds=(fixed_seconds + raw_reserve + len(cells) * sui_cell_seconds
                                + len(cases) * kv_pair_seconds) if kv_pair_seconds is not None else None,
        qualified=False,
        scope='planning sensitivity only; 155s is observed local swapped Sui pair, setup/raw reserves are assumptions; new hot plus scalar/grouped physical-probe hardware timings unobserved; fixed absolute work/export deadlines remain authoritative')


def main(argv=None):
    """Offline admission only: no process execution, EC2, SSM, or provisioning."""
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    planner = sub.add_parser('plan')
    planner.add_argument('--config', type=Path, required=True)
    budget = sub.add_parser('plan-budget')
    budget.add_argument('--config', type=Path, required=True)
    budget.add_argument('--sui-cell-seconds', type=float, default=155)
    budget.add_argument('--fixed-seconds', type=float, default=600)
    budget.add_argument('--kv-pair-seconds', type=float)
    identity = sub.add_parser('identity')
    identity.add_argument('directory', type=Path)
    identity.add_argument('--config', type=Path, required=True)
    identity.add_argument('--spec', type=Path, required=True)
    identity.add_argument('--mode', choices=('tcp', 'native-sync'), required=True)
    matcher = sub.add_parser('compare')
    matcher.add_argument('reference', type=Path)
    matcher.add_argument('candidate', type=Path)
    matcher.add_argument('--compared', nargs='+', choices=('implementation', 'transport', 'clocks'),
                         default=['implementation'])
    matcher.add_argument('--population', choices=('full', 'bounded-hot', 'registered-arm', 'physical-cold', 'physical-cold-group'), default='full')
    cold = sub.add_parser('cold-evidence', help='retain all physical probes, including a failed/unqualified case')
    cold.add_argument('directory', type=Path)
    cold.add_argument('--config', type=Path, required=True)
    cold.add_argument('--spec', type=Path, required=True)
    cold.add_argument('--mode', choices=('tcp', 'native-sync'), required=True)
    evidence = sub.add_parser('evidence')
    evidence.add_argument('directory', type=Path)
    evidence.add_argument('--mode', choices=('tcp', 'native-sync'), required=True)
    args = parser.parse_args(argv)
    if args.action == 'plan':
        cfg = json.loads(args.config.read_text())
        result = []
        for spec in plan(cfg['kv_suite'], cfg['transports']):
            prepare, commands = kv_case_commands(cfg, spec)
            remote, owners = kv_case_policy(cfg, spec)
            value, batch, _ = spec['shape']
            batches = int(prepare[prepare.index('--batches') + 1])
            workload = dict(loaded_batches=batches, loaded_keys=batches * batch,
                loaded_payload_bytes=batches * batch * value,
                read_count=int(prepare[prepare.index('--reads') + 1]),
                read_algorithm='mix64-v1', read_seed=0, read_order='mix(sample)%loaded_keys')
            result.append(dict(specification=spec, prepare=prepare, commands=commands,
                               qualified=False, qualification='planned_not_observed',
                               expected_workload=workload, expected_remote_policy=remote,
                               expected_owner_policies=owners,
                               source_archive_sha256=cfg.get('source_archive_sha256'),
                               binary_sha256=cfg.get('binaries', {}).get('storage-node')))
    elif args.action == 'plan-budget':
        values = (args.sui_cell_seconds, args.fixed_seconds, args.kv_pair_seconds)
        if any(value is not None and (not math.isfinite(value) or value <= 0) for value in values):
            raise ValueError('planning durations must be finite and positive')
        result = planning_summary(json.loads(args.config.read_text()),
            sui_cell_seconds=args.sui_cell_seconds, fixed_seconds=args.fixed_seconds,
            kv_pair_seconds=args.kv_pair_seconds)
    elif args.action == 'cold-evidence':
        cfg, spec = json.loads(args.config.read_text()), json.loads(args.spec.read_text())
        identity = comparison_identity(args.directory, spec, args.mode, cfg)
        result = dict(comparison_identity=identity, case_source_qualified=identity['qualified'],
            physical_cold_probes=physical_cold_probe_evidence(args.directory, spec, args.mode,
                [node['partition'] for node in cfg['nodes']]),
            physical_cold_group_probes=physical_cold_group_probe_evidence(args.directory, spec, args.mode,
                [node['partition'] for node in cfg['nodes']]),
            scope='raw probe qualification is not a successful complete campaign/source claim')
    elif args.action == 'identity':
        result = comparison_identity(args.directory, json.loads(args.spec.read_text()), args.mode,
                                     json.loads(args.config.read_text()))
    elif args.action == 'evidence':
        result = dict(read_service=read_service_evidence(args.directory, args.mode),
                      resident_reads=resident_read_evidence(args.directory),
                      bounded_hot=bounded_control_evidence(args.directory, args.mode))
    else:
        match = (match_physical_cold_group if args.population == 'physical-cold-group' else
                 match_physical_cold if args.population == 'physical-cold' else
                 match_registered_arm if args.population == 'registered-arm' else
                 match_bounded_hot if args.population == 'bounded-hot' else match_comparison)
        result = match(json.loads(args.reference.read_text()),
                       json.loads(args.candidate.read_text()),
                       compared=() if args.population in ('registered-arm', 'physical-cold', 'physical-cold-group') else args.compared)
    print(json.dumps(result, indent=2))
    return 2 if args.action == 'compare' and not result['comparable'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
