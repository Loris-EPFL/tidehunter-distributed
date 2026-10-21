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

import transport_floor

from checkpoint_matrix import efa_snapshot, efa_proof, validate_efa_proof, validate_provider
from checkpoint_matrix import fabric_local_log, fabric_local_archive_log
from guest import Campaign, upload, write_json, sha, benchmark_case_identity, kv_case_commands, kv_case_policy


def validate_options(options):
    if options is None:
        return
    if (not isinstance(options, dict)
            or not {'repetitions', 'diagnostics'} <= options.keys()
            or options.keys() - {'repetitions', 'diagnostics', 'scope'}
            or type(options['repetitions']) is not int or not 1 <= options['repetitions'] <= 3
            or type(options['diagnostics']) is not bool
            or options.get('scope', 'full') not in ('full', 'targeted')):
        raise ValueError('kv_suite requires repetitions 1..3, boolean diagnostics, and optional full/targeted scope')


def plan(options, transports):
    validate_options(options)
    if options is None:
        return []
    if options.get('scope', 'full') == 'targeted':
        cases = []
        for repeat in range(options['repetitions']):
            for transport in (transports if repeat % 2 == 0 else transports[::-1]):
                for value, batch, window, cached, shard_cache, cold, chunk, layout in (
                        (256, 1, 1, False, 0, False, 16, 'small'),
                        (256, 1, 1, False, 8 * 1024**2, False, 16, 'small'),
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
                        transport_floor=bool(repeat == 0 and value == 256 and not shard_cache)))
        if options['diagnostics']:
            for transport in transports:
                # Fresh case/repetition: stage clocks must not contaminate the
                # ordinary no-cache control used for cache comparisons.
                cases.append(dict(shape=[8192, 1, 1], repetition=options['repetitions'],
                    transport=transport, cold=False, cached=False, diagnostics=True,
                    authority_resident_batches=None, shard_cache_bytes=0,
                    preparation_chunk_batches=16, wal_layout='small',
                    workload_batches=320, workload_reads=4096, transport_floor=False))
        return cases
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
                                  cold=True, cached=False, diagnostics=True,
                                  authority_resident_batches=None))
    return cases


def validate_contention(path, spec, mode):
    """Recompute the opt-in hit-only population from raw per-caller API samples."""
    # Each outer case starts a fresh --repetitions 1 process; its internal index is zero.
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if len(rows) != 2:
        raise ValueError('cache contention requires separate one/eight-caller populations')
    backend = 'native_value_wal_barrier' if mode == 'native-sync' else {
        'tcp': 'experimental_tcp_authority', 'efa': 'experimental_efa_authority',
        'efa-direct': 'experimental_efa_direct_authority'}[spec['transport']]
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
    for (phase, role, partition), pair in boundaries.items():
        if set(pair) != {'before', 'after'}:
            raise ValueError('read-service evidence lacks complete interval')
        delta = {}
        for field in fields:
            before, after = int(pair['before'][field]), int(pair['after'][field])
            if before < 0 or after < before:
                raise ValueError('read-service counter reset or negative evidence')
            delta[field] = after - before
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
        evidence['async_submissions_prove_device_io'] = False
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
            wal_layout=spec.get('wal_layout'))
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
    """Compare the admitted effective policy, not two matching foreign receipts."""
    if not isinstance(observed, dict):
        raise ValueError('cold effective KV policy is missing or invalid')
    for key, value in expected.items():
        actual = observed.get(key)
        if isinstance(value, dict):
            require_kv_policy(actual, value)
        elif type(actual) is not type(value) or actual != value:
            raise ValueError(f'cold effective KV policy differs from admission: {key}')


def cold_owner_pid(path, endpoint, binary_sha256, expected_policy, partition):
    """Read the original owner identity, never infer it from the IO being certified."""
    with tarfile.open(path, 'r:gz') as archive:
        members = list(islice(archive, 1025))
        starts = [member for member in members if member.name == 'start.json']
        launches = [member for member in members if member.name == 'launch.yaml']
        if (len(members) > 1024 or len(starts) != 1 or not starts[0].isfile()
                or not 0 < starts[0].size <= 4096):
            raise ValueError('cold owner archive lacks one bounded regular original start receipt')
        if (len(launches) != 1 or not launches[0].isfile()
                or not 0 < launches[0].size <= 64 * 1024):
            raise ValueError('cold owner archive lacks one bounded regular original launch policy')
        def unique_pairs(pairs):
            result = dict(pairs)
            if len(result) != len(pairs):
                raise ValueError('duplicate original owner start identity key')
            return result
        with archive.extractfile(starts[0]) as stream:
            start = json.loads(stream.read(4097), object_pairs_hook=unique_pairs)
        with archive.extractfile(launches[0]) as stream:
            launches = transport_floor.yaml_documents(stream.read(64 * 1024 + 1).decode())
        if len(launches) != 1 or not isinstance(launches[0], dict):
            raise ValueError('cold original owner launch policy is missing or ambiguous')
        launch = launches[0]
        require_kv_policy(launch, expected_policy)
        require_kv_policy(launch, dict(listen=endpoint, tls=dict(transport='efa-direct'),
                                     node=dict(partition=dict(partition=partition))))
    if (not isinstance(start, dict) or type(start.get('pid')) is not int
            or not 0 < start['pid'] <= 2**32 - 1
            or start.get('ready') != endpoint or start.get('build') != 'current'
            or not isinstance(binary_sha256, str) or len(binary_sha256) != 64
            or start.get('binary_sha256') != binary_sha256):
        raise ValueError('cold original owner PID/endpoint/build identity differs')
    return start['pid']


def cold_probe_certificate(directory, spec, cfg, client):
    """Bind mixed cold-case stderr to the separately profiled certification pass.

    read_campaign::run enables these profiles only after the three primary cold
    phases. The auxiliary API samples have their own clocks; they are not clean
    latency observations and must never be pooled with the primary population.
    """
    directory = Path(directory)
    nodes = cfg['nodes']
    expected_binary_sha256 = cfg['binaries']['storage-node']
    if spec['transport'] != 'efa-direct' or not spec['cold'] or spec['diagnostics']:
        raise ValueError('cold auxiliary attribution requires a clean direct cold case')
    if not directory.name.endswith('-tcp'):
        raise ValueError('cold auxiliary requires the original remote case output')
    case_id = directory.name[:-4]
    expected_prepare, commands = kv_case_commands(cfg, spec)
    try:
        preparation = json.loads((directory.parent / (case_id + '-prepare.command.json')).read_text())
    except (OSError, ValueError) as error:
        raise ValueError('cold auxiliary lacks its original preparation receipt') from error
    if (not isinstance(preparation, dict)
            or type(preparation.get('exit_code')) is not int or preparation['exit_code'] != 0
            or preparation.get('binary_sha256') != expected_binary_sha256
            or preparation.get('command') != expected_prepare):
        raise ValueError('cold auxiliary preparation differs from its successful admitted KV command/build')
    deployment_path = directory.parent / (case_id + '-remote.yaml')
    deployments = transport_floor.yaml_documents(deployment_path.read_text())
    if len(deployments) != 1 or not isinstance(deployments[0], dict):
        raise ValueError('cold auxiliary deployment configuration is missing or ambiguous')
    deployment = deployments[0]
    remote_policy, owner_policy = kv_case_policy(cfg, spec)
    require_kv_policy(deployment, remote_policy)
    expected_endpoints = {node['partition']: node['private_ip'] + ':7700' for node in nodes}
    endpoints = deployment.get('endpoints', [])
    if (len(endpoints) != len(nodes)
            or {row['partition']: row['address'] for row in endpoints} != expected_endpoints
            or deployment.get('tls', {}).get('transport') != 'efa-direct'
            or len(deployment['storage']['partitions']) != len(nodes)
            or set(deployment['storage']['partitions']) != set(expected_endpoints)):
        raise ValueError('cold auxiliary deployment owners/transport differ from the admitted fleet')
    process = json.loads((directory.parent / (directory.name + '-process.command.json')).read_text())
    command = process.get('command')
    if (type(process.get('exit_code')) is not int or process['exit_code'] != 0
            or process.get('binary_sha256') != expected_binary_sha256
            or not isinstance(command, list) or not command
            or any(not isinstance(value, str) for value in command)):
        raise ValueError('cold auxiliary lacks its successful original process receipt')
    if command != commands['tcp']:
        raise ValueError('cold auxiliary original process differs from its complete admitted KV command')
    configured_lanes = deployment.get('read_lanes')
    if type(configured_lanes) is not int or not 1 <= configured_lanes <= 8:
        raise ValueError('cold auxiliary deployment read lane count is invalid')
    # KV preparation controls the deployment; measurement commands never override lanes.
    lanes = configured_lanes
    metadata = {}
    for line in (directory / 'campaign.txt').read_text().splitlines():
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        if key in metadata:
            raise ValueError('duplicate cold auxiliary campaign metadata')
        metadata[key] = value
    reads = spec.get('workload_reads') or 4096  # CheckpointKvCampaign uses efa-cache defaults.
    count = min(reads, 256)
    if (metadata.get('cold_read_pass') != 'true'
            or metadata.get('transport_diagnostics') != 'false'
            or metadata.get('transport') != 'efa-direct'
            or metadata.get('point_reads') != str(reads)):
        raise ValueError('cold auxiliary metadata differs from the immutable plan')
    backend = 'experimental_efa_direct_authority'
    identity = dict(zip(('value_bytes', 'batch_size', 'window'), spec['shape'])) | {'repetition': 0}
    def same_case(row):
        return row.get('backend') == backend and all(
            (type(row.get(key)) is int and row[key] == value)
            or (type(row.get(key)) is str and row[key] == str(value))
            for key, value in identity.items())
    with (directory / 'remote_read_profiles.csv').open() as stream:
        rows = list(islice(csv.DictReader(stream), 2 * count + 1))
    if (len(rows) != 2 * count or not all(same_case(row) for row in rows)
            or [row['state'] for row in rows] != (
                ['cache-evicted'] * count + ['reopened-cache-cleared'] * count)
            or [int(row['sample']) for row in rows] != list(range(count)) * 2
            or any(int(row['whole_call_ns']) <= 0 for row in rows)):
        raise ValueError('cold auxiliary API samples lack exact case/state/sample evidence')
    profiles = [row for row in transport_floor.yaml_documents(
        (directory / 'node_profiles.yaml').read_text()) if row is not None]
    partitions = {node['partition'] for node in nodes}
    if (len(partitions) != len(nodes) or len(profiles) != 1
            or not same_case(profiles[0])
            or profiles[0].get('phase') != 'cache-evicted-read-diagnostics'
            or len(profiles[0]['nodes']) != len(nodes)
            or {partition for partition, _ in profiles[0]['nodes']} != partitions):
        raise ValueError('cold auxiliary node profile phase/owner identity differs')
    profiles = dict(profiles[0]['nodes'])
    if (any(type(profile.get('requests')) is not int
            or profile['requests'] < 0
            or profile['client']['requests'] != profile['requests'] for profile in profiles.values())
            or sum(profile['requests'] for profile in profiles.values()) != count):
        raise ValueError('cold auxiliary node requests differ from the verified API samples')
    phases = ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window')
    io_phases = (*phases, 'cache_evicted_diagnostics')
    owner_pids = {
        partition: cold_owner_pid(
            directory.parent / 'nodes' / case_id / f'node-{partition}.tar.gz',
            expected_endpoints[partition], expected_binary_sha256, owner_policy, partition)
        for partition in partitions}
    expected_io = {(phase, str(partition)) for phase in io_phases for partition in partitions}
    io_path = directory / 'read_io.csv'
    if io_path.stat().st_size > 4096 * (len(expected_io) + 1):
        raise ValueError('cold physical IO CSV exceeds its bounded owner/phase scan')
    with io_path.open() as stream:
        io_rows = list(islice(csv.DictReader(stream), len(expected_io) + 1))
    if (len(io_rows) != len(expected_io)
            or {(row['phase'], row['partition']) for row in io_rows} != expected_io
            or not all(same_case(row) for row in io_rows)
            or any(row.get('pid') != str(owner_pids[int(row['partition'])]) for row in io_rows)):
        raise ValueError('cold physical IO lacks the exact case/phase/original owner PID population')
    service = read_service_evidence(directory, 'tcp', partitions)
    for phase in phases:
        evidence = service.get(phase)
        if (evidence is None or not evidence['owner_counters_available']
                or evidence['frontend'] is None
                or set(evidence['physical']) != {str(partition) for partition in partitions}
                or evidence['charged_physical_read_bytes'] <= 0):
            raise ValueError('cold auxiliary lacks complete separate primary read evidence intervals')
    # record() emits physical IO for the auxiliary probe, not read-path snapshots.
    diagnostic_io = service.get('cache_evicted_diagnostics')
    if (diagnostic_io is None
            or set(diagnostic_io['physical']) != {str(partition) for partition in partitions}
            or diagnostic_io['charged_physical_read_bytes'] <= 0):
        raise ValueError('cold auxiliary lacks its separate positive physical IO interval')
    with (directory / 'read_path_evidence.csv').open() as stream:
        boundaries = [row for row in csv.DictReader(stream) if row['phase'] in phases]
    expected_boundaries = {(boundary, role, partition) for boundary in ('before', 'after')
                           for role, partition in [('frontend', ''), *(
                               ('storage', str(partition)) for partition in partitions)]}
    if ([row['phase'] for row in boundaries] != [
            phase for phase in phases for _ in expected_boundaries]
            or not all(same_case(row) for row in boundaries)
            or any(len(selected := [row for row in boundaries if row['phase'] == phase]) != len(expected_boundaries)
                   or {(row['boundary'], row['role'], row['partition']) for row in selected} != expected_boundaries
                   for phase in phases)):
        raise ValueError('cold auxiliary phase order or exact owner boundary population differs')
    owners = {}
    remaining = list(client['records'])
    for node in nodes:
        endpoint = expected_endpoints[node['partition']]
        selected = [row for row in remaining if row['peer'] is not None
                    and row['peer'].split('->')[-1] == endpoint]
        peers = validate_cold_probe_windows(selected, profiles[node['partition']]['client_transport'], lanes)
        if profiles[node['partition']]['requests'] > 0 and sum(row['data_posts'] for row in selected) == 0:
            raise ValueError('cold auxiliary requested reads lack client read-lane DATA posts')
        remaining = [row for row in remaining if row not in selected]
        owners[node['partition']] = dict(endpoint=endpoint, client_peers=peers,
                                        storage_transport=profiles[node['partition']]['transport'],
                                        requests=profiles[node['partition']]['requests'])
    if remaining:
        raise ValueError('cold auxiliary client windows include foreign or unattributable peers')
    return dict(phase='cache-evicted-read-diagnostics', api_state='cache-evicted',
                evidence_phase='cache_evicted_diagnostics', samples=count, owners=owners,
                read_lanes_per_owner=lanes, storage_sessions_per_owner=lanes + 1,
                deployment_source=str(deployment_path),
                scope='separately profiled auxiliary certification API calls; not primary clean latency')


def validate_cold_probe_windows(records, transport, expected_count):
    """Match finalized local windows to their existing aggregate profile snapshot."""
    peers = [row['peer'] for row in records]
    if (len(records) != expected_count or any(row['reason'] != 'profile-disable' for row in records)
            or any(peer is None or len(peer.split('->')) != 2 for peer in peers)
            or len(set(peers)) != expected_count):
        raise ValueError('cold auxiliary profile window count/reason/peer identity differs')
    keys = ('send_post_calls', 'send_post_would_block', 'send_post_errors')
    if any(type(transport.get(key)) is not int or transport[key] < 0 for key in keys):
        raise ValueError('cold auxiliary transport post counters are invalid')
    successful = transport['send_post_calls'] - transport['send_post_would_block'] - transport['send_post_errors']
    if successful < 0 or sum(row['data_posts'] + row['credit_posts'] for row in records) != successful:
        raise ValueError('cold auxiliary local posts differ from the existing transport snapshot')
    return peers


def validate_cold_probe_storage(evidence, certificate, partition):
    owner = certificate['owners'][partition]
    # RemoteDatabase opens one writer/control session in addition to its read lanes.
    peers = validate_cold_probe_windows(evidence['records'], owner['storage_transport'],
                                       certificate['storage_sessions_per_owner'])
    expected = {'->'.join(reversed(peer.split('->'))) for peer in owner['client_peers']}
    controller_hosts = {peer.split('->')[0].rsplit(':', 1)[0] for peer in owner['client_peers']}
    if (len(controller_hosts) != 1 or not expected <= set(peers)
            or any(peer.split('->')[0] != owner['endpoint']
                   or peer.split('->')[1].rsplit(':', 1)[0] not in controller_hosts for peer in peers)):
        raise ValueError('cold auxiliary storage peers differ from its configured owner/client lanes')
    if (owner['requests'] > 0 and sum(row['data_posts'] for row in evidence['records']
                                     if row['peer'] in expected) == 0):
        raise ValueError('cold auxiliary requested reads lack storage read-peer DATA posts')


def validate_auxiliary(root, state, cfg):
    """Fail closed on added controls; never relax the original paired-case count."""
    root = Path(root)
    expected_plan = plan(cfg['kv_suite'], cfg['transports'])
    completed = state.get('completed_cases', [])
    floors = state.get('completed_floor_cases', [])
    if (state.get('plan') != expected_plan
            or state.get('binary_sha256') != cfg['binaries']['storage-node']
            or state.get('floor_binary_sha256') != cfg['binaries']['transport-floor']):
        raise ValueError('auxiliary controls differ from the immutable full KV plan/build')
    expected_floors, contention, read_service, fabric = [], [], [], []
    for spec, case_id in zip(expected_plan, completed_case_ids(expected_plan, completed), strict=True):
        cold_certificate = None
        for mode in ('native-sync', 'tcp'):
            directory = root / (case_id + '-' + mode)
            if not (directory / 'COMPLETE').is_file():
                raise ValueError('KV case omitted its original complete value/reopen audit')
            validate_checkpoint_records(directory, spec, mode)
            read_service.append(dict(case_id=case_id, mode=mode, diagnostic=spec['diagnostics'],
                phases=read_service_evidence(directory, mode,
                    owner_partitions=[node['partition'] for node in cfg['nodes']])))
            direct_diagnostic = spec['transport'] == 'efa-direct' and spec['diagnostics'] and mode == 'tcp'
            cold_auxiliary = (spec['transport'] == 'efa-direct' and spec['cold']
                              and not spec['diagnostics'] and mode == 'tcp')
            log = root / (case_id + '-' + mode + '-process.stderr.log')
            evidence = fabric_local_log(log, required=direct_diagnostic or cold_auxiliary,
                                        forbidden=not (direct_diagnostic or cold_auxiliary))
            if cold_auxiliary:
                cold_certificate = cold_probe_certificate(directory, spec, cfg, evidence)
            fabric.append(dict(case_id=case_id, role=mode,
                population='mixed_primary_clean_and_auxiliary_profiled_cold_certificate' if cold_auxiliary
                           else 'diagnostic' if spec['diagnostics'] else 'clean',
                auxiliary_certificate=None if cold_certificate is None or mode != 'tcp'
                                      else {key: value for key, value in cold_certificate.items() if key != 'owners'},
                evidence=evidence))
        # These five logs span the whole case. The first clean scalar case also
        # runs the later diagnostic RAM control; its node logs are mixed, never clean.
        node_diagnostic = spec['diagnostics'] or spec.get('transport_floor', False) or cold_certificate is not None
        for partition in range(5):
            archive_path = root / 'nodes' / case_id / f'node-{partition}.tar.gz'
            with tarfile.open(archive_path, 'r:gz') as archive:
                evidence = fabric_local_archive_log(archive, 'node.stderr.log',
                    required=spec['transport'] == 'efa-direct' and node_diagnostic,
                    forbidden=spec['transport'] != 'efa-direct' or not node_diagnostic)
            if cold_certificate is not None:
                validate_cold_probe_storage(evidence, cold_certificate, partition)
            fabric.append(dict(case_id=case_id, role='storage', partition=partition,
                population='mixed_clean_KV_and_diagnostic_RAM' if spec.get('transport_floor', False)
                           else 'mixed_primary_clean_and_auxiliary_profiled_cold_certificate' if cold_certificate is not None
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
                required=transport == 'efa-direct' and instrumented,
                forbidden=transport != 'efa-direct' or not instrumented)))
        metrics.append(dict(case_id=case_id, population=observed['population'], specification=spec,
            metrics=[dict(kind=kind, phase=phase, counter=counter, value=value)
                     for (kind, phase, counter), value in sorted(values.items())]))
    return dict(ram_controls=metrics, cache_contention=contention, read_service=read_service,
                fabric_local_diagnostics=fabric,
                scope='RAM exchange includes TLS/software/payload validation; not pure wire or device IO')


class CheckpointKvCampaign(Campaign):
    def __init__(self, parent):
        # Deliberately do not call the generic executor/constructor: its output
        # root, two-source rebuild and fleet termination belong to another run.
        self.cfg = dict(parent.cfg, campaign='efa-cache', efa=any(
                            transport in ('efa', 'efa-direct') for transport in parent.cfg['transports']),
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
        if spec['transport'] in ('efa', 'efa-direct'):
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
                      workload_reads=spec.get('workload_reads'), wal_layout=spec.get('wal_layout'))
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
