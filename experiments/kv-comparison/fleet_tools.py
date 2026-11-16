"""F9 standalone admission models and disposable live NVMe-oF write ceilings.

Never opens a TideHunter WAL. The writable target is a fresh, exactly owned file;
its nvmet buffered_io=1 contract matches the owner's live file exports.
"""
import hashlib
import importlib.util
import itertools
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import statistics
import time
import uuid

import floors
import owner_ftrace

SELECTION = 'nvmeof_f9_tools'
KINDS = ('nvmeof_write_ceiling', 'admission_options')
TOOL_BUDGET_SECONDS = dict(zip(KINDS, (900, 1008)))
REGION_BYTES = 2 * 1024**3
LABEL = 'F9 z4m-highmem-16 admission models'
ADMISSION_BATCHES = [1, 2, 22, 57]
ADMISSION_FRACTIONS = [0, 55, 100]
ADMISSION_OPTIONS = ['A-model', 'B', 'C', 'D']
MAX_ADMISSION_BYTES = 8 * 1024**2
CAVEATS = ('Admission models only; no payload work, storage or network I/O. '
           'A-model is not engine J1; C excludes durable commit and network RTT. '
           'Contaminated or load-generator-limited rows do not establish a ceiling.')


def make_jobs():
    """Fixed reviewed F9 selection. Admission models only; no payload work,
    storage or network I/O. A-model is not engine J1; C excludes durable commit
    and network RTT. Contaminated or load-generator-limited rows do not establish
    a ceiling.
    """
    return _measurement_jobs(
        repetitions=1,
        cell_filter=lambda callers, owners, distribution, batch:
        not (callers in (1, 4) and batch in (1, 57))
        and not (callers in (16, 64) and owners in (4, 16) and distribution == 'zipf099'))


def _measurement_jobs(*, repetitions, cell_filter, options=ADMISSION_OPTIONS, selection=SELECTION):
    """Internal builder for future named, separately digest-reviewed selections."""
    if type(repetitions) is not int or not 1 <= repetitions <= 3:
        raise ValueError('tool repetitions must be 1..3')
    if not options or len(options) != len(set(options)) or set(options) - set(ADMISSION_OPTIONS):
        raise ValueError('invalid admission options')
    jobs = []
    def add(kind, backend, group, repeat, size, callers, spec):
        job = dict(selection=selection, case_kind=kind, group=group, backend=backend,
                   repeat=repeat, owner_count=1, profile=kind, cohort='standalone_tools',
                   value_size=size, concurrency=callers, keys=4096,
                   mix=dict(read=0, put=100, delete=0, missing_read=0, get_many=0, put_batch=0),
                   measurement_operations=0, warmup_operations=0, read_lanes=0,
                   offered_ops_per_second=None, distribution='uniform',
                   coordinator_cache_bytes=0, drop_os_caches=False,
                   native_durability='value_wal_barrier', durability_tier='synchronous',
                   execution={'queue_capacity': 256}, **spec)
        identity = hashlib.sha256(json.dumps(job, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:20]
        job['case_id'] = f'{group}-r{repeat}-{identity}'
        jobs.append(job)
    for repeat in range(repetitions):
        for size, count, dsync in itertools.product((4096, 65536), (1, 4), (False, True)):
            add(KINDS[0], 'tidehunter_nvme_rdma', 'G33F', repeat, size, count,
                {'fio_write': dict(block_bytes=size, jobs=count, dsync=dsync, queue_depth=32,
                                   runtime_seconds=30, region_bytes=REGION_BYTES,
                                   target_buffered_io=1, transport='rdma')})
    # Fixed F9 invocation: 640 timed seconds + 128 warmup seconds, leaving
    # four minutes for trace/topology setup inside its 1008-second cap.
    for callers, owners, distribution in itertools.product((1, 4, 16, 64, 256), (1, 4, 16), ('uniform', 'zipf099')):
        batches = [batch for batch in ADMISSION_BATCHES if cell_filter(callers, owners, distribution, batch)]
        if not batches:
            continue
        add(KINDS[1], 'native', 'G33A', 0, 64, callers,
            {'admission': dict(callers=callers, owners=owners, distribution=distribution,
                               batches=batches, fractions_pct=ADMISSION_FRACTIONS,
                               options=options, seconds=1, repeats=repetitions,
                               sample_limit=8000000, value_independent_controls_bytes=[64, 1024])})
    return jobs


def queue_cpus(device, jobs, sys_block=Path('/sys/block'), allowed=None):
    """Choose one allowed CPU from each distinct non-admin hardware queue."""
    if jobs not in (1, 4) or not re.fullmatch(r'nvme[0-9]+n[0-9]+', Path(device).name):
        raise ValueError('exact NVMe namespace and reviewed job count required')
    allowed = set(os.sched_getaffinity(0) if allowed is None else allowed)
    queues = []
    for entry in sorted((Path(sys_block) / Path(device).name / 'mq').iterdir(), key=lambda p: int(p.name)):
        text = (entry / 'cpu_list').read_text().strip()
        cpus = set()
        for part in text.split(','):
            bounds = part.split('-')
            if len(bounds) not in (1, 2) or any(not value.isdigit() for value in bounds):
                raise ValueError('invalid hardware queue CPU list')
            first, last = int(bounds[0]), int(bounds[-1])
            if not 0 <= first <= last < 4096:
                raise ValueError('hardware queue CPU range exceeds bound')
            cpus.update(range(first, last + 1))
        candidates = sorted((cpus & allowed) - {q['cpu'] for q in queues})
        if candidates:
            queues.append(dict(queue=int(entry.name), cpu_list=text, cpu=candidates[0]))
    if len(queues) < jobs:
        raise ValueError('insufficient distinct hardware queues with allowed CPUs')
    return queues[:jobs]


def fio_argv(binary, target, spec, queues):
    count = spec['jobs']
    if (spec['block_bytes'] not in (4096, 65536) or count not in (1, 4)
            or spec['queue_depth'] != 32 or spec['runtime_seconds'] != 30
            or spec['region_bytes'] != REGION_BYTES or type(spec['dsync']) is not bool
            or len(queues) != count or len({q['queue'] for q in queues}) != count
            or len({q['cpu'] for q in queues}) != count):
        raise ValueError('invalid reviewed write ceiling dimensions')
    size = REGION_BYTES // count
    return [str(binary), '--name=nvmeof-write-ceiling', '--output-format=json',
            '--ioengine=io_uring', '--rw=write', '--direct=1', '--thread=1',
            '--group_reporting=1', '--allow_file_create=0', '--filename=' + str(target),
            '--numjobs=' + str(count), '--bs=' + str(spec['block_bytes']), '--iodepth=32',
            '--time_based=1', '--runtime=30', '--size=' + str(size),
            '--offset_increment=' + str(size), '--cpus_allowed=' + ','.join(str(q['cpu']) for q in queues),
            '--cpus_allowed_policy=split', '--sync=' + ('dsync' if spec['dsync'] else 'none'),
            '--percentile_list=50:99:99.9']


def fio_metrics(raw, spec):
    if len(raw) > floors.MAX_LOG_BYTES:
        raise ValueError('fio output exceeds bound')
    document = json.loads(raw)
    if not isinstance(document.get('jobs'), list) or len(document['jobs']) != 1:
        raise ValueError('fio group reporting must yield exactly one result')
    job = document['jobs'][0]
    write = job['write']
    if job['error'] != 0 or job['read']['total_ios'] != 0:
        raise ValueError('fio write ceiling has errors or unexpected reads')
    seconds = write['runtime'] / 1000
    count, size = write['total_ios'], write['io_bytes']
    if seconds <= 0 or count <= 0 or size != count * spec['block_bytes']:
        raise ValueError('fio write population or runtime invalid')
    percentiles = write['clat_ns']['percentile']
    latencies = {key: percentiles[source] for key, source in
                 (('p50_ns', '50.000000'), ('p99_ns', '99.000000'), ('p999_ns', '99.900000'))}
    if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in latencies.values()):
        raise ValueError('invalid fio completion latency')
    return dict(elapsed_seconds=seconds, write_ios=count, write_bytes=size,
                iops=count / seconds, mb_per_second=size / seconds / 1e6,
                completion_latency=latencies, iodepth_level=job['iodepth_level'])


def admission_argv(binary, directory, spec):
    return [str(binary), '--callers', str(spec['callers']), '--owners', str(spec['owners']),
            '--batches', ','.join(map(str, spec['batches'])),
            '--fractions-pct', ','.join(map(str, spec['fractions_pct'])),
            '--distributions', spec['distribution'], '--options', ','.join(spec['options']),
            '--seconds', str(spec['seconds']), '--repeats', str(spec['repeats']),
            '--sample-limit', str(spec['sample_limit']), '--skip-codec', '--label', LABEL,
            '--output', str(Path(directory) / 'admission-options.csv'),
            '--json-output', str(Path(directory) / 'admission-options.jsonl')]


def admission_records(raw, spec):
    """Reject incomplete/duplicate matrices; retain every binary population and flag."""
    if not raw or len(raw) > MAX_ADMISSION_BYTES:
        raise ValueError('admission JSONL missing or exceeds bound')
    records = [json.loads(line) for line in raw.splitlines()]
    metadata, rows = records[0], records[1:]
    if (metadata.get('type') != 'metadata' or metadata.get('schema') != 2
            or metadata.get('label') != LABEL or metadata.get('engine_path') is not False
            or metadata.get('callers') != [spec['callers']] or metadata.get('owners') != [spec['owners']]
            or metadata.get('batches') != spec['batches'] or metadata.get('fractions_pct') != spec['fractions_pct']
            or metadata.get('distributions') != [spec['distribution']] or metadata.get('options') != spec['options']
            or metadata.get('seconds') != spec['seconds'] or metadata.get('repeats') != spec['repeats']
            or metadata.get('sample_limit_per_caller') != spec['sample_limit']
            or not metadata.get('pin_map_actor_then_callers')):
        raise ValueError('admission metadata differs from reviewed pinned model run')
    expected = {(batch, fraction, option, repeat)
                for batch in spec['batches']
                for fraction in ([0] if spec['owners'] == 1 or batch == 1 else spec['fractions_pct'])
                for option in spec['options'] for repeat in range(1, spec['repeats'] + 1)}
    observed = set()
    for row in rows:
        key = (row.get('batch_keys'), row.get('requested_multi_pct'), row.get('option'), row.get('repetition'))
        if (row.get('type') != 'case' or key not in expected or key in observed
                or row.get('label') != LABEL or row.get('callers') != spec['callers']
                or row.get('owners') != spec['owners'] or row.get('distribution') != spec['distribution']
                or row.get('seconds') != spec['seconds']):
            raise ValueError('admission row differs from exact matrix or is duplicated')
        for field in ('offered', 'successful', 'latency_samples', 'successful_complete_buckets'):
            if type(row.get(field)) is not int or row[field] <= 0:
                raise ValueError('admission population missing')
        if (row['offered'] != row['successful'] or row['latency_samples'] != row['successful']
                or row.get('rejected') != 0 or row['successful_complete_buckets'] > row['successful']):
            raise ValueError('admission populations disagree')
        for field in ('batches_s', 'mutations_s', 'p50_ns', 'p99_ns', 'p999_ns', 'mean_inflight', 'cpu_ns_per_batch'):
            value = row.get(field)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError('admission metrics missing or invalid')
        if not row['p50_ns'] <= row['p99_ns'] <= row['p999_ns'] or type(row.get('contaminated')) is not bool:
            raise ValueError('admission latency order or contamination flag invalid')
        observed.add(key)
    if observed != expected:
        raise ValueError('admission JSONL contains an incomplete matrix')
    return dict(metadata=metadata, rows=rows,
                value_independent_controls_bytes=spec['value_independent_controls_bytes'],
                interpretation=CAVEATS)


def manager(repository):
    path = Path(repository) / 'experiments/nvmeof/manage.py'
    spec = importlib.util.spec_from_file_location('fleet_tools_nvme_manage', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def target_identity(config, job):
    token = str(uuid.uuid5(uuid.NAMESPACE_URL, config['prefix'] + ':' + job['case_id']))
    return dict(nqn='nqn.2026-11.io.tidehunter:f9-' + token,
                hostnqn='nqn.2026-11.io.tidehunter:f9-host-' + token,
                uuid=token, nsid=1, port_id=19090, service=19090,
                address=config['fleet']['nodes'][0]['rdma_address'], transport='rdma')


def target_setup(config, job, directory, api):
    """Publish a disposable file namespace; reuse sealed manager ownership/cleanup."""
    m = manager(config['repository_root'])
    m.preflight('rdma', True)
    identity = target_identity(config, job)
    sub = m.CONFIG / 'subsystems' / identity['nqn']
    host = m.CONFIG / 'hosts' / identity['hostnqn']
    port = m.CONFIG / 'ports' / str(identity['port_id'])
    if any(path.exists() for path in (sub, host, port)):
        raise api.SafetyError('F9 target configfs objects already exist; refusing to share')
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    scratch, state_directory = None, None
    try:
        scratch = api.provision_fio_scratch(directory / 'scratch', job['case_id'], REGION_BYTES,
                                          deadline=time.monotonic() + 150)
        state = dict(version=1, kind='target', phase='creating', **identity,
                     scratch=scratch, region_bytes=REGION_BYTES,
                     backing_device=api.media_device_snapshot(directory, 0, Path('/proc/diskstats').read_text()),
                     filesystem=api.command(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', scratch['path']], timeout=10).strip())
        state_directory = m.new_state(directory / 'target', state)
        for flag, path in (('subsystem', sub), ('host', host), ('port', port)):
            path.mkdir()
            state[flag] = True
            state[flag + '_identity'] = m.object_identity(path)
            m.save(state_directory, state)
        m.put(sub / 'attr_allow_any_host', '0')
        (sub / 'allowed_hosts' / identity['hostnqn']).symlink_to(host)
        ns = sub / 'namespaces' / '1'
        ns.mkdir()
        state.update(namespace=True, namespace_identity=m.object_identity(ns))
        m.save(state_directory, state)
        m.put(ns / 'device_path', scratch['path'])
        m.put(ns / 'device_uuid', identity['uuid'])
        state['buffered_io'] = m.enable_buffered_io(ns)
        m.put(ns / 'enable', '1')
        for key, value in (('addr_trtype', 'rdma'), ('addr_adrfam', 'ipv4'),
                           ('addr_traddr', identity['address']), ('addr_trsvcid', identity['service'])):
            m.put(port / key, value)
        if (port / 'addr_trtype').read_text().strip() != 'rdma':
            raise api.SafetyError('F9 target RDMA readback failed')
        (port / 'subsystems' / identity['nqn']).symlink_to(sub)
        state['phase'] = 'live_writable'
        m.save(state_directory, state)
        return state
    except BaseException:
        if state_directory is not None:
            m.target_cleanup(state_directory, state)
        if scratch is not None:
            api.remove_fio_scratch(directory / 'scratch', job['case_id'], scratch)
        raise
    finally:
        if state_directory is not None:
            state_directory.close()


def guest(config, request, api):
    job = request.get('job')
    if (not isinstance(job, dict) or job.get('case_kind') != KINDS[0]
            or request['index'] != 0 or config['spec'].get('mode') != 'focused'
            or request.get('quiescent') is not True or request.get('case_id') != job.get('case_id')):
        raise api.SafetyError('F9 tools require the exact quiescent owner')
    api.cloud_campaign.validate_focused_job(job)
    root = api.guarded(config['fleet']['data_root'], Path('/mnt/tidehunter-kv-comparison'), must_exist=True)
    directory = api.guarded(root / 'cells' / job['case_id'] / 'fio-live', root)
    action = request['action']
    if action == 'fleet_tools_target_setup':
        return target_setup(config, job, directory, api)
    m = manager(config['repository_root'])
    state_directory, state = m.load_state(directory / 'target', 'target')
    expected = target_identity(config, job)
    if any(state.get(key) != value for key, value in expected.items()):
        raise api.SafetyError('F9 target ownership state differs')
    if action == 'fleet_tools_cpu_start':
        state_cpu = owner_ftrace.start_cpu(directory / 'cpu', 120)
        if 'directory' not in state_cpu:
            raise api.InfrastructureError('F9 owner CPU sampler failed: ' + str(state_cpu))
        api.dump(directory / 'cpu-state.json', state_cpu)
        return state_cpu
    if action == 'fleet_tools_cpu_stop':
        return owner_ftrace.stop_cpu(json.loads((directory / 'cpu-state.json').read_text()))
    if action == 'fleet_tools_target_cleanup':
        if request.get('host_disconnected') != expected:
            raise api.SafetyError('F9 cleanup lacks exact initiator disconnect proof')
        m.target_cleanup(state_directory, state)
        api.remove_fio_scratch(directory / 'scratch', job['case_id'], state['scratch'])
        return {'complete': True, 'identity': expected}
    raise api.SafetyError('unsupported F9 guest action')


def connect(config, job, directory, api, state):
    """Connect a writable scratch namespace; never use the sealed RO connector."""
    m = manager(config['repository_root'])
    m.preflight('rdma', False)
    expected = target_identity(config, job)
    if (any(state.get(key) != value for key, value in expected.items())
            or state.get('buffered_io') != 1 or state.get('region_bytes') != REGION_BYTES
            or not state.get('filesystem')):
        raise api.SafetyError('F9 target receipt differs from buffered RDMA scratch contract')
    before = m.controller_snapshot()
    if any(row['nqn'] == expected['nqn'] for row in before.values()):
        raise api.SafetyError('F9 scratch controller already exists')
    connection = dict(nqn=expected['nqn'], hostnqn=expected['hostnqn'], transport='rdma')
    try:
        api.command(['nvme', 'connect', '-t', 'rdma', '-a', expected['address'], '-s', str(expected['service']),
                     '-q', expected['hostnqn'], '-I', str(uuid.uuid5(uuid.NAMESPACE_URL, expected['hostnqn'])),
                     '-n', expected['nqn'], '-l', '0', '-i', '4', '-Q', '128'], timeout=30)
        deadline = time.monotonic() + 30
        manifest = dict(nqn=expected['nqn'], uuid_string=expected['uuid'], nsid=1,
                        image_len=REGION_BYTES, block_size=4096)
        while time.monotonic() < deadline:
            current = m.controller_snapshot()
            matches = m.new_controller_matches(before, current, expected['nqn'], 'rdma', expected['address'], expected['service'])
            if matches:
                ctrl = matches[0]
                connection.update(controller=ctrl, controller_inode=current[ctrl]['inode'],
                                  controller_address=current[ctrl]['address'])
                api.dump(directory / 'initiator-state.json', connection)
                namespaces = m.namespace_candidates(m.SYS_NVME / ctrl, manifest)
                if len(namespaces) == 1:
                    device = m.DEV / namespaces[0].name
                    info = device.stat()
                    if not stat.S_ISBLK(info.st_mode):
                        raise api.SafetyError('F9 namespace is not a block device')
                    block = Path('/sys/block') / device.name
                    if int((block / 'size').read_text()) * 512 != REGION_BYTES:
                        raise api.SafetyError('F9 namespace size differs from exact scratch file')
                    if (Path('/sys/block') / device.name / 'ro').read_text().strip() != '0':
                        raise api.SafetyError('F9 scratch namespace is unexpectedly read-only')
                    geometry = m.verify_queue_geometry(m.SYS_NVME / ctrl, (4, 128))
                    return connection, device, geometry
            time.sleep(.05)
        raise api.InfrastructureError('F9 writable namespace did not become ready')
    except BaseException:
        disconnect(config, job, connection, api, before=before)
        raise


def disconnect(config, job, connection, api, before=None):
    m = manager(config['repository_root'])
    expected = target_identity(config, job)
    if 'controller' not in connection:
        current = m.controller_snapshot()
        matches = m.new_controller_matches(before or {}, current, expected['nqn'], 'rdma', expected['address'], expected['service'])
        if matches:
            ctrl = matches[0]
            connection.update(controller=ctrl, controller_inode=current[ctrl]['inode'], controller_address=current[ctrl]['address'])
    m.disconnect(connection)
    if any(row['nqn'] == expected['nqn'] for row in m.controller_snapshot().values()):
        raise api.InfrastructureError('F9 controller absence unproved; preserving target')
    return expected


def tool_timeout(controller, deadline, requested, api, *, reserve=0):
    seconds = min(controller.remaining(), deadline - time.monotonic() - reserve, requested)
    if seconds < 1:
        raise api.DeadlineError('F9 measurement budget reached; preserving cleanup reserve')
    return seconds


def run_fio(controller, job, directory, api, deadline):
    request = dict(case_id=job['case_id'], job=job, quiescent=True)
    connection = dict(nqn=target_identity(controller.config, job)['nqn'],
                      hostnqn=target_identity(controller.config, job)['hostnqn'], transport='rdma')
    target = None
    cpu_started = False
    failure = None
    try:
        timeout = int(tool_timeout(controller, deadline, 180, api, reserve=90))
        try:
            target = controller.request(0, dict(request, action='fleet_tools_target_setup', timeout=timeout),
                                        case=job['case_id'], timeout=timeout)
        except Exception as exc:
            raise api.InfrastructureError('F9 target setup outcome uncertain; preserving exact target: ' + repr(exc)) from exc
        api.dump(directory / 'target-state.json', target)
        connection, device, geometry = connect(controller.config, job, directory, api, target)
        queues = queue_cpus(device, job['fio_write']['jobs'])
        binary = shutil.which('fio')
        if binary is None:
            raise api.InfrastructureError('fio unavailable on coordinator')
        argv = fio_argv(binary, device, job['fio_write'], queues)
        api.dump(directory / 'fio_command.json', argv)
        controller.request(0, dict(request, action='fleet_tools_cpu_start'), case=job['case_id'],
                           timeout=int(tool_timeout(controller, deadline, 15, api, reserve=90)))
        cpu_started = True
        # Reuse the floor supervisor: timeout kills and joins the full process
        # group, so no target can be deleted while a fio child still writes.
        status, text = api.floor_client_command(argv, tool_timeout(controller, deadline, 60, api, reserve=90))
        raw = text.encode()
        (directory / 'fio_raw.json').write_bytes(raw)
        if status:
            raise api.FioFailure('fio write ceiling failed: ' + text[-4096:])
        evidence = dict(schema_version=1, case_id=job['case_id'], **fio_metrics(raw, job['fio_write']),
                        namespace=target_identity(controller.config, job), region_bytes=REGION_BYTES,
                        filesystem=target['filesystem'], backing_device=target['backing_device'], target_buffered_io=1,
                        queue_geometry=geometry, pinned_queues=queues,
                        command=argv, fio_binary_sha256=api.digest(binary), device=str(device),
                        quantile_method='fio completion histogram percentiles; grouped jobs within one cell',
                        contract='O_DIRECT initiator; live writable buffered file target; ' +
                                 ('O_DSYNC each write' if job['fio_write']['dsync'] else 'page-cache write completion; not durable'),
                        raw_path='fio_raw.json', raw_sha256=hashlib.sha256(raw).hexdigest())
    except BaseException as exc:
        failure = exc
    finally:
        cpu, cpu_error = {'available': False}, None
        if cpu_started:
            try:
                cpu = controller.request(0, dict(request, action='fleet_tools_cpu_stop'),
                                         case=job['case_id'], timeout=20, export=True)
                api.dump(directory / 'owner-cpu.json', cpu)
            except Exception as exc:
                cpu_error = exc
        # A CPU RPC failure never skips exact controller/target teardown.
        # Setup timeouts are uncertain: do not delete without the setup receipt.
        try:
            proof = disconnect(controller.config, job, connection, api)
            if target is not None:
                cleanup = controller.request(0, dict(request, action='fleet_tools_target_cleanup', host_disconnected=proof),
                                             case=job['case_id'], timeout=30, export=True)
                api.dump(directory / 'target-cleanup.json', cleanup)
                if cleanup.get('complete') is not True:
                    raise api.InfrastructureError('F9 target cleanup unconfirmed')
        except Exception as exc:
            raise api.InfrastructureError('F9 namespace cleanup unconfirmed: ' + repr(exc)) from exc
        if cpu_error is not None:
            raise api.InfrastructureError('F9 CPU collection failed after target cleanup: ' + repr(cpu_error)) from cpu_error
    if failure is not None:
        if not isinstance(failure, api.FioFailure):
            raise api.InfrastructureError('F9 namespace/measurement failed: ' + repr(failure)) from failure
        raise failure
    if cpu.get('available') is not True or cpu.get('truncated'):
        raise api.InfrastructureError('F9 owner per-CPU busy evidence unavailable')
    evidence['owner_cpu'] = cpu
    return evidence


def run_admission(controller, job, directory, api, deadline):
    binary = Path(controller.config['binary']).with_name('admission-options')
    if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise api.InfrastructureError('sealed runtime/bin/admission-options executable unavailable')
    argv = admission_argv(binary, directory, job['admission'])
    api.dump(directory / 'admission_command.json', argv)
    # The output is flushed each case; preserve partial files if a later case
    # fails. A partial matrix is never admitted as a completed invocation.
    status, text = api.floor_client_command(argv, tool_timeout(controller, deadline, 600, api))
    (directory / 'admission-stdout.log').write_text(text)
    if status:
        raise api.FioFailure('admission_options measurement failed: ' + text[-4096:])
    path = directory / 'admission-options.jsonl'
    if path.is_symlink() or path.stat().st_size > MAX_ADMISSION_BYTES:
        raise api.SafetyError('admission JSONL exceeds bound or is a symlink')
    raw = path.read_bytes()
    result = admission_records(raw, job['admission'])
    return dict(schema_version=1, case_id=job['case_id'], **result,
                command=argv,
                binary_sha256=api.digest(binary), raw_path=path.name, raw_sha256=hashlib.sha256(raw).hexdigest())


def run_case(controller, job, api):
    directory = api.guarded(controller.root / 'cases' / job['case_id'], controller.root)
    directory.mkdir(parents=True, exist_ok=False)
    receipt = dict(job=job, status='failed', binary_blake2b512=controller.config['binary_blake2b512'],
                   artifacts_sha256={}, measurement_evidence={}, counter_evidence={}, raw_artifacts_validated=False)
    started, failure = time.monotonic(), None
    spent = getattr(controller, '_fleet_tool_seconds', {})
    allowance = TOOL_BUDGET_SECONDS[job['case_kind']] - spent.get(job['case_kind'], 0)
    deadline = started + min(600, allowance)
    controller.select_monitoring(job, directory)
    try:
        if allowance < 1:
            raise api.DeadlineError('F9 tool measurement budget exhausted')
        api.cloud_campaign.validate_focused_job(job)
        controller.guard()
        if controller.active_nvme is not None:
            raise api.SafetyError('F9 tools require a quiescent initiator')
        controller.stop_tide()
        controller.shared_tide = None
        if controller.active_tide is not None or controller.active_backend is not None:
            raise api.SafetyError('F9 tools require a quiescent fleet')
        result = (run_fio if job['case_kind'] == KINDS[0] else run_admission)(controller, job, directory, api, deadline)
        path = directory / 'fleet_tools_result.json'
        api.dump(path, result)
        receipt['fleet_tools_result'] = dict(path=path.name, sha256=api.digest(path))
        receipt['status'] = 'completed'
    except Exception as exc:
        failure = exc
        receipt['error'] = type(exc).__name__ + ': ' + str(exc)
    finally:
        path = directory / 'phase_timing.json'
        spent[job['case_kind']] = spent.get(job['case_kind'], 0) + time.monotonic() - started
        controller._fleet_tool_seconds = spent
        api.dump(path, dict(schema_version=1, case_id=job['case_id'], spans=[
            dict(operation='standalone_case', host_index=0, monotonic_start_ns=int(started * 1e9),
                 monotonic_end_ns=time.monotonic_ns(), status=receipt['status'])]))
        receipt['phase_timing'] = dict(path=path.name, sha256=api.digest(path))
        api.dump(directory / 'receipt.json', receipt)
        controller.finish_monitoring(job, directory, receipt)
        api.dump(directory / 'receipt.json', receipt)
    if isinstance(failure, (api.SafetyError, api.InfrastructureError, OSError, subprocess.TimeoutExpired)):
        raise failure
    return receipt


def validate_result(data, job, root):
    if data.get('schema_version') != 1 or data.get('case_id') != job['case_id']:
        raise ValueError('F9 result case identity differs')
    expected_path = 'fio_raw.json' if job['case_kind'] == KINDS[0] else 'admission-options.jsonl'
    if data.get('raw_path') != expected_path:
        raise ValueError('F9 raw path differs')
    path = Path(root) / expected_path
    limit = floors.MAX_LOG_BYTES if job['case_kind'] == KINDS[0] else MAX_ADMISSION_BYTES
    if path.is_symlink() or path.stat().st_size > limit:
        raise ValueError('F9 raw evidence exceeds bound or is a symlink')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != data.get('raw_sha256'):
        raise ValueError('F9 raw evidence hash differs')
    argv = data.get('command')
    if not isinstance(argv, list) or not argv or any(not isinstance(arg, str) or not arg for arg in argv):
        raise ValueError('F9 exact command missing')
    command_name = 'fio_command.json' if job['case_kind'] == KINDS[0] else 'admission_command.json'
    command_path = Path(root) / command_name
    if command_path.is_symlink() or json.loads(command_path.read_text()) != argv:
        raise ValueError('F9 command artifact differs from bound command')
    if job['case_kind'] == KINDS[0]:
        if (not re.fullmatch(r'/dev/nvme[0-9]+n[0-9]+', data.get('device', ''))
                or data.get('command') != fio_argv(argv[0], data['device'], job['fio_write'], data.get('pinned_queues', []))
                or not re.fullmatch(r'[0-9a-f]{64}', data.get('fio_binary_sha256', ''))):
            raise ValueError('F9 fio command or binary identity differs')
        expected = fio_metrics(raw, job['fio_write'])
        if any(data.get(k) != v for k, v in expected.items()):
            raise ValueError('F9 fio metrics differ from raw output')
        if (data.get('target_buffered_io') != 1 or data.get('region_bytes') != REGION_BYTES
                or not data.get('filesystem') or data.get('owner_cpu', {}).get('available') is not True):
            raise ValueError('F9 fio target/CPU evidence missing')
    else:
        output_root = Path(argv[-1]).parent
        if (argv != admission_argv(argv[0], output_root, job['admission'])
                or not re.fullmatch(r'[0-9a-f]{64}', data.get('binary_sha256', ''))):
            raise ValueError('F9 admission command or binary identity differs')
        expected = admission_records(raw, job['admission'])
        if any(data.get(k) != v for k, v in expected.items()):
            raise ValueError('F9 admission records differ from raw output')


def analysis_rows(cases):
    tables = {kind: [] for kind in KINDS}
    for case in cases:
        job = case['job']
        kind = job.get('case_kind')
        if kind not in tables:
            continue
        base = dict(case_id=job['case_id'], case_ids=[job['case_id']], status=case.get('status'),
                    repeat=job['repeat'], topology='one coordinator VM + one owner VM' if kind == KINDS[0] else 'coordinator VM; in-process null owners')
        result = case.get('focused_evidence', {}).get('fleet_tools_result', {}) if case.get('status') == 'completed' else {}
        if kind == KINDS[0]:
            cpu = result.get('owner_cpu', {})
            tables[kind].append(dict(base, **job['fio_write'], iops=result.get('iops'),
                                     mb_per_second=result.get('mb_per_second'),
                                     p50_us=(result.get('completion_latency', {}).get('p50_ns', 0) / 1000) if result else None,
                                     p99_us=(result.get('completion_latency', {}).get('p99_ns', 0) / 1000) if result else None,
                                     filesystem=result.get('filesystem'), contract=result.get('contract'),
                                     pinned_queues=result.get('pinned_queues'), owner_cpu_summary=cpu.get('summary'),
                                     owner_per_cpu_samples=cpu.get('samples')))
        elif result:
            for row in result['rows']:
                tables[kind].append(dict(base, **row,
                    value_independent_controls_bytes=job['admission']['value_independent_controls_bytes'],
                    load_generator_limited=row['mean_inflight'] < row['callers'] / 2,
                    interpretation=result['interpretation']))
        else:
            tables[kind].append(dict(base, **job['admission'], error=case.get('error')))
    repeat_variance(tables[KINDS[0]], ('block_bytes', 'jobs', 'dsync', 'queue_depth'),
                    ('iops', 'mb_per_second', 'p50_us', 'p99_us'))
    repeat_variance(tables[KINDS[1]], ('option', 'callers', 'owners', 'distribution', 'batch_keys', 'requested_multi_pct'),
                    ('batches_s', 'p50_ns', 'p99_ns', 'p999_ns', 'cpu_ns_per_batch'))
    return tables


def repeat_variance(rows, dimensions, fields):
    """Timed metrics only, after fresh warmup per repeat; n=1 is undefined."""
    groups = {}
    for row in rows:
        if row.get('status') == 'completed':
            groups.setdefault(tuple(row.get(k) for k in dimensions), []).append(row)
    for row in rows:
        peers = groups.get(tuple(row.get(k) for k in dimensions), [])
        row['repeat_count'] = len(peers)
        row['timed_metric_sample_variance'] = {
            field: statistics.variance([p[field] for p in peers])
            if len(peers) > 1 and all(p.get(field) is not None for p in peers) else None
            for field in fields}
        row['variance_population'] = 'timed metrics only; fresh warmup before every repeat; no warmup calls included'
