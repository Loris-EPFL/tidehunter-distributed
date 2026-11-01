"""Exact-owner, bounded NVMe-oF campaign lifecycle; no cloud operations."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

import yaml


ACTIONS = {"nvme_owner_stop", "nvme_owner_restart", "nvme_owner_export",
           "nvme_owner_target_setup", "nvme_owner_target_cleanup", "nvme_owner_cleanup_export"}
MAX_JSON = 64 * 1024 * 1024


def _trusted_dir(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute canonical directory required')
    for part in (path, *path.parents):
        st = part.lstat()
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022:
            raise ValueError('untrusted directory: ' + str(part))
    return path

def _remove_export(path, remaining):
    """Remove only trusted export files, checking the action deadline at each entry."""
    _trusted_dir(path)
    with os.scandir(path) as entries:
        for entry in entries:
            remaining()
            child = path / entry.name
            info = entry.stat(follow_symlinks=False)
            if info.st_uid != 0 or info.st_mode & 0o022:
                raise ValueError('untrusted export entry: ' + str(child))
            if stat.S_ISDIR(info.st_mode):
                _remove_export(child, remaining)
            elif stat.S_ISREG(info.st_mode):
                child.unlink()
            else:
                raise ValueError('unexpected export entry: ' + str(child))
    remaining()
    path.rmdir()



def _read_bytes(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022 or st.st_size > MAX_JSON:
            raise ValueError('untrusted file: ' + str(path))
        return stream.read(MAX_JSON + 1)


def _read_json(path):
    raw = _read_bytes(path)
    if len(raw) > MAX_JSON:
        raise ValueError('JSON exceeds size bound')
    return json.loads(raw)


def _write_new(path, value):
    raw = json.dumps(value, sort_keys=True).encode()
    if len(raw) > MAX_JSON:
        raise ValueError('JSON exceeds size bound')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())

def _write_matching(path, value):
    try:
        _write_new(path, value)
    except FileExistsError:
        if _read_json(path) != value:
            raise ValueError('existing receipt differs: ' + str(path))



def _stopped(status):
    return (status.get('MainPID') == '0' and status.get('SubState') == 'exited'
            and status.get('Result') == 'success' and status.get('ExecMainStatus') == '0')


def _read_export_entry(export, manifest):
    manifest = Path(manifest)
    if not manifest.is_absolute() or not manifest.is_relative_to(export) or manifest.name != 'manifest.json' or '..' in manifest.parts:
        raise ValueError('manifest outside exact export root')
    _trusted_dir(manifest.parent)
    raw = _read_bytes(manifest)
    if len(raw) > MAX_JSON:
        raise ValueError('manifest exceeds size bound')
    return raw


def read_export_manifest(config, case, index, manifest_path, *, validate_launch):
    """Return bytes only for a manifest explicitly listed by this exact owner export."""
    if not isinstance(case, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case):
        raise ValueError('invalid owner case')
    if type(index) is not int or index not in range(5):
        raise ValueError('exact owner index required')
    fleet = config['fleet']
    root = Path(fleet['data_root'])
    if root != Path('/mnt/tidehunter-kv-comparison') / fleet['run_id'] or fleet['run_id'] != config['prefix']:
        raise ValueError('invalid exact campaign root')
    node = root / 'cells' / case / ('node-' + str(index))
    _trusted_dir(node)
    launch = yaml.safe_load(_read_bytes(node / 'launch.yaml'))
    validate_launch(config, launch, case, index)
    export = node / 'nvmeof' / 'export'
    _trusted_dir(export)
    manifest = Path(manifest_path)
    ready = _read_json(export / 'ready.json')
    entries = ready.get('exports')
    if not isinstance(entries, list) or sum(item.get('manifest_path') == str(manifest) for item in entries if isinstance(item, dict)) != 1:
        raise ValueError('manifest not uniquely listed by owner export')
    return _read_export_entry(export, manifest)


def execute(config, request, *, command, validate_launch, unit_status, unit_name):
    """Execute one action. Failure receipts retain any attempted target state for recovery."""
    action = request['action']
    case, index = request['case_id'], request['index']
    if action not in ACTIONS or not isinstance(case, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case):
        raise ValueError('invalid owner action or case')
    if type(index) is not int or index not in range(5):
        raise ValueError('exact owner index required')
    generation = request.get('generation')
    if (generation is not None and
            (not isinstance(generation, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', generation))):
        raise ValueError('safe target generation required')
    if action in ('nvme_owner_target_setup', 'nvme_owner_target_cleanup') and generation is None:
        raise ValueError('safe target generation required')
    timeout = request['timeout']
    if type(timeout) not in (int, float) or not 0 < timeout <= 86400:
        raise ValueError('bounded positive timeout required')
    deadline = time.monotonic() + timeout

    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError('owner action deadline expired')
        return seconds

    fleet = config['fleet']
    root = Path(fleet['data_root'])
    if root != Path('/mnt/tidehunter-kv-comparison') / fleet['run_id'] or fleet['run_id'] != config['prefix']:
        raise ValueError('invalid exact campaign root')
    node = root / 'cells' / case / ('node-' + str(index))
    _trusted_dir(node)
    launch_path = node / 'launch.yaml'
    fd = os.open(launch_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022 or st.st_size > MAX_JSON:
            raise ValueError('untrusted launch')
        launch = yaml.safe_load(stream)
    validate_launch(config, launch, case, index)
    _trusted_dir(node / 'storage')
    work = node / 'nvmeof'
    work.mkdir(mode=0o700, exist_ok=True)
    _trusted_dir(work)
    if work.stat().st_mode & 0o077:
        raise ValueError('owner work directory must be private')
    name = unit_name(config, case, 'storage-' + str(index))
    state = (work / ('target-state-' + generation)
             if action in ('nvme_owner_target_setup', 'nvme_owner_target_cleanup') else None)
    export = work / 'export'
    stop_proof = work / 'stopped.json'
    script = Path(config['repository_root']) / 'experiments/nvmeof/campaign_targets.py'
    receipt = {'action': action, 'case_id': case, 'index': index, 'unit_name': name,
               'work_dir': str(work), 'complete': False}

    def call(argv):
        return command(argv, timeout=remaining())

    def target_status(path):
        return json.loads(call([sys.executable, '-B', script, 'status', '--state', path, '--kind', 'target']))

    def runtime_environment():
        from cloud_controller import environment
        return environment(config)

    def require_stopped():
        proof = _read_json(stop_proof)
        if proof.get('unit') != name or not _stopped(proof.get('status', {})):
            raise ValueError('successful graceful stop proof required')
        status = unit_status(name)
        if status.get('MainPID') != '0' and status.get('LoadState') != 'not-found':
            raise ValueError('owner is not stopped')
        return status


    try:
        if action == 'nvme_owner_stop':
            before = unit_status(name)
            receipt['before'] = before
            marker = node / 'stop'
            if not marker.exists():
                if before.get('ActiveState') != 'active' or before.get('MainPID') in (None, '0'):
                    raise ValueError('owner must be running before graceful stop')
                fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                os.close(fd)
            elif marker.is_symlink() or not marker.is_file():
                raise ValueError('unsafe stop marker')
            receipt['stop_requested'] = True
            while True:
                status = unit_status(name)
                receipt['after'] = status
                if _stopped(status):
                    break
                if status.get('ActiveState') in ('failed', 'inactive') or status.get('SubState') == 'exited':
                    raise RuntimeError('owner did not exit successfully after drain')
                time.sleep(min(0.2, remaining()))
            _write_matching(stop_proof, {'unit': name, 'status': status})
            receipt['database_preserved'] = str(node / 'storage')
        elif action == 'nvme_owner_restart':
            runtime = request['runtime_seconds']
            if type(runtime) is not int or runtime < 1:
                raise ValueError('positive owner runtime required')
            campaign_end = datetime.fromisoformat(config['deadline_utc'].replace('Z', '+00:00'))
            if campaign_end.tzinfo is None:
                raise ValueError('campaign deadline must be timezone-aware')
            seconds = min(runtime, int((campaign_end - datetime.now(timezone.utc)).total_seconds()))
            if seconds < 1:
                raise TimeoutError('no owner runtime remains')
            for target in work.glob('target-state-*'):
                if target_status(target).get('phase') != 'cleaned':
                    raise ValueError('target must be cleaned after readers drain before restarting owner')
            status = require_stopped()
            if status.get('LoadState') != 'not-found':
                call(['systemctl', 'stop', name])
                status = unit_status(name)
                if status.get('LoadState') != 'not-found' and status.get('ActiveState') == 'failed':
                    call(['systemctl', 'reset-failed', name])
            for marker in (node / 'stop', node / 'ready'):
                if marker.is_symlink():
                    raise ValueError('unsafe marker')
                marker.unlink(missing_ok=True)
            stop_proof.unlink()
            remaining()
            env = {key: value for key, value in runtime_environment().items()
                   if key in ('LD_LIBRARY_PATH', 'FDB_CLIENT_LIB_DIR', 'FDB_CLIENT_LIB_PATH', 'FI_EFA_USE_DEVICE_RDMA')}
            argv = ['systemd-run', '--unit=' + name, '--service-type=exec',
                    '--property=CPUQuota=' + str(fleet['server_cpus'] * 100) + '%',
                    '--property=MemoryMax=' + str(int(fleet['server_memory_mib'])) + 'M',
                    '--property=MemorySwapMax=0', '--property=LimitMEMLOCK=infinity',
                    '--property=RuntimeMaxSec=' + str(seconds), '--property=TimeoutStopSec=25',
                    '--property=KillMode=control-group', '--property=RemainAfterExit=yes',
                    '--property=CPUAccounting=yes', '--property=MemoryAccounting=yes', '--property=IOAccounting=yes',
                    '--property=WorkingDirectory=' + config['repository_root'],
                    *['--setenv=' + k + '=' + v for k, v in env.items()], '--', config['binary'],
                    '--experimental-storage-node', launch_path]
            receipt['restart_attempted'] = True
            call(argv)
            while True:
                status = unit_status(name)
                receipt['after'] = status
                if status.get('SubState') == 'exited' or status.get('ActiveState') in ('failed', 'inactive'):
                    raise RuntimeError('owner exited before readiness')
                ready = node / 'ready'
                if ready.is_file() and not ready.is_symlink():
                    if ready.read_text().strip() != launch['listen']:
                        raise ValueError('owner readiness endpoint mismatch')
                    receipt['ready'] = launch['listen']
                    break
                time.sleep(min(0.2, remaining()))
        elif action == 'nvme_owner_export':
            require_stopped()
            prepared = request['prepared']
            if not isinstance(prepared, dict):
                raise ValueError('prepared dataset must be an object')
            prepared_path = work / 'prepared.json'
            _write_matching(prepared_path, prepared)
            receipt['export_attempted'] = True
            command([config['binary'], 'export-nvmeof-dataset', launch_path, prepared_path, export],
                    timeout=remaining(), env=runtime_environment())
            _trusted_dir(export)
            receipt['ready'] = _read_json(export / 'ready.json')
            entries = receipt['ready'].get('exports')
            if not isinstance(entries, list):
                raise ValueError('invalid export readiness')
            for entry in entries:
                raw = _read_export_entry(export, entry['manifest_path'])
                entry['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
        elif action == 'nvme_owner_target_setup':
            status = unit_status(name)
            if status.get('ActiveState') != 'active' or status.get('MainPID') in (None, '0'):
                raise ValueError('target setup requires a running owner')
            _trusted_dir(export)
            ready = _read_json(export / 'ready.json')
            if not isinstance(ready.get('exports'), list):
                raise ValueError('target setup requires complete export readiness')
            host = fleet['nodes'][index]
            address_field = ('rdma_address' if config.get('platform', {}).get('name') == 'gcp'
                             and request['transport'] == 'rdma' else 'address')
            if request['address'] != host[address_field]:
                raise ValueError('target address differs from selected owner interface')
            transport = request['transport']
            if transport not in ('tcp', 'rdma'):
                raise ValueError('explicit NVMe transport must be tcp or rdma')
            receipt['target_setup_attempted'] = True
            receipt['target'] = json.loads(call([sys.executable, '-B', script, 'target-setup',
                '--ready', export / 'ready.json', '--export-root', export, '--state', state,
                '--address', request['address'], '--base-port-id', str(request['base_port_id']),
                '--base-service', str(request['base_service']), '--transport', transport]))
        elif action == 'nvme_owner_cleanup_export':
            status = unit_status(name)
            if status.get('ActiveState') != 'active' or status.get('MainPID') in (None, '0'):
                raise ValueError('export cleanup requires a running owner')
            _trusted_dir(export)
            ready_path = export / 'ready.json'
            cleanup_ready = work / 'export-cleanup-ready.json'
            ready = _read_json(ready_path if ready_path.exists() else cleanup_ready)
            if not isinstance(ready.get('exports'), list):
                raise ValueError('missing export readiness proof')
            targets = sorted(work.glob('target-state-*'))
            if not targets:
                if request.get('no_targets_published') is not True:
                    raise ValueError('missing target cleanup proof or explicit no-target attestation')
                receipt['no_targets_published'] = True
            receipt['targets'] = []
            for target in targets:
                generation_name = target.name.removeprefix('target-state-')
                if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', generation_name):
                    raise ValueError('invalid target generation')
                target_receipt = target_status(target)
                proof = _read_json(work / ('host-disconnected-' + generation_name + '.json'))
                entries = target_receipt.get('exports')
                host_entries = proof.get('exports')
                if (target_receipt.get('kind') != 'campaign-target'
                        or target_receipt.get('phase') != 'cleaned'
                        or target_receipt.get('readers_drained') is not True
                        or target_receipt.get('export_root') != str(export)
                        or proof.get('version') != 1 or proof.get('kind') != 'campaign-host'
                        or proof.get('phase') != 'disconnected' or proof.get('readers_drained') is not True
                        or not target_receipt.get('campaign_id')
                        or proof.get('campaign_id') != target_receipt['campaign_id']
                        or not isinstance(entries, list)
                        or not isinstance(host_entries, list) or len(entries) != len(host_entries)):
                    raise ValueError('missing or ambiguous target drain proof')
                for entry, host in zip(entries, host_entries):
                    if (entry.get('status') not in ('cleaned', 'pending')
                            or host.get('status') not in ('disconnected', 'pending')
                            or any(entry.get(k) is None or entry.get(k) != host.get(k)
                                   for k in ('uuid', 'nqn', 'hostnqn', 'hostid', 'address', 'service'))):
                        raise ValueError('target disconnect proof mismatch')
                receipt['targets'].append({
                    'generation': generation_name, 'campaign_id': target_receipt['campaign_id'],
                    'phase': target_receipt['phase'], 'readers_drained': True,
                    'target_state_path': str(target / 'state.json'),
                    'host_disconnect_path': str(work / ('host-disconnected-' + generation_name + '.json')),
                    'exports': [{**{key: entry[key] for key in
                                    ('uuid', 'nqn', 'hostnqn', 'hostid', 'address', 'service')},
                                 'target_status': entry['status'], 'host_status': host['status']}
                                for entry, host in zip(entries, host_entries)]})
            _write_matching(cleanup_ready, ready)
            receipt['export_cleanup_attempted'] = True
            _remove_export(export, remaining)
            receipt['export_removed'] = str(export)
        else:
            if request.get('readers_drained') is not True or not isinstance(request.get('host_disconnected'), dict):
                raise ValueError('explicit reader drain and host disconnect proof required')
            proof_path = work / ('host-disconnected-' + generation + '.json')
            _write_matching(proof_path, request['host_disconnected'])
            receipt['target_cleanup_attempted'] = True
            receipt['target'] = json.loads(call([sys.executable, '-B', script, 'target-cleanup',
                '--state', state, '--readers-drained', '--host-disconnected', proof_path]))
        receipt['complete'] = True
    except Exception as exc:
        receipt['error'] = type(exc).__name__ + ': ' + str(exc)
        if state is not None and state.exists():
            try:
                receipt['target'] = target_status(state)
            except Exception as status_error:
                receipt['target_status_error'] = type(status_error).__name__ + ': ' + str(status_error)
    return receipt
