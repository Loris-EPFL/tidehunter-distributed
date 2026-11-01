"""Gate-controlled, owner-local NVMe target trace capture and raw evidence export."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import uuid

import cold_device_trace
import nvme_campaign_owner as owner

ACTIONS = {'nvme_trace_start', 'nvme_trace_stop'}
MAX_EXPORTS = 256
MAX_FILE = 64 * 1024 * 1024
MAX_TOTAL = 512 * 1024 * 1024
MAX_FILES = 2048


def _uuid(value):
    if isinstance(value, list) and len(value) == 16 and all(type(x) is int and 0 <= x <= 255 for x in value):
        return str(uuid.UUID(bytes=bytes(value)))
    if isinstance(value, str):
        return str(uuid.UUID(value))
    raise ValueError('invalid export UUID')


def _exports(work, generation, controllers):
    export = work / 'export'
    state_dir = work / ('target-state-' + generation)
    owner._trusted_dir(export)
    owner._trusted_dir(state_dir)
    state = owner._read_json(state_dir / 'state.json')
    ready = owner._read_json(export / 'ready.json')
    entries, targets = ready.get('exports'), state.get('exports')
    if state.get('kind') != 'campaign-target' or state.get('phase') != 'published' or state.get('export_root') != str(export):
        raise ValueError('exact published campaign target required')
    if not isinstance(entries, list) or not isinstance(targets, list) or len(entries) > MAX_EXPORTS or len(entries) != len(targets):
        raise ValueError('invalid target export inventory')
    if not isinstance(controllers, list) or len(controllers) != len(entries):
        raise ValueError('controller mapping must cover exactly the exports')
    mapping = {}
    for item in controllers:
        ident = _uuid(item['export_uuid'])
        ctrl = item['ctrl_id']
        if ident in mapping or type(ctrl) is not int or not 1 <= ctrl <= 65535:
            raise ValueError('duplicate export or invalid controller ID')
        mapping[ident] = ctrl
    found = set()
    result = []
    for number, (entry, target) in enumerate(zip(entries, targets)):
        manifest = Path(entry['manifest_path'])
        if not manifest.is_absolute() or '..' in manifest.parts or not manifest.is_relative_to(export) or manifest.name != 'manifest.json':
            raise ValueError('manifest outside exact export root')
        owner._trusted_dir(manifest.parent)
        manifest_raw = owner._read_bytes(manifest)
        if len(manifest_raw) > owner.MAX_JSON:
            raise ValueError('manifest exceeds size bound')
        data = json.loads(manifest_raw)
        manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
        ident = _uuid(data['export_uuid'])
        if ident in found or ident not in mapping:
            raise ValueError('export UUID membership mismatch')
        found.add(ident)
        for field in ('export_uuid', 'nqn', 'nsid', 'image_len', 'block_size'):
            if entry.get(field) != data.get(field):
                raise ValueError('ready and manifest disagree: ' + field)
        if (target.get('manifest') != str(manifest) or target.get('manifest_sha256') != manifest_sha256
                or _uuid(target.get('export_uuid')) != ident or target.get('uuid') != ident
                or target.get('nqn') != data.get('nqn') or target.get('nsid') != data.get('nsid')
                or target.get('status') != 'published' or target.get('state') != str(state_dir / ('export-%03d' % number))):
            raise ValueError('target state and immutable export disagree')
        image = manifest.parent / 'image.wal'
        info = image.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
                or info.st_nlink != 1 or type(data.get('image_len')) is not int or info.st_size != data['image_len']):
            raise ValueError('untrusted export image')
        device = cold_device_trace.resolve_device(image)
        name = Path(device['sysfs_path']).name
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or name in ('.', '..'):
            raise ValueError('invalid backing device')
        result.append({'export_uuid': entry['export_uuid'], 'uuid': ident, 'ctrl_id': mapping[ident],
                       'manifest': str(manifest), 'manifest_sha256': manifest_sha256,
                       'image': str(image), 'device': device,
                       'block_device': name, 'directory': 'export-%03d' % number})
    if found != set(mapping):
        raise ValueError('controller mapping contains foreign export')
    return result


def _upload(directory, config, clients, key_root, remaining):
    files, errors = [], []
    total = 0
    if not directory.exists():
        return files, errors
    owner._trusted_dir(directory)
    for path in sorted(directory.iterdir()):
        try:
            st = path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022 or st.st_nlink != 1:
                raise ValueError('unsafe trace file')
            total += st.st_size
            if len(files) + len(errors) >= MAX_FILES or st.st_size > MAX_FILE or total > MAX_TOTAL:
                raise ValueError('trace upload bound exceeded')
            remaining()
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as stream:
                raw = stream.read(MAX_FILE + 1)
            if len(raw) != st.st_size:
                raise ValueError('trace file changed while reading')
            digest = hashlib.sha256(raw).hexdigest()
            key = key_root + '/' + path.name
            _s3_upload(config, key, raw, digest, remaining)
            files.append({'key': key, 'bytes': len(raw), 'sha256': digest})
        except Exception as exc:
            errors.append(path.name + ': ' + type(exc).__name__ + ': ' + str(exc))
    return files, errors


def execute(config, request, clients, *, command):
    action, case, generation, index = (request.get(k) for k in ('action', 'case_id', 'generation', 'index'))
    if action not in ACTIONS or not isinstance(case, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case):
        raise ValueError('invalid trace action or case')
    if not isinstance(generation, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', generation):
        raise ValueError('invalid generation')
    if type(index) is not int or index not in range(5):
        raise ValueError('invalid owner index')
    timeout = request.get('timeout')
    if type(timeout) not in (int, float) or not 0 < timeout <= 86400:
        raise ValueError('bounded positive timeout required')
    deadline = time.monotonic() + timeout
    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError('trace action deadline expired')
        return seconds
    run = config['prefix']
    if not isinstance(run, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', run) or config['fleet']['run_id'] != run:
        raise ValueError('invalid exact run')
    root = Path(config['fleet']['data_root'])
    if root != Path('/mnt/tidehunter-kv-comparison') / run:
        raise ValueError('invalid exact campaign root')
    work = root / 'cells' / case / ('node-' + str(index)) / 'nvmeof'
    owner._trusted_dir(work)
    trace_root = work / ('trace-' + generation)
    script = Path(config['repository_root']) / 'experiments/nvmeof/trace.py'
    key_root = f'{run}/results/cells/{case}/storage/{index}/nvmeof/trace-{generation}'
    receipt = {'action': action, 'case_id': case, 'generation': generation, 'index': index,
               'complete': False, 'traces': [], 'errors': []}
    attempted = []
    admitted = False
    try:
        exports = _exports(work, generation, request.get('controllers'))
        if action == 'nvme_trace_start':
            duration = request.get('duration_seconds')
            if type(duration) not in (int, float) or not 0 < duration <= 3600:
                raise ValueError('bounded trace duration required')
            trace_root.mkdir(mode=0o700)
            admitted = True
            owner._trusted_dir(trace_root)
            owner._write_new(trace_root / 'bindings.json', exports)
            for item in exports:
                directory = trace_root / item['directory']
                record = {'export_uuid': item['export_uuid'], 'ctrl_id': item['ctrl_id'], 'directory': str(directory)}
                receipt['traces'].append(record)
                attempted.append((item, record))
                owner._write_new(trace_root / (item['directory'] + '-attempt.json'),
                                 {'export_uuid': item['export_uuid'], 'ctrl_id': item['ctrl_id']})
                argv = [sys.executable, '-B', script, '--start', '--seconds', str(duration), '--ctrl-id', str(item['ctrl_id']),
                        '--block-device', item['block_device'], '--output', directory]
                result = json.loads(command(argv, timeout=remaining()))
                owner._write_new(trace_root / (item['directory'] + '-start.json'), result)
                record['start'] = result
                if not result.get('available') or 'start_monotonic_ns' not in result or 'pid' not in result:
                    raise RuntimeError('trace collector did not remain running at gate')
        else:
            owner._trusted_dir(trace_root)
            if owner._read_json(trace_root / 'bindings.json') != exports:
                raise ValueError('trace bindings differ from admitted target')
            admitted = True
            attempted = [(item, {'export_uuid': item['export_uuid'], 'ctrl_id': item['ctrl_id'],
                                 'directory': str(trace_root / item['directory'])}) for item in exports]
            receipt['traces'] = [record for _, record in attempted]
        receipt['complete'] = True
    except Exception as exc:
        receipt['errors'].append(type(exc).__name__ + ': ' + str(exc))
    if action == 'nvme_trace_stop' or (action == 'nvme_trace_start' and not receipt['complete']):
        receipt['cleanup_complete'] = admitted
        receipt['upload_complete'] = admitted
        # Cleanup gets a separate, bounded grace period even if startup exhausted
        # the request deadline; collectors otherwise run until their own timeout.
        cleanup_deadline = max(deadline, time.monotonic() + 30)
        def cleanup_remaining():
            seconds = cleanup_deadline - time.monotonic()
            if seconds <= 0:
                raise TimeoutError('trace cleanup deadline expired')
            return seconds
        for item, record in attempted:
            directory = trace_root / item['directory']
            try:
                attempt_file = trace_root / (item['directory'] + '-attempt.json')
                if not attempt_file.exists():
                    if directory.exists():
                        raise ValueError('collector directory without attempt proof')
                    record['cleanup'] = 'not_attempted'
                    receipt['complete'] = False
                    continue
                attempt = owner._read_json(attempt_file)
                if attempt != {'export_uuid': item['export_uuid'], 'ctrl_id': item['ctrl_id']}:
                    raise ValueError('trace attempt identity mismatch')
                if not directory.exists():
                    start_file = trace_root / (item['directory'] + '-start.json')
                    start = owner._read_json(start_file)
                    if start.get('available') is not False or start.get('reason') != 'collector exited before tracing started':
                        raise ValueError('missing collector state without proof of exit')
                    record['cleanup'] = 'exited_before_start'
                    record['summary'] = start
                    receipt['complete'] = False
                    continue
                summary = json.loads(command([sys.executable, '-B', script, '--stop', directory], timeout=min(16, cleanup_remaining())))
                record['summary'] = summary
                state = owner._read_json(directory / 'state.json')
                if state.get('kind') != 'trace' or state.get('phase') != 'cleaned':
                    if summary.get('reason') == 'collector exited before cleanup; use --recover':
                        command([sys.executable, '-B', script, '--recover', directory], timeout=min(16, cleanup_remaining()))
                        state = owner._read_json(directory / 'state.json')
                    if state.get('kind') != 'trace' or state.get('phase') != 'cleaned':
                        raise RuntimeError('collector cleanup not proven')
                record['cleanup'] = 'cleaned'
                if not summary.get('available'):
                    receipt['complete'] = False
            except Exception as exc:
                record['stop_error'] = type(exc).__name__ + ': ' + str(exc)
                receipt['cleanup_complete'] = False
                receipt['complete'] = False
        # Stop every collector before potentially slow artifact uploads.
        for item, record in attempted:
            directory = trace_root / item['directory']
            try:
                record['files'], errors = _upload(directory, config, clients, key_root + '/' + item['directory'], cleanup_remaining)
                if errors:
                    record['upload_errors'] = errors
                    receipt['upload_complete'] = False
                    receipt['complete'] = False
            except Exception as exc:
                record['upload_error'] = type(exc).__name__ + ': ' + str(exc)
                receipt['upload_complete'] = False
                receipt['complete'] = False
        if admitted and trace_root.exists():
            try:
                # Include the admission binding alongside the raw collector files.
                records, errors = _upload_binding(trace_root, config, clients, key_root, cleanup_remaining)
                receipt['binding_files'] = records
                if errors:
                    receipt['errors'].extend(errors)
                    receipt['upload_complete'] = False
                    receipt['complete'] = False
            except Exception as exc:
                receipt['errors'].append(type(exc).__name__ + ': ' + str(exc))
                receipt['upload_complete'] = False
                receipt['complete'] = False
    return receipt


def _upload_binding(root, config, clients, key_root, remaining):
    if not (root / 'bindings.json').exists():
        return [], ['bindings.json missing']
    paths = [root / 'bindings.json']
    paths.extend(sorted(p for p in root.iterdir()
                        if re.fullmatch(r'export-[0-9]{3}-(?:attempt|start)\.json', p.name)))
    if len(paths) > 1 + 2 * MAX_EXPORTS:
        raise ValueError('trace journal file bound exceeded')
    records, errors = [], []
    for path in paths:
        try:
            raw = json.dumps(owner._read_json(path), sort_keys=True).encode()
            digest = hashlib.sha256(raw).hexdigest()
            key = key_root + '/' + path.name
            _s3_upload(config, key, raw, digest, remaining)
            records.append({'key': key, 'bytes': len(raw), 'sha256': digest})
        except Exception as exc:
            errors.append(path.name + ': ' + type(exc).__name__ + ': ' + str(exc))
    return records, errors


def _s3_upload(config, key, raw, digest, remaining):
    """Run each SDK transaction in a killable process bounded by the action deadline."""
    args = {'bucket': config['bucket'], 'account': config['account'],
            'region': config['bucket_region'], 'key': key, 'sha256': digest, 'size': len(raw)}
    subprocess.run([sys.executable, '-B', __file__, '--upload-worker', json.dumps(args)],
                   input=raw, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   timeout=remaining(), check=True)


def _upload_worker(args):
    # The guest only admits instance-profile credentials. Create a fresh client
    # in this process; inherited botocore clients are not process-safe.
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=args['region'])
    credentials = session.get_credentials()
    if credentials is None or credentials.method != 'iam-role':
        raise ValueError('instance-profile credentials required')
    s3 = session.client('s3', config=Config(connect_timeout=4, read_timeout=15,
                                            retries={'total_max_attempts': 1, 'mode': 'standard'}))
    raw = sys.stdin.buffer.read(MAX_FILE + 1)
    if len(raw) != args['size'] or hashlib.sha256(raw).hexdigest() != args['sha256']:
        raise ValueError('trace upload bytes differ from admitted digest')
    s3.put_object(Bucket=args['bucket'], Key=args['key'], Body=raw, ExpectedBucketOwner=args['account'],
                  ServerSideEncryption='AES256', Metadata={'sha256': args['sha256']})
    head = s3.head_object(Bucket=args['bucket'], Key=args['key'], ExpectedBucketOwner=args['account'])
    if head.get('ContentLength') != len(raw) or head.get('Metadata', {}).get('sha256') != args['sha256']:
        raise ValueError('uploaded trace identity not confirmed')


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '--upload-worker':
        raise SystemExit('upload worker invocation required')
    _upload_worker(json.loads(sys.argv[2]))
