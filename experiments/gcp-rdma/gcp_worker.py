"""Exact-instance GCS mailbox worker; never executes a request twice.

A request either carries an admitted `argv` from the sealed package or one
campaign shell script (`/bin/sh -c`), and it may name one input blob and one
output blob under the run's transfer prefix so a campaign command can read
stdin and spill a large stdout through the bucket instead of the receipt. Both
blobs are bound to the request's own operation ID, both are SHA-256 verified,
and both run under the existing identity, cwd, environment, timeout,
at-most-once and poweroff rules.
"""
import concurrent.futures
import fcntl
import hashlib
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
sys.dont_write_bytecode = True
import urllib.request
from pathlib import Path

import gcp_api
import gcp_runtime
import gcp_poweroff

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
from control_execution import execute_at_most_once
import cloud_package
import physical_storage

MAX_OUTPUT = 1024 * 1024
# The campaign drivers issue one shell script per host command. This is the
# largest script the worker will admit; the drivers emit well under it.
MAX_SCRIPT = 64 * 1024
# One input blob and one output blob per request, both streamed through the
# bucket. A larger stdout must be split by the caller.
MAX_BLOB = 16 * 1024 * 1024 * 1024
MAX_ACTIVE = 16
MAX_REQUESTS = 32768
MAX_ATTEMPTS = 3
SHELL = '/bin/sh'
DATA_ROOT = '/mnt/tidehunter-kv-comparison'



class InvalidRequest(gcp_runtime.RuntimeSafetyError):
    """A malformed or inadmissible immutable request, not a delivery failure."""

class WorkerState:
    """SQLite-backed request accounting, locked for the worker lifetime."""
    def __init__(self, root, prefix):
        self.root, self.prefix = str(root), prefix
        self.path = os.path.join(self.root, 'mailbox-state.sqlite3')
        self.lock_fd = os.open(os.path.join(self.root, 'mailbox-state.lock'),
                               os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self.db = None
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self.db = sqlite3.connect(self.path, timeout=0)
            self.db.execute('PRAGMA journal_mode=DELETE')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.execute('CREATE TABLE IF NOT EXISTS requests '
                            '(key TEXT PRIMARY KEY, generation TEXT NOT NULL, attempts INTEGER NOT NULL, '
                            'status TEXT NOT NULL, retry_after REAL NOT NULL)')
            self.db.commit()
            if self.db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                raise ValueError('corrupt mailbox state')
            rows = self.db.execute('SELECT key, generation, attempts, status, retry_after '
                                   'FROM requests LIMIT ?', (MAX_REQUESTS + 1,)).fetchall()
            if len(rows) > MAX_REQUESTS:
                raise ValueError('invalid mailbox state')
            self.entries = {}
            for key, generation, attempts, status, retry_after in rows:
                record = {'generation': generation, 'attempts': attempts,
                          'status': status, 'retry_after': retry_after}
                if (not self.valid_key(key) or type(attempts) is not int or
                        not 1 <= attempts <= MAX_ATTEMPTS or not isinstance(generation, str) or
                        not generation.isascii() or not generation.isdecimal() or len(generation) > 32 or
                        status not in ('pending', 'completed', 'rejected') or
                        type(retry_after) not in (int, float) or
                        not 0 <= retry_after <= 253402300799):
                    raise ValueError('invalid mailbox state entry')
                self.entries[key] = record
        except BaseException:
            if self.db is not None:
                self.db.close()
            os.close(self.lock_fd)
            raise

    def valid_key(self, key):
        return (isinstance(key, str) and key.startswith(self.prefix) and
                re.fullmatch(r'[A-Za-z0-9_-]{1,128}\.json', key[len(self.prefix):]) is not None)

    def save(self, key):
        record = self.entries[key]
        with self.db:
            self.db.execute('INSERT INTO requests (key, generation, attempts, status, retry_after) '
                            'VALUES (?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET '
                            'generation=excluded.generation, attempts=excluded.attempts, '
                            'status=excluded.status, retry_after=excluded.retry_after',
                            (key, record['generation'], record['attempts'],
                             record['status'], record['retry_after']))

    def close(self):
        try:
            self.db.close()
        finally:
            os.close(self.lock_fd)

def metadata(path):
    request = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path,
                                     headers={'Metadata-Flavor': 'Google'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            raise gcp_runtime.RuntimeSafetyError('untrusted metadata response')
        return response.read(4096).decode().strip()


def identify(config):
    project = metadata('project/project-id')
    number = metadata('project/numeric-project-id')
    instance_id = metadata('instance/id')
    name = metadata('instance/name')
    zone = metadata('instance/zone').rsplit('/', 1)[-1]
    matches = [host for host in config['fleet'] if str(host['id']) == instance_id]
    if project != config['project_id'] or not number.isdecimal() or len(matches) != 1:
        raise gcp_runtime.RuntimeSafetyError('metadata project or instance mismatch')
    host = matches[0]
    if host['name'] != name or host['zone'] != zone:
        raise gcp_runtime.RuntimeSafetyError('metadata name or zone mismatch')
    if str(config.get('project_number', '')) != number:
        raise gcp_runtime.RuntimeSafetyError('metadata project number mismatch')
    return host


def admit_scratch(config, host, api):
    """Admit API, metadata, symlink and physical local NVMe identities."""
    instance = api.request('GET', f'zones/{host["zone"]}/instances/{host["name"]}')
    if str(instance.get('id')) != str(host['id']) or instance.get('name') != host['name']:
        raise gcp_runtime.RuntimeSafetyError('storage instance identity mismatch')
    disks = instance.get('disks', [])
    scratch = [d for d in disks if d.get('type') == 'SCRATCH']
    expected = gcp_runtime.LOCAL_SSD_COUNTS.get(config['machine_type'])
    if (not expected or len(scratch) != expected or
            any(d.get('interface') != 'NVME' or d.get('boot') for d in scratch)):
        raise gcp_runtime.RuntimeSafetyError('invalid API scratch disk inventory')
    entries = metadata('instance/disks/').splitlines()
    if (not entries or len(entries) > 128 or len(set(entries)) != len(entries) or
            any(not re.fullmatch(r'[0-9]+/', e) for e in entries)):
        raise gcp_runtime.RuntimeSafetyError('invalid metadata disk listing')
    guest_types = [metadata('instance/disks/' + entry + 'type') for entry in entries]
    # Compute uses SCRATCH for attachments; metadata exposes LOCAL-SSD media.
    if guest_types.count('LOCAL-SSD') != expected:
        raise gcp_runtime.RuntimeSafetyError(
            f'scratch metadata mismatch: expected {expected} LOCAL-SSD entries, got {guest_types}')
    links = sorted((p for p in Path('/dev/disk/by-id').glob('google-local-nvme-ssd-*')
                    if re.fullmatch(r'google-local-nvme-ssd-[0-9]+', p.name)),
                   key=lambda p: p.name)
    if len(links) != expected or any(not link.is_symlink() for link in links):
        raise gcp_runtime.RuntimeSafetyError('GCE local NVMe scratch inventory mismatch')
    devices = [str(link.resolve(strict=True)) for link in links]
    if len(set(devices)) != expected or any(not re.fullmatch(r'/dev/nvme[0-9]+n[0-9]+', d) for d in devices):
        raise gcp_runtime.RuntimeSafetyError('invalid local SSD devices')
    if config['machine_type'] == 'h4d-highmem-192-lssd':
        indexes = [i for i, link in enumerate(links) if link.name == 'google-local-nvme-ssd-0']
        if len(indexes) != 1:
            raise gcp_runtime.RuntimeSafetyError('H4D scratch zero identity missing')
        selected = indexes[0]
    else:
        selected = 0
    for disk in disks:
        if disk.get('boot'):
            boot_name = disk.get('deviceName') or host['name']
            if not isinstance(boot_name, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', boot_name):
                raise gcp_runtime.RuntimeSafetyError('invalid boot disk identity')
            boot_link = Path('/dev/disk/by-id') / ('google-' + boot_name)
            if not boot_link.is_symlink() or str(boot_link.resolve(strict=True)) in devices:
                raise gcp_runtime.RuntimeSafetyError('boot disk identity unavailable or overlaps scratch')
    if sum(bool(d.get('boot')) for d in disks) != 1:
        raise gcp_runtime.RuntimeSafetyError('expected exactly one boot disk')
    inventory = []
    for path in devices:
        info = json.loads(subprocess.check_output(
            ['lsblk', '--json', '--bytes', '--output', 'NAME,PATH,TYPE,MODEL,SERIAL,SIZE,MOUNTPOINTS', path],
            text=True, timeout=10))
        rows = info.get('blockdevices', [])
        if len(rows) != 1:
            raise gcp_runtime.RuntimeSafetyError('local SSD device unavailable')
        disk = rows[0]
        if (disk.get('path') != path or disk.get('type') != 'disk' or
                not physical_storage.admitted_model('gcp', disk.get('model')) or
                not isinstance(disk.get('serial'), str) or not disk['serial'].strip() or
                type(disk.get('size')) is not int or disk['size'] <= 0):
            raise gcp_runtime.RuntimeSafetyError(
                'local SSD model or serial not admitted: ' + json.dumps(
                    {key: disk.get(key) for key in ('path', 'type', 'model', 'serial', 'size')}, sort_keys=True))
        inventory.append(disk)
    if len({disk['serial'].strip() for disk in inventory}) != expected:
        raise gcp_runtime.RuntimeSafetyError('local SSD physical serials are not unique')
    disk = inventory[selected]
    return {'device': devices[selected], 'model': disk['model'], 'serial': disk['serial'],
            'size': disk['size'], 'block_device': disk}


def prepare_storage(config, host, api):
    admitted = admit_scratch(config, host, api)
    device, disk = admitted['device'], admitted['block_device']
    target = Path('/mnt/tidehunter-kv-comparison') / config['run_id']
    target.mkdir(parents=True, mode=0o700, exist_ok=True)
    evidence_path = target / 'storage-evidence.json'
    mounted = subprocess.run(['findmnt', '-rn', '-M', str(target), '-o', 'SOURCE'], capture_output=True, text=True, timeout=10)
    if mounted.returncode == 0:
        if Path(mounted.stdout.strip()).resolve() != Path(device):
            raise gcp_runtime.RuntimeSafetyError('run data mount belongs to another device')
        return (json.loads(evidence_path.read_text()) if evidence_path.exists() else
                {'device': device, 'mkfs_duration_seconds': None,
                 'lazy_initialization_disabled': None, 'reason': 'existing mount has no formatting evidence'})
    if mounted.returncode != 1:
        raise gcp_runtime.RuntimeSafetyError('could not inspect run data mount')
    if disk.get('children') or any(disk.get('mountpoints') or []):
        raise gcp_runtime.RuntimeSafetyError('refusing partitioned or mounted local SSD')
    signatures = json.loads(subprocess.check_output(
        ['wipefs', '--no-act', '--json', device], text=True, timeout=10))
    if not isinstance(signatures, dict) or signatures.get('signatures') != []:
        raise gcp_runtime.RuntimeSafetyError('refusing local SSD with signatures or invalid probe')
    probe = subprocess.run(['blkid', '-p', '-o', 'export', device],
                           capture_output=True, text=True, timeout=10)
    if probe.returncode != 2 or probe.stdout.strip() or probe.stderr.strip():
        raise gcp_runtime.RuntimeSafetyError('refusing local SSD with signatures or failed probe')
    remaining = gcp_runtime._deadline(config['deadline_utc']) - time.time()
    if remaining <= 0:
        raise gcp_runtime.RuntimeSafetyError('storage admission expired')
    mkfs_args = ['mkfs.ext4', '-F', '-T', 'largefile4', '-E', 'lazy_itable_init=0,lazy_journal_init=0', device]
    mkfs_started = time.monotonic()
    # largefile4 uses one inode per 4 MiB (~0.9 GiB of inode tables on 14,000 GiB).
    # Bound eager inode/journal initialization to ten minutes and the fleet expiry.
    subprocess.run(mkfs_args, check=True, timeout=min(600, remaining))
    evidence = {'run_id': config['run_id'], 'device': device, 'mkfs_arguments': mkfs_args,
                'mkfs_duration_seconds': time.monotonic() - mkfs_started,
                'lazy_initialization_disabled': True, 'inode_ratio_bytes': 4 * 1024**2,
                'mkfs_profile': 'largefile4'}
    remaining = gcp_runtime._deadline(config['deadline_utc']) - time.time()
    if remaining <= 0:
        raise gcp_runtime.RuntimeSafetyError('storage admission expired')
    filesystem = subprocess.check_output(
        ['tune2fs', '-l', device], text=True, timeout=min(30, remaining),
        env={**os.environ, 'LC_ALL': 'C'})
    inode_counts = re.findall(r'^Inode count:[ \t]*([0-9]+)[ \t]*$', filesystem, flags=re.MULTILINE)
    if len(inode_counts) != 1 or int(inode_counts[0]) <= 0:
        raise gcp_runtime.RuntimeSafetyError('formatted scratch inode count unavailable or invalid')
    evidence['inode_count'] = int(inode_counts[0])
    remaining = gcp_runtime._deadline(config['deadline_utc']) - time.time()
    if remaining <= 0:
        raise gcp_runtime.RuntimeSafetyError('storage admission expired')
    subprocess.run(['mount', device, str(target)], check=True, timeout=min(30, remaining))

    evidence_path.write_text(json.dumps(evidence) + '\n')
    return evidence

def _inside(path, root):
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
        return True
    except (ValueError, OSError):
        return False


def run_roots(config):
    """Every directory this run owns on the guest; transfers may not leave it."""
    roots = [Path('/mnt/tidehunter') / config['run_id'], Path(DATA_ROOT) / config['run_id']]
    roots.extend(Path(value) for value in config.get('guest_roots') or [])
    return roots


def transfer_key(config, host, operation_id, leaf):
    """The one bucket key a request may read (`stdin`) or write (`stdout`)."""
    return f'runs/{config["run_id"]}/transfer/{host["id"]}/{operation_id}/{leaf}'


def admit_transfer(config, host, op, request):
    """Admit the optional input blob and output blob of one request."""
    source, sink = request.get('stdin'), request.get('stdout_object')
    if source is not None:
        if (not isinstance(source, dict) or set(source) != {'object', 'sha256', 'size'}
                or source['object'] != transfer_key(config, host, op, 'stdin')
                or not isinstance(source['sha256'], str)
                or re.fullmatch(r'[0-9a-f]{64}', source['sha256']) is None
                or type(source['size']) is not int or not 0 <= source['size'] <= MAX_BLOB):
            raise gcp_runtime.RuntimeSafetyError('invalid command stdin blob')
    if sink is not None and sink != transfer_key(config, host, op, 'stdout'):
        raise gcp_runtime.RuntimeSafetyError('invalid command stdout blob')
    return source, sink


def admit_command(config, request):
    argv = request.get('argv')
    if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or '\x00' in x for x in argv):
        raise gcp_runtime.RuntimeSafetyError('invalid command')
    root = Path(config['package_root'])
    if argv[0] == SHELL:
        # One bounded shell script replaces a sealed argv for campaign commands
        # that no admitted binary can express: archive extraction, process-group
        # start and stop, `/proc` sampling and the disk collector's stdin.
        if (len(argv) != 3 or argv[1] != '-c' or not argv[2]
                or len(argv[2].encode('utf-8', 'surrogatepass')) > MAX_SCRIPT):
            raise gcp_runtime.RuntimeSafetyError('invalid campaign shell command')
    elif argv[0] == gcp_runtime.GUEST_PYTHON:
        if len(argv) < 2 or not argv[1].endswith('.py') or not _inside(Path(argv[1]), root / 'source'):
            raise gcp_runtime.RuntimeSafetyError('Python script must be from admitted source')
    elif not _inside(Path(argv[0]), root / 'runtime' / 'bin'):
        raise gcp_runtime.RuntimeSafetyError('executable must be from admitted runtime')
    cwd = request.get('cwd')
    if cwd is not None and not any(_inside(Path(cwd), root) for root in run_roots(config)):
        raise gcp_runtime.RuntimeSafetyError('working directory outside run root')
    env = request.get('env')
    if not isinstance(env, dict) or any(not re.fullmatch(r'[A-Z][A-Z0-9_]*', k) or not isinstance(v, str)
                                        or '\x00' in v or k.startswith(('LD_', 'PYTHON', 'BASH_', 'GCONV_', 'PERL', 'RUBY'))
                                        or k in ('PATH', 'IFS', 'ENV', 'SHELLOPTS', 'CDPATH')
                                        for k, v in env.items()):
        raise gcp_runtime.RuntimeSafetyError('unsafe command environment')
    return argv, cwd, env


def _file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch_stdin(store, blob, directory, deadline):
    """Download the admitted input blob and prove its digest before it is used."""
    handle, path = tempfile.mkstemp(dir=str(directory))
    os.close(handle)
    try:
        def check():
            if time.time() >= deadline:
                raise gcp_runtime.RuntimeSafetyError('command expired before its input arrived')
        store.download(blob['object'], path, max_bytes=max(blob['size'], 1), deadline_check=check)
        if os.path.getsize(path) != blob['size'] or _file_sha256(path) != blob['sha256']:
            raise gcp_runtime.RuntimeSafetyError('command stdin blob differs from its admitted digest')
    except BaseException:
        os.unlink(path)
        raise
    return path


def scratch_root(config, gate_root):
    """Staging for command I/O: the run's data root when it is mounted, else the gate."""
    candidate = Path(DATA_ROOT) / config['run_id']
    try:
        if candidate.is_dir():
            return candidate / '.mailbox'
    except OSError:
        pass
    return Path(gate_root) / 'scratch'


def run_request(config, host, store, key, generation, gate_root):
    raw = store.get(key, generation=generation, max_bytes=4 * 1024 * 1024)
    try:
        request = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidRequest('invalid request JSON') from exc
    if not isinstance(request, dict):
        raise InvalidRequest('request must be an object')
    op = request.get('operation_id')
    expected = f'runs/{config["run_id"]}/mailbox/{host["id"]}/requests/{op}.json'
    if (not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', op) or key != expected
        or request.get('schema_version') != 1 or request.get('run_id') != config['run_id']
        or request.get('source_sha256') != config['source_sha256'] or request.get('instance_id') != str(host['id'])
        or request.get('index') != host['index'] or request.get('deadline_utc') != config['deadline_utc']):
        raise InvalidRequest('request identity mismatch')
    deadline = gcp_runtime._deadline(config['deadline_utc'])
    timeout = request.get('timeout_seconds')
    if type(timeout) is not int or timeout < 1 or time.time() >= deadline:
        raise InvalidRequest('expired or invalid command timeout')
    try:
        argv, cwd, env = admit_command(config, request)
        source, sink = admit_transfer(config, host, op, request)
    except gcp_runtime.RuntimeSafetyError as exc:
        raise InvalidRequest(str(exc)) from exc
    request_sha = hashlib.sha256(raw).hexdigest()

    def execute():
        remaining = min(timeout, deadline - time.time())
        if remaining <= 0:
            raise gcp_runtime.RuntimeSafetyError('command expired before start')
        scratch = scratch_root(config, gate_root)
        scratch.mkdir(parents=True, mode=0o700, exist_ok=True)
        source_path = _fetch_stdin(store, source, scratch, deadline) if source is not None else None
        stdout_file = tempfile.NamedTemporaryFile(dir=str(scratch), delete=False)
        stderr_file = tempfile.NamedTemporaryFile(dir=str(scratch), delete=False)
        stdout_path, stderr_path = stdout_file.name, stderr_file.name
        spill = None
        try:
            stdin_stream = open(source_path, 'rb') if source_path else open(os.devnull, 'rb')
            try:
                process = subprocess.Popen(
                    argv, cwd=cwd,
                    env={**os.environ, **env, 'PATH': gcp_runtime.GUEST_PATH, 'PYTHONDONTWRITEBYTECODE': '1'},
                    stdin=stdin_stream, stdout=stdout_file, stderr=stderr_file, start_new_session=True)
                try:
                    code = process.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                    code = 124
            finally:
                stdin_stream.close()
            stdout_file.close()
            stderr_file.close()
            size = os.path.getsize(stdout_path)
            with open(stderr_path, 'rb') as stream:
                err = stream.read(MAX_OUTPUT + 1)
            if len(err) > MAX_OUTPUT:
                raise gcp_runtime.RuntimeSafetyError('command output exceeds mailbox bound')
            if size > MAX_OUTPUT:
                # A campaign copy-back (a tar of an output tree) is larger than
                # a receipt may carry; it goes to the bucket instead.
                if sink is None or size > MAX_BLOB:
                    raise gcp_runtime.RuntimeSafetyError('command output exceeds mailbox bound')
                if time.time() >= deadline:
                    raise gcp_runtime.RuntimeSafetyError('command expired before its output was stored')
                receipt = store.upload(sink, stdout_path, content_type='application/octet-stream',
                                       if_generation_match=0)
                digest = (receipt.get('metadata') or {}).get('sha256')
                if digest is None or int(receipt.get('size', -1)) != size:
                    raise gcp_runtime.RuntimeSafetyError('command output upload receipt mismatch')
                spill = {'object': sink, 'sha256': digest, 'size': size}
                out = b''
            else:
                with open(stdout_path, 'rb') as stream:
                    out = stream.read()
        finally:
            stdout_file.close()
            stderr_file.close()
            for path in (source_path, stdout_path, stderr_path):
                if path is not None:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
        if len(out) > MAX_OUTPUT:
            raise gcp_runtime.RuntimeSafetyError('command output exceeds mailbox bound')
        return {'schema_version': 1, 'operation_id': op, 'instance_id': str(host['id']),
                'source_sha256': config['source_sha256'], 'request_sha256': request_sha,
                'exit_code': code, 'stdout': out.decode('utf-8', 'replace'),
                'stderr': err.decode('utf-8', 'replace'),
                **({'stdout_object': spill} if spill is not None else {})}

    result = execute_at_most_once(gate_root, key, request, execute, deadline_epoch=deadline)
    response_key = key.replace('/requests/', '/responses/')
    gcp_runtime._put_once(store, response_key, gcp_runtime._encode(result))
    if (op == 'campaign' and host['index'] == 0
            and argv == [gcp_runtime.GUEST_PYTHON, config['package_root'] + '/source/experiments/gcp-rdma/gcp_runtime.py',
                         'execute', '/mnt/tidehunter/' + config['run_id'] + '/runtime.json']):
        gcp_poweroff.publish_terminal(config, host, store, result, response_key)
    return result


def publish_rejection(store, config, host, key, generation, error):
    """Publish the terminal refusal so the caller learns why it was refused.

    A refused request never runs, so this receipt is the only evidence the
    caller can read instead of waiting for a response that cannot arrive.
    """
    try:
        raw = store.get(key, generation=generation, max_bytes=4 * 1024 * 1024)
        request = json.loads(raw)
        op = request.get('operation_id') if isinstance(request, dict) else None
        if not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', op):
            return None
        receipt = {'schema_version': 1, 'operation_id': op, 'instance_id': str(host['id']),
                   'source_sha256': config['source_sha256'],
                   'request_sha256': hashlib.sha256(raw).hexdigest(),
                   'exit_code': 126, 'stdout': '', 'stderr': str(error)[:4096], 'rejected': True}
        gcp_runtime._put_once(store, key.replace('/requests/', '/responses/'), gcp_runtime._encode(receipt))
        return receipt
    except Exception as exc:
        print(f'worker rejection receipt deferred: {type(exc).__name__}: {exc}',
              file=sys.stderr, flush=True)
        return None


def serve(config, *, api=None, poll_seconds=1):
    gcp_runtime.validate_runtime(config)
    host = identify(config)
    admission = cloud_package.verify_directory(Path(config['package_root']), runtime=True, provider='gcp')
    if admission['source_sha256'] != config['source_sha256']:
        raise gcp_runtime.RuntimeSafetyError('sealed source identity mismatch')
    api = api or gcp_api.GoogleApi(config['project_id'], token_provider=gcp_api.metadata_token)
    gcp_runtime.verify_fleet({'platform': {'runtime': config}}, {'platform': api})
    nvmet_fix = gcp_runtime.admit_nvmet_fix(config, host, admission)
    storage_evidence = prepare_storage(config, host, api)
    store = api.objects(config['bucket'])
    gate_root = Path('/var/lib/tidehunter-gcp-control') / config['run_id']
    gate_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    prefix = f'runs/{config["run_id"]}/mailbox/{host["id"]}/requests/'
    active = {}
    state = WorkerState(gate_root, prefix)
    deadline = gcp_runtime._deadline(config['deadline_utc'])
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_ACTIVE)
    next_poweroff_check = 0
    try:
        ready = {'run_id': config['run_id'], 'source_sha256': config['source_sha256'],
                 'manifest_sha256': admission['manifest_sha256'], 'deadline_utc': config['deadline_utc'],
                 'instance_name': host['name'], 'instance_id': str(host['id']), 'complete': True}
        ready['storage'] = storage_evidence
        if nvmet_fix is not None:
            ready['nvmet_fix'] = nvmet_fix
        gcp_runtime._put_once(
            store, f'runs/{config["run_id"]}/qualification/{host["name"]}/worker-ready.json',
            gcp_runtime._encode(ready))
        while time.time() < deadline:
            for key, future in list(active.items()):
                if future.done():
                    record = state.entries[key]
                    try:
                        future.result()
                    except InvalidRequest as exc:
                        print(f'worker rejected {key}: {exc}', file=sys.stderr, flush=True)
                        record['status'] = 'rejected'
                        publish_rejection(store, config, host, key, record['generation'], exc)
                    except Exception as exc:
                        print(f'worker operation {key}: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
                        if record['attempts'] >= MAX_ATTEMPTS:
                            record['status'] = 'rejected'
                            publish_rejection(store, config, host, key, record['generation'], exc)
                        else:
                            record['retry_after'] = time.time() + min(2 ** (record['attempts'] - 1), 8)
                    else:
                        record['status'] = 'completed'
                    state.save(key)
                    del active[key]
            if len(active) < MAX_ACTIVE:
                for item in store.list(prefix):
                    if time.time() >= deadline or len(active) >= MAX_ACTIVE:
                        break
                    key = item.get('name')
                    if not state.valid_key(key) or key in active:
                        continue
                    record = state.entries.get(key)
                    if record is not None:
                        if record['status'] != 'pending' or time.time() < record['retry_after']:
                            continue
                        if record['attempts'] >= MAX_ATTEMPTS:
                            record['status'] = 'rejected'
                            state.save(key)
                            continue
                    else:
                        if len(state.entries) >= MAX_REQUESTS:
                            raise gcp_runtime.RuntimeSafetyError('worker request limit exceeded')
                        generation = str(item['generation'])
                        if not generation.isascii() or not generation.isdecimal() or len(generation) > 32:
                            raise gcp_runtime.RuntimeSafetyError('invalid request generation')
                        record = {'generation': generation, 'attempts': 0, 'status': 'pending', 'retry_after': 0}
                        state.entries[key] = record
                    record['attempts'] += 1
                    record['retry_after'] = time.time() + min(2 ** (record['attempts'] - 1), 8)
                    state.save(key)
                    active[key] = pool.submit(run_request, config, host, store, key, record['generation'], gate_root)
            if time.monotonic() >= next_poweroff_check:
                next_poweroff_check = time.monotonic() + gcp_poweroff.POLL_SECONDS
                try:
                    outcome = gcp_poweroff.evaluate(config, host, store, active=bool(active))
                    if outcome in ('poweroff_no_acknowledgement', 'poweroff_hard_cap'):
                        subprocess.run(['/usr/bin/systemctl', 'poweroff'], check=True, timeout=30)
                except Exception as exc:
                    print(f'poweroff deferred: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
                    try:
                        gcp_poweroff.log_decision(config, host, store, time.time(), 'unknown')
                    except Exception as log_error:
                        print(f'poweroff decision log failed: {log_error}', file=sys.stderr, flush=True)
            time.sleep(min(poll_seconds, max(0, deadline - time.time())))
    finally:
        # Running I/O may still block; the systemd expiry and RuntimeMaxSec
        # terminate the worker process even when a thread cannot be cancelled.
        pool.shutdown(wait=False, cancel_futures=True)
        # A still-running future retains the process-wide lock until process exit.
        if all(future.done() for future in active.values()):
            state.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: gcp_worker.py RUNTIME_CONFIG')
    with open(sys.argv[1], encoding='utf-8') as stream:
        serve(json.load(stream))
