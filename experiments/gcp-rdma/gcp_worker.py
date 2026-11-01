"""Exact-instance GCS mailbox worker; never executes a request twice."""
import concurrent.futures
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
sys.dont_write_bytecode = True
import urllib.request
from pathlib import Path

import gcp_api
import gcp_runtime

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
from control_execution import execute_at_most_once
import cloud_package

MAX_OUTPUT = 1024 * 1024


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


def prepare_storage(config):
    """Admit the exact GCE scratch disk; format only a completely blank disk."""
    link = Path('/dev/disk/by-id/google-local-nvme-ssd-0')
    if not link.is_symlink():
        raise gcp_runtime.RuntimeSafetyError('GCE local NVMe scratch disk missing')
    device = str(link.resolve(strict=True))
    if not re.fullmatch(r'/dev/nvme[0-9]+n[0-9]+', device):
        raise gcp_runtime.RuntimeSafetyError('invalid local SSD device')
    info = json.loads(subprocess.check_output(['lsblk', '--json', '--nodeps', '--output', 'NAME,MODEL,SERIAL', device], text=True, timeout=10))
    devices = info.get('blockdevices', [])
    if len(devices) != 1 or 'local ssd' not in (devices[0].get('model') or '').lower() or not devices[0].get('serial'):
        raise gcp_runtime.RuntimeSafetyError('local SSD model or serial not admitted')
    target = Path('/mnt/tidehunter-kv-comparison') / config['run_id']
    target.mkdir(parents=True, mode=0o700, exist_ok=True)
    mounted = subprocess.run(['findmnt', '-rn', '-M', str(target), '-o', 'SOURCE'], capture_output=True, text=True, timeout=10)
    if mounted.returncode == 0:
        if Path(mounted.stdout.strip()).resolve() != Path(device):
            raise gcp_runtime.RuntimeSafetyError('run data mount belongs to another device')
        return
    if mounted.returncode != 1:
        raise gcp_runtime.RuntimeSafetyError('could not inspect run data mount')
    signatures = subprocess.check_output(['wipefs', '--noheadings', '--output', 'TYPE', device], text=True, timeout=10).strip()
    if signatures:
        raise gcp_runtime.RuntimeSafetyError('refusing to format a nonblank local SSD')
    subprocess.run(['mkfs.ext4', '-F', device], check=True, timeout=120)
    subprocess.run(['mount', device, str(target)], check=True, timeout=30)


def _inside(path, root):
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
        return True
    except (ValueError, OSError):
        return False


def admit_command(config, request):
    argv = request.get('argv')
    if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or '\x00' in x for x in argv):
        raise gcp_runtime.RuntimeSafetyError('invalid command')
    root = Path(config['package_root'])
    if argv[0] == '/usr/bin/python3':
        if len(argv) < 2 or not argv[1].endswith('.py') or not _inside(Path(argv[1]), root / 'source'):
            raise gcp_runtime.RuntimeSafetyError('Python script must be from admitted source')
    elif not _inside(Path(argv[0]), root / 'runtime' / 'bin'):
        raise gcp_runtime.RuntimeSafetyError('executable must be from admitted runtime')
    cwd = request.get('cwd')
    if cwd is not None and not _inside(Path(cwd), Path('/mnt/tidehunter') / config['run_id']):
        raise gcp_runtime.RuntimeSafetyError('working directory outside run root')
    env = request.get('env')
    if not isinstance(env, dict) or any(not re.fullmatch(r'[A-Z][A-Z0-9_]*', k) or not isinstance(v, str)
                                        or '\x00' in v or k.startswith(('LD_', 'PYTHON', 'BASH_', 'GCONV_', 'PERL', 'RUBY'))
                                        or k in ('PATH', 'IFS', 'ENV', 'SHELLOPTS', 'CDPATH')
                                        for k, v in env.items()):
        raise gcp_runtime.RuntimeSafetyError('unsafe command environment')
    return argv, cwd, env


def run_request(config, host, store, key, generation, gate_root):
    raw = store.get(key, generation=generation, max_bytes=4 * 1024 * 1024)
    request = json.loads(raw)
    op = request.get('operation_id')
    expected = f'runs/{config["run_id"]}/mailbox/{host["id"]}/requests/{op}.json'
    if (not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', op) or key != expected
        or request.get('schema_version') != 1 or request.get('run_id') != config['run_id']
        or request.get('source_sha256') != config['source_sha256'] or request.get('instance_id') != str(host['id'])
        or request.get('index') != host['index'] or request.get('deadline_utc') != config['deadline_utc']):
        raise gcp_runtime.RuntimeSafetyError('request identity mismatch')
    deadline = gcp_runtime._deadline(config['deadline_utc'])
    timeout = request.get('timeout_seconds')
    if type(timeout) is not int or timeout < 1 or time.time() >= deadline:
        raise gcp_runtime.RuntimeSafetyError('expired or invalid command timeout')
    argv, cwd, env = admit_command(config, request)
    request_sha = hashlib.sha256(raw).hexdigest()

    def execute():
        remaining = min(timeout, deadline - time.time())
        if remaining <= 0:
            raise gcp_runtime.RuntimeSafetyError('command expired before start')
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            process = subprocess.Popen(argv, cwd=cwd, env={**os.environ, **env, 'PYTHONDONTWRITEBYTECODE': '1'}, stdout=stdout,
                                       stderr=stderr, start_new_session=True)
            try:
                code = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                code = 124
            stdout.seek(0)
            stderr.seek(0)
            out, err = stdout.read(MAX_OUTPUT + 1), stderr.read(MAX_OUTPUT + 1)
        if len(out) > MAX_OUTPUT or len(err) > MAX_OUTPUT:
            raise gcp_runtime.RuntimeSafetyError('command output exceeds mailbox bound')
        return {'schema_version': 1, 'operation_id': op, 'instance_id': str(host['id']),
                'source_sha256': config['source_sha256'], 'request_sha256': request_sha,
                'exit_code': code, 'stdout': out.decode('utf-8', 'replace'), 'stderr': err.decode('utf-8', 'replace')}

    result = execute_at_most_once(gate_root, key, request, execute, deadline_epoch=deadline)
    response_key = key.replace('/requests/', '/responses/')
    gcp_runtime._put_once(store, response_key, gcp_runtime._encode(result))
    return result


def serve(config, *, api=None, poll_seconds=1):
    gcp_runtime.validate_runtime(config)
    host = identify(config)
    admission = cloud_package.verify_directory(Path(config['package_root']), runtime=True, provider='gcp')
    if admission['source_sha256'] != config['source_sha256']:
        raise gcp_runtime.RuntimeSafetyError('sealed source identity mismatch')
    api = api or gcp_api.GoogleApi(config['project_id'], token_provider=gcp_api.metadata_token)
    gcp_runtime.verify_fleet({'platform': {'runtime': config}}, {'platform': api})
    prepare_storage(config)
    store = api.objects(config['bucket'])
    gate_root = Path('/var/lib/tidehunter-gcp-control') / config['run_id']
    gate_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    prefix = f'runs/{config["run_id"]}/mailbox/{host["id"]}/requests/'
    active, completed = {}, set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        while time.time() < gcp_runtime._deadline(config['deadline_utc']):
            for key, future in list(active.items()):
                if future.done():
                    try:
                        future.result()
                        completed.add(key)
                    except Exception as exc:
                        print(f'worker operation {key}: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
                    del active[key]
            for item in store.list(prefix):
                key = item.get('name')
                if isinstance(key, str) and key.startswith(prefix) and key.endswith('.json') and key not in active and key not in completed:
                    active[key] = pool.submit(run_request, config, host, store, key, item['generation'], gate_root)
            time.sleep(poll_seconds)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: gcp_worker.py RUNTIME_CONFIG')
    with open(sys.argv[1], encoding='utf-8') as stream:
        serve(json.load(stream))
