"""Generate a self-contained, source-bound GCE startup script."""
import json
import re
import shlex

import gcp_runtime


def startup_script(bucket, run_id, source_sha256, deadline_utc):
    if not isinstance(bucket, str) or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]', bucket):
        raise ValueError('invalid bucket')
    gcp_runtime._run(run_id)
    gcp_runtime._sha(source_sha256)
    gcp_runtime._deadline(deadline_utc)
    constants = json.dumps({'bucket': bucket, 'run_id': run_id, 'source_sha256': source_sha256,
                            'deadline_utc': deadline_utc, 'guest_python': gcp_runtime.GUEST_PYTHON,
                            'guest_path': gcp_runtime.GUEST_PATH}, sort_keys=True)
    return '''#!/bin/bash
set -euo pipefail
export PATH=''' + shlex.quote(gcp_runtime.GUEST_PATH) + '''
exec ''' + shlex.quote(gcp_runtime.GUEST_PYTHON) + ''' - <<'TIDEHUNTER_BOOTSTRAP'
''' + 'constants = ' + repr(constants) + '''
import datetime, hashlib, json, os, pathlib, shutil, subprocess, tarfile, time, urllib.error, urllib.parse, urllib.request, zipfile
c = json.loads(constants)
deadline = datetime.datetime.fromisoformat(c['deadline_utc'].replace('Z', '+00:00')).timestamp()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def metadata(path):
    req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path, headers={'Metadata-Flavor':'Google'})
    with opener.open(req, timeout=5) as response:
        if response.headers.get('Metadata-Flavor') != 'Google': raise ValueError('metadata flavor')
        return response.read(16384).decode().strip()
if (metadata('instance/attributes/tidehunter-run-id') != c['run_id'] or
    metadata('instance/attributes/tidehunter-source-sha256') != c['source_sha256'] or
    metadata('instance/attributes/tidehunter-deadline-utc') != c['deadline_utc']):
    raise ValueError('startup metadata mismatch')
if time.time() >= deadline: raise ValueError('startup expired')
# Stop this run's control worker; GCE DELETE bounds the VM and separate workload units.
subprocess.run(['/usr/bin/systemd-run', '--unit=tidehunter-expire-' + c['run_id'],
                '--on-calendar=' + datetime.datetime.fromtimestamp(deadline, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                '/usr/bin/systemctl', 'stop', 'tidehunter-worker-' + c['run_id'] + '.service'], check=True, timeout=30)
def get(key, generation=None, limit=536870912):
    token = json.loads(metadata('instance/service-accounts/default/token'))['access_token']
    url = 'https://storage.googleapis.com/storage/v1/b/' + urllib.parse.quote(c['bucket'], safe='') + '/o/' + urllib.parse.quote(key, safe='') + '?alt=media'
    if generation is not None: url += '&generation=' + urllib.parse.quote(str(generation), safe='')
    req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
    with opener.open(req, timeout=60) as response:
        data = response.read(limit + 1)
    if len(data) > limit: raise ValueError('oversized object')
    return data
def wait(key):
    while time.time() < deadline:
        try: return json.loads(get(key, limit=4194304))
        except urllib.error.HTTPError as e:
            if e.code != 404: raise
            time.sleep(2)
    raise TimeoutError('startup objects missing')
base = 'runs/' + c['run_id'] + '/'
runtime, package = wait(base + 'runtime.json'), wait(base + 'package.json')
if (runtime['run_id'] != c['run_id'] or runtime['source_sha256'] != c['source_sha256'] or
    runtime['deadline_utc'] != c['deadline_utc'] or runtime['bucket'] != c['bucket'] or
    runtime['package'] != package or package['source_sha256'] != c['source_sha256'] or
    package['object_key'] != base + 'package.tar'):
    raise ValueError('runtime/package binding mismatch')
raw = get(package['object_key'], package['generation'])
if hashlib.sha256(raw).hexdigest() != package['sha256']: raise ValueError('package checksum mismatch')
root = pathlib.Path('/mnt/tidehunter') / c['run_id']
root.mkdir(mode=0o700, parents=True, exist_ok=True)
target = root / 'package'
if target.exists(): raise ValueError('package target already exists')
target.mkdir(mode=0o700)
archive = root / 'package.tar'
with archive.open('xb') as f: f.write(raw)
with tarfile.open(archive, 'r:gz') as tar:
    names, size = set(), 0
    for member in tar:
        p = pathlib.PurePosixPath(member.name)
        size += member.size
        if (p.is_absolute() or '..' in p.parts or not member.isfile() or member.name in names or
            size > 536870912 or len(names) >= 20000): raise ValueError('unsafe package archive')
        names.add(member.name)
        path = target.joinpath(*p.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tar.extractfile(member) as source, path.open('xb') as destination: shutil.copyfileobj(source, destination)
        path.chmod(0o755 if member.mode & 0o111 else 0o644)
if hashlib.sha256((target / 'source.zip').read_bytes()).hexdigest() != c['source_sha256']:
    raise ValueError('source snapshot checksum mismatch')
with zipfile.ZipFile(target / 'source.zip') as source:
    entries = source.infolist()
    if len(entries) > 20000 or sum(e.file_size for e in entries) > 268435456:
        raise ValueError('source archive exceeds bound')
    names = set()
    for entry in entries:
        p = pathlib.PurePosixPath(entry.filename)
        if p.is_absolute() or '..' in p.parts or entry.filename in names or entry.file_size > 16777216:
            raise ValueError('unsafe source entry')
        names.add(entry.filename)
        if hashlib.sha256(source.read(entry)).digest() != hashlib.sha256((target / 'source').joinpath(*p.parts).read_bytes()).digest():
            raise ValueError('installed source differs from admitted snapshot')
    actual = {p.relative_to(target / 'source').as_posix() for p in (target / 'source').rglob('*') if not p.is_dir()}
    if actual != names: raise ValueError('source inventory mismatch')
subprocess.run(['mount', '--bind', str(target), str(target)], check=True, timeout=30)
subprocess.run(['mount', '-o', 'remount,bind,ro', str(target)], check=True, timeout=30)
config_path = root / 'runtime.json'
with config_path.open('x') as f: json.dump(runtime, f, sort_keys=True)
os.chmod(config_path, 0o600)
worker = target / 'source/experiments/gcp-rdma/gcp_worker.py'
remaining = int(deadline - time.time())
if remaining <= 0: raise TimeoutError('worker startup expired')
subprocess.run(['/usr/bin/systemd-run', '--unit=tidehunter-worker-' + c['run_id'],
                '--setenv=PATH=' + c['guest_path'],
                '--property=Restart=on-failure', '--property=RestartSec=3', '--property=KillMode=control-group',
                '--property=LimitMEMLOCK=infinity', '--property=RuntimeMaxSec=' + str(remaining),
                '--property=TimeoutStopSec=1',
                c['guest_python'], str(worker), str(config_path)], check=True, timeout=30)
TIDEHUNTER_BOOTSTRAP
'''
