#!/usr/bin/env python3
"""One rehearsal guest: the real `gcp_worker.serve`, with the cloud identity stubbed.

The container has no GCE metadata server, no local SSD and no Compute API, so
three guest-local functions differ from a real guest: `metadata` reads the
rehearsal identity file, `verify_fleet` admits that identity against the runtime
fleet, and `prepare_storage` uses the container filesystem. Everything else is
the real worker: gate checks, package admission, request admission, at-most-once
execution, transfers, receipts and the poweroff decision log.

The bootstrap mirrors `gcp_bootstrap.startup_script` for the steps a container
can perform: wait for the identity-bound gate, verify its checksum, fetch and
unpack the sealed package, verify `source.zip` and write `runtime.json`. The
GCE-only steps (systemd expiry unit, bind mount) are absent.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

SRC = Path('/rehearsal/src')
for relative in ('', 'experiments/gcp-rdma', 'experiments/kv-comparison',
                 'experiments/sui-checkpoint'):
    sys.path.insert(0, str(SRC / relative) if relative else str(SRC))
sys.dont_write_bytecode = True

MAX_PACKAGE = 512 * 1024 * 1024
MAX_SOURCE = 256 * 1024 * 1024


def configuration():
    return json.loads(Path(os.environ['REHEARSAL_IDENTITY']).read_text())


def metadata(path):
    """The stubbed GCE metadata identity of this guest."""
    data = configuration()
    table = {'project/project-id': data['project_id'],
             'project/numeric-project-id': data['project_number'],
             'instance/id': str(data['id']),
             'instance/name': data['name'],
             'instance/zone': data['zone']}
    if path not in table:
        raise ValueError('the rehearsal metadata has no ' + path)
    return table[path]


def verify_fleet(config, clients, **kwargs):
    """Local stand-in for the GCE describe: the stubbed identity must match."""
    runtime = config['platform']['runtime']
    host = configuration()
    matches = [item for item in runtime['fleet'] if str(item['id']) == str(host['id'])]
    if len(matches) != 1:
        raise ValueError('the rehearsal identity is not in the runtime fleet')
    item = matches[0]
    if any(item[key] != host[key] for key in ('name', 'zone', 'internal_ip', 'rdma_ip')):
        raise ValueError('the rehearsal identity differs from the runtime fleet')
    return {item['name']: dict(item, status='RUNNING')}


def prepare_storage(config, host, api):
    """The container filesystem stands in for the admitted local SSD."""
    target = Path('/mnt/tidehunter-kv-comparison') / config['run_id']
    target.mkdir(parents=True, exist_ok=True)
    return {'device': '(rehearsal container filesystem)', 'mkfs_duration_seconds': None,
            'lazy_initialization_disabled': None,
            'reason': 'the rehearsal guest has no GCE local SSD'}


def fetch(store, key, generation=None, limit=MAX_PACKAGE):
    meta = store.head(key) if generation is None else {'generation': generation}
    data = store.get(key, generation=meta['generation'], max_bytes=limit)
    return data


def wait_gate(store, prefix, identity, deadline):
    """Wait for one identity-bound gate and verify its script checksum."""
    key = prefix + 'gate.json'
    while time.time() < deadline:
        try:
            meta = store.head(key)
            gate = json.loads(store.get(key, generation=meta['generation'],
                                        max_bytes=1024 * 1024))
        except Exception as exc:  # a missing object is the normal case here
            if getattr(exc, 'status', None) != 404:
                raise
            time.sleep(1)
            continue
        if (gate.get('run_id') != identity['run_id']
                or gate.get('source_sha256') != identity['source_sha256']
                or str(gate.get('instance_id')) != str(identity['id'])
                or gate.get('deadline_utc') != identity['deadline_utc']):
            raise ValueError('gate identity mismatch')
        script = gate.get('script')
        if (not isinstance(script, str)
                or hashlib.sha256(script.encode()).hexdigest() != gate.get('sha256')):
            raise ValueError('gate checksum mismatch')
        # The real gate verifies the identity, reads this script's object and
        # executes it. Its body must be the bootstrap that starts the worker.
        if 'gate.json' not in script or 'exec' not in script:
            raise ValueError('gate script does not release the bootstrap')
        return gate
    raise TimeoutError('gate was not released before the deadline')


def install_package(store, identity, root):
    """The container-runnable part of the real bootstrap."""
    prefix = 'runs/' + identity['run_id'] + '/'
    runtime = json.loads(fetch(store, prefix + 'runtime.json', limit=4 * 1024 * 1024))
    package = json.loads(fetch(store, prefix + 'package.json', limit=4 * 1024 * 1024))
    if (runtime.get('run_id') != identity['run_id']
            or runtime.get('source_sha256') != identity['source_sha256']
            or runtime.get('deadline_utc') != identity['deadline_utc']
            or runtime.get('package') != package
            or package.get('object_key') != prefix + 'package.tar'):
        raise ValueError('runtime/package binding mismatch')
    raw = fetch(store, package['object_key'], package['generation'])
    if hashlib.sha256(raw).hexdigest() != package['sha256']:
        raise ValueError('package checksum mismatch')
    target = root / 'package'
    if target.exists():
        raise ValueError('package target already exists')
    target.mkdir(mode=0o700)
    archive = root / 'package.tar'
    archive.write_bytes(raw)
    del raw
    with tarfile.open(archive, 'r:gz') as tar:
        names, size = set(), 0
        for member in tar:
            path = Path(member.name)
            size += member.size
            if (path.is_absolute() or '..' in path.parts or not member.isfile()
                    or member.name in names or size > MAX_PACKAGE or len(names) >= 20000):
                raise ValueError('unsafe package archive')
            names.add(member.name)
            destination = target.joinpath(*path.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(member) as source, destination.open('xb') as output:
                shutil.copyfileobj(source, output)
            destination.chmod(0o755 if member.mode & 0o111 else 0o644)
    if hashlib.sha256((target / 'source.zip').read_bytes()).hexdigest() != identity['source_sha256']:
        raise ValueError('source snapshot checksum mismatch')
    with zipfile.ZipFile(target / 'source.zip') as source:
        entries = source.infolist()
        if len(entries) > 20000 or sum(entry.file_size for entry in entries) > MAX_SOURCE:
            raise ValueError('source archive exceeds bound')
        names = set()
        for entry in entries:
            path = Path(entry.filename)
            if (path.is_absolute() or '..' in path.parts or entry.filename in names
                    or entry.file_size > 16 * 1024 * 1024):
                raise ValueError('unsafe source entry')
            names.add(entry.filename)
            installed = (target / 'source').joinpath(*path.parts).read_bytes()
            if hashlib.sha256(source.read(entry)).digest() != hashlib.sha256(installed).digest():
                raise ValueError('installed source differs from admitted snapshot')
        actual = {path.relative_to(target / 'source').as_posix()
                  for path in (target / 'source').rglob('*') if not path.is_dir()}
        if actual != names:
            raise ValueError('source inventory mismatch')
    config_path = root / 'runtime.json'
    with config_path.open('x') as output:
        json.dump(runtime, output, sort_keys=True)
    config_path.chmod(0o600)
    return runtime


def main():
    import gcp_api
    import gcp_runtime
    import gcp_worker

    identity = json.loads(Path(os.environ['REHEARSAL_IDENTITY']).read_text())
    endpoint = os.environ['TIDEHUNTER_REHEARSAL_STORE']
    gcp_worker.metadata = metadata
    gcp_runtime.verify_fleet = verify_fleet
    gcp_worker.prepare_storage = prepare_storage
    api = gcp_api.GoogleApi(identity['project_id'], token_provider=lambda: 'rehearsal',
                            endpoint=endpoint)
    store = api.objects(identity['bucket'])
    root = Path('/mnt/tidehunter') / identity['run_id']
    root.mkdir(parents=True, exist_ok=True)
    prefix = 'runs/' + identity['run_id'] + '/qualification/' + identity['name'] + '/'
    deadline = gcp_runtime._deadline(identity['deadline_utc'])
    wait_gate(store, prefix, identity, deadline)
    runtime = install_package(store, identity, root)
    print('rehearsal guest ' + identity['name'] + ': gate released, package installed',
          flush=True)
    gcp_worker.serve(runtime, api=api)


if __name__ == '__main__':
    main()
