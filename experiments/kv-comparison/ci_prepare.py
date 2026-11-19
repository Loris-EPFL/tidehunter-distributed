#!/usr/bin/env python3
"""Bound package preparation and admit same-build API qualification evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import time

import cloud_package
BACKENDS = {'native', 'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'garnet', 'kvrocks'}


def sha(path):
    cloud_package.regular(Path(path))
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def proof_inventory(directory):
    files = {}
    total = 0
    for path in sorted(Path(directory).rglob('*')):
        if path.is_dir() and not path.is_symlink():
            continue
        cloud_package.regular(path)
        name = path.relative_to(directory).as_posix()
        cloud_package.safe_name(name)
        total += path.stat().st_size
        if len(files) >= 2000 or total > 128 << 20 or path.stat().st_size > 32 << 20:
            raise ValueError('qualification evidence exceeds bounded inventory')
        files[name] = sha(path)
    return files


def admit_qualification(directory, ready, *, focused=False):
    if type(focused) is not bool:
        raise ValueError('invalid qualification profile')
    required_backends = {'native'} if focused else BACKENDS
    if proof_inventory(directory) != ready.get('qualification_files'):
        raise ValueError('same-build qualification evidence inventory differs')
    receipts = sorted(Path(directory).glob('*/receipt.json'))
    found = set()
    for path in receipts:
        receipt = json.loads(path.read_text())
        backend = receipt.get('job', {}).get('backend')
        if (backend not in required_backends or backend in found or receipt.get('status') != 'completed'
                or receipt.get('cleanup_errors') or receipt.get('binary_sha256') != ready['binary_sha256']
                or receipt.get('claim_boundary') != 'API qualification only'):
            raise ValueError('complete same-binary real API qualification required for every software backend')
        found.add(backend)
        config = json.loads((path.parent / 'normalized-config.json').read_text())
        if config.get('qualification_only') is not True or config.get('source_revision') != ready['engine_sha']:
            raise ValueError('qualification source/config identity differs')
        for name, digest in receipt.get('artifacts_sha256', {}).items():
            if name not in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom') or sha(path.parent / 'result' / name) != digest:
                raise ValueError('qualification raw artifact differs')
        if set(receipt.get('artifacts_sha256', {})) != {'manifest.json', 'summary.json', 'samples.csv', 'metrics.prom'}:
            raise ValueError('qualification raw artifacts incomplete')
    if found != required_backends:
        raise ValueError('qualification must cover every software-admissible adapter; RDMA hardware remains separate')


def bounded(command, deadline, *, environment=None, cwd=None):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('absolute KV preparation deadline exhausted; fleet must not launch')
    subprocess.run([str(value) for value in command], check=True, timeout=remaining,
                   cwd=cwd, env=environment)
