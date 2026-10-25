#!/usr/bin/env python3
"""Build and qualify current committed KV sources without AWS credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

import cloud_package

ROOT = Path(__file__).resolve().parents[2]
MAX_ARCHIVE = 768 << 20
MAX_EXPANDED = 1 << 30
MAX_FILES = 22000
BACKENDS = {'native', 'tidehunter_tcp', 'tikv', 'cassandra', 'foundationdb'}


def sha(path):
    cloud_package.regular(Path(path))
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def trusted_checkout(engine, expected=None):
    environment = os.environ
    if (environment.get('GITHUB_REPOSITORY') != 'Loris-EPFL/tidehunter-distributed'
            or environment.get('GITHUB_REF') != 'refs/heads/bench/aws-efa'
            or environment.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch')):
        raise ValueError('KV preparation requires the protected repository and benchmark branch')
    expected = expected or environment.get('GITHUB_SHA', '')
    if not re.fullmatch('[0-9a-f]{40}', expected) or expected != environment.get('GITHUB_SHA'):
        raise ValueError('exact current workflow source commit required')
    actual = cloud_package.command(['git', 'rev-parse', 'HEAD'], cwd=engine)
    status = cloud_package.command(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=engine)
    if actual != expected or status:
        raise ValueError('KV CI requires the exact clean committed workflow checkout')
    return expected


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


def admit_qualification(directory, ready):
    if proof_inventory(directory) != ready.get('qualification_files'):
        raise ValueError('same-build qualification evidence inventory differs')
    receipts = sorted(Path(directory).glob('*/receipt.json'))
    found = set()
    for path in receipts:
        receipt = json.loads(path.read_text())
        backend = receipt.get('job', {}).get('backend')
        if (backend not in BACKENDS or backend in found or receipt.get('status') != 'completed'
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
    if found != BACKENDS:
        raise ValueError('qualification must cover all five software-admissible adapters; EFA hardware remains separate')


def admit_inputs(inputs, engine=None, expected_commit=None):
    """Shared operator admission. No AWS access, build, or database startup."""
    inputs, engine = Path(inputs), Path(engine or ROOT).resolve(strict=True)
    expected = trusted_checkout(engine, expected_commit)
    cloud_package.regular(inputs / 'READY.json')
    ready = json.loads((inputs / 'READY.json').read_text())
    if (ready.get('schema_version') != 1 or ready.get('complete') is not True
            or ready.get('suite') != 'kv-comparison'
            or ready.get('engine_sha') != expected or ready.get('source_commit') != expected
            or ready.get('qualification_complete') is not True
            or sha(inputs / 'bundle.tar.gz') != ready.get('bundle_sha256')):
        raise ValueError('prepared KV inputs are not bound to the current workflow source commit')
    package = inputs / 'package'
    verified = cloud_package.verify_directory(package)
    source = cloud_package.admit_source(package)
    if (source['revision'] != expected or source['dirty'] is not False
            or source['sha256'] != cloud_package.inventory(engine)
            or any(verified[key] != ready.get(key) for key in ('source_sha256', 'binary_sha256'))
            or source['tree_sha256'] != ready.get('source_tree_sha256')
            or verified['manifest_sha256'] != ready.get('manifest_sha256')):
        raise ValueError('prepared runtime/current checkout/source/binary binding differs')
    admit_qualification(inputs / 'qualification', ready)
    return ready


def bounded(command, deadline, *, environment=None, cwd=None):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('absolute KV preparation deadline exhausted; fleet must not launch')
    subprocess.run([str(value) for value in command], check=True, timeout=remaining,
                   cwd=cwd, env=environment)


def prepare(args):
    engine = args.engine.resolve(strict=True)
    expected = trusted_checkout(engine)
    if engine != ROOT:
        raise ValueError('executing KV preparer must belong to the requested engine checkout')
    if os.environ.get('AWS_BENCH_ENABLED') != 'true':
        raise ValueError('AWS benchmark preparation is disarmed')
    if args.jobs != 2 or not 1 <= args.deadline <= 14400:
        raise ValueError('cloud_package uses exactly two jobs; bounded preparation deadline required')
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    deadline = time.monotonic() + args.deadline
    fabric = Path(os.environ['TIDEHUNTER_LIBFABRIC_PREFIX']).resolve(strict=True)
    fdb = output / 'fdb-client'
    bounded([sys.executable, engine / 'experiments/kv-comparison/local.py', 'prepare-fdb', '--prefix', fdb], deadline)
    snapshot = output / 'snapshot'
    cloud_package.snapshot(engine, snapshot)
    build = output / 'build'
    remaining = min(7200, int(deadline - time.monotonic()) - 1200)
    if remaining <= 0:
        raise TimeoutError('KV preparation has no remaining build/qualification reserve')
    cloud_package.build(snapshot, build, fdb, fabric, timeout=remaining)
    cloud_package.stage(engine, snapshot, build, fdb, output / 'package')
    shutil.copy2(output / 'package/bundle.tar.gz', output / 'bundle.tar.gz')
    # campaign.py deliberately replaces LD_LIBRARY_PATH with this admitted client
    # prefix for FDB. Supply the same pinned build-time fabric ABI there as well;
    # this local loader closure is not added to the EFA guest runtime bundle.
    shutil.copy2((fabric / 'lib/libfabric.so.1').resolve(strict=True), fdb / 'lib/libfabric.so.1')
    environment = cloud_package.clean_environment()
    environment.update({'FDB_CLIENT_LIB_DIR': str(fdb / 'lib'), 'FDB_CLIENT_LIB_PATH': str(fdb / 'lib'),
                        'LD_LIBRARY_PATH': f'{fdb}/lib:{fabric}/lib', 'FI_PROVIDER': 'tcp;ofi_rxm'})
    raw = output / 'qualification-raw'
    bounded([sys.executable, engine / 'experiments/kv-comparison/campaign.py', '--manifest',
             engine / 'experiments/kv-comparison/ci.json', '--binary', output / 'package/runtime/bin/kv-research',
             '--qualification', '--output', raw], deadline, environment=environment, cwd=engine)
    proof = output / 'qualification'
    proof.mkdir()
    for receipt in sorted(raw.glob('*/receipt.json')):
        destination = proof / receipt.parent.name
        destination.mkdir()
        for name in ('receipt.json', 'config.json', 'normalized-config.json', 'run.log', 'client-resources.jsonl'):
            shutil.copy2(receipt.parent / name, destination / name)
        (destination / 'result').mkdir()
        for name in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom'):
            shutil.copy2(receipt.parent / 'result' / name, destination / 'result' / name)
    manifest = json.loads((output / 'package/manifest.json').read_text())
    ready = dict(schema_version=1, complete=True, suite='kv-comparison', engine_sha=expected,
                 source_commit=expected, bundle_sha256=sha(output / 'bundle.tar.gz'),
                 source_sha256=manifest['source_sha256'], source_tree_sha256=manifest['source_tree_sha256'],
                 binary_sha256=manifest['binary_sha256'], manifest_sha256=sha(output / 'package/manifest.json'),
                 qualification_complete=True, qualification_files=proof_inventory(proof),
                 claim_boundary='same-release real local software API qualification; no EFA hardware or performance claim')
    cloud_package.dump(output / 'READY.json', ready)
    admit_inputs(output, engine, expected)
    return ready


def package_inputs(inputs, output):
    inputs, output = Path(inputs), Path(output)
    admit_inputs(inputs)
    paths = {'READY.json': inputs / 'READY.json', 'bundle.tar.gz': inputs / 'bundle.tar.gz'}
    paths.update({'qualification/' + name: inputs / 'qualification' / name
                  for name in proof_inventory(inputs / 'qualification')})
    # Only ordinary files cross the trust boundary; no databases or compiler tree.
    with output.open('xb') as target, tarfile.open(fileobj=target, mode='w:gz') as archive:
        for name, path in sorted(paths.items()):
            cloud_package.regular(path)
            archive.add(path, arcname=name, recursive=False)
    if output.stat().st_size > MAX_ARCHIVE:
        raise ValueError('prepared artifact exceeds compressed bound')
    digest = sha(output)
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('package_sha256=' + digest + '\n')
    return {'archive_sha256': digest, 'bytes': output.stat().st_size}


def restore_inputs(archive, expected_sha256, output):
    archive, output = Path(archive), Path(output)
    trusted_checkout(ROOT)
    if (not re.fullmatch('[0-9a-f]{64}', expected_sha256) or archive.stat().st_size > MAX_ARCHIVE
            or sha(archive) != expected_sha256):
        raise ValueError('cross-job KV artifact checksum/size differs')
    with tarfile.open(archive, 'r:gz') as stream:
        members, names, total = [], set(), 0
        for member in stream:
            path = cloud_package.safe_name(member.name)
            total += member.size
            if (not member.isfile() or member.name in names or len(names) >= MAX_FILES
                    or total > MAX_EXPANDED or member.mode & 0o7000
                    or (member.name == 'READY.json' and member.size > 1 << 20)
                    or (member.name == 'bundle.tar.gz' and member.size > cloud_package.MAX_BUNDLE)
                    or (path.parts[0] == 'qualification' and (len(path.parts) < 2 or member.size > 32 << 20))
                    or (member.name not in ('READY.json', 'bundle.tar.gz') and path.parts[0] != 'qualification')):
                raise ValueError('unsafe/duplicate/oversized KV preparation archive member')
            names.add(member.name)
            members.append(member)
        if not {'READY.json', 'bundle.tar.gz'} <= names:
            raise ValueError('KV artifact missing readiness or runtime bundle')
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
        for member in members:
            target = output / member.name
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with stream.extractfile(member) as source, target.open('xb') as destination:
                shutil.copyfileobj(source, destination)
            target.chmod(member.mode & 0o777)
    ready = json.loads((output / 'READY.json').read_text())
    cloud_package.verify_archive(output / 'bundle.tar.gz', ready['bundle_sha256'], output / 'package')
    return admit_inputs(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    build = commands.add_parser('build')
    build.add_argument('--engine', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    build.add_argument('--deadline', type=int, default=10800)
    build.add_argument('--jobs', type=int, default=2)
    package = commands.add_parser('package')
    package.add_argument('--inputs', type=Path, required=True)
    package.add_argument('--output', type=Path, required=True)
    restore = commands.add_parser('restore')
    restore.add_argument('--archive', type=Path, required=True)
    restore.add_argument('--sha256', required=True)
    restore.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args) if args.action == 'build' else (package_inputs(args.inputs, args.output)
             if args.action == 'package' else restore_inputs(args.archive, args.sha256, args.output))
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
