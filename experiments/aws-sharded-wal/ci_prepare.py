#!/usr/bin/env python3
"""Prepare current committed sources and local proof before any paid allocation."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'
FIXTURE_SHA256 = '7eb43b71435547939760598e541d13b9a77f722c5a9d0f8c8ee5bd583b751f15'
TIP = 1775
CHECKPOINT = 'EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def trusted_environment(env):
    if (env.get('GITHUB_REPOSITORY') != REPOSITORY
            or env.get('GITHUB_REF') != 'refs/heads/' + BRANCH
            or env.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch')):
        raise ValueError('AWS benchmark preparation requires the trusted repository and benchmark branch')
    if not re.fullmatch(r'[0-9a-f]{40}', env.get('GITHUB_SHA', '')):
        raise ValueError('exact engine commit SHA required')


def settings(env):
    trusted_environment(env)
    if env.get('AWS_BENCH_ENABLED') != 'true':
        return {'enabled': False, 'reason': 'AWS benchmarks are disarmed; no AWS operation or build requested'}
    required = ['AWS_BENCH_ROLE_ARN', 'AWS_BENCH_BUCKET', 'AWS_BENCH_BUCKET_REGION']
    if env.get('AWS_BENCH_OPERATION', 'benchmark') != 'cost-report':
        required += ['AWS_BENCH_INPUT_ROLE_ARN', 'AWS_BENCH_GUEST_BOUNDARY_ARN',
                     'AWS_BENCH_WATCHDOG_FUNCTION', 'AWS_BENCH_WATCHDOG_RULE', 'AWS_BENCH_SUI_SHA']
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise ValueError('missing GitHub environment variables: ' + ', '.join(missing))
    if env.get('AWS_BENCH_OPERATION', 'benchmark') != 'cost-report' and not re.fullmatch(r'[0-9a-f]{40}', env['AWS_BENCH_SUI_SHA']):
        raise ValueError('AWS_BENCH_SUI_SHA must be a reviewed full commit SHA, not a moving branch')
    from ci_budget import load_budget
    budget = load_budget()
    return {'enabled': True, 'engine_sha': env['GITHUB_SHA'], 'sui_sha': env.get('AWS_BENCH_SUI_SHA'),
            'budget_policy_sha256': budget['sha256']}


def clean_checkout(path, expected):
    actual = subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], text=True, timeout=20).strip()
    if actual != expected:
        raise ValueError('checkout commit differs from requested SHA: ' + str(path))
    status = subprocess.check_output(['git', '-C', str(path), 'status', '--porcelain', '--untracked-files=all'], text=True, timeout=30)
    if status:
        raise ValueError('CI build requires a clean committed checkout: ' + str(path))


def unpack_fixture(archive, output, expected=FIXTURE_SHA256):
    archive, output = Path(archive), Path(output)
    if expected != FIXTURE_SHA256 or sha(archive) != expected:
        raise ValueError('frozen real checkpoint fixture archive SHA-256 differs')
    if archive.stat().st_size > 1 << 30:
        raise ValueError('fixture archive exceeds compressed size bound')
    with tarfile.open(archive, 'r:gz') as stream:
        members = stream.getmembers()
        names = set()
        expanded = 0
        for member in members:
            path = Path(member.name)
            if (path.is_absolute() or '..' in path.parts or not path.parts
                    or path.parts[0] != 'fixture' or member.name in names
                    or not (member.isfile() or member.isdir())):
                raise ValueError('unsafe or duplicate fixture archive member')
            names.add(member.name)
            expanded += member.size
            if len(names) > 100000 or expanded > 8 << 30:
                raise ValueError('expanded fixture exceeds admission bound')
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
        stream.extractall(output, members=members, filter='data')
    fixture = output / 'fixture'
    manifest = json.loads((fixture / 'fixture-manifest.json').read_text())
    prior = json.loads((fixture / 'experiment/results.json').read_text())
    if (manifest.get('checkpoint_limit') != TIP or manifest.get('checkpoint_digest') != CHECKPOINT
            or not prior.get('source_frozen') or prior.get('failure')
            or prior.get('checkpoint_limit') != TIP or prior.get('checkpoint_digest') != CHECKPOINT
            or manifest.get('source_range') != prior.get('verified_source_range')):
        raise ValueError('fixture is not the complete frozen genesis-through-1775 chain')
    actual = {str(p.relative_to(fixture)) for p in fixture.rglob('*') if p.is_file()}
    if actual != set(manifest['files']) | {'fixture-manifest.json'}:
        raise ValueError('portable fixture inventory differs')
    for relative, digest in manifest['files'].items():
        path = fixture / relative
        if not path.resolve().is_relative_to(fixture.resolve()) or path.is_symlink() or sha(path) != digest:
            raise ValueError('fixture file identity differs: ' + relative)
    return fixture


def verification_bundle(source, destination):
    source, destination = Path(source), Path(destination)
    receipt = json.loads((source / 'receipt.json').read_text())
    result = json.loads((source / 'experiment/results.json').read_text())
    audit = json.loads((source / 'experiment/audit.json').read_text())
    if (receipt.get('complete') is not True or result.get('complete') is not True
            or result.get('failure') or audit != result.get('audit') or audit.get('all_equal') is not True
            or not audit.get('databases') or not audit.get('rocksdb')
            or 'loopback' not in result.get('topology', '') or result.get('external_manifest')
            or sha(source / 'evidence.tar.gz') != receipt.get('evidence_sha256')):
        raise ValueError('complete same-build loopback replay and full-state audit required')
    schema = source / 'experiment/native-schema'
    if not schema.is_dir() or not any(p.is_file() for p in schema.rglob('*')):
        raise ValueError('local replay did not retain native namespace schemas')
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    (destination / 'experiment').mkdir()
    for name in ('receipt.json', 'evidence.tar.gz'):
        shutil.copy2(source / name, destination / name)
    for name in ('results.json', 'audit.json'):
        shutil.copy2(source / 'experiment' / name, destination / 'experiment' / name)
    shutil.copytree(schema, destination / 'experiment/native-schema')
    return destination


def prepare(args):
    config = settings(os.environ)
    if not config['enabled']:
        raise ValueError(config['reason'])
    engine, sui = args.engine.resolve(strict=True), args.sui.resolve(strict=True)
    clean_checkout(engine, config['engine_sha'])
    clean_checkout(sui, config['sui_sha'])
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    if shutil.disk_usage(output).free < args.minimum_free_gib * (1 << 30):
        raise ValueError('insufficient build disk space; configure an adequately sized GitHub build runner')
    fixture = unpack_fixture(args.fixture, output / 'fixture-input')
    script = engine / 'experiments/sui-checkpoint/build.py'
    spec = importlib.util.spec_from_file_location('ci_checkpoint_build', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    deadline = time.monotonic() + args.deadline
    build = output / 'build'
    target = args.target.resolve()
    replay = output / 'local-replay'
    commands = [
        [sys.executable, str(script), 'prepare', '--sui', str(sui), '--engine', str(engine), '--output', str(build), '--fabric', '--jobs', str(args.jobs)],
        [sys.executable, str(script), 'build', '--output', str(build), '--target', str(target), '--deadline', str(args.deadline)],
        [sys.executable, str(engine / 'experiments/sui-checkpoint/run.py'), '--build', str(build), '--fixture', str(fixture), '--output', str(replay), '--deadline', '900'],
    ]
    for command in commands:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('absolute CPU build/local-verification deadline exhausted; fleet must not launch')
        module.bounded(command, timeout=remaining)
    verification = verification_bundle(replay, output / 'verification')
    manifest = json.loads((build / 'build-manifest.json').read_text())
    if manifest.get('fabric') is not True or manifest.get('protocol') != 8:
        raise ValueError('current fabric-capable protocol build required')
    save(output / 'READY.json', {
        'complete': True, 'created_utc': datetime.now(timezone.utc).isoformat(),
        'engine_sha': config['engine_sha'], 'sui_sha': config['sui_sha'],
        'build': str(build), 'fixture': str(args.fixture.resolve()), 'verification': str(verification),
        'build_manifest_sha256': sha(build / 'build-manifest.json'),
        'source_archive_sha256': manifest['source_archive_sha256'],
        'source_manifest_sha256': manifest['source_manifest_sha256'],
        'binaries': manifest['binaries'],
        'topology': 'same-build five-process local TCP correctness proof; not hardware performance evidence',
    })
    print(output / 'READY.json')


def package_inputs(inputs, fixture, output):
    """Preserve executable modes and source links across isolated GitHub jobs."""
    inputs, fixture, output = Path(inputs), Path(fixture), Path(output)
    ready = json.loads((inputs / 'READY.json').read_text())
    if (ready.get('complete') is not True or sha(fixture) != FIXTURE_SHA256
            or sha(inputs / 'build/build-manifest.json') != ready['build_manifest_sha256']):
        raise ValueError('deployment package requires a complete current build and exact fixture')
    with output.open('xb') as target, tarfile.open(fileobj=target, mode='w:gz') as archive:
        for path in sorted((inputs / 'build').iterdir()):
            # The immutable source archive is already present. Do not transfer
            # the expanded Cargo source workspace a second time.
            if path.name != 'sources':
                archive.add(path, arcname='build/' + path.name)
        archive.add(inputs / 'verification', arcname='verification')
        archive.add(fixture, arcname='fixture.tar.gz')
    digest = sha(output)
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('package_sha256=' + digest + '\n')
    return {'archive_sha256': digest, 'bytes': output.stat().st_size}


def restore_inputs(archive, expected_sha256, output):
    if not re.fullmatch(r'[0-9a-f]{64}', expected_sha256) or sha(archive) != expected_sha256:
        raise ValueError('cross-job deployment artifact SHA-256 differs')
    with tarfile.open(archive) as stream:
        if any(not Path(member.name).parts or Path(member.name).parts[0] not in
               ('build', 'verification', 'fixture.tar.gz')
               or Path(member.name).parts[:2] == ('build', 'sources')
               for member in stream.getmembers()):
            raise ValueError('unexpected deployment artifact root or expanded sources')
    from sui_checkpoint import safe_extract
    safe_extract(archive, output, symlinks=True, max_bytes=16 << 30)
    build = Path(output) / 'build'
    manifest = json.loads((build / 'build-manifest.json').read_text())
    for name, key in [('sources.tar.gz', 'source_archive_sha256'),
                      ('source-manifest.json', 'source_manifest_sha256')]:
        path = build / name
        if path.is_symlink() or not path.is_file() or sha(path) != manifest[key]:
            raise ValueError('source archive/manifest binding differs')
    identities = json.loads((build / 'source-manifest.json').read_text())
    source_unpack = Path(output) / 'source-unpack'
    safe_extract(build / 'sources.tar.gz', source_unpack, symlinks=True)
    if {path.name for path in source_unpack.iterdir()} != {'sources'}:
        raise ValueError('source archive must contain only the frozen sources')
    sources = source_unpack / 'sources'
    if sources.is_symlink() or not sources.is_dir() or any(
            path.is_symlink() and not path.resolve().is_relative_to(sources.resolve())
            for path in sources.rglob('*')):
        raise ValueError('source symlink escapes frozen sources')
    spec = importlib.util.spec_from_file_location(
        'ci_restore_checkpoint_build', ROOT / 'experiments/sui-checkpoint/build.py')
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    builder.verify_staged(source_unpack, identities)
    (source_unpack / 'sources').rename(build / 'sources')
    source_unpack.rmdir()
    return {'restored': True, 'archive_sha256': expected_sha256,
            'build_manifest_sha256': sha(Path(output) / 'build/build-manifest.json')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('settings')
    unpack = sub.add_parser('fixture')
    unpack.add_argument('--fixture', type=Path, required=True)
    unpack.add_argument('--output', type=Path, required=True)
    package = sub.add_parser('package')
    package.add_argument('--inputs', type=Path, required=True)
    package.add_argument('--fixture', type=Path, required=True)
    package.add_argument('--output', type=Path, required=True)
    restore = sub.add_parser('restore')
    restore.add_argument('--archive', type=Path, required=True)
    restore.add_argument('--sha256', required=True)
    restore.add_argument('--output', type=Path, required=True)
    build = sub.add_parser('build')
    build.add_argument('--engine', type=Path, default=ROOT)
    build.add_argument('--sui', type=Path, required=True)
    build.add_argument('--fixture', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    build.add_argument('--target', type=Path, required=True)
    build.add_argument('--deadline', type=int, default=10800)
    build.add_argument('--jobs', type=int, default=2)
    build.add_argument('--minimum-free-gib', type=int, default=45)
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise InterruptedError('CI preparation interrupted; stopping owned build/replay descendants')
    signal.signal(signal.SIGTERM, interrupted)
    if args.action == 'settings':
        value = settings(os.environ)
        print(json.dumps(value))
        if os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
                stream.write('enabled=' + str(value['enabled']).lower() + '\n')
        if not value['enabled'] and os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
                stream.write('## AWS benchmarks disarmed\n\nNo AWS calls, build or fleet launch requested.\n')
    elif args.action == 'fixture':
        print(unpack_fixture(args.fixture, args.output))
    elif args.action == 'package':
        print(json.dumps(package_inputs(args.inputs, args.fixture, args.output)))
    elif args.action == 'restore':
        print(json.dumps(restore_inputs(args.archive, args.sha256, args.output)))
    else:
        if not 1 <= args.jobs <= 8 or not 600 <= args.deadline <= 14400 or args.minimum_free_gib < 1:
            raise ValueError('invalid CPU preparation limits')
        prepare(args)


if __name__ == '__main__':
    main()
