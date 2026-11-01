#!/usr/bin/env python3
"""Build and qualify the exact clean engine checkout without cloud credentials."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'experiments/kv-comparison'))
import cloud_package
import ci_prepare


def checkout(repo, revision):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('a full lowercase commit SHA is required')
    actual = cloud_package.command(['git', 'rev-parse', 'HEAD'], cwd=repo)
    dirty = cloud_package.command(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=repo)
    if actual != revision or dirty:
        raise ValueError(f'checkout {repo} must be clean and at the requested commit')


def prepare(engine, output, revision, fabric, deadline_seconds):
    engine = Path(engine).resolve(strict=True)
    if engine != ROOT:
        raise ValueError('preparer must execute from the selected engine checkout')
    checkout(engine, revision)
    if not 1800 <= deadline_seconds <= 14400:
        raise ValueError('deadline must be 1800..14400 seconds')
    output = Path(output).absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    deadline = time.monotonic() + deadline_seconds
    fabric = Path(fabric).resolve(strict=True)
    fdb = output / 'fdb-client'
    ci_prepare.bounded([sys.executable, engine / 'experiments/kv-comparison/local.py', 'prepare-fdb', '--prefix', fdb], deadline)
    snapshot = output / 'snapshot'
    cloud_package.snapshot(engine, snapshot)
    build = output / 'build'
    remaining = min(7200, int(deadline - time.monotonic()) - 1200)
    if remaining <= 0:
        raise TimeoutError('no build and qualification reserve remains')
    cloud_package.build(snapshot, build, fdb, fabric, timeout=remaining)
    package = output / 'package'
    cloud_package.stage(engine, snapshot, build, fdb, package)
    if (package / 'bundle.tar.gz').stat().st_size > 512 << 20:
        raise ValueError('compressed KV runtime exceeds guest download limit')
    shutil.copy2((fabric / 'lib/libfabric.so.1').resolve(strict=True), fdb / 'lib/libfabric.so.1')
    env = cloud_package.clean_environment()
    env.update({'FDB_CLIENT_LIB_DIR': str(fdb / 'lib'), 'FDB_CLIENT_LIB_PATH': str(fdb / 'lib'),
                'LD_LIBRARY_PATH': f'{fdb}/lib:{fabric}/lib', 'FI_PROVIDER': 'tcp;ofi_rxm'})
    raw = output / 'qualification-raw'
    ci_prepare.bounded([sys.executable, engine / 'experiments/kv-comparison/campaign.py', '--manifest',
                        engine / 'experiments/kv-comparison/ci.json', '--binary', package / 'runtime/bin/kv-research',
                        '--qualification', '--output', raw], deadline, environment=env, cwd=engine)
    proof = output / 'qualification'
    proof.mkdir()
    for receipt in sorted(raw.glob('*/receipt.json')):
        dest = proof / receipt.parent.name
        dest.mkdir()
        for name in ('receipt.json', 'config.json', 'normalized-config.json', 'run.log', 'client-resources.jsonl'):
            shutil.copy2(receipt.parent / name, dest / name)
        (dest / 'result').mkdir()
        for name in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom'):
            shutil.copy2(receipt.parent / 'result' / name, dest / 'result' / name)
    manifest = json.loads((package / 'manifest.json').read_text())
    diagnostic = ci_prepare.run_tcp_diagnostic(package / 'runtime/bin/kv-research', output / 'tcp-diagnostic-work',
                                               proof / 'tcp-diagnostic', revision, deadline, env)
    if diagnostic['binary_sha256'] != manifest['binary_sha256'] or diagnostic['binary_blake2b512'] != manifest['binary_blake2b512']:
        raise ValueError('TCP proof is not from the packaged executable')
    ready = {'schema_version': 1, 'engine_sha': revision, 'binary_sha256': manifest['binary_sha256'],
             'binary_blake2b512': manifest['binary_blake2b512'], 'tcp_diagnostic_sha256': diagnostic['tcp_diagnostic_sha256'],
             'qualification_files': ci_prepare.proof_inventory(proof)}
    ci_prepare.admit_qualification(proof, ready)
    verified = cloud_package.verify_directory(package)
    source = cloud_package.admit_source(package)
    checkout(engine, revision)
    if source['revision'] != revision or source['dirty'] or source['sha256'] != cloud_package.inventory(engine):
        raise ValueError('packaged source no longer matches clean checkout')
    receipt = {'archive_path': str((package / 'bundle.tar.gz').resolve()),
               'sha256': ci_prepare.sha(package / 'bundle.tar.gz'), 'source_sha256': verified['source_sha256'],
               'source_revision': revision, 'binary_sha256': verified['binary_sha256'],
               'binary_blake2b512': verified['binary_blake2b512'], 'qualification': ready}
    cloud_package.dump(output / 'package-receipt.json', receipt)
    return receipt

def admit_sui_archives(bundle, verification):
    import gcp_sui

    def nested(parent, name, target):
        with tarfile.open(parent, 'r:gz') as archive:
            member = archive.getmember(name)
            if not member.isfile():
                raise ValueError('nested Sui archive is not a regular file')
            with archive.extractfile(member) as source, target.open('xb') as destination:
                shutil.copyfileobj(source, destination)
        gcp_sui.inspect_archive(target)

    gcp_sui.inspect_archive(bundle)
    gcp_sui.inspect_archive(verification)
    with tempfile.TemporaryDirectory(prefix='gcp-sui-admission-') as temporary:
        root = Path(temporary)
        nested(bundle, 'build-bundle.tar.gz', root / 'build.tar.gz')
        nested(root / 'build.tar.gz', 'sources.tar.gz', root / 'sources.tar.gz')
        nested(verification, 'verification/evidence.tar.gz', root / 'evidence.tar.gz')



def prepare_sui(output, sui, revision, fixture, target, deadline_seconds):
    output = Path(output).resolve(strict=True)
    sui = Path(sui).resolve(strict=True)
    checkout(ROOT, json.loads((output / 'package-receipt.json').read_text())['source_revision'])
    checkout(sui, revision)
    if revision != 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86':
        raise ValueError('Sui revision must be the reviewed checkpoint metrics commit')
    if not 1800 <= deadline_seconds <= 21600:
        raise ValueError('Sui preparation deadline must be 1800..21600 seconds')
    spec = importlib.util.spec_from_file_location('checkpoint_ci', ROOT / 'experiments/aws-sharded-wal/ci_prepare.py')
    ci = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ci)
    work = output / 'sui'
    work.mkdir(mode=0o700)
    import gcp_sui
    gcp_sui.inspect_archive(fixture)
    admitted_fixture = ci.unpack_fixture(Path(fixture), work / 'fixture-input')
    transport = ci.local_replay_transport()
    transport_config = work / 'transport.json'
    ci.write_local_replay_transport(transport_config)
    build = work / 'build'
    replay = work / 'replay'
    deadline = time.monotonic() + deadline_seconds
    script = ROOT / 'experiments/sui-checkpoint/build.py'
    commands = [
        [sys.executable, script, 'prepare', '--sui', sui, '--engine', ROOT, '--output', build, '--fabric', '--jobs', '2'],
        [sys.executable, script, 'build', '--output', build, '--target', Path(target), '--deadline', str(deadline_seconds)],
        [sys.executable, ROOT / 'experiments/sui-checkpoint/run.py', '--build', build, '--fixture', admitted_fixture,
         '--output', replay, '--deadline', '900', '--transport-config', transport_config],
    ]
    for command in commands:
        ci_prepare.bounded(command, deadline)
    verification = ci.verification_bundle(replay, work / 'verification', transport)
    source = json.loads((build / 'source-manifest.json').read_text())
    if source['sui']['revision'] != revision or source['tidehunter-sharded-wal']['revision'] != json.loads((output / 'package-receipt.json').read_text())['source_revision']:
        raise ValueError('Sui build source revisions differ from admitted checkouts')
    checkout(sui, revision)
    bundle = work / 'sui-package.tar.gz'
    with tarfile.open(bundle, 'x:gz') as archive:
        archive.add(build / 'build-bundle.tar.gz', arcname='build-bundle.tar.gz')
        archive.add(build / 'build-receipt.json', arcname='build-receipt.json')
    verification_archive = work / 'verification.tar.gz'
    with tarfile.open(verification_archive, 'x:gz') as archive:
        archive.add(verification, arcname='verification')
    admit_sui_archives(bundle, verification_archive)
    receipt = json.loads((output / 'package-receipt.json').read_text())
    receipt.update(sui_archive_path=str(bundle.resolve()), sui_sha256=ci_prepare.sha(bundle), sui_revision=revision,
                   verification_archive_path=str(verification_archive.resolve()),
                   verification_sha256=ci_prepare.sha(verification_archive))
    cloud_package.dump(output / 'package-receipt.json', receipt)
    return receipt

def admit_transfer(output, revision):
    output = Path(output).resolve(strict=True)
    checkout(ROOT, revision)
    receipt_path = output / 'package-receipt.json'
    receipt = json.loads(receipt_path.read_text())
    package = output / 'package'
    verified = cloud_package.verify_directory(package)
    source = cloud_package.admit_source(package)
    if (source['revision'] != revision or source['dirty'] or source['sha256'] != cloud_package.inventory(ROOT)
            or receipt.get('source_revision') != revision
            or any(receipt.get(k) != verified[k] for k in ('source_sha256', 'binary_sha256', 'binary_blake2b512'))
            or receipt.get('sha256') != ci_prepare.sha(package / 'bundle.tar.gz')):
        raise ValueError('transferred package does not match the current checkout and binary')
    archive = package / 'bundle.tar.gz'
    if archive.stat().st_size > 512 << 20:
        raise ValueError('compressed KV runtime exceeds guest download limit')
    archive_identity = cloud_package.verify_archive(archive, receipt['sha256'])
    if any(archive_identity.get(k) != verified[k] for k in ('source_sha256', 'binary_sha256', 'binary_blake2b512')):
        raise ValueError('transferred runtime archive differs from qualified directory')
    proof = receipt.get('qualification', {})
    if proof.get('engine_sha') != revision or proof.get('binary_sha256') != verified['binary_sha256'] or proof.get('binary_blake2b512') != verified['binary_blake2b512']:
        raise ValueError('qualification does not match transferred source and binary')
    ci_prepare.admit_qualification(output / 'qualification', proof)
    receipt['archive_path'] = str((package / 'bundle.tar.gz').resolve())
    if receipt.get('sui_sha256'):
        if receipt.get('sui_revision') != 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86':
            raise ValueError('transferred Sui revision is not reviewed')
        for path_key, sha_key, filename in (
                ('sui_archive_path', 'sui_sha256', 'sui-package.tar.gz'),
                ('verification_archive_path', 'verification_sha256', 'verification.tar.gz')):
            path = output / 'sui' / filename
            if ci_prepare.sha(path) != receipt.get(sha_key):
                raise ValueError('transferred Sui archive digest differs')
            receipt[path_key] = str(path.resolve())
        admit_sui_archives(Path(receipt['sui_archive_path']), Path(receipt['verification_archive_path']))
    cloud_package.dump(receipt_path, receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--fabric-prefix', type=Path)
    parser.add_argument('--deadline', type=int, default=10800)
    parser.add_argument('--sui', type=Path)
    parser.add_argument('--fixture', type=Path)
    parser.add_argument('--target', type=Path)
    parser.add_argument('--admit-transfer', action='store_true')
    args = parser.parse_args()
    if args.admit_transfer:
        result = admit_transfer(args.output, args.revision)
    elif args.sui:
        if not args.fixture or not args.target:
            parser.error('--sui requires --fixture and --target')
        result = prepare_sui(args.output, args.sui, args.revision, args.fixture, args.target, args.deadline)
    else:
        if not args.fabric_prefix:
            parser.error('--fabric-prefix is required for KV preparation')
        result = prepare(args.engine, args.output, args.revision, args.fabric_prefix, args.deadline)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
