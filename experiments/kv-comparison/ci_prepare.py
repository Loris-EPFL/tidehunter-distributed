#!/usr/bin/env python3
"""Build and qualify current committed KV sources without AWS credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time

import cloud_package
import analyze
import campaign
import cloud_campaign

ROOT = Path(__file__).resolve().parents[2]
MAX_ARCHIVE = 768 << 20
MAX_EXPANDED = 1 << 30
MAX_FILES = 22000
BACKENDS = {'native', 'tidehunter_tcp', 'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'garnet', 'kvrocks'}


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


def admit_tcp_diagnostic(directory, ready):
    """Admit raw same-build TCP diagnostic evidence; no cluster or credentials."""
    directory = Path(directory)
    receipt_path = directory / 'diagnostic-proof.json'
    cloud_package.regular(receipt_path)
    if sha(receipt_path) != ready.get('tcp_diagnostic_sha256'):
        raise ValueError('TCP diagnostic proof identity differs from READY')
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get('schema_version') != 1 or receipt.get('status') != 'completed'
            or receipt.get('source_revision') != ready.get('engine_sha')
            or receipt.get('binary_sha256') != ready.get('binary_sha256')
            or receipt.get('binary_blake2b512') != ready.get('binary_blake2b512')
            or receipt.get('owner_exit_codes') != [0] * 5):
        raise ValueError('completed same-source/binary five-owner TCP diagnostic required')
    artifacts = ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom',
                 'read-diagnostics.json', 'measurement-read-evidence.json')
    hashes = receipt.get('artifacts_sha256')
    if not isinstance(hashes, dict) or set(hashes) != set(artifacts):
        raise ValueError('TCP diagnostic raw artifacts incomplete')
    for name in artifacts:
        if sha(directory / 'result' / name) != hashes[name]:
            raise ValueError('TCP diagnostic raw artifact differs')
    raw = json.loads((directory / 'config.json').read_text())
    config = json.loads((directory / 'normalized-config.json').read_text())
    analyze.verify_normalization(raw, config)
    options = config.get('backend', {}).get('options', {})
    owners = options.get('external_owner_read_config')
    if (config.get('source_revision') != ready['engine_sha']
            or config.get('qualification_only') is not False
            or config.get('backend', {}).get('kind') != 'tidehunter'
            or options.get('transport') != {'kind': 'tcp'}
            or options.get('owners') != 5 or options.get('coordinator_read_cache_bytes') != 0
            or not isinstance(owners, list) or len(owners) != 5
            or {owner.get('partition') for owner in owners} != set(range(5))
            or config.get('dataset', {}).get('keys') != 256
            or config.get('dataset', {}).get('value_size') != 256
            or config.get('phases', {}).get('preload') is not True
            or config.get('phases', {}).get('measurement_operations') != 64
            or config.get('diagnostics', {}).get('read_profile_limit') != 32
            or config.get('workload', {}).get('mix') != cloud_campaign.mix(read=100)):
        raise ValueError('TCP diagnostic workload, cache or source configuration differs')
    campaign.verify_run_identity(directory / 'result', receipt, config)
    manifest = json.loads((directory / 'result/manifest.json').read_text())
    summary = json.loads((directory / 'result/summary.json').read_text())
    if (manifest.get('raw_samples_complete') is not True or summary.get('status') != 'completed'
            or summary.get('backend') != manifest.get('backend_info')
            or manifest.get('backend_info', {}).get('name') != 'tidehunter_tcp'):
        raise ValueError('TCP diagnostic terminal backend evidence differs')
    effective = manifest['backend_info'].get('configuration', {})
    actual_owners = effective.get('owner_read_configuration')
    if (effective.get('coordinator', {}).get('read_cache_bytes') != 0
            or not isinstance(actual_owners, list) or len(actual_owners) != 5
            or {owner.get('partition') for owner in actual_owners} != set(range(5))):
        raise ValueError('TCP diagnostic actual owner/coordinator cache policy differs')
    counts = campaign.verify_timing_result(directory / 'result', config)
    if counts.get('preload') != 256 or counts.get('measurement') != 64:
        raise ValueError('TCP diagnostic raw sample count differs')
    if any(row['outcome'] != 'success' for row in analyze.read_samples(
            directory / 'result/samples.csv', measurement_only=False)):
        raise ValueError('TCP diagnostic contains unsuccessful API samples')
    job = {'backend': 'tidehunter_tcp', 'diagnostics': {'read_profile_limit': 32}}
    diagnostic = cloud_campaign.admit_read_diagnostics(directory, job, completed=True)
    interval = cloud_campaign.admit_measurement_read_evidence(directory)
    if diagnostic is None or interval is None:
        raise ValueError('TCP diagnostic or measurement evidence unavailable')
    profile = diagnostic['observed']['profile']
    coverage = profile.get('coverage')
    if (not isinstance(coverage, dict) or type(coverage.get('nvme_completed')) is not int
            or coverage.get('nvme_completed') != 0
            or type(coverage.get('completed_remote_requests')) is not int
            or coverage['completed_remote_requests'] <= 0):
        raise ValueError('real TCP read profile coverage is missing or incompatible')
    observed = interval['observed']
    coordinator = observed.get('deltas', {}).get('coordinator', {})
    counters = coordinator.get('counters', {})
    if (observed.get('measurement_drained') is not True or coordinator.get('available') is not True
            or type(counters.get('read_rpc_count')) is not int or counters['read_rpc_count'] <= 0
            or counters.get('hits') != 0):
        raise ValueError('TCP diagnostic does not prove uncached measured owner reads')
    return receipt


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
        raise ValueError('qualification must cover every software-admissible adapter; RDMA hardware remains separate')
    admit_tcp_diagnostic(Path(directory) / 'tcp-diagnostic', ready)


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
            or not isinstance(ready.get('tcp_diagnostic_sha256'), str)
            or not re.fullmatch('[0-9a-f]{64}', ready['tcp_diagnostic_sha256'])
            or not isinstance(ready.get('binary_blake2b512'), str)
            or not re.fullmatch('[0-9a-f]{128}', ready['binary_blake2b512'])
            or sha(inputs / 'bundle.tar.gz') != ready.get('bundle_sha256')):
        raise ValueError('prepared KV inputs are not bound to the current workflow source commit')
    package = inputs / 'package'
    verified = cloud_package.verify_directory(package)
    source = cloud_package.admit_source(package)
    if (source['revision'] != expected or source['dirty'] is not False
            or source['sha256'] != cloud_package.inventory(engine)
            or any(verified[key] != ready.get(key) for key in ('source_sha256', 'binary_sha256', 'binary_blake2b512'))
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


def run_tcp_diagnostic(binary, work, proof, source_revision, deadline, environment):
    """Exercise the built binary against five real local TCP owner processes."""
    binary = Path(binary).resolve(strict=True)
    work, proof = Path(work).absolute(), Path(proof).absolute()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    guest = work / 'guest'
    guest.mkdir(mode=0o700)
    sockets, processes, logs = [], [], []
    failure = None
    try:
        for _ in range(5):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(('127.0.0.1', 0))
            sockets.append(listener)
        addresses = [f'127.0.0.1:{listener.getsockname()[1]}' for listener in sockets]
        preparation = {'guest_root': str(guest), 'addresses': addresses, 'transport': {'kind': 'tcp'},
            'max_batches_per_partition': 16384, 'max_admitted_batches': 16384,
            'max_index_bytes': 128 * 1024**2, 'coordinator_cache_bytes': 0,
            'owner_read_workers': 2, 'owner_wal_member_read': 'async',
            'owner_read_memory_allowance_bytes': 512 * 1024**2, 'polling': 'adaptive'}
        cloud_package.dump(work / 'prepare.json', preparation)
        prepared = work / 'prepared'
        bounded([binary, '--prepare-external-case', work / 'prepare.json', '--output', prepared],
                deadline, environment=environment, cwd=ROOT)
        if (prepared / 'tls').is_dir():
            shutil.copytree(prepared / 'tls', guest / 'tls')
        for index in range(5):
            (guest / f'node-{index}').mkdir(mode=0o700)
        remote = json.loads((prepared / 'remote.json').read_text())
        owner_configs = json.loads((prepared / 'external_owner_read_config.json').read_text())
        options = {'path': str(work / 'database'), 'evidence': str(work / 'read-evidence'),
                   'owners': 5, 'key_count': 1048576, 'max_batches_per_partition': 16384,
                   'coordinator_read_cache_bytes': 0, 'external_remote_config': remote,
                   'external_owner_read_config': owner_configs}
        job = {'backend': 'tidehunter_tcp', 'case_id': 'local-tcp-diagnostic', 'repeat': 0,
               'keys': 256, 'value_size': 256, 'mix': cloud_campaign.mix(read=100),
               'concurrency': 1, 'measurement_operations': 64, 'warmup_operations': 0,
               'offered_ops_per_second': None, 'distribution': 'uniform',
               'diagnostics': {'read_profile_limit': 32}}
        config = cloud_campaign.make_run_config(job, work, options, source_revision, 180)
        config['key_namespace_hex'] = '00112233445566778899aabbccddeeff'
        config['measurement_gate'] = None
        config['measurement_completion_gate'] = None
        cloud_package.dump(work / 'config.json', config)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('TCP diagnostic admission deadline exhausted')
        campaign.admit_config(binary, work / 'config.json', work, environment, timeout=min(30, remaining))
        for index, listener in enumerate(sockets):
            listener.close()
            log = (work / f'owner-{index}.log').open('wb')
            logs.append(log)
            process = subprocess.Popen([str(binary), '--experimental-storage-node',
                str(prepared / f'node-{index}' / 'launch.yaml')], cwd=ROOT, env=environment,
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            processes.append(process)
        ready_deadline = min(deadline, time.monotonic() + 90)
        while True:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('TCP diagnostic owner exited before readiness')
            markers = [guest / f'node-{index}' / 'ready' for index in range(5)]
            if all(path.is_file() for path in markers):
                if [path.read_text().strip() for path in markers] != addresses:
                    raise ValueError('TCP diagnostic owner endpoints differ from admitted launch')
                break
            if time.monotonic() >= ready_deadline:
                raise TimeoutError('TCP diagnostic owners did not become ready')
            time.sleep(0.1)
        bounded([binary, '--config', work / 'config.json', '--output', work / 'result'],
                deadline, environment=environment, cwd=ROOT)
    except BaseException as exc:
        failure = exc
    finally:
        for listener in sockets:
            listener.close()
        for index in range(len(processes)):
            try:
                (guest / f'node-{index}' / 'stop').touch(exist_ok=True)
            except OSError as exc:
                failure = failure or exc
        cleanup_deadline = time.monotonic() + 25
        for process in processes:
            try:
                process.wait(timeout=max(0.01, cleanup_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    failure = failure or RuntimeError('TCP diagnostic owner remains live after forced cleanup')
                failure = failure or RuntimeError('TCP diagnostic owner did not drain before cleanup deadline')
        for log in logs:
            log.close()
    if failure is not None:
        raise failure
    exits = [process.returncode for process in processes]
    if len(exits) != 5 or any(code != 0 for code in exits):
        raise RuntimeError('TCP diagnostic owner shutdown did not succeed')
    if time.monotonic() >= deadline:
        raise TimeoutError('TCP diagnostic finished outside the preparation deadline')
    proof.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ('config.json', 'normalized-config.json'):
        shutil.copy2(work / name, proof / name)
    (proof / 'result').mkdir(mode=0o700)
    artifacts = ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom',
                 'read-diagnostics.json', 'measurement-read-evidence.json')
    for name in artifacts:
        shutil.copy2(work / 'result' / name, proof / 'result' / name)
    identity = cloud_package.identity(binary)
    receipt = {'schema_version': 1, 'status': 'completed', 'source_revision': source_revision,
        'binary_sha256': identity['sha256'], 'binary_blake2b512': identity['blake2b512'],
        'owner_exit_codes': exits, 'artifacts_sha256': {name: sha(proof / 'result' / name) for name in artifacts},
        'scope': 'five real local TCP owners, sampled API diagnostics; not EFA or performance qualification'}
    cloud_package.dump(proof / 'diagnostic-proof.json', receipt)
    ready = {'engine_sha': source_revision, 'binary_sha256': identity['sha256'],
             'binary_blake2b512': identity['blake2b512'],
             'tcp_diagnostic_sha256': sha(proof / 'diagnostic-proof.json')}
    admit_tcp_diagnostic(proof, ready)
    if time.monotonic() >= deadline:
        raise TimeoutError('TCP diagnostic proof admission exceeded the preparation deadline')
    shutil.rmtree(work)
    return ready


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
    diagnostic = run_tcp_diagnostic(output / 'package/runtime/bin/kv-research',
        output / 'tcp-diagnostic-work', proof / 'tcp-diagnostic', expected, deadline, environment)
    if (diagnostic['binary_sha256'] != manifest['binary_sha256']
            or diagnostic['binary_blake2b512'] != manifest['binary_blake2b512']):
        raise ValueError('TCP diagnostic binary differs from staged package')
    ready = dict(schema_version=1, complete=True, suite='kv-comparison', engine_sha=expected,
                 source_commit=expected, bundle_sha256=sha(output / 'bundle.tar.gz'),
                 source_sha256=manifest['source_sha256'], source_tree_sha256=manifest['source_tree_sha256'],
                 binary_sha256=manifest['binary_sha256'], binary_blake2b512=manifest['binary_blake2b512'],
                 tcp_diagnostic_sha256=diagnostic['tcp_diagnostic_sha256'],
                 manifest_sha256=sha(output / 'package/manifest.json'),
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
