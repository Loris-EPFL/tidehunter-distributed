#!/usr/bin/env python3
"""Source-bound real Sui checkpoint replay on an existing GCP fleet.

The launcher owns the fleet. This module never provisions or terminates VMs.
Fixture and local verification descriptors are immutable GCS generations.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import posixpath
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

TIP = 1775
CHECKPOINT = 'EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6'
BINARIES = {'sui-node-native', 'sui-node-remote', 'storage-node', 'state-audit', 'rocksdb-audit', 'transport-floor'}
TRANSPORTS = {'tcp', 'tidehunter_verbs', 'tidehunter_verbs_native'}


def owner_deadline(fleet_deadline, now=None):
    now = time.time() if now is None else now
    fleet_end = datetime.fromisoformat(fleet_deadline.replace('Z', '+00:00'))
    if fleet_end.utcoffset() != timezone.utc.utcoffset(fleet_end):
        raise ValueError('fleet deadline must be UTC')
    end = min(fleet_end.timestamp(), now + 7000)
    if end - now <= 60:
        raise TimeoutError('insufficient time for Sui owner cleanup')
    return datetime.fromtimestamp(end, timezone.utc).isoformat()


def storage_root(run_id):
    if not re.fullmatch(r'th-[0-9a-f]{16}', run_id):
        raise ValueError('invalid owned run ID')
    root = Path('/mnt/tidehunter-kv-comparison') / run_id
    device = Path('/dev/disk/by-id/google-local-nvme-ssd-0')
    if not device.is_symlink():
        raise ValueError('admitted Google local SSD is absent')
    mount = subprocess.run(['findmnt', '-rn', '-M', str(root), '-o', 'SOURCE'],
                           capture_output=True, text=True, timeout=10)
    if mount.returncode != 0 or Path(mount.stdout.strip()).resolve() != device.resolve():
        raise ValueError('Sui data root is not mounted on the admitted local SSD')
    return root


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def descriptor(value):
    if (not isinstance(value, dict) or not isinstance(value.get('key'), str)
            or not value['key'] or value['key'].startswith('/') or '..' in value['key'].split('/')
            or not str(value.get('generation', '')).isdigit() or int(value['generation']) <= 0
            or not re.fullmatch('[0-9a-f]{64}', str(value.get('sha256', '')))):
        raise ValueError('immutable GCS key, generation and SHA256 required')
    return value


def fetch(store, item, path, max_bytes=32 << 30):
    item = descriptor(item)
    store.download(item['key'], path, generation=str(item['generation']), max_bytes=max_bytes)
    if sha(path) != item['sha256']:
        raise ValueError('immutable GCS object digest differs')


def _archive_members(stream):
    members = stream.getmembers()
    if len(members) > 500000 or sum(m.size for m in members) > 64 << 30:
        raise ValueError('archive exceeds extraction bounds')
    names, links = set(), set()
    for member in members:
        name = Path(member.name)
        canonical = posixpath.normpath(member.name)
        if (name.is_absolute() or '..' in name.parts or canonical in ('.', '..')
                or canonical in names or not (member.isfile() or member.isdir() or member.issym())):
            raise ValueError('unsafe or duplicate archive member')
        names.add(canonical)
        if member.issym():
            target = posixpath.normpath(posixpath.join(posixpath.dirname(canonical), member.linkname))
            if member.linkname.startswith('/') or target == '..' or target.startswith('../'):
                raise ValueError('archive symlink escapes root')
            links.add(canonical)
    for name in names:
        parent = posixpath.dirname(name)
        while parent and parent != '.':
            if parent in links:
                raise ValueError('archive member traverses symlink')
            parent = posixpath.dirname(parent)
    return members


def inspect_archive(archive):
    """Apply the guest compressed-size and member safety limits without extraction."""
    archive = Path(archive)
    if archive.stat().st_size > 32 << 30:
        raise ValueError('archive exceeds compressed size bound')
    with tarfile.open(archive, 'r:gz') as stream:
        members = _archive_members(stream)
        return {'members': len(members), 'expanded_bytes': sum(m.size for m in members),
                'compressed_bytes': archive.stat().st_size}


def extract(archive, destination):
    destination = Path(destination)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    if Path(archive).stat().st_size > 32 << 30:
        raise ValueError('archive exceeds compressed size bound')
    with tarfile.open(archive, 'r:gz') as stream:
        stream.extractall(destination, members=_archive_members(stream), filter='data')


def admit_build(root, package, engine_revision):
    plan, manifest = load(root / 'plan.json'), load(root / 'build-manifest.json')
    if (sha(root / 'sources.tar.gz') != manifest['source_archive_sha256']
            or sha(root / 'source-manifest.json') != manifest['source_manifest_sha256']
            or any(plan.get(k) != manifest.get(k) for k in ('protocol', 'source_archive_sha256', 'source_manifest_sha256', 'fabric'))
            or set(manifest['binaries']) != BINARIES):
        raise ValueError('build source, protocol or executable inventory differs')
    source = load(root / 'source-manifest.json')
    if (set(source) != {'sui', 'tidehunter-sharded-wal'}
            or source['sui']['revision'] != package['sui_revision']
            or source['tidehunter-sharded-wal']['revision'] != engine_revision):
        raise ValueError('Sui or engine revision differs from sealed source')
    commands = manifest['commands']
    if len(commands) != len(BINARIES) or {c['label'] for c in commands} != BINARIES:
        raise ValueError('missing source-bound build command')
    for command in commands:
        name = command['label']
        if (sha(root / 'binaries' / name) != manifest['binaries'][name]
                or command['sha256'] != manifest['binaries'][name]
                or sha(root / ('build-' + name + '.log')) != command['log_sha256']
                or sha(root / ('build-' + name + '.jsonl')) != command['messages_sha256']):
            raise ValueError('binary or cargo provenance differs')
    unpack = root / 'source-unpack'
    extract(root / 'sources.tar.gz', unpack)
    (unpack / 'sources').rename(root / 'sources')
    unpack.rmdir()
    for owner in source:
        base = root / 'sources' / owner
        entries = source[owner]['files']
        actual = {str(p.relative_to(base)) for p in base.rglob('*') if p.is_file() or p.is_symlink()}
        if actual != set(entries):
            raise ValueError('frozen source inventory differs')
        for name, entry in entries.items():
            path = base / name
            if not path.resolve().is_relative_to(base.resolve()):
                raise ValueError('frozen source path escapes root')
            if 'link' in entry:
                if not path.is_symlink() or os.readlink(path) != entry['link']:
                    raise ValueError('frozen source link differs')
            elif path.is_symlink() or sha(path) != entry['sha256']:
                raise ValueError('frozen source file differs')
    return manifest


def admit_fixture(fixture, verification, manifest, build_root):
    fm = load(fixture / 'fixture-manifest.json')
    expected = fm['files']
    if {str(p.relative_to(fixture)) for p in fixture.rglob('*') if p.is_file()} != set(expected) | {'fixture-manifest.json'}:
        raise ValueError('fixture inventory differs')
    for name, digest in expected.items():
        p = fixture / name
        if p.is_symlink() or not p.resolve().is_relative_to(fixture.resolve()) or sha(p) != digest:
            raise ValueError('fixture content differs')
    prior = load(fixture / 'experiment/results.json')
    if (not prior.get('source_frozen') or prior.get('failure') or prior.get('checkpoint_limit') != TIP
            or prior.get('checkpoint_digest') != CHECKPOINT or fm.get('checkpoint_limit') != TIP
            or fm.get('checkpoint_digest') != CHECKPOINT or fm.get('source_range') != prior.get('verified_source_range')):
        raise ValueError('fixture is not the pinned complete real checkpoint chain')
    receipt = load(verification / 'receipt.json')
    result = load(verification / 'experiment/results.json')
    audit = load(verification / 'experiment/audit.json')
    if (receipt.get('complete') is not True or result.get('complete') is not True or result.get('failure')
            or sha(verification / 'evidence.tar.gz') != receipt.get('evidence_sha256')
            or any(r.get('build_manifest_sha256') != sha(build_root / 'build-manifest.json')
                   or r.get('source_archive_sha256') != manifest['source_archive_sha256'] for r in (receipt, result))
            or result.get('binaries') != manifest['binaries'] or result.get('checkpoint_digest') != CHECKPOINT
            or result.get('checkpoint_limit') != TIP or result.get('source_range') != prior['verified_source_range']
            or 'loopback' not in result.get('topology', '') or result.get('external_manifest')
            or audit != result.get('audit') or audit.get('all_equal') is not True
            or not audit.get('databases') or not audit.get('rocksdb')
            or any(result.get(mode, {}).get('completed_checkpoint_digest') != CHECKPOINT for mode in ('native', 'remote'))):
        raise ValueError('same-build local replay or full-state audit is incomplete')
    if {k: v for k, v in expected.items() if k.startswith('producer/')} != {k: v for k, v in result.get('fixture_files', {}).items() if k.startswith('producer/')}:
        raise ValueError('local replay used different producer data')
    evidence = verification / 'bound-evidence'
    extract(verification / 'evidence.tar.gz', evidence)
    supplied = verification / 'experiment'
    files = [p for p in supplied.rglob('*') if p.is_file()]
    if not any('native-schema' in p.parts for p in files):
        raise ValueError('native schema missing')
    for path in files:
        rel = path.relative_to(supplied)
        bound = evidence / 'experiment' / rel
        if rel.parts[0] not in {'results.json', 'audit.json', 'native-schema'} or not bound.is_file() or sha(path) != sha(bound):
            raise ValueError('local schema/results not bound to evidence')
    schema = supplied / 'native-schema'
    bound_schema = evidence / 'experiment/native-schema'
    if {str(p.relative_to(schema)) for p in schema.rglob('*') if p.is_file()} != {str(p.relative_to(bound_schema)) for p in bound_schema.rglob('*') if p.is_file()}:
        raise ValueError('native schema coverage differs')
    return prior

def selected_transports(spec):
    choices = spec.get('sui_transports', ['tcp'])
    if not isinstance(choices, list) or not choices or len(choices) > 4 or len(set(choices)) != len(choices):
        raise ValueError('Sui transports must be a nonempty unique list of at most four')
    if any(not isinstance(c, str) or c not in TRANSPORTS for c in choices):
        raise ValueError('unsupported Sui transport: remote software fabric, NVMe replay and EFA are unavailable on GCP')
    return choices

def check_receipt(receipt):
    if receipt.get('exit_code') != 0:
        raise RuntimeError('remote Sui operation failed: ' + str(receipt.get('stderr', ''))[-2048:])
    return receipt


def command(argv, timeout):
    subprocess.run([str(x) for x in argv], check=True, timeout=timeout)


def node_action(args):
    import gcp_api
    store = gcp_api.GoogleApi(args.project, token_provider=gcp_api.metadata_token).objects(args.bucket)
    base = storage_root(args.run_id) / 'checkpoint-cells' / args.cell / 'storage'
    if not re.fullmatch(r'th-[0-9a-f]{16}', args.run_id) or not re.fullmatch(r'r0[1-4]-(?:tcp|tidehunter_verbs(?:_native)?)', args.cell):
        raise ValueError('invalid run/cell identity')
    if args.action == 'start':
        archive = base.parent / 'payload.tar.gz'
        base.parent.mkdir(parents=True, exist_ok=True)
        fetch(store, dict(key=args.key, generation=args.generation, sha256=args.sha256), archive)
        extract(archive, base)
        return subprocess.run([sys.executable, str(base / 'cloud_node.py'), '--config', str(base / 'node.json'),
                               '--owned-base', '/mnt/tidehunter-kv-comparison'], check=False).returncode
    if args.action == 'ready':
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if (base / 'ready').is_file():
                return 0
            receipt = base / 'cloud-node-receipt.json'
            if receipt.is_file() and load(receipt).get('finished_unix_ns') is not None:
                raise RuntimeError('node exited before readiness')
            time.sleep(.2)
        raise TimeoutError('node readiness timed out')
    if args.action == 'stop':
        (base / 'stop').touch(exist_ok=False)
        return 0
    if args.action == 'collect':
        resource = base / 'resources.json'
        receipt = base / 'cloud-node-receipt.json'
        if not resource.is_file() or not receipt.is_file():
            raise ValueError('node final evidence missing')
        logs = base / 'cloud-node-logs'
        if not logs.is_dir():
            raise ValueError('node logs missing')
        log_archive = base.parent / 'node-logs.tar.gz'
        with tarfile.open(log_archive, 'w:gz') as archive:
            archive.add(logs, arcname='cloud-node-logs')
        log_key = f'runs/{args.run_id}/sui/{args.cell}/owner-{args.index}-logs.tar.gz'
        log_meta = store.upload(log_key, log_archive, if_generation_match=0)
        log_descriptor = dict(key=log_key, generation=str(log_meta['generation']), sha256=sha(log_archive))
        value = {'resources': load(resource), 'receipt': load(receipt), 'logs': log_descriptor}
        key = f'runs/{args.run_id}/sui/{args.cell}/owner-{args.index}-evidence.json'
        raw = json.dumps(value, sort_keys=True).encode()
        meta = store.put(key, raw, if_generation_match=0)
        print(json.dumps({'key': key, 'generation': str(meta['generation']), 'sha256': hashlib.sha256(raw).hexdigest()}))
        return 0
    raise ValueError('unknown node action')


def run_suite(config, mailbox, package, fixture, *, output_root):
    """Run paired native/distributed Sui cells; raise on any incomplete evidence."""
    spec, fleet, store = config['spec'], config['fleet'], mailbox.store
    transports = selected_transports(spec)
    output = Path(output_root)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    data_root = storage_root(config['run_id'])
    inputs = data_root / 'sui-inputs'
    inputs.mkdir(mode=0o700)
    build_item = dict(key=package['sui_object_key'], generation=package['sui_generation'], sha256=package['sui_sha256'])
    for name, item in (('build', build_item), ('fixture', fixture), ('verification', spec['sui_verification'])):
        fetch(store, item, inputs / (name + '.tar.gz'))
        extract(inputs / (name + '.tar.gz'), inputs / name)
    wrapped = inputs / 'build'
    build_receipt = load(wrapped / 'build-receipt.json')
    if sha(wrapped / 'build-bundle.tar.gz') != build_receipt['bundle_sha256']:
        raise ValueError('Sui build bundle receipt differs')
    extract(wrapped / 'build-bundle.tar.gz', inputs / 'build-unpacked')
    build_root = inputs / 'build-unpacked'
    fixture_root = inputs / 'fixture/fixture'
    verification = inputs / 'verification/verification'
    engine_revision = load(Path(config['package_root']) / 'manifest.json')['revision']
    manifest = admit_build(build_root, package, engine_revision)
    if (sha(build_root / 'build-manifest.json') != build_receipt['build_manifest_sha256']
            or manifest['source_archive_sha256'] != build_receipt['source_archive_sha256']):
        raise ValueError('Sui build manifest receipt differs')
    sealed = load(Path(config['package_root']) / 'source.json')['sha256']
    frozen = load(build_root / 'source-manifest.json')['tidehunter-sharded-wal']['files']
    for name, digest in sealed.items():
        if name not in ('Cargo.lock', 'experiments/sui-checkpoint/audit/Cargo.lock'):
            if frozen.get(name, {}).get('sha256') != digest:
                raise ValueError('Sui build engine source differs from sealed runtime: ' + name)
    admit_fixture(fixture_root, verification, manifest, build_root)
    scripts = build_root / 'sources/tidehunter-sharded-wal/experiments/sui-checkpoint'
    sys.path.insert(0, str(scripts.parent))
    from portable_transport import transport_for_case, select_transports
    if any(t != 'tcp' for t in transports) and not manifest.get('fabric'):
        raise ValueError('selected fabric transport requires a fabric-enabled Sui build')
    helper = Path(config['package_root']) / 'source/experiments/gcp-rdma/gcp_sui.py'
    base = data_root / 'checkpoint-cells'
    results = []
    for number, label in enumerate(transports, 1):
        cell = f'r0{number}-{label}'
        cell_root = base / cell
        cell_root.mkdir(parents=True, exist_ok=False, mode=0o700)
        out = output / cell
        out.mkdir()
        replay = cell_root / 'replay'
        import yaml
        suffix = Path(yaml.safe_load((fixture_root / 'distributed.yaml').read_text())['db-path']).relative_to('distributed')
        transport = transport_for_case(label)
        owners = select_transports(transport, None, 5)
        if label.startswith('tidehunter_verbs'):
            transport['fabric']['source_address'] = fleet[0]['rdma_ip']
            for host, owner in zip(fleet[1:], owners):
                owner['fabric']['source_address'] = host['rdma_ip']
        hosts = [h['internal_ip'] for h in fleet[1:]]
        save(cell_root / 'transport.json', transport)
        save(cell_root / 'owners.json', owners)
        cache = spec.get('sui_coordinator_cache_mib', 8)
        read_profile = spec.get('sui_read_profile_limit', 0)
        write_profile = spec.get('sui_write_profile_limit', 0)
        native_profile = spec.get('sui_native_write_profile_limit', 0)
        deployment = cell_root / 'deployment'
        command([sys.executable, scripts / 'prepare_external.py', '--schema', verification / 'experiment/native-schema', '--build', build_root,
                 '--output', deployment, '--db-root', replay / 'remote' / suffix, '--guest-root', cell_root / 'storage', '--hosts', ','.join(hosts),
                 '--transport-config', cell_root / 'transport.json', '--owner-transports', cell_root / 'owners.json', '--coordinator-cache-mib', cache,
                 '--write-profile-limit', write_profile], 120)
        external = load(deployment / 'external-manifest.json')
        if (external.get('config_sha256') != sha(deployment / 'config.json')
                or external.get('build_manifest_sha256') != sha(build_root / 'build-manifest.json')
                or external.get('storage_binary_sha256') != manifest['binaries']['storage-node']
                or len(external.get('hosts', [])) != 5):
            raise ValueError('prepared deployment provenance or host count differs')
        references = []
        stop_attempted = set()
        owner_completions = []
        cell_deadline = owner_deadline(config['deadline_utc'])
        def call(index, action, extra=(), timeout=180):
            argv = ['/usr/bin/python3', str(helper), 'node', action, '--project', config['project_id'], '--bucket', config['bucket'], '--run-id', config['run_id'], '--cell', cell, '--index', str(index), *map(str, extra)]
            return mailbox.submit(index, argv, operation_id=f'sui-{cell}-{action}-{index}', timeout_seconds=timeout)
        try:
            for index, host in enumerate(external['hosts'], 1):
                if host.get('partition') != index - 1 or host.get('host') != hosts[index - 1] or sha(host['archive']) != host['archive_sha256']:
                    raise ValueError('prepared host payload identity differs')
                payload = cell_root / f'payload-{index}'
                extract(host['archive'], payload)
                shutil.copy2(scripts / 'cloud_node.py', payload / 'cloud_node.py')
                (payload / 'runner').mkdir()
                shutil.copy2(scripts.parent / 'portable_transport.py', payload / 'runner/portable_transport.py')
                node = {k: host[k] for k in ('commands', 'ready_files', 'stop_files')}
                node.update(run_id=config['run_id'], cell_id=cell, transport=label, client_transport=transport, owner_transport=owners[index-1],
                            deadline_utc=cell_deadline, storage_binary_sha256=manifest['binaries']['storage-node'],
                            build_manifest_sha256=sha(build_root / 'build-manifest.json'), config_sha256=sha(deployment / 'config.json'),
                            checkpoint_digest=CHECKPOINT, root=str(cell_root / 'storage'), resource_path=str(cell_root / 'storage/resources.json'),
                            ready_path=str(cell_root / 'storage/ready'), stop_path=str(cell_root / 'storage/stop'))
                save(payload / 'node.json', node)
                archive = cell_root / f'payload-{index}.tar.gz'
                with tarfile.open(archive, 'w:gz') as stream:
                    for path in sorted(payload.iterdir()):
                        stream.add(path, arcname=path.name)
                key = f'runs/{config["run_id"]}/sui/{cell}/payload-{index}.tar.gz'
                meta = store.upload(key, archive, if_generation_match=0)
                references.append(call(index, 'start', ('--key', key, '--generation', str(meta['generation']), '--sha256', sha(archive)), timeout=7200))
            for index in range(1, 6):
                check_receipt(mailbox.wait(call(index, 'ready'), timeout_seconds=150))
            stages = cell_root / 'stages'
            stages.mkdir()
            resources = cell_root / 'resources.json'
            args = [sys.executable, scripts / 'run.py', '--build', build_root, '--fixture', fixture_root, '--output', replay,
                    '--deadline', spec.get('sui_replay_deadline_seconds', 900), '--external-config', deployment / 'config.json',
                    '--external-resources', resources, '--external-stage-dir', stages, '--coordinator-cache-mib', cache,
                    '--read-profile-limit', read_profile, '--write-profile-limit', write_profile,
                    '--native-write-profile-limit', native_profile]
            with (out / 'replay.log').open('wb') as log:
                child = subprocess.Popen(list(map(str, args)), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    deadline = datetime.fromisoformat(cell_deadline).timestamp()
                    while child.poll() is None and not (stages / 'resources-required.json').is_file():
                        if time.time() >= deadline - 30:
                            raise TimeoutError('replay exceeded the owner deadline reserve')
                        time.sleep(.2)
                    if child.poll() is not None:
                        raise RuntimeError('replay exited before resource handshake')
                    stage_rows = {name: load(stages / (name + '.json')) for name in ('remote-starting', 'remote-finished', 'resources-required')}
                    binding = dict(config_sha256=sha(deployment / 'config.json'), build_manifest_sha256=sha(build_root / 'build-manifest.json'), checkpoint_digest=CHECKPOINT)
                    if any(any(row.get(k) != v for k, v in binding.items()) or row.get('stage') != name for name, row in stage_rows.items()):
                        raise ValueError('stage provenance differs')
                    for index in range(1, 6):
                        stop_attempted.add(index)
                        check_receipt(mailbox.wait(call(index, 'stop'), timeout_seconds=180))
                    for ref in references:
                        owner_completions.append(check_receipt(mailbox.wait(ref, timeout_seconds=180)))
                    host_resources = {}
                    for index in range(1, 6):
                        rec = check_receipt(mailbox.wait(call(index, 'collect'), timeout_seconds=180))
                        desc = descriptor(json.loads(rec['stdout'].strip().splitlines()[-1]))
                        data = store.get(desc['key'], generation=desc['generation'], max_bytes=32 << 20)
                        if hashlib.sha256(data).hexdigest() != desc['sha256']:
                            raise ValueError('node evidence digest differs')
                        value = json.loads(data)
                        if value['receipt'].get('complete') is not True:
                            raise ValueError('node supervisor did not complete')
                        fetch(store, value['logs'], out / f'owner-{index}-logs.tar.gz', max_bytes=1 << 30)
                        host_resources[hosts[index-1]] = value['resources']
                        save(out / f'owner-{index}.json', value)
                    save(resources, dict(**binding, cell_id=cell, stages=stage_rows, hosts=host_resources))
                    save(stages / 'resources-ready', dict(**binding, unix_ns=time.time_ns()))
                    if child.wait(timeout=180) != 0:
                        raise RuntimeError('replay or state audit failed')
                finally:
                    if child.poll() is None:
                        import signal
                        os.killpg(child.pid, signal.SIGTERM)
                        try:
                            child.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait()
            command([sys.executable, scripts / 'analyze.py', replay, '--verify-only'], 120)
            result, receipt = load(replay / 'experiment/results.json'), load(replay / 'receipt.json')
            if (result.get('complete') is not True or result.get('failure') or result.get('audit', {}).get('all_equal') is not True
                    or receipt.get('complete') is not True or sha(replay / 'evidence.tar.gz') != receipt.get('evidence_sha256')
                    or result.get('binaries') != manifest['binaries'] or result.get('checkpoint_digest') != CHECKPOINT):
                raise ValueError('replay receipt or full-state audit failed')
            results.append(dict(cell=cell, transport=label, receipt=receipt, owner_receipts=owner_completions))
        finally:
            # run.py writes its failure receipt/evidence in its own finally.
            # Keep any completed files even when replay or qualification fails.
            active_failure = sys.exc_info()[0] is not None
            retention_error = None
            for name in ('evidence.tar.gz', 'receipt.json'):
                source = replay / name
                if source.is_file():
                    try:
                        shutil.copy2(source, out / name)
                    except OSError as error:
                        retention_error = retention_error or error
                        print(f'could not retain replay {name}: {error}', file=sys.stderr)
            # Submit each remaining stop at most once, even after a lost response.
            # The guest watchdog remains the last-resort absolute deadline.
            cleanup = {}
            for ref in references:
                index = ref['index']
                if index not in stop_attempted:
                    stop_attempted.add(index)
                    try:
                        cleanup[index] = mailbox.wait(call(index, 'stop', timeout=30), timeout_seconds=35)
                    except Exception as error:
                        cleanup[index] = {'error': type(error).__name__, 'message': str(error)}
            if cleanup:
                try:
                    save(out / 'cleanup.json', cleanup)
                except OSError as error:
                    retention_error = retention_error or error
                    print(f'could not retain cleanup evidence: {error}', file=sys.stderr)
            if retention_error is not None and not active_failure:
                raise retention_error
    receipt = dict(complete=True, checkpoint_digest=CHECKPOINT, checkpoint_limit=TIP, fixture=fixture,
                   verification=spec['sui_verification'], build=build_item, cells=results)
    save(output / 'suite-receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='kind', required=True)
    node = sub.add_parser('node')
    node.add_argument('action', choices=['start', 'ready', 'stop', 'collect'])
    for name in ('project', 'bucket', 'run-id', 'cell', 'index'):
        node.add_argument('--' + name, required=True)
    for name in ('key', 'generation', 'sha256'):
        node.add_argument('--' + name)
    args = parser.parse_args()
    return node_action(args)


if __name__ == '__main__':
    raise SystemExit(main())
