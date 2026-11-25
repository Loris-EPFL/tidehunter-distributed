#!/usr/bin/env python3
"""Freeze exact dirty sources, build owned copies, and bind outputs to source archives."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
sys.dont_write_bytecode = True
import tarfile
import time
import tomllib


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


class OwnedProcess:
    """A session leader whose PID stays reserved until group cleanup finishes.

    poll/wait deliberately observe without reaping, including calls by checkpoint
    readiness helpers. Only reap releases the leader after the final group kill.
    """
    def __init__(self, command, **kwargs):
        self._process = subprocess.Popen(command, start_new_session=True, **kwargs)
        self.pid, self.args = self._process.pid, self._process.args
        self.returncode = None
        self.reaped = False

    def poll(self):
        if self.reaped:
            return self.returncode
        if self._process.returncode is not None:
            raise RuntimeError('owned process leader was reaped outside group cleanup')
        # No cached-exit fast path: verify that we still own the unreaped child.
        # ECHILD is an ownership failure, never permission to signal a reused PID.
        try:
            status = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except ChildProcessError:
            raise RuntimeError('owned process leader was reaped outside group cleanup') from None
        if status is not None:
            if status.si_code == os.CLD_EXITED:
                self.returncode = status.si_status
            elif status.si_code in (os.CLD_KILLED, os.CLD_DUMPED):
                self.returncode = -status.si_status
            else:
                raise RuntimeError('unexpected owned process wait status')
        return self.returncode

    def wait(self, timeout):
        deadline = time.monotonic()+timeout
        while True:
            code = self.poll()
            if code is not None:
                return code
            left = deadline-time.monotonic()
            if left <= 0:
                raise subprocess.TimeoutExpired(self.args, timeout)
            time.sleep(min(.05, left))

    def signal_group(self, number):
        if self.reaped:
            raise RuntimeError('cannot signal a released process group identity')
        self.poll()
        try:
            os.killpg(self.pid, number)
        except ProcessLookupError:
            # The leader can remain waitable while its process group is empty.
            pass

    def reap(self, timeout):
        self.poll()
        self.returncode = self._process.wait(timeout=timeout)
        self.reaped = True
        return self.returncode


def stop_group(child, grace=5):
    """Signal descendants before reaping the leader; never signal recycled IDs."""
    if not isinstance(child, OwnedProcess):
        raise TypeError('group cleanup requires an owned, unreaped session leader')
    if child.reaped:
        return
    try:
        child.signal_group(signal.SIGTERM)
        try:
            child.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
    finally:
        child.signal_group(signal.SIGKILL)
        child.reap(timeout=5)


def bounded(command, *, timeout, **kwargs):
    """A timeout/interrupt owns Cargo and its rustc/linker descendants."""
    child = OwnedProcess(command, **kwargs)
    try:
        code = child.wait(timeout=timeout)
        if code:
            raise subprocess.CalledProcessError(code, command)
    finally:
        stop_group(child)


def source_files(root):
    names = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], timeout=60).split(b'\0')
    result = {}
    for raw in sorted(set(names)):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('unsafe source path')
        path = root / relative
        if not path.exists():  # tracked deletion is part of the exact working tree
            continue
        if path.is_symlink():
            if not path.resolve().is_relative_to(root):
                raise ValueError(f'external source symlink: {path}')
            result[str(relative)] = {'link': os.readlink(path)}
        elif path.is_file():
            if path.stat().st_size > 128 << 20:
                raise ValueError(f'oversize untracked/source file: {path}')
            result[str(relative)] = {'sha256': digest(path), 'mode': path.stat().st_mode & 0o777}
        else:
            raise ValueError(f'submodule/non-file must be explicitly resolved: {path}')
    return result


def snapshot(source, target):
    before = source_files(source)
    target.mkdir(parents=True)
    for name, entry in before.items():
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if 'link' in entry:
            destination.symlink_to(entry['link'])
        else:
            shutil.copy2(source / name, destination)
            if digest(destination) != entry['sha256']:
                raise ValueError(f'source changed while copying: {name}')
    if before != source_files(source):
        raise ValueError('source changed during snapshot; preserve output and retry in a fresh directory')
    return {'revision': subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], timeout=30).decode().strip(),
            'files': before}


def bind_staged_engine(sources, identities, fabric):
    """Patch only the frozen Sui workspace, never its portable Git dependency."""
    engine = sources / 'tidehunter-sharded-wal'
    manifests = {'tidehunter': engine / 'tidehunter/Cargo.toml'}
    fabric_manifest = engine / 'tidehunter-fabric/Cargo.toml'
    if fabric or fabric_manifest.exists():
        manifests['tidehunter-fabric'] = fabric_manifest
    for name, path in manifests.items():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'selected engine package manifest must be an ordinary file: {path}')
        package = tomllib.loads(path.read_text()).get('package', {})
        if package.get('name') != name:
            raise ValueError(f'selected engine package name differs: {path}')

    def dependencies(table):
        for key, value in table.items():
            if key in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                yield from value.items()
            elif isinstance(value, dict):
                yield from dependencies(value)

    sui = sources / 'sui'
    pins = set()
    found = set()
    for relative in identities['sui']['files']:
        if Path(relative).name != 'Cargo.toml':
            continue
        path = sui / relative
        document = tomllib.loads(path.read_text())
        for patches in document.get('patch', {}).values():
            for alias, spec in patches.items():
                if alias in manifests or isinstance(spec, dict) and spec.get('package') in manifests:
                    raise ValueError(f'pre-existing engine source patch is ambiguous: {path}')
        for alias, spec in dependencies(document):
            name = spec.get('package', alias) if isinstance(spec, dict) else alias
            if name not in ('tidehunter', 'tidehunter-fabric'):
                continue
            if not isinstance(spec, dict) or name not in manifests:
                raise ValueError(f'unsupported engine dependency source: {path}: {alias}')
            if spec.get('workspace') is True:
                continue  # The workspace declaration is inspected separately.
            found.add(name)
            if 'path' in spec and not any(key in spec for key in ('git', 'registry')):
                if (path.parent / spec['path']).resolve(strict=True) != manifests[name].parent.resolve():
                    raise ValueError(f'engine dependency path does not select staged engine: {path}: {alias}')
            elif ('git' in spec and 'path' not in spec and 'registry' not in spec
                  and not any(key in spec for key in ('branch', 'tag'))
                  and isinstance(spec.get('rev'), str) and len(spec['rev']) == 40
                  and all(c in '0123456789abcdefABCDEF' for c in spec['rev'])):
                pins.add((spec['git'], spec['rev']))
            else:
                raise ValueError(f'engine dependency must use a staged path or immutable Git revision: {path}: {alias}')
    if 'tidehunter' not in found or len(pins) > 1:
        raise ValueError('Sui must select one unambiguous engine source')
    if pins:
        manifest = sui / 'Cargo.toml'
        if manifest.is_symlink() or not manifest.is_file():
            raise ValueError('Sui workspace manifest must be an ordinary file')
        source, _ = pins.pop()
        with manifest.open('a') as stream:
            for name, path in manifests.items():
                relative = os.path.relpath(path.parent, sui)
                stream.write(f'\n[patch.{json.dumps(source)}.{name}]\npath = {json.dumps(relative)}\n')
        identities['sui']['files']['Cargo.toml'] = {
            'sha256': digest(manifest), 'mode': manifest.stat().st_mode & 0o777}
    return manifests


def verify_engine_metadata(metadata, manifests, fabric):
    """Admit Cargo's resolved package graph, not just manifest override text."""
    expected = {'tidehunter'} | ({'tidehunter-fabric'} if fabric else set())
    selected = {}
    for package in metadata['packages']:
        name = package['name']
        if name not in ('tidehunter', 'tidehunter-fabric'):
            continue
        manifest = manifests.get(name)
        if (manifest is None or name in selected or package['source'] is not None
                or Path(package['manifest_path']).resolve(strict=True) != manifest.resolve(strict=True)):
            raise ValueError(f'Cargo resolved an engine package outside the selected staged source: {name}')
        selected[name] = package['id']
    graph = metadata.get('resolve')
    if graph is None:
        raise ValueError('Cargo metadata must include its resolved dependency graph')
    nodes = {node['id']: node for node in graph['nodes']}
    reachable = set()
    pending = list(metadata['workspace_members'])
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        pending.extend(edge['pkg'] for edge in nodes[package_id]['deps'])
    if any(name not in selected or selected[name] not in reachable for name in expected):
        raise ValueError('Cargo dependency graph does not select the staged engine packages')


def metadata_features(relative, fabric):
    if relative == 'sui':
        return ['typed-store/tidehunter', 'sui-node/sharded-wal' + ('-fabric' if fabric else '')]
    if relative == 'tidehunter-sharded-wal':
        return ['tidehunter/experimental-distributed', 'tidehunter/test-utils'] + (
            ['tidehunter/experimental-fabric'] if fabric else [])
    return ['fabric'] if fabric else []


def prepare(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = output / 'sources'
    identities = {name: snapshot(path.resolve(strict=True), sources / name)
                  for name, path in [('sui', args.sui), ('tidehunter-sharded-wal', args.engine)]}
    verify_staged(output, identities)
    manifests = bind_staged_engine(sources, identities, args.fabric)
    # Resolve only in owned copies, then archive the actual build inputs. Cargo
    # metadata preserves existing pins where possible and accommodates new paths.
    for relative in ('sui', 'tidehunter-sharded-wal', 'tidehunter-sharded-wal/experiments/sui-checkpoint/audit'):
        workspace = sources / relative
        stem = 'resolve-' + relative.replace('/', '-')
        metadata_path = output / (stem + '.json')
        features = metadata_features(relative, args.fabric)
        command = ['cargo', 'metadata', '--format-version', '1']
        if features:
            command.extend(['--features', ','.join(features)])
        with metadata_path.open('x') as metadata, (output / (stem + '.log')).open('xb') as log:
            bounded(command, cwd=workspace, stdout=metadata, stderr=log, timeout=900)
        verify_engine_metadata(json.loads(metadata_path.read_text()), manifests, args.fabric)
        lock = workspace / 'Cargo.lock'
        owner, _, subpath = relative.partition('/')
        lock_relative = str(Path(subpath) / 'Cargo.lock')
        identities[owner]['files'][lock_relative] = {'sha256': digest(lock), 'mode': lock.stat().st_mode & 0o777}
    verify_staged(output, identities)
    save(output / 'source-manifest.json', identities)
    with tarfile.open(output / 'sources.tar.gz', 'w:gz') as archive:
        archive.add(sources, arcname='sources')
    save(output / 'plan.json', {'schema': 1, 'source_archive_sha256': digest(output / 'sources.tar.gz'),
          'source_manifest_sha256': digest(output / 'source-manifest.json'), 'fabric': args.fabric,
          'jobs': args.jobs, 'profile': 'release', 'protocol': 9,
          'native_features': ['typed-store/tidehunter'], 'remote_features': ['sui-node/sharded-wal' + ('-fabric' if args.fabric else '')]})
    print(output / 'plan.json')


def verify_staged(root, identities):
    owners = {'sui', 'tidehunter-sharded-wal'}
    if set(identities) != owners or {p.name for p in (root / 'sources').iterdir()} != owners:
        raise ValueError('frozen sources must contain only Sui and the active engine')
    support = 'experiments/sui-checkpoint/checkpoint_support.py'
    entry = identities['tidehunter-sharded-wal']['files'].get(support, {})
    path = root / 'sources/tidehunter-sharded-wal' / support
    if 'sha256' not in entry or path.is_symlink() or not path.is_file():
        raise ValueError('checkpoint support must be an ordinary hash-bound engine source')
    for name in ('sui', 'tidehunter-sharded-wal'):
        expected = identities[name]['files']
        actual = {str(p.relative_to(root/'sources'/name)) for p in (root/'sources'/name).rglob('*') if p.is_file() or p.is_symlink()}
        if actual != set(expected):
            raise ValueError(f'frozen source inventory changed: {name}')
        for relative, entry in expected.items():
            path = root/'sources'/name/relative
            if ('sha256' in entry and digest(path) != entry['sha256']) or ('link' in entry and os.readlink(path) != entry['link']):
                raise ValueError(f'frozen source changed: {path}')


def cargo_executable(log, name, kind):
    """Select only the artifact Cargo emitted for this successful invocation."""
    paths = set()
    for line in log.read_text().splitlines():
        event = json.loads(line)
        target = event.get('target', {})
        if (event.get('reason') == 'compiler-artifact' and target.get('name') == name
                and kind in target.get('kind', []) and event.get('executable')):
            paths.add(Path(event['executable']).resolve(strict=True))
    if len(paths) != 1:
        raise ValueError(f'Cargo did not emit exactly one {kind} executable for {name}')
    return paths.pop()


def compiler_identity(cwd, env):
    # cwd matters: rustup honors the frozen workspace's toolchain file.
    rustc = env.get('RUSTC', 'rustc')
    return dict(cargo=subprocess.check_output(['cargo', '-vV'], cwd=cwd, env=env, timeout=30).decode(),
                rustc=subprocess.check_output([rustc, '-vV'], cwd=cwd, env=env, timeout=30).decode(),
                GIT_REVISION=env['GIT_REVISION'])


def build(args):
    root = args.output.resolve(strict=True)
    plan = json.loads((root / 'plan.json').read_text())
    if digest(root / 'sources.tar.gz') != plan['source_archive_sha256'] or digest(root / 'source-manifest.json') != plan['source_manifest_sha256']:
        raise ValueError('source archive/manifest binding differs')
    identities = json.loads((root / 'source-manifest.json').read_text())
    verify_staged(root, identities)
    bins = root / 'binaries'
    bins.mkdir()
    env = os.environ.copy()
    env['CARGO_BUILD_JOBS'] = str(plan['jobs'])
    env['USE_TIDEHUNTER'] = '1'
    env['GIT_REVISION'] = identities['sui']['revision']
    # Override Cargo config's compiler selection so the recorded probe is the
    # compiler used by this invocation (rustup still resolves by workspace cwd).
    env['RUSTC'] = shutil.which(env.get('RUSTC', 'rustc')) or env.get('RUSTC', 'rustc')
    cache = args.target.resolve()
    env['CARGO_TARGET_DIR'] = str(cache)
    commands = []
    def compile_one(label, cwd, arguments, artifact):
        command = ['cargo', 'build', '--release', '--locked', '--message-format=json-render-diagnostics', *arguments]
        identity = compiler_identity(cwd, env)
        started = time.time_ns()
        messages = root / f'build-{label}.jsonl'
        with messages.open('xb') as stdout, (root / f'build-{label}.log').open('xb') as stderr:
            bounded(command, cwd=cwd, env=env, stdout=stdout, stderr=stderr, timeout=args.deadline)
        source = cargo_executable(messages, Path(artifact).name, 'example' if artifact.startswith('examples/') else 'bin')
        shutil.copy2(source, bins / label)
        commands.append({'label': label, 'command': command, 'cwd': str(cwd), 'started_ns': started,
                         'finished_ns': time.time_ns(), 'sha256': digest(bins / label),
                         'artifact': str(source), 'compiler': identity,
                         'messages_sha256': digest(messages),
                         'log_sha256': digest(root / f'build-{label}.log')})
    sui = root / 'sources/sui'
    engine = root / 'sources/tidehunter-sharded-wal'
    compile_one('sui-node-native', sui, ['-p', 'sui-node', '--features', ','.join(plan['native_features'])], 'sui-node')
    compile_one('sui-node-remote', sui, ['-p', 'sui-node', '--features', ','.join(plan['remote_features'])], 'sui-node')
    compile_one('rocksdb-audit', sui, ['-p', 'sui-node', '--features', ','.join([*plan['native_features'], 'sui-node/testing']), '--example', 'distributed_storage_local_audit'], 'examples/distributed_storage_local_audit')
    features = 'experimental-distributed,test-utils' + (',experimental-fabric' if plan['fabric'] else '')
    compile_one('storage-node', engine, ['-p', 'tidehunter', '--example', 'sharded_wal_bench', '--features', features], 'examples/sharded_wal_bench')
    compile_one('transport-floor', engine, ['-p', 'tidehunter', '--example', 'sharded_wal_transport_bench', '--features', features], 'examples/sharded_wal_transport_bench')
    compile_one('state-audit', engine / 'experiments/sui-checkpoint/audit', ['--features', 'fabric'] if plan['fabric'] else [], 'sui-sharded-state-audit')
    verify_staged(root, identities)
    portability = {}
    for binary in bins.iterdir():
        portability[binary.name] = {
            'elf_versions': subprocess.check_output(['readelf', '--version-info', str(binary)], timeout=30).decode(),
            'dynamic_libraries': subprocess.check_output(['ldd', str(binary)], env=env, timeout=30).decode(),
        }
    save(root/'portability.json', portability)
    save(root / 'build-manifest.json', dict(plan, commands=commands,
         binaries={p.name: digest(p) for p in bins.iterdir()},
         environment={k: v for k, v in env.items() if k in ('USE_TIDEHUNTER', 'GIT_REVISION') or k.startswith(('CARGO_', 'RUST', 'CC', 'CXX', 'LD_LIBRARY_PATH', 'LIBRARY_PATH', 'PKG_CONFIG'))},
         resolved_locks={str(p.relative_to(root / 'sources')): digest(p) for p in (sui / 'Cargo.lock', engine / 'Cargo.lock')},
         executable_binding='only successful cargo invocations in this owned frozen source tree; never import old binaries'))
    with tarfile.open(root/'build-bundle.tar.gz','w:gz') as archive:
        for path in sorted(root.iterdir()):
            if path.name in ('build-bundle.tar.gz','sources'):
                continue
            archive.add(path,arcname=path.name)
    save(root/'build-receipt.json', {'bundle_sha256':digest(root/'build-bundle.tar.gz'),
         'build_manifest_sha256':digest(root/'build-manifest.json'),
         'source_archive_sha256':plan['source_archive_sha256']})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    prep = sub.add_parser('prepare')
    prep.add_argument('--sui', type=Path, required=True)
    prep.add_argument('--engine', type=Path, required=True)
    prep.add_argument('--output', type=Path, required=True)
    prep.add_argument('--fabric', action='store_true')
    prep.add_argument('--jobs', type=int, default=2)
    compile_parser = sub.add_parser('build')
    compile_parser.add_argument('--output', type=Path, required=True)
    compile_parser.add_argument('--target', type=Path, required=True)
    compile_parser.add_argument('--deadline', type=int, default=10800)
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise InterruptedError(f'build preparation received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    if args.action == 'prepare':
        if not 1 <= args.jobs <= 8:
            raise ValueError('build jobs must be 1..8')
        prepare(args)
    else:
        if not 60 <= args.deadline <= 21600:
            raise ValueError('build deadline must be 60..21600 seconds')
        build(args)


if __name__ == '__main__':
    main()
