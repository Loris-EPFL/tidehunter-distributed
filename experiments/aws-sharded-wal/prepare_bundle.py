#!/usr/bin/env python3
"""Build a source-bound sharded-WAL payload locally. This makes no AWS calls."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_source_manifest(manifest):
    sources = manifest['source_sha256']
    if not isinstance(sources, dict) or not sources:
        raise ValueError('source manifest must bind a nonempty file set')
    for name in sources:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or str(path) != name:
            raise ValueError('unsafe source manifest path')
    tree = hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if tree != manifest['source_tree_sha256']:
        raise ValueError('source manifest tree hash differs from its file identities')
    return sources


def validate_source_archive(path, manifest):
    sources = validate_source_manifest(manifest)
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        if sum(member.size for member in members) > 512 * 1024 * 1024:
            raise ValueError('source archive exceeds expanded size bound')
        seen = set()
        for member in members:
            name = Path(member.name)
            if name.is_absolute() or '..' in name.parts or not (member.isfile() or member.isdir()):
                raise ValueError('unsafe source archive member')
            if member.isdir():
                continue
            if member.name not in sources or member.name in seen:
                raise ValueError('source archive contains unbound or duplicate files')
            seen.add(member.name)
            with archive.extractfile(member) as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != sources[member.name]:
                    raise ValueError('source archive differs from manifest: ' + member.name)
        if seen != set(sources):
            raise ValueError('source archive omits manifest-bound files')


def prepare(args):
    repo = Path(__file__).resolve().parents[2]
    for binary, manifest, current in [(args.current_binary, args.current_manifest, True),
                                      (args.baseline_binary, args.baseline_manifest, False)]:
        data = json.loads(manifest.read_text())
        validate_source_manifest(data)
        if sha(binary) != data['binary_sha256']:
            raise ValueError(f'binary differs from its manifest: {binary}')
        if current:
            for path, expected in data['source_sha256'].items():
                if sha(repo / path) != expected:
                    raise ValueError(f'current source differs from build manifest: {path}')
    output = args.output.resolve()
    output.mkdir()
    payload = output / 'bundle'
    payload.mkdir()
    artifacts = {}
    for label, binary, manifest, source in [
        ('baseline', args.baseline_binary, args.baseline_manifest, args.baseline_source),
        ('current', args.current_binary, args.current_manifest, None),
    ]:
        target = payload / label
        target.mkdir()
        shutil.copy2(binary, target / 'sharded_wal_bench')
        shutil.copy2(manifest, target / 'source-manifest.json')
        data = json.loads(manifest.read_text())
        for name, key in [('sharded_wal_transport_bench', 'floor_binary_sha256'),
                          ('sharded_wal_recovery_bench', 'recovery_binary_sha256')]:
            if key not in data:
                continue
            artifact = manifest.parent / name
            if sha(artifact) != data[key]:
                raise ValueError(f'auxiliary executable differs from manifest: {name}')
            shutil.copy2(artifact, target / name)
        if source:
            shutil.copy2(source, target / 'source.tar.gz')
        else:
            with tarfile.open(target / 'source.tar.gz', 'w:gz') as archive:
                sources = json.loads(manifest.read_text())['source_sha256']
                for name in sorted(sources):
                    archive.add(repo / name, arcname=name, recursive=False)
        copied_manifest = json.loads((target / 'source-manifest.json').read_text())
        validate_source_archive(target / 'source.tar.gz', copied_manifest)
        if sha(target / 'sharded_wal_bench') != copied_manifest['binary_sha256']:
            raise ValueError('bundled executable differs from source manifest')
        artifacts[label] = {
            'binary_sha256': sha(target / 'sharded_wal_bench'),
            'manifest_sha256': sha(target / 'source-manifest.json'),
            'source_archive_sha256': sha(target / 'source.tar.gz'),
        }
    helpers = payload / 'runner'
    helpers.mkdir()
    for script in ['guest.py', 'bootstrap.py', 'analyze_cloud.py', 'efa_progress.py', 'checkpoint_async.py', 'cluster_ram.py', 'scalar_transport.py', 'transport_floor.py', 'stage_timing.py', 'local_compare.py', 'prepare_bundle.py']:
        shutil.copy2(Path(__file__).with_name(script), helpers / script)
    recovery_source = repo / 'tidehunter/examples/sharded_wal_recovery_bench.rs'
    overlay = payload / 'recovery-overlay'
    overlay.mkdir()
    shutil.copy2(recovery_source, overlay / recovery_source.name)
    manifest = {
        'schema': 1,
        'source_repository': 'tidehunter-sharded-wal',
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
        'builds': artifacts,
        'files_sha256': {str(p.relative_to(payload)): sha(p) for p in sorted(payload.rglob('*')) if p.is_file()},
        'aws_operations': False,
    }
    (payload / 'bundle-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    archive_path = output / 'bundle.tar.gz'
    with tarfile.open(archive_path, 'w:gz') as archive:
        archive.add(payload, arcname='bundle')
    (output / 'upload-manifest.json').write_text(json.dumps({
        'archive': archive_path.name, 'sha256': sha(archive_path),
        'bytes': archive_path.stat().st_size,
        'bundle_manifest_sha256': sha(payload / 'bundle-manifest.json'),
    }, indent=2) + '\n')
    print(json.dumps({'archive': str(archive_path), 'sha256': sha(archive_path),
                      'bytes': archive_path.stat().st_size, 'aws_operations': False}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['current-binary', 'current-manifest', 'baseline-binary', 'baseline-manifest', 'baseline-source', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    prepare(parser.parse_args())


if __name__ == '__main__':
    main()
