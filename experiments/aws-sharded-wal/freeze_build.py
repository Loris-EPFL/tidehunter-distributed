#!/usr/bin/env python3
"""Build a frozen release benchmark and bind it to every Rust/Cargo input."""
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def sources(repo):
    names = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=repo)
    return {name: sha(repo / name) for name in sorted(set(names.decode().strip('\0').split('\0')))
            if name.endswith(('.rs', '.toml')) or Path(name).name in ['Cargo.lock', 'rust-toolchain']}


def main():
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=repo / 'logs/optimization-2026-10-06/current')
    parser.add_argument('--features', choices=['experimental-distributed', 'experimental-distributed,test-utils', 'experimental-fabric,test-utils'],
                        default='experimental-distributed')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir()
    before = sources(repo)
    (output / 'source-before.json').write_text(json.dumps(before, indent=2) + '\n')
    command = ['cargo', 'build', '--release', '--offline', '--locked', '-p', 'tidehunter',
               '--features', args.features, '--example', 'sharded_wal_bench']
    with (output / 'build.log').open('w') as log:
        subprocess.run(command, cwd=repo, env=os.environ | {'CARGO_BUILD_JOBS': '2'},
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    auxiliary = {}
    if 'test-utils' in args.features:
        for name, prefix in [('sharded_wal_transport_bench', 'floor'),
                             ('sharded_wal_recovery_bench', 'recovery')]:
            extra = command[:-1] + [name]
            if prefix == 'recovery':
                extra[1] = 'rustc'
                extra += ['--', '--check-cfg', 'cfg(checkpoint_recovery_stats)',
                          '--cfg', 'checkpoint_recovery_stats']
            with (output / f'build-{prefix}.log').open('w') as log:
                subprocess.run(extra, cwd=repo, env=os.environ | {'CARGO_BUILD_JOBS': '2'},
                               stdout=log, stderr=subprocess.STDOUT, check=True)
            artifact = output / name
            shutil.copy2(repo / 'target/release/examples' / name, artifact)
            auxiliary.update({prefix + '_binary_sha256': sha(artifact),
                              prefix + '_build_command': extra})
    after = sources(repo)
    if before != after:
        raise ValueError('source changed while building; result cannot be deployed')
    binary = output / 'sharded_wal_bench'
    shutil.copy2(repo / 'target/release/examples/sharded_wal_bench', binary)
    manifest = {'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
                'branch': subprocess.check_output(['git', 'branch', '--show-current'], cwd=repo, text=True).strip(),
                'dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo)),
                'source_sha256': after,
                'source_tree_sha256': hashlib.sha256(json.dumps(after, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                'build_command': command, 'features': args.features,
                'binary_sha256': sha(binary), 'binary_bytes': binary.stat().st_size,
                'rustc': subprocess.check_output(['rustc', '--version'], cwd=repo, text=True).strip(),
                'topology': 'Five local child processes share one NVMe; same source prepared for five separate RF1 TCP nodes'}
    manifest.update(auxiliary)
    if 'experimental-fabric' in args.features:
        library = Path(subprocess.check_output(['pkg-config', '--variable=libdir', 'libfabric'], text=True).strip()) / 'libfabric.so.1'
        manifest['libfabric'] = {'version': subprocess.check_output(['pkg-config', '--modversion', 'libfabric'], text=True).strip(),
                                'path': str(library.resolve()), 'sha256': sha(library.resolve())}
    (output / 'source-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({k: v for k, v in manifest.items() if k != 'source_sha256'}, indent=2))


if __name__ == '__main__':
    main()
