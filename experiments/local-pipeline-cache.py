#!/usr/bin/env python3
"""Local matched pipeline/cache ablations. No cloud operations; five shared-SSD nodes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

import sys


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import portable_transport

spec = importlib.util.spec_from_file_location('local_compare', HERE / 'aws-sharded-wal/local_compare.py')
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 5:
        raise ValueError('bounded repetition count required')
    args.output.mkdir()
    transport_config = args.output / 'requested-configs' / 'transport.json'
    transport_config.parent.mkdir()
    portable_transport.write_transport_config(
        transport_config, portable_transport.transport_for_case('tcp')
    )
    binaries = {name: getattr(args, name).resolve() for name in ('baseline', 'candidate')}
    hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
    configs = [
        ('previous-tcp', 'baseline', ['--backend', 'tcp']),
        ('control-tcp', 'candidate', ['--backend', 'tcp', '--tcp-pipeline', 'false', '--tcp-read-cache-bytes', '0']),
        ('pipeline-tcp', 'candidate', ['--backend', 'tcp', '--tcp-pipeline', 'true', '--tcp-read-cache-bytes', '0']),
        ('cache-pipeline-tcp', 'candidate', ['--backend', 'tcp', '--tcp-pipeline', 'true', '--tcp-read-cache-bytes', str(8 * 1024 * 1024)]),
        ('previous-native', 'baseline', ['--backend', 'native-sync']),
        ('current-native', 'candidate', ['--backend', 'native-sync']),
    ]
    common = ['--repetitions', '1', '--windows', '1,8,32', '--values', '1024,8192', '--batch-sizes', '1,32', '--batches', '128', '--mixed', '32', '--reads', '512']
    plan = {'binary_sha256': hashes, 'binaries': {k: str(v) for k, v in binaries.items()}, 'configs': configs, 'common': common, 'repetitions': args.repetitions, 'transport_config': str(transport_config), 'topology': 'five loopback node processes on one SSD; no RDMA; all reads verify complete values'}
    (args.output / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    for repetition in range(args.repetitions):
        order = configs[repetition:] + configs[:repetition]
        if repetition % 2:
            order = list(reversed(order))
        for name, binary, flags in order:
            if hashlib.sha256(binaries[binary].read_bytes()).hexdigest() != hashes[binary]:
                raise ValueError('benchmark executable changed')
            output = args.output / f'{name}-r{repetition}'
            runtime_transport = (
                ['--transport-config', str(transport_config)] if binary == 'candidate' else []
            )
            local.run([str(binaries[binary]), '--output', str(output), *common,
                       *runtime_transport, *flags], output)
            if not (output / 'COMPLETE').exists():
                raise ValueError('missing correctness completion')
    # A separate identical repeated read trace distinguishes first-fill misses
    # from truly hot reference-cache hits, with matching native/control workloads.
    for repetition in range(args.repetitions):
        hot = [configs[1], configs[3], configs[5]]
        if repetition % 2:
            hot.reverse()
        for name, binary, flags in hot:
            output = args.output / f'hot-{name}-r{repetition}'
            runtime_transport = (
                ['--transport-config', str(transport_config)] if binary == 'candidate' else []
            )
            local.run([str(binaries[binary]), '--output', str(output),
                       '--values', '1024,8192', '--batch-sizes', '32', '--windows', '32',
                       '--repetitions', '1', '--hot-read-pass', 'true',
                       *runtime_transport, *flags], output)
            if not (output / 'COMPLETE').exists():
                raise ValueError('missing hot-trace correctness completion')
    (args.output / 'COMPLETE').write_text('all matched ablations and separately labelled hot traces verified\n')


if __name__ == '__main__':
    main()
