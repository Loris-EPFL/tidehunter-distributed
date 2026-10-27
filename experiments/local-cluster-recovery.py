#!/usr/bin/env python3
"""Exercise the real recovery executable with five local owned storage processes; no AWS."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / 'aws-sharded-wal'))
import cluster_ram
import local_compare
import portable_transport




def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--recovery', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--transport', choices=('tcp', 'fabric-tcp'), default='tcp')
    args = parser.parse_args()
    bench, recovery = args.benchmark.resolve(strict=True), args.recovery.resolve(strict=True)
    manifest = json.loads(args.manifest.read_text())
    if sha(bench) != manifest['binary_sha256']:
        raise ValueError('benchmark differs from frozen source manifest')
    repo = HERE.parent
    for name, digest in manifest['source_sha256'].items():
        if sha(repo / name) != digest:
            raise ValueError('recovery source differs from frozen benchmark: ' + name)
    timeout = shutil.which('timeout')
    if timeout is None:
        raise RuntimeError('GNU timeout required')
    args.output.mkdir()
    requested_configs = args.output / 'requested-configs'
    requested_configs.mkdir()
    identity = dict(benchmark_sha256=sha(bench), recovery_sha256=sha(recovery),
                    source_tree_sha256=manifest['source_tree_sha256'], transport=args.transport,
                    topology='five RF1 local processes share one SSD; process restart, not reboot or OS eviction')
    (args.output / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    rows = []
    for restart in ('coordinator', 'full'):
        for tail in (0, 128):
            output = args.output / f'{restart}-tail{tail}'
            output.mkdir()
            with tempfile.TemporaryDirectory(prefix='owned-recovery-', dir=output) as directory:
                root = Path(directory).resolve() / 'case'
                spec = cluster_ram.case('recovery', 'current', 8192, 0, args.transport)
                transport_config = requested_configs / f'{restart}-tail{tail}' / 'transport.json'
                transport_config.parent.mkdir()
                portable_transport.write_transport_config(
                    transport_config, portable_transport.transport_for_case(args.transport)
                )
                spec.update(recovery=True, restart=restart, checkpoint_batches=64,
                            tail_batches=tail, modes=['recovery'], cold=False)
                # Reserve distinct loopback ports until the exact launch files exist.
                sockets = [socket.socket() for _ in range(5)]
                try:
                    for listener in sockets:
                        listener.bind(('127.0.0.1', 0))
                    endpoints = ','.join(f'127.0.0.1:{s.getsockname()[1]}' for s in sockets)
                    command = [str(bench), '--prepare-external-case', str(root),
                               '--guest-case-root', str(root), '--remote-endpoints', endpoints]
                    command += cluster_ram.flags(
                        spec, 'tcp', prepare=True, transport_config=transport_config
                    )
                    local_compare.run([timeout, '60s', *command], output / 'prepare')
                finally:
                    for listener in sockets:
                        listener.close()
                (output / 'remote.yaml').write_bytes((root / 'remote.yaml').read_bytes())
                nodes, streams, generations = [], [], []

                def start_nodes(generation):
                    started = []
                    for partition in range(5):
                        node = root / f'node-{partition}'
                        (node / 'ready').unlink(missing_ok=True)
                        (node / 'stop').unlink(missing_ok=True)
                        stream = (output / f'node-{partition}-{generation}.log').open('wb')
                        streams.append(stream)
                        process = subprocess.Popen([str(bench), '--experimental-storage-node', str(node / 'launch.yaml')],
                                                   stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                        nodes.append((process, node))
                        started.append(dict(partition=partition, pid=process.pid))
                    for _ in range(600):
                        if any(p.poll() is not None for p, _ in nodes):
                            raise RuntimeError('owned storage process exited before readiness')
                        if all((node / 'ready').exists() for _, node in nodes):
                            break
                        time.sleep(0.05)
                    else:
                        raise TimeoutError('owned storage readiness exceeded 30 seconds')
                    generations.append(started)

                def stop_nodes(abrupt=False):
                    for process, node in nodes:
                        if abrupt:
                            process.kill()
                        else:
                            (node / 'stop').touch()
                    errors = []
                    for process, _ in nodes:
                        try:
                            code = process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                            errors.append('graceful storage exit deadline')
                        else:
                            if not abrupt and code != 0:
                                errors.append(f'owned storage exit {code}')
                    nodes.clear()
                    if errors:
                        raise RuntimeError('; '.join(errors))

                try:
                    start_nodes('initial')
                    for action in ('seed', 'reopen'):
                        if action == 'reopen' and restart == 'full':
                            stop_nodes(abrupt=True)
                            start_nodes('restart')
                        destination = output / action
                        command = [str(recovery), '--action', action, '--remote-config', str(root / 'remote.yaml'),
                                   '--output', str(destination), '--value-bytes', '8192',
                                   '--checkpoint-batches', '64', '--tail-batches', str(tail)]
                        local_compare.run([timeout, '--signal=TERM', '--kill-after=5s', '120s', *command], destination)
                        row = json.loads((destination / 'recovery.json').read_text())
                        cluster_ram.validate_recovery_json(row, spec, action)
                        if not (destination / 'COMPLETE').is_file():
                            raise ValueError('recovery action did not complete its audit')
                        rows.append(dict(restart=restart, tail=tail, **row))
                    stop_nodes()
                finally:
                    for process, _ in nodes:
                        if process.poll() is None:
                            process.kill()
                        process.wait(timeout=5)
                    for stream in streams:
                        stream.close()
                    (output / 'owned-processes.json').write_text(json.dumps(generations, indent=2) + '\n')
                (output / 'COMPLETE').write_text('verified checkpoint/tail/retry and reaped every owned storage process\n')
    if sha(bench) != identity['benchmark_sha256'] or sha(recovery) != identity['recovery_sha256']:
        raise ValueError('executable changed during recovery campaign')
    (args.output / 'measurements.json').write_text(json.dumps(rows, indent=2) + '\n')
    (args.output / 'COMPLETE').write_text('all four local recovery cases passed\n')
    print(json.dumps(dict(complete=True, cases=4, transport=args.transport)))


if __name__ == '__main__':
    main()
