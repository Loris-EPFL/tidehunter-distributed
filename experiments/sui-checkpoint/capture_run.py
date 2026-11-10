#!/usr/bin/env python3
"""Run the source-frozen paired replay with opt-in measured-node KV capture.

The producer, shard servers, and auditors do not receive the capture switch.
The unchanged runner still owns process groups, metrics, deadlines and audits.
Stop admission and drain diagnostics before the runner terminates each node.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
import run as replay_runner


def status(path):
    rows = dict(line.split('\t', 1) for line in path.read_text().splitlines())
    return {key: int(value) for key, value in rows.items()}


def validate(directory):
    if (directory / 'writer.error').exists():
        raise ValueError(f'capture writer failed: {(directory / "writer.error").read_text()}')
    result = status(directory / 'final.status')
    if (result['accepted'] != result['written'] or result['dropped_full']
            or result['dropped_closed'] or result['active']):
        raise ValueError(f'invalid capture: {result}')
    return result


def run(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('--capture-output', required=True, type=Path)
    options, remaining = parser.parse_known_args(argv)
    destination = options.capture_output.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    original = replay_runner.Owned

    class CapturedOwned(original):
        def __init__(self, root, env):
            super().__init__(root, env)
            self.captures = {}

        def spawn(self, command, stdout, stderr, extra=None):
            command = list(map(str, command))
            extra = dict(extra or {})
            mode = None
            if '--config-path' in command:
                config = Path(command[command.index('--config-path') + 1])
                if config.name in ('native.yaml', 'remote.yaml'):
                    mode = config.stem
                    extra['SUI_TH_KV_CAPTURE_DIR'] = str(destination / mode)
            child = super().spawn(command, stdout, stderr, extra)
            if mode:
                self.captures[child.pid] = destination / mode
            return child

        def stop(self, child):
            directory = self.captures.pop(child.pid, None)
            error = None
            if directory is not None:
                try:
                    if not directory.is_dir():
                        raise ValueError(f'measured node never opened capture: {directory}')
                    (directory / 'stop').write_text('drain\n')
                    end = time.monotonic() + 30
                    while not (directory / 'final.status').exists():
                        if (directory / 'writer.error').exists() or child.poll() is not None or time.monotonic() >= end:
                            raise ValueError(f'capture failed to drain: {directory}')
                        time.sleep(.02)
                    receipt = validate(directory)
                    (directory / 'controller.json').write_text(json.dumps({
                        'valid': True, 'status': receipt, 'drained_unix_ns': time.time_ns(),
                        'cutoff': 'after final node metrics, before SIGTERM',
                    }, indent=2) + '\n')
                except BaseException as failure:
                    error = failure
                    (destination / f'{directory.name}-failure.txt').write_text(str(failure) + '\n')
            try:
                super().stop(child)
            finally:
                if error is not None:
                    raise error

    replay_runner.Owned = CapturedOwned
    previous = sys.argv
    sys.argv = [str(Path(replay_runner.__file__)), *remaining]
    try:
        return replay_runner.main()
    finally:
        replay_runner.Owned = original
        sys.argv = previous


if __name__ == '__main__':
    run()
