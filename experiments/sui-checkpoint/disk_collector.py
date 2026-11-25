#!/usr/bin/env python3
"""Per-database coordinator disk evidence for the campaign manifest.

The coordinator index WAL uses `index_<16 hex>` file names and the shared WAL
uses `wal_<16 hex>` (`WalLayout::wal_file_name` in `tidehunter/src/wal/layout.rs`).
A fragment is a `frag_size` region inside such a file, not a separate file.
Allocated bytes are `st_blocks * 512`; apparent bytes are `st_size`. A
preallocated fragment is therefore reported as a lower bound derived from the
filesystem allocation, never as an engine allocation record.
"""
import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

sys.dont_write_bytecode = True

MIB = 1024 * 1024
INDEX_WAL = re.compile(r'^index_[0-9a-f]{16}$')
REPLAY_WAL = re.compile(r'^wal_[0-9a-f]{16}$')
LIMIT = 0.70


def classify(name):
    if INDEX_WAL.fullmatch(name):
        return 'index_wal'
    if REPLAY_WAL.fullmatch(name):
        return 'replay_wal'
    return 'other'


def free_space(path):
    status = os.statvfs(Path(path))
    return dict(free_bytes=status.f_bavail * status.f_frsize,
                total_bytes=status.f_blocks * status.f_frsize,
                fragment_size=status.f_frsize)


def _empty():
    return dict(files=0, apparent_bytes=0, allocated_bytes=0,
                apparent_fragment_slots=0, allocated_fragment_slots=0)


def _file_fragment_slots(size, allocated, fragment_bytes):
    """Fragment slots covered by the apparent length and fully allocated slots."""
    if fragment_bytes is None:
        return 0, 0
    apparent = (size + fragment_bytes - 1) // fragment_bytes
    allocated_slots = min(apparent, allocated // fragment_bytes)
    return apparent, allocated_slots


def sample(db_root, namespaces, *, fragment_bytes=None, now=None):
    """Sample one consistent inventory over every owned database directory."""
    if fragment_bytes is not None and (type(fragment_bytes) is not int or fragment_bytes <= 0):
        raise ValueError('fragment size must be a positive integer')
    root = Path(db_root)
    databases = {}
    for namespace in namespaces:
        relative = Path(namespace)
        if relative.is_absolute() or '..' in relative.parts or not relative.parts:
            raise ValueError('unsafe database namespace')
        directory = root / relative
        if not directory.is_dir():
            raise ValueError('database directory is absent: ' + namespace)
        classes = {}
        for entry in sorted(directory.iterdir()):
            if entry.is_symlink():
                raise ValueError('database tree contains a symlink: ' + str(entry))
            try:
                status = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not entry.is_file():
                continue
            row = classes.setdefault(classify(entry.name), _empty())
            apparent, allocated = status.st_size, status.st_blocks * 512
            row['files'] += 1
            row['apparent_bytes'] += apparent
            row['allocated_bytes'] += allocated
            if fragment_bytes:
                slots = _file_fragment_slots(apparent, allocated, fragment_bytes)
                row['apparent_fragment_slots'] += slots[0]
                row['allocated_fragment_slots'] += slots[1]
        total = _empty()
        for row in classes.values():
            for key in total:
                total[key] += row[key]
        databases[namespace] = dict(classes=classes, total=total,
                                    index_wal_files=classes.get('index_wal', {}).get('files', 0))
    return dict(sampled_unix_ns=int(now if now is not None else time.time_ns()),
                root=str(root), fragment_bytes=fragment_bytes,
                databases=databases,
                free=free_space(root),
                scope='coordinator database directories only; st_blocks*512 is filesystem allocation, '
                      'fragment slots are derived bounds, not engine preallocation records')


def budget(collection, *, limit=LIMIT):
    """Compare observed peaks with the owning root's free space."""
    if not isinstance(collection, dict) or 'databases' not in collection:
        raise ValueError('disk collection is malformed')
    apparent = sum(row['total']['apparent_bytes'] for row in collection['databases'].values())
    allocated = sum(row['total']['allocated_bytes'] for row in collection['databases'].values())
    free = collection['free']['free_bytes']
    if free <= 0:
        raise ValueError('owning root reports no free space')
    peak = max(apparent, allocated)
    return dict(apparent_bytes=apparent, allocated_bytes=allocated,
                projected_peak_bytes=peak, free_bytes=free,
                fraction=peak / free, limit=limit,
                within_limit=peak / free <= limit,
                note='projected peak is the larger of apparent and allocated bytes; '
                     'stop when it exceeds the configured share of free space')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--namespace', action='append', default=[], required=True,
                        help='database namespace relative to the coordinator root; repeat once per database')
    parser.add_argument('--fragment-mib', type=int,
                        help='production fragment size in MiB (campaign geometry is 1024)')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.fragment_mib is not None and args.fragment_mib <= 0:
        raise ValueError('fragment size must be positive')
    collection = sample(args.root, args.namespace,
                        fragment_bytes=args.fragment_mib * MIB if args.fragment_mib else None)
    collection['budget'] = budget(collection)
    text = json.dumps(collection, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(text + '\n')
    else:
        print(text)


if __name__ == '__main__':
    main()
