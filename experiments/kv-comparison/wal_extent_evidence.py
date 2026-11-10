"""Read-only FIEMAP attribution for diagnostic block traces, not raw-device I/O.

Linux ABI: https://docs.kernel.org/filesystems/fiemap.html and
include/uapi/linux/fiemap.h. No FIEMAP_FLAG_SYNC: never add a foreground fsync.
A stable extent match identifies an owned WAL block request, not an API caller
or NAND access. Shared, encoded, delayed and unwritten extents are excluded.
"""
import fcntl
from itertools import islice
import os
from pathlib import Path
import re
import struct
import stat

HEADER = struct.Struct("=QQIIII")
EXTENT = struct.Struct("=QQQQQIIII")
FS_IOC_FIEMAP = 0xC020660B
MAX_FILES = 32
MAX_EXTENTS = 128
ALLOWED_FLAGS = 0x00000001 | 0x00001000  # LAST and MERGED only.


def open_directory(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or len(path.parts) > 128:
        raise ValueError("WAL root must be an absolute bounded directory path")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open('/', flags)
    try:
        for name in path.parts[1:]:
            previous = fd
            fd = os.open(name, flags, dir_fd=previous)
            os.close(previous)
        return fd
    except BaseException:
        os.close(fd)
        raise


def file_extents(path, partition_start, *, dir_fd=None, label=None):
    if dir_fd is None:
        parent = open_directory(Path(path).parent)
        try:
            return file_extents(Path(path).name, partition_start, dir_fd=parent, label=str(path))
        finally:
            os.close(parent)
    if Path(path).name != str(path):
        raise ValueError("WAL descriptor-relative open requires one filename")
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("WAL extent source is not a regular file")
        raw = bytearray(HEADER.size + MAX_EXTENTS * EXTENT.size)
        HEADER.pack_into(raw, 0, 0, (1 << 64) - 1, 0, 0, MAX_EXTENTS, 0)
        fcntl.ioctl(fd, FS_IOC_FIEMAP, raw, True)
        _, _, flags, mapped, count, _ = HEADER.unpack_from(raw)
        if flags or mapped > MAX_EXTENTS or count != MAX_EXTENTS:
            raise ValueError("unexpected FIEMAP response flags or extent count")
        extents, excluded = [], []
        for index in range(mapped):
            logical, physical, length, _, _, bits, _, _, _ = EXTENT.unpack_from(raw, HEADER.size + index * EXTENT.size)
            if bits & ~ALLOWED_FLAGS or physical % 512 or length % 512 or not length:
                excluded.append({"logical": logical, "length": length, "flags": bits})
            else:
                extents.append({"logical": logical, "sector": physical // 512 + partition_start,
                                "sectors": length // 512, "flags": bits})
        after = os.fstat(fd)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if identity(before) != identity(after):
            raise ValueError("WAL changed during FIEMAP snapshot")
        last = EXTENT.unpack_from(raw, HEADER.size + (mapped - 1) * EXTENT.size)[5] if mapped else 0
        return {"path": label or str(path), "identity": list(identity(after)), "extents": extents,
                "excluded": excluded, "truncated": bool(mapped == MAX_EXTENTS and not last & 1)}
    finally:
        os.close(fd)


def snapshot(root, mapping):
    """Bound directory discovery to real wal_<16 hex> files; never follow links."""
    try:
        root = Path(root)
        original = Path('/sys/dev/block') / mapping['mount_device']
        start = int((original / 'start').read_text()) if mapping['partition'] else 0
        disk = Path(mapping['sysfs_path'])
        diskseq = int((disk / 'diskseq').read_text())
        boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if start < 0 or diskseq <= 0 or (disk / 'dev').read_text().strip() != mapping['device']:
            raise ValueError("unverified block-device identity")
        root_fd = open_directory(root)
        files, pending, scanned, truncated = [], [(root_fd, root, 0)], 0, False
        try:
            root_stat = os.fstat(root_fd)
            while pending and scanned < 1024 and len(files) < MAX_FILES:
                fd, directory, depth = pending.pop()
                try:
                    with os.scandir(fd) as entries:
                        rows = sorted(entry.name for entry in islice(entries, 1025 - scanned))
                    if len(rows) > 1024 - scanned:
                        rows = rows[:1024 - scanned]
                        truncated = True
                    scanned += len(rows)
                    for name in rows:
                        mode = os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode
                        if stat.S_ISDIR(mode):
                            if depth < 5 and len(pending) < 64:
                                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                                pending.append((child, directory / name, depth + 1))
                            else:
                                truncated = True
                        elif stat.S_ISREG(mode) and re.fullmatch(r"wal_[0-9a-f]{16}", name):
                            files.append(file_extents(name, start, dir_fd=fd, label=str(directory / name)))
                            if len(files) >= MAX_FILES:
                                truncated = True
                                break
                finally:
                    os.close(fd)
        finally:
            truncated = truncated or bool(pending)
            for fd, _, _ in pending:
                os.close(fd)
        files.sort(key=lambda row: row['path'])
        if not files:
            raise ValueError("no bounded owned WAL files were discovered")
        if any(row['identity'][0] != os.makedev(*map(int, mapping['mount_device'].split(':'))) for row in files):
            raise ValueError("WAL files cross the selected filesystem device")
        return {"available": True, "device": mapping['device'], "diskseq": diskseq,
                "boot_id": boot_id, "root_identity": [root_stat.st_dev, root_stat.st_ino],
                "partition_start_sector": start, "files": files, "selection_truncated": truncated,
                "scope": "owned WAL filename/extent subset; no raw-device access or per-API attribution"}
    except (OSError, ValueError, KeyError) as error:
        return {"available": False, "reason": type(error).__name__ + ': ' + str(error)}


def classify(parsed, before, after):
    unavailable = {"available": False, "qualified": False,
                   "scope": "owned-WAL block requests, not per-API or NAND service"}
    if not before.get('available') or not after.get('available'):
        return unavailable | {"reason": "FIEMAP/file/device evidence unavailable"}
    if before != after:
        return unavailable | {"reason": "WAL extent, file or device identity changed"}
    if not parsed.get('qualified'):
        return unavailable | {"reason": "request-identity trace lost, incomplete, ambiguous or erroneous"}
    ranges = sorted((e['sector'], e['sector'] + e['sectors'])
                    for row in before['files'] for e in row['extents'])
    merged = []
    for start, stop in ranges:
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], stop)
        else:
            merged.append([start, stop])
    geometry, duplicate, pairs = {}, set(), []
    for line in parsed.get('raw_events', []):
        parts = line.split()
        if parts[0] == 'S':
            if len(parts) != 4 or not all(p.isdigit() for p in parts[1:]):
                return unavailable | {"reason": "malformed request sector geometry"}
            timestamp, sector, size = map(int, parts[1:])
            if timestamp in geometry:
                duplicate.add(timestamp)
            geometry[timestamp] = (sector, size)
        elif parts[0] == 'C':
            pairs.append((int(parts[1]), int(parts[2])))
    if not geometry:
        return unavailable | {"reason": "trace has no recorded request sector geometry"}
    intervals, read_bytes, unattributed = [], 0, 0
    for begin, end in pairs:
        if begin not in geometry or begin in duplicate:
            unattributed += 1
            continue
        sector, size = geometry[begin]
        if not size or size % 512:
            return unavailable | {"reason": "unaligned request sector geometry"}
        stop = sector + size // 512
        if any(lo <= sector and stop <= hi for lo, hi in merged):
            intervals.append(end - begin)
            read_bytes += size
        else:
            unattributed += 1
    return {"available": True, "qualified": bool(intervals), "read_ops": len(intervals),
            "read_bytes": read_bytes, "unattributed_requests": unattributed,
            "ambiguous_start_timestamps": len(duplicate), "raw_intervals_ns": intervals,
            "selected_wal_files": len(before['files']),
            "extent_selection_complete": not before['selection_truncated'] and all(
                not row['excluded'] and not row['truncated'] for row in before['files']),
            "scope": "stable owned-WAL extent block issue-to-completion within collector window; "
            "not per-API, page-cache-miss rate, pure RDMA or NAND service"}
