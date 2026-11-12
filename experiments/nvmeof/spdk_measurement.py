"""Admit owner-local SPDK counters at the drained API measurement boundaries.

These counters measure file-bdev completion, not pure RDMA, NAND service or
per-API latency. Cumulative extrema cannot produce interval percentiles.
"""
import hashlib
import json
from pathlib import Path
import stat

COUNTERS = ("bytes_read", "num_read_ops", "bytes_written", "num_write_ops", "read_latency_ticks")
IDENTITY = ("campaign_id", "unit_name", "pid", "process_start_ticks", "binary_sha256")


def _snapshot(directory, binding, boundary, *, owner_count=5):
    if type(owner_count) is not int or not 1 <= owner_count <= 5:
        raise ValueError("invalid SPDK owner count")
    name = "spdk-target-" + boundary + ".json"
    if not isinstance(binding, dict) or binding.get("path") != name:
        raise ValueError("SPDK boundary binding has an unexpected path")
    path = Path(directory) / name
    if not stat.S_ISREG(path.lstat().st_mode) or not 0 < path.stat().st_size <= 16 << 20:
        raise ValueError("SPDK boundary is not a bounded ordinary file")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != binding.get("sha256"):
        raise ValueError("SPDK boundary differs from its integrity binding")
    rows = json.loads(raw)
    if not isinstance(rows, list) or len(rows) != owner_count:
        raise ValueError("SPDK boundary requires all active owners")
    result = {}
    for row in rows:
        index = row.get("index") if isinstance(row, dict) else None
        if type(index) is not int or index not in range(owner_count) or index in result or row.get("complete") is not True:
            raise ValueError("SPDK owner boundary identity is missing or duplicated")
        target = row.get("target")
        if (not isinstance(target, dict) or target.get("kind") != "spdk-target-stats"
                or target.get("target_implementation") != "spdk"
                or any(target.get(k) in (None, "") for k in IDENTITY)
                or any(type(target.get(k)) is not int or target[k] <= 0 for k in ("pid", "process_start_ticks"))):
            raise ValueError("SPDK target process identity is unavailable")
        digest = target["binary_sha256"]
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("SPDK target binary digest is invalid")
        stats = target.get("bdev_iostat")
        if (not isinstance(stats, dict) or any(type(stats.get(k)) is not int or stats[k] <= 0
                for k in ("ticks", "tick_rate")) or not isinstance(stats.get("bdevs"), list) or not stats["bdevs"]):
            raise ValueError("SPDK actual bdev counters are unavailable")
        bdevs = {}
        for bdev in stats["bdevs"]:
            name = bdev.get("name") if isinstance(bdev, dict) else None
            if (not isinstance(name, str) or not name or name in bdevs
                    or any(type(bdev.get(k)) is not int or bdev[k] < 0 for k in COUNTERS)):
                raise ValueError("SPDK bdev counters are invalid or duplicated")
            errors = bdev.get("io_error")
            if (not isinstance(errors, dict) or len(errors) > 64
                    or any(not isinstance(k, str) or type(v) is not int or v < 0 for k, v in errors.items())):
                raise ValueError("SPDK actual I/O error counters are unavailable")
            bdevs[name] = bdev
        result[index] = (target, stats, bdevs)
    return result


def admit(directory, binding, *, allow_zero_reads=False, owner_count=5):
    """Reject restarts, resets, errors, writes, mixed binaries and missing samples."""
    if type(allow_zero_reads) is not bool:
        raise ValueError("invalid SPDK zero-read policy")
    if not isinstance(binding, dict):
        raise ValueError("SPDK target measurement binding is unavailable")
    start = _snapshot(directory, binding.get("start"), "start", owner_count=owner_count)
    end = _snapshot(directory, binding.get("end"), "end", owner_count=owner_count)
    owners, binaries = [], set()
    reads = total_bytes = 0
    latency_seconds = 0.0
    for index in range(owner_count):
        before, first, bdevs = start[index]
        after, last, completed = end[index]
        if any(before[k] != after[k] for k in IDENTITY):
            raise ValueError("SPDK target process changed during measurement")
        binaries.add(before["binary_sha256"])
        if first["tick_rate"] != last["tick_rate"] or last["ticks"] <= first["ticks"] or bdevs.keys() != completed.keys():
            raise ValueError("SPDK counter clock or bdev inventory changed")
        totals = dict.fromkeys(COUNTERS, 0)
        for name, initial in bdevs.items():
            final = completed[name]
            for field in COUNTERS:
                delta = final[field] - initial[field]
                if delta < 0:
                    raise ValueError("SPDK bdev counters reset during measurement")
                totals[field] += delta
            # SPDK emits the present io_error object but omits its zero entries.
            for status in initial["io_error"].keys() | final["io_error"].keys():
                delta = final["io_error"].get(status, 0) - initial["io_error"].get(status, 0)
                if delta != 0:
                    raise ValueError("SPDK I/O errors or an error-counter reset occurred during measurement")
        if totals["bytes_written"] or totals["num_write_ops"]:
            raise ValueError("SPDK sealed read-only target observed a write")
        count = totals["num_read_ops"]
        if (bool(count) != bool(totals["bytes_read"])
                or bool(count) != bool(totals["read_latency_ticks"])):
            raise ValueError("SPDK read counts, bytes and latency disagree")
        seconds = totals["read_latency_ticks"] / first["tick_rate"]
        reads += count
        total_bytes += totals["bytes_read"]
        latency_seconds += seconds
        owners.append({"index": index, "process": {k: before[k] for k in IDENTITY},
                       "observed_interval_seconds": (last["ticks"] - first["ticks"]) / first["tick_rate"],
                       "read_ops": count, "read_bytes": totals["bytes_read"],
                       "mean_read_completion_us": seconds * 1e6 / count if count else None})
    if (not reads and not allow_zero_reads) or len(binaries) != 1:
        raise ValueError("SPDK measurement has no actual reads or has mixed target binaries")
    return {"available": True, "read_ops": reads, "read_bytes": total_bytes,
            "binary_sha256": binaries.pop(), "mean_read_completion_us": latency_seconds * 1e6 / reads if reads else None,
            "owners": owners, "scope": "owner-local file-bdev completion; excludes verification and setup; "
            "not per-API, pure fabric or NAND latency; no interval percentile from cumulative extrema"}


LIVE_IDENTITY = ("pid", "process_start_ticks", "executable", "executable_device",
                 "executable_inode", "binary_sha256", "rpc_socket", "rpc_device", "rpc_inode")
LIVE_COUNTERS = COUNTERS + ("write_latency_ticks",)


def _live_rows(directory, binding, boundary):
    name = "spdk-target-" + boundary + ".json"
    if not isinstance(binding, dict) or binding.get("path") != name:
        raise ValueError("SPDK live boundary binding has an unexpected path")
    path = Path(directory) / name
    if not stat.S_ISREG(path.lstat().st_mode) or not 0 < path.stat().st_size <= 16 << 20:
        raise ValueError("SPDK live boundary is not a bounded ordinary file")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != binding.get("sha256"):
        raise ValueError("SPDK live boundary differs from its integrity binding")
    rows = json.loads(raw)
    if not isinstance(rows, list) or len(rows) != 5:
        raise ValueError("SPDK live boundary requires five owner receipts")
    result = {}
    for row in rows:
        index = row.get("index") if isinstance(row, dict) else None
        if type(index) is not int or index not in range(5) or index in result:
            raise ValueError("SPDK live owner identity is missing or duplicated")
        if not isinstance(row.get("case_id"), str) or not row["case_id"]:
            raise ValueError("SPDK live case identity is missing")
        result[index] = row
    return result


def _live_owner(start, end):
    if start["case_id"] != end["case_id"]:
        raise ValueError("case identity changed")
    for row in (start, end):
        if row.get("available") is False:
            raise ValueError("snapshot unavailable: " + str(row.get("reason", "unknown reason")))
    first, last = start.get("target"), end.get("target")
    if not isinstance(first, dict) or not isinstance(last, dict):
        raise ValueError("target snapshot unavailable")
    before, after = first.get("daemon"), last.get("daemon")
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise ValueError("daemon identity unavailable")
    for key in LIVE_IDENTITY:
        if before.get(key) in (None, "") or before.get(key) != after.get(key):
            raise ValueError("daemon identity missing or changed: " + key)
    for key in ("pid", "process_start_ticks", "executable_device", "executable_inode", "rpc_device", "rpc_inode"):
        if type(before[key]) is not int or before[key] <= 0:
            raise ValueError("invalid daemon identity: " + key)
    digest = before["binary_sha256"]
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("invalid daemon binary digest")
    inventories, statistics = [], []
    for target in (first, last):
        inventory, stats = target.get("bdevs"), target.get("iostat")
        if not isinstance(inventory, list) or not isinstance(stats, dict):
            raise ValueError("bdev inventory or counters unavailable")
        if any(type(stats.get(k)) is not int or stats[k] <= 0 for k in ("ticks", "tick_rate")):
            raise ValueError("invalid bdev counter clock")
        if not isinstance(stats.get("bdevs"), list):
            raise ValueError("bdev counters unavailable")
        def indexed(rows):
            result = {}
            for item in rows:
                name = item.get("name") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name or name in result:
                    raise ValueError("invalid or duplicate bdev identity")
                result[name] = item
            return result
        inventories.append(indexed(inventory))
        statistics.append((stats, indexed(stats["bdevs"])))
    initial, final = statistics
    if (initial[0]["tick_rate"] != final[0]["tick_rate"] or final[0]["ticks"] <= initial[0]["ticks"]
            or not initial[1] or initial[1].keys() != final[1].keys()
            or inventories[0].keys() != inventories[1].keys()
            or initial[1].keys() != inventories[0].keys()):
        raise ValueError("counter clock or bdev inventory changed; rotated files cannot be subtracted")
    # Compare backing metadata where supplied: reusing a bdev name after file
    # rotation must not turn two different counter lifetimes into a delta.
    for name in initial[1]:
        if inventories[0][name] != inventories[1][name]:
            raise ValueError("bdev identity or geometry changed")
    if "backing_fds" in first or "backing_fds" in last:
        for target in (first, last):
            if not isinstance(target.get("backing_fds"), list):
                raise ValueError("backing file identity unavailable")
        def backing(rows):
            found = {}
            for row in rows:
                if not isinstance(row, dict) or row.get("available") is not True:
                    raise ValueError("backing file identity unavailable")
                name = row.get("name")
                if not isinstance(name, str) or name in found:
                    raise ValueError("invalid backing file identity")
                found[name] = tuple(row.get(k) for k in ("filename", "device", "inode", "mode", "bytes"))
                if any(value is None for value in found[name]):
                    raise ValueError("backing file identity incomplete")
            return found
        if backing(first["backing_fds"]) != backing(last["backing_fds"]):
            raise ValueError("backing file identity changed")
    totals = dict.fromkeys(LIVE_COUNTERS, 0)
    errors_available, errors = True, {}
    for name, source in initial[1].items():
        dest = final[1][name]
        for field in LIVE_COUNTERS:
            a, b = source.get(field), dest.get(field)
            if type(a) is not int or type(b) is not int or a < 0 or b < a:
                raise ValueError("missing or reset bdev counter: " + field)
            totals[field] += b - a
        a, b = source.get("io_error"), dest.get("io_error")
        if a is None or b is None:
            errors_available = False
            continue
        if (not isinstance(a, dict) or not isinstance(b, dict) or len(a) > 64 or len(b) > 64
                or any(not isinstance(k, str) or type(v) is not int or v < 0
                       for d in (a, b) for k, v in d.items())):
            raise ValueError("invalid I/O error counters")
        for status in a.keys() | b.keys():
            delta = b.get(status, 0) - a.get(status, 0)
            if delta < 0:
                raise ValueError("I/O error counter reset")
            errors[status] = errors.get(status, 0) + delta
    row = {"index": start["index"], "available": True, "process": dict(before),
           "observed_interval_seconds": (final[0]["ticks"] - initial[0]["ticks"]) / initial[0]["tick_rate"],
           "io_error_available": errors_available}
    if errors_available:
        row["io_errors"] = errors
    for direction, byte_key, op_key in (("read", "bytes_read", "num_read_ops"),
                                         ("write", "bytes_written", "num_write_ops")):
        ops, size = totals[op_key], totals[byte_key]
        if bool(ops) != bool(size) or (not ops and totals[direction + "_latency_ticks"]):
            raise ValueError("bdev operations, bytes and timing disagree")
        row[direction + "_ops"] = ops
        row[direction + "_bytes"] = size
        row["mean_" + direction + "_completion_us"] = (
            totals[direction + "_latency_ticks"] * 1e6 / initial[0]["tick_rate"] / ops if ops else None)
    return row


def admit_live(directory, binding):
    """Return live per-owner intervals; never infer absent owners or counters as zero."""
    if not isinstance(binding, dict):
        raise ValueError("SPDK live measurement binding is unavailable")
    start = _live_rows(directory, binding.get("start"), "start")
    end = _live_rows(directory, binding.get("end"), "end")
    case_ids = {row["case_id"] for rows in (start, end) for row in rows.values()}
    if len(case_ids) != 1:
        raise ValueError("SPDK live boundary case identities differ")
    owners = []
    for index in range(5):
        try:
            owners.append(_live_owner(start[index], end[index]))
        except (ValueError, KeyError, TypeError) as error:
            owners.append({"index": index, "available": False, "reason": str(error)})
    complete = all(row["available"] for row in owners)
    result = {"kind": "live", "case_id": case_ids.pop(), "available": complete,
              "owners": owners, "scope": "owner-local SPDK bdev completion; not per-API, pure fabric or NAND latency"}
    if complete:
        result["io_error_available"] = all(row["io_error_available"] for row in owners)
        if result["io_error_available"]:
            errors = {}
            for row in owners:
                for status, value in row["io_errors"].items():
                    errors[status] = errors.get(status, 0) + value
            result["io_errors"] = errors
        for direction in ("read", "write"):
            ops = sum(row[direction + "_ops"] for row in owners)
            result[direction + "_ops"] = ops
            result[direction + "_bytes"] = sum(row[direction + "_bytes"] for row in owners)
            result["mean_" + direction + "_completion_us"] = (
                sum((row["mean_" + direction + "_completion_us"] or 0) * row[direction + "_ops"]
                    for row in owners) / ops if ops else None)
    return result
