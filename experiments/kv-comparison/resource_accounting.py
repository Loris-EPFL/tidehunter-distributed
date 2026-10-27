"""Owned-cgroup snapshots and honest terminal interval gauges, outside API timers."""
import math
from pathlib import Path
import re
import time

TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}")
MAX_PROCESSES = 16
MAX_KERNEL_BYTES = 64 * 1024
SCOPE = "measurement_boundary_interval"


def integer(value):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise ValueError("kernel counter is not a nonnegative integer")
    return int(value)


def read_kernel(path):
    with path.open() as stream:
        text = stream.read(MAX_KERNEL_BYTES + 1)
    if len(text) > MAX_KERNEL_BYTES:
        raise ValueError("kernel counter exceeds its bound")
    return text


def fields(text):
    result = {}
    for line in text.splitlines():
        key, value = line.split()
        if key in result or not re.fullmatch(r"[a-z_][a-z0-9_.]*", key):
            raise ValueError("duplicate or invalid kernel counter")
        result[key] = integer(value)
    return result


def io_devices(text):
    result = {}
    for line in text.splitlines():
        parts = line.split()
        if not parts or not re.fullmatch(r"[0-9]+:[0-9]+", parts[0]) or parts[0] in result:
            raise ValueError("invalid or duplicate cgroup IO device")
        row = {}
        for item in parts[1:]:
            key, value = item.split("=", 1)
            if key in row or not re.fullmatch(r"[a-z_]+", key):
                raise ValueError("invalid or duplicate cgroup IO counter")
            row[key] = integer(value)
        if not {"rbytes", "wbytes"} <= row.keys():
            raise ValueError("cgroup IO device lacks byte counters")
        result[parts[0]] = row
    return result


def capture(host_index, boot_id, processes, *, cgroup_root=Path("/sys/fs/cgroup"), clock=time.monotonic_ns):
    if type(host_index) is not int or host_index not in range(-1, 5):
        raise ValueError("explicit fleet host index required")
    if not isinstance(boot_id, str) or not boot_id or len(boot_id) > 128:
        raise ValueError("observed host boot identity required")
    if not isinstance(processes, list) or len(processes) > MAX_PROCESSES:
        raise ValueError("owned process selection exceeds bound")
    root = Path(cgroup_root).resolve()
    rows, roles = [], set()
    for process in processes:
        role, group = process.get("role"), process.get("cgroup")
        if not isinstance(role, str) or not TOKEN.fullmatch(role) or role in roles:
            raise ValueError("unique bounded owned process role required")
        roles.add(role)
        if (not isinstance(group, str) or not group.startswith("/") or group == "/"
                or ".." in group.split("/") or str(Path(group)) != group):
            raise ValueError("explicit non-root owned cgroup path required")
        directory = root / group.lstrip("/")
        if not directory.resolve().is_relative_to(root):
            raise ValueError("owned cgroup escapes accounting root")
        row = {"process_role": role, "cgroup": group, "available": False}
        try:
            inode = directory.stat().st_ino
            cpu = fields(read_kernel(directory / "cpu.stat"))
            if "usage_usec" not in cpu:
                raise ValueError("cgroup CPU usage is unavailable")
            row.update(inode=inode, cpu=cpu, monotonic_ns=clock())
            for name in ("current", "peak"):
                try:
                    row["memory_" + name + "_bytes"] = integer(read_kernel(directory / ("memory." + name)).strip())
                except FileNotFoundError:
                    pass  # Unsupported kernel fields remain absent, not zero.
            try:
                row["io_devices"] = io_devices(read_kernel(directory / "io.stat"))
            except FileNotFoundError:
                pass
            if directory.stat().st_ino != inode:
                raise ValueError("owned cgroup changed during accounting capture")
            row["available"] = True
        except (OSError, ValueError) as error:
            row["reason"] = (type(error).__name__ + ": " + str(error))[:300]
        rows.append(row)
    return {"schema_version": 1, "host_index": host_index, "boot_id": boot_id, "processes": rows}


def difference(before, after):
    if type(before) is not int or type(after) is not int or before < 0 or after < before:
        raise ValueError("resource counter reset or invalid boundary")
    return after - before


def interval(before_hosts, after_hosts):
    """No shared clock or live-rate inference; each row uses its own host clock."""
    def hosts(values):
        result = {}
        if not isinstance(values, list) or len(values) > 6:
            raise ValueError("bounded explicit host-accounting frames required")
        for frame in values:
            index = frame.get("host_index")
            if frame.get("schema_version") != 1 or type(index) is not int or index not in range(-1, 5) or index in result:
                raise ValueError("duplicate or invalid host-accounting identity")
            if not isinstance(frame.get("processes"), list) or len(frame["processes"]) > MAX_PROCESSES:
                raise ValueError("invalid process-accounting frame")
            result[index] = frame
        return result
    before_map, after_map = hosts(before_hosts), hosts(after_hosts)
    rows = []
    for index in sorted(before_map.keys() | after_map.keys()):
        before, after = before_map.get(index), after_map.get(index)
        def processes(frame):
            values = {}
            for row in frame["processes"] if frame else []:
                role = row.get("process_role")
                if not isinstance(role, str) or not TOKEN.fullmatch(role) or role in values:
                    raise ValueError("duplicate or invalid process role in accounting frame")
                values[role] = row
            return values
        initial, final = processes(before), processes(after)
        for role in sorted(initial.keys() | final.keys()):
            row = {"host_index": index, "process_role": role, "available": False}
            start, end = initial.get(role), final.get(role)
            try:
                if not start or not end or not start.get("available") or not end.get("available"):
                    raise ValueError("owned process counters unavailable at a boundary")
                if not before.get("boot_id") or before["boot_id"] != after.get("boot_id"):
                    raise ValueError("host boot identity changed or is unavailable")
                if (not start.get("cgroup") or start["cgroup"] != end.get("cgroup")
                        or type(start.get("inode")) is not int or start["inode"] != end.get("inode")):
                    raise ValueError("owned cgroup identity changed or is unavailable")
                elapsed = difference(start["monotonic_ns"], end["monotonic_ns"]) / 1e9
                if not math.isfinite(elapsed) or elapsed <= 0:
                    raise ValueError("positive host-local resource interval required")
                observed = {"elapsed_seconds": elapsed}
                for field, target, scale in (("usage_usec", "cpu_seconds", 1e6),
                        ("throttled_usec", "cpu_throttled_seconds", 1e6),
                        ("nr_periods", "cpu_periods", 1), ("nr_throttled", "cpu_throttled_periods", 1)):
                    if field in start["cpu"] and field in end["cpu"]:
                        delta = difference(start["cpu"][field], end["cpu"][field])
                        observed[target] = delta if scale == 1 else delta / scale
                if "cpu_seconds" not in observed:
                    raise ValueError("CPU usage is missing at a boundary")
                if observed.get("cpu_throttled_periods", 0) > observed.get("cpu_periods", math.inf):
                    raise ValueError("throttled periods exceed observed CPU periods")
                if "io_devices" in start and "io_devices" in end:
                    devices = start["io_devices"].keys() | end["io_devices"].keys()
                    reads = writes = 0
                    for device in devices:
                        if device in start["io_devices"] and device not in end["io_devices"]:
                            raise ValueError("previous cgroup IO device counters disappeared")
                        previous = start["io_devices"].get(device, {"rbytes": 0, "wbytes": 0})
                        current = end["io_devices"][device]
                        reads += difference(previous["rbytes"], current["rbytes"])
                        writes += difference(previous["wbytes"], current["wbytes"])
                    observed.update(io_read_bytes=reads, io_write_bytes=writes)
                for name in ("memory_current_bytes", "memory_peak_bytes"):
                    if name in end:
                        value = end[name]
                        if type(value) is not int or value < 0:
                            raise ValueError("invalid observed memory counter")
                        observed[name] = value
                row.update(available=True, **observed)
            except (KeyError, TypeError, ValueError) as error:
                row["reason"] = str(error)[:300]
            rows.append(row)
    if len(rows) > MAX_PROCESSES:
        raise ValueError("terminal process accounting exceeds total process bound")
    return {"schema_version": 1, "scope": SCOPE, "processes": rows}
