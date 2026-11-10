"""Bounded parsers for the focused floor measurements."""
import csv
import io
import math
import re

MAX_LOG_BYTES = 12 * 1024 * 1024
LATENCY_FIELDS = ("min", "p50", "avg", "p90", "p99", "p999", "max", "stdev")


def parse_perftest(text, size, iterations):
    """Read the perftest latency table, not incidental numeric diagnostics."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if "#bytes" not in line or "#iterations" not in line:
            continue
        for row in lines[index + 1:]:
            fields = row.split()
            if len(fields) != 9:
                continue
            try:
                n, count = int(fields[0]), int(fields[1])
                values = [float(x) for x in fields[2:]]
            except ValueError:
                continue
            if n == size and count == iterations and all(math.isfinite(x) and x >= 0 for x in values):
                low, high, median, avg, stdev, p99, p999 = values
                return dict(zip(LATENCY_FIELDS, (low, median, avg, None, p99, p999, high, stdev)))
    raise ValueError("perftest result row missing or invalid")


def parse_fi_pingpong(text, size):
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if "usec/xfer" not in line or "#sent" not in line:
            continue
        for row in lines[index + 1:]:
            fields = row.split()
            if len(fields) != 8:
                continue
            try:
                match = re.fullmatch(r"(\d+)([kKmMgG]?)", fields[0])
                if match is None:
                    continue
                n = int(match.group(1)) * {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3}[match.group(2).lower()]
                usec = float(fields[6])
            except ValueError:
                continue
            if n == size and math.isfinite(usec) and usec >= 0:
                return dict(zip(LATENCY_FIELDS, (None, None, usec, None, None, None, None, None)))
    raise ValueError("fi_pingpong result row missing or invalid")


def parse_fio_log(raw, *, block_bytes=None, region_bytes=None, direction=0):
    if not isinstance(raw, bytes) or len(raw) > MAX_LOG_BYTES:
        raise ValueError("fio log exceeds bound")
    rows = []
    try:
        for fields in csv.reader(io.StringIO(raw.decode("ascii"))):
            if len(fields) not in (5, 6):
                raise ValueError("fio latency log must have five or six columns")
            row = tuple(int(value.strip()) for value in fields[:5])
            if len(fields) == 6 and int(fields[5].strip()) < 0:
                raise ValueError("invalid fio log priority")
            t, latency, row_direction, block, offset = row
            if (min(row) < 0 or row_direction != direction or block <= 0
                    or (block_bytes is not None and block != block_bytes)
                    or (region_bytes is not None and offset + block > region_bytes)):
                raise ValueError("invalid fio latency row")
            rows.append(row)
    except (UnicodeError, csv.Error) as exc:
        raise ValueError("invalid fio latency log") from exc
    if not rows:
        raise ValueError("fio latency log is empty")
    return rows


WRITE_LOG_ROWS = 100000
APPEND_MODES = ("append_pwrite", "append_mmap", "append_mmap_fsync")
APPEND_WARMUP = 512
APPEND_LOG_ROWS = 4096


def write_argv(binary, target, log_path, spec, identity):
    from pathlib import Path
    append = spec.get("mode") in APPEND_MODES
    if (spec.get("mode") not in ("buffered", "fdatasync", "direct", *APPEND_MODES)
            or spec.get("block_bytes") not in (1024, 8192, 65536)
            or spec.get("region_bytes") != 536870912
            or spec.get("queue_depth") != 1 or spec.get("location") != "local_file"
            or (append and (spec.get("host") not in ("client", "owner0")
                            or spec.get("operations") != APPEND_LOG_ROWS
                            or spec.get("warmup_operations") != APPEND_WARMUP))
            or (not append and (spec.get("runtime_seconds") != 6 or spec.get("preheat") is not True))):
        raise ValueError("invalid SSD write floor dimensions")
    if "host" in spec and spec["host"] not in ("client", "owner0"):
        raise ValueError("invalid SSD write host")
    target = Path(target)
    return [str(binary), str(target.parent), target.name, str(log_path), spec["mode"],
            str(spec["block_bytes"]), str(spec["region_bytes"]), str(6 if append else spec["runtime_seconds"]), str(APPEND_LOG_ROWS if append else WRITE_LOG_ROWS),
            str(identity["device"]), str(identity["inode"])]


def parse_write_log(raw, spec):
    if not isinstance(raw, bytes) or len(raw) > MAX_LOG_BYTES:
        raise ValueError("write log exceeds bound")
    rows = []
    try:
        for fields in csv.reader(io.StringIO(raw.decode("ascii"))):
            if len(fields) != 4:
                raise ValueError("invalid write log columns")
            write, sync, total, offset = map(int, fields)
            if (min(write, sync, total, offset) < 0 or total != write + sync
                    or offset % spec["block_bytes"] or offset + spec["block_bytes"] > spec["region_bytes"]
                    or (spec["mode"] not in ("fdatasync", *APPEND_MODES) and sync != 0)
                    or (spec["mode"] in APPEND_MODES and offset != (APPEND_WARMUP + len(rows)) * spec["block_bytes"])):
                raise ValueError("invalid write log row")
            rows.append((write, sync, total, offset))
    except (UnicodeError, csv.Error) as exc:
        raise ValueError("invalid write log") from exc
    if (not rows or len(rows) > WRITE_LOG_ROWS
            or (spec["mode"] in APPEND_MODES and len(rows) != APPEND_LOG_ROWS)):
        raise ValueError("invalid write log row count")
    return rows


def write_result(job, raw, log, device_identity, actual_direct):
    import json
    spec = job["ssd_write"]
    rows = parse_write_log(log, spec)
    document = json.loads(raw)
    total = document["total_operations"]
    if (type(total) is not int or total < len(rows) or document["logged_operations"] != len(rows)
            or type(document["elapsed_ns"]) is not int or document["elapsed_ns"] <= 0
            or type(actual_direct) is not bool or actual_direct != (spec["mode"] == "direct")
            or not isinstance(device_identity, str) or not device_identity):
        raise ValueError("write helper observations differ from contract")
    append = spec["mode"] in APPEND_MODES
    if (append and (total != APPEND_LOG_ROWS or spec.get("host") not in ("client", "owner0"))) or ("host" in spec and spec["host"] not in ("client", "owner0")):
        raise ValueError("invalid append floor observations")
    def summary(column):
        values = sorted(row[column] for row in rows)
        def pct(p):
            return values[math.ceil(len(values) * p) - 1]
        return {"n": len(values), "min": values[0], "mean": sum(values) / len(values),
                "p50": pct(.5), "p90": pct(.9), "p99": pct(.99), "p999": pct(.999), "max": values[-1]}
    result = {"schema_version": 1, "case_id": job["case_id"], "mode": spec["mode"],
              "block_bytes": spec["block_bytes"], "region_bytes": spec["region_bytes"],
              "location": spec["location"], "requested_depth": 1,
              "actual_direct": actual_direct, "device_identity": device_identity,
              "elapsed_seconds": document["elapsed_ns"] / 1e9, "total_operations": total,
              "logged_operations": len(rows), "dropped_operations": total - len(rows),
              "sampling": "first_rows", "summary_population": "first N logged operations",
              "write": summary(0), "sync": summary(1), "total": summary(2)}
    if not append:
        result["preheat"] = spec["preheat"]
    if "host" in spec:
        result["host_index"] = {"client": -1, "owner0": 0}[spec["host"]]
    if append:
        result.update(warmup_operations=APPEND_WARMUP, sampling="post_warmup",
                      summary_population="all measured operations")
    return result

def port_for(case_id, tool, index):
    import hashlib
    base = 18700 if tool.startswith("ib_") else 18900 if tool == "fi_pingpong" else 18800
    offset = int.from_bytes(hashlib.sha256(case_id.encode()).digest()[:4], "big") % 25
    return base + 4 * offset + index


def server_argv(net, size, port, address, binary):
    tool = net["tool"]
    if tool.startswith("ib_"):
        argv = [tool, "-R", "-d", "irdma0", "-p", str(port), "-s", str(size), "-n", str(net["iterations"])]
        return argv + (["-e"] if net["mode"] == "event" else [])
    if tool == "fi_pingpong":
        return [tool, "-p", net["provider"], "-e", net["endpoint"], "-s", address,
                "-B", str(port), "-S", str(size), "-I", str(net["iterations"])]
    return [str(binary), "server", address, str(port), net["mode"], str(size),
            str(net["iterations"]), str(net["warmup"])]


def client_argv(net, size, port, client_address, server_address, binary):
    argv = server_argv(net, size, port, client_address, binary)
    if net["tool"] == "tcp_rtt":
        argv[1:3] = ["client", server_address]
    elif net["tool"] == "fi_pingpong":
        argv[argv.index("-B")] = "-P"
        argv.append(server_address)
    else:
        argv.append(server_address)
    return argv

def tcp_listening(pid, port):
    import os
    from pathlib import Path
    sockets = set()
    try:
        for fd in (Path("/proc") / str(pid) / "fd").iterdir():
            try:
                target = os.readlink(fd)
                if target.startswith("socket:["):
                    sockets.add(target[8:-1])
            except FileNotFoundError:
                pass
        for row in Path("/proc/net/tcp").read_text().splitlines()[1:]:
            fields = row.split()
            if fields[3] == "0A" and int(fields[1].split(":")[1], 16) == port and fields[9] in sockets:
                return True
    except FileNotFoundError:
        return False
    return False


def rdma_listening(pid, port, timeout, output_path):
    """Accept an owned CM listener or perftest's bounded listener message."""
    import json
    from pathlib import Path
    import subprocess

    def output_ready():
        try:
            with Path(output_path).open("rb") as output:
                return b"Waiting for client" in output.read(16384)
        except FileNotFoundError:
            return False

    if output_ready():
        return True
    try:
        result = subprocess.run(["rdma", "-j", "resource", "show", "cm_id"],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=True)
        if len(result.stdout) > 1024 * 1024:
            raise ValueError("RDMA listener response exceeds bound")
        records = json.loads(result.stdout)
        if not isinstance(records, list):
            raise ValueError("invalid RDMA listener response")
        for item in records:
            if (isinstance(item, dict) and item.get("pid") == pid and item.get("state") == "LISTEN"
                    and isinstance(item.get("src-addr"), str)
                    and item["src-addr"].rpartition(":")[2] == str(port)):
                return True
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return output_ready()


def process_identity(pid):
    from pathlib import Path
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        return fields[0], fields[19]
    except FileNotFoundError:
        return None



def process_group_alive(pgid):
    from pathlib import Path
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            stat = (path / "stat").read_text()
            fields = stat[stat.rfind(")") + 2:].split()
            if int(fields[2]) == pgid and fields[0] != "Z":
                return True
        except (FileNotFoundError, ProcessLookupError):
            continue
    return False


def supervise(directory, index, argv):
    """A detached supervisor records the child status after the guest RPC exits."""
    import json
    import os
    from pathlib import Path
    import resource
    import subprocess
    directory = Path(directory)
    resource.setrlimit(resource.RLIMIT_FSIZE, (16384, 16384))
    with (directory / f"server-{index}.out").open("wb") as out:
        process = subprocess.Popen(argv, stdout=out, stderr=subprocess.STDOUT)
        pid_file = directory / f"server-{index}.pid.tmp"
        pid_file.write_text(str(process.pid))
        os.replace(pid_file, directory / f"server-{index}.pid")
        status = process.wait()
    temp = directory / f"server-{index}.status.tmp"
    temp.write_text(json.dumps({"exit_status": status}))
    os.replace(temp, directory / f"server-{index}.status")


if __name__ == "__main__":
    import json
    import sys
    supervise(sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3]))
