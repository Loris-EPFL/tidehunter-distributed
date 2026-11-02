#!/usr/bin/env python3
"""Bounded local qualification of the packaged paired libvalkey TCP adapter.

The default uses five real Valkey Cluster primaries, not mocks. This is not a
performance result or RDMA hardware qualification. Only owned subprocesses are
started and stopped; the output directory must be new.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import uuid

import local
import valkey_service


def owns_listener(pid, host, port):
    address = socket.inet_aton(host)[::-1].hex().upper() + f":{port:04X}"
    inodes = {line.split()[9] for line in Path("/proc/net/tcp").read_text().splitlines()[1:]
              if line.split()[1] == address and line.split()[3] == "0A"}
    descriptors = Path("/proc") / str(pid) / "fd"
    return any(os.readlink(fd) == f"socket:[{inode}]" for inode in inodes for fd in descriptors.iterdir())


def reserve_pair(host):
    # Keep both sockets reserved until immediately before the matching server is
    # spawned; process ownership is checked before issuing any cluster command.
    for _ in range(100):
        first = socket.socket()
        second = socket.socket()
        try:
            first.bind((host, 0))
            port = first.getsockname()[1]
            if port > 55535:
                continue
            second.bind((host, port + 10000))
            return port, first, second
        except OSError:
            pass
        finally:
            if second.getsockname()[1] == 0:
                first.close()
                second.close()
    raise RuntimeError("unable to reserve TCP and cluster-bus ports")


def qualify(runtime, binary, example, output, timeout=120, standalone=False):
    if not 10 <= timeout <= 600:
        raise ValueError("timeout must be 10..600 seconds")
    runtime = Path(runtime).resolve(strict=True)
    binary = Path(binary).resolve(strict=True)
    example = Path(example).resolve(strict=True)
    output = Path(output).absolute()
    metadata = valkey_service.identity(runtime)
    if binary != (runtime / "bin/kv-research").resolve(strict=True):
        raise ValueError("qualification requires the packaged kv-research binary")
    output.mkdir(parents=True, exist_ok=False)
    deadline = time.monotonic() + timeout
    environment = dict(os.environ)
    environment.pop("LD_PRELOAD", None)
    environment.pop("LD_AUDIT", None)
    count = 1 if standalone else 5
    reservations = []
    servers = []
    logs = []
    try:
        for i in range(count):
            host = f"127.0.0.{21+i}"
            port, first, second = reserve_pair(host)
            reservations.append((host, port, first, second))
        endpoints = [f"{host}:{port}" for host, port, _, _ in reservations]
        argv_list = []
        for i, (host, port, first, second) in enumerate(reservations):
            data = output / f"node-{i}" / "data"
            data.mkdir(parents=True)
            argv = local.valkey_server_args(host, port, port + 10000, count > 1, 64)
            argv = [str(runtime / "bin/valkey-server") if x == "valkey-server" else
                    x.replace("/data", str(data)) for x in argv]
            argv_list.append(argv)
            first.close()
            second.close()
            log = (output / f"server-{i}.log").open("wb")
            logs.append(log)
            servers.append(subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                                            env=environment, start_new_session=True))
        for server, (host, port, _, _) in zip(servers, reservations):
            while True:
                if server.poll() is not None:
                    raise RuntimeError("owned Valkey server exited before readiness")
                if time.monotonic() >= deadline:
                    raise TimeoutError("owned Valkey server readiness deadline")
                try:
                    with socket.create_connection((host, port), timeout=1) as sock:
                        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                        if sock.recv(32) == b"+PONG\r\n" and owns_listener(server.pid, host, port):
                            break
                except OSError:
                    pass
                time.sleep(0.1)
        cli_log = (output / "client-stderr.log").open("w")
        logs.append(cli_log)
        def cli(endpoint, *args):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Valkey qualification deadline")
            return subprocess.check_output([str(runtime / "bin/valkey-cli"), *args[1:]],
                env=environment, text=True, stderr=cli_log, timeout=min(30, remaining))
        if count > 1:
            local.initialize_valkey(cli, endpoints, output, max(1, deadline - time.monotonic()))
        dirs = {endpoint: str(output / f"node-{i}" / "data") for i, endpoint in enumerate(endpoints)}
        while True:
            try:
                observed = local.valkey_evidence(cli, endpoints, count > 1)
                coverage = local.admit_valkey(observed["nodes"], endpoints, count > 1, data_dir=dirs)
                break
            except (RuntimeError, ValueError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Valkey cluster admission deadline")
                time.sleep(0.1)
        local.dump(output / "deployment.json", {"observed": observed, "coverage": coverage})
        config = json.loads(example.read_text())
        config["backend"] = {"kind": "valkey_tcp", "options": {"nodes": endpoints,
            "cluster": count > 1, "required_replication_factor": 1, "required_primary_nodes": count,
            "request_timeout_ms": 5000, "client": "libvalkey"}}
        config["namespace"] = "paired-tcp-" + uuid.uuid4().hex
        config["dataset"].update(keys=64, value_size=256)
        config["workload"]["mix"] = {"read": 35, "put": 30, "delete": 0, "missing_read": 10,
                                    "get_many": 25, "put_batch": 0}
        config["execution"].update(mode="closed_loop", concurrency=4, queue_capacity=16, request_timeout_ms=5000)
        config["phases"] = {"preload": True, "warmup_operations": 8, "aging_operations": 0,
                            "measurement_operations": 64}
        config["qualification_only"] = False
        config["limits"].update(deadline_seconds=min(90, timeout - 5), shutdown_grace_seconds=5)
        (output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("paired TCP qualification deadline")
        with (output / "runner.log").open("wb") as log:
            subprocess.run([str(binary), "--config", str(output / "config.json"), "--output", str(output / "result")],
                           stdout=log, stderr=subprocess.STDOUT, env=environment, timeout=remaining, check=True)
        for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom"):
            if not (output / "result" / name).is_file():
                raise RuntimeError("paired TCP runner omitted " + name)
        manifest = json.loads((output / "result/manifest.json").read_text())
        if (manifest.get("status") != "completed" or not manifest.get("raw_samples_complete")
                or manifest.get("errors_or_wrong_values") != 0 or manifest.get("timeouts_or_unknown_mutations") != 0
                or manifest.get("run", {}).get("qualification_failure") is not None):
            raise RuntimeError("paired TCP adapter qualification or workload failed")
        receipt = {"status": "passed", "scope": "local paired libvalkey TCP functional qualification only",
                   "server_source_commit": metadata["source_commit"],
                   "server_sha256": metadata["artifacts"]["src/valkey-server"]["sha256"],
                   "binary_sha256": valkey_service.digest(binary), "server_argv": argv_list,
                   "primaries": count, "slot_coverage": coverage["slot_count"],
                   "client": "libvalkey", "rdma_qualified": False}
        local.dump(output / "qualification.json", receipt)
        return receipt
    finally:
        for _, _, first, second in reservations:
            first.close()
            second.close()
        for server in reversed(servers):
            if server.poll() is None:
                try:
                    os.killpg(server.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for server in reversed(servers):
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait(timeout=5)
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--example", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--standalone", action="store_true")
    args = parser.parse_args()
    print(json.dumps(qualify(args.runtime, args.binary, args.example, args.output, args.timeout, args.standalone), sort_keys=True))


if __name__ == "__main__":
    main()
