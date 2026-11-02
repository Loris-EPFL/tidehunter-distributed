"""Owned local Valkey RDMA primaries; explicit operator-provided RC endpoint."""
import binascii
import ipaddress
import os
from pathlib import Path
import socket
import time
import uuid

import local
import valkey_service


def options(count):
    if not os.environ.get("VALKEY_RDMA_RUNTIME") or not os.environ.get("VALKEY_RDMA_ADDRESS"):
        raise ValueError("VALKEY_RDMA_RUNTIME and VALKEY_RDMA_ADDRESS are required")
    runtime = Path(os.environ["VALKEY_RDMA_RUNTIME"]).resolve(strict=True)
    valkey_service.identity(runtime)
    address = str(ipaddress.IPv4Address(os.environ["VALKEY_RDMA_ADDRESS"]))
    if ipaddress.IPv4Address(address).is_loopback:
        raise ValueError("RDMA address must be an explicitly configured non-loopback fabric address")
    return {"nodes": [f"{address}:{6380+i}" for i in range(count)], "cluster": count > 1,
            "required_replication_factor": 1, "required_primary_nodes": count, "request_timeout_ms": 5000}


def validate_state(state):
    count = 1 if state["layout"] == "qualification" else 5
    if state["nodes"] != count or len(state["services"]) != count:
        raise ValueError("local Valkey RDMA primary inventory differs")
    runtime = Path(state["runtime"]).resolve(strict=True)
    metadata = valkey_service.identity(runtime)
    endpoints = state["rdma_endpoints"]
    controls = [f"127.0.0.{21+i}:{6379+i}" for i in range(count)]
    if state["control_endpoints"] != controls or len(endpoints) != count:
        raise ValueError("local Valkey endpoint inventory differs")
    address = str(ipaddress.IPv4Address(endpoints[0].rsplit(":", 1)[0]))
    if ipaddress.IPv4Address(address).is_loopback or endpoints != [f"{address}:{6380+i}" for i in range(count)]:
        raise ValueError("local Valkey RDMA endpoint mapping differs")
    selected = options_for_backend(state["backend"], count, endpoints)
    if state["backend_options"] != selected:
        raise ValueError("local Valkey selected client settings differ")
    for i, service in enumerate(state["services"]):
        data = Path(state["directory"]) / f"node-{i}" / "data"
        argv = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, count > 1, 1024)
        argv = [str(runtime / "bin/valkey-server") if x == "valkey-server" else x.replace("/data", str(data)) for x in argv]
        argv += ["--rdma-bind", address, "--rdma-port", str(6380+i)]
        expected = {"runtime": str(runtime), "unit": f"kvcloud-valkey-{state['owner'][:20]}-{i}", "args": argv,
                    "cpus": 2, "memory_mib": 1536, "source_commit": metadata["source_commit"],
                    "server_sha256": metadata["artifacts"]["src/valkey-server"]["sha256"]}
        if service != expected:
            raise ValueError("local Valkey owned service contract differs")
    return state


def options_for_backend(backend, count, rdma_endpoints):
    nodes = (rdma_endpoints if backend == "valkey_rdma" else
             [f"127.0.0.{21+i}:{6379+i}" for i in range(count)])
    result = {"nodes": nodes, "cluster": count > 1, "required_replication_factor": 1,
              "required_primary_nodes": count, "request_timeout_ms": 5000}
    if backend == "valkey_tcp":
        result["client"] = "libvalkey"
    return result


def run(state, rdma, endpoint, *args):
    cli = Path(state["runtime"]) / "bin/valkey-cli"
    valkey_service.identity(Path(state["runtime"]))
    argv = list(args)
    if rdma:
        target = state["rdma_endpoints"][state["control_endpoints"].index(endpoint)]
        host, port = target.rsplit(":", 1)
        argv[argv.index("-h") + 1] = host
        argv[argv.index("-p") + 1] = port
        argv.insert(1, "--rdma")
    return local.command([str(cli), *argv[1:]], timeout=30)


def probe(state):
    runtime = [entry for service in state["services"] for entry in valkey_service.evidence({"native_valkey": service})]
    endpoints = state["control_endpoints"]
    rdma = state["backend"] == "valkey_rdma"
    execute = lambda endpoint, *args: run(state, rdma, endpoint, *args)
    proof = local.valkey_evidence(execute, endpoints, state["nodes"] > 1)
    proof.update({"image": None, "transport": "RDMA-CM RC" if rdma else "TCP", "cluster_bus": "TCP control network",
                  "source_commit": valkey_service.identity(Path(state["runtime"]))["source_commit"], "runtime": runtime})
    dirs = {endpoint: str(Path(state["directory"]) / f"node-{i}" / "data") for i, endpoint in enumerate(endpoints)}
    proof["coverage"] = local.admit_valkey(proof["nodes"], endpoints, state["nodes"] > 1, data_dir=dirs)
    for node in proof["nodes"]:
        endpoint = node["endpoint"]
        if local.valkey_cli(execute, endpoint, "PING").strip() != "PONG":
            raise RuntimeError("local Valkey PING failed")
        if state["nodes"] > 1:
            own = next(line.split() for line in node["cluster_nodes"].splitlines()
                       if "myself" in line.split()[2].split(","))
            ranges = [tuple(map(int, token.split("-"))) if "-" in token else (int(token), int(token))
                      for token in own[8:] if not token.startswith("[")]
        else:
            ranges = [(0, 16383)]
        key = next((f"local-rdma-{state['owner'][:12]}-{i}" for i in range(200000)
                    if any(a <= binascii.crc_hqx(f"local-rdma-{state['owner'][:12]}-{i}".encode(), 0) % 16384 <= b
                           for a, b in ranges)), None)
        if key is None or (local.valkey_cli(execute, endpoint, "SET", key, "qualified").strip() != "OK" or
                           local.valkey_cli(execute, endpoint, "GET", key).strip() != "qualified" or
                           local.valkey_cli(execute, endpoint, "DEL", key).strip() != "1"):
            raise RuntimeError("local Valkey SET/GET/DEL failed")
    proof["rdma_qualification" if rdma else "tcp_qualification"] = {
        "status": "passed", "endpoints": state["rdma_endpoints"] if rdma else endpoints,
        "operations": ["PING", "SET", "GET", "DEL"]}
    return proof


def start(args):
    directory = args.state.resolve()
    if (directory / "owner.json").exists():
        state = local.load(directory)
        if state["backend"] != args.backend or state["layout"] != args.layout:
            raise ValueError("existing local Valkey ownership mismatch")
        local.ready(state, args.timeout)
        return
    if directory.exists() and any(directory.iterdir()):
        raise ValueError("refusing nonempty unowned local directory")
    count = 1 if args.layout == "qualification" else 5
    rdma_options = options(count)
    selected = options_for_backend(args.backend, count, rdma_options["nodes"])
    runtime = Path(os.environ["VALKEY_RDMA_RUNTIME"]).resolve(strict=True)
    metadata = valkey_service.identity(runtime)
    address = rdma_options["nodes"][0].rsplit(":", 1)[0]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((address, 0))
    directory.mkdir(parents=True, exist_ok=True)
    owner = uuid.uuid4().hex
    controls = [f"127.0.0.{21+i}:{6379+i}" for i in range(count)]
    services = []
    for i in range(count):
        data = directory / f"node-{i}" / "data"
        data.mkdir(parents=True)
        argv = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, count > 1, 1024)
        argv = [str(runtime / "bin/valkey-server") if x == "valkey-server" else x.replace("/data", str(data)) for x in argv]
        argv += ["--rdma-bind", address, "--rdma-port", str(6380+i)]
        services.append({"runtime": str(runtime), "unit": f"kvcloud-valkey-{owner[:20]}-{i}", "args": argv,
                         "cpus": 2, "memory_mib": 1536, "source_commit": metadata["source_commit"],
                         "server_sha256": metadata["artifacts"]["src/valkey-server"]["sha256"]})
    state = {"owner": owner, "project": "kv-" + owner[:16], "backend": args.backend, "layout": args.layout,
             "nodes": count, "directory": str(directory), "compose_file": str(directory / "compose.json"),
             "physical_hosts": 1, "independent_disks": False, "runtime": str(runtime), "services": services,
             "control_endpoints": controls, "rdma_endpoints": rdma_options["nodes"], "backend_options": selected,
             "paired_valkey": True, "created_at_unix": time.time()}
    local.dump(directory / "owner.json", state)
    try:
        for service in services:
            valkey_service.start({"native_valkey": service})
        if count > 1:
            local.initialize_valkey(lambda endpoint, *argv: run(state, False, endpoint, *argv), controls, directory, args.timeout)
        local.ready(state, args.timeout)
    except BaseException as error:
        try:
            stop(state)
        except Exception as cleanup:
            error.add_note("Valkey cleanup failed: " + repr(cleanup))
        raise


def stop(state):
    removed, failures = [], []
    for service in reversed(state["services"]):
        try:
            removed.extend(valkey_service.stop({"native_valkey": service}))
        except Exception as error:
            failures.append({"unit": service["unit"], "error": repr(error)})
    local.dump(Path(state["directory"]) / "cleanup.json",
               {"removed_units": removed, "failures": failures, "data_retained": True})
    if failures:
        raise RuntimeError("Valkey cleanup failures: " + repr(failures))


def export(state, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    local.dump(output / "services.json", [valkey_service.evidence({"native_valkey": x}) for x in state["services"]])
    for name in ("owner.json", "deployment.json", "initialization.json", "cleanup.json"):
        source = Path(state["directory"]) / name
        if source.exists():
            import shutil
            shutil.copyfile(source, output / name)
