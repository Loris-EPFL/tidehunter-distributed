#!/usr/bin/env python3
"""Offline inventory and explicit, non-performance Valkey RDMA qualification."""

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import subprocess
import time
import uuid

# The release pin is coordinated with the independent TCP comparison.
RELEASE = "9.0.7"
SOURCE_COMMIT = "fe8d406c0b5a64ec897f6455e4de684629883764"
MAX_OUTPUT = 65536
MAX_COMMANDS = 32
KEY_TTL_MS = 120000
MAX_FILE = 128 * 1024 * 1024
CLEANUP_SCRIPT = (
    "local v=redis.call('GET',KEYS[1]); "
    "if v==ARGV[1] or v==ARGV[2] then return redis.call('DEL',KEYS[1]) "
    "elseif not v then return 0 else return -1 end"
)
SERVER_FIELDS = {
    "server_name", "valkey_version", "redis_version", "redis_git_sha1",
    "redis_git_dirty", "redis_build_id", "server_mode", "redis_mode",
    "os", "arch_bits", "multiplexing_api", "gcc_version", "io_threads_active",
}
CONFIG_FIELDS = [
    "rdma-port", "rdma-bind", "appendonly", "appendfsync",
    "no-appendfsync-on-rewrite", "maxmemory-policy", "port", "cluster-enabled",
]


class QualificationError(Exception):
    pass


def sha256_file(path):
    path = Path(path)
    size = path.stat().st_size
    if not path.is_file() or size > MAX_FILE:
        raise QualificationError(f"Not a regular file of at most {MAX_FILE} bytes: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        remaining = MAX_FILE + 1
        while remaining:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                return digest.hexdigest()
            remaining -= len(chunk)
            digest.update(chunk)
    raise QualificationError(f"File grew beyond hash limit: {path}")


def read_small(path):
    try:
        with Path(path).open("r", encoding="utf-8", errors="replace") as stream:
            return stream.read(4096).strip()
    except OSError:
        return None


def inventory(device, interface, sys_root=Path("/sys"), dev_root=Path("/dev")):
    """Inventory requested hardware only; never assert that a client selected it."""
    missing = []
    result = {"device_requested": device, "interface_requested": interface,
              "device_use_proven": False, "hardware_path_attested": False,
              "kernel": platform.release(), "platform": platform.system()}
    if platform.system() != "Linux":
        missing.append("Linux is required by the official Valkey RDMA transport")
    cm = dev_root / "infiniband/rdma_cm"
    result["rdma_cm_node"] = str(cm)
    result["rdma_cm_accessible"] = cm.is_char_device() and os.access(cm, os.R_OK | os.W_OK)
    if not result["rdma_cm_accessible"]:
        missing.append("RDMA-CM character device is absent or not readable/writable: " + str(cm))
    if not device:
        missing.append("Supply --device naming an existing InfiniBand verbs device")
    else:
        base = sys_root / "class/infiniband" / device
        result["device_present"] = base.exists()
        if not base.exists():
            missing.append(f"Requested verbs device is absent: {device}")
        driver = base / "device/driver"
        result["driver"] = driver.resolve().name if driver.exists() else None
        if device.startswith(("efa", "rxe", "siw")) or result["driver"] in {"efa", "rdma_rxe", "siw"}:
            missing.append("EFA/SRD and software RDMA are not this InfiniBand RC hardware cohort")
        ports = sorted((base / "ports").glob("*"))[:16]
        result["ports"] = [
            {"port": p.name, "state": read_small(p / "state"),
             "link_layer": read_small(p / "link_layer"), "rate": read_small(p / "rate")}
            for p in ports
        ]
        if not any(p["link_layer"] == "InfiniBand" and (p["state"] or "").startswith("4:")
                   for p in result["ports"]):
            missing.append("Requested device has no observed ACTIVE InfiniBand port")
        uverbs = []
        for entry in sorted((sys_root / "class/infiniband_verbs").glob("*"))[:64]:
            if read_small(entry / "ibdev") == device:
                node = dev_root / "infiniband" / entry.name
                uverbs.append({"node": str(node),
                               "accessible": node.is_char_device() and os.access(node, os.R_OK | os.W_OK)})
        result["uverbs"] = uverbs
        if not any(entry["accessible"] for entry in uverbs):
            missing.append("No accessible uverbs node maps to the requested device")
        result["interface_maps_to_device"] = bool(interface and (base / "device/net" / interface).exists())
        if interface and not result["interface_maps_to_device"]:
            missing.append("Requested IPoIB interface is not mapped to requested device in sysfs")
    if not interface:
        missing.append("Supply --interface naming the already configured IPoIB interface")
    else:
        net = sys_root / "class/net" / interface
        result["interface_type"] = read_small(net / "type")
        result["interface_state"] = read_small(net / "operstate")
        if result["interface_type"] != "32":
            missing.append("Requested interface is absent or not IPoIB (ARPHRD_INFINIBAND=32)")
        if result["interface_state"] != "up":
            missing.append("Requested IPoIB interface is not observed up")
    return result, missing


class Recorder:
    """A fixed command count, wall deadline and output cap; no shell or services."""

    def __init__(self, evidence, timeout):
        self.evidence = evidence
        self.timeout = timeout
        # Authentication and cloud credentials are deliberately not inherited.
        self.env = {"PATH": os.environ.get("PATH", os.defpath), "LC_ALL": "C", "LANG": "C"}

    def run(self, argv, *, cleanup=False, operation=None):
        commands = self.evidence["commands"]
        limit = MAX_COMMANDS + (4 if cleanup else 0)
        if len(commands) >= limit:
            raise QualificationError("Bounded command budget exhausted")
        entry = {"argv": [str(arg) for arg in argv], "timeout_seconds": self.timeout,
                 "cleanup": cleanup, "operation": operation}
        commands.append(entry)
        chunks = {"stdout": bytearray(), "stderr": bytearray()}
        try:
            process = subprocess.Popen(entry["argv"], stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=self.env, start_new_session=True)
        except OSError as exc:
            entry["launch_error"] = str(exc)
            raise QualificationError(f"Cannot execute {argv[0]}: {exc}") from exc
        failure = None
        deadline = time.monotonic() + self.timeout
        try:
            with selectors.DefaultSelector() as selector:
                for stream, label in [(process.stdout, "stdout"), (process.stderr, "stderr")]:
                    os.set_blocking(stream.fileno(), False)
                    selector.register(stream, selectors.EVENT_READ, label)
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        failure = "command_timeout"
                        break
                    for key, _ in selector.select(remaining):
                        chunk = os.read(key.fileobj.fileno(), 8192)
                        if not chunk:
                            selector.unregister(key.fileobj)
                        elif len(chunks[key.data]) + len(chunk) > MAX_OUTPUT:
                            failure = "output_limit_exceeded"
                            break
                        else:
                            chunks[key.data].extend(chunk)
                    if failure:
                        break
                if not failure:
                    try:
                        process.wait(timeout=max(0.001, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        failure = "command_timeout"
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
            process.stderr.close()
            entry["returncode"] = process.returncode
            for label, content in chunks.items():
                entry[label] = content.decode("utf-8", errors="backslashreplace")
                entry[label + "_sha256"] = hashlib.sha256(content).hexdigest()
            if failure:
                entry["failure"] = failure
        if failure or process.returncode:
            raise QualificationError(f"{argv[0]} failed: {failure or 'exit ' + str(process.returncode)}")
        return bytes(chunks["stdout"])


def validate_provenance(path, cli_hash):
    """Retain bounded operator provenance, without pretending it is a signature."""
    if not Path(path).is_file() or Path(path).stat().st_size > 16384:
        raise QualificationError("Provenance must be a regular JSON file of at most 16 KiB")
    with Path(path).open("rb") as stream:
        data = stream.read(16385)
    if len(data) > 16384:
        raise QualificationError("Provenance JSON exceeds 16 KiB")
    try:
        source = json.loads(data)
    except (ValueError, UnicodeError) as exc:
        raise QualificationError("Invalid provenance JSON") from exc
    required = {"release", "source_url", "source_commit", "cli_sha256", "server_sha256"}
    if not isinstance(source, dict) or set(source) != required:
        raise QualificationError("Provenance must contain exactly: " + ", ".join(sorted(required)))
    if source["release"] != RELEASE:
        raise QualificationError(f"Provenance release must be {RELEASE}")
    if source["source_commit"] != SOURCE_COMMIT:
        raise QualificationError(f"Provenance source_commit must be {SOURCE_COMMIT}")
    expected_url = "https://github.com/valkey-io/valkey/tree/" + SOURCE_COMMIT
    if source["source_url"] != expected_url:
        raise QualificationError("source_url must identify the exact pinned official source commit")
    for field in ("cli_sha256", "server_sha256"):
        if not isinstance(source[field], str) or not re.fullmatch(r"[a-f0-9]{64}", source[field]):
            raise QualificationError(f"{field} must be a lower-case SHA-256 digest")
    if cli_hash is not None and source["cli_sha256"] != cli_hash:
        raise QualificationError("Installed valkey-cli hash differs from supplied provenance")
    return {**source, "provenance_file_sha256": hashlib.sha256(data).hexdigest(),
            "trust": "operator_supplied_not_independently_authenticated",
            "client_binary_hash_observed_locally": cli_hash is not None,
            "server_binary_hash_observed_locally": False}


def probe(args, evidence, recorder):
    missing = evidence["missing"]
    hardware, unavailable = inventory(args.device, args.interface)
    evidence["inventory"] = hardware
    missing.extend(unavailable)
    cli = shutil.which(args.cli)
    evidence["client"] = {"requested": args.cli, "resolved": cli}
    for field in ("host", "port"):
        if getattr(args, field) is None:
            missing.append(f"Supply --{field} for an existing RDMA endpoint")
    if not cli:
        missing.append("valkey-cli executable is unavailable")
    else:
        try:
            evidence["client"]["sha256"] = sha256_file(cli)
        except (OSError, QualificationError) as exc:
            missing.append(str(exc))
    if not args.provenance:
        missing.append("Supply --provenance with pinned official source and client/server hashes")
    else:
        try:
            evidence["provenance"] = validate_provenance(args.provenance, evidence["client"].get("sha256"))
        except (OSError, QualificationError) as exc:
            missing.append(str(exc))
    if args.action == "plan":
        missing.append("Client version/build/RDMA flags not executed in offline plan; run preflight")
        missing.append("Local diagnostic commands/route not executed in offline plan; run preflight")
        return
    if cli:
        try:
            version = recorder.run([cli, "--version"]).decode("utf-8").strip()
            evidence["client"]["version_output"] = version
            if not re.fullmatch(r"valkey-cli " + re.escape(RELEASE) + r"(?:\s+.*)?", version):
                missing.append(f"Actual valkey-cli version is not the pinned {RELEASE}")
            help_text = recorder.run([cli, "--help"]).decode("utf-8")
            evidence["client"]["rdma_flag_advertised"] = "--rdma" in help_text
            for flag in ("--rdma", "--json", "--quoted-input"):
                if flag not in help_text:
                    missing.append(f"Installed valkey-cli does not advertise {flag} build support")
        except (UnicodeError, QualificationError) as exc:
            missing.append(str(exc))
    diagnostics = []
    if args.device:
        diagnostics.append(["ibv_devinfo", "-d", args.device])
    diagnostics.append(["rdma", "link", "show"])
    if args.host:
        family = "-6" if ipaddress.ip_address(args.host).version == 6 else "-4"
        diagnostics.append(["ip", family, "-j", "route", "get", args.host])
    for command in diagnostics:
        executable = shutil.which(command[0])
        if not executable:
            missing.append(f"Read-only diagnostic executable is unavailable: {command[0]}")
            continue
        try:
            raw = recorder.run([executable, *command[1:]])
            if command[0] == "ip":
                routes = json.loads(raw)
                evidence["route"] = routes
                if not isinstance(routes, list) or len(routes) != 1 or routes[0].get("dev") != args.interface:
                    missing.append("Kernel route does not select the requested IPoIB interface")
        except (ValueError, QualificationError) as exc:
            missing.append(f"Diagnostic {command[0]} failed: {exc}")


def quote_bytes(value):
    # valkey-cli --quoted-input uses C-style \xHH escapes, not JSON \uHHHH.
    return '"' + "".join(f"\\x{byte:02x}" for byte in value) + '"'


class RdmaClient:
    def __init__(self, args, cli, recorder):
        self.base = [cli, "--rdma", "-h", args.host, "-p", str(args.port),
                     "-t", str(args.command_timeout), "-e", "-2", "--quoted-input"]
        self.recorder = recorder

    def call(self, *command, raw=False, cleanup=False):
        output = ["--raw", "-D", ""] if raw else ["--json"]
        arguments = [quote_bytes(arg if isinstance(arg, bytes) else arg.encode("utf-8"))
                     for arg in command]
        result = self.recorder.run([*self.base, *output, *arguments],
                                   cleanup=cleanup, operation=command[0])
        if raw:
            return result
        try:
            return json.loads(result)
        except (ValueError, UnicodeError) as exc:
            raise QualificationError("CLI response was not one exact JSON value") from exc


def exact(actual, expected, observation):
    if type(actual) is not type(expected) or actual != expected:
        raise QualificationError(f"Exact response mismatch: {observation}")


def runtime_identity(client, args, evidence):
    raw = client.call("INFO", "server", raw=True).decode("utf-8")
    fields = dict(line.split(":", 1) for line in raw.splitlines() if ":" in line and not line.startswith("#"))
    evidence["server"] = {key: value for key, value in fields.items() if key in SERVER_FIELDS}
    if fields.get("valkey_version") != RELEASE or not fields.get("redis_build_id"):
        raise QualificationError("Server INFO does not prove pinned Valkey version and build identity")
    mode = fields.get("server_mode", fields.get("redis_mode"))
    if mode != "standalone":
        raise QualificationError("Only a preexisting standalone RDMA endpoint is qualified here")
    config = client.call("CONFIG", "GET", *CONFIG_FIELDS)
    if not isinstance(config, list) or len(config) % 2:
        raise QualificationError("Unexpected CONFIG GET response")
    config = dict(zip(config[::2], config[1::2]))
    evidence["server_config"] = config
    if config.get("rdma-port") != str(args.port) or not config.get("rdma-bind"):
        raise QualificationError("Server RDMA listener configuration is absent or differs from endpoint")
    if config.get("cluster-enabled") != "no":
        raise QualificationError("Cluster endpoints are outside this standalone qualification")
    # This is observed through --rdma; no TCP check/fallback is ever attempted.
    evidence["transport"] = {"requested": "official_valkey_rdma", "rdma_command_completed": True,
                             "device_use_proven": False, "hardware_path_attested": False}
    persistence = client.call("INFO", "persistence", raw=True).decode("utf-8")
    allowed = {"aof_enabled", "aof_last_write_status", "aof_rewrite_in_progress",
               "aof_pending_bio_fsync", "aof_delayed_fsync", "rdb_bgsave_in_progress"}
    evidence["persistence"] = {key: value for line in persistence.splitlines() if ":" in line
                               for key, value in [line.split(":", 1)] if key in allowed}
    evidence["storage_contract"] = {
        "reads": "RAM-resident Valkey values, not NVMe/cold WAL reads",
        "durable_write_settings_observed": all(config.get(key) == value for key, value in {
            "appendonly": "yes", "appendfsync": "always",
            "no-appendfsync-on-rewrite": "no", "maxmemory-policy": "noeviction"}.items()),
        "durability_or_crash_recovery_tested": False,
    }


def qualify_values(client, evidence):
    token = uuid.uuid4().hex
    key = "tidehunter:valkey-ib-qualification:" + token + ":value"
    first = bytes(range(256)) + b"\x00original\r\n" + token.encode("ascii")
    second = token.encode("ascii") + b"overwrite\x00\r\n" + bytes(reversed(range(256)))
    evidence["functional"] = {"key": key, "ttl_ms": KEY_TTL_MS,
                              "original_sha256": hashlib.sha256(first).hexdigest(),
                              "overwrite_sha256": hashlib.sha256(second).hexdigest(),
                              "passed": False}
    mutated = False
    error = None
    try:
        exact(client.call("GET", key), None, "initial namespaced key is absent")
        # Track before send: even an error/timeout can have applied a mutation.
        mutated = True
        exact(client.call("SET", key, first, "NX", "PX", str(KEY_TTL_MS)), "OK", "SET NX")
        exact(client.call("GET", key, raw=True), first, "binary GET after SET")
        exact(client.call("SET", key, second, "XX", "PX", str(KEY_TTL_MS)), "OK", "overwrite XX")
        exact(client.call("GET", key, raw=True), second, "binary GET after overwrite")
        exact(client.call("DEL", key), 1, "DEL existing")
        exact(client.call("GET", key), None, "GET missing after DEL")
        exact(client.call("DEL", key), 0, "DEL missing")
        evidence["functional"]["passed"] = True
    except (QualificationError, KeyboardInterrupt) as exc:
        error = exc
    finally:
        cleanup = {"attempted": mutated, "only_generated_key": key, "verified_absent": False}
        evidence["cleanup"] = cleanup
        if mutated:
            try:
                # Conditional atomic deletion never deletes a colliding/replaced foreign value.
                result = client.call("EVAL", CLEANUP_SCRIPT, "1", key, first, second, cleanup=True)
                if type(result) is not int or result not in (0, 1):
                    raise QualificationError("Cleanup refused a value not owned by this qualification")
                exact(client.call("GET", key, cleanup=True), None, "cleanup missing observation")
                cleanup["verified_absent"] = True
            except (QualificationError, KeyboardInterrupt) as exc:
                cleanup["error"] = str(exc) or "interrupted during cleanup"
                if error is None:
                    error = exc
        uncertain = any((command.get("failure") or command.get("returncode")) and
                        command.get("operation") == "SET" for command in evidence["commands"])
        cleanup["mutation_outcome_uncertain"] = uncertain
        if uncertain:
            cleanup["verified_absent"] = False
            cleanup["note"] = "Failed mutation may complete late; PX TTL is the bounded backstop"
    if error is not None:
        raise error


def safe_name(value):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,63}", value):
        raise argparse.ArgumentTypeError("Expected a device/interface name, not a path or options")
    return value


def numeric_host(value):
    try:
        result = ipaddress.ip_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use a numeric preexisting IPoIB endpoint IP, not a URI/hostname") from exc
    if result.is_unspecified or result.is_multicast or result.is_loopback:
        raise argparse.ArgumentTypeError("Endpoint must be a unicast, non-loopback IPoIB address")
    return str(result)


def bounded_int(low, high):
    def parse(value):
        try:
            number = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("Expected an integer") from exc
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"Expected {low}..{high}")
        return number
    return parse


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("action", choices=("plan", "preflight", "qualify"), nargs="?", default="plan",
                        help="plan is offline; preflight runs local read-only diagnostics; qualify explicitly contacts RDMA")
    result.add_argument("--cli", default="valkey-cli", help="Existing official RDMA-capable valkey-cli; never installed here")
    result.add_argument("--device", type=safe_name, help="Requested existing InfiniBand verbs device (inventory, not binding)")
    result.add_argument("--interface", type=safe_name, help="Requested existing configured IPoIB interface")
    result.add_argument("--host", type=numeric_host, help="Numeric address of an existing RDMA listener")
    result.add_argument("--port", type=bounded_int(1, 65535), help="Explicit RDMA listener port; no default/TCP fallback")
    result.add_argument("--provenance", type=Path, help="Existing bounded JSON provenance file; see README")
    result.add_argument("--command-timeout", type=bounded_int(1, 10), default=5,
                        help="Per-command wall timeout in seconds, 1..10 (default 5)")
    result.add_argument("--output", type=Path, help="New evidence JSON file, required for qualify; never overwritten")
    return result


def main(argv=None):
    argument_parser = parser()
    args = argument_parser.parse_args(argv)
    if args.action == "qualify" and args.output is None:
        argument_parser.error("qualify requires --output to retain evidence")
    # Reserve evidence before any mutation; unavailable/unwritable paths cannot lose run evidence.
    output = None
    if args.output:
        try:
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            output = os.fdopen(descriptor, "w", encoding="utf-8")
        except OSError as exc:
            argument_parser.error(f"Cannot reserve new evidence file: {exc}")
    evidence = {"schema": 1, "action": args.action, "release_required": RELEASE,
                "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "status": "not_qualified", "missing": [], "commands": [],
                "performance_measurement": False,
                "hardware_path_attested": False, "cloud_or_service_lifecycle_actions": False}
    exit_code = 2
    try:
        recorder = Recorder(evidence, args.command_timeout)
        probe(args, evidence, recorder)
        if args.action == "plan":
            evidence["status"] = "plan_only"
            exit_code = 0
        elif evidence["missing"]:
            evidence["status"] = "preflight_missing_requirements"
        elif args.action == "preflight":
            evidence["status"] = "local_preflight_ready_not_transport_qualified"
            exit_code = 0
        else:
            client = RdmaClient(args, evidence["client"]["resolved"], recorder)
            runtime_identity(client, args, evidence)
            qualify_values(client, evidence)
            evidence["status"] = "rdma_functional_qualified_not_hardware_attested"
            exit_code = 0
    except (OSError, ValueError, UnicodeError, QualificationError, KeyboardInterrupt) as exc:
        evidence["status"] = "qualification_failed"
        evidence["error"] = str(exc) or "interrupted"
    finally:
        evidence["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        if output:
            with output:
                output.write(rendered)
                output.flush()
                os.fsync(output.fileno())
        print(rendered, end="")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
