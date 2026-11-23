#!/usr/bin/env python3
"""Bounded, randomized local campaigns with preserved inputs and real adapters."""
import argparse
import copy
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import sys
import signal
import time
import uuid
import zipfile

import local
import analyze
import ack_contract as contracts
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import select_backends


BACKENDS = {"native", "hybrid", "rocksdb", "blobdb", "tikv", "cassandra", "foundationdb",
            "valkey_tcp", "valkey_rdma", "garnet", "kvrocks"}


def digest(path, algorithm="sha256"):
    h = hashlib.new(algorithm)
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def admit_config(binary, config_path, run_dir, environment=None, *, timeout=30):
    result = subprocess.run([str(binary), "--config", str(config_path), "--validate-config"],
                            cwd=local.ROOT, capture_output=True, text=True, timeout=timeout, env=environment)
    # Full NVMe read catalogs can exceed the ordinary control-receipt limit;
    # this is local output from the admitted binary, not an SSM/S3 envelope.
    if len(result.stdout) + len(result.stderr) > 96 * 1024 * 1024:
        raise ValueError("configuration admission output exceeds bounded local limit")
    (run_dir / "admission.log").write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"configuration preflight failed ({result.returncode}); see admission.log")
    envelope = json.loads(result.stdout)
    normalized = envelope.get("admitted_config")
    if not isinstance(normalized, dict):
        raise ValueError("preflight omitted JSON admitted_config; no database was started")
    normalized_path = run_dir / "normalized-config.json"
    temporary = normalized_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(normalized_path)
    return normalized


def admitted_fdb_environment(identity):
    library = Path(identity["path"]).resolve(strict=True)
    if digest(library) != identity["sha256"] or digest(library, "blake2b") != identity["blake2b512"]:
        raise RuntimeError("admitted FoundationDB native client bytes changed")
    environment = dict(os.environ)
    environment["LD_LIBRARY_PATH"] = str(library.parent)
    environment["FDB_CLIENT_LIB_DIR"] = str(library.parent)
    environment["FDB_CLIENT_LIB_PATH"] = str(library.parent)
    environment.pop("LD_PRELOAD", None)
    environment.pop("LD_AUDIT", None)
    return environment


def verify_run_identity(result_dir, receipt, normalized_config, fdb_identity=None, *, allow_failed=False):
    manifest = json.loads((result_dir / "manifest.json").read_text())
    run = manifest["run"]
    statuses = {"completed", "failed"} if allow_failed else {"completed"}
    if manifest.get("status") not in statuses or run["config"] != normalized_config:
        raise RuntimeError("Rust manifest status/config differs from the admitted execution boundary")
    if run["binary"].get("blake2b512") != receipt["binary_blake2b512"]:
        raise RuntimeError("Rust manifest binary Blake2b-512 differs from admitted executable")
    if fdb_identity:
        loaded = manifest["backend_info"]["configuration"]["loaded_native_library"]
        if Path(loaded["path"]) != Path(fdb_identity["path"]) or loaded.get("blake2b512") != fdb_identity["blake2b512"]:
            raise RuntimeError("FoundationDB actually loaded native library path/hash differs from admitted client")
        admitted_fdb_environment(fdb_identity)
        receipt["loaded_native_library"] = loaded


def planned_timing_rows(config):
    if config.get("workload", {}).get("kind") == "sui_mainnet_replay":
        manifest = json.loads(Path(config["workload"]["manifest_path"]).read_text())
        selected = config["workload"].get("node_databases")
        batches = [batch for batch in manifest["batches"]
                   if selected is None or batch["db_id"] in selected]
        # The v1 trace declares all store reads in the perpetual objects table.
        reads = manifest["store_reads"] if selected is None or 0 in selected else 0
        return {"preload": 0, "warmup": 0, "aging": 0,
                "measurement": (reads + len(batches)) * config["workload"]["repetitions"]}
    expected = {
        "preload": config["dataset"]["keys"] if config["phases"]["preload"] else 0,
        "warmup": config["phases"]["warmup_operations"],
        "aging": config["phases"]["aging_operations"],
        "measurement": config["phases"]["measurement_operations"],
    }
    if config["phases"].get("verify_existing_dataset", False):
        expected["verify_existing_dataset"] = config["dataset"]["keys"]
    if config.get("qualification_only", False):
        expected = dict.fromkeys(expected, 0)
    return expected


def verify_timing_result(result_dir, config, *, allow_partial=False, phase_windows=None):
    expected = planned_timing_rows(config)
    counts = dict.fromkeys(expected, 0)
    for row in analyze.read_samples(result_dir / "samples.csv", measurement_only=False):
        if row["phase"] not in counts:
            raise ValueError("unrecognized raw timing phase")
        counts[row["phase"]] += 1
        if phase_windows is not None:
            window = phase_windows.get(row["phase"])
            if window is None:
                window = {"start_ns": row["scheduled_ns"], "end_ns": row["completed_ns"],
                          "rows": 0, "unsuccessful_rows": 0}
                phase_windows[row["phase"]] = window
            window["start_ns"] = min(window["start_ns"], row["scheduled_ns"])
            window["end_ns"] = max(window["end_ns"], row["completed_ns"])
            window["elapsed_ns"] = window["end_ns"] - window["start_ns"]
            window["rows"] += 1
            window["unsuccessful_rows"] += row["outcome"] != "success"
    if any(counts[name] > expected[name] for name in counts) or (not allow_partial and counts != expected):
        raise ValueError(f"raw timing rows differ from admitted phase bounds: expected={expected}, observed={counts}")
    return counts


def finalize_run(run_dir, state_dir, external, reuse, receipt):
    failures = []
    if external and receipt.get("status") == "completed" and not (state_dir / "owner.json").exists():
        failures.append({"phase": "load_owned_cleanup_state", "error": "owner receipt disappeared after successful database run"})
    if external and (state_dir / "owner.json").exists():
        try:
            state = local.load(state_dir)
        except BaseException as exc:
            failures.append({"phase": "load_owned_cleanup_state", "error": f"{type(exc).__name__}: {exc}"})
        else:
            try:
                local.export(state, run_dir / "deployment")
            except BaseException as exc:
                failures.append({"phase": "deployment_export", "error": f"{type(exc).__name__}: {exc}"})
            if not reuse:
                try:
                    local.stop(state)
                except BaseException as exc:
                    failures.append({"phase": "exact_owned_stop", "error": f"{type(exc).__name__}: {exc}"})
    if not external:
        database = run_dir / "database"
        try:
            if database.exists() or database.is_symlink():
                shutil.rmtree(database)
            receipt["database_files_retained"] = False
        except BaseException as exc:
            receipt["database_files_retained"] = True
            failures.append({"phase": "exact_owned_database_files", "error": f"{type(exc).__name__}: {exc}"})
    if failures:
        receipt["status"] = "failed"
        receipt.setdefault("cleanup_errors", []).extend(failures)
        reasons = [receipt["error"]] if receipt.get("error") else []
        reasons.extend(f"{item['phase']}: {item['error']}" for item in failures)
        receipt["error"] = "; ".join(reasons)
    receipt["finished_at_unix"] = time.time()
    local.dump(run_dir / "receipt.json", receipt)
    if failures:
        raise RuntimeError("run export/cleanup failed; receipt marked failed: " + receipt["error"])


def bounded_int(value, lower, upper, name):
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ValueError(f"{name} must be integer in [{lower}, {upper}]")
    return value


def matrix(spec):
    allowed = {"schema_version", "seed", "repeats", "layout", "backends", "profiles", "rates", "base_config", "max_runs", "client_cpus", "client_memory_mib"}
    if set(spec) - allowed or spec.get("schema_version") != 1:
        raise ValueError("unsupported campaign schema or unknown fields")
    bounded_int(spec.get("seed"), 0, 2**63 - 1, "seed")
    repeats = bounded_int(spec.get("repeats"), 1, 100, "repeats")
    maximum = bounded_int(spec.get("max_runs"), 1, 10000, "max_runs")
    bounded_int(spec.get("client_cpus", 2), 1, 64, "client_cpus")
    bounded_int(spec.get("client_memory_mib", 2048), 256, 65536, "client_memory_mib")
    if spec.get("layout") not in ("qualification", "distributed"):
        raise ValueError("layout must explicitly name qualification or distributed")
    supported = BACKENDS
    backends, profiles, rates = spec.get("backends", []), spec.get("profiles", []), spec.get("rates", [])
    if (not isinstance(backends, list) or not backends or any(not isinstance(x, str) for x in backends)
            or set(backends) - supported or len(backends) != len(set(backends))):
        raise ValueError("backends must be unique supported backend names")
    if not profiles or any(set(x) != {"name", "keys", "aging_operations"} for x in profiles):
        raise ValueError("profiles require explicit name, keys and aging_operations")
    if len({x["name"] for x in profiles}) != len(profiles):
        raise ValueError("duplicate profile name")
    for profile in profiles:
        if not isinstance(profile["name"], str) or not profile["name"] or not profile["name"].replace("-", "").replace("_", "").isalnum():
            raise ValueError("profile names must be simple filesystem-safe labels")
        bounded_int(profile["keys"], 1, 10000000, "profile keys")
        bounded_int(profile["aging_operations"], 0, 100000000, "finite aging_operations")
    if not rates or len(rates) != len(set(rates)):
        raise ValueError("rates must be a nonempty unique fixed-rate sweep")
    for rate in rates:
        bounded_int(rate, 1, 10000000, "offered rate")
    size = repeats * len(backends) * len(profiles) * len(rates)
    if size > maximum:
        raise ValueError(f"matrix has {size} runs, exceeding explicit max_runs={maximum}")
    jobs = [{"repeat": repeat, "backend": backend, "profile": profile, "rate": rate}
            for repeat, backend, profile, rate in itertools.product(range(repeats), backends, profiles, rates)]
    random.Random(spec["seed"]).shuffle(jobs)
    return jobs


def source_snapshot(output):
    paths = local.command(["git", "ls-files", "-z"]).split("\0")
    framework = ["experiments/kv-comparison", "experiments/portable_transport.py",
                 "benchmarks/kv-research", "tidehunter/src", "tidehunter/examples",
                 "tidehunter-fabric/src", "tidehunter-fabric/tests",
                 ".github/workflows/kv-comparison.yaml", ":(glob).github/scripts/kv-comparison*",
                 "docs/distributed/KV_COMPARISON_PROTOCOL.md"]
    paths += local.command(["git", "ls-files", "--others", "--exclude-standard", "-z", "--", *framework]).split("\0")
    hashes = {}
    with zipfile.ZipFile(output / "source.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        # Include dirty tracked bytes and new framework files, not database/runtime state.
        candidates = {local.ROOT / p for p in paths if p}
        excluded_parts = {".state", "__pycache__", "target", "results", "artifacts", "secrets", "credentials"}
        for path in sorted(candidates):
            relative = path.relative_to(local.ROOT)
            if relative.parts[:2] == ("experiments", "valkey-infiniband"):
                continue
            if any(part in excluded_parts for part in relative.parts) or path.is_symlink():
                continue
            if path.is_file() and (path.suffix in {".rs", ".toml", ".lock", ".py", ".json", ".yaml", ".yml", ".sh", ".md"} or path.name in {".gitignore", "Dockerfile"}):
                name = str(path.relative_to(local.ROOT))
                fingerprint = hashlib.sha256()
                with path.open("rb") as source, archive.open(name, "w") as destination:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        fingerprint.update(block)
                        destination.write(block)
                hashes[name] = fingerprint.hexdigest()
    local.dump(output / "source.json", {"revision": local.command(["git", "rev-parse", "HEAD"]).strip(), "sha256": hashes,
                                        "snapshot_sha256": digest(output / "source.zip")})


def backend_options(kind, state, output, ack_contract="synced"):
    contract = contracts.selected(kind, ack_contract)
    if state and state.get("ack_contract", "synced") != contract:
        raise ValueError("local backend acknowledgement contract differs")
    nodes = state["nodes"] if state else 1
    if kind == "tikv":
        return {"pd_endpoints": ["127.0.0.1:2379"], "pd_api_endpoints": ["http://127.0.0.1:2379"],
                "required_replication_factor": nodes}
    if kind == "cassandra":
        return {"nodes": [f"127.0.0.{11+i}:9042" for i in range(nodes)], "keyspace": "kv_comparison",
                "required_replication_factor": nodes, "consistency": "quorum"}
    if kind == "foundationdb":
        return {"cluster_file": str(Path(state["directory"]) / "fdb.cluster"), "required_replication_factor": 1 if nodes == 1 else 2}
    if kind in ("garnet", "kvrocks", "valkey_rdma", "valkey_tcp"):
        if state and "backend_options" in state:
            options = copy.deepcopy(state["backend_options"])
        elif kind in ("garnet", "kvrocks"):
            import industry_service
            options = industry_service.options(kind, industry_service.local_endpoints())
        elif kind == "valkey_rdma":
            import local_valkey_rdma
            options = local_valkey_rdma.options(nodes)
        elif state and state.get("paired_valkey"):
            import local_valkey_rdma
            rdma = local_valkey_rdma.options(nodes)
            options = local_valkey_rdma.options_for_backend(kind, nodes, rdma["nodes"])
        else:
            options = {"nodes": [f"127.0.0.{21+i}:{6379+i}" for i in range(nodes)],
                       "cluster": nodes > 1, "required_replication_factor": 1,
                       "required_primary_nodes": nodes, "request_timeout_ms": 5000}
        if contract != "synced":
            options["ack_contract"] = contract
        return options
    if kind == "hybrid":
        # Rootless leased ordinary-file fixture: the coordinator and its owner
        # process run here over trusted loopback TCP. The Rust backend kind is
        # `tidehunter`; an absent external_remote_config selects the fixture,
        # which only a test-utils Linux build admits.
        return {"transport": {"kind": "tcp-trusted"},
                "path": str((Path(output) / "database").resolve()),
                "owners": 1,
                "durability_tier": "native",
                # Qualification-only bound: smaller than the adapter default
                # (16384) so a zero-operation cell reserves no retained-batch
                # capacity it cannot use. Measured 2026-11-23: this knob does
                # not move the open peak (the index WAL window does), so it is
                # not a memory lever.
                "max_batches_per_partition": 256,
                # The fixture geometry's own complete-batch bound (320 KiB);
                # a larger limit would not fit one 4 MiB live fragment.
                "batch_limits": {"max_operations": 32, "max_encoded_bytes": 320 * 1024}}
    return {"path": str(output / "database")}


def build_config(base, job, state, output, seed, qualification):
    config = copy.deepcopy(base)
    kind = job["backend"]
    contract = contracts.selected(kind, job.get("ack_contract", "synced"))
    kwargs = {"ack_contract": contract} if contract != "synced" else {}
    if kind not in BACKENDS:
        raise ValueError("unsupported local campaign backend; use local_dry_run.py for leased Tidehunter")
    if kind == "hybrid" and not qualification:
        # The leased fixture is a rootless loopback route with no independent
        # device, so its timing is no performance claim. At production index
        # geometry it also opens above the one manifest memory budget (3 GiB of
        # pre-faulted index WAL fragments). Qualification is the only run this
        # route admits.
        raise ValueError("hybrid runs only as an API qualification cell; use "
                         "local_dry_run.py for a leased Tidehunter timing case")
    options = backend_options(kind, state, output, **kwargs)
    # The manifest names the engine that ships; the Rust registry names the
    # adapter route. `hybrid` is the leased route, whose kind is `tidehunter`.
    config["backend"] = {"kind": "tidehunter" if kind == "hybrid" else kind, "options": options}
    if kind == "native":
        config["backend"]["options"].update(
            durability="page_cache",
            native_wal_geometry="small" if qualification else "native_default")
    if kind == "hybrid":
        # The refusal above leaves qualification as the only hybrid run, so the
        # native-scale geometry below always applies to this route.
        # The coordinator inherits Sui's production index geometry by default:
        # a 1 GiB fragment, so the three eagerly pre-faulted writer-window
        # fragments of its index WAL are 3 GiB resident at open. Measured
        # 2026-11-23: 3087 MiB of coordinator RSS for zero operations. A
        # qualification cell writes a handful of keys, so it uses the native
        # qualification geometry instead (1 MiB fragment, 4 MiB WAL) and stays
        # inside the single manifest budget; measured 19 MiB.
        config["backend"]["options"]["index"] = {
            "frag_size": 1024 * 1024, "wal_file_size": 4 * 1024 * 1024}
    config["seed"] = seed
    config["namespace"] = "local-" + uuid.uuid4().hex
    config["dataset"]["keys"] = job["profile"]["keys"]
    config["phases"]["aging_operations"] = job["profile"]["aging_operations"]
    config["execution"]["mode"] = "open_loop"
    config["execution"]["offered_ops_per_second"] = job["rate"]
    config["qualification_only"] = qualification
    if qualification:
        config["phases"] = {"preload": False, "warmup_operations": 0, "aging_operations": 0, "measurement_operations": 0}
        config["execution"]["request_timeout_ms"] = 5000
        config["limits"]["deadline_seconds"] = 90
        config["limits"]["shutdown_grace_seconds"] = 10
    return config

def group_resources(group):
    """Record all owned runner/coordinator/owner processes, not only client RSS."""
    result = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            text = (entry / "stat").read_text()
            fields = text[text.rfind(")") + 2:].split()
            if int(fields[2]) != group:
                continue
            counters = dict(line.split(":", 1) for line in (entry / "status").read_text().splitlines() if ":" in line)
            result.append({"pid": int(entry.name), "name": counters["Name"].strip(),
                           "rss_bytes": int(counters.get("VmRSS", "0 kB").split()[0]) * 1024,
                           "user_ticks": int(fields[11]), "system_ticks": int(fields[12])})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def supervise(argv, log_path, resource_path, cpus, memory_mib, deadline_seconds, grace_seconds, environment=None):
    """Own a fresh process group; wall deadline covers even blocked native calls."""
    process = subprocess.Popen(argv, cwd=local.ROOT, stdout=log_path, stderr=subprocess.STDOUT,
                               start_new_session=True, preexec_fn=lambda: os.sched_setaffinity(0, cpus), env=environment)
    deadline = time.monotonic() + deadline_seconds + grace_seconds
    def exited():
        return os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    try:
        with resource_path.open("w") as resources:
            while exited() is None:
                if time.monotonic() >= deadline:
                    raise RuntimeError("independent wall deadline expired; mutation outcome may be unknown")
                try:
                    processes = group_resources(process.pid)
                    rss = sum(item["rss_bytes"] for item in processes)
                    resources.write(json.dumps({"monotonic_ns": time.monotonic_ns(), "process_group": process.pid,
                                                "aggregate_rss_bytes": rss, "processes": processes}) + "\n")
                    resources.flush()
                    if rss > memory_mib * 1024**2:
                        raise RuntimeError("client sampled RSS budget exceeded")
                except FileNotFoundError:
                    if exited() is None:
                        raise
                time.sleep(.25)
        status = exited()
        return status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
    finally:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        # WNOWAIT above keeps our leader unreaped, preventing PID/group reuse until
        # every signal has been sent to this exact originally-owned process group.
        stop_deadline = time.monotonic() + max(1, min(grace_seconds, 10))
        while exited() is None and time.monotonic() < stop_deadline:
            time.sleep(.05)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)



def observer_snapshot(requested):
    """Only attest the exact known Compose project; arbitrary observers remain unknown."""
    project = "kv-comparison-observability"
    try:
        ids = local.command(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + project]).split()
        items = json.loads(local.command(["docker", "inspect", *ids])) if ids else []
        services = []
        for item in items:
            labels = item["Config"]["Labels"]
            if labels.get("com.docker.compose.project") != project:
                raise RuntimeError("observer inventory project changed during inspection")
            services.append({"service": labels.get("com.docker.compose.service"), "image_id": item["Image"],
                             "image": item["Config"]["Image"], "memory_bytes": item["HostConfig"]["Memory"],
                             "nano_cpus": item["HostConfig"]["NanoCpus"],
                             "health": item["State"].get("Health", {}).get("Status", "unavailable")})
        return {"requested": requested,
                "state": "exact_project_present" if services else "exact_project_absent_other_observers_unknown",
                "services": sorted(services, key=lambda item: item["service"] or "")}
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        return {"requested": requested, "state": "unknown", "error_type": type(exc).__name__, "services": []}


def qualify_mainnet_resp(binary, state, output):
    """Replay on this cell's exact-owned servers before its normal teardown."""
    import mainnet_replay_resp as replay

    output.mkdir()
    reset = {"reason": "clear completed synthetic cell before replay fresh-empty admission",
             "acknowledged_flushes": []}
    local.dump(output / "reset.json", reset)

    def flush():
        local.inspect_owned(state)
        for endpoint in state["backend_options"]["nodes"]:
            if replay.industry_service.request(endpoint, "FLUSHALL") != "OK":
                raise RuntimeError("owned replay database reset failed")
            reset["acknowledged_flushes"].append(endpoint)
            local.dump(output / "reset.json", reset)

    flush()
    freshness = replay.check_nonempty_rejection(
        binary, output, replay.configuration(state["backend"], state["backend_options"], 1))
    evidence = []
    for concurrency in (1, 4):
        flush()
        config = replay.configuration(state["backend"], state["backend_options"], concurrency)
        path = output / f"N{concurrency}.json"
        local.dump(path, config)
        result = output / f"N{concurrency}"
        with (output / f"N{concurrency}.log").open("w") as log:
            subprocess.run([str(binary), "--config", str(path), "--output", str(result)],
                           stdout=log, stderr=subprocess.STDOUT, timeout=75, check=True)
        evidence.append(replay.check_result(result))
    if evidence[0]["verification"] != evidence[1]["verification"]:
        raise RuntimeError("N=1 and N=4 replay final contents differ")
    receipt = {"status": "completed", "runs": evidence, "freshness_regression": freshness,
               "reset": reset, "reused_exact_owned_synthetic_lifecycle": True}
    local.dump(output / "results.json", receipt)
    return receipt


def run_one(args, spec, base, job, index):
    run_dir = args.output / f"{index:04d}-{job['backend']}-{job['profile']['name']}-r{job['rate']}-n{job['repeat']}"
    run_dir.mkdir()
    state_dir = args.reuse_state.resolve() if args.reuse_state else run_dir / "server"
    state = None
    external = job["backend"] in ("tikv", "cassandra", "foundationdb", "valkey_tcp", "valkey_rdma", "garnet", "kvrocks")
    receipt = {"job": job, "status": "starting", "started_at_unix": time.time(),
               "physical_hosts": 1, "shared_ssd": True, "cloud_billing_estimate": None,
               "claim_boundary": "API qualification only" if args.qualification else "local shared-SSD timing; no HA or independent-NVMe claim",
               "client_budget": {"cpus": spec.get("client_cpus", 2), "memory_mib": spec.get("client_memory_mib", 2048)},
               "observation": {"requested": args.observe, "state": "not_yet_observed"},
               "storage_population": "reused database/new isolated namespace; not fresh physical state" if args.reuse_state else "fresh owned database per cell",
               "hardware": {"machine": platform.machine(), "cpu": Path("/proc/cpuinfo").read_text(), "kernel": platform.release(),
                            "affinity": sorted(os.sched_getaffinity(0)), "memory": Path("/proc/meminfo").read_text(),
                            "filesystem": local.command(["findmnt", "-J", "-T", str(run_dir)]),
                            "block_devices": local.command(["lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MODEL,MOUNTPOINTS"])}}
    local.dump(run_dir / "receipt.json", receipt)
    try:
        contract = contracts.selected(job["backend"], job.get("ack_contract", "synced"))
        prospective = {"nodes": (5 if job["backend"] in ("garnet", "kvrocks") else
                                 1 if spec["layout"] == "qualification" else
                                 5 if job["backend"] in ("valkey_tcp", "valkey_rdma") else 3),
                       "directory": str(state_dir),
                       "paired_valkey": job["backend"] == "valkey_tcp" and "valkey_rdma" in spec["backends"]} if external else None
        if prospective is not None and contract != "synced":
            prospective["ack_contract"] = contract
        if args.reuse_state:
            prospective = local.load(state_dir)
            if prospective.get("ack_contract", "synced") != contract:
                raise ValueError("reused backend acknowledgement contract differs")
            if (job["backend"] == "valkey_tcp" and "valkey_rdma" in spec["backends"]
                    and prospective.get("paired_valkey") is not True):
                raise ValueError("paired Valkey comparison cannot reuse the ordinary TCP deployment")
        config = build_config(base, job, prospective, run_dir, spec["seed"], args.qualification)
        config["source_revision"] = local.command(["git", "rev-parse", "HEAD"]).strip()
        local.dump(run_dir / "config.json", config)
        # Reject unsupported binary features/invalid budgets before pulling or starting DBs.
        fdb_identity = args.fdb_client_identity if job["backend"] == "foundationdb" else None
        child_environment = admitted_fdb_environment(fdb_identity) if fdb_identity else None
        config = admit_config(args.binary, run_dir / "config.json", run_dir, child_environment)
        receipt["config_sha256"] = digest(run_dir / "config.json")
        receipt["normalized_config_sha256"] = digest(run_dir / "normalized-config.json")
        receipt["binary_sha256"] = digest(args.binary)
        receipt["binary_blake2b512"] = digest(args.binary, "blake2b")
        if fdb_identity:
            receipt["admitted_native_library"] = fdb_identity
            receipt["native_loader_policy"] = "exact admitted LD_LIBRARY_PATH; LD_PRELOAD and LD_AUDIT removed"
        if external:
            if args.reuse_state:
                state = local.load(state_dir)
                local.ready(state, 180)
            else:
                local.start(argparse.Namespace(state=state_dir, backend=job["backend"], layout=spec["layout"], timeout=180,
                                               paired_valkey=job["backend"] == "valkey_tcp" and "valkey_rdma" in spec["backends"],
                                               **({"ack_contract": contract} if contract != "synced" else {})))
                state = local.load(state_dir)
        receipt["status"] = "running"
        local.dump(run_dir / "receipt.json", receipt)
        result_dir = run_dir / "result"
        if args.observe:
            local.command([sys.executable, local.HERE / "observability/stack.py", "configure", "--results", result_dir])
            local.command([sys.executable, local.HERE / "observability/stack.py", "start"], timeout=180)
        receipt["observation"] = observer_snapshot(args.observe)
        if args.observe and (receipt["observation"]["state"] != "exact_project_present"
                             or {item["service"] for item in receipt["observation"]["services"]} != {"prometheus", "grafana", "exporter", "node"}
                             or any(item["health"] != "healthy" for item in receipt["observation"]["services"])):
            raise RuntimeError("requested observer project is missing, incomplete, unhealthy or unobservable")
        local.dump(run_dir / "receipt.json", receipt)
        cpus = sorted(os.sched_getaffinity(0))[:spec.get("client_cpus", 2)]
        if len(cpus) != spec.get("client_cpus", 2):
            raise RuntimeError("requested client CPU budget exceeds process affinity")
        # CPU affinity is enforced; RSS budget is sampled at 250ms, not a hard cgroup.
        with (run_dir / "run.log").open("w") as log:
            returncode = supervise([str(args.binary), "--config", str(run_dir / "normalized-config.json"), "--output", str(result_dir)],
                                   log, run_dir / "client-resources.jsonl", cpus,
                                   spec.get("client_memory_mib", 2048),
                                   config["limits"]["deadline_seconds"], config["limits"]["shutdown_grace_seconds"], child_environment)
        timeline = analyze.admit_write_timeline(
            run_dir, config, completed=returncode == 0, expected_backend=job["backend"])
        if timeline is not None:
            receipt["write_timeline"] = timeline
        receipt["observation_after"] = observer_snapshot(args.observe)
        if receipt["observation_after"] != receipt["observation"]:
            raise RuntimeError("observer presence/health/resource identity changed during run; no matched observation contract")
        if returncode:
            raise RuntimeError(f"actual adapter run exited {returncode}; see {run_dir / 'run.log'}")
        if not all((result_dir / name).is_file() for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")):
            raise RuntimeError("actual runner omitted required raw/result artifacts")
        receipt["artifacts_sha256"] = {name: digest(result_dir / name)
                                        for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}
        evidence = result_dir / "measurement-read-evidence.json"
        if evidence.is_file():
            receipt["measurement_read_evidence"] = {"path": "result/measurement-read-evidence.json",
                                                     "sha256": digest(evidence)}
        marker = json.loads((result_dir / "manifest.json").read_text()).get("run", {}).get("measurement_read_evidence_schema_version")
        if marker is not None and (type(marker) is not int or marker != 1):
            raise RuntimeError("unsupported measurement read evidence capability")
        if marker == 1 and not evidence.is_file():
            raise RuntimeError("advertised measurement read evidence artifact is missing")
        verify_run_identity(result_dir, receipt, config, fdb_identity)
        receipt["validated_timing_rows_by_phase"] = verify_timing_result(result_dir, config)
        if external:
            local.ready(state, 60)
        if (getattr(args, "mainnet_replay_resp", False)
                and job["backend"] in ("valkey_tcp", "garnet", "kvrocks")):
            receipt["mainnet_replay"] = qualify_mainnet_resp(
                args.binary, state, run_dir / "mainnet-replay")
        receipt["status"] = "completed"
    except BaseException as exc:
        receipt["status"] = "failed"
        receipt["error"] = str(exc)
        raise
    finally:
        partial_error = None
        if (run_dir / "result/write-timeline.json").is_file() and "write_timeline" not in receipt:
            try:
                admitted = json.loads((run_dir / "normalized-config.json").read_text())
                timeline = analyze.admit_write_timeline(run_dir, admitted, completed=False,
                                                        expected_backend=job["backend"])
                if timeline is not None:
                    receipt["write_timeline"] = timeline
            except (OSError, ValueError, KeyError, TypeError) as exc:
                receipt["status"] = "failed"
                receipt["write_timeline_admission_error"] = str(exc)
                partial_error = exc
        finalize_run(run_dir, state_dir, external, bool(args.reuse_state), receipt)
        if partial_error is not None:
            raise ValueError("partial write timeline admission failed") from partial_error


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--binary", type=Path, default=local.ROOT / "target/release/kv-research")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--backends", help="comma-separated nonempty subset of backend IDs in the tracked manifest")
    p.add_argument("--reuse-state", type=Path, help="reuse exact-owned ready deployment; implies explicitly labeled nonfresh storage and leaves it running")
    p.add_argument("--qualification", action="store_true", help="real API qualification, not performance")
    p.add_argument("--observe", action="store_true", help="explicitly configure/start monitoring per cell before timing")
    p.add_argument("--mainnet-replay-resp", action="store_true",
                   help="qualify two-repetition N=1/N=4 RESP replay within each owned server lifecycle")
    args = p.parse_args()
    args.output = args.output.resolve()
    args.binary = args.binary.resolve()
    if args.output.exists():
        raise ValueError("output must be fresh; refusing to overwrite provenance")
    spec = json.loads(args.manifest.read_text())
    spec["backends"] = select_backends(args.backends, spec.get("backends"), BACKENDS)
    jobs = matrix(spec)
    if args.reuse_state:
        if len(spec["backends"]) != 1:
            raise ValueError("--reuse-state requires exactly one selected backend")
        reused = local.load(args.reuse_state)
        if reused["backend"] != spec["backends"][0] or reused["layout"] != spec["layout"]:
            raise ValueError("reused backend/layout differs from campaign declaration")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise RuntimeError("requested kv-research binary missing/not executable; build explicitly first")
    client_identity = None
    if "foundationdb" in [x["backend"] for x in jobs]:
        library = Path(os.environ.get("FDB_CLIENT_LIB_DIR", "/nonexistent")) / "libfdb_c.so"
        metadata = library.parent.parent / "client.json"
        if not library.is_file() or not metadata.is_file():
            raise RuntimeError("matching FDB_CLIENT_LIB_DIR/libfdb_c.so and client.json missing; run local.py prepare-fdb")
        client_identity = json.loads(metadata.read_text())
        if client_identity.get("version") != "7.3.77" or client_identity.get("api") != 730 or client_identity.get("sha256") != digest(library):
            raise RuntimeError("FoundationDB user-prefix runtime library provenance/version/API mismatch")
        observed_blake = digest(library, "blake2b")
        if client_identity.get("blake2b512", observed_blake) != observed_blake:
            raise RuntimeError("FoundationDB client metadata Blake2b-512 mismatch")
        client_identity.update({"path": str(library.resolve(strict=True)), "blake2b512": observed_blake})
    args.fdb_client_identity = client_identity
    base_path = (args.manifest.resolve().parent / spec["base_config"]).resolve()
    base = json.loads(base_path.read_text())
    args.output.mkdir(parents=True)
    local.dump(args.output / "campaign.json", spec)
    local.dump(args.output / "order.json", jobs)
    local.dump(args.output / "base-config.json", base)
    if client_identity:
        local.dump(args.output / "fdb-client.json", client_identity)
    source_snapshot(args.output)
    for index, job in enumerate(jobs):
        run_one(args, spec, base, job, index)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}; retaining failure artifacts and cleaning exact-owned resources")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"kv-comparison campaign error: {exc}", file=sys.stderr)
        sys.exit(1)
