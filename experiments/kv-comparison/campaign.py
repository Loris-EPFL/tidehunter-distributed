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


def digest(path, algorithm="sha256"):
    h = hashlib.new(algorithm)
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def admit_config(binary, config_path, run_dir, environment=None):
    result = subprocess.run([str(binary), "--config", str(config_path), "--validate-config"],
                            cwd=local.ROOT, capture_output=True, text=True, timeout=30, env=environment)
    (run_dir / "admission.log").write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"configuration preflight failed ({result.returncode}); see admission.log")
    envelope = json.loads(result.stdout)
    normalized = envelope.get("admitted_config")
    if not isinstance(normalized, dict):
        raise ValueError("preflight omitted JSON admitted_config; no database was started")
    local.dump(run_dir / "normalized-config.json", normalized)
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


def verify_timing_result(result_dir, config, *, allow_partial=False):
    counts = dict.fromkeys(("preload", "warmup", "aging", "measurement"), 0)
    for row in analyze.read_samples(result_dir / "samples.csv", measurement_only=False):
        if row["phase"] not in counts:
            raise ValueError("unrecognized raw timing phase")
        counts[row["phase"]] += 1
    expected = {
        "preload": config["dataset"]["keys"] if config["phases"]["preload"] else 0,
        "warmup": config["phases"]["warmup_operations"],
        "aging": config["phases"]["aging_operations"],
        "measurement": config["phases"]["measurement_operations"],
    }
    if config["qualification_only"]:
        expected = dict.fromkeys(expected, 0)
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
    backends, profiles, rates = spec.get("backends", []), spec.get("profiles", []), spec.get("rates", [])
    if not backends or len(backends) != len(set(backends)) or set(backends) - {"native", "tidehunter_tcp", "rocksdb", "blobdb", "tikv", "cassandra", "foundationdb"}:
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
    framework = ["experiments/kv-comparison", "benchmarks/kv-research",
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


def backend_options(kind, state, output):
    nodes = state["nodes"] if state else 1
    if kind == "tikv":
        return {"pd_endpoints": ["127.0.0.1:2379"], "pd_api_endpoints": ["http://127.0.0.1:2379"],
                "required_replication_factor": nodes}
    if kind == "cassandra":
        return {"nodes": [f"127.0.0.{11+i}:9042" for i in range(nodes)], "keyspace": "kv_comparison",
                "required_replication_factor": nodes, "consistency": "quorum"}
    if kind == "foundationdb":
        return {"cluster_file": str(Path(state["directory"]) / "fdb.cluster"), "required_replication_factor": 1 if nodes == 1 else 2}
    options = {"path": str(output / "database")}
    if kind == "tidehunter_tcp":
        options.update({"evidence": str(output / "tcp-evidence"), "owners": 5})
    return options


def build_config(base, job, state, output, seed, qualification):
    config = copy.deepcopy(base)
    config["backend"] = {"kind": job["backend"], "options": backend_options(job["backend"], state, output)}
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
    if job["backend"] == "tidehunter_tcp":
        retained = (config["dataset"]["keys"] if config["phases"]["preload"] else 0)
        retained += sum(config["phases"][name] for name in ("warmup_operations", "aging_operations", "measurement_operations")) + 1024
        if retained > 1_000_000:
            raise ValueError("finite TCP retained-history requirement exceeds supported 1,000,000 batches per partition; reduce the declared workload explicitly")
        config["backend"]["options"]["max_batches_per_partition"] = retained
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


def run_one(args, spec, base, job, index):
    run_dir = args.output / f"{index:04d}-{job['backend']}-{job['profile']['name']}-r{job['rate']}-n{job['repeat']}"
    run_dir.mkdir()
    state_dir = args.reuse_state.resolve() if args.reuse_state else run_dir / "server"
    state = None
    external = job["backend"] in ("tikv", "cassandra", "foundationdb")
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
        prospective = {"nodes": 1 if spec["layout"] == "qualification" else 3, "directory": str(state_dir)} if external else None
        if args.reuse_state:
            prospective = local.load(state_dir)
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
        if job["backend"] == "tidehunter_tcp":
            receipt["retention_admission"] = {"max_batches_per_partition": config["backend"]["options"]["max_batches_per_partition"],
                                               "derivation": "preload keys + all phase calls + 1024 qualification allowance; worst-case one-owner concentration",
                                               "physical_wal_gc": False}
        if fdb_identity:
            receipt["admitted_native_library"] = fdb_identity
            receipt["native_loader_policy"] = "exact admitted LD_LIBRARY_PATH; LD_PRELOAD and LD_AUDIT removed"
        if external:
            if args.reuse_state:
                state = local.load(state_dir)
                local.ready(state, 180)
            else:
                local.start(argparse.Namespace(state=state_dir, backend=job["backend"], layout=spec["layout"], timeout=180))
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
                                   log, run_dir / "client-resources.jsonl", cpus, spec.get("client_memory_mib", 2048),
                                   config["limits"]["deadline_seconds"], config["limits"]["shutdown_grace_seconds"], child_environment)
        receipt["observation_after"] = observer_snapshot(args.observe)
        if receipt["observation_after"] != receipt["observation"]:
            raise RuntimeError("observer presence/health/resource identity changed during run; no matched observation contract")
        if returncode:
            raise RuntimeError(f"actual adapter run exited {returncode}; see {run_dir / 'run.log'}")
        if not all((result_dir / name).is_file() for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")):
            raise RuntimeError("actual runner omitted required raw/result artifacts")
        receipt["artifacts_sha256"] = {name: digest(result_dir / name)
                                        for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}
        verify_run_identity(result_dir, receipt, config, fdb_identity)
        receipt["validated_timing_rows_by_phase"] = verify_timing_result(result_dir, config)
        if external:
            local.ready(state, 60)
        receipt["status"] = "completed"
    except BaseException as exc:
        receipt["status"] = "failed"
        receipt["error"] = str(exc)
        raise
    finally:
        finalize_run(run_dir, state_dir, external, bool(args.reuse_state), receipt)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--binary", type=Path, default=local.ROOT / "target/release/kv-research")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--backend", help="rerun only this backend, preserving declared matrix and seed")
    p.add_argument("--reuse-state", type=Path, help="reuse exact-owned ready deployment; implies explicitly labeled nonfresh storage and leaves it running")
    p.add_argument("--qualification", action="store_true", help="real API qualification, not performance")
    p.add_argument("--observe", action="store_true", help="explicitly configure/start monitoring per cell before timing")
    args = p.parse_args()
    args.output = args.output.resolve()
    args.binary = args.binary.resolve()
    if args.output.exists():
        raise ValueError("output must be fresh; refusing to overwrite provenance")
    spec = json.loads(args.manifest.read_text())
    jobs = matrix(spec)
    if args.backend:
        if args.backend not in spec["backends"]:
            raise ValueError("requested backend is absent from admitted manifest")
        jobs = [x for x in jobs if x["backend"] == args.backend]
    if args.reuse_state:
        if not args.backend:
            raise ValueError("--reuse-state requires exactly one explicit --backend")
        reused = local.load(args.reuse_state)
        if reused["backend"] != args.backend or reused["layout"] != spec["layout"]:
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
