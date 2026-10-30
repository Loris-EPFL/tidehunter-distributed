#!/usr/bin/env python3
"""Explicit multi-host comparison inputs and raw-sample reports; never allocates AWS."""
import argparse
import csv
import copy
import hashlib
import json
from pathlib import Path
import random
import statistics
import sys

import analyze
import local
import resource_accounting

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import runtime_backend, transport_for_case, validate_transport, protocol_key, select_backends

BACKENDS = ("native", "tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct",
            "tidehunter_efa_native", "tikv", "cassandra", "foundationdb", "valkey_tcp")
LEGACY_BACKENDS = BACKENDS[:-1]
TRANSPORTS = {name: transport_for_case(name) for name in BACKENDS if name.startswith("tidehunter_")}
MIX_FIELDS = ("read", "put", "delete", "missing_read", "get_many", "put_batch")


def mix(**values):
    return {name: values.get(name, 0) for name in MIX_FIELDS}


def make_matrix(spec):
    """Block by backend to amortize server deployment; shuffle within each repeat."""
    required = {"schema_version", "seed", "repeats", "value_sizes", "backends",
                "keys", "max_cases", "offered_rates", "offered_seconds",
                "case_deadline_seconds", "hard_deadline_seconds", "export_reserve_seconds"}
    if not required <= set(spec) or set(spec) - required - {"diagnostics", "mode"}:
        raise ValueError("cloud matrix requires its explicit schema")
    if spec["schema_version"] != 1 or isinstance(spec["seed"], bool) or not isinstance(spec["seed"], int):
        raise ValueError("invalid cloud schema/seed")
    for name, lower, upper in (("repeats", 1, 10), ("keys", 1024, 65536), ("max_cases", 1, 2000),
                              ("offered_seconds", 2, 60), ("case_deadline_seconds", 60, 600),
                              ("hard_deadline_seconds", 3600, 10800), ("export_reserve_seconds", 300, 1800)):
        value = spec[name]
        if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
            raise ValueError(f"invalid {name}")
    if spec["export_reserve_seconds"] >= spec["hard_deadline_seconds"] // 3:
        raise ValueError("export reserve consumes the campaign")
    sizes, backends, rates = spec["value_sizes"], spec["backends"], spec["offered_rates"]
    if set(sizes) != {64, 256, 1024, 8192, 65536} or len(sizes) != 5:
        raise ValueError("full campaign requires all five value sizes without duplicates")
    backends = select_backends(None, backends, set(BACKENDS))
    rates = spec["offered_rates"]
    if set(rates) != {1000, 10000, 50000} or len(rates) != 3 or any(type(rate) is not int for rate in rates):
        raise ValueError("full campaign requires all three offered rates without duplicates")
    diagnostics = spec.get("diagnostics", {})
    if (not isinstance(diagnostics, dict)
            or set(diagnostics) - {"write_profile_limit", "read_profile_limit"}
            or ("diagnostics" in spec and not diagnostics)
            or any(type(value) is not int or not 0 <= value <= 65536
                   for value in diagnostics.values())):
        raise ValueError("diagnostics limits must be integers between 0 and 65536")
    diagnostics = {"write_profile_limit": diagnostics.get("write_profile_limit", 0),
                   "read_profile_limit": diagnostics.get("read_profile_limit", 0)}
    if spec.get("mode", "standard") == "read-tier-focused":
        if spec["repeats"] != 1:
            raise ValueError("focused campaign requires one repetition")
        expanded = {**spec, "mode": "read-tier", "max_cases": 2000}
        tier_jobs = make_read_tier_matrix(expanded, backends, diagnostics)
        standard_jobs = make_matrix({**expanded, "mode": "standard"})
        early = [job for job in tier_jobs if
                 (job["read_tier"]["name"] in ("native_hot_wal_lru0_pagecache", "native_cold_os_lru0")
                  and job["value_size"] in (256, 1024) and job["concurrency"] == 1)
                 or (job["read_tier"]["name"] == "cold_device" and job["value_size"] == 256
                     and job["concurrency"] == 1)]
        wanted = {"remote", "coordinator_hit", "owner_hit", "registered_steady", "registered_reuse",
                  "wal_warm", "registered_steady_c16_mixed_acquisition_reuse",
                  "registered_reuse_c16_mixed_acquisition_reuse"}
        later = [job for job in tier_jobs if job["read_tier"]["name"] in wanted
                 and ((job["concurrency"] == 1 and job["value_size"] in (256, 8192))
                      or (job["concurrency"] == 16 and job["value_size"] in (1024, 8192)))]
        # Keep legacy controls unchanged, and separately measure full-set
        # headroom: destination handle limits are divided across five owners.
        headroom = []
        for original in later:
            if original["read_tier"]["name"] not in ("registered_steady", "registered_reuse") or original["concurrency"] != 1:
                continue
            job = copy.deepcopy(original)
            job["read_tier"]["name"] += "_handle_headroom"
            job["cohort"] = "read_tier_" + job["read_tier"]["name"]
            job["resident_reads"]["max_handles"] = 5 * job["keys"]
            job["read_tier"]["requested"]["registered_budgets"]["max_handles"] = 5 * job["keys"]
            job["read_tier"]["source_id"] += "-handle-headroom"
            job["case_id"] += "-handle-headroom"
            headroom.append(job)
        comparisons = [job for job in standard_jobs if job["cohort"] == "primary_rf1" and
                       (job["value_size"], job["profile"]) in ((1024, "write-concurrent"),
                           (1024, "read-concurrent"), (8192, "read-many32"))]
        comparisons += [job for job in standard_jobs if job["cohort"] == "write_stage_diagnostics"
                        and job["value_size"] == 1024 and job["profile"] == "write-concurrent"]
        jobs = early + comparisons + headroom + later
        if len(jobs) > spec["max_cases"]:
            raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
        return jobs
    if spec.get("mode", "standard") == "read-tier":
        return make_read_tier_matrix(spec, backends, diagnostics)
    if spec.get("mode", "standard") != "standard":
        raise ValueError("unknown cloud campaign mode")
    profiles = [
        ("point-mixed", mix(read=45, put=45, delete=5, missing_read=5), 1, 8192),
        ("read-concurrent", mix(read=100), 16, 8192),
        ("write-concurrent", mix(put=100), 16, 4096),
        ("read-many32", mix(get_many=100), 16, 2048),
    ]
    jobs, read_jobs, fast_read_jobs = [], [], []
    for repeat in range(spec["repeats"]):
        order = list(backends)
        random.Random(spec["seed"] + repeat).shuffle(order)
        for backend in order:
            block = []
            def add(label, size, operation_mix, concurrency, count, *, cohort="primary_rf1",
                    rate=None, distribution="uniform", keys=None, cache=0, owner_cache=0, warmup=512):
                block.append({"backend": backend, "repeat": repeat, "profile": label,
                    "cohort": cohort, "value_size": size, "keys": keys or spec["keys"],
                    "mix": operation_mix, "concurrency": concurrency, "measurement_operations": count,
                    "warmup_operations": warmup, "offered_ops_per_second": rate,
                    "distribution": distribution, "coordinator_cache_bytes": cache,
                    "owner_read_cache_bytes": owner_cache, "drop_os_caches": cohort == "cold_os_cache",
                    "native_durability": "page_cache" if cohort == "native_pagecache" else "value_wal_barrier"})
            for size in sizes:
                for label, operation_mix, concurrency, count in profiles:
                    add(label, size, operation_mix, concurrency, count)
            for rate in rates:
                add(f"offered-{rate}", 1024, mix(read=70, put=30), 32,
                    rate * spec["offered_seconds"], rate=rate)
            add("zipf-read", 1024, mix(read=100), 16, 8192, distribution={"zipf": {"exponent": 0.99}})
            # A large finite dataset plus a quiescent OS-cache drop is not called
            # an all-NVMe population: engine block caches still exist and I/O is observed.
            add("cold-os-large", 65536, mix(read=100), 16, 4096,
                cohort="cold_os_cache", keys=131072, warmup=0)
            if backend in TRANSPORTS:
                for size in (256, 8192):
                    add("coordinator-cache", size, mix(read=100), 1, 8192,
                        cohort="coordinator_cache_configured", cache=128 * 1024**2, warmup=1024)
                    add("owner-cache", size, mix(read=100), 1, 8192,
                        cohort="owner_cache_configured", owner_cache=128 * 1024**2, warmup=1024)
            if backend == "tidehunter_efa_native":
                for size in (256, 8192):
                    add("registered-ram", size, mix(read=100), 1, 8192,
                        cohort="registered_ram_configured", warmup=1024)
                    block[-1]["resident_reads"] = {
                        "source_bytes": 16 * 1024**2, "source_regions": 4096,
                        "destination_bytes": 20 * 1024**2, "destination_slots": 640,
                        "max_read_bytes": 32768, "max_handles": 4096,
                        "require_device_read": True}
            if backend == "native":
                for size in sizes:
                    for label, operation_mix, concurrency, count in (profiles[0], profiles[3]):
                        add(label, size, operation_mix, concurrency, count, cohort="native_pagecache")
            primary = [job for job in block if job["cohort"] == "primary_rf1"]
            extra = [job for job in block if job["cohort"] not in ("primary_rf1", "cold_os_cache")]
            cold = [job for job in block if job["cohort"] == "cold_os_cache"]
            # Match the common workload/overwrite history exactly across stores.
            random.Random(spec["seed"] ^ (repeat << 8)).shuffle(primary)
            random.Random(spec["seed"] ^ (repeat << 8)).shuffle(extra)
            diagnostic_jobs = []
            if diagnostics["write_profile_limit"] and backend in ("native", *TRANSPORTS):
                diagnostic_jobs = [job | {"cohort": "write_stage_diagnostics",
                    "diagnostics": {"write_profile_limit": diagnostics["write_profile_limit"]}}
                    for job in primary if job["value_size"] == 1024 and job["profile"] == "write-concurrent"]
            if diagnostics["read_profile_limit"] and backend in TRANSPORTS:
                sources = [job for job in primary
                           if (job["value_size"], job["profile"]) in
                           ((1024, "read-concurrent"), (8192, "read-many32"))]
                sources += [job for job in extra if job["value_size"] == 8192
                            and job["cohort"] in ("coordinator_cache_configured",
                                                  "owner_cache_configured",
                                                  "registered_ram_configured")]
                read_jobs += [job | {"cohort": "read_stage_diagnostics",
                    "diagnostic_source_cohort": job["cohort"],
                    "diagnostics": {"read_profile_limit": diagnostics["read_profile_limit"]}}
                    for job in sources]
                resident_sources = [job for job in primary
                    if (job["value_size"], job["profile"]) in (
                        (256, "read-concurrent"), (1024, "read-concurrent"),
                        (8192, "read-concurrent"), (65536, "read-concurrent"),
                        (8192, "read-many32"))]
                fast_read_jobs += [job | {
                    "cohort": "resident_first_stage_diagnostics",
                    "diagnostic_source_cohort": job["cohort"],
                    "owner_read_resident_first": True,
                    "diagnostics": {"read_profile_limit": diagnostics["read_profile_limit"]}}
                    for job in resident_sources]
                if backend == "tidehunter_efa_native":
                    for job in sources:
                        if job["cohort"] != "primary_rf1":
                            continue
                        keys_per_call = 32 if job["mix"]["get_many"] else 1
                        fast_read_jobs.append(job | {
                            "cohort": "registered_ram_fullset_control",
                            "diagnostic_source_cohort": job["cohort"],
                            "profile": "registered-many32" if keys_per_call == 32 else "registered-scalar",
                            "concurrency": 1,
                            "read_lanes": 1,
                            "owner_read_resident_first": True,
                            "warmup_key_sweep": True,
                            "warmup_operations": max(job["warmup_operations"],
                                                     -(-job["keys"] // keys_per_call)),
                            "resident_reads": {
                                "source_bytes": 16 * 1024**2, "source_regions": 4096,
                                "destination_bytes": 20 * 1024**2, "destination_slots": 640,
                                "max_read_bytes": 32768, "max_handles": 8192,
                                "require_device_read": True},
                            "diagnostics": {"read_profile_limit": diagnostics["read_profile_limit"]}})
            jobs.extend(primary + extra + diagnostic_jobs + cold)
    # Keep every pre-existing case ID and its derived namespace stable.
    jobs.extend(read_jobs)
    jobs.extend(fast_read_jobs)
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        job["case_id"] = f"{number:04d}-{job['backend']}-{job['value_size']}-{job['profile']}-{job['cohort']}-r{job['repeat']}"
    return jobs


def make_read_tier_matrix(spec, backends, diagnostics):
    """A single ordered fleet experiment; each instrumented twin has its own population."""
    selected = [backend for backend in backends if backend in TRANSPORTS]
    if not selected:
        raise ValueError("read-tier mode requires at least one distributed Tidehunter backend")
    limit = diagnostics["read_profile_limit"] or 4096
    working_keys = max(spec["keys"], 8192)
    resident = {"source_bytes": 64 * 1024**2, "source_regions": 8192,
                "destination_bytes": 20 * 1024**2, "destination_slots": 640,
                "max_read_bytes": 32768, "max_handles": 8192, "require_device_read": True}
    tiers = ("remote", "coordinator_hit", "owner_hit", "registered_steady",
             "first_touch_no_promotion", "registered_acquire", "registered_reuse",
             "wal_warm", "native_hot_wal_lru0_pagecache", "cold_device",
             "native_cold_os_lru0", "bounded_mixed")
    jobs = []
    for tier in tiers:
        for repeat in range(spec["repeats"]):
            for backend in (["native"] if tier.startswith("native_") and "native" in backends
                            else [] if tier.startswith("native_") else selected):
                if (tier.startswith("registered_") or tier == "first_touch_no_promotion") and backend != "tidehunter_efa_native":
                    continue
                for size in ((1024, 8192) if tier in ("first_touch_no_promotion", "registered_acquire")
                             else (256, 1024, 8192, 65536) if backend == "native"
                             else (256, 8192, 65536)):
                    if (tier.startswith("registered_") or tier == "first_touch_no_promotion") and size > resident["max_read_bytes"]:
                        continue
                    for profiled in ((False,) if backend == "native" else (False, True)):
                        # Size pure-hit controls for the finite dataset plus cache metadata,
                        # without claiming four-way placement can retain every key.
                        footprint = working_keys * (size + 512)
                        cache = max(128 * 1024**2, 2 * footprint) if tier == "coordinator_hit" else (1024**2 if tier == "bounded_mixed" else 0)
                        owner = max(128 * 1024**2, (footprint + 1) // 2) if tier == "owner_hit" else (1024**2 if tier == "bounded_mixed" else 0)
                        coordinator_allowance = max(512 * 1024**2, cache + 64 * 1024**2)
                        owner_allowance = max(512 * 1024**2, owner + 64 * 1024**2)
                        registered = tier.startswith("registered_")
                        sweep = tier in ("coordinator_hit", "owner_hit", "registered_steady", "registered_reuse",
                                         "wal_warm", "native_hot_wal_lru0_pagecache")
                        warmup = working_keys if sweep else (0 if tier in ("registered_acquire", "first_touch_no_promotion", "cold_device", "native_cold_os_lru0") else 512)
                        source = f"{backend}-{size}-keys{working_keys}-scalar-c1-l{'0' if backend == 'native' else '1'}-seed{1775 + repeat}-r{repeat}"
                        tier_name = "wal_warm_oversize_async_fallback" if tier == "wal_warm" and size > 16384 else tier
                        job = {"backend": backend, "repeat": repeat, "profile": "read-tier-profiled" if profiled else "read-tier-baseline",
                            "cohort": "registered_ram_fullset_control" if tier == "registered_steady" else "read_tier_" + tier_name,
                            "value_size": size, "keys": working_keys, "mix": mix(read=100),
                            "concurrency": 1, "read_lanes": 1,
                            "measurement_operations": 8192,
                            "warmup_operations": warmup, "offered_ops_per_second": None,
                            "distribution": "uniform", "coordinator_cache_bytes": cache,
                            "owner_read_cache_bytes": owner, "drop_os_caches": tier in ("cold_device", "native_cold_os_lru0"),
                            "native_durability": "value_wal_barrier", "warmup_key_sweep": sweep,
                            "owner_read_resident_first": registered or tier in ("wal_warm", "first_touch_no_promotion"),
                            "read_tier": {"name": tier_name, "source_id": source,
                                "data_id": f"read-tier-v1-seed{1775 + repeat}-r{repeat}-size{size}-keys{working_keys}",
                                "profiled": profiled,
                                "requested": {"coordinator_cache_bytes": cache, "owner_read_cache_bytes": owner,
                                    "registered_policy": "hardware_required" if registered else "disabled",
                                    "registered_budgets": resident.copy() if registered else {
                                        "source_bytes": 0, "source_regions": 0, "destination_bytes": 0,
                                        "destination_slots": 0, "max_read_bytes": 0, "max_handles": 0,
                                        "require_device_read": False},
                                    "owner_read_resident_first": registered or tier in ("wal_warm", "first_touch_no_promotion"),
                                    "page_cache_policy": "drop_at_measurement_gate" if tier in ("cold_device", "native_cold_os_lru0") else "not_dropped",
                                    "warmup_operations": warmup, "warmup_key_sweep": sweep,
                                    "device_trace": "enabled" if backend == "native" or tier == "cold_device" else "disabled",
                                    "measurement_key_sweep": tier in ("first_touch_no_promotion", "registered_acquire"),
                                    "read_lanes": 0 if backend == "native" else 1, "concurrency": 1,
                                    "sessions": "not_applicable" if backend == "native" else "runtime_default"},
                                "effective": None}}
                        if backend != "native":
                            job["coordinator_read_memory_allowance_bytes"] = coordinator_allowance
                            job["owner_read_memory_allowance_bytes"] = owner_allowance
                            job["read_tier"]["requested"]["coordinator_read_memory_allowance_bytes"] = coordinator_allowance
                            job["read_tier"]["requested"]["owner_read_memory_allowance_bytes"] = owner_allowance
                        if backend == "native":
                            job["read_tier"]["requested"]["native_value_lru_bytes"] = 0
                            job["read_tier"]["requested"]["native_value_lru_source"] = "KeyShape::new_single default"
                        if registered:
                            job["resident_reads"] = resident.copy()
                        if tier in ("first_touch_no_promotion", "registered_acquire"):
                            job["measurement_key_sweep"] = True
                        if profiled:
                            job["diagnostics"] = {"read_profile_limit": limit}
                        jobs.append(job)
    supplemental = []
    wanted = {"remote", "coordinator_hit", "owner_hit", "wal_warm", "bounded_mixed",
              "registered_steady", "registered_reuse", "native_hot_wal_lru0_pagecache"}
    for original in jobs:
        tier = original["read_tier"]["name"]
        if tier not in wanted or original["value_size"] != 8192:
            continue
        for size, shape, calls in ((1024, "scalar", 8192), (8192, "batch32", 2048)):
            job = copy.deepcopy(original)
            profiled = job["read_tier"]["profiled"]
            if tier in ("registered_steady", "registered_reuse"):
                job["read_tier"]["name"] = tier + "_c16_mixed_acquisition_reuse"
                job["cohort"] = "read_tier_" + job["read_tier"]["name"]
            job["value_size"] = size
            job["profile"] = f"read-tier-c16-{shape}-{'profiled' if profiled else 'baseline'}"
            job["mix"] = mix(get_many=100) if shape == "batch32" else mix(read=100)
            job["concurrency"] = 16
            job["read_lanes"] = 0 if job["backend"] == "native" else 4
            job["measurement_operations"] = calls
            job["warmup_key_sweep"] = True
            job["warmup_operations"] = max(1024, -(-working_keys // (32 if shape == "batch32" else 1)))
            job["read_tier"]["source_id"] = (f"{job['backend']}-{size}-keys{working_keys}-{shape}-c16-"
                f"l{job['read_lanes']}-seed{1775 + job['repeat']}-r{job['repeat']}")
            job["read_tier"]["data_id"] = f"read-tier-v1-seed{1775 + job['repeat']}-r{job['repeat']}-size{size}-keys{working_keys}"
            requested = job["read_tier"]["requested"]
            requested.update(concurrency=16, read_lanes=job["read_lanes"],
                             warmup_operations=job["warmup_operations"], warmup_key_sweep=True)
            supplemental.append(job)
    jobs.extend(supplemental)
    tier_order = {name: index for index, name in enumerate(tiers)}
    tier_order["wal_warm_oversize_async_fallback"] = tier_order["wal_warm"]
    for name in ("registered_steady", "registered_reuse"):
        tier_order[name + "_c16_mixed_acquisition_reuse"] = tier_order[name]
    jobs.sort(key=lambda job: tier_order[job["read_tier"]["name"]])
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        variant = job["profile"] if "-c16-" in job["profile"] else ("profiled" if job["read_tier"]["profiled"] else "baseline")
        job["case_id"] = f"{number:04d}-{job['backend']}-{job['value_size']}-{job['read_tier']['name']}-{variant}-r{job['repeat']}"
    return jobs


def make_run_config(job, case_root, options, source_revision, deadline_seconds=180):
    case_root = Path(case_root)
    if not case_root.is_absolute():
        raise ValueError("absolute case root required")
    maximum = job["keys"] + job["warmup_operations"] + job["measurement_operations"] + 4096
    options = dict(options)
    if job["backend"] in TRANSPORTS:
        expected = TRANSPORTS[job["backend"]]
        selected = validate_transport(options.get("transport", expected))
        if protocol_key(selected) != protocol_key(expected):
            raise ValueError("runtime transport differs from research case provider/security")
        options["transport"] = selected
    config = {"schema_version": 1, "backend": {"kind": runtime_backend(job["backend"]), "options": options},
        "seed": 1775 + job["repeat"],
        "namespace": "cloud-" + hashlib.blake2b(job.get("read_tier", {}).get("data_id", job["case_id"]).encode(), digest_size=12).hexdigest(),
        "dataset": {"keys": job["keys"], "value_size": job["value_size"], "entropy": 1.0},
        "workload": {"distribution": job["distribution"], "mix": job["mix"], "batch_size": 32},
        "execution": {"mode": "open_loop" if job["offered_ops_per_second"] else "closed_loop",
            "concurrency": job["concurrency"], "queue_capacity": 128,
            "request_timeout_ms": 10000, "offered_ops_per_second": job["offered_ops_per_second"]},
        "phases": {"preload": True, "warmup_operations": job["warmup_operations"],
            "aging_operations": 0, "measurement_operations": job["measurement_operations"]},
        "qualification_only": False, "source_revision": source_revision,
        "measurement_gate": {"ready_file": str(case_root / "measurement.ready"),
            "release_file": str(case_root / "measurement.release"), "timeout_seconds": 120},
        "measurement_completion_gate": {"ready_file": str(case_root / "measurement.complete"),
            "release_file": str(case_root / "measurement.observed"), "timeout_seconds": 120},
        "limits": {"max_keys": max(job["keys"], 1000000), "max_bytes": 16 * 1024**3,
            "max_operations": maximum, "deadline_seconds": max(deadline_seconds, 600 if job["keys"] > 65536 else deadline_seconds),
            "shutdown_grace_seconds": 20}}
    if "diagnostics" in job:
        config["diagnostics"] = job["diagnostics"]
    if job.get("warmup_key_sweep"):
        config["phases"]["warmup_key_sweep"] = True
    if job.get("measurement_key_sweep"):
        config["phases"]["measurement_key_sweep"] = True
    return config


def admit_measurement_read_evidence(directory):
    result = Path(directory) / "result"
    path = result / "measurement-read-evidence.json"
    manifest_path = result / "manifest.json"
    marker = None
    if manifest_path.is_file():
        marker = json.loads(manifest_path.read_text()).get("run", {}).get("measurement_read_evidence_schema_version")
    if marker is not None and (type(marker) is not int or marker != 1):
        raise ValueError("unsupported measurement read evidence capability")
    if not path.is_file():
        if marker == 1:
            raise ValueError("advertised measurement read evidence artifact is missing")
        return None  # Older source revisions did not advertise this artifact.
    if path.stat().st_size > 256 * 1024:
        raise ValueError("measurement read evidence exceeds bounded artifact size")
    data = json.loads(path.read_text())
    if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
            or data["schema_version"] != 1 or data.get("scope") != "measurement_boundary"
            or type(data.get("measurement_attempted")) is not bool
            or type(data.get("measurement_drained")) is not bool
            or not isinstance(data.get("deltas"), dict)
            or not all(isinstance(data.get(k), (dict, type(None))) for k in ("start", "end"))):
        raise ValueError("invalid measurement read evidence schema")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.measurement_read_metrics(data, {})
    return {"path": "result/measurement-read-evidence.json",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": data}


def admit_write_diagnostics(directory, job, *, completed):
    path = Path(directory) / "result/write-diagnostics.json"
    requested = job.get("diagnostics", {}).get("write_profile_limit", 0)
    if not path.is_file():
        if completed and requested:
            raise ValueError("requested write-stage diagnostics artifact is missing")
        return None
    if path.stat().st_size > 64 * 1024:
        raise ValueError("write-stage diagnostics exceed the bounded artifact size")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("selection"), dict):
        raise ValueError("write-stage diagnostics require object and selection evidence")
    selection = data.get("selection", {})
    if (type(data.get("schema_version")) is not int or data["schema_version"] != 1
            or data.get("backend") != job["backend"] or data.get("phase") != "measurement"
            or selection.get("requested_limit") != requested
            or type(data.get("phase_attempted")) is not bool or type(data.get("runtime_enabled")) is not bool
            or type(selection.get("observed_samples")) is not int or selection["observed_samples"] < 0):
        raise ValueError("write-stage diagnostics schema, backend, phase or requested limit differs")
    if completed and requested and (not data["phase_attempted"] or not data["runtime_enabled"]
            or selection["observed_samples"] == 0 or not isinstance(data.get("profile"), dict) or data.get("error")):
        raise ValueError("requested write-stage diagnostics did not observe real measurement samples")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.write_diagnostic_metrics(data, {})
    return {"path": "result/write-diagnostics.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "observed": data}

def admit_read_diagnostics(directory, job, *, completed):
    path = Path(directory) / "result/read-diagnostics.json"
    requested = job.get("diagnostics", {}).get("read_profile_limit", 0)
    if not path.is_file():
        if completed and requested:
            raise ValueError("requested read-stage diagnostics artifact is missing")
        return None
    if path.stat().st_size > 256 * 1024:
        raise ValueError("read-stage diagnostics exceed the bounded artifact size")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("selection"), dict):
        raise ValueError("read-stage diagnostics require object and selection evidence")
    selection = data["selection"]
    if (type(data.get("schema_version")) is not int or data["schema_version"] != 1
            or data.get("backend") != job["backend"] or data.get("phase") != "measurement"
            or type(selection.get("requested_limit")) is not int or selection["requested_limit"] != requested
            or type(data.get("phase_attempted")) is not bool or type(data.get("runtime_enabled")) is not bool):
        raise ValueError("read-stage diagnostics schema, backend, phase or requested limit differs")
    if completed and requested and (not data["phase_attempted"] or not data["runtime_enabled"]
                                    or not isinstance(data.get("profile"), dict) or data.get("error")):
        raise ValueError("requested read-stage diagnostics failed to run")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.read_diagnostic_metrics(data, {})
    return {"path": "result/read-diagnostics.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "observed": data}


def qualify_fullset_read_path(job, evidence):
    """Qualify observed paths, not a configured cache or a transport latency floor."""
    if job.get("cohort") != "registered_ram_fullset_control":
        return None
    deltas = evidence.get("deltas", {}) if isinstance(evidence, dict) else {}

    def total(component, field):
        group = deltas.get(component, {})
        rows = group.get("partitions")
        if (group.get("available") is not True or not isinstance(rows, dict)
                or set(rows) != {str(index) for index in range(5)}):
            return None
        values = []
        for row in rows.values():
            value = row.get("counters", {}).get(field)
            if row.get("available") is not True or type(value) is not int or value < 0:
                return None
            values.append(value)
        return sum(values)

    counters = ("message_read_requests", "grant_acquisitions", "grant_candidates",
                "rma_posts", "rma_completions", "rma_errors", "fallback_missing",
                "fallback_oversize", "fallback_capacity", "fallback_unsupported",
                "fallback_stale")
    observed = {name: total("client_resident", name) for name in counters}
    for name in ("wal_reads", "async_read_submissions", "cache_hits"):
        observed["owner_" + name] = total("owner_executor", name)
    coordinator = deltas.get("coordinator", {})
    hits = coordinator.get("counters", {}).get("hits")
    observed["coordinator_cache_hits"] = (hits if coordinator.get("available") is True
        and type(hits) is int and hits >= 0 else None)
    reasons = []
    if not job.get("warmup_key_sweep"):
        reasons.append("full working-set warm-up was not requested")
    if job.get("resident_reads", {}).get("require_device_read") is not True:
        reasons.append("hardware FI_READ was not required")
    if not isinstance(evidence, dict) or evidence.get("measurement_drained") is not True:
        reasons.append("measurement did not drain")
    for name, value in observed.items():
        if value is None:
            reasons.append(name + " is unavailable")
        elif name not in ("rma_posts", "rma_completions") and value != 0:
            reasons.append(name + " was nonzero")
    if observed["rma_posts"] is not None and observed["rma_posts"] == 0:
        reasons.append("no measured FI_READ was posted")
    if observed["rma_posts"] != observed["rma_completions"]:
        reasons.append("posted and completed FI_READ counts differ")
    return {"qualified": not reasons, "scope": "measurement_boundary",
        "hardware_read_required": job.get("resident_reads", {}).get("require_device_read"),
        "observed": observed, "reasons": reasons,
        "claim_boundary": "No observed message, grant-acquisition, owner-WAL or explicit-cache fallback; "
            "not a hardware-only RTT or physical NVMe measurement"}


def qualify_read_tier(job, evidence, receipt):
    """Use complete boundary counters; unknown never means zero."""
    tier = job.get("read_tier", {}).get("name")
    if tier is None:
        return None
    deltas = evidence.get("deltas", {}) if isinstance(evidence, dict) else {}
    def counter(component, field):
        group = deltas.get(component, {})
        if group.get("available") is not True:
            return None
        if component == "coordinator":
            values = [group.get("counters", {}).get(field)]
        else:
            rows = group.get("partitions")
            if not isinstance(rows, dict) or set(rows) != {str(i) for i in range(5)}:
                return None
            if any(row.get("available") is not True for row in rows.values()):
                return None
            values = [row.get("counters", {}).get(field) for row in rows.values()]
        return sum(values) if all(type(v) is int and v >= 0 for v in values) else None
    fields = {"coord_hits": ("coordinator", "hits"), "coord_misses": ("coordinator", "misses"),
        "rpc": ("coordinator", "read_rpc_count"), "owner_hits": ("owner_executor", "cache_hits"),
        "owner_misses": ("owner_executor", "cache_misses"), "wal_reads": ("owner_executor", "wal_reads"),
        "async": ("owner_executor", "async_read_submissions"), "resident_success": ("owner_executor", "resident_read_successes"),
        "resident_fallback": ("owner_executor", "resident_read_fallbacks"),
        "grants": ("client_resident", "grant_acquisitions"), "posts": ("client_resident", "rma_posts"),
        "completions": ("client_resident", "rma_completions"), "rma_errors": ("client_resident", "rma_errors")}
    observed_owner_grants = {name: counter("owner_grants", name) for name in ("issued", "reused")}
    observed = {name: counter(*field) for name, field in fields.items()}
    for name in ("fallback_missing", "fallback_oversize", "fallback_capacity", "fallback_unsupported", "fallback_stale"):
        observed[name] = counter("client_resident", name)
    observed["message_reads"] = counter("client_resident", "message_read_requests")
    # Authority counters are cumulative in raw boundary snapshots, not in
    # owner_executor deltas. Never interpret a missing/reset counter as zero.
    authority_loads = []
    for partition in map(str, range(5)):
        try:
            if any(evidence[side]["owner_executor"].get("available") is not True for side in ("start", "end")):
                raise ValueError("authority boundary unavailable")
            start = evidence["start"]["owner_executor"]["partitions"][partition]["authority"]["history_loads"]
            end = evidence["end"]["owner_executor"]["partitions"][partition]["authority"]["history_loads"]
            if type(start) is not int or type(end) is not int or start < 0 or end < start:
                raise ValueError("invalid authority boundary")
            authority_loads.append(end - start)
        except (TypeError, KeyError, ValueError):
            authority_loads = []
            break
    observed["authority_history_loads"] = sum(authority_loads) if len(authority_loads) == 5 else None
    observed["owner_grants"] = observed_owner_grants
    reasons = []
    if receipt.get("status") != "completed" or not isinstance(evidence, dict) or evidence.get("measurement_drained") is not True:
        reasons.append("measurement incomplete or undrained")
    if not isinstance(receipt.get("read_tier_effective"), dict):
        reasons.append("effective cache configuration unavailable")
    else:
        requested = job["read_tier"]["requested"]
        effective = receipt["read_tier_effective"]
        for name in ("coordinator_cache_bytes", "owner_read_cache_bytes", "registered_policy",
                     "coordinator_read_memory_allowance_bytes", "owner_read_memory_allowance_bytes",
                     "registered_budgets", "owner_read_resident_first", "page_cache_policy",
                     "warmup_operations", "warmup_key_sweep", "measurement_key_sweep", "read_lanes", "concurrency"):
            if effective.get(name) != requested.get(name):
                reasons.append(name + " effective configuration differs or is unavailable")
    def expect(name, predicate, description):
        value = observed[name]
        if value is None or not predicate(value):
            reasons.append(description + (" unavailable" if value is None else f" (observed {value})"))
    zero = lambda value: value == 0
    positive = lambda value: value > 0
    if tier == "coordinator_hit":
        for name in ("coord_misses", "rpc", "owner_hits", "wal_reads", "posts"):
            expect(name, zero, name + " must be zero")
        expect("coord_hits", positive, "coordinator hits required")
    elif tier == "owner_hit":
        for name in ("coord_hits", "owner_misses", "wal_reads", "posts"):
            expect(name, zero, name + " must be zero")
        for name in ("rpc", "owner_hits"):
            expect(name, positive, name + " required")
    elif tier == "wal_warm":
        for name in ("coord_hits", "owner_hits", "async", "resident_fallback", "posts"):
            expect(name, zero, name + " must be zero")
        expect("authority_history_loads", zero, "cold authority history loads must be zero")
        expect("wal_reads", positive, "owner WAL reads required")
        if job["value_size"] > 16384:
            reasons.append("value exceeds 16 KiB resident-first member limit")
        if observed["resident_success"] is None or observed["resident_success"] != observed["wal_reads"]:
            reasons.append("resident-first successes do not cover WAL reads")
    elif tier in ("registered_steady", "registered_reuse", "registered_steady_handle_headroom",
                   "registered_reuse_handle_headroom",
                   "registered_steady_c16_mixed_acquisition_reuse", "registered_reuse_c16_mixed_acquisition_reuse"):
        full = qualify_fullset_read_path(job | {"cohort": "registered_ram_fullset_control"}, evidence)
        reasons.extend(full["reasons"])
    elif tier == "registered_acquire":
        start = (evidence.get("start") or {}).get("client_resident_lanes", {}) if isinstance(evidence, dict) else {}
        partitions = start.get("partitions", {}) if isinstance(start, dict) else {}
        if start.get("available") is not True or set(partitions) != {str(i) for i in range(5)}:
            reasons.append("premeasurement read-lane sessions unavailable")
        else:
            for row in partitions.values():
                lanes = row.get("lanes", {})
                if set(lanes) != {"0"} or lanes["0"].get("session_present") is not True:
                    reasons.append("premeasurement read lane lacks a present session")
                    break
        expect("grants", lambda value: value == job["measurement_operations"], "one fresh grant per measured read required")
        expect("message_reads", lambda value: value == job["measurement_operations"], "one first-touch message per read required")
        if (not job.get("measurement_key_sweep") or job.get("warmup_operations") != 0
                or receipt.get("raw_artifacts_validated") is not True):
            reasons.append("validated first-touch unique-key sweep or zero warmup missing")
        for field, expected in (("issued", job["measurement_operations"]), ("reused", 0)):
            value = observed_owner_grants[field]
            if value != expected:
                reasons.append("owner grant " + field + " unavailable or differs from first-touch population")
        for name in ("coord_hits", "owner_hits", "rma_errors", "posts", "completions",
                     "fallback_oversize", "fallback_capacity", "fallback_unsupported", "fallback_stale"):
            expect(name, zero, name + " must be zero")
    elif tier == "cold_device":
        reasons.append("device events cover a collector window wider than measurement; activity cannot establish measurement-scoped device I/O")
        for name in ("coord_hits", "owner_hits", "posts"):
            expect(name, zero, name + " must be zero")
        expect("wal_reads", positive, "owner WAL reads required")
        hosts = receipt.get("cold_device_evidence")
        if not isinstance(hosts, list) or len(hosts) != 6:
            reasons.append("owned-device traces unavailable")
        else:
            owners = hosts[1:]
            if not all(isinstance(x, dict) and x.get("available") is True for x in owners):
                reasons.append("one or more owner traces unavailable")
            if not any(isinstance(x, dict) and x.get("available") is True
                       and type(x.get("read_issues")) is int and x["read_issues"] > 0
                       and type(x.get("read_completions")) is int and x["read_completions"] > 0
                       for x in owners):
                reasons.append("no owned device had both read issue and completion events")
            for row in owners:
                if not isinstance(row, dict) or any(row.get(k) not in (0, False) for k in
                    ("malformed_events", "lost", "truncated", "requeues", "completion_errors")):
                    reasons.append("trace loss, malformed events, requeues, errors or truncation")
                    break
    else:
        reasons.append("configured comparison, not a pure-path claim")
    return {"qualified": not reasons,
        "scope": "measurement_counters_and_wider_collector_window" if tier == "cold_device" else "measurement_boundary",
        "observed": observed, "reasons": reasons,
        "claim_boundary": "Device-wide collector-window events include gate overlap, background and metadata I/O; they do not establish measurement-scoped or per-API device time, NAND latency, or pure wire latency."}


def summarize_case(directory):
    directory = Path(directory)
    receipt = json.loads((directory / "receipt.json").read_text())
    job = receipt["job"]
    value = {"case_id": job["case_id"], "job": job, "status": receipt.get("status"),
             "error": receipt.get("error"), "measurement_evidence": receipt.get("measurement_evidence"),
             "counter_evidence": receipt.get("counter_evidence")}
    if receipt.get("status") != "completed" and not receipt.get("raw_artifacts_validated"):
        return {**value, "metrics_unavailable": "failed before trustworthy raw artifact admission"}
    read_interval = admit_measurement_read_evidence(directory)
    if read_interval is not None:
        if read_interval != receipt.get("measurement_read_evidence"):
            raise ValueError("measurement read evidence differs from its receipt integrity binding")
        value["measurement_read_evidence"] = read_interval
        qualification = qualify_fullset_read_path(job, read_interval["observed"])
        if qualification is not None:
            value["read_path_qualification"] = qualification
    elif receipt.get("measurement_read_evidence") is not None:
        raise ValueError("measurement read evidence artifact missing")
    if "read_tier" in job:
        value["read_tier_effective"] = receipt.get("read_tier_effective")
        boundaries = (read_interval["observed"] if read_interval else {})
        starts = (boundaries.get("start") or {}).get("client_resident_lanes")
        ends = (boundaries.get("end") or {}).get("client_resident_lanes")
        sessions_available = (isinstance(starts, dict) and starts.get("available") is True
                              and isinstance(ends, dict) and ends.get("available") is True)
        value["read_tier_sessions"] = {
            "available": sessions_available, "start": starts, "end": ends,
            "measurement_deltas": (boundaries.get("deltas") or {}).get("client_resident_lanes"),
            "reason": None if sessions_available else ("not applicable to native" if job["backend"] == "native"
                                                       else "lane/session boundary snapshots unavailable"),
            "scope": "local lane session presence; not peer health or stable session identifier"}
        value["cold_device_start"] = receipt.get("cold_device_start")
        value["cold_device_evidence"] = receipt.get("cold_device_evidence")
        value["cold_device_raw_location"] = receipt.get("cold_device_raw_location")
        value["read_tier_qualification"] = qualify_read_tier(
            job, read_interval["observed"] if read_interval else None, receipt)
    resource_binding = receipt.get("resource_evidence")
    if resource_binding is not None:
        if resource_binding.get("path") != "resource-interval.json":
            raise ValueError("resource evidence path differs from the owned interval artifact")
        artifact = directory / "resource-interval.json"
        resources = json.loads(artifact.read_text())
        if (hashlib.sha256(artifact.read_bytes()).hexdigest() != resource_binding.get("sha256")
                or resources != resource_binding.get("observed")):
            raise ValueError("resource interval evidence differs from its integrity binding")
        frames = []
        for field, filename in (("before", "measurement-before.json"), ("end", "measurement-end.json")):
            path = directory / filename
            boundary = receipt.get("measurement_evidence", {})
            if (boundary.get(field) != filename
                    or hashlib.sha256(path.read_bytes()).hexdigest() != boundary.get(field + "_sha256")):
                raise ValueError("resource boundary evidence differs from its integrity binding")
            frames.append([host["resource_accounting"] for host in json.loads(path.read_text())["hosts"]])
        if resource_accounting.interval(*frames) != resources:
            raise ValueError("resource interval cannot be reproduced from owned boundary counters")
        value["resource_evidence"] = resource_binding
    diagnostics = admit_write_diagnostics(directory, job, completed=receipt.get("status") == "completed")
    if diagnostics is not None:
        if diagnostics != receipt.get("write_diagnostics"):
            raise ValueError("write-stage diagnostics differ from their receipt integrity binding")
        value["write_diagnostics"] = diagnostics
    read_diagnostics = admit_read_diagnostics(directory, job, completed=receipt.get("status") == "completed")
    if read_diagnostics is not None:
        if read_diagnostics != receipt.get("read_diagnostics"):
            raise ValueError("read-stage diagnostics differ from their receipt integrity binding")
        value["read_diagnostics"] = read_diagnostics
    elif receipt.get("read_diagnostics") is not None:
        raise ValueError("read-stage diagnostics artifact missing")
    names = ("manifest.json", "summary.json", "samples.csv", "metrics.prom")
    if any(name not in receipt.get("artifacts_sha256", {}) for name in names):
        raise ValueError("admitted raw artifacts lack their complete integrity binding")
    result = directory / "result"
    config = json.loads((directory / "config.json").read_text())
    normalized = json.loads((directory / "normalized-config.json").read_text())
    analyze.verify_normalization(config, normalized)
    manifest = json.loads((result / "manifest.json").read_text())
    summary = json.loads((result / "summary.json").read_text())
    if manifest.get("status") not in ("completed", "failed") or summary.get("status") != manifest.get("status"):
        raise ValueError("manifest and summary disagree about the actual core status")
    if receipt.get("status") == "completed" and manifest.get("status") != "completed":
        raise ValueError("completed receipt contradicts failed core result")
    if manifest["run"]["config"] != normalized:
        raise ValueError("executed configuration differs from admission")
    if manifest["run"]["binary"]["blake2b512"] != receipt["binary_blake2b512"]:
        raise ValueError("actual executable differs from admitted release binary")
    if manifest.get("backend_info") != summary.get("backend"):
        raise ValueError("manifest and summary disagree about the executed backend")
    for name in names:
        if hashlib.sha256((result / name).read_bytes()).hexdigest() != receipt["artifacts_sha256"][name]:
            raise ValueError("raw result checksum differs from its original admission")
    samples = list(analyze.read_samples(result / "samples.csv"))
    if not samples:
        return {**value, "backend": summary["backend"],
                "metrics_unavailable": "validated execution has no observed measurement calls"}
    totals = analyze.aggregate(samples)
    elapsed = (max(row["completed_ns"] for row in samples) - min(row["scheduled_ns"] for row in samples)) / 1e9
    if elapsed <= 0:
        raise ValueError("measurement exposure must be positive")
    good = [row for row in samples if row["outcome"] in ("ok", "success")]
    totals.update({"elapsed_seconds": elapsed, "exposure": "observed raw measurement span",
        "planned_measurement_calls": config["phases"]["measurement_operations"],
        "measurement_rows_complete": len(samples) == config["phases"]["measurement_operations"],
        "success_calls_per_second": len(good) / elapsed,
        "success_logical_keys_per_second": sum(row["logical_keys"] for row in good) / elapsed,
        "success_value_mib_per_second": sum(row["bytes"] for row in good) / elapsed / 1024**2})
    for population in totals["populations"]:
        selected = [row for row in samples if row["operation"] == population["operation"]
                    and row["outcome"] == population["outcome"]]
        for kind in ("api", "response", "scheduling"):
            values = [row[kind + "_ns"] for row in selected if row[kind + "_ns"] is not None]
            population[kind + "_mean_ns"] = statistics.mean(values) if values else None
            population[kind + "_min_ns"] = min(values) if values else None
            population[kind + "_stddev_ns"] = statistics.pstdev(values) if values else None
        population["tail_warning"] = "p99 descriptive below 10000 calls; p999 descriptive below 100000 calls"
    return {**value, "backend": summary["backend"], "metrics": totals}



def write_csv_report(root, results):
    planned_repeats = sorted({row["job"]["repeat"] for row in results})
    repetition_mode = "single" if len(planned_repeats) == 1 else "multiple"
    fields = ["case_id", "status", "backend", "cohort", "profile", "repeat",
              "planned_independent_repetitions", "repetition_mode", "timing_stddev_scope", "value_bytes", "keys",
              "concurrency", "offered_ops_per_second", "observed_calls", "planned_calls",
              "measurement_rows_complete", "successful_calls", "unsuccessful_calls", "elapsed_seconds",
              "case_success_calls_per_second", "case_success_logical_keys_per_second", "case_success_value_mib_per_second",
              "operation", "outcome", "population_calls", "population_logical_keys",
              "acknowledgement", "consistency", "replication_factor", "measurement_boundary",
              "get_many_strategy", "batch_atomicity", "observed_topology", "effective_configuration",
              "resource_evidence", "read_path_qualification"]
    fields += [f"{kind}_{stat}_us" for kind in ("api", "response", "scheduling")
               for stat in ("mean", "min", "stddev")]
    fields += [f"{kind}_{stat}_us" for kind in ("api", "response", "scheduling")
               for stat in ("p50", "p95", "p99", "p999", "max")]
    fields += ["error", "metrics_unavailable"]
    with (root / "comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for result in results:
            job, metrics = result["job"], result.get("metrics", {})
            row = {key: job.get(key) for key in ("backend", "cohort", "profile", "repeat", "keys", "concurrency", "offered_ops_per_second")}
            row.update(case_id=result["case_id"], status=result["status"], value_bytes=job["value_size"],
                       error=result.get("error"), metrics_unavailable=result.get("metrics_unavailable"))
            row.update(planned_independent_repetitions=len(planned_repeats), repetition_mode=repetition_mode,
                       timing_stddev_scope="within_case_population")
            if "read_path_qualification" in result:
                row["read_path_qualification"] = json.dumps(result["read_path_qualification"], sort_keys=True)
            backend = result.get("backend", {})
            row.update({key: backend.get(key) for key in ("acknowledgement", "consistency",
                       "replication_factor", "measurement_boundary", "get_many_strategy", "batch_atomicity")})
            row.update(observed_topology=analyze.canonical(backend.get("topology")),
                       effective_configuration=analyze.canonical(backend.get("configuration")))
            row["resource_evidence"] = analyze.canonical(result.get("resource_evidence"))
            row.update({target: metrics.get(source) for target, source in (
                ("observed_calls", "offered_calls"), ("planned_calls", "planned_measurement_calls"),
                ("measurement_rows_complete", "measurement_rows_complete"), ("successful_calls", "success_calls"),
                ("unsuccessful_calls", "unsuccessful_calls"), ("elapsed_seconds", "elapsed_seconds"),
                ("case_success_calls_per_second", "success_calls_per_second"),
                ("case_success_logical_keys_per_second", "success_logical_keys_per_second"),
                ("case_success_value_mib_per_second", "success_value_mib_per_second"))})
            for population in metrics.get("populations", [None]):
                flat = dict(row)
                if population is not None:
                    flat.update(operation=population["operation"], outcome=population["outcome"],
                                population_calls=population["count"], population_logical_keys=population["logical_keys"])
                    for kind in ("api", "response", "scheduling"):
                        for stat in ("mean", "min", "stddev"):
                            number = population[kind + "_" + stat + "_ns"]
                            flat[f"{kind}_{stat}_us"] = number / 1000 if number is not None else None
                    for kind in ("api", "response", "scheduling"):
                        for stat in ("p50", "p95", "p99", "p999", "max"):
                            number = population[kind + "_ns"][stat]
                            flat[f"{kind}_{stat}_us"] = number / 1000 if number is not None else None
                writer.writerow(flat)


def read_diagnostic_report(rows):
    """Render independent sampled-prefix stage populations without pooling clocks."""
    lines = ["", "## Read-stage diagnostics", "",
             "Only observed measurement-phase samples are shown. Means are each stage's own sum/count; "
             "nested scopes overlap and must not be added or compared as wire or physical-I/O fractions. "
             "Stage quantiles are unavailable. Aggregate and individual partitions are separate views of "
             "the same globally selected requests, not independent per-owner budgets.", "",
             "| Case | Source cohort | Clock domain | Partition | Stage | Count | Sum ns | Mean ns | Max ns |",
             "|---|---|---|---|---|---:|---:|---:|---:|"]
    for row in rows:
        binding = row.get("read_diagnostics")
        if binding is None:
            continue
        evidence = binding["observed"]
        profile = evidence.get("profile") or {}
        eligible = evidence["phase_attempted"] and evidence["runtime_enabled"] and not evidence["error"]
        source = row["job"].get("diagnostic_source_cohort", row["job"]["cohort"])
        groups = [("aggregate", profile.get("stages") or {})]
        groups.extend((partition, owner.get("stages") or {})
                      for partition, owner in sorted((profile.get("owners") or {}).items()))
        for partition, stages in groups:
            if not stages:
                lines.append(f"| {row['case_id']} | {source} | — | {partition} | No stage data | — | — | — | — |")
            for stage, values in sorted(stages.items()):
                domain = "owner" if partition != "aggregate" or stage.startswith("server_") else "coordinator"
                if eligible and values["available"]:
                    count = values["count"]
                    maximum = values["max_ns"] if values["max_ns"] is not None else "—"
                    lines.append(f"| {row['case_id']} | {source} | {domain} | {partition} | {stage} | {count} | "
                                 f"{values['sum_ns']} | {values['sum_ns'] / count:.3f} | {maximum} |")
                else:
                    lines.append(f"| {row['case_id']} | {source} | {domain} | {partition} | {stage} | — | — | — | — |")
    return lines


def report(root):
    root = Path(root)
    planned = json.loads((root / "matrix.json").read_text())
    results = []
    for job in planned["jobs"]:
        directory = root / "cases" / job["case_id"]
        if (directory / "receipt.json").exists():
            results.append(summarize_case(directory))
        else:
            results.append({"case_id": job["case_id"], "job": job, "status": "not_run"})
    planned_repeats = sorted({job["repeat"] for job in planned["jobs"]})
    repetition_mode = "single" if len(planned_repeats) == 1 else "multiple"
    repetition_note = ("One planned independent repetition; no run-to-run variability estimate or confidence/significance claim."
                       if repetition_mode == "single" else
                       f"{len(planned_repeats)} planned independent repetitions; results below remain per-case sample statistics, not pooled run quantiles.")
    value = {"schema_version": 1, "complete": all(row["status"] == "completed" for row in results),
        "planned_cases": len(results), "completed_cases": sum(row["status"] == "completed" for row in results),
        "cases": results,
        "planned_repeats": planned_repeats, "planned_independent_repetitions": len(planned_repeats),
        "repetition_mode": repetition_mode,
        "planned_backends": sorted({job["backend"] for job in planned["jobs"]}),
        "current_backend_coverage": set(BACKENDS) <= {job["backend"] for job in planned["jobs"]},
        "interpretation": ["Common RF1 API experiment, not equivalent HA/transaction/security guarantees.",
            "API-call latency for get_many is the complete ordered group, not latency per key.",
            "OS-cache eviction does not evict engine caches; qualified device intervals, when available, are retained separately.",
            "Valkey GET is memory-resident even after OS-cache eviction; AOF always is write acknowledgment, not a value-WAL-backed read.",
            "No Sui checkpoint/application or sustainable infinite-aging claim."]}
    if planned.get("spec", {}).get("mode") in ("read-tier", "read-tier-focused"):
        capability = root / "trace-capability.json"
        value["trace_capability"] = (json.loads(capability.read_text()) if capability.is_file() else
            {"identity_backend_available": False, "gaps": ["fleet capability probe not completed"]})
    local.dump(root / "comparison.json", value)
    write_csv_report(root, results)
    lines = ["# Cloud distributed-KV comparison", "", f"Completed {value['completed_cases']}/{len(results)} planned cases.", "",
             "Raw per-operation API, response and scheduling latency are preserved; failed/rejected arrivals remain in denominators.", "",
             repetition_note, "Timing standard deviations describe within-case populations, not run-to-run variability.", "",
             "| Case/status | Backend | Value B | Cohort | Operation/outcome | N | Case calls/s | API mean µs | API p50 µs | API p95 µs | API p99 µs | Response p99 µs |", "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    if "trace_capability" in value:
        capability = value["trace_capability"]
        lines[2:2] = [f"Request-identity tracing attached and stopped on all hosts: {capability.get('identity_backend_available')}.",
                      "Capability is not evidence of block I/O or qualified device latency; consult trace-capability.json for host gaps.", ""]
    for row in results:
        if "metrics" not in row:
            lines.append(f"| {row['case_id']} | {row['job']['backend']} | {row['job']['value_size']} | {row['job']['cohort']} | {row['status']} | — | — | — | — | — | — | — |")
            continue
        for population in row["metrics"]["populations"]:
            def us(value):
                return f"{value / 1000:.3f}" if value is not None else "—"
            lines.append(f"| {row['case_id']} ({row['status']}) | {row['job']['backend']} | {row['job']['value_size']} | {row['job']['cohort']} | {population['operation']}/{population['outcome']} | {population['count']} | {row['metrics']['success_calls_per_second']:.2f} | {us(population['api_mean_ns'])} | {us(population['api_ns']['p50'])} | {us(population['api_ns']['p95'])} | {us(population['api_ns']['p99'])} | {us(population['response_ns']['p99'])} |")
    lines.extend(["", "Quantiles are exact nearest-rank raw-sample quantiles, not histogram bucket bounds. Tail precision depends on population size. Compare only matching size/workload/cohort/acknowledgement; consult comparison.json for full configuration, per-repeat logical-key throughput, bandwidth, errors and additional quantiles.", ""])
    lines.extend(read_diagnostic_report(results))
    controls = [row for row in results if "read_path_qualification" in row]
    if controls:
        lines.extend(["", "## Full-working-set registered-read controls", "",
            "A configured control is not called pure RMA unless every observed fallback/cache count is zero, "
            "FI_READ posts are nonzero, and completions match. Unavailable observations remain unknown. "
            "Hardware capability is separately enforced by the runtime require_device_read policy.", "",
            "| Case | Qualified | FI_READ posts | FI_READ completions | Reasons |",
            "|---|---|---:|---:|---|"])
        for row in controls:
            proof = row["read_path_qualification"]
            observed = proof["observed"]
            shown = lambda value: "—" if value is None else str(value)
            lines.append(f"| {row['case_id']} | {proof['qualified']} | {shown(observed['rma_posts'])} | "
                f"{shown(observed['rma_completions'])} | {'; '.join(proof['reasons']) or 'No observed fallback'} |")
    tier_rows = [row for row in results if "read_tier_qualification" in row]
    if tier_rows:
        lines.extend(["", "## Read-tier qualification", "",
            "Each baseline and sampled case remains a separate population. Native references request device tracing even when "
            "read profiling is off; do not infer observer-free overhead against untraced distributed controls. "
            "Device traces are device-wide, not attributed to individual calls or NAND service.", "",
            "| Case | Tier | Qualified | Reasons |", "|---|---|---|---|"])
        for row in tier_rows:
            proof = row["read_tier_qualification"]
            lines.append(f"| {row['case_id']} | {row['job']['read_tier']['name']} | {proof['qualified']} | "
                         f"{'; '.join(proof['reasons']) or 'Observed path conditions met'} |")
        lines.extend(["", "### Contended read-lane populations", "",
            "Measurement-boundary deltas are shown independently for each partition and lane; "
            "missing/reset lanes are unavailable, not zero.", "",
            "| Case | Partition | Lane | Message reads | Grant acquisitions | FI_READ posts | FI_READ completions |",
            "|---|---:|---:|---:|---:|---:|---:|"])
        for row in tier_rows:
            if "-c16-" not in row["job"]["profile"]:
                continue
            delta = (row.get("read_tier_sessions") or {}).get("measurement_deltas") or {}
            partitions = delta.get("partitions", {}) if delta.get("available") is True else {}
            if not partitions:
                lines.append(f"| {row['case_id']} | — | — | unavailable | unavailable | unavailable | unavailable |")
                continue
            for partition, group in sorted(partitions.items()):
                lanes = group.get("lanes", {}) if isinstance(group, dict) else {}
                if not lanes:
                    lines.append(f"| {row['case_id']} | {partition} | — | unavailable | unavailable | unavailable | unavailable |")
                for lane, item in sorted(lanes.items()):
                    counters = item.get("counters", {}) if item.get("available") is True else {}
                    values = [counters.get(name) for name in ("message_read_requests", "grant_acquisitions", "rma_posts", "rma_completions")]
                    shown = ["—" if value is None else str(value) for value in values]
                    lines.append(f"| {row['case_id']} | {partition} | {lane} | {' | '.join(shown)} |")
    (root / "comparison.md").write_text("\n".join(lines))
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--spec", type=Path, default=Path(__file__).with_name("cloud-campaign.json"))
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--backends", help="comma-separated nonempty subset of tracked backend IDs")
    plan.add_argument("--mode", choices=("standard", "read-tier", "read-tier-focused"), default="standard")
    plan.add_argument("--write-profile-limit", type=int,
                      help="diagnostic-only 1KiB write cohort for native/Tidehunter; leaves normal plans unprofiled")
    plan.add_argument("--read-profile-limit", type=int,
                      help="diagnostic-only representative read cohorts for distributed Tidehunter")
    summary = commands.add_parser("report")
    summary.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "plan":
        spec = json.loads(args.spec.read_text())
        spec["mode"] = args.mode
        if args.write_profile_limit is not None or args.read_profile_limit is not None:
            diagnostics = dict(spec.get("diagnostics", {}))
            if args.write_profile_limit is not None:
                diagnostics["write_profile_limit"] = args.write_profile_limit
            if args.read_profile_limit is not None:
                diagnostics["read_profile_limit"] = args.read_profile_limit
            spec["diagnostics"] = diagnostics
        if args.backends is not None:
            spec["backends"] = select_backends(args.backends, spec.get("backends"), set(BACKENDS))
        else:
            spec["backends"] = select_backends(None, spec.get("backends"), set(BACKENDS))
        value = {"schema_version": 1, "spec": spec, "jobs": make_matrix(spec)}
        local.dump(args.output, value)
        print(json.dumps({"planned_cases": len(value["jobs"]),
                          "hard_deadline_seconds": spec["hard_deadline_seconds"]}))
    else:
        value = report(args.root)
        print(json.dumps({key: value[key] for key in ("complete", "planned_cases", "completed_cases")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
