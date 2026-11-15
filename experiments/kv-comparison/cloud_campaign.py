#!/usr/bin/env python3
"""Explicit multi-host comparison inputs and raw-sample reports; never allocates AWS."""
import argparse
import csv
import copy
import hashlib
import ipaddress
import json
from pathlib import Path
import random
import statistics
import re
import uuid
import sys

import ack_contract
import analyze
from cloud_layout import OWNER_HOST_COUNTS
import local
import resource_accounting

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import runtime_backend, transport_for_case, validate_transport, protocol_key, select_backends
from nvmeof import spdk_measurement

LEGACY_BACKENDS = ("native", "tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct",
                   "tidehunter_efa_native", "tikv", "cassandra", "foundationdb")
BACKENDS = (*LEGACY_BACKENDS, "valkey_tcp", "garnet", "kvrocks")
PORTABLE_BACKENDS = {"tidehunter_verbs", "tidehunter_verbs_native", "tidehunter_tcp_trusted", "valkey_rdma"}
TRANSPORTS = {name: transport_for_case(name) for name in (*BACKENDS, *sorted(PORTABLE_BACKENDS))
              if name.startswith("tidehunter_")}
NVME_BACKENDS = {"tidehunter_nvme_tcp", "tidehunter_nvme_rdma"}
for name in NVME_BACKENDS:
    TRANSPORTS[name] = transport_for_case("tidehunter_tcp")
MIX_FIELDS = ("read", "put", "delete", "missing_read", "get_many", "put_batch")
# Owner acknowledgement tier of every Tidehunter case. `synchronous` waits for the covering WAL
# barrier and matches native `value_wal_barrier` and Valkey `appendfsync always`; `native` acks after
# the page-cache write and matches only native `page_cache`. Tiers are never mixed in a comparison.
DURABILITY_TIERS = ("native", "synchronous")
# Tier given to jobs of a newly generated matrix. A job record without a tier predates tiers and
# ran the engine default, which is native.
DEFAULT_DURABILITY_TIER = "synchronous"
ONE_OWNER_CASE_KINDS = frozenset({"remote_put"})
NVMEOF_PATH_SELECTIONS = ("nvmeof_path_compact", "nvmeof_path_core", "nvmeof_path_fix", "nvmeof_first_put", "nvmeof_contracts", "nvmeof_contracts_crossing", "nvmeof_contracts_crossing2", "nvmeof_parallel_writes", "nvmeof_single_caller", "nvmeof_f6", "nvmeof_f6b", "nvmeof_f8", "nvmeof_f7_highmem32", "nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2")



def durability_tier(job):
    """Planned tier of a Tidehunter case; other backends have no owner tier."""
    if job["backend"] not in TRANSPORTS:
        return None
    tier = job.get("durability_tier", "native")
    if tier not in DURABILITY_TIERS:
        raise ValueError("invalid durability_tier")
    return tier

def active_owner_count(job):
    count = job.get("owner_count", 5)
    if type(count) is not int or count not in OWNER_HOST_COUNTS:
        raise ValueError("owner_count must be 1 or 5")
    one_owner_read = (job.get("selection") == "remote_rw_compact"
                      and ((job.get("group") == "G15C" and job.get("case_kind") == "media_read")
                           or (job.get("group") == "G15M" and job.get("case_kind") == "sealed_knee")))
    fabric_read = (job.get("selection") == "fabric_ab_compact" and (
        (job.get("group") == "G16P" and job.get("case_kind") == "sealed_knee"
         and job.get("backend") in ("tidehunter_tcp", "tidehunter_tcp_trusted", "tidehunter_verbs_native")
         and "nvmeof_route" not in job and job.get("owner_polling") in ("adaptive", "busy"))
        or (job.get("group") == "G16C" and job.get("case_kind") == "media_read"
            and ((job.get("backend") in ("tidehunter_tcp_trusted", "tidehunter_verbs_native")
                  and "nvmeof_route" not in job and job.get("owner_polling") == "adaptive")
                 or (job.get("backend") in NVME_BACKENDS and job.get("nvmeof_route") == "sealed_read"
                     and "owner_polling" not in job
                     and ((job.get("nvmeof_target") == "kernel" and job.get("target_io") == "direct")
                          or (job.get("backend") == "tidehunter_nvme_rdma"
                              and job.get("nvmeof_target") == "spdk" and job.get("target_io") == "spdk_direct")))))))
    if count == 1 and job.get("selection") in ("leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS):
        expected = next((item for item in make_focused_matrix(job["selection"])
                         if item["case_id"] == job.get("case_id")), None)
        if expected is None or not _focused_exact(job, expected):
            raise ValueError("one owner requires an exact reviewed leased campaign case")
        return count
    if count == 1 and fabric_read:
        if job.get("durability_tier") != "synchronous":
            raise ValueError("fabric read requires synchronous owner durability")
        return count
    if count == 1 and (job.get("backend") not in ("tidehunter_verbs_native", "tidehunter_tcp")
                       or job.get("durability_tier") != "synchronous"
                       or "nvmeof_route" in job
                       or (not one_owner_read and (job.get("case_kind") not in ONE_OWNER_CASE_KINDS
                                                   or "sealed_dataset" in job))):
        raise ValueError("one owner is restricted to reviewed synchronous owner RPC cases")
    return count


def active_partitions(job):
    return {str(index) for index in range(active_owner_count(job))}


OWNER_RECEIPT_FIELDS = frozenset((
    "partition", "database", "address", "launch_nonce", "pid", "boot_id", "started_unix_ns",
    "durability"))


def owner_receipt_path(launch):
    """The receipt an owner process publishes next to its ready marker, before the marker."""
    ready = Path(launch["ready_file"])
    if ready.suffix:
        raise ValueError("owner ready marker must have no extension")
    return ready.with_name(ready.name + ".durability.json")


def validate_owner_receipt(receipt, launch):
    """The receipt must name this exact owner and launch and carry an explicit tier."""
    durability = receipt.get("durability") if isinstance(receipt, dict) else None
    launched = (launch.get("durability") or {}).get("tier", "native")
    partition = launch["node"]["partition"]["partition"]
    if (set(receipt) not in (OWNER_RECEIPT_FIELDS, OWNER_RECEIPT_FIELDS | {"history_persistence"})
            or receipt.get("history_persistence", "on_disk") != launch.get("history_persistence", "on_disk")
            or not isinstance(durability, dict)
            or durability.get("tier") not in DURABILITY_TIERS or durability["tier"] != launched
            or receipt["partition"] != partition
            or receipt["database"] != launch["node"]["partition"]["inventory"]["database"]
            or receipt["address"] != launch["listen"]
            or not launch.get("launch_nonce") or receipt["launch_nonce"] != launch["launch_nonce"]
            or not isinstance(receipt["pid"], int) or not receipt["boot_id"]
            or not isinstance(receipt["started_unix_ns"], int)):
        raise ValueError("owner receipt does not match its owner, launch or tier")
    return receipt


def read_owner_receipt(launch):
    path = owner_receipt_path(launch)
    if not path.is_file():
        raise FileNotFoundError("owner did not publish its receipt")
    return validate_owner_receipt(json.loads(path.read_text()), launch)


def owner_receipt_is_fresh(previous, receipt):
    """Same owner and launch, but a later start by a different process incarnation."""
    identity = ("partition", "database", "address", "launch_nonce")
    process = ("pid", "boot_id", "started_unix_ns")
    return (isinstance(receipt, dict)
            and set(receipt) in (OWNER_RECEIPT_FIELDS, OWNER_RECEIPT_FIELDS | {"history_persistence"})
            and receipt.get("history_persistence", "on_disk") == previous.get("history_persistence", "on_disk")
            and all(receipt[name] == previous[name] for name in identity)
            and receipt["durability"] == previous["durability"]
            and [receipt[name] for name in process] != [previous[name] for name in process]
            and receipt["started_unix_ns"] > previous["started_unix_ns"])


def admit_durability_tier(job, options, manifest_path):
    """The executed tier must be the planned one, with explicit evidence for planned tiers.

    A missing executed option is the pre-tier engine default, native; it never inherits the plan,
    so a synchronous case cannot be admitted without evidence that it executed. A job record with
    no tier predates tiers and needs no receipt confirmation.
    """
    if job["backend"] not in TRANSPORTS:
        return
    planned = durability_tier(job)
    if options.get("durability_tier", "native") != planned:
        raise ValueError("Tidehunter durability_tier differs from the planned case")
    if "durability_tier" in job:
        executed = json.loads(Path(manifest_path).read_text()).get("durability") or {}
        if executed.get("confirmed_by_backend_receipt") != planned:
            raise ValueError("owner receipts did not confirm the planned durability_tier")


def mix(**values):
    return {name: values.get(name, 0) for name in MIX_FIELDS}


def make_matrix(spec, *, _allow_live_nvme=False):
    """Every Tidehunter job carries its tier; jobs planned without one default to synchronous."""
    if spec.get("mode") == "focused":
        if spec.get("direct_control_arm") is not True:
            raise ValueError("focused campaign requires the reviewed direct control arm")
        return make_focused_matrix(spec.get("selection"))
    jobs = _make_matrix(spec, _allow_live_nvme=_allow_live_nvme)
    for job in jobs:
        if job["backend"] in TRANSPORTS:
            job["durability_tier"] = job.get("durability_tier", DEFAULT_DURABILITY_TIER)
            durability_tier(job)
    return jobs


def _make_matrix(spec, *, _allow_live_nvme=False):
    """Block by backend to amortize server deployment; shuffle within each repeat."""
    required = {"schema_version", "seed", "repeats", "value_sizes", "backends",
                "keys", "max_cases", "offered_rates", "offered_seconds",
                "case_deadline_seconds", "hard_deadline_seconds", "export_reserve_seconds"}
    if not required <= set(spec) or set(spec) - required - {"diagnostics", "mode", "spdk_comparison", "valkey_reproduction_profiles", "native_durability_cohort", "shapes", "arms"}:
        raise ValueError("cloud matrix requires its explicit schema")
    if spec["schema_version"] != 1 or isinstance(spec["seed"], bool) or not isinstance(spec["seed"], int):
        raise ValueError("invalid cloud schema/seed")
    for name, lower, upper in (("repeats", 1, 10), ("keys", 1024, 65536), ("max_cases", 1, 2000),
                              ("offered_seconds", 2, 60), ("case_deadline_seconds", 60, 600),
                              ("hard_deadline_seconds", 3600, 21600), ("export_reserve_seconds", 300, 1800)):
        value = spec[name]
        if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
            raise ValueError(f"invalid {name}")
    if spec["export_reserve_seconds"] >= spec["hard_deadline_seconds"] // 3:
        raise ValueError("export reserve consumes the campaign")
    sizes, backends, rates = spec["value_sizes"], spec["backends"], spec["offered_rates"]
    if set(sizes) != {64, 256, 1024, 8192, 65536} or len(sizes) != 5:
        raise ValueError("full campaign requires all five value sizes without duplicates")
    if type(spec.get("spdk_comparison", False)) is not bool:
        raise ValueError("spdk_comparison must be a boolean")
    if type(spec.get("native_durability_cohort", False)) is not bool:
        raise ValueError("native_durability_cohort must be a boolean")
    if spec.get("native_durability_cohort") and spec.get("mode") != "standard-with-live-nvmeof":
        raise ValueError("native durability cohort requires the matched live and sealed campaign")
    if spec.get("spdk_comparison") and (spec.get("mode") != "standard-with-live-nvmeof"
                                      or "tidehunter_nvme_rdma" not in backends):
        raise ValueError("SPDK comparison requires the matched live/ sealed RDMA campaign")
    profiles = spec.get("valkey_reproduction_profiles", [])
    if (not isinstance(profiles, list) or any(not isinstance(name, str) for name in profiles)
            or len(profiles) != len(set(profiles))
            or set(profiles) - {"published-as-written", "matched-ram"}
            or profiles and not {"valkey_tcp", "valkey_rdma"} <= set(backends)):
        raise ValueError("native Valkey profiles require both TCP and RDMA")
    supported = set(BACKENDS) | PORTABLE_BACKENDS | (NVME_BACKENDS if _allow_live_nvme or spec.get("mode") in ("nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof") else set())
    backends = select_backends(None, backends, supported)
    if "shapes" in spec and spec["shapes"] != "contract_core":
        raise ValueError("shapes must be contract_core")
    if "arms" in spec:
        allowed = {
            "tidehunter_verbs_native": ("synchronous", "native"),
            "tidehunter_nvme_rdma": ("synchronous", "native"),
            "native": ("value_wal_barrier", "page_cache"),
            "valkey_rdma": ("synced", "os_buffered", "periodic_sync"),
            "valkey_tcp": ("synced",), "kvrocks": ("synced", "page_cache")}
        arms = spec["arms"]
        if "shapes" not in spec:
            raise ValueError("arms requires shapes")
        if not isinstance(arms, dict) or set(arms) != set(backends):
            raise ValueError("arms must name every selected backend exactly once")
        for backend, names in arms.items():
            if (backend not in allowed or not isinstance(names, list) or not names
                    or any(not isinstance(name, str) or name not in allowed[backend] for name in names)
                    or len(names) != len(set(names))):
                raise ValueError("invalid arms for " + backend)
            if backend not in TRANSPORTS and backend != "native":
                for name in names:
                    ack_contract.selected(backend, name)
    rates = spec["offered_rates"]
    if set(rates) != {1000, 10000, 50000} or len(rates) != 3 or any(type(rate) is not int for rate in rates):
        raise ValueError("full campaign requires all three offered rates without duplicates")
    diagnostics = spec.get("diagnostics", {})
    if (not isinstance(diagnostics, dict)
            or set(diagnostics) - {"write_profile_limit", "read_profile_limit", "write_timeline_limit", "valkey_profile_limit"}
            or ("diagnostics" in spec and not diagnostics)
            or any(type(value) is not int or not 0 <= value <= 65536
                   for value in diagnostics.values())):
        raise ValueError("diagnostics limits must be integers between 0 and 65536")
    diagnostics = {"write_profile_limit": diagnostics.get("write_profile_limit", 0),
                   "read_profile_limit": diagnostics.get("read_profile_limit", 0),
                   "write_timeline_limit": diagnostics.get("write_timeline_limit", 0),
                   "valkey_profile_limit": diagnostics.get("valkey_profile_limit", 0)}
    if spec.get("shapes") == "contract_core":
        if spec.get("mode", "standard") not in ("standard", "standard-with-nvmeof", "standard-with-live-nvmeof"):
            raise ValueError("contract_core requires a standard campaign")
        base = {key: value for key, value in spec.items() if key not in ("shapes", "arms")}
        base.update(mode="standard", max_cases=2000, spdk_comparison=False, native_durability_cohort=False)
        sources = make_matrix(base, _allow_live_nvme=_allow_live_nvme or spec.get("mode") == "standard-with-live-nvmeof")
        return _contract_core_matrix(spec, sources)
    if spec.get("mode") in ("standard-with-nvmeof", "standard-with-live-nvmeof"):
        live = spec["mode"] == "standard-with-live-nvmeof"
        standard_backends = backends if live else [backend for backend in backends if backend not in NVME_BACKENDS]
        matched_backends = [backend for backend in backends if backend in
                            ({"native", "tidehunter_tcp", "tidehunter_verbs", "tidehunter_verbs_native",
                              "tidehunter_efa", "tidehunter_efa_direct", "tidehunter_efa_native"} | NVME_BACKENDS)]
        expanded = {**spec, "max_cases": 2000}
        jobs = make_matrix({**expanded, "mode": "standard", "backends": standard_backends,
                            "spdk_comparison": False, "native_durability_cohort": False}, _allow_live_nvme=live)
        valkey_jobs = [job for job in jobs if job["cohort"] == "valkey_client_diagnostics"]
        jobs = [job for job in jobs if job["cohort"] != "valkey_client_diagnostics"]
        matched = make_nvmeof_matrix(expanded, matched_backends, diagnostics)
        for number, job in enumerate(matched, start=len(jobs)):
            job["case_id"] = f"{number:04d}-{comparison_backend(job)}-{job['value_size']}-{job['profile']}"
        jobs.extend(matched)
        for number, job in enumerate(valkey_jobs, start=len(jobs)):
            job["case_id"] = (f"{number:04d}-{job['backend']}-{job['value_size']}-"
                              f"{job['profile']}-{job['cohort']}-r{job['repeat']}")
        jobs.extend(valkey_jobs)
        if spec.get("spdk_comparison"):
            paired = []
            for job in jobs:
                paired.append(job)
                if job["backend"] == "tidehunter_nvme_rdma" and job.get("nvmeof_route") == "live":
                    twin = copy.deepcopy(job)
                    twin["nvmeof_target"] = "spdk"
                    paired.append(twin)
            jobs = paired
            # Admit every storage target early, before spending the fleet on
            # unrelated write/load controls. Keep each immutable source block intact.
            jobs = ([job for job in jobs if job["cohort"] == "sealed_read"]
                    + [job for job in jobs if job["cohort"] != "sealed_read"])
            for number, job in enumerate(jobs):
                job["case_id"] = (f"{number:04d}-{comparison_backend(job)}-{job['value_size']}-"
                                  f"{job['profile']}-{job['cohort']}-r{job['repeat']}")
        if spec.get("native_durability_cohort"):
            sealed_native, standard_native = [], []
            for job in jobs:
                if job["backend"] not in TRANSPORTS:
                    continue
                sealed = (job["cohort"] == "sealed_read"
                          and "-scalar-" in job["profile"]
                          and job["profile"].endswith("-baseline"))
                standard = (job["cohort"] == "primary_rf1"
                            and job["profile"] in ("point-mixed", "read-many32", "write-concurrent"))
                if not (sealed or standard):
                    continue
                twin = copy.deepcopy(job)
                twin["durability_tier"] = "native"
                twin["case_id"] += "-durability-native"
                if sealed:
                    twin["sealed_dataset"]["source_id"] += "-durability-native"
                    sealed_native.append(twin)
                else:
                    standard_native.append(twin)
            split = next((i for i, job in enumerate(jobs) if job["cohort"] != "sealed_read"), len(jobs))
            jobs = jobs[:split] + sealed_native + jobs[split:] + standard_native
        if len(jobs) > spec["max_cases"]:
            raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
        return jobs
    if spec.get("mode") == "nvmeof-focused":
        return make_nvmeof_matrix(spec, backends, diagnostics)
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
        wanted = {"remote", "coordinator_hit", "wal_warm"}
        later = [job for job in tier_jobs if job["read_tier"]["name"] in wanted
                 and ((job["concurrency"] == 1 and job["value_size"] in (256, 8192))
                      or (job["concurrency"] == 16 and job["value_size"] in (1024, 8192)))]
        comparisons = [job for job in standard_jobs if job["cohort"] == "primary_rf1" and
                       (job["value_size"], job["profile"]) in ((1024, "write-concurrent"),
                           (1024, "read-concurrent"), (8192, "read-many32"))]
        comparisons += [job for job in standard_jobs if job["cohort"] == "write_stage_diagnostics"
                        and job["value_size"] == 1024 and job["profile"] == "write-concurrent"]
        comparisons += [job for job in standard_jobs if job["cohort"] == "write_timeline_diagnostics"
                        and job["value_size"] == 1024 and job["profile"] == "write-concurrent"]
        comparisons += [job for job in standard_jobs if job["cohort"] == "native_async_write"
                        and job["value_size"] == 1024]
        jobs = early + comparisons + later
        jobs.extend(job for job in standard_jobs if job["cohort"] == "valkey_client_diagnostics")
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
    jobs, read_jobs, fast_read_jobs, async_write_jobs = [], [], [], []
    for repeat in range(spec["repeats"]):
        order = list(backends)
        random.Random(spec["seed"] + repeat).shuffle(order)
        for backend in order:
            block = []
            def add(label, size, operation_mix, concurrency, count, *, cohort="primary_rf1",
                    rate=None, distribution="uniform", keys=None, cache=0, warmup=512):
                job = {"backend": backend, "repeat": repeat, "profile": label,
                    "cohort": cohort, "value_size": size, "keys": keys or spec["keys"],
                    "mix": operation_mix, "concurrency": concurrency, "measurement_operations": count,
                    "warmup_operations": warmup, "offered_ops_per_second": rate,
                    "distribution": distribution, "coordinator_cache_bytes": cache,
                    "drop_os_caches": cohort == "cold_os_cache",
                    "native_durability": "page_cache" if cohort == "native_pagecache" else "value_wal_barrier"}
                if backend == "native":
                    job["native_wal_geometry"] = "native_default"
                if backend in NVME_BACKENDS:
                    job["nvmeof_route"] = "live"
                if backend in TRANSPORTS:
                    job["owner_read_resident_first"] = cohort != "cold_os_cache"
                block.append(job)
            for size in sizes:
                for label, operation_mix, concurrency, count in profiles:
                    add(label, size, operation_mix, concurrency, count)
                # External comparators lacking atomic batches cannot join this cohort.
                if _allow_live_nvme and (backend == "native" or backend in TRANSPORTS):
                    add("write-batch32", size, mix(put_batch=100), 16, 2048)
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
            if backend == "native":
                for size in sizes:
                    for label, operation_mix, concurrency, count in (profiles[0], profiles[3]):
                        add(label, size, operation_mix, concurrency, count, cohort="native_pagecache")
                for job in block:
                    if job["cohort"] == "primary_rf1" and job["profile"] == "write-concurrent":
                        async_write_jobs.append(job | {"cohort": "native_async_write",
                                                       "native_durability": "page_cache"})
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
                            and job["cohort"] == "coordinator_cache_configured"]
                read_jobs += [job | {"cohort": "read_stage_diagnostics",
                    "diagnostic_source_cohort": job["cohort"],
                    "diagnostics": {"read_profile_limit": diagnostics["read_profile_limit"]}}
                    for job in sources]
            jobs.extend(primary + extra + diagnostic_jobs + cold)
    # Keep every pre-existing case ID and its derived namespace stable.
    jobs.extend(read_jobs)
    jobs.extend(fast_read_jobs)
    jobs.extend(async_write_jobs)
    if diagnostics["write_timeline_limit"]:
        jobs.extend(job | {"cohort": "write_timeline_diagnostics",
                           "diagnostics": {"write_profile_limit": 0,
                                           "write_timeline_limit": diagnostics["write_timeline_limit"]}}
                    for job in list(jobs) if job["backend"] in TRANSPORTS
                    and job["cohort"] == "primary_rf1"
                    and job["value_size"] == 1024 and job["profile"] == "write-concurrent")
    if diagnostics["valkey_profile_limit"]:
        jobs.extend(job | {"cohort": "valkey_client_diagnostics",
                           "diagnostic_source_cohort": job["cohort"],
                           "diagnostics": {"valkey_profile_limit": diagnostics["valkey_profile_limit"]}}
                    for job in list(jobs) if job["backend"] in ("valkey_tcp", "valkey_rdma")
                    and job["cohort"] == "primary_rf1"
                    and job["value_size"] in (1024, 8192)
                    and job["profile"] in ("read-concurrent", "write-concurrent", "read-many32"))
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        route = "-live-nvmeof" if job.get("nvmeof_route") == "live" else ""
        job["case_id"] = f"{number:04d}-{job['backend']}-{job['value_size']}-{job['profile']}-{job['cohort']}-r{job['repeat']}{route}"
    return jobs


def _contract_core_matrix(spec, sources):
    live_settings = {}
    if "tidehunter_nvme_rdma" in spec["backends"]:
        live = _clone_source(make_focused_matrix("nvmeof_path_compact"), "G18W",
                             "tidehunter_nvme_rdma", durability_tier="native",
                             diagnostics=None, measurement_operations=4096)
        live_settings = {key: live[key] for key in (*NVMEOF_PATH_LIVE_SETTING_KEYS, *NVMEOF_PATH_OPTIONAL_LIVE_SETTING_KEYS) if key in live}
        live_settings.update(owner_count=5, history_persistence="wal_replay",
                             live_lease_ahead=True, live_polled_writes=False)
    jobs = []
    for source in sources:
        if (source["cohort"] != "primary_rf1" or source["value_size"] not in (1024, 65536)
                or source["profile"] not in ("write-concurrent", "read-concurrent")):
            continue
        writing = source["profile"] == "write-concurrent"
        shapes = [copy.deepcopy(source), {**copy.deepcopy(source),
                  "profile": "write-c1" if writing else "read-c1", "concurrency": 1}]
        if writing and source["value_size"] == 1024 and source["repeat"] == 0:
            shapes.append({**copy.deepcopy(source), "profile": "write-sustained",
                           "measurement_operations": 200000})
        backend = source["backend"]
        default = "synchronous" if backend in TRANSPORTS else (
            "value_wal_barrier" if backend == "native" else "synced")
        arms = spec.get("arms", {}).get(backend, [default])
        for shape in shapes:
            for arm in arms if writing else arms[:1]:
                job = copy.deepcopy(shape)
                if backend in TRANSPORTS:
                    job["durability_tier"] = arm
                    job["history_persistence"] = "wal_replay"
                    # Standard one-caller point-mixed uses RemoteDatabaseConfig's default.
                    job["read_lanes"] = 2
                elif backend == "native":
                    job["native_durability"] = arm
                elif arm != "synced":
                    job["ack_contract"] = ack_contract.selected(backend, arm)
                if backend == "tidehunter_nvme_rdma":
                    job.update(live_settings)
                route = "-live-nvmeof" if job.get("nvmeof_route") == "live" else ""
                job["case_id"] = (f"{len(jobs):04d}-{backend}-{job['value_size']}-{job['profile']}-"
                                  f"{job['cohort']}-r{job['repeat']}-{arm}{route}")
                jobs.append(job)
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    return jobs


def make_nvmeof_matrix(spec, backends, diagnostics):
    """Matched sealed-source read cohorts; profiled twins are independent runs."""
    if spec["repeats"] != 1:
        raise ValueError("NVMe-oF comparison requires one repetition")
    required = {"native", "tidehunter_tcp"}
    if not required <= set(backends) or not (set(backends) & NVME_BACKENDS):
        raise ValueError("NVMe-oF comparison requires native, RPC TCP and at least one NVMe-oF backend")
    allowed = required | NVME_BACKENDS | {"tidehunter_efa", "tidehunter_efa_direct", "tidehunter_efa_native",
                                          "tidehunter_verbs", "tidehunter_verbs_native"}
    if set(backends) - allowed:
        raise ValueError("NVMe-oF comparison excludes unrelated KV backends")
    # These two readers must share one physical source block. A native or
    # different transport case ends that block; its evidence directory remains
    # immutable and cannot be recreated later in the same matrix.
    tcp_readers = {"tidehunter_tcp"} | NVME_BACKENDS
    first_tcp = next(index for index, backend in enumerate(backends) if backend in tcp_readers)
    backends = (list(backends[:first_tcp])
                + [backend for backend in backends if backend in tcp_readers]
                + [backend for backend in backends[first_tcp:] if backend not in tcp_readers])
    jobs, writes = [], []
    for size in (1024, 8192, 64, 256, 65536):
        keys = max(spec["keys"], 65536 if size in (1024, 8192) else 8192)
        source = f"sealed-v2-seed{spec['seed']}-size{size}-keys{keys}"
        shapes = (("scalar", 1), ("scalar", 16), ("batch32", 1), ("batch32", 16)) if size in (1024, 8192) else (("scalar", 1),)
        for backend in backends:
            physical = ("tcp" if backend in tcp_readers else
                        "native" if backend == "native" else backend)
            for shape, concurrency in shapes:
                for tier in ("first_touch", "repeat_read"):
                    for profiled in ((False, True) if backend != "native" and
                                     (size, shape, concurrency) in ((1024, "scalar", 1), (8192, "batch32", 16))
                                     else (False,)):
                        drop = tier == "first_touch"
                        mapped_eviction = drop and backend not in NVME_BACKENDS
                        job = {"backend": backend, "repeat": 0,
                               "profile": f"{tier}-{shape}-c{concurrency}-{'diagnostic' if profiled else 'baseline'}",
                               "cohort": "sealed_read", "value_size": size, "keys": keys,
                               "mix": mix(get_many=100) if shape == "batch32" else mix(read=100),
                               "concurrency": concurrency, "read_lanes": 0 if backend == "native" else (4 if concurrency == 16 else 1),
                               "measurement_operations": 2048 if shape == "batch32" else 8192,
                               "warmup_operations": 0, "offered_ops_per_second": None,
                               "distribution": "uniform", "coordinator_cache_bytes": 0,
                               "drop_os_caches": drop,
                               "native_durability": "value_wal_barrier",
                               "measurement_key_sweep": tier == "first_touch",
                               "evict_mapped_wal_before_measurement": mapped_eviction,
                               "sealed_dataset": {"source_id": f"{source}-{physical}", "data_id": source, "seed": spec["seed"], "profiled": profiled,
                                                  "tier": tier, "instrumentation": "read_profile" if profiled and backend != "native" else "none",
                                                  "page_cache_policy": "drop_at_measurement_gate" if drop else "not_dropped",
                                                  "mapped_wal_eviction": "before_measurement_gate" if mapped_eviction else "not_requested",
                                                  "directness": "initiator_O_DIRECT_target_buffered" if backend in NVME_BACKENDS else "buffered_WAL_pagecache_or_device",
                                                  "cold_claim": "distinct keys after OS cache drop do not prove per-call device reads; shared pages, readahead and device caches remain",
                                                  "target_cache": "buffered_file_page_cache" if backend in NVME_BACKENDS else "not_applicable"}}
                        if backend in NVME_BACKENDS:
                            job["nvmeof_route"] = "sealed_read"
                        if backend == "native":
                            job["native_wal_geometry"] = "native_default"
                        if profiled and backend != "native":
                            job["diagnostics"] = {"read_profile_limit": diagnostics["read_profile_limit"] or 256}
                        jobs.append(job)
                        if spec.get("spdk_comparison") and backend == "tidehunter_nvme_rdma":
                            spdk = copy.deepcopy(job)
                            spdk["nvmeof_target"] = "spdk"
                            spdk["sealed_dataset"]["directness"] = "initiator_O_DIRECT_target_file_O_DIRECT"
                            spdk["sealed_dataset"]["target_cache"] = "direct_file_bdev_no_OS_pagecache"
                            spdk["sealed_dataset"]["cold_claim"] = (
                                "SPDK file direct I/O bypasses OS cache; device/controller caches remain. "
                                "Compare actual device evidence, not first-touch keys alone.")
                            jobs.append(spdk)
        for backend in (name for name in backends if name not in NVME_BACKENDS):
            for profiled in ((False, True) if backend != "native" and size in (1024, 8192) else (False,)):
                job = {"backend": backend, "repeat": 0, "profile": "write-c16-" + ("diagnostic" if profiled else "baseline"),
                       "cohort": "native_write_regression", "value_size": size, "keys": keys,
                       "write_path": "native_local_WAL" if backend == "native" else "common_RF1_owner_WAL_existing_transport_no_NVMe_reader_installed",
                       "mix": mix(put=100), "concurrency": 16, "read_lanes": 0 if backend == "native" else 4,
                       "measurement_operations": 4096, "warmup_operations": 512,
                       "offered_ops_per_second": None, "distribution": "uniform",
                       "coordinator_cache_bytes": 0,
                       "drop_os_caches": False, "native_durability": "value_wal_barrier"}
                if backend == "native":
                    job["native_wal_geometry"] = "native_default"
                if profiled and backend != "native":
                    job["diagnostics"] = {"write_profile_limit": diagnostics["write_profile_limit"] or 256}
                writes.append(job)
                if size in (1024, 8192) and not profiled:
                    batch = copy.deepcopy(job)
                    batch["profile"] = job["profile"].replace("write-c16-", "write-batch32-c16-", 1)
                    batch["mix"] = mix(put_batch=100)
                    batch["measurement_operations"] = 2048
                    writes.append(batch)
    jobs.extend(writes)
    if diagnostics["write_timeline_limit"]:
        jobs.extend(job | {"cohort": "write_timeline_diagnostics",
                           "profile": job["profile"].replace("-baseline", "-timeline"),
                           "diagnostics": {"write_profile_limit": 0,
                                           "write_timeline_limit": diagnostics["write_timeline_limit"]}}
                    for job in writes if job["backend"] in TRANSPORTS
                    and ((job["value_size"] == 1024 and job["profile"] == "write-c16-baseline")
                         or (job["value_size"] == 8192 and job["profile"] == "write-batch32-c16-baseline")))
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        job["case_id"] = f"{number:04d}-{comparison_backend(job)}-{job['value_size']}-{job['profile']}"
    return jobs


def make_read_tier_matrix(spec, backends, diagnostics):
    """A single ordered fleet experiment; each instrumented twin has its own population."""
    selected = [backend for backend in backends if backend in TRANSPORTS]
    if not selected:
        raise ValueError("read-tier mode requires at least one distributed Tidehunter backend")
    limit = diagnostics["read_profile_limit"] or 4096
    working_keys = max(spec["keys"], 8192)
    tiers = ("remote", "coordinator_hit", "wal_warm", "native_hot_wal_lru0_pagecache",
             "cold_device", "native_cold_os_lru0", "bounded_mixed")
    jobs = []
    for tier in tiers:
        for repeat in range(spec["repeats"]):
            for backend in (["native"] if tier.startswith("native_") and "native" in backends
                            else [] if tier.startswith("native_") else selected):
                for size in ((256, 1024, 8192, 65536) if backend == "native"
                             else (256, 8192, 65536)):
                    for profiled in ((False,) if backend == "native" else (False, True)):
                        # Size pure-hit controls for the finite dataset plus cache metadata,
                        # without claiming four-way placement can retain every key.
                        footprint = working_keys * (size + 512)
                        cache = max(128 * 1024**2, 2 * footprint) if tier == "coordinator_hit" else (1024**2 if tier == "bounded_mixed" else 0)
                        coordinator_allowance = max(512 * 1024**2, cache + 64 * 1024**2)
                        owner_allowance = 512 * 1024**2
                        sweep = tier in ("coordinator_hit", "wal_warm", "native_hot_wal_lru0_pagecache")
                        warmup = working_keys if sweep else (0 if tier in ("cold_device", "native_cold_os_lru0") else 512)
                        source = f"{backend}-{size}-keys{working_keys}-scalar-c1-l{'0' if backend == 'native' else '1'}-seed{1775 + repeat}-r{repeat}"
                        tier_name = tier
                        job = {"backend": backend, "repeat": repeat, "profile": "read-tier-profiled" if profiled else "read-tier-baseline",
                            "cohort": "read_tier_" + tier_name,
                            "value_size": size, "keys": working_keys, "mix": mix(read=100),
                            "concurrency": 1, "read_lanes": 1,
                            "measurement_operations": 8192,
                            "warmup_operations": warmup, "offered_ops_per_second": None,
                            "distribution": "uniform", "coordinator_cache_bytes": cache,
                            "drop_os_caches": tier in ("cold_device", "native_cold_os_lru0"),
                            "native_durability": "value_wal_barrier", "warmup_key_sweep": sweep,
                            "owner_read_resident_first": tier == "wal_warm",
                            "read_tier": {"name": tier_name, "source_id": source,
                                "data_id": f"read-tier-v1-seed{1775 + repeat}-r{repeat}-size{size}-keys{working_keys}",
                                "profiled": profiled,
                                "requested": {"coordinator_cache_bytes": cache,
                                    "owner_read_resident_first": tier == "wal_warm",
                                    "page_cache_policy": "drop_at_measurement_gate" if tier in ("cold_device", "native_cold_os_lru0") else "not_dropped",
                                    "warmup_operations": warmup, "warmup_key_sweep": sweep,
                                    "device_trace": "enabled" if backend == "native" or tier == "cold_device" else "disabled",
                                    "measurement_key_sweep": False,
                                    "read_lanes": 0 if backend == "native" else 1, "concurrency": 1,
                                    "sessions": "not_applicable" if backend == "native" else "runtime_default"},
                                "effective": None}}
                        if backend != "native":
                            job["coordinator_read_memory_allowance_bytes"] = coordinator_allowance
                            job["owner_read_memory_allowance_bytes"] = owner_allowance
                            job["read_tier"]["requested"]["coordinator_read_memory_allowance_bytes"] = coordinator_allowance
                            job["read_tier"]["requested"]["owner_read_memory_allowance_bytes"] = owner_allowance
                        if backend == "native":
                            job["native_wal_geometry"] = "native_default"
                            job["read_tier"]["requested"]["native_value_lru_bytes"] = 0
                            job["read_tier"]["requested"]["native_value_lru_source"] = "KeyShape::new_single default"
                        if profiled:
                            job["diagnostics"] = {"read_profile_limit": limit}
                        jobs.append(job)
    supplemental = []
    wanted = {"remote", "coordinator_hit", "wal_warm", "bounded_mixed",
              "native_hot_wal_lru0_pagecache"}
    for original in jobs:
        tier = original["read_tier"]["name"]
        if tier not in wanted or original["value_size"] != 8192:
            continue
        for size, shape, calls in ((1024, "scalar", 8192), (8192, "batch32", 2048)):
            job = copy.deepcopy(original)
            profiled = job["read_tier"]["profiled"]
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
    jobs.sort(key=lambda job: tier_order[job["read_tier"]["name"]])
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        variant = job["profile"] if "-c16-" in job["profile"] else ("profiled" if job["read_tier"]["profiled"] else "baseline")
        job["case_id"] = f"{number:04d}-{job['backend']}-{job['value_size']}-{job['read_tier']['name']}-{variant}-r{job['repeat']}"
    return jobs


FOCUSED_SELECTIONS = ("recommended", "full", "device", "floors", "stages", "unified",
                      "append_floors", "fair_native", "parity", "parity_compact", "fair_five_owner",
                      "remote_rw_compact", "fabric_ab_compact", "leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS)
FOCUSED_COUNTS = {"recommended": 279, "full": 333, "device": 120, "floors": 69,
                  "stages": 183, "ssd_write": 18, "unified": 270, "append_floors": 48,
                  "fair_native": 168, "parity": 216, "parity_compact": 177, "fair_five_owner": 39,
                  "remote_rw_compact": 342, "fabric_ab_compact": 80, "leased_ab_compact": 72,
                  "nvmeof_path_compact": 157, "nvmeof_path_core": 66, "nvmeof_path_fix": 37,
                  "nvmeof_first_put": 12, "nvmeof_contracts": 34, "nvmeof_contracts_crossing": 35,
                  "nvmeof_contracts_crossing2": 36, "nvmeof_parallel_writes": 44, "nvmeof_single_caller": 20, "valkey_rdma_reads": 22,
                  "nvmeof_reads_geometry": 74, "nvmeof_reads_geometry_rest": 20, "nvmeof_reads_geometry_rest2": 18}
FOCUSED_COUNTS["nvmeof_f6"] = 42
FOCUSED_COUNTS["nvmeof_f6b"] = 58
FOCUSED_COUNTS["nvmeof_f8"] = 36
FOCUSED_COUNTS["nvmeof_f7_highmem32"] = 8
NVMEOF_PATH_NATIVE_SETTING_KEYS = ("native_shards", "native_wal_geometry")
NVMEOF_PATH_REMOTE_SETTING_KEYS = ("owner_count", "owner_polling", "owner_wal_geometry", "history_persistence")
NVMEOF_PATH_LIVE_SETTING_KEYS = (*NVMEOF_PATH_REMOTE_SETTING_KEYS, "nvmeof_route", "nvmeof_target",
                                "target_io", "live_write_mode", "live_export_mode", "live_inline_reads",
                                "live_io_queues", "live_io_queue_size", "live_poll_queues",
                                "live_lease_ahead", "live_polled_writes")
NVMEOF_PATH_OPTIONAL_LIVE_SETTING_KEYS = ("live_leased_write_window", "live_max_inflight_requests",
                                        "coordinator_read_memory_allowance_bytes", "live_leased_sync",
                                        "live_pin_read_lanes", "live_leased_write_lanes",
                                        "live_leased_flush_min_frame_bytes")
NVMEOF_PATH_SETTING_KEYS = {
    "native": NVMEOF_PATH_NATIVE_SETTING_KEYS,
    "tidehunter_verbs_native": NVMEOF_PATH_REMOTE_SETTING_KEYS,
    # Compact/core documents predate the two fix-selection settings.
    "tidehunter_nvme_rdma": NVMEOF_PATH_LIVE_SETTING_KEYS[:-2],
}
_FOCUSED_GROUP_COUNTS = {
    "recommended": {"G1": 81, "G2": 24, "G3": 6, "G4": 12, "G5": 150, "anchors": 6},
    "full": {"G1": 81, "G2": 24, "G3": 6, "G4": 12, "G5": 150,
             "anchors": 6, "G7": 48, "G9": 6},
    # The device-only follow-up holds only the cases appended after the full selection.
    "device": {"G10R": 60, "G10W": 60},
    "floors": {"G11N": 45, "G11S": 24},
    "stages": {"G12N": 52, "G12W": 60, "G12H": 12, "G12K": 12, "G12D": 43, "G12S": 4},
    "unified": {"G12N": 52, "G12W": 60, "G12H": 12, "G12K": 12, "G12D": 43,
                "G12S": 4, "G11N": 45, "G11S": 24, "G11W": 18},
    "append_floors": {"G13W": 48},
    "fair_native": {"G14N": 54, "G14S": 36, "G14O1": 36, "G14O5": 36, "G14P": 6},
    "parity": {"G13W": 48, "G14N": 54, "G14S": 36, "G14O1": 36, "G14O5": 36, "G14P": 6},
    "parity_compact": {"G13W": 48, "G14N": 54, "G14S": 36, "G14O1": 36, "G14P": 3},
    "fair_five_owner": {"G14O5": 36, "G14P": 3},
    "remote_rw_compact": {"G13W": 48, "G11N": 45, "G11S": 24, "G15F": 24,
                          "G14N": 54, "G14O1": 36, "G14P": 3, "G15C": 54, "G15M": 54},
    "fabric_ab_compact": {"G16P": 40, "G16C": 40},
    "leased_ab_compact": {"G17W": 50, "G17R": 22},
    "nvmeof_path_compact": {"G18F": 10, "G18N": 32, "G18W": 35, "G18R": 24,
                             "G18M": 18, "G18B": 30, "G18X": 8},
    "nvmeof_path_core": {"G18F": 4, "G18N": 17, "G18W": 19, "G18R": 12,
                          "G18M": 7, "G18B": 5, "G18X": 2},
    "nvmeof_path_fix": {"G19W": 21, "G19R": 7, "G19N": 8, "G19X": 1},
    "nvmeof_first_put": {"G20W": 9, "G20R": 1, "G20N": 2},
    "nvmeof_contracts": {"G21W": 16, "G21R": 8, "G21N": 10},
    "nvmeof_contracts_crossing": {"G21W": 16, "G21R": 8, "G21N": 10, "G22W": 1},
    "nvmeof_contracts_crossing2": {"G21W": 16, "G21R": 8, "G21N": 10, "G22W": 2},
    "nvmeof_parallel_writes": {"G23W": 32, "G23X": 6, "G23N": 6},
    "nvmeof_single_caller": {"G28W": 8, "G28C": 8, "G28N": 4},
    "nvmeof_f6": {"G29W": 20, "G29X": 6, "G29R": 8, "G29G": 4, "G24M": 4},
    "nvmeof_f6b": {"G31W": 24, "G31H": 8, "G31S": 2, "G31X": 6, "G31G": 18},
    "nvmeof_f8": {"G32W": 16, "G32H": 8, "G32S": 4, "G32N": 8},
    "nvmeof_f7_highmem32": {"G30R": 4, "G30S": 2, "G30W": 2},
    "nvmeof_reads_geometry": {"G24R": 24, "G24Q": 24, "G24L": 2, "G24P": 2,
                              "G24S": 2, "G24M": 2, "G24C": 6, "G24W": 8, "G24N": 4},
    "nvmeof_reads_geometry_rest": {"G24M": 2, "G24C": 6, "G24W": 8, "G24N": 4},
    "nvmeof_reads_geometry_rest2": {"G24C": 6, "G24W": 8, "G24N": 4},
    "valkey_rdma_reads": {"G25R": 12, "G25M": 2, "G25A": 8},
}


def selection_owner_hosts(selection):
    """Return the reviewed layout's owner host count."""
    return 1 if selection in ("parity_compact", "remote_rw_compact", "fabric_ab_compact", "leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS) else 5


def job_owner_hosts(job):
    """Return the owner hosts required by this job."""
    # Native and these floors need at most owner 0 regardless of the fleet layout.
    if (job.get("backend") == "native" or job.get("case_kind") == "ssd_write_floor"
            or job.get("group") in ("G11N", "G11S", "G15F")):
        return 1
    count = job.get("owner_count", 5)
    if type(count) is not int or count not in OWNER_HOST_COUNTS:
        raise ValueError("owner_count must be 1 or 5")
    return count


def focused_group_counts(selection):
    if selection not in FOCUSED_SELECTIONS:
        raise ValueError("unknown focused selection")
    groups = dict(_FOCUSED_GROUP_COUNTS[selection])
    if selection in ("stages", "unified", "append_floors", "fair_native", "parity", "parity_compact", "fair_five_owner", "remote_rw_compact", "fabric_ab_compact", "leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS):
        jobs = make_focused_matrix(selection)
        for kind in {job["case_kind"] for job in jobs}:
            groups[kind] = sum(job["case_kind"] == kind for job in jobs)
        return groups
    kinds = {"G1": "media_read", "G2": "fio_floor", "G3": "block_trace",
             "G4": "interference", "G5": "sealed_knee", "anchors": "native_anchor",
             "G7": "high_concurrency", "G9": "write_wait_diagnostic",
             "G10R": "media_read", "G10W": "remote_put",
             "G11N": "net_floor", "G11S": "ssd_floor"}
    return {**groups, **{kinds[group]: count for group, count in groups.items()}}


def _focused_jobs():
    """Build the frozen matrix without consulting mutable external specifications."""
    jobs = []
    routes = (("tidehunter_verbs_native", None), ("tidehunter_tcp", None),
              ("tidehunter_nvme_rdma", "kernel"), ("tidehunter_nvme_rdma", "spdk"))
    def add(group, kind, repeat, backend, target=None, *, size=1024, concurrency=1,
            operation="read", tier="repeat_read", media=None, extra=None):
        profile = f"{group}-{operation}-c{concurrency}-{tier}"
        if kind == "native_anchor":
            profile = "read-concurrent" if operation == "read" else "write-concurrent"
        job = {"case_kind": kind,
               "selection": ("nvmeof_path_compact" if group.startswith("G18") else
                             "leased_ab_compact" if group.startswith("G17") else
                             "fabric_ab_compact" if group.startswith("G16") else
                             "remote_rw_compact" if group.startswith("G15")
                             else "fair_native" if group.startswith("G14")
                             else "append_floors" if group == "G13W"
                             else "unified" if group == "G11W"
                             else "stages" if group.startswith("G12")
                             else "floors" if group in ("G11N", "G11S")
                             else "device" if group in ("G10R", "G10W")
                             else "full" if group in ("G7", "G9") else "recommended"),
               "group": group, "backend": backend, "repeat": repeat,
               "profile": profile, "cohort": "sealed_read" if operation == "read" or kind == "interference" else "primary_rf1",
               "value_size": size, "keys": 4096, "mix": mix(**{operation: 100}),
               "concurrency": concurrency, "read_lanes": 0 if backend == "native" else (4 if concurrency >= 16 else 1),
               "measurement_operations": (400000 if kind == "interference" else
                                          4096 if tier == "first_touch" or operation == "put" else 8192),
               "warmup_operations": 0 if operation == "read" else 512,
               "offered_ops_per_second": None, "distribution": "uniform",
               "coordinator_cache_bytes": 0, "drop_os_caches": tier == "first_touch",
               "native_durability": "value_wal_barrier",
               "durability_tier": "value_wal_barrier" if backend == "native" else "synchronous",
               "target_io": "not_applicable" if target is None else ("buffered" if target == "kernel" else "spdk_direct"),
               "execution": {"queue_capacity": max(128, concurrency)}}
        if backend == "native":
            job["native_wal_geometry"] = "native_default"
        if target:
            job.update(nvmeof_route="sealed_read", nvmeof_target=target)
        if media is not None:
            job["media"] = media
            if media["history"] == "prewarm":
                job["drop_os_caches"] = False
                job["evict_mapped_wal_before_measurement"] = True
            elif media["wal_eviction"] == "selective":
                # Device-only owner arms: evict the mapped WAL, then drop the page cache at the gate.
                job["evict_mapped_wal_before_measurement"] = True
            if media["expected"] == "memory" or media["history"] == "prewarm":
                job["warmup_operations"] = job["keys"]
                job["warmup_key_sweep"] = True
        if extra:
            job.update(extra)
        # Every physical source is a single contiguous block, isolated by repeat,
        # transport, target, tier and complete case dimensions.
        identity = hashlib.sha256(json.dumps(job, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
        job["case_id"] = f"{group}-r{repeat}-{identity}"
        if ((operation == "read" and not group.startswith("G17")) or kind in ("interference", "hybrid_put")) and (not group.startswith("G18") or backend == "native"):
            physical = backend + ("-" + target if target else "")
            dataset_size = 1024 if kind == "hybrid_put" else size
            job["sealed_dataset"] = {
                "source_id": f"focused-r{repeat}-{physical}-s{dataset_size}-seed1775-{tier}-{identity}",
                "data_id": f"focused-r{repeat}-s{dataset_size}-seed1775",
                "seed": 1775, "tier": tier, "profiled": False, "instrumentation": "none",
                "page_cache_policy": ("evict_mapped_wal_then_drop_at_measurement_gate"
                                      if job.get("evict_mapped_wal_before_measurement") and job["drop_os_caches"]
                                      else "selective_wal_eviction" if job.get("evict_mapped_wal_before_measurement")
                                      else "drop_at_measurement_gate" if tier == "first_touch" else "not_dropped"),
                "mapped_wal_eviction": ("before_measurement_gate" if job.get("evict_mapped_wal_before_measurement")
                                        else "not_requested"),
                "directness": ("initiator_O_DIRECT_target_kernel_buffered_io_0" if job["target_io"] == "direct" else
                               "initiator_O_DIRECT_target_file_O_DIRECT" if target == "spdk" else
                               "initiator_O_DIRECT_target_buffered" if target else "buffered_WAL_pagecache_or_device"),
                "cold_claim": "first touch does not establish per-call device reads",
                "target_cache": ("direct_kernel_buffered_io_0_no_target_pagecache" if job["target_io"] == "direct" else
                                 "direct_file_bdev_no_OS_pagecache" if target == "spdk" else
                                 "buffered_file_page_cache" if target else "not_applicable")}
            job["measurement_key_sweep"] = tier == "first_touch" and job.get("key_order") != "random_with_replacement"
        jobs.append(job)

    for repeat in range(3):
        for size in (1024, 8192, 65536):
            for backend, target in routes:
                add("G1", "media_read", repeat, backend, target, size=size, tier="first_touch",
                    media={"expected": "cold", "history": "normal", "wal_eviction": "none"})
            add("G1", "media_read", repeat, "tidehunter_verbs_native", size=size, tier="first_touch",
                media={"expected": "cold", "history": "prewarm", "wal_eviction": "selective"})
            for backend, target in routes[:3]:
                add("G1", "media_read", repeat, backend, target, size=size,
                    media={"expected": "memory", "history": "normal", "wal_eviction": "none"})
        # Append direct controls after the existing G1 cases for this repeat.
        for size in (1024, 8192, 65536):
            add("G1", "media_read", repeat, "tidehunter_nvme_rdma", "kernel", size=size,
                tier="first_touch", media={"expected": "cold", "history": "normal", "wal_eviction": "none"},
                extra={"target_io": "direct"})
        for location in ("local_file", "nvmeof_namespace"):
            for block in (4096, 65536):
                for depth in (1, 16):
                    add("G2", "fio_floor", repeat, "tidehunter_nvme_rdma" if location == "nvmeof_namespace" else "tidehunter_verbs_native",
                        "kernel" if location == "nvmeof_namespace" else None, size=65536,
                        extra={"fio": {"location": location, "block_bytes": block, "queue_depth": depth,
                                       "runtime_seconds": 10, "direct": True, "pattern": "randread"}})
        for scope, operation in (("cold_owner_read", "read"), ("owner_put", "put")):
            add("G3", "block_trace", repeat, "tidehunter_verbs_native", operation=operation,
                tier="first_touch" if operation == "read" else "repeat_read",
                extra={"trace": {"scope": scope, "require_file_attribution": True}})
        for backend, target in routes:
            add("G4", "interference", repeat, backend, target, concurrency=16,
                extra={"interference": {"read_offered_per_second": 1000, "read_queue_capacity": 4096,
                       "put_concurrency": 16, "duration_seconds": 20, "key_policy": "disjoint_read_put"}})
        for backend, target in (("native", None), *routes):
            for tier in ("first_touch", "repeat_read"):
                for concurrency in (1, 2, 4, 8, 16):
                    add("G5", "sealed_knee", repeat, backend, target, concurrency=concurrency, tier=tier)
        for operation in ("read", "put"):
            add("anchors", "native_anchor", repeat, "native", concurrency=16, operation=operation)
    for repeat in range(3):
        for operation in ("read", "put"):
            for backend, target in (("native", None), *routes):
                if operation == "put" and target:
                    continue
                for concurrency in (64, 256):
                    add("G7", "high_concurrency", repeat, backend, target, concurrency=concurrency, operation=operation)
        for enabled in (False, True):
            limit = 256 if enabled else 0
            add("G9", "write_wait_diagnostic", repeat, "tidehunter_verbs_native", concurrency=16,
                operation="put", extra={"write_wait_diagnostics": {"enabled": enabled, "timeline_limit": limit},
                                        "diagnostics": {"write_timeline_limit": limit}})
    # G10, the device-only follow-up: cold reads on spread datasets with random keys, and plain puts.
    # A 4,096-key sweep reads each owner's WAL in order, so kernel readahead serves most calls.
    # Random keys over a large dataset make each call a first touch; device counters qualify it.
    first_device = len(jobs)
    # The sealed preparer admits at most 65,536 keys, and a hybrid reader charges 4 KiB per catalog entry
    # against a 448 MiB allowance. With 65,536 keys, 4,096 random reads of 8 or 64 KiB values first-touch
    # about 0.97 of their keys. At 1 KiB about four values share a 4 KiB page, so 2,048 reads keep the
    # first-touch share near 0.94 (4,096 reads would give about 0.88, below the 0.9 cold test).
    device_routes = (("tidehunter_verbs_native", None), ("tidehunter_tcp", None),
                     ("tidehunter_nvme_rdma", "kernel"), ("tidehunter_nvme_rdma", "spdk"))
    for repeat in range(3):
        for backend, target in device_routes:
            for size, concurrency in ((1024, 1), (8192, 1), (65536, 1), (1024, 16), (65536, 16)):
                extra = {"keys": 65536, "key_order": "random_with_replacement"}
                if size == 1024:
                    extra["measurement_operations"] = 2048
                if target == "kernel":
                    extra["target_io"] = "direct"
                add("G10R", "media_read", repeat, backend, target, size=size, concurrency=concurrency,
                    tier="first_touch", extra=extra,
                    media={"expected": "cold", "history": "normal",
                           "wal_eviction": "selective" if target is None else "none"})
        for backend in ("tidehunter_verbs_native", "tidehunter_tcp"):
            for owner_tier in ("synchronous", "native"):
                for size in (1024, 65536):
                    for concurrency in (1, 16):
                        add("G10W", "remote_put", repeat, backend, size=size, concurrency=concurrency,
                            operation="put", extra={"durability_tier": owner_tier})
                add("G10W", "remote_put", repeat, backend, size=1024, concurrency=16, operation="put",
                    extra={"durability_tier": owner_tier, "measurement_operations": 200000})
    device = jobs[first_device:]

    def canary(group, backend, size, concurrency, detail=None, operations=None):
        for job in device:
            if (job["repeat"] == 0 and job["group"] == group and job["backend"] == backend
                    and job["value_size"] == size and job["concurrency"] == concurrency
                    and detail in (None, job.get("nvmeof_target"), job["durability_tier"])
                    and operations in (None, job["measurement_operations"])):
                return job
        raise ValueError("device canary case missing")

    # One case of every new flow first, so a failed cold gate or put flow shows within minutes.
    canaries = [canary("G10R", "tidehunter_verbs_native", 65536, 1),
                canary("G10R", "tidehunter_tcp", 1024, 1),
                canary("G10R", "tidehunter_nvme_rdma", 1024, 1, "spdk"),
                canary("G10R", "tidehunter_nvme_rdma", 8192, 1, "kernel"),
                canary("G10W", "tidehunter_verbs_native", 1024, 1, "synchronous", 4096),
                canary("G10W", "tidehunter_verbs_native", 1024, 1, "native", 4096),
                canary("G10W", "tidehunter_verbs_native", 1024, 16, "synchronous", 200000)]
    chosen = {job["case_id"] for job in canaries}
    jobs[first_device:] = canaries + [job for job in device if job["case_id"] not in chosen]
    first_floors = len(jobs)
    sizes = [8, 1024, 4096, 65536]
    for repeat in range(3):
        for tool in ("ib_send_lat", "ib_write_lat", "ib_read_lat"):
            for mode in ("polled", "event"):
                add("G11N", "net_floor", repeat, "tidehunter_verbs_native", size=65536,
                    extra={"net": {"tool": tool, "mode": mode, "interface": "rdma0",
                                   "provider": None, "endpoint": None, "sizes": sizes,
                                   "iterations": 5000, "warmup": 0}})
        for provider, endpoint, interface in (("verbs;ofi_rxm", "rdm", "rdma0"),
                                               ("verbs", "msg", "rdma0"),
                                               ("tcp;ofi_rxm", "rdm", "eth0")):
            add("G11N", "net_floor", repeat, "tidehunter_verbs_native", size=65536,
                extra={"net": {"tool": "fi_pingpong", "mode": None, "interface": interface,
                               "provider": provider, "endpoint": endpoint, "sizes": sizes,
                               "iterations": 5000, "warmup": 0}})
        for interface in ("eth0", "rdma0"):
            for mode in ("block", "spin", "busy_poll"):
                add("G11N", "net_floor", repeat, "tidehunter_verbs_native", size=65536,
                    extra={"net": {"tool": "tcp_rtt", "mode": mode, "interface": interface,
                                   "provider": None, "endpoint": None, "sizes": sizes,
                                   "iterations": 20000, "warmup": 2000}})
        for block in (4096, 65536):
            for region in (67108864, 2147483648):
                for state, age in (("fresh", 0), ("aged", 60)):
                    add("G11S", "ssd_floor", repeat, "tidehunter_verbs_native", size=65536,
                        extra={"ssd": {"block_bytes": block, "region_bytes": region,
                                       "state": state, "age_seconds": age, "runtime_seconds": 6,
                                       "queue_depth": 1, "pattern": "randread", "direct": True}})
    floors = jobs[first_floors:]
    canaries = [
        next(job for job in floors if job["repeat"] == 0 and job.get("net", {}).get("tool") == "ib_send_lat"
             and job["net"]["mode"] == "polled"),
        next(job for job in floors if job["repeat"] == 0 and job.get("net", {}).get("provider") == "verbs;ofi_rxm"),
        next(job for job in floors if job["repeat"] == 0 and job.get("net", {}).get("tool") == "tcp_rtt"
             and job["net"]["interface"] == "eth0" and job["net"]["mode"] == "block"),
        *[next(job for job in floors if job["repeat"] == 0 and job.get("ssd", {}).get("state") == state
               and job["ssd"]["block_bytes"] == 4096 and job["ssd"]["region_bytes"] == 67108864)
          for state in ("fresh", "aged")],
    ]
    chosen = {job["case_id"] for job in canaries}
    jobs[first_floors:] = canaries + [job for job in floors if job["case_id"] not in chosen]
    first_stages = len(jobs)

    def write_case(group, kind, repeat, backend, operation, size, concurrency, durability,
                   target=None, extra=None):
        fields = {"measurement_operations": 1024 if operation == "put_batch" else 4096,
                  "durability_tier": durability}
        if backend == "native":
            fields["native_durability"] = durability
        if target == "kernel":
            fields["target_io"] = "direct"
        if kind == "hybrid_put":
            fields["read_lanes"] = 4
        fields.update(extra or {})
        add(group, kind, repeat, backend, target, size=size, concurrency=concurrency,
            operation=operation, extra=fields)

    for repeat in range(2):
        for operation, sizes in (("put", (1024, 8192, 65536)), ("put_batch", (1024, 8192)),
                                 ("delete", (1024,))):
            if repeat and operation != "put":
                continue
            for size in sizes:
                for concurrency in (1, 16):
                    for durability in ("page_cache", "value_wal_barrier"):
                        write_case("G12N", "native_op", repeat, "native", operation, size, concurrency, durability)
        for size in (8192, 65536):
            for concurrency in (1, 16):
                add("G12N", "native_op", repeat, "native", size=size, concurrency=concurrency)
        for size in (1024, 8192, 65536):
            for concurrency in (1, 16):
                if concurrency == 16 and (repeat or size == 8192):
                    continue
                add("G12N", "native_media_read", repeat, "native", size=size, concurrency=concurrency,
                    tier="first_touch", media={"expected": "cold", "history": "normal", "wal_eviction": "selective"},
                    extra={"keys": 65536, "key_order": "random_with_replacement",
                           "measurement_operations": 2048 if size == 1024 else 4096})
        for operation, sizes in (("put", (1024, 8192, 65536)), ("put_batch", (1024, 8192)),
                                 ("delete", (1024,))):
            if repeat and operation != "put":
                continue
            for size in sizes:
                for concurrency in (1, 16):
                    for tier in ("synchronous", "native"):
                        write_case("G12W", "remote_put", repeat, "tidehunter_verbs_native",
                                   operation, size, concurrency, tier)
        for backend, tiers in (("tidehunter_tcp", ("synchronous", "native")),
                               ("tidehunter_verbs", ("synchronous",))):
            for size in (1024, 65536):
                for concurrency in (1, 16):
                    for tier in tiers:
                        write_case("G12W", "remote_put", repeat, backend, "put", size, concurrency, tier)
        for target in ("kernel", "spdk"):
            for size, concurrency in ((1024, 1), (1024, 16), (65536, 16)):
                write_case("G12H", "hybrid_put", repeat, "tidehunter_nvme_rdma",
                           "put", size, concurrency, "synchronous", target)
        for backend, kind, tier in (("native", "native_op", "value_wal_barrier"),
                                     ("tidehunter_verbs_native", "remote_put", "synchronous")):
            for size, concurrency in ((1024, 1), (1024, 16), (65536, 16)):
                write_case("G12K", kind, repeat, backend, "put", size, concurrency, tier,
                           extra={"put_key_policy": "new"})

    def diagnostic_copy(source, field, limit, group="G12D", extra=None):
        job = copy.deepcopy(source)
        job["group"] = group
        job["diagnostics"] = {field: limit}
        job.update(extra or {})
        job.pop("case_id")
        identity = hashlib.sha256(json.dumps(job, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
        job["case_id"] = f"{group}-r0-{identity}"
        if "sealed_dataset" in job:
            job["sealed_dataset"]["source_id"] += "-" + identity
            if field == "read_profile_limit":
                job["sealed_dataset"]["profiled"] = True
                job["sealed_dataset"]["instrumentation"] = "read_profile"
        jobs.append(job)

    base = jobs[first_stages:]
    def find(group, backend, operation, size, concurrency, tier, target=None):
        return next(j for j in base if j["repeat"] == 0 and j["group"] == group and j["backend"] == backend
                    and j["mix"][operation] == 100 and j["value_size"] == size
                    and j["concurrency"] == concurrency and j["durability_tier"] == tier
                    and (target is None or j.get("nvmeof_target") == target))

    for backend, operation, size, concurrency, tier, target in (
            ("tidehunter_verbs_native", "put", 1024, 1, "synchronous", None),
            ("tidehunter_verbs_native", "put", 65536, 1, "synchronous", None),
            ("tidehunter_verbs_native", "put", 1024, 16, "synchronous", None),
            ("tidehunter_verbs_native", "put_batch", 8192, 1, "synchronous", None),
            ("tidehunter_verbs_native", "delete", 1024, 1, "synchronous", None),
            ("tidehunter_verbs_native", "put", 1024, 1, "native", None),
            ("tidehunter_tcp", "put", 1024, 1, "synchronous", None),
            ("tidehunter_tcp", "put", 1024, 16, "synchronous", None),
            ("tidehunter_nvme_rdma", "put", 1024, 1, "synchronous", "kernel"),
            ("tidehunter_nvme_rdma", "put", 1024, 1, "synchronous", "spdk")):
        diagnostic_copy(find("G12H" if target else "G12W", backend, operation, size, concurrency, tier, target),
                        "write_timeline_limit", 4096)
    for backend, operation, size, concurrency, tier in (
            ("native", "put", 1024, 1, "value_wal_barrier"), ("native", "put", 1024, 1, "page_cache"),
            ("native", "put", 65536, 1, "value_wal_barrier"), ("native", "put", 1024, 16, "value_wal_barrier"),
            ("native", "delete", 1024, 1, "value_wal_barrier"),
            ("tidehunter_verbs_native", "put", 1024, 1, "synchronous"),
            ("tidehunter_verbs_native", "put", 1024, 16, "synchronous")):
        diagnostic_copy(find("G12N" if backend == "native" else "G12W", backend, operation, size, concurrency, tier),
                        "write_profile_limit", 4096)
    for backend, target, states in (("tidehunter_verbs_native", None, ("memory", "cold")),
                                     ("tidehunter_tcp", None, ("memory", "cold")),
                                     ("tidehunter_nvme_rdma", "kernel", ("memory", "cold")),
                                     ("tidehunter_nvme_rdma", "spdk", ("cold",))):
        for state in states:
            for size in (1024, 65536):
                cold = state == "cold"
                extra = {"diagnostics": {"read_profile_limit": 4096}, "measurement_operations": 4096}
                if cold:
                    extra.update(keys=65536, key_order="random_with_replacement",
                                 measurement_operations=2048 if size == 1024 else 4096)
                    if target == "kernel":
                        extra["target_io"] = "direct"
                add("G12D", "media_read", 0, backend, target, size=size,
                    tier="first_touch" if cold else "repeat_read",
                    media={"expected": state, "history": "normal",
                           "wal_eviction": "selective" if cold and target is None else "none"},
                    extra=extra)
                jobs[-1]["sealed_dataset"]["profiled"] = True
                jobs[-1]["sealed_dataset"]["instrumentation"] = "read_profile"
    for backend, target, states, concurrency in (
            ("tidehunter_verbs_native", None, ("memory", "cold"), 16),
            ("tidehunter_tcp", None, ("memory", "cold"), 16),
            ("tidehunter_nvme_rdma", "kernel", ("cold",), 16),
            ("tidehunter_nvme_rdma", "spdk", ("cold",), 16),
            ("tidehunter_verbs", None, ("memory", "cold"), 1)):
        for state in states:
            cold = state == "cold"
            extra = {"diagnostics": {"read_profile_limit": 4096}, "measurement_operations": 4096}
            if cold:
                extra.update(keys=65536, key_order="random_with_replacement", measurement_operations=2048)
                if target == "kernel":
                    extra["target_io"] = "direct"
            add("G12D", "media_read", 0, backend, target, size=1024, concurrency=concurrency,
                tier="first_touch" if cold else "repeat_read",
                media={"expected": state, "history": "normal",
                       "wal_eviction": "selective" if cold and target is None else "none"},
                extra=extra)
            jobs[-1]["sealed_dataset"]["profiled"] = True
            jobs[-1]["sealed_dataset"]["instrumentation"] = "read_profile"
    diagnostic_copy(find("G12W", "tidehunter_verbs", "put", 1024, 1, "synchronous"),
                    "write_timeline_limit", 4096)
    for operation, size in (("put", 65536), ("put_batch", 8192), ("delete", 1024)):
        write_case("G12D", "remote_put", 0, "tidehunter_tcp", operation, size, 1, "synchronous",
                   extra={"diagnostics": {"write_timeline_limit": 4096}})
    for policy in ("overwrite", "new"):
        for field, limit in (("write_timeline_limit", 65536), ("write_profile_limit", 4096)):
            source = find("G12W", "tidehunter_verbs_native", "put", 1024, 16, "synchronous")
            extra = {"measurement_operations": 200000}
            if policy == "new":
                extra["put_key_policy"] = "new"
            diagnostic_copy(source, field, limit, "G12S", extra)
    stages = jobs[first_stages:]
    canaries = [
        find("G12N", "native", "put", 1024, 1, "value_wal_barrier"),
        next(j for j in stages if j["case_kind"] == "native_media_read" and j["repeat"] == 0
             and j["value_size"] == 1024 and j["concurrency"] == 1),
        find("G12W", "tidehunter_verbs_native", "put", 1024, 1, "synchronous"),
        find("G12W", "tidehunter_verbs_native", "put_batch", 8192, 1, "synchronous"),
        find("G12W", "tidehunter_verbs_native", "delete", 1024, 1, "synchronous"),
        find("G12H", "tidehunter_nvme_rdma", "put", 1024, 1, "synchronous", "kernel"),
        find("G12H", "tidehunter_nvme_rdma", "put", 1024, 1, "synchronous", "spdk"),
        find("G12K", "tidehunter_verbs_native", "put", 1024, 1, "synchronous"),
        next(j for j in stages if j["group"] == "G12D" and "write_timeline_limit" in j["diagnostics"]),
        next(j for j in stages if j["group"] == "G12D" and "read_profile_limit" in j["diagnostics"]),
        next(j for j in stages if j["group"] == "G12D" and "write_profile_limit" in j["diagnostics"]),
    ]
    chosen = {j["case_id"] for j in canaries}
    remaining = [j for j in stages if j["case_id"] not in chosen]
    remaining.sort(key=lambda j: (j["repeat"], j["group"]))
    jobs[first_stages:] = canaries + remaining
    first_ssd_write = len(jobs)
    for repeat in range(2):
        for mode in ("buffered", "fdatasync", "direct"):
            for block in (1024, 8192, 65536):
                add("G11W", "ssd_write_floor", repeat, "tidehunter_verbs_native", size=65536,
                    extra={"ssd_write": {"mode": mode, "block_bytes": block, "region_bytes": 536870912,
                                         "runtime_seconds": 6, "queue_depth": 1, "location": "local_file",
                                         "preheat": True}})
    ssd_write = jobs[first_ssd_write:]
    canary = next(job for job in ssd_write if job["repeat"] == 0
                  and job["ssd_write"]["mode"] == "fdatasync" and job["ssd_write"]["block_bytes"] == 1024)
    jobs[first_ssd_write:] = [canary] + [job for job in ssd_write if job["case_id"] != canary["case_id"]]
    first_append = len(jobs)
    for block in (1024, 8192, 65536):
        for mode in ("fdatasync", "append_pwrite", "append_mmap", "append_mmap_fsync"):
            for repeat in (0, 1):
                for host in (("client", "owner0") if repeat == 0 else ("owner0", "client")):
                    spec = {"mode": mode, "block_bytes": block, "region_bytes": 536870912,
                            "queue_depth": 1, "location": "local_file", "host": host}
                    if mode == "fdatasync":
                        spec.update(runtime_seconds=6, preheat=True)
                    else:
                        spec.update(operations=4096, warmup_operations=512)
                    add("G13W", "ssd_write_floor", repeat, "tidehunter_verbs_native",
                        size=65536, extra={"ssd_write": spec})
    append = jobs[first_append:]
    canary = next(job for job in append if job["repeat"] == 0
                  and job["ssd_write"]["mode"] == "append_mmap"
                  and job["ssd_write"]["block_bytes"] == 1024
                  and job["ssd_write"]["host"] == "client")
    jobs[first_append:] = [canary] + [job for job in append if job["case_id"] != canary["case_id"]]
    first_fair = len(jobs)
    for repeat in (0, 1):
        for shards, group, concurrencies in ((1, "G14N", (1, 5, 16)),
                                             (5, "G14S", (5, 16))):
            for durability in ("page_cache", "value_wal_barrier", "value_wal_group_barrier"):
                for size in (1024, 8192, 65536):
                    for concurrency in concurrencies:
                        write_case(group, "native_op", repeat, "native", "put", size,
                                   concurrency, durability,
                                   extra={"profile": "write-concurrent", "native_shards": shards})
        for owners, group in ((1, "G14O1"), (5, "G14O5")):
            for backend in ("tidehunter_verbs_native", "tidehunter_tcp"):
                for size in (1024, 8192, 65536):
                    for concurrency in (1, 5, 16):
                        write_case(group, "remote_put", repeat, backend, "put", size,
                                   concurrency, "synchronous",
                                   extra={"profile": "write-concurrent", "owner_count": owners})
    fair = jobs[first_fair:]
    for backend, concurrency, tier, owners, shards in (
            ("tidehunter_verbs_native", 1, "synchronous", 5, None),
            ("tidehunter_tcp", 1, "synchronous", 5, None),
            ("tidehunter_verbs_native", 16, "synchronous", 5, None),
            ("tidehunter_verbs_native", 16, "synchronous", 1, None),
            ("native", 16, "value_wal_barrier", None, 1),
            ("native", 1, "value_wal_barrier", None, 1)):
        source = next(j for j in fair if j["repeat"] == 0 and j["backend"] == backend
                      and j["value_size"] == 65536 and j["concurrency"] == concurrency
                      and j["durability_tier"] == tier and j.get("owner_count") == owners
                      and j.get("native_shards") == shards)
        diagnostic_copy(source, "write_profile_limit", 4096, "G14P")
    fair = jobs[first_fair:]
    canary = next(j for j in fair if j["group"] == "G14N" and j["repeat"] == 0
                  and j["value_size"] == 1024 and j["concurrency"] == 1
                  and j["native_durability"] == "value_wal_group_barrier")
    jobs[first_fair:] = [canary] + [j for j in fair if j["case_id"] != canary["case_id"]]
    # New read cases follow the frozen blocks; existing job records stay unchanged.
    for source in floors:
        if source["group"] == "G11S":
            add("G15F", "ssd_floor", source["repeat"], "tidehunter_verbs_native",
                size=65536, extra={"ssd": {**source["ssd"], "host": "client"}})
    for group in ("G15C", "G15M"):
        cold = group == "G15C"
        for size in (1024, 8192, 65536):
            for concurrency in (1, 5, 16):
                for repeat in (0, 1):
                    arms = (("native", "tidehunter_tcp", "tidehunter_verbs_native") if cold
                            else ("native", "tidehunter_verbs_native", "tidehunter_tcp"))
                    for backend in arms if repeat == 0 else reversed(arms):
                        native = backend == "native"
                        kind = ("native_media_read" if native else "media_read") if cold else "sealed_knee"
                        extra = ({"keys": 65536, "key_order": "random_with_replacement",
                                  "measurement_operations": 2048 if size == 1024 else 4096}
                                 if cold else {})
                        if not native:
                            extra["owner_count"] = 1
                        add(group, kind, repeat, backend, size=size, concurrency=concurrency,
                            tier="first_touch" if cold else "repeat_read",
                            media={"expected": "cold" if cold else "memory", "history": "normal",
                                   "wal_eviction": "selective" if cold else "none"}, extra=extra)
    for group in ("G16P", "G16C"):
        cold = group == "G16C"
        arms = ((("tidehunter_tcp_trusted", None, "adaptive"),
                 ("tidehunter_verbs_native", None, "adaptive"),
                 ("tidehunter_nvme_rdma", "kernel", None),
                 ("tidehunter_nvme_rdma", "spdk", None),
                 ("tidehunter_nvme_tcp", "kernel", None)) if cold else
                (("tidehunter_tcp", None, "adaptive"),
                 ("tidehunter_tcp_trusted", None, "adaptive"),
                 ("tidehunter_tcp_trusted", None, "busy"),
                 ("tidehunter_verbs_native", None, "adaptive"),
                 ("tidehunter_verbs_native", None, "busy")))
        for size in (1024, 65536):
            for concurrency in (1, 16):
                for repeat in (0, 1):
                    for backend, target, polling in arms if repeat == 0 else reversed(arms):
                        extra = {"owner_count": 1}
                        if polling is not None:
                            extra["owner_polling"] = polling
                        if cold:
                            extra.update(keys=65536, key_order="random_with_replacement",
                                         measurement_operations=2048 if size == 1024 else 4096)
                        if target == "kernel":
                            extra["target_io"] = "direct"
                        add(group, "media_read" if cold else "sealed_knee", repeat, backend, target,
                            size=size, concurrency=concurrency, tier="first_touch" if cold else "repeat_read",
                            media={"expected": "cold" if cold else "memory", "history": "normal",
                                   "wal_eviction": "selective" if cold and target is None else "none"},
                            extra=extra)
    for size in (1024, 65536):
        for concurrency in (1, 16):
            for tier in ("synchronous", "native"):
                for repeat in (0, 1):
                    for mode in ("owner", "leased", "reserved"):
                        if mode == "reserved" and size != 1024:
                            continue
                        backend = "tidehunter_verbs_native" if mode == "owner" else "tidehunter_nvme_rdma"
                        extra = {"owner_count": 1, "owner_polling": "adaptive"}
                        if mode != "owner":
                            extra.update(nvmeof_route="live", nvmeof_target="kernel",
                                         target_io="buffered", live_write_mode=mode)
                        write_case("G17W", "remote_put", repeat, backend, "put", size, concurrency, tier, extra=extra)
                        if repeat == 0 and (mode == "leased" or (mode == "reserved" and concurrency == 1)):
                            diagnostic_copy(jobs[-1], "write_profile_limit", 4096, group="G17W")
    for size in (1024, 65536):
        for concurrency in (1, 16):
            for repeat in (0, 1):
                for inline in (True, False, None):
                    if inline is None and concurrency != 1:
                        continue
                    backend = "tidehunter_verbs_native" if inline is None else "tidehunter_nvme_rdma"
                    extra = {"owner_count": 1, "owner_polling": "adaptive", "cohort": "primary_rf1",
                             "warmup_operations": 4096, "warmup_key_sweep": True}
                    if inline is not None:
                        extra.update(nvmeof_route="live", nvmeof_target="kernel", target_io="buffered",
                                     live_write_mode="leased", live_inline_reads=inline)
                    add("G17R", "live_read", repeat, backend, size=size, concurrency=concurrency, extra=extra)
                    if size == 1024 and concurrency == 1 and inline is True:
                        add("G17R", "live_read", repeat, backend, size=size, concurrency=concurrency,
                            operation="get_many", extra=extra)
    first_path = len(jobs)
    native_settings = {"native_shards": 1, "native_wal_geometry": "native_default"}
    remote_settings = {"owner_count": 1, "owner_polling": "adaptive",
                       "owner_wal_geometry": "native_default", "history_persistence": "memory_only"}
    live_settings = {**remote_settings, "nvmeof_route": "live", "nvmeof_target": "kernel",
                     "target_io": "buffered", "live_write_mode": "leased", "live_export_mode": "per_owner",
                     "live_inline_reads": True, "live_io_queues": 1, "live_io_queue_size": 32, "live_poll_queues": 0}
    def settings(backend):
        return dict(native_settings if backend == "native" else
                    live_settings if backend == "tidehunter_nvme_rdma" else remote_settings)
    def put(group, backend, repeat, size, concurrency, tier, operation="put", **extra):
        write_case(group, "native_op" if backend == "native" else "remote_put", repeat,
                   backend, operation, size, concurrency, tier,
                   extra={**settings(backend), "device_counters": True, "batch_size": 32, **extra})
    def read(group, backend, repeat, size, concurrency, cold=False, operation="read", **extra):
        fields = {**settings(backend), "cohort": "primary_rf1", "keys": 65536,
                  "warmup_operations": 0 if cold else 65536,
                  "warmup_key_sweep": not cold, "batch_size": 32,
                  "measurement_operations": (2048 if size == 1024 else 4096) if cold else 8192,
                  "evict_mapped_wal_before_measurement": cold}
        if cold:
            fields["key_order"] = "random_with_replacement"
        fields.update(extra)
        add(group, ("native_media_read" if cold else "native_op") if backend == "native" else "live_read",
            repeat, backend, size=size, concurrency=concurrency, operation=operation,
            tier="first_touch" if cold else "repeat_read",
            media={"expected": "cold" if cold else "memory", "history": "normal",
                   "wal_eviction": "selective" if cold else "none"}, extra=fields)
    for tool in ("ib_send_lat", "ib_read_lat"):
        for mode in ("polled", "event"):
            add("G18F", "net_floor", 0, "tidehunter_verbs_native", size=65536,
                extra={**remote_settings, "net": {"tool": tool, "mode": mode, "interface": "rdma0",
                       "provider": None, "endpoint": None, "sizes": [8, 1024, 4096, 65536],
                       "iterations": 5000, "warmup": 0}})
    for block in (4096, 65536):
        add("G18F", "ssd_floor", 0, "tidehunter_verbs_native", size=65536,
            extra={**remote_settings, "ssd": {"host": "owner0", "block_bytes": block,
                   "region_bytes": 2147483648, "state": "fresh", "age_seconds": 0,
                   "runtime_seconds": 6, "queue_depth": 1, "pattern": "randread", "direct": True}})
    for mode in ("append_pwrite", "fdatasync"):
        for block in (1024, 65536):
            floor = {"mode": mode, "block_bytes": block, "region_bytes": 536870912,
                     "queue_depth": 1, "location": "local_file", "host": "owner0"}
            floor.update({"runtime_seconds": 6, "preheat": True} if mode == "fdatasync" else
                         {"operations": 4096, "warmup_operations": 512})
            add("G18F", "ssd_write_floor", 0, "tidehunter_verbs_native", size=65536,
                extra={**remote_settings, "ssd_write": floor})
    for repeat in (0, 1):
        for size in (1024, 65536):
            for concurrency in (1, 16):
                for tier in ("page_cache", "value_wal_barrier"):
                    put("G18N", "native", repeat, size, concurrency, tier)
                for cold in (False, True):
                    read("G18N", "native", repeat, size, concurrency, cold)
                for tier in ("native", "synchronous"):
                    for backend in ("tidehunter_verbs_native", "tidehunter_nvme_rdma"):
                        put("G18W", backend, repeat, size, concurrency, tier)
            for inline, polls in ((True, 0), (False, 0), (True, 1)):
                flags = {"require_media_qualified": True} if repeat == 0 and size == 65536 and inline and polls == 0 else {}
                read("G18R", "tidehunter_nvme_rdma", repeat, size, 1, True,
                     live_inline_reads=inline, live_poll_queues=polls, **flags)
            for lanes, queues, depth in ((4, 1, 32), (4, 4, 128), (8, 4, 128)):
                read("G18R", "tidehunter_nvme_rdma", repeat, size, 16, True,
                     read_lanes=lanes, live_io_queues=queues, live_io_queue_size=depth)
            for polls in (0, 1):
                read("G18M", "tidehunter_nvme_rdma", repeat, size, 1, live_poll_queues=polls)
            for queues, depth in ((1, 32), (4, 128)):
                read("G18M", "tidehunter_nvme_rdma", repeat, size, 16,
                     read_lanes=4, live_io_queues=queues, live_io_queue_size=depth)
        read("G18M", "tidehunter_nvme_rdma", repeat, 1024, 1, keys=65536,
             warmup_operations=65536, coordinator_cache_bytes=8388608,
             distribution={"zipf": {"exponent": 0.99}})
        for concurrency in (1, 16):
            for queues, depth in ((1, 32), (4, 128)):
                read("G18X", "tidehunter_nvme_rdma", repeat, 65536, concurrency, True,
                     owner_wal_geometry="sui_harness", read_lanes=1 if concurrency == 1 else 4,
                     live_io_queues=queues, live_io_queue_size=depth)
    source = next(j for j in jobs[first_path:] if j["group"] == "G18W" and j["repeat"] == 0
                  and j["backend"] == "tidehunter_nvme_rdma" and j["value_size"] == 1024
                  and j["concurrency"] == 1 and j["durability_tier"] == "native")
    for field in ("write_profile_limit", "write_timeline_limit"):
        diagnostic_copy(source, field, 4096, "G18W", {"owner_history_diagnostics": True})
    put("G18W", "tidehunter_nvme_rdma", 0, 1024, 16, "native", measurement_operations=200000)
    for size in (1024, 8192):
        for concurrency in (1, 16):
            for native_tier, remote_tier in (("page_cache", "native"), ("value_wal_barrier", "synchronous")):
                for backend in ("native", "tidehunter_verbs_native", "tidehunter_nvme_rdma"):
                    put("G18B", backend, 0, size, concurrency,
                        native_tier if backend == "native" else remote_tier, "put_batch")
        for backend in ("native", "tidehunter_nvme_rdma"):
            read("G18B", backend, 0, size, 1, operation="get_many")
    for backend in ("native", "tidehunter_nvme_rdma"):
        read("G18B", backend, 0, 8192, 1, True, "get_many", measurement_operations=256)
    path = jobs[first_path:]
    def pick(group, backend, operation="read", size=1024, concurrency=1, **fields):
        return next(j for j in path if j["group"] == group and j["backend"] == backend
                    and j["repeat"] == 0 and j["value_size"] == size and j["concurrency"] == concurrency
                    and j["mix"][operation] == 100 and not j.get("diagnostics")
                    and all(j.get(k) == v for k, v in fields.items()))
    canaries = [pick("G18N", "native", "put", durability_tier="page_cache"),
                pick("G18W", "tidehunter_verbs_native", "put", durability_tier="native"),
                pick("G18W", "tidehunter_nvme_rdma", "put", durability_tier="native"),
                pick("G18N", "native", drop_os_caches=False),
                pick("G18M", "tidehunter_nvme_rdma", live_poll_queues=0, coordinator_cache_bytes=0),
                pick("G18R", "tidehunter_nvme_rdma", live_inline_reads=True, live_poll_queues=0),
                pick("G18R", "tidehunter_nvme_rdma", size=65536, require_media_qualified=True),
                pick("G18R", "tidehunter_nvme_rdma", live_poll_queues=1),
                pick("G18R", "tidehunter_nvme_rdma", concurrency=16, read_lanes=4, live_io_queues=4),
                pick("G18B", "tidehunter_nvme_rdma", "put_batch", durability_tier="native"),
                pick("G18X", "tidehunter_nvme_rdma", size=65536, live_io_queues=1),
                next(j for j in path if j.get("net", {}).get("tool") == "ib_read_lat"
                     and j["net"]["mode"] == "polled")]
    chosen = {j["case_id"] for j in canaries}
    order = ("G18F", "G18N", "G18W", "G18M", "G18R", "G18B", "G18X")
    jobs[first_path:] = canaries + sorted((j for j in path if j["case_id"] not in chosen),
                                         key=lambda j: order.index(j["group"]))
    return jobs


def _clone_source(pool, group, backend, size=1024, concurrency=1, repeat=0, operation="put", **fields):
    matches = [job for job in pool if job["group"] == group and job["backend"] == backend
               and job["value_size"] == size and job["concurrency"] == concurrency
               and job["repeat"] == repeat and job["mix"][operation] == 100
               and all(job.get(key) == value for key, value in fields.items())]
    if len(matches) != 1:
        raise ValueError("NVMe-oF source cell is not unique: " + str(
            (group, backend, size, concurrency, repeat, operation, fields)))
    return matches[0]


def _clone_job(result, selection, item, group, **overrides):
    job = copy.deepcopy(item)
    job.update(selection=selection, group=group)
    if job["backend"] != "native":
        job["history_persistence"] = "wal_replay"
    if job.get("nvmeof_route") == "live":
        job.update(live_lease_ahead=True, live_polled_writes=False)
    job.update(overrides)
    identity = hashlib.sha256(json.dumps(
        {key: value for key, value in job.items() if key != "case_id"},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
    job["case_id"] = f"{group}-r{job['repeat']}-{identity}"
    result.append(job)
    return job


def _nvmeof_path_fix(path, fabric):
    """Clone reviewed cells without changing their dataset identities."""
    result = []
    def put(backend="tidehunter_nvme_rdma", size=1024, concurrency=1, repeat=0, tier="native"):
        return _clone_source(path, "G18W", backend, size, concurrency, repeat,
                      durability_tier=tier, diagnostics=None, measurement_operations=4096)
    leased64 = [_clone_job(result, "nvmeof_path_fix", put(size=65536, concurrency=16, repeat=r), "G19W") for r in (0, 1)]
    for r in (0, 1):
        _clone_job(result, "nvmeof_path_fix", put(size=65536, concurrency=16, repeat=r), "G19W", live_lease_ahead=False)
    _clone_job(result, "nvmeof_path_fix", put(size=65536, concurrency=16), "G19W", history_persistence="memory_only")
    for concurrency in (1, 16):
        for r in (0, 1):
            _clone_job(result, "nvmeof_path_fix", put(concurrency=concurrency, repeat=r), "G19W")
    _clone_job(result, "nvmeof_path_fix", put(tier="synchronous"), "G19W")
    _clone_job(result, "nvmeof_path_fix", put(size=65536), "G19W")
    owner = [_clone_job(result, "nvmeof_path_fix", put("tidehunter_verbs_native", repeat=r), "G19W") for r in (0, 1)]
    _clone_job(result, "nvmeof_path_fix", put("tidehunter_verbs_native", size=65536, concurrency=16), "G19W")
    _clone_job(result, "nvmeof_path_fix", put("tidehunter_verbs_native", tier="synchronous"), "G19W")
    sustained = _clone_source(path, "G18W", "tidehunter_nvme_rdma", concurrency=16,
                       measurement_operations=200000)
    sustained_job = _clone_job(result, "nvmeof_path_fix", sustained, "G19W")
    _clone_job(result, "nvmeof_path_fix", sustained, "G19W", history_persistence="on_disk")
    timeline = next(job for job in path if job["group"] == "G18W"
                    and "write_timeline_limit" in job.get("diagnostics", {}))
    _clone_job(result, "nvmeof_path_fix", timeline, "G19W")
    _clone_job(result, "nvmeof_path_fix", put(size=65536, concurrency=16), "G19W",
          diagnostics=copy.deepcopy(timeline["diagnostics"]),
          owner_history_diagnostics=timeline["owner_history_diagnostics"])
    polled = _clone_job(result, "nvmeof_path_fix", put(), "G19W", live_poll_queues=1, live_polled_writes=True)
    _clone_job(result, "nvmeof_path_fix", put(size=65536, concurrency=16), "G19W", live_poll_queues=1, live_polled_writes=True)
    cold = []
    for size, concurrency in ((1024, 1), (65536, 1), (1024, 16)):
        cold.append(_clone_job(result, "nvmeof_path_fix", _clone_source(path, "G18R", "tidehunter_nvme_rdma", size, concurrency,
                                 operation="read", live_inline_reads=True, live_poll_queues=0,
                                 live_io_queues=1, live_io_queue_size=32,
                                 read_lanes=4 if concurrency == 16 else 1), "G19R"))
    warm = []
    for polls in (0, 1):
        warm.append(_clone_job(result, "nvmeof_path_fix", _clone_source(path, "G18M", "tidehunter_nvme_rdma", operation="read",
                                 live_poll_queues=polls, coordinator_cache_bytes=0), "G19R"))
    owner_cold = _clone_job(result, "nvmeof_path_fix", _clone_source(fabric, "G16C", "tidehunter_verbs_native", operation="read",
                              owner_polling="adaptive"), "G19R", owner_wal_geometry="native_default")
    _clone_job(result, "nvmeof_path_fix", _clone_source(fabric, "G16P", "tidehunter_verbs_native", operation="read",
                 owner_polling="adaptive"), "G19R", owner_wal_geometry="native_default")
    native = []
    for size, concurrency, repeats in ((1024, 1, (0, 1)), (1024, 16, (0,)),
                                       (65536, 16, (0, 1)), (65536, 1, (0,))):
        for r in repeats:
            native.append(_clone_job(result, "nvmeof_path_fix", _clone_source(path, "G18N", "native", size, concurrency, r,
                                       native_durability="page_cache"), "G19N"))
    for expected in ("memory", "cold"):
        _clone_job(result, "nvmeof_path_fix", _clone_source(path, "G18N", "native", operation="read",
                     media={"expected": expected, "history": "normal",
                            "wal_eviction": "selective" if expected == "cold" else "none"}), "G19N")
    _clone_job(result, "nvmeof_path_fix", _clone_source(path, "G18X", "tidehunter_nvme_rdma", 65536, operation="read",
                 live_io_queues=1, live_io_queue_size=32), "G19X")
    priority = [native[0], leased64[0], owner[0], cold[1], owner_cold, warm[0], polled, sustained_job]
    ids = {job["case_id"] for job in priority}
    result = priority + [job for job in result if job["case_id"] not in ids]
    if (len(result) != 37 or len({job["case_id"] for job in result}) != 37
            or {group: sum(job["group"] == group for job in result)
                for group in _FOCUSED_GROUP_COUNTS["nvmeof_path_fix"]}
            != _FOCUSED_GROUP_COUNTS["nvmeof_path_fix"]):
        raise ValueError("NVMe-oF fix matrix differs from its reviewed counts")
    return result


def _nvmeof_contracts(path):
    """Compare the device-synced and page-cache contracts on one fleet."""
    result = []
    leased = {}
    for tier in ("synchronous", "native"):
        leased[tier] = []
        for size in (1024, 65536):
            for concurrency in (1, 16):
                item = _clone_source(path, "G18W", "tidehunter_nvme_rdma", size, concurrency,
                                     durability_tier=tier, diagnostics=None, measurement_operations=4096)
                leased[tier].append(_clone_job(result, "nvmeof_contracts", item, "G21W"))
    sustained = _clone_source(path, "G18W", "tidehunter_nvme_rdma", 1024, 16,
                              measurement_operations=200000)
    for tier in ("synchronous", "native"):
        _clone_job(result, "nvmeof_contracts", sustained, "G21W", durability_tier=tier)
    timeline = _clone_source(path, "G18W", "tidehunter_nvme_rdma",
                             diagnostics={"write_timeline_limit": 4096})
    for tier in ("synchronous", "native"):
        _clone_job(result, "nvmeof_contracts", leased[tier][-1], "G21W",
                   diagnostics=copy.deepcopy(timeline["diagnostics"]),
                   owner_history_diagnostics=timeline["owner_history_diagnostics"])
    for size in (1024, 65536):
        for concurrency in (1, 16):
            item = _clone_source(path, "G18W", "tidehunter_verbs_native", size, concurrency,
                                 durability_tier="synchronous", diagnostics=None,
                                 measurement_operations=4096)
            _clone_job(result, "nvmeof_contracts", item, "G21W")
    cold = []
    for group in ("G18M", "G18R"):
        for size in (1024, 65536):
            for concurrency in (1, 16):
                item = _clone_source(path, group, "tidehunter_nvme_rdma", size, concurrency,
                                     operation="read", live_poll_queues=0, coordinator_cache_bytes=0,
                                     read_lanes=4 if concurrency == 16 else 1, live_inline_reads=True,
                                     live_io_queues=1, live_io_queue_size=32)
                job = _clone_job(result, "nvmeof_contracts", item, "G21R")
                if group == "G18R":
                    cold.append(job)
    native = {}
    for tier in ("value_wal_barrier", "page_cache"):
        native[tier] = []
        for size in (1024, 65536):
            for concurrency in (1, 16):
                item = _clone_source(path, "G18N", "native", size, concurrency,
                                     native_durability=tier)
                native[tier].append(_clone_job(result, "nvmeof_contracts", item, "G21N"))
    for concurrency in (1, 16):
        item = _clone_source(path, "G18N", "native", 1024, concurrency, operation="read",
                             media={"expected": "memory", "history": "normal", "wal_eviction": "none"})
        _clone_job(result, "nvmeof_contracts", item, "G21N")
    priority = [native["page_cache"][0], leased["synchronous"][0], cold[2]]
    ids = {job["case_id"] for job in priority}
    result = priority + [job for job in result if job["case_id"] not in ids]
    if (len(result) != FOCUSED_COUNTS["nvmeof_contracts"]
            or len({job["case_id"] for job in result}) != len(result)
            or {group: sum(job["group"] == group for job in result)
                for group in _FOCUSED_GROUP_COUNTS["nvmeof_contracts"]}
            != _FOCUSED_GROUP_COUNTS["nvmeof_contracts"]):
        raise ValueError("NVMe-oF contracts matrix differs from its reviewed counts")
    return result


def _nvmeof_contracts_crossing(path):
    """Retain the contracts matrix and append one instrumented file crossing."""
    result = _nvmeof_contracts(path)
    source = _clone_source(result, "G21W", "tidehunter_nvme_rdma", 65536, 16,
                           durability_tier="synchronous", diagnostics=None)
    _clone_job(result, "nvmeof_contracts_crossing", source, "G22W",
               owner_wal_file_bytes=268435456, measurement_operations=16384,
               diagnostics={"write_timeline_limit": 16384}, owner_history_diagnostics=True)
    return result


def _nvmeof_contracts_crossing2(path):
    """Retain the synced crossing and append its page-cache contract pair."""
    result = _nvmeof_contracts_crossing(path)
    _clone_job(result, "nvmeof_contracts_crossing2", result[-1], "G22W",
               durability_tier="native")
    return result


def _nvmeof_reads_geometry(path):
    """Vary bounded read resources without changing dataset identities."""
    sources = _nvmeof_contracts(path)
    result = []
    selection = "nvmeof_reads_geometry"
    def read(group, size=1024, cold=False, concurrency=16, lanes=None, **extra):
        source = _clone_source(sources, "G21R", "tidehunter_nvme_rdma", size,
                               1 if concurrency == 1 else 16, operation="read",
                               drop_os_caches=cold)
        return _clone_job(result, selection, source, group, concurrency=concurrency,
                          read_lanes=lanes if lanes is not None else (1 if concurrency == 1 else 4),
                          live_max_inflight_requests=128, **extra)
    for size in (1024, 65536):
        for cold in (False, True):
            for concurrency in (1, 16, 64):
                for repeat in (0, 1):
                    read("G24R", size, cold, concurrency, repeat=repeat)
    for queues in (1, 4, 16):
        for size in (1024, 65536):
            for cold in (False, True):
                for concurrency in (16, 64):
                    read("G24Q", size, cold, concurrency,
                         live_io_queues=queues, live_io_queue_size=128)
    for lanes in (1, 8):
        read("G24L", concurrency=64, lanes=lanes, live_io_queues=4, live_io_queue_size=128)
    for concurrency in (16, 64):
        read("G24P", concurrency=concurrency, live_io_queues=4,
             live_io_queue_size=128, live_poll_queues=1)
    for queues, depth in ((1, 32), (4, 128)):
        read("G24S", live_io_queues=queues, live_io_queue_size=depth,
             diagnostics={"read_profile_limit": 4096})
    for size in (1024, 65536):
        read("G24M", size, live_io_queues=4, live_io_queue_size=128,
             mix={"read": 50, "put": 50, "delete": 0, "missing_read": 0, "put_batch": 0, "get_many": 0},
             durability_tier="synchronous", live_leased_write_window=1, warmup_key_sweep=False)
        for cache, distribution in ((8 * 1024**2, "uniform"),
                                    (8 * 1024**2, {"zipf": {"exponent": 0.99}}),
                                    (128 * 1024**2 if size == 1024 else 6 * 1024**3, "uniform")):
            read("G24C", size, coordinator_cache_bytes=cache, distribution=distribution,
                 **({"coordinator_read_memory_allowance_bytes": 512 * 1024**2 + cache}
                    if cache >= size * 65536 else {}))
    for concurrency in (16, 64):
        source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", 65536, 16,
                               durability_tier="synchronous", diagnostics=None)
        for queues, depth in ((1, 32), (4, 128)):
            for window in (1, None):
                _clone_job(result, selection, source, "G24W", concurrency=concurrency,
                           read_lanes=4, live_io_queues=queues, live_io_queue_size=depth,
                           live_max_inflight_requests=128,
                           **({"live_leased_write_window": window} if window is not None else {}))
        for size in (1024, 65536):
            source = _clone_source(path, "G18N", "native", size, 16, operation="read",
                                   drop_os_caches=False)
            _clone_job(result, selection, source, "G24N", concurrency=concurrency,
                       live_max_inflight_requests=128)
    return result


def _nvmeof_reads_geometry_rest(path):
    """Rerun only the cells that did not complete in the original read fleet."""
    result = []
    for source in _nvmeof_reads_geometry(path):
        if source["group"] in ("G24C", "G24M", "G24W", "G24N"):
            _clone_job(result, "nvmeof_reads_geometry_rest", source, source["group"])
    return result


def _nvmeof_reads_geometry_rest2(path):
    """Retain the unfinished read/cache and write controls without mixed reads."""
    result = []
    for source in _nvmeof_reads_geometry_rest(path):
        if source["group"] in ("G24C", "G24W", "G24N"):
            _clone_job(result, "nvmeof_reads_geometry_rest2", source, source["group"])
    return result


def _nvmeof_parallel_writes(path):
    """Clone reviewed contract cells for the bounded leased-write window sweep."""
    sources = _nvmeof_contracts_crossing2(path)
    result = []
    selection = "nvmeof_parallel_writes"
    for tier in ("synchronous", "native"):
        for size in (65536, 1024):
            source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", size, 16,
                                   durability_tier=tier, diagnostics=None,
                                   measurement_operations=4096)
            for window in (1, 4, None):
                for repeat in (0, 1):
                    _clone_job(result, selection, source, "G23W", repeat=repeat,
                               **({"live_leased_write_window": window} if window is not None else {}),
                               **({"diagnostics": {"write_timeline_limit": 4096}}
                                  if size == 65536 and repeat == 0 else {}))
        for concurrency in (1, 64):
            source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", 65536, 1,
                                   durability_tier=tier, diagnostics=None)
            for window in (1, None):
                _clone_job(result, selection, source, "G23W", concurrency=concurrency,
                           read_lanes=4 if concurrency == 64 else 1,
                           **({"live_leased_write_window": window} if window is not None else {}))
        source = _clone_source(sources, "G22W", "tidehunter_nvme_rdma", 65536, 16,
                               durability_tier=tier)
        for file_bytes, windows in ((268435456, (1, None)), (10737418240, (None,))):
            for window in windows:
                _clone_job(result, selection, source, "G23X",
                           owner_wal_file_bytes=file_bytes,
                           **({"live_leased_write_window": window} if window is not None else {}),
                           diagnostics={"write_timeline_limit": 16384},
                           owner_history_diagnostics=True, owner_trace=True)
    for tier in ("value_wal_barrier", "page_cache"):
        for size, concurrency in ((65536, 16), (1024, 16), (65536, 64)):
            source = _clone_source(sources, "G21N", "native", size, 16,
                                   native_durability=tier)
            _clone_job(result, selection, source, "G23N", concurrency=concurrency)
    return result


def _nvmeof_single_caller(path):
    """Measure single-caller completion wakeups with matched c16 controls."""
    sources = _nvmeof_contracts_crossing2(path)
    result = []
    selection = "nvmeof_single_caller"
    for tier in ("synchronous", "native"):
        for size in (65536, 1024):
            source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", size, 16,
                                   durability_tier=tier, diagnostics=None,
                                   measurement_operations=4096)
            for repeat in (0, 1):
                _clone_job(result, selection, source, "G28W", repeat=repeat,
                           concurrency=1, read_lanes=1,
                           diagnostics={"write_timeline_limit": 4096})
    for tier in ("synchronous", "native"):
        for size in (65536, 1024):
            source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", size, 16,
                                   durability_tier=tier, diagnostics=None,
                                   measurement_operations=4096)
            for window in (1, None):
                _clone_job(result, selection, source, "G28C",
                           **({"live_leased_write_window": window} if window is not None else {}),
                           **({"diagnostics": {"write_timeline_limit": 4096}} if size == 65536 else {}))
    for tier in ("value_wal_barrier", "page_cache"):
        for size in (65536, 1024):
            source = _clone_source(sources, "G21N", "native", size, 16,
                                   native_durability=tier)
            _clone_job(result, selection, source, "G28N", concurrency=1)
    return result


# Kept separate until the write-lane experiment authorizes an arm.
NVMEOF_F6_WRITE_LANES = []


def _nvmeof_f6(path):
    """Compare opt-in sync and CPU placement on the reviewed -16 fleet."""
    sources = _nvmeof_contracts_crossing2(path)
    result = []
    selection = "nvmeof_f6"
    for concurrency in (1, 16, 64):
        for size in (1024, 65536):
            source = _clone_source(sources, "G21W", "tidehunter_nvme_rdma", size, 16,
                                   durability_tier="synchronous", diagnostics=None,
                                   measurement_operations=4096)
            for sync in ("per_frame_fua", "group_flush"):
                for repeat in range(1 if concurrency == 1 else 2):
                    _clone_job(result, selection, source, "G29W", concurrency=concurrency,
                               read_lanes=1 if concurrency == 1 else 4, repeat=repeat,
                               live_leased_sync=sync, live_polled_writes=False,
                               **({"diagnostics": {"write_timeline_limit": 4096}}
                                  if size == 65536 and repeat == 0 else {}))
    for source in _nvmeof_parallel_writes(path):
        if source["group"] == "G23X" and source["durability_tier"] == "synchronous":
            for sync in ("per_frame_fua", "group_flush"):
                _clone_job(result, selection, source, "G29X", live_leased_sync=sync,
                           live_polled_writes=False)
    reads = _nvmeof_contracts(path)
    source = _clone_source(reads, "G21R", "tidehunter_nvme_rdma", 1024, 16,
                           operation="read", drop_os_caches=False)
    for concurrency in (16, 64):
        for pinned in (False, True):
            for repeat in (0, 1):
                _clone_job(result, selection, source, "G29R", concurrency=concurrency,
                           repeat=repeat, read_lanes=4, live_io_queues=4, live_io_queue_size=128,
                           live_pin_read_lanes=pinned, live_max_inflight_requests=128,
                           durability_tier="synchronous", live_polled_writes=False, owner_trace=True)
    for size in (1024, 65536):
        source = _clone_source(reads, "G21R", "tidehunter_nvme_rdma", size, 16,
                               operation="read", drop_os_caches=False)
        for queues, depth in ((1, 32), (4, 128)):
            _clone_job(result, selection, source, "G29G", read_lanes=4,
                       live_io_queues=queues, live_io_queue_size=depth,
                       live_max_inflight_requests=128, durability_tier="synchronous",
                       live_polled_writes=False)
    for source in _nvmeof_reads_geometry(path):
        if source["group"] == "G24M":
            for repeat in (0, 1):
                _clone_job(result, selection, source, "G24M", repeat=repeat,
                           warmup_key_sweep=False, live_polled_writes=False)
    return result


def _nvmeof_f6b(path):
    """Measure size-gated group flush and matched explicit read geometries."""
    sources = _nvmeof_f6(path)
    result = []
    selection = "nvmeof_f6b"
    arms = (("per_frame_fua", None), ("group_flush", None), ("group_flush", 32768))
    for group, concurrency, sizes, selected, repeats in (
            ("G31W", 16, (1024, 4096, 16384, 65536), arms, 2),
            ("G31H", 64, (1024, 65536), (arms[0], arms[2]), 2),
            ("G31S", 1, (65536,), (arms[0], arms[2]), 1)):
        for size in sizes:
            for sync, minimum in selected:
                source = _clone_source(sources, "G29W", "tidehunter_nvme_rdma",
                                       65536 if size == 65536 else 1024, concurrency,
                                       live_leased_sync=sync)
                for repeat in range(repeats):
                    item = {key: value for key, value in source.items() if key != "diagnostics"}
                    _clone_job(result, selection, item, group, value_size=size, repeat=repeat,
                               **({"live_leased_flush_min_frame_bytes": minimum}
                                  if minimum is not None else {}),
                               **({"diagnostics": {"write_timeline_limit": 4096}}
                                  if size == 65536 and repeat == 0 else {}))
    for window, selected, repeats in ((None, (arms[0], arms[2]), 2),
                                      (1, arms[:2], 1)):
        for sync, minimum in selected:
            source = _clone_source(sources, "G29X", "tidehunter_nvme_rdma", 65536, 16,
                                   live_leased_sync=sync, live_leased_write_window=window,
                                   owner_wal_file_bytes=268435456)
            for repeat in range(repeats):
                _clone_job(result, selection, source, "G31X", repeat=repeat,
                           **({"live_leased_flush_min_frame_bytes": minimum}
                              if minimum is not None else {}))
    for size in (1024, 65536):
        source = _clone_source(sources, "G29G", "tidehunter_nvme_rdma", size, 16,
                               operation="read", live_io_queues=1, live_io_queue_size=32)
        for queues, depth in ((1, 32), (4, 32), (4, 128)):
            for concurrency in (16, 64):
                for repeat in range(2 if concurrency == 16 else 1):
                    _clone_job(result, selection, source, "G31G", concurrency=concurrency,
                               repeat=repeat, live_io_queues=queues, live_io_queue_size=depth)
    return result


def _nvmeof_f8(path):
    """Compare engine-default size-gated flush against explicit per-frame FUA."""
    sources = _nvmeof_f6b(path)
    result = []
    for group, source_group, concurrency, sizes, repeats in (
            ("G32W", "G31W", 16, (1024, 4096, 16384, 65536), 2),
            ("G32H", "G31H", 64, (1024, 65536), 2),
            ("G32S", "G31S", 1, (1024, 65536), 1)):
        for size in sizes:
            source = _clone_source(sources, source_group, "tidehunter_nvme_rdma",
                                   65536 if source_group == "G31S" else size, concurrency,
                                   live_leased_sync="per_frame_fua")
            item = {key: value for key, value in source.items()
                    if key not in ("live_leased_sync", "live_leased_flush_min_frame_bytes", "diagnostics")}
            for explicit_fua in (False, True):
                for repeat in range(repeats):
                    _clone_job(result, "nvmeof_f8", item, group, value_size=size, repeat=repeat,
                               thread_cpu_sampling=True,
                               **({"live_leased_sync": "per_frame_fua"} if explicit_fua else {}),
                               **({"diagnostics": {"write_timeline_limit": 4096}}
                                  if size == 65536 and repeat == 0 else {}))
    native = _nvmeof_single_caller(path)
    for tier in ("page_cache", "value_wal_barrier"):
        for size in (1024, 65536):
            source = _clone_source(native, "G28N", "native", size, 1, native_durability=tier)
            for concurrency in (1, 16):
                _clone_job(result, "nvmeof_f8", source, "G32N", concurrency=concurrency,
                           thread_cpu_sampling=True, native_wal_sampling=True)
    return result


def _nvmeof_f7_highmem32(path):
    """Keep -16 workload controls with roughly ten-second measurement windows."""
    result = []
    selection = "nvmeof_f7_highmem32"
    reads = _nvmeof_reads_geometry(path)
    for group, size, concurrencies, operations in (("G30R", 65536, (16, 64), 600000),
                                                   ("G30S", 1024, (64,), 1500000)):
        for concurrency in concurrencies:
            source = _clone_source(reads, "G24Q", "tidehunter_nvme_rdma",
                                   size, concurrency, operation="read", drop_os_caches=False,
                                   live_io_queues=4, live_io_queue_size=128)
            for repeat in (0, 1):
                _clone_job(result, selection, source, group, repeat=repeat,
                           owner_trace=True, measurement_operations=operations,
                           warmup_operations=operations // 5)
    source = _clone_source(_nvmeof_parallel_writes(path), "G23W",
                           "tidehunter_nvme_rdma", 65536, 64,
                           durability_tier="native", live_leased_write_window=None)
    for repeat in (0, 1):
        _clone_job(result, selection, source, "G30W", repeat=repeat,
                   measurement_operations=350000, warmup_operations=70000)
    return result


def _nvmeof_first_put(sources):
    """Keep the reviewed first-put checks in their canary-first order."""
    result = []
    def pick(group, backend, size=1024, concurrency=1, repeat=0, **overrides):
        fields = dict(group=group, backend=backend, value_size=size, concurrency=concurrency,
                      repeat=repeat, diagnostics=None, measurement_operations=4096)
        if backend == "native":
            fields["native_durability"] = "page_cache"
        else:
            fields.update(history_persistence="wal_replay", live_write_mode="leased",
                          durability_tier="native", live_lease_ahead=True,
                          live_polled_writes=False, live_poll_queues=0)
        fields.update(overrides)
        matches = [source for source in sources
                   if all(source.get(key) == value for key, value in fields.items())]
        if len(matches) != 1:
            raise ValueError("NVMe-oF first-put source cell is not unique: " + str(fields))
        job = copy.deepcopy(matches[0])
        job.update(selection="nvmeof_first_put", group=group.replace("G19", "G20", 1))
        identity = hashlib.sha256(json.dumps(
            {key: value for key, value in job.items() if key != "case_id"},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
        job["case_id"] = f"{job['group']}-r{repeat}-{identity}"
        result.append(job)
    pick("G19N", "native")
    pick("G19W", "tidehunter_nvme_rdma")
    pick("G19R", "tidehunter_nvme_rdma", size=65536, require_media_qualified=True,
         durability_tier="synchronous")
    pick("G19W", "tidehunter_nvme_rdma", repeat=1)
    for repeat in (0, 1):
        pick("G19W", "tidehunter_nvme_rdma", concurrency=16, repeat=repeat)
    for repeat in (0, 1):
        pick("G19W", "tidehunter_nvme_rdma", size=65536, concurrency=16, repeat=repeat)
    for size, concurrency in ((1024, 1), (65536, 16)):
        pick("G19W", "tidehunter_nvme_rdma", size=size, concurrency=concurrency,
             diagnostics={"write_timeline_limit": 4096})
    pick("G19W", "tidehunter_nvme_rdma", concurrency=16, measurement_operations=200000)
    pick("G19N", "native", size=65536, concurrency=16)
    return result


VALKEY_ANCHOR_GEOMETRY = (4, 128)


def _valkey_rdma_reads():
    """One standalone RAM-read primary and matched live TideHunter anchors."""
    result = []
    sources = make_focused_matrix("nvmeof_contracts")
    for size in (1024, 65536):
        source = _clone_source(sources, "G21R", "tidehunter_nvme_rdma", size, 16,
                               operation="read", drop_os_caches=False)
        for concurrency in (1, 16, 64):
            for repeat in (0, 1):
                job = copy.deepcopy(source)
                for key in tuple(job):
                    if key.startswith("live_") or key.startswith("nvmeof_") or key in (
                            "sealed_dataset", "history_persistence", "owner_polling",
                            "owner_wal_geometry", "media", "durability_tier"):
                        job.pop(key)
                job.update(backend="valkey_rdma", owner_count=1, repeat=repeat,
                           concurrency=concurrency, read_lanes=1, target_io="not_applicable",
                           case_kind="standard", profile="read-concurrent",
                           cohort="primary_rf1", warmup_operations=65536,
                           warmup_key_sweep=True)
                job = {key: value for key, value in job.items() if value is not None or key == "offered_ops_per_second"}
                _clone_job(result, "valkey_rdma_reads", job, "G25R")
        mixed = copy.deepcopy(result[-1])
        mixed.update(concurrency=16, repeat=0, profile="point-mixed",
                     mix=mix(read=50, put=50), warmup_key_sweep=False)
        _clone_job(result, "valkey_rdma_reads", mixed, "G25M")
        for concurrency in (16, 64):
            for queues, depth in ((1, 32), VALKEY_ANCHOR_GEOMETRY):
                clean = {key: value for key, value in source.items()
                         if value is not None or key == "offered_ops_per_second"}
                _clone_job(result, "valkey_rdma_reads", clean, "G25A",
                           concurrency=concurrency, read_lanes=4,
                           live_io_queues=queues, live_io_queue_size=depth,
                           live_max_inflight_requests=128)
    return result


def make_focused_matrix(selection):
    if selection not in FOCUSED_SELECTIONS:
        raise ValueError("unknown focused selection")
    if selection == "valkey_rdma_reads":
        return _valkey_rdma_reads()
    jobs = _focused_jobs()
    ids = [job["case_id"] for job in jobs]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate focused case ID")
    if len(jobs) != sum(FOCUSED_COUNTS[name] for name in ("full", "device", "floors", "stages", "ssd_write", "append_floors", "fair_native", "fabric_ab_compact", "leased_ab_compact", "nvmeof_path_compact")) + 132:
        raise ValueError("focused matrix size differs from its reviewed counts")
    if selection == "recommended":
        return jobs[:FOCUSED_COUNTS["recommended"]]
    if selection == "full":
        return jobs[:FOCUSED_COUNTS["full"]]
    device_end = FOCUSED_COUNTS["full"] + FOCUSED_COUNTS["device"]
    if selection == "device":
        return jobs[FOCUSED_COUNTS["full"]:device_end]
    floors_end = device_end + FOCUSED_COUNTS["floors"]
    if selection == "floors":
        return jobs[device_end:floors_end]
    stages_end = floors_end + FOCUSED_COUNTS["stages"]
    if selection == "stages":
        return jobs[floors_end:stages_end]
    write_end = stages_end + FOCUSED_COUNTS["ssd_write"]
    append_end = write_end + FOCUSED_COUNTS["append_floors"]
    fair_end = append_end + FOCUSED_COUNTS["fair_native"]
    if selection == "append_floors":
        return jobs[write_end:append_end]
    if selection == "fair_native":
        return jobs[append_end:fair_end]
    if selection in ("parity", "parity_compact", "fair_five_owner"):
        parity = jobs[append_end:append_end + 1] + jobs[write_end:append_end] + jobs[append_end + 1:fair_end]
        if selection == "parity":
            return parity
        wanted = 1 if selection == "parity_compact" else 5
        return [job for job in parity if job_owner_hosts(job) == wanted]
    if selection == "remote_rw_compact":
        new = jobs[fair_end:fair_end + 132]
        reads = [job for job in new if job["group"] in ("G15C", "G15M")]
        canary_specs = (("G15C", "native"), ("G15C", "tidehunter_tcp"),
                        ("G15C", "tidehunter_verbs_native"), ("G15M", "native"),
                        ("G15M", "tidehunter_verbs_native"))
        canaries = [next(job for job in reads if job["group"] == group and job["backend"] == backend
                         and job["value_size"] == 1024 and job["concurrency"] == 1 and job["repeat"] == 0)
                    for group, backend in canary_specs]
        canaries.append(next(job for job in reads if job["group"] == "G15M"
                             and job["backend"] == "tidehunter_tcp" and job["value_size"] == 1024
                             and job["concurrency"] == 1 and job["repeat"] == 0))
        floor_jobs = (jobs[write_end:append_end] + jobs[device_end:floors_end]
                      + [job for job in new if job["group"] == "G15F"])
        write_jobs = [job for job in jobs[append_end:fair_end] if job["group"] in ("G14N", "G14O1")]
        arms = (("native", "page_cache"), ("native", "value_wal_barrier"),
                ("native", "value_wal_group_barrier"), ("tidehunter_tcp", "synchronous"),
                ("tidehunter_verbs_native", "synchronous"))
        def write_order(job):
            arm = arms.index((job["backend"], job["durability_tier"]))
            return job["value_size"], job["concurrency"], job["repeat"], arm if job["repeat"] == 0 else -arm
        write_jobs.sort(key=write_order)
        profiles = [job for job in jobs[append_end:fair_end]
                    if job["group"] == "G14P" and job_owner_hosts(job) == 1]
        # Keep the five proven read canaries first. One repeat-0 maximum per
        # flow follows: read arms, floor variants, then write arms.
        def variant(job):
            group = job["group"]
            if group == "G13W":
                return group, job["case_kind"], job["backend"], job["ssd_write"]["mode"], job["ssd_write"]["host"]
            if group == "G11N":
                net = job["net"]
                return (group, job["case_kind"], job["backend"],
                        *(net.get(k) for k in ("tool", "mode", "interface", "provider", "endpoint")))
            if group in ("G11S", "G15F"):
                return group, job["case_kind"], job["backend"], job["ssd"]["state"], job["ssd"].get("host", "owner0")
            return group, job["case_kind"], job["backend"], job.get("durability_tier")
        def weight(job):
            floor = job.get("ssd_write") or job.get("ssd") or {}
            return (job["value_size"], job["concurrency"], floor.get("block_bytes", 0),
                    floor.get("queue_depth", 0), floor.get("region_bytes", 0))
        selected = {}
        for job in reads + floor_jobs + write_jobs + profiles:
            if job["repeat"] != 0:
                continue
            key = variant(job)
            if key not in selected or weight(job) > weight(selected[key]):
                selected[key] = job
        chosen = {job["case_id"] for job in canaries}
        for job in selected.values():
            if job["case_id"] not in chosen:
                canaries.append(job)
                chosen.add(job["case_id"])
        result = canaries + [job for job in floor_jobs + write_jobs + profiles + reads
                             if job["case_id"] not in chosen]
        if len(result) != FOCUSED_COUNTS[selection] or parity_violations(result, complete=True):
            raise ValueError("remote read/write matrix violates reviewed parity")
        return result
    if selection == "fabric_ab_compact":
        fabric = jobs[fair_end + 132:fair_end + 132 + FOCUSED_COUNTS["fabric_ab_compact"]]
        warm = [job for job in fabric if job["group"] == "G16P"]
        cold = [job for job in fabric if job["group"] == "G16C"]
        canaries = [job for job in warm + cold if job["repeat"] == 0
                    and job["value_size"] == 1024 and job["concurrency"] == 1]
        canaries += [job for job in cold if job["repeat"] == 0
                     and job["value_size"] == 65536 and job["concurrency"] == 16
                     and job["backend"] in NVME_BACKENDS]
        canaries += [job for job in warm if job["repeat"] == 0
                     and job["value_size"] == 65536 and job["concurrency"] == 16
                     and job["owner_polling"] == "busy"]
        chosen = {job["case_id"] for job in canaries}
        remaining = (warm + [job for job in cold if job["backend"] not in NVME_BACKENDS]
                     + [job for job in cold if job["backend"] in NVME_BACKENDS])
        result = canaries + [job for job in remaining if job["case_id"] not in chosen]
        if len(result) != FOCUSED_COUNTS[selection] or fabric_parity_violations(result):
            raise ValueError("fabric matrix violates reviewed parity")
        return result
    if selection == "leased_ab_compact":
        start = fair_end + 132 + FOCUSED_COUNTS["fabric_ab_compact"]
        leased = jobs[start:start + FOCUSED_COUNTS["leased_ab_compact"]]
        canaries = [job for job in leased if job["repeat"] == 0 and job["value_size"] == 1024
                    and job["concurrency"] == 1 and not job.get("diagnostics")]
        chosen = {job["case_id"] for job in canaries}
        return canaries + [job for job in leased if job["case_id"] not in chosen]
    if selection in NVMEOF_PATH_SELECTIONS:
        path = jobs[-FOCUSED_COUNTS[NVMEOF_PATH_SELECTIONS[0]]:]
        if selection == NVMEOF_PATH_SELECTIONS[0]:
            return path
        if selection == "nvmeof_path_fix":
            return _nvmeof_path_fix(path, make_focused_matrix("fabric_ab_compact"))
        if selection == "nvmeof_first_put":
            return _nvmeof_first_put(make_focused_matrix("nvmeof_path_fix"))
        if selection == "nvmeof_reads_geometry_rest2":
            return _nvmeof_reads_geometry_rest2(path)
        if selection == "nvmeof_reads_geometry_rest":
            return _nvmeof_reads_geometry_rest(path)
        if selection == "nvmeof_reads_geometry":
            return _nvmeof_reads_geometry(path)
        if selection == "nvmeof_parallel_writes":
            return _nvmeof_parallel_writes(path)
        if selection == "nvmeof_single_caller":
            return _nvmeof_single_caller(path)
        if selection == "nvmeof_f6":
            return _nvmeof_f6(path)
        if selection == "nvmeof_f6b":
            return _nvmeof_f6b(path)
        if selection == "nvmeof_f8":
            return _nvmeof_f8(path)
        if selection == "nvmeof_f7_highmem32":
            return _nvmeof_f7_highmem32(path)
        if selection == "nvmeof_contracts_crossing2":
            return _nvmeof_contracts_crossing2(path)
        if selection == "nvmeof_contracts_crossing":
            return _nvmeof_contracts_crossing(path)
        if selection == "nvmeof_contracts":
            return _nvmeof_contracts(path)
        result = []
        for source in path:
            group = source["group"]
            c1 = source["concurrency"] == 1
            r0 = source["repeat"] == 0
            small = source["value_size"] == 1024
            tier = source["native_durability"] if source["backend"] == "native" else source["durability_tier"]
            page = tier in ("page_cache", "native")
            cold = source.get("media", {}).get("expected") == "cold"
            if group == "G18F":
                keep = source.get("net", {}).get("tool") == "ib_read_lat" or source["case_kind"] == "ssd_floor"
            elif group == "G18N":
                keep = ((page or c1) and (r0 or (page and c1)) if source["mix"]["put"]
                        else (r0 or c1) if cold else r0 and (c1 or small))
            elif group == "G18W":
                keep = (source.get("owner_history_diagnostics", False)
                        or source["measurement_operations"] == 200000
                        or (page or c1) and (r0 or (page and c1)))
            elif group == "G18R":
                keep = ((source["live_inline_reads"] and source["live_poll_queues"] == 0)
                        or (small and r0)) if c1 else r0
            elif group == "G18M":
                keep = r0 and (source["coordinator_cache_bytes"] or c1 or small)
            elif group == "G18B":
                keep = small and c1 and page if source["mix"]["put_batch"] else cold
            else:
                keep = r0 and ((c1 and source["live_io_queues"] == 1)
                               or (source["concurrency"] == 16 and source["live_io_queues"] == 4))
            if keep:
                job = copy.deepcopy(source)
                job["selection"] = selection
                # Dataset identities remain those of the reviewed compact source.
                identity_fields = {key: value for key, value in job.items() if key != "case_id"}
                identity = hashlib.sha256(json.dumps(identity_fields, sort_keys=True,
                                                     separators=(",", ":")).encode()).hexdigest()[:20]
                job["case_id"] = f"{group}-r{job['repeat']}-{identity}"
                result.append(job)
        counts = {group: sum(job["group"] == group for job in result)
                  for group in _FOCUSED_GROUP_COUNTS[selection]}
        if len(result) != FOCUSED_COUNTS[selection] or counts != _FOCUSED_GROUP_COUNTS[selection]:
            raise ValueError("NVMe-oF core matrix differs from its reviewed counts")
        priority = []
        for matches_case in (
                lambda job: (job["group"] == "G18N" and job["backend"] == "native"
                             and job["mix"]["put"] == 100 and job["value_size"] == 1024
                             and job["concurrency"] == 1 and job["native_durability"] == "page_cache"
                             and job["repeat"] == 0),
                lambda job: job.get("require_media_qualified", False),
                lambda job: (job["group"] == "G18B" and job["backend"] == "native"
                             and job["mix"]["get_many"] == 100
                             and job.get("media", {}).get("expected") == "cold")):
            matches = [job for job in result if matches_case(job)]
            if len(matches) != 1:
                raise ValueError("NVMe-oF core priority case is not unique")
            priority.append(matches[0])
        priority_ids = {job["case_id"] for job in priority}
        result = priority + [job for job in result if job["case_id"] not in priority_ids]
        return result
    return jobs[floors_end:stages_end] + jobs[device_end:floors_end] + jobs[stages_end:write_end]


def parity_violations(jobs, *, complete=True):
    """Check matched grid contracts, independent of execution order."""
    violations = []
    groups = {}
    for job in jobs:
        group = job.get("group")
        if group not in ("G15C", "G15M", "G14N", "G14O1"):
            continue
        category = "write" if group in ("G14N", "G14O1") else group
        key = (category, job.get("value_size"), job.get("concurrency"), job.get("repeat"))
        groups.setdefault(key, []).append(job)
    if complete:
        required = {(category, size, concurrency, repeat)
                    for category in ("G15C", "G15M", "write")
                    for size in (1024, 8192, 65536)
                    for concurrency in (1, 5, 16)
                    for repeat in (0, 1)}
        for key in sorted(required - groups.keys()):
            violations.append(f"{key}: missing grid cell")
    common = ("cohort", "value_size", "concurrency", "repeat", "keys", "key_order", "distribution",
              "measurement_operations", "warmup_operations", "mix", "coordinator_cache_bytes")
    for key, members in groups.items():
        category, size, concurrency, repeat = key
        if (type(size) is not int or size not in (1024, 8192, 65536)
                or type(concurrency) is not int or concurrency not in (1, 5, 16)
                or type(repeat) is not int or repeat not in (0, 1)):
            violations.append(f"{key}: invalid grid dimensions")
        if category == "write":
            expected = {("native", mode) for mode in ("page_cache", "value_wal_barrier", "value_wal_group_barrier")}
            expected |= {(backend, "synchronous") for backend in ("tidehunter_tcp", "tidehunter_verbs_native")}
            observed = [(job.get("backend"), job.get("durability_tier")) for job in members]
        else:
            expected = {"native", "tidehunter_tcp", "tidehunter_verbs_native"}
            observed = [job.get("backend") for job in members]
        if len(observed) != len(expected) or set(observed) != expected:
            violations.append(f"{key}: missing, duplicate, or unexpected arms")
        baseline = members[0]
        for job in members:
            if any(job.get(field) != baseline.get(field) for field in common):
                violations.append(f"{key}: workload fields differ")
            native = job.get("backend") == "native"
            if not native and job.get("owner_count") != 1:
                violations.append(f"{key}: owner count differs")
            if category == "write":
                if ((native and (job.get("group") != "G14N"
                                 or job.get("native_durability") != job.get("durability_tier")))
                        or (not native and (job.get("group") != "G14O1"
                                            or job.get("durability_tier") != "synchronous"))):
                    violations.append(f"{key}: write durability differs")
                continue
            if (job.get("native_durability") != "value_wal_barrier"
                    or job.get("durability_tier") != ("value_wal_barrier" if native else "synchronous")):
                violations.append(f"{key}: read durability differs")
            cold = category == "G15C"
            dataset = job.get("sealed_dataset") or {}
            base_dataset = baseline.get("sealed_dataset") or {}
            if ({field: value for field, value in dataset.items() if field != "source_id"} !=
                    {field: value for field, value in base_dataset.items() if field != "source_id"}):
                violations.append(f"{key}: dataset geometry differs")
            if (job.get("case_kind") != (("native_media_read" if native else "media_read") if cold else "sealed_knee")
                    or dataset.get("tier") != ("first_touch" if cold else "repeat_read")
                    or job.get("media") != {"expected": "cold" if cold else "memory", "history": "normal",
                                             "wal_eviction": "selective" if cold else "none"}
                    or job.get("coordinator_cache_bytes") != 0
                    or job.get("keys") != (65536 if cold else 4096)
                    or job.get("measurement_operations") != ((2048 if size == 1024 else 4096) if cold else 8192)
                    or job.get("warmup_operations") != (0 if cold else 4096)
                    or job.get("key_order") != ("random_with_replacement" if cold else None)
                    or job.get("distribution") != "uniform"
                    or job.get("drop_os_caches") is not cold
                    or bool(job.get("evict_mapped_wal_before_measurement")) != cold
                    or bool(job.get("warmup_key_sweep")) == cold):
                violations.append(f"{key}: read contract differs")
        if category != "write":
            owners = [job for job in members if job.get("backend") in ("tidehunter_tcp", "tidehunter_verbs_native")]
            if len(owners) == 2:
                def owner_shape(job):
                    shape = copy.deepcopy(job)
                    shape.pop("backend", None)
                    shape.pop("case_id", None)
                    shape.get("sealed_dataset", {}).pop("source_id", None)
                    return shape
                if owner_shape(owners[0]) != owner_shape(owners[1]):
                    violations.append(f"{key}: owner transports differ in non-transport fields")
    return violations


def fabric_parity_violations(jobs, *, complete=True):
    """Check matched fabric read cells; partial mode checks only present cells."""
    violations, cells = [], {}
    warm_arms = {("tidehunter_tcp", None, "adaptive"),
                 ("tidehunter_tcp_trusted", None, "adaptive"),
                 ("tidehunter_tcp_trusted", None, "busy"),
                 ("tidehunter_verbs_native", None, "adaptive"),
                 ("tidehunter_verbs_native", None, "busy")}
    cold_arms = {("tidehunter_tcp_trusted", None, "adaptive"),
                 ("tidehunter_verbs_native", None, "adaptive"),
                 ("tidehunter_nvme_rdma", "kernel", None),
                 ("tidehunter_nvme_rdma", "spdk", None),
                 ("tidehunter_nvme_tcp", "kernel", None)}
    for job in jobs:
        if job.get("group") in ("G16P", "G16C"):
            key = (job["group"], job.get("value_size"), job.get("concurrency"), job.get("repeat"))
            cells.setdefault(key, []).append(job)
    if complete:
        required = {(group, size, concurrency, repeat)
                    for group in ("G16P", "G16C") for size in (1024, 65536)
                    for concurrency in (1, 16) for repeat in (0, 1)}
        for key in sorted(required - cells.keys()):
            violations.append(f"{key}: missing cell")
    for key, members in cells.items():
        group, size, concurrency, repeat = key
        cold = group == "G16C"
        if (type(size) is not int or size not in (1024, 65536)
                or type(concurrency) is not int or concurrency not in (1, 16)
                or type(repeat) is not int or repeat not in (0, 1)):
            violations.append(f"{key}: invalid grid dimensions")
        arms = [(j.get("backend"), j.get("nvmeof_target"), j.get("owner_polling")) for j in members]
        expected = cold_arms if cold else warm_arms
        if len(arms) != len(expected) or set(arms) != expected:
            violations.append(f"{key}: missing, duplicate, or unexpected arms")
        def common_shape(job):
            shape = copy.deepcopy(job)
            # These fields describe the permitted transport/cache-specific arm differences.
            for field in ("case_id", "profile", "backend", "owner_polling", "nvmeof_route",
                          "nvmeof_target", "target_io", "evict_mapped_wal_before_measurement"):
                shape.pop(field, None)
            if isinstance(shape.get("media"), dict):
                shape["media"].pop("wal_eviction", None)
            if isinstance(shape.get("sealed_dataset"), dict):
                for field in ("source_id", "directness", "target_cache", "mapped_wal_eviction",
                              "cold_claim", "page_cache_policy"):
                    shape["sealed_dataset"].pop(field, None)
            return shape
        baseline = common_shape(members[0])
        for job in members:
            if common_shape(job) != baseline:
                violations.append(f"{key}: non-transport workload or dataset fields differ")
            backend = job.get("backend")
            nvme = backend in NVME_BACKENDS
            target = job.get("nvmeof_target")
            dataset = job.get("sealed_dataset")
            if not isinstance(dataset, dict):
                violations.append(f"{key}: dataset is missing")
                continue
            policy = ("drop_at_measurement_gate" if nvme else
                      "evict_mapped_wal_then_drop_at_measurement_gate") if cold else "not_dropped"
            directness = ("initiator_O_DIRECT_target_kernel_buffered_io_0" if target == "kernel" else
                          "initiator_O_DIRECT_target_file_O_DIRECT" if target == "spdk" else
                          "buffered_WAL_pagecache_or_device")
            cache = ("direct_kernel_buffered_io_0_no_target_pagecache" if target == "kernel" else
                     "direct_file_bdev_no_OS_pagecache" if target == "spdk" else "not_applicable")
            if (job.get("selection") != "fabric_ab_compact" or job.get("owner_count") != 1
                    or job.get("case_kind") != ("media_read" if cold else "sealed_knee")
                    or job.get("cohort") != "sealed_read" or job.get("durability_tier") != "synchronous"
                    or job.get("keys") != (65536 if cold else 4096)
                    or job.get("key_order") != ("random_with_replacement" if cold else None)
                    or job.get("distribution") != "uniform"
                    or job.get("measurement_operations") != ((2048 if size == 1024 else 4096) if cold else 8192)
                    or job.get("warmup_operations") != (0 if cold else 4096)
                    or job.get("read_lanes") != (4 if concurrency == 16 else 1)
                    or job.get("coordinator_cache_bytes") != 0
                    or job.get("mix") != mix(read=100)
                    or job.get("drop_os_caches") is not cold
                    or bool(job.get("warmup_key_sweep")) == cold
                    or bool(job.get("evict_mapped_wal_before_measurement")) != (cold and not nvme)
                    or job.get("media") != {"expected": "cold" if cold else "memory", "history": "normal",
                                             "wal_eviction": "selective" if cold and not nvme else "none"}
                    or dataset.get("tier") != ("first_touch" if cold else "repeat_read")
                    or dataset.get("page_cache_policy") != policy
                    or dataset.get("mapped_wal_eviction") != ("before_measurement_gate" if cold and not nvme else "not_requested")
                    or dataset.get("directness") != directness or dataset.get("target_cache") != cache
                    or job.get("target_io") != ("direct" if target == "kernel" else
                                                "spdk_direct" if target == "spdk" else "not_applicable")
                    or (nvme and (job.get("nvmeof_route") != "sealed_read" or "owner_polling" in job))
                    or (not nvme and ("nvmeof_route" in job or "nvmeof_target" in job))):
                violations.append(f"{key}: arm cache, geometry, or route contract differs")
    return violations


def _focused_exact(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(_focused_exact(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(_focused_exact(a, b) for a, b in zip(actual, expected))
    return actual == expected


def validate_focused_job(job):
    """Validate an exact case; selection is its minimum included campaign selection.

    Recommended cases also belong to full; only the appended cases require full.
    """
    if not isinstance(job, dict):
        raise ValueError("focused job must be an object")
    active_owner_count(job)
    if job.get("case_kind") == "sui_trace_replay":
        raise ValueError("Sui trace replay is reserved and not admitted")
    case_id = job.get("case_id")
    if not isinstance(case_id, str):
        raise ValueError("focused case_id is required")
    candidates = (make_focused_matrix(job["selection"])
                  if job.get("selection") in ("valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS) else _focused_jobs())
    expected = next((item for item in candidates if item["case_id"] == case_id), None)
    if expected is None or not _focused_exact(job, expected):
        raise ValueError("focused job differs from the exact reviewed case")
    return job


def validate_media_sample_count(media, samples, config):
    successful_reads = analyze.successful_read_keys(samples, config["workload"]["batch_size"])
    if media["successful_reads"] != successful_reads:
        raise ValueError("media successful reads differ from raw samples")


def make_run_config(job, case_root, options, source_revision, deadline_seconds=180):
    active_owner_count(job)
    if "case_kind" in job:
        validate_focused_job(job)
    case_root = Path(case_root)
    if not case_root.is_absolute():
        raise ValueError("absolute case root required")
    maximum = job["keys"] + job["warmup_operations"] + job["measurement_operations"] + 4096
    options = dict(options)
    contract = ack_contract.selected(job["backend"], job.get("ack_contract", "synced"))
    if "ack_contract" in options and ack_contract.selected(job["backend"], options["ack_contract"]) != contract:
        raise ValueError("backend options ack_contract differs from the planned case")
    if contract != "synced":
        options["ack_contract"] = contract
    if job.get("diagnostics", {}).get("valkey_profile_limit"):
        if job["backend"] not in ("valkey_tcp", "valkey_rdma"):
            raise ValueError("Valkey client diagnostics require a Valkey backend")
        if job["backend"] == "valkey_tcp":
            if options.get("client", "libvalkey") != "libvalkey":
                raise ValueError("Valkey client diagnostics require libvalkey")
            options["client"] = "libvalkey"
    if job["backend"] == "native":
        for option, value in (("durability", job["native_durability"]),
                              ("native_wal_geometry", job.get("native_wal_geometry", "native_default"))):
            if option in options and options[option] != value:
                raise ValueError(f"native {option} differs from the planned case")
            options[option] = value
        if "native_shards" in job:
            shards = job["native_shards"]
            if "native_shards" in options and (type(options["native_shards"]) is not int
                                                or options["native_shards"] != shards):
                raise ValueError("native_shards differs from the planned case")
            options["native_shards"] = shards
    if job["backend"] in TRANSPORTS:
        tier = durability_tier(job)
        if options.get("durability_tier", tier) != tier:
            raise ValueError("durability_tier differs from the planned case")
        options["durability_tier"] = tier
        expected = (transport_for_case("tidehunter_verbs_native")
                    if job.get("case_kind") and
                    (job["backend"] == "tidehunter_nvme_rdma" or
                     (job["backend"] == "tidehunter_nvme_tcp" and job.get("nvmeof_route") == "sealed_read"))
                    else TRANSPORTS[job["backend"]])
        selected = validate_transport(options.get("transport", expected))
        if protocol_key(selected) != protocol_key(expected):
            raise ValueError("runtime transport differs from research case provider/security")
        options["transport"] = selected
    config = {"schema_version": 1, "backend": {"kind": "tidehunter" if job["backend"] in NVME_BACKENDS else runtime_backend(job["backend"]), "options": options},
        "seed": job.get("sealed_dataset", {}).get("seed", 1775 + job["repeat"]),
        "namespace": "cloud-" + hashlib.blake2b(job.get("sealed_dataset", job.get("read_tier", {})).get("data_id", job["case_id"]).encode(), digest_size=12).hexdigest(),
        "dataset": {"keys": job["keys"], "value_size": job["value_size"], "entropy": 1.0},
        "workload": {"distribution": job["distribution"], "mix": job["mix"], "batch_size": 32},
        "execution": {"mode": "open_loop" if job["offered_ops_per_second"] else "closed_loop",
            "concurrency": job["concurrency"], "queue_capacity": job.get("execution", {}).get("queue_capacity", 128),
            "request_timeout_ms": 10000, "offered_ops_per_second": job["offered_ops_per_second"]},
        "phases": {"preload": True, "warmup_operations": job["warmup_operations"],
            "aging_operations": 0, "measurement_operations": job["measurement_operations"]},
        "qualification_only": False, "source_revision": source_revision,
        "measurement_gate": {"ready_file": str(case_root / "measurement.ready"),
            "release_file": str(case_root / "measurement.release"), "timeout_seconds": 240},
        "measurement_completion_gate": {"ready_file": str(case_root / "measurement.complete"),
            "release_file": str(case_root / "measurement.observed"), "timeout_seconds": 240},
        "limits": {"max_keys": max(job["keys"], 1000000), "max_bytes": 16 * 1024**3,
            "max_operations": maximum, "deadline_seconds": max(deadline_seconds, 600 if job["keys"] > 65536 else deadline_seconds),
            "shutdown_grace_seconds": 20}}
    if "put_key_policy" in job:
        config["workload"]["put_key_policy"] = job["put_key_policy"]
    if "case_kind" in job:
        config["case_id"] = job["case_id"]
    if job.get("case_kind") == "interference":
        config["execution"]["mode"] = "interference"
        config["interference"] = dict(job["interference"])
    if "sealed_dataset" in job:
        config["phases"]["preload"] = False
        config["phases"]["verify_existing_dataset"] = True
        config["phases"]["evict_mapped_wal_before_measurement"] = job.get("evict_mapped_wal_before_measurement", False)
    elif job.get("evict_mapped_wal_before_measurement"):
        config["phases"]["evict_mapped_wal_before_measurement"] = True
    if job.get("case_kind") == "hybrid_put":
        config["phases"]["preload"] = True
        config["phases"]["verify_existing_dataset"] = False
    if "diagnostics" in job:
        config["diagnostics"] = job["diagnostics"]
    if job.get("warmup_key_sweep"):
        config["phases"]["warmup_key_sweep"] = True
    if job.get("measurement_key_sweep"):
        config["phases"]["measurement_key_sweep"] = True
    return config


def admit_wal_eviction_evidence(directory, job, *, completed):
    if not job.get("evict_mapped_wal_before_measurement"):
        return None
    path = Path(directory) / "result/pre-measurement-wal-eviction.json"
    if not path.is_file():
        if completed:
            raise ValueError("requested mapped WAL eviction evidence missing")
        return None
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("mapped WAL eviction evidence exceeds bound")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or data.get("requested") is not True or type(data.get("available")) is not bool:
        raise ValueError("invalid mapped WAL eviction evidence")
    if completed and (data["available"] is not True or not isinstance(data.get("evidence"), dict)):
        raise ValueError("requested mapped WAL eviction did not succeed")
    return {"path": "result/pre-measurement-wal-eviction.json",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": data}


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


LIVE_NVME_COUNTERS = ("completed_read_ops", "completed_read_bytes", "completed_write_ops",
                      "completed_write_bytes", "recovery_read_ops", "recovery_read_bytes",
                      "metadata_requests", "namespace_connects", "payload_rpc_requests",
                      "unknown_writes", "errors", "queue_ns", "queue_samples",
                      "submit_ns", "submit_samples", "cq_ns", "cq_samples",
                      "control_ns", "control_samples", "validation_ns", "validation_samples",
                      "allocation_ns", "allocation_samples", "publication_ns", "publication_samples",
                      "namespace_setup_ns", "namespace_setup_samples", "read_io_ns", "read_io_samples",
                      "persistence_ns", "persistence_samples",
                      "owner_completion_ns", "owner_completion_samples",
                      "wal_persistence_ns", "wal_persistence_samples",
                      "wal_namespace_ns", "wal_namespace_samples",
                      "frontier_file_ns", "frontier_file_samples",
                      "frontier_namespace_ns", "frontier_namespace_samples",
                      "route_evidence_dropped", "fence_evidence_dropped")

def requested_leased_sync(job):
    return job.get("live_leased_sync", "group_flush")


def requested_leased_flush_min_frame_bytes(job):
    return job.get("live_leased_flush_min_frame_bytes", 65536)


def executed_leased_settings(backend_info):
    """Resolve execution from the binary export, not from requested job keys."""
    if backend_info is not None and not isinstance(backend_info, dict):
        raise ValueError("invalid executed backend information")
    configuration = (backend_info or {}).get("configuration", {})
    if not isinstance(configuration, dict):
        raise ValueError("invalid executed leased sync configuration")
    if "live_leased_sync" not in configuration:
        return "per_frame_fua", 0
    mode = configuration["live_leased_sync"]
    minimum = configuration.get("live_leased_flush_min_frame_bytes", 0)
    if mode not in ("per_frame_fua", "group_flush") or type(minimum) is not int or minimum < 0:
        raise ValueError("invalid executed leased sync configuration")
    return mode, minimum


# Each exchange increments metadata_requests at the same submission site.
WRITE_METADATA_EXCHANGES = ("adoption_async_exchanges", "adoption_sync_exchanges",
                            "lease_exchanges", "reserve_exchanges", "complete_exchanges")


def qualify_live_nvme_measurement(evidence, job, backend_info=None):
    """Validate deltas against raw boundaries; a requested route alone is not proof."""
    reasons = []
    data = evidence if isinstance(evidence, dict) else {}
    f8_default = (job.get("selection") == "nvmeof_f8" and job.get("backend") in NVME_BACKENDS
                  and "live_leased_sync" not in job and "live_leased_flush_min_frame_bytes" not in job)
    try:
        executed_sync, executed_minimum = executed_leased_settings(backend_info)
        executed_configuration = (backend_info or {}).get("configuration", {})
        executed_polled = executed_configuration.get("live_polled_writes", False)
        if type(executed_polled) is not bool:
            raise ValueError("invalid executed leased polled configuration")
    except ValueError as exc:
        return {"qualified": False, "reasons": [str(exc)] + (
            ["F8 default arm did not run the engine default"] if f8_default else [])}
    start = data.get("start") or {}
    end = data.get("end") or {}
    start = start.get("live_nvmeof") if isinstance(start, dict) else None
    end = end.get("live_nvmeof") if isinstance(end, dict) else None
    delta = data.get("live_nvmeof")
    counters = delta.get("counters") if isinstance(delta, dict) else None
    if (data.get("measurement_attempted") is not True or data.get("measurement_drained") is not True
            or not isinstance(delta, dict) or delta.get("available") is not True
            or not isinstance(start, dict) or not isinstance(end, dict) or not isinstance(counters, dict)):
        return {"qualified": False, "reasons": ["live NVMe boundary counters unavailable"]}
    for name in LIVE_NVME_COUNTERS:
        a, b, value = start.get(name), end.get(name), counters.get(name)
        if (type(a) is not int or type(b) is not int or type(value) is not int
                or a < 0 or b < a or b >= 2**64 - 1 or value != b - a):
            reasons.append(f"invalid live NVMe counter {name}")
    if reasons:
        return {"qualified": False, "reasons": reasons}
    if job.get("selection") in ("valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS):
        profile_names = ("lease_reserve_ns", "lease_namespace_sync_ns", "lease_publish_ns",
                         "lease_profile_samples", "adopt_owner_ns", "adopt_close_scan_ns",
                         "adopt_close_scan_bytes", "adopt_profile_samples",
                         "extent_preleases", "extent_switch_waits")
        names = ("grant_misses", "controller_connect_ns", "controller_connect_samples")
        for name in names + tuple(name for name in profile_names
                                  if any(name in record for record in (start, end, counters))):
            a, b, value = start.get(name), end.get(name), counters.get(name)
            if (type(a) is not int or type(b) is not int or type(value) is not int
                    or a < 0 or b < a or b >= 2**64 - 1 or value != b - a):
                reasons.append(f"invalid live NVMe counter {name}")
    if job.get("selection") in ("leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS):
        names = ("reserve_exchanges", "complete_exchanges", "leased_frames", "leased_skip_markers", "fua_writes")
        if (executed_sync == "group_flush"
                and job.get("durability_tier") == "synchronous"
                and any(job["mix"].get(name, 0) for name in ("put", "put_batch"))):
            names += ("leased_flushes", "leased_flush_covered_frames")
        if job.get("group") == "G29R" and job.get("live_pin_read_lanes"):
            names += ("read_lane_pin_failures",)
        for name in names:
            a, b, value = start.get(name), end.get(name), counters.get(name)
            if (type(a) is not int or type(b) is not int or type(value) is not int
                    or a < 0 or b < a or b >= 2**64 - 1 or value != b - a):
                reasons.append(f"invalid live NVMe counter {name}")
        if not reasons and job["group"] in ("G17W", "G18W", "G18B", "G19W", "G20W", "G21W", "G22W", "G23W", "G23X", "G28W", "G28C", "G29W", "G29X", "G31W", "G31H", "G31S", "G31X", "G32W", "G32H", "G32S", "G30W", "G24W", "G24M") and any(
                job["mix"].get(name, 0) for name in ("put", "put_batch")):
            if job["live_write_mode"] == "leased":
                if counters["reserve_exchanges"] or counters["complete_exchanges"] or counters["leased_frames"] <= 0:
                    reasons.append("leased write exchanges or frame count differ")
                if executed_polled and any(
                        counters.get(name, 0) != 0 for name in ("leased_flushes", "leased_flush_covered_frames")):
                    reasons.append("polled leased writes observed shared flushes")
                if (executed_sync == "group_flush" and not executed_polled
                        and job["durability_tier"] == "synchronous"):
                    if counters["fua_writes"] > counters["leased_frames"] + counters["leased_skip_markers"]:
                        reasons.append("group flush FUA count exceeds frames and skip markers")
                    if (job["value_size"] == 65536 and job["concurrency"] in (16, 64)
                            and executed_minimum <= job["value_size"] * job["concurrency"]
                            and job.get("live_leased_write_window") != 1) and (
                            counters["leased_flushes"] <= 0
                            or counters["leased_flush_covered_frames"] <= counters["leased_flushes"]):
                        reasons.append("group flush did not cover shared frames")
                else:
                    expected_fua = (counters["leased_frames"] + counters["leased_skip_markers"]
                                    if job["durability_tier"] == "synchronous" else 0)
                    if counters["fua_writes"] != expected_fua:
                        reasons.append("leased FUA count differs from frames and skip markers")
            elif counters["leased_frames"]:
                reasons.append("reserved write observed leased frames")
        if (job.get("group") == "G29R" and job.get("live_pin_read_lanes")
                and counters.get("read_lane_pin_failures") != 0):
            reasons.append("read lane pin failures observed")
        if (job["group"] in ("G17R", "G18R", "G18M", "G18X", "G19R", "G19X", "G20R", "G21R", "G24R", "G24Q", "G24L", "G24P", "G24S", "G24C", "G24M", "G25A", "G29R", "G29G", "G31G", "G30R", "G30S")
                and job["backend"] == "tidehunter_nvme_rdma" and job["mix"]["read"] > 0):
            coordinator = (data.get("deltas") or {}).get("coordinator") or {}
            value = (coordinator.get("counters") or {}).get("read_profile_live_inline")
            first = (data.get("start") or {}).get("coordinator") or {}
            last = (data.get("end") or {}).get("coordinator") or {}
            a, b = first.get("read_profile_live_inline"), last.get("read_profile_live_inline")
            if (coordinator.get("available") is not True or type(a) is not int or type(b) is not int
                    or type(value) is not int or a < 0 or b < a or value != b - a
                    or (not job["live_inline_reads"] and value != 0)
                    or (job["live_inline_reads"] and job["concurrency"] == 1 and value <= 0)):
                reasons.append("live inline admission evidence differs")
    if counters["payload_rpc_requests"]:
        reasons.append("payload RPC observed on live NVMe route")
    if counters["errors"] or counters["unknown_writes"]:
        reasons.append("live NVMe errors or unknown writes observed")
    if counters["route_evidence_dropped"] or counters["fence_evidence_dropped"]:
        reasons.append("live NVMe route or fence evidence was dropped")
    if job.get("live_export_mode") in ("per_owner", "per_file"):
        if counters["namespace_connects"] > 1:
            reasons.append("permanent export made more than one controller connect")
        if any(job.get("mix", {}).get(name, 0) for name in ("read", "get_many")):
            mixed_puts = any(job.get("mix", {}).get(name, 0) for name in ("put", "put_batch"))
            metadata_bound = job["read_lanes"] + 1
            read_metadata = counters["metadata_requests"]
            if mixed_puts:
                valid = True
                for name in ("extents_leased", *WRITE_METADATA_EXCHANGES):
                    a, b, value = start.get(name), end.get(name), counters.get(name)
                    if (type(a) is not int or type(b) is not int or type(value) is not int
                            or a < 0 or b < a or b >= 2**64 - 1 or value != b - a):
                        reasons.append("invalid live NVMe counter " + name)
                        valid = False
                if valid:
                    metadata_bound += job["read_lanes"] * counters["extents_leased"]
                    read_metadata -= sum(counters[name] for name in WRITE_METADATA_EXCHANGES)
                coordinator = (data.get("deltas") or {}).get("coordinator") or {}
                a = ((data.get("start") or {}).get("coordinator") or {}).get("read_rpc_count")
                b = ((data.get("end") or {}).get("coordinator") or {}).get("read_rpc_count")
                value = (coordinator.get("counters") or {}).get("read_rpc_count")
                if (coordinator.get("available") is not True or type(a) is not int
                        or type(b) is not int or type(value) is not int
                        or a < 0 or b < a or value != b - a or value != 0):
                    reasons.append("mixed workload coordinator read RPC evidence differs")
            if read_metadata < 0 or read_metadata > metadata_bound:
                reasons.append("permanent export sent owner requests for reads")
    mix = job.get("mix", {})
    if any(mix.get(name, 0) for name in ("put", "put_batch", "delete")):
        if counters["completed_write_ops"] <= 0 or counters["completed_write_bytes"] <= 0:
            reasons.append("no completed live NVMe writes observed")
    if any(mix.get(name, 0) for name in ("read", "get_many")):
        if counters["completed_read_ops"] <= 0 or counters["completed_read_bytes"] <= 0:
            if not job.get("coordinator_cache_bytes"):
                reasons.append("no completed live NVMe reads observed")
            else:
                first = (data.get("start") or {}).get("coordinator") or {}
                last = (data.get("end") or {}).get("coordinator") or {}
                hits = (data.get("deltas") or {}).get("coordinator") or {}
                values = hits.get("counters") if isinstance(hits, dict) else None
                values = values if isinstance(values, dict) else {}
                a = first.get("hits") if isinstance(first, dict) else None
                b = last.get("hits") if isinstance(last, dict) else None
                count = values.get("hits")
                if (not isinstance(hits, dict) or hits.get("available") is not True
                        or type(a) is not int or type(b) is not int or type(count) is not int
                        or a < 0 or b < a or count != b - a
                        or count < job.get("measurement_operations", 0)):
                    reasons.append("cache hits do not account for the measured read calls")
    if f8_default and (executed_sync != "group_flush" or executed_minimum != 65536
                       or executed_configuration.get("live_polled_writes") is not False):
        reasons.append("F8 default arm did not run the engine default")
    if (job.get("selection") == "nvmeof_f8" and job.get("backend") in NVME_BACKENDS
            and job.get("live_leased_sync") == "per_frame_fua"
            and executed_configuration.get("live_leased_sync") != "per_frame_fua"):
        reasons.append("F8 explicit FUA arm did not export per_frame_fua")
    return {"qualified": not reasons, "reasons": reasons}


def admit_live_write_contract(job, backend_info):
    if "live_leased_sync" in job and job["live_leased_sync"] not in ("per_frame_fua", "group_flush"):
        raise ValueError("invalid live_leased_sync")
    if "live_leased_flush_min_frame_bytes" in job and (
            type(job["live_leased_flush_min_frame_bytes"]) is not int
            or job["live_leased_flush_min_frame_bytes"] < 0
            or requested_leased_sync(job) != "group_flush"):
        raise ValueError("invalid live_leased_flush_min_frame_bytes")
    if "live_pin_read_lanes" in job and type(job["live_pin_read_lanes"]) is not bool:
        raise ValueError("invalid live_pin_read_lanes")
    if "live_leased_write_lanes" in job and (
            type(job["live_leased_write_lanes"]) is not int or not 1 <= job["live_leased_write_lanes"] <= 4):
        raise ValueError("invalid live_leased_write_lanes")
    configuration = backend_info.get("configuration", {})
    for key in ("live_leased_sync", "live_leased_flush_min_frame_bytes"):
        if key in job and configuration.get(key) != job[key]:
            raise ValueError(key + " differs")
    if ("live_pin_read_lanes" in job
            and configuration.get("client", {}).get("pin_read_lanes") != job["live_pin_read_lanes"]):
        raise ValueError("live_pin_read_lanes differs")
    if job.get("selection") not in ("leased_ab_compact", "valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS) or job.get("nvmeof_route") != "live":
        return
    if job.get("selection") in ("valkey_rdma_reads", *NVMEOF_PATH_SELECTIONS):
        configuration = backend_info.get("configuration", {})
        geometry = {"io_queues": job["live_io_queues"], "queue_size": job["live_io_queue_size"],
                    "poll_queues": job["live_poll_queues"]}
        if (configuration.get("live_export_mode") != job["live_export_mode"]
                or configuration.get("live_queue_geometry") != geometry
                or any(configuration.get(key, None if key in job else default) != job.get(key, default)
                       for key, default in (("live_lease_ahead", True), ("live_polled_writes", False)))):
            raise ValueError("live export mode or queue geometry differs")
    if job.get("selection") in ("nvmeof_parallel_writes", "nvmeof_single_caller", "nvmeof_f6", "nvmeof_f6b", "nvmeof_f8", "nvmeof_f7_highmem32", "nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2", "valkey_rdma_reads") and job["live_write_mode"] == "leased":
        configuration = backend_info.get("configuration", {})
        window = configuration.get("live_leased_write_window")
        expected_window = job.get("live_leased_write_window")
        bound = configuration.get("live_leased_write_window_bound")
        if ("live_leased_write_window" not in configuration
                or (window is not None and type(window) is not int)
                or window != expected_window
                or type(bound) is not int or bound <= 0
                or bound > job.get("live_max_inflight_requests", 32)):
            raise ValueError("leased write window or resource bound differs")
    elif "live_leased_write_window" in job:
        configuration = backend_info.get("configuration", {})
        if (type(configuration.get("live_leased_write_window")) is not int
                or configuration["live_leased_write_window"] != job["live_leased_write_window"]):
            raise ValueError("leased write window differs")
    evidence = backend_info.get("configuration", {}).get("live_write", {})
    mode, tier = job["live_write_mode"], job["durability_tier"]
    expected = {
        ("leased", "native"): "leased extent: page-cache write",
        ("leased", "synchronous"): "synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous",
        ("reserved", "native"): "reserved frame: owner validated page-cache write",
        ("reserved", "synchronous"): "reserved frame: owner WAL and frontier root synchronized",
    }[(mode, tier)]
    if evidence.get("write_mode") != mode or evidence.get("acknowledgement_contract") != expected:
        raise ValueError("live write mode or acknowledgement contract differs")


def qualify_live_nvme_route(evidence, job, expected, backend_info=None):
    """Bind verified initiator namespaces and target implementations to the fleet."""
    result = qualify_live_nvme_measurement(evidence, job, backend_info)
    reasons = result["reasons"]
    transport = "rdma" if job["backend"] == "tidehunter_nvme_rdma" else "tcp"
    if not isinstance(expected, dict) or expected.get("transport") != transport:
        reasons.append("live NVMe expected transport unavailable or mismatched")
        return {"qualified": False, "reasons": reasons}
    addresses = expected.get("addresses")
    host_id = expected.get("host_id")
    host_nqn = expected.get("host_nqn")
    service = expected.get("service")
    try:
        if (not isinstance(addresses, list) or not addresses
                or len(set(addresses)) != len(addresses)
                or any(ipaddress.ip_address(address).is_loopback for address in addresses)
                or type(service) is not int or not 1 <= service <= 65535
                or not isinstance(host_id, list) or len(host_id) != 16
                or any(type(value) is not int or not 0 <= value <= 255 for value in host_id)
                or not any(host_id)):
            raise ValueError("invalid fleet identity")
        identity = uuid.UUID(bytes=bytes(host_id))
        if host_nqn != "nqn.2014-08.org.nvmexpress:uuid:" + str(identity):
            raise ValueError("host identity mismatch")
    except (ValueError, TypeError):
        reasons.append("invalid admitted live NVMe fleet identity")
        return {"qualified": False, "reasons": reasons}
    end = evidence.get("end") if isinstance(evidence, dict) else None
    stats = end.get("live_nvmeof") if isinstance(end, dict) else None
    routes = stats.get("routes") if isinstance(stats, dict) else None
    if not isinstance(routes, list) or not routes:
        reasons.append("no verified kernel live NVMe namespace route")
    else:
        for route in routes:
            ns = route.get("namespace_identity") if isinstance(route, dict) else None
            if (not isinstance(route, dict) or route.get("transport") != transport
                    or route.get("address") not in addresses or route.get("service") != service
                    or route.get("host_nqn") != host_nqn or route.get("host_id") != host_id
                    or not isinstance(ns, dict)
                    or not isinstance(ns.get("namespace_path"), str)
                    or not re.fullmatch(r"/dev/nvme[0-9]+n[0-9]+", ns["namespace_path"])
                    or type(ns.get("controller_instance")) is not int or ns["controller_instance"] < 0
                    or not isinstance(ns.get("nqn"), str) or not ns["nqn"].startswith("nqn.")
                    or not isinstance(ns.get("uuid"), list) or len(ns["uuid"]) != 16
                    or any(type(value) is not int or not 0 <= value <= 255 for value in ns["uuid"])
                    or not any(ns["uuid"])
                    or type(ns.get("nsid")) is not int or ns["nsid"] <= 0
                    or type(ns.get("bytes")) is not int or ns["bytes"] <= 0
                    or type(ns.get("block_size")) is not int or ns["block_size"] <= 0
                    or ns["bytes"] % ns["block_size"]):
                reasons.append("observed live NVMe route differs from admitted kernel namespace")
                break
            target = job.get("nvmeof_target", "kernel")
            if (target not in ("kernel", "spdk")
                    or route.get("target_implementation", "kernel") != target
                    or ns.get("target_implementation", "kernel") != target):
                reasons.append("live NVMe target implementation differs from the selected target")
                break
            if target == "spdk":
                binding = route.get("spdk")
                writer = route.get("writer")
                file = writer.get("file_identity") if isinstance(writer, dict) else None
                if (not isinstance(binding, dict)
                        or not all(isinstance(binding.get(key), str) and Path(binding[key]).is_absolute()
                                   for key in ("rpc_socket", "image_path"))
                        or not isinstance(binding.get("bdev_name"), str) or not binding["bdev_name"]
                        or any(type(binding.get(key)) is not int or binding[key] <= 0
                               for key in ("pid", "process_start_ticks"))
                        or writer is not None and (not isinstance(file, dict)
                            or file.get("path") != binding["image_path"]
                            or type(file.get("device")) is not int or file["device"] < 0
                            or type(file.get("inode")) is not int or file["inode"] <= 0
                            or file.get("bytes") != ns["bytes"]
                            or file.get("block_size") != ns["block_size"])):
                    reasons.append("live SPDK route lacks a valid authenticated owner binding")
                    break
    return {"qualified": not reasons, "reasons": reasons}


def comparison_backend(job):
    """Keep target implementations distinct without inventing an engine backend."""
    target = job.get("nvmeof_target", "kernel")
    if target not in ("kernel", "spdk") or ("nvmeof_target" in job and job["backend"] not in NVME_BACKENDS):
        raise ValueError("invalid NVMe target implementation")
    if target == "spdk":
        if job["backend"] != "tidehunter_nvme_rdma" or job.get("nvmeof_route") not in ("sealed_read", "live"):
            raise ValueError("SPDK requires a sealed or live RDMA route")
        return "tidehunter_spdk_live_rdma" if job["nvmeof_route"] == "live" else "tidehunter_spdk_rdma"
    return job["backend"]


def diagnostic_backend(job):
    """Runtime evidence uses a distinct identity for the immutable reader."""
    backend = job["backend"]
    if backend in NVME_BACKENDS and job.get("nvmeof_route") == "live":
        return "tidehunter_live_nvmeof"
    if backend in NVME_BACKENDS and job.get("nvmeof_route") == "sealed_read":
        return backend + "_sealed_read"
    return backend


def admit_write_timeline(directory, job, config, *, completed):
    requested = job.get("diagnostics", {}).get("write_timeline_limit", 0)
    configured = config.get("diagnostics", {}).get("write_timeline_limit", 0)
    if type(requested) is not int or requested != configured:
        raise ValueError("write timeline job and executed configuration limits differ")
    return analyze.admit_write_timeline(directory, config, completed=completed,
                                        expected_backend=diagnostic_backend(job))


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
            or data.get("backend") != diagnostic_backend(job) or data.get("phase") != "measurement"
            or type(requested) is not int or type(selection.get("requested_limit")) is not int
            or selection["requested_limit"] != requested
            or type(data.get("phase_attempted")) is not bool or type(data.get("runtime_enabled")) is not bool
            or type(selection.get("observed_samples")) is not int or selection["observed_samples"] < 0):
        raise ValueError("write-stage diagnostics schema, backend, phase or requested limit differs")
    if completed and requested and (not data["phase_attempted"] or not data["runtime_enabled"]
            or data.get("measurement_drained") is False or selection["observed_samples"] == 0
            or not isinstance(data.get("profile"), dict) or data.get("error")):
        raise ValueError("requested write-stage diagnostics did not observe real measurement samples")
    if completed and requested and data["backend"] == "tidehunter_live_nvmeof":
        if data.get("runtime_observation_mode") != "live_nvmeof_phase_counters":
            raise ValueError("requested live NVMe write diagnostics have an unrecognized observation mode")
        if data.get("measurement_drained") is not True:
            raise ValueError("live NVMe write diagnostics require a drained measurement")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.write_diagnostic_metrics(data, {})
    return {"path": "result/write-diagnostics.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "observed": data}

def _diagnostic_raw_rows(directory):
    path = Path(directory) / "result/samples.csv"
    if not path.is_file():
        return None
    rows = {}
    for row in analyze.read_samples(path):
        sample_id = row["caller_sample_id"]
        if sample_id is not None:
            if sample_id in rows:
                raise ValueError("duplicate diagnostic caller identity")
            rows[sample_id] = row
    return rows


def _uint(value):
    return type(value) is int and 0 <= value <= 2**64 - 1


def admit_valkey_diagnostics(directory, job, config=None, *, completed):
    """Bind client pipeline records to measurement API calls, not to API success."""
    requested = job.get("diagnostics", {}).get("valkey_profile_limit", 0)
    if type(requested) is not int or not 0 <= requested <= 65536:
        raise ValueError("invalid Valkey profile limit")
    if config is not None and config.get("diagnostics", {}).get("valkey_profile_limit", 0) != requested:
        raise ValueError("Valkey job and executed configuration limits differ")
    path = Path(directory) / "result/valkey-timings.json"
    if not path.is_file():
        if completed and requested:
            raise ValueError("requested Valkey diagnostics artifact is missing")
        return None
    if not requested or job.get("backend") not in ("valkey_tcp", "valkey_rdma"):
        raise ValueError("unexpected Valkey diagnostics")
    if config is not None:
        backend = config.get("backend", {})
        if backend.get("kind") != job["backend"] or (job["backend"] == "valkey_tcp"
                and backend.get("options", {}).get("client") != "libvalkey"):
            raise ValueError("Valkey diagnostics require the configured libvalkey backend")
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Valkey diagnostics exceed bounded artifact size")
    raw = path.read_bytes()
    data = json.loads(raw)
    if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
            or data["schema_version"] != 1 or data.get("requested_limit") != requested
            or type(data.get("requested_limit")) is not int
            or any(type(data.get(name)) is not bool for name in
                   ("measurement_attempted", "measurement_drained", "available"))
            or not isinstance(data.get("error"), (str, type(None)))):
        raise ValueError("invalid Valkey diagnostics wrapper")
    profile = data.get("profile")
    if data["available"]:
        if not isinstance(profile, dict) or profile.get("available") is not True:
            raise ValueError("available Valkey diagnostics lack a profile")
        if profile.get("source_commit") != "fe8d406c0b5a64ec897f6455e4de684629883764":
            raise ValueError("Valkey profile source commit differs")
        for name in ("rdma_source_sha256", "rdma_archive_sha256", "client_archive_sha256"):
            if not isinstance(profile.get(name), str) or not re.fullmatch(r"[0-9a-f]{64}", profile[name]):
                raise ValueError("invalid Valkey artifact provenance digest")
        for name in ("snapshot", "clock", "poll_wait_scope", "wake_to_cq_scope", "transport_scope"):
            if not isinstance(profile.get(name), str) or not profile[name]:
                raise ValueError("Valkey profile lacks clock or population scope")
        cq_available = profile.get("cq_available")
        reason = profile.get("cq_unavailable_reason")
        if (type(cq_available) is not bool or (cq_available and reason is not None)
                or (not cq_available and (not isinstance(reason, str) or not reason))
                or (job["backend"] == "valkey_tcp" and cq_available)):
            raise ValueError("invalid Valkey CQ availability")
        records = profile.get("records")
        if not isinstance(records, list) or len(records) > requested:
            raise ValueError("invalid bounded Valkey pipeline population")
        rows = _diagnostic_raw_rows(directory)
        seen = set()
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("invalid Valkey pipeline record")
            ident, operation = record.get("caller_sample_id"), record.get("operation")
            owner, connection = record.get("owner"), record.get("connection")
            if (not all(_uint(value) for value in (ident, owner, connection))
                    or operation not in ("get", "get_many", "put", "delete")
                    or type(record.get("worker_finished")) is not bool
                    or "pipeline_outcome" not in record or "cq" not in record
                    or record.get("pipeline_outcome") not in (None, "ok", "error", "cancelled")):
                raise ValueError("invalid Valkey pipeline identity or outcome")
            key = (ident, owner, connection)
            if key in seen:
                raise ValueError("duplicate Valkey caller/owner/connection pipeline")
            seen.add(key)
            for name in ("mutex_wait_ns", "blocking_enqueue_to_start_ns", "blocking_return_to_wake_ns",
                         "command_encode_ns", "transport_and_parser_ns", "reply_decode_ns"):
                if name not in record or (record[name] is not None and not _uint(record[name])):
                    raise ValueError("invalid nullable Valkey duration")
            cq = record.get("cq")
            if cq is not None:
                if not cq_available or not isinstance(cq, dict) or any(not _uint(cq.get(name)) for name in
                        ("poll_wait_ns", "poll_calls", "cq_poll_ns", "cq_poll_calls", "wake_to_cq_ns",
                         "wake_to_cq_samples", "completions", "clock_failures")):
                    raise ValueError("invalid Valkey CQ observations")
            if rows is None:
                if completed:
                    raise ValueError("Valkey diagnostics lack raw measurement samples")
            elif ident not in rows:
                raise ValueError("Valkey pipeline references a foreign caller")
            else:
                row = rows[ident]
                expected = {"get": ("read", "missing_read"), "get_many": ("get_many",),
                            "put": ("put",), "delete": ("delete",)}[operation]
                if row["operation"] not in expected or row["dispatch_observed"] != "true":
                    raise ValueError("Valkey pipeline does not match raw API operation")
        # Multiple owner pipelines can represent one MGET; their outcomes do not
        # prove that the enclosing API succeeded or that every owner was sampled.
    elif profile is not None and (not isinstance(profile, dict) or profile.get("available") is not False):
        raise ValueError("unavailable Valkey profile has inconsistent availability")
    if completed and (not data["measurement_attempted"] or not data["measurement_drained"]
                      or not data["available"] or data["error"]):
        raise ValueError("requested Valkey diagnostics did not complete")
    coverage = None
    if data["available"]:
        unfinished = sum(not record["worker_finished"] for record in records)
        missing_cq = sum(record["cq"] is None for record in records) if cq_available else 0
        clock_failures = sum(record["cq"]["clock_failures"] for record in records if record["cq"] is not None)
        failed_clocks = sum(record["cq"] is not None and record["cq"]["clock_failures"] > 0 for record in records)
        coverage = {"pipeline_records": len(records), "unfinished_workers": unfinished,
                    "cq_available": cq_available, "cq_unavailable_reason": reason,
                    "cq_missing_records": missing_cq, "cq_clock_failure_records": failed_clocks,
                    "cq_clock_failures": clock_failures,
                    "partial": bool(unfinished or missing_cq or clock_failures)}
    return {"path": "result/valkey-timings.json", "sha256": hashlib.sha256(raw).hexdigest(),
            "coverage": coverage, "observed": data}


def _validate_read_correlation(directory, correlation, requested, completed):
    if not isinstance(correlation, dict) or type(correlation.get("available")) is not bool:
        raise ValueError("invalid read correlation availability")
    for name in ("clock_scope", "granularity", "limitations"):
        if not isinstance(correlation.get(name), str) or not correlation[name]:
            raise ValueError("read correlation requires clock and granularity scopes")
    trace = correlation.get("trace")
    if not correlation["available"]:
        if trace is not None:
            raise ValueError("unavailable read correlation contains a trace")
        return
    if not isinstance(trace, dict) or trace.get("limit") != requested or type(trace.get("limit")) is not int:
        raise ValueError("invalid read trace limit")
    calls = trace.get("calls")
    if not isinstance(calls, list) or len(calls) > requested:
        raise ValueError("invalid read trace population")
    if any(not _uint(trace.get(name)) for name in ("dropped_calls", "contention_drops", "budget_drops")):
        raise ValueError("invalid read trace drop counts")
    rows = _diagnostic_raw_rows(directory)
    ids = set()
    budget = 512 * len(calls)
    for call in calls:
        if not isinstance(call, dict):
            raise ValueError("invalid read trace call")
        ident, members = call.get("caller_id"), call.get("members")
        if (not _uint(ident) or ident in ids or not _uint(members) or not members
                or call.get("status") not in ("complete", "error", "cancelled_or_incomplete", "incomplete")
                or any(not _uint(call.get(name)) for name in ("dropped_children", "dropped_events"))):
            raise ValueError("invalid read trace identity or status")
        ids.add(ident)
        if rows is None:
            if completed:
                raise ValueError("read trace lacks raw measurement samples")
        elif ident not in rows:
            raise ValueError("read trace references foreign caller")
        else:
            row = rows[ident]
            if (row["operation"] not in ("read", "missing_read", "get_many")
                    or row["dispatch_observed"] != "true" or row["logical_keys"] != members):
                raise ValueError("read trace does not match raw API identity")
        groups = call.get("groups")
        if not isinstance(groups, list) or len(groups) > 256:
            raise ValueError("invalid read trace group population")
        def events(items):
            if not isinstance(items, list) or len(items) > 512:
                raise ValueError("invalid read trace event population")
            for event in items:
                if (not isinstance(event, dict) or not isinstance(event.get("name"), str)
                        or not event["name"] or not _uint(event.get("elapsed_ns"))):
                    raise ValueError("invalid read trace event")
                queue, token = event.get("io_queue_id"), event.get("io_token")
                if (queue is None) != (token is None) or (queue is not None and
                        (not _uint(queue) or not _uint(token))):
                    raise ValueError("invalid read IO identity")
        events(call.get("events"))
        budget += 256 * len(call["events"])
        for group in groups:
            if (not isinstance(group, dict) or not _uint(group.get("owner"))
                    or type(group.get("membership_classified")) is not bool
                    or not _uint(group.get("dropped_events"))):
                raise ValueError("invalid read trace group")
            indices = group.get("members")
            coalesced = group.get("coalesced_members")
            if (not isinstance(indices, list) or not indices or len(indices) > members
                    or any(not _uint(index) or index >= members for index in indices)
                    or len(set(indices)) != len(indices)
                    or not isinstance(coalesced, list)
                    or any(not _uint(index) or index not in indices for index in coalesced)
                    or len(set(coalesced)) != len(coalesced)):
                raise ValueError("invalid read trace member identities")
            if any(group.get(name) is not None and not _uint(group[name])
                   for name in ("lane", "flight_slot")):
                raise ValueError("invalid read trace lane or flight")
            events(group.get("events"))
            budget += 512 + 64 * len(indices) + 256 * len(group["events"])
    if budget > 4 * 1024 * 1024:
        raise ValueError("read trace exceeds producer budget")


def admit_read_diagnostics(directory, job, *, completed):
    path = Path(directory) / "result/read-diagnostics.json"
    requested = job.get("diagnostics", {}).get("read_profile_limit", 0)
    if not path.is_file():
        if completed and requested:
            raise ValueError("requested read-stage diagnostics artifact is missing")
        return None
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("read-stage diagnostics exceed the bounded artifact size")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("selection"), dict):
        raise ValueError("read-stage diagnostics require object and selection evidence")
    selection = data["selection"]
    if (type(data.get("schema_version")) is not int or data["schema_version"] != 1
            or data.get("backend") != diagnostic_backend(job) or data.get("phase") != "measurement"
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
    correlation = (data.get("profile") or {}).get("caller_correlation")
    export_data = data
    if correlation is not None:
        _validate_read_correlation(directory, correlation, requested, completed)
        # The independent aggregate validator does not consume the bounded trace.
        export_data = dict(data)
        export_data["profile"] = dict(data["profile"])
        export_data["profile"].pop("caller_correlation")
    campaign_exporter.read_diagnostic_metrics(export_data, {})
    return {"path": "result/read-diagnostics.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "observed": data}


def qualify_nvme_measurement(evidence, expected_readers, expected_transport, targets, expected_target="kernel"):
    """Require actual interval activity, not startup verification or lifetime counters."""
    reasons = []
    if expected_transport not in ("tcp", "rdma"):
        reasons.append("expected NVMe transport is invalid")
    cache_policy = {"kernel": "buffered_file_page_cache", "kernel_direct": "direct_file_no_OS_pagecache",
                    "spdk": "direct_file_bdev_no_OS_pagecache"}.get(expected_target)
    if cache_policy is None or expected_target == "spdk" and expected_transport != "rdma":
        reasons.append("expected NVMe target implementation is invalid")
    exports = []
    if isinstance(targets, list):
        for owner in targets:
            target = owner.get("target") if isinstance(owner, dict) else None
            entries = target.get("exports") if isinstance(target, dict) else None
            if not isinstance(entries, list) or not entries:
                reasons.append("target export receipts unavailable")
                continue
            exports.extend(entries)
    if len(exports) != expected_readers:
        reasons.append("target export receipt count differs from reader count")
    target_policies = []
    for entry in exports:
        policy = entry.get("target_receipt") if isinstance(entry, dict) else None
        target_policies.append({key: policy.get(key) if isinstance(policy, dict) else None
                                for key in ("target_implementation", "target_cache", "target_cache_verified",
                                            "selected_transport", "direct_io_verified", "read_only_verified")})
        if (not isinstance(policy, dict)
                or policy.get("target_cache") != cache_policy
                or policy.get("target_cache_verified") is not True
                or policy.get("selected_transport") != expected_transport):
            reasons.append("target cache/open-mode or selected transport is unverified")
            continue
        if expected_target == "spdk" and (
                policy.get("target_implementation") != "spdk"
                or policy.get("direct_io_verified") is not True
                or policy.get("read_only_verified") is not True):
            reasons.append("SPDK direct-I/O/read-only target identity is unverified")
        if expected_target == "kernel_direct" and policy.get("target_implementation") != "kernel_direct":
            reasons.append("direct kernel target identity is unverified")
    nvme = evidence.get("nvmeof") if isinstance(evidence, dict) else None
    readers = nvme.get("readers") if isinstance(nvme, dict) else None
    if not isinstance(readers, dict) or not readers or len(readers) != expected_readers or nvme.get("available") is not True:
        reasons.append("measurement-boundary NVMe reader evidence unavailable or incomplete")
        readers = {}
    fields = ("eligible_hits", "ineligible_fallbacks", "io_submissions", "io_completions", "io_errors", "io_timeouts")
    totals = dict.fromkeys(fields, 0)
    reader_transports = {}
    for key, reader in readers.items():
        identity = reader.get("identity") if isinstance(reader, dict) else None
        reader_transports[key] = identity.get("transport") if isinstance(identity, dict) else None
        if reader_transports[key] != expected_transport:
            reasons.append("measurement reader transport is unavailable or differs from target")
    for reader in readers.values():
        counter = reader.get("counters") if isinstance(reader, dict) else None
        values = counter.get("counters") if isinstance(counter, dict) else None
        if (not isinstance(values, dict) or counter.get("available") is not True
                or any(type(values.get(field)) is not int or values[field] < 0 for field in fields)):
            reasons.append("measurement reader counters unavailable")
            totals = None
            break
        for field in fields:
            totals[field] += values[field]
    if not readers:
        totals = None
    if totals is not None and (totals["eligible_hits"] <= 0 or totals["io_submissions"] <= 0
            or totals["io_submissions"] != totals["io_completions"]
            or any(totals[name] != 0 for name in ("ineligible_fallbacks", "io_errors", "io_timeouts"))):
        reasons.append("NVMe measurement lacks successful eligible I/O or observed a fallback/error")
    coordinator = (evidence.get("deltas", {}).get("coordinator") if isinstance(evidence, dict) else None)
    values = coordinator.get("counters") if isinstance(coordinator, dict) else None
    if (not isinstance(values, dict) or coordinator.get("available") is not True
            or type(values.get("read_rpc_count")) is not int or type(values.get("hits")) is not int
            or values["read_rpc_count"] < 0 or values["hits"] < 0):
        reasons.append("measurement coordinator RPC/cache counters unavailable")
        values = None
    elif values["read_rpc_count"] != 0 or values["hits"] != 0:
        reasons.append("measurement used owner record RPC or coordinator cache")
    return {"qualified": not reasons, "reasons": reasons, "nvmeof_totals": totals,
            "expected_transport": expected_transport, "reader_transports": reader_transports,
            "expected_target": expected_target,
            "target_policies": target_policies,
            "coordinator_read_rpc_count": values.get("read_rpc_count") if values is not None else None,
            "coordinator_cache_hits": values.get("hits") if values is not None else None,
            "scope": "drained measurement interval; excludes full-key dataset verification; not NAND or wire timing"}


def admit_spdk_target_measurement(directory, job, receipt, *, completed):
    if job.get("nvmeof_target") != "spdk":
        if "spdk_target_measurement" in receipt or "spdk_target_io" in receipt:
            raise ValueError("SPDK evidence is attached to another target")
        return None
    binding = receipt.get("spdk_target_measurement")
    if not completed and (not isinstance(binding, dict) or "end" not in binding):
        return None
    if job.get("nvmeof_route") == "live":
        observed = spdk_measurement.admit_live(directory, binding)
    else:
        observed = spdk_measurement.admit(directory, binding,
            allow_zero_reads=job.get("case_kind") == "hybrid_put",
            owner_count=active_owner_count(job))
    recorded = receipt.get("spdk_target_io")
    if recorded is not None and recorded != observed:
        # Python 3.12 changed float sum to compensated summation. Accept the
        # older left-to-right result only for the two derived live means.
        normalized = dict(recorded) if isinstance(recorded, dict) else {}
        if job.get("nvmeof_route") == "live" and observed.get("available"):
            for direction in ("read", "write"):
                key = "mean_" + direction + "_completion_us"
                ops = observed[direction + "_ops"]
                total = 0.0
                for owner in observed["owners"]:
                    total += (owner[key] or 0) * owner[direction + "_ops"]
                legacy = total / ops if ops else None
                if key in normalized and normalized[key] == legacy:
                    normalized[key] = observed[key]
        if normalized != observed:
            raise ValueError("SPDK interval accounting differs from its bound raw evidence")
    return observed


def qualify_fullset_read_path(job, evidence):
    """Qualify observed paths, not a configured cache or a transport latency floor."""
    if job.get("cohort") != "registered_ram_fullset_control":
        return None
    deltas = evidence.get("deltas", {}) if isinstance(evidence, dict) else {}

    def total(component, field):
        group = deltas.get(component, {})
        rows = group.get("partitions")
        if (group.get("available") is not True or not isinstance(rows, dict)
                or set(rows) != active_partitions(job)):
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


def qualify_read_tier(job, evidence, receipt, *, local_mode=False):
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
            if not isinstance(rows, dict) or set(rows) != active_partitions(job):
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
    for partition in sorted(active_partitions(job)):
        try:
            if any(evidence[side]["owner_executor"].get("available") is not True
                   or set(evidence[side]["owner_executor"]["partitions"]) != active_partitions(job)
                   for side in ("start", "end")):
                raise ValueError("authority boundary unavailable")
            start = evidence["start"]["owner_executor"]["partitions"][partition]["authority"]["history_loads"]
            end = evidence["end"]["owner_executor"]["partitions"][partition]["authority"]["history_loads"]
            if type(start) is not int or type(end) is not int or start < 0 or end < start:
                raise ValueError("invalid authority boundary")
            authority_loads.append(end - start)
        except (TypeError, KeyError, ValueError):
            authority_loads = []
            break
    observed["authority_history_loads"] = (sum(authority_loads)
                                             if len(authority_loads) == active_owner_count(job) else None)
    observed["owner_grants"] = observed_owner_grants
    reasons = []
    if receipt.get("status") != "completed" or not isinstance(evidence, dict) or evidence.get("measurement_drained") is not True:
        reasons.append("measurement incomplete or undrained")
    if not isinstance(receipt.get("read_tier_effective"), dict):
        reasons.append("effective cache configuration unavailable")
    else:
        requested = job["read_tier"]["requested"]
        effective = receipt["read_tier_effective"]
        for name in ("coordinator_cache_bytes",
                     "coordinator_read_memory_allowance_bytes", "owner_read_memory_allowance_bytes",
                     "page_cache_policy", "warmup_operations", "warmup_key_sweep",
                     "measurement_key_sweep", "read_lanes", "concurrency"):
            if (local_mode and name == "page_cache_policy"
                    and requested.get(name) == "drop_at_measurement_gate"
                    and effective.get(name) == "scoped_file_eviction"):
                evictions = receipt.get("measurement_evidence", {}).get("scoped_file_eviction")
                if (isinstance(evictions, list) and len(evictions) == 6
                        and all(isinstance(row, dict) and row.get("method") == "scoped_file_eviction"
                                for row in evictions)
                        and any(row.get("files") for row in evictions)):
                    continue
            if effective.get(name) != requested.get(name):
                reasons.append(name + " effective configuration differs or is unavailable")
    def expect(name, predicate, description):
        value = observed[name]
        if value is None or not predicate(value):
            reasons.append(description + (" unavailable" if value is None else f" (observed {value})"))
    zero = lambda value: value == 0
    positive = lambda value: value > 0
    if tier == "coordinator_hit":
        for name in ("coord_misses", "rpc", "wal_reads"):
            expect(name, zero, name + " must be zero")
        expect("coord_hits", positive, "coordinator hits required")
    elif tier == "owner_hit":
        for name in ("coord_hits", "owner_misses", "wal_reads", "posts"):
            expect(name, zero, name + " must be zero")
        for name in ("rpc", "owner_hits"):
            expect(name, positive, name + " required")
    elif tier == "wal_warm":
        expect("coord_hits", zero, "coordinator hits must be zero")
        expect("wal_reads", positive, "owner WAL reads required")
        reasons.append("warm-up does not establish that every WAL read hit the Linux page cache")
    elif tier in ("registered_steady", "registered_reuse", "registered_steady_handle_headroom",
                   "registered_reuse_handle_headroom",
                   "registered_steady_c16_mixed_acquisition_reuse", "registered_reuse_c16_mixed_acquisition_reuse"):
        full = qualify_fullset_read_path(job | {"cohort": "registered_ram_fullset_control"}, evidence)
        reasons.extend(full["reasons"])
    elif tier == "registered_acquire":
        start = (evidence.get("start") or {}).get("client_resident_lanes", {}) if isinstance(evidence, dict) else {}
        partitions = start.get("partitions", {}) if isinstance(start, dict) else {}
        if start.get("available") is not True or set(partitions) != active_partitions(job):
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
        for name in ("coord_hits",):
            expect(name, zero, name + " must be zero")
        expect("wal_reads", positive, "owner WAL reads required")
        hosts = receipt.get("cold_device_evidence")
        if not isinstance(hosts, list) or len(hosts) != 6:
            reasons.append("owned-device traces unavailable")
        else:
            owners = hosts[1:1 + active_owner_count(job)]
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


def summarize_case(directory, *, local_mode=False):
    directory = Path(directory)
    receipt = json.loads((directory / "receipt.json").read_text())
    job = receipt["job"]
    value = {"case_id": job["case_id"], "job": job, "status": receipt.get("status"),
             "ack_contract": ack_contract.selected(job["backend"], job.get("ack_contract", "synced")),
             "error": receipt.get("error"), "measurement_evidence": receipt.get("measurement_evidence"),
             "counter_evidence": receipt.get("counter_evidence")}
    if "case_kind" in job:
        validate_focused_job(job)
        focused = analyze.admit_focused_artifacts(
            directory, job, receipt, completed=receipt.get("status") == "completed")
        value["focused_evidence"] = focused
    standalone = job.get("case_kind") in ("fio_floor", "block_trace", "net_floor", "ssd_floor", "ssd_write_floor")
    owner_layout = job["backend"] in TRANSPORTS and "owner_count" in job
    owner_bindings = receipt.get("owner_durability_receipts")
    if (owner_layout and not standalone and receipt.get("status") == "completed"
            and owner_bindings is None):
        raise ValueError("completed case lacks owner durability receipts")
    if owner_bindings is not None:
        if (not isinstance(owner_bindings, list) or len(owner_bindings) > active_owner_count(job)
                or (owner_layout and receipt.get("status") == "completed"
                    and len(owner_bindings) != active_owner_count(job))):
            raise ValueError("invalid owner durability receipt bindings")
        owners = []
        for index, binding in enumerate(owner_bindings):
            expected = f"owner-durability/node-{index}.durability.json"
            if not isinstance(binding, dict) or binding.get("path") != expected:
                raise ValueError("invalid owner durability receipt path")
            path = directory / expected
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != binding.get("sha256"):
                raise ValueError("owner durability receipt differs from its integrity binding")
            owner = json.loads(data)
            if owner_layout and (type(owner.get("partition")) is not int
                                           or owner["partition"] != index):
                raise ValueError("owner durability receipt partition differs from its binding")
            owners.append(owner)
        value["owner_durability_receipts"] = owners
    facts = receipt.get("owner_provider_facts")
    if isinstance(facts, dict) and facts.get("path") == "owner-provider-facts.json":
        data = (directory / facts["path"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != facts.get("sha256"):
            raise ValueError("owner provider facts differ from their integrity binding")
        value["owner_provider_facts"] = json.loads(data)
    elif facts is not None:
        value["owner_provider_facts"] = facts
    if standalone:
        return {**value, "metrics_unavailable": "standalone evidence; no KV samples"}
    if "sealed_dataset" in job:
        value["nvmeof_trace_evidence"] = receipt.get("nvmeof_trace_evidence")
        value["nvmeof_trace_raw_location"] = receipt.get("nvmeof_trace_raw_location")
        value["nvmeof_trace_upload_complete"] = receipt.get("nvmeof_trace_upload_complete")
        value["cold_device_evidence"] = receipt.get("cold_device_evidence")
        value["cold_device_raw_location"] = receipt.get("cold_device_raw_location")
        value["nvmeof_measurement_qualification"] = receipt.get("nvmeof_measurement_qualification")
        value["dataset_source"] = receipt.get("dataset_source")
    if job.get("nvmeof_target") == "spdk":
        value["spdk_target_measurement"] = receipt.get("spdk_target_measurement")
        if receipt.get("status") == "completed" or receipt.get("raw_artifacts_validated"):
            observed_spdk = admit_spdk_target_measurement(
                directory, job, receipt, completed=receipt.get("status") == "completed")
            if observed_spdk is not None:
                value["spdk_target_io"] = observed_spdk
    if receipt.get("status") != "completed" and not receipt.get("raw_artifacts_validated"):
        binding = receipt.get("write_timeline")
        path = directory / "result/write-timeline.json"
        if binding is not None or path.exists():
            normalized = json.loads((directory / "normalized-config.json").read_text())
            timeline = admit_write_timeline(directory, job, normalized, completed=False)
            if timeline != binding:
                raise ValueError("partial write timeline differs from its receipt integrity binding")
            value["write_timeline"] = timeline
        for key, filename, admit in (
                ("read_diagnostics", "read-diagnostics.json", admit_read_diagnostics),
                ("valkey_diagnostics", "valkey-timings.json", admit_valkey_diagnostics)):
            binding = receipt.get(key)
            if binding is not None or (directory / "result" / filename).exists():
                observed = admit(directory, job, completed=False)
                if observed != binding:
                    raise ValueError(f"partial {key} differs from its receipt integrity binding")
                value[key] = observed
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
            job, read_interval["observed"] if read_interval else None, receipt, local_mode=local_mode)
    eviction = admit_wal_eviction_evidence(directory, job, completed=receipt.get("status") == "completed")
    if eviction is not None:
        if eviction != receipt.get("wal_eviction_evidence"):
            raise ValueError("mapped WAL eviction evidence differs from its receipt")
        value["wal_eviction_evidence"] = eviction
    elif receipt.get("wal_eviction_evidence") is not None:
        raise ValueError("mapped WAL eviction artifact missing")
    cpu_binding = receipt.get("host_cpu_evidence")
    if cpu_binding is not None:
        if cpu_binding.get("path") != "host-cpu-interval.json":
            raise ValueError("host CPU evidence path differs from the owned interval artifact")
        path = directory / "host-cpu-interval.json"
        if hashlib.sha256(path.read_bytes()).hexdigest() != cpu_binding.get("sha256") or json.loads(path.read_text()) != cpu_binding.get("observed"):
            raise ValueError("host CPU interval differs from its integrity binding")
        value["host_cpu_evidence"] = cpu_binding
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
        if "core_complete" in resource_binding or "io_complete" in resource_binding:
            coverage = resource_accounting.coverage(resources)
            if any(type(resource_binding.get(key)) is not bool or resource_binding[key] != value
                   for key, value in coverage.items()):
                raise ValueError("resource coverage differs from reproduced boundary counters")
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
    valkey = admit_valkey_diagnostics(directory, job, normalized, completed=receipt.get("status") == "completed")
    if valkey != receipt.get("valkey_diagnostics"):
        raise ValueError("Valkey diagnostics differ from their receipt integrity binding")
    if valkey is not None:
        value["valkey_diagnostics"] = valkey
    timeline = admit_write_timeline(directory, job, normalized, completed=receipt.get("status") == "completed")
    if timeline != receipt.get("write_timeline"):
        raise ValueError("write timeline differs from its receipt integrity binding")
    if timeline is not None:
        value["write_timeline"] = timeline
    if job["backend"] == "native":
        options = normalized["backend"]["options"]
        for planned, option in (("native_durability", "durability"),
                                ("native_wal_geometry", "native_wal_geometry"),
                                ("native_shards", "native_shards")):
            if planned in job and options.get(option) != job[planned]:
                raise ValueError(f"native {option} differs from the planned case")
    admit_durability_tier(job, normalized["backend"]["options"], result / "manifest.json")
    if job["backend"] in NVME_BACKENDS and job.get("nvmeof_route") == "live":
        manifest_path = result / "manifest.json"
        backend_info = json.loads(manifest_path.read_text()).get("backend_info", {}) if manifest_path.is_file() else {}
        if receipt.get("status") == "completed":
            admit_live_write_contract(job, backend_info)
        binding = receipt.get("live_nvme_expected")
        admitted = receipt.get("live_nvme_measurement_qualification")
        expected = None
        if binding is not None:
            if not isinstance(binding, dict) or binding.get("path") != "live-nvme-expected.json":
                raise ValueError("invalid live NVMe expectation binding")
            artifact = directory / binding["path"]
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != binding.get("sha256"):
                raise ValueError("live NVMe expectation differs from its integrity binding")
            expected = json.loads(artifact.read_text())
            live = config["backend"]["options"].get("external_remote_config", {}).get("live_nvme")
            if not isinstance(live, dict) or any(expected.get(k) != live.get(k) for k in ("host_id", "host_nqn")):
                raise ValueError("live NVMe identity differs from executed configuration")
        qualification = qualify_live_nvme_route(read_interval["observed"] if read_interval else None, job, expected, backend_info)
        value["live_nvme_measurement_qualification"] = qualification
        if admitted is not None and qualification != admitted:
            raise ValueError("live NVMe qualification differs from measurement evidence")
        if receipt.get("status") == "completed" and (binding is None or admitted is None or not qualification["qualified"]):
            raise ValueError("completed live NVMe result lacks qualified physical route evidence")
    if (job["backend"] in NVME_BACKENDS and job.get("nvmeof_route") != "live"
            and job.get("case_kind") != "hybrid_put"):
        expected = len(config["backend"]["options"]["nvmeof_readers"])
        transport = "rdma" if job["backend"] == "tidehunter_nvme_rdma" else "tcp"
        binding = receipt.get("nvmeof_target_evidence")
        admitted_qualification = receipt.get("nvmeof_measurement_qualification")
        if receipt.get("status") == "completed" and (binding is None or admitted_qualification is None):
            raise ValueError("completed NVMe result lacks bound target evidence or measurement qualification")
        targets = None
        if binding is not None:
            if not isinstance(binding, dict) or binding.get("path") != "nvme-targets.json":
                raise ValueError("invalid NVMe target evidence binding")
            artifact = directory / binding["path"]
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != binding.get("sha256"):
                raise ValueError("NVMe target evidence differs from its integrity binding")
            targets = json.loads(artifact.read_text())
        qualification = qualify_nvme_measurement(
            read_interval["observed"] if read_interval else None, expected, transport, targets,
            expected_target=("kernel_direct" if job.get("target_io") == "direct"
                             else job.get("nvmeof_target", "kernel")))
        if admitted_qualification is None:
            qualification["qualified"] = False
            qualification["reasons"].append("measurement qualification receipt unavailable")
        value["nvmeof_measurement_qualification"] = qualification
        if binding is not None and admitted_qualification is not None:
            if qualification != admitted_qualification:
                raise ValueError("NVMe path qualification differs from measurement evidence")
        if job.get("case_kind") and job.get("nvmeof_target") == "kernel":
            target_io = analyze.qualify_target_buffered_io(job, targets, expected)
            value["target_io_qualification"] = target_io
        if receipt.get("status") == "completed" and not qualification["qualified"]:
            target_io = value.get("target_io_qualification") or {}
            policies = qualification.get("target_policies") or []
            expected_cache = ("direct_file_no_OS_pagecache" if job.get("target_io") == "direct"
                              else "buffered_file_page_cache")
            only_readback = (target_io.get("qualified") is False and policies
                             and set(qualification["reasons"]) == {"target cache/open-mode or selected transport is unverified"}
                             and all(p.get("target_cache") == expected_cache
                                     and p.get("selected_transport") == transport
                                     and (job.get("target_io") != "direct"
                                          or p.get("target_implementation") == "kernel_direct") for p in policies))
            if not only_readback:
                raise ValueError("completed NVMe result lacks qualified measurement path")
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
    value["ack_contract"] = analyze.admit_ack_contract(
        job["backend"], summary.get("backend", {}), job.get("ack_contract", "synced"))
    if ack_contract.selected(job["backend"], normalized["backend"].get("options", {}).get(
            "ack_contract", "synced")) != value["ack_contract"]:
        raise ValueError("executable ack_contract differs from the reported backend contract")
    if receipt.get("status") == "completed" and job.get("case_kind") in ("interference", "write_wait_diagnostic"):
        marker_name = ("interference" if job["case_kind"] == "interference"
                       else "write_wait_metrics" if job["write_wait_diagnostics"]["enabled"] else None)
        if marker_name is not None:
            marker = manifest["run"].get(marker_name)
            artifact_name = "interference_result.json" if marker_name == "interference" else "write_wait_metrics.json"
            if (not isinstance(marker, dict) or type(marker.get("schema_version")) is not int
                    or marker["schema_version"] != 1 or marker.get("path") != artifact_name):
                raise ValueError("focused benchmark artifact manifest marker unavailable")
    for name in names:
        if hashlib.sha256((result / name).read_bytes()).hexdigest() != receipt["artifacts_sha256"][name]:
            raise ValueError("raw result checksum differs from its original admission")
    if "durability" in manifest and "durability" in summary and manifest["durability"] != summary["durability"]:
        raise ValueError("manifest and summary disagree about durability")
    value["harness_evidence"] = {
        "manifest": {key: manifest[key] for key in
                     ("durability", "sample_writer_admission", "worker_release") if key in manifest},
        "summary": {key: summary[key] for key in ("durability", "distributions") if key in summary},
    }
    samples = list(analyze.read_samples(result / "samples.csv"))
    if job.get("case_kind") == "hybrid_put":
        binding = receipt.get("nvmeof_target_evidence")
        targets = None
        if binding is not None:
            if not isinstance(binding, dict) or binding.get("path") != "nvme-targets.json":
                raise ValueError("invalid NVMe target evidence binding")
            artifact = directory / binding["path"]
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != binding.get("sha256"):
                raise ValueError("NVMe target evidence differs from its integrity binding")
            targets = json.loads(artifact.read_text())
        qualification = analyze.qualify_hybrid_put(
            job, summary["backend"], read_interval["observed"] if read_interval else None,
            targets, config["backend"]["options"].get("nvmeof_readers"), samples,
            receipt.get("status") == "completed"
            and receipt.get("measurement_evidence", {}).get("completion_gate_observed") is True,
            normalized, value.get("spdk_target_io"))
        value["hybrid_put_qualification"] = qualification
        if receipt.get("hybrid_put_qualification") is not None and qualification != receipt["hybrid_put_qualification"]:
            raise ValueError("hybrid put qualification differs from measurement evidence")
        if receipt.get("status") == "completed" and receipt.get("hybrid_put_qualification") is None:
            raise ValueError("completed hybrid put lacks measurement qualification")
    if not samples:
        if (job.get("case_kind") in ("interference", "media_read", "native_media_read")
                or (job.get("case_kind") == "live_read" and isinstance(job.get("media"), dict))) and receipt.get("status") == "completed":
            raise ValueError("completed focused case lacks measurement samples")
        return {**value, "backend": summary["backend"],
                "metrics_unavailable": "validated execution has no observed measurement calls"}
    if (job.get("case_kind") in ("media_read", "native_media_read")
            or (job.get("case_kind") == "live_read" and isinstance(job.get("media"), dict))):
        media = value["focused_evidence"].get("media_evidence")
        if media is None:
            raise ValueError("media evidence unavailable")
        validate_media_sample_count(media, samples, config)
    totals = analyze.aggregate(samples)
    if (job.get("case_kind") in ("media_read", "sealed_knee")
            and job.get("backend") in NVME_BACKENDS):
        successful_reads = sum(row["operation"] == "read" and row["outcome"] in ("ok", "success")
                               for row in samples)
        value["sealed_read_accounting"] = analyze.qualify_sealed_read_accounting(
            value.get("nvmeof_measurement_qualification"), successful_reads)
    if job.get("case_kind") == "interference":
        interference = value["focused_evidence"].get("interference_result")
        if interference is None:
            raise ValueError("interference stream evidence unavailable")
        totals["populations"] = analyze.reconcile_interference_samples(samples, interference)
        route = analyze.qualify_interference_route(
            job, summary["backend"], manifest,
            read_interval["observed"] if read_interval else None,
            value.get("nvmeof_measurement_qualification"), interference)
        value["interference_route_qualification"] = route
        interference["qualification"]["qualified"] &= route["qualified"]
        interference["qualification"]["reasons"].extend(route["reasons"])
        interference["qualification"]["notes"] = route["notes"]
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
    if job.get("case_kind") == "interference":
        totals["planned_measurement_calls"] = None
        totals["measurement_rows_complete"] = interference["measurement_drained"]
        totals["measurement_operation_ceiling"] = config["phases"]["measurement_operations"]
    totals["measurement_timeline"] = analyze.measurement_timeline(samples)
    traces = value.get("nvmeof_trace_evidence")
    if isinstance(traces, list):
        ratios = []
        for host in traces:
            if not isinstance(host, dict):
                continue
            for trace in host.get("traces", []):
                observed = trace.get("summary") or {}
                for kind in ("nvmet", "block"):
                    stage = (observed.get("stages") or {}).get(kind) or {}
                    count = stage.get("count")
                    complete = (host.get("complete") is True and observed.get("available") is True
                                and observed.get("overrun_or_dropped") == 0
                                and all(stage.get(field) == 0 for field in
                                        ("unmatched_start", "unmatched_completion", "ambiguous")))
                    ratios.append({"owner_index": host.get("index"), "export_uuid": trace.get("export_uuid"),
                        "event": kind, "paired_events": count if type(count) is int else None,
                        "measurement_api_calls": len(samples),
                        "events_per_api_call": count / len(samples) if complete and type(count) is int else None,
                        "complete": complete,
                        "scope": "collector-window pairs divided by measured API calls; populations differ, "
                                 "block events are device-wide and can be duplicated across export collectors; "
                                 "not per-call attribution or NAND latency"})
        value["trace_event_ratios"] = ratios
    device = value.get("cold_device_evidence")
    device_rows = list(enumerate(device, start=-1)) if isinstance(device, list) else [(-1, device)] if isinstance(device, dict) else []
    if device_rows:
        ratios = value.setdefault("trace_event_ratios", [])
        for index, observed in device_rows:
            if not isinstance(observed, dict):
                continue
            count = observed.get("read_issues")
            complete = (observed.get("available") is True and observed.get("lost") == 0
                        and observed.get("truncated") is False)
            ratios.append({"owner_index": index, "event": "block_read_issue",
                "observed_events": count if type(count) is int else None,
                "measurement_api_calls": len(samples),
                "events_per_api_call": count / len(samples) if complete and type(count) is int else None,
                "complete": complete,
                "scope": "device-wide collector window versus API calls; overlapping gates and background I/O prevent per-call attribution"})
    for population in totals["populations"]:
        selected = [row for row in samples if row["operation"] == population["operation"]
                    and row["outcome"] == population["outcome"]
                    and ("stream" not in population or row.get("stream") == population["stream"])]
        for kind in ("api", "response", "scheduling"):
            values = [row[kind + "_ns"] for row in selected if row[kind + "_ns"] is not None]
            population[kind + "_mean_ns"] = statistics.mean(values) if values else None
            population[kind + "_min_ns"] = min(values) if values else None
            population[kind + "_stddev_ns"] = statistics.pstdev(values) if values else None
        population["tail_warning"] = "p99 descriptive below 10000 calls; p999 descriptive below 100000 calls"
    if (job.get("case_kind") in ("media_read", "native_media_read")
            or (job.get("case_kind") == "live_read" and isinstance(job.get("media"), dict))):
        media = value["focused_evidence"]["media_evidence"]
        value["media_qualification"] = analyze.qualify_media_target(media, value.get("target_io_qualification"))
    return {**value, "backend": summary["backend"], "metrics": totals}



def qualify_history_prewarm(row, baseline, root):
    """Compare one admitted prewarm case with its matched cold baseline."""
    media = row.get("focused_evidence", {}).get("media_evidence")
    base_media = baseline.get("focused_evidence", {}).get("media_evidence") if baseline else None
    reasons = []
    def add(reason):
        if reason not in reasons:
            reasons.append(reason)
    def delta(evidence):
        if not isinstance(evidence, dict):
            return None
        before, after = evidence.get("history_loads_before"), evidence.get("history_loads_after")
        if type(before) is not int or type(after) is not int or not 0 <= before <= after < 2**64:
            return None
        return after - before
    if baseline is None or baseline.get("status") != "completed" or not isinstance(base_media, dict):
        add("history_baseline_missing")
    elif base_media.get("status") != "qualified" or base_media.get("measurement_drained") is not True:
        add("history_baseline_unqualified")
    warm_delta, baseline_delta = delta(media), delta(base_media)
    if warm_delta is None or (isinstance(base_media, dict) and baseline_delta is None):
        add("history_counter_unavailable")
    ratio = warm_delta / baseline_delta if warm_delta is not None and baseline_delta else None
    if baseline_delta == 0:
        add("history_baseline_zero")
    elif ratio is not None and warm_delta * 10 > baseline_delta:
        add("history_delta_above_threshold")
    if row.get("status") != "completed" or not isinstance(media, dict):
        add("cold_evidence_unavailable")
    else:
        for reason in media.get("reason_codes", []):
            if reason not in ("history_baseline_compared_in_report", "history_counter_unavailable",
                              "cold_read_ratio_below_threshold"):
                add(reason)
        cold = media.get("read_ops_per_success")
        if type(cold) not in (int, float) or not cold >= 0.9:
            add("cold_ratio_below_threshold")
        gate = row.get("measurement_evidence") or {}
        if (media.get("measurement_drained") is not True
                or gate.get("gate_released") is not True
                or gate.get("completion_gate_observed") is not True):
            add("cold_evidence_unavailable")
        devices = media.get("owner_devices")
        if not isinstance(devices, list) or not devices:
            add("cold_evidence_unavailable")
        try:
            analyze._verify_media_snapshots(
                Path(root).resolve() / "cases" / row["case_id"],
                {"measurement_evidence": gate}, media)
        except (OSError, ValueError, KeyError, TypeError):
            add("cold_evidence_unavailable")
    return {"status": "qualified" if not reasons else "unqualified", "reason_codes": reasons,
            "baseline_case_id": baseline.get("case_id") if baseline else None,
            "prewarm_history_load_delta": warm_delta, "baseline_history_load_delta": baseline_delta,
            "history_delta_ratio": ratio, "history_ratio_max": 0.10}


def write_csv_report(root, results):
    planned_repeats = sorted({row["job"]["repeat"] for row in results})
    repetition_mode = "single" if len(planned_repeats) == 1 else "multiple"
    fields = ["case_id", "status", "backend", "ack_contract", "durability_tier", "native_durability", "nvmeof_target", "requested_target_cache", "cohort", "profile", "repeat",
              "planned_independent_repetitions", "repetition_mode", "timing_stddev_scope", "value_bytes", "keys",
              "concurrency", "offered_ops_per_second", "observed_calls", "planned_calls",
              "measurement_rows_complete", "successful_calls", "unsuccessful_calls", "elapsed_seconds",
              "case_success_calls_per_second", "case_success_logical_keys_per_second", "case_success_value_mib_per_second",
              "operation", "outcome", "population_calls", "population_logical_keys",
              "acknowledgement", "consistency", "replication_factor", "measurement_boundary",
              "get_many_strategy", "batch_atomicity", "observed_topology", "effective_configuration",
              "resource_evidence", "read_path_qualification"]
    fields += ["case_kind", "stream", "media_status", "media_read_ops_per_success", "media_reason_codes",
               "focused_evidence", "interference_route_qualification", "history_baseline_case_id",
               "prewarm_history_load_delta", "baseline_history_load_delta", "history_delta_ratio", "history_ratio_max",
               "target_io", "target_io_qualification", "nvme_completions", "successful_reads",
               "accounting_gap", "read_rpc_count", "sealed_accounting_notes"]
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
            row["ack_contract"] = ack_contract.selected(job["backend"], job.get("ack_contract", "synced"))
            row["durability_tier"] = durability_tier(job)
            row["native_durability"] = job.get("native_durability") if job["backend"] == "native" else None
            row["backend"] = comparison_backend(job)
            row["nvmeof_target"] = job.get("nvmeof_target", "kernel" if job["backend"] in NVME_BACKENDS else None)
            row["requested_target_cache"] = (
                "direct_kernel_buffered_io_0_no_target_pagecache" if job.get("target_io") == "direct" else
                "direct_file_bdev_no_OS_pagecache" if row["nvmeof_target"] == "spdk" else
                "buffered_file_page_cache" if row["nvmeof_target"] == "kernel" else None)
            row.update(case_id=result["case_id"], status=result["status"], value_bytes=job["value_size"],
                       error=result.get("error"), metrics_unavailable=result.get("metrics_unavailable"))
            evidence = result.get("focused_evidence", {})
            media = evidence.get("media_evidence", {})
            row.update(case_kind=job.get("case_kind"), media_status=media.get("status"),
                       media_read_ops_per_success=media.get("read_ops_per_success"),
                       media_reason_codes=analyze.canonical(media.get("reason_codes")))
            verdict = result.get("media_qualification")
            if verdict is not None:
                row.update(media_status=verdict["status"], media_reason_codes=analyze.canonical(verdict["reason_codes"]))
            row["target_io"] = job.get("target_io")
            row["target_io_qualification"] = analyze.canonical(result.get("target_io_qualification"))
            history = result.get("history_prewarm_qualification")
            if history is not None:
                row.update(media_status=history["status"], media_reason_codes=analyze.canonical(history["reason_codes"]),
                           history_baseline_case_id=history["baseline_case_id"],
                           prewarm_history_load_delta=history["prewarm_history_load_delta"],
                           baseline_history_load_delta=history["baseline_history_load_delta"],
                           history_delta_ratio=history["history_delta_ratio"], history_ratio_max=history["history_ratio_max"])
            row["focused_evidence"] = analyze.canonical(evidence) if evidence else None
            row["interference_route_qualification"] = analyze.canonical(result.get("interference_route_qualification"))
            accounting = result.get("sealed_read_accounting") or result.get("interference_route_qualification") or {}
            for key in ("nvme_completions", "successful_reads", "accounting_gap", "read_rpc_count"):
                row[key] = accounting.get(key)
            row["sealed_accounting_notes"] = analyze.canonical(accounting.get("notes")) if accounting else None
            row.update(planned_independent_repetitions=len(planned_repeats), repetition_mode=repetition_mode,
                       timing_stddev_scope="within_case_population")
            if "read_path_qualification" in result:
                row["read_path_qualification"] = json.dumps(result["read_path_qualification"], sort_keys=True)
            backend = result.get("backend", {})
            if backend:
                row["ack_contract"] = analyze.admit_ack_contract(
                    job["backend"], backend, job.get("ack_contract", "synced"))
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
                                stream=population.get("stream"),
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


def write_diagnostic_summary(row):
    """Keep each observed sum with its own denominator; never infer tail/wire time."""
    evidence = (row.get("write_diagnostics") or {}).get("observed") or {}
    profile = evidence.get("profile") or {}
    observed = (evidence.get("selection") or {}).get("observed_samples")
    boundary_eligible = (evidence.get("phase_attempted") is True
                         and evidence.get("measurement_drained") is not False)
    eligible = (boundary_eligible
                and evidence.get("runtime_enabled") is True and evidence.get("error") is None
                and type(observed) is int and observed > 0)
    pairs = (
        ("client_exchange", "client_requests", "mixed_connection"),
        ("connection_control_queue", "connection_control_queue_samples", "mixed_connection"),
        ("connection_control_execution", "connection_control_execution_samples", "mixed_connection"),
        ("storage_append_wal", "storage_append_calls", "sampled_append_invocations"),
        ("storage_persist_wal_file", "storage_file_persist_calls", "sampled_file_syncs"),
    )
    clocks = []
    def observe(stage, values, clock, denominator, scope, available):
        if clock not in values and denominator not in values:
            return
        total, count = values.get(clock), values.get(denominator)
        available = (available and type(total) is int and total >= 0
                     and type(count) is int and count > 0)
        clocks.append({"stage": stage, "scope": scope, "available": available,
                       "sum_ns": total, "count": count,
                       "mean_ns": total / count if available else None})
    stages = profile.get("stages") or {}
    for stage, count, scope in pairs:
        observe(stage, stages, "write_profile_" + stage + "_ns",
                "write_profile_" + count, scope, eligible)
    if (evidence.get("backend") == "tidehunter_live_nvmeof"
            and evidence.get("runtime_observation_mode") == "live_nvmeof_phase_counters"
            and profile.get("scope") == "measurement_nvme_io_cumulative_delta"):
        nvme = stages.get("live_nvmeof") or {}
        for field in sorted(nvme):
            if field.endswith("_ns"):
                observe(field[:-3], nvme, field, field[:-3] + "_samples",
                        "measurement_nvme_io_cumulative_delta",
                        eligible and evidence.get("measurement_drained") is True)
    observe("checkpoint_wait", stages, "write_profile_checkpoint_wait_ns",
            "write_profile_checkpoint_completed", "sampled_checkpoint_submit_to_retire_post_gate_capture",
            boundary_eligible and evidence.get("error") is None
            and stages.get("write_profile_checkpoint_available") == 1)
    pre = evidence.get("pre_completion_gate") or {}
    if pre.get("available") is True:
        coordinator = pre.get("stages") or {}
        interval_scope = "coordinator_arm_through_pre_completion_gate"
    else:
        coordinator = (profile.get("coordinator_interval") or {}).get("stages") or {}
        interval_scope = "coordinator_arm_through_post_gate"
    wait = coordinator.get("write_storage_wait_ns")
    if wait is not None:
        valid_wait = boundary_eligible and type(wait) is int and wait >= 0
        clocks.append({"stage": "write_completion_wait", "scope": interval_scope,
                       "available": valid_wait, "sum_ns": wait, "count": None, "mean_ns": None})
    observe("foreground_admission_pressure", coordinator, "write_foreground_checkpoint_ns",
            "write_foreground_checkpoint_count", interval_scope,
            boundary_eligible)
    for phase in ("root_adoption", "owner_adoption", "obsolete_cleanup", "index_prepare"):
        prefix = "write_checkpoint_diagnostic_" + phase
        observe(phase, coordinator, prefix + "_ns", prefix + "_count",
                interval_scope + "_completion_charged_checkpoint",
                boundary_eligible
                and profile.get("coordinator_checkpoint_diagnostics_available") is True)
    return clocks


def write_diagnostic_report(rows):
    lines = ["", "## Write-stage diagnostics", "",
             "Independent diagnostic runs, not the primary API populations. Each mean uses its own "
             "observed sum/count. Mixed connection requests include checkpoint controls, not only appends; "
             "pre-gate snapshots exclude completion-gate background progress; post-gate "
             "snapshots and sampled checkpoint controls can include it. Completion-charged "
             "counters may straddle measurement boundaries; consult measurement_drained for failures. "
             "Scopes overlap; do not add rows, subtract them from API p99, or infer network RTT. "
             "Missing or unavailable clocks are not zero. Stage p99 is unavailable.", "",
             "These sampled runs are perturbative: reaching an append sample cap can block later "
             "appends while in-flight appends drain and any pending checkpoint control finishes "
             "before synchronous profile stop. Limit-zero primary runs do not use this gate; "
             "profiled API latencies are not primary comparison latencies.", "",
             "Live NVMe phase-window clocks cover completed client-side I/O events, not a first-N "
             "append sample: requested_limit enables the window and does not cap its population. "
             "Their clocks overlap and do not isolate wire, device or CPU time.", "",
             "The selected prefix may exclude later slow calls; there is no API-to-owner request join. "
             "cq_poll_gap_max_ns includes idle gaps; cq_max_poll_gap_ns covers pending TX only, "
             "not the whole outstanding RPC or receive wait.", "",
             "| Case | Scope | Stage | Count | Sum ns | Mean µs |",
             "|---|---|---|---:|---:|---:|"]
    for row in rows:
        for clock in write_diagnostic_summary(row):
            count = clock["count"] if clock["count"] is not None else "—"
            total = clock["sum_ns"] if clock["available"] else "—"
            mean = f"{clock['mean_ns'] / 1000:.3f}" if clock["mean_ns"] is not None else "—"
            lines.append(f"| {row['case_id']} | {clock['scope']} | {clock['stage']} | {count} | {total} | {mean} |")
    return lines


def report(root):
    root = Path(root)
    planned = json.loads((root / "matrix.json").read_text())
    results = []
    focused_ids = set()
    for job in planned["jobs"]:
        if "case_kind" in job:
            validate_focused_job(job)
            if job["case_id"] in focused_ids:
                raise ValueError("duplicate focused case ID")
            focused_ids.add(job["case_id"])
    for job in planned["jobs"]:
        directory = root / "cases" / job["case_id"]
        if (directory / "receipt.json").exists():
            try:
                row = summarize_case(directory)
                if "case_kind" in job and row["job"] != job:
                    raise ValueError("focused receipt job differs from planned job")
                results.append(row)
            except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
                receipt = json.loads((directory / "receipt.json").read_text())
                if receipt.get("status") == "completed":
                    raise
                results.append({"case_id": job["case_id"], "job": job, "status": "failed",
                                "error": str(receipt.get("error") or "case failed") +
                                         "; report admission: " + type(exc).__name__ + ": " + str(exc)})
        else:
            results.append({"case_id": job["case_id"], "job": job, "status": "not_run"})
    baselines = {}
    for row in results:
        job = row["job"]
        media = job.get("media", {})
        if (job.get("group") == "G1" and job.get("case_kind") == "media_read" and job.get("backend") == "tidehunter_verbs_native"
                and media.get("expected") == "cold" and media.get("history") == "normal"):
            key = (job["repeat"], job["value_size"])
            if key in baselines:
                raise ValueError("duplicate history comparison baseline")
            baselines[key] = row
    for row in results:
        job = row["job"]
        if job.get("case_kind") == "media_read" and job.get("media", {}).get("history") == "prewarm":
            row["history_prewarm_qualification"] = qualify_history_prewarm(
                row, baselines.get((job["repeat"], job["value_size"])), root)
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
        "planned_backends": sorted({comparison_backend(job) for job in planned["jobs"]}),
        "current_backend_coverage": set(BACKENDS) <= {job["backend"] for job in planned["jobs"]},
        "interpretation": ["Common RF1 API experiment, not equivalent HA/transaction/security guarantees.",
            "API-call latency for get_many is the complete ordered group, not latency per key.",
            "OS-cache eviction does not evict engine caches; qualified device intervals, when available, are retained separately.",
            "Live SPDK target direct-file I/O bypasses the target OS page cache; device and controller caches may remain.",
            "TideHunter native owner acknowledgement is a page-cache write with background fsync; compare only to a matching page-cache acknowledgement contract.",
            "Valkey GET is memory-resident even after OS-cache eviction; AOF is write acknowledgment, not a value-WAL-backed read. Synced uses appendfsync always; periodic_sync uses everysec; os_buffered uses no. Neither relaxed mode proves a per-case page-cache write; Valkey page_cache is unsupported.",
            "No Sui checkpoint/application or sustainable infinite-aging claim."]}
    if focused_ids:
        value["focused_coverage"] = {
            field: {name: sum(job.get(field) == name for job in planned["jobs"])
                    for name in sorted({job[field] for job in planned["jobs"] if field in job})}
            for field in ("group", "case_kind")}
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
            lines.append(f"| {row['case_id']} | {comparison_backend(row['job'])} | {row['job']['value_size']} | {row['job']['cohort']} | {row['status']} | — | — | — | — | — | — | — |")
            continue
        for population in row["metrics"]["populations"]:
            def us(value):
                return f"{value / 1000:.3f}" if value is not None else "—"
            lines.append(f"| {row['case_id']} ({row['status']}) | {comparison_backend(row['job'])} | {row['job']['value_size']} | {row['job']['cohort']} | {population['operation']}/{population['outcome']} | {population['count']} | {row['metrics']['success_calls_per_second']:.2f} | {us(population['api_mean_ns'])} | {us(population['api_ns']['p50'])} | {us(population['api_ns']['p95'])} | {us(population['api_ns']['p99'])} | {us(population['response_ns']['p99'])} |")
    if any(row["job"].get("case_kind") for row in results):
        lines.extend(["", "## Focused evidence",
                      "Media verdicts and reason codes are retained per case in comparison.csv and comparison.json.",
                      "NVMe-oF fio measures the buffered target path, not the SSD floor.",
                      "Interference write routing is configured-route evidence, not an independent append counter.",
                      "Each stream and repetition remains a separate population.", ""])
        lines.extend(["| Media case | Repeat | Value bytes | Target I/O | Verdict |",
                      "|---|---:|---:|---|---|"])
        for row in results:
            if row["job"].get("case_kind") == "media_read":
                verdict = row.get("history_prewarm_qualification") or row.get("media_qualification") or {}
                lines.append(f"| {row['case_id']} | {row['job']['repeat']} | {row['job']['value_size']} | "
                             f"{row['job']['target_io']} | {verdict.get('status', 'unavailable')} |")
        lines.append("")
        lines.extend(["### Warm-history comparison", "",
                      "The reviewed maximum history-load delta ratio is 0.10; this is not a measurement.", "",
                      "| Case | Repeat | Value bytes | Baseline | Prewarm delta | Baseline delta | Ratio | Verdict | Reasons |",
                      "|---|---:|---:|---|---:|---:|---:|---|---|"])
        for row in results:
            comparison = row.get("history_prewarm_qualification")
            if comparison is None:
                continue
            def shown(value):
                return "—" if value is None else str(value)
            lines.append(f"| {row['case_id']} | {row['job']['repeat']} | {row['job']['value_size']} | "
                         f"{shown(comparison['baseline_case_id'])} | {shown(comparison['prewarm_history_load_delta'])} | "
                         f"{shown(comparison['baseline_history_load_delta'])} | {shown(comparison['history_delta_ratio'])} | "
                         f"{comparison['status']} | {'; '.join(comparison['reason_codes']) or '—'} |")
        lines.append("")
    lines.extend(["", "Quantiles are exact nearest-rank raw-sample quantiles, not histogram bucket bounds. Tail precision depends on population size. Compare only matching size/workload/cohort/acknowledgement; consult comparison.json for full configuration, per-repeat logical-key throughput, bandwidth, errors and additional quantiles.", ""])
    lines.extend(["", "## Observed write transport and acknowledgement", "",
                  "Compare transport timings only after checking security and acknowledgement; missing metadata is unknown, not matched.", "",
                  "| Case | Transport | Security | Acknowledgement |", "|---|---|---|---|"])
    for row in results:
        if row["job"].get("cohort") != "write_stage_diagnostics":
            continue
        backend = row.get("backend") or {}
        configuration = backend.get("configuration") or {}
        def shown(value):
            return str(value).replace("|", "\\|").replace("\n", " ") if value is not None else "—"
        lines.append(f"| {row['case_id']} | {shown(configuration.get('transport'))} | "
                     f"{shown(configuration.get('transport_security'))} | {shown(backend.get('acknowledgement'))} |")
    lines.extend(read_diagnostic_report(results))
    lines.extend(write_diagnostic_report(results))
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
    plan.add_argument("--mode", choices=("standard", "read-tier", "read-tier-focused", "nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof", "focused"), default="standard")
    plan.add_argument("--selection", choices=FOCUSED_SELECTIONS, default="recommended")
    plan.add_argument("--write-profile-limit", type=int,
                      help="diagnostic-only 1KiB write cohort for native/Tidehunter; leaves normal plans unprofiled")
    plan.add_argument("--read-profile-limit", type=int,
                      help="diagnostic-only representative read cohorts for distributed Tidehunter")
    plan.add_argument("--write-timeline-limit", type=int,
                      help="independent Tidehunter write timeline cohorts; legacy profiler disabled")
    plan.add_argument("--valkey-profile-limit", type=int,
                      help="independent libvalkey scalar and batch diagnostic cohorts")
    summary = commands.add_parser("report")
    summary.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "plan":
        if args.mode == "focused":
            if args.backends is not None or any(value is not None for value in
                    (args.write_profile_limit, args.read_profile_limit, args.write_timeline_limit, args.valkey_profile_limit)):
                parser.error("focused mode does not admit backend or diagnostic overrides")
            jobs = make_focused_matrix(args.selection)
            value = {"schema_version": 1, "spec": {"mode": "focused", "selection": args.selection,
                                                     "direct_control_arm": True}, "jobs": jobs}
            local.dump(args.output, value)
            print(json.dumps({"planned_cases": len(jobs), "groups": focused_group_counts(args.selection),
                              "job_selection_semantics": "minimum campaign selection containing the job"}))
            return 0
        spec = json.loads(args.spec.read_text())
        spec["mode"] = args.mode
        if any(value is not None for value in (args.write_profile_limit, args.read_profile_limit,
                                                args.write_timeline_limit, args.valkey_profile_limit)):
            diagnostics = dict(spec.get("diagnostics", {}))
            if args.write_profile_limit is not None:
                diagnostics["write_profile_limit"] = args.write_profile_limit
            if args.read_profile_limit is not None:
                diagnostics["read_profile_limit"] = args.read_profile_limit
            if args.write_timeline_limit is not None:
                diagnostics["write_timeline_limit"] = args.write_timeline_limit
            if args.valkey_profile_limit is not None:
                diagnostics["valkey_profile_limit"] = args.valkey_profile_limit
            spec["diagnostics"] = diagnostics
        supported = set(BACKENDS) | PORTABLE_BACKENDS | (NVME_BACKENDS if args.mode in ("nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof") else set())
        configured = select_backends(None, spec.get("backends"), supported)
        if args.mode in ("nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof") and args.backends is not None:
            # Provider-specific workflow admission remains separate.
            configured += sorted((NVME_BACKENDS | {"tidehunter_verbs", "tidehunter_verbs_native"}).difference(configured))
        spec["backends"] = select_backends(args.backends, configured, supported)
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
