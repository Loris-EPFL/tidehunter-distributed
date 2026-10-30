#!/usr/bin/env python3
"""Expose one explicit campaign selection; never discover processes or invent history."""
import argparse
from http.server import BaseHTTPRequestHandler
import json
import math
import os
from pathlib import Path
import re

import exporter

MAX_SELECTION = 1024 * 1024
MAX_CGROUP_FILE = 256 * 1024
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
METRIC = re.compile(r"[A-Za-z_:][A-Za-z0-9_:]*")
LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*("(?:[^"\\]|\\.)*")')
QUANTILES = {"p50": "0.5", "p95": "0.95", "p99": "0.99", "p999": "0.999", "max": "1"}
COORDINATOR_INTERVAL_COUNTERS = {
    "write_storage_wait_ns", "write_publication_ns", "write_dispatch_ns", "write_preparation_admission_ns",
    "write_checkpoint_ns", "write_checkpoint_count", "write_foreground_checkpoint_ns",
    "write_foreground_checkpoint_count", "write_background_checkpoint_ns",
    "write_background_checkpoint_suffix_batches", "write_durable_receipt_publication_blocked_ns",
    "write_durable_receipt_publication_blocked_batches", "write_append_requests", "write_direct_appends",
    "write_durable_batches", "write_concurrent_admitted", "write_concurrent_completed",
    "write_out_of_prefix_published_batches", "write_dependency_wait_batches"}


def absolute(value):
    if not isinstance(value, str) or not Path(value).is_absolute() or ".." in Path(value).parts:
        raise ValueError("expected an explicit absolute path without traversal")
    return value


def token(value):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise ValueError("invalid bounded metric identity")
    return value


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError("expected a finite nonnegative measurement")
    return value


def count(value):
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("expected a nonnegative count")
    return value


def read_json(path):
    path = Path(absolute(str(path)))
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        payload = exporter.read_regular(directory, path.name)
    finally:
        os.close(directory)
    if len(payload) > MAX_SELECTION:
        raise ValueError("selection exceeds the bounded size")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("selection must be an object")
    return value


def context(run_id, job):
    labels = {"run_id": token(run_id), "case_id": "idle"}
    if job is None:
        return labels
    if not isinstance(job, dict):
        raise ValueError("job must be an object or null")
    if count(job["value_size"]) == 0:
        raise ValueError("value size must be positive")
    labels.update(case_id=token(job["case_id"]), requested_backend=token(job["backend"]),
                  profile=token(job["profile"]), cohort=token(job["cohort"]),
                  value_bytes=str(count(job["value_size"])), repeat=str(count(job["repeat"])))
    return labels


def label_text(labels):
    return ",".join(key + "=" + json.dumps(str(value), ensure_ascii=True) for key, value in labels.items())


def sample(name, value, labels):
    number(value)
    return name + "{" + label_text(labels) + "} " + str(value) + "\n"


def scoped(payload, labels):
    """Add one atomic selection identity to classic Prometheus exposition.

    Labels are parsed rather than split at commas or braces inside quoted values.
    A source cannot impersonate the campaign's selection identity.
    """
    output = []
    for line in payload.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            output.append(line)
            continue
        matched = METRIC.match(line)
        if matched is None:
            raise ValueError("invalid metric name")
        end = matched.end()
        original = ""
        if line[end:end + 1] == "{":
            start = end + 1
            cursor = start
            names = set()
            while cursor < len(line) and line[cursor] != "}":
                item = LABEL.match(line, cursor)
                if item is None or item.group(1) in names or item.group(1) in labels:
                    raise ValueError("invalid, duplicate or reserved context label")
                names.add(item.group(1))
                cursor = item.end()
                if line[cursor:cursor + 1] == ",":
                    cursor += 1
                    if line[cursor:cursor + 1] == "}":
                        raise ValueError("trailing metric label separator")
                elif line[cursor:cursor + 1] != "}":
                    raise ValueError("invalid metric label separator")
            if cursor >= len(line):
                raise ValueError("unterminated metric labels")
            original = line[start:cursor]
            end = cursor + 1
        if not line[end:end + 1].isspace():
            raise ValueError("metric lacks a separated value")
        output.append(line[:matched.end()] + "{" + (original + "," if original else "")
                      + label_text(labels) + "}" + line[end:])
    return ("\n".join(output) + "\n").encode()


def read_path_metrics(evidence, labels):
    if evidence is None:
        return ""
    if not isinstance(evidence, dict):
        raise ValueError("read evidence must be an observed object")
    labels = labels | {"scope": "adapter_lifetime"}
    result = ""
    coordinator = evidence.get("coordinator")
    if isinstance(coordinator, dict):
        local = labels | {"component": "coordinator"}
        result += sample("kv_case_read_evidence_available", 1, local)
        for source, path in (("hits", "cache_hits"), ("misses", "cache_misses")):
            if source in coordinator.get("cache", {}):
                result += sample("kv_case_read_path_calls", count(coordinator["cache"][source]),
                                 local | {"path": path})
        if "read_rpc_count" in coordinator:
            result += sample("kv_case_read_path_calls", count(coordinator["read_rpc_count"]),
                             local | {"path": "read_rpc_count"})
    owner = evidence.get("owner_io")
    if not isinstance(owner, dict):
        return result
    families = (("owner", "available", "owner_read_executor_stats",
                 ("cache_hits", "cache_misses", "wal_reads", "async_read_submissions",
                  "resident_read_successes", "errors")),
                ("resident", "resident_available", "resident_read_stats",
                 ("rma_hits", "rma_errors", "fallback_missing", "fallback_capacity",
                  "message_read_requests", "grant_candidates", "grant_unavailable")))
    for component, availability, field, keys in families:
        available = owner.get(availability)
        if not isinstance(available, bool):
            continue
        local = labels | {"component": component}
        result += sample("kv_case_read_evidence_available", int(available), local)
        if not available:
            continue
        rows = owner.get(field)
        if not isinstance(rows, list) or len(rows) > 5:
            raise ValueError("invalid bounded owner counter evidence")
        partitions = set()
        for row in rows:
            partition = count(row["partition"])
            if partition > 4 or partition in partitions or not isinstance(row["stats"], dict):
                raise ValueError("invalid or duplicated owner counter partition")
            partitions.add(partition)
            for path in keys:
                if path in row["stats"]:
                    result += sample("kv_case_read_path_calls", count(row["stats"][path]),
                                     local | {"partition": str(partition), "path": path})
    return result


def measurement_read_metrics(evidence, labels):
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or evidence.get("scope") != "measurement_boundary":
        raise ValueError("invalid measurement read evidence")
    deltas = evidence.get("deltas")
    if not isinstance(deltas, dict):
        raise ValueError("measurement read deltas missing")
    labels = labels | {"scope": "measurement_boundary"}
    result = ""
    if deltas.get("available") is False:
        return sample("kv_case_read_evidence_available", 0, labels | {"component": "measurement"})
    def emit(row, local):
        if not isinstance(row, dict) or type(row.get("available")) is not bool:
            raise ValueError("invalid measurement read component")
        text = sample("kv_case_read_evidence_available", int(row["available"]), local)
        if row["available"]:
            counters = row.get("counters")
            if not isinstance(counters, dict):
                raise ValueError("measurement read counters missing")
            for name, value in counters.items():
                text += sample("kv_case_read_path_calls", count(value), local | {"path": token(name)})
        return text
    result += emit(deltas.get("coordinator"), labels | {"component": "coordinator"})
    components = ("owner_executor", "client_resident")
    if "owner_grants" in deltas:
        components += ("owner_grants",)
    if "owner_output_abandoned" in deltas:
        components += ("owner_output_abandoned",)
    for component in components:
        group = deltas.get(component)
        if not isinstance(group, dict) or type(group.get("available")) is not bool:
            raise ValueError("invalid measurement partition group")
        local = labels | {"component": component}
        result += sample("kv_case_read_evidence_available", int(group["available"]), local)
        if group["available"]:
            partitions = group.get("partitions")
            if not isinstance(partitions, dict) or len(partitions) > 128:
                raise ValueError("invalid measurement partitions")
            for partition, row in partitions.items():
                if not partition.isascii() or not partition.isdecimal() or str(int(partition)) != partition:
                    raise ValueError("invalid partition identity")
                result += emit(row, local | {"partition": partition})
    lanes = deltas.get("client_resident_lanes")
    if lanes is not None:
        if not isinstance(lanes, dict) or type(lanes.get("available")) is not bool:
            raise ValueError("invalid lane evidence")
        local = labels | {"component": "client_resident_lane"}
        result += sample("kv_case_read_evidence_available", int(lanes["available"]), local)
        if lanes["available"]:
            partitions = lanes.get("partitions")
            if not isinstance(partitions, dict) or len(partitions) > 5:
                raise ValueError("invalid lane evidence partitions")
            for partition, group in partitions.items():
                if (not isinstance(partition, str) or not partition.isascii() or not partition.isdecimal()
                        or len(partition) > 10 or str(int(partition)) != partition or int(partition) > 0xffffffff
                        or not isinstance(group, dict)
                        or type(group.get("available")) is not bool):
                    raise ValueError("invalid lane evidence partition")
                result += sample("kv_case_read_evidence_available", int(group["available"]),
                                 local | {"partition": partition})
                if not group["available"]:
                    continue
                rows = group.get("lanes")
                if not isinstance(rows, dict) or len(rows) > 8:
                    raise ValueError("invalid lane evidence rows")
                for lane, row in rows.items():
                    if lane not in {str(index) for index in range(8)}:
                        raise ValueError("invalid lane identity")
                    result += emit(row, local | {"partition": partition, "lane": lane})
    for boundary in ("start", "end"):
        snapshot = evidence.get(boundary)
        group = snapshot.get("client_resident_lanes") if isinstance(snapshot, dict) else None
        if not isinstance(group, dict) or group.get("available") is not True:
            continue
        partitions = group.get("partitions")
        if not isinstance(partitions, dict) or len(partitions) > 5:
            raise ValueError("invalid lane session partitions")
        for partition, partition_row in partitions.items():
            if (not isinstance(partition, str) or not partition.isascii() or not partition.isdecimal()
                    or len(partition) > 10 or str(int(partition)) != partition or int(partition) > 0xffffffff
                    or not isinstance(partition_row, dict)):
                raise ValueError("invalid lane session partition")
            rows = partition_row.get("lanes")
            if not isinstance(rows, dict) or len(rows) > 8:
                raise ValueError("invalid lane session rows")
            for lane, row in rows.items():
                if lane not in {str(index) for index in range(8)} or not isinstance(row, dict):
                    raise ValueError("invalid lane session identity")
                present = row.get("session_present")
                if present is not None and type(present) is not bool:
                    raise ValueError("invalid lane session availability")
                local = labels | {"boundary": boundary, "partition": partition, "lane": lane}
                result += sample("kv_case_read_lane_session_available", int(present is not None), local)
                if present is not None:
                    result += sample("kv_case_read_lane_session_present", int(present), local)
    response = deltas.get("owner_response")
    if response is not None:
        if not isinstance(response, dict) or type(response.get("available")) is not bool:
            raise ValueError("invalid owner response evidence")
        local = labels | {"component": "owner_response"}
        result += sample("kv_case_read_evidence_available", int(response["available"]), local)
        if response["available"]:
            partitions = response.get("partitions")
            if not isinstance(partitions, dict) or len(partitions) > 128:
                raise ValueError("invalid owner response partitions")
            for partition, stages in partitions.items():
                if not partition.isascii() or not partition.isdecimal() or str(int(partition)) != partition:
                    raise ValueError("invalid owner response partition")
                if not isinstance(stages, dict) or set(stages) != {
                        "read_response_encode", "read_output_queue", "read_output_submit", "read_response_retire",
                        "read_grant_lookup", "read_grant_reservation", "read_grant_envelope",
                        "read_grant_registration", "read_grant_recheck_publication"}:
                    raise ValueError("invalid owner response stages")
                for stage, row in stages.items():
                    dimensions = local | {"partition": partition, "stage": stage}
                    if not isinstance(row, dict) or type(row.get("available")) is not bool:
                        raise ValueError("invalid owner response stage")
                    result += sample("kv_case_read_boundary_stage_available", int(row["available"]), dimensions)
                    if row["available"]:
                        counters = row.get("counters")
                        if not isinstance(counters, dict) or set(counters) != {"ns", "count", "errors"}:
                            raise ValueError("invalid owner response stage counters")
                        for name in ("ns", "count", "errors"):
                            count(counters[name])
                        result += sample("kv_case_read_boundary_stage_seconds", counters["ns"] / 1e9,
                                         dimensions | {"statistic": "sum"})
                        for name in ("count", "errors"):
                            result += sample("kv_case_read_boundary_stage_count", counters[name],
                                             dimensions | {"counter": name})
                        if counters["count"]:
                            result += sample("kv_case_read_boundary_stage_seconds",
                                             counters["ns"] / counters["count"] / 1e9,
                                             dimensions | {"statistic": "mean"})
        end_group = evidence.get("end", {}).get("owner_grants", {})
        if isinstance(end_group, dict) and end_group.get("available") is True:
            end_partitions = end_group.get("partitions")
            if not isinstance(end_partitions, dict) or len(end_partitions) > 128:
                raise ValueError("invalid owner lifetime maxima")
            for partition, row in end_partitions.items():
                if (not isinstance(partition, str) or not partition.isascii()
                        or not partition.isdecimal() or str(int(partition)) != partition
                        or not isinstance(row, dict)):
                    raise ValueError("invalid owner lifetime maximum partition")
                stages = {name: row.get(name) for name in
                          ("read_response_encode", "read_output_queue", "read_output_submit", "read_response_retire")}
                grant = row.get("read_grant")
                if isinstance(grant, dict):
                    stages.update({"read_grant_" + name: grant.get(name) for name in
                                   ("lookup", "reservation", "envelope", "registration", "recheck_publication")})
                for stage, value in stages.items():
                    if value is None:
                        continue
                    if not isinstance(value, dict):
                        raise ValueError("invalid owner lifetime stage")
                    samples = count(value.get("count"))
                    maximum = count(value.get("max_ns"))
                    if samples:
                        result += sample("kv_case_read_boundary_stage_seconds", maximum / 1e9,
                                         labels | {"scope": "node_lifetime_end", "component": "owner_response",
                                                   "partition": partition, "stage": stage, "statistic": "max"})
    gauge_fields = {
        "coordinator": ("retained_bytes", "metadata_bytes", "entries", "budget_bytes"),
        "owner_grants": ("source_bytes_live", "source_regions_live", "source_bytes_pending",
                         "source_regions_pending", "source_bytes_quarantined"),
        "client_resident": ("source_bytes_live", "source_regions_live", "source_bytes_quarantined",
                            "destination_slots_live", "handles_live", "grants_live"),
    }
    for boundary in ("start", "end"):
        snapshot = evidence.get(boundary)
        if not isinstance(snapshot, dict) or snapshot.get("available") is not True:
            continue
        for component, fields in gauge_fields.items():
            group = snapshot.get(component)
            if component == "coordinator":
                rows = {"aggregate": group} if isinstance(group, dict) else {}
            elif isinstance(group, dict) and group.get("available") is True:
                rows = group.get("partitions", {})
                if not isinstance(rows, dict) or len(rows) > 128:
                    raise ValueError("invalid read boundary gauges")
            else:
                continue
            for partition, row in rows.items():
                if not isinstance(row, dict):
                    raise ValueError("invalid read boundary gauge row")
                if partition != "aggregate" and (not isinstance(partition, str) or not partition.isascii()
                        or not partition.isdecimal() or str(int(partition)) != partition):
                    raise ValueError("invalid read boundary gauge partition")
                for field in fields:
                    if field in row and row[field] is not None:
                        result += sample("kv_case_read_boundary_gauge", count(row[field]),
                                         labels | {"boundary": boundary, "component": component,
                                                   "partition": partition, "gauge": field})
    return result


def cold_device_metrics(evidence, labels):
    """Per-host device-wide block observations; never promote tracefs candidates."""
    if evidence is None:
        return ""
    if not isinstance(evidence, list) or len(evidence) > 6:
        raise ValueError("invalid cold device hosts")
    if len(json.dumps(evidence, allow_nan=False, separators=(",", ":")).encode()) > 128 * 1024:
        raise ValueError("cold device evidence exceeds bound")

    def bounded(value, depth=0):
        if depth > 8:
            raise ValueError("cold device evidence nesting exceeds bound")
        if isinstance(value, dict):
            if len(value) > 128 or any(not isinstance(k, str) or not k or len(k) > 160 for k in value):
                raise ValueError("invalid cold device object")
            if {"raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns"} & value.keys():
                raise ValueError("raw cold device records are not terminal metrics")
            for child in value.values():
                bounded(child, depth + 1)
        elif isinstance(value, list):
            if len(value) > 32:
                raise ValueError("cold device list exceeds bound")
            for child in value:
                bounded(child, depth + 1)
        elif isinstance(value, str):
            if len(value) > 4096:
                raise ValueError("cold device text exceeds bound")
        elif value is not None and type(value) is not bool:
            if not isinstance(value, (int, float)) or isinstance(value, float) and not math.isfinite(value):
                raise ValueError("invalid cold device metadata number")

    bounded(evidence)
    seen, result = set(), ""
    for item in evidence:
        if (not isinstance(item, dict) or set(item) != {"host_index", "evidence"}
                or type(item["host_index"]) is not int or not -1 <= item["host_index"] <= 4
                or item["host_index"] in seen or not isinstance(item["evidence"], dict)):
            raise ValueError("invalid cold device host")
        host = item["host_index"]
        seen.add(host)
        row = item["evidence"]
        if type(row.get("available")) is not bool:
            raise ValueError("invalid cold device availability")
        backend = row.get("backend")
        if backend == "bpftrace-request-identity":
            backend = "request_identity"
        elif backend is None and "candidate_intervals_ns" in row:
            backend = "tracefs_candidate"
        elif backend is None:
            backend = "unknown"
        else:
            raise ValueError("invalid cold device backend")
        local = labels | {"scope": "collector_window_block_device_wide", "resource_host_index": str(host),
                          "trace_backend": backend}
        result += sample("kv_case_block_trace_available", int(row["available"]), local)
        if not row["available"]:
            continue
        for field in ("qualified", "truncated"):
            if type(row.get(field)) is not bool:
                raise ValueError("invalid cold device trace status")
            result += sample("kv_case_block_trace_" + field, int(row[field]), local)
        for field in ("read_issues", "read_completions", "matched_reads", "lost", "requeues",
                      "unmatched_issues", "unmatched_completions", "completion_errors",
                      "outstanding_overflow", "malformed_events", "partial_completions"):
            value = row.get(field)
            result += sample("kv_case_block_trace_counter_available", int(value is not None),
                             local | {"counter": field})
            if value is not None:
                result += sample("kv_case_block_trace_count", count(value), local | {"counter": field})
        for field in ("timestamp_unit_ns", "observed_timestamp_resolution_ns"):
            value = row.get(field)
            result += sample("kv_case_block_trace_timestamp_available", int(value is not None),
                             local | {"precision": field})
            if value is not None:
                if count(value) == 0:
                    raise ValueError("invalid block trace timestamp precision")
                result += sample("kv_case_block_trace_timestamp_seconds", value / 1e9,
                                 local | {"precision": field})
        interval = row.get("intervals_ns")
        if interval is not None:
            if backend != "request_identity" or not isinstance(interval, dict):
                raise ValueError("only request-identity intervals may be exported")
            required = {"count", "sum", "max", "mean", "population_stddev", "p50", "p95", "p99", "p999", "tail_warnings"}
            if set(interval) != required or not isinstance(interval["tail_warnings"], list):
                raise ValueError("invalid block interval distribution")
            n = count(interval["count"])
            total = count(interval["sum"])
            if row.get("matched_reads") != n:
                raise ValueError("block interval population differs from matched requests")
            for field in ("max", "mean", "population_stddev", "p50", "p95", "p99", "p999"):
                value = interval[field]
                if (n == 0) != (value is None):
                    raise ValueError("invalid block interval availability")
                if value is not None:
                    number(value)
            if n == 0 and total != 0:
                raise ValueError("empty block interval population has nonzero sum")
            usable = n > 0 and row.get("lost") == 0 and not row["truncated"]
            result += sample("kv_case_block_interval_available", int(usable), local)
            if usable:
                result += sample("kv_case_block_interval_count", n, local)
                result += sample("kv_case_block_interval_seconds", total / 1e9, local | {"statistic": "sum"})
                for field in ("max", "mean", "population_stddev", "p50", "p95", "p99", "p999"):
                    result += sample("kv_case_block_interval_seconds", interval[field] / 1e9,
                                     local | {"statistic": field})
        else:
            result += sample("kv_case_block_interval_available", 0, local)
    return result


def resource_interval_metrics(evidence, labels):
    """Terminal boundary deltas/gauges, never reconstructed live scrape rates."""
    if evidence is None:
        return ""
    if (not isinstance(evidence, dict) or set(evidence) != {"schema_version", "scope", "processes"}
            or type(evidence["schema_version"]) is not int or evidence["schema_version"] != 1
            or evidence["scope"] != "measurement_boundary_interval"
            or not isinstance(evidence["processes"], list) or len(evidence["processes"]) > 16):
        raise ValueError("invalid bounded resource interval schema")
    seconds = {"cpu_seconds", "cpu_throttled_seconds"}
    integers = {"cpu_periods", "cpu_throttled_periods", "io_read_bytes", "io_write_bytes",
                "memory_current_bytes", "memory_peak_bytes"}
    identities, result = set(), ""
    for row in evidence["processes"]:
        if not isinstance(row, dict):
            raise ValueError("resource process must be an object")
        host = row.get("host_index")
        if type(host) is not int or not -1 <= host <= 4:
            raise ValueError("invalid resource host index")
        role = token(row.get("process_role"))
        identity = (host, role)
        if identity in identities or type(row.get("available")) is not bool:
            raise ValueError("duplicate resource process or invalid availability")
        identities.add(identity)
        local = labels | {"scope": evidence["scope"], "resource_host_index": str(host), "process_role": role}
        result += sample("kv_case_resource_interval_available", int(row["available"]), local)
        required = {"host_index", "process_role", "available"}
        if not row["available"]:
            reason = row.get("reason")
            if (set(row) != required | {"reason"} or not isinstance(reason, str)
                    or not reason.strip() or len(reason) > 512):
                raise ValueError("unavailable resource evidence requires only a bounded reason")
            continue
        if (not required | {"elapsed_seconds"} <= set(row)
                or set(row) - required - seconds - integers - {"elapsed_seconds"}):
            raise ValueError("invalid available resource fields")
        elapsed = number(row["elapsed_seconds"])
        if elapsed <= 0:
            raise ValueError("resource interval elapsed time must be positive")
        if ("cpu_periods" in row and "cpu_throttled_periods" in row
                and count(row["cpu_throttled_periods"]) > count(row["cpu_periods"])):
            raise ValueError("throttled periods exceed observed periods")
        result += sample("kv_case_resource_interval_elapsed_seconds", elapsed, local)
        for field in sorted(seconds | integers):
            if field in row:
                value = count(row[field]) if field in integers else number(row[field])
                result += sample("kv_case_resource_interval_" + field, value, local)
    return result


def write_diagnostic_metrics(evidence, labels):
    """Bounded accumulated clocks/counts with independent explicit population scopes."""
    if evidence is None:
        return ""
    required = {"schema_version", "backend", "phase", "phase_attempted", "runtime_enabled",
                "runtime_observation_mode", "selection", "clock_scope", "overlap_semantics",
                "unsupported_stages", "profile", "error"}
    if not isinstance(evidence, dict) or set(evidence) != required:
        raise ValueError("invalid write diagnostic artifact fields")
    if len(json.dumps(evidence, allow_nan=False, separators=(",", ":")).encode()) > 64 * 1024:
        raise ValueError("write diagnostic artifact exceeds 64KiB")
    numeric_fields = 0

    def bounded_tree(value, depth=0):
        nonlocal numeric_fields
        if depth > 6:
            raise ValueError("write diagnostic nesting exceeds bound")
        if isinstance(value, dict):
            if len(value) > 512 or any(not isinstance(key, str) or not key or len(key) > 160 for key in value):
                raise ValueError("invalid bounded write diagnostic object")
            for child in value.values():
                bounded_tree(child, depth + 1)
        elif isinstance(value, list):
            if len(value) > 32:
                raise ValueError("write diagnostic list exceeds bound")
            for child in value:
                bounded_tree(child, depth + 1)
        elif isinstance(value, str):
            if len(value) > 2048:
                raise ValueError("write diagnostic string exceeds bound")
        elif value is not None and not isinstance(value, bool):
            count(value)
            numeric_fields += 1
            if numeric_fields > 512:
                raise ValueError("write diagnostic numeric field count exceeds bound")

    bounded_tree(evidence)
    if (type(evidence["schema_version"]) is not int or evidence["schema_version"] != 1
            or evidence["phase"] != "measurement" or type(evidence["phase_attempted"]) is not bool
            or type(evidence["runtime_enabled"]) is not bool
            or not isinstance(evidence["runtime_observation_mode"], str)
            or not evidence["runtime_observation_mode"]
            or any(not isinstance(evidence[field], str) or not evidence[field].strip()
                   for field in ("clock_scope", "overlap_semantics"))
            or not isinstance(evidence["unsupported_stages"], list)
            or any(not isinstance(stage, str) for stage in evidence["unsupported_stages"])
            or evidence["error"] is not None and not isinstance(evidence["error"], str)):
        raise ValueError("invalid write diagnostic phase, mode or status")
    backend = token(evidence["backend"])
    selection = evidence["selection"]
    if (not isinstance(selection, dict) or set(selection) != {
            "requested_limit", "observed_samples", "selection_policy", "phase_exclusions"}
            or not isinstance(selection["selection_policy"], str) or not selection["selection_policy"]
            or not isinstance(selection["phase_exclusions"], list)
            or any(not isinstance(phase, str) for phase in selection["phase_exclusions"])
            or not 0 < count(selection["requested_limit"]) <= 65536):
        raise ValueError("invalid write diagnostic selection")
    observed = count(selection["observed_samples"])
    profile = evidence["profile"]
    if profile is not None and not isinstance(profile, dict):
        raise ValueError("write diagnostic profile must be an object or null")
    available = (evidence["phase_attempted"] and evidence["runtime_enabled"] and observed > 0
                 and profile is not None and isinstance(profile.get("stages"), dict)
                 and bool(profile["stages"]) and evidence["error"] is None)
    local = labels | {"phase": "measurement", "scope": "sampled_prefix", "diagnostic_backend": backend}
    result = sample("kv_case_write_diagnostic_available", int(available), local)
    result += sample("kv_case_write_diagnostic_operations", selection["requested_limit"], local | {"population": "requested_limit"})
    result += sample("kv_case_write_diagnostic_operations", observed, local | {"population": "observed_samples"})
    if profile is None:
        return result
    stages = profile.get("stages")
    persistence = profile.get("value_wal_persistence")
    if ((stages is not None and not isinstance(stages, dict))
            or (persistence is not None and not isinstance(persistence, dict))):
        raise ValueError("write diagnostic stages must be bounded objects")
    counters = []

    def stage_tree(tree, component, depth=0):
        if depth > 4:
            raise ValueError("write diagnostic stage nesting exceeds bound")
        if tree.get("status") == "not_applicable":
            if set(tree) - {"status", "reason"}:
                raise ValueError("unsupported write diagnostic stage cannot claim counters")
            return
        for field, value in tree.items():
            token(field)
            if isinstance(value, dict):
                stage_tree(value, component + "." + field, depth + 1)
            elif isinstance(value, str) and field in ("status", "reason"):
                continue
            else:
                value = count(value)
                if field.endswith("_ns"):
                    counters.append(("kv_case_write_diagnostic_stage_seconds", value / 1e9,
                                     {"component": component, "stage": field}))
                elif field.endswith(("_calls", "_errors", "_samples", "_requests", "_retries", "_groups",
                                     "_receipts", "_publications", "_successes", "_operations", "_barriers",
                                     "_connections", "_count", "_batches", "_appends", "_admitted", "_completed")):
                    counters.append(("kv_case_write_diagnostic_stage_count", value,
                                     {"component": component, "counter": field}))

    if stages is not None:
        stage_tree(stages, "stages")
    if persistence is not None:
        stage_tree(persistence, "value_wal_persistence")
    if available:
        for metric, value, dimensions in counters:
            result += sample(metric, value, local | dimensions)
    coordinator = profile.get("coordinator_interval")
    coordinator_error = profile.get("coordinator_interval_error")
    if coordinator_error is not None:
        if not isinstance(coordinator_error, str) or not coordinator_error.strip() or coordinator is not None:
            raise ValueError("invalid coordinator interval error or conflicting evidence")
    if coordinator is not None:
        if (not isinstance(coordinator, dict) or set(coordinator) != {"scope", "clock_scope", "stages"}
                or coordinator["scope"] != "measurement_boundary_cumulative_delta"
                or not isinstance(coordinator["clock_scope"], str) or not coordinator["clock_scope"].strip()
                or not isinstance(coordinator["stages"], dict) or not coordinator["stages"]
                or set(coordinator["stages"]) - COORDINATOR_INTERVAL_COUNTERS):
            raise ValueError("invalid coordinator cumulative interval schema or counter whitelist")
        for value in coordinator["stages"].values():
            count(value)
        boundary_labels = local | {"scope": coordinator["scope"]}
        boundary_available = evidence["phase_attempted"]
        result += sample("kv_case_write_diagnostic_available", int(boundary_available), boundary_labels)
        if boundary_available:
            for field, value in sorted(coordinator["stages"].items()):
                if field.endswith("_ns"):
                    result += sample("kv_case_write_diagnostic_stage_seconds", value / 1e9,
                                     boundary_labels | {"component": "coordinator_interval", "stage": field})
                else:
                    result += sample("kv_case_write_diagnostic_stage_count", value,
                                     boundary_labels | {"component": "coordinator_interval", "counter": field})
    elif coordinator_error is not None:
        result += sample("kv_case_write_diagnostic_available", 0,
                         local | {"scope": "measurement_boundary_cumulative_delta"})
    return result


def read_diagnostic_metrics(evidence, labels):
    """Expose sampled read clocks only when their own observed population exists."""
    if evidence is None:
        return ""
    required = {"schema_version", "backend", "phase", "phase_attempted", "runtime_enabled",
                "runtime_observation_mode", "selection", "clock_scope", "overlap_semantics",
                "unsupported_stages", "profile", "error"}
    if not isinstance(evidence, dict) or set(evidence) != required:
        raise ValueError("invalid read diagnostic artifact fields")
    if len(json.dumps(evidence, allow_nan=False, separators=(",", ":")).encode()) > 256 * 1024:
        raise ValueError("read diagnostic artifact exceeds 256KiB")

    def bounded(value, depth=0):
        if depth > 10:
            raise ValueError("read diagnostic nesting exceeds bound")
        if isinstance(value, dict):
            if len(value) > 512 or any(not isinstance(k, str) or not k or len(k) > 160 for k in value):
                raise ValueError("invalid bounded read diagnostic object")
            for child in value.values():
                bounded(child, depth + 1)
        elif isinstance(value, list):
            if len(value) > 32:
                raise ValueError("read diagnostic list exceeds bound")
            for child in value:
                bounded(child, depth + 1)
        elif isinstance(value, str):
            if len(value) > 2048:
                raise ValueError("read diagnostic string exceeds bound")
        elif value is not None and not isinstance(value, bool):
            count(value)

    bounded(evidence)
    if (type(evidence["schema_version"]) is not int or evidence["schema_version"] != 1
            or evidence["phase"] != "measurement" or type(evidence["phase_attempted"]) is not bool
            or type(evidence["runtime_enabled"]) is not bool
            or not isinstance(evidence["runtime_observation_mode"], str)
            or not evidence["runtime_observation_mode"]
            or any(not isinstance(evidence[field], str) or not evidence[field].strip()
                   for field in ("clock_scope", "overlap_semantics"))
            or not isinstance(evidence["unsupported_stages"], list)
            or any(not isinstance(stage, str) for stage in evidence["unsupported_stages"])
            or evidence["error"] is not None and not isinstance(evidence["error"], str)):
        raise ValueError("invalid read diagnostic phase, mode or status")
    backend = token(evidence["backend"])
    selection = evidence["selection"]
    if (not isinstance(selection, dict) or set(selection) != {
            "requested_limit", "selected_operations", "completed_operations", "selection_policy", "phase_exclusions"}
            or not isinstance(selection["selection_policy"], str) or not selection["selection_policy"]
            or not isinstance(selection["phase_exclusions"], list)
            or any(not isinstance(phase, str) for phase in selection["phase_exclusions"])
            or not 0 < count(selection["requested_limit"]) <= 65536):
        raise ValueError("invalid read diagnostic selection")
    selected = count(selection["selected_operations"])
    completed = count(selection["completed_operations"])
    if completed > selected or selected > selection["requested_limit"]:
        raise ValueError("invalid read diagnostic population")
    profile = evidence["profile"]
    if profile is not None and not isinstance(profile, dict):
        raise ValueError("read diagnostic profile must be an object or null")
    local = labels | {"phase": "measurement", "scope": "sampled_prefix", "diagnostic_backend": backend}
    eligible = evidence["phase_attempted"] and evidence["runtime_enabled"] and evidence["error"] is None
    stages = profile.get("stages") if profile is not None else None
    if stages is not None and not isinstance(stages, dict):
        raise ValueError("invalid read diagnostic stages")
    counters = profile.get("counters") if profile is not None else None
    if counters is not None and not isinstance(counters, dict):
        raise ValueError("invalid read diagnostic counters")
    coverage = profile.get("coverage") if profile is not None else None
    if coverage is not None:
        if (not isinstance(coverage, dict) or set(coverage) != {
                "selected_remote_requests", "completed_remote_requests", "profiled_owner_responses", "reason"}
                or not isinstance(coverage["reason"], str)):
            raise ValueError("invalid read diagnostic coverage")
        for field in ("selected_remote_requests", "completed_remote_requests", "profiled_owner_responses"):
            count(coverage[field])
        if not (coverage["completed_remote_requests"] <= coverage["selected_remote_requests"]
                <= selection["requested_limit"]):
            raise ValueError("invalid read diagnostic coverage populations")
    def stage_group(group_stages, group_counters, selected_population=selected):
        if not isinstance(group_stages, dict) or not isinstance(group_counters, dict):
            raise ValueError("invalid read diagnostic stages or counters")
        observations = []
        for stage, value in group_stages.items():
            token(stage)
            if (not isinstance(value, dict)
                    or set(value) != {"available", "sum_ns", "count", "max_ns", "unavailable_reason"}
                    or type(value["available"]) is not bool
                    or value["unavailable_reason"] is not None and not isinstance(value["unavailable_reason"], str)):
                raise ValueError("invalid read diagnostic stage")
            for field in ("sum_ns", "count", "max_ns"):
                if value[field] is not None:
                    count(value[field])
            if value["available"]:
                if value["sum_ns"] is None or value["count"] is None or value["count"] == 0:
                    raise ValueError("available read stage requires observed samples")
                if selected_population == 0:
                    raise ValueError("read stage observations require selected operations")
                observations.append((stage, value))
            elif (value["count"] != 0 or value["sum_ns"] is not None or value["max_ns"] is not None
                  or not isinstance(value["unavailable_reason"], str) or not value["unavailable_reason"].strip()):
                raise ValueError("unavailable read stage cannot claim observations")
        for field, value in group_counters.items():
            token(field)
            count(value)
        return observations

    groups = [("aggregate", stage_group(stages or {}, counters or {}), counters or {})]
    owners = profile.get("owners", {}) if profile is not None else {}
    if not isinstance(owners, dict) or len(owners) > 5:
        raise ValueError("invalid read diagnostic owners")
    for partition, owner in sorted(owners.items()):
        if (not re.fullmatch(r"0|[1-9][0-9]{0,9}", partition)
                or int(partition) > 0xffffffff
                or not isinstance(owner, dict) or set(owner) != {"stages", "counters"}):
            raise ValueError("invalid read diagnostic owner partition")
        groups.append((partition, stage_group(owner["stages"], owner["counters"]), owner["counters"]))
    result = ""
    for partition, observations, _ in groups:
        result += sample("kv_case_read_diagnostic_available", int(bool(eligible and observations)),
                         local | {"partition": partition})
    for population in ("requested_limit", "selected_operations", "completed_operations"):
        result += sample("kv_case_read_diagnostic_operations", selection[population], local | {"population": population})
    if coverage is not None and eligible:
        for population in ("selected_remote_requests", "completed_remote_requests", "profiled_owner_responses"):
            result += sample("kv_case_read_diagnostic_operations", coverage[population], local | {"population": population})
    for partition, observations, group_counters in groups:
        population_eligible = eligible or (
            evidence["phase_attempted"] and evidence["runtime_enabled"]
            and profile is not None and profile.get("coordinator_resolution_valid") is True)
        if population_eligible:
            for field in ("read_profile_completed", "read_profile_errors",
                          "read_profile_failed_owner_profile_retained", "read_profile_failed_owner_profile_unavailable",
                          "read_profile_failed_owner_profile_not_applicable"):
                if field in group_counters:
                    result += sample("kv_case_read_diagnostic_operations", group_counters[field],
                                     local | {"partition": partition, "population": field})
        if not eligible:
            continue
        for stage, value in sorted(observations):
            domain = "owner" if stage.startswith("server_") else "coordinator"
            dimensions = local | {"partition": partition, "clock_domain": domain}
            result += sample("kv_case_read_diagnostic_stage_seconds", value["sum_ns"] / 1e9,
                             dimensions | {"stage": stage, "statistic": "sum"})
            result += sample("kv_case_read_diagnostic_stage_seconds", value["sum_ns"] / value["count"] / 1e9,
                             dimensions | {"stage": stage, "statistic": "mean"})
            if value["max_ns"] is not None:
                result += sample("kv_case_read_diagnostic_stage_seconds", value["max_ns"] / 1e9,
                                 dimensions | {"stage": stage, "statistic": "max"})
            result += sample("kv_case_read_diagnostic_stage_count", value["count"], dimensions | {"counter": stage})
            for field, counter_value in sorted(group_counters.items()):
                if (field.endswith(("_errors", "_partial", "_deferred"))
                        and field.startswith("read_profile_" + stage + "_")):
                    result += sample("kv_case_read_diagnostic_stage_count", counter_value,
                                     dimensions | {"counter": field})
    resolution = profile.get("coordinator_resolution") if profile is not None else None
    if resolution is not None:
        populations = ("selected", "point", "bulk", "errors", "logical_members",
                       "cache_hits", "index_misses", "remote_misses", "member_errors")
        stage_names = {"index_lock_wait", "index_lookup", "cache_lock_wait", "cache_lookup",
                       "cut_validation", "cut_capture", "route_preparation", "pin_preparation", "batch_preparation"}
        if (not isinstance(resolution, dict)
                or set(resolution) != set(populations) | {"selection_policy", "stages"}
                or not isinstance(resolution["selection_policy"], str)
                or not resolution["selection_policy"]
                or not isinstance(resolution["stages"], dict)
                or set(resolution["stages"]) != stage_names):
            raise ValueError("invalid coordinator resolution profile")
        for field in populations:
            count(resolution[field])
        if (resolution["selected"] > selection["requested_limit"]
                or resolution["point"] + resolution["bulk"] != resolution["selected"]
                or resolution["errors"] > resolution["selected"]
                or resolution["cache_hits"] + resolution["index_misses"]
                   + resolution["remote_misses"] + resolution["member_errors"] > resolution["logical_members"]):
            raise ValueError("invalid coordinator resolution populations")
        observations = stage_group(resolution["stages"], {}, resolution["selected"])
        scope = labels | {"phase": "measurement", "scope": "api_resolution_prefix",
                          "diagnostic_backend": backend, "partition": "aggregate"}
        resolution_valid = profile.get("coordinator_resolution_valid", eligible)
        if type(resolution_valid) is not bool:
            raise ValueError("invalid coordinator resolution validity")
        resolution_eligible = evidence["phase_attempted"] and evidence["runtime_enabled"] and resolution_valid
        result += sample("kv_case_read_diagnostic_available", int(bool(resolution_eligible and observations)), scope)
        if resolution_eligible:
            for field in populations:
                result += sample("kv_case_read_diagnostic_operations", resolution[field],
                                 scope | {"population": field})
            for stage, value in sorted(resolution["stages"].items()):
                result += sample("kv_case_read_diagnostic_stage_available", int(value["available"]),
                                 scope | {"clock_domain": "coordinator", "stage": stage})
            for stage, value in sorted(observations):
                dimensions = scope | {"clock_domain": "coordinator", "stage": stage}
                result += sample("kv_case_read_diagnostic_stage_seconds", value["sum_ns"] / 1e9,
                                 dimensions | {"statistic": "sum"})
                result += sample("kv_case_read_diagnostic_stage_seconds", value["sum_ns"] / value["count"] / 1e9,
                                 dimensions | {"statistic": "mean"})
                if value["max_ns"] is not None:
                    result += sample("kv_case_read_diagnostic_stage_seconds", value["max_ns"] / 1e9,
                                     dimensions | {"statistic": "max"})
                result += sample("kv_case_read_diagnostic_stage_count", value["count"],
                                 scope | {"clock_domain": "coordinator", "counter": stage})
    return result


def terminal_metrics(terminal, labels):
    if terminal is None:
        return sample("kv_observer_case_terminal", 0, labels)
    if not isinstance(terminal, dict) or terminal.get("status") not in ("completed", "failed"):
        raise ValueError("invalid terminal case state")
    result = sample("kv_observer_case_terminal", 1, labels)
    result += sample("kv_observer_case_status", 1, labels | {"status": terminal["status"]})
    enabled = terminal.get("write_profile_enabled")
    if enabled is not None:
        if not isinstance(enabled, bool):
            raise ValueError("write profile state must be observed boolean or absent")
        result += sample("kv_observer_write_profile_enabled", int(enabled), labels)
    enabled = terminal.get("read_profile_enabled")
    if enabled is not None:
        if not isinstance(enabled, bool):
            raise ValueError("read profile state must be observed boolean or absent")
        result += sample("kv_observer_read_profile_enabled", int(enabled), labels)
    result += read_path_metrics(terminal.get("read_evidence"), labels)
    result += measurement_read_metrics(terminal.get("measurement_read_evidence"), labels)
    result += cold_device_metrics(terminal.get("cold_device_evidence"), labels)
    result += resource_interval_metrics(terminal.get("resource_evidence"), labels)
    result += write_diagnostic_metrics(terminal.get("write_diagnostics"), labels)
    result += read_diagnostic_metrics(terminal.get("read_diagnostics"), labels)
    metrics = terminal.get("metrics")
    if metrics is None:
        return result
    if not isinstance(metrics, dict) or metrics.get("measurement_rows_complete") is not True:
        raise ValueError("terminal metric gauges require admitted complete measurement rows")
    offered = count(metrics["offered_calls"])
    successes = count(metrics["success_calls"])
    populations = metrics["populations"]
    if not isinstance(populations, list) or len(populations) > 64:
        raise ValueError("invalid bounded metric populations")
    identities = set()
    total = success_total = rejected = 0
    for population in populations:
        identity = (token(population["operation"]), token(population["outcome"]))
        if identity in identities:
            raise ValueError("duplicate operation/outcome population")
        identities.add(identity)
        calls = count(population["count"])
        total += calls
        success_total += calls if identity[1] == "success" else 0
        rejected += calls if identity[1] == "rejected" else 0
        result += sample("kv_case_outcome_calls", calls,
                         labels | {"operation": identity[0], "outcome": identity[1]})
        for clock in ("api", "response", "scheduling"):
            timing = population[clock + "_ns"]
            timed = count(timing["sample_count"])
            if timed > calls:
                raise ValueError("timed sample count exceeds its population")
            timing_labels = labels | {"operation": identity[0], "outcome": identity[1]}
            result += sample("kv_case_timed_samples", timed, timing_labels | {"clock": clock})
            for key, quantile in QUANTILES.items():
                value = timing.get(key)
                if value is not None:
                    if timed == 0:
                        raise ValueError("untimed population cannot have latency quantiles")
                    result += sample("kv_case_" + clock + "_quantile_seconds", number(value) / 1e9,
                                     timing_labels | {"quantile": quantile})
    if total != offered or success_total != successes or successes > offered:
        raise ValueError("terminal population accounting differs from offered/success counts")
    fields = {"kv_case_measurement_seconds": "elapsed_seconds",
              "kv_case_success_calls_per_second": "success_calls_per_second",
              "kv_case_success_logical_keys_per_second": "success_logical_keys_per_second",
              "kv_case_success_value_mib_per_second": "success_value_mib_per_second"}
    for metric, field in fields.items():
        result += sample(metric, number(metrics[field]), labels)
    for metric, value in (("kv_case_offered_calls", offered), ("kv_case_success_calls", successes),
                          ("kv_case_rejected_calls", rejected)):
        result += sample(metric, value, labels)
    return result


def read_tier_metrics(job, labels, effective=None):
    if job is None or job.get("read_tier") is None:
        return ""
    tier = job["read_tier"]
    if not isinstance(tier, dict) or not isinstance(tier.get("requested"), dict):
        raise ValueError("invalid read tier configuration")
    result = ""
    for policy_scope in ("requested", "effective"):
        policy = effective if policy_scope == "effective" and effective is not None else tier.get(policy_scope)
        result += sample("kv_case_cache_policy_available", int(policy is not None),
                         labels | {"policy_scope": policy_scope})
        if policy is None:
            continue
        if not isinstance(policy, dict):
            raise ValueError("invalid read tier policy")
        for key, allowed in (("registered_policy", {"disabled", "hardware_required"}),
                             ("page_cache_policy", {"not_dropped", "drop_at_measurement_gate"})):
            value = policy.get(key)
            if value is None and policy_scope == "effective":
                result += sample("kv_case_cache_policy_field_available", 0,
                                 labels | {"policy_scope": policy_scope, "setting": key})
                continue
            if value not in allowed:
                raise ValueError("invalid read tier policy label")
            result += sample("kv_case_cache_policy_field_available", 1,
                             labels | {"policy_scope": policy_scope, "setting": key})
            result += sample("kv_case_cache_policy_info", 1,
                             labels | {"policy_scope": policy_scope, "component": key, "policy": value})
        for key in ("coordinator_cache_bytes", "owner_read_cache_bytes", "warmup_operations",
                    "read_lanes", "concurrency"):
            value = policy.get(key)
            result += sample("kv_case_cache_policy_field_available", int(value is not None),
                             labels | {"policy_scope": policy_scope, "setting": key})
            if value is None and policy_scope == "effective":
                continue
            result += sample("kv_case_cache_policy_value", count(value),
                             labels | {"policy_scope": policy_scope, "setting": key})
        for key in ("coordinator_read_memory_allowance_bytes", "owner_read_memory_allowance_bytes"):
            allowance = policy.get(key)
            if allowance is not None:
                result += sample("kv_case_cache_policy_value", count(allowance),
                                 labels | {"policy_scope": policy_scope, "setting": key})
        for key in ("owner_read_resident_first", "warmup_key_sweep"):
            value = policy.get(key)
            if value is not None:
                if type(value) is not bool:
                    raise ValueError("invalid read tier boolean")
                result += sample("kv_case_cache_policy_value", int(value),
                                 labels | {"policy_scope": policy_scope, "setting": key})
        budgets = policy.get("registered_budgets")
        if budgets is not None:
            if not isinstance(budgets, dict):
                raise ValueError("invalid registered RAM budgets")
            for key in ("source_bytes", "source_regions", "destination_bytes", "destination_slots",
                        "max_read_bytes", "max_handles"):
                result += sample("kv_case_cache_policy_value", count(budgets.get(key)),
                                 labels | {"policy_scope": policy_scope, "setting": "registered_" + key})
            required = budgets.get("require_device_read")
            if type(required) is not bool:
                raise ValueError("invalid registered RAM device-read requirement")
            result += sample("kv_case_cache_policy_value", int(required),
                             labels | {"policy_scope": policy_scope, "setting": "registered_require_device_read"})
    return result


def benchmark_payload(config, selection):
    if set(selection) != {"job", "results", "terminal"}:
        raise ValueError("unexpected benchmark selection fields")
    job = selection["job"]
    labels = context(config["run_id"], job)
    results = selection["results"]
    if job is None:
        if results is not None or selection["terminal"] is not None:
            raise ValueError("idle selection cannot claim a result")
    else:
        absolute(results)
        selected = Path(results)
        if selected.name != "result" or selected.parent.name != job["case_id"]:
            raise ValueError("result directory does not belong to the selected case")
    current = exporter.snapshot(results, config["stale_seconds"])
    if current.payload and re.search(rb"^kv_case_", current.payload, re.M):
        raise ValueError("source cannot impersonate terminal summary metrics")
    return (scoped(exporter.exposition(current), labels)
            + read_tier_metrics(job, labels, (selection["terminal"] or {}).get("read_tier_effective")).encode()
            + terminal_metrics(selection["terminal"], labels).encode())


def cgroup_directory(root, relative):
    if not isinstance(relative, str) or not relative.startswith("/") or len(relative) > 512:
        raise ValueError("invalid explicit cgroup path")
    parts = relative[1:].split("/")
    if not parts or any(not part or part in (".", "..") or not re.fullmatch(r"[A-Za-z0-9_.:@-]+", part) for part in parts):
        raise ValueError("invalid cgroup components")
    if parts[0] not in ("system.slice", "docker"):
        raise ValueError("only explicitly owned service/container cgroups are supported")
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        return directory
    except BaseException:
        os.close(directory)
        raise


def cgroup_file(directory, filename):
    value = exporter.read_regular(directory, filename)
    if len(value) > MAX_CGROUP_FILE:
        raise ValueError("cgroup statistic exceeds bound")
    return value.decode("ascii")


def integer_fields(text):
    result = {}
    for line in text.splitlines():
        key, value = line.split()
        if key in result or not value.isascii() or not value.isdigit():
            raise ValueError("invalid kernel counter")
        result[key] = int(value)
    return result


def process_payload(root, process, labels):
    if set(process) != {"role", "cgroup"}:
        raise ValueError("unexpected explicit process fields")
    labels = labels | {"process_role": token(process["role"])}
    try:
        directory = cgroup_directory(root, process["cgroup"])
    except (OSError, ValueError):
        return sample("kv_observer_cgroup_available", 0, labels)
    try:
        cpu = integer_fields(cgroup_file(directory, "cpu.stat"))
        if "usage_usec" not in cpu:
            raise ValueError("CPU usage unavailable")
        result = sample("kv_observer_cgroup_available", 1, labels)
        for field, name, scale in (("usage_usec", "cpu_seconds_total", 1e6),
                                    ("throttled_usec", "cpu_throttled_seconds_total", 1e6),
                                    ("nr_periods", "cpu_periods_total", 1),
                                    ("nr_throttled", "cpu_throttled_periods_total", 1)):
            if field in cpu:
                result += sample("kv_observer_cgroup_" + name, cpu[field] / scale, labels)
        for filename, metric in (("memory.current", "memory_current_bytes"),
                                 ("memory.peak", "memory_peak_bytes")):
            try:
                text = cgroup_file(directory, filename).strip()
                if not text.isascii() or not text.isdigit():
                    raise ValueError("invalid kernel memory statistic")
                result += sample("kv_observer_cgroup_" + metric, int(text), labels)
            except FileNotFoundError:
                pass  # Unsupported kernel fields are absent, not zero.
        try:
            events = integer_fields(cgroup_file(directory, "memory.events"))
            if "oom_kill" in events:
                result += sample("kv_observer_cgroup_oom_kills_total", events["oom_kill"], labels)
        except FileNotFoundError:
            pass
        try:
            io = cgroup_file(directory, "io.stat")
        except FileNotFoundError:
            io = ""
        for line in io.splitlines():
            fields = line.split()
            if not fields or not re.fullmatch(r"[0-9]+:[0-9]+", fields[0]):
                raise ValueError("invalid cgroup device identity")
            counters = {}
            for field in fields[1:]:
                key, value = field.split("=", 1)
                if key in counters or not value.isascii() or not value.isdigit():
                    raise ValueError("invalid cgroup IO counter")
                counters[key] = int(value)
            for key, suffix in (("rbytes", "io_read_bytes_total"), ("wbytes", "io_write_bytes_total"),
                                ("rios", "io_reads_total"), ("wios", "io_writes_total")):
                if key in counters:
                    result += sample("kv_observer_cgroup_" + suffix, counters[key], labels | {"device": fields[0]})
        return result
    except (OSError, ValueError, UnicodeError):
        return sample("kv_observer_cgroup_available", 0, labels)
    finally:
        os.close(directory)


def host_payload(config, selection):
    if set(selection) != {"job", "processes"}:
        raise ValueError("unexpected host selection fields")
    labels = context(config["run_id"], selection["job"])
    processes = selection["processes"]
    if not isinstance(processes, list) or len(processes) > 8:
        raise ValueError("at most eight explicitly selected cgroups")
    roles = [process.get("role") for process in processes if isinstance(process, dict)]
    if len(roles) != len(processes) or len(roles) != len(set(roles)):
        raise ValueError("duplicate/invalid process roles")
    result = sample("kv_observer_host_selection_available", 1, labels)
    for process in processes:
        result += process_payload(config["cgroup_root"], process, labels)
    return result.encode()


def metric_types(body):
    names = set()
    for line in body.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        matched = METRIC.match(line)
        if matched and (matched.group().startswith(("kv_case_", "kv_observer_cgroup_"))
                        or matched.group() in ("kv_observer_host_selection_available",
                                               "kv_observer_case_terminal", "kv_observer_case_status",
                                               "kv_observer_write_profile_enabled",
                                               "kv_observer_selection_invalid")):
            names.add(matched.group())
    return ("".join("# TYPE " + name + (" counter\n" if name.endswith("_total") else " gauge\n")
                    for name in sorted(names))).encode() + body


def payload(config):
    try:
        selection = read_json(config["selection"])
        body = benchmark_payload(config, selection) if config["mode"] == "benchmark" else host_payload(config, selection)
        if len(body) > exporter.MAX_BYTES:
            raise ValueError("bounded exposition exceeded")
        return metric_types(body)
    except (OSError, ValueError, KeyError, TypeError, UnicodeError):
        labels = {"run_id": config["run_id"], "case_id": "invalid"}
        return metric_types(sample("kv_observer_selection_invalid", 1, labels).encode())


def validate_config(config):
    common = {"schema_version", "mode", "run_id", "selection"}
    expected = common | ({"stale_seconds"} if config.get("mode") == "benchmark" else {"proc_root", "cgroup_root"})
    if (set(config) != expected or type(config["schema_version"]) is not int
            or config["schema_version"] != 1 or config["mode"] not in ("benchmark", "host")):
        raise ValueError("invalid campaign exporter configuration")
    token(config["run_id"])
    absolute(config["selection"])
    if config["mode"] == "benchmark":
        stale = number(config["stale_seconds"])
        if not 2 <= stale <= 3600:
            raise ValueError("invalid snapshot staleness bound")
    else:
        absolute(config["proc_root"])
        absolute(config["cgroup_root"])
    return config


class Handler(exporter.Handler):
    def respond(self, head):
        if self.path == "/healthz":
            return super().respond(head)
        if self.path != "/metrics":
            self.send_error(404)
            return
        if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Length", "0") != "0":
            self.send_error(400)
            return
        body = payload(self.server.config)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if not head:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass


def main():
    import ipaddress
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--listen", required=True)
    args = parser.parse_args()
    config = validate_config(read_json(args.config))
    address, port = args.listen.rsplit(":", 1)
    ip = ipaddress.ip_address(address)
    if ip.version != 4 or ip.is_unspecified or ip.is_multicast or not 1 <= int(port) <= 65535:
        parser.error("listen must be an explicit non-wildcard IPv4:port")
    with exporter.MetricsServer((address, int(port)), Handler) as server:
        server.config = config
        # Intentionally accept bounded HTTP requests until the owned service stops.
        server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
