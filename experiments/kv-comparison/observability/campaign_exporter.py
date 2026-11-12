#!/usr/bin/env python3
"""Expose one explicit campaign selection; never discover processes or invent history."""
import argparse
from http.server import BaseHTTPRequestHandler
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import time

import exporter

MAX_SELECTION = 1024 * 1024
MAX_CGROUP_FILE = 256 * 1024
# Match the compact and five-owner fleet layouts.
SEALED_OWNER_COUNTS = (1, 5)
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
METRIC = re.compile(r"[A-Za-z_:][A-Za-z0-9_:]*")
LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*("(?:[^"\\]|\\.)*")')
QUANTILES = {"p50": "0.5", "p95": "0.95", "p99": "0.99", "p999": "0.999", "max": "1"}
COORDINATOR_INTERVAL_COUNTERS = {
    "write_storage_wait_ns", "write_publication_ns", "write_dispatch_ns", "write_preparation_admission_ns",
    "write_checkpoint_ns", "write_checkpoint_count", "write_foreground_checkpoint_ns",
    "write_foreground_checkpoint_count", "write_background_checkpoint_ns",
    "write_concurrent_checkpoint_deferrals", "write_concurrent_checkpoint_wait_ns",
    "write_background_checkpoint_suffix_batches", "write_durable_receipt_publication_blocked_ns",
    "write_durable_receipt_publication_blocked_batches", "write_append_requests", "write_direct_appends",
    "write_durable_batches", "write_concurrent_admitted", "write_concurrent_completed",
    "write_out_of_prefix_published_batches", "write_dependency_wait_batches",
    "write_checkpoint_diagnostic_root_adoption_ns", "write_checkpoint_diagnostic_root_adoption_count",
    "write_checkpoint_diagnostic_owner_adoption_ns", "write_checkpoint_diagnostic_owner_adoption_count",
    "write_checkpoint_diagnostic_obsolete_cleanup_ns", "write_checkpoint_diagnostic_obsolete_cleanup_count",
    "write_checkpoint_diagnostic_index_prepare_ns", "write_checkpoint_diagnostic_index_prepare_count"}


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
    route = job.get("nvmeof_route")
    if route is not None:
        if (route not in ("live", "sealed_read")
                or job["backend"] not in ("tidehunter_nvme_tcp", "tidehunter_nvme_rdma")
                or (job.get("case_kind") == "hybrid_put" and
                    (route != "sealed_read" or job["cohort"] != "primary_rf1" or job["backend"] != "tidehunter_nvme_rdma"))
                or (job.get("case_kind") != "hybrid_put" and
                    (route == "sealed_read") != (job["cohort"] == "sealed_read"))):
            raise ValueError("invalid requested NVMe-oF route or population")
        labels["requested_nvmeof_route"] = route
        target = job.get("nvmeof_target", "kernel")
        if target not in ("kernel", "spdk") or target == "spdk" and job["backend"] != "tidehunter_nvme_rdma":
            raise ValueError("invalid NVMe-oF target implementation")
        labels["requested_nvmeof_target"] = target
        if target == "spdk":
            labels["requested_backend"] = ("tidehunter_spdk_live_rdma" if route == "live"
                                           else "tidehunter_spdk_rdma")
    elif job.get("nvmeof_target") is not None or job.get("case_kind") == "hybrid_put":
        raise ValueError("target implementation requires an NVMe-oF route")
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
    # Historical snapshots may contain owner-cache counters; emit only observed fields.
    families = (("owner", "available", "owner_read_executor_stats",
                 ("wal_reads", "wal_read_bytes", "async_read_submissions", "completed_reads",
                  "errors", "cancellations", "buffer_reuses", "buffer_allocations",
                  "resident_read_attempts", "resident_read_successes", "resident_read_fallbacks",
                  "resident_read_unsupported", "resident_read_partial", "resident_read_deferred",
                  "cache_hits", "cache_misses")),
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
    supplemental = client_pool_metrics(evidence.get("client_pool"), labels)
    supplemental += live_nvmeof_peak_metrics(evidence.get("live_nvmeof_peaks"), labels)
    supplemental += nvmeof_reader_boundaries(evidence, labels)
    result = ""
    if deltas.get("available") is False:
        return (sample("kv_case_read_evidence_available", 0, labels | {"component": "measurement"})
                + nvmeof_reader_metrics(evidence.get("nvmeof"), labels)
                + live_nvmeof_metrics(evidence.get("live_nvmeof"), labels) + supplemental)
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
    components = ("owner_executor",)
    if "client_resident" in deltas:
        components += ("client_resident",)
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
                current_stages = {"read_response_encode", "read_output_queue", "read_output_submit",
                                  "read_response_retire"}
                historical_stages = current_stages | {
                    "read_grant_lookup", "read_grant_reservation", "read_grant_envelope",
                    "read_grant_registration", "read_grant_recheck_publication"}
                if not isinstance(stages, dict) or set(stages) not in (current_stages, historical_stages):
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
    return (result + nvmeof_reader_metrics(evidence.get("nvmeof"), labels)
            + live_nvmeof_metrics(evidence.get("live_nvmeof"), labels) + supplemental)


def client_pool_metrics(evidence, labels):
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or type(evidence.get("available")) is not bool:
        raise ValueError("invalid client pool evidence")
    result = sample("kv_client_pool_evidence_available", int(evidence["available"]), labels)
    if evidence["available"]:
        counters = evidence.get("counters")
        if not isinstance(counters, dict):
            raise ValueError("invalid client pool counters")
        for field, value in counters.items():
            result += sample("kv_client_pool_events", count(value), labels | {"counter": token(field)})
    return result


def live_nvmeof_peak_metrics(evidence, labels):
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or set(evidence) - {"lane_inflight_request_max", "lane_inflight_op_max"}:
        raise ValueError("invalid live NVMe-oF peaks")
    result = ""
    for field, row in evidence.items():
        if not isinstance(row, dict):
            raise ValueError("invalid live NVMe-oF peak")
        for boundary, key in (("start", "at_start"), ("end", "at_end")):
            value = row.get(key)
            if value is not None:
                result += sample("kv_live_nvmeof_lifetime_peak", count(value),
                                 labels | {"scope": "node_lifetime", "boundary": boundary, "peak": field})
    return result


def nvmeof_reader_boundaries(evidence, labels):
    """Expose new sealed-reader fields as lifetime snapshots, not interval deltas."""
    result = ""
    for boundary in ("start", "end"):
        snapshot = evidence.get(boundary)
        nvme = snapshot.get("nvmeof") if isinstance(snapshot, dict) else None
        if not isinstance(nvme, dict) or nvme.get("available") is not True:
            continue
        readers = nvme.get("readers")
        if not isinstance(readers, list) or len(readers) > 128:
            raise ValueError("invalid NVMe reader boundary")
        for index, reader in enumerate(readers):
            if not isinstance(reader, dict):
                raise ValueError("invalid NVMe reader boundary row")
            local = labels | {"scope": "node_lifetime", "boundary": boundary, "reader": str(index),
                              "partition": str(count(reader.get("partition")))}
            for field in ("admitted_sqes", "merged_members", "pool_hits", "pool_misses",
                          "pool_recycled", "pool_discarded"):
                if field in reader and reader[field] is not None:
                    result += sample("kv_nvmeof_reader_lifetime_count", count(reader[field]),
                                     local | {"counter": field})
            for stage in ("lock_wait", "lock_hold"):
                row = reader.get(stage)
                if row is None:
                    continue
                if not isinstance(row, dict):
                    raise ValueError("invalid NVMe reader boundary stage")
                samples = count(row.get("count"))
                total = count(row.get("sum_ns"))
                maximum = count(row.get("max_ns"))
                if not samples and (total or maximum):
                    raise ValueError("NVMe reader boundary timing without samples")
                dimensions = local | {"stage": stage}
                result += sample("kv_nvmeof_reader_lifetime_stage_samples", samples, dimensions)
                if samples:
                    result += sample("kv_nvmeof_reader_lifetime_stage_seconds", total / 1e9,
                                     dimensions | {"statistic": "sum"})
                    result += sample("kv_nvmeof_reader_lifetime_stage_seconds", maximum / 1e9,
                                     dimensions | {"statistic": "max"})
    return result


def live_nvmeof_metrics(evidence, labels):
    """Export only admitted measurement deltas, not lifetime snapshots or route claims."""
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or type(evidence.get("available")) is not bool:
        raise ValueError("invalid live NVMe-oF measurement evidence")
    result = sample("kv_live_nvmeof_evidence_available", int(evidence["available"]), labels)
    if not evidence["available"]:
        return result
    counters = evidence.get("counters")
    fields = ("completed_read_ops", "completed_read_bytes", "completed_write_ops",
              "completed_write_bytes", "recovery_read_ops", "recovery_read_bytes",
              "metadata_requests", "namespace_connects", "payload_rpc_requests",
              "unknown_writes", "errors", "route_evidence_dropped", "fence_evidence_dropped")
    stages = ("queue", "submit", "cq", "control", "validation", "allocation",
              "publication", "namespace_setup", "read_io", "persistence", "owner_completion",
              "wal_persistence", "wal_namespace", "frontier_file", "frontier_namespace")
    extra_fields = ("lane_inflight_samples", "lane_inflight_request_sum", "lane_inflight_op_sum",
                    "grant_hits", "grant_misses", "grant_evictions", "copy_bytes_avoided",
                    "reserve_exchanges", "complete_exchanges", "lease_exchanges", "leased_frames",
                    "leased_bytes", "leased_skip_markers", "extents_leased", "adoption_async_exchanges",
                    "adoption_sync_exchanges", "fua_writes", "leased_failed_writes", "leased_short_writes",
                    "leased_timed_out_writes", "leased_full_writes", "failed_write_ops", "short_write_ops",
                    "short_write_bytes", "adoption_history_full_settles")
    extra_stages = ("delivery", "controller_connect", "controller_disconnect", "leased_write",
                    "extent_switch", "adoption_async", "adoption_settle")
    expected = set(fields) | {stage + suffix for stage in stages for suffix in ("_ns", "_samples")}
    allowed = expected | set(extra_fields) | {stage + suffix for stage in extra_stages for suffix in ("_ns", "_samples")}
    if not isinstance(counters, dict) or not expected <= counters.keys() or counters.keys() - allowed:
        raise ValueError("incomplete live NVMe-oF measurement counters")
    for field in fields + tuple(field for field in extra_fields if field in counters):
        metric = "kv_live_nvmeof_bytes" if field.endswith("_bytes") or field == "copy_bytes_avoided" else "kv_live_nvmeof_events"
        result += sample(metric, count(counters[field]), labels | {"counter": field})
    for stage in stages + tuple(stage for stage in extra_stages if stage + "_ns" in counters or stage + "_samples" in counters):
        if stage + "_ns" not in counters or stage + "_samples" not in counters:
            raise ValueError("incomplete live NVMe-oF timing")
        samples = count(counters[stage + "_samples"])
        total = count(counters[stage + "_ns"])
        if not samples and total:
            raise ValueError("live NVMe-oF timing sum without samples")
        stage_labels = labels | {"stage": stage}
        result += sample("kv_live_nvmeof_stage_available", int(samples > 0), stage_labels)
        result += sample("kv_live_nvmeof_stage_samples", samples, stage_labels)
        if samples:
            result += sample("kv_live_nvmeof_stage_seconds", total / 1e9, stage_labels | {"statistic": "sum"})
    return result


def nvmeof_reader_metrics(nvme, labels):
    result = ""
    if nvme is not None:
        if not isinstance(nvme, dict) or type(nvme.get("available")) is not bool:
            raise ValueError("invalid NVMe measurement evidence")
        result += sample("kv_nvmeof_reader_evidence_available", int(nvme["available"]), labels)
        readers = nvme.get("readers")
        if readers is not None:
            if not isinstance(readers, dict) or len(readers) > 128:
                raise ValueError("invalid NVMe measurement readers")
            for index, row in enumerate(readers.values()):
                if not isinstance(row, dict) or type(row.get("available")) is not bool:
                    raise ValueError("invalid NVMe reader")
                identity = row.get("identity")
                if not isinstance(identity, dict):
                    raise ValueError("missing NVMe reader identity")
                transport = identity.get("transport")
                if transport not in (None, "tcp", "rdma"):
                    raise ValueError("invalid NVMe reader transport")
                local = labels | {"reader": str(index), "partition": str(count(identity.get("partition"))),
                                  "transport": transport or "unknown"}
                result += sample("kv_nvmeof_reader_available", int(row["available"]), local)
                counters = row.get("counters")
                if isinstance(counters, dict) and counters.get("available") is True:
                    values = counters.get("counters")
                    if not isinstance(values, dict):
                        raise ValueError("invalid NVMe reader counters")
                    for name, value in values.items():
                        result += sample("kv_nvmeof_reader_count", count(value), local | {"counter": token(name)})
                stages = row.get("stages")
                if not isinstance(stages, dict):
                    raise ValueError("invalid NVMe reader stages")
                for stage, stage_row in stages.items():
                    if not isinstance(stage_row, dict):
                        raise ValueError("invalid NVMe reader stage")
                    delta = stage_row.get("delta")
                    if not isinstance(delta, dict) or type(delta.get("available")) is not bool:
                        raise ValueError("invalid NVMe stage availability")
                    stage_labels = local | {"stage": token(stage)}
                    result += sample("kv_nvmeof_reader_stage_available", int(delta["available"]), stage_labels)
                    if delta["available"]:
                        values = delta.get("counters")
                        if not isinstance(values, dict):
                            raise ValueError("invalid NVMe stage counters")
                        result += sample("kv_nvmeof_reader_stage_count", count(values.get("count")), stage_labels)
                        result += sample("kv_nvmeof_reader_stage_seconds", count(values.get("sum_ns")) / 1e9,
                                         stage_labels | {"statistic": "sum"})
    return result


def spdk_target_metrics(evidence, labels):
    if evidence is None:
        return ""
    if isinstance(evidence, dict) and evidence.get("kind") == "live":
        return spdk_live_target_metrics(evidence, labels)
    if not isinstance(evidence, dict) or evidence.get("available") is not True:
        raise ValueError("SPDK metrics require admitted target intervals")
    owners = evidence.get("owners")
    if (not isinstance(owners, list) or len(owners) not in SEALED_OWNER_COUNTS
            or any(not isinstance(row, dict) or type(row.get("index")) is not int for row in owners)
            or {row["index"] for row in owners} != set(range(len(owners)))):
        raise ValueError("SPDK target metrics require every owner")
    result = sample("kv_spdk_target_evidence_available", 1, labels)
    for owner, row in [("all", evidence)] + [(str(row["index"]), row) for row in owners]:
        local = labels | {"target_owner": owner}
        reads, size = count(row["read_ops"]), count(row["read_bytes"])
        mean = row.get("mean_read_completion_us")
        if bool(reads) != bool(size) or (reads == 0) != (mean is None):
            raise ValueError("SPDK target sample count, bytes and mean disagree")
        result += sample("kv_spdk_target_read_ops", reads, local)
        result += sample("kv_spdk_target_read_bytes", size, local)
        if mean is not None:
            result += sample("kv_spdk_target_read_completion_seconds", number(mean) / 1e6,
                             local | {"statistic": "mean"})
    return result


def spdk_live_target_metrics(evidence, labels):
    """Export admitted owner intervals, without inventing partial aggregates."""
    owners = evidence.get("owners")
    if (type(evidence.get("available")) is not bool or not isinstance(owners, list)
            or len(owners) != 5 or any(not isinstance(row, dict) for row in owners)
            or {row.get("index") for row in owners} != set(range(5))
            or any(type(row.get("available")) is not bool for row in owners)
            or evidence["available"] != all(row["available"] for row in owners)):
        raise ValueError("invalid live SPDK owner availability")
    result = sample("kv_spdk_live_target_evidence_available", int(evidence["available"]), labels)
    rows = [(str(row["index"]), row) for row in owners]
    if evidence["available"]:
        rows.insert(0, ("all", evidence))
    for owner, row in rows:
        local = labels | {"target_owner": owner}
        result += sample("kv_spdk_live_target_owner_available", int(row["available"]), local)
        if not row["available"]:
            continue
        if type(row.get("io_error_available")) is not bool:
            raise ValueError("invalid live SPDK error availability")
        result += sample("kv_spdk_live_target_io_error_available", int(row["io_error_available"]), local)
        if row["io_error_available"]:
            errors = row.get("io_errors")
            if not isinstance(errors, dict):
                raise ValueError("missing live SPDK error counters")
            for status, value in errors.items():
                result += sample("kv_spdk_live_target_io_errors", count(value), local | {"status": token(status)})
        for direction in ("read", "write"):
            ops, size = count(row[direction + "_ops"]), count(row[direction + "_bytes"])
            mean = row.get("mean_" + direction + "_completion_us")
            if bool(ops) != bool(size) or (ops == 0) != (mean is None):
                raise ValueError("live SPDK operation count, bytes and mean disagree")
            result += sample("kv_spdk_live_target_ops", ops, local | {"direction": direction})
            result += sample("kv_spdk_live_target_bytes", size, local | {"direction": direction})
            if mean is not None:
                result += sample("kv_spdk_live_target_completion_seconds", number(mean) / 1e6,
                                 local | {"direction": direction, "statistic": "mean"})
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
        populations = [(row, local, "kv_case_block_interval")]
        owned = row.get("owned_wal_io")
        if owned is not None:
            if not isinstance(owned, dict) or type(owned.get("available")) is not bool:
                raise ValueError("invalid owned-WAL evidence availability")
            owned_labels = local | {"scope": "collector_window_owned_wal_extent_subset"}
            result += sample("kv_case_owned_wal_evidence_available", int(owned["available"]), owned_labels)
            if owned["available"]:
                if backend != "request_identity" or row["qualified"] is not True:
                    raise ValueError("owned-WAL attribution requires a qualified request-identity trace")
                reads = count(owned.get("read_ops"))
                size = count(owned.get("read_bytes"))
                if type(owned.get("qualified")) is not bool or owned["qualified"] != bool(reads) or bool(size) != bool(reads):
                    raise ValueError("owned-WAL I/O counts and qualification disagree")
                result += sample("kv_case_owned_wal_read_bytes", size, owned_labels)
                populations.append((owned | {"matched_reads": reads, "lost": row["lost"], "truncated": row["truncated"]},
                                    owned_labels, "kv_case_owned_wal_interval"))
        for population, interval_labels, prefix in populations:
            interval = population.get("intervals_ns")
            if interval is not None:
                if backend != "request_identity" or not isinstance(interval, dict):
                    raise ValueError("only request-identity intervals may be exported")
                required = {"count", "sum", "max", "mean", "population_stddev", "p50", "p95", "p99", "p999", "tail_warnings"}
                if set(interval) != required or not isinstance(interval["tail_warnings"], list):
                    raise ValueError("invalid block interval distribution")
                n = count(interval["count"])
                total = count(interval["sum"])
                if population.get("matched_reads") != n:
                    raise ValueError("block interval population differs from matched requests")
                for field in ("max", "mean", "population_stddev", "p50", "p95", "p99", "p999"):
                    value = interval[field]
                    if (n == 0) != (value is None):
                        raise ValueError("invalid block interval availability")
                    if value is not None:
                        number(value)
                if n == 0 and total != 0:
                    raise ValueError("empty block interval population has nonzero sum")
                usable = n > 0 and population.get("lost") == 0 and not population["truncated"]
                result += sample(prefix + "_available", int(usable), interval_labels)
                if usable:
                    result += sample(prefix + "_count", n, interval_labels)
                    result += sample(prefix + "_seconds", total / 1e9, interval_labels | {"statistic": "sum"})
                    for field in ("max", "mean", "population_stddev", "p50", "p95", "p99", "p999"):
                        result += sample(prefix + "_seconds", interval[field] / 1e9,
                                         interval_labels | {"statistic": field})
            else:
                result += sample(prefix + "_available", 0, interval_labels)
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
        partial = not row["available"] and row.get("core_available") is True
        if not row["available"]:
            reason = row.get("reason")
            if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
                raise ValueError("unavailable resource evidence requires a bounded reason")
            if not partial:
                if set(row) != required | {"reason"}:
                    raise ValueError("unavailable resource evidence requires only a bounded reason")
                continue
            core_integers = integers - {"io_read_bytes", "io_write_bytes"}
            if (not required | {"reason", "core_available", "elapsed_seconds", "cpu_seconds"} <= set(row)
                    or set(row) - required - seconds - core_integers - {"reason", "core_available", "elapsed_seconds"}):
                raise ValueError("invalid partially available resource fields")
            result += sample("kv_case_resource_interval_core_available", 1, local)
            result += sample("kv_case_resource_interval_io_available", 0, local)
        elif (not required | {"elapsed_seconds"} <= set(row)
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


LIVE_NVME_WRITE_COUNTERS = {
    "completed_read_ops", "completed_read_bytes", "completed_write_ops", "completed_write_bytes",
    "recovery_read_ops", "recovery_read_bytes", "metadata_requests", "namespace_connects",
    "unknown_writes", "errors", "payload_rpc_requests",
    "lane_inflight_samples", "lane_inflight_request_sum", "lane_inflight_op_sum",
    "grant_hits", "grant_misses", "grant_evictions", "copy_bytes_avoided",
    "reserve_exchanges", "complete_exchanges", "lease_exchanges", "leased_frames", "leased_bytes",
    "leased_skip_markers", "extents_leased", "adoption_async_exchanges", "adoption_sync_exchanges",
    "fua_writes", "leased_failed_writes", "leased_short_writes", "leased_timed_out_writes",
    "leased_full_writes", "failed_write_ops", "short_write_ops", "short_write_bytes",
    "adoption_history_full_settles",
    *(stage + suffix for stage in ("queue", "submit", "cq", "control", "validation", "persistence",
                                   "allocation", "publication", "namespace_setup", "read_io",
                                   "owner_completion", "wal_persistence", "wal_namespace",
                                   "frontier_file", "frontier_namespace", "delivery",
                                   "controller_connect", "controller_disconnect", "leased_write",
                                   "extent_switch", "adoption_async", "adoption_settle")
      for suffix in ("_ns", "_samples")),
}


def write_diagnostic_metrics(evidence, labels):
    """Bounded accumulated clocks/counts with independent explicit population scopes."""
    if evidence is None:
        return ""
    required = {"schema_version", "backend", "phase", "phase_attempted", "runtime_enabled",
                "runtime_observation_mode", "selection", "clock_scope", "overlap_semantics",
                "unsupported_stages", "profile", "error"}
    if not isinstance(evidence, dict) or set(evidence) - {"pre_completion_gate", "measurement_drained"} != required:
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
    if "measurement_drained" in evidence and type(evidence["measurement_drained"]) is not bool:
        raise ValueError("invalid write measurement drain status")
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
    mode = evidence["runtime_observation_mode"]
    nvme = backend == "tidehunter_live_nvmeof" and mode == "live_nvmeof_phase_counters"
    if mode == "live_nvmeof_phase_counters" and not nvme:
        raise ValueError("live NVMe write window requires the live NVMe backend")
    if profile is not None and not nvme:
        stages = profile.get("stages")
        if (profile.get("scope") == "measurement_nvme_io_cumulative_delta"
                or isinstance(stages, dict) and "live_nvmeof" in stages):
            raise ValueError("live NVMe write evidence requires the live backend and observation mode")
    if nvme and profile is not None:
        stages = profile.get("stages")
        counters = stages.get("live_nvmeof") if isinstance(stages, dict) else None
        if (profile.get("scope") != "measurement_nvme_io_cumulative_delta"
                or profile.get("runtime_observation_mode") != mode
                or type(profile.get("requested_limit")) is not int
                or profile["requested_limit"] != selection["requested_limit"]
                or not isinstance(counters, dict) or set(stages) != {"live_nvmeof"}
                or set(counters) - LIVE_NVME_WRITE_COUNTERS
                or type(profile.get("selected_operations")) is not int
                or type(counters.get("completed_write_ops")) is not int
                or profile["selected_operations"] != observed
                or counters["completed_write_ops"] != observed
                or type(counters.get("errors")) is not int or counters["errors"] != 0
                or type(counters.get("unknown_writes")) is not int or counters["unknown_writes"] != 0
                or profile.get("finalization_error") is not None
                or evidence["runtime_enabled"] != (observed > 0)):
            raise ValueError("invalid live NVMe write counter window")
        for field, value in counters.items():
            token(field)
            count(value)
        for field in counters:
            if field.endswith("_ns"):
                samples_field = field[:-3] + "_samples"
                if samples_field not in counters:
                    raise ValueError("live NVMe clock lacks its sample count")
            elif field.endswith("_samples") and field != "lane_inflight_samples":
                if field[:-8] + "_ns" not in counters:
                    raise ValueError("live NVMe sample count lacks its clock")
    elif nvme and (observed or evidence["runtime_enabled"]):
        raise ValueError("live NVMe write counter window is unavailable")
    elif backend == "tidehunter_live_nvmeof" and evidence["runtime_enabled"]:
        raise ValueError("unrecognized live NVMe write observation mode")
    if profile is not None and profile.get("runtime_observation_mode") == "live_nvmeof_phase_counters" and not nvme:
        raise ValueError("live NVMe write profile mode differs from artifact")
    scope = "measurement_nvme_io_cumulative_delta" if nvme else "sampled_prefix"
    boundary_eligible = evidence["phase_attempted"] and evidence.get("measurement_drained") is not False
    available = (boundary_eligible and (not nvme or evidence.get("measurement_drained") is True)
                 and evidence["runtime_enabled"] and observed > 0
                 and profile is not None and isinstance(profile.get("stages"), dict)
                 and bool(profile["stages"]) and evidence["error"] is None)
    local = labels | {"phase": "measurement", "scope": scope, "diagnostic_backend": backend}
    result = sample("kv_case_write_diagnostic_available", int(available), local)
    result += sample("kv_case_write_diagnostic_operations", selection["requested_limit"], local | {"population": "requested_limit"})
    result += sample("kv_case_write_diagnostic_operations", observed, local | {
        "population": "completed_write_ops" if nvme else "observed_samples"})
    pre = evidence.get("pre_completion_gate")
    if pre is not None:
        if not isinstance(pre, dict) or type(pre.get("available")) is not bool:
            raise ValueError("invalid pre-completion-gate snapshot")
        boundary = local | {"scope": "coordinator_arm_through_pre_completion_gate"}
        result += sample("kv_case_write_diagnostic_available", int(pre["available"] and boundary_eligible), boundary)
        if pre["available"]:
            if (set(pre) != {"available", "scope", "clock_scope", "stages"}
                    or pre["scope"] != boundary["scope"]
                    or not isinstance(pre["clock_scope"], str) or not pre["clock_scope"].strip()
                    or not isinstance(pre["stages"], dict) or not pre["stages"]
                    or set(pre["stages"]) - COORDINATOR_INTERVAL_COUNTERS):
                raise ValueError("invalid pre-completion-gate coordinator interval")
            checkpoint_available = profile.get("coordinator_checkpoint_diagnostics_available") if profile else None
            if checkpoint_available is not None and type(checkpoint_available) is not bool:
                raise ValueError("invalid coordinator checkpoint diagnostic availability")
            for field, value in sorted(pre["stages"].items()):
                value = count(value)
                if not boundary_eligible:
                    continue
                if field.startswith("write_checkpoint_diagnostic_") and checkpoint_available is not True:
                    continue
                if field.endswith("_ns"):
                    result += sample("kv_case_write_diagnostic_stage_seconds", value / 1e9,
                                     boundary | {"component": "coordinator_interval", "stage": field})
                else:
                    result += sample("kv_case_write_diagnostic_stage_count", value,
                                     boundary | {"component": "coordinator_interval", "counter": field})
        elif set(pre) != {"available", "reason"} or not isinstance(pre["reason"], str) or not pre["reason"]:
            raise ValueError("unavailable pre-completion-gate snapshot requires a reason")
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
            elif ((value is None and field in ("sampled_registered_files_min", "sampled_registered_files_max"))
                  or (isinstance(value, str) and field in ("status", "reason"))):
                continue
            else:
                value = count(value)
                if nvme and component == "stages.live_nvmeof" and field.endswith("_ns") and tree.get(field[:-3] + "_samples") == 0:
                    continue
                if field.endswith("_ns"):
                    counters.append(("kv_case_write_diagnostic_stage_seconds", value / 1e9,
                                     {"component": component, "stage": field}))
                elif (field in ("sampled_registered_files_min", "sampled_registered_files_max")
                      or field.endswith(("_calls", "_errors", "_samples", "_requests", "_retries", "_groups",
                                         "_receipts", "_publications", "_successes", "_operations", "_barriers",
                                         "_connections", "_count", "_batches", "_appends", "_admitted", "_completed",
                                         "_polls", "_would_block", "_stalls", "_completions", "_ops", "_bytes"))
                      or (nvme and component == "stages.live_nvmeof")):
                    counters.append(("kv_case_write_diagnostic_stage_count", value,
                                     {"component": component, "counter": field}))

    if stages is not None:
        stage_tree(stages, "stages")
    if persistence is not None:
        stage_tree(persistence, "value_wal_persistence")
    checkpoint_fields = {"write_profile_checkpoint_requests", "write_profile_checkpoint_completed",
                         "write_profile_checkpoint_wait_ns", "write_profile_checkpoint_append_completions",
                         "write_profile_checkpoint_admission_stalls"}
    checkpoint_state = stages.get("write_profile_checkpoint_available") if stages else None
    if checkpoint_state is not None:
        if type(checkpoint_state) is not int or checkpoint_state not in (0, 1):
            raise ValueError("invalid sampled checkpoint availability")
        if checkpoint_state and not checkpoint_fields <= stages.keys():
            raise ValueError("sampled checkpoint counters incomplete")
        checkpoint_available = bool(checkpoint_state and boundary_eligible and evidence["error"] is None)
        result += sample("kv_case_write_diagnostic_available", int(checkpoint_available),
                         local | {"scope": "sampled_checkpoint_control"})
    else:
        checkpoint_available = False
    for metric, value, dimensions in counters:
        field = dimensions.get("stage", dimensions.get("counter"))
        if (checkpoint_available if field in checkpoint_fields else available):
            result += sample(metric, value, local | dimensions)
    inventory = profile.get("value_wal_inventory")
    if inventory is not None and boundary_eligible:
        if not isinstance(inventory, dict):
            raise ValueError("invalid Value-WAL inventory")
        for boundary in ("start", "end"):
            row = inventory.get(boundary)
            if row is None:
                continue
            if not isinstance(row, dict) or type(row.get("available")) is not bool:
                raise ValueError("invalid Value-WAL inventory boundary")
            dimensions = local | {"scope": "measurement_boundary", "boundary": boundary}
            result += sample("kv_case_native_wal_inventory_available", int(row["available"]), dimensions)
            if row["available"]:
                for field in ("registered_files", "logical_file_length_bytes"):
                    result += sample("kv_case_native_wal_" + field, count(row.get(field)), dimensions)
            elif "registered_files" in row or "logical_file_length_bytes" in row:
                raise ValueError("unavailable Value-WAL inventory claims observed quantities")
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
        boundary_available = boundary_eligible
        result += sample("kv_case_write_diagnostic_available", int(boundary_available), boundary_labels)
        if boundary_available:
            checkpoint_available = profile.get("coordinator_checkpoint_diagnostics_available")
            if checkpoint_available is not None and type(checkpoint_available) is not bool:
                raise ValueError("invalid coordinator checkpoint diagnostic availability")
            result += sample("kv_case_write_diagnostic_available", int(checkpoint_available is True),
                             boundary_labels | {"component": "coordinator_checkpoint"})
            for field, value in sorted(coordinator["stages"].items()):
                if field.startswith("write_checkpoint_diagnostic_") and checkpoint_available is not True:
                    continue
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
            if isinstance(value, float):
                number(value)
            else:
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
    coverage_populations = ("selected_remote_requests", "completed_remote_requests", "profiled_owner_responses")
    if coverage is not None:
        coverage_fields = set(coverage_populations) | {"reason"}
        if (not isinstance(coverage, dict)
                or set(coverage) not in (coverage_fields, coverage_fields | {"nvme_completed"})
                or not isinstance(coverage["reason"], str)):
            raise ValueError("invalid read diagnostic coverage")
        # Historical artifacts lack this counter. Do not invent an observed zero.
        if "nvme_completed" in coverage:
            coverage_populations += ("nvme_completed",)
        for field in coverage_populations:
            count(coverage[field])
        if not (coverage["completed_remote_requests"] <= coverage["selected_remote_requests"]
                <= selection["requested_limit"]):
            raise ValueError("invalid read diagnostic coverage populations")
        if ("nvme_completed" in coverage
                and coverage["nvme_completed"] > coverage["completed_remote_requests"]):
            raise ValueError("NVMe diagnostic completions exceed completed remote requests")
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
        for population in coverage_populations:
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
        transport_fields = ("cq_timed_calls", "tx_all_slots_pending_polls", "rx_no_posted_slots_polls",
                            "rx_unread_slots_polls", "send_post_would_block", "receive_post_would_block")
        denominator = group_counters.get("read_profile_transport_cq_timed_calls")
        if denominator is not None and count(denominator) > 0:
            for suffix in transport_fields:
                field = "read_profile_transport_" + suffix
                if field in group_counters:
                    result += sample("kv_case_read_transport_observations", group_counters[field],
                                     local | {"partition": partition, "counter": suffix})
        if group_counters.get("read_profile_transport_progress_available") == 1:
            for suffix in ("sweeps", "frame_gate_stalls", "retirement_stalls", "cq_polls",
                           "cq_empty_polls", "cq_completions", "send_size_le_4k", "send_size_le_16k",
                           "send_size_le_64k", "send_size_gt_64k", "send_post_eagain",
                           "send_post_retries", "cq_idle_waits"):
                field = "read_profile_transport_" + suffix
                if field in group_counters:
                    result += sample("kv_case_read_transport_observations", group_counters[field],
                                     local | {"partition": partition, "counter": suffix})
        if selected:
            for suffix in ("admission_retry_credit", "admission_retry_flight", "admission_retry_lane",
                           "admission_retry_paused", "admission_retry_config", "admission_retry_mixed",
                           "live_lane_full", "live_lane_depth_max", "live_lane_pending_max"):
                field = "read_profile_" + suffix
                if field in group_counters:
                    result += sample("kv_case_read_diagnostic_stage_count", group_counters[field],
                                     local | {"partition": partition, "clock_domain": "coordinator", "counter": field})
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
                or not stage_names <= set(resolution["stages"])
                or not set(resolution["stages"]) <= stage_names | {
                    "batch_wait", "postread_cache_fill", "result_materialization"}):
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


def nvmeof_target_metrics(evidence, labels):
    """Export independently sampled target aggregates, never infer request stages."""
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or evidence.get("version") != 1:
        raise ValueError("invalid NVMe-oF target evidence")
    device = token(evidence["block_device"])
    result = ""
    for component, allowed in (
        ("block", ("reads_completed", "reads_merged", "read_sectors", "read_ms",
                   "writes_completed", "writes_merged", "write_sectors", "write_ms",
                   "io_ms", "weighted_io_ms", "read_bytes", "write_bytes")),
        ("cpu", ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"))):
        interval = evidence[component]
        start = count(interval["start_monotonic_ns"])
        end = count(interval["end_monotonic_ns"])
        if end <= start or not isinstance(interval["delta"], dict):
            raise ValueError("invalid NVMe-oF target interval")
        local = labels | {"component": component, "device": device}
        result += sample("kv_nvmeof_target_interval_seconds", (end - start) / 1e9, local)
        for field in allowed:
            if field in interval["delta"]:
                result += sample("kv_nvmeof_target_interval_delta", count(interval["delta"][field]),
                                 local | {"counter": field})
    trace = evidence.get("trace")
    if isinstance(trace, dict) and isinstance(trace.get("available"), bool):
        result += sample("kv_nvmeof_target_trace_available", int(trace["available"]), labels | {"device": device})
    if "clock_ticks_per_second" in evidence:
        result += sample("kv_nvmeof_target_cpu_ticks_per_second",
                         count(evidence["clock_ticks_per_second"]), labels | {"device": device})
    return result


def nvmeof_trace_metrics(evidence, labels):
    if evidence is None:
        return ""
    if not isinstance(evidence, dict) or evidence.get("version") != 1 or evidence.get("available") is not True:
        raise ValueError("invalid NVMe-oF trace evidence")
    start = count(evidence["start_monotonic_ns"])
    end = count(evidence["end_monotonic_ns"])
    if end <= start:
        raise ValueError("invalid NVMe-oF trace interval")
    local = labels | {"device": token(evidence["block_device"]),
                      "controller_id": str(count(evidence["controller_id"]))}
    result = sample("kv_nvmeof_trace_interval_seconds", (end - start) / 1e9, local)
    result += sample("kv_nvmeof_trace_dropped_events", count(evidence["overrun_or_dropped"]), local)
    for stage in ("nvmet", "block"):
        data = evidence["stages"][stage]
        for field in ("count", "unmatched_start", "unmatched_completion", "ambiguous"):
            result += sample("kv_nvmeof_trace_stage_events", count(data[field]),
                             local | {"stage": stage, "statistic": field})
        if count(data["count"]):
            for field in ("sum_ns", "max_ns"):
                result += sample("kv_nvmeof_trace_stage_seconds", count(data[field]) / 1e9,
                                 local | {"stage": stage, "statistic": field.removesuffix("_ns")})
    return result


# These intervals are coordinator-local and may overlap. They are not wire or
# device latency; the retained records, rather than these sums, describe tails.
CORRELATED_READ_PAIRS = {
    "api": ("api_started", "api_finished"),
    "index_resolution": ("index_resolution_started", "index_resolution_finished"),
    "admission_wait": ("admission_wait_started", "admission_wait_finished"),
}
CORRELATED_READ_GROUP_PAIRS = {
    "admission": ("admission_started", "admitted"),
    "actor_to_worker": ("actor_dequeued", "live_worker_dequeued"),
    "live_resolve": ("live_resolve_started", "live_resolve_completed"),
    "flight_to_delivery": ("flight_completed", "request_result_delivered"),
    "live_io": ("live_io_sqe_submitted", "live_io_cqe_observed"),
    "sealed_nvme_request": ("sealed_nvme_request_submitted", "sealed_nvme_completion_observed"),
    "sealed_nvme_validation": ("sealed_nvme_validation_started", "sealed_nvme_validation_finished"),
}
CORRELATED_WRITE_PAIRS = {
    "caller": ("caller_start", "caller_finished"),
    "admission_wait": ("admission_wait_started", "admission_acquired"),
    "live_group_build": ("live_group_build_started", "live_group_built"),
    "live_reserve": ("live_reserve_started", "live_reserve_completed"),
    "live_namespace_setup": ("live_namespace_setup_started", "live_namespace_setup_completed"),
    "live_io": ("live_io_started", "live_io_completed"),
    "live_complete": ("live_complete_started", "live_complete_completed"),
}
VALKEY_STAGES = ("mutex_wait", "blocking_enqueue_to_start", "blocking_return_to_wake",
                 "command_encode", "transport_and_parser", "reply_decode")
VALKEY_CQ_STAGES = {"poll_wait": "poll_calls", "cq_poll": "cq_poll_calls",
                    "wake_to_cq": "wake_to_cq_samples"}
OWNER_WRITE_STAGES = ("control_queue", "session_admission_wait", "appender_queue", "flusher_queue",
                      "append_barrier_wait", "append_wal", "append_validation",
                      "append_basic_validation", "append_conflict_lookup", "append_geometry_capacity",
                      "append_metadata_reservation", "append_descriptor_preparation", "append_persist",
                      "wal_file_persist", "handler", "response_ready", "namespace_persist")
LIVE_PERSIST_STAGES = {"persist_total": "persist_calls", "persist_wal_file": "file_persist_calls",
                       "persist_namespace": "namespace_persist_calls",
                       "persist_inventory_file": "inventory_file_persist_calls",
                       "persist_inventory_namespace": "inventory_namespace_persist_calls",
                       "live_frontier_file": "live_frontier_file_calls",
                       "live_frontier_namespace": "live_frontier_namespace_calls"}


def summarize_correlated_timings(kind, artifact, backend):
    """Reduce admitted raw records to bounded sums; missing endpoints are never zero."""
    if kind not in ("valkey", "read", "write") or not isinstance(artifact, dict):
        raise ValueError("invalid correlated timing artifact")
    token(backend)
    totals = {}
    availability = None

    def add(scope, population, stage, duration, observations=1):
        count(duration)
        count(observations)
        if not observations:
            return
        key = (scope, population, stage)
        total = totals.setdefault(key, [0, 0])
        total[0] += duration
        total[1] += observations

    def intervals(events, pairs, scope, population, name_field, keyed=False):
        if not isinstance(events, list):
            raise ValueError("invalid timing events")
        for stage, (start, end) in pairs.items():
            pending = {}
            for event in events:
                if not isinstance(event, dict):
                    raise ValueError("invalid timing event")
                name = event.get(name_field)
                if name not in (start, end):
                    continue
                if keyed and stage in ("live_io", "sealed_nvme_request"):
                    queue, io_token = event.get("io_queue_id"), event.get("io_token")
                    if queue is None or io_token is None:
                        continue
                    key = (count(queue), count(io_token))
                elif kind == "write" and stage not in ("caller", "admission_wait"):
                    key = (event.get("owner"), event.get("envelope_id"))
                    if key[0] is None:
                        continue
                else:
                    key = ()
                timestamp = count(event.get("elapsed_ns"))
                if name == start:
                    # Ambiguous overlapping starts cannot establish an interval.
                    pending[key] = timestamp if key not in pending else None
                else:
                    begin = pending.pop(key, None)
                    if begin is not None and timestamp >= begin:
                        add(scope, population, stage, timestamp - begin)

    if kind == "valkey":
        profile = artifact.get("profile")
        if isinstance(profile, dict) and profile.get("available") is False:
            profile = None
        if profile is not None:
            if not isinstance(profile, dict) or not isinstance(profile.get("records"), list):
                raise ValueError("invalid Valkey timing profile")
            if type(profile.get("cq_available")) is not bool:
                raise ValueError("invalid Valkey CQ availability")
            availability = {"cq_status": "available" if profile["cq_available"] else "unavailable",
                            "unfinished_workers": 0, "cq_missing_records": 0,
                            "cq_clock_failure_records": 0, "cq_clock_failures": 0}
            for record in profile["records"]:
                if not isinstance(record, dict):
                    raise ValueError("invalid Valkey timing record")
                if type(record.get("worker_finished")) is not bool:
                    raise ValueError("invalid Valkey worker completion")
                if not record["worker_finished"]:
                    availability["unfinished_workers"] += 1
                operation = record.get("operation")
                outcome = record.get("pipeline_outcome")
                if operation not in ("get", "put", "delete", "get_many"):
                    continue
                if outcome not in ("ok", "error", "cancelled", None):
                    raise ValueError("invalid Valkey pipeline outcome")
                population = operation + "_" + (outcome or "incomplete")
                for stage in VALKEY_STAGES:
                    value = record.get(stage + "_ns")
                    if value is not None:
                        add("client_pipeline", population, stage, value)
                cq = record.get("cq")
                if profile.get("cq_available") is True and cq is None:
                    availability["cq_missing_records"] += 1
                if cq is not None:
                    if not isinstance(cq, dict):
                        raise ValueError("invalid Valkey CQ sample")
                    failures = count(cq["clock_failures"])
                    availability["cq_clock_failures"] += failures
                    availability["cq_clock_failure_records"] += int(failures > 0)
                    if profile.get("cq_available") is True and count(cq["clock_failures"]) == 0:
                        for stage, denominator in VALKEY_CQ_STAGES.items():
                            add("client_cq", population, stage, count(cq[stage + "_ns"]),
                                count(cq[denominator]))
    elif kind == "read":
        profile = artifact.get("profile")
        correlation = profile.get("caller_correlation") if isinstance(profile, dict) else None
        trace = correlation.get("trace") if isinstance(correlation, dict) else None
        if trace is not None:
            if not isinstance(trace, dict) or not isinstance(trace.get("calls"), list):
                raise ValueError("invalid read correlation trace")
            for call in trace["calls"]:
                if not isinstance(call, dict) or not isinstance(call.get("groups"), list):
                    raise ValueError("invalid read correlation call")
                status = call.get("status")
                if status not in ("complete", "error", "incomplete", "cancelled_or_incomplete"):
                    raise ValueError("invalid read correlation status")
                intervals(call.get("events"), CORRELATED_READ_PAIRS, "read_call", status, "name")
                for group in call["groups"]:
                    if not isinstance(group, dict):
                        raise ValueError("invalid read correlation group")
                    if group.get("membership_classified") is True:
                        intervals(group.get("events"), CORRELATED_READ_GROUP_PAIRS,
                                  "read_group", status, "name", keyed=True)
    else:
        timeline = artifact.get("timeline")
        if timeline is not None:
            if not isinstance(timeline, dict) or not isinstance(timeline.get("records"), list):
                raise ValueError("invalid write correlation timeline")
            for record in timeline["records"]:
                if not isinstance(record, dict) or type(record.get("completed")) is not bool:
                    raise ValueError("invalid write correlation record")
                population = ("incomplete" if not record["completed"] else
                              "error" if record.get("error") is not None else "success")
                intervals(record.get("events"), CORRELATED_WRITE_PAIRS, "write_call", population, "stage")
            owners = timeline.get("owner_records", [])
            if not isinstance(owners, list):
                raise ValueError("invalid write owner records")
            for owner in owners:
                if not isinstance(owner, dict) or type(owner.get("success")) is not bool:
                    raise ValueError("invalid write owner record")
                population = "success" if owner["success"] else "error"
                for stage in OWNER_WRITE_STAGES:
                    value = owner.get(stage + "_ns")
                    if value is not None:
                        add("write_owner_envelope", population, stage, value)
            groups = timeline.get("live_groups", [])
            if not isinstance(groups, list):
                raise ValueError("invalid live write groups")
            for group in groups:
                if not isinstance(group, dict):
                    raise ValueError("invalid live write group")
                validation = group.get("owner_validation_ns")
                if validation is not None:
                    add("live_owner_group", "observed_group", "owner_validation", validation)
                persistence = group.get("owner_persistence")
                if persistence is not None:
                    if not isinstance(persistence, dict):
                        raise ValueError("invalid live owner persistence")
                    for stage, denominator in LIVE_PERSIST_STAGES.items():
                        if stage + "_ns" in persistence and denominator in persistence:
                            add("live_owner_persistence", denominator, stage,
                                count(persistence[stage + "_ns"]), count(persistence[denominator]))
    result = {"schema_version": 1, "kind": kind, "backend": backend,
              "stages": [{"scope": scope, "population": population, "stage": stage,
                          "sum_ns": values[0], "count": values[1]}
                         for (scope, population, stage), values in sorted(totals.items())]}
    if kind == "valkey":
        result["availability"] = availability
    return result


def correlated_timing_metrics(summaries, labels):
    if summaries is None:
        return ""
    if not isinstance(summaries, list) or len(summaries) > 3:
        raise ValueError("invalid correlated timing summaries")
    result, identities, kinds = "", set(), set()
    for summary in summaries:
        if (not isinstance(summary, dict) or set(summary) != ({"schema_version", "kind", "backend", "stages", "availability"} if summary.get("kind") == "valkey" else {"schema_version", "kind", "backend", "stages"})
                or type(summary["schema_version"]) is not int or summary["schema_version"] != 1
                or summary["kind"] not in ("valkey", "read", "write")
                or summary["kind"] in kinds or not isinstance(summary["stages"], list)
                or len(summary["stages"]) > 512):
            raise ValueError("invalid correlated timing summary")
        kind = summary["kind"]
        kinds.add(kind)
        backend = token(summary["backend"])
        if kind == "valkey":
            availability = summary["availability"]
            if availability is None:
                result += sample("kv_case_valkey_cq_availability", 1,
                                 labels | {"diagnostic_backend": backend, "status": "profile_unavailable"})
            else:
                fields = ("unfinished_workers", "cq_missing_records", "cq_clock_failure_records", "cq_clock_failures")
                if (not isinstance(availability, dict) or set(availability) != set(fields) | {"cq_status"}
                        or availability["cq_status"] not in ("available", "unavailable")):
                    raise ValueError("invalid Valkey CQ availability summary")
                result += sample("kv_case_valkey_cq_availability", 1,
                                 labels | {"diagnostic_backend": backend, "status": availability["cq_status"]})
                for field in fields:
                    result += sample("kv_case_valkey_diagnostic_observations", count(availability[field]),
                                     labels | {"diagnostic_backend": backend, "population": field})
        for entry in summary["stages"]:
            if not isinstance(entry, dict) or set(entry) != {"scope", "population", "stage", "sum_ns", "count"}:
                raise ValueError("invalid correlated timing stage")
            scope, population, stage = entry["scope"], entry["population"], entry["stage"]
            if kind == "valkey":
                valid = (scope == "client_pipeline" and stage in VALKEY_STAGES or
                         scope == "client_cq" and stage in VALKEY_CQ_STAGES)
                valid = valid and population in {op + "_" + outcome
                    for op in ("get", "put", "delete", "get_many")
                    for outcome in ("ok", "error", "cancelled", "incomplete")}
            elif kind == "read":
                valid = ((scope == "read_call" and stage in CORRELATED_READ_PAIRS or
                          scope == "read_group" and stage in CORRELATED_READ_GROUP_PAIRS)
                         and population in ("complete", "error", "incomplete", "cancelled_or_incomplete"))
            else:
                valid = (scope == "write_call" and stage in CORRELATED_WRITE_PAIRS
                         and population in ("success", "error", "incomplete") or
                         scope == "write_owner_envelope" and stage in OWNER_WRITE_STAGES
                         and population in ("success", "error") or
                         scope == "live_owner_group" and stage == "owner_validation" and population == "observed_group" or
                         scope == "live_owner_persistence" and stage in LIVE_PERSIST_STAGES
                         and LIVE_PERSIST_STAGES[stage] == population)
            identity = (kind, backend, scope, population, stage)
            if not valid or identity in identities or count(entry["count"]) == 0:
                raise ValueError("invalid or duplicate correlated timing population")
            identities.add(identity)
            dimensions = labels | {"phase": "measurement", "diagnostic_backend": backend,
                                   "scope": scope, "population": population, "stage": stage}
            result += sample("kv_case_correlated_stage_seconds_sum", count(entry["sum_ns"]) / 1e9, dimensions)
            result += sample("kv_case_correlated_stage_samples", entry["count"], dimensions)
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
    result += spdk_target_metrics(terminal.get("spdk_target_io"), labels)
    result += cold_device_metrics(terminal.get("cold_device_evidence"), labels)
    result += resource_interval_metrics(terminal.get("resource_evidence"), labels)
    result += write_diagnostic_metrics(terminal.get("write_diagnostics"), labels)
    result += read_diagnostic_metrics(terminal.get("read_diagnostics"), labels)
    result += correlated_timing_metrics(terminal.get("correlation_summaries"), labels)
    result += nvmeof_target_metrics(terminal.get("nvmeof_target_evidence"), labels)
    result += nvmeof_trace_metrics(terminal.get("nvmeof_trace_evidence"), labels)
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
            if key == "registered_policy" and key not in policy:
                continue
            value = policy.get(key)
            if value is None and (policy_scope == "effective" or key == "registered_policy"):
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
            if key == "owner_read_cache_bytes" and key not in policy:
                continue
            value = policy.get(key)
            result += sample("kv_case_cache_policy_field_available", int(value is not None),
                             labels | {"policy_scope": policy_scope, "setting": key})
            if value is None and (policy_scope == "effective" or key == "owner_read_cache_bytes"):
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


# Field names and semantics are the pinned implementations, not Redis aliases:
# https://github.com/microsoft/garnet/blob/v2.2.5/libs/server/Metrics/Info/GarnetInfoMetrics.cs
# https://github.com/apache/kvrocks/blob/v2.17.0/src/server/server.cc
INFO_COUNTERS = {
    "garnet": {"total_connections_received", "total_connections_disposed", "rejected_connections",
               "total_commands_processed", "total_net_input_bytes", "total_net_output_bytes",
               "total_found", "total_notfound", "total_cluster_commands_processed",
               "total_write_commands_processed", "total_read_commands_processed"},
    "kvrocks": {"total_connections_received", "total_commands_processed", "total_net_input_bytes",
                "total_net_output_bytes", "sync_full", "sync_partial_ok", "sync_partial_err",
                "client_output_buffer_limit_disconnections", "keyspace_hits", "keyspace_misses",
                "flush_count", "compaction_count", "block_cache_hit", "block_cache_miss",
                "block_cache_index_hit", "block_cache_index_miss", "block_cache_filter_hit",
                "block_cache_filter_miss", "block_cache_data_hit", "block_cache_data_miss"},
}
INFO_GAUGES = {
    "garnet": {"proc_physical_memory_size", "proc_private_memory_size", "gc_heap_bytes",
               "store_index_size", "store_mainlog_memory_size", "store_readcache_memory_size",
               "total_main_store_size", "aof_memory_size", "total_connections_active",
               "instantaneous_ops_per_sec", "CurrentVersion", "LastCheckpointedVersion",
               "IndexBucketCount", "IndexBucketSizeBytes", "IndexMemorySizeBytes",
               "IndexOverflowBucketCount", "IndexOverflowMemorySizeBytes", "IndexOverflowMaxMemorySizeBytes",
               "IndexTotalMemorySizeBytes", "IndexMemoryBudgetBytes", "IndexMemoryChargedToLogBudgetBytes",
               "Log.PageSizeBytes", "Log.MaxPageCount", "Log.AllocatedPageCount", "Log.MaxMemorySizeBytes",
               "Log.CurrentMemorySizeBytes", "Log.CurrentHeapSizeBytes", "Log.BeginAddress",
               "Log.HeadAddress", "Log.SafeReadOnlyAddress", "Log.FlushedUntilAddress", "Log.TailAddress",
               "ReadCache.PageSizeBytes", "ReadCache.MaxPageCount", "ReadCache.AllocatedPageCount",
               "ReadCache.MaxMemorySizeBytes", "ReadCache.CurrentMemorySizeBytes", "ReadCache.CurrentHeapSizeBytes",
               "ReadCache.BeginAddress", "ReadCache.HeadAddress", "ReadCache.TailAddress",
               "CommittedBeginAddress", "CommittedUntilAddress", "FlushedUntilAddress", "BeginAddress",
               "TailAddress", "SafeAofAddress"},
    "kvrocks": {"used_memory_rss", "used_memory_lua", "used_memory_startup",
                "instantaneous_ops_per_sec", "pubsub_channels", "pubsub_patterns",
                "loading", "bgsave_in_progress", "last_bgsave_time", "last_bgsave_time_sec",
                "block_cache_usage", "all_mem_tables", "cur_mem_tables", "snapshots",
                "num_immutable_tables", "num_running_flushes", "memtable_flush_pending",
                "compaction_pending", "num_running_compactions", "num_live_versions", "num_super_version",
                "num_background_errors", "put_per_sec", "get_per_sec", "seek_per_sec", "next_per_sec", "prev_per_sec"},
}
KVROCKS_CF_GAUGES = {"block_cache_pinned_usage", "estimate_keys", "index_and_filter_cache_usage",
                     "estimate_pending_compaction_bytes"}
KVROCKS_CF_COUNTERS = {"level0_file_limit_slowdown", "level0_file_limit_stop",
                       "pending_compaction_bytes_slowdown", "pending_compaction_bytes_stop",
                       "level0_file_limit_stop_with_ongoing_compaction",
                       "level0_file_limit_slowdown_with_ongoing_compaction",
                       "memtable_count_limit_slowdown", "memtable_count_limit_stop"}


def info_section(address, port, section, deadline=None):
    """Bound a complete RESP reply, including a peer that makes slow progress."""
    deadline = min(deadline if deadline is not None else float("inf"), time.monotonic() + 0.15)
    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("INFO collection deadline")
        return seconds
    command = ("*2\r\n$4\r\nINFO\r\n$" + str(len(section)) + "\r\n" + section + "\r\n").encode()
    with socket.create_connection((address, port), timeout=remaining()) as connection:
        connection.settimeout(remaining())
        connection.sendall(command)
        def receive(size):
            connection.settimeout(remaining())
            data = connection.recv(size)
            if not data:
                raise ValueError("truncated INFO reply")
            return data
        header = bytearray()
        for _ in range(128):
            header.extend(receive(1))
            if header.endswith(b"\r\n"):
                break
        if not re.fullmatch(rb"\$[0-9]+\r\n", header):
            raise ValueError("INFO did not return a RESP bulk string")
        size = int(header[1:-2])
        if size > 256 * 1024:
            raise ValueError("INFO reply exceeds bound")
        data = bytearray()
        while len(data) < size + 2:
            data.extend(receive(min(65536, size + 2 - len(data))))
        if data[-2:] != b"\r\n":
            raise ValueError("truncated INFO reply")
    fields = {}
    for line in data[:-2].decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition(":")
        if not separator or key in fields:
            raise ValueError("malformed or duplicate INFO field")
        fields[key] = value
    return fields


def info_payload(target, job, labels):
    if (not isinstance(target, dict) or set(target) != {"engine", "address", "port"}
            or target["engine"] not in INFO_COUNTERS or not isinstance(job, dict)
            or target["engine"] != job.get("backend") or target["port"] != 6379):
        raise ValueError("invalid explicit INFO target")
    address = str(ipaddress.IPv4Address(target["address"]))
    if address != target["address"] or ipaddress.IPv4Address(address).is_unspecified:
        raise ValueError("invalid INFO address")
    engine = target["engine"]
    result = ""
    sections = {}
    requested = ("server", "stats", "memory", "persistence",
                 "store" if engine == "garnet" else "rocksdb")
    # Leave headroom within the host Prometheus job's two-second scrape budget.
    deadline = time.monotonic() + 0.75
    for section in requested:
        try:
            sections[section] = info_section(address, target["port"], section, deadline)
            result += sample("kv_observer_info_section_available", 1, labels | {"section": section})
        except (OSError, ValueError, UnicodeError):
            result += sample("kv_observer_info_section_available", 0, labels | {"section": section})
    # Garnet explicitly returns zeroed stats when the sampling monitor is off.
    # Missing server evidence is also insufficient to establish real counters.
    if engine == "garnet" and sections.get("server", {}).get("monitor_task") != "enabled":
        sections.pop("stats", None)
    for section in requested[1:]:
        for key, value in sections.get(section, {}).items():
            if not value.isascii() or not value.isdigit():
                continue
            cf = re.fullmatch(r"([a-z0-9_]+)\[([A-Za-z0-9_.-]{1,64})\]", key) if engine == "kvrocks" and section == "rocksdb" else None
            if key in INFO_COUNTERS[engine] or cf and cf.group(1) in KVROCKS_CF_COUNTERS:
                metric = "kv_observer_info_counter_total"
            elif key in INFO_GAUGES[engine] or cf and cf.group(1) in KVROCKS_CF_GAUGES:
                metric = "kv_observer_info_gauge"
            else:
                continue
            result += sample(metric, int(value), labels | {"field": key})
    return result


def host_payload(config, selection):
    if set(selection) not in ({"job", "processes"}, {"job", "processes", "info"}):
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
    if "info" in selection:
        result += info_payload(selection["info"], selection["job"], labels)
    return result.encode()


def metric_types(body):
    names = set()
    for line in body.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        matched = METRIC.match(line)
        if matched and (matched.group().startswith(("kv_case_", "kv_observer_cgroup_", "kv_observer_info_"))
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
