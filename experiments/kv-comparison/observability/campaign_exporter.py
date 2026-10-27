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

MAX_SELECTION = 256 * 1024
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
    "write_durable_batches", "write_concurrent_admitted", "write_concurrent_completed"}


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
                  "message_read_requests")))
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
    result += read_path_metrics(terminal.get("read_evidence"), labels)
    result += resource_interval_metrics(terminal.get("resource_evidence"), labels)
    result += write_diagnostic_metrics(terminal.get("write_diagnostics"), labels)
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
    return scoped(exporter.exposition(current), labels) + terminal_metrics(selection["terminal"], labels).encode()


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
