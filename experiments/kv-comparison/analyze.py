#!/usr/bin/env python3
"""Pool raw observations only within an exact admitted comparison boundary."""
import argparse
import csv
import hashlib
import ipaddress
import json
import math
import re
from pathlib import Path
import sys

import floors
import local


_FOCUSED_ARTIFACTS = {
    "media_read": "media_evidence", "fio_floor": "fio_result",
    "native_media_read": "media_evidence",
    "block_trace": "block_trace_evidence", "interference": "interference_result",
    "write_wait_diagnostic": "write_wait_metrics",
    "net_floor": "net_floor_result", "ssd_floor": "ssd_floor_result",
    "ssd_write_floor": "ssd_write_floor_result",
}

_FIO_FIELDS = "schema_version case_id location device_identity namespace_uuid block_bytes requested_depth effective_depth actual_direct read_only elapsed_seconds read_ios read_bytes errors latency_ns fio_sha256 raw_fio_path region_bytes write_ios opened_read_only quiescent target_buffered_io"


def _focused_object(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields.split()):
        raise ValueError("focused artifact has an unexpected schema")


def _focused_uint(value, *, nullable=False):
    if value is None and nullable:
        return
    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise ValueError("invalid focused artifact count")


def _focused_number(value, *, nullable=False):
    if value is None and nullable:
        return
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 2**64 - 1:
        raise ValueError("invalid focused artifact number")


def _focused_bool(value):
    if type(value) is not bool:
        raise ValueError("invalid focused artifact boolean")


def _focused_strings(value):
    if not isinstance(value, list) or any(not isinstance(x, str) or not x for x in value):
        raise ValueError("invalid focused artifact reasons")


def _focused_text(value):
    if not isinstance(value, str) or not value:
        raise ValueError("missing focused artifact identity")


def successful_read_keys(samples, batch_size):
    return sum((1 if row["operation"] == "read" else batch_size if row["operation"] == "get_many" else 0)
               for row in samples if row["outcome"] in ("ok", "success"))


def _validate_focused_artifact(name, data, job, receipt):
    if name == "device_counters":
        _focused_object(data, "before after")
        rows = []
        for boundary in ("before", "after"):
            values = data[boundary]
            if not isinstance(values, list) or not values:
                raise ValueError("invalid device counters")
            indexed = {}
            for row in values:
                _focused_object(row, "owner_index major minor read_ops read_sectors write_ops write_sectors")
                index = row["owner_index"]
                if job.get("backend") == "native":
                    if type(index) is not int or index != -1:
                        raise ValueError("invalid client device index")
                else:
                    _focused_uint(index)
                    if index >= job.get("owner_count", 5):
                        raise ValueError("owner device index outside active owners")
                for key in ("major", "minor", "read_ops", "read_sectors", "write_ops", "write_sectors"):
                    _focused_uint(row[key])
                identity = (index, row["major"], row["minor"])
                if identity in indexed:
                    raise ValueError("duplicate device counter")
                indexed[identity] = row
            rows.append(indexed)
        if rows[0].keys() != rows[1].keys() or any(
                rows[1][identity][counter] < rows[0][identity][counter]
                for identity in rows[0] for counter in ("read_ops", "read_sectors", "write_ops", "write_sectors")):
            raise ValueError("device counter identity changed or decreased")
        return
    schemas = {
        "media_evidence": "schema_version case_id expected status reason_codes measurement_drained owner_devices successful_reads read_ops_per_success history_loads_before history_loads_after history_prewarm_verified",
        "fio_result": _FIO_FIELDS,
        "block_trace_evidence": "schema_version case_id scope available qualified device_identity files lost_events truncated reasons",
        "phase_timing": "schema_version case_id spans",
        "interference_result": "schema_version case_id read put measurement_drained read_offered_per_second measurement_elapsed_ns window_start_ns window_end_ns",
        "write_wait_metrics": "schema_version case_id enabled windows saturated complete",
        "net_floor_result": "schema_version case_id tool mode interface provider endpoint client_host server_host client_address server_address device build runs",
        "ssd_floor_result": _FIO_FIELDS + " state age_seconds lat_log",
        "ssd_write_floor_result": "schema_version case_id mode block_bytes region_bytes location preheat requested_depth actual_direct device_identity elapsed_seconds total_operations logged_operations dropped_operations sampling summary_population write sync total raw lat_log",
    }
    if name == "media_evidence" and job.get("case_kind") == "native_media_read":
        schemas[name] = schemas[name].replace("owner_devices", "client_devices")
    if name == "media_evidence" and data.get("schema_version") == 2:
        schemas[name] += " device_bytes_per_read"
    if name == "ssd_floor_result" and "host" in job.get("ssd", {}):
        schemas[name] += " host_index"
    if name == "ssd_write_floor_result":
        spec = job.get("ssd_write", {})
        if spec.get("mode") in floors.APPEND_MODES:
            schemas[name] = schemas[name].replace(" preheat", "") + " host_index warmup_operations"
        elif "host" in spec:
            schemas[name] += " host_index"
    _focused_object(data, schemas[name])
    if (type(data["schema_version"]) is not int
            or data["schema_version"] not in ((1, 2) if name == "media_evidence" else (1,))
            or data["case_id"] != job.get("case_id")):
        raise ValueError("focused artifact schema or case identity mismatch")
    if name == "ssd_write_floor_result":
        spec = job.get("ssd_write", {})
        dimensions = ("mode", "block_bytes", "region_bytes", "location")
        if spec.get("mode") not in floors.APPEND_MODES:
            dimensions += ("preheat",)
        if job.get("case_kind") != "ssd_write_floor" or any(
                data[key] != spec.get(key) for key in dimensions):
            raise ValueError("SSD write dimensions differ from job")
        if "host" in spec:
            host_index = {"client": -1, "owner0": 0}.get(spec["host"])
            if host_index is None or type(data["host_index"]) is not int or data["host_index"] != host_index:
                raise ValueError("SSD write host differs from job")
            if type(receipt.get("host_index")) is not int or receipt["host_index"] != host_index:
                raise ValueError("SSD write receipt host differs from job")
        _focused_object(data["raw"], "path sha256")
        _focused_object(data["lat_log"], "path sha256 rows columns")
        if data["lat_log"]["columns"] != ["write_ns", "sync_ns", "total_ns", "offset"]:
            raise ValueError("invalid SSD write log columns")
        return
    if name == "ssd_floor_result":
        ssd = job.get("ssd", {})
        if any(data[key] != ssd.get(key) for key in ("state", "age_seconds", "region_bytes", "block_bytes")):
            raise ValueError("SSD dimensions differ from job")
        if "host" in ssd:
            host_index = {"client": -1, "owner0": 0}.get(ssd["host"])
            if (host_index is None or type(data["host_index"]) is not int or data["host_index"] != host_index
                    or type(receipt.get("host_index")) is not int or receipt["host_index"] != host_index):
                raise ValueError("SSD floor host differs from job or receipt")
        fio_data = {key: data[key] for key in _FIO_FIELDS.split()}
        fio_job = {**job, "fio": {"location": "local_file", "block_bytes": ssd.get("block_bytes"),
                                  "queue_depth": ssd.get("queue_depth")}}
        _validate_focused_artifact("fio_result", fio_data, fio_job, receipt)
        _focused_object(data["lat_log"], "path sha256 rows columns")
        _focused_uint(data["lat_log"]["rows"])
        if data["lat_log"]["columns"] != ["time_ms", "latency_ns", "direction", "block_bytes", "offset"]:
            raise ValueError("invalid SSD latency log columns")
        return
    if name == "net_floor_result":
        net = job.get("net", {})
        if any(data[key] != net.get(key) for key in ("tool", "mode", "interface", "provider", "endpoint")):
            raise ValueError("network dimensions differ from job")
        if type(data["client_host"]) is not int or type(data["server_host"]) is not int or (data["client_host"], data["server_host"]) != (0, 1):
            raise ValueError("network host roles differ from job")
        for key in ("client_address", "server_address"):
            _focused_text(data[key])
        if data["device"] is not None:
            _focused_text(data["device"])
        if data["build"] is not None:
            _focused_object(data["build"], "compiler source_sha256 binary_sha256")
            _focused_text(data["build"]["compiler"])
            for key in ("source_sha256", "binary_sha256"):
                if not isinstance(data["build"][key], str) or not re.fullmatch(r"[0-9a-f]{64}", data["build"][key]):
                    raise ValueError("invalid network build hash")
        if not isinstance(data["runs"], list) or len(data["runs"]) != len(net.get("sizes", [])):
            raise ValueError("network run count mismatch")
        for run, size in zip(data["runs"], net["sizes"]):
            if not isinstance(run, dict):
                raise ValueError("invalid network run")
            _focused_object({key: value for key, value in run.items() if key != "attempts"},
                            "size_bytes iterations status argv_client argv_server latency_semantics latency_us setsockopt raw_client_output raw_server_output raw_sha256 error")
            if "attempts" in run and (type(run["attempts"]) is not int or not 1 <= run["attempts"] <= 3):
                raise ValueError("invalid network attempt count")
            if run["size_bytes"] != size or run["iterations"] != net.get("iterations") or run["status"] not in ("completed", "failed"):
                raise ValueError("network run dimensions or status mismatch")
            if run["latency_semantics"] not in ("half_round_trip", "round_trip"):
                raise ValueError("invalid network latency semantics")
            for key in ("argv_client", "argv_server"):
                if not isinstance(run[key], list) or any(not isinstance(arg, str) for arg in run[key]):
                    raise ValueError("invalid network argv")
            _focused_object(run["latency_us"], "min p50 avg p90 p99 p999 max stdev")
            for value in run["latency_us"].values():
                _focused_number(value, nullable=True)
            if run["error"] is not None and not isinstance(run["error"], str):
                raise ValueError("invalid network error")
            for key in ("raw_client_output", "raw_server_output"):
                if not isinstance(run[key], str) or len(run[key].encode("utf-8")) > 16 * 1024:
                    raise ValueError("network raw output exceeds bound")
            if hashlib.sha256(run["raw_client_output"].encode("utf-8")).hexdigest() != run["raw_sha256"]:
                raise ValueError("network raw output hash mismatch")
        return
    if name == "media_evidence":
        if data["expected"] != job.get("media", {}).get("expected") or data["status"] not in ("qualified", "unqualified"):
            raise ValueError("media expectation or status mismatch")
        _focused_strings(data["reason_codes"])
        _focused_bool(data["measurement_drained"])
        _focused_uint(data["successful_reads"])
        for key in ("history_loads_before", "history_loads_after"):
            _focused_uint(data[key], nullable=True)
        if data["history_prewarm_verified"] is not None:
            _focused_bool(data["history_prewarm_verified"])
        if data["history_loads_before"] is not None and data["history_loads_after"] is not None and data["history_loads_after"] < data["history_loads_before"]:
            raise ValueError("history counter decreased")
        _focused_number(data["read_ops_per_success"], nullable=True)
        client = job.get("case_kind") == "native_media_read"
        field, index_field = ("client_devices", "client_index") if client else ("owner_devices", "owner_index")
        if not isinstance(data[field], list) or (client and len(data[field]) > 1):
            raise ValueError("invalid media device list")
        devices, delta, counters_complete = set(), 0, True
        sectors_delta, sectors_complete = 0, True
        counters = ("read_ops",) if data["schema_version"] == 1 else ("read_ops", "read_sectors", "write_ops", "write_sectors")
        for device in data[field]:
            _focused_object(device, index_field + " major minor " + " ".join(
                f"{counter}_{boundary}" for counter in counters for boundary in ("before", "after")))
            if client:
                if type(device[index_field]) is not int or device[index_field] != -1:
                    raise ValueError("invalid client device index")
            else:
                _focused_uint(device[index_field])
                if device[index_field] >= job.get("owner_count", 5):
                    raise ValueError("owner device index outside active owners")
            for key in ("major", "minor"):
                _focused_uint(device[key])
            for counter in counters:
                for boundary in ("before", "after"):
                    _focused_uint(device[f"{counter}_{boundary}"], nullable=True)
                first, last = device[f"{counter}_before"], device[f"{counter}_after"]
                if first is not None and last is not None and last < first:
                    raise ValueError("decreasing owner device counter")
                if first is None or last is None:
                    sectors_complete = False
                elif counter == "read_sectors":
                    sectors_delta += last - first
            identity = (device[index_field], device["major"], device["minor"])
            if identity in devices:
                raise ValueError("duplicate owner device counter")
            devices.add(identity)
            before, after = device["read_ops_before"], device["read_ops_after"]
            if before is None or after is None:
                counters_complete = False
            elif after < before:
                raise ValueError("decreasing owner device counter")
            else:
                delta += after - before
        ratio = delta / data["successful_reads"] if devices and counters_complete and data["successful_reads"] else None
        if data["read_ops_per_success"] != ratio:
            raise ValueError("media ratio differs from observed counters")
        if data["schema_version"] == 2:
            _focused_number(data["device_bytes_per_read"], nullable=True)
            expected_bytes = 512 * sectors_delta / delta if devices and counters_complete and sectors_complete and delta else None
            if data["device_bytes_per_read"] != expected_bytes:
                raise ValueError("media bytes per read differs from observed counters")
        gate = receipt.get("measurement_evidence", {})
        if data["status"] == "qualified":
            if (data["reason_codes"] or not data["measurement_drained"] or ratio is None
                    or gate.get("gate_released") is not True or gate.get("completion_gate_observed") is not True
                    or (data["expected"] == "cold" and ratio < .9)
                    or (data["expected"] == "memory" and ratio > .06)
                    or job.get("media", {}).get("history") == "prewarm"):
                raise ValueError("media qualification lacks gate, counter or approved history proof")
        elif not data["reason_codes"]:
            raise ValueError("unqualified media evidence needs a reason")
    elif name == "fio_result":
        fio = job.get("fio", {})
        if (data["location"] != fio.get("location") or data["block_bytes"] != fio.get("block_bytes")
                or data["requested_depth"] != fio.get("queue_depth")):
            raise ValueError("fio dimensions differ from job")
        for key in ("block_bytes", "requested_depth", "read_ios", "read_bytes", "errors", "region_bytes", "write_ios"):
            _focused_uint(data[key])
        _focused_uint(data["effective_depth"], nullable=True)
        _focused_number(data["elapsed_seconds"])
        for key in ("actual_direct", "read_only", "opened_read_only", "quiescent"):
            _focused_bool(data[key])
        for key in ("device_identity", "namespace_uuid"):
            _focused_text(data[key])
        _focused_text(data["raw_fio_path"])
        if data["target_buffered_io"] is not None and (type(data["target_buffered_io"]) is not int or data["target_buffered_io"] not in (0, 1)):
            raise ValueError("invalid fio target buffered_io")
        if data["location"] == "local_file" and data["target_buffered_io"] is not None:
            raise ValueError("unexpected local fio target readback")
        if data["write_ios"] != 0:
            raise ValueError("fio reported write I/O")
        if data["errors"] != 0:
            raise ValueError("fio reported an error")
        if not re.fullmatch(r"[0-9a-f]{64}", data["fio_sha256"] if isinstance(data["fio_sha256"], str) else ""):
            raise ValueError("fio output hash missing")
        _focused_object(data["latency_ns"], "p50 p99 p999")
        for value in data["latency_ns"].values():
            _focused_number(value)
        if not data["latency_ns"]["p50"] <= data["latency_ns"]["p99"] <= data["latency_ns"]["p999"]:
            raise ValueError("fio latency quantiles out of order")
    elif name == "block_trace_evidence":
        if data["scope"] != job.get("trace", {}).get("scope"):
            raise ValueError("block trace scope mismatch")
        for key in ("available", "qualified", "truncated"):
            _focused_bool(data[key])
        _focused_uint(data["lost_events"], nullable=not data["available"] and not data["qualified"])
        _focused_strings(data["reasons"])
        if data["device_identity"] is not None:
            _focused_text(data["device_identity"])
        if not isinstance(data["files"], list):
            raise ValueError("invalid block trace files")
        for row in data["files"]:
            _focused_object(row, "file_identity role matching_operations")
            _focused_text(row["file_identity"])
            if row["role"] not in ("value_wal", "authority_history", "frontier_metadata", "unknown"):
                raise ValueError("invalid block trace role")
            _focused_uint(row["matching_operations"])
        if data["qualified"] and (not data["available"] or data["device_identity"] is None or data["lost_events"] or data["truncated"] or data["reasons"]):
            raise ValueError("incomplete trace cannot qualify")
        if not data["qualified"] and not data["reasons"]:
            raise ValueError("unqualified trace needs a reason")
    elif name == "phase_timing":
        if not isinstance(data["spans"], list):
            raise ValueError("invalid phase spans")
        for span in data["spans"]:
            _focused_object(span, "operation host_index monotonic_start_ns monotonic_end_ns status")
            if span["operation"] not in ("dataset_preload", "export_copy", "target_setup", "backend_deploy", "target_teardown"):
                raise ValueError("invalid phase operation")
            _focused_uint(span["host_index"])
            _focused_uint(span["monotonic_start_ns"])
            _focused_uint(span["monotonic_end_ns"])
            _focused_text(span["status"])
            if span["monotonic_end_ns"] < span["monotonic_start_ns"]:
                raise ValueError("reversed phase span")
    elif name == "interference_result":
        _focused_bool(data["measurement_drained"])
        _focused_uint(data["read_offered_per_second"])
        _focused_uint(data["measurement_elapsed_ns"])
        _focused_uint(data["window_start_ns"])
        _focused_uint(data["window_end_ns"])
        if data["window_end_ns"] <= data["window_start_ns"] or data["measurement_elapsed_ns"] != data["window_end_ns"] - data["window_start_ns"]:
            raise ValueError("invalid interference gate window")
        if data["read_offered_per_second"] != job.get("interference", {}).get("read_offered_per_second"):
            raise ValueError("interference rate differs from job")
        for stream in ("read", "put"):
            row = data[stream]
            _focused_object(row, "offered admitted completed successful rejected failed first_dispatch_ns last_completion_ns")
            for key in ("offered", "admitted", "completed", "successful", "rejected", "failed"):
                _focused_uint(row[key])
            for key in ("first_dispatch_ns", "last_completion_ns"):
                _focused_uint(row[key], nullable=True)
            first, last = row["first_dispatch_ns"], row["last_completion_ns"]
            if (first is None) != (last is None):
                raise ValueError("incomplete interference stream window")
            if first is not None and not data["window_start_ns"] <= first <= last <= data["window_end_ns"]:
                raise ValueError("interference stream outside gate window")
            if (row["offered"] != row["admitted"] + row["rejected"]
                    or row["admitted"] != row["completed"] + row["failed"]
                    or row["successful"] > row["completed"]
                    or (data["measurement_drained"] and row["failed"] != 0)):
                raise ValueError("interference stream accounting mismatch")
    else:
        _focused_bool(data["enabled"])
        _focused_bool(data["saturated"])
        _focused_bool(data["complete"])
        if data["enabled"] != job.get("write_wait_diagnostics", {}).get("enabled"):
            raise ValueError("write wait diagnostic mode mismatch")
        if not isinstance(data["windows"], list):
            raise ValueError("invalid wait windows")
        seen = set()
        for row in data["windows"]:
            _focused_object(row, "wait observations depth_sum depth_max wake_attempts wake_coalesced")
            if row["wait"] not in ("owner_enqueue_to_dequeue", "admitted_to_append_enqueue", "harvest_to_durable") or row["wait"] in seen:
                raise ValueError("invalid duplicate wait window")
            seen.add(row["wait"])
            for key in ("observations", "depth_sum", "depth_max", "wake_attempts", "wake_coalesced"):
                _focused_uint(row[key])
            if row["wake_coalesced"] > row["wake_attempts"]:
                raise ValueError("invalid coalesced wake count")
        if (not data["enabled"] and seen) or (data["enabled"] and len(seen) != 3):
            raise ValueError("write wait windows do not match diagnostic mode")


def _verify_fio_command(root, receipt):
    binding = receipt.get("fio_command")
    _focused_object(binding, "path sha256")
    path, checksum = binding["path"], binding["sha256"]
    if (not isinstance(path, str) or Path(path).is_absolute()
            or any(part in ("", ".", "..") for part in path.split("/"))
            or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
        raise ValueError("invalid fio command binding")
    candidate = (root / path).resolve()
    if root not in candidate.parents:
        raise ValueError("fio command path escapes case directory")
    try:
        raw = candidate.read_bytes()
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise ValueError("fio command hash mismatch")
        argv = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("fio command unavailable") from exc
    if not isinstance(argv, list) or any(not isinstance(arg, str) or not arg for arg in argv):
        raise ValueError("invalid fio command argv")
    for option in ("--readonly", "--direct=1", "--rw=randread", "--output-format=json", "--numjobs=1"):
        if argv.count(option) != 1:
            raise ValueError("fio command lacks required exact option")
    if len([arg for arg in argv if arg.startswith("--name=") and len(arg) > len("--name=")]) != 1:
        raise ValueError("fio command requires one named job")
    if any(arg.startswith("--readonly=") for arg in argv):
        raise ValueError("conflicting fio readonly option")
    for prefix, expected in (("--direct", "--direct=1"), ("--rw", "--rw=randread"),
                             ("--output-format", "--output-format=json"), ("--numjobs", "--numjobs=1")):
        if any(arg.startswith(prefix + "=") and arg != expected or arg == prefix for arg in argv):
            raise ValueError("conflicting fio command option")


def _verify_fio_raw(raw, data):
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid raw fio JSON") from exc
    if not isinstance(document, dict) or not isinstance(document.get("jobs"), list) or len(document["jobs"]) != 1:
        return False, False
    fio = document["jobs"][0]
    if not isinstance(fio, dict):
        return False, False
    read = fio.get("read") if isinstance(fio.get("read"), dict) else {}
    write = fio.get("write") if isinstance(fio.get("write"), dict) else {}
    clat = read.get("clat_ns") if isinstance(read.get("clat_ns"), dict) else {}
    percentiles = clat.get("percentile") if isinstance(clat.get("percentile"), dict) else {}
    observed = {}
    for destination, source, key in (("read_ios", read, "total_ios"), ("read_bytes", read, "io_bytes"),
                                     ("write_ios", write, "total_ios"), ("errors", fio, "error")):
        if key in source:
            _focused_uint(source[key])
            observed[destination] = source[key]
    if "runtime" in read:
        _focused_number(read["runtime"])
        observed["elapsed_seconds"] = read["runtime"] / 1000
    elif "elapsed" in fio:
        _focused_number(fio["elapsed"])
        observed["elapsed_seconds"] = fio["elapsed"]
    if any(data[key] != value for key, value in observed.items()):
        raise ValueError("fio summary differs from raw fio observations")
    latency_count = 0
    for destination, key in (("p50", "50.000000"), ("p99", "99.000000"), ("p999", "99.900000")):
        if key in percentiles:
            _focused_number(percentiles[key])
            if data["latency_ns"][destination] != percentiles[key]:
                raise ValueError("fio latency differs from raw fio observations")
            latency_count += 1
    complete = len(observed) == 5 and latency_count == 3
    try:
        percent = fio["iodepth_level"][str(data["requested_depth"])]
    except (KeyError, TypeError):
        return complete, False
    _focused_number(percent)
    if percent > 100:
        raise ValueError("invalid fio depth percentage")
    expected = data["requested_depth"] if percent >= 90 else None
    if data["effective_depth"] != expected:
        raise ValueError("fio effective depth differs from raw observations")
    return complete, expected is not None


def _focused_qualification(name, data, root, receipt):
    reasons = []
    if name in ("fio_result", "ssd_floor_result"):
        path = data["raw_fio_path"]
        if Path(path).is_absolute() or any(part in ("", ".", "..") for part in path.split("/")):
            raise ValueError("invalid raw fio path")
        candidate = (root / path).resolve()
        if root not in candidate.parents:
            raise ValueError("raw fio path escapes case directory")
        try:
            raw = candidate.read_bytes()
        except OSError as exc:
            raise ValueError("raw fio output unavailable") from exc
        if hashlib.sha256(raw).hexdigest() != data["fio_sha256"]:
            raise ValueError("raw fio output hash mismatch")
        _verify_fio_command(root, receipt)
        raw_complete, depth_confirmed = _verify_fio_raw(raw, data)
        if not raw_complete:
            reasons.append("raw_fio_fields_unavailable")
        if data["read_bytes"] != data["read_ios"] * data["block_bytes"]:
            raise ValueError("fio read bytes disagree with completed blocks")
        if not data["elapsed_seconds"] or not data["read_ios"] or not data["read_bytes"]:
            reasons.append("read_exposure_unavailable")
        depth_reasons = [] if depth_confirmed else ["effective_depth_unobserved"]
        if name == "fio_result" and data["location"] == "local_file" and data["region_bytes"] < 1024**3:
            reasons.append("region_too_small")
        if data["location"] == "nvmeof_namespace" and data["target_buffered_io"] != 1:
            reasons.append("target_not_buffered")
        if not data["actual_direct"]:
            reasons.append("direct_io_unproven")
        if not data["read_only"]:
            reasons.append("read_only_unproven")
        if not data["opened_read_only"]:
            reasons.append("read_only_open_unproven")
        if not data["quiescent"]:
            reasons.append("quiescence_unproven")
        if data["errors"]:
            reasons.append("fio_errors")
    elif name == "interference_result":
        windows = [(data[s]["first_dispatch_ns"], data[s]["last_completion_ns"]) for s in ("read", "put")]
        if any(start is None for start, _ in windows):
            reasons.append("stream_window_unavailable")
        else:
            overlap = max(0, min(end for _, end in windows) - max(start for start, _ in windows))
            if 10 * overlap < 9 * (data["window_end_ns"] - data["window_start_ns"]):
                reasons.append("stream_overlap_below_threshold")
        if not data["measurement_drained"]:
            reasons.append("measurement_not_drained")
    elif name == "write_wait_metrics":
        if data["saturated"]:
            reasons.append("counters_saturated")
        if not data["complete"]:
            reasons.append("wake_metrics_incomplete")
    result = {"qualified": not reasons, "reasons": reasons}
    if name in ("fio_result", "ssd_floor_result"):
        result["queue_depth"] = {"qualified": not depth_reasons, "reasons": depth_reasons}
    elif name == "write_wait_metrics":
        result["depth"] = {"qualified": not data["saturated"],
                           "reasons": ["counters_saturated"] if data["saturated"] else []}
        result["wake"] = {"qualified": not data["saturated"] and data["complete"],
                          "reasons": (["counters_saturated"] if data["saturated"] else []) +
                                     (["wake_metrics_incomplete"] if not data["complete"] else [])}
    return result


def _verify_media_snapshots(root, receipt, evidence):
    gate = receipt.get("measurement_evidence")
    if not isinstance(gate, dict):
        raise ValueError("missing measurement snapshot bindings")
    frames = []
    for field in ("before", "end"):
        path, checksum = gate.get(field), gate.get(field + "_sha256")
        if (not isinstance(path, str) or not path or Path(path).is_absolute()
                or any(part in ("", ".", "..") for part in path.split("/"))
                or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
            raise ValueError("missing measurement snapshot binding")
        candidate = (root / path).resolve()
        if root not in candidate.parents:
            raise ValueError("measurement snapshot escapes case directory")
        try:
            raw = candidate.read_bytes()
            if hashlib.sha256(raw).hexdigest() != checksum:
                raise ValueError("measurement snapshot hash mismatch")
            frame = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("unreadable measurement snapshot") from exc
        if not isinstance(frame, dict) or not isinstance(frame.get("hosts"), list):
            raise ValueError("invalid measurement snapshot hosts")
        frames.append(frame["hosts"])
    client = "client_devices" in evidence
    devices = evidence["client_devices"] if client else evidence["owner_devices"]
    if client:
        try:
            boots = [hosts[0]["nvme"]["host_identity"]["boot_id"] for hosts in frames]
        except (IndexError, KeyError, TypeError) as exc:
            raise ValueError("measurement client boot identity missing") from exc
        if not isinstance(boots[0], str) or not boots[0] or boots[0] != boots[1]:
            raise ValueError("measurement client boot identity changed")
    for device in devices:
        owner = device["client_index"] if client else device["owner_index"]
        for hosts, boundary in zip(frames, ("before", "after")):
            counters = {"read_ops": 3} if evidence.get("schema_version", 1) == 1 else {
                "read_ops": 3, "read_sectors": 5, "write_ops": 7, "write_sectors": 9}
            if owner + 1 >= len(hosts) or not isinstance(hosts[owner + 1], dict):
                raise ValueError("measurement snapshot host missing")
            host = hosts[owner + 1]
            if client:
                expected = {"owner_index": -1, "major": device["major"], "minor": device["minor"],
                            **{counter: device[f"{counter}_{boundary}"] for counter in counters}}
                if host.get("media_device") != expected:
                    raise ValueError("client backing device observation differs")
            diskstats = host.get("diskstats")
            if not isinstance(diskstats, str):
                raise ValueError("measurement snapshot diskstats missing")
            matches = []
            for line in diskstats.splitlines():
                fields = line.split()
                if len(fields) > max(counters.values()) and fields[0].isdigit() and fields[1].isdigit():
                    if (int(fields[0]), int(fields[1])) == (device["major"], device["minor"]):
                        if any(not fields[index].isdigit() for index in counters.values()):
                            raise ValueError("invalid diskstats counter")
                        matches.append({counter: int(fields[index]) for counter, index in counters.items()})
            expected_counters = {counter: device[f"{counter}_{boundary}"] for counter in counters}
            if len(matches) != 1 or matches[0] != expected_counters:
                raise ValueError("measurement snapshot device identity or counter mismatch")


def qualify_target_buffered_io(job, targets, expected_readers):
    """Require readback on each bound kernel export; SPDK is separate."""
    if job.get("nvmeof_target") != "kernel" or job.get("target_io") not in ("direct", "buffered"):
        return None
    expected = 0 if job["target_io"] == "direct" else 1
    exports = []
    if isinstance(targets, list):
        for owner in targets:
            target = owner.get("target") if isinstance(owner, dict) else None
            rows = target.get("exports") if isinstance(target, dict) else None
            if not isinstance(rows, list) or not rows:
                return {"qualified": False, "reasons": ["target_buffered_io_unverified"]}
            exports.extend(rows)
    if type(expected_readers) is not int or expected_readers <= 0 or len(exports) != expected_readers:
        return {"qualified": False, "reasons": ["target_buffered_io_unverified"]}
    for export in exports:
        policy = export.get("target_receipt") if isinstance(export, dict) else None
        if (not isinstance(policy, dict) or type(policy.get("buffered_io")) is not int
                or policy["buffered_io"] != expected or policy.get("target_cache_verified") is not True):
            return {"qualified": False, "reasons": ["target_buffered_io_unverified"]}
    return {"qualified": True, "reasons": []}


def qualify_media_target(media, target_io):
    """Keep the artifact verdict and add target readback qualification."""
    reasons = list(media["reason_codes"])
    if target_io is not None and not target_io["qualified"]:
        reasons.extend(reason for reason in target_io["reasons"] if reason not in reasons)
    return {"status": "qualified" if media["status"] == "qualified" and not reasons else "unqualified",
            "reason_codes": reasons}


def _read_bound_floor_file(root, binding, limit):
    _focused_object(binding, "path sha256")
    path, checksum = binding["path"], binding["sha256"]
    if (not isinstance(path, str) or not path or Path(path).is_absolute()
            or any(part in ("", ".", "..") for part in path.split("/"))
            or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
        raise ValueError("invalid floor file binding")
    candidate = (root / path).resolve()
    if root not in candidate.parents:
        raise ValueError("floor file escapes case directory")
    try:
        with candidate.open("rb") as stream:
            raw = stream.read(limit + 1)
    except OSError as exc:
        raise ValueError("floor file unavailable") from exc
    if len(raw) > limit:
        raise ValueError("floor file exceeds bound")
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError("floor file hash mismatch")
    return raw


def _verify_ssd_floor(root, data, receipt):
    log = data["lat_log"]
    binding = receipt.get("ssd_floor_lat_log")
    if binding != {"path": log["path"], "sha256": log["sha256"]}:
        raise ValueError("SSD log receipt differs from result")
    raw = _read_bound_floor_file(root, binding, floors.MAX_LOG_BYTES)
    rows = floors.parse_fio_log(raw, block_bytes=data["block_bytes"], region_bytes=data["region_bytes"])
    if len(rows) != log["rows"] or len(rows) != data["read_ios"]:
        raise ValueError("SSD latency log row count mismatch")
    raw_fio = _read_bound_floor_file(root, {"path": data["raw_fio_path"], "sha256": data["fio_sha256"]}, 8 * 1024 * 1024)
    _verify_fio_raw(raw_fio, data)


def _verify_ssd_write_floor(root, data, receipt, job):
    log = data["lat_log"]
    binding = receipt.get("ssd_write_floor_lat_log")
    if binding != {"path": log["path"], "sha256": log["sha256"]}:
        raise ValueError("SSD write log receipt differs from result")
    raw_log = _read_bound_floor_file(root, binding, floors.MAX_LOG_BYTES)
    raw = _read_bound_floor_file(root, data["raw"], 1024 * 1024)
    try:
        expected = floors.write_result(job, raw, raw_log, data["device_identity"], data["actual_direct"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid SSD write evidence") from exc
    expected["raw"] = data["raw"]
    expected["lat_log"] = {**binding, "rows": expected["logged_operations"],
                           "columns": ["write_ns", "sync_ns", "total_ns", "offset"]}
    if expected != data:
        raise ValueError("SSD write summary differs from raw evidence")


def _verify_ssd_write_command(root, job, receipt):
    binding = receipt.get("ssd_write_command")
    if binding is None:
        raise ValueError("completed SSD write floor lacks command binding")
    try:
        command = json.loads(_read_bound_floor_file(root, binding, 1024 * 1024))
        _focused_object(command, "argv build scratch_identity")
        _focused_object(command["build"], "source_sha256 binary_sha256")
        _focused_object(command["scratch_identity"], "device inode")
        identity = command["scratch_identity"]
        for value in identity.values():
            _focused_uint(value)
        for value in command["build"].values():
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError("invalid write helper build hash")
        source = Path(__file__).with_name("floors_ssd_write.c").read_bytes()
        if command["build"]["source_sha256"] != hashlib.sha256(source).hexdigest():
            raise ValueError("write helper source differs from admitted source")
        argv = command["argv"]
        if not isinstance(argv, list) or len(argv) != 11 or any(not isinstance(x, str) for x in argv):
            raise ValueError("invalid write helper argv")
        scratch = Path(argv[1])
        if not scratch.is_absolute() or ".." in scratch.parts or scratch.name != "fio-scratch":
            raise ValueError("invalid write scratch root")
        target = scratch / (job["case_id"] + ".fio-scratch")
        case_root = scratch.parent / "floors" / job["case_id"]
        expected = floors.write_argv(case_root / "ssd_write", target, case_root / "lat.log",
                                     job["ssd_write"], identity)
        if argv != expected:
            raise ValueError("write helper argv differs from admitted job")
    except (OSError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid SSD write command evidence") from exc


def admit_focused_artifacts(directory, job, receipt, *, completed):
    """Validate case-local, hash-bound focused observations without pooling."""
    if type(completed) is not bool or not isinstance(job, dict) or not isinstance(receipt, dict):
        raise ValueError("invalid focused admission arguments")
    required = {"phase_timing"}
    kind = job.get("case_kind")
    if kind in _FOCUSED_ARTIFACTS and (kind != "write_wait_diagnostic" or job.get("write_wait_diagnostics", {}).get("enabled") is True):
        required.add(_FOCUSED_ARTIFACTS[kind])
    if kind == "live_read" and isinstance(job.get("media"), dict):
        required.add("media_evidence")
    if job.get("device_counters"):
        required.add("device_counters")
    if kind == "write_wait_diagnostic" and job.get("write_wait_diagnostics", {}).get("enabled") is False and receipt.get("write_wait_metrics") is not None:
        raise ValueError("disabled write wait diagnostics cannot export counters")
    names = set(_FOCUSED_ARTIFACTS.values()) | {"phase_timing", "device_counters"}
    result = {}
    root = Path(directory).resolve()
    if kind == "ssd_write_floor" and (completed or receipt.get("ssd_write_command") is not None):
        _verify_ssd_write_command(root, job, receipt)
    for name in sorted(names):
        binding = receipt.get(name)
        if binding is None:
            if completed and name in required:
                raise ValueError("completed case lacks requested " + name)
            continue
        _focused_object(binding, "path sha256")
        path = binding["path"]
        checksum = binding["sha256"]
        if (not isinstance(path, str) or not path or Path(path).is_absolute()
                or any(part in ("..", ".") for part in path.split("/"))
                or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
            raise ValueError("invalid focused artifact binding")
        candidate = root / path
        if candidate.resolve().parent != root and root not in candidate.resolve().parents:
            raise ValueError("focused artifact escapes case directory")
        try:
            raw = candidate.read_bytes()
            if hashlib.sha256(raw).hexdigest() != checksum:
                raise ValueError("focused artifact hash mismatch")
            data = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("unreadable focused artifact") from exc
        _validate_focused_artifact(name, data, job, receipt)
        if name == "ssd_floor_result":
            _verify_ssd_floor(root, data, receipt)
        if name == "ssd_write_floor_result":
            _verify_ssd_write_floor(root, data, receipt, job)
        if name == "net_floor_result" and completed and any(run["status"] != "completed" for run in data["runs"]):
            raise ValueError("completed case contains failed network run")
        if name == "media_evidence" and data["status"] == "qualified":
            _verify_media_snapshots(root, receipt, data)
        if name in ("fio_result", "ssd_floor_result", "interference_result", "write_wait_metrics"):
            data = {**data, "qualification": _focused_qualification(name, data, root, receipt)}
        result[name] = data
    return result


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def quantile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def read_samples(path, *, measurement_only=True):
    with Path(path).open(newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"phase", "operation", "outcome", "scheduled_ns", "dispatched_ns", "dispatch_observed", "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes", "error_kind"}
        if not required <= set(reader.fieldnames or []):
            raise ValueError("raw samples lack the required timing/accounting schema")
        for row in reader:
            if measurement_only and row["phase"] != "measurement":
                continue
            if None in row or row["dispatch_observed"] not in ("true", "false"):
                raise ValueError("invalid raw timing row or dispatch identity")
            sample_id = row.get("caller_sample_id")
            if sample_id not in (None, ""):
                if not sample_id.isascii() or not sample_id.isdigit() or int(sample_id) > 2**64 - 1:
                    raise ValueError("invalid caller sample ID")
                row["caller_sample_id"] = int(sample_id)
            else:
                row["caller_sample_id"] = None
            for key in ("scheduled_ns", "dispatched_ns", "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes"):
                text = row[key]
                if text is None or not text.isascii() or not text.isdigit():
                    raise ValueError("missing or invalid timing/count in raw sample")
                row[key] = int(text)
            if row["logical_keys"] == 0 or row["completed_ns"] < row["scheduled_ns"]:
                raise ValueError("invalid logical-key count or terminal clock ordering")
            if row["response_ns"] != row["completed_ns"] - row["scheduled_ns"]:
                raise ValueError("response latency disagrees with terminal clocks")
            dispatched = row["dispatch_observed"] == "true"
            if dispatched:
                if not row["scheduled_ns"] <= row["dispatched_ns"] <= row["completed_ns"]:
                    raise ValueError("invalid dispatch clock ordering")
                if (row["api_ns"] != row["completed_ns"] - row["dispatched_ns"]
                        or row["scheduling_ns"] != row["dispatched_ns"] - row["scheduled_ns"]):
                    raise ValueError("API/scheduling latency disagrees with monotonic clocks")
                if row["outcome"] == "rejected":
                    raise ValueError("rejected arrival cannot have an observed API dispatch")
            else:
                if row["dispatched_ns"] or row["api_ns"] or row["scheduling_ns"]:
                    raise ValueError("undispatched call has invented API clocks")
                row["api_ns"] = row["scheduling_ns"] = None
            yield row


def reconcile_interference_samples(samples, evidence):
    """Reconcile each stream before reducing operation and outcome populations."""
    by_stream = {"read": [], "put": []}
    for row in samples:
        stream = row.get("stream")
        if stream not in by_stream or row.get("operation") != stream:
            raise ValueError("interference sample stream differs from operation")
        if row.get("outcome") not in ("success", "rejected", "timeout", "unknown_mutation", "error", "wrong_value"):
            raise ValueError("invalid interference sample outcome")
        if row["outcome"] == "rejected" and row["dispatch_observed"] != "false":
            raise ValueError("interference rejection differs from dispatch observation")
        by_stream[stream].append(row)
    populations = []
    for stream, rows in by_stream.items():
        counters = evidence[stream]
        rejected = sum(row["outcome"] == "rejected" for row in rows)
        successful = sum(row["outcome"] == "success" for row in rows)
        completed = len(rows) - rejected
        if (counters["rejected"] != rejected or counters["successful"] != successful
                or counters["completed"] != completed
                or counters["offered"] != len(rows) + counters["failed"]
                or counters["admitted"] != completed + counters["failed"]):
            raise ValueError("interference stream counters differ from raw samples")
        dispatched = [row for row in rows if row["dispatch_observed"] == "true"]
        first = min((row["dispatched_ns"] for row in dispatched), default=None)
        last = max((row["completed_ns"] for row in dispatched), default=None)
        if counters["first_dispatch_ns"] != first or counters["last_completion_ns"] != last:
            raise ValueError("interference stream window differs from raw samples")
        if rows:
            for population in aggregate(rows)["populations"]:
                populations.append({**population, "stream": stream})
    return populations


def qualify_hybrid_put(job, backend, evidence, targets, readers, samples, drained, config, spdk_io=None):
    """Qualify an owner write with protected readers present, without claiming read I/O."""
    reasons = []
    preload = isinstance(config, dict) and config.get("phases", {}).get("preload") is True
    namespace_origin = ("runner_fresh" if isinstance(config, dict) and config.get("key_namespace_hex") is None
                        else "explicit")
    if not preload or namespace_origin != "runner_fresh":
        reasons.append("preload_or_namespace_unverified")
    configuration = backend.get("configuration") if isinstance(backend, dict) else None
    write_backend = configuration.get("write_backend") if isinstance(configuration, dict) else None
    if write_backend != "owner_record_protocol":
        reasons.append("configured_write_route_mismatch")
    successful = sum(row.get("operation") == "put" and row.get("outcome") == "success"
                     for row in samples if isinstance(row, dict)) if isinstance(samples, list) else 0
    if (not isinstance(samples, list) or successful != job.get("measurement_operations")
            or any(not isinstance(row, dict) or row.get("operation") != "put" or row.get("outcome") != "success"
                   for row in samples)):
        reasons.append("put_samples_incomplete")
    if drained is not True:
        reasons.append("measurement_not_drained")
    exports = []
    if isinstance(targets, list):
        for owner in targets:
            target = owner.get("target") if isinstance(owner, dict) else None
            entries = target.get("exports") if isinstance(target, dict) else None
            if not isinstance(entries, list) or not entries:
                reasons.append("target_export_receipts_unavailable")
            else:
                exports.extend(entries)
    if not isinstance(readers, list) or not readers:
        readers = []
        reasons.append("configured_readers_unavailable")
    if not exports or len(exports) != len(readers):
        reasons.append("target_reader_count_mismatch")
    expected_target = "spdk" if job.get("nvmeof_target") == "spdk" else "kernel_direct"
    if expected_target == "spdk":
        if (not isinstance(spdk_io, dict) or spdk_io.get("available") is not True
                or any(type(spdk_io.get(field)) is not int or spdk_io[field] < 0
                       for field in ("read_ops", "read_bytes"))):
            reasons.append("target_read_counters_unavailable")
        elif spdk_io["read_ops"] != 0 or spdk_io["read_bytes"] != 0:
            reasons.append("target_reads_observed")
    expected_cache = ("direct_file_bdev_no_OS_pagecache" if expected_target == "spdk"
                      else "direct_file_no_OS_pagecache")
    for entry in exports:
        policy = entry.get("target_receipt") if isinstance(entry, dict) else None
        if (not isinstance(policy, dict) or policy.get("target_implementation") != expected_target
                or policy.get("target_cache") != expected_cache or policy.get("target_cache_verified") is not True
                or policy.get("selected_transport") != "rdma"
                or (expected_target == "spdk" and (policy.get("direct_io_verified") is not True
                                                    or policy.get("read_only_verified") is not True))):
            reasons.append("target_policy_unverified")
    nvme = evidence.get("nvmeof") if isinstance(evidence, dict) else None
    observed = nvme.get("readers") if isinstance(nvme, dict) else None
    if (not isinstance(nvme, dict) or nvme.get("available") is not True
            or not isinstance(observed, dict) or len(observed) != len(readers) or not observed):
        reasons.append("measurement_readers_unavailable")
        observed = {}
    expected_ids, observed_ids, export_ids = set(), set(), set()
    try:
        for reader in readers:
            if reader["binding"]["transport"] != "rdma":
                raise ValueError("reader transport")
            manifest = reader["manifest"]
            expected_ids.add(json.dumps([manifest[k] for k in ("database_uuid", "epoch", "partition", "export_uuid")]))
        for row in observed.values():
            if row["available"] is not True or row["identity"]["transport"] != "rdma":
                raise ValueError("reader boundary")
            counter = row.get("counters")
            values = counter.get("counters") if isinstance(counter, dict) else None
            if not isinstance(counter, dict) or counter.get("available") is not True or not isinstance(values, dict):
                reasons.append("reader_measurement_counters_unavailable")
            else:
                for field in ("logical_requests", "io_submissions"):
                    value = values.get(field)
                    if type(value) is not int or value < 0:
                        reasons.append("reader_" + field + "_unavailable")
                    elif value != 0:
                        reasons.append("reader_" + field + "_nonzero")
            identity = row["identity"]
            observed_ids.add(json.dumps([identity[k] for k in ("database_uuid", "epoch", "partition", "export_uuid")]))
        export_ids = {json.dumps(entry["export_uuid"]) for entry in exports}
        if (len(expected_ids) != len(readers) or expected_ids != observed_ids
                or len(export_ids) != len(exports)
                or export_ids != {json.dumps(reader["manifest"]["export_uuid"]) for reader in readers}):
            raise ValueError("reader identity")
    except (KeyError, TypeError, ValueError):
        reasons.append("reader_target_identity_mismatch")
    return {"qualified": not reasons, "reasons": list(dict.fromkeys(reasons)),
            "write_backend": write_backend, "target_count": len(exports),
            "reader_count": len(readers), "successful_puts": successful,
            "preload": preload, "namespace_origin": namespace_origin}


def qualify_sealed_read_accounting(qualification, successful_reads):
    """Report the gap; singleflight followers are not measured at the boundary."""
    qualification = qualification if isinstance(qualification, dict) else {}
    totals = qualification.get("nvmeof_totals")
    totals = totals if isinstance(totals, dict) else {}
    completions = totals.get("io_completions")
    rpc = qualification.get("coordinator_read_rpc_count")
    valid_counts = type(completions) is int and completions >= 0 and type(successful_reads) is int and successful_reads >= 0
    gap = successful_reads - completions if valid_counts else None
    consistent = (qualification.get("qualified") is True and valid_counts
                  and 0 < completions <= successful_reads and type(rpc) is int and rpc == 0
                  and type(qualification.get("coordinator_cache_hits")) is int
                  and qualification["coordinator_cache_hits"] == 0
                  and type(totals.get("eligible_hits")) is int and totals["eligible_hits"] > 0
                  and type(totals.get("io_submissions")) is int and totals["io_submissions"] == completions
                  and all(type(totals.get(k)) is int and totals[k] == 0
                          for k in ("ineligible_fallbacks", "io_errors", "io_timeouts")))
    return {"qualified": consistent, "reasons": [] if consistent else ["sealed_read_counter_inconsistent"],
            "nvme_completions": completions, "successful_reads": successful_reads,
            "accounting_gap": gap, "read_rpc_count": rpc, "notes": ["coalescing_unmeasured"]}


def qualify_interference_route(job, backend, manifest, evidence, nvme_qualification, streams):
    """Qualify configured owner routes and independently observed hybrid reads."""
    reasons = []
    configuration = backend.get("configuration") if isinstance(backend, dict) else None
    configuration = configuration if isinstance(configuration, dict) else {}
    hybrid = job["backend"] == "tidehunter_nvme_rdma"
    expected_read = ("linux_nvmeof_sealed_read_with_owner_record_fallback"
                     if hybrid else "owner_record_protocol")
    if configuration.get("read_backend") != expected_read or configuration.get("write_backend") != "owner_record_protocol":
        reasons.append("configured_route_mismatch")
    if (job.get("durability_tier") != "synchronous"
            or (manifest.get("durability") or {}).get("confirmed_by_backend_receipt") != "synchronous"
            or not isinstance(backend.get("acknowledgement"), str)
            or "covering fsync complete" not in backend["acknowledgement"]):
        reasons.append("synchronous_acknowledgement_unproven")
    if streams["put"]["successful"] <= 0:
        reasons.append("no_successful_puts")
    if hybrid:
        accounting = qualify_sealed_read_accounting(nvme_qualification, streams["read"]["successful"])
        reasons.extend(accounting["reasons"])
    else:
        # With no installed reader, this backend can only use owner RPC.
        readers = configuration.get("nvmeof_readers")
        if (job.get("backend") not in ("tidehunter_tcp", "tidehunter_verbs_native")
                or type(readers) is not list or readers
                or configuration.get("read_backend") != "owner_record_protocol"
                or configuration.get("write_backend") != "owner_record_protocol"):
            reasons.append("owner_nvme_zero_unproven")
    result = {"qualified": not reasons, "reasons": reasons, "notes": ["write_route_configured_only"]}
    if hybrid:
        result.update({key: accounting[key] for key in
                       ("nvme_completions", "successful_reads", "accounting_gap", "read_rpc_count")})
        result["notes"].extend(accounting["notes"])
    return result


def summarize_write_timeline(samples, evidence):
    """Join bounded coordinator events to raw calls without crossing clock domains."""
    export = evidence.get("timeline")
    if not isinstance(export, dict):
        return {"available": False, "error": evidence.get("error")}
    rows = {}
    for row in samples:
        sample_id = row.get("caller_sample_id")
        if sample_id is not None:
            if sample_id in rows:
                raise ValueError("duplicate caller sample ID in raw samples")
            rows[sample_id] = row
    pairs = {
        "shared_gate_ns": ("caller_start", "gate_acquired"),
        "gate_to_validation_start_ns": ("gate_acquired", "validation_start"),
        "owner_admission_ns": ("validation_start", "admission_acquired"),
        "credit_to_owner_enqueue_start_ns": ("admission_acquired", "owner_enqueue_started"),
        "owner_enqueue_to_dequeue_ns": ("owner_enqueue_started", "owner_dequeued"),
        "preparation_ns": ("owner_dequeued", "prepared"),
        "prepared_to_admitted_ns": ("prepared", "admitted"),
        "admitted_to_append_enqueue_start_ns": ("admitted", "append_enqueue_started"),
        "append_enqueue_to_dequeue_ns": ("append_enqueue_started", "worker_dequeued"),
        "worker_dequeue_to_tx_queued_ns": ("worker_dequeued", "tx_queued"),
        "queued_to_harvest_including_remote_ns": ("tx_queued", "completion_harvested"),
        "harvest_to_durable_observation_ns": ("completion_harvested", "durable_observed"),
        "publication_ns": ("durable_observed", "published"),
        "publication_to_reply_queued_ns": ("published", "caller_reply_queued"),
        "reply_queued_to_caller_finished_ns": ("caller_reply_queued", "caller_finished"),
        "coordinator_call_ns": ("caller_start", "caller_finished"),
        "live_group_build_ns": ("live_group_build_started", "live_group_built"),
        "live_reserve_ns": ("live_reserve_started", "live_reserve_completed"),
        "live_namespace_setup_ns": ("live_namespace_setup_started", "live_namespace_setup_completed"),
        "live_io_ns": ("live_io_started", "live_io_completed"),
        "live_complete_ns": ("live_complete_started", "live_complete_completed"),
    }
    durations = {name: [] for name in pairs}
    unavailable = {name: {} for name in pairs}
    joined, seen, linked_envelopes = [], set(), set()
    for record in export.get("records", []):
        sample_id = record["caller_sample_id"]
        if sample_id in seen:
            raise ValueError("duplicate caller sample ID in timeline")
        seen.add(sample_id)
        row = rows.get(sample_id)
        events = record.get("events", [])
        if row is not None:
            linked_envelopes.update((event["owner"], event["envelope_id"]) for event in events
                                    if event.get("stage") in ("tx_queued", "completion_harvested")
                                    and event.get("owner") is not None and event.get("envelope_id") is not None)
        stages, stage_unavailable = {}, {}
        for name, (start, end) in pairs.items():
            starts = [event for event in events if event.get("stage") == start]
            ends = [event for event in events if event.get("stage") == end]
            reason = None
            if not starts or not ends:
                reason = "missing_event"
            elif len(starts) != 1 or len(ends) != 1:
                reason = "multiple_events"
            else:
                a, b = starts[0], ends[0]
                first, last = a.get("elapsed_ns"), b.get("elapsed_ns")
                if type(first) is not int or type(last) is not int or first < 0 or last < 0:
                    reason = "invalid_timestamp"
                elif (a.get("owner") is not None and b.get("owner") is not None
                      and a["owner"] != b["owner"]) or (
                      a.get("envelope_id") is not None and b.get("envelope_id") is not None
                      and a["envelope_id"] != b["envelope_id"]):
                    reason = "identity_mismatch"
                elif last < first:
                    reason = "reversed_events"
            value = None if reason else last - first
            stages[name] = value
            if reason:
                stage_unavailable[name] = reason
                if row is not None:
                    counts = unavailable[name]
                    counts[reason] = counts.get(reason, 0) + 1
            elif row is not None:
                durations[name].append(value)
        joined.append({"caller_sample_id": sample_id, "sample": row, "record": record,
                       "stages_ns": stages, "stage_unavailable": stage_unavailable})
    summaries = {name: {"count": len(values), "p50": quantile(values, .50),
                        "p99": quantile(values, .99), "max": max(values) if values else None,
                        "unavailable": unavailable[name]}
                 for name, values in durations.items()}
    owner_records = {}
    for record in export.get("owner_records", []):
        key = (record["owner"], record["envelope_id"])
        if key in owner_records:
            raise ValueError("duplicate owner/envelope record")
        owner_records[key] = record
    owner_summaries = {}
    owner_all_summaries = {}
    for field in ("control_queue_ns", "session_admission_wait_ns", "appender_queue_ns", "flusher_queue_ns",
                  "append_barrier_wait_ns", "append_wal_ns", "append_persist_ns",
                  "wal_file_persist_ns", "namespace_persist_ns", "response_ready_ns", "handler_ns"):
        for target, records in ((owner_summaries, (owner_records[key] for key in linked_envelopes if key in owner_records)),
                                (owner_all_summaries, owner_records.values())):
            values = [record[field] for record in records if record.get(field) is not None]
            target[field] = {"count": len(values), "p50": quantile(values, .50),
                             "p99": quantile(values, .99), "max": max(values) if values else None}
    live_groups = export.get("live_groups")
    live_linked = []
    if isinstance(live_groups, list):
        batch_records = {(record["batch"]["epoch"], record["batch"]["sequence"]): record
                         for record in export.get("records", []) if record.get("batch") is not None}
        for index, group in enumerate(live_groups):
            for batch in group["batches"]:
                version = (batch["epoch"], batch["sequence"])
                key = (group["owner"], *version)
                record = batch_records.get(version)
                if (record is not None and record["caller_sample_id"] in rows
                        and any(event.get("stage", "").startswith("live_")
                                and event.get("owner") == group["owner"]
                                for event in record.get("events", []))):
                    live_linked.append((key, index))
        linked_indexes = {index for _, index in live_linked}
        live_summary = {
            "scope": "unique_linked_live_groups",
            "groups": len(live_groups), "linked_groups": len(linked_indexes),
            "linked_memberships": len(live_linked),
            "dropped_groups": export.get("dropped_live_groups"),
            "owner_validation_ns": [], "owner_persistence": [], "observations": [],
            "interpretation": "Each captured group is counted once, including retry groups. Owner durations use a separate clock; absent completion profiles and phases are not zero.",
        }
        for index in sorted(linked_indexes):
            group = live_groups[index]
            live_summary["observations"].append({"group_index": index, **group})
            if group["owner_validation_ns"] is not None:
                live_summary["owner_validation_ns"].append(group["owner_validation_ns"])
            if group["owner_persistence"] is not None:
                live_summary["owner_persistence"].append(group["owner_persistence"])
        live_summary["owner_validation_unavailable"] = len(linked_indexes) - len(live_summary["owner_validation_ns"])
        live_summary["owner_persistence_unavailable"] = len(linked_indexes) - len(live_summary["owner_persistence"])
    else:
        live_summary = None
    slow = sorted((item for item in joined if item["sample"] is not None and item["sample"]["api_ns"] is not None),
                  key=lambda item: item["sample"]["api_ns"], reverse=True)[:20]
    for item in slow:
        events = item["record"].get("events", [])
        envelope_keys = {(event["owner"], event["envelope_id"]) for event in events
                         if event.get("stage") in ("tx_queued", "completion_harvested")
                         and event.get("owner") is not None and event.get("envelope_id") is not None}
        item["owner_records"] = [owner_records[key] for key in sorted(envelope_keys) if key in owner_records]
        item["owner_records_unavailable"] = sum(key not in owner_records for key in envelope_keys)
        starts = [event["elapsed_ns"] for event in events if event.get("stage") == "caller_start"]
        ends = [event["elapsed_ns"] for event in events if event.get("stage") == "caller_finished"]
        item["checkpoint_events_during_call"] = (
            [event for event in export.get("checkpoint_events", [])
             if min(starts) <= event["elapsed_ns"] <= max(ends)] if starts and ends else None)
    return {"available": evidence.get("available", False), "joined_calls": sum(item["sample"] is not None for item in joined),
            "status": evidence.get("status"), "error": evidence.get("error"),
            "raw_ids_without_record": len(set(rows) - seen), "records_without_raw": len(seen - set(rows)),
            "dropped_calls": export.get("dropped_calls"), "dropped_events": export.get("dropped_events"),
            "dropped_checkpoint_events": export.get("dropped_checkpoint_events"),
            "owner_storage_durations_available": export.get("owner_storage_durations_available", False),
            "owner_dropped_records": export.get("owner_dropped_records"),
            "owner_incomplete_records": export.get("owner_incomplete_records"),
            "owner_expected_connections": export.get("owner_expected_connections"),
            "owner_captured_connections": export.get("owner_captured_connections"),
            "captured_group_deltas": {"available": False,
                "reason": "No independent isolation proof excludes untraced persistence captures on other connections; raw capture frontiers and cumulative counts are retained in the sidecar."},
            "owner_envelope_stage_summaries_ns": owner_summaries,
            "all_owner_envelope_stage_summaries_ns": owner_all_summaries,
            "linked_owner_envelopes": len(linked_envelopes),
            "live_group_observations": live_summary,
            "linked_owner_envelopes_without_record": len(linked_envelopes - owner_records.keys()),
            "owner_errors": export.get("owner_errors", []),
            "incomplete_calls": export.get("incomplete_calls"), "stage_summaries_ns": summaries,
            "stage_descriptions": {
                "gate_to_validation_start_ns": "shared write gate acquired through remote validation entry",
                "credit_to_owner_enqueue_start_ns": "admission credit acquired through input detachment, reply creation and owner enqueue start",
                "prepared_to_admitted_ns": "prepared batch through authority admission, including pressure and reservation work",
                "admitted_to_append_enqueue_start_ns": "admission through staged dispatch and append enqueue start; can include owner pipeline capacity wait",
                "worker_dequeue_to_tx_queued_ns": "append worker dequeue through local reactor queueing, not NIC transmission",
                "reply_queued_to_caller_finished_ns": "owner reply enqueue through caller wake, health check and traced completion",
            },
            "slowest_calls": slow,
            "interpretation": "Coordinator-local intervals are nonadditive when populations overlap; quantiles cannot be summed. Multiple, missing, reversed or mismatched events are unavailable. TX queued to completion harvested includes remote work and local processing, not isolated wire time. Server clocks are separate. Checkpoint adopted marks owner observation before live installation; live_install_end marks completion. Checkpoint generations link stages when present; older artifacts lack them. Live-install elapsed time can include interleaved serving work."}


def aggregate(samples):
    groups = {}
    offered = completed = failures = 0
    for row in samples:
        offered += 1
        outcome = row["outcome"]
        if outcome in ("ok", "success"):
            completed += 1
        else:
            failures += 1
        key = (row["operation"], outcome)
        bucket = groups.setdefault(key, {"count": 0, "logical_keys": 0, "api_ns": [], "response_ns": [], "scheduling_ns": []})
        bucket["count"] += 1
        bucket["logical_keys"] += row["logical_keys"]
        for field in ("api_ns", "response_ns", "scheduling_ns"):
            if row[field] is not None:
                bucket[field].append(row[field])
    if offered == 0:
        raise ValueError("zero measurement samples are not a successful performance run")
    result = []
    for (operation, outcome), bucket in sorted(groups.items()):
        item = {"operation": operation, "outcome": outcome, "count": bucket["count"], "logical_keys": bucket["logical_keys"]}
        for field in ("api_ns", "response_ns", "scheduling_ns"):
            values = bucket[field]
            item[field] = {"sample_count": len(values), "p50": quantile(values, .50), "p95": quantile(values, .95),
                           "p99": quantile(values, .99), "p999": quantile(values, .999), "max": max(values) if values else None}
        result.append(item)
    return {"offered_calls": offered, "success_calls": completed, "unsuccessful_calls": failures,
            "success_fraction": completed / offered, "populations": result,
            "tail_caveat": "Quantiles are conditional on operation/outcome and timed samples. Rejected/unknown/timeout calls remain in denominator; no overall successful-tail or HA claim."}


def measurement_timeline(samples):
    """Describe finite measurement exposure without removing stalls or rejections."""
    start = min(row["scheduled_ns"] for row in samples)
    end = max(row["completed_ns"] for row in samples)
    span = end - start
    if span <= 0:
        raise ValueError("measurement exposure must be positive")
    count = min(32, span)
    windows = [{"start_offset_ns": (index * span + count - 1) // count,
                "end_offset_ns": ((index + 1) * span + count - 1) // count,
                "completed_calls": 0, "successful_calls": 0}
               for index in range(count)]
    for row in samples:
        index = min(count - 1, (row["completed_ns"] - start) * count // span)
        windows[index]["completed_calls"] += 1
        windows[index]["successful_calls"] += row["outcome"] in ("ok", "success")
    completions = sorted(row["completed_ns"] for row in samples)
    boundaries = [start, *completions, end]
    gap_start, gap_end = max(zip(boundaries, boundaries[1:]), key=lambda pair: pair[1] - pair[0])
    dispatched = [row for row in samples if row["api_ns"] is not None]
    slowest = sorted(dispatched, key=lambda row: row["api_ns"], reverse=True)[:16]
    api_sum = sum(row["api_ns"] for row in dispatched) if dispatched else None
    return {
        "scope": "Recorded measurement clocks, all outcomes; includes initial wait. "
                 "Not steady-state capacity, CPU time, device time or network RTT.",
        "duration_ns": span,
        "exposure_below_one_second": span < 1_000_000_000,
        "dispatched_calls": len(dispatched),
        "api_time_sum_ns": api_sum,
        "mean_observed_api_concurrency": api_sum / span if api_sum is not None else None,
        "largest_completion_gap_ns": gap_end - gap_start,
        "largest_completion_gap_start_offset_ns": gap_start - start,
        "largest_completion_gap_end_offset_ns": gap_end - start,
        "api_calls_above_ns": {str(limit): sum(row["api_ns"] > limit for row in dispatched)
                              for limit in (1_000_000, 10_000_000, 100_000_000)} if dispatched else None,
        "slowest_calls": [{"operation": row["operation"], "outcome": row["outcome"],
                           "dispatch_offset_ns": row["dispatched_ns"] - start,
                           "completion_offset_ns": row["completed_ns"] - start,
                           "api_ns": row["api_ns"]} for row in slowest],
        "completion_windows": windows,
    }

def verify_normalization(raw, admitted):
    """Defaults may be added; explicitly supplied values may not be changed."""
    if isinstance(raw, dict):
        if not isinstance(admitted, dict) or any(key not in admitted for key in raw):
            raise ValueError("normalized configuration dropped supplied fields")
        for key, value in raw.items():
            verify_normalization(value, admitted[key])
    elif isinstance(raw, list):
        if not isinstance(admitted, list) or len(raw) != len(admitted):
            raise ValueError("normalized configuration changed a supplied list")
        for value, normalized in zip(raw, admitted):
            verify_normalization(value, normalized)
    elif (isinstance(raw, bool) != isinstance(admitted, bool)) or raw != admitted:
        raise ValueError("normalized configuration changed an explicitly supplied value")


_STORAGE_WRITE_PROFILE_FIELDS = frozenset("""
    append_calls append_errors append_retries appended_groups appended_receipts
    append_total_ns append_state_lock_wait_ns append_validation_ns append_basic_validation_ns
    append_conflict_lookup_ns append_geometry_capacity_ns append_metadata_reservation_ns
    append_descriptor_preparation_ns append_history_ns append_wal_ns append_publication_ns
    persist_calls persist_errors persist_noops persisted_receipts persist_total_ns
    persist_serialization_lock_wait_ns persist_state_lock_wait_ns persist_validation_ns
    persist_wal_file_ns persist_namespace_ns persist_inventory_file_ns persist_inventory_namespace_ns
    inventory_file_persist_calls inventory_file_persist_successes inventory_namespace_persist_calls
    inventory_namespace_persist_successes live_frontier_file_ns live_frontier_namespace_ns
    live_frontier_file_calls live_frontier_file_successes live_frontier_namespace_calls
    live_frontier_namespace_successes persist_publication_ns file_persist_calls file_persist_successes
    namespace_persist_calls namespace_persist_successes durable_publications
""".split())


def _validate_storage_write_profile(profile):
    if (not isinstance(profile, dict)
            or set(profile) != _STORAGE_WRITE_PROFILE_FIELDS | {"persistence_capture"}
            or any(type(profile[field]) is not int or not 0 <= profile[field] < 2**64
                   for field in _STORAGE_WRITE_PROFILE_FIELDS)):
        raise ValueError("invalid live group storage persistence profile")
    capture = profile["persistence_capture"]
    fields = {"durable_end", "captured_appended_end", "cumulative_appended_groups"}
    if capture is not None and (
            not isinstance(capture, dict) or set(capture) != fields
            or any(type(capture[field]) is not int or not 0 <= capture[field] < 2**64 for field in fields)):
        raise ValueError("invalid live group persistence capture")


def admit_write_timeline(run, config, *, completed, expected_backend=None):
    """Validate and compactly bind a bounded measurement timeline, including partial exports."""
    run = Path(run)
    path = run / "result/write-timeline.json"
    diagnostics = config.get("diagnostics", {})
    limit = diagnostics.get("write_timeline_limit", 0)
    if type(limit) is not int or not 0 <= limit <= 65_536:
        raise ValueError("invalid write timeline limit")
    if not path.is_file():
        if completed and limit:
            raise ValueError("requested write timeline artifact is missing")
        return None
    if not limit or config.get("backend", {}).get("kind") != "tidehunter":
        raise ValueError("unexpected write timeline for configuration/backend")
    if path.stat().st_size > 1024 * 1024 * 1024:
        raise ValueError("write timeline exceeds artifact size bound")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    checksum = digest.hexdigest()
    manifest_path = run / "result/manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("run", {}).get("config") != config:
            raise ValueError("write timeline belongs to a different executed configuration")
        if expected_backend is not None and manifest.get("backend_info", {}).get("name") != expected_backend:
            raise ValueError("write timeline belongs to a different backend case")
    elif completed:
        raise ValueError("write timeline lacks executed manifest")
    data = json.loads(path.read_text())
    if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
            or data["schema_version"] != 1 or type(data.get("requested_limit")) is not int
            or data["requested_limit"] != limit
            or type(data.get("measurement_attempted")) is not bool
            or type(data.get("measurement_drained")) is not bool
            or type(data.get("available")) is not bool
            or data.get("status") not in ("complete", "failed")
            or not isinstance(data.get("error"), (str, type(None)))):
        raise ValueError("invalid write timeline schema, scope or requested limit")
    trace = data.get("timeline")
    if trace is None:
        if completed or data["available"] or data["status"] != "failed" or not data["error"]:
            raise ValueError("write timeline payload unavailable")
        return {"path": "result/write-timeline.json", "sha256": checksum,
                "coverage": {"status": data["status"], "available": False, "records": None,
                             "joined_records": None, "owner_expected_connections": None,
                             "owner_captured_connections": None, "error": data["error"]}}
    if not isinstance(trace, dict) or type(trace.get("limit")) is not int or trace["limit"] != limit:
        raise ValueError("write timeline payload limit differs")
    live = (expected_backend == "tidehunter_live_nvmeof"
            or (manifest_path.is_file() and manifest.get("backend_info", {}).get("name") == "tidehunter_live_nvmeof"))
    groups = trace.get("live_groups")
    dropped_groups = trace.get("dropped_live_groups")
    if live or "live_groups" in trace or "dropped_live_groups" in trace:
        if (not isinstance(groups, list) or len(groups) > limit
                or type(dropped_groups) is not int or not 0 <= dropped_groups < 2**64):
            raise ValueError("invalid live group timeline bounds")
    else:
        groups = None
    for field in ("dropped_calls", "dropped_events", "incomplete_calls", "dropped_checkpoint_events",
                  "owner_dropped_records", "owner_incomplete_records", "owner_expected_connections",
                  "owner_captured_connections"):
        if type(trace.get(field)) is not int or not 0 <= trace[field] <= 2**64 - 1:
            raise ValueError("invalid write timeline counter")
    for field in ("deadline_exceeded", "owner_storage_durations_available"):
        if type(trace.get(field)) is not bool:
            raise ValueError("invalid write timeline availability")
    expected = trace["owner_expected_connections"]
    captured = trace["owner_captured_connections"]
    records, owners, checkpoints, errors = (trace.get(k) for k in
                                           ("records", "owner_records", "checkpoint_events", "owner_errors"))
    if (not all(isinstance(value, list) for value in (records, owners, checkpoints, errors))
            or len(records) > limit or len(checkpoints) > limit * 64
            or len(owners) > limit * expected or captured > expected or len(errors) > expected * 2):
        raise ValueError("write timeline record counts exceed bounds")
    stages = {"caller_start", "gate_acquired", "validation_start", "admission_wait_started", "admission_acquired",
              "owner_enqueue_started", "owner_dequeued", "prepared", "admitted",
              "append_enqueue_started", "worker_dequeued", "tx_queued", "completion_harvested",
              "durable_observed", "published", "caller_reply_queued", "caller_finished",
              "live_group_build_started", "live_group_built", "live_reserve_started", "live_reserve_completed",
              "live_namespace_setup_started", "live_namespace_setup_completed", "live_io_started",
              "live_io_completed", "live_complete_started", "live_complete_completed", "live_receipts_validated",
              "live_extent_switch_started", "live_extent_switch_completed"}
    ids, owner_keys = set(), set()
    owner_success = {}
    operations, batches = set(), set()
    for record in records:
        if (not isinstance(record, dict) or type(record.get("caller_sample_id")) is not int
                or not 0 <= record["caller_sample_id"] < 2**64 or record["caller_sample_id"] in ids
                or type(record.get("completed")) is not bool or not isinstance(record.get("error"), (str, type(None)))
                or not isinstance(record.get("events"), list) or len(record["events"]) > 32):
            raise ValueError("invalid or ambiguous caller timeline record")
        ids.add(record["caller_sample_id"])
        operation, batch = record.get("operation"), record.get("batch")
        if operation is not None and (not isinstance(operation, list) or len(operation) != 16
                                      or any(type(byte) is not int or not 0 <= byte <= 255 for byte in operation)):
            raise ValueError("invalid timeline operation identity")
        if batch is not None and (not isinstance(batch, dict) or any(
                type(batch.get(key)) is not int or not 0 <= batch[key] <= 2**64 - 1
                for key in ("epoch", "sequence"))):
            raise ValueError("invalid timeline batch identity")
        if operation is not None:
            identity = tuple(operation)
            if identity in operations:
                raise ValueError("ambiguous timeline operation identity")
            operations.add(identity)
        if batch is not None:
            identity = (batch["epoch"], batch["sequence"])
            if identity in batches:
                raise ValueError("ambiguous timeline batch identity")
            batches.add(identity)
        for event in record["events"]:
            if (not isinstance(event, dict) or event.get("stage") not in stages
                    or type(event.get("elapsed_ns")) is not int or not 0 <= event["elapsed_ns"] <= 2**64 - 1
                    or (event.get("owner") is not None and
                        (type(event["owner"]) is not int or not 0 <= event["owner"] < 2**32))
                    or (event.get("envelope_id") is not None and
                        (type(event["envelope_id"]) is not int or not 0 <= event["envelope_id"] < 2**64))):
                raise ValueError("invalid coordinator timeline event")
    batch_records = {(record["batch"]["epoch"], record["batch"]["sequence"]): record
                     for record in records if record.get("batch") is not None}
    group_memberships = 0
    for group in groups or []:
        if (not isinstance(group, dict)
                or set(group) != {"owner", "batches", "reservation_token", "owner_validation_ns", "owner_persistence"}
                or type(group["owner"]) is not int or not 0 <= group["owner"] < 2**32
                or not isinstance(group["batches"], list) or not group["batches"]):
            raise ValueError("invalid live group timeline")
        token = group["reservation_token"]
        if token is not None and (not isinstance(token, list) or len(token) != 16
                                  or any(type(byte) is not int or not 0 <= byte <= 255 for byte in token)):
            raise ValueError("invalid live group reservation token")
        validation = group["owner_validation_ns"]
        if validation is not None and (type(validation) is not int or not 0 <= validation < 2**64):
            raise ValueError("invalid live group owner validation")
        if group["owner_persistence"] is not None:
            _validate_storage_write_profile(group["owner_persistence"])
        members = set()
        for batch in group["batches"]:
            if (not isinstance(batch, dict) or set(batch) != {"epoch", "sequence"}
                    or any(type(batch[field]) is not int or not 0 <= batch[field] < 2**64
                           for field in ("epoch", "sequence"))):
                raise ValueError("invalid live group batch identity")
            version = (batch["epoch"], batch["sequence"])
            if version in members or version not in batches:
                raise ValueError("duplicate or unjoined live group batch")
            observed_owners = {event["owner"] for event in batch_records[version]["events"]
                               if event["stage"].startswith("live_") and event.get("owner") is not None}
            if observed_owners and group["owner"] not in observed_owners and not trace["dropped_events"]:
                raise ValueError("live group owner does not match batch events")
            members.add(version)
        group_memberships += len(members)
        if group_memberships > 4 * limit:
            raise ValueError("live group memberships exceed bound")
    for owner in owners:
        if (not isinstance(owner, dict) or type(owner.get("owner")) is not int or owner["owner"] < 0
                or type(owner.get("envelope_id")) is not int or owner["envelope_id"] < 0
                or type(owner.get("success")) is not bool):
            raise ValueError("invalid owner timeline record")
        key = (owner["owner"], owner["envelope_id"])
        if key in owner_keys:
            raise ValueError("ambiguous owner timeline envelope")
        owner_keys.add(key)
        owner_success[key] = owner["success"]
        for field in ("control_queue_ns", "session_admission_wait_ns", "appender_queue_ns",
                      "flusher_queue_ns", "append_barrier_wait_ns", "append_wal_ns",
                      "append_persist_ns", "wal_file_persist_ns", "handler_ns", "response_ready_ns",
                      "namespace_persist_ns", "batch_count", "encoded_bytes", "epoch"):
            value = owner.get(field)
            if value is not None and (type(value) is not int or not 0 <= value <= 2**64 - 1):
                raise ValueError("invalid owner timeline duration or count")
    for event in checkpoints:
        if (not isinstance(event, dict) or not isinstance(event.get("stage"), str)
                or type(event.get("elapsed_ns")) is not int or not 0 <= event["elapsed_ns"] <= 2**64 - 1
                or (event.get("generation") is not None and
                    (type(event["generation"]) is not int or not 0 <= event["generation"] <= 2**64 - 1))):
            raise ValueError("invalid checkpoint timeline event")
        pressure = event.get("pressure")
        if pressure is not None and (
                not isinstance(pressure, dict) or not isinstance(pressure.get("reason"), str)
                or any(type(pressure.get(field)) is not int or not 0 <= pressure[field] <= 2**64 - 1
                       for field in ("batches", "encoded_bytes", "max_batches", "max_encoded_bytes"))
                or (pressure.get("incoming_encoded_bytes") is not None and
                    (type(pressure["incoming_encoded_bytes"]) is not int or
                     not 0 <= pressure["incoming_encoded_bytes"] <= 2**64 - 1))):
            raise ValueError("invalid checkpoint pressure")
    for error in errors:
        if (not isinstance(error, dict) or type(error.get("owner")) is not int
                or error["owner"] < 0 or error.get("phase") not in ("fetch", "disarm")
                or not isinstance(error.get("message"), str) or type(error.get("truncated")) is not bool):
            raise ValueError("invalid owner timeline error")
    samples_path = run / "result/samples.csv"
    rows = list(read_samples(samples_path)) if samples_path.is_file() else []
    raw_ids = [row["caller_sample_id"] for row in rows if row["caller_sample_id"] is not None]
    if len(raw_ids) != len(set(raw_ids)):
        raise ValueError("ambiguous raw caller IDs")
    joined = ids.intersection(raw_ids)
    linked = {(event["owner"], event["envelope_id"]) for record in records
              if record["caller_sample_id"] in joined for event in record["events"]
              if event.get("stage") in ("tx_queued", "completion_harvested")
              and event.get("owner") is not None and event.get("envelope_id") is not None}
    per_record_envelopes = []
    for record in records:
        queued = [event for event in record["events"] if event["stage"] == "tx_queued"]
        harvested = [event for event in record["events"] if event["stage"] == "completion_harvested"]
        if len(queued) != 1 or len(harvested) != 1:
            per_record_envelopes.append(False)
            continue
        first, last = queued[0], harvested[0]
        key = (first.get("owner"), first.get("envelope_id"))
        per_record_envelopes.append(
            None not in key and key == (last.get("owner"), last.get("envelope_id"))
            and first["elapsed_ns"] <= last["elapsed_ns"] and key in owner_keys)
    owner_coverage_missing = (not trace["owner_storage_durations_available"]
                              or not expected or captured != expected or errors
                              or trace["owner_incomplete_records"] or not linked
                              or not linked <= owner_keys or not all(per_record_envelopes)
                              or any(not owner_success[key] for key in linked if key in owner_success))
    if completed and (not data["measurement_attempted"] or not data["measurement_drained"]
            or not data["available"] or data["status"] != "complete" or data["error"] is not None
            or trace["deadline_exceeded"] or trace["incomplete_calls"]
            or not joined or ids != joined
            or (not live and owner_coverage_missing)
            or (live and not groups and not dropped_groups)
            or any(not record["completed"] or record["error"] is not None for record in records)
            or any(record.get("operation") is None or record.get("batch") is None for record in records)
            or any(row["outcome"] not in ("success", "ok") or row["operation"] not in ("put", "delete", "put_batch")
                   for row in rows if row["caller_sample_id"] in joined)):
        raise ValueError("completed write timeline lacks complete joined owner coverage")
    return {"path": "result/write-timeline.json", "sha256": checksum,
            "coverage": {"status": data["status"], "available": data["available"],
                         "records": len(records), "joined_records": len(joined) if samples_path.is_file() else None,
                         "owner_expected_connections": expected, "owner_captured_connections": captured,
                         "owner_errors": len(errors), "live_groups": len(groups) if groups is not None else None,
                         "dropped_live_groups": dropped_groups, "error": data["error"]}}


def verify_binding(run, config, receipt, manifest, summary):
    admitted_path = run / "normalized-config.json"
    admitted = json.loads(admitted_path.read_text())
    verify_normalization(config, admitted)
    if hashlib.sha256(admitted_path.read_bytes()).hexdigest() != receipt["normalized_config_sha256"]:
        raise ValueError(f"{run}: normalized admission changed after execution")
    executed = manifest.get("run", {})
    if executed.get("config") != admitted:
        raise ValueError(f"{run}: result configuration differs from admitted executable input")
    job = receipt.get("job", {})
    if "put_key_policy" in job:
        policy = job["put_key_policy"]
        if (admitted.get("workload", {}).get("put_key_policy") != policy
                or executed.get("data_policy", {}).get("put_key_policy") != policy):
            raise ValueError(f"{run}: executed put key policy differs from the job")
    if executed.get("binary", {}).get("blake2b512") != receipt["binary_blake2b512"]:
        raise ValueError(f"{run}: result executable differs from admitted binary")
    if manifest.get("backend_info") != summary.get("backend"):
        raise ValueError(f"{run}: manifest and summary disagree about the backend contract")
    hashes = receipt.get("artifacts_sha256", {})
    for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom"):
        file = run / "result" / name
        digest = hashlib.sha256()
        with file.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if hashes.get(name) != digest.hexdigest():
            raise ValueError(f"{run}: result artifact {name} is missing its original integrity binding")
    marker = manifest.get("run", {}).get("measurement_read_evidence_schema_version")
    if marker is not None and (type(marker) is not int or marker != 1):
        raise ValueError(f"{run}: unsupported measurement read evidence capability")
    if marker == 1 and receipt.get("measurement_read_evidence") is None:
        raise ValueError(f"{run}: advertised measurement read evidence is missing its integrity binding")
    evidence = receipt.get("measurement_read_evidence")
    if evidence is not None:
        if (not isinstance(evidence, dict) or evidence.get("path") != "result/measurement-read-evidence.json"
                or hashlib.sha256((run / evidence["path"]).read_bytes()).hexdigest() != evidence.get("sha256")):
            raise ValueError(f"{run}: measurement read evidence integrity binding differs")
    diagnostics = receipt.get("read_diagnostics")
    requested = config.get("diagnostics", {}).get("read_profile_limit", 0)
    path = run / "result/read-diagnostics.json"
    if diagnostics is None:
        if requested or path.exists():
            raise ValueError(f"{run}: read diagnostics are missing their integrity binding")
    elif (not isinstance(diagnostics, dict) or diagnostics.get("path") != "result/read-diagnostics.json"
          or hashlib.sha256(path.read_bytes()).hexdigest() != diagnostics.get("sha256")
          or json.loads(path.read_text()) != diagnostics.get("observed")):
        raise ValueError(f"{run}: read diagnostics integrity binding differs")
    if receipt.get("job", {}).get("selection") in ("stages", "unified", "fair_native"):
        diagnostics = receipt.get("write_diagnostics")
        requested = config.get("diagnostics", {}).get("write_profile_limit", 0)
        path = run / "result/write-diagnostics.json"
        if diagnostics is None:
            if requested or path.exists():
                raise ValueError(f"{run}: write diagnostics are missing their integrity binding")
        elif (not isinstance(diagnostics, dict) or diagnostics.get("path") != "result/write-diagnostics.json"
              or hashlib.sha256(path.read_bytes()).hexdigest() != diagnostics.get("sha256")
              or json.loads(path.read_text()) != diagnostics.get("observed")):
            raise ValueError(f"{run}: write diagnostics integrity binding differs")
    expected_backend = receipt.get("job", {}).get("backend")
    if expected_backend is not None:
        import cloud_campaign
        expected_backend = cloud_campaign.diagnostic_backend(receipt["job"])
    timeline = admit_write_timeline(run, admitted, completed=receipt.get("status") == "completed",
                                    expected_backend=expected_backend)
    if timeline != receipt.get("write_timeline"):
        raise ValueError(f"{run}: write timeline differs from its receipt integrity binding")


def valkey_view(text, identities, self_id):
    """Reduce a validated cluster view to advertised endpoint and slot ownership."""
    owners, seen, own = {}, set(), []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 9:
            raise ValueError("malformed Valkey cluster view")
        node_id, address, flags, master, _, _, _, link, *ranges = fields
        flags = set(flags.split(","))
        endpoint, _, bus = address.split(",")[0].partition("@")
        if (node_id not in identities or node_id in seen or
                endpoint != identities[node_id] or not bus.isdigit() or
                int(bus) != int(endpoint.rsplit(":", 1)[1]) + 10000 or
                flags - {"myself", "master"} or "master" not in flags or
                master != "-" or link != "connected"):
            raise ValueError("invalid Valkey cluster membership")
        seen.add(node_id)
        if "myself" in flags:
            own.append(node_id)
        for token in ranges:
            if not re.fullmatch(r"\d+(?:-\d+)?", token):
                raise ValueError("invalid Valkey slot range")
            parts = token.split("-")
            start, end = int(parts[0]), int(parts[-1])
            if not 0 <= start <= end < 16384:
                raise ValueError("invalid Valkey slot range")
            for slot in range(start, end + 1):
                if slot in owners:
                    raise ValueError("duplicate Valkey slot")
                owners[slot] = identities[node_id]
    if seen != set(identities) or own != [self_id] or len(owners) != 16384:
        raise ValueError("incomplete Valkey cluster view")
    return [owners[i] for i in range(16384)]


def admit_native_valkey(run, deployment, backend, config):
    """Admit the local native exporter, including its independently captured proof."""
    import local_valkey_rdma

    state = deployment["topology"]
    owner = json.loads((run / "deployment/owner.json").read_text())
    services = json.loads((run / "deployment/services.json").read_text())
    observed = deployment["observed"]
    count = state["nodes"]
    if owner != state or state.get("paired_valkey") is not True or state["backend"] != backend["name"]:
        raise ValueError("native Valkey owner/deployment mismatch")
    controls, rdma = state["control_endpoints"], state["rdma_endpoints"]
    cluster = state["layout"] != "qualification"
    if (count != (5 if cluster else 1) or len(controls) != count or len(rdma) != count or
            len(set(controls)) != count or len(set(rdma)) != count or
            len(state["services"]) != count or len(services) != count):
        raise ValueError("native Valkey inventory mismatch")
    options = local_valkey_rdma.options_for_backend(state["backend"], count, rdma)
    if (state["backend_options"] != options or
            config["backend"].get("options") != options or
            controls != [f"127.0.0.{21+i}:{6379+i}" for i in range(count)]):
        raise ValueError("native Valkey client endpoint contract differs")
    address = rdma[0].rsplit(":", 1)[0]
    if (ipaddress.IPv4Address(address).is_loopback or
            rdma != [f"{address}:{6380+i}" for i in range(count)]):
        raise ValueError("native Valkey RDMA endpoints differ")
    source = "fe8d406c0b5a64ec897f6455e4de684629883764"
    client = backend["configuration"]
    expected_transport = ("upstream libvalkey VALKEY_CONN_RDMA, RDMA-CM RC"
                          if state["backend"] == "valkey_rdma" else "upstream libvalkey VALKEY_CONN_TCP")
    if (observed.get("source_commit") != source or
            observed.get("transport") != ("RDMA-CM RC" if state["backend"] == "valkey_rdma" else "TCP") or
            client.get("source_commit") != source or client.get("transport") != expected_transport or
            "bundled libvalkey 0.2.1" not in backend.get("client_version", "") or
            backend.get("server_version") != "9.0.7"):
        raise ValueError("native Valkey source or transport differs")
    qualification = observed.get("rdma_qualification" if state["backend"] == "valkey_rdma" else "tcp_qualification", {})
    if qualification != {"status": "passed", "endpoints": options["nodes"], "operations": ["PING", "SET", "GET", "DEL"]}:
        raise ValueError("native Valkey client qualification missing")
    runtime = observed.get("runtime")
    if not isinstance(runtime, list) or len(runtime) != count:
        raise ValueError("native Valkey readiness runtime missing")
    roles = []
    for i, (service, exported, ready) in enumerate(zip(state["services"], services, runtime)):
        data = str(Path(state["directory"]) / f"node-{i}" / "data")
        argv = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, cluster, 1024)
        argv = [str(Path(state["runtime"]) / "bin/valkey-server") if x == "valkey-server"
                else x.replace("/data", data) for x in argv]
        argv += ["--rdma-bind", address, "--rdma-port", str(6380+i)]
        expected = {"runtime": state["runtime"], "unit": f"kvcloud-valkey-{state['owner'][:20]}-{i}",
                    "args": argv, "cpus": 2, "memory_mib": 1536, "source_commit": source,
                    "server_sha256": service.get("server_sha256")}
        digest = service.get("server_sha256")
        if service != expected or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("native Valkey service contract differs")
        if not isinstance(exported, list) or len(exported) != 1:
            raise ValueError("native Valkey exported runtime missing")
        for record in (ready, exported[0]):
            systemd = record.get("systemd", {})
            unit = service["unit"]
            cgroup = "/system.slice/" + unit + ".service"
            files = record.get("cgroup_files")
            limits = record.get("proc_limits")
            if not isinstance(files, dict) or not isinstance(limits, list):
                raise ValueError("native Valkey raw resource proof missing")
            cpu = files.get("cpu.max")
            match = re.fullmatch(r"([0-9]+) ([0-9]+)", cpu) if isinstance(cpu, str) else None
            if (files.get("memory.max") != str(service["memory_mib"] * 1024**2) or
                    files.get("memory.swap.max") != "0" or not match or int(match[2]) <= 0 or
                    int(match[1]) != service["cpus"] * int(match[2]) or
                    not any(isinstance(line, str) and line.startswith("Max locked memory") and
                            line.split()[3:5] == ["unlimited", "unlimited"] for line in limits)):
                raise ValueError("native Valkey cgroup or memlock proof differs")
            if (record.get("unit") != unit or record.get("cgroup") != cgroup or
                    type(record.get("pid")) is not int or record["pid"] <= 0 or
                    record.get("source") != source or record.get("server_sha256") != digest or
                    systemd.get("LoadState") != "loaded" or systemd.get("ActiveState") != "active" or
                    systemd.get("MainPID") != str(record["pid"]) or systemd.get("ControlGroup") != cgroup or
                    systemd.get("MemoryMax") != str(service["memory_mib"] * 1024**2) or
                    systemd.get("LimitMEMLOCK") not in ("infinity", "18446744073709551615") or
                    systemd.get("CPUQuotaPerSecUSec") not in ("2s", "2000000us", "2000000") or
                    "argv[]=" + " ".join(service["args"]) + " ;" not in systemd.get("ExecStart", "") or
                    not record.get("argv")):
                raise ValueError("native Valkey systemd runtime proof differs")
        if ready["pid"] != exported[0]["pid"] or ready["systemd"]["CPUQuotaPerSecUSec"] != exported[0]["systemd"]["CPUQuotaPerSecUSec"]:
            raise ValueError("native Valkey runtime changed")
        roles.append({"server_sha256": digest, "cpu_quota_cpus": service["cpus"],
                      "memory_max_bytes": service["memory_mib"] * 1024**2,
                      "memory_swap_max_bytes": 0, "memlock": "unlimited"})
    dirs = {endpoint: str(Path(state["directory"]) / f"node-{i}" / "data") for i, endpoint in enumerate(controls)}
    try:
        coverage = local.admit_valkey(observed["nodes"], controls, cluster, data_dir=dirs)
        if coverage != observed["coverage"]:
            raise ValueError("native Valkey recorded coverage differs")
        core_settings = client["node_config"]
        if len(core_settings) != count:
            raise ValueError("native Valkey client settings missing")
        persistence = client["persistence"]
        if len(persistence) != count or any(
                item.get("loading") != "0" or item.get("aof_enabled") != "1" or
                item.get("aof_last_write_status") != "ok" or item.get("aof_last_bgrewrite_status") != "ok"
                for item in persistence):
            raise ValueError("native Valkey client persistence health differs")
        keys = ("appendonly", "appendfsync", "no-appendfsync-on-rewrite", "maxmemory-policy")
        for node, settings in zip(observed["nodes"], core_settings):
            if any(settings.get(key) != node["effective_config"].get(key) for key in keys):
                raise ValueError("native Valkey client durability settings differ")
            cluster_file = node["effective_config"].get("cluster-config-file")
            if cluster and cluster_file != str(Path(dirs[node["endpoint"]]) / "nodes.conf"):
                raise ValueError("native Valkey cluster configuration path differs")
        topology = backend["topology"]
        ids = topology["node_ids"]
        views = topology["cluster_nodes_views"]
        if (topology["cluster"] is not cluster or topology["primary_count"] != count or
                topology["slot_count"] != (16384 if cluster else None) or topology["endpoints"] != options["nodes"]):
            raise ValueError("native Valkey backend topology differs")
        expected_slots = None
        if cluster:
            if (len(ids) != count or len(set(ids)) != count or len(views) != count or
                    ids != [coverage["primary_ids"][endpoint] for endpoint in controls]):
                raise ValueError("native Valkey backend identity differs")
            identities = dict(zip(ids, controls))
            expected_slots = valkey_view(observed["nodes"][0]["cluster_nodes"], identities, ids[0])
            for i, view in enumerate(views):
                if valkey_view(view, identities, ids[i]) != expected_slots:
                    raise ValueError("native Valkey backend ownership differs")
        elif ids != [] or views != []:
            raise ValueError("standalone Valkey cluster proof differs")
    except (KeyError, TypeError, IndexError, RuntimeError) as exc:
        raise ValueError(f"invalid native Valkey evidence: {exc}") from exc
    # Slots map to advertised control endpoints; selected client endpoints are retained separately.
    stable = {key: value for key, value in topology.items() if key not in ("node_ids", "cluster_nodes_views")}
    stable["slot_owners"] = expected_slots
    settings = []
    for node in observed["nodes"]:
        omitted = ("dir", "cluster-config-file") if cluster else ("dir",)
        settings.append({key: value for key, value in node["effective_config"].items() if key not in omitted})
    return ({"kind": "native_systemd", "layout": state["layout"], "nodes": count,
             "physical_hosts": state["physical_hosts"], "independent_disks": state["independent_disks"],
             "roles": roles, "source_commit": source,
             "resource_evidence": "observed systemd, cgroup files and process limits"},
            stable, settings)


def stable_topology(name, original):
    topology = dict(original)
    for field in ("path", "process_id", "cluster_file", "cluster_name", "coordinator_process_id", "owner_process_evidence", "owner_processes"):
        topology.pop(field, None)
    if name == "tidehunter_tcp" and "endpoints" in topology:
        topology["endpoints"] = sorted(
            ({"partition": item["partition"]} for item in topology["endpoints"]),
            key=lambda item: item["partition"])
    if name in ("valkey_tcp", "valkey_rdma", "garnet", "kvrocks") and isinstance(topology.get("primaries"), list):
        topology["primaries"] = sorted(
            ({key: value for key, value in item.items() if key != "node_id"}
             for item in topology["primaries"]), key=canonical)
    if name == "tikv_raw" and "store_metadata" in topology:
        # IDs, capacity/current load, start/heartbeat times are observations,
        # not independent-run configuration. Preserve the raw original artifact.
        topology["store_metadata"] = sorted((
            {key: row["store"].get(key) for key in ("address", "status_address", "version", "git_hash", "state_name")}
            | {"labels": sorted(row["store"].get("labels", []), key=canonical)}
            for row in topology["store_metadata"].get("stores", [])), key=canonical)
    if name == "foundationdb" and "cluster_status" in topology:
        cluster = topology["cluster_status"].get("cluster", {})
        # Role assignments/IDs/loads can change across legitimate repetitions.
        # Keep configured placement/classes and health, not volatile recruitment.
        topology["cluster_status"] = {
            "configuration": cluster.get("configuration"),
            "database_available": cluster.get("database_available"),
            "full_replication": cluster.get("full_replication"),
            "protocol_version": cluster.get("protocol_version"),
            "processes": sorted(({
                "address": item.get("address"), "version": item.get("version"),
                "class_type": item.get("class_type"),
                "locality": {key: item.get("locality", {}).get(key) for key in ("dcid", "machineid", "zoneid")}
            } for item in cluster.get("processes", {}).values()), key=canonical),
        }
    return topology


def stable_configuration(original, name=None):
    config = {key: value for key, value in original.items() if key not in ("coordinator_path", "cluster_file")}
    if isinstance(config.get("loaded_native_library"), dict):
        config["loaded_native_library"] = {
            key: value for key, value in config["loaded_native_library"].items() if key != "path"}
    if name in ("valkey_tcp", "valkey_rdma") and isinstance(config.get("persistence"), list):
        # Retain health/guarantee evidence, not changing AOF sizes, timestamps,
        # rewrite progress or counters. Original artifacts retain every field.
        fields = ("aof_enabled", "loading", "async_loading", "aof_last_write_status", "aof_last_bgrewrite_status")
        config["persistence"] = [{key: item[key] for key in fields if key in item}
                                 for item in config["persistence"]]
    if name in ("garnet", "kvrocks"):
        # Raw INFO snapshots include changing counters, sizes and timestamps;
        # effective CONFIG and the storage policy remain part of the contract.
        config.pop("node_info", None)
    return config


def host_capacity(meminfo):
    values = {}
    for line in meminfo.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in ("MemTotal", "SwapTotal"):
            amount, unit = value.split()
            if unit != "kB" or not amount.isdigit():
                raise ValueError("invalid host memory-capacity evidence")
            values[key] = int(amount) * 1024
    if set(values) != {"MemTotal", "SwapTotal"} or values["MemTotal"] <= 0:
        raise ValueError("missing host RAM/swap capacity evidence")
    return values



def admit_industry_observation(kind, deployment, backend):
    """Reconstruct RF1 slot and durable-setting proof from the raw observations."""
    import industry_service

    topology = deployment.get("topology", {})
    observed = deployment.get("observed", {})
    if (topology.get("nodes") != 5 or backend.get("name") != kind
            or backend.get("replication_factor") != 1):
        raise ValueError("industry deployment must have five actual RF1 primaries")
    nodes = observed.get("nodes")
    coverage = observed.get("coverage")
    if not isinstance(nodes, list) or not isinstance(coverage, dict):
        raise ValueError("missing industry primary/slot observations")
    endpoints = coverage.get("primary_endpoints")
    if not isinstance(endpoints, list) or len(endpoints) != 5 or len(set(endpoints)) != 5:
        raise ValueError("missing industry primary endpoint inventory")
    try:
        reconstructed = industry_service.admit(kind, nodes, endpoints)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise ValueError(f"invalid industry primary/slot/durability evidence: {exc}") from exc
    if reconstructed != coverage:
        raise ValueError("industry recorded coverage disagrees with raw observations")
    core = backend.get("topology", {})
    primaries = core.get("primaries")
    if (core.get("cluster") is not True or core.get("primary_count") != 5
            or core.get("slot_count") != 16384 or not isinstance(primaries, list)
            or len(primaries) != 5):
        raise ValueError("missing industry backend primary/slot observation")
    try:
        core_slots = {}
        for primary in primaries:
            endpoint = primary["address"]
            if reconstructed["primary_ids"].get(endpoint) != primary["node_id"]:
                raise ValueError("industry backend primary identities disagree with deployment")
            for start, end in primary["slot_ranges"]:
                if type(start) is not int or type(end) is not int or not 0 <= start <= end < 16384:
                    raise ValueError("invalid industry backend slot range")
                for slot in range(start, end + 1):
                    if slot in core_slots:
                        raise ValueError("duplicate industry backend slot")
                    core_slots[slot] = endpoint
        actual_slots = {}
        for start, end, primary in nodes[0]["cluster_slots"]:
            endpoint = f"{primary[0]}:{primary[1]}"
            actual_slots.update((slot, endpoint) for slot in range(start, end + 1))
        if core_slots != actual_slots:
            raise ValueError("industry backend slot ownership disagrees with deployment")
    except (KeyError, TypeError, IndexError) as exc:
        raise ValueError("missing industry backend slot observations") from exc
    return reconstructed


def admit_industry_runtime(kind, deployment, containers):
    import industry_service

    runtime = deployment.get("observed", {}).get("runtime")
    if not isinstance(runtime, list) or not isinstance(containers, list):
        raise ValueError("missing actual industry launch and storage-tier proof")
    try:
        proof = industry_service.admit_local_runtime(kind, runtime, deployment["topology"])
        observed = {(item["service"], item["id"], item["image_id"]) for item in runtime}
        exported = {(item["service"], item["id"], item["image_id"]) for item in containers}
        if len(observed) != 5 or observed != exported:
            raise ValueError("industry launch receipt disagrees with exported containers")
        return proof
    except (KeyError, TypeError, RuntimeError) as exc:
        raise ValueError(f"invalid industry launch and storage-tier proof: {exc}") from exc


def requested_nvmeof_route(receipt):
    """Preserve the planned route in the pooling boundary, not as observed proof."""
    job = receipt.get("job")
    if job is None:
        return None
    if not isinstance(job, dict):
        raise ValueError("invalid receipt job")
    route = job.get("nvmeof_route")
    if route is not None:
        if route not in ("live", "sealed_read") or job.get("backend") not in ("tidehunter_nvme_tcp", "tidehunter_nvme_rdma"):
            raise ValueError("invalid requested NVMe-oF route")
        sealed_population = (job.get("cohort") == "sealed_read"
                             or (job.get("case_kind") == "hybrid_put" and job.get("cohort") == "primary_rf1"))
        if (route == "sealed_read") != sealed_population:
            raise ValueError("NVMe-oF route and workload population disagree")
    return route


def boundary(run):
    receipt = json.loads((run / "receipt.json").read_text())
    if receipt.get("status") != "completed" or receipt.get("claim_boundary") == "API qualification only":
        raise ValueError(f"{run}: failed/qualification run cannot enter performance comparison")
    observation = receipt.get("observation")
    if not isinstance(observation, dict) or observation.get("state") not in ("exact_project_present", "exact_project_absent_other_observers_unknown"):
        raise ValueError(f"{run}: unknown/missing actual observer-project presence")
    if receipt.get("observation_after") != observation:
        raise ValueError(f"{run}: observer observation contract changed or lacks final verification")
    config = json.loads((run / "config.json").read_text())
    manifest = json.loads((run / "result/manifest.json").read_text())
    # Core records actual backend metadata; a config echo cannot substitute for it.
    summary = json.loads((run / "result/summary.json").read_text())
    if manifest.get("status") not in ("completed", "success") or summary.get("status") not in ("completed", "success"):
        raise ValueError(f"{run}: core result did not complete successfully")
    if hashlib.sha256((run / "config.json").read_bytes()).hexdigest() != receipt["config_sha256"]:
        raise ValueError(f"{run}: effective config hash changed after execution")
    verify_binding(run, config, receipt, manifest, summary)
    backend = summary.get("backend", manifest.get("backend_info", manifest.get("backend")))
    required = {"name", "server_version", "client_version", "acknowledgement", "consistency", "replication_factor", "measurement_boundary", "get_many_strategy", "batch_atomicity", "topology", "configuration"}
    if not isinstance(backend, dict) or not required <= set(backend):
        raise ValueError(f"{run}: manifest lacks observed effective BackendInfo")
    deployment_path = run / "deployment/deployment.json"
    deployment = json.loads(deployment_path.read_text()) if deployment_path.exists() else None
    if config["backend"]["kind"] in ("tikv", "cassandra", "foundationdb", "valkey_tcp", "valkey_rdma", "garnet", "kvrocks"):
        if not deployment:
            raise ValueError(f"{run}: missing actual external deployment/replication proof")
        observed, nodes = deployment["observed"], deployment["topology"]["nodes"]
        expected_rf = (1 if config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma", "garnet", "kvrocks") else
                       (1 if nodes == 1 else 2) if config["backend"]["kind"] == "foundationdb" else nodes)
        if backend["replication_factor"] != expected_rf:
            raise ValueError(f"{run}: observed backend RF does not match admitted deployment")
        if config["backend"]["kind"] == "tikv":
            regions = observed["regions"]["regions"]
            if not regions or any(len(region.get("peers", [])) != nodes or not region.get("leader") or region.get("down_peers") or region.get("pending_peers") for region in regions):
                raise ValueError(f"{run}: no complete TiKV region ownership proof")
        elif config["backend"]["kind"] == "foundationdb":
            if not observed["cluster"].get("full_replication") or not observed["cluster"].get("database_available"):
                raise ValueError(f"{run}: missing available/full-replication FoundationDB observation")
        elif config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma"):
            expected_primaries = 1 if deployment["topology"]["layout"] == "qualification" else 5
            coverage = observed.get("coverage", {})
            if (nodes != expected_primaries or coverage.get("required_primary_nodes") != nodes
                    or coverage.get("replication_factor") != 1 or coverage.get("replica_count") != 0
                    or len(observed.get("nodes", [])) != nodes
                    or len(set(coverage.get("primary_endpoints", []))) != nodes
                    or coverage.get("slot_count") != (0 if nodes == 1 else 16384)):
                raise ValueError(f"{run}: incomplete Valkey RF1 primary/slot ownership proof")
        elif config["backend"]["kind"] in ("garnet", "kvrocks"):
            admit_industry_observation(config["backend"]["kind"], deployment, backend)
        elif len(observed["nodes"]) != nodes:
            raise ValueError(f"{run}: incomplete Cassandra member observations")
    # Dynamic per-run namespaces/storage roots are not compared. Retain raw originals and
    # compare all semantic info plus the admitted topology/resource budget instead.
    guarantee = {key: backend[key] for key in required - {"configuration", "topology"}}
    if config["backend"]["kind"] == "foundationdb":
        guarantee["native_client_runtime"] = {
            key: value for key, value in json.loads((run.parent / "fdb-client.json").read_text()).items()
            if key != "path"}
    guarantee["effective_configuration"] = stable_configuration(backend["configuration"], backend["name"])
    native = (deployment and config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma")
              and deployment["topology"].get("paired_valkey") is True)
    if native:
        deployment_budget, stable_valkey, valkey_settings = admit_native_valkey(run, deployment, backend, config)
        guarantee["observed_topology"] = stable_valkey
        guarantee["deployment_effective_configuration"] = valkey_settings
    else:
        guarantee["observed_topology"] = stable_topology(backend["name"], backend["topology"])
    if config["backend"]["kind"] in ("garnet", "kvrocks"):
        guarantee["deployment_effective_configuration"] = sorted(
            (node["effective_config"] for node in deployment["observed"]["nodes"]), key=canonical)
    hardware = receipt["hardware"]
    capacity = host_capacity(hardware["memory"])
    hardware = {key: hardware[key] for key in ("machine", "cpu", "kernel", "affinity", "block_devices")}
    hardware["memory_capacity_bytes"] = capacity
    first_cpu = dict(line.split(":", 1) for line in hardware["cpu"].split("\n\n", 1)[0].splitlines() if ":" in line)
    hardware["cpu"] = {key.strip(): value.strip() for key, value in first_cpu.items()
                       if key.strip() in ("vendor_id", "cpu family", "model", "model name", "stepping", "flags", "CPU implementer", "CPU architecture", "CPU part")}
    if not native:
        deployment_budget = None
    if deployment and not native:
        topology = deployment["topology"]
        compose = json.loads((run / "deployment/compose.json").read_text())
        actual = json.loads((run / "deployment/containers.json").read_text())
        resolved_images = {item["requested"]: item["image_id"] for item in json.loads((run / "deployment/images.json").read_text())}
        if config["backend"]["kind"] in ("garnet", "kvrocks"):
            admit_industry_runtime(config["backend"]["kind"], deployment, actual)
        if {item["service"] for item in actual} != set(compose["services"]):
            raise ValueError(f"{run}: actual role/container inventory differs from deployment")
        for item in actual:
            limits = compose["services"][item["service"]]
            if item["image_id"] != resolved_images.get(limits["image"]):
                raise ValueError(f"{run}: actual server image differs from the admitted digest-pinned image")
            memory = limits["mem_limit"].lower()
            expected_bytes = int(memory[:-1]) * {"m": 1024**2, "g": 1024**3}[memory[-1]]
            if item["memory_bytes"] != expected_bytes or item["nano_cpus"] != int(limits["cpus"] * 1e9):
                raise ValueError(f"{run}: actual server resource caps differ from declared budgets")
            if not isinstance(item.get("runtime_configuration_sha256"), str) or len(item["runtime_configuration_sha256"]) != 64:
                raise ValueError(f"{run}: missing actual server runtime-configuration identity")
        role_identities = {item["service"]: item["runtime_configuration_sha256"] for item in actual}
        deployment_budget = {"layout": topology["layout"], "nodes": topology["nodes"],
                             "physical_hosts": topology["physical_hosts"], "independent_disks": topology["independent_disks"],
                             "roles": {name: {**{key: service[key] for key in ("cpus", "mem_limit", "image")},
                                             "runtime_configuration_sha256": role_identities[name]}
                                       for name, service in compose["services"].items()},
                             "mounted_configuration_sha256": {name: hashlib.sha256((run / "deployment" / name).read_bytes()).hexdigest()
                                                               for name in ("pd.toml", "tikv.toml")
                                                               if config["backend"]["kind"] == "tikv"}}
    return {"backend": guarantee, "workload": {key: config[key] for key in ("dataset", "workload", "execution", "phases", "limits", "seed")},
            "hardware": hardware, "deployment": deployment_budget, "client_budget": receipt["client_budget"],
            "binary_sha256": receipt["binary_sha256"], "claim_boundary": receipt["claim_boundary"],
            "observation": receipt["observation"],
            "diagnostics": {"write_profile_limit": 0, "read_profile_limit": 0, "write_timeline_limit": 0, **config.get("diagnostics", {})},
            "completion_accounting_gate": config.get("measurement_completion_gate") is not None,
            "storage_population": receipt["storage_population"],
            "requested_nvmeof_route": requested_nvmeof_route(receipt)}


def independent_run_identity(run):
    config = json.loads((run / "config.json").read_text())
    receipt = json.loads((run / "receipt.json").read_text())
    identity = (config.get("namespace"), receipt.get("started_at_unix"), receipt.get("binary_sha256"))
    if (not isinstance(identity[0], str) or not identity[0]
            or type(identity[1]) not in (int, float) or not math.isfinite(identity[1]) or identity[1] <= 0
            or not isinstance(identity[2], str) or not identity[2]):
        raise ValueError("missing independent-run identity")
    return identity


def analyze(runs):
    if not runs:
        raise ValueError("at least one result directory required")
    runs = [path.resolve(strict=True) for path in runs]
    if len(set(runs)) != len(runs):
        raise ValueError("duplicate result directory")
    boundaries = [boundary(path) for path in runs]
    identity = canonical(boundaries[0])
    if any(canonical(item) != identity for item in boundaries[1:]):
        raise ValueError("different workload/hardware/guarantee/version/resource/observation contracts; analyze separately, never average p99 or assert speedup")
    identities = [independent_run_identity(path) for path in runs]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicated independent-run identity; copied results are not repetitions")
    samples, per_run = [], []
    for run in runs:
        rows = list(read_samples(run / "result/samples.csv"))
        if not rows:
            raise ValueError(f"{run}: no measured arrivals")
        config = json.loads((run / "config.json").read_text())
        requested = config["phases"]["measurement_operations"]
        if len(rows) != requested:
            raise ValueError(f"{run}: {len(rows)} raw arrivals do not account for all {requested} requested arrivals")
        offered_rate = config["execution"].get("offered_ops_per_second") if config["execution"]["mode"] == "open_loop" else None
        start = min(row["scheduled_ns"] for row in rows)
        end = max(row["completed_ns"] or row["scheduled_ns"] for row in rows)
        elapsed = (end - start) / 1e9
        offered_window = requested / offered_rate if offered_rate else None
        exposure = max(elapsed, offered_window or 0)
        success = sum(row["outcome"] in ("ok", "success") for row in rows)
        per_run.append({"run": str(run), "offered_calls": requested, "success_calls": success,
                        "offered_calls_per_second": offered_rate, "offered_window_seconds": offered_window,
                        "response_window_seconds": elapsed, "achieved_success_calls_per_second": success / exposure if exposure else None})
        samples.extend(rows)
    return {"schema_version": 1, "runs": [str(x) for x in runs], "boundary_sha256": hashlib.sha256(identity.encode()).hexdigest(),
            "boundary": boundaries[0], "aggregation": "nearest-rank quantiles of pooled raw samples, never average run p99",
            "per_run_accounting": per_run, "statistics": aggregate(samples)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    local.dump(args.output, analyze(args.runs))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as exc:
        print(f"kv-comparison analysis rejected: {exc}", file=sys.stderr)
        sys.exit(1)
