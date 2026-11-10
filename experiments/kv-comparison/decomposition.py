"""Pure helpers for measurement-boundary decomposition evidence.

Means are conditional on each stage's own count.  Resolution and remote-flight
samples are independently selected; annotations overlap and cannot be subtracted.
Timeline clocks are coordinator-local; owner durations are never aligned with them.
"""

from collections import Counter, defaultdict
from statistics import fmean, median


def _row(stage, total_ns, count, selection, overlap, parent=None, exactness="sampled_mean"):
    return {
        "stage": stage, "mean_us": total_ns / count / 1000 if count and total_ns is not None else None,
        "count": count, "exactness": exactness, "selection": selection,
        "overlap": overlap, "parent": parent,
    }


def reader_stages(evidence):
    """Aggregate measurement deltas over readers, weighted by stage counts."""
    totals = defaultdict(lambda: [0, 0])
    for reader in evidence.get("nvmeof", {}).get("readers", {}).values():
        for name, stage in reader.get("stages", {}).items():
            delta = stage.get("delta") or {}
            counters = delta.get("counters")
            if not delta.get("available") or not counters or "sum_ns" not in counters or "count" not in counters:
                continue
            totals[name][0] += counters["sum_ns"]
            totals[name][1] += counters["count"]
    return [_row(name, total, count, "measurement-boundary reader delta",
                 "reader stage annotation; do not add to coordinator stages")
            for name, (total, count) in sorted(totals.items())]


def _diskstats(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 14:
            result[(int(fields[0]), int(fields[1]))] = tuple(int(fields[i]) for i in (3, 5, 6))
    return result


def kernel_device(before, after, devices):
    """Delta Linux diskstats for explicitly selected host/device identities.

    Devices use major/minor and host_index, owner_index (owner + 1), or
    client_index (-1 for the client in hosts[0]).
    A missing device, reset counter, or duplicate identity is an error, not zero IO.
    """
    totals = [0, 0, 0]
    seen = set()
    for device in devices:
        if "host_index" in device:
            host = device["host_index"]
        elif "client_index" in device:
            if device["client_index"] != -1:
                raise ValueError("unsupported client device index")
            host = 0
        else:
            host = device["owner_index"] + 1
        key = (device["major"], device["minor"])
        identity = (host, key)
        if identity in seen:
            raise ValueError("duplicate device identity")
        seen.add(identity)
        start = _diskstats(before["hosts"][host]["diskstats"])[key]
        end = _diskstats(after["hosts"][host]["diskstats"])[key]
        delta = [b - a for a, b in zip(start, end)]
        if any(value < 0 for value in delta):
            raise ValueError("diskstats counter decreased")
        totals = [a + b for a, b in zip(totals, delta)]
    reads, sectors, milliseconds = totals
    return {"count": reads, "bytes": sectors * 512,
            "mean_us": milliseconds * 1000 / reads if reads else None,
            "exactness": "device_counter", "selection": "selected devices, measurement-boundary delta"}


def spdk_bdev(start, end):
    """Compute weighted SPDK bdev read latency from corresponding targets."""
    if len(start) != len(end):
        raise ValueError("SPDK target count changed")
    count = byte_count = 0
    latency_us = 0.0
    for first, last in zip(start, end):
        a, b = first["target"]["bdev_iostat"], last["target"]["bdev_iostat"]
        if a["tick_rate"] != b["tick_rate"] or b["tick_rate"] <= 0:
            raise ValueError("SPDK tick rate changed or invalid")
        old = {item["name"]: item for item in a["bdevs"]}
        new = {item["name"]: item for item in b["bdevs"]}
        if len(old) != len(a["bdevs"]) or len(new) != len(b["bdevs"]) or old.keys() != new.keys():
            raise ValueError("SPDK bdev identities changed or duplicated")
        for name, item in new.items():
            delta = [item[field] - old[name][field] for field in
                     ("num_read_ops", "bytes_read", "read_latency_ticks")]
            if any(value < 0 for value in delta):
                raise ValueError("SPDK counter decreased")
            count += delta[0]
            byte_count += delta[1]
            latency_us += delta[2] * 1_000_000 / b["tick_rate"]
    return {"count": count, "bytes": byte_count,
            "mean_us": latency_us / count if count else None,
            "exactness": "device_counter", "selection": "SPDK bdev measurement-boundary delta"}


def read_profile(profile):
    """Return coordinator resolution leaves and independent remote annotations."""
    rows = []
    resolution = profile.get("coordinator_resolution") or {}
    for name, data in resolution.get("stages", {}).items():
        # These counters are not instrumented even when the profiler emits zeros.
        if name in ("index_lock_wait", "cache_lock_wait"):
            continue
        rows.append(_row(name, data.get("sum_ns"), data.get("count", 0),
                         resolution.get("selection_policy", "independent API resolution prefix"),
                         "coordinator sibling stage; different population from remote flights"))
    for name, data in profile.get("stages", {}).items():
        if name.startswith("server_") and name != "server_handler":
            parent = "server_handler" if name in ("server_authority_lookup", "server_record_read", "server_response_recheck_wait", "server_response_recheck") else "server_record_read"
        elif name in ("request_encode", "response_decode", "server_handler"):
            parent = "exchange"
        else:
            parent = None
        overlap = "inclusive annotation; overlaps other stages, not additive"
        if name == "nvme_flight":
            overlap += "; flight admission through terminal completion and verification, including queueing"
        rows.append(_row(name, data.get("sum_ns"), data.get("count", 0),
                         profile.get("selection_policy", "independent remote-flight prefix"), overlap, parent))
    return rows


def read_residuals(profile, readers, device, api_mean_us, measured_count, route, cold):
    """Compute per-read differences only with explicit, complete window coverage.

    Conditional stage totals are normalized by the measured API population.
    Device time is the window's total device service divided by that population,
    not its per-I/O latency. Owner inline includes the nested exchange; its
    outside-exchange portion is accounted separately in the component table.
    Reader submit-to-CQ already includes device and derived fabric, so it is
    subtracted once. No cross-host clocks are aligned.
    """
    if route not in ("native", "owner", "hybrid"):
        raise ValueError("unknown read route")
    profile = profile or {}
    device = device or {}
    rows = []
    population_ok = type(measured_count) is int and measured_count > 0

    def make_row(name, value, missing, sources):
        rows.append({
            "stage": name, "mean_us": value if not missing else None,
            "count": measured_count if population_ok else 0,
            "exactness": "derived_mean_difference",
            "selection": ("full successful measured population; measurement-boundary device and reader windows"
                          if not missing else "not computed: " + "; ".join(missing)),
            "source_stages": sources,
        })

    def device_time(missing):
        count, mean = device.get("count"), device.get("mean_us")
        if (not population_ok or type(count) is not int or count <= 0
                or not isinstance(mean, (int, float)) or isinstance(mean, bool) or mean < 0):
            missing.append("missing measurement-window device count or mean")
            return 0
        return count * mean / measured_count

    if route == "native":
        if cold:
            missing = []
            if not population_ok or api_mean_us is None:
                missing.append("missing successful measured population or API mean")
            elapsed = device_time(missing)
            if not missing and device["count"] / measured_count > 1.05:
                missing.append("device counters include non-API reads")
            if not missing and api_mean_us < elapsed:
                missing.append("device time exceeds API mean")
            make_row("native cold software", api_mean_us - elapsed if not missing else None,
                     missing, ["api_mean", "client device"])
        return rows

    resolution = profile.get("coordinator_resolution") or {}
    counters = profile.get("counters") or {}
    missing = []
    if not population_ok or api_mean_us is None:
        missing.append("missing successful measured population or API mean")
    if resolution.get("selected") != measured_count or not population_ok:
        missing.append("coordinator selected != measured_count")
    if profile.get("coordinator_resolution_valid") is not True:
        missing.append("coordinator resolution validity unavailable")
    for field in ("errors", "member_errors"):
        if resolution.get(field) != 0:
            missing.append("coordinator " + field + " missing or nonzero")
    if profile.get("selected_operations") != measured_count or not population_ok:
        missing.append("remote selected_operations != measured_count")
    if profile.get("completed_operations") != measured_count or not population_ok:
        missing.append("remote completed_operations != measured_count")
    if counters.get("read_profile_errors") != 0:
        missing.append("remote errors missing or nonzero")
    if profile.get("finalization_error"):
        missing.append("remote profile finalization error")
    coverage_missing = missing.copy()

    leaves = ("index_lookup", "cache_lookup", "cut_validation", "cut_capture",
              "route_preparation", "pin_preparation", "batch_preparation",
              "batch_wait", "postread_cache_fill", "result_materialization")

    def stage_total(stages, name, errors):
        stage = stages.get(name)
        if not isinstance(stage, dict):
            errors.append("missing " + name)
            return 0
        count, total = stage.get("count"), stage.get("sum_ns")
        if type(count) is not int or count < 0 or count > measured_count:
            errors.append("invalid " + name + " count")
            return 0
        if count == 0 and total is None and stage.get("unavailable_reason") == "no observations for this stage":
            return 0
        if (not isinstance(total, (int, float)) or isinstance(total, bool)
                or total < 0 or stage.get("available") is False):
            errors.append("missing " + name + " duration")
            return 0
        return total / measured_count / 1000

    leaf_total = 0
    if population_ok:
        for name in leaves:
            leaf_total += stage_total(resolution.get("stages") or {}, name, missing)
    sources = ["api_mean", *leaves]
    if route == "owner":
        if population_ok:
            leaf_total += stage_total(profile.get("stages") or {}, "inline", missing)
        sources.append("inline")
    else:
        by_name = {row.get("stage"): row for row in readers or []}
        for name in ("admission", "alignment", "queue", "submit_to_cq", "extraction", "verification"):
            row = by_name.get(name) or {}
            count, mean = row.get("count"), row.get("mean_us")
            if (not population_ok or type(count) is not int or count < 0
                    or count > measured_count or not isinstance(mean, (int, float))
                    or isinstance(mean, bool) or mean < 0):
                missing.append("missing or invalid reader " + name)
            else:
                leaf_total += count * mean / measured_count
            sources.append("reader " + name)
        sources.append("reader submit_to_cq includes device and fabric")
    make_row("client other", api_mean_us - leaf_total if not missing else None, missing, sources)

    if route == "owner" and cold:
        server_missing = coverage_missing.copy()
        handler = (profile.get("stages") or {}).get("server_handler") or {}
        if handler.get("count") != measured_count or not population_ok:
            server_missing.append("server_handler does not cover measured population")
            handler_total = 0
        else:
            handler_total = stage_total({"server_handler": handler}, "server_handler", server_missing)
        elapsed = device_time(server_missing)
        make_row("owner server software", handler_total - elapsed, server_missing,
                 ["server_handler", "owner device"])
    return rows


def write_timeline(evidence):
    """Return exact adjacent coordinator event intervals for every retained call."""
    rows = []
    for record in evidence.get("records", []):
        events = record.get("events", [])
        for first, last in zip(events, events[1:]):
            start, end = first["elapsed_ns"], last["elapsed_ns"]
            if end < start:
                raise ValueError("timeline event order is not monotonic")
            row = _row(first["stage"] + " -> " + last["stage"], end - start, 1,
                       "retained coordinator call", "adjacent coordinator interval", exactness="exact_interval")
            row.update(caller_sample_id=record["caller_sample_id"], start_stage=first["stage"],
                       end_stage=last["stage"], start_ns=start, end_ns=end,
                       completed=record.get("completed"), error=record.get("error"))
            rows.append(row)
    return rows


def owner_timeline(timeline):
    """Summarize linked owner envelopes and unshared exchange residuals.

    Owner and coordinator timestamps have independent origins.  The residual
    subtracts only their locally measured durations, never their timestamps.
    """
    linked = defaultdict(list)
    for record in timeline.get("records", []):
        events = record.get("events", [])
        keys = {(event.get("owner"), event.get("envelope_id")) for event in events
                if event.get("stage") == "tx_queued"
                and event.get("owner") is not None and event.get("envelope_id") is not None}
        for key in keys:
            linked[key].append(record)

    owners = {(record["owner"], record["envelope_id"]): record
              for record in timeline.get("owner_records", [])}
    shared = {key for key, calls in linked.items() if key in owners
              and (len(calls) != 1 or (owners[key].get("batch_count") is not None
                                       and owners[key]["batch_count"] != len(calls)))}
    rows = []
    for field in ("handler_ns", "append_persist_ns", "append_wal_ns",
                  "control_queue_ns", "appender_queue_ns", "flusher_queue_ns"):
        values = [owners[key][field] for key in linked
                  if key in owners and owners[key].get(field) is not None]
        row = _row("owner_" + field, sum(values), len(values),
                   "distinct owner envelopes linked to retained coordinator calls",
                   "owner duration annotation; may overlap other stages",
                   "owner_handler_ns" if field != "handler_ns" else "tx_queued → completion_harvested")
        row["excluded_shared"] = 0
        rows.append(row)

    residuals = []
    for key, calls in linked.items():
        owner = owners.get(key)
        if owner is None or key in shared or owner.get("batch_count") != 1 or owner.get("handler_ns") is None:
            continue
        events = calls[0].get("events", [])
        queued = [event for event in events if event.get("stage") == "tx_queued"]
        harvested = [event for event in events if event.get("stage") == "completion_harvested"]
        if len(queued) != 1 or len(harvested) != 1:
            continue
        first, last = queued[0], harvested[0]
        if ((first.get("owner"), first.get("envelope_id")) != key
                or (last.get("owner"), last.get("envelope_id")) != key
                or first.get("elapsed_ns") is None or last.get("elapsed_ns") is None
                or last["elapsed_ns"] < first["elapsed_ns"]):
            continue
        residuals.append(last["elapsed_ns"] - first["elapsed_ns"] - owner["handler_ns"])
    row = _row("exchange minus owner handler (wire, both network stacks, TLS, dispatch)", sum(residuals), len(residuals),
               "one retained caller and batch_count=1 per linked owner envelope",
               "difference of local durations; assumes handler is inside exchange; not pure wire time",
               "tx_queued → completion_harvested", exactness="derived_duration_difference")
    row["excluded_shared"] = sum(len(linked[key]) for key in shared)
    rows.append(row)
    return rows


def write_profile(evidence):
    """Return only timings with explicit denominators from bounded write profiles."""
    rows = []
    selection = evidence.get("selection_policy", "bounded write profile")
    stages = evidence.get("stages", {})
    if evidence.get("runtime_observation_mode") == "runtime_selected_native_scalar_write_stages":
        selected = evidence.get("selected_operations", 0)
        for name in ("wal_append", "index_apply"):
            # Index is attempted only after a successful append.
            count = selected if name == "wal_append" else selected - stages.get("wal_append_errors", 0)
            rows.append(_row(name, stages.get(name + "_ns"), count, selection,
                             "native sequential scalar stage"))
        persistence = evidence.get("value_wal_persistence", {})
        if persistence.get("status") == "sampled":
            for name in ("file", "namespace"):
                rows.append(_row("barrier_" + name, persistence.get(name + "_ns"),
                                 persistence.get(name + "_calls", 0), selection,
                                 "nested persistence annotation; per file or namespace call", "barrier"))
        return rows
    live = stages.get("live_nvmeof", {})
    for key, total in live.items():
        if not key.endswith("_ns") or not isinstance(total, (int, float)):
            continue
        base = key[:-3]
        count_key = base + "_samples"
        if count_key in live:
            rows.append(_row("live_nvmeof_" + base, total, live[count_key], selection,
                             "measurement-window cumulative delta; independent overlapping populations"))
    interval = evidence.get("coordinator_interval") or {}
    interval_stages = interval.get("stages") or {}
    for key, total in interval_stages.items():
        if not key.endswith("_ns") or not isinstance(total, (int, float)):
            continue
        base = key[:-3]
        count = interval_stages.get(base + "_count")
        row = _row("coordinator_interval_" + base, total, count,
                   interval.get("scope", "measurement_boundary_cumulative_delta"),
                   "overlapping cumulative delta; background work may cross the measurement boundary")
        row["sum_ns"] = total
        rows.append(row)
    # Explicit denominators are required; counters with unrelated or unknown
    # populations are not silently divided by sampled_requests.
    for key, total in stages.items():
        if not key.endswith("_ns") or not isinstance(total, (int, float)):
            continue
        base = key[:-3]
        candidates = [base + "_count", base + "_calls", base + "_samples"]
        if base.startswith("write_profile_storage_append_"):
            candidates.append("write_profile_storage_append_calls")
        elif base.startswith("write_profile_storage_persist_"):
            candidates.append("write_profile_storage_persist_calls")
        elif base.startswith("write_profile_connection_"):
            candidates.append("write_profile_connection_requests")
        elif base.startswith("write_profile_client_") and not base.startswith("write_profile_client_transport_"):
            candidates.append("write_profile_client_requests")
        count_key = next ( (candidate for candidate in candidates if candidate in stages), None )
        if count_key is None:
            continue
        rows.append(_row(base.removeprefix("write_profile_"), total, stages[count_key], selection,
                         "inclusive overlapping owner/client annotation; denominator " + count_key))
    return rows


def _fit(points):
    if len(points) < 2:
        return None
    xbar = fmean(x for x, _ in points)
    ybar = fmean(y for _, y in points)
    variance = sum((x - xbar) ** 2 for x, _ in points)
    if not variance:
        return None
    slope = sum((x - xbar) * (y - ybar) for x, y in points) / variance
    return {"slope_us_per_put": slope, "intercept_us": ybar - slope * xbar, "count": len(points)}


def stall(samples, timeline):
    """Detect >5 ms gaps between successful measurement put completions.

    Cumulative puts count completions, not dispatches. The following completion's
    caller ID links its own coordinator intervals; benchmark and timeline clocks
    have different origins and are never subtracted or aligned.
    """
    completions = sorted(((int(row["completed_ns"]), row) for row in samples
                          if row.get("phase") == "measurement" and row.get("outcome") == "success"
                          and row.get("operation") == "put"), key=lambda item: item[0])
    intervals = defaultdict(list)
    for row in write_timeline(timeline):
        intervals[str(row["caller_sample_id"])].append(row)
    stalls = []
    for index in range(1, len(completions)):
        previous = completions[index - 1][0]
        now, sample = completions[index]
        gap = now - previous
        if gap <= 5_000_000:
            continue
        phases = intervals.get(str(sample.get("caller_sample_id", "")), [])
        longest = max(phases, key=lambda row: row["mean_us"], default=None)
        stalls.append({"cumulative_puts": index + 1, "start_ns": previous, "end_ns": now,
                       "duration_us": gap / 1000, "caller_sample_id": sample.get("caller_sample_id"),
                       "longest_phase": longest["stage"] if longest else None,
                       "longest_phase_us": longest["mean_us"] if longest else None})
    spacings = [b["cumulative_puts"] - a["cumulative_puts"] for a, b in zip(stalls, stalls[1:])]
    phase_counts = Counter(item["longest_phase"] for item in stalls if item["longest_phase"] is not None)
    return {"count": len(stalls), "stalls": stalls, "completion_count": len(completions),
            "spacing_puts": spacings, "mean_spacing_puts": fmean(spacings) if spacings else None,
            "median_spacing_puts": median(spacings) if spacings else None,
            "duration_fit": _fit([(item["cumulative_puts"], item["duration_us"]) for item in stalls]),
            "phase_counts": dict(phase_counts),
            "phase_scope": "longest interval of linked following completion; not proof of global stall cause"}
