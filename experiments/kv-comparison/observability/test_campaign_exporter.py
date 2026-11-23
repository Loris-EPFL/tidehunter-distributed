"""Consumer-visible identity, clock, counter and missing-data regressions."""
import copy
import hashlib
import io
import importlib.util
import json
import re
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import campaign_exporter as campaign


def job():
    return {"case_id": "0001-native-1024-read-concurrent-primary_rf1-r0", "backend": "native",
            "profile": "read-concurrent", "cohort": "primary_rf1", "value_size": 1024, "repeat": 0}


def timing(count, value):
    return {"sample_count": count, "p50": value, "p95": value, "p99": value,
            "p999": value, "max": value}


def metrics():
    return {"measurement_rows_complete": True, "elapsed_seconds": 2, "offered_calls": 12,
            "success_calls": 10, "success_calls_per_second": 5,
            "success_logical_keys_per_second": 5, "success_value_mib_per_second": 10 / 1024 / 2,
            "populations": [
                {"operation": "read", "outcome": "success", "count": 10,
                 "api_ns": timing(10, 2000), "response_ns": timing(10, 8000),
                 "scheduling_ns": timing(10, 6000)},
                {"operation": "read", "outcome": "rejected", "count": 2,
                 "api_ns": timing(0, None), "response_ns": timing(2, 4000),
                 "scheduling_ns": timing(0, None)}]}


def write_diagnostics():
    return {"schema_version": 1, "backend": "native", "phase": "measurement", "phase_attempted": True,
            "runtime_enabled": True, "runtime_observation_mode": "sampled_prefix",
            "selection": {"requested_limit": 8, "observed_samples": 3,
                          "selection_policy": "first N accepted scalar measurement writes",
                          "phase_exclusions": ["preload", "warmup", "qualification"]},
            "clock_scope": "independent nested clocks", "overlap_semantics": "not additive",
            "unsupported_stages": ["pure network time"], "error": None,
            "profile": {"selected_operations": 3,
                        "stages": {"wal_append_ns": 2000000000, "wal_append_errors": 0,
                                   "index_apply_ns": 300000000, "index_apply_errors": 1},
                        "value_wal_persistence": {"status": "sampled", "profiled_barriers": 3,
                                                  "file_calls": 3, "file_successes": 2,
                                                  "file_ns": 1000000000}}}

def live_write_diagnostics():
    value = write_diagnostics()
    value.update(backend="tidehunter_live_nvmeof", runtime_observation_mode="live_nvmeof_phase_counters")
    value["measurement_drained"] = True
    value["selection"].update(requested_limit=2, observed_samples=3,
                              selection_policy="completed I/O in armed measurement window")
    value["profile"] = {
        "runtime_observation_mode": "live_nvmeof_phase_counters",
        "scope": "measurement_nvme_io_cumulative_delta", "requested_limit": 2, "selected_operations": 3,
        "stages": {"live_nvmeof": {
            "completed_write_ops": 3, "completed_write_bytes": 12288,
            "completed_read_ops": 0, "metadata_requests": 4,
            "unknown_writes": 0, "errors": 0, "queue_ns": 3000000000, "queue_samples": 3,
            "cq_ns": 0, "cq_samples": 0}},
        "finalization_error": None}
    return value


def read_diagnostics():
    return {"schema_version": 1, "backend": "tidehunter_tcp", "phase": "measurement",
            "phase_attempted": True, "runtime_enabled": True,
            "runtime_observation_mode": "bounded_remote_read_requests",
            "selection": {"requested_limit": 8, "selected_operations": 3, "completed_operations": 2,
                          "selection_policy": "first N measurement reads",
                          "phase_exclusions": ["preload", "warmup", "qualification"]},
            "clock_scope": "independent clocks", "overlap_semantics": "not additive",
            "unsupported_stages": [], "error": None,
            "profile": {"stages": {
                "server_async": {"available": True, "sum_ns": 2000000000, "count": 2,
                                 "max_ns": None, "unavailable_reason": None},
                "server_nowait": {"available": False, "sum_ns": None, "count": 0,
                                  "max_ns": None, "unavailable_reason": "no samples"}},
                "counters": {"read_profile_server_async_count": 2},
                "coverage": {"selected_remote_requests": 3, "completed_remote_requests": 2,
                             "profiled_owner_responses": 2, "reason": "sampled"},
                "read_evidence": {}}}


def owner_trace():
    return {"truncated": True, "buffer_overruns": 7, "trace_coverage": {
        "event_count": 100, "first_timestamp_ns": 10_000_000_000,
        "last_timestamp_ns": 12_000_000_000, "duration_ns": 2_000_000_000,
        "scope": "captured_complete_trace_events", "clock": "mono"}}


class OwnerTraceTests(unittest.TestCase):
    def test_truncated_observed_rate_uses_coverage_not_case_window(self):
        measured = metrics() | {"elapsed_seconds": 100}
        text = campaign.terminal_metrics({
            "status": "completed", "metrics": measured,
            "owner_trace_evidence": [owner_trace() | {"owner": "node-0"}]}, {})
        suffix = '{owner="node-0",scope="captured_complete_trace_events",clock="mono"} '
        for name, value in (("captured_events", 100), ("covered_seconds", 2.0),
                            ("observed_events_per_second", 50.0), ("truncated", 1),
                            ("buffer_overruns", 7), ("coverage_available", 1)):
            self.assertIn("kv_owner_trace_" + name + suffix + str(value) + "\n", text)
        self.assertIn('kv_case_measurement_seconds{} 100\n', text)

    def test_missing_bad_or_zero_coverage_is_unavailable_without_fake_rate(self):
        valid = owner_trace()["trace_coverage"]
        broken = [None, {}, "bad"]
        for key, value in (
                ("event_count", True), ("event_count", -1), ("event_count", 1),
                ("event_count", 0), ("first_timestamp_ns", None),
                ("last_timestamp_ns", 9_000_000_000), ("duration_ns", 0),
                ("duration_ns", 2_000_000_001), ("duration_ns", 2e9),
                ("scope", "case"), ("clock", "wall")):
            broken.append(valid | {key: value})
        broken.extend([
            valid | {"first_timestamp_ns": 12_000_000_000, "duration_ns": 0},
            valid | {"event_count": 0, "first_timestamp_ns": None,
                     "last_timestamp_ns": None, "duration_ns": None}])
        for coverage in broken:
            with self.subTest(coverage=coverage):
                evidence = owner_trace() | {"owner": "node-0", "trace_coverage": coverage}
                text = campaign.owner_trace_metrics([evidence], {})
                self.assertNotIn("kv_owner_trace_observed_events_per_second", text)
                self.assertNotIn("kv_owner_trace_covered_seconds", text)
                self.assertRegex(text, r'kv_owner_trace_coverage_available\{[^\n]+\} 0\n')
                self.assertRegex(text, r'kv_owner_trace_truncated\{[^\n]+\} 1\n')
                self.assertRegex(text, r'kv_owner_trace_buffer_overruns\{[^\n]+\} 7\n')

    def test_monitor_artifact_hook_keeps_owner_identity_and_only_metadata(self):
        import campaign_monitor as monitor
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selected_job = job()
            result = root / "cases" / selected_job["case_id"] / "result"
            result.mkdir(parents=True)
            state = root / "observer"
            (state / "benchmark").mkdir(parents=True)
            selection_path = state / "benchmark/selection.json"
            selection_path.write_text(json.dumps({
                "job": selected_job, "results": str(result), "terminal": None}))
            (state / "session.json").write_text(json.dumps({
                "seen_case_ids": [], "terminal_observations": []}))
            for index in (0, 1):
                target = result.parent if index == 0 else result.parent / "owner-ftrace/node-1"
                target.mkdir(parents=True, exist_ok=True)
                artifact = owner_trace() | {"trace": "not selection data", "cpu_summary": {"busy": 99}}
                artifact["trace_coverage"]["event_count"] += index
                (target / "owner-ftrace.json").write_text(json.dumps(artifact))
            corrupt = result.parent / "owner-ftrace/node-2/owner-ftrace.json"
            corrupt.parent.mkdir(parents=True)
            corrupt.write_text('{"trace_coverage":')
            with patch.object(monitor, "validate_spec", side_effect=lambda spec: spec), \
                    patch.object(monitor, "state_for", return_value=(state, {})), \
                    patch.object(monitor, "validate_job"), \
                    patch.object(monitor, "result_path", return_value=result), \
                    patch.object(monitor, "budget", return_value=2.5), \
                    patch.object(monitor, "query", return_value={
                        "resultType": "vector", "result": [{"value": [1, "1"]}]}):
                monitor.finish_case({"run_id": "proof"}, selected_job, result)
            terminal = json.loads(selection_path.read_text())["terminal"]
            evidence = terminal["owner_trace_evidence"]
            self.assertEqual([item["owner"] for item in evidence], ["node-0", "node-1", "node-2"])
            self.assertEqual(evidence[2], {"owner": "node-2"})
            self.assertNotIn("trace", evidence[0])
            self.assertNotIn("cpu_summary", evidence[0])
            text = campaign.terminal_metrics(terminal, {})
            self.assertRegex(text, r'kv_owner_trace_observed_events_per_second\{owner="node-0",[^\n]+\} 50.0\n')
            self.assertRegex(text, r'kv_owner_trace_observed_events_per_second\{owner="node-1",[^\n]+\} 50.5\n')
            self.assertRegex(text, r'kv_owner_trace_coverage_available\{owner="node-2",[^\n]+\} 0\n')
            self.assertNotRegex(text, r'kv_owner_trace_observed_events_per_second\{owner="node-2",')
            for name in re.findall(r"^(kv_owner_trace_\w+)\{", text, re.M):
                self.assertIn(name, monitor.QUERY_METRICS)


class CaseIdentityTests(unittest.TestCase):
    def test_scopes_quoted_labels_without_splitting_their_commas_or_braces(self):
        source = b'# TYPE observed counter\nobserved{op="read,brace}quote\\\""} 7\n'
        text = campaign.scoped(source, {"run_id": "th-proof", "case_id": "case-1"}).decode()
        self.assertIn('observed{op="read,brace}quote\\\"",run_id="th-proof",case_id="case-1"} 7', text)
        self.assertEqual(text.count('# TYPE observed counter'), 1)

    def test_source_cannot_impersonate_a_different_case(self):
        with self.assertRaisesRegex(ValueError, "reserved context label"):
            campaign.scoped(b'counter{case_id="other"} 10\n', {"case_id": "owned"})

    def test_a_final_failed_snapshot_does_not_imply_measurement_zeros(self):
        text = campaign.terminal_metrics({"status": "failed", "metrics": None}, {"run_id": "th-proof"})
        self.assertIn('kv_observer_case_status{run_id="th-proof",status="failed"} 1', text)
        self.assertNotIn('kv_case_', text)

    def test_terminal_gauges_keep_rejection_denominators_and_undefined_clocks(self):
        text = campaign.terminal_metrics({"status": "completed", "metrics": metrics()}, {"run_id": "th-proof"})
        self.assertIn('kv_case_offered_calls{run_id="th-proof"} 12', text)
        self.assertIn('kv_case_rejected_calls{run_id="th-proof"} 2', text)
        self.assertIn('operation="read",outcome="success",quantile="0.99"} 2e-06', text)
        self.assertIn('kv_case_response_quantile_seconds{run_id="th-proof",operation="read",outcome="rejected"', text)
        self.assertNotIn('kv_case_api_quantile_seconds{run_id="th-proof",operation="read",outcome="rejected"', text)
        self.assertNotIn('kv_case_scheduling_quantile_seconds{run_id="th-proof",operation="read",outcome="rejected"', text)

    def test_inconsistent_accounting_is_rejected_before_success_gauges_are_returned(self):
        value = metrics()
        value['offered_calls'] = 13
        with self.assertRaisesRegex(ValueError, "accounting differs"):
            campaign.terminal_metrics({"status": "completed", "metrics": value}, {})

    def test_duplicate_latency_populations_cannot_be_double_counted(self):
        value = metrics()
        value['populations'].append(value['populations'][0])
        with self.assertRaisesRegex(ValueError, "duplicate operation/outcome"):
            campaign.terminal_metrics({"status": "completed", "metrics": value}, {})

    def test_untimed_population_cannot_claim_zero_latency(self):
        value = metrics()
        value['populations'][1]['api_ns']['p99'] = 0
        with self.assertRaisesRegex(ValueError, "untimed population"):
            campaign.terminal_metrics({"status": "completed", "metrics": value}, {})

    def test_profile_availability_is_not_inferred_from_a_missing_field(self):
        unknown = campaign.terminal_metrics({"status": "completed", "metrics": None}, {})
        self.assertNotIn('write_profile_enabled', unknown)
        disabled = campaign.terminal_metrics({"status": "completed", "metrics": None,
                                               "write_profile_enabled": False}, {})
        self.assertIn('kv_observer_write_profile_enabled{} 0', disabled)


    def test_case_path_cannot_relabel_another_case_result(self):
        with tempfile.TemporaryDirectory() as directory:
            config = {"run_id": "th-proof", "stale_seconds": 15}
            selection = {"job": job(), "results": str(Path(directory) / "different-case" / "result"),
                         "terminal": None}
            with self.assertRaisesRegex(ValueError, "does not belong"):
                campaign.benchmark_payload(config, selection)

    def test_lifetime_read_paths_keep_unavailable_owner_data_absent(self):
        evidence = {"coordinator": {"cache": {"hits": 7, "misses": 3}, "read_rpc_count": 3},
                    "owner_io": {"available": False, "resident_available": True,
                                 "owner_read_executor_stats": [{"partition": 0, "stats": {"wal_reads": 99}}],
                                 "resident_read_stats": [{"partition": 0, "stats": {
                                     "rma_hits": 2, "fallback_missing": 3, "rma_errors": 0}}]}}
        text = campaign.read_path_metrics(evidence, {"case_id": "owned"})
        self.assertIn('scope="adapter_lifetime",component="coordinator",path="cache_hits"} 7', text)
        self.assertIn('component="resident",partition="0",path="rma_hits"} 2', text)
        self.assertIn('component="owner"} 0', text)
        self.assertNotIn('path="wal_reads"', text)
        self.assertNotIn('hit_fraction', text)

    def test_duplicate_partition_cannot_double_count_read_paths(self):
        evidence = {"owner_io": {"available": True, "owner_read_executor_stats": [
            {"partition": 0, "stats": {"wal_reads": 3}},
            {"partition": 0, "stats": {"wal_reads": 4}}]}}
        with self.assertRaisesRegex(ValueError, "duplicated"):
            campaign.read_path_metrics(evidence, {})

    def test_cacheless_owner_boundary_keeps_absent_counters_absent(self):
        owner = {"available": True, "partitions": {"0": {
            "available": True, "counters": {"wal_reads": 3, "wal_read_bytes": 768,
                                             "resident_read_attempts": 3, "resident_read_successes": 1,
                                             "async_read_submissions": 2}}}}
        evidence = {"scope": "measurement_boundary", "deltas": {
            "coordinator": {"available": True, "counters": {"hits": 0}},
            "owner_executor": owner}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('component="owner_executor",partition="0",path="wal_read_bytes"} 768', text)
        self.assertIn('component="owner_executor",partition="0",path="resident_read_successes"} 1', text)
        self.assertNotIn("client_resident", text)
        self.assertNotIn("cache_misses", text)
        del owner["partitions"]["0"]["counters"]["async_read_submissions"]
        self.assertNotIn("async_read_submissions", campaign.measurement_read_metrics(evidence, {}))

    def test_requested_route_labels_do_not_invent_live_measurements(self):
        selected = job() | {"backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"}
        labels = campaign.context("run", selected)
        self.assertEqual(labels["requested_nvmeof_route"], "live")
        text = campaign.terminal_metrics({"status": "failed"}, labels)
        self.assertIn('requested_nvmeof_route="live"', text)
        self.assertNotIn("kv_nvmeof_reader_count", text)
        with self.assertRaisesRegex(ValueError, "invalid requested"):
            campaign.context("run", selected | {"nvmeof_route": "other"})

    def test_spdk_routes_have_distinct_labels_and_reject_invalid_combinations(self):
        live = job() | {"backend": "tidehunter_nvme_rdma", "nvmeof_route": "live",
                        "nvmeof_target": "spdk"}
        labels = campaign.context("run", live)
        self.assertEqual(labels["requested_backend"], "tidehunter_spdk_live_rdma")
        self.assertEqual(labels["requested_nvmeof_route"], "live")
        self.assertEqual(labels["requested_nvmeof_target"], "spdk")
        sealed = live | {"nvmeof_route": "sealed_read", "cohort": "sealed_read"}
        self.assertEqual(campaign.context("run", sealed)["requested_backend"], "tidehunter_spdk_rdma")
        for invalid in (live | {"backend": "tidehunter_nvme_tcp"},
                        live | {"backend": "native"},
                        live | {"nvmeof_route": None},
                        live | {"nvmeof_route": "other"},
                        live | {"cohort": "sealed_read"},
                        sealed | {"cohort": "primary_rf1"},
                        live | {"nvmeof_target": None},
                        live | {"nvmeof_target": "other"}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                campaign.context("run", invalid)

    def test_hybrid_put_route_is_admitted_without_weakening_other_jobs(self):
        hybrid = job() | {"backend": "tidehunter_nvme_rdma", "nvmeof_route": "sealed_read",
                          "cohort": "primary_rf1", "case_kind": "hybrid_put", "nvmeof_target": "kernel"}
        self.assertEqual(campaign.context("run", hybrid)["requested_nvmeof_route"], "sealed_read")
        for invalid in (hybrid | {"nvmeof_route": "live"},
                        hybrid | {"nvmeof_route": None, "nvmeof_target": None},
                        hybrid | {"cohort": "sealed_read"},
                        hybrid | {"backend": "tidehunter_nvme_tcp"},
                        hybrid | {"case_kind": "remote_put"}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                campaign.context("run", invalid)

    def test_live_measurement_deltas_are_scoped_and_missing_is_not_zero(self):
        fields = ("completed_read_ops", "completed_read_bytes", "completed_write_ops",
                  "completed_write_bytes", "recovery_read_ops", "recovery_read_bytes",
                  "metadata_requests", "namespace_connects",
                  "unknown_writes", "errors", "route_evidence_dropped", "fence_evidence_dropped")
        counters = {field: index for index, field in enumerate(fields)}
        for stage in ("queue", "submit", "cq", "control", "validation",
                      "publication", "namespace_setup", "read_io"):
            counters[stage + "_ns"] = 0
            counters[stage + "_samples"] = 0
        counters["queue_ns"], counters["queue_samples"] = 100, 2
        evidence = {"scope": "measurement_boundary", "deltas": {"available": False},
                    "live_nvmeof": {"available": True, "counters": counters}}
        labels = campaign.context("run", job() | {"backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"})
        text = campaign.measurement_read_metrics(evidence, labels)
        self.assertIn('scope="measurement_boundary",counter="completed_read_ops"} 0', text)
        self.assertIn('scope="measurement_boundary",counter="completed_read_bytes"} 1', text)
        self.assertIn("kv_live_nvmeof_bytes", text)
        self.assertIn("kv_live_nvmeof_events", text)
        self.assertIn('stage="queue",statistic="sum"} 1e-07', text)
        self.assertIn('stage="cq"} 0', text)
        self.assertNotIn('stage="cq",statistic="sum"', text)
        evidence["live_nvmeof"] = {"available": False}
        text = campaign.measurement_read_metrics(evidence, labels)
        self.assertIn('scope="measurement_boundary"} 0', text)
        self.assertNotIn("kv_live_nvmeof_events", text)
        evidence["live_nvmeof"] = {"available": True, "counters": {"errors": 0}}
        with self.assertRaisesRegex(ValueError, "incomplete live"):
            campaign.measurement_read_metrics(evidence, labels)

    def test_new_live_deltas_peaks_and_pool_keep_distinct_scopes(self):
        counters = {field: 0 for field in (
            "completed_read_ops", "completed_read_bytes", "completed_write_ops", "completed_write_bytes",
            "recovery_read_ops", "recovery_read_bytes", "metadata_requests", "namespace_connects",
            "unknown_writes", "errors", "route_evidence_dropped", "fence_evidence_dropped")}
        for stage in ("queue", "submit", "cq", "control", "validation", "publication",
                      "namespace_setup", "read_io"):
            counters.update({stage + "_ns": 0, stage + "_samples": 0})
        counters.update(lane_inflight_samples=2, lane_inflight_request_sum=3, grant_hits=4,
                        copy_bytes_avoided=4096, delivery_ns=200, delivery_samples=2)
        evidence = {"scope": "measurement_boundary", "deltas": {"available": False},
                    "live_nvmeof": {"available": True, "counters": counters},
                    "live_nvmeof_peaks": {"lane_inflight_request_max": {"at_start": 2, "at_end": 5},
                                           "lane_inflight_op_max": {"at_start": None, "at_end": None}},
                    "client_pool": {"available": True, "counters": {"connections_replaced": 1}}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('counter="grant_hits"} 4', text)
        self.assertIn('kv_live_nvmeof_bytes{scope="measurement_boundary",counter="copy_bytes_avoided"} 4096', text)
        self.assertIn('stage="delivery",statistic="sum"} 2e-07', text)
        self.assertIn('scope="node_lifetime",boundary="end",peak="lane_inflight_request_max"} 5', text)
        self.assertNotIn('peak="lane_inflight_op_max"', text)
        self.assertIn('counter="connections_replaced"} 1', text)
        counters.update(lease_exchanges=2, adoption_history_full_settles=1,
                        adoption_async_ns=0, adoption_async_samples=0,
                        adoption_settle_ns=30, adoption_settle_samples=3)
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('counter="lease_exchanges"} 2', text)
        self.assertNotIn('stage="adoption_async",statistic="sum"', text)
        self.assertIn('stage="adoption_settle",statistic="sum"} 3e-08', text)
        # Run 13 first launch: the engine exports owner lease/adoption profiles and lease-ahead counts.
        counters.update(lease_reserve_ns=60177392, lease_namespace_sync_ns=199, lease_publish_ns=2807,
                        lease_profile_samples=3, adopt_owner_ns=356584672, adopt_close_scan_ns=86328087,
                        adopt_close_scan_bytes=4096, adopt_profile_samples=93, extent_preleases=2,
                        extent_switch_waits=0, leased_worker_waits=20, leased_worker_wait_timeouts=1,
                        leased_flushes=3, leased_flush_ns=200, leased_flush_samples=3,
                        leased_flush_covered_frames=12, read_lane_pin_failures=0)
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('counter="extent_preleases"} 2', text)
        self.assertIn('counter="lease_reserve_ns"} 60177392', text)
        self.assertIn('kv_live_nvmeof_bytes{scope="measurement_boundary",counter="adopt_close_scan_bytes"} 4096', text)
        self.assertIn('counter="leased_worker_waits"} 20', text)
        self.assertIn('counter="leased_worker_wait_timeouts"} 1', text)
        for name, value in (('leased_flushes', 3), ('leased_flush_ns', 200),
                            ('leased_flush_samples', 3), ('leased_flush_covered_frames', 12),
                            ('read_lane_pin_failures', 0)):
            self.assertIn(f'counter="{name}"}} {value}', text)
        counters["unknown_leased_counter"] = 0
        with self.assertRaises(ValueError):
            campaign.measurement_read_metrics(evidence, {})
        del counters["unknown_leased_counter"]
        del counters["delivery_samples"]
        with self.assertRaisesRegex(ValueError, "incomplete live"):
            campaign.measurement_read_metrics(evidence, {})

    def test_live_counter_sets_match_the_rust_delta_and_peak_fixture(self):
        # kv-research pins the same fixture to its exported delta/peak lists and to the
        # serialized LiveNvmeStats fields, so either side changing alone fails CI.
        fixture = json.loads((Path(__file__).parent / "live_nvmeof_counter_fields.json").read_text())
        self.assertEqual(set(fixture["delta"]), campaign.live_nvmeof_allowed_counters())
        self.assertLessEqual(campaign.live_nvmeof_expected_counters(), set(fixture["delta"]))
        self.assertEqual(set(fixture["peak"]), set(campaign.LIVE_NVMEOF_PEAKS))
        self.assertFalse(set(fixture["delta"]) & set(fixture["peak"]))
        # A complete Rust delta is admitted, and each field lands in its unit family.
        counters = {field: 1 for field in fixture["delta"]}
        text = campaign.measurement_read_metrics(
            {"scope": "measurement_boundary", "deltas": {"available": False},
             "live_nvmeof": {"available": True, "counters": counters}}, {})
        for name in ("leased_frame_batches", "write_submit_calls", "owner_drains", "owner_drain_batches",
                     "owner_drain_stop_empty", "owner_drain_stop_allocated", "owner_drain_stop_fence",
                     "owner_drain_stop_limit", "owner_drain_stop_credit", "leased_submit_ready_left",
                     "leased_submit_ready_left_batches", "leased_submit_ready_left_full",
                     "leased_submit_ready_left_full_batches", "leased_deferred_submits"):
            self.assertIn(f'kv_live_nvmeof_events{{scope="measurement_boundary",counter="{name}"}} 1', text)

    def test_q0_counter_contract_has_exact_fields_and_measured_rates(self):
        fixture = json.loads((Path(__file__).parent / "q0_overhead_counter_fields.json").read_text())
        self.assertEqual(tuple(fixture), campaign.Q0_OVERHEAD_FIELDS)
        counters = dict(zip(fixture, (30, 6)))
        evidence = {"scope": "measurement_boundary", "deltas": {"available": False},
                    "q0_overhead": {"elapsed_ns": 2_000_000_000, "counts": counters}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('kv_q0_events_per_second{scope="measurement_boundary",event="live_frontier_installs"} 15', text)
        self.assertIn('kv_q0_events_per_second{scope="measurement_boundary",event="durable_cut_nudges"} 3', text)
        counters["extra"] = 1
        with self.assertRaisesRegex(ValueError, "invalid Q0 overhead counter"):
            campaign.measurement_read_metrics(evidence, {})

    def test_new_transport_observations_do_not_require_cq_timing(self):
        evidence = read_diagnostics()
        counters = evidence["profile"]["counters"]
        counters.update(read_profile_transport_progress_available=1,
                        read_profile_transport_send_size_le_4k=7,
                        read_profile_transport_send_post_eagain=2,
                        read_profile_transport_send_post_retries=1,
                        read_profile_transport_cq_idle_waits=4,
                        read_profile_admission_retry_credit=3)
        text = campaign.read_diagnostic_metrics(evidence, {})
        for field, value in (("send_size_le_4k", 7), ("send_post_eagain", 2),
                             ("send_post_retries", 1), ("cq_idle_waits", 4)):
            self.assertIn(f'counter="{field}"}} {value}', text)
        self.assertIn('counter="read_profile_admission_retry_credit"} 3', text)
        counters["read_profile_transport_progress_available"] = 0
        self.assertNotIn('counter="send_post_eagain"', campaign.read_diagnostic_metrics(evidence, {}))

    def test_sealed_reader_new_fields_are_lifetime_snapshots(self):
        reader = {"partition": 1, "admitted_sqes": 8, "merged_members": 3,
                  "pool_hits": 4, "lock_wait": {"count": 0, "sum_ns": 0, "max_ns": 0},
                  "lock_hold": {"count": 2, "sum_ns": 100, "max_ns": 60}}
        evidence = {"scope": "measurement_boundary", "deltas": {"available": False},
                    "end": {"nvmeof": {"available": True, "readers": [reader]}}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('scope="node_lifetime",boundary="end",reader="0",partition="1",counter="merged_members"} 3', text)
        self.assertIn('stage="lock_hold",statistic="max"} 6e-08', text)
        self.assertNotIn('stage="lock_wait",statistic="sum"', text)
        self.assertNotIn('counter="pool_misses"', text)
        reader["lock_wait"]["sum_ns"] = 1
        with self.assertRaisesRegex(ValueError, "without samples"):
            campaign.measurement_read_metrics(evidence, {})

    def test_live_write_new_counters_are_exported_without_inventing_clocks(self):
        evidence = live_write_diagnostics()
        counters = evidence["profile"]["stages"]["live_nvmeof"]
        counters.update(grant_hits=2, copy_bytes_avoided=128, controller_connect_ns=0,
                        controller_connect_samples=0, delivery_ns=9, delivery_samples=1)
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('counter="grant_hits"} 2', text)
        self.assertIn('counter="copy_bytes_avoided"} 128', text)
        self.assertIn('stage="delivery_ns"} 9e-09', text)
        self.assertNotIn('stage="controller_connect_ns"', text)

    def test_live_write_profile_fields_and_shared_clocks_match_rust_contract(self):
        expected = json.loads(Path(__file__).with_name("live_nvmeof_write_profile_fields.json").read_text())
        self.assertEqual(sorted(campaign.LIVE_NVME_WRITE_COUNTERS), expected)
        evidence = live_write_diagnostics()
        counters = dict.fromkeys(expected, 0) | evidence["profile"]["stages"]["live_nvmeof"]
        evidence["profile"]["stages"]["live_nvmeof"] = counters
        clocks = {"lease_reserve_ns": 125_000_000, "lease_namespace_sync_ns": 250_000_000,
                  "lease_publish_ns": 375_000_000, "adopt_owner_ns": 500_000_000,
                  "adopt_close_scan_ns": 625_000_000}
        events = {"lease_profile_samples": 2, "adopt_profile_samples": 3,
                  "extent_preleases": 4, "extent_switch_waits": 5}
        counters.update(clocks | events | {"adopt_close_scan_bytes": 8_192})
        text = campaign.write_diagnostic_metrics(evidence, {})
        labels = ('phase="measurement",scope="measurement_nvme_io_cumulative_delta",'
                  'diagnostic_backend="tidehunter_live_nvmeof",component="stages.live_nvmeof"')
        for name, value in clocks.items():
            self.assertIn(f'kv_case_write_diagnostic_stage_seconds{{{labels},stage="{name}"}} {value / 1e9}\n', text)
        for name, value in events.items():
            self.assertIn(f'kv_case_write_diagnostic_stage_count{{{labels},counter="{name}"}} {value}\n', text)
        self.assertIn(f'kv_live_nvmeof_bytes{{{labels},counter="adopt_close_scan_bytes"}} 8192\n', text)
        self.assertNotIn(f'kv_case_write_diagnostic_stage_count{{{labels},counter="adopt_close_scan_bytes"}}', text)
        counters["lease_profile_samples"] = 0
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertNotIn('stage="lease_reserve_ns"', text)
        self.assertIn('stage="adopt_owner_ns"', text)
        for missing in ("lease_profile_samples", "adopt_close_scan_ns"):
            with self.subTest(missing=missing):
                previous = counters.pop(missing)
                with self.assertRaisesRegex(ValueError, "lacks its clock|lacks its sample count"):
                    campaign.write_diagnostic_metrics(evidence, {})
                counters[missing] = previous

    def test_leased_fields_are_strict_and_zero_stages_unavailable(self):
        evidence = live_write_diagnostics()
        counters = evidence["profile"]["stages"]["live_nvmeof"]
        counters.update(lease_exchanges=2, leased_frames=3, adoption_history_full_settles=1,
                        adoption_async_ns=0, adoption_async_samples=0,
                        adoption_settle_ns=20, adoption_settle_samples=2)
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('counter="lease_exchanges"} 2', text)
        self.assertNotIn('stage="adoption_async_ns"', text)
        self.assertIn('stage="adoption_settle_ns"} 2e-08', text)
        counters["unrecognized"] = 0
        with self.assertRaises(ValueError):
            campaign.write_diagnostic_metrics(evidence, {})
        del counters["unrecognized"]
        del counters["adoption_async_samples"]
        with self.assertRaises(ValueError):
            campaign.write_diagnostic_metrics(evidence, {})

    def test_nvme_reader_exports_transport_and_unavailable_stages(self):
        row = {"available": True, "identity": {"partition": 0, "transport": "rdma"},
               "counters": {"available": True, "counters": {"aligned_read_bytes": 4096}},
               "stages": {"queue": {"delta": {"available": True, "counters": {
                   "count": 2, "sum_ns": 100}}},
                          "submit_to_cq": {"delta": {"available": False}}}}
        evidence = {"scope": "measurement_boundary", "deltas": {
            "coordinator": {"available": False}, "owner_executor": {"available": False}},
            "nvmeof": {"available": True, "readers": {"identity": row}}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('transport="rdma",counter="aligned_read_bytes"} 4096', text)
        self.assertIn('transport="rdma",stage="submit_to_cq"} 0', text)
        self.assertNotIn('stage="submit_to_cq",statistic="sum"', text)
        row["identity"]["transport"] = None
        self.assertIn('transport="unknown"', campaign.measurement_read_metrics(evidence, {}))
        evidence["deltas"] = {"available": False}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('component="measurement"} 0', text)
        self.assertIn('transport="unknown",counter="aligned_read_bytes"} 4096', text)
        evidence["nvmeof"] = {"available": False}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('kv_nvmeof_reader_evidence_available{scope="measurement_boundary"} 0', text)

    def test_malformed_label_separator_is_not_emitted_to_prometheus(self):
        with self.assertRaisesRegex(ValueError, "trailing"):
            campaign.scoped(b'observed{op="read",} 7\n', {"case_id": "owned"})


    def test_terminal_resource_gauges_preserve_observed_zero_and_missing_counters(self):
        evidence = {"schema_version": 1, "scope": "measurement_boundary_interval", "processes": [
            {"host_index": -1, "process_role": "client", "available": True, "elapsed_seconds": 1.2,
             "cpu_seconds": 0.75, "cpu_throttled_seconds": 0, "cpu_periods": 10,
             "cpu_throttled_periods": 0, "io_read_bytes": 8192},
            {"host_index": 0, "process_role": "storage", "available": False, "reason": "cgroup replaced"}]}
        text = campaign.terminal_metrics({"status": "completed", "metrics": None, "resource_evidence": evidence}, {})
        self.assertIn('kv_case_resource_interval_cpu_throttled_seconds{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 0', text)
        self.assertIn('kv_case_resource_interval_available{scope="measurement_boundary_interval",resource_host_index="0",process_role="storage"} 0', text)
        self.assertNotIn("io_write_bytes", text)
        self.assertNotIn("memory_peak_bytes", text)
        self.assertNotIn("cgroup replaced", text)
        self.assertNotIn("_total", text)

    def test_resource_absence_is_not_an_available_zero_interval(self):
        terminal = {"status": "failed", "metrics": None}
        self.assertNotIn("resource_interval", campaign.terminal_metrics(terminal, {}))
        terminal["resource_evidence"] = {"schema_version": 1, "scope": "measurement_boundary_interval", "processes": [
            {"host_index": -1, "process_role": "client", "available": False, "reason": "cpu counter reset"}]}
        text = campaign.terminal_metrics(terminal, {})
        self.assertIn('resource_interval_available{', text)
        self.assertNotIn('resource_interval_elapsed_seconds', text)
        self.assertNotIn('resource_interval_cpu_seconds', text)

    def test_resource_partial_core_exports_without_claiming_io(self):
        row = {"host_index": -1, "process_role": "client", "available": False,
               "core_available": True, "reason": "ambiguous IO", "elapsed_seconds": 2,
               "cpu_seconds": .7, "cpu_periods": 4, "cpu_throttled_periods": 1,
               "memory_current_bytes": 123}
        evidence = {"schema_version": 1, "scope": "measurement_boundary_interval", "processes": [row]}
        text = campaign.resource_interval_metrics(evidence, {})
        self.assertIn('kv_case_resource_interval_available{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 0', text)
        self.assertIn('kv_case_resource_interval_core_available{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 1', text)
        self.assertIn('kv_case_resource_interval_io_available{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 0', text)
        self.assertIn('kv_case_resource_interval_cpu_seconds{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 0.7', text)
        self.assertIn('kv_case_resource_interval_memory_current_bytes{scope="measurement_boundary_interval",resource_host_index="-1",process_role="client"} 123', text)
        self.assertNotIn("io_read_bytes", text)
        self.assertNotIn("io_write_bytes", text)
        for change in ({"io_read_bytes": 0}, {"io_write_bytes": 1}, {"cpu_seconds": -1},
                       {"cpu_seconds": float("nan")}, {"elapsed_seconds": 0},
                       {"cpu_throttled_periods": 5}, {"host_index": True}, {"extra": 1},
                       {"core_available": False}, {"core_available": 1}, {"reason": ""}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.resource_interval_metrics(evidence | {"processes": [row | change]}, {})
        for missing in ("cpu_seconds", "elapsed_seconds", "reason"):
            broken = row.copy()
            del broken[missing]
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                campaign.resource_interval_metrics(evidence | {"processes": [broken]}, {})

    def test_captured_device_change_exports_qualified_core_only(self):
        directory = Path(__file__).resolve().parent.parent
        spec = importlib.util.spec_from_file_location("resource_accounting", directory / "resource_accounting.py")
        accounting = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(accounting)
        before, after = json.loads((directory / "resource-client-boundaries-0024.json").read_text())
        evidence = accounting.interval([before], [after])
        text = campaign.resource_interval_metrics(evidence, {})
        self.assertIn("kv_case_resource_interval_core_available", text)
        self.assertIn("kv_case_resource_interval_io_available", text)
        self.assertIn("0.670104", text)
        self.assertIn("19963904", text)
        self.assertNotIn("io_read_bytes", text)
        self.assertNotIn("io_write_bytes", text)

    def test_resource_rejects_reset_impossible_or_unavailable_counter_claims(self):
        valid = {"host_index": -1, "process_role": "client", "available": True, "elapsed_seconds": 1,
                 "cpu_periods": 5, "cpu_throttled_periods": 2}
        for change in ({"cpu_seconds": -1}, {"elapsed_seconds": 0}, {"elapsed_seconds": float("nan")},
                       {"cpu_throttled_periods": 6}, {"io_read_bytes": 1.5}, {"host_index": True},
                       {"available": False, "reason": "reset"}, {"available": True, "reason": "not allowed"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.resource_interval_metrics({"schema_version": 1, "scope": "measurement_boundary_interval",
                                                     "processes": [valid | change]}, {})
        for rows in ([valid, valid], [valid] * 17):
            with self.assertRaises(ValueError):
                campaign.resource_interval_metrics({"schema_version": 1, "scope": "measurement_boundary_interval",
                                                     "processes": rows}, {})

    def test_cold_device_identity_zero_unknown_and_fallback_are_distinct(self):
        distribution = {"count": 2, "sum": 300, "max": 200, "mean": 150,
                        "population_stddev": 50, "p50": 100, "p95": 200,
                        "p99": 200, "p999": 200, "tail_warnings": ["small population"]}
        identity = {"available": True, "backend": "bpftrace-request-identity",
                    "qualified": True, "truncated": False, "lost": 0,
                    "read_issues": 2, "read_completions": 2, "matched_reads": 2,
                    "intervals_ns": distribution, "timestamp_unit_ns": 1,
                    "observed_timestamp_resolution_ns": None}
        fallback = {"available": True, "qualified": False, "truncated": False, "lost": None,
                    "read_issues": 3, "read_completions": 3, "matched_reads": None,
                    "intervals_ns": None, "candidate_intervals_ns": distribution}
        rows = [{"host_index": 0, "evidence": identity},
                {"host_index": 1, "evidence": fallback},
                {"host_index": 2, "evidence": {"available": False, "reason": "not attached"}}]
        text = campaign.terminal_metrics({"status": "completed", "metrics": None,
                                          "cold_device_evidence": rows}, {})
        self.assertIn('resource_host_index="0",trace_backend="request_identity",statistic="p99"} 2e-07', text)
        self.assertIn('resource_host_index="1",trace_backend="tracefs_candidate",counter="lost"} 0', text)
        self.assertIn('resource_host_index="2",trace_backend="unknown"} 0', text)
        self.assertNotIn('trace_backend="tracefs_candidate",statistic=', text)
        self.assertNotIn('precision="observed_timestamp_resolution_ns"} 1e-09', text)
        empty = distribution | {"count": 0, "sum": 0, "max": None, "mean": None,
                                "population_stddev": None, "p50": None, "p95": None, "p99": None, "p999": None}
        text = campaign.cold_device_metrics([{"host_index": -1, "evidence": identity | {
            "read_issues": 0, "read_completions": 0, "matched_reads": 0, "intervals_ns": empty,
            "qualified": False}}], {})
        self.assertIn('kv_case_block_interval_available{', text)
        self.assertNotIn('kv_case_block_interval_seconds', text)
        self.assertIn('counter="read_issues"} 0', text)
        for change in ({"lost": None}, {"lost": 1}, {"truncated": True}):
            text = campaign.cold_device_metrics([{"host_index": 0, "evidence": identity | change}], {})
            self.assertNotIn("kv_case_block_interval_seconds", text)
        with self.assertRaises(ValueError):
            campaign.cold_device_metrics([{"host_index": 1, "evidence": fallback | {"intervals_ns": distribution}}], {})
        with self.assertRaises(ValueError):
            campaign.cold_device_metrics(rows + [rows[0]], {})
        with self.assertRaises(ValueError):
            campaign.cold_device_metrics([{"host_index": 0, "evidence": identity | {"raw_events": []}}], {})

    def test_read_diagnostics_export_separate_observed_populations(self):
        text = campaign.terminal_metrics({"status": "completed", "metrics": None,
                                          "read_diagnostics": read_diagnostics(),
                                          "read_profile_enabled": True}, {})
        self.assertIn('kv_observer_read_profile_enabled{} 1', text)
        self.assertIn('population="selected_operations"} 3', text)
        self.assertIn('population="completed_operations"} 2', text)
        self.assertIn('clock_domain="owner",stage="server_async"', text)
        self.assertIn('stage="server_async",statistic="sum"} 2.0', text)
        self.assertIn('counter="server_async"} 2', text)
        self.assertNotIn('statistic="max"', text)
        self.assertNotIn("server_nowait", text)
        self.assertNotIn("kv_case_write_diagnostic", text)
        evidence = read_diagnostics()
        evidence["profile"]["coverage"]["profiled_owner_responses"] = 5
        self.assertIn('population="profiled_owner_responses"} 5',
                      campaign.read_diagnostic_metrics(evidence, {}))
        self.assertNotIn("quantile", text)

    def test_nvme_coverage_rejects_impossible_counts_and_does_not_fill_historical_gaps(self):
        evidence = read_diagnostics()
        self.assertNotIn('population="nvme_completed"', campaign.read_diagnostic_metrics(evidence, {}))
        for completed in (0, 2):
            with self.subTest(completed=completed):
                evidence["profile"]["coverage"]["nvme_completed"] = completed
                text = campaign.read_diagnostic_metrics(evidence, {})
                self.assertIn(f'population="nvme_completed"}} {completed}', text)
        for invalid in (True, -1, 1.5, 3):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                evidence["profile"]["coverage"]["nvme_completed"] = invalid
                campaign.read_diagnostic_metrics(evidence, {})
        evidence["profile"]["coverage"]["nvme_completed"] = 0
        evidence["profile"]["coverage"]["unobserved_population"] = 0
        with self.assertRaisesRegex(ValueError, "invalid read diagnostic coverage"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_read_evidence_accepts_fractional_means_but_rejects_nonfinite_measurements(self):
        evidence = read_diagnostics()
        stage = {"mean_ns": 3095.6875, "count": 32, "sum_ns": 99062, "max_ns": None}
        evidence["profile"]["read_evidence"] = {
            "nvmeof": {"readers": {"reader": {"stages": {"admission": stage}}}}}
        campaign.read_diagnostic_metrics(evidence, {})
        for invalid in (-1.0, float("nan"), float("inf"), -float("inf")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                stage["mean_ns"] = invalid
                campaign.read_diagnostic_metrics(evidence, {})
        stage["mean_ns"] = 3095.6875
        evidence["profile"]["counters"]["read_profile_server_async_count"] = 2.5
        with self.assertRaisesRegex(ValueError, "expected a nonnegative count"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_retired_read_retry_counters_remain_admissible_without_exporting_them(self):
        evidence = read_diagnostics()
        evidence["profile"]["counters"].update(
            read_profile_admission_retry_credit=1,
            read_profile_admission_retry_config=2,
            read_profile_admission_retry_mixed=3)
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('counter="read_profile_admission_retry_credit"', text)
        self.assertNotIn("read_profile_admission_retry_config", text)
        self.assertNotIn("read_profile_admission_retry_mixed", text)

    def test_read_diagnostics_no_data_and_validation(self):
        self.assertEqual(campaign.read_diagnostic_metrics(None, {}), "")
        evidence = read_diagnostics()
        for changes in ({"runtime_enabled": False}, {"profile": None}, {"error": "unavailable"}):
            text = campaign.read_diagnostic_metrics(evidence | changes, {})
            self.assertIn('diagnostic_backend="tidehunter_tcp",partition="aggregate"} 0', text)
            self.assertNotIn("diagnostic_stage_seconds", text)
            self.assertNotIn("diagnostic_stage_count", text)
        evidence["profile"]["stages"]["server_async"] = {
            "available": False, "sum_ns": None, "count": 0, "max_ns": None, "unavailable_reason": "no samples"}
        self.assertNotIn("diagnostic_stage_seconds", campaign.read_diagnostic_metrics(evidence, {}))
        for changes in ({"phase": "warmup"}, {"schema_version": True},
                        {"runtime_enabled": 1}, {"clock_scope": ""}, {"overlap_semantics": "x" * 65537}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                campaign.read_diagnostic_metrics(read_diagnostics() | changes, {})
        evidence = read_diagnostics()
        evidence["selection"]["completed_operations"] = 4
        with self.assertRaises(ValueError):
            campaign.read_diagnostic_metrics(evidence, {})
        evidence = read_diagnostics()
        evidence["profile"]["stages"]["server_async"]["count"] = 0
        with self.assertRaises(ValueError):
            campaign.read_diagnostic_metrics(evidence, {})
        evidence = read_diagnostics()
        evidence["selection"]["selected_operations"] = 0
        evidence["selection"]["completed_operations"] = 0
        with self.assertRaisesRegex(ValueError, "require selected operations"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_read_diagnostics_preserve_per_owner_scopes_and_validate_partition(self):
        evidence = read_diagnostics()
        stage = {"available": True, "sum_ns": 500000000, "count": 1,
                 "max_ns": 500000000, "unavailable_reason": None}
        absent = {"available": False, "sum_ns": None, "count": 0,
                  "max_ns": None, "unavailable_reason": "no observations"}
        evidence["profile"]["owners"] = {
            "0": {"stages": {"server_async": stage}, "counters": {"read_profile_completed": 1}},
            "4": {"stages": {"server_async": absent}, "counters": {"read_profile_completed": 0}}}
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('partition="aggregate",clock_domain="owner",stage="server_async",statistic="sum"} 2.0', text)
        self.assertIn('partition="0",clock_domain="owner",stage="server_async",statistic="sum"} 0.5', text)
        self.assertIn('partition="0",clock_domain="owner",stage="server_async",statistic="max"} 0.5', text)
        self.assertIn('partition="aggregate",clock_domain="owner",stage="server_async",statistic="mean"} 1.0', text)
        self.assertIn('partition="0",clock_domain="owner",stage="server_async",statistic="mean"} 0.5', text)
        self.assertIn('partition="4"} 0', text)
        self.assertIn('partition="4",population="read_profile_completed"} 0', text)
        evidence["profile"]["owners"]["0"]["stages"]["client_grant_install"] = stage
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('partition="0",clock_domain="coordinator",stage="client_grant_install"', text)
        self.assertNotIn('partition="4",clock_domain=', text)
        for owners in ({"aggregate": evidence["profile"]["owners"]["0"]},
                       {"01": evidence["profile"]["owners"]["0"]},
                       {"4294967296": evidence["profile"]["owners"]["0"]},
                       {str(i): evidence["profile"]["owners"]["0"] for i in range(6)}):
            with self.subTest(owners=owners), self.assertRaises(ValueError):
                campaign.read_diagnostic_metrics(evidence | {
                    "profile": evidence["profile"] | {"owners": owners}}, {})

    def test_errored_run_keeps_per_owner_populations(self):
        # The Exact-era gate also read `profile.coordinator_resolution_valid`, which the
        # tree no longer writes. A run with an error record must still emit the owner counters.
        def errored():
            evidence = read_diagnostics()
            evidence["error"] = "one selected remote flight failed"
            evidence["profile"]["counters"]["read_profile_errors"] = 1
            evidence["profile"]["owners"] = {"0": {"stages": {}, "counters": {
                "read_profile_completed": 1, "read_profile_errors": 1}}}
            return evidence
        evidence = errored()
        self.assertNotIn("coordinator_resolution_valid", evidence["profile"])
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('partition="0",population="read_profile_errors"} 1', text)
        self.assertIn('partition="0",population="read_profile_completed"} 1', text)
        self.assertIn('partition="aggregate",population="read_profile_errors"} 1', text)
        self.assertIn('partition="0"} 0', text)  # availability stays 0 for an errored run
        # No data without a run, a runtime selection or a profile.
        for changes in ({"runtime_enabled": False}, {"phase_attempted": False}):
            with self.subTest(changes=changes):
                text = campaign.read_diagnostic_metrics(errored() | changes, {})
                self.assertNotIn('population="read_profile_errors"', text)
        text = campaign.read_diagnostic_metrics(errored() | {"profile": None}, {})
        self.assertNotIn('population="read_profile_errors"', text)
        # A clean run is unchanged.
        clean = errored() | {"error": None}
        self.assertIn('partition="0",population="read_profile_errors"} 1',
                      campaign.read_diagnostic_metrics(clean, {}))

    def test_resolution_is_an_independent_population_with_unavailable_stages(self):
        evidence = read_diagnostics()
        evidence["selection"]["selected_operations"] = 0
        evidence["selection"]["completed_operations"] = 0
        evidence["profile"]["coverage"].update(selected_remote_requests=0, completed_remote_requests=0,
                                                profiled_owner_responses=0)
        evidence["profile"]["stages"] = {}
        absent = {"available": False, "sum_ns": None, "count": 0,
                  "max_ns": None, "unavailable_reason": "no observations"}
        observed = {"available": True, "sum_ns": 100, "count": 2, "max_ns": 70,
                    "unavailable_reason": None}
        evidence["profile"]["coordinator_resolution"] = {
            "selection_policy": "independent first N", "selected": 2, "point": 1, "bulk": 1,
            "errors": 0, "logical_members": 3, "cache_hits": 1, "index_misses": 1,
            "remote_misses": 1, "member_errors": 0,
            "stages": {"index_lock_wait": observed, "index_lookup": observed,
                       "cache_lock_wait": absent, "cache_lookup": absent,
                       "cut_validation": observed, "cut_capture": absent, "route_preparation": observed,
                       "pin_preparation": observed, "batch_preparation": absent}}
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('scope="api_resolution_prefix"', text)
        self.assertIn('population="logical_members"} 3', text)
        self.assertIn('stage="index_lookup",statistic="max"} 7e-08', text)
        self.assertIn('stage="cache_lookup"} 0', text)
        self.assertNotIn('stage="cache_lookup",statistic=', text)
        self.assertNotIn('stage="batch_wait"', text)
        stages = evidence["profile"]["coordinator_resolution"]["stages"]
        stages.update(batch_wait={"available": True, "sum_ns": 500, "count": 1,
                                  "max_ns": 500, "unavailable_reason": None},
                      postread_cache_fill=absent, result_materialization=absent)
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('stage="batch_wait",statistic="mean"} 5e-07', text)
        self.assertIn('stage="postread_cache_fill"} 0', text)
        self.assertNotIn('stage="postread_cache_fill",statistic=', text)
        self.assertNotIn('stage="result_materialization",statistic=', text)
        stages["unbounded_stage_name"] = observed
        with self.assertRaisesRegex(ValueError, "resolution profile"):
            campaign.read_diagnostic_metrics(evidence, {})
        del stages["unbounded_stage_name"]
        evidence["error"] = "one selected remote flight failed"
        evidence["profile"]["coordinator_resolution_valid"] = True
        evidence["profile"]["coordinator_resolution"]["errors"] = 1
        evidence["profile"]["counters"]["read_profile_failed_owner_profile_not_applicable"] = 1
        evidence["profile"]["owners"] = {"0": {"stages": {}, "counters": {
            "read_profile_failed_owner_profile_not_applicable": 1}}}
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('scope="api_resolution_prefix"', text)
        self.assertIn('population="errors"} 1', text)
        self.assertIn('stage="index_lookup",statistic="max"} 7e-08', text)
        self.assertIn('partition="0",population="read_profile_failed_owner_profile_not_applicable"} 1', text)
        self.assertIn('partition="aggregate",population="read_profile_failed_owner_profile_not_applicable"} 1', text)
        evidence["profile"]["coordinator_resolution_valid"] = False
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertNotIn('population="logical_members"} 3', text)
        evidence["profile"]["coordinator_resolution"]["bulk"] = 2
        with self.assertRaisesRegex(ValueError, "resolution populations"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_node_boundaries_keep_missing_and_lifetime_maxima_distinct(self):
        names = ("read_response_encode", "read_output_queue", "read_output_submit",
                 "read_response_retire", "read_grant_lookup", "read_grant_reservation",
                 "read_grant_envelope", "read_grant_registration", "read_grant_recheck_publication")
        missing = {"available": False, "error": "missing counter"}
        stages = {name: missing for name in names}
        stages["read_grant_registration"] = {"available": True,
                                              "counters": {"ns": 20, "count": 1, "errors": 1}}
        unavailable = {"available": False}
        evidence = {"scope": "measurement_boundary", "deltas": {
            "coordinator": unavailable, "owner_executor": unavailable, "client_resident": unavailable,
            "owner_response": {"available": True, "partitions": {"0": stages}}},
            "end": {"available": True, "coordinator": {"retained_bytes": 64},
                    "owner_grants": {"available": True, "partitions": {"0": {
                        "read_grant": {"registration": {"ns": 50, "count": 2, "errors": 1, "max_ns": 30}}}}}}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('stage="read_grant_registration",counter="errors"} 1', text)
        self.assertIn('stage="read_output_queue"} 0', text)
        self.assertIn('scope="node_lifetime_end"', text)
        self.assertIn('statistic="max"} 3e-08', text)
        self.assertIn('boundary="end",component="coordinator",partition="aggregate",gauge="retained_bytes"} 64', text)
        self.assertNotIn('stage="read_output_queue",statistic=', text)

    def test_lane_evidence_keeps_unknown_sessions_and_counter_scopes(self):
        missing = {"available": False}
        evidence = {"scope": "measurement_boundary", "deltas": {
            "coordinator": missing, "owner_executor": missing, "client_resident": missing,
            "client_resident_lanes": {"available": True, "partitions": {"0": {
                "available": True, "lanes": {"0": {"available": True, "counters": {"rma_hits": 4}},
                                               "1": {"available": False}}}}}},
            "start": {"client_resident_lanes": {"available": True, "partitions": {"0": {"lanes": {
                "0": {"session_present": True}, "1": {"session_present": None}}}}}}}
        text = campaign.measurement_read_metrics(evidence, {})
        self.assertIn('component="client_resident_lane",partition="0",lane="0",path="rma_hits"} 4', text)
        self.assertIn('boundary="start",partition="0",lane="1"} 0', text)
        self.assertNotIn('kv_case_read_lane_session_present{scope="measurement_boundary",boundary="start",partition="0",lane="1"}', text)
        evidence["deltas"]["client_resident_lanes"]["partitions"]["0"]["lanes"]["8"] = {"available": False}
        with self.assertRaises(ValueError):
            campaign.measurement_read_metrics(evidence, {})

    def test_cache_policy_does_not_invent_effective_receipt(self):
        requested = {"coordinator_cache_bytes": 0, "page_cache_policy": "not_dropped",
                     "warmup_operations": 10, "read_lanes": 1, "concurrency": 1}
        text = campaign.read_tier_metrics({"read_tier": {"requested": requested, "effective": None}}, {})
        self.assertIn('policy_scope="effective"} 0', text)
        self.assertIn('component="page_cache_policy",policy="not_dropped"} 1', text)
        self.assertNotIn("owner_read_cache_bytes", text)
        self.assertNotIn("registered_policy", text)
        effective = requested | {"page_cache_policy": None}
        text = campaign.read_tier_metrics({"read_tier": {"requested": requested, "effective": None}},
                                          {}, effective)
        self.assertIn('policy_scope="effective",setting="page_cache_policy"} 0', text)
        self.assertNotIn('policy_scope="effective",component="page_cache_policy"', text)
        with self.assertRaises(ValueError):
            campaign.read_tier_metrics({"read_tier": {"requested": requested | {"registered_policy": "unknown"}}}, {})

    def test_unavailable_read_stages_reject_claimed_observations(self):
        unavailable = {"available": False, "sum_ns": None, "count": 0,
                       "max_ns": None, "unavailable_reason": "no observations"}
        for change in ({"count": None}, {"count": 1}, {"sum_ns": 0}, {"max_ns": 0},
                       {"unavailable_reason": None}, {"unavailable_reason": ""}):
            for owner in (False, True):
                evidence = read_diagnostics()
                bad_stage = unavailable | change
                if owner:
                    evidence["profile"]["owners"] = {"0": {"stages": {"server_async": bad_stage}, "counters": {}}}
                else:
                    evidence["profile"]["stages"]["server_async"] = bad_stage
                with self.subTest(change=change, owner=owner), self.assertRaises(ValueError):
                    campaign.read_diagnostic_metrics(evidence, {})

    def test_read_diagnostic_size_limit_accepts_large_evidence_and_rejects_oversize(self):
        evidence = read_diagnostics()
        evidence["profile"]["read_evidence"] = {str(i): "x" * 1024 for i in range(100)}
        self.assertIn("kv_case_read_diagnostic_available", campaign.read_diagnostic_metrics(evidence, {}))
        evidence["profile"]["read_evidence"] = {str(i): "x" * 1024 for i in range(260)}
        with self.assertRaisesRegex(ValueError, "exceeds 256KiB"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_read_diagnostic_accepts_nested_owner_boundary_snapshots(self):
        evidence = read_diagnostics()
        snapshot = {"available": True, "owner_executor": {"available": True,
                    "partitions": {str(i): {"read_requests": i, "read_bytes": i * 4096}
                                   for i in range(5)}},
                    "client_resident": {"available": True,
                    "partitions": {str(i): {"cache_hits": i} for i in range(5)}},
                    "client_resident_lanes": {"available": True, "partitions": {
                        str(i): {"lanes": {"0": {"session_present": True,
                                                  "stats": {"rma_hits": i}}}} for i in range(5)}},
                    "owner_grants": {"available": True, "partitions": {
                        str(i): {"read_grant": {"registration": {
                            "ns": i, "count": i, "max_ns": i, "errors": 0}}}
                        for i in range(5)}}}
        evidence["profile"]["read_evidence"] = {"start": snapshot, "end": snapshot}
        self.assertIn("kv_case_read_diagnostic_available", campaign.read_diagnostic_metrics(evidence, {}))
        evidence["profile"]["read_evidence"]["extra"] = {"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": 1}}}}}}}}
        with self.assertRaisesRegex(ValueError, "nesting exceeds bound"):
            campaign.read_diagnostic_metrics(evidence, {})

    def test_write_diagnostics_export_actual_totals_counts_and_nested_clock_scopes(self):
        text = campaign.terminal_metrics({"status": "completed", "metrics": None,
                                          "write_diagnostics": write_diagnostics()}, {})
        self.assertIn('phase="measurement",scope="sampled_prefix"', text)
        self.assertIn('component="stages",stage="wal_append_ns"} 2.0', text)
        self.assertIn('component="stages",counter="wal_append_errors"} 0', text)
        self.assertIn('component="value_wal_persistence",stage="file_ns"} 1.0', text)
        self.assertIn('component="value_wal_persistence",counter="file_successes"} 2', text)
        self.assertIn('population="observed_samples"} 3', text)
        self.assertNotIn("quantile", text)
        self.assertNotIn("percent", text)
        remote = write_diagnostics()
        remote["profile"] = {"stages": {"write_profile_client_exchange_ns": 700000000,
                                          "write_profile_storage_append_calls": 3,
                                          "write_profile_storage_append_errors": 1}}
        text = campaign.write_diagnostic_metrics(remote, {})
        self.assertIn('stage="write_profile_client_exchange_ns"} 0.7', text)
        self.assertIn('counter="write_profile_storage_append_calls"} 3', text)

    def test_live_nvme_write_window_is_distinct_and_preserves_actual_counters(self):
        value = live_write_diagnostics()
        value["pre_completion_gate"] = {
            "available": True, "scope": "coordinator_arm_through_pre_completion_gate",
            "clock_scope": "independent", "stages": {"write_storage_wait_ns": 12}}
        text = campaign.write_diagnostic_metrics(value, {})
        self.assertIn('scope="measurement_nvme_io_cumulative_delta"', text)
        self.assertIn('population="completed_write_ops"} 3', text)
        self.assertIn('counter="completed_write_bytes"} 12288', text)
        self.assertIn('counter="queue_samples"} 3', text)
        self.assertIn('counter="unknown_writes"} 0', text)
        self.assertIn('stage="queue_ns"} 3.0', text)
        self.assertIn('scope="coordinator_arm_through_pre_completion_gate"', text)
        self.assertNotIn('scope="sampled_prefix"', text)
        self.assertNotIn('stage="cq_ns"', text)
        self.assertNotIn('counter="completed_read_bytes"', text)
        self.assertNotIn("quantile", text)

    def test_live_nvme_write_window_rejects_inconsistent_evidence(self):
        def cases():
            value = live_write_diagnostics()
            value["profile"]["selected_operations"] = 2
            yield value
            value = live_write_diagnostics()
            value["profile"]["stages"]["live_nvmeof"]["completed_write_ops"] = 2
            yield value
            for key, replacement in (("errors", 1), ("unknown_writes", 1), ("queue_ns", -1),
                                     ("completed_write_ops", None), ("errors", None)):
                value = live_write_diagnostics()
                value["profile"]["stages"]["live_nvmeof"][key] = replacement
                yield value
            value = live_write_diagnostics()
            del value["profile"]["stages"]["live_nvmeof"]["queue_samples"]
            yield value
            for key, replacement in (("scope", "sampled_prefix"), ("requested_limit", 7),
                                     ("runtime_observation_mode", "other"),
                                     ("finalization_error", "counter reset")):
                value = live_write_diagnostics()
                value["profile"][key] = replacement
                yield value
            value = live_write_diagnostics()
            value["backend"] = "native"
            yield value
            value = live_write_diagnostics()
            value["runtime_observation_mode"] = "other"
            yield value
            for remove in ("neither", "scope", "stages"):
                value = live_write_diagnostics()
                value["backend"] = "native"
                value["runtime_observation_mode"] = "other"
                value["profile"]["runtime_observation_mode"] = "other"
                if remove == "scope":
                    value["profile"]["scope"] = "sampled_prefix"
                elif remove == "stages":
                    value["profile"]["stages"] = {"wal_append_ns": 1}
                yield value
        for value in cases():
            with self.subTest(value=value), self.assertRaises(ValueError):
                campaign.write_diagnostic_metrics(value, {})

    def test_write_dashboard_selects_independent_write_populations(self):
        dashboard = json.loads((Path(__file__).parent / "dashboards/campaign.json").read_text())
        expressions = [target.get("expr", "") for panel in dashboard["panels"]
                       for target in panel.get("targets", [])]
        expression = next(expr for expr in expressions if "kv_case_write_diagnostic_(available|operations" in expr)
        pattern = re.search(r'scope=~"([^"]+)"', expression).group(1)
        for scope in ("sampled_prefix", "measurement_nvme_io_cumulative_delta",
                      "coordinator_arm_through_pre_completion_gate", "measurement_boundary_cumulative_delta"):
            self.assertRegex(scope, "^(?:" + pattern + ")$")

    def test_write_pre_gate_snapshot_exports_only_available_observations(self):
        evidence = write_diagnostics()
        evidence["pre_completion_gate"] = {
            "available": True, "scope": "coordinator_arm_through_pre_completion_gate",
            "clock_scope": "independent completion-charged counters",
            "stages": {"write_checkpoint_ns": 2000000000,
                       "write_checkpoint_count": 2}}
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('scope="coordinator_arm_through_pre_completion_gate"', text)
        self.assertIn('stage="write_checkpoint_ns"} 2.0', text)
        evidence["pre_completion_gate"] = {"available": False, "reason": "missing"}
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('scope="coordinator_arm_through_pre_completion_gate",diagnostic_backend=', text)
        self.assertNotIn('stage="write_checkpoint_ns"', text)

    def test_incomplete_measurement_does_not_export_boundary_counters(self):
        evidence = write_diagnostics()
        evidence["pre_completion_gate"] = {
            "available": True, "scope": "coordinator_arm_through_pre_completion_gate",
            "clock_scope": "independent clocks", "stages": {
                "write_checkpoint_ns": 2000000000,
                "write_checkpoint_count": 2}}
        evidence["measurement_drained"] = False
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('scope="coordinator_arm_through_pre_completion_gate",diagnostic_backend="native"} 0', text)
        self.assertNotIn('stage="write_checkpoint_ns"', text)
        del evidence["measurement_drained"]
        self.assertIn('stage="write_checkpoint_ns"} 2.0',
                      campaign.write_diagnostic_metrics(evidence, {}))
        evidence["error"] = "finalization failed"
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('stage="write_checkpoint_ns"} 2.0', text)

    def test_transport_pressure_requires_observed_poll_denominator(self):
        evidence = read_diagnostics()
        counters = evidence["profile"]["counters"]
        counters.update(read_profile_transport_tx_all_slots_pending_polls=3,
                        read_profile_transport_rx_unread_slots_polls=1)
        self.assertNotIn("kv_case_read_transport_observations", campaign.read_diagnostic_metrics(evidence, {}))
        counters["read_profile_transport_cq_timed_calls"] = 10
        text = campaign.read_diagnostic_metrics(evidence, {})
        self.assertIn('counter="cq_timed_calls"} 10', text)
        self.assertIn('counter="tx_all_slots_pending_polls"} 3', text)
        self.assertNotIn('counter="rx_no_posted_slots_polls"', text)
        counters["read_profile_transport_cq_timed_calls"] = 0
        self.assertNotIn("kv_case_read_transport_observations", campaign.read_diagnostic_metrics(evidence, {}))
        write = write_diagnostics()
        write["profile"]["stages"] = {"write_profile_client_transport_cq_timed_calls": 10,
                                      "write_profile_client_transport_tx_all_slots_pending_polls": 3,
                                      "write_profile_transport_receive_post_would_block": 2}
        text = campaign.write_diagnostic_metrics(write, {})
        self.assertIn('counter="write_profile_client_transport_tx_all_slots_pending_polls"} 3', text)
        self.assertIn('counter="write_profile_transport_receive_post_would_block"} 2', text)

    def test_native_inventory_is_boundary_evidence_not_zero_filled_io(self):
        evidence = write_diagnostics()
        evidence["profile"]["value_wal_persistence"].update(
            sampled_registered_files_min=None, sampled_registered_files_max=None)
        evidence["profile"]["value_wal_inventory"] = {
            "start": None, "end": {"available": True, "registered_files": 80, "logical_file_length_bytes": 335544320}}
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertIn('scope="measurement_boundary"', text)
        self.assertIn('kv_case_native_wal_registered_files{phase="measurement",scope="measurement_boundary",diagnostic_backend="native",boundary="end"} 80', text)
        self.assertNotIn('boundary="start"', text)
        self.assertNotIn('counter="sampled_registered_files_min"', text)
        evidence["profile"]["value_wal_inventory"]["end"] = {"available": False, "error": "stat failed"}
        text = campaign.write_diagnostic_metrics(evidence, {})
        self.assertNotIn('kv_case_native_wal_registered_files{', text)
        evidence["profile"]["value_wal_inventory"]["end"]["registered_files"] = 0
        with self.assertRaisesRegex(ValueError, "unavailable"):
            campaign.write_diagnostic_metrics(evidence, {})

    def test_write_diagnostics_absence_and_unavailable_stages_are_not_zeros(self):
        self.assertEqual(campaign.write_diagnostic_metrics(None, {}), "")
        value = write_diagnostics()
        value["profile"]["value_wal_persistence"] = {"status": "not_applicable", "reason": "page cache"}
        text = campaign.write_diagnostic_metrics(value, {})
        self.assertNotIn("file_ns", text)
        for changes in ({"runtime_enabled": False}, {"profile": None}, {"error": "profile unavailable"}):
            text = campaign.write_diagnostic_metrics(value | changes, {})
            self.assertIn('diagnostic_backend="native"} 0', text)
            self.assertNotIn("diagnostic_stage_seconds", text)
            self.assertNotIn("diagnostic_stage_count", text)
        value["selection"]["observed_samples"] = 0
        self.assertNotIn("diagnostic_stage_seconds", campaign.write_diagnostic_metrics(value, {}))

    def test_write_diagnostics_reject_wrong_phase_bad_counts_and_oversize(self):
        for changes in ({"phase": "warmup"}, {"schema_version": True}, {"clock_scope": ""},
                        {"runtime_enabled": 1}, {"overlap_semantics": "x" * 65537}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                campaign.write_diagnostic_metrics(write_diagnostics() | changes, {})
        for bad in (-1, 1.5, float("nan"), float("inf"), True):
            value = write_diagnostics()
            value["profile"]["stages"]["wal_append_ns"] = bad
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                campaign.write_diagnostic_metrics(value, {})
        value = write_diagnostics()
        value["profile"]["value_wal_persistence"] = {"status": "not_applicable", "file_ns": 0}
        with self.assertRaisesRegex(ValueError, "unsupported"):
            campaign.write_diagnostic_metrics(value, {})

    def test_authority_write_intervals_are_allowlisted_with_independent_scopes(self):
        value = write_diagnostics()
        counters = dict(zip(sorted(campaign.AUTHORITY_INTERVAL_COUNTERS), (64000, 4, 32000, 2)))
        value["pre_completion_gate"] = {
            "available": True, "scope": "coordinator_arm_through_pre_completion_gate",
            "clock_scope": "exclusive wall elapsed", "stages": counters}
        value["profile"]["coordinator_interval"] = {
            "scope": "measurement_boundary_cumulative_delta",
            "clock_scope": "exclusive wall elapsed", "stages": counters}
        text = campaign.write_diagnostic_metrics(value, {})
        for name in campaign.AUTHORITY_INTERVAL_COUNTERS:
            label = 'stage' if name.endswith('_ns') else 'counter'
            self.assertEqual(sum(f'{label}="{name}"' in line for line in text.splitlines()), 2)
        self.assertIn('scope="coordinator_arm_through_pre_completion_gate"', text)
        self.assertIn('scope="measurement_boundary_cumulative_delta"', text)

    def test_authority_read_observation_uses_write_stage_seconds_not_read_call_units(self):
        counters = {"write_authority_prepare_ns": 32000, "write_authority_admit_ns": 64000,
                    "write_authority_windows": 2, "write_authority_batches": 4}
        evidence = {"scope": "measurement_boundary", "deltas": {
            "coordinator": {"available": True, "counters": counters},
            "owner_executor": {"available": False}}}
        text = campaign.measurement_read_metrics(evidence, {})
        for name, value in counters.items():
            metric = "kv_case_write_diagnostic_stage_seconds" if name.endswith("_ns") else "kv_case_write_diagnostic_stage_count"
            lines = [line for line in text.splitlines() if f'="{name}"' in line]
            self.assertEqual(len(lines), 1)
            self.assertTrue(lines[0].startswith(metric + "{"))
            self.assertEqual(float(lines[0].rsplit(" ", 1)[1]), value / 1e9 if name.endswith("_ns") else value)
            self.assertIn('scope="measurement_boundary"', lines[0])
        self.assertNotIn('path="write_authority_', text)

    def test_full_remote_profile_and_coordinator_window_fit_without_clipping(self):
        value = write_diagnostics()
        value["backend"] = "tidehunter_tcp"
        value["profile"]["stages"] = {
            "transport_" + str(index) + "_ns": index * 1000000000
            for index in range(268)}
        value["profile"]["coordinator_interval"] = {
            "scope": "measurement_boundary_cumulative_delta",
            "clock_scope": "separate full measurement interval",
            "stages": {"write_concurrent_admitted": 4096, "write_publication_ns": 9000000000}}
        text = campaign.write_diagnostic_metrics(value, {})
        self.assertIn('component="stages",stage="transport_267_ns"} 267.0', text)
        self.assertIn('component="coordinator_interval",stage="write_publication_ns"} 9.0', text)

    def test_numeric_budget_is_global_across_profile_components(self):
        value = write_diagnostics()
        for component in ("stages", "value_wal_persistence"):
            value["profile"][component] = {
                "stage_" + str(index) + "_calls": 1 for index in range(256)}
        with self.assertRaises(ValueError):
            campaign.write_diagnostic_metrics(value, {})

    def test_coordinator_interval_is_full_window_even_without_prefix_samples(self):
        value = write_diagnostics()
        value["runtime_enabled"] = False
        value["selection"]["observed_samples"] = 0
        value["profile"]["coordinator_interval"] = {
            "scope": "measurement_boundary_cumulative_delta",
            "clock_scope": "coordinator counters before release through completion gate",
            "stages": {"write_storage_wait_ns": 2000000000, "write_publication_ns": 300000000,
                       "write_checkpoint_count": 4, "write_concurrent_admitted": 100,
                       "write_out_of_prefix_published_batches": 3, "write_dependency_wait_batches": 2}}
        text = campaign.write_diagnostic_metrics(value, {})
        boundary = ('phase="measurement",scope="measurement_boundary_cumulative_delta",'
                    'diagnostic_backend="native",component="coordinator_interval"')
        self.assertIn('stage_seconds{' + boundary + ',stage="write_storage_wait_ns"} 2.0', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_checkpoint_count"} 4', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_concurrent_admitted"} 100', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_out_of_prefix_published_batches"} 3', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_dependency_wait_batches"} 2', text)
        self.assertNotIn('component="stages"', text)
        self.assertIn('scope="sampled_prefix",diagnostic_backend="native"} 0', text)
        self.assertIn('scope="measurement_boundary_cumulative_delta",diagnostic_backend="native"} 1', text)

    def test_removed_checkpoint_phase_counters_are_not_admitted(self):
        # The Exact index and its checkpoint diagnostic ring are gone: evidence that
        # still names a phase counter is not from this tree and must not be exported.
        for key in ("write_checkpoint_diagnostic_root_adoption_ns",
                    "write_checkpoint_diagnostic_index_prepare_count"):
            value = write_diagnostics()
            value["profile"]["coordinator_interval"] = {
                "scope": "measurement_boundary_cumulative_delta",
                "clock_scope": "completed checkpoint phases", "stages": {key: 3}}
            with self.subTest(key=key), self.assertRaises(ValueError):
                campaign.write_diagnostic_metrics(value, {})

    def test_coordinator_interval_absence_or_counter_reset_does_not_invent_zero(self):
        value = write_diagnostics()
        self.assertNotIn("measurement_boundary_cumulative_delta", campaign.write_diagnostic_metrics(value, {}))
        value["profile"]["coordinator_interval_error"] = "write_storage_wait_ns reset"
        text = campaign.write_diagnostic_metrics(value, {})
        self.assertIn('scope="measurement_boundary_cumulative_delta",diagnostic_backend="native"} 0', text)
        self.assertNotIn('component="coordinator_interval"', text)

    def test_coordinator_interval_rejects_wrong_scope_gauges_resets_and_conflicts(self):
        interval = {"scope": "measurement_boundary_cumulative_delta", "clock_scope": "coordinator interval",
                    "stages": {"write_storage_wait_ns": 1}}
        for changes in ({"scope": "sampled_prefix"}, {"clock_scope": ""},
                        {"stages": {"write_inflight_peak": 100}}, {"stages": {"write_storage_wait_ns": -1}},
                        {"stages": {"write_storage_wait_ns": float("nan")}},
                        {"stages": {"write_storage_wait_ns": True}}, {"stages": {}}):
            value = write_diagnostics()
            value["profile"]["coordinator_interval"] = interval | changes
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                campaign.write_diagnostic_metrics(value, {})
        value = write_diagnostics()
        value["profile"]["coordinator_interval"] = interval
        value["profile"]["coordinator_interval_error"] = "counter reset"
        with self.assertRaises(ValueError):
            campaign.write_diagnostic_metrics(value, {})

class ExplicitCgroupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.group = self.root / 'system.slice' / 'kvcloud-th-proof-client'
        self.group.mkdir(parents=True)
        (self.group / 'cpu.stat').write_text('usage_usec 2500000\nnr_periods 10\nnr_throttled 3\nthrottled_usec 500000\n')
        (self.group / 'memory.current').write_text('4096\n')
        (self.group / 'memory.events').write_text('oom 0\noom_kill 0\n')
        (self.group / 'io.stat').write_text('259:1 rbytes=8192 wbytes=4096 rios=2 wios=1\n')
        self.process = {"role": "client", "cgroup": '/system.slice/kvcloud-th-proof-client'}

    def tearDown(self):
        self.directory.cleanup()

    def test_cpu_units_and_device_io_are_not_mixed_with_memory_gauges(self):
        text = campaign.process_payload(self.root, self.process, {"run_id": "th-proof"})
        self.assertIn('cpu_seconds_total{run_id="th-proof",process_role="client"} 2.5', text)
        self.assertIn('cpu_throttled_seconds_total{run_id="th-proof",process_role="client"} 0.5', text)
        self.assertIn('memory_current_bytes{run_id="th-proof",process_role="client"} 4096', text)
        self.assertIn('io_read_bytes_total{run_id="th-proof",process_role="client",device="259:1"} 8192', text)
        self.assertNotIn('memory_peak_bytes', text)  # Unsupported field is absent, not zero.

    def test_disappeared_process_is_unavailable_not_idle_zero_usage(self):
        self.process['cgroup'] += '-gone'
        text = campaign.process_payload(self.root, self.process, {})
        self.assertIn('kv_observer_cgroup_available{process_role="client"} 0', text)
        self.assertNotIn('cpu_seconds_total', text)

    def test_symlink_cannot_substitute_an_unrelated_process_group(self):
        link = self.group.parent / 'linked'
        link.symlink_to(self.group, target_is_directory=True)
        self.process['cgroup'] = '/system.slice/linked'
        text = campaign.process_payload(self.root, self.process, {})
        self.assertIn('kv_observer_cgroup_available{process_role="client"} 0', text)
        self.assertNotIn('cpu_seconds_total', text)

    def test_path_cannot_escape_the_selected_cgroup_mount(self):
        for path in ('/system.slice/../other', '/../../etc', '/unrelated', '/system.slice//x',
                     '/kvowners.slice/x', '/user.slice/a'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                campaign.cgroup_directory(self.root, path)

    def test_exact_multi_owner_slice_is_readable(self):
        group = self.root / ('kvowners' + 'a' * 24 + '.slice') / 'kvcloud-case-storage-15.service'
        group.mkdir(parents=True)
        (group / 'cpu.stat').write_text('usage_usec 2500000\n')
        process = {'role': 'storage-15', 'cgroup': '/' + str(group.relative_to(self.root))}
        text = campaign.process_payload(self.root, process, {})
        self.assertIn('cpu_seconds_total{process_role="storage-15"} 2.5', text)

    def test_malformed_kernel_counter_is_unavailable_not_clamped(self):
        (self.group / 'cpu.stat').write_text('usage_usec -1\n')
        text = campaign.process_payload(self.root, self.process, {})
        self.assertIn('kv_observer_cgroup_available{process_role="client"} 0', text)
        self.assertNotIn('cpu_seconds_total', text)


class IndustryInfoTests(unittest.TestCase):
    def test_resp_info_request_and_bounded_reply(self):
        from unittest.mock import MagicMock
        connection = MagicMock()
        connection.__enter__.return_value = connection
        stream = io.BytesIO(b'$18\r\nused_memory_rss:42\r\n')
        connection.recv.side_effect = stream.read
        with patch.object(campaign.socket, "create_connection", return_value=connection):
            self.assertEqual(campaign.info_section("127.0.0.2", 6379, "memory"), {"used_memory_rss": "42"})
        connection.recv.side_effect = io.BytesIO(b'$262145\r\n').read
        with patch.object(campaign.socket, "create_connection", return_value=connection), self.assertRaises(ValueError):
            campaign.info_section("127.0.0.2", 6379, "memory")

    def test_slow_progress_cannot_extend_info_deadline(self):
        from unittest.mock import MagicMock
        connection = MagicMock()
        connection.__enter__.return_value = connection
        clock = [0.0]
        data = io.BytesIO(b'$18\r\nused_memory_rss:42\r\n')
        def receive(size):
            clock[0] += 0.04
            return data.read(size)
        connection.recv.side_effect = receive
        with patch.object(campaign.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(campaign.socket, "create_connection", return_value=connection):
            with self.assertRaises(TimeoutError):
                campaign.info_section("127.0.0.2", 6379, "memory")

    def target(self, engine):
        return {"engine": engine, "address": "127.0.0.2", "port": 6379}

    def test_fields_remain_engine_specific_and_unavailable_fields_are_absent(self):
        replies = {
            "server": {"monitor_task": "enabled"},
            "stats": {"total_commands_processed": "17", "total_found": "4",
                      "keyspace_hits": "3", "invented_counter": "99"},
            "memory": {"proc_physical_memory_size": "123", "used_memory_rss": "456"},
        }
        for engine, expected, absent in (("garnet", "total_found", "keyspace_hits"),
                                         ("kvrocks", "keyspace_hits", "total_found")):
            with self.subTest(engine=engine), patch.object(campaign, "info_section",
                    side_effect=lambda address, port, section, deadline: replies.get(section, {})):
                body = campaign.info_payload(self.target(engine), {"backend": engine}, {})
                self.assertIn('field="' + expected + '"', body)
                self.assertNotIn('field="' + absent + '"', body)
                self.assertNotIn("invented_counter", body)
                self.assertIn("kv_observer_info_counter_total", body)
                self.assertIn("kv_observer_info_gauge", body)
                self.assertIn("# TYPE kv_observer_info_counter_total counter", campaign.metric_types(body.encode()).decode())

    def test_disabled_or_unavailable_garnet_monitor_never_exports_zeroed_stats(self):
        for server in ({"monitor_task": "disabled"}, {}):
            with self.subTest(server=server), patch.object(campaign, "info_section",
                    side_effect=lambda address, port, section, deadline: {
                        "server": server, "stats": {"total_commands_processed": "0"},
                        "memory": {"store_index_size": "0"}}.get(section, {})):
                body = campaign.info_payload(self.target("garnet"), {"backend": "garnet"}, {})
                self.assertNotIn("kv_observer_info_counter_total", body)
                self.assertIn('field="store_index_size"} 0', body)

    def test_failed_section_is_not_zero_filled_and_other_sections_survive(self):
        def fetch(address, port, section, deadline):
            if section == "stats":
                raise TimeoutError("unavailable")
            return {"used_memory_rss": "42"} if section == "memory" else {}
        with patch.object(campaign, "info_section", side_effect=fetch):
            body = campaign.info_payload(self.target("kvrocks"), {"backend": "kvrocks"}, {})
        self.assertIn('section="stats"} 0', body)
        self.assertIn('field="used_memory_rss"} 42', body)
        self.assertNotIn("kv_observer_info_counter_total", body)

    def test_engine_storage_and_persistence_sections_use_pinned_fields(self):
        replies = {"server": {"monitor_task": "enabled"},
                   "store": {"Log.TailAddress": "109", "ReadCache.CurrentMemorySizeBytes": "N/A"},
                   "persistence": {"CommittedUntilAddress": "88", "bgsave_in_progress": "1"},
                   "rocksdb": {"compaction_count": "3", "block_cache_hit": "12",
                               "block_cache_usage": "4096", "estimate_pending_compaction_bytes[metadata]": "34",
                               "pending_compaction_bytes_stop[metadata]": "2", "unknown[metadata]": "9"}}
        for engine in ("garnet", "kvrocks"):
            with self.subTest(engine=engine), patch.object(campaign, "info_section",
                    side_effect=lambda address, port, section, deadline: replies.get(section, {})):
                body = campaign.info_payload(self.target(engine), {"backend": engine}, {})
                if engine == "garnet":
                    self.assertIn('field="Log.TailAddress"} 109', body)
                    self.assertIn('field="CommittedUntilAddress"} 88', body)
                    self.assertNotIn('field="ReadCache.CurrentMemorySizeBytes"', body)
                    self.assertNotIn("compaction_count", body)
                else:
                    self.assertIn('field="bgsave_in_progress"} 1', body)
                    self.assertIn('field="block_cache_hit"} 12', body)
                    self.assertIn('field="compaction_count"} 3', body)
                    self.assertIn('field="estimate_pending_compaction_bytes[metadata]"} 34', body)
                    self.assertIn('field="pending_compaction_bytes_stop[metadata]"} 2', body)
                    self.assertNotIn("unknown[metadata]", body)

    def test_info_target_must_match_selected_engine(self):
        with self.assertRaises(ValueError):
            campaign.info_payload(self.target("garnet"), {"backend": "kvrocks"}, {})


class CorrelatedTimingTests(unittest.TestCase):
    def test_valkey_pipeline_and_cq_keep_independent_observed_populations(self):
        cq = {"poll_wait_ns": 100, "poll_calls": 2, "cq_poll_ns": 8,
              "cq_poll_calls": 4, "wake_to_cq_ns": 0, "wake_to_cq_samples": 0,
              "clock_failures": 0}
        record = {"operation": "get_many", "pipeline_outcome": "ok",
                  "mutex_wait_ns": 0, "command_encode_ns": 30, "cq": cq, "worker_finished": True}
        artifact = {"profile": {"cq_available": True, "records": [
            record, record | {"pipeline_outcome": "cancelled", "command_encode_ns": None, "cq": None, "worker_finished": False}]}}
        summary = campaign.summarize_correlated_timings("valkey", artifact, "valkey_rdma")
        text = campaign.terminal_metrics({"status": "failed", "correlation_summaries": [summary]}, {})
        self.assertIn('scope="client_pipeline",population="get_many_ok",stage="command_encode"} 3e-08', text)
        self.assertIn('scope="client_cq",population="get_many_ok",stage="poll_wait"} 2', text)
        self.assertIn('scope="client_pipeline",population="get_many_cancelled",stage="mutex_wait"} 0.0', text)
        self.assertNotIn('stage="wake_to_cq"', text)
        self.assertNotIn('stage="reply_decode"', text)
        self.assertNotIn("caller_sample_id", text)
        artifact["profile"]["records"][0]["cq"]["clock_failures"] = 1
        partial = campaign.summarize_correlated_timings("valkey", artifact, "valkey_rdma")
        self.assertEqual(partial["availability"]["cq_clock_failure_records"], 1)
        self.assertEqual(partial["availability"]["cq_clock_failures"], 1)
        self.assertEqual(partial["availability"]["unfinished_workers"], 1)
        self.assertEqual(partial["availability"]["cq_missing_records"], 1)
        text = campaign.correlated_timing_metrics([partial], {})
        self.assertNotIn("client_cq", text)
        self.assertIn('population="cq_clock_failures"} 1', text)
        unavailable = campaign.summarize_correlated_timings("valkey",
            {"profile": {"cq_available": False, "records": []}}, "valkey_tcp")
        self.assertIn('status="unavailable"} 1', campaign.correlated_timing_metrics([unavailable], {}))

    def test_read_pairs_are_local_and_nvme_io_uses_queue_and_token(self):
        def event(name, timestamp, queue=None, token=None):
            return {"name": name, "elapsed_ns": timestamp, "io_queue_id": queue, "io_token": token}
        group = {"membership_classified": True, "events": [
            event("live_io_sqe_submitted", 10, 1, 7),
            event("live_io_sqe_submitted", 11, 2, 7),
            event("live_io_cqe_observed", 20, 2, 7),
            event("live_io_cqe_observed", 30, 1, 7),
            event("live_resolve_started", 40)]}
        call = {"status": "complete", "events": [event("api_started", 1), event("api_finished", 101)],
                "groups": [group, group | {"membership_classified": False}]}
        artifact = {"profile": {"caller_correlation": {"trace": {"calls": [call]}}}}
        summary = campaign.summarize_correlated_timings("read", artifact, "tidehunter_live_nvmeof")
        stages = {(s["scope"], s["stage"]): s for s in summary["stages"]}
        self.assertEqual((stages["read_group", "live_io"]["sum_ns"],
                          stages["read_group", "live_io"]["count"]), (29, 2))
        self.assertNotIn(("read_group", "live_resolve"), stages)
        text = campaign.correlated_timing_metrics([summary], {})
        self.assertIn('population="complete",stage="api"} 1e-07', text)
        self.assertNotIn("io_token", text)

    def test_write_group_not_multiplied_by_callers_and_partial_endpoints_omitted(self):
        def event(stage, timestamp, owner=None):
            return {"stage": stage, "elapsed_ns": timestamp, "owner": owner, "envelope_id": None}
        record = {"completed": True, "error": None, "events": [
            event("caller_start", 10), event("caller_finished", 100),
            event("live_reserve_started", 20, 0), event("live_reserve_completed", 50, 0),
            event("live_io_started", 60, 0)]}
        persistence = {"persist_total_ns": 80, "persist_calls": 1,
                       "persist_wal_file_ns": 0, "file_persist_calls": 0}
        artifact = {"timeline": {"records": [record, record],
            "owner_records": [{"success": True, "append_validation_ns": 35},
                              {"success": False, "append_validation_ns": None}],
            "live_groups": [{"owner_validation_ns": 25, "owner_persistence": persistence},
                            {"owner_validation_ns": None, "owner_persistence": None}]}}
        summary = campaign.summarize_correlated_timings("write", artifact, "tidehunter_live_nvmeof")
        stages = {(s["scope"], s["stage"]): s for s in summary["stages"]}
        self.assertEqual(stages["write_call", "live_reserve"]["count"], 2)
        self.assertEqual(stages["live_owner_group", "owner_validation"]["count"], 1)
        self.assertEqual(stages["write_owner_envelope", "append_validation"]["count"], 1)
        self.assertNotIn(("write_call", "live_io"), stages)
        self.assertNotIn(("live_owner_persistence", "persist_wal_file"), stages)
        self.assertEqual(stages["live_owner_persistence", "persist_total"]["sum_ns"], 80)

    def test_leased_write_io_substages_keep_local_intervals(self):
        names = ['live_io_started', 'live_frame_encoded', 'live_io_submitted', 'live_io_completed']
        record = dict(completed=True, error=None, events=[
            dict(stage=name, elapsed_ns=index * 10, owner=0, envelope_id=None)
            for index, name in enumerate(names)])
        summary = campaign.summarize_correlated_timings(
            "write", {"timeline": {"records": [record]}}, "tidehunter_live_nvmeof")
        stages = {s["stage"]: s for s in summary["stages"]}
        for stage in ("live_frame_encode", "live_io_submit", "live_io_wait"):
            self.assertEqual((stages[stage]["sum_ns"], stages[stage]["count"]), (10, 1))
        self.assertEqual(stages["live_io"]["sum_ns"], 30)

    def test_invalid_summaries_and_absence(self):
        self.assertEqual(campaign.correlated_timing_metrics(None, {}), "")
        self.assertEqual(campaign.summarize_correlated_timings("valkey", {"profile": None},
                                                              "valkey_tcp")["stages"], [])
        bad = {"schema_version": 1, "kind": "read", "backend": "tidehunter_tcp",
               "stages": [{"scope": "read_call", "population": "complete", "stage": "api",
                           "sum_ns": 0, "count": 0}]}
        with self.assertRaises(ValueError):
            campaign.correlated_timing_metrics([bad], {})
        bad["stages"][0]["count"] = 1
        with self.assertRaises(ValueError):
            campaign.correlated_timing_metrics([bad, bad], {})
        bad["stages"][0]["stage"] = "caller_123"
        with self.assertRaises(ValueError):
            campaign.correlated_timing_metrics([bad], {})


class SealedSpdkExporterTests(unittest.TestCase):
    def test_owner_counts_match_fleet_layout(self):
        with patch.object(sys, "path", [str(Path(__file__).resolve().parent.parent), *sys.path]):
            import cloud_layout
            self.assertEqual(campaign.SEALED_OWNER_COUNTS, cloud_layout.OWNER_HOST_COUNTS)
            self.assertEqual(campaign.MAX_SELECTED_PROCESSES, max(cloud_layout.LOGICAL_OWNER_COUNTS))
            import cloud_controller
            name, _ = cloud_controller.owner_slice_limits(
                {'prefix': 'bench-' + '0' * 16,
                 'fleet': {'server_cpus': 8, 'server_memory_mib': 16384}}, 'case')
            self.assertIsNotNone(campaign.OWNER_SLICE_CGROUP.fullmatch(
                '/' + name + '/kvcloud-case-storage-15.service'))

    def evidence(self, count=1):
        # Run 10 compact SPDK canary, trimmed to exporter fields.
        row = {"index": 0, "read_ops": 2048, "read_bytes": 10670080,
               "mean_read_completion_us": 75.01668017578125}
        return {"available": True, "read_ops": 2048, "read_bytes": 10670080,
                "mean_read_completion_us": 75.01668017578125,
                "owners": [row | {"index": i} for i in range(count)]}

    def test_all_admitted_owner_samples(self):
        for count in (1, 2, 4, 5):
            with self.subTest(count=count):
                text = campaign.spdk_target_metrics(self.evidence(count), {})
                self.assertIn('target_owner="all"} 2048', text)
                for i in range(count):
                    self.assertIn(f'target_owner="{i}"' + '} 2048', text)
                self.assertNotIn(f'target_owner="{count}"', text)

    def test_rejects_bad_owner_counts_and_indices(self):
        for count in (0, 3, 6, 16):
            with self.subTest(count=count), self.assertRaises(ValueError):
                campaign.spdk_target_metrics(self.evidence(count), {})
        for indices in ([0, 1, 2, 3, 3], [0, 1, 2, 3, 5], [1],
                        [True], ["0"], [None], [0, 2], [0, 1, 2, 3, 4, 5]):
            value = self.evidence(len(indices))
            for row, index in zip(value["owners"], indices):
                row["index"] = index
            with self.subTest(indices=indices), self.assertRaises(ValueError):
                campaign.spdk_target_metrics(value, {})
        value = self.evidence()
        value["owners"] = [None]
        with self.assertRaises(ValueError):
            campaign.spdk_target_metrics(value, {})

    def test_five_tcp_readers_on_one_owner(self):
        readers = {str(i): {"available": True, "identity": {"partition": 0, "transport": "tcp"},
                            "counters": {"available": True, "counters": {"io_completions": 1}},
                            "stages": {}} for i in range(5)}
        text = campaign.nvmeof_reader_metrics({"available": True, "readers": readers}, {})
        self.assertEqual(text.count('partition="0",transport="tcp",counter="io_completions"} 1'), 5)

    def test_compact_nvme_contexts_and_idle_selection(self):
        for backend, target in (("tidehunter_nvme_rdma", "kernel"),
                                ("tidehunter_nvme_rdma", "spdk"),
                                ("tidehunter_nvme_tcp", "kernel")):
            selected = job() | {"backend": backend, "cohort": "sealed_read",
                                "owner_count": 1, "nvmeof_route": "sealed_read", "nvmeof_target": target}
            labels = campaign.context("run", selected)
            self.assertEqual(labels["requested_nvmeof_target"], target)
            self.assertEqual(labels["requested_nvmeof_route"], "sealed_read")
        idle = campaign.benchmark_payload({"run_id": "run", "stale_seconds": 30},
                                          {"job": None, "results": None, "terminal": None})
        self.assertIn(b'case_id="idle"', idle)
    def test_compact_live_read_requires_primary_cohort(self):
        selected = job() | {"backend": "tidehunter_nvme_rdma", "cohort": "primary_rf1",
                            "selection": "leased_ab_compact", "group": "G17R", "case_kind": "live_read",
                            "nvmeof_route": "live", "nvmeof_target": "kernel"}
        self.assertEqual(campaign.context("run", selected)["requested_nvmeof_route"], "live")
        with self.assertRaises(ValueError):
            campaign.context("run", selected | {"cohort": "sealed_read"})

    def test_nvmeof_path_backend_contexts(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        import cloud_campaign
        for selection in cloud_campaign.NVMEOF_PATH_SELECTIONS:
            for backend in ("native", "tidehunter_verbs_native", "tidehunter_nvme_rdma"):
                selected = job() | {"backend": backend, "cohort": "primary_rf1",
                                    "selection": selection, "group": "G18R",
                                    "case_kind": "live_read", "owner_count": 1}
                if backend == "tidehunter_nvme_rdma":
                    selected.update(nvmeof_route="live", nvmeof_target="kernel")
                labels = campaign.context("run", selected)
                self.assertEqual(labels["requested_backend"], backend)
                if backend == "tidehunter_nvme_rdma":
                    self.assertEqual(labels["requested_nvmeof_route"], "live")
                    self.assertEqual(labels["requested_nvmeof_target"], "kernel")
                    with self.assertRaises(ValueError):
                        campaign.context("run", selected | {"cohort": "sealed_read"})
                else:
                    self.assertNotIn("requested_nvmeof_route", labels)



class LiveSpdkExporterTests(unittest.TestCase):
    def test_live_truncated_five_owner_evidence_is_rejected(self):
        row = {"index": 0, "available": False}
        with self.assertRaises(ValueError):
            campaign.spdk_live_target_metrics({"available": False, "owners": [row]}, {})

    def test_live_read_and_write_selections_render_admitted_intervals(self):
        path = Path(__file__).resolve().parents[2] / "nvmeof" / "spdk_measurement.py"
        spec = importlib.util.spec_from_file_location("spdk_measurement", path)
        measurement = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(measurement)
        for profile in ("read-concurrent", "write-concurrent"):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                selected = job() | {"backend": "tidehunter_nvme_rdma", "nvmeof_route": "live",
                                    "nvmeof_target": "spdk", "profile": profile, "value_size": 65536}
                start = []
                for index in range(5):
                    daemon = {"pid": 100 + index, "process_start_ticks": 10, "executable": "/bin/nvmf_tgt",
                              "executable_device": 1, "executable_inode": 2, "binary_sha256": "a" * 64,
                              "rpc_socket": "/run/rpc", "rpc_device": 3, "rpc_inode": 4}
                    start.append({"case_id": selected["case_id"], "index": index, "target": {
                        "daemon": daemon, "bdevs": [{"name": "wal"}], "subsystems": [],
                        "iostat": {"ticks": 100, "tick_rate": 1000000, "bdevs": [
                            {"name": "wal", "bytes_read": 4096, "num_read_ops": 1,
                             "bytes_written": 4096, "num_write_ops": 1,
                             "read_latency_ticks": 10, "write_latency_ticks": 20}]}}})
                end = copy.deepcopy(start)
                for row in end:
                    stats = row["target"]["iostat"]
                    stats["ticks"] += 1000000
                    stats["bdevs"][0].update(bytes_read=12288, num_read_ops=3, read_latency_ticks=50,
                                              bytes_written=8192, num_write_ops=2, write_latency_ticks=50)
                binding = {}
                for boundary, rows in (("start", start), ("end", end)):
                    name = "spdk-target-" + boundary + ".json"
                    raw = json.dumps(rows).encode()
                    (Path(directory) / name).write_bytes(raw)
                    binding[boundary] = {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
                evidence = measurement.admit_live(directory, binding)
                selection = {"job": selected, "results": str(Path(directory) / selected["case_id"] / "result"),
                             "terminal": {"status": "completed", "spdk_target_io": evidence}}
                text = campaign.benchmark_payload({"run_id": "run", "stale_seconds": 30}, selection).decode()
                self.assertIn('requested_backend="tidehunter_spdk_live_rdma"', text)
                self.assertIn('requested_nvmeof_route="live",requested_nvmeof_target="spdk"', text)
                self.assertIn('target_owner="all",direction="read"} 10', text)
                self.assertIn('target_owner="all",direction="write"} 5', text)
                self.assertIn('target_owner="all",direction="read"} 40960', text)
                self.assertIn('target_owner="all",direction="write"} 20480', text)
                self.assertIn('target_owner="all",direction="read",statistic="mean"} 2e-05', text)
                self.assertIn('target_owner="all",direction="write",statistic="mean"} 3e-05', text)
                self.assertIn('target_owner="all"} 0', text)
                self.assertNotIn("kv_spdk_live_target_io_errors", text)
                self.assertNotIn("kv_spdk_target_read_ops", text)

    def test_partial_owner_metrics_do_not_invent_aggregate_or_error_counts(self):
        owners = [{"index": i, "available": True, "io_error_available": False,
                   "read_ops": 2, "read_bytes": 8192, "mean_read_completion_us": 20,
                   "write_ops": 1, "write_bytes": 4096, "mean_write_completion_us": 30}
                  for i in range(5)]
        owners[2] = {"index": 2, "available": False, "reason": "rotated"}
        text = campaign.spdk_target_metrics({"kind": "live", "available": False, "owners": owners}, {})
        availability = re.search(
            r'(?m)^kv_spdk_live_target_evidence_available(?:\{\})? (\S+)$', text)
        self.assertIsNotNone(availability)
        self.assertEqual(float(availability.group(1)), 0)
        self.assertIn('target_owner="2"', text)
        self.assertIn('kv_spdk_live_target_io_error_available', text)
        self.assertNotIn('kv_spdk_live_target_io_errors', text)
        self.assertNotIn('target_owner="all"', text)
        self.assertNotIn('kv_spdk_target_read_ops', text)


if __name__ == '__main__':
    unittest.main()
