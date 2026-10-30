"""Consumer-visible identity, clock, counter and missing-data regressions."""
import json
from pathlib import Path
import tempfile
import unittest

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
        requested = {"coordinator_cache_bytes": 0, "owner_read_cache_bytes": 0,
                     "registered_policy": "disabled", "page_cache_policy": "not_dropped",
                     "warmup_operations": 10, "read_lanes": 1, "concurrency": 1}
        text = campaign.read_tier_metrics({"read_tier": {"requested": requested, "effective": None}}, {})
        self.assertIn('policy_scope="effective"} 0', text)
        self.assertIn('component="page_cache_policy",policy="not_dropped"} 1', text)
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

    def test_full_remote_profile_and_coordinator_window_fit_without_clipping(self):
        value = write_diagnostics()
        value["backend"] = "tidehunter_tcp"
        value["profile"]["stages"] = {
            "transport_" + str(index) + "_ns": index * 1000000000
            for index in range(268)}
        value["profile"]["coordinator_interval"] = {
            "scope": "measurement_boundary_cumulative_delta",
            "clock_scope": "separate full measurement interval",
            "stages": {"write_concurrent_completed": 4096, "write_publication_ns": 9000000000}}
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
                       "write_checkpoint_count": 4, "write_concurrent_completed": 100,
                       "write_out_of_prefix_published_batches": 3, "write_dependency_wait_batches": 2}}
        text = campaign.write_diagnostic_metrics(value, {})
        boundary = ('phase="measurement",scope="measurement_boundary_cumulative_delta",'
                    'diagnostic_backend="native",component="coordinator_interval"')
        self.assertIn('stage_seconds{' + boundary + ',stage="write_storage_wait_ns"} 2.0', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_checkpoint_count"} 4', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_concurrent_completed"} 100', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_out_of_prefix_published_batches"} 3', text)
        self.assertIn('stage_count{' + boundary + ',counter="write_dependency_wait_batches"} 2', text)
        self.assertNotIn('component="stages"', text)
        self.assertIn('scope="sampled_prefix",diagnostic_backend="native"} 0', text)
        self.assertIn('scope="measurement_boundary_cumulative_delta",diagnostic_backend="native"} 1', text)

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
        for path in ('/system.slice/../other', '/../../etc', '/unrelated', '/system.slice//x'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                campaign.cgroup_directory(self.root, path)

    def test_malformed_kernel_counter_is_unavailable_not_clamped(self):
        (self.group / 'cpu.stat').write_text('usage_usec -1\n')
        text = campaign.process_payload(self.root, self.process, {})
        self.assertIn('kv_observer_cgroup_available{process_role="client"} 0', text)
        self.assertNotIn('cpu_seconds_total', text)


if __name__ == '__main__':
    unittest.main()
