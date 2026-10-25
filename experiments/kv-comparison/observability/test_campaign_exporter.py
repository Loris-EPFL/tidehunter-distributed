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
