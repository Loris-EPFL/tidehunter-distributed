"""Focused data-integrity tests; no Matplotlib or benchmark execution required."""
import copy
import unittest

import plot_cloud_comparison as plots


def population(operation="read", outcome="success", count=10, p50=100, p99=900):
    def timing():
        return {"sample_count": count, "p50": p50, "p95": p99, "p99": p99,
                "p999": p99, "max": p99}
    result = {"operation": operation, "outcome": outcome, "count": count,
              "logical_keys": count, **{name: timing() for name in plots.TIMINGS}}
    if outcome == "rejected":
        for name in ("api_ns", "scheduling_ns"):
            result[name] = {"sample_count": 0, "p50": None, "p95": None,
                            "p99": None, "p999": None, "max": None}
    return result


def case(backend="native", repeat=0, status="completed", populations=None):
    case_id = f"{backend}-read-concurrent-64-r{repeat}"
    groups = populations if populations is not None else [population()]
    offered = sum(item["count"] for item in groups)
    successful = sum(item["count"] for item in groups if item["outcome"] == "success")
    return {"case_id": case_id, "status": status,
            "backend": {"name": plots.OBSERVED_NAMES.get(backend, backend)},
            "job": {"case_id": case_id, "backend": backend, "cohort": "primary_rf1",
                    "profile": "read-concurrent", "value_size": 64, "repeat": repeat,
                    "measurement_operations": offered, "concurrency": 16},
            "metrics": None if status != "completed" else {
                "measurement_rows_complete": True, "offered_calls": offered,
                "planned_measurement_calls": offered, "success_calls": successful,
                "unsuccessful_calls": offered - successful, "success_fraction": successful / offered,
                "elapsed_seconds": 1, "success_calls_per_second": successful,
                "success_logical_keys_per_second": successful, "success_value_mib_per_second": successful * 64 / 1024**2,
                "populations": groups}}


def dataset(cases):
    return plots.Dataset({"schema_version": 1, "planned_cases": len(cases),
                          "completed_cases": sum(item["status"] == "completed" for item in cases), "cases": cases})


class MatchedRepeatIntegrity(unittest.TestCase):
    def test_extra_fdb_repeat_cannot_contaminate_eight_system_median(self):
        cases = [case(backend, repeat) for backend in plots.BACKENDS for repeat in (0, 1)]
        extra = case("foundationdb", 2, populations=[population(p50=999999, p99=999999)])
        cases.append(extra)
        data = dataset(cases)
        cells = [(backend, "primary_rf1", "read-concurrent", 64) for backend in plots.BACKENDS]
        repeats = data.matched_repeats(cells)
        self.assertEqual(repeats, [0, 1])
        point, reason = plots.aggregate_point(data, cells[-1], repeats, "api_ns", "read", percentile="p50")
        self.assertIsNone(reason)
        self.assertEqual(point["median"], 100)
        self.assertEqual([row["repeat"] for row in point["observations"]], [0, 1])
        self.assertNotIn(extra["case_id"], [row["case_id"] for row in point["observations"]])

    def test_no_silent_one_repeat_subset_on_eight_system_chart(self):
        cases = [case(backend, repeat) for backend in plots.BACKENDS for repeat in (0, 1)]
        cases[-1] = case("foundationdb", 1, "not_run")
        data = dataset(cases)
        cells = [(backend, "primary_rf1", "read-concurrent", 64) for backend in plots.BACKENDS]
        with self.assertRaisesRegex(ValueError, "fewer than two common"):
            data.matched_repeats(cells)

    def test_median_is_over_per_repeat_quantiles_with_observed_range(self):
        data = dataset([case(repeat=0, populations=[population(count=10, p99=100),
                                                  population("put", count=9990)]),
                        case(repeat=1, populations=[population(count=10000, p99=900)])])
        point, reason = plots.aggregate_point(data, ("native", "primary_rf1", "read-concurrent", 64),
                                             [0, 1], "api_ns", "read")
        self.assertIsNone(reason)
        self.assertEqual(point["median"], 500)
        self.assertEqual((point["observed_min"], point["observed_max"]), (100, 900))
        self.assertNotIn("confidence", point)
        self.assertEqual([item["value"] for item in point["observations"]], [100, 900])


class MissingAndOutcomeIntegrity(unittest.TestCase):
    def test_failed_or_missing_case_is_unavailable_not_zero(self):
        data = dataset([case(repeat=0), case(repeat=1, status="failed")])
        cell = ("native", "primary_rf1", "read-concurrent", 64)
        for repeats in ([0, 1], [0, 2]):
            point, reason = plots.aggregate_point(data, cell, repeats, "api_ns", "read")
            self.assertIsNone(point)
            self.assertIn("failed or missing", reason)
        self.assertEqual(plots.coverage(data)["total"], {"completed": 1, "failed": 1})

    def test_latency_keeps_operations_and_outcomes_separate(self):
        groups = [population("read", p99=500), population("put", p99=9000),
                  population("read", "rejected", count=2, p99=99000)]
        data = dataset([case(repeat=repeat, populations=copy.deepcopy(groups)) for repeat in (0, 1)])
        cell = ("native", "primary_rf1", "read-concurrent", 64)
        point, _ = plots.aggregate_point(data, cell, [0, 1], "api_ns", "read")
        self.assertEqual(point["median"], 500)
        response, _ = plots.aggregate_point(data, cell, [0, 1], "response_ns", "read", "rejected")
        self.assertEqual(response["median"], 99000)
        missing, reason = plots.aggregate_point(data, cell, [0, 1], "api_ns", "read", "rejected")
        self.assertIsNone(missing)
        self.assertIn("untimed", reason)
        goodput, _ = plots.aggregate_point(data, cell, [0, 1], plots.GOODPUT[0])
        self.assertEqual(goodput["median"], 20)  # Whole workload, NOT read-only throughput.
        fraction, _ = plots.aggregate_point(data, cell, [0, 1], "rejection_fraction")
        self.assertAlmostEqual(fraction["median"], 2 / 22)  # ALL offered arrivals.

    def test_population_missing_in_one_repeat_is_not_a_single_repeat_point(self):
        data = dataset([case(repeat=0, populations=[population("read"), population("put")]),
                        case(repeat=1, populations=[population("read", count=20)])])
        # Different observed random operation counts need not change declared job count.
        point, reason = plots.aggregate_point(data, ("native", "primary_rf1", "read-concurrent", 64),
                                             [0, 1], "api_ns", "put")
        self.assertIsNone(point)
        self.assertIn("absent", reason)

    def test_failed_case_cannot_carry_success_metrics(self):
        item = case()
        item["status"] = "failed"
        with self.assertRaisesRegex(ValueError, "failed/missing"):
            dataset([item])

    def test_rejected_arrival_cannot_invent_api_latency(self):
        item = case(populations=[population("read", "rejected")])
        item["metrics"]["populations"][0]["api_ns"] = population()["api_ns"]
        with self.assertRaisesRegex(ValueError, "no API/scheduling"):
            dataset([item])


class DuplicateIdentityIntegrity(unittest.TestCase):
    def test_duplicate_case_id_is_rejected(self):
        item = case()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            dataset([item, copy.deepcopy(item)])

    def test_different_case_id_cannot_disguise_duplicate_run_identity(self):
        item = case()
        copied = copy.deepcopy(item)
        copied["case_id"] = copied["job"]["case_id"] = "renamed-copy"
        with self.assertRaisesRegex(ValueError, "duplicate backend/cohort"):
            dataset([item, copied])

    def test_duplicate_population_is_not_added_or_overwritten(self):
        with self.assertRaisesRegex(ValueError, "duplicate operation/outcome"):
            dataset([case(populations=[population(), population()])])

    def test_inconsistent_denominator_is_rejected(self):
        item = case()
        item["metrics"]["success_fraction"] = .5
        with self.assertRaisesRegex(ValueError, "success fraction"):
            dataset([item])

    def test_shared_profile_name_cannot_hide_different_workload_contract(self):
        changed = case("foundationdb")
        changed["job"]["concurrency"] = 1
        with self.assertRaisesRegex(ValueError, "workload contract changed"):
            dataset([case("native"), changed])

    def test_cli_alias_requires_its_actual_observed_adapter_contract(self):
        valid = case("tikv")
        self.assertEqual(valid["backend"]["name"], "tikv_raw")
        self.assertEqual(len(dataset([valid]).cases), 1)
        valid["backend"]["name"] = "cassandra_cql_vendor_unverified"
        with self.assertRaisesRegex(ValueError, "job and observed backend differ"):
            dataset([valid])


if __name__ == "__main__":
    unittest.main()
