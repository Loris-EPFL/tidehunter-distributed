"""Behavioral data-integrity and offline rendering tests; no benchmark execution."""
import copy
import csv
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

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


def dataset(cases, planned_backends=None):
    document = {"schema_version": 1, "planned_cases": len(cases),
                "completed_cases": sum(item["status"] == "completed" for item in cases), "cases": cases}
    if planned_backends is not None:
        document["planned_backends"] = list(planned_backends)
    return plots.Dataset(document)


def full_matrix_cases(repeats=(0,)):
    """Independent fixture of the full per-repeat campaign cells."""
    cells = [(backend, "primary_rf1", profile, size)
             for backend in plots.BACKENDS
             for profile in ("read-concurrent", "write-concurrent", "read-many32", "point-mixed")
             for size in plots.SIZES]
    cells += [(backend, "primary_rf1", profile, 1024)
              for backend in plots.BACKENDS
              for profile in ("offered-1000", "offered-10000", "offered-50000", "zipf-read")]
    cells += [(backend, cohort, profile, size)
              for backend in plots.TH for size in (256, 8192)
              for cohort, profile in (("coordinator_cache_configured", "coordinator-cache"),
                                      ("owner_cache_configured", "owner-cache"))]
    cells += [("tidehunter_efa_native", "registered_ram_configured", "registered-ram", size)
              for size in (256, 8192)]
    cells += [(backend, "cold_os_cache", "cold-os-large", 65536) for backend in plots.BACKENDS]
    cells += [("native", "native_pagecache", profile, size)
              for profile in ("point-mixed", "read-many32") for size in plots.SIZES]
    result = []
    for backend, cohort, profile, size in cells:
        for repeat in repeats:
            groups = [population(operation, p50=100 + repeat * 100, p99=900 + repeat * 100)
                      for operation in ("read", "put", "delete", "missing_read", "get_many")]
            item = case(backend, repeat, populations=groups)
            identity = f"{backend}-{cohort}-{profile}-{size}-r{repeat}"
            item["case_id"] = identity
            item["job"].update(case_id=identity, cohort=cohort, profile=profile, value_size=size)
            item["metrics"]["success_value_mib_per_second"] = 50 * size / 1024**2
            result.append(item)
    return result


class SingleRepeatPolicy(unittest.TestCase):
    def test_historical_eight_system_report_keeps_coverage_and_single_repeat_policy(self):
        original = [row for row in full_matrix_cases() if row["job"]["backend"] != "valkey_tcp"]
        frozen = copy.deepcopy(original)
        data = dataset(original)
        figures, omitted = plots.build_figures(data)
        self.assertEqual(omitted, [])
        self.assertEqual(original, frozen)
        coverage = plots.coverage(data)
        self.assertEqual(coverage["total"], {"completed": 228})
        self.assertFalse(coverage["current_backend_coverage"])
        self.assertNotIn("valkey_tcp", coverage["planned_backends"])
        primary = next(row for row in figures if row["name"] == "primary-read-concurrent")
        self.assertEqual(len(primary["panels"][0]["series"]), 8)
        self.assertFalse(primary["run_to_run_errorbars"])
        self.assertEqual(primary["repeats"], [0])

    def test_valkey_has_real_observations_in_its_series(self):
        data = dataset(full_matrix_cases())
        figures, _ = plots.build_figures(data)
        self.assertTrue(plots.coverage(data)["current_backend_coverage"])
        primary = next(row for row in figures if row["name"] == "primary-read-concurrent")
        valkey = next(row for row in primary["panels"][0]["series"] if row["backend"] == "valkey_tcp")
        self.assertEqual(valkey["points"][0]["median"], 100)

    def test_backend_subsets_keep_primary_figures_without_unplanned_cache_series(self):
        subsets = (("valkey_tcp",), ("native", "valkey_tcp"),
                   ("tidehunter_tcp", "tidehunter_efa_native"))
        for selected in subsets:
            with self.subTest(selected=selected):
                cases = [row for row in full_matrix_cases() if row["job"]["backend"] in selected]
                figures, omitted = plots.build_figures(dataset(cases, selected))
                self.assertEqual(omitted, [])
                primary = next(row for row in figures if row["name"] == "primary-read-concurrent")
                self.assertEqual({row["backend"] for row in primary["panels"][0]["series"]}, set(selected))
                for figure in figures:
                    for panel in figure["panels"]:
                        self.assertTrue({row["backend"] for row in panel["series"]} <= set(selected))
                names = {row["name"] for row in figures}
                self.assertEqual("cache-cohorts" in names, bool(set(selected) & set(plots.TH)))
                self.assertEqual("native-pagecache-latency" in names, "native" in selected)

    def test_selected_backend_failure_cannot_be_dropped_to_complete_a_figure(self):
        selected = ("native", "valkey_tcp")
        cases = [row for row in full_matrix_cases((0, 1)) if row["job"]["backend"] in selected]
        missing = next(row for row in cases if row["job"]["backend"] == "valkey_tcp"
                       and row["job"]["profile"] == "read-concurrent"
                       and row["job"]["value_size"] == 64 and row["job"]["repeat"] == 1)
        missing.update(status="not_run", metrics=None)
        figures, omitted = plots.build_figures(dataset(cases, selected))
        self.assertNotIn("primary-read-concurrent", {row["name"] for row in figures})
        self.assertIn("primary-read-concurrent", {row["figure"] for row in omitted})

    def test_declared_inventory_cannot_hide_an_observed_backend(self):
        with self.assertRaises(ValueError):
            dataset([case("native")], ("valkey_tcp",))

    def test_one_planned_identity_accepts_one_shared_complete_repeat(self):
        data = dataset([case(backend, 7) for backend in plots.BACKENDS])
        cells = [(backend, "primary_rf1", "read-concurrent", 64) for backend in plots.BACKENDS]
        self.assertEqual(data.planned_repeats, [7])
        self.assertEqual(data.matched_repeats(cells), [7])
        point, reason = plots.aggregate_point(data, cells[0], [7], "api_ns", "read")
        self.assertIsNone(reason)
        self.assertEqual(point["median"], 900)
        self.assertEqual(point["observations"][0]["timed_sample_count_or_denominator"], 10)
        self.assertEqual(point["independent_repetitions"], 1)
        self.assertFalse(point["run_to_run_errorbars"])
        self.assertIn("no run-to-run variability estimate", data.repetition_metadata()["variation"])

    def test_failed_and_not_run_plans_prevent_one_repeat_reclassification(self):
        cell = ("native", "primary_rf1", "read-concurrent", 64)
        for status in ("failed", "not_run"):
            with self.subTest(status=status):
                data = dataset([case(), case("foundationdb", 1, status)])
                self.assertEqual(data.planned_repeats, [0, 1])
                with self.assertRaisesRegex(ValueError, "fewer than two common"):
                    data.matched_repeats([cell])
                point, reason = plots.aggregate_point(data, cell, [0], "api_ns", "read")
                self.assertIsNone(point)
                self.assertIn("fewer than 2", reason)

    def test_missing_cell_prevents_single_repeat_comparison(self):
        cells = [(backend, "primary_rf1", "read-concurrent", 64) for backend in plots.BACKENDS]
        for status in ("failed", "not_run", "absent"):
            with self.subTest(status=status):
                cases = [case(backend) for backend in plots.BACKENDS if backend != "foundationdb"]
                if status != "absent":
                    cases.append(case("foundationdb", status=status))
                data = dataset(cases)
                with self.assertRaises(ValueError):
                    data.matched_repeats(cells)

    def test_full_single_repeat_has_all_twelve_performance_figures_plus_coverage(self):
        cases = full_matrix_cases()
        original = copy.deepcopy(cases)
        figures, omitted = plots.build_figures(dataset(cases))
        self.assertEqual(omitted, [])
        self.assertEqual(len(figures) + 1, 13)
        self.assertEqual(cases, original)  # Source summaries/populations are never rewritten.
        for figure in figures:
            self.assertEqual(figure["repeats"], [0])
            self.assertEqual(figure["repetition_mode"], "single")
            self.assertFalse(figure["run_to_run_errorbars"])

    def test_historical_three_planned_two_complete_preserves_matched_ranges(self):
        cases = full_matrix_cases((0, 1, 2))
        for item in cases:
            if item["job"]["repeat"] == 2 and item["job"]["backend"] != "foundationdb":
                item.update(status="not_run", metrics=None)
        figures, omitted = plots.build_figures(dataset(cases))
        self.assertEqual(omitted, [])
        self.assertEqual(len(figures), 12)
        for figure in figures:
            self.assertEqual(figure["planned_repeats"], [0, 1, 2])
            self.assertEqual(figure["repeats"], [0, 1])
            self.assertTrue(figure["run_to_run_errorbars"])
            for request in figure["panels"]:
                for series in request["series"]:
                    for point in series["points"]:
                        self.assertEqual(point["independent_repetitions"], 2)
                        self.assertEqual([row["repeat"] for row in point["observations"]], [0, 1])
                        if point["metric"] in plots.TIMINGS:
                            self.assertEqual(point["observed_max"] - point["observed_min"], 100)

    def test_single_repeat_csv_retains_units_source_values_and_explicit_mode(self):
        data = dataset(full_matrix_cases())
        figures, _ = plots.build_figures(data)
        with tempfile.TemporaryDirectory() as directory:
            plots.export_data(figures, plots.coverage(data), Path(directory))
            with (Path(directory) / "plot-data.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
        latency = [row for row in rows if row["metric"] == "api_ns"]
        self.assertTrue(latency)
        for row in latency:
            self.assertEqual(row["unit"], "ns")
            self.assertEqual(row["value"], row["median"])
            self.assertEqual(row["planned_repeats"], "[0]")
            self.assertEqual(row["repetition_mode"], "single")
            self.assertEqual(row["independent_repetitions"], "1")
            self.assertEqual(row["run_to_run_errorbars"], "False")
            self.assertIn("no run-to-run variability estimate", row["variation"])

    def test_renderer_emits_whiskers_only_for_multiple_repetitions(self):
        # Inspect real SVG drawing primitives, not mocked Matplotlib calls.
        for repeats in ((0,), (0, 1)):
            with self.subTest(repeats=repeats), tempfile.TemporaryDirectory() as directory:
                data = dataset(full_matrix_cases(repeats))
                figures, _ = plots.build_figures(data)
                figure = copy.deepcopy(next(row for row in figures if row["name"] == "cold-os-cache"))
                figure.update(panels=figure["panels"][:1], columns=1, files=["observations.svg"])
                plots.render([figure], plots.coverage(data), Path(directory))
                root = ET.parse(Path(directory) / "observations.svg").getroot()
                whiskers = [element for element in root.iter()
                            if element.get("id", "").startswith("LineCollection_")]
                self.assertEqual(bool(whiskers), len(repeats) > 1)


class MatchedRepeatIntegrity(unittest.TestCase):
    def test_extra_fdb_repeat_cannot_contaminate_all_system_median(self):
        cases = [case(backend, repeat) for backend in plots.BACKENDS for repeat in (0, 1)]
        extra = case("foundationdb", 2, populations=[population(p50=999999, p99=999999)])
        cases.append(extra)
        data = dataset(cases)
        cells = [(backend, "primary_rf1", "read-concurrent", 64) for backend in plots.BACKENDS]
        repeats = data.matched_repeats(cells)
        self.assertEqual(repeats, [0, 1])
        fdb_cell = next(cell for cell in cells if cell[0] == "foundationdb")
        point, reason = plots.aggregate_point(data, fdb_cell, repeats, "api_ns", "read", percentile="p50")
        self.assertIsNone(reason)
        self.assertEqual(point["median"], 100)
        self.assertEqual([row["repeat"] for row in point["observations"]], [0, 1])
        self.assertNotIn(extra["case_id"], [row["case_id"] for row in point["observations"]])

    def test_no_silent_one_repeat_subset_on_all_system_chart(self):
        cases = [case(backend, repeat) for backend in plots.BACKENDS for repeat in (0, 1)]
        index = next(i for i, row in enumerate(cases)
                     if row["job"]["backend"] == "foundationdb" and row["job"]["repeat"] == 1)
        cases[index] = case("foundationdb", 1, "not_run")
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


class ColdAvailabilityIntegrity(unittest.TestCase):
    def cold_cases(self):
        cases = []
        for backend in plots.BACKENDS:
            for repeat in (0, 1):
                item = case(backend, repeat)
                item["case_id"] = f"{backend}-cold-r{repeat}"
                item["job"].update(case_id=item["case_id"], cohort="cold_os_cache",
                                   profile="cold-os-large", value_size=65536)
                item["metrics"]["success_value_mib_per_second"] = 10 * 65536 / 1024**2
                cases.append(item)
        return cases

    def test_completed_foundationdb_cold_reads_are_not_historically_excluded(self):
        figures, _ = plots.build_figures(dataset(self.cold_cases()))
        cold = next(row for row in figures if row["name"] == "cold-os-cache")
        self.assertEqual(cold["unavailable_systems"], [])
        series = cold["panels"][0]["series"]
        self.assertEqual({row["backend"] for row in series}, set(plots.BACKENDS))
        fdb = next(row for row in series if row["backend"] == "foundationdb")
        self.assertEqual(fdb["points"][0]["median"], 100)

    def test_failed_cold_backend_has_explicit_coverage_not_a_latency_zero(self):
        cases = self.cold_cases()
        for item in cases:
            if item["job"]["backend"] == "foundationdb":
                item["status"] = "failed"
        figures, _ = plots.build_figures(dataset(cases))
        cold = next(row for row in figures if row["name"] == "cold-os-cache")
        self.assertEqual([row["backend"] for row in cold["unavailable_systems"]], ["foundationdb"])
        self.assertEqual(len(cold["unavailable_systems"][0]["case_ids"]), 2)
        self.assertNotIn("foundationdb", {row["backend"] for row in cold["panels"][0]["series"]})
        self.assertEqual(cold["repeats"], [0, 1])

    def test_single_repeat_cold_missing_systems_are_explicit_not_zero_filled(self):
        for status in ("failed", "not_run", "absent"):
            with self.subTest(status=status):
                cases = [row for row in self.cold_cases() if row["job"]["repeat"] == 0]
                fdb = next(row for row in cases if row["job"]["backend"] == "foundationdb")
                if status == "absent":
                    cases.remove(fdb)
                else:
                    fdb.update(status=status, metrics=None)
                figures, _ = plots.build_figures(dataset(cases, plots.BACKENDS))
                cold = next(row for row in figures if row["name"] == "cold-os-cache")
                self.assertEqual(cold["repeats"], [0])
                self.assertFalse(cold["run_to_run_errorbars"])
                excluded = cold["unavailable_systems"]
                self.assertEqual([row["backend"] for row in excluded], ["foundationdb"])
                self.assertEqual(excluded[0]["case_ids"], [] if status == "absent" else [fdb["case_id"]])
                self.assertNotIn("foundationdb", {row["backend"] for row in cold["panels"][0]["series"]})

    def test_cold_multi_plan_does_not_fall_back_to_one_common_repeat(self):
        cases = [row for row in full_matrix_cases((0, 1, 2)) if row["job"]["cohort"] == "cold_os_cache"]
        for row in cases:
            excluded_repeat = 2 if row["job"]["backend"] == "native" else 0
            if row["job"]["repeat"] == excluded_repeat:
                row.update(status="not_run", metrics=None)
        figures, omitted = plots.build_figures(dataset(cases))
        self.assertNotIn("cold-os-cache", {row["name"] for row in figures})
        cold = next(row for row in omitted if row["figure"] == "cold-os-cache")
        self.assertIn("fewer than two common", cold["reason"])
        self.assertEqual(cold["unavailable_systems"], [])

    def test_cold_single_plan_with_no_complete_system_is_omitted_with_exclusions(self):
        cases = [row for row in self.cold_cases() if row["job"]["repeat"] == 0]
        for row in cases:
            row.update(status="not_run", metrics=None)
        figures, omitted = plots.build_figures(dataset(cases))
        self.assertEqual(figures, [])
        cold = next(row for row in omitted if row["figure"] == "cold-os-cache")
        self.assertEqual({row["backend"] for row in cold["unavailable_systems"]}, set(plots.BACKENDS))


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

    def test_partial_failed_measurements_do_not_hide_completed_repeats(self):
        complete = [case(repeat=repeat, populations=[population(count=20)]) for repeat in (0, 1)]
        partial = case(repeat=2, populations=[population(p50=99999, p99=99999)])
        partial["status"] = "failed"
        partial["job"]["measurement_operations"] = 20
        partial["metrics"]["planned_measurement_calls"] = 20
        partial["metrics"]["measurement_rows_complete"] = False
        data = dataset(complete + [partial])
        cell = ("native", "primary_rf1", "read-concurrent", 64)
        self.assertEqual(plots.coverage(data)["total"], {"completed": 2, "failed": 1})
        repeats = data.matched_repeats([cell])
        self.assertEqual(repeats, [0, 1])
        point, reason = plots.aggregate_point(data, cell, repeats, "api_ns", "read", percentile="p50")
        self.assertIsNone(reason)
        self.assertEqual(point["median"], 100)
        self.assertEqual([row["repeat"] for row in point["observations"]], [0, 1])

    def test_not_run_case_cannot_carry_measurement_metrics(self):
        item = case()
        item["status"] = "not_run"
        with self.assertRaises(ValueError):
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

    def test_single_planned_repeat_still_rejects_incompatible_value_contracts(self):
        changed = case("foundationdb")
        changed["job"]["measurement_operations"] = 20
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
