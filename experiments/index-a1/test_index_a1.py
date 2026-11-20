"""Offline tests of the A1 case matrix, metrics and decision rule."""

from __future__ import annotations

import gzip
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import a1_cases  # noqa: E402
import a1_metrics  # noqa: E402
import analyze_a1  # noqa: E402

WORKSPACE = "/home/hybr1d/Desktop/PDM/TideHunter"


class Configs(unittest.TestCase):
    def test_canonical_build_matches_the_design_document(self):
        case = a1_cases.case_name(1_000_000, "e4", "native", 0)
        config = a1_cases.config(WORKSPACE, case, 1_000_000, "e4", "native", "build")
        self.assertEqual(config["case_id"], "n1000000-e4-native-r0-build")
        options = config["backend"]["options"]
        self.assertEqual(options["path"], f"{WORKSPACE}/scratch/IndexA1/cases/n1000000-e4-native-r0/db")
        self.assertEqual(options["replay_limits"], {"max_batches": 4096, "max_encoded_bytes": 268435456})
        self.assertEqual(
            options["index_fixture"],
            {"stage": "build", "arm": "exact", "max_admitted_batches": 262144, "max_index_bytes": 536870912, "fragment_bytes": 4194304, "wal_file_bytes": 1073741824},
        )
        self.assertEqual(config["workload"]["put_key_policy"], "new")
        self.assertEqual(config["execution"], {"mode": "closed_loop", "concurrency": 16, "client_shards": 1, "queue_capacity": 64, "request_timeout_ms": 30000})
        self.assertEqual(config["phases"]["measurement_operations"], 1_000_000)
        self.assertEqual(config["seed"], 20261119)
        self.assertEqual(config["limits"]["max_bytes"], 21474836480)

    def test_overwrite_and_arm_substitutions(self):
        case = a1_cases.case_name(5_000_000, "c-scaled", "synchronous", 3)
        config = a1_cases.config(WORKSPACE, case, 5_000_000, "c-scaled", "synchronous", "overwrite", rate=12345)
        fixture = config["backend"]["options"]["index_fixture"]
        self.assertEqual(fixture["arm"], "per_cell")
        self.assertEqual(fixture["per_cell"], {"dirty_keys": 160, "snapshot_written_bytes": 671088640, "stale_wal_bytes": 1342177280})
        self.assertEqual(config["backend"]["options"]["replay_limits"], a1_cases.E16)
        self.assertEqual(config["execution"]["mode"], "open_loop")
        self.assertEqual(config["execution"]["offered_ops_per_second"], 12345)
        self.assertEqual(config["workload"]["put_key_policy"], "overwrite")
        self.assertEqual(config["phases"]["measurement_operations"], a1_cases.TIMED_OPERATIONS)
        pilot = a1_cases.config(WORKSPACE, "p", 1_000_000, "e4", "native", "overwrite", operations=1_000_000)
        self.assertEqual(pilot["execution"]["mode"], "closed_loop")
        self.assertNotIn("offered_ops_per_second", pilot["execution"])
        with self.assertRaises(ValueError):
            a1_cases.config(WORKSPACE, "p", 1_000_000, "e4", "native", "build", rate=1)
        with self.assertRaises(ValueError):
            a1_cases.config(WORKSPACE, "p", 1_000_000, "e4", "native", "overwrite", rate=1.5)

    def test_block_order_rotates_and_skips_unavailable_cells(self):
        cells = a1_cases.local_cells(frozenset({(1_000_000, "native")}))
        self.assertNotIn((10_000_000, "native", "c-scaled"), cells)
        for repeat in range(a1_cases.REPEATS):
            order = a1_cases.block_order(repeat, cells)
            self.assertEqual(sorted(order), sorted(cells))
        first = a1_cases.block_order(0, cells)
        self.assertEqual(first[:3], [(1_000_000, "native", "e4"), (1_000_000, "native", "e16"), (1_000_000, "native", "c-scaled")])
        second = a1_cases.block_order(1, cells)
        # R=1: sizes start at 5M; (R + size_index) % 2 == 0 keeps native first.
        self.assertEqual(second[0][0], 5_000_000)
        self.assertEqual(second[0][1], "native")
        # Arms for R=1, size index 1, native: rotate by 2 -> [c, c-scaled, e4, e16], reversed.
        self.assertEqual([arm for size, tier, arm in second[:2]], ["e16", "e4"])

    def test_worst_case_budget(self):
        budget = a1_cases.case_budget(10_000_000)
        self.assertEqual(budget["owner_worst_bytes"],
                         (10_000_000 + a1_cases.TIMED_OPERATIONS) * 4096)
        self.assertTrue(budget["fits_owner_ceiling"])


class Metrics(unittest.TestCase):
    def test_nearest_rank_and_thirds(self):
        values = list(range(1, 101))
        self.assertEqual(a1_metrics.nearest_rank(values, 0.5), 50)
        self.assertEqual(a1_metrics.nearest_rank(values, 0.99), 99)
        self.assertEqual(a1_metrics.nearest_rank(values, 0.999), 100)
        self.assertIsNone(a1_metrics.thirds_delta([1, 2]))
        self.assertEqual(a1_metrics.thirds_delta([1, 1, 1, 5, 5, 5, 9, 9, 9]), 8)

    def test_measurement_samples_from_compressed_csv(self):
        header = "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id,released_ns,stream,source_checkpoint,source_tx,repetition,db,table,batch_phase,parent_batch,members"
        rows = [header, "qualification,put,success,0,0,true,10,10,10,0,1,0,,,10,,,,,,,,,"]
        for second in range(8):
            for index in range(10):
                at = second * 1_000_000_000 + index * 1000
                rows.append(f"measurement,put,success,{at},{at},true,{at + 500},500,500,0,1,1024,,,{at + 600},,,,,,,,,")
        rows.append("measurement,put,rejected,1,0,false,1,0,0,0,1,1024,,,1,,,,,,,,,")
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
            path = Path(directory)
            with gzip.open(path / "samples.csv.gz", "wt") as handle:
                handle.write("\n".join(rows) + "\n")
            samples = a1_metrics.measurement_samples(path)
        self.assertEqual(samples["successful_puts"], 80)
        self.assertEqual(samples["rejected"], 1)
        self.assertEqual(samples["failed_or_unknown"], 1)
        self.assertEqual(samples["latency_ns"]["p50"], 500)
        self.assertFalse(samples["p999_qualified"])
        self.assertEqual(samples["complete_buckets_after_skip"], 2)
        self.assertEqual(samples["median_puts_per_s"], 10)

    def test_debt_deltas_use_only_the_timed_window(self):
        diagnostics = {"index_checkpoint": {
            "windows": [{"name": "startup", "start_ns": 0, "end_ns": 10}, {"name": "timed", "start_ns": 10, "end_ns": 100}],
            "samples_1s": [{"at_ns": at, "databases": [{"db": "coordinator", "suffix_bytes": at, "suffix_batches": 1, "oldest_age_ns": 5}]} for at in range(0, 120, 10)],
        }}
        debt = a1_metrics.debt_deltas(diagnostics)
        self.assertTrue(debt["available"])
        self.assertEqual(debt["samples"], 9)
        self.assertEqual(debt["batches"], 0)
        self.assertEqual(debt["age_ns"], 0)
        self.assertGreater(debt["bytes"], 0)


def case(ratio: float, cold: float, per_put: float = 100.0, valid: bool = True, debt=None) -> dict:
    return {
        "valid": valid,
        "invalid_reasons": [] if valid else ["x"],
        "ratio": ratio,
        "cold_open_to_first_read_s": cold,
        "coordinator_bytes_per_put": per_put,
        "debt": debt or {"available": True, "bytes": -1, "batches": 0, "age_ns": -1},
    }


def setting(ratio_of, cold_of, **overrides) -> dict:
    return {
        tier: {(size, repeat): case(ratio_of(size, repeat), cold_of(size, repeat), **overrides) for size in a1_cases.SIZES for repeat in range(a1_cases.REPEATS)}
        for tier in a1_cases.TIERS
    }


class Decision(unittest.TestCase):
    def require_three_sizes(self):
        # Pass/fail paths need a screened fit; a two-point fit is exact and the
        # analyzer refuses to qualify it (fewer than three sizes).
        if len(set(a1_cases.SIZES)) < 3:
            self.skipTest("decision pass/fail paths need three sizes")

    def test_power_law_fit_is_exact_for_a_power_law(self):
        fit = analyze_a1.fit({size: 0.01 * (size / 1e6) ** 0.5 for size in a1_cases.SIZES}, 709_000_000)
        self.assertAlmostEqual(fit["slope"], 0.5)
        self.assertAlmostEqual(fit["prediction"], 0.01 * math.sqrt(709))
        self.assertLess(fit["max_abs_residual"], 1e-12)
        self.assertFalse(analyze_a1.fit({1: 0.0, 2: 1.0, 3: 1.0}, 10)["qualified_input"])

    def test_flat_small_metrics_pass_and_select_lower_bytes(self):
        self.require_three_sizes()
        good = setting(lambda size, repeat: 0.05, lambda size, repeat: 2.0 + 0.01 * repeat)
        cheaper = setting(lambda size, repeat: 0.05, lambda size, repeat: 2.0, per_put=50.0)
        settings = {"e4": analyze_a1.evaluate_setting("e4", good), "e16": analyze_a1.evaluate_setting("e16", cheaper)}
        self.assertEqual(settings["e4"]["status"], "pass")
        decision = analyze_a1.decide(settings)
        self.assertEqual(decision["verdict"], "Exact sufficient (projected)")
        self.assertEqual(decision["selected_setting"], "e16")

    def test_growing_ratio_fails_and_both_failing_is_insufficient(self):
        self.require_three_sizes()
        growing = setting(lambda size, repeat: 0.1 * (size / 1e6), lambda size, repeat: 1.0)
        result = analyze_a1.evaluate_setting("e4", growing)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(result["exceeding_predictions"])
        decision = analyze_a1.decide({"e4": result, "e16": analyze_a1.evaluate_setting("e16", growing)})
        self.assertEqual(decision["verdict"], "Exact insufficient")

    def test_noisy_fit_or_missing_case_is_unqualified_not_failed(self):
        noisy = setting(lambda size, repeat: 0.05 * (2.0 if size == 5_000_000 else 1.0), lambda size, repeat: 1.0)
        result = analyze_a1.evaluate_setting("e4", noisy)
        self.assertEqual(result["status"], "unqualified")
        if len(set(a1_cases.SIZES)) >= 3:
            self.assertIn("absolute log residual above 0.2", result["tiers"]["native"]["fits"]["ratio"]["unqualified_reasons"])
        else:
            # A two-point fit is exact; the size-count guard is the screen.
            self.assertIn("fewer than three sizes: a two-point log-log fit is exact",
                          result["tiers"]["native"]["fits"]["ratio"]["unqualified_reasons"])
        missing = setting(lambda size, repeat: 0.05, lambda size, repeat: 1.0)
        del missing["native"][(1_000_000, 4)]
        self.assertEqual(analyze_a1.evaluate_setting("e4", missing)["status"], "unqualified")
        decision = analyze_a1.decide({"e4": result, "e16": analyze_a1.evaluate_setting("e16", missing)})
        self.assertEqual(decision["verdict"], "inconclusive")
        self.assertEqual(decision["selected_setting"], "e4")

    def test_flip_across_reduced_fits_is_unqualified(self):
        # The median sits on the bound; dropping one repeat moves it across.
        near = setting(lambda size, repeat: [0.9, 0.95, 1.0, 1.05, 1.1][repeat], lambda size, repeat: 1.0)
        fit = analyze_a1.evaluate_setting("e4", near)["tiers"]["native"]["fits"]["ratio"]
        self.assertIn("pass/fail flips across the six predictions", fit["unqualified_reasons"])

    def test_debt_growth_in_four_cases_disqualifies(self):
        growing = {"available": True, "bytes": 10, "batches": 1, "age_ns": 5}
        debt = setting(lambda size, repeat: 0.05, lambda size, repeat: 1.0, debt=growing)
        result = analyze_a1.evaluate_setting("e4", debt)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(result["measured_disqualifications"])

    def test_load_ignores_pilots_and_c_scaled(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
            root = Path(directory)
            for name, identity in (
                ("a", {"arm": "e4", "tier": "native", "size": 1_000_000, "repeat": 0}),
                ("b", {"arm": "e4", "tier": "native", "size": 1_000_000, "repeat": "pilot"}),
                ("c", {"arm": "c-scaled", "tier": "native", "size": 1_000_000, "repeat": 0}),
            ):
                (root / name).mkdir()
                (root / name / "case-metrics.json").write_text(json.dumps({**case(0.1, 1.0), "identity": identity}))
            loaded = analyze_a1.load(root)
        self.assertEqual(list(loaded), ["e4"])
        self.assertEqual(list(loaded["e4"]["native"]), [(1_000_000, 0)])


class V6Gate(unittest.TestCase):
    """analyze() with passing evidence: only a matching V6 pass receipt lets
    the measured decision through."""

    TREE = "c4909d7+land3-11f58282+op-replay-16da0b36+index-a0-ba442379+index-a1-runner"
    BINARY = "ab" * 32

    def run_analysis(self, receipt: dict | None, damaged: bool = False) -> dict:
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
            root = Path(directory)
            (root / "run-manifest.json").write_text(json.dumps({"schema_version": 1, "tree": self.TREE, "binary_sha256": self.BINARY}))
            for arm in a1_cases.DECISION_ARMS:
                for tier in a1_cases.TIERS:
                    for size in a1_cases.SIZES:
                        for repeat in range(a1_cases.REPEATS):
                            name = a1_cases.case_name(size, arm, tier, repeat)
                            (root / name).mkdir()
                            identity = {"arm": arm, "tier": tier, "size": size, "repeat": repeat}
                            (root / name / "case-metrics.json").write_text(json.dumps({**case(0.05, 2.0), "identity": identity}))
            if damaged:
                (root / name / "case-metrics.json").write_text('{"valid": tr')
            path = None
            if receipt is not None:
                path = root / "v6.json"
                path.write_text(json.dumps(receipt))
            return analyze_a1.analyze(root, path)

    def receipt(self, **changes) -> dict:
        return {"schema_version": 1, "gate": analyze_a1.V6_GATE, "passed": True, "tree": self.TREE, "binary_sha256": self.BINARY, "max_abs_residual_ratio": 0.04, **changes}

    def assert_unqualified(self, report: dict, reason: str) -> None:
        decision = report["decision"]
        self.assertEqual(decision["verdict"], analyze_a1.V6_UNQUALIFIED)
        self.assertIn(reason, decision["v6_gate"]["reason"])
        self.assertEqual(report["settings"], {})
        for arm in a1_cases.DECISION_ARMS:
            self.assertEqual(decision["setting_status"][arm], "unqualified")
        self.assertNotIn("pass", json.dumps(report["decision"]["setting_status"]))

    def test_matching_pass_receipt_allows_the_measured_decision(self):
        if len(set(a1_cases.SIZES)) < 3:
            self.skipTest("the measured pass path needs three sizes")
        report = self.run_analysis(self.receipt())
        self.assertTrue(report["decision"]["v6_gate"]["passed"])
        self.assertEqual(report["decision"]["verdict"], "Exact sufficient (projected)")

    def test_missing_receipt_is_unqualified(self):
        self.assert_unqualified(self.run_analysis(None), "missing")
        # The gate is checked before any case file is read.
        self.assert_unqualified(self.run_analysis(None, damaged=True), "missing")

    def test_failed_or_malformed_receipt_is_unqualified(self):
        for changes in ({"passed": False}, {"max_abs_residual_ratio": 0.2}, {"gate": "other"}, {"schema_version": 2}):
            self.assert_unqualified(self.run_analysis(self.receipt(**changes)), "failed or malformed")

    def test_mismatched_tree_or_binary_is_unqualified(self):
        self.assert_unqualified(self.run_analysis(self.receipt(tree="other-tree")), "tree does not match")
        self.assert_unqualified(self.run_analysis(self.receipt(binary_sha256="cd" * 32)), "binary_sha256 does not match")


if __name__ == "__main__":
    unittest.main()
