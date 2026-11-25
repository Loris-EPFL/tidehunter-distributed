"""Offline tests for the Q1 native-vs-leased comparison arms (q1_compare.py),
the seal matrix extension, dispatch phases, the no-waiting qualification rule
(derived per repeat from its own metrics, sweep stops at the last qualifying
point), and exact nearest-rank quantiles from a synthetic samples.csv."""
import tempfile
import unittest
from pathlib import Path

from q1_compare import (COMPARISON_CASES, CONCURRENCY_SWEEP, NAMED_DIFFERENCES, PUT_REFERENCE,
                        REPLAY_REFERENCE, _derived_status, arm_report, cross_check_buckets,
                        exact_quantiles, read_samples_csv, validate_case, validate_point_report)
from q1_dispatch import PHASES
from q1_manifest import COMPARISON, MANDATORY, roles

STOPPED = {"status": "not run (sweep stopped)"}


def context(sync_writes):
    return {"engine_sha": "0" * 40, "client_sha256": "a" * 64, "harness_sha": "b" * 64,
            "machine_type": "z4m-highmem-16", "image_id": 7, "ssd": "local-ssd",
            "wal_geometry": "native_default", "wal_prewrite": True,
            "sync_writes": sync_writes, "named_differences": NAMED_DIFFERENCES}


def put_case(tier, repeats=2, row_operations=None, sync_writes=None):
    return {"kind": "comparison", "tier": tier, "repeats": repeats,
            "backends": ("leased", "native"), "concurrency": list(CONCURRENCY_SWEEP),
            "context": context(sync_writes if sync_writes is not None else tier == "synchronous"),
            "workload": {"case_kind": "remote_put", "value_size": 512, "batch_size": 32,
                         "keys": 4096, "warmup_operations": 65536,
                         "row_operations": row_operations or PUT_REFERENCE["row_operations_reference"],
                         "distribution": "uniform", "mix": {"put": 100}, "owner_count": 2,
                         "owner_wal_geometry": "native_default", "owner_wal_prewrite": True,
                         "target_io": "buffered", "min_row_seconds": 10}}


def replay_case(tier, repeats=2, sync_writes=None):
    return {"kind": "comparison", "tier": tier, "repeats": repeats,
            "backends": ("leased", "native"), "concurrency": list(CONCURRENCY_SWEEP),
            "context": context(sync_writes if sync_writes is not None else tier == "synchronous"),
            "workload": {"case_kind": "mainnet_replay", "value_size": 512, "batch_size": 32,
                         "keys": 4096, "repetitions_reference": 4096,
                         "distribution": "uniform", "mix": {"put": 50, "read": 50}, "owner_count": 2,
                         "owner_wal_geometry": "native_default", "owner_wal_prewrite": True,
                         "target_io": "buffered",
                         "fixture_manifest_sha256": REPLAY_REFERENCE["fixture_manifest_sha256"],
                         "row_operations": 4096, "min_row_seconds": 10}}


def row(repeat, p50, throughput_ns, status="qualified", queue_wait_ns_per_op=0,
        duration_ns=15_000_000_000):
    return {"repeat": repeat, "samples": 1000, "rows": 1000, "duration_ns": duration_ns,
            "throughput_ns": throughput_ns, "queue_wait_ns_per_op": queue_wait_ns_per_op,
            "status": status, "p50": p50, "p90": p50, "p99": 2 * p50,
            "p999": 4 * p50, "p9999": 8 * p50}


def arm_rows(repeats, p50, throughput_ns, status="qualified", queue_wait=0):
    return [row(i, p50, throughput_ns, status, queue_wait) for i in range(repeats)]


def scaling_plan(repeats, base_p50=1000, base_throughput_ns=1000):
    """A clean plan: flat p50, ns/op shrinking as 1/c, zero queue wait."""
    plan = [arm_rows(repeats, base_p50, base_throughput_ns)]
    for concurrency in CONCURRENCY_SWEEP[1:]:
        plan.append(arm_rows(repeats, base_p50, base_throughput_ns // concurrency))
    return plan


def point_report(case, config, leased_plan, native_plan):
    points = []
    for i, concurrency in enumerate(config["concurrency"]):
        points.append({"concurrency": concurrency,
                       "arms": {"leased": leased_plan[i], "native": native_plan[i]}})
    return {"case": case, "source": "samples.csv exact nearest-rank", "points": points}


class ComparisonCaseTests(unittest.TestCase):
    def test_put_case_accepts_q0_pin(self):
        validate_case(put_case("native"), "put_native")
        validate_case(put_case("synchronous"), "put_synchronous")

    def test_replay_case_accepts_q0_pin(self):
        validate_case(replay_case("native"), "replay_native")
        validate_case(replay_case("synchronous"), "replay_synchronous")

    def test_sweep_rejects_c64(self):
        config = put_case("native")
        config["concurrency"] = [1, 4, 8, 16, 64]
        with self.assertRaisesRegex(ValueError, "sweep"):
            validate_case(config, "put_native")

    def test_context_is_closed(self):
        # bucket_width_log2 was removed: the sealed context admits no dead field.
        config = put_case("native")
        config["context"]["bucket_width_log2"] = 9
        with self.assertRaisesRegex(ValueError, "invalid comparison context"):
            validate_case(config, "put_native")

    def test_sync_writes_must_match_tier(self):
        with self.assertRaisesRegex(ValueError, "sync_writes"):
            validate_case(put_case("native", sync_writes=True), "put_native")
        with self.assertRaisesRegex(ValueError, "sync_writes"):
            validate_case(put_case("synchronous", sync_writes=False), "put_synchronous")

    def test_workload_pin(self):
        config = put_case("native")
        config["workload"]["value_size"] = 1024
        with self.assertRaisesRegex(ValueError, "value_size"):
            validate_case(config, "put_native")
        config = put_case("native")
        config["workload"]["batch_size"] = 64
        with self.assertRaisesRegex(ValueError, "batch_size"):
            validate_case(config, "put_native")

    def test_repeats_floor_and_row_cut(self):
        with self.assertRaisesRegex(ValueError, "repeats"):
            validate_case(put_case("native", repeats=1), "put_native")
        config = put_case("native", row_operations=PUT_REFERENCE["row_operations_reference"] + 1)
        with self.assertRaisesRegex(ValueError, "row_operations"):
            validate_case(config, "put_native")

    def test_open_difference_set_rejected(self):
        config = put_case("native")
        config["context"]["named_differences"] = {**NAMED_DIFFERENCES, "clock": "different NTP"}
        with self.assertRaisesRegex(ValueError, "closed set"):
            validate_case(config, "put_native")

    def test_roles_and_phases(self):
        for case in COMPARISON_CASES:
            self.assertEqual(roles(case),
                             {"coordinator": ("V0", "leased"), "owner_O0": ("O0", "leased"),
                              "owner_O1": ("O1", "leased"), "native_host": ("V1", "native")})
            self.assertEqual(PHASES[case], ("prepare", "measure", "collect"))
        self.assertEqual(COMPARISON, COMPARISON_CASES)
        self.assertTrue(set(COMPARISON).isdisjoint(MANDATORY))


class DerivedStatusTests(unittest.TestCase):
    BASE = {"p50": 1000, "throughput_ns": 1000}

    def test_clean_row_qualifies(self):
        clean = row(0, 1000, 1000 // 16)
        self.assertEqual(_derived_status(clean, 16, self.BASE), "qualified")

    def test_p50_blowup_saturates(self):
        self.assertEqual(_derived_status(row(0, 1500, 1000 // 16), 16, self.BASE),
                         "waiting (saturated)")

    def test_sublinear_throughput_saturates(self):
        self.assertEqual(_derived_status(row(0, 1000, 1000), 16, self.BASE),
                         "waiting (saturated)")

    def test_waiting_workers_saturate(self):
        self.assertEqual(_derived_status(row(0, 1000, 1000 // 16, queue_wait_ns_per_op=500), 16,
                                         self.BASE), "waiting (saturated)")


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.config = put_case("native")

    def test_clean_scaling_report_accepted(self):
        report = point_report("put_native", self.config, scaling_plan(2), scaling_plan(2))
        validate_point_report(report, "put_native", self.config)

    def test_short_row_rejected_against_measured_duration(self):
        plan = scaling_plan(2)
        plan[0] = [row(0, 1000, 1000, duration_ns=2_000_000_000), row(1, 1000, 1000)]
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "identity"):
            validate_point_report(report, "put_native", self.config)

    def test_label_contradicting_metrics_rejected(self):
        plan = scaling_plan(2)
        # failing p50 labelled qualified: the label contradicts the metrics
        plan[3] = arm_rows(2, 1500, 1000 // 16)
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "contradicts"):
            validate_point_report(report, "put_native", self.config)
        # passing metrics labelled waiting: contradiction in the other direction
        plan = scaling_plan(2)
        plan[3] = arm_rows(2, 1000, 1000 // 16, status="waiting (saturated)")
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "contradicts"):
            validate_point_report(report, "put_native", self.config)

    def test_saturation_at_last_point_accepted(self):
        plan = scaling_plan(2)
        plan[3] = arm_rows(2, 1500, 1000 // 16, status="waiting (saturated)")
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        validate_point_report(report, "put_native", self.config)

    def test_sweep_stop_with_markers_accepted(self):
        # leased saturates at c=8; its c=16 point is an explicit not-run marker
        # while the native arm runs all four points: the pair stays visible.
        leased = [scaling_plan(2)[0], scaling_plan(2)[1],
                  arm_rows(2, 1500, 1000 // 8, status="waiting (saturated)"), STOPPED]
        report = point_report("put_native", self.config, leased, scaling_plan(2))
        validate_point_report(report, "put_native", self.config)

    def test_c1_pilot_must_qualify(self):
        plan = scaling_plan(2)
        # a saturated pilot (waiting workers even at c=1) with a consistent
        # label: only the pilot-must-qualify rule can reject it.
        plan[0] = arm_rows(2, 1000, 1000, status="waiting (saturated)", queue_wait=500)
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "c=1 pilot"):
            validate_point_report(report, "put_native", self.config)

    def test_measured_rows_after_saturation_rejected(self):
        leased = [scaling_plan(2)[0], scaling_plan(2)[1],
                  arm_rows(2, 1500, 1000 // 8, status="waiting (saturated)"),
                  arm_rows(2, 1500, 1000 // 16, status="waiting (saturated)")]
        report = point_report("put_native", self.config, leased, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "not run"):
            validate_point_report(report, "put_native", self.config)

    def test_marker_before_any_saturation_rejected(self):
        leased = [scaling_plan(2)[0], scaling_plan(2)[1], STOPPED, STOPPED]
        report = point_report("put_native", self.config, leased, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "one row per interleaved repeat"):
            validate_point_report(report, "put_native", self.config)

    def test_saturation_repeats_must_agree(self):
        plan = scaling_plan(2)
        # repeat 0 qualifies; repeat 1's own metrics saturate and say so:
        # each label matches its own metrics, but the point cannot be both.
        plan[3] = [row(0, 1000, 1000 // 16),
                   row(1, 1500, 1000 // 16, status="waiting (saturated)")]
        report = point_report("put_native", self.config, plan, scaling_plan(2))
        with self.assertRaisesRegex(ValueError, "disagree"):
            validate_point_report(report, "put_native", self.config)


class SamplesCsvTests(unittest.TestCase):
    def test_exact_quantiles_from_synthetic_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "samples.csv"
            api = list(range(1, 1001))          # p50=500, p99=990, p999=999
            response = list(range(2, 1002, 1))  # p50=501, p99=991, p999=1000
            path.write_text("api_ns,response_ns\n"
                            + "\n".join(f"{a},{r}" for a, r in zip(api, response)) + "\n")
            got_api, got_response = read_samples_csv(path)
            self.assertEqual(got_api, api)
            report = arm_report(got_api, got_response)
            self.assertEqual(report["api"], {"p50": 500, "p99": 990, "p999": 999})
            self.assertEqual(report["response"], {"p50": 501, "p99": 991, "p999": 1000})

    def test_exact_quantiles_on_10k(self):
        values = list(range(1, 10001))
        got = exact_quantiles(values)
        self.assertEqual(got["p50"], 5000)
        self.assertEqual(got["p99"], 9900)
        self.assertEqual(got["p999"], 9990)

    def test_missing_columns_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "samples.csv"
            path.write_text("latency_ns\n1\n2\n")
            with self.assertRaisesRegex(ValueError, "api_ns/response_ns"):
                read_samples_csv(path)

    def test_bucket_cross_check(self):
        # log2 buckets: the bucket ending at U covers (U/2, U]
        self.assertTrue(cross_check_buckets(500, 511))   # 500 in (255, 511]
        self.assertTrue(cross_check_buckets(511, 511))
        with self.assertRaisesRegex(ValueError, "bucket"):
            cross_check_buckets(200, 511)                # below the bucket's lower bound
        with self.assertRaisesRegex(ValueError, "bucket"):
            cross_check_buckets(500, 300)                # above the reported bound

    def test_exact_quantiles_are_nearest_rank(self):
        self.assertEqual(exact_quantiles([5, 1, 4, 2, 3])["p50"], 3)
        self.assertEqual(exact_quantiles([5, 1, 4, 2, 3])["p999"], 5)

class DispatchAndAdmitTests(unittest.TestCase):
    def test_every_case_has_phases_and_capacity_survives(self):
        from q1_manifest import CAPACITY
        for case in (*MANDATORY, *COMPARISON_CASES, CAPACITY):
            self.assertIn(case, PHASES, f"{case} lost its dispatch phases")
        self.assertEqual(PHASES[CAPACITY], ("control", "policy", "collect"))

    def test_pre_launch_admit_needs_no_points_report(self):
        """_admit_case binds the case contract only: a package with
        q1-<case>.json and NO measured points report passes pre-launch
        admission; the collect step validates the report after the run."""
        import hashlib
        import json as _json
        from q1_admit import _admit_case, admit_comparison_points

        spec = {"inputs": {"q1-workload.json": "1" * 64}, "exact": "E4"}
        cell = {"id": "put_native", "seconds": 600, "inputs_sha": None}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = put_case("native")
            (root / "q1-put_native.json").write_text(_json.dumps(config))
            cell["inputs_sha"] = hashlib.sha256(
                (root / "q1-put_native.json").read_bytes()).hexdigest()
            spec["inputs"]["q1-put_native.json"] = cell["inputs_sha"]
            # no q1-put_native-points.json exists anywhere: pre-launch
            # admission must succeed on the case contract alone
            _admit_case(root, spec, cell)
            # the collect step then validates the measured report through the
            # collect receipt's own artifact binding, not the seal inputs
            report = point_report("put_native", config, scaling_plan(2), scaling_plan(2))
            report_path = root / "runtime" / "bin" / "q1-put_native-points.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(_json.dumps(report))
            report_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
            spec["cases"] = [cell]
            receipt = {"artifacts": [{"path": "runtime/bin/q1-put_native-points.json",
                                      "sha256": report_sha, "generation": 1, "bytes": 1}]}
            got = admit_comparison_points(receipt, root, spec, "put_native")
            self.assertEqual(got, report)
            with self.assertRaisesRegex(ValueError, "exactly one points report"):
                admit_comparison_points({"artifacts": []}, root, spec, "put_native")
            with self.assertRaisesRegex(ValueError, "not part of this seal"):
                admit_comparison_points(receipt, root, spec, "replay_native")





class OptionCSealTests(unittest.TestCase):
    def c_seal(self):
        from q1_manifest import PROOFS, INDEX_PROOFS, GOLDEN_PROOFS, COMPARISON
        proofs = (PROOFS | {"comparison_admission", "comparison_local"}) - INDEX_PROOFS - GOLDEN_PROOFS
        return {"version": 1, "profile": "nvmeof_q1", "plan_sha": "1" * 64,
                "engine_sha": "0" * 40, "sui_sha": "1" * 40, "image_id": 7,
                "executables": {r: {"path": f"runtime/bin/{r}", "sha256": "1" * 64} for r in
                                ("kv_research", "sui_golden_native", "sui_golden_sharded")},
                "inputs": {"q1-workload.json": "1" * 64, "q1-build-receipt.json": "1" * 64},
                "proofs": dict.fromkeys(sorted(proofs), "1" * 64),
                "ci": {"engine_sha": "0" * 40, "run": 5, "attempt": 1,
                       "kernel_artifact": "kernel-nvme-tcp-5-1",
                       "workflow_sha": "1" * 64, "kernel_sha": "1" * 64},
                "cases": [{"id": case, "seconds": 900 if case.startswith("put") else 3150,
                           "inputs_sha": "2" * 64} for case in COMPARISON],
                "exact": None, "ceiling_cents": 9668, "artifact_bytes": 1000}

    def test_comparison_only_seal_validates(self):
        from q1_manifest import validate_seal
        validate_seal(self.c_seal())
        # 3,300 fixed + 900 + 900 + 3,150 + 3,150 = 11,400 <= 12,300

    def test_c_seal_rejects_index_proofs_and_exact(self):
        from q1_manifest import validate_seal
        seal = self.c_seal()
        seal["proofs"]["a0"] = "1" * 64
        with self.assertRaisesRegex(ValueError, "proof digests"):
            validate_seal(seal)
        seal = self.c_seal()
        seal["exact"] = "E4"
        with self.assertRaisesRegex(ValueError, "Exact"):
            validate_seal(seal)

    def test_c_seal_rejects_partial_comparison_set(self):
        from q1_manifest import validate_seal
        seal = self.c_seal()
        seal["cases"] = seal["cases"][:3]
        with self.assertRaisesRegex(ValueError, "partial Q1 matrix"):
            validate_seal(seal)


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
