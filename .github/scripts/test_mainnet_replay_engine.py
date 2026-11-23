"""Engine-scoped replay audit: native constants stay fixed, hybrid never inherits the native overlay."""
import json
from pathlib import Path
import tempfile
import unittest

import mainnet_replay_ci

ROOT = Path(__file__).resolve().parents[2]
GET_BYTES = 233123
COMMIT_MEMBERS = 707
CHECKS = 507
COMMITS = 9


def per_repetition_samples():
    rows = []
    quotient, remainder = divmod(GET_BYTES, CHECKS)
    for index in range(CHECKS):
        rows.append({"operation": "get", "members": 1,
                     "bytes": quotient + (1 if index < remainder else 0)})
    quotient, remainder = divmod(COMMIT_MEMBERS, COMMITS)
    for index in range(COMMITS):
        rows.append({"operation": "commit", "members": quotient + (1 if index < remainder else 0),
                     "bytes": 0})
    return rows


def write_run(directory, repetitions, counts):
    result = Path(directory) / "result"
    result.mkdir(parents=True)
    calls = repetitions * mainnet_replay_ci.CALLS_PER_REPETITION
    profile = {
        "rejected_calls": 0, "timed_out_calls": 0,
        "scope": "archive-derivable node KV subset",
        "offered_calls": calls, "successful_calls": calls,
        "tables": {"get:objects": {"members": repetitions * CHECKS,
                                   "value_bytes": repetitions * GET_BYTES}},
        "measurement_elapsed_seconds": 1.25,
        "checkpoints_per_second": 1.0, "logical_ops_per_second": 2.0, "api_calls_per_second": 3.0,
        "latency": {
            "operation:get:success": {"api": {"count": calls - repetitions * COMMITS,
                                              "p50_ns": 1, "p99_ns": 2}},
            "operation:commit:success": {"api": {"count": repetitions * COMMITS,
                                                 "p50_ns": 3, "p99_ns": 4}},
        },
    }
    summary = {"status": "completed", "phase_counts": {
        "run_status": "completed", "error": None, "unknown_outcomes": 0,
        "unresolved_workers": 0, "counts": counts, "resources": {}, "profile": profile}}
    manifest = {"status": "completed", "errors_or_wrong_values": 0,
                "timeouts_or_unknown_mutations": 0, "raw_samples_complete": True,
                "raw_sample_writer_error": None, "completed_operations": calls,
                "succeeded_operations": calls}
    (result / "summary.json").write_text(json.dumps(summary))
    (result / "manifest.json").write_text(json.dumps(manifest))
    header = ("phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,"
              "api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id,"
              "released_ns,stream,source_checkpoint,source_tx,repetition,db,table,batch_phase,"
              "parent_batch,members\n")
    lines = [header]
    for repetition in range(repetitions):
        for index, row in enumerate(per_repetition_samples()):
            lines.append(f"measurement,{row['operation']},success,{index},{index},true,{index},1,1,1,"
                         f"1,{row['bytes']},,,,closed,345683547,,{repetition},0,,,,{row['members']}\n")
    (result / "samples.csv").write_text("".join(lines))
    return result


def counts_for(engine, repetitions):
    return {key: repetitions * count
            for key, count in mainnet_replay_ci.PER_REPEAT[engine].items()}


class EngineScopeTests(unittest.TestCase):
    def test_native_counters_are_unchanged(self):
        self.assertEqual(mainnet_replay_ci.PER_REPEAT["native"],
                         dict(store_gets=507, overlay_demands=116, put_members=707,
                              write_calls=9, checkpoints=1, transactions=45))

    def test_hybrid_counters_are_pinned_without_the_native_overlay(self):
        self.assertEqual(mainnet_replay_ci.PER_REPEAT["hybrid"],
                         dict(store_gets=507, put_members=707,
                              write_calls=9, checkpoints=1, transactions=45))
        self.assertNotIn("overlay_demands", mainnet_replay_ci.PER_REPEAT["hybrid"])
        self.assertEqual(mainnet_replay_ci.ENGINES, ("native", "hybrid"))

    def test_audit_accepts_both_engines(self):
        with tempfile.TemporaryDirectory() as temporary:
            native = write_run(Path(temporary) / "native", 2, counts_for("native", 2))
            report = mainnet_replay_ci.audit(native, 2, "native")
            self.assertEqual(report["status"], "completed")
            self.assertIn("116 per repetition", report["overlay_demands"])
            hybrid_counts = counts_for("hybrid", 2)
            hybrid_counts["overlay_demands"] = 999
            hybrid = write_run(Path(temporary) / "hybrid", 2, hybrid_counts)
            report = mainnet_replay_ci.audit(hybrid, 2, "hybrid")
            self.assertEqual(report["status"], "completed")
            self.assertNotIn("116 per repetition", report["overlay_demands"])

    def test_native_still_gates_the_native_overlay(self):
        counts = counts_for("native", 2)
        counts["overlay_demands"] = 231
        with tempfile.TemporaryDirectory() as temporary:
            result = write_run(Path(temporary), 2, counts)
            with self.assertRaisesRegex(ValueError, "overlay_demands for native"):
                mainnet_replay_ci.audit(result, 2, "native")

    def test_hybrid_never_gates_the_native_overlay(self):
        counts = counts_for("hybrid", 2)
        counts["overlay_demands"] = 0
        with tempfile.TemporaryDirectory() as temporary:
            result = write_run(Path(temporary), 2, counts)
            self.assertEqual(mainnet_replay_ci.audit(result, 2, "hybrid")["status"], "completed")
        counts = counts_for("hybrid", 2)
        counts["overlay_demands"] = 7
        mainnet_replay_ci.audit_counts(counts, 2, "hybrid")

    def test_wrong_hybrid_counter_is_refused(self):
        counts = counts_for("hybrid", 2)
        counts["checkpoints"] = 4
        with self.assertRaisesRegex(ValueError, "checkpoints for hybrid"):
            mainnet_replay_ci.audit_counts(counts, 2, "hybrid")

    def test_missing_counter_is_refused(self):
        counts = counts_for("hybrid", 2)
        del counts["transactions"]
        with self.assertRaisesRegex(ValueError, "transactions for hybrid"):
            mainnet_replay_ci.audit_counts(counts, 2, "hybrid")

    def test_unknown_engine_is_refused(self):
        with self.assertRaisesRegex(ValueError, "unknown replay engine"):
            mainnet_replay_ci.audit_counts({}, 2, "sidecar")

    def test_hybrid_run_requires_the_leased_document(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "remote-config"):
                mainnet_replay_ci.backend_config("hybrid", root, False, None)
            document = root / "remote.json"
            document.write_text(json.dumps({"databases": [{"db_id": 0, "options": {}}]}))
            backend = mainnet_replay_ci.backend_config("hybrid", root, False, document)
            self.assertEqual(backend["kind"], "tidehunter_tcp")
            self.assertEqual(backend["options"]["databases"][0]["db_id"], 0)
            with self.assertRaisesRegex(ValueError, "leased databases"):
                empty = root / "empty.json"
                empty.write_text(json.dumps({"databases": []}))
                mainnet_replay_ci.backend_config("hybrid", root, False, empty)
            native = mainnet_replay_ci.backend_config("native", root, False, document)
            self.assertEqual(native["kind"], "native")
            self.assertEqual(native["options"]["native_wal_geometry"], "small")

    def test_mainnet_replay_job_keeps_the_native_gate(self):
        workflow = (ROOT / ".github/workflows/rust-tests.yaml").read_text()
        self.assertIn("mainnet_replay_ci.py correctness", workflow)
        self.assertNotIn("--engine hybrid", workflow)
        self.assertIn("python3 -B -m unittest discover -s .github/scripts "
                      "-p 'test_mainnet_replay_engine.py'", workflow)


if __name__ == "__main__":
    unittest.main()
