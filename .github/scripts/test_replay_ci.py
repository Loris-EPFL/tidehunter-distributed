import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
import campaign
import mainnet_replay_resp as replay
import distributed_mainnet_ci as distributed
import kernel_nvme_tcp_ci as kernel


def write_cleanup_state(state, proof, prewrite=None, before=None):
    """Write the inventory receipt and the proof files that cleanup reads."""
    kernel.dump(state / "before.json", {} if before is None else before)
    for name, value in (("proof", proof), ("proof-prewrite", prewrite)):
        if value is None:
            continue
        (state / name).mkdir()
        kernel.dump(state / name / "proof.json", value)


class ReplayCITests(unittest.TestCase):
    def test_campaign_replay_is_opt_in(self):
        for flag, expected in (([], False), (["--mainnet-replay-resp"], True)):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as temporary:
                argv = ["campaign.py", "--manifest", str(ROOT / "experiments/kv-comparison/ci.json"),
                        "--backends", "valkey_tcp", "--binary", sys.executable,
                        "--output", str(Path(temporary) / "campaign"), *flag]
                with patch.object(sys, "argv", argv), patch.object(campaign, "source_snapshot"), \
                        patch.object(campaign, "run_one") as run:
                    campaign.main()
                self.assertTrue(run.called)
                self.assertEqual(run.call_args.args[0].mainnet_replay_resp, expected)

    def test_replay_reuses_state_and_records_every_reset(self):
        state = {"backend": "valkey_tcp", "backend_options": {"nodes": ["127.0.0.1:6380"]}}
        result = {"verification": {"hash": "same-full-content"}}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(campaign.local, "inspect_owned") as inspect, \
                patch.object(replay.industry_service, "request", return_value="OK") as request, \
                patch.object(replay, "check_nonempty_rejection", return_value={"nonempty_rejected": True}) as freshness, \
                patch.object(replay, "check_result", return_value=result) as checked, \
                patch.object(campaign.subprocess, "run") as child, \
                patch.object(campaign.local, "start") as start, patch.object(campaign.local, "stop") as stop:
            output = Path(temporary) / "replay"
            evidence = campaign.qualify_mainnet_resp(Path("/actual/binary"), state, output)
            configs = [json.loads((output / f"N{n}.json").read_text()) for n in (1, 4)]
            self.assertEqual([c["execution"]["concurrency"] for c in configs], [1, 4])
            self.assertTrue(all(c["workload"]["repetitions"] == 2 for c in configs))
            self.assertEqual(evidence["reset"]["acknowledged_flushes"], ["127.0.0.1:6380"] * 3)
            self.assertEqual(inspect.call_count, 3)
            self.assertEqual(request.call_count, 3)
            freshness.assert_called_once()
            self.assertEqual(checked.call_count, 2)
            self.assertTrue(all(call.kwargs["check"] for call in child.call_args_list))
            self.assertTrue(all(call.kwargs["timeout"] == 75 for call in child.call_args_list))
            start.assert_not_called()
            stop.assert_not_called()

    def test_failed_reset_stops_before_replay(self):
        state = {"backend": "kvrocks", "backend_options": {"nodes": ["127.0.0.1:6380"]}}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(campaign.local, "inspect_owned"), \
                patch.object(replay.industry_service, "request", return_value="ERROR"), \
                patch.object(replay, "check_nonempty_rejection") as freshness:
            with self.assertRaisesRegex(RuntimeError, "reset failed"):
                campaign.qualify_mainnet_resp(Path("/actual/binary"), state, Path(temporary) / "replay")
            freshness.assert_not_called()

    def test_distributed_selection_gates_only_the_leased_replay_module(self):
        for passed in (6, 7):
            with self.subTest(passed=passed), patch.object(distributed, "cargo", return_value=(
                    f"test result: ok. {passed} passed; 0 failed; 0 ignored; 100 filtered out;")) as cargo:
                self.assertEqual(distributed.main(), 0)
                cargo.assert_called_once()
                self.assertEqual(cargo.call_args.args[6:],
                                 ("--lib", "backends::tidehunter::replay", "--", "--nocapture"))

    def test_distributed_selection_fails_closed_on_empty_or_failed_output(self):
        for output in ("", "test result: ok. 0 passed; 0 failed; 0 ignored;",
                       "test result: FAILED. 6 passed; 1 failed; 0 ignored;"):
            with self.subTest(output=output), patch.object(distributed, "cargo", return_value=output):
                with self.assertRaisesRegex(RuntimeError, "nonempty passing suite"):
                    distributed.main()

    def test_kernel_termination_enters_maintained_cleanup(self):
        with patch.object(kernel.signal, "signal") as handler:
            with self.assertRaisesRegex(RuntimeError, "interrupted by signal"):
                kernel.interrupted(15, None)
            self.assertEqual(handler.call_count, 2)

    def test_kernel_live_qualification_gates_wal_replay_and_cleanup(self):
        import argparse
        import local_dry_run_targets
        for complete, clean, restored in ((True, True, True), (False, True, True),
                                          (False, False, True), (True, True, False)):
            with self.subTest(complete=complete, clean=clean, restored=restored), \
                    tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                args = argparse.Namespace(state=state, binary=state / "binary")
                calls = []

                def worker(output, binary, route, seconds, deadline, *, owner_wal_file_bytes):
                    self.assertEqual((binary, route), (args.binary, "tcp"))
                    self.assertEqual(owner_wal_file_bytes, 8 * 1024**2)
                    self.assertEqual(output.name, "wal_replay")
                    calls.append(output.name)
                    return ({"complete": complete, "fixture_removed": clean}, output / "evidence")

                with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(kernel, "inventory", side_effect=[{}, {} if restored else {"leak": True}, {}]):
                    if clean and restored:
                        self.assertEqual(kernel.qualify(args), 0 if complete else 1)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.qualify(args)
                expected = ["wal_replay"]
                self.assertEqual(calls, expected)
                summary = json.loads((state / "single-owner/summary.json").read_text())
                self.assertEqual(list(summary), expected)
                self.assertEqual(summary["wal_replay"]["inventory_restored"], restored)

    def test_prewrite_pass_gates_the_run(self):
        import argparse
        import local_multiowner_check
        import local_dry_run_targets
        original_root = local_dry_run_targets.ROOT
        for clean in (True, False):
            with self.subTest(clean=clean), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                binary = state / "binary"
                binary.touch()
                args = argparse.Namespace(state=state, binary=binary)
                calls = []
                def proof():
                    self.assertEqual(local_dry_run_targets.ROOT, [])
                    prewrite = "--wal-prewrite" in sys.argv
                    # Both passes state their choice; neither relies on the default.
                    self.assertNotEqual(prewrite, "--no-wal-prewrite" in sys.argv)
                    calls.append(prewrite)
                    output = Path(sys.argv[sys.argv.index("--output") + 1])
                    output.mkdir()
                    kernel.dump(output / "proof.json", {
                        "complete": True,
                        "cleanup_complete": clean if prewrite else True,
                        "fixture_removed": clean if prewrite else True,
                        "errors": [],
                    })
                    return 1 if prewrite else 0
                with patch.object(kernel, "inventory", return_value={}), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(sys, "argv", ["test"]), \
                        patch.object(local_multiowner_check, "main", side_effect=proof):
                    self.assertEqual(kernel.run(args), 1)
                    self.assertIs(local_dry_run_targets.ROOT, original_root)
                    self.assertEqual(calls, [False, True])
                    prewrite = json.loads((state / "prewrite-result.json").read_text())
                    self.assertTrue(prewrite["attempted"])
                    self.assertFalse(prewrite["passed"])
                    self.assertFalse(json.loads((state / "result.json").read_text())["passed"])
                    if clean:
                        self.assertEqual(kernel.cleanup(args), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.cleanup(args)
                        self.assertFalse(json.loads((state / "cleanup.json").read_text())["complete"])

    def test_both_passes_gate_the_run(self):
        import argparse
        import local_multiowner_check
        import local_dry_run_targets
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                self.assertEqual(local_dry_run_targets.ROOT, [])
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 0)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertTrue(json.loads((state / "result.json").read_text())["passed"])

    def test_changed_final_inventory_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", side_effect=[{}, {}, {"leak": True}]), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertEqual(prewrite["defect"],
                                 "last pass left the host inventory changed")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_prewrite_proof_fixture_removal_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                prewrite = "--wal-prewrite" in sys.argv
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": not prewrite, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertEqual(prewrite["defect"],
                                 "proof-prewrite proof fixture_removed is not true")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_missing_prewrite_proof_gates_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                if "--wal-prewrite" not in sys.argv:
                    output = Path(sys.argv[sys.argv.index("--output") + 1])
                    output.mkdir()
                    kernel.dump(output / "proof.json", {
                        "complete": True, "cleanup_complete": True,
                        "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertTrue(prewrite["attempted"])
                self.assertTrue(prewrite["passed"])
                self.assertIn("proof-prewrite proof missing", prewrite["defect"])
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_proof_errors_gate_the_run(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": ["kernel log interval incomplete"]})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_first_pass_failure_gates_even_when_prewrite_passes(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            args = argparse.Namespace(state=state, binary=binary)
            def proof():
                prewrite = "--wal-prewrite" in sys.argv
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": True, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0 if prewrite else 1
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(args), 1)
                self.assertTrue(json.loads((state / "prewrite-result.json").read_text())["passed"])
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])

    def test_prewrite_pass_does_not_start_after_incomplete_cleanup(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            def proof():
                output = state / "proof"
                output.mkdir()
                kernel.dump(output / "proof.json", {"cleanup_complete": False})
                return 1
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof) as run:
                self.assertEqual(kernel.run(argparse.Namespace(state=state, binary=binary)), 1)
                run.assert_called_once()
                self.assertFalse(json.loads((state / "prewrite-result.json").read_text())["attempted"])


    def test_cleanup_accepts_an_incomplete_proof_with_a_clean_teardown(self):
        import argparse
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            write_cleanup_state(state, {"complete": False, "cleanup_complete": True,
                                        "fixture_removed": True,
                                        "errors": ["admission benchmark failed"]})
            with patch.object(kernel, "inventory", return_value={}):
                self.assertEqual(kernel.cleanup(argparse.Namespace(state=state)), 0)
            cleanup = json.loads((state / "cleanup.json").read_text())
            self.assertEqual(cleanup, {"complete": True, "defects": [],
                                       "inventory_restored": True})

    def test_cleanup_rejects_teardown_defects_only(self):
        import argparse
        for proof, defect in (
                ({"complete": True, "cleanup_complete": False, "fixture_removed": True,
                  "errors": []}, "proof proof cleanup_complete is not true"),
                ({"complete": False, "cleanup_complete": True, "fixture_removed": False,
                  "errors": ["admission benchmark failed"]},
                 "proof proof fixture_removed is not true")):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                write_cleanup_state(state, proof)
                with patch.object(kernel, "inventory", return_value={}):
                    with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                        kernel.cleanup(argparse.Namespace(state=state))
                cleanup = json.loads((state / "cleanup.json").read_text())
                self.assertFalse(cleanup["complete"])
                self.assertEqual(cleanup["defects"], [defect])

    def test_cleanup_rejects_a_missing_proof(self):
        import argparse
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            write_cleanup_state(state, None)
            with patch.object(kernel, "inventory", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                    kernel.cleanup(argparse.Namespace(state=state))
            cleanup = json.loads((state / "cleanup.json").read_text())
            self.assertFalse(cleanup["complete"])
            self.assertTrue(cleanup["defects"][0].startswith("proof proof missing"))

    def test_cleanup_uses_teardown_evidence_for_the_prewrite_pass(self):
        import argparse
        clean = {"complete": True, "cleanup_complete": True,
                 "fixture_removed": True, "errors": []}
        incomplete = {"complete": False, "cleanup_complete": True,
                      "fixture_removed": True, "errors": ["admission benchmark failed"]}
        for prewrite, complete in ((incomplete, True), (dict(clean, fixture_removed=False), False)):
            with self.subTest(complete=complete), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                write_cleanup_state(state, clean, prewrite)
                with patch.object(kernel, "inventory", return_value={}):
                    if complete:
                        self.assertEqual(kernel.cleanup(argparse.Namespace(state=state)), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.cleanup(argparse.Namespace(state=state))
                recorded = json.loads((state / "prewrite-cleanup.json").read_text())
                self.assertEqual(recorded, {"complete": complete})

    def test_run_still_rejects_an_incomplete_proof_with_a_clean_teardown(self):
        import argparse
        import local_multiowner_check
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.touch()
            def proof():
                output = Path(sys.argv[sys.argv.index("--output") + 1])
                output.mkdir()
                kernel.dump(output / "proof.json", {
                    "complete": False, "cleanup_complete": True,
                    "fixture_removed": True, "errors": []})
                return 0
            with patch.object(kernel, "inventory", return_value={}), \
                    patch.object(kernel.signal, "signal"), \
                    patch.object(sys, "argv", ["test"]), \
                    patch.object(local_multiowner_check, "main", side_effect=proof):
                self.assertEqual(kernel.run(argparse.Namespace(state=state, binary=binary)), 1)
                prewrite = json.loads((state / "prewrite-result.json").read_text())
                self.assertFalse(prewrite["attempted"])
                self.assertEqual(prewrite["defect"], "proof proof complete is not true")
                self.assertFalse(json.loads((state / "result.json").read_text())["passed"])


if __name__ == "__main__":
    unittest.main()
