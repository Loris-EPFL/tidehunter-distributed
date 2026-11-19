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

    def test_distributed_selection_fails_closed_and_allows_new_module_tests(self):
        success = lambda n: f"test result: ok. {n} passed; 0 failed; 0 ignored; 100 filtered out;"
        for output, passes in (([success(6), success(1)], True),
                               ([success(7), success(1)], True),
                               ([success(0)], False),
                               ([success(6), success(0)], False),
                               ([success(6), success(2)], False)):
            with self.subTest(output=output), \
                    patch.object(distributed, "cargo", side_effect=output):
                if passes:
                    self.assertEqual(distributed.main(), 0)
                else:
                    with self.assertRaises(RuntimeError):
                        distributed.main()

    def test_kernel_termination_enters_maintained_cleanup(self):
        with patch.object(kernel.signal, "signal") as handler:
            with self.assertRaisesRegex(RuntimeError, "interrupted by signal"):
                kernel.interrupted(15, None)
            self.assertEqual(handler.call_count, 2)

    def test_kernel_live_qualification_gates_both_history_modes_and_cleanup(self):
        import argparse
        import local_dry_run_targets
        for complete, clean, restored in ((True, True, True), (False, True, True),
                                          (False, False, True), (True, True, False)):
            with self.subTest(complete=complete, clean=clean, restored=restored), \
                    tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                args = argparse.Namespace(state=state, binary=state / "binary")
                calls = []

                def worker(output, binary, route, seconds, deadline, *,
                           history_persistence, owner_wal_file_bytes):
                    self.assertEqual((binary, route), (args.binary, "tcp"))
                    self.assertEqual(owner_wal_file_bytes, 8 * 1024**2)
                    self.assertEqual(output.name, history_persistence)
                    calls.append(history_persistence)
                    return ({"complete": complete or history_persistence == "wal_replay",
                             "fixture_removed": clean}, output / "evidence")

                with patch.object(local_dry_run_targets, "_kernel_live_worker", side_effect=worker), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(kernel, "inventory", side_effect=[{}, {} if restored else {"leak": True}, {}]):
                    if clean and restored:
                        self.assertEqual(kernel.qualify(args), 0 if complete else 1)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.qualify(args)
                expected = ["on_disk", "wal_replay"] if clean and restored else ["on_disk"]
                self.assertEqual(calls, expected)
                summary = json.loads((state / "single-owner/summary.json").read_text())
                self.assertEqual(list(summary), expected)
                self.assertEqual(summary["on_disk"]["inventory_restored"], restored)

    def test_prewrite_pass_is_non_gating_but_cleanup_is_required(self):
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
                        "cleanup_complete": clean if prewrite else True,
                        "fixture_removed": clean if prewrite else True,
                    })
                    return 1 if prewrite else 0
                with patch.object(kernel, "inventory", return_value={}), \
                        patch.object(kernel.signal, "signal"), \
                        patch.object(sys, "argv", ["test"]), \
                        patch.object(local_multiowner_check, "main", side_effect=proof):
                    self.assertEqual(kernel.run(args), 0)
                    self.assertIs(local_dry_run_targets.ROOT, original_root)
                    self.assertEqual(calls, [False, True])
                    self.assertFalse(json.loads((state / "prewrite-result.json").read_text())["passed"])
                    if clean:
                        self.assertEqual(kernel.cleanup(args), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                            kernel.cleanup(args)
                        self.assertFalse(json.loads((state / "cleanup.json").read_text())["complete"])

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


if __name__ == "__main__":
    unittest.main()
