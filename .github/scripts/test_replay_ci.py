import json
import os
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
                    patch.object(distributed, "fabric_environment", return_value=dict(os.environ)), \
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


if __name__ == "__main__":
    unittest.main()
