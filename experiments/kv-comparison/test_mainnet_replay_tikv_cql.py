import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local
import mainnet_replay_tikv_cql as replay


class MainnetTikvCqlTests(unittest.TestCase):
    def test_guardrail_is_opt_in_and_keeps_warning(self):
        ordinary, _ = local.topology("cassandra", "qualification", Path("/tmp/owned"), "a" * 32)
        admitted, _ = local.topology("cassandra", "qualification", Path("/tmp/owned"), "a" * 32, replay=True)
        self.assertNotIn("entrypoint", ordinary["services"]["cassandra0"])
        server = admitted["services"]["cassandra0"]
        self.assertEqual(server["entrypoint"], ["/bin/bash", "-ec"])
        self.assertIn("batch_size_fail_threshold: 1MiB", server["command"][0])
        self.assertNotIn("batch_size_warn_threshold", server["command"][0])
        self.assertIn("docker-entrypoint.sh cassandra -f", server["command"][0])

    def test_options_require_captured_effective_guardrail(self):
        state = {"backend": "cassandra", "owner": "a" * 32}
        with tempfile.TemporaryDirectory() as directory, patch.object(local, "probe") as probe, patch.object(local, "execute") as execute:
            probe.return_value = {"nodes": [{"effective_yaml": "batch_size_fail_threshold: 50KiB\nbatch_size_warn_threshold: 5KiB\n"}]}
            with self.assertRaisesRegex(RuntimeError, "guardrails"):
                replay.options(state, Path(directory))
            probe.return_value["nodes"][0]["effective_yaml"] = "batch_size_fail_threshold: 1MiB\nbatch_size_warn_threshold: 5KiB\n"
            execute.return_value = "\n".join(json.dumps({"name": name, "value": value}) for name, value in {
                "batch_size_fail_threshold": "1MiB", "batch_size_warn_threshold": "5KiB",
                "commitlog_sync": "periodic", "commitlog_sync_period": "10000ms"}.items())
            options = replay.options(state, Path(directory))
            receipt = json.loads(Path(options["replay_server_config_receipt"]).read_text())
            self.assertEqual(receipt["batch_size_fail_threshold_bytes"], 1048576)
            self.assertEqual(receipt["owner"], state["owner"])
            self.assertEqual(options["consistency"], "quorum")
            self.assertEqual(receipt["effective_settings"][0]["node"], "127.0.0.11:9042")
            self.assertEqual(receipt["effective_settings"][0]["settings"]["commitlog_sync"], "periodic")
            probe.return_value["nodes"] *= 2
            execute.side_effect = [execute.return_value, execute.return_value.replace("1MiB", "50KiB")]
            with self.assertRaisesRegex(RuntimeError, "127.0.0.12"):
                replay.options(state, Path(directory))

    def test_tikv_and_cql_reuse_bounded_exact_replay_config(self):
        for store in replay.BACKENDS:
            for n in (1, 4):
                config = replay.configuration(store, {}, n)
                self.assertEqual(config["workload"]["repetitions"], 2)
                self.assertEqual(config["workload"]["minimum_measurement_seconds"], 0)
                self.assertEqual(config["execution"]["concurrency"], n)
                self.assertEqual(config["execution"]["request_timeout_ms"], 10000)
                self.assertEqual(config["limits"]["deadline_seconds"], 60)


if __name__ == "__main__":
    unittest.main()
