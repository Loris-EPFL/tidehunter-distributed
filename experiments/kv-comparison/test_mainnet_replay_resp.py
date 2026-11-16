import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local
import mainnet_replay_resp as replay


class MainnetRespTests(unittest.TestCase):
    def test_cached_images_never_pull_and_missing_image_fails(self):
        spec = {"services": {"server": {"image": local.IMAGES["garnet"]}}}
        observed = [{"Id": "cached", "RepoDigests": ["pinned"]}]
        with patch.object(local, "compose") as compose, patch.object(
                local, "command", return_value=json.dumps(observed)) as command:
            result = local.resolve_images({}, spec, cached_only=True)
            self.assertEqual(result[0]["image_id"], "cached")
            command.assert_called_once_with(["docker", "image", "inspect", local.IMAGES["garnet"]])
            compose.assert_not_called()
        with patch.object(local, "compose") as compose, patch.object(
                local, "command", side_effect=RuntimeError("image missing")):
            with self.assertRaisesRegex(RuntimeError, "missing"):
                local.resolve_images({}, spec, cached_only=True)
            compose.assert_not_called()

    def test_replay_configuration_is_bounded_and_uses_unmodified_fixture(self):
        for concurrency in (1, 4):
            config = replay.configuration("garnet", {"cluster": True}, concurrency)
            self.assertEqual(config["workload"]["repetitions"], 2)
            self.assertEqual(config["workload"]["minimum_measurement_seconds"], 0)
            self.assertEqual(config["execution"]["concurrency"], concurrency)
            self.assertEqual(config["execution"]["request_timeout_ms"], 10000)
            self.assertEqual(config["limits"]["deadline_seconds"], 60)
            self.assertEqual(config["workload"]["manifest_sha256"], replay.MANIFEST_SHA)
            self.assertNotIn("dataset", config)

    def test_result_admission_requires_complete_raw_samples_and_final_rows(self):
        profile = {"functional_qualification": True,
                   "final_verification": {"full_values": True, "empty_marker_table_verified": True,
                                          "fresh_empty_database_admitted": True,
                                          "table_rows": [0] * 17, "enumerated_physical_rows": 1800,
                                          "verified_rows": 1800},
                   "replay_contract": {}, "physical": {}, "measurement_elapsed_seconds": 1}
        summary = {"status": "completed", "phase_counts": {
            "counts": replay.EXPECTED.copy(), "unknown_outcomes": 0, "unresolved_workers": 0,
            "profile": profile, "resources": {"prepared_placement": {}}}}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with (output / "samples.csv").open("w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["operation", "outcome"])
                writer.writerows([("get", "success")] * 1014 + [("commit", "success")] * 18)
            (output / "summary.json").write_text(json.dumps(summary))
            self.assertEqual(replay.check_result(output)["samples"], 1032)
            for mutate in ("unknown", "counts", "marker", "rows", "failed"):
                changed = json.loads(json.dumps(summary))
                if mutate == "unknown": changed["phase_counts"]["unknown_outcomes"] = 1
                if mutate == "counts": changed["phase_counts"]["counts"]["put_members"] -= 1
                if mutate == "marker": changed["phase_counts"]["profile"]["final_verification"]["table_rows"][12] = 1
                if mutate == "rows": changed["phase_counts"]["profile"]["final_verification"]["enumerated_physical_rows"] -= 1
                if mutate == "failed": changed["status"] = "failed"
                (output / "summary.json").write_text(json.dumps(changed))
                with self.subTest(mutate=mutate), self.assertRaises(RuntimeError):
                    replay.check_result(output)
            (output / "summary.json").write_text(json.dumps(summary))
            with (output / "samples.csv").open("a") as file:
                file.write("get,success\n")
            with self.assertRaisesRegex(RuntimeError, "raw replay"):
                replay.check_result(output)


if __name__ == "__main__":
    unittest.main()
