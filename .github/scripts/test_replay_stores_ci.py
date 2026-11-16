import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import replay_stores_ci as ci


def artifact(executable="/tmp/tests"):
    return json.dumps({"reason": "compiler-artifact", "target": {"name": "kv_research", "kind": ["lib"]},
                       "profile": {"test": True}, "executable": executable})


class ReplayStoresTests(unittest.TestCase):
    def test_test_binary_requires_unique_library(self):
        self.assertEqual(ci.test_binary(artifact()), Path("/tmp/tests"))
        for messages in ("", artifact() + "\n" + artifact("/tmp/other")):
            with self.assertRaises(ValueError):
                ci.test_binary(messages)

    def exercise(self, failure=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            messages = root / "build.jsonl"
            messages.write_text(artifact())
            calls = []

            def execute(command, **kwargs):
                calls.append(command)
                self.assertTrue(kwargs["check"])
                self.assertGreater(kwargs["timeout"], 0)
                if failure and "mainnet_replay_tikv_cql.py" in str(command):
                    raise subprocess.CalledProcessError(1, command)

            with patch.object(ci.subprocess, "run", side_effect=execute):
                if failure:
                    with self.assertRaises(subprocess.CalledProcessError):
                        ci.run(Path("/binary"), messages, root / "output")
                else:
                    ci.run(Path("/binary"), messages, root / "output")
            receipt = json.loads((root / "output/receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed" if failure else "completed")
            self.assertIn("not executed", receipt["rdma_execution"])
            qualifiers = calls[5:]
            self.assertEqual(len(qualifiers), 1 if failure else 2)
            self.assertIn("mainnet_replay_tikv_cql.py", qualifiers[0][2])
            if not failure:
                self.assertIn("mainnet_replay_fdb_rdma.py", qualifiers[1][2])
                self.assertIn("--test-binary", qualifiers[1])

    def test_qualifiers_are_sequential_and_receive_test_binary(self):
        self.exercise()

    def test_failure_preserves_receipt_and_stops_next_lifecycle(self):
        self.exercise(failure=True)


if __name__ == "__main__":
    unittest.main()
