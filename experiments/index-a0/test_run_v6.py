"""run_v6.py writes --tree into every run config, and only runs carrying that
revision produce a passing receipt."""

from __future__ import annotations

import json
import stat
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_v6  # noqa: E402
import v6_receipt  # noqa: E402

TREE = "c4909d7+land3-11f58282+op-replay-aaaaaaaa+index-a0-bbbbbbbb+index-a1-runner-cccccccc"

# Stand-in for kv-research: writes the three files a real V6 run leaves, with
# the config it was given (as the real manifest does) and its own BLAKE2b-512.
FAKE = """#!{python}
import hashlib, json, sys
from pathlib import Path
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
config = json.loads(Path(args["--config"]).read_text())
if config.get("fake_drop_revision"):
    config.pop("source_revision", None)
out = Path(args["--output"]); out.mkdir(parents=True)
me = hashlib.blake2b(Path(sys.argv[0]).read_bytes()).hexdigest()
(out / "manifest.json").write_text(json.dumps({{"schema_version": 1, "run": {{"binary": {{"blake2b512": me}}, "config": config}}}}))
(out / "summary.json").write_text(json.dumps({{"status": "completed", "durability": {{"tier": config["fake_tier"]}}}}))
gate = {{"eligible": config["diagnostics"]["index_checkpoint"] is True, "reasons": [],
         "reconciliation": {{"abs_residual_ratio": config["fake_ratio"]}}}}
(out / "write-diagnostics.json").write_text(json.dumps({{"index_checkpoint": {{"device_reconciliation": gate}}}}))
sys.exit(config.get("fake_exit", 0))
"""


class Controller(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = self.root / "kv-research"
        self.binary.write_text(FAKE.format(python=sys.executable))
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        self.case = 0

    def tearDown(self) -> None:
        self.temp.cleanup()

    def config(self, tier: str, **extra) -> Path:
        path = self.root / f"base-{tier}-{self.case}.json"
        path.write_text(json.dumps({"source_revision": "stale", "fake_tier": tier, "fake_ratio": 0.05, **extra}))
        return path

    def control(self, native: dict | None = None, tree: str = TREE) -> tuple[int, dict]:
        self.case += 1
        work = self.root / f"work-{self.case}"
        status = run_v6.main(
            [
                "--tree", tree, "--binary", str(self.binary), "--work", str(work),
                "--config-native", str(self.config("native", **(native or {}))),
                "--config-synchronous", str(self.config("synchronous")),
            ]
        )
        return status, json.loads((work / "v6-receipt.json").read_text())

    def test_controller_output_produces_a_passing_receipt(self) -> None:
        status, receipt = self.control()
        self.assertEqual(status, 0, receipt["failures"])
        self.assertTrue(receipt["passed"])
        self.assertEqual(receipt["tree"], TREE)
        self.assertEqual(receipt["tiers_covered"], ["native", "synchronous"])
        self.assertEqual(receipt["binary_sha256"], v6_receipt.sha256(self.binary))
        # The stale base revision was replaced by --tree in the written config.
        written = json.loads((self.root / "work-1" / "native" / "config.json").read_text())
        self.assertEqual(written["source_revision"], TREE)
        self.assertTrue(written["diagnostics"]["index_checkpoint"])

    def test_missing_or_other_revision_fails(self) -> None:
        status, receipt = self.control(native={"fake_drop_revision": True})
        self.assertEqual(status, 1)
        self.assertIn("config.source_revision differs from --tree", json.dumps(receipt["failures"]))
        # A receipt for another tree rejects runs produced for TREE.
        status, receipt = self.control()
        self.assertEqual(status, 0, receipt["failures"])
        good = [self.root / f"work-{self.case}" / tier / "output" for tier in ("native", "synchronous")]
        self.assertTrue(v6_receipt.build_receipt(TREE, self.binary, good, [])["passed"])
        self.assertFalse(v6_receipt.build_receipt("another-tree", self.binary, good, [])["passed"])

    def test_failed_run_and_empty_tree_fail(self) -> None:
        status, receipt = self.control(native={"fake_exit": 3})
        self.assertEqual(status, 1)
        self.assertIn("native run exited with status 3", json.dumps(receipt["failures"]))
        with self.assertRaises(SystemExit):
            run_v6.main(["--tree", "", "--binary", str(self.binary), "--work", str(self.root / "w"),
                         "--config-native", str(self.config("native")), "--config-synchronous", str(self.config("synchronous"))])


if __name__ == "__main__":
    unittest.main()
