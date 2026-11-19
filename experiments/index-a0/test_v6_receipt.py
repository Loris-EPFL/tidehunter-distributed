"""The V6 receipt: bound to the measured binary, tree and both tiers, and
accepted by analyze_a1.v6_gate only when it passes."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v6_receipt  # noqa: E402

A1_DIR = Path(__file__).resolve().parents[1] / "index-a1"
TREE = "c4909d7+land3-11f58282+op-replay-aaaaaaaa+index-a0-bbbbbbbb+index-a1-runner-cccccccc"


class Receipt(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = self.root / "kv-research"
        self.binary.write_bytes(b"\x7fELF measured binary")
        self.other = self.root / "kv-research-other"
        self.other.write_bytes(b"\x7fELF another binary")
        self.runs = 0

    def tearDown(self) -> None:
        self.temp.cleanup()

    def run_dir(
        self,
        tier: str,
        ratio: float | None = 0.04,
        eligible: bool = True,
        binary: Path | None = None,
        tree: str = TREE,
        status: str = "completed",
    ) -> Path:
        """One kv-research output directory as the V6 procedure leaves it."""
        self.runs += 1
        run = self.root / f"run-{self.runs}"
        run.mkdir()
        gate = {"eligible": eligible, "reasons": [] if eligible else ["syncfs failed or no coordinator path"], "reconciliation": {}}
        if ratio is not None:
            gate["reconciliation"]["abs_residual_ratio"] = ratio
        (run / "write-diagnostics.json").write_text(json.dumps({"schema_version": 1, "index_checkpoint": {"device_reconciliation": gate}}))
        blake = hashlib.blake2b((binary or self.binary).read_bytes()).hexdigest()
        manifest = {"schema_version": 1, "run": {"binary": {"blake2b512": blake}, "config": {"source_revision": tree}}}
        (run / "manifest.json").write_text(json.dumps(manifest))
        (run / "summary.json").write_text(json.dumps({"status": status, "durability": {"tier": tier}}))
        return run

    def receipt(self, *runs: Path, rejections: list[str] | None = None) -> tuple[Path, dict]:
        output = self.root / f"v6-{self.runs}.json"
        status = v6_receipt.main(
            ["--tree", TREE, "--binary", str(self.binary), "--output", str(output)]
            + [argument for reason in rejections or [] for argument in ("--reject", reason)]
            + [str(run) for run in runs]
        )
        value = json.loads(output.read_text())
        self.assertEqual(status == 0, value["passed"])
        return output, value

    def both_tiers(self, **native) -> list[Path]:
        return [self.run_dir("native", **native), self.run_dir("synchronous", ratio=0.07)]

    def test_passes_only_with_bound_complete_evidence(self) -> None:
        _, value = self.receipt(*self.both_tiers())
        self.assertTrue(value["passed"], value["failures"])
        self.assertEqual(value["max_abs_residual_ratio"], 0.07)
        self.assertEqual(value["binary_sha256"], v6_receipt.sha256(self.binary))
        self.assertEqual(value["tree"], TREE)
        self.assertEqual(value["tiers_covered"], ["native", "synchronous"])
        failing = {
            "one tier only": [self.run_dir("native")],
            "other binary": self.both_tiers(binary=self.other),
            "other tree": self.both_tiers(tree="other-tree"),
            "ineligible gate": self.both_tiers(eligible=False),
            "residual": self.both_tiers(ratio=0.11),
            "no ratio": self.both_tiers(ratio=None),
            "failed run": self.both_tiers(status="failed"),
            "no runs dir content": [self.root / "missing", self.run_dir("synchronous")],
        }
        for case, runs in failing.items():
            with self.subTest(case):
                value = self.receipt(*runs)[1]
                self.assertFalse(value["passed"])
                self.assertTrue(value["failures"])
        self.assertFalse(self.receipt(*self.both_tiers(), rejections=["idle gate wrote 3%"])[1]["passed"])

    def test_receipt_satisfies_analyze_a1_gate(self) -> None:
        if not (A1_DIR / "analyze_a1.py").is_file():
            self.skipTest("experiments/index-a1 (index-a1-runner patch) not applied")
        sys.path.insert(0, str(A1_DIR))
        import analyze_a1  # noqa: PLC0415

        manifest = {"schema_version": 1, "tree": TREE, "binary_sha256": v6_receipt.sha256(self.binary)}
        passing, _ = self.receipt(*self.both_tiers())
        gate = analyze_a1.v6_gate(passing, manifest)
        self.assertTrue(gate["passed"], gate)
        self.assertEqual(gate["max_abs_residual_ratio"], 0.07)
        foreign, value = self.receipt(*self.both_tiers(binary=self.other))
        self.assertFalse(value["passed"])
        self.assertFalse(analyze_a1.v6_gate(foreign, manifest)["passed"])
        self.assertFalse(analyze_a1.v6_gate(passing, dict(manifest, tree="another-tree"))["passed"])


if __name__ == "__main__":
    unittest.main()
