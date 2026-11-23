"""The hybrid manifest ID runs the leased hybrid engine on the ordinary-file fixture."""
import json
from pathlib import Path
import tempfile
import unittest

import campaign
import ci_prepare

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "experiments/kv-comparison/ci.json"
BASE = ROOT / "benchmarks/kv-research/example.json"
JOB = {"repeat": 0, "backend": "hybrid", "rate": 100,
       "profile": {"name": "api-qualification-only", "keys": 64, "aging_operations": 0}}


def spec():
    return json.loads(MANIFEST.read_text())


class HybridBackendTests(unittest.TestCase):
    def test_manifest_names_the_hybrid_beside_the_native_baseline(self):
        backends = spec()["backends"]
        self.assertEqual(backends[:2], ["native", "hybrid"])
        self.assertEqual(len(backends), len(set(backends)))
        self.assertTrue(set(backends) <= campaign.BACKENDS)
        self.assertIn("hybrid", campaign.BACKENDS)

    def test_matrix_keeps_every_declared_backend_inside_max_runs(self):
        jobs = campaign.matrix(spec())
        self.assertEqual({job["backend"] for job in jobs}, set(spec()["backends"]))
        self.assertEqual(len(jobs), spec()["max_runs"])
        self.assertTrue(any(job["backend"] == "hybrid" for job in jobs))

    def test_hybrid_config_uses_the_leased_fixture_route(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            options = campaign.backend_options("hybrid", None, output)
            self.assertEqual(options["transport"], {"kind": "tcp-trusted"})
            self.assertEqual(options["owners"], 1)
            self.assertEqual(options["durability_tier"], "native")
            self.assertEqual(options["max_batches_per_partition"], 256)
            self.assertEqual(options["batch_limits"], {"max_operations": 32,
                                                       "max_encoded_bytes": 320 * 1024})
            self.assertTrue(Path(options["path"]).is_absolute())
            self.assertNotIn("index", options)
            for native_only in ("durability", "native_wal_geometry", "native_shards"):
                self.assertNotIn(native_only, options)

    def test_hybrid_is_qualification_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "qualification"):
                campaign.build_config(json.loads(BASE.read_text()), JOB, None, output, 1775, False)

    def test_hybrid_qualification_runs_no_operations(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            config = campaign.build_config(json.loads(BASE.read_text()), JOB, None, output, 1775, True)
            self.assertTrue(config["qualification_only"])
            self.assertEqual(config["phases"], {"preload": False, "warmup_operations": 0,
                                                "aging_operations": 0, "measurement_operations": 0})

    def test_native_config_is_unchanged(self):
        job = dict(JOB, backend="native")
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            config = campaign.build_config(json.loads(BASE.read_text()), job, None, output, 1775, True)
            self.assertEqual(config["backend"]["kind"], "native")
            self.assertEqual(config["backend"]["options"]["native_wal_geometry"], "small")
            self.assertEqual(config["backend"]["options"]["durability"], "page_cache")

    def test_hybrid_acknowledgement_contract_stays_synced(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            options = campaign.backend_options("hybrid", None, output)
            self.assertNotIn("ack_contract", options)
            with self.assertRaises(ValueError):
                campaign.backend_options("hybrid", None, output, ack_contract="page_cache")

    def test_select_backends_admits_the_hybrid_id(self):
        selected = campaign.select_backends("hybrid", spec()["backends"], campaign.BACKENDS)
        self.assertEqual(selected, ["hybrid"])

    def test_hybrid_qualification_uses_the_native_scale_index_geometry(self):
        # Measured 2026-11-23: the production coordinator index geometry pre-faults
        # three 1 GiB writer-window fragments (3087 MiB RSS for zero operations);
        # the native-scale qualification geometry pre-faults three 1 MiB fragments
        # (19 MiB). Every backend keeps the single manifest memory budget.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            config = campaign.build_config(json.loads(BASE.read_text()), JOB, None, output, 1775, True)
            self.assertEqual(config["backend"]["options"]["index"],
                             {"frag_size": 1024 * 1024, "wal_file_size": 4 * 1024 * 1024})
            self.assertFalse(hasattr(campaign, "BACKEND_MEMORY_MIB"))
            self.assertFalse(hasattr(campaign, "memory_budget"))
            native = campaign.build_config(json.loads(BASE.read_text()), dict(JOB, backend="native"),
                                           None, output, 1775, True)
            self.assertNotIn("index", native["backend"]["options"])

    def test_cloud_package_admission_keeps_its_portable_set(self):
        # ci_prepare admits a packaged cloud qualification, which cannot run a
        # Linux test-utils fixture; the hybrid ID is local to the CI manifest.
        self.assertNotIn("hybrid", ci_prepare.BACKENDS)
        self.assertIn("native", ci_prepare.BACKENDS)


if __name__ == "__main__":
    unittest.main()
