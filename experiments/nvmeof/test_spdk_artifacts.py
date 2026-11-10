import json
from pathlib import Path
import shutil
import tempfile
import unittest

import spdk_artifacts


class SpdkArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.prefix = Path(self.temp.name) / "prefix"
        (self.prefix / "bin").mkdir(parents=True)
        binary = self.prefix / "bin/nvmf_tgt"
        shutil.copyfile(Path(shutil.which("true")).resolve(), binary)
        binary.chmod(0o755)
        (self.prefix / "LICENSE.spdk").write_text("BSD-3-Clause fixture\n")
        self.metadata = {"schema_version": 1, "source_commit": spdk_artifacts.COMMIT,
                         "submodules": spdk_artifacts.SUBMODULES,
                         "configure_options": spdk_artifacts.CONFIGURE,
                         "builder_image": "sha256:" + "a" * 64,
                         "target_scope": "sealed_read_only_file_aio_direct",
                         "files": {name: spdk_artifacts.identity(self.prefix / name)
                                   for name in ("bin/nvmf_tgt", "LICENSE.spdk")}}

    def save(self):
        (self.prefix / "spdk.json").write_text(json.dumps(self.metadata))

    def test_reviewed_patch_file_has_expected_digest(self):
        patch = Path(spdk_artifacts.__file__).parent / "patches/spdk-26.01-nvmf-remove-ns-status.patch"
        self.assertEqual(spdk_artifacts.identity(patch)["sha256"], spdk_artifacts.PATCH_SHA256)

    def test_provider_override_is_rejected_even_with_matching_hash(self):
        (self.prefix / "lib").mkdir()
        provider = self.prefix / "lib/libibverbs.so.1"
        shutil.copyfile(self.prefix / "bin/nvmf_tgt", provider)
        self.metadata["files"]["lib/libibverbs.so.1"] = spdk_artifacts.identity(provider)
        self.save()
        with self.assertRaises(ValueError):
            spdk_artifacts.admit_prefix(self.prefix)

    def test_binary_tampering_is_rejected_after_sealing(self):
        self.save()
        binary = self.prefix / "bin/nvmf_tgt"
        binary.write_bytes(binary.read_bytes() + b"changed")
        with self.assertRaises(ValueError):
            spdk_artifacts.admit_prefix(self.prefix)

    def test_manifest_cannot_promote_read_only_comparison_to_live_wal(self):
        self.metadata["target_scope"] = "live_wal"
        self.save()
        with self.assertRaises(ValueError):
            spdk_artifacts.admit_prefix(self.prefix)

    def test_live_scope_requires_exact_reviewed_patch_identity(self):
        self.metadata["target_scope"] = "file_aio_direct_live_and_sealed"
        self.save()
        with self.assertRaisesRegex(ValueError, "reviewed namespace-drain patch"):
            spdk_artifacts.admit_prefix(self.prefix)
        self.metadata["source_patch_sha256"] = "0" * 64
        self.save()
        with self.assertRaisesRegex(ValueError, "reviewed namespace-drain patch"):
            spdk_artifacts.admit_prefix(self.prefix)
        self.metadata["source_patch_sha256"] = spdk_artifacts.PATCH_SHA256
        self.save()
        with self.assertRaisesRegex(ValueError, "retained build provenance"):
            spdk_artifacts.admit_prefix(self.prefix)
        (self.prefix / "build.log").write_text("clean build fixture\n")
        binary = self.metadata["files"]["bin/nvmf_tgt"]
        log = spdk_artifacts.identity(self.prefix / "build.log")
        self.metadata["version"] = "SPDK v26.01 git sha1 2ef883e"
        proof = {"complete": True, "builder_image": self.metadata["builder_image"],
                 "upstream_commit": spdk_artifacts.COMMIT, "source_patch_sha256": spdk_artifacts.PATCH_SHA256,
                 "binary_sha256": binary["sha256"], "binary_bytes": binary["bytes"],
                 "build_log_sha256": log["sha256"], "version": self.metadata["version"],
                 "commands": [["make", "clean"], ["make", "-j4"]],
                 "config_sha256": "1" * 64, "rpc_source_sha256": "2" * 64}
        (self.prefix / "build-proof.json").write_text(json.dumps(proof))
        self.metadata["files"].update({"build.log": log,
            "build-proof.json": spdk_artifacts.identity(self.prefix / "build-proof.json")})
        self.save()
        self.assertEqual(spdk_artifacts.admit_prefix(self.prefix)["source_patch_sha256"],
                         spdk_artifacts.PATCH_SHA256)

    def test_binary_symlink_cannot_escape_exact_prefix_inventory(self):
        self.save()
        binary = self.prefix / "bin/nvmf_tgt"
        original = Path(self.temp.name) / "outside-target"
        binary.rename(original)
        binary.symlink_to(original)
        with self.assertRaises(ValueError):
            spdk_artifacts.admit_prefix(self.prefix)

    def test_unrecorded_file_is_not_admitted_as_part_of_target(self):
        self.save()
        (self.prefix / "unrecorded.so").write_bytes(b"unadmitted provider")
        with self.assertRaises(ValueError):
            spdk_artifacts.admit_prefix(self.prefix)
