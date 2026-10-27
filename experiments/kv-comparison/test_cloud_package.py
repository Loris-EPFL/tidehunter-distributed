"""Offline artifact-boundary tests; no Docker, compiler, AWS or database required."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import os
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("cloud_package", Path(__file__).with_name("cloud_package.py"))
package = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(package)


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        for name in package.REQUIRED:
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# fixture\n")
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Package Test", "-c", "user.email=package@example.invalid", "commit", "-qm", "fixture")

    def tearDown(self):
        self.temporary.cleanup()

    def git(self, *argv):
        return subprocess.check_output(["git", *argv], cwd=self.repo, text=True)

    def test_dirty_and_new_framework_bytes_not_unrelated_notes_or_credentials(self):
        dirty = "benchmarks/kv-research/src/main.rs"
        (self.repo / dirty).write_text("// exact dirty bytes\n")
        framework = self.repo / "experiments/kv-comparison/fresh.py"
        framework.write_text("# new controller support\n")
        (self.repo / "unrelated-notes.md").write_text("not a runtime dependency")
        (framework.parent / "credentials.json").write_text('{"password":"private"}')
        (framework.parent / "linked.py").symlink_to(framework)
        result = package.snapshot(self.repo, self.root / "snapshot")
        self.assertTrue(result["dirty"])
        self.assertIn("experiments/kv-comparison/fresh.py", result["sha256"])
        self.assertNotIn("unrelated-notes.md", result["sha256"])
        self.assertNotIn("experiments/kv-comparison/credentials.json", result["sha256"])
        self.assertNotIn("experiments/kv-comparison/linked.py", result["sha256"])
        with zipfile.ZipFile(self.root / "snapshot/source.zip") as archive:
            self.assertEqual(archive.read(dirty), b"// exact dirty bytes\n")

    def test_infiniband_preparation_never_enters_active_source_bundle(self):
        prep = self.repo / "experiments/valkey-infiniband/qualify.py"
        prep.parent.mkdir(parents=True)
        prep.write_text("# operator-only preparation\n")
        self.git("add", "experiments/valkey-infiniband/qualify.py")
        snapshot = package.snapshot(self.repo, self.root / "snapshot")
        self.assertNotIn("experiments/valkey-infiniband/qualify.py", snapshot["sha256"])
        with zipfile.ZipFile(self.root / "snapshot/source.zip") as archive:
            self.assertNotIn("experiments/valkey-infiniband/qualify.py", archive.namelist())

    def test_source_archive_is_reproducible_and_extracted_inventory_is_exact(self):
        first = package.snapshot(self.repo, self.root / "first")
        second = package.snapshot(self.repo, self.root / "second")
        self.assertEqual(first, second)
        self.assertEqual((self.root / "first/source.zip").read_bytes(), (self.root / "second/source.zip").read_bytes())
        target = self.root / "extracted"
        package.extract_source(self.root / "first", target)
        (target / "unexpected.py").write_text("# extra\n")
        with self.assertRaisesRegex(ValueError, "extra or missing"):
            package.admit_source(self.root / "first", target)

    def test_changed_extracted_source_and_directory_symlink_are_rejected(self):
        package.snapshot(self.repo, self.root / "snapshot")
        target = self.root / "extracted"
        package.extract_source(self.root / "snapshot", target)
        (target / "linked-directory").symlink_to(self.repo, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "extra or missing"):
            package.admit_source(self.root / "snapshot", target)
        (target / "linked-directory").unlink()
        (target / "Cargo.lock").write_text("changed")
        with self.assertRaisesRegex(ValueError, "extracted source differs"):
            package.admit_source(self.root / "snapshot", target)

    def test_private_key_contents_are_rejected_even_in_source_extension(self):
        path = self.repo / "experiments/kv-comparison/hidden.py"
        path.write_text("-----BEGIN " + "PRIVATE KEY-----\nprivate\n")
        with self.assertRaisesRegex(ValueError, "private credential"):
            package.snapshot(self.repo, self.root / "snapshot")

    def test_missing_required_helper_fails_before_snapshot_can_be_admitted(self):
        (self.repo / "experiments/kv-comparison/local.py").unlink()
        with self.assertRaisesRegex(ValueError, "required source closure"):
            package.snapshot(self.repo, self.root / "snapshot")

    def test_new_untracked_monitoring_closure_is_hash_bound_in_manual_package(self):
        names = sorted(name for name in package.REQUIRED if "/observability/" in name)
        self.git("rm", "--cached", "--", *names)
        for name in names:
            (self.repo / name).write_text("# exact new monitoring source: " + name + "\n")
        snapshot = package.snapshot(self.repo, self.root / "snapshot")
        with zipfile.ZipFile(self.root / "snapshot/source.zip") as archive:
            for name in names:
                self.assertIn(name, snapshot["sha256"])
                self.assertEqual(archive.read(name), (self.repo / name).read_bytes())
        missing = self.repo / "experiments/kv-comparison/observability/campaign_monitor.py"
        missing.unlink()
        with self.assertRaisesRegex(ValueError, "required source closure"):
            package.source_paths(self.repo)

    def test_archive_paths_and_links_fail_before_extraction(self):
        for name in ("../escape", "/absolute", "a/../escape", "a\\escape", "a//double"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                package.safe_name(name)
        archive = self.root / "unsafe.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            member = tarfile.TarInfo("source/link")
            member.type = tarfile.SYMTYPE
            member.linkname = "/etc/passwd"
            output.addfile(member)
        with self.assertRaisesRegex(ValueError, "unsafe/duplicate"):
            package.verify_archive(archive, package.identity(archive)["sha256"])

    def test_wrong_outer_hash_and_duplicate_archive_members_are_rejected(self):
        archive = self.root / "duplicate.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            for _ in range(2):
                member = tarfile.TarInfo("file")
                member.size = 1
                output.addfile(member, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "outer archive checksum"):
            package.verify_archive(archive, "0" * 64)
        with self.assertRaisesRegex(ValueError, "unsafe/duplicate"):
            package.verify_archive(archive, package.identity(archive)["sha256"])

    def test_zip_symlink_is_rejected_even_with_updated_inventory_digest(self):
        directory = self.root / "snapshot"
        metadata = package.snapshot(self.repo, directory)
        with zipfile.ZipFile(directory / "source.zip") as archive:
            contents = [(x, archive.read(x)) for x in archive.infolist()]
        with zipfile.ZipFile(directory / "source.zip", "w") as archive:
            for info, data in contents:
                if info.filename == "Cargo.lock":
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(info, data)
        metadata["snapshot_sha256"] = package.identity(directory / "source.zip")["sha256"]
        package.dump(directory / "source.json", metadata)
        with self.assertRaisesRegex(ValueError, "non-regular"):
            package.admit_source(directory)

    def test_ubuntu24_abi_and_soname_admission(self):
        library = self.root / "library"
        library.write_bytes(b"not executed")
        header = "ELF64 Advanced Micro Devices X86-64"
        for versions, dynamic in (("Version needs section GLIBC_2.40", ""),
                                  ("Version needs section GLIBCXX_3.4.34", ""),
                                  ("Version needs section OPENSSL_3.2.0", ""),
                                  ("", "(NEEDED) Shared library: [libssl.so.1.1]"),
                                  ("", "(RUNPATH) Library runpath: [/home/developer/lib]")):
            with self.subTest(versions=versions, dynamic=dynamic), patch.object(package, "command", side_effect=[header, dynamic, versions]):
                with self.assertRaises(ValueError):
                    package.elf_identity(library)
        with patch.object(package, "command", side_effect=[header, "(NEEDED) [libc.so.6]", "Version needs section GLIBC_2.39"]):
            self.assertEqual(package.elf_identity(library)["version_requirements"]["GLIBC"], ["2.39"])

    def test_newer_host_is_not_silently_packaged_as_portable(self):
        with patch.object(package.platform, "machine", return_value="x86_64"), patch.object(package, "command", return_value="glibc 2.40"):
            with self.assertRaisesRegex(ValueError, "container|Dockerfile"):
                package.build(self.root, self.root / "build", self.root, self.root)
        self.assertFalse((self.root / "build").exists())

    def test_native_client_bytes_are_bound_not_just_metadata(self):
        prefix = self.root / "client"
        (prefix / "lib").mkdir(parents=True)
        (prefix / "lib/libfdb_c.so").write_bytes(b"wrong native client")
        package.dump(prefix / "client.json", {"version": "7.3.77", "api": 730, "image": package.FDB_IMAGE,
                                               "sha256": package.FDB_SHA256, "blake2b512": package.FDB_BLAKE2B})
        with self.assertRaisesRegex(ValueError, "native client differs"):
            package.fdb_identity(prefix)

    def test_compile_environment_discards_native_rustflags(self):
        with patch.dict(package.os.environ, {"RUSTFLAGS": "-C target-cpu=native", "CARGO_ENCODED_RUSTFLAGS": "native",
                                            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS": "native",
                                            "LD_PRELOAD": "foreign.so", "CFLAGS": "-march=native"}):
            environment = package.clean_environment()
        self.assertNotIn("RUSTFLAGS", environment)
        self.assertNotIn("CARGO_ENCODED_RUSTFLAGS", environment)
        self.assertNotIn("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS", environment)
        self.assertNotIn("LD_PRELOAD", environment)
        self.assertEqual(environment["CFLAGS"], "-march=x86-64 -mtune=generic")

    def test_native_probe_failure_is_contained_and_preserves_child_diagnostics(self):
        library = self.root / "not-a-native-client.so"
        library.write_bytes(b"not an ELF shared library")
        with self.assertRaisesRegex(RuntimeError, r"native version probe exited 1; stderr=.*Traceback"):
            package.fdb_client_version(library)


class NativeClientAdmissionTests(unittest.TestCase):
    """Use the real hash-pinned library when available; never start FDB networking."""

    @classmethod
    def setUpClass(cls):
        directory = os.environ.get("FDB_CLIENT_LIB_DIR")
        cls.library = (Path(directory) if directory else
                       package.ROOT / "checkpoint-work/kv-comparison/fdb-client-7.3.77/lib") / "libfdb_c.so"
        if not cls.library.is_file():
            raise unittest.SkipTest("real pinned FDB7.3.77 native client unavailable; prepare-fdb or set FDB_CLIENT_LIB_DIR")
        if package.identity(cls.library)["sha256"] != package.FDB_SHA256:
            raise AssertionError("native regression fixture is not the admitted FDB7.3.77 library")
        cls.version = "7.3.77,3ea44ce1d9003ad095e408039e1f755c319c4dfb,fdb00b073000000"

    def test_actual_client_repeated_and_concurrent_api_admissions_are_isolated(self):
        # The native getter crashes without API selection. Each real admission
        # must select730 in a fresh child, including overlapping admissions.
        self.assertEqual(package.fdb_client_version(self.library), self.version)
        with ThreadPoolExecutor(max_workers=4) as executor:
            versions = list(executor.map(package.fdb_client_version, [self.library] * 8))
        self.assertEqual(versions, [self.version] * 8)
        self.assertEqual(package.fdb_client_version(self.library), self.version)

    def test_local_identity_exercises_api_and_retains_actual_version(self):
        prefix = self.library.parent.parent
        if not (prefix / "client.json").is_file():
            self.skipTest("native library supplied without prepare-fdb client.json")
        receipt = package.fdb_identity(prefix)
        self.assertEqual(receipt["client_version"], self.version)
        self.assertEqual(receipt["sha256"], package.FDB_SHA256)
        self.assertEqual(receipt["blake2b512"], package.FDB_BLAKE2B)


if __name__ == "__main__":
    unittest.main()
