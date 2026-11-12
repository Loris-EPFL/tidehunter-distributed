"""Offline artifact-boundary tests, including real ELF loader checks when tools exist."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import os
import shutil
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

    def test_nvmet_assets_must_be_declared_complete_and_source_bound(self):
        files = {"nvmet-fix/" + name: {} for name in package.NVMET_FILES}
        files.update({"source/" + package.NVMET_HELPER: {}, "source/" + package.NVMET_PATCH: {}})
        candidate = {"module_sha256": "a" * 64}
        with patch.object(package, "nvmet_fix_identity", return_value=candidate):
            self.assertEqual(package.admit_nvmet_assets(self.root, files, candidate), candidate)
            with self.assertRaisesRegex(ValueError, "undeclared"):
                package.admit_nvmet_assets(self.root, files, None)
            with self.assertRaisesRegex(ValueError, "inventory"):
                package.admit_nvmet_assets(self.root, {k: v for k, v in files.items()
                                                       if k != "nvmet-fix/nvmet.ko"}, candidate)
            with self.assertRaisesRegex(ValueError, "source closure"):
                package.admit_nvmet_assets(self.root, {k: v for k, v in files.items()
                                                       if k != "source/" + package.NVMET_HELPER}, candidate)
            with self.assertRaisesRegex(ValueError, "static identity"):
                package.admit_nvmet_assets(self.root, files, {"module_sha256": "b" * 64})
        self.assertIsNone(package.admit_nvmet_assets(self.root, {}, None))

    def test_nvmet_patch_must_equal_frozen_source_before_static_admission(self):
        source_patch = self.root / "source" / package.NVMET_PATCH
        source_patch.parent.mkdir(parents=True)
        source_patch.write_bytes(b"reviewed patch")
        prefix = self.root / "fix"
        prefix.mkdir()
        (prefix / "partial-nowait-read.patch").write_bytes(b"different patch")
        with patch.object(package, "nvmet_fix_module") as helper:
            with self.assertRaisesRegex(ValueError, "differs from frozen source"):
                package.nvmet_fix_identity(self.root / "source", prefix)
            helper.assert_not_called()

    def test_valkey_source_admission_requires_pinned_clean_rdma_artifacts(self):
        source = self.root / "valkey"
        source.mkdir()
        for name in package.VALKEY_ARTIFACTS:
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(name.encode())
        settings = source / "src/.make-settings"
        settings.write_text("BUILD_RDMA=yes\nUSE_SYSTEMD=no\n")

        def command(argv, **kwargs):
            if argv[1:3] == ["rev-parse", "HEAD"]:
                return package.VALKEY_COMMIT
            if argv[1:3] == ["status", "--porcelain"]:
                return ""
            return "--rdma"

        with patch.object(package, "command", side_effect=command), \
                patch.object(package, "elf_identity", return_value={"elf": True}):
            settings.write_text("BUILD_RDMA=yes\nUSE_SYSTEMD=yes\n")
            with self.assertRaisesRegex(ValueError, "fixed runtime ABI"):
                package.valkey_source_identity(source)
            settings.write_text("BUILD_RDMA=yes\nUSE_SYSTEMD=no\n")
            (source / package.VALKEY_ARTIFACTS[-1]).unlink()
            with self.assertRaises(FileNotFoundError):
                package.valkey_source_identity(source)
        with patch.object(package, "command", return_value="different"):
            with self.assertRaisesRegex(ValueError, "pinned commit"):
                package.valkey_source_identity(source)


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

    def test_focused_report_source_closure_requires_analyzer(self):
        for name in ("experiments/kv-comparison/analyze.py",
                     "experiments/kv-comparison/campaign.py"):
            self.assertIn(name, package.REQUIRED)
        (self.repo / "experiments/kv-comparison/analyze.py").unlink()
        with self.assertRaisesRegex(ValueError, "required source closure"):
            package.source_paths(self.repo)

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

    def test_live_nvme_sources_are_required_and_hash_bound(self):
        names = ("tidehunter/src/distributed/nvmeof/live_storage.rs",
                 "tidehunter/src/distributed/nvmeof/live_io.rs",
                 "tidehunter/src/distributed/nvmeof/live_target.rs",
                 "tidehunter/src/distributed/nvmeof/live_target/spdk.rs",
                 "tidehunter/src/distributed/nvmeof/live_connect.rs",
                 "tidehunter/src/distributed/nvmeof/live_namespace.rs")
        self.git("rm", "--cached", "--", *names)
        snapshot = package.snapshot(self.repo, self.root / "snapshot")
        with zipfile.ZipFile(self.root / "snapshot/source.zip") as archive:
            for name in names:
                self.assertIn(name, snapshot["sha256"])
                self.assertEqual(archive.read(name), (self.repo / name).read_bytes())
        (self.repo / names[0]).unlink()
        with self.assertRaisesRegex(ValueError, "required source closure"):
            package.source_paths(self.repo)

    def test_live_spdk_helpers_are_required_in_source_closure(self):
        names = ("experiments/nvmeof/live_spdk_daemon.py",
                 "experiments/nvmeof/spdk_targets.py",
                 "experiments/nvmeof/spdk_artifacts.py",
                 "experiments/nvmeof/spdk_measurement.py")
        for name in names:
            self.assertIn(name, package.REQUIRED)
        (self.repo / names[0]).unlink()
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

    def test_hwloc_is_admitted_only_for_gcp_resolved_closure(self):
        library = self.root / "fabric"
        library.write_bytes(b"fixture")
        header = "ELF64 Advanced Micro Devices X86-64"
        dynamic = "(NEEDED) Shared library: [libhwloc.so.15]"
        with patch.object(package, "command", side_effect=[header, dynamic, ""]):
            self.assertIn("libhwloc.so.15", package.elf_identity(library, "gcp")["needed"])
        for provider in ("aws", "other"):
            with self.subTest(provider=provider), patch.object(package, "command", side_effect=[header, dynamic, ""]):
                with self.assertRaises(ValueError):
                    package.elf_identity(library, provider)

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


@unittest.skipUnless(all(shutil.which(tool) for tool in ("cc", "readelf", "ldd")), "native ELF tools unavailable")
class HostAbiClosureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "crypto.c").write_text("int answer(void) { return 7; }\n")
        (self.root / "ssl.c").write_text("extern int answer(void); int ssl_answer(void) { return answer(); }\n")
        (self.root / "main.c").write_text("extern int ssl_answer(void); int main(void) { return ssl_answer() != 7; }\n")
        self.provider(self.root, "OPENSSL_3.4.0")
        self.cc("-shared", "-fPIC", "-nostdlib", self.root / "ssl.c", "-L" + str(self.root),
                "-lcrypto", "-Wl,-soname,libssl.so.3", "-o", self.root / "libssl.so.3")
        (self.root / "libssl.so").symlink_to("libssl.so.3")
        self.cc(self.root / "main.c", "-L" + str(self.root), "-lssl", "-lcrypto",
                "-o", self.root / "client")

    def cc(self, *arguments):
        subprocess.run(["cc", *(str(value) for value in arguments)], check=True, capture_output=True, timeout=30)

    def provider(self, directory, version):
        arguments = []
        if version is not None:
            script = directory / "versions.map"
            script.write_text(version + " { global: answer; local: *; };\n")
            arguments.append("-Wl,--version-script=" + str(script))
        self.cc("-shared", "-fPIC", "-nostdlib", self.root / "crypto.c", *arguments,
                "-Wl,-soname,libcrypto.so.3", "-o", directory / "libcrypto.so.3")
        (directory / "libcrypto.so").symlink_to("libcrypto.so.3")

    def test_newer_compatible_host_closure_does_not_relax_packaged_artifact_floor(self):
        environment = package.clean_environment()
        environment["LD_LIBRARY_PATH"] = str(self.root)
        subprocess.run([self.root / "client"], env=environment, check=True, timeout=10)
        with self.assertRaisesRegex(ValueError, "OPENSSL_3.4.0"):
            package.elf_identity(self.root / "libssl.so.3")
        closure = package.linked_libraries(self.root / "client", environment)
        self.assertEqual(closure["resolved"][str(self.root / "libssl.so.3")]["version_requirements"]["OPENSSL"],
                         ["3.4.0"])

    def test_runtime_rejects_provider_missing_the_required_symbol_version(self):
        broken = self.root / "broken"
        broken.mkdir()
        self.provider(broken, "OPENSSL_3.0.0")
        environment = package.clean_environment()
        environment["LD_LIBRARY_PATH"] = str(broken) + ":" + str(self.root)
        with self.assertRaises((ValueError, RuntimeError, subprocess.CalledProcessError)):
            package.linked_libraries(self.root / "client", environment)

    def test_runtime_rejects_provider_without_version_definitions(self):
        unversioned = self.root / "unversioned"
        unversioned.mkdir()
        self.provider(unversioned, None)
        environment = package.clean_environment()
        environment["LD_LIBRARY_PATH"] = str(unversioned) + ":" + str(self.root)
        with self.assertRaisesRegex(ValueError, "symbol version"):
            package.linked_libraries(self.root / "client", environment)


    def test_extra_soname_uses_loader_and_rejects_silently_ignored_preload(self):
        self.cc('-shared', '-fPIC', '-nostdlib', self.root / 'crypto.c',
                '-Wl,-soname,libstdc++.so.6', '-o', self.root / 'libstdc++.so.6')
        environment = package.clean_environment()
        environment['LD_LIBRARY_PATH'] = str(self.root)
        baseline = package.linked_libraries(self.root / 'client', environment)
        self.assertNotIn('libstdc++.so.6', {x['soname'] for x in baseline['resolved'].values()})
        proof = package.linked_libraries(self.root / 'client', environment, extra_names=['libstdc++.so.6'])
        self.assertEqual(proof['resolved'][str(self.root / 'libstdc++.so.6')]['soname'], 'libstdc++.so.6')
        with self.assertRaisesRegex(ValueError, 'libmissing-sui.so.1'):
            package.linked_libraries(self.root / 'client', environment, extra_names=['libmissing-sui.so.1'])
        for names in (['../lib.so'], ['lib:bad'], ['lib bad'], ['same', 'same'], ['x']*17):
            with self.subTest(names=names), self.assertRaisesRegex(ValueError, 'invalid extra'):
                package.linked_libraries(self.root / 'client', environment, extra_names=names)
        with patch.object(package, 'command', return_value=baseline['ldd']) as loader, \
             patch.object(package, 'elf_identity', side_effect=lambda path, *args, **kwargs: baseline['resolved'][str(path)]):
            self.assertEqual(package.linked_libraries(self.root / 'client', environment, extra_names=()), baseline)
        self.assertEqual(loader.call_args.kwargs['env'], {**environment, 'LC_ALL':'C'})


if __name__ == "__main__":
    unittest.main()
