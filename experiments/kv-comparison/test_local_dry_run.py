import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import local_dry_run
import cloud_controller



class OwnerPrewriteTests(unittest.TestCase):
    def test_prewrite_proof_requires_positive_cost_for_every_owner(self):
        from local_multiowner_check import validate_prewrite_receipts
        valid = {owner: {"wal_prewrite_bytes": 8192, "wal_prewrite_ns": 1, "wal_prewrite_files": 1}
                 for owner in range(4)}
        validate_prewrite_receipts(valid, 4)
        with self.assertRaisesRegex(ValueError, "every owner"):
            validate_prewrite_receipts({0: valid[0]}, 4)
        for field in ("wal_prewrite_bytes", "wal_prewrite_ns", "wal_prewrite_files"):
            for missing in (None, 0, -1, True):
                invalid = copy.deepcopy(valid)
                invalid[0][field] = missing
                with self.subTest(field=field, value=missing), \
                        self.assertRaisesRegex(ValueError, field):
                    validate_prewrite_receipts(invalid, 4)

    def test_local_owner_writes_explicit_prewrite_before_validation(self):
        import yaml
        # (local override, reviewed case job, explicit launch value)
        for override, job, expected in ((None, {}, False), (False, {}, False), (True, {}, True),
                                        (None, {"owner_wal_prewrite": False}, False),
                                        (None, {"owner_wal_prewrite": True}, True)):
            with self.subTest(override=override, job=job), tempfile.TemporaryDirectory() as temporary:
                mode = object.__new__(local_dry_run.LocalMode)
                mode.fixture = Path(temporary)
                mode.config = {"prefix": "owned"}
                if override is not None:
                    mode.config["wal_prewrite"] = override
                mode.controller = mock.Mock()
                mode.cloud = mock.Mock()
                mode.cloud.case_owner_job.return_value = job
                mode.cloud.bind_owner_wal_prewrite.side_effect = cloud_controller.bind_owner_wal_prewrite
                mode.cloud.digest.return_value = "digest"
                mode.cloud.download_object.side_effect = (
                    lambda clients, config, key, destination: destination.touch())
                def extract(archive, guest, files):
                    guest.mkdir()
                    (guest / "launch.yaml").write_text(yaml.safe_dump({"node": {}}))
                mode.cloud.safe_extract_capsule.side_effect = extract
                mode.cloud.validate_launch.side_effect = RuntimeError("stop before launch")
                request = {"case_id": "case", "key": "owned/deploy/cells/case/storage-0.tar.gz",
                           "sha256": "digest", "files": []}
                with self.assertRaisesRegex(RuntimeError, "stop before launch"):
                    mode.start_owner(0, request, "case", 10)
                launch = mode.cloud.validate_launch.call_args.args[1]
                self.assertIs(launch["node"]["wal_prewrite"], expected)
                saved = yaml.safe_load((mode.fixture / "cells/case/node-0/launch.yaml").read_text())
                self.assertEqual(saved, launch)

    def test_local_cli_prewrite_defaults_on_with_explicit_opt_out(self):
        from local_multiowner_check import parse_args
        base = ["--binary", "b", "--libs", "l", "--output", "o"]
        self.assertIs(parse_args(base).wal_prewrite, True)
        self.assertIs(parse_args(base + ["--wal-prewrite"]).wal_prewrite, True)
        self.assertIs(parse_args(base + ["--no-wal-prewrite"]).wal_prewrite, False)


class OperationScalingTests(unittest.TestCase):
    def test_layout_inventory_does_not_own_local_client_lifecycle(self):
        mode = object.__new__(local_dry_run.LocalMode)
        mode.cloud = cloud_controller
        mode.config = {}
        mode.layout_fleet = {}
        mode.layout_backend = "tikv"
        mode.units = set()
        mode.valkey = None
        job = {"backend": "tikv", "case_id": "case"}
        evidence = {"processes": [], "missing": [{"role": "client", "reason": "not started"}]}
        with mock.patch.object(cloud_controller, "unit_name", return_value="client"), mock.patch.object(
                cloud_controller, "owned_monitor_processes", return_value=evidence):
            self.assertEqual(mode.owned_processes(-1, job), [])
            evidence["missing"].append({"role": "tikv", "reason": "identity unavailable"})
            with self.assertRaises(RuntimeError):
                mode.owned_processes(-1, job)
    def test_runtime_environment_applies_peer_limit_to_subprocesses(self):
        import os
        with mock.patch.dict(os.environ, {"FI_UNIVERSE_SIZE": "1024"}):
            local_dry_run.configure_runtime_environment("/fixture/runtime")
            self.assertEqual(cloud_controller.environment({"fdb_library_dir": "/fixture/runtime/lib"})["FI_UNIVERSE_SIZE"], "6")
            self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/fixture/runtime/lib")

    def test_provider_log_level_reaches_local_units(self):
        import os
        with mock.patch.dict(os.environ, {"FI_LOG_LEVEL": "info"}):
            argv = ["systemd-run", "--unit=owned"]
            local_dry_run.local_fabric_unit_environment(argv)
            self.assertIn("--setenv=FI_LOG_LEVEL=info", argv)
            self.assertIn("--setenv=FI_UNIVERSE_SIZE=6", argv)
        with mock.patch.dict(os.environ, {"FI_LOG_LEVEL": "unexpected"}):
            with self.assertRaises(ValueError):
                local_dry_run.local_fabric_unit_environment(["systemd-run"])

    def test_outstanding_nvme_is_quarantined_even_without_owner_flag(self):
        controller = mock.Mock(tide_cleanup_forbidden=False, active_nvme=None)
        self.assertFalse(local_dry_run.owner_or_nvme_quarantined(controller))
        controller.active_nvme = {}
        self.assertTrue(local_dry_run.owner_or_nvme_quarantined(controller))
        controller.active_nvme = None
        controller.tide_cleanup_forbidden = True
        self.assertTrue(local_dry_run.owner_or_nvme_quarantined(controller))

    def test_zero_warmup_and_dataset_are_preserved(self):
        job = {"warmup_operations": 0, "measurement_operations": 8192,
               "keys": 65536, "read_tier": {"requested": {"warmup_operations": 0}}}
        original = copy.deepcopy(job)
        scaled = local_dry_run.scale_operations(job, 0.001)
        self.assertEqual(scaled["warmup_operations"], 0)
        self.assertEqual(scaled["measurement_operations"], 32)
        self.assertEqual(scaled["keys"], 65536)
        self.assertEqual(job, original)

    def test_read_tier_request_tracks_scaled_warmup(self):
        job = {"warmup_operations": 1000, "measurement_operations": 300,
               "read_tier": {"requested": {"warmup_operations": 1000}}}
        scaled = local_dry_run.scale_operations(job, 0.1)
        self.assertEqual(scaled["warmup_operations"], 100)
        self.assertEqual(scaled["measurement_operations"], 32)
        self.assertEqual(scaled["read_tier"]["requested"]["warmup_operations"], 100)

    def test_rejects_invalid_fraction_and_never_increases_count(self):
        job = {"warmup_operations": 1, "measurement_operations": 10}
        self.assertEqual(local_dry_run.scale_operations(job, 0.5), job)
        for fraction in (0, -1, 2, float("nan"), float("inf")):
            with self.subTest(fraction=fraction), self.assertRaises(ValueError):
                local_dry_run.scale_operations(job, fraction)

    def test_order_is_bound_before_scaling(self):
        import hashlib
        import json
        jobs = [{"case_id": "a", "backend": "native"},
                {"case_id": "b", "backend": "native"}]
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        order = {"matrix_sha256": digest, "planned_cases": 2, "case_ids": ["b", "a"]}
        with mock.patch("cloud_campaign.make_matrix", return_value=jobs):
            result = local_dry_run.ordered_jobs({}, order)
        self.assertEqual([job["case_id"] for job in result], ["b", "a"])

    def test_focused_order_admits_only_reviewed_spec_and_unscaled_cases(self):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gcp-rdma"))
        import manual_campaign
        import risk_order
        spec = copy.deepcopy(manual_campaign.REVIEWED_SPECS["nvmeof_path_core"])
        jobs = manual_campaign._reviewed_jobs(spec)
        order = risk_order.generate_order(jobs)
        ordered = local_dry_run.ordered_jobs(spec, order)
        self.assertEqual(len(ordered), 53)
        self.assertTrue(all(local_dry_run.scale_operations(job, 1.0) == job for job in ordered))
        spec["keys"] += 1
        with self.assertRaises(ValueError):
            local_dry_run.ordered_jobs(spec, order)
        order["matrix_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            local_dry_run.ordered_jobs(manual_campaign.REVIEWED_SPECS["nvmeof_path_core"], order)

    def test_focused_default_scaling_is_rejected_before_fixture(self):
        import json
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gcp-rdma"))
        import manual_campaign
        import risk_order
        spec = manual_campaign.REVIEWED_SPECS["nvmeof_path_core"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "binary").touch()
            (root / "spec").write_text(json.dumps(spec))
            (root / "order").write_text(json.dumps(
                risk_order.generate_order(manual_campaign._reviewed_jobs(spec))))
            argv = ["--binary", str(root / "binary"), "--runtime", str(root),
                    "--libs", str(root), "--source-revision", "test",
                    "--spec", str(root / "spec"), "--order", str(root / "order")]
            with mock.patch("os.geteuid", return_value=0), self.assertRaisesRegex(
                    SystemExit, "require --fraction 1.0"):
                local_dry_run.main(argv)


class CloudIsolationTests(unittest.TestCase):
    def test_cloud_config_rejects_local_mode(self):
        for config in ({"local_mode": {}}, {"spec": {"local_mode": {}}}):
            with self.subTest(config=config), self.assertRaises(cloud_controller.SafetyError):
                cloud_controller.validate_config(config)

    def test_guest_rejects_local_physical_trace_actions(self):
        mode = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(cloud_controller, "guarded", return_value=Path(directory)):
            config = {"fleet": {"data_root": directory, "nodes": [
                {"index": i, "instance_id": f"i-owner-{i}", "address": f"10.0.0.{i + 2}"}
                for i in range(5)]}}
            for action in ("cold_trace_probe", "cold_trace_start", "cold_trace_stop",
                           "nvme_trace_start", "nvme_trace_stop"):
                with self.subTest(action=action), self.assertRaises(cloud_controller.SafetyError):
                    cloud_controller.guest_operation(config, {"index": 0, "action": action}, {}, local_mode=mode)

    def test_owned_command_failure_includes_both_streams(self):
        failed = mock.Mock(returncode=1, stdout="out-detail", stderr="err-detail")
        with mock.patch("subprocess.run", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "out-detail.*err-detail"):
                local_dry_run.LocalMode.root_command(["false"], timeout=1)

    def test_unit_cleanup_handles_collection_but_rejects_live_process(self):
        with tempfile.TemporaryDirectory() as directory:
            mode = local_dry_run.LocalMode(directory, {"prefix": "th-owned", "fleet": {
                "client_cpus": 1, "server_cpus": 1, "server_memory_mib": 256}}, directory)
            mode.units.add("owned")
            absent = {"LoadState": "not-found"}
            running = {"LoadState": "loaded", "MainPID": "42", "ActiveState": "active"}
            with mock.patch.object(mode, "root_command", side_effect=RuntimeError("not loaded")), \
                    mock.patch.object(cloud_controller, "unit_status",
                                      side_effect=[running, absent, absent]):
                mode.stop_client("owned")
            with mock.patch.object(mode, "root_command"), \
                    mock.patch.object(cloud_controller, "unit_status", return_value=running):
                with self.assertRaisesRegex(RuntimeError, "still active"):
                    mode.stop_client("owned")
            with self.assertRaisesRegex(ValueError, "not owned"):
                mode.stop_client("foreign")


class ScopedEvictionTests(unittest.TestCase):
    def test_owned_regular_file_and_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wal"
            path.write_bytes(b"wal")
            result = local_dry_run.scoped_file_eviction(directory, [path, path])
            self.assertEqual(result["method"], "scoped_file_eviction")
            self.assertEqual(len(result["files"]), 1)
            self.assertEqual(result["files"][0]["inode"], path.stat().st_ino)

    def test_rejects_foreign_file_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as other:
            foreign = Path(other) / "foreign"
            foreign.write_bytes(b"foreign")
            link = Path(directory) / "link"
            link.symlink_to(foreign)
            for path in (foreign, link):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    local_dry_run.scoped_file_eviction(directory, [path])
