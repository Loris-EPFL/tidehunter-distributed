#!/usr/bin/env python3
"""Controller ownership/unknown-outcome tests; all clients are local test doubles."""
import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import io
import json
import sys
from pathlib import Path
import tarfile
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch

import cloud_controller as controller

HERE = Path(__file__).resolve().parent


def config_fixture():
    run = "th-0123456789abcdef"
    return {"account": controller.ACCOUNT, "bucket": "test-comparison-bucket", "bucket_region": "us-east-1", "prefix": run,
        "source_revision": "a" * 40, "source_sha256": "b" * 64, "binary_sha256": "c" * 64,
        "binary_blake2b512": "d" * 128, "repository_root": "/opt/test-comparison/source",
        "binary": "/opt/test-comparison/runtime/bin/kv-research", "fdb_library_dir": "/opt/test-comparison/runtime/lib",
        "deadline_utc": "2099-01-01T02:00:00Z", "spec": json.loads((HERE / "cloud-campaign.json").read_text()),
        "fleet": {"schema_version": 1, "run_id": run, "region": "us-east-1",
            "data_root": "/mnt/tidehunter-kv-comparison/" + run,
            "coordinator": {"instance_id": "i-00000000000000001", "address": "10.0.0.1"},
            "nodes": [{"index": i, "instance_id": f"i-{i + 2:017x}", "address": f"10.0.0.{i + 2}"} for i in range(5)],
            "server_cpus": 4, "server_memory_mib": 8192, "client_cpus": 4, "client_memory_mib": 8192,
            "replication_factor": 1}}


class F7PackageTests(unittest.TestCase):
    def test_focused_package_profile_is_selection_specific(self):
        for selection, expected, rejected in (
                ("nvmeof_f7_highmem32", "focused", "focused_valkey"),
                ("valkey_rdma_reads", "focused_valkey", "focused")):
            config = config_fixture()
            config["spec"] = {"mode": "focused", "selection": selection}
            admission = {key: config[key] for key in
                         ("source_sha256", "binary_sha256", "binary_blake2b512")}
            admission["build_profile"] = expected
            with self.subTest(selection=selection), \
                    patch("cloud_package.verify_directory", return_value=admission), \
                    patch.object(controller, "platform_module", return_value=None), \
                    patch.object(Path, "read_text", return_value=json.dumps(
                        {"revision": config["source_revision"]})):
                self.assertIsNone(controller.verify_package(config)["fdb"])
                admission["build_profile"] = rejected
                with self.assertRaisesRegex(controller.SafetyError, "package profile"):
                    controller.verify_package(config)


def instance_response(config):
    tags = {**controller.TAGS, "RunId": config["prefix"], "ExpiresAt": config["deadline_utc"]}
    return {"Reservations": [{"OwnerId": config["account"], "Instances": [
        {"InstanceId": host["instance_id"], "PrivateIpAddress": host["address"], "State": {"Name": "running"},
         "Tags": [{"Key": key, "Value": value} for key, value in tags.items()]}
        for host in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]]}]}


def clients_fixture(config):
    ec2 = Mock()
    ec2.describe_instances.return_value = instance_response(config)
    ids = [h["instance_id"] for h in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]]
    ec2.terminate_instances.return_value = {"TerminatingInstances": [
        {"InstanceId": identity, "CurrentState": {"Name": "shutting-down"}} for identity in ids]}
    ssm = Mock()
    ssm.exceptions.InvocationDoesNotExist = type("InvocationDoesNotExist", (Exception,), {})
    ssm.exceptions.InvalidInstanceId = type("InvalidInstanceId", (Exception,), {})
    return {"ec2": ec2, "ssm": ssm, "s3": Mock(), "sts": Mock()}

class OwnerCountTests(unittest.TestCase):
    def test_counter_and_trace_expect_exact_active_partitions(self):
        for count in (1, 5):
            job = {"backend": "tidehunter_tcp", "coordinator_cache_bytes": 0, "owner_count": count}
            evidence = {"schema_version": 1, "backend": "tidehunter_tcp",
                        "coordinator": {"read_rpc_count": 0, "cache": {"budget_bytes": 0}},
                        "owner_read_configuration": [{"partition": i} for i in range(count)]}
            controller.verify_counter_evidence(job, evidence)
            evidence["owner_read_configuration"].append({"partition": count})
            with self.assertRaises(controller.SafetyError):
                controller.verify_counter_evidence(job, evidence)
            traces = [{}] + [{"available": True, "lost": 0, "device": {"device": str(i)}}
                             for i in range(count)] + [{"inactive": True}] * (5 - count)
            result = controller.block_trace_evidence("case", "owner", traces, count)
            self.assertTrue(result["available"])
            self.assertEqual(result["device_identity"], ",".join(map(str, range(count))))

    def test_compact_rejects_five_owner_and_unsupported_cases_before_dispatch(self):
        config = config_fixture()
        config["fleet"]["nodes"] = config["fleet"]["nodes"][:1]
        config["spec"].update(mode="focused", selection="parity_compact")
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.run_floor_case = Mock()
        runtime.run_fio_case = Mock()
        base = {"case_id": "case", "case_kind": "remote_put", "backend": "tidehunter_tcp",
                "durability_tier": "synchronous"}
        cases = [base, {**base, "owner_count": 5}, {**base, "backend": "tikv"},
                 {**base, "owner_count": 1, "case_kind": "interference"},
                 {**base, "owner_count": 1, "case_kind": "fio_floor"},
                 {**base, "owner_count": 1, "case_kind": "unknown"}]
        matrix = controller.cloud_campaign.make_focused_matrix("parity_compact")
        cases.append(next(job for job in controller.cloud_campaign.make_focused_matrix("fair_five_owner")
                          if job["case_kind"] == "remote_put"))
        valid_owner = next(job for job in matrix if job["case_kind"] == "remote_put")
        cases.extend(({**valid_owner, "case_id": "unknown"}, {**valid_owner, "keys": valid_owner["keys"] + 1},
                      {key: value for key, value in valid_owner.items() if key != "owner_count"}))
        for kind in ("native_op", "ssd_write_floor", "remote_put"):
            valid = next(job for job in matrix if job["case_kind"] == kind)
            cases.extend(({**valid, "sealed_dataset": {}}, {**valid, "nvmeof_route": "live"},
                          {**valid, "interference": {}}, {**valid, "fio": {}}))
        cases += [{"case_id": "bad", "case_kind": "native_op", "backend": "tidehunter_tcp",
                   "owner_count": 1},
                  {"case_id": "bad", "case_kind": "ssd_write_floor", "backend": "native", "group": "G13W"},
                  {"case_id": "bad", "case_kind": "ssd_write_floor", "backend": "tidehunter_verbs_native",
                   "group": "G11W"}]
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            for job in cases:
                with self.subTest(job=job), self.assertRaises(ValueError):
                    runtime.run_case(job)
            self.assertEqual(list(runtime.root.iterdir()), [])
        runtime.run_floor_case.assert_not_called()
        runtime.run_fio_case.assert_not_called()
        runtime.config["spec"]["selection"] = "parity"
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with self.assertRaises(ValueError):
                runtime.run_case(matrix[0])
            self.assertEqual(list(runtime.root.iterdir()), [])
        runtime.run_floor_case.assert_not_called()
        runtime.run_fio_case.assert_not_called()

    def test_compact_snapshots_contact_only_two_hosts(self):
        config = config_fixture()
        config["fleet"]["nodes"] = config["fleet"]["nodes"][:1]
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.request = Mock(side_effect=lambda index, request, **kwargs: {"index": index})
        result = runtime.all_hosts({"action": "bootstrap"})
        self.assertEqual(result, [{"index": -1}, {"index": 0}])
        self.assertEqual(sorted(call.args[0] for call in runtime.request.call_args_list), [-1, 0])

    def test_compact_accepts_native_and_owner_floor_without_extra_hosts(self):
        config = config_fixture()
        config["fleet"]["nodes"] = config["fleet"]["nodes"][:1]
        config["spec"].update(mode="focused", selection="parity_compact")
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.run_floor_case = Mock(return_value="floor")
        matrix = controller.cloud_campaign.make_focused_matrix("parity_compact")
        floor = next(job for job in matrix if job["case_kind"] == "ssd_write_floor")
        self.assertEqual(runtime.run_case(floor), "floor")
        native = next(job for job in matrix if job["case_kind"] == "native_op")
        remotes = [next(job for job in matrix if job["case_kind"] == "remote_put" and job["backend"] == backend)
                   for backend in ("tidehunter_tcp", "tidehunter_verbs_native")]
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            for job in (native, *remotes):
                with self.subTest(kind=job["case_kind"]), \
                     patch.object(controller, "guarded", side_effect=RuntimeError("guard reached")):
                    with self.assertRaisesRegex(RuntimeError, "guard reached"):
                        runtime.run_case(job)

    def test_compact_backend_cleanup_uses_only_fleet_hosts(self):
        config = config_fixture()
        config["fleet"]["nodes"] = config["fleet"]["nodes"][:1]
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.active_backend = "tikv"
        runtime.cleanup_deadline = Mock(return_value=300)
        runtime.cleanup_timeout = Mock(return_value=30)
        runtime.request = Mock(return_value={"status": "reset", "data_empty": True,
                                             "initialization_removed": True})
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            runtime.stop_backend()
        self.assertEqual([call.args[0] for call in runtime.request.call_args_list], [-1, 0] * 3)

    def test_invalid_one_owner_case_rejected_before_directory_creation(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        runtime.run_floor_case = Mock()
        runtime.run_fio_case = Mock()
        base = {"owner_count": 1, "backend": "tidehunter_tcp", "case_kind": "remote_put",
                "durability_tier": "synchronous", "case_id": "case"}
        cases = [{**base, "case_kind": kind} for kind in (
            "net_floor", "ssd_floor", "ssd_write_floor", "fio_floor", "hybrid_put",
            "sealed_knee", "interference", "media_read", "block_trace", "unknown", None)]
        cases += [{**base, "backend": backend} for backend in ("native", "tidehunter_verbs", "tidehunter_nvme_tcp")]
        cases += [{**base, "durability_tier": tier} for tier in ("native", None)]
        cases += [{**base, "sealed_dataset": {}}, {**base, "nvmeof_route": "live"},
                  {**base, "owner_count": 2}, {**base, "owner_count": True}]
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            for job in cases:
                with self.subTest(job=job), self.assertRaises(ValueError):
                    runtime.run_case(job)
            self.assertEqual(list(runtime.root.iterdir()), [])
        runtime.run_floor_case.assert_not_called()
        runtime.run_fio_case.assert_not_called()

    def test_synchronous_one_owner_rpc_passes_entry_guard(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        job = {"owner_count": 1, "backend": "tidehunter_tcp", "case_kind": "remote_put",
               "durability_tier": "synchronous", "case_id": "case"}
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "guarded", side_effect=RuntimeError("guard reached")):
                with self.assertRaisesRegex(RuntimeError, "guard reached"):
                    runtime.run_case(job)

    def test_preparation_launches_and_receipts_only_selected_owners(self):
        import yaml
        for count in (1, 5):
            config = config_fixture()
            runtime = controller.Controller(config, clients_fixture(config))
            runtime.remaining = Mock(return_value=300)
            runtime.request = Mock(return_value={"receipt": "proof"})
            job = {"case_id": "case", "backend": "tidehunter_tcp", "owner_count": count,
                   "keys": 16, "warmup_operations": 1, "measurement_operations": 2,
                   "coordinator_cache_bytes": 0}
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                def prepare(argv, **kwargs):
                    prep = json.loads((directory / "prepare-input.json").read_text())
                    self.assertEqual(prep["owner_wal_geometry"], "research")
                    self.assertEqual(prep["addresses"], [
                        host["address"] + ":9400" for host in config["fleet"]["nodes"][:count]])
                    prepared = directory / "prepared"
                    prepared.mkdir()
                    (prepared / "remote.json").write_text(json.dumps({"tls": {}, "storage": {"fragment_bytes": 4194304, "wal_file_bytes": 1073741824}}))
                    (prepared / "external_owner_read_config.json").write_text("[]")
                    for index in range(count):
                        node = prepared / f"node-{index}"
                        node.mkdir()
                        (node / "launch.yaml").write_text(yaml.safe_dump({"tls": {}, "node": {"partition": {"fragment_bytes": 4194304, "wal_file_bytes": 1073741824}}}))
                with patch.object(controller, "command", side_effect=prepare), \
                     patch.object(controller, "validate_launch"), \
                     patch.object(controller, "upload_object"), \
                     patch.object(controller, "confirm_owner_durability",
                                  side_effect=lambda proof, tier, index, nonce: {"partition": index}):
                    options = runtime.prepare_tide(job, directory)
                self.assertEqual(options, {
                    "path": str(directory / "database"), "evidence": str(directory / "read-evidence"),
                    "owners": count, "key_count": 1048576, "max_batches_per_partition": 16384,
                    "coordinator_read_cache_bytes": 0, "owner_read_resident_first": False,
                    "external_remote_config": {"tls": {}, "storage": {"fragment_bytes": 4194304, "wal_file_bytes": 1073741824}}, "durability_tier": "native",
                    "external_owner_durability_receipts": [
                        str((directory / "owner-durability" / f"node-{i}.durability.json").absolute())
                        for i in range(count)], "external_owner_read_config": []})
                self.assertEqual(len(options["external_owner_durability_receipts"]), count)
                self.assertEqual(sorted(call.args[0] for call in runtime.request.call_args_list), list(range(count)))
                self.assertEqual(sorted(path.name for path in (directory / "owner-durability").iterdir()),
                                 [f"node-{i}.durability.json" for i in range(count)])


    def test_owner_batch_and_cleanup_only_touch_active_owner(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.remaining = Mock(return_value=300)
        runtime.cleanup_deadline = Mock(return_value=300)
        runtime.cleanup_timeout = Mock(return_value=30)
        runtime.request = Mock(return_value={"complete": True})
        with tempfile.TemporaryDirectory() as tmp:
            runtime.active_tide = ({"case_id": "case"}, Path(tmp), [0])
            self.assertEqual(runtime.owner_batch("nvme_owner_stop", "case"), [{"complete": True}])
            runtime.stop_tide()
        self.assertEqual([call.args[0] for call in runtime.request.call_args_list], [0, 0])

    def test_active_owner_failure_still_cleans_only_that_owner(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.remaining = Mock(return_value=300)
        runtime.cleanup_deadline = Mock(return_value=300)
        runtime.cleanup_timeout = Mock(return_value=30)
        runtime.request = Mock(side_effect=[{"complete": False}, {"complete": True}])
        with tempfile.TemporaryDirectory() as tmp:
            runtime.active_tide = ({"case_id": "case"}, Path(tmp), [0])
            with self.assertRaises(controller.InfrastructureError):
                runtime.owner_batch("nvme_owner_stop", "case")
            runtime.stop_tide()
        self.assertEqual([call.args[0] for call in runtime.request.call_args_list], [0, 0])

    def test_inactive_hosts_have_no_owner_cgroup(self):
        config = config_fixture()
        job = {"backend": "tidehunter_tcp", "case_id": "case", "owner_count": 1}
        self.assertEqual(len(controller.owned_monitor_processes(config, 0, job)["processes"]), 1)
        for index in range(1, 5):
            self.assertEqual(controller.owned_monitor_processes(config, index, job)["processes"], [])

    def test_owner_history_requires_selected_partition_set(self):
        def boundary(count):
            return {"owner_executor": {"available": True, "partitions": {
                str(i): {"authority": {"history_loads": i + 1}} for i in range(count)}}}
        evidence = {"measurement_drained": True, "start": boundary(1), "end": boundary(1)}
        self.assertEqual(controller.owner_history_boundaries(evidence, 1), (1, 1))
        self.assertEqual(controller.owner_history_boundaries(evidence), (None, None))
        evidence["end"] = boundary(5)
        self.assertEqual(controller.owner_history_boundaries(evidence, 1), (None, None))


class TerminalTimelineAdmissionTests(unittest.TestCase):
    def test_failed_manifest_records_timeline_error_completed_manifest_rejects(self):
        error = "invalid live group storage persistence profile"
        with patch.object(controller.cloud_campaign, "admit_write_timeline",
                          side_effect=ValueError(error)) as admit:
            receipt = {}
            controller.admit_terminal_write_timeline(
                Path("/case"), {}, {}, {"status": "failed"}, receipt)
            self.assertEqual(receipt["write_timeline_admission_error"], "ValueError: " + error)
            self.assertNotIn("write_timeline", receipt)
            self.assertFalse(admit.call_args.kwargs["completed"])
            with self.assertRaisesRegex(ValueError, error):
                controller.admit_terminal_write_timeline(
                    Path("/case"), {}, {}, {"status": "completed"}, {})
            self.assertTrue(admit.call_args.kwargs["completed"])

    def test_failed_manifest_retains_valid_partial_binding(self):
        binding = {"path": "result/write-timeline.json", "sha256": "a" * 64}
        with patch.object(controller.cloud_campaign, "admit_write_timeline", return_value=binding):
            receipt = {}
            controller.admit_terminal_write_timeline(
                Path("/case"), {}, {}, {"status": "failed"}, receipt)
            self.assertEqual(receipt["write_timeline"], binding)
            self.assertNotIn("write_timeline_admission_error", receipt)


class OwnerFailureEvidenceTests(unittest.TestCase):

    def test_owner_capture_precedes_guard_and_retains_partial_failures(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            runtime.active_tide = ({"case_id": "owner"}, directory, [0, 1])
            runtime.tide_cleanup_forbidden = True
            def request(index, request, **kwargs):
                self.assertEqual(request, {"action": "tide_evidence", "case_id": "owner"})
                if index == 1:
                    raise RuntimeError("host unavailable")
                return {"unit": {"available": True}, "journal": {"available": True, "text": "first owner error"}}
            runtime.request = Mock(side_effect=request)
            rows = runtime.failure_owner_evidence(directory)
            self.assertEqual([row["index"] for row in rows], [0, 1])
            self.assertTrue(rows[0]["available"])
            self.assertFalse(rows[1]["available"])
            self.assertIn("first owner error", (directory / "server-proofs/failure-0.json").read_text())
            self.assertIn("host unavailable", (directory / "server-proofs/failure-1.json").read_text())

    def test_journal_capture_limits_output_and_uses_exact_unit(self):
        process = Mock()
        process.stdout.fileno.return_value = 5
        process.poll.return_value = None
        def read(fd, count):
            self.assertEqual(fd, 5)
            return b"x" * count
        with patch.object(controller.subprocess, "Popen", return_value=process) as popen, \
             patch.object(controller.select, "select", return_value=([process.stdout], [], [])), \
             patch.object(controller.os, "read", side_effect=read):
            result = controller.bounded_owner_journal("exact-unit")
        self.assertTrue(result["truncated"])
        self.assertEqual(len(result["text"]), 1048576)
        self.assertEqual(result["max_lines"], 512)
        self.assertEqual(popen.call_args.args[0][3], "exact-unit")
        process.kill.assert_called_once()

    def test_failed_diagnostic_retains_its_actual_exit_status_and_stderr(self):
        result = controller.bounded_capture(
            [sys.executable, "-c", "import sys; sys.stderr.write('permission denied\\n'); sys.exit(7)"],
            max_bytes=1024, timeout=5)
        self.assertFalse(result["available"])
        self.assertEqual(result["returncode"], 7)
        self.assertEqual(result["text"], "permission denied\n")
        self.assertFalse(result["truncated"])

    def test_initiator_uses_only_receipt_bound_controllers_and_reports_tool_failure(self):
        config = config_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "case"
            directory.mkdir()
            hosts = []
            for index in range(6):
                receipt = {"controller": f"nvme{index}", "controller_inode": 42,
                           "nqn": f"nqn.test.{index}", "transport": "rdma",
                           "controller_address": "traddr=10.0.0.1,trsvcid=4420"}
                controller.dump(directory / f"nvme-host-{index}.json",
                                {"phase": "connected", "exports": [{"host_receipt": receipt}]})
                hosts.append({"index": index})
            active = {"case_directory": str(directory), "hosts": hosts}
            original_read = Path.read_text
            original_stat = Path.stat
            def read_text(path, *args, **kwargs):
                if str(path).startswith("/sys/class/nvme/"):
                    name, field = path.parent.name, path.name
                    return {"subsysnqn": "nqn.test." + name[4:], "transport": "rdma",
                            "address": "traddr=10.0.0.1,trsvcid=4420"}[field]
                return original_read(path, *args, **kwargs)
            def stat(path, *args, **kwargs):
                if str(path).startswith("/sys/class/nvme/"):
                    return Mock(st_ino=42)
                return original_stat(path, *args, **kwargs)
            def capture(argv, **kwargs):
                if argv[0] == "nvme" and argv[1] == "error-log":
                    raise FileNotFoundError("nvme unavailable")
                return {"available": True, "text": "", "truncated": False}
            with patch.object(controller, "verify_local_identity"), \
                 patch.object(Path, "stat", stat), patch.object(Path, "read_text", read_text), \
                 patch.object(controller, "bounded_capture", side_effect=capture) as bounded:
                frame = controller.initiator_failure_frame(
                    config, "case", "2026-11-05T00:00:00Z", "2026-11-05T00:01:00Z", active)
            self.assertEqual(frame["controllers_omitted"], 1)
            self.assertEqual(len(frame["controllers"]), 5)
            self.assertTrue(all(not row["error_log"]["available"] for row in frame["controllers"]))
            nvme_calls = [call for call in bounded.call_args_list if call.args[0][0] == "nvme"]
            self.assertEqual(len(nvme_calls), 10)
            self.assertEqual({call.args[0][2] for call in nvme_calls},
                             {f"/dev/nvme{i}" for i in range(5)})
            self.assertTrue(all(call.kwargs == {"max_bytes": 32768, "timeout": 5} for call in nvme_calls))
            self.assertTrue(all("--log-entries=16" in call.args[0]
                                for call in nvme_calls if call.args[0][1] == "error-log"))

    def test_initiator_refuses_observation_when_host_identity_unavailable(self):
        with patch.object(controller, "verify_local_identity", side_effect=RuntimeError("unverified")), \
             patch.object(controller, "bounded_capture") as capture:
            frame = controller.initiator_failure_frame(config_fixture(), "case", "start", "end", None)
        self.assertFalse(frame["kernel_journal"]["available"])
        self.assertIn("identity", frame["controllers_unavailable"])
        capture.assert_not_called()

    def test_initiator_rejects_replaced_controller(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "case"
            directory.mkdir()
            controller.dump(directory / "nvme-host-0.json", {"phase": "connected", "exports": [
                {"host_receipt": {"controller": "nvme999999999", "controller_inode": 1,
                                  "nqn": "nqn.test", "transport": "rdma",
                                  "controller_address": "traddr=10.0.0.1"}}]})
            active = {"case_directory": str(directory), "hosts": [{"index": 0}]}
            with patch.object(controller, "verify_local_identity"), \
                 patch.object(controller, "bounded_capture", return_value={"available": True}) as capture:
                frame = controller.initiator_failure_frame(
                    config_fixture(), "case", "2026-11-05T00:00:00Z", "2026-11-05T00:01:00Z", active)
            self.assertFalse(frame["controllers"][0]["available"])
            self.assertEqual(len(capture.call_args_list), 1)
            self.assertEqual(capture.call_args.args[0][0], "journalctl")


class NvmeActionAdmissionTests(unittest.TestCase):
    def test_nvme_mutations_reject_standard_mode_and_coordinator_role(self):
        for action in ("nvme_owner_stop", "nvme_owner_target_setup", "nvme_trace_start"):
            for mode, index in (("standard", 0), ("standard-with-nvmeof", -1),
                                ("nvmeof-focused", -1), ("standard-with-live-nvmeof", -1)):
                config = config_fixture()
                config["spec"]["mode"] = mode
                with self.subTest(action=action, mode=mode, index=index), \
                     patch.object(controller, "verify_local_identity", return_value={"instanceId": "owned"}), \
                     patch.object(controller, "guarded", return_value=Path("/owned")), \
                     patch.object(controller, "command") as command:
                    with self.assertRaises(controller.SafetyError):
                        controller.guest_operation(config, {"action": action, "index": index}, {})
                    command.assert_not_called()

    def test_live_and_sealed_routes_remain_distinct(self):
        for backend in ("tidehunter_nvme_tcp", "tidehunter_nvme_rdma"):
            controller.require_supported_nvme_route({"backend": backend, "nvmeof_route": "live"})
            for job in (
                {"backend": backend, "nvmeof_route": "live", "sealed_dataset": {}},
                {"backend": backend},
                {"backend": backend, "nvmeof_route": "sealed_read"},
                {"backend": backend, "nvmeof_route": "unknown", "sealed_dataset": {}},
            ):
                with self.subTest(job=job), self.assertRaises(controller.SafetyError):
                    controller.require_supported_nvme_route(job)
            for route in (None, "sealed_read"):
                job = {"backend": backend, "sealed_dataset": {}}
                if route is not None:
                    job["nvmeof_route"] = route
                controller.require_supported_nvme_route(job)
    def test_direct_device_route_and_native_put_admission(self):
        jobs = controller.cloud_campaign.make_focused_matrix("device")
        direct = next(job for job in jobs if job.get("target_io") == "direct")
        controller.require_supported_nvme_route(direct)
        for change in ([{"group": group} for group in ("G4", "G5", "G7", "G9")]
                       + [{"nvmeof_target": "spdk"}, {"nvmeof_route": "live"},
                          {"backend": "tidehunter_tcp"}]):
            with self.subTest(change=change), self.assertRaises(controller.SafetyError):
                controller.require_supported_nvme_route({**direct, **change})
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(json.dumps({"durability": {"confirmed_by_backend_receipt": "native"}}))
            for job in jobs:
                if job["case_kind"] == "remote_put" and job["durability_tier"] == "native":
                    controller.require_supported_nvme_route(job)
                    controller.cloud_campaign.admit_durability_tier(job, {"durability_tier": "native"}, manifest)


    def test_spdk_live_requires_rdma_and_remains_distinct_from_sealed(self):
        controller.require_supported_nvme_route({"backend": "tidehunter_nvme_rdma",
            "nvmeof_route": "live", "nvmeof_target": "spdk"})
        for job in (
                {"backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"},
                {"backend": "tidehunter_nvme_tcp", "nvmeof_route": "sealed_read", "sealed_dataset": {}},
                {"backend": "native", "sealed_dataset": {}}):
            with self.subTest(job=job), self.assertRaises(controller.SafetyError):
                controller.require_supported_nvme_route({**job, "nvmeof_target": "spdk"})

    def test_live_route_rejected_before_target_connection(self):
        runtime = object.__new__(controller.Controller)
        runtime.shared_tide = None
        runtime.active_nvme = None
        with self.assertRaisesRegex(controller.SafetyError, "live NVMe route"):
            runtime.connect_nvme({"backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"}, Path("/unused"))

    def test_aws_live_case_rejected_before_owner_start(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        job = next(row for row in controller.cloud_campaign.make_matrix(config["spec"]) if row["backend"] == "native")
        job = {**job, "backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"}
        runtime.guard = Mock()
        runtime.select_monitoring = Mock()
        runtime.finish_monitoring = Mock()
        runtime.prepare_tide = Mock()
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with self.assertRaisesRegex(controller.SafetyError, "pair qualification"):
                runtime.run_case(job)
        runtime.prepare_tide.assert_not_called()

    def test_live_pair_admission_failure_aborts_before_owner_start(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        job = next(row for row in controller.cloud_campaign.make_matrix(config["spec"]) if row["backend"] == "native")
        job = {**job, "backend": "tidehunter_nvme_tcp", "nvmeof_route": "live"}
        runtime.config["platform"] = {"name": "gcp", "runtime": {}}
        runtime.guard = Mock()
        runtime.select_monitoring = Mock()
        runtime.finish_monitoring = Mock()
        runtime.prepare_tide = Mock()
        platform = Mock()
        platform.admit_live_qualification.side_effect = ValueError("pair mismatch")
        with tempfile.TemporaryDirectory() as tmp, patch.object(controller, "platform_module", return_value=platform):
            runtime.root = Path(tmp)
            with self.assertRaisesRegex(controller.SafetyError, "pair qualification failed"):
                runtime.run_case(job)
        runtime.prepare_tide.assert_not_called()

    def test_live_preparation_binds_owner_address_and_stable_host_identity(self):
        config = config_fixture()
        config["spec"]["mode"] = "standard-with-live-nvmeof"
        config["spec"]["backends"] = ["tidehunter_nvme_tcp"]
        prep = controller.live_nvme_preparation(config, "case-1", "tcp")
        self.assertEqual(prep["addresses"], [node["address"] for node in config["fleet"]["nodes"]])
        host_nqn = "nqn.2014-08.org.nvmexpress:uuid:12345678-1234-4234-8234-123456789abc"
        self.assertEqual(controller.live_host_identity(host_nqn),
                         list(controller.uuid.UUID(host_nqn.rsplit(":", 1)[1]).bytes))
        target = controller.live_target_for_owner(config, "case-1", 2, "tcp", host_nqn)
        self.assertEqual(target["address"], config["fleet"]["nodes"][2]["address"])
        self.assertEqual(target["state_dir"], config["fleet"]["data_root"] + "/cells/case-1/node-2/live-nvmeof")
        with self.assertRaises(controller.SafetyError):
            controller.live_nvme_preparation(config, "case-1", "rdma")

    def test_live_spdk_config_is_exact_and_explicit(self):
        config = config_fixture()
        config["spec"].update(mode="standard-with-live-nvmeof",
                              backends=["tidehunter_nvme_rdma"], spdk_comparison=True)
        host = "nqn.2014-08.org.nvmexpress:uuid:12345678-1234-4234-8234-123456789abc"
        with patch.object(controller, "platform_module", return_value=None):
            kernel = controller.live_target_for_owner(config, "case-1", 2, "rdma", host)
            target = controller.live_target_for_owner(config, "case-1", 2, "rdma", host, "spdk")
            self.assertNotIn("spdk", kernel)
            self.assertEqual(set(target["spdk"]), {"rpc_socket", "executable"})
            self.assertEqual(target["spdk"]["executable"], "/opt/test-comparison/runtime/spdk/bin/nvmf_tgt")
            self.assertEqual(target["spdk"]["rpc_socket"],
                             controller.live_target_for_owner(config, "case-1", 2, "rdma", host, "spdk")["spdk"]["rpc_socket"])
            self.assertNotEqual(target["spdk"]["rpc_socket"],
                                controller.live_target_for_owner(config, "case-1", 3, "rdma", host, "spdk")["spdk"]["rpc_socket"])
            config["spec"]["spdk_comparison"] = False
            with self.assertRaises(controller.SafetyError):
                controller.live_target_for_owner(config, "case-1", 2, "rdma", host, "spdk")

    def test_live_spdk_boundary_retains_partial_owner_failures(self):
        runtime = object.__new__(controller.Controller)
        runtime.config = config_fixture()
        def request(index, request, **kwargs):
            if index == 2:
                raise RuntimeError("owner unavailable")
            return {"case_id": "case-1", "index": index, "target": {"daemon": {"pid": index + 1}}}
        runtime.request = Mock(side_effect=request)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            proof = runtime.spdk_boundary(directory, "start",
                                          {"case_id": "case-1", "nvmeof_route": "live"})
            rows = json.loads((directory / proof["path"]).read_text())
            self.assertEqual(len(rows), 5)
            self.assertEqual(rows[2]["available"], False)
            self.assertIn("owner unavailable", rows[2]["reason"])
            self.assertIn("target", rows[0])


class TracerCleanupTests(unittest.TestCase):
    def test_partial_start_is_stopped_and_evidence_is_bounded(self):
        receipt = {}
        rows = [{"available": False, "reason": "coordinator"}] + [
            {"available": False, "state_dir": f"/owned/{i}", "reason": "tracing unavailable",
             "raw_events": ["retained separately"]} for i in range(5)]
        stop = Mock(return_value=rows)
        self.assertTrue(controller.cleanup_unfinished_tracer(True, False, stop, receipt))
        stop.assert_called_once_with()
        self.assertTrue(receipt["cold_device_cleanup_complete"])
        self.assertNotIn("raw_events", receipt["cold_device_cleanup"][1])

    def test_unconfirmed_stop_aborts_next_case_and_retains_error(self):
        for stop in (Mock(side_effect=TimeoutError("lost response")),
                     Mock(return_value={"available": False, "state_dir": "/owned", "reason": "collector stop timed out"}),
                     Mock(return_value={"available": False, "state_dir": "/owned", "cleanup_error": "instance busy"})):
            receipt = {}
            self.assertFalse(controller.cleanup_unfinished_tracer(True, False, stop, receipt))
            self.assertFalse(receipt["cold_device_cleanup_complete"])
            stop.assert_called_once_with()

    def test_completed_or_never_started_tracer_is_not_stopped_twice(self):
        stop = Mock(side_effect=AssertionError("unexpected stop"))
        self.assertTrue(controller.cleanup_unfinished_tracer(False, False, stop, {}))
        self.assertTrue(controller.cleanup_unfinished_tracer(True, True, stop, {}))
        stop.assert_not_called()

    def test_identity_probe_zero_events_is_ready_and_stops(self):
        start = {"available": True, "backend": "bpftrace-request-identity", "state_dir": "/owned"}
        end = start | {"read_issues": 0, "read_completions": 0, "qualified": False}
        with patch.object(controller, "command", side_effect=[json.dumps(start), json.dumps(end)]) as run:
            proof = controller.probe_trace_capability("/collector", "/wal", "/owned", 30, {})
        self.assertTrue(proof["identity_backend_available"])
        self.assertTrue(proof["cleanup_complete"])
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[1].args[0][3], "stop")

    def test_probe_reports_attach_gap_and_always_attempts_stop(self):
        fallback = {"available": True, "state_dir": "/owned"}
        with patch.object(controller, "command", side_effect=[json.dumps(fallback), json.dumps(fallback)]) as run:
            proof = controller.probe_trace_capability("/collector", "/wal", "/owned", 30, {})
        self.assertFalse(proof["identity_backend_available"])
        self.assertTrue(proof["cleanup_complete"])
        self.assertTrue(proof["gaps"])
        self.assertEqual(run.call_count, 2)
        with patch.object(controller, "command", side_effect=[RuntimeError("start lost"), RuntimeError("stop lost")]) as run:
            proof = controller.probe_trace_capability("/collector", "/wal", "/owned", 30, {})
        self.assertFalse(proof["cleanup_complete"])
        self.assertEqual(run.call_count, 2)

    def test_device_terminal_evidence_preserves_host_scope_and_unknowns(self):
        stop = [{"available": False, "reason": "coordinator not traced"}] + [
            {"available": i != 2, "read_issues": None if i == 2 else i,
             "matched_reads": None, "raw_events": ["not exported"]} for i in range(5)]
        stop[3]["reason"] = "tracefs startup ring unavailable"
        stop[3]["identity_attempt"] = {"returncode": 1, "stderr": "root required"}
        actual = controller.terminal_device_evidence({"backend": "tidehunter_tcp"}, {"cold_device_evidence": stop})
        self.assertEqual([row["host_index"] for row in actual], list(range(5)))
        self.assertIsNone(actual[2]["evidence"]["read_issues"])
        self.assertEqual(actual[2]["evidence"]["identity_attempt"]["returncode"], 1)
        self.assertEqual(actual[2]["evidence"]["reason"], "tracefs startup ring unavailable")
        self.assertNotIn("raw_events", actual[0]["evidence"])
        native = controller.terminal_device_evidence({"backend": "native"},
                    {"cold_device_evidence": {"available": False, "reason": "unavailable"}})
        self.assertEqual(native, [{"host_index": -1, "evidence": {"available": False, "reason": "unavailable"}}])
        self.assertIsNone(controller.terminal_device_evidence({"backend": "tidehunter_tcp"},
                    {"cold_device_evidence": {"available": False, "reason": "host unknown"}}))


def physical_fixture():
    return [{"qualification_only": False, "independent_nvme_admitted": True,
             "host_identity": {"boot_id": f"boot-{i}"},
             "block_devices": {"blockdevices": [{"model": "Amazon EC2 NVMe Instance Storage", "serial": f"serial-{i}"}]}}
            for i in range(6)]
class MissingS3ObjectError(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}



class ConfigTests(unittest.TestCase):
    def test_offline_validation_needs_no_sdk_or_health(self):
        config = config_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(config))
            with patch.object(controller, "make_clients", side_effect=AssertionError("offline opened AWS")), patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(controller.main(["plan", "--config", str(path), "--output", str(Path(tmp) / "plan.json")]), 0)
            value = json.loads((Path(tmp) / "plan.json").read_text())
            self.assertEqual(set(j["backend"] for j in value["jobs"]), set(controller.cloud_campaign.BACKENDS))
            self.assertIn("no cloud health", value["scope"])

    def test_valkey_external_role_inventory_has_five_primaries_and_no_control_server(self):
        config = config_fixture()
        job = next(job for job in controller.cloud_campaign.make_matrix(config["spec"])
                   if job["backend"] == "valkey_tcp")
        coordinator = controller.owned_monitor_processes(config, -1, job)
        self.assertEqual([item["role"] for item in coordinator["processes"]], ["client"])
        options = controller.cloud_layout.backend_options(config["fleet"], "valkey_tcp", "/tmp/client")
        self.assertEqual(options["nodes"], [f"10.0.0.{i+2}:6379" for i in range(5)])
        self.assertEqual((options["required_primary_nodes"], options["required_replication_factor"]), (5, 1))
        roles = []
        for index in range(5):
            deployment = controller.cloud_layout.node_config(config["fleet"], "valkey_tcp", index)
            self.assertEqual(len(deployment["containers"]), 1)
            role = deployment["containers"][0]["service"]
            with patch.object(controller.cloud_layout, "inspect_container", return_value={}), \
                    patch.object(controller, "docker_monitor_identity", return_value={
                        "container_id": str(index) * 64, "pid": 100 + index,
                        "cgroup": f"/owned/valkey-{index}"}):
                observed = controller.owned_monitor_processes(config, index, job)
            self.assertEqual(observed["missing"], [])
            self.assertEqual(observed["processes"], [{"role": role, "cgroup": f"/owned/valkey-{index}"}])
            roles.append(role)
        self.assertEqual(len(set(roles)), 5)


    def test_account_duplicate_nonphysical_and_extra_fields_rejected(self):
        mutations = [lambda c: c.update(account="000000000000"),
            lambda c: c["fleet"]["nodes"][0].update(instance_id=c["fleet"]["coordinator"]["instance_id"]),
            lambda c: c["fleet"].update(qualification_only=True), lambda c: c.update(access_key="not-admitted"),
            lambda c: c.update(prefix="another-run"), lambda c: c.update(deadline_utc="2099-01-01T02:00:00")]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                config = config_fixture()
                mutate(config)
                with self.assertRaises((controller.SafetyError, ValueError)):
                    controller.validate_config(config)

    def test_guarded_paths_refuse_escape_and_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "link").symlink_to(root / "outside")
            for path in (root / ".." / "escape", root / "link" / "file", root.parent / "foreign"):
                with self.subTest(path=path), self.assertRaises(controller.SafetyError):
                    controller.guarded(path, root)

    def test_systemd_limits_and_provider_environment_are_explicit(self):
        config = config_fixture()
        argv = controller.systemd_argv(config, "0000-test", "storage-0", 4, 8192, 60, [config["binary"], "launch.yaml"])
        for expected in ("--property=CPUQuota=400%", "--property=MemoryMax=8192M", "--property=MemorySwapMax=0",
                         "--property=LimitMEMLOCK=infinity", "--property=RuntimeMaxSec=60",
                         "--setenv=FI_EFA_USE_DEVICE_RDMA=1", "--property=IOAccounting=yes",
                         "--property=RemainAfterExit=yes", "--property=CPUAccounting=yes", "--property=MemoryAccounting=yes"):
            self.assertIn(expected, argv)
        self.assertNotIn("--scope", argv)  # The service manager, not SSM, is the long-lived parent.
        argv = controller.systemd_argv(config, "0000-test", "storage-0", 4, 8192, 60, ["node"],
                                      setenv={"TIDEHUNTER_HISTORY_DIAGNOSTICS": "1"})
        self.assertIn("--setenv=TIDEHUNTER_HISTORY_DIAGNOSTICS=1", argv)


class FabricAdmissionTests(unittest.TestCase):
    def test_failed_read_discovery_retains_prior_observations_without_admitting_host(self):
        config = config_fixture()
        clients = clients_fixture(config)
        stored = {}
        clients["s3"].put_object.side_effect = lambda **value: stored.update(value)
        clients["s3"].head_object.side_effect = lambda **_: {
            "ContentLength": len(stored["Body"]), "Metadata": stored["Metadata"]}
        failure = controller.InfrastructureError("fi_getinfo: -61 (No data available)")
        with tempfile.TemporaryDirectory() as tmp, patch.object(
                controller, "command", side_effect=["ordinary observed info", "direct observed info", failure]):
            with self.assertRaises(controller.InfrastructureError) as caught:
                controller.admit_fabrics(config, 2, clients, Path(tmp))
            receipt = json.loads((Path(tmp) / "fabric-admission.json").read_text())
        self.assertIs(caught.exception, failure)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["instance_id"], config["fleet"]["nodes"][2]["instance_id"])
        self.assertEqual(receipt["source_sha256"], config["source_sha256"])
        self.assertEqual(receipt["binary_sha256"], config["binary_sha256"])
        probes = receipt["probes"]
        self.assertEqual(probes["efa_message"]["stdout"], "ordinary observed info")
        self.assertEqual(probes["efa_direct_message"]["stdout"], "direct observed info")
        self.assertEqual(probes["efa_direct_read"]["status"], "failed")
        self.assertIn("No data available", probes["efa_direct_read"]["error"])
        self.assertNotIn("stdout", probes["efa_direct_read"])
        self.assertEqual(json.loads(stored["Body"]), receipt)

    def test_receipt_export_failure_cannot_replace_original_discovery_failure(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["s3"].put_object.side_effect = OSError("receipt upload unavailable")
        failure = controller.InfrastructureError("EFA discovery unavailable")
        with tempfile.TemporaryDirectory() as tmp, patch.object(controller, "command", side_effect=failure):
            with self.assertRaises(controller.InfrastructureError) as caught:
                controller.admit_fabrics(config, -1, clients, Path(tmp))
            receipt = json.loads((Path(tmp) / "fabric-admission.json").read_text())
        self.assertIs(caught.exception, failure)
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["instance_id"], config["fleet"]["coordinator"]["instance_id"])
        self.assertEqual(set(receipt["probes"]), {"efa_message"})
        self.assertEqual(receipt["probes"]["efa_message"]["status"], "failed")


class OwnershipTests(unittest.TestCase):
    def test_exact_six_current_hosts_required(self):
        config = config_fixture()
        good = instance_response(config)
        controller.verify_instances(config, good)
        mutations = [lambda r: r["Reservations"][0].update(OwnerId="000000000000"),
            lambda r: r["Reservations"][0]["Instances"].pop(),
            lambda r: r["Reservations"][0]["Instances"].append(copy.deepcopy(r["Reservations"][0]["Instances"][0])),
            lambda r: r["Reservations"][0]["Instances"][0].update(PrivateIpAddress="10.9.9.9"),
            lambda r: r["Reservations"][0]["Instances"][0]["Tags"].append({"Key": "RunId", "Value": "foreign"}),
            lambda r: r["Reservations"][0]["Instances"][0]["Tags"].append({"Key": "ExpiresAt", "Value": "2098-01-01T00:00:00Z"})]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                broken = copy.deepcopy(good)
                mutate(broken)
                with self.assertRaises(controller.SafetyError):
                    controller.verify_instances(config, broken)

    def test_expired_fleet_allows_cleanup_but_not_work(self):
        config = config_fixture()
        now = datetime(2100, 1, 1, tzinfo=timezone.utc)
        with self.assertRaises(controller.DeadlineError):
            controller.verify_instances(config, instance_response(config), now=now)
        self.assertEqual(len(controller.verify_instances(config, instance_response(config), terminating=True, now=now)), 6)

    def test_pending_is_readiness_only_never_work_admission(self):
        config = config_fixture()
        response = instance_response(config)
        response["Reservations"][0]["Instances"][2]["State"]["Name"] = "pending"
        controller.verify_instances(config, response, starting=True)
        with self.assertRaises(controller.InfrastructureError):
            controller.verify_instances(config, response)

    def test_foreign_tag_aborts_termination_entirely(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ec2"].describe_instances.return_value["Reservations"][0]["Instances"][4]["Tags"].append({"Key": "ManagedBy", "Value": "foreign"})
        runtime = controller.Controller(config, clients)
        with self.assertRaises(controller.SafetyError):
            runtime.terminate()
        clients["ec2"].terminate_instances.assert_not_called()

    def test_expiry_cannot_change_after_initial_ownership_admission(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        runtime.guard()
        for host in clients["ec2"].describe_instances.return_value["Reservations"][0]["Instances"]:
            host["Tags"].append({"Key": "ExpiresAt", "Value": "2099-01-01T03:00:00Z"})
        with self.assertRaisesRegex(controller.SafetyError, "deadline differs|immutable startup admission"):
            runtime.terminate()
        clients["ec2"].terminate_instances.assert_not_called()

    def test_termination_targets_only_admitted_ids_and_validates_response(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        runtime.terminate()
        clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)
        clients["ec2"].terminate_instances.return_value = {"TerminatingInstances": []}
        with self.assertRaises(controller.InfrastructureError):
            runtime.terminate()

    def test_physical_independence_requires_actual_unique_boot_and_nvme(self):
        controller.validate_physical_proofs(physical_fixture())
        for field in ("boot", "serial", "qualification", "admission"):
            proofs = physical_fixture()
            if field == "boot":
                proofs[1]["host_identity"]["boot_id"] = proofs[0]["host_identity"]["boot_id"]
            elif field == "serial":
                proofs[1]["block_devices"] = proofs[0]["block_devices"]
            elif field == "qualification":
                proofs[2]["qualification_only"] = True
            else:
                proofs[2]["independent_nvme_admitted"] = False
            with self.subTest(field=field), self.assertRaises(controller.SafetyError):
                controller.validate_physical_proofs(proofs)


class CounterAdmissionTests(unittest.TestCase):
    def test_zero_hits_is_observation_not_fabricated_cache_success(self):
        job = {"backend": "tidehunter_tcp", "coordinator_cache_bytes": 128 * 1024**2}
        evidence = {"schema_version": 1, "backend": "tidehunter_tcp",
            "coordinator": {"read_rpc_count": 42, "cache": {"budget_bytes": 128 * 1024**2, "hits": 0}},
            "owner_read_configuration": [{"partition": i} for i in range(5)]}
        controller.verify_counter_evidence(job, evidence)
        for mutation in (lambda x: x["coordinator"]["cache"].update(budget_bytes=0),
                         lambda x: x.update(coordinator=None),
                         lambda x: x["owner_read_configuration"][0].update(partition=1),
                         lambda x: x["owner_read_configuration"].pop()):
            broken = copy.deepcopy(evidence)
            mutation(broken)
            with self.assertRaises(controller.SafetyError):
                controller.verify_counter_evidence(job, broken)

    def test_live_and_sealed_counter_identities_are_distinct(self):
        evidence = {"schema_version": 1, "coordinator": {
            "read_rpc_count": 0, "cache": {"budget_bytes": 0}},
            "owner_read_configuration": [{"partition": i} for i in range(5)]}
        for backend in ("tidehunter_nvme_tcp", "tidehunter_nvme_rdma"):
            for route, actual in (("sealed_read", backend + "_sealed_read"),
                                  ("live", "tidehunter_live_nvmeof")):
                job = {"backend": backend, "nvmeof_route": route, "coordinator_cache_bytes": 0}
                with self.subTest(backend=backend, route=route):
                    controller.verify_counter_evidence(job, {**evidence, "backend": actual})
                    with self.assertRaises(controller.SafetyError):
                        controller.verify_counter_evidence(job, {**evidence, "backend": backend})




class BackendCleanupTests(unittest.TestCase):
    def test_export_all_then_stop_all_then_reset_all(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.active_backend = "tikv"
        calls = []

        def request(index, payload, **kwargs):
            calls.append((payload["operation"], index))
            if payload["operation"] == "reset-data":
                return {"status": "reset", "data_empty": True, "initialization_removed": True}
            return {"operation": payload["operation"], "host": index}

        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            runtime.request = request
            runtime.stop_backend()
        self.assertEqual(calls, [(operation, index) for operation in ("export", "stop", "reset-data")
                                 for index in range(-1, 5)])
        self.assertIsNone(runtime.active_backend)

    def test_export_or_stop_failure_preserves_data_and_still_attempts_every_stop(self):
        for failed_operation in ("export", "stop"):
            config = config_fixture()
            runtime = controller.Controller(config, clients_fixture(config))
            runtime.active_backend = "tikv"
            calls = []

            def request(index, payload, **kwargs):
                calls.append((payload["operation"], index))
                if payload["operation"] == failed_operation and index == 2:
                    raise controller.InfrastructureError("unknown operation outcome")
                return {"operation": payload["operation"], "host": index}

            with self.subTest(operation=failed_operation), tempfile.TemporaryDirectory() as tmp:
                runtime.root = Path(tmp)
                runtime.request = request
                with self.assertRaises(controller.InfrastructureError):
                    runtime.stop_backend()
            self.assertEqual([index for operation, index in calls if operation == "stop"], list(range(-1, 5)))
            self.assertFalse(any(operation == "reset-data" for operation, _ in calls))
            self.assertIsNone(runtime.active_backend)


    def test_adjacent_same_backend_repetitions_are_distinct_fresh_blocks(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        jobs = [{"backend": "tikv", "repeat": repeat, "case_id": f"case-{repeat}"} for repeat in range(2)]
        events = []

        def start(backend):
            events.append(("start", backend))
            runtime.active_backend = backend

        def stop():
            if runtime.active_backend:
                events.append(("stop", runtime.active_backend))
                runtime.active_backend = None

        def run_case(job):
            controller.dump(runtime.root / "cases" / job["case_id"] / "receipt.json", {"status": "completed"})

        runtime.bootstrap = Mock()
        runtime.start_backend = start
        runtime.stop_backend = stop
        runtime.run_case = run_case
        runtime.persist = Mock()
        runtime.export = Mock(return_value={"complete": True})
        runtime.terminate = Mock(return_value={})
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller.cloud_campaign, "make_matrix", return_value=jobs):
                runtime.run()
        self.assertEqual(events, [("start", "tikv"), ("stop", "tikv"), ("start", "tikv"), ("stop", "tikv")])


class CommandSafetyTests(unittest.TestCase):
    def test_lost_send_ack_accepts_exact_completed_receipt_without_resend(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.side_effect = TimeoutError("response lost after acceptance")
        runtime = controller.Controller(config, clients)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json") as read_receipt:
                read_receipt.side_effect = lambda _client, _config, _key, limit=controller.MAX_JSON_BYTES: {
                    "request": {"action": "snapshot", "index": 0},
                    "instance_id": runtime.ids[1], "status": "completed",
                    "result": {"snapshot": "exact-result"}}
                self.assertEqual(runtime.request(0, {"action": "snapshot"}), {"snapshot": "exact-result"})
                self.assertEqual(read_receipt.call_count, 1)
        clients["ssm"].send_command.assert_called_once()
        clients["ssm"].get_command_invocation.assert_not_called()
    def test_unknown_mutating_snapshot_is_reconciled_without_resending(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.side_effect = TimeoutError("lost ack")
        now = [100.0]
        runtime = controller.Controller(config, clients, clock=lambda: now[0])
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json", side_effect=MissingS3ObjectError()), \
                 patch.object(controller.time, "sleep", side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds)):
                with self.assertRaises(controller.InfrastructureError):
                    runtime.request(0, {"action": "snapshot", "drop": True}, timeout=3)
        clients["ssm"].send_command.assert_called_once()

    def test_snapshot_retry_is_per_host_and_never_retries_mutations(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        calls = []
        def request(index, req, **kwargs):
            calls.append((index, kwargs["timeout"]))
            if index == 3 and sum(i == 3 for i, _ in calls) == 1:
                raise controller.SnapshotTimeout("late")
            return {"index": index}
        runtime.request = request
        result = runtime.all_hosts({"action": "snapshot", "drop": False}, timeout=60)
        self.assertEqual([row["index"] for row in result], list(range(-1, 5)))
        self.assertEqual(sum(index == 3 for index, _ in calls), 2)
        self.assertTrue(all(sum(i == index for i, _ in calls) == 1 for index in (-1, 0, 1, 2, 4)))
        for mutation in ({"action": "snapshot", "drop": True},
                         {"action": "snapshot", "monitor_job": {"case_id": "x"}},
                         {"action": "layout", "operation": "start"}):
            calls.clear()
            with self.assertRaises(controller.SnapshotTimeout):
                runtime.all_hosts(mutation, timeout=60)
            self.assertEqual(sum(index == 3 for index, _ in calls), 1)

    def test_snapshot_first_attempt_keeps_sixty_second_budget(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        runtime.request = Mock(side_effect=lambda index, req, **kwargs: {"index": index})
        runtime.all_hosts({"action": "snapshot"}, timeout=110)
        remote = [call.kwargs["timeout"] for call in runtime.request.call_args_list if call.args[0] != -1]
        self.assertEqual(remote, [60] * 5)

    def test_snapshot_retry_does_not_hide_safety_errors(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        runtime.request = Mock(side_effect=controller.SafetyError("bad receipt"))
        with self.assertRaises(controller.SafetyError):
            runtime.all_hosts({"action": "snapshot"}, timeout=60)
        self.assertLessEqual(runtime.request.call_count, 6)

    def test_read_snapshot_safety_failure_does_not_wait_for_slow_host(self):
        runtime = controller.Controller(config_fixture(), clients_fixture(config_fixture()))
        slow_started = threading.Event()
        release = threading.Event()
        returned = threading.Event()
        errors = []
        def request(index, req, **kwargs):
            if index == -1:
                slow_started.set()
                release.wait(5)
            elif index == 0:
                self.assertTrue(slow_started.wait(5))
                raise controller.SafetyError("bad binding")
            return {"index": index}
        def run():
            try:
                runtime.all_hosts({"action": "snapshot", "drop": False}, timeout=60)
            except Exception as exc:
                errors.append(exc)
            finally:
                returned.set()
        runtime.request = request
        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(returned.wait(2), "snapshot failure waited for unrelated host")
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], controller.SafetyError)
        finally:
            release.set()
            thread.join(5)

    def test_timed_out_read_snapshot_reconciles_exact_receipt(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.return_value = {"Command": {"CommandId": "00000000-0000-0000-0000-000000000000"}}
        clients["ssm"].get_command_invocation.return_value = {"Status": "TimedOut", "ResponseCode": -1}
        runtime = controller.Controller(config, clients)
        receipt = {"request": {"action": "snapshot", "index": 0}, "instance_id": runtime.ids[1],
                   "status": "completed", "result": {"snapshot": "bound"}}
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json", return_value=receipt):
                self.assertEqual(runtime.request(0, {"action": "snapshot"}), {"snapshot": "bound"})
        clients["ssm"].send_command.assert_called_once()


    def test_absent_receipt_replays_only_identical_invocation_with_bounded_attempts(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.side_effect = [TimeoutError("lost ack")] * 3
        now = [100.0]
        runtime = controller.Controller(config, clients, clock=lambda: now[0])
        def advance(seconds):
            now[0] += seconds
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json", side_effect=MissingS3ObjectError()), \
                 patch.object(controller.time, "sleep", side_effect=advance):
                with self.assertRaises(controller.InfrastructureError):
                    runtime.request(0, {"action": "snapshot"}, timeout=3)
        self.assertEqual(clients["ssm"].send_command.call_count, 3)
        calls = clients["ssm"].send_command.call_args_list
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[1], calls[2])

    def test_untrusted_or_incomplete_receipts_never_succeed(self):
        config = config_fixture()
        cases = (
            ("null object", lambda runtime: None),
            ("array object", lambda runtime: []),
            ("foreign host", lambda runtime: {
                "request": {"action": "snapshot", "index": 0}, "instance_id": "i-foreign",
                "status": "completed", "result": {"forged": True}}),
            ("foreign request", lambda runtime: {
                "request": {"action": "other", "index": 0}, "instance_id": runtime.ids[1],
                "status": "completed", "result": {"forged": True}}),
            ("failed", lambda runtime: {
                "request": {"action": "snapshot", "index": 0}, "instance_id": runtime.ids[1],
                "status": "failed", "error": "operation failed"}),
            ("missing status", lambda runtime: {
                "request": {"action": "snapshot", "index": 0}, "instance_id": runtime.ids[1],
                "result": {"forged": True}}),
            ("missing result", lambda runtime: {
                "request": {"action": "snapshot", "index": 0}, "instance_id": runtime.ids[1],
                "status": "completed"}),
        )
        for name, make_receipt in cases:
            with self.subTest(name=name):
                clients = clients_fixture(config)
                clients["ssm"].send_command.side_effect = TimeoutError("lost ack")
                runtime = controller.Controller(config, clients)
                with tempfile.TemporaryDirectory() as tmp:
                    runtime.root = Path(tmp)
                    with patch.object(controller, "read_s3_json", side_effect=lambda *_: make_receipt(runtime)):
                        with self.assertRaises((controller.InfrastructureError, controller.SafetyError)):
                            runtime.request(0, {"action": "snapshot"}, timeout=3)
                clients["ssm"].send_command.assert_called_once()

    def test_expired_ambiguous_request_is_not_replayed_or_reported_successful(self):
        config = config_fixture()
        clients = clients_fixture(config)
        now = [100.0]
        def lost_ack_after_deadline(**_kwargs):
            now[0] += 2
            raise TimeoutError("lost ack after operation deadline")
        clients["ssm"].send_command.side_effect = lost_ack_after_deadline
        runtime = controller.Controller(config, clients, clock=lambda: now[0])
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json", side_effect=MissingS3ObjectError()):
                with self.assertRaises(controller.InfrastructureError):
                    runtime.request(0, {"action": "snapshot"}, timeout=1)
        clients["ssm"].send_command.assert_called_once()


    def test_failed_invocation_status_is_not_hidden_by_receipt(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.return_value = {"Command": {"CommandId": "00000000-0000-0000-0000-000000000000"}}
        clients["ssm"].get_command_invocation.return_value = {"Status": "Failed", "ResponseCode": 1}
        runtime = controller.Controller(config, clients)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, "read_s3_json") as read_receipt:
                with self.assertRaisesRegex(controller.InfrastructureError, "failed/unknown terminal status"):
                    runtime.request(0, {"action": "snapshot"})
                read_receipt.assert_not_called()

    def test_bootstrap_retries_only_explicit_nonacceptance_not_unknown_outcomes(self):
        config = config_fixture()
        clients = clients_fixture(config)
        rejection = clients["ssm"].exceptions.InvalidInstanceId("agent not registered")
        clients["ssm"].send_command.side_effect = [rejection,
            {"Command": {"CommandId": "00000000-0000-0000-0000-000000000000"}}]
        clients["ssm"].get_command_invocation.return_value = {"Status": "Success", "ResponseCode": 0}
        runtime = controller.Controller(config, clients)
        response = {"request": {"action": "bootstrap", "index": 0}, "instance_id": runtime.ids[1],
                    "status": "completed", "result": {"proof_fixture": True}}
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller.time, "sleep"), patch.object(controller, "read_s3_json", return_value=response):
                self.assertEqual(runtime.request(0, {"action": "bootstrap"}), {"proof_fixture": True})
        self.assertEqual(clients["ssm"].send_command.call_count, 2)
        clients["ssm"].describe_instance_information.assert_not_called()

    def test_unknown_invocation_status_is_not_success(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.return_value = {"Command": {"CommandId": "00000000-0000-0000-0000-000000000000"}}
        clients["ssm"].get_command_invocation.return_value = {"Status": "FutureUnknownStatus", "ResponseCode": 0}
        runtime = controller.Controller(config, clients)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with self.assertRaisesRegex(controller.InfrastructureError, "unknown terminal status"):
                runtime.request(0, {"action": "snapshot"})
            clients["s3"].get_object.assert_not_called()

    def test_work_deadline_does_not_consume_export_reserve(self):
        config = config_fixture()
        now = [100.0]
        runtime = controller.Controller(config, clients_fixture(config), clock=lambda: now[0])
        now[0] = runtime.work_end + 1
        with self.assertRaises(controller.DeadlineError):
            runtime.remaining()
        self.assertGreater(runtime.remaining(export=True), 0)
        now[0] = runtime.end + 1
        with self.assertRaises(controller.DeadlineError):
            runtime.remaining(export=True)

    def test_export_failure_still_terminates_exact_fleet(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            runtime.bootstrap = Mock(side_effect=controller.InfrastructureError("bootstrap failed"))
            runtime.export = Mock(side_effect=OSError("export unavailable"))
            runtime.persist = Mock(side_effect=OSError("S3 unavailable"))
            with self.assertRaises(controller.InfrastructureError):
                runtime.run()
        clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)

    def test_case_failure_and_finalization_failure_preserve_evidence_and_terminate(self):
        for failure in ("receipt", "checkpoint", "progress"):
            for primary in ("exception", "base", None):
                with self.subTest(finalization=failure, primary=primary), tempfile.TemporaryDirectory() as tmp:
                    config = config_fixture()
                    clients = clients_fixture(config)
                    runtime = controller.Controller(config, clients)
                    platform = Mock()
                    platform.terminate.side_effect = lambda config, clients: clients["ec2"].terminate_instances(
                        InstanceIds=runtime.ids)
                    runtime.root = Path(tmp)
                    runtime.bootstrap = Mock()
                    runtime.stop_tide = Mock()
                    runtime.stop_backend = Mock()
                    runtime.export = Mock(return_value={"complete": True})
                    runtime.persist = Mock(side_effect=OSError("progress persistence failed") if failure == "progress" else None)
                    archive_error = (KeyboardInterrupt("checkpoint archive failed") if primary == "base"
                                     else OSError("checkpoint archive failed"))
                    runtime.checkpoint_case = Mock(side_effect=archive_error if failure == "checkpoint" else None)
                    job = {"backend": "native", "repeat": 0, "case_id": "case"}

                    def run_case(_job):
                        path = runtime.root / "cases" / "case" / "receipt.json"
                        path.parent.mkdir(parents=True)
                        path.write_text("invalid json" if failure == "receipt" else
                                        json.dumps({"status": "failed" if primary else "completed"}))
                        if primary == "base":
                            raise SystemExit("preload owner failed")
                        if primary:
                            raise controller.InfrastructureError("preload owner failed")

                    runtime.run_case = run_case
                    with patch.object(controller.cloud_campaign, "make_matrix", return_value=[job]), \
                         patch.object(controller, "platform_module", return_value=platform), \
                         patch.object(controller, "persist_json"), \
                         self.assertRaises(controller.InfrastructureError) as caught:
                        runtime.run()
                    evidence = runtime.export.call_args.args[0]
                    self.assertEqual(json.loads(str(caught.exception))["controller_error"], evidence)
                    secondary = {"receipt": "JSONDecodeError", "checkpoint": "checkpoint archive failed",
                                 "progress": "progress persistence failed"}[failure]
                    self.assertIn(secondary, evidence)
                    if primary:
                        kind = "SystemExit" if primary == "base" else "InfrastructureError"
                        self.assertTrue(evidence.startswith(f"{kind}: preload owner failed; case finalization: "))
                    else:
                        self.assertNotIn("preload owner failed", evidence)
                    runtime.stop_tide.assert_called_once()
                    self.assertEqual(runtime.stop_backend.call_count, 2)
                    clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)

    def test_cleanup_failure_does_not_prevent_export_or_termination(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        runtime.bootstrap = Mock(side_effect=RuntimeError("failed before cases"))
        runtime.stop_backend = Mock(side_effect=RuntimeError("cleanup failed"))
        runtime.export = Mock(return_value={"complete": False})
        with self.assertRaises(controller.InfrastructureError):
            runtime.run()
        runtime.export.assert_called_once()
        self.assertIn("cleanup failed", runtime.export.call_args.args[0])
        clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)
        self.assertLessEqual(runtime.cleanup_deadline(), runtime.end - config["spec"]["export_reserve_seconds"] / 2)
        with self.assertRaises(controller.DeadlineError):
            runtime.cleanup_timeout(runtime.clock() - 1)

    def test_local_cache_drop_rechecks_current_tags_before_touching_host(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ec2"].describe_instances.return_value["Reservations"][0]["Instances"][0]["Tags"].append({"Key": "RunId", "Value": "foreign"})
        runtime = controller.Controller(config, clients)
        with patch.object(controller, "guest_operation") as operation:
            with self.assertRaises(controller.SafetyError):
                runtime.request(-1, {"action": "snapshot", "drop": True})
        operation.assert_not_called()

    def test_bad_normalization_aborts_before_starting_workload(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        job = next(row for row in controller.cloud_campaign.make_matrix(config["spec"]) if row["backend"] == "native")
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller.campaign, "admit_config", return_value={"wrong": "config"}), \
                 patch.object(controller.analyze, "verify_normalization", side_effect=ValueError("changed field")), \
                 patch.object(controller, "command") as command:
                with self.assertRaises(controller.SafetyError):
                    runtime.run_case(job)
            command.assert_not_called()
            directory = runtime.root / "cases" / job["case_id"]
            self.assertFalse((directory / "measurement.release").exists())
            self.assertEqual(json.loads((directory / "receipt.json").read_text())["status"], "failed")


    def test_uncertain_client_start_and_failed_stop_never_remove_live_database(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        job = next(row for row in controller.cloud_campaign.make_matrix(config["spec"]) if row["backend"] == "native")
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            database = runtime.root / "cases" / job["case_id"] / "database"

            def failed_command(argv, **kwargs):
                if argv[0] == "systemd-run":
                    database.mkdir()
                    (database / "live-wal").write_text("not-safe-to-delete")
                    raise controller.InfrastructureError("start response unknown")
                raise controller.InfrastructureError("stop failed")

            with patch.object(controller.campaign, "admit_config", return_value={}), \
                 patch.object(controller.analyze, "verify_normalization"), \
                 patch.object(controller, "command", side_effect=failed_command):
                with self.assertRaises(controller.InfrastructureError) as raised:
                    runtime.run_case(job)
            self.assertTrue((database / "live-wal").exists())
            receipt = json.loads((database.parent / "receipt.json").read_text())
            self.assertTrue(receipt["database_files_retained"])
            self.assertEqual(receipt["status"], "failed")
            self.assertIn("start response unknown", receipt["error"])
            self.assertEqual([error["phase"] for error in receipt["cleanup_errors"]], ["client_stop"])
            self.assertEqual(receipt["cleanup_errors"][0]["type"], "InfrastructureError")
            self.assertIn("stop failed", receipt["cleanup_errors"][0]["message"])
            self.assertIn("start response unknown", str(raised.exception))
            self.assertIn("stop failed", str(raised.exception))

    def test_live_case_core_failure_precedes_route_and_preserves_cleanup(self):
        self._exercise_live_case_diagnostics(failed=True)


    def test_live_diagnostic_captures_preload_failure_boundaries(self):
        self._exercise_live_case_diagnostics(failed=True, observe=True)

    def test_live_cold_case_traces_preload_without_profiling(self):
        self._exercise_live_case_diagnostics(failed=True, observe=True, cold=True)

    def test_completed_live_case_without_route_proof_is_rejected(self):
        self._exercise_live_case_diagnostics(failed=False)

    def _exercise_live_case_diagnostics(self, failed, observe=False, cold=False):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.config["platform"] = {"name": "gcp", "runtime": {}}
        runtime.monitoring_complete = True
        runtime.select_monitoring = Mock()
        runtime.finish_monitoring = Mock()
        runtime.guard = Mock()
        runtime.live_boundaries = Mock(return_value=[])
        runtime.live_boundary_deltas = Mock(return_value=[])
        events = []
        if observe:
            runtime.live_boundaries = Mock(side_effect=lambda case, boundary, directory, expected, **kw:
                                           events.append("boundary-" + boundary) or [])
            runtime.live_boundary_deltas = Mock(return_value=[])
            def hosts(request, **kwargs):
                action = request["action"]
                if action.startswith("cold_trace_"):
                    events.append(action)
                    return [{"state_dir": "/owned", "available": True} for _ in range(6)]
                return [{"resource_accounting": {}} for _ in range(6)]
            runtime.all_hosts = Mock(side_effect=hosts)
        else:
            runtime.all_hosts = Mock(return_value=[{"resource_accounting": {}} for _ in range(6)])
        runtime.observe_resource_interval = Mock(return_value={})
        runtime.prepare_tide = Mock(return_value={"external_remote_config": {"live_nvme": {
            "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:12345678-1234-4234-8234-123456789abc",
            "host_id": list(controller.uuid.UUID("12345678-1234-4234-8234-123456789abc").bytes)}}})
        runtime.stop_tide = Mock(side_effect=lambda: (_ for _ in ()).throw(
            controller.InfrastructureError("client quiescence unconfirmed; shared owner data retained"))
            if runtime.tide_cleanup_forbidden else None)
        job = next(row for row in controller.cloud_campaign.make_matrix(config["spec"]) if row["backend"] == "native")
        job = {**job, "backend": "tidehunter_nvme_rdma", "nvmeof_route": "live"}
        if cold:
            job["cohort"] = "cold_os_cache"
        elif observe:
            job["diagnostics"] = {"write_timeline_limit": 16}
        job.pop("read_tier", None)
        admitted = {"dataset": {"keys": 2}, "phases": {"preload": True, "warmup_operations": 0,
            "aging_operations": 0, "measurement_operations": 1}, "execution": {"concurrency": 1}}
        run_config = {"backend": {"options": {}}, "limits": {"deadline_seconds": 30}}
        terminal = {"SubState": "exited", "ActiveState": "failed" if failed else "inactive",
                    "Result": "exit-code" if failed else "success", "ExecMainStatus": "1" if failed else "0"}
        active = {"SubState": "running", "ActiveState": "active"}
        stopped = {"LoadState": "loaded", "ActiveState": "inactive", "MainPID": "0"}
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            directory = runtime.root / "cases" / job["case_id"]
            if observe:
                runtime.active_tide = (job, directory, [0])
                runtime.failure_owner_evidence = Mock(
                    side_effect=lambda directory: events.append("owner-evidence") or [{"index": 0}])
                def stop_owner():
                    events.append("owner-stop")
                    if runtime.tide_cleanup_forbidden:
                        raise controller.InfrastructureError("client quiescence unconfirmed; shared owner data retained")
                runtime.stop_tide = Mock(side_effect=stop_owner)
            def run_command(argv, **kwargs):
                if argv[0] == "systemd-run":
                    events.append("client-start")
                    result = directory / "result"
                    result.mkdir()
                    core_status = "failed" if failed else "completed"
                    controller.dump(result / "manifest.json", {"schema_version": 1, "status": core_status,
                        "backend_info": {}, "raw_samples_complete": True})
                    controller.dump(result / "summary.json", {"status": core_status, "backend": {}})
                    (result / "samples.csv").write_text(
                        "phase,operation,outcome,error_kind\npreload,put,error,authority stopped before acknowledgement\n"
                        if failed else "phase,operation,outcome,error_kind\nmeasurement,get,success,\n")
                    (result / "metrics.prom").write_text("")
                    if not failed:
                        (directory / "measurement.ready").touch()
                        (directory / "measurement.complete").touch()
                return ""
            counts = {"preload": 2, "warmup": 0, "aging": 0, "measurement": 0 if failed else 1}
            with ExitStack() as stack:
                def mock(obj, name, **kwargs):
                    return stack.enter_context(patch.object(obj, name, **kwargs))
                mock(controller, "platform_module", return_value=Mock())
                mock(controller, "live_nvme_preparation", return_value={
                    "addresses": ["10.0.0.2"], "service": 4420})
                mock(controller.cloud_campaign, "make_run_config", return_value=run_config)
                mock(controller.campaign, "admit_config", return_value=admitted)
                mock(controller.analyze, "verify_normalization")
                mock(controller.campaign, "verify_run_identity")
                mock(controller.campaign, "verify_timing_result", return_value=counts)
                for name in ("admit_wal_eviction_evidence", "admit_write_diagnostics",
                             "admit_read_diagnostics", "admit_valkey_diagnostics", "admit_write_timeline",
                             "admit_measurement_read_evidence"):
                    mock(controller.cloud_campaign, name, return_value=None)
                mock(controller, "command", side_effect=run_command)
                def failed_initiator(*args):
                    events.append("initiator-evidence")
                    raise OSError("collector unavailable")
                mock(controller, "initiator_failure_frame", side_effect=failed_initiator)
                mock(controller, "unit_status", side_effect=[terminal, terminal, stopped] if failed else [active, terminal, stopped])
                mock(controller, "snapshot", return_value={})
                mock(controller.resource_accounting, "interval", return_value={})
                mock(controller, "host_cpu_interval", return_value={})
                if failed:
                    with self.assertRaises(controller.InfrastructureError) as raised:
                        runtime.run_case(job)
                    self.assertIn("authority stopped before acknowledgement", str(raised.exception))
                    self.assertIn("shared owner data retained", str(raised.exception))
                else:
                    receipt = runtime.run_case(job)
            receipt = json.loads((directory / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual(receipt["validated_timing_rows_by_phase"]["measurement"], counts["measurement"])
            qualification = receipt["live_nvme_measurement_qualification"]
            self.assertFalse(qualification["qualified"])
            self.assertIn("live NVMe boundary counters unavailable", qualification["reasons"])
            self.assertIn("no verified kernel live NVMe namespace route", qualification["reasons"])
            if failed:
                self.assertIn('"phase": "preload"', receipt["error"])
                self.assertIn('"outcome": "error"', receipt["error"])
                self.assertIn("authority stopped before acknowledgement", receipt["error"])
                self.assertEqual(receipt["failure_initiator_evidence"],
                                 {"available": False, "reason": "OSError"})
                self.assertEqual(receipt["cleanup_errors"][0]["phase"], "owner_storage")
                self.assertTrue(runtime.tide_cleanup_forbidden)
                if observe:
                    self.assertLess(events.index("boundary-begin"), events.index("client-start"))
                    self.assertLess(events.index("cold_trace_start"), events.index("client-start"))
                    self.assertLess(events.index("client-start"), events.index("boundary-end"))
                    self.assertLess(events.index("boundary-end"), events.index("cold_trace_stop"))
                    self.assertTrue(receipt["live_block_trace_cleanup_complete"])
                    self.assertLess(events.index("initiator-evidence"), events.index("owner-stop"))
                    self.assertLess(events.index("owner-evidence"), events.index("owner-stop"))
                    self.assertEqual(receipt["failure_owner_evidence"], [{"index": 0}])
            else:
                self.assertIn("live NVMe measurement path was not qualified", receipt["error"])

    def test_systemd_not_found_is_observed_without_inventing_exit_success(self):
        observed = controller.subprocess.CompletedProcess([], 1, "LoadState=not-found\nActiveState=inactive\n", "Unit not found")
        with patch.object(controller.subprocess, "run", return_value=observed):
            status = controller.unit_status("kvcloud-owned.service")
        self.assertEqual(status["LoadState"], "not-found")
        self.assertNotIn("Result", status)
        self.assertNotIn("MainPID", status)
        unknown = controller.subprocess.CompletedProcess([], 1, "", "unknown manager error")
        with patch.object(controller.subprocess, "run", return_value=unknown):
            with self.assertRaises(controller.InfrastructureError):
                controller.unit_status("kvcloud-owned.service")


class CapsuleAndExportTests(unittest.TestCase):
    def make_tar(self, path, name, *, symlink=False):
        with tarfile.open(path, "w:gz") as archive:
            member = tarfile.TarInfo(name)
            if symlink:
                member.type = tarfile.SYMTYPE
                member.linkname = "/etc/passwd"
                archive.addfile(member)
            else:
                member.size = 4
                archive.addfile(member, io.BytesIO(b"test"))

    def test_capsule_refuses_traversal_links_and_unexpected_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for number, (name, symlink, expected) in enumerate([
                    ("../escape", False, ["../escape"]), ("launch.yaml", True, ["launch.yaml"]),
                    ("ca.key", False, ["launch.yaml"]) ]):
                archive = root / f"{number}.tar.gz"
                self.make_tar(archive, name, symlink=symlink)
                with self.subTest(name=name), self.assertRaises(controller.SafetyError):
                    controller.safe_extract_capsule(archive, root / f"out-{number}", expected)

    def test_raw_export_keeps_samples_configs_and_proof_not_secrets_or_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "results"
            for name in ("matrix.json", "cases/case/config.json", "cases/case/result/samples.csv",
                         "cases/case/result/read-diagnostics.json",
                         "cases/case/prepared/tls/ca.key", "cases/case/prepared/tls/node.pk8",
                         "cases/case/database/wal", "cases/case/storage-0.tar.gz"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"retained-or-denied")
            archive = Path(tmp) / "result.tar.gz"
            records = controller.archive_results(root, archive)
            self.assertEqual(set(records), {"matrix.json", "cases/case/config.json",
                "cases/case/result/samples.csv", "cases/case/result/read-diagnostics.json"})
            with tarfile.open(archive) as bundle:
                self.assertEqual(set(bundle.getnames()), set(records))
                self.assertTrue(all(m.isfile() for m in bundle.getmembers()))

    def test_head_mismatch_fails_json_admission(self):
        config = config_fixture()
        s3 = Mock()
        s3.head_object.return_value = {"ContentLength": 0, "Metadata": {"sha256": "wrong"}}
        with self.assertRaises(controller.InfrastructureError):
            controller.s3_json(s3, config, config["prefix"] + "/results/startup.json", {"status": "starting"})
        with self.assertRaises(controller.SafetyError):
            controller.s3_json(s3, config, "foreign/results/startup.json", {})

    def test_typed_launch_rejects_foreign_tls_or_node_root(self):
        config = config_fixture()
        case = "0000-case"
        root = Path(config["fleet"]["data_root"]) / "cells" / case
        launch = {"ready_file": str(root / "node-0/ready"), "stop_file": str(root / "node-0/stop"),
            "node": {"root": str(root / "node-0/storage")}, "listen": "10.0.0.2:9400",
            "tls": {"transport": {"kind": "tcp"}, "ca": str(root / "tls/ca.der"),
                    "cert": str(root / "tls/tidehunter-node-0.der"), "key": str(root / "tls/tidehunter-node-0.pk8")}}
        controller.validate_launch(config, launch, case, 0)
        for mutation in (lambda x: x["tls"].update(key=str(root / "tls/ca.key")),
                         lambda x: x["node"].update(root="/mnt/foreign/storage")):
            broken = copy.deepcopy(launch)
            mutation(broken)
            with self.assertRaises(controller.SafetyError):
                controller.validate_launch(config, broken, case, 0)
        trusted = copy.deepcopy(launch)
        trusted["tls"].update(
            transport={"kind": "ofi", "fabric": {"backend": "efa-direct", "domain": "efa0"},
                       "security": "trusted"},
            ca="", cert="", key="")
        controller.validate_launch(config, trusted, case, 0)
        for field in ("ca", "cert", "key"):
            broken = copy.deepcopy(trusted)
            broken["tls"][field] = str(root / "tls/ca.der")
            with self.subTest(field=field), self.assertRaises(controller.SafetyError):
                controller.validate_launch(config, broken, case, 0)

    def test_live_launch_rejects_changed_target_identity_and_state(self):
        config = config_fixture()
        config["spec"]["mode"] = "standard-with-live-nvmeof"
        config["spec"]["backends"] = ["tidehunter_nvme_tcp"]
        case = "0000-case"
        root = Path(config["fleet"]["data_root"]) / "cells" / case
        launch = {"ready_file": str(root / "node-0/ready"), "stop_file": str(root / "node-0/stop"),
            "node": {"root": str(root / "node-0/storage"),
                     "live_target": controller.live_target_for_owner(config, case, 0, "tcp",
                         "nqn.2014-08.org.nvmexpress:uuid:12345678-1234-4234-8234-123456789abc")},
            "listen": "10.0.0.2:9400",
            "tls": {"transport": {"kind": "tcp"}, "ca": str(root / "tls/ca.der"),
                    "cert": str(root / "tls/tidehunter-node-0.der"), "key": str(root / "tls/tidehunter-node-0.pk8")}}
        controller.validate_launch(config, launch, case, 0)
        for field, value in (("state_dir", "/tmp/foreign"), ("address", "10.0.0.3"),
                             ("host_nqn", "nqn.foreign"), ("port_id", 999)):
            broken = copy.deepcopy(launch)
            broken["node"]["live_target"][field] = value
            with self.subTest(field=field), self.assertRaises(controller.SafetyError):
                controller.validate_launch(config, broken, case, 0)


class MonitoringBoundaryTests(unittest.TestCase):
    def test_targets_are_only_the_admitted_fleet_and_state_is_not_on_nvme(self):
        config = config_fixture()
        spec = controller.monitoring_spec(config, Path(config["fleet"]["data_root"]) / "results")
        self.assertEqual([host["address"] for host in spec["hosts"]],
            [host["address"] for host in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]])
        self.assertEqual([host["index"] for host in spec["hosts"]], list(range(-1, 5)))
        self.assertEqual(spec["deadline_utc"], config["deadline_utc"])
        self.assertEqual(spec["state_dir"], "/var/lib/tidehunter-observation/" + config["prefix"])
        self.assertFalse(spec["allow_loopback_hosts"])
        self.assertNotIn("instance_id", json.dumps(spec))

    def test_private_access_commands_bind_actual_coordinator_without_claiming_iam_grants(self):
        config = config_fixture()
        proofs = [{"snapshot": {"nvme": {"mount": {"filesystems": [
            {"source": f"/dev/nvme{index + 1}n1p1[/run]"}]}}}} for index in range(6)]
        receipt = controller.monitoring_access(config, proofs)
        self.assertEqual(receipt["coordinator_instance_id"], config["fleet"]["coordinator"]["instance_id"])
        self.assertEqual(receipt["region"], config["fleet"]["region"])
        for service, port in (("grafana", 13000), ("prometheus", 19090)):
            command = receipt["access"][service]
            argv = controller.shlex.split(command["command"])
            self.assertEqual(argv, command["argv"])
            self.assertEqual(argv[argv.index("--target") + 1], receipt["coordinator_instance_id"])
            self.assertEqual(argv[argv.index("--region") + 1], receipt["region"])
            self.assertEqual(json.loads(argv[-1]), {"portNumber": [str(port)], "localPortNumber": [str(port)]})
        self.assertIn("unknown", receipt["iam_granted"])
        self.assertEqual(receipt["required_permissions"][0]["action"], "ssm:StartSession")
        self.assertEqual(receipt["hosts"][0]["wal_device"], "nvme1n1")
        self.assertEqual(receipt["hosts"][-1]["labels"],
            {"run_id": config["prefix"], "host_index": "4", "host_role": "storage-4"})
        missing = controller.monitoring_access(config)
        self.assertNotIn("wal_device", missing["hosts"][0])
        self.assertIn("device_evidence_gap", missing["hosts"][0])

    def test_exact_docker_pid_membership_is_required_and_cgroup_v1_is_not_faked(self):
        identifier = "a" * 64
        item = {"Id": identifier, "State": {"Running": True, "Pid": 1234}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "1234").mkdir()
            path = root / "1234/cgroup"
            for group in ("/system.slice/docker-" + identifier + ".scope",
                          "/docker/system.slice/docker-" + identifier + ".scope"):
                path.write_text("0::" + group + "\n")
                observed = controller.docker_monitor_identity(item, root)
                self.assertEqual(observed, {"container_id": identifier, "pid": 1234, "cgroup": group})
            for content in ("1:cpu:/docker/" + identifier + "\n",
                            "0::/system.slice/docker-" + "b" * 64 + ".scope\n",
                            "0::/user.slice/session.scope\n", "0::/docker/" + identifier + "/../../other\n",
                            "x" * 16385):
                path.write_text(content)
                with self.subTest(content=content[:80]), self.assertRaises(ValueError):
                    controller.docker_monitor_identity(item, root)
            item["State"]["Pid"] = 0
            with self.assertRaises(ValueError):
                controller.docker_monitor_identity(item, root)

    def test_observer_failure_stays_incomplete_after_later_success_and_records_time(self):
        config = config_fixture()
        clock = Mock(side_effect=[0, 10, 10.25, 11, 11.5])
        runtime = controller.Controller(config, clients_fixture(config), clock=clock)
        runtime.monitoring_attempted = True
        def fail():
            raise OSError("observer socket unavailable")
        self.assertIsNone(runtime.observe("collector_start", fail))
        self.assertEqual(runtime.observe("collector_stop", lambda: {"complete": True}), {"complete": True})
        receipt = runtime.monitoring_receipt()
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["events"][0]["elapsed_seconds"], 0.25)
        self.assertEqual(receipt["events"][1]["elapsed_seconds"], 0.5)
        self.assertIn("observer socket unavailable", receipt["events"][0]["error"])

    def test_monitoring_sends_compact_correlation_without_mutating_raw(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        runtime.monitoring_attempted = True
        runtime.remaining = Mock(return_value=100)
        observer = Mock()
        observer.finish_case.return_value = {"complete": True}
        exporter = Mock()
        exporter.summarize_correlated_timings.return_value = {"kind": "read", "stages": []}
        raw = {"runtime_enabled": True,
               "profile": {"caller_correlation": {"trace": [{"sample": i} for i in range(40)]}}}
        receipt = {"raw_artifacts_validated": False, "status": "failed",
                   "read_diagnostics": {"observed": raw}}
        job = {"case_id": "case", "backend": "tidehunter_tcp"}
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(controller, "monitor_module", return_value=observer), \
             patch.object(controller, "monitoring_spec", return_value={}), \
             patch.object(controller.cloud_campaign, "diagnostic_backend", return_value="tidehunter_tcp"), \
             patch.dict("sys.modules", {"campaign_exporter": exporter}):
            runtime.root = Path(tmp)
            runtime.finish_monitoring(job, Path(tmp), receipt)
        kwargs = observer.finish_case.call_args.kwargs
        self.assertEqual(kwargs["correlation_summaries"], [{"kind": "read", "stages": []}])
        self.assertNotIn("caller_correlation", kwargs["read_diagnostics"]["profile"])
        self.assertEqual(len(raw["profile"]["caller_correlation"]["trace"]), 40)

    def test_monitoring_start_failure_blocks_work_and_keeps_cleanup(self):
        observer = controller.monitor_module({"repository_root": str(HERE.parents[1])})
        for failed in ("host", "collector"):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as tmp:
                config = config_fixture()
                runtime = controller.Controller(config, clients_fixture(config))
                runtime.root = Path(tmp) / "results"
                runtime.request = lambda index, request, **kwargs: {
                    "complete": not (failed == "host" and index == 2)}
                def bootstrap():
                    runtime.root.mkdir()
                    runtime.root_owned = True
                    runtime.start_monitoring()
                runtime.bootstrap = bootstrap
                runtime.run_case = Mock()
                runtime.export = Mock(return_value={"complete": False})
                runtime.stop_tide = Mock()
                runtime.stop_backend = Mock()
                runtime.terminate = Mock(return_value={"external_lifecycle_owner": True})
                with patch.object(observer, "start_collector",
                                  side_effect=OSError("Grafana unavailable")) as collector:
                    with self.assertRaises(controller.InfrastructureError):
                        runtime.run()
                runtime.run_case.assert_not_called()
                runtime.export.assert_called_once()
                self.assertIn("readiness failed", runtime.export.call_args.args[0])
                runtime.terminate.assert_called_once()
                self.assertFalse(runtime.monitoring_receipt()["complete"])
                self.assertEqual(collector.call_count, 0 if failed == "host" else 1)

    def test_monitoring_loss_stops_before_next_case(self):
        config = config_fixture()
        runtime = controller.Controller(config, clients_fixture(config))
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp) / "results"
            def bootstrap():
                runtime.root.mkdir()
                runtime.root_owned = True
                runtime.monitoring_attempted = True
                runtime.monitoring_complete = False
            runtime.bootstrap = bootstrap
            runtime.run_case = Mock()
            runtime.export = Mock(return_value={"complete": False})
            runtime.stop_tide = Mock()
            runtime.stop_backend = Mock()
            runtime.persist = lambda name, value: controller.dump(runtime.root / name, value)
            runtime.terminate = Mock(return_value={"external_lifecycle_owner": True})
            with self.assertRaises(controller.InfrastructureError):
                runtime.run()
            runtime.run_case.assert_not_called()
            self.assertIn("monitoring failed", runtime.export.call_args.args[0])
            runtime.terminate.assert_called_once()

    def test_io_partial_allows_next_case_but_core_or_observer_failure_does_not(self):
        fixture = json.loads((HERE / "resource-client-boundaries-0024.json").read_text())
        partial = controller.resource_accounting.interval([fixture[0]], [fixture[1]])
        jobs = [{"case_id": "first", "backend": "native", "repeat": 0},
                {"case_id": "second", "backend": "native", "repeat": 0}]
        for failure in ("io", "core", "observer"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                config = config_fixture()
                runtime = controller.Controller(config, clients_fixture(config))
                runtime.root = Path(tmp) / "results"
                def bootstrap():
                    runtime.root.mkdir()
                    runtime.root_owned = True
                    runtime.monitoring_attempted = True
                runtime.bootstrap = bootstrap
                runtime.stop_tide = Mock()
                runtime.stop_backend = Mock()
                runtime.export = Mock(return_value={"complete": True})
                runtime.terminate = Mock(return_value={"external_lifecycle_owner": True})
                runtime.persist = lambda name, value: controller.dump(runtime.root / name, value)
                def run_case(job):
                    if job["case_id"] == "first":
                        if failure == "observer":
                            runtime.observe("collector", lambda: {"complete": False})
                        elif failure == "core":
                            broken = copy.deepcopy(fixture[1])
                            broken["processes"][0]["cpu"]["usage_usec"] = 0
                            runtime.observe_resource_interval("first", controller.resource_accounting.interval(
                                [fixture[0]], [broken]))
                        else:
                            runtime.observe_resource_interval("first", partial)
                runtime.run_case = Mock(side_effect=run_case)
                with patch.object(controller.cloud_campaign, "make_matrix", return_value=jobs), \
                     patch.object(controller, "platform_module", return_value=None):
                    if failure == "io":
                        runtime.run()
                        self.assertEqual(runtime.run_case.call_count, 2)
                        receipt = runtime.monitoring_receipt()
                        self.assertTrue(receipt["complete"])
                        self.assertFalse(receipt["resource_io_complete"])
                        self.assertFalse(receipt["events"][0]["receipt"]["resource_complete"])
                    else:
                        with self.assertRaises(controller.InfrastructureError):
                            runtime.run()
                        self.assertEqual(runtime.run_case.call_count, 1)

    def test_monitoring_cleanup_preserves_raw_upload_floor_even_near_deadline(self):
        config = config_fixture()
        now = [100]
        runtime = controller.Controller(config, clients_fixture(config), clock=lambda: now[0])
        runtime.end = 300
        self.assertEqual(runtime.observer_cleanup_budget(90, following_seconds=40), 10)
        self.assertEqual(runtime.observer_cleanup_budget(20), 20)
        now[0] = 151
        with self.assertRaises(controller.DeadlineError):
            runtime.observer_cleanup_budget(20)
        self.assertEqual(runtime.remaining(export=True), 149)

    def test_monitoring_cleanup_errors_still_upload_real_raw_archive_and_terminate(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        uploads = {}
        def upload(path, bucket, key, **kwargs):
            uploads[key] = Path(path).read_bytes()
        def put(**kwargs):
            uploads[kwargs["Key"]] = kwargs["Body"]
        clients["s3"].put_object.side_effect = put
        def head(**kwargs):
            content = uploads[kwargs["Key"]]
            return {"ContentLength": len(content), "Metadata": {
                "sha256": controller.hashlib.sha256(content).hexdigest()}}
        clients["s3"].upload_file.side_effect = upload
        clients["s3"].head_object.side_effect = head
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp) / "results"
            def bootstrap():
                runtime.root.mkdir()
                runtime.root_owned = True
                runtime.monitoring_attempted = True
                source = runtime.root / "cases/failed/result/samples.csv"
                source.parent.mkdir(parents=True)
                source.write_bytes(b"authoritative,failed,raw\n")
                raise controller.InfrastructureError("workload did not start")
            runtime.bootstrap = bootstrap
            runtime.export_monitoring = Mock(side_effect=OSError("snapshot disk failed"))
            runtime.stop_monitoring = Mock(side_effect=OSError("collector stop failed"))
            runtime.persist = lambda name, value: controller.dump(runtime.root / name, value)
            transfer = Mock()
            with patch.dict(controller.sys.modules, {"boto3.s3.transfer": transfer}), \
                 self.assertRaises(controller.InfrastructureError):
                runtime.run()
            content = uploads[config["prefix"] + "/results/controller.tar.gz"]
            with tarfile.open(fileobj=io.BytesIO(content)) as archive:
                self.assertEqual(archive.extractfile("cases/failed/result/samples.csv").read(),
                                 b"authoritative,failed,raw\n")
                observation = json.load(archive.extractfile("observability/receipt.json"))
            self.assertFalse(observation["complete"])
            self.assertEqual([event["operation"] for event in observation["events"]],
                             ["session_export", "session_stop"])
            self.assertEqual(json.loads((runtime.root / "receipt.json").read_text())["status"], "incomplete")
        clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)

    def test_inventory_publication_failure_preserves_raw_archive_and_termination(self):
        config = config_fixture()
        clients = clients_fixture(config)
        runtime = controller.Controller(config, clients)
        uploads = {}
        def upload(path, bucket, key, **kwargs):
            uploads[key] = Path(path).read_bytes()
        def head(**kwargs):
            raw = uploads[kwargs["Key"]]
            return {"ContentLength": len(raw),
                    "Metadata": {"sha256": controller.hashlib.sha256(raw).hexdigest()}}
        clients["s3"].upload_file.side_effect = upload
        clients["s3"].head_object.side_effect = head
        clients["s3"].put_object.side_effect = OSError("inventory publication unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp) / "results"
            def bootstrap():
                runtime.root.mkdir()
                runtime.root_owned = True
                path = runtime.root / "cases/failed/result/samples.csv"
                path.parent.mkdir(parents=True)
                path.write_bytes(b"retain this complete raw payload\n")
                raise controller.InfrastructureError("workload failed")
            runtime.bootstrap = bootstrap
            runtime.export_monitoring = Mock()
            runtime.stop_monitoring = Mock()
            runtime.persist = lambda name, value: controller.dump(runtime.root / name, value)
            with patch.dict(controller.sys.modules, {"boto3.s3.transfer": Mock()}), \
                    self.assertRaises(controller.InfrastructureError):
                runtime.run()
            with tarfile.open(fileobj=io.BytesIO(uploads[config["prefix"] + "/results/controller.tar.gz"])) as archive:
                self.assertEqual(archive.extractfile("cases/failed/result/samples.csv").read(),
                                 b"retain this complete raw payload\n")
            receipt = json.loads((runtime.root / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "export_failed")
            self.assertIn("inventory publication unavailable", receipt["export_error"])
        clients["ec2"].terminate_instances.assert_called_once_with(InstanceIds=runtime.ids)

    def test_only_exact_monitoring_archive_is_retained_with_raw_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "results"
            paths = ("observability/monitoring.tar.gz", "observability/foreign.tar.gz",
                     "cases/case/storage-0.tar.gz", "cases/case/result/samples.csv")
            for name in paths:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"exact archive bytes")
            archive = Path(tmp) / "raw.tar.gz"
            records = controller.archive_results(root, archive)
            self.assertEqual(set(records), {"observability/monitoring.tar.gz", "cases/case/result/samples.csv"})
            with tarfile.open(archive) as retained:
                self.assertEqual(retained.extractfile("observability/monitoring.tar.gz").read(), b"exact archive bytes")


class TikvDiagnosticsTests(unittest.TestCase):
    def test_node_snapshots_real_metric_names_and_bounded_log_interval(self):
        config = config_fixture()
        opener = MagicMock()
        response = opener.open.return_value.__enter__.return_value
        response.read1.side_effect = [b"tikv_raft_apply_wait_duration_seconds_count 4\n"
                                      b"tikv_storage_command_total 8\nunrelated_metric 2\n", b""]
        since = "2026-11-03T03:52:02+00:00"
        with patch.object(controller.cloud_layout, "require_state"), \
                patch.object(controller.cloud_layout, "inspect_container", return_value={"Id": "a" * 64}), \
                patch.object(controller.cloud_layout, "command", return_value="one log line\n") as logs, \
                patch.object(controller.urllib.request, "build_opener", return_value=opener):
            result = controller.tikv_diagnostics(config, 0, since=since)
        self.assertTrue(result["metrics"]["available"])
        self.assertIn("tikv_raft_apply_wait_duration_seconds_count 4", result["metrics"]["value"])
        self.assertNotIn("unrelated_metric", result["metrics"]["value"])
        self.assertFalse(result["client_retry_counts"]["available"])
        self.assertEqual(result["logs"]["value"]["since"], since)
        self.assertIn("--until", logs.call_args.args[0])
        self.assertIn("--tail", logs.call_args.args[0])
        self.assertLessEqual(result["started"]["monotonic_ns"], result["finished"]["monotonic_ns"])
        opener.open.assert_called_once_with("http://10.0.0.2:20180/metrics", timeout=3)

    def test_pd_region_peers_and_missing_or_oversized_data(self):
        config = config_fixture()
        opener = MagicMock()
        response = opener.open.return_value.__enter__.return_value
        response.read1.side_effect = [
            b'{"max-replicas":1}', b"",
            b'{"count":1,"regions":[{"id":1,"peers":[{"store_id":2}]}]}', b"",
        ]
        with patch.object(controller.cloud_layout, "require_state"), \
                patch.object(controller.cloud_layout, "inspect_container", return_value={"Id": "a" * 64}), \
                patch.object(controller.urllib.request, "build_opener", return_value=opener):
            result = controller.tikv_diagnostics(config, -1)
        self.assertEqual(result["pd_regions"]["value"]["regions"][0]["peers"][0]["store_id"], 2)
        self.assertEqual(result["pd_replication"]["value"]["max-replicas"], 1)
        response.read1.side_effect = None
        response.read1.return_value = b"x" * (4 * 1024 * 1024 + 1)
        with patch.object(controller.cloud_layout, "require_state"), \
                patch.object(controller.cloud_layout, "inspect_container", return_value={"Id": "a" * 64}), \
                patch.object(controller.urllib.request, "build_opener", return_value=opener):
            self.assertFalse(controller.tikv_diagnostics(config, 0)["metrics"]["available"])

    def test_unowned_container_never_queries_endpoint(self):
        with patch.object(controller.cloud_layout, "require_state", side_effect=ValueError("unowned")), \
                patch.object(controller.urllib.request, "build_opener") as opener:
            result = controller.tikv_diagnostics(config_fixture(), 0)
        self.assertFalse(result["container"]["available"])
        self.assertFalse(result["metrics"]["available"])
        opener.assert_not_called()


class OwnerDurabilityReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.ready = Path(self.directory.name) / "ready"
        self.launch = {
            "ready_file": str(self.ready), "listen": "10.0.0.3:9400", "launch_nonce": "nonce-3",
            "durability": {"tier": "synchronous"},
            "node": {"partition": {"partition": 3, "inventory": {"database": list(range(16))}}}}

    def receipt(self, **changes):
        value = {"partition": 3, "database": list(range(16)), "address": "10.0.0.3:9400",
                 "launch_nonce": "nonce-3", "pid": 10, "boot_id": "boot", "started_unix_ns": 100,
                 "durability": {"tier": "synchronous", "sync": {}, "max_pending_completions": 8}}
        return {**value, **changes}

    def publish(self, value):
        controller.publish_synced(
            self.ready.with_name("ready.durability.json"), json.dumps(value).encode())

    def test_receipt_must_exist_and_name_this_owner_launch_and_tier(self):
        with self.assertRaises(controller.InfrastructureError):
            controller.owner_durability_receipt(self.launch)
        foreign = (
            self.receipt(partition=2), self.receipt(database=[0] * 16),
            self.receipt(address="10.0.0.9:9400"), self.receipt(launch_nonce="old"),
            self.receipt(durability={"tier": "native", "sync": {}, "max_pending_completions": 8}),
            {k: v for k, v in self.receipt().items() if k != "boot_id"},
            {**self.receipt(), "extra": 1}, {}, {"durability": {}})
        for value in foreign:
            self.publish(value)
            with self.subTest(value=value), self.assertRaises(controller.SafetyError):
                controller.owner_durability_receipt(self.launch)
        self.publish(self.receipt())
        self.assertEqual(controller.owner_durability_receipt(self.launch), self.receipt())

    def test_a_launch_without_a_tier_means_native_but_a_receipt_must_state_it(self):
        launch = {k: v for k, v in self.launch.items() if k != "durability"}
        self.publish(self.receipt(durability={"tier": "native", "sync": {}, "max_pending_completions": 8}))
        self.assertEqual(controller.owner_durability_receipt(launch)["durability"]["tier"], "native")
        self.publish(self.receipt())
        with self.assertRaises(controller.SafetyError):
            controller.owner_durability_receipt(launch)

    def test_the_start_proof_must_confirm_tier_partition_and_launch_nonce(self):
        proof = {"durability_receipt": self.receipt()}
        self.assertEqual(
            controller.confirm_owner_durability(proof, "synchronous", 3, "nonce-3"), self.receipt())
        bad = ({}, {"durability_receipt": "synchronous"}, {"durability_receipt": {}},
               {"durability_receipt": self.receipt(launch_nonce="x")},
               {"durability_receipt": self.receipt(partition=0)})
        for value in bad:
            with self.assertRaises(controller.SafetyError):
                controller.confirm_owner_durability(value, "synchronous", 3, "nonce-3")
        with self.assertRaises(controller.SafetyError):
            controller.confirm_owner_durability(proof, "native", 3, "nonce-3")

    def test_a_restarted_owner_needs_its_own_later_receipt(self):
        previous = self.receipt()
        fresh = self.receipt(pid=11, started_unix_ns=200)
        fresh_check = controller.cloud_campaign.owner_receipt_is_fresh
        self.assertTrue(fresh_check(previous, fresh))
        # The copy left from the earlier process, or an older or different-owner one, is stale.
        self.assertFalse(fresh_check(previous, dict(previous)))
        self.assertFalse(fresh_check(previous, self.receipt(pid=11, started_unix_ns=50)))
        self.assertFalse(fresh_check(previous, self.receipt(pid=11, started_unix_ns=200, partition=4)))
        self.assertFalse(fresh_check(previous, self.receipt(pid=11, started_unix_ns=200, launch_nonce="n")))
        self.assertFalse(fresh_check(previous, None))

    def test_refresh_replaces_every_receipt_copy_and_rejects_a_stale_one(self):
        paths = []
        for index in range(2):
            path = Path(self.directory.name) / f"node-{index}.json"
            controller.publish_synced(path, json.dumps(self.receipt(partition=index)).encode())
            paths.append(str(path))
        options = {"external_owner_durability_receipts": paths}
        fresh = [{"durability_receipt": self.receipt(partition=i, pid=11, started_unix_ns=200)} for i in range(2)]
        stale = [fresh[0], {"durability_receipt": self.receipt(partition=1)}]
        shell = controller.Controller.__new__(controller.Controller)
        with self.assertRaises(controller.SafetyError):
            shell.refresh_owner_receipts(options, stale, "synchronous")
        # A rejected refresh leaves every copy untouched.
        self.assertEqual(json.loads(Path(paths[0]).read_text())["pid"], 10)
        shell.refresh_owner_receipts(options, fresh, "synchronous")
        self.assertEqual([json.loads(Path(path).read_text())["pid"] for path in paths], [11, 11])

    def test_publish_leaves_no_temporary_file_and_never_overwrites_a_pending_one(self):
        target = Path(self.directory.name) / "owner-durability" / "node-0.durability.json"
        controller.publish_synced(target, b"{}\n")
        self.assertEqual(target.read_bytes(), b"{}\n")
        self.assertEqual([path.name for path in target.parent.iterdir()], [target.name])
        target.with_name(target.name + ".pending").write_bytes(b"stale")
        with self.assertRaises(FileExistsError):
            controller.publish_synced(target, b"{}\n")

    def test_a_job_planned_before_tiers_is_native_and_needs_no_synchronous_proof(self):
        self.assertEqual(controller.cloud_campaign.durability_tier({"backend": "tidehunter_tcp"}), "native")
        self.assertIsNone(controller.cloud_campaign.durability_tier({"backend": "native"}))

    def test_admission_never_infers_an_executed_tier_from_the_plan(self):
        admit = controller.cloud_campaign.admit_durability_tier
        manifest = Path(self.directory.name) / "manifest.json"
        manifest.write_text(json.dumps({"durability": {"confirmed_by_backend_receipt": "synchronous"}}))
        planned = {"backend": "tidehunter_tcp", "durability_tier": "synchronous"}
        admit(planned, {"durability_tier": "synchronous"}, manifest)
        # Missing executed option is native, so a synchronous plan is refused.
        with self.assertRaisesRegex(ValueError, "differs from the planned"):
            admit(planned, {}, manifest)
        # Option present but no owner confirmation (or the wrong one) is refused too.
        for evidence in ({}, {"durability": {}}, {"durability": {"confirmed_by_backend_receipt": "native"}}):
            manifest.write_text(json.dumps(evidence))
            with self.assertRaisesRegex(ValueError, "did not confirm"):
                admit(planned, {"durability_tier": "synchronous"}, manifest)
        # Historical artifact: no tier in the job, none executed, no manifest evidence required.
        admit({"backend": "tidehunter_tcp"}, {}, Path(self.directory.name) / "absent.json")
        with self.assertRaises(ValueError):
            admit({"backend": "tidehunter_tcp"}, {"durability_tier": "synchronous"}, manifest)
        admit({"backend": "native"}, {}, manifest)



if __name__ == "__main__":
    unittest.main()
