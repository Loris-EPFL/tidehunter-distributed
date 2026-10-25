#!/usr/bin/env python3
"""Controller ownership/unknown-outcome tests; all clients are local test doubles."""
import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

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


def physical_fixture():
    return [{"qualification_only": False, "independent_nvme_admitted": True,
             "host_identity": {"boot_id": f"boot-{i}"},
             "block_devices": {"blockdevices": [{"model": "Amazon EC2 NVMe Instance Storage", "serial": f"serial-{i}"}]}}
            for i in range(6)]


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
        job = {"backend": "tidehunter_tcp", "coordinator_cache_bytes": 128 * 1024**2, "owner_read_cache_bytes": 0}
        evidence = {"schema_version": 1, "backend": "tidehunter_tcp",
            "coordinator": {"read_rpc_count": 42, "cache": {"budget_bytes": 128 * 1024**2, "hits": 0}},
            "owner_read_configuration": [{"partition": i, "read_cache_bytes": 0,
                "resident_reads": dict(controller.DISABLED_RESIDENT_READS)} for i in range(5)]}
        controller.verify_counter_evidence(job, evidence)
        for mutation in (lambda x: x["coordinator"]["cache"].update(budget_bytes=0),
                         lambda x: x.update(coordinator=None),
                         lambda x: x["owner_read_configuration"].pop()):
            broken = copy.deepcopy(evidence)
            mutation(broken)
            with self.assertRaises(controller.SafetyError):
                controller.verify_counter_evidence(job, broken)


    def test_registered_rma_policy_and_device_requirement_must_match_actual_owners(self):
        resident = {**controller.DISABLED_RESIDENT_READS, "source_bytes": 2 * 1024**2,
            "source_regions": 8, "destination_bytes": 2 * 1024**2, "destination_slots": 8,
            "max_read_bytes": 65536, "max_handles": 8, "require_device_read": True}
        job = {"backend": "tidehunter_efa_native", "coordinator_cache_bytes": 0,
               "owner_read_cache_bytes": 0, "resident_reads": resident}
        evidence = {"schema_version": 1, "backend": job["backend"],
            "coordinator": {"read_rpc_count": 1, "cache": {"budget_bytes": 0}},
            "owner_read_configuration": [{"partition": i, "read_cache_bytes": 0,
                "resident_reads": dict(resident)} for i in range(5)]}
        controller.verify_counter_evidence(job, evidence)
        for value in (False, 1):
            broken = copy.deepcopy(evidence)
            broken["owner_read_configuration"][0]["resident_reads"]["require_device_read"] = value
            with self.subTest(value=value), self.assertRaises(controller.SafetyError):
                controller.verify_counter_evidence(job, broken)
        broken = copy.deepcopy(evidence)
        broken["owner_read_configuration"][2]["resident_reads"]["source_bytes"] = 0
        with self.assertRaises(controller.SafetyError):
            controller.verify_counter_evidence(job, broken)


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
    def test_unknown_send_outcome_is_never_retried(self):
        config = config_fixture()
        clients = clients_fixture(config)
        clients["ssm"].send_command.side_effect = TimeoutError("response lost after acceptance")
        runtime = controller.Controller(config, clients)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with self.assertRaisesRegex(controller.InfrastructureError, "never blindly retry"):
                runtime.request(0, {"action": "snapshot"})
            self.assertEqual(clients["ssm"].send_command.call_count, 1)
            clients["ssm"].get_command_invocation.assert_not_called()
            intent = json.loads(next((Path(tmp) / "commands").glob("*.json")).read_text())
            self.assertEqual(intent["status"], "unknown_send_response")

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
                with self.assertRaises(controller.InfrastructureError):
                    runtime.run_case(job)
            self.assertTrue((database / "live-wal").exists())
            receipt = json.loads((database.parent / "receipt.json").read_text())
            self.assertTrue(receipt["database_files_retained"])
            self.assertEqual(receipt["status"], "failed")

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
                         "cases/case/prepared/tls/ca.key", "cases/case/prepared/tls/node.pk8",
                         "cases/case/database/wal", "cases/case/storage-0.tar.gz"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"retained-or-denied")
            archive = Path(tmp) / "result.tar.gz"
            records = controller.archive_results(root, archive)
            self.assertEqual(set(records), {"matrix.json", "cases/case/config.json", "cases/case/result/samples.csv"})
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
            "tls": {"transport": "tcp", "ca": str(root / "tls/ca.der"),
                    "cert": str(root / "tls/tidehunter-node-0.der"), "key": str(root / "tls/tidehunter-node-0.pk8")}}
        controller.validate_launch(config, launch, case, 0)
        for mutation in (lambda x: x["tls"].update(key=str(root / "tls/ca.key")),
                         lambda x: x["node"].update(root="/mnt/foreign/storage")):
            broken = copy.deepcopy(launch)
            mutation(broken)
            with self.assertRaises(controller.SafetyError):
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


if __name__ == "__main__":
    unittest.main()
