"""Owner-host (launch `host`) receipts, shutdown proofs, history evidence and launch admission.

Archive single-database launches (`node`) keep their schema; every check here
also proves that the two shapes never satisfy each other's contract.
"""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_campaign
import cloud_controller as controller
import nvme_campaign_owner as owner

POOL_PREP = {"addresses": ["10.1.0.1", "10.1.0.2"], "service": 4420, "port_id": 1000}


def host_launch():
    return {"ready_file": "/run/ready", "launch_nonce": "nonce", "listen": "10.0.0.1:9400",
            "durability": {"tier": "native"}, "partition": 1, "host": {"root": "/data/storage"}}


def host_receipt(**changes):
    return {"partition": 1, "address": "10.0.0.1:9400", "launch_nonce": "nonce", "pid": 7,
            "boot_id": "boot", "started_unix_ns": 5, "durability": {"tier": "native"}} | changes


class OwnerHostReceiptTests(unittest.TestCase):
    def test_launch_helpers_dispatch_on_schema(self):
        launch = host_launch()
        archive = {"node": {"root": "/archive", "partition": {"partition": 2}}}
        self.assertEqual(cloud_campaign.owner_launch_partition(launch), 1)
        self.assertEqual(cloud_campaign.owner_launch_partition(archive), 2)
        self.assertIs(cloud_campaign.owner_launch_config(launch), launch["host"])
        self.assertIs(cloud_campaign.owner_launch_config(archive), archive["node"])
        self.assertTrue(cloud_campaign.owner_launch_live(launch))
        self.assertFalse(cloud_campaign.owner_launch_live(archive))
        archive["node"]["live_target"] = {}
        self.assertTrue(cloud_campaign.owner_launch_live(archive))

    def test_owner_host_receipt_names_no_database(self):
        launch = host_launch()
        cloud_campaign.validate_owner_receipt(host_receipt(), launch)
        for bad in (host_receipt(database=[1] * 16), host_receipt(partition=0),
                    host_receipt(launch_nonce="stale"), host_receipt(address="10.0.0.2:9400")):
            with self.subTest(receipt=bad), self.assertRaises(ValueError):
                cloud_campaign.validate_owner_receipt(bad, launch)
        # An archive launch still requires and compares its configured database.
        archive = {key: launch[key] for key in ("ready_file", "launch_nonce", "listen", "durability")}
        archive["node"] = {"partition": {"partition": 1, "inventory": {"database": [1] * 16}}}
        cloud_campaign.validate_owner_receipt(host_receipt(database=[1] * 16), archive)
        for bad in (host_receipt(), host_receipt(database=[2] * 16)):
            with self.subTest(archive=bad), self.assertRaises(ValueError):
                cloud_campaign.validate_owner_receipt(bad, archive)

    def test_restart_freshness_never_crosses_schemas(self):
        previous = host_receipt()
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(previous, host_receipt(pid=8, started_unix_ns=6)))
        self.assertFalse(cloud_campaign.owner_receipt_is_fresh(previous, host_receipt()))
        self.assertFalse(cloud_campaign.owner_receipt_is_fresh(
            previous, host_receipt(pid=8, started_unix_ns=6, database=[1] * 16)))
        archive = host_receipt(database=[1] * 16)
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(
            archive, dict(archive, pid=8, started_unix_ns=6)))
        self.assertFalse(cloud_campaign.owner_receipt_is_fresh(archive, host_receipt(pid=8, started_unix_ns=6)))


class OwnerHostShutdownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.node = Path(self.tmp.name)
        self.launch_path = self.node / "launch.yaml"
        self.launch_path.write_text("host: {}\n")
        self.launch = {"partition": 0, "host": {"root": str(self.node / "storage")}}
        self.identity = {"process_id": 11, "process_start_ticks": 22,
                         "launch_blake2b512": hashlib.blake2b(self.launch_path.read_bytes()).hexdigest()}
        (self.node / "live-owner-identity.json").write_text(json.dumps(self.identity))
        self.stopped = {"MainPID": "0", "SubState": "exited", "Result": "success", "ExecMainStatus": "0"}
        clean = {"fixture_only": False, "errors": 0, "owned_namespace_count": 0}
        self.proof = {"schema_version": 2, "scope": "kernel_live_target_shutdown", "complete": True,
                      "owner": {"partition": 0, "root": self.launch["host"]["root"]}, **self.identity,
                      "targets": [{"database": [1] * 16, "target": dict(clean)},
                                  {"database": [2] * 16, "target": dict(clean)}],
                      "serve_error": None, "cleanup_error": None}
        for p in (patch.object(owner, "_trusted_dir"),
                  patch.object(owner, "_read_bytes", side_effect=lambda path: Path(path).read_bytes())):
            p.start()
            self.addCleanup(p.stop)

    def admit(self, proof, **kwargs):
        (self.node / "launch.live-target-shutdown.json").write_text(json.dumps(proof))
        return owner.admit_live_shutdown(self.node, self.launch_path, self.launch, 0, self.stopped, **kwargs)

    def test_qualification_shutdown_requires_exact_database(self):
        database = bytes(self.proof["targets"][0]["database"]).hex()
        with self.assertRaises(ValueError):
            self.admit(self.proof, expected_database=database)
        proof = dict(self.proof, targets=self.proof["targets"][:1])
        self.assertEqual(self.admit(proof, expected_database=database), proof)
        with self.assertRaises(ValueError):
            self.admit(proof, expected_database="ff" * 16)

    def test_one_clean_target_per_routed_database(self):
        self.assertEqual(self.admit(self.proof), self.proof)
        mutations = [
            lambda p: p.update(schema_version=1),
            lambda p: p.update(targets=[]),
            lambda p: p.pop("targets"),
            lambda p: p["targets"][1].update(database=[1] * 16),
            lambda p: p["targets"][1].update(database=[0] * 16),
            lambda p: p["targets"][1].update(database=[1] * 15),
            lambda p: p["targets"][1]["target"].update(owned_namespace_count=1),
            lambda p: p["targets"][1]["target"].update(fixture_only=True),
            lambda p: p["targets"][1].update(extra=True),
            lambda p: p["owner"].update(database=[1] * 16),
            lambda p: p["owner"].update(partition=1),
            lambda p: p.update(scope="spdk_live_target_shutdown"),
            lambda p: p.update(target=p["targets"][0]["target"]),
            lambda p: p.update(cleanup_error="teardown failed"),
        ]
        for mutate in mutations:
            proof = copy.deepcopy(self.proof)
            mutate(proof)
            if "target" in proof:
                proof.pop("targets")
            with self.subTest(proof=proof), self.assertRaises(ValueError):
                self.admit(proof)


class OwnerHostLaunchTests(unittest.TestCase):
    def test_target_pool_layout_and_archive_inputs(self):
        with patch.object(controller, "live_nvme_preparation", return_value=POOL_PREP):
            self.assertEqual(controller.live_target_pool_for_owner({}, "case", 1, "rdma"), {
                "address": "10.1.0.2", "transport": "rdma", "public_port_base": 1002,
                "public_service_base": 4422, "commissioning_port_base": 1003,
                "commissioning_service_base": 4423})
        self.assertEqual(controller.archive_live_preparation(POOL_PREP),
                         dict(POOL_PREP, commissioning_service=4421, commissioning_port_id=1001))
        self.assertNotIn("commissioning_service", POOL_PREP)

    def test_effective_initiator_pattern(self):
        good = "nqn.2026-01.local.tidehunter:db-" + "ab" * 16 + "-p00000001-host"
        self.assertTrue(cloud_campaign.LIVE_HOST_SCOPE.fullmatch(good))
        for bad in (good.replace("-host", "-sub-x"), good.replace("ab", "AB", 1),
                    "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-4111-8111-111111111111"):
            self.assertIsNone(cloud_campaign.LIVE_HOST_SCOPE.fullmatch(bad))

    def test_validate_launch_admits_only_the_exact_host_slot(self):
        config = {"spec": {}, "fleet": {"data_root": "/data", "nodes": [{}, {}]}}
        node = Path("/data/cells/case/node-1")
        transport = cloud_campaign.CONTROL_TRANSPORTS[cloud_campaign.OFI_CONTROL]
        with patch.object(controller, "live_nvme_preparation", return_value=POOL_PREP):
            pool = controller.live_target_pool_for_owner(config, "case", 1, "rdma")
        launch = {"ready_file": str(node / "ready"), "stop_file": str(node / "stop"), "partition": 1,
                  "listen": "10.0.0.2:9400", "tls": {"transport": transport, "ca": "", "cert": "", "key": ""},
                  "host": {"root": str(node / "storage"), "wal_prewrite": False, "max_databases": 1,
                           "live_target_pool": pool}}
        with patch.object(controller, "guarded", side_effect=lambda path, *_a, **_k: Path(path)), \
             patch.object(controller, "logical_owner_hosts", return_value=[{}, {"address": "10.0.0.2"}]), \
             patch.object(controller, "live_nvme_preparation", return_value=POOL_PREP):
            controller.validate_launch(config, launch, "case", 1)
            mutations = [
                lambda l: l.update(partition=0),
                lambda l: l["host"].update(max_databases=2),
                lambda l: l["host"].update(partition={"partition": 1}),
                lambda l: l["host"].update(live_target={}),
                lambda l: l["host"].pop("live_target_pool"),
                lambda l: l["host"]["live_target_pool"].update(public_service_base=4420),
                lambda l: l["host"]["live_target_pool"].update(address="10.1.0.1"),
            ]
            for mutate in mutations:
                changed = copy.deepcopy(launch)
                mutate(changed)
                with self.subTest(launch=changed), self.assertRaisesRegex(
                        controller.SafetyError, "owner host launch differs"):
                    controller.validate_launch(config, changed, "case", 1)
            changed = copy.deepcopy(launch)
            changed["host"]["root"] = str(node / "elsewhere")
            with self.assertRaisesRegex(controller.SafetyError, "database root escapes"):
                controller.validate_launch(config, changed, "case", 1)

    def test_prewrite_binds_into_either_launch_schema(self):
        for shape in ("host", "node"):
            launch = {shape: {}}
            controller.bind_owner_wal_prewrite({"owner_wal_prewrite": True}, launch)
            self.assertIs(launch[shape]["wal_prewrite"], True)


if __name__ == "__main__":
    unittest.main()
