import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_layout as layout


def fleet(qualification=False):
    addresses = [f"127.0.0.{20 + i}" if qualification else f"10.1.0.{20 + i}" for i in range(6)]
    return {"schema_version": 1, "run_id": "layout-test", "region": "test-region",
            "data_root": ("/tmp" if qualification else "/mnt") + "/tidehunter-kv-comparison/layout-test",
            "coordinator": {"instance_id": "i-controller", "address": addresses[0]},
            "nodes": [{"index": i, "instance_id": f"i-storage-{i}", "address": addresses[i + 1]} for i in range(5)],
            "server_cpus": 4, "server_memory_mib": 8192, "client_cpus": 4, "client_memory_mib": 8192,
            "replication_factor": 1, "qualification_only": qualification}


class Plans(unittest.TestCase):
    def test_pinned_runnable_bind_mounts_and_equal_storage_caps(self):
        for backend in layout.BACKENDS:
            with self.subTest(backend=backend):
                plan = layout.build_plan(fleet(), backend)
                self.assertEqual(len(plan["nodes"]), 6)
                stores = plan["nodes"][1:]
                self.assertEqual({json.dumps(x["budget"], sort_keys=True) for x in stores},
                                 {json.dumps({"cpus": 4, "memory_mib": 8192}, sort_keys=True)})
                names = []
                for node in plan["nodes"]:
                    for action in ("prepare", "start", "probe", "stop", "export", "reset-data"):
                        argv = node["actions"][action]
                        self.assertEqual(argv[2], action)
                        self.assertEqual(json.loads(argv[argv.index("--fleet-json") + 1]), fleet())
                    self.assertNotIn("--cluster", node["actions"]["bootstrap-probe"])
                    for container in node["containers"]:
                        names.append(container["name"])
                        argv = container["run_argv"]
                        self.assertEqual(argv[:3], ["docker", "run", "--detach"])
                        self.assertIn("@sha256:", container["image"])
                        self.assertIn(container["image"], argv)
                        self.assertIn("type=bind,src=" + node["directory"] + "/data,dst=" + container["data_bind"]["destination"], argv)
                        self.assertIn(layout.local.OWNER_LABEL + "=" + node["owner"], argv)
                        self.assertNotIn("--privileged", argv)
                        self.assertEqual(argv[argv.index("--memory-swap") + 1], "8192m" if node["index"] >= 0 else f"{node['budget']['memory_mib']}m")
                self.assertEqual(len(names), len(set(names)))

    def test_private_listen_advertise(self):
        value = fleet()
        plan = layout.build_plan(value, "tikv")
        pd = plan["nodes"][0]
        self.assertIn("--advertise-client-urls=http://10.1.0.20:2379", pd["containers"][0]["run_argv"])
        self.assertIn("max-replicas = 1", pd["files"]["pd.toml"])
        self.assertIn("merge-schedule-limit = 0", pd["files"]["pd.toml"])
        self.assertIn("enable-placement-rules = true", pd["files"]["pd.toml"])
        self.assertIn('region-score-formula-version = "v2"', pd["files"]["pd.toml"])
        qualification = layout.node_config(fleet(True), "tikv", -1)
        self.assertIn('region-score-formula-version = "v1"', qualification["files"]["pd.toml"])
        physical_store = layout.node_config(value, "tikv", 0)["containers"][0]["run_argv"]
        qualification_store = layout.node_config(fleet(True), "tikv", 0)["containers"][0]["run_argv"]
        self.assertNotIn("--capacity", physical_store)
        self.assertEqual(qualification_store[qualification_store.index("--capacity") + 1], "8GB")
        for node in plan["nodes"][1:]:
            argv = node["containers"][0]["run_argv"]
            self.assertEqual(argv[argv.index("--advertise-addr") + 1], node["address"] + ":20160")
            self.assertIn("max-apply-unpersisted-log-limit = 0", node["files"]["tikv.toml"])
            self.assertNotIn("sync-log =", node["files"]["tikv.toml"])

    def test_fdb_rejects_insufficient_control_and_client_host_memory(self):
        value = fleet()
        value["client_memory_mib"] = 4096
        with self.assertRaises(ValueError):
            layout.build_plan(value, "foundationdb")

    def test_cassandra_batch_commitlog_and_even_initial_tokens(self):
        plan = layout.build_plan(fleet(), "cassandra")
        self.assertEqual(plan["nodes"][0]["containers"], [])
        tokens = []
        for node in plan["nodes"][1:]:
            config = node["files"]["cassandra.yaml"]
            self.assertIn("commitlog_sync: batch", config)
            self.assertNotIn("periodic", config)
            self.assertIn("commitlog_directory: /var/lib/cassandra/commitlog", config)
            self.assertIn("rpc_address: " + node["address"], config)
            tokens.append(int(config.split("initial_token: '")[1].split("'")[0]))
        self.assertEqual(len(set(tokens)), 5)
        self.assertEqual(tokens[0], -(2**63))
        self.assertTrue(all(b - a == 2**64 // 5 for a, b in zip(tokens, tokens[1:])))

    def test_valkey_five_primaries_and_client_only_coordinator(self):
        for qualification in (False, True):
            value = fleet(qualification)
            plan = layout.build_plan(value, "valkey_tcp")
            coordinator = plan["nodes"][0]
            self.assertEqual(coordinator["containers"], [])
            self.assertEqual(coordinator["budget"], {"cpus": 0, "memory_mib": 0})
            self.assertEqual(coordinator["client_budget"], {"cpus": 4, "memory_mib": 8192})
            self.assertEqual(plan["backend_options"], {
                "nodes": [node["address"] + ":6379" for node in value["nodes"]], "cluster": True,
                "required_replication_factor": 1, "required_primary_nodes": 5, "request_timeout_ms": 5000})
            for node in plan["nodes"][1:]:
                self.assertEqual(len(node["containers"]), 1)
                container = node["containers"][0]
                argv = container["run_argv"]
                for key, expected in (("--bind", node["address"]), ("--port", "6379"), ("--cluster-port", "16379"),
                                      ("--appendonly", "yes"), ("--appendfsync", "always"),
                                      ("--no-appendfsync-on-rewrite", "no"), ("--maxmemory-policy", "noeviction"),
                                      ("--cluster-announce-ip", node["address"]), ("--cluster-enabled", "yes")):
                    self.assertEqual(argv[argv.index(key) + 1], expected)
                self.assertNotIn("--save", argv)
                self.assertNotIn("--auto-aof-rewrite-percentage", argv)
                self.assertNotIn("--rdma-port", argv)
                self.assertIn(":9.0.7@sha256:", container["image"])
                self.assertIn("--initialize", coordinator["actions"]["probe"])
                self.assertIn("--cluster", node["actions"]["probe"])

    def test_fdb_machine_ids_and_actual_cluster_file_options(self):
        value = fleet()
        plan = layout.build_plan(value, "foundationdb")
        machines = []
        for node in plan["nodes"]:
            argv = node["containers"][0]["run_argv"]
            machines.append(argv[argv.index("--locality-machineid") + 1])
            self.assertEqual(argv[argv.index("--listen-address") + 1], node["address"] + ":4500")
            self.assertIn("@10.1.0.20:4500", node["files"]["fdb.cluster"])
        self.assertEqual(len(set(machines)), 6)
        self.assertEqual(plan["backend_options"]["cluster_file"], value["data_root"] + "/foundationdb/coordinator/fdb.cluster")
        self.assertEqual(set(layout.backend_options(value, "tikv", "/tmp/client")),
                         {"pd_endpoints", "pd_api_endpoints", "required_replication_factor", "request_timeout_ms"})
        self.assertEqual(layout.backend_options(value, "cassandra", "/tmp/client")["nodes"],
                         [x["address"] + ":9042" for x in value["nodes"]])

    def test_fdb_disk_floor_override_is_qualification_only(self):
        for qualification in (False, True):
            plan = layout.build_plan(fleet(qualification), "foundationdb")
            for node in plan["nodes"]:
                argv = node["containers"][0]["run_argv"]
                knobs = [arg for arg in argv if arg.startswith("--knob-")]
                self.assertEqual(knobs, ["--knob-min_available_space_ratio"] if qualification else [])
                if qualification:
                    self.assertEqual(argv[argv.index(knobs[0]) + 1], "0.01")

    def test_loopback_is_explicitly_nonphysical_and_node_paths_are_distinct(self):
        plan = layout.build_plan(fleet(True), "foundationdb")
        self.assertTrue(plan["qualification_only"])
        self.assertIsNone(plan["physical_storage_hosts"])
        self.assertEqual(len({x["directory"] for x in plan["nodes"]}), 6)
        value = fleet(True)
        value["qualification_only"] = False
        with self.assertRaises(ValueError):
            layout.build_plan(value, "tikv")

    def test_fleet_boundaries_fail_closed(self):
        cases = [("replication_factor", 5), ("server_cpus", True), ("client_cpus", float("nan")),
                 ("server_memory_mib", 1024), ("client_memory_mib", 8192.5),
                 ("data_root", "/mnt/tidehunter-kv-comparison/other"), ("run_id", "../other")]
        for key, value in cases:
            data = fleet()
            data[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                layout.build_plan(data, "tikv")
        for field in ("address", "instance_id"):
            data = fleet()
            data["nodes"][4][field] = data["nodes"][0][field]
            with self.assertRaises(ValueError):
                layout.build_plan(data, "tikv")
        data = fleet()
        data["nodes"][0]["address"] = "8.8.8.8"
        with self.assertRaises(ValueError):
            layout.build_plan(data, "tikv")
        with self.assertRaises(ValueError):
            layout.node_config(fleet(), "tikv", 5)
        for index in (False, 0.0):
            with self.assertRaises(ValueError):
                layout.node_config(fleet(), "tikv", index)
        for malformed in (None, [], {"schema_version": True}, {"schema_version": 1, "run_id": 42}):
            with self.assertRaises(ValueError):
                layout.validate_fleet(malformed)


class LiveAdmission(unittest.TestCase):
    def tikv(self):
        stores = {"stores": [{"store": {"id": i + 1, "address": node["address"] + ":20160", "state_name": "Up", "version": "8.5.4"}}
                             for i, node in enumerate(fleet()["nodes"])]}
        regions = {"regions": [{"id": i + 100, "peers": [{"store_id": i % 5 + 1}], "leader": {"store_id": i % 5 + 1}}
                               for i in range(20)]}
        settings = {"raftstore": {"max-apply-unpersisted-log-limit": 0, "store-io-pool-size": 1},
                    "raft-engine": {"enable": True, "dir": "/data/raft-engine"},
                    "storage": {"engine": "raft-kv", "api-version": 1, "data-dir": "/data"}}
        return stores, regions, {"max-replicas": 1, "enable-placement-rules": "true"}, [copy.deepcopy(settings) for _ in range(5)]

    def test_tikv_requires_real_five_way_region_and_leader_coverage(self):
        args = self.tikv()
        self.assertEqual(layout.admit_tikv(fleet(), *args)["leader_store_ids"], [1, 2, 3, 4, 5])
        for mutate in (lambda x: x[1]["regions"].pop(),
                       lambda x: x[1]["regions"][0].update({"pending_peers": [{"store_id": 2}]}),
                       lambda x: x[2].update({"max-replicas": 5}),
                       lambda x: x[3][0]["raftstore"].update({"max-apply-unpersisted-log-limit": 1024}),
                       lambda x: x[0]["stores"][0]["store"].update({"address": "other:20160"})):
            changed = copy.deepcopy(args)
            mutate(changed)
            with self.assertRaises(RuntimeError):
                layout.admit_tikv(fleet(), *changed)
        changed = copy.deepcopy(args)
        for region in changed[1]["regions"]:
            region["peers"][0]["store_id"] = 1
            region["leader"]["store_id"] = 1
        with self.assertRaises(RuntimeError):
            layout.admit_tikv(fleet(), *changed)

    def test_pinned_pd_string_booleans_and_effective_rule_are_not_assumed(self):
        args = list(self.tikv())
        for disabled in (False, "false", None):
            args[2] = {"max-replicas": 1, "enable-placement-rules": disabled}
            with self.assertRaises(RuntimeError):
                layout.admit_tikv(fleet(), *args)
        rule = {"group_id": "pd", "id": "default", "start_key": "", "end_key": "", "role": "voter",
                "count": 1, "is_witness": False, "location_labels": ["host"]}
        self.assertEqual(layout.admit_tikv_rules([rule]), rule)
        for rules in ([], [rule, rule], [dict(rule, count=3)], [dict(rule, start_key="80")],
                      [dict(rule, role="learner")], [dict(rule, label_constraints=[{"key": "host", "op": "in", "values": ["one"]}])]):
            with self.assertRaises(RuntimeError):
                layout.admit_tikv_rules(rules)

    def test_deprecated_sync_log_or_early_apply_cannot_fake_durable_admission(self):
        with self.assertRaises(RuntimeError):
            layout.admit_tikv_ack({"raftstore": {"sync-log": True}})
        settings = self.tikv()[3][0]
        for update in ({"max-apply-unpersisted-log-limit": 1}, {"max-apply-unpersisted-log-limit": False},
                       {"store-io-pool-size": 0}, {"store-io-pool-size": True}):
            changed = copy.deepcopy(settings)
            changed["raftstore"].update(update)
            with self.assertRaises(RuntimeError):
                layout.admit_tikv_ack(changed)
        for section, update in (("raft-engine", {"enable": False}), ("raft-engine", {"dir": "/outside"}),
                                ("storage", {"api-version": 2}), ("storage", {"engine": "raft-kv2"})):
            changed = copy.deepcopy(settings)
            changed[section].update(update)
            with self.assertRaises(RuntimeError):
                layout.admit_tikv_ack(changed)

    def fdb(self, qualification=False):
        hosts = [fleet(qualification)["coordinator"], *fleet(qualification)["nodes"]]
        return {"cluster": {"database_available": True, "full_replication": True, "configuration": {"redundancy_mode": "single"},
                            "processes": {str(i): {"address": host["address"] + ":4500", "version": "7.3.77",
                                "locality": {"machineid": host["instance_id"]},
                                "roles": [{"role": role} for role in (["log", "commit_proxy", "grv_proxy", "resolver", "master", "cluster_controller"] if i == 0 else ["storage"])]}
                                for i, host in enumerate(hosts)}}}

    def test_fdb_running_process_count_is_not_storage_role_coverage(self):
        status = self.fdb()
        self.assertEqual(len(layout.admit_fdb(fleet(), status)["storage_machine_ids"]), 5)
        mutations = [lambda x: x["cluster"]["processes"]["5"].update({"roles": []}),
                     lambda x: x["cluster"]["configuration"].update({"redundancy_mode": "double"}),
                     lambda x: x["cluster"].update({"full_replication": False}),
                     lambda x: x["cluster"]["processes"]["1"]["locality"].update({"machineid": "i-controller"}),
                     lambda x: x["cluster"]["processes"]["0"].update({"roles": [{"role": "log"}]})]
        for mutate in mutations:
            changed = copy.deepcopy(status)
            mutate(changed)
            with self.assertRaises(RuntimeError):
                layout.admit_fdb(fleet(), changed)

    def test_fdb_file_creation_errors_cannot_pass_topology_admission(self):
        for error in ("trace_log_could_not_create_file", "file_open_error"):
            status = self.fdb()
            status["cluster"]["processes"]["2"]["messages"] = [{"name": error}]
            with self.subTest(error=error), self.assertRaisesRegex(RuntimeError, "file/trace creation"):
                layout.admit_fdb(fleet(), status)

    def test_fdb_proof_records_actual_argv_and_filesystem_without_lowering_physical_floor(self):
        for qualification in (False, True):
            value = fleet(qualification)
            config = layout.node_config(value, "foundationdb", 0)
            argv = config["containers"][0]["run_argv"]
            expected = argv[argv.index(config["containers"][0]["image"]) + 1:]
            filesystem = type("Filesystem", (), {"f_blocks": 1000, "f_bavail": 37, "f_frsize": 4096})()
            with patch.object(layout, "require_state", return_value=Path(config["directory"])), \
                 patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout, "runtime_evidence", return_value=[]), \
                 patch.object(layout, "execute", return_value=json.dumps(self.fdb(qualification))), \
                 patch.object(layout, "inspect_container", return_value={"Config": {"Cmd": expected}}), \
                 patch.object(layout.os, "statvfs", return_value=filesystem), patch.object(layout, "save"):
                proof = layout.probe_once(value, config)
            disk = proof["disk_admission"]
            self.assertEqual(disk["observed_server_argv"], expected)
            self.assertEqual(disk["min_available_space_ratio"], 0.01 if qualification else 0.05)
            self.assertEqual(disk["absolute_min_available_space_bytes"], 100000000)
            self.assertEqual(disk["default_safety_buffer_ratio"], 0.01)
            self.assertEqual(disk["local_filesystem_available_bytes"], 37 * 4096)
            self.assertAlmostEqual(disk["local_filesystem_available_ratio"], 0.037)

    def test_fdb_unplanned_runtime_knob_is_rejected(self):
        config = layout.node_config(fleet(), "foundationdb", 0)
        argv = config["containers"][0]["run_argv"]
        expected = argv[argv.index(config["containers"][0]["image"]) + 1:]
        with patch.object(layout, "require_state", return_value=Path(config["directory"])), \
             patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout, "runtime_evidence", return_value=[]), \
             patch.object(layout, "execute", return_value=json.dumps(self.fdb())), \
             patch.object(layout, "inspect_container", return_value={"Config": {"Cmd": expected + ["--knob-min_available_space_ratio", "0.01"]}}):
            with self.assertRaisesRegex(RuntimeError, "actual server argv"):
                layout.probe_once(fleet(), config)

    def test_cassandra_requires_running_batch_group_not_just_file(self):
        value, config = fleet(), layout.node_config(fleet(), "cassandra", 0)
        local_row = json.dumps({"release_version": "5.0.9", "data_center": "dc1", "rack": "i-storage-0",
                                "host_id": "actual-host-0", "tokens": [str(-(2**63))]})
        with patch.object(layout, "require_state", return_value=Path(config["directory"])), \
             patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout, "runtime_evidence", return_value=[]), \
             patch.object(layout, "execute", side_effect=["UN 10.1.0.21 status", "commitlog_sync: batch\n"]), \
             patch.object(layout, "cql", side_effect=[local_row, json.dumps({"name": "commitlog_sync", "value": "periodic"})]):
            with self.assertRaisesRegex(RuntimeError, "running commitlog"):
                layout.probe_once(value, config)

    def test_cassandra_effective_paths_and_ack_are_retained(self):
        config = layout.node_config(fleet(), "cassandra", 0)
        local_row = json.dumps({"release_version": "5.0.9", "data_center": "dc1", "rack": "i-storage-0",
                                "host_id": "actual-host-0", "tokens": [str(-(2**63))]})
        settings = {"commitlog_sync": "batch", "commitlog_directory": "/var/lib/cassandra/commitlog",
                    "hints_directory": "/var/lib/cassandra/hints", "saved_caches_directory": "/var/lib/cassandra/saved_caches",
                    "cdc_raw_directory": "/var/lib/cassandra/cdc_raw", "data_file_directories": "[/var/lib/cassandra/data]"}
        effective = "\n".join(json.dumps({"name": key, "value": value}) for key, value in settings.items())
        with patch.object(layout, "require_state", return_value=Path(config["directory"])), \
             patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout, "runtime_evidence", return_value=[]), \
             patch.object(layout, "execute", side_effect=["UN 10.1.0.21 status", "commitlog_sync: batch\n"]), \
             patch.object(layout, "cql", side_effect=[local_row, effective]):
            proof = layout.probe_once(fleet(), config)
        self.assertEqual(proof["commitlog_sync"], "batch")
        self.assertEqual(len(proof["effective_settings"]), 6)

    def test_existing_split_boundaries_accept_zero_progress_without_resubmission(self):
        config = layout.node_config(fleet(), "tikv", -1)
        stores, regions, _, _ = self.tikv()
        keys = [""] + [f"{i * 256 // 20:02X}" for i in range(1, 20)]
        for region, key in zip(regions["regions"], keys):
            region["start_key"] = key
        with tempfile.TemporaryDirectory() as directory:
            replies = [stores, regions, {"processed-percentage": 0}, regions]
            with patch.object(layout, "require_state", return_value=Path(directory)), \
                 patch.object(layout, "http", side_effect=replies) as http:
                layout.initialize(fleet(), config)
            self.assertFalse(any(call.args[1] == "/regions/split" for call in http.call_args_list))
            self.assertTrue((Path(directory) / "initialization.json").is_file())

    def test_partial_split_or_unbalanced_scatter_cannot_publish_initialization(self):
        config = layout.node_config(fleet(), "tikv", -1)
        stores, complete, _, _ = self.tikv()
        keys = [""] + [f"{i * 256 // 20:02X}" for i in range(1, 20)]
        for region, key in zip(complete["regions"], keys):
            region["start_key"] = key
        incomplete = copy.deepcopy(complete)
        incomplete["regions"].pop()
        unbalanced = copy.deepcopy(complete)
        for region in unbalanced["regions"]:
            region["leader"]["store_id"] = 1
        for replies in ([stores, incomplete, {"processed-percentage": 100}, incomplete],
                        [stores, complete, {"processed-percentage": 100}, unbalanced]):
            with tempfile.TemporaryDirectory() as directory:
                with patch.object(layout, "require_state", return_value=Path(directory)), \
                     patch.object(layout, "http", side_effect=replies):
                    with self.assertRaises(RuntimeError):
                        layout.initialize(fleet(), config)
                self.assertFalse((Path(directory) / "initialization.json").exists())


class OwnershipAndDevices(unittest.TestCase):
    def test_fdb_prepare_creates_trace_directory_and_start_recreates_it_after_reset(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "node"
            config = layout.node_config(fleet(True), "foundationdb", 0)
            config["directory"] = str(root)
            with patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout.socket, "socket"):
                layout.prepare(config)
            self.assertTrue((root / "data" / "logs").is_dir())
            (root / "data" / "logs").rmdir()
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "nvme_evidence", return_value={}), \
                 patch.object(layout, "inspect_container", return_value=None), patch.object(layout, "ensure_image"), \
                 patch.object(layout, "command"), patch.object(layout, "runtime_evidence", return_value=[]):
                layout.start(config)
            self.assertTrue((root / "data" / "logs").is_dir())

    def test_fdb_start_refuses_symlinked_trace_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "data").mkdir()
            (root / "foreign").mkdir()
            (root / "data" / "logs").symlink_to(root / "foreign", target_is_directory=True)
            config = layout.node_config(fleet(True), "foundationdb", 0)
            with patch.object(layout, "require_state", return_value=root), patch.object(layout, "nvme_evidence", return_value={}), \
                 patch.object(layout, "command") as command:
                with self.assertRaisesRegex(ValueError, "symlink"):
                    layout.start(config)
                command.assert_not_called()

    def test_symlink_and_noncanonical_paths_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "real").mkdir()
            (directory / "link").symlink_to(directory / "real", target_is_directory=True)
            with self.assertRaises(ValueError):
                layout.guarded(directory / "link" / "data")
            with self.assertRaises(ValueError):
                layout.guarded(directory / "real" / ".." / "foreign")

    def test_forged_saved_state_cannot_escape_run_root(self):
        config = layout.node_config(fleet(), "tikv", 0)
        with tempfile.TemporaryDirectory() as temporary:
            config["directory"] = temporary
            (Path(temporary) / "owner.json").write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "escapes admitted run root"):
                layout.require_state(config)

    def test_foreign_label_cannot_be_stopped(self):
        config = layout.node_config(fleet(), "tikv", 0)
        container = config["containers"][0]
        foreign = {"Name": "/" + container["name"], "Id": "foreign", "Config": {"Labels": {}}, "State": {"Running": True}}
        with patch.object(layout, "require_state"), patch.object(layout, "command", side_effect=["foreign\n", json.dumps([foreign])]) as command:
            with self.assertRaisesRegex(RuntimeError, "foreign container"):
                layout.stop(config)
            self.assertEqual(command.call_count, 2)
            self.assertTrue(all(call.args[0][1] in ("ps", "inspect") for call in command.call_args_list))

    def test_cleanup_retains_data_and_never_prunes_volumes(self):
        config = layout.node_config(fleet(), "tikv", 0)
        item = {"Id": "owned", "State": {"Running": True}}
        with patch.object(layout, "require_state"), patch.object(layout, "inspect_container", side_effect=[item, item, None]), \
             patch.object(layout, "command", return_value="") as command, patch.object(layout, "save"):
            result = layout.stop(config)
        self.assertTrue(result["data_retained"])
        self.assertEqual([x.args[0] for x in command.call_args_list],
                         [["docker", "stop", "--time", "20", "owned"], ["docker", "rm", "owned"]])

    def test_nvme_model_and_writable_mount_are_admission_not_device_name(self):
        mount = {"filesystems": [{"source": "/dev/nvme1n1", "target": "/mnt", "fstype": "xfs", "options": "rw,noatime"}]}
        blocks = {"blockdevices": [{"name": "/dev/nvme1n1", "model": "Amazon EC2 NVMe Instance Storage", "serial": "local"}]}
        with patch.object(layout, "command", side_effect=[json.dumps(mount), json.dumps(blocks)]):
            self.assertTrue(layout.nvme_evidence("/mnt", False)["independent_nvme_admitted"])
        blocks["blockdevices"][0]["model"] = "Amazon Elastic Block Store"
        with patch.object(layout, "command", side_effect=[json.dumps(mount), json.dumps(blocks)]):
            with self.assertRaisesRegex(RuntimeError, "not identified"):
                layout.nvme_evidence("/mnt", False)
        with patch.object(layout, "command", return_value=json.dumps(mount)):
            self.assertFalse(layout.nvme_evidence("/mnt", True)["independent_nvme_admitted"])

    def test_probe_deadline_and_initialize_ownership_are_bounded(self):
        config = layout.node_config(fleet(), "tikv", 0)
        with self.assertRaisesRegex(ValueError, "coordinator-only"):
            layout.initialize(fleet(), config)
        for timeout in (0, 901):
            with self.assertRaises(ValueError):
                layout.probe(fleet(), config, timeout=timeout)
        with patch.object(layout.time, "monotonic", side_effect=[0, 0, 1, 2]), \
             patch.object(layout.time, "sleep"), patch.object(layout, "probe_once", side_effect=RuntimeError("not ready")) as probe:
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                layout.probe(fleet(), config, timeout=1)
            self.assertEqual(probe.call_count, 1)


class ResetDataAndPinnedCache(unittest.TestCase):
    def config(self, directory):
        config = layout.node_config(fleet(True), "tikv", -1)
        config["directory"] = str(directory)
        (directory / "data").mkdir()
        (directory / "initialization.json").write_text('{"old":true}')
        return config

    def test_stopped_owned_container_still_prevents_reset(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", return_value="stopped-owned-id") as command:
                with self.assertRaisesRegex(RuntimeError, "including stopped"):
                    layout.reset_data(config)
                self.assertEqual(command.call_count, 1)
                self.assertTrue((root / "initialization.json").exists())

    def test_foreign_container_parent_bind_prevents_reset(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            foreign = {"Id": "foreign", "Mounts": [{"Source": str(root), "Destination": "/borrowed"}]}
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", side_effect=["", "foreign", json.dumps([foreign])]) as command:
                with self.assertRaisesRegex(RuntimeError, "another container"):
                    layout.reset_data(config)
                self.assertEqual(command.call_count, 3)
                self.assertTrue((root / "initialization.json").exists())

    def test_foreign_container_symlink_alias_prevents_reset(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            (root / "alias").symlink_to(root / "data", target_is_directory=True)
            foreign = {"Id": "foreign", "Mounts": [{"Source": str(root / "alias"), "Destination": "/borrowed"}]}
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", side_effect=["", "foreign", json.dumps([foreign])]):
                with self.assertRaisesRegex(RuntimeError, "another container"):
                    layout.reset_data(config)

    def test_nested_mount_is_not_traversed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            mountinfo = f"1 0 0:1 / {root}/data/foreign rw - ext4 /dev/other rw\n"
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", return_value=""), patch.object(Path, "read_text", return_value=mountinfo):
                with self.assertRaisesRegex(RuntimeError, "mountpoint"):
                    layout.reset_data(config)

    def test_reset_retains_config_exports_and_symlink_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            (root / "owner.json").write_text("keep-owner")
            (root / "pd.toml").write_text("keep-config")
            (root / "export").mkdir()
            (root / "export/evidence.json").write_text("keep-proof")
            (root / "foreign").write_text("never-delete")
            (root / "data/foreign-link").symlink_to(root / "foreign")
            (root / "data/payload").write_text("old-data")
            def command(argv, **kwargs):
                if argv[:2] == ["docker", "run"]:
                    self.assertEqual(argv[-5:], ["/data", "-mindepth", "1", "-xdev", "-delete"])
                    self.assertIn("type=bind,src=" + str(root / "data") + ",dst=/data", argv)
                    self.assertEqual(kwargs["timeout"], 300)
                    for path in (root / "data").iterdir():
                        path.unlink()
                return ""
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", side_effect=command), patch.object(layout, "ensure_image"), \
                 patch.object(layout, "nvme_evidence", return_value={"qualification_only": True}), \
                 patch.object(layout, "inspect_container", return_value=None):
                result = layout.reset_data(config)
            self.assertTrue(result["data_empty"])
            self.assertEqual(list((root / "data").iterdir()), [])
            self.assertFalse((root / "initialization.json").exists())
            self.assertEqual((root / "owner.json").read_text(), "keep-owner")
            self.assertEqual((root / "pd.toml").read_text(), "keep-config")
            self.assertEqual((root / "export/evidence.json").read_text(), "keep-proof")
            self.assertEqual((root / "foreign").read_text(), "never-delete")

    def test_failed_delete_invalidates_old_initialization_but_never_admits_fresh(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self.config(root)
            (root / "data/payload").write_text("old-data")
            def command(argv, **kwargs):
                if argv[:2] == ["docker", "run"]:
                    raise RuntimeError("delete failed")
                return ""
            with patch.object(layout, "require_state", return_value=root), \
                 patch.object(layout, "command", side_effect=command), patch.object(layout, "ensure_image"), \
                 patch.object(layout, "nvme_evidence", return_value={}), patch.object(layout, "inspect_container", return_value=None):
                with self.assertRaisesRegex(RuntimeError, "delete failed"):
                    layout.reset_data(config)
            self.assertFalse((root / "initialization.json").exists())
            self.assertEqual(json.loads((root / "reset.json").read_text())["status"], "resetting")
            self.assertTrue((root / "data/payload").exists())

    def test_cached_exact_digest_never_requires_registry(self):
        observed = [{"Id": "sha256:cached"}]
        with patch.object(layout, "command", return_value=json.dumps(observed)) as command:
            self.assertEqual(layout.ensure_image(layout.local.IMAGES["pd"]), observed[0])
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][1:3], ["image", "inspect"])
        with self.assertRaises(ValueError):
            layout.ensure_image("pingcap/pd:latest")

    def test_absent_exact_digest_is_pulled_once_and_resolved(self):
        observed = [{"Id": "sha256:pulled"}]
        with patch.object(layout, "command", side_effect=[RuntimeError("missing"), "", json.dumps(observed)]) as command:
            self.assertEqual(layout.ensure_image(layout.local.IMAGES["pd"]), observed[0])
            self.assertEqual([call.args[0][1] for call in command.call_args_list], ["image", "pull", "image"])


if __name__ == "__main__":
    unittest.main()
