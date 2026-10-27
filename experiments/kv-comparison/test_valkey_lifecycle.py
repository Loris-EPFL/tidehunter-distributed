import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import cloud_layout
import local
from test_cloud_layout import fleet


def evidence(cluster=True):
    endpoints = [f"127.0.0.{21+i}:{6379+i}" for i in range(5 if cluster else 1)]
    identities = [f"{i+1:040x}" for i in range(len(endpoints))]
    nodes = []
    for index, endpoint in enumerate(endpoints):
        node = {"endpoint": endpoint,
                "effective_config": {"appendonly": "yes", "appendfsync": "always",
                    "no-appendfsync-on-rewrite": "no", "maxmemory-policy": "noeviction", "dir": "/data",
                    "appenddirname": "appendonlydir", "appendfilename": "appendonly.aof",
                    "cluster-enabled": "yes" if cluster else "no", "save": "3600 1 300 100 60 10000",
                    "auto-aof-rewrite-percentage": "100"},
                "info_server": {"valkey_version": "9.0.7", "redis_mode": "cluster" if cluster else "standalone"},
                "info_persistence": {"loading": "0", "aof_enabled": "1", "aof_last_write_status": "ok",
                                     "aof_last_bgrewrite_status": "ok", "aof_rewrite_in_progress": "0"},
                "info_replication": {"role": "master", "connected_slaves": "0"},
                "info_cluster": {"cluster_enabled": "1" if cluster else "0"}}
        if cluster:
            node["cluster_info"] = {"cluster_state": "ok", "cluster_slots_assigned": "16384",
                                    "cluster_slots_ok": "16384", "cluster_slots_pfail": "0",
                                    "cluster_slots_fail": "0", "cluster_known_nodes": "5", "cluster_size": "5"}
            node["cluster_nodes"] = "\n".join(
                f"{identity} {address}@{16379+i} {'myself,master' if i == index else 'master'} - 0 0 {i+1} connected "
                f"{i*16384//5}-{(i+1)*16384//5-1}"
                for i, (identity, address) in enumerate(zip(identities, endpoints)))
            node["cluster_links"] = [{"direction": direction, "node": identity, "events": "r"}
                                     for identity in identities if identity != identities[index]
                                     for direction in ("to", "from")]
        nodes.append(node)
    return nodes, endpoints


class ValkeyAdmission(unittest.TestCase):
    def test_standalone_and_real_cluster_are_distinct(self):
        for cluster in (False, True):
            nodes, endpoints = evidence(cluster)
            proof = local.admit_valkey(nodes, endpoints, cluster)
            self.assertEqual(proof["required_primary_nodes"], 5 if cluster else 1)
            self.assertEqual(proof["slot_count"], 16384 if cluster else 0)
            self.assertEqual(proof["replica_count"], 0)
        with self.assertRaises(RuntimeError):
            local.admit_valkey(*evidence(False), True)

    def test_current_server_mode_and_compatibility_mode_fields(self):
        nodes, endpoints = evidence(False)
        nodes[0]["info_server"]["server_mode"] = nodes[0]["info_server"].pop("redis_mode")
        self.assertEqual(local.admit_valkey(nodes, endpoints, False)["required_primary_nodes"], 1)
        nodes[0]["info_server"]["server_mode"] = "sentinel"
        with self.assertRaises(RuntimeError):
            local.admit_valkey(nodes, endpoints, False)

    def test_effective_durability_version_mode_and_replication_fail_closed(self):
        mutations = [("effective_config", "appendonly", "no"),
                     ("effective_config", "appendfsync", "everysec"),
                     ("effective_config", "no-appendfsync-on-rewrite", "yes"),
                     ("effective_config", "maxmemory-policy", "allkeys-lru"),
                     ("effective_config", "dir", "/tmp"),
                     ("effective_config", "appenddirname", "../unowned"),
                     ("effective_config", "cluster-enabled", "no"),
                     ("info_server", "valkey_version", "9.0.6"),
                     ("info_server", "redis_mode", "standalone"),
                     ("info_persistence", "aof_enabled", "0"),
                     ("info_persistence", "aof_last_write_status", "err"),
                     ("info_persistence", "aof_last_bgrewrite_status", "err"),
                     ("info_persistence", "loading", "1"),
                     ("info_replication", "connected_slaves", "1"),
                     ("info_replication", "role", "slave"),
                     ("cluster_info", "cluster_slots_ok", "16383"),
                     ("cluster_info", "cluster_slots_pfail", "1"),
                     ("cluster_info", "cluster_known_nodes", "6")]
        for section, field, value in mutations:
            with self.subTest(section=section, field=field):
                nodes, endpoints = evidence()
                nodes[0][section][field] = value
                with self.assertRaises(RuntimeError):
                    local.admit_valkey(nodes, endpoints, True)

    def test_slot_holes_duplicates_migrations_and_bad_links_are_rejected(self):
        for old, new in (("0-3275", "1-3275"), ("3276-6552", "3275-6552"),
                         ("0-3275", "[0->-" + "2" * 40 + "]"), ("connected", "disconnected"),
                         (" master - ", " slave " + "1" * 40 + " "),
                         ("@16379", "@16380")):
            nodes, endpoints = evidence()
            nodes[0]["cluster_nodes"] = nodes[0]["cluster_nodes"].replace(old, new)
            with self.subTest(old=old), self.assertRaises(RuntimeError):
                local.admit_valkey(nodes, endpoints, True)
        nodes, endpoints = evidence()
        nodes[0]["cluster_links"].pop()
        with self.assertRaises(RuntimeError):
            local.admit_valkey(nodes, endpoints, True)
        nodes, endpoints = evidence()
        nodes[0]["cluster_nodes"] = nodes[0]["cluster_nodes"].replace("0-3275", "0-3276").replace("3276-6552", "3277-6552")
        with self.assertRaises(RuntimeError):
            local.admit_valkey(nodes, endpoints, True)

    def test_node_bootstrap_receipt_cannot_claim_full_slot_coverage(self):
        nodes, endpoints = evidence()
        nodes[0]["cluster_info"]["cluster_state"] = "fail"
        proof = local.admit_valkey(nodes[:1], endpoints[:1], True, complete=False)
        self.assertIsNone(proof["slot_count"])
        self.assertEqual(proof["required_primary_nodes"], 5)
        with self.assertRaises(RuntimeError):
            local.admit_valkey(nodes, endpoints, True)



class Lifecycle(unittest.TestCase):
    def test_supported_cluster_create_is_not_replayed_on_partial_membership(self):
        _, endpoints = evidence()
        for pristine in (False, True):
            calls = []

            def run(endpoint, *args):
                calls.append(args)
                if args[-1] == "PING":
                    return "PONG\n"
                if args[-2:] == ("CLUSTER", "INFO"):
                    return "cluster_known_nodes:" + ("1" if pristine else "3") + "\ncluster_slots_assigned:0\n"
                self.assertIn("--cluster-replicas", args)
                self.assertEqual(args[args.index("--cluster-replicas") + 1], "0")
                self.assertIn("--cluster-yes", args)
                return "[OK] cluster configured"

            with tempfile.TemporaryDirectory() as directory:
                local.initialize_valkey(run, endpoints, directory, 1)
                receipt = json.loads((Path(directory) / "initialization.json").read_text())
                self.assertEqual(len(receipt["initial_cluster_info"]), 5)
            self.assertEqual(sum("--cluster" in args for args in calls), int(pristine))

    def test_client_only_tool_is_bounded_labelled_and_cleans_up_after_timeout(self):
        config = cloud_layout.node_config(fleet(True), "valkey_tcp", -1)
        calls = []

        def command(argv, **kwargs):
            calls.append(argv)
            if argv[:2] == ["docker", "run"]:
                raise subprocess.TimeoutExpired(argv, 30)
            return ""

        with patch.object(cloud_layout, "command", side_effect=command), patch.object(
                cloud_layout, "inspect_container", return_value={"Id": "owned-tool"}):
            with self.assertRaises(subprocess.TimeoutExpired):
                cloud_layout.valkey_command(config, "127.0.0.21:6379", "valkey-cli", "--raw", "PING")
        argv = calls[0]
        self.assertIn("--rm", argv)
        self.assertEqual(argv[argv.index("--memory") + 1], "512m")
        self.assertEqual(argv[argv.index("--cpus") + 1], "1")
        self.assertIn(local.OWNER_LABEL + "=" + config["owner"], argv)
        self.assertEqual(calls[-1], ["docker", "rm", "--force", "owned-tool"])


if __name__ == "__main__":
    unittest.main()
