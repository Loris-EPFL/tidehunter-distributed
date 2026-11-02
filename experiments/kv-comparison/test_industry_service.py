import copy
import unittest
from unittest import mock

import cloud_layout
import industry_service as service
import local


def valid_nodes(backend):
    endpoints = service.local_endpoints()
    ids = [f"{i+1:040x}" for i in range(5)]
    slots = [[i * 16384 // 5, (i + 1) * 16384 // 5 - 1,
              [endpoint.split(':')[0], 6379, ids[i]]] for i, endpoint in enumerate(endpoints)]
    nodes = []
    for i, endpoint in enumerate(endpoints):
        config = ({"appendonly": "yes", "aof-commit-freq": "0", "aof-commit-wait": "yes", "logdir": "/data", "cluster-enabled": "yes"}
                  if backend == "garnet" else {"dir": "/data", "cluster-enabled": "yes", "persist-cluster-nodes-enabled": "yes",
                  "rocksdb.write_options.sync": "yes", "rocksdb.write_options.disable_wal": "no", "rocksdb.disable_auto_compactions": "no",
                  "rocksdb.write_buffer_size": "32", "rocksdb.max_write_buffer_number": "2", "rocksdb.max_background_jobs": "2",
                  "rocksdb.block_cache_size": "384", "rocksdb.subkey_block_cache_size": "128", "rocksdb.metadata_block_cache_size": "128"})
        lines = [f"{ids[j]} {ep}@16379 {'myself,master' if i == j else 'master'} - 0 0 {j+1} connected {slots[j][0]}-{slots[j][1]}"
                 for j, ep in enumerate(endpoints)]
        nodes.append({"endpoint": endpoint, "effective_config": config,
                      "info_server": {"garnet_version" if backend == "garnet" else "kvrocks_version": service.SOURCES[backend]["version"]},
                      "info_replication": {"role": "master", "connected_slaves": "0"},
                      "cluster_info": {"cluster_state": "ok", "cluster_slots_assigned": "16384", "cluster_slots_ok": "16384",
                                       "cluster_slots_pfail": "0", "cluster_slots_fail": "0", "cluster_known_nodes": "5"},
                      "cluster_nodes": "\n".join(lines), "cluster_slots": copy.deepcopy(slots)})
    return nodes, endpoints


class IndustryAdmissionTests(unittest.TestCase):
    def test_complete_and_weakened_durability(self):
        for backend, key, value in (("garnet", "aof-commit-wait", "no"), ("kvrocks", "rocksdb.write_options.sync", "no"),
                                    ("kvrocks", "rocksdb.write_options.disable_wal", "yes")):
            nodes, endpoints = valid_nodes(backend)
            self.assertEqual(service.admit(backend, nodes, endpoints)["slot_count"], 16384)
            nodes[2]["effective_config"][key] = value
            with self.assertRaises(RuntimeError):
                service.admit(backend, nodes, endpoints)

    def test_unhealthy_inconsistent_and_duplicate_slot_views(self):
        for change in ("unhealthy", "overlap", "disagree", "missing", "replica", "foreign"):
            nodes, endpoints = valid_nodes("garnet")
            if change == "unhealthy":
                nodes[0]["cluster_info"]["cluster_slots_fail"] = "1"
            elif change == "overlap":
                nodes[0]["cluster_slots"][1][0] -= 1
            elif change == "disagree":
                nodes[0]["cluster_slots"][0][2][2] = "f" * 40
            elif change == "missing":
                nodes[0]["cluster_slots"].pop()
            elif change == "replica":
                nodes[0]["cluster_slots"][0].append(["127.0.0.9", 6379, "a" * 40])
            else:
                nodes[0]["cluster_nodes"] = nodes[0]["cluster_nodes"].replace("connected", "disconnected", 1)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                service.admit("garnet", nodes, endpoints)

    def test_escaped_data_bind_and_weakened_argv(self):
        config = {"backend": "garnet", "index": 0, "address": "127.0.0.31", "directory": "/tmp/owned",
                  "budget": {"memory_mib": 2048, "cpus": 2}, "containers": [{"name": "owned"}], "labels": {"owner": "abc"}}
        image = local.IMAGES["garnet"]
        item = {"image": image, "cmd": service.garnet_args(config["address"], 6379, 2048),
                "entrypoint": ["/app/GarnetServer"], "state": {"Running": True},
                "name": "/owned", "labels": {"owner": "abc"},
                "cgroup_limits": {"Memory": 2048 * 1024**2, "MemorySwap": 2048 * 1024**2, "NanoCpus": 2_000_000_000},
                "repo_digests": [image.split(":2.2.5")[0] + "@" + image.split("@")[1]],
                "mounts": [{"Type": "bind", "Source": "/tmp/owned/data", "Destination": "/data", "RW": True}]}
        service.admit_runtime("garnet", [item], config)
        for field in ("mount", "argv", "image"):
            bad = copy.deepcopy(item)
            if field == "mount": bad["mounts"][0]["Source"] = "/tmp/foreign"
            if field == "argv": bad["cmd"].remove("--storage-tier")
            if field == "image": bad["repo_digests"] = []
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                service.admit_runtime("garnet", [bad], config)

    def test_local_five_primaries_and_exact_cleanup(self):
        for backend in ("garnet", "kvrocks"):
            spec, count = local.topology(backend, "qualification", __import__("pathlib").Path("/tmp/owned"), "abc")
            self.assertEqual(count, 5)
            self.assertEqual(len(spec["services"]), 5)
        state = {"backend": "garnet", "project": "kv-owned", "owner": "owner", "directory": "/tmp/owned"}
        foreign = [{"Labels": {local.OWNER_LABEL: "someone-else"}}]
        with mock.patch.object(local, "inspect_owned"), mock.patch.object(local, "command", side_effect=["volume", __import__("json").dumps(foreign)]), mock.patch.object(local, "compose") as compose:
            with self.assertRaises(RuntimeError):
                local.stop(state)
            compose.assert_not_called()


if __name__ == "__main__":
    unittest.main()
