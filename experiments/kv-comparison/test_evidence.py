import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import analyze


class EvidenceAdmission(unittest.TestCase):
    def binding(self, root):
        result = root / 'result'
        result.mkdir()
        config = {'seed': 17, 'dataset': {'value_size': 1024}}
        normalized = {'seed': 17, 'dataset': {'value_size': 1024, 'entropy': 1.0}}
        (root / 'normalized-config.json').write_text(json.dumps(normalized))
        metadata = {'name': 'native', 'acknowledgement': 'page_cache'}
        manifest = {'run': {'config': normalized, 'binary': {'blake2b512': 'binary-proof'}}, 'backend_info': metadata}
        summary = {'backend': metadata}
        files = {'manifest.json': json.dumps(manifest), 'summary.json': json.dumps(summary),
                 'samples.csv': 'one raw original sample\n', 'metrics.prom': 'observed_counter_total 1\n'}
        for name, text in files.items():
            (result / name).write_text(text)
        receipt = {'normalized_config_sha256': hashlib.sha256((root / 'normalized-config.json').read_bytes()).hexdigest(),
                   'binary_blake2b512': 'binary-proof',
                   'artifacts_sha256': {name: hashlib.sha256((result / name).read_bytes()).hexdigest() for name in files}}
        return config, receipt, manifest, summary

    def test_measurement_read_evidence_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            path = root / "result/measurement-read-evidence.json"
            path.write_text('{"scope":"measurement_boundary"}')
            receipt["measurement_read_evidence"] = {
                "path": "result/measurement-read-evidence.json",
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            analyze.verify_binding(root, config, receipt, manifest, summary)
            path.write_text('{"scope":"adapter_lifetime"}')
            with self.assertRaisesRegex(ValueError, "measurement read evidence"):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_advertised_measurement_evidence_requires_binding_but_old_manifest_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            analyze.verify_binding(root, config, receipt, manifest, summary)
            manifest["run"]["measurement_read_evidence_schema_version"] = 1
            (root / "result/manifest.json").write_text(json.dumps(manifest))
            receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(
                (root / "result/manifest.json").read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "advertised measurement read evidence"):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_read_diagnostics_require_exact_receipt_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            path = root / "result/read-diagnostics.json"
            data = {"schema_version": 1, "phase": "measurement"}
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "read diagnostics.*integrity binding"):
                analyze.verify_binding(root, config, receipt, manifest, summary)
            receipt["read_diagnostics"] = {"path": "result/read-diagnostics.json",
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": data}
            analyze.verify_binding(root, config, receipt, manifest, summary)
            receipt["read_diagnostics"]["observed"] = {"schema_version": 2}
            with self.assertRaisesRegex(ValueError, "read diagnostics.*integrity binding"):
                analyze.verify_binding(root, config, receipt, manifest, summary)
            receipt["read_diagnostics"]["observed"] = data
            path.write_text('{"schema_version": 2}')
            with self.assertRaisesRegex(ValueError, "read diagnostics.*integrity binding"):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_result_swap_rejected_even_when_its_artifact_hash_is_updated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            analyze.verify_binding(root, config, receipt, manifest, summary)
            altered = copy.deepcopy(manifest)
            altered['run']['config']['dataset']['value_size'] = 8192
            (root / 'result/manifest.json').write_text(json.dumps(altered))
            receipt['artifacts_sha256']['manifest.json'] = hashlib.sha256((root / 'result/manifest.json').read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, 'configuration differs'):
                analyze.verify_binding(root, config, receipt, altered, summary)

    def test_raw_metrics_tampering_does_not_enter_a_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            (root / 'result/metrics.prom').write_text('observed_counter_total 0\n')
            with self.assertRaisesRegex(ValueError, 'integrity binding'):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_other_executable_and_changed_explicit_input_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            altered = copy.deepcopy(manifest)
            altered['run']['binary']['blake2b512'] = 'different-binary'
            with self.assertRaisesRegex(ValueError, 'executable differs'):
                analyze.verify_binding(root, config, receipt, altered, summary)
            config['seed'] = 18
            with self.assertRaisesRegex(ValueError, 'explicitly supplied'):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_volatile_process_and_tikv_identity_do_not_prevent_repetitions(self):
        first = {'process_id': 123, 'owners': 1, 'path': '/first'}
        second = {'process_id': 456, 'owners': 1, 'path': '/second'}
        self.assertEqual(analyze.stable_topology('native', first), analyze.stable_topology('native', second))
        base = {'store_metadata': {'stores': [{'store': {'id': 1, 'address': '127.0.0.1:20160',
                'version': '8.5.4', 'state_name': 'Up'}, 'status': {'last_heartbeat_ts': 'first'}}]}}
        changed = copy.deepcopy(base)
        changed['store_metadata']['stores'][0]['store']['id'] = 44
        changed['store_metadata']['stores'][0]['status']['last_heartbeat_ts'] = 'later'
        self.assertEqual(analyze.stable_topology('tikv_raw', base), analyze.stable_topology('tikv_raw', changed))
        changed['store_metadata']['stores'][0]['store']['version'] = 'different-release'
        self.assertNotEqual(analyze.stable_topology('tikv_raw', base), analyze.stable_topology('tikv_raw', changed))


    def test_valkey_dynamic_aof_counters_remain_raw_not_repetition_configuration(self):
        first = {"node_config": [{"appendfsync": "always"}], "persistence": [{
            "aof_enabled": "1", "aof_last_write_status": "ok", "aof_current_size": "100",
            "aof_rewrite_in_progress": "0"}]}
        later = copy.deepcopy(first)
        later["persistence"][0].update(aof_current_size="999", aof_rewrite_in_progress="1")
        self.assertEqual(analyze.stable_configuration(first, "valkey_tcp"),
                         analyze.stable_configuration(later, "valkey_tcp"))
        self.assertEqual(first["persistence"][0]["aof_current_size"], "100")
        later["persistence"][0]["aof_last_write_status"] = "err"
        self.assertNotEqual(analyze.stable_configuration(first, "valkey_tcp"),
                            analyze.stable_configuration(later, "valkey_tcp"))


    def native_valkey_fixture(self, root, kind="valkey_tcp", generation=1):
        import local
        import local_valkey_rdma
        from test_valkey_lifecycle import evidence

        nodes, controls = evidence()
        ids = [f"{generation * 100 + i:040x}" for i in range(5)]
        for node in nodes:
            for i, identity in enumerate(ids):
                node["cluster_nodes"] = node["cluster_nodes"].replace(f"{i+1:040x}", identity)
                for link in node["cluster_links"]:
                    if link["node"] == f"{i+1:040x}":
                        link["node"] = identity
            node["cluster_nodes"] = node["cluster_nodes"].replace(" - 0 0 ", f" - {generation} {generation} ")
        rdma = [f"10.1.2.3:{6380+i}" for i in range(5)]
        source = "fe8d406c0b5a64ec897f6455e4de684629883764"
        directory = f"/tmp/owned-{generation}"
        runtime = "/opt/valkey"
        owner = f"{generation:032x}"
        options = local_valkey_rdma.options_for_backend(kind, 5, rdma)
        state = {"owner": owner, "backend": kind, "layout": "full", "nodes": 5,
                 "directory": directory, "runtime": runtime, "physical_hosts": 1, "independent_disks": False,
                 "paired_valkey": True, "control_endpoints": controls, "rdma_endpoints": rdma,
                 "backend_options": options, "services": []}
        records = []
        for i, node in enumerate(nodes):
            data = f"{directory}/node-{i}/data"
            node["effective_config"]["dir"] = data
            node["effective_config"]["cluster-config-file"] = f"{data}/nodes.conf"
            args = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, True, 1024)
            args = [f"{runtime}/bin/valkey-server" if x == "valkey-server" else x.replace("/data", data) for x in args]
            args += ["--rdma-bind", "10.1.2.3", "--rdma-port", str(6380+i)]
            unit = f"kvcloud-valkey-{owner[:20]}-{i}"
            state["services"].append({"runtime": runtime, "unit": unit, "args": args, "cpus": 2,
                                      "memory_mib": 1536, "source_commit": source, "server_sha256": "a" * 64})
            cgroup = f"/system.slice/{unit}.service"
            pid = generation * 1000 + i + 1
            records.append({"unit": unit, "pid": pid, "cgroup": cgroup, "argv": ["valkey-server"],
                            "source": source, "server_sha256": "a" * 64,
                            "cgroup_files": {"memory.max": str(1536 * 1024**2),
                                             "memory.swap.max": "0", "cpu.max": "200000 100000"},
                            "proc_limits": ["Max locked memory         unlimited            unlimited            bytes"],
                            "systemd": {"LoadState": "loaded", "ActiveState": "active", "MainPID": str(pid),
                                        "ControlGroup": cgroup, "CPUQuotaPerSecUSec": "2s",
                                        "MemoryMax": str(1536 * 1024**2), "LimitMEMLOCK": "infinity",
                                        "ExecStart": "argv[]=" + " ".join(args) + " ;"}})
        dirs = {endpoint: f"{directory}/node-{i}/data" for i, endpoint in enumerate(controls)}
        observed = {"nodes": nodes, "runtime": records, "source_commit": source,
                    "transport": "RDMA-CM RC" if kind == "valkey_rdma" else "TCP",
                    "coverage": local.admit_valkey(nodes, controls, True, data_dir=dirs)}
        observed["rdma_qualification" if kind == "valkey_rdma" else "tcp_qualification"] = {
            "status": "passed", "endpoints": options["nodes"], "operations": ["PING", "SET", "GET", "DEL"]}
        topology = {"cluster": True, "primary_count": 5, "endpoints": options["nodes"], "node_ids": ids,
                    "slot_count": 16384, "cluster_nodes_views": [node["cluster_nodes"] for node in nodes],
                    "routing": "static CLUSTER NODES slot map linked by CLUSTER MYID; MOVED/ASK fail"}
        settings = [{k: node["effective_config"][k] for k in
                     ("appendonly", "appendfsync", "no-appendfsync-on-rewrite", "maxmemory-policy")} for node in nodes]
        backend = {"name": kind, "server_version": "9.0.7", "client_version": "bundled libvalkey 0.2.1",
                   "replication_factor": 1, "acknowledgement": "AOF", "consistency": "primary",
                   "measurement_boundary": "client", "get_many_strategy": "slot", "batch_atomicity": "unsupported",
                   "topology": topology, "configuration": {"source_commit": source, "node_config": settings,
                   "persistence": [node["info_persistence"] for node in nodes],
                   "transport": ("upstream libvalkey VALKEY_CONN_RDMA, RDMA-CM RC" if kind == "valkey_rdma"
                                 else "upstream libvalkey VALKEY_CONN_TCP")}}
        config = {"backend": {"kind": kind, "options": options}, "namespace": f"run-{generation}",
                  "seed": 17, "dataset": {}, "workload": {}, "execution": {"mode": "closed_loop"},
                  "phases": {"measurement_operations": 1}, "limits": {}}
        deployment = {"topology": state, "observed": observed}
        (root / "deployment").mkdir(parents=True)
        (root / "result").mkdir()
        def dump(path, value):
            path.write_text(json.dumps(value))
        dump(root / "deployment/owner.json", state)
        dump(root / "deployment/services.json", [[record] for record in records])
        dump(root / "deployment/deployment.json", deployment)
        dump(root / "config.json", config)
        dump(root / "normalized-config.json", config)
        manifest = {"status": "completed", "run": {"config": config, "binary": {"blake2b512": "binary-proof"}},
                    "backend_info": backend}
        dump(root / "result/manifest.json", manifest)
        dump(root / "result/summary.json", {"status": "completed", "backend": backend})
        (root / "result/samples.csv").write_text(
            "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind\n"
            "measurement,get,ok,1,2,true,5,3,4,1,1,8,\n")
        (root / "result/metrics.prom").write_text("")
        observation = {"state": "exact_project_present"}
        receipt = {"status": "completed", "claim_boundary": "performance", "observation": observation,
                   "observation_after": observation, "started_at_unix": generation, "binary_sha256": "binary",
                   "binary_blake2b512": "binary-proof", "client_budget": {}, "storage_population": {},
                   "hardware": {"memory": "MemTotal: 1024 kB\nSwapTotal: 0 kB\n", "machine": "x86_64",
                                "cpu": "vendor_id: test\n", "kernel": "test", "affinity": "all", "block_devices": []}}
        receipt["config_sha256"] = hashlib.sha256((root / "config.json").read_bytes()).hexdigest()
        receipt["normalized_config_sha256"] = hashlib.sha256((root / "normalized-config.json").read_bytes()).hexdigest()
        receipt["artifacts_sha256"] = {name: hashlib.sha256((root / "result" / name).read_bytes()).hexdigest()
                                       for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}
        dump(root / "receipt.json", receipt)
        return deployment, backend, config

    def test_native_valkey_real_shape_pools_regenerated_views(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / "first", Path(directory) / "second"
            first.mkdir()
            second.mkdir()
            self.native_valkey_fixture(first, generation=1)
            self.native_valkey_fixture(second, generation=2)
            self.assertEqual(analyze.boundary(first), analyze.boundary(second))
            self.assertEqual(len(analyze.analyze([first, second])["per_run_accounting"]), 2)
            self.assertEqual(len(analyze.analyze([first])["per_run_accounting"]), 1)

    def test_native_valkey_rejects_missing_or_inconsistent_runtime_and_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment, backend, config = self.native_valkey_fixture(root)
            analyze.admit_native_valkey(root, deployment, backend, config)
            for change in ("view", "bus", "identity", "durability", "coverage", "runtime", "cpu",
                           "cgroup", "swap", "memlock", "exec", "cluster_file", "missing"):
                altered, core = copy.deepcopy(deployment), copy.deepcopy(backend)
                if change == "view":
                    core["topology"]["cluster_nodes_views"][0] = core["topology"]["cluster_nodes_views"][0].replace("0-3275", "1-3275")
                elif change == "bus":
                    core["topology"]["cluster_nodes_views"][0] = core["topology"]["cluster_nodes_views"][0].replace("@16379", "@16380")
                elif change == "identity":
                    core["topology"]["node_ids"][0] = "f" * 40
                elif change == "durability":
                    altered["observed"]["nodes"][0]["effective_config"]["appendfsync"] = "everysec"
                elif change == "coverage":
                    altered["observed"]["coverage"]["slot_count"] = 100
                elif change == "runtime":
                    altered["observed"]["runtime"][0]["systemd"]["MemoryMax"] = "1"
                elif change == "cpu":
                    altered["observed"]["runtime"][0]["systemd"]["CPUQuotaPerSecUSec"] = "1s"
                elif change == "cgroup":
                    del altered["observed"]["runtime"][0]["cgroup_files"]
                elif change == "swap":
                    altered["observed"]["runtime"][0]["cgroup_files"]["memory.swap.max"] = "max"
                elif change == "memlock":
                    altered["observed"]["runtime"][0]["proc_limits"] = ["Max locked memory  unlimited  1024  bytes"]
                elif change == "exec":
                    altered["observed"]["runtime"][0]["systemd"]["ExecStart"] = "different"
                elif change == "cluster_file":
                    del altered["observed"]["nodes"][0]["effective_config"]["cluster-config-file"]
                else:
                    del altered["observed"]["runtime"]
                with self.subTest(change=change), self.assertRaises(ValueError):
                    analyze.admit_native_valkey(root, altered, core, config)

    def test_native_valkey_valid_changed_ownership_and_settings_change_boundary(self):
        import local

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment, backend, config = self.native_valkey_fixture(root)
            original = analyze.admit_native_valkey(root, deployment, backend, config)
            altered, core = copy.deepcopy(deployment), copy.deepcopy(backend)
            for node in altered["observed"]["nodes"]:
                node["cluster_nodes"] = node["cluster_nodes"].replace("0-3275", "0-3274").replace("3276-6552", "3275-6552")
            core["topology"]["cluster_nodes_views"] = [node["cluster_nodes"] for node in altered["observed"]["nodes"]]
            state = altered["topology"]
            dirs = {endpoint: f"{state['directory']}/node-{i}/data"
                    for i, endpoint in enumerate(state["control_endpoints"])}
            altered["observed"]["coverage"] = local.admit_valkey(altered["observed"]["nodes"],
                                                                  state["control_endpoints"], True, data_dir=dirs)
            self.assertNotEqual(original[1], analyze.admit_native_valkey(root, altered, core, config)[1])
            altered = copy.deepcopy(deployment)
            altered["observed"]["nodes"][0]["effective_config"]["save"] = "900 1"
            self.assertNotEqual(original[2], analyze.admit_native_valkey(root, altered, backend, config)[2])

    def test_native_tcp_and_rdma_are_distinct_from_each_other_and_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            tcp, rdma = Path(directory) / "tcp", Path(directory) / "rdma"
            tcp.mkdir()
            rdma.mkdir()
            self.native_valkey_fixture(tcp)
            self.native_valkey_fixture(rdma, kind="valkey_rdma")
            self.assertNotEqual(analyze.boundary(tcp), analyze.boundary(rdma))
            with self.assertRaisesRegex(ValueError, "different workload"):
                analyze.analyze([tcp, rdma])
            deployment = json.loads((tcp / "deployment/deployment.json").read_text())
            deployment["topology"]["paired_valkey"] = False
            (tcp / "deployment/deployment.json").write_text(json.dumps(deployment))
            with self.assertRaises(FileNotFoundError):
                analyze.boundary(tcp)

    def test_industry_signatures_keep_policies_but_exclude_info_counters(self):
        for name, policy, changed in (
            ("garnet", {"aof-commit-freq": "0", "aof-commit-wait": "yes"},
             {"aof-commit-freq": "1", "aof-commit-wait": "yes"}),
            ("kvrocks", {"rocksdb.write_options.disable_wal": "no", "rocksdb.write_options.sync": "yes",
                         "rocksdb.block_cache_size": "1024"},
             {"rocksdb.write_options.disable_wal": "no", "rocksdb.write_options.sync": "no",
              "rocksdb.block_cache_size": "1024"}),
        ):
            first = {"node_config": [policy], "node_info": [{"used_memory": "1"}],
                     "storage_tier_evidence": "lifecycle proof required"}
            second = copy.deepcopy(first)
            second["node_info"][0]["used_memory"] = "500"
            self.assertEqual(analyze.stable_configuration(first, name),
                             analyze.stable_configuration(second, name))
            second["node_config"] = [changed]
            self.assertNotEqual(analyze.stable_configuration(first, name),
                                analyze.stable_configuration(second, name))
            topology = {"primaries": [{"node_id": "old", "address": "host:1", "slots": [[0, 100]]}]}
            later = {"primaries": [{**topology["primaries"][0], "node_id": "new"}]}
            self.assertEqual(analyze.stable_topology(name, topology), analyze.stable_topology(name, later))
            later["primaries"][0]["slots"] = [[0, 99]]
            self.assertNotEqual(analyze.stable_topology(name, topology), analyze.stable_topology(name, later))

    def industry_fixture(self, kind):
        from test_industry_service import valid_nodes
        nodes, endpoints = valid_nodes(kind)
        slots = nodes[0]["cluster_slots"]
        ids = [row[2][2] for row in slots]
        coverage = {"required_primary_nodes": 5, "replication_factor": 1, "replica_count": 0,
                    "primary_endpoints": endpoints, "slot_count": 16384,
                    "primary_ids": dict(zip(endpoints, ids)),
                    "durability": ("AOF commit per operation and commit-wait" if kind == "garnet"
                                   else "RocksDB WAL enabled and write_options.sync=yes")}
        primaries = [{"node_id": ids[i], "address": endpoints[i], "slots": slots[i][1] - slots[i][0] + 1,
                      "slot_ranges": [[slots[i][0], slots[i][1]]]} for i in range(5)]
        return ({"topology": {"nodes": 5}, "observed": {"nodes": nodes, "coverage": coverage}},
                {"name": kind, "replication_factor": 1,
                 "topology": {"cluster": True, "primary_count": 5, "slot_count": 16384, "primaries": primaries}})

    def test_industry_offline_reconstruction_rejects_missing_or_conflicting_proof(self):
        for kind in ("garnet", "kvrocks"):
            deployment, backend = self.industry_fixture(kind)
            self.assertEqual(analyze.admit_industry_observation(kind, deployment, backend),
                             deployment["observed"]["coverage"])
            cases = []
            missing = copy.deepcopy(deployment)
            del missing["observed"]["nodes"]
            cases.append((missing, backend))
            incomplete = copy.deepcopy(deployment)
            incomplete["observed"]["nodes"][0]["cluster_slots"][0][1] -= 1
            cases.append((incomplete, backend))
            durability = copy.deepcopy(deployment)
            key, value = (("aof-commit-wait", "no") if kind == "garnet"
                          else ("rocksdb.write_options.sync", "no"))
            durability["observed"]["nodes"][0]["effective_config"][key] = value
            cases.append((durability, backend))
            false_coverage = copy.deepcopy(deployment)
            false_coverage["observed"]["coverage"]["replica_count"] = 1
            cases.append((false_coverage, backend))
            inconsistent = copy.deepcopy(backend)
            inconsistent["topology"]["primaries"][0]["slot_ranges"][0][1] -= 1
            cases.append((deployment, inconsistent))
            for candidate, core in cases:
                with self.subTest(kind=kind, candidate=candidate, core=core):
                    with self.assertRaises(ValueError):
                        analyze.admit_industry_observation(kind, candidate, core)

    def test_industry_launch_proof_is_required_and_bound_to_containers(self):
        import industry_service
        import local

        for kind in ("garnet", "kvrocks"):
            state = {"backend": kind, "project": "owned", "owner": "run", "directory": "/tmp/owned"}
            runtime = []
            for i in range(5):
                service = f"{kind}{i}"
                address = f"127.0.0.{31+i}"
                mounts = [{"Type": "volume", "Name": f"owned_{service}-data",
                           "Destination": "/data", "RW": True}]
                item = {"service": service, "id": str(i), "image_id": "sha256:image",
                        "image": local.IMAGES[kind], "state": {"Running": True},
                        "repo_digests": [local.IMAGES[kind]], "mounts": mounts,
                        "memory_bytes": 1536 * 1024**2, "nano_cpus": 2_000_000_000,
                        "memory_swap_bytes": 1536 * 1024**2,
                        "volume_labels": {local.OWNER_LABEL: "run"},
                        "entrypoint": ["/app/GarnetServer"] if kind == "garnet" else ["kvrocks"],
                        "cmd": industry_service.garnet_args(address, 6379, 1536) if kind == "garnet"
                               else ["-c", "/config/kvrocks.conf"]}
                if kind == "kvrocks":
                    mounts.append({"Type": "bind", "Source": f"/tmp/owned/{service}.conf",
                                   "Destination": "/config/kvrocks.conf", "RW": False})
                    item["config_text"] = industry_service.kvrocks_config(address, 6379, 1536)
                runtime.append(item)
            deployment = {"topology": state, "observed": {"runtime": runtime}}
            containers = [{key: item[key] for key in ("service", "id", "image_id")} for item in runtime]
            self.assertEqual(analyze.admit_industry_runtime(kind, deployment, containers)["containers"], 5)
            missing = copy.deepcopy(deployment)
            del missing["observed"]["runtime"]
            with self.assertRaises(ValueError):
                analyze.admit_industry_runtime(kind, missing, containers)
            altered = copy.deepcopy(deployment)
            if kind == "garnet":
                altered["observed"]["runtime"][0]["cmd"].remove("--storage-tier")
            else:
                altered["observed"]["runtime"][0]["config_text"] = "rocksdb.write_options.sync no"
            with self.assertRaises(ValueError):
                analyze.admit_industry_runtime(kind, altered, containers)
            foreign = copy.deepcopy(containers)
            foreign[0]["id"] = "foreign"
            with self.assertRaises(ValueError):
                analyze.admit_industry_runtime(kind, deployment, foreign)

    def test_host_memory_and_swap_capacity_are_part_of_the_contract(self):
        small = analyze.host_capacity('MemTotal: 1024 kB\nMemFree: 12 kB\nSwapTotal: 0 kB\n')
        equally_sized = analyze.host_capacity('MemTotal: 1024 kB\nMemFree: 900 kB\nSwapTotal: 0 kB\n')
        bigger = analyze.host_capacity('MemTotal: 2048 kB\nSwapTotal: 0 kB\n')
        self.assertEqual(small, equally_sized)
        self.assertNotEqual(small, bigger)
        self.assertEqual(small['MemTotal'], 1048576)
        with self.assertRaisesRegex(ValueError, 'capacity evidence'):
            analyze.host_capacity('MemFree: 1024 kB\n')


class ClockAdmission(unittest.TestCase):
    fields = ['phase','operation','outcome','scheduled_ns','dispatched_ns','dispatch_observed',
              'completed_ns','api_ns','response_ns','scheduling_ns','logical_keys','bytes','error_kind']

    def read_row(self, row):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.csv'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=self.fields)
                writer.writeheader()
                writer.writerow(row)
            return list(analyze.read_samples(path))[0]

    def sample(self):
        return dict(zip(self.fields, ['measurement','read','success',100,110,'true',140,30,40,10,1,256,'']))

    def test_clock_decomposition_and_undispatched_rejection_are_distinct(self):
        row = self.read_row(self.sample())
        self.assertEqual(row['response_ns'], row['api_ns'] + row['scheduling_ns'])
        rejected = self.sample()
        rejected.update(outcome='rejected', dispatch_observed='false', dispatched_ns=0, api_ns=0, scheduling_ns=0)
        row = self.read_row(rejected)
        self.assertIsNone(row['api_ns'])
        self.assertIsNone(row['scheduling_ns'])
        self.assertEqual(row['response_ns'], 40)

    def test_inconsistent_or_invented_clocks_are_rejected(self):
        cases = [{'api_ns': 29}, {'response_ns': 41}, {'scheduling_ns': 9},
                 {'dispatched_ns': 150}, {'outcome': 'rejected'},
                 {'dispatch_observed': 'false'}, {'logical_keys': 0}]
        for changes in cases:
            with self.subTest(changes=changes):
                row = self.sample()
                row.update(changes)
                with self.assertRaises(ValueError):
                    self.read_row(row)


class TerminalTimingAdmission(unittest.TestCase):
    def test_incomplete_phases_and_early_dispatch_cannot_be_completed(self):
        import campaign
        with tempfile.TemporaryDirectory() as folder:
            result = Path(folder)
            config = {'dataset': {'keys': 1}, 'qualification_only': False,
                      'phases': {'preload': False, 'warmup_operations': 1,
                                 'aging_operations': 0, 'measurement_operations': 1}}
            warmup = ClockAdmission().sample()
            warmup['phase'] = 'warmup'
            measured = ClockAdmission().sample()
            for field in ('scheduled_ns', 'dispatched_ns', 'completed_ns'):
                measured[field] += 1000
            def publish(rows):
                with (result / 'samples.csv').open('w', newline='') as stream:
                    writer = csv.DictWriter(stream, fieldnames=ClockAdmission.fields)
                    writer.writeheader()
                    writer.writerows(rows)
            publish([warmup, measured])
            self.assertEqual(campaign.verify_timing_result(result, config),
                             {'preload': 0, 'warmup': 1, 'aging': 0, 'measurement': 1})
            publish([measured])
            with self.assertRaises(ValueError):
                campaign.verify_timing_result(result, config)
            warmup.update(dispatched_ns=90, api_ns=50, scheduling_ns=0)
            publish([warmup, measured])
            with self.assertRaises(ValueError):
                campaign.verify_timing_result(result, config)


class EffectiveRoleIdentity(unittest.TestCase):
    def test_cache_and_heap_changes_are_not_masked_as_ephemeral_identity(self):
        import local
        original = {'Config': {'Cmd': ['fdbserver', '--cache-memory', '128MiB'],
                               'Entrypoint': None,
                               'Env': ['HOSTNAME=first', 'CASSANDRA_CLUSTER_NAME=first',
                                       'MAX_HEAP_SIZE=512M']}}
        identity = local.runtime_configuration_hash(original)
        renamed = json.loads(json.dumps(original))
        renamed['Config']['Env'][:2] = ['HOSTNAME=second', 'CASSANDRA_CLUSTER_NAME=second']
        self.assertEqual(local.runtime_configuration_hash(renamed), identity)
        changed_cache = json.loads(json.dumps(original))
        changed_cache['Config']['Cmd'][-1] = '256MiB'
        self.assertNotEqual(local.runtime_configuration_hash(changed_cache), identity)
        changed_heap = json.loads(json.dumps(original))
        changed_heap['Config']['Env'][-1] = 'MAX_HEAP_SIZE=1G'
        self.assertNotEqual(local.runtime_configuration_hash(changed_heap), identity)


if __name__ == '__main__':
    unittest.main()
