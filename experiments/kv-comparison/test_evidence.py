import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import analyze


class LiveTimelineAdmission(unittest.TestCase):
    def test_live_groups_and_partial_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "result"
            result.mkdir()
            config = {"backend": {"kind": "tidehunter"}, "diagnostics": {"write_timeline_limit": 2}}
            backend = "tidehunter_live_nvmeof"
            (result / "manifest.json").write_text(json.dumps(
                {"run": {"config": config}, "backend_info": {"name": backend}}))
            (result / "samples.csv").write_text(
                "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id\n"
                "measurement,put,success,0,1,true,11,10,11,1,1,8,,0\n"
                "measurement,put,success,0,1,true,11,10,11,1,1,8,,1\n")
            batches = [{"epoch": 1, "sequence": i} for i in range(2)]
            records = [{"caller_sample_id": i, "completed": True, "error": None,
                        "operation": [i] * 16, "batch": batches[i],
                        "events": [{"stage": "live_io_started", "elapsed_ns": 2, "owner": 0, "envelope_id": None},
                                   {"stage": "live_io_completed", "elapsed_ns": 5, "owner": 0, "envelope_id": None}]}
                       for i in range(2)]
            profile = {field: 0 for field in analyze._STORAGE_WRITE_PROFILE_FIELDS}
            profile["persistence_capture"] = None
            group = {"owner": 0, "batches": copy.deepcopy(batches), "reservation_token": [1] * 16,
                     "owner_validation_ns": 3, "owner_persistence": profile}
            trace = {"limit": 2, "records": records, "live_groups": [group], "dropped_live_groups": 0,
                     "owner_records": [], "checkpoint_events": [], "owner_errors": [],
                     "dropped_calls": 0, "dropped_events": 0, "incomplete_calls": 0,
                     "dropped_checkpoint_events": 0, "owner_dropped_records": 0,
                     "owner_incomplete_records": 0, "owner_expected_connections": 0,
                     "owner_captured_connections": 0, "deadline_exceeded": False,
                     "owner_storage_durations_available": False}
            data = {"schema_version": 1, "requested_limit": 2, "measurement_attempted": True,
                    "measurement_drained": True, "available": True, "status": "complete",
                    "error": None, "timeline": trace}
            path = result / "write-timeline.json"
            def admit(value, completed=True):
                path.write_text(json.dumps(value))
                return analyze.admit_write_timeline(root, config, completed=completed, expected_backend=backend)
            self.assertEqual(admit(data)["coverage"]["live_groups"], 1)
            summary = analyze.summarize_write_timeline(list(analyze.read_samples(result / "samples.csv")), data)
            self.assertEqual(summary["live_group_observations"]["linked_groups"], 1)
            self.assertEqual(summary["live_group_observations"]["linked_memberships"], 2)
            self.assertEqual(len(summary["live_group_observations"]["owner_persistence"]), 1)
            self.assertEqual(summary["stage_summaries_ns"]["live_io_ns"]["p50"], 3)
            split = copy.deepcopy(data)
            for record in split["timeline"]["records"]:
                record["events"][1:1] = [
                    dict(stage="live_frame_encoded", elapsed_ns=3, owner=0, envelope_id=None),
                    dict(stage="live_io_submitted", elapsed_ns=4, owner=0, envelope_id=None)]
            self.assertEqual(admit(split)["coverage"]["live_groups"], 1)
            split_summary = analyze.summarize_write_timeline(
                list(analyze.read_samples(result / "samples.csv")), split)
            for stage in ("live_frame_encode_ns", "live_io_submit_ns", "live_io_wait_ns"):
                self.assertEqual(split_summary["stage_summaries_ns"][stage]["p50"], 1)
            retry = copy.deepcopy(data)
            retry["timeline"]["live_groups"].append(copy.deepcopy(group))
            self.assertEqual(admit(retry)["coverage"]["live_groups"], 2)
            # Leased groups carry frame coordinates (schema 2 exports); bad values are rejected.
            located = copy.deepcopy(data)
            located["schema_version"] = 2
            located["timeline"]["origin_monotonic_ns"] = 7
            located["timeline"]["live_groups"][0].update(
                wal_position=8192, file_offset=8192, frame_len=4096, wal_file=0, namespace_block_size=4096)
            self.assertEqual(admit(located)["coverage"]["live_groups"], 1)
            for field, value in (("frame_len", 0), ("namespace_block_size", 3000),
                                 ("file_offset", -1), ("wal_file", True), ("extra", 1)):
                bad = copy.deepcopy(located)
                bad["timeline"]["live_groups"][0][field] = value
                with self.assertRaisesRegex(ValueError, "invalid live group"):
                    admit(bad)
            for mutate in ("legacy", "duplicate", "unknown", "owner", "token", "storage", "counter"):
                broken = copy.deepcopy(data)
                g = broken["timeline"]["live_groups"][0]
                if mutate == "legacy":
                    g["batches"] = [[1, 0]]
                elif mutate == "duplicate":
                    g["batches"].append(g["batches"][0])
                elif mutate == "unknown":
                    g["batches"][0]["sequence"] = 9
                elif mutate == "owner":
                    g["owner"] = 99
                elif mutate == "token":
                    g["reservation_token"][0] = True
                elif mutate == "storage":
                    g["owner_persistence"]["persist_calls"] = None
                else:
                    broken["timeline"]["dropped_live_groups"] = True
                with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                    admit(broken)
            partial = copy.deepcopy(data)
            partial.update(available=False, status="failed", error="completion failed")
            partial["timeline"]["live_groups"][0].update(reservation_token=None, owner_validation_ns=None,
                                                            owner_persistence=None)
            partial["timeline"]["records"][0].update(completed=False, error="completion failed")
            partial["timeline"]["incomplete_calls"] = 1
            self.assertFalse(admit(partial, completed=False)["coverage"]["available"])
            partial["timeline"]["live_groups"][0]["owner"] = 99
            partial["timeline"]["dropped_events"] = 1
            admit(partial, completed=False)
            partial_summary = analyze.summarize_write_timeline(
                list(analyze.read_samples(result / "samples.csv")), partial)
            self.assertEqual(partial_summary["live_group_observations"]["linked_groups"], 0)
            with self.assertRaises(ValueError):
                admit(partial)


class EvidenceAdmission(unittest.TestCase):
    def test_nvme_route_is_requested_population_not_observed_proof(self):
        live = {"job": {"backend": "tidehunter_nvme_tcp", "cohort": "primary_rf1",
                        "nvmeof_route": "live"}}
        sealed = {"job": {"backend": "tidehunter_nvme_tcp", "cohort": "sealed_read",
                          "nvmeof_route": "sealed_read"}}
        self.assertEqual(analyze.requested_nvmeof_route(live), "live")
        self.assertEqual(analyze.requested_nvmeof_route(sealed), "sealed_read")
        self.assertIsNone(analyze.requested_nvmeof_route({}))
        sealed["job"]["cohort"] = "primary_rf1"
        with self.assertRaisesRegex(ValueError, "population disagree"):
            analyze.requested_nvmeof_route(sealed)

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

    def test_fair_native_write_diagnostics_require_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            receipt["job"] = {"selection": "fair_native"}
            path = root / "result/write-diagnostics.json"
            data = {"schema_version": 1}
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "write diagnostics.*integrity binding"):
                analyze.verify_binding(root, config, receipt, manifest, summary)
            receipt["write_diagnostics"] = {"path": "result/write-diagnostics.json",
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": data}
            analyze.verify_binding(root, config, receipt, manifest, summary)
            path.write_text("{}")
            with self.assertRaisesRegex(ValueError, "write diagnostics.*integrity binding"):
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


    def native_valkey_fixture(self, root, kind="valkey_tcp", generation=1, ack_contract="synced"):
        import local
        import local_valkey_rdma
        from test_valkey_lifecycle import evidence

        nodes, controls = evidence()
        for node in nodes:
            node["effective_config"]["appendfsync"] = analyze.contracts.valkey_fsync(ack_contract)
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
        options = local_valkey_rdma.options_for_backend(kind, 5, rdma, ack_contract=ack_contract)
        state = {"owner": owner, "backend": kind, "layout": "full", "nodes": 5,
                 "directory": directory, "runtime": runtime, "physical_hosts": 1, "independent_disks": False,
                 "paired_valkey": True, "control_endpoints": controls, "rdma_endpoints": rdma,
                 "backend_options": options, "services": []}
        if ack_contract != "synced":
            state["ack_contract"] = ack_contract
        records = []
        for i, node in enumerate(nodes):
            data = f"{directory}/node-{i}/data"
            node["effective_config"]["dir"] = data
            node["effective_config"]["cluster-config-file"] = f"{data}/nodes.conf"
            args = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, True, 1024,
                                           ack_contract=ack_contract)
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
                    "coverage": local.admit_valkey(nodes, controls, True, data_dir=dirs,
                                                  ack_contract=ack_contract)}
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
        if ack_contract != "synced":
            backend["acknowledgement"] = analyze.contracts.acknowledgement(kind, ack_contract)
            backend["configuration"]["ack_contract"] = ack_contract
        config = {"backend": {"kind": kind, "options": dict(options)}, "namespace": f"run-{generation}",
                  "seed": 17, "dataset": {}, "workload": {}, "execution": {"mode": "closed_loop"},
                  "phases": {"measurement_operations": 1}, "limits": {}}
        if ack_contract != "synced":
            config["backend"]["options"]["ack_contract"] = ack_contract
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

    def test_external_ack_contracts_use_live_settings_and_never_pool(self):
        for kind, contract in ((kind, contract) for kind in ("valkey_tcp", "valkey_rdma")
                               for contract in ("periodic_sync", "os_buffered")):
            with self.subTest(kind=kind, contract=contract), tempfile.TemporaryDirectory() as directory:
                synced, relaxed = Path(directory) / "synced", Path(directory) / "relaxed"
                synced.mkdir()
                relaxed.mkdir()
                self.native_valkey_fixture(synced, kind=kind)
                deployment, backend, config = self.native_valkey_fixture(
                    relaxed, kind=kind, generation=2, ack_contract=contract)
                self.assertNotIn("ack_contract", analyze.boundary(synced)["backend"])
                selected = analyze.boundary(relaxed)["backend"]
                self.assertEqual(selected["ack_contract"], contract)
                self.assertEqual(selected["acknowledgement"],
                                 analyze.contracts.acknowledgement(kind, contract))
                with self.assertRaisesRegex(ValueError, "different workload"):
                    analyze.analyze([synced, relaxed])
                with self.assertRaisesRegex(ValueError, "selected acknowledgement"):
                    analyze.admit_native_valkey(relaxed, deployment, backend, config)
                deployment["observed"]["nodes"][0]["effective_config"]["appendfsync"] = "always"
                with self.assertRaises(ValueError):
                    analyze.admit_native_valkey(
                        relaxed, deployment, backend, config, ack_contract=contract)

    def test_nonpaired_valkey_checks_live_fsync_and_job_deployment_contract(self):
        for contract in ("synced", "periodic_sync", "os_buffered"):
            with self.subTest(contract=contract), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                deployment, _, _ = self.native_valkey_fixture(root, ack_contract=contract)
                deployment["topology"]["paired_valkey"] = False
                deployment["observed"]["nodes"][0]["effective_config"]["appendfsync"] = (
                    "everysec" if contract == "synced" else "always")
                (root / "deployment/deployment.json").write_text(json.dumps(deployment))
                with self.assertRaisesRegex(ValueError, "live appendfsync"):
                    analyze.boundary(root)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.native_valkey_fixture(root, ack_contract="periodic_sync")
            receipt = json.loads((root / "receipt.json").read_text())
            receipt["job"] = {"ack_contract": "synced"}
            (root / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "reported backend ack_contract"):
                analyze.boundary(root)

    def test_reported_external_contract_is_required_and_must_match_selection(self):
        for selected, reported in (("periodic_sync", None), ("os_buffered", "periodic_sync"),
                                   ("synced", "os_buffered")):
            with self.subTest(selected=selected, reported=reported), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.native_valkey_fixture(root, ack_contract=selected)
                for filename, field in (("manifest.json", "backend_info"), ("summary.json", "backend")):
                    path = root / "result" / filename
                    data = json.loads(path.read_text())
                    configuration = data[field]["configuration"]
                    configuration.pop("ack_contract", None)
                    if reported is not None:
                        configuration["ack_contract"] = reported
                    path.write_text(json.dumps(data))
                receipt = json.loads((root / "receipt.json").read_text())
                for filename in ("manifest.json", "summary.json"):
                    receipt["artifacts_sha256"][filename] = hashlib.sha256(
                        (root / "result" / filename).read_bytes()).hexdigest()
                (root / "receipt.json").write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, "reported backend ack_contract"):
                    analyze.boundary(root)

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

    def test_kvrocks_relaxed_contract_requires_unsynced_enabled_live_wal(self):
        import industry_service

        deployment, backend = self.industry_fixture("kvrocks")
        nodes = deployment["observed"]["nodes"]
        endpoints = deployment["observed"]["coverage"]["primary_endpoints"]
        for node in nodes:
            node["effective_config"]["rocksdb.write_options.sync"] = "no"
        deployment["observed"]["coverage"] = industry_service.admit(
            "kvrocks", nodes, endpoints, ack_contract="page_cache")
        self.assertEqual(analyze.admit_industry_observation(
            "kvrocks", deployment, backend, ack_contract="page_cache"), deployment["observed"]["coverage"])
        with self.assertRaises(ValueError):
            analyze.admit_industry_observation("kvrocks", deployment, backend)
        for key, value in (("rocksdb.write_options.sync", "yes"),
                           ("rocksdb.write_options.disable_wal", "yes")):
            changed = copy.deepcopy(deployment)
            changed["observed"]["nodes"][0]["effective_config"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                analyze.admit_industry_observation(
                    "kvrocks", changed, backend, ack_contract="page_cache")

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

    def test_timeline_ids_and_partial_records(self):
        self.assertIsNone(self.read_row(self.sample())['caller_sample_id'])
        self.fields = self.fields + ['caller_sample_id']
        sample = self.sample()
        sample['caller_sample_id'] = '7'
        row = self.read_row(sample)
        self.assertEqual(row['caller_sample_id'], 7)
        sample['caller_sample_id'] = '-1'
        with self.assertRaises(ValueError):
            self.read_row(sample)
        evidence = {'available': True, 'timeline': {'records': [
            {'caller_sample_id': 7, 'completed': False, 'error': 'error',
             'events': [{'stage': 'caller_start', 'elapsed_ns': 10},
                        {'stage': 'gate_acquired', 'elapsed_ns': 20},
                        {'stage': 'tx_queued', 'elapsed_ns': 21, 'owner': 3, 'envelope_id': 9},
                        {'stage': 'completion_harvested', 'elapsed_ns': 30, 'owner': 3, 'envelope_id': 9},
                        {'stage': 'owner_enqueue_started', 'elapsed_ns': 15},
                        {'stage': 'owner_dequeued', 'elapsed_ns': 19},
                        {'stage': 'append_enqueue_started', 'elapsed_ns': 20},
                        {'stage': 'worker_dequeued', 'elapsed_ns': 21},
                        {'stage': 'published', 'elapsed_ns': 35},
                        {'stage': 'caller_reply_queued', 'elapsed_ns': 39}]},
            {'caller_sample_id': 8, 'completed': False, 'events': []}],
            'owner_records': [{'owner': 3, 'envelope_id': 9, 'success': True,
                               'control_queue_ns': 5, 'wal_file_persist_ns': None}],
            'owner_storage_durations_available': True, 'owner_dropped_records': 0,
            'dropped_calls': 0, 'dropped_events': 1, 'incomplete_calls': 2}}
        result = analyze.summarize_write_timeline([row], evidence)
        self.assertEqual(result['joined_calls'], 1)
        self.assertEqual(result['records_without_raw'], 1)
        self.assertEqual(result['stage_summaries_ns']['shared_gate_ns']['p50'], 10)
        self.assertEqual(result['stage_summaries_ns']['queued_to_harvest_including_remote_ns']['p50'], 9)
        self.assertEqual(result['stage_summaries_ns']['owner_enqueue_to_dequeue_ns']['p50'], 4)
        self.assertEqual(result['stage_summaries_ns']['append_enqueue_to_dequeue_ns']['p50'], 1)
        self.assertEqual(result['stage_summaries_ns']['publication_to_reply_queued_ns']['p50'], 4)
        self.assertEqual(result['owner_envelope_stage_summaries_ns']['control_queue_ns']['p50'], 5)
        self.assertIsNone(result['owner_envelope_stage_summaries_ns']['wal_file_persist_ns']['p50'])
        self.assertEqual(result['slowest_calls'][0]['owner_records'][0]['envelope_id'], 9)
        self.assertIsNone(result['slowest_calls'][0]['stages_ns']['publication_ns'])
        self.assertEqual(result['slowest_calls'][0]['record']['error'], 'error')
        evidence['available'] = False
        evidence['status'] = 'failed'
        evidence['error'] = 'deadline exceeded'
        partial = analyze.summarize_write_timeline([row], evidence)
        self.assertFalse(partial['available'])
        self.assertEqual(partial['status'], 'failed')
        self.assertEqual(partial['joined_calls'], 1)
        self.assertFalse(analyze.summarize_write_timeline([], {'available': False, 'error': 'failed'})['available'])

    def test_timeline_reports_admission_to_append_gap_and_unavailable_events(self):
        rows = [{'caller_sample_id': n, 'api_ns': 20_000_000 + n} for n in (1, 2, 3, 4)]
        records = [
            {'caller_sample_id': 1, 'events': [
                {'stage': 'prepared', 'elapsed_ns': 95},
                {'stage': 'admitted', 'elapsed_ns': 100},
                {'stage': 'append_enqueue_started', 'elapsed_ns': 13_362_352},
                {'stage': 'worker_dequeued', 'elapsed_ns': 13_362_362, 'owner': 2},
                {'stage': 'tx_queued', 'elapsed_ns': 13_362_372, 'owner': 2, 'envelope_id': 9},
                {'stage': 'caller_reply_queued', 'elapsed_ns': 14_000_000},
                {'stage': 'caller_finished', 'elapsed_ns': 14_000_010}]},
            {'caller_sample_id': 2, 'events': [{'stage': 'admitted', 'elapsed_ns': 100}]},
            {'caller_sample_id': 3, 'events': [
                {'stage': 'admitted', 'elapsed_ns': 100},
                {'stage': 'admitted', 'elapsed_ns': 101},
                {'stage': 'append_enqueue_started', 'elapsed_ns': 200}]},
            {'caller_sample_id': 4, 'events': [
                {'stage': 'admitted', 'elapsed_ns': 200},
                {'stage': 'append_enqueue_started', 'elapsed_ns': 100}]},
        ]
        result = analyze.summarize_write_timeline(
            rows, {'available': True, 'timeline': {'records': records}})
        gap = result['stage_summaries_ns']['admitted_to_append_enqueue_start_ns']
        self.assertEqual((gap['count'], gap['p50'], gap['max']), (1, 13_362_252, 13_362_252))
        self.assertEqual(gap['unavailable'],
                         {'missing_event': 1, 'multiple_events': 1, 'reversed_events': 1})
        slow = {item['caller_sample_id']: item for item in result['slowest_calls']}
        self.assertEqual(slow[1]['stages_ns']['prepared_to_admitted_ns'], 5)
        self.assertEqual(slow[1]['stages_ns']['worker_dequeue_to_tx_queued_ns'], 10)
        self.assertEqual(slow[1]['stages_ns']['reply_queued_to_caller_finished_ns'], 10)
        for sample, reason in ((2, 'missing_event'), (3, 'multiple_events'), (4, 'reversed_events')):
            self.assertIsNone(slow[sample]['stages_ns']['admitted_to_append_enqueue_start_ns'])
            self.assertEqual(slow[sample]['stage_unavailable']['admitted_to_append_enqueue_start_ns'], reason)

    def test_timeline_owner_envelopes_are_joined_and_deduplicated(self):
        rows = [{'caller_sample_id': n, 'api_ns': 100 + n} for n in (1, 2, 3, 4)]
        records = [{'caller_sample_id': n, 'events': [
            {'stage': 'tx_queued', 'elapsed_ns': 10, 'owner': 5, 'envelope_id': 12},
            {'stage': 'completion_harvested', 'elapsed_ns': 30, 'owner': 5, 'envelope_id': 12},
        ]} for n in (1, 2, 3)]
        records.append({'caller_sample_id': 4, 'events': [
            {'stage': 'tx_queued', 'elapsed_ns': 20, 'owner': 5, 'envelope_id': 13},
        ]})
        owner = {'owner': 5, 'envelope_id': 12, 'epoch': 2, 'success': True,
                 'wal_file_persist_ns': 50, 'appender_queue_ns': 7,
                 'persistence_capture': {'durable_end': 0, 'captured_appended_end': 10,
                                         'cumulative_appended_groups': 3}}
        unrelated = {'owner': 6, 'envelope_id': 1, 'success': True, 'wal_file_persist_ns': 900}
        evidence = {'available': True, 'timeline': {'records': records,
                    'owner_records': [owner, unrelated], 'owner_dropped_records': 0}}
        summary = analyze.summarize_write_timeline(rows, evidence)
        self.assertEqual(summary['joined_calls'], 4)
        self.assertEqual(summary['linked_owner_envelopes'], 2)
        self.assertEqual(summary['linked_owner_envelopes_without_record'], 1)
        linked = summary['owner_envelope_stage_summaries_ns']['wal_file_persist_ns']
        self.assertEqual((linked['count'], linked['p50']), (1, 50))
        self.assertEqual(summary['all_owner_envelope_stage_summaries_ns']['wal_file_persist_ns']['count'], 2)
        slow = {item['caller_sample_id']: item for item in summary['slowest_calls']}
        self.assertEqual(slow[1]['owner_records'], [owner])
        self.assertEqual(slow[2]['owner_records'], [owner])
        self.assertEqual(slow[3]['owner_records'], [owner])
        self.assertEqual(slow[4]['owner_records_unavailable'], 1)
        self.assertFalse(summary['captured_group_deltas']['available'])
        evidence['timeline']['owner_records'].append(owner)
        with self.assertRaisesRegex(ValueError, 'duplicate owner/envelope'):
            analyze.summarize_write_timeline(rows, evidence)

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


class MeasurementTimeline(unittest.TestCase):
    @staticmethod
    def call(start, end, *, outcome="ok", scheduled=None):
        return {"operation": "put", "outcome": outcome, "logical_keys": 1,
                "scheduled_ns": start if scheduled is None else scheduled,
                "dispatched_ns": start, "completed_ns": end,
                "api_ns": end - start, "response_ns": end - start, "scheduling_ns": 0}

    def test_rare_full_concurrency_stall_survives_ordinary_p99(self):
        samples = []
        for _ in range(16):
            samples.append(self.call(0, 400_000_000))
            samples.extend(self.call(400_000_000 + index * 100_000,
                                     400_100_000 + index * 100_000) for index in range(255))
        samples.sort(key=lambda row: row["completed_ns"], reverse=True)
        population = analyze.aggregate(samples)["populations"][0]
        self.assertEqual(population["api_ns"]["p99"], 100_000)
        self.assertEqual(population["api_ns"]["p999"], 400_000_000)
        timeline = analyze.measurement_timeline(samples)
        self.assertEqual(timeline["largest_completion_gap_ns"], 400_000_000)
        self.assertEqual(timeline["api_calls_above_ns"]["100000000"], 16)
        self.assertEqual(sum(window["completed_calls"] for window in timeline["completion_windows"]), 4096)
        self.assertEqual([row["dispatch_offset_ns"] for row in timeline["slowest_calls"]], [0] * 16)
        self.assertLess(timeline["mean_observed_api_concurrency"], 16.01)

    def test_rejected_arrivals_keep_exposure_without_inventing_api_time(self):
        rejected = self.call(0, 20, outcome="rejected", scheduled=0)
        rejected["api_ns"] = None
        rejected["dispatched_ns"] = 0
        samples = [self.call(10, 15), rejected]
        timeline = analyze.measurement_timeline(samples)
        self.assertEqual(timeline["duration_ns"], 20)
        self.assertEqual(timeline["dispatched_calls"], 1)
        self.assertEqual(timeline["api_time_sum_ns"], 5)
        self.assertEqual(sum(row["successful_calls"] for row in timeline["completion_windows"]), 1)
        only_rejected = analyze.measurement_timeline([rejected])
        self.assertIsNone(only_rejected["api_time_sum_ns"])
        self.assertIsNone(only_rejected["mean_observed_api_concurrency"])
        self.assertIsNone(only_rejected["api_calls_above_ns"])

    def test_integer_clock_boundaries_preserve_every_completion(self):
        origin = 10**18
        samples = [self.call(origin + index, origin + index + 1) for index in range(67)]
        timeline = analyze.measurement_timeline(samples)
        self.assertEqual(timeline["duration_ns"], 67)
        self.assertEqual(timeline["largest_completion_gap_ns"], 1)
        self.assertEqual(len(timeline["completion_windows"]), 32)
        self.assertEqual(sum(window["completed_calls"] for window in timeline["completion_windows"]), 67)
        self.assertEqual(timeline["completion_windows"][-1]["end_offset_ns"], 67)
        with self.assertRaisesRegex(ValueError, "positive"):
            analyze.measurement_timeline([self.call(origin, origin)])


if __name__ == '__main__':
    unittest.main()
