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

    def test_valkey_primary_order_is_stable_but_topology_and_settings_remain_distinct(self):
        first = {"cluster": True, "required_primary_nodes": 5, "observed_primary_nodes": 5,
                 "primaries": [{"node_id": f"old-{i}", "address": f"10.0.0.{i+2}:6379",
                                "slots": [[i*1000, i*1000+999]]} for i in range(5)]}
        original = copy.deepcopy(first)
        reordered = {**first, "primaries": [{**row, "node_id": "new-" + row["node_id"]}
                                           for row in reversed(first["primaries"])]}
        self.assertEqual(analyze.stable_topology("valkey_tcp", first),
                         analyze.stable_topology("valkey_tcp", reordered))
        changed = {**first, "observed_primary_nodes": 1}
        self.assertNotEqual(analyze.stable_topology("valkey_tcp", first),
                            analyze.stable_topology("valkey_tcp", changed))
        always = {"node_config": [{"appendfsync": "always", "maxmemory-policy": "noeviction"}]}
        everysec = {"node_config": [{"appendfsync": "everysec", "maxmemory-policy": "noeviction"}]}
        self.assertNotEqual(analyze.stable_configuration(always), analyze.stable_configuration(everysec))
        self.assertEqual(first, original)

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

    def test_valkey_rdma_evidence_normalization_preserves_health_and_slots(self):
        topology = {"primaries": [{"node_id": "one", "address": "10.0.0.1:6379", "slots": [[0, 10]]}]}
        changed_id = {"primaries": [{**topology["primaries"][0], "node_id": "two"}]}
        self.assertEqual(analyze.stable_topology("valkey_rdma", topology),
                         analyze.stable_topology("valkey_rdma", changed_id))
        first = {"persistence": [{"aof_enabled": "1", "aof_last_write_status": "ok", "aof_current_size": "1"}]}
        second = copy.deepcopy(first)
        second["persistence"][0]["aof_current_size"] = "2"
        self.assertEqual(analyze.stable_configuration(first, "valkey_rdma"),
                         analyze.stable_configuration(second, "valkey_rdma"))
        second["persistence"][0]["aof_last_write_status"] = "err"
        self.assertNotEqual(analyze.stable_configuration(first, "valkey_rdma"),
                            analyze.stable_configuration(second, "valkey_rdma"))

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
