import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_campaign as cloud
import cloud_controller as controller


class WriteTimelineAdmissionTests(unittest.TestCase):
    def fixture(self, root):
        result = root / "result"
        result.mkdir()
        config = {"backend": {"kind": "tidehunter"}, "diagnostics": {"write_timeline_limit": 1}}
        job = {"case_id": "trace", "backend": "tidehunter_tcp",
               "diagnostics": {"write_timeline_limit": 1}}
        (root / "normalized-config.json").write_text(json.dumps(config))
        (result / "manifest.json").write_text(json.dumps({
            "run": {"config": config}, "backend_info": {"name": "tidehunter_tcp"}}))
        (result / "samples.csv").write_text(
            "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id\n"
            "measurement,put,success,0,1,true,11,10,11,1,1,8,,0\n")
        event = lambda stage, ns: {"stage": stage, "elapsed_ns": ns, "owner": 0, "envelope_id": 7}
        trace = {"limit": 1, "records": [{"caller_sample_id": 0, "completed": True, "error": None,
                  "operation": [0] * 16, "batch": {"epoch": 1, "sequence": 0},
                  "events": [event("tx_queued", 1), event("completion_harvested", 9)]}],
                 "owner_records": [{"owner": 0, "envelope_id": 7, "success": True}],
                 "checkpoint_events": [], "owner_errors": [], "dropped_calls": 0, "dropped_events": 0,
                 "incomplete_calls": 0, "dropped_checkpoint_events": 0, "owner_dropped_records": 0,
                 "owner_incomplete_records": 0, "owner_expected_connections": 1,
                 "owner_captured_connections": 1, "deadline_exceeded": False,
                 "owner_storage_durations_available": True}
        data = {"schema_version": 1, "requested_limit": 1, "measurement_attempted": True,
                "measurement_drained": True, "available": True, "status": "complete",
                "error": None, "timeline": trace}
        path = result / "write-timeline.json"
        path.write_text(json.dumps(data))
        return job, config, data, path

    def test_next_schema_requires_monotonic_origin_and_keeps_legacy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            cloud.admit_write_timeline(root, job, config, completed=True)
            data["schema_version"] = 2
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "monotonic origin"):
                cloud.admit_write_timeline(root, job, config, completed=True)
            data["timeline"]["origin_monotonic_ns"] = 123
            path.write_text(json.dumps(data))
            cloud.admit_write_timeline(root, job, config, completed=True)

    def test_completed_timeline_requires_real_matched_case_and_bounded_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertEqual(binding["coverage"]["joined_records"], 1)
            self.assertNotIn("timeline", binding)
            for change in ("limit", "backend", "missing_frame", "missing_sample", "duplicate", "wrong_phase"):
                with self.subTest(change=change):
                    altered = copy.deepcopy(data)
                    altered_job = copy.deepcopy(job)
                    samples = root / "result/samples.csv"
                    original = samples.read_text()
                    if change == "limit":
                        altered["requested_limit"] = 2
                    elif change == "backend":
                        altered_job["backend"] = "tidehunter_efa"
                    elif change == "missing_frame":
                        altered["timeline"]["owner_records"] = []
                    elif change == "missing_sample":
                        samples.write_text(original.replace(",,0\n", ",,\n"))
                    elif change == "duplicate":
                        altered["timeline"]["records"].append(copy.deepcopy(altered["timeline"]["records"][0]))
                    else:
                        altered["measurement_attempted"] = False
                    path.write_text(json.dumps(altered))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_timeline(root, altered_job, config, completed=True)
                    samples.write_text(original)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                cloud.admit_write_timeline(root, job, config, completed=True)

    def test_extent_switch_stages_are_admitted(self):
        # Run 13 first launch: the engine emits these stages on a call whose group switched extents.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            events = data["timeline"]["records"][0]["events"]
            events[1:1] = [{"stage": "live_extent_switch_started", "elapsed_ns": 2, "owner": 0, "envelope_id": None},
                           {"stage": "live_extent_switch_completed", "elapsed_ns": 3, "owner": 0, "envelope_id": None}]
            path.write_text(json.dumps(data))
            self.assertEqual(cloud.admit_write_timeline(root, job, config, completed=True)["coverage"]["joined_records"], 1)
            events[1]["stage"] = "live_extent_switch_unknown"
            path.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                cloud.admit_write_timeline(root, job, config, completed=True)

    def test_sparse_write_ids_join_with_mixed_and_unprofiled_measurement_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            data["timeline"]["records"][0]["caller_sample_id"] = 17
            samples = root / "result/samples.csv"
            samples.write_text(samples.read_text().replace(",,0\n", ",,17\n") +
                "measurement,read,success,20,21,true,31,10,11,1,1,8,,18\n" +
                "measurement,put,success,40,41,true,51,10,11,1,1,8,,19\n")
            path.write_text(json.dumps(data))
            admitted = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertEqual(admitted["coverage"]["joined_records"], 1)
            for caller in (18, 20, 2**64):
                with self.subTest(caller=caller):
                    data["timeline"]["records"][0]["caller_sample_id"] = caller
                    path.write_text(json.dumps(data))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_timeline(root, job, config, completed=True)

    def test_checkpoint_generation_is_optional_validated_and_hash_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            event = {"stage": "adopted", "elapsed_ns": 3, "epoch": 1}
            data["timeline"]["checkpoint_events"] = [event]
            path.write_text(json.dumps(data))
            old_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            event["generation"] = 7
            path.write_text(json.dumps(data))
            new_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertNotEqual(old_binding["sha256"], new_binding["sha256"])
            for invalid in (-1, True, 2**64):
                event["generation"] = invalid
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "checkpoint timeline"):
                    cloud.admit_write_timeline(root, job, config, completed=True)
            event["generation"] = 7
            event["pressure"] = {"reason": "bytes", "batches": 10, "encoded_bytes": 100,
                                 "max_batches": 20, "max_encoded_bytes": 120}
            path.write_text(json.dumps(data))
            pressure_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertNotEqual(new_binding["sha256"], pressure_binding["sha256"])
            event["pressure"]["encoded_bytes"] = -1
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "checkpoint pressure"):
                cloud.admit_write_timeline(root, job, config, completed=True)
            event["pressure"]["encoded_bytes"] = 100
            for invalid in (-1, True, 2**64):
                event["pressure"]["incoming_encoded_bytes"] = invalid
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "checkpoint pressure"):
                    cloud.admit_write_timeline(root, job, config, completed=True)

    def test_local_consumer_checks_timeline_receipt_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, _, path = self.fixture(root)
            result = root / "result"
            backend = {"name": "tidehunter_tcp"}
            binary = "a" * 128
            manifest = {"run": {"config": config, "binary": {"blake2b512": binary}},
                        "backend_info": backend}
            summary = {"backend": backend}
            (result / "manifest.json").write_text(json.dumps(manifest))
            (result / "summary.json").write_text(json.dumps(summary))
            (result / "metrics.prom").write_text("")
            receipt = {"job": job, "status": "completed", "binary_blake2b512": binary,
                       "normalized_config_sha256": hashlib.sha256((root / "normalized-config.json").read_bytes()).hexdigest(),
                       "artifacts_sha256": {name: hashlib.sha256((result / name).read_bytes()).hexdigest()
                                            for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}}
            receipt["write_timeline"] = cloud.admit_write_timeline(root, job, config, completed=True)
            cloud.analyze.verify_binding(root, config, receipt, manifest, summary)
            path.write_text(path.read_text() + " ")
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.analyze.verify_binding(root, config, receipt, manifest, summary)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                cloud.analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_partial_receipt_is_bound_and_tampering_or_missing_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            data.update(status="failed", available=False, error="deadline exceeded")
            data["timeline"].update(deadline_exceeded=True, owner_captured_connections=0,
                                    owner_storage_durations_available=False)
            path.write_text(json.dumps(data))
            binding = cloud.admit_write_timeline(root, job, config, completed=False)
            receipt = {"job": job, "status": "failed", "raw_artifacts_validated": False,
                       "write_timeline": binding}
            (root / "receipt.json").write_text(json.dumps(receipt))
            case = cloud.summarize_case(root)
            self.assertEqual(case["write_timeline"]["coverage"]["status"], "failed")
            self.assertFalse(case["write_timeline"]["coverage"]["available"])
            with self.assertRaises(ValueError):
                cloud.admit_write_timeline(root, job, config, completed=True)
            path.write_text(path.read_text() + " ")
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.summarize_case(root)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.summarize_case(root)

    def test_report_retains_failed_invalid_partial_and_rejects_completed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = root / "cases" / "trace"
            case.mkdir(parents=True)
            job, config, data, path = self.fixture(case)
            job.update(repeat=0, value_size=8, cohort="write_timeline_diagnostics")
            data["timeline"]["live_groups"] = [{
                "owner": 0, "reservation_token": None, "owner_validation_ns": None,
                "owner_persistence": {"unexpected": 1},
                "batches": [{"epoch": 1, "sequence": 0}]}]
            data["timeline"]["dropped_live_groups"] = 0
            path.write_text(json.dumps(data))
            receipt = {"job": job, "status": "failed", "error": "deadline exceeded",
                       "raw_artifacts_validated": False}
            (case / "receipt.json").write_text(json.dumps(receipt))
            (root / "matrix.json").write_text(json.dumps({"jobs": [job]}))
            row = cloud.report(root)["cases"][0]
            self.assertEqual(row["status"], "failed")
            self.assertIn("deadline exceeded", row["error"])
            self.assertIn("invalid live group storage persistence profile", row["error"])
            self.assertTrue((root / "comparison.csv").is_file())
            with self.assertRaisesRegex(ValueError, "invalid live group storage persistence profile"):
                cloud.admit_write_timeline(case, job, config, completed=True)
            receipt["status"] = "completed"
            (case / "receipt.json").write_text(json.dumps(receipt))
            with patch.object(cloud, "summarize_case", side_effect=ValueError(
                    "invalid live group storage persistence profile")):
                with self.assertRaisesRegex(ValueError, "invalid live group storage persistence profile"):
                    cloud.report(root)


class CloudMatrixTests(unittest.TestCase):
    def contract_spec(self):
        arms = {'native': ['value_wal_barrier', 'page_cache'],
                'tidehunter_nvme_rdma': ['synchronous', 'native'],
                'valkey_tcp': ['synced'],
                'valkey_rdma': ['synced', 'os_buffered', 'periodic_sync'],
                'kvrocks': ['synced', 'page_cache']}
        return self.spec(2) | {'mode': 'standard-with-live-nvmeof', 'backends': list(arms),
                               'max_cases': 2000, 'shapes': 'contract_core', 'arms': arms}

    def test_contract_core_profiles_and_arms(self):
        spec = self.contract_spec()
        jobs = cloud.make_matrix(spec)
        self.assertEqual(len(jobs), 130)
        self.assertEqual(len({job['case_id'] for job in jobs}), len(jobs))
        self.assertEqual({job['backend'] for job in jobs}, set(spec['backends']))
        self.assertEqual({job['profile'] for job in jobs},
                         {'write-concurrent', 'write-c1', 'write-sustained', 'read-concurrent', 'read-c1'})
        def arm(job):
            backend = job['backend']
            return (job['durability_tier'] if backend in cloud.TRANSPORTS else
                    job['native_durability'] if backend == 'native' else job.get('ack_contract', 'synced'))
        for job in jobs:
            self.assertEqual(job['cohort'], 'primary_rf1')
            self.assertEqual(job['distribution'], 'uniform')
            self.assertIsNone(job['offered_ops_per_second'])
            self.assertNotIn('diagnostics', job)
            self.assertNotIn('sealed_dataset', job)
            self.assertIn(job['value_size'], (1024, 65536))
            self.assertEqual(job['concurrency'], 1 if job['profile'].endswith('-c1') else 16)
            writing = job['profile'].startswith('write-')
            self.assertEqual(job['mix'], cloud.mix(**{'put' if writing else 'read': 100}))
            self.assertEqual(job['measurement_operations'], 200000 if job['profile'] == 'write-sustained'
                             else 4096 if writing else 8192)
            if job['profile'] == 'write-sustained':
                self.assertEqual((job['repeat'], job['value_size']), (0, 1024))
            backend = job['backend']
            if backend == 'native' or backend in cloud.TRANSPORTS:
                self.assertNotIn('ack_contract', job)
            else:
                self.assertEqual(cloud.ack_contract.selected(backend, job.get('ack_contract', 'synced')), arm(job))
                self.assertEqual('ack_contract' in job, arm(job) != 'synced')
            self.assertIn(arm(job), job['case_id'])
            self.assertIn(job['profile'], job['case_id'])
            self.assertEqual(cloud.active_owner_count(job), 5)
            cloud.make_run_config(job, '/tmp/contract-core', {}, 'test')
            if backend in cloud.TRANSPORTS:
                self.assertEqual(job['history_persistence'], 'wal_replay')
                self.assertEqual(job['read_lanes'], 2)
            if backend == 'tidehunter_nvme_rdma':
                self.assertEqual(job['nvmeof_route'], 'live')
                self.assertEqual(job['live_write_mode'], 'leased')
                self.assertTrue(job['live_lease_ahead'])
                self.assertFalse(job['live_polled_writes'])
                self.assertEqual(job['live_export_mode'], 'per_owner')
                self.assertEqual(job['owner_wal_geometry'], 'native_default')
        for backend, arms in spec['arms'].items():
            selected = [job for job in jobs if job['backend'] == backend]
            reads = [job for job in selected if job['mix']['read']]
            self.assertEqual(len(reads), 8)
            self.assertEqual({arm(job) for job in reads}, {arms[0]})
            puts = {}
            for job in selected:
                if job['mix']['put']:
                    puts.setdefault((job['profile'], job['value_size'], job['repeat']), []).append(arm(job))
            self.assertEqual(len(puts), 9)
            self.assertTrue(all(values == arms for values in puts.values()))
        self.assertEqual(controller.controller_matrix(spec), jobs)
        config = {'spec': spec, 'fleet': {'nodes': [{'address': f'10.0.0.{i+1}'} for i in range(5)]}}
        live = next(job for job in jobs if job['backend'] == 'tidehunter_nvme_rdma')
        self.assertEqual(len(controller.live_nvme_preparation(config, live['case_id'], 'rdma')['addresses']), 5)

    def test_contract_core_optional_keys_are_strict(self):
        spec = self.contract_spec()
        cases = []
        for value in (None, False, '', 'other', ['contract_core']):
            cases.append(spec | {'shapes': value})
        for value in (None, [], {}, {'native': ['page_cache']}):
            cases.append(spec | {'arms': value})
        for backend, values in (('native', []), ('native', ['page_cache', 'page_cache']),
                                ('native', ['unknown']), ('valkey_rdma', ['page_cache']),
                                ('valkey_tcp', ['os_buffered']), ('kvrocks', ['periodic_sync']),
                                ('native', 'page_cache'), ('native', [None])):
            cases.append(spec | {'arms': spec['arms'] | {backend: values}})
        cases.append({key: value for key, value in spec.items() if key != 'shapes'})
        cases.append(spec | {'arms': spec['arms'] | {'unknown': ['synced']}})
        for changed in cases:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                cloud.make_matrix(changed)
        self.assertEqual(len(cloud.make_matrix({key: value for key, value in spec.items() if key != 'arms'})), 85)

    def test_advertised_read_evidence_cannot_be_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            result.mkdir()
            manifest = result / "manifest.json"
            manifest.write_text(json.dumps({"run": {}}))
            self.assertIsNone(cloud.admit_measurement_read_evidence(directory))
            manifest.write_text(json.dumps({"run": {"measurement_read_evidence_schema_version": 1}}))
            with self.assertRaisesRegex(ValueError, "advertised measurement read evidence"):
                cloud.admit_measurement_read_evidence(directory)

    def spec(self, repeats):
        return {
            "schema_version": 1, "seed": 1775, "repeats": repeats,
            "value_sizes": [64, 256, 1024, 8192, 65536],
            "backends": list(cloud.BACKENDS),
            "keys": 4096, "max_cases": 924, "offered_rates": [1000, 10000, 50000],
            "offered_seconds": 8, "case_deadline_seconds": 180,
            "hard_deadline_seconds": 10800, "export_reserve_seconds": 600,
        }
    def test_nvme_diagnostic_identity_distinguishes_historical_and_sealed(self):
        job = {"backend": "tidehunter_nvme_rdma"}
        self.assertEqual(cloud.diagnostic_backend(job), "tidehunter_nvme_rdma")
        self.assertEqual(cloud.diagnostic_backend(job | {"nvmeof_route": "live"}),
                         "tidehunter_live_nvmeof")
        self.assertEqual(cloud.diagnostic_backend(job | {"nvmeof_route": "sealed_read"}),
                         "tidehunter_nvme_rdma_sealed_read")

    def test_live_measurement_requires_unmodified_boundary_counters_and_io(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        end = zero | {"completed_read_ops": 2, "completed_read_bytes": 8192,
                      "completed_write_ops": 1, "completed_write_bytes": 4096}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end},
                    "live_nvmeof": {"available": True, "counters": end.copy()}}
        job = {"mix": cloud.mix(read=50, put=50)}
        self.assertTrue(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["live_nvmeof"]["counters"]["completed_write_ops"] = 0
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["live_nvmeof"]["counters"]["completed_write_ops"] = 1
        evidence["end"]["live_nvmeof"]["unknown_writes"] = 1
        evidence["live_nvmeof"]["counters"]["unknown_writes"] = 1
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])

    def test_m3_live_measurement_does_not_require_retired_owner_executor_counters(self):
        zero = dict.fromkeys((*cloud.LIVE_NVME_COUNTERS, "grant_misses",
                              "controller_connect_ns", "controller_connect_samples",
                              "leased_frames", "leased_skip_markers", "fua_writes"), 0)
        end = zero | {"completed_write_ops": 1, "completed_write_bytes": 4096, "leased_frames": 1}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end},
                    "live_nvmeof": {"available": True, "counters": end.copy()}}
        job = {"selection": "nvmeof_m3_t2_2", "group": "G33W", "owner_count": 2,
               "backend": "tidehunter_nvme_rdma", "mix": cloud.mix(put=100)}
        self.assertTrue(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["end"]["live_nvmeof"]["fence_evidence_dropped"] = 1
        evidence["live_nvmeof"]["counters"]["fence_evidence_dropped"] = 1
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])

    def test_live_cache_hit_cohort_can_avoid_measurement_io_with_accounting(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero, "coordinator": {"hits": 10}},
                    "end": {"live_nvmeof": zero, "coordinator": {"hits": 110}},
                    "live_nvmeof": {"available": True, "counters": zero},
                    "deltas": {"coordinator": {"available": True, "counters": {"hits": 100}}}}
        job = {"mix": cloud.mix(read=100), "coordinator_cache_bytes": 1024,
               "measurement_operations": 100}
        self.assertTrue(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["deltas"]["coordinator"]["counters"]["hits"] = 99
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])

    def test_live_route_requires_actual_matching_kernel_namespace(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        end = zero | {"completed_write_ops": 1, "completed_write_bytes": 4096}
        host = [1] * 16
        expected = {"transport": "rdma", "addresses": ["10.0.0.1"], "service": 4420,
                    "host_id": host, "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:01010101-0101-0101-0101-010101010101"}
        route = {"transport": "rdma", "address": "10.0.0.1", "service": 4420,
                 "host_id": host, "host_nqn": expected["host_nqn"],
                 "namespace_identity": {"namespace_path": "/dev/nvme0n1",
                    "controller_instance": 0, "nqn": "nqn.2026-01.example:wal",
                    "uuid": [2] * 16, "nsid": 1, "bytes": 4096, "block_size": 4096}}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end | {"routes": [route]}},
                    "live_nvmeof": {"available": True, "counters": end}}
        job = {"backend": "tidehunter_nvme_rdma", "mix": cloud.mix(put=100)}
        self.assertTrue(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["transport"] = "tcp"
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["transport"] = "rdma"
        spdk_job = job | {"nvmeof_target": "spdk"}
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])
        route["target_implementation"] = "spdk"
        route["namespace_identity"]["target_implementation"] = "spdk"
        route["spdk"] = {"rpc_socket": "/run/spdk.sock", "image_path": "/wal/file",
                         "bdev_name": "wal0", "pid": 12, "process_start_ticks": 34}
        route["writer"] = {"file_identity": {"path": "/wal/file", "device": 8, "inode": 9,
                                               "bytes": 4096, "block_size": 4096}}
        self.assertTrue(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["writer"]["file_identity"]["inode"] = 0
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])

    def test_live_route_binds_database_and_partition_to_configured_seed(self):
        database = "aa" * 16
        host = {"host_id": [1] * 16, "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:01010101-0101-0101-0101-010101010101"}
        owner = dict(host, address="10.0.0.1", service=4420)
        owners = {"0": owner}
        for i in range(1, 5):
            host_id = [i + 1] * 16
            nqn = "nqn.2014-08.org.nvmexpress:uuid:" + str(cloud.uuid.UUID(bytes=bytes(host_id)))
            owners[str(i)] = dict(host_id=host_id, host_nqn=nqn, address=f"10.0.0.{i + 1}", service=4420 + 2 * i)
        expected = dict(host, transport="tcp", addresses=[r["address"] for r in owners.values()],
                        service=4420, owners=owners, identity_scope="database")
        effective = cloud.effective_live_owner(owner, database, 0)
        self.assertEqual(bytes(effective["host_id"]).hex(), "ef6d1c505470b25f2ee82873ff29ed23")
        route = {"transport": "tcp", **{key: effective[key] for key in ("host_id", "host_nqn", "address", "service")},
                 "namespace_identity": {"namespace_path": "/dev/nvme0n1", "controller_instance": 0,
                    "nqn": effective["subsystem_scope"] + "sub-00000000-0000-0000-0000-000000000001",
                    "uuid": [2] * 16, "nsid": 1, "bytes": 4096, "block_size": 4096}}
        routes = [route]
        for index, seed in list(owners.items())[1:]:
            effective = cloud.effective_live_owner(seed, database, int(index))
            routes.append({"transport": "tcp",
                **{key: effective[key] for key in ("host_id", "host_nqn", "address", "service")},
                "namespace_identity": dict(route["namespace_identity"],
                    nqn=effective["subsystem_scope"] + "sub-00000000-0000-0000-0000-000000000001")})
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        end = zero | {"completed_write_ops": 1, "completed_write_bytes": 4096}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end | {"routes": routes}},
                    "live_nvmeof": {"available": True, "counters": end}}
        job = {"backend": "tidehunter_nvme_tcp", "mix": cloud.mix(put=100)}
        backend = {"configuration": {"database": database}}
        def qualifies():
            return cloud.qualify_live_nvme_route(evidence, job, expected, backend)
        self.assertTrue(qualifies()["qualified"], qualifies()["reasons"])
        for wrong in (None, "0" * 32, "bb" * 16, "aa" * 15):
            with self.subTest(database=wrong):
                backend["configuration"]["database"] = wrong
                self.assertFalse(qualifies()["qualified"])
        backend["configuration"]["database"] = database
        nqn = route["namespace_identity"]["nqn"]
        route["namespace_identity"]["nqn"] = nqn.replace("-p00000000-", "-p00000001-")
        self.assertFalse(qualifies()["qualified"])
        route["namespace_identity"]["nqn"] = nqn
        route["host_id"] = host["host_id"]
        self.assertFalse(qualifies()["qualified"])

    def test_timeline_twins_have_matched_counts_and_no_legacy_profile(self):
        spec = self.spec(1) | {"mode": "standard-with-live-nvmeof",
                "backends": ["native", "tidehunter_nvme_tcp", "tidehunter_nvme_rdma"],
                "diagnostics": {"write_profile_limit": 256, "write_timeline_limit": 8192}}
        jobs = cloud.make_matrix(spec)
        timeline = [j for j in jobs if j["cohort"] == "write_timeline_diagnostics"]
        self.assertEqual(len(timeline), 2)
        for job in timeline:
            self.assertEqual(job["diagnostics"], {"write_profile_limit": 0, "write_timeline_limit": 8192})
            candidates = [j for j in jobs if j["backend"] == job["backend"]
                          and j["value_size"] == job["value_size"]
                          and j["mix"] == job["mix"] and j["concurrency"] == job["concurrency"]
                          and j["cohort"] in ("primary_rf1", "native_write_regression")
                          and j["profile"] in ("write-concurrent", "write-c16-baseline", "write-batch32-c16-baseline")]
            self.assertTrue(any(j["measurement_operations"] == job["measurement_operations"]
                                and j["warmup_operations"] == job["warmup_operations"]
                                and j["keys"] == job["keys"] for j in candidates))
            config = cloud.make_run_config(job, Path("/tmp/case"), {}, "source")
            self.assertEqual(config["diagnostics"], job["diagnostics"])
        self.assertTrue(all("diagnostics" not in j for j in jobs if j["cohort"] == "primary_rf1"))
        for value in (-1, 65537, True, "8192"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(spec | {"diagnostics": {"write_timeline_limit": value}})

    def test_portable_backends_preserve_standard_common_workloads(self):
        spec = self.spec(1) | {"mode": "standard-with-live-nvmeof"}
        selected = cloud.make_matrix(spec | {"backends": ["valkey_tcp", "valkey_rdma",
                                                         "tidehunter_nvme_tcp", "tidehunter_nvme_rdma"]})
        workload_fields = ("repeat", "profile", "cohort", "value_size", "keys", "mix",
                           "concurrency", "measurement_operations", "warmup_operations",
                           "offered_ops_per_second", "distribution")
        common = lambda backend: sorted(
            cloud.analyze.canonical({k: job[k] for k in workload_fields})
            for job in selected if job["backend"] == backend and job["cohort"] == "primary_rf1"
            and job["profile"] != "write-batch32")
        self.assertTrue(common("valkey_tcp"))
        self.assertEqual(common("valkey_tcp"), common("valkey_rdma"))
        self.assertEqual(common("valkey_tcp"), common("tidehunter_nvme_tcp"))
        self.assertEqual(common("valkey_tcp"), common("tidehunter_nvme_rdma"))


    def test_nvmeof_measurement_qualification_requires_complete_clean_interval(self):
        def evidence():
            def reader(count):
                return {"identity": {"transport": "tcp"}, "counters": {"available": True, "counters": {
                    "eligible_hits": count, "ineligible_fallbacks": 0,
                    "io_submissions": count, "io_completions": count,
                    "io_errors": 0, "io_timeouts": 0,
                }}}
            return {"nvmeof": {"available": True, "readers": {"0": reader(3), "1": reader(5)}},
                    "deltas": {"coordinator": {"available": True, "counters": {
                        "read_rpc_count": 0, "hits": 0}}}}
        targets = [{"target": {"exports": [{"target_receipt": {
            "target_cache": "buffered_file_page_cache", "target_cache_verified": True,
            "selected_transport": "tcp"}} for _ in range(2)]}}]
        def qualify(data, target_rows=targets):
            return cloud.qualify_nvme_measurement(data, 2, "tcp", target_rows)

        result = qualify(evidence())
        self.assertTrue(result["qualified"])
        self.assertEqual(result["reasons"], [])
        self.assertEqual(result["nvmeof_totals"], {
            "eligible_hits": 8, "ineligible_fallbacks": 0, "io_submissions": 8,
            "io_completions": 8, "io_errors": 0, "io_timeouts": 0})
        self.assertEqual(result["coordinator_read_rpc_count"], 0)
        self.assertEqual(result["coordinator_cache_hits"], 0)

        missing = evidence()
        del missing["nvmeof"]["readers"]["1"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["nvmeof_totals"])
        missing = evidence()
        del missing["nvmeof"]["readers"]["0"]["counters"]["counters"]["io_completions"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["nvmeof_totals"])
        missing = evidence()
        del missing["deltas"]["coordinator"]["counters"]["read_rpc_count"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["coordinator_read_rpc_count"])
        self.assertIsNone(result["coordinator_cache_hits"])

        for field, value in (("ineligible_fallbacks", 1), ("io_errors", 1),
                             ("io_timeouts", 1), ("io_completions", 2)):
            with self.subTest(field=field):
                invalid = evidence()
                invalid["nvmeof"]["readers"]["0"]["counters"]["counters"][field] = value
                self.assertFalse(qualify(invalid)["qualified"])
        for field in ("read_rpc_count", "hits"):
            with self.subTest(field=field):
                invalid = evidence()
                invalid["deltas"]["coordinator"]["counters"][field] = 1
                self.assertFalse(qualify(invalid)["qualified"])
        invalid = evidence()
        invalid["nvmeof"]["readers"]["0"]["identity"]["transport"] = None
        self.assertFalse(qualify(invalid)["qualified"])
        self.assertFalse(qualify(evidence(), [])["qualified"])
        self.assertFalse(qualify(evidence(), [{"target": {"exports": [
            {"target_receipt": {"target_cache": "buffered_file_page_cache",
                                "target_cache_verified": False, "selected_transport": "tcp"}}]}}])["qualified"])

    def test_one_repeat_preserves_every_first_repeat_job_and_run_configuration(self):
        single = cloud.make_matrix(self.spec(1))
        historical = cloud.make_matrix(self.spec(3))
        self.assertEqual((len(single), len(historical)), (190, 570))
        first = [job for job in historical if job["repeat"] == 0]
        for job, old_job in zip(single, first):
            self.assertEqual({k: v for k, v in job.items() if k != "case_id"},
                             {k: v for k, v in old_job.items() if k != "case_id"})
            if job["cohort"] != "native_async_write":
                self.assertEqual(job["case_id"], old_job["case_id"])

    def test_write_diagnostics_are_a_distinct_cohort_without_reducing_any_work(self):
        normal = cloud.make_matrix(self.spec(1))
        diagnostic = cloud.make_matrix(self.spec(1) | {"diagnostics": {"write_profile_limit": 256}})
        self.assertEqual(len(diagnostic), len(normal) + 1)
        def identity(job):
            return job["backend"], job["repeat"], job["profile"], job["cohort"], job["value_size"]
        retained = {identity(job): job for job in diagnostic if job["cohort"] != "write_stage_diagnostics"}
        self.assertEqual(len(retained), len(normal))
        for before in normal:
            after = retained[identity(before)]
            self.assertEqual({key: value for key, value in before.items() if key != "case_id"},
                             {key: value for key, value in after.items() if key != "case_id"})
        selected = [job for job in diagnostic if job["cohort"] == "write_stage_diagnostics"]
        self.assertEqual({job["backend"] for job in selected},
                         {"native"} | (set(self.spec(1)["backends"]) & set(cloud.TRANSPORTS)))
        for after in selected:
            before = retained[(*identity(after)[:3], "primary_rf1", after["value_size"])]
            self.assertEqual(after["diagnostics"]["write_profile_limit"], 256)
            for field in ("mix", "concurrency", "keys", "measurement_operations", "warmup_operations",
                          "offered_ops_per_second", "distribution", "coordinator_cache_bytes",
                          "native_durability", "drop_os_caches"):
                self.assertEqual(before[field], after[field])


    def test_historical_fullset_qualification_rejects_fallback_and_missing_observations(self):
        job = {"cohort": "registered_ram_fullset_control", "warmup_key_sweep": True,
               "resident_reads": {"require_device_read": True}}
        resident = {name: 0 for name in ("message_read_requests", "grant_acquisitions",
            "grant_candidates", "rma_errors", "fallback_missing", "fallback_oversize",
            "fallback_capacity", "fallback_unsupported", "fallback_stale")}
        resident.update(rma_posts=1, rma_completions=1)
        owners = {"wal_reads": 0, "async_read_submissions": 0, "cache_hits": 0}
        def group(counters):
            return {"available": True, "partitions": {
                str(index): {"available": True, "counters": dict(counters)} for index in range(5)}}
        evidence = {"measurement_drained": True, "deltas": {
            "coordinator": {"available": True, "counters": {"hits": 0}},
            "client_resident": group(resident), "owner_executor": group(owners)}}
        self.assertTrue(cloud.qualify_fullset_read_path(job, evidence)["qualified"])
        evidence["deltas"]["client_resident"]["partitions"]["2"]["counters"]["message_read_requests"] = 1
        proof = cloud.qualify_fullset_read_path(job, evidence)
        self.assertFalse(proof["qualified"])
        self.assertEqual(proof["observed"]["message_read_requests"], 1)
        evidence["deltas"]["client_resident"]["partitions"].pop("2")
        proof = cloud.qualify_fullset_read_path(job, evidence)
        self.assertFalse(proof["qualified"])
        self.assertIsNone(proof["observed"]["message_read_requests"])


    def test_manual_read_diagnostics_are_propagated_for_mixed_cases(self):
        spec = self.spec(1) | {"mode": "standard-with-live-nvmeof", "backends": ["tidehunter_nvme_tcp"]}
        job = next(job for job in cloud.make_matrix(spec) if job["profile"] == "point-mixed")
        job["diagnostics"] = {"read_profile_limit": 7, "write_profile_limit": 0}
        self.assertEqual(cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source")["diagnostics"],
                         job["diagnostics"])

    def test_invalid_read_diagnostic_limits_are_rejected(self):
        for value in (True, -1, 65537, 0.5, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(self.spec(1) | {"diagnostics": {"read_profile_limit": value}})

    def test_invalid_diagnostic_budget_is_rejected_before_launch(self):
        for value in (True, -1, 65537, {}, {"write_profile_limit": 16, "unknown": 1}):
            diagnostics = value if isinstance(value, dict) else {"write_profile_limit": value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(self.spec(1) | {"diagnostics": diagnostics})

    def test_full_profile_cohort_and_per_case_phase_contract(self):
        profiles = {
            "point-mixed": ({"read": 45, "put": 45, "delete": 5, "missing_read": 5}, 1, 8192),
            "read-concurrent": ({"read": 100}, 16, 8192),
            "write-concurrent": ({"put": 100}, 16, 4096),
            "read-many32": ({"get_many": 100}, 16, 2048),
        }
        spec = self.spec(1)
        jobs = cloud.make_matrix(spec)
        identities = {(job["backend"], job["cohort"], job["profile"], job["value_size"]) for job in jobs}
        expected = set()
        for backend in spec["backends"]:
            expected.update((backend, "primary_rf1", profile, size)
                            for profile in profiles for size in spec["value_sizes"])
            expected.update((backend, "primary_rf1", f"offered-{rate}", 1024)
                            for rate in spec["offered_rates"])
            expected.add((backend, "primary_rf1", "zipf-read", 1024))
            expected.add((backend, "cold_os_cache", "cold-os-large", 65536))
            if backend in ("tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct", "tidehunter_efa_native"):
                expected.update((backend, "coordinator_cache_configured", "coordinator-cache", size)
                                for size in (256, 8192))
            if backend == "native":
                expected.update((backend, "native_pagecache", profile, size)
                                for profile in ("point-mixed", "read-many32") for size in spec["value_sizes"])
                expected.update((backend, "native_async_write", "write-concurrent", size)
                                for size in spec["value_sizes"])
        self.assertEqual(identities, expected)
        self.assertEqual(len(jobs), len(expected))
        for job in jobs:
            with self.subTest(case=job["case_id"]):
                profile, cohort = job["profile"], job["cohort"]
                if profile in profiles:
                    operation_mix, concurrency, operations = profiles[profile]
                elif job["offered_ops_per_second"] is not None:
                    operation_mix, concurrency = {"read": 70, "put": 30}, 32
                    operations = job["offered_ops_per_second"] * 8
                else:
                    operation_mix = {"read": 100}
                    concurrency = 16 if profile in ("zipf-read", "cold-os-large") else 1
                    operations = 4096 if profile == "cold-os-large" else 8192
                self.assertEqual({key: value for key, value in job["mix"].items() if value}, operation_mix)
                self.assertEqual((job["concurrency"], job["measurement_operations"]), (concurrency, operations))
                warmup = 0 if cohort == "cold_os_cache" else 1024 if cohort.endswith("_configured") else 512
                self.assertEqual(job["warmup_operations"], warmup)
                self.assertEqual(job["keys"], 131072 if cohort == "cold_os_cache" else 4096)
                self.assertEqual(job["drop_os_caches"], cohort == "cold_os_cache")
                self.assertEqual(job["coordinator_cache_bytes"], 128 * 1024**2 if cohort == "coordinator_cache_configured" else 0)
                self.assertNotIn("owner_read_cache_bytes", job)
                self.assertEqual(job["native_durability"], "page_cache" if cohort in ("native_pagecache", "native_async_write") else "value_wal_barrier")
                self.assertEqual(job["distribution"], {"zipf": {"exponent": 0.99}} if profile == "zipf-read" else "uniform")
                self.assertNotIn("resident_reads", job)
                config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source", 180)
                if job["backend"] == "native":
                    options = config["backend"]["options"]
                    self.assertEqual(options["native_wal_geometry"], "native_default")
                    self.assertEqual(options["durability"], job["native_durability"])
                self.assertEqual(config["phases"], {"preload": True, "warmup_operations": warmup,
                    "aging_operations": 0, "measurement_operations": operations})
                self.assertFalse(config["qualification_only"])
                self.assertEqual(config["workload"]["batch_size"], 32)
                self.assertEqual(config["limits"]["max_operations"], job["keys"] + warmup + operations + 4096)
                self.assertEqual(config["limits"]["deadline_seconds"], 600 if cohort == "cold_os_cache" else 180)

    def test_native_async_reference_matches_primary_work_but_not_acknowledgement(self):
        jobs = cloud.make_matrix(self.spec(1))
        references = [job for job in jobs if job["cohort"] == "native_async_write"]
        self.assertEqual({job["value_size"] for job in references}, set(self.spec(1)["value_sizes"]))
        for reference in references:
            primary = next(job for job in jobs if job["backend"] == "native"
                           and job["cohort"] == "primary_rf1"
                           and job["profile"] == reference["profile"]
                           and job["value_size"] == reference["value_size"])
            for field in ("mix", "concurrency", "keys", "measurement_operations",
                          "warmup_operations", "distribution", "native_wal_geometry"):
                self.assertEqual(reference[field], primary[field])
            durable = cloud.make_run_config(primary, Path("/tmp/owned-case"), {}, "source")
            asynchronous = cloud.make_run_config(reference, Path("/tmp/owned-case"), {}, "source")
            self.assertEqual(durable["backend"]["options"]["durability"], "value_wal_barrier")
            self.assertEqual(asynchronous["backend"]["options"]["durability"], "page_cache")
            self.assertNotEqual(durable["namespace"], asynchronous["namespace"])
            with self.assertRaisesRegex(ValueError, "durability differs"):
                cloud.make_run_config(primary, Path("/tmp/owned-case"), {"durability": "page_cache"}, "source")
            with self.assertRaisesRegex(ValueError, "native_wal_geometry differs"):
                cloud.make_run_config(primary, Path("/tmp/owned-case"), {"native_wal_geometry": "small"}, "source")
            legacy = cloud.make_run_config(primary | {"native_wal_geometry": "small"},
                                           Path("/tmp/owned-case"), {}, "source")
            self.assertEqual(legacy["backend"]["options"]["native_wal_geometry"], "small")
            self.assertEqual(legacy["backend"]["options"]["durability"], "value_wal_barrier")

    def test_exact_case_bound_admits_all_jobs_but_never_truncates(self):
        for repeats, count in ((1, 190), (3, 570)):
            with self.subTest(repeats=repeats):
                spec = self.spec(repeats) | {"max_cases": count}
                self.assertEqual(len(cloud.make_matrix(spec)), count)
                with self.assertRaisesRegex(ValueError, "exceeding explicit max_cases"):
                    cloud.make_matrix(spec | {"max_cases": count - 1})

    def test_selection_accepts_supported_subsets_and_keeps_complete_selected_cases(self):
        full = self.spec(1)
        full_jobs = cloud.make_matrix(full)
        subset = full | {"backends": ["valkey_tcp"], "max_cases": 25}
        selected = cloud.make_matrix(subset)
        self.assertEqual(len(selected), 25)
        self.assertEqual({job["backend"] for job in selected}, {"valkey_tcp"})
        valkey = [job for job in full_jobs if job['backend'] == 'valkey_tcp']
        common = [job for job in full_jobs if job['backend'] == 'tikv']
        ignored = {'backend', 'case_id'}
        self.assertEqual(
            sorted((cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in valkey)),
            sorted((cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in common)))
        self.assertEqual(len(valkey), 25)
        for name in ("garnet", "kvrocks"):
            jobs = [job for job in full_jobs if job["backend"] == name]
            self.assertEqual(len(jobs), 25)
            self.assertEqual(
                sorted(cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in jobs),
                sorted(cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in valkey))
        expected = [job for job in full_jobs if job["backend"] == "valkey_tcp"]
        self.assertEqual(
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in selected],
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in expected])
        for invalid in ([], ["native", "native"], ["not-a-backend"]):
            with self.subTest(backends=invalid), self.assertRaises(ValueError):
                cloud.make_matrix(full | {"backends": invalid})
        self.assertEqual(len(cloud.make_matrix(self.spec(3) | {
            'diagnostics': {'write_profile_limit': 256}})), 573)

    def test_historical_frozen_jobs_report_without_current_completeness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = [job for job in cloud.make_matrix(self.spec(1))
                    if job["backend"] != "kvrocks"]
            jobs.append({**jobs[0], "case_id": "historical-registered-value",
                         "cohort": "registered_ram_fullset_control"})
            frozen = {"schema_version": 1, "spec": self.spec(1) | {
                "backends": [b for b in cloud.BACKENDS if b != "kvrocks"]}, "jobs": jobs}
            original = json.dumps(frozen)
            (root / "matrix.json").write_text(original)
            report = cloud.report(root)
            self.assertEqual(report["planned_cases"], len(jobs))
            self.assertEqual(report["planned_backends"], sorted(set(cloud.BACKENDS) - {"kvrocks"}))
            self.assertFalse(report["current_backend_coverage"])
            self.assertEqual((root / "matrix.json").read_text(), original)
            self.assertEqual([row["job"] for row in report["cases"]], jobs)


class CloudReports(unittest.TestCase):
    def failure_fixture(self, root):
        spec = json.loads(Path(cloud.__file__).with_name("cloud-campaign.json").read_text())
        job = dict(cloud.make_matrix(spec)[0], backend="native", keys=4, warmup_operations=0,
                   measurement_operations=4, value_size=256)
        directory = root / "cases" / job["case_id"]
        result = directory / "result"
        result.mkdir(parents=True)
        config = cloud.make_run_config(job, directory, {"path": str(directory / "database")}, None)
        config["phases"]["preload"] = False
        backend = {"name": "native", "acknowledgement": "fixture", "replication_factor": 1}
        binary = "a" * 128
        (directory / "config.json").write_text(json.dumps(config))
        (directory / "normalized-config.json").write_text(json.dumps(config))
        (result / "manifest.json").write_text(json.dumps({"status": "failed", "backend_info": backend,
            "run": {"config": config, "binary": {"blake2b512": binary}}}))
        (result / "summary.json").write_text(json.dumps({"status": "failed", "backend": backend}))
        (result / "metrics.prom").write_text("# unit-test fixture\n")
        fields = ["phase", "operation", "outcome", "scheduled_ns", "dispatched_ns", "dispatch_observed",
                  "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes", "error_kind"]
        with (result / "samples.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for outcome, scheduled, dispatched, completed, size, error in (
                ("ok", 100, 110, 150, 256, ""),
                ("timeout", 200, 220, 1220, 0, "timeout"),
                ("ok", 300, 320, 380, 256, "")):
                writer.writerow({"phase": "measurement", "operation": "get", "outcome": outcome,
                    "scheduled_ns": scheduled, "dispatched_ns": dispatched, "dispatch_observed": "true",
                    "completed_ns": completed, "api_ns": completed - dispatched,
                    "response_ns": completed - scheduled, "scheduling_ns": dispatched - scheduled,
                    "logical_keys": 1, "bytes": size, "error_kind": error})
        receipt = {"job": job, "status": "failed", "raw_artifacts_validated": True,
            "binary_blake2b512": binary, "error": "one timed-out call; fourth arrival not issued",
            "artifacts_sha256": {name: hashlib.sha256((result / name).read_bytes()).hexdigest()
                for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}}
        (directory / "receipt.json").write_text(json.dumps(receipt))
        (root / "matrix.json").write_text(json.dumps({"jobs": [job]}))
        return directory, receipt

    def test_owner_evidence_is_integrity_bound_even_for_failed_cases(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["raw_artifacts_validated"] = False
            owner = directory / "owner-durability" / "node-0.durability.json"
            owner.parent.mkdir()
            owner.write_text('{"partition": 0}')
            facts = directory / "owner-provider-facts.json"
            facts.write_text('{"owners": []}')
            receipt["owner_durability_receipts"] = [{"path": str(owner.relative_to(directory)),
                "sha256": hashlib.sha256(owner.read_bytes()).hexdigest()}]
            receipt["owner_provider_facts"] = {"path": facts.name,
                "sha256": hashlib.sha256(facts.read_bytes()).hexdigest()}
            (directory / "receipt.json").write_text(json.dumps(receipt))
            result = cloud.summarize_case(directory)
            self.assertEqual(result["owner_durability_receipts"], [{"partition": 0}])
            self.assertEqual(result["owner_provider_facts"], {"owners": []})
            facts.write_text('{"owners": [1]}')
            with self.assertRaisesRegex(ValueError, "provider facts differ"):
                cloud.summarize_case(directory)
            facts.write_text('{"owners": []}')
            owner.write_text('{"partition": 1}')
            with self.assertRaisesRegex(ValueError, "durability receipt differs"):
                cloud.summarize_case(directory)

    def test_native_report_rejects_self_consistent_wrong_planned_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["job"].update(cohort="primary_rf1", native_durability="value_wal_barrier",
                                  native_wal_geometry="native_default")
            receipt_path = directory / "receipt.json"
            receipt_path.write_text(json.dumps(receipt))
            cloud.summarize_case(directory)
            for option, wrong in (("durability", "page_cache"), ("native_wal_geometry", "small")):
                with self.subTest(option=option):
                    config_path = directory / "config.json"
                    original = json.loads(config_path.read_text())
                    config = copy.deepcopy(original)
                    config["backend"]["options"][option] = wrong
                    config_path.write_text(json.dumps(config))
                    normalized_path = directory / "normalized-config.json"
                    normalized_path.write_text(json.dumps(config))
                    manifest_path = directory / "result/manifest.json"
                    manifest = json.loads(manifest_path.read_text())
                    manifest["run"]["config"] = config
                    manifest_path.write_text(json.dumps(manifest))
                    receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                    receipt_path.write_text(json.dumps(receipt))
                    with self.assertRaisesRegex(ValueError, f"native {option} differs from the planned case"):
                        cloud.summarize_case(directory)
                    config_path.write_text(json.dumps(original))
                    normalized_path.write_text(json.dumps(original))
                    manifest["run"]["config"] = original
                    manifest_path.write_text(json.dumps(manifest))
                    receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                    receipt_path.write_text(json.dumps(receipt))

    def test_nvme_report_requires_bound_target_and_measurement_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["job"]["backend"] = "tidehunter_nvme_tcp"
            config_path = directory / "config.json"
            config = json.loads(config_path.read_text())
            config["backend"]["options"]["nvmeof_readers"] = [{}]
            config_path.write_text(json.dumps(config))
            (directory / "normalized-config.json").write_text(json.dumps(config))
            manifest_path = directory / "result/manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["run"]["config"] = config
            manifest_path.write_text(json.dumps(manifest))
            receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            receipt_path = directory / "receipt.json"

            # A trusted failed partial remains readable, but cannot acquire a proof.
            receipt_path.write_text(json.dumps(receipt))
            partial = cloud.summarize_case(directory)["nvmeof_measurement_qualification"]
            self.assertFalse(partial["qualified"])
            self.assertIn("measurement qualification receipt unavailable", partial["reasons"])

            receipt["status"] = "completed"
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)
            receipt["nvmeof_measurement_qualification"] = {"qualified": True}
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)
            del receipt["nvmeof_measurement_qualification"]
            target_path = directory / "nvme-targets.json"
            target_path.write_text(json.dumps([{"target": {"exports": [{"target_receipt": {
                "target_cache": "buffered_file_page_cache", "target_cache_verified": True,
                "selected_transport": "tcp"}}]}}]))
            receipt["nvmeof_target_evidence"] = {"path": "nvme-targets.json",
                "sha256": hashlib.sha256(target_path.read_bytes()).hexdigest()}
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)

            receipt["nvmeof_measurement_qualification"] = {"qualified": True}
            receipt["nvmeof_target_evidence"]["sha256"] = "0" * 64
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "target evidence differs from its integrity binding"):
                cloud.summarize_case(directory)

    def test_resource_interval_must_match_both_observed_boundary_counters(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            before, after = json.loads(
                Path(__file__).with_name("resource-client-boundaries-0024.json").read_text())
            resources = cloud.resource_accounting.interval([before], [after])
            receipt["measurement_evidence"] = {}
            for field, name, frame in (("before", "measurement-before.json", before),
                                      ("end", "measurement-end.json", after)):
                path = directory / name
                path.write_text(json.dumps({"hosts": [{"resource_accounting": frame}]}))
                receipt["measurement_evidence"][field] = name
                receipt["measurement_evidence"][field + "_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            path = directory / "resource-interval.json"
            path.write_text(json.dumps(resources))
            receipt["resource_evidence"] = {"path": path.name, "observed": resources,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                **cloud.resource_accounting.coverage(resources)}
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)["resource_evidence"]["observed"]["processes"][0]
            self.assertFalse(row["available"])
            self.assertTrue(row["core_available"])
            self.assertEqual(row["cpu_seconds"], .670104)
            self.assertEqual(row["memory_current_bytes"], 19963904)
            self.assertNotIn("io_read_bytes", row)
            receipt["resource_evidence"]["io_complete"] = True
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "resource coverage differs"):
                cloud.summarize_case(directory)
            receipt["resource_evidence"]["io_complete"] = False
            resources["processes"][0]["cpu_seconds"] = 5
            path.write_text(json.dumps(resources))
            receipt["resource_evidence"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "cannot be reproduced"):
                cloud.summarize_case(directory)

    def test_completed_requested_profile_requires_real_measurement_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            job = {"backend": "native", "diagnostics": {"write_profile_limit": 16}}
            with self.assertRaisesRegex(ValueError, "artifact is missing"):
                cloud.admit_write_diagnostics(directory, job, completed=True)
            profile = {"schema_version": 1, "backend": "native", "phase": "measurement",
                "selection": {"requested_limit": 16, "observed_samples": 0},
                "phase_attempted": True, "runtime_enabled": False, "profile": {}, "error": None}
            path = directory / "result/write-diagnostics.json"
            path.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ValueError, "real measurement samples"):
                cloud.admit_write_diagnostics(directory, job, completed=True)
            profile["phase"] = "warmup"
            path.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ValueError, "phase"):
                cloud.admit_write_diagnostics(directory, job, completed=False)

    def test_live_nvme_write_diagnostics_require_completed_consistent_io(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            path = directory / "result/write-diagnostics.json"
            job = {"backend": "tidehunter_nvme_rdma", "nvmeof_route": "live",
                   "diagnostics": {"write_profile_limit": 2}}
            fields = json.loads((Path(__file__).parent / "observability" /
                                 "live_nvmeof_write_profile_fields.json").read_text())
            def evidence():
                return {"schema_version": 1, "backend": "tidehunter_live_nvmeof",
                        "phase": "measurement", "phase_attempted": True, "measurement_drained": True,
                        "runtime_enabled": True, "runtime_observation_mode": "live_nvmeof_phase_counters",
                        "selection": {"requested_limit": 2, "observed_samples": 3,
                                      "selection_policy": "measurement window", "phase_exclusions": ["warmup"]},
                        "clock_scope": "client-side", "overlap_semantics": "independent overlapping clocks",
                        "unsupported_stages": [], "error": None,
                        "profile": {"runtime_observation_mode": "live_nvmeof_phase_counters",
                                    "scope": "measurement_nvme_io_cumulative_delta", "requested_limit": 2,
                                    "selected_operations": 3,
                                    "finalization_error": None, "stages": {"live_nvmeof":
                                        dict.fromkeys(fields, 0) | {
                                            "completed_write_ops": 3, "completed_write_bytes": 12288,
                                            "queue_ns": 600, "queue_samples": 3,
                                            "lease_reserve_ns": 600, "lease_namespace_sync_ns": 800,
                                            "lease_publish_ns": 1000, "lease_profile_samples": 2,
                                            "adopt_owner_ns": 900, "adopt_close_scan_ns": 600,
                                            "adopt_profile_samples": 3, "adopt_close_scan_bytes": 4096}}}}
            value = evidence()
            path.write_text(json.dumps(value))
            admitted = cloud.admit_write_diagnostics(directory, job, completed=True)
            self.assertEqual(admitted["observed"], value)
            row = {"case_id": "case", "write_diagnostics": admitted}
            clocks = {clock["stage"]: clock for clock in cloud.write_diagnostic_summary(row)}
            self.assertEqual(clocks["queue"]["mean_ns"], 200)
            self.assertEqual(clocks["queue"]["scope"], "measurement_nvme_io_cumulative_delta")
            self.assertIsNone(clocks["cq"]["mean_ns"])
            self.assertEqual(clocks["lease_reserve"]["mean_ns"], 300)
            self.assertEqual(clocks["lease_namespace_sync"]["mean_ns"], 400)
            self.assertEqual(clocks["lease_publish"]["mean_ns"], 500)
            self.assertEqual(clocks["adopt_owner"]["mean_ns"], 300)
            self.assertEqual(clocks["adopt_close_scan"]["mean_ns"], 200)
            self.assertIn("measurement_nvme_io_cumulative_delta", "\n".join(cloud.write_diagnostic_report([row])))
            bad = []
            value = evidence()
            value["runtime_enabled"] = False
            value["selection"]["observed_samples"] = 0
            value["profile"]["selected_operations"] = 0
            value["profile"]["stages"]["live_nvmeof"]["completed_write_ops"] = 0
            bad.append(value)
            value = evidence()
            value["profile"] = None
            bad.append(value)
            value = evidence()
            value["profile"]["selected_operations"] = 4
            bad.append(value)
            value = evidence()
            value["profile"]["stages"]["live_nvmeof"]["completed_write_ops"] = 4
            bad.append(value)
            value = evidence()
            value["profile"]["stages"]["live_nvmeof"]["errors"] = 1
            bad.append(value)
            value = evidence()
            value["error"] = "counter reset"
            bad.append(value)
            value = evidence()
            value["runtime_observation_mode"] = "dynamically_armed_remote_append_connection_profiles"
            bad.append(value)
            value = evidence()
            value["selection"]["requested_limit"] = True
            bad.append(value)
            value = evidence()
            value["phase"] = "warmup"
            bad.append(value)
            value = evidence()
            value["backend"] = "tidehunter_tcp"
            bad.append(value)
            value = evidence()
            del value["measurement_drained"]
            bad.append(value)
            for value in bad:
                with self.subTest(value=value):
                    path.write_text(json.dumps(value))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_diagnostics(directory, job, completed=True)

    def test_write_stage_means_keep_distinct_populations(self):
        evidence = {"phase_attempted": True, "runtime_enabled": True, "error": None,
                    "selection": {"observed_samples": 99}, "profile": {"stages": {
                        "write_profile_client_exchange_ns": 1000,
                        "write_profile_client_requests": 2,
                        "write_profile_storage_persist_wal_file_ns": 900,
                        "write_profile_storage_file_persist_calls": 3},
                        "coordinator_interval": {"stages": {
                            "write_storage_wait_ns": 800}}}}
        def summarize():
            return {item["stage"]: item for item in cloud.write_diagnostic_summary(
                {"write_diagnostics": {"observed": evidence}})}
        clocks = summarize()
        self.assertEqual(clocks["client_exchange"]["mean_ns"], 500)
        self.assertEqual(clocks["storage_persist_wal_file"]["mean_ns"], 300)
        # The interval clock keeps its own sum and never invents a call mean.
        self.assertEqual(clocks["write_completion_wait"]["sum_ns"], 800)
        self.assertIsNone(clocks["write_completion_wait"]["mean_ns"])
        # A connection finalization error does not invalidate the independent boundary.
        evidence["error"] = "connection profile capture failed"
        clocks = summarize()
        self.assertIsNone(clocks["client_exchange"]["mean_ns"])
        self.assertTrue(clocks["write_completion_wait"]["available"])

    def test_write_summary_uses_pre_gate_boundary_without_relabeling_fallback(self):
        evidence = {"phase_attempted": True, "profile": {"coordinator_interval": {"stages": {
            "write_storage_wait_ns": 900}}},
            "pre_completion_gate": {"available": True, "stages": {
                "write_storage_wait_ns": 200}}}
        row = {"write_diagnostics": {"observed": evidence}}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["sum_ns"], 200)
        self.assertEqual(clock["scope"], "coordinator_arm_through_pre_completion_gate")
        evidence["pre_completion_gate"] = {"available": False, "reason": "missing"}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["sum_ns"], 900)
        self.assertEqual(clock["scope"], "coordinator_arm_through_post_gate")

    def test_incomplete_measurement_does_not_publish_boundary_clocks(self):
        evidence = {"phase_attempted": True, "measurement_drained": False,
                    "pre_completion_gate": {"available": True, "stages": {
                        "write_storage_wait_ns": 200}}}
        clock = cloud.write_diagnostic_summary({"write_diagnostics": {"observed": evidence}})[0]
        self.assertFalse(clock["available"])
        self.assertIsNone(clock["mean_ns"])
        del evidence["measurement_drained"]
        clock = cloud.write_diagnostic_summary({"write_diagnostics": {"observed": evidence}})[0]
        self.assertTrue(clock["available"])
        self.assertEqual(clock["sum_ns"], 200)

    def test_write_completion_wait_has_no_invented_call_mean(self):
        row = {"write_diagnostics": {"observed": {
            "phase_attempted": True, "pre_completion_gate": {"available": True,
                "stages": {"write_storage_wait_ns": 1234}}}}}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["sum_ns"], 1234)
        self.assertIsNone(clock["count"])
        self.assertIsNone(clock["mean_ns"])
        del row["write_diagnostics"]["observed"]["pre_completion_gate"]["stages"]["write_storage_wait_ns"]
        self.assertEqual(cloud.write_diagnostic_summary(row), [])

    def test_write_stage_missing_and_unavailable_is_not_zero(self):
        for total, count in ((None, 2), (0, None), (0, 0), (-1, 2), (1, True)):
            with self.subTest(total=total, count=count):
                row = {"write_diagnostics": {"observed": {
                    "phase_attempted": True, "runtime_enabled": True, "error": None,
                    "selection": {"observed_samples": 2},
                    "profile": {"stages": {"write_profile_client_exchange_ns": total,
                        "write_profile_client_requests": count}}}}}
                result = cloud.write_diagnostic_summary(row)[0]
                self.assertFalse(result["available"])
                self.assertIsNone(result["mean_ns"])

    def test_read_diagnostics_admission_checks_runtime_and_integrity(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            job = {"backend": "tidehunter_tcp", "diagnostics": {"read_profile_limit": 16}}
            with self.assertRaisesRegex(ValueError, "artifact is missing"):
                cloud.admit_read_diagnostics(directory, job, completed=True)
            profile = {"schema_version": 1, "backend": "tidehunter_tcp", "phase": "measurement",
                       "phase_attempted": True, "runtime_enabled": True,
                       "runtime_observation_mode": "sampled_prefix",
                       "selection": {"requested_limit": 16, "selected_operations": 1,
                                     "completed_operations": 1, "selection_policy": "first",
                                     "phase_exclusions": ["warmup"]},
                       "clock_scope": "process monotonic", "overlap_semantics": "nested",
                       "unsupported_stages": [], "profile": {"stages": {
                           "server_async": {"available": True, "sum_ns": 10, "count": 2, "max_ns": 8,
                                            "unavailable_reason": None},
                           "server_nowait": {"available": False, "sum_ns": None, "count": 0,
                                             "max_ns": None, "unavailable_reason": "no samples"}},
                           "counters": {}, "owners": {"3": {"stages": {
                               "server_async": {"available": True, "sum_ns": 8, "count": 1,
                                                "max_ns": 8, "unavailable_reason": None}},
                               "counters": {}}}}, "error": None}
            path = directory / "result/read-diagnostics.json"
            path.write_text(json.dumps(profile))
            binding = cloud.admit_read_diagnostics(directory, job, completed=True)
            self.assertEqual(binding["observed"], profile)
            self.assertEqual(binding["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            text = "\n".join(cloud.read_diagnostic_report([{"case_id": "case", "job": {
                "cohort": "read_stage_diagnostics", "diagnostic_source_cohort": "owner_cache_configured"},
                "read_diagnostics": binding}]))
            self.assertIn("| case | owner_cache_configured | owner | aggregate | server_async | 2 | 10 | 5.000 | 8 |", text)
            self.assertIn("| case | owner_cache_configured | owner | aggregate | server_nowait | — | — | — | — |", text)
            self.assertIn("| case | owner_cache_configured | owner | 3 | server_async | 1 | 8 | 8.000 | 8 |", text)
            self.assertNotIn("p99", text)
            for change in ({"runtime_enabled": False}, {"error": "failed"}, {"phase": "warmup"}):
                path.write_text(json.dumps(profile | change))
                with self.assertRaises(ValueError):
                    cloud.admit_read_diagnostics(directory, job, completed=True)
            invalid = profile | {"selection": profile["selection"] | {"completed_operations": 2}}
            path.write_text(json.dumps(invalid))
            with self.assertRaises(ValueError):
                cloud.admit_read_diagnostics(directory, job, completed=True)

    def test_failed_partial_cell_preserves_timeout_denominator_and_observed_quantiles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.failure_fixture(root)
            value = cloud.report(root)
            self.assertFalse(value["complete"])
            self.assertEqual(value["planned_repeats"], [0])
            self.assertEqual(value["planned_independent_repetitions"], 1)
            self.assertEqual(value["repetition_mode"], "single")
            row = value["cases"][0]
            self.assertEqual(row["status"], "failed")
            metrics = row["metrics"]
            self.assertEqual((metrics["offered_calls"], metrics["success_calls"], metrics["unsuccessful_calls"]), (3, 2, 1))
            self.assertEqual(metrics["success_fraction"], 2 / 3)
            self.assertEqual(metrics["planned_measurement_calls"], 4)
            self.assertFalse(metrics["measurement_rows_complete"])
            populations = {item["outcome"]: item for item in metrics["populations"]}
            self.assertEqual(populations["ok"]["api_ns"]["p50"], 40)
            self.assertEqual(populations["timeout"]["count"], 1)
            self.assertEqual(populations["ok"]["api_mean_ns"], 50)
            self.assertEqual(populations["ok"]["api_stddev_ns"], 10)
            self.assertEqual(populations["ok"]["response_mean_ns"], 65)
            self.assertEqual(populations["ok"]["response_stddev_ns"], 15)
            self.assertEqual(populations["ok"]["scheduling_mean_ns"], 15)
            self.assertEqual(populations["ok"]["scheduling_stddev_ns"], 5)
            with (root / "comparison.csv").open() as stream:
                output = {item["outcome"]: item for item in csv.DictReader(stream)}
            self.assertEqual(output["timeout"]["status"], "failed")
            self.assertEqual(output["timeout"]["unsuccessful_calls"], "1")
            self.assertEqual(output["ok"]["response_mean_us"], "0.065")
            self.assertEqual(output["ok"]["response_stddev_us"], "0.015")
            self.assertEqual(output["ok"]["scheduling_mean_us"], "0.015")
            self.assertEqual(output["ok"]["scheduling_stddev_us"], "0.005")
            self.assertEqual(output["ok"]["planned_independent_repetitions"], "1")
            self.assertEqual(output["ok"]["repetition_mode"], "single")
            self.assertEqual(output["ok"]["timing_stddev_scope"], "within_case_population")

    def test_csv_retains_actual_acknowledgement_roles_and_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, _ = self.failure_fixture(root)
            row = cloud.summarize_case(directory)
            row["backend"] = {"name": "valkey_tcp", "acknowledgement": "observed AOF always",
                "consistency": "primary TCP", "replication_factor": 1,
                "measurement_boundary": "application API; memory-resident GET",
                "get_many_strategy": "ordered bounded per-key GET", "batch_atomicity": "unsupported",
                "topology": {"primary_count": 5, "replica_count": 0},
                "configuration": {"appendfsync": "always", "maxmemory-policy": "noeviction"}}
            row["resource_evidence"] = {"observed": {"processes": [{"process_role": "valkey0", "cpu_seconds": 1.25}]}}
            cloud.write_csv_report(root, [row])
            with (root / "comparison.csv").open() as stream:
                exported = list(csv.DictReader(stream))
            for item in exported:
                self.assertEqual(item["acknowledgement"], row["backend"]["acknowledgement"])
                self.assertEqual(item["batch_atomicity"], "unsupported")
                self.assertEqual(json.loads(item["observed_topology"]), row["backend"]["topology"])
                self.assertEqual(json.loads(item["effective_configuration"]), row["backend"]["configuration"])
                self.assertEqual(json.loads(item["resource_evidence"]), row["resource_evidence"])

    def test_csv_exports_selected_external_ack_contract_without_changing_jobs(self):
        for backend in ("valkey_tcp", "valkey_rdma", "kvrocks"):
            contracts = ("synced", "periodic_sync", "os_buffered") if backend != "kvrocks" else ("synced", "page_cache")
            for contract in contracts:
                with self.subTest(backend=backend, contract=contract), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    directory, _ = self.failure_fixture(root)
                    row = cloud.summarize_case(directory)
                    row["job"]["backend"] = backend
                    if contract != "synced":
                        row["job"]["ack_contract"] = contract
                    row["backend"] = {"acknowledgement": "observed strict boundary"}
                    if contract != "synced":
                        row["backend"]["configuration"] = {"ack_contract": contract}
                    original = copy.deepcopy(row["job"])
                    cloud.write_csv_report(root, [row])
                    with (root / "comparison.csv").open() as stream:
                        exported = list(csv.DictReader(stream))
                    self.assertTrue(exported)
                    for item in exported:
                        self.assertEqual(item["ack_contract"], contract)
                        self.assertEqual(item["acknowledgement"], row["backend"]["acknowledgement"])
                    self.assertEqual(row["job"], original)

    def test_run_config_binds_only_explicit_nondefault_ack_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            for kind, contract in (("valkey_tcp", "synced"), ("valkey_tcp", "periodic_sync"),
                                   ("valkey_rdma", "os_buffered"), ("kvrocks", "page_cache")):
                job = dict(receipt["job"], backend=kind)
                if contract != "synced":
                    job["ack_contract"] = contract
                original = copy.deepcopy(job)
                config = cloud.make_run_config(job, directory, {}, None)
                self.assertEqual(config["backend"]["options"].get("ack_contract", "synced"), contract)
                self.assertEqual("ack_contract" in config["backend"]["options"], contract != "synced")
                self.assertEqual(job, original)
                different = "periodic_sync" if kind.startswith("valkey") and contract == "synced" else "synced"
                with self.assertRaisesRegex(ValueError, "ack_contract"):
                    cloud.make_run_config(job, directory, {"ack_contract": different}, None)

    def test_case_summary_requires_reported_and_executed_ack_contract_match(self):
        cases = (("periodic_sync", None, "periodic_sync", False),
                 ("synced", "periodic_sync", "synced", False),
                 ("periodic_sync", "periodic_sync", None, False),
                 ("periodic_sync", "periodic_sync", "periodic_sync", True),
                 ("os_buffered", "os_buffered", "os_buffered", True),
                 ("page_cache", "page_cache", "page_cache", True))
        for selected, reported, executed, admitted in cases:
            with self.subTest(selected=selected, reported=reported, executed=executed), tempfile.TemporaryDirectory() as temporary:
                directory, receipt = self.failure_fixture(Path(temporary))
                kind = "kvrocks" if selected == "page_cache" else "valkey_tcp"
                receipt["job"].update(backend=kind, ack_contract=selected)
                config = json.loads((directory / "config.json").read_text())
                config["backend"] = {"kind": kind, "options": {}}
                if executed is not None:
                    config["backend"]["options"]["ack_contract"] = executed
                for filename in ("config.json", "normalized-config.json"):
                    (directory / filename).write_text(json.dumps(config))
                for filename, field in (("manifest.json", "backend_info"), ("summary.json", "backend")):
                    path = directory / "result" / filename
                    data = json.loads(path.read_text())
                    data[field]["name"] = kind
                    data[field]["configuration"] = {} if reported is None else {"ack_contract": reported}
                    if filename == "manifest.json":
                        data["run"]["config"] = config
                    path.write_text(json.dumps(data))
                    receipt["artifacts_sha256"][filename] = hashlib.sha256(path.read_bytes()).hexdigest()
                (directory / "receipt.json").write_text(json.dumps(receipt))
                if admitted:
                    self.assertEqual(cloud.summarize_case(directory)["ack_contract"], reported)
                else:
                    with self.assertRaisesRegex(ValueError, "ack_contract"):
                        cloud.summarize_case(directory)

    def test_undispatched_rejection_retains_response_statistics_without_api_clocks(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            samples = directory / "result/samples.csv"
            with samples.open() as stream:
                fields = csv.DictReader(stream).fieldnames
            with samples.open("a", newline="") as stream:
                csv.DictWriter(stream, fieldnames=fields).writerow({
                    "phase": "measurement", "operation": "get", "outcome": "rejected",
                    "scheduled_ns": 400, "dispatched_ns": 0, "dispatch_observed": "false",
                    "completed_ns": 430, "api_ns": 0, "response_ns": 30, "scheduling_ns": 0,
                    "logical_keys": 1, "bytes": 0, "error_kind": "capacity"})
            receipt["artifacts_sha256"]["samples.csv"] = hashlib.sha256(samples.read_bytes()).hexdigest()
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)
            rejected = next(item for item in row["metrics"]["populations"] if item["outcome"] == "rejected")
            self.assertEqual(rejected["response_ns"]["sample_count"], 1)
            self.assertEqual(rejected["response_mean_ns"], 30)
            self.assertEqual(rejected["response_stddev_ns"], 0)
            for kind in ("api", "scheduling"):
                self.assertEqual(rejected[kind + "_ns"]["sample_count"], 0)
                for stat in ("mean", "min", "stddev"):
                    self.assertIsNone(rejected[kind + "_" + stat + "_ns"])

    def test_unvalidated_failure_never_becomes_a_performance_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, receipt = self.failure_fixture(root)
            receipt["raw_artifacts_validated"] = False
            (directory / "receipt.json").write_text(json.dumps(receipt))
            # A timed-out client can finish writing after receipt admission failed.
            # Those late artifacts are preserved, not promoted into observations.
            late = directory / "result" / "measurement-read-evidence.json"
            late.write_text(json.dumps({
                "schema_version": 1, "scope": "measurement_boundary",
                "measurement_attempted": True, "measurement_drained": False,
                "start": None, "end": None, "deltas": {"available": False}}))
            original_receipt = (directory / "receipt.json").read_bytes()
            row = cloud.summarize_case(directory)
            self.assertNotIn("metrics", row)
            self.assertEqual(row["status"], "failed")
            self.assertEqual((directory / "receipt.json").read_bytes(), original_receipt)
            receipt["status"] = "completed"
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                cloud.summarize_case(directory)

    def test_common_operation_order_is_identical_across_backends(self):
        spec = json.loads(Path(cloud.__file__).with_name("cloud-campaign.json").read_text())
        jobs = cloud.make_matrix(spec)
        for repeat in range(spec["repeats"]):
            expected = None
            for backend in cloud.BACKENDS:
                block = [job for job in jobs if job["repeat"] == repeat and job["backend"] == backend]
                common = [(job["profile"], job["value_size"], job["offered_ops_per_second"])
                          for job in block if job["cohort"] == "primary_rf1"]
                if expected is None:
                    expected = common
                self.assertEqual(common, expected)
                for job in block:
                    config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, None)
                    self.assertLessEqual(len(config["namespace"].encode()), 64)


if __name__ == "__main__":
    unittest.main()
