"""Logical-owner identities remain distinct when physical hosts are shared."""
import copy
import csv
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cloud_campaign as campaign
import cloud_controller as controller
import campaign as run_campaign


class MultiOwnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.jobs = [job for selection in campaign.M3_SELECTIONS for job in campaign.make_focused_matrix(selection)]

    def test_reviewed_matrix(self):
        self.assertEqual(len(self.jobs), 179)
        self.assertEqual(len({job['case_id'] for job in self.jobs}), 179)
        self.assertEqual({(j['owner_topology'], j['owner_count']) for j in self.jobs},
                         {('colocated', 16), ('separate', 2), ('separate', 5)})
        for job in self.jobs:
            campaign.validate_focused_job(job)
            self.assertTrue(job['thread_cpu_sampling'])
            if job['selection'] == 'nvmeof_p13_write_engine':
                concurrencies = (4, 8, 16, 32, 64) if job['case_kind'] == 'mainnet_replay' else (8, 16, 32, 64)
            else:
                concurrencies = (16, 64, 256)
            self.assertIn(job['concurrency'], concurrencies)
            self.assertEqual(campaign.active_owner_count(job), job['owner_count'])
            self.assertEqual(campaign.job_owner_hosts(job),
                             1 if job['owner_topology'] == 'colocated' else job['owner_count'])

    def test_write_ceiling_and_historical_sync_controls_survive_margin(self):
        jobs = [job for job in self.jobs if job["selection"] == "nvmeof_m3_t2_5"]
        puts = [job for job in jobs if job["mix"]["put"] == 100]
        self.assertEqual({(job["concurrency"], job["value_size"], job["repeat"]) for job in puts},
                         {(16, 1024, 0), (16, 65536, 0), (256, 1024, 0)})
        mixed, = [job for job in jobs if job["mix"]["read"] == 50]
        self.assertEqual((mixed["concurrency"], mixed["value_size"]), (16, 1024))
        self.assertTrue(all(job["durability_tier"] == "synchronous" for job in puts))
        self.assertTrue(all(job["repeat"] == 0 for job in jobs))
        t1 = [job for job in self.jobs if job["selection"] == "nvmeof_m3_t1"]
        self.assertEqual(len(t1), 8)
        self.assertTrue(all(job["owner_count"] == 16 for job in t1))

    def test_valkey_matched_cells_run_before_mixed_and_probes(self):
        jobs = campaign.make_focused_matrix("nvmeof_m3_t2_5")
        self.assertEqual([(job["mix"]["read"], job["value_size"]) for job in jobs[:4]],
                         [(100, 1024), (100, 65536), (0, 1024), (0, 65536)])
        for job in jobs[:4]:
            self.assertEqual((job["owner_count"], job["concurrency"], job["keys"], job["distribution"],
                              job["durability_tier"], job["coordinator_cache_bytes"]),
                             (5, 16, 4096, "uniform", "synchronous", 0))
        self.assertEqual(jobs[0]["measurement_sizing"]["measurement_seconds"], 5)
        self.assertEqual(jobs[4]["mix"]["read"], 50)
        self.assertEqual([(job["concurrency"], job["coordinator_cache_bytes"]) for job in jobs[5:]],
                         [(64, 0), (64, 8 * 1024**2), (256, 0)])

    def test_authority_is_removed_only_after_every_owner_stops(self):
        for fail in (False, True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as tmp:
                instance = object.__new__(controller.Controller)
                instance.root = Path(tmp)
                directory = instance.root / "case"
                database = directory / "database"
                database.mkdir(parents=True)
                (database / "authority").write_text("granted epoch 1")
                job = {"case_id": "case", "selection": "nvmeof_m3_t2_2",
                       "owner_topology": "separate", "owner_count": 2}
                instance.active_tide = (job, directory, [0, 1])
                instance.active_nvme = instance.shared_tide = None
                instance.tide_cleanup_forbidden = False
                instance.cleanup_deadline = lambda: 30
                instance.cleanup_timeout = lambda deadline: 30
                calls = []
                def request(index, payload, **kwargs):
                    self.assertTrue(database.exists())
                    calls.append(index)
                    if fail and index == 1:
                        raise controller.InfrastructureError("owner stop unconfirmed")
                    return {"stop_command_completed": True}
                instance.request = request
                if fail:
                    with self.assertRaisesRegex(controller.InfrastructureError, "cleanup incomplete"):
                        instance.stop_tide()
                    self.assertTrue(database.exists())
                else:
                    instance.stop_tide()
                    self.assertFalse(database.exists())
                self.assertEqual(sorted(calls), [0, 1])

    def test_unreviewed_owner_count_fails_closed(self):
        job = copy.deepcopy(self.jobs[0])
        job['owner_count'] = 2
        with self.assertRaises(ValueError):
            campaign.active_owner_count(job)

    def test_physical_fleet_selects_only_its_topology(self):
        for hosts, expected, selection in ((1, 8, "nvmeof_m3_t1"), (2, 8, "nvmeof_m3_t2_2"), (5, 8, "nvmeof_m3_t2_5")):
            jobs = controller.controller_matrix({'mode': 'focused',
                'selection': selection, 'owner_hosts': hosts})
            self.assertEqual(len(jobs), expected)
            self.assertTrue(all(campaign.job_owner_hosts(job) == hosts for job in jobs))

    def test_read_diagnostics_and_cache_controls(self):
        for selection, owners in (("nvmeof_m3_t1", 16), ("nvmeof_m3_t2_2", 2), ("nvmeof_m3_t2_5", 5)):
            jobs = [job for job in self.jobs if job["selection"] == selection]
            reads = [job for job in jobs if job["mix"]["read"] == 100]
            self.assertTrue(all(job["thread_cpu_sampling"] for job in reads))
            profiles = [job for job in reads if job.get("diagnostics", {}).get("read_profile_limit")]
            self.assertEqual({job["value_size"] for job in profiles}, {1024, 65536})
            self.assertTrue(all(job["diagnostics"] == {"read_profile_limit": 1024} for job in profiles))
            timelines = [job for job in jobs if job.get("diagnostics", {}).get("write_timeline_limit")]
            self.assertEqual(len(timelines), 1)
            self.assertEqual((timelines[0]["mix"]["put"], timelines[0]["value_size"],
                              timelines[0]["durability_tier"]), (100, 1024, "synchronous"))
            self.assertEqual(timelines[0]["diagnostics"], {"write_timeline_limit": 1024})
            cached, = [job for job in reads if job["coordinator_cache_bytes"]]
            self.assertEqual((cached["owner_count"], cached["concurrency"], cached["value_size"]),
                             (owners, 64, 1024))
            self.assertTrue(cached["warmup_key_sweep"])
            self.assertGreaterEqual(cached["coordinator_cache_bytes"], 2 * cached["keys"] * cached["value_size"])
            off, = [job for job in reads if not job["coordinator_cache_bytes"]
                    and (job["owner_count"], job["concurrency"], job["value_size"]) == (owners, 64, 1024)]
            excluded = {"case_id", "coordinator_cache_bytes"}
            self.assertEqual({k: v for k, v in cached.items() if k not in excluded},
                             {k: v for k, v in off.items() if k not in excluded})

    def test_read_sampler_includes_coordinator_and_each_logical_owner(self):
        for job in self.jobs:
            if job["mix"]["read"] != 100:
                continue
            count = campaign.active_owner_count(job)
            config = {"spec": {"selection": job["selection"]},
                      "fleet": {"nodes": [{}] * campaign.job_owner_hosts(job)}}
            instance = SimpleNamespace(config=config, request=lambda index, *args, **kwargs: index)
            for action in ("thread_cpu_start", "thread_cpu_stop"):
                with self.subTest(case=job["case_id"], action=action):
                    self.assertEqual(controller.Controller.all_hosts(
                        instance, {"action": action, "case_id": job["case_id"]},
                        case=job["case_id"], owner_count=count), list(range(-1, count)))

    def test_measurement_windows_and_admission_cap(self):
        for job in self.jobs:
            sizing = job["measurement_sizing"]
            if job["case_kind"] == "mainnet_replay":
                self.assertEqual(job["measurement_operations"], 4096 * 510)
                deadline = {4: 1000, 8: 800}.get(job["concurrency"], 600)
                self.assertEqual(sizing["case_deadline_seconds"], deadline)
                continue
            if job["case_kind"] == "remote_put" and job["selection"] in (
                    "nvmeof_p9_small_put", *campaign.WRITE_ENGINE_SELECTIONS,
                    campaign.REDRAIN_AB_SELECTION, campaign.Q0_SELECTION):
                self.assertEqual(sizing["measurement_seconds"],
                                 job["measurement_operations"] / sizing["rate_calls_per_second"])
            else:
                self.assertIn(sizing["measurement_seconds"], (5, 10))
                if sizing["rate_calls_per_second"] >= 90000:
                    self.assertEqual(sizing["measurement_seconds"], 5)
                elif job["mix"]["read"] != 50:
                    self.assertEqual(sizing["measurement_seconds"], 10)
            self.assertGreaterEqual(job["warmup_operations"] / sizing["rate_calls_per_second"], 2)
            if job["mix"]["put"]:
                self.assertLessEqual(4096 + job["warmup_operations"] + job["measurement_operations"], 1_000_000)
        for selection in campaign.M3_SELECTIONS:
            arms = ("file", "prewritten") if selection in campaign.WRITE_ENGINE_SELECTIONS else (None,)
            limit = campaign.WRITE_ENGINE_ARCHIVE_LIMIT_BYTES.get(selection, 3_000_000_000)
            for arm in arms:
                self.assertLessEqual(sum(job["measurement_sizing"]["expected_archive_bytes"]
                                         for job in self.jobs if job["selection"] == selection
                                         and (arm is None or job["arm"] == arm)), limit)

    def test_cache_pair_has_explicit_hit_window_and_remote_deadline_rates(self):
        pairs = [job for job in self.jobs
                 if job["measurement_sizing"].get("minimum_phase_seconds")]
        self.assertEqual(len(pairs), 6)
        for job in pairs:
            sizing = job["measurement_sizing"]
            self.assertEqual((job["measurement_operations"], job["warmup_operations"]),
                             (2_500_000, 1_000_000))
            self.assertEqual(sizing["window_planning_rate_calls_per_second"], 500_000)
            self.assertEqual(sizing["minimum_phase_seconds"], {"warmup": 2, "measurement": 5})
            self.assertEqual(sizing["case_deadline_seconds"], 315)
            self.assertEqual(sizing["rate_calls_per_second"], 143199.73694040513)
            self.assertIn("not a hardware bound", sizing["window_planning_assumption"])
        with self.assertRaisesRegex(ValueError, "pure read"):
            campaign._m3_measurement_sizing("put", 1024, 64, read_window_rate=500_000)

    def test_duration_qualification_uses_complete_raw_phase_timestamps(self):
        job = copy.deepcopy(next(job for job in self.jobs if job["coordinator_cache_bytes"]))
        job.update(warmup_operations=2, measurement_operations=2)
        config = {"qualification_only": False, "dataset": {"keys": 4096},
                  "phases": {"preload": False, "warmup_operations": 2,
                             "measurement_operations": 2, "aging_operations": 0}}
        for warmup, measurement, outcome, omit in (
                (2_000_000_000, 5_000_000_000, "success", False),
                (1_999_999_999, 5_000_000_000, "success", False),
                (2_000_000_000, 4_999_999_999, "success", False),
                (2_000_000_000, 5_000_000_000, "error", False),
                (2_000_000_000, 5_000_000_000, "success", True)):
            rows = []
            for phase, start, elapsed in (("warmup", 0, warmup), ("measurement", 3_000_000_000, measurement)):
                for index in range(2):
                    scheduled = start if index == 0 else start + elapsed - 1
                    completed = start + 1 if index == 0 else start + elapsed
                    rows.append({"phase": phase, "operation": "read",
                                 "outcome": outcome if phase == "measurement" and index else "success",
                                 "scheduled_ns": scheduled, "dispatched_ns": scheduled,
                                 "dispatch_observed": "true", "completed_ns": completed,
                                 "api_ns": completed - scheduled, "response_ns": completed - scheduled,
                                 "scheduling_ns": 0, "logical_keys": 1, "bytes": 1024,
                                 "error_kind": "timeout" if outcome == "error" else ""})
            if omit:
                rows.pop()
            with self.subTest(warmup=warmup, measurement=measurement, outcome=outcome, omit=omit), \
                    tempfile.TemporaryDirectory() as root:
                root = Path(root)
                with (root / "samples.csv").open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)
                windows = {}
                counts = run_campaign.verify_timing_result(root, config, allow_partial=True, phase_windows=windows)
                self.assertEqual(sum(counts.values()), len(rows))
                self.assertEqual(windows["warmup"]["elapsed_ns"], warmup)
                self.assertEqual(windows["measurement"]["unsuccessful_rows"], outcome == "error")
                qualification = campaign.qualify_m3_phase_windows(job, windows)
                self.assertEqual(qualification["qualified"],
                                 warmup >= 2_000_000_000 and measurement >= 5_000_000_000
                                 and outcome == "success" and not omit)
                self.assertEqual(qualification["observed"], windows)
                if not qualification["qualified"]:
                    self.assertTrue(qualification["reasons"])
        self.assertFalse(campaign.qualify_m3_phase_windows(job, {})["qualified"])

    def test_repeated_hosts_keep_logical_indices(self):
        config = {'fleet': {'nodes': [{'index': 0, 'address': '10.0.0.1'}]}}
        job = next(j for j in self.jobs if j['owner_count'] == 16)
        self.assertEqual(len(controller.logical_owner_hosts(config, job)), 16)
        self.assertEqual(controller.owner_host_index(config, job, 15), 0)
        with patch.object(controller, 'unit_name', side_effect=lambda c, case, role: role), patch.object(
                controller, 'owner_slice_limits', return_value=('kvowners123.slice', '')), patch.object(
                controller, 'unit_status', side_effect=lambda name: {'ControlGroup': '/kvowners123.slice/' + name}):
            observed = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual([p['role'] for p in observed['processes']],
                         ['storage-' + str(i) for i in range(16)])
        self.assertEqual([p['cgroup'] for p in observed['processes']],
                         ['/kvowners123.slice/storage-' + str(i) + '.service' for i in range(16)])
        with patch.object(controller, 'unit_name', return_value='owner'), patch.object(
                controller, 'owner_slice_limits', return_value=('kvowners123.slice', '')), patch.object(
                controller, 'unit_status', return_value={'ControlGroup': ''}):
            missing = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual(missing['processes'], [])
        self.assertEqual(len(missing['missing']), 16)
        with patch.object(controller, 'unit_name', return_value='owner'), patch.object(
                controller, 'unit_status', side_effect=controller.InfrastructureError('cannot observe unit')):
            missing = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual(missing['processes'], [])
        self.assertEqual(len(missing['missing']), 16)
        self.assertIn('cannot observe unit', missing['missing'][0]['reason'])

    def test_admission_does_not_inherit_case_workload(self):
        job = self.jobs[0]
        options = {'coordinator_read_cache_bytes': 999,
                   'external_remote_config': {'read_cache_bytes': 999, 'read_lanes': 8,
                                              'pin_read_lanes': True}}
        original = campaign.make_run_config(job, Path('/case'), options, 'a' * 40)
        expected = campaign.make_owner_admission_config(original, job['owner_count'])
        mutated = copy.deepcopy(original)
        mutated.update(seed=999, namespace='measured', qualification_only=True,
                       diagnostics={'read_profile_limit': 512}, interference={'unexpected': True})
        mutated['dataset'] = {'keys': 999, 'value_size': 65536, 'entropy': 0.1}
        mutated['workload'] = {'distribution': {'zipf': {'exponent': 0.99}},
                               'mix': campaign.mix(put=100), 'batch_size': 256, 'put_key_policy': 'new'}
        mutated['execution'] = {'mode': 'open_loop', 'concurrency': 256}
        mutated['phases'] = {'preload': False, 'verify_existing_dataset': True,
            'evict_mapped_wal_before_measurement': True, 'warmup_key_sweep': True,
            'measurement_key_sweep': False, 'measurement_operations': 999}
        mutated['limits'] = {'deadline_seconds': 999}
        self.assertEqual(campaign.make_owner_admission_config(mutated, job['owner_count']), expected)
        self.assertEqual(original['backend']['options']['external_remote_config']['read_cache_bytes'], 999)
        self.assertNotIn('measurement_gate', expected)
        self.assertNotIn('measurement_completion_gate', expected)
        self.assertNotIn('diagnostics', expected)

    def test_short_admission_connects_writes_and_reads_each_slot(self):
        job = next(j for j in self.jobs if j['owner_count'] == 16 and j['owner_topology'] == 'colocated')
        identities = {str(i): {'host_nqn': 'owner-' + str(i), 'host_id': [i + 1] * 16} for i in range(16)}
        options = {'owners': 16, 'external_remote_config': {'storage': {'partitions': list(range(16))},
            'endpoints': [{'partition': i, 'address': '10.0.0.1:' + str(9400 + i)} for i in range(16)],
            'live_nvme': {'owner_identities': identities}}, 'external_owner_read_config': [
                {'partition': i} for i in range(16)], 'external_owner_durability_receipts': ['receipt-' + str(i) for i in range(16)]}
        instance = SimpleNamespace(config={'binary': '/binary', 'source_revision': 'a' * 40,
            'repository_root': '/repo', 'fleet': {'nodes': [{'index': 0}]}},
            remaining=lambda: 3600, clock=time.monotonic)
        instance.run_logical_owner_admission = lambda *args: controller.Controller.run_logical_owner_admission(instance, *args)
        configs = []
        missing = None
        completed_writes = 256
        def run(argv, **kwargs):
            config = json.loads(Path(argv[2]).read_text())
            configs.append(config)
            selected = config['backend']['options']
            # Status compares the server's complete immutable partition inventory.
            self.assertEqual(selected['external_remote_config'], {
                **options['external_remote_config'], 'read_cache_bytes': 0,
                'read_lanes': 1, 'pin_read_lanes': False})
            self.assertEqual(selected['external_owner_durability_receipts'],
                             options['external_owner_durability_receipts'])
            self.assertEqual(selected['external_owner_read_config'], options['external_owner_read_config'])
            Path(selected['path']).mkdir()
            output = Path(argv[-1])
            output.mkdir()
            (output / 'summary.json').write_text(json.dumps({'status': 'completed'}))
            (output / 'samples.csv').write_text('phase,operation,outcome\n'
                + 'preload,put,success\n' * 256 + 'measurement,read,success\n' * 256)
            routes = [{'host_nqn': identity['host_nqn'], 'writer': writer,
                       'transport': 'rdma', 'address': '10.0.0.1', 'service': 4420 + 2 * i,
                       'namespace_identity': {'nqn': 'target-' + str(i)}}
                      for i, identity in enumerate(identities.values()) if i != missing
                      for writer in (None, {'epoch': 1, 'file_index': 0})]
            (output / 'measurement-read-evidence.json').write_text(json.dumps({'end': {'live_nvmeof': {
                'completed_read_ops': 256, 'completed_write_ops': completed_writes, 'errors': 0,
                'payload_rpc_requests': 0, 'routes': routes}}}))
        helper = SimpleNamespace(controller_snapshot=lambda: {}, controller_matches=lambda *args: True)
        spec = SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None))
        with tempfile.TemporaryDirectory() as tmp, patch.object(controller, 'command', side_effect=run), patch.object(
                controller, 'environment', return_value={}), patch(
                'importlib.util.spec_from_file_location', return_value=spec), patch(
                'importlib.util.module_from_spec', return_value=helper):
            root = Path(tmp) / 'success'
            root.mkdir()
            controller.Controller.admit_logical_owners(instance, job, root, options)
            proofs = json.loads((root / 'owner-admission.json').read_text())
            self.assertEqual(len(configs), 1)
            self.assertEqual(len(proofs['owners']), 16)
            self.assertTrue(proofs['coordinator_exited'])
            self.assertTrue(proofs['coordinator_database_retained'])
            self.assertEqual(proofs['authority_database'], 'database')
            self.assertEqual(Path(configs[0]['backend']['options']['path']), root / 'database')
            self.assertTrue((root / 'database').is_dir())
            self.assertFalse((root / 'owner-admission/database').exists())
            # Grouped transport writes are not the API preload-put population.
            completed_writes = 16
            grouped = Path(tmp) / 'grouped-writes'
            grouped.mkdir()
            controller.Controller.admit_logical_owners(instance, job, grouped, options)
            self.assertTrue(json.loads((grouped / 'owner-admission.json').read_text())['coordinator_exited'])
            completed_writes = 256
            for missing in range(16):
                failed = Path(tmp) / f'missing-{missing}'
                failed.mkdir()
                with self.assertRaisesRegex(controller.SafetyError, 'write/read admission failed'):
                    controller.Controller.admit_logical_owners(instance, job, failed, options)
            missing = None
            helper.controller_snapshot = lambda: {'nvme1': {}}
            failed = Path(tmp) / 'controller-left'
            failed.mkdir()
            with self.assertRaisesRegex(controller.SafetyError, 'left live controllers'):
                controller.Controller.admit_logical_owners(instance, job, failed, options)
            self.assertTrue((failed / 'database').is_dir())
        config = configs[0]
        self.assertFalse(config['qualification_only'])
        self.assertTrue(config['phases']['preload'])
        self.assertEqual(config['phases']['measurement_operations'], 256)
        self.assertTrue(config['phases']['measurement_key_sweep'])
        self.assertEqual(config['execution']['concurrency'], 16)
        self.assertNotIn('measurement_gate', config)
        self.assertEqual(config['backend']['options']['owners'], 16)
        self.assertEqual(config['backend']['options']['coordinator_read_cache_bytes'], 0)
        self.assertEqual(options['external_remote_config']['storage']['partitions'], list(range(16)))

    def test_real_unit_names_and_shared_host_caps(self):
        config = {'prefix': 'bench-' + '0' * 16, 'repository_root': '/source',
                  'spec': {'selection': 'nvmeof_m3_t1'},
                  'fleet': {'nodes': [{'index': 0}], 'server_cpus': 8, 'server_memory_mib': 16384}}
        job = next(j for j in self.jobs if j['owner_count'] == 16)
        self.assertTrue(controller.unit_name(config, job['case_id'], 'storage-15').endswith('-storage-15'))
        with self.assertRaises(controller.SafetyError):
            controller.unit_name(config, job['case_id'], 'storage-16')
        budgets = [controller.owner_process_budget(config, job, i) for i in range(16)]
        self.assertEqual({cpu for cpu, _ in budgets}, {8})
        self.assertEqual(sum(memory for _, memory in budgets), 16384)
        name, limits = controller.owner_slice_limits(config, job['case_id'])
        self.assertRegex(name, r'^kvowners[0-9a-f]{24}\.slice$')
        self.assertNotEqual(name, controller.owner_slice_limits(config, job['case_id'] + 'x')[0])
        self.assertIn('CPUQuota=800%', limits)
        self.assertIn('MemoryMax=16384M', limits)
        with patch.object(controller, 'environment', return_value={}):
            argv = controller.systemd_argv(config, job['case_id'], 'storage-15', 8, 1024, 600, ['/binary'])
        self.assertIn('--property=Slice=' + name, argv)
        self.assertIn('--property=MemoryMax=1024M', argv)
        self.assertFalse(any(arg.startswith('--property=CPUQuota=') for arg in argv))


if __name__ == '__main__':
    unittest.main()
