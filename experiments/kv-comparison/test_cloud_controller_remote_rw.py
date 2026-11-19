import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import cloud_controller as controller
import q0_campaign


class CompactReadLifecycleTests(unittest.TestCase):

    def test_live_anchor_preparation_receipt_and_emitted_transport_match(self):
        from test_cloud_controller import config_fixture
        for selection in ("nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "valkey_rdma_reads"):
            jobs = [job for job in controller.cloud_campaign.make_focused_matrix(selection)
                    if job.get("nvmeof_route") == "live"]
            for job in jobs:
                with self.subTest(selection=selection, case=job["case_id"]), tempfile.TemporaryDirectory() as tmp:
                    config = config_fixture()
                    config["spec"].update(mode="focused", selection=selection,
                                          backends=["native", "tidehunter_nvme_rdma"])
                    runtime = controller.Controller.__new__(controller.Controller)
                    runtime.config = config
                    runtime.remaining = lambda: 600
                    with patch.object(controller, "command", side_effect=RuntimeError("preparation captured")), \
                         patch.object(controller, "environment", return_value={}):
                        with self.assertRaisesRegex(RuntimeError, "preparation captured"):
                            runtime.prepare_tide(job, Path(tmp))
                    prep = json.loads((Path(tmp) / "prepare-input.json").read_text())
                    emitted = controller.cloud_campaign.make_run_config(job, tmp, {}, "a" * 40)
                    transport = emitted["backend"]["options"]["transport"]
                    # run_case adds this exact record to the case receipt.
                    receipt = controller.owner_control_settings(job)
                    self.assertEqual(prep["transport"], transport)
                    self.assertEqual(receipt["owner_transport"], transport)
                    self.assertEqual(receipt["owner_control_backend"], "tidehunter_verbs_native")
                    self.assertEqual(receipt["owner_transport_security_label"], "trusted_network_no_tls")
                    # The external preparer maps verbs+trusted to ofi-verbs-trusted TLS.
                    self.assertEqual(transport["fabric"]["backend"], "verbs")
                    self.assertEqual(transport["security"], "trusted")
                    if "read_lanes" in job:
                        self.assertEqual(prep["read_lanes"], job["read_lanes"])
                    else:
                        self.assertNotIn("read_lanes", prep)
                    self.assertNotIn("owner_read_workers", prep)

    def test_cache_coordinator_allowance_is_independent_of_owner_allowance(self):
        jobs = [j for j in controller.cloud_campaign.make_focused_matrix("nvmeof_reads_geometry")
                if "coordinator_read_memory_allowance_bytes" in j]
        self.assertEqual(len(jobs), 2)
        for job in jobs:
            self.assertNotIn("owner_read_memory_allowance_bytes", job)
            evidence = {"schema_version": 1, "backend": "tidehunter_live_nvmeof",
                        "coordinator": {"read_rpc_count": 0, "cache": {"budget_bytes": job["coordinator_cache_bytes"]}},
                        "owner_read_configuration": [{"partition": 0, "read_memory_allowance_bytes": 512 * 1024**2}],
                        "effective_configuration": {"coordinator": {
                            "read_memory_allowance_bytes": job["coordinator_read_memory_allowance_bytes"]}}}
            self.assertEqual(controller.verify_counter_evidence(job, evidence), evidence)
            evidence["effective_configuration"]["coordinator"]["read_memory_allowance_bytes"] -= 1
            with self.assertRaisesRegex(controller.SafetyError, "coordinator read allowance"):
                controller.verify_counter_evidence(job, evidence)
    def test_cache_coordinator_allowance_bounds(self):
        from test_cloud_controller import config_fixture
        runtime = controller.Controller.__new__(controller.Controller)
        runtime.config = config_fixture()
        runtime.config["spec"].update(mode="focused", selection="nvmeof_reads_geometry",
                                      backends=["native", "tidehunter_nvme_rdma"])
        job = next(j for j in controller.cloud_campaign.make_focused_matrix("nvmeof_reads_geometry")
                   if "coordinator_read_memory_allowance_bytes" in j)
        for value in (True, 512 * 1024**2 - 1, 16 * 1024**3 + 1):
            with self.subTest(value=value), self.assertRaisesRegex(controller.SafetyError, "512 MiB"):
                runtime.prepare_tide(dict(job, coordinator_read_memory_allowance_bytes=value), Path("/tmp"))


    def test_live_request_bound_preparation(self):
        from test_cloud_controller import config_fixture
        config = config_fixture()
        config["spec"].update(mode="focused", selection="nvmeof_reads_geometry")
        config["spec"]["backends"] = ["native", "tidehunter_nvme_rdma"]
        jobs = controller.cloud_campaign.make_focused_matrix("nvmeof_reads_geometry")
        prep = controller.live_nvme_preparation(config, jobs[0]["case_id"], "rdma")
        self.assertEqual(prep["max_inflight_requests"], 128)
        self.assertEqual(prep["max_inflight_bytes"], 64 * 1024**2)
        for value in (0, 1025, True, None):
            with patch.object(controller.cloud_campaign, "make_focused_matrix",
                              return_value=[dict(jobs[0], live_max_inflight_requests=value)]):
                with self.assertRaises(controller.SafetyError):
                    controller.live_nvme_preparation(config, jobs[0]["case_id"], "rdma")

    def test_nvmeof_path_reviewed_cases_pass_compact_admission(self):
        from test_cloud_controller import config_fixture, clients_fixture
        for selection in controller.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                config = config_fixture()
                config['fleet']['nodes'] = config['fleet']['nodes'][:(
                    2 if selection == q0_campaign.SELECTION else 1)]
                config['spec'].update(mode='focused', selection=selection)
                jobs = controller.cloud_campaign.make_focused_matrix(selection)
                if selection == q0_campaign.SELECTION:
                    with self.assertRaisesRegex(ValueError, 'every selected binary role'):
                        controller.Controller(config, clients_fixture(config))
                    seals = {role: {
                        'revision': (q0_campaign.BASELINE if role == 'baseline_ab'
                                     else 'a' * 40 if role == 'land4' else role),
                        'sha256': f'{index:064x}',
                        **({'instrumentation_patch_sha256': q0_campaign.COUNTER_PATCH_SHA256}
                           if role in q0_campaign.AB_ROLES else {}),
                    } for index, role in enumerate(q0_campaign.ROLES)}
                    jobs, attribution = q0_campaign.select_cases(jobs, seals)
                    digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode())
                    config['spec'].update(case_binaries=seals, bisect_revisions=attribution,
                                          job_count=len(jobs), matrix_sha256=digest.hexdigest())
                runtime = controller.Controller(config, clients_fixture(config))
                runtime.run_floor_case = MagicMock(return_value='floor')
                runtime.run_fio_case = MagicMock(return_value='fio')
                with tempfile.TemporaryDirectory() as tmp:
                    runtime.root = Path(tmp)
                    for job in jobs:
                        if selection in controller.cloud_campaign.M3_SELECTIONS:
                            config['fleet']['nodes'] = config_fixture()['fleet']['nodes'][:controller.cloud_campaign.job_owner_hosts(job)]
                        with self.subTest(case=job['case_id']):
                            if job['case_kind'] in ('net_floor', 'ssd_floor', 'ssd_write_floor'):
                                self.assertEqual(runtime.run_case(job), 'floor')
                            elif job['case_kind'] == 'fio_floor':
                                self.assertEqual(runtime.run_case(job), 'fio')
                            else:
                                with patch.object(controller, 'guarded', side_effect=RuntimeError('admitted')):
                                    with self.assertRaisesRegex(RuntimeError, 'admitted'):
                                        runtime.run_case(job)
                    remote = next(j for j in jobs if j.get('nvmeof_route') == 'live')
                    if selection in controller.cloud_campaign.M3_SELECTIONS:
                        config['fleet']['nodes'] = config['fleet']['nodes'][:1]
                    for mutated in (dict(remote, selection='leased_ab_compact'),
                                    dict(remote, owner_count=5), dict(remote, history_persistence='unexpected')):
                        with self.subTest(mutated=mutated), self.assertRaises(ValueError):
                            runtime.run_case(mutated)

    def test_cold_device_ratio_requires_nine_reads_per_ten_calls(self):
        before = [{'owner_index': 0, 'major': 259, 'minor': 1, 'read_ops': 10,
                   'read_sectors': 0, 'write_ops': 0, 'write_sectors': 0}]
        after = [{**before[0], 'read_ops': 18}]
        result = controller.qualify_media_evidence(
            'case', {'expected': 'cold', 'history': 'normal'}, before, after, 10, True)
        self.assertEqual(result['status'], 'unqualified')
        self.assertIn('cold_read_ratio_below_threshold', result['reason_codes'])
