import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

import gcp_campaign
import manual_campaign as manual
import gcp_runtime
import gcp_image
import prepare
import workflow_input


CONFIG = {'project_id': 'mysten-research', 'bucket': 'mysten-research-bench-artifacts',
          'service_account': gcp_campaign.GUEST_SERVICE_ACCOUNT, 'machine_type': 'z4m-highmem-16',
          'max_distance': 3, 'candidates': [{'zone': 'us-central1-a'}]}


class FocusedAdmissionTest(unittest.TestCase):
    def test_rejects_topology_profile_and_order_mutations(self):
        spec = manual.REVIEWED_SPECS['nvmeof_path_core']
        jobs = manual._reviewed_jobs(spec)
        for field, value in (('owner_count', 5), ('concurrency', 5), ('case_id', 'fake')):
            altered = copy.deepcopy(jobs)
            altered[0][field] = value
            with self.subTest(field=field), patch.object(workflow_input.cloud_campaign, 'make_focused_matrix', return_value=altered):
                with self.assertRaisesRegex(ValueError, 'focused matrix differs'):
                    manual._reviewed_jobs(spec)
        with patch.object(workflow_input.cloud_campaign, 'make_focused_matrix', return_value=jobs[::-1]):
            with self.assertRaisesRegex(ValueError, 'focused matrix differs'):
                manual._reviewed_jobs(spec)

    def test_frozen_observer_proof_binds_focused_matrix(self):
        spec = manual.REVIEWED_SPECS['nvmeof_path_core']
        proof = {'planned_cases': spec['job_count'], 'matrix_sha256': spec['matrix_sha256']}
        process = subprocess.CompletedProcess([], 0, json.dumps(proof), '')
        with patch.object(manual.subprocess, 'run', return_value=process):
            self.assertEqual(manual._qualify_observer('/package', spec), proof)
        for wrong in ({**proof, 'planned_cases': proof['planned_cases'] - 1},
                      {**proof, 'matrix_sha256': 'f' * 64}):
            process.stdout = json.dumps(wrong)
            with patch.object(manual.subprocess, 'run', return_value=process):
                with self.assertRaisesRegex(ValueError, 'different generated matrix'):
                    manual._qualify_observer('/package', spec)

    def test_rejects_changed_fields_types_or_extra_fields(self):
        spec = copy.deepcopy(manual.REVIEWED_SPECS['nvmeof_path_compact'])
        mutations = ({'job_count': 323}, {'matrix_sha256': '0' * 64}, {'selection': 'other'},
                     {'direct_control_arm': 0}, {'hard_deadline_seconds': 21600.0}, {'extra': True},
                     {'backends': ['native']})
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                manual._check_config(CONFIG, {**spec, **mutation}, 0)
        with self.assertRaisesRegex(ValueError, 'ceiling'):
            manual._check_config(CONFIG, spec, manual.CEILING_CENTS - manual.COMPUTE_CENTS + 1)
        with self.assertRaisesRegex(ValueError, 'zonal'):
            manual._check_config({**CONFIG, 'max_distance': 2}, spec, 0)

    def test_compact_lease_selection_and_budget(self):
        spec = manual.REVIEWED_SPECS['nvmeof_path_core']
        self.assertEqual(spec['owner_hosts'], 1)
        self.assertEqual(len(manual._reviewed_jobs(spec)), spec['job_count'])
        manual._check_config(CONFIG, spec, 889)
        with self.assertRaises(ValueError):
            manual._check_config(CONFIG, spec, 890)
        with self.assertRaisesRegex(ValueError, 'five.owner'):
            gcp_campaign._require_six_host_spec(spec)

    def test_nvmeof_path_exact_spec(self):
        for selection in manual.NVMEOF_PATH_SELECTIONS:
            if selection == manual.q0_campaign.SELECTION:
                continue  # Q0 builds its spec from binary seals, not the static registry.
            expected_count = workflow_input.cloud_campaign.FOCUSED_COUNTS[selection]
            with self.subTest(selection=selection):
                spec = copy.deepcopy(manual.REVIEWED_SPECS[selection])
                kwargs = {}
                if selection == workflow_input.cloud_campaign.Q0_SELECTION:
                    from test_q0_campaign import seals
                    kwargs['case_binaries'] = seals()
                    spec = manual.q0_spec(**kwargs)
                if selection == 'nvmeof_parallel_writes':
                    self.assertEqual(spec['matrix_sha256'], '64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4')
                if selection == 'nvmeof_single_caller':
                    self.assertEqual(spec['matrix_sha256'], '294b50957b21d6acafadcf1b682c71d1b424524a144ecd6c2c18e2b1cc40fd4e')
                if selection == 'nvmeof_f6':
                    self.assertEqual(spec['matrix_sha256'], '1186674f1706550f0faaac6d7852dcee711b43dba8c87b22be4dd071d768e8a7')
                    self.assertEqual(spec['machine_type'], 'z4m-highmem-16')
                    with self.assertRaises(ValueError):
                        manual._reviewed_jobs({**spec, 'machine_type': 'z4m-highmem-32'})
                if selection == 'nvmeof_f7_highmem32':
                    self.assertEqual(spec['machine_type'], 'z4m-highmem-32')
                    with self.assertRaises(ValueError):
                        manual._reviewed_jobs({**spec, 'machine_type': 'z4m-highmem-16'})
                jobs = manual._reviewed_jobs(spec)
                expected_fleet_count = {'nvmeof_p9_write_engine': 9, 'nvmeof_p13_write_engine': 25}.get(selection, expected_count)
                self.assertEqual(len(jobs), expected_fleet_count)
                self.assertEqual(spec['max_cases'], expected_count)
                self.assertEqual(spec['owner_hosts'], workflow_input.cloud_campaign.selection_owner_hosts(selection))
                self.assertNotIn('spdk_comparison', spec)
                self.assertEqual(spec['backends'], ['tidehunter_nvme_rdma'] if selection in workflow_input.cloud_campaign.M3_SELECTIONS or selection == 'nvmeof_f7_highmem32' else ['native', 'tidehunter_nvme_rdma'])
                self.assertEqual(workflow_input.specification(
                    'kv', ','.join(spec['backends']), 'focused', selection=selection, **kwargs),
                    {'reservation_hours': 2, **spec} if selection in workflow_input.cloud_campaign.M3_SELECTIONS else spec)
                self.assertTrue(gcp_runtime.live_mode(spec))
                for mutation in ({'job_count': expected_count - 1}, {'matrix_sha256': '0' * 64},
                                 {'owner_hosts': 6 if selection in workflow_input.cloud_campaign.M3_SELECTIONS else 5}, {'spdk_comparison': True},
                                 {'backends': ['tidehunter_verbs_native', 'tidehunter_nvme_rdma']}):
                    with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                        manual._reviewed_jobs({**spec, **mutation})

    def test_workflow_spec_selects_only_reviewed_inputs(self):
        backends = ','.join(manual.REVIEWED_SPECS['nvmeof_path_core']['backends'])
        for selection in ('nvmeof_path_compact', 'nvmeof_path_core', 'nvmeof_contracts'):
            selected = ','.join(manual.REVIEWED_SPECS[selection]['backends'])
            spec = workflow_input.specification('kv', selected, 'focused', selection=selection)
            self.assertEqual(spec, manual.REVIEWED_SPECS[selection])
            spec['backends'].clear()
            self.assertTrue(manual.REVIEWED_SPECS[selection]['backends'])
        for suite, names, selection in (('both', backends, 'nvmeof_path_core'), ('kv', 'native', 'nvmeof_path_core'),
                                        ('kv', backends, None), ('kv', backends, 'other')):
            with self.subTest(suite=suite, backends=names, selection=selection), self.assertRaises(ValueError):
                workflow_input.specification(suite, names, 'focused', selection=selection)
        with self.assertRaises(ValueError):
            workflow_input.specification('kv', 'native', 'standard', selection='full')

    def test_compact_budget_and_runtime_admission(self):
        self.assertEqual((manual.COMPUTE_CENTS, manual.CEILING_CENTS), (11834, 14500))
        self.assertEqual((manual.COMPACT_COMPUTE_CENTS, manual.COMPACT_CEILING_CENTS), (3945, 4834))
        now = datetime.now(timezone.utc)
        hosts = [{'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': f'run-{i}',
                  'id': str(100 + i), 'zone': 'us-central1-a', 'internal_ip': f'10.0.0.{i+1}',
                  'rdma_ip': f'10.1.0.{i+1}'} for i in range(6)]
        for count in (2, 3, 5, 6):
            self.assertEqual(gcp_runtime._fleet(hosts[:count]), hosts[:count])
        for count in (0, 1, 4):
            with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                gcp_runtime._fleet(hosts[:count])
    def test_direct_ci_launch_rejects_compact_before_cloud_access(self):
        from unittest.mock import Mock
        api = Mock()
        spec = manual.REVIEWED_SPECS['nvmeof_path_core']
        for candidate in (spec, {key: value for key, value in spec.items() if key != 'owner_hosts'}):
            with self.assertRaisesRegex(ValueError, 'five.owner'):
                gcp_campaign.launch(api, {}, {}, {}, None, {}, candidate, '/unused')
        api.assert_not_called()
        self.assertEqual(api.mock_calls, [])

    def test_dirty_preparation_admits_only_frozen_reviewed_variants(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(prepare, 'checkout'), \
                patch.object(prepare.cloud_package, 'inventory', return_value={}):
            spec_path = Path(directory) / 'spec.json'
            for selection in ('nvmeof_path_compact', 'nvmeof_path_core'):
                spec = copy.deepcopy(manual.REVIEWED_SPECS[selection])
                spec['matrix_sha256'] = 'f' * 64
                spec_path.write_text(json.dumps(spec))
                with self.assertRaisesRegex(ValueError, 'exact reviewed manual spec'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'out', 'a' * 40, None, 1800,
                                    spec_path, manual_allow_dirty=True)
