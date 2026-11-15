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
    def test_focused_spdk_requires_reviewed_target_identity(self):
        identity = {'target_scope': 'file_aio_direct_live_and_sealed',
                    'source_patch_sha256': 'ba21692d7ac70190a505a7b94dc749c8f544d6eef8632a38980f4acf5e987373'}
        for mode in ('focused', 'standard-with-live-nvmeof'):
            spec = {'mode': mode, 'spdk_comparison': True}
            gcp_image.admit_spdk_scope(spec, identity)
            for invalid in (None, {}, {**identity, 'source_patch_sha256': 'f' * 64},
                            {**identity, 'target_scope': 'sealed'}):
                with self.subTest(mode=mode, identity=invalid), self.assertRaises(ValueError):
                    gcp_image.admit_spdk_scope(spec, invalid)

    def test_both_reviewed_specs_bind_exact_canonical_matrix(self):
        for selection, count in (('recommended', 279), ('full', 333), ('device', 120), ('floors', 69), ('stages', 183), ('unified', 270), ('append_floors', 48), ('fair_native', 168), ('parity', 216), ('parity_compact', 177), ('fair_five_owner', 39), ('remote_rw_compact', 342)):
            with self.subTest(selection=selection):
                spec = copy.deepcopy(manual.REVIEWED_SPECS[selection])
                jobs = workflow_input.cloud_campaign.make_focused_matrix(selection)
                digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                self.assertEqual((spec['job_count'], len(jobs), spec['matrix_sha256']), (count, count, digest))
                self.assertEqual(workflow_input.cloud_campaign.validate_focused_job(jobs[0]), jobs[0])
                counts = workflow_input.cloud_campaign.focused_group_counts(selection)
                expected = ({'G12N': 52, 'G12W': 60, 'G12H': 12, 'G12K': 12, 'G12D': 43, 'G12S': 4}
                            if selection in ('stages', 'unified') else
                            {'G14N': 54, 'G14S': 36, 'G14O1': 36, 'G14O5': 36, 'G14P': 6} if selection == 'fair_native' else
                            {'G13W': 48, 'G14N': 54, 'G14S': 36, 'G14O1': 36, 'G14O5': 36, 'G14P': 6} if selection == 'parity' else
                            {'G13W': 48, 'G14N': 54, 'G14S': 36, 'G14O1': 36, 'G14P': 3} if selection == 'parity_compact' else
                            {'G14O5': 36, 'G14P': 3} if selection == 'fair_five_owner' else
                            {'G13W': 48, 'G11N': 45, 'G11S': 24, 'G15F': 24, 'G14N': 54, 'G14O1': 36, 'G14P': 3, 'G15C': 54, 'G15M': 54} if selection == 'remote_rw_compact' else
                            {'G13W': 48} if selection == 'append_floors' else
                            {'G11N': 45, 'G11S': 24} if selection == 'floors' else
                            {'G10R': 60, 'G10W': 60} if selection == 'device' else
                            {'G1': 81, 'G2': 24, 'G3': 6, 'G4': 12, 'G5': 150, 'anchors': 6})
                if selection == 'full':
                    expected.update(G7=48, G9=6)
                if selection == 'unified':
                    expected.update(G11N=45, G11S=24, G11W=18)
                for group, group_count in expected.items():
                    self.assertEqual(counts[group], group_count)
                compact = selection in ('parity_compact', 'remote_rw_compact')
                self.assertEqual(spec.get('owner_hosts', 5), 1 if compact else 5)
                self.assertEqual(workflow_input.cloud_campaign.selection_owner_hosts(selection), spec.get('owner_hosts', 5))
                limit = ((manual.COMPACT_CEILING_CENTS - manual.COMPACT_COMPUTE_CENTS) if compact
                         else manual.CEILING_CENTS - manual.COMPUTE_CENTS)
                manual._check_config(CONFIG, spec, limit)
                with self.assertRaises(ValueError):
                    manual._check_config(CONFIG, spec, limit + 1)
                if compact:
                    with self.assertRaises(ValueError):
                        gcp_campaign.validate_spec(spec)
                else:
                    gcp_campaign.validate_spec(spec)
                with patch.object(workflow_input.cloud_campaign, 'make_focused_matrix', return_value=jobs + [{}]):
                    with self.assertRaisesRegex(ValueError, 'focused matrix differs'):
                        manual._check_config(CONFIG, spec, 0)
                altered = copy.deepcopy(jobs)
                altered[0]['case_id'] += '-tampered'
                with patch.object(workflow_input.cloud_campaign, 'make_focused_matrix', return_value=altered):
                    with self.assertRaisesRegex(ValueError, 'focused matrix differs'):
                        manual._check_config(CONFIG, spec, 0)
                with self.assertRaises(ValueError):
                    manual._reviewed_jobs({**spec, 'job_count': count - 1})

    def test_rejects_topology_and_profile_mutations(self):
        for selection in ('fair_native', 'parity'):
            spec = manual.REVIEWED_SPECS[selection]
            jobs = workflow_input.cloud_campaign.make_focused_matrix(selection)
            for group, field, value in (
                    ('G14S', 'native_shards', 1),
                    ('G14O1', 'owner_count', 5),
                    ('G14O5', 'owner_count', 1),
                    ('G14P', 'concurrency', 5)):
                with self.subTest(selection=selection, group=group):
                    altered = copy.deepcopy(jobs)
                    next(job for job in altered if job['group'] == group)[field] = value
                    with patch.object(workflow_input.cloud_campaign, 'make_focused_matrix', return_value=altered):
                        with self.assertRaisesRegex(ValueError, 'focused matrix differs'):
                            manual._reviewed_jobs(spec)

    def test_frozen_observer_proof_binds_focused_matrix(self):
        spec = manual.REVIEWED_SPECS['recommended']
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
        spec = copy.deepcopy(manual.REVIEWED_SPECS['full'])
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

    def test_fabric_ab_reviewed_spec_and_mutations(self):
        spec = manual.REVIEWED_SPECS['fabric_ab_compact']
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 80)
        self.assertEqual(workflow_input.cloud_campaign.make_matrix(
            {key: value for key, value in spec.items() if key != 'suite'}), jobs)
        self.assertEqual(spec['max_cases'], 80)
        self.assertEqual(spec['owner_hosts'], 1)
        self.assertEqual(spec['backends'], ['tidehunter_tcp', 'tidehunter_tcp_trusted',
                         'tidehunter_verbs_native', 'tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'])
        encoded = ','.join(spec['backends'])
        self.assertEqual(workflow_input.specification('kv', encoded, 'focused', selection='fabric_ab_compact'), spec)
        for mutation in ({'job_count': 81}, {'matrix_sha256': '0' * 64},
                         {'owner_hosts': 5}, {'backends': ['native']}):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                manual._check_config(CONFIG, {**spec, **mutation}, 0)

    def test_leased_compact_exact_spec_and_budget(self):
        spec = copy.deepcopy(manual.REVIEWED_SPECS['leased_ab_compact'])
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 72)
        self.assertFalse(spec['spdk_comparison'])
        self.assertEqual(spec['backends'], ['tidehunter_verbs_native', 'tidehunter_nvme_rdma'])
        manual._check_config(CONFIG, spec, 889)
        with self.assertRaises(ValueError):
            manual._check_config(CONFIG, spec, 890)
        for mutation in ({'job_count': 71}, {'matrix_sha256': '0' * 64}, {'owner_hosts': 5}):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                manual._reviewed_jobs({**spec, **mutation})
        with self.assertRaisesRegex(ValueError, 'five.owner'):
            gcp_campaign._require_six_host_spec(spec)

    def test_nvmeof_path_exact_spec(self):
        for selection in manual.NVMEOF_PATH_SELECTIONS:
            expected_count = {'nvmeof_path_compact': 157, 'nvmeof_path_core': 66, 'nvmeof_path_fix': 37, 'nvmeof_first_put': 12, 'nvmeof_contracts': 34, 'nvmeof_contracts_crossing': 35, 'nvmeof_contracts_crossing2': 36, 'nvmeof_parallel_writes': 44, 'nvmeof_single_caller': 20, 'nvmeof_f6': 42, 'nvmeof_f6b': 58, 'nvmeof_f8': 36, 'nvmeof_f7_highmem32': 8, 'nvmeof_reads_geometry': 74, 'nvmeof_reads_geometry_rest': 20, 'nvmeof_reads_geometry_rest2': 18}[selection]
            with self.subTest(selection=selection):
                spec = copy.deepcopy(manual.REVIEWED_SPECS[selection])
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
                self.assertEqual(len(jobs), expected_count)
                self.assertEqual(spec['max_cases'], expected_count)
                self.assertEqual(spec['owner_hosts'], 1)
                self.assertFalse(spec['spdk_comparison'])
                self.assertEqual(spec['backends'], ['tidehunter_nvme_rdma'] if selection == 'nvmeof_f7_highmem32' else ['native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'])
                self.assertEqual(workflow_input.specification(
                    'kv', ','.join(spec['backends']), 'focused', selection=selection), spec)
                self.assertTrue(gcp_runtime.live_mode(spec))
                for mutation in ({'job_count': expected_count - 1}, {'matrix_sha256': '0' * 64},
                                 {'owner_hosts': 5}, {'spdk_comparison': True},
                                 {'backends': ['tidehunter_verbs_native', 'tidehunter_nvme_rdma']}):
                    with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                        manual._reviewed_jobs({**spec, **mutation})

    def test_workflow_spec_selects_only_reviewed_inputs(self):
        backends = ','.join(manual.REVIEWED_SPECS['full']['backends'])
        for selection in ('recommended', 'full', 'device', 'floors', 'stages', 'unified', 'append_floors', 'fair_native', 'parity', 'parity_compact', 'fair_five_owner', 'remote_rw_compact'):
            selected = ','.join(manual.REVIEWED_SPECS[selection]['backends'])
            spec = workflow_input.specification('kv', selected, 'focused', selection=selection)
            self.assertEqual(spec, manual.REVIEWED_SPECS[selection])
            spec['backends'].clear()
            self.assertTrue(manual.REVIEWED_SPECS[selection]['backends'])
        for suite, names, selection in (('both', backends, 'full'), ('kv', 'native', 'full'),
                                        ('kv', backends, None), ('kv', backends, 'other')):
            with self.subTest(suite=suite, backends=names, selection=selection), self.assertRaises(ValueError):
                workflow_input.specification(suite, names, 'focused', selection=selection)
        with self.assertRaises(ValueError):
            workflow_input.specification('kv', 'native', 'standard', selection='full')

    def test_runtime_admits_only_reviewed_focused_spec(self):
        now = datetime.now(timezone.utc)
        fleet = [{'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': f'run-{i}',
                  'id': str(100 + i), 'zone': 'us-central1-a', 'internal_ip': f'10.0.0.{i+1}',
                  'rdma_ip': f'10.1.0.{i+1}'} for i in range(6)]
        config = {'project_id': 'project', 'project_number': '123', 'bucket': 'bucket',
                  'machine_type': 'z4m-highmem-16', 'run_id': 'run', 'fleet': fleet,
                  'source_sha256': 'a' * 64, 'deadline_utc': (now + timedelta(minutes=5)).isoformat(),
                  'campaign_deadline_utc': (now + timedelta(minutes=2)).isoformat(),
                  'package_root': '/mnt/tidehunter/run/package',
                  'package': {'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                              'object_key': 'runs/run/package.tar', 'generation': '1', 'sha256': 'c' * 64},
                  'spec': copy.deepcopy(manual.REVIEWED_SPECS['full'])}
        self.assertIs(gcp_runtime.validate_runtime(config), config)
        config['spec']['direct_control_arm'] = False
        with self.assertRaises(ValueError):
            gcp_runtime.validate_runtime(config)

    def test_compact_budget_and_runtime_admission(self):
        self.assertEqual((manual.COMPUTE_CENTS, manual.CEILING_CENTS), (11834, 14500))
        self.assertEqual((manual.COMPACT_COMPUTE_CENTS, manual.COMPACT_CEILING_CENTS), (3945, 4834))
        now = datetime.now(timezone.utc)
        hosts = [{'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': f'run-{i}',
                  'id': str(100 + i), 'zone': 'us-central1-a', 'internal_ip': f'10.0.0.{i+1}',
                  'rdma_ip': f'10.1.0.{i+1}'} for i in range(6)]
        for count in (2, 6):
            self.assertEqual(gcp_runtime._fleet(hosts[:count]), hosts[:count])
        for count in (0, 1, 3, 4, 5):
            with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                gcp_runtime._fleet(hosts[:count])
        config = {'project_id': 'project', 'project_number': '123', 'bucket': 'bucket',
                  'machine_type': 'z4m-highmem-16', 'run_id': 'run', 'fleet': hosts[:2],
                  'source_sha256': 'a' * 64, 'deadline_utc': (now + timedelta(minutes=5)).isoformat(),
                  'campaign_deadline_utc': (now + timedelta(minutes=2)).isoformat(),
                  'package_root': '/mnt/tidehunter/run/package',
                  'package': {'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                              'object_key': 'runs/run/package.tar', 'generation': '1', 'sha256': 'c' * 64},
                  'spec': copy.deepcopy(manual.REVIEWED_SPECS['parity_compact'])}
        for selection in ('parity_compact', 'remote_rw_compact', 'fabric_ab_compact'):
            config['spec'] = copy.deepcopy(manual.REVIEWED_SPECS[selection])
            self.assertIs(gcp_runtime.validate_runtime(config), config)
        for mutation in ({'mode': 'standard'}, {'suite': 'sui'}, {'backends': ['tikv']},
                         {'selection': 'parity'}, {'owner_hosts': 5}, {'job_count': 81},
                         {'matrix_sha256': '0' * 64}, {'backends': ['tidehunter_tcp']}):
            with self.subTest(mutation=mutation), self.assertRaises((ValueError, gcp_runtime.RuntimeSafetyError)):
                gcp_runtime.validate_runtime({**config, 'spec': {**config['spec'], **mutation}})

    def test_direct_ci_launch_rejects_compact_before_cloud_access(self):
        from unittest.mock import Mock
        api = Mock()
        spec = manual.REVIEWED_SPECS['parity_compact']
        for candidate in (spec, {key: value for key, value in spec.items() if key != 'owner_hosts'}):
            with self.assertRaisesRegex(ValueError, 'five.owner'):
                gcp_campaign.launch(api, {}, {}, {}, None, {}, candidate, '/unused')
        api.assert_not_called()
        self.assertEqual(api.mock_calls, [])

    def test_planning_estimate_fits_work_window_but_not_worst_case(self):
        work_minutes = (3 * (1069.5 + 240 + 120 + 156.9 + 1904 + 2 * 19.4)
                        + 54 * 40) / 60
        fleet_hours = (22 + 12.6 + 3.8 + 1.2 * work_minutes) / 60
        total = fleet_hours * 6 * 3.287109973 + 26.66
        self.assertAlmostEqual(work_minutes, 212.46)
        self.assertAlmostEqual(fleet_hours, 4.8892)
        recommended_work_minutes = work_minutes - 54 * 40 / 60
        recommended_hours = (22 + 12.6 + 3.8 + 1.2 * recommended_work_minutes) / 60
        self.assertAlmostEqual(recommended_work_minutes, 176.46)
        self.assertAlmostEqual(recommended_hours, 4.1692)
        self.assertLess(recommended_hours * 6 * 3.287109973 + 26.66, manual.CEILING_CENTS / 100)
        self.assertLess(total, manual.CEILING_CENTS / 100)
        work_window = (manual.FLEET_SECONDS - manual.CAMPAIGN_MARGIN_SECONDS - manual.EXPORT_RESERVE_SECONDS) / 60
        self.assertLess(22 + 1.2 * work_minutes, work_window)
        self.assertGreater(333 * manual.REVIEWED_SPECS['full']['case_deadline_seconds'] / 60, work_window)

    def test_unified_planning_estimate_fits_fleet_and_reserve(self):
        suite_minutes = sum((60, 55, 26, 11, 38, 8, 47, 14.5, 7.5))
        fleet_overhead_minutes = 36  # Includes export and teardown.
        fleet_minutes = manual.FLEET_SECONDS / 60
        self.assertEqual(suite_minutes, 267)
        self.assertLess(suite_minutes + fleet_overhead_minutes,
                        fleet_minutes - manual.CAMPAIGN_MARGIN_SECONDS / 60)
        self.assertGreaterEqual(fleet_overhead_minutes * 60, manual.EXPORT_RESERVE_SECONDS)
        spec = manual.REVIEWED_SPECS['unified']
        self.assertLessEqual(spec['job_count'], spec['max_cases'])
        self.assertEqual(spec['case_deadline_seconds'], 600)
        self.assertEqual(spec['export_reserve_seconds'], 900)
        self.assertEqual(manual.FLEET_SECONDS - manual.CAMPAIGN_MARGIN_SECONDS -
                         manual.EXPORT_RESERVE_SECONDS - spec['case_deadline_seconds'], 19980)

    def test_append_floors_planning_estimate_fits_fleet_and_reserve(self):
        spec = manual.REVIEWED_SPECS['append_floors']
        suite_seconds = spec['job_count'] * 40
        fleet_overhead_seconds = 36 * 60  # Includes export and teardown, as in unified.
        self.assertEqual(spec['job_count'], 48)
        self.assertEqual(spec['backends'], manual.REVIEWED_SPECS['floors']['backends'])
        for field in ('max_cases', 'case_deadline_seconds', 'hard_deadline_seconds', 'export_reserve_seconds'):
            self.assertEqual(spec[field], manual.REVIEWED_SPECS['unified'][field])
        self.assertLessEqual(spec['job_count'], spec['max_cases'])
        self.assertGreaterEqual(fleet_overhead_seconds, spec['export_reserve_seconds'])
        self.assertLess(suite_seconds + fleet_overhead_seconds,
                        manual.FLEET_SECONDS - manual.CAMPAIGN_MARGIN_SECONDS)

    def test_dirty_preparation_admits_only_frozen_reviewed_variants(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(prepare, 'checkout'), \
                patch.object(prepare.cloud_package, 'inventory', return_value={}):
            spec_path = Path(directory) / 'spec.json'
            for selection in ('recommended', 'full'):
                spec = copy.deepcopy(manual.REVIEWED_SPECS[selection])
                spec_path.write_text(json.dumps(spec))
                with self.assertRaisesRegex(ValueError, 'SPDK comparison requires'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'out', 'a' * 40, None, 1800,
                                    spec_path, manual_allow_dirty=True)
                spec['matrix_sha256'] = 'f' * 64
                spec_path.write_text(json.dumps(spec))
                with self.assertRaisesRegex(ValueError, 'exact reviewed manual spec'):
                    prepare.prepare(prepare.ROOT, Path(directory) / 'out', 'a' * 40, None, 1800,
                                    spec_path, manual_allow_dirty=True)
