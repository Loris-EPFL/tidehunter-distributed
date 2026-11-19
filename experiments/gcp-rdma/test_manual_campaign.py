import asyncio
from collections import Counter
import copy
from decimal import Decimal
import hashlib
import http.client
import json
import shutil
import ssl
import subprocess
from pathlib import Path
import tempfile
import threading
import time
import urllib.error
import unittest
from unittest.mock import patch

import gcp_campaign
import gcp_image_receipt
from gcp_api import ApiError
import manual_campaign as manual


CONFIG = {'project_id': 'mysten-research', 'project_number': '514867804006',
          'bucket': 'mysten-research-bench-artifacts',
          'service_account': gcp_campaign.GUEST_SERVICE_ACCOUNT, 'machine_type': 'z4m-highmem-16',
          'max_distance': 3, 'candidates': [{'zone': 'us-central1-a',
          'ordinary_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/default',
          'rdma_subnet': 'projects/mysten-research/regions/us-central1/subnetworks/rdma'}]}

# Projection of the actual F7 v3 regional response, control-log/0008.json.
QUOTAS = [
    {'metric': 'CPUS', 'limit': 5000.0, 'usage': 4.0},
    {'metric': 'IN_USE_ADDRESSES', 'limit': 2300.0, 'usage': 2.0},
    {'metric': 'LOCAL_SSD_TOTAL_GB', 'limit': 9.223372036854776e+18, 'usage': 0.0},
    {'metric': 'INSTANCES', 'limit': 24000.0, 'usage': 3.0},
]

SPDK_IDENTITY = {
    'target_scope': 'file_aio_direct_live_and_sealed',
    'source_patch_sha256': 'ba21692d7ac70190a505a7b94dc749c8f544d6eef8632a38980f4acf5e987373',
}


class Store:
    def __init__(self):
        self.objects = {}
        self.next_generation = 1
        self.downloads = []

    def put(self, key, data, *, if_generation_match=None, **kwargs):
        old = self.objects.get(key)
        if if_generation_match is not None and str(if_generation_match) != str(old[0] if old else 0):
            raise ApiError(412, 'generation mismatch')
        generation = str(self.next_generation)
        self.next_generation += 1
        self.objects[key] = (generation, bytes(data), hashlib.sha256(data).hexdigest())
        return {'generation': generation, 'metadata': {'sha256': self.objects[key][2]}}

    def head(self, key):
        if key not in self.objects:
            raise ApiError(404, 'missing')
        generation, data, sha = self.objects[key]
        return {'name': key, 'generation': generation, 'size': len(data), 'metadata': {'sha256': sha}}

    def get(self, key, *, generation=None, max_bytes=None):
        meta = self.head(key)
        if generation is not None and str(generation) != meta['generation']:
            raise ApiError(404, 'generation missing')
        data = self.objects[key][1]
        if max_bytes is not None and len(data) > max_bytes:
            raise ValueError('oversized')
        return data

    def upload(self, key, path, **kwargs):
        return self.put(key, Path(path).read_bytes(), **kwargs)

    def download(self, key, path, *, generation=None, max_bytes=None):
        self.downloads.append((key, str(generation)))
        Path(path).write_bytes(self.get(key, generation=generation, max_bytes=max_bytes))

    def list(self, prefix):
        return iter([self.head(key) for key in sorted(self.objects) if key.startswith(prefix)])

    def delete(self, key, *, if_generation_match):
        if self.head(key)['generation'] != str(if_generation_match):
            raise ApiError(412, 'generation mismatch')
        del self.objects[key]


def parent():
    return {'run_id': 'bench-' + '1' * 16, 'project_id': CONFIG['project_id'],
            'source_sha256': 'a' * 64, 'deadline': manual._utc(time.time() + 20000),
            'children': {}}


def publish_export(store, state, status='incomplete'):
    prefix = state['run_id'] + '/results/'
    archive, inventory = b'partial archive', b'{}'
    store.put(prefix + 'controller.tar.gz', archive)
    meta = store.put(prefix + 'controller-inventory.json', inventory)
    package = {'binary_blake2b512': 'b' * 128, 'manifest_sha256': 'c' * 64}
    receipt = {'schema_version': 2, 'status': status, 'source_sha256': state['source_sha256'],
               'binary_blake2b512': package['binary_blake2b512'],
               'key': prefix + 'controller.tar.gz', 'bytes': len(archive),
               'sha256': hashlib.sha256(archive).hexdigest(),
               'inventory': {'key': prefix + 'controller-inventory.json', 'bytes': len(inventory),
                             'sha256': hashlib.sha256(inventory).hexdigest(),
                             'generation': meta['generation']}}
    store.put(prefix + 'receipt.json', json.dumps(receipt).encode())
    return package


class ManualCampaignTest(unittest.TestCase):
    def test_larger_reviewed_machine_uses_its_compute_allowance(self):
        config = {**CONFIG, 'machine_type': 'z4m-highmem-32'}
        spec = {**manual.REVIEWED_SPEC, 'machine_type': 'z4m-highmem-32'}
        manual._check_config(config, spec, 2666)
        with self.assertRaisesRegex(ValueError, 'machine type differs'):
            manual._check_config(config, manual.REVIEWED_SPEC, 0)

    def test_existing_standard_matrix_hash_is_unchanged(self):
        jobs = manual._reviewed_jobs(manual.REVIEWED_SPEC)
        self.assertEqual(len(jobs), 652)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(',', ':')).encode()).hexdigest(),
                         'f48f60156261589c539ca030d535d80cba8a0d987df30184d597a419f82adb9d')

    def test_reviewed_contract_spec_exact_admission_and_matrix(self):
        spec = manual.REVIEWED_CONTRACT_SPEC
        self.assertNotIn('valkey_reproduction_profiles', spec)
        self.assertFalse(spec['spdk_comparison'])
        self.assertTrue(spec['native_durability_cohort'])
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 156)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(',', ':')).encode()).hexdigest(),
                         '32cda41becb0128f3d7f50cf02a4ffc4c9c9650ce9e708226516d862e1cca8e2')
        self.assertEqual(Counter(job['backend'] for job in jobs), {
            'native': 26, 'tidehunter_verbs_native': 26, 'tidehunter_nvme_rdma': 26,
            'valkey_tcp': 17, 'valkey_rdma': 35, 'kvrocks': 26})
        for key in spec:
            changed = copy.deepcopy(spec)
            value = changed[key]
            changed[key] = (not value if isinstance(value, bool) else value + 1 if isinstance(value, int)
                            else value + '-changed' if isinstance(value, str) else None)
            with self.subTest(key=key), self.assertRaises(ValueError):
                manual._reviewed_jobs(changed)

    def test_reviewed_contract_mid_spec_exact_admission_and_matrix(self):
        spec = manual.REVIEWED_CONTRACT_MID_SPEC
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 182)
        digest = lambda value: hashlib.sha256(json.dumps(
            value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest(spec), manual.REVIEWED_CONTRACT_MID_SPEC_SHA256)
        self.assertEqual(digest(jobs), manual.REVIEWED_CONTRACT_MID_MATRIX_SHA256)
        sealed = {**spec, 'reservation_hours': 2, 'hard_deadline_seconds': 7200,
                  'machine_type': 'z4m-highmem-16'}
        self.assertEqual(digest(sealed), manual.REVIEWED_CONTRACT_MID_SEALED_SPEC_SHA256)
        self.assertEqual(manual._reviewed_jobs(sealed), jobs)
        self.assertEqual({key: value for key, value in spec.items()
                          if key not in ('backends', 'arms', 'value_sizes')},
                         {key: value for key, value in manual.REVIEWED_CONTRACT_SPEC.items()
                          if key not in ('backends', 'arms', 'value_sizes')})
        self.assertEqual(spec['backends'], ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                                          'tidehunter_nvme_rdma', 'valkey_tcp', 'valkey_rdma', 'kvrocks'])
        self.assertEqual(spec['arms'], {**manual.REVIEWED_CONTRACT_SPEC['arms'],
                                       'tidehunter_tcp': ['synchronous', 'native']})
        self.assertEqual({job['value_size'] for job in jobs}, {512, 4096})
        self.assertEqual({job['concurrency'] for job in jobs}, {1, 16})
        self.assertEqual({job['repeat'] for job in jobs}, {0, 1})
        old_jobs = manual._reviewed_jobs(manual.REVIEWED_CONTRACT_SPEC)
        expected = []
        for old in old_jobs:
            size = {1024: 512, 65536: 4096}[old['value_size']]
            expected.append({**old, 'value_size': size,
                             'case_id': old['case_id'].split('-', 1)[1].replace(
                                 f"-{old['value_size']}-", f"-{size}-")})
        retained = [{**job, 'case_id': job['case_id'].split('-', 1)[1]}
                    for job in jobs if job['backend'] != 'tidehunter_tcp']
        self.assertEqual(sorted(map(manual._json, expected)), sorted(map(manual._json, retained)))
        tcp = [job for job in jobs if job['backend'] == 'tidehunter_tcp']
        self.assertEqual(len(tcp), 26)
        def comparable(job):
            return {key: value for key, value in job.items() if key not in ('backend', 'case_id')}
        self.assertEqual(sorted(manual._json(comparable(job)) for job in tcp),
                         sorted(manual._json(comparable(job)) for job in jobs
                                if job['backend'] == 'tidehunter_verbs_native'))
        for job in jobs:
            self.assertEqual(job['coordinator_cache_bytes'], 0)
            self.assertEqual(job['keys'], 4096)
        for backend in ('tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'):
            self.assertEqual({job['durability_tier'] for job in jobs
                              if job['backend'] == backend}, {'native', 'synchronous'})
        for key in spec:
            changed = copy.deepcopy(spec)
            changed[key] = None
            with self.subTest(key=key), self.assertRaises(ValueError):
                manual._reviewed_jobs(changed)

    def test_contract_mid_reservation_and_export_envelope(self):
        spec = manual.REVIEWED_CONTRACT_MID_SPEC
        self.assertEqual(manual.reservation_quote({**spec, 'reservation_hours': 2},
                                                 'z4m-highmem-16'), (7200, 3945, 4834))
        jobs = manual._reviewed_jobs(spec)
        estimate = sum((job['keys'] + job['warmup_operations'] + job['measurement_operations'])
                       * 256 + 8 * 1024**2 for job in jobs) + 256 * 1024**2
        self.assertEqual(estimate, 2961539072)
        self.assertLessEqual(estimate, 3_000_000_000)

    def test_contract_mid_sizes_require_the_contract_shape(self):
        cloud = manual.workflow_input.cloud_campaign
        base = {key: value for key, value in manual.REVIEWED_CONTRACT_MID_SPEC.items()
                if key != 'suite'}
        for sizes in ([512], [4096, 512], [512, 4096, 4096], [512, 65536]):
            with self.subTest(sizes=sizes), self.assertRaises(ValueError):
                cloud.make_matrix({**base, 'value_sizes': sizes})
        with self.assertRaises(ValueError):
            cloud.make_matrix({key: value for key, value in base.items()
                               if key not in ('shapes', 'arms')})

    def test_nvmeof_path_reviewed_spec_is_bounded(self):
        for selection in manual.NVMEOF_PATH_SELECTIONS:
            expected_count = manual.workflow_input.cloud_campaign.FOCUSED_COUNTS[selection]
            with self.subTest(selection=selection):
                spec = manual.REVIEWED_SPECS[selection]
                self.assertEqual(spec['max_cases'], expected_count)
                self.assertEqual(spec['backends'],
                                 ['tidehunter_nvme_rdma'] if selection in manual.workflow_input.cloud_campaign.M3_SELECTIONS or selection == 'nvmeof_f7_highmem32' else ['native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'])
                self.assertFalse(spec['spdk_comparison'])
                self.assertEqual(spec['matrix_sha256'], manual.FOCUSED_MATRIX_SHA256[selection])
                self.assertEqual(spec['matrix_sha256'], {
                    'nvmeof_path_compact': '8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036',
                    'nvmeof_path_core': 'f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55',
                    'nvmeof_path_fix': 'aa94a3842d196bd9cdb486b0c746e9939455b25d4d152e8127c1aba51f9ef606',
                    'nvmeof_first_put': 'bc17406e8712b9ad7f49662554bcca316cff7c87774216c39c5cc42aebe1efdc',
                    'nvmeof_contracts_crossing2': '8dde34953df300c798b0a5605abf393d310bd6e321c3568c861cfdb27caab295',
                    'nvmeof_parallel_writes': '64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4',
                    'nvmeof_single_caller': '294b50957b21d6acafadcf1b682c71d1b424524a144ecd6c2c18e2b1cc40fd4e',
                    'nvmeof_f6': '1186674f1706550f0faaac6d7852dcee711b43dba8c87b22be4dd071d768e8a7',
                    'nvmeof_f6b': 'f5e680e6959f3a74369bae97c21354b6dc2911f0b6e15f048c6e7172e4c823de',
                    'nvmeof_f8': '30e7121a926003ebe18e691425db063c4093a942efdf5528913003720b9d5e1a',
                    'nvmeof_m3_t1': 'df343feef5fc4568d44e98f28ad1486f5882e559ac23d935ea6a4ac4b5a5ac21',
                    'nvmeof_m3_t2_2': '115bc93da0f8bca91cd1c5c09214e9ef5c4d91153f940218f7fef0cf0fff548a',
                    'nvmeof_m3_t2_5': '9ec5b0dcf21131e3e4c1296862561a2660389b30aed0cbac18f548cc18cbf094',
                    'nvmeof_p9_small_put': 'e4c667a6080f8cc19efc3b35ba4ed7120d864a7d20c0eca12b998e753095f176',
                    'nvmeof_p9_write_engine': '7e9089717674ec2980a652984005bf6c210a52a21d94e3ba393e12ed47b275b9',
                    'nvmeof_p13_write_engine': '674cd71e117824550abb4624747e462b739cb9535912b5a6415782e186e03d18',
                    'nvmeof_p16_redrain_ab': '33349d644ad5295f39bc88d23d0fcd8dbf088a6fb9e2dec429545a792f631808',
                    'nvmeof_q0': 'ec7b1e585125b53a952b97f8da54b4fb10376c9b54db1f45163c8d442b9a82ec',
                    'nvmeof_f7_highmem32': '25ef27bc334edc0d07a84061427893063e162f16f8f1a986ea99d758ca2c05dd',
                    'nvmeof_reads_geometry': 'c27b1598f34949d642d3cc518985a2f92f5c388e77e2742da9ce780ce8b41aea',
                    'nvmeof_reads_geometry_rest': 'b5db99b28d0faf8386e5b885e25c55d50b74f732e9b220ef552bd6c90a761935',
                    'nvmeof_reads_geometry_rest2': 'c80e079b256fad0c765dc66fec387cdb7bac160c90aa0747ac184347cb5bbed4',
                    'nvmeof_contracts_crossing': '7cf192750aa556071a73aaf3c1f19ce8f7b73c4baa51f3abfc46816896620bfa',
                    'nvmeof_contracts': 'cad4e58cc1e01a4977f7b7e3e0c87ed6ff00cfb15d498959472a511a8bab0208',
                }[selection])

    def test_state_save_retries_transient_with_original_generation(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                store, state = Store(), parent()
                path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
                calls = []
                def put(key, data, **kwargs):
                    calls.append(kwargs['if_generation_match'])
                    if len(calls) == 1:
                        raise ApiError(status, 'temporary')
                    return Store.put(store, key, data, **kwargs)
                with patch.object(store, 'put', side_effect=put), \
                        patch.object(gcp_campaign.time, 'sleep') as sleep, \
                        patch.object(gcp_campaign.random, 'uniform', return_value=1):
                    gcp_campaign.save(path, state)
                self.assertEqual(calls, [0, 0])
                sleep.assert_called_once_with(1)
                self.assertEqual(json.loads(path.path.read_text()), state)

    def test_state_save_exhaustion_and_conflict_fail_closed(self):
        for status, attempts in ((429, 8), (403, 1), (412, 1)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                store, state = Store(), parent()
                path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
                with patch.object(store, 'put', side_effect=ApiError(status, 'failure')) as put, \
                        patch.object(gcp_campaign.time, 'sleep'), \
                        patch.object(gcp_campaign.random, 'uniform', return_value=1):
                    with self.assertRaises(ApiError):
                        gcp_campaign.save(path, state)
                self.assertEqual(put.call_count, attempts)
                self.assertTrue(all(call.kwargs['if_generation_match'] == 0 for call in put.call_args_list))
                self.assertEqual(path.generation, 0)
                self.assertFalse(path.path.exists())

    def test_state_save_reconciles_ambiguous_committed_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            store, state = Store(), parent()
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            def committed_then_failed(key, data, **kwargs):
                Store.put(store, key, data, **kwargs)
                raise ApiError(503, 'response lost')
            with patch.object(store, 'put', side_effect=committed_then_failed) as put, \
                    patch.object(gcp_campaign.time, 'sleep') as sleep:
                gcp_campaign.save(path, state)
            put.assert_called_once()
            sleep.assert_not_called()
            self.assertEqual(path.generation, '1')
            self.assertEqual(json.loads(path.path.read_text()), state)

    def test_module_admission_rejects_other_guests_and_unloaded_candidates(self):
        candidate = {'candidate_srcversion': 'candidate-src', 'candidate_build_id': 'candidate-build'}
        state = dict(parent(), owner_nonce='1' * 32)
        host = {'name': state['run_id'] + '-0', 'id': '123'}
        loaded = {**candidate, 'loaded_nvmet': {'srcversion': 'candidate-src', 'build_id': 'candidate-build'},
                  'guest': {'instance_name': host['name'], 'instance_id': host['id'],
                            'run_id': state['run_id'], 'owner_nonce': state['owner_nonce']}}
        corruptions = [('loaded_nvmet', 'build_id', 'stock-build'),
                       ('loaded_nvmet', 'srcversion', 'stock-src'),
                       ('guest', 'instance_id', '124'), ('guest', 'run_id', 'other-run'),
                       ('guest', 'owner_nonce', '2' * 32)]
        for section, key, wrong in corruptions:
            bad = copy.deepcopy(loaded)
            bad[section][key] = wrong
            with self.subTest(section=section, field=key), self.assertRaisesRegex(ValueError, 'loaded identity'):
                manual._admit_nvmet_proof({'candidate': candidate, 'loaded': bad}, candidate, host, state)
        with self.assertRaisesRegex(ValueError, 'candidate proof'):
            manual._admit_nvmet_proof(None, candidate, host, state)
        with self.assertRaisesRegex(ValueError, 'unrequested'):
            manual._admit_nvmet_proof({'candidate': candidate, 'loaded': loaded}, None, host, state)

    def test_bounded_quote_and_exact_matrix_are_admitted(self):
        spec = json.loads(json.dumps(manual.REVIEWED_SPEC))
        with patch.object(manual.workflow_input.cloud_campaign, 'make_matrix', return_value=[{}] * 473):
            manual._check_config(CONFIG, spec, manual.CEILING_CENTS - manual.COMPUTE_CENTS)
            with self.assertRaisesRegex(ValueError, 'ceiling'):
                manual._check_config(CONFIG, spec, manual.CEILING_CENTS - manual.COMPUTE_CENTS + 1)
            with self.assertRaises(ValueError):
                manual._check_config(dict(CONFIG, max_distance=2), spec, 0)
            with self.assertRaisesRegex(ValueError, 'specification'):
                manual._check_config(CONFIG, dict(spec, backends=['native', 'valkey_rdma']), 0)
        host_seconds = 6 * manual.FLEET_SECONDS
        compute_cents = Decimal("3.287109973") * host_seconds / 3600 * 100
        self.assertGreaterEqual(manual.COMPUTE_CENTS, compute_cents)
        self.assertLess(manual.COMPUTE_CENTS, compute_cents + 1)
        self.assertEqual(manual.COMPUTE_CENTS, 11834)
        self.assertEqual(manual.CEILING_CENTS, 14500)
        self.assertGreaterEqual(manual.CEILING_CENTS - manual.COMPUTE_CENTS, 1000)
        self.assertEqual(manual.FLEET_SECONDS, 6 * 3600)
        self.assertEqual(spec['hard_deadline_seconds'], manual.FLEET_SECONDS)
        self.assertEqual((manual.PREPARATION_SECONDS, manual.PAIR_SECONDS,
                          manual.WORKER_READY_SECONDS, manual.EXPORT_RESERVE_SECONDS),
                         (900, 1200, 180, 900))
        self.assertEqual(manual.PAIR_CLEANUP_SECONDS, 300)
        self.assertEqual(manual.qualify.M3_PAIR_SECONDS + manual.PAIR_CLEANUP_SECONDS, 780)
        with self.assertRaisesRegex(ValueError, 'specification'):
            manual._check_config(CONFIG, dict(spec, hard_deadline_seconds=21601), 0)

    def test_reviewed_spec_generates_complete_real_matrix(self):
        jobs = manual.workflow_input.cloud_campaign.make_matrix(
            {key: value for key, value in manual.REVIEWED_SPEC.items() if key != 'suite'})
        previous_spec = json.loads(json.dumps(manual.REVIEWED_SPEC))
        previous_spec['diagnostics'].pop('valkey_profile_limit')
        previous = manual.workflow_input.cloud_campaign.make_matrix(
            {key: value for key, value in previous_spec.items() if key != 'suite'})
        self.assertEqual(len(jobs) - len(previous), 12)
        self.assertEqual({job['case_id'] for job in previous},
                         {job['case_id'] for job in jobs if job['cohort'] != 'valkey_client_diagnostics'})
        self.assertEqual({job['backend'] for job in jobs}, set(manual.REVIEWED_SPEC['backends']))
        live = [job for job in jobs if job.get('nvmeof_route') == 'live']
        kernel = [job for job in live if job.get('nvmeof_target', 'kernel') == 'kernel']
        spdk = [job for job in live if job.get('nvmeof_target') == 'spdk']
        self.assertEqual((len(kernel), len(spdk)), (52, 52))
        self.assertTrue(all(job['backend'] == 'tidehunter_nvme_rdma' for job in live))
        def workload(job):
            return json.dumps({key: value for key, value in job.items()
                               if key not in ('case_id', 'nvmeof_target')}, sort_keys=True)
        self.assertEqual(Counter(map(workload, kernel)), Counter(map(workload, spdk)))
        native = [job for job in jobs if job.get('durability_tier') == 'native']
        self.assertEqual(len(native), 116)
        self.assertEqual({job['backend'] for job in native},
                         {'tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'})
        self.assertTrue(all(job['case_id'].endswith('-durability-native') for job in native))
        self.assertTrue(all(job['sealed_dataset']['source_id'].endswith('-durability-native')
                            for job in native if 'sealed_dataset' in job))
        self.assertTrue(all(job['mix']['put'] == job['mix']['put_batch'] == 0
                            for job in native if job.get('nvmeof_route') == 'sealed_read'))
        self.assertEqual(len({job['case_id'] for job in jobs}), len(jobs))

    def test_expansion_preserves_original_jobs_and_external_standard_profiles(self):
        base = json.loads(json.dumps(manual.REVIEWED_SPEC))
        added = {'tikv', 'cassandra', 'foundationdb', 'kvrocks', 'garnet'}
        base['backends'] = [backend for backend in base['backends'] if backend not in added]
        base.pop('native_durability_cohort')
        generate = manual.workflow_input.cloud_campaign.make_matrix
        old = generate({key: value for key, value in base.items() if key != 'suite'})
        new = generate({key: value for key, value in manual.REVIEWED_SPEC.items() if key != 'suite'})
        def identity(job):
            return json.dumps({key: value for key, value in job.items() if key != 'case_id'}, sort_keys=True)
        original = [job for job in new if job['backend'] not in added
                    and job.get('durability_tier') != 'native']
        self.assertEqual(Counter(map(identity, old)), Counter(map(identity, original)))
        external = [job for job in new if job['backend'] in added]
        self.assertEqual(Counter(job['backend'] for job in external), Counter({name: 25 for name in added}))

    def test_pair_rejects_missing_live_proof_before_fleet(self):
        hosts = [{'name': 'pair-a', 'rdma_ip': '10.1.0.1', 'internal_ip': '10.0.0.1'},
                 {'name': 'pair-b', 'rdma_ip': '10.1.0.2', 'internal_ip': '10.0.0.2'}]
        with self.assertRaisesRegex(ValueError, 'qualification proof missing'):
            manual._admit_live_pair([{}, {}], hosts, manual.REVIEWED_SPEC, 'irdma0')

    def test_dirty_snapshot_requires_exact_truthful_full_inventory(self):
        revision = 'a' * 40
        names = ('manual_campaign.py', 'gcp_manual_compute.py', 'gcp_manual_image.py',
                 'gcp_manual_storage.py', 'gcp_manual_preflight.py', 'gcp_campaign.py',
                 'gcp_runtime.py', 'gcp_lease.py', 'gcp_qualify_bootstrap.py', 'gcp_live_qualification.py',
                 'qualify_observer_matrix.py')
        hashes = {'experiments/gcp-rdma/' + name: 'b' * 64 for name in names}
        source = {'revision': revision, 'branch': 'work', 'dirty': True, 'sha256': hashes}

        def git(argv, *, cwd):
            if argv[1] == 'rev-parse':
                return revision
            if argv[1] == 'branch':
                return 'work'
            return ' M source.rs'

        with patch.object(manual.prepare.cloud_package, 'command', side_effect=git), \
             patch.object(manual.prepare.cloud_package, 'inventory', return_value=hashes):
            manual._check_snapshot(source, revision)
            with self.assertRaisesRegex(ValueError, 'checkout'):
                manual._check_snapshot(dict(source, dirty=False), revision)
            with self.assertRaisesRegex(ValueError, 'checkout'):
                manual._check_snapshot(dict(source, sha256={}), revision)
            with self.assertRaisesRegex(ValueError, 'checkout'):
                manual._check_snapshot(dict(source, revision='c' * 40), revision)

    def test_manual_package_checks_directory_archive_and_qualification_identity(self):
        revision = 'a' * 40
        verified = {'source_sha256': 'b' * 64, 'binary_sha256': 'c' * 64,
                    'binary_blake2b512': 'd' * 128, 'manifest_sha256': 'e' * 64, 'abi': {},
                    'spdk': dict(SPDK_IDENTITY)}
        proof = {'engine_sha': revision, 'binary_sha256': verified['binary_sha256'],
                 'binary_blake2b512': verified['binary_blake2b512']}
        source = {'dirty': True}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'package'
            root.mkdir()
            (root / 'bundle.tar.gz').write_bytes(b'archive')
            files = ('runtime/spdk/spdk.json', 'runtime/spdk/bin/nvmf_tgt',
                     'source/experiments/nvmeof/spdk_targets.py',
                     'source/experiments/nvmeof/spdk_artifacts.py',
                     'source/experiments/nvmeof/spdk_measurement.py',
                     'source/experiments/nvmeof/live_spdk_daemon.py',
                     'source/experiments/kv-comparison/nvme_campaign_owner.py',
                     'runtime/bin/valkey-benchmark', 'runtime/valkey-rdma.json',
                     'source/experiments/kv-comparison/valkey_cloud.py',
                     'source/experiments/kv-comparison/valkey_reproduction.py',
                     'source/experiments/kv-comparison/valkey-public-reproduction.json')
            (root / 'manifest.json').write_text(json.dumps({
                'features': ['valkey-rdma', 'tikv', 'cassandra', 'foundationdb', 'garnet', 'kvrocks'],
                'files': dict.fromkeys(files, {})}))
            receipt = {**verified, 'source_revision': revision, 'qualification': proof,
                       'sha256': hashlib.sha256(b'archive').hexdigest()}
            (Path(tmp) / 'package-receipt.json').write_text(json.dumps(receipt))
            package = manual.prepare.cloud_package
            with patch.object(package, 'verify_directory', return_value=verified), \
                 patch.object(package, 'admit_source', return_value=source), \
                 patch.object(package, 'verify_archive', return_value=verified) as archive_check, \
                 patch.object(manual, '_check_snapshot') as snapshot_check, \
                 patch.object(manual.prepare.ci_prepare, 'admit_qualification') as qualify_check:
                admitted, actual_source = manual._admit_package(tmp, revision, manual.REVIEWED_SPEC)
                self.assertEqual(actual_source, source)
                self.assertEqual(admitted['archive_path'], str((root / 'bundle.tar.gz').resolve()))
                self.assertEqual(snapshot_check.call_count, 2)
                archive_check.assert_called_once()
                qualify_check.assert_called_once()
            focused_verified = {**verified, 'build_profile': 'focused'}
            with patch.object(package, 'verify_directory', return_value=focused_verified), \
                 patch.object(package, 'admit_source', return_value=source), \
                 patch.object(package, 'verify_archive', return_value=focused_verified), \
                 patch.object(manual, '_check_snapshot'), \
                 patch.object(manual.prepare.ci_prepare, 'admit_qualification'):
                focused_spec = manual.REVIEWED_SPECS['recommended']
                with self.assertRaisesRegex(ValueError, 'must not contain Valkey'):
                    manual._admit_package(tmp, revision, focused_spec)
                focused_files = {name: {} for name in files
                                 if not name.startswith('runtime/bin/valkey-') and name != 'runtime/valkey-rdma.json'}
                (root / 'manifest.json').write_text(json.dumps({'build_profile': 'focused',
                    'features': package.FOCUSED_FEATURES, 'files': focused_files}))
                manual._admit_package(tmp, revision, focused_spec)
                tools_spec = manual.REVIEWED_SPECS['nvmeof_f9_tools']
                with self.assertRaisesRegex(ValueError, 'require sealed runtime/bin/admission-options'):
                    manual._admit_package(tmp, revision, tools_spec)
                focused_files['runtime/bin/admission-options'] = {}
                (root / 'manifest.json').write_text(json.dumps({'build_profile': 'focused',
                    'features': package.FOCUSED_FEATURES, 'files': focused_files}))
                manual._admit_package(tmp, revision, tools_spec)
            for identity in (None, {**SPDK_IDENTITY, 'source_patch_sha256': '0' * 64},
                             {**SPDK_IDENTITY, 'target_scope': 'sealed_read_only_file_aio_direct'}):
                with patch.object(package, 'verify_directory', return_value={**verified, 'spdk': identity}), \
                     self.assertRaisesRegex(ValueError, 'reviewed writable target'):
                    manual._admit_package(tmp, revision, manual.REVIEWED_SPEC)
            with patch.object(package, 'verify_directory', return_value=verified), \
                 patch.object(package, 'admit_source', return_value=source), \
                 patch.object(package, 'verify_archive', return_value=dict(verified, binary_sha256='f' * 64)), \
                 patch.object(manual, '_check_snapshot'):
                with self.assertRaisesRegex(ValueError, 'archive differs'):
                    manual._admit_package(tmp, revision, manual.REVIEWED_SPEC)

    def test_child_phase_is_durable_with_nonce_and_no_retry(self):
        store, state = Store(), parent()
        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            gcp_campaign.save(path, state)
            before = int(time.time()) + 0.145358
            with patch.object(manual.time, 'time', return_value=before):
                child, child_path = manual._new_child(state, path, 'bake', 900)
            observed = gcp_campaign.parse_time(child['deadline']).isoformat(timespec='milliseconds')
            vm = {'scheduling': {'terminationTime': observed, 'instanceTerminationAction': 'DELETE'}}
            self.assertTrue(gcp_campaign.has_delete_expiry(vm, child['deadline']))
            self.assertLessEqual(gcp_campaign.parse_time(child['deadline']).timestamp(), before + 900)
            self.assertRegex(child['owner_nonce'], '^[0-9a-f]{32}$')
            self.assertNotEqual(child['run_id'], state['run_id'])
            child['instances'].append({'name': child['run_id'] + '-0', 'uncertain': True})
            gcp_campaign.save(child_path, child)
            durable = json.loads(store.get(path.key))
            self.assertTrue(durable['children']['bake']['instances'][0]['uncertain'])
            self.assertEqual(json.loads((Path(tmp) / 'state.json').read_text()), durable)
            with self.assertRaisesRegex(RuntimeError, 'retry'):
                manual._new_child(state, path, 'bake', 900)

    def test_private_capsule_cleanup_requires_exact_identity_and_generation(self):
        store, child = Store(), parent()
        child['package'] = {'manifest_sha256': 'b' * 64}
        key = 'runs/' + child['run_id'] + '/private-live-rdma.json'
        identity = {'run_id': child['run_id'], 'source_sha256': child['source_sha256'],
                    'deadline_utc': child['deadline'], 'manifest_sha256': 'b' * 64}
        store.put(key, json.dumps({'identity': dict(identity, run_id='foreign')}).encode())
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            manual._cleanup_live_capsules(store, child)
        self.assertIn(key, store.objects)
        store.put(key, json.dumps({'identity': identity}).encode())
        manual._cleanup_live_capsules(store, child)
        self.assertNotIn(key, store.objects)

    def test_gate_checks_exact_vm_source_and_deadline_before_execution(self):
        state = parent()
        gate = manual._gate(CONFIG['bucket'], state)
        for fragment in ("meta('instance/id')", "gate.get('instance_id') != instance",
                         "gate.get('source_sha256') != c['source']",
                         "gate.get('deadline_utc') != c['deadline']", 'hashlib.sha256(script)',
                         "subprocess.run(['/bin/bash']"):
            self.assertIn(fragment, gate)

    def test_source_bound_staging_pins_archive_and_wheel_generations(self):
        store, state = Store(), parent()
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / 'bundle.tar.gz'
            archive.write_bytes(b'package')
            wheel = Path(tmp) / 'PyYAML-6.0.2-cp311-cp311-manylinux_2_17_x86_64.whl'
            wheel.write_bytes(b'wheel')
            receipt = {'archive_path': str(archive), 'sha256': hashlib.sha256(b'package').hexdigest(),
                       'source_sha256': state['source_sha256']}
            package, prep_sha = manual._stage(store, CONFIG['bucket'], state, receipt,
                [{'path': str(wheel), 'sha256': hashlib.sha256(b'wheel').hexdigest()}], qualification=True)
            prefix = 'runs/' + state['run_id'] + '/'
            self.assertEqual(store.get(package['object_key'], generation=package['generation']), b'package')
            raw = store.get(prefix + 'preparation.json')
            self.assertEqual(hashlib.sha256(raw).hexdigest(), prep_sha)
            descriptor = json.loads(raw)['wheels'][0]
            self.assertEqual(store.get(descriptor['object_key'], generation=descriptor['generation']), b'wheel')
            self.assertEqual(json.loads(store.get(prefix + 'runtime.json'))['package'], package)
            with self.assertRaises(ValueError):
                manual._stage(Store(), CONFIG['bucket'], state, dict(receipt, sha256='f' * 64))

    def test_terminal_export_rejects_foreign_or_unbound_objects(self):
        store, state = Store(), parent()
        package = {'binary_blake2b512': 'b' * 128}
        prefix = state['run_id'] + '/results/'
        archive, inventory = b'archive', b'{}'
        store.put(prefix + 'controller.tar.gz', archive)
        inventory_meta = store.put(prefix + 'controller-inventory.json', inventory)
        receipt = {'schema_version': 2, 'status': 'completed', 'source_sha256': state['source_sha256'],
                   'binary_blake2b512': package['binary_blake2b512'], 'key': prefix + 'controller.tar.gz',
                   'bytes': len(archive), 'sha256': hashlib.sha256(archive).hexdigest(),
                   'inventory': {'key': prefix + 'controller-inventory.json', 'bytes': len(inventory),
                                 'sha256': hashlib.sha256(inventory).hexdigest(),
                                 'generation': inventory_meta['generation']}}
        key = prefix + 'receipt.json'
        store.put(key, json.dumps(receipt).encode())
        pinned = manual._terminal_export(store, state, package)
        self.assertEqual(set(pinned), {key, prefix + 'controller.tar.gz', prefix + 'controller-inventory.json'})
        for field, value in (('source_sha256', 'c' * 64), ('status', 'incomplete'),
                             ('binary_blake2b512', 'd' * 128), ('key', 'foreign/results/controller.tar.gz')):
            changed = dict(receipt, **{field: value})
            store.put(key, json.dumps(changed).encode())
            with self.assertRaisesRegex(ValueError, 'provenance'):
                manual._terminal_export(store, state, package)
        store.put(key, json.dumps(receipt).encode())
        store.put(prefix + 'controller.tar.gz', b'corrupt')
        with self.assertRaisesRegex(ValueError, 'durably bound'):
            manual._terminal_export(store, state, package)

    def test_collection_retains_successful_siblings_after_one_corrupt_object(self):
        store = Store()
        run_id = parent()['run_id']
        prefix = run_id + '/results/'
        for name in ('a-bad', 'b-good', 'c-good', 'd-good'):
            store.put(prefix + name, name.encode())
        original_head = store.head
        def head(key):
            meta = original_head(key)
            if key == prefix + 'a-bad':
                meta['metadata']['sha256'] = '0' * 64
            return meta
        with tempfile.TemporaryDirectory() as tmp, patch.object(store, 'head', side_effect=head):
            with self.assertRaisesRegex(ValueError, 'checksum'):
                manual._collect(store, run_id, tmp, {})
            inventory = manual._restore_inventory(run_id, tmp)
            self.assertEqual(set(inventory), {prefix + name for name in ('b-good', 'c-good', 'd-good')})
            self.assertFalse((Path(tmp) / 'results/a-bad').exists())
            self.assertFalse(list(Path(tmp).rglob('*.part')))

    def test_collection_reserves_inflight_bytes_against_the_aggregate_bound(self):
        store = Store()
        run_id = parent()['run_id']
        prefix = run_id + '/results/'
        store.put(prefix + 'a', b'1234')
        store.put(prefix + 'b', b'5678')
        with tempfile.TemporaryDirectory() as tmp, patch.object(manual.campaign, 'MAX_RESULT_TOTAL_BYTES', 6):
            with self.assertRaisesRegex(ValueError, 'bounds'):
                manual._collect(store, run_id, tmp, {})
            inventory = manual._restore_inventory(run_id, tmp)
            self.assertEqual({key: row['size'] for key, row in inventory.items()}, {prefix + 'a': 4})
            self.assertFalse((Path(tmp) / 'results/b').exists())

    def test_incremental_collection_retains_partial_results_and_rejects_replacement(self):
        store = Store()
        run_id = parent()['run_id']
        key = run_id + '/results/case/receipt.json'
        store.put(key, b'first', if_generation_match=0)
        with tempfile.TemporaryDirectory() as tmp:
            inventory = {}
            manual._collect(store, run_id, tmp, inventory)
            manual._collect(store, run_id, tmp, inventory)
            self.assertEqual(len(store.downloads), 1)
            self.assertEqual((Path(tmp) / 'results/case/receipt.json').read_bytes(), b'first')
            store.put(key, b'replacement')
            with self.assertRaisesRegex(ValueError, 'changed'):
                manual._collect(store, run_id, tmp, inventory)
            self.assertEqual(len(json.loads((Path(tmp) / 'result-inventory.json').read_text())), 1)

    def test_only_named_mutable_results_accept_verified_new_generations(self):
        import cloud_controller
        store, run_id = Store(), parent()['run_id']
        prefix = run_id + '/results/'
        names = ('startup.json', 'progress.json', 'receipt.json')
        with tempfile.TemporaryDirectory() as tmp:
            writer, reader = Path(tmp) / 'writer', Path(tmp) / 'reader'
            writer.mkdir()
            reader.mkdir()
            controller = object.__new__(cloud_controller.Controller)
            controller.root = writer
            controller.config = {'prefix': run_id}
            controller.clients = {'objects': store}
            for name in names:
                controller.persist(name, {'status': 'starting'})
            inventory = {}
            manual._collect(store, run_id, reader, inventory)
            for name in names:
                controller.persist(name, {'status': 'admitted', 'name': name})
            manual._collect(store, run_id, reader, inventory)
            for name in names:
                self.assertEqual(json.loads((reader / 'results' / name).read_text()),
                                 {'status': 'admitted', 'name': name})
                self.assertEqual(inventory[prefix + name]['generation'], store.head(prefix + name)['generation'])

    def test_failed_final_export_preserves_prior_verified_inventory(self):
        store, run_id = Store(), parent()['run_id']
        store.put(run_id + '/results/partial.json', b'partial', if_generation_match=0)
        with tempfile.TemporaryDirectory() as tmp:
            manual._collect(store, run_id, tmp, {})
            saved = manual._restore_inventory(run_id, tmp)
            with patch.object(store, 'list', side_effect=RuntimeError('listing failed')):
                with self.assertRaisesRegex(RuntimeError, 'listing failed'):
                    manual._collect(store, run_id, tmp, saved)
            rows = json.loads((Path(tmp) / 'result-inventory.json').read_text())
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['sha256'], hashlib.sha256(b'partial').hexdigest())

    def test_mutable_result_reconciles_list_to_download_replacement(self):
        class RacingStore(Store):
            def __init__(self):
                super().__init__()
                self.raced = False

            def download(self, key, path, *, generation=None, max_bytes=None):
                if not self.raced:
                    self.raced = True
                    self.put(key, b'new')
                    raise ApiError(404, 'listed generation no longer exists')
                return super().download(key, path, generation=generation, max_bytes=max_bytes)

        store, run_id = RacingStore(), parent()['run_id']
        key = run_id + '/results/progress.json'
        store.put(key, b'old', if_generation_match=0)
        with tempfile.TemporaryDirectory() as tmp:
            inventory = {}
            manual._collect(store, run_id, tmp, inventory)
            self.assertEqual((Path(tmp) / 'results/progress.json').read_bytes(), b'new')
            self.assertEqual(inventory[key]['generation'], '2')

    def test_qualification_collection_keeps_partial_inventory_on_digest_error(self):
        store, state = Store(), parent()
        prefix = 'runs/' + state['run_id'] + '/qualification/bake/'
        store.put(prefix + 'diagnostic.json', b'first', if_generation_match=0)
        store.put(prefix + 'status.json', b'second', if_generation_match=0)
        original_head = store.head

        def head(key):
            result = original_head(key)
            if key.endswith('status.json'):
                result['metadata']['sha256'] = 'f' * 64
            return result

        store.head = head
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'checksum'):
                manual._collect_qualification(store, {'bake': state}, tmp)
            inventory = json.loads((Path(tmp) / 'qualification-inventory.json').read_text())
            self.assertEqual(len(inventory), 1)
            self.assertTrue(inventory[0]['sha256_metadata_verified'])
            self.assertEqual(inventory[0]['generation'], '1')

    def test_placeholder_guest_records_never_mint_qualification(self):
        with tempfile.TemporaryDirectory() as tmp:
            entries = []
            for i in range(2):
                path = Path(tmp) / str(i)
                raw = json.dumps({'complete': True}).encode()
                path.write_bytes(raw)
                entries.append({'file': str(path), 'reference': {'bucket': CONFIG['bucket'],
                    'object': 'evidence/' + str(i), 'generation': str(i + 1),
                    'sha256': hashlib.sha256(raw).hexdigest()}})
            with self.assertRaises(ValueError):
                gcp_image_receipt.make_receipt({'image': {'path': 'projects/mysten-research/global/images/test-image', 'id': '123'},
                    'runtime_sha256': 'a' * 64, 'service_account': CONFIG['service_account'], 'evidence': entries})


class AsyncSafetyTest(unittest.IsolatedAsyncioTestCase):
    async def test_status_poll_retries_credentials_until_success(self):
        state = parent()
        state['deadline'] = manual._utc(time.time() + 300)
        failure = manual.CredentialUnavailable('secret-token')
        with patch.object(manual, '_wait_status_poll', side_effect=[failure, failure, {'complete': True}]), \
                patch.object(manual.asyncio, 'sleep', new_callable=unittest.mock.AsyncMock) as sleep, \
                patch('sys.stderr') as stderr:
            result = await manual._wait_status(Store(), state, 'host', 'a' * 64)
        self.assertEqual(result, {'complete': True})
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(5), unittest.mock.call(10)])
        self.assertNotIn('secret-token', str(stderr.write.call_args_list))

    async def test_status_poll_credential_outage_ends_at_deadline(self):
        state = parent()
        state['deadline'] = manual._utc(220)
        clock, sleeps = [0], []
        async def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        with patch.object(manual, '_wait_status_poll',
                          side_effect=manual.CredentialUnavailable('secret')), \
                patch.object(manual.time, 'time', side_effect=lambda: clock[0]), \
                patch.object(manual.asyncio, 'sleep', side_effect=sleep), patch('sys.stderr'):
            with self.assertRaisesRegex(TimeoutError, 'qualification status missing before deadline'):
                await manual._wait_status(Store(), state, 'host', 'a' * 64)
        self.assertEqual(sleeps, [5, 10, 20, 40, 60, 60, 25])
        self.assertEqual(clock[0], 220)

    async def test_cleanup_credential_failure_is_fail_fast(self):
        store, state = Store(), parent()
        execute = unittest.mock.AsyncMock()
        with patch.object(manual, '_lease_owned', side_effect=manual.CredentialUnavailable('unavailable')) as lease:
            with self.assertRaises(manual.CredentialUnavailable):
                await manual._cleanup(CONFIG, state, None, store, execute)
        lease.assert_called_once()
        execute.assert_not_called()

    async def test_cancelled_blocking_write_is_joined_before_return(self):
        import threading
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def write():
            entered.set()
            release.wait(5)
            finished.set()

        task = asyncio.create_task(manual._blocking(write))
        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(finished.is_set())

    async def test_phase_waiters_join_artifact_io_before_cleanup(self):
        import threading

        for filename in ('preparation.json', 'status.json', 'worker-ready.json'):
            for outcome in ('failure', 'cancel', 'peer-io-failure'):
                with self.subTest(filename=filename, outcome=outcome):
                    entered, release, failed, finished = (threading.Event() for _ in range(4))
                    cleanup = []
                    first = RuntimeError('first qualification failure')
                    io_failure = ValueError('artifact I/O failure')
                    state = parent()

                    class ConcurrentStore(Store):
                        def head(self, key):
                            if '/slow/' in key:
                                entered.set()
                                if not release.wait(5):
                                    raise TimeoutError('test release missing')
                                finished.set()
                                if outcome == 'peer-io-failure':
                                    raise io_failure
                                return {'generation': '1'}
                            if '/bad/' in key:
                                if not entered.wait(5):
                                    raise TimeoutError('test waiter did not enter')
                                failed.set()
                                raise first
                            return super().head(key)

                    store = ConcurrentStore()

                    async def run_phase():
                        try:
                            await manual._joined_waiters(
                                manual._wait_status(store, state, name, 'c' * 64, filename=filename)
                                for name in ('slow', 'bad') if outcome != 'cancel' or name == 'slow')
                        finally:
                            cleanup.append(finished.is_set())

                    task = asyncio.create_task(run_phase())
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                        if outcome == 'cancel':
                            task.cancel()
                        else:
                            self.assertTrue(await asyncio.to_thread(failed.wait, 2))
                        with self.assertRaises(TimeoutError):
                            await asyncio.wait_for(asyncio.shield(task), 0.05)
                        self.assertEqual(cleanup, [])
                        if outcome == 'cancel':
                            task.cancel()
                        release.set()
                        if outcome == 'cancel':
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        elif outcome == 'failure':
                            with self.assertRaises(RuntimeError) as caught:
                                await task
                            self.assertIs(caught.exception, first)
                        else:
                            with self.assertRaises(BaseExceptionGroup) as caught:
                                await task
                            self.assertIs(caught.exception.exceptions[0], first)
                            self.assertIn(io_failure, caught.exception.exceptions[1].exceptions)
                        self.assertEqual(cleanup, [True])
                    finally:
                        release.set()
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)

    async def test_cleanup_finishes_when_caller_is_cancelled(self):
        entered, release = asyncio.Event(), asyncio.Event()
        actions = []

        async def teardown(*args):
            actions.append('vm')
            entered.set()
            await release.wait()
            actions.extend(('disk', 'policy', 'image', 'lease'))

        with patch.object(manual, '_cleanup', side_effect=teardown):
            task = asyncio.create_task(manual._cleanup_joined(CONFIG, {}, None, Store(), None))
            await entered.wait()
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(actions, ['vm', 'disk', 'policy', 'image', 'lease'])

    async def test_boot_disk_requires_exact_guest_image_and_disk_ids(self):
        state = parent()
        record = {'name': state['run_id'] + '-0', 'disk_token': 'd' * 32, 'disk_id': '123'}
        state['instances'] = [record]
        image = {'path': 'projects/mysten-research/global/images/qualified', 'id': '987'}
        disk = {'name': record['name'], 'id': '123', 'sourceImageId': '987',
                'sourceImage': 'https://www.googleapis.com/compute/v1/' + image['path'],
                'description': json.dumps({'run': state['run_id'], 'source': state['source_sha256'],
                                           'expiry': state['deadline'], 'token': record['disk_token']}, sort_keys=True)}

        class Compute:
            async def _describe(self, kind, name, *, guest=False):
                self.assertion = (kind, name, guest)
                return disk

        compute = Compute()
        await manual._admit_boot_images(compute, state, image)
        self.assertEqual(compute.assertion, ('disk', record['name'], True))
        for field, replacement in (('id', '124'), ('sourceImageId', '988'),
                                   ('sourceImage', image['path'] + '-replacement'),
                                   ('description', '{}')):
            old = disk[field]
            disk[field] = replacement
            with self.assertRaisesRegex(RuntimeError, 'source image identity'):
                await manual._admit_boot_images(compute, state, image)
            disk[field] = old

    async def test_status_rejects_wrong_identity_and_partial_result(self):
        store, state = Store(), parent()
        state['deadline'] = manual._utc(time.time() + 60)
        prefix = 'runs/' + state['run_id'] + '/qualification/bake/status.json'
        status = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
                  'manifest_sha256': 'b' * 64, 'deadline_utc': state['deadline'], 'complete': False}
        store.put(prefix, json.dumps(status).encode())
        with self.assertRaisesRegex(RuntimeError, 'qualification failed'):
            await manual._wait_status(store, state, 'bake', 'b' * 64)
        status['complete'] = True
        status['source_sha256'] = 'c' * 64
        store.put(prefix, json.dumps(status).encode())
        with self.assertRaisesRegex(RuntimeError, 'identity'):
            await manual._wait_status(store, state, 'bake', 'b' * 64)

    async def test_bootstrap_error_is_reported_without_waiting_for_expiry(self):
        store, state = Store(), parent()
        state['deadline'] = manual._utc(time.time() + 60)
        key = 'runs/' + state['run_id'] + '/qualification/bake/startup-error.json'
        store.put(key, json.dumps({'complete': False, 'source_sha256': state['source_sha256'],
                                   'error': 'bootstrap failed'}).encode())
        with self.assertRaisesRegex(RuntimeError, 'bootstrap failed'):
            await manual._wait_status(store, state, 'bake', 'b' * 64)

    async def test_late_worker_failure_missing_ready_and_replaced_identity_block_campaign(self):
        fleet = [{'name': f'instance-{i}', 'id': str(100 + i)} for i in range(6)]
        for failure in ('startup-error', 'missing', 'replaced'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                state = parent()
                state['policy'] = {'name': state['run_id'] + '-placement', 'id': '42'}
                state['deadline'] = manual._utc(time.time() + 7200)
                package = {'manifest_sha256': 'b' * 64}
                loop = asyncio.get_running_loop()
                deadline = asyncio.timeout(30)
                release = threading.Event()
                probe_finished = threading.Event()
                missing_key = f'runs/{state["run_id"]}/qualification/{fleet[0]["name"]}/startup-error.json'

                def expire_readiness():
                    # Queue the already-expired deadline before releasing the I/O.
                    deadline.reschedule(loop.time())
                    loop.call_soon(release.set)

                class GuestStore(Store):
                    def head(self, key):
                        if failure == 'missing' and key == missing_key:
                            loop.call_soon_threadsafe(expire_readiness)
                            if not release.wait(5):
                                raise AssertionError('readiness timeout did not release status probe')
                            try:
                                return super().head(key)
                            finally:
                                probe_finished.set()
                        return super().head(key)

                    def put(self, key, data, **kwargs):
                        result = super().put(key, data, **kwargs)
                        if key.endswith('/runtime.json'):
                            for index, host in enumerate(fleet):
                                prefix = f'runs/{state["run_id"]}/qualification/{host["name"]}/'
                                if index == 0 and failure == 'missing':
                                    continue
                                if index == 0 and failure == 'startup-error':
                                    super().put(prefix + 'startup-error.json',
                                                json.dumps({'source_sha256': state['source_sha256'],
                                                            'complete': False, 'error': 'worker unit refused'}).encode())
                                    continue
                                super().put(prefix + 'worker-ready.json',
                                            json.dumps({'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
                                                        'manifest_sha256': package['manifest_sha256'],
                                                        'deadline_utc': state['deadline'], 'instance_name': host['name'],
                                                        'instance_id': '999' if index == 0 else host['id'],
                                                        'complete': True}).encode())
                        return result
                store = GuestStore()
                path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
                with patch.object(manual.gcp_runtime, 'build_runtime_config',
                                  return_value={'package_root': '/package'}) as build_runtime:
                    if failure == 'missing':
                        with self.assertRaises(TimeoutError):
                            async with deadline:
                                await manual._suite(
                                    store, CONFIG, state, path, fleet, package, manual.REVIEWED_SPEC, tmp, 'irdma0')
                        self.assertTrue(deadline.expired())
                        self.assertTrue(probe_finished.is_set())
                    else:
                        message = 'worker unit refused' if failure == 'startup-error' else 'identity mismatch'
                        with self.assertRaisesRegex(RuntimeError, message):
                            await asyncio.wait_for(manual._suite(
                                store, CONFIG, state, path, fleet, package, manual.REVIEWED_SPEC, tmp, 'irdma0'), 30)
                campaign_deadline = build_runtime.call_args.kwargs['campaign_deadline_utc']
                expiry = manual.campaign.parse_time(state['deadline']).timestamp()
                campaign_end = manual.campaign.parse_time(campaign_deadline).timestamp()
                self.assertEqual(campaign_end, expiry - manual.CAMPAIGN_MARGIN_SECONDS)
                self.assertEqual(campaign_end - manual.REVIEWED_SPEC['export_reserve_seconds'],
                                 expiry - manual.CAMPAIGN_MARGIN_SECONDS - 900)
                self.assertNotIn('campaign_submitted', state)
                self.assertFalse(any('/mailbox/' in key for key in store.objects))

    async def test_lease_replacement_never_authorizes_cleanup(self):
        store, state = Store(), parent()
        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            gcp_campaign.save(path, state)
            import gcp_lease
            lease, generation = gcp_lease.acquire(store, state)
            state['lease_token'] = lease['token']
            gcp_campaign.save(path, state)
            lease['token'] = 'foreign'
            registry = json.loads(store.get(gcp_lease.REGISTRY_KEY))
            registry['runs'][state['run_id']] = lease
            store.put(gcp_lease.REGISTRY_KEY, json.dumps(registry).encode(),
                      if_generation_match=store.head(gcp_lease.REGISTRY_KEY)['generation'])
            async def execute(_):
                self.fail('foreign lease must prevent Compute calls')
            with self.assertRaisesRegex(RuntimeError, 'token changed'):
                await manual._cleanup(CONFIG, state, path, store, execute)

    async def test_cleanup_collects_all_errors_and_retains_lease(self):
        import gcp_lease
        from unittest.mock import AsyncMock
        store, state, visited = Store(), parent(), []
        for index, name in enumerate(('bake', 'pair', 'fleet'), 2):
            state['children'][name] = {'run_id': 'bench-' + str(index) * 16}

        class BrokenCompute:
            def __init__(self, config, child, path, execute):
                self.child = child

            async def cleanup(self):
                visited.append(self.child['run_id'])
                raise RuntimeError('stale resource ID')

        async def execute(_):
            self.fail('fake cleanup does not issue commands')

        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            gcp_campaign.save(path, state)
            lease, _ = gcp_lease.acquire(store, state)
            state['lease_token'] = lease['token']
            gcp_campaign.save(path, state)
            with patch('gcp_manual_compute.Compute', BrokenCompute), \
                 patch('gcp_manual_image.cleanup_image', new_callable=AsyncMock,
                       side_effect=RuntimeError('foreign image')):
                with self.assertRaisesRegex(RuntimeError, 'cleanup incomplete'):
                    await manual._cleanup(CONFIG, state, path, store, execute)
            self.assertEqual(len(visited), 3)
            self.assertEqual(len(state['cleanup_errors']), 4)
            self.assertFalse(state['cleanup_verified'])
            self.assertIsNotNone(gcp_lease.read(store))

    async def test_expired_phase_does_not_wait_for_status(self):
        state = parent()
        state['deadline'] = manual._utc(time.time() - 1)
        with self.assertRaises(TimeoutError):
            await manual._wait_status(Store(), state, 'bake', 'b' * 64)

    async def test_incomplete_terminal_is_checked_without_bulk_collection(self):
        store, state = Store(), parent()
        state['deadline'] = manual._utc(time.time() + 7200)
        state['policy'] = {'name': state['run_id'] + '-placement', 'id': '42'}
        runtime = {'package_root': '/mnt/tidehunter/' + state['run_id'] + '/package'}
        package = publish_export(store, state)

        class Mailbox:
            def __init__(self, *args):
                pass

            def submit(self, *args, **kwargs):
                return {'request': 'immutable'}

            def wait(self, *args, **kwargs):
                return {'exit_code': 0, 'stdout': json.dumps({'kv': {'complete': False,
                        'planned_cases': 473, 'completed_cases': 11}})}

        def collect(*args):
            self.fail('suite must not download the result prefix')
        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            gcp_campaign.save(path, state)
            with patch.object(manual.gcp_runtime, 'build_runtime_config', return_value=runtime), \
                 patch.object(manual.gcp_runtime, 'RemoteMailbox', Mailbox), \
                 patch.object(manual, '_headroom', return_value={}), \
                 patch.object(manual, '_collect', side_effect=collect):
                with self.assertRaisesRegex(RuntimeError, 'reviewed combined'):
                    await manual._suite(store, CONFIG, state, path, [], package, manual.REVIEWED_SPEC, tmp, 'irdma0')
            self.assertEqual(store.downloads, [])
            self.assertTrue(json.loads((Path(tmp) / 'state.json').read_text())['campaign_submitted'])
            self.assertTrue((Path(tmp) / 'campaign-receipt.json').exists())

    async def test_guest_token_timeout_reuses_request_and_respects_original_deadline(self):
        retry_spec = dict(manual.REVIEWED_SPEC, valkey_reproduction_profiles=[])
        class DelegatedStore(Store, manual.DelegatedArtifacts):
            def __init__(self, error_kind):
                Store.__init__(self)
                self.executable = 'gcloud'
                self.submissions = 0
                self.timed_out = False
                self.response_heads = 0
                self.error_kind = error_kind

            def put(self, key, data, **kwargs):
                meta = Store.put(self, key, data, **kwargs)
                if key.endswith('/requests/campaign.json'):
                    self.submissions += 1
                    request = json.loads(data)
                    response = {'request_sha256': hashlib.sha256(data).hexdigest(),
                                'instance_id': request['instance_id'], 'operation_id': 'campaign',
                                'source_sha256': request['source_sha256'], 'exit_code': 0, 'stderr': '',
                                'stdout': json.dumps({'kv': {'complete': True, 'planned_cases': 1,
                                                             'completed_cases': 1}})}
                    Store.put(self, key.replace('/requests/', '/responses/'), json.dumps(response).encode())
                return meta

            def head(self, key):
                if '/responses/campaign.json' in key:
                    self.response_heads += 1
                if '/responses/campaign.json' in key and not self.timed_out:
                    self.timed_out = True
                    if self.error_kind == 'network':
                        try:
                            raise urllib.error.URLError(TimeoutError('GET timed out'))
                        except urllib.error.URLError as cause:
                            raise ApiError(None, 'GET timed out') from cause
                    if self.error_kind == 'disconnect':
                        try:
                            raise http.client.RemoteDisconnected('Remote end closed connection without response')
                        except ConnectionError as cause:
                            raise ApiError(None, 'closed') from cause
                    if self.error_kind == 'certificate':
                        try:
                            raise urllib.error.URLError(ssl.SSLCertVerificationError('untrusted certificate'))
                        except urllib.error.URLError as cause:
                            raise ApiError(None, 'TLS failure') from cause
                    if self.error_kind == 'permission':
                        raise ApiError(403, 'denied')
                    if self.error_kind == 'no_cause':
                        raise ApiError(None, 'unproven')
                    raise subprocess.TimeoutExpired(
                        [self.executable, 'auth', 'print-access-token',
                         '--impersonate-service-account=' + manual.ACCOUNT,
                         '--project=' + manual.PROJECT, '--quiet'], 30)
                return Store.head(self, key)

        fleet = [{'index': i, 'role': 'coordinator' if i == 0 else 'owner',
                  'id': str(i + 100), 'name': 'instance-' + str(i), 'zone': 'us-central1-a',
                  'internal_ip': '10.0.0.' + str(i + 1), 'rdma_ip': '10.1.0.' + str(i + 1)}
                 for i in range(6)]
        for error_kind, deadline_expired in (('token', False), ('token', True),
                                             ('network', False), ('network', True), ('disconnect', False),
                                             ('permission', False), ('no_cause', False),
                                             ('certificate', False)):
            with self.subTest(error_kind=error_kind, deadline_expired=deadline_expired), tempfile.TemporaryDirectory() as tmp:
                store, state = DelegatedStore(error_kind), parent()
                state['deadline'] = manual._utc(time.time() + (30 if deadline_expired else 7200))
                state['policy'] = {'name': state['run_id'] + '-placement', 'id': '42'}
                package = publish_export(store, state, status='completed')
                runtime = {'package_root': '/mnt/tidehunter/' + state['run_id'] + '/package'}
                path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
                gcp_campaign.save(path, state)
                for host in fleet:
                    store.put(f'runs/{state["run_id"]}/qualification/{host["name"]}/worker-ready.json',
                              json.dumps({'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
                                          'manifest_sha256': package['manifest_sha256'], 'deadline_utc': state['deadline'],
                                          'instance_name': host['name'], 'instance_id': host['id'], 'complete': True}).encode())
                with patch.object(manual.gcp_runtime, 'build_runtime_config', return_value=runtime), \
                     patch.object(manual.campaign, 'coordinator_timeout', return_value=1), \
                     patch.object(manual, '_headroom', return_value={}), \
                     patch.object(manual.workflow_input.cloud_campaign, 'make_matrix', return_value=[{}]):
                    if error_kind in ('permission', 'no_cause', 'certificate'):
                        with self.assertRaises(ApiError):
                            await manual._suite(store, CONFIG, state, path, fleet, package,
                                                retry_spec, tmp, 'irdma0')
                        self.assertNotIn('terminal_export', state)
                        self.assertEqual(store.response_heads, 1)
                    elif deadline_expired:
                        with self.assertRaisesRegex(TimeoutError, 'outcome unknown'):
                            await manual._suite(store, CONFIG, state, path, fleet, package,
                                                retry_spec, tmp, 'irdma0')
                        self.assertNotIn('terminal_export', state)
                        self.assertFalse((Path(tmp) / 'campaign-receipt.json').exists())
                    else:
                        terminal = await manual._suite(store, CONFIG, state, path, fleet, package,
                                                       retry_spec, tmp, 'irdma0')
                        self.assertEqual(terminal['exit_code'], 0)
                        self.assertIn('terminal_export', state)
                if not deadline_expired:
                    # An expired deadline can end the wait before any poll.
                    self.assertTrue(store.timed_out)
                self.assertEqual(store.submissions, 1)

    async def test_failed_mailbox_terminal_is_not_delayed_by_bulk_download(self):
        store, state = Store(), parent()
        state['deadline'] = manual._utc(time.time() + 7200)
        state['policy'] = {'name': state['run_id'] + '-placement', 'id': '42'}
        runtime = {'package_root': '/mnt/tidehunter/' + state['run_id'] + '/package'}
        package = publish_export(store, state)
        waits = []

        class Mailbox:
            def __init__(self, *args):
                pass

            def submit(self, *args, **kwargs):
                return {'request': 'immutable'}

            def wait(self, *args, **kwargs):
                waits.append(True)
                if len(waits) == 1:
                    raise TimeoutError('terminal response missing')
                return {'exit_code': 1, 'stdout': '', 'stderr': 'controller failed'}

        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, state['run_id'])
            gcp_campaign.save(path, state)
            with patch.object(manual.gcp_runtime, 'build_runtime_config', return_value=runtime), \
                 patch.object(manual.gcp_runtime, 'RemoteMailbox', Mailbox), \
                 patch.object(manual, '_headroom', return_value={}), \
                 patch.object(manual, '_collect', side_effect=AssertionError('bulk collection before terminal')):
                with self.assertRaisesRegex(RuntimeError, 'coordinator failed') as caught:
                    await manual._suite(store, CONFIG, state, path, [], package, manual.REVIEWED_SPEC, tmp, 'irdma0')
            self.assertEqual(json.loads((Path(tmp) / 'campaign-receipt.json').read_text())['stderr'],
                             'controller failed')
            self.assertEqual(len(waits), 2)
            self.assertIn('terminal_export', state)
            owner = parent()
            owner['lease_token'] = 'owner'
            owner['children']['fleet'] = state
            actions = []

            async def cleanup(*args):
                actions.append('cleanup')

            download = store.download
            def guarded_download(*args, **kwargs):
                self.assertEqual(actions, ['cleanup'])
                return download(*args, **kwargs)

            with patch.object(manual, '_cleanup_joined', side_effect=cleanup), \
                 patch.object(store, 'download', side_effect=guarded_download):
                with self.assertRaisesRegex(RuntimeError, 'coordinator failed'):
                    await manual._finish_campaign(CONFIG, owner, path, store, None, tmp, caught.exception)
            self.assertEqual((Path(tmp) / 'results/controller.tar.gz').read_bytes(), b'partial archive')

    async def test_corrupt_terminal_export_uses_partial_collection_before_cleanup(self):
        store, child = Store(), parent()
        child['deadline'] = manual._utc(time.time() + 7200)
        child['policy'] = {'name': child['run_id'] + '-placement', 'id': '42'}
        package = publish_export(store, child)
        store.put(child['run_id'] + '/results/controller.tar.gz', b'corrupt')
        runtime = {'package_root': '/mnt/tidehunter/' + child['run_id'] + '/package'}

        class Mailbox:
            def __init__(self, *args):
                pass

            def submit(self, *args, **kwargs):
                return {'request': 'immutable'}

            def wait(self, *args, **kwargs):
                return {'exit_code': 1, 'stdout': '', 'stderr': 'workload failed'}

        owner = parent()
        owner['lease_token'] = 'owner'
        owner['children']['fleet'] = child
        actions = []

        async def cleanup(*args):
            actions.append('cleanup')

        original_download = store.download
        def download(*args, **kwargs):
            self.assertNotIn('cleanup', actions)
            actions.append('download')
            return original_download(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            path = gcp_campaign.StateFile(Path(tmp) / 'state.json', store, child['run_id'])
            gcp_campaign.save(path, child)
            with patch.object(manual.gcp_runtime, 'build_runtime_config', return_value=runtime), \
                 patch.object(manual.gcp_runtime, 'RemoteMailbox', Mailbox), \
                 patch.object(manual, '_headroom', return_value={}):
                with self.assertRaises(BaseExceptionGroup) as caught:
                    await manual._suite(store, CONFIG, child, path, [], package, manual.REVIEWED_SPEC, tmp, 'irdma0')
            self.assertNotIn('terminal_export', child)
            self.assertIn('coordinator failed', str(caught.exception.exceptions[0]))
            self.assertIn('durably bound', str(caught.exception.exceptions[1]))
            with patch.object(manual, '_cleanup_joined', side_effect=cleanup), \
                 patch.object(store, 'download', side_effect=download):
                with self.assertRaises(BaseExceptionGroup):
                    await manual._finish_campaign(CONFIG, owner, path, store, None, tmp, caught.exception)
            self.assertEqual(actions[-1], 'cleanup')
            self.assertIn('download', actions)

    async def test_finish_tears_down_before_collection_and_preserves_failures(self):
        store, state = Store(), parent()
        state['lease_token'] = 'owner'
        child = parent()
        state['children']['fleet'] = child
        package = publish_export(store, child)
        child['terminal_export'] = manual._terminal_export(store, child, package, require_complete=False)
        store.put(child['run_id'] + '/results/case/data', b'partial')
        actions = []
        original_error = RuntimeError('original workload failure')

        async def cleanup(*args):
            actions.append('cleanup')

        def broken_collection(*args):
            actions.append('collect')
            raise OSError('local download failed')

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(manual, '_cleanup_joined', side_effect=cleanup), \
                 patch.object(manual, '_collect', side_effect=broken_collection):
                with self.assertRaises(BaseExceptionGroup) as caught:
                    await manual._finish_campaign(CONFIG, state, None, store, None, tmp, original_error)
            self.assertEqual(actions, ['cleanup', 'collect'])
            self.assertIs(caught.exception.exceptions[0], original_error)
            self.assertIn('local download failed', str(caught.exception.exceptions[1]))
            self.assertIn(child['run_id'] + '/results/case/data', store.objects)

    async def test_finish_retains_partial_objects_after_cleanup(self):
        store, state = Store(), parent()
        state['lease_token'] = 'owner'
        child = parent()
        state['children']['fleet'] = child
        package = publish_export(store, child)
        child['terminal_export'] = manual._terminal_export(store, child, package, require_complete=False)
        key = child['run_id'] + '/results/case/data'
        store.put(key, b'partial')
        actions = []

        async def cleanup(*args):
            actions.append('cleanup')

        original_download = store.download
        def download(*args, **kwargs):
            self.assertEqual(actions, ['cleanup'])
            return original_download(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(manual, '_cleanup_joined', side_effect=cleanup), \
                 patch.object(store, 'download', side_effect=download):
                with self.assertRaisesRegex(RuntimeError, 'original failure'):
                    await manual._finish_campaign(CONFIG, state, None, store, None, tmp,
                                                  RuntimeError('original failure'))
            self.assertEqual((Path(tmp) / 'results/case/data').read_bytes(), b'partial')

    async def test_frozen_observer_mismatch_stops_before_preflight_or_allocation(self):
        revision = 'b' * 40
        image = 'projects/mysten-research/global/images/base-image'
        receipt = {'source_revision': revision, 'source_sha256': 'a' * 64, 'manifest_sha256': 'c' * 64}
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp) / 'qualified'
            experiments = package_dir / 'package/source/experiments'
            kv = experiments / 'kv-comparison'
            kv.mkdir(parents=True)
            actual = manual.prepare.ROOT / 'experiments'
            for entry in (actual / 'kv-comparison').iterdir():
                if entry.name != 'observability':
                    (kv / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())
            shutil.copytree(actual / 'kv-comparison/observability', kv / 'observability')
            (experiments / 'nvmeof').symlink_to(actual / 'nvmeof', target_is_directory=True)
            (experiments / 'portable_transport.py').symlink_to(actual / 'portable_transport.py')
            scripts = experiments / 'gcp-rdma'
            scripts.mkdir()
            shutil.copyfile(actual / 'gcp-rdma/qualify_observer_matrix.py', scripts / 'qualify_observer_matrix.py')
            observer = kv / 'observability/campaign_exporter.py'
            text = observer.read_text()
            text = text.replace('def context(run_id, job):', 'def context(run_id, job):\n'
                '    if job is not None and job.get("nvmeof_target") == "spdk" and job.get("nvmeof_route") == "live":\n'
                '        raise ValueError("unsupported live SPDK target")', 1)
            observer.write_text(text)
            (package_dir / 'spec.json').write_text(json.dumps(manual.REVIEWED_SPEC))
            async def execute(argv):
                self.fail('cloud operation reached before observer qualification')
            with patch.object(manual, '_admit_package', return_value=(receipt, {})), \
                 patch.object(manual.gcp_manual_preflight, 'permission_preflight') as preflight:
                with self.assertRaisesRegex(ValueError, 'unsupported live SPDK target'):
                    await manual.run(CONFIG, package_dir=package_dir, revision=revision, spec=manual.REVIEWED_SPEC,
                                     wheels=[], base_image=image, base_image_id='42', rdma_device='irdma0',
                                     output=Path(tmp) / 'out', execute=execute, surcharge_cents=0)
                preflight.assert_not_called()
                self.assertFalse((Path(tmp) / 'out').exists())

    async def test_cancelled_create_cleans_child_and_releases_lease(self):
        import gcp_lease
        from unittest.mock import AsyncMock
        store, cleaned = Store(), []
        source, revision = 'a' * 64, 'b' * 40
        image = 'projects/mysten-research/global/images/base-image'

        class CancelCompute:
            def __init__(self, config, state, state_path, execute):
                self.state, self.state_path = state, state_path

            async def create_policy(self):
                self.state['policy'] = {'name': self.state['run_id'] + '-placement', 'uncertain': True}
                gcp_campaign.save(self.state_path, self.state)
                raise asyncio.CancelledError()

            async def cleanup(self):
                cleaned.append(self.state['run_id'])

        async def execute(argv):
            return {'id': '42', 'status': 'READY', 'selfLink': 'https://www.googleapis.com/compute/v1/' + image}

        receipt = {'source_revision': revision, 'source_sha256': source,
                   'manifest_sha256': 'c' * 64, 'sha256': 'd' * 64}
        with tempfile.TemporaryDirectory() as tmp:
            package_dir = Path(tmp) / 'package'
            package_dir.mkdir()
            (package_dir / 'spec.json').write_text(json.dumps(manual.REVIEWED_SPEC))
            with patch.object(manual.workflow_input.cloud_campaign, 'make_matrix', return_value=[{}] * 473), \
                 patch.object(manual, '_admit_package', return_value=(receipt, {'tree_sha256': 'f' * 64, 'dirty': True})), \
                 patch.object(manual, '_qualify_observer', return_value={}), \
                 patch.object(manual, '_check_snapshot'), \
                 patch.object(manual.gcp_manual_preflight, 'permission_preflight'), \
                 patch.object(manual.gcp_manual_preflight, 'quota_preflight', new_callable=AsyncMock, return_value=QUOTAS), \
                 patch.object(manual.gcp_manual_preflight, 'infrastructure_preflight', new_callable=AsyncMock,
                              return_value={'id': '42', 'status': 'READY', 'selfLink': 'https://www.googleapis.com/compute/v1/' + image}), \
                 patch.object(manual.gcp_budget, 'reserve', return_value={}), \
                 patch.object(manual, '_stage', return_value=({}, 'e' * 64)), \
                 patch('gcp_manual_compute.Compute', CancelCompute), \
                 patch('gcp_manual_image.cleanup_image', new_callable=AsyncMock):
                with self.assertRaises(asyncio.CancelledError):
                    await manual.run(CONFIG, package_dir=package_dir, revision=revision,
                                     spec=manual.REVIEWED_SPEC, wheels=[], base_image=image,
                                     base_image_id='42', rdma_device='irdma0', output=Path(tmp) / 'out',
                                     execute=execute, artifacts=store, surcharge_cents=0)
            state = json.loads((Path(tmp) / 'out/state.json').read_text())
            self.assertEqual(cleaned, [state['children']['fleet']['run_id']])
            self.assertTrue(state['cleanup_verified'])
            self.assertIsNone(gcp_lease.read(store))

    async def test_compact_campaign_creates_and_cleans_exact_pair(self):
        from unittest.mock import AsyncMock
        revision, source = 'b' * 40, 'a' * 64
        image = 'projects/mysten-research/global/images/base-image'
        receipt = {'source_revision': revision, 'source_sha256': source,
                   'manifest_sha256': 'c' * 64, 'sha256': 'd' * 64}
        spec = manual.REVIEWED_SPECS['parity_compact']
        operations = []

        async def execute(argv):
            operations.append(tuple(argv))
            return {}

        class PairCompute:
            def __init__(self, config, state, state_path, execute):
                self.state, self.path, self.execute = state, state_path, execute

            async def create_policy(self):
                self.state['policy'] = {'name': self.state['run_id'] + '-placement', 'id': '9'}

            async def create_vm(self, index, image, deadline, startup):
                name = self.state['run_id'] + '-' + str(index)
                await self.execute(['create', name])
                host = {'name': name, 'id': str(index + 1), 'index': index,
                        'zone': 'us-central1-a', 'internal_ip': '10.1.0.' + str(index + 1),
                        'rdma_ip': '10.2.0.' + str(index + 1)}
                self.state['instances'].append({'name': name, 'id': host['id'], 'fleet': host})
                gcp_campaign.save(self.path, self.state)
                return host

            async def admit_guests(self):
                return [record['fleet'] for record in self.state['instances']]

            async def cleanup(self):
                for record in self.state['instances']:
                    await self.execute(['delete', record['name']])

        async def stop_after_creation(*args, **kwargs):
            raise RuntimeError('stop after creation')

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = root / 'package'
            package.mkdir()
            (package / 'spec.json').write_text(json.dumps(spec))
            with patch.object(manual, '_admit_package', return_value=(receipt, {'tree_sha256': 'f' * 64, 'dirty': True})), \
                 patch.object(manual, '_qualify_observer', return_value={}), \
                 patch.object(manual, '_check_snapshot'), \
                 patch.object(manual.gcp_manual_preflight, 'permission_preflight'), \
                 patch.object(manual.gcp_manual_preflight, 'quota_preflight', new_callable=AsyncMock, return_value=QUOTAS), \
                 patch.object(manual.gcp_manual_preflight, 'infrastructure_preflight', new_callable=AsyncMock,
                              return_value={'id': '42', 'status': 'READY', 'selfLink': 'https://example/' + image}), \
                 patch.object(manual.gcp_budget, 'reserve', return_value={}), \
                 patch.object(manual, '_stage', return_value=({'manifest_sha256': 'c' * 64}, 'e' * 64)), \
                 patch.object(manual, '_admit_boot_images', side_effect=stop_after_creation), \
                 patch('gcp_manual_compute.Compute', PairCompute):
                with self.assertRaises(Exception):
                    await manual.run(CONFIG, package_dir=package, revision=revision, spec=spec, wheels=[],
                                     base_image=image, base_image_id='42', rdma_device='irdma0',
                                     output=root / 'out', execute=execute, artifacts=Store(), surcharge_cents=0)
            state = json.loads((root / 'out/state.json').read_text())['children']['fleet']
            expected = {state['run_id'] + '-0', state['run_id'] + '-1'}
            self.assertEqual(state['host_count'], 2)
            self.assertEqual({name for action, name in operations if action == 'create'}, expected)
            self.assertEqual({name for action, name in operations if action == 'delete'}, expected)

    async def test_single_fleet_failures_never_allocate_replacement_or_start_suite(self):
        import gcp_lease
        from unittest.mock import AsyncMock

        revision, source, manifest = 'a' * 40, 'b' * 64, 'c' * 64
        image = 'projects/mysten-research/global/images/base-image'
        receipt = {'source_revision': revision, 'source_sha256': source, 'manifest_sha256': manifest,
                   'sha256': 'd' * 64}
        frozen = {'tree_sha256': 'e' * 64, 'dirty': True}
        for failure in ('partial-create', 'preparation', 'pair', 'cleanup-proof'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                store, events, phase_errors = Store(), [], []
                package_dir = Path(tmp) / 'package'
                package_dir.mkdir()
                (package_dir / 'spec.json').write_text(json.dumps(manual.REVIEWED_SPEC))

                class Compute:
                    def __init__(self, config, state, state_path, execute):
                        self.state, self.path = state, state_path

                    async def create_policy(self):
                        events.append(('policy',))
                        self.state['policy'] = {'name': self.state['run_id'] + '-placement', 'id': '9'}

                    async def create_vm(self, index, boot_image, deadline, startup):
                        events.append(('create', index, boot_image, deadline))
                        host = {'name': self.state['run_id'] + '-' + str(index), 'id': str(index + 1),
                                'index': index, 'zone': 'us-central1-a', 'internal_ip': '10.1.0.' + str(index + 1),
                                'rdma_ip': '10.2.0.' + str(index + 1)}
                        self.state['instances'].append({'name': host['name'], 'id': host['id'], 'fleet': host})
                        gcp_campaign.save(self.path, self.state)
                        if failure == 'partial-create' and index == 2:
                            raise RuntimeError('unknown create outcome')
                        return host

                    async def admit_guests(self):
                        return [record['fleet'] for record in self.state['instances']]

                    async def cleanup(self):
                        events.append(('cleanup', tuple(x['id'] for x in self.state['instances'])))

                async def status(store, state, name, manifest_arg, *, filename='status.json', host=None):
                    if ((failure == 'preparation' and filename == 'preparation.json')
                            or (failure == 'pair' and filename == 'status.json')):
                        error = RuntimeError('guest qualification failed')
                        phase_errors.append(error)
                        raise error
                    if filename == 'status.json':
                        return {'cleanup_complete': False, 'pair': host['index'] < 2,
                                '_reference': {'object': name, 'generation': '1', 'sha256': '2' * 64}}
                    return {'proof': {'kind': 'bake-software-diagnostic', 'source_sha256': source,
                                      'manifest_sha256': manifest, 'preparation_sha256': 'f' * 64,
                                      'wheel_sha256': '1' * 64},
                            '_reference': {'object': name, 'generation': '1', 'sha256': '2' * 64}}

                async def execute(argv):
                    return {'id': '42', 'status': 'READY', 'selfLink': 'https://example/' + image}

                with patch.object(manual, '_admit_package', return_value=(receipt, frozen)), \
                     patch.object(manual, '_qualify_observer', return_value={}), \
                     patch.object(manual, '_check_snapshot'), \
                     patch.object(manual.gcp_manual_preflight, 'permission_preflight'), \
                     patch.object(manual.gcp_manual_preflight, 'quota_preflight', new_callable=AsyncMock, return_value=QUOTAS), \
                     patch.object(manual.gcp_manual_preflight, 'infrastructure_preflight', new_callable=AsyncMock,
                                  return_value={'id': '42', 'status': 'READY', 'selfLink': 'https://example/' + image}), \
                     patch.object(manual.gcp_budget, 'reserve', return_value={}), \
                     patch.object(manual, '_stage', return_value=({'manifest_sha256': manifest}, 'f' * 64)), \
                     patch.object(manual, '_admit_boot_images', new_callable=AsyncMock), \
                     patch.object(manual.qualify, 'fleet_startup_script', return_value='script'), \
                     patch.object(manual, '_wait_status', side_effect=status), \
                     patch.object(manual, '_suite', new_callable=AsyncMock) as suite, \
                     patch('gcp_manual_compute.Compute', Compute):
                    with self.assertRaises(Exception) as caught:
                        await manual.run(CONFIG, package_dir=package_dir, revision=revision,
                                         spec=manual.REVIEWED_SPEC, wheels=[{'sha256': '1' * 64}],
                                         base_image=image, base_image_id='42', rdma_device='irdma0',
                                         output=Path(tmp) / 'out', execute=execute, artifacts=store, surcharge_cents=0)
                root_error = caught.exception
                while isinstance(root_error, BaseExceptionGroup):
                    root_error = root_error.exceptions[0]
                if failure in ('preparation', 'pair'):
                    self.assertIs(root_error, phase_errors[0])
                else:
                    self.assertIsInstance(root_error, ValueError if failure == 'cleanup-proof' else RuntimeError)
                state = json.loads((Path(tmp) / 'out/state.json').read_text())
                self.assertEqual(set(state['children']), {'fleet'})
                creates = [event for event in events if event[0] == 'create']
                self.assertEqual(len(creates), 3 if failure == 'partial-create' else 6)
                self.assertTrue(all(event[2] == image and event[3] == state['deadline'] for event in creates))
                self.assertEqual(len([event for event in events if event[0] == 'policy']), 1)
                self.assertEqual(len([event for event in events if event[0] == 'cleanup']), 1)
                self.assertTrue(state['cleanup_verified'])
                self.assertIsNone(gcp_lease.read(store))
                suite.assert_not_awaited()

    async def test_real_compute_lifecycle_reconciles_failures_without_extra_fleet(self):
        from test_manual_compute import FakeMcp
        import gcp_lease
        from unittest.mock import AsyncMock

        revision, source, manifest = 'a' * 40, 'b' * 64, 'c' * 64
        image = 'projects/mysten-research/global/images/base-image'
        for failure in ('lost-partial-create', 'preparation', 'pair'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                package_dir = root / 'package'
                package_dir.mkdir()
                (package_dir / 'spec.json').write_text(json.dumps(manual.REVIEWED_SPEC))
                archive, wheel = root / 'bundle.tar', root / 'PyYAML-6.0.2-cp311-cp311-manylinux_2_17_x86_64.whl'
                archive.write_bytes(b'package')
                wheel.write_bytes(b'wheel')
                wheel_sha = hashlib.sha256(b'wheel').hexdigest()
                receipt = {'archive_path': str(archive), 'sha256': hashlib.sha256(b'package').hexdigest(),
                           'source_revision': revision, 'source_sha256': source, 'manifest_sha256': manifest}
                frozen = {'tree_sha256': 'd' * 64, 'dirty': True}
                state_path = root / 'out' / 'state.json'

                class GuestStore(Store):
                    def put(self, key, data, **kwargs):
                        result = super().put(key, data, **kwargs)
                        if key.endswith('/gate.json'):
                            state = json.loads(state_path.read_text())['children']['fleet']
                            name = key.split('/')[-2]
                            host = next(rec for rec in state['instances'] if rec['name'] == name)
                            index = int(name.rsplit('-', 1)[1])
                            prefix = 'runs/' + state['run_id'] + '/qualification/' + name + '/'
                            identity = {'run_id': state['run_id'], 'source_sha256': source,
                                        'manifest_sha256': manifest, 'deadline_utc': state['deadline'],
                                        'instance_name': name, 'instance_id': host['id']}
                            if failure == 'preparation' and index == 0:
                                super().put(prefix + 'status.json', json.dumps(
                                    {**identity, 'complete': False, 'error': 'preparation failed'}).encode(),
                                    if_generation_match=0)
                            else:
                                proof = {'kind': 'bake-software-diagnostic', 'source_sha256': source,
                                         'manifest_sha256': manifest, 'preparation_sha256': state['preparation_sha256'],
                                         'wheel_sha256': wheel_sha}
                                super().put(prefix + 'preparation.json', json.dumps(
                                    {**identity, 'complete': True, 'proof': proof}).encode(), if_generation_match=0)
                                super().put(prefix + 'status.json', json.dumps(
                                    {**identity, 'complete': failure != 'pair', 'cleanup_complete': True,
                                     'pair': index < 2, 'error': 'pair failed'}).encode(), if_generation_match=0)
                        return result

                store = GuestStore()
                api = FakeMcp(None, CONFIG, root / 'child-state.json')
                reads, deleted = [], []
                timed_out = False

                async def execute(argv):
                    nonlocal timed_out
                    state = json.loads(state_path.read_text())['children']['fleet']
                    api.path.write_text(json.dumps(state))
                    if argv[:3] == ['compute', 'operations', 'describe']:
                        name = api.operation_target.rsplit('/', 1)[-1]
                        return {'text': json.dumps({'name': api.operation_name, 'status': 'DONE',
                            'zone': 'zones/us-central1-a', 'targetLink': api.operation_target,
                            'targetId': api.resources['instance'][name]['id']})}
                    if argv[:3] == ['compute', 'instances', 'describe'] and not timed_out:
                        timed_out = True
                        reads.append(tuple(argv))
                        raise TimeoutError('transient describe')
                    if argv[:3] == ['compute', 'instances', 'describe'] and len(reads) == 1:
                        reads.append(tuple(argv))
                    is_create = argv[:3] == ['compute', 'instances', 'create']
                    if is_create:
                        index = int(argv[3].rsplit('-', 1)[1])
                        api.lost = 'instance' if failure == 'lost-partial-create' and index == 2 else None
                    if argv[:3] == ['compute', 'instances', 'delete']:
                        record = next(item for item in state['instances'] if item['name'] == argv[3])
                        actual = api.resources['instance'][argv[3]]
                        if record.get('id') != actual['id'] or record.get('delete_requested') is not True:
                            raise AssertionError('delete lacks exact persisted instance identity')
                        deleted.append((argv[3], actual['id']))
                    try:
                        return await api(argv)
                    finally:
                        if is_create and argv[3] in api.resources['instance']:
                            vm = api.resources['instance'][argv[3]]
                            disk = api.resources['disk'][argv[3]]
                            vm['id'] = str(200 + index)
                            vm['networkInterfaces'][0]['networkIP'] = '10.0.0.' + str(index + 2)
                            vm['networkInterfaces'][1]['networkIP'] = '10.1.0.' + str(index + 2)
                            disk['id'] = str(300 + index)
                            disk['sourceImageId'] = '42'
                            if failure == 'lost-partial-create' and index == 2:
                                disk['sourceImage'] = 'projects/mysten-research/global/images/incorrect'

                with patch.object(manual, '_admit_package', return_value=(receipt, frozen)), \
                     patch.object(manual, '_qualify_observer', return_value={}), \
                     patch.object(manual, '_check_snapshot'), \
                     patch.object(manual.gcp_manual_preflight, 'permission_preflight'), \
                     patch.object(manual.gcp_manual_preflight, 'quota_preflight', new_callable=AsyncMock, return_value=QUOTAS), \
                     patch.object(manual.gcp_manual_preflight, 'infrastructure_preflight', new_callable=AsyncMock,
                                  return_value={'id': '42', 'status': 'READY', 'selfLink': 'https://example/' + image}), \
                     patch.object(manual.gcp_budget, 'reserve', return_value={}), \
                     patch.object(manual.qualify, 'fleet_startup_script', return_value='script'), \
                     patch.object(manual, '_suite', new_callable=AsyncMock) as suite:
                    with self.assertRaises(Exception):
                        await manual.run(CONFIG, package_dir=package_dir, revision=revision,
                                         spec=manual.REVIEWED_SPEC, wheels=[{'path': str(wheel), 'sha256': wheel_sha}],
                                         base_image=image, base_image_id='42', rdma_device='irdma0',
                                         output=root / 'out', execute=execute, artifacts=store, surcharge_cents=0)
                parent_state = json.loads(state_path.read_text())
                fleet = parent_state['children']['fleet']
                creates = [item for item in api.mutations if item[:2] == ('instance', 'create')]
                self.assertEqual(len(creates), 3 if failure == 'lost-partial-create' else 6)
                self.assertLessEqual(len(creates), 6)
                self.assertEqual(set(parent_state['children']), {'fleet'})
                self.assertEqual(reads[0], reads[1])
                self.assertEqual(set(deleted), {(rec['name'], rec['id']) for rec in fleet['instances']})
                self.assertTrue(all(rec['deleted'] and rec['disk_deleted'] for rec in fleet['instances']))
                self.assertTrue(fleet['policy']['deleted'])
                self.assertTrue(parent_state['cleanup_verified'])
                self.assertIsNone(gcp_lease.read(store))
                suite.assert_not_awaited()

    async def test_complete_campaign_uses_real_receipt_runtime_and_mailbox_admission(self):
        """Model transport/guests only; exercise production orchestration validators."""
        import gcp_image
        import gcp_lease
        from unittest.mock import AsyncMock

        revision, source, manifest = 'a' * 40, 'b' * 64, 'c' * 64
        expected_cases = len(manual.workflow_input.cloud_campaign.make_matrix(
            {k: v for k, v in manual.REVIEWED_SPEC.items() if k != 'suite'}))
        base_image = 'projects/mysten-research/global/images/base-image'
        captured = {'path': base_image, 'id': '42'}
        definitions = {'GLIBC': ['2.34'], 'GLIBCXX': [], 'CXXABI': [], 'OPENSSL': []}
        abi = {'needed': ['libfabric.so.1'], 'version_requirements': definitions}
        events, states = [], {}

        def proof(own, peer):
            fabric = '/lib/libfabric.so.1'
            resolved = {
                fabric: {'identity': {'sha256': 'a' * 64}, 'soname': 'libfabric.so.1',
                         'version_definitions': definitions},
                '/lib/libfdb_c.so': {'identity': {'sha256': 'a' * 64}, 'soname': 'libfdb_c.so',
                                     'version_definitions': definitions}}
            linkage = {'ldd': 'libfabric.so.1 => /lib/libfabric.so.1', 'resolved': resolved}
            latency = ('rdma_cm QPs : ON\n#bytes #iterations t_min t_max t_typical t_avg t_stdev p99 p999\n'
                       '1024 1000 7.65 11.12 7.72 7.75 0.12 8.00 11.12')
            peer_ip = peer['rdma_ip']
            value = 'f' * 32
            return {'kind': 'guest-diagnostic', 'image': captured, 'recipes': gcp_image.recipe_hashes(),
                    'instance_id': own['id'], 'peer_instance_id': peer['id'],
                    'service_account': CONFIG['service_account'], 'rdma_device': 'irdma0',
                    'scratch_devices': {'local-ssd-0': '/dev/nvme0n1'},
                    'local_rdma': own['rdma_ip'], 'peer': peer_ip,
                    'outputs': {'io_uring': 'io_uring_setup(2) succeeded',
                        'guest_runtime': {'executable': '/opt/tidehunter-python/bin/python3',
                            'python': '3.11.13', 'yaml': '6.0.2', 'nofile_soft': 1024, 'nofile_hard': 524288},
                        'memlock': {'soft': 'infinity', 'hard': 'infinity', 'docker': 'infinity'},
                        'io_uring_workload': {'jobs': [{'error': 0, 'read': {'io_bytes': 4096}}]},
                        'nvme_ioctl': {'local-ssd-0': 'NVMe Identify Controller'},
                        'package': {'manifest_sha256': manifest, 'runtime_checked': True, 'abi': abi,
                            'fabric_library': fabric, 'linkage': linkage,
                            'spdk': dict(SPDK_IDENTITY), 'spdk_runtime_linkage': linkage,
                            'valkey_runtime_linkage': {'valkey-server': linkage, 'valkey-cli': linkage, 'valkey-benchmark': linkage}},
                        'libfabric_sha256': 'a' * 64, 'fi_info_fabric': fabric,
                        'fi_info_ldd': 'libfabric.so.1 => /lib/libfabric.so.1', 'libfabric_api_version': 65541,
                        'kernel': '6.12', 'installed_rpms': ['kmod-idpf-irdma-0:1-1.x86_64'],
                        'libfabric_version': 'fi_info: 2.6.0',
                        'providers': {'verbs': 'provider: verbs;ofi_rxm', 'tcp': 'provider: tcp;ofi_rxm'},
                        'rdma_links': 'link irdma0/1 state ACTIVE',
                        'nvme_modules': {name: 'filename: module' for name in
                            ('irdma', 'idpf', 'rdma_cm', 'nvme', 'nvmet', 'nvme_fabrics', 'nvme_rdma', 'nvmet_rdma', 'nvme_tcp', 'nvmet_tcp')},
                        'openssl': 'OpenSSL 3.2', 'python': '3.12.0',
                        'containers': {ref: [{'RepoDigests': [ref]}] for ref in gcp_image.required_containers()},
                        'rdma_commands': {op: ['ib_' + op + '_lat', '-R', '-d', 'irdma0', '-p', port,
                                               '-s', '1024', '-n', '1000', peer_ip]
                                          for op, port in (('read', '18515'), ('send', '18516'))},
                        'rdma_read': latency, 'rdma_send': latency,
                        'valkey': {'endpoint': peer_ip + ':6380', 'value': value,
                                   'get': value, 'set': 'OK', 'del': '1'}}}

        sample_hosts = [{'id': '1', 'rdma_ip': '10.3.0.1'},
                        {'id': '2', 'rdma_ip': '10.3.0.2'}]
        records = [proof(sample_hosts[i], sample_hosts[1 - i]) for i in range(2)]
        gcp_image.admit_spdk({'_records': records}, manual.REVIEWED_SPEC)
        for alteration in ('missing', 'different', 'linkage'):
            invalid = copy.deepcopy(records)
            package = invalid[1]['outputs']['package']
            if alteration == 'missing':
                del package['spdk']
            elif alteration == 'different':
                package['spdk']['source_patch_sha256'] = '0' * 64
            else:
                package['spdk_runtime_linkage']['ldd'] = 'libaio.so.1 => not found'
            with self.subTest(alteration=alteration), self.assertRaises(ValueError):
                gcp_image.admit_spdk({'_records': invalid}, manual.REVIEWED_SPEC)

        class GuestStore(Store):
            def put(self, key, data, **kwargs):
                result = super().put(key, data, **kwargs)
                if key.endswith('/gate.json'):
                    run_id = key.split('/')[1]
                    state = states[run_id]
                    name = key.split('/')[-2]
                    gate = json.loads(data)
                    events.append(('gate', run_id, name))
                    self.assert_gate(state, name, gate)
                    identity = {'run_id': run_id, 'source_sha256': source,
                                'manifest_sha256': manifest, 'deadline_utc': state['deadline']}
                    prefix = 'runs/' + run_id + '/qualification/'
                    raw = self.get('runs/' + run_id + '/preparation.json')
                    preparation = json.loads(raw)
                    own_index = next(i for i, x in enumerate(state['instances']) if x['name'] == name)
                    own = state['instances'][own_index]['fleet']
                    identity.update(instance_name=name, instance_id=own['id'])
                    diag = {'kind': 'bake-software-diagnostic', 'source_sha256': source,
                            'manifest_sha256': manifest, 'preparation_sha256': hashlib.sha256(raw).hexdigest(),
                            'wheel_sha256': preparation['wheels'][0]['sha256']}
                    super().put(prefix + name + '/preparation.json',
                                json.dumps({**identity, 'complete': True, 'proof': diag}).encode(), if_generation_match=0)
                    if own_index < 2:
                        peer = state['instances'][1 - own_index]['fleet']
                        super().put(prefix + name + '/diagnostic.json', json.dumps(proof(own, peer)).encode(), if_generation_match=0)
                    super().put(prefix + name + '/status.json',
                                json.dumps({**identity, 'complete': True, 'cleanup_complete': True,
                                            'pair': own_index < 2}).encode(), if_generation_match=0)
                if key.endswith('/runtime.json'):
                    runtime = json.loads(data)
                    state = states[runtime['run_id']]
                    if [host['id'] for host in runtime['fleet']] != [record['id'] for record in state['instances']]:
                        raise AssertionError('suite fleet differs from prepared and qualified instances')
                    if len(state.get('preparation_evidence', [])) != 6 or len(state.get('cleanup_evidence', [])) != 6:
                        raise AssertionError('suite runtime published before all-host qualification')
                    if [host['id'] for host in runtime['live_qualification']['pair_hosts']] != [
                            state['instances'][0]['id'], state['instances'][1]['id']]:
                        raise AssertionError('physical pair differs from suite fleet')
                    for host in runtime['fleet']:
                        super().put(f'runs/{runtime["run_id"]}/qualification/{host["name"]}/worker-ready.json',
                                    json.dumps({'run_id': runtime['run_id'], 'source_sha256': runtime['source_sha256'],
                                                'manifest_sha256': runtime['package']['manifest_sha256'],
                                                'deadline_utc': runtime['deadline_utc'], 'instance_name': host['name'],
                                                'instance_id': str(host['id']), 'complete': True}).encode(),
                                    if_generation_match=0)
                if '/mailbox/' in key and '/requests/headroom-' in key:
                    request = json.loads(data)
                    operation = request['operation_id']
                    events.append(('mailbox', operation))
                    if operation.startswith('headroom-probe-'):
                        body = {'admitted': True, 'connected': 225, 'cleanup_complete': True,
                                'teardown_started_unix': time.time(), 'samples': []}
                    else:
                        body = {'clean': True}
                    response = {'request_sha256': hashlib.sha256(data).hexdigest(),
                                'instance_id': request['instance_id'], 'operation_id': operation,
                                'source_sha256': source, 'exit_code': 0, 'stderr': '',
                                'stdout': json.dumps(body)}
                    super().put(key.replace('/requests/', '/responses/'), json.dumps(response).encode(),
                                if_generation_match=0)
                if '/mailbox/' in key and key.endswith('/requests/campaign.json'):
                    events.append(('mailbox', 'campaign'))
                    request = json.loads(data)
                    response = {'request_sha256': hashlib.sha256(data).hexdigest(),
                                'instance_id': request['instance_id'], 'operation_id': 'campaign',
                                'source_sha256': source, 'exit_code': 0, 'stderr': '',
                                'stdout': '/mnt/checkpoint-cells/r01/deployment/external-manifest.json\n{\n  "verified": true\n}\n' + json.dumps({'kv': {'complete': True, 'planned_cases': expected_cases,
                                                             'completed_cases': expected_cases,
                                                             'native_valkey_reproduction': {
                                                                 'complete': True, 'planned_cases': 4, 'completed_cases': 4,
                                                                 'cases': [{'profile': p, 'transport': t, 'complete': True,
                                                                            'owned_server_stopped': True}
                                                                           for p in manual.REVIEWED_SPEC['valkey_reproduction_profiles']
                                                                           for t in ('tcp', 'rdma')]}}})}
                    prefix = request['run_id'] + '/results/'
                    archive_data, inventory_data = b'controller archive', b'{}'
                    super().put(prefix + 'controller.tar.gz', archive_data, if_generation_match=0)
                    inventory_meta = super().put(prefix + 'controller-inventory.json', inventory_data, if_generation_match=0)
                    export = {'schema_version': 2, 'status': 'completed', 'source_sha256': source,
                              'binary_blake2b512': 'e' * 128,
                              'key': prefix + 'controller.tar.gz', 'bytes': len(archive_data),
                              'sha256': hashlib.sha256(archive_data).hexdigest(),
                              'inventory': {'key': prefix + 'controller-inventory.json',
                                            'bytes': len(inventory_data),
                                            'sha256': hashlib.sha256(inventory_data).hexdigest(),
                                            'generation': inventory_meta['generation']}}
                    super().put(prefix + 'receipt.json', json.dumps(export).encode(), if_generation_match=0)
                    super().put(key.replace('/requests/', '/responses/'), json.dumps(response).encode(), if_generation_match=0)
                return result

            def assert_gate(self, state, name, gate):
                if not state.get('guest_admitted') or not state.get('image_admitted'):
                    raise AssertionError('gate released before guest and image admission')
                record = next(x for x in state['instances'] if x['name'] == name)
                if gate['instance_id'] != record['id'] or hashlib.sha256(gate['script'].encode()).hexdigest() != gate['sha256']:
                    raise AssertionError('unbound gate')

        class SimulatedCompute:
            counter = 0

            def __init__(self, config, state, state_path, execute):
                self.state, self.state_path = state, state_path
                states[state['run_id']] = state

            async def create_policy(self):
                self.state['policy'] = {'name': self.state['run_id'] + '-placement', 'id': '999'}
                gcp_campaign.save(self.state_path, self.state)

            async def create_vm(self, index, image, deadline, startup):
                SimulatedCompute.counter += 1
                identifier = str(100 + SimulatedCompute.counter)
                name = self.state['run_id'] + '-' + str(index)
                fleet = {'index': index, 'role': 'coordinator' if index == 0 else 'owner',
                         'name': name, 'id': identifier, 'zone': 'us-central1-a',
                         'internal_ip': '10.1.0.' + str(SimulatedCompute.counter),
                         'rdma_ip': '10.2.0.' + str(SimulatedCompute.counter)}
                rec = {'name': name, 'id': identifier, 'disk_id': str(1000 + SimulatedCompute.counter),
                       'disk_token': format(SimulatedCompute.counter, '032x'), 'fleet': fleet,
                       'source_image': image, 'source_image_id': '42' if image == base_image else captured['id']}
                self.state['instances'].append(rec)
                gcp_campaign.save(self.state_path, self.state)
                return fleet

            async def admit_guests(self):
                self.state['guest_admitted'] = True
                return [x['fleet'] for x in self.state['instances']]

            async def _describe(self, kind, name, *, guest=False):
                if kind != 'disk' or not guest:
                    raise AssertionError('boot disk must be guest-described')
                rec = next(x for x in self.state['instances'] if x['name'] == name)
                self.state['image_admitted'] = True
                return {'name': name, 'id': rec['disk_id'], 'sourceImage': rec['source_image'],
                        'sourceImageId': rec['source_image_id'], 'description': json.dumps({
                            'run': self.state['run_id'], 'source': source, 'expiry': self.state['deadline'],
                            'token': rec['disk_token']}, sort_keys=True)}

            async def cleanup(self):
                events.append(('cleanup', self.state['run_id']))



        async def execute(argv):
            return {'id': captured['id'], 'status': 'READY',
                    'selfLink': 'https://www.googleapis.com/compute/v1/' + captured['path']}

        store = GuestStore()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / 'prepared'
            package_dir.mkdir()
            (package_dir / 'spec.json').write_text(json.dumps(manual.REVIEWED_SPEC))
            archive = root / 'bundle.tar.gz'
            archive.write_bytes(b'sealed archive')
            wheel = root / 'PyYAML-6.0.2-cp311-cp311-manylinux_2_17_x86_64.whl'
            wheel.write_bytes(b'wheel')
            receipt = {'archive_path': str(archive), 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
                       'source_revision': revision, 'source_sha256': source, 'manifest_sha256': manifest,
                       'binary_blake2b512': 'e' * 128, 'abi': abi}
            frozen = {'tree_sha256': 'd' * 64, 'dirty': True}
            with patch.object(manual, '_admit_live_pair'), \
                 patch.object(manual, '_admit_package', return_value=(receipt, frozen)), \
                 patch.object(manual, '_qualify_observer', return_value={}), \
                 patch.object(manual, '_check_snapshot'), \
                 patch.object(manual.gcp_manual_preflight, 'permission_preflight'), \
                 patch.object(manual.gcp_manual_preflight, 'quota_preflight', new_callable=AsyncMock, return_value=QUOTAS), \
                 patch.object(manual.gcp_manual_preflight, 'infrastructure_preflight', new_callable=AsyncMock,
                              return_value={'id': '42', 'status': 'READY',
                                  'selfLink': 'https://www.googleapis.com/compute/v1/' + base_image}), \
                 patch.object(manual.gcp_budget, 'reserve', return_value={}), \
                 patch('gcp_manual_compute.Compute', SimulatedCompute):
                result = await asyncio.wait_for(manual.run(CONFIG, package_dir=package_dir, revision=revision,
                    spec=manual.REVIEWED_SPEC, wheels=[{'path': str(wheel), 'sha256': hashlib.sha256(b'wheel').hexdigest()}],
                    base_image=base_image, base_image_id='42', rdma_device='irdma0',
                    output=root / 'out', execute=execute, artifacts=store, surcharge_cents=0), timeout=30)
            self.assertEqual(set(result['children']), {'fleet'})
            self.assertEqual(len(result['children']['fleet']['instances']), 6)
            self.assertEqual(SimulatedCompute.counter, 6)
            self.assertEqual(sum(event[0] == 'gate' for event in events), 6)
            self.assertEqual(sum(event[0] == 'cleanup' for event in events), 1)
            operations = [event[1] for event in events if event[0] == 'mailbox']
            self.assertLess(operations.index('headroom-probe-0'), operations.index('campaign'))
            self.assertTrue(all(operations.index(f'headroom-cleanup-{i}') < operations.index('campaign')
                                for i in range(1, 6)))
            self.assertEqual(result['terminal']['exit_code'], 0)
            self.assertTrue((root / 'out/results/controller.tar.gz').exists())
            self.assertTrue(json.loads((root / 'out/state.json').read_text())['cleanup_verified'])
            self.assertIsNone(gcp_lease.read(store))




class ReviewedKvMatrixRegressionTests(unittest.TestCase):
    def test_reviewed_matrix_geometry(self):
        cases = (
            ('nvmeof_parallel_writes', 44),
            ('nvmeof_f6', 42),
            ('nvmeof_f6b', 58),
            ('nvmeof_f8', 36),
            ('nvmeof_reads_geometry', 74),
            ('nvmeof_reads_geometry_rest', 20),
            ('nvmeof_reads_geometry_rest2', 18),
        )
        for name, count in cases:
            with self.subTest(selection=name):
                spec = manual.REVIEWED_SPECS[name]
                self.assertEqual(spec['job_count'], count)
                jobs = manual._reviewed_jobs(spec)
                self.assertEqual(len(jobs), count)
                if name == 'nvmeof_parallel_writes':
                    self.assertEqual({group: sum(job['group'] == group for job in jobs)
                                      for group in ('G23W', 'G23X', 'G23N')},
                                     {'G23W': 32, 'G23X': 6, 'G23N': 6})
                if name in ('nvmeof_f6', 'nvmeof_f6b', 'nvmeof_f8'):
                    self.assertEqual(spec['machine_type'], 'z4m-highmem-16')
                if name == 'nvmeof_f6b':
                    with self.assertRaises(ValueError):
                        manual._reviewed_jobs({**spec, 'machine_type': 'z4m-highmem-32'})
                if name == 'nvmeof_f8':
                    self.assertNotIn('live_leased_sync', spec)
                    self.assertNotIn('live_leased_flush_min_frame_bytes', spec)

    def test_reviewed_standard_specs_keep_their_synchronous_matrices(self):
        # Digests of the generated matrices before native became the matrix default.
        campaign = manual.workflow_input.cloud_campaign
        for spec, count, digest, relies_on_spec_tier in (
                (manual.REVIEWED_SPEC, 652, 'f48f60156261589c539ca030d535d80cba8a0d987df30184d597a419f82adb9d', True),
                # Contract arms name every Tidehunter tier, so it never used the spec default.
                (manual.REVIEWED_CONTRACT_SPEC, 156, '32cda41becb0128f3d7f50cf02a4ffc4c9c9650ce9e708226516d862e1cca8e2',
                 False)):
            with self.subTest(shapes=spec.get('shapes')):
                self.assertEqual(spec['durability_tier'], 'synchronous')
                jobs = campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
                self.assertEqual(len(jobs), count)
                self.assertEqual(hashlib.sha256(json.dumps(
                    jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(), digest)
                unpinned = campaign.make_matrix({k: v for k, v in spec.items()
                                                 if k not in ('suite', 'durability_tier')})
                # Without the explicit tier, the standard spec would plan the native default.
                self.assertEqual(jobs != unpinned, relies_on_spec_tier)


if __name__ == '__main__':
    unittest.main()
