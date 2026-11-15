import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_package
import cloud_controller
import gcp_image_qualify
import gcp_runtime
import manual_campaign


class FocusedValkeyQualificationTests(unittest.TestCase):
    def test_guest_package_admission_requires_exact_variant(self):
        manifest = {'build_profile': 'focused_valkey',
                    'features': cloud_package.FOCUSED_VALKEY_FEATURES}
        proof = {'build_profile': 'focused_valkey', 'valkey_runtime_linkage':
                 {name: {} for name in ('valkey-server', 'valkey-cli', 'valkey-benchmark')}}
        gcp_image_qualify.admit_package(manifest, proof, 'focused')
        for change in ({'features': cloud_package.FOCUSED_FEATURES},
                       {'features': cloud_package.FEATURES + ['valkey-rdma']},
                       {'build_profile': 'focused'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                gcp_image_qualify.admit_package({**manifest, **change}, proof, 'focused')
        for change in ({'build_profile': 'focused'}, {'valkey_runtime_linkage': {}},
                       {'valkey_runtime_linkage': None}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                gcp_image_qualify.admit_package(manifest, {**proof, **change}, 'focused')
        plain = {'build_profile': 'focused', 'features': cloud_package.FOCUSED_FEATURES}
        gcp_image_qualify.admit_package(plain, {'build_profile': 'focused'}, 'focused')
        with self.assertRaisesRegex(ValueError, 'TideHunter-only'):
            gcp_image_qualify.admit_package(plain, {**proof, 'build_profile': 'focused'}, 'focused')

    def test_f3_live_anchor_admission(self):
        spec = manual_campaign.REVIEWED_SPECS['valkey_rdma_reads']
        self.assertTrue(gcp_runtime.live_mode(spec))
        for job in manual_campaign._reviewed_jobs(spec):
            campaign = cloud_controller.cloud_campaign
            self.assertEqual(campaign.active_owner_count(job), 1)
            run = campaign.make_run_config(job, '/tmp/valkey-reviewed-case', {}, 'a' * 40)
            self.assertEqual(run['execution']['concurrency'], job['concurrency'])
            with self.assertRaises(ValueError):
                campaign.active_owner_count(dict(job, concurrency=999))
            self.assertEqual(cloud_controller.admitted_live_campaign({'spec': spec}, job['case_id']),
                             job['group'] == 'G25A')
        self.assertFalse(cloud_controller.admitted_live_campaign({'spec': spec}, 'unknown'))

    def test_anchor_effective_geometry_and_counter_checks(self):
        campaign = cloud_controller.cloud_campaign
        jobs = [job for job in campaign.make_focused_matrix('valkey_rdma_reads') if job['group'] == 'G25A']
        contracts = {
            ('leased', 'synchronous'): 'synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous',
            ('leased', 'native'): 'leased extent: page-cache write',
            ('reserved', 'synchronous'): 'reserved frame: owner WAL and frontier root synchronized',
            ('reserved', 'native'): 'reserved frame: owner validated page-cache write',
        }
        for job in jobs:
            config = dict(live_export_mode=job['live_export_mode'],
                          live_queue_geometry=dict(io_queues=job['live_io_queues'],
                              queue_size=job['live_io_queue_size'], poll_queues=job['live_poll_queues']),
                          live_lease_ahead=job['live_lease_ahead'],
                          live_polled_writes=job['live_polled_writes'],
                          live_leased_write_window=None, live_leased_write_window_bound=128,
                          live_write=dict(write_mode=job['live_write_mode'],
                              acknowledgement_contract=contracts[(job['live_write_mode'], job['durability_tier'])]))
            campaign.admit_live_write_contract(job, {'configuration': config})
            bad = copy.deepcopy(config)
            bad['live_queue_geometry']['queue_size'] += 1
            with self.subTest(case=job['case_id']), self.assertRaisesRegex(ValueError, 'queue geometry'):
                campaign.admit_live_write_contract(job, {'configuration': bad})
        counters = dict.fromkeys(campaign.LIVE_NVME_COUNTERS, 0)
        proof = dict(measurement_attempted=True, measurement_drained=True,
                     start={'live_nvmeof': dict(counters)}, end={'live_nvmeof': dict(counters)},
                     live_nvmeof={'available': True, 'counters': dict(counters)})
        reasons = campaign.qualify_live_nvme_measurement(proof, jobs[0])['reasons']
        self.assertIn('invalid live NVMe counter grant_misses', reasons)

    def test_controller_configuration_and_package_variant(self):
        spec = copy.deepcopy(manual_campaign.REVIEWED_SPECS['valkey_rdma_reads'])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {name: {} for name in ('runtime/bin/valkey-server', 'runtime/bin/valkey-cli',
                     'runtime/bin/valkey-benchmark', 'runtime/valkey-rdma.json',
                     'source/experiments/kv-comparison/nvme_campaign_owner.py')}
            manifest = dict(source_sha256='a'*64, revision='b'*40, binary_sha256='c'*64,
                            binary_blake2b512='d'*128, build_profile='focused_valkey',
                            features=cloud_package.FOCUSED_VALKEY_FEATURES, files=files)
            for name, value in (('manifest.json', manifest), ('source.json', {'revision': 'b'*40}),
                                ('build.json', {})):
                (root / name).write_text(json.dumps(value))
            hosts = [dict(name=f'host-{i}', id=str(i+1), internal_ip=f'10.0.0.{i+1}',
                          rdma_ip=f'10.1.0.{i+1}', zone='us-central1-a') for i in range(2)]
            runtime = dict(package_root=directory, source_sha256='a'*64,
                           package={'source_revision': 'b'*40}, spec=spec, fleet=hosts,
                           run_id='bench-aaaaaaaaaaaaaaaa', machine_type='z4m-highmem-16',
                           bucket='bucket', project_number='123', deadline_utc='2099-01-01T00:00:00Z')
            config = gcp_runtime._controller_config(runtime)
            self.assertTrue(config['fleet']['valkey_rdma_package'])
            proof = dict(source_sha256='a'*64, binary_sha256='c'*64,
                         binary_blake2b512='d'*128, build_profile='focused_valkey')
            with patch.object(cloud_package, 'verify_directory', return_value=proof):
                self.assertIsNone(cloud_controller.verify_package(config)['fdb'])
                config['spec']['selection'] = 'nvmeof_reads_geometry'
                with self.assertRaises(cloud_controller.SafetyError):
                    cloud_controller.verify_package(config)
            manifest['features'] = cloud_package.FOCUSED_FEATURES
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'exact selected'):
                gcp_runtime._controller_config(runtime)
