import copy
import hashlib
import json
import tempfile
from unittest import mock
import unittest
from collections import Counter
from pathlib import Path

import cloud_campaign as campaign
import cloud_layout


class FocusedMatrixTests(unittest.TestCase):
    def test_geometry_prefix_and_ids(self):
        full = campaign.make_focused_matrix('full')
        recommended = campaign.make_focused_matrix('recommended')
        self.assertEqual(len(full), 333)
        self.assertEqual(len(recommended), 279)
        self.assertEqual(recommended, full[:279])
        self.assertEqual(full, campaign.make_focused_matrix('full'))
        for selection, jobs in (('full', full), ('recommended', recommended)):
            counts = campaign.focused_group_counts(selection)
            for field in ('group', 'case_kind'):
                for name, count in Counter(j[field] for j in jobs).items():
                    self.assertEqual(counts[name], count)
        self.assertTrue(all(j['selection'] == 'recommended' for j in recommended))
        self.assertTrue(all(j['selection'] == 'full' for j in full[279:]))
        self.assertEqual(len({j['case_id'] for j in full}), len(full))
        old = [j for j in recommended if j['target_io'] != 'direct']
        digest = hashlib.sha256(json.dumps(old, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest, 'd95f90a4daffb2557e445bebe67436c0c782a131c31c49953e7a5decd542131e')
        self.assertEqual({j['backend'] for j in full}, {'native', 'tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'})
        for job in full:
            self.assertIs(campaign.validate_focused_job(job), job)
    def test_device_matrix_and_configuration(self):
        jobs = campaign.make_focused_matrix('device')
        self.assertEqual(jobs, campaign._focused_jobs()[333:453])
        self.assertEqual(len(jobs), 120)
        digests = {
            'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
            'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
            'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
        }
        for selection, expected in digests.items():
            data = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
        self.assertEqual([job['case_kind'] for job in jobs[:7]],
                         ['media_read'] * 4 + ['remote_put'] * 3)
        for job in jobs:
            self.assertIs(campaign.validate_focused_job(job), job)
            config = campaign.make_run_config(job, Path('/tmp/device-case'), {}, 'revision')
            self.assertEqual(config['dataset']['keys'], job['keys'])
            self.assertFalse(config['phases'].get('measurement_key_sweep', False))
            expected_eviction = job['case_kind'] == 'media_read' and job['backend'] != 'tidehunter_nvme_rdma'
            self.assertEqual(config['phases'].get('evict_mapped_wal_before_measurement', False), expected_eviction)
            self.assertEqual(config['backend']['options']['durability_tier'], job['durability_tier'])
            if job['case_kind'] == 'remote_put':
                self.assertTrue(config['phases']['preload'])
                self.assertEqual(config['execution']['mode'], 'closed_loop')
        altered = copy.deepcopy(jobs[0])
        altered['keys'] += 1
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(altered)

    def test_floors_matrix_is_stable_and_valid(self):
        jobs = campaign.make_focused_matrix('floors')
        self.assertEqual(jobs, campaign._focused_jobs()[453:522])
        self.assertEqual(len(jobs), 69)
        self.assertEqual(Counter(j['group'] for j in jobs), {'G11N': 45, 'G11S': 24})
        self.assertEqual(Counter(j['case_kind'] for j in jobs), {'net_floor': 45, 'ssd_floor': 24})
        self.assertEqual(campaign.focused_group_counts('floors'),
                         {'G11N': 45, 'G11S': 24, 'net_floor': 45, 'ssd_floor': 24})
        self.assertEqual([j['repeat'] for j in jobs], [0] * 23 + [1] * 23 + [2] * 23)
        self.assertEqual([(j['net']['tool'], j['net']['mode'], j['net']['interface'])
                          for j in jobs[:3]],
                         [('ib_send_lat', 'polled', 'rdma0'), ('fi_pingpong', None, 'rdma0'),
                          ('tcp_rtt', 'block', 'eth0')])
        self.assertEqual(jobs[1]['net']['provider'], 'verbs;ofi_rxm')
        self.assertEqual([(j['ssd']['state'], j['ssd']['block_bytes'], j['ssd']['region_bytes'])
                          for j in jobs[3:5]], [('fresh', 4096, 67108864), ('aged', 4096, 67108864)])
        data = json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(data).hexdigest(),
                         '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429')
        for job in jobs:
            self.assertEqual(job['selection'], 'floors')
            if job.get('net', {}).get('tool') == 'fi_pingpong':
                self.assertEqual(job['net']['warmup'], 0)
                altered = copy.deepcopy(job)
                altered['net']['warmup'] = 500
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            if job['case_kind'] == 'ssd_floor':
                self.assertEqual(job['ssd']['runtime_seconds'], 6)
                altered = copy.deepcopy(job)
                altered['ssd']['runtime_seconds'] = 10
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            self.assertIs(campaign.validate_focused_job(job), job)
            key = 'net' if job['case_kind'] == 'net_floor' else 'ssd'
            altered = copy.deepcopy(job)
            altered[key]['unexpected'] = True
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for job, key, field, value in ((jobs[0], 'net', 'iterations', True),
                                       (jobs[1], 'net', 'endpoint', 'msg'),
                                       (jobs[2], 'net', 'sizes', [8]),
                                       (jobs[3], 'ssd', 'age_seconds', 60)):
            altered = copy.deepcopy(job)
            altered[key][field] = value
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        altered = copy.deepcopy(campaign.make_focused_matrix('full')[0])
        altered['net'] = copy.deepcopy(jobs[0]['net'])
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(altered)


    def test_append_floors_and_frozen_unified_ids(self):
        jobs = campaign.make_focused_matrix('append_floors')
        tuples = [(j['ssd_write']['block_bytes'], j['ssd_write']['mode'],
                   j['repeat'], j['ssd_write']['host']) for j in jobs]
        expected = [(block, mode, repeat, host)
                    for block in (1024, 8192, 65536)
                    for mode in ('fdatasync', 'append_pwrite', 'append_mmap', 'append_mmap_fsync')
                    for repeat in (0, 1)
                    for host in (('client', 'owner0') if repeat == 0 else ('owner0', 'client'))]
        canary = (1024, 'append_mmap', 0, 'client')
        expected.remove(canary)
        self.assertEqual(tuples, [canary] + expected)
        self.assertEqual(len({j['case_id'] for j in jobs}), 48)
        self.assertEqual(campaign.focused_group_counts('append_floors'),
                         {'G13W': 48, 'ssd_write_floor': 48})
        for job in jobs:
            self.assertIs(campaign.validate_focused_job(job), job)
            spec = job['ssd_write']
            if spec['mode'] == 'fdatasync':
                self.assertEqual(spec['runtime_seconds'], 6)
                self.assertTrue(spec['preheat'])
            else:
                self.assertEqual((spec['warmup_operations'], spec['operations']), (512, 4096))
                self.assertNotIn('runtime_seconds', spec)
            invalid = copy.deepcopy(job)
            invalid['ssd_write']['host'] = 'other'
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(invalid)
        raw = campaign._focused_jobs()
        self.assertEqual(jobs, raw[723:771])
        self.assertEqual(hashlib.sha256(json.dumps(
            [j['case_id'] for j in jobs], separators=(',', ':')).encode()).hexdigest(),
            '7a8fb1f66a56485abe4c955362be76ce887df8a54a3518b218bc13fa95a04e5c')
        self.assertEqual(hashlib.sha256(json.dumps(
            jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40')
        ids = [j['case_id'] for j in campaign.make_focused_matrix('unified')]
        self.assertEqual(hashlib.sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest(),
                         'e3d250e3da6874851db87ef137e59fa38a52a48eae12d7ebcdbbce37f788fe18')

    def test_fair_native_matrix(self):
        raw = campaign._focused_jobs()
        fair = campaign.make_focused_matrix('fair_native')
        append = campaign.make_focused_matrix('append_floors')
        parity = campaign.make_focused_matrix('parity')
        self.assertEqual(fair, raw[771:])
        self.assertEqual(parity, fair[:1] + append + fair[1:])
        self.assertEqual(len(fair), 168)
        self.assertEqual(len(parity), 216)
        self.assertEqual(len({j['case_id'] for j in raw}), len(raw))
        groups = {'G14N': 54, 'G14S': 36, 'G14O1': 36, 'G14O5': 36, 'G14P': 6}
        self.assertEqual(Counter(j['group'] for j in fair), groups)
        self.assertEqual(campaign.focused_group_counts('fair_native'),
                         {**groups, 'native_op': 92, 'remote_put': 76})
        self.assertEqual(campaign.focused_group_counts('parity'),
                         {'G13W': 48, **groups, 'ssd_write_floor': 48,
                          'native_op': 92, 'remote_put': 76})
        self.assertEqual((fair[0]['native_durability'], fair[0]['concurrency'], fair[0]['value_size']),
                         ('value_wal_group_barrier', 1, 1024))
        for selection, expected in (
                ('fair_native', '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9'),
                ('parity', 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6')):
            data = json.dumps(campaign.make_focused_matrix(selection),
                              sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
        for group, shards, concurrencies in (('G14N', 1, (1, 5, 16)), ('G14S', 5, (5, 16))):
            self.assertEqual({(j['native_shards'], j['native_durability'], j['value_size'],
                               j['concurrency'], j['repeat']) for j in fair if j['group'] == group},
                             {(shards, mode, size, concurrency, repeat)
                              for mode in ('page_cache', 'value_wal_barrier', 'value_wal_group_barrier')
                              for size in (1024, 8192, 65536)
                              for concurrency in concurrencies for repeat in (0, 1)})
        for group, owners in (('G14O1', 1), ('G14O5', 5)):
            self.assertEqual({(j['owner_count'], j['backend'], j['value_size'], j['concurrency'], j['repeat'])
                              for j in fair if j['group'] == group},
                             {(owners, backend, size, concurrency, repeat)
                              for backend in ('tidehunter_verbs_native', 'tidehunter_tcp')
                              for size in (1024, 8192, 65536)
                              for concurrency in (1, 5, 16) for repeat in (0, 1)})
        profiles = [j for j in fair if j['group'] == 'G14P']
        self.assertEqual({(j['backend'], j['concurrency'], j.get('owner_count'), j.get('native_shards'))
                          for j in profiles},
                         {('tidehunter_verbs_native', 1, 5, None),
                          ('tidehunter_tcp', 1, 5, None),
                          ('tidehunter_verbs_native', 16, 5, None),
                          ('tidehunter_verbs_native', 16, 1, None),
                          ('native', 16, None, 1), ('native', 1, None, 1)})
        for job in fair:
            self.assertEqual((job['selection'], job['profile'], job['cohort']),
                             ('fair_native', 'write-concurrent', 'primary_rf1'))
            self.assertEqual((job['keys'], job['measurement_operations'], job['warmup_operations']),
                             (4096, 4096, 512))
            self.assertEqual(job['mix']['put'], 100)
            self.assertNotIn('put_key_policy', job)
            if job['backend'] == 'native':
                self.assertEqual(job['native_wal_geometry'], 'native_default')
                self.assertEqual(job['native_durability'], job['durability_tier'])
            else:
                self.assertEqual(job['durability_tier'], 'synchronous')
            if job['group'] == 'G14P':
                self.assertEqual((job['value_size'], job['repeat']), (65536, 0))
                self.assertEqual(job['diagnostics'], {'write_profile_limit': 4096})
            self.assertIs(campaign.validate_focused_job(job), job)
            altered = copy.deepcopy(job)
            altered['measurement_operations'] += 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)

    def test_compact_selections_partition_parity(self):
        parity = campaign.make_focused_matrix('parity')
        compact = campaign.make_focused_matrix('parity_compact')
        five = campaign.make_focused_matrix('fair_five_owner')
        self.assertEqual((len(compact), len(five)), (177, 39))
        compact_ids = [job['case_id'] for job in compact]
        five_ids = [job['case_id'] for job in five]
        self.assertEqual(len(set(compact_ids + five_ids)), len(parity))
        self.assertEqual(set(compact_ids + five_ids), {job['case_id'] for job in parity})
        self.assertEqual(compact_ids, [j['case_id'] for j in parity
                                      if j['group'] in ('G13W', 'G14N', 'G14S', 'G14O1')
                                      or (j['group'] == 'G14P'
                                          and (j['backend'] == 'native' or j.get('owner_count') == 1))])
        self.assertEqual(five_ids, [j['case_id'] for j in parity if j['case_id'] not in set(compact_ids)])
        self.assertEqual(compact[0], parity[0])
        self.assertEqual(five[0]['group'], 'G14O5')
        self.assertEqual((campaign.selection_owner_hosts('parity_compact'),
                          campaign.selection_owner_hosts('fair_five_owner')), (1, 5))
        for job in compact:
            self.assertEqual(campaign.job_owner_hosts(job), 1)
        for job in five:
            self.assertEqual(campaign.job_owner_hosts(job), 5)
        for selection, digest in (
            ('parity_compact', '4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa'),
            ('fair_five_owner', '479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946'),
            ('unified', 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506'),
            ('append_floors', '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40'),
            ('fair_native', '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9'),
            ('parity', 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6')):
            encoded = json.dumps(campaign.make_focused_matrix(selection),
                                 sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(encoded).hexdigest(), digest)

    def test_owner_host_requirements(self):
        self.assertIs(campaign.OWNER_HOST_COUNTS, cloud_layout.OWNER_HOST_COUNTS)
        floors = campaign.make_focused_matrix('append_floors')
        self.assertEqual({job['ssd_write']['mode'] for job in floors},
                         {'fdatasync', 'append_pwrite', 'append_mmap', 'append_mmap_fsync'})
        self.assertTrue(all(campaign.job_owner_hosts(job) == 1 for job in floors))
        self.assertEqual(campaign.job_owner_hosts({'backend': 'native'}), 1)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': 1}), 1)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp'}), 5)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': 5}), 5)
        for invalid in (True, 0, 2, '1', None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': invalid})

    def test_fair_topology_identity_and_native_options(self):
        jobs = campaign.make_focused_matrix('fair_native')
        for field in ('native_shards', 'owner_count'):
            job = next(j for j in jobs if j.get(field) == 1)
            for invalid in (True, 1.0, '1', 5, None):
                altered = copy.deepcopy(job)
                altered[field] = invalid
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            del altered[field]
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for shards in (1, 5):
            job = next(j for j in jobs if j.get('native_shards') == shards)
            config = campaign.make_run_config(job, '/tmp/case', {}, 'revision')
            self.assertEqual(config['backend']['options']['native_shards'], shards)
            for invalid in (True, float(shards), str(shards), 6 - shards):
                with self.assertRaises(ValueError):
                    campaign.make_run_config(job, '/tmp/case', {'native_shards': invalid}, 'revision')

    def test_active_owner_evidence_and_restrictions(self):
        self.assertEqual(campaign.active_partitions({}), {'0', '1', '2', '3', '4'})
        base = {'owner_count': 1, 'case_kind': 'remote_put',
                'backend': 'tidehunter_verbs_native', 'durability_tier': 'synchronous'}
        self.assertEqual(campaign.active_partitions(base), {'0'})
        for count in (True, 1.0, '1', 0, 2):
            with self.assertRaises(ValueError):
                campaign.active_owner_count({'owner_count': count})
        for field, value in (('sealed_dataset', {}), ('nvmeof_route', 'live'),
                             ('case_kind', 'interference'), ('case_kind', 'hybrid_put'),
                             ('case_kind', 'sealed_knee'), ('case_kind', 'ssd_floor'),
                             ('case_kind', 'ssd_write_floor'), ('case_kind', 'net_floor'),
                             ('case_kind', 'fio'), ('case_kind', 'unknown'),
                             ('backend', 'tidehunter_nvme_rdma'),
                             ('durability_tier', 'page_cache')):
            with self.subTest(field=field, value=value):
                invalid = {**base, field: value}
                with self.assertRaises(ValueError):
                    campaign.active_owner_count(invalid)
                with self.assertRaisesRegex(ValueError, 'one owner is restricted'):
                    campaign.validate_focused_job(invalid)
        job = {**base, 'cohort': 'registered_ram_fullset_control',
               'warmup_key_sweep': True, 'resident_reads': {'require_device_read': True}}
        counters = {name: 0 for name in ('message_read_requests', 'grant_acquisitions',
                    'grant_candidates', 'rma_errors', 'fallback_missing', 'fallback_oversize',
                    'fallback_capacity', 'fallback_unsupported', 'fallback_stale',
                    'wal_reads', 'async_read_submissions', 'cache_hits')}
        counters.update(rma_posts=1, rma_completions=1)
        row = {'available': True, 'counters': counters}
        evidence = {'measurement_drained': True, 'deltas': {
            'client_resident': {'available': True, 'partitions': {'0': row}},
            'owner_executor': {'available': True, 'partitions': {'0': row}},
            'coordinator': {'available': True, 'counters': {'hits': 0}}}}
        self.assertTrue(campaign.qualify_fullset_read_path(job, evidence)['qualified'])
        evidence['deltas']['owner_executor']['partitions']['1'] = row
        self.assertFalse(campaign.qualify_fullset_read_path(job, evidence)['qualified'])

    def test_one_owner_read_tier_partition_counters(self):
        job = {'owner_count': 1, 'case_kind': 'remote_put', 'backend': 'tidehunter_tcp',
               'durability_tier': 'synchronous', 'read_tier': {'name': 'wal_warm', 'requested': {}}}
        row = {'available': True, 'counters': {'wal_reads': 2, 'hits': 0}}
        boundary = {'available': True, 'partitions': {'0': {'authority': {'history_loads': 3}}}}
        end = {'available': True, 'partitions': {'0': {'authority': {'history_loads': 7}}}}
        evidence = {'measurement_drained': True, 'start': {'owner_executor': boundary},
                    'end': {'owner_executor': end}, 'deltas': {
                        'owner_executor': {'available': True, 'partitions': {'0': row}},
                        'coordinator': {'available': True, 'counters': {'hits': 0}}}}
        receipt = {'status': 'completed', 'read_tier_effective': {}}
        observed = campaign.qualify_read_tier(job, evidence, receipt)['observed']
        self.assertEqual((observed['wal_reads'], observed['authority_history_loads']), (2, 4))
        evidence['deltas']['owner_executor']['partitions']['1'] = row
        self.assertIsNone(campaign.qualify_read_tier(job, evidence, receipt)['observed']['wal_reads'])

    def test_owner_receipt_count_and_partition_binding(self):
        job = next(j for j in campaign.make_focused_matrix('fair_native')
                   if j.get('owner_count') == 1)
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            receipt = {'job': job, 'status': 'completed'}
            with mock.patch.object(campaign.analyze, 'admit_focused_artifacts', return_value={}):
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'lacks owner durability'):
                    campaign.summarize_case(root)
                receipt['owner_durability_receipts'] = []
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'invalid owner durability'):
                    campaign.summarize_case(root)
                receipt['owner_durability_receipts'] = [{}, {}]
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'invalid owner durability'):
                    campaign.summarize_case(root)
                path = root / 'owner-durability/node-0.durability.json'
                path.parent.mkdir()
                data = json.dumps({'partition': 1}).encode()
                path.write_bytes(data)
                receipt['owner_durability_receipts'] = [{'path': str(path.relative_to(root)),
                    'sha256': hashlib.sha256(data).hexdigest()}]
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'partition differs'):
                    campaign.summarize_case(root)

    def test_stages_and_unified_are_stable_and_valid(self):
        stages = campaign.make_focused_matrix('stages')
        unified = campaign.make_focused_matrix('unified')
        floors = campaign.make_focused_matrix('floors')
        self.assertEqual(unified[:183], stages)
        self.assertEqual(unified[183:252], floors)
        self.assertEqual(len(stages), 183)
        self.assertEqual(len(unified), 270)
        counts = {'G12N': 52, 'G12W': 60, 'G12H': 12, 'G12K': 12, 'G12D': 43, 'G12S': 4}
        self.assertEqual(Counter(j['group'] for j in stages), counts)
        self.assertEqual(Counter(j['group'] for j in unified), counts | {'G11N': 45, 'G11S': 24, 'G11W': 18})
        self.assertEqual([j['case_kind'] for j in stages[:11]],
                         ['native_op', 'native_media_read', 'remote_put', 'remote_put', 'remote_put',
                          'hybrid_put', 'hybrid_put', 'remote_put', 'remote_put', 'media_read', 'native_op'])
        self.assertEqual([next(k for k, v in j['mix'].items() if v) for j in stages[:8]],
                         ['put', 'read', 'put', 'put_batch', 'delete', 'put', 'put', 'put'])
        self.assertEqual([j['nvmeof_target'] for j in stages[5:7]], ['kernel', 'spdk'])
        self.assertEqual(stages[7]['put_key_policy'], 'new')
        digests = {
            'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
            'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
            'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
            'floors': '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429',
            'stages': '646207fe6727aa37367b727759a4d46005ebd56a59f29ec49b13a690de24a724',
            'unified': 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506',
        }
        for selection, expected in digests.items():
            raw = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), expected)
        for job in stages:
            self.assertIs(campaign.validate_focused_job(job), job)
            config = campaign.make_run_config(job, Path('/tmp/stages-case'), {}, 'revision')
            self.assertEqual(config['workload']['batch_size'], 32)
            if job['mix']['put_batch']:
                self.assertEqual(job['measurement_operations'], 1024)
            if 'put_key_policy' in job:
                self.assertEqual(config['workload']['put_key_policy'], 'new')
            if 'diagnostics' in job:
                self.assertEqual(config['diagnostics'], job['diagnostics'])
            if job['case_kind'] == 'hybrid_put':
                self.assertFalse(config['phases']['verify_existing_dataset'])
                self.assertTrue(config['phases']['preload'])
                self.assertNotIn('key_namespace_hex', config)
            for field in ('put_key_policy', 'diagnostics'):
                if field in job:
                    invalid = copy.deepcopy(job)
                    del invalid[field]
                    with self.assertRaises(ValueError):
                        campaign.validate_focused_job(invalid)
            altered = copy.deepcopy(job)
            execution, workload = config['execution'], config['workload']
            self.assertTrue(1 <= execution['concurrency'] <= execution['queue_capacity'] <= 65536)
            self.assertLessEqual(execution['concurrency'], 4096)
            self.assertTrue(2 <= workload['batch_size'] <= 1024)
            self.assertGreater(sum(workload['mix'].values()), 0)
            self.assertLessEqual(config['dataset']['keys'] * config['dataset']['value_size'],
                                 config['limits']['max_bytes'])
            if job['backend'] != 'native':
                self.assertTrue(1 <= job['read_lanes'] <= 8)
                # Default owner admission: 32 operations and 320 KiB encoded.
                batch_ops = workload['batch_size'] if workload['mix']['put_batch'] else 2
                self.assertLessEqual(batch_ops, 32)
                self.assertLessEqual((job['value_size'] + 32 + 32) * batch_ops + 96, 320 * 1024)
            diagnostics = config.get('diagnostics', {})
            self.assertTrue(all(0 <= limit <= 65536 for limit in diagnostics.values()))
            self.assertFalse(diagnostics.get('write_profile_limit') and diagnostics.get('write_timeline_limit'))
            if diagnostics.get('write_timeline_limit') or diagnostics.get('read_profile_limit'):
                self.assertEqual(config['backend']['kind'], 'tidehunter')
            if workload.get('put_key_policy') == 'new':
                self.assertTrue(workload['mix']['put'] or workload['mix']['put_batch'])
                self.assertFalse(any(workload['mix'][op] for op in ('read', 'get_many', 'missing_read', 'delete')))
                self.assertFalse(config['phases'].get('measurement_key_sweep', False))
            altered['concurrency'] += 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for job in (j for j in stages if j['case_kind'] == 'native_media_read'):
            self.assertTrue(job['drop_os_caches'] and job['evict_mapped_wal_before_measurement'])
            self.assertEqual(job['key_order'], 'random_with_replacement')
            self.assertFalse(job['measurement_key_sweep'])
        for job in (j for j in stages if j['case_kind'] == 'hybrid_put'):
            self.assertEqual(job['read_lanes'], 4)
            self.assertEqual(job['nvmeof_route'], 'sealed_read')
            self.assertEqual(job['keys'], 4096)
        profiled_reads = [j for j in stages if j['group'] == 'G12D' and j['case_kind'] == 'media_read']
        self.assertEqual(len(profiled_reads), 22)
        self.assertTrue(all(j['measurement_operations'] <= j['diagnostics']['read_profile_limit']
                            for j in profiled_reads))
        c16 = [j for j in profiled_reads if j['concurrency'] == 16]
        self.assertEqual(len(c16), 6)
        self.assertEqual(Counter((j['backend'], j.get('nvmeof_target'), j['media']['expected'])
                                 for j in c16),
                         {('tidehunter_verbs_native', None, 'memory'): 1,
                          ('tidehunter_verbs_native', None, 'cold'): 1,
                          ('tidehunter_tcp', None, 'memory'): 1, ('tidehunter_tcp', None, 'cold'): 1,
                          ('tidehunter_nvme_rdma', 'kernel', 'cold'): 1,
                          ('tidehunter_nvme_rdma', 'spdk', 'cold'): 1})
        self.assertEqual(Counter(j['media']['expected'] for j in profiled_reads
                                 if j['backend'] == 'tidehunter_verbs'), {'memory': 1, 'cold': 1})
        for operation, size in (('put', 65536), ('put_batch', 8192), ('delete', 1024)):
            self.assertTrue(any(j['group'] == 'G12D' and j['backend'] == 'tidehunter_tcp'
                                and j['mix'][operation] == 100 and j['value_size'] == size
                                and j['concurrency'] == 1 and j['diagnostics'].get('write_timeline_limit') == 4096
                                for j in stages))
        writes = unified[252:]
        self.assertEqual(len(writes), 18)
        self.assertEqual((writes[0]['repeat'], writes[0]['ssd_write']['mode'],
                          writes[0]['ssd_write']['block_bytes']), (0, 'fdatasync', 1024))
        self.assertEqual({(j['repeat'], j['ssd_write']['mode'], j['ssd_write']['block_bytes']) for j in writes},
                         {(r, mode, size) for r in (0, 1) for mode in ('buffered', 'fdatasync', 'direct')
                          for size in (1024, 8192, 65536)})
        for job in writes:
            self.assertEqual(job['selection'], 'unified')
            self.assertIs(campaign.validate_focused_job(job), job)
            self.assertEqual(job['ssd_write']['region_bytes'], 536870912)
            self.assertEqual(job['ssd_write']['runtime_seconds'], 6)
            self.assertEqual(job['ssd_write']['queue_depth'], 1)
            self.assertEqual(job['ssd_write']['location'], 'local_file')
            self.assertIs(job['ssd_write']['preheat'], True)
            altered = copy.deepcopy(job)
            altered['ssd_write']['preheat'] = False
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            altered['ssd_write']['unknown'] = 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)

    def test_all_jobs_fit_external_storage_batch_limit(self):
        # prepare_external_case_file rejects more than 1,000,000 batches per partition.
        backend_limit = 1_000_000
        for job in campaign.make_focused_matrix('full'):
            bound = max(16384, job['keys'] + job['warmup_operations'] + job['measurement_operations'])
            self.assertLessEqual(bound, backend_limit, job['case_id'])
        interference = [j for j in campaign.make_focused_matrix('full') if j['case_kind'] == 'interference']
        self.assertTrue(all(j['measurement_operations'] == 400000 for j in interference))

    def test_sealed_datasets_fit_the_preparer_and_the_hybrid_catalog_budget(self):
        # The sealed preparer (nvmeof_campaign.rs MAX_KEYS) rejects more than 65,536 keys. A hybrid reader
        # charges 4 KiB per catalog entry of its owner plus fixed budgets against a 448 MiB coordinator
        # allowance (cloud_controller.connect_nvme). Either limit aborts the campaign at preparation.
        mib = 1024 ** 2
        allowance = 512 * mib - 64 * mib
        for job in campaign.make_focused_matrix('full') + campaign.make_focused_matrix('device'):
            bound = max(16384, job['keys'] + job['warmup_operations'] + job['measurement_operations'])
            self.assertLessEqual(bound, 1_000_000, job['case_id'])
            if 'sealed_dataset' not in job:
                continue
            self.assertLessEqual(job['keys'], 65536, job['case_id'])
            if job['backend'] == 'tidehunter_nvme_rdma':
                entries = -(-job['keys'] // 5)
                charged = entries * 4096 + 223 + 32 * 4096 + 128 * 8192 + 4096
                reader = -(-charged // mib) * mib + 16 * mib + 128 * 2048 + 32 * 1024
                self.assertLessEqual(5 * reader, allowance, job['case_id'])

    def test_sources_are_repeat_isolated_and_contiguous(self):
        jobs = campaign.make_focused_matrix('full')
        sources = {}
        for index, job in enumerate(jobs):
            source = job.get('sealed_dataset')
            if source:
                sources.setdefault(source['source_id'], []).append((index, job['repeat']))
                self.assertIn('r' + str(job['repeat']), source['data_id'])
        for positions in sources.values():
            self.assertEqual(len({r for _, r in positions}), 1)
            self.assertEqual([i for i, _ in positions], list(range(positions[0][0], positions[-1][0] + 1)))

    def test_strict_admission(self):
        job = campaign.make_focused_matrix('full')[0]
        for change in ({'unexpected': 1}, {'repeat': True}, {'case_id': 'wrong'},
                       {'target_io': 'direct'}, {'case_kind': 'sui_trace_replay'},
                       {'value_size': 1025}, {'execution': {'queue_capacity': 256, 'extra': 1}}):
            candidate = copy.deepcopy(job)
            candidate.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.validate_focused_job(candidate)
        candidate = copy.deepcopy(job)
        candidate['sealed_dataset']['extra'] = 1
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(candidate)
        for selection in ('unknown', None, 1):
            with self.assertRaises(ValueError):
                campaign.make_focused_matrix(selection)

    def test_routes_and_execution(self):
        jobs = campaign.make_focused_matrix('full')
        for job in jobs:
            config = campaign.make_run_config(job, Path('/tmp/focused-case'), {}, 'revision')
            self.assertEqual(config['case_id'], job['case_id'])
            if job['backend'] == 'tidehunter_nvme_rdma':
                self.assertEqual(job['nvmeof_route'], 'sealed_read')
                self.assertIn(job['target_io'], ('buffered', 'spdk_direct', 'direct'))
            if job['group'] == 'G7' and job['mix']['put']:
                self.assertNotEqual(job['backend'], 'tidehunter_nvme_rdma')
            if job['group'] == 'G7':
                self.assertEqual(config['case_id'], job['case_id'])
                self.assertGreaterEqual(config['execution']['queue_capacity'], job['concurrency'])
        hybrid = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_rdma')
        config = campaign.make_run_config(hybrid, Path('/tmp/focused-case'), {}, 'revision')
        self.assertEqual(config['backend']['options']['transport'], campaign.transport_for_case('tidehunter_verbs_native'))
        interference = next(j for j in jobs if j["group"] == "G4")
        config = campaign.make_run_config(interference, Path('/tmp/focused-case'), {}, 'revision')
        self.assertEqual(config["execution"]["mode"], "interference")
        self.assertEqual(config["case_id"], interference["case_id"])
        self.assertEqual(config["interference"], interference["interference"])
        self.assertGreaterEqual(config["execution"]["queue_capacity"],
                                config["execution"]["concurrency"] + interference["interference"]["put_concurrency"])
        self.assertGreater(config["phases"]["measurement_operations"], 20000)

    def test_reviewed_kind_dimensions(self):
        jobs = campaign.make_focused_matrix('full')
        self.assertEqual({j['trace']['scope'] for j in jobs if j['group'] == 'G3'}, {'cold_owner_read', 'owner_put'})
        self.assertEqual({j['fio']['location'] for j in jobs if j['group'] == 'G2'}, {'local_file', 'nvmeof_namespace'})
        direct = [j for j in jobs if j['target_io'] == 'direct']
        self.assertEqual(len(direct), 9)
        for job in direct:
            self.assertEqual((job['group'], job['case_kind'], job['backend'], job['nvmeof_route'],
                              job['nvmeof_target'], job['media']['expected'], job['concurrency']),
                             ('G1', 'media_read', 'tidehunter_nvme_rdma', 'sealed_read', 'kernel', 'cold', 1))
            self.assertEqual(job['sealed_dataset']['directness'], 'initiator_O_DIRECT_target_kernel_buffered_io_0')
            self.assertEqual(job['sealed_dataset']['target_cache'], 'direct_kernel_buffered_io_0_no_target_pagecache')
            campaign.validate_focused_job(job)
            for field, value in (('group', 'G4'), ('group', 'G5'), ('group', 'G7'),
                                 ('nvmeof_route', 'live'), ('nvmeof_target', 'spdk'),
                                 ('mix', campaign.mix(put=100))):
                altered = copy.deepcopy(job)
                altered[field] = value
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            altered['media']['expected'] = 'memory'
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        self.assertEqual({j['write_wait_diagnostics']['enabled'] for j in jobs if j['group'] == 'G9'}, {False, True})
        prewarm = [j for j in jobs if j.get('media', {}).get('history') == 'prewarm']
        self.assertEqual(len(prewarm), 9)
        for job in prewarm:
            self.assertFalse(job['drop_os_caches'])
            self.assertTrue(job['evict_mapped_wal_before_measurement'])
            self.assertEqual(job['sealed_dataset']['mapped_wal_eviction'], 'before_measurement_gate')
            config = campaign.make_run_config(job, Path('/tmp/focused-case'), {}, 'revision')
            self.assertTrue(config['phases']['evict_mapped_wal_before_measurement'])
            self.assertEqual(config['phases']['warmup_operations'], job['keys'])
            self.assertTrue(config['phases']['warmup_key_sweep'])
        for job in jobs:
            if job['group'] == 'G4':
                self.assertEqual(job['interference'], {'read_offered_per_second': 1000, 'read_queue_capacity': 4096,
                    'put_concurrency': 16, 'duration_seconds': 20, 'key_policy': 'disjoint_read_put'})
