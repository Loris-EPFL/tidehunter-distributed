import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import cloud_campaign as campaign


class ValkeyReadsTests(unittest.TestCase):
    def test_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix('valkey_rdma_reads')
        self.assertEqual(len(jobs), 22)
        self.assertEqual(len({job['case_id'] for job in jobs}), 22)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                         'fbbecb9ecba897a624c388dc7ab51fe080fd2ce3363edfebf4b6069c5454c0a2')
        self.assertEqual(campaign.selection_owner_hosts('valkey_rdma_reads'), 1)
        self.assertEqual(sum(job['backend'] == 'valkey_rdma' for job in jobs), 14)
        reads = [job for job in jobs if job['group'] == 'G25R']
        self.assertEqual({(job['value_size'], job['concurrency'], job['repeat']) for job in reads},
                         {(size, callers, repeat) for size in (1024, 65536)
                          for callers in (1, 16, 64) for repeat in (0, 1)})
        for job in jobs:
            self.assertEqual(job['keys'], 65536)
            self.assertEqual(job['owner_count'], 1)
            self.assertFalse(job['drop_os_caches'])
            self.assertEqual(job['warmup_key_sweep'], job['group'] != 'G25M')
            self.assertFalse([key for key, value in job.items()
                              if value is None and key != 'offered_ops_per_second'])
        for job in jobs:
            if job['group'] == 'G25A':
                self.assertEqual(job['live_max_inflight_requests'], 128)
                self.assertIn((job['live_io_queues'], job['live_io_queue_size']),
                              ((1, 32), campaign.VALKEY_ANCHOR_GEOMETRY))
            elif job['group'] == 'G25M':
                self.assertEqual(job['mix'], campaign.mix(read=50, put=50))
                self.assertEqual(job['concurrency'], 16)


class ValkeyOwnerReceiptTests(unittest.TestCase):
    def write_case(self, root, job):
        root = Path(root)
        result = root / 'result'
        result.mkdir()
        config = campaign.make_run_config(job, root, {}, 'a' * 40)
        binary = 'b' * 128
        backend = {'name': job['backend'], 'configuration': {'ack_contract': 'synced'}}
        for name in ('config.json', 'normalized-config.json'):
            (root / name).write_text(json.dumps(config))
        (result / 'manifest.json').write_text(json.dumps({
            'status': 'completed', 'backend_info': backend,
            'run': {'config': config, 'binary': {'blake2b512': binary}}}))
        (result / 'summary.json').write_text(json.dumps({'status': 'completed', 'backend': backend}))
        (result / 'metrics.prom').write_text('')
        rows = ['phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind']
        for index, operation in enumerate(key for key in ('read', 'put') if job['mix'][key]):
            start = 1000 * (index + 1)
            rows.append(f'measurement,{operation},ok,{start},{start + 10},true,{start + 110},100,110,10,1,{job["value_size"]},')
        (result / 'samples.csv').write_text('\n'.join(rows) + '\n')
        timing = root / 'phase_timing.json'
        timing.write_text(json.dumps({'schema_version': 1, 'case_id': job['case_id'], 'spans': []}))
        receipt = {'job': job, 'status': 'completed', 'binary_blake2b512': binary,
                   'phase_timing': {'path': timing.name, 'sha256': hashlib.sha256(timing.read_bytes()).hexdigest()},
                   'artifacts_sha256': {name: hashlib.sha256((result / name).read_bytes()).hexdigest()
                       for name in ('manifest.json', 'summary.json', 'samples.csv', 'metrics.prom')}}
        (root / 'receipt.json').write_text(json.dumps(receipt))

    def test_completed_valkey_primary_needs_no_tidehunter_owner_receipt(self):
        jobs = campaign.make_focused_matrix('valkey_rdma_reads')
        selected = [next(job for job in jobs if job['group'] == 'G25R')]
        selected += [job for job in jobs if job['group'] == 'G25M']
        for job in selected:
            with self.subTest(case=job['case_id']), tempfile.TemporaryDirectory() as root:
                self.write_case(root, job)
                summary = campaign.summarize_case(root)
                self.assertEqual(summary['status'], 'completed')
                self.assertNotIn('owner_durability_receipts', summary)
                self.assertEqual({row['operation'] for row in summary['metrics']['populations']},
                                 {key for key in ('read', 'put') if job['mix'][key]})

    def test_completed_tidehunter_owner_still_requires_receipt(self):
        spec = json.loads(Path(campaign.__file__).with_name('cloud-nvmeof-campaign.json').read_text())
        job = next(job for job in campaign.make_matrix(spec) if job['backend'] in campaign.NVME_BACKENDS)
        with tempfile.TemporaryDirectory() as root:
            self.write_case(root, job)
            with self.assertRaisesRegex(ValueError, 'owner receipts did not confirm the planned durability_tier'):
                campaign.summarize_case(root)
