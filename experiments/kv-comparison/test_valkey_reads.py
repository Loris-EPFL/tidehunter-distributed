import hashlib
import json
import unittest
import cloud_campaign as campaign


class ValkeyReadsTests(unittest.TestCase):
    def test_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix('valkey_rdma_reads')
        self.assertEqual(len(jobs), 22)
        self.assertEqual(len({job['case_id'] for job in jobs}), 22)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                         '784c732c8f9c641d6e5e594a1bc271fcccd69fb90e534fbaa6dd995ef9f07cfc')
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
            self.assertTrue(job['warmup_key_sweep'])
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
