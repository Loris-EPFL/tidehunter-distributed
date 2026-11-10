import unittest

import analyze


class FocusedStreamTests(unittest.TestCase):
    def test_streams_reconcile_and_keep_rejections_separate(self):
        rows = [
            {'stream': 'read', 'operation': 'read', 'outcome': 'success', 'dispatch_observed': 'true',
             'logical_keys': 1, 'api_ns': 10, 'response_ns': 20, 'scheduling_ns': 10,
             'dispatched_ns': 10, 'completed_ns': 20},
            {'stream': 'read', 'operation': 'read', 'outcome': 'rejected', 'dispatch_observed': 'false',
             'logical_keys': 1, 'api_ns': None, 'response_ns': 9, 'scheduling_ns': None},
            {'stream': 'put', 'operation': 'put', 'outcome': 'success', 'dispatch_observed': 'true',
             'logical_keys': 1, 'api_ns': 30, 'response_ns': 35, 'scheduling_ns': 5,
             'dispatched_ns': 5, 'completed_ns': 35},
        ]
        evidence = {
            'read': {'offered': 2, 'admitted': 1, 'completed': 1, 'successful': 1, 'rejected': 1, 'failed': 0,
                     'first_dispatch_ns': 10, 'last_completion_ns': 20},
            'put': {'offered': 1, 'admitted': 1, 'completed': 1, 'successful': 1, 'rejected': 0, 'failed': 0,
                    'first_dispatch_ns': 5, 'last_completion_ns': 35},
        }
        populations = analyze.reconcile_interference_samples(rows, evidence)
        self.assertEqual({(x['stream'], x['operation'], x['outcome']) for x in populations},
                         {('read', 'read', 'success'), ('read', 'read', 'rejected'), ('put', 'put', 'success')})
        rejected = next(x for x in populations if x['outcome'] == 'rejected')
        self.assertEqual(rejected['api_ns']['sample_count'], 0)
        evidence['read']['successful'] = 0
        with self.assertRaisesRegex(ValueError, 'counters differ'):
            analyze.reconcile_interference_samples(rows, evidence)
        evidence['read']['successful'] = 1
        rows[0]['stream'] = 'put'
        with self.assertRaisesRegex(ValueError, 'stream differs'):
            analyze.reconcile_interference_samples(rows, evidence)


    def test_owner_route_is_proven_by_effective_configuration(self):
        job = {'backend': 'tidehunter_verbs_native', 'durability_tier': 'synchronous'}
        backend = {'configuration': {'read_backend': 'owner_record_protocol',
                                     'write_backend': 'owner_record_protocol', 'nvmeof_readers': []},
                   'acknowledgement': 'covering fsync complete'}
        manifest = {'durability': {'confirmed_by_backend_receipt': 'synchronous'}}
        streams = {'read': {'successful': 10}, 'put': {'successful': 2}}
        for owner in ('tidehunter_verbs_native', 'tidehunter_tcp'):
            job['backend'] = owner
            result = analyze.qualify_interference_route(job, backend, manifest, {}, None, streams)
            self.assertTrue(result['qualified'])
            self.assertEqual(result['notes'], ['write_route_configured_only'])
        for field, value in (('nvmeof_readers', [{'id': 1}]), ('read_backend', 'live_nvmeof'),
                             ('write_backend', 'live_nvmeof'), ('nvmeof_readers', None)):
            altered = {'configuration': dict(backend['configuration']), 'acknowledgement': backend['acknowledgement']}
            altered['configuration'][field] = value
            result = analyze.qualify_interference_route(job, altered, manifest, {}, None, streams)
            self.assertFalse(result['qualified'])
            self.assertIn('owner_nvme_zero_unproven', result['reasons'])
        missing = {'configuration': dict(backend['configuration']), 'acknowledgement': backend['acknowledgement']}
        del missing['configuration']['nvmeof_readers']
        self.assertIn('owner_nvme_zero_unproven',
                      analyze.qualify_interference_route(job, missing, manifest, {}, None, streams)['reasons'])
        job['backend'] = 'native'
        self.assertIn('owner_nvme_zero_unproven',
                      analyze.qualify_interference_route(job, backend, manifest, {}, None, streams)['reasons'])
        job['backend'] = 'tidehunter_nvme_rdma'
        backend['configuration']['read_backend'] = 'linux_nvmeof_sealed_read_with_owner_record_fallback'
        counters = {'qualified': True, 'coordinator_read_rpc_count': 0, 'coordinator_cache_hits': 0,
                    'nvmeof_totals': {'io_completions': 9, 'io_submissions': 9, 'eligible_hits': 9,
                                      'ineligible_fallbacks': 0, 'io_errors': 0, 'io_timeouts': 0}}
        result = analyze.qualify_interference_route(job, backend, manifest, {}, counters, streams)
        self.assertTrue(result['qualified'])
        self.assertEqual((result['nvme_completions'], result['successful_reads'], result['accounting_gap']), (9, 10, 1))
        self.assertIn('coalescing_unmeasured', result['notes'])
        counters['nvmeof_totals'].update(io_completions=11, io_submissions=11)
        result = analyze.qualify_interference_route(job, backend, manifest, {}, counters, streams)
        self.assertIn('sealed_read_counter_inconsistent', result['reasons'])
        counters['nvmeof_totals'].update(io_completions=9, io_submissions=9)
        for field, value in (('coordinator_read_rpc_count', 1), ('coordinator_cache_hits', 1)):
            counters[field] = value
            self.assertIn('sealed_read_counter_inconsistent',
                          analyze.qualify_interference_route(job, backend, manifest, {}, counters, streams)['reasons'])
            counters[field] = 0
        for field in ('ineligible_fallbacks', 'io_errors', 'io_timeouts'):
            counters['nvmeof_totals'][field] = 1
            self.assertIn('sealed_read_counter_inconsistent',
                          analyze.qualify_interference_route(job, backend, manifest, {}, counters, streams)['reasons'])
            counters['nvmeof_totals'][field] = 0


if __name__ == '__main__':
    unittest.main()
