import copy
import json
from pathlib import Path
import tempfile
import unittest

import cloud_campaign as cloud


class CorrelatedAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'result').mkdir()
        (self.root / 'result/samples.csv').write_text(
            'phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id\n'
            'measurement,get_many,success,0,1,true,11,10,11,1,2,16,,17\n')
        self.job = {'backend': 'valkey_tcp', 'diagnostics': {'valkey_profile_limit': 2}}
        self.config = {'backend': {'kind': 'valkey_tcp', 'options': {'client': 'libvalkey'}},
                       'diagnostics': {'valkey_profile_limit': 2}}
        self.record = {'caller_sample_id': 17, 'operation': 'get_many', 'owner': 0, 'connection': 1,
                       'mutex_wait_ns': 4, 'blocking_enqueue_to_start_ns': 5,
                       'blocking_return_to_wake_ns': None, 'command_encode_ns': 2,
                       'transport_and_parser_ns': 8, 'reply_decode_ns': 1, 'cq': None,
                       'worker_finished': False, 'pipeline_outcome': 'cancelled'}
        self.profile = {'available': True, 'source_commit': 'fe8d406c0b5a64ec897f6455e4de684629883764',
                        'rdma_source_sha256': 'a'*64, 'rdma_archive_sha256': 'b'*64,
                        'client_archive_sha256': 'c'*64, 'snapshot': 'partial snapshot',
                        'clock': 'client monotonic', 'cq_available': False,
                        'cq_unavailable_reason': 'TCP has no RDMA CQ', 'poll_wait_scope': 'poll',
                        'wake_to_cq_scope': 'wakeup', 'transport_scope': 'parser and transport',
                        'records': [self.record, dict(self.record, owner=1)]}
        self.data = {'schema_version': 1, 'requested_limit': 2, 'measurement_attempted': True,
                     'measurement_drained': True, 'available': True, 'error': None,
                     'profile': self.profile}

    def tearDown(self):
        self.temp.cleanup()

    def admit(self, data=None, completed=True):
        (self.root / 'result/valkey-timings.json').write_text(json.dumps(data or self.data))
        return cloud.admit_valkey_diagnostics(self.root, self.job, self.config, completed=completed)

    def test_mget_owners_and_cancelled_worker_are_retained(self):
        admitted = self.admit()
        self.assertEqual(len(admitted['observed']['profile']['records']), 2)
        self.assertIsNone(admitted['observed']['profile']['records'][0]['blocking_return_to_wake_ns'])
        self.assertEqual(admitted['observed']['profile']['records'][0]['pipeline_outcome'], 'cancelled')
        self.assertTrue(admitted['coverage']['partial'])
        self.assertEqual(admitted['coverage']['cq_unavailable_reason'], 'TCP has no RDMA CQ')

    def test_cq_clock_failure_and_missing_worker_are_explicit_partial(self):
        self.job['backend'] = 'valkey_rdma'
        self.config['backend'] = {'kind': 'valkey_rdma', 'options': {}}
        self.profile['cq_available'] = True
        self.profile['cq_unavailable_reason'] = None
        self.record['cq'] = {'poll_wait_ns': 5, 'poll_calls': 1, 'cq_poll_ns': 2,
                             'cq_poll_calls': 1, 'wake_to_cq_ns': 0, 'wake_to_cq_samples': 0,
                             'completions': 1, 'clock_failures': 2}
        coverage = self.admit()['coverage']
        self.assertTrue(coverage['partial'])
        self.assertEqual(coverage['cq_clock_failures'], 2)
        self.assertEqual(coverage['cq_clock_failure_records'], 1)
        self.assertEqual(coverage['cq_missing_records'], 1)
        self.assertEqual(coverage['unfinished_workers'], 2)

    def test_foreign_identity_bad_provenance_and_population_rejected(self):
        for mutation in ('caller', 'source', 'cap', 'operation', 'cq'):
            data = copy.deepcopy(self.data)
            if mutation == 'caller': data['profile']['records'][0]['caller_sample_id'] = 18
            if mutation == 'source': data['profile']['source_commit'] = 'bad'
            if mutation == 'cap': data['profile']['records'].append(dict(self.record, owner=2))
            if mutation == 'operation': data['profile']['records'][0]['operation'] = 'put'
            if mutation == 'cq': data['profile']['records'][0]['cq'] = {}
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.admit(data)

    def test_failed_profile_explicit_and_configuration_bound(self):
        partial = dict(self.data, available=False, profile=None, error='not armed', measurement_drained=False)
        self.assertEqual(self.admit(partial, completed=False)['observed']['error'], 'not armed')
        with self.assertRaises(ValueError): self.admit(partial, completed=True)
        self.config['diagnostics']['valkey_profile_limit'] = 1
        with self.assertRaises(ValueError): self.admit()

    def test_read_trace_identity_caps_and_io_pair(self):
        event = {'name': 'flight', 'elapsed_ns': 7, 'io_queue_id': 3, 'io_token': 9}
        group = {'owner': 0, 'members': [0, 1], 'coalesced_members': [1], 'membership_classified': True,
                 'lane': 0, 'flight_slot': 0, 'events': [event], 'dropped_events': 0}
        call = {'caller_id': 17, 'members': 2, 'status': 'complete', 'events': [],
                'groups': [group], 'dropped_children': 0, 'dropped_events': 0}
        trace = {'limit': 2, 'calls': [call], 'dropped_calls': 0, 'contention_drops': 0, 'budget_drops': 0}
        correlation = {'available': True, 'trace': trace, 'clock_scope': 'client',
                       'granularity': 'API and owner', 'limitations': 'best effort'}
        cloud._validate_read_correlation(self.root, correlation, 2, True)
        for mutation in ('caller', 'members', 'io', 'cap'):
            bad = copy.deepcopy(correlation)
            c = bad['trace']['calls'][0]
            if mutation == 'caller': c['caller_id'] = 99
            if mutation == 'members': c['groups'][0]['members'] = [2]
            if mutation == 'io': c['groups'][0]['events'][0]['io_token'] = None
            if mutation == 'cap': c['groups'] = [group] * 257
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                cloud._validate_read_correlation(self.root, bad, 2, True)

    def test_matrix_appends_diagnostics_without_changing_existing_cases(self):
        spec = json.loads(Path(cloud.__file__).with_name('cloud-campaign.json').read_text())
        spec['backends'] = ['valkey_tcp', 'native']
        before = cloud.make_matrix(spec)
        spec['diagnostics']['valkey_profile_limit'] = 8
        after = cloud.make_matrix(spec)
        self.assertEqual(before, after[:len(before)])
        twins = after[len(before):]
        self.assertEqual(len(twins), 6)
        self.assertEqual({(j['value_size'], j['profile']) for j in twins},
                         {(size, profile) for size in (1024, 8192)
                          for profile in ('read-concurrent', 'write-concurrent', 'read-many32')})
        config = cloud.make_run_config(twins[0], '/tmp/diagnostic', {}, 'revision')
        self.assertEqual(config['backend']['options']['client'], 'libvalkey')
        self.assertEqual(config['diagnostics']['valkey_profile_limit'], 8)
        spec['diagnostics']['valkey_profile_limit'] = True
        with self.assertRaises(ValueError):
            cloud.make_matrix(spec)


if __name__ == '__main__':
    unittest.main()
