"""Offline per-owner counter availability and interval semantics."""
import unittest

import analyze


class ShardReadEvidence(unittest.TestCase):
    def metrics(self, available=1, offset=0):
        status = dict(shard_sample_available=available, resident_sample_available=available,
            shard_sample_generation=offset + 1, shard_sample_errors=0,
            shard_sample_age_ms=0, shard_sampler_stopped=0)
        text = ''.join(f'tidehunter_operational_read_io{{namespace="perpetual",stat="{key}"}} {value}\n'
                       for key, value in status.items())
        for owner in (0, 1):
            for stat, value in {'cache_hits': 7 + owner + offset,
                                'wal_reads': 3 + owner + offset,
                                'async_read_submissions': 3 + offset,
                                'resident_read_successes': owner + offset,
                                'cache_bytes': 1024 - offset,
                                'inflight_requests': 1 - offset}.items():
                text += f'tidehunter_shard_read{{namespace="perpetual",partition="{owner}",stat="{stat}"}} {value}\n'
        return analyze.parse_metrics(text)

    def test_partitions_are_not_collapsed_into_a_fleet_total(self):
        evidence = analyze.shard_read_stats(self.metrics())
        owners = evidence['by_namespace']['perpetual']
        self.assertEqual(set(owners), {'0', '1'})
        self.assertEqual(owners['0']['cache_hits'], 7)
        self.assertEqual(owners['1']['cache_hits'], 8)
        self.assertFalse(evidence['deltas_available'])

    def test_old_source_and_failed_sample_do_not_fabricate_zeroes(self):
        missing = analyze.shard_read_stats({})
        self.assertFalse(missing['available'])
        self.assertIn('snapshot', missing['delta_unavailable_reason'])
        failed = analyze.shard_read_stats(self.metrics(available=0))
        self.assertIsNone(failed['by_namespace']['perpetual']['0']['wal_reads'])
        omitted = analyze.shard_read_stats(analyze.parse_metrics(
            'tidehunter_adapter{namespace="perpetual",stat="shard_sample_available"} 0\n'))
        self.assertFalse(omitted['available'])
        self.assertEqual(omitted['by_namespace'], {})

    def test_deltas_preserve_gauges_and_unavailable_observations(self):
        delta = analyze.shard_read_deltas(self.metrics(), self.metrics(offset=1))
        self.assertTrue(delta['available'])
        owner = delta['by_namespace']['perpetual']['0']
        self.assertEqual(owner['wal_reads'], 1)
        self.assertEqual(owner['cache_bytes'], dict(before=1024, after=1023))
        self.assertEqual(owner['inflight_requests'], dict(before=1, after=0))
        missing = analyze.shard_read_deltas(self.metrics(), self.metrics(available=0, offset=1))
        self.assertIsNone(missing['by_namespace']['perpetual']['0']['wal_reads'])

    def test_population_mismatch_and_counter_reset_fail_closed(self):
        before, after = self.metrics(), self.metrics(offset=1)
        key = 'tidehunter_shard_read{namespace="perpetual",partition="1",stat="wal_reads"}'
        del after[key]
        self.assertFalse(analyze.shard_read_deltas(before, after)['available'])
        after = self.metrics(offset=1)
        after[key] = 0
        with self.assertRaisesRegex(ValueError, 'reset'):
            analyze.shard_read_deltas(before, after)

    def test_duplicate_owner_stats_and_negative_values_are_rejected(self):
        values = self.metrics()
        values['tidehunter_shard_read{stat="wal_reads",partition="0",namespace="perpetual"}'] = 1
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            analyze.shard_read_stats(values)
        values = self.metrics()
        key = 'tidehunter_shard_read{namespace="perpetual",partition="0",stat="wal_reads"}'
        values[key] = -1
        with self.assertRaisesRegex(ValueError, 'invalid'):
            analyze.shard_read_stats(values)

    def test_sui_database_label_preserves_owner_identity_and_rejects_collisions(self):
        values = {metric[:-1] + ',db="perpetual"}': value
                  for metric, value in self.metrics().items()}
        owners = analyze.shard_read_stats(values)['by_namespace']['perpetual']
        self.assertEqual(owners['0']['wal_reads'], 3)
        self.assertEqual(owners['1']['wal_reads'], 4)
        values['tidehunter_shard_read{namespace="perpetual",partition="0",stat="wal_reads",db="other"}'] = 5
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            analyze.shard_read_stats(values)

    def test_optional_authority_and_initiator_failures_mask_old_counters(self):
        for available in (0, 1):
            with self.subTest(available=available):
                values = self.metrics()
                for stat, value in {
                    'authority_available': available,
                    'authority_history_loads': 17,
                    'initiator_available': available,
                    'initiator_rma_posts': 23,
                }.items():
                    values[f'tidehunter_shard_read{{namespace="perpetual",partition="0",stat="{stat}"}}'] = value
                stats = analyze.shard_read_stats(values)['by_namespace']['perpetual']['0']
                self.assertEqual(stats['authority_history_loads'], 17 if available else None)
                self.assertEqual(stats['initiator_rma_posts'], 23 if available else None)
                self.assertEqual(stats['wal_reads'], 3)

    def test_reclaimed_destinations_and_unavailable_flags_are_gauges(self):
        before, after = self.metrics(), self.metrics(offset=1)
        for values, slots, available in ((before, 4, 1), (after, 1, 1)):
            for stat, value in {
                'initiator_available': available,
                'initiator_destination_slots_live': slots,
                'authority_available': available,
            }.items():
                values[f'tidehunter_shard_read{{namespace="perpetual",partition="0",stat="{stat}"}}'] = value
        owner = analyze.shard_read_deltas(before, after)['by_namespace']['perpetual']['0']
        self.assertEqual(owner['initiator_destination_slots_live'], dict(before=4, after=1))
        after['tidehunter_shard_read{namespace="perpetual",partition="0",stat="authority_available"}'] = 0
        owner = analyze.shard_read_deltas(before, after)['by_namespace']['perpetual']['0']
        self.assertEqual(owner['authority_available'], dict(before=1, after=0))

    def test_historical_adapter_status_normalizes_without_current_qualification(self):
        current = self.metrics()
        historical = {key: value for key, value in current.items()
                      if not key.startswith('tidehunter_operational_read_io{')}
        historical['tidehunter_adapter{namespace="perpetual",stat="shard_sample_available"}'] = 1
        self.assertEqual(analyze.shard_read_stats(current)['by_namespace'],
                         analyze.shard_read_stats(historical)['by_namespace'])
        self.assertEqual(analyze.adapter_stats(current)['perpetual']['shard_sample_available'],
                         analyze.adapter_stats(historical)['perpetual']['shard_sample_available'])
        with self.assertRaises(ValueError):
            analyze.current_metric_streams(historical, {'perpetual'}, remote=True)

    def test_new_owner_status_requires_resident_freshness_and_not_stopped(self):
        for key, value in (('resident_sample_available', 0), ('shard_sample_age_ms', 3001),
                           ('shard_sampler_stopped', 1), ('shard_sample_available', 0)):
            values = self.metrics()
            values[f'tidehunter_operational_read_io{{namespace="perpetual",stat="{key}"}}'] = value
            with self.subTest(key=key):
                evidence = analyze.shard_read_stats(values)
                self.assertFalse(evidence['sample_available_by_namespace']['perpetual'])
                self.assertIsNone(evidence['by_namespace']['perpetual']['0']['wal_reads'])


class ResidentReplayPopulation(unittest.TestCase):
    def owner(self, posts=2, wire=5390):
        return dict(initiator_available=1, initiator_rma_posts=posts,
            initiator_rma_completions=posts, initiator_rma_hits=posts,
            initiator_message_read_requests=wire, initiator_rma_errors=0,
            initiator_grant_acquisitions=2, initiator_fallback_missing=0,
            initiator_fallback_oversize=0, initiator_fallback_capacity=0,
            initiator_fallback_unsupported=0, initiator_fallback_stale=0)

    def test_natural_replay_keeps_mixed_rpc_and_actual_fi_read_distinct(self):
        result = analyze.resident_replay_population({'2': self.owner()}, configured=True)
        self.assertEqual(result['population'], 'mixed_RPC_and_registered_FI_READ_observed')
        self.assertEqual(result['totals']['initiator_rma_posts'], 2)
        self.assertEqual(result['totals']['initiator_message_read_requests'], 5390)
        self.assertFalse(result['pure_registered_hit_population'])
        self.assertFalse(result['per_lane_prepared_population_available'])
        self.assertIn('not_FI_READ', result['server_profile_scope'])

    def test_zero_rpc_snapshot_is_not_a_prepared_pure_hit_claim(self):
        result = analyze.resident_replay_population({'2': self.owner(wire=0)}, configured=True)
        self.assertEqual(result['population'], 'registered_FI_READ_observed_no_wire_Read_in_snapshot')
        self.assertFalse(result['pure_registered_hit_population'])
        missing = analyze.resident_replay_population({}, configured=True)
        self.assertIsNone(missing['totals'])
        native = analyze.resident_replay_population({}, configured=False, native=True)
        self.assertFalse(native['applicable'])

    def test_off_control_cannot_contain_actual_posts(self):
        with self.assertRaises(ValueError):
            analyze.resident_replay_population({'0': self.owner()}, configured=False)
        result = analyze.resident_replay_population({'0': self.owner(posts=0)}, configured=False)
        self.assertEqual(result['population'], 'RPC_only_observed')


if __name__ == '__main__':
    unittest.main()
