"""Pure controller and matrix boundary tests; no NVMe hardware is exercised."""
import copy
import hashlib
import json
import itertools
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cloud_campaign
import cloud_controller as controller


HERE = Path(__file__).resolve().parent


class CampaignModeTests(unittest.TestCase):
    @staticmethod
    def f6_evidence(**values):
        names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges', 'complete_exchanges',
                 'leased_frames', 'leased_skip_markers', 'fua_writes', 'grant_misses',
                 'controller_connect_ns', 'controller_connect_samples', 'leased_flushes',
                 'leased_flush_covered_frames', 'read_lane_pin_failures')
        start = dict.fromkeys(names, 7)
        counters = dict.fromkeys(names, 0)
        counters.update(values)
        end = {name: start[name] + value for name, value in counters.items()}
        return {'measurement_attempted': True, 'measurement_drained': True,
                'start': {'live_nvmeof': start, 'coordinator': {'read_profile_live_inline': 0}},
                'end': {'live_nvmeof': end, 'coordinator': {'read_profile_live_inline': 0}},
                'deltas': {'coordinator': {'available': True, 'counters': {'read_profile_live_inline': 0}}},
                'live_nvmeof': {'available': True, 'counters': counters}}

    @staticmethod
    def exported(job):
        """Simulate the effective configuration exported by the current binary."""
        return {'configuration': {
            'live_leased_sync': cloud_campaign.requested_leased_sync(job),
            'live_leased_flush_min_frame_bytes': cloud_campaign.requested_leased_flush_min_frame_bytes(job),
            'live_polled_writes': job.get('live_polled_writes', False)}}

    def test_executed_sync_provenance_not_job_defaults(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('nvmeof_f8')
                   if j['value_size'] == 65536 and j['concurrency'] == 16
                   and 'live_leased_sync' not in j)
        old = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                               leased_frames=4, leased_skip_markers=1, fua_writes=5)
        for record in (old['start']['live_nvmeof'], old['end']['live_nvmeof'],
                       old['live_nvmeof']['counters']):
            del record['leased_flushes']
            del record['leased_flush_covered_frames']
        archived = dict(job, selection='nvmeof_parallel_writes', group='G23W')
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(old, archived)['qualified'])
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(
            old, job, self.exported(job))['qualified'])
        new = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                               leased_frames=4, fua_writes=2,
                               leased_flushes=1, leased_flush_covered_frames=3)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(
            new, job, self.exported(job))['qualified'])
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(new, archived)['qualified'])
        with self.assertRaisesRegex(ValueError, 'live_leased_sync differs'):
            cloud_campaign.admit_live_write_contract(
                dict(job, live_leased_sync='per_frame_fua'), self.exported(job))

    def test_absent_key_polled_export_is_configured_group_flush_but_uses_fua(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('nvmeof_f8')
                   if 'live_leased_sync' not in j)
        job = dict(job, selection='nvmeof_contracts', group='G21W', live_polled_writes=True)
        backend = self.exported(job)
        self.assertEqual(backend['configuration']['live_leased_sync'], 'group_flush')
        cloud_campaign.admit_live_write_contract(
            {'live_polled_writes': True}, backend)
        evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                                    leased_frames=4, leased_skip_markers=1, fua_writes=5)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job, backend)['qualified'])
        for field, value in (('fua_writes', 4), ('leased_flushes', 1)):
            bad = copy.deepcopy(evidence)
            bad['end']['live_nvmeof'][field] = bad['start']['live_nvmeof'][field] + value
            bad['live_nvmeof']['counters'][field] = value
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(bad, job, backend)['qualified'])

    def test_f8_arms_require_the_reviewed_executed_configuration(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_f8')
        default = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_rdma'
                       and 'live_leased_sync' not in j)
        fua = next(j for j in jobs if j.get('live_leased_sync') == 'per_frame_fua')
        evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                                    leased_frames=4, fua_writes=4)
        good = self.exported(default)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, default, good)['qualified'])
        invalid = [None, {'configuration': {}},
                   {'configuration': {**good['configuration'], 'live_leased_sync': 'per_frame_fua'}},
                   {'configuration': {**good['configuration'], 'live_leased_flush_min_frame_bytes': 0}},
                   {'configuration': {**good['configuration'], 'live_polled_writes': True}}]
        for backend in invalid:
            with self.subTest(backend=backend):
                self.assertIn('F8 default arm did not run the engine default',
                              cloud_campaign.qualify_live_nvme_measurement(evidence, default, backend)['reasons'])
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(
            evidence, fua, self.exported(fua))['qualified'])
        for backend in (None, good):
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(evidence, fua, backend)['qualified'])

    def test_f6_sync_qualification_branches(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_f6')
        for group in ('G29W', 'G29X'):
            for concurrency in ((1, 16, 64) if group == 'G29W' else (16,)):
                for sync in ('per_frame_fua', 'group_flush'):
                    job = next(j for j in jobs if j['group'] == group and j['concurrency'] == concurrency
                               and j['live_leased_sync'] == sync and j.get('live_leased_write_window') != 1)
                    evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                        leased_frames=4, leased_skip_markers=1,
                        fua_writes=5 if sync == 'per_frame_fua' else 1,
                        leased_flushes=1, leased_flush_covered_frames=3)
                    with self.subTest(group=group, concurrency=concurrency, sync=sync):
                        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'])
                        changes = [('fua_writes', 4 if sync == 'per_frame_fua' else 6)]
                        if sync == 'group_flush':
                            for name in ('leased_flushes', 'leased_flush_covered_frames'):
                                bad = copy.deepcopy(evidence)
                                bad['live_nvmeof']['counters'][name] += 1
                                self.assertIn('invalid live NVMe counter ' + name,
                                              cloud_campaign.qualify_live_nvme_measurement(bad, job, self.exported(job))['reasons'])
                                del bad['live_nvmeof']['counters'][name]
                                self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(bad, job, self.exported(job))['qualified'])
                            if job['value_size'] == 65536 and concurrency in (16, 64):
                                changes += [('leased_flushes', 0), ('leased_flush_covered_frames', 1)]
                            else:
                                lone = self.f6_evidence(completed_write_ops=1, completed_write_bytes=1024,
                                                       leased_frames=1, fua_writes=1)
                                self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(lone, job, self.exported(job))['qualified'])
                                # A lone frame that crosses a fragment also writes a FUA skip marker.
                                crossing = self.f6_evidence(completed_write_ops=1, completed_write_bytes=1024,
                                                            leased_frames=1, leased_skip_markers=1, fua_writes=2)
                                self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(crossing, job, self.exported(job))['qualified'])
                        for name, value in changes:
                            bad = copy.deepcopy(evidence)
                            bad['end']['live_nvmeof'][name] = bad['start']['live_nvmeof'][name] + value
                            bad['live_nvmeof']['counters'][name] = value
                            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(bad, job, self.exported(job))['qualified'])

    def test_f6_group_flush_window_of_one_needs_no_shared_coverage(self):
        # F6 G29X-r0-c8b826d0881ac415257f counters: window 1 cannot share a flush.
        job = next(j for j in cloud_campaign.make_focused_matrix('nvmeof_f6')
                   if j['group'] == 'G29X' and j['live_leased_sync'] == 'group_flush'
                   and j.get('live_leased_write_window') == 1)
        evidence = self.f6_evidence(completed_write_ops=2070, completed_write_bytes=4096,
                                    leased_frames=2070, leased_skip_markers=8, fua_writes=2063,
                                    leased_flushes=15, leased_flush_covered_frames=15)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'])
        unbounded = dict(job, live_leased_write_window=None)
        self.assertIn('group flush did not cover shared frames',
                      cloud_campaign.qualify_live_nvme_measurement(evidence, unbounded, self.exported(unbounded))['reasons'])

    def test_f6_read_pinning_and_inline_qualification(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_f6')
        for group in ('G29R', 'G29G'):
            job = next(j for j in jobs if j['group'] == group and
                       (group == 'G29G' or j.get('live_pin_read_lanes')))
            job = dict(job, live_inline_reads=False)
            evidence = self.f6_evidence(completed_read_ops=4, completed_read_bytes=4096)
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
            bad = copy.deepcopy(evidence)
            bad['end']['coordinator']['read_profile_live_inline'] = 1
            bad['deltas']['coordinator']['counters']['read_profile_live_inline'] = 1
            self.assertIn('live inline admission evidence differs',
                          cloud_campaign.qualify_live_nvme_measurement(bad, job)['reasons'])
            if group == 'G29R':
                for value in (1, None):
                    bad = copy.deepcopy(evidence)
                    if value is None:
                        del bad['live_nvmeof']['counters']['read_lane_pin_failures']
                    else:
                        bad['end']['live_nvmeof']['read_lane_pin_failures'] += value
                        bad['live_nvmeof']['counters']['read_lane_pin_failures'] = value
                    self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(bad, job)['qualified'])
                bad = copy.deepcopy(evidence)
                bad['live_nvmeof']['counters']['read_lane_pin_failures'] = 0
                bad['end']['live_nvmeof']['read_lane_pin_failures'] += 1
                self.assertIn('invalid live NVMe counter read_lane_pin_failures',
                              cloud_campaign.qualify_live_nvme_measurement(bad, job)['reasons'])

    def test_engine_default_flush_qualification_and_admission(self):
        for job in cloud_campaign.make_focused_matrix('nvmeof_f8'):
            if job["backend"] == "native":
                continue
            evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                                        leased_frames=4, leased_skip_markers=1, fua_writes=5)
            default = 'live_leased_sync' not in job
            needs_shared = default and job['value_size'] == 65536 and job['concurrency'] in (16, 64)
            self.assertEqual(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'],
                             not needs_shared)
            evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                                        leased_frames=4, leased_skip_markers=1, fua_writes=2,
                                        leased_flushes=1, leased_flush_covered_frames=3)
            self.assertEqual(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'], default)
        for polled, mode in ((False, 'group_flush'), (True, 'group_flush')):
            job = {'live_polled_writes': polled}
            self.assertEqual(cloud_campaign.requested_leased_sync(job), mode)
            self.assertEqual(cloud_campaign.requested_leased_flush_min_frame_bytes(job), 65536)
            cloud_campaign.admit_live_write_contract(job, {'configuration': {
                'live_leased_sync': mode, 'live_leased_flush_min_frame_bytes': 65536}})
            cloud_campaign.admit_live_write_contract(job, {'configuration': {
                'live_leased_sync': mode, 'live_leased_flush_min_frame_bytes': 0}})

    def test_flush_minimum_validation_and_effective_binding(self):
        key = 'live_leased_flush_min_frame_bytes'
        for value in (-1, True, 1.5, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud_campaign.admit_live_write_contract(
                    {'live_leased_sync': 'group_flush', key: value}, {})
        with self.assertRaises(ValueError):
            cloud_campaign.admit_live_write_contract({key: 0}, {})
        for value in (0, 32768):
            job = {'live_leased_sync': 'group_flush', key: value}
            with self.assertRaises(ValueError):
                cloud_campaign.admit_live_write_contract(
                    job, {'configuration': {'live_leased_sync': 'group_flush'}})
            cloud_campaign.admit_live_write_contract(
                job, {'configuration': {'live_leased_sync': 'group_flush', key: value}})
        for job in cloud_campaign.make_focused_matrix('nvmeof_f6b'):
            if job['group'] not in ('G31W', 'G31H', 'G31S', 'G31X'):
                continue
            evidence = self.f6_evidence(completed_write_ops=4, completed_write_bytes=4096,
                                        leased_frames=4, fua_writes=4)
            requires_shared = (job['live_leased_sync'] == 'group_flush'
                               and job['value_size'] == 65536 and job['concurrency'] in (16, 64)
                               and job.get('live_leased_write_window') != 1)
            self.assertEqual(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'],
                             not requires_shared)
            if requires_shared:
                gated = dict(job, live_leased_flush_min_frame_bytes=
                             job['value_size'] * job['concurrency'] + 1)
                self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, gated, self.exported(gated))['qualified'])

    def test_optional_effective_configuration_admission(self):
        for key, configuration in (
                ('live_leased_sync', {'live_leased_sync': 'group_flush'}),
                ('live_pin_read_lanes', {'client': {'pin_read_lanes': True}})):
            value = 'group_flush' if key == 'live_leased_sync' else True
            with self.assertRaises(ValueError):
                cloud_campaign.admit_live_write_contract({key: value}, {'configuration': {}})
            cloud_campaign.admit_live_write_contract({key: value}, {'configuration': configuration})
        cloud_campaign.admit_live_write_contract({}, {'configuration': {}})

    def test_owner_history_receipt_defaults_and_freshness(self):
        launch = {'ready_file': '/tmp/ready', 'launch_nonce': 'nonce', 'listen': '127.0.0.1:1',
                  'node': {'partition': {'partition': 0, 'inventory': {'database': [1] * 16}}}}
        receipt = {'partition': 0, 'database': [1] * 16, 'address': '127.0.0.1:1',
                   'launch_nonce': 'nonce', 'pid': 1, 'boot_id': 'boot',
                   'started_unix_ns': 1, 'durability': {'tier': 'native'}}
        cloud_campaign.validate_owner_receipt(receipt, launch)
        explicit = dict(receipt, history_persistence='on_disk')
        cloud_campaign.validate_owner_receipt(explicit, launch)
        cloud_campaign.validate_owner_receipt(receipt, dict(launch, history_persistence='on_disk'))
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(
            receipt, dict(explicit, pid=2, started_unix_ns=2)))
        replay = dict(receipt, history_persistence='wal_replay')
        cloud_campaign.validate_owner_receipt(replay, dict(launch, history_persistence='wal_replay'))
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(
            replay, dict(replay, pid=2, started_unix_ns=2)))
        with self.assertRaises(ValueError):
            cloud_campaign.validate_owner_receipt(receipt, dict(launch, history_persistence='wal_replay'))
        memory_launch = dict(launch, history_persistence='memory_only')
        memory_receipt = dict(receipt, history_persistence='memory_only')
        cloud_campaign.validate_owner_receipt(memory_receipt, memory_launch)
        for actual, planned in ((receipt, memory_launch), (memory_receipt, launch)):
            with self.assertRaises(ValueError):
                cloud_campaign.validate_owner_receipt(actual, planned)
        newer = dict(memory_receipt, pid=2, started_unix_ns=2)
        self.assertTrue(cloud_campaign.owner_receipt_is_fresh(memory_receipt, newer))
        self.assertFalse(cloud_campaign.owner_receipt_is_fresh(receipt, newer))

    def test_permanent_export_measurement_and_configuration(self):
        for selection in cloud_campaign.NVMEOF_PATH_SELECTIONS:
            with self.subTest(selection=selection):
                self._permanent_export_measurement_and_configuration(selection)

    def _permanent_export_measurement_and_configuration(self, selection):
        job = {'selection': selection, 'group': 'G18R', 'backend': 'tidehunter_nvme_rdma',
               'nvmeof_route': 'live', 'mix': {'read': 100}, 'concurrency': 1,
               'read_lanes': 1, 'live_inline_reads': False, 'live_write_mode': 'leased',
               'durability_tier': 'native', 'live_export_mode': 'per_owner',
               'live_io_queues': 1, 'live_io_queue_size': 32, 'live_poll_queues': 0}
        if selection in cloud_campaign.WRITE_ENGINE_SELECTIONS:
            job = next(j for j in cloud_campaign.make_focused_matrix(selection)
                       if j['case_kind'] == 'live_read' and j['arm'] == 'file')
        names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                 'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes',
                 'grant_misses', 'controller_connect_ns', 'controller_connect_samples')
        start = dict.fromkeys(names, 0)
        end = dict(start, completed_read_ops=1, completed_read_bytes=1024,
                   namespace_connects=1, metadata_requests=2)
        evidence = {'measurement_attempted': True, 'measurement_drained': True,
                    'start': {'live_nvmeof': start, 'coordinator': {'read_profile_live_inline': 0}},
                    'end': {'live_nvmeof': end, 'coordinator': {'read_profile_live_inline': 0}},
                    'deltas': {'coordinator': {'available': True,
                               'counters': {'read_profile_live_inline': 0}}},
                    'live_nvmeof': {'available': True, 'counters': dict(end)}}
        if selection in cloud_campaign.M3_SELECTIONS:
            owner_count = cloud_campaign.active_owner_count(job)
            for boundary in ('start', 'end', 'deltas'):
                evidence[boundary]['owner_executor'] = {'available': True, 'partitions': {
                    str(i): {'available': True, 'counters': {}} for i in range(owner_count)}}
            incomplete = copy.deepcopy(evidence)
            incomplete['deltas']['owner_executor']['partitions'].pop(str(owner_count - 1))
            self.assertIn('per-owner measurement counter population unavailable',
                cloud_campaign.qualify_live_nvme_measurement(incomplete, job, self.exported(job))['reasons'])
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['qualified'])
        mixed = dict(job, mix={'read': 50, 'put': 50})
        if selection in cloud_campaign.WRITE_ENGINE_SELECTIONS:
            mixed = next(j for j in cloud_campaign.make_focused_matrix(selection)
                         if j['case_kind'] == 'mainnet_replay' and j['arm'] == job['arm']
                         and j['durability_tier'] == 'native')
        current = copy.deepcopy(evidence)
        current['end']['live_nvmeof'].update(completed_write_ops=1, completed_write_bytes=1024)
        current['live_nvmeof']['counters'].update(completed_write_ops=1, completed_write_bytes=1024)
        # q3 rest attempt-1 G24M: metadata 15038 -> 15592, extents 2 -> 2.
        current['start']['live_nvmeof'].update(metadata_requests=15038, extents_leased=2)
        current['end']['live_nvmeof'].update(metadata_requests=15592, extents_leased=2)
        current['live_nvmeof']['counters'].update(metadata_requests=554, extents_leased=0)
        for name in cloud_campaign.WRITE_METADATA_EXCHANGES:
            current['start']['live_nvmeof'][name] = 0
            value = 554 if name == 'adoption_async_exchanges' else 0
            current['end']['live_nvmeof'][name] = value
            current['live_nvmeof']['counters'][name] = value
        current['start']['coordinator']['read_rpc_count'] = 0
        current['end']['coordinator']['read_rpc_count'] = 0
        current['deltas']['coordinator']['counters']['read_rpc_count'] = 0
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(current, mixed, self.exported(mixed))['qualified'])
        for name, value, reason in (
                ('metadata_requests', 554 + mixed['read_lanes'] + 2, 'permanent export sent owner requests for reads'),
                ('payload_rpc_requests', 1, 'payload RPC observed on live NVMe route')):
            bad = copy.deepcopy(current)
            bad['end']['live_nvmeof'][name] = bad['start']['live_nvmeof'][name] + value
            bad['live_nvmeof']['counters'][name] = value
            self.assertIn(reason, cloud_campaign.qualify_live_nvme_measurement(bad, mixed, self.exported(mixed))['reasons'])
        bad = copy.deepcopy(current)
        del bad['live_nvmeof']['counters']['adoption_async_exchanges']
        self.assertIn('invalid live NVMe counter adoption_async_exchanges',
                      cloud_campaign.qualify_live_nvme_measurement(bad, mixed, self.exported(mixed))['reasons'])
        for name in ('grant_misses', 'controller_connect_ns', 'controller_connect_samples'):
            bad = copy.deepcopy(evidence)
            del bad['live_nvmeof']['counters'][name]
            self.assertIn(f'invalid live NVMe counter {name}',
                          cloud_campaign.qualify_live_nvme_measurement(bad, job, self.exported(job))['reasons'])
        for name in ('lease_reserve_ns', 'lease_namespace_sync_ns', 'lease_publish_ns',
                     'lease_profile_samples', 'adopt_owner_ns', 'adopt_close_scan_ns',
                     'adopt_close_scan_bytes', 'adopt_profile_samples',
                     'extent_preleases', 'extent_switch_waits'):
            current = copy.deepcopy(evidence)
            current['start']['live_nvmeof'][name] = 0
            current['end']['live_nvmeof'][name] = 2
            current['live_nvmeof']['counters'][name] = 2
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(current, job, self.exported(job))['qualified'])
            current['live_nvmeof']['counters'][name] = 1
            self.assertIn(f'invalid live NVMe counter {name}',
                          cloud_campaign.qualify_live_nvme_measurement(current, job, self.exported(job))['reasons'])
        for field, value, reason in (
                ('namespace_connects', job.get('owner_count', 1) + 1, 'permanent export made more than one controller connect'),
                ('metadata_requests', job['read_lanes'] + 2, 'permanent export sent owner requests for reads')):
            bad = copy.deepcopy(evidence)
            bad['end']['live_nvmeof'][field] = value
            bad['live_nvmeof']['counters'][field] = value
            self.assertIn(reason, cloud_campaign.qualify_live_nvme_measurement(bad, job, self.exported(job))['reasons'])
        info = {'configuration': {'live_export_mode': 'per_owner',
                'live_queue_geometry': {'io_queues': 1, 'queue_size': 32, 'poll_queues': 0},
                'live_write': {'write_mode': 'leased',
                               'acknowledgement_contract': (
                                   'leased extent: page-cache write' if job['durability_tier'] == 'native'
                                   else 'synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous')}}}
        info['configuration'].update(
            live_leased_sync=cloud_campaign.requested_leased_sync(job),
            live_leased_flush_min_frame_bytes=cloud_campaign.requested_leased_flush_min_frame_bytes(job))
        info['configuration'].update({key: job[key] for key in ('live_lease_ahead', 'live_polled_writes')
                                      if key in job})
        if job['selection'] in ('nvmeof_parallel_writes', 'nvmeof_single_caller', 'nvmeof_f6', 'nvmeof_f6b', 'nvmeof_f8', 'nvmeof_f7_highmem32', 'nvmeof_reads_geometry', 'nvmeof_reads_geometry_rest', 'nvmeof_reads_geometry_rest2'):
            window = job.get('live_leased_write_window')
            info['configuration'].update(live_leased_write_window=window,
                live_leased_write_window_bound=31 if window is None else window)
        cloud_campaign.admit_live_write_contract(job, info)
        for field in ('live_export_mode', 'live_queue_geometry'):
            bad = copy.deepcopy(info)
            bad['configuration'][field] = None
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(job, bad)
        for field, value in (('live_lease_ahead', False), ('live_polled_writes', True)):
            bad = copy.deepcopy(info)
            bad['configuration'][field] = value
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(job, bad)
        for field, default in (('live_lease_ahead', True), ('live_polled_writes', False)):
            explicit_job = dict(job, **{field: default})
            missing_info = copy.deepcopy(info)
            missing_info['configuration'].pop(field, None)
            with self.assertRaisesRegex(ValueError, 'live export mode or queue geometry differs'):
                cloud_campaign.admit_live_write_contract(explicit_job, missing_info)
            explicit_info = copy.deepcopy(info)
            explicit_info['configuration'][field] = default
            cloud_campaign.admit_live_write_contract(explicit_job, explicit_info)

    def test_f7_sized_windows_admit_limits_and_keep_source_qualification(self):
        for job in cloud_campaign.make_focused_matrix('nvmeof_f7_highmem32'):
            config = cloud_campaign.make_run_config(job, '/tmp/f7-sized', {}, 'revision', 600)
            self.assertEqual(config['phases']['warmup_operations'], job['warmup_operations'])
            self.assertEqual(config['phases']['measurement_operations'], job['measurement_operations'])
            self.assertEqual(config['limits']['max_operations'],
                             job['keys'] + job['warmup_operations'] + job['measurement_operations'] + 4096)
            self.assertLessEqual(config['dataset']['keys'], config['limits']['max_keys'])
            self.assertLessEqual(job['keys'] * job['value_size'], config['limits']['max_bytes'])
            capacity = controller.planned_write_batches_per_partition(job)
            self.assertLessEqual(capacity, 1000000)
            if job['mix']['read'] == 100:
                self.assertEqual(capacity, 65536 + 4096)
                self.assertTrue(config['phases']['warmup_key_sweep'])
                self.assertGreaterEqual(config['phases']['warmup_operations'], config['dataset']['keys'])
                evidence = self.f6_evidence(completed_read_ops=job['measurement_operations'],
                    completed_read_bytes=job['measurement_operations'] * job['value_size'])
            else:
                self.assertEqual(capacity, 4096 + 70000 + 350000 + 4096)
                evidence = self.f6_evidence(completed_write_ops=job['measurement_operations'],
                    completed_write_bytes=job['measurement_operations'] * job['value_size'],
                    leased_frames=job['measurement_operations'])
            result = cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))
            self.assertTrue(result['qualified'], (job['case_id'], result))
            if job['mix']['read'] == 100:
                del evidence['deltas']['coordinator']['counters']['read_profile_live_inline']
                self.assertIn('live inline admission evidence differs',
                    cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['reasons'])
            else:
                evidence['end']['live_nvmeof']['fua_writes'] += 1
                evidence['live_nvmeof']['counters']['fua_writes'] += 1
                self.assertIn('leased FUA count differs from frames and skip markers',
                    cloud_campaign.qualify_live_nvme_measurement(evidence, job, self.exported(job))['reasons'])

    def test_planned_write_capacity_counts_writes_and_rounds_up_per_owner(self):
        job = dict(owner_count=5, keys=11, warmup_operations=2, measurement_operations=8,
                   mix=cloud_campaign.mix(read=100))
        self.assertEqual(controller.planned_write_batches_per_partition(job), 4099)
        for kind in ('put', 'delete', 'put_batch'):
            changed = dict(job, mix=cloud_campaign.mix(**{kind: 100}))
            self.assertEqual(controller.planned_write_batches_per_partition(changed), 4101)
        mixed = dict(job, mix=cloud_campaign.mix(read=99, put=1))
        self.assertEqual(controller.planned_write_batches_per_partition(mixed), 4101)
        reads = dict(job, warmup_operations=300000, measurement_operations=1500000)
        self.assertEqual(controller.planned_write_batches_per_partition(reads), 4099)

    def test_parallel_c64_run_configuration_admits_all_callers(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_parallel_writes')
        cases = [j for j in jobs if j['concurrency'] == 64]
        self.assertEqual(len(cases), 6)
        for job in cases:
            config = cloud_campaign.make_run_config(job, '/tmp/parallel-c64', {}, 'revision', 600)
            self.assertEqual(config['execution']['concurrency'], 64)
            self.assertEqual(config['execution']['mode'], 'closed_loop')
            self.assertGreaterEqual(config['execution']['queue_capacity'], 64)
            self.assertEqual(config['execution']['request_timeout_ms'], 10000)
            self.assertEqual(config['limits']['deadline_seconds'], 600)
            self.assertEqual(config['measurement_gate']['timeout_seconds'], 240)

    def test_parallel_write_window_and_resource_bound_admission(self):
        jobs = cloud_campaign.make_focused_matrix('nvmeof_parallel_writes')
        for window in (None, 1, 4):
            job = next(j for j in jobs if j.get('live_leased_write_window') == window
                       and j.get('live_write_mode') == 'leased' and j['durability_tier'] == 'native')
            configuration = dict(live_export_mode=job['live_export_mode'],
                live_queue_geometry=dict(io_queues=1, queue_size=32, poll_queues=0),
                live_lease_ahead=True, live_polled_writes=False,
                live_leased_write_window=window, live_leased_write_window_bound=31 if window is None else window,
                live_write=dict(write_mode='leased', acknowledgement_contract='leased extent: page-cache write'))
            cloud_campaign.admit_live_write_contract(job, dict(configuration=configuration))
            for field, value in (('live_leased_write_window', 2), ('live_leased_write_window', False),
                                 ('live_leased_write_window_bound', None), ('live_leased_write_window_bound', True),
                                 ('live_leased_write_window_bound', 0)):
                bad = dict(configuration, **{field: value})
                with self.subTest(window=window, field=field, value=value), self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, dict(configuration=bad))
            for field in ('live_leased_write_window', 'live_leased_write_window_bound'):
                bad = dict(configuration)
                del bad[field]
                with self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, dict(configuration=bad))

    def test_prepared_live_client_exact_contract(self):
        host_nqn = 'nqn.2014-08.org.nvmexpress:uuid:01234567-89ab-4def-8123-456789abcdef'
        for mode in ('reserved', 'leased'):
            for tier in ('native', 'synchronous'):
                for inline in (False, True):
                    prep = {'max_inflight_requests': 32, 'max_inflight_bytes': 16777216,
                            'connect_timeout_ms': 30000, 'io_timeout_ms': 30000,
                            'write_mode': mode, 'inline_reads': inline, 'export_mode': 'on_demand',
                            'lease_ahead': True, 'polled_writes': False,
                            'queue_geometry': {'io_queues': 1, 'queue_size': 32, 'poll_queues': 0}}
                    client = {**prep, 'host_nqn': host_nqn, 'host_id': controller.live_host_identity(host_nqn),
                              'leased_durability': tier if mode == 'leased' else None}
                    exported = dict(client, leased_sync='group_flush', leased_flush_min_frame_bytes=65536)
                    self.assertEqual(controller.admit_prepared_live_client(exported, prep, tier), host_nqn)
                    polled_prep = dict(prep, polled_writes=True)
                    polled_client = dict(exported, polled_writes=True)
                    self.assertEqual(controller.admit_prepared_live_client(polled_client, polled_prep, tier), host_nqn)
                    with self.assertRaises(controller.SafetyError):
                        controller.admit_prepared_live_client(
                            dict(exported, leased_flush_min_frame_bytes=0), prep, tier)
                    self.assertEqual(controller.admit_prepared_live_client(client, prep, tier), host_nqn)
                    for key, value in (('leased_sync', 'group_flush'),):
                        requested = dict(prep, **{key: value})
                        with self.assertRaises(controller.SafetyError):
                            controller.admit_prepared_live_client(client, requested, tier)
                        self.assertEqual(controller.admit_prepared_live_client(
                            dict(client, **{key: value}), requested, tier), host_nqn)
                    for field in client:
                        changed = dict(client)
                        changed[field] = None
                        if changed[field] == client[field]:
                            changed[field] = 'changed'
                        with self.assertRaises(controller.SafetyError):
                            controller.admit_prepared_live_client(changed, prep, tier)

    def test_live_compact_admission_is_exact(self):
        jobs = cloud_campaign.make_focused_matrix('leased_ab_compact')
        live = next(j for j in jobs if j.get('nvmeof_route') == 'live')
        owner = next(j for j in jobs if j['backend'] == 'tidehunter_verbs_native')
        config = {'spec': {'mode': 'focused', 'selection': 'leased_ab_compact',
                           'backends': ['tidehunter_nvme_rdma']},
                  'fleet': {'nodes': [{'address': '10.0.0.2'}]}}
        with patch.object(controller, 'platform_module', return_value=None):
            prep = controller.live_nvme_preparation(config, live['case_id'], 'rdma')
            self.assertEqual(prep['addresses'], ['10.0.0.2'])
            with self.assertRaises(controller.SafetyError):
                controller.live_nvme_preparation(config, owner['case_id'], 'rdma')
            config['spec']['selection'] = 'fabric_ab_compact'
            with self.assertRaises(controller.SafetyError):
                controller.live_nvme_preparation(config, live['case_id'], 'rdma')

    def test_leased_measurement_evidence_rejects_counter_changes(self):
        job = next(j for j in cloud_campaign.make_focused_matrix('leased_ab_compact')
                   if j.get('live_write_mode') == 'leased' and j['group'] == 'G17W'
                   and j['durability_tier'] == 'synchronous')
        job = dict(job, live_leased_sync='per_frame_fua')
        names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                 'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes')
        start = {name: 0 for name in names}
        end = dict(start, completed_write_ops=2, completed_write_bytes=2048, leased_frames=2, fua_writes=2)
        evidence = {'measurement_attempted': True, 'measurement_drained': True,
                    'start': {'live_nvmeof': start}, 'end': {'live_nvmeof': end},
                    'live_nvmeof': {'available': True, 'counters': dict(end)}}
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
        for group in ('G23W', 'G23X', 'G28W', 'G28C'):
            current = dict(job, group=group)
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, current)['qualified'])
            bad = copy.deepcopy(evidence)
            bad['end']['live_nvmeof']['fua_writes'] = 1
            bad['live_nvmeof']['counters']['fua_writes'] = 1
            self.assertIn('leased FUA count differs from frames and skip markers',
                          cloud_campaign.qualify_live_nvme_measurement(bad, current)['reasons'])
        skipped = copy.deepcopy(evidence)
        skipped['end']['live_nvmeof'].update(leased_skip_markers=1, fua_writes=3)
        skipped['live_nvmeof']['counters'].update(leased_skip_markers=1, fua_writes=3)
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(skipped, job)['qualified'])
        skipped['end']['live_nvmeof']['fua_writes'] = 2
        skipped['live_nvmeof']['counters']['fua_writes'] = 2
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(skipped, job)['qualified'])
        for name, value in [('reserve_exchanges', 1), ('complete_exchanges', 1),
                            ('leased_frames', 0), ('fua_writes', 1)]:
            changed = copy.deepcopy(evidence)
            changed['end']['live_nvmeof'][name] = value
            changed['live_nvmeof']['counters'][name] = value
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(changed, job)['qualified'], name)
        missing = copy.deepcopy(evidence)
        del missing['live_nvmeof']['counters']['leased_skip_markers']
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(missing, job)['qualified'])
        native = dict(job, durability_tier='native')
        native_evidence = copy.deepcopy(evidence)
        native_evidence['end']['live_nvmeof']['fua_writes'] = 0
        native_evidence['live_nvmeof']['counters']['fua_writes'] = 0
        self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(native_evidence, native)['qualified'])
        native_evidence['end']['live_nvmeof']['fua_writes'] = 1
        native_evidence['live_nvmeof']['counters']['fua_writes'] = 1
        self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(native_evidence, native)['qualified'])

    def test_live_contract_and_inline_admission(self):
        jobs = cloud_campaign.make_focused_matrix('leased_ab_compact')
        for inline in (False, True):
            job = next(j for j in jobs if j['group'] == 'G17R'
                       and j.get('live_inline_reads') is inline and j['concurrency'] == 1
                       and j['mix']['read'] == 100)
            info = {'configuration': {'live_write': {'write_mode': 'leased',
                'acknowledgement_contract': 'synchronous leased: written and covered by a FUA write or a later namespace flush before acknowledgement; owner frontier root asynchronous'}}}
            cloud_campaign.admit_live_write_contract(job, info)
            for field in ('write_mode', 'acknowledgement_contract'):
                bad = copy.deepcopy(info)
                bad['configuration']['live_write'][field] = 'wrong'
                with self.assertRaises(ValueError):
                    cloud_campaign.admit_live_write_contract(job, bad)
            names = (*cloud_campaign.LIVE_NVME_COUNTERS, 'reserve_exchanges',
                     'complete_exchanges', 'leased_frames', 'leased_skip_markers', 'fua_writes')
            start = {name: 0 for name in names}
            end = dict(start, completed_read_ops=1, completed_read_bytes=1024)
            n = int(inline)
            evidence = {'measurement_attempted': True, 'measurement_drained': True,
                        'start': {'live_nvmeof': start, 'coordinator': {'read_profile_live_inline': 0}},
                        'end': {'live_nvmeof': end, 'coordinator': {'read_profile_live_inline': n}},
                        'deltas': {'coordinator': {'available': True, 'counters': {'read_profile_live_inline': n}}},
                        'live_nvmeof': {'available': True, 'counters': dict(end)}}
            self.assertTrue(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])
            evidence['end']['coordinator']['read_profile_live_inline'] = 1 - n
            evidence['deltas']['coordinator']['counters']['read_profile_live_inline'] = 1 - n
            self.assertFalse(cloud_campaign.qualify_live_nvme_measurement(evidence, job)['qualified'])

    def test_focused_matrix_matches_read_sources_and_excludes_nvme_writes(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        jobs = cloud_campaign.make_matrix(spec)
        self.assertEqual(len(jobs), 125)
        reads = [job for job in jobs if job['cohort'] == 'sealed_read']
        self.assertTrue(reads)
        for size in (64, 256, 1024, 8192, 65536):
            rows = [job for job in reads if job['value_size'] == size]
            self.assertEqual(len({job['sealed_dataset']['data_id'] for job in rows}), 1)
            self.assertEqual({job['backend'] for job in rows}, set(spec['backends']))
            source = {job['backend']: job['sealed_dataset']['source_id'] for job in rows}
            self.assertNotEqual(source['native'], source['tidehunter_tcp'])
            for backend in cloud_campaign.NVME_BACKENDS & source.keys():
                self.assertEqual(source[backend], source['tidehunter_tcp'])
            for backend in source.keys() - {'native', 'tidehunter_tcp'} - cloud_campaign.NVME_BACKENDS:
                self.assertNotEqual(source[backend], source['tidehunter_tcp'])
                self.assertNotEqual(source[backend], source['native'])
        self.assertFalse(any(job['backend'] in cloud_campaign.NVME_BACKENDS and
                             job['cohort'] == 'native_write_regression' for job in jobs))

    def test_selected_backend_order_cannot_reenter_physical_source(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        # Exercise the portable matrix, including both optional NVMe transports.
        backends = ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                    'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma']
        for order in itertools.permutations(backends):
            with self.subTest(order=order):
                jobs = cloud_campaign.make_matrix(dict(spec, backends=list(order), max_cases=2000))
                reads = [job for job in jobs if job['cohort'] == 'sealed_read']
                completed = set()
                previous = None
                for job in reads:
                    source = job['sealed_dataset']['source_id']
                    if source != previous:
                        self.assertNotIn(source, completed)
                        if previous is not None:
                            completed.add(previous)
                        previous = source

    def test_nvme_backend_is_rejected_outside_focused_mode(self):
        spec = json.loads((HERE / 'cloud-nvmeof-campaign.json').read_text())
        spec['mode'] = 'standard'
        with self.assertRaises(ValueError):
            cloud_campaign.make_matrix(spec)


class HostCpuIntervalTests(unittest.TestCase):
    def test_kernel_work_is_host_scoped_and_reboots_are_unavailable(self):
        def snapshot(boot, counters):
            return {'nvme': {'host_identity': {'boot_id': boot}}, 'host_cpu_hz': 100,
                    'host_cpu_stat': 'cpu ' + ' '.join(map(str, counters))}
        start = [snapshot(str(i), [0] * 8) for i in range(6)]
        end = [snapshot(str(i), [100, 0, 200, 300, 0, 50, 25, 0]) for i in range(6)]
        end[3]['nvme']['host_identity']['boot_id'] = 'rebooted'
        result = controller.host_cpu_interval(start, end)
        self.assertEqual(result['hosts'][0]['system_seconds'], 2.75)
        self.assertFalse(result['hosts'][3]['available'])
        self.assertIn('rebooted', result['hosts'][3]['reason'])
        self.assertIn('not attribution', result['scope'])


class ControllerBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.c = controller.Controller.__new__(controller.Controller)
        self.c.local_mode = None
        self.c.config = {'prefix': 'run', 'repository_root': str(self.root),
                         'spec': {'case_deadline_seconds': 600}, 'deadline_utc': '2099-01-01T00:00:00Z',
                         'fleet': {'nodes': [{'address': f'10.0.0.{i + 1}'} for i in range(5)]}}
        self.c.root = self.root
        self.c.clients = {'s3': Mock()}
        self.c.active_nvme = None
        self.c.active_tide = None
        self.c.tide_cleanup_forbidden = False
        self.c.shared_tide = None
        self.c.remaining = Mock(return_value=300)
        self.c.cleanup_deadline = Mock(return_value=100)
        self.c.cleanup_timeout = Mock(return_value=60)
        self.c.request = Mock(return_value={'complete': True})

    def mirror_fixtures(self, base, uuid, manifest):
        self.c.config['fleet']['data_root'] = str(base / 'fleet')
        roots, targets = [], []
        for index in range(5):
            root = base / f'mirror-{index}'
            root.mkdir()
            path = root / 'manifest.json'
            path.write_text(json.dumps(manifest))
            remote = base / 'fleet' / 'cells' / 'source-case' / f'node-{index}' / 'nvmeof' / 'export'
            roots.append(str(root))
            targets.append({'target': {'transport': 'tcp', 'export_root': str(remote), 'exports': [{
                'export_uuid': uuid, 'manifest': str(remote / 'manifest.json'),
                'manifest_sha256': controller.digest(path),
                'target_receipt': {'selected_transport': 'tcp', 'target_cache': 'buffered_file_page_cache',
                                   'target_cache_verified': True, 'buffered_io': 1}}]}})
        return roots, targets

    def test_staging_requires_exact_object_and_preserves_verified_bytes(self):
        raw = b'{\"catalog\":[{}],\"nqn\":\"test\"}'
        checksum = hashlib.sha256(raw).hexdigest()
        self.c.config.update(bucket='bucket', account='account')
        self.c.config['fleet']['data_root'] = str(self.root / 'fleet')
        uuid = [1] * 16
        work = self.root / 'fleet/cells/case/node-0/nvmeof'
        key = f'run/results/cells/case/storage/0/nvme-manifests/{bytes(uuid).hex()}.json'
        entry = {'manifest_path': str(work / 'export/partition/manifest.json'),
                 'export_uuid': uuid, 'manifest_key': key, 'manifest_sha256': checksum,
                 'manifest_bytes': len(raw)}
        exports = [{'work_dir': str(work), 'ready': {'exports': [entry]}}]
        s3 = self.c.clients['s3']
        s3.head_object.return_value = {'ContentLength': len(raw), 'Metadata': {'sha256': checksum}}
        s3.download_file.side_effect = lambda bucket, obj, dest, **kw: Path(dest).write_bytes(raw)
        real_path = Path
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        with patch.object(controller, 'Path', side_effect=mapped_path):
            mirrors = self.c.stage_nvme_manifests('case', exports)
        self.assertEqual((real_path(mirrors[0]) / 'partition/manifest.json').read_bytes(), raw)
        s3.head_object.assert_called_once_with(Bucket='bucket', Key=key, ExpectedBucketOwner='account')
        self.assertEqual(s3.download_file.call_args.args[:2], ('bucket', key))
        self.assertEqual(s3.download_file.call_args.kwargs['ExtraArgs'], {'ExpectedBucketOwner': 'account'})

    def test_owner_batch_requires_every_exact_owner_to_confirm(self):
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 3}
        with self.assertRaises(controller.InfrastructureError):
            self.c.owner_batch('nvme_owner_stop', 'case')
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))

    def test_guest_owner_cleanup_accepts_only_exact_s3_proof_location(self):
        import nvme_campaign_owner
        config = {'prefix': 'run', 'spec': {'mode': 'nvmeof-focused'},
                  'fleet': {'data_root': '/unused', 'nodes': [
                      {'index': i, 'instance_id': f'i-owner-{i}', 'address': f'10.0.0.{i + 2}'}
                      for i in range(5)]}}
        request = {'index': 2, 'action': 'nvme_owner_target_cleanup', 'case_id': 'case',
                   'generation': 'gen', 'host_disconnected_key': 'run/results/cells/case/storage/2/nvme-disconnect/gen.json'}
        clients = {'s3': Mock()}
        proof = {'phase': 'disconnected'}
        with patch.object(controller, 'verify_local_identity'), \
             patch.object(controller, 'guarded', return_value=self.root), \
             patch.object(controller, 'read_s3_json', return_value=proof) as read_proof, \
             patch.object(nvme_campaign_owner, 'execute', return_value={'complete': True}) as execute:
            self.assertEqual(controller.guest_operation(config, request, clients), {'complete': True})
            self.assertIs(execute.call_args.args[1]['host_disconnected'], proof)
            execute.reset_mock()
            read_proof.reset_mock()
            forged = dict(request, host_disconnected_key='run/results/cells/case/storage/1/nvme-disconnect/gen.json',
                          host_disconnected=proof)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, forged, clients)
            read_proof.assert_not_called()
            execute.assert_not_called()
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(dict(config, spec={'mode': 'standard'}), request, clients)
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, dict(request, index=-1), clients)
            execute.assert_not_called()

    def test_setup_addresses_are_taken_from_owned_fleet(self):
        self.c.owner_batch('nvme_owner_target_setup', 'case', generation='generation')
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['address'], self.c.config['fleet']['nodes'][index]['address'])
            self.assertEqual(request['generation'], 'generation')

    def test_rdma_target_uses_owned_gcp_rdma_interface(self):
        for i, node in enumerate(self.c.config['fleet']['nodes']):
            node['rdma_address'] = f'10.1.0.{i + 1}'
        with patch.object(controller, 'platform_module', return_value=object()):
            self.c.owner_batch('nvme_owner_target_setup', 'case', generation='g', transport='rdma')
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['address'], self.c.config['fleet']['nodes'][index]['rdma_address'])

    def test_shared_tcp_source_reuses_prepared_owner_dataset(self):
        options = {'external_remote_config': {'read_lanes': 1}, 'evidence': 'old'}
        self.c.shared_tide = {'identity': ('source', 'tcp', 'synchronous', 'adaptive'), 'options': copy.deepcopy(options)}
        job = {'backend': 'tidehunter_nvme_tcp', 'sealed_dataset': {'source_id': 'source'}, 'read_lanes': 4,
               'durability_tier': 'synchronous'}
        self.c.prepare_tide = Mock(side_effect=AssertionError('must reuse prepared dataset'))
        actual = self.c.prepared_tide_options(job, self.root / 'case')
        self.assertEqual(actual['external_remote_config']['read_lanes'], 4)
        self.assertEqual(actual['evidence'], str(self.root / 'case' / 'read-evidence'))
        self.assertEqual(self.c.shared_tide['options'], options)
        self.c.prepare_tide.assert_not_called()

    def test_shared_auto_reader_does_not_inherit_preparation_lane_pin(self):
        options = {'external_remote_config': {'read_lanes': 64}, 'evidence': 'old'}
        self.c.shared_tide = {'identity': ('source', 'tcp', 'synchronous', 'adaptive'),
                              'options': copy.deepcopy(options)}
        job = {'backend': 'tidehunter_tcp', 'sealed_dataset': {'source_id': 'source'},
               'durability_tier': 'synchronous'}
        actual = self.c.prepared_tide_options(job, self.root / 'case')
        self.assertNotIn('read_lanes', actual['external_remote_config'])
        self.assertEqual(self.c.shared_tide['options'], options)

    def test_a_different_durability_tier_never_reuses_the_prepared_owners(self):
        # Owners run one tier for their whole life; a case at another tier needs fresh owners.
        self.c.shared_tide = {'identity': ('source', 'tcp', 'native'), 'options': {}}
        job = {'backend': 'tidehunter_nvme_tcp', 'sealed_dataset': {'source_id': 'source'},
               'durability_tier': 'synchronous', 'read_lanes': 1, 'case_id': 'fresh'}
        self.c.stop_tide = Mock()
        self.c.prepare_tide = Mock(side_effect=RuntimeError('fresh owners requested'))
        with patch.object(controller, 'controller_matrix', return_value=[job]), \
                self.assertRaisesRegex(RuntimeError, 'fresh owners requested'):
            self.c.prepared_tide_options(job, self.root / 'case')
        self.c.stop_tide.assert_called_once_with()

    def test_outstanding_namespaces_prevent_source_replacement_and_owner_stop(self):
        self.c.shared_tide = {'identity': ('old', 'tcp', 'synchronous')}
        self.c.active_nvme = {'generation': 'g', 'case': 'old-case', 'hosts': []}
        job = {'backend': 'tidehunter_tcp', 'sealed_dataset': {'source_id': 'new'}}
        with self.assertRaises(controller.SafetyError):
            self.c.prepared_tide_options(job, self.root)
        self.c.active_tide = ({'case_id': 'old-case'}, self.root, [0])
        with self.assertRaises((controller.SafetyError, controller.InfrastructureError)):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_unconfirmed_client_quiescence_forbids_owner_cleanup(self):
        self.c.active_tide = ({'case_id': 'case'}, self.root, [0])
        self.c.tide_cleanup_forbidden = True
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.c.request.assert_not_called()
        self.assertIsNotNone(self.c.active_tide)

    def test_failed_owner_cleanup_quarantines_remaining_owners(self):
        active = ({'case_id': 'case'}, self.root, [0])
        self.c.active_tide = active
        self.c.request.side_effect = RuntimeError('owner shutdown unconfirmed')
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.assertIs(self.c.active_tide, active)
        self.assertTrue(self.c.tide_cleanup_forbidden)
        self.c.request.reset_mock()
        with self.assertRaises(controller.InfrastructureError):
            self.c.stop_tide()
        self.c.request.assert_not_called()

    def test_undrained_or_incomplete_host_set_is_quarantined(self):
        for drained, count in ((False, 5), (True, 4)):
            with self.subTest(drained=drained, hosts=count):
                active = {'generation': 'g', 'case': 'case',
                          'hosts': [{'index': i, 'state': f'/state/{i}'} for i in range(count)]}
                self.c.active_nvme = active
                with patch.object(controller, 'command') as command:
                    with self.assertRaises(controller.InfrastructureError):
                        self.c.disconnect_nvme(drained=drained)
                command.assert_not_called()
                self.c.request.assert_not_called()
                self.assertIs(self.c.active_nvme, active)

    def test_cleanup_uses_exact_per_owner_proof_and_retains_unknown_outcome(self):
        real_path = Path
        control = self.root / 'control' / 'run'
        control.mkdir(parents=True)
        hosts = []
        for index in range(5):
            host = {'index': index, 'state': str(control / f'state-{index}'),
                    'mirror': str(control / f'mirror-{index}'),
                    'ready_path': str(control / f'ready-{index}.json'),
                    'target_path': str(control / f'target-{index}.json')}
            real_path(host['state']).mkdir()
            real_path(host['mirror']).mkdir()
            real_path(host['ready_path']).write_text('{}')
            real_path(host['target_path']).write_text('{}')
            hosts.append(host)
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        active = {'generation': 'g', 'case': 'case', 'case_directory': str(self.root),
                  'hosts': hosts, 'target_implementation': 'kernel'}
        self.c.active_nvme = active
        self.c.request.side_effect = lambda index, *a, **kw: {'complete': index != 2}
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', return_value=json.dumps({'phase': 'disconnected'})), \
             patch.object(controller, 's3_json') as upload:
            with self.assertRaises(controller.InfrastructureError):
                self.c.disconnect_nvme(drained=True)
        self.assertIs(self.c.active_nvme, active)
        self.assertEqual({call.args[0] for call in self.c.request.call_args_list}, set(range(5)))
        for call in self.c.request.call_args_list:
            index, request = call.args
            self.assertEqual(request['host_disconnected_key'], f'run/results/cells/case/storage/{index}/nvme-disconnect/g.json')
            self.assertIs(request['readers_drained'], True)
            self.assertEqual(request['action'], 'nvme_owner_target_cleanup')
        for host in hosts:
            for field in ('state', 'ready_path', 'target_path'):
                self.assertEqual(real_path(host[field]).exists(), host['index'] == 2)
            self.assertTrue(real_path(host['mirror']).exists())

    def test_compact_sealed_targets_connect_trace_and_drain(self):
        real_path = Path
        jobs = cloud_campaign.make_focused_matrix('fabric_ab_compact')
        arms = [next(j for j in jobs if j['backend'] == backend and j.get('nvmeof_target') == target)
                for backend, target in (('tidehunter_nvme_rdma', 'kernel'),
                                        ('tidehunter_nvme_rdma', 'spdk'),
                                        ('tidehunter_nvme_tcp', 'kernel'))]
        self.c.config['fleet']['nodes'] = self.c.config['fleet']['nodes'][:1]
        for job in arms:
            with self.subTest(backend=job['backend'], target=job['nvmeof_target']), \
                 tempfile.TemporaryDirectory() as tmp:
                base = real_path(tmp)
                roots, targets = self.mirror_fixtures(base, [1, 2], {'catalog': [{}], 'nqn': 'test-nqn'})
                transport = 'tcp' if job['backend'].endswith('_tcp') else 'rdma'
                implementation = 'spdk' if job['nvmeof_target'] == 'spdk' else 'kernel_direct'
                target = targets[0]['target']
                target['transport'] = transport
                entry = target['exports'][0]
                entry['target'] = implementation
                entry['target_receipt'] = {'selected_transport': transport, 'target_cache_verified': True,
                    'target_cache': ('direct_file_bdev_no_OS_pagecache' if implementation == 'spdk'
                                     else 'direct_file_no_OS_pagecache'),
                    'buffered_io': 0, 'target_implementation': implementation,
                    'direct_io_verified': True, 'read_only_verified': True}
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}],
                    'mirror_roots': roots[:1], 'options': {'external_remote_config': {
                        'read_memory_allowance_bytes': 256 * 1024**2,
                        'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
                self.c.owner_batch = Mock(return_value=targets[:1])
                self.c.active_nvme = None
                result = {'phase': 'connected', 'transport': transport,
                    'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test',
                                  'transport': transport, 'target': implementation}],
                    'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}
                def mapped_path(value):
                    path = real_path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                def command(argv, *, timeout):
                    if argv[:2] == ['nvme', 'id-ctrl']:
                        return json.dumps({'cntlid': 7})
                    if 'host-disconnect' in argv:
                        return json.dumps({'phase': 'disconnected'})
                    real_path(argv[argv.index('--state') + 1]).mkdir()
                    return json.dumps(result)
                with patch.object(controller, 'Path', side_effect=mapped_path), \
                     patch.object(controller, 'command', side_effect=command), \
                     patch.object(controller, 'persist_json'):
                    readers = self.c.connect_nvme(job, base)
                    self.assertEqual(len(readers), 1)
                    self.assertEqual(readers[0]['binding']['transport'], transport)
                    self.assertEqual(self.c.nvme_trace('start', 10), [{'complete': True}])
                    with self.assertRaises(controller.InfrastructureError):
                        self.c.disconnect_nvme(drained=False)
                    self.c.disconnect_nvme(drained=True)
                    self.assertIsNone(self.c.active_nvme)
                self.assertEqual(self.c.owner_batch.call_args.kwargs['transport'], transport)

    def test_rdma_transport_is_used_for_targets_and_namespace_bindings(self):
        real_path = Path
        manifest = {'catalog': [{}], 'nqn': 'test-nqn'}
        roots, targets = self.mirror_fixtures(self.root, [1, 2], manifest)
        for owner in targets:
            owner['target']['transport'] = 'rdma'
            owner['target']['exports'][0]['target_receipt']['selected_transport'] = 'rdma'
        self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                              'mirror_roots': roots, 'options': {'external_remote_config': {
                                  'read_memory_allowance_bytes': 256 * 1024**2,
                                  'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
        self.c.owner_batch = Mock(return_value=targets)
        result = {'phase': 'connected', 'transport': 'rdma',
                  'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test-only', 'transport': 'rdma'}],
                  'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}
        def mapped_path(value):
            path = real_path(value)
            return self.root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
        def command(argv, *, timeout):
            return json.dumps({'cntlid': 7} if argv[:2] == ['nvme', 'id-ctrl'] else result)
        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', side_effect=command):
            readers = self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_rdma',
                                           'nvmeof_route': 'sealed_read',
                                           'sealed_dataset': {'source_id': 'source-case'}}, self.root)
        self.assertEqual(self.c.owner_batch.call_args.kwargs['transport'], 'rdma')
        self.assertEqual(self.c.active_nvme['transport'], 'rdma')
        self.assertEqual({row['binding']['transport'] for row in readers}, {'rdma'})

    def test_missing_or_extra_reader_binding_is_rejected_and_quarantined(self):
        real_path = Path
        for bindings in ([], [{'export_uuid': [1], 'namespace_path': '/dev/test', 'transport': 'tcp'},
                              {'export_uuid': [2], 'namespace_path': '/dev/other', 'transport': 'tcp'}]):
            with self.subTest(bindings=bindings), tempfile.TemporaryDirectory() as directory:
                base = real_path(directory)
                self.c.active_nvme = None
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = real_path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                result = {'phase': 'connected', 'transport': 'tcp', 'bindings': bindings,
                          'exports': [{'export_uuid': [1], 'host_receipt': {'controller': 'nvme0'}}]}
                def command(argv, *, timeout):
                    if argv[:2] == ['nvme', 'id-ctrl']:
                        return json.dumps({'cntlid': 7})
                    return json.dumps(result)
                with patch.object(controller, 'Path', side_effect=mapped_path), \
                     patch.object(controller, 'command', side_effect=command):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp',
                                             'nvmeof_route': 'sealed_read',
                                             'sealed_dataset': {'source_id': 'source-case'}}, base)
                self.assertEqual(len(self.c.active_nvme['hosts']), 1)

    def test_target_policy_and_transport_mismatch_prevent_host_connection(self):
        for field, value in (('selected_transport', 'rdma'), ('target_cache', 'direct'),
                             ('target_cache_verified', False)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                roots, targets = self.mirror_fixtures(base, [1], {'catalog': [{}], 'nqn': 'test-nqn'})
                targets[0]['target']['exports'][0]['target_receipt'][field] = value
                self.c.active_nvme = None
                self.c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
                                      'mirror_roots': roots}
                self.c.owner_batch = Mock(return_value=targets)
                def mapped_path(value):
                    path = Path(value)
                    return base / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path
                with patch.object(controller, 'command') as command, \
                     patch.object(controller, 'dump'), \
                     patch.object(controller, 'Path', side_effect=mapped_path):
                    with self.assertRaises(controller.SafetyError):
                        self.c.connect_nvme({'case_id': 'read-case', 'backend': 'tidehunter_nvme_tcp',
                                             'nvmeof_route': 'sealed_read',
                                             'sealed_dataset': {'source_id': 'source-case'}}, base)
                command.assert_not_called()
                self.assertEqual(self.c.active_nvme['hosts'], [])
                self.assertIs(self.c.active_nvme['targets'], targets)


if __name__ == '__main__':
    unittest.main()
