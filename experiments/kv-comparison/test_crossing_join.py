"""Synthetic clock and first-match evidence coverage; no host tracing needed."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import crossing_join as join


def fixture(width=100_000):
    timeline = {'origin_monotonic_ns': 1_000_000_000,
                'control_events': [{'owner': 0, 'envelope_id': 7, 'stage': 'submit', 'elapsed_ns': 0},
                                   {'owner': 0, 'envelope_id': 7, 'stage': 'complete', 'elapsed_ns': width}],
                'records': [{'batch': {'epoch': 1, 'sequence': 1}, 'events': [
                    {'stage': 'live_io_submitted', 'elapsed_ns': 10_000_000, 'owner': 0,
                     'nsid': 3, 'file_offset': 4096, 'namespace_block_size': 512, 'frame_len': 4096},
                    {'stage': 'live_io_completed', 'elapsed_ns': 20_000_000, 'owner': 0}]}]}
    return timeline, [{'kind': 'control_received', 't_ns': 2_000_000_000, 'partition': 0, 'a': 7, 'b': 0}]


def trace(name, time, args='', worker='nvmet/0-10'):
    return f'{worker} [000] .... {time}: {name}: {args}\n'


def command(start='2.012000', end='2.012500', nsid=3):
    return trace('nvmet_req_init', start, f'qid=1 cid=2 nsid={nsid} slba=8 len=7') + trace('nvmet_req_complete', end, 'qid=1 cid=2')


class CrossingJoinTests(unittest.TestCase):
    def test_clock_bounds(self):
        t, o = fixture()
        self.assertEqual(join.alignment(t, o), {'status': 'aligned', 'samples': 1, 'inliers': 1, 'outliers': 0,
                         'lower_ns': 999_900_000, 'upper_ns': 1_000_000_000, 'width_ns': 100_000})

    def test_clock_voting_keeps_ninety_percent_inliers(self):
        t, _ = fixture()
        t['control_events'] = []
        owner = []
        for identity in range(10):
            t['control_events'] += [dict(owner=0, envelope_id=identity, stage='submit', elapsed_ns=0,
                                         target=dict(kind='adoption')),
                                    dict(owner=0, envelope_id=identity, stage='complete', elapsed_ns=100_000,
                                         target=dict(kind='adoption'))]
            owner.append(dict(kind='control_received', a=identity, b=34,
                              t_ns=2_000_000_000 if identity < 9 else 3_000_000_000))
        result = join.alignment(t, owner)
        self.assertEqual(result['status'], 'aligned')
        self.assertEqual((result['inliers'], result['outliers'], result['width_ns']), (9, 1, 100_000))

    def test_reused_envelope_ids_match_request_kind(self):
        t, _ = fixture()
        t['control_events'] = [dict(owner=0, envelope_id=7, stage=stage, elapsed_ns=elapsed,
                                   target=dict(kind=kind))
                               for kind in ('adoption', 'position')
                               for stage, elapsed in (('submit', 0), ('complete', 100_000))]
        owner = [dict(kind='control_received', a=7, b=34, t_ns=2_000_000_000),
                 dict(kind='control_received', a=7, b=18, t_ns=2_000_000_000),
                 dict(kind='control_received', a=7, b=33, t_ns=3_000_000_000)]
        result = join.alignment(t, owner)
        self.assertEqual((result['samples'], result['inliers'], result['outliers']), (2, 2, 0))
        self.assertEqual(result['status'], 'aligned')

    def test_kernel_nvmet_layout(self):
        t, o = fixture()
        text = trace('nvmet_req_init', '2.012000',
                     'nvmet1: disk=nvme0n1, qid=1, cid=2, nsid=3, cmd=(nvme_cmd_write slba=8, len=7)')
        text += trace('nvmet_req_complete', '2.012500',
                      'nvmet1: qid=1, cid=2, status=0')
        self.assertEqual(join.analyze(t, o, text)['counts']['M4_target_fast'], 1)

    def test_unaligned(self):
        t, o = fixture(2_000_000)
        report = join.analyze(t, o, command())
        self.assertEqual(report['clock']['status'], 'unaligned')
        self.assertEqual(report['counts']['M4_target_fast'], 1)

    def test_inconsistent_stops(self):
        t, o = fixture()
        t['control_events'] += [{'owner': 0, 'envelope_id': 8, 'stage': 'submit', 'elapsed_ns': 0},
                                {'owner': 0, 'envelope_id': 8, 'stage': 'complete', 'elapsed_ns': 100}]
        o.append({'event': 'control_received', 'monotonic_ns': 3_000_000_000, 'a': {'id': 8}})
        t['control_events'] += [{'owner': 0, 'envelope_id': 9, 'stage': 'submit', 'elapsed_ns': 0},
                                {'owner': 0, 'envelope_id': 9, 'stage': 'complete', 'elapsed_ns': 100}]
        o.append({'kind': 'control_received', 't_ns': 4_000_000_000, 'a': 9, 'b': 0})
        report = join.analyze(t, o)
        self.assertEqual(report['clock']['status'], 'inconsistent')
        self.assertEqual(report['joins'], [])

    def test_missing_clocks_keep_unique_slow_groups(self):
        t, _ = fixture()
        t['records'].append(copy.deepcopy(t['records'][0]))
        report = join.analyze(t)
        self.assertEqual(report['clock']['status'], 'no_owner_clock')
        self.assertEqual(len(report['joins']), 1)
        self.assertEqual(len(report['joins'][0]['members']), 2)
        self.assertEqual(report['counts']['unattributed'], 1)
        t.pop('origin_monotonic_ns')
        self.assertEqual(join.analyze(t, fixture()[1])['clock']['status'], 'no_owner_clock')

    def test_m1_first_match(self):
        t, o = fixture()
        text = command('2.011000', '2.019000') + trace('balance_dirty_pages', '2.016000', 'pause=3')
        self.assertEqual(join.analyze(t, o, text)['counts']['M1_dirty_throttle'], 1)

    def test_m1_kworker_identified_by_nvmet_tracepoint(self):
        t, o = fixture()
        text = command('2.011000', '2.019000') + trace('balance_dirty_pages', '2.016000', 'pause=3')
        text = text.replace('nvmet/0-10', 'kworker/3:1H-10')
        self.assertEqual(join.analyze(t, o, text)['counts']['M1_dirty_throttle'], 1)

    def test_dirty_pause_wrong_worker_not_m1(self):
        t, o = fixture()
        text = command('2.011000', '2.019000') + trace('balance_dirty_pages', '2.016000', 'pause=3', 'other-3')
        self.assertEqual(join.analyze(t, o, text)['counts']['unattributed'], 1)

    def test_m2(self):
        t, o = fixture()
        o.append({'kind': 'prepare_start', 't_ns': 2_010_000_000, 'partition': 0, 'a': 0, 'b': 0})
        text = trace('jbd2_start_commit', '2.012000', 'dev=1 transaction=2', 'jbd2-3') + trace('jbd2_end_commit', '2.018000', 'dev=1 transaction=2', 'jbd2-3')
        self.assertEqual(join.analyze(t, o, text)['counts']['M2_journal_commit'], 1)
        o[-1]['t_ns'] = 2_000_000_000
        self.assertEqual(join.analyze(t, o, text)['counts']['unattributed'], 1)

    def test_live_group_membership_geometry_and_deduplication(self):
        t, o = fixture()
        event = t['records'][0]['events'][0]
        geometry = {key: event.pop(key) for key in
                    ('nsid', 'file_offset', 'namespace_block_size', 'frame_len')}
        second = copy.deepcopy(t['records'][0])
        second['batch']['sequence'] = 2
        second['events'][0]['elapsed_ns'] += 1
        t['records'].append(second)
        t['live_groups'] = [{'owner': 0, 'batches': [r['batch'] for r in t['records']], **geometry}]
        report = join.analyze(t, o, command())
        self.assertEqual(len(report['joins']), 1)
        self.assertEqual(len(report['joins'][0]['members']), 2)
        self.assertEqual(report['counts']['M4_target_fast'], 1)
        t['live_groups'][0] = {'owner': 0, 'batches': t['live_groups'][0]['batches']}
        report = join.analyze(t, o, command())
        self.assertEqual(report['counts']['unattributed'], 1)
        self.assertIn('live_group_geometry_unavailable', report['joins'][0]['missing_points'])

    def test_m3_new_file_and_rebound(self):
        for event in ({'wal_file': 'wal-1'}, {}):
            t, o = fixture()
            t['records'][0]['events'][0].update(event)
            if not event:
                t['records'][0]['events'].insert(0, {'stage': 'live_queue_rebound', 'elapsed_ns': 9_000_000})
            self.assertEqual(join.analyze(t, o)['counts']['M3_queue_rebind'], 1)

    def test_m3_disallowed_with_fs_overlap(self):
        t, o = fixture()
        t['records'][0]['events'][0]['wal_file'] = 'wal-1'
        text = trace('ext4_sync_file_enter', '2.011000', 'dev=1 ino=2') + trace('ext4_sync_file_exit', '2.019000', 'dev=1 ino=2')
        self.assertEqual(join.analyze(t, o, text)['counts']['unattributed'], 1)

    def test_m4_and_namespace_lba_matching(self):
        t, o = fixture()
        self.assertEqual(join.analyze(t, o, command())['counts']['M4_target_fast'], 1)
        for text in (command(nsid=4), command().replace('slba=8', 'slba=9')):
            report = join.analyze(t, o, text)
            self.assertEqual(report['counts']['unattributed'], 1)
            self.assertIn('matching_nvmet_command', report['joins'][0]['missing_points'])

    def test_fallback_and_threshold(self):
        t, o = fixture()
        t['records'][0]['events'][0]['stage'] = 'live_io_started'
        self.assertTrue(join.analyze(t, o)['joins'][0]['fallback_started'])
        t['records'][0]['events'][1]['elapsed_ns'] = 13_000_000
        self.assertEqual(join.analyze(t, o)['joins'], [])

    def test_missing_trace_points(self):
        _, intervals, missing = join.parse_ftrace(trace('nvmet_req_complete', '2.000000', 'qid=1 cid=2') + trace('nvmet_req_init', '2.001000', 'qid=1 cid=3 nsid=3 slba=8'))
        self.assertEqual(intervals, [])
        self.assertEqual({p['reason'] for p in missing}, {'missing_start', 'missing_end'})

    def test_saved_case_layout(self):
        t, o = fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'result').mkdir()
            (root/'owner-events').mkdir()
            (root/'result/write-timeline.json').write_text(json.dumps({'timeline': t}))
            (root/'owner-events/node-0.json').write_text(json.dumps({'events': o}))
            (root/'owner-ftrace.txt').write_text(command())
            self.assertEqual(join.load_case(root)['counts']['M4_target_fast'], 1)


if __name__ == '__main__':
    unittest.main()
