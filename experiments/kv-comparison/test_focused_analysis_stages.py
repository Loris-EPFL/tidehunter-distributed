import csv
import json
from pathlib import Path
import tempfile
import unittest

import focused_analysis


class StagesAnalysisTests(unittest.TestCase):
    def test_sync_step_keeps_call_populations_and_contracts(self):
        import sync_step
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = [
                {'case_id': 'n', 'group': 'G12D', 'backend': 'native', 'mix': {'put': 100},
                 'native_durability': 'value_wal_barrier', 'value_size': 1024, 'concurrency': 1},
                {'case_id': 'o', 'group': 'G12D', 'backend': 'tidehunter_tcp', 'mix': {'put': 100},
                 'durability_tier': 'synchronous', 'value_size': 1024, 'concurrency': 1}]
            for job in jobs:
                (root / 'cases' / job['case_id'] / 'result').mkdir(parents=True)
            (root / 'cases/n/result/write-diagnostics.json').write_text(json.dumps({'profile': {
                'selected_operations': 2, 'value_wal_persistence': {'status': 'sampled',
                'file_ns': 4000, 'namespace_ns': 2000, 'file_calls': 2, 'namespace_calls': 2}}}))
            (root / 'cases/o/result/write-timeline.json').write_text(json.dumps({'timeline': {'owner_records': [
                {'wal_file_persist_ns': 1000, 'append_persist_ns': 4000},
                {'wal_file_persist_ns': 3000}, {'append_persist_ns': 8000}]}}))
            rows = sync_step.rows(root, [{'job': j, 'case_id': j['case_id'], 'status': 'completed'} for j in jobs],
                                  [], focused_analysis.stages_route)
            by_step = {(r['case_id'], r['step']): r for r in rows}
            self.assertEqual(by_step['n', 'file + directory fsync']['mean_us'], 3)
            self.assertEqual(by_step['o', 'file fdatasync']['p99_us'], 3)
            self.assertEqual(by_step['o', 'append persistence (inclusive)']['mean_us'], 6)
            self.assertEqual(by_step['o', 'file fdatasync']['n'], 2)

    def test_repetitions_are_not_pooled_and_headline_is_contract_matched(self):
        def job(case_id, repeat, policy='overwrite'):
            result = {'case_id': case_id, 'group': 'G12W', 'case_kind': 'remote_put',
                      'backend': 'tidehunter_verbs_native', 'value_size': 1024,
                      'concurrency': 1, 'durability_tier': 'synchronous', 'repeat': repeat,
                      'mix': {'put': 100}, 'keys': 4096, 'distribution': 'uniform'}
            if policy == 'new':
                result['put_key_policy'] = policy
                result['group'] = 'G12K'
            return result
        jobs = [job('a', 0), job('b', 1), job('c', 0, 'new')]
        cases = [{'case_id': j['case_id'], 'job': j, 'status': 'completed'} for j in jobs]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case, values in zip(cases, ([1000, 2000, 9000], [10000, 20000], [500000])):
                folder = root / 'cases' / case['case_id'] / 'result'
                folder.mkdir(parents=True)
                with (folder / 'samples.csv').open('w', newline='') as stream:
                    writer = csv.writer(stream)
                    writer.writerow(['phase', 'operation', 'outcome', 'dispatched_ns', 'completed_ns', 'api_ns'])
                    for i, value in enumerate(values):
                        writer.writerow(['measurement', 'put', 'success', i * 1000000, i * 1000000 + value, value])
            rows = focused_analysis.stages_ops(root, cases)
            row = next(r for r in rows if r['key_policy'] == 'overwrite')
            # Independent nearest-rank and arithmetic-mean recomputation from each raw file.
            independent = []
            for case in cases[:2]:
                with (root / 'cases' / case['case_id'] / 'result' / 'samples.csv').open() as stream:
                    vals = sorted(int(r['api_ns']) for r in csv.DictReader(stream))
                independent.append((vals[(len(vals) - 1) // 2] / 1000, sum(vals) / len(vals) / 1000))
            self.assertEqual([x['p50_us'] for x in row['repetitions']], [x[0] for x in independent])
            self.assertEqual([x['mean_us'] for x in row['repetitions']], [x[1] for x in independent])
            self.assertEqual(row['p50_range_us']['min'], 2)
            self.assertEqual(row['p50_range_us']['max'], 10)
            one = next(r for r in rows if r['key_policy'] == 'new')
            self.assertEqual(one['repetition_count'], 1)
            self.assertEqual(one['p50_range_us'], 'range n/a (1 repetition)')
            headline = next(r for r in focused_analysis.stages_apples(rows)
                            if r['route'] == 'owner verbs' and r['size_bytes'] == 1024 and r['concurrency'] == 1)
            self.assertEqual(headline['durable_write']['case_ids'], ['a', 'b'])
            self.assertEqual(headline['page_cache_write'], 'not measured')
            self.assertEqual(headline['disk_read'], 'not measured')
            remote_native = {**row, 'case_ids': ['native-tier'], 'durability': 'native'}
            measured = next(r for r in focused_analysis.stages_apples(rows + [remote_native])
                            if r['route'] == 'owner verbs' and r['size_bytes'] == 1024 and r['concurrency'] == 1)
            self.assertEqual(measured['page_cache_write']['case_ids'], ['native-tier'])
            self.assertEqual(measured['page_cache_write']['durability'], 'native')
            buffered = {**row, 'case_ids': ['buffered'], 'route': 'hybrid kernel buffered',
                        'shape': 'read', 'cache_state': 'memory', 'target_io': 'buffered'}
            direct = {**buffered, 'case_ids': ['direct'], 'route': 'hybrid kernel DIRECT',
                      'cache_state': 'cold', 'target_io': 'direct'}
            hybrid = next(r for r in focused_analysis.stages_apples([buffered, direct])
                          if r['route'] == 'hybrid kernel DIRECT' and r['size_bytes'] == 1024 and r['concurrency'] == 1)
            self.assertEqual((hybrid['cache_read']['case_ids'], hybrid['cache_read']['target_io']), (['buffered'], 'buffered'))
            self.assertEqual((hybrid['disk_read']['case_ids'], hybrid['disk_read']['target_io']), (['direct'], 'direct'))

    def test_headline_prefers_qualified_profile_when_baseline_not_run(self):
        job = {'case_id': 'baseline', 'group': 'G12N', 'case_kind': 'native_op',
               'backend': 'native', 'value_size': 65536, 'concurrency': 1,
               'repeat': 0, 'mix': {'put': 100}, 'durability_tier': 'value_wal_barrier'}
        profile = {**job, 'case_id': 'profile', 'group': 'G12D',
                   'diagnostics': {'write_profile_limit': 4096}}
        cases = [{'case_id': 'baseline', 'job': job, 'status': 'not_run'},
                 {'case_id': 'profile', 'job': profile, 'status': 'completed'}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case in cases:
                result = root / 'cases' / case['case_id'] / 'result'
                result.mkdir(parents=True)
                (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                                                     'measurement,put,success,0,10000,10000\n')
            def cell():
                rows = focused_analysis.stages_ops(root, cases)
                return next(r for r in focused_analysis.stages_apples(rows)
                            if r['route'] == 'native' and r['size_bytes'] == 65536 and r['concurrency'] == 1)['durable_write']
            measured = cell()
            self.assertEqual((measured['p50_us'], measured['source'], measured['case_ids']), (10, 'profiled copy', ['profile']))
            cases[0]['status'] = 'completed'
            self.assertEqual((cell()['source'], cell()['case_ids']), ('unprofiled baseline', ['baseline']))
            cases[0]['target_io_qualification'] = {'status': 'unqualified', 'reason_codes': ['unverified']}
            self.assertEqual(cell()['source'], 'profiled copy')
            del cases[0]['target_io_qualification']
            cases[1]['status'] = 'not_run'
            cases[0]['status'] = 'not_run'
            self.assertEqual(cell(), 'not measured')

    def test_hybrid_missing_qualification_does_not_become_qualified(self):
        job = {'case_id': 'hybrid', 'group': 'G12H', 'case_kind': 'hybrid_put',
               'backend': 'tidehunter_nvme_rdma', 'target_io': 'direct',
               'value_size': 1024, 'concurrency': 1, 'repeat': 0,
               'mix': {'put': 100}, 'durability_tier': 'synchronous'}
        case = {'case_id': 'hybrid', 'job': job, 'status': 'completed'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / 'cases' / 'hybrid' / 'result'
            result.mkdir(parents=True)
            (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\nmeasurement,put,success,1,1001,1000\n')
            row = focused_analysis.stages_ops(root, [case])[0]
            self.assertEqual(row['verdicts'], ['missing'])
            self.assertIsNone(row['p50_us'])

    def test_generated_tables_unwrap_diagnostic_evidence(self):
        jobs = [
            {'case_id': 'trace', 'group': 'G12D', 'case_kind': 'remote_put',
             'backend': 'tidehunter_verbs_native', 'value_size': 1024, 'concurrency': 1,
             'repeat': 0, 'mix': {'put': 100}, 'durability_tier': 'synchronous',
             'diagnostics': {'write_timeline_limit': 4}},
            {'case_id': 'read', 'group': 'G12D', 'case_kind': 'media_read',
             'backend': 'tidehunter_verbs_native', 'value_size': 1024, 'concurrency': 1,
             'repeat': 0, 'mix': {'read': 100}, 'media': {'expected': 'memory'},
             'diagnostics': {'read_profile_limit': 4}}]
        cases = [{'case_id': j['case_id'], 'job': j, 'status': 'completed'} for j in jobs]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report, output = root / 'export', root / 'report', root / 'output'
            export.mkdir()
            report.mkdir()
            (export / 'matrix.json').write_text(json.dumps({'jobs': jobs}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            for case in cases:
                result = export / 'cases' / case['case_id'] / 'result'
                result.mkdir(parents=True)
            trace = export / 'cases' / 'trace' / 'result'
            (trace / 'samples.csv').write_text(
                'phase,operation,outcome,dispatched_ns,completed_ns,api_ns,caller_sample_id\n'
                'measurement,put,success,0,10000,10000,7\n'
                'measurement,put,success,0,11000,11000,8\n')
            (trace / 'write-timeline.json').write_text(json.dumps({'timeline': {'dropped_calls': 0, 'dropped_events': 0,
                'records': [{'caller_sample_id': 7, 'completed': True, 'events': [
                    {'stage': 'start', 'elapsed_ns': 1000}, {'stage': 'end', 'elapsed_ns': 3000}]},
                    {'caller_sample_id': 8, 'completed': True, 'events': [
                    {'stage': 'start', 'elapsed_ns': 1000}, {'stage': 'end', 'elapsed_ns': 5000}]}]}}))
            read = export / 'cases' / 'read' / 'result'
            (read / 'read-diagnostics.json').write_text(json.dumps({'profile': {
                'selection_policy': 'remote flights', 'stages': {'exchange': {'sum_ns': 4000, 'count': 1}},
                'coordinator_resolution': {'selection_policy': 'API resolutions',
                                           'stages': {'index_lookup': {'sum_ns': 2000, 'count': 1}}}}}))
            tables = focused_analysis.generate(export, report, output)['tables']
            interval = next(r for r in tables['decomposition_write'] if r['stage'] == 'start → end')
            self.assertEqual(interval['mean_us'], 3)
            self.assertEqual(interval['p50_us'], 2)
            self.assertAlmostEqual(interval['share_of_api_mean'], 6 / 21)
            self.assertEqual(interval['exactness'], 'aggregate_of_exact_intervals')
            self.assertTrue(all(x['exactness'] == 'exact_interval' for x in interval['intervals']))
            residual = next(r for r in tables['decomposition_write'] if r['stage'] == 'unattributed')
            self.assertEqual(residual['p50_us'], 7)
            self.assertIn('aggregate_of_exact_intervals', (output / 'decomposition_write.md').read_text())
            stages = {r['stage']: r for r in tables['decomposition_read']}
            self.assertEqual(stages['index_lookup']['mean_us'], 2)
            self.assertEqual(stages['exchange']['mean_us'], 4)
            self.assertNotEqual(stages['index_lookup']['selection'], stages['exchange']['selection'])
            for name in ('ops_summary', 'apples_to_apples', 'decomposition_write', 'decomposition_read', 'stall'):
                self.assertTrue((output / (name + '.csv')).is_file())
                self.assertTrue((output / (name + '.md')).is_file())
            self.assertTrue((output / 'where_time_goes.md').is_file())

    def test_floor_annotation_keeps_mean_and_p50_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            floors = {'net_floor': [{'case_ids': ['n'], 'status': 'completed',
                                     'p50_us': 2, 'avg_us': 4}],
                      'ssd_floor': [{'case_ids': ['s'], 'status': 'completed',
                                     'p50_us': 6, 'mean_us': 9},
                                    {'case_ids': ['no-mean'], 'status': 'completed', 'p50_us': 8}]}
            write, read, _ = focused_analysis.stages_decompositions(Path(directory), [], floors)
            for rows in (write, read):
                annotated = {r['case_ids'][0]: r for r in rows}
                self.assertEqual((annotated['n']['p50_us'], annotated['n']['mean_us']), (2, 4))
                self.assertEqual((annotated['s']['p50_us'], annotated['s']['mean_us']), (6, 9))
                self.assertIsNone(annotated['no-mean']['mean_us'])

    def test_native_and_hybrid_unqualified_are_visible(self):
        jobs = [
            {'case_id': 'native-cold', 'group': 'G12N', 'case_kind': 'native_media_read',
             'backend': 'native', 'repeat': 0, 'mix': {'read': 100},
             'value_size': 1024, 'concurrency': 1, 'media': {'expected': 'cold'}},
            {'case_id': 'hybrid', 'group': 'G12H', 'case_kind': 'hybrid_put',
             'backend': 'tidehunter_nvme_rdma', 'target_io': 'direct', 'repeat': 0,
             'mix': {'put': 100}, 'value_size': 1024, 'concurrency': 1}]
        cases = [
            {'case_id': 'native-cold', 'job': jobs[0], 'status': 'completed',
             'focused_evidence': {'media_evidence': {'status': 'unqualified',
                 'reason_codes': ['device_read_ratio_low'], 'read_ops_per_success': .5}}},
            {'case_id': 'hybrid', 'job': jobs[1], 'status': 'completed',
             'hybrid_put_qualification': {'qualified': False, 'reasons': ['targets_unverified']}}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report, output = root / 'export', root / 'report', root / 'output'
            export.mkdir()
            report.mkdir()
            (export / 'matrix.json').write_text(json.dumps({'jobs': jobs}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            tables = focused_analysis.generate(export, report, output)['tables']
            rows = {r['case_id']: r for r in tables['unqualified']}
            self.assertEqual(rows['native-cold']['reason_codes'], ['device_read_ratio_low'])
            self.assertEqual(rows['hybrid']['reason_codes'], ['targets_unverified'])
            self.assertEqual(rows['hybrid']['verdict'], 'unqualified')
            self.assertTrue((output / 'unqualified.csv').is_file())

    def test_compact_stages_markdown(self):
        contract = {'route': 'owner verbs', 'shape': 'put', 'size_bytes': 1024,
                    'concurrency': 1, 'durability': 'synchronous', 'key_policy': 'overwrite',
                    'cache_state': None, 'target_io': 'not_applicable', 'group': 'G12W'}
        rep = {'case_id': 'a', 'repeat': 0, 'status': 'completed', 'verdict': 'qualified',
               'reason_codes': [], 'n': 8192}
        op = {**contract, 'case_ids': ['a'], 'repetitions': [rep], 'n': {'0': 8192},
              'qualified_repetitions': 1, 'p50_us': 40.9, 'p99_us': 90, 'p999_us': 100,
              'p50_range_us': {'min': 38.2, 'max': 42.0}, 'verdicts': ['qualified'], 'diagnostics': {}}
        headline = focused_analysis.stages_apples([op])
        text = '\n'.join(focused_analysis.stages_markdown('apples_to_apples', headline, {'ops_summary': [op]}))
        self.assertIn('p50 40.9 us', text)
        self.assertIn('n=1x8192; qualified; unprofiled baseline; synchronous', text)
        self.assertIn('rows with no measured cells omitted', text)
        self.assertNotIn('case_ids', text)
        stage = {**contract, 'case_ids': ['a'], 'stage': 'send → reply',
                 'selection': 'retained calls', 'exactness': 'aggregate_of_exact_intervals',
                 'count': 2, 'mean_us': 3, 'p50_us': 2, 'share_of_api_mean': .5,
                 'intervals': [{'start_ns': 1, 'end_ns': 2, 'caller_sample_id': 7}]}
        write = '\n'.join(focused_analysis.stages_markdown('decomposition_write', [stage], {}))
        self.assertIn('send → reply | aggregate_of_exact_intervals | 2 | 3 | 2 | 0.5', write)
        self.assertIn('Selection: retained calls', write)
        self.assertNotIn('caller_sample_id', write)
        read_rows = [{**stage, 'stage': 'index_lookup', 'exactness': 'sampled_mean'},
                     {**stage, 'stage': 'exchange', 'exactness': 'sampled_mean'},
                     {**stage, 'stage': 'server_handler', 'parent': 'exchange', 'exactness': 'sampled_mean'}]
        read = '\n'.join(focused_analysis.stages_markdown('decomposition_read', read_rows, {}))
        self.assertLess(read.index('| index_lookup |'), read.index('| exchange |'))
        self.assertLess(read.index('| exchange |'), read.index('| ↳ server_handler |'))
        stall = {**contract, 'case_id': 's', 'completion_count': 200, 'count': 2,
                 'mean_spacing_puts': 42, 'duration_fit': {'intercept_us': 5, 'slope_us_per_put': .1},
                 'phase_counts': {'reply → finish': 2, 'send → reply': 1},
                 'stalls': [{'longest_phase': 'reply → finish', 'longest_phase_us': 2},
                            {'longest_phase': 'send → reply', 'longest_phase_us': 8}]}
        text = '\n'.join(focused_analysis.stages_markdown('stall', [{'overwrite': stall, 'new': 'not measured'}], {}))
        self.assertIn('s | overwrite | 200 | 2 | 42 | 5 us + 0.1 us/put', text)
        self.assertIn('us/put | send → reply |', text)
        self.assertIn('reply → finish: 2', text)

    def test_markdown_hides_empty_groups_and_limits_transport_annotations(self):
        contract = {'route': 'owner verbs', 'shape': 'put', 'size_bytes': 1024, 'concurrency': 1,
                    'durability': 'synchronous', 'key_policy': 'overwrite', 'cache_state': None}
        base = {**contract, 'case_ids': ['case'], 'stage': 'event', 'count': 1, 'mean_us': 2,
                'exactness': 'sampled_mean', 'selection': 'scope'}
        rows = [base, {**base, 'stage': 'unavailable', 'count': 0, 'mean_us': None}]
        rows.extend({**base, 'stage': f'client_transport_{i:02}', 'mean_us': i} for i in range(12))
        for table in ('decomposition_write', 'decomposition_read'):
            text = '\n'.join(focused_analysis.stages_markdown(table, rows, {}))
            self.assertNotIn('unavailable', text)
            self.assertNotIn('client_transport_01', text)
            self.assertIn('client_transport_11', text)
            self.assertIn('top 10 by mean', text)
        summary = [{**contract, 'case_id': 'measured', 'api_mean_us': 10, 'component': 'client coordinator',
                    'mean_us': 4, 'share_of_api_mean': .4, 'exactness': 'sampled_mean',
                    'source_stages': ['a'], 'selection': 'scope', 'floor': [], 'excess_over_floor_us': []},
                   {**contract, 'case_id': 'empty', 'api_mean_us': None, 'component': 'client coordinator',
                    'mean_us': None, 'share_of_api_mean': None, 'exactness': 'not computed',
                    'source_stages': [], 'selection': 'missing', 'floor': [], 'excess_over_floor_us': []}]
        text = '\n'.join(focused_analysis.stages_markdown('component_summary', summary, {}))
        self.assertIn('## measured:', text)
        self.assertNotIn('## empty:', text)
        self.assertIn('1 groups with no measured components omitted', text)
        routes = ('owner verbs', 'owner TCP', 'verbs mTLS', 'hybrid kernel', 'hybrid SPDK', 'native')
        empty = {'action': 'put', 'size_bytes': 8192, 'concurrency': 1, 'tier': 'durable',
                 'key_policy': 'overwrite', 'workload': 'standard', 'component': 'client coordinator',
                 **{route: 'not measured' for route in routes}}
        text = '\n'.join(focused_analysis.stages_markdown('component_compare', [empty], {}))
        self.assertNotIn('## put / 8192', text)
        self.assertIn('1 groups with no measured components omitted', text)

    def test_where_time_goes_uses_case_local_components_and_floor(self):
        base = {'route': 'owner verbs', 'shape': 'put', 'size_bytes': 1024, 'concurrency': 1,
                'durability': 'synchronous', 'key_policy': 'overwrite', 'cache_state': None,
                'case_id': 'case', 'api_mean_us': 10, 'exactness': 'sampled_mean', 'selection': '',
                'excess_over_floor_us': []}
        rows = [{**base, 'component': 'client coordinator', 'mean_us': 4, 'share_of_api_mean': .4},
                {**base, 'component': focused_analysis.OWNER_PERSIST, 'mean_us': 3, 'share_of_api_mean': .3,
                 'excess_over_floor_us': [{'mode': 'fdatasync', 'size_bytes': 8192, 'case_id': 'floor',
                                           'mean_minus_floor_p50_us': 2}]},
                {**base, 'component': 'residual', 'mean_us': 1, 'share_of_api_mean': .1}]
        text = '\n'.join(focused_analysis.where_time_goes(rows))
        self.assertIn('client coordinator: 4 us (40%, sampled)', text)
        self.assertIn('owner persist (file sync, includes software): 3 us (30%, sampled)', text)
        self.assertIn('fdatasync 8192B floor, floor', text)
        self.assertIn('not a pooled repetition', text)

    def test_native_residual_and_read_transport_with_floor(self):
        native = {'case_id': 'native', 'group': 'G12D', 'case_kind': 'native_op', 'backend': 'native',
                  'value_size': 1024, 'concurrency': 1, 'repeat': 0, 'mix': {'put': 100},
                  'durability_tier': 'value_wal_barrier', 'diagnostics': {'write_profile_limit': 4}}
        reader = {'case_id': 'reader', 'group': 'G12D', 'case_kind': 'media_read',
                  'backend': 'tidehunter_verbs_native', 'value_size': 1024, 'concurrency': 1,
                  'repeat': 0, 'mix': {'read': 100}, 'media': {'expected': 'cold'},
                  'durability_tier': 'synchronous', 'diagnostics': {'read_profile_limit': 4}}
        cases = [{'case_id': 'native', 'job': native, 'status': 'completed'},
                 {'case_id': 'reader', 'job': reader, 'status': 'completed',
                  'focused_evidence': {'media_evidence': {'status': 'qualified'}}}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case, operation in zip(cases, ('put', 'read')):
                result = root / 'cases' / case['case_id'] / 'result'
                result.mkdir(parents=True)
                (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                    f'measurement,{operation},success,0,20000,20000\n'
                    f'measurement,{operation},success,30000,50000,20000\n')
            profile = {'runtime_observation_mode': 'runtime_selected_native_scalar_write_stages',
                       'selected_operations': 2, 'stages': {'wal_append_ns': 4000, 'index_apply_ns': 2000},
                       'value_wal_persistence': {'status': 'sampled', 'file_ns': 6000, 'file_calls': 2}}
            native_dir = root / 'cases' / 'native' / 'result'
            (native_dir / 'write-diagnostics.json').write_text(json.dumps({'profile': profile}))
            reader_dir = root / 'cases' / 'reader' / 'result'
            read_profile = {'selected_operations': 2, 'selection_policy': 'first N flights',
                            'stages': {'exchange': {'count': 2, 'sum_ns': 20000},
                                       'server_handler': {'count': 2, 'sum_ns': 8000}}}
            (reader_dir / 'read-diagnostics.json').write_text(json.dumps({'profile': read_profile}))
            writes, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            residual = next(r for r in writes if r['stage'] == 'unattributed')
            self.assertEqual(residual['mean_us'], 20 - (4000 + 2000 + 6000) / 2 / 1000)
            derived = next(r for r in reads if r['stage'].startswith('exchange minus'))
            self.assertEqual(derived['mean_us'], (20000 - 8000) / 2 / 1000)
            self.assertIn('identity not proven', derived['selection'])
            floors = [{'case_id': 'floor', 'tool': 'ib_send_lat', 'mode': 'polled', 'interface': 'rdma0',
                       'size_bytes': 1024, 'repeat': 0, 'verdict': 'qualified', 'round_trip_p50_us': 3}]
            rows = focused_analysis.stages_transport(focused_analysis.stages_ops(root, cases), writes, reads, floors)
            row = next(r for r in rows if r['case_id'] == 'reader')
            self.assertEqual((row['api_mean_us'], row['service_us'], row['transport_us']), (20, 4, 6))
            self.assertEqual(row['share_of_api_mean'], .3)
            self.assertEqual((row['floor_p50_us'], row['floor_exactness']), (3, 'floor_derived'))
            profile['selected_operations'] = 1
            (native_dir / 'write-diagnostics.json').write_text(json.dumps({'profile': profile}))
            writes, _, _ = focused_analysis.stages_decompositions(root, cases, {})
            self.assertIsNone(next(r for r in writes if r['stage'] == 'unattributed')['mean_us'])
            read_profile['stages']['server_handler']['count'] = 1
            (reader_dir / 'read-diagnostics.json').write_text(json.dumps({'profile': read_profile}))
            _, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            self.assertIsNone(next(r for r in reads if r['stage'].startswith('exchange minus'))['mean_us'])

    def test_hybrid_reader_transport_difference(self):
        job = {'case_id': 'hybrid', 'group': 'G12D', 'case_kind': 'media_read',
               'backend': 'tidehunter_nvme_rdma', 'target_io': 'direct', 'value_size': 1024,
               'concurrency': 1, 'repeat': 0, 'mix': {'read': 100}, 'media': {'expected': 'cold'},
               'durability_tier': 'synchronous', 'diagnostics': {'read_profile_limit': 4}}
        case = {'case_id': 'hybrid', 'job': job, 'status': 'completed', 'focused_evidence': {
            'media_evidence': {'status': 'qualified', 'owner_devices': [{'owner_index': 0, 'major': 8, 'minor': 0}]}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / 'cases' / 'hybrid'
            result = folder / 'result'
            result.mkdir(parents=True)
            (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                                                'measurement,read,success,0,10000000,10000000\n'
                                                'measurement,read,success,0,10000000,10000000\n')
            evidence = {'nvmeof': {'readers': {'r': {'stages': {'submit_to_cq': {
                'delta': {'available': True, 'counters': {'count': 2, 'sum_ns': 8000000}}}}}}}}
            (result / 'measurement-read-evidence.json').write_text(json.dumps(evidence))
            before = {'hosts': [{'diskstats': ''}, {'diskstats': '8 0 sda 10 0 20 30 0 0 0 0 0 0 0'}]}
            after = {'hosts': [{'diskstats': ''}, {'diskstats': '8 0 sda 12 0 36 34 0 0 0 0 0 0 0'}]}
            (folder / 'measurement-before.json').write_text(json.dumps(before))
            (folder / 'measurement-end.json').write_text(json.dumps(after))
            writes, reads, _ = focused_analysis.stages_decompositions(root, [case], {})
            derived = next(r for r in reads if r['stage'].startswith('fabric and stack'))
            expected = (8000000 / 2 / 1000) - ((34 - 30) * 1000 / (12 - 10))
            self.assertEqual(derived['mean_us'], expected)
            rows = focused_analysis.stages_transport(focused_analysis.stages_ops(root, [case]), writes, reads, [])
            self.assertEqual(rows[0]['transport_us'], expected)
            self.assertEqual(rows[0]['transport_exactness'], 'derived_mean_difference')
            self.assertEqual(rows[0]['floor_exactness'], 'not computed')

    def test_real_verbs_mtls_memory_profile_is_separable(self):
        # G12D-r0-c43d911a13fcb571c6dc, trimmed read-diagnostics.json.
        n = 4096
        flight = {'inline': 141819010, 'exchange': 140311916, 'server_handler': 30974066,
                  'record_verify': 2103798, 'request_encode': 1968550, 'response_decode': 1074101}
        leaves = {'index_lookup': 2093809, 'cache_lookup': 368824,
                  'pin_preparation': 514834, 'route_preparation': 219132}
        names = ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture', 'route_preparation',
                 'pin_preparation', 'batch_preparation', 'batch_wait', 'postread_cache_fill', 'result_materialization')
        raw = {name: ({'available': True, 'count': n, 'sum_ns': leaves[name]} if name in leaves else
                      {'available': False, 'count': 0, 'sum_ns': None,
                       'unavailable_reason': 'no observations for this stage'}) for name in names}
        profile = {'selected_operations': n, 'completed_operations': n, 'coordinator_resolution_valid': True,
                   'counters': {'read_profile_errors': 0}, 'coordinator_resolution': {
                       'selected': n, 'errors': 0, 'member_errors': 0, 'stages': raw},
                   'stages': {name: {'available': True, 'count': n, 'sum_ns': total}
                              for name, total in flight.items()}}
        job = {'case_id': 'memory', 'group': 'G12D', 'case_kind': 'media_read',
               'backend': 'tidehunter_verbs', 'value_size': 1024, 'concurrency': 1,
               'repeat': 0, 'mix': {'read': 100}, 'measurement_operations': n,
               'media': {'expected': 'memory'}, 'diagnostics': {'read_profile_limit': n}}
        case = {'case_id': 'memory', 'job': job, 'status': 'completed',
                'focused_evidence': {'media_evidence': {'status': 'qualified',
                    'owner_devices': [{'owner_index': 0, 'major': 8, 'minor': 0}]}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / 'cases' / 'memory'
            result = folder / 'result'
            result.mkdir(parents=True)
            (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n' +
                'measurement,read,success,0,50000,50000\n' * n)
            (result / 'read-diagnostics.json').write_text(json.dumps({'profile': profile}))
            snapshot = {'hosts': [{'diskstats': ''},
                {'diskstats': '8 0 sda 10 0 20 30 0 0 0 0 0 0 0'}]}
            for name in ('measurement-before.json', 'measurement-end.json'):
                (folder / name).write_text(json.dumps(snapshot))
            ops = focused_analysis.stages_ops(root, [case])
            writes, reads, _ = focused_analysis.stages_decompositions(root, [case], {})
            rows = {r['component']: r for r in
                    focused_analysis.stages_component_summary(root, [case], ops, writes, reads, {})}
            self.assertAlmostEqual(rows['owner software']['mean_us'], flight['server_handler'] / n / 1000)
            expected = (flight['exchange'] - flight['server_handler'] - flight['record_verify']
                        - flight['request_encode'] - flight['response_decode']) / n / 1000
            self.assertAlmostEqual(rows['network (wire, both stacks, TLS, dispatch)']['mean_us'], expected)
            self.assertEqual(rows['ssd device']['selection'], 'not applicable (cache hit)')
            self.assertIsNone(rows[focused_analysis.EXCHANGE_UNSEPARATED]['mean_us'])
            self.assertAlmostEqual(sum(r['mean_us'] for r in rows.values() if r['mean_us'] is not None), 50)

    def test_component_compare_owner_read_and_put_timeline(self):
        read = {'case_id': 'read', 'group': 'G12D', 'case_kind': 'media_read', 'backend': 'tidehunter_tcp',
                'value_size': 1024, 'concurrency': 1, 'repeat': 0, 'mix': {'read': 100},
                'measurement_operations': 2, 'media': {'expected': 'cold'}, 'durability_tier': 'synchronous',
                'diagnostics': {'read_profile_limit': 2}}
        write = {'case_id': 'put', 'group': 'G12D', 'case_kind': 'remote_put', 'backend': 'tidehunter_verbs_native',
                 'value_size': 1024, 'concurrency': 1, 'repeat': 0, 'mix': {'put': 100},
                 'measurement_operations': 1, 'durability_tier': 'synchronous',
                 'diagnostics': {'write_timeline_limit': 1}}
        cases = [{'case_id': 'read', 'job': read, 'status': 'completed', 'focused_evidence': {
                    'media_evidence': {'status': 'qualified', 'owner_devices': [{'owner_index': 0, 'major': 8, 'minor': 0}]}}},
                 {'case_id': 'put', 'job': write, 'status': 'completed'}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case in cases:
                (root / 'cases' / case['case_id'] / 'result').mkdir(parents=True)
            rd = root / 'cases' / 'read'
            (rd / 'result' / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                'measurement,read,success,0,20000000,20000000\nmeasurement,read,success,30000000,50000000,20000000\n')
            names = ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture', 'route_preparation',
                     'pin_preparation', 'batch_preparation', 'batch_wait', 'postread_cache_fill', 'result_materialization')
            totals = {'index_lookup': 2000, 'cache_lookup': 1000}
            res_stages = {name: ({'available': True, 'count': 2, 'sum_ns': totals[name]} if name in totals else
                          {'available': False, 'count': 0, 'sum_ns': None,
                           'unavailable_reason': 'no observations for this stage'}) for name in names}
            flight = {'inline': 20000000, 'exchange': 12000000, 'server_handler': 8000000,
                      'request_encode': 2000, 'response_decode': 2000, 'record_verify': 2000}
            profile = {'selected_operations': 2, 'completed_operations': 2, 'coordinator_resolution_valid': True,
                       'counters': {'read_profile_errors': 0}, 'coordinator_resolution': {
                           'selected': 2, 'errors': 0, 'member_errors': 0, 'stages': res_stages},
                       'stages': {name: {'available': True, 'count': 2, 'sum_ns': total} for name, total in flight.items()}}
            (rd / 'result' / 'read-diagnostics.json').write_text(json.dumps({'profile': profile}))
            (rd / 'measurement-before.json').write_text(json.dumps({'hosts': [
                {'diskstats': ''}, {'diskstats': '8 0 sda 10 0 20 30 0 0 0 0 0 0 0'}]}))
            (rd / 'measurement-end.json').write_text(json.dumps({'hosts': [
                {'diskstats': ''}, {'diskstats': '8 0 sda 12 0 36 34 0 0 0 0 0 0 0'}]}))
            put = root / 'cases' / 'put' / 'result'
            (put / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns,caller_sample_id\n'
                                             'measurement,put,success,0,12000,12000,7\n')
            events = [(name, value) for name, value in (
                ('caller_start', 0), ('owner_enqueue_started', 1000), ('owner_dequeued', 2000),
                ('admitted', 3000), ('tx_queued', 4000), ('completion_harvested', 10000),
                ('published', 11000), ('caller_finished', 12000))]
            record = {'caller_sample_id': 7, 'completed': True, 'events': [
                {'stage': name, 'elapsed_ns': value, **({'owner': 0, 'envelope_id': 5}
                 if name in ('tx_queued', 'completion_harvested') else {})} for name, value in events]}
            (put / 'write-timeline.json').write_text(json.dumps({'timeline': {'records': [record], 'owner_records': [
                {'owner': 0, 'envelope_id': 5, 'batch_count': 1, 'handler_ns': 4000, 'append_persist_ns': 2000}]}}))
            ops = focused_analysis.stages_ops(root, cases)
            writes, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            components = focused_analysis.stages_component_summary(root, cases, ops, writes, reads, {})
            compare = focused_analysis.stages_component_compare(components)
            def cell(action, component, route):
                return next(row[route] for row in compare if row['action'] == action and row['component'] == component)
            # Recompute from raw timestamp and counter arithmetic, not the production helper.
            self.assertEqual(cell('put', 'client coordinator', 'owner verbs')['mean_us'], (1000 + 1000 + 1000) / 1000)
            self.assertEqual(cell('put', 'network (wire, both stacks, TLS, dispatch)', 'owner verbs')['mean_us'], (10000 - 4000 - 4000) / 1000)
            self.assertEqual(cell('put', focused_analysis.OWNER_PERSIST, 'owner verbs')['mean_us'], 2)
            self.assertEqual(cell('read', 'owner software', 'owner TCP')['mean_us'], 8000000 / 2 / 1000 - 4000 / 2)
            self.assertEqual(cell('read', 'ssd device', 'owner TCP')['mean_us'], 4000 / 2)
            self.assertAlmostEqual(cell('read', 'residual', 'owner TCP')['mean_us'], 20000 - 3000 / 2 / 1000 - 20000000 / 2 / 1000)
            self.assertEqual(cell('read', 'client queueing and wake-up', 'owner TCP')['mean_us'],
                             (20000000 - 12000000) / 2 / 1000)
            self.assertEqual(cell('read', 'client messaging (encode, decode, verify)', 'owner TCP')['mean_us'],
                             (2000 + 2000 + 2000) / 2 / 1000)
            self.assertEqual(cell('read', 'network (wire, both stacks, TLS, dispatch)', 'owner TCP')['mean_us'],
                             (12000000 - 8000000 - 2000 - 2000 - 2000) / 2 / 1000)
            measured = [r['mean_us'] for r in components if r['case_id'] == 'read' and r['mean_us'] is not None]
            self.assertAlmostEqual(sum(measured), 20000)
            profile['stages']['request_encode']['sum_ns'] = 5000000
            (rd / 'result' / 'read-diagnostics.json').write_text(json.dumps({'profile': profile}))
            writes, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            changed = focused_analysis.stages_component_summary(root, cases, ops, writes, reads, {})
            negative = next(r for r in changed if r['case_id'] == 'read'
                            and r['component'] == 'network (wire, both stacks, TLS, dispatch)')
            self.assertIsNone(negative['mean_us'])
            self.assertIn('negative difference', negative['selection'])
            profile['stages']['request_encode']['count'] = 1
            (rd / 'result' / 'read-diagnostics.json').write_text(json.dumps({'profile': profile}))
            writes, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            changed = focused_analysis.stages_component_summary(root, cases, ops, writes, reads, {})
            self.assertIsNone(next(r for r in changed if r['case_id'] == 'read'
                                   and r['component'] == 'network (wire, both stacks, TLS, dispatch)')['mean_us'])
            read_rows = {r['component']: r for r in changed if r['case_id'] == 'read'}
            self.assertEqual(read_rows[focused_analysis.EXCHANGE_UNSEPARATED]['mean_us'], 10000)
            self.assertIn('whole inline', read_rows[focused_analysis.EXCHANGE_UNSEPARATED]['selection'])
            self.assertAlmostEqual(sum(r['mean_us'] for r in read_rows.values() if r['mean_us'] is not None), 20000)
            trace = json.loads((put / 'write-timeline.json').read_text())
            trace['timeline']['owner_records'][0]['batch_count'] = 2
            (put / 'write-timeline.json').write_text(json.dumps(trace))
            writes, reads, _ = focused_analysis.stages_decompositions(root, cases, {})
            changed = focused_analysis.stages_component_summary(root, cases, ops, writes, reads, {})
            put_rows = {r['component']: r for r in changed if r['case_id'] == 'put'}
            self.assertEqual(put_rows[focused_analysis.EXCHANGE_UNSEPARATED]['mean_us'], 6)
            self.assertIsNone(put_rows[focused_analysis.OWNER_PERSIST]['mean_us'])
            self.assertIsNone(put_rows['network (wire, both stacks, TLS, dispatch)']['mean_us'])
            self.assertAlmostEqual(sum(r['mean_us'] for r in put_rows.values() if r['mean_us'] is not None), 12)
            self.assertIn('Largest components', '\n'.join(focused_analysis.stages_markdown('component_compare', compare, {})))

    def test_write_floor_raw_log_and_size_labeled_excess(self):
        floor_jobs = [{'case_id': f'floor-{repeat}', 'group': 'G11W', 'case_kind': 'ssd_write_floor',
                       'backend': 'tidehunter_verbs_native', 'repeat': repeat,
                       'ssd_write': {'mode': 'fdatasync', 'block_bytes': 8192, 'region_bytes': 536870912}}
                      for repeat in (0, 1)]
        cases = []
        raw_rows = [(1000, 3000, 4000, 0), (2000, 4000, 6000, 8192),
                    (1000, 5000, 6000, 16384), (3000, 5000, 8000, 24576)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export, report, out = root / 'export', root / 'report', root / 'out'
            export.mkdir()
            report.mkdir()
            for job in floor_jobs:
                directory = export / 'cases' / job['case_id']
                directory.mkdir(parents=True)
                log = directory / 'ssd_write_floor_lat.log'
                log.write_text(''.join(','.join(map(str, row)) + '\n' for row in raw_rows))
                raw = [list(map(int, line.split(','))) for line in log.read_text().splitlines()]
                values = sorted(row[2] for row in raw)
                total = {'n': len(values), 'mean': sum(values) / len(values),
                         'p50': values[(len(values) + 1) // 2 - 1], 'p99': values[-1], 'p999': values[-1]}
                evidence = {'lat_log': {'path': log.name}, 'logged_operations': len(values),
                            'total_operations': len(values), 'dropped_operations': 0, 'sampling': 'first_rows',
                            'summary_population': 'first N logged operations', 'write': {}, 'sync': {}, 'total': total}
                cases.append({'case_id': job['case_id'], 'job': job, 'status': 'completed',
                              'focused_evidence': {'ssd_write_floor_result': evidence}})
            failed = {**floor_jobs[0], 'case_id': 'failed', 'repeat': 2}
            floor_jobs.append(failed)
            cases.append({'case_id': 'failed', 'job': failed, 'status': 'failed'})
            (export / 'matrix.json').write_text(json.dumps({'jobs': floor_jobs}))
            (report / 'comparison.json').write_text(json.dumps({'cases': cases}))
            tables = focused_analysis.generate(export, report, out)['tables']
            row = next(r for r in tables['ssd_write_floor'] if r['case_id'] == 'floor-0')
            self.assertEqual(row['p50_us'], values[1] / 1000)
            self.assertEqual(row['mean_us'], sum(values) / len(values) / 1000)
            self.assertEqual(row['n'], len(values))
            self.assertEqual(row['p50_range_us']['min'], values[1] / 1000)
            self.assertTrue((out / 'ssd_write_floor.csv').is_file())
            missing = next(r for r in tables['ssd_write_floor'] if r['case_id'] == 'failed')
            self.assertEqual(missing['verdict'], 'missing')
            self.assertIsNone(missing['p50_us'])
            component = {'component': focused_analysis.OWNER_PERSIST, 'route': 'owner verbs', 'shape': 'put',
                         'size_bytes': 1024, 'repeat': 0, 'durability': 'synchronous'}
            match = focused_analysis._component_floors(component, tables)
            self.assertEqual((match[0]['size_bytes'], match[0]['p50_us']), (8192, values[1] / 1000))
            self.assertEqual(match[0]['exactness'], 'floor_derived')

    def test_hybrid_reader_components_do_not_double_count_submit(self):
        job = {'case_id': 'h', 'group': 'G12D', 'case_kind': 'media_read',
               'backend': 'tidehunter_nvme_rdma', 'target_io': 'direct',
               'value_size': 1024, 'concurrency': 1, 'measurement_operations': 2,
               'repeat': 0, 'mix': {'read': 100}, 'media': {'expected': 'cold'},
               'durability_tier': 'synchronous', 'diagnostics': {'read_profile_limit': 2}}
        case = {'case_id': 'h', 'job': job, 'status': 'completed',
                'focused_evidence': {'media_evidence': {'status': 'qualified'}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / 'cases' / 'h' / 'result'
            result.mkdir(parents=True)
            (result / 'samples.csv').write_text('phase,operation,outcome,dispatched_ns,completed_ns,api_ns\n'
                'measurement,read,success,0,40000,40000\nmeasurement,read,success,50000,90000,40000\n')
            names = ('index_lookup', 'cache_lookup', 'cut_validation', 'cut_capture', 'route_preparation',
                     'pin_preparation', 'batch_preparation', 'batch_wait', 'postread_cache_fill', 'result_materialization')
            raw = {name: {'count': 0, 'sum_ns': None, 'unavailable_reason': 'no observations for this stage'}
                   for name in names}
            raw['index_lookup'] = {'count': 2, 'sum_ns': 2000, 'available': True}
            raw['batch_wait'] = {'count': 2, 'sum_ns': 4000, 'available': True}
            profile = {'selected_operations': 2, 'completed_operations': 2, 'coordinator_resolution_valid': True,
                       'counters': {'read_profile_errors': 0}, 'coordinator_resolution': {
                           'selected': 2, 'errors': 0, 'member_errors': 0, 'stages': raw}}
            (result / 'read-diagnostics.json').write_text(json.dumps({'profile': profile}))
            profiled = [{'case_id': 'h', 'stage': name, 'count': data['count'],
                         'mean_us': data['sum_ns'] / data['count'] / 1000 if data['count'] else None,
                         'exactness': 'sampled_mean'} for name, data in raw.items()]
            profiled += [{'case_id': 'h', 'stage': name, 'count': 2, 'mean_us': value,
                          'exactness': 'sampled_mean'} for name, value in (
                              ('admission', 1), ('alignment', 2), ('queue', 3),
                              ('submit_to_cq', 10), ('extraction', 4), ('verification', 5))]
            profiled += [{'case_id': 'h', 'stage': 'owner_block_layer', 'count': 2, 'mean_us': 4,
                          'exactness': 'device_counter'},
                         {'case_id': 'h', 'stage': 'client other', 'count': 2, 'mean_us': 12,
                          'exactness': 'derived_mean_difference', 'selection': 'full window'}]
            ops = focused_analysis.stages_ops(root, [case])
            components = focused_analysis.stages_component_summary(root, [case], ops, [], profiled, {})
            by_name = {row['component']: row for row in components}
            self.assertEqual(by_name['client queueing and wake-up']['mean_us'], 1 + 2 + 3)
            self.assertEqual(by_name['client messaging (encode, decode, verify)']['mean_us'], 4 + 5)
            self.assertEqual(by_name['network (wire, both stacks, TLS, dispatch)']['mean_us'], 10 - 4)
            self.assertEqual(by_name['ssd device']['mean_us'], 4)
            self.assertEqual(sum(row['mean_us'] for row in components if row['mean_us'] is not None), 40)
            # The real direct case has more block reads than API reads.
            block = next(r for r in profiled if r['stage'] == 'owner_block_layer')
            block.update(count=3, mean_us=4)
            components = focused_analysis.stages_component_summary(root, [case], ops, [], profiled, {})
            by_name = {row['component']: row for row in components}
            self.assertIn('non-API reads', by_name['ssd device']['selection'])
            self.assertEqual(by_name[focused_analysis.DEVICE_UNSEPARATED]['mean_us'], 10)
            self.assertEqual(sum(row['mean_us'] for row in components if row['mean_us'] is not None), 40)
            # A buffered memory hit with zero device operations retains the whole submit interval.
            block.update(count=0, mean_us=None)
            components = focused_analysis.stages_component_summary(root, [case], ops, [], profiled, {})
            by_name = {row['component']: row for row in components}
            self.assertEqual(by_name['network (wire, both stacks, TLS, dispatch)']['mean_us'], 10)
            self.assertEqual(sum(row['mean_us'] for row in components if row['mean_us'] is not None), 40)

    def test_component_compare_keeps_one_case_per_route(self):
        base = {'route': 'owner verbs', 'shape': 'put', 'size_bytes': 1024, 'concurrency': 1,
                'durability': 'synchronous', 'cache_state': None, 'key_policy': 'overwrite',
                'group': 'G12D', 'diagnostics': {'write_timeline_limit': 4096},
                'exactness': 'sampled_mean', 'share_of_api_mean': .1, 'source_stages': [],
                'selection': 'synthetic', 'floor': [], 'excess_over_floor_us': []}
        rows = [{**base, 'case_id': 'timeline', 'repeat': 0, 'component': 'client coordinator', 'mean_us': 3},
                {**base, 'case_id': 'timeline', 'repeat': 0, 'component': 'waiting and coalescing', 'mean_us': 2},
                {**base, 'case_id': 'profile', 'repeat': 1, 'component': 'owner software', 'mean_us': 100}]
        compared = focused_analysis.stages_component_compare(rows)
        by_component = {row['component']: row for row in compared}
        self.assertEqual(by_component['client coordinator']['owner verbs']['case_id'], 'timeline')
        self.assertEqual(by_component['owner software']['owner verbs'], 'not measured')
        self.assertEqual(by_component['largest components']['owner verbs']['case_id'], 'timeline')
        self.assertEqual(by_component['largest components']['owner verbs']['component'], 'client coordinator')


if __name__ == '__main__':
    unittest.main()
