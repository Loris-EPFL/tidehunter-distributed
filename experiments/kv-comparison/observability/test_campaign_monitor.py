"""Lifecycle boundaries only; integration must separately exercise real Docker/APIs."""
import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import socket
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

import campaign_monitor as monitor


class CampaignMonitorBoundaries(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='kv-monitor-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.results = self.root / 'results'
        self.results.mkdir()
        self.spec = {'schema_version': 1, 'run_id': 'th-unit-boundary',
                     'state_dir': str(self.root / 'session'), 'results_root': str(self.results),
                     'deadline_utc': datetime.fromtimestamp(time.time() + 300, timezone.utc).isoformat(),
                     'hosts': [{'index': -1, 'role': 'coordinator', 'address': '127.0.0.1'}],
                     'ports': {'prometheus': 19090, 'grafana': 13000, 'benchmark': 19899, 'node': 19100, 'host': 19898},
                     'allow_loopback_hosts': True}
        self.job = {'case_id': 'case-01', 'backend': 'native', 'profile': 'read',
                    'cohort': 'warm', 'value_size': 64, 'repeat': 0}

    def initialize_selection(self):
        state, owner = monitor.state_for(self.spec, create=True)
        (state / 'benchmark').mkdir()
        (state / 'host--1').mkdir()
        monitor.atomic_json(state / 'benchmark/selection.json', {'job': None, 'results': None, 'terminal': None})
        monitor.atomic_json(state / 'session.json', {'started_timestamp_seconds': time.time() - 10,
                            'expected_case_ids': [], 'selected_case_ids': [], 'seen_case_ids': [], 'terminal_observations': []})
        case = self.results / 'cases' / self.job['case_id']
        case.mkdir(parents=True)
        return state, owner, case / 'result'

    def test_spec_requires_exact_cloud_fleet_and_rejects_public_targets(self):
        spec = copy.deepcopy(self.spec)
        spec['allow_loopback_hosts'] = False
        spec['state_dir'] = '/var/lib/tidehunter-observation/' + spec['run_id']
        with self.assertRaises(ValueError):
            monitor.validate_spec(spec)
        spec['hosts'] = [{'index': index, 'role': 'coordinator' if index == -1 else f'storage-{index}',
                          'address': f'10.7.0.{index + 3}'} for index in range(-1, 5)]
        self.assertEqual(monitor.validate_spec(spec), spec)
        spec['hosts'][0]['address'] = '8.8.8.8'
        with self.assertRaises(ValueError):
            monitor.validate_spec(spec)

    def test_spec_does_not_require_remote_controller_results_to_exist(self):
        self.spec['results_root'] = str(self.root / 'absent-controller-results')
        self.assertEqual(monitor.validate_spec(self.spec), self.spec)

    def test_spec_version_must_not_accept_boolean_or_unknown_fields(self):
        for changes in ({'schema_version': True}, {'schema_version': '1'}, {'extra': 'unadmitted'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                monitor.validate_spec(self.spec | changes)
        with self.assertRaises(ValueError):
            monitor.validate_spec(list(self.spec.items()))

    def test_six_loopback_hosts_are_explicit_logical_targets_not_a_cloud_fleet(self):
        spec = copy.deepcopy(self.spec)
        spec['hosts'] = [{'index': index, 'role': 'coordinator' if index == -1 else f'storage-{index}',
                          'address': f'127.0.0.{index + 2}'} for index in range(-1, 5)]
        self.assertEqual(len(monitor.validate_spec(spec)['hosts']), 6)
        spec['hosts'][2]['address'] = spec['hosts'][1]['address']
        with self.assertRaises(ValueError):
            monitor.validate_spec(spec)

    def test_private_state_is_bound_to_entire_spec(self):
        state, owner = monitor.state_for(self.spec, create=True)
        self.assertEqual(state.stat().st_mode & 0o777, 0o700)
        changed = copy.deepcopy(self.spec)
        changed['ports']['node'] += 1
        with self.assertRaises(ValueError):
            monitor.state_for(changed)
        state.chmod(0o755)
        with self.assertRaises(ValueError):
            monitor.state_for(self.spec)
        self.assertEqual(owner['run_id'], self.spec['run_id'])

    def test_existing_unowned_state_is_never_adopted(self):
        state = Path(self.spec['state_dir'])
        state.mkdir()
        with self.assertRaises(ValueError):
            monitor.state_for(self.spec, create=True)

    def test_state_cannot_use_source_results_or_symlink(self):
        for state in (str(self.results / 'mutable'), str(monitor.ROOT / 'mutable')):
            with self.subTest(state=state):
                spec = self.spec | {'state_dir': state}
                with self.assertRaises(ValueError):
                    monitor.validate_spec(spec)
        (self.root / 'link').symlink_to(self.results, target_is_directory=True)
        with self.assertRaises(ValueError):
            monitor.validate_spec(self.spec | {'state_dir': str(self.root / 'link' / 'mutable')})

    def test_select_cannot_cross_case_or_overwrite_unobserved_terminal(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        other = self.job | {'case_id': 'case-02'}
        (self.results / 'cases/case-02').mkdir()
        with self.assertRaises(ValueError):
            monitor.select_case(self.spec, other, result)
        with self.assertRaises(ValueError):
            monitor.select_case(self.spec, other, self.results / 'cases/case-02/result')
        self.assertEqual(monitor.load_json(state / 'benchmark/selection.json')['job'], self.job)

    def test_explicit_host_selection_rejects_scans_and_duplicate_roles(self):
        state, _, _ = self.initialize_selection()
        monitor.select_host(self.spec, -1, self.job, [])
        self.assertEqual(monitor.load_json(state / 'host--1/selection.json')['processes'], [])
        for cgroup in ('/', '/proc', '/system.slice/../proc', '/system.slice', '/user.slice/a'):
            with self.subTest(cgroup=cgroup), self.assertRaises(ValueError):
                monitor.select_host(self.spec, -1, self.job, [{'role': 'worker', 'cgroup': cgroup}])
        process = {'role': 'worker', 'cgroup': '/system.slice/exact-owned.service'}
        with self.assertRaises(ValueError):
            monitor.select_host(self.spec, -1, self.job, [process, process])
        answer = monitor.select_host(self.spec, -1, self.job, [process])
        self.assertTrue(answer['complete'])

    def test_terminal_barrier_requires_fresh_scrape_and_explicit_status(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        expressions = []
        resource = {'schema_version': 1, 'scope': 'measurement_boundary_interval', 'processes': [
            {'host_index': -1, 'process_role': 'client', 'available': False, 'reason': 'counter reset'}]}
        def observed(spec, expression, **kwargs):
            expressions.append(expression)
            return {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        with patch.object(monitor, 'query', side_effect=observed):
            answer = monitor.finish_case(self.spec, self.job, result, status='failed',
                                         write_profile_enabled=False, read_evidence={'owner_io': {'available': False}},
                                         resource_evidence=resource)
        self.assertTrue(answer['complete'])
        self.assertIn('kv_observer_case_terminal', expressions[0])
        self.assertIn('status="failed"', expressions[0])
        self.assertIn('timestamp(kv_observer_case_terminal', expressions[0])
        terminal = monitor.load_json(state / 'benchmark/selection.json')['terminal']
        self.assertEqual(terminal['write_profile_enabled'], False)
        self.assertEqual(terminal['read_evidence'], {'owner_io': {'available': False}})
        self.assertEqual(terminal['resource_evidence'], resource)
        self.assertIsNone(terminal['metrics'])
        self.assertIn(self.job['case_id'], monitor.load_json(state / 'session.json')['seen_case_ids'])

    def test_malformed_terminal_metrics_cannot_poison_active_selection(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        selection = (state / 'benchmark/selection.json').read_bytes()
        session = (state / 'session.json').read_bytes()
        for metrics in ({'measurement_rows_complete': False},
                        {'measurement_rows_complete': True, 'offered_calls': 4,
                         'success_calls': 5, 'populations': []}):
            with self.subTest(metrics=metrics), self.assertRaises(ValueError):
                monitor.finish_case(self.spec, self.job, result, metrics=metrics)
            self.assertEqual((state / 'benchmark/selection.json').read_bytes(), selection)
            self.assertEqual((state / 'session.json').read_bytes(), session)

    def test_terminal_read_diagnostics_validate_before_publication(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        evidence = {'schema_version': 1, 'backend': 'tidehunter_tcp', 'phase': 'measurement',
                    'phase_attempted': True, 'runtime_enabled': True,
                    'runtime_observation_mode': 'bounded_remote_read_requests',
                    'selection': {'requested_limit': 2, 'selected_operations': 2, 'completed_operations': 1,
                                  'selection_policy': 'first measurement reads',
                                  'phase_exclusions': ['warmup', 'preload']},
                    'clock_scope': 'local accumulated clocks', 'overlap_semantics': 'overlapping totals',
                    'unsupported_stages': [], 'error': None,
                    'profile': {'stages': {'server_async': {'available': True, 'sum_ns': 500,
                                'count': 1, 'max_ns': None, 'unavailable_reason': None}}, 'counters': {}}}
        before = (state / 'benchmark/selection.json').read_bytes()
        with self.assertRaises(ValueError):
            monitor.finish_case(self.spec, self.job, result, read_diagnostics=evidence | {'phase': 'preload'})
        self.assertEqual((state / 'benchmark/selection.json').read_bytes(), before)
        observed = {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        with patch.object(monitor, 'query', return_value=observed):
            answer = monitor.finish_case(self.spec, self.job, result, read_diagnostics=evidence,
                                         read_profile_enabled=True)
        self.assertTrue(answer['complete'])
        terminal = monitor.load_json(state / 'benchmark/selection.json')['terminal']
        self.assertEqual(terminal['read_diagnostics'], evidence)
        self.assertIs(terminal['read_profile_enabled'], True)

    def test_rich_terminal_selection_is_admitted_and_oversize_is_atomic(self):
        import campaign_exporter
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        stats = {f"resident_read_counter_{index}": 123456789012345 for index in range(32)}
        lane_group = {"available": True, "partitions": {
            str(owner): {"lanes": {str(lane): {"session_present": True, "stats": stats}
                                   for lane in range(8)}} for owner in range(5)}}
        snapshot = {"available": True, "client_resident_lanes": lane_group}
        boundary = {"scope": "measurement_boundary", "start": snapshot, "end": snapshot,
                    "deltas": {"available": False, "error": "not observed"}}
        diagnostics = {'schema_version': 1, 'backend': 'tidehunter_tcp', 'phase': 'measurement',
                       'phase_attempted': True, 'runtime_enabled': True,
                       'runtime_observation_mode': 'bounded_remote_read_requests',
                       'selection': {'requested_limit': 2, 'selected_operations': 0, 'completed_operations': 0,
                                     'selection_policy': 'first remote flights', 'phase_exclusions': []},
                       'clock_scope': 'local clocks', 'overlap_semantics': 'not additive',
                       'unsupported_stages': [], 'error': None,
                       'profile': {'stages': {}, 'counters': {}, 'read_evidence': boundary}}
        before = (state / 'benchmark/selection.json').read_bytes()
        oversized = boundary | {"extra": "x" * campaign_exporter.MAX_SELECTION}
        with self.assertRaises(ValueError):
            monitor.finish_case(self.spec, self.job, result, measurement_read_evidence=oversized,
                                read_diagnostics=diagnostics)
        self.assertEqual((state / 'benchmark/selection.json').read_bytes(), before)
        observed = {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        with patch.object(monitor, 'query', return_value=observed):
            answer = monitor.finish_case(self.spec, self.job, result, measurement_read_evidence=boundary,
                                         read_diagnostics=diagnostics)
        self.assertTrue(answer['complete'])
        selection_path = state / 'benchmark/selection.json'
        self.assertGreater(selection_path.stat().st_size, 256 * 1024)
        self.assertLessEqual(selection_path.stat().st_size, campaign_exporter.MAX_SELECTION)
        selection = campaign_exporter.read_json(selection_path)
        before = selection_path.read_bytes()
        with self.assertRaises(ValueError):
            monitor.atomic_json(selection_path, selection | {"extra": "x" * campaign_exporter.MAX_SELECTION},
                                max_bytes=campaign_exporter.MAX_SELECTION)
        self.assertEqual(selection_path.read_bytes(), before)
        oversized_path = self.root / 'oversized-selection.json'
        oversized_path.write_bytes(b' ' * (campaign_exporter.MAX_SELECTION + 1))
        with self.assertRaises(ValueError):
            campaign_exporter.read_json(oversized_path)

    def test_effective_cache_receipt_validated_before_terminal_publication(self):
        requested = {'coordinator_cache_bytes': 0, 'owner_read_cache_bytes': 0,
                     'registered_policy': 'disabled', 'page_cache_policy': 'not_dropped',
                     'warmup_operations': 10, 'read_lanes': 1, 'concurrency': 1}
        self.job['read_tier'] = {'requested': requested, 'effective': None}
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        before = (state / 'benchmark/selection.json').read_bytes()
        with self.assertRaises(ValueError):
            monitor.finish_case(self.spec, self.job, result,
                                read_tier_effective=requested | {'registered_policy': 'invalid'})
        self.assertEqual((state / 'benchmark/selection.json').read_bytes(), before)
        observed = {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        effective = requested | {'page_cache_policy': None}
        with patch.object(monitor, 'query', return_value=observed):
            monitor.finish_case(self.spec, self.job, result, read_tier_effective=effective)
        selection = monitor.load_json(state / 'benchmark/selection.json')
        self.assertEqual(selection['job'], self.job)
        self.assertEqual(selection['terminal']['read_tier_effective'], effective)

    def test_cold_device_evidence_rejected_before_publication_then_recorded(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        before = (state / 'benchmark/selection.json').read_bytes()
        with self.assertRaises(ValueError):
            monitor.finish_case(self.spec, self.job, result, cold_device_evidence=[
                {'host_index': 0, 'evidence': {'available': False, 'raw_events': []}}])
        self.assertEqual((state / 'benchmark/selection.json').read_bytes(), before)
        evidence = [{'host_index': 0, 'evidence': {'available': False, 'reason': 'unavailable'}}]
        observed = {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        with patch.object(monitor, 'query', return_value=observed):
            monitor.finish_case(self.spec, self.job, result, cold_device_evidence=evidence)
        terminal = monitor.load_json(state / 'benchmark/selection.json')['terminal']
        self.assertEqual(terminal['cold_device_evidence'], evidence)

    def test_terminal_write_diagnostics_are_validated_before_atomic_publication(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        evidence = {'schema_version': 1, 'backend': 'native', 'phase': 'measurement',
                    'phase_attempted': True, 'runtime_enabled': True, 'runtime_observation_mode': 'sampled_prefix',
                    'selection': {'requested_limit': 2, 'observed_samples': 2,
                                  'selection_policy': 'first N accepted scalar measurement writes',
                                  'phase_exclusions': ['warmup', 'preload']},
                    'clock_scope': 'local accumulated clocks', 'overlap_semantics': 'overlapping totals',
                    'unsupported_stages': [], 'error': None,
                    'profile': {'stages': {'wal_append_ns': 500, 'wal_append_errors': 0}}}
        before = (state / 'benchmark/selection.json').read_bytes()
        with self.assertRaises(ValueError):
            monitor.finish_case(self.spec, self.job, result, write_diagnostics=evidence | {'phase': 'preload'})
        self.assertEqual((state / 'benchmark/selection.json').read_bytes(), before)
        observed = {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}
        with patch.object(monitor, 'query', return_value=observed):
            answer = monitor.finish_case(self.spec, self.job, result, write_diagnostics=evidence)
        self.assertTrue(answer['complete'])
        terminal = monitor.load_json(state / 'benchmark/selection.json')['terminal']
        self.assertEqual(terminal['write_diagnostics'], evidence)

    def test_unavailable_terminal_does_not_claim_observed_or_release_next_case(self):
        state, _, result = self.initialize_selection()
        monitor.select_case(self.spec, self.job, result)
        with patch.object(monitor, 'query', side_effect=OSError('not listening')):
            answer = monitor.finish_case(self.spec, self.job, result, status='failed', timeout_seconds=0.03)
        self.assertFalse(answer['complete'])
        self.assertFalse(answer['final_observed'])
        self.assertTrue(answer['gaps'])
        self.assertEqual(monitor.load_json(state / 'session.json')['seen_case_ids'], [])

    def test_offline_config_has_no_targets_and_retains_recorded_time(self):
        state, _ = monitor.state_for(self.spec, create=True)
        dashboard = {'uid': monitor.DASHBOARD, 'panels': [], 'refresh': '5s'}
        config = monitor.configure_collector(self.spec, state, offline=True, dashboard=dashboard,
                                             time_range={'start': 1600000000, 'end': 1600000030})
        self.assertEqual(config['scrape_configs'], [])
        copied = monitor.load_json(state / 'config/dashboards/campaign.json')
        self.assertEqual(copied['refresh'], '')
        self.assertEqual(datetime.fromisoformat(copied['time']['from']).timestamp(), 1600000000)
        self.assertEqual(datetime.fromisoformat(copied['time']['to']).timestamp(), 1600000030)

    def test_captured_query_hash_binds_samples_and_timestamps_not_result_order(self):
        first = {'resultType': 'matrix', 'result': [
            {'metric': {'case_id': 'b'}, 'values': [[1600000001, '1']]},
            {'metric': {'case_id': 'a'}, 'values': [[1600000002, '2']]}]}
        second = copy.deepcopy(first)
        second['result'].reverse()
        self.assertEqual(monitor.query_digest(first), monitor.query_digest(second))
        second['result'][0]['values'][0][0] += 1
        self.assertNotEqual(monitor.query_digest(first), monitor.query_digest(second))

    def test_full_matrix_pages_retain_samples_without_monolithic_json(self):
        state, _ = monitor.state_for(self.spec, create=True)
        window = {'start': 1600000000, 'end': 1600000300}
        calls = []
        def observed(spec, expression, timestamp=None):
            calls.append((expression, timestamp))
            return {'resultType': 'matrix', 'result': [
                {'metric': {'case_id': 'case-' + str(index), 'identity': 'x' * 600,
                            'source_metric': expression.split('{', 1)[0]},
                 'values': [[1600000001 + offset, '1'] for offset in range(60)]}
                for index in range(684)]}
        with patch.object(monitor, 'query', side_effect=observed):
            captured, seen = monitor.collect_query_evidence(self.spec, state, window)
            replayed, replay_seen = monitor.collect_query_evidence(self.spec, state, window, captured)
        self.assertEqual(captured, replayed)
        self.assertEqual(seen, replay_seen)
        self.assertEqual(len(seen), 684)
        self.assertGreater(sum(page['bytes'] for page in captured['pages']), monitor.MAX_JSON)
        self.assertTrue(all(page['bytes'] <= monitor.MAX_JSON for page in captured['pages']))
        self.assertTrue(all('[' in expression and timestamp == window['end'] for expression, timestamp in calls))
        monitor.atomic_json(state / 'bounded-receipt.json', {'query_evidence': captured})
        for page in captured['pages']:
            payload = monitor.load_json(state / 'query-evidence' / page['file'])
            self.assertEqual(payload['result'][0]['values'], [[1600000001 + offset, '1'] for offset in range(60)])
            self.assertEqual(monitor.query_digest(payload), page['query_sha256'])

    def test_query_pages_split_oversized_response_and_replay_exact_boundaries(self):
        state, _ = monitor.state_for(self.spec, create=True)
        window = {'start': 1600000000, 'end': 1600000301}
        calls = []
        def observed(spec, expression, timestamp=None):
            duration = float(expression.rsplit('[', 1)[1][:-2])
            calls.append((duration, timestamp))
            if duration > 150:
                raise monitor.ResponseTooLarge('test response bound')
            return {'resultType': 'matrix', 'result': [
                {'metric': {'case_id': 'last'}, 'values': [[timestamp, '1']]}]}
        with patch.object(monitor, 'query', side_effect=observed):
            captured, seen = monitor.collect_query_evidence(self.spec, state, window)
            calls.clear()
            replayed, _ = monitor.collect_query_evidence(self.spec, state, window, captured)
        self.assertEqual(captured, replayed)
        self.assertEqual(seen, {'last'})
        self.assertTrue(all(duration <= 150 for duration, _ in calls))
        for change in ('missing-page', 'overlap', 'wrong-hash', 'oversized-file'):
            broken = copy.deepcopy(captured)
            if change == 'missing-page':
                broken['pages'].pop()
            elif change == 'overlap':
                broken['pages'][1]['start'] -= 1
            elif change == 'wrong-hash':
                broken['pages'][0]['sha256'] = 'not-a-hash'
            else:
                broken['pages'][0]['bytes'] = monitor.MAX_JSON + 1
            with self.subTest(change=change), self.assertRaises(ValueError):
                monitor.validate_query_evidence(broken, window)

    def test_query_page_preserves_one_sample_and_detects_changed_historical_value(self):
        state, _ = monitor.state_for(self.spec, create=True)
        window = {'start': 1600000000, 'end': 1600000001}
        one = {'resultType': 'matrix', 'result': [
            {'metric': {'case_id': 'short'}, 'values': [[1600000001, '1']]}]}
        with patch.object(monitor, 'query', return_value=one):
            captured, seen = monitor.collect_query_evidence(self.spec, state, window)
        changed = copy.deepcopy(one)
        changed['result'][0]['values'][0][1] = '0'
        with patch.object(monitor, 'query', return_value=changed):
            replayed, replay_seen = monitor.collect_query_evidence(self.spec, state, window, captured)
        self.assertNotEqual(captured, replayed)
        self.assertEqual(seen, {'short'})
        self.assertEqual(replay_seen, set())
        with patch.object(monitor, 'query', side_effect=monitor.ResponseTooLarge('single-second too large')):
            with self.assertRaises(monitor.ResponseTooLarge):
                monitor.collect_query_evidence(self.spec, state, window)

    def test_warned_query_cannot_silently_accept_truncated_evidence(self):
        with patch.object(monitor, 'http', return_value={'status': 'success', 'warnings': ['truncated'],
                                                        'data': {'resultType': 'matrix', 'result': []}}):
            with self.assertRaisesRegex(ValueError, 'warned'):
                monitor.query(self.spec, 'metric[1s]', timestamp=1600000001)

    def test_capture_pages_are_manifest_bound(self):
        state, _, _ = self.initialize_selection()
        window = {'start': 1600000000, 'end': 1600000300}
        with patch.object(monitor, 'query', return_value={'resultType': 'matrix', 'result': []}):
            pages, _ = monitor.collect_query_evidence(self.spec, state, window)
        snapshot = state / 'prometheus/snapshots/unit-snapshot/block'
        snapshot.mkdir(parents=True)
        monitor.atomic_json(snapshot / 'meta.json', {'minTime': 1600000000000, 'maxTime': 1600000300001})
        verification = {'complete': True, 'gaps': [], 'time_range': window, 'evidence': {'query_evidence': pages}}
        destination = self.root / 'paged.tar.gz'
        with patch.object(monitor, 'verify', return_value=verification), \
                patch.object(monitor, 'http', return_value={'status': 'success', 'data': {'name': 'unit-snapshot'}}):
            exported = monitor.export_session(self.spec, destination)
        self.assertTrue(exported['complete'])
        restored = self.root / 'restored'
        restored.mkdir()
        capture = monitor.unpack_archive(destination, restored)
        self.assertEqual(capture['query_evidence'], pages)
        manifest = monitor.load_json(restored / 'manifest.json')
        for page in pages['pages']:
            entry = manifest['files']['evidence/query-evidence/' + page['file']]
            self.assertEqual(entry['sha256'], page['sha256'])
            self.assertEqual(entry['bytes'], page['bytes'])
        # A self-consistent container manifest cannot override the semantic query
        # index: keep the capture's historical hashes and substitute one raw page.
        page_name = 'evidence/query-evidence/' + pages['pages'][0]['file']
        monitor.atomic_json(restored / page_name, {'resultType': 'matrix', 'result': [
            {'metric': {'case_id': 'substituted'}, 'values': [[1600000001, '1']]}]}, compact=True)
        manifest['files'][page_name] = {'sha256': monitor.digest(restored / page_name),
                                       'bytes': (restored / page_name).stat().st_size}
        monitor.atomic_json(restored / 'manifest.json', manifest)
        forged = self.root / 'substituted.tar.gz'
        with tarfile.open(forged, 'w:gz') as archive:
            for path in monitor.regular_files(restored):
                archive.add(path, arcname=str(path.relative_to(restored)))
        monitor.atomic_json(str(forged) + '.receipt.json', {
            'sha256': monitor.digest(forged), 'manifest_sha256': monitor.digest(restored / 'manifest.json')})
        fresh = self.root / 'reject-substitution'
        fresh.mkdir()
        with self.assertRaisesRegex(ValueError, 'query page does not match'):
            monitor.unpack_archive(forged, fresh)

    def test_verify_preserves_legacy_missing_hash_gap_and_checks_authenticated_grafana(self):
        state, _, _ = self.initialize_selection()
        historical = {'time_range': {'start': 1600000000, 'end': 1600000300},
                      'seen_case_ids': [], 'expected_case_ids': ['missing-case'], 'query_sha256': {},
                      'source_capture': {'gaps': ['historic missing receipt']}}
        monitor.atomic_json(state / 'replay.json', historical)
        requests = []
        def endpoint(spec, service, path, payload=None, authenticated=False, **kwargs):
            requests.append((service, path, authenticated))
            if path == '/api/v1/targets':
                return {'data': {'activeTargets': []}}
            if path.endswith('/health'):
                return {'status': 'OK'}
            if path.startswith('/api/datasources/uid/'):
                return {'type': 'prometheus', 'url': f'http://127.0.0.1:{self.spec["ports"]["prometheus"]}'}
            if path.startswith('/api/dashboards/uid/'):
                return {'dashboard': {'uid': monitor.DASHBOARD, 'panels': [
                    {'id': 1, 'datasource': {'type': 'prometheus', 'uid': monitor.DATASOURCE},
                     'targets': [{'refId': 'A', 'expr': 'up'}]}]}}
            if path == '/api/ds/query':
                return {'results': {'A': {'status': 200, 'frames': []}}}
            self.fail('unexpected endpoint')
        with patch.object(monitor, 'http', side_effect=endpoint), \
                patch.object(monitor, 'query', return_value={'resultType': 'matrix', 'result': []}):
            result = monitor.verify(self.spec)
        self.assertFalse(result['complete'])
        self.assertTrue(any('historic missing receipt' in gap for gap in result['gaps']))
        self.assertTrue(any('historical samples differ' in gap for gap in result['gaps']))
        self.assertTrue(any('missing-case' in gap for gap in result['gaps']))
        self.assertTrue(all(authenticated for service, _, authenticated in requests if service == 'grafana'))
        self.assertEqual(monitor.load_json(state / 'replay.json'), historical)

    def test_interpolation_keeps_one_case_and_original_window(self):
        text = monitor.interpolate('last_over_time(x{run_id="$run_id",case_id="$case_id",outcome="$outcome"}[$__range])',
                                   self.spec, 'case-01', 31.2)
        self.assertIn('case_id="case-01"', text)
        self.assertIn('outcome="success"', text)
        self.assertIn('[32s]', text)
        with self.assertRaises(ValueError):
            monitor.interpolate('$unexpected', self.spec, 'case-01', 30)

    def test_promql_capture_replacements_and_anchors_survive_interpolation(self):
        for replacement in ('$1', '${1}'):
            expression = ('label_replace(x{run_id="$run_id",name=~"value$"}, '
                          '"metric", "' + replacement + '", "__name__", "(.*)")')
            rendered = monitor.interpolate(expression, self.spec, 'case-01', 30)
            self.assertIn('run_id="' + self.spec['run_id'] + '"', rendered)
            self.assertIn('"metric", "' + replacement + '"', rendered)
            self.assertIn('name=~"value$"', rendered)
        with self.assertRaises(ValueError):
            monitor.interpolate('${unexpected}', self.spec, 'case-01', 30)

    def make_archive(self, members):
        archive = self.root / 'capture.tar.gz'
        with tarfile.open(archive, 'w:gz') as output:
            for name, data, kind in members:
                item = tarfile.TarInfo(name)
                item.type = kind
                item.size = len(data) if kind == tarfile.REGTYPE else 0
                item.linkname = '/etc/passwd' if kind == tarfile.SYMTYPE else ''
                output.addfile(item, io.BytesIO(data) if item.size else None)
        monitor.atomic_json(str(archive) + '.receipt.json', {'sha256': monitor.digest(archive), 'manifest_sha256': 'not-reached'})
        target = self.root / 'unpack'
        target.mkdir()
        return archive, target

    def test_archive_rejects_traversal_symlinks_duplicates_and_extra_members(self):
        for member in ('../escape', '/absolute', 'snapshot/../escape', 'unexpected/secret'):
            with self.subTest(member=member), tempfile.TemporaryDirectory(dir=self.root) as temporary:
                old_root = self.root
                self.root = Path(temporary)
                try:
                    archive, target = self.make_archive([(member, b'bad', tarfile.REGTYPE)])
                    with self.assertRaises(ValueError):
                        monitor.unpack_archive(archive, target)
                finally:
                    self.root = old_root
        with tempfile.TemporaryDirectory(dir=self.root) as temporary:
            old_root = self.root
            self.root = Path(temporary)
            try:
                archive, target = self.make_archive([('snapshot/link', b'', tarfile.SYMTYPE)])
                with self.assertRaises(ValueError):
                    monitor.unpack_archive(archive, target)
            finally:
                self.root = old_root
        with tempfile.TemporaryDirectory(dir=self.root) as temporary:
            old_root = self.root
            self.root = Path(temporary)
            try:
                archive, target = self.make_archive([('snapshot/duplicate', b'one', tarfile.REGTYPE),
                                                     ('snapshot/duplicate', b'two', tarfile.REGTYPE)])
                with self.assertRaises(ValueError):
                    monitor.unpack_archive(archive, target)
            finally:
                self.root = old_root

    def test_archive_detects_external_hash_tampering_before_extracting(self):
        archive, target = self.make_archive([('evidence/capture.json', b'{}', tarfile.REGTYPE)])
        with archive.open('ab') as stream:
            stream.write(b'tampering')
        with self.assertRaises(ValueError):
            monitor.unpack_archive(archive, target)
        self.assertEqual(list(target.iterdir()), [])

    def test_regular_file_walk_and_member_byte_caps_reject_unsafe_data(self):
        data = self.root / 'bounded'
        data.mkdir()
        (data / 'one').write_bytes(b'12345')
        with patch.object(monitor, 'MAX_BYTES', 4), self.assertRaises(ValueError):
            monitor.regular_files(data)
        (data / 'link').symlink_to(data / 'one')
        with self.assertRaises(ValueError):
            monitor.regular_files(data)

    def test_stop_refuses_other_owners_and_removes_by_inspected_id(self):
        _, owner = monitor.state_for(self.spec, create=True)
        calls = []
        def docker(spec, *args, **kwargs):
            calls.append(args)
            if args[0] == 'ps':
                return 'exact-id'
            if args[0] == 'inspect':
                labels = {monitor.LABEL: spec['run_id'], monitor.LABEL + '.owner': owner['token']}
                return json.dumps([{'Id': 'exact-id', 'Config': {'Labels': labels}}])
            return ''
        with patch.object(monitor, 'docker', side_effect=docker):
            answer = monitor.stop_collector(self.spec)
        self.assertTrue(answer['complete'])
        self.assertTrue(all(call[2] == 'exact-id' for call in calls if call[0] == 'rm'))
        with patch.object(monitor, 'docker', side_effect=lambda spec, *args, **kwargs:
                          'other-id' if args[0] == 'ps' else json.dumps([{'Id': 'other-id', 'Config': {'Labels': {}}}])):
            answer = monitor.stop_collector(self.spec)
        self.assertFalse(answer['complete'])
        self.assertEqual(answer['removed'], [])

    def test_expired_campaign_still_permits_bounded_exact_cleanup(self):
        expired = self.spec | {'deadline_utc': '2020-01-01T00:00:00+00:00'}
        with self.assertRaises(TimeoutError):
            monitor.budget(expired)
        self.assertEqual(monitor.budget(expired, maximum=2, cleanup=True), 2)
        with monitor.operation_scope(0.001):
            time.sleep(0.002)
            with self.assertRaises(TimeoutError):
                monitor.budget(expired, cleanup=True)

    def test_missing_cached_image_never_attempts_registry_or_run(self):
        state, owner = monitor.state_for(self.spec, create=True)
        with patch.object(monitor, 'docker', side_effect=RuntimeError('cached pin missing')) as docker:
            with self.assertRaises(RuntimeError):
                monitor.run_container(self.spec, state, owner, 'benchmark', monitor.images()['library/python'], [])
        self.assertEqual(docker.call_args.args[1:3], ('image', 'inspect'))
        self.assertEqual(docker.call_count, 1)

    def test_failed_snapshot_exports_gap_evidence_without_secret_or_grafana_database(self):
        state, _, _ = self.initialize_selection()
        (state / 'grafana_admin_password').write_text('never-archive-this-secret')
        (state / 'grafana').mkdir()
        (state / 'grafana/grafana.db').write_bytes(b'private-user-database')
        destination = self.root / 'capture.tar.gz'
        with patch.object(monitor, 'verify', side_effect=ValueError('service unavailable')), \
                patch.object(monitor, 'http', side_effect=OSError('snapshot endpoint unavailable')) as endpoint:
            result = monitor.export_session(self.spec, destination)
        self.assertFalse(result['complete'])
        self.assertFalse(result['snapshot']['complete'])
        self.assertTrue(result['gaps'])
        self.assertIn('skip_head=false', endpoint.call_args.args[2])
        restored = self.root / 'verified-archive'
        restored.mkdir()
        capture = monitor.unpack_archive(destination, restored)
        self.assertFalse(capture['complete'])
        self.assertFalse((restored / 'snapshot').exists())
        self.assertNotIn(b'never-archive-this-secret', b''.join(path.read_bytes() for path in restored.rglob('*') if path.is_file()))
        self.assertFalse(any('grafana.db' in str(path) or 'password' in str(path) for path in restored.rglob('*')))

    def test_export_never_overwrites_existing_archive_or_receipt(self):
        self.initialize_selection()
        destination = self.root / 'capture.tar.gz'
        destination.write_bytes(b'immutable')
        with self.assertRaises(ValueError):
            monitor.export_session(self.spec, destination)
        self.assertEqual(destination.read_bytes(), b'immutable')
        destination.unlink()
        sidecar = Path(str(destination) + '.receipt.json')
        sidecar.write_bytes(b'older-receipt')
        with self.assertRaises(ValueError):
            monitor.export_session(self.spec, destination)
        self.assertEqual(sidecar.read_bytes(), b'older-receipt')

    def test_occupied_prometheus_port_rejects_collector_before_any_partial_start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as foreign:
            foreign.bind(('127.0.0.1', 0))
            foreign.listen(1)
            self.spec['ports']['prometheus'] = foreign.getsockname()[1]
            with patch.object(monitor, 'run_container') as create, patch.object(monitor, 'http') as endpoint:
                with self.assertRaisesRegex(RuntimeError, 'listen address is unavailable'):
                    monitor.start_collector(self.spec, timeout_seconds=2)
            create.assert_not_called()
            endpoint.assert_not_called()
            self.assertFalse(Path(self.spec['state_dir']).exists())
            # The foreign listener is still the same open socket, not stopped.
            self.assertEqual(foreign.getsockname()[1], self.spec['ports']['prometheus'])

    def test_owned_container_running_image_and_labels_are_checked_not_http_alone(self):
        _, owner = monitor.state_for(self.spec, create=True)
        expected = {'name': 'exact-name', 'container_id': 'exact-id', 'image': monitor.images()['prom/prometheus'],
                    'image_id': 'sha256:exact-image'}
        detail = {'Id': 'exact-id', 'Image': expected['image_id'], 'State': {'Running': True},
                  'Config': {'Image': expected['image'], 'Labels': {
                      monitor.LABEL: self.spec['run_id'], monitor.LABEL + '.owner': owner['token']}}}
        for mutation in ('exited', 'wrong-image', 'wrong-pin', 'wrong-owner', 'wrong-id'):
            changed = copy.deepcopy(detail)
            if mutation == 'exited':
                changed['State']['Running'] = False
            elif mutation == 'wrong-image':
                changed['Image'] = 'sha256:foreign'
            elif mutation == 'wrong-pin':
                changed['Config']['Image'] = 'prom/prometheus:latest'
            elif mutation == 'wrong-owner':
                changed['Config']['Labels'][monitor.LABEL + '.owner'] = 'foreign'
            else:
                changed['Id'] = 'foreign-id'
            with self.subTest(mutation=mutation), patch.object(monitor, 'docker', return_value=json.dumps([changed])):
                with self.assertRaises(RuntimeError):
                    monitor.assert_owned_running(self.spec, owner, [expected])

    def test_foreign_prometheus_effective_config_cannot_pass_session_identity(self):
        _, owner = monitor.state_for(self.spec, create=True)
        for quote in ('', "'", '"'):
            correct = ('global:\n  external_labels:\n    campaign_observer_run_id: ' + quote + self.spec['run_id'] + quote
                       + '\n    campaign_observer_session: ' + quote + owner['identity'] + quote + '\n')
            with self.subTest(quote=quote), patch.object(monitor, 'http', return_value={'status': 'success', 'data': {'yaml': correct}}):
                self.assertEqual(monitor.assert_prometheus_identity(self.spec)['session_identity'], owner['identity'])
        foreign = correct.replace(owner['identity'], 'f' * 64)
        with patch.object(monitor, 'http', return_value={'status': 'success', 'data': {'yaml': foreign}}):
            with self.assertRaisesRegex(ValueError, 'another session'):
                monitor.assert_prometheus_identity(self.spec)

    def test_exited_new_container_is_automatically_removed_by_id_without_readiness_probe(self):
        state, owner = monitor.state_for(self.spec, create=True)
        image = monitor.images()['library/python']
        launched, removed = [], []
        detail = {'Id': 'new-id', 'Image': 'sha256:cached-image', 'State': {'Running': False},
                  'Config': {'Image': image, 'Labels': {
                      monitor.LABEL: self.spec['run_id'], monitor.LABEL + '.owner': owner['token']}}}
        def docker(spec, *args, **kwargs):
            if args[:2] == ('image', 'inspect'):
                return json.dumps([{'Id': 'sha256:cached-image', 'RepoDigests': [image]}])
            if args[0] == 'ps':
                return 'new-id' if launched else ''
            if args[0] == 'run':
                launched.append('new-id')
                return 'new-id'
            if args[0] == 'inspect':
                return json.dumps([detail])
            if args[0] == 'rm':
                removed.append(args[-1])
                return ''
            self.fail('unexpected Docker operation')
        @monitor.startup_guard
        def startup():
            monitor.run_container(self.spec, state, owner, 'benchmark', image, [])
        with patch.object(monitor, 'docker', side_effect=docker), patch.object(monitor, 'http') as endpoint:
            with self.assertRaisesRegex(RuntimeError, 'not running'):
                startup()
        self.assertEqual(removed, ['new-id'])
        endpoint.assert_not_called()
        self.assertTrue((state / 'startup-failure-benchmark.json').is_file())

    def test_nested_start_failure_rolls_back_new_hosts_even_after_operation_deadline(self):
        monitor.state_for(self.spec, create=True)
        @monitor.startup_guard
        def child():
            monitor.STARTUP_RESOURCES.get().append((self.spec, 'benchmark'))
            raise RuntimeError('collector failed after host creation')
        @monitor.startup_guard
        @monitor.bounded(0.001)
        def local_start():
            monitor.STARTUP_RESOURCES.get().append((self.spec, 'node--1'))
            time.sleep(0.002)
            child()
        cleaned = []
        def stop(spec, services):
            self.assertGreater(monitor.budget(spec, maximum=1, cleanup=True), 0)
            cleaned.extend(services)
            return {'complete': True}
        with patch.object(monitor, 'stop_services', side_effect=stop):
            with self.assertRaisesRegex(RuntimeError, 'collector failed'):
                local_start()
        self.assertEqual(cleaned, ['benchmark', 'node--1'])
        self.assertIsNone(monitor.STARTUP_RESOURCES.get())

    def test_existing_named_container_never_becomes_a_startup_rollback_target(self):
        state, owner = monitor.state_for(self.spec, create=True)
        def docker(spec, *args, **kwargs):
            if args[:2] == ('image', 'inspect'):
                return '[]'
            if args[0] == 'ps':
                return 'previous-owned-container'
            self.fail('must not run or remove existing container')
        @monitor.startup_guard
        def startup():
            monitor.run_container(self.spec, state, owner, 'benchmark', monitor.images()['library/python'], [])
        with patch.object(monitor, 'docker', side_effect=docker), patch.object(monitor, 'stop_services') as stop:
            with self.assertRaisesRegex(ValueError, 'existing observer container'):
                startup()
        stop.assert_not_called()


if __name__ == '__main__':
    unittest.main()
