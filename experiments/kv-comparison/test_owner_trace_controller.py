import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import cloud_controller as controller


class OwnerTraceEvidenceTests(unittest.TestCase):
    def test_case_trace_files_and_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            coverage = {'event_count': 3, 'first_timestamp_ns': 100, 'last_timestamp_ns': 300,
                        'duration_ns': 200, 'scope': 'captured_complete_trace_events', 'clock': 'mono'}
            empty_coverage = {'event_count': 0, 'first_timestamp_ns': None, 'last_timestamp_ns': None,
                              'duration_ns': None, 'scope': 'captured_complete_trace_events', 'clock': 'mono'}
            controller.store_owner_ftrace(root, [
                {'available': False}, {'trace': 'nvmet trace\n', 'events_enabled': ['nvmet/nvmet_req_init'],
                                      'truncated': False, 'buffer_overruns': 0,
                                      'trace_coverage': coverage,
                                      'cpu_samples': {'available': True, 'samples': [{'cpus': {'cpu0': {}}}],
                                                      'summary': {'max_busy_pct': 95}}},
                {'trace': 'second owner', 'truncated': True, 'buffer_overruns': 9,
                 'trace_coverage': empty_coverage,
                 'cpu_samples': {'available': False, 'reason': 'denied'}},
                {'inactive': True}])
            self.assertEqual((root / 'owner-ftrace.txt').read_text(), 'nvmet trace\n')
            data = json.loads((root / 'owner-ftrace.json').read_text())
            self.assertNotIn('trace', data)
            self.assertEqual(data['buffer_overruns'], 0)
            self.assertEqual(data['trace_coverage'], coverage)
            self.assertFalse(data['truncated'])
            self.assertNotIn('cpu_summary', data)
            self.assertNotIn('cpu_samples', data)
            self.assertTrue(json.loads((root / 'owner-cpu.json').read_text())['available'])
            self.assertEqual(json.loads((root / 'owner-cpu.json').read_text())['summary'], {'max_busy_pct': 95})
            second = root / 'owner-ftrace/node-1/owner-cpu.json'
            self.assertFalse(json.loads(second.read_text())['available'])
            second_trace = json.loads((second.parent / 'owner-ftrace.json').read_text())
            self.assertEqual(second_trace['trace_coverage'], empty_coverage)
            self.assertTrue(second_trace['truncated'])
            self.assertEqual(second_trace['buffer_overruns'], 9)
            self.assertNotIn('cpu_summary', second_trace)

    def test_stop_collects_owner_events_with_requested_flag(self):
        with tempfile.TemporaryDirectory() as root:
            runtime = object.__new__(controller.Controller)
            runtime.active_nvme = None
            runtime.tide_cleanup_forbidden = False
            runtime.shared_tide = None
            runtime.active_tide = ({'case_id': 'case', 'owner_trace': True}, Path(root), [0])
            runtime.cleanup_deadline = Mock(return_value=10)
            runtime.cleanup_timeout = Mock(return_value=5)
            runtime.request = Mock(return_value={'owner_events': {'schema': 'th-owner-events-v1', 'events': []}})
            runtime.stop_tide()
            self.assertTrue(runtime.request.call_args.args[1]['owner_trace'])
            data = json.loads((Path(root) / 'owner-events/node-0.json').read_text())
            self.assertEqual(data['schema'], 'th-owner-events-v1')
