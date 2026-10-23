"""Clock applicability is observable, not a zero-valued remote handler."""
import unittest

import analyze_cloud


class ProfilePopulations(unittest.TestCase):
    def test_registered_completion_has_no_handler_or_message_exchange_population(self):
        observed = analyze_cloud.remote_profile_observations(dict(
            whole_call_ns='2000', index_ns='100', server_handler_ns='0',
            server_record_read_ns='0', exchange_ns='0', request_encode_ns='0',
            response_decode_ns='0', wire_read_requests='0', server_timing_available='false'))
        self.assertEqual(observed['whole_call_ns'], 2000)
        self.assertEqual(observed['wire_read_requests'], 0)
        self.assertEqual(observed['server_timing_available'], 0)
        for key in ('server_handler_ns', 'server_record_read_ns', 'exchange_ns',
                    'request_encode_ns', 'response_decode_ns'):
            self.assertNotIn(key, observed)

    def test_actual_rpc_retains_its_observed_handler_clocks(self):
        observed = analyze_cloud.remote_profile_observations(dict(
            whole_call_ns='2000', server_handler_ns='500', server_record_read_ns='200',
            exchange_ns='1200', wire_read_requests='1', server_timing_available='true'))
        self.assertEqual(observed['server_handler_ns'], 500)
        self.assertEqual(observed['server_record_read_ns'], 200)
        self.assertEqual(observed['exchange_ns'], 1200)

    def test_absent_old_population_metadata_does_not_become_a_zero_counter(self):
        observed = analyze_cloud.remote_profile_observations(dict(whole_call_ns='2000'))
        self.assertNotIn('wire_read_requests', observed)
        self.assertNotIn('server_timing_available', observed)
        self.assertNotIn('server_handler_ns', observed)

    def test_unavailable_server_spans_cannot_hide_observed_clock_values(self):
        with self.assertRaisesRegex(ValueError, 'unavailable server span'):
            analyze_cloud.remote_profile_observations(dict(
                server_timing_available='false', server_handler_ns='10'))


if __name__ == '__main__':
    unittest.main()
