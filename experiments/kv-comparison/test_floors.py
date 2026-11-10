import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import cloud_controller
import floors


class FloorTests(unittest.TestCase):
    def test_real_perftest_row(self):
        log = (Path(__file__).resolve().parents[3] / 'checkpoint-work/focused-launch-20261109-g10b/fleet-output/qualification/bench-6e40ca83d74146cf/bench-6e40ca83d74146cf-0/tidehunter-qualify-send.log').read_text()
        row = floors.parse_perftest(log, 1024, 1000)
        self.assertEqual((row['min'], row['p50'], row['avg'], row['p99'], row['p999']), (5.83, 6.03, 6.08, 8.39, 9.44))
        self.assertIsNone(row['p90'])
        with self.assertRaises(ValueError):
            floors.parse_perftest(log, 8, 1000)

    def test_fabric_row(self):
        header = 'bytes #sent #ack total time MB/sec usec/xfer Mxfers/sec\n'
        rows = [
            (8, '8 5k =5k 78k 0.03s 3.04 2.63 0.38', 2.63),
            (1024, '1k 5k =5k 9.7m 0.03s 391.72 2.61 0.38', 2.61),
            (4096, '4k 5k =5k 39m 0.03s 1247.03 3.28 0.30', 3.28),
            (65536, '64k 5k =5k 625m 0.11s 5830.71 11.24 0.09', 11.24),
        ]
        for size, row, expected in rows:
            self.assertEqual(floors.parse_fi_pingpong(header + row, size)['avg'], expected)
        with self.assertRaises(ValueError):
            floors.parse_fi_pingpong(header + rows[0][1], 4096)

    def test_fio_log_columns_and_bounds(self):
        self.assertEqual(floors.parse_fio_log(b'2, 2191830, 0, 4096, 4096000, 0\n', block_bytes=4096, region_bytes=4194304), [(2, 2191830, 0, 4096, 4096000)])
        self.assertEqual(len(floors.parse_fio_log(b'1,2,0,4096,0\n')), 1)
        for raw in (b'', b'1,2,1,4096,0\n', b'1,2,0,4096\n', b'1,2,0,4096,4194304\n'):
            with self.assertRaises(ValueError):
                floors.parse_fio_log(raw, block_bytes=4096, region_bytes=4194304)
        with self.assertRaises(ValueError):
            floors.parse_fio_log(b'x' * (floors.MAX_LOG_BYTES + 1))
        self.assertLess(floors.MAX_LOG_BYTES * 4 / 3 + 1024 * 1024, cloud_controller.MAX_JSON_BYTES)

    def test_ssd_scratch_uses_fresh_random_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(cloud_controller.os, 'urandom', side_effect=lambda n: bytes([n % 251]) * n) as random:
                identity = cloud_controller.provision_fio_scratch(tmp, 'random', 1024 * 1024 + 64, pattern='random')
            self.assertEqual([call.args[0] for call in random.call_args_list], [1024 * 1024, 64])
            raw = Path(identity['path']).read_bytes()
            self.assertEqual(raw[:1024 * 1024], bytes([(1024 * 1024) % 251]) * (1024 * 1024))
            self.assertEqual(raw[-64:], bytes([64]) * 64)
            cloud_controller.remove_fio_scratch(tmp, 'random', identity)

    def test_ssd_fio_command_uses_job_runtime(self):
        job = next(j for j in cloud_controller.cloud_campaign.make_focused_matrix('floors')
                   if j['case_kind'] == 'ssd_floor')
        ssd = job['ssd']
        argv = cloud_controller.fio_read_command('fio', '/scratch', {**ssd, 'location': 'local_file'},
                                                ssd['region_bytes'], runtime_seconds=ssd['runtime_seconds'])
        self.assertIn('--runtime=' + str(ssd['runtime_seconds']), argv)
        with self.assertRaises(ValueError):
            cloud_controller.fio_read_command('fio', '/scratch', {**ssd, 'location': 'local_file'},
                                              ssd['region_bytes'])

    def test_ports_and_fixed_argv(self):
        net = {'tool': 'ib_send_lat', 'mode': 'event', 'interface': 'rdma0', 'provider': None,
               'endpoint': None, 'sizes': [8, 1024, 4096, 65536], 'iterations': 5000, 'warmup': 0}
        ports = [floors.port_for('case', net['tool'], i) for i in range(4)]
        self.assertEqual(len(set(ports)), 4)
        self.assertTrue(all(18700 <= p <= 18799 for p in ports))
        server = floors.server_argv(net, 8, ports[0], '10.0.0.1', None)
        self.assertEqual(server, ['ib_send_lat', '-R', '-d', 'irdma0', '-p', str(ports[0]), '-s', '8', '-n', '5000', '-e'])
        self.assertEqual(floors.client_argv(net, 8, ports[0], '10.0.0.2', '10.0.0.1', None), server + ['10.0.0.1'])
        net.update(tool='fi_pingpong', mode=None, provider='verbs;ofi_rxm', endpoint='rdm')
        server = floors.server_argv(net, 8, 18901, '10.0.0.1', None)
        client = floors.client_argv(net, 8, 18901, '10.0.0.2', '10.0.0.1', None)
        self.assertIn('-B', server)
        self.assertIn('-P', client)
        self.assertEqual(client[-1], '10.0.0.1')

    def test_rdma_listener_wildcards_and_device_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'server.out'
            for address in ('0.0.0.0:18701', '*:18701', '[::]:18701'):
                records = [{'pid': 42, 'state': 'LISTEN', 'src-addr': address}]
                with patch('subprocess.run', return_value=MagicMock(stdout=json.dumps(records).encode())):
                    self.assertTrue(floors.rdma_listening(42, 18701, 1, output))
                    self.assertFalse(floors.rdma_listening(42, 18702, 1, output))
            records = [{'pid': 42, 'state': 'LISTEN', 'src-addr': '10.20.0.2:18701',
                        'ifname': 'irdma0', 'ps': 'TCP'}]
            with patch('subprocess.run', return_value=MagicMock(stdout=json.dumps(records).encode())):
                self.assertTrue(floors.rdma_listening(42, 18701, 1, output))

    def test_rdma_listener_output_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'server.out'
            output.write_text('Waiting for client to connect')
            with patch('subprocess.run') as command:
                self.assertTrue(floors.rdma_listening(42, 18701, 1, output))
                command.assert_not_called()
            output.write_text('')
            def malformed(*args, **kwargs):
                output.write_text('Waiting for client to connect')
                return MagicMock(stdout=b'not JSON')
            with patch('subprocess.run', side_effect=malformed):
                self.assertTrue(floors.rdma_listening(42, 18701, 1, output))
            output.write_text('')
            with patch('subprocess.run', side_effect=FileNotFoundError):
                self.assertFalse(floors.rdma_listening(42, 18701, 1, output))

    def test_perftest_retries_are_bounded(self):
        with patch.object(cloud_controller, 'floor_client_command',
                          return_value=(1, 'Connection refused')) as command, \
                patch.object(cloud_controller.time, 'monotonic', return_value=0), \
                patch.object(cloud_controller.time, 'sleep'):
            self.assertEqual(cloud_controller.floor_client_attempts(['ib_send_lat'], 10, perftest=True),
                             (1, 'Connection refused', 3))
            self.assertEqual(command.call_count, 3)
        with patch.object(cloud_controller, 'floor_client_command',
                          return_value=(1, 'rdma_cm connect failed')) as command, \
                patch.object(cloud_controller.time, 'monotonic', side_effect=[0, 0, 0, 10]):
            with self.assertRaises(cloud_controller.DeadlineError):
                cloud_controller.floor_client_attempts(['ib_send_lat'], 10, perftest=True)
            self.assertEqual(command.call_count, 1)
        with patch.object(cloud_controller, 'floor_client_command',
                          return_value=(1, 'invalid argument')) as command, \
                patch.object(cloud_controller.time, 'monotonic', return_value=0):
            self.assertEqual(cloud_controller.floor_client_attempts(['ib_send_lat'], 10, perftest=True)[2], 1)
            self.assertEqual(command.call_count, 1)

    def test_all_matrix_jobs_reach_floor_dispatcher(self):
        jobs = cloud_controller.cloud_campaign.make_focused_matrix('floors')
        self.assertEqual(len(jobs), 69)
        for job in jobs:
            cloud_controller.cloud_campaign.validate_focused_job(job)
        instance = object.__new__(cloud_controller.Controller)
        for job in jobs:
            with patch.object(instance, 'run_floor_case', return_value='dispatched') as dispatch:
                self.assertEqual(instance.run_case(job), 'dispatched')
                dispatch.assert_called_once_with(job)

    def controller(self, root):
        instance = cloud_controller.Controller.__new__(cloud_controller.Controller)
        instance.root = Path(root)
        instance.config = {'binary_blake2b512': 'a' * 128,
                           'fleet': {'coordinator': {'address': '10.0.0.1', 'rdma_address': '10.1.0.1'},
                                     'nodes': [{'address': '10.0.0.2', 'rdma_address': '10.1.0.2'}]}}
        instance.active_tide = instance.active_nvme = instance.active_backend = instance.shared_tide = None
        instance.monitoring_attempted = False
        instance.monitoring_events = []
        instance.select_monitoring = MagicMock()
        instance.finish_monitoring = MagicMock()
        instance.stop_tide = MagicMock()
        instance.guard = MagicMock()
        instance.remaining = MagicMock(return_value=100)
        instance.cleanup_deadline = MagicMock(return_value=100)
        instance.cleanup_timeout = MagicMock(return_value=30)
        return instance

    def test_ssd_case_binds_evidence_and_handles_measurement_failure(self):
        job = next(j for j in cloud_controller.cloud_campaign.make_focused_matrix('floors')
                   if j['case_kind'] == 'ssd_floor')
        raw = json.dumps({'jobs': [{'read': {'total_ios': 1, 'io_bytes': 4096, 'runtime': 1000,
                    'clat_ns': {'percentile': {'50.000000': 1, '99.000000': 2, '99.900000': 3}}},
                    'write': {'total_ios': 0}, 'error': 0, 'iodepth_level': {'1': 100}}]}).encode()
        log = b'1, 2, 0, 4096, 0, 0\n'
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.controller(tmp)
            instance.request = MagicMock(return_value={'raw_base64': base64.b64encode(raw).decode(),
                'lat_base64': base64.b64encode(log).decode(), 'argv': ['fio', '--readonly'],
                'device_identity': '259:0', 'opened_read_only': True, 'actual_direct': True})
            receipt = instance.run_floor_case(job)
            self.assertEqual(instance.request.call_args.args[0], 0)
            self.assertEqual(receipt['status'], 'completed')
            result = json.loads((Path(tmp) / 'cases' / job['case_id'] / 'ssd_floor_result.json').read_text())
            self.assertEqual(result['lat_log']['path'], receipt['ssd_floor_lat_log']['path'])
            self.assertEqual(result['lat_log']['sha256'], receipt['ssd_floor_lat_log']['sha256'])
            self.assertEqual(result['lat_log']['rows'], 1)
            self.assertIn('fio_command', receipt)
            self.assertIn('phase_timing', receipt)
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.controller(tmp)
            instance.request = MagicMock(return_value={'fio_failed': 'measurement error'})
            receipt = instance.run_floor_case(job)
            self.assertEqual(receipt['status'], 'failed')
            self.assertIn('measurement error', receipt['error'])

    def test_network_client_failure_collects_server(self):
        job = next(j for j in cloud_controller.cloud_campaign.make_focused_matrix('floors')
                   if j['case_kind'] == 'net_floor')
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.controller(tmp)
            def request(index, request, **kwargs):
                if request['action'] == 'net_floor_serve':
                    return {'ready': True}
                if request['action'] == 'net_floor_run':
                    raise ValueError('client error')
                return {'servers': []}
            instance.request = MagicMock(side_effect=request)
            receipt = instance.run_floor_case(job)
            self.assertEqual(receipt['status'], 'failed')
            self.assertEqual(instance.request.call_args_list[-1].args[1]['action'], 'net_floor_collect')
            instance.request = MagicMock(side_effect=[{'ready': True}, ValueError('client error'),
                                                       RuntimeError('cleanup error')])
        with tempfile.TemporaryDirectory() as tmp:
            instance.root = Path(tmp)
            with self.assertRaises(cloud_controller.InfrastructureError):
                instance.run_floor_case(job)

    def test_network_evidence_binds_server_and_result(self):
        import hashlib
        job = next(j for j in cloud_controller.cloud_campaign.make_focused_matrix('floors')
                   if j['case_kind'] == 'net_floor')
        net = job['net']
        runs = [{'size_bytes': size, 'iterations': net['iterations'], 'status': 'completed',
                 'argv_client': ['client'], 'argv_server': None, 'latency_semantics': 'half_round_trip',
                 'latency_us': dict.fromkeys(floors.LATENCY_FIELDS), 'setsockopt': None,
                 'raw_client_output': '', 'raw_server_output': '',
                 'raw_sha256': hashlib.sha256(b'').hexdigest(), 'error': None} for size in net['sizes']]
        servers = [{'argv': ['server'], 'exit_status': 0, 'output': ''} for _ in net['sizes']]
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.controller(tmp)
            instance.request = MagicMock(side_effect=[{'ready': True}, {'runs': runs, 'build': None},
                                                       {'servers': servers}])
            receipt = instance.run_floor_case(job)
            self.assertEqual(receipt['status'], 'completed')
            result = json.loads((Path(tmp) / 'cases' / job['case_id'] / 'net_floor_result.json').read_text())
            cloud_controller.analyze._validate_focused_artifact('net_floor_result', result, job, receipt)
            result['runs'][0]['attempts'] = 3
            cloud_controller.analyze._validate_focused_artifact('net_floor_result', result, job, receipt)
            result['runs'][0]['attempts'] = 4
            with self.assertRaises(ValueError):
                cloud_controller.analyze._validate_focused_artifact('net_floor_result', result, job, receipt)
            self.assertTrue(all(row['argv_server'] == ['server'] for row in result['runs']))


    def test_append_argv_and_log_contract(self):
        spec = {'mode': 'append_pwrite', 'block_bytes': 1024, 'region_bytes': 536870912,
                'queue_depth': 1, 'location': 'local_file', 'operations': 4096,
                'warmup_operations': 512, 'host': 'client'}
        identity = {'device': 12, 'inode': 34}
        log = b''.join(f'10,5,15,{(512 + i) * 1024}\n'.encode() for i in range(4096))
        for mode in floors.APPEND_MODES:
            spec['mode'] = mode
            argv = floors.write_argv('/helper', '/tmp/scratch', '/tmp/log', spec, identity)
            self.assertEqual(argv[4:9], [mode, '1024', '536870912', '6', '4096'])
            self.assertEqual(len(floors.parse_write_log(log, spec)), 4096)
            for invalid in (log[:-len(f'10,5,15,{(512 + 4095) * 1024}\n')],
                            log.replace(b'10,5,15,524288\n', b'10,5,15,0\n', 1),
                            log + b'10,5,15,0\n'):
                with self.assertRaises(ValueError):
                    floors.parse_write_log(invalid, spec)
        spec['host'] = 'invalid'
        with self.assertRaises(ValueError):
            floors.write_argv('/helper', '/tmp/scratch', '/tmp/log', spec, identity)


if __name__ == '__main__':
    unittest.main()
