import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import analyze
import floors
from test_floors import ssd_floor_job


class FloorsEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.net = {'case_id': 'network-floor', 'case_kind': 'net_floor',
                    'net': {'tool': 'ib_send_lat', 'mode': 'polled', 'interface': 'rdma0',
                            'provider': None, 'endpoint': None, 'sizes': [8, 1024, 4096, 65536],
                            'iterations': 5000, 'warmup': 0}}
        self.ssd = ssd_floor_job()

    def bind(self, name, content):
        raw = content if isinstance(content, bytes) else json.dumps(content).encode()
        (self.root / name).write_bytes(raw)
        return {'path': name, 'sha256': hashlib.sha256(raw).hexdigest()}

    def phase(self, job):
        return self.bind('phase.json', {'schema_version': 1, 'case_id': job['case_id'], 'spans': []})

    def network(self):
        job = self.net
        net = job['net']
        run = {'size_bytes': 0, 'iterations': net['iterations'], 'status': 'completed',
               'argv_client': ['client'], 'argv_server': ['server'], 'latency_semantics': 'half_round_trip',
               'latency_us': dict.fromkeys(('min', 'p50', 'avg', 'p90', 'p99', 'p999', 'max', 'stdev'), None),
               'setsockopt': None, 'raw_client_output': 'output', 'raw_server_output': '',
               'raw_sha256': hashlib.sha256(b'output').hexdigest(), 'error': None}
        result = {'schema_version': 1, 'case_id': job['case_id'], **{k: net[k] for k in ('tool', 'mode', 'interface', 'provider', 'endpoint')},
                  'client_host': 0, 'server_host': 1, 'client_address': '10.0.0.1', 'server_address': '10.0.0.2',
                  'device': 'irdma0', 'build': None, 'runs': [{**run, 'size_bytes': size} for size in net['sizes']]}
        return result

    def test_network_admission_and_rejections(self):
        result = self.network()
        receipt = {'phase_timing': self.phase(self.net), 'net_floor_result': self.bind('net.json', result)}
        self.assertIn('net_floor_result', analyze.admit_focused_artifacts(self.root, self.net, receipt, completed=True))
        missing = dict(receipt); del missing['net_floor_result']
        with self.assertRaises(ValueError):
            analyze.admit_focused_artifacts(self.root, self.net, missing, completed=True)
        wrong_hash = copy.deepcopy(receipt); wrong_hash['net_floor_result']['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            analyze.admit_focused_artifacts(self.root, self.net, wrong_hash, completed=True)
        for field, value in [('case_id', 'other'), ('schema_version', 2)]:
            bad = copy.deepcopy(result); bad[field] = value
            receipt['net_floor_result'] = self.bind('net.json', bad)
            with self.subTest(field=field), self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(self.root, self.net, receipt, completed=True)
        for side in ('raw_client_output', 'raw_server_output'):
            bad = copy.deepcopy(result); bad['runs'][0][side] = 'x' * (16 * 1024 + 1)
            receipt['net_floor_result'] = self.bind('net.json', bad)
            with self.subTest(side=side), self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(self.root, self.net, receipt, completed=True)

    def test_ssd_log_binding_and_bounds(self):
        job, ssd = self.ssd, self.ssd['ssd']
        fio = {'jobs': [{'read': {'total_ios': 1, 'io_bytes': ssd['block_bytes'], 'runtime': 1000,
                                 'clat_ns': {'percentile': {'50.000000': 10, '99.000000': 10, '99.900000': 10}}},
                         'write': {'total_ios': 0}, 'error': 0, 'iodepth_level': {'1': 100}}]}
        raw = self.bind('fio_raw.json', fio)
        log = self.bind('ssd_floor_lat.log', f"0, 10, 0, {ssd['block_bytes']}, 0\n".encode())
        command = self.bind('fio_command.json', ['fio', '--readonly', '--direct=1', '--rw=randread', '--output-format=json', '--numjobs=1', '--name=read'])
        result = {'schema_version': 1, 'case_id': job['case_id'], 'location': 'local_file', 'device_identity': 'disk',
                  'namespace_uuid': 'none', 'block_bytes': ssd['block_bytes'], 'requested_depth': 1, 'effective_depth': 1,
                  'actual_direct': True, 'read_only': True, 'elapsed_seconds': 1, 'read_ios': 1, 'read_bytes': ssd['block_bytes'],
                  'errors': 0, 'latency_ns': {'p50': 10, 'p99': 10, 'p999': 10}, 'fio_sha256': raw['sha256'],
                  'raw_fio_path': raw['path'], 'region_bytes': ssd['region_bytes'], 'write_ios': 0, 'opened_read_only': True,
                  'quiescent': True, 'target_buffered_io': None, 'state': ssd['state'], 'age_seconds': ssd['age_seconds'],
                  'lat_log': {**log, 'rows': 1, 'columns': ['time_ms', 'latency_ns', 'direction', 'block_bytes', 'offset']}}
        receipt = {'phase_timing': self.phase(job), 'ssd_floor_lat_log': log, 'fio_command': command,
                   'ssd_floor_result': self.bind('ssd.json', result)}
        admitted = analyze.admit_focused_artifacts(self.root, job, receipt, completed=True)
        self.assertTrue(admitted['ssd_floor_result']['qualification']['qualified'])
        for key, value in (('case_id', 'other'), ('schema_version', 2), ('fio_sha256', '0' * 64)):
            bad_result = copy.deepcopy(result)
            bad_result[key] = value
            bad_receipt = dict(receipt, ssd_floor_result=self.bind('ssd-bad.json', bad_result))
            with self.subTest(key=key), self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(self.root, job, bad_receipt, completed=True)
        for mutation in ('missing', 'hash', 'oversize'):
            bad = copy.deepcopy(receipt)
            if mutation == 'missing':
                del bad['ssd_floor_lat_log']
            elif mutation == 'hash':
                (self.root / log['path']).write_bytes(b'wrong')
            else:
                content = b'x' * (floors.MAX_LOG_BYTES + 1)
                bad['ssd_floor_lat_log'] = self.bind(log['path'], content)
                result['lat_log'].update(bad['ssd_floor_lat_log'])
                bad['ssd_floor_result'] = self.bind('ssd.json', result)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(self.root, job, bad, completed=True)

    def test_append_evidence_binds_host_and_rows(self):
        spec = {'mode': 'append_mmap_fsync', 'block_bytes': 1024, 'region_bytes': 536870912,
                'queue_depth': 1, 'location': 'local_file', 'operations': 4096,
                'warmup_operations': 512, 'host': 'owner0'}
        job = {'case_id': 'append', 'ssd_write': spec}
        raw = json.dumps({'total_operations': 4096, 'logged_operations': 4096,
                          'elapsed_ns': 100000}).encode()
        log = b''.join(f'10,5,15,{(512 + i) * 1024}\n'.encode() for i in range(4096))
        result = floors.write_result(job, raw, log, 'disk', False)
        raw_binding = self.bind('append-raw.json', raw)
        log_binding = self.bind('append.log', log)
        result['raw'] = raw_binding
        result['lat_log'] = {**log_binding, 'rows': 4096,
                             'columns': ['write_ns', 'sync_ns', 'total_ns', 'offset']}
        receipt = {'ssd_write_floor_lat_log': log_binding}
        analyze._verify_ssd_write_floor(self.root, result, receipt, job)
        for key, value in (('host_index', -1), ('logged_operations', 4095),
                           ('warmup_operations', 511)):
            altered = copy.deepcopy(result)
            altered[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                analyze._verify_ssd_write_floor(self.root, altered, receipt, job)
        bad_raw = json.dumps({'total_operations': 4097, 'logged_operations': 4096,
                              'elapsed_ns': 100000}).encode()
        with self.assertRaises(ValueError):
            floors.write_result(job, bad_raw, log, 'disk', False)


if __name__ == '__main__':
    unittest.main()
