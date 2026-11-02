import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from pathlib import Path

import gcp_runtime as runtime
import gcp_worker as worker


class Error(Exception):
    def __init__(self, status):
        self.status = status


class Store:
    def __init__(self):
        self.values = {}

    def put(self, key, data, **kwargs):
        if kwargs.get('if_generation_match') == 0 and key in self.values:
            raise Error(412)
        self.values[key] = (str(len(self.values) + 1), data)
        return self.head(key)

    def head(self, key):
        if key not in self.values:
            raise Error(404)
        return {'name': key, 'generation': self.values[key][0]}

    def get(self, key, *, generation=None, max_bytes=0):
        meta = self.head(key)
        if generation is not None and str(generation) != meta['generation']:
            raise Error(404)
        return self.values[key][1]


class Api:
    def __init__(self):
        self.store = Store()

    def objects(self, bucket):
        return self.store


def fleet():
    return [{'index': i, 'role': 'coordinator' if i == 0 else 'owner', 'name': f'run-{i}',
             'id': str(100 + i), 'zone': 'us-central1-a', 'internal_ip': f'10.0.0.{i+1}',
             'rdma_ip': f'10.1.0.{i+1}'} for i in range(6)]


def deadline():
    return (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()


class MailboxTests(unittest.TestCase):
    def test_real_subprocess_round_trip_and_replay_gate(self):
        api = Api()
        expires = deadline()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, expires)
        with tempfile.TemporaryDirectory() as temporary, patch.object(runtime, 'GUEST_PYTHON', sys.executable):
            root = Path(temporary)
            source = root / 'package/source'
            source.mkdir(parents=True)
            script = source / 'command.py'
            script.write_text('import pathlib,sys\np=pathlib.Path(sys.argv[1]); p.write_text(p.read_text()+"x" if p.exists() else "x"); print("done")\n')
            counter = root / 'counter'
            reference = mailbox.submit(1, [runtime.GUEST_PYTHON, str(script), str(counter)], operation_id='one', timeout_seconds=10)
            config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'deadline_utc': expires, 'package_root': str(root / 'package')}
            gate = root / 'gate'; gate.mkdir()
            for _ in range(2):
                worker.run_request(config, fleet()[1], api.store, reference['key'], reference['generation'], gate)
            receipt = mailbox.wait(reference, timeout_seconds=1)
            self.assertEqual(counter.read_text(), 'x')
            self.assertEqual(receipt['exit_code'], 0)
            self.assertEqual(receipt['stdout'], 'done\n')

    def test_collision_and_missing_response_fail_closed(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/b.py'], operation_id='id', timeout_seconds=1)
        with self.assertRaises(TimeoutError):
            mailbox.wait(ref, timeout_seconds=0.01)

    def test_reject_wrong_receipt_source(self):
        api = Api()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, deadline())
        ref = mailbox.submit(0, [runtime.GUEST_PYTHON, '/source/a.py'], operation_id='id', timeout_seconds=1)
        key = ref['key'].replace('/requests/', '/responses/')
        api.store.put(key, json.dumps({'request_sha256': ref['request_sha256'], 'instance_id': ref['instance_id'],
                                      'operation_id': 'id', 'source_sha256': 'b' * 64, 'exit_code': 0,
                                      'stdout': '', 'stderr': ''}).encode())
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.wait(ref, timeout_seconds=1)

    def test_deadline_and_fleet_identity_are_enforced(self):
        api = Api()
        expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        mailbox = runtime.RemoteMailbox(api, 'bucket', 'run', fleet(), 'a' * 64, expired)
        with self.assertRaises(runtime.RuntimeSafetyError):
            mailbox.submit(0, ['x'], operation_id='id', timeout_seconds=1)
        bad = fleet(); bad[1]['id'] = bad[0]['id']
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.RemoteMailbox(api, 'bucket', 'run', bad, 'a' * 64, deadline())

    def test_python_command_requires_admitted_source_and_interpreter(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'source'
            source.mkdir()
            script = source / 'command.py'
            script.write_text('pass\n')
            config = {'package_root': temporary, 'run_id': 'run'}
            request = {'argv': [runtime.GUEST_PYTHON, str(script)], 'cwd': None, 'env': {}}
            self.assertEqual(worker.admit_command(config, request)[0], request['argv'])
            for argv in ([runtime.GUEST_PYTHON, '/tmp/untrusted.py'], ['/usr/bin/python3', str(script)]):
                with self.subTest(argv=argv), self.assertRaises(runtime.RuntimeSafetyError):
                    worker.admit_command(config, {**request, 'argv': argv})
            for key in ('PATH', 'LD_PRELOAD', 'PYTHONPATH'):
                with self.subTest(environment=key), self.assertRaises(runtime.RuntimeSafetyError):
                    worker.admit_command(config, {**request, 'env': {key: '/tmp/untrusted'}})

    def test_live_fleet_rejects_changed_expiry_and_identity(self):
        expires = deadline()
        hosts = fleet()
        config = {'platform': {'runtime': {'fleet': hosts, 'run_id': 'run', 'source_sha256': 'a' * 64,
                                             'deadline_utc': expires, 'machine_type': 'z4m-highmem-16'}}}
        rows = {}
        for host in hosts:
            rows[host['name']] = {'id': host['id'], 'name': host['name'], 'status': 'RUNNING',
                'machineType': 'zones/us-central1-a/machineTypes/z4m-highmem-16',
                'labels': {'bench-run': 'run', 'bench-source': 'a' * 63},
                'metadata': {'items': [{'key': 'bench-run', 'value': 'run'},
                                       {'key': 'bench-source', 'value': 'a' * 64},
                                       {'key': 'bench-expiry', 'value': expires}]},
                'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': expires},
                'networkInterfaces': [{'networkIP': host['internal_ip'], 'nicType': 'GVNIC'},
                                      {'networkIP': host['rdma_ip'], 'nicType': 'IRDMA'}],
                'disks': [{'type': 'SCRATCH', 'interface': 'NVME'}],
                'resourcePolicies': ['regions/us-central1/resourcePolicies/run-placement']}
        class Compute:
            def request(self, method, path):
                return rows[path.rsplit('/', 1)[-1]]
        clients = {'platform': Compute()}
        self.assertEqual(len(runtime.verify_fleet(config, clients)), 6)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'STOP'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'DELETE'
        rows['run-1']['id'] = '999'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)

    def test_h4d_fleet_requires_all_ten_nvme_scratch_disks(self):
        expires = deadline()
        hosts = fleet()
        shape = 'h4d-highmem-192-lssd'
        config = {'platform': {'runtime': {'fleet': hosts, 'run_id': 'run', 'source_sha256': 'a' * 64,
                                           'deadline_utc': expires, 'machine_type': shape}}}
        rows = {}
        for host in hosts:
            rows[host['name']] = {'id': host['id'], 'name': host['name'], 'status': 'RUNNING',
                'machineType': 'zones/us-central1-a/machineTypes/' + shape,
                'labels': {'bench-run': 'run', 'bench-source': 'a' * 63},
                'metadata': {'items': [{'key': 'bench-run', 'value': 'run'},
                                       {'key': 'bench-source', 'value': 'a' * 64},
                                       {'key': 'bench-expiry', 'value': expires}]},
                'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': expires},
                'networkInterfaces': [{'networkIP': host['internal_ip'], 'nicType': 'GVNIC'},
                                      {'networkIP': host['rdma_ip'], 'nicType': 'IRDMA'}],
                'disks': [{'type': 'PERSISTENT', 'interface': 'NVME', 'boot': True}] +
                         [{'type': 'SCRATCH', 'interface': 'NVME'} for _ in range(10)],
                'resourcePolicies': ['regions/us-central1/resourcePolicies/run-placement']}
        class Compute:
            def request(self, method, path):
                return rows[path.rsplit('/', 1)[-1]]
        clients = {'platform': Compute()}
        result = runtime.verify_fleet(config, clients)
        self.assertEqual(result['run-0']['selected_local_ssd'], 'google-local-nvme-ssd-0')
        self.assertEqual(result['run-0']['unused_local_ssd_count'], 9)
        disks = rows['run-1']['disks']
        disks.pop()
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        disks.append({'type': 'SCRATCH', 'interface': 'SCSI'})
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        disks[-1]['interface'] = 'NVME'
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'STOP'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)
        rows['run-1']['scheduling']['instanceTerminationAction'] = 'DELETE'
        config['platform']['runtime']['machine_type'] = 'h4d-standard-192'
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.verify_fleet(config, clients)

    def test_rdma_requires_admitted_network_binding(self):
        config = {'spec': {'backends': ['tidehunter_nvme_rdma']},
                  'platform': {'runtime': {'fleet': fleet()}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('subprocess.run'), patch('subprocess.check_output', side_effect=['link rdma0/1 netdev eth1', addresses]):
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertEqual(proof['network_interface'], 'eth1')
        with patch('subprocess.run'), patch('subprocess.check_output', side_effect=['link rdma0/1 netdev eth0', addresses]):
            with self.assertRaises(runtime.RuntimeSafetyError):
                runtime.admit_fabrics(config, 0, {}, '/tmp')

    def test_verbs_binds_each_host_without_mutating_case_defaults(self):
        transport = {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'}
        config = {'fleet': {'coordinator': {'rdma_address': '10.1.0.1'},
                            'nodes': [{'rdma_address': f'10.1.0.{i+2}'} for i in range(5)]}}
        prepared = runtime.prepare_transport(config, {'transport': transport})
        self.assertEqual(prepared['transport']['fabric']['source_address'], '10.1.0.1')
        self.assertEqual([value['fabric']['source_address'] for value in prepared['owner_transports']],
                         [f'10.1.0.{i+2}' for i in range(5)])
        self.assertEqual(transport, {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'})
        self.assertEqual(runtime.prepare_transport(config, {'transport': {'kind': 'tcp'}}),
                         {'transport': {'kind': 'tcp'}})

    def test_valkey_rdma_requires_cm_and_its_admitted_nic(self):
        config = {'spec': {'backends': ['valkey_tcp', 'valkey_rdma']},
                  'platform': {'runtime': {'fleet': fleet()}}}
        addresses = json.dumps([{'ifname': 'eth1', 'addr_info': [{'local': '10.1.0.2'}]}])
        with patch('subprocess.run'), patch.object(Path, 'is_char_device', return_value=False):
            with self.assertRaisesRegex(runtime.RuntimeSafetyError, 'RDMA-CM'):
                runtime.admit_fabrics(config, 0, {}, '/tmp')
        with patch('subprocess.run'), patch.object(Path, 'is_char_device', return_value=True), \
             patch('subprocess.check_output', side_effect=['link rdma0/1 netdev eth1', addresses]):
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertTrue(proof['valkey_rdma_requested'])
        self.assertFalse(proof['nvme_rdma_requested'])
        self.assertEqual(proof['address'], '10.1.0.2')

    def test_runtime_accepts_real_comparison_backends_but_not_efa(self):
        expires = deadline()
        config = {'project_id': 'project', 'project_number': '123', 'bucket': 'bucket',
                  'machine_type': 'z4m-highmem-16', 'run_id': 'run', 'fleet': fleet(),
                  'source_sha256': 'a' * 64, 'deadline_utc': expires,
                  'campaign_deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
                  'package_root': '/mnt/tidehunter/run/package',
                  'package': {'source_sha256': 'a' * 64, 'source_revision': 'b' * 40,
                              'object_key': 'runs/run/package.tar', 'generation': '1', 'sha256': 'c' * 64},
                  'spec': {'backends': ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                                        'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'valkey_rdma',
                                        'garnet', 'kvrocks']}}
        self.assertIs(runtime.validate_runtime(config), config)
        config['spec']['backends'].append('tidehunter_efa')
        with self.assertRaises(runtime.RuntimeSafetyError):
            runtime.validate_runtime(config)

    def test_industry_stores_do_not_request_rdma(self):
        config = {'spec': {'backends': ['garnet', 'kvrocks']}}
        with patch('subprocess.run') as run, patch('subprocess.check_output') as output:
            proof = runtime.admit_fabrics(config, 0, {}, '/tmp')
        self.assertFalse(proof['rdma_requested'])
        self.assertFalse(proof['valkey_rdma_requested'])
        self.assertFalse(proof['nvme_rdma_requested'])
        run.assert_not_called()
        output.assert_not_called()


if __name__ == '__main__':
    unittest.main()
