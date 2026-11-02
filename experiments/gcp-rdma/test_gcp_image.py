import hashlib
import json
import unittest
from unittest.mock import patch

import gcp_image


class Store:
    def __init__(self, objects):
        self.objects = objects

    def get(self, key, *, generation, max_bytes):
        data = self.objects[(key, generation)]
        if len(data) > max_bytes:
            raise ValueError('oversized')
        return data


class Api:
    def __init__(self, objects):
        self.store = Store(objects)

    def objects(self, bucket):
        return self.store


class ImageAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.config = {'project_id': 'mysten-research', 'bucket': 'artifacts',
                       'service_account': 'guest@example.com',
                       'image': 'projects/mysten-research/global/images/reviewed-image'}
        self.image = {'status': 'READY', 'id': '123',
                      'selfLink': 'https://www.googleapis.com/compute/v1/' + self.config['image']}

    def put_receipt(self, receipt):
        data = json.dumps(receipt).encode()
        self.config['image_qualification'] = {'bucket': 'artifacts', 'object': 'receipt',
                                              'generation': '8', 'sha256': hashlib.sha256(data).hexdigest()}
        return Api({('receipt', '8'): data})

    def test_missing_qualification_rejected(self):
        with self.assertRaisesRegex(ValueError, 'not configured'):
            gcp_image.admit_image(Api({}), self.config, self.image)

    def test_ready_alone_is_not_qualification(self):
        api = self.put_receipt({'schema_version': 1, 'status': 'READY'})
        with self.assertRaises(ValueError):
            gcp_image.admit_image(api, self.config, self.image)

    def test_object_digest_is_checked(self):
        api = self.put_receipt({})
        self.config['image_qualification']['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            gcp_image.admit_image(api, self.config, self.image)

    def test_receipt_must_bind_numeric_image_id(self):
        api = self.put_receipt({'schema_version': 1, 'image': {'path': self.config['image'], 'id': '124'}})
        with self.assertRaisesRegex(ValueError, 'numeric ID'):
            gcp_image.admit_image(api, self.config, self.image)

    def test_public_image_rejected(self):
        self.config['image'] = 'projects/rocky-linux-cloud/global/images/rocky-linux-10-optimized-gcp-v20261026'
        api = self.put_receipt({})
        with self.assertRaisesRegex(ValueError, 'custom image'):
            gcp_image.admit_image(api, self.config, self.image)

    def test_generation_must_be_immutable_decimal(self):
        api = self.put_receipt({})
        self.config['image_qualification']['generation'] = 8
        with self.assertRaisesRegex(ValueError, 'generation'):
            gcp_image.admit_image(api, self.config, self.image)

    def test_container_digest_normalizes_docker_hub_but_not_repository(self):
        digest = 'a' * 64
        self.assertEqual(gcp_image.canonical_image_digest('library/cassandra:5@sha256:' + digest),
                         gcp_image.canonical_image_digest('docker.io/library/cassandra@sha256:' + digest))
        self.assertNotEqual(gcp_image.canonical_image_digest('other/cassandra@sha256:' + digest),
                            gcp_image.canonical_image_digest('cassandra@sha256:' + digest))

    def test_reciprocal_proofs_admitted_and_tampering_rejected(self):
        container = 'example/image:v1@sha256:' + 'b' * 64
        recipes = {'prepare_sha256': 'c' * 64, 'qualify_sha256': 'd' * 64}
        runtime = 'e' * 64
        identity = {'path': self.config['image'], 'id': '123'}
        def record(instance, peer):
            value = 'f' * 32
            local_ip, peer_ip = (('10.20.0.1', '10.20.0.2') if instance == '101' else ('10.20.0.2', '10.20.0.1'))
            fabric = '/lib/libfabric.so.1'
            defs = {'GLIBC': ['2.34'], 'GLIBCXX': [], 'CXXABI': [], 'OPENSSL': []}
            resolved = {fabric: {'identity': {'sha256': 'a' * 64}, 'soname': 'libfabric.so.1', 'version_definitions': defs},
                        '/lib/libfdb_c.so': {'identity': {'sha256': 'a' * 64}, 'soname': 'libfdb_c.so', 'version_definitions': defs}}
            linkage = {'ldd': 'libfabric.so.1 => /lib/libfabric.so.1', 'resolved': resolved}
            latency = 'rdma_cm QPs : ON\n#bytes #iterations t_min t_max t_typical t_avg t_stdev p99 p999\n1024 1000 7.65 11.12 7.72 7.75 0.12 8.00 11.12'
            return {'kind': 'guest-diagnostic', 'image': identity, 'recipes': recipes,
                    'instance_id': instance, 'peer_instance_id': peer,
                    'service_account': self.config['service_account'], 'rdma_device': 'irdma0',
                    'scratch_devices': {'local-ssd-0': '/dev/nvme0n1'}, 'local_rdma': local_ip, 'peer': peer_ip,
                    'outputs': {'io_uring': 'io_uring_setup(2) succeeded',
                                'memlock': {'soft': 'infinity', 'hard': 'infinity', 'docker': 'infinity'},
                                'io_uring_workload': {'jobs': [{'error': 0, 'read': {'io_bytes': 4096}}]},
                                'nvme_ioctl': {'local-ssd-0': 'NVMe Identify Controller'},
                                'package': {'manifest_sha256': runtime, 'runtime_checked': True,
                                            'abi': {'needed': ['libfabric.so.1'], 'version_requirements': defs},
                                            'fabric_library': fabric, 'linkage': linkage,
                                            'valkey_runtime_linkage': {'valkey-server': linkage, 'valkey-cli': linkage}},
                                'libfabric_sha256': 'a' * 64, 'fi_info_fabric': fabric,
                                'fi_info_ldd': 'libfabric.so.1 => /lib/libfabric.so.1',
                                'libfabric_api_version': 65541, 'kernel': '6.12',
                                'installed_rpms': ['kmod-idpf-irdma-0:1-1.x86_64'],
                                'libfabric_version': 'fi_info: 2.6.0',
                                'providers': {'verbs': 'provider: verbs;ofi_rxm', 'tcp': 'provider: tcp;ofi_rxm'},
                                'rdma_links': 'link irdma0/1 state ACTIVE',
                                'nvme_modules': {x: 'filename: module' for x in ('irdma', 'idpf', 'rdma_cm', 'nvme', 'nvme_rdma', 'nvmet_rdma')},
                                'openssl': 'OpenSSL 3.2', 'python': '3.12.0',
                                'containers': {container: [{'RepoDigests': ['docker.io/example/image@sha256:' + 'b' * 64]}]},
                                'rdma_commands': {op: ['ib_' + op + '_lat', '-R', '-d', 'irdma0', '-p', port, '-s', '1024', '-n', '1000', peer_ip]
                                                  for op, port in (('read', '18515'), ('send', '18516'))},
                                'rdma_read': latency, 'rdma_send': latency,
                                'valkey': {'endpoint': peer_ip + ':6380', 'value': value,
                                           'get': value, 'set': 'OK', 'del': '1'}}}
        records = [record('101', '102'), record('102', '101')]
        objects, references = {}, []
        for index, item in enumerate(records):
            data = json.dumps(item).encode()
            key = f'evidence-{index}'
            objects[(key, '9')] = data
            references.append({'bucket': 'artifacts', 'object': key, 'generation': '9',
                               'sha256': hashlib.sha256(data).hexdigest()})
        receipt = {'schema_version': 1, 'image': identity, 'recipes': recipes,
                   'runtime_sha256': runtime, 'evidence': references}
        api = self.put_receipt(receipt)
        api.store.objects.update(objects)
        with patch.object(gcp_image, 'recipe_hashes', return_value=recipes), \
             patch.object(gcp_image, 'required_containers', return_value={container}):
            self.assertEqual(gcp_image.admit_image(api, self.config, self.image), {**receipt, '_records': records})
            qualification = {'_records': records}
            versions = {'GLIBC': ['2.34'], 'GLIBCXX': [], 'CXXABI': [], 'OPENSSL': []}
            gcp_image.admit_abi(qualification, {'needed': ['libfabric.so.1'], 'version_requirements': versions})
            with self.assertRaisesRegex(ValueError, 'SONAME'):
                gcp_image.admit_abi(qualification, {'needed': ['libmissing.so'], 'version_requirements': versions})
            with self.assertRaisesRegex(ValueError, 'GLIBC versions'):
                gcp_image.admit_abi(qualification, {'needed': ['libfabric.so.1'], 'version_requirements': {**versions, 'GLIBC': ['2.35']}})
            with self.assertRaisesRegex(ValueError, 'families'):
                gcp_image.admit_abi(qualification, {'needed': ['libfabric.so.1'], 'version_requirements': {**versions, 'OTHER': []}})
            records[1]['peer_instance_id'] = '103'
            with self.assertRaisesRegex(ValueError, 'reciprocal'):
                gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])
            records[1]['peer_instance_id'] = '101'
            records[0]['outputs']['rdma_read'] = 'success'
            with self.assertRaisesRegex(ValueError, 'perftest'):
                gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])
            output = records[1]['outputs']['rdma_read']
            for value in ('0', 'nan', '...'):
                records[0]['outputs']['rdma_read'] = output.replace('7.65', value)
                with self.assertRaises(ValueError):
                    gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])
            records[0]['outputs']['rdma_read'] = output
            records[0]['outputs']['package']['linkage'] = {}
            with self.assertRaisesRegex(ValueError, 'linkage'):
                gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])
            records[0]['outputs']['package']['linkage'] = records[1]['outputs']['package']['linkage']
            records[0]['outputs']['fi_info_fabric'] = '/wrong/libfabric.so.1'
            with self.assertRaisesRegex(ValueError, 'fi_info'):
                gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])


if __name__ == '__main__':
    unittest.main()
