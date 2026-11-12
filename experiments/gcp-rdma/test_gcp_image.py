import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import gcp_image
import gcp_image_qualify
import gcp_image_receipt


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
    def test_rdma_capabilities_are_required_without_a_threshold(self):
        fields = {name: '0' for name in (
            'max_mr', 'max_qp', 'max_qp_wr', 'max_cq', 'max_cqe',
            'max_pd', 'max_sge', 'max_sge_rd', 'max_mr_size', 'page_size_cap')}
        fields['max_cqe'] = '0xffff'
        evidence = {'capabilities': {'available': True, 'fields': fields}}
        gcp_image_qualify.require_rdma_capabilities(evidence)
        del fields['max_mr']
        with self.assertRaisesRegex(ValueError, 'capabilities unavailable'):
            gcp_image_qualify.require_rdma_capabilities(evidence)
        fields['max_mr'] = 'unknown'
        with self.assertRaises(ValueError):
            gcp_image_qualify.require_rdma_capabilities(evidence)
        fields['max_mr'] = '10'
        evidence['capabilities']['available'] = False
        with self.assertRaises(ValueError):
            gcp_image_qualify.require_rdma_capabilities(evidence)

    def test_focused_container_inventory_excludes_external_store_images(self):
        focused = gcp_image.required_containers(focused=True)
        full = gcp_image.required_containers()
        self.assertTrue(focused)
        self.assertTrue(focused < full)

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

    def test_selected_nvme_image_modules_are_required_on_both_guests(self):
        modules = {'nvmet': 'modinfo', 'nvme_fabrics': 'modinfo', 'rdma_cm': 'modinfo',
                   'nvme_rdma': 'modinfo', 'nvmet_rdma': 'modinfo'}
        records = [{'outputs': {'nvme_modules': dict(modules)}} for _ in range(2)]
        qualification = {'_records': records}
        gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'])
        with self.assertRaisesRegex(ValueError, 'NVMe transport modules'):
            gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'], live=True)
        with self.assertRaisesRegex(ValueError, 'NVMe transport modules'):
            gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_tcp'])
        for record in records:
            record['outputs']['nvme_modules'].update(nvme_tcp='modinfo', nvmet_tcp='modinfo')
        gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'], live=True)
        gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'])
        for record in records:
            for name in ('nvme_tcp', 'nvmet_tcp'):
                value = record['outputs']['nvme_modules'].pop(name)
                with self.assertRaisesRegex(ValueError, 'NVMe transport modules'):
                    gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'])
                record['outputs']['nvme_modules'][name] = value
        del records[0]['outputs']['nvme_modules']['nvme_fabrics']
        with self.assertRaisesRegex(ValueError, 'NVMe transport modules'):
            gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'])
        records[0]['outputs']['nvme_modules']['nvme_fabrics'] = 'modinfo'
        del records[1]['outputs']['nvme_modules']['nvmet_rdma']
        with self.assertRaisesRegex(ValueError, 'NVMe transport modules'):
            gcp_image.admit_nvme_transports(qualification, ['tidehunter_nvme_rdma'])
        gcp_image.admit_nvme_transports({}, ['native'])

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
                                'guest_runtime': {'executable': '/opt/tidehunter-python/bin/python3',
                                                  'python': '3.11.13', 'yaml': '6.0.2',
                                                  'nofile_soft': 1024, 'nofile_hard': 524288},
                                'memlock': {'soft': 'infinity', 'hard': 'infinity', 'docker': 'infinity'},
                                'io_uring_workload': {'jobs': [{'error': 0, 'read': {'io_bytes': 4096}}]},
                                'nvme_ioctl': {'local-ssd-0': 'NVMe Identify Controller'},
                                'package': {'manifest_sha256': runtime, 'runtime_checked': True,
                                            'abi': {'needed': ['libfabric.so.1'], 'version_requirements': defs},
                                            'fabric_library': fabric, 'linkage': linkage,
                                            'valkey_runtime_linkage': {'valkey-server': linkage, 'valkey-cli': linkage, 'valkey-benchmark': linkage}},
                                'libfabric_sha256': 'a' * 64, 'fi_info_fabric': fabric,
                                'fi_info_ldd': 'libfabric.so.1 => /lib/libfabric.so.1',
                                'libfabric_api_version': 65541, 'kernel': '6.12',
                                'installed_rpms': ['kmod-idpf-irdma-0:1-1.x86_64'],
                                'libfabric_version': 'fi_info: 2.6.0',
                                'providers': {'verbs': 'provider: verbs;ofi_rxm', 'tcp': 'provider: tcp;ofi_rxm'},
                                'rdma_links': 'link irdma0/1 state ACTIVE',
                                'nvme_modules': {x: 'filename: module' for x in ('irdma', 'idpf', 'rdma_cm', 'nvme', 'nvmet', 'nvme_fabrics', 'nvme_rdma', 'nvmet_rdma')},
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
            with self.assertRaisesRegex(ValueError, 'proof list'):
                gcp_image.validate_pair(records, identity, runtime, self.config['service_account'],
                                        qualification_profile='focused')
            focused = copy.deepcopy(records)
            for item in focused:
                item['qualification_proofs'] = list(gcp_image.FOCUSED_PROOFS)
                del item['outputs']['valkey']
                del item['outputs']['package']['valkey_runtime_linkage']
                item['outputs']['package']['build_profile'] = 'focused'
                del item['outputs']['package']['linkage']['resolved']['/lib/libfdb_c.so']
            gcp_image.validate_pair(focused, identity, runtime, self.config['service_account'],
                                    qualification_profile='focused')
            gcp_image.admit_abi({'_records': focused}, {'needed': ['libfabric.so.1'],
                'version_requirements': {'GLIBC': ['2.34'], 'GLIBCXX': [], 'CXXABI': [], 'OPENSSL': []}})
            with self.assertRaisesRegex(ValueError, 'focused proof'):
                gcp_image.validate_pair(focused, identity, runtime, self.config['service_account'])
            wrong = copy.deepcopy(focused)
            wrong[0]['qualification_proofs'].append('valkey')
            with self.assertRaisesRegex(ValueError, 'proof list'):
                gcp_image.validate_pair(wrong, identity, runtime, self.config['service_account'],
                                        qualification_profile='focused')
            wrong = copy.deepcopy(focused)
            wrong[0]['outputs']['valkey'] = records[0]['outputs']['valkey']
            with self.assertRaisesRegex(ValueError, 'Valkey proof'):
                gcp_image.validate_pair(wrong, identity, runtime, self.config['service_account'],
                                        qualification_profile='focused')
            wrong = copy.deepcopy(focused)
            del wrong[0]['outputs']['nvme_modules']['nvmet']
            with self.assertRaisesRegex(ValueError, 'target modules'):
                gcp_image.validate_pair(wrong, identity, runtime, self.config['service_account'],
                                        qualification_profile='focused')
            wrong = copy.deepcopy(focused)
            wrong[0]['outputs']['rdma_read'] = 'invalid'
            with self.assertRaisesRegex(ValueError, 'perftest'):
                gcp_image.validate_pair(wrong, identity, runtime, self.config['service_account'],
                                        qualification_profile='focused')
            with tempfile.TemporaryDirectory() as directory:
                entries = []
                for index, item in enumerate(focused):
                    data = json.dumps(item).encode()
                    path = Path(directory) / str(index)
                    path.write_bytes(data)
                    entries.append({'file': str(path), 'reference': {
                        'bucket': 'artifacts', 'object': f'focused-{index}', 'generation': '9',
                        'sha256': hashlib.sha256(data).hexdigest()}})
                focus_receipt = gcp_image_receipt.make_receipt(
                    {'image': identity, 'runtime_sha256': runtime, 'service_account': self.config['service_account'],
                     'evidence': entries}, qualification_profile='focused')
                self.assertEqual(focus_receipt['qualification_proofs'], list(gcp_image.FOCUSED_PROOFS))
                focus_api = self.put_receipt(focus_receipt)
                for index, entry in enumerate(entries):
                    focus_api.store.objects[(f'focused-{index}', '9')] = Path(entry['file']).read_bytes()
                focus_config = {**self.config, 'qualification_profile': 'focused'}
                self.assertEqual(gcp_image.admit_image(focus_api, focus_config, self.image)['_records'], focused)
                with self.assertRaisesRegex(ValueError, 'focused receipt'):
                    gcp_image.admit_image(focus_api, self.config, self.image)
                bad = dict(focus_receipt, qualification_proofs=['valkey'])
                bad_api = self.put_receipt(bad)
                with self.assertRaisesRegex(ValueError, 'proof list'):
                    gcp_image.admit_image(bad_api, {**self.config, 'qualification_profile': 'focused'}, self.image)
                self.put_receipt(receipt)
            guest = records[0]['outputs']['guest_runtime']
            for field, value in (('executable', '/usr/bin/python3.11'), ('yaml', None),
                                 ('nofile_hard', 1024)):
                original = guest[field]
                guest[field] = value
                with self.assertRaisesRegex(ValueError, 'guest Python'):
                    gcp_image.validate_pair(records, identity, runtime, self.config['service_account'])
                guest[field] = original
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
