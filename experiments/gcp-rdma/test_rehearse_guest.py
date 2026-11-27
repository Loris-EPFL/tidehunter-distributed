"""The rehearsal guest's recorded module admission.

A container cannot load the campaign's 5.14 kernel module. `admit_nvmet_fix`
therefore keeps the static four-file prefix admission and records the pinned
identity instead. These tests pin both halves: the static proof must match the
selected candidate, and the recorded receipt must carry the campaign's build
identity so the worker's readiness object stays truthful.
"""
import types
import unittest
from pathlib import Path
from unittest import mock

import rehearse_guest


def proof():
    return {'module_sha256': 'a' * 64, 'patch_sha256': 'b' * 64,
            'source_sha256': 'c' * 64, 'abi_sha256': 'd' * 64,
            'kernel_release': '5.14.0-687.49.1+2.1.el9_8_ciq.x86_64',
            'candidate_srcversion': '9F99AF855591751D53D463D',
            'candidate_build_id': '61aabd36adb3d8a51feb01cd289410d958c68c20',
            'scope': 'offline ABI admission only'}


class RecordedModuleAdmission(unittest.TestCase):
    def runtime(self):
        return {'run_id': 'bench-rehearsal', 'package_root': '/mnt/tidehunter/x/package',
                'package': {'manifest_sha256': 'f' * 64}}

    def package(self, candidate=None):
        return {'manifest_sha256': 'f' * 64,
                'nvmet_fix': proof() if candidate is None else candidate}

    def call(self, runtime, package, observed=None):
        value = dict(proof() if observed is None else observed)
        module = types.SimpleNamespace(validate_prefix=lambda prefix: dict(value))
        cloud_package = types.SimpleNamespace(nvmet_fix_module=lambda source: module)
        with mock.patch.dict('sys.modules', {'cloud_package': cloud_package}):
            return rehearse_guest.admit_nvmet_fix(
                runtime, {'name': 'bench-0', 'id': '9100'}, package)

    def test_a_package_without_the_fix_records_nothing(self):
        self.assertIsNone(self.call(self.runtime(), {'manifest_sha256': 'f' * 64}))

    def test_the_recorded_identity_carries_the_pinned_module_and_guest(self):
        result = self.call(self.runtime(), self.package())
        self.assertEqual(result['candidate'], proof())
        loaded = result['loaded']
        self.assertEqual(loaded['loaded_nvmet'],
                         {'srcversion': '9F99AF855591751D53D463D',
                          'build_id': '61aabd36adb3d8a51feb01cd289410d958c68c20'})
        self.assertEqual(loaded['guest'],
                         {'instance_name': 'bench-0', 'instance_id': '9100',
                          'run_id': 'bench-rehearsal'})
        self.assertEqual(loaded['module_sha256'], 'a' * 64)

    def test_a_manifest_mismatch_is_refused(self):
        package = self.package()
        package['manifest_sha256'] = 'e' * 64
        with self.assertRaises(ValueError):
            self.call(self.runtime(), package)

    def test_a_staged_prefix_that_differs_from_the_candidate_is_refused(self):
        observed = dict(proof(), module_sha256='0' * 64)
        with self.assertRaises(ValueError):
            self.call(self.runtime(), self.package(), observed)


class RecordedKernelAdmission(unittest.TestCase):
    def prepared(self):
        return {'bench-0': {'nvmet_fix': {'loaded': {'loaded_nvmet': {
            'srcversion': '9F99AF855591751D53D463D',
            'build_id': '61aabd36adb3d8a51feb01cd289410d958c68c20'}}}}}

    def test_the_recorded_initiator_passes_the_real_gate(self):
        import gcp_manual_compute
        admission = rehearse_guest.record_kernel_admission(self.prepared(), 'rdma')
        gcp_manual_compute.require_initiator(admission, live_transport='rdma')
        self.assertEqual(admission['initiator']['device'], '/dev/nvme-fabrics')
        record = admission['hosts']['bench-0']
        self.assertEqual(record['nvmet_srcversion'], '9F99AF855591751D53D463D')
        self.assertEqual(set(record['initiator']['modules']),
                         {'nvme_fabrics', 'nvme_tcp', 'rdma_cm', 'nvme_rdma'})

    def test_a_worker_without_the_pinned_module_is_refused(self):
        prepared = self.prepared()
        prepared['bench-0']['nvmet_fix']['loaded']['loaded_nvmet']['srcversion'] = 'stock'
        with self.assertRaises(ValueError):
            rehearse_guest.record_kernel_admission(prepared, 'tcp')


if __name__ == '__main__':
    unittest.main()
