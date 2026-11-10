import json
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import kernel_fix as fix


class StaticAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.prefix = Path(self.tmp.name)
        for name in fix.FILES:
            (self.prefix / name).write_text('{}')
        (self.prefix / 'source.json').write_text(json.dumps(fix.SOURCE))

    def test_unreviewed_receipt_is_rejected_before_modinfo(self):
        with mock.patch.object(fix, 'elf_modinfo') as info:
            with self.assertRaisesRegex(ValueError, 'receipt digest'):
                fix.validate_prefix(self.prefix)
            info.assert_not_called()

    def test_readelf_modinfo_parser_preserves_values_and_rejects_ambiguity(self):
        text = "String dump of section '.modinfo':\n  [     0]  name=nvmet\n  [    12]  vermagic=release SMP \n"
        with mock.patch.object(fix, 'run', return_value=text) as run:
            self.assertEqual(fix.elf_modinfo('module.ko', ('name', 'vermagic')),
                             {'name': 'nvmet', 'vermagic': 'release SMP '})
            run.assert_called_once_with('readelf', '-p', '.modinfo', 'module.ko', strip=False)
        with mock.patch.object(fix, 'run', return_value=text + '  [    30]  name=other\n'):
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                fix.elf_modinfo('module.ko', ('name', 'vermagic'))
        with mock.patch.object(fix, 'run', return_value=text):
            with self.assertRaisesRegex(ValueError, 'missing'):
                fix.elf_modinfo('module.ko', ('name', 'license'))

    def test_source_pin_and_extra_file_are_rejected(self):
        source = dict(fix.SOURCE, signing_key='bad')
        (self.prefix / 'source.json').write_text(json.dumps(source))
        with self.assertRaisesRegex(ValueError, 'source manifest'):
            fix.validate_prefix(self.prefix)
        (self.prefix / 'extra').touch()
        with self.assertRaisesRegex(ValueError, 'exactly'):
            fix.validate_prefix(self.prefix)

    def test_symlink_is_rejected(self):
        (self.prefix / 'nvmet.ko').unlink()
        (self.prefix / 'nvmet.ko').symlink_to(self.prefix / 'source.json')
        with self.assertRaisesRegex(ValueError, 'not links'):
            fix.validate_prefix(self.prefix)


class RuntimeBoundaryTests(unittest.TestCase):
    def test_loaded_build_id_is_read_from_kernel_note(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            module = root / 'nvmet'
            (module / 'notes').mkdir(parents=True)
            (module / 'srcversion').write_text(fix.CANDIDATE_SRC + '\n')
            value = bytes.fromhex(fix.CANDIDATE_BUILD)
            (module / 'notes/.note.gnu.build-id').write_bytes(struct.pack('<III', 4, len(value), 3) + b'GNU\0' + value)
            with mock.patch.object(fix, 'SYS_MODULE', root):
                self.assertEqual(fix.loaded_identity('nvmet'), {'srcversion': fix.CANDIDATE_SRC, 'build_id': fix.CANDIDATE_BUILD})
                (module / 'notes/.note.gnu.build-id').write_bytes(b'bad')
                with self.assertRaisesRegex(ValueError, 'note missing'):
                    fix.loaded_identity('nvmet')

    def test_non_idle_holders_and_refs_are_refused(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            module = root / 'nvmet'
            (module / 'holders').mkdir(parents=True)
            (module / 'holders/nvmet_rdma').touch()
            (module / 'refcnt').write_text('2')
            with mock.patch.object(fix, 'SYS_MODULE', root):
                with self.assertRaisesRegex(ValueError, 'not idle'):
                    fix.idle('nvmet', ['nvmet_rdma'])
                (module / 'refcnt').write_text('1')
                self.assertTrue(fix.idle('nvmet', ['nvmet_rdma']))
                with self.assertRaisesRegex(ValueError, 'not idle'):
                    fix.idle('nvmet')

    def test_policy_rejects_secure_boot_and_unknown_lockdown(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            config = root / 'config'
            config.write_text('# CONFIG_MODULE_SIG_FORCE is not set\nCONFIG_SECURITY_LOCKDOWN_LSM=y\n')
            cmdline = root / 'cmdline'
            cmdline.write_text('quiet')
            lockdown = root / 'lockdown'
            lockdown.write_text('[none] integrity confidentiality')
            efi = root / 'efi'
            (efi / 'efivars').mkdir(parents=True)
            secure = efi / 'efivars/SecureBoot-test'
            secure.write_bytes(b'\x07\x00\x00\x00\x00')
            paths = {'/boot/config-' + fix.RELEASE: config, '/proc/cmdline': cmdline,
                     '/sys/kernel/security/lockdown': lockdown, '/sys/firmware/efi': efi}
            with mock.patch.object(fix, 'Path', side_effect=lambda p: paths[str(p)]), \
                 mock.patch.object(fix, 'SYS_MODULE', root / 'modules'):
                self.assertFalse(fix.policy()['secure_boot'])
                secure.write_bytes(b'\x07\x00\x00\x00\x01')
                with self.assertRaisesRegex(ValueError, 'Secure Boot'):
                    fix.policy()
                secure.write_bytes(b'\x07\x00\x00\x00\x00')
                lockdown.unlink()
                with self.assertRaisesRegex(ValueError, 'lockdown state unavailable'):
                    fix.policy()

    def test_configfs_and_fabric_controllers_block_install(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            config = root / 'config'
            (config / 'ports').mkdir(parents=True)
            (config / 'subsystems').mkdir()
            controllers = root / 'controllers'
            controllers.mkdir()
            paths = {'/sys/kernel/config/nvmet': config, '/sys/class/nvme': controllers}
            with mock.patch.object(fix, 'Path', side_effect=lambda p: paths[str(p)]):
                fix.quiescent()
                (config / 'ports/1').mkdir()
                with self.assertRaisesRegex(ValueError, 'configfs'):
                    fix.quiescent()
                (config / 'ports/1').rmdir()
                (controllers / 'nvme0').mkdir()
                (controllers / 'nvme0/transport').write_text('rdma')
                with self.assertRaisesRegex(ValueError, 'fabrics'):
                    fix.quiescent()

    def test_metadata_requires_exact_owned_identity(self):
        values = iter(['bench-0123456789abcdef-0', 'bench-0123456789abcdef', 'b' * 32, '123'])
        with mock.patch.object(fix, 'metadata', side_effect=lambda _: next(values)):
            self.assertEqual(fix.guest_identity()['instance_id'], '123')
        values = iter(['other-0', 'bench-0123456789abcdef', 'b' * 32, '123'])
        with mock.patch.object(fix, 'metadata', side_effect=lambda _: next(values)):
            with self.assertRaisesRegex(ValueError, 'benchmark-owned'):
                fix.guest_identity()

    def test_active_candidate_readmission_is_read_only(self):
        candidate = {'srcversion': fix.CANDIDATE_SRC, 'build_id': fix.CANDIDATE_BUILD}
        transport = {'srcversion': 'rdma-src', 'build_id': 'rdma'}
        stock = {'nvmet-rdma': {'build_id': 'rdma', 'modinfo': {'srcversion': 'rdma-src'}},
                 'nvmet-tcp': {'build_id': 'tcp', 'modinfo': {'srcversion': 'tcp-src'}}}
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / 'abi-admission.json').write_text('{}')
            with mock.patch.object(fix.os, 'geteuid', return_value=0), \
                 mock.patch.object(fix.os, 'uname', return_value=mock.Mock(release=fix.RELEASE)), \
                 mock.patch.object(fix, 'trusted_prefix', return_value=Path(d)), \
                 mock.patch.object(fix, 'validate_prefix', return_value={}), \
                 mock.patch.object(fix, 'guest_identity', return_value={}), \
                 mock.patch.object(fix, 'policy', return_value={}), \
                 mock.patch.object(fix, 'provider_identities', return_value={}), \
                 mock.patch.object(fix, 'stock_identities', return_value=stock), \
                 mock.patch.object(fix, 'present_identity', side_effect=lambda n: {'nvmet': candidate, 'nvmet_rdma': transport}.get(n)), \
                 mock.patch.object(fix, 'quiescent') as quiescent, \
                 mock.patch.object(fix, 'idle') as idle, mock.patch.object(fix, 'run') as run:
                proof = fix.install(d)
                self.assertTrue(proof['already_loaded'])
                self.assertEqual(proof['loaded_nvmet'], candidate)
                quiescent.assert_not_called()
                idle.assert_not_called()
                run.assert_not_called()

    def test_timeout_after_module_side_effect_restores_original_state(self):
        stock = {'nvmet': {'build_id': 'core', 'modinfo': {'srcversion': fix.STOCK_SRC}},
                 'nvmet-rdma': {'build_id': 'rdma', 'modinfo': {'srcversion': 'rdma-src'}},
                 'nvmet-tcp': {'build_id': 'tcp', 'modinfo': {'srcversion': 'tcp-src'}}}
        original = {'nvmet': {'srcversion': fix.STOCK_SRC, 'build_id': 'core'},
                    'nvmet_rdma': {'srcversion': 'rdma-src', 'build_id': 'rdma'}}
        for timeout_on in (('rmmod', 'nvmet_rdma'), ('rmmod', 'nvmet'), ('insmod',)):
            with self.subTest(timeout_on=timeout_on), tempfile.TemporaryDirectory() as d:
                state = dict(original)
                calls = []
                timed_out = [False]
                def run(*args):
                    calls.append(args)
                    if args[0] == 'rmmod':
                        del state[args[1]]
                    elif args[0] == 'insmod':
                        state['nvmet'] = {'srcversion': fix.CANDIDATE_SRC, 'build_id': fix.CANDIDATE_BUILD}
                    elif args[0] == 'modprobe' and args[1] in original:
                        state[args[1]] = original[args[1]]
                    if args[:len(timeout_on)] == timeout_on and not timed_out[0]:
                        timed_out[0] = True
                        raise subprocess.TimeoutExpired(args, 30)
                    return ''
                (Path(d) / 'abi-admission.json').write_text('{}')
                with mock.patch.object(fix.os, 'geteuid', return_value=0), \
                     mock.patch.object(fix.os, 'uname', return_value=mock.Mock(release=fix.RELEASE)), \
                     mock.patch.object(fix, 'trusted_prefix', return_value=Path(d)), \
                     mock.patch.object(fix, 'validate_prefix', return_value={}), \
                     mock.patch.object(fix, 'guest_identity', return_value={}), \
                     mock.patch.object(fix, 'policy', return_value={}), \
                     mock.patch.object(fix, 'provider_identities', return_value={}), \
                     mock.patch.object(fix, 'stock_identities', return_value=stock), \
                     mock.patch.object(fix, 'quiescent'), \
                     mock.patch.object(fix, 'idle', side_effect=lambda name, allowed_holders=(): name in state), \
                     mock.patch.object(fix, 'loaded_identity', side_effect=lambda name: state[name]), \
                     mock.patch.object(fix, 'present_identity', side_effect=lambda name: state.get(name)), \
                     mock.patch.object(fix, 'run', side_effect=run):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        fix.install(d)
                self.assertEqual(state, original)
                self.assertTrue(timed_out[0])

    def test_rollback_never_removes_unknown_core(self):
        stock = {'nvmet': {'build_id': 'core'},
                 'nvmet-rdma': {'build_id': 'rdma', 'modinfo': {'srcversion': 'rdma-src'}},
                 'nvmet-tcp': {'build_id': 'tcp', 'modinfo': {'srcversion': 'tcp-src'}}}
        with mock.patch.object(fix, 'quiescent'), \
             mock.patch.object(fix, 'present_identity', return_value={'srcversion': 'unknown', 'build_id': 'unknown'}), \
             mock.patch.object(fix, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'unknown nvmet'):
                fix.rollback_stock(True, [], stock)
            run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
