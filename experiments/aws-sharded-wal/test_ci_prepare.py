import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import ci_prepare
import ci_campaign


class PreparationAdmissionTests(unittest.TestCase):
    def environment(self):
        return {'GITHUB_REPOSITORY': ci_prepare.REPOSITORY,
                'GITHUB_REF': 'refs/heads/' + ci_prepare.BRANCH,
                'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': 'a' * 40}

    def test_untrusted_ref_or_event_cannot_reach_preparation_even_when_disarmed(self):
        for change in ({'GITHUB_REF': 'refs/heads/main'}, {'GITHUB_EVENT_NAME': 'pull_request'},
                       {'GITHUB_REPOSITORY': 'someone/tidehunter-distributed'}, {'GITHUB_SHA': 'main'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                ci_prepare.settings(self.environment() | change)

    def test_missing_activation_reports_disarmed_instead_of_claiming_readiness(self):
        self.assertEqual(ci_prepare.settings(self.environment()), {
            'enabled': False, 'reason': 'AWS benchmarks are disarmed; no AWS operation or build requested'})
        with self.assertRaisesRegex(ValueError, 'missing GitHub environment'):
            ci_prepare.settings(self.environment() | {'AWS_BENCH_ENABLED': 'true'})

    def test_tampered_fixture_never_creates_an_extraction_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / 'fixture.tar.gz'
            archive.write_bytes(b'tampered checkpoint producer')
            output = root / 'extract'
            with self.assertRaisesRegex(ValueError, 'SHA-256'):
                ci_prepare.unpack_fixture(archive, output)
            self.assertFalse(output.exists())

    def test_hash_admitted_archive_still_rejects_links_and_traversal(self):
        for name, kind, link in [('fixture/../../escape', tarfile.REGTYPE, ''),
                                 ('fixture/producer/link', tarfile.SYMTYPE, '/etc/passwd')]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive = root / 'fixture.tar.gz'
                with tarfile.open(archive, 'w:gz') as stream:
                    item = tarfile.TarInfo(name)
                    item.type = kind
                    item.linkname = link
                    stream.addfile(item)
                digest = hashlib.sha256(archive.read_bytes()).hexdigest()
                with patch.object(ci_prepare, 'FIXTURE_SHA256', digest), self.assertRaisesRegex(ValueError, 'unsafe'):
                    ci_prepare.unpack_fixture(archive, root / 'extract', expected=digest)
                self.assertFalse((root / 'extract').exists())
                self.assertFalse((root / 'escape').exists())

    def test_duplicate_fixture_names_do_not_overwrite_admitted_members(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / 'fixture.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                for payload in (b'first', b'overwritten'):
                    item = tarfile.TarInfo('fixture/producer/data')
                    item.size = len(payload)
                    stream.addfile(item, io.BytesIO(payload))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            with patch.object(ci_prepare, 'FIXTURE_SHA256', digest), self.assertRaisesRegex(ValueError, 'duplicate'):
                ci_prepare.unpack_fixture(archive, root / 'extract', expected=digest)
            self.assertFalse((root / 'extract').exists())

    def test_external_or_incomplete_replay_cannot_be_packaged_as_local_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            (source / 'experiment').mkdir(parents=True)
            (source / 'evidence.tar.gz').write_bytes(b'bound evidence')
            evidence_sha = ci_prepare.sha(source / 'evidence.tar.gz')
            audit = {'all_equal': True, 'databases': {'db': True}, 'rocksdb': {'db': True}}
            (source / 'receipt.json').write_text(json.dumps({'complete': True, 'evidence_sha256': evidence_sha}))
            (source / 'experiment/audit.json').write_text(json.dumps(audit))
            for change in ({'complete': False}, {'external_manifest': {'transport': 'efa'}}, {'topology': 'six AWS hosts'}):
                result = {'complete': True, 'topology': 'five-process loopback', 'audit': audit} | change
                (source / 'experiment/results.json').write_text(json.dumps(result))
                with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'loopback replay'):
                    ci_prepare.verification_bundle(source, root / 'verification')
                self.assertFalse((root / 'verification').exists())


class FrozenSourceRestoreTests(unittest.TestCase):
    def inputs(self, root):
        inputs = root / 'prepared'
        build = inputs / 'build'
        sources = build / 'sources'
        sui = sources / 'sui'
        sui.mkdir(parents=True)
        (sui / 'Cargo.toml').write_text('[workspace]\n')
        (sui / 'run.sh').write_text('#!/bin/sh\nexit 0\n')
        (sui / 'run.sh').chmod(0o755)
        (sui / 'source-link').symlink_to('Cargo.toml')
        engine = sources / 'tidehunter-sharded-wal'
        for directory in ('aws-sharded-wal', 'sui-checkpoint'):
            target = engine / 'experiments' / directory
            target.mkdir(parents=True)
            for path in (ci_prepare.ROOT / 'experiments' / directory).glob('*.py'):
                if directory == 'sui-checkpoint' or not path.name.startswith('test_'):
                    shutil.copy2(path, target / path.name)
        identities = {}
        for owner in ('sui', 'tidehunter-sharded-wal'):
            base = sources / owner
            files = {}
            for path in base.rglob('*'):
                if path.is_symlink():
                    files[str(path.relative_to(base))] = {'link': str(path.readlink())}
                elif path.is_file():
                    files[str(path.relative_to(base))] = {
                        'sha256': ci_prepare.sha(path), 'mode': path.stat().st_mode & 0o777}
            identities[owner] = {'revision': 'a' * 40, 'files': files}
        ci_prepare.save(build / 'source-manifest.json', identities)
        with tarfile.open(build / 'sources.tar.gz', 'w:gz') as archive:
            archive.add(sources, arcname='sources')
        binaries = build / 'binaries'
        binaries.mkdir()
        commands = []
        for name in sorted(ci_campaign.BINARIES):
            binary = binaries / name
            binary.write_text('#!/bin/sh\nexit 0\n')
            binary.chmod(0o755)
            log, messages = build / f'build-{name}.log', build / f'build-{name}.jsonl'
            log.write_text('successful source-bound build\n')
            messages.write_text('{}\n')
            commands.append(dict(label=name, sha256=ci_prepare.sha(binary),
                log_sha256=ci_prepare.sha(log), messages_sha256=ci_prepare.sha(messages),
                started_ns=1, finished_ns=2))
        manifest = dict(protocol=8, fabric=True, commands=commands,
            binaries={path.name: ci_prepare.sha(path) for path in binaries.iterdir()},
            source_archive_sha256=ci_prepare.sha(build / 'sources.tar.gz'),
            source_manifest_sha256=ci_prepare.sha(build / 'source-manifest.json'))
        ci_prepare.save(build / 'build-manifest.json', manifest)
        for name in ('plan.json', 'portability.json'):
            ci_prepare.save(build / name, {})
        with tarfile.open(build / 'build-bundle.tar.gz', 'w:gz') as archive:
            for path in sorted(build.iterdir()):
                if path.name not in ('sources', 'build-bundle.tar.gz'):
                    archive.add(path, arcname=path.name)
        frozen = root / 'fixture'
        (frozen / 'producer').mkdir(parents=True)
        (frozen / 'producer/checkpoint').write_bytes(b'bound checkpoint fixture')
        (frozen / 'experiment').mkdir()
        source_range = {'first': 0, 'last': ci_prepare.TIP}
        ci_prepare.save(frozen / 'experiment/results.json', dict(
            source_frozen=True, checkpoint_limit=ci_prepare.TIP,
            checkpoint_digest=ci_prepare.CHECKPOINT, verified_source_range=source_range))
        fixture_files = {str(path.relative_to(frozen)): ci_prepare.sha(path)
                         for path in frozen.rglob('*') if path.is_file()}
        ci_prepare.save(frozen / 'fixture-manifest.json', dict(
            checkpoint_limit=ci_prepare.TIP, checkpoint_digest=ci_prepare.CHECKPOINT,
            source_range=source_range, files=fixture_files))
        fixture = root / 'fixture.tar.gz'
        with tarfile.open(fixture, 'w:gz') as archive:
            archive.add(frozen, arcname='fixture')
        verification = inputs / 'verification'
        experiment = verification / 'experiment'
        (experiment / 'native-schema').mkdir(parents=True)
        (experiment / 'native-schema/schema').write_bytes(b'bound native schema')
        audit = dict(all_equal=True, databases={'db': True}, rocksdb={'db': True})
        bindings = dict(build_manifest_sha256=ci_prepare.sha(build / 'build-manifest.json'),
                        source_archive_sha256=manifest['source_archive_sha256'])
        result = dict(bindings, complete=True, binaries=manifest['binaries'],
            checkpoint_digest=ci_prepare.CHECKPOINT, checkpoint_limit=ci_prepare.TIP,
            source_range=source_range, fixture_files=fixture_files,
            topology='five-process loopback', audit=audit,
            native={'completed_checkpoint_digest': ci_prepare.CHECKPOINT},
            remote={'completed_checkpoint_digest': ci_prepare.CHECKPOINT})
        ci_prepare.save(experiment / 'results.json', result)
        ci_prepare.save(experiment / 'audit.json', audit)
        with tarfile.open(verification / 'evidence.tar.gz', 'w:gz') as archive:
            archive.add(experiment, arcname='experiment')
        ci_prepare.save(verification / 'receipt.json', dict(bindings, complete=True,
            evidence_sha256=ci_prepare.sha(verification / 'evidence.tar.gz')))
        ci_prepare.save(inputs / 'READY.json', dict(complete=True,
            build_manifest_sha256=bindings['build_manifest_sha256']))
        return inputs, fixture

    def package(self, inputs, fixture, output):
        with patch.object(ci_prepare, 'FIXTURE_SHA256', ci_prepare.sha(fixture)):
            return ci_prepare.package_inputs(inputs, fixture, output)['archive_sha256']

    def test_package_restores_real_frozen_sources_and_passes_campaign_admission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs, fixture = self.inputs(root)
            package = root / 'deployment.tar.gz'
            digest = self.package(inputs, fixture, package)
            with tarfile.open(package) as archive:
                self.assertFalse(any(Path(member.name).parts[:2] == ('build', 'sources')
                                     for member in archive.getmembers()))
            shutil.rmtree(inputs)
            restored = root / 'fresh-vm'
            result = ci_prepare.restore_inputs(package, digest, restored)
            self.assertTrue(result['restored'])
            sources = restored / 'build/sources'
            self.assertEqual((sources / 'sui/Cargo.toml').read_text(), '[workspace]\n')
            self.assertEqual((sources / 'sui/run.sh').stat().st_mode & 0o777, 0o755)
            self.assertEqual((sources / 'sui/source-link').readlink(), Path('Cargo.toml'))
            self.assertEqual((restored / 'build/binaries/sui-node-native').stat().st_mode & 0o777, 0o755)
            self.assertFalse((restored / 'source-unpack').exists())
            admission = ci_campaign.admit_inputs(restored / 'build', restored / 'fixture.tar.gz',
                                                 restored / 'verification', root / 'admission')
            self.assertEqual(admission['bindings']['build_manifest_sha256'],
                             result['build_manifest_sha256'])
            self.assertTrue((root / 'admission/input-admission.json').is_file())
            with self.assertRaises(FileExistsError):
                ci_prepare.restore_inputs(package, digest, restored)

    def test_restore_rejects_missing_or_wrong_package_digest_before_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / 'deployment.tar.gz'
            package.write_bytes(b'not the admitted deployment package')
            restored = root / 'fresh-vm'
            for digest in ('', 'a' * 64, ci_prepare.sha(package).upper()):
                with self.subTest(digest=digest), self.assertRaises(ValueError):
                    ci_prepare.restore_inputs(package, digest, restored)
                self.assertFalse(restored.exists())

    def test_restore_rejects_source_hash_inventory_and_escape_before_acceptance(self):
        for change in ('archive-hash', 'manifest-hash', 'inventory', 'escape', 'relocation-escape'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                inputs, fixture = self.inputs(root)
                build = inputs / 'build'
                if change == 'archive-hash':
                    (build / 'sources.tar.gz').write_bytes(b'corrupted frozen sources')
                elif change == 'manifest-hash':
                    (build / 'source-manifest.json').write_text('{}')
                else:
                    if change == 'inventory':
                        (build / 'sources/sui/unbound.rs').write_text('unbound source\n')
                    else:
                        link = build / 'sources/sui/escape'
                        link.symlink_to('../../../escape' if change == 'escape' else '../../build-manifest.json')
                    with tarfile.open(build / 'sources.tar.gz', 'w:gz') as archive:
                        archive.add(build / 'sources', arcname='sources')
                    manifest = json.loads((build / 'build-manifest.json').read_text())
                    manifest['source_archive_sha256'] = ci_prepare.sha(build / 'sources.tar.gz')
                    ci_prepare.save(build / 'build-manifest.json', manifest)
                    ci_prepare.save(inputs / 'READY.json', dict(complete=True,
                        build_manifest_sha256=ci_prepare.sha(build / 'build-manifest.json')))
                package = root / 'deployment.tar.gz'
                digest = self.package(inputs, fixture, package)
                restored = root / 'fresh-vm'
                with self.assertRaisesRegex(ValueError, 'binding differs|inventory changed|symlink escapes'):
                    ci_prepare.restore_inputs(package, digest, restored)
                self.assertFalse((restored / 'build/sources').exists())
                self.assertFalse((root / 'escape').exists())


if __name__ == '__main__':
    unittest.main()
