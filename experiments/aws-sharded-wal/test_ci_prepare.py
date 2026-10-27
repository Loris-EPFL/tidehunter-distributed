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
            audit = {'all_equal': True, 'databases': {'db': {'equal': True,
                'native': {'tables': {}}, 'remote': {'tables': {}}}},
                'rocksdb': {'db': {'equal': True, 'native_semantic': {}, 'distributed_semantic': {}}}}
            (source / 'receipt.json').write_text(json.dumps({'complete': True, 'evidence_sha256': evidence_sha}))
            (source / 'experiment/audit.json').write_text(json.dumps(audit))
            requested = ci_prepare.local_replay_transport()
            for change in ({'complete': False}, {'external_manifest': {'transport': 'efa'}},
                           {'topology': 'six AWS hosts'},
                           {'local_transport': {'kind': 'tcp'}},
                           {'local_transport': {'kind': 'ofi', 'fabric': {'backend': 'software-tcp'},
                                                'security': 'mutual-tls'}},
                           {'local_transport': None}):
                result = {'complete': True, 'topology': 'five-process loopback',
                          'local_transport': requested, 'audit': audit} | change
                (source / 'experiment/results.json').write_text(json.dumps(result))
                with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'loopback replay'):
                    ci_prepare.verification_bundle(source, root / 'verification', requested)
                self.assertFalse((root / 'verification').exists())



class CheckpointCompletionAdmissionTests(unittest.TestCase):
    """Real archived metric consumer fixtures, not benchmark measurements."""
    def metrics(self, count=1774):
        return (
            f'checkpoint_completion_latency_bucket{{le="0.01"}} {count * 9 // 10}\n'
            f'checkpoint_completion_latency_bucket{{le="0.1"}} {count}\n'
            f'checkpoint_completion_latency_bucket{{le="+Inf"}} {count}\n'
            f'checkpoint_completion_latency_sum {count / 100}\n'
            f'checkpoint_completion_latency_count {count}\n')

    def source(self, root, *, native=None, remote=None, duplicate=False):
        source = Path(root) / 'source'
        experiment = source / 'experiment'
        (experiment / 'native-schema').mkdir(parents=True)
        (experiment / 'native-schema/schema.json').write_text('{"namespace":"real-schema-fixture"}')
        audit = dict(all_equal=True, databases={'db': {'equal': True,
            'native': {'tables': {}}, 'remote': {'tables': {}}}},
            rocksdb={'db': {'equal': True, 'native_semantic': {}, 'distributed_semantic': {}}})
        ci_prepare.save(experiment / 'audit.json', audit)
        ci_prepare.save(experiment / 'results.json', dict(schema=1, metrics_schema=2, protocol=9,
            backend='tidehunter-sharded-wal', durability='synchronous durable whole-batch publication',
            complete=True, audit=audit, topology='five physical shards per database; loopback, one SSD/shared host memory',
            local_transport=ci_prepare.local_replay_transport(),
            checkpoint_limit=ci_prepare.TIP, checkpoint_digest=ci_prepare.CHECKPOINT))
        for mode, text in (('native', native if native is not None else self.metrics()),
                           ('remote', remote if remote is not None else self.metrics(1773))):
            (experiment / (mode + '-foreground.prom')).write_text(text)
        with tarfile.open(source / 'evidence.tar.gz', 'w:gz') as archive:
            archive.add(experiment, arcname='experiment')
            if duplicate:
                archive.add(experiment / 'native-foreground.prom', arcname='experiment/native-foreground.prom')
        ci_prepare.save(source / 'receipt.json', dict(complete=True,
            evidence_sha256=ci_prepare.sha(source / 'evidence.tar.gz')))
        return source, Path(root) / 'verification'

    def verify(self, source, output):
        return ci_prepare.verification_bundle(source, output, ci_prepare.local_replay_transport())

    def test_actual_positive_native_and_remote_observations_and_bucket_bounds_are_retained(self):
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root)
            original = json.loads((source / 'receipt.json').read_text())
            requested_path = Path(root) / 'local-replay-transport.json'
            ci_prepare.write_local_replay_transport(requested_path)
            requested = json.loads(requested_path.read_text())
            self.assertEqual(json.loads((source / 'experiment/results.json').read_text())['local_transport'],
                             requested)
            ci_prepare.verification_bundle(source, output, requested)
            copied = json.loads((output / 'receipt.json').read_text())
            report = copied['checkpoint_completion_observations']
            self.assertEqual(report['evidence_sha256'], original['evidence_sha256'])
            self.assertEqual(report['modes']['native']['count'], 1774)
            self.assertEqual(report['modes']['remote']['count'], 1773)
            for mode, observation in report['modes'].items():
                self.assertTrue(observation['available'])
                self.assertAlmostEqual(observation['mean_us'], 10000)
                self.assertEqual(observation['quantiles']['p50']['lower_us'], 0)
                self.assertEqual(observation['quantiles']['p50']['upper_us'], 10000)
                self.assertEqual(observation['quantiles']['p99']['lower_us'], 10000)
                self.assertEqual(observation['quantiles']['p99']['upper_us'], 100000)
                self.assertIn('not raw quantiles', observation['quantile_method'])
                self.assertIn('not assumed equal', observation['count_contract'])
                self.assertEqual(observation['metrics_sha256'],
                    ci_prepare.sha(source / 'experiment' / (mode + '-foreground.prom')))
            self.assertEqual(json.loads((source / 'receipt.json').read_text()), original)
            self.assertEqual(ci_prepare.sha(output / 'evidence.tar.gz'), original['evidence_sha256'])

    def test_native_remote_full_state_audit_disagreement_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root)
            audit = json.loads((source / 'experiment/audit.json').read_text())
            audit['databases']['db']['remote']['tables'] = {'changed': True}
            ci_prepare.save(source / 'experiment/audit.json', audit)
            result = json.loads((source / 'experiment/results.json').read_text())
            result['audit'] = audit
            ci_prepare.save(source / 'experiment/results.json', result)
            with self.assertRaisesRegex(ValueError, 'full-state audit'):
                self.verify(source, output)
            self.assertFalse(output.exists())

    def test_missing_zero_or_semantically_old_timer_fails_each_backend_before_packaging(self):
        invalid = (
            '# metric missing\n',
            self.metrics(0),
            self.metrics().replace('checkpoint_completion_latency', 'checkpoint_exec_latency'),
            'checkpoint_exec_interval_count 1774\ncheckpoint_execution_started 1775\n',
        )
        for mode in ('native', 'remote'):
            for text in invalid:
                with self.subTest(mode=mode, text=text), tempfile.TemporaryDirectory() as root:
                    source, output = self.source(root, **{mode: text})
                    with self.assertRaises(ValueError):
                        self.verify(source, output)
                    self.assertFalse(output.exists())

    def test_nonfinite_missing_and_malformed_histogram_measurements_cannot_satisfy_readiness(self):
        text = self.metrics()
        invalid = [
            text.replace('_sum 17.74', '_sum 0'),
            text.replace('_sum 17.74', '_sum NaN'),
            text.replace('_sum 17.74', '_sum +Inf'),
            text.replace('_count 1774', '_count NaN'),
            text.replace('_count 1774', '_count +Inf'),
            text.replace('_count 1774', '_count 1.5'),
            text.replace('le="0.1"} 1774', 'le="0.1"} 1'),
            text.replace('le="+Inf"} 1774', 'le="+Inf"} 1773'),
            text.replace('le="0.01"', 'le="-0.01"'),
            text.replace('le="0.01"', 'le="0.01",le="0.1"'),
            text.replace('le="0.01"} 1596', 'le="0.01"} NaN'),
            text + text.splitlines()[0] + '\n',
            '\n'.join(line for line in text.splitlines() if '_sum ' not in line),
            '\n'.join(line for line in text.splitlines() if '_count ' not in line),
            '\n'.join(line for line in text.splitlines() if 'le="+Inf"' not in line),
            '\n'.join(line for line in text.splitlines() if '_bucket{' not in line),
        ]
        for measurement in invalid:
            with self.subTest(measurement=measurement), tempfile.TemporaryDirectory() as root:
                source, output = self.source(root, remote=measurement)
                with self.assertRaises(ValueError):
                    self.verify(source, output)
                self.assertFalse(output.exists())

    def test_scrape_is_bound_to_archive_not_replaceable_loose_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root, native=self.metrics(0))
            (source / 'experiment/native-foreground.prom').write_text(self.metrics())
            with self.assertRaises(ValueError):
                self.verify(source, output)
            self.assertFalse(output.exists())

    def test_duplicate_archive_scrapes_and_missing_remote_scrape_are_not_selected_silently(self):
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root, duplicate=True)
            with self.assertRaisesRegex(ValueError, 'one bounded foreground'):
                self.verify(source, output)
            self.assertFalse(output.exists())
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root)
            with tarfile.open(source / 'evidence.tar.gz', 'w:gz') as archive:
                archive.add(source / 'experiment/native-foreground.prom', arcname='experiment/native-foreground.prom')
            ci_prepare.save(source / 'receipt.json', dict(complete=True,
                evidence_sha256=ci_prepare.sha(source / 'evidence.tar.gz')))
            with self.assertRaisesRegex(ValueError, 'one bounded foreground'):
                self.verify(source, output)
            self.assertFalse(output.exists())

    def test_positive_small_actual_count_does_not_become_an_invented_p99(self):
        with tempfile.TemporaryDirectory() as root:
            source, output = self.source(root, native=self.metrics(1), remote=self.metrics(2))
            self.verify(source, output)
            observations = json.loads((output / 'receipt.json').read_text())['checkpoint_completion_observations']['modes']
            self.assertEqual(observations['native']['count'], 1)
            self.assertFalse(observations['native']['quantiles']['p99']['available'])
            self.assertIsNone(observations['native']['quantiles']['p99']['upper_us'])


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
        shutil.copy2(ci_prepare.ROOT / 'experiments/resident_config.py',
                     engine / 'experiments/resident_config.py')
        shutil.copy2(ci_prepare.ROOT / 'experiments/portable_transport.py',
                     engine / 'experiments/portable_transport.py')
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
        manifest = dict(protocol=9, fabric=True, commands=commands,
            binaries={path.name: ci_prepare.sha(path) for path in binaries.iterdir()},
            source_archive_sha256=ci_prepare.sha(build / 'sources.tar.gz'),
            source_manifest_sha256=ci_prepare.sha(build / 'source-manifest.json'))
        ci_prepare.save(build / 'build-manifest.json', manifest)
        ci_prepare.save(build / 'plan.json', dict(protocol=manifest['protocol']))
        ci_prepare.save(build / 'portability.json', {})
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
            topology='five-process loopback', local_transport={'kind': 'ofi',
                'fabric': {'backend': 'software-tcp'}, 'security': 'trusted'}, audit=audit,
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
            self.assertEqual(admission['runner_entrypoint']['exit_code'], 0)
            self.assertTrue(admission['runner_entrypoint']['isolated_python_environment'])
            import sui_checkpoint as supervisor
            runner = Path(admission['inputs']['runner_bundle']['path'])
            self.assertEqual(ci_prepare.sha(runner), admission['inputs']['runner_bundle']['sha256'])
            installed = root / 'installed-runner'
            supervisor.safe_extract(runner, installed)
            records = json.loads((restored / 'build/source-manifest.json').read_text())[
                'tidehunter-sharded-wal']['files']
            self.assertNotIn('experiments/aws-sharded-wal/resident_config.py', records)
            self.assertEqual(ci_prepare.sha(installed / 'runner/resident_config.py'),
                             records['experiments/resident_config.py']['sha256'])
            supervisor.validate_runner_helpers(installed / 'runner', records)
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
