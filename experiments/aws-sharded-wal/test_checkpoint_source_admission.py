"""Offline source-layout admission gates; no AWS clients or benchmark processes."""
import copy
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import sui_checkpoint as supervisor


class CheckpointSourceAdmission(unittest.TestCase):
    support_relative = 'experiments/sui-checkpoint/checkpoint_support.py'

    def inputs(self, root, change=None):
        bundle = root / 'bundle'
        sources = bundle / 'sources'
        (sources / 'sui').mkdir(parents=True)
        (sources / 'sui/Cargo.toml').write_text('[workspace]\n')
        engine = sources / 'tidehunter-sharded-wal'
        installed = Path(supervisor.__file__).resolve().parent
        helpers = engine / 'experiments/aws-sharded-wal'
        helpers.mkdir(parents=True)
        for path in installed.glob('*.py'):
            shutil.copy2(path, helpers / path.name)
        shared = engine / 'experiments/resident_config.py'
        shutil.copy2(installed.parent / 'resident_config.py', shared)
        support = engine / self.support_relative
        support.parent.mkdir(parents=True)
        support.write_text('"""Frozen standalone checkpoint helper."""\n')
        identities = {owner: dict(revision='frozen', files={
            str(path.relative_to(sources / owner)): dict(sha256=supervisor.sha(path), mode=0o644)
            for path in (sources / owner).rglob('*') if path.is_file()})
            for owner in ('sui', 'tidehunter-sharded-wal')}
        if change == 'changed':
            support.write_text('changed certified checkpoint verifier\n')
        elif change in ('missing', 'unbound'):
            support.unlink()
            if change == 'unbound':
                del identities['tidehunter-sharded-wal']['files'][self.support_relative]
        elif change == 'extra-tree':
            (sources / 'middleware').mkdir()
            (sources / 'middleware/helper.py').write_text('archived helper\n')
        elif change == 'extra-inventory':
            identities['legacy_tools'] = {}
        elif change == 'symlink':
            support.rename(support.with_name('other.py'))
            support.symlink_to('other.py')
            files = identities['tidehunter-sharded-wal']['files']
            files[self.support_relative] = {'link': 'other.py'}
            files[str(support.with_name('other.py').relative_to(engine))] = {
                'sha256': supervisor.sha(support), 'mode': 0o644}
        supervisor.assemble_runner(engine, root / 'installed/runner',
                                   identities['tidehunter-sharded-wal']['files'])
        supervisor.write_json(bundle / 'source-manifest.json', identities)
        with tarfile.open(bundle / 'sources.tar.gz', 'w:gz') as archive:
            archive.add(sources, arcname='sources')
        binaries = bundle / 'binaries'
        binaries.mkdir()
        commands = []
        for name in sorted(supervisor.BINARIES):
            binary = binaries / name
            binary.write_bytes(name.encode())
            log, messages = bundle / f'build-{name}.log', bundle / f'build-{name}.jsonl'
            log.write_text('successful build\n')
            messages.write_text('{}\n')
            commands.append(dict(label=name, sha256=supervisor.sha(binary),
                log_sha256=supervisor.sha(log), messages_sha256=supervisor.sha(messages)))
        manifest = dict(protocol=9, fabric=False, commands=commands,
            binaries={path.name: supervisor.sha(path) for path in binaries.iterdir()},
            source_archive_sha256=supervisor.sha(bundle / 'sources.tar.gz'),
            source_manifest_sha256=supervisor.sha(bundle / 'source-manifest.json'))
        supervisor.write_json(bundle / 'build-manifest.json', manifest)
        for name in ('plan.json', 'portability.json'):
            supervisor.write_json(bundle / name, {})
        objects = root / 'objects'
        objects.mkdir()
        with tarfile.open(objects / 'build_bundle.tar.gz', 'w:gz') as archive:
            for path in bundle.iterdir():
                if path.name != 'sources':
                    archive.add(path, arcname=path.name)
        for name, owner in (('fixture_bundle', 'fixture'), ('verification_bundle', 'verification')):
            content = root / owner
            content.mkdir()
            (content / 'unused.json').write_text('{}')
            with tarfile.open(objects / (name + '.tar.gz'), 'w:gz') as archive:
                archive.add(content, arcname=owner)
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.root = root / 'campaign'
        campaign.root.mkdir()
        campaign.results = campaign.root / 'results'
        campaign.results.mkdir()
        campaign.s3 = None
        campaign.installed_supervisor = root / 'installed/runner/sui_checkpoint.py'
        campaign.cfg = copy.deepcopy(supervisor.schema()['config'])
        campaign.cfg.update(transports=['tcp'], binaries=manifest['binaries'],
            build_manifest_sha256=supervisor.sha(bundle / 'build-manifest.json'),
            source_archive_sha256=manifest['source_archive_sha256'],
            source_manifest_sha256=manifest['source_manifest_sha256'])
        for name in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            campaign.cfg[name] = dict(key=name + '.tar.gz', sha256=supervisor.sha(objects / (name + '.tar.gz')))
        def download(client, cfg, key, digest, target):
            path = objects / key
            self.assertEqual(supervisor.sha(path), digest)
            shutil.copy2(path, target)
        return campaign, download

    def test_self_contained_sources_reach_local_verification_before_preflight(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign, download = self.inputs(Path(temporary))
            events = []
            with patch.object(supervisor, 'download', side_effect=download), \
                    patch.object(supervisor, '__file__', str(campaign.installed_supervisor)), \
                    patch.object(campaign, 'verify_local', side_effect=lambda: events.append('local')), \
                    patch.object(supervisor, 'preflight', side_effect=lambda *args: events.append('preflight')):
                campaign.inputs()
            self.assertEqual(events, ['local', 'preflight'])
            self.assertTrue((campaign.scripts / 'checkpoint_support.py').is_file())
            self.assertEqual(set(supervisor.read_json(campaign.build / 'source-manifest.json')),
                             {'sui', 'tidehunter-sharded-wal'})
            self.assertFalse((campaign.build / 'sources/middleware').exists())

    def test_missing_changed_unbound_or_external_support_fails_before_preflight(self):
        for change in ('changed', 'missing', 'unbound', 'extra-tree', 'extra-inventory', 'symlink'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                campaign, download = self.inputs(Path(temporary), change)
                with patch.object(supervisor, 'download', side_effect=download), \
                        patch.object(supervisor, '__file__', str(campaign.installed_supervisor)), \
                        patch.object(campaign, 'verify_local') as local, \
                        patch.object(supervisor, 'preflight') as preflight, \
                        patch.object(supervisor.subprocess, 'Popen') as spawn:
                    with self.assertRaises(ValueError):
                        campaign.inputs()
                    local.assert_not_called()
                    preflight.assert_not_called()
                    spawn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
