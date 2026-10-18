"""Offline false-success gates; no AWS calls or provider/device assumptions."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import checkpoint_matrix as matrix
import sui_checkpoint as supervisor
import test_sui_checkpoint_supervisor as fixtures
from test_sui_checkpoint_supervisor import example_config, traffic, completed_node

import checkpoint_kv

def completed_matrix(cfg):
    cells = []
    for planned in matrix.matrix_plan(cfg):
        cell = dict(planned, complete=True, config_sha256=f"{planned['ordinal'] + 1:064x}",
                    replay_evidence_sha256='d' * 64, resources_sha256='e' * 64,
                    namespace_ids={'perpetual': [planned['ordinal'] + 1] * 16}, node_archives={}, efa={})
        for key in ('build_manifest_sha256', 'binaries', 'fixture_bundle', 'verification_bundle',
                    'source_archive_sha256', 'source_manifest_sha256'):
            cell[key] = copy.deepcopy(cfg[key])
        for node in cfg['nodes']:
            cell['node_archives'][node['instance_id']] = dict(bucket=cfg['bucket'], sha256='f' * 64,
                key=f"{cfg['s3_prefix']}/results/cells/{cell['cell_id']}/storage/{node['partition']}/final/evidence.tar.gz")
        if cell['transport'] in ('efa', 'efa-direct'):
            for instance_id in [cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])]:
                identity = dict(run_id=cfg['run_id'], cell_id=cell['cell_id'], instance_id=instance_id,
                    config_sha256=cell['config_sha256'], build_manifest_sha256=cfg['build_manifest_sha256'])
                cell['efa'][instance_id] = traffic(identity)
        cells.append(cell)
    return cells


class MatrixEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.cfg = example_config()
        self.cells = completed_matrix(self.cfg)

    def test_complete_counterbalanced_pairs_and_single_transport(self):
        self.assertEqual([cell['cell_id'] for cell in self.cells],
                         ['r01-tcp', 'r01-efa', 'r02-efa', 'r02-tcp',
                          'r03-tcp', 'r03-efa', 'r04-efa', 'r04-tcp'])
        self.assertEqual(len({cell['cell_id'] for cell in self.cells}), 8)
        self.assertEqual([cell['read_profile_limit'] for cell in self.cells],
                         [0, 0, 4096, 4096, 4096, 4096, 0, 0])
        matrix.validate_matrix(self.cfg, self.cells)
        for transport in ('tcp', 'efa', 'efa-direct'):
            cfg = self.cfg | dict(transports=[transport], repetitions=1, read_profile_limits=[0],
                                  write_profile_limits=[0], native_write_profile_limits=[0])
            matrix.validate_matrix(cfg, completed_matrix(cfg))

    def test_cell_identity_accepts_four_repetitions_and_rejects_out_of_bounds(self):
        for repetition in range(1, 5):
            for transport in ('tcp', 'efa', 'efa-direct'):
                matrix.check_cell(f'r{repetition:02d}-{transport}', transport)
        for cell_id in ('r00-tcp', 'r05-tcp', 'r99-tcp', 'r4-tcp', 'r004-tcp',
                        'r04-efa', 'r04-tcp-extra', 'r04-tcp\n', None, 4):
            with self.subTest(cell_id=cell_id), self.assertRaises(ValueError):
                matrix.check_cell(cell_id, 'tcp')
        with self.assertRaises(ValueError):
            matrix.check_cell('r04-fabric-tcp', 'fabric-tcp')

    def test_triplets_require_all_six_direct_host_source_and_cell_proofs(self):
        cfg = self.cfg | dict(transports=['tcp', 'efa', 'efa-direct'])
        cells = completed_matrix(cfg)
        matrix.validate_matrix(cfg, cells)
        self.assertEqual([cell['transport'] for cell in cells[:6]],
                         ['tcp', 'efa', 'efa-direct', 'efa-direct', 'efa', 'tcp'])
        for host in cells[2]['efa']:
            for mutation in ('missing', 'source', 'cell', 'software'):
                changed = copy.deepcopy(cells)
                proof = changed[2]['efa'][host]
                if mutation == 'missing':
                    del changed[2]['efa'][host]
                elif mutation == 'software':
                    proof['after']['devices']['rdmap0s1']['driver'] = 'rxe'
                else:
                    field = 'build_manifest_sha256' if mutation == 'source' else 'cell_id'
                    proof['before']['identity'][field] = 'foreign'
                with self.subTest(host=host, mutation=mutation), self.assertRaises(ValueError):
                    matrix.validate_matrix(cfg, changed)
        for changed in (cells[:-1], cells + [cells[-1]]):
            with self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, changed)

    def test_exact_fabric_is_required_for_new_hardware_negotiation(self):
        for transport in ('efa', 'efa-direct'):
            text = f'provider: efa\n    fabric: {transport}\n    type: FI_EP_RDM\n'
            matrix.validate_provider(text, transport)
            verbose = f'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        name: {transport}\n        prov_name: efa\n'
            matrix.validate_provider(verbose, transport)
            for invalid in (text.replace('fabric: ' + transport, 'fabric: tcp'),
                            text.replace('    fabric: ' + transport + '\n', ''),
                            text.replace('provider: efa', 'provider: efa-direct'),
                            text + 'provider: tcp\n    fabric: tcp\n    type: FI_EP_RDM\n',
                            text + 'provider: efa\n    type: FI_EP_RDM\n',
                            text.replace('FI_EP_RDM', 'FI_EP_MSG')):
                with self.subTest(transport=transport, invalid=invalid), self.assertRaises(ValueError):
                    matrix.validate_provider(invalid, transport)
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa\n type: FI_EP_RDM\n', 'efa-direct')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n', 'efa')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n')

    def test_missing_duplicate_reordered_incomplete_cells_fail(self):
        cases = [self.cells[:-1], self.cells + [self.cells[0]], self.cells[::-1],
                 self.cells[:2] + [self.cells[1]] + self.cells[3:]]
        incomplete = copy.deepcopy(self.cells)
        incomplete[-1]['complete'] = False
        cases.append(incomplete)
        for cells in cases:
            with self.subTest(cells=cells), self.assertRaises(ValueError):
                matrix.validate_matrix(self.cfg, cells)

    def test_every_efa_host_requires_its_own_cell_bound_activity(self):
        for host in self.cells[1]['efa']:
            for mutation in ('missing', 'stale-cell', 'zero-tx', 'zero-rx', 'reset', 'software'):
                cells = copy.deepcopy(self.cells)
                proof = cells[1]['efa'][host]
                if mutation == 'missing':
                    del cells[1]['efa'][host]
                elif mutation == 'stale-cell':
                    proof['before']['identity']['cell_id'] = 'r02-efa'
                else:
                    device = proof['after']['devices']['rdmap0s1']
                    if mutation == 'software':
                        device['driver'] = 'rxe'
                    else:
                        counter = 'recv_bytes' if mutation == 'zero-rx' else 'send_bytes'
                        device['counters']['ports/1/hw_counters/' + counter] = 99 if mutation == 'reset' else 100
                with self.subTest(host=host, mutation=mutation), self.assertRaises(ValueError):
                    matrix.validate_matrix(self.cfg, cells)

    def test_old_namespace_config_archive_and_missing_pair_evidence_fail(self):
        for mutation in ('namespace', 'config', 'archive', 'replay', 'resources', 'fixture', 'profile'):
            cells = copy.deepcopy(self.cells)
            cell = cells[-1]
            if mutation == 'namespace':
                cell['namespace_ids'] = cells[0]['namespace_ids']
            elif mutation == 'config':
                cell['config_sha256'] = cells[0]['config_sha256']
            elif mutation == 'archive':
                cell['node_archives'] = cells[0]['node_archives']
            elif mutation == 'fixture':
                cell['fixture_bundle']['sha256'] = '0' * 64
            elif mutation == 'profile':
                cell['read_profile_limit'] += 1
            else:
                del cell['replay_evidence_sha256' if mutation == 'replay' else 'resources_sha256']
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                matrix.validate_matrix(self.cfg, cells)

    def test_matrix_bounds_reject_obsolete_or_ambiguous_configuration(self):
        for change in (dict(transport='tcp'), dict(transports=[]), dict(transports=['tcp', 'tcp']),
                       dict(transports=['fabric-tcp']), dict(repetitions=0), dict(repetitions=True),
                       dict(repetitions=5, read_profile_limits=[0] * 5), dict(read_profile_limit=0),
                       dict(read_profile_limits=[]), dict(read_profile_limits=[0]),
                       dict(read_profile_limits=[0, 4096, 4096]), dict(read_profile_limits=[0, True, 4096, 0]),
                       dict(read_profile_limits=[False, 4096, 4096, 0]), dict(read_profile_limits=[0, -1, 4096, 0]),
                       dict(read_profile_limits=[0, 65537, 4096, 0]), dict(read_profile_limits=[0, 1.0, 4096, 0]),
                       dict(read_profile_limits='0,4096,4096,0'), dict(read_profile_limits=None),
                       dict(write_profile_limit=128), dict(write_profile_limits=[]),
                       dict(write_profile_limits=[0, 128]), dict(write_profile_limits=[0, True, 128, 0]),
                       dict(write_profile_limits=[0, -1, 128, 0]), dict(write_profile_limits=[0, 65537, 128, 0]),
                       dict(write_profile_limits=[0, 1.0, 128, 0]), dict(write_profile_limits=None),
                       dict(native_write_profile_limits=[640]), dict(native_write_profile_limits=[0, True, 640, 0]),
                       dict(native_write_profile_limits=[0, 65537, 640, 0])):
            with self.subTest(change=change), self.assertRaises(ValueError):
                supervisor.validate_cfg(self.cfg | change)
        missing = self.cfg.copy()
        del missing['read_profile_limits']
        with self.assertRaises(ValueError):
            supervisor.validate_cfg(missing)

    def test_kv_scope_rejects_unknown_or_ambiguous_requests(self):
        for options in (
                {'repetitions': True, 'diagnostics': False, 'scope': 'targeted'},
                {'repetitions': 1, 'diagnostics': 0, 'scope': 'targeted'},
                {'repetitions': 1, 'diagnostics': False, 'scope': 'unknown'},
                {'repetitions': 1, 'diagnostics': False, 'scope': 'targeted', 'skip_efa': True},
                {'scope': 'targeted'}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                checkpoint_kv.validate_options(options)

    def test_explicit_direct_requires_hardware_admission_without_legacy_schedule_changes(self):
        from guest import Campaign, efa_cases
        campaign = Campaign.__new__(Campaign)
        campaign.cfg = dict(campaign='efa-cache', kv_subsuite=True, efa=False)
        with self.assertRaisesRegex(ValueError, 'EFA-enabled build and fleet'):
            campaign.run_case('current', (256, 1, 1), 0, storage=True, transport='efa-direct')
        self.assertEqual({spec[3].get('transport', 'tcp') for spec in efa_cases()}, {'tcp', 'efa'})
        options = dict(repetitions=1, diagnostics=False, scope='targeted')
        planned = checkpoint_kv.plan(options, ['tcp', 'efa', 'efa-direct'])
        self.assertEqual(len(planned), 30)
        for transport in ('tcp', 'efa', 'efa-direct'):
            rows = [spec for spec in planned if spec['transport'] == transport]
            self.assertEqual(len(rows), 10)
            self.assertEqual(sum(spec['transport_floor'] for spec in rows), 1)

    def test_targeted_diagnostics_leave_clean_comparison_populations_unchanged(self):
        for repetitions in (1, 3):
            for transports in (['tcp'], ['tcp', 'efa'], ['tcp', 'efa', 'efa-direct']):
                with self.subTest(repetitions=repetitions, transports=transports):
                    options = dict(scope='targeted', repetitions=repetitions, diagnostics=False)
                    clean = checkpoint_kv.plan(options, transports)
                    combined = checkpoint_kv.plan(options | dict(diagnostics=True), transports)
                    self.assertEqual([case for case in combined if not case['diagnostics']], clean)
                    profiled = [case for case in combined if case['diagnostics']]
                    self.assertEqual({case['transport'] for case in profiled}, set(transports))
                    self.assertTrue(all(case['repetition'] > max(row['repetition'] for row in clean)
                                        for case in profiled))

    def test_cache_schedules_are_bound_to_each_cell_and_cannot_be_forged(self):
        cfg = self.cfg | dict(shard_cache_mib_by_repetition=[0, 16, 16, 0])
        cells = completed_matrix(cfg)
        matrix.validate_matrix(cfg, cells)
        for index, cell in enumerate(cells):
            changed = copy.deepcopy(cells)
            changed[index]['shard_cache_mib'] = 0 if cell['shard_cache_mib'] else 16
            with self.subTest(cell=cell['cell_id']), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, changed)
        for field in ('coordinator_cache_mib', 'shard_cache_mib'):
            changed = copy.deepcopy(cells)
            changed[0][field] = False
            with self.subTest(field=field), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, changed)
        for budgets in ([0], [0, 16, 16, True], [0, -1, 16, 0], [0, 129, 16, 0], None):
            with self.subTest(budgets=budgets), self.assertRaises(ValueError):
                supervisor.validate_cfg(self.cfg | dict(shard_cache_mib_by_repetition=budgets))

    def test_profile_limits_are_bound_to_repetition_not_transport(self):
        cfg = self.cfg | dict(repetitions=2, read_profile_limits=[65536, 0],
                              write_profile_limits=[128, 0], native_write_profile_limits=[640, 0])
        cells = completed_matrix(cfg)
        self.assertEqual([cell['read_profile_limit'] for cell in cells], [65536, 65536, 0, 0])
        matrix.validate_matrix(cfg, cells)
        for index in range(4):
            for field in ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit'):
                changed = copy.deepcopy(cells)
                changed[index][field] = 0 if index < 2 else 1
                with self.subTest(cell=index, field=field), self.assertRaises(ValueError):
                    matrix.validate_matrix(cfg, changed)
        boolean = copy.deepcopy(self.cells)
        boolean[0]['read_profile_limit'] = False
        with self.assertRaises(ValueError):
            matrix.validate_matrix(self.cfg, boolean)

    def test_historical_unprofiled_write_evidence_remains_readable(self):
        for config_field, cell_field, limits in (
                ('write_profile_limits', 'write_profile_limit', [0, 128, 128, 0]),
                ('native_write_profile_limits', 'native_write_profile_limit', [0, 640, 640, 0])):
            cfg = self.cfg.copy()
            del cfg[config_field]
            cells = completed_matrix(cfg)
            for cell in cells:
                del cell[cell_field]
            matrix.validate_matrix(cfg, cells)
            cfg[config_field] = limits
            with self.subTest(field=cell_field), self.assertRaises(ValueError):
                matrix.validate_matrix(cfg, cells)

    def test_provider_must_be_hardware_efa_rdm(self):
        matrix.validate_provider('provider: efa\n    type: FI_EP_RDM\n')
        verbose = 'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        prov_name: efa\n'
        matrix.validate_provider(verbose)
        for text in ('provider: efa;ofi_rxd\n type: FI_EP_RDM\n',
                     'provider: tcp\n type: FI_EP_RDM\n', 'provider: efa\n type: FI_EP_MSG\n',
                     verbose.replace('prov_name: efa', 'prov_name: efa;ofi_rxd'),
                     verbose.replace('prov_name: efa', 'prov_name: tcp'),
                     verbose.replace('FI_EP_RDM', 'FI_EP_MSG'),
                     verbose + 'provider: tcp\n type: FI_EP_RDM\n'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                matrix.validate_provider(text)

    def test_sysfs_snapshot_fails_closed_and_binds_both_directions(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)
            driver = base / 'drivers/efa'
            driver.mkdir(parents=True)
            device = base / 'rdmap0s1'
            (device / 'device').mkdir(parents=True)
            (device / 'device/driver').symlink_to(driver)
            counters = device / 'ports/1/hw_counters'
            counters.mkdir(parents=True)
            (counters / 'send_bytes').write_text('100')
            with self.assertRaises(OSError):
                matrix.efa_snapshot({}, base)
            (counters / 'recv_bytes').write_text('200')
            before = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            (counters / 'send_bytes').write_text('120')
            (counters / 'recv_bytes').write_text('240')
            after = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            proof = matrix.efa_proof(before, after, {'cell_id': 'r01-efa'})
            self.assertEqual((proof['send_bytes'], proof['recv_bytes']), (20, 40))
            (counters / 'recv_bytes').write_text('unreadable')
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)

    def test_efa_archive_cannot_substitute_raw_counters_or_software_provider(self):
        validator = fixtures.EvidenceTests()
        for mutation in ('raw', 'provider', 'proof'):
            expected, resources, files = completed_node('efa')
            if mutation == 'raw':
                files['efa-before.json'] = b'{}'
            elif mutation == 'provider':
                files['preflight/efa-provider.txt'] = b'provider: efa;ofi_rxd\n type: FI_EP_RDM\n'
            else:
                proof = json.loads(files['efa-proof.json'])
                proof['send_bytes'] += 1
                files['efa-proof.json'] = json.dumps(proof).encode()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validator.validate(files, expected, resources)


class MatrixExecutionTests(unittest.TestCase):
    def campaign(self, root):
        campaign = object.__new__(supervisor.SuiCampaign)
        campaign.cfg = example_config()
        campaign.root, campaign.campaign_results = root, root / 'results'
        campaign.campaign_results.mkdir()
        campaign.results = campaign.campaign_results
        campaign.deadline = campaign.cfg['deadline_utc']
        campaign.plan = matrix.matrix_plan(campaign.cfg)
        campaign.nodes = campaign.cfg['nodes']
        campaign.state = dict(cells=[], complete=False)
        campaign.install_node, campaign.collect_node = Mock(), Mock()
        campaign.node_command = Mock(return_value='release-owned-unit')
        campaign.ssm_one = Mock()
        campaign.parallel_nodes = lambda action, function: [function(node) for node in campaign.nodes]
        campaign.prepare = Mock()
        campaign.replay = Mock()
        campaign.retain_replay = Mock()
        return campaign

    def test_all_pairs_run_in_order_with_release_barriers(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            templates = completed_matrix(campaign.cfg)
            events = []
            def prepare():
                template = templates[campaign.cell['ordinal']]
                campaign.external = dict(config_sha256=template['config_sha256'])
                campaign.cell['namespace_ids'] = template['namespace_ids']
                events.append(('prepare', campaign.cell['cell_id']))
            def replay():
                self.assertEqual(campaign.cell['read_profile_limit'],
                                 campaign.cfg['read_profile_limits'][campaign.cell['repetition'] - 1])
                events.append(('paired-replay', campaign.cell['cell_id']))
                (campaign.results / 'replay-evidence.tar.gz').write_bytes(b'paired full-state audit fixture')
                (campaign.results / 'resources.json').write_bytes(b'physical resources fixture')
            def collect(node, final=False, logs=False):
                if logs:
                    template = templates[campaign.cell['ordinal']]
                    campaign.cell['node_archives'][node['instance_id']] = template['node_archives'][node['instance_id']]
                    if campaign.cell['transport'] == 'efa':
                        target = campaign.results / 'storage' / str(node['partition']) / 'final'
                        target.mkdir(parents=True)
                        supervisor.write_json(target / 'efa-proof.json', template['efa'][node['instance_id']])
            def snapshot(identity):
                template = templates[campaign.cell['ordinal']]
                proof = template['efa'][identity['instance_id']]
                return proof['after' if (campaign.results / 'efa-before.json').exists() else 'before']
            campaign.prepare.side_effect = prepare
            campaign.replay.side_effect = replay
            campaign.collect_node.side_effect = collect
            campaign.ssm_one.side_effect = lambda *args, **kwargs: events.append(('release', campaign.cell['cell_id']))
            with patch.object(supervisor, 'fabric_preflight'), patch.object(supervisor, 'efa_snapshot', side_effect=snapshot):
                campaign.run_matrix()
            self.assertTrue(campaign.state['complete'])
            self.assertEqual([cell['cell_id'] for cell in campaign.state['cells']], [cell['cell_id'] for cell in templates])
            self.assertEqual(campaign.replay.call_count, 8)
            self.assertEqual([cell['read_profile_limit'] for cell in campaign.state['cells']],
                             [0, 0, 4096, 4096, 4096, 4096, 0, 0])
            for index, template in enumerate(templates):
                self.assertEqual(events[index * 7:(index + 1) * 7],
                    [('prepare', template['cell_id']), ('paired-replay', template['cell_id'])]
                    + [('release', template['cell_id'])] * 5)

    def test_partial_export_preserves_prior_cells_and_current_logs(self):
        import tarfile
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            prior = campaign.results / 'cells/r01-tcp'
            prior.mkdir(parents=True)
            (prior / 'replay-evidence.tar.gz').write_bytes(b'prior completed evidence')
            campaign.results = campaign.campaign_results / 'cells/r01-efa'
            campaign.results.mkdir()
            campaign.cell = dict(cell_id='r01-efa', transport='efa', complete=False)
            campaign.state['failures'] = ['injected partial replay failure']
            campaign.replay_root = campaign.root / 'checkpoint-cells/r01-efa/replay'
            experiment = campaign.replay_root / 'experiment'
            experiment.mkdir(parents=True)
            (experiment / 'remote.stderr.log').write_text('partial replay stderr')
            for name, content in (
                    ('native-schema/perpetual/shape.json', 'partial native schema'),
                    ('remote-schema/perpetual/shape.yaml', 'partial remote schema'),
                    ('tls/private.pk8', 'secret'),
                    ('storage/node-0/data', 'bulk database')):
                path = experiment / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            (experiment / 'linked-secret').symlink_to(experiment / 'tls/private.pk8')
            campaign.retain_replay = lambda: supervisor.SuiCampaign.retain_replay(campaign)
            campaign.s3 = Mock()
            with patch.object(supervisor, 'evidence', return_value={}), patch.object(supervisor, 'upload_final', return_value={}):
                campaign.export()
            with tarfile.open(campaign.root / 'checkpoint-results.tar.gz') as archive:
                self.assertEqual(archive.extractfile('results/cells/r01-tcp/replay-evidence.tar.gz').read(), b'prior completed evidence')
                self.assertEqual(archive.extractfile('results/cells/r01-efa/replay-logs/remote.stderr.log').read(), b'partial replay stderr')
                self.assertEqual(archive.extractfile(
                    'results/cells/r01-efa/replay-logs/native-schema/perpetual/shape.json').read(),
                    b'partial native schema')
                self.assertEqual(archive.extractfile(
                    'results/cells/r01-efa/replay-logs/remote-schema/perpetual/shape.yaml').read(),
                    b'partial remote schema')
                for name in ('tls/private.pk8', 'storage/node-0/data', 'linked-secret'):
                    self.assertNotIn('results/cells/r01-efa/replay-logs/' + name, archive.getnames())
                self.assertNotIn('results/COMPLETE', archive.getnames())
                receipt = json.load(archive.extractfile('results/campaign-receipt.json'))
                self.assertFalse(receipt['complete'])

    def test_cell_failure_never_starts_later_cells(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.prepare.side_effect = RuntimeError('injected deployment failure')
            with self.assertRaisesRegex(RuntimeError, 'deployment failure'):
                campaign.run_matrix()
            self.assertEqual([cell['cell_id'] for cell in campaign.state['cells']], ['r01-tcp'])
            campaign.install_node.assert_not_called()
            self.assertFalse(campaign.state['complete'])

    def test_work_deadline_cannot_silently_shrink_matrix(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            with patch.object(supervisor, 'remaining', return_value=59), self.assertRaises(TimeoutError):
                campaign.run_matrix()
            campaign.prepare.assert_not_called()
            self.assertFalse(campaign.state['complete'])

    def test_release_failure_prevents_next_cell_even_after_replay_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.campaign(Path(temporary))
            campaign.external = dict(config_sha256='a' * 64)
            campaign.ssm_one.side_effect = RuntimeError('owned process remains')
            with self.assertRaisesRegex(RuntimeError, 'owned process remains'):
                campaign.run_matrix()
            campaign.replay.assert_called_once()
            self.assertEqual(len(campaign.state['cells']), 1)
            self.assertFalse(campaign.state['cells'][0]['complete'])
            campaign.retain_replay.assert_not_called()


class PlacementTests(unittest.TestCase):
    def test_all_six_hosts_require_exact_cluster_and_efa(self):
        cfg = example_config()
        group = dict(GroupName=cfg['placement_group'], GroupId='pg-owned', Strategy='cluster', State='available')
        instances = [dict(InstanceId=instance_id, InstanceType=cfg['instance_type'],
            Placement=dict(AvailabilityZone=cfg['availability_zone'], GroupName=group['GroupName'], GroupId=group['GroupId']),
            NetworkInterfaces=[dict(InterfaceType='efa', Status='in-use', Attachment=dict(Status='attached'))])
            for instance_id in [cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])]]
        matrix.validate_placement(cfg, instances, [group])
        cfg = cfg | dict(transports=['efa-direct'])
        matrix.validate_placement(cfg, instances, [group])
        for index in range(6):
            for mutation in ('zone', 'group', 'group-id', 'hardware', 'interface', 'detached'):
                changed = copy.deepcopy(instances)
                instance = changed[index]
                if mutation in ('zone', 'group', 'group-id'):
                    field = {'zone': 'AvailabilityZone', 'group': 'GroupName', 'group-id': 'GroupId'}[mutation]
                    instance['Placement'][field] = 'foreign'
                elif mutation == 'hardware':
                    instance['InstanceType'] = 'i4i.xlarge'
                elif mutation == 'interface':
                    instance['NetworkInterfaces'][0]['InterfaceType'] = 'interface'
                else:
                    instance['NetworkInterfaces'][0]['Attachment']['Status'] = 'detached'
                with self.subTest(index=index, mutation=mutation), self.assertRaises(ValueError):
                    matrix.validate_placement(cfg, changed, [group])
        for change in (dict(Strategy='spread'), dict(State='pending'), dict(GroupName='foreign')):
            with self.subTest(change=change), self.assertRaises(ValueError):
                matrix.validate_placement(cfg, instances, [group | change])
        with self.assertRaises(ValueError):
            matrix.validate_placement(cfg, instances[:-1], [group])


if __name__ == '__main__':
    unittest.main()
