"""Source-bound KV measurements inside the Sui controller's existing fleet lifetime.

Reuse the tested storage-node lifecycle and real WAL benchmark. This module does
not provision, rebuild, extend deadlines, or terminate a second fleet. The outer
Sui supervisor owns unconditional export and termination, including failures.
"""
import json
import shutil
import tarfile

from checkpoint_matrix import efa_snapshot, efa_proof, validate_efa_proof, validate_provider
from guest import Campaign, upload, write_json, sha


def validate_options(options):
    if options is None:
        return
    if (not isinstance(options, dict)
            or not {'repetitions', 'diagnostics'} <= options.keys()
            or options.keys() - {'repetitions', 'diagnostics', 'scope'}
            or type(options['repetitions']) is not int or not 1 <= options['repetitions'] <= 3
            or type(options['diagnostics']) is not bool
            or options.get('scope', 'full') not in ('full', 'targeted')):
        raise ValueError('kv_suite requires repetitions 1..3, boolean diagnostics, and optional full/targeted scope')


def plan(options, transports):
    validate_options(options)
    if options is None:
        return []
    if options.get('scope', 'full') == 'targeted':
        cases = []
        for repeat in range(options['repetitions']):
            for transport in (transports if repeat % 2 == 0 else transports[::-1]):
                for value, batch, window, cached, shard_cache, cold, chunk, layout in (
                        (256, 1, 1, False, 0, False, 16, 'small'),
                        (256, 1, 1, False, 8 * 1024**2, False, 16, 'small'),
                        (8192, 1, 1, False, 0, False, 16, 'small'),
                        (8192, 1, 1, False, 8 * 1024**2, False, 16, 'small'),
                        (8192, 1, 1, True, 0, False, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 32, 'small'),
                        (8192, 32, 32, False, 8 * 1024**2, False, 16, 'small'),
                        (8192, 32, 32, False, 0, True, 16, 'small'),
                        (8192, 32, 32, False, 0, False, 32, 'sui')):
                    cases.append(dict(shape=[value, batch, window], repetition=repeat,
                        transport=transport, cold=cold, cached=cached,
                        diagnostics=False,
                        authority_resident_batches=None, shard_cache_bytes=shard_cache,
                        preparation_chunk_batches=chunk, wal_layout=layout,
                        workload_batches=3200 if batch == 32 else 320,
                        workload_reads=8192 if batch == 32 else 4096,
                        transport_floor=bool(repeat == 0 and value == 256 and not shard_cache)))
        if options['diagnostics']:
            for transport in transports:
                # Fresh case/repetition: stage clocks must not contaminate the
                # ordinary no-cache control used for cache comparisons.
                cases.append(dict(shape=[8192, 1, 1], repetition=options['repetitions'],
                    transport=transport, cold=False, cached=False, diagnostics=True,
                    authority_resident_batches=None, shard_cache_bytes=0,
                    preparation_chunk_batches=16, wal_layout='small',
                    workload_batches=320, workload_reads=4096, transport_floor=False))
        return cases
    cases = []
    shapes = [(256, 1, 1), (256, 32, 32), (8192, 1, 1), (8192, 32, 32)]
    for repeat in range(options['repetitions']):
        order = transports if repeat % 2 == 0 else transports[::-1]
        for transport in order:
            for shape in shapes:
                for family in ('warm', 'cache', 'cold'):
                    cases.append(dict(shape=list(shape), repetition=repeat, transport=transport,
                                      cold=family == 'cold', cached=family == 'cache', diagnostics=False,
                                      authority_resident_batches=None))
            cases.append(dict(shape=[8192, 1, 1], repetition=repeat, transport=transport,
                              cold=True, cached=False, diagnostics=False,
                              authority_resident_batches=32))
    if options['diagnostics']:
        for transport in transports:
            for shape in shapes:
                cases.append(dict(shape=list(shape), repetition=options['repetitions'], transport=transport,
                                  cold=True, cached=False, diagnostics=True,
                                  authority_resident_batches=None))
    return cases


class CheckpointKvCampaign(Campaign):
    def __init__(self, parent):
        # Deliberately do not call the generic executor/constructor: its output
        # root, two-source rebuild and fleet termination belong to another run.
        self.cfg = dict(parent.cfg, campaign='efa-cache', efa=any(
                            transport in ('efa', 'efa-direct') for transport in parent.cfg['transports']),
                        repetitions=parent.cfg['kv_suite']['repetitions'], kv_subsuite=True)
        self.root, self.ids = parent.root, parent.ids
        self.bundle = self.root  # The admitted atomic bootstrap installs root/runner/*.py.
        self.results = parent.campaign_results / 'kv'
        self.results.mkdir()
        self.session, self.s3, self.ssm, self.ec2 = parent.session, parent.s3, parent.ssm, parent.ec2
        self.deadline = parent.deadline
        self.state = dict(complete=False, completed_cases=[], failures=[], plan=plan(
            parent.cfg['kv_suite'], parent.cfg['transports']),
            source_archive_sha256=parent.cfg['source_archive_sha256'],
            binary_sha256=parent.cfg['binaries']['storage-node'],
            floor_binary_sha256=parent.cfg['binaries']['transport-floor'],
            cold_scope='value caches cleared and WAL residency/physical IO checked per phase; not metadata-cold or per-key device timing',
            authority_scope='32-batch resident window stress; no claim every descriptor or metadata file page is cold')
        self.cloud_builds = {'current': {'binary_sha256': self.state['binary_sha256']}}
        self.active_payload = None
        self.parent = parent

    def progress(self, stage):
        self.state['stage'] = stage
        write_json(self.results / 'progress.json', self.state)
        self.parent.state['kv_suite'] = self.state
        self.parent.progress('kv-' + stage)

    def install(self):
        source = self.parent.build / 'binaries/storage-node'
        if sha(source) != self.state['binary_sha256']:
            raise ValueError('KV binary differs from admitted Sui storage executable')
        target = self.root / 'cloud-bin/current/sharded_wal_bench'
        target.parent.mkdir(parents=True, exist_ok=False)
        shutil.copy2(source, target)
        artifact = upload(self.s3, source, self.cfg, f"{self.cfg['s3_prefix']}/inputs/kv-storage-node")
        self.install_cloud_binary('current', artifact)
        floor_source = self.parent.build / 'binaries/transport-floor'
        if sha(floor_source) != self.state['floor_binary_sha256']:
            raise ValueError('transport-floor binary differs from admitted build')
        shutil.copy2(floor_source, target.with_name('sharded_wal_transport_bench'))
        write_json(self.results / 'binary-receipt.json', artifact)

    def verify_node_hardware(self, case_id, transport):
        for node in self.cfg['nodes']:
            archive = self.results / 'nodes' / case_id / f"node-{node['partition']}.tar.gz"
            with tarfile.open(archive) as stream:
                member = stream.getmember('efa-proof.json')
                if not member.isfile() or member.size > 1024 * 1024:
                    raise ValueError('KV EFA proof missing or too large')
                with stream.extractfile(member) as source:
                    proof = json.load(source)
                member = stream.getmember('efa-provider.txt')
                if not member.isfile() or member.size > 1024 * 1024:
                    raise ValueError('KV hardware provider evidence missing or too large')
                with stream.extractfile(member) as source:
                    validate_provider(source.read().decode(), transport)
            identity = dict(run_id=self.cfg['run_id'], case_id=case_id,
                            instance_id=node['instance_id'], binary_sha256=self.state['binary_sha256'])
            validate_efa_proof(proof, identity)

    def run_planned_case(self, ordinal, spec):
        identity = dict(run_id=self.cfg['run_id'], case_ordinal=ordinal,
                        instance_id=self.cfg['coordinator_instance_id'],
                        binary_sha256=self.state['binary_sha256'], specification=spec)
        before = None
        if spec['transport'] in ('efa', 'efa-direct'):
            from sui_checkpoint import fabric_preflight
            output = self.results / f"preflight-{ordinal}-{spec['transport']}"
            output.mkdir()
            fabric_preflight(output, spec['transport'])
            before = efa_snapshot(identity)
            write_json(self.results / f'efa-{ordinal}-before.json', before)
        completed_before = len(self.state['completed_cases'])
        self.run_case('current', tuple(spec['shape']), spec['repetition'], storage=True,
                      cold=spec['cold'], diagnostics=spec['diagnostics'], transport=spec['transport'],
                      cached=spec['cached'], resource_evidence=True,
                      authority_resident_batches=spec['authority_resident_batches'],
                      shard_cache_bytes=spec.get('shard_cache_bytes', 0),
                      transport_floor_probe=spec.get('transport_floor', False),
                      preparation_chunk_batches=spec.get('preparation_chunk_batches', 16),
                      workload_batches=spec.get('workload_batches'),
                      workload_reads=spec.get('workload_reads'), wal_layout=spec.get('wal_layout'))
        completed = self.state['completed_cases'][completed_before:]
        if len(completed) != 2 or {row['backend'] for row in completed} != {'native-sync', 'tcp'}:
            raise ValueError('KV case missing its native or distributed control')
        if before is not None:
            after = efa_snapshot(identity)
            write_json(self.results / f'efa-{ordinal}-after.json', after)
            write_json(self.results / f'efa-{ordinal}-proof.json', efa_proof(before, after, identity))
            case_ids = {row['case_id'] for row in completed}
            if len(case_ids) != 1:
                raise ValueError('KV paired case identities differ')
            self.verify_node_hardware(case_ids.pop(), spec['transport'])

    def run(self):
        try:
            write_json(self.results / 'plan.json', self.state)
            self.install()
            for ordinal, spec in enumerate(self.state['plan']):
                self.run_planned_case(ordinal, spec)
            if len(self.state['completed_cases']) != 2 * len(self.state['plan']):
                raise ValueError('KV suite incomplete')
            self.state['complete'] = True
        except BaseException as error:
            self.state['failures'].append(repr(error))
            raise
        finally:
            if self.active_payload is not None:
                try:
                    self.ssm_nodes(self.active_payload | {'action': 'stop'})
                    self.collect_node_results(self.active_payload['case_id'])
                except BaseException as error:
                    self.state['failures'].append('KV cleanup/export: ' + repr(error))
                    self.state['complete'] = False
            self.parent.state['kv_suite'] = self.state
            write_json(self.results / 'receipt.json', self.state)


def run_suite(parent):
    suite = CheckpointKvCampaign(parent)
    parent.state['kv_suite'] = suite.state
    suite.run()
    if not suite.state['complete']:
        raise RuntimeError('KV subsuite incomplete; outer supervisor must export and terminate')
