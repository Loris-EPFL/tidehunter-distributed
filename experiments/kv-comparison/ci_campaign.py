#!/usr/bin/env python3
"""Protected GitHub KV campaign using the maintained atomic fleet lifecycle."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import sys
import tarfile
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
OLD = HERE.parent / 'aws-sharded-wal'
# Import KV's own modules first: both suites deliberately have a local.py.
import cloud_bootstrap
import cloud_campaign
import cloud_controller
import cloud_layout
import cloud_package
import ci_prepare
import local

# Keep delayed maintained imports available without shadowing KV modules.
sys.path.append(str(OLD))
_spec = importlib.util.spec_from_file_location('kv_maintained_ci_campaign', OLD / 'ci_campaign.py')
core = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = core
_spec.loader.exec_module(core)
from portable_transport import select_backends


def load_config(path, env=None, backend_selection=None, mode="standard"):
    """Separate KV admission; the original Sui lifetime/matrix stays unchanged."""
    env = os.environ if env is None else env
    cfg = core.read_json(path)
    required = ('bucket', 'bucket_region', 'guest_boundary_arn', 'watchdog_function', 'watchdog_rule')
    if {'weekly_budget_usd', 'weekly_limit_usd', 'budget_path'} & cfg.keys():
        raise ValueError('the maintained budget.json is the only budget authority')
    for name in required:
        if not cfg.get(name):
            cfg[name] = env.get('AWS_BENCH_' + name.upper())
        if not cfg[name]:
            raise ValueError('required setup output absent: ' + name)
    if (cfg.get('schema') != 1 or cfg.get('suite') != 'kv-comparison'
            or cfg.get('repository') != core.REPOSITORY or cfg.get('branch') != core.BRANCH
            or cfg.get('account') != cloud_controller.ACCOUNT
            or cfg.get('ttl_seconds') != 10800 or cfg.get('export_reserve_seconds') != 600
            or not isinstance(cfg.get('regions'), list) or not cfg['regions']
            or len(set(cfg['regions'])) != len(cfg['regions']) or not set(cfg['regions']) <= set(core.REGIONS)
            or not isinstance(cfg.get('instance_types'), list) or not cfg['instance_types']
            or len(set(cfg['instance_types'])) != len(cfg['instance_types'])
            or not set(cfg['instance_types']) <= set(core.TYPES)):
        raise ValueError('exact protected KV suite, approved EFA/NVMe candidates and 10800/600 lifetime required')
    if (cfg['bucket'] != 'tidehunter-ci-354775971805-us-east-1' or cfg['bucket_region'] != 'us-east-1'
            or not re.fullmatch(r'arn:aws:iam::' + cfg['account'] + r':policy/(?:[\w+=,.@-]+/)*[\w+=,.@-]+',
                                cfg['guest_boundary_arn'])):
        raise ValueError('approved same-account bucket and immutable guest boundary required')
    overrides = cfg.get('regional_overrides', {})
    if not isinstance(overrides, dict) or not set(overrides) <= set(cfg['regions']):
        raise ValueError('regional overrides outside approved regions')
    for name in ('retention_days', 'result_gib', 'request_count', 'reconciliation_seconds'):
        if type(cfg.get('cost_assumptions', {}).get(name)) is not int or cfg['cost_assumptions'][name] <= 0:
            raise ValueError('positive reviewed cost_assumptions.' + name + ' required')
    raw_bound = cfg.get('raw_export_max_gib')
    if type(raw_bound) is not int or raw_bound < 32 or cfg['cost_assumptions']['result_gib'] < raw_bound:
        raise ValueError('explicit raw export bound >=32GiB must be covered by the shared cost reservation')
    resources = cfg.get('fleet', {})
    if set(resources) != {'server_cpus', 'server_memory_mib', 'client_cpus', 'client_memory_mib'}:
        raise ValueError('explicit equal owner/coordinator resource caps required')
    for name, value in resources.items():
        low, high = (2, 256) if name.endswith('cpus') else (4096, 1048576)
        if type(value) is not int or not low <= value <= high:
            raise ValueError('invalid fleet resource cap: ' + name)
    spec = core.read_json(HERE / 'cloud-campaign.json')
    if (spec.get('hard_deadline_seconds') != cfg['ttl_seconds']
            or spec.get('export_reserve_seconds') != cfg['export_reserve_seconds']):
        raise ValueError('committed campaign lifetime must match approved CI reservations')
    spec['backends'] = select_backends(backend_selection, spec.get('backends'),
                                       set(cloud_campaign.BACKENDS))
    spec['mode'] = mode
    cloud_controller.validate_read_tier_memory(cloud_campaign.make_matrix(spec), resources)
    cfg['spec'] = spec
    return cfg


def admit_inputs(inputs, output):
    """Reuse qualification/source admission and verify the actual guest archive."""
    inputs = Path(inputs)
    ready = ci_prepare.admit_inputs(inputs)
    verified = cloud_package.verify_archive(inputs / 'bundle.tar.gz', ready['bundle_sha256'])
    if any(verified[key] != ready[key] for key in ('manifest_sha256', 'source_sha256', 'binary_sha256')):
        raise ValueError('actual guest archive differs from source-qualified prepared directory')
    manifest = core.read_json(inputs / 'package/manifest.json')
    if any(not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}', image) for image in local.IMAGES.values()):
        raise ValueError('third-party runtime images must be digest pinned')
    lock = core.read_json(inputs / 'package/source/experiments/kv-comparison/observability/images.lock.json')
    if (set(lock.get('images', {})) != {'prom/prometheus', 'grafana/grafana', 'prom/node-exporter', 'library/python'}
            or any(not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}', entry.get('image', ''))
                   for entry in lock['images'].values())):
        raise ValueError('complete digest-pinned monitoring image lock required')
    bindings = {name: ready[name] for name in ('source_commit', 'bundle_sha256', 'source_sha256',
                                              'source_tree_sha256', 'binary_sha256', 'manifest_sha256')}
    bindings['binary_blake2b512'] = manifest['binary_blake2b512']
    admission = dict(bindings=bindings, inputs={'runner_bundle': dict(path=str((inputs / 'bundle.tar.gz').resolve()),
        sha256=ready['bundle_sha256'], bytes=(inputs / 'bundle.tar.gz').stat().st_size)},
        bootstrap_sha256=core.sha(HERE / 'cloud_bootstrap.py'),
        atomic_start_sha256=core.sha(HERE / 'cloud_controller.py'), qualification=ready)
    Path(output).mkdir(parents=True, exist_ok=True)
    core.dump(Path(output) / 'input-admission.json', admission)
    return admission


def guest_policy(cfg, run_id, role, *, region, expires_at, instances=None):
    """Narrow the existing generated template; storage can HEAD its own writes."""
    import control
    if role not in core.ROLES:
        raise ValueError('invalid final guest role')
    params = dict(bucket=cfg['bucket'], prefix=run_id, run_id=run_id, account=cfg['account'], region=region,
        tags=core.ownership(run_id) | {'ExpiresAt': expires_at}, atomic_controller=True, protected_instances=[])
    base = control.role_policy(params, 'controller' if role == 'controller' else 'node')
    read_actions = [action for action in base['Statement'][1]['Action'] if action == 's3:GetObject']
    write_actions = [action for action in base['Statement'][1]['Action'] if action != 's3:GetObject']
    reads = [run_id + '/inputs/bundle.tar.gz']
    writes = []
    if instances is not None:
        if len(instances) != 6 or {item['AmiLaunchIndex'] for item in instances} != set(range(6)):
            raise ValueError('exact six launch-index identities required for final guest policy')
        if role == 'controller':
            reads += [run_id + '/inputs/controller.json', run_id + '/results/*', run_id + '/deploy/cells/*']
            writes += [run_id + '/results/*', run_id + '/deploy/cells/*']
        else:
            index = int(role.rsplit('-', 1)[1])
            writes += [f'{run_id}/results/cells/*/storage/{index}/*']
            reads += [f'{run_id}/deploy/cells/*/storage-{index}.tar.gz']
    reads += writes
    listing = dict(base['Statement'][0], Condition={'StringLike': {'s3:prefix': sorted(set(reads + writes))}})
    statements = [listing, dict(Effect='Allow', Action=read_actions,
        Resource=[f"arn:aws:s3:::{cfg['bucket']}/{key}" for key in sorted(set(reads))])]
    if writes:
        statements.append(dict(Effect='Allow', Action=write_actions,
            Resource=[f"arn:aws:s3:::{cfg['bucket']}/{key}" for key in sorted(set(writes))]))
    if instances is not None and role == 'controller':
        statements.extend(base['Statement'][2:])
        for statement in statements:
            if statement.get('Action') == 'ec2:TerminateInstances':
                statement['Resource'] = [f"arn:aws:ec2:{region}:{cfg['account']}:instance/{item['InstanceId']}"
                                         for item in instances]
        statements.extend(control.command_policy(params,
            [item['InstanceId'] for item in instances if item['AmiLaunchIndex'] < 5])['Statement'])
    return dict(Version=base['Version'], Statement=statements)


def live_preflight(aws, cfg):
    result = core.live_preflight(aws, cfg)
    accepted, rejected = [], result['rejected']
    for candidate in result['candidates']:
        hardware = candidate['capability']
        cpus, memory = hardware['VCpuInfo']['DefaultVCpus'], hardware['MemoryInfo']['SizeInMiB']
        if (max(cfg['fleet']['server_cpus'], cfg['fleet']['client_cpus']) + 2 > cpus
                or max(cfg['fleet']['server_memory_mib'], cfg['fleet']['client_memory_mib']) + 4096 > memory):
            rejected.append(dict(region=candidate['region'], instance_type=candidate['instance_type'],
                                 reason='measured resource caps plus host/monitor reserve exceed hardware'))
        else:
            accepted.append(candidate)
    if not accepted:
        raise ValueError('no approved EFA/NVMe candidate fits the physical resource caps')
    result['candidates'] = accepted
    return result


def result_inventory(receipt, run_id):
    descriptor = receipt.get('inventory')
    if (receipt.get('schema_version') != 2 or not isinstance(descriptor, dict)
            or set(descriptor) != {'key', 'bytes', 'sha256'}
            or descriptor['key'] != run_id + '/results/controller-inventory.json'
            or type(descriptor['bytes']) is not int
            or not 0 < descriptor['bytes'] <= cloud_controller.MAX_JSON_BYTES
            or not isinstance(descriptor['sha256'], str)
            or not re.fullmatch('[a-f0-9]{64}', descriptor['sha256'])):
        raise ValueError('invalid source-bound result inventory descriptor')
    return descriptor


class Campaign(core.Campaign):
    guest_policy = staticmethod(guest_policy)

    def run(self, admission, preflight):
        try:
            return super().run(admission, preflight)
        except BaseException as error:
            target = self.output / 'campaign-validation.json'
            if not target.exists():
                cfg = (self.journal.data or {}).get('controller_config')
                if cfg and (self.output / 's3' / self.run_id / 'results/controller.tar.gz').is_file():
                    try:
                        self.validate_results(cfg)
                    except BaseException as validation_error:
                        if not target.exists():
                            core.dump(target, dict(complete=False, benchmark_complete=None, monitoring_complete=None,
                                lifecycle_error=repr(error), validation_error=repr(validation_error)))
                else:
                    core.dump(target, dict(complete=False, benchmark_complete=None, monitoring_complete=None,
                        lifecycle_error=repr(error), raw_export_available=False))
            raise

    def upload(self, path, key):
        # The retained allocator calls its generic runner key /input/bundle;
        # KV bootstrap has one explicit, already-reviewed /inputs layout.
        if key == self.run_id + '/input/bundle.tar.gz':
            key = self.run_id + '/inputs/bundle.tar.gz'
        return super().upload(path, key)

    def pending_activation(self, plan):
        pending = cloud_bootstrap.activation_record(plan)
        raw = core.encoded(pending)
        key = self.run_id + '/inputs/controller.json'
        try:
            response = self.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=raw,
                ExpectedBucketOwner=self.cfg['account'], ServerSideEncryption='AES256', IfNoneMatch='*',
                Metadata={'sha256': hashlib.sha256(raw).hexdigest()})
            etag = response['ETag']
        except Exception as error:
            if core.error_code(error) not in ('PreconditionFailed', '412'):
                raise
            response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
            with response['Body'] as body:
                existing = body.read(1024 * 1024 + 1)
            if existing != raw:
                raise ValueError('existing activation is not this exact pending source/run; never overwrite ready')
            etag = response['ETag']
        if not etag:
            raise ValueError('durable pending activation lacks an ETag')
        self.journal.data['activation'] = dict(key=key, pending_etag=etag, state='pending')
        self.journal.save()

    def request(self, candidate, group_id, admission, profiles):
        runner, bindings = admission['inputs']['runner_bundle'], admission['bindings']
        boot = dict(run_id=self.run_id, account=self.cfg['account'], region=candidate['region'],
            bucket=self.cfg['bucket'], bucket_region=self.cfg['bucket_region'],
            bundle_key=self.run_id + '/inputs/bundle.tar.gz', bundle_sha256=runner['sha256'], bundle_bytes=runner['bytes'],
            source_sha256=bindings['source_sha256'], binary_sha256=bindings['binary_sha256'],
            deadline_utc=self.journal.data['expires_at'], ttl_seconds=self.cfg['ttl_seconds'], instance_type=candidate['instance_type'])
        if self.cfg['spec'].get('mode', 'standard') == 'read-tier':
            boot['read_tier_trace'] = True
        userdata = cloud_bootstrap.render_userdata(boot)
        self.pending_activation(boot)  # Must succeed before the allocator's RunInstances intent.
        tags = core.ownership(self.run_id) | cloud_controller.TAGS | {
            'ExpiresAt': boot['deadline_utc'], 'Role': 'bootstrap', 'Name': self.run_id}
        request = dict(ImageId=candidate['ami'], InstanceType=candidate['instance_type'], MinCount=6, MaxCount=6,
            ClientToken=core.candidate_token(self.run_id, candidate),
            NetworkInterfaces=[dict(DeviceIndex=0, NetworkCardIndex=0, InterfaceType='efa', SubnetId=candidate['subnet'],
                Groups=[group_id], DeleteOnTermination=True, AssociatePublicIpAddress=True)],
            Placement=dict(AvailabilityZone=candidate['az'], GroupName=self.run_id),
            BlockDeviceMappings=[dict(DeviceName=candidate['root_device'], Ebs=dict(VolumeSize=candidate['root_gib'],
                VolumeType='gp3', Encrypted=True, DeleteOnTermination=True))],
            MetadataOptions=dict(HttpEndpoint='enabled', HttpTokens='required', HttpPutResponseHopLimit=1,
                InstanceMetadataTags='enabled'), InstanceInitiatedShutdownBehavior='terminate',
            TagSpecifications=[dict(ResourceType=kind, Tags=core.aws_tags(tags))
                               for kind in ('instance', 'volume', 'network-interface')], UserData=userdata)
        return request, dict(bootstrap=boot, atomic_bindings=bindings,
                            atomic_client_token=request['ClientToken'], instance_profiles=profiles)

    def configure(self, instances, candidate, plan):
        end = min(self.clock.time() + 300, core.parsed(self.journal.data['expires_at']).timestamp() - 1200)
        while self.clock.time() < end:
            current = self.validate_fleet(self.active(), candidate)
            if all(item['State']['Name'] == 'running' and item.get('PrivateIpAddress') for item in current):
                break
            self.clock.sleep(5)
        else:
            raise TimeoutError('exact six-host running readiness expired')
        current = self.bind_profiles(current, candidate, plan['instance_profiles'], end)
        index = {item['AmiLaunchIndex']: item for item in current}
        for item in current:
            if not any(nic.get('InterfaceType') == 'efa' for nic in item.get('NetworkInterfaces', [])):
                raise ValueError('exact allocated host lacks an attached EFA interface')
        ec2 = self.aws.client('ec2', candidate['region'])
        groups = ec2.describe_placement_groups(GroupNames=[self.run_id])['PlacementGroups']
        if len(groups) != 1 or groups[0].get('Strategy') != 'cluster' or groups[0].get('State') != 'available':
            raise ValueError('mandatory available cluster placement group differs')
        self.exact(groups[0])
        for i in range(6):
            self.mutate('create_tags', candidate['region'], dict(Resources=[index[i]['InstanceId']],
                Tags=core.aws_tags({'Role': 'controller' if i == 5 else f'storage-{i}'})))
        boot, bindings = plan['bootstrap'], plan['atomic_bindings']
        stage = Path('/mnt/tidehunter') / self.run_id / 'package'
        fleet = dict(schema_version=1, run_id=self.run_id, region=candidate['region'], replication_factor=1,
            qualification_only=False, data_root='/mnt/tidehunter-kv-comparison/' + self.run_id,
            coordinator=dict(instance_id=index[5]['InstanceId'], address=index[5]['PrivateIpAddress']),
            nodes=[dict(index=i, instance_id=index[i]['InstanceId'], address=index[i]['PrivateIpAddress']) for i in range(5)],
            **self.cfg['fleet'])
        cfg = dict(fleet=fleet, bucket=self.cfg['bucket'], bucket_region=self.cfg['bucket_region'], prefix=self.run_id,
            account=self.cfg['account'], source_revision=bindings['source_commit'], source_sha256=bindings['source_sha256'],
            binary_sha256=bindings['binary_sha256'], binary_blake2b512=bindings['binary_blake2b512'],
            repository_root=str(stage / 'source'), binary=str(stage / 'runtime/bin/kv-research'),
            fdb_library_dir=str(stage / 'runtime/lib'), deadline_utc=boot['deadline_utc'], spec=self.cfg['spec'])
        cloud_controller.validate_config(cfg)
        # Revalidate exact tags, expiry, private addresses and six independent IDs
        # before publishing the sole ready transition. Guests also do this live.
        refreshed = self.validate_fleet(self.active(), candidate)
        cloud_controller.verify_instances(cfg, {'Reservations': [{'OwnerId': self.cfg['account'], 'Instances': refreshed}]},
                                          starting=True)
        envelope = cloud_bootstrap.activation_record(boot, cfg)
        raw = core.encoded(envelope)
        activation = self.journal.data['activation']
        if activation['state'] != 'pending':
            raise ValueError('activation already transitioned; never rebind a paid fleet')
        response = self.s3.put_object(Bucket=self.cfg['bucket'], Key=activation['key'], Body=raw,
            ExpectedBucketOwner=self.cfg['account'], ServerSideEncryption='AES256',
            IfMatch=activation['pending_etag'], Metadata={'sha256': hashlib.sha256(raw).hexdigest()})
        activation.update(state='ready', ready_etag=response['ETag'], sha256=hashlib.sha256(raw).hexdigest())
        self.journal.data['controller_config'] = cfg
        self.journal.save()
        core.dump(self.output / 'controller-config.json', cfg)
        return cfg

    def monitor(self, cfg):
        end = core.parsed(cfg['deadline_utc']).timestamp() - 15
        while self.clock.time() < end:
            receipt = self.object_json(self.run_id + '/results/receipt.json')
            if receipt is not None:
                # Incomplete/export-failed receipts are terminal evidence, not
                # permission to wait until TTL or to call a partial run success.
                if (receipt.get('key') != self.run_id + '/results/controller.tar.gz'
                        or receipt.get('source_sha256') != cfg['source_sha256']
                        or receipt.get('binary_blake2b512') != cfg['binary_blake2b512']
                        or not re.fullmatch(r'[a-f0-9]{64}', receipt.get('sha256', ''))
                        or type(receipt.get('bytes')) is not int or receipt['bytes'] <= 0):
                    raise ValueError('terminal controller export is missing or differs from admitted source/binary')
                head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=receipt['key'], ExpectedBucketOwner=self.cfg['account'])
                if head['ContentLength'] != receipt['bytes'] or head.get('Metadata', {}).get('sha256') != receipt['sha256']:
                    raise ValueError('controller export not durably bound before termination')
                inventory = result_inventory(receipt, self.run_id)
                head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=inventory['key'],
                                          ExpectedBucketOwner=self.cfg['account'])
                if (head.get('ContentLength') != inventory['bytes']
                        or head.get('Metadata', {}).get('sha256') != inventory['sha256']):
                    raise ValueError('bulk result inventory not durably bound before termination')
                core.dump(self.output / 'export-receipt.json', receipt)
                return receipt
            if not self.active():
                raise RuntimeError('fleet terminated without final raw export')
            self.clock.sleep(10)
        raise TimeoutError('absolute campaign/export deadline reached')

    def validate_results(self, cfg, *, require_complete=True):
        """Validate raw evidence; only cleanup may retain an incomplete outcome."""
        base = self.output / 's3' / self.run_id / 'results'
        receipt = core.read_json(base / 'receipt.json')
        archive = base / 'controller.tar.gz'
        if archive.stat().st_size != receipt['bytes'] or core.sha(archive) != receipt['sha256']:
            raise ValueError('retrieved controller archive differs from durable export receipt')
        root = self.output / 'validated-results'
        root.mkdir(parents=True, exist_ok=True)
        descriptor = result_inventory(receipt, self.run_id)
        inventory_path = base / 'controller-inventory.json'
        cloud_package.regular(inventory_path)
        if (inventory_path.stat().st_size != descriptor['bytes']
                or core.sha(inventory_path) != descriptor['sha256']):
            raise ValueError('retrieved bulk result inventory differs from durable export receipt')
        inventory = core.read_json(inventory_path)
        if (not isinstance(inventory, dict) or not inventory or len(inventory) > 200000
                or any(type(entry.get('bytes')) is not int or entry['bytes'] < 0
                       or not re.fullmatch(r'[a-f0-9]{64}', entry.get('sha256', '')) for entry in inventory.values())
                or sum(entry['bytes'] for entry in inventory.values()) > self.cfg['raw_export_max_gib'] * 1024**3):
            raise ValueError('controller raw inventory missing, invalid or beyond reviewed export bound')
        seen = set()
        with tarfile.open(archive, 'r:gz') as stream:
            for member in stream:
                name = PurePosixPath(member.name)
                if (not member.isfile() or name.is_absolute() or '..' in name.parts or str(name) != member.name
                        or member.name in seen or member.name not in inventory
                        or member.size != inventory[member.name]['bytes']):
                    raise ValueError('unsafe/unreceipted/duplicate result archive member')
                seen.add(member.name)
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with stream.extractfile(member) as source, target.open('wb') as destination:
                    shutil.copyfileobj(source, destination)
                expected = inventory[member.name]
                if target.stat().st_size != expected['bytes'] or core.sha(target) != expected['sha256']:
                    raise ValueError('raw result file differs from export inventory: ' + member.name)
        if seen != set(inventory):
            raise ValueError('raw archive omits receipted files')
        expected_jobs = cloud_campaign.make_matrix(cfg['spec'])
        missing_files = []
        controller_outcome = (core.read_json(root / 'controller-outcome.json')
                              if 'controller-outcome.json' in seen else None)
        if 'matrix.json' not in seen:
            # Startup can fail before the matrix is written. Do not fabricate a
            # report (or reuse a matrix left by an earlier local validation).
            missing_files.append('matrix.json')
            report = dict(complete=False, completed_cases=None)
        else:
            planned = core.read_json(root / 'matrix.json')
            if planned != {'schema_version': 1, 'spec': cfg['spec'], 'jobs': expected_jobs}:
                raise ValueError(f'exported matrix differs from all {len(expected_jobs)} admitted cases')
            report = cloud_campaign.report(root)
        monitor_error = None
        try:
            if not {'observability/receipt.json', 'observability/monitoring.tar.gz'} <= seen:
                raise ValueError('controller export lacks monitoring receipt or archive')
            monitoring = core.read_json(root / 'observability/receipt.json')
            monitor_complete = monitoring.get('complete') is True and receipt.get('monitoring_complete') is True
            monitor_spec = importlib.util.spec_from_file_location('kv_ci_monitor', HERE / 'observability/campaign_monitor.py')
            observer = importlib.util.module_from_spec(monitor_spec)
            monitor_spec.loader.exec_module(observer)
            with tempfile.TemporaryDirectory(prefix='kv-ci-monitor-') as temporary:
                restored = Path(temporary)
                capture = observer.unpack_archive((root / 'observability/monitoring.tar.gz').resolve(), restored)
                verification = core.read_json(restored / 'evidence/verification.json')
                expected_ids = {job['case_id'] for job in expected_jobs}
                monitor_complete = (monitor_complete and capture.get('complete') is True
                    and capture.get('run_id') == self.run_id and capture.get('snapshot', {}).get('complete') is True
                    and verification.get('complete') is True
                    and set(capture.get('expected_case_ids', [])) == expected_ids
                    and set(verification.get('evidence', {}).get('actual_seen_case_ids', [])) == expected_ids
                    and any((restored / 'snapshot').rglob('meta.json'))
                    and any(path.is_file() and path.parent.name == 'chunks' for path in (restored / 'snapshot').rglob('*')))
        except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
            monitor_complete, monitor_error = False, repr(error)
        outcome = dict(schema_version=1, suite='kv-comparison', planned_cases=len(expected_jobs),
            completed_cases=report['completed_cases'], benchmark_complete=report['complete'],
            monitoring_complete=monitor_complete, controller_status=receipt.get('status'),
            controller_error=receipt.get('error') or (controller_outcome or {}).get('error'),
            controller_outcome=controller_outcome, missing_files=missing_files,
            monitoring_error=monitor_error,
            complete=report['complete'] and monitor_complete and receipt.get('status') == 'completed'
                and not receipt.get('error') and not (controller_outcome or {}).get('error'))
        core.dump(self.output / 'campaign-validation.json', outcome)
        if require_complete and not outcome['complete']:
            raise RuntimeError('incomplete campaign: ' + json.dumps(outcome, sort_keys=True))
        return outcome


def cleanup_campaign(campaign):
    """Always settle even when retrieval fails; cleanup failure never looks green."""
    cleanup = campaign.cleanup()
    try:
        campaign.retrieve()
    finally:
        campaign.settle(cleanup)
    return cleanup['cleanup_complete']


def validate_cleanup_results(campaign, cleanup_complete):
    """Keep verified resource closure independent of benchmark completion.

    Invalid/unsafe evidence still raises, but an honestly incomplete export is
    persisted without making a successfully closed fleet look like a leak.
    """
    summary = dict(schema_version=1, resource_cleanup_complete=cleanup_complete,
                   campaign_complete=None)
    try:
        controller = (campaign.journal.data or {}).get('controller_config')
        if controller:
            archive = campaign.output / 's3' / campaign.run_id / 'results/controller.tar.gz'
            if archive.is_file():
                outcome = campaign.validate_results(controller, require_complete=False)
            else:
                outcome = dict(complete=False, benchmark_complete=None, monitoring_complete=None,
                    raw_export_available=False, lifecycle_error='activated campaign has no retained controller export')
                core.dump(campaign.output / 'campaign-validation.json', outcome)
            summary['campaign_complete'] = outcome['complete']
            summary['campaign_validation'] = outcome
    except Exception as error:
        summary.update(campaign_complete=False, validation_error=repr(error))
        raise
    finally:
        core.dump(campaign.output / 'cleanup-validation.json', summary)
    return cleanup_complete


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('run', 'cleanup', 'validate'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--backends', help='comma-separated nonempty subset of tracked backend IDs')
    parser.add_argument('--mode', choices=('standard', 'read-tier'), default='standard')
    parser.add_argument('--budget-file', type=Path, default=core.DEFAULT_BUDGET)
    args = parser.parse_args(argv)
    if args.action == 'validate':
        cfg = load_config(args.config, backend_selection=args.backends, mode=args.mode)
        jobs = cloud_campaign.make_matrix(cfg['spec'])
        print(json.dumps({'status': 'valid', 'selected_backends': cfg['spec']['backends'],
                          'planned_cases': len(jobs)}))
        return 0
    if args.output is None:
        parser.error('--output is required for run/cleanup')
    cfg = run_id = campaign = aws = None
    work_started = False
    try:
        cfg = load_config(args.config, backend_selection=args.backends, mode=args.mode)
        cfg['budget_path'] = str(args.budget_file.absolute())
        # Derive before source admission so a restarted/cancelled same-attempt
        # process can reconcile owned hosts even if its local inputs vanished.
        run_id = core.trusted_run()
        if args.action == 'run':
            if args.inputs is None:
                raise ValueError('--inputs is required for run')
            admission = admit_inputs(args.inputs, args.output)
        aws = core.AWS()
        if aws.client('sts', cfg['bucket_region']).get_caller_identity().get('Account') != cfg['account']:
            raise ValueError('caller account differs from approved owner')
        campaign = Campaign(aws, cfg, run_id, args.output, cleanup_only=args.action == 'cleanup')
        if args.action == 'cleanup':
            complete = cleanup_campaign(campaign)
            return 0 if validate_cleanup_results(campaign, complete) else 1
        preflight = live_preflight(aws, cfg)
        core.dump(args.output / 'live-preflight.json', preflight)
        def interrupted(number, frame):
            raise InterruptedError('GitHub cancellation signal ' + str(number))
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        work_started = True
        campaign.run(admission, preflight)
        return 0
    except BaseException as error:
        if args.action == 'run' and cfg and run_id and not work_started:
            try:
                aws = aws or core.AWS()
                if aws.client('sts', cfg['bucket_region']).get_caller_identity().get('Account') != cfg['account']:
                    raise ValueError('emergency cleanup caller account differs')
                campaign = Campaign(aws, cfg, run_id, args.output, cleanup_only=True)
                cleanup_campaign(campaign)
            except BaseException as cleanup_error:
                core.dump(args.output / 'emergency-cleanup-failure.json', dict(error=repr(cleanup_error)))
        core.dump(args.output / 'operator-failure.json', dict(error=repr(error), at=core.utc(),
            **core.config_audit(cfg, campaign.journal.data if campaign else None)))
        print('kv ci_campaign: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
