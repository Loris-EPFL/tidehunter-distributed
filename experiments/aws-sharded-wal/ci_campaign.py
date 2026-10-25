#!/usr/bin/env python3
"""Source-bound, unattended GitHub/OIDC six-host benchmark operator.

``preflight`` performs local admission and live read-only discovery. ``plan
--offline --run-id th-...`` never creates AWS clients. Explicit ``cleanup
--run-id th-...`` also permits account-checked local recovery using the immutable
owned journal, without building, uploading inputs or allocating compute. Guest IAM
is restricted to six exact per-run roles/profiles under a pre-provisioned boundary.
Every mutation intent precedes its request in an S3 conditional-write journal;
ambiguous RunInstances acknowledgements are reconciled, never blindly retried.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, localcontext
import gzip
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
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent

from bootstrap import INSTANCE_STORE_DISKS
from ci_budget import DEFAULT_BUDGET, load_budget
from checkpoint_matrix import TRANSPORTS
REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'
REGIONS = ('us-west-2', 'us-east-1', 'us-east-2', 'eu-west-1', 'eu-central-1')
TYPES = ('g6.8xlarge', 'g6e.8xlarge', 'g6.16xlarge', 'c6id.32xlarge')
ROLES = {'controller', *(f'storage-{i}' for i in range(5))}
BINARIES = {'sui-node-native', 'sui-node-remote', 'storage-node', 'state-audit', 'rocksdb-audit', 'transport-floor'}
CHECKPOINT = 'EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6'
CAPACITY_ERRORS = {'InsufficientInstanceCapacity', 'InstanceLimitExceeded', 'VcpuLimitExceeded',
                   'InsufficientFreeAddressesInSubnet', 'Unsupported', 'UnsupportedOperation'}
MISSING = {'NoSuchKey', '404', 'NotFound', 'InvalidGroup.NotFound', 'InvalidPlacementGroup.Unknown',
           'InvalidNetworkInterfaceID.NotFound', 'InvalidVolume.NotFound'}
ACTIVE = ('pending', 'running', 'stopping', 'stopped', 'shutting-down')


def utc(value=None):
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def parsed(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.utcoffset() is None or result.utcoffset().total_seconds() != 0:
        raise ValueError('absolute UTC timestamp required')
    return result


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()


def usd_ceiling(value):
    """Publish money at 12 decimal places, always rounding toward higher cost."""
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('USD values require a decimal string, Decimal or integer')
    with localcontext() as context:
        context.prec = 60
        number = Decimal(value)
        if not number.is_finite() or number < 0:
            raise ValueError('USD values must be finite and nonnegative')
        return number.quantize(Decimal('0.000000000001'), rounding=ROUND_CEILING)


def _reservation_quote(fleet_estimate, overhead, *, ttl_seconds=3600):
    with localcontext() as context:
        context.prec, context.rounding = 60, ROUND_CEILING
        fleet_estimate = usd_ceiling(fleet_estimate)
        reconciliation = usd_ceiling(fleet_estimate * Decimal(overhead['assumptions']['reconciliation_seconds']) / Decimal(ttl_seconds))
        return usd_ceiling(fleet_estimate + reconciliation + Decimal(overhead['estimate_usd'])), reconciliation


def ledger_candidates(candidates):
    """Small audited projection; full live hardware/price proofs remain journalled."""
    return [dict(
        **{key: candidate[key] for key in ('region', 'az', 'subnet', 'ami', 'instance_type')},
        root_gib=candidate.get('root_gib'),
        host_hourly_usd=str(usd_ceiling(candidate['host_hourly_usd'])),
        maximum_estimate_usd=str(usd_ceiling(candidate['maximum_estimate_usd'])),
        price_observed_utc=candidate['price'].get('observed_utc'),
        proof_sha256=hashlib.sha256(encoded(candidate)).hexdigest())
        for candidate in candidates]


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + '.pending')
    pending.write_bytes(encoded(value) + b'\n')
    pending.chmod(0o600)
    pending.replace(path)


def read_json(path):
    return json.loads(Path(path).read_bytes())


def config_audit(cfg, state=None):
    """Audit identity only; malformed/missing budget files never gate shutdown."""
    state = state or {}
    result = {'config_sha256': state.get('config_sha256') or
              (hashlib.sha256(encoded(cfg)).hexdigest() if cfg is not None else None)}
    policy = state.get('budget_policy')
    if (isinstance(policy, dict) and isinstance(policy.get('path'), str)
            and isinstance(policy.get('sha256'), str) and re.fullmatch(r'[a-f0-9]{64}', policy['sha256'])):
        result.update(budget_config_sha256=policy['sha256'], budget_config_path=policy['path'],
                      budget_config_source='recorded-policy')
        return result
    path = Path((cfg or {}).get('budget_path', DEFAULT_BUDGET))
    result.update(budget_config_path=str(path), budget_config_source='file-bytes', budget_config_sha256=None)
    try:
        if path.stat().st_size > 65536:
            raise ValueError('budget file exceeds audit size bound')
        result['budget_config_sha256'] = sha(path)
    except (OSError, ValueError, RuntimeError) as error:
        result['budget_config_error'] = repr(error)
    return result


def error_code(error):
    return (getattr(error, 'response', None) or {}).get('Error', {}).get('Code')


def tag_map(resource):
    return {item['Key']: item['Value'] for item in resource.get('Tags', [])}


def aws_tags(tags):
    return [{'Key': key, 'Value': value} for key, value in tags.items()]


def ownership(run_id):
    if not re.fullmatch(r'th-[a-f0-9]{16}', run_id):
        raise ValueError('invalid run ID')
    return dict(Project='TideHunter', ManagedBy='sharded-wal-benchmark', RunId=run_id,
                Repository=REPOSITORY, BenchmarkBranch=BRANCH)


def candidate_token(run_id, candidate):
    """EC2 idempotency is one immutable launch identity, not a whole campaign."""
    ownership(run_id)
    identity = {key: candidate[key] for key in ('region', 'az', 'subnet', 'ami', 'instance_type')}
    if any(not isinstance(value, str) or not value for value in identity.values()):
        raise ValueError('candidate token identity is incomplete')
    return run_id + '-atomic-' + hashlib.sha256(encoded(identity)).hexdigest()[:24]


def trusted_run(env=None):
    env = os.environ if env is None else env
    if (env.get('GITHUB_ACTIONS') != 'true' or env.get('GITHUB_REPOSITORY') != REPOSITORY
            or env.get('GITHUB_REF') != 'refs/heads/' + BRANCH
            or env.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch')
            or not re.fullmatch(r'[0-9]+', env.get('GITHUB_RUN_ID', ''))
            or not re.fullmatch(r'[1-9][0-9]*', env.get('GITHUB_RUN_ATTEMPT', ''))):
        raise ValueError('live AWS operations require a trusted repository push/workflow_dispatch to refs/heads/' + BRANCH)
    identity = '\n'.join((REPOSITORY, env['GITHUB_RUN_ID'], env['GITHUB_RUN_ATTEMPT']))
    return 'th-' + hashlib.sha256(identity.encode()).hexdigest()[:16]


def load_config(path, env=None):
    env = os.environ if env is None else env
    cfg = read_json(path)
    if {'weekly_budget_usd', 'weekly_limit_usd'} & cfg.keys():
        raise ValueError('weekly budget authority belongs only in budget.json, not benchmark config')
    for name in ('bucket', 'bucket_region', 'guest_boundary_arn', 'watchdog_function', 'watchdog_rule'):
        if not cfg.get(name):
            cfg[name] = env.get('AWS_BENCH_' + name.upper())
    if (cfg.get('schema') != 1 or cfg.get('repository') != REPOSITORY or cfg.get('branch') != BRANCH
            or not re.fullmatch(r'[0-9]{12}', cfg.get('account', ''))
            or type(cfg.get('ttl_seconds')) is not int or not 3600 <= cfg['ttl_seconds'] <= 7200
            or cfg.get('export_reserve_seconds') != 600
            or not isinstance(cfg.get('regions'), list) or not cfg['regions']
            or len(set(cfg['regions'])) != len(cfg['regions']) or not set(cfg['regions']) <= set(REGIONS)
            or not isinstance(cfg.get('instance_types'), list) or not cfg['instance_types']
            or not set(cfg['instance_types']) <= set(TYPES)
            or len(set(cfg['instance_types'])) != len(cfg['instance_types'])
            or set(cfg.get('transports', [])) != set(TRANSPORTS)
            or len(cfg['transports']) != len(TRANSPORTS)
            or cfg.get('kv_suite', {}).get('scope') not in ('full', 'hardware-populations')
            or cfg.get('kv_suite', {}).get('diagnostics') is not True):
        raise ValueError('CI config must retain exact branch, approved hardware, fixed TTL and complete TCP/EFA/Direct/Native/Sui/KV populations')
    from checkpoint_matrix import matrix_plan
    from checkpoint_kv import validate_options
    matrix_plan(cfg)
    validate_options(cfg['kv_suite'])
    for name in ('coordinator_cache_mib', 'shard_cache_mib'):
        if type(cfg.get(name)) is not int or not 0 <= cfg[name] <= 128:
            raise ValueError('invalid cache budget')
    if type(cfg.get('replay_deadline_seconds')) is not int or not 30 <= cfg['replay_deadline_seconds'] <= 3600:
        raise ValueError('invalid replay deadline')
    for name in ('bucket', 'bucket_region', 'guest_boundary_arn', 'watchdog_function', 'watchdog_rule'):
        if not cfg.get(name):
            raise ValueError('required setup output absent: ' + name)
    if (not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', cfg['bucket'])
            or cfg['bucket_region'] not in REGIONS):
        raise ValueError('invalid owned results bucket or region')
    if not re.fullmatch(r'arn:aws:iam::' + cfg['account'] + r':policy/(?:[\w+=,.@-]+/)*[\w+=,.@-]+',
                        cfg['guest_boundary_arn']):
        raise ValueError('approved same-account immutable guest permissions boundary ARN required')
    overrides = cfg.get('regional_overrides', {})
    if not isinstance(overrides, dict) or not set(overrides) <= set(cfg['regions']):
        raise ValueError('regional overrides outside approved regions')
    for name in ('retention_days', 'result_gib', 'request_count', 'reconciliation_seconds'):
        if type(cfg.get('cost_assumptions', {}).get(name)) is not int or cfg['cost_assumptions'][name] <= 0:
            raise ValueError('positive reviewed cost_assumptions.' + name + ' required')
    return cfg


def current_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def fresh_archive(destination, entries):
    """Fresh current-source bytes, deterministic across retries of one attempt."""
    def normalized(member):
        member.mtime, member.uid, member.gid = 0, 0, 0
        member.uname = member.gname = ''
        return member
    with Path(destination).open('wb') as output:
        with gzip.GzipFile(filename='', fileobj=output, mode='wb', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w') as archive:
                for path, name in entries:
                    archive.add(path, arcname=name, filter=normalized)


def archive_files(path):
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        names = set()
        if len(members) > 500000 or sum(item.size for item in members) > 32 << 30:
            raise ValueError('input archive exceeds bounded expansion')
        for item in members:
            rel = PurePosixPath(item.name)
            if (rel.is_absolute() or '..' in rel.parts or not rel.parts or str(rel) in names
                    or not (item.isdir() or item.isfile())):
                raise ValueError('unsafe/duplicate input archive member')
            names.add(str(rel))
        return {str(PurePosixPath(item.name)): item for item in members if item.isfile()}


def admit_inputs(build, fixture, verification, output):
    """Reuse maintained build and Sui proof checks, not historical frozen imports."""
    import sui_checkpoint as supervisor
    build, fixture, verification, output = map(Path, (build, fixture, verification, output))
    build = build.resolve(strict=True)
    fixture = fixture.resolve(strict=True)
    verification = verification.resolve(strict=True)
    output.mkdir(parents=True, exist_ok=True)
    manifest = read_json(build / 'build-manifest.json')
    identities = read_json(build / 'source-manifest.json')
    if (manifest.get('protocol') != 9 or manifest.get('fabric') is not True
            or set(manifest.get('binaries', {})) != BINARIES
            or sha(build / 'sources.tar.gz') != manifest['source_archive_sha256']
            or sha(build / 'source-manifest.json') != manifest['source_manifest_sha256']):
        raise ValueError('incomplete source-bound fabric build')
    builder = current_module('ci_maintained_build', HERE.parent / 'sui-checkpoint/build.py')
    builder.verify_staged(build, identities)
    with tempfile.TemporaryDirectory(prefix='ci-source-admission-') as temporary:
        supervisor.safe_extract(build / 'sources.tar.gz', Path(temporary) / 'unpack', symlinks=True)
        builder.verify_staged(Path(temporary) / 'unpack', identities)
    # The currently executing operator and every bootstrap/runtime helper must
    # be part of this build's source identity, not a downloaded frozen importer.
    source_records = identities['tidehunter-sharded-wal']['files']
    for path in [*(HERE.parent.parent / relative for relative in
                   supervisor.runner_helper_origins(source_records).values()),
                 *(HERE.parent / 'sui-checkpoint').glob('*.py')]:
        rel = str(path.relative_to(HERE.parent.parent))
        if path.is_symlink() or not path.is_file() or sha(path) != source_records.get(rel, {}).get('sha256'):
            raise ValueError('current source differs from build: ' + rel)
    commands = manifest.get('commands', [])
    if len(commands) != 6 or {item['label'] for item in commands} != BINARIES:
        raise ValueError('six successful source-bound Cargo invocations required')
    for command in commands:
        name = command['label']
        if (sha(build / 'binaries' / name) != manifest['binaries'][name]
                or command['sha256'] != manifest['binaries'][name]
                or sha(build / f'build-{name}.log') != command['log_sha256']
                or sha(build / f'build-{name}.jsonl') != command['messages_sha256']
                or command['finished_ns'] <= command['started_ns']):
            raise ValueError('build command/artifact receipt differs: ' + name)
    files = archive_files(build / 'build-bundle.tar.gz')
    required = {'build-manifest.json', 'source-manifest.json', 'sources.tar.gz', 'plan.json', 'portability.json',
                *(f'binaries/{name}' for name in BINARIES),
                *(f'build-{name}.{suffix}' for name in BINARIES for suffix in ('log', 'jsonl'))}
    if not required <= files.keys():
        raise ValueError('build archive missing required source/binary/log envelope')
    with tarfile.open(build / 'build-bundle.tar.gz') as archive:
        for name in files:
            local = build / name
            if not local.is_file() or local.is_symlink():
                raise ValueError('build archive contains unbound file: ' + name)
            with archive.extractfile(files[name]) as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != sha(local):
                    raise ValueError('build archive differs from admitted directory: ' + name)
    bindings = dict(build_manifest_sha256=sha(build / 'build-manifest.json'),
                    source_archive_sha256=manifest['source_archive_sha256'],
                    source_manifest_sha256=manifest['source_manifest_sha256'],
                    binaries=manifest['binaries'], checkpoint_digest=CHECKPOINT, checkpoint_limit=1775,
                    transports=list(TRANSPORTS))
    with tempfile.TemporaryDirectory(prefix='ci-admission-') as temporary:
        temporary = Path(temporary)
        archive_files(fixture)
        supervisor.safe_extract(fixture, temporary / 'fixture-input')
        staged = temporary / 'verification'
        staged.mkdir()
        for name in ('receipt.json', 'evidence.tar.gz'):
            shutil.copy2(verification / name, staged / name)
        shutil.copytree(verification / 'experiment', staged / 'experiment')
        checker = supervisor.SuiCampaign.__new__(supervisor.SuiCampaign)
        checker.cfg, checker.fixture, checker.verification, checker.results = bindings, temporary / 'fixture-input/fixture', staged, output
        checker.verify_local()
        verification_bundle = output / 'verification.tar.gz'
        fresh_archive(verification_bundle, [(staged / name, 'verification/' + name)
                      for name in ('receipt.json', 'evidence.tar.gz', 'experiment')])
    runner = output / 'bundle.tar.gz'
    with tempfile.TemporaryDirectory(prefix='ci-runner-assembly-') as temporary:
        staged = supervisor.assemble_runner(HERE.parent.parent, Path(temporary) / 'runner', source_records)
        fresh_archive(runner, [(staged, 'runner')])
    if runner.stat().st_size > 512 << 20:
        raise ValueError('bootstrap bundle exceeds guest bound')
    # Exercise the controller's installed-layout binding on the actual archive
    # before exposing an admitted runner to any paid fleet.
    with tempfile.TemporaryDirectory(prefix='ci-runner-admission-') as temporary:
        installed = Path(temporary) / 'installed'
        supervisor.safe_extract(runner, installed, max_bytes=512 << 20)
        runner_entrypoint = supervisor.validate_runner_entrypoint(installed / 'runner', source_records)
    paths = dict(build_bundle=build / 'build-bundle.tar.gz', fixture_bundle=fixture,
                 verification_bundle=verification_bundle, runner_bundle=runner)
    result = dict(bindings=bindings, inputs={name: dict(path=str(path), bytes=path.stat().st_size, sha256=sha(path))
                                           for name, path in paths.items()},
                  bootstrap_sha256=sha(HERE / 'bootstrap.py'), atomic_start_sha256=sha(HERE / 'atomic_start.py'),
                  runner_entrypoint=runner_entrypoint)
    dump(output / 'input-admission.json', result)
    return result


class AWS:
    """Standard SDK credential chain (including GitHub web identity), never profiles."""
    def __init__(self):
        import boto3
        from botocore.config import Config
        self.session = boto3.Session()
        self.config = Config(connect_timeout=5, read_timeout=20,
                             retries={'mode': 'standard', 'total_max_attempts': 1})
        # Lambda is used only for readiness reads and the read-only health probe.
        # Retry transient Lambda throttling without replaying EC2 mutation requests.
        self.lambda_readiness_config = self.config.merge(Config(
            retries={'mode': 'standard', 'total_max_attempts': 12}))
        # Price List calls are read-only; throttling must not replay EC2 mutations.
        self.pricing_config = self.config.merge(Config(
            retries={'mode': 'standard', 'total_max_attempts': 6}))
        # S3 may lose a response after committing a conditional write. Journal
        # readback confirms identical bytes; EC2 mutations still get one attempt.
        self.s3_config = self.config.merge(Config(
            retries={'mode': 'standard', 'total_max_attempts': 3}))
        self.clients = {}

    def client(self, service, region):
        key = service, region
        if key not in self.clients:
            if service == 'lambda':
                config = self.lambda_readiness_config
            elif service == 'pricing':
                config = self.pricing_config
            elif service == 's3':
                config = self.s3_config
            else:
                config = self.config
            self.clients[key] = self.session.client(service, region_name=region, config=config)
        return self.clients[key]


def pages(request, *, pagination='next', max_pages=1000, **kwargs):
    """Bound real SDK reads, preserving each service's opaque cursor protocol.

    Explicit SDK callbacks keep the actual operations visible to IAM Autopilot,
    which does not analyze boto3 paginator configuration.
    """
    tokens = {
        'next': (('NextToken',), ('NextToken',)),
        'iam': (('Marker',), ('Marker',)),
        's3-objects': (('ContinuationToken',), ('NextContinuationToken',)),
        's3-multipart': (('KeyMarker', 'UploadIdMarker'), ('NextKeyMarker', 'NextUploadIdMarker')),
    }
    if pagination not in tokens or type(max_pages) is not int or not 1 <= max_pages <= 1000:
        raise ValueError('invalid bounded pagination configuration')
    inputs, outputs = tokens[pagination]
    params = dict(kwargs)
    initial = tuple(params.get(name) for name in inputs)
    if any(value is not None and (not isinstance(value, str) or not value) for value in initial):
        raise ValueError('invalid initial pagination cursor')
    if pagination == 's3-multipart' and initial[1] is not None and initial[0] is None:
        raise ValueError('multipart UploadIdMarker requires KeyMarker')
    seen = {initial} if any(value is not None for value in initial) else set()
    for ordinal in range(max_pages):
        page = request(**params)
        if not isinstance(page, dict):
            raise ValueError('pagination response is not an object')
        if pagination == 'next':
            token = page.get(outputs[0])
            following = None if token is None or token == '' else (token,)
        else:
            truncated = page.get('IsTruncated')
            if type(truncated) is not bool:
                raise ValueError('pagination response lacks boolean IsTruncated')
            following = tuple(page.get(name) for name in outputs) if truncated else None
        if following is not None:
            if not isinstance(following[0], str) or not following[0]:
                raise ValueError('truncated pagination response lacks a nonempty continuation cursor')
            if pagination == 's3-multipart':
                upload = following[1]
                if upload is not None and not isinstance(upload, str):
                    raise ValueError('invalid multipart upload continuation marker')
                # A common-prefix boundary can omit its upload marker. Remove
                # any previous UploadIdMarker instead of carrying stale state.
                following = (following[0], upload or None)
            if following in seen:
                raise ValueError('pagination continuation cursor repeated or cycled')
            if ordinal + 1 == max_pages:
                raise RuntimeError('pagination page bound exhausted before complete inventory')
            seen.add(following)
        yield page
        if following is None:
            return
        for name in inputs:
            params.pop(name, None)
        params.update((name, value) for name, value in zip(inputs, following) if value is not None)


def rate(products, unit):
    rates, evidence = set(), []
    for raw in products:
        product = json.loads(raw) if isinstance(raw, str) else raw
        for term in product.get('terms', {}).get('OnDemand', {}).values():
            for dimension in term['priceDimensions'].values():
                if (dimension.get('unit') == unit and dimension.get('beginRange') == '0'
                        and dimension.get('endRange') == 'Inf'):
                    value = Decimal(dimension['pricePerUnit']['USD'])
                    if value > 0:
                        rates.add(value)
                        evidence.append(dict(sku=product['product']['sku'], offer_term=term.get('offerTermCode'),
                                             effective_date=term.get('effectiveDate'), dimension=dimension))
    if len(rates) != 1:
        raise ValueError('live pricing must expose one unambiguous positive On-Demand rate')
    return rates.pop(), evidence


def maximum_rate(products, unit):
    """Ignore free tiers/discount assumptions; reserve at the highest paid tier."""
    choices = []
    for raw in products:
        product = json.loads(raw) if isinstance(raw, str) else raw
        for term in product.get('terms', {}).get('OnDemand', {}).values():
            for dimension in term['priceDimensions'].values():
                if dimension.get('unit') == unit:
                    value = Decimal(dimension['pricePerUnit']['USD'])
                    if value.is_finite() and value > 0:
                        choices.append((value, dict(sku=product['product']['sku'],
                            effective_date=term.get('effectiveDate'), dimension=dimension)))
    if not choices:
        raise ValueError('live ancillary pricing has no positive paid ' + unit + ' dimension')
    value, evidence = max(choices, key=lambda item: item[0])
    return str(value), evidence


def live_ancillary_prices(pricing, cfg):
    def products(service, filters):
        return [item for page in pages(lambda **params: pricing.get_products(**params), ServiceCode=service,
            Filters=[dict(Type='TERM_MATCH', Field=key, Value=value) for key, value in filters.items()],
            FormatVersion='aws_v1') for item in page.get('PriceList', [])]
    region = cfg['bucket_region']
    storage, storage_proof = maximum_rate(products('AmazonS3', dict(regionCode=region,
        productFamily='Storage', volumeType='Standard')), 'GB-Mo')
    requests = products('AmazonS3', dict(regionCode=region, productFamily='API Request'))
    requests = [item for item in requests if json.loads(item)['product']['attributes'].get('group')
                in ('S3-API-Tier1', 'S3-API-Tier2')]
    request, request_proof = maximum_rate(requests, 'Requests')
    transfer, transfer_proof = maximum_rate([item for source in sorted(set([region, *cfg['regions']]))
        for item in products('AWSDataTransfer', dict(fromRegionCode=source, productFamily='Data Transfer'))], 'GB')
    return dict(storage_usd_per_gib_month=storage, request_usd=request, transfer_usd_per_gib=transfer,
        observed_utc=utc(), evidence=dict(storage=storage_proof, request=request_proof, transfer=transfer_proof))


def overhead_estimate(admission, prices, assumptions):
    """Explicit sizing allowances, separately reported from final AWS billing."""
    for name in ('retention_days', 'result_gib', 'request_count', 'reconciliation_seconds'):
        if type(assumptions.get(name)) is not int or assumptions[name] <= 0:
            raise ValueError('positive reviewed cost_assumptions.' + name + ' required')
    with localcontext() as context:
        context.prec, context.rounding = 60, ROUND_CEILING
        input_gib = Decimal(sum(item['bytes'] for item in admission['inputs'].values())) / Decimal(1024 ** 3)
        result_gib = Decimal(assumptions['result_gib'])
        retained_gib = input_gib + result_gib
        # Six guest downloads, one local retrieval, plus result upload/retrieval;
        # use the highest paid transfer tier rather than free-tier assumptions.
        transferred_gib = input_gib * 7 + result_gib * 2
        storage = usd_ceiling(retained_gib * Decimal(prices['storage_usd_per_gib_month']) * Decimal(assumptions['retention_days']) / Decimal(28))
        requests = usd_ceiling(Decimal(assumptions['request_count']) * Decimal(prices['request_usd']))
        transfer = usd_ceiling(transferred_gib * Decimal(prices['transfer_usd_per_gib']))
        total = usd_ceiling(storage + requests + transfer)
    return dict(estimate_usd=str(total), storage_usd=str(storage),
        request_usd=str(requests), transfer_usd=str(transfer), input_gib=str(input_gib),
        result_gib=str(result_gib), retained_gib=str(retained_gib), transferred_gib=str(transferred_gib),
        assumptions=assumptions, prices=prices)


def live_prices(pricing, region, instance_type):
    def products(service, filters):
        return [item for page in pages(lambda **params: pricing.get_products(**params), ServiceCode=service,
                    Filters=[{'Type': 'TERM_MATCH', 'Field': key, 'Value': value} for key, value in filters.items()],
                    FormatVersion='aws_v1') for item in page.get('PriceList', [])]
    compute, compute_proof = rate(products('AmazonEC2', dict(regionCode=region, instanceType=instance_type,
                operatingSystem='Linux', tenancy='Shared', preInstalledSw='NA', capacitystatus='Used',
                productFamily='Compute Instance')), 'Hrs')
    storage, storage_proof = rate(products('AmazonEC2', dict(regionCode=region, volumeApiName='gp3',
                                                            productFamily='Storage')), 'GB-Mo')
    ipv4_products = products('AmazonVPC', dict(regionCode=region, group='VPCPublicIPv4Address'))
    ipv4_products = [item for item in ipv4_products if json.loads(item)['product']['attributes'].get(
                        'usagetype', '').endswith('PublicIPv4:InUseAddress')]
    ipv4, ipv4_proof = rate(ipv4_products, 'Hrs')
    return dict(compute_usd_per_hour=str(compute), ebs_usd_per_gib_month=str(storage),
                ipv4_usd_per_hour=str(ipv4), observed_utc=utc(),
                evidence=dict(compute=compute_proof, ebs=storage_proof, ipv4=ipv4_proof))


def discover_candidates(aws, cfg):
    pricing = aws.client('pricing', 'us-east-1')
    candidates, rejected = [], []
    for region in cfg['regions']:
        ec2 = aws.client('ec2', region)
        override = cfg.get('regional_overrides', {}).get(region, {})
        image_id = override.get('ami')
        image_proof = {'selection': 'reviewed-config'}
        if not image_id:
            parameter = '/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id'
            try:
                response = aws.client('ssm', region).get_parameter(Name=parameter)
            except Exception as error:
                # A missing public image parameter is regional unavailability,
                # not an authentication, endpoint or service-health failure.
                if error_code(error) != 'ParameterNotFound':
                    raise
                rejected.extend(dict(region=region, instance_type=kind,
                    reason='SSM ParameterNotFound: ' + parameter) for kind in cfg['instance_types'])
                continue
            image_id = response['Parameter']['Value']
            image_proof = dict(selection='canonical-public-ssm', parameter=parameter,
                               version=response['Parameter']['Version'])
        try:
            images = ec2.describe_images(ImageIds=[image_id])['Images']
        except Exception as error:
            # NotFound can also mean an Allowed AMIs policy denial: fail closed.
            if error_code(error) != 'InvalidAMIID.Unavailable':
                raise
            rejected.extend(dict(region=region, instance_type=kind,
                reason='EC2 InvalidAMIID.Unavailable: ' + image_id) for kind in cfg['instance_types'])
            continue
        if len(images) != 1:
            raise ValueError('exact AMI lookup failed')
        image = images[0]
        if (image.get('OwnerId') not in ('099720109477', cfg['account']) or image.get('State') != 'available'
                or image.get('Architecture') != 'x86_64' or image.get('VirtualizationType') != 'hvm'
                or image.get('RootDeviceType') != 'ebs' or not image.get('EnaSupport')):
            raise ValueError('AMI lacks approved owner or x86_64/HVM/EBS/ENA capability')
        image_proof.update(image_id=image_id, owner=image['OwnerId'], name=image.get('Name'), observed_utc=utc())
        subnets = [item for page in pages(lambda **params: ec2.describe_subnets(**params), Filters=[
            {'Name': 'default-for-az', 'Values': ['true']}, {'Name': 'state', 'Values': ['available']}])
                   for item in page['Subnets']]
        if override.get('subnets'):
            subnets = [item for item in subnets if item['SubnetId'] in override['subnets']]
        usable = []
        for subnet in subnets:
            if not subnet.get('MapPublicIpOnLaunch') or subnet.get('AvailableIpAddressCount', 0) < 6:
                continue
            routes = ec2.describe_route_tables(Filters=[{'Name': 'association.subnet-id', 'Values': [subnet['SubnetId']]}])['RouteTables']
            if not routes:
                routes = ec2.describe_route_tables(Filters=[{'Name': 'vpc-id', 'Values': [subnet['VpcId']]},
                                                           {'Name': 'association.main', 'Values': ['true']}])['RouteTables']
            if len(routes) == 1 and any(route.get('DestinationCidrBlock') == '0.0.0.0/0'
                    and route.get('GatewayId', '').startswith('igw-') and route.get('State') == 'active'
                    for route in routes[0]['Routes']):
                usable.append(subnet)
        if not usable:
            rejected.extend(dict(region=region, instance_type=kind,
                reason='no usable public subnet with six free addresses') for kind in cfg['instance_types'])
            continue
        # A bulk explicit-type request can fail entirely when just one approved
        # type is absent in this region. Bound discovery to one request per type.
        for kind in cfg['instance_types']:
            try:
                types = ec2.describe_instance_types(InstanceTypes=[kind])['InstanceTypes']
            except Exception as error:
                code = error_code(error)
                message = (getattr(error, 'response', None) or {}).get('Error', {}).get('Message')
                # InvalidParameterValue is otherwise too broad to treat as
                # unavailability; recognize only EC2's exact singleton response.
                if code != 'InvalidInstanceType' and not (
                        code == 'InvalidParameterValue'
                        and message == f'The following supplied instance types do not exist: [{kind}]'):
                    raise
                rejected.append(dict(region=region, instance_type=kind,
                    reason='describe_instance_types: ' + code))
                continue
            if (not isinstance(types, list) or len(types) != 1
                    or not isinstance(types[0], dict) or types[0].get('InstanceType') != kind):
                raise ValueError('exact regional instance type lookup failed')
            hardware = types[0]
            disks = hardware.get('InstanceStorageInfo', {}).get('Disks', [])
            if ('x86_64' not in hardware.get('ProcessorInfo', {}).get('SupportedArchitectures', [])
                    or not hardware.get('NetworkInfo', {}).get('EfaSupported')
                    or not hardware.get('InstanceStorageSupported') or len(disks) != 1
                    or disks[0].get('Count') != INSTANCE_STORE_DISKS[kind] or disks[0].get('Type') != 'ssd'
                    or 'cluster' not in hardware.get('PlacementGroupInfo', {}).get('SupportedStrategies', [])):
                rejected.append(dict(region=region, instance_type=kind, reason='requires EFA, cluster and the exact local SSD inventory; bootstrap uses exactly one'))
                continue
            price = live_prices(pricing, region, kind)
            offered = {item['Location'] for page in pages(lambda **params: ec2.describe_instance_type_offerings(**params),
                         LocationType='availability-zone', Filters=[{'Name': 'instance-type', 'Values': [kind]}])
                       for item in page['InstanceTypeOfferings']}
            root_sizes = [item['Ebs']['VolumeSize'] for item in image.get('BlockDeviceMappings', [])
                          if item.get('DeviceName') == image['RootDeviceName']
                          and 'VolumeSize' in item.get('Ebs', {})]
            root_gib = max([32, *root_sizes])
            with localcontext() as context:
                context.prec, context.rounding = 60, ROUND_CEILING
                host_rate = usd_ceiling(Decimal(price['compute_usd_per_hour']) + Decimal(price['ipv4_usd_per_hour'])
                    + Decimal(price['ebs_usd_per_gib_month']) * root_gib / Decimal(730))
                fleet_estimate = usd_ceiling(host_rate * 6 * Decimal(cfg['ttl_seconds']) / Decimal(3600))
            if not any(subnet['AvailabilityZone'] in offered for subnet in usable):
                rejected.append(dict(region=region, instance_type=kind,
                    reason='no instance type offering in a usable subnet availability zone'))
            for subnet in usable:
                if subnet['AvailabilityZone'] in offered:
                    candidates.append(dict(region=region, az=subnet['AvailabilityZone'], subnet=subnet['SubnetId'],
                        vpc=subnet['VpcId'], ami=image_id, ami_proof=image_proof, root_device=image['RootDeviceName'],
                        root_gib=root_gib, instance_type=kind, price=price, host_hourly_usd=str(host_rate),
                        maximum_estimate_usd=str(fleet_estimate), capability=hardware))
    candidates.sort(key=lambda item: (Decimal(item['host_hourly_usd']), cfg['instance_types'].index(item['instance_type']),
                                       cfg['regions'].index(item['region']), item['az'], item['subnet']))
    if not candidates:
        raise ValueError('no live compatible six-host cluster candidate: ' + repr(rejected))
    return dict(candidates=candidates, rejected=rejected, observed_utc=utc(),
                ancillary_prices=live_ancillary_prices(pricing, cfg))


def planned_profiles(cfg, run_id):
    return {role: dict(name=f'tidehunter-ci-{run_id}-{role}', role_name=f'tidehunter-ci-{run_id}-{role}',
        arn=f"arn:aws:iam::{cfg['account']}:instance-profile/tidehunter-ci-{run_id}-{role}") for role in ROLES}


def guest_policy(cfg, run_id, role, *, region, expires_at, instances=None):
    """Narrow the existing reviewed Autopilot-derived guest policy templates."""
    import control
    if role not in ROLES:
        raise ValueError('invalid final guest role')
    tags = ownership(run_id) | {'ExpiresAt': expires_at}
    p = dict(bucket=cfg['bucket'], prefix=run_id, run_id=run_id, account=cfg['account'],
             region=region, tags=tags, atomic_controller=True, protected_instances=[])
    base = control.role_policy(p, 'controller' if role == 'controller' else 'node')
    read_action = [action for action in base['Statement'][1]['Action'] if action == 's3:GetObject']
    write_actions = [action for action in base['Statement'][1]['Action'] if action != 's3:GetObject']
    read = [run_id + '/input/bundle.tar.gz']
    write = []
    if instances is not None:
        if len(instances) != 6 or {item['AmiLaunchIndex'] for item in instances} != set(range(6)):
            raise ValueError('exact six launch-index identities required for final guest policy')
        if role == 'controller':
            read += [run_id + suffix for suffix in ('/inputs/*', '/results/*', '/deploy/*', '/case-artifacts/*')]
            write += [run_id + '/results/' + name for name in (
                'startup.json', 'atomic-start.log', 'controller-autostart.log', 'progress.json',
                'controller.tar.gz', 'receipt.json', 'export-permission-probe.json')]
            write += [run_id + suffix for suffix in (
                '/deploy/cells/*', '/case-artifacts/*', '/inputs/kv-storage-node')]
        else:
            from checkpoint_matrix import matrix_plan
            partition = int(role.rsplit('-', 1)[1])
            cells = [item['cell_id'] for item in matrix_plan(cfg)]
            read += [f'{run_id}/deploy/cells/{cell}/storage-{partition}.tar.gz' for cell in cells]
            read += [run_id + suffix for suffix in (
                f'/case-artifacts/*/node-{partition}.tar.gz', '/inputs/kv-storage-node')]
            write += [f'{run_id}/results/cells/{cell}/storage/{partition}/*' for cell in [*cells, 'startup']]
            write.append(f'{run_id}/results/nodes/*/node-{partition}.tar.gz*')
        read += write
    prefixes = sorted(set(read + write))
    listing = dict(base['Statement'][0])
    listing['Condition'] = {'StringLike': {'s3:prefix': prefixes}}
    statements = [listing, dict(Effect='Allow', Action=read_action,
        Resource=[f"arn:aws:s3:::{cfg['bucket']}/{key}" for key in sorted(set(read))])]
    if write:
        statements.append(dict(Effect='Allow', Action=write_actions,
            Resource=[f"arn:aws:s3:::{cfg['bucket']}/{key}" for key in sorted(set(write))]))
    if instances is not None and role == 'controller':
        statements.extend(base['Statement'][2:])
        for statement in statements:
            if statement.get('Action') == 'ec2:TerminateInstances':
                statement['Resource'] = [f"arn:aws:ec2:{region}:{cfg['account']}:instance/{item['InstanceId']}"
                                         for item in instances]
        storage_ids = [item['InstanceId'] for item in instances if item['AmiLaunchIndex'] < 5]
        statements.extend(control.command_policy(p, storage_ids)['Statement'])
    return dict(Version=base['Version'], Statement=statements)


def live_preflight(aws, cfg):
    identity = aws.client('sts', cfg['bucket_region']).get_caller_identity()
    if identity.get('Account') != cfg['account']:
        raise ValueError('standard-chain caller account differs from trusted setup')
    s3 = aws.client('s3', cfg['bucket_region'])
    s3.head_bucket(Bucket=cfg['bucket'], ExpectedBucketOwner=cfg['account'])
    iam = aws.client('iam', cfg['bucket_region'])
    boundary = iam.get_policy(PolicyArn=cfg['guest_boundary_arn'])['Policy']
    if boundary.get('Arn') != cfg['guest_boundary_arn'] or boundary.get('IsAttachable') is not True:
        raise ValueError('approved immutable guest boundary is not attachable')
    boundary_version = iam.get_policy_version(PolicyArn=cfg['guest_boundary_arn'],
        VersionId=boundary['DefaultVersionId'])['PolicyVersion']
    from ci_watchdog import validate_readiness
    health = validate_readiness(aws.client('lambda', cfg['bucket_region']),
        aws.client('events', cfg['bucket_region']), function_name=cfg['watchdog_function'],
        rule_name=cfg['watchdog_rule'], account=cfg['account'], regions=cfg['regions'],
        repository=REPOSITORY, branch=BRANCH, guest_boundary_arn=cfg['guest_boundary_arn'])
    from ci_budget import BudgetLedger
    budget = BudgetLedger(s3, cfg['bucket'], cfg['account'], repository=REPOSITORY, branch=BRANCH,
                          budget_path=cfg.get('budget_path', DEFAULT_BUDGET)).report()
    return dict(identity=identity, boundary=dict(arn=boundary['Arn'], version=boundary['DefaultVersionId'],
                sha256=hashlib.sha256(encoded(boundary_version['Document'])).hexdigest()),
                watchdog=health, budget=budget, budget_policy=budget['budget_policy'],
                **config_audit(cfg, {'budget_policy': budget['budget_policy']}), **discover_candidates(aws, cfg))


class Journal:
    def __init__(self, s3, cfg, run_id, output, *, load=True):
        self.s3, self.cfg, self.run_id, self.output = s3, cfg, run_id, Path(output)
        self.key = 'ci/control/' + run_id + '/journal.json'
        self.etag = None
        self.data = None
        if not load:
            return
        try:
            response, raw = self._read(8 * 1024 * 1024 + 1)
        except Exception as error:
            if error_code(error) not in MISSING:
                raise
        else:
            if len(raw) > 8 * 1024 * 1024:
                raise ValueError('oversized control journal')
            self.etag, self.data = response['ETag'], json.loads(raw)
            if (self.data.get('run_id') != run_id or self.data.get('ownership') != ownership(run_id)
                    or self.data.get('account') != cfg['account']):
                raise ValueError('foreign control journal account or ownership')

    def _read(self, limit):
        from botocore.exceptions import HTTPClientError, IncompleteReadError
        for attempt in range(3):
            response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=self.key,
                ExpectedBucketOwner=self.cfg['account'])
            stream = response['Body']
            try:
                with stream:
                    raw = stream.read(limit)
            except (HTTPClientError, IncompleteReadError):
                # SDK retries stop once GetObject returns its streaming body.
                if attempt == 2:
                    raise
            else:
                return response, raw

    def save(self):
        self.data['updated_utc'] = utc()
        raw = encoded(self.data)
        condition = {'IfMatch': self.etag} if self.etag else {'IfNoneMatch': '*'}
        try:
            response = self.s3.put_object(Bucket=self.cfg['bucket'], Key=self.key, Body=raw,
                ExpectedBucketOwner=self.cfg['account'], ServerSideEncryption='AES256',
                ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode(), **condition)
        except Exception as error:
            if error_code(error) not in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409'):
                from botocore.exceptions import ConnectionError, HTTPClientError
                status = (getattr(error, 'response', None) or {}).get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
                if not isinstance(error, (ConnectionError, HTTPClientError)) and not 500 <= status < 600:
                    raise
            try:
                response, observed = self._read(len(raw) + 1)
            except Exception as read_error:
                raise error from read_error
            if observed != raw:
                raise
        self.etag = response['ETag']
        dump(self.output / 'resource-journal.json', self.data)

    def intent(self, operation, region, params):
        item = dict(operation=operation, region=region, params=params, status='outcome-unknown', at=utc())
        self.data['mutations'].append(item)
        self.save()
        return item


class Campaign:
    guest_policy = staticmethod(guest_policy)

    def __init__(self, aws, cfg, run_id, output, *, clock=time, cleanup_only=False):
        self.aws, self.cfg, self.run_id, self.output, self.clock = aws, cfg, run_id, Path(output), clock
        self.output.mkdir(parents=True, exist_ok=True)
        self.s3 = aws.client('s3', cfg['bucket_region'])
        self.journal_error = None
        try:
            self.journal = Journal(self.s3, cfg, run_id, self.output)
        except Exception as error:
            if not cleanup_only:
                raise
            # S3 failure must not prevent exact-tag/token-owned EC2 shutdown.
            # It still makes the cleanup receipt incomplete and nonzero.
            self.journal_error = repr(error)
            self.journal = Journal(self.s3, cfg, run_id, self.output, load=False)
        if cleanup_only and self.journal.data:
            regions = set(cfg['regions']) | set(self.journal.data.get('configured_regions', []))
            regions.update(item['region'] for item in self.journal.data.get('candidates', []))
            if not regions <= set(REGIONS):
                raise ValueError('recovery journal names an unapproved region')
            self.cfg = dict(cfg, regions=sorted(regions))

    def filters(self):
        return [{'Name': 'tag:' + key, 'Values': [value]} for key, value in ownership(self.run_id).items()]

    def exact(self, resource):
        tags = tag_map(resource)
        if any(tags.get(key) != value for key, value in ownership(self.run_id).items()):
            raise ValueError('foreign ownership resource cannot be adopted or terminated')
        parsed(tags['ExpiresAt'])
        if self.journal.data and self.journal.data.get('expires_at') and tags['ExpiresAt'] != self.journal.data['expires_at']:
            raise ValueError('resource expiration differs from immutable journal')
        return resource

    def approved_tokens(self):
        allowed = {}
        for attempt in (self.journal.data or {}).get('attempts', []):
            candidate = attempt['candidate']
            token = candidate_token(self.run_id, candidate)
            if attempt.get('client_token') != token:
                raise ValueError('journalled candidate token fingerprint differs')
            allowed[token] = candidate
        return allowed

    def check_token(self, region, token):
        candidate = self.approved_tokens().get(token)
        if candidate is None or candidate['region'] != region:
            raise ValueError('instance atomic token is not an approved journalled regional candidate')

    def inventory(self, regions=None, *, errors=None):
        found = []
        for region in self.cfg['regions'] if regions is None else regions:
            ec2 = self.aws.client('ec2', region)
            for page in pages(lambda **params: ec2.describe_instances(**params), Filters=self.filters()):
                for reservation in page['Reservations']:
                    if reservation.get('OwnerId') != self.cfg['account']:
                        if errors is None:
                            raise ValueError('foreign EC2 reservation owner')
                        errors.append(region + ': foreign EC2 reservation owner')
                        continue
                    for item in reservation['Instances']:
                        try:
                            self.exact(item)
                            self.check_token(region, item.get('ClientToken'))
                        except Exception as error:
                            if errors is None:
                                raise
                            errors.append(region + '/' + item.get('InstanceId', 'unknown') + ': ' + repr(error))
                            continue
                        found.append(dict(region=region, instance=item))
        return found

    def active(self):
        return [item for item in self.inventory() if item['instance']['State']['Name'] != 'terminated']

    def mutate(self, operation, region, params):
        intent = self.journal.intent(operation, region, params)
        # Explicit operation dispatch keeps the mutation surface inspectable by
        # IAM Autopilot; there is no arbitrary adapter operation or CLI fallback.
        ec2 = self.aws.client('ec2', region)
        if operation == 'create_placement_group':
            response = ec2.create_placement_group(**params)
        elif operation == 'create_security_group':
            response = ec2.create_security_group(**params)
        elif operation == 'authorize_security_group_ingress':
            response = ec2.authorize_security_group_ingress(**params)
        elif operation == 'authorize_security_group_egress':
            response = ec2.authorize_security_group_egress(**params)
        elif operation == 'revoke_security_group_egress':
            response = ec2.revoke_security_group_egress(**params)
        elif operation == 'create_tags':
            response = ec2.create_tags(**params)
        elif operation == 'replace_iam_instance_profile_association':
            response = ec2.replace_iam_instance_profile_association(**params)
        elif operation == 'associate_iam_instance_profile':
            response = ec2.associate_iam_instance_profile(**params)
        else:
            raise ValueError('unsupported campaign mutation')
        intent['status'] = 'acknowledged'
        intent['result'] = response
        self.journal.save()
        return response

    @staticmethod
    def iam_read(reader):
        try:
            return reader()
        except Exception as error:
            if error_code(error) == 'NoSuchEntity':
                return None
            raise

    def iam_mutate(self, operation, params, *, cleanup=False):
        intent = None
        if self.journal.data and not self.journal_error:
            try:
                intent = self.journal.intent(operation, self.cfg['bucket_region'], params)
            except Exception as error:
                if not cleanup:
                    raise
                self.journal_error = repr(error)
        elif not cleanup:
            raise ValueError('durable journal required before IAM provisioning')
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        if operation == 'create_role':
            result = iam.create_role(**params)
        elif operation == 'create_instance_profile':
            result = iam.create_instance_profile(**params)
        elif operation == 'attach_role_policy':
            result = iam.attach_role_policy(**params)
        elif operation == 'put_role_policy':
            result = iam.put_role_policy(**params)
        elif operation == 'add_role_to_instance_profile':
            result = iam.add_role_to_instance_profile(**params)
        elif operation == 'remove_role_from_instance_profile':
            result = iam.remove_role_from_instance_profile(**params)
        elif operation == 'delete_instance_profile':
            result = iam.delete_instance_profile(**params)
        elif operation == 'delete_role_policy':
            result = iam.delete_role_policy(**params)
        elif operation == 'detach_role_policy':
            result = iam.detach_role_policy(**params)
        elif operation == 'delete_role':
            result = iam.delete_role(**params)
        else:
            raise ValueError('unsupported bounded guest IAM mutation')
        if intent is not None:
            intent.update(status='acknowledged', result=result)
            try:
                self.journal.save()
            except Exception as error:
                if not cleanup:
                    raise
                self.journal_error = repr(error)
        return result

    def checked_role(self, role, name):
        self.exact(role)
        expected = f"arn:aws:iam::{self.cfg['account']}:role/{name}"
        if (role.get('RoleName') != name or role.get('Arn') != expected
                or role.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn') != self.cfg['guest_boundary_arn']):
            raise ValueError('per-run guest role name/account/immutable permissions boundary differs')
        trust = role.get('AssumeRolePolicyDocument')
        if isinstance(trust, str):
            trust = json.loads(trust)
        if trust != dict(Version='2012-10-17', Statement=[dict(Effect='Allow',
                Principal={'Service': 'ec2.amazonaws.com'}, Action='sts:AssumeRole')]):
            raise ValueError('guest role EC2-only trust differs')
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        inline = [policy for page in pages(lambda **params: iam.list_role_policies(**params),
                  pagination='iam', RoleName=name) for policy in page['PolicyNames']]
        attached = [item['PolicyArn'] for page in pages(lambda **params: iam.list_attached_role_policies(**params),
                    pagination='iam', RoleName=name) for item in page['AttachedPolicies']]
        import control
        if set(inline) - {'run-scope'} or set(attached) - {control.SSM_CORE}:
            raise ValueError('per-run guest role has unapproved additional policies')
        return inline, attached

    def checked_profile(self, profile, identity, *, require_member=False):
        self.exact(profile)
        roles = profile.get('Roles', [])
        if (profile.get('InstanceProfileName') != identity['name'] or profile.get('Arn') != identity['arn']
                or len(roles) > 1 or (require_member and len(roles) != 1)
                or any(item.get('RoleName') != identity['role_name']
                       or item.get('Arn') != f"arn:aws:iam::{self.cfg['account']}:role/{identity['role_name']}"
                       for item in roles)):
            raise ValueError('per-run instance profile has foreign identity or membership')
        return profile

    def wait_iam(self, reader, predicate, reason):
        for _ in range(8):
            value = reader()
            if value is not None and predicate(value):
                return value
            self.clock.sleep(2)
        raise TimeoutError(reason)

    def install_guest_policy(self, role, identity, *, region, instances=None):
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        policy = self.guest_policy(self.cfg, self.run_id, role, region=region,
            expires_at=self.journal.data['expires_at'], instances=instances)
        name = identity['role_name']
        read = lambda: self.iam_read(lambda: iam.get_role_policy(RoleName=name, PolicyName='run-scope'))
        existing = read()
        if existing is None or existing.get('PolicyDocument') != policy:
            try:
                self.iam_mutate('put_role_policy', dict(RoleName=name, PolicyName='run-scope',
                    PolicyDocument=encoded(policy).decode()))
            except Exception:
                self.wait_iam(read, lambda value: value.get('PolicyDocument') == policy,
                              'unknown guest policy update did not reconcile')
            else:
                self.wait_iam(read, lambda value: value.get('PolicyDocument') == policy,
                              'exact guest policy readback did not converge')
        self.journal.data.setdefault('guest_policy_sha256', {})[role] = hashlib.sha256(encoded(policy)).hexdigest()
        self.journal.save()

    def provision_profiles(self):
        import control
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        profiles = self.journal.data['instance_profiles']
        if profiles != planned_profiles(self.cfg, self.run_id):
            raise ValueError('per-run profile plan changed')
        trust = dict(Version='2012-10-17', Statement=[dict(Effect='Allow',
            Principal={'Service': 'ec2.amazonaws.com'}, Action='sts:AssumeRole')])
        for role in sorted(ROLES):
            identity = profiles[role]
            name = identity['name']
            tags = ownership(self.run_id) | {'ExpiresAt': self.journal.data['expires_at'], 'Name': name, 'Role': role}
            read_role = lambda: self.iam_read(lambda: iam.get_role(RoleName=name))
            found = read_role()
            if found is None:
                try:
                    self.iam_mutate('create_role', dict(RoleName=name, AssumeRolePolicyDocument=encoded(trust).decode(),
                        PermissionsBoundary=self.cfg['guest_boundary_arn'], Tags=aws_tags(tags)))
                except Exception:
                    found = self.wait_iam(read_role, lambda value: True, 'unknown role creation did not reconcile')
                else:
                    found = self.wait_iam(read_role, lambda value: True, 'new guest role is not visible')
            _, attached = self.checked_role(found['Role'], name)
            if control.SSM_CORE not in attached:
                try:
                    self.iam_mutate('attach_role_policy', dict(RoleName=name, PolicyArn=control.SSM_CORE))
                except Exception:
                    self.wait_iam(lambda: self.checked_role(read_role()['Role'], name)[1],
                                  lambda policies: control.SSM_CORE in policies, 'unknown SSM policy attachment')
            self.wait_iam(lambda: self.checked_role(read_role()['Role'], name)[1],
                          lambda policies: control.SSM_CORE in policies, 'SSM policy attachment did not converge')
            self.install_guest_policy(role, identity, region=self.cfg['bucket_region'])
            read_profile = lambda: self.iam_read(lambda: iam.get_instance_profile(InstanceProfileName=name))
            profile = read_profile()
            if profile is None:
                try:
                    self.iam_mutate('create_instance_profile', dict(InstanceProfileName=name, Tags=aws_tags(tags)))
                except Exception:
                    profile = self.wait_iam(read_profile, lambda value: True, 'unknown profile creation did not reconcile')
                else:
                    profile = self.wait_iam(read_profile, lambda value: True, 'new guest profile is not visible')
            self.checked_profile(profile['InstanceProfile'], identity)
            if not profile['InstanceProfile'].get('Roles'):
                try:
                    self.iam_mutate('add_role_to_instance_profile', dict(InstanceProfileName=name, RoleName=name))
                except Exception:
                    pass
            self.wait_iam(read_profile, lambda value: bool(self.checked_profile(
                value['InstanceProfile'], identity)['Roles']), 'exact guest profile membership did not converge')
        self.clock.sleep(10)
        return profiles

    def finalize_guest_policies(self, instances, candidate, profiles):
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        for role in sorted(ROLES):
            identity = profiles[role]
            self.checked_role(iam.get_role(RoleName=identity['role_name'])['Role'], identity['role_name'])
            self.checked_profile(iam.get_instance_profile(InstanceProfileName=identity['name'])['InstanceProfile'],
                                 identity, require_member=True)
            self.install_guest_policy(role, identity, region=candidate['region'], instances=instances)
        self.clock.sleep(10)

    def observe_s3(self, operation=None, *, read_bytes=0, failed_download=False):
        """Bounded, partial observations; managed-transfer and guest counts are unknown."""
        if not self.journal.data:
            return
        counters = self.journal.data.setdefault('s3_observations', dict(
            successful_local_read_bytes=0, successful_requests={}, failed_managed_downloads=0))
        if operation is not None:
            if operation not in ('ListObjectsV2', 'HeadObject', 'GetObject'):
                raise ValueError('unreviewed S3 request counter')
            requests = counters['successful_requests']
            requests[operation] = requests.get(operation, 0) + 1
        counters['successful_local_read_bytes'] += read_bytes
        counters['failed_managed_downloads'] += int(failed_download)

    def upload(self, path, key):
        from boto3.s3.transfer import TransferConfig
        path = Path(path)
        digest, size = sha(path), path.stat().st_size
        if not key.startswith(self.run_id + '/'):
            raise ValueError('input upload outside exact run prefix')
        try:
            head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
            self.observe_s3('HeadObject')
        except Exception as error:
            if error_code(error) not in MISSING:
                raise
            self.s3.upload_file(str(path), self.cfg['bucket'], key, ExtraArgs={
                'ExpectedBucketOwner': self.cfg['account'], 'ServerSideEncryption': 'AES256',
                'Metadata': {'sha256': digest}, 'ChecksumAlgorithm': 'SHA256'},
                Config=TransferConfig(multipart_threshold=16 << 20, multipart_chunksize=16 << 20,
                                      max_concurrency=4, use_threads=True))
            head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
            self.observe_s3('HeadObject')
        if head['ContentLength'] != size or head.get('Metadata', {}).get('sha256') != digest:
            raise ValueError('remote immutable object length/digest differs: ' + key)
        # Multipart SHA256 is composite, not the SHA256 of the complete file.
        # Read back the entire object to bind the full digest before allocation.
        response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
        self.observe_s3('GetObject')
        remote_hash, read_bytes = hashlib.sha256(), 0
        with response['Body'] as body:
            while chunk := body.read(1024 * 1024):
                read_bytes += len(chunk)
                remote_hash.update(chunk)
        self.observe_s3(read_bytes=read_bytes)
        self.journal.save()
        if read_bytes != size or remote_hash.hexdigest() != digest:
            raise ValueError('remote full-object digest differs: ' + key)
        return dict(key=key, sha256=digest, bytes=size, bucket=self.cfg['bucket'],
                    **config_audit(self.cfg, self.journal.data))

    def network(self, candidate):
        region = candidate['region']
        ec2 = self.aws.client('ec2', region)
        tags = ownership(self.run_id) | {'ExpiresAt': self.journal.data['expires_at'], 'Name': self.run_id}
        groups = ec2.describe_placement_groups(Filters=[{'Name': 'group-name', 'Values': [self.run_id]}])['PlacementGroups']
        if not groups:
            self.mutate('create_placement_group', region, dict(GroupName=self.run_id, Strategy='cluster',
                        TagSpecifications=[dict(ResourceType='placement-group', Tags=aws_tags(tags))]))
            groups = ec2.describe_placement_groups(GroupNames=[self.run_id])['PlacementGroups']
        if len(groups) != 1 or groups[0].get('Strategy') != 'cluster' or groups[0].get('State') != 'available':
            raise ValueError('mandatory available cluster placement group missing')
        self.exact(groups[0])
        found = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [self.run_id]},
                                                       {'Name': 'vpc-id', 'Values': [candidate['vpc']]}])['SecurityGroups']
        if not found:
            self.mutate('create_security_group', region, dict(GroupName=self.run_id,
                Description='TideHunter CI atomic fleet', VpcId=candidate['vpc'],
                TagSpecifications=[dict(ResourceType='security-group', Tags=aws_tags(tags))]))
            found = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [self.run_id]},
                                                           {'Name': 'vpc-id', 'Values': [candidate['vpc']]}])['SecurityGroups']
        if len(found) != 1:
            raise ValueError('ambiguous run security group')
        group = self.exact(found[0])
        group_id = group['GroupId']
        inside = dict(IpProtocol='-1', UserIdGroupPairs=[dict(GroupId=group_id)])
        outside = [dict(IpProtocol='tcp', FromPort=port, ToPort=port,
                        IpRanges=[dict(CidrIp='0.0.0.0/0')]) for port in (80, 443)]
        def rules(permissions):
            normalized = []
            for permission in permissions:
                if permission.get('Ipv6Ranges') or permission.get('PrefixListIds'):
                    raise ValueError('unexpected IPv6/prefix-list group permission')
                peers = permission.get('UserIdGroupPairs', [])
                if any(peer.get('UserId', self.cfg['account']) != self.cfg['account'] for peer in peers):
                    raise ValueError('foreign security group peer')
                normalized.append((permission['IpProtocol'], permission.get('FromPort'),
                    permission.get('ToPort'), tuple(sorted(peer['GroupId'] for peer in peers)),
                    tuple(sorted(peer['CidrIp'] for peer in permission.get('IpRanges', [])))))
            return sorted(normalized, key=repr)
        if (rules(group.get('IpPermissions', [])) == rules([inside])
                and rules(group.get('IpPermissionsEgress', [])) == rules([inside, *outside])):
            return group_id
        interfaces = ec2.describe_network_interfaces(Filters=[{'Name': 'group-id', 'Values': [group_id]}])['NetworkInterfaces']
        if interfaces or group.get('IpPermissions'):
            raise ValueError('refusing attached or partially configured security group edits')
        for permission in group.get('IpPermissionsEgress', []):
            self.mutate('revoke_security_group_egress', region, dict(GroupId=group_id, IpPermissions=[permission]))
        self.mutate('authorize_security_group_ingress', region, dict(GroupId=group_id, IpPermissions=[inside]))
        self.mutate('authorize_security_group_egress', region, dict(GroupId=group_id, IpPermissions=[inside, *outside]))
        observed = ec2.describe_security_groups(GroupIds=[group_id])['SecurityGroups']
        if (len(observed) != 1 or rules(observed[0].get('IpPermissions', [])) != rules([inside])
                or rules(observed[0].get('IpPermissionsEgress', [])) != rules([inside, *outside])):
            raise ValueError('EFA self-only ingress and self/80/443 egress did not verify')
        self.exact(observed[0])
        return group_id

    def request(self, candidate, group_id, admission, profiles):
        import control
        bindings = dict(admission['bindings'])
        bindings.update({name: self.journal.data['uploads'][name] for name in
                         ('build_bundle', 'fixture_bundle', 'verification_bundle')})
        bindings.update({name: self.cfg[name] for name in ('transports', 'repetitions', 'read_profile_limits',
                         'write_profile_limits', 'native_write_profile_limits', 'kv_suite')})
        bindings.update({name: self.cfg[name] for name in
            ('coordinator_cache_mib_by_repetition', 'shard_cache_mib_by_repetition') if name in self.cfg})
        runner = admission['inputs']['runner_bundle']
        plan = dict(run_id=self.run_id, account=self.cfg['account'], region=candidate['region'],
            bucket=self.cfg['bucket'], prefix=self.run_id, ttl_seconds=self.cfg['ttl_seconds'], export_reserve_seconds=600,
            instance_type=candidate['instance_type'], placement_group=self.run_id, az=candidate['az'],
            protected_instances=[], bundle_sha256=runner['sha256'], bundle_bytes=runner['bytes'],
            atomic_bindings=bindings, atomic_controller=True, atomic_client_token=candidate_token(self.run_id, candidate),
            instance_profiles=profiles, ownership_tags={key: value for key, value in
                (ownership(self.run_id) | {'ExpiresAt': self.journal.data['expires_at']}).items()
                if key in ('Repository', 'BenchmarkBranch', 'ExpiresAt')}, efa=True)
        userdata = control.user_data(plan)
        tags = ownership(self.run_id) | {'ExpiresAt': self.journal.data['expires_at'],
                                       'Role': 'bootstrap', 'Name': self.run_id}
        request = dict(ImageId=candidate['ami'], InstanceType=candidate['instance_type'], MinCount=6, MaxCount=6,
            ClientToken=candidate_token(self.run_id, candidate),
            NetworkInterfaces=[dict(DeviceIndex=0, NetworkCardIndex=0, InterfaceType='efa',
                SubnetId=candidate['subnet'], Groups=[group_id], DeleteOnTermination=True, AssociatePublicIpAddress=True)],
            Placement=dict(AvailabilityZone=candidate['az'], GroupName=self.run_id),
            BlockDeviceMappings=[dict(DeviceName=candidate['root_device'], Ebs=dict(VolumeSize=candidate['root_gib'],
                VolumeType='gp3', Encrypted=True, DeleteOnTermination=True))],
            MetadataOptions=dict(HttpEndpoint='enabled', HttpTokens='required', HttpPutResponseHopLimit=1,
                                 InstanceMetadataTags='enabled'), InstanceInitiatedShutdownBehavior='terminate',
            TagSpecifications=[dict(ResourceType=kind, Tags=aws_tags(tags)) for kind in ('instance', 'volume', 'network-interface')],
            UserData=userdata)
        return request, plan

    def validate_fleet(self, records, candidate):
        if len(records) != 6 or len({item['instance']['InstanceId'] for item in records}) != 6:
            raise ValueError('atomic allocation did not yield exactly six independent hosts')
        for record in records:
            item = self.exact(record['instance'])
            if (record['region'] != candidate['region'] or item['InstanceType'] != candidate['instance_type']
                    or item['Placement'].get('GroupName') != self.run_id
                    or item['Placement'].get('AvailabilityZone') != candidate['az']
                    or item['ClientToken'] != candidate_token(self.run_id, candidate)):
                raise ValueError('fleet differs from immutable atomic candidate')
        if {item['instance'].get('AmiLaunchIndex') for item in records} != set(range(6)):
            raise ValueError('atomic launch index inventory differs')
        return [item['instance'] for item in records]

    def allocate(self, admission, preflight):
        data = self.journal.data
        if data is not None:
            if (data['config_sha256'] != hashlib.sha256(encoded(self.cfg)).hexdigest()
                    or data['input_bindings'] != admission['bindings']
                    or data.get('guest_boundary') != preflight['boundary']):
                raise ValueError('restart source/config identity differs from stable journal')
            self.approved_tokens()
            if data.get('cleanup', {}).get('cleanup_complete'):
                raise RuntimeError('completed attempt may not allocate another fleet')
            active = self.active()
            attempted = [attempt for attempt in data['attempts'] if attempt['status'] != 'capacity-rejected']
            if attempted:
                attempt = attempted[-1]
                if active:
                    return self.validate_fleet(active, attempt['candidate']), attempt['candidate'], data['plan']
                raise RuntimeError('previous allocation intent has no visible fleet; unknown/finished outcomes never allocate again')
            if active:
                raise RuntimeError('owned fleet exists without matching launch intent')
        else:
            if self.active():
                raise RuntimeError('same-attempt fleet exists without trusted journal; cleanup required')
        from ci_budget import BudgetExceeded, BudgetLedger
        ledger = BudgetLedger(self.s3, self.cfg['bucket'], self.cfg['account'], repository=REPOSITORY,
                              branch=BRANCH, budget_path=self.cfg.get('budget_path', DEFAULT_BUDGET))
        if data is None:
            overhead = overhead_estimate(admission, preflight['ancillary_prices'], self.cfg.get('cost_assumptions', {}))
            available = Decimal(ledger.report()['available_usd'])
            candidates = [candidate for candidate in preflight['candidates']
                          if _reservation_quote(candidate['maximum_estimate_usd'], overhead,
                                                ttl_seconds=self.cfg['ttl_seconds'])[0] <= available]
            if not candidates:
                raise BudgetExceeded('no full six-host candidate fits the remaining weekly budget')
            self.journal.data = data = dict(schema=1, run_id=self.run_id, ownership=ownership(self.run_id),
                account=self.cfg['account'], configured_regions=self.cfg['regions'],
                config_sha256=hashlib.sha256(encoded(self.cfg)).hexdigest(), input_bindings=admission['bindings'],
                mutations=[], attempts=[], uploads={}, created_utc=utc(), expires_at=None,
                candidates=candidates, instance_profiles=planned_profiles(self.cfg, self.run_id),
                guest_boundary=preflight['boundary'],
                budget_policy=preflight.get('budget_policy'), overhead=overhead)
            self.journal.save()
        estimate, reconciliation = _reservation_quote(
            max(Decimal(candidate['maximum_estimate_usd']) for candidate in data['candidates']), data['overhead'],
            ttl_seconds=self.cfg['ttl_seconds'])
        reserved = ledger.reserve(self.run_id, estimate, dict(input_bindings=admission['bindings'],
            candidates=ledger_candidates(data['candidates']), overhead=data['overhead'],
            reconciliation_usd=str(reconciliation), ttl_seconds=self.cfg['ttl_seconds'], instance_count=6))
        data['budget_policy'] = reserved['budget_policy']
        data.update(config_audit(self.cfg, data))
        data['budget_reserved'] = True
        self.journal.save()
        self.report_budget(ledger, 'reserve')
        for name, item in admission['inputs'].items():
            filename = {'build_bundle': 'build-bundle.tar.gz', 'fixture_bundle': 'fixture.tar.gz',
                        'verification_bundle': 'verification.tar.gz', 'runner_bundle': 'bundle.tar.gz'}[name]
            key = self.run_id + ('/input/' if name == 'runner_bundle' else '/inputs/') + filename
            data['uploads'][name] = self.upload(item['path'], key)
            self.journal.save()
        if not data['expires_at']:
            data['expires_at'] = utc(datetime.fromtimestamp(self.clock.time(), timezone.utc) + timedelta(seconds=self.cfg['ttl_seconds']))
            self.journal.save()
        self.provision_profiles()
        for candidate in data['candidates']:
            if any(attempt['candidate'] == candidate for attempt in data['attempts']):
                continue
            if self.active():
                raise RuntimeError('owned fleet appeared before allocation; refusing overlap')
            if parsed(data['expires_at']).timestamp() - self.clock.time() < 1800:
                raise TimeoutError('candidate fallback cannot extend original paid lifetime')
            group_id = self.network(candidate)
            request, plan = self.request(candidate, group_id, admission, data['instance_profiles'])
            if (request['ClientToken'] != candidate_token(self.run_id, candidate)
                    or plan['atomic_client_token'] != request['ClientToken']):
                raise ValueError('atomic request and guest plan differ from immutable candidate token')
            data['plan'] = plan
            attempt = dict(candidate=candidate, client_token=request['ClientToken'], status='allocation-outcome-unknown',
                           request_sha256=hashlib.sha256(encoded(request)).hexdigest(), started_utc=utc(),
                           userdata_sha256=hashlib.sha256(request['UserData'].encode()).hexdigest(),
                           bootstrap_sha256=admission['bootstrap_sha256'],
                           atomic_start_sha256=admission['atomic_start_sha256'])
            data['attempts'].append(attempt)
            self.journal.intent('run_instances', candidate['region'], dict(request_sha256=attempt['request_sha256'],
                                client_token=request['ClientToken'], candidate=candidate))
            try:
                response = self.aws.client('ec2', candidate['region']).run_instances(**request)
                attempt['returned_ids'] = sorted(item['InstanceId'] for item in response.get('Instances', []))
                self.journal.save()
            except Exception as error:
                attempt['error'] = repr(error)
                attempt['error_code'] = error_code(error)
                self.journal.save()
                reconciled = []
                for _ in range(6):
                    reconciled = self.active()
                    if reconciled:
                        break
                    self.clock.sleep(3)
                if reconciled:
                    instances = self.validate_fleet(reconciled, candidate)
                    attempt['reconciled_ids'] = sorted(item['InstanceId'] for item in instances)
                    attempt['status'] = 'reconciled-six-host-allocation'
                    self.journal.save()
                    return instances, candidate, plan
                if error_code(error) in CAPACITY_ERRORS:
                    attempt['status'] = 'capacity-rejected'
                    self.journal.save()
                    continue
                raise RuntimeError('RunInstances outcome unknown; stable intent retained, no second candidate') from error
            records = []
            for _ in range(6):
                records = self.active()
                if records:
                    break
                self.clock.sleep(3)
            instances = self.validate_fleet(records, candidate)
            if set(attempt['returned_ids']) != {item['InstanceId'] for item in instances}:
                raise ValueError('returned and reconciled atomic host IDs differ')
            attempt['status'] = 'allocated'
            self.journal.save()
            return instances, candidate, plan
        raise RuntimeError('all live-priced candidates definitively rejected capacity or quota')

    def bind_profiles(self, current, candidate, profiles, end):
        """Bind six exact per-run profiles before any controller activation."""
        ec2 = self.aws.client('ec2', candidate['region'])
        index = {item['AmiLaunchIndex']: item for item in current}
        if profiles != planned_profiles(self.cfg, self.run_id):
            raise ValueError('final profile map is not the exact per-run six-role namespace')
        self.finalize_guest_policies(current, candidate, profiles)
        for i in range(6):
            role = 'controller' if i == 5 else f'storage-{i}'
            instance = index[i]
            if instance.get('IamInstanceProfile', {}).get('Arn') != profiles[role]['arn']:
                associations = ec2.describe_iam_instance_profile_associations(Filters=[
                    {'Name': 'instance-id', 'Values': [instance['InstanceId']]}])['IamInstanceProfileAssociations']
                if not associations:
                    self.mutate('associate_iam_instance_profile', candidate['region'], dict(
                        InstanceId=instance['InstanceId'], IamInstanceProfile={'Name': profiles[role]['name']}))
                elif (len(associations) == 1 and associations[0]['State'] == 'associated'
                      and associations[0].get('IamInstanceProfile', {}).get('Arn')
                      in {identity['arn'] for identity in profiles.values()}):
                    self.mutate('replace_iam_instance_profile_association', candidate['region'], dict(
                        AssociationId=associations[0]['AssociationId'], IamInstanceProfile={'Name': profiles[role]['name']}))
                else:
                    raise ValueError('existing profile association is not exact run-owned')
        while self.clock.time() < end:
            current = self.validate_fleet(self.active(), candidate)
            if all(item.get('IamInstanceProfile', {}).get('Arn') == profiles[
                    'controller' if item['AmiLaunchIndex'] == 5 else f"storage-{item['AmiLaunchIndex']}"]['arn'] for item in current):
                return current
            self.clock.sleep(5)
        raise TimeoutError('final exact per-run profiles did not converge')

    def configure(self, instances, candidate, plan):
        from checkpoint_matrix import validate_placement
        import sui_checkpoint as supervisor
        ec2 = self.aws.client('ec2', candidate['region'])
        end = min(self.clock.time() + 240, parsed(self.journal.data['expires_at']).timestamp() - 1200)
        while self.clock.time() < end:
            current = self.validate_fleet(self.active(), candidate)
            if all(item['State']['Name'] == 'running' and item.get('PrivateIpAddress') for item in current):
                break
            self.clock.sleep(5)
        else:
            raise TimeoutError('six-host running readiness exceeded original startup reserve')
        current = self.bind_profiles(current, candidate, plan['instance_profiles'], end)
        index = {item['AmiLaunchIndex']: item for item in current}
        deadline = min(parsed(self.journal.data['expires_at']),
                       *(parsed(item['LaunchTime']) + timedelta(seconds=self.cfg['ttl_seconds']) for item in current))
        cfg = {key: self.cfg[key] for key in ('transports', 'repetitions', 'read_profile_limits',
            'write_profile_limits', 'native_write_profile_limits', 'kv_suite', 'coordinator_cache_mib',
            'shard_cache_mib', 'replay_deadline_seconds')}
        cfg.update({name: self.cfg[name] for name in
            ('coordinator_cache_mib_by_repetition', 'shard_cache_mib_by_repetition') if name in self.cfg})
        cfg.update(plan['atomic_bindings'])
        cfg.update(campaign='sui-checkpoint', run_id=self.run_id, account=self.cfg['account'], region=candidate['region'],
            availability_zone=candidate['az'], placement_group=self.run_id, bucket=self.cfg['bucket'], s3_prefix=self.run_id,
            run_root='/mnt/tidehunter/' + self.run_id, deadline_utc=utc(deadline), export_reserve_seconds=600,
            terminate_on_finish=True, instance_type=candidate['instance_type'], coordinator_instance_id=index[5]['InstanceId'],
            coordinator_private_ip=index[5]['PrivateIpAddress'], nodes=[dict(partition=i, instance_id=index[i]['InstanceId'],
            private_ip=index[i]['PrivateIpAddress']) for i in range(5)])
        cfg.update(config_audit(self.cfg, self.journal.data))
        supervisor.validate_cfg(cfg)
        groups = ec2.describe_placement_groups(GroupNames=[self.run_id])['PlacementGroups']
        for group in groups:
            self.exact(group)
        validate_placement(cfg, current, groups)
        target = self.output / 'controller-config.json'
        dump(target, cfg)
        receipt = self.upload(target, self.run_id + '/inputs/controller-config.json')
        self.journal.data['controller_config'] = cfg
        self.journal.data['controller_config_sha256'] = receipt['sha256']
        self.journal.save()
        # Role tags are the final guest-side permission to acquire credentials.
        for i in range(6):
            tags = {'Role': 'controller' if i == 5 else f'storage-{i}'}
            if i == 5:
                tags['ControllerConfigSha256'] = receipt['sha256']
            self.mutate('create_tags', candidate['region'], dict(Resources=[index[i]['InstanceId']], Tags=aws_tags(tags)))
        return cfg

    def object_json(self, key):
        try:
            response = self.s3.get_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
        except Exception as error:
            if error_code(error) in MISSING:
                return None
            raise
        with response['Body'] as body:
            raw = body.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError('oversized guest JSON')
        digest = response.get('Metadata', {}).get('sha256')
        if digest and hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('guest JSON digest mismatch')
        return json.loads(raw)

    def monitor(self, cfg):
        end = parsed(cfg['deadline_utc']).timestamp() - 15
        while self.clock.time() < end:
            receipt = self.object_json(self.run_id + '/results/receipt.json')
            if receipt:
                if (receipt.get('run_id') != self.run_id or receipt.get('bucket') != self.cfg['bucket']
                        or receipt.get('key') != self.run_id + '/results/controller.tar.gz'
                        or receipt.get('build_manifest_sha256') != cfg['build_manifest_sha256']
                        or receipt.get('checkpoint_digest') != CHECKPOINT
                        or not re.fullmatch(r'[a-f0-9]{64}', receipt.get('sha256', ''))):
                    raise ValueError('controller export receipt identity differs')
                head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=receipt['key'], ExpectedBucketOwner=self.cfg['account'])
                if head['ContentLength'] != receipt['bytes'] or head.get('Metadata', {}).get('sha256') != receipt['sha256']:
                    raise ValueError('controller S3 export was not durably bound before termination')
                dump(self.output / 'export-receipt.json', dict(receipt, **config_audit(self.cfg, self.journal.data)))
                return receipt
            if not self.active():
                raise RuntimeError('fleet terminated without requested final result export')
            self.clock.sleep(10)
        raise TimeoutError('original absolute campaign/export deadline reached')

    def exact_instance(self, region, instance_id):
        ec2 = self.aws.client('ec2', region)
        try:
            response = ec2.describe_instances(InstanceIds=[instance_id])
        except Exception as error:
            if error_code(error) == 'InvalidInstanceID.NotFound':
                return None
            raise
        found = []
        for reservation in response['Reservations']:
            if reservation.get('OwnerId') != self.cfg['account']:
                raise ValueError('foreign exact-ID reservation owner')
            found.extend(reservation['Instances'])
        if not found:
            return None
        if len(found) != 1 or found[0]['InstanceId'] != instance_id:
            raise ValueError('exact-ID lookup returned another instance')
        item = self.exact(found[0])
        self.check_token(region, item.get('ClientToken'))
        return item

    def cleanup_inventory(self, receipt):
        data = self.journal.data or {}
        previous = {}
        try:
            candidates = list(data.get('terminal_proofs', {}).values())
            candidates += [item for item in data.get('cleanup', {}).get('instances', [])
                           if item.get('final_state') == 'terminated']
        except (AttributeError, TypeError) as error:
            receipt['errors'].append('malformed persisted terminal inventory: ' + repr(error))
            candidates = []
        for proof in candidates:
            try:
                if (proof.get('final_state') != 'terminated' or not proof.get('verified_utc')
                        or proof.get('region') not in self.cfg['regions']
                        or not re.fullmatch(r'i-[a-f0-9]{17}', proof.get('instance_id', ''))):
                    raise ValueError('persisted terminal proof identity is malformed')
                parsed(proof['verified_utc'])
                parsed(proof['launch_time'])
                self.check_token(proof['region'], proof.get('client_token'))
                previous[proof['region'] + '/' + proof['instance_id']] = proof
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                receipt['errors'].append('malformed terminal proof: ' + repr(error))
        known = set()
        for attempt in data.get('attempts', []):
            try:
                region = attempt['candidate']['region']
                for instance_id in [*attempt.get('returned_ids', []), *attempt.get('reconciled_ids', [])]:
                    if region not in self.cfg['regions'] or not re.fullmatch(r'i-[a-f0-9]{17}', instance_id):
                        raise ValueError('journalled allocation identity is malformed')
                    known.add((region, instance_id))
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                receipt['errors'].append('malformed allocation identity: ' + repr(error))
        known.update((proof['region'], proof['instance_id']) for proof in previous.values())
        discovered = {}
        for region in self.cfg['regions']:
            try:
                for record in self.inventory([region], errors=receipt['errors']):
                    item = record['instance']
                    discovered[region, item['InstanceId']] = item
            except Exception as error:
                receipt['errors'].append(region + ' inventory: ' + repr(error))
        for region, instance_id in sorted(known - discovered.keys()):
            try:
                item = None
                for _ in range(6):
                    item = self.exact_instance(region, instance_id)
                    if item is not None:
                        discovered[region, instance_id] = item
                        break
                    if region + '/' + instance_id in previous:
                        break
                    self.clock.sleep(3)
                if item is None and region + '/' + instance_id not in previous:
                    receipt['errors'].append(instance_id + ': known allocation lacks observed terminal proof; disappearance is not termination')
            except Exception as error:
                receipt['errors'].append(instance_id + ': ' + repr(error))
        return discovered, previous

    def cleanup(self):
        """Reconcile journal-ID UNION tag inventory; only observed termination is proof."""
        receipt = dict(run_id=self.run_id, cleanup_complete=False, instances=[], auxiliary=[], iam=[],
                       errors=[], observed_utc=utc(), **config_audit(self.cfg, self.journal.data))
        if self.journal_error:
            receipt['errors'].append('control journal unavailable: ' + self.journal_error)
        data = self.journal.data or {}
        try:
            discovered, previous = self.cleanup_inventory(receipt)
        except Exception as error:
            discovered, previous = {}, {}
            receipt['errors'].append('cleanup identity reconciliation: ' + repr(error))
        keys = set(discovered) | {(item['region'], item['instance_id']) for item in previous.values()}
        pending = {}
        # Reconcile/request the whole fleet before spending any time waiting for one host.
        for region, instance_id in sorted(keys):
            item = discovered.get((region, instance_id))
            prior = previous.get(region + '/' + instance_id)
            if prior and (item is None or item['State']['Name'] == 'terminated'):
                receipt['instances'].append(prior)
                continue
            if item is None:
                continue
            summary = dict(region=region, instance_id=instance_id, launch_time=str(item.get('LaunchTime')),
                           instance_type=item['InstanceType'], initial_state=item['State']['Name'],
                           client_token=item['ClientToken'])
            receipt['instances'].append(summary)
            try:
                current = self.exact_instance(region, instance_id)
                if current is None:
                    raise ValueError('known allocation disappeared without observed terminal proof')
                summary['final_state'] = current['State']['Name']
                if current['State']['Name'] == 'terminated':
                    summary['verified_utc'] = utc()
                    continue
                if current['State']['Name'] != 'shutting-down':
                    try:
                        self.aws.client('ec2', region).terminate_instances(InstanceIds=[instance_id])
                    except Exception as error:
                        if error_code(error) in {'UnauthorizedOperation', 'AccessDenied', 'AuthFailure'}:
                            raise
                        summary['termination_request_error'] = repr(error)
                pending[region, instance_id] = summary
            except Exception as error:
                receipt['errors'].append(instance_id + ': ' + repr(error))
        # One shared observation window: slow hosts must not delay requests to their peers
        # or leave a sticky per-host timeout when they terminate within the fleet window.
        end = self.clock.time() + 600
        for _ in range(120):
            remaining = end - self.clock.time()
            if not pending or remaining <= 0:
                break
            for (region, instance_id), summary in list(pending.items()):
                if self.clock.time() >= end:
                    break
                try:
                    current = self.exact_instance(region, instance_id)
                    if current is None:
                        raise ValueError('known allocation disappeared without observed terminal proof')
                    summary['final_state'] = current['State']['Name']
                    if current['State']['Name'] == 'terminated':
                        summary['verified_utc'] = utc()
                        del pending[region, instance_id]
                except Exception as error:
                    receipt['errors'].append(instance_id + ': ' + repr(error))
                    del pending[region, instance_id]
            remaining = end - self.clock.time()
            if pending and remaining > 0:
                self.clock.sleep(min(5, remaining))
        for region, instance_id in pending:
            receipt['errors'].append(instance_id + ': ' + repr(TimeoutError('EC2 did not confirm terminated')))
        unknown = [attempt for attempt in data.get('attempts', []) if attempt['status'] == 'allocation-outcome-unknown']
        if unknown and len(receipt['instances']) != 6:
            receipt['errors'].append('allocation outcome still unknown; incomplete visibility is not proof of no mutation')
        try:
            remaining = self.active()
            if remaining:
                receipt['errors'].append('active owned instances remain: ' + repr([item['instance']['InstanceId'] for item in remaining]))
        except Exception as error:
            receipt['errors'].append('final inventory: ' + repr(error))
        receipt['compute_cleanup_complete'] = not receipt['errors']
        if self.journal.data:
            data['terminal_proofs'] = {item['region'] + '/' + item['instance_id']: item for item in receipt['instances']
                                      if item.get('final_state') == 'terminated'}
            try:
                self.journal.save()
            except Exception as error:
                self.journal_error = repr(error)
                receipt['errors'].append('terminal proof persistence: ' + repr(error))
        for region in self.cfg['regions']:
            self.cleanup_auxiliary(region, receipt)
        if receipt['compute_cleanup_complete']:
            self.cleanup_profiles(receipt)
        else:
            receipt['errors'].append('IAM preserved until complete cross-region compute termination proof')
        self.cleanup_multipart(receipt)
        receipt['cleanup_complete'] = not receipt['errors'] and not self.journal_error
        immutable_cleanup = data.get('cleanup')
        if immutable_cleanup and immutable_cleanup.get('cleanup_complete'):
            dump(self.output / 'cleanup-receipt.json', immutable_cleanup)
            receipt_path = self.output / 'cleanup-verification.json'
        else:
            receipt_path = self.output / 'cleanup-receipt.json'
        dump(receipt_path, receipt)
        if self.journal.data:
            if not data.get('cleanup', {}).get('cleanup_complete'):
                data['cleanup'] = receipt
            else:
                data['latest_cleanup_verification'] = receipt
            try:
                self.journal.save()
            except Exception as error:
                receipt['errors'].append('cleanup journal persistence: ' + repr(error))
                receipt['cleanup_complete'] = False
                dump(receipt_path, receipt)
        return receipt

    def wait_iam_absent(self, reader, reason):
        for _ in range(8):
            if reader() is None:
                return
            self.clock.sleep(2)
        raise TimeoutError(reason)

    def cleanup_profiles(self, receipt):
        iam = self.aws.client('iam', self.cfg['bucket_region'])
        profiles = planned_profiles(self.cfg, self.run_id)
        expected_arns = {identity['arn'] for identity in profiles.values()}
        for region in self.cfg['regions']:
            try:
                ec2 = self.aws.client('ec2', region)
                for page in pages(lambda **params: ec2.describe_instances(**params)):
                    for reservation in page['Reservations']:
                        if reservation.get('OwnerId') != self.cfg['account']:
                            raise ValueError('foreign owner in fresh profile-use scan')
                        if any(item.get('IamInstanceProfile', {}).get('Arn') in expected_arns
                               and item['State']['Name'] != 'terminated' for item in reservation['Instances']):
                            raise ValueError('a live instance still references a run profile')
            except Exception as error:
                receipt['errors'].append('IAM preserved: incomplete fresh profile-use scan ' + region + ': ' + repr(error))
                return
        for role in sorted(ROLES):
            identity = profiles[role]
            name = identity['name']
            try:
                read_role = lambda: self.iam_read(lambda: iam.get_role(RoleName=name))
                read_profile = lambda: self.iam_read(lambda: iam.get_instance_profile(InstanceProfileName=name))
                found, profile = read_role(), read_profile()
                inline, attached = [], []
                if found:
                    inline, attached = self.checked_role(found['Role'], name)
                if profile:
                    self.checked_profile(profile['InstanceProfile'], identity)
                    if profile['InstanceProfile'].get('Roles'):
                        if found is None:
                            raise ValueError('profile membership has no boundary-verified role')
                        self.iam_mutate('remove_role_from_instance_profile',
                            dict(InstanceProfileName=name, RoleName=name), cleanup=True)
                        self.wait_iam(read_profile, lambda value: not self.checked_profile(
                            value['InstanceProfile'], identity).get('Roles'), 'profile role removal did not converge')
                    self.iam_mutate('delete_instance_profile', dict(InstanceProfileName=name), cleanup=True)
                    self.wait_iam_absent(read_profile, 'instance profile deletion is not verified')
                if found:
                    for policy in inline:
                        self.iam_mutate('delete_role_policy', dict(RoleName=name, PolicyName=policy), cleanup=True)
                    for policy_arn in attached:
                        self.iam_mutate('detach_role_policy', dict(RoleName=name, PolicyArn=policy_arn), cleanup=True)
                    self.iam_mutate('delete_role', dict(RoleName=name), cleanup=True)
                    self.wait_iam_absent(read_role, 'guest role deletion is not verified')
                unknown = [item for item in (self.journal.data or {}).get('mutations', [])
                    if item['operation'] in ('create_role', 'create_instance_profile')
                    and item['status'] == 'outcome-unknown'
                    and name in (item['params'].get('RoleName'), item['params'].get('InstanceProfileName'))]
                prior_deleted = any(item.get('name') == name and item.get('verified_absent')
                    for item in (self.journal.data or {}).get('cleanup', {}).get('iam', []))
                if unknown and found is None and profile is None and not prior_deleted:
                    raise RuntimeError('IAM creation outcome still unknown; absence is not deletion proof')
                receipt['iam'].append(dict(name=name, verified_absent=True,
                    boundary_arn=self.cfg['guest_boundary_arn'], observed_utc=utc()))
            except Exception as error:
                receipt['errors'].append('IAM ' + name + ': ' + repr(error))

    def cleanup_multipart(self, receipt):
        try:
            uploads = [item for page in pages(lambda **params: self.s3.list_multipart_uploads(**params),
                pagination='s3-multipart', Bucket=self.cfg['bucket'], Prefix=self.run_id + '/', ExpectedBucketOwner=self.cfg['account'])
                for item in page.get('Uploads', [])]
            for item in uploads:
                if not item['Key'].startswith(self.run_id + '/'):
                    raise ValueError('multipart upload outside exact run prefix')
                self.s3.abort_multipart_upload(Bucket=self.cfg['bucket'], Key=item['Key'],
                    UploadId=item['UploadId'], ExpectedBucketOwner=self.cfg['account'])
            remaining = [item for page in pages(lambda **params: self.s3.list_multipart_uploads(**params),
                pagination='s3-multipart', Bucket=self.cfg['bucket'], Prefix=self.run_id + '/', ExpectedBucketOwner=self.cfg['account'])
                for item in page.get('Uploads', [])]
            if remaining:
                raise RuntimeError('owned unfinished multipart uploads remain')
            receipt['multipart_cleanup_complete'] = True
        except Exception as error:
            receipt['errors'].append('multipart cleanup: ' + repr(error))


    def cleanup_auxiliary(self, region, receipt):
        ec2 = self.aws.client('ec2', region)
        specs = [('network-interface', lambda **params: ec2.describe_network_interfaces(**params), 'NetworkInterfaces', 'NetworkInterfaceId'),
                 ('volume', lambda **params: ec2.describe_volumes(**params), 'Volumes', 'VolumeId'),
                 ('security-group', lambda **params: ec2.describe_security_groups(**params), 'SecurityGroups', 'GroupId'),
                 ('placement-group', None, 'PlacementGroups', 'GroupName')]
        for kind, request, field, identifier in specs:
            try:
                resources = (ec2.describe_placement_groups(Filters=self.filters())[field]
                             if kind == 'placement-group' else
                             [item for page in pages(request, Filters=self.filters()) for item in page[field]])
            except Exception as error:
                receipt['errors'].append(region + '/' + kind + ' inventory: ' + repr(error))
                continue
            for item in resources:
                name = item[identifier]
                try:
                    self.exact(item)
                    for _ in range(36):
                        if kind == 'network-interface':
                            check = ec2.describe_network_interfaces(Filters=[{'Name': 'network-interface-id', 'Values': [name]}])['NetworkInterfaces']
                        elif kind == 'volume':
                            check = ec2.describe_volumes(Filters=[{'Name': 'volume-id', 'Values': [name]}])['Volumes']
                        elif kind == 'security-group':
                            check = ec2.describe_security_groups(Filters=[{'Name': 'group-id', 'Values': [name]}])['SecurityGroups']
                        else:
                            check = ec2.describe_placement_groups(Filters=[{'Name': 'group-name', 'Values': [name]}])['PlacementGroups']
                        if not check:
                            receipt['auxiliary'].append(dict(kind=kind, region=region, identifier=name, verified_absent=True))
                            break
                        if len(check) != 1:
                            raise ValueError('auxiliary exact-ID lookup is ambiguous')
                        current = self.exact(check[0])
                        try:
                            if kind == 'network-interface':
                                if not current.get('Attachment') and current.get('Status') == 'available':
                                    ec2.delete_network_interface(NetworkInterfaceId=name)
                            elif kind == 'volume':
                                if not current.get('Attachments') and current.get('State') == 'available':
                                    ec2.delete_volume(VolumeId=name)
                            elif kind == 'security-group':
                                ec2.delete_security_group(GroupId=name)
                            else:
                                if name != self.run_id or current.get('Strategy') != 'cluster':
                                    raise ValueError('unexpected run placement group')
                                ec2.delete_placement_group(GroupName=name)
                        except Exception as error:
                            if error_code(error) not in MISSING | {'DependencyViolation', 'VolumeInUse', 'InvalidPlacementGroup.InUse'}:
                                raise
                        self.clock.sleep(5)
                    else:
                        raise TimeoutError('detached auxiliary deletion not verified absent')
                except Exception as error:
                    receipt['errors'].append(region + '/' + name + ': ' + repr(error))

    def retrieve(self):
        inventory = []
        for page in pages(lambda **params: self.s3.list_objects_v2(**params), pagination='s3-objects',
                          Bucket=self.cfg['bucket'], Prefix=self.run_id + '/', ExpectedBucketOwner=self.cfg['account']):
            self.observe_s3('ListObjectsV2')
            inventory.extend(page.get('Contents', []))
        manifest, errors = [], []
        for item in inventory:
            key = item['Key']
            rel = PurePosixPath(key)
            if not key.startswith(self.run_id + '/') or '..' in rel.parts or rel.is_absolute():
                raise ValueError('unsafe full-prefix retrieval path')
            target = self.output / 's3' / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            downloaded = reused = download_attempted = False
            try:
                if target.is_file() and not target.is_symlink():
                    head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
                    self.observe_s3('HeadObject')
                    expected_digest = head.get('Metadata', {}).get('sha256')
                    if (expected_digest and target.stat().st_size == head['ContentLength'] == item['Size']
                            and sha(target) == expected_digest):
                        digest, reused = expected_digest, True
                if not reused:
                    download_attempted = True
                    self.s3.download_file(self.cfg['bucket'], key, str(target), ExtraArgs={'ExpectedBucketOwner': self.cfg['account']})
                    downloaded = True
                    self.observe_s3(read_bytes=target.stat().st_size)
                    head = self.s3.head_object(Bucket=self.cfg['bucket'], Key=key, ExpectedBucketOwner=self.cfg['account'])
                    self.observe_s3('HeadObject')
                    digest = sha(target)
                if target.stat().st_size != head['ContentLength'] or target.stat().st_size != item['Size']:
                    raise ValueError('retrieved full-prefix object length changed')
                if head.get('Metadata', {}).get('sha256') and head['Metadata']['sha256'] != digest:
                    raise ValueError('retrieved full-prefix object digest differs')
                manifest.append(dict(key=key, bytes=target.stat().st_size, sha256=digest,
                                     retrieval_mode='local-sha256-reuse' if reused else 'download',
                                     remote_etag=head.get('ETag'), inventory_etag=item.get('ETag')))
            except Exception as error:
                if download_attempted and not downloaded:
                    self.observe_s3(failed_download=True)
                errors.append(dict(key=key, error=repr(error)))
        receipt = dict(run_id=self.run_id, complete=not errors, inventory=inventory, files=manifest, errors=errors,
                       **config_audit(self.cfg, self.journal.data))
        dump(self.output / 'retrieval-manifest.json', receipt)
        accounting = self.account_export(inventory)
        if accounting:
            receipt['s3_accounting'] = accounting
            dump(self.output / 'retrieval-manifest.json', receipt)
        if errors:
            raise RuntimeError('complete owned-prefix retrieval failed: ' + repr(errors))
        if accounting and accounting['export_allowance_exceeded']:
            raise RuntimeError('actual exported bytes exceed reviewed result_gib admission sizing; full evidence retained and actual size accounted')
        return receipt

    def account_export(self, inventory):
        """Price observed resources only; future retention never enters booked usage."""
        data = self.journal.data or {}
        if not data.get('overhead'):
            return None
        prices, assumptions = data['overhead']['prices'], data['overhead']['assumptions']
        observed = datetime.fromtimestamp(self.clock.time(), timezone.utc)
        input_bytes = sum(item['Size'] for item in inventory if item['Key'].startswith(
            (self.run_id + '/inputs/', self.run_id + '/input/')))
        total_bytes = sum(item['Size'] for item in inventory)
        exported_bytes = total_bytes - input_bytes
        counters = data.get('s3_observations')
        unknown = ['historical/guest/managed-transfer requests and retries',
                   'guest downloads and uploads, failed/partial reads, cross-region routing',
                   'deleted/overwritten objects, versions, multipart and control-prefix storage',
                   'final AWS billed charges, discounts and free tiers']
        missing_timestamps = 0
        with localcontext() as context:
            context.prec, context.rounding = 60, ROUND_CEILING
            byte_seconds = Decimal(0)
            for item in inventory:
                if not item.get('LastModified'):
                    missing_timestamps += 1
                    continue
                duration = observed - parsed(item['LastModified'])
                seconds = Decimal(duration.days * 86400 + duration.seconds) + Decimal(duration.microseconds) / Decimal(1000000)
                if seconds < 0:
                    raise ValueError('S3 object modification time is after accounting observation')
                byte_seconds += Decimal(item['Size']) * seconds
            storage = usd_ceiling(byte_seconds / Decimal(1024 ** 3) / Decimal(28 * 86400)
                                  * Decimal(prices['storage_usd_per_gib_month']))
            projection = usd_ceiling(Decimal(total_bytes) / Decimal(1024 ** 3)
                * Decimal(prices['storage_usd_per_gib_month']) * Decimal(assumptions['retention_days']) / Decimal(28))
            # These prices are highest paid tiers, not proof that a transfer/request
            # was billable at that rate. Do not infer managed SDK or guest counts.
            transfer = (usd_ceiling(Decimal(counters['successful_local_read_bytes']) / Decimal(1024 ** 3)
                        * Decimal(prices['transfer_usd_per_gib'])) if counters else None)
            requests = (usd_ceiling(Decimal(sum(counters['successful_requests'].values()))
                        * Decimal(prices['request_usd'])) if counters else None)
            total = usd_ceiling(storage + (transfer or Decimal(0)) + (requests or Decimal(0)))
        if missing_timestamps:
            unknown.append('elapsed storage for objects without LastModified')
        record = dict(accounting_basis='observed-resources-v2', coverage='partial',
            input_bytes=input_bytes, exported_bytes=exported_bytes, retained_bytes=total_bytes,
            observed_utc=utc(observed), sizing_result_gib=assumptions['result_gib'],
            export_allowance_exceeded=exported_bytes > assumptions['result_gib'] * 1024 ** 3,
            estimate_usd=str(total), storage_estimate_usd=str(storage) if inventory and missing_timestamps < len(inventory) else None,
            storage_observed_byte_seconds=str(byte_seconds), storage_missing_timestamps=missing_timestamps,
            transfer_estimate_usd=str(transfer) if transfer is not None else None,
            request_estimate_usd=str(requests) if requests is not None else None,
            observations=json.loads(encoded(counters)) if counters else None,
            unknown_components=unknown, prices=prices,
            projections=dict(amount_usd=str(projection), s3_retention_days=assumptions['retention_days'],
                             basis='future retained-current-bytes storage only; excluded from usage_usd'),
            scope='priced upper-bound estimate for observed resources only; partial coverage, not a final AWS bill',
            **config_audit(self.cfg, data))
        data['s3_accounting'] = record
        self.journal.save()
        dump(self.output / 's3-cost-evidence.json', record)
        return record


    def validate_results(self, cfg):
        import sui_checkpoint as supervisor
        from checkpoint_matrix import validate_matrix, fabric_local_log, fabric_local_archive_log
        from checkpoint_kv import plan as kv_plan, validate_auxiliary
        root = self.output / 's3' / self.run_id / 'results'
        receipt = read_json(root / 'receipt.json')
        archive = root / 'controller.tar.gz'
        if receipt.get('complete') is not True or sha(archive) != receipt['sha256']:
            raise ValueError('requested campaign did not export complete bound results')
        expanded = self.output / 'controller-evidence'
        supervisor.safe_extract(archive, expanded)
        results = expanded / 'results'
        state = read_json(results / 'campaign-receipt.json')
        if state.get('complete') is not True or state.get('failures') or state.get('run_id') != self.run_id:
            raise ValueError('controller campaign receipt incomplete')
        validate_matrix(cfg, state['cells'])
        fabric_sui = []
        for cell in state['cells']:
            directory = results / 'cells' / cell['cell_id']
            if sha(directory / 'replay-evidence.tar.gz') != cell['replay_evidence_sha256'] or sha(directory / 'resources.json') != cell['resources_sha256']:
                raise ValueError('matrix retained replay/resource digest differs')
            replay = read_json(directory / 'replay-logs/results.json')
            audit = read_json(directory / 'replay-logs/audit.json')
            if (replay.get('complete') is not True or replay.get('failure') or replay.get('audit') != audit
                    or audit.get('all_equal') is not True or not audit.get('databases') or not audit.get('rocksdb')
                    or replay['build_manifest_sha256'] != cfg['build_manifest_sha256']
                    or replay['binaries'] != cfg['binaries']
                    or any(replay[mode]['completed_checkpoint_digest'] != CHECKPOINT for mode in ('native', 'remote'))):
                raise ValueError('requested cell missing real checkpoint/full-state audit proof')
            diagnostic = bool(cell['read_profile_limit'] or cell.get('write_profile_limit', 0))
            cell_fabric = dict(cell_id=cell['cell_id'], transport=cell['transport'],
                population='diagnostic' if diagnostic else 'clean', clients=[], storage=[],
                scope='emitted finalized local endpoint windows only; SIGTERM can bypass Drop; not packetization, RTT or latency')
            for mode in ('native', 'remote'):
                cell_fabric['clients'].append(fabric_local_log(
                    directory / 'replay-logs' / (mode + '.log'),
                    forbidden=mode == 'native' or not diagnostic))
            for node in cfg['nodes']:
                expected = cell['node_archives'][node['instance_id']]
                local = self.output / 's3' / expected['key']
                if not local.is_file() or sha(local) != expected['sha256']:
                    raise ValueError('requested cell storage evidence missing/different')
                with tarfile.open(local, 'r:gz') as stream:
                    names = sorted(member.name for member in stream.getmembers()
                        if re.fullmatch(r'cloud-node-logs/namespace-[0-9]+\.stderr\.log', member.name))
                    if not names:
                        raise ValueError('Sui node archive omitted namespace stderr logs')
                    logs = [fabric_local_archive_log(stream, name, forbidden=not diagnostic) for name in names]
                cell_fabric['storage'].append(dict(partition=node['partition'], archive=str(local), logs=logs))
            fabric_sui.append(cell_fabric)
        kv = state.get('kv_suite', {})
        expected_plan = kv_plan(cfg['kv_suite'], cfg['transports'])
        if (kv.get('complete') is not True or kv.get('failures') or kv.get('plan') != expected_plan
                or len(kv.get('completed_cases', [])) != 2 * len(expected_plan)
                or kv.get('source_archive_sha256') != cfg['source_archive_sha256']
                or kv.get('binary_sha256') != cfg['binaries']['storage-node']):
            raise ValueError('full requested KV suite missing or narrowed')
        auxiliary = validate_auxiliary(results / 'kv', kv, cfg)
        dump(self.output / 'auxiliary-measurements.json', auxiliary)
        dump(self.output / 'fabric-local-diagnostics.json', dict(sui=fabric_sui,
            kv=auxiliary['fabric_local_diagnostics']))
        dump(self.output / 'completion-proof.json', dict(run_id=self.run_id, complete=True,
             controller_sha256=sha(archive), cells=len(state['cells']), kv_cases=len(kv['completed_cases']),
             cleanup_sha256=sha(self.output / 'cleanup-receipt.json'), **config_audit(self.cfg, self.journal.data)))

    def report_budget(self, ledger, phase):
        """Aggregate publication/reporting is separate from financial mutation."""
        try:
            report = ledger.report(run_id=self.run_id)
        except Exception as error:
            dump(self.output / 'budget-report-error.json', dict(run_id=self.run_id, phase=phase,
                error=repr(error), **config_audit(self.cfg, self.journal.data)))
            return
        dump(self.output / 'budget-report.json', dict(report, run_id=self.run_id, phase=phase,
             **config_audit(self.cfg, self.journal.data)))

    def authorize_reconciliation(self, previous_digest):
        """Read-only authorization before cleanup; the later ledger CAS still binds races."""
        from ci_budget import BudgetLedger, settlement_digest
        ledger = BudgetLedger(self.s3, self.cfg['bucket'], self.cfg['account'], repository=REPOSITORY,
                              branch=BRANCH, budget_path=self.cfg.get('budget_path', DEFAULT_BUDGET))
        run = ledger.report(run_id=self.run_id)['runs'].get(self.run_id)
        if not run or not run.get('cleanup_verified') or not run.get('receipt', {}).get('cleanup_complete'):
            raise ValueError('reconciliation requires a completed authoritative ledger settlement before cleanup')
        data = self.journal.data or {}
        pending = data.get('usage_reconciliation')
        previous = pending.get('previous_settlement') if pending else data.get('settlement')
        if (not previous or previous['receipt'].get('cleanup_complete') is not True
                or previous['receipt'].get('usage_evidence', {}).get('accounting_basis') == 'observed-resources-v2'
                or settlement_digest(self.run_id, previous['usage_usd'], previous['receipt']) != previous_digest):
            raise ValueError('reconciliation requires the matching completed legacy journal settlement before cleanup')
        if pending:
            replacement = pending['replacement']
            receipt = replacement['receipt']
            if (pending['previous_settlement_digest'] != previous_digest
                    or receipt.get('cleanup_complete') is not True
                    or receipt.get('cleanup_evidence', {}).get('cleanup_complete') is not True
                    or receipt.get('usage_evidence', {}).get('accounting_basis') != 'observed-resources-v2'
                    or any(receipt.get(name) != expected for name, expected in (
                        ('account', self.cfg['account']), ('run_id', self.run_id),
                        ('repository', REPOSITORY), ('branch', BRANCH)))):
                raise ValueError('pending reconciliation identity or cleanup binding differs')
            replacement_digest = settlement_digest(self.run_id, replacement['usage_usd'], receipt)
            committed = any(item['settlement_digest'] == previous_digest
                and item['replacement_settlement_digest'] == replacement_digest
                for item in run.get('usage_reconciliation_history', []))
            if run.get('settlement_digest') != previous_digest and not committed:
                raise ValueError('pending reconciliation is not bound to the authoritative settlement history')
        elif run.get('settlement_digest') != previous_digest:
            raise ValueError('reconciliation requires the matching completed authoritative ledger settlement before cleanup')
        if self.active():
            raise ValueError('cost reconciliation is closed-run only; active instances require ordinary authorized recovery')
        return pending

    def settle(self, cleanup, *, reconcile_digest=None):
        data = self.journal.data or {}
        if not data.get('budget_reserved'):
            return
        from ci_budget import BudgetLedger, settlement_digest, usage_cost_usd
        ledger = BudgetLedger(self.s3, self.cfg['bucket'], self.cfg['account'], repository=REPOSITORY,
                              branch=BRANCH, budget_path=self.cfg.get('budget_path', DEFAULT_BUDGET))
        previous = data.get('settlement')
        if reconcile_digest is not None:
            if not re.fullmatch(r'[a-f0-9]{64}', reconcile_digest):
                raise ValueError('reconciliation requires the previous ledger settlement digest')
            if not cleanup.get('cleanup_complete'):
                raise ValueError('reconciliation requires verified complete cleanup')
            pending = data.get('usage_reconciliation')
            if pending:
                if pending['previous_settlement_digest'] != reconcile_digest:
                    raise ValueError('reconciliation digest differs from immutable pending correction')
                replacement = pending['replacement']
                result = ledger.reconcile_usage(self.run_id, replacement['usage_usd'],
                    replacement['receipt'], reconcile_digest)
                if data.get('settlement') != replacement:
                    data['settlement'] = replacement
                    self.journal.save()
                dump(self.output / 'settlement-receipt.json', replacement)
                self.report_budget(ledger, 'reconcile-replay')
                return result
            if (not previous or previous['receipt'].get('cleanup_complete') is not True
                    or previous['receipt'].get('usage_evidence', {}).get('accounting_basis') == 'observed-resources-v2'):
                raise ValueError('explicit reconciliation requires a completed legacy estimate')
            if settlement_digest(self.run_id, previous['usage_usd'], previous['receipt']) != reconcile_digest:
                raise ValueError('previous settlement digest does not match immutable journal estimate')
        if reconcile_digest is None and previous and previous['receipt'].get('cleanup_complete') is True:
            result = ledger.settle(self.run_id, previous['usage_usd'], previous['receipt'])
            dump(self.output / 'settlement-receipt.json', previous)
            self.report_budget(ledger, 'settle-replay')
            return result
        usage, evidence = Decimal(0), []
        for item in cleanup['instances']:
            if item.get('final_state') != 'terminated' or not item.get('launch_time'):
                continue
            candidate = next((candidate for candidate in data['candidates'] if candidate['region'] == item['region']
                              and candidate['instance_type'] == item['instance_type']), None)
            if candidate is None:
                raise ValueError('observed instance has no exact regional price proof')
            duration = parsed(item['verified_utc']) - parsed(item['launch_time'])
            with localcontext() as context:
                context.prec, context.rounding = 60, ROUND_CEILING
                elapsed = max(Decimal(0), Decimal(duration.days * 86400 + duration.seconds)
                              + Decimal(duration.microseconds) / Decimal(1000000))
                charged_rate = usd_ceiling(candidate['host_hourly_usd'])
                usage += usage_cost_usd(charged_rate, elapsed)
            evidence.append(dict(instance_id=item['instance_id'], elapsed_seconds=str(elapsed),
                                 host_hourly_usd=str(charged_rate), price=candidate['price']))
        s3_evidence = data.get('s3_accounting', {})
        if s3_evidence.get('accounting_basis') != 'observed-resources-v2':
            s3_evidence = dict(coverage='unavailable', estimate_usd=None,
                               unknown_components=['S3 inventory, requests, transfers and elapsed storage unavailable'])
        with localcontext() as context:
            context.prec, context.rounding = 60, ROUND_CEILING
            compute_usage = usd_ceiling(usage)
            usage = usd_ceiling(usage + Decimal(s3_evidence.get('estimate_usd') or '0'))
        receipt = dict(cleanup_complete=cleanup['cleanup_complete'], cleanup_evidence=cleanup,
            account=self.cfg['account'], run_id=self.run_id, repository=REPOSITORY, branch=BRANCH,
            **config_audit(self.cfg, data),
            usage_evidence=dict(accounting_basis='observed-resources-v2', coverage='partial',
                instances=evidence, compute_estimate_usd=str(compute_usage),
                s3=s3_evidence, billing=dict(status='unavailable', amount_usd=None),
                projections=s3_evidence.get('projections', dict(amount_usd=None,
                    s3_retention_days=self.cfg['cost_assumptions']['retention_days'],
                    basis='future retained-current-bytes storage only; excluded from usage_usd')),
                elapsed_seconds=str(max((Decimal(item['elapsed_seconds']) for item in evidence), default=Decimal(0))),
                components='observed EC2 Linux On-Demand, gp3, public IPv4 lifetime; partial observed S3 priced upper bounds; not final AWS bill'))
        settlement = json.loads(encoded(dict(usage_usd=str(usage), receipt=receipt)))
        if reconcile_digest is not None:
            data['usage_reconciliation'] = dict(previous_settlement_digest=reconcile_digest,
                previous_settlement=previous, replacement=settlement)
            self.journal.save()
            result = ledger.reconcile_usage(self.run_id, settlement['usage_usd'], settlement['receipt'], reconcile_digest)
            data['settlement'] = settlement
            self.journal.save()
        else:
            data['settlement'] = settlement
            self.journal.save()
            result = ledger.settle(self.run_id, settlement['usage_usd'], settlement['receipt'])
        dump(self.output / 'settlement-receipt.json', settlement)
        self.report_budget(ledger, 'settle')
        return result

    def run(self, admission, preflight):
        failure, cfg = None, None
        try:
            instances, candidate, plan = self.allocate(admission, preflight)
            cfg = self.configure(instances, candidate, plan)
            self.monitor(cfg)
        except BaseException as error:
            failure = error
        finally:
            # Paid compute must be gone before local downloads, archive checks,
            # timing analysis or billing reconciliation can hold up the runner.
            cleanup = self.cleanup()
            if not cleanup['cleanup_complete']:
                failure = failure or RuntimeError('exact-owned termination/auxiliary cleanup not verified')
            try:
                self.retrieve()
            except BaseException as error:
                failure = failure or error
            try:
                self.settle(cleanup)
            except BaseException as error:
                failure = failure or error
        if not failure and cfg is not None:
            self.validate_results(cfg)
        if failure:
            raise failure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('run', 'preflight', 'plan', 'cleanup'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--build', type=Path)
    parser.add_argument('--fixture', type=Path)
    parser.add_argument('--verification', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--budget-file', type=Path, default=DEFAULT_BUDGET)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--run-id')
    parser.add_argument('--expected-account', help='explicit configured owner for non-GitHub cleanup recovery')
    parser.add_argument('--reconcile-usage', action='store_true', help='explicitly correct a completed legacy estimate')
    parser.add_argument('--previous-settlement-digest', help='CAS digest from the existing ledger settlement')
    args = parser.parse_args()
    cfg, run_id, aws, campaign = None, None, None, None
    work_started = False
    try:
        cfg = load_config(args.config)
        cfg['budget_path'] = str(args.budget_file.absolute())
        if args.reconcile_usage != bool(args.previous_settlement_digest):
            raise ValueError('--reconcile-usage and --previous-settlement-digest are required together')
        if args.reconcile_usage and (args.action != 'cleanup' or not args.run_id):
            raise ValueError('reconciliation requires cleanup with an explicit old --run-id')
        if args.previous_settlement_digest and not re.fullmatch(r'[a-f0-9]{64}', args.previous_settlement_digest):
            raise ValueError('invalid previous settlement digest')
        if args.expected_account is not None and (args.action != 'cleanup' or args.expected_account != cfg['account']):
            raise ValueError('expected cleanup account must match configured owner')
        if args.action == 'plan':
            if not args.offline or not args.run_id:
                raise ValueError('offline illustration requires plan --offline --run-id')
            ownership(args.run_id)
            run_id = args.run_id
        else:
            if args.offline or (args.run_id and args.action != 'cleanup'):
                raise ValueError('live allocation identity derives only from trusted GitHub environment')
            if args.action == 'cleanup' and args.run_id:
                ownership(args.run_id)
                if os.environ.get('GITHUB_ACTIONS') == 'true':
                    trusted_run()
                elif args.expected_account != cfg['account']:
                    raise ValueError('local cleanup requires explicit --expected-account matching configured owner')
                run_id = args.run_id
            else:
                run_id = trusted_run()
        if args.action != 'cleanup':
            if not all((args.build, args.fixture, args.verification)):
                raise ValueError('build, fixture and local verification are required before admission')
            admission = admit_inputs(args.build, args.fixture, args.verification, args.output)
            admission.update(config_audit(cfg))
            dump(args.output / 'input-admission.json', admission)
        if args.action == 'plan':
            policy = load_budget(args.budget_file)
            dump(args.output / 'offline-plan.json', dict(run_id=run_id, config=cfg, admission=admission,
                 budget_policy=policy, live_readiness_required=True, aws_calls=False, allocation_authorized=False,
                 **config_audit(cfg, {'budget_policy': policy})))
            return 0
        aws = AWS()
        if args.action == 'cleanup':
            identity = aws.client('sts', cfg['bucket_region']).get_caller_identity()
            if identity['Account'] != cfg['account']:
                raise ValueError('cleanup caller account differs from configured owner')
            campaign = Campaign(aws, cfg, run_id, args.output, cleanup_only=True)
            if args.run_id and (not campaign.journal.data or campaign.journal_error):
                raise ValueError('explicit old-run recovery requires its immutable same-account owned S3 journal')
            if args.reconcile_usage:
                pending = campaign.authorize_reconciliation(args.previous_settlement_digest)
                if pending:
                    cleanup = pending['replacement']['receipt']['cleanup_evidence']
                    campaign.settle(cleanup, reconcile_digest=args.previous_settlement_digest)
                    return 0
            cleanup = campaign.cleanup()
            try:
                campaign.retrieve()
            finally:
                campaign.settle(cleanup, reconcile_digest=args.previous_settlement_digest)
            return 0 if cleanup['cleanup_complete'] else 1
        preflight = live_preflight(aws, cfg)
        dump(args.output / 'live-preflight.json', preflight)
        if args.action == 'preflight':
            return 0
        campaign = Campaign(aws, cfg, run_id, args.output)
        def interrupted(number, frame):
            raise InterruptedError('CI cancellation signal ' + str(number))
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        work_started = True
        campaign.run(admission, preflight)
        return 0
    except BaseException as error:
        if args.action == 'run' and run_id and not work_started:
            # A restart may fail source/readiness admission while the prior
            # process still owns hosts. Cleanup needs no build or watchdog gate.
            try:
                aws = aws or AWS()
                identity = aws.client('sts', cfg['bucket_region']).get_caller_identity()
                if identity.get('Account') != cfg['account']:
                    raise ValueError('emergency cleanup account mismatch')
                campaign = Campaign(aws, cfg, run_id, args.output, cleanup_only=True)
                cleanup = campaign.cleanup()
                try:
                    campaign.retrieve()
                finally:
                    campaign.settle(cleanup)
            except BaseException as cleanup_error:
                dump(args.output / 'emergency-cleanup-failure.json', dict(error=repr(cleanup_error), at=utc(),
                     **config_audit(cfg, campaign.journal.data if campaign else None)))
        dump(args.output / 'operator-failure.json', dict(error=repr(error), at=utc(),
             **config_audit(cfg, campaign.journal.data if campaign else None)))
        print('ci_campaign: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
