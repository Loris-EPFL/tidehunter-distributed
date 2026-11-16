"""Explicit, bounded manual GCP campaign; importing this module never allocates.

Compute operations use an injected asynchronous command callback: MCP is
preferred; a CLI fallback must preserve argv and exit status. Storage uses
an injected ObjectStore-compatible transport (or DelegatedArtifacts). The persisted
parent lease owns one admitted fleet with a shared absolute expiry.
Cleanup is recoverable without GitHub. Compute describe/delete is necessarily a
TOCTOU: the API does not provide an ID precondition for name-based deletion.
"""
import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import uuid

import gcp_budget
import gcp_campaign as campaign
import gcp_image
import gcp_image_receipt
import gcp_lease
import gcp_manual_preflight
import gcp_qualify_bootstrap as qualify
import gcp_runtime
import prepare
import workflow_input
from cloud_campaign import NVMEOF_PATH_SELECTIONS
from gcp_api import ApiError
from gcp_manual_storage import ACCOUNT, PROJECT, CredentialUnavailable, DelegatedArtifacts, TRANSPORT

PREPARATION_SECONDS = 900
PAIR_SECONDS = 1200
# The pair child has its own bound. Leave a separate five minutes inside the
# controller wait for child exit, guest cleanup, status polling and evidence.
PAIR_CLEANUP_SECONDS = 300
WORKER_READY_SECONDS = 180
FLEET_SECONDS = 21600
EXPORT_RESERVE_SECONDS = 900
# Export reserve is inside the campaign; this margin leaves time after its
# deadline for the terminal receipt and cleanup before VM expiry.
CAMPAIGN_MARGIN_SECONDS = 720  # 600 s bounded Compute cleanup + 120 s terminal receipt.
CEILING_CENTS = 14500
# Rounded-up compute for six six-hour guests at $3.287109973 per host-hour.
# Iowa on-demand: https://cloud.google.com/products/compute/pricing/storage-optimized
Z4M_HIGHMEM_16_USD_PER_HOUR = 3.287109973
Z4M_HIGHMEM_32_USD_PER_HOUR = 6.574219945
MACHINE_RATE_NANODOLLARS = {'z4m-highmem-16': 3287109973, 'z4m-highmem-32': 6574219945}
COMPUTE_CENTS = (3287109973 * 6 * FLEET_SECONDS * 100 + 3600 * 10**9 - 1) // (3600 * 10**9)
COMPACT_COMPUTE_CENTS = (3287109973 * 2 * FLEET_SECONDS * 100 + 3600 * 10**9 - 1) // (3600 * 10**9)
COMPACT_CEILING_CENTS = (COMPACT_COMPUTE_CENTS * CEILING_CENTS + COMPUTE_CENTS - 1) // COMPUTE_CENTS
# This spec keeps the previously reviewed live and sealed comparison.
REVIEWED_SPEC = {
    'schema_version': 1, 'suite': 'kv', 'mode': 'standard-with-live-nvmeof', 'seed': 1775,
    'repeats': 1, 'value_sizes': [64, 256, 1024, 8192, 65536],
    'backends': ['native', 'tidehunter_tcp', 'tidehunter_verbs_native',
                 'tidehunter_nvme_rdma', 'valkey_tcp', 'valkey_rdma',
                 'tikv', 'cassandra', 'foundationdb', 'kvrocks', 'garnet'], 'keys': 4096,
    'spdk_comparison': True, 'native_durability_cohort': True,
    'valkey_reproduction_profiles': ['published-as-written', 'matched-ram'],
    'max_cases': 2000, 'offered_rates': [1000, 10000, 50000],
    'offered_seconds': 8, 'case_deadline_seconds': 600, 'hard_deadline_seconds': FLEET_SECONDS,
    'export_reserve_seconds': 900,
    'diagnostics': {'write_profile_limit': 256, 'read_profile_limit': 256, 'write_timeline_limit': 8192,
                    'valkey_profile_limit': 256},
}

REVIEWED_CONTRACT_SPEC = {
    **{key: copy.deepcopy(value) for key, value in REVIEWED_SPEC.items()
       if key != 'valkey_reproduction_profiles'},
    'backends': ['native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma',
                 'valkey_tcp', 'valkey_rdma', 'kvrocks'],
    'repeats': 2, 'spdk_comparison': False, 'shapes': 'contract_core',
    'arms': {'native': ['value_wal_barrier', 'page_cache'],
             'tidehunter_verbs_native': ['synchronous', 'native'],
             'tidehunter_nvme_rdma': ['synchronous', 'native'],
             'valkey_tcp': ['synced'],
             'valkey_rdma': ['synced', 'os_buffered', 'periodic_sync'],
             'kvrocks': ['synced', 'page_cache']},
}

# Full focused estimate: 4.8892 fleet hours and $123.09 with allowance.
# It is a planning estimate, not a bound; the full $145 quote is reserved.
# Matrix digests bind the exact reviewed job order and content.
FOCUSED_MATRIX_SHA256 = {
    'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
    'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
    'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
    'floors': '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429',
    'stages': '646207fe6727aa37367b727759a4d46005ebd56a59f29ec49b13a690de24a724',
    'unified': 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506',
    'append_floors': '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40',
    'fair_native': '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9',
    'parity': 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6',
    'parity_compact': '4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa',
    'fair_five_owner': '479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946',
    'remote_rw_compact': 'b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620',
    'fabric_ab_compact': '8e12ba4352dc461680565b3bfaef5db705e702e03bce986323cc1e31ae9bde6e',
    'leased_ab_compact': '52029dc2047cecca4903b4240d7a84d8dcb402bfbfddcad827d605405f15c2dc',
    'nvmeof_path_compact': '8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036',
    'nvmeof_path_core': 'f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55',
    'nvmeof_path_fix': 'aa94a3842d196bd9cdb486b0c746e9939455b25d4d152e8127c1aba51f9ef606',
    'nvmeof_first_put': 'bc17406e8712b9ad7f49662554bcca316cff7c87774216c39c5cc42aebe1efdc',
    'nvmeof_contracts_crossing': '7cf192750aa556071a73aaf3c1f19ce8f7b73c4baa51f3abfc46816896620bfa',
    'nvmeof_contracts_crossing2': '8dde34953df300c798b0a5605abf393d310bd6e321c3568c861cfdb27caab295',
    'nvmeof_parallel_writes': '64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4',
    'nvmeof_single_caller': '294b50957b21d6acafadcf1b682c71d1b424524a144ecd6c2c18e2b1cc40fd4e',
    'nvmeof_f6': '1186674f1706550f0faaac6d7852dcee711b43dba8c87b22be4dd071d768e8a7',
    'nvmeof_f6b': 'f5e680e6959f3a74369bae97c21354b6dc2911f0b6e15f048c6e7172e4c823de',
    'nvmeof_f8': '30e7121a926003ebe18e691425db063c4093a942efdf5528913003720b9d5e1a',
    'nvmeof_f7_highmem32': '25ef27bc334edc0d07a84061427893063e162f16f8f1a986ea99d758ca2c05dd',
    'nvmeof_reads_geometry': 'c27b1598f34949d642d3cc518985a2f92f5c388e77e2742da9ce780ce8b41aea',
    'nvmeof_reads_geometry_rest': 'b5db99b28d0faf8386e5b885e25c55d50b74f732e9b220ef552bd6c90a761935',
    'nvmeof_reads_geometry_rest2': 'c80e079b256fad0c765dc66fec387cdb7bac160c90aa0747ac184347cb5bbed4',
    'nvmeof_contracts': 'cad4e58cc1e01a4977f7b7e3e0c87ed6ff00cfb15d498959472a511a8bab0208',
    'valkey_rdma_reads': 'fbbecb9ecba897a624c388dc7ab51fe080fd2ce3363edfebf4b6069c5454c0a2',
    'nvmeof_f9_tools': '68915419fabaa9f74f52c3f4f29beedbf1aa64550c81109682bc0db1eada682d',
}
REVIEWED_SPECS = {
    selection: {
        'schema_version': 1, 'suite': 'kv', 'mode': 'focused', 'selection': selection,
        'matrix_sha256': digest, 'job_count': count, 'seed': 1775, 'repeats': 3,
        'backends': (['native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma']
                     if selection in NVMEOF_PATH_SELECTIONS else
                     ['tidehunter_verbs_native', 'tidehunter_nvme_rdma']
                     if selection == 'leased_ab_compact' else
                     ['tidehunter_tcp', 'tidehunter_tcp_trusted', 'tidehunter_verbs_native',
                      'tidehunter_nvme_rdma', 'tidehunter_nvme_tcp']
                     if selection == 'fabric_ab_compact' else
                     ['native', 'tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma', 'tidehunter_verbs']
                     if selection in ('stages', 'unified') else
                     ['native', 'tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma']),
        'keys': 4096, 'spdk_comparison': selection not in ('leased_ab_compact', *NVMEOF_PATH_SELECTIONS), 'direct_control_arm': True,
        'max_cases': count if selection in NVMEOF_PATH_SELECTIONS else 72 if selection == 'leased_ab_compact' else 80 if selection == 'fabric_ab_compact' else 342 if selection == 'remote_rw_compact' else 333,
        'case_deadline_seconds': 600, 'hard_deadline_seconds': FLEET_SECONDS,
        'export_reserve_seconds': EXPORT_RESERVE_SECONDS,
        'diagnostics': {'write_profile_limit': 256, 'read_profile_limit': 256, 'write_timeline_limit': 8192},
        **({'owner_hosts': 1} if selection in ('parity_compact', 'remote_rw_compact', 'fabric_ab_compact', 'leased_ab_compact', *NVMEOF_PATH_SELECTIONS) else {}),
    }
    for selection, digest, count in (
        ('recommended', FOCUSED_MATRIX_SHA256['recommended'], 279),
        ('full', FOCUSED_MATRIX_SHA256['full'], 333),
        ('device', FOCUSED_MATRIX_SHA256['device'], 120),
        ('floors', FOCUSED_MATRIX_SHA256['floors'], 69),
        ('stages', FOCUSED_MATRIX_SHA256['stages'], 183),
        ('unified', FOCUSED_MATRIX_SHA256['unified'], 270),
        ('append_floors', FOCUSED_MATRIX_SHA256['append_floors'], 48),
        ('fair_native', FOCUSED_MATRIX_SHA256['fair_native'], 168),
        ('parity', FOCUSED_MATRIX_SHA256['parity'], 216),
        ('parity_compact', FOCUSED_MATRIX_SHA256['parity_compact'], 177),
        ('fair_five_owner', FOCUSED_MATRIX_SHA256['fair_five_owner'], 39),
        ('remote_rw_compact', FOCUSED_MATRIX_SHA256['remote_rw_compact'], 342),
        ('fabric_ab_compact', FOCUSED_MATRIX_SHA256['fabric_ab_compact'], 80),
        ('leased_ab_compact', FOCUSED_MATRIX_SHA256['leased_ab_compact'], 72),
        ('nvmeof_path_compact', FOCUSED_MATRIX_SHA256['nvmeof_path_compact'], 157),
        ('nvmeof_path_core', FOCUSED_MATRIX_SHA256['nvmeof_path_core'], 66),
        ('nvmeof_path_fix', FOCUSED_MATRIX_SHA256['nvmeof_path_fix'], 37),
        ('nvmeof_first_put', FOCUSED_MATRIX_SHA256['nvmeof_first_put'], 12),
        ('nvmeof_contracts', FOCUSED_MATRIX_SHA256['nvmeof_contracts'], 34),
        ('nvmeof_contracts_crossing', FOCUSED_MATRIX_SHA256['nvmeof_contracts_crossing'], 35),
        ('nvmeof_contracts_crossing2', FOCUSED_MATRIX_SHA256['nvmeof_contracts_crossing2'], 36),
        ('nvmeof_parallel_writes', FOCUSED_MATRIX_SHA256['nvmeof_parallel_writes'], 44),
        ('nvmeof_single_caller', FOCUSED_MATRIX_SHA256['nvmeof_single_caller'], 20),
        ('nvmeof_f6', FOCUSED_MATRIX_SHA256['nvmeof_f6'], 42),
        ('nvmeof_f6b', FOCUSED_MATRIX_SHA256['nvmeof_f6b'], 58),
        ('nvmeof_f8', FOCUSED_MATRIX_SHA256['nvmeof_f8'], 36),
        ('nvmeof_f7_highmem32', FOCUSED_MATRIX_SHA256['nvmeof_f7_highmem32'], 8),
        ('nvmeof_reads_geometry', FOCUSED_MATRIX_SHA256['nvmeof_reads_geometry'], 74),
        ('nvmeof_reads_geometry_rest', FOCUSED_MATRIX_SHA256['nvmeof_reads_geometry_rest'], 20),
        ('nvmeof_reads_geometry_rest2', FOCUSED_MATRIX_SHA256['nvmeof_reads_geometry_rest2'], 18),
    )
}
REVIEWED_SPECS['valkey_rdma_reads'] = {
    **copy.deepcopy(REVIEWED_SPECS['nvmeof_parallel_writes']),
    'selection': 'valkey_rdma_reads', 'job_count': 22, 'max_cases': 22,
    'matrix_sha256': FOCUSED_MATRIX_SHA256['valkey_rdma_reads'],
    'backends': ['valkey_rdma', 'tidehunter_nvme_rdma'], 'owner_hosts': 1,
}
REVIEWED_SPECS['nvmeof_f6']['machine_type'] = 'z4m-highmem-16'
REVIEWED_SPECS['nvmeof_f6b']['machine_type'] = 'z4m-highmem-16'
REVIEWED_SPECS['nvmeof_f8']['machine_type'] = 'z4m-highmem-16'
REVIEWED_SPECS['nvmeof_f9_tools'] = {
    **copy.deepcopy(REVIEWED_SPECS['nvmeof_f8']),
    'selection': 'nvmeof_f9_tools', 'job_count': 34, 'max_cases': 34, 'repeats': 1,
    'matrix_sha256': FOCUSED_MATRIX_SHA256['nvmeof_f9_tools'],
    'backends': ['native', 'tidehunter_nvme_rdma'], 'owner_hosts': 1,
}
REVIEWED_SPECS['nvmeof_f7_highmem32'].update(
    machine_type='z4m-highmem-32', backends=['tidehunter_nvme_rdma'])

# One immutable matrix and one fleet size per M3 selection.
M3_MATRIX_BINDINGS = {
    1: ("nvmeof_m3_t1", 8, "df343feef5fc4568d44e98f28ad1486f5882e559ac23d935ea6a4ac4b5a5ac21"),
    2: ("nvmeof_m3_t2_2", 8, "115bc93da0f8bca91cd1c5c09214e9ef5c4d91153f940218f7fef0cf0fff548a"),
    5: ("nvmeof_m3_t2_5", 8, "9ec5b0dcf21131e3e4c1296862561a2660389b30aed0cbac18f548cc18cbf094"),
}
for owners, (selection, count, digest) in M3_MATRIX_BINDINGS.items():
    REVIEWED_SPECS[selection] = {
        **copy.deepcopy(REVIEWED_SPECS["nvmeof_f8"]),
        "selection": selection, "owner_hosts": owners, "job_count": count, "max_cases": count,
        "matrix_sha256": digest, "backends": ["tidehunter_nvme_rdma"],
        "case_deadline_seconds": max(job["measurement_sizing"]["case_deadline_seconds"]
                                     for job in workflow_input.cloud_campaign.make_focused_matrix(selection)),
    }
    FOCUSED_MATRIX_SHA256[selection] = digest


def m3_spec(owner_hosts):
    if type(owner_hosts) is not int or owner_hosts not in M3_MATRIX_BINDINGS:
        raise ValueError("M3 requires one, two or five owner VMs")
    selection = M3_MATRIX_BINDINGS[owner_hosts][0]
    return copy.deepcopy(REVIEWED_SPECS[selection])


def _utc(seconds):
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).isoformat(timespec='seconds')


def _run_id():
    return 'bench-' + uuid.uuid4().hex[:16]


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _json(value):
    return campaign.json_bytes(value)


async def _blocking(function, *args, **kwargs):
    """Join in-flight synchronous I/O before propagating cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except BaseException as failure:
            raise BaseExceptionGroup('cancelled while artifact I/O failed', [cancelled, failure])
        raise


async def _joined_waiters(waiters):
    """Fail fast, but join every waiter and its in-flight artifact I/O first."""
    tasks = [asyncio.create_task(waiter) for waiter in waiters]
    try:
        return await asyncio.gather(*tasks)
    except BaseException as first:
        for task in tasks:
            if not task.done():
                task.cancel()
        joined = asyncio.gather(*tasks, return_exceptions=True)
        while not joined.done():
            try:
                await asyncio.shield(joined)
            except asyncio.CancelledError:
                # A repeated caller cancellation must not abandon artifact I/O.
                continue
        failures = []
        for result in joined.result():
            if not isinstance(result, BaseException) or result is first:
                continue
            if isinstance(result, BaseExceptionGroup):
                _, result = result.split(asyncio.CancelledError)
            if result is not None and not isinstance(result, asyncio.CancelledError):
                failures.append(result)
        if failures:
            raise BaseExceptionGroup('phase waiter and peer failures', [first, *failures])
        raise


class _StoreApi:
    def __init__(self, store, bucket):
        self.store, self.bucket = store, bucket

    def objects(self, bucket):
        if bucket != self.bucket:
            raise ValueError('unexpected artifact bucket')
        return self.store


class _ChildFile(campaign.StateFile):
    """Make each child save durable as part of the single parent snapshot."""
    def __init__(self, parent_path, parent, name):
        self.parent_path, self.parent, self.name = parent_path, parent, name

    def persist(self, state):
        self.parent['children'][self.name] = state
        campaign.save(self.parent_path, self.parent)


def _record(path, state, phase):
    state['phase'] = phase
    campaign.save(path, state)


def reservation_seconds(spec):
    hours = spec.get('reservation_hours', 6)
    if type(hours) is not int or not 2 <= hours <= 6:
        raise ValueError('reservation_hours must be an integer from 2 to 6')
    return hours * 3600


def reservation_quote(spec, machine):
    duration = reservation_seconds(spec)
    hosts = spec.get('owner_hosts', 5) + 1
    rate = MACHINE_RATE_NANODOLLARS[machine]
    compute = (rate * hosts * duration * 100 + 3600 * 10**9 - 1) // (3600 * 10**9)
    ceiling = (compute * CEILING_CENTS + COMPUTE_CENTS - 1) // COMPUTE_CENTS
    return duration, compute, ceiling


def reviewed_window_spec(spec):
    """Normalize only the reviewed reservation window, not the workload."""
    if not isinstance(spec, dict):
        return spec
    seconds = reservation_seconds(spec)
    if 'reservation_hours' not in spec:
        return spec
    if spec.get('hard_deadline_seconds') not in (FLEET_SECONDS, seconds):
        raise ValueError('hard deadline differs from reservation window')
    return {**{key: value for key, value in spec.items() if key != 'reservation_hours'},
            'hard_deadline_seconds': FLEET_SECONDS}


def _reviewed_jobs(spec):
    spec = reviewed_window_spec(spec)
    if isinstance(spec, dict) and 'machine_type' in spec:
        if spec['machine_type'] not in MACHINE_RATE_NANODOLLARS:
            raise ValueError('unreviewed machine type')
        # A shape pinned by the reviewed selection is part of its exact contract.
        if 'machine_type' not in REVIEWED_SPECS.get(spec.get('selection'), {}):
            spec = {key: value for key, value in spec.items() if key != 'machine_type'}
    if isinstance(spec, dict) and spec.get("suite") in ("sui", "both"):
        import manual_sui
        if not manual_sui.reviewed_sui_spec(manual_sui.reviewed_part(spec)):
            raise ValueError("an exact reviewed focused specification is required")
        return [] if spec["suite"] == "sui" else _reviewed_jobs(manual_sui.kv_spec(manual_sui.reviewed_part(spec)))
    if isinstance(spec, dict) and any(_json(spec) == _json(reviewed)
                                     for reviewed in (REVIEWED_SPEC, REVIEWED_CONTRACT_SPEC)):
        return workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
    reviewed = REVIEWED_SPECS.get(spec.get('selection')) if isinstance(spec, dict) else None
    if isinstance(spec, dict) and spec.get('selection') in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5'):
        reviewed = m3_spec(spec.get('owner_hosts'))
    if reviewed is None or _json(spec) != _json(reviewed):
        raise ValueError('an exact reviewed focused specification is required')
    jobs = workflow_input.cloud_campaign.make_focused_matrix(spec['selection'])
    if spec['selection'] in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5'):
        jobs = [job for job in jobs if workflow_input.cloud_campaign.job_owner_hosts(job) == spec['owner_hosts']]
    if (len(jobs) != spec['job_count'] or _digest(_json(jobs)) != spec['matrix_sha256']):
        raise ValueError('focused matrix differs from the reviewed specification')
    owners = spec.get('owner_hosts', 5)
    if (type(owners) is not int or owners not in gcp_runtime.owner_host_counts()
            or (spec['selection'] not in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5')
                and workflow_input.cloud_campaign.selection_owner_hosts(spec['selection']) != owners)
            or any(workflow_input.cloud_campaign.job_owner_hosts(job) > owners for job in jobs)):
        raise ValueError('focused selection and reviewed owner layout disagree')
    return jobs


def _check_config(config, spec, surcharge_cents):
    if (config.get('project_id') != 'mysten-research'
            or config.get('bucket') != 'mysten-research-bench-artifacts'
            or config.get('service_account') != campaign.GUEST_SERVICE_ACCOUNT
            or config.get('machine_type', 'z4m-highmem-16') not in MACHINE_RATE_NANODOLLARS
            or config.get('max_distance') != 3
            or len(config.get('candidates', [])) != 1
            or config['candidates'][0].get('zone') != 'us-central1-a'):
        raise ValueError('manual campaign requires the reviewed single zonal configuration')
    _reviewed_jobs(spec)
    machine = config.get('machine_type', 'z4m-highmem-16')
    if machine != spec.get('machine_type', 'z4m-highmem-16'):
        raise ValueError('machine type differs from reviewed specification')
    _, compute, ceiling = reservation_quote(spec, machine)
    if type(surcharge_cents) is not int or not 0 <= surcharge_cents <= ceiling - compute:
        raise ValueError('non-compute quote exceeds the selected fleet ceiling')


def _check_snapshot(source, revision):
    """Compare the full sealed source closure with the actual working tree."""
    package = prepare.cloud_package
    root = prepare.ROOT
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('full source revision required')
    head = package.command(['git', 'rev-parse', 'HEAD'], cwd=root)
    branch = package.command(['git', 'branch', '--show-current'], cwd=root)
    dirty = bool(package.command(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=root))
    required = ('manual_campaign.py', 'gcp_manual_compute.py', 'gcp_manual_image.py',
                'gcp_manual_storage.py', 'gcp_manual_preflight.py', 'gcp_campaign.py',
                'gcp_runtime.py', 'gcp_lease.py', 'gcp_qualify_bootstrap.py', 'gcp_live_qualification.py',
                'qualify_observer_matrix.py')
    names = {'experiments/gcp-rdma/' + name for name in required}
    if (source.get('revision') != revision or head != revision or source.get('branch') != branch
            or type(source.get('dirty')) is not bool or source['dirty'] != dirty
            or not names <= set(source.get('sha256', {}))
            or source['sha256'] != package.inventory(root)):
        raise ValueError('sealed source snapshot differs from the executing checkout')


def _admit_package(package_dir, revision, spec):
    """Admit a truthful clean or dirty snapshot without weakening CI admission."""
    package_dir = Path(package_dir).resolve(strict=True)
    package = prepare.cloud_package
    receipt = json.loads((package_dir / 'package-receipt.json').read_text())
    root = package_dir / 'package'
    verified = package.verify_directory(root)
    import manual_sui
    focused = manual_sui.focused_qualification(spec)
    expected_profile = 'focused_valkey' if spec.get('selection') == 'valkey_rdma_reads' else 'focused'
    if focused and verified.get('build_profile') != expected_profile:
        raise ValueError('focused campaign requires its exact package profile')
    if not focused and verified.get('build_profile') is not None:
        raise ValueError('full campaign cannot use a focused package')
    gcp_image.admit_spdk_scope(spec, verified.get('spdk'))
    source = package.admit_source(root)
    _check_snapshot(source, revision)
    fields = ('source_sha256', 'binary_sha256', 'binary_blake2b512', 'manifest_sha256', 'abi')
    if (receipt.get('source_revision') != revision
            or any(receipt.get(field) != verified[field] for field in fields)
            or receipt.get('nvmet_fix') != verified.get('nvmet_fix')):
        raise ValueError('package receipt does not bind the source, binary and ABI')
    archive = root / 'bundle.tar.gz'
    if archive.stat().st_size > 512 * 1024 * 1024 or campaign.file_sha256(archive) != receipt.get('sha256'):
        raise ValueError('runtime archive checksum or size mismatch')
    archive_identity = package.verify_archive(archive, receipt['sha256'])
    if any(archive_identity.get(field) != verified[field] for field in fields):
        raise ValueError('runtime archive differs from qualified package')
    if archive_identity.get('build_profile') != verified.get('build_profile'):
        raise ValueError('runtime archive build profile differs from qualified package')
    proof = receipt.get('qualification', {})
    if (proof.get('engine_sha') != revision or proof.get('binary_sha256') != verified['binary_sha256']
            or proof.get('binary_blake2b512') != verified['binary_blake2b512']):
        raise ValueError('qualification does not bind the sealed executable')
    manifest = json.loads((root / 'manifest.json').read_text())
    if (spec.get('selection') == 'nvmeof_f9_tools'
            and 'runtime/bin/admission-options' not in manifest.get('files', {})):
        raise ValueError('F9 tools require sealed runtime/bin/admission-options')
    if focused and expected_profile != 'focused_valkey' and (
            'valkey-rdma' in manifest.get('features', [])
            or any(name.startswith('runtime/bin/valkey-') or name == 'runtime/valkey-rdma.json'
                   for name in manifest.get('files', {}))):
        raise ValueError('focused package must not contain Valkey binaries')
    if expected_profile == 'focused_valkey':
        required = {'runtime/bin/valkey-server', 'runtime/bin/valkey-cli',
                    'runtime/bin/valkey-benchmark', 'runtime/valkey-rdma.json'}
        if (manifest.get('features') != package.FOCUSED_VALKEY_FEATURES
                or not required <= set(manifest.get('files', {}))):
            raise ValueError('Valkey read package requires exact features and sealed runtime')
    prepare.admit_selected_features(spec, manifest)
    if 'valkey_rdma' in spec['backends'] and 'valkey-rdma' not in manifest.get('features', []):
        raise ValueError('the selected Valkey RDMA comparison requires its sealed runtime')
    prepare.ci_prepare.admit_qualification(package_dir / 'qualification', proof,
                                           **({'focused': True} if focused else {}))
    _check_snapshot(source, revision)
    if spec.get('suite') in ('sui', 'both'):
        manual_sui.admit_sui_package(package_dir, receipt)
    receipt['archive_path'] = str(archive.resolve())
    return receipt, source


def _qualify_observer(package_dir, spec):
    """Exercise the admitted package's own matrix and observer without cloud access."""
    if spec.get("suite") == "sui":
        return {"skipped": "sui suite has no KV matrix"}
    script = Path(package_dir).resolve() / 'package/source/experiments/gcp-rdma/qualify_observer_matrix.py'
    result = subprocess.run([sys.executable, '-I', '-B', str(script)], input=json.dumps(spec),
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise ValueError('frozen observer matrix qualification failed: ' + result.stderr.strip())
    proof = json.loads(result.stdout)
    jobs = (workflow_input.cloud_campaign.make_focused_matrix(spec['selection'])
            if spec.get('mode') == 'focused'
            else workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k not in ('suite', 'reservation_hours')}))
    if spec.get('selection') in workflow_input.cloud_campaign.M3_SELECTIONS:
        jobs = [job for job in jobs if workflow_input.cloud_campaign.job_owner_hosts(job) == spec['owner_hosts']]
    digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if proof.get('planned_cases') != len(jobs) or proof.get('matrix_sha256') != digest:
        raise ValueError('frozen observer qualified a different generated matrix')
    return proof



def _put_json(store, key, value):
    raw = _json(value)
    meta = store.put(key, raw, content_type='application/json', if_generation_match=0)
    return {'object': key, 'generation': str(meta['generation']), 'sha256': _digest(raw)}


def _stage(store, bucket, state, receipt, wheels=None, *, qualification=False, spec=None, fixture=None):
    """Stage the same locally admitted source and archive for each child."""
    prefix = 'runs/' + state['run_id'] + '/'
    package = dict(receipt)
    package['object_key'] = prefix + 'package.tar'
    package['generation'] = campaign.upload_file(store, package['archive_path'], package['sha256'], package['object_key'])
    if spec is not None and spec.get('suite') in ('sui', 'both'):
        import manual_sui
        staged = manual_sui.stage_sui(store, prefix, receipt, fixture)
        package.update({k: staged[k] for k in ('sui_object_key', 'sui_generation')})
        spec.update({k: staged[k] for k in ('sui_fixture', 'sui_verification')})
    _put_json(store, prefix + 'package.json', package)
    # The qualification bootstrap only uses these identity fields. Normal fleet
    # runtime is written later, once all numeric IDs and addresses are admitted.
    if qualification:
        runtime = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
                   'deadline_utc': state['deadline'], 'bucket': bucket, 'package': package}
        _put_json(store, prefix + 'runtime.json', runtime)
    if wheels is not None:
        entries = []
        for item in wheels:
            if set(item) != {'path', 'sha256'}:
                raise ValueError('wheel requires path and SHA-256')
            path = Path(item['path'])
            if not re.fullmatch(r'(?i:pyyaml)-6\.0\.2-cp3[0-9]+-cp3[0-9]+-manylinux[0-9_a-z.]+_x86_64\.whl', path.name):
                raise ValueError('invalid preparation wheel name')
            key = prefix + 'preparation/' + path.name
            generation = campaign.upload_file(store, path, item['sha256'], key)
            entries.append({'path': path.name, 'sha256': item['sha256'], 'object_key': key, 'generation': generation})
        if not 1 <= len(entries) <= 8 or len({e['path'] for e in entries}) != len(entries):
            raise ValueError('one to eight unique preparation wheels required')
        prep = _put_json(store, prefix + 'preparation.json', {'wheels': entries})
        return package, prep['sha256']
    return package, None


def _gate(bucket, state):
    """A VM executes its immutable, identity-bound script only after admission."""
    data = json.dumps({'bucket': bucket, 'run_id': state['run_id'], 'source': state['source_sha256'], 'deadline': state['deadline']}, sort_keys=True)
    return '''#!/bin/bash
set -euo pipefail
exec /usr/bin/python3 - <<'PY'
import datetime, hashlib, json, subprocess, time, urllib.error, urllib.parse, urllib.request
c = json.loads(%r)
end = datetime.datetime.fromisoformat(c['deadline'].replace('Z','+00:00')).timestamp()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def meta(path):
    req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path, headers={'Metadata-Flavor':'Google'})
    with opener.open(req, timeout=5) as r:
        if r.headers.get('Metadata-Flavor') != 'Google': raise ValueError('metadata flavor')
        return r.read(16384).decode().strip()
if meta('instance/attributes/bench-run') != c['run_id'] or meta('instance/attributes/bench-source') != c['source'] or meta('instance/attributes/bench-expiry') != c['deadline']: raise ValueError('gate metadata mismatch')
name, instance = meta('instance/name'), meta('instance/id')
key = 'runs/' + c['run_id'] + '/qualification/' + name + '/gate.json'
url = 'https://storage.googleapis.com/storage/v1/b/' + urllib.parse.quote(c['bucket'], safe='') + '/o/' + urllib.parse.quote(key, safe='') + '?alt=media'
while time.time() < end:
    token = json.loads(meta('instance/service-accounts/default/token'))['access_token']
    try:
        with opener.open(urllib.request.Request(url, headers={'Authorization':'Bearer ' + token}), timeout=15) as r: raw = r.read(1048577)
        if len(raw) > 1048576: raise ValueError('oversized gate')
        gate = json.loads(raw)
        if gate.get('run_id') != c['run_id'] or gate.get('source_sha256') != c['source'] or gate.get('instance_id') != instance or gate.get('deadline_utc') != c['deadline']: raise ValueError('gate identity mismatch')
        script = gate['script'].encode()
        if hashlib.sha256(script).hexdigest() != gate['sha256']: raise ValueError('gate checksum mismatch')
        remaining = int(end - time.time())
        if remaining <= 0: raise TimeoutError('gate expired')
        subprocess.run(['/bin/bash'], input=script, check=True, timeout=remaining)
        break
    except urllib.error.HTTPError as e:
        if e.code != 404: raise
        time.sleep(1)
else: raise TimeoutError('gate admission missing')
PY
''' % data


async def _wait_status(store, state, name, manifest, *, filename='status.json', host=None):
    deadline = campaign.parse_time(state['deadline']).timestamp()
    failures, backoff = 0, 5
    while time.time() < deadline:
        try:
            return await _wait_status_poll(store, state, name, manifest, filename=filename, host=host)
        except CredentialUnavailable:
            failures += 1
            print(f'credential attempt {failures} failed; re-authenticate gcloud', file=sys.stderr)
            await asyncio.sleep(min(backoff, max(0, deadline - time.time())))
            backoff = min(backoff * 2, 60)
    raise TimeoutError('qualification status missing before deadline')


async def _wait_status_poll(store, state, name, manifest, *, filename='status.json', host=None):
    key = 'runs/' + state['run_id'] + '/qualification/' + name + '/' + filename
    while time.time() < campaign.parse_time(state['deadline']).timestamp():
        try:
            meta = await _blocking(store.head, key)
            raw = await _blocking(store.get, key, generation=meta['generation'], max_bytes=1024 * 1024)
            value = json.loads(raw)
            if host is not None and (_digest(raw) != meta.get('metadata', {}).get('sha256')
                                     or not re.fullmatch(r'[1-9][0-9]*', str(meta.get('generation', '')))):
                raise ValueError('qualification status checksum or generation mismatch')
        except ApiError as exc:
            if exc.status != 404:
                raise
            failure_key = 'runs/' + state['run_id'] + '/qualification/' + name + '/startup-error.json'
            try:
                failure_meta = await _blocking(store.head, failure_key)
                failure = json.loads(await _blocking(store.get, failure_key, generation=failure_meta['generation'], max_bytes=1024 * 1024))
            except ApiError as failure_error:
                if failure_error.status != 404:
                    raise
            else:
                if failure.get('source_sha256') != state['source_sha256'] or failure.get('complete') is not False:
                    raise RuntimeError('startup error identity mismatch')
                raise RuntimeError('qualification startup failed: ' + str(failure.get('error')))
            if filename == 'preparation.json':
                status_key = key.removesuffix(filename) + 'status.json'
                try:
                    status_meta = await _blocking(store.head, status_key)
                    status = json.loads(await _blocking(store.get, status_key, generation=status_meta['generation'],
                                                        max_bytes=1024 * 1024))
                except ApiError as status_error:
                    if status_error.status != 404:
                        raise
                else:
                    if status.get('complete') is False:
                        raise RuntimeError('guest preparation failed: ' + str(status.get('error')))
            await asyncio.sleep(1)
            continue
        expected = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
                    'manifest_sha256': manifest, 'deadline_utc': state['deadline']}
        if host is not None:
            expected.update(instance_name=host['name'], instance_id=str(host['id']))
        if any(value.get(k) != v for k, v in expected.items()) or value.get('complete') is not True:
            raise RuntimeError('qualification failed or status identity mismatch: ' + str(value))
        if host is not None:
            value['_reference'] = {'object': key, 'generation': str(meta['generation']), 'sha256': _digest(raw)}
        return value
    raise TimeoutError('qualification status missing before deadline')


def _evidence(store, bucket, state, name, output):
    key = 'runs/' + state['run_id'] + '/qualification/' + name + '/diagnostic.json'
    meta = store.head(key)
    data = store.get(key, generation=meta['generation'], max_bytes=16 * 1024 * 1024)
    path = Path(output) / (name + '-diagnostic.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {'file': str(path), 'reference': {'bucket': bucket, 'object': key,
            'generation': str(meta['generation']), 'sha256': _digest(data)}}

def _admit_live_pair(records, hosts, spec, rdma_device, run_id=None):
    if not gcp_runtime.live_mode(spec):
        return
    import gcp_live_qualification
    if len(records) != 2 or len(hosts) != 2 or hosts[0]['name'] == hosts[1]['name']:
        raise ValueError('two distinct admitted qualification guests required')
    coordinator_index = min(range(2), key=lambda i: hosts[i]['name'])
    owner_index = 1 - coordinator_index
    coordinator, owner = hosts[coordinator_index], hosts[owner_index]
    proofs = records[coordinator_index].get('live_nvmeof')
    required = {backend.removeprefix('tidehunter_nvme_') for backend in spec['backends']
                if backend in ('tidehunter_nvme_tcp', 'tidehunter_nvme_rdma')}
    if not isinstance(proofs, dict) or set(proofs) != required:
        raise ValueError('two-host live NVMe qualification proof missing')
    for transport in sorted(required):
        tiers = proofs[transport]
        if not isinstance(tiers, dict) or set(tiers) != {'native', 'synchronous'}:
            raise ValueError('live NVMe qualification requires both durability tiers')
        for tier, result in tiers.items():
            if not isinstance(result, dict):
                raise ValueError('invalid live NVMe qualification proof')
            address = 'rdma_ip' if transport == 'rdma' else 'internal_ip'
            if transport == 'rdma' and any(
                    not isinstance(result.get(key), dict)
                    or not isinstance(result[key].get('before'), dict)
                    or result[key]['before'].get('device') != rdma_device
                    for key in ('coordinator_counters', 'owner_counters')):
                raise ValueError('physical RDMA evidence does not match the admitted device')
            gcp_live_qualification.admit(result.get('proof'), transport, owner[address], coordinator[address],
                                         owner['internal_ip'], result.get('coordinator_counters'),
                                         result.get('owner_counters'), target='kernel', tier=tier,
                                         owner_launch=result.get('owner_launch'), owner_receipt=result.get('owner_receipt'))
    if spec.get('spdk_comparison'):
        if not isinstance(run_id, str) or not re.fullmatch(r'bench-[0-9a-f]{16}', run_id):
            raise ValueError('live SPDK qualification requires the bound pair run identity')
        tiers = records[coordinator_index].get('live_spdk_rdma')
        if not isinstance(tiers, dict) or set(tiers) != {'native', 'synchronous'} or 'rdma' not in required:
            raise ValueError('two-host live SPDK RDMA qualification requires both durability tiers')
        identity = records[owner_index].get('outputs', {}).get('package', {}).get('spdk', {})
        gcp_image.admit_spdk_scope(spec, identity)
        sha = identity.get('files', {}).get('bin/nvmf_tgt', {}).get('sha256')
        if not isinstance(sha, str) or not re.fullmatch(r'[0-9a-f]{64}', sha):
            raise ValueError('qualified SPDK binary identity missing')
        for tier, result in tiers.items():
            if not isinstance(result, dict):
                raise ValueError('invalid live SPDK qualification proof')
            for key in ('coordinator_counters', 'owner_counters'):
                if result.get(key, {}).get('before', {}).get('device') != rdma_device:
                    raise ValueError('SPDK physical RDMA evidence differs from admitted device')
            proof = result.get('proof')
            gcp_live_qualification.admit(proof, 'rdma', owner['rdma_ip'], coordinator['rdma_ip'],
                                         owner['internal_ip'], result.get('coordinator_counters'),
                                         result.get('owner_counters'), target='spdk', tier=tier,
                                         owner_launch=result.get('owner_launch'), owner_receipt=result.get('owner_receipt'))
            root = Path('/mnt/tidehunter-kv-comparison') / run_id / f'live-rdma-spdk-{tier}/node-0/storage'
            gcp_live_qualification.admit_spdk_observation(result.get('spdk_observation'),
                result.get('spdk_daemon'), proof, root, sha)



async def _admit_boot_images(compute, state, image):
    """Re-read exact guest-visible boot disks and pin both image path and ID."""
    image_id = str(image.get('id', ''))
    image_path = image.get('path', '')
    if not re.fullmatch(r'[1-9][0-9]*', image_id) or not re.fullmatch(
            r'projects/[a-z0-9-]+/global/images/[a-z0-9-]+', image_path):
        raise ValueError('exact source image identity required')
    if not state.get('instances'):
        raise ValueError('no boot disks to admit')
    for record in state['instances']:
        disk = await compute._describe('disk', record['name'], guest=True)
        source = disk.get('sourceImage', '')
        if (not campaign.owned_disk(disk, state, record)
                or str(disk.get('id', '')) != str(record.get('disk_id', ''))
                or str(disk.get('sourceImageId', '')) != image_id
                or not isinstance(source, str)
                or not (source == image_path or source.endswith('/' + image_path))):
            raise RuntimeError('guest boot disk source image identity mismatch')


def _release_gate(store, state, state_path, host, script):
    gate = {'run_id': state['run_id'], 'source_sha256': state['source_sha256'],
            'instance_id': str(host['id']), 'deadline_utc': state['deadline'],
            'script': script, 'sha256': _digest(script.encode())}
    state.setdefault('gates', {})[host['name']] = {
        'instance_id': str(host['id']), 'sha256': gate['sha256']}
    campaign.save(state_path, state)
    _put_json(store, 'runs/' + state['run_id'] + '/qualification/' + host['name'] + '/gate.json', gate)


def _collect_object(store, meta, target, old, remaining_bytes, *, mutable):
    """Download one exact generation; only named mutable objects may refresh."""
    key = meta['name']
    for _ in range(3):
        if meta.get('name') != key:
            raise ValueError('result object identity mismatch')
        generation = str(meta['generation'])
        digest = meta.get('metadata', {}).get('sha256', '')
        size = int(meta['size'])
        if not re.fullmatch(r'[1-9][0-9]*', generation) or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ValueError('result identity missing')
        if old is not None:
            if old['generation'] == generation:
                if (old['size'], old['sha256']) != (size, digest):
                    raise ValueError('result generation metadata changed')
                return None
            if not mutable or int(generation) <= int(old['generation']):
                raise ValueError('result object changed after collection')
        if size < 0 or size > campaign.MAX_RESULT_OBJECT_BYTES or size > remaining_bytes:
            raise ValueError('result collection bounds exceeded')
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + '.' + uuid.uuid4().hex + '.part')
        try:
            try:
                store.download(key, temporary, generation=generation, max_bytes=size)
            except ApiError as exc:
                if not mutable or exc.status != 404:
                    raise
                try:
                    meta = store.head(key)
                except ApiError as missing:
                    if missing.status != 404:
                        raise
                    return None
                continue
            if temporary.stat().st_size != size or campaign.file_sha256(temporary) != digest:
                raise ValueError('result size or checksum mismatch')
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        return {'key': key, 'generation': generation, 'size': size, 'sha256': digest}
    return None


def _collect(store, run_id, output, inventory):
    """Keep every object, with at most eight immutable transfers in flight."""
    prefix = run_id + '/results/'
    total = sum(row['size'] for row in inventory.values())
    pending, reserved, new_objects = [], 0, 0

    def remember(row):
        nonlocal total
        if row is not None:
            old = inventory.get(row['key'])
            total += row['size'] - (old['size'] if old else 0)
            inventory[row['key']] = row

    def drain():
        nonlocal reserved, new_objects
        for future in pending:
            remember(future.result())
        pending.clear()
        reserved, new_objects = 0, 0

    try:
        with ThreadPoolExecutor(max_workers=8) as transfers:
            for meta in store.list(prefix):
                key = meta['name']
                relative = key.removeprefix(prefix)
                parts = relative.split('/')
                if not key.startswith(prefix) or not relative or any(p in ('', '.', '..') for p in parts):
                    raise ValueError('unsafe result object name')
                mutable = relative in {'startup.json', 'progress.json', 'receipt.json'}
                if mutable or len(pending) == 8:
                    drain()
                old = inventory.get(key)
                if old is None and len(inventory) + new_objects >= campaign.MAX_RESULT_OBJECTS:
                    raise ValueError('result collection bounds exceeded')
                target = Path(output, 'results', *parts)
                remaining = campaign.MAX_RESULT_TOTAL_BYTES - total - reserved + (old['size'] if old else 0)
                if mutable:
                    remember(_collect_object(store, meta, target, old, remaining, mutable=True))
                else:
                    size = int(meta['size'])
                    if size < 0 or size > campaign.MAX_RESULT_OBJECT_BYTES or size > remaining:
                        raise ValueError('result collection bounds exceeded')
                    pending.append(transfers.submit(_collect_object, store, meta, target, old, remaining, mutable=False))
                    reserved += max(0, size - (old['size'] if old else 0))
                    new_objects += old is None
            drain()
    finally:
        # Preserve successful siblings even if listing or another transfer failed.
        for future in pending:
            if future.done() and not future.cancelled() and future.exception() is None:
                remember(future.result())
        Path(output, 'result-inventory.json').write_bytes(_json(list(inventory.values())))


def _restore_inventory(run_id, output):
    path = Path(output, 'result-inventory.json')
    if not path.exists():
        return {}
    rows = json.loads(path.read_bytes())
    if not isinstance(rows, list) or len(rows) > campaign.MAX_RESULT_OBJECTS:
        raise ValueError('invalid saved result inventory')
    inventory = {}
    total = 0
    prefix = run_id + '/results/'
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'key', 'generation', 'size', 'sha256'}:
            raise ValueError('invalid saved result entry')
        key = row['key']
        size = row['size']
        if (not isinstance(key, str) or not key.startswith(prefix) or key in inventory
                or any(part in ('', '.', '..') for part in key[len(prefix):].split('/'))
                or not re.fullmatch(r'[1-9][0-9]*', str(row['generation']))
                or not isinstance(row['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', row['sha256'])
                or type(size) is not int or size < 0 or size > campaign.MAX_RESULT_OBJECT_BYTES):
            raise ValueError('invalid saved result identity')
        inventory[key] = row
        total += size
        if total > campaign.MAX_RESULT_TOTAL_BYTES:
            raise ValueError('saved result aggregate exceeds bound')
    return inventory


def _collect_qualification(store, children, output):
    """Export bounded immutable qualification objects, retaining partial inventory."""
    inventory = []
    total = 0
    try:
        for phase in ('fleet', 'pair', 'bake'):
            child = children.get(phase)
            if child is None:
                continue
            prefix = 'runs/' + child['run_id'] + '/qualification/'
            for meta in store.list(prefix):
                key = meta['name']
                relative = key.removeprefix(prefix)
                parts = relative.split('/')
                if not key.startswith(prefix) or not relative or any(p in ('', '.', '..') for p in parts):
                    raise ValueError('unsafe qualification object name')
                generation = str(meta['generation'])
                size = int(meta['size'])
                if (len(inventory) >= 1024 or not re.fullmatch(r'[1-9][0-9]*', generation)
                        or size < 0 or size > 16 * 1024 * 1024 or total + size > 128 * 1024 * 1024):
                    raise ValueError('qualification artifact bounds exceeded')
                expected = meta.get('metadata', {}).get('sha256')
                if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
                    raise ValueError('qualification object lacks valid SHA256 metadata')
                data = store.get(key, generation=generation, max_bytes=size)
                digest = _digest(data)
                if len(data) != size or digest != expected:
                    raise ValueError('qualification object checksum or size mismatch')
                target = Path(output, 'qualification', child['run_id'], *parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                inventory.append({'key': key, 'generation': generation, 'size': size,
                                  'sha256': digest, 'sha256_metadata_verified': True})
                total += size
    finally:
        Path(output, 'qualification-inventory.json').write_bytes(_json(inventory))


def _new_child(parent, path, name, lifetime):
    if name in parent['children']:
        raise RuntimeError('automatic retry of a phase is forbidden')
    now = time.time()
    deadline = min(now + lifetime, campaign.parse_time(parent['deadline']).timestamp())
    if deadline <= now + EXPORT_RESERVE_SECONDS and name == 'fleet':
        raise TimeoutError('insufficient fleet and export lifetime')
    child = {'run_id': _run_id(), 'project_id': parent['project_id'],
             'source_sha256': parent['source_sha256'], 'deadline': _utc(deadline),
             'owner_nonce': uuid.uuid4().hex, 'policy': None, 'instances': []}
    if child['run_id'] == parent['run_id'] or any(
            item['run_id'] == child['run_id'] for item in parent['children'].values()):
        raise RuntimeError('run ID collision; refusing automatic replacement')
    parent['children'][name] = child
    campaign.save(path, parent)
    return child, _ChildFile(path, parent, name)


def _cleanup_live_capsules(store, child):
    run_id = child['run_id']
    if not re.fullmatch(r'bench-[0-9a-f]{16}', run_id):
        raise ValueError('invalid pair run identity')
    for transport in ('tcp', 'rdma'):
        key = 'runs/' + run_id + '/private-live-' + transport + '.json'
        try:
            meta = store.head(key)
        except ApiError as exc:
            if exc.status == 404:
                continue
            raise
        generation = str(meta.get('generation', ''))
        if not re.fullmatch(r'[1-9][0-9]*', generation):
            raise ValueError('private capsule has invalid generation')
        raw = store.get(key, generation=generation, max_bytes=1024 * 1024)
        if _digest(raw) != meta.get('metadata', {}).get('sha256'):
            raise ValueError('private capsule checksum mismatch')
        value = json.loads(raw)
        identity = value.get('identity', {})
        if (identity.get('run_id') != run_id or identity.get('source_sha256') != child['source_sha256']
                or identity.get('deadline_utc') != child['deadline']
                or identity.get('manifest_sha256') != child.get('package', {}).get('manifest_sha256')):
            raise ValueError('private capsule identity mismatch')
        store.delete(key, if_generation_match=generation)


async def _cleanup(config, parent, path, store, execute):
    from gcp_manual_compute import Compute
    from gcp_manual_image import cleanup_image
    # A missing or replaced lease never authorizes deletion.
    lease = await _blocking(_lease_owned, store, parent)
    errors = []
    for name in ('fleet', 'pair', 'bake'):
        child = parent['children'].get(name)
        if child is None:
            continue
        child_path = _ChildFile(path, parent, name)
        await _blocking(_lease_owned, store, parent)
        try:
            await Compute(config, child, child_path, execute).cleanup()
            if name in ('fleet', 'pair'):
                await _blocking(_lease_owned, store, parent)
                await _blocking(_cleanup_live_capsules, store, child)
        except Exception as exc:
            errors.append(name + ': ' + str(exc))
    bake = parent['children'].get('bake')
    if bake is not None:
        await _blocking(_lease_owned, store, parent)
        try:
            await cleanup_image(config, bake, _ChildFile(path, parent, 'bake'), execute)
        except Exception as exc:
            errors.append('image: ' + str(exc))
    parent['cleanup_errors'] = errors
    parent['cleanup_verified'] = not errors
    await _blocking(campaign.save, path, parent)
    if errors:
        raise RuntimeError('cleanup incomplete: ' + '; '.join(errors))
    await _blocking(gcp_lease.release, store, *lease)


async def _cleanup_joined(config, parent, path, store, execute):
    """Finish teardown despite caller cancellation, then propagate it."""
    task = asyncio.create_task(_cleanup(config, parent, path, store, execute))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except BaseException as failure:
            raise BaseExceptionGroup('cancelled while cleanup failed', [cancelled, failure])
        raise


async def recover_cleanup(config, *, run_id, state_path, execute, artifacts=None):
    """Recover using a fresh bounded store, without requiring GitHub."""
    if not re.fullmatch(r'bench-[0-9a-f]{16}', run_id):
        raise ValueError('invalid parent run ID')
    artifacts = artifacts or DelegatedArtifacts(config['bucket'], deadline=time.time() + 1800)
    path, parent = await _blocking(campaign.StateFile.recover, state_path, artifacts, run_id)
    if parent.get('run_id') != run_id or parent.get('project_id') != config.get('project_id'):
        raise ValueError('recovered parent identity mismatch')
    if campaign.is_admission_refusal(parent):
        if await _blocking(gcp_lease.read, artifacts, run_id) is not None:
            raise RuntimeError('admission refusal has an unexpected active lease')
        return parent
    if parent.get('cleanup_verified') is True and await _blocking(gcp_lease.read, artifacts, run_id) is None:
        return parent
    await _cleanup_joined(config, parent, path, artifacts, execute)
    return parent


def _terminal_export(store, state, package, *, require_complete=True, result=None, suite=None):
    """Pin the final controller receipt and its durable archive descriptors."""
    if result is not None:
        import manual_sui
        references = {}
        if 'kv' in result or suite == 'both':
            try:
                references.update(_terminal_export(store, state, package, require_complete=require_complete))
            except ApiError as error:
                if require_complete or error.status != 404:
                    raise
        references.update(manual_sui.terminal_export_sui(store, state, result, require_complete=require_complete))
        return references
    prefix = state['run_id'] + '/results/'
    key = prefix + 'receipt.json'
    meta = store.head(key)
    generation = str(meta.get('generation', ''))
    size = int(meta.get('size', -1))
    digest = meta.get('metadata', {}).get('sha256', '')
    if (meta.get('name') != key or not re.fullmatch(r'[1-9][0-9]*', generation)
            or not 0 < size <= 4 * 1024 * 1024
            or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)):
        raise ValueError('terminal export receipt identity missing')
    raw = store.get(key, generation=generation, max_bytes=size)
    if len(raw) != size or _digest(raw) != digest:
        raise ValueError('terminal export receipt checksum mismatch')
    receipt = json.loads(raw)
    inventory = receipt.get('inventory') if isinstance(receipt, dict) else None
    if (not isinstance(receipt, dict) or receipt.get('schema_version') != 2
            or receipt.get('status') not in (('completed',) if require_complete else ('completed', 'incomplete'))
            or receipt.get('source_sha256') != state['source_sha256']
            or receipt.get('binary_blake2b512') != package['binary_blake2b512']
            or receipt.get('key') != prefix + 'controller.tar.gz'
            or not isinstance(inventory, dict)
            or set(inventory) != {'key', 'bytes', 'sha256', 'generation'}
            or inventory.get('key') != prefix + 'controller-inventory.json'):
        raise ValueError('terminal controller export provenance mismatch')
    references = {key: {'generation': generation, 'size': size, 'sha256': digest}}
    for descriptor in (receipt, inventory):
        object_key = descriptor['key']
        length, checksum = descriptor.get('bytes'), descriptor.get('sha256')
        if (type(length) is not int or not 0 < length <= campaign.MAX_RESULT_OBJECT_BYTES
                or not isinstance(checksum, str) or not re.fullmatch(r'[0-9a-f]{64}', checksum)):
            raise ValueError('terminal controller export descriptor invalid')
        head = store.head(object_key)
        object_generation = str(head.get('generation', ''))
        if (head.get('name') != object_key or not re.fullmatch(r'[1-9][0-9]*', object_generation)
                or int(head.get('size', -1)) != length
                or head.get('metadata', {}).get('sha256') != checksum
                or (descriptor is inventory and str(descriptor.get('generation')) != object_generation)):
            raise ValueError('terminal controller export is not durably bound')
        references[object_key] = {'generation': object_generation, 'size': length, 'sha256': checksum}
    return references

def _certificate_failure(error):
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        pending.extend((current.__cause__, current.__context__))
        if isinstance(current, urllib.error.URLError):
            pending.append(current.reason)
    return False



def _admit_nvmet_proof(proof, candidate, host, state):
    """Require the admitted module to be loaded on this exact owned guest."""
    if candidate is None:
        if proof is not None:
            raise ValueError('unrequested nvmet fix proof')
        return
    if not isinstance(proof, dict) or proof.get('candidate') != candidate:
        raise ValueError('nvmet fix candidate proof mismatch')
    loaded = proof.get('loaded', {})
    guest = loaded.get('guest', {})
    if (any(loaded.get(key) != value for key, value in candidate.items())
            or loaded.get('loaded_nvmet') != {'srcversion': candidate['candidate_srcversion'],
                                              'build_id': candidate['candidate_build_id']}
            or guest.get('instance_name') != host['name']
            or guest.get('instance_id') != str(host['id'])
            or guest.get('run_id') != state['run_id']
            or guest.get('owner_nonce') != state['owner_nonce']):
        raise ValueError('nvmet fix loaded identity does not match the admitted guest')


async def _headroom(mailbox, fleet, state, package_root, rdma_device, output):
    """Probe disposable owner targets and always retire all owned resources."""
    program = package_root + '/source/experiments/gcp-rdma/gcp_headroom_probe.py'
    run_id = state['run_id']
    cleanup_timeout = 180

    async def request(index, action, extra):
        argv = [gcp_runtime.GUEST_PYTHON, program, action, '--run-id', run_id, *extra]
        timeout = cleanup_timeout if action == 'cleanup' else 360
        ref = await _blocking(mailbox.submit, index, argv,
                              operation_id='headroom-' + action + '-' + str(index),
                              timeout_seconds=timeout, cwd=package_root)
        receipt = await _blocking(mailbox.wait, ref, timeout_seconds=timeout + 15)
        Path(output, 'headroom-' + action + '-' + str(index) + '.json').write_bytes(_json(receipt))
        return receipt

    error = None
    result = None
    probe_submitted = False
    try:
        created = await _joined_waiters(
            request(index, 'create', ['--owner', str(index), '--address', fleet[index]['rdma_ip']])
            for index in range(1, len(fleet)))
        if any(item['exit_code'] != 0 for item in created):
            raise RuntimeError('headroom target creation failed; see mailbox receipts')
        probe_submitted = True
        receipt = await request(0, 'probe', ['--addresses', ','.join(host['rdma_ip'] for host in fleet[1:]),
                                             '--device', rdma_device])
        Path(output, 'headroom-probe-receipt.json').write_bytes(_json(receipt))
        result = json.loads(receipt['stdout'])
        cleanup_timeout = max(0, int(result.get('teardown_started_unix', 0) + 180 - time.time()))
        if (receipt['exit_code'] != 0 or result.get('admitted') is not True
                or result.get('cleanup_complete') is not True or result.get('connected', -1) < 135):
            raise RuntimeError('headroom probe did not admit 135 controllers with clean retirement')
    except BaseException as exc:
        error = exc
    if probe_submitted and (not isinstance(result, dict) or result.get('cleanup_complete') is not True):
        raise RuntimeError('probe drain unconfirmed; owner targets retained for VM teardown') from error
    try:
        if cleanup_timeout < 1:
            raise TimeoutError('headroom teardown deadline exhausted')
        cleaned = await _joined_waiters(request(index, 'cleanup', ['--owner', str(index)])
                                        for index in range(1, len(fleet)))
        if probe_submitted and time.time() > result['teardown_started_unix'] + 180:
            raise TimeoutError('headroom teardown exceeded 180 seconds')
        if any(item['exit_code'] != 0 or json.loads(item['stdout']).get('clean') is not True for item in cleaned):
            raise RuntimeError('headroom target cleanup unconfirmed; see mailbox receipts')
    except BaseException as cleanup_error:
        if error is not None:
            raise BaseExceptionGroup('headroom probe and cleanup failed', [error, cleanup_error])
        raise
    if error is not None:
        raise error
    return result


async def _suite(store, config, state, path, fleet, package, spec, output, rdma_device):
    runtime = gcp_runtime.build_runtime_config(
        config, fleet, package, spec, state['deadline'], run_id=state['run_id'],
        campaign_deadline_utc=_utc(campaign.parse_time(state['deadline']).timestamp() - CAMPAIGN_MARGIN_SECONDS),
        placement_policy={'name': state['policy']['name'], 'id': state['policy']['id']})
    state['runtime_sha256'] = _digest(_json(runtime))
    await _blocking(campaign.save, path, state)
    await _blocking(_put_json, store, 'runs/' + state['run_id'] + '/runtime.json', runtime)
    async with asyncio.timeout(min(WORKER_READY_SECONDS, campaign.coordinator_timeout(
            state['deadline'], EXPORT_RESERVE_SECONDS, dt.datetime.now(dt.timezone.utc)))):
        ready = await _joined_waiters(
            _wait_status(store, state, host['name'], package['manifest_sha256'],
                         filename='worker-ready.json', host=host) for host in fleet)
    for host, item in zip(fleet, ready):
        _admit_nvmet_proof(item.get('nvmet_fix'), package.get('nvmet_fix'), host, state)
    state['worker_readiness'] = [dict(bucket=config['bucket'], **item['_reference']) for item in ready]
    await _blocking(campaign.save, path, state)
    root = runtime['package_root']
    mailbox = gcp_runtime.RemoteMailbox(_StoreApi(store, config['bucket']), config['bucket'],
                                        state['run_id'], fleet, state['source_sha256'], state['deadline'])
    if len(fleet) > 2:
        state['headroom_probe'] = await _headroom(mailbox, fleet, state, root, rdma_device, output)
        await _blocking(campaign.save, path, state)
    remaining = campaign.coordinator_timeout(state['deadline'], EXPORT_RESERVE_SECONDS, dt.datetime.now(dt.timezone.utc))
    state['campaign_submitted'] = True
    await _blocking(campaign.save, path, state)
    ref = await _blocking(mailbox.submit, 0, [gcp_runtime.GUEST_PYTHON, root + '/source/experiments/gcp-rdma/gcp_runtime.py',
                                               'execute', '/mnt/tidehunter/' + state['run_id'] + '/runtime.json'],
                          operation_id='campaign', timeout_seconds=remaining, cwd=root)
    while True:
        try:
            timeout = campaign.parse_time(state['deadline']).timestamp() - 60 - time.time()
            if timeout <= 0:
                raise TimeoutError('terminal response missing before deadline; operation outcome unknown')
            terminal = await _blocking(mailbox.wait, ref, timeout_seconds=timeout)
            break
        except TimeoutError:
            if time.time() >= campaign.parse_time(state['deadline']).timestamp() - 60:
                raise
            await asyncio.sleep(0)
        except subprocess.TimeoutExpired as exc:
            if not isinstance(store, DelegatedArtifacts):
                raise
            expected = [store.executable, 'auth', 'print-access-token',
                        '--impersonate-service-account=' + ACCOUNT,
                        '--project=' + PROJECT, '--quiet']
            if exc.cmd != expected:
                raise
            if time.time() >= campaign.parse_time(state['deadline']).timestamp() - 60:
                raise TimeoutError('terminal response unavailable before deadline; operation outcome unknown') from exc
            # Only this read-side token subprocess timeout is retryable. Keep
            # the immutable request reference and original absolute deadline.
            await asyncio.sleep(0)
        except ApiError as exc:
            cause = exc.__cause__
            if (not isinstance(store, DelegatedArtifacts) or exc.status is not None
                    or not isinstance(cause, (urllib.error.URLError, TimeoutError, ConnectionError))
                    or isinstance(cause, urllib.error.HTTPError) or _certificate_failure(exc)):
                raise
            if time.time() >= campaign.parse_time(state['deadline']).timestamp() - 60:
                raise TimeoutError('terminal response unavailable before deadline; operation outcome unknown') from exc
            # mailbox.wait only observes the already submitted immutable ref.
            await asyncio.sleep(0)
    # The worker publishes its request-bound response only after the process
    # exits. Controller receipts alone can be provisional and are not a signal
    # that the coordinator has stopped writing.
    Path(output, 'campaign-receipt.json').write_bytes(_json(terminal))
    workload_error = None
    result = {}
    if terminal['exit_code'] != 0:
        workload_error = RuntimeError('coordinator failed; see campaign-receipt.json')
    else:
        try:
            # The runtime prints the result as its last line; earlier lines are
            # output of verification subprocesses (for example `verified: true` blocks).
            result = json.loads(terminal['stdout'].strip().splitlines()[-1])
            if spec.get('suite') == 'sui':
                import manual_sui
                if not manual_sui.sui_complete(result, spec):
                    raise RuntimeError('coordinator did not complete the reviewed Sui suite')
            else:
                kv = result.get('kv', {})
                expected_cases = (spec['job_count'] if spec.get('mode') == 'focused' else
                                  len(workflow_input.cloud_campaign.make_matrix(
                                      {k: v for k, v in spec.items() if k != 'suite'})))
                if kv.get('complete') is not True or kv.get('planned_cases') != expected_cases or kv.get('completed_cases') != expected_cases:
                    raise RuntimeError('coordinator did not complete the reviewed combined suite')
                profiles = spec.get('valkey_reproduction_profiles', [])
                if profiles:
                    native = kv.get('native_valkey_reproduction', {})
                    expected = {(profile, transport) for profile in profiles for transport in ('tcp', 'rdma')}
                    cases = native.get('cases', [])
                    observed = {(case.get('profile'), case.get('transport')) for case in cases}
                    if (native.get('complete') is not True or observed != expected or len(cases) != len(expected)
                            or native.get('planned_cases') != len(expected) or native.get('completed_cases') != len(expected)
                            or any(case.get('complete') is not True or case.get('owned_server_stopped') is not True for case in cases)):
                        raise RuntimeError('coordinator did not complete every isolated native Valkey control')
            if spec.get('suite') == 'both':
                import manual_sui
                if not manual_sui.sui_complete(result, spec):
                    raise RuntimeError('coordinator did not complete the reviewed Sui suite')
        except Exception as exc:
            workload_error = exc
    try:
        references = await _blocking(_terminal_export, store, state, package,
                                     require_complete=workload_error is None,
                                     **({"result": result, "suite": spec["suite"]} if spec.get("suite") in ("sui", "both") else {}))
        state['terminal_export'] = references
        try:
            await _blocking(campaign.save, path, state)
        except BaseException:
            state.pop('terminal_export', None)
            raise
    except BaseException as export_error:
        if workload_error is not None:
            raise BaseExceptionGroup('workload and terminal export validation failed',
                                     [workload_error, export_error])
        raise
    if workload_error is not None:
        raise workload_error
    await _blocking(_put_json, store, 'runs/' + state['run_id'] + '/poweroff/operator.json',
                    {'schema_version': 1, 'run_id': state['run_id'],
                     'source_sha256': state['source_sha256'], 'action': 'operator_has_taken_over'})
    return terminal


def _lease_owned(store, parent):
    lease, generation = gcp_lease.owned(store, parent)
    if lease.get('token') != parent.get('lease_token'):
        raise RuntimeError('active lease owner token changed')
    return lease, generation


def _start_lease(store, parent, path, *, max_runs=3, ceiling_cents=CEILING_CENTS, quotas=None):
    lease, generation = gcp_lease.acquire(
        store, parent, max_runs=max_runs, ceiling_cents=ceiling_cents, quotas=quotas)
    parent['lease_token'] = lease['token']
    try:
        campaign.save(path, parent)
    except BaseException:
        gcp_lease.release(store, lease, generation)
        parent.pop('lease_token', None)
        raise


async def _finish_campaign(config, parent, path, store, execute, output, error):
    """Use early teardown only after a terminal, durably bound export."""
    failures = [error] if error is not None else []

    async def cleanup():
        if parent.get('lease_token'):
            try:
                await _cleanup_joined(config, parent, path, store, execute)
            except BaseException as exc:
                failures.append(exc)

    async def collect():
        try:
            await _blocking(_collect_qualification, store, parent['children'], output)
        except BaseException as exc:
            failures.append(exc)
        if 'fleet' in parent['children']:
            try:
                fleet_state = parent['children']['fleet']
                saved = _restore_inventory(fleet_state['run_id'], output)
                await _blocking(_collect, store, fleet_state['run_id'], output, saved)
                for key, expected in fleet_state.get('terminal_export', {}).items():
                    actual = saved.get(key)
                    if actual is None or any(actual[field] != expected[field] for field in ('generation', 'size', 'sha256')):
                        raise ValueError('collected controller export differs from terminal generation')
            except BaseException as exc:
                failures.append(exc)

    durable_terminal = bool(parent['children'].get('fleet', {}).get('terminal_export'))
    if durable_terminal:
        await cleanup()
        if parent.get('cleanup_verified') and isinstance(store, DelegatedArtifacts):
            # Offline transfer grace cannot extend the deleted fleet's expiry.
            store.deadline = max(store.deadline, time.time() + 7200)
        await collect()
    else:
        # Preserve the previous best-effort partial export before terminating an
        # unknown or unverified workload, including failed publication.
        await collect()
        await cleanup()
    if failures:
        raise failures[0] if len(failures) == 1 else BaseExceptionGroup(
            'campaign, cleanup or export failed', failures)


async def run(config, *, package_dir, revision, spec, wheels, base_image,
              base_image_id, rdma_device, output, execute, artifacts=None,
              surcharge_cents, preflight_api=None, fixture=None):
    """Run one reviewed campaign; never creates a replacement on failure.

    ``execute`` is an async callback accepting gcloud argv without ``gcloud``.
    ``package_dir`` is a previously qualified, exact-checkout snapshot. The
    surcharge quote must cover disks, image, objects, requests and transfers;
    the reservation is an estimate, not a project-wide billing cap.
    """
    from gcp_manual_compute import Compute, _json_response

    import manual_sui
    if spec.get("suite") in ("sui", "both") and fixture is None:
        raise ValueError("Sui suite requires a fixture path")
    _check_config(config, spec, surcharge_cents)
    spec = copy.deepcopy(spec)
    host_count = spec.get('owner_hosts', 5) + 1
    config = dict(config, machine_type=config.get('machine_type', 'z4m-highmem-16'))
    duration, compute, ceiling = reservation_quote(spec, config['machine_type'])
    weekly_limit = json.loads(Path(__file__).with_name('budget.json').read_text()).get('weekly_limit_cents')
    if type(weekly_limit) is not int or weekly_limit <= 0:
        raise ValueError('budget.json requires a positive integer weekly_limit_cents')
    if not re.fullmatch(r'[A-Za-z0-9_]+', rdma_device):
        raise ValueError('invalid RDMA device')
    match = re.fullmatch(r'projects/mysten-research/global/images/[a-z0-9-]+', base_image)
    if not match or not re.fullmatch(r'[1-9][0-9]*', str(base_image_id)):
        raise ValueError('exact base image path and numeric ID required')
    if json.loads((Path(package_dir) / 'spec.json').read_text()) != spec:
        raise ValueError('package qualification specification differs from campaign')
    receipt, frozen_source = await _blocking(_admit_package, package_dir, revision, spec)
    if receipt.get('source_revision') != revision or not re.fullmatch(r'[0-9a-f]{64}', receipt.get('manifest_sha256', '')):
        raise ValueError('package source or sealed manifest mismatch')
    spec['hard_deadline_seconds'] = duration
    observer_qualification = await _blocking(_qualify_observer, package_dir, spec)
    source = receipt['source_sha256']
    gcp_runtime._sha(source)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    # Preflight does not make mutations or allocate and can refresh local credentials.
    await asyncio.to_thread(gcp_manual_preflight.permission_preflight, config, api=preflight_api)
    image_info = await gcp_manual_preflight.infrastructure_preflight(config, base_image, execute)
    if (not isinstance(image_info, dict) or str(image_info.get('id')) != str(base_image_id)
            or image_info.get('status') != 'READY'
            or not str(image_info.get('selfLink', '')).endswith('/' + base_image)):
        raise ValueError('base image numeric ID or READY identity mismatch')
    await _blocking(_check_snapshot, frozen_source, revision)
    start = time.time()
    parent = {'run_id': _run_id(), 'project_id': config['project_id'], 'source_sha256': source,
              'deadline': _utc(start + duration),
              'children': {}, 'phase': 'initial', 'cleanup_verified': False,
              'budget': {'ceiling_cents': ceiling, 'compute_cents': compute,
                         'non_compute_quote_cents': surcharge_cents, 'weekly_limit_cents': weekly_limit,
                         'reservation_hours': duration // 3600},
              'reservation': {'cents': ceiling, 'region': 'us-central1',
                              'instances': host_count,
                              'vcpus': int(config['machine_type'].rsplit('-', 1)[-1]) * host_count,
                              'local_ssds': gcp_runtime.LOCAL_SSD_COUNTS[config['machine_type']] * host_count,
                              'local_ssd_gib': gcp_runtime.LOCAL_SSD_COUNTS[config['machine_type']] * host_count * 14000},
              'base_image': {'path': base_image, 'id': str(base_image_id)},
              'package': {'sha256': receipt['sha256'], 'manifest_sha256': receipt['manifest_sha256'],
                          'source_revision': revision, 'source_tree_sha256': frozen_source['tree_sha256'],
                          'source_dirty': frozen_source['dirty']}, 'spec_sha256': _digest(_json(spec)),
              'observer_qualification': observer_qualification,
              'artifact_transport': 'injected' if artifacts is not None else TRANSPORT}
    store = artifacts or DelegatedArtifacts(config['bucket'], deadline=start + duration + 1800)
    path = campaign.StateFile(out / 'state.json', store, parent['run_id'])
    await _blocking(campaign.save, path, parent)
    error = None
    result = None
    try:
        try:
            quotas = await gcp_manual_preflight.quota_preflight(config, execute)
            await _blocking(_start_lease, store, parent, path,
                            max_runs=config.get('max_concurrent_runs', 3),
                            ceiling_cents=config.get('active_run_limit_cents', CEILING_CENTS), quotas=quotas)
        except BaseException:
            # Every operation above is read-only or lifecycle-state storage.
            # Child creation, package upload, create_policy and create_vm occur
            # only below, after _start_lease returns. No compute resource can
            # exist at this boundary; the recorder also proves no lease exists.
            await _blocking(campaign.record_admission_refusal, store, parent, path)
            raise
        await _blocking(_lease_owned, store, parent)
        await _blocking(_record, path, parent, 'budget')
        report = await _blocking(gcp_budget.reserve, store, parent['run_id'], ceiling, weekly_limit)
        (out / 'budget-report.json').write_bytes(_json(report))

        await _blocking(_record, path, parent, 'fleet')
        fleet_state, fleet_path = await _blocking(_new_child, parent, path, 'fleet', duration)
        fleet_state['host_count'] = host_count
        package, prep_sha = await _blocking(_stage, store, config['bucket'], fleet_state, receipt, wheels,
                                                **({"spec": spec, "fixture": fixture} if spec.get("suite") in ("sui", "both") else {}))
        fleet_state['package'] = package
        fleet_state['preparation_sha256'] = prep_sha
        await _blocking(campaign.save, fleet_path, fleet_state)
        fleet_compute = Compute(config, fleet_state, fleet_path, execute)
        await _blocking(_lease_owned, store, parent)
        await _blocking(_check_snapshot, frozen_source, revision)
        await fleet_compute.create_policy()
        fleet = []
        gated = _gate(config['bucket'], fleet_state)
        for i in range(host_count):
            await _blocking(_lease_owned, store, parent)
            fleet.append(await fleet_compute.create_vm(i, base_image, fleet_state['deadline'], gated))
        if await fleet_compute.admit_guests() != fleet:
            raise RuntimeError('guest fleet identity changed before admission')
        image = {'path': base_image, 'id': str(base_image_id)}
        await _admit_boot_images(fleet_compute, fleet_state, image)
        pair_hosts = fleet[:2]
        live_transports = ([backend.removeprefix('tidehunter_nvme_') for backend in spec['backends']
                            if backend in ('tidehunter_nvme_tcp', 'tidehunter_nvme_rdma')]
                           if gcp_runtime.live_mode(spec) else [])
        script = qualify.fleet_startup_script(
            config['bucket'], fleet_state['run_id'], source, fleet_state['deadline'],
            receipt['manifest_sha256'], prep_sha, members=fleet, rdma_device=rdma_device,
            machine_type=config['machine_type'], live_transports=live_transports,
            live_spdk=bool(spec.get('spdk_comparison') and gcp_runtime.live_mode(spec)),
            **({'qualification_profile': 'focused', 'selection': spec.get('selection'), **({'suite_spec': manual_sui.reviewed_part(spec), 'sui_extra_libraries': manual_sui.extra_libraries(receipt), 'sui_needed_libraries': receipt['sui_abi']['needed']} if spec.get('suite') in ('sui', 'both') else {})}
               if manual_sui.focused_qualification(spec) else {}))
        await _blocking(_record, path, parent, 'preparation')
        async with asyncio.timeout(PREPARATION_SECONDS):
            for host in fleet:
                await _blocking(_lease_owned, store, parent)
                await _blocking(_release_gate, store, fleet_state, fleet_path, host, script)
            prepared = await _joined_waiters(
                _wait_status(store, fleet_state, host['name'], receipt['manifest_sha256'],
                             filename='preparation.json', host=host) for host in fleet)
        for host, item in zip(fleet, prepared):
            proof = item.get('proof', {})
            if (proof.get('kind') != 'bake-software-diagnostic'
                    or proof.get('source_sha256') != source
                    or proof.get('manifest_sha256') != receipt['manifest_sha256']
                    or proof.get('preparation_sha256') != prep_sha
                    or proof.get('wheel_sha256') not in {w['sha256'] for w in wheels}):
                raise ValueError('guest preparation proof does not bind admitted software')
            _admit_nvmet_proof(proof.get('nvmet_fix'), receipt.get('nvmet_fix'), host, fleet_state)
        fleet_state['preparation_evidence'] = [dict(bucket=config['bucket'], **item['_reference']) for item in prepared]
        await _blocking(campaign.save, fleet_path, fleet_state)
        await _blocking(_record, path, parent, 'pair')
        pair_seconds = qualify.M3_PAIR_SECONDS if spec.get("selection") in workflow_input.cloud_campaign.M3_SELECTIONS else PAIR_SECONDS
        async with asyncio.timeout(min(pair_seconds + PAIR_CLEANUP_SECONDS,
                campaign.parse_time(fleet_state['deadline']).timestamp() - time.time() - EXPORT_RESERVE_SECONDS)):
            statuses = await _joined_waiters(
                _wait_status(store, fleet_state, host['name'], receipt['manifest_sha256'], host=host)
                for host in fleet)
            for index, status in enumerate(statuses):
                if status.get('cleanup_complete') is not True or status.get('pair') is not (index < 2):
                    raise ValueError('guest qualification cleanup proof missing or mismatched')
            evidence = [await _blocking(_evidence, store, config['bucket'], fleet_state, host['name'], out)
                        for host in pair_hosts]
        fleet_state['cleanup_evidence'] = [dict(bucket=config['bucket'], **item['_reference']) for item in statuses]
        await _blocking(campaign.save, fleet_path, fleet_state)
        records = [json.loads(Path(item['file']).read_bytes()) for item in evidence]
        for index, record in enumerate(records):
            if (record.get('instance_id') != str(pair_hosts[index]['id'])
                    or record.get('peer_instance_id') != str(pair_hosts[1-index]['id'])
                    or record.get('local_rdma') != pair_hosts[index]['rdma_ip']
                    or record.get('peer') != pair_hosts[1-index]['rdma_ip']):
                raise ValueError('qualification records do not match admitted pair')
        _admit_live_pair(records, pair_hosts, spec, rdma_device, fleet_state['run_id'])
        image_receipt = gcp_image_receipt.make_receipt(
            {'image': image, 'runtime_sha256': receipt['manifest_sha256'],
             'service_account': config['service_account'], 'evidence': evidence},
            **({'qualification_profile': 'focused'} if manual_sui.focused_qualification(spec) else {}))
        reference = await _blocking(_put_json, store, 'runs/' + fleet_state['run_id'] + '/qualification/image-receipt.json', image_receipt)
        reference['bucket'] = config['bucket']
        fleet_state['qualification_receipt'] = reference
        await _blocking(campaign.save, fleet_path, fleet_state)
        (out / 'image-receipt.json').write_bytes(_json(image_receipt))
        admitted_config = dict(config, image=image['path'], image_qualification=reference)
        if spec.get('suite') in ('sui', 'both'):
            admitted_config['sui_extra_libraries'] = manual_sui.extra_libraries(receipt)
            admitted_config['sui_needed_libraries'] = receipt['sui_abi']['needed']
        if manual_sui.focused_qualification(spec):
            admitted_config['qualification_profile'] = 'focused'
        if gcp_runtime.live_mode(spec):
            admitted_config['live_qualification'] = {
                'image': image['path'], 'image_qualification': reference,
                'pair_hosts': [{key: host[key] for key in ('name', 'id', 'zone', 'internal_ip', 'rdma_ip')}
                               for host in pair_hosts],
                'rdma_device': rdma_device, 'service_account': config['service_account']}
        remaining = campaign.parse_time(fleet_state['deadline']).timestamp() - time.time()
        if remaining <= EXPORT_RESERVE_SECONDS:
            raise TimeoutError('fleet deadline expired before qualification admission')
        info = _json_response(await asyncio.wait_for(
            execute(['compute', 'images', 'describe', image['path'].rsplit('/', 1)[-1],
                     '--project=' + config['project_id'], '--quiet', '--format=json']),
            timeout=min(30, remaining - EXPORT_RESERVE_SECONDS)))
        qualification = await _blocking(gcp_image.admit_image, _StoreApi(store, config['bucket']), admitted_config, info)
        gcp_image.admit_nvme_transports(qualification, spec['backends'],
                                        live=gcp_runtime.live_mode(spec))
        gcp_image.admit_spdk(qualification, spec)
        gcp_image.admit_abi(qualification, receipt['abi'])
        if spec.get('suite') in ('sui', 'both'):
            gcp_image.admit_abi(qualification, receipt['sui_abi'], sui=True)
        if await fleet_compute.admit_guests() != fleet:
            raise RuntimeError('guest fleet identity changed after qualification')
        await _admit_boot_images(fleet_compute, fleet_state, image)
        await _blocking(_record, path, parent, 'suite')

        await _blocking(_lease_owned, store, parent)
        result = await _suite(store, admitted_config, fleet_state, fleet_path, fleet, package, spec, out, rdma_device)
        await _blocking(_record, path, parent, 'complete')
    except BaseException as exc:
        error = exc
    await _finish_campaign(config, parent, path, store, execute, out, error)
    return {'parent_run_id': parent['run_id'], 'children': parent['children'],
            'observer_qualification': observer_qualification,
            'image_qualification': reference, 'terminal': result}
