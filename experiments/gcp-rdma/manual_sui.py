"""Reviewed compact Sui admission and generation-pinned artifacts."""
import copy
import json
from pathlib import Path
import re

import gcp_campaign as campaign
import prepare

SUI_REVISION = 'ef6701911cc07d9102f55e52f5a0632e5ddb0a86'
FIXTURE_SHA256 = '7eb43b71435547939760598e541d13b9a77f722c5a9d0f8c8ee5bd583b751f15'


def focused_qualification(spec):
    return spec.get("mode") == "focused" or (spec.get("suite") == "sui"
        and reviewed_sui_spec(reviewed_part(spec)))


def _specs():
    # Lazy import: manual_campaign imports this module before defining constants.
    from manual_campaign import FLEET_SECONDS, EXPORT_RESERVE_SECONDS, REVIEWED_SPECS
    specs = {'sui_nvmeof_compact': dict(
        schema_version=1, suite='sui', mode='standard-with-live-nvmeof', owner_hosts=1,
        backends=['native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'],
        sui_cells=[dict(transport=transport, contract=contract, owner_tier=tier, owner_wal='engine_default')
                   for contract, tier in [('original_default', 'native'), ('harness', 'synchronous')]
                   for transport in ['tidehunter_verbs_native', 'tidehunter_nvme_rdma']],
        sui_live_export_mode='per_owner',
        sui_live_queue_geometry=dict(io_queues=1, queue_size=32, poll_queues=0),
        sui_fixture=dict(sha256=FIXTURE_SHA256), hard_deadline_seconds=FLEET_SECONDS,
        export_reserve_seconds=EXPORT_RESERVE_SECONDS)}
    retry = copy.deepcopy(specs['sui_nvmeof_compact'])
    retry['sui_cells'] = retry['sui_cells'][2:]
    specs['sui_nvmeof_harness_synchronous'] = retry
    for name, selection in (('sui_nvmeof_with_kv', 'nvmeof_first_put'),
                            ('sui_nvmeof_with_contracts', 'nvmeof_contracts'),
                            ('sui_nvmeof_with_contracts_crossing', 'nvmeof_contracts_crossing'),
                            ('sui_nvmeof_with_contracts_crossing2', 'nvmeof_contracts_crossing2'),
                            ('sui_nvmeof_with_parallel_writes', 'nvmeof_parallel_writes')):
        combined = copy.deepcopy(REVIEWED_SPECS[selection])
        combined.update({key: copy.deepcopy(value) for key, value in specs['sui_nvmeof_compact'].items()
                         if key.startswith('sui_')})
        combined['suite'] = 'both'
        specs[name] = combined
    return specs


def reviewed_part(spec):
    value = copy.deepcopy(spec)
    for key in ('sui_object_key', 'sui_generation', 'sui_verification'):
        value.pop(key, None)
    if isinstance(value.get('sui_fixture'), dict):
        value['sui_fixture'].pop('key', None)
        value['sui_fixture'].pop('generation', None)
    return value


def reviewed_sui_spec(spec):
    return any(json.dumps(spec, sort_keys=True) == json.dumps(value, sort_keys=True)
               for value in REVIEWED_SUI_SPECS.values())


def extra_libraries(receipt):
    return sorted(set(receipt["sui_abi"]["needed"]) - set(receipt["abi"]["needed"]))


def kv_spec(spec):
    value = {key: copy.deepcopy(value) for key, value in spec.items() if not key.startswith("sui_")}
    value["suite"] = "kv"
    return value


def admit_sui_package(package_dir, receipt):
    if any(not receipt.get(key) for key in ('sui_sha256', 'verification_sha256', 'sui_abi', 'sui_revision')):
        raise ValueError('Sui package is missing')
    prepare.admit_rocky9_abi(receipt['sui_abi'])
    receipt['sui_extra_libraries'] = extra_libraries(receipt)
    if receipt['sui_revision'] != SUI_REVISION:
        raise ValueError('transferred Sui revision is not reviewed')
    for path_key, sha_key, filename in (
            ('sui_archive_path', 'sui_sha256', 'sui-package.tar.gz'),
            ('verification_archive_path', 'verification_sha256', 'verification.tar.gz')):
        path = Path(package_dir) / 'sui' / filename
        if not path.is_file():
            raise ValueError('Sui package is missing')
        if prepare.ci_prepare.sha(path) != receipt[sha_key]:
            raise ValueError('transferred Sui archive digest differs')
        receipt[path_key] = str(path.resolve())
    abi = prepare.admit_sui_archives(Path(receipt['sui_archive_path']), Path(receipt['verification_archive_path']))
    if receipt['sui_abi'] != abi:
        raise ValueError('transferred Sui ABI requirements differ from sealed executables')


def stage_sui(store, prefix, receipt, fixture_path):
    if fixture_path is None:
        raise ValueError('Sui suite requires a fixture path')
    if campaign.file_sha256(fixture_path) != FIXTURE_SHA256:
        raise ValueError('Sui fixture digest differs')
    key = prefix + 'sui-package.tar'
    generation = campaign.upload_file(store, receipt['sui_archive_path'], receipt['sui_sha256'], key)
    result = dict(sui_object_key=key, sui_generation=generation)
    for name, path, digest, filename in (
            ('sui_verification', receipt['verification_archive_path'], receipt['verification_sha256'], 'sui-verification.tar'),
            ('sui_fixture', fixture_path, FIXTURE_SHA256, 'sui-fixture.tar.gz')):
        key = prefix + filename
        result[name] = dict(key=key, generation=campaign.upload_file(store, path, digest, key), sha256=digest)
    return result


def _descriptor(value):
    return (isinstance(value, dict) and set(value) == {'key', 'generation', 'sha256'}
            and isinstance(value['key'], str) and bool(value['key'])
            and re.fullmatch(r'[1-9][0-9]*', str(value['generation'])) is not None
            and re.fullmatch(r'[0-9a-f]{64}', str(value['sha256'])) is not None)


def sui_complete(result, spec):
    sui = result.get('sui', {})
    cells = sui.get('cells', [])
    keys = ('transport', 'contract', 'owner_tier', 'owner_wal')
    return (sui.get('complete') is True and len(cells) == len(spec['sui_cells'])
            and [{key: cell.get(key) for key in keys} for cell in cells] == spec['sui_cells']
            and _descriptor(result.get('sui_archive')))


def terminal_export_sui(store, state, result, *, require_complete=True):
    from gcp_api import ApiError
    descriptor = result.get('sui_archive')
    key = state['run_id'] + '/results/sui-results.tar.gz'
    source = 'coordinator'
    if descriptor is None and not require_complete:
        source = 'object_metadata'
        try:
            head = store.head(key)
        except ApiError as error:
            if error.status == 404:
                return {}
            raise
    else:
        if not _descriptor(descriptor):
            raise ValueError('terminal Sui export descriptor invalid')
        if descriptor['key'] != key:
            raise ValueError('terminal Sui export provenance mismatch')
        head = store.head(key)
    generation = str(head.get('generation', ''))
    digest = head.get('metadata', {}).get('sha256')
    size = int(head.get('size', -1))
    if (head.get('name') != key or not re.fullmatch(r'[1-9][0-9]*', generation)
            or not re.fullmatch(r'[0-9a-f]{64}', str(digest))
            or not 0 < size <= 4 * 1024**3
            or (source == 'coordinator' and (generation != str(descriptor['generation'])
                                            or digest != descriptor['sha256']))):
        raise ValueError('terminal Sui export is not durably bound')
    return {key: dict(generation=generation, size=size, sha256=digest, descriptor=source)}


# The constants are available when imported lazily by the launcher gates.
REVIEWED_SUI_SPECS = _specs()
