"""Source-bound scalar/transport campaign; reuse the RAM workload/evidence parsers."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import metadata_transport_label

import checkpoint_async
import cluster_ram

BASELINE_SOURCE_SHA256 = '1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28'
BASELINE_BINARY_SHA256 = '0a96d28ece36649fec0333ac3f427c3d4b3dab0c9f21fb430b420b5f1dc85d8e'
CURRENT_SOURCE_SHA256 = '2e694cb0be9fe0738c217b75afb9fe0ba5d2eef170573bd95f092fa40e93dc57'
SCOPES = ('full', 'core', 'recovery')
FABRIC_VERSION = '2.6.0amzn1.0'
# EFA uses OFI_VERSION_DEF_PROV, not the public FI_VERSION(major, minor) encoding.
FABRIC_PROVIDER_VERSION = 206 << 16
FABRIC_STAGES = ('allocation', 'discovery', 'fabric', 'domain', 'cq', 'av', 'endpoint',
                 'bind_enable', 'register', 'address', 'peer_setup', 'first_progress')


def _identity(spec):
    spec = dict(spec)
    spec['family'] = spec['family'].replace('ram-', 'transport-', 1)
    spec['case_id'] = spec['case_id'].replace('ram-', 'transport-', 1)
    return spec


def qualification_cases():
    return [_identity(spec) for spec in cluster_ram.qualification_cases()]


def main_cases(scope='full'):
    if scope not in SCOPES:
        raise ValueError('unknown scalar transport campaign scope')
    return [_identity(spec) for spec in cluster_ram.main_cases('core', repetitions=3)] if scope != 'recovery' else []


def recovery_cases():
    result = []
    for transport in ('tcp', 'efa'):
        for old in cluster_ram.recovery_cases():
            if old['rep'] != 0:
                continue
            spec = _identity(old)
            spec['transport'] = transport
            spec['family'] = f"transport-recovery-{spec['restart']}-t{spec['tail_batches']}-{transport}"
            spec['case_id'] = f"{spec['family']}-{spec['build']}-r0"
            result.append(spec)
    return result


def plan(scope='full'):
    if scope not in SCOPES:
        raise ValueError('unknown scalar transport campaign scope')
    return qualification_cases() + main_cases(scope) + (recovery_cases() if scope != 'core' else [])


def expected_cases(scope='full'):
    return {spec['case_id'] + '-' + mode:
            (spec['family'], spec['build'], spec['value'], spec['batch'], spec['window'], spec['rep'], mode, spec['case_id'])
            for spec in plan(scope) for mode in spec['modes']}


# Keep native controls, sampling and durable workload flags identical to RAM.
flags = cluster_ram.flags


def validate_build_identity(root, label):
    import analyze_cloud as audit
    root = Path(root)
    if label not in ('baseline', 'current'):
        raise ValueError('unplanned scalar transport build')
    receipt = audit.read_json(root / f'cloud-build-{label}.json')
    source = BASELINE_SOURCE_SHA256 if label == 'baseline' else CURRENT_SOURCE_SHA256
    if (receipt.get('label') != label or receipt.get('source_tree_sha256') != source
            or receipt.get('features') != 'experimental-fabric,test-utils'
            or receipt.get('libfabric_version') != FABRIC_VERSION
            or any(not audit.is_sha256(receipt.get(key)) for key in
                   ('binary_sha256', 'local_binary_sha256', 'source_manifest_sha256', 'libfabric_sha256'))):
        raise ValueError('scalar transport source/binary/provider identity differs')
    if label == 'baseline' and receipt['local_binary_sha256'] != BASELINE_BINARY_SHA256:
        raise ValueError('baseline differs from the preserved October 11 executable')
    pair = audit.read_json(root / 'cloud-build-pair-verified.json')
    if pair.get('binary_sha256', {}).get(label) != receipt['binary_sha256']:
        raise ValueError('scalar transport executable differs from verified build pair')
    return receipt


def validate_endpoint_profile(counters):
    """Runtime enforces EFA protocol/config gates; archived actual geometry must agree."""
    for key in ('requested_tx', 'requested_rx', 'selected_tx', 'selected_rx'):
        if counters.get('fabric_open_' + key + '_max') != 128:
            raise ValueError('EFA endpoint requested/selected TX/RX geometry must be 128')
    if counters.get('fabric_open_provider_version_max') != FABRIC_PROVIDER_VERSION:
        raise ValueError('EFA endpoint requires pinned provider 2.6')
    if checkpoint_async.positive(counters.get('fabric_open_mtu_max')) < 4096:
        raise ValueError('EFA endpoint MTU below supported packet bound')
    times = [checkpoint_async.positive(counters.get('fabric_open_' + stage + '_worker_sum_ns'), True)
             for stage in FABRIC_STAGES]
    if not sum(times):
        raise ValueError('missing measured EFA endpoint setup stages')


def validate_lifecycle(metrics, spec, mode):
    if mode != 'tcp' or spec['build'] != 'current':
        return
    for phase in ('reopen_reused_transport', 'reopen_reused_get_audit'):
        if ('summary', phase, 'logical_operations') not in metrics:
            raise ValueError('missing separate transport-reused reopen/audit')
    if (metrics['summary', 'reopen_reused_transport', 'logical_operations'] != 1 or
            metrics['summary', 'reopen_reused_get_audit', 'logical_operations'] != spec['batches'] * spec['batch']):
        raise ValueError('transport-reused reopen lacks complete value/tombstone audit')
    for phase in ('open_recovery', 'reopen_recovery', 'reopen_reused_transport_recovery'):
        counters = {key: value for (kind, p, key), value in metrics.items() if kind == 'stage' and p == phase}
        reused = phase == 'reopen_reused_transport_recovery'
        connections = 5 * (1 + spec['lanes'])
        if (counters.get('created_connections') != (0 if reused else connections)
                or counters.get('reused_connections') != (connections if reused else 0)
                or counters.get('checkpoint_loaded') != int(phase != 'open_recovery')):
            raise ValueError('cold/reused transport lifetime evidence differs')
        for key in ('transport_checkout_ns', 'read_lane_wait_ns', 'read_lane_setup_ns', 'tls_config_ns'):
            checkpoint_async.positive(counters.get(key), True)
        if spec['transport'] == 'efa' and not reused:
            validate_endpoint_profile(counters)
        elif any(key.startswith('fabric_open_') for key in counters):
            raise ValueError('TCP/reused transport must not invent new EFA endpoint creation')


def validate_recovery_json(row, spec, action):
    cluster_ram.validate_recovery_json(row, spec, action)
    if spec['build'] != 'current':
        return
    stats = row['recovery_stats']
    if stats.get('created_connections') != 5 * (1 + spec['lanes']) or stats.get('reused_connections') != 0:
        raise ValueError('process recovery must create fresh transports, never reuse them')
    for key in ('transport_checkout_ns', 'read_lane_wait_ns'):
        checkpoint_async.positive(stats.get(key), True)
    if spec['transport'] == 'efa':
        validate_endpoint_profile(stats)
    elif any(key.startswith('fabric_open_') for key in stats):
        raise ValueError('TCP recovery must not invent EFA endpoint creation')


def read_cloud_case(root, name, spec, mode):
    import analyze_cloud as audit
    root = Path(root)
    cfg = audit.read_json(root / 'campaign-config.json')
    if (cfg.get('campaign') != 'scalar-transport' or spec not in plan(cfg.get('checkpoint_scope', 'full'))
            or mode not in spec['modes'] or name != spec['case_id'] + '-' + mode):
        raise ValueError('unplanned scalar transport case or mode')
    receipt = validate_build_identity(root, spec['build'])
    preparation = audit.read_json(root / (spec['case_id'] + '-prepare.command.json'))
    if preparation.get('binary_sha256') != receipt['binary_sha256']:
        raise ValueError('preparation executable differs from source-bound build')
    if mode == 'recovery':
        for action in ('seed', 'reopen'):
            process = audit.read_json(root / f'{name}-{action}-process.command.json')
            if process.get('binary_sha256') != receipt.get('recovery_binary_sha256') or not audit.is_sha256(process.get('binary_sha256')):
                raise ValueError('recovery process executable differs from source-bound build')
        return audit.read_recovery_case(root, name, spec, audit.scalar_transport)
    process = audit.read_json(root / (name + '-process.command.json'))
    if process.get('binary_sha256') != receipt['binary_sha256']:
        raise ValueError('benchmark process executable differs from source-bound build')
    metrics = cluster_ram.read_cloud_case(root, name, spec, mode)
    expected_transport = (metadata_transport_label(spec['transport'])
                          if spec['build'] == 'current' else spec['transport'])
    for row in cluster_ram.read_csv(root / name / 'summary.csv'):
        transport = row.get('transport', spec['transport'] if spec['build'] == 'baseline' else None)
        if row['durability'] != 'value_wal_file_and_directory_barrier' or transport != expected_transport:
            raise ValueError('scalar transport durability/transport identity differs')
    validate_lifecycle(metrics, spec, mode)
    return metrics
