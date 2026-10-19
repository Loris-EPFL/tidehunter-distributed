"""Bounded checkpoint cells and fail-closed EFA hardware activity evidence."""
from pathlib import Path
import re
import time


def matrix_plan(cfg):
    transports = cfg.get('transports')
    repetitions = cfg.get('repetitions')
    if ('transport' in cfg or not isinstance(transports, list) or not transports
            or any(value not in ('tcp', 'efa', 'efa-direct') for value in transports)
            or len(set(transports)) != len(transports)
            or type(repetitions) is not int or not 1 <= repetitions <= 4):
        raise ValueError('require distinct transports [tcp, efa and/or efa-direct] and repetitions 1..4; singular transport is obsolete')
    limits = cfg.get('read_profile_limits')
    if ('read_profile_limit' in cfg or not isinstance(limits, list)
            or len(limits) != repetitions
            or any(type(limit) is not int or not 0 <= limit <= 65536 for limit in limits)):
        raise ValueError('require read_profile_limits with one integer 0..65536 per repetition; singular limit is obsolete')
    # Historical retained campaigns predate write sampling; omission disables it.
    write_limits = cfg.get('write_profile_limits', [0] * repetitions)
    if ('write_profile_limit' in cfg or not isinstance(write_limits, list)
            or len(write_limits) != repetitions
            or any(type(limit) is not int or not 0 <= limit <= 65536 for limit in write_limits)):
        raise ValueError('require write_profile_limits with one integer 0..65536 per repetition')
    native_limits = cfg.get('native_write_profile_limits', [0] * repetitions)
    if ('native_write_profile_limit' in cfg or not isinstance(native_limits, list)
            or len(native_limits) != repetitions
            or any(type(limit) is not int or not 0 <= limit <= 65536 for limit in native_limits)):
        raise ValueError('require native_write_profile_limits with one integer 0..65536 per repetition')
    cache_budgets = {}
    for name, default in (('coordinator_cache_mib', 8), ('shard_cache_mib', 0)):
        budgets = cfg.get(name + '_by_repetition', [cfg.get(name, default)] * repetitions)
        if (not isinstance(budgets, list) or len(budgets) != repetitions
                or any(type(value) is not int or not 0 <= value <= 128 for value in budgets)):
            raise ValueError('cache schedules require one integer 0..128 MiB per repetition')
        cache_budgets[name] = budgets
    return [dict(cell_id=f'r{repeat + 1:02d}-{transport}', repetition=repeat + 1,
                 transport=transport, ordinal=ordinal, read_profile_limit=limits[repeat],
                 write_profile_limit=write_limits[repeat], native_write_profile_limit=native_limits[repeat],
                 **{name: budgets[repeat] for name, budgets in cache_budgets.items()})
            for ordinal, (repeat, transport) in enumerate(
                ( (repeat, transport) for repeat in range(repetitions)
                  for transport in (transports if repeat % 2 == 0 else transports[::-1]) ))]


def check_cell(cell_id, transport):
    if (transport not in ('tcp', 'efa', 'efa-direct') or not isinstance(cell_id, str)
            or not re.fullmatch(r'r0[1-4]-' + transport, cell_id)):
        raise ValueError('invalid checkpoint cell identity')


def validate_provider(text, transport=None):
    # Omitted transport admits retained pre-fabric-name evidence only. New
    # executions always name their transport and require exact negotiation.
    if transport not in (None, 'efa', 'efa-direct'):
        raise ValueError('hardware provider proof requires an EFA transport')
    providers = re.findall(r'^\s*(?:provider|prov_name):\s*(\S+)\s*$', text, re.MULTILINE)
    endpoints = re.findall(r'^\s*type:\s*(FI_EP_\S+)\s*$', text, re.MULTILINE)
    fabrics = re.findall(r'^\s*fabric:\s*(\S+)\s*$', text, re.MULTILINE)
    fabrics += re.findall(r'fi_fabric_attr:\s*\n\s*name:\s*(\S+)', text)
    if (not providers or any(provider != 'efa' for provider in providers)
            or not endpoints or any(endpoint != 'FI_EP_RDM' for endpoint in endpoints)):
        raise ValueError('requires hardware efa RDM provider, not software/layered fallback')
    if ((transport is not None and (len(fabrics) != len(providers) or len(endpoints) != len(providers)))
            or any(fabric != (transport or 'efa') for fabric in fabrics)):
        raise ValueError('negotiated fabric differs from requested transport; no fallback')


def efa_snapshot(identity, base=Path('/sys/class/infiniband')):
    devices = {}
    for device in sorted(base.iterdir()):
        driver = device / 'device/driver'
        if not driver.is_symlink() or driver.resolve().name != 'efa':
            continue
        counters = {}
        for directory in sorted(device.glob('ports/*/hw_counters')):
            for name in ('send_bytes', 'recv_bytes'):
                path = directory / name
                value = int(path.read_text().strip())
                if value < 0:
                    raise ValueError('negative EFA hardware counter')
                counters[str(path.relative_to(device))] = value
        if not counters:
            raise ValueError('EFA device has no readable send/receive byte counters')
        devices[device.name] = dict(driver='efa', counters=counters)
    if not devices:
        raise ValueError('no hardware EFA device with readable data counters')
    return dict(identity=identity, unix_ns=time.time_ns(), devices=devices)


def efa_proof(before, after, identity):
    if before.get('identity') != identity or after.get('identity') != identity:
        raise ValueError('EFA snapshot cell/host/source identity differs')
    if not 0 < before['unix_ns'] < after['unix_ns']:
        raise ValueError('EFA snapshot interval is not ordered')
    if not before.get('devices') or before['devices'].keys() != after.get('devices', {}).keys():
        raise ValueError('EFA hardware inventory missing or changed')
    deltas, tx, rx = {}, 0, 0
    for name, device in before['devices'].items():
        end = after['devices'][name]
        if device.get('driver') != 'efa' or end.get('driver') != 'efa':
            raise ValueError('software-only EFA evidence')
        counters = device['counters']
        if not counters or counters.keys() != end['counters'].keys():
            raise ValueError('EFA counter coverage changed')
        delta = {}
        for key, value in counters.items():
            final = end['counters'][key]
            if type(value) is not int or type(final) is not int or value < 0 or final < value:
                raise ValueError('EFA counters unreadable or reset')
            delta[key] = final - value
            if Path(key).name == 'send_bytes':
                tx += delta[key]
            elif Path(key).name == 'recv_bytes':
                rx += delta[key]
        deltas[name] = delta
    if tx <= 0 or rx <= 0:
        raise ValueError('EFA requires positive hardware send AND receive data bytes on every host')
    return dict(identity=identity, before=before, after=after, deltas=deltas,
                send_bytes=tx, recv_bytes=rx,
                scope='host-wide EFA hardware activity during this cell; not pure-wire timing or process attribution')


def validate_efa_proof(proof, identity):
    if proof != efa_proof(proof['before'], proof['after'], identity):
        raise ValueError('EFA proof totals/identity differ from raw snapshots')


def validate_matrix(cfg, cells):
    plan = matrix_plan(cfg)
    if len(cells) != len(plan):
        raise ValueError('matrix misses requested cells')
    hashes, namespace_ids = set(), set()
    namespaces = None
    for planned, cell in zip(plan, cells):
        if (type(cell.get('read_profile_limit')) is not int
                or type(cell.get('write_profile_limit', 0)) is not int
                or type(cell.get('native_write_profile_limit', 0)) is not int
                or type(cell.get('coordinator_cache_mib')) is not int
                or type(cell.get('shard_cache_mib')) is not int):
            raise ValueError('matrix cell profile limits and cache budgets must be integers')
        if any(cell.get(key, 0 if key in ('write_profile_limit', 'native_write_profile_limit') else None) != value
               for key, value in planned.items()) or cell.get('complete') is not True:
            raise ValueError('matrix cell missing, reordered, mismatched or incomplete')
        if cell.get('build_manifest_sha256') != cfg['build_manifest_sha256'] or cell.get('binaries') != cfg['binaries']:
            raise ValueError('matrix cell source differs')
        for key in ('fixture_bundle', 'verification_bundle', 'source_archive_sha256', 'source_manifest_sha256'):
            if cell.get(key) != cfg[key]:
                raise ValueError('matrix immutable input differs: ' + key)
        for key in ('replay_evidence_sha256', 'resources_sha256'):
            if not re.fullmatch(r'[0-9a-f]{64}', cell.get(key, '')):
                raise ValueError('matrix misses paired replay/resource evidence')
        identities = cell.get('namespace_ids', {})
        if not identities or (namespaces is not None and set(identities) != namespaces):
            raise ValueError('matrix namespace/schema coverage differs')
        namespaces = set(identities)
        for identity in identities.values():
            if (not isinstance(identity, list) or len(identity) != 16
                    or any(type(value) is not int or not 0 <= value <= 255 for value in identity)
                    or tuple(identity) in namespace_ids):
                raise ValueError('matrix reuses namespace database identity')
            namespace_ids.add(tuple(identity))
        digest = cell.get('config_sha256', '')
        if not re.fullmatch(r'[0-9a-f]{64}', digest) or digest in hashes:
            raise ValueError('matrix reuses an old namespace/config identity')
        hashes.add(digest)
        expected_hosts = {cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])}
        if set(cell.get('node_archives', {})) != {node['instance_id'] for node in cfg['nodes']}:
            raise ValueError('matrix misses node archives')
        for instance_id, receipt in cell['node_archives'].items():
            partition = next(node['partition'] for node in cfg['nodes'] if node['instance_id'] == instance_id)
            key = f"{cfg['s3_prefix']}/results/cells/{cell['cell_id']}/storage/{partition}/final/evidence.tar.gz"
            if receipt.get('key') != key or receipt.get('bucket') != cfg['bucket'] or not re.fullmatch(r'[0-9a-f]{64}', receipt.get('sha256', '')):
                raise ValueError('matrix node archive belongs to another cell')
        proofs = cell.get('efa', {})
        if set(proofs) != (expected_hosts if cell['transport'] in ('efa', 'efa-direct') else set()):
            raise ValueError('matrix EFA proof misses a host or contaminates TCP control')
        for instance_id, proof in proofs.items():
            identity = dict(run_id=cfg['run_id'], cell_id=cell['cell_id'], instance_id=instance_id,
                            config_sha256=digest, build_manifest_sha256=cfg['build_manifest_sha256'])
            validate_efa_proof(proof, identity)


def validate_placement(cfg, instances, groups):
    expected = {cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])}
    if len(instances) != 6 or {instance['InstanceId'] for instance in instances} != expected:
        raise ValueError('placement evidence misses an exact fleet host')
    if (len(groups) != 1 or groups[0].get('GroupName') != cfg['placement_group']
            or groups[0].get('Strategy') != 'cluster' or groups[0].get('State') != 'available'
            or not groups[0].get('GroupId')):
        raise ValueError('owned named placement group is not an available cluster')
    for instance in instances:
        placement = instance.get('Placement', {})
        if (placement.get('AvailabilityZone') != cfg['availability_zone']
                or placement.get('GroupName') != cfg['placement_group']
                or placement.get('GroupId') != groups[0]['GroupId']
                or instance.get('InstanceType') != cfg['instance_type']):
            raise ValueError('fleet hardware/zone/cluster placement differs')
        if any(transport in ('efa', 'efa-direct') for transport in cfg['transports']) and not any(
                nic.get('InterfaceType') == 'efa' and nic.get('Status') == 'in-use'
                and nic.get('Attachment', {}).get('Status') == 'attached'
                for nic in instance.get('NetworkInterfaces', [])):
            raise ValueError('EFA matrix host lacks an attached EFA interface')
