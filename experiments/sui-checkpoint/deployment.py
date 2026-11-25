"""Exact per-database RF1 deployment, using the engine's existing node YAML protocol."""
import hashlib
import json
from pathlib import Path
import subprocess
import uuid
import yaml
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from resident_config import (resident_read_config, validate_destination_total,
                             software_fabric_environment)
from portable_transport import (validate_transport, select_transports,
                                is_trusted, is_software, is_fabric, transport_for_case)

MIB = 1024 * 1024
HARDWARE_TRANSPORTS = ('efa', 'efa-direct', 'efa-native')
SOFTWARE_TRANSPORTS = ('fabric-tcp', 'fabric-tcp-native')
TRUSTED_TRANSPORTS = ('efa-native', 'fabric-tcp-native')
TRANSPORTS = ('tcp', *HARDWARE_TRANSPORTS, *SOFTWARE_TRANSPORTS)
LIVE_TRANSPORTS = ('tcp', 'rdma')
DEFAULT_QUEUE_GEOMETRY = dict(io_queues=4, queue_size=128, poll_queues=0)
OWNER_MAX_PENDING_COMPLETIONS = 4096
LABEL_TRANSPORTS = {
    'tcp': {'kind': 'tcp'},
    'tcp-trusted': {'kind': 'tcp-trusted'},
    'efa': {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'mutual-tls'},
    'efa-direct': {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'mutual-tls'},
    'efa-native': {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'},
    'fabric-tcp': {'kind': 'ofi', 'fabric': {'backend': 'software-tcp'}, 'security': 'mutual-tls'},
    'fabric-tcp-native': {'kind': 'ofi', 'fabric': {'backend': 'software-tcp'}, 'security': 'trusted'},
}


def canonical_transport(value):
    """Accept a canonical Transport object or the harness's driver labels."""
    if isinstance(value, str):
        value = LABEL_TRANSPORTS.get(value, value)
    return validate_transport(value)


def select_owner_transports(client, owner_transports, count):
    """Select host-local owner transports through the shared protocol validator."""
    return select_transports(canonical_transport(client), owner_transports, count)


def process_environment(environment, *, transport):
    transport = canonical_transport(transport)
    return software_fabric_environment(environment, software=is_software(transport))


def dump(path, value):
    path.write_text(yaml.safe_dump(value, sort_keys=False))


def certificates(root, owners):
    if type(owners) is not int or not 1 <= owners <= 16:
        raise ValueError('owner count must be 1..16')
    root.mkdir(mode=0o700)
    def openssl(*args):
        subprocess.run(['openssl', *map(str, args)], stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, check=True, timeout=30)
    openssl('req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
            '-sha256', '-days', '2', '-subj', '/CN=sui-checkpoint-owned-ca',
            '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
            '-keyout', root / 'ca.key', '-out', root / 'ca.pem')
    openssl('x509', '-in', root / 'ca.pem', '-outform', 'DER', '-out', root / 'ca.der')
    for index, name in enumerate(['authority'] + [f'node-{i}' for i in range(owners)]):
        (root / f'{name}.ext').write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n'
            f'extendedKeyUsage={"clientAuth" if name == "authority" else "serverAuth"}\nsubjectAltName=DNS:{name}\n')
        openssl('req', '-new', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
                '-subj', f'/CN={name}', '-keyout', root / f'{name}.pem', '-out', root / f'{name}.csr')
        openssl('x509', '-req', '-sha256', '-days', '2', '-in', root / f'{name}.csr',
                '-CA', root / 'ca.pem', '-CAkey', root / 'ca.key', '-set_serial', index + 2,
                '-extfile', root / f'{name}.ext', '-out', root / f'{name}.crt')
        openssl('x509', '-in', root / f'{name}.crt', '-outform', 'DER', '-out', root / f'{name}.der')
        openssl('pkcs8', '-topk8', '-nocrypt', '-in', root / f'{name}.pem', '-outform', 'DER', '-out', root / f'{name}.pk8')
    for path in root.iterdir():
        path.chmod(0o600)


def schemas(directory, expected_backend=None):
    result = {}
    for manifest in sorted(directory.rglob('manifest.json')):
        entry = json.loads(manifest.read_text())
        if expected_backend is not None and entry.get('backend') != expected_backend:
            raise ValueError('selected database backend differs; native fallback is forbidden')
        namespace = entry['namespace']
        relative = Path(namespace)
        if relative.is_absolute() or '..' in relative.parts or namespace in result:
            raise ValueError('unsafe or duplicate namespace')
        result[namespace] = json.loads((manifest.parent / 'shape.json').read_text())
    if not 1 <= len(result) <= 16:
        raise ValueError('schema must describe 1..16 selected Tidehunter databases')
    return result

def live_owner_identity(live, namespace, database, partition):
    """One stable initiator identity per database/partition, not per VM."""
    base = uuid.UUID(bytes=bytes(live['host_id']))
    owner = uuid.uuid5(base, f'{namespace}:{database.hex}:{partition}')
    return dict(host_nqn='nqn.2014-08.org.nvmexpress:uuid:' + str(owner),
                host_id=list(owner.bytes))


def live_deployment(live_nvme, geometry):
    """Validate or create the coordinator's live NVMe-oF initiator identity."""
    if live_nvme is None:
        identity = uuid.uuid4()
        value = dict(host_nqn='nqn.2014-08.org.nvmexpress:uuid:' + str(identity),
                     host_id=list(identity.bytes))
        geometry = dict(DEFAULT_QUEUE_GEOMETRY if geometry is None else geometry)
    else:
        if not isinstance(live_nvme, dict):
            raise ValueError('live NVMe-oF deployment must be an object')
        if set(live_nvme) - {'host_nqn', 'host_id', 'queue_geometry'}:
            raise ValueError('live NVMe-oF deployment carries an unknown field')
        value = dict(host_nqn=live_nvme.get('host_nqn'), host_id=live_nvme.get('host_id'))
        geometry = dict(DEFAULT_QUEUE_GEOMETRY if geometry is None
                       else live_nvme.get('queue_geometry', geometry))
    valid = (isinstance(value['host_id'], list) and len(value['host_id']) == 16
             and all(type(x) is int and 0 <= x <= 255 for x in value['host_id'])
             and any(value['host_id'])
             and isinstance(value['host_nqn'], str)
             and value['host_nqn'].startswith('nqn.')
             and len(value['host_nqn']) <= 223
             and isinstance(geometry, dict)
             and set(geometry) == {'io_queues', 'queue_size', 'poll_queues'}
             and all(type(x) is int for x in geometry.values()))
    if not valid:
        raise ValueError('invalid live NVMe-oF deployment')
    value['queue_geometry'] = geometry
    return value


def live_transport_for(transport, live_transport):
    if live_transport is not None:
        if live_transport not in LIVE_TRANSPORTS:
            raise ValueError('live NVMe-oF transport must be tcp or rdma')
        return live_transport
    transport = canonical_transport(transport)
    if transport['kind'] == 'ofi' and transport['fabric']['backend'] == 'verbs':
        return 'rdma'
    return 'tcp'


def prepare(directory, schema, db_root, *, shape_yamls, hosts, port, transport, coordinator_cache,
            shard_cache=0, guest_root=None, write_profile_limit=0, resident_reads=None,
            owners=None, owner_transports=None, owner_tier=None, owner_wal='harness',
            live_nvme=None, live_transport=None, live_geometry=None):
    if owner_tier not in (None, 'native', 'synchronous'):
        raise ValueError('owner tier must be native or synchronous')
    if owner_wal not in ('harness', 'engine_default'):
        raise ValueError('owner WAL layout must be harness or engine_default')
    if type(write_profile_limit) is not int or not 0 <= write_profile_limit <= 65536:
        raise ValueError('write profile limit must be an integer 0..65536')
    owners = len(hosts) if owners is None else owners
    if type(owners) is not int or not 1 <= owners <= 16:
        raise ValueError('owner count must be an integer 1..16')
    if len(hosts) != owners:
        raise ValueError(f'exactly {owners} owner hosts are required')
    if type(port) is not int or not 1024 <= port or port + 16 * owners > 65535:
        raise ValueError('control port and sixteen namespace blocks must fit u16')
    live_ceiling = 4420 + 64 * owners
    if 1000 <= port < 1032 + 64 * owners or 4420 <= port < live_ceiling:
        raise ValueError('control port overlaps the live target port or service range')
    transport = canonical_transport(transport)
    owner_transports = select_owner_transports(transport, owner_transports, owners)
    if is_software(transport) and (guest_root is not None or any(host != '127.0.0.1' for host in hosts)):
        raise ValueError('software fabric is loopback-only in the native engine')
    if is_fabric(transport) and not is_software(transport) and guest_root is None:
        raise ValueError('hardware fabric needs explicit external mode')
    if (guest_root is not None or any(host != '127.0.0.1' for host in hosts)) and len(set(hosts)) != owners:
        raise ValueError(f'exactly {owners} distinct external hosts are required')
    if set(shape_yamls) != set(schema):
        raise ValueError('canonical engine YAML must cover every selected namespace')
    resident = resident_read_config(resident_reads)
    if resident['destination_slots'] and not is_trusted(transport):
        raise ValueError('resident_reads requires an explicit trusted-native transport')
    if resident['require_device_read'] and is_software(transport):
        raise ValueError('software fabric cannot qualify hardware device reads')
    validate_destination_total(resident, len(schema))
    live = live_deployment(live_nvme, live_geometry)
    live_transport = live_transport_for(transport, live_transport)
    tier = owner_tier or 'native'
    directory.mkdir(mode=0o700)
    if not is_trusted(transport):
        certificates(directory / 'tls', owners)
    runtime = guest_root or directory
    tls_root = runtime / 'tls'

    def tls(name, base, selected):
        if is_trusted(selected):
            return dict(transport=selected, ca='', cert='', key='', server_name=name)
        return dict(transport=selected, ca=str(base / 'ca.der'), cert=str(base / f'{name}.der'),
                    key=str(base / f'{name}.pk8'), server_name=name)

    databases, nodes = {}, []
    for number, (namespace, shape) in enumerate(schema.items()):
        database = uuid.uuid4()
        slug = hashlib.sha256(namespace.encode()).hexdigest()[:16]
        storage = dict(partitions=list(range(owners)),
            batch_limits=dict(max_operations=65536, max_encoded_bytes=8*MIB),
            group_limits=dict(max_batches=64, max_encoded_bytes=12*MIB),
            fragment_bytes=(128 if owner_wal == 'engine_default' else 16)*MIB,
            wal_file_bytes=(10240 if owner_wal == 'engine_default' else 256)*MIB,
            max_batches_per_partition=1000000,
            max_reference_metadata_bytes=256*MIB)
        owner_identities = {partition: live_owner_identity(live, namespace, database, partition)
                            for partition in range(owners)}
        endpoints = []
        for partition, host in enumerate(hosts):
            address = f'{host}:{port + number * owners + partition}'
            node_dir = directory / slug / f'node-{partition}'
            node_dir.mkdir(parents=True)
            runtime_node = runtime / slug / f'node-{partition}'
            base = 64 * partition
            host_config = dict(
                root=str(runtime_node / 'storage'), max_databases=16,
                live_target_pool=dict(address=host, transport=live_transport,
                    public_port_base=1000 + base, public_service_base=4420 + base,
                    commissioning_port_base=1016 + base,
                    commissioning_service_base=4436 + base),
                wal_member_read_policy='async', wal_prewrite=False,
                max_device_reads_per_status=128,
                durability=dict(max_unsynced_age_ms=None,
                                max_pending_completions=OWNER_MAX_PENDING_COMPLETIONS))
            if is_trusted(owner_transports[partition]):
                # The trusted transports carry no client CA, so the owner names
                # the one coordinator principal it admits.
                host_config['trusted_coordinator_id'] = [0x7d] * 32
            launch = dict(partition=partition, host=host_config,
                tls=tls(f'node-{partition}', tls_root, owner_transports[partition]),
                limits=dict(max_frame_bytes=16*MIB, max_connections=16, max_reads=256,
                    recovery_page_batches=128, io_timeout=dict(secs=60, nanos=0), polling='adaptive'),
                listen=address, ready_file=str(runtime_node / 'ready'),
                stop_file=str(runtime_node / 'stop'), launch_nonce='',
                durability=dict(tier=tier))
            dump(node_dir / 'launch.yaml', launch)
            nodes.append(dict(namespace=namespace, partition=partition, host=host,
                launch=str(node_dir / 'launch.yaml'), ready=str(runtime_node / 'ready'),
                stop=str(runtime_node / 'stop'),
                owner_identity=owner_identities[partition],
                live_target_pool=host_config['live_target_pool']))
            endpoints.append(dict(partition=partition, address=address,
                                  server_name=f'node-{partition}'))
        client_tls = tls('authority', directory / 'tls', transport)
        if resident['destination_slots']:
            client_tls['resident_reads'] = resident
        write_frame = (storage['group_limits']['max_encoded_bytes'] + 8 + 4095) & ~4095
        recovery_bytes = 2 * (storage['batch_limits']['max_encoded_bytes'] + 8) + 3 * 4095
        live_bytes = (max(write_frame + 4095, recovery_bytes) + 4095) & ~4095
        databases[namespace] = dict(storage=storage, endpoints=endpoints, tls=client_tls,
            timeout=dict(secs=30, nanos=0), max_admitted_batches=storage['max_batches_per_partition'],
            max_pending_batches=128, max_pending_bytes=128*MIB, pipelined_writes=True,
            write_profile_limit=write_profile_limit,
            read_cache_bytes=coordinator_cache,
            read_memory_allowance_bytes=128*MIB+2*coordinator_cache,
            read_lanes=2, max_pending_reads=256, max_pending_read_bytes=64*MIB,
            live_nvme=dict(host_nqn=live['host_nqn'], host_id=live['host_id'],
                owner_identities=owner_identities,
                max_inflight_requests=32, max_inflight_bytes=live_bytes,
                connect_timeout_ms=30000, io_timeout_ms=30000,
                leased_durability=tier, inline_reads=True,
                queue_geometry=live['queue_geometry']))
    config = dict(root=str(db_root), databases=databases)
    (directory / 'config.json').write_text(json.dumps(config, indent=2)+'\n')
    (directory / 'nodes.json').write_text(json.dumps(nodes, indent=2)+'\n')
    return config, nodes
