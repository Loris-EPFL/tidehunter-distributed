"""Exact per-database RF1 deployment, using the engine's existing node YAML protocol."""
import hashlib
import json
from pathlib import Path
import subprocess
import uuid
import yaml
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from resident_config import resident_read_config, validate_destination_total

MIB = 1024 * 1024
HARDWARE_TRANSPORTS = ('efa', 'efa-direct', 'efa-native')
SOFTWARE_TRANSPORTS = ('fabric-tcp', 'fabric-tcp-native')
TRUSTED_TRANSPORTS = ('efa-native', 'fabric-tcp-native')
TRANSPORTS = ('tcp', *HARDWARE_TRANSPORTS, *SOFTWARE_TRANSPORTS)


def dump(path, value):
    path.write_text(yaml.safe_dump(value, sort_keys=False))


def certificates(root):
    root.mkdir(mode=0o700)
    def openssl(*args):
        subprocess.run(['openssl', *map(str, args)], stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, check=True, timeout=30)
    openssl('req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
            '-sha256', '-days', '2', '-subj', '/CN=sui-checkpoint-owned-ca',
            '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
            '-keyout', root / 'ca.key', '-out', root / 'ca.pem')
    openssl('x509', '-in', root / 'ca.pem', '-outform', 'DER', '-out', root / 'ca.der')
    for index, name in enumerate(['authority'] + [f'node-{i}' for i in range(5)]):
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


def prepare(directory, schema, db_root, *, shape_yamls, hosts, port, transport, coordinator_cache, shard_cache, guest_root=None, write_profile_limit=0, resident_reads=None):
    if type(write_profile_limit) is not int or not 0 <= write_profile_limit <= 65536:
        raise ValueError('write profile limit must be an integer 0..65536')
    if transport not in TRANSPORTS or len(hosts) != 5:
        raise ValueError('a supported transport and exactly five hosts are required')
    if transport in SOFTWARE_TRANSPORTS and (guest_root is not None or any(host != '127.0.0.1' for host in hosts)):
        raise ValueError('software fabric is loopback-only in the native engine; use tcp or EFA for external hosts')
    if transport in HARDWARE_TRANSPORTS and guest_root is None:
        raise ValueError('hardware EFA needs explicit external mode')
    if set(shape_yamls) != set(schema):
        raise ValueError('canonical engine YAML must cover every selected namespace')
    resident = resident_read_config(resident_reads)
    if resident['destination_slots'] and transport not in TRUSTED_TRANSPORTS:
        raise ValueError('resident_reads requires an explicit trusted-native transport')
    if resident['require_device_read'] and transport in SOFTWARE_TRANSPORTS:
        raise ValueError('software fabric cannot qualify hardware device reads')
    validate_destination_total(resident, len(schema))
    directory.mkdir(mode=0o700)
    if transport not in TRUSTED_TRANSPORTS:
        certificates(directory / 'tls')
    runtime = guest_root or directory
    tls_root = runtime / 'tls'
    def tls(name, base):
        if transport in TRUSTED_TRANSPORTS:
            return dict(transport=transport, ca='', cert='', key='', server_name=name)
        return dict(transport=transport, ca=str(base / 'ca.der'), cert=str(base / f'{name}.der'),
                    key=str(base / f'{name}.pk8'), server_name=name)
    databases, nodes = {}, []
    for number, (namespace, shape) in enumerate(schema.items()):
        identity = list(uuid.uuid4().bytes)
        slug = hashlib.sha256(namespace.encode()).hexdigest()[:16]
        storage = dict(database=identity, partitions=list(range(5)),
            batch_limits=dict(max_operations=65536, max_encoded_bytes=8*MIB),
            group_limits=dict(max_batches=64, max_encoded_bytes=12*MIB),
            fragment_bytes=16*MIB, wal_file_bytes=256*MIB, max_batches_per_partition=1000000,
            max_reference_metadata_bytes=256*MIB, max_epochs=32)
        endpoints = []
        for partition, host in enumerate(hosts):
            address = f'{host}:{port + number * 5 + partition}'
            node_dir = directory / slug / f'node-{partition}'
            node_dir.mkdir(parents=True)
            runtime_node = runtime / slug / f'node-{partition}'
            inventory = dict(database=identity, partitions=list(range(5)), epoch=1, segment_generation=1)
            partition_config = dict(inventory=inventory, partition=partition,
                batch_limits=storage['batch_limits'], group_limits=storage['group_limits'],
                fragment_bytes=storage['fragment_bytes'], wal_file_bytes=storage['wal_file_bytes'],
                max_batches=storage['max_batches_per_partition'], max_reference_metadata_bytes=storage['max_reference_metadata_bytes'])
            node = dict(root=str(runtime_node / 'storage'), partition=partition_config,
                shape_yaml=shape_yamls[namespace], max_epochs=32, read_workers=2,
                wal_member_read='async', read_resident_first=True, read_cache_bytes=shard_cache,
                read_memory_allowance_bytes=128*MIB + 2*shard_cache,
                read_max_inflight_requests=32, read_max_inflight_bytes=64*MIB, read_max_device_reads=128)
            if resident['destination_slots']:
                node['resident_reads'] = resident
            # Adaptive parks drained TCP on readiness and keeps fabric CQ
            # progress live; neither transport uses periodic idle sleeps.
            launch = dict(node=node, tls=tls(f'node-{partition}', tls_root),
                limits=dict(max_frame_bytes=16*MIB, max_connections=16, max_reads=256,
                    recovery_page_batches=128, io_timeout=dict(secs=60, nanos=0), polling='adaptive'),
                listen=address, ready_file=str(runtime_node / 'ready'), stop_file=str(runtime_node / 'stop'),
                drop_first_append_response=False)
            dump(node_dir / 'launch.yaml', launch)
            nodes.append(dict(namespace=namespace, partition=partition, host=host,
                launch=str(node_dir / 'launch.yaml'), ready=str(runtime_node / 'ready'), stop=str(runtime_node / 'stop')))
            endpoints.append(dict(partition=partition, address=address, server_name=f'node-{partition}'))
        # Coordinator-local credentials are unnecessary in explicit trusted mode.
        client_tls = tls('authority', directory / 'tls')
        # Retained epoch history is not the in-flight window: acknowledged
        # identities remain necessary for exact retries and crash recovery.
        databases[namespace] = dict(storage=storage, endpoints=endpoints, tls=client_tls,
            timeout=dict(secs=30, nanos=0), max_admitted_batches=storage['max_batches_per_partition'],
            max_index_bytes=512*MIB,
            max_pending_batches=128, max_pending_bytes=128*MIB, pipelined_writes=True,
            write_profile_limit=write_profile_limit,
            read_cache_bytes=coordinator_cache, read_memory_allowance_bytes=128*MIB+2*coordinator_cache,
            preparation_workers=2, scan_prefetch_pages=2, read_lanes=2,
            max_pending_reads=256, max_pending_read_bytes=64*MIB)
        if resident['destination_slots']:
            client_tls['resident_reads'] = resident
    config = dict(root=str(db_root), databases=databases)
    (directory / 'config.json').write_text(json.dumps(config, indent=2)+'\n')
    (directory / 'nodes.json').write_text(json.dumps(nodes, indent=2)+'\n')
    return config, nodes
