#!/usr/bin/env python3
"""Validate generated external Sui configs with the sealed storage binary; no nvmet."""
import argparse
import itertools
import json
from pathlib import Path
import subprocess
import uuid
import yaml

import build
import deployment

def validate_owner_resources(directory):
    """Reject cross-owner collisions and coordinator/target identity mismatch."""
    config = json.loads((directory / 'config.json').read_text())
    nodes = json.loads((directory / 'nodes.json').read_text())
    claimed = set()
    observed = set()
    expected = {(name, partition) for name, db in config['databases'].items()
                for partition in db['storage']['partitions']}

    def claim(resource, value):
        key = (resource, value)
        if key in claimed:
            raise ValueError(f'duplicate owner {resource}: {value}')
        claimed.add(key)

    for node in nodes:
        launch = yaml.safe_load(Path(node['launch']).read_text())
        owner = launch['node']
        db = config['databases'][node['namespace']]
        partition = owner['partition']['partition']
        if (partition != node['partition']
                or owner['partition']['inventory']['database'] != db['storage']['database']):
            raise ValueError('owner database or partition differs from coordinator')
        claim('database/partition', (tuple(db['storage']['database']), partition))
        observed.add((node['namespace'], partition))
        claim('node root', owner['root'])
        claim('listen', launch['listen'])
        target = owner.get('live_target')
        if target is not None:
            live = db['live_nvme']
            identities = live.get('owner_identities', {})
            identity = identities.get(str(partition)) if identities else live
            if identity is None or target['host_nqn'] != identity['host_nqn']:
                raise ValueError('owner host NQN differs from coordinator identity')
            claim('host NQN', target['host_nqn'])
            claim('host ID', tuple(identity['host_id']))
            claim('state directory', target['state_dir'])
            for field in ('port_id', 'commissioning_port_id'):
                claim('configfs port', target[field])
            claim('target address/service', (target['address'], target['service']))
            claim('target address/service', ('127.0.0.1', target['commissioning_service']))
    if observed != expected:
        raise ValueError('owner launches do not cover coordinator partitions')



def validate(build_root, verification_root, output, *, nvmeof):
    output.mkdir(mode=0o700)
    binary = build_root / 'binaries/storage-node'
    manifest = json.loads((build_root / 'build-manifest.json').read_text())
    if build.digest(binary) != manifest['binaries']['storage-node']:
        raise ValueError('storage validator binary differs from sealed build')
    rows = []
    for contract, proof, tier in (
            ('harness', 'verification', 'synchronous'),
            ('original_default', 'verification-original-default', 'native')):
        schema_root = verification_root / proof / 'experiment/native-schema'
        schema = deployment.schemas(schema_root, 'native')
        shapes = {name: (schema_root / name / 'shape.yaml').read_text() for name in schema}
        for wal, vms, live in itertools.product(
                ('harness', 'engine_default'), (2, 6), (False, True) if nvmeof else (False,)):
            hosts = ['10.0.0.2']*5 if vms == 2 else [f'10.0.0.{i+2}' for i in range(5)]
            label = f'{contract}-{wal}-{vms}vm-' + ('nvmeof' if live else 'verbs')
            directory = output / label
            identity = uuid.uuid5(uuid.NAMESPACE_URL, 'tidehunter-config-admission:' + label)
            live_config = (dict(addresses=hosts,
                host_nqn='nqn.2014-08.org.nvmexpress:uuid:' + str(identity), host_id=list(identity.bytes),
                queue_geometry=dict(io_queues=1, queue_size=32, poll_queues=0), export_mode='per_owner')
                if live else None)
            _, nodes = deployment.prepare(directory, schema, output / 'remote', shape_yamls=shapes,
                hosts=hosts, port=24000, transport=deployment.validate_transport(
                    dict(kind='ofi', fabric=dict(backend='verbs'), security='trusted')),
                coordinator_cache=8*deployment.MIB, guest_root=output / 'guest' / label,
                owner_tier=tier, owner_wal=wal, live_nvme=live_config)
            validate_owner_resources(directory)
            subprocess.run([str(binary), '--experimental-validate-config', str(directory / 'config.json'),
                            *[node['launch'] for node in nodes]], check=True, timeout=30)
            rows.append(dict(case=label, contract=contract, owner_wal=wal, owner_tier=tier,
                             owners=len(nodes), config_sha256=build.digest(directory/'config.json'),
                             launch_sha256=[build.digest(node['launch']) for node in nodes]))
    receipt = dict(storage_binary_sha256=build.digest(binary), complete=True, cells=rows,
                   scope='static deserialization and Rust validators; no sockets, WAL or nvmet opened')
    build.save(output / 'receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--verification-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--nvmeof', action='store_true')
    args = parser.parse_args()
    validate(args.build, args.verification_root, args.output, nvmeof=args.nvmeof)


if __name__ == '__main__':
    main()
