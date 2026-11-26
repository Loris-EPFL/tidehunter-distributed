#!/usr/bin/env python3
"""Prepare fresh RF1 namespaces for an explicit owner set; never launches hosts or SSH."""
import argparse
import json
from pathlib import Path
import sys
import tarfile
sys.dont_write_bytecode = True
import build
import deployment


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema',type=Path,required=True,help='native-schema from the same frozen source build')
    parser.add_argument('--build',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--db-root',type=Path,required=True,help='future coordinator remote Sui db-path')
    parser.add_argument('--guest-root',type=Path,required=True,help='fresh absolute namespace root, identical path on all hosts')
    parser.add_argument('--hosts',required=True,help='distinct private IPv4 owner control addresses, comma separated (one per owner)')
    parser.add_argument('--live-hosts',help='distinct owner NVMe-oF target addresses; defaults to --hosts')
    parser.add_argument('--owners',type=int,help='expected owner count; defaults to the number of --hosts')
    parser.add_argument('--port',type=int,default=24000)
    parser.add_argument('--transport',choices=('tcp', 'tcp-trusted', *deployment.HARDWARE_TRANSPORTS),default='tcp')
    parser.add_argument('--durability-tier',choices=('native','synchronous'),default='synchronous',
                        help='acknowledgement tier every provisioned database must match')
    parser.add_argument('--live-transport',choices=deployment.LIVE_TRANSPORTS,
                        help='NVMe-oF transport of the owner targets; EFA defaults to rdma, TCP to tcp')
    parser.add_argument('--live-io-queues',type=int,default=deployment.DEFAULT_QUEUE_GEOMETRY['io_queues'])
    parser.add_argument('--live-queue-size',type=int,default=deployment.DEFAULT_QUEUE_GEOMETRY['queue_size'])
    parser.add_argument('--live-poll-queues',type=int,default=deployment.DEFAULT_QUEUE_GEOMETRY['poll_queues'])
    parser.add_argument('--owner-wal',choices=('harness','engine_default'),default='harness')
    parser.add_argument('--coordinator-cache-mib',type=int,default=8)
    parser.add_argument('--shard-cache-mib',type=int,default=0)
    parser.add_argument('--resident-reads',type=Path,
                        help='bounded per-database registered-read JSON policy; trusted-native fabric only')
    parser.add_argument('--write-profile-limit',type=int,default=0,
                        help='maximum profiled append RPCs per owning connection, 0 disables diagnostics')
    args=parser.parse_args()
    hosts=args.hosts.split(',')
    live_hosts=hosts if args.live_hosts is None else args.live_hosts.split(',')
    owners=len(hosts) if args.owners is None else args.owners
    if (not 1<=owners<=16 or len(set(hosts))!=owners or len(set(live_hosts))!=owners
            or any(not host for host in hosts) or any(not host for host in live_hosts)
            or not args.guest_root.is_absolute() or not args.db_root.is_absolute()):
        raise ValueError('distinct owner hosts and absolute owned roots required')
    if not 1024<=args.port<=65000 or any(not 0<=v<=128 for v in (args.coordinator_cache_mib,args.shard_cache_mib)):
        raise ValueError('port/cache outside bounded limits')
    if not 0<=args.write_profile_limit<=65536:
        raise ValueError('write profile limit must be 0..65536')
    resident = (deployment.resident_read_config(json.loads(args.resident_reads.read_text()))
                if args.resident_reads else None)
    manifest=json.loads((args.build/'build-manifest.json').read_text())
    binary=args.build/'binaries/storage-node'
    if build.digest(binary)!=manifest['binaries']['storage-node'] or manifest['protocol']!=9:
        raise ValueError('storage binary/source binding differs')
    if args.transport!='tcp' and not manifest['fabric']:
        raise ValueError('hardware/fabric transport requires a fabric-enabled frozen build')
    transport = deployment.canonical_transport(args.transport)
    trusted = deployment.is_trusted(transport)
    geometry = dict(io_queues=args.live_io_queues, queue_size=args.live_queue_size,
                    poll_queues=args.live_poll_queues)
    output=args.output.resolve()
    schema=deployment.schemas(args.schema, 'native')
    config,nodes=deployment.prepare(output,schema,args.db_root,
        shape_yamls={name:(args.schema/name/'shape.yaml').read_text() for name in schema},hosts=hosts,port=args.port,
        transport=transport,coordinator_cache=args.coordinator_cache_mib*deployment.MIB,
        shard_cache=args.shard_cache_mib*deployment.MIB,guest_root=args.guest_root,
        write_profile_limit=args.write_profile_limit,resident_reads=resident,owners=owners,
        owner_tier=args.durability_tier, live_transport=args.live_transport,
        live_geometry=geometry, owner_wal=args.owner_wal, live_addresses=live_hosts)
    host_records=[]
    for partition,host in enumerate(hosts):
        selected=[node for node in nodes if node['partition']==partition]
        archive_path=output/f'host-{partition}.tar.gz'
        with tarfile.open(archive_path,'w:gz') as archive:
            archive.add(binary,arcname='storage-node')
            if not trusted:
                for name in ('ca.der',f'node-{partition}.der',f'node-{partition}.pk8'):
                    archive.add(output/'tls'/name,arcname='tls/'+name)
            for node in selected:
                path=Path(node['launch'])
                archive.add(path,arcname=str(path.relative_to(output)))
        host_records.append(dict(host=host,partition=partition,archive=str(archive_path),
            archive_sha256=build.digest(archive_path),guest_root=str(args.guest_root),
            commands=[[str(args.guest_root/'storage-node'),'--experimental-storage-node',
                       str(args.guest_root/Path(n['launch']).relative_to(output))] for n in selected],
            ready_files=[n['ready'] for n in selected],stop_files=[n['stop'] for n in selected]))
    build.save(output/'external-manifest.json',dict(schema=1,protocol=9,transport=transport,owners=owners,
        durability_tier=args.durability_tier,live_transport=args.live_transport or
            deployment.live_transport_for(transport, None),owner_wal=args.owner_wal,
        config_sha256=build.digest(output/'config.json'),build_manifest_sha256=build.digest(args.build/'build-manifest.json'),
        source_archive_sha256=manifest['source_archive_sha256'],storage_binary_sha256=manifest['binaries']['storage-node'],
        shard_cache_bytes=args.shard_cache_mib*deployment.MIB,write_profile_limit=args.write_profile_limit,hosts=host_records,
        resident_reads=deployment.resident_read_config(resident),
        ownership='controller creates each fresh guest root, starts only listed process groups, bounds deadlines, collects /proc IO/RSS/swap and stops only owned groups',
        security=('trusted isolated cluster; no application TLS or certificate files'
                  if trusted else
                  'each host receives its own leaf key plus public CA; no coordinator key or CA signing key')))
    print(output/'external-manifest.json')


if __name__=='__main__':
    main()
