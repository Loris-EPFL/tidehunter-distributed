#!/usr/bin/env python3
"""Orchestrate explicitly owned, buffered TCP or RDMA NVMe-oF exports.

All paths are absolute and rooted in trusted, root-owned directories. JSON output
is a receipt; retain it for the other machine. State directories are never reused.
"""
import argparse
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import stat
import sys
import time
import uuid
from types import SimpleNamespace

import manage

MAX_EXPORTS = 256
MAX_JSON = 64 * 1024 * 1024
PRIVATE = tuple(ipaddress.ip_network(x) for x in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))


def absolute(value):
    p = Path(value)
    if not p.is_absolute() or '..' in p.parts or str(p) != value:
        raise ValueError('path must be absolute and canonical: ' + str(value))
    return p


def trusted_json(value, max_bytes=MAX_JSON, with_hash=False):
    path = absolute(value)
    fd = manage.trusted_directory(path.parent)
    try:
        f = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
        with os.fdopen(f, 'rb') as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022 or st.st_size > max_bytes:
                raise ValueError('untrusted JSON file: ' + str(path))
            raw = stream.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ValueError('JSON file exceeds size bound')
            data = json.loads(raw)
            return (data, hashlib.sha256(raw).hexdigest()) if with_hash else data
    finally:
        os.close(fd)


def new_campaign_state(path, state):
    parent = manage.trusted_directory(path.parent)
    try:
        os.mkdir(path.name, 0o700, dir_fd=parent)
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fsync(parent)
    finally:
        os.close(parent)
    directory = manage.StateDirectory(fd)
    manage.save(directory, state)
    return directory


def campaign_state(path, kind):
    fd = manage.trusted_directory(path)
    directory = manage.StateDirectory(fd)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if os.fstat(fd).st_mode & 0o077:
            raise ValueError('state directory must be private')
        f = os.open('state.json', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
        with os.fdopen(f, 'rb') as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o077 or st.st_size > MAX_JSON:
                raise ValueError('invalid campaign state file')
            state = json.load(stream)
        if state.get('version') != 1 or state.get('kind') != kind:
            raise ValueError('invalid state kind/version')
        return directory, state
    except BaseException:
        directory.close()
        raise


def private_address(value):
    addr = ipaddress.ip_address(value)
    if addr.version != 4 or not any(addr in network for network in PRIVATE):
        raise ValueError('address must be an RFC1918 IPv4 fleet address')
    return str(addr)


def load_exports(ready, export_root, source_root=None):
    root = absolute(export_root)
    source = absolute(source_root) if source_root is not None else root
    fd = manage.trusted_directory(root)
    os.close(fd)
    data = trusted_json(ready)
    entries = data.get('exports')
    if not isinstance(entries, list) or len(entries) > MAX_EXPORTS:
        raise ValueError('ready exports must be a bounded list')
    result, seen_uuid, seen_nqn = [], set(), set()
    for item in entries:
        source_path = absolute(item['manifest_path'])
        if not source_path.is_relative_to(source) or source_path.name != 'manifest.json':
            raise ValueError('manifest outside trusted export root')
        path = root / source_path.relative_to(source)
        manifest_data, digest = trusted_json(str(path), with_hash=True)
        m = manage.manifest(path)
        validated = dict(m)
        validated.pop('uuid_string')
        if validated != manifest_data:
            raise ValueError('manifest changed during validation')
        for field in ('export_uuid', 'nqn', 'nsid', 'image_len', 'block_size'):
            if item.get(field) != m[field]:
                raise ValueError('ready and manifest disagree: ' + field)
        ident = m['uuid_string']
        if ident in seen_uuid or m['nqn'] in seen_nqn:
            raise ValueError('duplicate export UUID or NQN')
        seen_uuid.add(ident); seen_nqn.add(m['nqn'])
        result.append({'manifest': str(path), 'manifest_sha256': digest, 'export_uuid': m['export_uuid'],
                       'uuid': ident, 'nqn': m['nqn'], 'nsid': m['nsid']})
    return result



def host_identity(campaign_id, ident):
    token = uuid.uuid5(uuid.UUID(campaign_id), ident).hex
    hostnqn = 'nqn.2026-10.org.tidehunter:campaign-' + token
    return hostnqn, str(uuid.uuid5(uuid.NAMESPACE_URL, hostnqn))


def receipt(state):
    result = dict(state)
    if state['kind'] == 'campaign-host' and state['phase'] == 'connected':
        result['bindings'] = [{'export_uuid': e['export_uuid'], 'namespace_path': e['device'],
                               'transport': state['transport']} for e in state['exports']]
    return result


def setup(a):
    address = private_address(a.address)
    exports = load_exports(a.ready, a.export_root)
    if not 1 <= a.base_port_id <= 65535 or not 1 <= a.base_service <= 65535 or \
            a.base_port_id + len(exports) > 65536 or a.base_service + len(exports) > 65536:
        raise ValueError('port range out of bounds')
    campaign = str(uuid.uuid4())
    state_path = absolute(a.state)
    for i, e in enumerate(exports):
        e.update(address=address, port_id=a.base_port_id + i, service=a.base_service + i,
                 hostnqn=host_identity(campaign, e['uuid'])[0], status='pending',
                 state=str(state_path / ('export-%03d' % i)))
        e['hostid'] = host_identity(campaign, e['uuid'])[1]
    s = {'version': 1, 'kind': 'campaign-target', 'phase': 'creating', 'campaign_id': campaign,
         'transport': a.transport, 'export_root': str(absolute(a.export_root)), 'exports': exports}
    directory = new_campaign_state(state_path, s)
    try:
        for e in exports:
            e['status'] = 'attempted'; manage.save(directory, s)
            result = manage.target_setup(SimpleNamespace(manifest=e['manifest'], state=e['state'], transport=a.transport,
                address=address, service=e['service'], port_id=e['port_id'], hostnqn=e['hostnqn']))
            e['target_receipt'] = result; e['status'] = 'published'; manage.save(directory, s)
        s['phase'] = 'published'; manage.save(directory, s)
        return receipt(s)
    finally:
        directory.close()


def connect(a):
    targets = trusted_json(a.targets)
    if targets.get('version') != 1 or targets.get('kind') != 'campaign-target':
        raise ValueError('invalid target receipt')
    transport = targets.get('transport')
    if transport not in ('tcp', 'rdma'):
        raise ValueError('invalid target transport')
    campaign = str(uuid.UUID(targets['campaign_id']))
    local = load_exports(a.ready, a.export_root, targets['export_root'])
    remote = targets['exports']
    if not isinstance(remote, list) or len(local) != len(remote):
        raise ValueError('target export count mismatch')
    state_path = absolute(a.state)
    entries = []
    for i, (e, r) in enumerate(zip(local, remote)):
        for key in ('uuid', 'export_uuid', 'nqn', 'nsid', 'manifest_sha256'):
            if e[key] != r[key]:
                raise ValueError('target manifest mismatch: ' + key)
        hostnqn, hostid = host_identity(campaign, e['uuid'])
        if r['hostnqn'] != hostnqn or r['hostid'] != hostid:
            raise ValueError('target host identity mismatch')
        addr = private_address(r['address'])
        if type(r['service']) is not int or not 1 <= r['service'] <= 65535:
            raise ValueError('invalid target service')
        e.update(address=addr, service=r['service'], hostnqn=hostnqn, hostid=hostid,
                 target_status=r['status'], status='pending', state=str(state_path / ('export-%03d' % i)))
        entries.append(e)
    s = {'version': 1, 'kind': 'campaign-host', 'phase': 'connecting', 'campaign_id': campaign,
         'transport': transport, 'exports': entries}
    directory = new_campaign_state(state_path, s)
    try:
        for e in entries:
            if e['target_status'] != 'published':
                continue
            e['status'] = 'attempted'; manage.save(directory, s)
            result = manage.host_connect(SimpleNamespace(manifest=e['manifest'], state=e['state'], transport=transport,
                address=e['address'], service=e['service'], hostnqn=e['hostnqn']))
            if result['hostid'] != e['hostid'] or result['phase'] != 'protected':
                raise ValueError('host protection or identity mismatch')
            e['host_receipt'] = result; e['device'] = result['device']; e['status'] = 'connected'; manage.save(directory, s)
        s['phase'] = 'connected' if all(e['status'] == 'connected' for e in entries) else 'partial'
        manage.save(directory, s)
        return receipt(s)
    finally:
        directory.close()


def disconnect(a):
    if not a.readers_drained:
        raise ValueError('readers-drained attestation required')
    directory, s = campaign_state(absolute(a.state), 'campaign-host')
    try:
        for e in s['exports']:
            if e['status'] == 'pending' or e['status'] == 'disconnected':
                continue
            if not Path(e['state']).exists():
                # host_connect creates its state before issuing nvme connect.
                e['status'] = 'disconnected'; manage.save(directory, s); continue
            child, host = manage.load_state(e['state'], 'host')
            try:
                if host['phase'] != 'disconnected':
                    if not host.get('controller'):
                        raise ValueError('connecting host has no recorded controller; cannot prove disconnect')
                    manage.disconnect(host)
                deadline = time.monotonic() + 20
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ValueError('controller still present after disconnect')
                    inventory = manage.controller_snapshot(timeout=min(5.0, remaining))
                    if not any(row['nqn'] == host['nqn'] for row in inventory.values()):
                        break
                    time.sleep(min(.1, max(0, deadline - time.monotonic())))
                host['phase'] = 'disconnected'; manage.save(child, host)
                e['host_receipt'] = host; e['status'] = 'disconnected'; manage.save(directory, s)
            finally:
                child.close()
        s['phase'] = 'disconnected'; s['readers_drained'] = True; manage.save(directory, s)
        return receipt(s)
    finally:
        directory.close()


def validate_disconnect_proof(s, proof):
    if (proof.get('kind') != 'campaign-host' or proof.get('version') != 1 or
        proof.get('phase') != 'disconnected' or proof.get('readers_drained') is not True or
        proof.get('campaign_id') != s['campaign_id']):
        raise ValueError('host disconnected proof does not match campaign')
    host = proof.get('exports')
    if not isinstance(host, list) or len(host) != len(s['exports']):
        raise ValueError('host proof export count mismatch')
    for e, h in zip(s['exports'], host):
        if any(e[k] != h.get(k) for k in ('uuid', 'nqn', 'hostnqn', 'hostid', 'address', 'service')) or h.get('status') not in ('disconnected', 'pending'):
            raise ValueError('host proof export mismatch or still connected')


def cleanup(a):
    if not a.readers_drained:
        raise ValueError('readers-drained attestation required')
    proof = trusted_json(a.host_disconnected)
    directory, s = campaign_state(absolute(a.state), 'campaign-target')
    try:
        validate_disconnect_proof(s, proof)
        for e in s['exports']:
            if e['status'] in ('pending', 'cleaned'):
                continue
            if Path(e['state']).exists():
                child, target = manage.load_state(e['state'], 'target')
                try:
                    manage.target_cleanup(child, target)
                    e['target_receipt'] = target
                finally:
                    child.close()
            e['status'] = 'cleaned'; manage.save(directory, s)
        s['phase'] = 'cleaned'; s['readers_drained'] = True; manage.save(directory, s)
        return receipt(s)
    finally:
        directory.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    for name in ('target-setup', 'host-connect'):
        c = sub.add_parser(name)
        for option in ('ready', 'export-root', 'state'):
            c.add_argument('--' + option, required=True)
        if name == 'target-setup':
            c.add_argument('--address', required=True)
            c.add_argument('--transport', choices=('tcp', 'rdma'), default='tcp')
            c.add_argument('--base-port-id', type=int, required=True)
            c.add_argument('--base-service', type=int, required=True)
        else:
            c.add_argument('--targets', required=True)
    for name in ('host-disconnect', 'target-cleanup'):
        c = sub.add_parser(name); c.add_argument('--state', required=True)
        c.add_argument('--readers-drained', action='store_true', required=True)
        if name == 'target-cleanup':
            c.add_argument('--host-disconnected', required=True)
    c = sub.add_parser('status'); c.add_argument('--state', required=True)
    c.add_argument('--kind', choices=('target', 'host'), required=True)
    a = p.parse_args()
    if os.geteuid() != 0:
        raise ValueError('root privileges required')
    if a.command == 'status':
        d, s = campaign_state(absolute(a.state), 'campaign-' + a.kind)
        try:
            result = receipt(s)
        finally:
            d.close()
    else:
        result = {'target-setup': setup, 'host-connect': connect,
                  'host-disconnect': disconnect, 'target-cleanup': cleanup}[a.command](a)
    print(json.dumps(result, sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print('campaign-targets: ' + str(exc), file=sys.stderr)
        sys.exit(1)
