"""Owned file-backed NVMe/RDMA controller headroom probe helpers."""
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'nvmeof'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
from cloud_layout import OWNER_HOST_COUNTS
import gcp_runtime
import manage
import rdma_diagnostics

COUNT_PER_OWNER = 45
MAX_CONTROLLERS = 225
MIN_CONTROLLERS = 135
PORT_ID = 19001
SERVICE = 19420


def identities(run_id, owner, count=COUNT_PER_OWNER):
    if (not re.fullmatch(r'bench-[0-9a-f]{16}', run_id) or type(owner) is not int or not 1 <= owner <= max(OWNER_HOST_COUNTS)
            or type(count) is not int or not 1 <= count <= MAX_CONTROLLERS):
        raise ValueError('invalid probe identity')
    host = 'nqn.2026-01.io.tidehunter:headroom.' + run_id
    names = [f'{host}.{owner}.{i:02d}' for i in range(count)]
    return host, names


def _identity(path):
    item = path.lstat()
    return [item.st_dev, item.st_ino]


def _save(root, state):
    temporary = root / 'state.new'
    with temporary.open('w') as out:
        json.dump(state, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, root / 'state.json')


def create_targets(root, configfs, run_id, owner, address, count=COUNT_PER_OWNER):
    """Publish only run-owned disposable files and persist configfs identities."""
    hostnqn, names = identities(run_id, owner, count)
    ip = str(ipaddress.IPv4Address(address))
    root, configfs = Path(root), Path(configfs)
    port, host = configfs / 'ports' / str(PORT_ID), configfs / 'hosts' / hostnqn
    if port.exists() or host.exists() or any((configfs / 'subsystems' / n).exists() for n in names):
        raise ValueError('probe configfs identity already exists')
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    state = {'run_id': run_id, 'owner': owner, 'count': count, 'address': ip, 'hostnqn': hostnqn,
             'objects': [], 'scope': 'writable disposable sparse files; no campaign WAL data'}
    _save(root, state)

    def created(path):
        state['objects'].append({'path': str(path), 'identity': _identity(path)})
        _save(root, state)

    try:
        host.mkdir(); created(host)
        port.mkdir(); created(port)
        for attr, value in [('addr_trtype', 'rdma'), ('addr_adrfam', 'ipv4'),
                            ('addr_traddr', ip), ('addr_trsvcid', str(SERVICE))]:
            manage.put(port / attr, value)
        for index, name in enumerate(names):
            image = root / f'{index:02d}.wal'
            with image.open('xb') as file:
                file.truncate(4096)
            sub = configfs / 'subsystems' / name
            sub.mkdir(); created(sub)
            manage.put(sub / 'attr_allow_any_host', '0')
            link = sub / 'allowed_hosts' / hostnqn
            link.symlink_to(host); created(link)
            namespace = sub / 'namespaces' / '1'
            namespace.mkdir(); created(namespace)
            manage.put(namespace / 'device_path', image)
            manage.put(namespace / 'device_uuid', str(uuid.uuid5(uuid.NAMESPACE_URL, name)))
            manage.enable_buffered_io(namespace)
            manage.put(namespace / 'enable', '1')
            link = port / 'subsystems' / name
            link.symlink_to(sub); created(link)
        state['complete'] = True
        _save(root, state)
        return {'owner': owner, 'names': names, 'port': SERVICE, 'address': ip}
    except BaseException:
        cleanup_targets(root, configfs, run_id, owner, count)
        raise


def cleanup_targets(root, configfs, run_id, owner, count=COUNT_PER_OWNER):
    """Remove only journaled objects with unchanged inode identities."""
    root, configfs = Path(root), Path(configfs)
    hostnqn, names = identities(run_id, owner, count)
    if not root.exists():
        if (configfs / 'ports' / str(PORT_ID)).exists() or (configfs / 'hosts' / hostnqn).exists() or any((configfs / 'subsystems' / n).exists() for n in names):
            raise ValueError('probe objects exist without ownership journal')
        return {'clean': True}
    state = json.loads((root / 'state.json').read_text())
    if state.get('run_id') != run_id or state.get('owner') != owner or state.get('hostnqn') != hostnqn or state.get('count') != count:
        raise ValueError('probe ownership journal mismatch')
    allowed = {configfs / 'ports' / str(PORT_ID), configfs / 'hosts' / hostnqn}
    for name in names:
        sub = configfs / 'subsystems' / name
        allowed.update((sub, sub / 'allowed_hosts' / hostnqn, sub / 'namespaces' / '1',
                        configfs / 'ports' / str(PORT_ID) / 'subsystems' / name))
    for item in state['objects']:
        path = Path(item['path'])
        if path not in allowed or not path.exists() and not path.is_symlink() or _identity(path) != item['identity']:
            raise ValueError('probe object missing or replaced')
    for item in reversed(state['objects']):
        path = Path(item['path'])
        if path.is_symlink():
            path.unlink()
        elif path.parent.name == 'namespaces':
            manage.put(path / 'enable', '0')
            path.rmdir()
        else:
            path.rmdir()
    if (configfs / 'ports' / str(PORT_ID)).exists() or (configfs / 'hosts' / hostnqn).exists() or any((configfs / 'subsystems' / n).exists() for n in names):
        raise ValueError('probe target cleanup incomplete')
    for index in range(count):
        image = root / f'{index:02d}.wal'
        if image.exists():
            image.unlink()
    (root / 'state.json').unlink()
    root.rmdir()
    return {'clean': True}


def resource_sample(device, count):
    evidence = rdma_diagnostics.capture_resources(device)
    resources = evidence['resources']
    if not resources['available']:
        raise ValueError('RDMA resource counts unavailable: ' + str(resources['error']))
    rows = [row for row in resources['devices'] if row['device'] == device]
    if len(rows) != 1 or any(name not in rows[0]['counts'] for name in ('mr', 'qp', 'cq', 'pd')):
        raise ValueError('RDMA MR/QP/CQ/PD counts unavailable')
    return {'controllers': count, 'capture': evidence}


def retire_controllers(expected, hostnqn, timeout=180):
    deadline = time.monotonic() + timeout
    desired = dict(expected)
    current = manage.controller_snapshot(timeout=min(5, timeout))
    selected = [(ctrl, identity) for ctrl, identity in current.items() if identity['nqn'] in desired]
    if len({identity['nqn'] for _, identity in selected}) != len(selected):
        raise ValueError('ambiguous owned probe controller')

    def retire(item):
        ctrl, identity = item
        if not manage.controller_matches(identity, identity['nqn'], 'rdma',
                                         desired[identity['nqn']], SERVICE):
            raise ValueError('probe controller identity changed')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('probe controller retirement deadline reached')
        manage.delete_controller(ctrl, identity, {'hostnqn': hostnqn})

    errors = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for future in [pool.submit(retire, item) for item in selected]:
            try:
                future.result()
            except Exception as exc:
                errors.append(f'{type(exc).__name__}: {exc}')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('probe controller retirement exceeded deadline')
    current = manage.controller_snapshot(timeout=min(5, remaining))
    if errors or any(identity['nqn'] in desired for identity in current.values()):
        raise ValueError('probe controller retirement unconfirmed: ' + '; '.join(errors[:8]))
    return len(selected)


def targets_per_owner(owners):
    """Coordinator-wide headroom: spread MAX_CONTROLLERS over the owners so the
    MIN_CONTROLLERS threshold is reachable for every reviewed owner count."""
    if owners not in OWNER_HOST_COUNTS:
        raise ValueError('unreviewed owner count')
    return MAX_CONTROLLERS // owners


def probe(run_id, addresses, device, seconds=120):
    if len(addresses) not in OWNER_HOST_COUNTS or not re.fullmatch(r'[A-Za-z0-9_]+', device):
        raise ValueError('a reviewed owner count and a valid RDMA device are required')
    addresses = [str(ipaddress.IPv4Address(x)) for x in addresses]
    hostnqn, _ = identities(run_id, 1)
    hostid = str(uuid.uuid5(uuid.NAMESPACE_URL, hostnqn))
    count = targets_per_owner(len(addresses))
    expected = [(name, address) for owner, address in enumerate(addresses, 1)
                for name in identities(run_id, owner, count)[1]]
    before = manage.controller_snapshot()
    if any(row['nqn'] in {name for name, _ in expected} for row in before.values()):
        raise ValueError('probe controller already exists')
    start = time.monotonic()
    result = {'scope': 'writable disposable sparse files, coordinator does not write',
              'requested_geometry': {'io_queues': 1, 'queue_size': 32},
              'attempted': 0, 'connected': 0, 'samples': [], 'failure': None,
              'cleanup_complete': False}
    result['samples'].append(resource_sample(device, 0))
    stage = 'connect'
    failure = None
    try:
        for name, address in expected:
            stage = 'connect'
            remaining = seconds - (time.monotonic() - start)
            if remaining <= 0:
                raise TimeoutError('headroom probe time limit reached')
            result['attempted'] += 1
            command = ['nvme', 'connect', '-t', 'rdma', '-a', address, '-s', str(SERVICE),
                       '-q', hostnqn, '-I', hostid, '-n', name, '-l', '0', '-i', '1', '-Q', '32']
            stage = 'connect'
            subprocess.run(command, check=True, capture_output=True, timeout=min(10, remaining))
            stage = 'identity'
            current = manage.controller_snapshot(timeout=min(5, max(.01, seconds - (time.monotonic() - start))))
            matches = manage.new_controller_matches(before, current, name, 'rdma', address, SERVICE)
            if len(matches) != 1:
                raise ValueError('connected probe controller not uniquely visible')
            manage.verify_queue_geometry(manage.SYS_NVME / matches[0], (1, 32))
            result['connected'] += 1
            if result['connected'] % 15 == 0:
                stage = 'resources'
                result['samples'].append(resource_sample(device, result['connected']))
    except Exception as exc:
        failure = exc
        result['failure'] = f'{type(exc).__name__}: {exc}'
        result['failure_stage'] = stage
        if isinstance(exc, subprocess.CalledProcessError):
            result['stderr'] = (exc.stderr or b'').decode(errors='replace')[-8192:]
        try:
            result['samples'].append(resource_sample(device, result['connected']))
        except Exception as sample_error:
            result['sample_failure'] = f'{type(sample_error).__name__}: {sample_error}'
    finally:
        result['connect_loop_seconds'] = time.monotonic() - start
        teardown_start = time.monotonic()
        result['teardown_started_unix'] = time.time()
        # Include an ambiguous failed connect if the kernel created it.
        try:
            result['retired_controllers'] = retire_controllers(expected, hostnqn)
            result['cleanup_complete'] = True
        except Exception as exc:
            result['cleanup_failure'] = f'{type(exc).__name__}: {exc}'
        result['controller_teardown_seconds'] = time.monotonic() - teardown_start
    result['elapsed_seconds'] = time.monotonic() - start
    result['admitted'] = (result['cleanup_complete'] and not result.get('sample_failure')
                          and (failure is None or (stage == 'connect' and result['connected'] >= MIN_CONTROLLERS)))
    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('create', 'cleanup', 'probe'))
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--owner', type=int)
    parser.add_argument('--address')
    parser.add_argument('--addresses')
    parser.add_argument('--device')
    parser.add_argument('--owners', type=int, help='owner host count (create/cleanup)')
    args = parser.parse_args()
    root = gcp_runtime.run_data_root(args.run_id) / 'headroom-probe'
    configfs = Path('/sys/kernel/config/nvmet')
    if args.action in ('create', 'cleanup'):
        if args.owners is None:
            parser.error('--owners is required for create and cleanup')
        count = targets_per_owner(args.owners)
    if args.action == 'create':
        result = create_targets(root, configfs, args.run_id, args.owner, args.address, count)
    elif args.action == 'cleanup':
        result = cleanup_targets(root, configfs, args.run_id, args.owner, count)
    else:
        result = probe(args.run_id, args.addresses.split(','), args.device)
    print(json.dumps(result, sort_keys=True), flush=True)
    if args.action == 'probe' and not result['admitted']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
