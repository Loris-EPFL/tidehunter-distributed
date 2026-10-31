#!/usr/bin/env python3
"""Manage one explicitly owned Linux nvmet export or initiator connection.

Run as trusted root. State directories must be private, local and persistent.
No operation removes an image or invokes a global NVMe disconnect.
"""
import argparse
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import struct
import sys
import time
import uuid

CONFIG = Path('/sys/kernel/config/nvmet')
SYS_NVME = Path('/sys/class/nvme')
SYS_SUBSYSTEM = Path('/sys/class/nvme-subsystem')
SYS_BLOCK = Path('/sys/class/block')
DEV = Path('/dev')
NQN = re.compile(r'^nqn\.[A-Za-z0-9][A-Za-z0-9.:-]{1,218}$')
CONTROLLER = re.compile(r'^nvme[0-9]+$')


def fail(message):
    raise ValueError(message)


def nqn(value):
    if not isinstance(value, str) or not NQN.fullmatch(value):
        fail('invalid NQN')
    return value


def manifest(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or path.resolve() != path:
        fail('manifest must be an absolute canonical path without symlinks')
    if path.name != 'manifest.json' or not path.is_file():
        fail('expected admitted export manifest.json')
    data = json.loads(path.read_text())
    nqn(data['nqn'])
    if data['version'] != 1 or type(data['nsid']) is not int or not 1 <= data['nsid'] < 0xffffffff:
        fail('unsupported manifest version or namespace ID')
    if type(data['image_len']) is not int or type(data['block_size']) is not int or data['block_size'] not in (512, 1024, 2048, 4096) or data['image_len'] <= 0 or data['image_len'] % data['block_size']:
        fail('invalid namespace geometry')
    ident = data['export_uuid']
    if not isinstance(ident, list) or len(ident) != 16 or any(type(x) is not int or not 0 <= x <= 255 for x in ident):
        fail('invalid export UUID')
    data['uuid_string'] = str(uuid.UUID(bytes=bytes(ident)))
    return data


def image_for(path, data):
    image = path.parent / 'image.wal'
    st = image.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size != data['image_len']:
        fail('image must be an exclusive regular file with the manifest length')
    # nvmet opens by path; no untrusted actor may replace the image or its parents.
    for part in [image, *image.parents]:
        s = part.lstat()
        if s.st_uid != 0 or s.st_mode & 0o022:
            fail('image and all parent directories must be root-owned and not group/world writable')
    return image


def preflight(transport, target):
    if os.geteuid() != 0:
        fail('root privileges required')
    if not shutil.which('nvme'):
        fail('nvme-cli is required')
    required = ['nvmet', 'nvmet_' + transport] if target else ['nvme_fabrics', 'nvme_' + transport]
    for module in required:
        if not (Path('/sys/module') / module).is_dir():
            fail('required kernel module not loaded: ' + module)
    if transport == 'rdma' and not list(Path('/sys/class/infiniband').glob('*')):
        fail('RDMA requires a usable RDMA-CM/RC device; no RDMA device found')
    if target and not CONFIG.is_dir():
        fail('nvmet configfs is not mounted')
    if not target and not (DEV / 'nvme-fabrics').exists():
        fail('/dev/nvme-fabrics unavailable')


def run(argv, timeout=20, binary=False):
    try:
        return subprocess.run(argv, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout, text=not binary).stdout
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr or exc.stdout or ''
        if isinstance(detail, bytes):
            detail = detail.decode('utf-8', errors='replace')
        fail(f'command failed ({exc.returncode}): {argv!r}: {detail.strip()}')


class StateDirectory:
    def __init__(self, fd):
        self.fd = fd

    def __del__(self):
        os.close(self.fd)


def trusted_directory(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        fail('state path must be absolute without traversal')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in ('/', *path.parts[1:]):
            if component != '/':
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            st = os.fstat(fd)
            if st.st_uid != 0 or st.st_mode & 0o022:
                fail('state path ancestors must be root-owned and not group/world writable')
        return fd
    except BaseException:
        os.close(fd)
        raise


def save(directory, data):
    # Production retains the validated directory fd, never reopening a path.
    # A Path is accepted for isolated, nonprivileged unit fixtures only.
    fixture = not isinstance(directory, StateDirectory)
    dfd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW) if fixture else directory.fd
    temp = '.state-' + uuid.uuid4().hex
    try:
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=dfd)
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f, sort_keys=True, indent=2)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp, 'state.json', src_dir_fd=dfd, dst_dir_fd=dfd)
            os.fsync(dfd)
        finally:
            try:
                os.unlink(temp, dir_fd=dfd)
            except FileNotFoundError:
                pass
    finally:
        if fixture:
            os.close(dfd)


def new_state(path, data):
    path = Path(path)
    if not path.name or path.name in ('.', '..'):
        fail('invalid state directory name')
    parent = trusted_directory(path.parent)
    try:
        os.mkdir(path.name, 0o700, dir_fd=parent)
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        os.fsync(parent)
    finally:
        os.close(parent)
    directory = StateDirectory(fd)
    save(directory, data)
    return directory


def load_state(path, kind):
    fd = trusted_directory(path)
    directory = StateDirectory(fd)
    if os.fstat(fd).st_mode & 0o077:
        fail('state directory must be private')
    state_fd = os.open('state.json', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
    with os.fdopen(state_fd, 'rb') as f:
        st = os.fstat(f.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o077 or st.st_size > 1024 * 1024:
            fail('invalid state file')
        data = json.load(f)
    if data.get('version') != 1 or data.get('kind') != kind:
        fail('invalid state kind/version')
    return directory, data


def put(path, value):
    path.write_text(str(value))


def owned_link(path, destination):
    if path.is_symlink():
        if path.resolve() != destination.resolve():
            fail('owned link points elsewhere: ' + str(path))
        path.unlink()
    elif path.exists():
        fail('unexpected non-link at ' + str(path))


def object_identity(path):
    st = path.lstat()
    if not stat.S_ISDIR(st.st_mode):
        fail('owned configfs object is not a directory: ' + str(path))
    return [st.st_dev, st.st_ino]


def verify_owned_object(s, flag, path):
    if s.get(flag) and path.exists():
        if s.get(flag + '_identity') != object_identity(path):
            fail('owned configfs object was replaced: ' + str(path))


def verify_owned_link(path, destination):
    if path.is_symlink():
        if path.resolve() != destination.resolve():
            fail('owned link points elsewhere: ' + str(path))
    elif path.exists():
        fail('unexpected non-link at ' + str(path))


def target_cleanup(directory, s):
    if s.get('phase') == 'cleaned':
        return
    nqn(s['nqn']); nqn(s['hostnqn'])
    port = CONFIG / 'ports' / str(int(s['port_id']))
    sub = CONFIG / 'subsystems' / s['nqn']
    host = CONFIG / 'hosts' / s['hostnqn']
    ns = sub / 'namespaces' / str(int(s['nsid']))
    # Check every extant object and link before the first mutation. Inode plus
    # filesystem identity prevents a retry from touching a replacement object.
    for flag, path in [('subsystem', sub), ('host', host), ('port', port), ('namespace', ns)]:
        verify_owned_object(s, flag, path)
    if s.get('port') and s.get('subsystem'):
        verify_owned_link(port / 'subsystems' / s['nqn'], sub)
    if s.get('subsystem') and s.get('host'):
        verify_owned_link(sub / 'allowed_hosts' / s['hostnqn'], host)
    if s.get('port') and s.get('subsystem'):
        owned_link(port / 'subsystems' / s['nqn'], sub)
    if s.get('namespace'):
        if ns.exists():
            put(ns / 'enable', '0')
            ns.rmdir()
        s['namespace'] = False
        save(directory, s)
    if s.get('subsystem') and s.get('host'):
        owned_link(sub / 'allowed_hosts' / s['hostnqn'], host)
    for flag, path in [('subsystem', sub), ('host', host), ('port', port)]:
        if s.get(flag):
            if path.exists():
                path.rmdir()
            s[flag] = False
            save(directory, s)
    s['phase'] = 'cleaned'
    save(directory, s)


def target_setup(args):
    preflight(args.transport, True)
    mpath = Path(args.manifest)
    m = manifest(mpath)
    image = image_for(mpath, m)
    hostnqn = nqn(args.hostnqn)
    addr = ipaddress.ip_address(args.address)
    if not 1 <= args.port_id <= 65535 or not 1 <= args.service <= 65535:
        fail('port ID/service out of range')
    sub = CONFIG / 'subsystems' / m['nqn']
    host = CONFIG / 'hosts' / hostnqn
    port = CONFIG / 'ports' / str(args.port_id)
    if any(p.exists() for p in (sub, host, port)):
        fail('subsystem, host or port already exists; refusing to share')
    s = {'version': 1, 'kind': 'target', 'phase': 'creating', 'manifest': str(mpath),
         'image': str(image), 'image_device': image.stat().st_dev, 'image_inode': image.stat().st_ino,
         'nqn': m['nqn'], 'hostnqn': hostnqn, 'nsid': m['nsid'], 'uuid': m['uuid_string'],
         'port_id': args.port_id, 'service': args.service, 'address': str(addr),
         'transport': args.transport, 'target_cache': args.target_cache,
         'initiator_cache': 'O_DIRECT required by reader', 'created_at_unix_ns': time.time_ns()}
    directory = new_state(args.state, s)
    try:
        for flag, path in [('subsystem', sub), ('host', host), ('port', port)]:
            path.mkdir()
            s[flag] = True
            s[flag + '_identity'] = object_identity(path)
            save(directory, s)
        put(sub / 'attr_allow_any_host', '0')
        (sub / 'allowed_hosts' / hostnqn).symlink_to(host)
        ns = sub / 'namespaces' / str(m['nsid'])
        ns.mkdir()
        s['namespace'] = True
        s['namespace_identity'] = object_identity(ns)
        save(directory, s)
        put(ns / 'device_path', image)
        put(ns / 'device_uuid', m['uuid_string'])
        put(ns / 'buffered_io', '1' if args.target_cache == 'buffered' else '0')
        put(ns / 'enable', '1')
        for key, value in [('addr_trtype', args.transport), ('addr_adrfam', 'ipv4' if addr.version == 4 else 'ipv6'),
                           ('addr_traddr', addr), ('addr_trsvcid', args.service)]:
            put(port / key, value)
        (port / 'subsystems' / m['nqn']).symlink_to(sub)
        s['phase'] = 'published_unprotected'; save(directory, s)
    except BaseException:
        target_cleanup(directory, s)
        raise
    return s


def controllers():
    return {p.name for p in SYS_NVME.glob('nvme*') if CONTROLLER.fullmatch(p.name)}


def disconnect(s):
    ctrl = s.get('controller')
    if ctrl is None:
        return
    if not CONTROLLER.fullmatch(ctrl):
        fail('invalid recorded controller')
    p = SYS_NVME / ctrl
    if not p.exists():
        return
    if (p / 'subsysnqn').read_text().strip() != s['nqn'] or (p / 'transport').read_text().strip() != s['transport']:
        fail('recorded controller identity changed; refusing disconnect')
    if 'controller_inode' not in s or p.stat().st_ino != s['controller_inode']:
        fail('recorded controller has been replaced; refusing disconnect')
    if (p / 'address').read_text().strip() != s['controller_address']:
        fail('recorded controller address changed; refusing disconnect')
    run(['nvme', 'disconnect', '-d', str(DEV / ctrl)])


def discovery_has_endpoint(value, expected_nqn, transport, address, service):
    if isinstance(value, list):
        return any(discovery_has_endpoint(v, expected_nqn, transport, address, service) for v in value)
    if not isinstance(value, dict):
        return False
    normalized = {str(k).lower().replace('_', ''): v for k, v in value.items()}
    if (normalized.get('subnqn') == expected_nqn
            and str(normalized.get('trtype', '')).lower() == transport
            and str(normalized.get('traddr', '')).strip() == address
            and str(normalized.get('trsvcid', '')).strip() == str(service)):
        return True
    return any(discovery_has_endpoint(v, expected_nqn, transport, address, service)
               for v in value.values())


def controller_matches(path, expected_nqn, transport, address, service):
    if (path / 'subsysnqn').read_text().strip() != expected_nqn:
        return False
    if (path / 'transport').read_text().strip() != transport:
        return False
    fields = dict(item.split('=', 1) for item in (path / 'address').read_text().strip().split(',')
                  if '=' in item)
    return fields.get('traddr') == address and fields.get('trsvcid') == str(service)


def namespace_candidates(ctrl, m):
    subsystems = []
    for subsystem in SYS_SUBSYSTEM.glob('nvme-subsys*'):
        member = subsystem / ctrl.name
        if member.exists() and member.resolve() == ctrl.resolve():
            subsystems.append(subsystem)
    if not subsystems:
        return []
    if len(subsystems) != 1:
        fail('controller has ambiguous subsystem membership')
    subsystem = subsystems[0]
    if (subsystem / 'subsysnqn').read_text().strip() != m['nqn']:
        fail('subsystem NQN mismatch')
    members = [p for p in subsystem.glob('nvme*') if CONTROLLER.fullmatch(p.name)]
    if len(members) != 1 or members[0].resolve() != ctrl.resolve():
        fail('subsystem has multiple or unexpected controllers')
    candidates = []
    for path in SYS_BLOCK.glob('nvme*n*'):
        if not re.fullmatch(r'nvme[0-9]+n[0-9]+', path.name):
            continue
        if not (path / 'device').exists():
            continue
        if (path / 'device').resolve() not in (ctrl.resolve(), subsystem.resolve()):
            continue
        if not (path / 'nsid').exists() or (path / 'nsid').read_text().strip() != str(m['nsid']):
            continue
        if ((path / 'uuid').read_text().strip().lower() != m['uuid_string']
                or (path / 'wwid').read_text().strip() != 'uuid.' + m['uuid_string']):
            fail('namespace UUID mismatch')
        candidates.append(path)
    return candidates


def ensure_host_readonly(device):
    # Target feature and identify checks are separate and must precede this.
    fd = os.open(device, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        if not stat.S_ISBLK(os.fstat(fd).st_mode):
            fail('namespace device is not a block device')
        value = bytearray(4)
        fcntl.ioctl(fd, 0x125e, value, True)  # BLKROGET
        if struct.unpack('i', value)[0] != 1:
            fcntl.ioctl(fd, 0x125d, struct.pack('i', 1))  # BLKROSET: host guard only
            value = bytearray(4)
            fcntl.ioctl(fd, 0x125e, value, True)
        if struct.unpack('i', value)[0] != 1:
            fail('host block device did not become read-only')
    finally:
        os.close(fd)


def host_connect(args):
    preflight(args.transport, False)
    m = manifest(Path(args.manifest))
    hostnqn = nqn(args.hostnqn)
    addr = str(ipaddress.ip_address(args.address))
    if not 1 <= args.service <= 65535:
        fail('service out of range')
    before = controllers()
    for ctrl in before:
        if (SYS_NVME / ctrl / 'subsysnqn').read_text().strip() == m['nqn']:
            fail('a controller for this NQN already exists; refusing to share')
    # The kernel binds each host ID to one NQN. Do not reuse the machine's
    # ambient host ID for a private experiment NQN or change /etc/nvme.
    hostid = str(uuid.uuid5(uuid.NAMESPACE_URL, hostnqn))
    common = ['-t', args.transport, '-a', addr, '-s', str(args.service),
              '-q', hostnqn, '-I', hostid]
    discovery = json.loads(run(['nvme', 'discover', *common, '-o', 'json', '--no-nbft']))
    if not discovery_has_endpoint(discovery, m['nqn'], args.transport, addr, args.service):
        fail('expected NQN absent from discovery')
    s = {'version': 1, 'kind': 'host', 'phase': 'connecting', 'manifest': args.manifest,
         'nqn': m['nqn'], 'uuid': m['uuid_string'], 'nsid': m['nsid'], 'hostnqn': hostnqn, 'hostid': hostid,
         'address': addr, 'service': args.service, 'transport': args.transport,
         'created_at_unix_ns': time.time_ns(), 'initiator_cache': 'O_DIRECT required by reader'}
    directory = new_state(args.state, s)
    try:
        run(['nvme', 'connect', *common, '-n', m['nqn'], '-l', '0'])
        deadline = time.monotonic() + 20
        while True:
            new = controllers() - before
            matching = [c for c in new if controller_matches(SYS_NVME / c, m['nqn'], args.transport, addr, args.service)]
            if len(matching) == 1:
                break
            if len(matching) > 1 or time.monotonic() >= deadline:
                fail('expected exactly one newly connected controller')
            time.sleep(.1)
        s['controller'] = matching[0]
        s['controller_inode'] = (SYS_NVME / matching[0]).stat().st_ino
        s['controller_address'] = (SYS_NVME / matching[0] / 'address').read_text().strip()
        save(directory, s)
        ctrl = SYS_NVME / s['controller']
        if (ctrl / 'transport').read_text().strip() != args.transport:
            fail('controller transport mismatch')
        devctrl = DEV / s['controller']
        run(['nvme', 'set-feature', str(devctrl), '-f', '0x84', '-n', str(m['nsid']), '-V', '1'])
        feature = run(['nvme', 'get-feature', str(devctrl), '-f', '0x84', '-n', str(m['nsid'])])
        found = re.search(r'Current value\s*:\s*(0x[0-9a-f]+|[0-9]+)', feature, re.I)
        if not found or int(found.group(1), 0) != 1:
            fail('namespace write protection readback failed: ' + feature.strip())
        ident = run(['nvme', 'id-ns', str(devctrl), '-n', str(m['nsid']), '-b'], binary=True)
        if len(ident) != 4096 or not ident[99] & 1:
            fail('identify namespace does not report read-only')
        flbas = (ident[26] & 15) | ((ident[26] & 0x60) >> 1)
        block_size = 1 << ident[128 + 4 * flbas + 2]
        if int.from_bytes(ident[:8], 'little') * block_size != m['image_len'] or block_size != m['block_size']:
            fail('namespace geometry mismatch')
        deadline = time.monotonic() + 20
        while True:
            namespaces = namespace_candidates(ctrl, m)
            if len(namespaces) == 1:
                break
            if len(namespaces) > 1 or time.monotonic() >= deadline:
                fail('namespace not uniquely present in sysfs')
            time.sleep(.1)
        ns = namespaces[0]
        # The block readonly flag is an independent initiator-side guard.
        device = DEV / ns.name
        ensure_host_readonly(device)
        s['device'] = str(device); s['phase'] = 'protected'; s['verified_at_unix_ns'] = time.time_ns()
        save(directory, s)
        return s
    except BaseException:
        # A failed connect may nevertheless have created a controller.
        if not s.get('controller'):
            for c in controllers() - before:
                p = SYS_NVME / c
                if controller_matches(p, m['nqn'], args.transport, addr, args.service):
                    s['controller'] = c
                    s['controller_inode'] = p.stat().st_ino
                    s['controller_address'] = (p / 'address').read_text().strip()
                    save(directory, s); disconnect(s)
        else:
            disconnect(s)
        s['phase'] = 'disconnected'; save(directory, s)
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest='command', required=True)
    for name in ('target-setup', 'host-connect'):
        c = commands.add_parser(name)
        c.add_argument('--manifest', required=True); c.add_argument('--state', required=True)
        c.add_argument('--transport', choices=('tcp', 'rdma'), default='tcp')
        c.add_argument('--address', required=True); c.add_argument('--service', type=int, default=4420)
        c.add_argument('--hostnqn', required=True)
        if name == 'target-setup':
            c.add_argument('--port-id', type=int, required=True)
            c.add_argument('--target-cache', choices=('direct', 'buffered'), default='direct')
    for name in ('target-cleanup', 'host-disconnect'):
        c = commands.add_parser(name); c.add_argument('--state', required=True)
        if name == 'target-cleanup':
            c.add_argument('--readers-drained', action='store_true', required=True,
                           help='attest all initiators and readers have been quiesced and disconnected')
    a = p.parse_args()
    if a.command == 'target-setup':
        result = target_setup(a)
    elif a.command == 'host-connect':
        result = host_connect(a)
    elif a.command == 'target-cleanup':
        d, result = load_state(a.state, 'target'); target_cleanup(d, result)
    else:
        d, result = load_state(a.state, 'host')
        if result.get('phase') != 'disconnected':
            disconnect(result); result['phase'] = 'disconnected'; save(d, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        print('nvmeof: ' + str(exc), file=sys.stderr)
        sys.exit(1)
