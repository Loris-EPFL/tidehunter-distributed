"""Two-host, source-bound live NVMe-oF qualification for the manual campaign.

The coordinator and owner use the existing immutable object-store barriers.  The
owner's TLS capsule is kept outside the exported qualification prefix and is
removed after it has been consumed. A result requires namespace operation and
independent NIC counter evidence; SPDK additionally requires direct backing FDs.
"""
import base64
import errno
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import shutil
import subprocess
import secrets
import sys
import time
import threading
import uuid

import gcp_qualify_bootstrap as bootstrap
import gcp_worker

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_campaign

def _label(transport, target, tier):
    if transport not in ('tcp', 'rdma') or target not in ('kernel', 'spdk') or tier not in ('native', 'synchronous'):
        raise ValueError('invalid live qualification selection')
    return 'live-' + transport + ('-spdk' if target == 'spdk' else '') + '-' + tier

def _owner_receipt(receipt, launch, tier):
    try:
        cloud_campaign.validate_owner_receipt(receipt, launch)
        if (tier not in ('native', 'synchronous') or launch['durability']['tier'] != tier
                or receipt['durability'] != launch['durability']
                or set(receipt['durability']) != {'tier', 'max_unsynced_age_ms', 'max_pending_completions'}
                or type(receipt['pid']) is not int or receipt['pid'] <= 0
                or type(receipt['started_unix_ns']) is not int or receipt['started_unix_ns'] <= 0):
            raise ValueError('owner tier or process identity mismatch')
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('owner durability receipt missing, stale or mismatched') from exc
    return receipt

def _running_owner_receipt(launch, tier, process, started_ns):
    receipt = _owner_receipt(cloud_campaign.read_owner_receipt(launch), launch, tier)
    boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if (process.poll() is not None or receipt['pid'] != process.pid
            or receipt['boot_id'] != boot_id or receipt['started_unix_ns'] < started_ns):
        raise ValueError('owner durability receipt is not from the running launch')
    return receipt


def _gid_netdev(path):
    try:
        return path.read_text().strip()
    except OSError as error:
        # Linux reports an unused or netdev-less GID slot as EINVAL.
        if error.errno != errno.EINVAL:
            raise
        return None



def _route(peer, source, transport, rdma_device, deadline):
    rows = json.loads(bootstrap._call(['ip', '-j', 'route', 'get', peer, 'from', source], deadline, 10))
    if not isinstance(rows, list) or len(rows) != 1:
        raise ValueError('one physical route to the peer is required')
    row = rows[0]
    interface = row.get('dev')
    if (not isinstance(interface, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', interface)
            or interface == 'lo' or row.get('prefsrc', row.get('src', source)) != source
            or row.get('type', 'unicast') != 'unicast'):
        raise ValueError('route does not use the admitted source interface')
    addresses = json.loads(bootstrap._call(['ip', '-j', 'address', 'show', 'dev', interface], deadline, 10))
    if (not isinstance(addresses, list) or len(addresses) != 1
            or addresses[0].get('ifname') != interface
            or not any(item.get('local') == source for item in addresses[0].get('addr_info', []))):
        raise ValueError('route interface does not own the admitted source address')
    def mapped_root():
        if transport == 'tcp':
            return Path('/sys/class/net') / interface / 'statistics', interface, None
        if transport != 'rdma':
            raise ValueError('invalid physical transport')
        if not isinstance(rdma_device, str) or not re.fullmatch(r'[A-Za-z0-9_]+', rdma_device):
            raise ValueError('invalid admitted RDMA device')
        ports = Path('/sys/class/infiniband') / rdma_device / 'ports'
        matches = []
        for port in ports.glob('[0-9]*'):
            names = port / 'gid_attrs' / 'ndevs'
            if names.is_dir() and any(_gid_netdev(p) == interface for p in names.iterdir() if p.is_file()):
                if not (port / 'state').read_text().strip().startswith('4:'):
                    raise ValueError('admitted RDMA port is not ACTIVE')
                matches.append(port)
        if len(matches) != 1:
            raise ValueError('admitted network interface lacks one matching RDMA port')
        return matches[0], rdma_device, matches[0].name

    root, device, port = mapped_root()
    stat = root.stat()
    identity = [stat.st_dev, stat.st_ino]
    families = _counter_families(root, transport)
    samples, unavailable = {}, []
    for family, (unit, paths) in families.items():
        values = []
        for path in paths:
            try:
                text = path.read_text().strip()
            except FileNotFoundError:
                values.append(None)
                continue
            if not text.isascii() or not text.isdecimal():
                raise ValueError('malformed physical counter')
            values.append(int(text))
        if values == [None, None]:
            unavailable.append(family)
        elif None in values:
            raise ValueError('partial physical counter pair')
        else:
            samples[family] = {'unit': unit, 'paths': [str(p) for p in paths],
                               'tx': values[0], 'rx': values[1]}
    counter_files = {}
    counter_files_truncated = {}
    if transport == 'rdma':
        for directory in ('counters', 'hw_counters'):
            names = []
            truncated = False
            try:
                with os.scandir(root / directory) as entries:
                    for entry in entries:
                        if len(entry.name) > 128 or len(names) == 256:
                            truncated = True
                            break
                        names.append(entry.name)
            except FileNotFoundError:
                counter_files[directory] = None
                counter_files_truncated[directory] = False
            else:
                counter_files[directory] = sorted(names)
                counter_files_truncated[directory] = truncated
    checked_root, checked_device, checked_port = mapped_root()
    checked_stat = checked_root.stat()
    if (checked_root != root or checked_device != device or checked_port != port
            or [checked_stat.st_dev, checked_stat.st_ino] != identity):
        raise ValueError('physical port changed during counter snapshot')
    result = {'source': source, 'peer': peer, 'transport': transport, 'interface': interface,
              'device': device, 'port': port, 'port_identity': identity,
              'counter_samples': samples, 'unavailable_counter_kinds': unavailable}
    if transport == 'rdma':
        result['counter_files'] = counter_files
        result['counter_files_truncated'] = counter_files_truncated
    return result


def _counter_families(root, transport):
    if transport == 'tcp':
        return {'netdev_bytes': ('octets', [root / 'tx_bytes', root / 'rx_bytes'])}
    if transport == 'rdma':
        return {
            'ib_pma': ('4_octets', [root / 'counters/port_xmit_data', root / 'counters/port_rcv_data']),
            'irdma_ipv4': ('octets', [root / 'hw_counters/ip4OutOctets', root / 'hw_counters/ip4InOctets']),
            'irdma_sends': ('operations', [root / 'hw_counters/OutRdmaSends', root / 'hw_counters/InRdmaSends']),
        }
    raise ValueError('invalid physical transport')

def _prepare_kernel(transport, deadline):
    if not shutil.which('nvme'):
        raise ValueError('live qualification requires nvme-cli')
    modules = ['nvmet', 'nvme_fabrics', 'nvme_tcp', 'nvmet_tcp']
    if transport == 'rdma':
        modules.extend(('rdma_cm', 'nvme_rdma', 'nvmet_rdma'))
    for module in modules:
        subprocess.run(['modprobe', module], check=True, timeout=bootstrap._remaining(deadline, 30))
    mount = subprocess.run(['findmnt', '-rn', '-M', '/sys/kernel/config', '-o', 'FSTYPE'],
                           capture_output=True, text=True, timeout=bootstrap._remaining(deadline, 10))
    if mount.returncode == 1:
        subprocess.run(['mount', '-t', 'configfs', 'configfs', '/sys/kernel/config'],
                       check=True, timeout=bootstrap._remaining(deadline, 30))
    elif mount.returncode or mount.stdout.strip() != 'configfs':
        raise ValueError('live qualification requires configfs')
    if not Path('/sys/kernel/config/nvmet').is_dir() or not Path('/dev/nvme-fabrics').is_char_device():
        raise ValueError('live target or initiator kernel interface unavailable')



def _delta(before, after, source, peer, transport):
    for value in (before, after):
        if (not isinstance(value, dict) or value.get('source') != source or value.get('peer') != peer
                or value.get('transport') != transport
                or not isinstance(value.get('interface'), str)
                or not re.fullmatch(r'[A-Za-z0-9_.-]+', value['interface'])
                or not isinstance(value.get('device'), str)
                or not re.fullmatch(r'[A-Za-z0-9_.-]+', value['device'])
                or not isinstance(value.get('port_identity'), list) or len(value['port_identity']) != 2
                or any(type(n) is not int or n < 0 for n in value['port_identity'])):
            raise ValueError('physical route evidence identity mismatch')
        if transport == 'tcp':
            if value.get('port') is not None or value['device'] != value['interface']:
                raise ValueError('invalid TCP port identity')
            root = Path('/sys/class/net') / value['interface'] / 'statistics'
        elif transport == 'rdma':
            if not isinstance(value.get('port'), str) or not re.fullmatch(r'[0-9]+', value['port']):
                raise ValueError('invalid RDMA port identity')
            root = Path('/sys/class/infiniband') / value['device'] / 'ports' / value['port']
        else:
            raise ValueError('invalid physical transport')
        families = _counter_families(root, transport)
        samples, unavailable = value.get('counter_samples'), value.get('unavailable_counter_kinds')
        if (not isinstance(samples, dict) or not isinstance(unavailable, list)
                or any(not isinstance(name, str) for name in unavailable)
                or len(set(unavailable)) != len(unavailable)
                or set(samples) & set(unavailable)
                or set(samples) | set(unavailable) != set(families)):
            raise ValueError('invalid physical counter availability')
        for name, sample in samples.items():
            unit, paths = families[name]
            if (not isinstance(sample, dict) or sample.get('unit') != unit
                    or sample.get('paths') != [str(path) for path in paths]
                    or any(type(sample.get(k)) is not int or sample[k] < 0 for k in ('tx', 'rx'))):
                raise ValueError('invalid physical counter sample')
    if any(before.get(key) != after.get(key) for key in ('interface', 'device', 'port', 'port_identity')):
        raise ValueError('physical route changed during qualification')
    if (set(before['counter_samples']) != set(after['counter_samples'])
            or set(before['unavailable_counter_kinds']) != set(after['unavailable_counter_kinds'])):
        raise ValueError('physical counter availability changed during qualification')
    advancing = set()
    for name, first in before['counter_samples'].items():
        last = after['counter_samples'][name]
        if any(last[k] < first[k] for k in ('tx', 'rx')):
            raise ValueError('physical counter decreased during qualification')
        if all(last[k] > first[k] for k in ('tx', 'rx')):
            advancing.add(name)
    return advancing


def admit(proof, transport, owner_fabric, coordinator_fabric, owner_control, coordinator_counters, owner_counters, *,
          target, tier, owner_launch, owner_receipt, archive_binary=False):
    scope = ('verified_spdk_namespace_not_physical_media_performance' if target == 'spdk'
             else 'verified_kernel_namespace_not_physical_media_performance')
    _owner_receipt(owner_receipt, owner_launch, tier)
    if (target not in ('kernel', 'spdk') or target == 'spdk' and transport != 'rdma'
            or not isinstance(proof, dict) or proof.get('schema_version') != 1
            or proof.get('kind') != 'live_nvmeof_qualification'
            or proof.get('expected_route') != transport or proof.get('scope') != scope
            or proof.get('durability_tier') != tier
            or proof.get('target_implementation', 'kernel') != target):
        raise ValueError('live qualification receipt missing or mismatched')
    if owner_launch.get('listen') != owner_control + ':9400':
        raise ValueError('owner launch endpoint mismatch')
    checks = proof.get('checks', {})
    required = ('scalar_write_delete', 'mixed_batch', 'ordered_batch_missing_exists',
                'checkpoint_iteration', 'flush_reopen', 'abrupt_tail_recovery', 'fence_drain', 'cache_miss_hit')
    if (not isinstance(checks, dict) or any(checks.get(name) is not True for name in required)
            or (archive_binary and checks.get('payload_rpc_fallback') is not False)):
        raise ValueError('live write/read/fence/recovery checks incomplete')
    samples = [proof.get(name) for name in ('start', 'before_close', 'recovery', 'after_miss', 'after_hit')]
    if any(not isinstance(sample, dict) for sample in samples):
        raise ValueError('live operation counter samples missing')
    start, before_close, recovery, after_miss, after_hit = samples
    abrupt = proof.get('abrupt_child')
    crash_recovery = proof.get('abrupt_recovery')
    if (not isinstance(abrupt, dict) or abrupt.get('schema_version') != 1
            or abrupt.get('kind') != 'acknowledged_unrooted_mixed_batch'
            or abrupt.get('exit_model') != 'process_exit_without_db_drop_or_checkpoint'
            or not isinstance(abrupt.get('start'), dict) or not isinstance(abrupt.get('end'), dict)
            or not isinstance(crash_recovery, dict)):
        raise ValueError('abrupt uncheckpointed recovery evidence missing')
    def counter(sample, name):
        value = sample.get(name)
        if type(value) is not int or value < 0:
            raise ValueError('live counter missing or invalid: ' + name)
        return value
    for field in ('completed_write_ops', 'completed_write_bytes'):
        if counter(before_close, field) <= counter(start, field):
            raise ValueError('live write did not complete')
    for field in ('completed_write_ops', 'completed_write_bytes'):
        if counter(abrupt['end'], field) <= counter(abrupt['start'], field):
            raise ValueError('abrupt child did not complete a live write')
    for field in ('recovery_read_ops', 'recovery_read_bytes'):
        if counter(crash_recovery, field) <= 0:
            raise ValueError('abrupt live recovery read did not complete')
    for field in ('completed_read_ops', 'completed_read_bytes'):
        if counter(after_miss, field) <= counter(recovery, field):
            raise ValueError('live cache miss did not read the namespace')
    if counter(after_hit, 'completed_read_ops') != counter(after_miss, 'completed_read_ops'):
        raise ValueError('live repeat read was not a cache hit')
    names = ('errors', 'unknown_writes', 'route_evidence_dropped', 'fence_evidence_dropped')
    if archive_binary:
        names += ('payload_rpc_requests',)
    for sample in (before_close, recovery, after_hit, abrupt['end'], crash_recovery):
        if any(counter(sample, name) != 0 for name in names):
            raise ValueError('live errors, unknown writes or dropped evidence')
    transition = proof.get('member_transition')
    if not isinstance(transition, dict):
        raise ValueError('member_transition evidence missing or invalid')
    limits = {'wal_file_bytes': 1024 * 1024 * 1024,
              'completed_write_bytes': 2 * 1024 * 1024 * 1024,
              'completed_write_ops': 2 * 1024 * 1024 * 1024,
              'writes_acknowledged': 65536}
    for name, maximum in limits.items():
        value = transition.get(name)
        if type(value) is not int or not 0 < value <= maximum:
            raise ValueError('member_transition invalid ' + name)
    if transition['completed_write_bytes'] <= transition['wal_file_bytes']:
        raise ValueError('member_transition writes did not exceed WAL file capacity')
    if any(transition.get(name) is not True for name in
           ('before_key_read', 'after_key_read', 'early_key_read', 'post_transition_delete')):
        raise ValueError('member_transition read/delete checks incomplete')
    writer_routes = transition.get('writer_routes')
    if not isinstance(writer_routes, list) or not 2 <= len(writer_routes) <= 1024:
        raise ValueError('member_transition writer_routes missing or invalid')
    members, namespaces = {}, {}
    for route in writer_routes:
        if not isinstance(route, dict) or not isinstance(route.get('writer'), dict):
            raise ValueError('member_transition writable writer record missing')
        writer = route['writer']
        epoch, file_index = writer.get('epoch'), writer.get('file_index')
        if (type(epoch) is not int or epoch != 1 or type(file_index) is not int
                or not 0 <= file_index <= (1 << 64) - 1):
            raise ValueError('member_transition writer epoch or file_index invalid')
        ident = route.get('namespace_identity')
        namespace = ident.get('uuid') if isinstance(ident, dict) else None
        if (not isinstance(namespace, list) or len(namespace) != 16
                or any(type(value) is not int or not 0 <= value <= 255 for value in namespace)
                or not any(namespace)):
            raise ValueError('member_transition namespace UUID invalid')
        namespace = tuple(namespace)
        member = (epoch, file_index)
        if ((member in members and members[member] != namespace)
                or (namespace in namespaces and namespaces[namespace] != member)):
            raise ValueError('member_transition inconsistent member/UUID identity')
        members[member] = namespace
        namespaces[namespace] = member
    initial = members.get((1, 0))
    if initial is None or not any(file_index > 0 and namespace != initial
                                  for (epoch, file_index), namespace in members.items()):
        raise ValueError('member_transition missing distinct first and later writable members')
    host_identity = None
    for sample in (before_close, after_hit, abrupt['end'], crash_recovery,
                   {'routes': writer_routes}):
        routes = sample.get('routes')
        if not isinstance(routes, list) or not routes:
            raise ValueError('observed kernel namespace route missing')
        for route in routes:
            if not isinstance(route, dict):
                raise ValueError('invalid observed kernel namespace route')
            host_id = route.get('host_id')
            host_nqn = route.get('host_nqn')
            if (not isinstance(host_id, list) or len(host_id) != 16
                    or any(type(value) is not int or not 0 <= value <= 255 for value in host_id)):
                raise ValueError('invalid observed initiator identity')
            host_uuid = uuid.UUID(bytes=bytes(host_id))
            if (host_uuid.version != 4 or host_nqn != 'nqn.2014-08.org.nvmexpress:uuid:' + str(host_uuid)
                    or (host_identity is not None and host_identity != host_nqn)):
                raise ValueError('observed initiator identity is inconsistent')
            host_identity = host_nqn
            ident = route.get('namespace_identity', {})
            if target == 'spdk' and (route.get('target_implementation') != 'spdk'
                                     or not isinstance(ident, dict) or ident.get('target_implementation') != 'spdk'):
                raise ValueError('observed route does not identify the SPDK target')
            if (route.get('transport') != transport or route.get('address') != owner_fabric
                    or route.get('service') != 4420 or not isinstance(ident, dict)
                    or not re.fullmatch(r'/dev/nvme[0-9]+n[0-9]+', str(ident.get('namespace_path', '')))
                    or not str(ident.get('nqn', '')).startswith('nqn.')
                    or type(ident.get('nsid')) is not int or ident['nsid'] < 1
                    or type(ident.get('bytes')) is not int or ident['bytes'] < 1
                    or type(ident.get('controller_instance')) is not int or ident['controller_instance'] < 0
                    or type(ident.get('block_size')) is not int or ident['block_size'] < 512
                    or ident['block_size'] & (ident['block_size'] - 1)
                    or not isinstance(ident.get('uuid'), list) or len(ident['uuid']) != 16
                    or any(type(value) is not int or not 0 <= value <= 255 for value in ident['uuid'])
                    or not any(ident['uuid'])):
                raise ValueError('observed namespace does not match the admitted owner route')
    fences = crash_recovery.get('fences')
    if not isinstance(fences, list) or not any(
            isinstance(f, dict) and f.get('partition') == 0 and type(f.get('authority_epoch')) is int
            and f['authority_epoch'] >= 2 and isinstance(f.get('target'), dict)
            and f['target'].get('fixture_only') is False
            and (target != 'spdk' or f['target'].get('target_implementation') == 'spdk')
            and type(f['target'].get('disable_drain_successes')) is int
            and f['target']['disable_drain_successes'] > 0 and f['target'].get('errors') == 0
            for f in fences):
        raise ValueError('kernel namespace fencing and drain evidence missing')
    shutdown = proof.get('target_shutdown')
    if (not isinstance(shutdown, list) or len(shutdown) != 1 or not isinstance(shutdown[0], dict)
            or shutdown[0].get('partition') != 0 or shutdown[0].get('endpoint') != owner_control + ':9400'
            or type(shutdown[0].get('epoch')) is not int or shutdown[0]['epoch'] < 2
            or not isinstance(shutdown[0].get('database'), list) or len(shutdown[0]['database']) != 16
            or any(type(value) is not int or not 0 <= value <= 255 for value in shutdown[0]['database'])
            or not any(shutdown[0]['database'])
            or not isinstance(shutdown[0].get('stats'), dict)
            or shutdown[0]['stats'].get('fixture_only') is not False
            or shutdown[0]['stats'].get('owned_namespace_count') != 0
            or shutdown[0]['stats'].get('errors') != 0):
        raise ValueError('owner namespace shutdown evidence missing')
    if shutdown[0]['database'] != owner_receipt['database'] or shutdown[0]['partition'] != owner_receipt['partition']:
        raise ValueError('shutdown identity differs from owner receipt')
    if target == 'spdk' and shutdown[0]['stats'].get('target_implementation') != 'spdk':
        raise ValueError('owner shutdown did not identify the SPDK target')
    if not isinstance(coordinator_counters, dict) or not isinstance(owner_counters, dict):
        raise ValueError('both physical host counter samples are required')
    coordinator_advancing = _delta(coordinator_counters.get('before'), coordinator_counters.get('after'), coordinator_fabric, owner_fabric, transport)
    owner_advancing = _delta(owner_counters.get('before'), owner_counters.get('after'), owner_fabric, coordinator_fabric, transport)
    common = sorted(coordinator_advancing & owner_advancing)
    if not common:
        raise ValueError('no common physical counter family advanced in both directions on both hosts')
    return {'proof': proof, 'owner_launch': owner_launch, 'owner_receipt': owner_receipt,
            'coordinator_counters': coordinator_counters, 'owner_counters': owner_counters,
            'counter_evidence': {'common_advancing_families': common}}


def admit_spdk_observation(observation, daemon, proof, owner_root, expected_sha):
    if not isinstance(observation, dict) or not isinstance(daemon, dict):
        raise ValueError('SPDK daemon and backing descriptor evidence missing')
    actual = observation.get('daemon')
    fields = ('pid', 'process_start_ticks', 'executable', 'executable_device', 'executable_inode',
              'binary_sha256', 'rpc_socket', 'rpc_device', 'rpc_inode')
    if (not isinstance(actual, dict) or any(actual.get(k) != daemon.get(k) for k in fields)
            or actual.get('binary_sha256') != expected_sha
            or any(type(actual.get(k)) is not int or actual[k] <= 0
                   for k in ('pid', 'process_start_ticks', 'executable_inode', 'rpc_inode'))):
        raise ValueError('SPDK daemon identity differs from the admitted executable')
    routes = proof.get('member_transition', {}).get('writer_routes', [])
    route_ids = {}
    for route in routes:
        namespace = route.get('namespace_identity', {})
        raw = namespace.get('uuid')
        if isinstance(raw, list) and len(raw) == 16:
            key = (namespace.get('nqn'), namespace.get('nsid'), str(uuid.UUID(bytes=bytes(raw))))
            binding = route.get('spdk')
            writer = route.get('writer')
            file_identity = writer.get('file_identity') if isinstance(writer, dict) else None
            if not isinstance(binding, dict) or not isinstance(file_identity, dict):
                continue
            value = (namespace.get('bytes'), namespace.get('block_size'), binding, file_identity)
            if key in route_ids and route_ids[key] != value:
                raise ValueError('writer namespace has conflicting authenticated owner bindings')
            route_ids[key] = value
    subsystems = observation.get('subsystems')
    bdevs = observation.get('bdevs')
    fds = observation.get('backing_fds')
    if not all(isinstance(value, list) for value in (subsystems, bdevs, fds)):
        raise ValueError('SPDK namespace and backing descriptor evidence missing')
    admitted_names = {}
    for subsystem in subsystems:
        for namespace in subsystem.get('namespaces', []):
            try:
                key = (subsystem.get('nqn'), namespace.get('nsid'), str(uuid.UUID(str(namespace.get('uuid')))))
            except (ValueError, TypeError, AttributeError):
                continue
            if key in route_ids:
                admitted_names[namespace.get('bdev_name')] = route_ids[key]
    for row in fds:
        if not isinstance(row, dict) or row.get('name') not in admitted_names or row.get('available') is not True:
            continue
        filename = row.get('filename')
        if not isinstance(filename, str) or not Path(filename).is_absolute() or '..' in Path(filename).parts:
            continue
        try:
            Path(filename).relative_to(owner_root)
        except ValueError:
            continue
        matches = [b for b in bdevs if isinstance(b, dict) and b.get('name') == row['name']]
        if len(matches) != 1:
            continue
        aio = matches[0].get('driver_specific', {}).get('aio', {})
        size, block_size, binding, file_identity = admitted_names[row['name']]
        if (binding.get('rpc_socket') != actual['rpc_socket']
                or binding.get('pid') != actual['pid']
                or binding.get('process_start_ticks') != actual['process_start_ticks']
                or binding.get('bdev_name') != row['name'] or binding.get('image_path') != filename
                or file_identity.get('path') != filename or file_identity.get('device') != row.get('device')
                or file_identity.get('inode') != row.get('inode') or file_identity.get('bytes') != size
                or file_identity.get('block_size') != block_size):
            continue
        bdev = matches[0]
        if (type(size) is not int or type(block_size) is not int
                or type(bdev.get('num_blocks')) is not int or type(bdev.get('block_size')) is not int
                or bdev['block_size'] != block_size or bdev['num_blocks'] * block_size != size
                or row.get('bytes') != size):
            continue
        descriptors = row.get('descriptors')
        if (aio.get('filename') != filename or aio.get('readonly') is not False
                or type(row.get('mode')) is not int or not stat.S_ISREG(row['mode'])
                or any(type(row.get(k)) is not int or row[k] <= 0 for k in ('inode', 'bytes'))
                or type(row.get('device')) is not int or row['device'] < 0
                or not isinstance(descriptors, list) or not descriptors
                or any(not isinstance(fd, dict) or type(fd.get('flags')) is not int
                       or fd['flags'] & os.O_ACCMODE != os.O_RDWR or not fd['flags'] & os.O_DIRECT
                       for fd in descriptors)):
            continue
        return
    raise ValueError('SPDK live writable direct backing inode is not bound to an observed writer namespace')


def _select_spdk_observation(observations, daemon, proof, owner_root, expected_sha):
    if not isinstance(observations, list) or not 1 <= len(observations) <= 16:
        raise ValueError('bounded SPDK namespace observations missing')
    for candidate in observations:
        try:
            admit_spdk_observation(candidate, daemon, proof, owner_root, expected_sha)
        except ValueError:
            continue
        return candidate
    raise ValueError('no SPDK observation matches an authenticated writer route')


def _published_spdk_snapshot(snapshot):
    """Ignore partially published bdevs while the target creates namespaces."""
    if not isinstance(snapshot, dict):
        return False
    bdevs, fds, subsystems = (snapshot.get(key) for key in ('bdevs', 'backing_fds', 'subsystems'))
    if not all(isinstance(value, list) for value in (bdevs, fds, subsystems)):
        return False
    for row in fds:
        if not isinstance(row, dict) or row.get('available') is not True:
            continue
        descriptors = row.get('descriptors')
        if (not isinstance(descriptors, list) or not descriptors
                or any(not isinstance(fd, dict) or type(fd.get('flags')) is not int
                       or fd['flags'] & os.O_ACCMODE != os.O_RDWR or not fd['flags'] & os.O_DIRECT
                       for fd in descriptors)):
            continue
        matching = [b for b in bdevs if isinstance(b, dict) and b.get('name') == row.get('name')]
        if len(matching) != 1:
            continue
        aio = matching[0].get('driver_specific', {}).get('aio', {})
        bdev = matching[0]
        if (aio.get('filename') != row.get('filename') or aio.get('readonly') is not False
                or not isinstance(row.get('filename'), str)
                or type(row.get('mode')) is not int or not stat.S_ISREG(row['mode'])
                or type(row.get('inode')) is not int or row['inode'] <= 0
                or type(row.get('device')) is not int or row['device'] < 0
                or type(row.get('bytes')) is not int or row['bytes'] <= 0
                or type(bdev.get('num_blocks')) is not int or bdev['num_blocks'] <= 0
                or type(bdev.get('block_size')) is not int or bdev['block_size'] <= 0
                or bdev['num_blocks'] * bdev['block_size'] != row['bytes']):
            continue
        for subsystem in subsystems:
            if not isinstance(subsystem, dict) or not str(subsystem.get('nqn', '')).startswith('nqn.'):
                continue
            namespaces = subsystem.get('namespaces')
            if not isinstance(namespaces, list):
                continue
            for namespace in namespaces:
                if not isinstance(namespace, dict) or namespace.get('bdev_name') != row.get('name'):
                    continue
                try:
                    ns_uuid = uuid.UUID(str(namespace.get('uuid')))
                except (ValueError, TypeError, AttributeError):
                    continue
                if ns_uuid.int and type(namespace.get('nsid')) is int and namespace['nsid'] > 0:
                    return True
    return False


def _write_secret(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open('xb') as output:
        os.fchmod(output.fileno(), 0o600)
        output.write(data)


def _copy_tls(prepared, root, tls):
    for field in ('ca', 'cert', 'key'):
        name = tls.get(field)
        if not name:
            continue
        target = Path(name)
        if target.parent != root / 'tls' or not re.fullmatch(r'[A-Za-z0-9_.-]+', target.name):
            raise ValueError('TLS path escapes the qualification root')
        data = (prepared / 'tls' / target.name).read_bytes()
        if not data or len(data) > 65536:
            raise ValueError('invalid bounded TLS material')
        if not target.exists():
            _write_secret(target, data)


def _run(package, cfg, deadline, store, own, own_id, identity, api, info, zone, transport, tier):
    import yaml
    if transport not in ('tcp', 'rdma'):
        raise ValueError('invalid live qualification transport')
    target_kind = cfg.get('qualification_target', 'kernel')
    if target_kind != 'kernel':
        raise ValueError('only the kernel live target is supported')
    owner = max(own, cfg['peer_instance'])
    own_is_owner = own == owner
    peer_info = api.request('GET', f'zones/{zone}/instances/{cfg["peer_instance"]}')
    def addresses(instance):
        interfaces = instance.get('networkInterfaces', [])
        found = {}
        for item in interfaces:
            kind = item.get('nicType')
            if kind in ('GVNIC', 'IRDMA'):
                if kind in found:
                    raise ValueError('duplicate qualified network interface')
                address = ipaddress.IPv4Address(item.get('networkIP'))
                if address.is_loopback or address.is_multicast or address.is_unspecified:
                    raise ValueError('invalid qualification network address')
                found[kind] = str(address)
        if set(found) != {'GVNIC', 'IRDMA'}:
            raise ValueError('qualification host network identities missing')
        return found
    own_addrs, peer_addrs = addresses(info), addresses(peer_info)
    if (info.get('name') != own or str(info.get('id')) != own_id
            or peer_info.get('name') != cfg['peer_instance']
            or str(peer_info.get('id')) != cfg['peer_instance_id']
            or peer_addrs['IRDMA'] != cfg['peer_ip']):
        raise ValueError('qualification host numeric identity mismatch')
    own_fabric = own_addrs['IRDMA' if transport == 'rdma' else 'GVNIC']
    peer_fabric = peer_addrs['IRDMA' if transport == 'rdma' else 'GVNIC']
    owner_fabric = own_fabric if own_is_owner else peer_fabric
    coordinator_fabric = peer_fabric if own_is_owner else own_fabric
    owner_control = own_addrs['GVNIC'] if own_is_owner else peer_addrs['GVNIC']
    host = {'name': own, 'zone': zone, 'id': own_id}
    storage_cfg = {'machine_type': cfg['machine_type'], 'run_id': cfg['run_id'], 'deadline_utc': cfg['deadline_utc']}
    gcp_worker.prepare_storage(storage_cfg, host, api)
    _prepare_kernel(transport, deadline)
    label = _label(transport, target_kind, tier)
    root = Path('/mnt/tidehunter-kv-comparison') / cfg['run_id'] / label
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    prefix = 'runs/' + cfg['run_id'] + '/qualification/' + label + '/'
    capsule_key = 'runs/' + cfg['run_id'] + '/private-' + label + '.json'
    binary = package / 'runtime/bin/kv-research'
    archive_binary = 'case_binaries' in json.loads((package / 'manifest.json').read_text())
    environment = dict(os.environ, LD_LIBRARY_PATH=str(package / 'runtime/lib'))
    def barrier(name):
        return bootstrap._wait(store, prefix + name + '.json', deadline, identity)
    def put(name, data):
        bootstrap._put(store, prefix + name + '.json', {**identity, **data})
    if not own_is_owner:
        preparation = {'guest_root': str(root), 'addresses': [owner_control + ':9400'], 'transport': {'kind': 'tcp'},
            'owner_wal_geometry': 'research',
            'durability_tier': tier, 'max_batches_per_partition': 16384, 'max_admitted_batches': 16384, 'max_index_bytes': 128 * 1024**2,
            'max_epochs': 4, 'coordinator_cache_bytes': 4 * 1024**2,
            'owner_wal_member_read': 'async', 'polling': 'adaptive',
            'live_nvmeof': {'transport': transport, 'addresses': [owner_fabric], 'service': 4420, 'port_id': 1000,
                'commissioning_service': 4421, 'commissioning_port_id': 1001,
                'max_inflight_requests': 32, 'max_inflight_bytes': 16 * 1024**2,
                'connect_timeout_ms': 30000, 'io_timeout_ms': 30000}}
        if archive_binary:
            preparation.update(owner_read_workers=2, owner_read_resident_first=False,
                               owner_read_memory_allowance_bytes=512 * 1024**2)
            preparation['history_persistence'] = 'wal_replay'
            preparation['live_nvmeof'].update(write_mode='leased', export_mode='per_owner')
        input_path = root / 'prepare.json'
        input_path.write_text(json.dumps(preparation))
        prepared = root / 'prepared'
        subprocess.run([str(binary), '--prepare-external-case', str(input_path), '--output', str(prepared)],
                       env=environment, check=True, timeout=bootstrap._remaining(deadline, 90))
        remote = json.loads((prepared / 'remote.json').read_text())
        launch = yaml.safe_load((prepared / 'node-0/launch.yaml').read_text())
        launch['launch_nonce'] = secrets.token_hex(16)
        if launch.get('listen') != owner_control + ':9400' or launch.get('ready_file') != str(root / 'node-0/ready'):
            raise ValueError('prepared owner launch identity mismatch')
        _copy_tls(prepared, root, remote['tls'])
        launch_text = yaml.safe_dump(launch, sort_keys=False)
        capsule = {'identity': identity, 'launch': launch_text,
                   'launch_sha256': hashlib.sha256(launch_text.encode()).hexdigest(), 'tls': {}}
        for field in ('ca', 'cert', 'key'):
            name = launch['tls'].get(field)
            if name:
                path = Path(name)
                if path.parent != root / 'node-0/tls' or not re.fullmatch(r'[A-Za-z0-9_.-]+', path.name):
                    raise ValueError('owner TLS path outside root')
                data = (prepared / 'node-0/tls' / path.name).read_bytes()
                if not data or len(data) > 65536:
                    raise ValueError('invalid owner TLS material')
                capsule['tls'][path.name] = base64.b64encode(data).decode()
        bootstrap._put(store, capsule_key, capsule)
        try:
            ready = barrier('owner-ready')
            _owner_receipt(ready.get('owner_receipt'), launch, tier)
        finally:
            meta = store.head(capsule_key)
            store.delete(capsule_key, if_generation_match=meta['generation'])
        before = _route(owner_fabric, coordinator_fabric, transport, cfg['rdma_device'], deadline)
        put('raw-coordinator-before', {'status': 'partial_not_admitted', 'route': before})
        put('run', {})
        barrier('owner-before')
        command = {'remote_config': remote, 'coordinator_path': str(root / 'database'),
                   'namespace': list(uuid.uuid4().bytes), 'expected_route': transport, 'value_bytes': 1024,
                   'durability_tier': tier}
        if archive_binary:
            command['target_implementation'] = target_kind
        input_file = root / 'qualification-input.json'
        input_file.write_text(json.dumps(command))
        output = root / 'result'
        with (root / 'qualification.log').open('wb') as log:
            subprocess.run([str(binary), '--experimental-live-nvmeof-qualification', str(input_file), '--output', str(output)],
                           env=environment, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=bootstrap._remaining(deadline, 600))
        _export_partial(root, store, cfg, identity, own, own_id, transport, target_kind + '-complete',
                        ('qualification.log', 'result/qualification.json', 'result/qualification-progress.json',
                         'result/qualification-child-progress.json', 'result/abrupt-child-ready.json'), deadline, tier=tier)
        proof = json.loads((output / 'qualification.json').read_text())
        after = _route(owner_fabric, coordinator_fabric, transport, cfg['rdma_device'], deadline)
        put('raw-coordinator-after', {'status': 'partial_not_admitted', 'route': after})
        put('run-done', {})
        peer = barrier('owner-after')
        if peer.get('launch_sha256') != capsule['launch_sha256']:
            raise ValueError('owner launch digest differs from the prepared capsule')
        if peer.get('owner_receipt') != ready['owner_receipt']:
            raise ValueError('owner receipt changed during qualification')
        result = admit(proof, transport, owner_fabric, coordinator_fabric, owner_control,
                       {'before': before, 'after': after}, {'before': peer['before'], 'after': peer['after']},
                       target=target_kind, tier=tier, owner_launch=launch,
                       owner_receipt=peer.get('owner_receipt'), archive_binary=archive_binary)
        if target_kind == 'spdk':
            metadata = json.loads((package / 'runtime/spdk/spdk.json').read_text())
            observations, daemon = peer.get('spdk_observations'), peer.get('spdk_daemon')
            observation = _select_spdk_observation(observations, daemon, proof, root / 'node-0/storage',
                                                   metadata['files']['bin/nvmf_tgt']['sha256'])
            result.update(spdk_observation=observation, spdk_daemon=daemon,
                          launch_sha256=capsule['launch_sha256'])
        put('admitted', {'result': result})
        barrier('owner-stopped')
        return result
    capsule = bootstrap._wait(store, capsule_key, deadline, {})
    if capsule.get('identity') != identity or not isinstance(capsule.get('tls'), dict):
        raise ValueError('owner capsule identity mismatch')
    node = root / 'node-0'
    node.mkdir(mode=0o700)
    (node / 'live-nvmeof').mkdir(mode=0o700)
    launch_text = capsule.get('launch')
    if (not isinstance(launch_text, str) or len(launch_text) > 65536
            or capsule.get('launch_sha256') != hashlib.sha256(launch_text.encode()).hexdigest()):
        raise ValueError('invalid owner launch capsule or launch digest')
    launch = yaml.safe_load(launch_text)
    target = launch.get('node', {}).get('live_target', {})
    if (launch.get('listen') != owner_control + ':9400' or launch.get('ready_file') != str(node / 'ready')
            or launch.get('stop_file') != str(node / 'stop') or launch.get('node', {}).get('root') != str(node / 'storage')
            or target.get('address') != owner_fabric or target.get('transport') != transport
            or target.get('service') != 4420 or target.get('port_id') != 1000
            or target.get('commissioning_service') != 4421 or target.get('commissioning_port_id') != 1001
            or target.get('state_dir') != str(node / 'live-nvmeof')):
        raise ValueError('owner launch does not match admitted identities')
    expected_spdk = None
    if target_kind == 'spdk':
        token = uuid.uuid5(uuid.NAMESPACE_URL, cfg['run_id'] + ':' + owner + ':spdk-qualification:' + tier).hex
        expected_spdk = {'rpc_socket': '/run/tidehunter-spdk-live/' + token + '/rpc.sock',
                         'executable': str(package / 'runtime/spdk/bin/nvmf_tgt')}
    if target.get('spdk') != expected_spdk:
        raise ValueError('owner live target implementation differs from qualification selection')
    if (not isinstance(launch.get('durability'), dict) or launch['durability'].get('tier') != tier
            or not isinstance(launch.get('launch_nonce'), str) or not launch['launch_nonce']):
        raise ValueError('owner launch tier or nonce mismatch')
    for name, encoded in capsule['tls'].items():
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or not isinstance(encoded, str) or len(encoded) > 90000:
            raise ValueError('invalid owner TLS capsule member')
        data = base64.b64decode(encoded, validate=True)
        if not data or len(data) > 65536:
            raise ValueError('invalid owner TLS capsule member')
        _write_secret(node / 'tls' / name, data)
    launch_file = node / 'launch.yaml'
    _write_secret(launch_file, launch_text.encode())
    daemon_script = package / 'source/experiments/nvmeof/live_spdk_daemon.py'
    daemon_state = node / 'live-spdk-daemon'
    daemon = None
    if expected_spdk:
        daemon = json.loads(bootstrap._call([
            'python3', '-B', str(daemon_script), 'start', '--state', str(daemon_state),
            '--socket', expected_spdk['rpc_socket'], '--executable', expected_spdk['executable'],
            '--deadline-epoch', str(deadline)], deadline, 45))
    log = (node / 'owner.log').open('wb')
    started_ns = time.time_ns()
    process = subprocess.Popen([str(binary), '--experimental-storage-node', str(launch_file)],
                               env=environment, stdin=subprocess.DEVNULL, stdout=log,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        while not (node / 'ready').is_file():
            if process.poll() is not None:
                raise RuntimeError('live owner exited before readiness')
            time.sleep(min(0.2, bootstrap._remaining(deadline, 1)))
        if (node / 'ready').read_text().strip() != owner_control + ':9400':
            raise ValueError('live owner readiness endpoint mismatch')
        owner_helper = None
        if daemon:
            helper_path = package / 'source/experiments/kv-comparison/nvme_campaign_owner.py'
            helper_spec = importlib.util.spec_from_file_location('qualification_nvme_owner', helper_path)
            owner_helper = importlib.util.module_from_spec(helper_spec)
            helper_spec.loader.exec_module(owner_helper)
            if process.poll() is not None:
                raise RuntimeError('live SPDK owner exited before identity capture')
            owner_helper.capture_live_owner_identity(node, launch_file, {'MainPID': str(process.pid)})
        owner_receipt = _running_owner_receipt(launch, tier, process, started_ns)
        put('owner-ready', {'owner_receipt': owner_receipt})
        barrier('run')
        before = _route(coordinator_fabric, owner_fabric, transport, cfg['rdma_device'], deadline)
        put('raw-owner-before', {'status': 'partial_not_admitted', 'route': before})
        put('owner-before', {})
        _export_partial(root, store, cfg, identity, own, own_id, transport, 'owner-before-wait',
                        ('node-0/owner.log',), deadline, tier=tier)
        observations = []
        observed_topologies = set()
        observation_bytes = 0
        stop_sampling = threading.Event()
        def sample_spdk():
            nonlocal observation_bytes
            while not stop_sampling.is_set():
                try:
                    snapshot = json.loads(subprocess.check_output(
                        ['python3', '-B', str(daemon_script), 'observe', '--state', str(daemon_state)],
                        text=True, timeout=10))
                    if _published_spdk_snapshot(snapshot):
                        file_ids = [{k: row.get(k) for k in ('name', 'filename', 'device', 'inode', 'bytes', 'mode', 'available', 'descriptors')}
                                    for row in snapshot['backing_fds'] if isinstance(row, dict)]
                        topology = json.dumps([snapshot['subsystems'], file_ids], sort_keys=True, separators=(',', ':'))
                        encoded_size = len(json.dumps(snapshot, separators=(',', ':')).encode())
                        if (topology not in observed_topologies and len(observations) < 16
                                and observation_bytes + encoded_size <= 2 * 1024 * 1024):
                            observations.append(snapshot)
                            observed_topologies.add(topology)
                            observation_bytes += encoded_size
                except (OSError, ValueError, subprocess.SubprocessError):
                    pass
                stop_sampling.wait(0.1)
        sampler = threading.Thread(target=sample_spdk, daemon=True) if daemon else None
        if sampler:
            sampler.start()
        try:
            barrier('run-done')
        finally:
            stop_sampling.set()
            if sampler:
                sampler.join(timeout=11)
        if sampler and (sampler.is_alive() or not observations):
            raise ValueError('live SPDK target backing descriptors were not observed during qualification')
        after = _route(coordinator_fabric, owner_fabric, transport, cfg['rdma_device'], deadline)
        put('raw-owner-after', {'status': 'partial_not_admitted', 'route': after})
        if _running_owner_receipt(launch, tier, process, started_ns) != owner_receipt:
            raise ValueError('owner receipt changed during qualification')
        put('owner-after', {'before': before, 'after': after, 'launch_sha256': capsule['launch_sha256'],
                            'owner_receipt': owner_receipt,
                            **({'spdk_daemon': daemon, 'spdk_observations': observations} if daemon else {})})
        _export_partial(root, store, cfg, identity, own, own_id, transport, 'owner-before-admission',
                        ('node-0/owner.log',), deadline, tier=tier)
        barrier('admitted')
        (node / 'stop').touch(exist_ok=False)
        process.wait(timeout=bootstrap._remaining(deadline, 30))
        if process.returncode != 0:
            raise RuntimeError('live owner did not shut down cleanly')
        if daemon:
            owner_helper.admit_live_shutdown(node, launch_file, launch, 0,
                {'MainPID': '0', 'SubState': 'exited', 'Result': 'success', 'ExecMainStatus': '0'})
            observation = bootstrap._call(['python3', '-B', str(daemon_script), 'observe',
                '--state', str(daemon_state), '--diagnostics'], deadline, 30)
            (node / 'daemon-before-stop.json').write_text(observation)
            _export_partial(root, store, cfg, identity, own, own_id, transport, 'owner-before-daemon-stop',
                ('node-0/owner.log', 'node-0/daemon-before-stop.json',
                 'node-0/launch.live-target-shutdown.json', 'node-0/live-spdk-daemon/state.json'), deadline, tier=tier)
            bootstrap._call(['python3', '-B', str(daemon_script), 'stop', '--state', str(daemon_state),
                '--owner-proof', str(launch_file.with_suffix('.live-target-shutdown.json')),
                '--initiator-drained'], deadline, 45)
        put('owner-stopped', {})
        return None
    finally:
        bootstrap._stop_processes([process])
        log.close()


def _export_partial(root, store, cfg, identity, own, own_id, transport, stage, files, deadline, failure=None, *, tier):
    label = _label(transport, cfg.get('qualification_target', 'kernel'), tier)
    prefix = 'runs/' + cfg['run_id'] + '/qualification/' + label + '/partial-' + own + '/' + stage + '/'
    for name in files:
        try:
            path = root / name
            if not path.is_file() or path.is_symlink():
                continue
            if deadline - time.time() < 65:
                raise TimeoutError('insufficient partial evidence export budget')
            with path.open('rb') as source:
                size = os.fstat(source.fileno()).st_size
                if size > 1024 * 1024:
                    source.seek(size - 1024 * 1024)
                data = source.read(1024 * 1024)
            if deadline - time.time() < 65:
                raise TimeoutError('insufficient partial evidence export budget')
            store.put(prefix + name.replace('/', '-'), data, if_generation_match=0,
                      metadata={**identity, 'instance_id': str(own_id),
                                'status': 'partial_not_admitted',
                                'original_size': str(size), 'truncated': str(size > len(data)).lower()})
        except Exception as export_error:
            if failure is None:
                raise
            failure.add_note('partial evidence export failed for ' + name + ': ' + str(export_error))
            if isinstance(export_error, TimeoutError):
                break


def run(package, cfg, deadline, store, own, own_id, identity, api, info, zone, transport, tier):
    label = _label(transport, cfg.get('qualification_target', 'kernel'), tier)
    root = Path('/mnt/tidehunter-kv-comparison') / cfg['run_id'] / label
    existed = root.exists()
    try:
        # Leave time before the guest's hard deadline for process cleanup and evidence export.
        work_deadline = deadline - 120
        bootstrap._remaining(work_deadline, 1)
        return _run(package, cfg, work_deadline, store, own, own_id, identity, api, info, zone, transport, tier)
    except Exception as failure:
        try:
            if not existed and root.is_dir() and transport in ('tcp', 'rdma'):
                _export_partial(root, store, cfg, identity, own, own_id, transport, 'failure',
                                ('node-0/owner.log', 'node-0/daemon-before-stop.json',
                                 'node-0/launch.live-target-shutdown.json', 'node-0/live-spdk-daemon/state.json',
                                 'node-0/live-spdk-daemon/exit.json', 'result/qualification.json',
                                 'result/qualification-progress.json', 'result/qualification-child-progress.json',
                                 'result/abrupt-child-ready.json', 'qualification.log'), deadline - 50, failure, tier=tier)
        except Exception as export_error:
            failure.add_note('partial evidence export setup failed: ' + str(export_error))
        raise
