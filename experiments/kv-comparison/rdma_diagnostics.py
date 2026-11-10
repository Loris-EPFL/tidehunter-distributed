"""Bounded, read-only device-wide RDMA sysfs snapshots; no counter interpretation."""
from datetime import datetime, timezone
from itertools import islice
import os
from pathlib import Path
import stat
import time
import json
import re
import selectors
import subprocess

MAX_COMMAND_BYTES = 262144
COMMAND_TIMEOUT = 10
CAP_FIELDS = ('fw_ver', 'vendor_id', 'vendor_part_id', 'hw_ver', 'max_mr',
              'max_qp', 'max_qp_wr', 'max_cq', 'max_cqe', 'max_pd',
              'max_sge', 'max_sge_rd', 'max_mr_size', 'page_size_cap')


def _bounded_command(argv):
    """Capture a bounded command transcript without buffering unlimited output."""
    result = {'argv': argv, 'available': False, 'truncated': False, 'raw': '', 'error': None}
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'[:300]
        return result
    chunks = []
    size = 0
    deadline = time.monotonic() + COMMAND_TIMEOUT
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    result['error'] = 'command timed out'
                    break
                data = os.read(process.stdout.fileno(), min(65536, MAX_COMMAND_BYTES + 1 - size))
                if not data:
                    break
                chunks.append(data)
                size += len(data)
                if size > MAX_COMMAND_BYTES:
                    result['truncated'] = True
                    result['error'] = 'command output exceeded limit'
                    break
        if result['error']:
            process.kill()
        try:
            code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            result['error'] = 'command timed out'
            code = process.returncode
        result['returncode'] = code
        result['raw'] = b''.join(chunks)[:MAX_COMMAND_BYTES].decode('utf-8', errors='replace')
        if code != 0 and not result['error']:
            result['error'] = f'command exited {code}'
        result['available'] = result['error'] is None
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.kill()
            process.wait()
    return result


def capture_resources(device=None):
    """Read device capabilities and host-wide resource summaries at boundaries only.

    max_cqe is a per-CQ limit, not an aggregate CQE capacity. Counts reflect
    whatever objects the kernel reports to this process, not hardware occupancy.
    """
    if device is not None and (not isinstance(device, str) or
                               not re.fullmatch(r'[A-Za-z0-9_]+', device)):
        raise ValueError('invalid RDMA device')
    argv = ['ibv_devinfo', '-v']
    if device is not None:
        argv += ['-d', device]
    caps = _bounded_command(argv)
    caps['fields'] = {}
    caps['devices'] = []
    if caps['available']:
        current = None
        seen = set()
        for line in caps['raw'].splitlines():
            match = re.fullmatch(r'\s*([A-Za-z0-9_]+):\s*(.*?)\s*', line)
            if not match:
                continue
            key, value = match.groups()
            if key == 'hca_id':
                if not re.fullmatch(r'[A-Za-z0-9_]+', value) or value in seen:
                    caps['available'] = False
                    caps['error'] = 'invalid or duplicate HCA identity'
                    break
                seen.add(value)
                current = {'device': value, 'fields': {}}
                caps['devices'].append(current)
            elif key in CAP_FIELDS:
                if current is None or key in current['fields'] or not value:
                    caps['available'] = False
                    caps['error'] = 'unidentified, duplicate or empty capability: ' + key
                    break
                current['fields'][key] = value
        if caps['available'] and (not caps['devices'] or any(not row['fields'] for row in caps['devices'])):
            caps['available'] = False
            caps['error'] = 'no recognized capabilities for one or more devices'
        if caps['available'] and device is not None:
            if len(caps['devices']) != 1 or caps['devices'][0]['device'] != device:
                caps['available'] = False
                caps['error'] = 'capability device identity mismatch'
            else:
                caps['fields'] = caps['devices'][0]['fields']
        elif caps['available'] and len(caps['devices']) == 1:
            caps['fields'] = caps['devices'][0]['fields']
    resources = _bounded_command(['rdma', '-j', 'resource', 'show'])
    resources['devices'] = []
    if resources['available']:
        try:
            rows = json.loads(resources['raw'])
            if not isinstance(rows, list) or not rows:
                raise ValueError('resource summary has no devices')
            seen = set()
            for row in rows:
                if (not isinstance(row, dict) or not isinstance(row.get('ifname'), str)
                        or not row['ifname'] or row['ifname'] in seen
                        or type(row.get('ifindex')) is not int):
                    raise ValueError('invalid resource device summary')
                seen.add(row['ifname'])
                counts, limits = {}, {}
                for name, value in row.items():
                    if name in ('ifname', 'ifindex'):
                        continue
                    if type(value) is not int or value < 0:
                        raise ValueError('invalid resource count')
                    (limits if name.endswith('-max') else counts)[name] = value
                if not counts:
                    raise ValueError('empty resource summary')
                resources['devices'].append({'device': row['ifname'],
                                             'ifindex': row['ifindex'],
                                             'counts': counts, 'reported_limits': limits})
        except (ValueError, TypeError) as exc:
            resources['devices'] = []
            resources['available'] = False
            resources['error'] = str(exc)[:300]
    return {'schema_version': 1, 'scope': 'host_device_wide',
            'device_requested': device, 'capabilities': caps, 'resources': resources}

MAX_COUNTERS = 128
MAX_DEVICES = 32
MAX_PORTS = 32
MAX_BYTES = 128


def capture(sys_root=Path('/sys')):
    root = Path(sys_root).resolve()
    started = {'utc': datetime.now(timezone.utc).isoformat(), 'monotonic_ns': time.monotonic_ns()}
    result = {'schema_version': 1, 'scope': 'host_device_wide', 'started': started,
              'available': False, 'truncated': False, 'devices': [], 'errors': []}
    remaining = MAX_COUNTERS

    def safe(path):
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise ValueError('sysfs path escapes admitted root')
        return resolved

    def entries(path, limit):
        with os.scandir(safe(path)) as directory:
            items = list(islice(directory, limit + 1))
        if len(items) > limit:
            result['truncated'] = True
            items = items[:limit]
        return sorted((item.name for item in items))

    def field(path, numeric=False):
        try:
            target = safe(path)
            if not stat.S_ISREG(target.stat().st_mode):
                raise ValueError('not a regular sysfs attribute')
            with target.open('rb') as stream:
                data = stream.read(MAX_BYTES)
            if len(data) == MAX_BYTES:
                raise ValueError('attribute reached 128-byte read limit; completeness unknown')
            value = data.decode('ascii').strip()
            if not value:
                raise ValueError('empty attribute')
            if numeric:
                if not value.isascii() or not value.isdecimal():
                    raise ValueError('counter is not a nonnegative decimal integer')
                value = int(value)
            return {'available': True, 'value': value}
        except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
            return {'available': False, 'reason': (type(exc).__name__ + ': ' + str(exc))[:300]}

    base = root / 'class' / 'infiniband'
    try:
        devices = entries(base, MAX_DEVICES)
        for device in devices:
            path = base / device
            try:
                device_path = str(safe(path))
            except (OSError, ValueError, RuntimeError):
                device_path = None
            row = {'name': device, 'sysfs_path': device_path,
                   'node_guid': field(path / 'node_guid'), 'ports': []}
            result['devices'].append(row)
            try:
                ports = entries(path / 'ports', MAX_PORTS)
            except (OSError, ValueError, RuntimeError) as exc:
                row['ports_error'] = (type(exc).__name__ + ': ' + str(exc))[:300]
                continue
            for port in ports:
                if not port.isascii() or not port.isdecimal():
                    result['errors'].append('ignored nonnumeric port name')
                    continue
                port_row = {'port': port, 'state': field(path / 'ports' / port / 'state'),
                            'link_layer': field(path / 'ports' / port / 'link_layer'), 'groups': {}}
                row['ports'].append(port_row)
                for group in ('counters', 'hw_counters'):
                    group_path = path / 'ports' / port / group
                    try:
                        names = entries(group_path, remaining)
                        fields = {}
                        for name in names:
                            fields[name] = field(group_path / name, numeric=True)
                            remaining -= 1
                        port_row['groups'][group] = {'available': True, 'fields': fields}
                    except (OSError, ValueError, RuntimeError) as exc:
                        port_row['groups'][group] = {'available': False,
                            'reason': (type(exc).__name__ + ': ' + str(exc))[:300]}
        if not devices:
            result['errors'].append('no RDMA devices found')
        result['available'] = any(field_value.get('available')
                                  for device in result['devices'] for port in device['ports']
                                  for group in port['groups'].values()
                                  for field_value in group.get('fields', {}).values())
        if not result['available'] and devices:
            result['errors'].append('no readable numeric counter attributes')
    except (OSError, ValueError, RuntimeError) as exc:
        result['errors'].append((type(exc).__name__ + ': ' + str(exc))[:300])
    result['finished'] = {'utc': datetime.now(timezone.utc).isoformat(), 'monotonic_ns': time.monotonic_ns()}
    return result


# Only the common PMA ABI has portable semantics. Provider-specific fields
# remain in capture() as raw values: names alone do not establish semantics.
PMA_UNITS = {
    'port_xmit_wait': 'ticks (credit starvation OR arbitration)',
    'port_xmit_data': '4-octet units', 'port_rcv_data': '4-octet units',
    'port_xmit_packets': 'packets', 'port_rcv_packets': 'packets',
    'port_rcv_errors': 'packets', 'port_rcv_remote_physical_errors': 'packets',
    'port_rcv_switch_relay_errors': 'packets', 'port_xmit_constraint_errors': 'packets',
    'port_rcv_constraint_errors': 'packets', 'port_xmit_discards': 'packets',
    'symbol_error': 'minor link errors', 'link_error_recovery': 'recoveries',
    'link_downed': 'link down events', 'local_link_integrity_errors': 'events',
    'excessive_buffer_overrun_errors': 'events', 'VL15_dropped': 'packets',
}


def compare(first, last):
    """Per-field deltas; never infer identity, missing values, wraps or provider ABI."""
    result = {'scope': 'host device/port-wide, not QP or workload-specific',
              'source': 'https://github.com/torvalds/linux/blob/master/Documentation/ABI/stable/sysfs-class-infiniband',
              'ports': [], 'truncated': first.get('truncated', False) or last.get('truncated', False)}
    def ports(snapshot):
        return {(device['name'], port['port']): (device, port)
                for device in snapshot.get('devices', []) for port in device.get('ports', [])}
    before, after = ports(first), ports(last)
    for key in sorted(before.keys() | after.keys()):
        row = {'device': key[0], 'port': key[1], 'available': False, 'fields': {}}
        result['ports'].append(row)
        if key not in before or key not in after:
            row['reason'] = 'port missing at boundary'
            continue
        d0, p0 = before[key]; d1, p1 = after[key]
        if (not d0.get('sysfs_path') or d0.get('sysfs_path') != d1.get('sysfs_path')
                or not d0.get('node_guid', {}).get('available')
                or d0.get('node_guid') != d1.get('node_guid')):
            row['reason'] = 'device identity unavailable or changed'
            continue
        row['identity'] = {'sysfs_path': d0['sysfs_path'], 'node_guid': d0['node_guid']['value']}
        row['available'] = True
        for group in ('counters', 'hw_counters'):
            g0 = p0.get('groups', {}).get(group, {})
            g1 = p1.get('groups', {}).get(group, {})
            f0, f1 = g0.get('fields', {}), g1.get('fields', {})
            fields = {}
            row['fields'][group] = fields
            names = f0.keys() | f1.keys()
            if group == 'counters':
                names |= PMA_UNITS.keys()
            for name in sorted(names):
                unit = PMA_UNITS.get(name) if group == 'counters' else None
                a, b = f0.get(name, {}), f1.get(name, {})
                entry = {'available': False, 'unit': unit, 'family': group}
                fields[name] = entry
                if unit is None:
                    entry['begin_raw'] = a
                    entry['end_raw'] = b
                    entry['reason'] = ('provider refresh interval is a gauge' if name == 'lifespan'
                                       else 'field semantics and units unknown; no delta inferred')
                elif not g0.get('available') or not g1.get('available') or not a.get('available') or not b.get('available'):
                    entry['reason'] = 'counter unavailable at boundary'
                elif b['value'] < a['value']:
                    entry['reason'] = 'counter reset or wrap'
                else:
                    entry.update(available=True, delta=b['value'] - a['value'])
    return result
