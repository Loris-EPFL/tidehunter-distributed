#!/usr/bin/env python3
"""Collect independent target block and CPU intervals; never infer per-request latency."""
import argparse
import json
import os
from pathlib import Path
import stat
import re
import shutil
import subprocess
import time

BLOCK_FIELDS = ('reads_completed', 'reads_merged', 'read_sectors', 'read_ms',
                'writes_completed', 'writes_merged', 'write_sectors', 'write_ms',
                'in_flight', 'io_ms', 'weighted_io_ms', 'discards_completed',
                'discards_merged', 'discard_sectors', 'discard_ms', 'flushes_completed', 'flush_ms')
CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal', 'guest', 'guest_nice')


def block_snapshot(name, root=Path('/sys/class/block')):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or name in ('.', '..'):
        raise ValueError('invalid sysfs block device name')
    start = time.monotonic_ns()
    values = [int(x) for x in (root / name / 'stat').read_text().split()]
    end = time.monotonic_ns()
    if len(values) < 11 or any(x < 0 for x in values):
        raise ValueError('invalid block stat')
    return {'start_ns': start, 'end_ns': end, 'values': dict(zip(BLOCK_FIELDS, values))}


def cpu_snapshot(path=Path('/proc/stat')):
    start = time.monotonic_ns()
    line = path.read_text().splitlines()[0].split()
    end = time.monotonic_ns()
    if not line or line[0] != 'cpu':
        raise ValueError('invalid aggregate CPU stat')
    return {'start_ns': start, 'end_ns': end, 'values': dict(zip(CPU_FIELDS, map(int, line[1:])))}


def delta(first, last):
    result = {}
    for key, value in first['values'].items():
        if key == 'in_flight' or key not in last['values']:
            continue
        end = last['values'][key]
        if end < value:
            raise ValueError('counter reset: ' + key)
        result[key] = end - value
    return {'start_monotonic_ns': first['start_ns'], 'end_monotonic_ns': last['end_ns'],
            'first_read_end_ns': first['end_ns'], 'last_read_start_ns': last['start_ns'], 'delta': result}

def _attribute(path, numeric=False):
    """Bound sysfs reads and represent missing attributes explicitly."""
    try:
        with path.open('rb') as stream:
            data = stream.read(129)
        if len(data) > 128:
            raise ValueError('attribute exceeds 128 bytes')
        value = data.decode('ascii').strip()
        if not value:
            raise ValueError('empty attribute')
        if numeric:
            if not value.isdecimal() or not value.isascii():
                raise ValueError('not a nonnegative integer')
            value = int(value)
        return {'available': True, 'value': value}
    except (OSError, UnicodeError, ValueError) as exc:
        return {'available': False, 'reason': (type(exc).__name__ + ': ' + str(exc))[:300]}


def controller_snapshot(name, sys_root=Path('/sys')):
    """Read one explicitly named host controller without opening NVMe devices."""
    if not re.fullmatch(r'nvme[0-9]+', name):
        raise ValueError('invalid NVMe controller name')
    ctrl = Path(sys_root) / 'class/nvme' / name
    try:
        resolved = ctrl.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        return {'available': False, 'reason': str(exc)[:300]}
    return {'available': True, 'identity': {'name': name, 'sysfs_path': str(resolved),
            'serial': _attribute(ctrl / 'serial'), 'model': _attribute(ctrl / 'model'),
            'subsysnqn': _attribute(ctrl / 'subsysnqn')},
            'queue_count': _attribute(ctrl / 'queue_count', numeric=True),
            'sqsize': _attribute(ctrl / 'sqsize', numeric=True),
            'qualification': 'kernel controller attributes, not observed in-flight requests; queue_count includes admin queue; sqsize is zero-based',
            'source': 'https://github.com/torvalds/linux/blob/master/drivers/nvme/host/sysfs.c'}


def file_residency(image, identity):
    """Optional stock fincore boundary residency, never a per-I/O cache hit."""
    tool = shutil.which('fincore')
    if not tool:
        return {'available': False, 'reason': 'stock fincore not installed', 'effective_uid': os.geteuid()}
    try:
        started = time.monotonic_ns()
        result = subprocess.run([tool, '--json', '--bytes', '--output', 'RES,SIZE,FILE', '--', str(image)],
                                capture_output=True, timeout=2, check=False)
        if len(result.stdout) > 16384 or len(result.stderr) > 16384:
            raise ValueError('fincore output exceeds limit')
        if result.returncode != 0:
            raise ValueError('fincore failed (exit %d): %s' %
                             (result.returncode, result.stderr.decode('utf-8', errors='replace')[:200]))
        data = json.loads(result.stdout)
        rows = data.get('fincore') if isinstance(data, dict) else None
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict) or rows[0].get('file') != str(image):
            raise ValueError('unexpected fincore JSON identity')
        row = rows[0]
        def number(value):
            if type(value) is int and value >= 0:
                return value
            if isinstance(value, str) and value.isascii() and value.isdecimal():
                return int(value)
            raise ValueError('fincore size is not a byte count')
        resident, size = number(row.get('res')), number(row.get('size'))
        current = os.stat(image, follow_symlinks=False)
        if (current.st_dev, current.st_ino, current.st_size) != (
                identity['st_dev'], identity['inode'], identity['size_bytes']) or size != current.st_size:
            raise ValueError('file identity or size changed during fincore')
        return {'available': True, 'resident_bytes': resident, 'size_bytes': size,
                'effective_uid': os.geteuid(), 'start_monotonic_ns': started, 'end_monotonic_ns': time.monotonic_ns(),
                'scope': 'file page-cache residency at boundary, not per-I/O hits or misses; subject to kernel permission semantics',
                'source': 'https://man7.org/linux/man-pages/man1/fincore.1.html'}
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        return {'available': False, 'effective_uid': os.geteuid(),
                'reason': (type(exc).__name__ + ': ' + str(exc))[:300]}


def case_snapshot(image, device, sys_root=Path('/sys'), *, residency=False,
                  boot_id_path=Path('/proc/sys/kernel/random/boot_id')):
    """Read-only case boundary; clocks and counters are local to this host."""
    start = time.monotonic_ns()
    result = {'version': 1, 'start_monotonic_ns': start,
              'boot_id': _attribute(Path(boot_id_path)),
              'scope': 'host device-wide; not attributable to one file or request',
              'image': {'available': False}, 'block': {'available': False},
              'nvme_controller': {'available': False},
              'target_cache': {'available': False, 'reason':
                  'no inode-attributed cache hit/miss trace is collected; block reads are not cache misses'}}
    try:
        info = os.stat(image, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('image is not a regular file')
        result['image'] = {'available': True, 'path': str(image), 'st_dev': info.st_dev,
                           'inode': info.st_ino, 'size_bytes': info.st_size}
    except (OSError, ValueError) as exc:
        result['image']['reason'] = str(exc)[:300]
    try:
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', device) or device in ('.', '..'):
            raise ValueError('invalid block device')
        block = Path(sys_root) / 'class/block' / device
        resolved = block.resolve(strict=True)
        identity = {'name': device, 'sysfs_path': str(resolved),
                    'dev': _attribute(block / 'dev'),
                    'diskseq': _attribute(block / 'diskseq', numeric=True)}
        if identity['diskseq'].get('available') and identity['diskseq']['value'] == 0:
            identity['diskseq'] = {'available': False, 'reason': 'diskseq must be positive'}
        if result['image']['available']:
            try:
                mount = (Path(sys_root) / 'dev/block' /
                         f'{os.major(info.st_dev)}:{os.minor(info.st_dev)}').resolve(strict=True)
                disk = mount.parent if (mount / 'partition').exists() else mount
                slaves = disk / 'slaves'
                if disk != resolved or (slaves.exists() and any(slaves.iterdir())):
                    raise ValueError('image backing device is different or stacked')
                result['image']['backing_device_verified'] = True
            except (OSError, ValueError, RuntimeError) as exc:
                result['image']['backing_device_verified'] = False
                result['image']['backing_reason'] = str(exc)[:300]
        snapshot = block_snapshot(device, Path(sys_root) / 'class/block')
        diskseq_after = _attribute(block / 'diskseq', numeric=True)
        if identity['diskseq'] != diskseq_after:
            identity['diskseq'] = {'available': False, 'reason': 'diskseq unavailable or changed during snapshot'}
        result['block'] = {'available': identity['dev']['available'], 'identity': identity,
                           'snapshot': snapshot, 'units': {
                               name: ('512-byte sectors' if name.endswith('_sectors') else
                                      'milliseconds' if name.endswith('_ms') else
                                      'requests (gauge)' if name == 'in_flight' else 'requests')
                               for name in snapshot['values']},
                           'source': 'https://docs.kernel.org/admin-guide/iostats.html'}
        # Controller sysfs queue_count and sqsize are kernel-reported values,
        # not configured userspace parallelism. An NVMe namespace path may not
        # resolve to a controller (multipath, partitions or non-NVMe devices).
        candidates = [p for p in (resolved, *resolved.parents)
                      if re.fullmatch(r'nvme[0-9]+', p.name)]
        if len(candidates) != 1:
            raise ValueError('unique NVMe controller unavailable for block path')
        ctrl = candidates[0]
        result['nvme_controller'] = {'available': True, 'identity': {
            'name': ctrl.name, 'sysfs_path': str(ctrl),
            'serial': _attribute(ctrl / 'serial'), 'model': _attribute(ctrl / 'model'),
            'subsysnqn': _attribute(ctrl / 'subsysnqn')},
            'queue_count': _attribute(ctrl / 'queue_count', numeric=True),
            'sqsize': _attribute(ctrl / 'sqsize', numeric=True),
            'qualification': 'raw controller attributes; queue_count includes the admin queue; sqsize is zero-based submission queue size, not a measured in-flight count'}
    except (OSError, ValueError, RuntimeError) as exc:
        reason = (type(exc).__name__ + ': ' + str(exc))[:300]
        if not result['block']['available']:
            result['block']['reason'] = reason
        result['nvme_controller']['reason'] = reason
    if residency and result['image'].get('backing_device_verified'):
        result['cache_residency'] = file_residency(image, result['image'])
    else:
        result['cache_residency'] = {'available': False, 'reason':
            'not sampled or image backing identity unavailable'}
    result['end_monotonic_ns'] = time.monotonic_ns()
    return result


def case_delta(first, last):
    """Compare boundary snapshots only when file and block identities match."""
    result = {'available': False, 'block': {}, 'target_cache': last['target_cache']}
    if (not first.get('boot_id', {}).get('available')
            or not last.get('boot_id', {}).get('available')
            or not first['boot_id'].get('value')
            or first['boot_id'] != last['boot_id']):
        result['reason'] = 'host boot identity unavailable or changed'
        return result
    for key in ('image', 'block'):
        if not first[key].get('available') or not last[key].get('available'):
            result['reason'] = key + ' unavailable at boundary'
            return result
    if not first['image'].get('backing_device_verified') or not last['image'].get('backing_device_verified'):
        result['reason'] = 'image backing device not verified at both boundaries'
        return result
    a, b = first['image'], last['image']
    if (a['st_dev'], a['inode']) != (b['st_dev'], b['inode']):
        result['reason'] = 'image identity changed'
        return result
    old_diskseq = first['block']['identity'].get('diskseq', {})
    new_diskseq = last['block']['identity'].get('diskseq', {})
    if (not old_diskseq.get('available') or not new_diskseq.get('available')
            or type(old_diskseq.get('value')) is not int or old_diskseq['value'] <= 0
            or old_diskseq != new_diskseq):
        result['reason'] = 'block diskseq unavailable or changed'
        return result
    if first['block']['identity'] != last['block']['identity']:
        result['reason'] = 'block identity changed'
        return result
    old, new = first['block']['snapshot']['values'], last['block']['snapshot']['values']
    for key in sorted(old.keys() | new.keys()):
        if key == 'in_flight':
            result['block'][key] = {'available': key in old and key in new,
                                    'begin': old.get(key), 'end': new.get(key), 'kind': 'gauge'}
        elif key not in old or key not in new:
            result['block'][key] = {'available': False, 'reason': 'missing at boundary'}
        elif new[key] < old[key]:
            result['block'][key] = {'available': False, 'reason': 'counter reset or wrap'}
        else:
            result['block'][key] = {'available': True, 'delta': new[key] - old[key]}
    result['available'] = True
    result['qualification'] = 'device-wide block counters; 512-byte sectors; no cache hit/miss attribution'
    return result


def case_capture(case_id, boundary, images, controllers, rdma_module, *,
                 sys_root=Path('/sys'), boot_id_path=Path('/proc/sys/kernel/random/boot_id')):
    """Capture a bounded host-local case boundary, including preload boundaries.

    images is a list of {'image': absolute path, 'block_device': sysfs name};
    controllers is a list of explicit initiator controller names. The caller
    owns persistence and must capture end in its failure/finally path.
    """
    if not isinstance(case_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case_id):
        raise ValueError('invalid case ID')
    if boundary not in ('begin', 'end'):
        raise ValueError('boundary must be begin or end')
    if not isinstance(images, list) or len(images) > 256:
        raise ValueError('images must be a list of at most 256 entries')
    if not isinstance(controllers, list) or len(controllers) > 256:
        raise ValueError('controllers must be a list of at most 256 names')
    keys = []
    for entry in images:
        if (not isinstance(entry, dict) or set(entry) != {'image', 'block_device'}
                or not isinstance(entry['image'], str) or len(entry['image']) > 4096
                or not Path(entry['image']).is_absolute()
                or not isinstance(entry['block_device'], str)):
            raise ValueError('invalid image binding')
        keys.append((entry['image'], entry['block_device']))
    if any(not isinstance(name, str) or not re.fullmatch(r'nvme[0-9]+', name) for name in controllers):
        raise ValueError('invalid controller name')
    if len(set(keys)) != len(keys) or len(set(controllers)) != len(controllers):
        raise ValueError('duplicate boundary binding')
    started = time.monotonic_ns()
    result = {'version': 1, 'case_id': case_id, 'boundary': boundary,
              'start_monotonic_ns': started, 'boot_id': _attribute(Path(boot_id_path)),
              'images': [], 'controllers': [], 'rdma': None}
    for number, (image, device) in enumerate(keys):
        result['images'].append({'image': image, 'block_device': device,
                                 'snapshot': case_snapshot(image, device, sys_root, residency=number < 8,
                                                           boot_id_path=boot_id_path)})
    for name in controllers:
        result['controllers'].append({'name': name, 'snapshot': controller_snapshot(name, sys_root)})
    try:
        result['rdma'] = rdma_module.capture(sys_root)
    except (OSError, ValueError, RuntimeError) as exc:
        result['rdma'] = {'available': False, 'devices': [], 'errors': [str(exc)[:300]]}
    result['rdma_resources'] = rdma_module.capture_resources()
    result['end_monotonic_ns'] = time.monotonic_ns()
    return result


def case_compare(begin, end, rdma_module):
    """Compare the exact same host and case, retaining all boundary evidence."""
    result = {'version': 1, 'available': False, 'case_id': begin.get('case_id'),
              'images': [], 'controllers': [], 'rdma': None}
    if (begin.get('boundary') != 'begin' or end.get('boundary') != 'end'
            or begin.get('case_id') != end.get('case_id')):
        result['reason'] = 'case or boundary mismatch'
        return result
    if (not begin.get('boot_id', {}).get('available')
            or not end.get('boot_id', {}).get('available')
            or not begin['boot_id'].get('value')
            or begin.get('boot_id') != end.get('boot_id')):
        result['reason'] = 'host boot identity unavailable or changed'
        return result
    if begin.get('end_monotonic_ns', 0) > end.get('start_monotonic_ns', -1):
        result['reason'] = 'boundary clocks overlap or reversed'
        return result
    def image_map(frame):
        return {(item['image'], item['block_device']): item['snapshot'] for item in frame['images']}
    old, new = image_map(begin), image_map(end)
    for key in sorted(old.keys() | new.keys()):
        row = {'image': key[0], 'block_device': key[1]}
        if key in old and key in new:
            if old[key].get('boot_id') != begin['boot_id'] or new[key].get('boot_id') != end['boot_id']:
                row['comparison'] = {'available': False, 'reason': 'snapshot boot identity differs from case boundary'}
            else:
                row['comparison'] = case_delta(old[key], new[key])
        else:
            row['comparison'] = {'available': False, 'reason': 'image binding missing at boundary'}
        result['images'].append(row)
    old_ctrl = {item['name']: item['snapshot'] for item in begin['controllers']}
    new_ctrl = {item['name']: item['snapshot'] for item in end['controllers']}
    for name in sorted(old_ctrl.keys() | new_ctrl.keys()):
        a, b = old_ctrl.get(name, {}), new_ctrl.get(name, {})
        stable = (a.get('available') and b.get('available') and a.get('identity') == b.get('identity')
                  and a.get('identity', {}).get('serial', {}).get('available'))
        result['controllers'].append({'name': name, 'available': bool(stable),
            'reason': None if stable else 'controller identity unavailable or changed',
            'begin': a, 'end': b})
    result['rdma'] = rdma_module.compare(begin['rdma'], end['rdma'])
    result['available'] = True
    result['scope'] = 'same-host whole case including preload; snapshots are non-atomic'
    return result


def collect(device, seconds):
    if not 0 < seconds <= 3600:
        raise ValueError('interval must be in (0, 3600] seconds')
    # These are deliberately separate clocks and non-atomic snapshots.
    b0 = block_snapshot(device); c0 = cpu_snapshot()
    time.sleep(seconds)
    b1 = block_snapshot(device); c1 = cpu_snapshot()
    block = delta(b0, b1)
    if 'read_sectors' in block['delta']:
        block['delta']['read_bytes'] = block['delta']['read_sectors'] * 512
    if 'write_sectors' in block['delta']:
        block['delta']['write_bytes'] = block['delta']['write_sectors'] * 512
    return {'version': 1, 'block_device': device, 'block': block, 'cpu': delta(c0, c1),
            'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
            'qualification': 'sysfs device-level aggregate; 512-byte accounting sectors; not per-request NAND latency or network time',
            'target_cache': {'warm_hits': None, 'storage_misses': None,
                             'denominator': None,
                             'reason': 'device-wide counters cannot attribute page-cache hits or misses to exports'},
            'trace': {'available': False, 'reason': 'per-request nvmet/file/block trace correlation is not collected by this interval sampler'}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--block-device', help='explicit /sys/class/block name on target')
    parser.add_argument('--seconds', type=float)
    parser.add_argument('--output', required=True, help='new JSON output path')
    parser.add_argument('--case-image', help='capture one case boundary for this target image')
    parser.add_argument('--compare', nargs=2, metavar=('BEGIN', 'END'),
                        help='compare two case boundary JSON files')
    args = parser.parse_args()
    if args.compare:
        if args.block_device or args.seconds is not None or args.case_image:
            parser.error('--compare cannot be combined with capture options')
        with open(args.compare[0]) as first, open(args.compare[1]) as last:
            result = case_delta(json.load(first), json.load(last))
    elif args.case_image:
        if not args.block_device or args.seconds is not None:
            parser.error('--case-image requires --block-device and excludes --seconds')
        result = case_snapshot(args.case_image, args.block_device)
    else:
        if not args.block_device or args.seconds is None:
            parser.error('--block-device and --seconds are required')
        result = collect(args.block_device, args.seconds)
    with open(args.output, 'x') as f:
        json.dump(result, f, sort_keys=True, indent=2); f.write('\n')


if __name__ == '__main__':
    main()
