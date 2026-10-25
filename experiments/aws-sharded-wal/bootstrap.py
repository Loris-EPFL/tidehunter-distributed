#!/usr/bin/env python3
"""Fresh guest only: arm absolute shutdown before mounting one blank instance SSD."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request


def identity():
    base = 'http://169.254.169.254/latest'
    request = urllib.request.Request(base + '/api/token', method='PUT',
                                    headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
    with urllib.request.urlopen(request, timeout=3) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request(base + '/dynamic/instance-identity/document',
                                    headers={'X-aws-ec2-metadata-token': token})
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


INSTANCE_STORE_DISKS = {'i4i.xlarge': 1, 'c6id.32xlarge': 4, 'g5.8xlarge': 1, 'g6.8xlarge': 2, 'g6.16xlarge': 2, 'g6e.8xlarge': 2}


def blank_instance_disk(devices, expected_count=1):
    candidates = [d for d in devices if (d.get('model') or '').strip() == 'Amazon EC2 NVMe Instance Storage']
    if expected_count not in [1, 2, 4] or len(candidates) != expected_count:
        raise ValueError('instance-store device count differs from exact hardware plan')
    if len({d['path'] for d in candidates}) != expected_count:
        raise ValueError('duplicate instance-store device')
    for d in candidates:
        if (not re.fullmatch(r'/dev/nvme[0-9]+n1', d['path']) or d['type'] != 'disk'
                or d.get('fstype') or d.get('children') or any(d.get('mountpoints') or [])):
            raise ValueError('refusing nonblank, mounted or partitioned NVMe')
    # Use exactly one drive per machine; do not RAID or format the other drives.
    return sorted(d['path'] for d in candidates)[0]


def bootstrap(args):
    if os.geteuid() != 0 or not re.fullmatch(r'th-[a-f0-9]{16}', args.run_id):
        raise ValueError('root and an exact fresh run ID required')
    if type(args.ttl_seconds) is not int or not 600 <= args.ttl_seconds <= 10800:
        raise ValueError('guest lifetime must be an integer in 600..10800 seconds')
    info = identity()
    if info['accountId'] != args.account or info['instanceType'] != args.instance_type:
        raise ValueError('unexpected account or instance hardware')
    deadline = datetime.fromisoformat(info['pendingTime'].replace('Z', '+00:00')) + timedelta(seconds=args.ttl_seconds)
    if getattr(args, 'deadline_utc', None) is not None:
        absolute = datetime.fromisoformat(args.deadline_utc.replace('Z', '+00:00'))
        if absolute.tzinfo is None or absolute.utcoffset() != timedelta(0) or absolute > deadline:
            raise ValueError('absolute deadline must be UTC and cannot extend the launch lifetime')
        deadline = absolute
    if deadline <= datetime.now(timezone.utc):
        subprocess.run(['/usr/sbin/poweroff'], check=True)
        raise ValueError('launch deadline already passed')
    name = 'sharded-wal-expire-' + args.run_id
    units = Path('/etc/systemd/system')
    service = units / (name + '.service')
    timer = units / (name + '.timer')
    # Creation-only prevents a repeated bootstrap from extending an existing TTL.
    with service.open('x') as stream:
        stream.write('[Unit]\nDescription=Terminate scoped sharded-WAL benchmark guest\n[Service]\nType=oneshot\nExecStart=/usr/sbin/poweroff\n')
    with timer.open('x') as stream:
        stream.write(f'[Unit]\nDescription=Absolute sharded-WAL expiry\n[Timer]\nOnCalendar={deadline.strftime("%Y-%m-%d %H:%M:%S UTC")}\nPersistent=true\nAccuracySec=1s\nUnit={name}.service\n[Install]\nWantedBy=timers.target\n')
    subprocess.run(['systemctl', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', 'enable', '--now', name + '.timer'], check=True)
    subprocess.run(['systemctl', 'is-active', '--quiet', name + '.timer'], check=True)
    devices = json.loads(subprocess.check_output(['lsblk', '--json', '--output', 'PATH,MODEL,TYPE,FSTYPE,MOUNTPOINTS']))
    disk = blank_instance_disk(devices['blockdevices'], INSTANCE_STORE_DISKS[args.instance_type])
    signatures = json.loads(subprocess.check_output(['wipefs', '--no-act', '--json', disk]))
    if signatures.get('signatures'):
        raise ValueError('refusing existing filesystem signatures')
    mount = Path('/mnt/tidehunter')
    mount.mkdir()
    subprocess.run(['mkfs.ext4', '-q', disk], check=True, timeout=120)
    subprocess.run(['mount', '-o', 'noatime', disk, str(mount)], check=True, timeout=30)
    root = mount / args.run_id
    root.mkdir(mode=0o700)
    (root / 'bootstrap.json').write_text(json.dumps({
        'run_id': args.run_id, 'identity': info, 'device': disk,
        'deadline_utc': deadline.isoformat(), 'shutdown_behavior_required': 'terminate',
    }, indent=2) + '\n')
    print(json.dumps({'run_root': str(root), 'instance_id': info['instanceId'], 'deadline_utc': deadline.isoformat()}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--instance-type', choices=['i4i.xlarge', 'c6id.32xlarge', 'g5.8xlarge', 'g6.8xlarge', 'g6.16xlarge', 'g6e.8xlarge'], default='i4i.xlarge')
    parser.add_argument('--account', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--ttl-seconds', type=int, required=True)
    parser.add_argument('--deadline-utc', help='Optional earlier immutable UTC termination deadline')
    bootstrap(parser.parse_args())


if __name__ == '__main__':
    main()
