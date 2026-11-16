"""Shared transport identities and fail-closed EFA hardware activity evidence."""
from pathlib import Path
import re
import time




# Application transport names are not necessarily libfabric fabric names:
# efa-native uses the direct adapter without the software TLS stream.
HARDWARE_TRANSPORTS = ('efa', 'efa-direct', 'efa-native')
DIRECT_TRANSPORTS = ('efa-direct', 'efa-native')
TRANSPORTS = ('tcp', *HARDWARE_TRANSPORTS)
TRANSPORT_BACKENDS = {
    'tcp': 'experimental_tcp_authority',
    'efa': 'experimental_efa_authority',
    'efa-direct': 'experimental_efa_direct_authority',
    'efa-native': 'trusted_experimental_efa_native_authority',
    'fabric-tcp': 'experimental_fabric_tcp_authority',
    'fabric-tcp-native': 'experimental_fabric_tcp_native_authority',
}


def fabric_name(transport):
    if transport not in HARDWARE_TRANSPORTS:
        raise ValueError('hardware fabric requires an EFA transport')
    return 'efa-direct' if transport in DIRECT_TRANSPORTS else 'efa'


def fabric_preflight(output, transport='efa'):
    import os
    import resource
    import shutil
    import subprocess
    if transport not in HARDWARE_TRANSPORTS:
        raise ValueError('hardware fabric preflight requires an EFA transport')
    resource.setrlimit(resource.RLIMIT_MEMLOCK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
    os.environ.update(FI_EFA_USE_DEVICE_RDMA='1', FI_EFA_ENABLE_SHM_TRANSFER='0')
    executable = shutil.which('fi_info') or '/opt/amazon/efa/bin/fi_info'
    result = subprocess.run([executable, '-p', 'efa', '-f', fabric_name(transport), '-t', 'FI_EP_RDM'],
                            capture_output=True, timeout=30)
    (output / 'efa-provider.txt').write_bytes(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f'{transport} requires an available hardware EFA RDM fabric; no transport fallback')
    validate_provider(result.stdout.decode(), transport)







def validate_provider(text, transport=None):
    # Omitted transport admits retained pre-fabric-name evidence only. New
    # executions always name their transport and require exact negotiation.
    if transport is not None and transport not in HARDWARE_TRANSPORTS:
        raise ValueError('hardware provider proof requires an EFA transport')
    providers = re.findall(r'^\s*(?:provider|prov_name):\s*(\S+)\s*$', text, re.MULTILINE)
    endpoints = re.findall(r'^\s*type:\s*(FI_EP_\S+)\s*$', text, re.MULTILINE)
    fabrics = re.findall(r'^\s*fabric:\s*(\S+)\s*$', text, re.MULTILINE)
    fabrics += re.findall(r'fi_fabric_attr:\s*\n\s*name:\s*(\S+)', text)
    if (not providers or any(provider != 'efa' for provider in providers)
            or not endpoints or any(endpoint != 'FI_EP_RDM' for endpoint in endpoints)):
        raise ValueError('requires hardware efa RDM provider, not software/layered fallback')
    expected_fabric = fabric_name(transport) if transport is not None else 'efa'
    if ((transport is not None and (len(fabrics) != len(providers) or len(endpoints) != len(providers)))
            or any(fabric != expected_fabric for fabric in fabrics)):
        raise ValueError('negotiated fabric differs from requested transport; no fallback')


def efa_snapshot(identity, base=Path('/sys/class/infiniband')):
    devices = {}
    for device in sorted(base.iterdir()):
        driver = device / 'device/driver'
        if not driver.is_symlink() or driver.resolve().name != 'efa':
            continue
        counters = {}
        for directory in sorted(device.glob('ports/*/hw_counters')):
            for name in ('send_bytes', 'recv_bytes'):
                path = directory / name
                value = int(path.read_text().strip())
                if value < 0:
                    raise ValueError('negative EFA hardware counter')
                counters[str(path.relative_to(device))] = value
        if not counters:
            raise ValueError('EFA device has no readable send/receive byte counters')
        devices[device.name] = dict(driver='efa', counters=counters)
    if not devices:
        raise ValueError('no hardware EFA device with readable data counters')
    return dict(identity=identity, unix_ns=time.time_ns(), devices=devices)


def efa_proof(before, after, identity):
    if before.get('identity') != identity or after.get('identity') != identity:
        raise ValueError('EFA snapshot cell/host/source identity differs')
    if not 0 < before['unix_ns'] < after['unix_ns']:
        raise ValueError('EFA snapshot interval is not ordered')
    if not before.get('devices') or before['devices'].keys() != after.get('devices', {}).keys():
        raise ValueError('EFA hardware inventory missing or changed')
    deltas, tx, rx = {}, 0, 0
    for name, device in before['devices'].items():
        end = after['devices'][name]
        if device.get('driver') != 'efa' or end.get('driver') != 'efa':
            raise ValueError('software-only EFA evidence')
        counters = device['counters']
        if not counters or counters.keys() != end['counters'].keys():
            raise ValueError('EFA counter coverage changed')
        delta = {}
        for key, value in counters.items():
            final = end['counters'][key]
            if type(value) is not int or type(final) is not int or value < 0 or final < value:
                raise ValueError('EFA counters unreadable or reset')
            delta[key] = final - value
            if Path(key).name == 'send_bytes':
                tx += delta[key]
            elif Path(key).name == 'recv_bytes':
                rx += delta[key]
        deltas[name] = delta
    if tx <= 0 or rx <= 0:
        raise ValueError('EFA requires positive hardware send AND receive data bytes on every host')
    return dict(identity=identity, before=before, after=after, deltas=deltas,
                send_bytes=tx, recv_bytes=rx,
                scope='host-wide EFA hardware activity during this cell; not pure-wire timing or process attribution')


