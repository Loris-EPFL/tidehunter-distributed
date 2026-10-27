"""Pure orchestration boundary for canonical engine transport configuration.

Research case names are labels, never serialized runtime backend/transport aliases.
NIC selectors are local to a host and independent of TCP control endpoints.
"""
import copy
import ipaddress
import json
from pathlib import Path

CASE_TRANSPORTS = {
    'tidehunter_tcp': ('tcp', None),
    'tidehunter_efa': ('efa', 'mutual-tls'),
    'tidehunter_efa_direct': ('efa-direct', 'mutual-tls'),
    'tidehunter_efa_native': ('efa-direct', 'trusted'),
    'tidehunter_efa_trusted': ('efa', 'trusted'),
    'tidehunter_verbs': ('verbs', 'mutual-tls'),
    'tidehunter_verbs_native': ('verbs', 'trusted'),
    'tidehunter_software_tcp': ('software-tcp', 'mutual-tls'),
    'tidehunter_software_tcp_native': ('software-tcp', 'trusted'),
}
# These are existing driver/research selections, not Rust config aliases.
DRIVER_CASES = {
    'tcp': 'tidehunter_tcp', 'efa': 'tidehunter_efa',
    'efa-direct': 'tidehunter_efa_direct', 'efa-native': 'tidehunter_efa_native',
    'fabric-tcp': 'tidehunter_software_tcp',
    'efa-trusted': 'tidehunter_efa_trusted',
    'fabric-tcp-native': 'tidehunter_software_tcp_native',
}


def select_backends(raw, configured, supported):
    if (not isinstance(configured, list) or not configured
            or any(not isinstance(item, str) or not item for item in configured)
            or len(configured) != len(set(configured))):
        raise ValueError('tracked backend selection must be a nonempty unique list')
    if (not isinstance(supported, (list, tuple, set, frozenset))
            or any(not isinstance(item, str) or not item for item in supported)):
        raise ValueError('supported backend identifiers must be a string collection')
    supported = set(supported)
    if set(configured) - supported:
        raise ValueError('tracked backend selection contains unsupported identifiers')
    if raw is None:
        selected = list(configured)
    else:
        if not isinstance(raw, str):
            raise ValueError('backend selection must be comma-separated text')
        selected = [item.strip() for item in raw.split(',')]
        if not selected or any(not item for item in selected):
            raise ValueError('explicit backend selection must not be empty')
        if len(selected) != len(set(selected)):
            raise ValueError('backend selection contains duplicates')
        if set(selected) - set(supported):
            raise ValueError('backend selection contains unsupported identifiers')
        if set(selected) - set(configured):
            raise ValueError('backend selection is not admitted by tracked config')
    return selected


def validate_transport(value):
    if not isinstance(value, dict):
        raise ValueError('transport must be a canonical object')
    if value.get('kind') == 'tcp':
        if set(value) != {'kind'}:
            raise ValueError('TCP transport accepts only kind')
        return {'kind': 'tcp'}
    if value.get('kind') != 'ofi' or set(value) - {'kind', 'fabric', 'security'}:
        raise ValueError('transport requires kind tcp or ofi and known fields')
    fabric = value.get('fabric')
    if not isinstance(fabric, dict) or set(fabric) - {'backend', 'domain', 'source_address'}:
        raise ValueError('OFI fabric requires explicit backend and known selectors')
    if fabric.get('backend') not in ('efa', 'efa-direct', 'verbs', 'software-tcp'):
        raise ValueError('unsupported canonical fabric backend')
    domain = fabric.get('domain')
    if domain is not None and (not isinstance(domain, str) or not domain or '\0' in domain or len(domain.encode()) > 255):
        raise ValueError('domain must be a nonempty bounded provider selector')
    source = fabric.get('source_address')
    address = None
    if source is not None:
        if not isinstance(source, str) or '%' in source:
            raise ValueError('source_address must be an unscoped IP address')
        address = ipaddress.ip_address(source)
        if address.is_unspecified or address.is_multicast:
            raise ValueError('source_address must identify a unicast local NIC')
        if fabric['backend'] in ('efa', 'efa-direct'):
            raise ValueError('EFA providers cannot attest a selected source_address')
        if fabric['backend'] == 'verbs' and address.version != 4:
            raise ValueError('verbs source_address must be IPv4 in the pinned RxM profile')
    security = value.get('security', 'mutual-tls')
    if security not in ('mutual-tls', 'trusted'):
        raise ValueError('OFI security must be mutual-tls or trusted')
    canonical_fabric = {'backend': fabric['backend']}
    if domain is not None:
        canonical_fabric['domain'] = domain
    if address is not None:
        canonical_fabric['source_address'] = str(address)
    return {'kind': 'ofi', 'fabric': canonical_fabric, 'security': security}


def transport_for_case(label):
    if isinstance(label, dict):
        return validate_transport(label)
    try:
        backend, security = CASE_TRANSPORTS[DRIVER_CASES.get(label, label)]
    except (KeyError, TypeError):
        raise ValueError('unknown transport research case') from None
    if backend == 'tcp':
        return {'kind': 'tcp'}
    return {'kind': 'ofi', 'fabric': {'backend': backend}, 'security': security}


def runtime_backend(case_label):
    return 'tidehunter' if case_label in CASE_TRANSPORTS else case_label


def protocol_key(value):
    transport = validate_transport(value)
    return ('tcp',) if transport['kind'] == 'tcp' else (
        'ofi', transport['fabric']['backend'], transport['security'])


def is_trusted(value):
    return validate_transport(value).get('security') == 'trusted'


def is_software(value):
    return validate_transport(value).get('fabric', {}).get('backend') == 'software-tcp'


def is_fabric(value):
    return validate_transport(value)['kind'] == 'ofi'


def select_transports(client, owner_transports, count):
    client = validate_transport(client)
    if type(count) is not int or count < 1:
        raise ValueError('positive owner count required')
    client_source = client.get('fabric', {}).get('source_address')
    if owner_transports is None:
        if client_source is not None:
            raise ValueError('client NIC source_address requires explicit owner_transports')
        return [copy.deepcopy(client) for _ in range(count)]
    if not isinstance(owner_transports, list) or len(owner_transports) != count:
        raise ValueError('owner_transports must cover every owner exactly once')
    owners = [validate_transport(value) for value in owner_transports]
    if any(protocol_key(value) != protocol_key(client) for value in owners):
        raise ValueError('owner transports must match client provider and security')
    return owners


def write_transport_config(path, transport):
    path = Path(path)
    path.write_text(json.dumps(validate_transport(transport), indent=2) + '\n')
    return str(path)
