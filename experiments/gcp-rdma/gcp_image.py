"""Review-bound admission of an exact image and two immutable hardware proofs.

The pinned receipt and evidence references are a human review boundary, not a
cryptographic attestation: a branch editor can approve fabricated evidence.
"""
import ast
import hashlib
import json
import math
from pathlib import Path
import re

_HEX = re.compile(r'^[0-9a-f]{64}$')
_IMAGE = re.compile(r'^projects/([a-z][a-z0-9-]{4,61}[a-z0-9])/global/images/([a-z][a-z0-9-]{0,61}[a-z0-9])$')
_FAMILIES = frozenset({'GLIBC', 'GLIBCXX', 'CXXABI', 'OPENSSL'})


def required_containers():
    root = Path(__file__).resolve().parent.parent / 'kv-comparison'
    tree = ast.parse((root / 'local.py').read_text())
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'IMAGES' for target in node.targets)]
    if len(assignments) != 1:
        raise ValueError('comparison image inventory is ambiguous')
    comparison = ast.literal_eval(assignments[0].value)
    monitoring = json.loads((root / 'observability' / 'images.lock.json').read_text())['images']
    return set(comparison.values()) | {entry['image'] for entry in monitoring.values()}


def recipe_hashes():
    root = Path(__file__).resolve().parent
    return {name + '_sha256': hashlib.sha256((root / filename).read_bytes()).hexdigest()
            for name, filename in (('prepare', 'gcp_image_prepare.py'), ('qualify', 'gcp_image_qualify.py'), ('scratch', 'gcp_worker.py'))}


def _digest(value, field):
    if not isinstance(value, str) or not _HEX.fullmatch(value):
        raise ValueError(f'invalid {field} SHA-256')
    return value


def _load(api, ref, bucket, limit):
    if not isinstance(ref, dict) or set(ref) != {'bucket', 'object', 'generation', 'sha256'}:
        raise ValueError('qualification object reference requires bucket, object, generation, sha256')
    if ref['bucket'] != bucket or not isinstance(ref['object'], str) or not ref['object'] or ref['object'].startswith('/'):
        raise ValueError('qualification object is outside the configured bucket')
    if not isinstance(ref['generation'], str) or not re.fullmatch(r'[1-9][0-9]*', ref['generation']):
        raise ValueError('qualification generation must be a positive decimal string')
    expected = _digest(ref['sha256'], 'object')
    data = api.objects(bucket).get(ref['object'], generation=ref['generation'], max_bytes=limit)
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError('qualification object digest mismatch')
    try:
        value = json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError('invalid qualification JSON') from exc
    if not isinstance(value, dict):
        raise ValueError('qualification JSON must be an object')
    return value


def canonical_image_digest(value):
    if not isinstance(value, str) or '@sha256:' not in value:
        raise ValueError('image reference must contain a digest')
    name, digest = value.rsplit('@sha256:', 1)
    _digest(digest, 'image')
    last_slash = name.rfind('/')
    colon = name.rfind(':')
    if colon > last_slash:
        name = name[:colon]
    parts = name.split('/')
    if not parts or any(not part for part in parts):
        raise ValueError('invalid image repository')
    if len(parts) == 1:
        parts = ['docker.io', 'library', parts[0]]
    elif '.' not in parts[0] and ':' not in parts[0] and parts[0] != 'localhost':
        parts.insert(0, 'docker.io')
    if parts[0] in ('index.docker.io', 'registry-1.docker.io'):
        parts[0] = 'docker.io'
    if parts[0] == 'docker.io' and len(parts) == 2:
        parts.insert(1, 'library')
    return '/'.join(parts), digest


def _versions(value):
    if not isinstance(value, dict) or set(value) != _FAMILIES:
        raise ValueError('four explicit ABI version families required')
    for family, versions in value.items():
        if not isinstance(versions, list) or any(not isinstance(v, str) or not re.fullmatch(r'\d+(?:\.\d+)+', v) for v in versions) or len(set(versions)) != len(versions):
            raise ValueError(f'invalid {family} ABI version list')
    return value


def _linkage(proof):
    if not isinstance(proof, dict) or not isinstance(proof.get('ldd'), str) or not proof['ldd'].strip() or 'not found' in proof['ldd']:
        raise ValueError('nonempty resolved ELF linkage evidence required')
    resolved = proof.get('resolved')
    if not isinstance(resolved, dict) or not resolved:
        raise ValueError('resolved ELF library identities required')
    for path, elf in resolved.items():
        if not isinstance(path, str) or not path.startswith('/') or not isinstance(elf, dict) or not isinstance(elf.get('identity'), dict):
            raise ValueError('invalid resolved ELF identity')
        _digest(elf['identity'].get('sha256'), 'ELF library')
        if elf.get('soname') is not None and (not isinstance(elf['soname'], str) or not elf['soname'] or '/' in elf['soname']):
            raise ValueError('invalid ELF SONAME')
        _versions(elf.get('version_definitions'))
    return resolved


def _abi(abi):
    if not isinstance(abi, dict) or set(abi) != {'needed', 'version_requirements'}:
        raise ValueError('ABI requires needed SONAMEs and version requirements')
    needed = abi['needed']
    if not isinstance(needed, list) or not needed or any(not isinstance(name, str) or not name or '/' in name for name in needed) or len(set(needed)) != len(needed):
        raise ValueError('invalid ABI SONAME requirements')
    return set(needed), _versions(abi['version_requirements'])


def _envelope(record):
    package = record['outputs']['package']
    _, baseline_versions = _abi(package.get('abi'))
    sonames = set()
    versions = {family: set(baseline_versions[family]) for family in _FAMILIES}
    proofs = [package['linkage'], *package['valkey_runtime_linkage'].values()]
    for proof in proofs:
        for elf in _linkage(proof).values():
            if elf.get('soname'):
                sonames.add(elf['soname'])
            for family, definitions in _versions(elf['version_definitions']).items():
                versions[family].update(definitions)
    if not sonames:
        raise ValueError('baseline has no resolved library SONAMEs')
    return sonames, versions


def admit_abi(qualification, abi):
    """Check SONAMEs and four version families, not full symbol ABI or hardware execution."""
    needed, required = _abi(abi)
    records = qualification.get('_records') if isinstance(qualification, dict) else None
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('validated physical qualification records required')
    envelopes = [_envelope(record) for record in records]
    if envelopes[0] != envelopes[1]:
        raise ValueError('qualified guest ABI envelopes differ')
    sonames, versions = envelopes[0]
    if needed - sonames:
        raise ValueError('required SONAMEs absent from qualified guest: ' + ', '.join(sorted(needed - sonames)))
    for family in _FAMILIES:
        missing = set(required[family]) - versions[family]
        if missing:
            raise ValueError(f'{family} versions absent from qualified guest: ' + ', '.join(sorted(missing)))


def validate_pair(records, image, runtime_sha, service_account):
    """Validate two reciprocal real-command records, not a pass/fail checklist."""
    _digest(runtime_sha, 'runtime manifest')
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('two guest diagnostics are required')
    for record in records:
        if not isinstance(record, dict) or record.get('kind') != 'guest-diagnostic' or record.get('image') != image:
            raise ValueError('guest evidence does not bind the exact image')
        if record.get('recipes') != recipe_hashes() or record.get('service_account') != service_account:
            raise ValueError('guest evidence recipe or workload identity mismatch')
        for key in ('instance_id', 'peer_instance_id'):
            if not isinstance(record.get(key), str) or not record[key].isdigit():
                raise ValueError('numeric instance identities required')
        if record['instance_id'] == record['peer_instance_id']:
            raise ValueError('physical peer must be distinct')
        if not isinstance(record.get('scratch_devices'), dict) or not record['scratch_devices']:
            raise ValueError('API-identified scratch disks required')
        device = record.get('rdma_device')
        if not isinstance(device, str) or not re.fullmatch(r'[A-Za-z0-9_]+', device):
            raise ValueError('physical RDMA device identity missing')
        outputs = record.get('outputs')
        if not isinstance(outputs, dict) or outputs.get('io_uring') != 'io_uring_setup(2) succeeded':
            raise ValueError('successful io_uring syscall evidence required')
        memlock = outputs.get('memlock')
        if not isinstance(memlock, dict) or memlock.get('soft') != 'infinity' or memlock.get('hard') != 'infinity' or memlock.get('docker') not in ('infinity', '18446744073709551615'):
            raise ValueError('unlimited workload and Docker locked memory evidence required')
        fio = outputs.get('io_uring_workload')
        jobs = fio.get('jobs') if isinstance(fio, dict) else None
        if not isinstance(jobs, list) or len(jobs) != 1 or jobs[0].get('error') != 0 or jobs[0].get('read', {}).get('io_bytes', 0) <= 0:
            raise ValueError('successful io_uring workload evidence required')
        ioctls = outputs.get('nvme_ioctl')
        if not isinstance(ioctls, dict) or set(ioctls) != set(record['scratch_devices']) or any(not isinstance(text, str) or not text for text in ioctls.values()):
            raise ValueError('scratch NVMe ioctl evidence required')
        package = outputs.get('package')
        if not isinstance(package, dict) or package.get('manifest_sha256') != runtime_sha or package.get('runtime_checked') is not True:
            raise ValueError('sealed runtime linkage evidence mismatch')
        resolved = _linkage(package.get('linkage'))
        valkey_linkage = package.get('valkey_runtime_linkage')
        if not isinstance(valkey_linkage, dict) or set(valkey_linkage) != {'valkey-server', 'valkey-cli'}:
            raise ValueError('both Valkey executable linkage records required')
        for linkage in valkey_linkage.values():
            _linkage(linkage)
        fabric_path = package.get('fabric_library')
        if not isinstance(fabric_path, str) or fabric_path not in resolved or not any(Path(path).name.startswith('libfdb_c.so') for path in resolved):
            raise ValueError('benchmark FDB and libfabric linkage required')
        fabric_sha = _digest(outputs.get('libfabric_sha256'), 'libfabric')
        if resolved[fabric_path]['identity']['sha256'] != fabric_sha:
            raise ValueError('linked libfabric digest mismatch')
        fi_ldd = outputs.get('fi_info_ldd')
        if not isinstance(fi_ldd, str) or 'libfabric.so' not in fi_ldd or 'not found' in fi_ldd or outputs.get('fi_info_fabric') != fabric_path:
            raise ValueError('fi_info must use the benchmark libfabric')
        if type(outputs.get('libfabric_api_version')) is not int or outputs['libfabric_api_version'] <= 0:
            raise ValueError('actual libfabric API version evidence required')
        installed = outputs.get('installed_rpms')
        if not isinstance(installed, list) or not all(isinstance(x, str) for x in installed) or not any(x.startswith('kmod-idpf-irdma-') for x in installed):
            raise ValueError('installed driver NEVRA evidence missing')
        if not isinstance(outputs.get('kernel'), str) or not outputs['kernel']:
            raise ValueError('kernel identity missing')
        if not re.search(r'\b2\.6\.0\b', str(outputs.get('libfabric_version', ''))):
            raise ValueError('libfabric 2.6.0 evidence missing')
        providers = outputs.get('providers', {})
        if not isinstance(providers, dict) or not all(isinstance(providers.get(p), str) and p + ';ofi_rxm' in providers[p] for p in ('verbs', 'tcp')):
            raise ValueError('verbs/RxM and TCP/RxM provider evidence required')
        if not re.search(r'\blink\s+' + re.escape(device) + r'/', str(outputs.get('rdma_links', ''))):
            raise ValueError('RDMA link evidence missing')
        modules = outputs.get('nvme_modules', {})
        if not isinstance(modules, dict) or any(not isinstance(modules.get(x), str) or not modules[x] for x in ('irdma', 'idpf', 'rdma_cm', 'nvme', 'nvme_rdma', 'nvmet_rdma')):
            raise ValueError('required kernel modules missing')
        if not str(outputs.get('openssl', '')).startswith('OpenSSL 3.') or not str(outputs.get('python', '')).startswith(('3.11.', '3.12.', '3.13.', '3.14.')):
            raise ValueError('Python/OpenSSL runtime evidence invalid')
        containers = outputs.get('containers')
        if not isinstance(containers, dict) or set(containers) != required_containers():
            raise ValueError('container inventory differs from reviewed pins')
        for reference, inspected in containers.items():
            if not isinstance(inspected, list) or len(inspected) != 1 or not isinstance(inspected[0], dict):
                raise ValueError('container inspection missing')
            digests = inspected[0].get('RepoDigests')
            if not isinstance(digests, list) or not any(canonical_image_digest(item) == canonical_image_digest(reference) for item in digests):
                raise ValueError('container digest inspection does not match reference')
        for op, port in (('read', '18515'), ('send', '18516')):
            commands = outputs.get('rdma_commands')
            expected = ['ib_' + op + '_lat', '-R', '-d', device, '-p', port, '-s', '1024', '-n', '1000', record.get('peer')]
            if not isinstance(commands, dict) or commands.get(op) != expected:
                raise ValueError('perftest must use the pinned RDMA-CM command')
            output = outputs.get('rdma_' + op)
            if not isinstance(output, str) or '#bytes #iterations' not in output or not re.search(r'rdma_cm QPs\s*:\s*ON', output, re.I):
                raise ValueError('perftest RDMA-CM transfer results missing')
            rows = [line.split() for line in output.splitlines() if re.match(r'^\s*1024\s+1000\s+', line)]
            if len(rows) != 1 or len(rows[0]) != 9:
                raise ValueError('perftest latency result shape invalid')
            try:
                values = [float(value) for value in rows[0][2:]]
            except ValueError as exc:
                raise ValueError('perftest latency result invalid') from exc
            if not all(math.isfinite(value) for value in values) or any(value <= 0 for index, value in enumerate(values) if index != 4) or values[4] < 0:
                raise ValueError('perftest latency values must be finite and positive')
        valkey = outputs.get('valkey', {})
        if not isinstance(valkey, dict) or valkey.get('endpoint') != str(record.get('peer')) + ':6380' or valkey.get('set') != 'OK' or valkey.get('del') != '1' or not re.fullmatch(r'[0-9a-f]{32}', str(valkey.get('value', ''))) or valkey.get('get') != valkey['value']:
            raise ValueError('Valkey RDMA SET/GET/DEL evidence invalid')
    if records[0]['instance_id'] != records[1]['peer_instance_id'] or records[1]['instance_id'] != records[0]['peer_instance_id']:
        raise ValueError('guest hardware proofs must be reciprocal')
    if records[0].get('local_rdma') != records[1].get('peer') or records[1].get('local_rdma') != records[0].get('peer') or records[0].get('local_rdma') == records[1].get('local_rdma'):
        raise ValueError('physical peer addresses must be reciprocal')
    for field in ('kernel', 'installed_rpms', 'libfabric_sha256', 'libfabric_version'):
        if records[0]['outputs'][field] != records[1]['outputs'][field]:
            raise ValueError(f'qualification guests differ in {field}')
    if _envelope(records[0]) != _envelope(records[1]):
        raise ValueError('qualified guest ABI envelopes differ')


def admit_image(api, config, image_info):
    ref = config.get('image_qualification')
    if not ref:
        raise ValueError('image qualification is not configured')
    image = config.get('image', '')
    match = _IMAGE.fullmatch(image)
    if not match or match.group(1) != config.get('project_id'):
        raise ValueError('qualification requires a custom image in the configured project')
    image_id = str(image_info.get('id', ''))
    if image_info.get('status') != 'READY' or not image_id.isdigit():
        raise ValueError('image is not READY with a numeric ID')
    link = image_info.get('selfLink', '')
    if not isinstance(link, str) or not link.endswith('/' + image):
        raise ValueError('fetched image identity does not match configured path')
    receipt = _load(api, ref, config.get('bucket'), 1024 * 1024)
    identity = {'path': image, 'id': image_id}
    if receipt.get('schema_version') != 1 or receipt.get('image') != identity:
        raise ValueError('qualification does not bind this exact image and numeric ID')
    if receipt.get('recipes') != recipe_hashes():
        raise ValueError('qualification recipe hashes do not match reviewed code')
    evidence = receipt.get('evidence')
    if not isinstance(evidence, list) or len(evidence) != 2:
        raise ValueError('two generation-pinned hardware evidence objects required')
    records = [_load(api, item, config['bucket'], 16 * 1024 * 1024) for item in evidence]
    validate_pair(records, identity, receipt.get('runtime_sha256'), config.get('service_account'))
    return {**receipt, '_records': records}
