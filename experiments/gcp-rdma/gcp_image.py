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

import gcp_runtime
_HEX = re.compile(r'^[0-9a-f]{64}$')
_IMAGE = re.compile(r'^projects/([a-z][a-z0-9-]{4,61}[a-z0-9])/global/images/([a-z][a-z0-9-]{0,61}[a-z0-9])$')
_FAMILIES = frozenset({'GLIBC', 'GLIBCXX', 'CXXABI', 'OPENSSL'})
FOCUSED_PROOFS = ('guest_runtime', 'runtime_abi', 'scratch_nvme_cli', 'io_uring',
                  'nvme_modules', 'libfabric_verbs', 'rdma_read_send', 'monitoring_images')


def required_containers(*, focused=False):
    if focused:
        root = Path(__file__).resolve().parent.parent / 'kv-comparison'
        monitoring = json.loads((root / 'observability' / 'images.lock.json').read_text())['images']
        return {entry['image'] for entry in monitoring.values()}
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


def _envelope(record, *, sui=False):
    package = record['outputs']['package']
    _, baseline_versions = _abi(package.get('abi'))
    sonames = set()
    versions = {family: set(baseline_versions[family]) for family in _FAMILIES}
    proofs = [package['linkage'], *package.get('valkey_runtime_linkage', {}).values()]
    if 'spdk' in package:
        proofs.append(package['spdk_runtime_linkage'])
    if sui:
        proofs = [package.get('sui_library_linkage')]
    for proof in proofs:
        for elf in _linkage(proof).values():
            if elf.get('soname'):
                sonames.add(elf['soname'])
            for family, definitions in _versions(elf['version_definitions']).items():
                versions[family].update(definitions)
    if not sonames:
        raise ValueError('baseline has no resolved library SONAMEs')
    return sonames, versions


def admit_abi(qualification, abi, *, sui=False):
    """Check SONAMEs and four version families, not full symbol ABI or hardware execution."""
    needed, required = _abi(abi)
    records = qualification.get('_records') if isinstance(qualification, dict) else None
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('validated physical qualification records required')
    envelopes = [_envelope(record, sui=sui) for record in records]
    if envelopes[0] != envelopes[1]:
        raise ValueError('qualified guest ABI envelopes differ')
    sonames, versions = envelopes[0]
    if needed - sonames:
        raise ValueError('required SONAMEs absent from qualified guest: ' + ', '.join(sorted(needed - sonames)))
    for family in _FAMILIES:
        missing = set(required[family]) - versions[family]
        if missing:
            raise ValueError(f'{family} versions absent from qualified guest: ' + ', '.join(sorted(missing)))


def validate_pair(records, image, runtime_sha, service_account, *, qualification_profile='full', sui_needed_libraries=()):
    """Validate two reciprocal real-command records, not a pass/fail checklist."""
    _digest(runtime_sha, 'runtime manifest')
    if qualification_profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('two guest diagnostics are required')
    for record in records:
        if not isinstance(record, dict) or record.get('kind') != 'guest-diagnostic' or record.get('image') != image:
            raise ValueError('guest evidence does not bind the exact image')
        if qualification_profile == 'focused':
            if record.get('qualification_proofs') != list(FOCUSED_PROOFS):
                raise ValueError('focused qualification proof list mismatch')
        elif 'qualification_proofs' in record:
            raise ValueError('focused proof cannot qualify a full campaign')
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
        guest = outputs.get('guest_runtime')
        if (not isinstance(guest, dict) or guest.get('executable') != gcp_runtime.GUEST_PYTHON
                or guest.get('yaml') != '6.0.2'
                or not isinstance(guest.get('python'), str)
                or not guest['python'].startswith(('3.11.', '3.12.', '3.13.', '3.14.'))
                or type(guest.get('nofile_soft')) is not int or type(guest.get('nofile_hard')) is not int
                or (guest['nofile_hard'] != -1 and guest['nofile_hard'] < 524288)):
            raise ValueError('canonical guest Python, PyYAML and NOFILE hard-limit evidence required')
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
        if sui_needed_libraries:
            if package.get('sui_needed_libraries') != list(sui_needed_libraries):
                raise ValueError('Sui library proof does not bind the requested SONAMEs')
            extra = _linkage(package.get('sui_library_linkage'))
            if 'ld_library_path' not in package['sui_library_linkage'] or (
                    package['sui_library_linkage']['ld_library_path'] is not None
                    and not isinstance(package['sui_library_linkage']['ld_library_path'], str)):
                raise ValueError('Sui library proof search path is missing or invalid')
            missing = set(sui_needed_libraries) - {elf.get('soname') for elf in extra.values()}
            if missing:
                raise ValueError('Sui libraries absent from guest proof: ' + ', '.join(sorted(missing)))
        elif 'sui_library_linkage' in package:
            _linkage(package['sui_library_linkage'])
        valkey_linkage = package.get('valkey_runtime_linkage')
        if qualification_profile == 'focused':
            if package.get('build_profile') != 'focused':
                raise ValueError('focused qualification requires the focused package')
            if valkey_linkage is not None or 'valkey' in outputs:
                raise ValueError('focused qualification must not contain Valkey proof')
        else:
            if not isinstance(valkey_linkage, dict) or set(valkey_linkage) != {'valkey-server', 'valkey-cli', 'valkey-benchmark'}:
                raise ValueError('server, CLI and native benchmark Valkey linkage records required')
            for linkage in valkey_linkage.values():
                _linkage(linkage)
        if 'spdk' in package:
            _linkage(package.get('spdk_runtime_linkage'))
        fabric_path = package.get('fabric_library')
        has_fdb = any(Path(path).name.startswith('libfdb_c.so') for path in resolved)
        if (not isinstance(fabric_path, str) or fabric_path not in resolved
                or has_fdb is (qualification_profile == 'focused')):
            raise ValueError('benchmark FDB and libfabric linkage differs from profile')
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
        if qualification_profile == 'focused' and any(
                not isinstance(modules.get(x), str) or not modules[x]
                for x in ('nvmet', 'nvme_fabrics')):
            raise ValueError('focused NVMe target modules missing')
        if not str(outputs.get('openssl', '')).startswith('OpenSSL 3.') or not str(outputs.get('python', '')).startswith(('3.11.', '3.12.', '3.13.', '3.14.')):
            raise ValueError('Python/OpenSSL runtime evidence invalid')
        containers = outputs.get('containers')
        if not isinstance(containers, dict) or set(containers) != required_containers(focused=qualification_profile == 'focused'):
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
        if qualification_profile == 'full':
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
    packages = [record['outputs']['package'] for record in records]
    if any('sui_library_linkage' in package for package in packages):
        if (_envelope(records[0], sui=True) != _envelope(records[1], sui=True)
                or packages[0]['sui_library_linkage'].get('ld_library_path') != packages[1]['sui_library_linkage'].get('ld_library_path')):
            raise ValueError('qualification guests differ in Sui library linkage')
    if _envelope(records[0]) != _envelope(records[1]):
        raise ValueError('qualified guest ABI envelopes differ')



def admit_nvme_transports(qualification, backends, *, live=False):
    """Require both physical image guests to evidence each selected NVMe transport.

    RDMA-CM perftest in validate_pair proves the underlying peer fabric, not
    NVMe target discovery or I/O; those must still succeed during the campaign.
    """
    required = set()
    for backend, modules in (
        ('tidehunter_nvme_rdma', ('nvmet', 'nvme_fabrics', 'rdma_cm', 'nvme_rdma', 'nvmet_rdma')),
        ('tidehunter_nvme_tcp', ('nvmet', 'nvme_fabrics', 'nvme_tcp', 'nvmet_tcp')),
    ):
        if backend in backends:
            required.update(modules)
    if live and required:
        # The live target privately commissions namespace geometry over loopback TCP.
        required.update(('nvme_tcp', 'nvmet_tcp'))
    if not required:
        return
    records = qualification.get('_records')
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('validated image guest records required for NVMe')
    for record in records:
        modules = record.get('outputs', {}).get('nvme_modules', {})
        if not isinstance(modules, dict) or any(
                not isinstance(modules.get(name), str) or not modules[name].strip()
                for name in required):
            raise ValueError('qualified image lacks selected NVMe transport modules')


def admit_spdk_scope(spec, identity):
    if spec.get('spdk_comparison') and spec.get('mode') in ('standard-with-live-nvmeof', 'focused'):
        if (not isinstance(identity, dict)
                or identity.get('target_scope') != 'file_aio_direct_live_and_sealed'
                or identity.get('source_patch_sha256') != 'ba21692d7ac70190a505a7b94dc749c8f544d6eef8632a38980f4acf5e987373'):
            label = 'live SPDK' if spec['mode'] == 'standard-with-live-nvmeof' else 'focused SPDK'
            raise ValueError(label + ' requires the reviewed writable target and namespace-drain patch')


def admit_spdk(qualification, spec):
    """Require the selected SPDK package and runtime linker proof on both guests."""
    if not spec.get('spdk_comparison'):
        return
    records = qualification.get('_records')
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError('validated image guest records required for SPDK')
    identities = []
    for record in records:
        package = record.get('outputs', {}).get('package', {})
        identity = package.get('spdk')
        if not isinstance(identity, dict) or not identity:
            raise ValueError('qualified image lacks SPDK package evidence')
        admit_spdk_scope(spec, identity)
        _linkage(package.get('spdk_runtime_linkage'))
        identities.append(identity)
    if identities[0] != identities[1]:
        raise ValueError('qualified image guests differ in SPDK package identity')


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
    profile = config.get('qualification_profile', 'full')
    if profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if profile == 'focused':
        if (receipt.get('qualification_profile') != 'focused'
                or receipt.get('qualification_proofs') != list(FOCUSED_PROOFS)):
            raise ValueError('focused receipt proof list mismatch')
    elif 'qualification_profile' in receipt or 'qualification_proofs' in receipt:
        raise ValueError('focused receipt cannot qualify a full campaign')
    if receipt.get('schema_version') != 1 or receipt.get('image') != identity:
        raise ValueError('qualification does not bind this exact image and numeric ID')
    if receipt.get('recipes') != recipe_hashes():
        raise ValueError('qualification recipe hashes do not match reviewed code')
    evidence = receipt.get('evidence')
    if not isinstance(evidence, list) or len(evidence) != 2:
        raise ValueError('two generation-pinned hardware evidence objects required')
    records = [_load(api, item, config['bucket'], 16 * 1024 * 1024) for item in evidence]
    validate_pair(records, identity, receipt.get('runtime_sha256'), config.get('service_account'),
                  qualification_profile=profile, sui_needed_libraries=config.get('sui_needed_libraries', ()))
    return {**receipt, '_records': records}
