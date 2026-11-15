#!/usr/bin/env python3
"""Run guest-side hardware and software checks; never mint an image receipt.

Execute as the benchmark guest identity on a VM booted from the captured custom
image. Cross-host tests require pre-existing, separately controlled peer servers.
No instances, servers, disks, or images are created by this command.
"""
import argparse
import ctypes
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import uuid

import gcp_api
import gcp_image
import gcp_runtime
from gcp_image_prepare import metadata


def command_failure(exc):
    output = getattr(exc, 'output', None) or getattr(exc, 'stdout', None) or ''
    error = getattr(exc, 'stderr', None) or ''
    if isinstance(output, bytes):
        output = output.decode(errors='replace')
    if isinstance(error, bytes):
        error = error.decode(errors='replace')
    return f'{type(exc).__name__}: {exc}; captured output:\n{(output + error)[-32768:]}'


def command(argv, timeout=30):
    try:
        return subprocess.check_output([str(x) for x in argv], text=True, stderr=subprocess.STDOUT, timeout=timeout).strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(command_failure(exc)) from exc


def io_uring():
    if platform.machine() != 'x86_64':
        raise ValueError('io_uring syscall probe only supports x86_64')
    libc = ctypes.CDLL(None, use_errno=True)
    params = ctypes.create_string_buffer(120)
    fd = libc.syscall(425, 2, ctypes.byref(params))
    if fd < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    os.close(fd)
    return 'io_uring_setup(2) succeeded'


def boot_image(api, info, zone, project):
    disks = [disk for disk in info.get('disks', []) if disk.get('boot')]
    if len(disks) != 1:
        raise ValueError('cannot identify boot disk')
    prefix = f'/projects/{project}/zones/{zone}/disks/'
    source = disks[0].get('source', '')
    if prefix not in source:
        raise ValueError('boot disk project or zone mismatch')
    name = source.split(prefix, 1)[1]
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', name):
        raise ValueError('invalid boot disk name')
    disk = api.request('GET', f'zones/{zone}/disks/{name}')
    image_url = disk.get('sourceImage', '')
    marker = '/projects/'
    if marker not in image_url:
        raise ValueError('boot disk has no captured source image')
    image = image_url.split(marker, 1)[1]
    image = 'projects/' + image
    image_id = str(disk.get('sourceImageId', ''))
    if not image_id.isdigit() or not image.startswith(f'projects/{project}/global/images/'):
        raise ValueError('boot source must be an exact custom image and ID')
    return {'path': image, 'id': image_id}

def require_rdma_capabilities(evidence):
    caps = evidence['capabilities']
    required = ('max_mr', 'max_qp', 'max_qp_wr', 'max_cq', 'max_cqe',
                'max_pd', 'max_sge', 'max_sge_rd', 'max_mr_size', 'page_size_cap')
    if (not caps['available'] or any(
            not re.fullmatch(r'(?:0[xX][0-9a-fA-F]+|[0-9]+)', caps['fields'].get(key, ''))
            for key in required)):
        raise ValueError('required RDMA device capabilities unavailable: ' + str(caps)[:1000])



def sui_library_linkage(package, needed):
    """Use the Sui process's inherited loader search, without the KV override."""
    import cloud_package
    if (not isinstance(needed, (list, tuple)) or not needed or len(needed) > 16
            or any(not isinstance(name, str) or not name or re.search(r'[/\s:]', name) for name in needed)
            or len(set(needed)) != len(needed)):
        raise ValueError('invalid Sui library SONAMEs')
    binary = Path(package) / 'runtime/bin/kv-research'
    environment = dict(os.environ)
    baseline = cloud_package.linked_libraries(binary, environment, provider='gcp')
    present = {elf.get('soname') for elf in baseline['resolved'].values()}
    extra = sorted(set(needed) - present - {'ld-linux-x86-64.so.2'})
    proof = (cloud_package.linked_libraries(binary, environment, provider='gcp', extra_names=extra)
             if extra else baseline)
    missing = set(needed) - {elf.get('soname') for elf in proof['resolved'].values()}
    if missing:
        raise ValueError('unresolved Sui libraries: ' + ', '.join(sorted(missing)))
    proof['ld_library_path'] = environment.get('LD_LIBRARY_PATH')
    return proof


def admit_package(manifest, proof, qualification_profile):
    """Bind the qualification scope to the exact sealed package variant."""
    import cloud_package
    features = manifest['features']
    if qualification_profile == 'focused' and manifest.get('build_profile') == 'focused_valkey':
        linkage = proof.get('valkey_runtime_linkage')
        if (features != cloud_package.FOCUSED_VALKEY_FEATURES
                or proof.get('build_profile') != 'focused_valkey'
                or not isinstance(linkage, dict)
                or set(linkage) != {'valkey-server', 'valkey-cli', 'valkey-benchmark'}):
            raise ValueError('focused Valkey qualification requires the exact RDMA runtime')
    elif qualification_profile == 'focused':
        if (manifest.get('build_profile') != 'focused' or features != cloud_package.FOCUSED_FEATURES
                or proof.get('build_profile') != 'focused' or 'valkey_runtime_linkage' in proof):
            raise ValueError('focused qualification requires the TideHunter-only runtime')
    elif 'valkey-rdma' not in features:
        raise ValueError('sealed runtime lacks Valkey RDMA')


def guest(package, peer, peer_instance, rdma_device, expected_manifest_sha, *, qualification_profile='full', sui_needed_libraries=()):
    if qualification_profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    account = metadata('instance/service-accounts/default/email')
    if account != 'bench-guest@mysten-research.iam.gserviceaccount.com':
        raise ValueError('incorrect workload identity')
    runtime_admission = gcp_runtime.admit_guest_runtime()
    os.environ['PATH'] = gcp_runtime.GUEST_PATH
    project = metadata('project/project-id')
    zone = metadata('instance/zone').rsplit('/', 1)[-1]
    instance = metadata('instance/name')
    instance_id = metadata('instance/id')
    api = gcp_api.GoogleApi(project, token_provider=gcp_api.metadata_token)
    info = api.request('GET', f'zones/{zone}/instances/{instance}')
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', peer_instance):
        raise ValueError('invalid peer instance name')
    other = api.request('GET', f'zones/{zone}/instances/{peer_instance}')
    peer_id = str(other.get('id', ''))
    if not peer_id.isdigit() or peer_id == instance_id:
        raise ValueError('peer must be a distinct physical instance')
    image = boot_image(api, info, zone, project)
    if boot_image(api, other, zone, project) != image:
        raise ValueError('peer was not booted from the exact same custom image')
    peer_addresses = {nic.get('networkIP') for nic in other.get('networkInterfaces', []) if nic.get('nicType') == 'IRDMA'}
    if peer not in peer_addresses:
        raise ValueError('peer address does not belong to its IRDMA NIC')
    local_addresses = [nic.get('networkIP') for nic in info.get('networkInterfaces', []) if nic.get('nicType') == 'IRDMA']
    if len(local_addresses) != 1 or not local_addresses[0] or local_addresses[0] == peer:
        raise ValueError('exact distinct local IRDMA address required')
    if not info.get('machineType', '').endswith('/z4m-highmem-16'):
        raise ValueError('qualification requires the reviewed Z4M machine shape')
    from gcp_worker import admit_scratch
    scratch = admit_scratch({'machine_type': 'z4m-highmem-16'},
                            {'name': instance, 'zone': zone, 'id': instance_id}, api)
    devices = {'local-ssd': scratch['device']}
    if not Path('/dev/infiniband/rdma_cm').is_char_device():
        raise ValueError('RDMA CM device absent')
    device_path = Path('/sys/class/infiniband') / rdma_device
    if not re.fullmatch(r'[A-Za-z0-9_]+', rdma_device) or not device_path.resolve(strict=True).as_posix().startswith('/sys/devices/'):
        raise ValueError('physical RDMA device required')
    memlock = resource.getrlimit(resource.RLIMIT_MEMLOCK)
    if memlock != (resource.RLIM_INFINITY, resource.RLIM_INFINITY):
        raise ValueError('qualification requires unlimited soft and hard locked memory')
    docker_memlock = command(['systemctl', 'show', 'docker', '--property=LimitMEMLOCK', '--value'])
    if docker_memlock not in ('infinity', '18446744073709551615'):
        raise ValueError('Docker service must have unlimited locked memory')
    peer = str(ipaddress.IPv4Address(peer))
    if ipaddress.ip_address(peer).is_loopback:
        raise ValueError('remote RDMA address cannot be loopback')
    links = command(['rdma', 'link', 'show'])
    if not re.search(r'\blink\s+' + re.escape(rdma_device) + r'/', links):
        raise ValueError('RDMA device missing from rdma link')
    root = Path(__file__).resolve().parent.parent / 'kv-comparison'
    sys.path.insert(0, str(root))
    import rdma_diagnostics
    resource_evidence = rdma_diagnostics.capture_resources(rdma_device)
    require_rdma_capabilities(resource_evidence)
    outputs = {'io_uring': io_uring(), 'rdma_links': links,
               'python': sys.version, 'openssl': command(['openssl', 'version']),
               'docker': command(['docker', 'version', '--format', '{{.Server.Version}}']),
               'kernel': platform.release(), 'nvme_modules': {}}
    outputs['rdma_resources'] = resource_evidence
    outputs['guest_runtime'] = runtime_admission
    outputs['memlock'] = {'soft': 'infinity', 'hard': 'infinity', 'docker': docker_memlock}
    outputs['nvme_ioctl'] = {name: command(['nvme', 'id-ctrl', path]) for name, path in devices.items()}
    with tempfile.TemporaryDirectory(prefix='tidehunter-io-uring-') as directory:
        target = Path(directory) / 'probe'
        with target.open('wb') as file:
            file.truncate(1024 * 1024)
        fio = json.loads(command(['fio', '--name=io-uring-qualification', '--ioengine=io_uring',
                                  '--rw=read', '--size=1m', '--bs=4k', '--filename=' + str(target),
                                  '--output-format=json'], timeout=60))
        jobs = fio.get('jobs', [])
        if len(jobs) != 1 or jobs[0].get('error') != 0 or jobs[0].get('read', {}).get('io_bytes', 0) <= 0:
            raise ValueError('io_uring workload did not complete reads')
        outputs['io_uring_workload'] = fio
    outputs['installed_rpms'] = sorted(command(['rpm', '-qa', '--qf', '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\\n']).splitlines())
    if not any(item.startswith('kmod-idpf-irdma-') for item in outputs['installed_rpms']):
        raise ValueError('CIQ IRDMA kernel module RPM missing')
    if not outputs['openssl'].startswith('OpenSSL 3.'):
        raise ValueError('OpenSSL 3 required')
    for module in ('irdma', 'idpf', 'rdma_cm', 'nvme', 'nvmet', 'nvme_fabrics', 'nvme_rdma', 'nvmet_rdma'):
        command(['modprobe', module])
        outputs['nvme_modules'][module] = command(['modinfo', module])
    # TCP support is optional for existing profiles. Record it when available;
    # the focused TCP arm is admitted only if both modules are evidenced.
    for module in ('nvme_tcp', 'nvmet_tcp'):
        try:
            command(['modprobe', module])
            outputs['nvme_modules'][module] = command(['modinfo', module])
        except RuntimeError:
            pass
    providers = {}
    for provider, name in (('verbs', 'verbs;ofi_rxm'), ('tcp', 'tcp;ofi_rxm')):
        providers[provider] = command(['fi_info', '-p', name])
    outputs['libfabric_version'] = command(['fi_info', '--version'])
    if not re.search(r'\b2\.6\.0\b', outputs['libfabric_version']):
        raise ValueError('libfabric 2.6.0 required')
    outputs['providers'] = providers
    outputs['containers'] = {image: json.loads(command(['docker', 'image', 'inspect', image]))
                             for image in sorted(gcp_image.required_containers(focused=qualification_profile == 'focused'))}
    # The diagnostics import above already added the packaged KV helpers.
    import cloud_package
    package = Path(package).resolve(strict=True)
    proof = cloud_package.verify_directory(package, runtime=True, provider='gcp')
    if proof['manifest_sha256'] != expected_manifest_sha:
        raise ValueError('runtime manifest hash mismatch')
    manifest = json.loads((package / 'manifest.json').read_text())
    admit_package(manifest, proof, qualification_profile)
    if sui_needed_libraries:
        proof['sui_library_linkage'] = sui_library_linkage(package, sui_needed_libraries)
        proof['sui_needed_libraries'] = list(sui_needed_libraries)
    outputs['package'] = proof
    fabric = Path(proof['fabric_library']).resolve(strict=True)
    outputs['libfabric_sha256'] = hashlib.sha256(fabric.read_bytes()).hexdigest()
    fi_info = shutil.which('fi_info')
    if not fi_info:
        raise ValueError('fi_info executable missing')
    fi_ldd = command(['ldd', fi_info])
    found = re.findall(r'(?m)^\s*libfabric\.so[^ ]*\s+=>\s+(/[^\s]+)', fi_ldd)
    if len(found) != 1 or Path(found[0]).resolve(strict=True) != fabric or 'not found' in fi_ldd:
        raise ValueError('fi_info and benchmark resolve different libfabric libraries')
    probe = 'import ctypes,sys; x=ctypes.CDLL(sys.argv[1]); x.fi_version.restype=ctypes.c_uint; print(x.fi_version())'
    api_version = int(command([gcp_runtime.GUEST_PYTHON, '-c', probe, fabric]))
    if api_version <= 0:
        raise ValueError('actual linked libfabric API version is invalid')
    outputs['fi_info_ldd'] = fi_ldd
    outputs['fi_info_fabric'] = str(fabric)
    outputs['libfabric_api_version'] = api_version
    outputs['rdma_commands'] = {}
    for op, port in (('read', '18515'), ('send', '18516')):
        argv = ['ib_' + op + '_lat', '-R', '-d', rdma_device, '-p', port, '-s', '1024', '-n', '1000', peer]
        outputs['rdma_' + op] = command(argv, timeout=60)
        outputs['rdma_commands'][op] = argv
    if qualification_profile == 'full' or proof.get('build_profile') == 'focused_valkey':
        cli = package / 'runtime/bin/valkey-cli'
        key = 'tidehunter-qualification-' + uuid.uuid4().hex
        value = uuid.uuid4().hex
        def valkey(*args):
            return command([cli, '--rdma', '-h', peer, '-p', '6380', *args], timeout=15)
        if valkey('SET', key, value) != 'OK' or valkey('GET', key) != value or valkey('DEL', key) != '1':
            raise ValueError('cross-host Valkey RDMA SET/GET/DEL failed')
        outputs['valkey'] = {'endpoint': peer + ':6380', 'key': key, 'value': value, 'set': 'OK', 'get': value, 'del': '1'}
    recipes = gcp_image.recipe_hashes()
    result = {'kind': 'guest-diagnostic', 'image': image, 'recipes': recipes,
              'instance_id': instance_id, 'peer_instance_id': peer_id,
              'service_account': account, 'project': project, 'zone': zone, 'rdma_device': rdma_device,
              'scratch_devices': devices, 'local_rdma': local_addresses[0], 'peer': peer, 'outputs': outputs}
    if qualification_profile == 'focused':
        result['qualification_proofs'] = list(gcp_image.FOCUSED_PROOFS)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--peer', required=True)
    parser.add_argument('--peer-instance', required=True)
    parser.add_argument('--rdma-device', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = guest(args.package, args.peer, args.peer_instance, args.rdma_device, args.manifest_sha256)
    with args.output.open('x') as output:
        json.dump(result, output, sort_keys=True, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
