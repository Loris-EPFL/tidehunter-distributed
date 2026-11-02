#!/usr/bin/env python3
"""Install reviewed offline RPMs and explicitly pull pinned images at bake time.

This command creates no VM or image. The manifest/RPM payload must be reviewed;
kernel upgrades and unsigned RPMs are refused. Registry access requires a flag.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import urllib.parse
import urllib.request

import gcp_api

import gcp_image


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def run(argv, timeout=300):
    return subprocess.check_output(argv, text=True, stderr=subprocess.STDOUT, timeout=timeout).strip()


def payload(root, item):
    if not isinstance(item, dict) or set(item) != {'path', 'sha256'}:
        raise ValueError('payload entry needs path and sha256')
    if not isinstance(item['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', item['sha256']):
        raise ValueError('invalid payload SHA-256')
    root = root.resolve(strict=True)
    path = (root / item['path']).resolve(strict=True)
    if not path.is_file() or root not in path.parents or sha(path) != item['sha256']:
        raise ValueError('payload path or digest mismatch')
    return path


BASE_IMAGE = 'projects/rocky-linux-cloud/global/images/rocky-linux-10-optimized-gcp-v20261026'
BASE_IMAGE_ID = '5989205676800505323'
HPC_BASE_IMAGE = 'projects/cloud-hpc-image-public/global/images/hpc-rocky-linux-9-v20260930'
HPC_BASE_IMAGE_ID = '3102614992461399694'
FABRIC_URL = 'https://github.com/ofiwg/libfabric/releases/download/v2.6.0/libfabric-2.6.0.tar.bz2'
FABRIC_SHA512 = '4aa70dfad7d38c142c6758ce00a23fc0f2f919cb3a4acf9ea294c6bc1db182fc402e4e6578148b34a37b9bd0dad09045cf0284d118c6a5e47eea8b766c051c37'


def metadata(path):
    request = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path,
                                     headers={'Metadata-Flavor': 'Google'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=10) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            raise ValueError('untrusted metadata server')
        return response.read().decode().strip()


def check_base_image(expected_image=BASE_IMAGE, expected_id=BASE_IMAGE_ID):
    project = metadata('project/project-id')
    zone = metadata('instance/zone').rsplit('/', 1)[-1]
    instance = metadata('instance/name')
    api = gcp_api.GoogleApi(project, token_provider=gcp_api.metadata_token)
    info = api.request('GET', f'zones/{zone}/instances/{instance}')
    disks = [disk for disk in info.get('disks', []) if disk.get('boot')]
    if len(disks) != 1 or not disks[0].get('source'):
        raise ValueError('cannot identify the boot disk')
    source = disks[0]['source']
    prefix = f'/projects/{project}/zones/{zone}/disks/'
    if prefix not in source:
        raise ValueError('boot disk is not in the local project and zone')
    name = source.split(prefix, 1)[1]
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', name):
        raise ValueError('invalid boot disk name')
    disk = api.request('GET', f'zones/{zone}/disks/{name}')
    if str(disk.get('sourceImageId', '')) != expected_id or not disk.get('sourceImage', '').endswith('/' + expected_image):
        raise ValueError('boot disk does not originate from the pinned public base image')


def install(manifest_path, expected_sha, activate, allow_registry_pull=False):
    if not activate or os.geteuid() != 0:
        raise ValueError('root and explicit --activate are required')
    if not allow_registry_pull:
        raise ValueError('bake requires explicit --allow-registry-pull')
    manifest_path = manifest_path.resolve(strict=True)
    if not re.fullmatch(r'[0-9a-f]{64}', expected_sha) or sha(manifest_path) != expected_sha:
        raise ValueError('manifest does not match reviewed SHA-256')
    manifest = json.loads(manifest_path.read_text())
    check_base_image()
    if manifest.get('schema_version') != 1 or manifest.get('kernel_release') != os.uname().release:
        raise ValueError('manifest must pin the running kernel release')
    packages = manifest.get('rpms')
    images = manifest.get('images')
    if not isinstance(packages, list) or not packages or not isinstance(images, list) or not images:
        raise ValueError('reviewed RPM and container payloads required')
    keys = manifest.get('signing_keys')
    if not isinstance(keys, list) or not keys:
        raise ValueError('reviewed signing keys and fingerprints required')
    signing_keys = {}
    for item in keys:
        if not isinstance(item, dict) or set(item) != {'payload', 'fingerprint'}:
            raise ValueError('signing key requires payload and fingerprint')
        fingerprint = item['fingerprint']
        if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9A-F]{40,64}', fingerprint) or fingerprint in signing_keys:
            raise ValueError('invalid or duplicate signing-key fingerprint')
        key = payload(manifest_path.parent, item['payload'])
        listing = run(['gpg', '--batch', '--show-keys', '--with-colons', str(key)])
        primary = [line.split(':')[9] for line in listing.splitlines() if line.startswith('fpr:')]
        if not primary or primary[0] != fingerprint:
            raise ValueError('signing-key fingerprint mismatch')
        signing_keys[fingerprint] = key
    paths, nevras = [], set()
    for item in packages:
        if not isinstance(item, dict) or set(item) != {'payload', 'nevra', 'signer'}:
            raise ValueError('RPM entry requires payload, NEVRA and signer fingerprint')
        path = payload(manifest_path.parent, item['payload'])
        if item['signer'] not in signing_keys:
            raise ValueError('RPM signer is not a reviewed key')
        with tempfile.TemporaryDirectory(prefix='tidehunter-rpmdb-') as db:
            run(['rpm', '--dbpath', db, '--initdb'])
            run(['rpm', '--dbpath', db, '--import', str(signing_keys[item['signer']])])
            signature = run(['rpm', '--dbpath', db, '--checksig', '--verbose', str(path)])
            if not re.search(r'(?im)^\s*Header .*signature.*: OK\s*$', signature):
                raise ValueError('RPM lacks the pinned trusted signature: ' + str(path))
        nevra = run(['rpm', '-qp', '--qf', '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}', str(path)])
        if nevra != item['nevra'] or nevra in nevras:
            raise ValueError('RPM NEVRA mismatch or duplicate')
        name = run(['rpm', '-qp', '--qf', '%{NAME}', str(path)])
        if name in {'kernel', 'kernel-core', 'kernel-modules', 'kernel-modules-core', 'kernel-modules-extra'}:
            raise ValueError('kernel RPM changes are forbidden')
        paths.append(path)
        nevras.add(nevra)
    if not any(x.startswith('kmod-idpf-irdma-') for x in nevras):
        raise ValueError('reviewed CIQ IRDMA driver RPM required')
    containers = []
    for item in images:
        if not isinstance(item, dict) or set(item) != {'reference'}:
            raise ValueError('container entry requires a digest-pinned reference')
        if not isinstance(item['reference'], str) or not re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', item['reference']):
            raise ValueError('container must use a digest-pinned reference')
        containers.append(item['reference'])
    references = containers
    if len(set(references)) != len(references) or set(references) != gcp_image.required_containers():
        raise ValueError('payload must contain all reviewed comparison and monitoring images')
    # Import only after all hashes, fingerprints, signatures and image pins validate.
    for key in signing_keys.values():
        run(['rpm', '--import', str(key)])
    run(['dnf', '-y', '--disablerepo=*', '--setopt=localpkg_gpgcheck=1',
         '--setopt=install_weak_deps=False', 'install'] + [str(p) for p in paths], timeout=1800)
    return finish_install(expected_sha, manifest['kernel_release'], containers)


def inspect_modules(kernel_release, *, allow_loaded_kabi=False):
    """Require exact vermagic, or an explicitly admitted loaded weak-update module."""
    evidence = {}
    for module in ('irdma', 'idpf'):
        vermagic = run(['modinfo', '-F', 'vermagic', module]).split()
        filename = Path(run(['modinfo', '-n', module]))
        resolved = filename.resolve(strict=True)
        owner = run(['rpm', '-qf', str(resolved)])
        if not owner.startswith('kmod-idpf-irdma-') or not vermagic:
            raise ValueError(f'{module} is not owned by the provider kmod package')
        running_src = Path('/sys/module') / module / 'srcversion'
        srcversion = run(['modinfo', '-F', 'srcversion', module])
        if vermagic[0] != kernel_release:
            if (not allow_loaded_kabi or 'weak-updates' not in filename.parts or
                    not filename.is_symlink() or not srcversion or
                    not running_src.is_file() or running_src.read_text().strip() != srcversion):
                raise ValueError(f'{module} requires a loaded, matching provider weak-update module')
        evidence[module] = {'filename': str(filename), 'resolved': str(resolved), 'owner': owner,
                            'vermagic': ' '.join(vermagic), 'srcversion': srcversion,
                            'loaded_srcversion': running_src.read_text().strip() if running_src.is_file() else None}
    return evidence


def finish_install(expected_sha, kernel_release, containers, python='/usr/bin/python3', allow_loaded_kabi=False):
    venv = Path('/opt/tidehunter-python')
    canonical_python = str(venv / 'bin/python3')
    if not venv.exists():
        if python == canonical_python:
            raise ValueError('canonical guest Python does not exist')
        run([python, '-m', 'venv', '--system-site-packages', str(venv)], timeout=120)
    python = canonical_python
    limits = Path('/etc/security/limits.d/99-tidehunter-memlock.conf')
    limits.parent.mkdir(parents=True, exist_ok=True)
    limits.write_text('root soft memlock unlimited\nroot hard memlock unlimited\n')
    docker_dropin = Path('/etc/systemd/system/docker.service.d/99-tidehunter-memlock.conf')
    docker_dropin.parent.mkdir(parents=True, exist_ok=True)
    docker_dropin.write_text('[Service]\nLimitMEMLOCK=infinity\n')
    run(['systemctl', 'daemon-reload'], timeout=30)
    run(['systemctl', 'enable', 'docker'], timeout=120)
    run(['systemctl', 'restart', 'docker'], timeout=120)
    compose_version = run(['docker', 'compose', 'version'])
    python_version = run([python, '-c',
                          'import sys,yaml; assert sys.version_info >= (3,11); print(sys.version)'])
    openssl = run(['openssl', 'version'])
    if not openssl.startswith('OpenSSL 3.'):
        raise ValueError('OpenSSL 3 is required')
    fabric_version = run(['fi_info', '--version'])
    if not re.search(r'\b2\.6\.0\b', fabric_version):
        raise ValueError('libfabric 2.6.0 is required')
    for tool in ('nvme', 'rdma', 'ibv_devinfo', 'ib_read_lat', 'ib_send_lat', 'ldd', 'gcc', 'make', 'fio', 'protoc'):
        run(['which', tool])
    for reference in containers:
        run(['docker', 'pull', reference], timeout=600)
        inspected = json.loads(run(['docker', 'image', 'inspect', reference]))
        if len(inspected) != 1 or not any(gcp_image.canonical_image_digest(d) == gcp_image.canonical_image_digest(reference)
                                          for d in inspected[0].get('RepoDigests', [])):
            raise ValueError('baked image repository digest mismatch')
    module_evidence = inspect_modules(kernel_release, allow_loaded_kabi=allow_loaded_kabi)
    if os.uname().release != kernel_release:
        raise ValueError('running kernel changed during installation')
    installed = sorted(run(['rpm', '-qa', '--qf', '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\\n']).splitlines())
    return {'manifest_sha256': expected_sha, 'kernel_release': os.uname().release,
            'installed_rpms': installed, 'python': python_version, 'openssl': openssl,
            'libfabric_version': fabric_version, 'docker_compose': compose_version, 'modules': module_evidence,
            'container_references': sorted(containers)}


def _hpc_inputs(manifest_path, manifest):
    """Validate reviewed package/repository inputs without mutating the guest."""
    if manifest.get('schema_version') != 2 or manifest.get('mode') != 'hpc-rocky9':
        raise ValueError('HPC manifest schema or mode mismatch')
    if manifest.get('kernel_release') != os.uname().release:
        raise ValueError('HPC manifest must pin the running kernel')
    packages = manifest.get('rpm_packages')
    if not isinstance(packages, list) or any(not isinstance(item, str) for item in packages) or len(packages) != len(set(packages)):
        raise ValueError('explicit reviewed package NEVRA list required')
    for package in packages:
        if not isinstance(package, str) or not re.fullmatch(r'[A-Za-z0-9_+.-]+-[0-9]+:[A-Za-z0-9_+~.]+-[A-Za-z0-9_+~.]+\.x86_64|[A-Za-z0-9_+.-]+-[0-9]+:[A-Za-z0-9_+~.]+-[A-Za-z0-9_+~.]+\.noarch', package):
            raise ValueError('package must be an exact epoch/version/release/architecture NEVRA')
        if re.match(r'^(?:kernel(?:-|$)|kmod-idpf-irdma-)', package):
            raise ValueError('kernel and provider driver upgrades are forbidden')
    keys = {}
    for item in manifest.get('signing_keys', []):
        if not isinstance(item, dict) or set(item) != {'payload', 'fingerprint'}:
            raise ValueError('signing key needs payload and reviewed fingerprint')
        fingerprint = item['fingerprint']
        if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9A-F]{40,64}', fingerprint) or fingerprint in keys:
            raise ValueError('invalid signing key fingerprint')
        key = payload(manifest_path.parent, item['payload'])
        listing = run(['gpg', '--batch', '--show-keys', '--with-colons', str(key)])
        fingerprints = [line.split(':')[9] for line in listing.splitlines() if line.startswith('fpr:')]
        if not fingerprints or fingerprints[0] != fingerprint:
            raise ValueError('signing key fingerprint mismatch')
        keys[fingerprint] = key
    repositories = manifest.get('repositories')
    if not isinstance(repositories, list) or (packages and (not keys or not repositories)):
        raise ValueError('reviewed official repositories and signing keys required for package installation')
    checked_repos = []
    for repo in repositories:
        if not isinstance(repo, dict) or set(repo) != {'id', 'baseurl', 'fingerprint'}:
            raise ValueError('repository needs ID, HTTPS base URL and pinned signing key')
        name, url, fingerprint = repo['id'], repo['baseurl'], repo['fingerprint']
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', name) or fingerprint not in keys:
            raise ValueError('invalid repository identity or unapproved key')
        parsed = urllib.parse.urlsplit(url) if isinstance(url, str) else None
        if (not parsed or not re.fullmatch(r'https://(?:dl\.rockylinux\.org|download\.docker\.com)/[A-Za-z0-9_./-]+', url)
                or parsed.scheme != 'https' or parsed.hostname not in {'dl.rockylinux.org', 'download.docker.com'}
                or parsed.username or parsed.password or parsed.port or parsed.query or parsed.fragment or '..' in parsed.path.split('/')):
            raise ValueError('repository must use an approved official HTTPS origin')
        expected_fingerprint = ('21CB256AE16FC54C6E652949702D426D350D275D'
                                if parsed.hostname == 'dl.rockylinux.org'
                                else '060A61C51B558A7F742B77AAC52FEB6B621E9F35')
        if fingerprint != expected_fingerprint:
            raise ValueError('repository signing key is not the verified official vendor key')
        checked_repos.append((name, url, keys[fingerprint]))
    if len({x[0] for x in checked_repos}) != len(checked_repos):
        raise ValueError('duplicate repository IDs')
    containers = manifest.get('images')
    if not isinstance(containers, list) or any(not isinstance(x, dict) or set(x) != {'reference'} for x in containers):
        raise ValueError('digest-pinned image inventory required')
    references = [x['reference'] for x in containers]
    if any(not isinstance(x, str) for x in references) or len(set(references)) != len(references) or set(references) != gcp_image.required_containers():
        raise ValueError('image inventory differs from reviewed pins')
    fabric_archive = payload(manifest_path.parent, manifest['fabric_archive']) if manifest.get('fabric_archive') is not None else None
    if fabric_archive:
        digest = hashlib.sha512()
        with fabric_archive.open('rb') as src:
            for chunk in iter(lambda: src.read(1024 * 1024), b''):
                digest.update(chunk)
        if digest.hexdigest() != FABRIC_SHA512:
            raise ValueError('libfabric archive differs from official release SHA-512')
    wheel = payload(manifest_path.parent, manifest['pyyaml_wheel']) if manifest.get('pyyaml_wheel') is not None else None
    if wheel is not None and (not wheel.name.endswith('.whl') or not wheel.name.lower().startswith('pyyaml-')):
        raise ValueError('PyYAML input must be a reviewed wheel')
    return packages, keys, checked_repos, references, fabric_archive, wheel


def _build_fabric(archive):
    if archive is None:
        raise ValueError('preinstalled libfabric is not 2.6.0 with required providers; reviewed official source archive required')
    with tempfile.TemporaryDirectory(prefix='tidehunter-libfabric-') as temporary:
        root = Path(temporary)
        run(['tar', '-xjf', str(archive), '-C', str(root)], timeout=120)
        source = root / 'libfabric-2.6.0'
        if not (source / 'configure').is_file():
            raise ValueError('official libfabric archive has unexpected layout')
        configure = ['./configure', '--prefix=/usr/local', '--libdir=/usr/local/lib',
                     '--enable-only', '--enable-verbs', '--enable-tcp', '--enable-rxm', '--disable-static']
        for argv, timeout in ((configure, 600), (['make', '-j2'], 1800), (['make', 'install'], 600)):
            subprocess.check_output(argv, cwd=source, text=True, stderr=subprocess.STDOUT, timeout=timeout)
    Path('/etc/ld.so.conf.d/tidehunter-libfabric.conf').write_text('/usr/local/lib\n')
    run(['ldconfig'], timeout=60)
    return {'source_url': FABRIC_URL, 'sha512': FABRIC_SHA512, 'sha256': sha(archive),
            'configure': configure}


def _fabric_ready():
    try:
        version = run(['fi_info', '--version'])
        if not re.search(r'\b2\.6\.0\b', version):
            return False
        for provider in ('verbs;ofi_rxm', 'tcp;ofi_rxm'):
            if provider not in run(['fi_info', '-p', provider]):
                return False
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False




def install_hpc(manifest_path, expected_sha, activate, allow_registry_pull, allow_repo_network, allow_loaded_kabi, allow_io_uring):
    if not activate or os.geteuid() != 0:
        raise ValueError('root and explicit --activate are required')
    if not allow_registry_pull or not allow_io_uring:
        raise ValueError('HPC bake requires --allow-registry-pull and --allow-io-uring')
    os.environ['PATH'] = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
    for key in ('LD_PRELOAD', 'LD_LIBRARY_PATH', 'PYTHONPATH', 'PYTHONHOME'):
        os.environ.pop(key, None)
    manifest_path = manifest_path.resolve(strict=True)
    if not re.fullmatch(r'[0-9a-f]{64}', expected_sha) or sha(manifest_path) != expected_sha:
        raise ValueError('manifest does not match reviewed SHA-256')
    manifest = json.loads(manifest_path.read_text())
    check_base_image(HPC_BASE_IMAGE, HPC_BASE_IMAGE_ID)
    packages, keys, repositories, containers, archive, wheel = _hpc_inputs(manifest_path, manifest)
    if packages and not allow_repo_network:
        raise ValueError('package installation requires explicit --allow-repo-network')
    kernel = manifest['kernel_release']
    before_modules = inspect_modules(kernel, allow_loaded_kabi=allow_loaded_kabi)
    driver = run(['rpm', '-q', 'kmod-idpf-irdma'])
    if not driver.startswith('kmod-idpf-irdma-'):
        raise ValueError('provider-preinstalled CIQ IRDMA driver required')
    # Explicitly reviewed repository URLs and key fingerprints; package signatures
    # stay mandatory, and neither kernel nor provider kmod can be replaced.
    downloaded_rpms = []
    if packages:
        for key in keys.values():
            run(['rpm', '--import', str(key)])
        with tempfile.TemporaryDirectory(prefix='tidehunter-yum-repos-') as temporary:
            for name, baseurl, key in repositories:
                (Path(temporary) / (name + '.repo')).write_text(
                    f'[{name}]\nname={name}\nbaseurl={baseurl}\nenabled=1\ngpgcheck=1\ngpgkey={key.as_uri()}\n')
            argv = ['dnf', '-y', '--setopt=reposdir=' + temporary, '--setopt=gpgcheck=1',
                    '--setopt=localpkg_gpgcheck=1', '--setopt=install_weak_deps=False',
                    '--setopt=keepcache=1', '--setopt=cachedir=' + str(Path(temporary) / 'cache'),
                    '--exclude=kernel*', '--exclude=kmod-idpf-irdma*', 'install', *packages]
            run(argv, timeout=1800)
            for rpm in sorted((Path(temporary) / 'cache').rglob('*.rpm')):
                signature = run(['rpm', '--checksig', '--verbose', str(rpm)])
                if not re.search(r'(?im)^\s*Header .*signature.*: OK\s*$', signature):
                    raise ValueError('downloaded RPM lacks a trusted signature')
                nevra = run(['rpm', '-qp', '--qf', '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}', str(rpm)])
                downloaded_rpms.append({'nevra': nevra, 'sha256': sha(rpm), 'signature': signature})
    if os.uname().release != kernel or run(['rpm', '-q', 'kmod-idpf-irdma']) != driver:
        raise ValueError('kernel or preinstalled driver changed')
    if inspect_modules(kernel, allow_loaded_kabi=allow_loaded_kabi) != before_modules:
        raise ValueError('provider modules changed during package installation')
    for nevra in packages:
        name = nevra.rsplit('-', 2)[0]
        observed = run(['rpm', '-q', name, '--qf', '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}'])
        if observed != nevra:
            raise ValueError('installed RPM differs from reviewed NEVRA: ' + name)
    venv = Path('/opt/tidehunter-python')
    if not venv.exists():
        run(['/usr/bin/python3.11', '-m', 'venv', '--system-site-packages', str(venv)], timeout=120)
    python = str(venv / 'bin/python3')
    if wheel:
        run([python, '-m', 'pip', 'install', '--no-index', '--no-deps', str(wheel)], timeout=120)
    run([python, '-c', 'import sys,yaml; assert sys.version_info >= (3,11); assert yaml.__version__ == \"6.0.2\"'])
    fabric_build = None
    if not _fabric_ready():
        fabric_build = _build_fabric(archive)
    if not _fabric_ready():
        raise ValueError('libfabric 2.6.0 verbs/RxM and TCP/RxM providers unavailable')
    Path('/etc/sysctl.d/99-tidehunter-io-uring.conf').write_text('kernel.io_uring_disabled = 0\n')
    run(['sysctl', '-w', 'kernel.io_uring_disabled=0'])
    if Path('/proc/sys/kernel/io_uring_disabled').read_text().strip() != '0':
        raise ValueError('io_uring sysctl could not be enabled')
    result = finish_install(expected_sha, kernel, containers, python=python, allow_loaded_kabi=allow_loaded_kabi)
    result.update({'base_image': HPC_BASE_IMAGE, 'base_image_id': HPC_BASE_IMAGE_ID, 'preinstalled_driver': driver,
                   'repositories': [{'id': name, 'baseurl': url, 'key_sha256': sha(key)} for name, url, key in repositories],
                   'signing_fingerprints': sorted(keys), 'fabric_build': fabric_build, 'downloaded_rpms': downloaded_rpms,
                   'pyyaml_wheel_sha256': sha(wheel) if wheel else None, 'io_uring_disabled': 0})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('offline-rocky10', 'hpc-rocky9'), default='offline-rocky10')
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--activate', action='store_true')
    parser.add_argument('--allow-registry-pull', action='store_true', help='allow digest-pinned registry pulls during bake only')
    parser.add_argument('--allow-repo-network', action='store_true', help='allow reviewed signed package repositories during HPC bake')
    parser.add_argument('--allow-loaded-kabi', action='store_true', help='accept verified loaded vendor weak-update modules')
    parser.add_argument('--allow-io-uring', action='store_true', help='persistently enable io_uring for benchmark workloads')
    args = parser.parse_args()
    if args.mode == 'hpc-rocky9':
        result = install_hpc(args.manifest, args.manifest_sha256, args.activate, args.allow_registry_pull,
                             args.allow_repo_network, args.allow_loaded_kabi, args.allow_io_uring)
    else:
        result = install(args.manifest, args.manifest_sha256, args.activate, args.allow_registry_pull)
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
