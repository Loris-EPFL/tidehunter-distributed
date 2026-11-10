#!/usr/bin/env python3
"""Admission and ephemeral installation of the reviewed CIQ nvmet fix.

The prefix has exactly nvmet.ko, abi-admission.json, source.json and
partial-nowait-read.patch. Offline admission is not runtime qualification.
Static admission requires readelf; guest installation additionally requires kmod.
Run install only on a newly allocated, quiescent benchmark-owned GCP guest.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import subprocess
import urllib.request

RELEASE = '5.14.0-687.49.1+2.1.el9_8_ciq.x86_64'
MODULE_SHA = '5ba7b96c3b232e7fc0d1200b674d6210324d87ed406f8b51f4b323609ccc233a'
PATCH_SHA = '520d1e109c7944664deb284199d1352cff4a3e05a9fd17d09d3ce73c664e2c04'
CANDIDATE_BUILD = '61aabd36adb3d8a51feb01cd289410d958c68c20'
CANDIDATE_SRC = '9F99AF855591751D53D463D'
STOCK_SRC = 'EE2F4FC798D5ABF4AC018D3'
SOURCE = {'schema_version': 1, 'kernel_release': RELEASE,
          'source_rpm_sha256': '3112a3f20d5536022f6e62c1d700e3b957ed23e79b5726898c2cdb5daaee1116',
          'devel_rpm_sha256': 'eab5f460a1fc57cce2467f4363e0e0128ca0f9fca755dc87b0d14b3fe53149a6',
          'source_rpm_url': 'https://depot.ciq.com/public/files/ciq-sigcloud-next-9/ciq-sigcloud-next-9.source/Packages/k/kernel-5.14.0-687.49.1+2.1.el9_8_ciq.src.rpm',
          'signing_key': '79D5C67D0851F7328AB20015E8258450C7780912'}
FILES = {'nvmet.ko', 'abi-admission.json', 'source.json', 'partial-nowait-read.patch'}
SYS_MODULE = Path('/sys/module')


def fail(message):
    raise ValueError(message)


def run(*args, strip=True):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, timeout=30, check=False)
    if result.returncode:
        raise RuntimeError(f'{args[0]} failed ({result.returncode}): {result.stderr.strip()}')
    return result.stdout.strip() if strip else result.stdout


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def build_id(path):
    text = run('readelf', '-n', str(path))
    ids = re.findall(r'Build ID:\s*([0-9a-fA-F]+)', text)
    if len(ids) != 1:
        fail('module must contain exactly one GNU build ID')
    return ids[0].lower()


def elf_modinfo(path, fields):
    """Read required .modinfo fields from the ELF, rejecting ambiguity."""
    text = run('readelf', '-p', '.modinfo', str(path), strip=False)
    values = {}
    for line in text.splitlines():
        match = re.fullmatch(r'\s*\[\s*[0-9a-fA-F]+\]\s+([A-Za-z0-9_]+)=(.*)', line)
        if not match or match.group(1) not in fields:
            continue
        key, value = match.groups()
        if key in values:
            fail('duplicate candidate modinfo field: ' + key)
        values[key] = value
    if set(values) != set(fields):
        fail('missing candidate modinfo fields')
    return values


def modinfo(path, fields):
    return {key: run('modinfo', '-F', key, str(path)) for key in fields}


def validate_prefix(prefix):
    prefix = Path(prefix)
    if not prefix.is_dir() or {p.name for p in prefix.iterdir()} != FILES:
        fail('kernel fix prefix must contain exactly the four admitted files')
    for name in FILES:
        p = prefix / name
        if p.is_symlink() or not p.is_file():
            fail('prefix entries must be regular files, not links')
    source = json.loads((prefix / 'source.json').read_text())
    if source != SOURCE:
        fail('source manifest does not match pinned CIQ inputs')
    if sha(prefix / 'abi-admission.json') != 'b739d600317b72f48a8591be1b4ed1dd9f949d5b8a95597337d3666e76141d67':
        fail('ABI receipt digest mismatch')
    receipt = json.loads((prefix / 'abi-admission.json').read_text())
    checks = receipt.get('checks')
    if (receipt.get('status') != 'PASS' or receipt.get('all_required_offline_checks_pass') is not True
            or not isinstance(checks, dict) or len(checks) != receipt.get('check_count')
            or any(v is not True for v in checks.values()) or receipt.get('failures') != []
            or receipt.get('kernel_release') != RELEASE
            or receipt.get('baseline_expected_srcversion') != STOCK_SRC
            or receipt.get('candidate_srcversion') != CANDIDATE_SRC):
        fail('offline ABI admission is incomplete or mismatched')
    fixed = receipt['variants']['fixed']
    if (sha(prefix / 'nvmet.ko') != MODULE_SHA or fixed['sha256'] != MODULE_SHA
            or fixed['gnu_build_id'] != CANDIDATE_BUILD
            or sha(prefix / 'partial-nowait-read.patch') != PATCH_SHA
            or receipt['reviewed_patch']['sha256'] != PATCH_SHA):
        fail('module or reviewed patch digest mismatch')
    fields = ('name', 'license', 'rhelversion', 'srcversion', 'depends', 'vermagic')
    actual = {k: v.strip() for k, v in elf_modinfo(prefix / 'nvmet.ko', fields).items()}
    expected = {k: fixed['modinfo'][k].strip() for k in fields}
    if actual != expected or actual['name'] != 'nvmet' or actual['srcversion'] != CANDIDATE_SRC:
        fail('candidate modinfo does not match ABI receipt')
    if build_id(prefix / 'nvmet.ko') != CANDIDATE_BUILD:
        fail('candidate ELF build ID mismatch')
    return {'scope': 'offline ABI admission only; cloud load and correctness not qualified',
            'module_sha256': MODULE_SHA, 'patch_sha256': PATCH_SHA,
            'source_sha256': sha(prefix / 'source.json'),
            'abi_sha256': sha(prefix / 'abi-admission.json'),
            'candidate_build_id': CANDIDATE_BUILD, 'candidate_srcversion': CANDIDATE_SRC,
            'kernel_release': RELEASE}


def metadata(key):
    req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + key,
                                 headers={'Metadata-Flavor': 'Google'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=5) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            fail('untrusted GCP metadata')
        return response.read(4097).decode().strip()


def guest_identity():
    name, run_id, owner, instance_id = (metadata(k) for k in
        ('instance/name', 'instance/attributes/bench-run', 'instance/attributes/bench-owner', 'instance/id'))
    if (not re.fullmatch(r'bench-[0-9a-f]{16}', run_id)
            or not name.startswith(run_id + '-') or not re.fullmatch(r'[0-9a-f]{32}', owner)
            or not instance_id.isdecimal()):
        fail('guest is not an identified benchmark-owned instance')
    return {'instance_name': name, 'instance_id': instance_id, 'run_id': run_id, 'owner_nonce': owner}


def policy():
    config = Path('/boot/config-' + RELEASE).read_text()
    cmdline = Path('/proc/cmdline').read_text().split()
    enforce = SYS_MODULE / 'module/parameters/sig_enforce'
    if enforce.exists():
        sig = enforce.read_text().strip().lower()
        if sig not in ('y', 'n', '1', '0'):
            fail('unknown module signature enforcement state')
        required = sig in ('y', '1')
    else:
        if not re.search(r'^(CONFIG_MODULE_SIG_FORCE=y|# CONFIG_MODULE_SIG_FORCE is not set)$', config, re.M):
            fail('module signature enforcement configuration unavailable')
        required = 'CONFIG_MODULE_SIG_FORCE=y' in config
    required |= any(x in ('module.sig_enforce=1', 'module.sig_enforce=y') for x in cmdline)
    lockdown_path = Path('/sys/kernel/security/lockdown')
    if lockdown_path.exists():
        lockdown = lockdown_path.read_text().strip()
        if not re.search(r'\[none\]', lockdown):
            fail('kernel lockdown prohibits unsigned module')
    else:
        if not re.search(r'^# CONFIG_SECURITY_LOCKDOWN_LSM is not set$', config, re.M):
            fail('kernel lockdown state unavailable')
        lockdown = 'not configured'
    if any(x.startswith('lockdown=') and x != 'lockdown=none' for x in cmdline):
        fail('kernel command line requests lockdown')
    efi = Path('/sys/firmware/efi')
    secure = False
    if efi.exists():
        variables = list((efi / 'efivars').glob('SecureBoot-*'))
        if len(variables) != 1:
            fail('EFI Secure Boot state unavailable')
        value = variables[0].read_bytes()
        if len(value) != 5 or value[4] not in (0, 1):
            fail('invalid EFI Secure Boot state')
        secure = bool(value[4])
    if required or secure:
        fail('unsigned candidate is prohibited by signature or Secure Boot policy')
    return {'module_sig_enforce': False, 'secure_boot': secure, 'lockdown': lockdown,
            'candidate_signed': False, 'btf_equivalence_claimed': False}


def loaded_identity(name):
    path = SYS_MODULE / name
    note = (path / 'notes/.note.gnu.build-id').read_bytes()
    if len(note) < 16:
        fail('loaded module build ID note missing')
    namesz, descsz, kind = struct.unpack_from('<III', note)
    start = 12 + ((namesz + 3) & ~3)
    if kind != 3 or note[12:12 + namesz] != b'GNU\0' or descsz != 20 or len(note) < start + descsz:
        fail('invalid loaded module build ID note')
    return {'srcversion': (path / 'srcversion').read_text().strip(),
            'build_id': note[start:start + descsz].hex()}


def quiescent():
    root = Path('/sys/kernel/config/nvmet')
    if not root.exists() and (SYS_MODULE / 'nvmet').exists():
        fail('loaded nvmet configfs state is unavailable')
    for child in ('ports', 'subsystems'):
        path = root / child
        if root.exists() and (not path.is_dir() or any(path.iterdir())):
            fail('nvmet configfs is not empty')
    for ctrl in Path('/sys/class/nvme').glob('nvme*'):
        transport = ctrl / 'transport'
        if not transport.exists() or transport.read_text().strip() != 'pcie':
            fail('NVMe fabrics controller exists or transport is unknown')


def idle(name, allowed_holders=()):
    module = SYS_MODULE / name
    if not module.exists():
        return False
    holders = {x.name for x in (module / 'holders').iterdir()}
    refcount = int((module / 'refcnt').read_text().strip())
    if holders - set(allowed_holders) or refcount != len(holders):
        fail(f'{name} is not idle (refcount/holders)')
    return True


def stock_identities(receipt):
    result = {}
    for name in ('nvmet', 'nvmet-rdma', 'nvmet-tcp'):
        p = Path(run('modinfo', '-n', name))
        original = receipt['original_vendor_rpm_payloads'].get(name + '.ko.xz')
        if not original or sha(p) != original['compressed_sha256']:
            fail('installed stock module mismatch: ' + name)
        fields = ('name', 'srcversion', 'depends', 'vermagic')
        info = modinfo(p, fields)
        if info != {k: original['modinfo'][k].strip() for k in fields}:
            fail('installed stock module metadata mismatch: ' + name)
        # The exact signed compressed bytes bind the independently admitted
        # transport import CRCs and the original ELF build ID.
        result[name] = {'sha256': sha(p), 'build_id': original['build_id'], 'modinfo': info}
    return result


def trusted_prefix(prefix):
    p = Path(prefix).absolute()
    for item in [p, *p.parents]:
        st = item.lstat()
        if stat.S_ISLNK(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022:
            fail('prefix and ancestors must be root-owned and not writable by others')
    for name in FILES:
        st = (p / name).lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o022 or st.st_nlink != 1:
            fail('prefix files must be root-owned immutable-by-others regular files')
    return p


def provider_identities():
    # These are observed, never unloaded or modified by this helper.
    return {name: loaded_identity(name) if (SYS_MODULE / name).exists() else None
            for name in ('idpf', 'irdma')}


def present_identity(name):
    return loaded_identity(name) if (SYS_MODULE / name).exists() else None


def rollback_stock(core_present, consumers, stock):
    """Reconcile sysfs after an ambiguous subprocess result; never remove unknown modules."""
    quiescent()
    candidate = {'srcversion': CANDIDATE_SRC, 'build_id': CANDIDATE_BUILD}
    stock_core = {'srcversion': STOCK_SRC, 'build_id': stock['nvmet']['build_id']}
    core = present_identity('nvmet')
    if core not in (None, candidate, stock_core):
        fail('rollback found an unknown nvmet identity')
    identities = {}
    for name in ('nvmet_rdma', 'nvmet_tcp'):
        actual = present_identity(name)
        expected = stock[name.replace('_', '-')]
        expected = {'srcversion': expected['modinfo']['srcversion'], 'build_id': expected['build_id']}
        if actual is not None and (name not in consumers or actual != expected):
            fail('rollback found an unexpected transport identity')
        identities[name] = actual
    if core == candidate:
        for name in reversed(consumers):
            if identities[name] is not None:
                idle(name)
                run('rmmod', name)
                identities[name] = None
        idle('nvmet')
        run('rmmod', 'nvmet')
        core = None
    if core_present and core is None:
        run('modprobe', 'nvmet')
        core = present_identity('nvmet')
        if core != stock_core:
            fail('restored core identity mismatch')
    if not core_present and core is not None:
        fail('unexpected stock core after rollback')
    for name in consumers:
        if identities[name] is None:
            run('modprobe', name)
        expected = stock[name.replace('_', '-')]
        if present_identity(name) != {'srcversion': expected['modinfo']['srcversion'], 'build_id': expected['build_id']}:
            fail('restored transport identity mismatch')


def install(prefix):
    if os.geteuid() != 0:
        fail('root required')
    prefix = trusted_prefix(prefix)
    proof = validate_prefix(prefix)
    if os.uname().release != RELEASE:
        fail('running kernel release mismatch')
    guest = guest_identity()
    security = policy()
    providers = provider_identities()
    receipt = json.loads((prefix / 'abi-admission.json').read_text())
    stock = stock_identities(receipt)
    current = present_identity('nvmet')
    if current == {'srcversion': CANDIDATE_SRC, 'build_id': CANDIDATE_BUILD}:
        for name in ('nvmet_rdma', 'nvmet_tcp'):
            actual = present_identity(name)
            expected = stock[name.replace('_', '-')]
            if actual is not None and actual != {'srcversion': expected['modinfo']['srcversion'], 'build_id': expected['build_id']}:
                fail('loaded transport is not exact stock')
        return {**proof, 'guest': guest, 'policy': security, 'stock_modules': stock,
                'provider_transports': providers, 'loaded_nvmet': current, 'already_loaded': True}
    quiescent()
    consumers = [name for name in ('nvmet_rdma', 'nvmet_tcp') if idle(name)]
    core_present = idle('nvmet', consumers)
    if consumers and not core_present:
        fail('loaded consumers without core')
    for name in consumers:
        expected = stock[name.replace('_', '-')]
        if loaded_identity(name) != {'srcversion': expected['modinfo']['srcversion'], 'build_id': expected['build_id']}:
            fail('loaded transport is not exact stock')
    if core_present:
        current = loaded_identity('nvmet')
        if current == {'srcversion': CANDIDATE_SRC, 'build_id': CANDIDATE_BUILD}:
            fail('nvmet identity changed during admission')
        expected = stock['nvmet']
        if current != {'srcversion': STOCK_SRC, 'build_id': expected['build_id']}:
            fail('loaded nvmet is neither admitted candidate nor exact stock')
    removed = []
    try:
        for name in consumers:
            run('rmmod', name)
            removed.append(name)
        if core_present:
            run('rmmod', 'nvmet')
        for dep in ('nvme-auth', 'nvme-keyring'):
            run('modprobe', dep)
        run('insmod', str(prefix / 'nvmet.ko'))
        current = loaded_identity('nvmet')
        if current != {'srcversion': CANDIDATE_SRC, 'build_id': CANDIDATE_BUILD}:
            fail('loaded candidate identity mismatch')
        for name in removed:
            run('modprobe', name)
            expected = stock[name.replace('_', '-')]
            if loaded_identity(name) != {'srcversion': expected['modinfo']['srcversion'], 'build_id': expected['build_id']}:
                fail('reloaded transport identity mismatch')
        if provider_identities() != providers:
            fail('provider transport identity changed during installation')
        return {**proof, 'guest': guest, 'policy': security, 'stock_modules': stock,
                'provider_transports': providers, 'loaded_nvmet': current, 'already_loaded': False}
    except Exception as error:
        try:
            rollback_stock(core_present, consumers, stock)
        except Exception as rollback:
            raise RuntimeError(f'installation failed: {error}; rollback failed: {rollback}') from error
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', required=True)
    args = parser.parse_args()
    print(json.dumps(install(args.prefix), sort_keys=True))


if __name__ == '__main__':
    main()
