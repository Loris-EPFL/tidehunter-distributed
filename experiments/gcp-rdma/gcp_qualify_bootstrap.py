#!/usr/bin/env python3
"""Bounded, source-bound bake and reciprocal image qualification startup.

The driver owns instance creation, admission, image capture and deletion. Guest
objects under the qualification prefix are diagnostics, never image receipts.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import textwrap
import tempfile
import time
import traceback

import gcp_bootstrap
import gcp_runtime

# Retained whole-bootstrap maximum: 133.906s (focused qualification).
# Double it, add 180s for log export/status, then round up to one minute.
M3_PAIR_SECONDS = 480
# The child alarm must fire before its parent subprocess timeout.
PAIR_EXIT_SECONDS = 30


def _script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, mode, **fields):
    gcp_runtime._sha(manifest_sha256)
    deadline_setup = (
        "import signal\n"
        "remaining_startup = deadline - time.time()\n"
        "if remaining_startup <= 0: raise TimeoutError('startup expired')\n"
        "if remaining_startup > " + str(21600 if mode == 'fleet' else 900 if mode == 'bake' else 1200) + ": raise ValueError('qualification lifetime exceeds bound')\n"
        "def startup_expired(signum, frame): raise TimeoutError('qualification absolute deadline expired')\n"
        "signal.signal(signal.SIGALRM, startup_expired)\n"
        "signal.setitimer(signal.ITIMER_REAL, remaining_startup)\n")
    fetch = gcp_bootstrap._fetch_program()
    if mode == 'fleet':
        fetch = fetch.replace(
            "runtime, package = wait(base + 'runtime.json'), wait(base + 'package.json')",
            "package = wait(base + 'package.json')\nruntime = {**c, 'package': package}")
    prefix = gcp_bootstrap._preamble(bucket, run_id, source_sha256, deadline_utc, deadline_setup) + fetch
    arguments = dict(bucket=bucket, run_id=run_id, source_sha256=source_sha256,
                     deadline_utc=deadline_utc, manifest_sha256=manifest_sha256,
                     mode=mode, **fields)
    body = (prefix + "\nargs = " + repr(json.dumps(arguments, sort_keys=True)) + "\n"
            "module = target / 'source/experiments/gcp-rdma/gcp_qualify_bootstrap.py'\n"
            "subprocess.run(['mount', '--bind', str(target), str(target)], check=True, timeout=30)\n"
            "subprocess.run(['mount', '-o', 'remount,bind,ro', str(target)], check=True, timeout=30)\n"
            "remaining = int(deadline - time.time())\n"
            "if remaining <= 0: raise TimeoutError('qualification startup expired')\n"
            "unit = 'tidehunter-qualify-' + c['run_id']\n"
            "try:\n"
            "    subprocess.run(['/usr/bin/systemd-run', '--wait', '--collect', '--unit=' + unit, '--property=RuntimeMaxSec=' + str(remaining), '--property=KillMode=control-group', '--property=TimeoutStopSec=2', '--property=LimitMEMLOCK=infinity', '--property=LimitNOFILE=524288', '--setenv=PYTHONDONTWRITEBYTECODE=1', c['guest_python'] if json.loads(args)['mode'] == 'pair' else '/usr/bin/python3.11' if json.loads(args)['mode'] == 'fleet' else '/usr/bin/python3', str(module), '--guest', args, '--package', str(target)], check=True, timeout=remaining + 15)\n"
            "finally:\n"
            "    subprocess.run(['/usr/bin/systemctl', 'stop', unit + '.service'], timeout=10, check=False)\n"
            + ("""
runtime = wait(base + 'runtime.json')
if (runtime.get('run_id') != c['run_id'] or runtime.get('source_sha256') != c['source_sha256'] or
    runtime.get('deadline_utc') != c['deadline_utc'] or runtime.get('bucket') != c['bucket'] or
    runtime.get('package') != package):
    raise ValueError('admitted runtime/package binding mismatch')
config_path = root / 'runtime.json'
with config_path.open('x') as f: json.dump(runtime, f, sort_keys=True)
os.chmod(config_path, 0o600)
subprocess.run(['/usr/bin/systemd-run', '--unit=tidehunter-expire-' + c['run_id'],
                '--on-calendar=' + datetime.datetime.fromtimestamp(deadline, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                '/usr/bin/systemctl', 'stop', 'tidehunter-worker-' + c['run_id'] + '.service'], check=True, timeout=30)
remaining = int(deadline - time.time())
if remaining <= 0: raise TimeoutError('worker startup expired')
worker = target / 'source/experiments/gcp-rdma/gcp_worker.py'
subprocess.run(['/usr/bin/systemd-run', '--unit=tidehunter-worker-' + c['run_id'],
                '--setenv=PATH=' + c['guest_path'],
                '--property=Restart=on-failure', '--property=RestartSec=3', '--property=KillMode=control-group',
                '--property=LimitMEMLOCK=infinity', '--property=RuntimeMaxSec=' + str(remaining),
                '--property=TimeoutStopSec=1', c['guest_python'], str(worker), str(config_path)],
               check=True, timeout=30)
""" if mode == 'fleet' else '') +
            "signal.setitimer(signal.ITIMER_REAL, 0)\n")
    recovery = """
except BaseException:
    import hashlib, json, re, signal, sys, traceback, urllib.parse, urllib.request, uuid
    signal.setitimer(signal.ITIMER_REAL, 0)
    failure = traceback.format_exc()
    print(failure, file=sys.stderr, flush=True)
    try:
        config = json.loads(BOOTSTRAP_CONFIG)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def meta(path):
            req = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/' + path, headers={'Metadata-Flavor': 'Google'})
            with opener.open(req, timeout=5) as response:
                if response.headers.get('Metadata-Flavor') != 'Google': raise ValueError('untrusted metadata')
                return response.read(16384).decode().strip()
        name = meta('instance/name')
        if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', name): raise ValueError('invalid instance')
        token = json.loads(meta('instance/service-accounts/default/token'))['access_token']
        directory = 'bake' if config['mode'] == 'bake' else name
        key = 'runs/' + config['run_id'] + '/qualification/' + directory + '/startup-error.json'
        url = 'https://storage.googleapis.com/upload/storage/v1/b/' + urllib.parse.quote(config['bucket'], safe='') + '/o?' + urllib.parse.urlencode({'uploadType':'multipart', 'ifGenerationMatch':'0'})
        data = json.dumps({'complete':False, 'source_sha256':config['source_sha256'], 'error':failure[-32000:]}).encode()
        digest = hashlib.sha256(data).hexdigest()
        boundary = 'qualification-' + uuid.uuid4().hex
        part = json.dumps({'name':key, 'metadata':{'sha256':digest}}).encode()
        body = (b'--' + boundary.encode() + b'\\r\\nContent-Type: application/json; charset=UTF-8\\r\\n\\r\\n' + part +
                b'\\r\\n--' + boundary.encode() + b'\\r\\nContent-Type: application/json\\r\\n\\r\\n' + data +
                b'\\r\\n--' + boundary.encode() + b'--\\r\\n')
        req = urllib.request.Request(url, data=body, headers={'Authorization':'Bearer ' + token, 'Content-Type':'multipart/related; boundary=' + boundary}, method='POST')
        with opener.open(req, timeout=15) as response: receipt = json.loads(response.read(16384))
        if receipt.get('metadata', {}).get('sha256') != digest or not receipt.get('generation') or int(receipt.get('size', -1)) != len(data):
            raise ValueError('startup error upload receipt mismatch')
    except Exception:
        traceback.print_exc()
    raise
"""
    python = '/usr/bin/python3.11' if mode == 'fleet' else '/usr/bin/python3' if mode == 'bake' else gcp_runtime.GUEST_PYTHON
    header = ("#!/bin/bash\nset -euo pipefail\nexport PATH=" + shlex.quote(gcp_runtime.GUEST_PATH) +
              "\nexec " + shlex.quote(python) + " - <<'TIDEHUNTER_BOOTSTRAP'\n")
    return (header + "BOOTSTRAP_CONFIG = " + repr(json.dumps(arguments, sort_keys=True)) +
            "\ntry:\n" + textwrap.indent(body, '    ') + recovery + "TIDEHUNTER_BOOTSTRAP\n")


def bake_startup_script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, preparation_sha256):
    gcp_runtime._sha(preparation_sha256)
    return _script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, 'bake',
                   preparation_sha256=preparation_sha256)


def pair_startup_script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, peer_instance, peer_ip, rdma_device, *, peer_instance_id=None, machine_type=None, live_transports=(), live_spdk=False):
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', peer_instance):
        raise ValueError('invalid peer instance')
    address = ipaddress.IPv4Address(peer_ip)
    if address.is_loopback or address.is_multicast or address.is_unspecified:
        raise ValueError('invalid peer address')
    if not re.fullmatch(r'[A-Za-z0-9_]+', rdma_device):
        raise ValueError('invalid RDMA device')
    if (not isinstance(live_transports, (tuple, list)) or len(set(live_transports)) != len(live_transports)
            or not set(live_transports) <= {'tcp', 'rdma'}):
        raise ValueError('invalid live transport selection')
    if type(live_spdk) is not bool or live_spdk and 'rdma' not in live_transports:
        raise ValueError('live SPDK qualification requires explicitly selected RDMA')
    if live_transports and (not re.fullmatch(r'[1-9][0-9]*', str(peer_instance_id))
                            or machine_type not in gcp_runtime.LOCAL_SSD_COUNTS):
        raise ValueError('live pair requires exact peer ID and scratch machine type')
    return _script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, 'pair',
                   peer_instance=peer_instance, peer_ip=str(address), rdma_device=rdma_device,
                   peer_instance_id=str(peer_instance_id) if peer_instance_id is not None else None,
                   machine_type=machine_type, live_transports=list(live_transports), live_spdk=live_spdk)


def fleet_startup_script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256,
                         preparation_sha256, *, members, rdma_device, machine_type,
                         live_transports=(), live_spdk=False, qualification_profile='full', selection=None):
    """Prepare and qualify the admitted fleet before starting its ordinary worker."""
    gcp_runtime._sha(preparation_sha256)
    if qualification_profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if not isinstance(members, (list, tuple)) or len(members) - 1 not in gcp_runtime.owner_host_counts():
        raise ValueError('fleet must have one or five owner hosts')
    if selection in ('nvmeof_p9_write_engine', 'nvmeof_p13_write_engine') and len(members) != 3:
        raise ValueError('write-engine fleet requires one coordinator and two owner hosts')
    if qualification_profile == 'focused' and (live_transports or live_spdk):
        if (not gcp_runtime.live_mode({'mode': 'focused', 'selection': selection})
                or list(live_transports) != ['rdma'] or live_spdk
                or (len(members) != 2 and selection not in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5', 'nvmeof_p9_small_put', 'nvmeof_p9_write_engine', 'nvmeof_p13_write_engine'))):
            raise ValueError('focused qualification forbids unreviewed live and Valkey probes')
    normalized = []
    for member in members:
        if not isinstance(member, dict):
            raise ValueError('invalid fleet member')
        name, instance_id = member.get('name'), str(member.get('id'))
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', name):
            raise ValueError('invalid fleet member name')
        if not re.fullmatch(r'[1-9][0-9]*', instance_id):
            raise ValueError('invalid fleet member ID')
        address = ipaddress.IPv4Address(member.get('rdma_ip'))
        if address.is_loopback or address.is_multicast or address.is_unspecified:
            raise ValueError('invalid fleet RDMA address')
        normalized.append({'name': name, 'id': instance_id, 'rdma_ip': str(address)})
    for key in ('name', 'id', 'rdma_ip'):
        if len({member[key] for member in normalized}) != len(normalized):
            raise ValueError('fleet members must have distinct identities and RDMA addresses')
    pair_startup_script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256,
                        normalized[1]['name'], normalized[1]['rdma_ip'], rdma_device,
                        peer_instance_id=normalized[1]['id'], machine_type=machine_type,
                        live_transports=live_transports, live_spdk=live_spdk)
    extra = {'qualification_profile': 'focused', 'selection': selection} if qualification_profile == 'focused' else {}
    return _script(bucket, run_id, source_sha256, deadline_utc, manifest_sha256, 'fleet',
                   preparation_sha256=preparation_sha256, members=normalized,
                   rdma_device=rdma_device, machine_type=machine_type,
                   live_transports=list(live_transports), live_spdk=live_spdk, **extra)


def _remaining(deadline, maximum):
    result = min(maximum, int(deadline - time.time()))
    if result < 1:
        raise TimeoutError('qualification deadline expired')
    return result


def _call(argv, deadline, maximum=30):
    from gcp_image_qualify import command_failure
    try:
        return subprocess.check_output([str(a) for a in argv], stderr=subprocess.STDOUT,
                                       text=True, timeout=_remaining(deadline, maximum)).strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(command_failure(exc)) from exc


def _owned_cm_listener(records, pid, device, port):
    if not isinstance(records, list):
        raise ValueError('RDMA CM resource response must be a JSON array')
    for item in records:
        if not isinstance(item, dict):
            raise ValueError('malformed RDMA CM resource')
        if (item.get('pid') == pid and item.get('ifname') == device and
                item.get('state') == 'LISTEN' and item.get('ps') == 'TCP'):
            source = item.get('src-addr')
            if not isinstance(source, str) or ':' not in source:
                raise ValueError('RDMA CM listener lacks source address')
            host, _, raw_port = source.rpartition(':')
            try:
                ipaddress.ip_address(host.strip('[]'))
                observed_port = int(raw_port)
            except ValueError as exc:
                raise ValueError('invalid RDMA CM listener source') from exc
            if observed_port == port:
                return True
    return False


def _wait_servers(processes, cli, device, deadline, ports=(18515, 18516)):
    last_error = ''
    try:
        while True:
            _remaining(deadline, 2)
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('qualification server exited before peer admission')
            raw = _call(['rdma', '-j', 'resource', 'show', 'cm_id'], deadline, 5)
            if len(raw) > 1024 * 1024:
                raise ValueError('RDMA CM resource output exceeds bound')
            records = json.loads(raw)
            listeners = (_owned_cm_listener(records, processes[0].pid, device, ports[0]) and
                         _owned_cm_listener(records, processes[1].pid, device, ports[1]))
            if listeners:
                if cli is None:
                    return
                try:
                    answer = _call([cli, '-h', '127.0.0.1', '-p', '16380', 'PING'], deadline, 2)
                    if answer == 'PONG':
                        return
                    last_error = 'unexpected Valkey PING response: ' + answer[-32000:]
                except RuntimeError as exc:
                    last_error = str(exc)[-32000:]
            time.sleep(min(0.2, _remaining(deadline, 1)))
    except TimeoutError as exc:
        raise TimeoutError('qualification server readiness expired; ' + last_error) from exc


def _stop_processes(processes):
    import signal
    for process in processes:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    for process in processes:
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def _put(store, key, value):
    store.put(key, json.dumps(value, sort_keys=True).encode(), content_type='application/json', if_generation_match=0)


def _wait(store, key, deadline, identity):
    import gcp_api
    while time.time() < deadline:
        try:
            value = json.loads(store.get(key, max_bytes=4 * 1024 * 1024))
            if any(value.get(k) != v for k, v in identity.items()):
                raise ValueError('peer barrier identity mismatch')
            return value
        except gcp_api.ApiError as exc:
            if exc.status != 404:
                raise
            time.sleep(min(2, _remaining(deadline, 2)))
    raise TimeoutError('peer barrier expired')


def _bake(package, cfg, deadline, store):
    python = Path(gcp_runtime.GUEST_PYTHON)
    if not python.exists():
        _call(['/usr/bin/python3.11', '-m', 'venv', '--system-site-packages', '/opt/tidehunter-python'], deadline, 120)
    abi = _call([python, '-c', 'import sys; print(\"cp%d%d\" % sys.version_info[:2])'], deadline)
    raw = store.get('runs/' + cfg['run_id'] + '/preparation.json', max_bytes=65536)
    if hashlib.sha256(raw).hexdigest() != cfg['preparation_sha256']:
        raise ValueError('preparation manifest checksum mismatch')
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or set(manifest) != {'wheels'} or not isinstance(manifest['wheels'], list) or not 1 <= len(manifest['wheels']) <= 8:
        raise ValueError('invalid preparation manifest')
    candidates = []
    for item in manifest['wheels']:
        if not isinstance(item, dict) or set(item) != {'path', 'sha256', 'object_key', 'generation'}:
            raise ValueError('invalid wheel descriptor')
        name = item['path']
        if not isinstance(name, str) or not re.fullmatch(r'(?i:pyyaml)-6\.0\.2-cp3[0-9]+-cp3[0-9]+-manylinux[0-9_a-z.]+_x86_64\.whl', name):
            raise ValueError('invalid wheel filename')
        gcp_runtime._sha(item['sha256'])
        if item['object_key'] != 'runs/' + cfg['run_id'] + '/preparation/' + name or not re.fullmatch(r'[0-9]+', str(item['generation'])):
            raise ValueError('wheel object outside admitted preparation')
        if name.lower().startswith('pyyaml-6.0.2-' + abi + '-' + abi + '-'):
            candidates.append(item)
    if len(candidates) != 1:
        raise ValueError('exactly one wheel for canonical Python ABI required')
    item = candidates[0]
    wheel = store.get(item['object_key'], generation=item['generation'], max_bytes=8 * 1024 * 1024)
    if hashlib.sha256(wheel).hexdigest() != item['sha256']:
        raise ValueError('wheel checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='tidehunter-wheel-') as directory:
        path = Path(directory) / item['path']
        path.write_bytes(wheel)
        _call([python, '-m', 'pip', 'install', '--no-index', '--no-deps', str(path)], deadline, 120)
    probe = 'import sys,yaml; assert sys.version_info >= (3,11); assert yaml.__version__ == \"6.0.2\"; assert yaml.safe_load(\"x: 1\") == {\"x\":1}; print(sys.version)'
    version = _call([python, '-c', probe], deadline)
    dropin = Path('/etc/systemd/system.conf.d/99-tidehunter-nofile.conf')
    dropin.parent.mkdir(parents=True, exist_ok=True)
    dropin.write_text('[Manager]\nDefaultLimitNOFILE=524288:524288\n')
    helper = Path(__file__).resolve()
    output = _call([python, helper, '--bake-proof', str(package), '--run-id', cfg['run_id']],
                   deadline, 90)
    proof = json.loads(output)
    return {'kind': 'bake-software-diagnostic', 'python': version, 'unit': proof,
            'source_sha256': cfg['source_sha256'], 'manifest_sha256': cfg['manifest_sha256'],
            'preparation_sha256': cfg['preparation_sha256'], 'wheel_sha256': item['sha256'], 'python_abi': abi}


def bake_proof(package, run_id):
    root = package / 'source/experiments/kv-comparison'
    sys.path.insert(0, str(root))
    data_root = Path('/mnt/tidehunter-qualification')
    data_root.mkdir(parents=True, exist_ok=True)
    unit_config = {'prefix': run_id, 'repository_root': str(package / 'source'),
                   'platform': {'name': 'gcp'}, 'fdb_library_dir': str(package / 'runtime/lib'),
                   'fleet': {'client_cpus': 1, 'server_cpus': 1,
                   'client_memory_mib': 256, 'server_memory_mib': 256, 'data_root': str(data_root)}}
    return gcp_runtime.admit_guest_unit(unit_config, -1)



def _pair(package, cfg, deadline, store, own, identity):
    import gcp_api
    import gcp_image_qualify
    profile = cfg.get('qualification_profile', 'full')
    if profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if profile == 'focused' and (cfg.get('live_transports') or cfg.get('live_spdk')
                                 or cfg.get('valkey_qualification')):
        if (not gcp_runtime.live_mode({'mode': 'focused', 'selection': cfg.get('selection')})
                or cfg.get('live_transports') != ['rdma'] or cfg.get('live_spdk')
                or cfg.get('valkey_qualification')):
            raise ValueError('focused qualification forbids unreviewed live and Valkey probes')
    sys.path.insert(0, str(package / 'source/experiments/kv-comparison'))
    import cloud_package
    proof = cloud_package.verify_directory(package, runtime=True, provider='gcp')
    if proof['manifest_sha256'] != cfg['manifest_sha256']:
        raise ValueError('sealed manifest mismatch')
    focused_valkey = proof.get('build_profile') == 'focused_valkey'
    if profile == 'focused' and (
            focused_valkey != (cfg.get('selection') == 'valkey_rdma_reads')):
        raise ValueError('focused Valkey selection/package profile mismatch')
    api = gcp_api.GoogleApi(gcp_image_qualify.metadata('project/project-id'), token_provider=gcp_api.metadata_token)
    zone = gcp_image_qualify.metadata('instance/zone').rsplit('/', 1)[-1]
    info = api.request('GET', f'zones/{zone}/instances/{own}')
    addresses = [x.get('networkIP') for x in info.get('networkInterfaces', []) if x.get('nicType') == 'IRDMA']
    if len(addresses) != 1:
        raise ValueError('exactly one local IRDMA address required')
    address = str(ipaddress.IPv4Address(addresses[0]))
    prefix = 'runs/' + cfg['run_id'] + '/qualification/'
    processes = []
    logs = []
    try:
        for op, port in (('read', '18515'), ('send', '18516')):
            log = open('/tmp/tidehunter-qualify-' + op + '.log', 'w+')
            logs.append(log)
            processes.append(subprocess.Popen(['ib_' + op + '_lat', '-R', '-d', cfg['rdma_device'], '-p', port, '-s', '1024', '-n', '1000'], stdout=log, stderr=subprocess.STDOUT, start_new_session=True))
        cli = None
        if profile == 'full' or focused_valkey:
            log = open('/tmp/tidehunter-qualify-valkey.log', 'w+')
            logs.append(log)
            processes.append(subprocess.Popen([str(package / 'runtime/bin/valkey-server'), '--bind', '127.0.0.1', '--port', '16380', '--protected-mode', 'no', '--rdma-bind', address, '--rdma-port', '6380', '--save', '', '--appendonly', 'no'], stdout=log, stderr=subprocess.STDOUT, start_new_session=True))
            cli = package / 'runtime/bin/valkey-cli'
        _wait_servers(processes, cli, cfg['rdma_device'], deadline)
        _put(store, prefix + own + '/ready.json', identity)
        _wait(store, prefix + cfg['peer_instance'] + '/ready.json', deadline, identity)
        if profile == 'focused':
            result = gcp_image_qualify.guest(package, cfg['peer_ip'], cfg['peer_instance'],
                                             cfg['rdma_device'], cfg['manifest_sha256'],
                                             qualification_profile='focused')
        else:
            result = gcp_image_qualify.guest(package, cfg['peer_ip'], cfg['peer_instance'],
                                             cfg['rdma_device'], cfg['manifest_sha256'])
        if cfg.get('live_transports'):
            import gcp_live_qualification
            own_id = gcp_image_qualify.metadata('instance/id')
            if not re.fullmatch(r'[1-9][0-9]*', own_id):
                raise ValueError('invalid local numeric instance identity')
            result['live_nvmeof'] = {}
            for transport in cfg['live_transports']:
                result['live_nvmeof'][transport] = {}
                for tier in ('native', 'synchronous'):
                    live = gcp_live_qualification.run(package, cfg, deadline, store, own, own_id, identity, api, info, zone, transport, tier)
                    if live is not None:
                        result['live_nvmeof'][transport][tier] = live
            if cfg.get('live_spdk'):
                result['live_spdk_rdma'] = {}
                for tier in ('native', 'synchronous'):
                    live = gcp_live_qualification.run(package, {**cfg, 'qualification_target': 'spdk'}, deadline, store, own, own_id, identity, api, info, zone, 'rdma', tier)
                    if live is not None:
                        result['live_spdk_rdma'][tier] = live
        _put(store, prefix + own + '/diagnostic.json', result)
        _put(store, prefix + own + '/done.json', identity)
        _wait(store, prefix + cfg['peer_instance'] + '/done.json', deadline, identity)
        return result
    finally:
        active_failure = sys.exc_info()[0] is not None
        cleanup_errors = []
        try:
            _stop_processes(processes)
        except Exception:
            cleanup_errors.append(traceback.format_exc())
        for log in logs:
            try:
                # Admit one bounded credential/POST operation and retain 50s for final status.
                if deadline - time.time() < 115:
                    raise TimeoutError('insufficient log-export and final-status budget')
                log.flush()
                log.seek(0)
                data = log.read(1024 * 1024).encode()
                if deadline - time.time() < 115:
                    raise TimeoutError('insufficient log-export and final-status budget')
                store.put(prefix + own + '/' + Path(log.name).name, data, if_generation_match=0)
            except Exception:
                cleanup_errors.append(traceback.format_exc())
            finally:
                try:
                    log.close()
                except Exception:
                    cleanup_errors.append(traceback.format_exc())
        if cleanup_errors:
            message = '\n'.join(cleanup_errors)[-32000:]
            print('qualification cleanup failures:\n' + message, file=sys.stderr, flush=True)
            if not active_failure:
                raise RuntimeError('qualification cleanup failures:\n' + message)


def _fleet_phase(deadline, seconds, operation):
    import signal
    phase_deadline = min(deadline, time.time() + seconds)
    remaining = _remaining(phase_deadline, seconds)
    previous = signal.getsignal(signal.SIGALRM)
    def expired(signum, frame):
        raise TimeoutError('fleet qualification phase expired')
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, remaining)
    try:
        return operation(phase_deadline)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _fleet_cleanup(run_id):
    import shutil
    gcp_runtime._run(run_id)
    root = Path('/mnt/tidehunter-kv-comparison') / run_id
    if root.is_symlink():
        raise ValueError('qualification data root is a symlink')
    for target in ('live-tcp', 'live-rdma', 'live-rdma-spdk'):
        for tier in ('native', 'synchronous'):
            path = root / f'{target}-{tier}'
            if path.is_symlink():
                raise ValueError('qualification data directory is a symlink')
            if path.exists():
                shutil.rmtree(path)


def _fleet_pair(package, cfg, deadline):
    seconds = M3_PAIR_SECONDS if cfg.get("selection") in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5', 'nvmeof_p9_small_put', 'nvmeof_p9_write_engine', 'nvmeof_p13_write_engine') else 1200
    pair_cfg = {**cfg, 'mode': 'fleet-pair',
                'phase_deadline': min(deadline - PAIR_EXIT_SECONDS, time.time() + seconds)}
    module = package / 'source/experiments/gcp-rdma/gcp_qualify_bootstrap.py'
    _call([gcp_runtime.GUEST_PYTHON, module, '--guest', json.dumps(pair_cfg, sort_keys=True),
           '--package', package], deadline, seconds + PAIR_EXIT_SECONDS)


def _fleet_nvmet_fix(package, cfg, own, own_id, metadata):
    """Admit this exact owned guest and sealed candidate before loading it."""
    manifest = json.loads((package / 'manifest.json').read_text())
    if 'nvmet_fix' not in manifest:
        return None
    import gcp_api
    sys.path.insert(0, str(package / 'source/experiments/kv-comparison'))
    import cloud_package
    admitted = cloud_package.verify_directory(package)
    if (admitted.get('manifest_sha256') != cfg['manifest_sha256']
            or admitted.get('source_sha256') != cfg['source_sha256']
            or admitted.get('nvmet_fix') != manifest['nvmet_fix']
            or admitted.get('nvmet_fix') is None):
        raise ValueError('nvmet fix package identity mismatch')
    zone = metadata('instance/zone').rsplit('/', 1)[-1]
    api = gcp_api.GoogleApi(metadata('project/project-id'), token_provider=gcp_api.metadata_token)
    instance = api.request('GET', f'beta/zones/{zone}/instances/{own}')
    meta = {item['key']: item.get('value') for item in instance.get('metadata', {}).get('items', [])}
    schedule = instance.get('scheduling', {})
    if (instance.get('name') != own or str(instance.get('id')) != own_id
            or instance.get('status') != 'RUNNING'
            or instance.get('labels', {}).get('bench-run') != cfg['run_id']
            or instance.get('labels', {}).get('bench-source') != cfg['source_sha256'][:63]
            or meta.get('bench-run') != cfg['run_id'] or meta.get('bench-source') != cfg['source_sha256']
            or gcp_runtime._deadline(meta.get('bench-expiry')) != gcp_runtime._deadline(cfg['deadline_utc'])
            or schedule.get('instanceTerminationAction') != 'DELETE'
            or gcp_runtime._deadline(schedule.get('terminationTime')) != gcp_runtime._deadline(cfg['deadline_utc'])
            or instance.get('machineType', '').split('/')[-1] != cfg['machine_type']):
        raise ValueError('nvmet fix guest ownership admission failed')
    loaded = cloud_package.nvmet_fix_module(package / 'source').install(package / 'nvmet-fix')
    candidate = admitted['nvmet_fix']
    if (any(loaded.get(key) != value for key, value in candidate.items())
            or loaded.get('guest', {}).get('instance_name') != own
            or loaded.get('guest', {}).get('instance_id') != own_id
            or loaded.get('guest', {}).get('run_id') != cfg['run_id']
            or loaded.get('loaded_nvmet') != {'srcversion': candidate['candidate_srcversion'],
                                              'build_id': candidate['candidate_build_id']}):
        raise ValueError('loaded nvmet proof differs from admitted candidate or guest')
    return {'candidate': admitted['nvmet_fix'], 'loaded': loaded}



def _fleet_guest(cfg, package, deadline, store, own, identity, metadata):
    own_id = metadata('instance/id')
    matches = [i for i, member in enumerate(cfg['members'])
               if member['name'] == own and member['id'] == own_id]
    if len(matches) != 1:
        raise ValueError('fleet guest identity mismatch')
    index = matches[0]
    host_identity = {**identity, 'instance_name': own, 'instance_id': own_id}
    prefix = 'runs/' + cfg['run_id'] + '/qualification/' + own + '/'
    nvmet_fix = _fleet_phase(deadline, 300,
                             lambda end: _fleet_nvmet_fix(package, cfg, own, own_id, metadata))
    proof = _fleet_phase(deadline, 900, lambda end: _bake(package, cfg, end, store))
    if nvmet_fix is not None:
        proof['nvmet_fix'] = nvmet_fix
    _put(store, prefix + 'preparation.json', {**host_identity, 'complete': True, 'proof': proof})
    if index < 2:
        peer = cfg["members"][1 - index]
        pair_cfg = {**cfg, "peer_instance": peer["name"], "peer_instance_id": peer["id"],
                    "peer_ip": peer["rdma_ip"], "own_instance": own, "own_instance_id": own_id}
        _fleet_pair(package, pair_cfg, deadline)
    _fleet_cleanup(cfg['run_id'])
    _put(store, prefix + 'status.json', {**host_identity, 'complete': True,
                                         'cleanup_complete': True, 'pair': index < 2})
    return proof


def guest(cfg, package):
    import gcp_api
    from gcp_image_prepare import metadata
    deadline = gcp_runtime._deadline(cfg['deadline_utc'])
    own = metadata('instance/name')
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', own):
        raise ValueError('invalid instance identity')
    project = metadata('project/project-id')
    if project != 'mysten-research':
        raise ValueError('qualification project mismatch')
    store = gcp_api.GoogleApi(project, token_provider=gcp_api.metadata_token).objects(cfg['bucket'])
    prefix = 'runs/' + cfg['run_id'] + '/qualification/' + ('bake' if cfg['mode'] == 'bake' else own) + '/'
    identity = {k: cfg[k] for k in ('run_id', 'source_sha256', 'manifest_sha256', 'deadline_utc')}
    try:
        if hashlib.sha256((package / 'manifest.json').read_bytes()).hexdigest() != cfg['manifest_sha256']:
            raise ValueError('sealed package manifest checksum mismatch')
        if cfg['mode'] == 'fleet':
            return _fleet_guest(cfg, package, deadline, store, own, identity, metadata)
        if cfg['mode'] == 'fleet-pair':
            if own != cfg['own_instance'] or metadata('instance/id') != cfg['own_instance_id']:
                raise ValueError('fleet pair guest identity mismatch')
            phase_deadline = min(deadline, cfg['phase_deadline'])
            return _fleet_phase(phase_deadline, 1200,
                                lambda end: _pair(package, cfg, end, store, own, identity))
        if cfg['mode'] == 'bake':
            result = _bake(package, cfg, deadline, store)
            _put(store, prefix + 'diagnostic.json', result)
        elif cfg['mode'] == 'pair':
            result = _pair(package, cfg, deadline, store, own, identity)
        else:
            raise ValueError('unknown qualification mode')
        _put(store, prefix + 'status.json', {**identity, 'complete': True})
        return result
    except BaseException:
        failure = traceback.format_exc()
        print(failure, file=sys.stderr, flush=True)
        if cfg['mode'] == 'fleet-pair':
            raise
        try:
            _put(store, prefix + 'status.json', {**identity, 'complete': False, 'error': failure[-32000:]})
        except Exception:
            traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--guest')
    parser.add_argument('--package', type=Path)
    parser.add_argument('--bake-proof', type=Path)
    parser.add_argument('--run-id')
    args = parser.parse_args()
    if args.bake_proof is not None:
        gcp_runtime._run(args.run_id)
        print(json.dumps(bake_proof(args.bake_proof.resolve(strict=True), args.run_id), sort_keys=True))
    elif args.guest and args.package:
        guest(json.loads(args.guest), args.package.resolve(strict=True))
    else:
        parser.error('guest and package, or bake-proof and run-id, are required')


if __name__ == '__main__':
    main()
