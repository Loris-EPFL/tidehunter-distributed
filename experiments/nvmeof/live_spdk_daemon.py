#!/usr/bin/env python3
"""Exact-owned, deadline-bounded SPDK daemon for live owner WALs.

This helper owns only the daemon, never namespaces or backing files. An owner
must prove its own namespace drain and the initiator must be quiescent before
calling stop. Failed or ambiguous operations retain the journal and daemon.
"""
import argparse
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import struct
import sys
import time

import campaign_targets as campaign
import manage
import spdk_artifacts
import spdk_targets as spdk


def _read_private_json(directory_fd, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077
                or info.st_nlink != 1 or info.st_size > 1024 * 1024):
            raise ValueError('untrusted live SPDK state or exit receipt')
        return json.load(stream)


def record_exit(state_path, unit_name):
    """Record systemd's terminal result without taking the caller's state lock."""
    state_path = campaign.absolute(state_path)
    directory_fd = manage.trusted_directory(state_path)
    try:
        info = os.fstat(directory_fd)
        if info.st_mode & 0o077:
            raise ValueError('live SPDK state directory is not private')
        state = _read_private_json(directory_fd, 'state.json')
        if (state.get('version') != 1 or state.get('kind') != 'live-spdk-daemon'
                or state.get('unit_name') != unit_name
                or state.get('state_directory_identity') != [info.st_dev, info.st_ino]
                or any(type(state.get(key)) is not int or state[key] <= 0
                       for key in ('pid', 'process_start_ticks'))):
            raise ValueError('live SPDK exit recorder identity is not bound')
        receipt = {'version': 1, 'unit_name': unit_name, 'pid': state['pid'],
                   'process_start_ticks': state['process_start_ticks'],
                   'service_result': os.environ.get('SERVICE_RESULT'),
                   'exit_code': os.environ.get('EXIT_CODE'),
                   'exit_status': os.environ.get('EXIT_STATUS')}
        fd = os.open('exit.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory_fd)
        with os.fdopen(fd, 'w') as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(directory_fd)
        return receipt
    finally:
        os.close(directory_fd)


def admit_exit(state_path, state):
    directory_fd = manage.trusted_directory(campaign.absolute(str(state_path)))
    try:
        info = os.fstat(directory_fd)
        if info.st_mode & 0o077 or state.get('state_directory_identity') != [info.st_dev, info.st_ino]:
            raise ValueError('live SPDK state directory identity changed')
        receipt = _read_private_json(directory_fd, 'exit.json')
    finally:
        os.close(directory_fd)
    if (receipt.get('version') != 1
            or any(receipt.get(key) != state.get(key) for key in
                   ('unit_name', 'pid', 'process_start_ticks'))
            or receipt.get('service_result') != 'success'
            or receipt.get('exit_code') != 'exited'
            or receipt.get('exit_status') != '0'):
        raise ValueError('live SPDK daemon did not prove graceful systemd exit: ' + repr(receipt))
    return receipt


def verify(state):
    status = spdk.unit_status(state)
    if status.get('ActiveState') != 'active' or status.get('MainPID') != str(state['pid']):
        raise ValueError('live SPDK service identity or state changed')
    spdk.verify_process(state)
    with socket.socket(socket.AF_UNIX) as channel:
        channel.settimeout(5)
        channel.connect(state['rpc_socket'])
        pid, uid, _ = struct.unpack('3i', channel.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if pid != state['pid'] or uid != 0:
            raise ValueError('live SPDK RPC peer is not the owned root daemon')
    return status


def start(state_path, rpc_socket, executable, deadline):
    state_path = campaign.absolute(state_path)
    rpc_socket = campaign.absolute(rpc_socket)
    executable = campaign.absolute(executable)
    if executable.name != 'nvmf_tgt' or executable.parent.name != 'bin':
        raise ValueError('expected pinned SPDK executable')
    prefix = executable.parent.parent
    artifact = spdk_artifacts.admit_prefix(prefix)
    if not math.isfinite(deadline) or not 30 <= deadline - time.time() <= 18000:
        raise ValueError('live SPDK deadline must be 30..18000 seconds away')
    runtime = rpc_socket.parent
    if artifact['target_scope'] != 'file_aio_direct_live_and_sealed':
        raise ValueError('live SPDK requires the reviewed writable artifact')
    if runtime.parent != Path('/run/tidehunter-spdk-live') or rpc_socket.name != 'rpc.sock':
        raise ValueError('invalid private live SPDK socket path')
    token = runtime.name
    if len(token) != 32 or any(c not in '0123456789abcdef' for c in token):
        raise ValueError('invalid live SPDK runtime token')
    binary_info = executable.stat()
    state = {'version': 1, 'kind': 'live-spdk-daemon', 'phase': 'creating',
             'unit_name': 'tidehunter-live-spdk-' + token,
             'unit_description': 'Tidehunter live SPDK ' + token,
             'rpc_socket': str(rpc_socket), 'deadline_epoch': deadline,
             'executable': str(executable), 'executable_device': binary_info.st_dev,
             'executable_inode': binary_info.st_ino,
             'binary_sha256': artifact['files']['bin/nvmf_tgt']['sha256']}
    directory = campaign.new_campaign_state(state_path, state)
    try:
        state_info = os.fstat(directory.fd)
        state['state_directory_identity'] = [state_info.st_dev, state_info.st_ino]
        manage.save(directory, state)
        if spdk.unit_status(state).get('LoadState') != 'not-found':
            raise ValueError('live SPDK unit already exists')
        runtime.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = manage.trusted_directory(runtime.parent)
        try:
            if os.fstat(fd).st_mode & 0o077:
                raise ValueError('live SPDK runtime parent is not private')
        finally:
            os.close(fd)
        runtime.mkdir(mode=0o700)
        info = runtime.stat()
        state['runtime_identity'] = [info.st_dev, info.st_ino]
        manage.save(directory, state)
        cores = sorted(os.sched_getaffinity(0))[:2]
        if not cores:
            raise ValueError('live SPDK requires an available CPU')
        # Reserve the entire bounded systemd-run acknowledgement interval:
        # RuntimeMaxSec starts when the unit launches, not at this calculation.
        seconds = math.floor(deadline - time.time()) - 31
        if seconds < 1:
            raise TimeoutError('live SPDK deadline expired during setup')
        state['unit_start_attempted'] = True
        manage.save(directory, state)
        recorder_paths = (str(Path(sys.executable).resolve()), str(Path(__file__).resolve()), str(state_path))
        if any(not re.fullmatch(r'/[A-Za-z0-9_./-]+', path) for path in recorder_paths):
            raise ValueError('live SPDK exit recorder requires safe absolute paths')
        exit_command = ' '.join([*recorder_paths[:2], 'record-exit', '--state',
                                 recorder_paths[2], '--unit', state['unit_name']])
        spdk.command(['systemd-run', '--unit=' + state['unit_name'],
            '--description=' + state['unit_description'], '--collect', '--service-type=exec',
            '--property=RuntimeMaxSec=' + str(seconds), '--property=Restart=no',
            '--property=KillMode=control-group', '--property=TimeoutStopSec=15', '--property=UMask=0077',
            '--property=ExecStopPost=' + exit_command,
            '--property=MemoryMax=2G', '--property=TasksMax=128', '--property=LimitMEMLOCK=infinity',
            '--setenv=LD_LIBRARY_PATH=' + str(prefix / 'lib'), '--', executable,
            '--no-huge', '--no-pci', '-s', '1024', '-m', str(cores).replace(' ', ''),
            '-r', rpc_socket, '--num-trace-entries', '0', '--wait-for-rpc'])
        state['unit_started'] = True
        manage.save(directory, state)
        until = min(time.monotonic() + 20, time.monotonic() + max(0, deadline - time.time()))
        while time.monotonic() < until:
            status = spdk.unit_status(state)
            if status.get('ActiveState') == 'failed':
                raise RuntimeError('live SPDK service failed: ' + str(status))
            pid = int(status.get('MainPID', '0'))
            if pid > 0 and rpc_socket.exists():
                observed = spdk.process_identity(pid)
                if any(observed[k] != state[k] for k in ('executable_device', 'executable_inode')):
                    raise ValueError('live SPDK executable changed')
                state.update(observed)
                sock = rpc_socket.lstat()
                if not stat.S_ISSOCK(sock.st_mode) or sock.st_uid != 0:
                    raise ValueError('invalid live SPDK socket')
                rpc_socket.chmod(0o600)
                state.update(rpc_device=sock.st_dev, rpc_inode=sock.st_ino)
                manage.save(directory, state)
                verify(state)
                state.update(spdk.capture_rpc_lock(state))
                manage.save(directory, state)
                spdk.rpc(rpc_socket, 'framework_start_init')
                verify(state)
                state['phase'] = 'ready'
                manage.save(directory, state)
                return state
            time.sleep(.05)
        raise TimeoutError('live SPDK readiness expired')
    except BaseException as error:
        state['phase'] = 'quarantined'
        state['error'] = type(error).__name__ + ': ' + str(error)
        if state.get('unit_start_attempted'):
            spdk.capture_journal(state)
        try:
            manage.save(directory, state)
        except Exception as journal_error:
            error.add_note('live SPDK failure journal unavailable: ' + repr(journal_error))
        raise
    finally:
        directory.close()


def backing_fds(pid, bdevs):
    """Report actual AIO inode and open flags; a disappearing file is unavailable."""
    if not isinstance(bdevs, list) or len(bdevs) > 256:
        raise ValueError('invalid live SPDK bdev inventory')
    fd_root = Path('/proc') / str(pid) / 'fd'
    descriptors = list(fd_root.iterdir())
    if len(descriptors) > 4096:
        raise ValueError('live SPDK descriptor inspection exceeds bound')
    result = []
    for bdev in bdevs:
        aio = bdev.get('driver_specific', {}).get('aio') if isinstance(bdev, dict) else None
        if not isinstance(aio, dict) or not isinstance(aio.get('filename'), str):
            continue
        row = {'name': bdev.get('name'), 'filename': aio['filename'], 'available': False}
        try:
            info = Path(aio['filename']).stat()
            row.update(device=info.st_dev, inode=info.st_ino, mode=info.st_mode, bytes=info.st_size, descriptors=[])
            for fd in descriptors:
                try:
                    actual = fd.stat()
                    if (actual.st_dev, actual.st_ino) != (info.st_dev, info.st_ino):
                        continue
                    fields = dict(line.split(':', 1) for line in
                                  (fd_root.parent / 'fdinfo' / fd.name).read_text().splitlines() if ':' in line)
                    row['descriptors'].append({'fd': int(fd.name), 'flags': int(fields['flags'].strip(), 8)})
                except FileNotFoundError:
                    continue
            row['available'] = bool(row['descriptors'])
            if not row['available']:
                row['reason'] = 'matching backing descriptor unavailable'
        except (OSError, ValueError, KeyError) as error:
            row['reason'] = type(error).__name__ + ': ' + str(error)
        result.append(row)
    return result


def thread_diagnostics(pid):
    """Bounded kernel thread observations; not proof that RDMA hardware drained."""
    root = Path('/proc') / str(pid) / 'task'
    threads, truncated = [], False
    with os.scandir(root) as entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            if len(threads) == 128:
                truncated = True
                break
            thread = {'tid': int(entry.name)}
            for field in ('wchan', 'stack'):
                try:
                    with (root / entry.name / field).open('rb') as stream:
                        raw = stream.read(4097)
                    thread[field] = {'available': True, 'text': raw[:4096].decode('utf-8', errors='replace'),
                                     'truncated': len(raw) > 4096}
                except OSError as error:
                    thread[field] = {'available': False, 'reason': type(error).__name__ + ': ' + str(error)}
            threads.append(thread)
    threads.sort(key=lambda value: value['tid'])
    return {'threads': threads, 'truncated': truncated,
            'scope': 'kernel thread snapshots; not physical RDMA drain proof'}


def observe(state_path, diagnostics=False):
    directory, state = campaign.campaign_state(campaign.absolute(state_path), 'live-spdk-daemon')
    try:
        if state['phase'] != 'ready':
            raise ValueError('live SPDK daemon is not ready')
        verify(state)
        result = {'daemon': {k: state[k] for k in ('pid', 'process_start_ticks', 'executable',
                  'executable_device', 'executable_inode', 'binary_sha256', 'rpc_socket', 'rpc_device', 'rpc_inode')},
                  'bdevs': spdk.rpc(state['rpc_socket'], 'bdev_get_bdevs'),
                  'iostat': spdk.rpc(state['rpc_socket'], 'bdev_get_iostat'),
                  'subsystems': spdk.rpc(state['rpc_socket'], 'nvmf_get_subsystems'),
                  'transports': spdk.rpc(state['rpc_socket'], 'nvmf_get_transports'),
                  'nvmf_stats': spdk.rpc(state['rpc_socket'], 'nvmf_get_stats')}
        result['backing_fds'] = backing_fds(state['pid'], result['bdevs'])
        if diagnostics:
            result['thread_diagnostics'] = thread_diagnostics(state['pid'])
        verify(state)
        return result
    finally:
        directory.close()


def stop(state_path, proof_path, initiator_drained):
    if not initiator_drained:
        raise ValueError('explicit initiator drain required')
    proof = campaign.trusted_json(proof_path)
    if proof.get('complete') is not True or proof.get('target', {}).get('owned_namespace_count') != 0:
        raise ValueError('owner namespace drain proof required')
    directory, state = campaign.campaign_state(campaign.absolute(state_path), 'live-spdk-daemon')
    try:
        verify(state)
        bdevs = spdk.rpc(state['rpc_socket'], 'bdev_get_bdevs')
        subsystems = spdk.rpc(state['rpc_socket'], 'nvmf_get_subsystems')
        if bdevs or any(item.get('namespaces') for item in subsystems):
            raise ValueError('live SPDK still owns bdevs or namespaces; quarantine')
        verify(state)
        spdk.capture_journal(state)
        state['phase'] = 'stopping'
        manage.save(directory, state)
        spdk.stop_owned(state)
        state['exit_receipt'] = admit_exit(state_path, state)
        state['phase'] = 'stopped'
        manage.save(directory, state)
        return state
    except BaseException as error:
        state['phase'] = 'quarantined'
        state['cleanup_error'] = type(error).__name__ + ': ' + str(error)
        spdk.capture_journal(state)
        try:
            manage.save(directory, state)
        except Exception as journal_error:
            error.add_note('live SPDK quarantine journal unavailable: ' + repr(journal_error))
        raise
    finally:
        directory.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    setup = sub.add_parser('start')
    setup.add_argument('--state', required=True)
    setup.add_argument('--socket', required=True)
    setup.add_argument('--executable', required=True)
    setup.add_argument('--deadline-epoch', required=True, type=float)
    snapshot = sub.add_parser('observe')
    snapshot.add_argument('--state', required=True)
    snapshot.add_argument('--diagnostics', action='store_true')
    cleanup = sub.add_parser('stop')
    cleanup.add_argument('--state', required=True)
    cleanup.add_argument('--owner-proof', required=True)
    cleanup.add_argument('--initiator-drained', action='store_true')
    recorder = sub.add_parser('record-exit')
    recorder.add_argument('--state', required=True)
    recorder.add_argument('--unit', required=True)
    args = parser.parse_args()
    if args.action == 'start':
        result = start(args.state, args.socket, args.executable, args.deadline_epoch)
    elif args.action == 'observe':
        result = observe(args.state, args.diagnostics)
    elif args.action == 'stop':
        result = stop(args.state, args.owner_proof, args.initiator_drained)
    else:
        result = record_exit(args.state, args.unit)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
