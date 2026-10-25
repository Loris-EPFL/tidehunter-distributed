#!/usr/bin/env python3
"""Configure and manage only the local KV comparison observability project."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
STATE = ROOT / '.state'
PROJECT = 'kv-comparison-observability'


def atomic_json(path, value):
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def target_group(endpoint):
    """Accept explicit loopback HTTP targets, without discovery or DNS lookups."""
    url = urlsplit(endpoint)
    if (url.scheme != 'http' or url.hostname not in ('localhost', '127.0.0.1')
            or url.username is not None or url.password is not None
            or url.query or url.fragment or not url.port or not 1 <= url.port <= 65535):
        raise ValueError('target must be http://127.0.0.1:PORT/metrics (loopback only)')
    path = url.path or '/metrics'
    if len(path) > 256 or not path.startswith('/') or any(c not in '/-_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.' for c in path):
        raise ValueError('target metrics path has unsafe characters')
    if any(part in ('.', '..') for part in path.split('/')):
        raise ValueError('target metrics path has dot segments')
    return {'targets': [f'127.0.0.1:{url.port}'], 'labels': {'__metrics_path__': path}}


def result_selection(path):
    candidate = path.expanduser()
    if not candidate.is_absolute() or '..' in candidate.parts:
        raise ValueError('results must be an absolute path without traversal')
    parent = candidate.parent.resolve(strict=True)
    if not parent.is_dir() or parent == Path('/'):
        raise ValueError('select a run below an existing dedicated result parent, not /')
    selected = parent / candidate.name
    if selected.is_symlink() or (selected.exists() and not selected.is_dir()):
        raise ValueError('selected run must be a directory, not a symlink or file')
    return selected


def initialize():
    if STATE.is_symlink():
        raise ValueError('state directory must not be a symlink')
    STATE.mkdir(mode=0o700, exist_ok=True)
    STATE.chmod(0o700)
    (STATE / 'targets').mkdir(mode=0o755, exist_ok=True)
    (STATE / 'no-run').mkdir(mode=0o755, exist_ok=True)
    password = STATE / 'grafana_admin_password'
    try:
        fd = os.open(password, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o444)
    except FileExistsError:
        if password.is_symlink() or not password.is_file():
            raise ValueError('Grafana secret must be an ordinary file')
    else:
        with os.fdopen(fd, 'w') as stream:
            stream.write(secrets.token_urlsafe(32) + '\n')
        # The containing state directory is private; Grafana's non-root UID must
        # be able to read the separately mounted secret file inside its container.
        password.chmod(0o444)
    config_path = STATE / 'selection.json'
    if not config_path.exists():
        save_configuration({'results': None, 'stale_seconds': 15, 'native': [], 'sui': []})


def save_configuration(config):
    atomic_json(STATE / 'selection.json', config)
    source = config['results'] or str(STATE / 'no-run')
    container_results = '/results' if config['results'] else None
    if config['results'] and not Path(source).exists():
        selected = Path(source)
        source = str(selected.parent)
        container_results = '/results/' + selected.name
    atomic_json(STATE / 'exporter.json', {
        'results': container_results,
        'stale_seconds': config['stale_seconds'],
    })
    atomic_json(STATE / 'runtime.json', {'services': {'exporter': {
        'user': f'{os.getuid()}:{os.getgid()}',
        'volumes': [{'type': 'bind', 'source': source, 'target': '/results', 'read_only': True,
                     'bind': {'create_host_path': False}}],
    }}})
    for name, targets in (
        ('comparison', [{'targets': ['127.0.0.1:9899']}]),
        ('host', [{'targets': ['127.0.0.1:9100']}]),
        ('native', config['native']), ('sui', config['sui']),
    ):
        atomic_json(STATE / 'targets' / (name + '.json'), targets)


def compose(*arguments, capture=False, timeout=180):
    command = ['docker', 'compose', '--project-name', PROJECT, '--project-directory', str(ROOT),
               '-f', str(ROOT / 'compose.yaml'), '-f', str(STATE / 'runtime.json'), *arguments]
    return subprocess.run(command, check=True, timeout=timeout, text=True,
                          stdout=subprocess.PIPE if capture else None)


def record_images():
    effective = json.loads(compose('config', '--format', 'json', capture=True).stdout)
    receipts = {}
    for service, settings in effective['services'].items():
        image = settings['image']
        metadata = json.loads(subprocess.run(['docker', 'image', 'inspect', image], check=True,
                                            capture_output=True, text=True, timeout=15).stdout)[0]
        receipts[service] = {'requested': image, 'id': metadata['Id'],
                             'repo_digests': metadata.get('RepoDigests', []),
                             'architecture': metadata['Architecture'], 'os': metadata['Os']}
    atomic_json(STATE / 'resolved-images.json', receipts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    configure = commands.add_parser('configure', help='Select one run and replace optional targets; no services start')
    selection = configure.add_mutually_exclusive_group()
    selection.add_argument('--results', type=Path, help='Absolute current run path; parent must exist, leaf may be absent')
    selection.add_argument('--clear-results', action='store_true')
    configure.add_argument('--stale-seconds', type=int, default=15)
    configure.add_argument('--native-target', action='append', default=[], metavar='HTTP_URL')
    configure.add_argument('--sui-target', action='append', default=[], metavar='HTTP_URL')
    start = commands.add_parser('start', help='Start pinned services; --offline requires cached images')
    start.add_argument('--offline', action='store_true', help='Do not contact image registries; fail if a pinned image is absent')
    commands.add_parser('stop', help='Stop exact project containers; retain data volumes and secret')
    commands.add_parser('status', help='Show exact project container health and selected run')
    commands.add_parser('pull', help='Pull pinned images without starting services')
    commands.add_parser('password', help='Print the locally generated Grafana researcher password')
    verify_command = commands.add_parser('verify', help='Exercise real Prometheus and authenticated Grafana dashboard query paths')
    verify_command.add_argument('--output', type=Path, help='Write compact JSON verification receipt atomically')
    verify_command.add_argument('--readiness-seconds', type=int, default=45,
                                help='Bounded wait for the selected producer and host scrape (1–120 seconds)')
    purge = commands.add_parser('purge', help='Remove only this project containers and data volumes; retain config/secret')
    purge.add_argument('--yes', action='store_true', required=True)
    args = parser.parse_args()
    try:
        if sys.platform != 'linux':
            parser.error('This host-network stack requires native Linux Docker Engine')
        initialize()
        current = json.loads((STATE / 'selection.json').read_text())
        if args.command == 'configure':
            if not 2 <= args.stale_seconds <= 3600:
                parser.error('--stale-seconds must be between 2 and 3600')
            if len(args.native_target) > 16 or len(args.sui_target) > 16:
                parser.error('at most 16 explicitly supplied targets per optional job')
            if args.results is not None:
                current['results'] = str(result_selection(args.results))
            elif args.clear_results:
                current['results'] = None
            current.update(stale_seconds=args.stale_seconds,
                           native=[target_group(x) for x in args.native_target],
                           sui=[target_group(x) for x in args.sui_target])
            save_configuration(current)
            print(json.dumps(current, indent=2))
            print('If running, use start to recreate the exporter mount. Target discovery reloads within 5 seconds.')
        elif args.command in ('pull', 'start'):
            if args.command == 'pull' or not args.offline:
                compose('pull', timeout=900)
            record_images()
            if args.command == 'start':
                compose('up', '-d', '--pull', 'never', '--wait', '--force-recreate', '--wait-timeout', '150', timeout=180)
                print('Grafana http://127.0.0.1:3000/d/kv-comparison-local ; user researcher')
                print('Read generated password with: python3 ' + str(ROOT / 'stack.py') + ' password')
        elif args.command == 'stop':
            compose('down', '--timeout', '20')
        elif args.command == 'purge':
            compose('down', '--timeout', '20', '--volumes')
        elif args.command == 'status':
            print(json.dumps(current, indent=2))
            compose('ps', '--all')
        elif args.command == 'password':
            print((STATE / 'grafana_admin_password').read_text().strip())
        elif args.command == 'verify':
            if not 1 <= args.readiness_seconds <= 120:
                parser.error('--readiness-seconds must be between 1 and 120')
            from verify import verify
            receipt = verify(STATE, current, args.readiness_seconds)
            if args.output is not None:
                destination = args.output.expanduser().absolute()
                if not destination.parent.is_dir():
                    parser.error('--output parent directory must exist')
                atomic_json(destination, receipt)
            print(json.dumps(receipt, separators=(',', ':')))
            return 0 if receipt['success'] else 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f'Observability command failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
