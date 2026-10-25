#!/usr/bin/env python3
"""Read one explicitly selected run. No discovery, rewriting or synthetic samples."""
import argparse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import math
import os
from pathlib import Path
import re
import stat
import time

MAX_BYTES = 8 * 1024 * 1024
TIMESTAMP = re.compile(r'^kv_comparison_snapshot_timestamp_seconds(?:\{[^\n]*\})?\s+([^\s]+)\s*$', re.M)
STATUS = re.compile(r'^kv_comparison_run_status(?:\{[^\n]*\})?\s+([^\s]+)\s*$', re.M)


@dataclass(frozen=True)
class Snapshot:
    state: str
    payload: bytes = b''
    age: float | None = None
    final: bool = False


def read_regular(directory_fd, name):
    """Fixed basenames only; reject symlinks, devices, FIFOs and oversized files."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BYTES:
            raise ValueError('snapshot is not a bounded regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(MAX_BYTES + 1)
        after = os.fstat(fd)
        if len(data) > MAX_BYTES or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError('snapshot changed during read')
        return data
    finally:
        os.close(fd)


def scalar(pattern, text):
    matches = pattern.findall(text)
    if len(matches) != 1:
        raise ValueError('expected one contract scalar')
    value = float(matches[0])
    if not math.isfinite(value):
        raise ValueError('nonfinite contract scalar')
    return value


def snapshot(directory, stale_seconds, now=None):
    if directory is None:
        return Snapshot('no_run')
    now = time.time() if now is None else now
    try:
        try:
            root = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return Snapshot('waiting')
        try:
            try:
                payload = read_regular(root, 'metrics.prom')
                final = True
            except FileNotFoundError:
                try:
                    payload = read_regular(root, 'live.prom')
                    final = False
                except FileNotFoundError:
                    return Snapshot('waiting')
        finally:
            os.close(root)
        text = payload.decode('utf-8', errors='strict')
        if not text.endswith('\n') or '\x00' in text or '\r' in text:
            raise ValueError('invalid snapshot framing')
        # Reserve our namespace so a result cannot impersonate exporter health.
        if re.search(r'^kv_observer_', text, re.M):
            raise ValueError('reserved exporter metric namespace')
        stamp = scalar(TIMESTAMP, text)
        status = scalar(STATUS, text)
        if stamp < 0 or stamp > now + 5 or status not in (0, 1, 2):
            raise ValueError('invalid snapshot timestamp or status')
        age = max(0.0, now - stamp)
        if final:
            if status == 0:
                raise ValueError('final snapshot is still running')
            return Snapshot('completed' if status == 1 else 'failed', payload, age, True)
        if age > stale_seconds:
            return Snapshot('stale', age=age)
        return Snapshot(('running', 'completed', 'failed')[int(status)], payload, age)
    except (OSError, ValueError, UnicodeError):
        return Snapshot('invalid')


def exposition(current):
    lines = [
        '# HELP kv_observer_state State of the explicitly selected result directory.',
        '# TYPE kv_observer_state gauge',
        f'kv_observer_state{{state="{current.state}"}} 1',
        '# HELP kv_observer_data_available Whether this scrape includes actual run samples.',
        '# TYPE kv_observer_data_available gauge',
        f'kv_observer_data_available {int(bool(current.payload))}',
        '# HELP kv_observer_snapshot_final Whether the selected snapshot is final.',
        '# TYPE kv_observer_snapshot_final gauge',
        f'kv_observer_snapshot_final {int(current.final)}',
    ]
    if current.age is not None:
        lines += ['# HELP kv_observer_snapshot_age_seconds Age of the producer timestamp, not scrape time.',
                  '# TYPE kv_observer_snapshot_age_seconds gauge',
                  f'kv_observer_snapshot_age_seconds {current.age:.6f}']
    return ('\n'.join(lines) + '\n').encode() + current.payload


class MetricsServer(HTTPServer):
    # A single bounded request at a time; no unbounded thread creation.
    request_queue_size = 8
    allow_reuse_address = True

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(3)
        return connection, address


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'
    server_version = 'kv-observer'
    sys_version = ''

    def do_GET(self):
        self.respond(False)

    def do_HEAD(self):
        self.respond(True)

    def do_POST(self):
        self.send_error(405, 'Method not allowed')

    def respond(self, head):
        if self.path not in ('/metrics', '/healthz'):
            self.send_error(404, 'Not found')
            return
        if self.headers.get('Transfer-Encoding') or self.headers.get('Content-Length', '0') != '0':
            self.send_error(400, 'Request body not accepted')
            return
        if self.path == '/healthz':
            body = b'exporter ready; inspect kv_observer_state for run health\n'
            kind = 'text/plain; charset=utf-8'
        else:
            body = exposition(snapshot(self.server.results, self.server.stale_seconds))
            kind = 'text/plain; version=0.0.4; charset=utf-8'
        self.send_response(200)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        if not head:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if set(config) != {'results', 'stale_seconds'}:
        parser.error('config must contain only results and stale_seconds')
    results = config['results']
    if results is not None and (not isinstance(results, str) or not Path(results).is_absolute()):
        parser.error('results must be null or an absolute directory')
    stale = config['stale_seconds']
    if not isinstance(stale, (float, int)) or not math.isfinite(stale) or not 2 <= stale <= 3600:
        parser.error('stale_seconds must be between 2 and 3600')
    with MetricsServer(('127.0.0.1', 9899), Handler) as server:
        server.results = results
        server.stale_seconds = stale
        server.serve_forever(poll_interval=0.5)


if __name__ == '__main__':
    main()
