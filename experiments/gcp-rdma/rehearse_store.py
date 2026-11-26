#!/usr/bin/env python3
"""A local object store that speaks the GCS JSON API the campaign clients use.

The free fleet rehearsal runs the real `gcp_api.ObjectStore` client, the real
mailbox, the real worker and the real drivers. Only the service behind those
clients is local: this module keeps generations, `ifGenerationMatch`
preconditions and SHA-256 metadata in one directory, and serves the four
requests the client makes. Nothing here talks to a cloud API.
"""
import fcntl
import hashlib
import json
import mmap
import os
from pathlib import Path
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BYTES = 8 * 1024 * 1024 * 1024


def _quoted(value):
    return urllib.parse.quote(str(value), safe='')


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


class Store:
    """One bucket directory: generations, immutable bytes, SHA-256 metadata."""

    def __init__(self, root, bucket):
        self.root = Path(root) / bucket
        (self.root / 'objects').mkdir(parents=True, exist_ok=True)
        self.index_path = self.root / 'index.json'
        self.lock_path = self.root / '.lock'
        if not self.index_path.exists():
            self.index_path.write_text(json.dumps({'next': 1, 'objects': {}}))

    # ---- index ---------------------------------------------------------
    def _lock(self, exclusive):
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        return descriptor

    def _unlock(self, descriptor):
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    def _index(self):
        return json.loads(self.index_path.read_text())

    def _save(self, index):
        temporary = self.index_path.with_name('index.json.' + os.urandom(4).hex())
        temporary.write_text(json.dumps(index))
        os.replace(temporary, self.index_path)

    def _check(self, record, if_generation_match):
        if if_generation_match is None:
            return
        expected = str(if_generation_match)
        if expected == '0':
            if record is not None:
                raise Precondition()
        elif record is None or str(record['generation']) != expected:
            raise Precondition()

    # ---- operations ----------------------------------------------------
    def put_path(self, key, path, *, if_generation_match=None, metadata=None,
                 content_type='application/octet-stream'):
        size = os.path.getsize(path)
        sha256 = _sha256_file(path)
        with self._locked(True):
            index = self._index()
            record = index['objects'].get(key)
            self._check(record, if_generation_match)
            generation = index['next']
            index['next'] = generation + 1
            target = self.root / 'objects' / sha256
            if not target.exists():
                temporary = target.with_name(sha256 + '.' + os.urandom(4).hex())
                with open(path, 'rb') as source, open(temporary, 'xb') as output:
                    for chunk in iter(lambda: source.read(1 << 20), b''):
                        output.write(chunk)
                os.replace(temporary, target)
            attrs = dict(metadata or {})
            attrs['sha256'] = sha256
            index['objects'][key] = {'generation': generation, 'size': size,
                                     'sha256': sha256, 'metadata': attrs,
                                     'contentType': content_type,
                                     'timeCreated': time.strftime(
                                         '%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
            self._save(index)
        return self.head(key)

    def _locked(self, exclusive):
        return _Guard(self, exclusive)

    def put(self, key, data, *, if_generation_match=None, metadata=None,
            content_type='application/octet-stream'):
        handle, name = tempfile.mkstemp(dir=str(self.root))
        os.close(handle)
        try:
            Path(name).write_bytes(bytes(data))
            return self.put_path(key, name, if_generation_match=if_generation_match,
                                 metadata=metadata, content_type=content_type)
        finally:
            os.unlink(name)

    def head(self, key):
        descriptor = self._lock(False)
        try:
            record = self._index()['objects'].get(key)
        finally:
            self._unlock(descriptor)
        if record is None:
            raise Missing()
        return {'name': key, 'generation': str(record['generation']),
                'size': record['size'], 'metadata': dict(record['metadata']),
                'contentType': record['contentType'], 'bucket': self.root.name,
                'timeCreated': record.get('timeCreated')}

    def read(self, key, generation=None, max_bytes=MAX_BYTES):
        descriptor = self._lock(False)
        try:
            record = self._index()['objects'].get(key)
        finally:
            self._unlock(descriptor)
        if record is None or (generation is not None
                              and str(generation) != str(record['generation'])):
            raise Missing()
        if record['size'] > max_bytes:
            raise TooLarge()
        path = self.root / 'objects' / record['sha256']
        data = path.read_bytes()
        if len(data) != record['size'] or hashlib.sha256(data).hexdigest() != record['sha256']:
            raise ValueError(f'{key}: stored object differs from its index')
        return data

    def list(self, prefix, limit=1000, page=None):
        descriptor = self._lock(False)
        try:
            objects = self._index()['objects']
        finally:
            self._unlock(descriptor)
        names = sorted(name for name in objects if name.startswith(prefix))
        if page:
            names = [name for name in names if name > page]
        selected = names[:limit]
        return [self.head(name) for name in selected], (
            selected[-1] if len(names) > limit else None)

    def delete(self, key, *, if_generation_match=None):
        with self._locked(True):
            index = self._index()
            record = index['objects'].get(key)
            if record is None:
                raise Missing()
            self._check(record, if_generation_match)
            del index['objects'][key]
            self._save(index)


class _Guard:
    def __init__(self, store, exclusive):
        self.store, self.exclusive = store, exclusive

    def __enter__(self):
        self.descriptor = self.store._lock(self.exclusive)
        return self

    def __exit__(self, *args):
        self.store._unlock(self.descriptor)


class Missing(Exception):
    pass


class Precondition(Exception):
    pass


class TooLarge(Exception):
    pass


def _multipart(body_path, boundary, workdir):
    """Split one multipart body: return (attributes, data path, data size, digest).

    A boundary delimiter is `CRLF--boundary` (RFC 2046), so payload bytes that
    merely contain the boundary token stay inside the data part.
    """
    first = b'--' + boundary.encode()
    marker = b'\r\n--' + boundary.encode()
    with open(body_path, 'rb') as stream, \
            mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as memory:
        if memory[:len(first)] != first:
            raise ValueError('multipart boundary missing')
        header_end = memory.find(b'\r\n\r\n', len(first))
        json_end = memory.find(marker, header_end + 4)
        if header_end < 0 or json_end < 0:
            raise ValueError('multipart attribute part is malformed')
        attributes = json.loads(memory[header_end + 4:json_end])
        position = json_end + len(marker)
        data_header_end = memory.find(b'\r\n\r\n', position)
        data_end = memory.find(marker, data_header_end + 4)
        if data_header_end < 0 or data_end < 0:
            raise ValueError('multipart data part is malformed')
        handle, name = tempfile.mkstemp(dir=str(workdir))
        digest = hashlib.sha256()
        written = 0
        with os.fdopen(handle, 'wb') as output:
            cursor = data_header_end + 4
            while cursor < data_end:
                chunk = memory[cursor:min(cursor + 1 << 20, data_end)]
                output.write(chunk)
                digest.update(chunk)
                written += len(chunk)
                cursor += len(chunk)
    return attributes, Path(name), written, digest.hexdigest()


def serve_handler(store):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def _reply(self, code, payload, content_type='application/json'):
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _error(self, code, message):
            self._reply(code, {'error': {'code': code, 'message': message}})

        def _split(self):
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            return parsed.path, {key: values[-1] for key, values in query.items()}

        def do_GET(self):
            path, query = self._split()
            if path.endswith('/o') or path.endswith('/o/'):
                prefix = query.get('prefix', '')
                limit = int(query.get('maxResults', '1000'))
                try:
                    items, page = store.list(prefix, limit, query.get('pageToken'))
                except ValueError as error:
                    return self._error(500, str(error))
                payload = {'items': items}
                if page is not None:
                    payload['nextPageToken'] = page
                return self._reply(200, payload)
            marker = '/o/'
            if marker not in path:
                return self._error(404, 'unsupported path')
            key = urllib.parse.unquote(path.split(marker, 1)[1])
            try:
                if query.get('alt') == 'media':
                    return self._reply(200, store.read(key, query.get('generation')),
                                       'application/octet-stream')
                return self._reply(200, store.head(key))
            except Missing:
                return self._error(404, 'no such object')
            except TooLarge:
                return self._error(400, 'object exceeds the transfer bound')

        def do_DELETE(self):
            path, query = self._split()
            key = urllib.parse.unquote(path.split('/o/', 1)[1])
            try:
                store.delete(key, if_generation_match=query.get('ifGenerationMatch'))
            except Missing:
                return self._error(404, 'no such object')
            except Precondition:
                return self._error(412, 'generation precondition failed')
            return self._reply(204, b'')

        def do_POST(self):
            path, query = self._split()
            if '/upload/storage/' not in path:
                return self._error(404, 'unsupported upload path')
            length = int(self.headers.get('Content-Length', '0'))
            content_type = self.headers.get('Content-Type', '')
            if 'boundary=' not in content_type:
                return self._error(400, 'multipart boundary missing')
            boundary = content_type.split('boundary=', 1)[1].strip().strip('"')
            handle, body = tempfile.mkstemp(dir=str(store.root))
            try:
                with os.fdopen(handle, 'wb') as output:
                    remaining = length
                    while remaining > 0:
                        chunk = self.rfile.read(min(1 << 20, remaining))
                        if not chunk:
                            return self._error(400, 'truncated body')
                        output.write(chunk)
                        remaining -= len(chunk)
                try:
                    attributes, data, size, digest = _multipart(body, boundary, store.root)
                except ValueError as error:
                    return self._error(400, str(error))
                try:
                    if size > MAX_BYTES:
                        return self._error(400, 'object exceeds the transfer bound')
                    record = store.put_path(
                        attributes.get('name'), data,
                        if_generation_match=query.get('ifGenerationMatch'),
                        metadata=attributes.get('metadata'),
                        content_type='application/octet-stream')
                except Precondition:
                    return self._error(412, 'generation precondition failed')
                if record['size'] != size or record['metadata']['sha256'] != digest:
                    return self._error(500, 'stored object differs from the request')
                return self._reply(200, record)
            finally:
                os.unlink(body)

    return Handler


class Server:
    """A threaded local endpoint for one bucket directory."""

    def __init__(self, root, bucket, *, host='0.0.0.0', port=0):
        self.store = Store(root, bucket)
        self.httpd = ThreadingHTTPServer((host, port), serve_handler(self.store))
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=10)
