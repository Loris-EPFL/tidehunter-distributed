"""Explicit local artifact byte transport using SDK-delegated guest credentials.

The MCP gcloud sandbox cannot access workstation files or stream arbitrary
binary data. Only artifact operations use the existing JSON REST ObjectStore;
Compute lifecycle remains exclusively on the supplied MCP callback. SDK-delegated
guest tokens are cached briefly in memory only, never persisted.
"""
import math
import os
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

import gcp_api
from gcp_api import GoogleApi, ObjectStore

ACCOUNT = 'bench-guest@mysten-research.iam.gserviceaccount.com'
PROJECT = 'mysten-research'
BUCKET = 'mysten-research-bench-artifacts'
TRANSPORT = 'json-rest-with-SDK-delegation'
MAX_JSON_BYTES = 16 * 1024 * 1024
TOKEN_ATTEMPTS = 3
TOKEN_CACHE_SECONDS = 60
SOCKET_SECONDS = 10
# An upload streams its body in 1 MiB chunks; one socket write sends one chunk.
# With 10 s a chunk needs about 105 KiB/s, and a slower uplink fails the launch
# before any VM exists (12 November 2026). The absolute deadline still applies
# before each chunk.
UPLOAD_SOCKET_SECONDS = 300


class CredentialUnavailable(RuntimeError):
    pass


class _DeadlineResponse:
    """Bound JSON bodies and check the deadline between response chunks."""
    def __init__(self, response, remaining, max_json_bytes=MAX_JSON_BYTES):
        self.response, self.remaining, self.max_json_bytes = response, remaining, max_json_bytes

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.response.close()

    def __getattr__(self, name):
        return getattr(self.response, name)

    def _read_once(self, size):
        self.remaining()
        read = getattr(self.response, 'read1', self.response.read)
        data = read(size)
        self.remaining()
        return data

    def read1(self, size=-1):
        return self._read_once(size if size >= 0 else 1024 * 1024)

    def read(self, size=-1):
        if size >= 0:
            return self._read_once(size)
        parts, length = [], 0
        while True:
            part = self._read_once(min(1024 * 1024, self.max_json_bytes - length + 1))
            if not part:
                return b''.join(parts)
            length += len(part)
            if length > self.max_json_bytes:
                raise ValueError('artifact response exceeds bounded JSON size')
            parts.append(part)


class DelegatedArtifacts(ObjectStore):
    """ObjectStore with short-lived SDK delegation and checked transfer deadlines.

    Socket inactivity and chunk checks do not guarantee a hard wall-clock bound
    for DNS, headers or a stalled read. VM lifetime must be enforced by GCE.

    The SDK returns an opaque bearer token without its actual expiry. The cache
    age is capped conservatively from refresh start, but this is not a guarantee
    of token validity; HTTP authorization failures are not retried here.
    """

    def __init__(self, bucket=BUCKET, *, deadline, executable='gcloud', opener=None,
                 guest=False):
        if bucket != BUCKET or type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError('exact reviewed bucket and finite absolute deadline required')
        self.deadline = deadline
        self.executable = executable
        self._open = opener or urllib.request.urlopen
        self._token_lock = threading.Lock()
        self._cached_token = None
        self._token_cache_until = 0.0
        # The coordinator copy already runs as the guest identity, so it takes
        # the metadata token directly and never shells out to `gcloud`.
        provider = gcp_api.metadata_token if guest else self._token
        api = GoogleApi(PROJECT, token_provider=provider, opener=self._bounded_open)
        super().__init__(api, bucket)

    def _remaining(self):
        remaining = self.deadline - time.time()
        if remaining <= 0:
            raise TimeoutError('artifact transfer deadline expired')
        return remaining

    def _token(self):
        # Check the current mutable deadline even when a cached token is available.
        while True:
            remaining = self._remaining()
            if self._token_lock.acquire(timeout=min(remaining, threading.TIMEOUT_MAX)):
                break
            # The deadline can be extended while the waiter is blocked.
            self._remaining()
        try:
            self._remaining()
            if self._cached_token is not None and time.monotonic() < self._token_cache_until:
                self._remaining()
                return self._cached_token
            self._cached_token = None
            self._token_cache_until = 0.0
            refresh_started = time.monotonic()
            token = self._refresh_token()
            self._remaining()
            self._cached_token = token
            self._token_cache_until = refresh_started + TOKEN_CACHE_SECONDS
            return token
        finally:
            self._token_lock.release()

    def _refresh_token(self):
        # No access token or SDK stderr is written to disk or included in errors.
        command = [self.executable, 'auth', 'print-access-token',
                   '--impersonate-service-account=' + ACCOUNT,
                   '--project=' + PROJECT, '--quiet']
        for attempt in range(TOKEN_ATTEMPTS):
            self._remaining()
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                       start_new_session=True)
            timed_out = False
            try:
                timeout = min(30, self._remaining())
                stdout, _ = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
                raise
            if timed_out:
                self._remaining()
                if attempt + 1 == TOKEN_ATTEMPTS:
                    # Preserve the recognized command for the mailbox consumer,
                    # but never propagate captured subprocess output.
                    raise subprocess.TimeoutExpired(command, timeout)
                continue
            if process.returncode != 0:
                raise CredentialUnavailable(f'guest SDK delegation failed (exit status {process.returncode})')
            try:
                token = stdout.decode('ascii').strip()
            except UnicodeDecodeError:
                raise RuntimeError('guest SDK returned an invalid token') from None
            if not token or any(c.isspace() for c in token):
                raise RuntimeError('guest SDK returned an invalid token')
            self._remaining()
            return token

    def _bounded_open(self, request, timeout):
        remaining = self._remaining()
        streamed = request.data is not None and not isinstance(request.data, (bytes, bytearray))
        try:
            response = self._open(request, timeout=min(
                timeout, remaining, UPLOAD_SOCKET_SECONDS if streamed else SOCKET_SECONDS))
        except urllib.error.HTTPError as error:
            if error.fp is not None:
                bounded = _DeadlineResponse(error.fp, self._remaining, 65536)
                error.read = bounded.read
            raise
        try:
            self._remaining()
        except BaseException:
            response.close()
            raise
        return _DeadlineResponse(response, self._remaining)

    def upload(self, key, path, *, content_type='application/octet-stream', if_generation_match=None,
               metadata=None, deadline_check=None):
        def check():
            self._remaining()
            if deadline_check:
                deadline_check()
        return super().upload(key, path, content_type=content_type, if_generation_match=if_generation_match,
                              metadata=metadata, deadline_check=check)

    def get(self, key, *, generation=None, max_bytes=256 * 1024 * 1024):
        # Do not trust a declared object size as a bound on an arbitrary media
        # response; reuse the streaming download's size and digest checks.
        with tempfile.TemporaryDirectory(prefix='gcp-artifact-') as directory:
            path = os.path.join(directory, 'object')
            self.download(key, path, generation=generation, max_bytes=max_bytes)
            with open(path, 'rb') as source:
                data = source.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError('artifact object exceeds max_bytes')
            self._remaining()
            return data

    def download(self, key, path, *, generation=None, max_bytes=256 * 1024 * 1024):
        return super().download(key, path, generation=generation, max_bytes=max_bytes,
                                deadline_check=self._remaining)

    def list(self, prefix):
        for item in super().list(prefix):
            self._remaining()
            yield item
