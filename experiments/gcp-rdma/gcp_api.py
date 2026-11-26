"""Small, bounded Compute Engine and Cloud Storage JSON REST clients."""
import datetime
import hashlib
import ipaddress
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

STORAGE_ENDPOINT = 'https://storage.googleapis.com'
# IAM Credentials mints delegated tokens of at most one hour. Refresh early so
# a long cell never sends an expired token.
TOKEN_LIFETIME_SECONDS = 3600
TOKEN_REFRESH_MARGIN_SECONDS = 60


class ApiError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(f'Google API {status}: {message}')

def local_token(project):
    # 120 s: a token refresh over a degraded workstation uplink exceeded 30 s (12 November 2026).
    return subprocess.check_output(['gcloud', 'auth', 'print-access-token', '--project=' + project, '--quiet'], text=True, timeout=120).strip()


def metadata_token():
    request = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token', headers={'Metadata-Flavor': 'Google'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=10) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            raise ValueError('untrusted metadata server')
        return json.load(response)['access_token']


def token_expiry(expire_time, *, now=None):
    """Monotonic instant to refresh by, from an IAM `expireTime`.

    A response without a parsable expiry is treated as the shortest admitted
    lifetime, so the next request mints a new token instead of trusting an
    unknown one.
    """
    now = now or time.monotonic
    fallback = now() + TOKEN_LIFETIME_SECONDS - 2 * TOKEN_REFRESH_MARGIN_SECONDS
    if not isinstance(expire_time, str) or not expire_time:
        return fallback
    try:
        parsed = datetime.datetime.fromisoformat(expire_time.replace('Z', '+00:00'))
    except ValueError:
        return fallback
    remaining = (parsed - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    if remaining <= 0:
        return now()
    return now() + max(0, remaining - TOKEN_REFRESH_MARGIN_SECONDS)


def delegated_token(api, account):
    """A provider that mints one delegated token and refreshes it before expiry.

    The workstation cannot read the campaign bucket as its own user, so every
    laptop-side client acts as the bench-guest service account. A token minted
    once and held for the run expires after an hour and a long cell then fails
    with 401; this provider mints again shortly before the recorded expiry.
    """
    if not isinstance(account, str) or not account:
        raise ValueError('a delegated service account is required')
    url = ('https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/'
           + urllib.parse.quote(account, safe='') + ':generateAccessToken')
    lock = threading.Lock()
    state = {'token': None, 'refresh_at': 0.0}

    def provider():
        with lock:
            if state['token'] is not None and time.monotonic() < state['refresh_at']:
                return state['token']
            body = json.dumps({'scope': ['https://www.googleapis.com/auth/cloud-platform'],
                               'lifetime': f'{TOKEN_LIFETIME_SECONDS}s'}).encode()
            response = json.loads(api._request('POST', url, body,
                                               {'Content-Type': 'application/json'}))
            token = response.get('accessToken')
            if not isinstance(token, str) or not token:
                raise ValueError('IAM Credentials did not return an access token')
            state['token'] = token
            state['refresh_at'] = token_expiry(response.get('expireTime'))
            return token

    return provider


REHEARSAL_ENDPOINT = 'TIDEHUNTER_REHEARSAL_STORE'


def rehearsal_endpoint():
    """The explicit local store of the free fleet rehearsal, or None.

    The value must be plain HTTP on a loopback or private address, so it can
    never name a cloud endpoint. Only the rehearsal driver sets it.
    """
    value = os.environ.get(REHEARSAL_ENDPOINT)
    if not value:
        return None
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme != 'http' or not parsed.hostname:
        raise ValueError(f'{REHEARSAL_ENDPOINT} must be a plain HTTP endpoint')
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        if parsed.hostname != 'localhost':
            raise ValueError(f'{REHEARSAL_ENDPOINT} must name a loopback or private address')
    else:
        if not (address.is_loopback or address.is_private):
            raise ValueError(f'{REHEARSAL_ENDPOINT} must name a loopback or private address')
    return value.rstrip('/')


def campaign_client(config, *, guest=False):
    """The one campaign credential path.

    A guest uses its own metadata identity. The workstation delegates to the
    campaign service account with a refreshing token. A local rehearsal names
    an explicit store endpoint and uses its own token; it has no cloud identity.
    """
    endpoint = config.get('store_endpoint') or rehearsal_endpoint()
    if endpoint is not None:
        return GoogleApi(config['project_id'], token_provider=lambda: 'rehearsal',
                         endpoint=endpoint)
    if guest:
        return GoogleApi(config['project_id'], token_provider=metadata_token)
    account = config.get('service_account')
    return GoogleApi(config['project_id']).impersonated(account)


class GoogleApi:
    def __init__(self, project, *, token_provider=None, opener=None, endpoint=None):
        if not project:
            raise ValueError('project_id is required')
        self.project = project
        self.endpoint = (endpoint or rehearsal_endpoint() or STORAGE_ENDPOINT).rstrip('/')
        if token_provider is None:
            token_provider = ((lambda: 'rehearsal') if self.endpoint != STORAGE_ENDPOINT
                              else (lambda: local_token(project)))
        self.token_provider = token_provider
        self.opener = opener or urllib.request.urlopen

    def _request(self, method, url, body=None, headers=None, read_retries=3):
        headers = dict(headers or {})
        attempts = read_retries if method == 'GET' else 1
        for n in range(attempts):
            headers['Authorization'] = 'Bearer ' + self.token_provider()
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with self.opener(req, timeout=30) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode('utf-8', 'replace')
                if exc.code not in (429, 500, 502, 503, 504) or n + 1 == attempts:
                    raise ApiError(exc.code, detail) from exc
            # urlopen wraps only connect/send errors as URLError; a peer close
            # while waiting for the response surfaces as a raw ConnectionError.
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                if n + 1 == attempts:
                    raise ApiError(None, str(exc)) from exc
            time.sleep(min(2 ** n, 4))
        raise AssertionError('unreachable')

    def request(self, method, path, body=None, query=None):
        version = 'beta' if path.startswith('beta/') else 'v1'
        path = path.removeprefix('beta/')
        url = 'https://compute.googleapis.com/compute/' + version + '/projects/' + urllib.parse.quote(self.project, safe='') + '/' + path.lstrip('/')
        if query:
            url += '?' + urllib.parse.urlencode(query)
        encoded = json.dumps(body).encode() if body is not None else None
        raw = self._request(method, url, encoded, {'Content-Type': 'application/json'} if encoded else None)
        return json.loads(raw) if raw else {}

    def project_permissions(self, permissions):
        url = 'https://cloudresourcemanager.googleapis.com/v1/projects/' + urllib.parse.quote(self.project, safe='') + ':testIamPermissions'
        body = json.dumps({'permissions': list(permissions)}).encode()
        return json.loads(self._request('POST', url, body, {'Content-Type': 'application/json'})).get('permissions', [])

    def service_account_permissions(self, account, permissions):
        url = 'https://iam.googleapis.com/v1/projects/-/serviceAccounts/' + urllib.parse.quote(account, safe='') + ':testIamPermissions'
        body = json.dumps({'permissions': list(permissions)}).encode()
        return json.loads(self._request('POST', url, body, {'Content-Type': 'application/json'})).get('permissions', [])

    def impersonated(self, account):
        """A client that acts as `account` with a token refreshed before expiry."""
        return GoogleApi(self.project, token_provider=delegated_token(self, account),
                         opener=self.opener, endpoint=self.endpoint)

    def bucket_permissions(self, bucket, permissions):
        url = 'https://storage.googleapis.com/storage/v1/b/' + urllib.parse.quote(bucket, safe='') + '/iam/testPermissions?'
        url += urllib.parse.urlencode([('permissions', p) for p in permissions])
        return json.loads(self._request('GET', url)).get('permissions', [])

    def objects(self, bucket):
        return ObjectStore(self, bucket)


class ObjectStore:
    def __init__(self, api, bucket):
        if not bucket:
            raise ValueError('bucket is required')
        self.api, self.bucket = api, bucket
        self.base = api.endpoint + '/storage/v1/b/' + urllib.parse.quote(bucket, safe='') + '/o'

    def _url(self, key, query=None):
        url = self.base + '/' + urllib.parse.quote(key, safe='')
        if query:
            url += '?' + urllib.parse.urlencode(query)
        return url

    def put(self, key, data, *, content_type='application/octet-stream', if_generation_match=None, metadata=None):
        digest = hashlib.sha256(data).hexdigest()
        attrs = dict(metadata or {})
        attrs['sha256'] = digest
        boundary = 'gcp-' + uuid.uuid4().hex
        description = json.dumps({'name': key, 'metadata': attrs}).encode()
        body = (b'--' + boundary.encode() + b'\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + description + b'\r\n--' + boundary.encode() + b'\r\nContent-Type: ' + content_type.encode() + b'\r\n\r\n' + data + b'\r\n--' + boundary.encode() + b'--\r\n')
        query = {'uploadType': 'multipart'}
        if if_generation_match is not None:
            query['ifGenerationMatch'] = str(if_generation_match)
        url = (self.api.endpoint + '/upload/storage/v1/b/'
               + urllib.parse.quote(self.bucket, safe='') + '/o?'
               + urllib.parse.urlencode(query))
        result = json.loads(self.api._request('POST', url, body, {'Content-Type': 'multipart/related; boundary=' + boundary}))
        if result.get('metadata', {}).get('sha256') != digest or int(result.get('size', -1)) != len(data) or not result.get('generation'):
            raise ValueError('GCS upload receipt mismatch')
        return result

    def head(self, key):
        return json.loads(self.api._request('GET', self._url(key)))

    def delete(self, key, *, if_generation_match):
        return self.api._request('DELETE', self._url(key, {'ifGenerationMatch': str(if_generation_match)}))

    def get(self, key, *, generation=None, max_bytes=256 * 1024 * 1024):
        meta = self.head(key) if generation is None else json.loads(self.api._request('GET', self._url(key, {'generation': generation})))
        if int(meta['size']) > max_bytes:
            raise ValueError('GCS object exceeds max_bytes')
        generation = str(meta['generation'])
        data = self.api._request('GET', self._url(key, {'alt': 'media', 'generation': generation}))
        if len(data) != int(meta['size']) or (meta.get('metadata', {}).get('sha256') and hashlib.sha256(data).hexdigest() != meta['metadata']['sha256']):
            raise ValueError('GCS object integrity mismatch')
        return data

    def upload(self, key, path, *, content_type='application/octet-stream', if_generation_match=None, metadata=None, deadline_check=None):
        path = Path(path)
        size = path.stat().st_size
        digest = hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
                if deadline_check:
                    deadline_check()
                digest.update(chunk)
        attrs = dict(metadata or {})
        attrs['sha256'] = digest.hexdigest()
        boundary = 'gcp-' + uuid.uuid4().hex
        prefix = (b'--' + boundary.encode() + b'\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' +
                  json.dumps({'name': key, 'metadata': attrs}).encode() +
                  b'\r\n--' + boundary.encode() + b'\r\nContent-Type: ' + content_type.encode() + b'\r\n\r\n')
        suffix = b'\r\n--' + boundary.encode() + b'--\r\n'
        def chunks():
            sent = 0
            actual = hashlib.sha256()
            yield prefix
            with path.open('rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    if deadline_check:
                        deadline_check()
                    sent += len(chunk)
                    if sent > size:
                        raise ValueError('file changed during upload')
                    actual.update(chunk)
                    yield chunk
            if sent != size or actual.hexdigest() != attrs['sha256']:
                raise ValueError('file changed during upload')
            yield suffix
        query = {'uploadType': 'multipart'}
        if if_generation_match is not None:
            query['ifGenerationMatch'] = str(if_generation_match)
        url = (self.api.endpoint + '/upload/storage/v1/b/'
               + urllib.parse.quote(self.bucket, safe='') + '/o?'
               + urllib.parse.urlencode(query))
        headers = {'Authorization': 'Bearer ' + self.api.token_provider(),
                   'Content-Type': 'multipart/related; boundary=' + boundary,
                   'Content-Length': str(len(prefix) + size + len(suffix))}
        if deadline_check:
            deadline_check()
        request = urllib.request.Request(url, data=chunks(), headers=headers, method='POST')
        try:
            with self.api.opener(request, timeout=300) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            raise ApiError(exc.code, exc.read().decode('utf-8', 'replace')) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ApiError(None, str(exc)) from exc
        if result.get('metadata', {}).get('sha256') != attrs['sha256'] or int(result.get('size', -1)) != size or not result.get('generation'):
            raise ValueError('GCS upload receipt mismatch')
        return result

    def download(self, key, path, *, generation=None, max_bytes=256 * 1024 * 1024, deadline_check=None):
        if deadline_check:
            deadline_check()
        meta = self.head(key) if generation is None else json.loads(self.api._request('GET', self._url(key, {'generation': generation})))
        size = int(meta['size'])
        if size > max_bytes:
            raise ValueError('GCS object exceeds max_bytes')
        url = self._url(key, {'alt': 'media', 'generation': meta['generation']})
        request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + self.api.token_provider()})
        target = Path(path)
        temporary = target.with_name(target.name + '.' + uuid.uuid4().hex + '.part')
        digest = hashlib.sha256()
        count = 0
        try:
            with self.api.opener(request, timeout=300) as response, temporary.open('xb') as output:
                while True:
                    if deadline_check:
                        deadline_check()
                    read = getattr(response, 'read1', response.read) if deadline_check else response.read
                    chunk = read(1024 * 1024)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > size or count > max_bytes:
                        raise ValueError('GCS object exceeds declared size')
                    digest.update(chunk)
                    output.write(chunk)
            if deadline_check:
                deadline_check()
            if count != size or (meta.get('metadata', {}).get('sha256') and digest.hexdigest() != meta['metadata']['sha256']):
                raise ValueError('GCS object integrity mismatch')
            temporary.replace(target)
        except urllib.error.HTTPError as exc:
            raise ApiError(exc.code, exc.read().decode('utf-8', 'replace')) from exc
        finally:
            temporary.unlink(missing_ok=True)
        return meta

    def list(self, prefix):
        token = None
        for _ in range(1000):
            query = {'prefix': prefix, 'maxResults': 1000}
            if token:
                query['pageToken'] = token
            page = json.loads(self.api._request('GET', self.base + '?' + urllib.parse.urlencode(query)))
            yield from page.get('items', [])
            token = page.get('nextPageToken')
            if not token:
                return
        raise ValueError('GCS listing exceeds bound')
