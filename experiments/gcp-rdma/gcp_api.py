"""Small, bounded Compute Engine and Cloud Storage JSON REST clients."""
import hashlib
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


class ApiError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(f'Google API {status}: {message}')

def local_token(project):
    return subprocess.check_output(['gcloud', 'auth', 'print-access-token', '--project=' + project, '--quiet'], text=True, timeout=30).strip()


def metadata_token():
    request = urllib.request.Request('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token', headers={'Metadata-Flavor': 'Google'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=10) as response:
        if response.headers.get('Metadata-Flavor') != 'Google':
            raise ValueError('untrusted metadata server')
        return json.load(response)['access_token']


class GoogleApi:
    def __init__(self, project, *, token_provider=None, opener=None):
        if not project:
            raise ValueError('project_id is required')
        self.project = project
        self.token_provider = token_provider or (lambda: local_token(project))
        self.opener = opener or urllib.request.urlopen

    def _request(self, method, url, body=None, headers=None, read_retries=3):
        headers = dict(headers or {})
        headers['Authorization'] = 'Bearer ' + self.token_provider()
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        attempts = read_retries if method == 'GET' else 1
        for n in range(attempts):
            try:
                with self.opener(req, timeout=30) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode('utf-8', 'replace')
                if exc.code not in (429, 500, 502, 503, 504) or n + 1 == attempts:
                    raise ApiError(exc.code, detail) from exc
            except (urllib.error.URLError, TimeoutError) as exc:
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
        url = 'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/' + urllib.parse.quote(account, safe='') + ':generateAccessToken'
        body = json.dumps({'scope': ['https://www.googleapis.com/auth/cloud-platform'], 'lifetime': '600s'}).encode()
        response = json.loads(self._request('POST', url, body, {'Content-Type': 'application/json'}))
        token = response.get('accessToken')
        if not isinstance(token, str) or not token:
            raise ValueError('IAM Credentials did not return an access token')
        return GoogleApi(self.project, token_provider=lambda: token, opener=self.opener)

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
        self.base = 'https://storage.googleapis.com/storage/v1/b/' + urllib.parse.quote(bucket, safe='') + '/o'

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
        url = 'https://storage.googleapis.com/upload/storage/v1/b/' + urllib.parse.quote(self.bucket, safe='') + '/o?' + urllib.parse.urlencode(query)
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

    def upload(self, key, path, *, content_type='application/octet-stream', if_generation_match=None, metadata=None):
        path = Path(path)
        size = path.stat().st_size
        digest = hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
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
        url = 'https://storage.googleapis.com/upload/storage/v1/b/' + urllib.parse.quote(self.bucket, safe='') + '/o?' + urllib.parse.urlencode(query)
        headers = {'Authorization': 'Bearer ' + self.api.token_provider(),
                   'Content-Type': 'multipart/related; boundary=' + boundary,
                   'Content-Length': str(len(prefix) + size + len(suffix))}
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

    def download(self, key, path, *, generation=None, max_bytes=256 * 1024 * 1024):
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
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > size or count > max_bytes:
                        raise ValueError('GCS object exceeds declared size')
                    digest.update(chunk)
                    output.write(chunk)
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
