from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import io
import json
from pathlib import Path
import tempfile
import subprocess
import threading
import time
import urllib.error
from unittest import TestCase, mock
import gcp_manual_storage

from gcp_manual_storage import DelegatedArtifacts


class Process:
    def __init__(self, token):
        self.token = token
        self.returncode = 0
        self.pid = 12345

    def communicate(self, timeout=None):
        return self.token, b''


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class SlowProcess(Process):
    """A token process that outlives its first timeout."""

    def __init__(self, token, *, slow):
        super().__init__(token)
        self.slow = slow

    def communicate(self, timeout=None):
        self.observed = timeout
        if self.slow:
            self.slow -= 1
            raise subprocess.TimeoutExpired('gcloud', timeout)
        return self.token, b''


class DelegatedArtifactsTest(TestCase):
    def test_a_slow_impersonation_call_is_not_an_abort(self):
        # 26 November 2026: three 30 s token timeouts aborted a run after its
        # live pair had already passed. A slow token call is bounded, not fatal.
        process = SlowProcess(b'token-one', slow=1)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process) as spawn, \
                mock.patch('gcp_manual_storage.os.killpg'):
            store = DelegatedArtifacts(deadline=time.time() + 600)
            self.assertEqual(store._token(), 'token-one')
        self.assertEqual(spawn.call_count, 2)
        self.assertEqual(process.observed, gcp_manual_storage.TOKEN_SOCKET_SECONDS)
        self.assertGreater(gcp_manual_storage.TOKEN_SOCKET_SECONDS, 30)

    def test_a_token_call_that_exceeds_the_deadline_still_raises(self):
        process = SlowProcess(b'token', slow=gcp_manual_storage.TOKEN_ATTEMPTS)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process), \
                mock.patch('gcp_manual_storage.os.killpg'):
            store = DelegatedArtifacts(deadline=time.time() + 600)
            with self.assertRaises(subprocess.TimeoutExpired):
                store._token()

    def test_the_rehearsal_store_needs_no_cloud_token(self):
        with mock.patch.dict('os.environ',
                             {gcp_manual_storage.gcp_api.REHEARSAL_ENDPOINT:
                              'http://127.0.0.1:9'}), \
                mock.patch('gcp_manual_storage.subprocess.Popen') as spawn:
            store = DelegatedArtifacts(deadline=time.time() + 60)
            self.assertEqual(store._token(), 'rehearsal')
        spawn.assert_not_called()

    def test_http_requests_share_cached_guest_token(self):
        seen = []
        def open_request(request, timeout):
            seen.append(request.get_header('Authorization'))
            return Response(json.dumps({'name': 'runs/a', 'generation': '1', 'size': '0'}).encode())
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token-one')) as spawn:
            store = DelegatedArtifacts(deadline=time.time() + 60, opener=open_request)
            store.head('runs/a')
            store.head('runs/a')
        self.assertEqual(seen, ['Bearer token-one', 'Bearer token-one'])
        self.assertEqual(spawn.call_count, 1)
        for call in spawn.call_args_list:
            self.assertIn('--impersonate-service-account=bench-guest@mysten-research.iam.gserviceaccount.com', call.args[0])
            self.assertTrue(call.kwargs['start_new_session'])

    def test_http_retry_reuses_cached_delegated_token(self):
        observed = []
        def open_request(request, timeout):
            observed.append(request.get_header('Authorization'))
            if len(observed) == 1:
                raise urllib.error.HTTPError(request.full_url, 503, 'temporary', {}, io.BytesIO(b'busy'))
            return Response(b'{"name":"runs/a","generation":"1","size":"0"}')
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token-one')) as spawn, \
                mock.patch('gcp_api.time.sleep'):
            store = DelegatedArtifacts(deadline=time.time() + 60, opener=open_request)
            store.head('runs/a')
        self.assertEqual(observed, ['Bearer token-one', 'Bearer token-one'])
        spawn.assert_called_once()

    def test_get_retries_connection_closed_before_response(self):
        attempts = []
        def open_request(request, timeout):
            attempts.append(request.get_method())
            if len(attempts) == 1:
                raise http.client.RemoteDisconnected('Remote end closed connection without response')
            return Response(b'{"name":"runs/a","generation":"1","size":"0"}')
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token-one')), \
                mock.patch('gcp_api.time.sleep'):
            store = DelegatedArtifacts(deadline=time.time() + 60, opener=open_request)
            self.assertEqual(store.head('runs/a')['generation'], '1')
        self.assertEqual(attempts, ['GET', 'GET'])

    def test_concurrent_requests_singleflight_token_refresh(self):
        started = threading.Event()
        release = threading.Event()
        barrier = threading.Barrier(8)
        process = Process(b'one-token')
        def communicate(timeout=None):
            started.set()
            if not release.wait(5):
                raise AssertionError('refresh was not released')
            return b'one-token', b''
        process.communicate = communicate
        def request(store):
            barrier.wait(timeout=5)
            return store.head('runs/a')
        def open_request(request, timeout):
            return Response(b'{"name":"runs/a","generation":"1","size":"0"}')
        store = DelegatedArtifacts(deadline=time.time() + 30, opener=open_request)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process) as spawn:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(request, store) for _ in range(8)]
                try:
                    self.assertTrue(started.wait(5))
                finally:
                    release.set()
                for future in futures:
                    future.result(timeout=5)
            spawn.assert_called_once()

    def test_token_cache_expires_from_refresh_start(self):
        clock = [100.0]
        store = DelegatedArtifacts(deadline=time.time() + 60)
        with mock.patch('gcp_manual_storage.time.monotonic', side_effect=lambda: clock[0]), \
                mock.patch('gcp_manual_storage.subprocess.Popen',
                           side_effect=[Process(b'first'), Process(b'second')]) as spawn:
            self.assertEqual(store._token(), 'first')
            clock[0] += gcp_manual_storage.TOKEN_CACHE_SECONDS - 1
            self.assertEqual(store._token(), 'first')
            clock[0] += 1
            self.assertEqual(store._token(), 'second')
        self.assertEqual(spawn.call_count, 2)

    def test_failed_refresh_is_not_cached(self):
        failed = Process(b'secret-token')
        failed.returncode = 1
        store = DelegatedArtifacts(deadline=time.time() + 60)
        with mock.patch('gcp_manual_storage.subprocess.Popen',
                        side_effect=[failed, Process(b'valid-token')]) as spawn:
            with self.assertRaises(gcp_manual_storage.CredentialUnavailable):
                store._token()
            self.assertEqual(store._token(), 'valid-token')
        self.assertEqual(spawn.call_count, 2)

    def test_waiting_for_refresh_is_bounded_by_deadline(self):
        store = DelegatedArtifacts(deadline=time.time() + 0.05)
        store._token_lock.acquire()
        try:
            with mock.patch('gcp_manual_storage.subprocess.Popen') as spawn:
                with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                    store._token()
                spawn.assert_not_called()
        finally:
            store._token_lock.release()

    def test_waiter_uses_extended_mutable_deadline(self):
        store = DelegatedArtifacts(deadline=time.time() + 60)
        real_lock = store._token_lock
        class ExtendingLock:
            def __init__(self):
                self.calls = 0
            def acquire(self, timeout):
                self.calls += 1
                if self.calls == 1:
                    store.deadline = time.time() + 120
                    return False
                return real_lock.acquire(timeout=timeout)
            def release(self):
                real_lock.release()
        lock = ExtendingLock()
        store._token_lock = lock
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token')):
            self.assertEqual(store._token(), 'token')
        self.assertEqual(lock.calls, 2)

    def test_expired_deadline_rejects_cached_token(self):
        store = DelegatedArtifacts(deadline=time.time() + 60)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token')) as spawn:
            self.assertEqual(store._token(), 'token')
            store.deadline = time.time() - 1
            with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                store._token()
        spawn.assert_called_once()

    def test_deadline_checked_after_lock_on_cache_hit(self):
        store = DelegatedArtifacts(deadline=time.time() + 60)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token')):
            self.assertEqual(store._token(), 'token')
        with mock.patch.object(store, '_remaining', side_effect=[1, TimeoutError('deadline expired')]):
            with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                store._token()

    def test_deadline_checked_before_returning_cache_hit(self):
        store = DelegatedArtifacts(deadline=time.time() + 60)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=Process(b'token')):
            self.assertEqual(store._token(), 'token')
        with mock.patch.object(store, '_remaining', side_effect=[1, 1, TimeoutError('deadline expired')]):
            with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                store._token()

    def test_refresh_crossing_deadline_does_not_cache_token(self):
        store = DelegatedArtifacts(deadline=time.time() + 60)
        expired = Process(b'expired-token')
        def expire_during_refresh(timeout=None):
            store.deadline = time.time() - 1
            return b'expired-token', b''
        expired.communicate = expire_during_refresh
        with mock.patch('gcp_manual_storage.subprocess.Popen',
                        side_effect=[expired, Process(b'fresh-token')]) as spawn:
            with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                store._token()
            store.deadline = time.time() + 60
            self.assertEqual(store._token(), 'fresh-token')
        self.assertEqual(spawn.call_count, 2)

    def test_media_body_cannot_exceed_declared_size(self):
        def open_request(request, timeout):
            if 'alt=media' in request.full_url:
                return Response(b'abcd')
            return Response(b'{"name":"runs/a","generation":"1","size":"3"}')
        with mock.patch('gcp_manual_storage.subprocess.Popen', side_effect=lambda *a, **k: Process(b'token')):
            store = DelegatedArtifacts(deadline=time.time() + 60, opener=open_request)
            with self.assertRaisesRegex(ValueError, 'declared size'):
                store.get('runs/a', generation='1', max_bytes=3)

    def test_streamed_upload_gets_the_upload_socket_bound_and_other_requests_the_short_bound(self):
        payload = b'x' * 4096
        seen = []
        def open_request(request, timeout):
            seen.append((request.get_method(), timeout))
            if request.get_method() == 'POST':
                b''.join(request.data)
                return Response(json.dumps({'name': 'runs/a', 'generation': '1', 'size': str(len(payload)),
                                            'metadata': {'sha256': hashlib.sha256(payload).hexdigest()}}).encode())
            return Response(b'{"name":"runs/a","generation":"1","size":"0"}')
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch('gcp_manual_storage.subprocess.Popen', side_effect=lambda *a, **k: Process(b'token')):
            path = Path(directory) / 'package.tar'
            path.write_bytes(payload)
            store = DelegatedArtifacts(deadline=time.time() + 3600, opener=open_request)
            store.upload('runs/a', path, if_generation_match=0)
            store.head('runs/a')
        self.assertEqual(seen, [('POST', gcp_manual_storage.UPLOAD_SOCKET_SECONDS),
                                ('GET', gcp_manual_storage.SOCKET_SECONDS)])

    def test_unbounded_json_read_has_size_limit(self):
        response = gcp_manual_storage._DeadlineResponse(Response(b'01234'), lambda: 30, max_json_bytes=4)
        with self.assertRaisesRegex(ValueError, 'bounded JSON'):
            response.read()

    def test_deadline_crossed_during_json_read_closes_response(self):
        raw = Response(b'data')
        check = mock.Mock(side_effect=[1, TimeoutError('expired')])
        with self.assertRaises(TimeoutError), gcp_manual_storage._DeadlineResponse(raw, check) as response:
            response.read()
        self.assertTrue(raw.closed)

    def test_bad_download_keeps_existing_target(self):
        def open_request(request, timeout):
            if 'alt=media' in request.full_url:
                return Response(b'abcd')
            return Response(b'{"name":"runs/a","generation":"1","size":"3"}')
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch('gcp_manual_storage.subprocess.Popen', side_effect=lambda *a, **k: Process(b'token')):
            target = Path(directory) / 'existing'
            target.write_bytes(b'old')
            store = DelegatedArtifacts(deadline=time.time() + 60, opener=open_request)
            with self.assertRaises(ValueError):
                store.download('runs/a', target, generation='1', max_bytes=3)
            self.assertEqual(target.read_bytes(), b'old')

    def test_expired_deadline_never_spawns_a_token_process(self):
        # The provider must check before spawning, not after.
        store = DelegatedArtifacts(deadline=time.time() - 1)
        with mock.patch('gcp_manual_storage.subprocess.Popen') as spawn:
            with self.assertRaises(TimeoutError):
                store.head('runs/a')
            spawn.assert_not_called()

    def test_nonzero_auth_failure_is_not_retried_and_hides_sdk_output(self):
        process = Process(b'secret-token')
        process.returncode = 1
        process.communicate = mock.Mock(return_value=(b'secret-token', b'secret-url'))
        opener = mock.Mock()
        stderr = io.StringIO()
        store = DelegatedArtifacts(deadline=time.time() + 60, opener=opener)
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process) as spawn, \
                mock.patch('sys.stderr', stderr):
            with self.assertRaises(gcp_manual_storage.CredentialUnavailable) as error:
                store.head('runs/a')
        self.assertEqual(str(error.exception), 'guest SDK delegation failed (exit status 1)')
        self.assertNotIn('secret', repr(error.exception))
        self.assertEqual(stderr.getvalue(), '')
        spawn.assert_called_once()
        self.assertEqual(spawn.call_args.kwargs['stderr'], subprocess.DEVNULL)
        opener.assert_not_called()

    def test_token_timeout_then_success_sends_mutation_once_and_reaps(self):
        process = Process(b'')
        process.communicate = mock.Mock(side_effect=[
            subprocess.TimeoutExpired('gcloud', 30, output=b'secret-token', stderr=b'secret-url'),
            (b'', b'')])
        opener = mock.Mock(return_value=Response(b''))
        with mock.patch('gcp_manual_storage.subprocess.Popen',
                        side_effect=[process, Process(b'valid-token')]) as spawn, \
                mock.patch('gcp_manual_storage.os.killpg') as kill:
            store = DelegatedArtifacts(deadline=time.time() + 90, opener=opener)
            store.delete('runs/a', if_generation_match='1')
        self.assertEqual(spawn.call_count, 2)
        kill.assert_called_once_with(process.pid, gcp_manual_storage.signal.SIGKILL)
        self.assertEqual(process.communicate.call_count, 2)
        self.assertEqual(process.communicate.call_args_list[1], mock.call())
        opener.assert_called_once()
        request = opener.call_args.args[0]
        self.assertEqual(request.get_method(), 'DELETE')
        self.assertEqual(request.get_header('Authorization'), 'Bearer valid-token')

    def test_timeout_deadline_exhaustion_does_not_spawn_or_request_again(self):
        process = Process(b'')
        clock = [0]
        def timeout_then_reap(timeout=None):
            if timeout is not None:
                clock[0] = 100
                raise subprocess.TimeoutExpired('gcloud', 30)
            return b'', b''
        process.communicate = mock.Mock(side_effect=timeout_then_reap)
        opener = mock.Mock()
        store = DelegatedArtifacts(deadline=100, opener=opener)
        with mock.patch('gcp_manual_storage.time.time', side_effect=lambda: clock[0]), \
                mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process) as spawn, \
                mock.patch('gcp_manual_storage.os.killpg') as kill:
            with self.assertRaisesRegex(TimeoutError, 'deadline expired'):
                store.head('runs/a')
        spawn.assert_called_once()
        kill.assert_called_once_with(process.pid, gcp_manual_storage.signal.SIGKILL)
        self.assertEqual(process.communicate.call_count, 2)
        opener.assert_not_called()

    def test_repeated_timeouts_are_bounded_and_output_is_not_exposed(self):
        processes = []
        for pid in range(3):
            process = Process(b'')
            process.pid = pid + 1
            process.communicate = mock.Mock(side_effect=[
                subprocess.TimeoutExpired('hostile-secret-url', 30, output=b'secret-token',
                                          stderr=b'hostile-secret-url'), (b'', b'')])
            processes.append(process)
        opener = mock.Mock()
        with mock.patch('gcp_manual_storage.subprocess.Popen', side_effect=processes) as spawn, \
                mock.patch('gcp_manual_storage.os.killpg') as kill:
            store = DelegatedArtifacts(deadline=time.time() + 120, opener=opener)
            with self.assertRaises(subprocess.TimeoutExpired) as error:
                store.head('runs/a')
        self.assertEqual(spawn.call_count, 3)
        self.assertEqual(kill.call_count, 3)
        self.assertTrue(all(p.communicate.call_count == 2 for p in processes))
        self.assertIsNone(error.exception.output)
        self.assertIsNone(error.exception.stderr)
        self.assertNotIn('secret', str(error.exception))
        opener.assert_not_called()

    def test_invalid_non_ascii_token_does_not_expose_bytes(self):
        process = Process(b'secret-token\xff')
        with mock.patch('gcp_manual_storage.subprocess.Popen', return_value=process):
            store = DelegatedArtifacts(deadline=time.time() + 60)
            with self.assertRaises(RuntimeError) as error:
                store.head('runs/a')
        self.assertEqual(str(error.exception), 'guest SDK returned an invalid token')
        self.assertTrue(error.exception.__suppress_context__)
