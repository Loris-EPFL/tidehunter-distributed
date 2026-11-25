"""The bucket-mailbox executor against a real worker and a real subprocess."""
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone

import gcp_runtime
import gcp_worker as worker
import host_exec
import mailbox_exec


class Error(Exception):
    def __init__(self, status):
        self.status = status


class Store:
    """One bucket: immutable generations for requests, receipts and blobs."""

    def __init__(self):
        self.objects = {}
        self.next_generation = 1

    def put(self, key, data, *, content_type='application/octet-stream',
            if_generation_match=None, **kwargs):
        if if_generation_match is not None and key in self.objects:
            raise Error(412)
        data = bytes(data)
        generation = str(self.next_generation)
        self.next_generation += 1
        self.objects[key] = {'generation': generation, 'data': data,
                             'sha256': hashlib.sha256(data).hexdigest()}
        return {'generation': generation, 'size': len(data),
                'metadata': {'sha256': self.objects[key]['sha256']}}

    def head(self, key):
        if key not in self.objects:
            raise Error(404)
        record = self.objects[key]
        return {'name': key, 'generation': record['generation'], 'size': len(record['data']),
                'metadata': {'sha256': record['sha256']}}

    def get(self, key, *, generation=None, max_bytes=256 * 1024 * 1024):
        meta = self.head(key)
        if generation is not None and str(generation) != meta['generation']:
            raise Error(404)
        data = self.objects[key]['data']
        if len(data) > max_bytes:
            raise ValueError('oversized')
        return data

    def upload(self, key, path, *, content_type='application/octet-stream',
               if_generation_match=None, metadata=None, deadline_check=None):
        return self.put(key, Path(path).read_bytes(), content_type=content_type,
                        if_generation_match=if_generation_match)

    def download(self, key, path, *, generation=None, max_bytes=256 * 1024 * 1024,
                 deadline_check=None):
        Path(path).write_bytes(self.get(key, generation=generation, max_bytes=max_bytes))

    def list(self, prefix):
        return iter([self.head(key) for key in sorted(self.objects) if key.startswith(prefix)])

    def delete(self, key, *, if_generation_match):
        del self.objects[key]


class Api:
    def __init__(self):
        self.store = Store()

    def objects(self, bucket):
        return self.store


RUN = 'bench-' + '2' * 16
SOURCE = 'b' * 64
FLEET = [{'index': index, 'role': 'coordinator' if index == 0 else 'owner',
          'name': f'{RUN}-{index}', 'id': str(200 + index), 'zone': 'us-central1-a',
          'internal_ip': f'10.0.0.{index + 1}', 'rdma_ip': f'10.1.0.{index + 1}'}
         for index in range(3)]


def deadline(minutes=30):
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


class Campaign(unittest.TestCase):
    """One laptop executor, one bucket, and the real worker for every request."""

    def setUp(self):
        self.api = Api()
        self.store = self.api.store
        self.client = mailbox_exec.MailboxExec(
            self.api, bucket='bucket', run_id=RUN, fleet=FLEET, source_sha256=SOURCE,
            deadline_utc=deadline(), poll_seconds=0)
        self.gate = tempfile.TemporaryDirectory()
        self.addCleanup(self.gate.cleanup)
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.config = {'run_id': RUN, 'source_sha256': SOURCE,
                       'deadline_utc': self.client.deadline_utc,
                       'package_root': '/mnt/tidehunter/' + RUN + '/package'}
        patcher = mock.patch.object(worker, 'scratch_root',
                                    side_effect=lambda *_: Path(self.scratch.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        # Deliver on submit so the seam methods below block only on the worker.
        self.delivering = True
        submit = self.client.mailbox.submit

        def deliver(*args, **kwargs):
            reference = submit(*args, **kwargs)
            if self.delivering:
                self.serve()
            return reference
        self.client.mailbox.submit = deliver

    def serve(self, limit=16):
        served = 0
        while served < limit:
            pending = [item for item in self.store.list(f'runs/{RUN}/mailbox/')
                       if '/requests/' in item['name']]
            if not pending:
                return served
            for item in pending:
                host = FLEET[int(item['name'].split('/')[3]) - 200]
                try:
                    worker.run_request(self.config, host, self.store, item['name'],
                                       item['generation'], Path(self.gate.name))
                except worker.InvalidRequest as exc:
                    # The guest loop publishes the refusal as a terminal receipt.
                    worker.publish_rejection(self.store, self.config, host, item['name'],
                                             item['generation'], exc)
                served += 1
        return served

    # ---- commands ------------------------------------------------------
    def test_one_request_per_command_carries_the_admitted_shell(self):
        code, out, err = self.client.execute(FLEET[1]['name'], 'printf campaign')
        self.assertEqual((code, out, err), (0, b'campaign', b''))
        request = self.request()
        self.assertEqual(request['schema_version'], 1)
        self.assertEqual(request['argv'], ['/bin/sh', '-c', 'printf campaign'])
        self.assertEqual(request['instance_id'], FLEET[1]['id'])
        self.assertEqual(request['index'], 1)
        self.assertEqual(request['source_sha256'], SOURCE)
        self.assertEqual(request['deadline_utc'], self.client.deadline_utc)
        self.assertNotIn('stdin', request)
        self.assertEqual(request['stdout_object'],
                         f'runs/{RUN}/transfer/{FLEET[1]["id"]}/'
                         f'{request["operation_id"]}/stdout')
        self.assertGreaterEqual(request['timeout_seconds'], 1)

    def test_exit_status_and_streams_survive_the_receipt(self):
        code, out, err = self.client.execute(FLEET[0]['name'],
                                             'echo one; echo two 1>&2; exit 7', )
        self.assertEqual(code, 7)
        self.assertEqual(out, b'one\n')
        self.assertEqual(err, b'two\n')
        receipt = next(iter(receipt for key, receipt in self.receipts().items()))
        self.assertEqual(receipt['exit_code'], 7)
        self.assertEqual(receipt['operation_id'], self.request()['operation_id'])

    def test_host_may_be_named_by_name_or_internal_address(self):
        self.client.execute(FLEET[2]['name'], 'true')
        self.client.execute(FLEET[2]['internal_ip'], 'true')
        requests = self.requests()
        self.assertEqual(len(requests), 2)
        self.assertEqual({item['instance_id'] for item in requests}, {FLEET[2]['id']})

    def test_unknown_host_is_refused_before_submission(self):
        with self.assertRaises(mailbox_exec.MailboxUnknownHost):
            self.client.execute('bench-other-0', 'true')
        self.assertEqual(self.requests(), [])

    # ---- files ---------------------------------------------------------
    def test_put_file_travels_through_the_bucket(self):
        target = Path(self.scratch.name) / 'guest/owner.tar'
        payload = bytes(range(256)) * 4096
        self.client.put_file(FLEET[1]['name'], target, payload)
        self.assertEqual(target.read_bytes(), payload)
        request = self.request()
        blob = request['stdin']
        self.assertEqual(blob['size'], len(payload))
        self.assertEqual(blob['object'],
                         f'runs/{RUN}/transfer/{FLEET[1]["id"]}/'
                         f'{request["operation_id"]}/stdin')
        self.assertEqual(self.store.get(blob['object']), payload)

    def test_get_file_reads_small_and_spilled_bytes(self):
        target = Path(self.scratch.name) / 'guest/pid'
        target.parent.mkdir(parents=True)
        target.write_text('4242\n')
        self.assertEqual(self.client.get_file(FLEET[0]['name'], target), b'4242\n')
        big = Path(self.scratch.name) / 'guest/big'
        big.write_bytes(b'x' * 1500000)
        self.assertEqual(len(self.client.read_bytes(FLEET[2]['name'], big)), 1500000)

    def test_large_stdout_spills_to_the_bucket_and_returns_every_byte(self):
        command = 'head -c 2000000 /dev/zero | tr "\\0" a'
        code, out, err = self.client.execute(FLEET[0]['name'], command)
        self.assertEqual((code, err), (0, b''))
        self.assertEqual(out, b'a' * 2000000)
        receipt = next(iter(self.receipts().values()))
        self.assertEqual(receipt['stdout'], '')
        self.assertEqual(receipt['stdout_object']['size'], 2000000)
        self.assertEqual(receipt['stdout_object']['object'],
                         f'runs/{RUN}/transfer/{FLEET[0]["id"]}/'
                         f'{receipt["operation_id"]}/stdout')

    # ---- failures ------------------------------------------------------
    def test_refused_request_raises_a_typed_failure(self):
        reference = self.client.mailbox.submit(
            0, ['/bin/sh', '-c', 'true'], operation_id='refused', timeout_seconds=30,
            cwd='/etc')
        with self.assertRaises(mailbox_exec.MailboxRefused) as caught:
            self.client.finish(FLEET[0]['name'], reference)
        self.assertIn('working directory outside run root', str(caught.exception))
        receipt = self.receipts()['refused']
        self.assertIs(receipt['rejected'], True)
        self.assertEqual(receipt['exit_code'], 126)

    def test_missing_receipt_times_out_without_running_anything(self):
        self.delivering = False
        clock = [time.time()]
        with mock.patch.object(gcp_runtime.time, 'time', side_effect=lambda: clock[0]), \
                mock.patch.object(gcp_runtime.time, 'sleep',
                                  side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
            with self.assertRaises(mailbox_exec.MailboxTimeout):
                self.client.execute(FLEET[0]['name'], 'true')
        self.assertEqual(len(self.requests()), 1)
        self.assertEqual(self.receipts(), {})

    def test_execute_uses_a_fresh_operation_each_time(self):
        self.client.execute(FLEET[0]['name'], 'true')
        self.client.execute(FLEET[0]['name'], 'true')
        self.assertEqual(len({item['operation_id'] for item in self.requests()}), 2)

    def test_run_once_keeps_one_immutable_request(self):
        self.client.run_once(FLEET[0]['name'], 'true', operation_id='fixed')
        self.client.run_once(FLEET[0]['name'], 'true', operation_id='fixed')
        self.assertEqual(len(self.requests()), 1)
        with self.assertRaises(gcp_runtime.RuntimeSafetyError):
            self.client.run_once(FLEET[0]['name'], 'false', operation_id='fixed')

    def test_a_blob_that_differs_from_its_digest_never_reaches_the_command(self):
        key = f'runs/{RUN}/transfer/{FLEET[0]["id"]}/tampered/stdin'
        self.delivering = False
        self.client.mailbox.submit(
            0, ['/bin/sh', '-c', 'cat'], operation_id='tampered', timeout_seconds=30,
            stdin={'object': key, 'sha256': 'c' * 64, 'size': 4})
        self.store.put(key, b'nope')
        with self.assertRaises(gcp_runtime.RuntimeSafetyError) as caught:
            self.serve()
        self.assertIn('differs from its admitted digest', str(caught.exception))
        self.assertEqual(self.receipts(), {})

    # ---- helpers -------------------------------------------------------
    def requests(self):
        return [json.loads(self.store.get(key)) for key in sorted(self.store.objects)
                if '/requests/' in key]

    def request(self):
        rows = self.requests()
        self.assertEqual(len(rows), 1)
        return rows[0]

    def receipts(self):
        return {key.rsplit('/', 1)[-1][:-5]: json.loads(self.store.get(key))
                for key in sorted(self.store.objects) if '/responses/' in key}


class SwapTests(unittest.TestCase):
    """The drivers swap one constructor: mailbox by default, SSH on request."""

    def args(self, **extra):
        value = {'gcloud_ssh': False, 'zone': 'us-central1-a', 'fleet_state': None,
                 'fleet_config': None, 'iap': False, 'tunnel_through_iap': False}
        value.update(extra)
        return type('Args', (), value)()

    def test_a_rehearsal_asks_for_ssh_explicitly(self):
        remote = mailbox_exec.open_executor(self.args(gcloud_ssh=True, iap=True))
        self.assertIsInstance(remote, host_exec.GcloudSsh)
        self.assertTrue(remote.iap)
        self.assertEqual(remote.argv('run-1', 'true'),
                         ['gcloud', 'compute', 'ssh', 'run-1', '--zone', 'us-central1-a',
                          '--quiet', '--command=true', '--tunnel-through-iap'])

    def test_a_campaign_without_a_fleet_state_is_refused(self):
        with self.assertRaises(SystemExit):
            mailbox_exec.open_executor(self.args())

    def test_the_mailbox_executor_is_built_from_the_fleet_state(self):
        state = {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline(),
                 'host_count': 3, 'bucket': 'bucket',
                 'instances': [{'name': f'{RUN}-{index}', 'fleet': host}
                               for index, host in enumerate(FLEET)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps(state))
            config = Path(directory) / 'config.json'
            config.write_text(json.dumps({'project_id': 'mysten-research', 'bucket': 'bucket'}))
            with mock.patch.object(mailbox_exec.gcp_api, 'GoogleApi', return_value=Api()):
                remote = mailbox_exec.open_executor(
                    self.args(fleet_state=path, fleet_config=config))
        self.assertIsInstance(remote, mailbox_exec.MailboxExec)
        self.assertEqual(remote.index_of(FLEET[2]['name']), 2)


class FleetStateTests(unittest.TestCase):
    def test_state_file_builds_the_executor(self):
        state = {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline(),
                 'host_count': 3, 'bucket': 'bucket',
                 'instances': [{'name': f'{RUN}-{index}', 'fleet': host}
                               for index, host in enumerate(FLEET)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps(state))
            client = mailbox_exec.MailboxExec.from_state(path, Api())
        self.assertEqual(client.index_of(FLEET[1]['internal_ip']), 1)

    def test_a_fleet_state_with_a_missing_host_is_refused(self):
        state = {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline(),
                 'host_count': 3, 'bucket': 'bucket',
                 'instances': [{'name': f'{RUN}-0', 'fleet': FLEET[0]}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps(state))
            with self.assertRaises(ValueError):
                mailbox_exec.MailboxExec.from_state(path, Api())

    def test_sui_spec_admits_only_bounded_shapes(self):
        self.assertEqual(
            mailbox_exec.sui_spec(2, ['native', 'tidehunter_nvme_rdma']),
            {'suite': 'sui', 'mode': 'sui', 'owners': 2,
             'backends': ['native', 'tidehunter_nvme_rdma']})
        for owners, backends in ((0, ['native']), (17, ['native']), (2, []),
                                 (2, ['nope']), (2, ['native', 'native'])):
            with self.subTest(owners=owners, backends=backends):
                with self.assertRaises(ValueError):
                    mailbox_exec.sui_spec(owners, backends)


if __name__ == '__main__':
    unittest.main()
