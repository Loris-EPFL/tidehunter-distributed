"""The bucket-mailbox executor against a real worker and a real subprocess."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import sys
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone

import campaign_upload
import gcp_api
import gcp_manual_storage
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
            if_generation_match=None, metadata=None, **kwargs):
        # GCS precondition: 0 means "must not exist"; any other value must be
        # the current generation. A match replaces the object.
        if if_generation_match is not None:
            current = self.objects.get(key)
            expected = str(if_generation_match)
            if expected == '0':
                if current is not None:
                    raise Error(412)
            elif current is None or current['generation'] != expected:
                raise Error(412)
        data = bytes(data)
        generation = str(self.next_generation)
        self.next_generation += 1
        attributes = dict(metadata or {})
        attributes['sha256'] = hashlib.sha256(data).hexdigest()
        self.objects[key] = {'generation': generation, 'data': data,
                             'sha256': attributes['sha256'], 'metadata': attributes}
        return {'generation': generation, 'size': len(data), 'metadata': attributes}

    def head(self, key):
        if key not in self.objects:
            raise Error(404)
        record = self.objects[key]
        return {'name': key, 'generation': record['generation'], 'size': len(record['data']),
                'metadata': record['metadata']}

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
                        if_generation_match=if_generation_match, metadata=metadata)

    def download(self, key, path, *, generation=None, max_bytes=256 * 1024 * 1024,
                 deadline_check=None):
        Path(path).write_bytes(self.get(key, generation=generation, max_bytes=max_bytes))

    def list(self, prefix):
        return iter([self.head(key) for key in sorted(self.objects) if key.startswith(prefix)])

    def copy(self, source, destination, *, if_generation_match=None):
        if if_generation_match == 0 and destination in self.objects:
            raise Error(412)
        record = self.objects[source]
        generation = str(self.next_generation)
        self.next_generation += 1
        self.objects[destination] = {'generation': generation, 'data': record['data'],
                                     'sha256': record['sha256'],
                                     'metadata': dict(record['metadata'])}
        return {'name': destination, 'generation': generation,
                'size': len(record['data']), 'metadata': record['metadata']}

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
        # The guest interpreter is absent on a workstation; the exit status and
        # stream contract is what this test exercises.
        patcher = mock.patch.object(gcp_runtime, 'GUEST_PYTHON', sys.executable)
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

    # ---- campaign files and the orchestrator ---------------------------
    def test_send_file_streams_one_bucket_blob(self):
        target = Path(self.scratch.name) / 'guest/bundle.bin'
        source = Path(self.scratch.name) / 'bundle.bin'
        source.write_bytes(b'x' * 100000)
        self.client.send_file(FLEET[1]['name'], target, source)
        self.assertEqual(target.read_bytes(), source.read_bytes())
        request = self.request()
        blob = request['stdin']
        self.assertEqual(blob['sha256'], hashlib.sha256(source.read_bytes()).hexdigest())
        self.assertEqual(blob['size'], 100000)
        self.assertEqual(self.store.get(blob['object']), source.read_bytes())

    def test_stage_tree_copies_a_directory_through_the_bucket(self):
        build = Path(self.scratch.name) / 'build'
        (build / 'binaries').mkdir(parents=True)
        (build / 'build-manifest.json').write_text('{"protocol": 9}')
        (build / 'binaries/sui-node-native').write_bytes(b'binary')
        guest = Path(self.scratch.name) / 'guest/build'
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        self.assertEqual((guest / 'build-manifest.json').read_text(), '{"protocol": 9}')
        self.assertEqual((guest / 'binaries/sui-node-native').read_bytes(), b'binary')

    def test_tree_digest_is_content_addressed_not_time_addressed(self):
        first = Path(self.scratch.name) / 'tree-a'
        second = Path(self.scratch.name) / 'tree-b'
        for root in (first, second):
            (root / 'binaries').mkdir(parents=True)
            (root / 'manifest.json').write_text('{"protocol": 9}')
            (root / 'binaries/tool').write_bytes(b'binary')
        self.assertEqual(mailbox_exec.tree_digest(first), mailbox_exec.tree_digest(second))
        os.utime(second / 'manifest.json', (1, 1))
        self.assertEqual(mailbox_exec.tree_digest(first), mailbox_exec.tree_digest(second))
        (second / 'manifest.json').write_text('{"protocol": 8}')
        self.assertNotEqual(mailbox_exec.tree_digest(first), mailbox_exec.tree_digest(second))

    def test_a_tree_is_staged_once_per_run_and_reused_by_the_next_cell(self):
        build = Path(self.scratch.name) / 'build'
        (build / 'binaries').mkdir(parents=True)
        (build / 'build-manifest.json').write_text('{"protocol": 9}')
        (build / 'binaries/sui-node-native').write_bytes(b'binary')
        guest = Path(self.scratch.name) / 'guest/build'
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        staged = [key for key in self.store.objects if key.startswith(f'runs/{RUN}/staging/')]
        self.assertEqual(len(staged), 1)
        self.assertEqual([key for key in self.store.objects if key.endswith('/stdin')], [])
        requests = len(self.requests())
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        # The guest reports the same digest, so the second cell transfers nothing.
        self.assertEqual(len(self.requests()), requests + 1)
        self.assertEqual([key for key in self.store.objects
                          if key.startswith(f'runs/{RUN}/staging/')], staged)
        self.assertEqual((guest / 'binaries/sui-node-native').read_bytes(), b'binary')
        self.assertEqual((guest.parent / '.staged/build').read_text(),
                         mailbox_exec.tree_digest(build))

    def test_a_changed_tree_is_staged_under_a_new_digest_key(self):
        build = Path(self.scratch.name) / 'build'
        build.mkdir()
        (build / 'build-manifest.json').write_text('{"protocol": 9}')
        guest = Path(self.scratch.name) / 'guest/build'
        before = mailbox_exec.tree_digest(build)
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        (build / 'build-manifest.json').write_text('{"protocol": 8}')
        after = mailbox_exec.tree_digest(build)
        self.assertNotEqual(before, after)
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        self.assertEqual(sorted(key for key in self.store.objects
                                if key.startswith(f'runs/{RUN}/staging/')),
                         sorted([f'runs/{RUN}/staging/build/{before}.tar.gz',
                                 f'runs/{RUN}/staging/build/{after}.tar.gz']))
        self.assertEqual((guest / 'build-manifest.json').read_text(), '{"protocol": 8}')

    def test_a_guest_without_the_tree_receives_a_server_side_copy(self):
        build = Path(self.scratch.name) / 'build'
        build.mkdir()
        (build / 'build-manifest.json').write_text('{"protocol": 9}')
        guest = Path(self.scratch.name) / 'guest/build'
        self.client.stage_tree(FLEET[0]['name'], build, guest)
        staged = [key for key in self.store.objects if key.startswith(f'runs/{RUN}/staging/')]
        # A re-created fleet keeps the run id but starts with an empty local SSD.
        shutil.rmtree(guest.parent)
        uploads = []
        original = self.store.upload

        def counted(key, path, **kwargs):
            uploads.append(key)
            return original(key, path, **kwargs)

        with mock.patch.object(self.store, 'upload', side_effect=counted):
            self.client.stage_tree(FLEET[0]['name'], build, guest)
        # The workstation uploads nothing: the staged object is copied in GCS.
        self.assertEqual(uploads, [])
        self.assertEqual([key for key in self.store.objects
                          if key.startswith(f'runs/{RUN}/staging/')], staged)
        self.assertEqual((guest / 'build-manifest.json').read_text(), '{"protocol": 9}')

    def test_run_driver_executes_the_admitted_script_on_the_host(self):
        script = Path(self.scratch.name) / 'driver.py'
        script.write_text('import json,sys\nprint(json.dumps({"argv": sys.argv[1:]}))\n')
        code, out, err = self.client.run_driver(
            FLEET[2]['name'], str(script), ['--owners', 'H1,H2', '--cell-id', 'S1 0'])
        self.assertEqual((code, err), (0, b''))
        self.assertEqual(json.loads(out), {'argv': ['--owners', 'H1,H2', '--cell-id', 'S1 0']})
        request = self.request()
        self.assertIn(shlex.quote(sys.executable) + ' ' + shlex.quote(str(script)),
                      request['argv'][2])
        self.assertEqual(request['stdout_object'],
                         f'runs/{RUN}/transfer/{FLEET[2]["id"]}/'
                         f'{request["operation_id"]}/stdout')

    def test_run_driver_returns_a_chatty_stdout_from_the_blob(self):
        script = Path(self.scratch.name) / 'chatty.py'
        script.write_text('import sys\nsys.stdout.write("y" * 1200000)\n')
        code, out, _ = self.client.run_driver(FLEET[0]['name'], str(script))
        self.assertEqual(code, 0)
        self.assertEqual(out, b'y' * 1200000)
        receipt = next(iter(self.receipts().values()))
        self.assertEqual(receipt['stdout'], '')
        self.assertEqual(receipt['stdout_object']['size'], 1200000)

    def test_run_driver_reports_a_failing_orchestrator(self):
        script = Path(self.scratch.name) / 'failing.py'
        script.write_text('import sys\nprint("boom", file=sys.stderr)\nsys.exit(3)\n')
        code, out, err = self.client.run_driver(FLEET[0]['name'], str(script))
        self.assertEqual(code, 3)
        self.assertEqual(out, b'')
        self.assertIn(b'boom', err)

    def test_an_absolute_script_is_required(self):
        for script in ('', 'driver.py', None):
            with self.subTest(script=script):
                with self.assertRaises(ValueError):
                    self.client.run_driver(FLEET[0]['name'], script)

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

    def test_ssh_without_a_zone_is_refused(self):
        with self.assertRaises(SystemExit):
            mailbox_exec.open_executor(self.args(gcloud_ssh=True, zone=None))

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
            with mock.patch.object(mailbox_exec.gcp_api, 'require_paid_credentials',
                                   return_value={'step': 'test', 'issued_epoch': 0}), \
                    mock.patch.object(mailbox_exec.gcp_api, 'campaign_client',
                                      return_value=Api()):
                remote = mailbox_exec.open_executor(
                    self.args(fleet_state=path, fleet_config=config))
        self.assertIsInstance(remote, mailbox_exec.MailboxExec)
        self.assertEqual(remote.index_of(FLEET[2]['name']), 2)


class PublishTests(unittest.TestCase):
    """The package and runtime objects the guest bootstrap waits for."""

    def config(self):
        return {'project_id': 'mysten-research', 'project_number': '514867804006',
                'bucket': 'bucket', 'machine_type': 'z4m-highmem-16'}

    def state(self):
        return {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline()}

    def receipt(self, directory):
        archive = Path(directory) / 'bundle.tar.gz'
        archive.write_bytes(b'package archive')
        return {'source_sha256': SOURCE, 'source_revision': 'd' * 40,
                'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
                'manifest_sha256': 'e' * 64, 'archive_path': str(archive),
                'nvmet_fix': {'module_sha256': mailbox_exec.NVMET_FIX_MODULE_SHA256}}

    def admit(self, directory):
        seen = {}

        def admit(path, revision, spec):
            seen['path'], seen['revision'], seen['spec'] = path, revision, spec
            return self.receipt(directory), '/source'
        return seen, mock.patch.object(mailbox_exec.manual_campaign, '_admit_package',
                                       side_effect=admit)

    def test_a_sui_package_is_admitted_as_the_focused_package(self):
        store = Store()
        spec = mailbox_exec.sui_spec(2, ['native'])
        with tempfile.TemporaryDirectory() as directory:
            seen, admit = self.admit(directory)
            with admit:
                runtime, reference = mailbox_exec.publish(
                    store, self.config(), self.state(), directory, 'd' * 40, spec, FLEET,
                    guest_roots=['/mnt/tidehunter/sui'])
        self.assertEqual(seen['spec'], dict(spec, mode='focused'))
        self.assertEqual(seen['revision'], 'd' * 40)
        self.assertEqual(runtime['spec'], spec)
        self.assertEqual(runtime['guest_roots'], ['/mnt/tidehunter/sui'])
        self.assertEqual(runtime['package']['object_key'], f'runs/{RUN}/package.tar')
        self.assertEqual(runtime['package']['generation'], '1')
        self.assertEqual(reference['object'], f'runs/{RUN}/runtime.json')
        self.assertEqual(json.loads(store.get(f'runs/{RUN}/runtime.json')), runtime)

    def test_a_retried_publish_reuses_an_identical_package_and_runtime(self):
        store, state = Store(), self.state()
        spec = mailbox_exec.sui_spec(2, ['native'])
        with tempfile.TemporaryDirectory() as directory:
            seen, admit = self.admit(directory)
            with admit:
                first, _ = mailbox_exec.publish(store, self.config(), state, directory,
                                                'd' * 40, spec, FLEET)
                second, reference = mailbox_exec.publish(store, self.config(), state,
                                                         directory, 'd' * 40, spec, FLEET)
        self.assertEqual(first, second)
        self.assertEqual(reference['generation'],
                         json.loads(store.get(f'runs/{RUN}/runtime.json')) == first
                         and store.head(f'runs/{RUN}/runtime.json')['generation'])
        self.assertEqual([key for key in store.objects if key.endswith('runtime.json')],
                         [f'runs/{RUN}/runtime.json'])
        self.assertEqual([key for key in store.objects if key.endswith('package.tar')],
                         [f'runs/{RUN}/package.tar'])

    def test_a_changed_source_replaces_the_previous_runtime_and_gate(self):
        store, state = Store(), self.state()
        spec = mailbox_exec.sui_spec(2, ['native'])
        with tempfile.TemporaryDirectory() as directory:
            seen, admit = self.admit(directory)
            with admit:
                mailbox_exec.publish(store, self.config(), state, directory,
                                     'd' * 40, spec, FLEET)
                mailbox_exec.release(store, state, FLEET[0], 'first script')
                changed = dict(state, source_sha256='c' * 64)
                mailbox_exec.release(store, changed, FLEET[0], 'second script')
        gate = json.loads(store.get(f'runs/{RUN}/qualification/{FLEET[0]["name"]}/gate.json'))
        self.assertEqual(gate['source_sha256'], 'c' * 64)
        self.assertEqual(gate['sha256'], hashlib.sha256(b'second script').hexdigest())
        # The second release replaced the object; the store hands out one new
        # generation for every write, so the gate is no longer the first copy.
        self.assertGreater(int(store.head(
            f'runs/{RUN}/qualification/{FLEET[0]["name"]}/gate.json')['generation']), 1)


class CoordinatorTests(unittest.TestCase):
    """The orchestrator runs on H0; the workstation only stages and relays."""

    def state(self, directory):
        state = {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline(),
                 'host_count': 3, 'bucket': 'bucket',
                 'instances': [{'name': f'{RUN}-{index}', 'fleet': host}
                               for index, host in enumerate(FLEET)]}
        path = Path(directory) / 'state.json'
        path.write_text(json.dumps(state))
        config = Path(directory) / 'config.json'
        config.write_text(json.dumps({'project_id': 'mysten-research', 'bucket': 'bucket'}))
        return path, config

    def test_the_coordinator_is_the_first_host(self):
        with tempfile.TemporaryDirectory() as directory:
            path, _ = self.state(directory)
            self.assertEqual(mailbox_exec.coordinator_of(path), FLEET[0]['name'])

    def test_a_state_without_a_coordinator_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps({'run_id': RUN, 'host_count': 0, 'instances': []}))
            with self.assertRaises(ValueError):
                mailbox_exec.coordinator_of(path)

    def test_rewrite_replaces_only_named_flag_values(self):
        argv = ['--build', '/local/build', '--cell', '--build-like', '--', '--build', '/run.py-x']
        self.assertEqual(mailbox_exec.rewrite(argv, {'--build': '/guest/build'}),
                         ['--build', '/guest/build', '--cell', '--build-like', '--',
                          '--build', '/run.py-x'])

    def test_dispatch_stages_the_inputs_and_runs_the_package_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            path, config = self.state(directory)
            remote = mock.Mock(spec=mailbox_exec.MailboxExec)
            remote.run_driver.return_value = (0, b'{"complete": true}\n', b'')
            args = type('Args', (), {'fleet_state': path, 'fleet_config': config,
                                     'coordinator_seconds': 60})()
            argv = ['manual_sui.py', '--build', '/local/build', '--fixture', '/local/fixture',
                    '--output', '/local/cell', '--on-coordinator', '--', '--checkpoints', '4']
            with mock.patch.object(mailbox_exec, 'open_executor', return_value=remote), \
                    mock.patch.object(mailbox_exec.sys, 'argv', argv):
                code = mailbox_exec.dispatch(
                    args, coordinator=FLEET[0]['name'],
                    script_relative='experiments/gcp-rdma/manual_sui.py',
                    trees=[('build', '/local/build'), ('fixture', '/local/fixture')],
                    values={'--build': 'build', '--fixture': 'fixture',
                            '--output': Path('out/cell')})
        self.assertEqual(code, 0)
        campaign = Path(f'/mnt/tidehunter/{RUN}/campaign')
        self.assertEqual(remote.stage_tree.call_args_list,
                         [mock.call(FLEET[0]['name'], '/local/build', campaign / 'build'),
                          mock.call(FLEET[0]['name'], '/local/fixture', campaign / 'fixture')])
        self.assertIn(mock.call(FLEET[0]['name'], campaign / 'fleet-state.json', path),
                      remote.send_file.call_args_list)
        self.assertIn(mock.call(FLEET[0]['name'], campaign / 'config.json', config),
                      remote.send_file.call_args_list)
        coordinator, script, guest_argv = remote.run_driver.call_args.args[:3]
        self.assertEqual(coordinator, FLEET[0]['name'])
        self.assertEqual(script,
                         f'/mnt/tidehunter/{RUN}/package/source/experiments/gcp-rdma/manual_sui.py')
        self.assertEqual(remote.run_driver.call_args.kwargs['timeout_seconds'], 60)
        expected = ['--build', str(campaign / 'build'),
                    '--fixture', str(campaign / 'fixture'),
                    '--output', str(campaign / 'out/cell'),
                    '--on-coordinator', '--guest-token',
                    '--fleet-state', str(campaign / 'fleet-state.json'),
                    '--fleet-config', str(campaign / 'config.json'),
                    '--', '--checkpoints', '4']
        self.assertEqual(guest_argv, expected)


class GuestIdentityTests(unittest.TestCase):
    """On the coordinator the campaign uses the guest identity directly."""

    def test_open_store_delegates_on_the_workstation_only(self):
        with mock.patch.object(gcp_api, 'GoogleApi') as api:
            store = campaign_upload.open_store('gs://mysten-research-bench-artifacts/campaign')
            self.assertIs(store[0], api.return_value.impersonated.return_value.objects.return_value)
            self.assertIsNone(api.call_args.kwargs.get('token_provider'))
            api.return_value.impersonated.assert_called_once()
        with mock.patch.object(gcp_api, 'GoogleApi') as api:
            campaign_upload.open_store('gs://mysten-research-bench-artifacts/campaign', guest=True)
            self.assertIs(api.call_args.kwargs['token_provider'], gcp_api.metadata_token)
            api.return_value.impersonated.assert_not_called()

    def test_the_delegated_store_takes_the_metadata_token_on_a_guest(self):
        with mock.patch.object(gcp_manual_storage, 'GoogleApi') as api:
            gcp_manual_storage.DelegatedArtifacts('mysten-research-bench-artifacts',
                                                  deadline=time.time() + 60, guest=True)
            self.assertIs(api.call_args.kwargs['token_provider'], gcp_api.metadata_token)
        with mock.patch.object(gcp_manual_storage, 'GoogleApi') as api:
            gcp_manual_storage.DelegatedArtifacts('mysten-research-bench-artifacts',
                                                  deadline=time.time() + 60)
            self.assertNotEqual(api.call_args.kwargs['token_provider'], gcp_api.metadata_token)


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

    def test_a_fleet_state_without_a_bucket_needs_one(self):
        state = {'run_id': RUN, 'source_sha256': SOURCE, 'deadline': deadline(),
                 'host_count': 3,
                 'instances': [{'name': f'{RUN}-{index}', 'fleet': host}
                               for index, host in enumerate(FLEET)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps(state))
            with self.assertRaises(ValueError):
                mailbox_exec.MailboxExec.from_state(path, Api())
            self.assertEqual(mailbox_exec.MailboxExec.from_state(
                path, Api(), bucket='bucket').run_id, RUN)

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
