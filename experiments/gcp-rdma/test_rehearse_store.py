"""The rehearsal store against the real campaign client and the real HTTP path.

The free rehearsal replaces only the service behind `gcp_api.ObjectStore`. This
test proves that the replacement keeps the contract the campaign depends on:
generations, `ifGenerationMatch` preconditions, SHA-256 metadata, streaming
uploads, server-side copies, listing and media reads.
"""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gcp_api
import rehearse_store


class LocalStore(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.server = rehearse_store.Server(self.temp.name, 'bucket',
                                            host='127.0.0.1', port=0)
        self.server.__enter__()
        self.addCleanup(self.server.__exit__, None, None, None)
        self.endpoint = 'http://127.0.0.1:' + str(self.server.port)
        patcher = mock.patch.dict('os.environ',
                                  {gcp_api.REHEARSAL_ENDPOINT: self.endpoint})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.store = gcp_api.GoogleApi('mysten-research').objects('bucket')

    def test_put_read_and_generation_metadata(self):
        data = b'{"complete": true}\n'
        receipt = self.store.put('runs/a/object.json', data,
                                 content_type='application/json', if_generation_match=0)
        self.assertEqual(receipt['metadata']['sha256'], hashlib.sha256(data).hexdigest())
        self.assertEqual(int(receipt['size']), len(data))
        head = self.store.head('runs/a/object.json')
        self.assertEqual(head['generation'], '1')
        self.assertEqual(self.store.get('runs/a/object.json'), data)
        self.assertEqual(self.store.get('runs/a/object.json', generation='1'), data)

    def test_an_unconditional_create_cannot_overwrite(self):
        self.store.put('runs/a/object.json', b'first', if_generation_match=0)
        with self.assertRaises(gcp_api.ApiError) as raised:
            self.store.put('runs/a/object.json', b'second', if_generation_match=0)
        self.assertEqual(raised.exception.status, 412)
        self.assertEqual(self.store.get('runs/a/object.json'), b'first')

    def test_a_matching_generation_replaces_the_object(self):
        self.store.put('runs/a/object.json', b'first', if_generation_match=0)
        receipt = self.store.put('runs/a/object.json', b'second',
                                 if_generation_match='1')
        self.assertEqual(receipt['generation'], '2')
        self.assertEqual(self.store.get('runs/a/object.json'), b'second')
        with self.assertRaises(gcp_api.ApiError) as raised:
            self.store.put('runs/a/object.json', b'third', if_generation_match='1')
        self.assertEqual(raised.exception.status, 412)

    def test_a_streamed_upload_keeps_the_client_sha256(self):
        path = Path(self.temp.name) / 'bundle.bin'
        path.write_bytes(b'x' * (3 * 1024 * 1024 + 7))
        receipt = self.store.upload('runs/a/package.tar', path, if_generation_match=0)
        self.assertEqual(receipt['metadata']['sha256'],
                         hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(self.store.get('runs/a/package.tar'), path.read_bytes())

    def test_listing_returns_every_object_under_the_prefix(self):
        for name in ('runs/a/one.json', 'runs/a/two.json', 'runs/b/three.json'):
            self.store.put(name, name.encode(), if_generation_match=0)
        names = [item['name'] for item in self.store.list('runs/a/')]
        self.assertEqual(names, ['runs/a/one.json', 'runs/a/two.json'])

    def test_a_server_side_copy_keeps_the_bytes_and_a_new_generation(self):
        data = b'x' * 4096
        self.store.put('runs/a/staged.tar.gz', data, if_generation_match=0,
                       metadata={'kind': 'staged-tree'})
        copied = self.store.copy('runs/a/staged.tar.gz',
                                 'runs/a/mailbox/1/stdin',
                                 if_generation_match=0)
        self.assertEqual(copied['name'], 'runs/a/mailbox/1/stdin')
        self.assertEqual(int(copied['size']), len(data))
        self.assertEqual(copied['metadata']['sha256'],
                         hashlib.sha256(data).hexdigest())
        self.assertEqual(copied['metadata']['kind'], 'staged-tree')
        self.assertNotEqual(copied['generation'],
                            self.store.head('runs/a/staged.tar.gz')['generation'])
        self.assertEqual(self.store.get('runs/a/mailbox/1/stdin'), data)
        self.assertEqual(self.store.get('runs/a/staged.tar.gz'), data)

    def test_a_copy_into_an_existing_destination_is_refused(self):
        self.store.put('runs/a/staged.tar.gz', b'first', if_generation_match=0)
        self.store.put('runs/a/transfer', b'second', if_generation_match=0)
        with self.assertRaises(gcp_api.ApiError) as raised:
            self.store.copy('runs/a/staged.tar.gz', 'runs/a/transfer',
                            if_generation_match=0)
        self.assertEqual(raised.exception.status, 412)
        self.assertEqual(self.store.get('runs/a/transfer'), b'second')

    def test_a_copy_of_a_missing_source_is_refused(self):
        with self.assertRaises(gcp_api.ApiError) as raised:
            self.store.copy('runs/a/missing', 'runs/a/transfer',
                            if_generation_match=0)
        self.assertEqual(raised.exception.status, 404)

    def test_delete_honours_its_precondition(self):
        self.store.put('runs/a/object.json', b'first', if_generation_match=0)
        with self.assertRaises(gcp_api.ApiError):
            self.store.delete('runs/a/object.json', if_generation_match='9')
        self.store.delete('runs/a/object.json', if_generation_match='1')
        with self.assertRaises(gcp_api.ApiError) as raised:
            self.store.head('runs/a/object.json')
        self.assertEqual(raised.exception.status, 404)

    def test_the_endpoint_is_the_only_rehearsal_difference(self):
        client = gcp_api.GoogleApi('mysten-research')
        self.assertEqual(client.endpoint, self.endpoint)
        self.assertEqual(client.token_provider(), 'rehearsal')
        self.assertEqual(client.objects('bucket').base,
                         self.endpoint + '/storage/v1/b/bucket/o')


class Multipart(unittest.TestCase):
    def test_a_body_with_the_boundary_bytes_inside_its_data_is_kept_whole(self):
        with tempfile.TemporaryDirectory() as directory:
            boundary = 'gcp-boundary'
            payload = b'head--' + boundary.encode() + b'tail' * 1024
            body = (b'--' + boundary.encode() + b'\r\nContent-Type: application/json\r\n\r\n'
                    + json.dumps({'name': 'k', 'metadata': {'sha256': 'x'}}).encode()
                    + b'\r\n--' + boundary.encode() + b'\r\nContent-Type: application/octet-stream\r\n\r\n'
                    + payload + b'\r\n--' + boundary.encode() + b'--\r\n')
            path = Path(directory) / 'body'
            path.write_bytes(body)
            attributes, data, size, digest = rehearse_store._multipart(
                path, boundary, Path(directory))
            try:
                self.assertEqual(attributes['name'], 'k')
                self.assertEqual(size, len(payload))
                self.assertEqual(Path(data).read_bytes(), payload)
                self.assertEqual(digest, hashlib.sha256(payload).hexdigest())
            finally:
                Path(data).unlink()


if __name__ == '__main__':
    unittest.main()
