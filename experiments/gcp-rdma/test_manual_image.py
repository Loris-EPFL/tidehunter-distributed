import datetime as dt
import json
from pathlib import Path
import tempfile
from unittest import IsolatedAsyncioTestCase, mock

import gcp_manual_image


class ImageCleanupTest(IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'state.json'
        self.config = {'project_id': 'mysten-research'}
        self.state = {'run_id': 'bench-0123456789abcdef', 'source_sha256': 'a' * 64, 'owner_nonce': 'b' * 32,
                      'image': {'name': 'bench-0123456789abcdef-image', 'description': 'owned',
                                'zone': 'us-central1-a', 'disk_name': 'bench-0123456789abcdef-0',
                                'source_disk_id': '123', 'id': '456', 'uncertain': False}}
        rec = self.state['image']
        self.image = {'name': rec['name'], 'id': '456', 'description': 'owned',
                      'labels': {'bench-run': self.state['run_id'], 'bench-source': 'a' * 63, 'bench-owner': 'b' * 32},
                      'sourceDiskId': '123',
                      'sourceDisk': 'https://www.googleapis.com/compute/v1/projects/mysten-research/zones/us-central1-a/disks/' + rec['disk_name'],
                      'selfLink': 'https://www.googleapis.com/compute/v1/projects/mysten-research/global/images/' + rec['name']}
        self.deletions = 0

    async def execute(self, argv):
        if argv[:3] == ['compute', 'images', 'list']:
            return {'text': json.dumps([self.image] if self.image else [])}
        if argv[:3] == ['compute', 'images', 'delete']:
            self.deletions += 1
            self.image = None
            return {'text': '', 'exit_code': None}
        raise AssertionError('unexpected operation')

    async def test_owned_image_deleted_and_absence_persisted(self):
        await gcp_manual_image.cleanup_image(self.config, self.state, self.path, self.execute)
        self.assertTrue(json.loads(self.path.read_text())['image']['deleted'])
        self.assertEqual(self.deletions, 1)
    async def test_failed_image_intent_is_restored_and_next_cleanup_deletes(self):
        record = self.state['image']
        record['delete_requested'] = False
        original_save = gcp_manual_image.gcp_campaign.save
        failures = [1]
        def save(path, state):
            if state['image'].get('delete_requested') and failures[0]:
                failures[0] -= 1
                raise RuntimeError('state save failed')
            return original_save(path, state)
        with mock.patch.object(gcp_manual_image.gcp_campaign, 'save', side_effect=save):
            with self.assertRaisesRegex(RuntimeError, 'state save failed'):
                await gcp_manual_image.cleanup_image(self.config, self.state, self.path, self.execute)
            self.assertIs(record['delete_requested'], False)
            self.assertEqual(self.deletions, 0)
            await gcp_manual_image.cleanup_image(self.config, self.state, self.path, self.execute)
        self.assertEqual(self.deletions, 1)


    async def test_reused_name_is_never_deleted(self):
        self.image['id'] = '999'
        with self.assertRaisesRegex(RuntimeError, 'replacement'):
            await gcp_manual_image.cleanup_image(self.config, self.state, self.path, self.execute)
        self.assertEqual(self.deletions, 0)

    async def test_unknown_create_is_not_declared_absent(self):
        self.image = None
        self.state['image']['uncertain'] = True
        self.state['image'].pop('id')
        with self.assertRaisesRegex(RuntimeError, 'uncertain'):
            await gcp_manual_image.cleanup_image(self.config, self.state, self.path, self.execute)
        self.assertEqual(self.deletions, 0)

    async def test_error_prose_is_not_absence(self):
        async def denied(argv):
            return {'text': 'STDERR: permission denied'}
        with self.assertRaisesRegex(RuntimeError, 'unknown'):
            await gcp_manual_image.cleanup_image(self.config, self.state, self.path, denied)
        self.assertNotIn('deleted', self.state['image'])

    async def test_expired_baker_never_sends_stop_mutation(self):
        self.state.pop('image')
        vm = {'name': self.state['run_id'] + '-0', 'zone': 'us-central1-a',
              'id': '42', 'disk_id': '123'}
        self.state['instances'] = [vm]
        self.state['deadline'] = '2030-01-01T00:00:00+00:00'
        execute = mock.AsyncMock()
        with mock.patch.object(gcp_manual_image, '_limit', return_value=10), \
                mock.patch.object(gcp_manual_image, '_list', new=mock.AsyncMock(return_value={})), \
                mock.patch.object(gcp_manual_image, '_valid_instance', return_value=True), \
                mock.patch.object(gcp_manual_image, '_valid_disk', return_value=True), \
                mock.patch.object(gcp_manual_image.gcp_campaign, 'has_delete_expiry', return_value=True), \
                mock.patch.object(gcp_manual_image.time, 'time', return_value=10):
            with self.assertRaisesRegex(TimeoutError, 'before stop'):
                await gcp_manual_image.capture_image(self.config, self.state, self.path, execute, self.state, vm)
        execute.assert_not_awaited()

    async def test_capture_reconciles_lost_stop_and_create_replies(self):
        template = self.image
        self.image = None
        self.state.pop('image')
        self.state['deadline'] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1)).isoformat()
        vm = {'name': self.state['run_id'] + '-0', 'zone': 'us-central1-a',
              'id': '42', 'disk_id': '123', 'disk_token': 'c' * 32}
        self.state['instances'] = [vm]
        zone = 'projects/mysten-research/zones/us-central1-a'
        metadata = {'bench-run': self.state['run_id'], 'bench-source': self.state['source_sha256'],
                    'bench-expiry': self.state['deadline'], 'bench-owner': self.state['owner_nonce']}
        instance = {'name': vm['name'], 'id': vm['id'], 'zone': zone, 'status': 'RUNNING',
                    'labels': template['labels'],
                    'metadata': {'items': [{'key': key, 'value': value} for key, value in metadata.items()]},
                    'serviceAccounts': [{'email': gcp_manual_image.gcp_campaign.GUEST_SERVICE_ACCOUNT}],
                    'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': self.state['deadline']}}
        disk = {'name': vm['name'], 'id': vm['disk_id'], 'zone': zone,
                'description': json.dumps({'run': self.state['run_id'], 'source': self.state['source_sha256'],
                                           'expiry': self.state['deadline'], 'token': vm['disk_token']}, sort_keys=True)}
        mutations = []

        async def execute(argv):
            if argv[:3] == ['compute', 'instances', 'list']:
                return {'text': json.dumps([instance])}
            if argv[:3] == ['compute', 'disks', 'list']:
                return {'text': json.dumps([disk])}
            if argv[:3] == ['compute', 'images', 'list']:
                return {'text': json.dumps([self.image] if self.image else [])}
            if argv[:3] == ['compute', 'instances', 'stop']:
                mutations.append('stop')
                instance['status'] = 'TERMINATED'
                raise TimeoutError('stop committed but reply was lost')
            if argv[:3] == ['compute', 'images', 'create']:
                mutations.append('create')
                intent = json.loads(self.path.read_text())['image']
                self.assertTrue(intent['uncertain'])
                self.assertNotIn('id', intent)
                self.image = dict(template, description=intent['description'], status='READY')
                raise TimeoutError('image committed but reply was lost')
            return await self.execute(argv)

        image = await gcp_manual_image.capture_image(
            self.config, self.state, self.path, execute, self.state, vm)
        self.assertEqual(image, {'path': 'projects/mysten-research/global/images/' + template['name'], 'id': '456'})
        self.assertEqual(mutations, ['stop', 'create'])
        self.assertFalse(json.loads(self.path.read_text())['image']['uncertain'])
        await gcp_manual_image.cleanup_image(self.config, self.state, self.path, execute)
        self.assertEqual(self.deletions, 1)
        self.assertTrue(json.loads(self.path.read_text())['image']['deleted'])
