"""No-cloud regression tests for the explicit MCP deployment controller."""
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('aws_control_test_target', HERE / 'control.py')
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)


class FakeMcp:
    def __init__(self):
        self.calls = []
        self.fleet = []
        self.fail_first_launch_after_creation = True

    def api(self, service, operation, params, region, profile):
        self.calls.append((service, operation, params))
        if operation == 'GetCallerIdentity':
            return {'Account': control.ACCOUNT}
        if operation == 'DescribeInstances':
            if 'InstanceIds' in params:
                return {'Reservations': [{'Instances': [{'InstanceId': i, 'State': {'Name': 'running'}} for i in params['InstanceIds']]}]}
            return {'Reservations': [{'Instances': self.fleet}]}
        if operation == 'RunInstances':
            for n in range(params['MaxCount']):
                self.fleet.append({'InstanceId': 'i-' + format(len(self.fleet) + 1, '017x'),
                    'ClientToken': params['ClientToken'], 'State': {'Name': 'running'},
                    'Tags': params['TagSpecifications'][0]['Tags'], 'LaunchTime': datetime.now(timezone.utc).isoformat(),
                    'PrivateIpAddress': '10.0.0.' + str(len(self.fleet) + 1)})
            if self.fail_first_launch_after_creation:
                self.fail_first_launch_after_creation = False
                raise TimeoutError('the fleet exists but response was lost')
            return {'Instances': self.fleet[-params['MaxCount']:]}
        if operation == 'TerminateInstances':
            for i in self.fleet:
                if i['InstanceId'] in params['InstanceIds']:
                    i['State']['Name'] = 'terminated'
            return {}
        raise AssertionError('unexpected fake AWS operation: ' + operation)


HOST_KEY = 'ssh-ed25519 ' + base64.b64encode(b'\0\0\0\x0bssh-ed25519\0\0\0\x20' + b'x' * 32).decode()


class FakeSshMcp(FakeMcp):
    def __init__(self, plan, fleet):
        super().__init__()
        self.plan, self.fleet = plan, fleet
        self.ingress = False

    def api(self, service, operation, params, region, profile):
        self.calls.append((service, operation, params))
        if operation == 'GetCallerIdentity':
            return {'Account': self.plan['account']}
        if operation == 'DescribeInstances':
            return {'Reservations': [{'Instances': self.fleet}]}
        if operation == 'DescribeSecurityGroups':
            return {'SecurityGroups': [{'GroupId': 'sg-fixture', 'GroupName': self.plan['run_id'], 'VpcId': self.plan['vpc'],
                                        'Tags': control.aws_tags(self.plan['tags'])}]}
        if operation == 'GetRole':
            return {'Role': {'Tags': control.aws_tags(self.plan['tags'])}}
        if operation == 'PutRolePolicy':
            return {}
        if operation == 'GetConsoleOutput':
            return {'InstanceId': params['InstanceId'], 'Output': '-----BEGIN SSH HOST KEY KEYS-----\n' + HOST_KEY + ' root@node\n-----END SSH HOST KEY KEYS-----\n'}
        if operation == 'AuthorizeSecurityGroupIngress':
            self.ingress = True
            return {'Return': True}
        if operation == 'RevokeSecurityGroupIngress':
            self.ingress = False
            return {'Return': True}
        if operation == 'SendSSHPublicKey':
            return {'Success': True}
        raise AssertionError('unexpected fake operation: ' + operation)


class ControllerTests(unittest.TestCase):
    def test_transport_error_without_response_has_no_service_code(self):
        error = TimeoutError('no service response')
        error.response = None
        self.assertIsNone(control.error_code(error))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.bundle = root / 'bundle.tar.gz'
        self.bundle.write_bytes(b'local source-bound fixture')
        (root / 'upload-manifest.json').write_text(json.dumps({'archive': self.bundle.name,
            'sha256': control.digest(self.bundle), 'bytes': self.bundle.stat().st_size}))
        self.run = root / 'run'
        self.plan = control.init(self.run, self.bundle)
        self.fake = FakeMcp()
        self.controller = control.Control(self.run, self.fake)
        self.controller.remember('uploaded_bundle', {'sha256': self.plan['bundle_sha256']})
        self.controller.remember('security_group', 'sg-fixture')

    def tearDown(self):
        self.temp.cleanup()

    def test_progress_selector_retains_hardware_ownership_and_lifetime_bounds(self):
        plan = control.init(Path(self.temp.name) / 'progress-run', self.bundle, campaign='efa-progress')
        self.assertEqual(plan['campaign'], 'efa-progress')
        self.assertTrue(plan['efa'])
        self.assertEqual(plan['instance_type'], 'c6id.32xlarge')
        self.assertEqual(plan['instance_count'], 6)
        self.assertEqual(plan['ttl_seconds'], 3600)
        self.assertEqual(plan['export_reserve_seconds'], 600)
        self.assertEqual(plan['tags'], control.ownership(plan['run_id']))
        self.assertEqual(plan['protected_instances'], self.plan['protected_instances'])
        self.assertEqual(plan['placement_group'], plan['run_id'])
        self.assertEqual(plan['placement_strategy'], 'cluster')

    def test_shorter_fleet_lifetime_preserves_launch_bound_and_export_reserve(self):
        run = Path(self.temp.name) / 'short-run'
        plan = control.init(run, self.bundle, campaign='efa-progress', ttl_seconds=3000)
        candidate = control.Control(run, self.fake)
        candidate.remember('uploaded_bundle', {'sha256': plan['bundle_sha256']})
        candidate.remember('security_group', 'sg-fixture')
        self.fake.fail_first_launch_after_creation = False
        candidate.launch()
        first = min(datetime.fromisoformat(i['LaunchTime']) for i in self.fake.fleet)
        cfg, _ = control.fleet_configuration(plan, self.fake.fleet)
        self.assertEqual(datetime.fromisoformat(cfg['deadline_utc']), first + timedelta(seconds=2970))
        self.assertEqual(cfg['export_reserve_seconds'], 600)
        self.assertLess(plan['estimated_hourly_usd'] * 3000 / 3600, plan['reserve_usd'])
        self.assertLessEqual(plan['reserve_usd'], 35)
        self.fake.fleet[0]['LaunchTime'] = (datetime.now(timezone.utc) - timedelta(seconds=1801)).isoformat()
        with self.assertRaisesRegex(ValueError, 'insufficient original lifetime'):
            control.fleet_configuration(plan, self.fake.fleet)
        for invalid in (2999, 3601, 7200, 3000.0):
            destination = Path(self.temp.name) / ('invalid-' + str(invalid))
            with self.subTest(lifetime=invalid), self.assertRaises(ValueError):
                control.init(destination, self.bundle, ttl_seconds=invalid)
            self.assertFalse(destination.exists())

    def test_rejects_hardware_outside_the_campaign_cost_scope(self):
        for campaign, hardware in [('overlap', 'g6.16xlarge'), ('efa-progress', 'g6.48xlarge')]:
            destination = Path(self.temp.name) / campaign
            with self.subTest(campaign=campaign, hardware=hardware), self.assertRaises(ValueError):
                control.init(destination, self.bundle, campaign=campaign, efa_instance_type=hardware)
            self.assertFalse(destination.exists())

    def test_checkpoint_campaign_rejects_unmeasurable_fleets_before_plan_creation(self):
        for hardware, zone in [('c6id.32xlarge', 'us-east-1a'), ('g6.16xlarge', 'us-east-1b')]:
            destination = Path(self.temp.name) / (hardware + '-' + zone)
            with self.subTest(hardware=hardware, zone=zone), self.assertRaises(ValueError):
                control.init(destination, self.bundle, campaign='checkpoint-async',
                             efa_instance_type=hardware, az=zone)
            self.assertFalse(destination.exists())

    def test_transport_selectors_preserve_hardware_deadline_and_scope_guards(self):
        for campaign in ('scalar-transport', 'transport-floor'):
            for hardware, zone, scope, ttl in [
                    ('c6id.32xlarge' if campaign == 'scalar-transport' else 'g6.48xlarge', 'us-east-1a', 'full', 3600),
                    ('g6.16xlarge', 'us-east-1b' if campaign == 'scalar-transport' else 'us-west-2a', 'full', 3600),
                    ('g6.16xlarge', 'us-east-1a', 'capacity', 3600),
                    ('g6.16xlarge', 'us-east-1a', 'core', 2400)]:
                destination = Path(self.temp.name) / f'{campaign}-{hardware}-{zone}-{scope}'
                with self.subTest(campaign=campaign, hardware=hardware, zone=zone, scope=scope), self.assertRaises(ValueError):
                    control.init(destination, self.bundle, campaign=campaign,
                                 efa_instance_type=hardware, az=zone, checkpoint_scope=scope, ttl_seconds=ttl)
                self.assertFalse(destination.exists())
            run = Path(self.temp.name) / (campaign + '-recovery')
            control.init(run, self.bundle, campaign=campaign,
                         efa_instance_type='g6.16xlarge', checkpoint_scope='recovery', ttl_seconds=2400)
            control.Control(run, self.fake)

    def test_transport_floor_plans_select_spread_hardware_within_hourly_reserve(self):
        for hardware, hourly in [('g6.16xlarge', 3.3968), ('c6id.32xlarge', 6.4512)]:
            with self.subTest(hardware=hardware):
                run = Path(self.temp.name) / hardware
                plan = control.init(run, self.bundle, campaign='transport-floor',
                                    efa_instance_type=hardware, az='us-east-1b')
                self.assertEqual(plan['instance_type'], hardware)
                self.assertEqual(plan['instance_count'], 6)
                self.assertTrue(plan['efa'])
                self.assertEqual(plan['az'], 'us-east-1b')
                self.assertEqual(plan['placement_strategy'], 'spread')
                self.assertEqual(plan['placement_group'], plan['run_id'])
                self.assertEqual(plan['ttl_seconds'], 3600)
                self.assertEqual(plan['export_reserve_seconds'], 600)
                self.assertAlmostEqual(plan['estimated_hourly_usd'], 6 * (hourly + .005 + 32 * .08 / 730))
                self.assertEqual(plan['reserve_usd'], 42)
                self.assertLess(plan['estimated_hourly_usd'], plan['reserve_usd'])
                controller = control.Control(run, self.fake)
                self.assertEqual(controller.plan, plan)
                self.assertEqual(self.fake.calls, [])
                other = 'c6id.32xlarge' if hardware == 'g6.16xlarge' else 'g6.16xlarge'
                (run / 'plan.json').write_text(json.dumps(plan | {'instance_type': other}))
                with self.assertRaisesRegex(ValueError, 'immutable'):
                    control.Control(run, self.fake)

    def test_transport_start_routes_reject_wrong_hardware_without_blocking_owned_teardown(self):
        for campaign, hardware in [('scalar-transport', 'g6.16xlarge'),
                                   ('transport-floor', 'g6.16xlarge'),
                                   ('transport-floor', 'c6id.32xlarge')]:
            for route in ('ssm', 'ssh'):
                for defect in ('mixed-hardware', 'wrong-hardware', 'unsupported-hardware', 'placement'):
                    with self.subTest(campaign=campaign, hardware=hardware, route=route, defect=defect):
                        run = Path(self.temp.name) / f'{campaign}-{hardware}-{route}-{defect}'
                        plan = control.init(run, self.bundle, campaign=campaign,
                                            efa_instance_type=hardware,
                                            az='us-east-1b' if campaign == 'transport-floor' else 'us-east-1a')
                        fake = FakeMcp()
                        fake.fleet = [
                            {'InstanceId': f'i-{n + 1:017x}', 'State': {'Name': 'running'},
                             'ClientToken': plan['run_id'] + ('-node' if n < 5 else '-controller'),
                             'Tags': control.aws_tags(plan['tags']),
                             'InstanceType': hardware, 'Placement': {'AvailabilityZone': plan['az']},
                             'LaunchTime': datetime.now(timezone.utc).isoformat(),
                             'PrivateIpAddress': f'10.0.0.{n + 1}'}
                            for n in range(6)]
                        cfg, coordinator = control.fleet_configuration(plan, fake.fleet)
                        self.assertEqual(cfg['instance_type'], hardware)
                        self.assertEqual(cfg['availability_zone'], plan['az'])
                        self.assertEqual(len(cfg['nodes']), 5)
                        self.assertEqual(coordinator['InstanceId'], fake.fleet[-1]['InstanceId'])
                        other = 'c6id.32xlarge' if hardware == 'g6.16xlarge' else 'g6.16xlarge'
                        if defect == 'mixed-hardware':
                            fake.fleet[0]['InstanceType'] = other
                        elif defect == 'wrong-hardware':
                            for instance in fake.fleet:
                                instance['InstanceType'] = other
                        elif defect == 'unsupported-hardware':
                            fake.fleet[0]['InstanceType'] = 'g6.48xlarge'
                        else:
                            fake.fleet[0]['Placement']['AvailabilityZone'] = 'us-east-1c'
                        controller = control.Control(run, fake)
                        with self.assertRaises(ValueError):
                            if route == 'ssm':
                                controller.start()
                            else:
                                controller.ssh_readiness_start('8.8.8.8/32')
                        self.assertFalse(any(op in ('PutRolePolicy', 'SendCommand', 'SendSSHPublicKey')
                                             for _, op, _ in fake.calls))
                        controller.terminate()
                        self.assertTrue(all(i['State']['Name'] == 'terminated' for i in fake.fleet))
                        requested = next(p['InstanceIds'] for _, op, p in fake.calls if op == 'TerminateInstances')
                        self.assertEqual(requested, sorted(i['InstanceId'] for i in fake.fleet))
                        self.assertTrue(set(requested).isdisjoint(control.OTHER_INSTANCES))

    def test_short_recovery_lifetime_cannot_be_used_for_other_workloads(self):
        for scope, campaign in [('full', 'checkpoint-async'), ('recovery', 'efa-progress'), ('unknown', 'checkpoint-async')]:
            destination = Path(self.temp.name) / (scope + '-' + campaign)
            with self.subTest(scope=scope, campaign=campaign), self.assertRaises(ValueError):
                control.init(destination, self.bundle, campaign=campaign, checkpoint_scope=scope,
                             efa_instance_type='g6.16xlarge', ttl_seconds=2400)
            self.assertFalse(destination.exists())

    def test_efa_fleet_has_explicit_interface_cost_bound_and_fixed_lifetime(self):
        run = Path(self.temp.name) / 'efa-run'
        plan = control.init(run, self.bundle, campaign='efa', az='us-east-1b',
                            subnet='subnet-0d8e9d9444e31bbac')
        self.assertEqual(plan['instance_type'], 'c6id.32xlarge')
        self.assertLess(plan['estimated_hourly_usd'], plan['reserve_usd'])
        self.assertLess(plan['reserve_usd'], 60)
        self.assertEqual(plan['ttl_seconds'], 3600)
        candidate = control.Control(run, self.fake)
        candidate.remember('uploaded_bundle', {'sha256': plan['bundle_sha256']})
        candidate.remember('security_group', 'sg-fixture')
        with self.assertRaises(TimeoutError):
            candidate.launch()
        params = next(p for _, op, p in self.fake.calls if op == 'RunInstances')
        self.assertEqual(params['NetworkInterfaces'][0]['InterfaceType'], 'efa')
        self.assertEqual(params['NetworkInterfaces'][0]['SubnetId'], plan['subnet'])
        self.assertEqual(params['Placement']['AvailabilityZone'], 'us-east-1b')
        self.assertEqual(params['Placement']['GroupName'], plan['run_id'])
        script = params['UserData']
        self.assertLess(len(script.encode()), 16384)
        parsed = subprocess.run(['bash', '-n'], input=script, text=True, capture_output=True)
        self.assertEqual(parsed.returncode, 0, parsed.stderr)

    def test_init_is_offline_and_plan_is_immutable(self):
        self.assertEqual(self.fake.calls, [])
        self.assertLess(self.plan['estimated_hourly_usd'], self.plan['reserve_usd'])
        self.assertEqual(self.plan['ttl_seconds'], 3600)
        damaged = self.plan | {'ttl_seconds': 7200}
        (self.run / 'plan.json').write_text(json.dumps(damaged))
        with self.assertRaisesRegex(ValueError, 'immutable'):
            control.Control(self.run, self.fake)

    def test_launch_requires_termination_imdsv2_and_encrypted_ephemeral_root(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        params = next(p for _, op, p in self.fake.calls if op == 'RunInstances')
        self.assertEqual(params['InstanceInitiatedShutdownBehavior'], 'terminate')
        self.assertEqual(params['MetadataOptions']['HttpTokens'], 'required')
        self.assertTrue(params['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'])
        self.assertTrue(params['BlockDeviceMappings'][0]['Ebs']['Encrypted'])

    def test_lost_launch_response_reconciles_existing_five_before_one_controller(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        state = json.loads((self.run / 'state.json').read_text())
        self.assertIn('launch-node', state['intents'])
        self.controller = control.Control(self.run, self.fake)
        self.controller.launch()
        launches = [p for _, op, p in self.fake.calls if op == 'RunInstances']
        self.assertEqual([p['MaxCount'] for p in launches], [5, 1])
        self.assertEqual(len(self.fake.fleet), 6)
        self.controller.launch()
        self.assertEqual(len([1 for _, op, _ in self.fake.calls if op == 'RunInstances']), 2)

    def test_unknown_owned_token_blocks_replacement_launch(self):
        self.fake.fleet = [{'InstanceId': 'i-' + '8' * 17, 'ClientToken': 'unexpected',
                            'Tags': control.aws_tags(self.plan['tags'])}]
        with self.assertRaisesRegex(ValueError, 'unexpected owned instance token'):
            self.controller.launch()
        self.assertFalse(any(op == 'RunInstances' for _, op, _ in self.fake.calls))

    def test_termination_only_targets_all_three_owned_tags_and_protected_ids_rejected(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        self.controller.terminate()
        requested = next(p['InstanceIds'] for _, op, p in self.fake.calls if op == 'TerminateInstances')
        self.assertEqual(len(requested), 5)
        self.assertTrue(set(requested).isdisjoint(control.OTHER_INSTANCES))
        self.fake.fleet[0]['InstanceId'] = control.OTHER_INSTANCES[0]
        with self.assertRaisesRegex(ValueError, 'protected'):
            self.controller.terminate()
        self.fake.fleet[0]['InstanceId'] = 'i-' + '7' * 17
        self.fake.fleet[0]['Tags'] = [{'Key': 'Project', 'Value': 'TideHunter'}]
        with self.assertRaisesRegex(ValueError, 'three exact ownership'):
            self.controller.terminate()

    def test_reprovision_preserves_efa_self_egress_and_original_intent(self):
        run = Path(self.temp.name) / 'resumed-efa'
        plan = control.init(run, self.bundle, campaign='efa')
        tags = control.aws_tags(plan['tags'])
        default = {'IpProtocol': '-1', 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
        group = {'GroupId': 'sg-owned', 'Tags': tags, 'IpPermissionsEgress': [default]}
        class ProvisionMcp(FakeMcp):
            bucket_exists = False

            def api(self, service, operation, params, region, profile):
                if operation == 'HeadBucket':
                    return {} if self.bucket_exists else None
                if operation == 'CreateBucket':
                    self.bucket_exists = True
                    return {}
                if operation == 'DescribeSecurityGroups':
                    return {'SecurityGroups': [group]}
                if operation == 'RevokeSecurityGroupEgress':
                    for permission in params['IpPermissions']:
                        group['IpPermissionsEgress'].remove(permission)
                    return {}
                if operation == 'AuthorizeSecurityGroupEgress':
                    for permission in params['IpPermissions']:
                        if permission not in group['IpPermissionsEgress']:
                            group['IpPermissionsEgress'].append(permission)
                    return {}
                if operation == 'GetRole':
                    return {'Role': {'Tags': tags}}
                if operation == 'GetInstanceProfile':
                    return {'InstanceProfile': {'Tags': tags, 'Roles': [
                        {'RoleName': params['InstanceProfileName']}]}}
                if operation == 'DescribePlacementGroups':
                    return {'PlacementGroups': [{'Tags': tags}]}
                if operation.startswith('PutBucket') or operation in (
                        'PutPublicAccessBlock', 'AuthorizeSecurityGroupIngress',
                        'AttachRolePolicy', 'PutRolePolicy'):
                    return {}
                return super().api(service, operation, params, region, profile)
        controller = control.Control(run, ProvisionMcp())
        controller.provision()
        original = controller.state['intents']['revoke-default-egress'].copy()
        controller.provision()
        self.assertNotIn(default, group['IpPermissionsEgress'])
        self.assertIn({'IpProtocol': '-1', 'UserIdGroupPairs': [{'GroupId': 'sg-owned'}]},
                      group['IpPermissionsEgress'])
        self.assertEqual(controller.state['intents']['revoke-default-egress'], original)

    def test_runtime_policies_are_exact_run_and_exact_nodes(self):
        policy = control.role_policy(self.plan, 'controller')
        terminate = next(s for s in policy['Statement'] if s['Action'] == 'ec2:TerminateInstances')
        self.assertEqual(len(terminate['Condition']['StringEquals']), 3)
        self.assertNotIn('PassRole', json.dumps(policy))
        self.assertNotIn('kms:', json.dumps(policy))
        atomic = control.role_policy(self.plan | {'atomic_controller': True}, 'controller')
        controller_actions = set(next(s['Action'] for s in atomic['Statement']
                                      if s.get('Resource') == '*' and isinstance(s.get('Action'), list)))
        self.assertTrue({'ec2:DescribeInstanceAttribute', 'ec2:DescribePlacementGroups',
                         'ssm:DescribeInstanceInformation', 'ssm:GetCommandInvocation'}
                        .issubset(controller_actions))
        self.assertNotIn('ssm:DescribeInstanceInformation', json.dumps(policy))
        ids = ['i-' + format(n, '017x') for n in range(1, 6)]
        commands = control.command_policy(self.plan, ids)
        self.assertEqual(commands['Statement'][1]['Resource'],
                         f"arn:aws:ssm:{self.plan['region']}::document/AWS-RunShellScript")
        self.assertEqual(len(commands['Statement'][0]['Resource']), 5)
        self.assertTrue(all(resource.rsplit('/', 1)[1] in ids for resource in commands['Statement'][0]['Resource']))
        self.assertNotIn('TerminateInstances', json.dumps(control.role_policy(self.plan, 'node')))
        with self.assertRaises(ValueError):
            control.command_policy(self.plan, ids[:4] + [control.OTHER_INSTANCES[0]])

    def test_cleanup_refuses_running_fleet_and_missing_verified_results(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        with self.assertRaisesRegex(ValueError, 'terminate first'):
            self.controller.cleanup()
        self.assertFalse(any(op.startswith('Delete') for _, op, _ in self.fake.calls))

    def test_running_fleet_cannot_start_without_guest_readiness(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        self.controller.launch()
        with self.assertRaisesRegex(ValueError, 'readiness of all six'):
            self.controller.start()
        self.assertFalse(any(op == 'SendCommand' for _, op, _ in self.fake.calls))

    def test_readiness_requires_all_six_exact_identity_receipts(self):
        ids = ['i-' + format(n, '017x') for n in range(1, 7)]
        self.controller.remember('readiness_command', {'command_id': 'c', 'instance_ids': ids})
        replies = {instance: {'instance_id': instance, 'run_id': self.plan['run_id'],
            'bundle_sha256': self.plan['bundle_sha256'], 'account': self.plan['account'],
            'mount_target': '/mnt/tidehunter', 'model': 'Amazon EC2 NVMe Instance Storage',
            'deadline_utc': (datetime.now(timezone.utc) + timedelta(seconds=3500)).isoformat()}
                   for instance in ids}
        def invoke(service, operation, **params):
            self.assertEqual(operation, 'GetCommandInvocation')
            return {'Status': 'Success', 'ResponseCode': 0,
                    'StandardOutputContent': json.dumps(replies[params['InstanceId']])}
        self.controller.api = invoke
        self.assertTrue(self.controller.readiness_status()['ready'])
        self.assertEqual(len(self.controller.state['resources']['readiness_verified']['guests']), 6)
        replies[ids[2]]['instance_id'] = ids[0]
        with self.assertRaisesRegex(ValueError, 'identity or bundle'):
            self.controller.readiness_status()

    def ssh_setup(self):
        with self.assertRaises(TimeoutError):
            self.controller.launch()
        self.controller.launch()
        self.fake.fleet[-1]['PublicIpAddress'] = '8.8.4.4'
        fake = FakeSshMcp(self.plan, self.fake.fleet)
        self.controller.adapter = fake
        def run(command, **kwargs):
            if command[0] == 'ssh-keygen':
                key = Path(command[-1])
                key.write_text('fake test-only private key')
                key.chmod(0o600)
                key.with_suffix('.pub').write_text(HOST_KEY + ' temporary-test-key')
                return subprocess.CompletedProcess(command, 0)
            self.assertEqual(command[:3], ['ssh', '-F', '/dev/null'])
            self.assertIn('StrictHostKeyChecking=yes', command)
            cfg = json.loads((self.run / 'controller.json').read_text())
            reply = {'started': True, 'deadline_utc': cfg['deadline_utc'],
                     'ready': {'guests': [{'instance_id': i['InstanceId']} for i in self.fake.fleet]}}
            return subprocess.CompletedProcess(command, 0, json.dumps(reply), '')
        return fake, run

    def test_eic_start_pins_console_key_and_closes_only_temporary_rule(self):
        fake, run = self.ssh_setup()
        with patch.object(control.subprocess, 'run', side_effect=run):
            result = self.controller.ssh_readiness_start('8.8.8.8/32')
        self.assertTrue(result['started'])
        self.assertFalse(fake.ingress)
        known = (self.run / 'ssh/known_hosts').read_text()
        self.assertEqual(known, '8.8.4.4 ' + HOST_KEY + '\n')
        permissions = [p for _, op, p in fake.calls if op == 'RevokeSecurityGroupIngress']
        self.assertEqual(permissions[0]['IpPermissions'][0]['IpRanges'], [{'CidrIp': '8.8.8.8/32'}])
        self.assertEqual(permissions[0]['IpPermissions'][0]['FromPort'], 22)
        self.assertFalse((self.run / 'ssh/ephemeral').exists())
        self.assertIn('ssh-start-controller', self.controller.state['intents'])
        self.assertFalse(any(service == 'ssm' for service, _, _ in fake.calls))

    def test_lost_ssh_start_response_never_restarts_and_closes_ingress(self):
        fake, run = self.ssh_setup()
        def fail_ssh(command, **kwargs):
            if command[0] == 'ssh':
                raise subprocess.TimeoutExpired(command, 180, output=b'partial output')
            return run(command, **kwargs)
        with patch.object(control.subprocess, 'run', side_effect=fail_ssh):
            with self.assertRaisesRegex(RuntimeError, 'do not resend'):
                self.controller.ssh_readiness_start('8.8.8.8/32')
        self.assertFalse(fake.ingress)
        self.assertIn('ssh-start-controller', self.controller.state['intents'])
        calls = len(fake.calls)
        with self.assertRaisesRegex(ValueError, 'already requested'):
            self.controller.ssh_readiness_start('8.8.8.8/32')
        with self.assertRaisesRegex(ValueError, 'already requested'):
            self.controller.start()
        self.assertEqual(len(fake.calls), calls)
        self.assertEqual((self.run / 'ssh/start.stdout.log').read_bytes(), b'partial output')

    def test_ssh_scope_rejects_subnets_private_addresses_and_untrusted_console_keys(self):
        for value in [None, '0.0.0.0/0', '8.8.8.0/24', '192.168.1.1/32', '::1/128', '8.8.8.8']:
            with self.assertRaises(ValueError):
                control.exact_public_ipv4(value)
        self.assertEqual(control.exact_public_ipv4('8.8.8.8/32'), '8.8.8.8/32')
        with self.assertRaises(ValueError):
            control.console_ed25519_key(HOST_KEY)
        bad = '-----BEGIN SSH HOST KEY KEYS-----\nssh-ed25519 YQ==\n-----END SSH HOST KEY KEYS-----'
        with self.assertRaises(ValueError):
            control.console_ed25519_key(bad)
        valid = '-----BEGIN SSH HOST KEY KEYS-----\n' + HOST_KEY + '\n-----END SSH HOST KEY KEYS-----'
        self.assertEqual(control.console_ed25519_key(valid), HOST_KEY)
        different = HOST_KEY[:-4] + 'eXk='
        with self.assertRaises(ValueError):
            control.console_ed25519_key(valid + valid.replace(HOST_KEY, different))

    def test_remote_eic_script_compiles_and_recomputes_runtime_after_readiness(self):
        self.ssh_setup()
        cfg, _ = control.fleet_configuration(self.plan, self.fake.fleet)
        script = control.ssh_start_script(self.plan, cfg)
        python = script.split("'SHARDED_WAL_FALLBACK'\n", 1)[1].rsplit('SHARDED_WAL_FALLBACK', 1)[0]
        compile(python, '<remote-eic-readiness>', 'exec')
        self.assertIn("assert len(replies)==6", python)
        self.assertIn("assert left>1200", python)
        self.assertIn("RuntimeMaxSec=0", python)
        self.assertLess(python.index("s3.put_object"), python.index("left=int("))

    def test_progress_get_accepts_grown_snapshot_without_stale_head(self):
        old = {'run_id': self.plan['run_id'], 'completed_cases': []}
        current = {'run_id': self.plan['run_id'], 'completed_cases': list(range(70)), 'stage': 'benchmark'}
        control.durable_json(self.run / 'progress.json', old)
        with patch.object(self.controller, 'presigned', return_value='https://example.invalid/progress') as signed, \
             patch.object(self.controller, 'api', side_effect=AssertionError('mutable progress must not HEAD')), \
             patch.object(control.urllib.request, 'urlopen', return_value=io.BytesIO(json.dumps(current).encode())) as opened:
            self.assertEqual(self.controller.progress(), current)
        self.assertEqual(json.loads((self.run / 'progress.json').read_text()), current)
        signed.assert_called_once_with(self.plan['prefix'] + '/results/progress.json', 'download', {'ExpectedBucketOwner': self.plan['account']})
        opened.assert_called_once()

    def test_progress_rejects_oversized_wrong_run_and_invalid_json_without_replacing_last_good(self):
        old = {'run_id': self.plan['run_id'], 'completed_cases': [1]}
        control.durable_json(self.run / 'progress.json', old)
        cases = [(b'x' * ((2 << 20) + 1), 'exceeds 2 MiB'),
                 (json.dumps({'run_id': 'another-run'}).encode(), 'different run'),
                 (b'{"run_id":', None)]
        for body, message in cases:
            with self.subTest(message=message):
                stream = io.BytesIO(body)
                with patch.object(self.controller, 'presigned', return_value='https://example.invalid/progress'), \
                     patch.object(control.urllib.request, 'urlopen', return_value=stream):
                    with self.assertRaises(ValueError):
                        self.controller.progress()
                self.assertTrue(stream.closed)
                self.assertEqual(json.loads((self.run / 'progress.json').read_text()), old)

    def test_mutation_intent_precedes_call_and_cannot_change(self):
        self.controller.intent('test', {'count': 5})
        with self.assertRaisesRegex(ValueError, 'immutable mutation'):
            self.controller.intent('test', {'count': 6})
        self.assertEqual(self.fake.calls, [])

    def test_controller_service_has_one_attempt_guard_and_no_home_override(self):
        cfg = {'run_id': self.plan['run_id'], 'run_root': '/mnt/tidehunter/' + self.plan['run_id']}
        command = control.start_command(cfg, 3000)
        self.assertIn('RuntimeMaxSec=3000', command)
        self.assertIn('systemd-run', command)
        encoded_script = command.split('printf %s ')[2].split(' | base64 -d')[0].strip("'")
        script = base64.b64decode(encoded_script).decode()
        self.assertIn('controller-start-once', script)
        self.assertNotIn('export HOME=', script)


if __name__ == '__main__':
    unittest.main()
