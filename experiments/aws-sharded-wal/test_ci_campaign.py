"""Behavioral offline admission/allocation/cleanup tests; no real AWS calls."""
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import fnmatch
import hashlib
import io
import json
import tarfile
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import ci_campaign as ci
from ci_budget import BudgetLedger as ActualBudgetLedger


RUN = 'th-0123456789abcdef'
ACCOUNT = '123456789012'


class ServiceError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}
        super().__init__(code)


class Clock:
    def __init__(self):
        self.now = datetime.now(timezone.utc).timestamp()

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class SDKReadinessRetryTests(unittest.TestCase):
    """Real botocore endpoints/parsers/retry handlers over a scripted HTTP transport."""

    class Body(io.BytesIO):
        def stream(self, amt=1024, decode_content=False):
            while chunk := self.read(amt):
                yield chunk

    class HTTP:
        def __init__(self, responses):
            self.responses, self.requests = list(responses), []

        def send(self, request):
            from botocore.awsrequest import AWSResponse
            self.requests.append(dict(method=request.method, url=request.url, body=request.body))
            if not self.responses:
                raise AssertionError('unexpected SDK HTTP attempt')
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            status, headers, body = response
            headers = dict(headers, **{'Content-Length': str(len(body))})
            return AWSResponse(request.url, status, headers, SDKReadinessRetryTests.Body(body))

    def setUp(self):
        import ci_watchdog
        from test_ci_watchdog import ACCOUNT as owner, BOUNDARY, REGION, function_configuration
        self.watchdog, self.region = ci_watchdog, REGION
        self.function = function_configuration()
        self.rule = dict(Arn=f'arn:aws:events:{REGION}:{owner}:rule/janitor',
                         State='ENABLED', ScheduleExpression=ci_watchdog.SCHEDULE)
        self.targets = [dict(Id='janitor', Arn=self.function['FunctionArn'])]
        self.arguments = dict(function_name='janitor', rule_name='janitor', account=owner,
                              regions=[REGION], guest_boundary_arn=BOUNDARY)
        self.verified = ci_watchdog.verify_readiness(self.function, self.rule, self.targets,
            account=owner, regions=[REGION], guest_boundary_arn=BOUNDARY)
        self.health = dict(schema=1, healthy=True, **self.verified['config'])
        self.permission = json.dumps(dict(Statement=[dict(Effect='Allow',
            Principal={'Service': 'events.amazonaws.com'}, Action='lambda:InvokeFunction',
            Resource=self.function['FunctionArn'], Condition=dict(
                ArnLike={'AWS:SourceArn': self.rule['Arn']}, StringEquals={'AWS:SourceAccount': owner}))]))
        self.delays = []
        sleeping = patch('botocore.endpoint.time.sleep', side_effect=self.delays.append)
        sleeping.start()
        self.addCleanup(sleeping.stop)

    @staticmethod
    def reply(body, status=200, error=None):
        headers = {'Content-Type': 'application/json', 'x-amzn-RequestId': 'offline-request'}
        if error:
            headers['x-amzn-ErrorType'] = error
        return status, headers, json.dumps(body).encode()

    def throttle(self):
        return self.reply({'message': 'reserved concurrency is occupied'}, 429, 'TooManyRequestsException')

    def sdk(self):
        import boto3
        session = boto3.Session(aws_access_key_id='offline-test-key',
            aws_secret_access_key='offline-test-secret', region_name=self.region)
        with patch('boto3.Session', return_value=session):
            aws = ci.AWS()
        def close():
            for client in aws.clients.values():
                client.close()
        self.addCleanup(close)
        return aws

    def transport(self, client, responses):
        http = self.HTTP(responses)
        sending = patch.object(client._endpoint.http_session, 'send', new=http.send)
        sending.start()
        self.addCleanup(sending.stop)
        return http

    def readiness(self, aws, responses):
        lambda_client = aws.client('lambda', self.region)
        events_client = aws.client('events', self.region)
        http = self.transport(lambda_client, responses)
        events = self.transport(events_client, [self.reply(self.rule), self.reply({'Targets': self.targets})])
        return lambda: self.watchdog.validate_readiness(lambda_client, events_client, **self.arguments), http, events

    def control_responses(self):
        return [self.reply(self.function), self.reply({'Policy': self.permission})]

    def test_transient_invoke_throttles_retry_real_sdk_then_accept_runtime_health(self):
        aws = self.sdk()
        validate, http, events = self.readiness(aws,
            [*self.control_responses(), self.throttle(), self.throttle(), self.reply(self.health)])
        self.assertEqual(validate(), self.verified)
        self.assertEqual(len(http.requests), 5)
        self.assertEqual(len(events.requests), 2)
        invokes = [request for request in http.requests if request['method'] == 'POST']
        self.assertEqual(len(invokes), 3)
        self.assertTrue(all(json.loads(request['body']) == {'action': 'health'} for request in invokes))
        self.assertEqual(len(self.delays), 2)
        self.assertTrue(all(0 <= delay <= 20 for delay in self.delays))
        self.assertEqual(http.responses, [])
        self.assertIs(aws.client('lambda', self.region), aws.client('lambda', self.region))

    def test_lambda_configuration_and_policy_reads_also_retry_transient_throttles(self):
        for stage in (0, 1):
            with self.subTest(stage=stage):
                responses = self.control_responses()
                responses.insert(stage, self.throttle())
                aws = self.sdk()
                validate, http, _ = self.readiness(aws, [*responses, self.reply(self.health)])
                before = len(self.delays)
                self.assertEqual(validate(), self.verified)
                self.assertEqual(len(http.requests), 4)
                self.assertEqual(len(self.delays) - before, 1)
                self.assertEqual(http.requests[stage]['method'], 'GET')
                self.assertEqual(http.requests[stage]['url'], http.requests[stage + 1]['url'])

    def test_exhausted_throttle_fails_closed_after_twelve_attempts(self):
        from botocore.exceptions import ClientError
        validate, http, _ = self.readiness(self.sdk(),
            [*self.control_responses(), *[self.throttle() for _ in range(12)]])
        with self.assertRaises(ClientError) as rejected:
            validate()
        self.assertEqual(rejected.exception.response['Error']['Code'], 'TooManyRequestsException')
        self.assertEqual(rejected.exception.response['ResponseMetadata']['RetryAttempts'], 11)
        self.assertEqual(len([item for item in http.requests if item['method'] == 'POST']), 12)
        self.assertEqual(len(self.delays), 11)
        self.assertTrue(all(0 <= delay <= 20 for delay in self.delays))
        self.assertLessEqual(sum(self.delays), 151)
        self.assertEqual(http.responses, [])

    def test_lambda_authorization_failure_is_not_retried_or_accepted(self):
        from botocore.exceptions import ClientError
        denied = self.reply({'message': 'not authorized'}, 403, 'AccessDeniedException')
        validate, http, _ = self.readiness(self.sdk(), [*self.control_responses(), denied])
        with self.assertRaises(ClientError) as rejected:
            validate()
        self.assertEqual(rejected.exception.response['Error']['Code'], 'AccessDeniedException')
        self.assertEqual(rejected.exception.response['ResponseMetadata']['RetryAttempts'], 0)
        self.assertEqual(len(http.requests), 3)
        self.assertEqual(self.delays, [])

    def test_invalid_runtime_identity_and_function_error_remain_fail_closed(self):
        wrong = dict(self.health, account='999999999999')
        function_error = self.reply(self.health)
        function_error[1]['x-amz-function-error'] = 'Unhandled'
        for response in (self.reply(wrong), function_error):
            with self.subTest(response=response):
                validate, http, _ = self.readiness(self.sdk(), [*self.control_responses(), response])
                with self.assertRaisesRegex(ValueError, 'runtime version/config/account'):
                    validate()
                self.assertEqual(len(http.requests), 3)
                self.assertEqual(self.delays, [])

    def test_ec2_ambiguous_connection_and_transient_service_error_are_not_replayed(self):
        from botocore.exceptions import ClientError, ConnectionClosedError
        unavailable = (503, {'Content-Type': 'text/xml'}, b'<Response><Errors><Error>'
            b'<Code>ServiceUnavailable</Code><Message>temporary</Message></Error></Errors>'
            b'<RequestID>offline-request</RequestID></Response>')
        success = (200, {'Content-Type': 'text/xml'}, b'<RunInstancesResponse>'
            b'<requestId>offline-request</requestId><reservationId>r-offline</reservationId>'
            b'<instancesSet/></RunInstancesResponse>')
        for failure, expected in ((ConnectionClosedError(endpoint_url='https://ec2.us-east-1.amazonaws.com'),
                                   ConnectionClosedError), (unavailable, ClientError)):
            with self.subTest(failure=failure):
                aws = self.sdk()
                ec2 = aws.client('ec2', self.region)
                http = self.transport(ec2, [failure, success])
                with self.assertRaises(expected):
                    ec2.run_instances(ImageId='ami-0123456789abcdef0', MinCount=6, MaxCount=6,
                        InstanceType='g6.8xlarge', ClientToken='offline-immutable-candidate')
                self.assertEqual(len(http.requests), 1)
                self.assertEqual(http.responses, [success])
                self.assertEqual(self.delays, [])


class PaginationTests(unittest.TestCase):
    def scripted(self, responses):
        calls = []
        def request(**kwargs):
            calls.append(copy.deepcopy(kwargs))
            return responses[len(calls) - 1]
        return request, calls

    def test_next_token_retains_filters_and_collects_all_pricing_pages(self):
        request, calls = self.scripted([
            {'PriceList': ['first'], 'NextToken': 'opaque-1'},
            {'PriceList': ['second'], 'NextToken': 'opaque-2'},
            {'PriceList': ['third']}])
        filters = [{'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': 'g6.8xlarge'}]
        result = list(ci.pages(request, ServiceCode='AmazonEC2', Filters=filters))
        self.assertEqual([item for page in result for item in page['PriceList']], ['first', 'second', 'third'])
        self.assertEqual([call.get('NextToken') for call in calls], [None, 'opaque-1', 'opaque-2'])
        self.assertTrue(all(call['Filters'] == filters and call['ServiceCode'] == 'AmazonEC2' for call in calls))
        self.assertEqual(filters, [{'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': 'g6.8xlarge'}])

    def test_next_token_cycles_and_initial_cursor_echo_fail_closed(self):
        request, calls = self.scripted([{'NextToken': 'one'}, {'NextToken': 'two'}, {'NextToken': 'one'}])
        with self.assertRaisesRegex(ValueError, 'repeated or cycled'):
            list(ci.pages(request))
        self.assertEqual(len(calls), 3)
        request, calls = self.scripted([{'NextToken': 'initial'}])
        with self.assertRaisesRegex(ValueError, 'repeated or cycled'):
            list(ci.pages(request, NextToken='initial'))
        self.assertEqual(len(calls), 1)

    def test_iam_marker_uses_truncation_flag_and_allows_terminal_marker_echo(self):
        request, calls = self.scripted([
            {'PolicyNames': ['run-scope'], 'IsTruncated': True, 'Marker': 'opaque-marker'},
            {'PolicyNames': ['other'], 'IsTruncated': False, 'Marker': 'opaque-marker'}])
        result = list(ci.pages(request, pagination='iam', RoleName='exact-role'))
        self.assertEqual([item for page in result for item in page['PolicyNames']], ['run-scope', 'other'])
        self.assertEqual(calls, [{'RoleName': 'exact-role'}, {'RoleName': 'exact-role', 'Marker': 'opaque-marker'}])

    def test_truncated_responses_require_valid_cursors_and_boolean_flags(self):
        invalid = [('next', {'NextToken': 123}),
                   ('iam', {'IsTruncated': True}), ('iam', {'IsTruncated': 'false'}),
                   ('iam', {'IsTruncated': True, 'Marker': ''}), ('iam', {}),
                   ('s3-objects', {'IsTruncated': True}),
                   ('s3-objects', {'NextContinuationToken': 'more'}),
                   ('s3-multipart', {'IsTruncated': True, 'NextUploadIdMarker': 'orphan'}),
                   ('s3-multipart', {'IsTruncated': True, 'NextKeyMarker': 'key', 'NextUploadIdMarker': 7})]
        for mode, response in invalid:
            with self.subTest(mode=mode, response=response):
                request, calls = self.scripted([response])
                with self.assertRaises(ValueError):
                    list(ci.pages(request, pagination=mode))
                self.assertEqual(len(calls), 1)

    def test_s3_objects_preserve_expected_owner_and_opaque_continuation(self):
        request, calls = self.scripted([
            {'Contents': [{'Key': 'first'}], 'IsTruncated': True, 'NextContinuationToken': 'opaque+/='},
            {'Contents': [{'Key': 'second'}], 'IsTruncated': False}])
        result = list(ci.pages(request, pagination='s3-objects', Bucket='owned',
                              Prefix=RUN + '/', ExpectedBucketOwner=ACCOUNT))
        self.assertEqual([row['Key'] for page in result for row in page['Contents']], ['first', 'second'])
        self.assertEqual(calls[1]['ContinuationToken'], 'opaque+/=')
        self.assertTrue(all(call['ExpectedBucketOwner'] == ACCOUNT and call['Prefix'] == RUN + '/' for call in calls))

    def test_multipart_paired_markers_advance_within_key_and_clear_omitted_upload_marker(self):
        request, calls = self.scripted([
            {'Uploads': [{'UploadId': 'one'}], 'IsTruncated': True,
             'NextKeyMarker': 'same-key', 'NextUploadIdMarker': 'upload-1'},
            {'Uploads': [{'UploadId': 'two'}], 'IsTruncated': True,
             'NextKeyMarker': 'same-key', 'NextUploadIdMarker': 'upload-2'},
            {'Uploads': [{'UploadId': 'three'}], 'IsTruncated': True, 'NextKeyMarker': 'next-prefix'},
            {'Uploads': [], 'IsTruncated': False}])
        result = list(ci.pages(request, pagination='s3-multipart', Bucket='owned', Prefix=RUN + '/',
                              ExpectedBucketOwner=ACCOUNT))
        self.assertEqual([row['UploadId'] for page in result for row in page['Uploads']], ['one', 'two', 'three'])
        self.assertEqual((calls[1]['KeyMarker'], calls[1]['UploadIdMarker']), ('same-key', 'upload-1'))
        self.assertEqual((calls[2]['KeyMarker'], calls[2]['UploadIdMarker']), ('same-key', 'upload-2'))
        self.assertEqual(calls[3]['KeyMarker'], 'next-prefix')
        self.assertNotIn('UploadIdMarker', calls[3])
        self.assertTrue(all(call['ExpectedBucketOwner'] == ACCOUNT for call in calls))

    def test_multipart_repeated_pair_is_not_accepted_as_complete(self):
        response = {'IsTruncated': True, 'NextKeyMarker': 'same', 'NextUploadIdMarker': 'same-upload'}
        request, calls = self.scripted([dict(response), dict(response)])
        with self.assertRaisesRegex(ValueError, 'repeated or cycled'):
            list(ci.pages(request, pagination='s3-multipart'))
        self.assertEqual(len(calls), 2)

    def test_every_protocol_has_a_finite_page_bound(self):
        protocols = {
            'next': [{'NextToken': 'one'}, {'NextToken': 'two'}],
            'iam': [{'IsTruncated': True, 'Marker': 'one'}, {'IsTruncated': True, 'Marker': 'two'}],
            's3-objects': [{'IsTruncated': True, 'NextContinuationToken': 'one'},
                           {'IsTruncated': True, 'NextContinuationToken': 'two'}],
            's3-multipart': [{'IsTruncated': True, 'NextKeyMarker': 'same', 'NextUploadIdMarker': 'one'},
                             {'IsTruncated': True, 'NextKeyMarker': 'same', 'NextUploadIdMarker': 'two'}]}
        for mode, responses in protocols.items():
            with self.subTest(mode=mode):
                request, calls = self.scripted(responses)
                with self.assertRaisesRegex(RuntimeError, 'page bound exhausted'):
                    list(ci.pages(request, pagination=mode, max_pages=2))
                self.assertEqual(len(calls), 2)

    def test_invalid_bounds_and_orphan_initial_upload_marker_make_no_sdk_call(self):
        for kwargs in ({'max_pages': 0}, {'max_pages': -1}, {'max_pages': 1001}, {'max_pages': True},
                       {'pagination': 'unknown'}, {'pagination': 's3-multipart', 'UploadIdMarker': 'orphan'}):
            request, calls = self.scripted([])
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                list(ci.pages(request, **kwargs))
            self.assertEqual(calls, [])

    def test_sdk_read_error_propagates_without_hidden_retry(self):
        calls = []
        def denied(**kwargs):
            calls.append(kwargs)
            raise ServiceError('AccessDenied')
        with self.assertRaisesRegex(ServiceError, 'AccessDenied'):
            list(ci.pages(denied))
        self.assertEqual(len(calls), 1)


class S3:
    def __init__(self):
        self.objects, self.etags = {}, {}
        self.unavailable = False

    def get_object(self, **kwargs):
        if self.unavailable:
            raise OSError('S3 unavailable')
        key = kwargs['Key']
        if key not in self.objects:
            raise ServiceError('NoSuchKey')
        return {'Body': io.BytesIO(self.objects[key]), 'ETag': self.etags[key], 'ContentLength': len(self.objects[key])}

    def put_object(self, **kwargs):
        key = kwargs['Key']
        if kwargs.get('IfNoneMatch') == '*' and key in self.objects:
            raise ServiceError('PreconditionFailed')
        if 'IfMatch' in kwargs and kwargs['IfMatch'] != self.etags.get(key):
            raise ServiceError('PreconditionFailed')
        assert kwargs['ExpectedBucketOwner'] == ACCOUNT
        assert kwargs['ServerSideEncryption'] == 'AES256'
        self.objects[key] = kwargs['Body']
        self.etags[key] = '"' + hashlib.sha256(kwargs['Body']).hexdigest() + '"'
        return {'ETag': self.etags[key]}


    def list_multipart_uploads(self, **kwargs):
        if self.unavailable:
            raise OSError('S3 unavailable')
        return {'Uploads': [], 'IsTruncated': False}

    def list_objects_v2(self, **kwargs):
        return {'Contents': [dict(Key=key, Size=len(value), ETag=self.etags[key])
                             for key, value in sorted(self.objects.items()) if key.startswith(kwargs['Prefix'])],
                'IsTruncated': False}

    def head_object(self, **kwargs):
        value = self.objects[kwargs['Key']]
        return dict(ContentLength=len(value), ETag=self.etags[kwargs['Key']],
                    Metadata={'sha256': hashlib.sha256(value).hexdigest()})

    def download_file(self, bucket, key, destination, **kwargs):
        Path(destination).write_bytes(self.objects[key])


class EC2:
    def __init__(self, region, clock):
        self.region, self.clock = region, clock
        self.instances, self.requests, self.terminations = [], [], []
        self.outcomes = []
        self.fail_termination = set()
        self.fail_inventory = False
        self.never_terminates = set()
        self.hide_tag_inventory = False


    def describe_instances(self, **kwargs):
        if self.fail_inventory:
            raise OSError('regional inventory unavailable')
        instances = self.instances
        if self.hide_tag_inventory and kwargs.get('Filters'):
            instances = []
        if kwargs.get('InstanceIds'):
            instances = [item for item in instances if item['InstanceId'] in kwargs['InstanceIds']]
        for condition in kwargs.get('Filters', []):
            if condition['Name'].startswith('tag:'):
                name = condition['Name'][4:]
                instances = [item for item in instances if ci.tag_map(item).get(name) in condition['Values']]
        return {'Reservations': [{'OwnerId': ACCOUNT, 'Instances': copy.deepcopy(instances)}]}

    def run_instances(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        outcome = self.outcomes.pop(0) if self.outcomes else 'success'
        if outcome == 'capacity':
            raise ServiceError('InsufficientInstanceCapacity')
        if outcome == 'unknown-empty':
            raise TimeoutError('lost launch acknowledgement')
        count = 3 if outcome == 'partial' else 6
        tags = next(item['Tags'] for item in kwargs['TagSpecifications'] if item['ResourceType'] == 'instance')
        for index in range(count):
            self.instances.append(dict(InstanceId=f'i-{index + 1:017x}', AmiLaunchIndex=index,
                InstanceType=kwargs['InstanceType'], ClientToken=kwargs['ClientToken'],
                LaunchTime=datetime.fromtimestamp(self.clock.time(), timezone.utc),
                PrivateIpAddress=f'10.0.0.{index+10}', State={'Name': 'running'},
                Placement=copy.deepcopy(kwargs['Placement']), Tags=copy.deepcopy(tags)))
        if outcome == 'unknown-six':
            raise TimeoutError('lost launch acknowledgement after AWS allocated six')
        return {'Instances': copy.deepcopy(self.instances)}

    def terminate_instances(self, **kwargs):
        self.terminations.append(kwargs['InstanceIds'])
        for instance_id in kwargs['InstanceIds']:
            if instance_id in self.fail_termination:
                raise ServiceError('UnauthorizedOperation')
            for item in self.instances:
                if item['InstanceId'] == instance_id:
                    item['State']['Name'] = 'shutting-down' if instance_id in self.never_terminates else 'terminated'
        return {}

    def describe_network_interfaces(self, **kwargs):
        return {'NetworkInterfaces': []}

    def describe_volumes(self, **kwargs):
        return {'Volumes': []}

    def describe_security_groups(self, **kwargs):
        return {'SecurityGroups': []}

    def describe_placement_groups(self, **kwargs):
        return {'PlacementGroups': []}


class IAM:
    def __init__(self):
        self.roles, self.profiles, self.inline, self.attached = {}, {}, {}, {}
        self.created, self.deleted = [], []


    def get_role(self, **kwargs):
        name = kwargs['RoleName']
        if name not in self.roles:
            raise ServiceError('NoSuchEntity')
        return {'Role': copy.deepcopy(self.roles[name])}

    def create_role(self, **kwargs):
        name = kwargs['RoleName']
        self.roles[name] = dict(RoleName=name, Arn=f'arn:aws:iam::{ACCOUNT}:role/{name}',
            AssumeRolePolicyDocument=json.loads(kwargs['AssumeRolePolicyDocument']), Tags=kwargs['Tags'],
            PermissionsBoundary={'PermissionsBoundaryArn': kwargs['PermissionsBoundary']})
        self.inline[name], self.attached[name] = {}, []
        self.created.append(name)
        return self.get_role(RoleName=name)

    def list_role_policies(self, **kwargs):
        return {'PolicyNames': list(self.inline[kwargs['RoleName']]), 'IsTruncated': False}

    def list_attached_role_policies(self, **kwargs):
        return {'AttachedPolicies': [dict(PolicyArn=arn) for arn in self.attached[kwargs['RoleName']]],
                'IsTruncated': False}

    def attach_role_policy(self, **kwargs):
        self.attached[kwargs['RoleName']].append(kwargs['PolicyArn'])
        return {}

    def put_role_policy(self, **kwargs):
        self.inline[kwargs['RoleName']][kwargs['PolicyName']] = json.loads(kwargs['PolicyDocument'])
        return {}

    def get_role_policy(self, **kwargs):
        policy = self.inline.get(kwargs['RoleName'], {}).get(kwargs['PolicyName'])
        if policy is None:
            raise ServiceError('NoSuchEntity')
        return {'PolicyDocument': copy.deepcopy(policy)}

    def get_instance_profile(self, **kwargs):
        name = kwargs['InstanceProfileName']
        if name not in self.profiles:
            raise ServiceError('NoSuchEntity')
        return {'InstanceProfile': copy.deepcopy(self.profiles[name])}

    def create_instance_profile(self, **kwargs):
        name = kwargs['InstanceProfileName']
        self.profiles[name] = dict(InstanceProfileName=name, Arn=f'arn:aws:iam::{ACCOUNT}:instance-profile/{name}',
                                   Roles=[], Tags=kwargs['Tags'])
        return self.get_instance_profile(InstanceProfileName=name)

    def add_role_to_instance_profile(self, **kwargs):
        self.profiles[kwargs['InstanceProfileName']]['Roles'] = [copy.deepcopy(self.roles[kwargs['RoleName']])]
        return {}

    def remove_role_from_instance_profile(self, **kwargs):
        self.profiles[kwargs['InstanceProfileName']]['Roles'] = []
        return {}

    def delete_instance_profile(self, **kwargs):
        del self.profiles[kwargs['InstanceProfileName']]
        return {}

    def delete_role_policy(self, **kwargs):
        del self.inline[kwargs['RoleName']][kwargs['PolicyName']]
        return {}

    def detach_role_policy(self, **kwargs):
        self.attached[kwargs['RoleName']].remove(kwargs['PolicyArn'])
        return {}

    def delete_role(self, **kwargs):
        name = kwargs['RoleName']
        self.deleted.append(name)
        del self.roles[name]
        return {}


class AWS:
    def __init__(self, clock):
        self.s3 = S3()
        self.iam = IAM()
        self.ec2 = {region: EC2(region, clock) for region in ('us-east-1', 'us-east-2')}

    def client(self, service, region):
        if service == 's3':
            return self.s3
        if service == 'iam':
            return self.iam
        if service == 'ec2':
            return self.ec2[region]
        if service == 'sts':
            return SimpleNamespace(get_caller_identity=lambda: {'Account': ACCOUNT})
        raise AssertionError('unexpected AWS service ' + service)


class Ledger:
    reservations = []
    settlements = {}
    policies = {}

    def __init__(self, *args, **kwargs):
        self.budget_path = kwargs.get('budget_path', ci.DEFAULT_BUDGET)

    def reserve(self, run_id, estimate, provenance):
        self.reservations.append((run_id, estimate, provenance))
        policy = self.policies.setdefault(run_id, ci.load_budget(self.budget_path))
        return {'run_id': run_id, 'budget_policy': copy.deepcopy(policy)}

    def settle(self, run_id, usage, receipt):
        value = dict(usage_usd=str(usage), receipt=copy.deepcopy(receipt))
        previous = self.settlements.get(run_id)
        if previous and previous['receipt']['cleanup_complete'] and previous != value:
            raise ValueError('completed settlement identity changed')
        self.settlements[run_id] = value
        return value

    def report(self, run_id=None):
        return dict(budget_policy=ci.load_budget(self.budget_path),
                    runs={key: dict(budget_policy=value) for key, value in self.policies.items()
                          if run_id is None or key == run_id})


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.clock = Clock()
        self.budget_file = self.output / 'budget.json'
        ci.dump(self.budget_file, {'weekly_budget_usd': '60'})
        self.cfg = dict(account=ACCOUNT, bucket='owned-results', bucket_region='us-east-1',
            regions=['us-east-1', 'us-east-2'], budget_path=str(self.budget_file),
            cost_assumptions=dict(retention_days=30, result_gib=2, request_count=100000, reconciliation_seconds=600))
        self.cfg['guest_boundary_arn'] = f'arn:aws:iam::{ACCOUNT}:policy/tidehunter-aws-benchmarks-guest-boundary'
        self.cfg.update(transports=['tcp', 'efa', 'efa-direct'], repetitions=1,
                        read_profile_limits=[0], write_profile_limits=[0], native_write_profile_limits=[0],
                        kv_suite=dict(scope='full', repetitions=1, diagnostics=True))
        Ledger.settlements = {}
        Ledger.policies = {}
        self.aws = AWS(self.clock)
        self.campaign = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock)
        self.admission = dict(bindings={'source_archive_sha256': 'a' * 64}, inputs={},
                              bootstrap_sha256='b' * 64, atomic_start_sha256='c' * 64)
        self.candidates = [dict(region='us-east-1', az='us-east-1a', instance_type='g6.8xlarge',
                               ami='ami-reviewed-east1', subnet='subnet-public-east1',
                               maximum_estimate_usd='12', host_hourly_usd='2', price={}),
                           dict(region='us-east-2', az='us-east-2b', instance_type='g6e.8xlarge',
                               ami='ami-reviewed-east2', subnet='subnet-public-east2',
                               maximum_estimate_usd='18', host_hourly_usd='3', price={})]
        self.preflight = dict(candidates=self.candidates, budget_policy=ci.load_budget(self.budget_file), boundary=dict(
            arn=self.cfg['guest_boundary_arn'], version='v1', sha256='d' * 64),
            ancillary_prices=dict(storage_usd_per_gib_month='0.023', request_usd='0.000005',
                                  transfer_usd_per_gib='0.09'))
        self.network = patch.object(self.campaign, 'network', return_value='sg-owned').start()
        self.addCleanup(patch.stopall)
        patch('ci_budget.BudgetLedger', Ledger).start()
        patch.object(self.campaign, 'request', side_effect=self.request).start()

    def request(self, candidate, group_id, admission, profiles):
        tags = ci.ownership(RUN) | {'ExpiresAt': self.campaign.journal.data['expires_at']}
        token = ci.candidate_token(RUN, candidate)
        return (dict(MinCount=6, MaxCount=6, ClientToken=token, InstanceType=candidate['instance_type'],
                     Placement=dict(GroupName=RUN, AvailabilityZone=candidate['az']), UserData='source-bound-bootstrap',
                     TagSpecifications=[dict(ResourceType='instance', Tags=ci.aws_tags(tags))]),
                dict(instance_profiles=profiles, atomic_client_token=token))

    def allocate(self):
        return self.campaign.allocate(self.admission, self.preflight)

    def test_atomic_six_count_stable_token_and_durable_intent(self):
        original = self.aws.ec2['us-east-1'].run_instances
        def after_durable_intent(**request):
            stored = json.loads(self.aws.s3.objects[self.campaign.journal.key])
            self.assertEqual(stored['attempts'][-1]['status'], 'allocation-outcome-unknown')
            self.assertEqual(stored['attempts'][-1]['client_token'], request['ClientToken'])
            self.assertEqual(stored['plan']['atomic_client_token'], request['ClientToken'])
            self.assertEqual(stored['mutations'][-1]['operation'], 'run_instances')
            return original(**request)
        with patch.object(self.aws.ec2['us-east-1'], 'run_instances', side_effect=after_durable_intent):
            instances, candidate, plan = self.allocate()
        self.assertEqual(len(instances), 6)
        request = self.aws.ec2['us-east-1'].requests[0]
        self.assertEqual((request['MinCount'], request['MaxCount']), (6, 6))
        self.assertEqual(request['ClientToken'], ci.candidate_token(RUN, candidate))
        remote = json.loads(self.aws.s3.objects[self.campaign.journal.key])
        self.assertEqual(remote['attempts'][0]['status'], 'allocated')
        self.assertEqual(remote['attempts'][0]['returned_ids'], sorted(item['InstanceId'] for item in instances))
        self.assertIn('run_instances', [item['operation'] for item in remote['mutations']])

    def test_unknown_empty_launch_never_falls_back_or_relaunches_on_restart(self):
        self.aws.ec2['us-east-1'].outcomes = ['unknown-empty']
        with self.assertRaisesRegex(RuntimeError, 'outcome unknown'):
            self.allocate()
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])
        restarted = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock)
        with self.assertRaisesRegex(RuntimeError, 'never allocate again'):
            restarted.allocate(self.admission, self.preflight)
        cleanup = restarted.cleanup()
        self.assertFalse(cleanup['cleanup_complete'])
        self.assertIn('outcome still unknown', ' '.join(cleanup['errors']))
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        token = self.aws.ec2['us-east-1'].requests[0]['ClientToken']
        self.assertEqual(token, ci.candidate_token(RUN, self.candidates[0]))
        self.assertEqual(token, restarted.journal.data['attempts'][0]['client_token'])
        self.assertEqual(token, ci.candidate_token(RUN, self.candidates[0] | {'price': {'new observation': 'same launch identity'}}))

    def test_unknown_launch_with_six_visible_hosts_is_reconciled_not_duplicated(self):
        self.aws.ec2['us-east-1'].outcomes = ['unknown-six']
        instances, candidate, plan = self.allocate()
        self.assertEqual(len(instances), 6)
        self.assertEqual(self.campaign.journal.data['attempts'][0]['status'], 'reconciled-six-host-allocation')
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_confirmed_capacity_failure_uses_next_candidate_token_without_extending_expiry(self):
        self.aws.ec2['us-east-1'].outcomes = ['capacity']
        instances, candidate, plan = self.allocate()
        self.assertEqual(candidate['region'], 'us-east-2')
        first = self.aws.ec2['us-east-1'].requests[0]
        second = self.aws.ec2['us-east-2'].requests[0]
        self.assertNotEqual(first['ClientToken'], second['ClientToken'])
        self.assertEqual(first['TagSpecifications'], second['TagSpecifications'])
        self.assertEqual(self.campaign.journal.data['attempts'][0]['status'], 'capacity-rejected')
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_same_az_type_fallback_uses_distinct_immutable_candidate_token(self):
        first = self.candidates[0]
        second = first | {'instance_type': 'g6e.8xlarge'}
        self.preflight['candidates'] = [first, second]
        self.aws.ec2['us-east-1'].outcomes = ['capacity', 'success']
        instances, candidate, plan = self.allocate()
        requests = self.aws.ec2['us-east-1'].requests
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0]['Placement'], requests[1]['Placement'])
        self.assertNotEqual(requests[0]['ClientToken'], requests[1]['ClientToken'])
        self.assertEqual(requests[1]['ClientToken'], ci.candidate_token(RUN, second))
        self.assertEqual(plan['atomic_client_token'], requests[1]['ClientToken'])
        self.assertEqual(requests[0]['TagSpecifications'], requests[1]['TagSpecifications'])
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_partial_allocation_terminates_every_visible_host_and_retains_uncertainty(self):
        self.aws.ec2['us-east-1'].outcomes = ['partial']
        with self.assertRaisesRegex(ValueError, 'exactly six'):
            self.allocate()
        cleanup = self.campaign.cleanup()
        self.assertFalse(cleanup['cleanup_complete'])
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 3)
        self.assertTrue(all(item['State']['Name'] == 'terminated' for item in self.aws.ec2['us-east-1'].instances))
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])

    def test_cleanup_attempts_remaining_instances_after_one_termination_fails(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        ec2.fail_termination.add(ec2.instances[0]['InstanceId'])
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(len(ec2.terminations), 6)
        self.assertEqual(sum(item['State']['Name'] == 'terminated' for item in ec2.instances), 5)

    def test_cleanup_requires_actual_terminated_state_not_request_acknowledgement(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        ec2.never_terminates.add(ec2.instances[0]['InstanceId'])
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertIn('did not confirm terminated', ' '.join(receipt['errors']))

    def test_inaccessible_region_does_not_prevent_shutdown_in_other_region(self):
        self.allocate()
        self.aws.ec2['us-east-2'].fail_inventory = True
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)

    def test_fresh_cleanup_without_readable_journal_rejects_unapproved_tokens(self):
        self.allocate()
        self.aws.s3.unavailable = True
        emergency = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock, cleanup_only=True)
        receipt = emergency.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
        self.assertIn('journal unavailable', ' '.join(receipt['errors']))

    def test_loaded_journal_still_allows_owned_shutdown_during_s3_outage(self):
        self.allocate()
        self.aws.s3.unavailable = True
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)

    def test_foreign_token_is_not_terminated_despite_matching_tags(self):
        self.allocate()
        self.aws.ec2['us-east-1'].instances[0]['ClientToken'] = 'foreign-token'
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 5)
        self.assertEqual(self.aws.ec2['us-east-1'].instances[0]['State']['Name'], 'running')

    def test_restart_with_source_mismatch_cannot_launch(self):
        self.allocate()
        changed = copy.deepcopy(self.admission)
        changed['bindings']['source_archive_sha256'] = 'f' * 64
        restarted = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock)
        with self.assertRaisesRegex(ValueError, 'source/config identity differs'):
            restarted.allocate(changed, self.preflight)
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])

    def test_completed_attempt_cannot_allocate_another_fleet(self):
        self.allocate()
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])
        with self.assertRaisesRegex(RuntimeError, 'completed attempt'):
            self.allocate()
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)

    def test_s3_journal_compare_and_swap_blocks_stale_writer(self):
        self.allocate()
        stale = ci.Journal(self.aws.s3, self.cfg, RUN, self.output)
        self.campaign.journal.data['event'] = 'new-write'
        self.campaign.journal.save()
        stale.data['event'] = 'stale-write'
        with self.assertRaisesRegex(ServiceError, 'PreconditionFailed'):
            stale.save()
        self.assertEqual(json.loads(self.aws.s3.objects[stale.key])['event'], 'new-write')

    def test_source_archive_mismatch_is_rejected_before_any_aws_client(self):
        build = self.output / 'build'
        build.mkdir()
        (build / 'sources.tar.gz').write_bytes(b'not-the-admitted-source')
        ci.dump(build / 'source-manifest.json', {})
        ci.dump(build / 'build-manifest.json', dict(protocol=8, fabric=True,
            binaries={name: 'a' * 64 for name in ci.BINARIES}, source_archive_sha256='0' * 64,
            source_manifest_sha256=ci.sha(build / 'source-manifest.json')))
        fixture = self.output / 'fixture.tar.gz'
        fixture.write_bytes(b'fixture')
        verification = self.output / 'verification'
        verification.mkdir()
        with patch.object(ci, 'AWS', side_effect=AssertionError('no credentials before source admission')):
            with self.assertRaisesRegex(ValueError, 'source-bound fabric build'):
                ci.admit_inputs(build, fixture, verification, self.output / 'admission')

    def test_configure_failure_terminates_before_any_local_download(self):
        observations = []
        def retrieve():
            observations.append([item['State']['Name'] for item in self.aws.ec2['us-east-1'].instances])
        with patch.object(self.campaign, 'configure', side_effect=ValueError('profile mismatch')), \
                patch.object(self.campaign, 'settle'), patch.object(self.campaign, 'retrieve', side_effect=retrieve):
            with self.assertRaisesRegex(ValueError, 'profile mismatch'):
                self.campaign.run(self.admission, self.preflight)
        self.assertEqual(observations, [['terminated'] * 6])

    def test_full_prefix_retrieval_preserves_unexpected_raw_and_partial_files(self):
        keys = [RUN + '/inputs/build-bundle.tar.gz', RUN + '/results/raw/nested/stderr.log',
                RUN + '/results/cells/partial/startup.json']
        for index, key in enumerate(keys):
            self.aws.s3.put_object(Key=key, Body=f'raw-evidence-{index}'.encode(),
                ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        receipt = self.campaign.retrieve()
        self.assertTrue(receipt['complete'])
        self.assertEqual({item['key'] for item in receipt['files']}, set(keys))
        for key in keys:
            self.assertEqual((self.output / 's3' / key).read_bytes(), self.aws.s3.objects[key])

    def test_multipart_cleanup_collects_same_key_uploads_across_paired_markers(self):
        class MultipartS3(S3):
            def __init__(self):
                super().__init__()
                self.pending, self.aborted, self.calls = ['upload-1', 'upload-2'], [], []
            def list_multipart_uploads(self, **params):
                self.calls.append(copy.deepcopy(params))
                if not self.pending:
                    return {'Uploads': [], 'IsTruncated': False}
                if params.get('UploadIdMarker') == 'upload-1':
                    return {'Uploads': [dict(Key=RUN + '/input/bundle.tar.gz', UploadId='upload-2')],
                            'IsTruncated': False}
                return {'Uploads': [dict(Key=RUN + '/input/bundle.tar.gz', UploadId='upload-1')],
                        'IsTruncated': True, 'NextKeyMarker': RUN + '/input/bundle.tar.gz',
                        'NextUploadIdMarker': 'upload-1'}
            def abort_multipart_upload(self, **params):
                self.aborted.append(params['UploadId'])
                self.pending.remove(params['UploadId'])
        client = MultipartS3()
        self.campaign.s3 = client
        receipt = {'errors': []}
        self.campaign.cleanup_multipart(receipt)
        self.assertEqual(receipt['errors'], [])
        self.assertTrue(receipt['multipart_cleanup_complete'])
        self.assertEqual(client.aborted, ['upload-1', 'upload-2'])
        self.assertEqual(client.calls[1]['KeyMarker'], RUN + '/input/bundle.tar.gz')
        self.assertTrue(all(call['ExpectedBucketOwner'] == ACCOUNT for call in client.calls))

    def test_missing_requested_transport_cannot_pass_result_validation(self):
        root = self.output / 's3' / RUN / 'results'
        root.mkdir(parents=True)
        staging = self.output / 'staging'
        (staging / 'results').mkdir(parents=True)
        ci.dump(staging / 'results/campaign-receipt.json', dict(run_id=RUN, complete=True, failures=[], cells=[]))
        archive = root / 'controller.tar.gz'
        with tarfile.open(archive, 'w:gz') as stream:
            stream.add(staging / 'results', arcname='results')
        ci.dump(root / 'receipt.json', dict(complete=True, sha256=ci.sha(archive)))
        cfg = dict(transports=['tcp', 'efa', 'efa-direct'], repetitions=1, read_profile_limits=[0])
        with self.assertRaisesRegex(ValueError, 'matrix misses requested'):
            self.campaign.validate_results(cfg)

    def no_launch_cleanup(self):
        with patch.object(self.campaign, 'request', side_effect=ValueError('EC2 user data exceeds 16KiB')):
            with self.assertRaisesRegex(ValueError, '16KiB'):
                self.allocate()
        receipt = self.campaign.cleanup()
        self.assertTrue(receipt['cleanup_complete'], receipt['errors'])
        self.assertEqual(receipt['instances'], [])
        self.assertTrue(all(not ec2.requests for ec2 in self.aws.ec2.values()))
        return receipt

    def test_no_launch_books_only_observed_elapsed_storage_not_admission_assumptions(self):
        cleanup = self.no_launch_cleanup()
        inventory = [dict(Key=RUN + '/inputs/build-bundle.tar.gz', Size=245450666,
                          LastModified=datetime.fromtimestamp(self.clock.time() - 120, timezone.utc))]
        record = self.campaign.account_export(inventory)
        self.campaign.settle(cleanup)
        settled = Ledger.settlements[RUN]
        evidence = settled['receipt']['usage_evidence']
        self.assertEqual(evidence['accounting_basis'], 'observed-resources-v2')
        self.assertEqual(Decimal(evidence['compute_estimate_usd']), 0)
        self.assertEqual(evidence['instances'], [])
        self.assertEqual(evidence['elapsed_seconds'], '0')
        self.assertIsNone(record['request_estimate_usd'])
        self.assertIsNone(record['transfer_estimate_usd'])
        self.assertEqual(settled['usage_usd'], record['storage_estimate_usd'])
        self.assertLess(Decimal(settled['usage_usd']), Decimal('0.50'))
        self.assertGreater(Decimal(record['projections']['amount_usd']), Decimal(settled['usage_usd']))
        self.assertEqual(evidence['billing'], dict(status='unavailable', amount_usd=None))
        self.assertIn('historical/guest/managed-transfer requests and retries', record['unknown_components'])
        self.assertEqual(self.campaign.journal.data['overhead']['assumptions']['request_count'], 100000)

    def test_elapsed_storage_and_future_projection_are_separate_measures(self):
        self.no_launch_cleanup()
        inventory = [dict(Key=RUN + '/inputs/fixture.tar.gz', Size=1024**3,
                          LastModified=datetime.fromtimestamp(self.clock.time() - 86400, timezone.utc))]
        first = self.campaign.account_export(inventory)
        with localcontext() as context:
            context.prec = 60
            expected = ci.usd_ceiling(Decimal('0.023') / Decimal(28))
        self.assertEqual(Decimal(first['storage_estimate_usd']), expected)
        self.assertEqual(Decimal(first['estimate_usd']), expected)
        self.assertEqual(first['storage_observed_byte_seconds'], str(Decimal(1024**3 * 86400)))
        self.clock.sleep(3600)
        later = self.campaign.account_export(inventory)
        self.assertGreater(Decimal(later['storage_estimate_usd']), Decimal(first['storage_estimate_usd']))
        self.assertEqual(later['projections'], first['projections'])
        self.assertIn('excluded from usage_usd', first['projections']['basis'])

    def test_known_local_retrieval_bytes_are_cumulative_without_guest_multipliers(self):
        self.no_launch_cleanup()
        contents = {RUN + '/inputs/fixture.tar.gz': b'input-bytes',
                    RUN + '/results/raw/partial.log': b'actual-result'}
        for key, value in contents.items():
            self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=value,
                ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        for count in (1, 2):
            receipt = self.campaign.retrieve()
            record = receipt['s3_accounting']
            observed = record['observations']
            byte_count = sum(map(len, contents.values()))
            self.assertEqual(observed['successful_local_read_bytes'], byte_count)
            self.assertEqual(observed['successful_requests'], dict(ListObjectsV2=count, HeadObject=count * len(contents)))
            with localcontext() as context:
                context.prec = 60
                expected = ci.usd_ceiling(Decimal(byte_count) / Decimal(1024**3) * Decimal('0.09'))
            self.assertEqual(Decimal(record['transfer_estimate_usd']), expected)
            self.assertEqual(record['coverage'], 'partial')
            self.assertEqual({item['key'] for item in receipt['files']}, set(contents))
            self.assertEqual({item['retrieval_mode'] for item in receipt['files']},
                             {'download' if count == 1 else 'local-sha256-reuse'})
        restarted = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock, cleanup_only=True)
        self.assertEqual(restarted.journal.data['s3_observations'], observed)
        key = RUN + '/inputs/fixture.tar.gz'
        (self.output / 's3' / key).write_bytes(b'changed-local-input')
        fetched = restarted.retrieve()
        self.assertEqual(fetched['s3_accounting']['observations']['successful_local_read_bytes'],
                         byte_count + len(contents[key]))
        modes = {item['key']: item['retrieval_mode'] for item in fetched['files']}
        self.assertEqual(modes[key], 'download')
        self.assertEqual(modes[RUN + '/results/raw/partial.log'], 'local-sha256-reuse')

    def test_local_reuse_requires_remote_sha_metadata_not_just_size_or_etag(self):
        self.no_launch_cleanup()
        key, value = RUN + '/inputs/unbound.tar.gz', b'unbound-input'
        self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=value,
            ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        target = self.output / 's3' / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(value)
        original_head, original_download = self.aws.s3.head_object, self.aws.s3.download_file
        def unbound_head(**kwargs):
            head = original_head(**kwargs)
            head['Metadata'] = {}
            return head
        with patch.object(self.aws.s3, 'head_object', side_effect=unbound_head), \
                patch.object(self.aws.s3, 'download_file', wraps=original_download) as download:
            receipt = self.campaign.retrieve()
        download.assert_called_once()
        self.assertEqual(receipt['files'][0]['retrieval_mode'], 'download')
        self.assertEqual(receipt['s3_accounting']['observations']['successful_local_read_bytes'], len(value))

    def test_input_verification_accounts_actual_completed_read_length(self):
        self.no_launch_cleanup()
        path, key = self.output / 'verified-input.tar.gz', RUN + '/inputs/verified-input.tar.gz'
        path.write_bytes(b'expected-complete-input')
        self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=path.read_bytes(),
            ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        # A complete shorter response must fail admission, not invent declared bytes.
        with patch.object(self.aws.s3, 'get_object', return_value={'Body': io.BytesIO(b'short')}):
            with self.assertRaisesRegex(ValueError, 'full-object digest differs'):
                self.campaign.upload(path, key)
        counters = self.campaign.journal.data['s3_observations']
        self.assertEqual(counters['successful_local_read_bytes'], len(b'short'))
        self.assertEqual(counters['successful_requests'], dict(HeadObject=1, GetObject=1))
        binding = self.campaign.upload(path, key)
        self.assertEqual(binding['bytes'], path.stat().st_size)
        self.assertEqual(counters['successful_local_read_bytes'], len(b'short') + path.stat().st_size)

    def test_partial_retrieval_does_not_book_failed_or_unverified_bytes_as_success(self):
        self.no_launch_cleanup()
        good, failed = RUN + '/results/complete.log', RUN + '/results/interrupted.log'
        for key in (good, failed):
            self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=b'complete-value',
                ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        original = self.aws.s3.download_file
        def interrupted(bucket, key, destination, **kwargs):
            if key == failed:
                Path(destination).write_bytes(b'partial')
                raise OSError('download interrupted')
            return original(bucket, key, destination, **kwargs)
        with patch.object(self.aws.s3, 'download_file', side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, 'retrieval failed'):
                self.campaign.retrieve()
        manifest = ci.read_json(self.output / 'retrieval-manifest.json')
        observed = manifest['s3_accounting']['observations']
        self.assertFalse(manifest['complete'])
        self.assertEqual(observed['successful_local_read_bytes'], len(b'complete-value'))
        self.assertEqual(observed['failed_managed_downloads'], 1)
        self.assertEqual({item['key'] for item in manifest['files']}, {good})
        self.assertIn('failed/partial reads', ' '.join(manifest['s3_accounting']['unknown_components']))

    def test_completed_download_is_counted_even_when_later_integrity_check_fails(self):
        self.no_launch_cleanup()
        key, value = RUN + '/results/corrupt.log', b'completed-transfer'
        self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=value,
            ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
        with patch.object(self.aws.s3, 'head_object', return_value=dict(ContentLength=len(value),
                          Metadata={'sha256': 'f' * 64})):
            with self.assertRaisesRegex(RuntimeError, 'retrieval failed'):
                self.campaign.retrieve()
        manifest = ci.read_json(self.output / 'retrieval-manifest.json')
        self.assertFalse(manifest['complete'])
        self.assertEqual(manifest['s3_accounting']['observations']['successful_local_read_bytes'], len(value))
        self.assertEqual(manifest['s3_accounting']['observations']['failed_managed_downloads'], 0)
        self.assertEqual(manifest['files'], [])

    def test_missing_observations_remain_unknown_and_never_fall_back_to_forecast(self):
        cleanup = self.no_launch_cleanup()
        self.campaign.settle(cleanup)
        evidence = Ledger.settlements[RUN]['receipt']['usage_evidence']
        self.assertEqual(Decimal(Ledger.settlements[RUN]['usage_usd']), 0)
        self.assertEqual(evidence['s3']['coverage'], 'unavailable')
        self.assertIsNone(evidence['s3']['estimate_usd'])
        self.assertIsNone(evidence['projections']['amount_usd'])
        # A later measured inventory with no timestamps cannot invent object ages.
        record = self.campaign.account_export([dict(Key=RUN + '/inputs/fixture.tar.gz', Size=1024**3)])
        self.assertIsNone(record['storage_estimate_usd'])
        self.assertIsNone(record['request_estimate_usd'])
        self.assertIsNone(record['transfer_estimate_usd'])
        self.assertEqual(record['storage_missing_timestamps'], 1)
        self.assertIn('elapsed storage for objects without LastModified', record['unknown_components'])

    def test_legacy_reconciliation_uses_real_ledger_cas_history_and_idempotency(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            cleanup = self.no_launch_cleanup()
            old = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True, cleanup_evidence=cleanup,
                usage_evidence=dict(instances=[], elapsed_seconds='0', s3=dict(request_estimate_usd='0.50'))))
            ledger.settle(RUN, old['usage_usd'], old['receipt'])
            self.campaign.journal.data['settlement'] = copy.deepcopy(old)
            self.campaign.journal.save()
            original = ledger.report(run_id=RUN)['runs'][RUN]
            # Default settle never silently rewrites a legacy completed estimate.
            self.campaign.account_export([dict(Key=RUN + '/inputs/fixture.tar.gz', Size=245450666,
                LastModified=datetime.fromtimestamp(self.clock.time() - 120, timezone.utc))])
            self.campaign.settle(cleanup)
            self.assertEqual(ledger.report(run_id=RUN)['runs'][RUN]['receipt'], old['receipt'])
            with self.assertRaisesRegex(ValueError, 'digest does not match'):
                self.campaign.settle(cleanup, reconcile_digest='f' * 64)
            self.assertNotIn('usage_reconciliation', self.campaign.journal.data)
            digest = original['settlement_digest']
            self.campaign.settle(cleanup, reconcile_digest=digest)
            report = ledger.report(run_id=RUN)
            corrected = report['runs'][RUN]
            history = corrected['usage_reconciliation_history']
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]['receipt'], old['receipt'])
            self.assertEqual(history[0]['usage_estimated_usd'], old['usage_usd'])
            self.assertEqual(history[0]['settlement_digest'], digest)
            self.assertEqual(corrected['reserved_usd'], original['reserved_usd'])
            self.assertEqual(corrected['settled_at'], original['settled_at'])
            self.assertEqual(corrected['active_reservation_usd'], '0')
            self.assertIsNone(corrected['aws_billed_to_date_usd'])
            self.assertEqual(report['weekly_budget_usd'], '60')
            self.assertEqual(corrected['usage_accounting_basis'], 'observed-resources-v2')
            self.assertLess(Decimal(corrected['usage_estimated_usd']), Decimal(old['usage_usd']))
            self.clock.sleep(3600)
            self.campaign.settle(cleanup, reconcile_digest=digest)
            replayed = ledger.report(run_id=RUN)['runs'][RUN]
            self.assertEqual(replayed, corrected)
            self.assertEqual(self.campaign.journal.data['usage_reconciliation']['previous_settlement'], old)

    def test_reconciliation_unknown_acknowledgement_replays_persisted_receipt(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            cleanup = self.no_launch_cleanup()
            old = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True, cleanup_evidence=cleanup,
                usage_evidence=dict(instances=[], elapsed_seconds='0', s3=dict(request_estimate_usd='0.50'))))
            ledger.settle(RUN, old['usage_usd'], old['receipt'])
            self.campaign.journal.data['settlement'] = copy.deepcopy(old)
            self.campaign.journal.save()
            digest = ledger.report(run_id=RUN)['runs'][RUN]['settlement_digest']
            self.campaign.account_export([dict(Key=RUN + '/inputs/fixture.tar.gz', Size=245450666,
                LastModified=datetime.fromtimestamp(self.clock.time() - 120, timezone.utc))])
            original = ActualBudgetLedger.reconcile_usage
            def committed_but_unknown(owner, *args, **kwargs):
                original(owner, *args, **kwargs)
                raise OSError('acknowledgement unavailable after committed conditional write')
            with patch.object(ActualBudgetLedger, 'reconcile_usage', new=committed_but_unknown):
                with self.assertRaisesRegex(OSError, 'acknowledgement unavailable'):
                    self.campaign.settle(cleanup, reconcile_digest=digest)
            committed = ledger.report(run_id=RUN)['runs'][RUN]
            self.assertEqual(self.campaign.journal.data['settlement'], old)
            restarted = ci.Campaign(self.aws, self.cfg, RUN, self.output, clock=self.clock, cleanup_only=True)
            self.clock.sleep(3600)
            restarted.settle(cleanup, reconcile_digest=digest)
            self.assertEqual(ledger.report(run_id=RUN)['runs'][RUN], committed)
            self.assertEqual(restarted.journal.data['settlement']['receipt'], committed['receipt'])
            self.assertEqual(len(committed['usage_reconciliation_history']), 1)

    def test_reconciliation_cannot_release_unverified_cleanup_reservation(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            self.allocate()
            ec2 = self.aws.ec2['us-east-1']
            ec2.never_terminates.add(ec2.instances[0]['InstanceId'])
            cleanup = self.campaign.cleanup()
            self.assertFalse(cleanup['cleanup_complete'])
            self.campaign.settle(cleanup)
            report = ledger.report(run_id=RUN)
            run = report['runs'][RUN]
            self.assertFalse(run['reservation_released'])
            self.assertEqual(run['active_reservation_usd'], run['reserved_usd'])
            with self.assertRaisesRegex(ValueError, 'verified complete cleanup'):
                self.campaign.settle(cleanup, reconcile_digest=run['settlement_digest'])

    def test_actual_export_size_excess_is_recorded_with_live_price_assumptions(self):
        self.allocate()
        record = self.campaign.account_export([
            dict(Key=RUN + '/inputs/source.tar.gz', Size=1024**3),
            dict(Key=RUN + '/results/controller.tar.gz', Size=3 * 1024**3)])
        self.assertEqual(record['retained_bytes'], 4 * 1024**3)
        self.assertTrue(record['export_allowance_exceeded'])
        self.assertIn('not a final AWS bill', record['scope'])
        self.assertEqual(record['projections']['s3_retention_days'], 30)
        self.assertIsNone(record['request_estimate_usd'])
        self.assertIsNone(record['transfer_estimate_usd'])
        self.assertEqual(record['coverage'], 'partial')

    def test_journalled_ids_hidden_from_tag_listing_are_still_terminated(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        ec2.hide_tag_inventory = True
        receipt = self.campaign.cleanup()
        self.assertTrue(receipt['cleanup_complete'], receipt['errors'])
        self.assertEqual(len(ec2.terminations), 6)
        self.assertEqual({row['final_state'] for row in receipt['instances']}, {'terminated'})

    def test_paginated_owned_inventory_and_cleanup_keep_every_known_host(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        original = ec2.describe_instances
        calls = []
        def chunked(**params):
            calls.append(copy.deepcopy(params))
            response = original(**params)
            if params.get('InstanceIds'):
                return response
            hosts = response['Reservations'][0]['Instances']
            if params.get('NextToken') == 'second-half':
                return {'Reservations': [dict(OwnerId=ACCOUNT, Instances=hosts[3:])]}
            return {'Reservations': [dict(OwnerId=ACCOUNT, Instances=hosts[:3])], 'NextToken': 'second-half'}
        with patch.object(ec2, 'describe_instances', side_effect=chunked):
            self.assertEqual(len(self.campaign.inventory()), 6)
            receipt = self.campaign.cleanup()
        self.assertTrue(receipt['cleanup_complete'], receipt['errors'])
        self.assertEqual(len(receipt['instances']), 6)
        self.assertEqual(len(ec2.terminations), 6)
        self.assertTrue(any(call.get('NextToken') == 'second-half' for call in calls))

    def test_empty_first_profile_scan_page_does_not_hide_foreign_profile_user(self):
        self.allocate()
        foreign = copy.deepcopy(self.aws.ec2['us-east-1'].instances[0])
        foreign.update(InstanceId='i-fffffffffffffffff', ClientToken='foreign',
            IamInstanceProfile={'Arn': ci.planned_profiles(self.cfg, RUN)['storage-0']['arn']}, Tags=[])
        ec2 = self.aws.ec2['us-east-2']
        ec2.instances.append(foreign)
        original = ec2.describe_instances
        def chunked(**params):
            if params.get('Filters') or params.get('InstanceIds'):
                return original(**params)
            if params.get('NextToken') == 'foreign-profile-user':
                return original(**params)
            return {'Reservations': [], 'NextToken': 'foreign-profile-user'}
        with patch.object(ec2, 'describe_instances', side_effect=chunked):
            receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(self.aws.iam.deleted, [])
        self.assertIn('live instance still references', ' '.join(receipt['errors']))

    def test_missing_known_instance_without_prior_terminal_proof_is_incomplete(self):
        self.allocate()
        ec2 = self.aws.ec2['us-east-1']
        ec2.instances.pop()
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertIn('known allocation lacks observed terminal proof', ' '.join(receipt['errors']))
        self.assertEqual(len(ec2.terminations), 5)
        self.assertEqual(len(self.aws.iam.roles), 6)

    def test_completed_cleanup_retains_terminal_proofs_after_ec2_disappearance(self):
        self.allocate()
        first = self.campaign.cleanup()
        self.assertTrue(first['cleanup_complete'], first['errors'])
        self.aws.ec2['us-east-1'].instances = []
        second = self.campaign.cleanup()
        self.assertTrue(second['cleanup_complete'], second['errors'])
        self.assertEqual(second['instances'], first['instances'])
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)

    def test_repeated_successful_cleanup_reuses_immutable_financial_receipt(self):
        self.allocate()
        first = self.campaign.cleanup()
        self.assertTrue(first['cleanup_complete'], first['errors'])
        self.campaign.settle(first)
        original = copy.deepcopy(Ledger.settlements[RUN])
        self.clock.sleep(30)
        second = self.campaign.cleanup()
        self.campaign.journal.data['s3_accounting'] = dict(estimate_usd='999', observed_utc='a later inventory')
        self.campaign.settle(second)
        self.assertEqual(Ledger.settlements[RUN], original)
        self.assertEqual(self.campaign.journal.data['settlement'], original)

    def test_roles_are_six_distinct_boundary_locked_and_exactly_tagged(self):
        self.allocate()
        self.assertEqual(len(self.aws.iam.created), 6)
        self.assertEqual(set(self.aws.iam.created), {identity['name'] for identity in ci.planned_profiles(self.cfg, RUN).values()})
        for role in self.aws.iam.roles.values():
            self.assertEqual(role['PermissionsBoundary']['PermissionsBoundaryArn'], self.cfg['guest_boundary_arn'])
            self.assertEqual(ci.tag_map(role)['RunId'], RUN)
        receipt = self.campaign.cleanup()
        self.assertTrue(receipt['cleanup_complete'], receipt['errors'])
        self.assertEqual(self.aws.iam.roles, {})
        self.assertEqual(self.aws.iam.profiles, {})

    def test_second_iam_page_cannot_hide_an_unapproved_inline_policy(self):
        self.allocate()
        name = ci.planned_profiles(self.cfg, RUN)['controller']['name']
        calls = []
        def listed(**params):
            calls.append(copy.deepcopy(params))
            if params.get('Marker') == 'second-page':
                return {'PolicyNames': ['unapproved-policy'], 'IsTruncated': False}
            return {'PolicyNames': ['run-scope'], 'IsTruncated': True, 'Marker': 'second-page'}
        with patch.object(self.aws.iam, 'list_role_policies', side_effect=listed):
            with self.assertRaisesRegex(ValueError, 'unapproved additional policies'):
                self.campaign.checked_role(self.aws.iam.roles[name], name)
        self.assertEqual([call.get('Marker') for call in calls], [None, 'second-page'])
        self.assertEqual(self.aws.iam.deleted, [])

    def test_real_ledger_accepts_live_fractional_gp3_and_raw_hardware_proofs(self):
        for candidate in self.candidates:
            with localcontext() as context:
                context.prec = 60
                raw_rate = Decimal('1.786') + Decimal('0.005') + Decimal('0.08') * 32 / Decimal(730)
                candidate['host_hourly_usd'] = str(raw_rate)
                candidate['maximum_estimate_usd'] = str(raw_rate * 6)
            candidate['root_gib'] = 32
            candidate['price'] = dict(compute_usd_per_hour='1.786', ipv4_usd_per_hour='0.005',
                ebs_usd_per_gib_month='0.08', observed_utc=ci.utc())
            candidate['capability'] = dict(ProcessorInfo={'SustainedClockSpeedInGhz': 3.4},
                NetworkInfo={'NetworkCards': [{'BaselineBandwidthInGbps': 25.0, 'PeakBandwidthInGbps': 50.0}]})
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            self.allocate()
            reserved = ledger.report(run_id=RUN)['runs'][RUN]
            self.assertGreaterEqual(Decimal(reserved['reserved_usd']).as_tuple().exponent, -12)
            with localcontext() as context:
                context.prec = 60
                prices = self.preflight['ancillary_prices']
                assumptions = self.cfg['cost_assumptions']
                raw_overhead = (Decimal(assumptions['result_gib']) * Decimal(prices['storage_usd_per_gib_month'])
                    * assumptions['retention_days'] / Decimal(28)
                    + Decimal(assumptions['request_count']) * Decimal(prices['request_usd'])
                    + Decimal(assumptions['result_gib']) * 2 * Decimal(prices['transfer_usd_per_gib']))
                raw_forecast = raw_rate * 6 * (1 + Decimal(assumptions['reconciliation_seconds']) / Decimal(3600)) + raw_overhead
                self.assertGreaterEqual(Decimal(reserved['reserved_usd']), raw_forecast)
            original = self.campaign.journal.data['candidates'][0]
            self.assertIsInstance(original['capability']['ProcessorInfo']['SustainedClockSpeedInGhz'], float)
            projected = reserved['provenance']['candidates'][0]
            self.assertNotIn('capability', projected)
            self.assertEqual(projected['proof_sha256'], hashlib.sha256(ci.encoded(original)).hexdigest())
            launched = datetime.now(timezone.utc) - timedelta(seconds=300, microseconds=123456)
            for instance in self.aws.ec2['us-east-1'].instances:
                instance['LaunchTime'] = launched
            cleanup = self.campaign.cleanup()
            self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
            s3_cost = self.campaign.account_export([
                dict(Key=RUN + '/inputs/build-bundle.tar.gz', Size=1024**3 + 17, LastModified=launched),
                dict(Key=RUN + '/results/controller.tar.gz', Size=123456789, LastModified=launched)])
            self.assertGreaterEqual(Decimal(s3_cost['storage_estimate_usd']).as_tuple().exponent, -12)
            self.campaign.settle(cleanup)
            settled = ledger.report(run_id=RUN)['runs'][RUN]
            self.assertTrue(settled['cleanup_verified'])
            self.assertGreaterEqual(Decimal(settled['usage_estimated_usd']).as_tuple().exponent, -12)
            self.assertTrue(all(Decimal(row['host_hourly_usd']).as_tuple().exponent >= -12
                                for row in settled['receipt']['usage_evidence']['instances']))

    def test_upward_money_boundary_works_with_actual_ledger_at_low_decimal_precision(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            precise = Decimal('1.0000000000000000000000000001')
            with localcontext() as context:
                context.prec = 6
                amount = ci.usd_ceiling(precise)
            self.assertGreaterEqual(amount, precise)
            ledger.reserve(RUN, amount, {'quote_usd': str(precise)})
            ledger.settle(RUN, amount, dict(cleanup_complete=True,
                cleanup_evidence={'observed_terminated': ['i-00000000000000001']},
                usage_evidence={'elapsed_seconds': '60', 'rate_usd': str(amount)}))
            self.assertTrue(ledger.report(run_id=RUN)['runs'][RUN]['cleanup_verified'])

    def test_tracked_budget_edit_preserves_actual_admission_and_completed_settlement(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            original_sha = ci.sha(self.budget_file)
            self.allocate()
            cleanup = self.campaign.cleanup()
            self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
            self.campaign.settle(cleanup)
            original = ledger.report(run_id=RUN)['runs'][RUN]
            receipt = copy.deepcopy(self.campaign.journal.data['settlement'])
            ci.dump(self.budget_file, {'weekly_budget_usd': '75'})
            self.campaign.settle(self.campaign.cleanup())
            report = ledger.report(run_id=RUN)
            current = report['runs'][RUN]
            self.assertEqual(current['budget_policy']['sha256'], original_sha)
            self.assertEqual(current['usage_estimated_usd'], original['usage_estimated_usd'])
            self.assertEqual(current['receipt'], original['receipt'])
            self.assertEqual(self.campaign.journal.data['settlement'], receipt)
            self.assertEqual(report['budget_policy']['sha256'], ci.sha(self.budget_file))
            self.assertNotEqual(report['budget_policy']['sha256'], original_sha)
            self.assertEqual(report['budget_policy']['weekly_budget_usd'], '75')
            self.assertEqual(cleanup['budget_config_sha256'], original_sha)
            self.assertEqual(current['receipt']['budget_config_sha256'], original_sha)
            self.assertEqual(len(report['runs']), 1)

    def test_missing_current_budget_never_blocks_actual_ledger_cleanup_or_settlement(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            self.allocate()
            original_sha = ci.sha(self.budget_file)
            self.budget_file.unlink()
            cleanup = self.campaign.cleanup()
            self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
            self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)
            self.campaign.settle(cleanup)
            self.assertEqual(cleanup['budget_config_sha256'], original_sha)
            self.assertTrue((self.output / 'budget-report-error.json').is_file())
            ci.dump(self.budget_file, {'weekly_budget_usd': '60'})
            settled = ledger.report(run_id=RUN)['runs'][RUN]
            self.assertTrue(settled['cleanup_verified'])
            self.assertEqual(settled['budget_policy']['sha256'], original_sha)

    def test_budget_report_failure_does_not_prevent_launch_cleanup_or_settlement(self):
        with patch.object(Ledger, 'report', side_effect=RuntimeError('aggregate publication unavailable')):
            self.allocate()
            cleanup = self.campaign.cleanup()
            self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
            self.campaign.settle(cleanup)
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)
        self.assertTrue(Ledger.settlements[RUN]['receipt']['cleanup_complete'])
        error = ci.read_json(self.output / 'budget-report-error.json')
        self.assertIn('aggregate publication unavailable', error['error'])
        self.assertEqual(error['budget_config_sha256'], ci.sha(self.budget_file))

    def test_cleanup_without_admission_hashes_bad_budget_without_parsing_or_blocking(self):
        self.budget_file.write_text('not valid JSON')
        with patch.object(ci, 'load_budget', side_effect=AssertionError('no admission during cleanup')):
            receipt = self.campaign.cleanup()
        self.assertTrue(receipt['cleanup_complete'], receipt['errors'])
        self.assertEqual(receipt['budget_config_sha256'], ci.sha(self.budget_file))

    def test_weekly_budget_cli_override_has_been_removed(self):
        argv = ['ci_campaign.py', 'cleanup', '--output', str(self.output), '--weekly-budget-usd', '999']
        with patch.object(ci.sys, 'argv', argv), patch.object(ci, 'AWS') as aws:
            with self.assertRaises(SystemExit) as rejected:
                ci.main()
        self.assertEqual(rejected.exception.code, 2)
        aws.assert_not_called()

    def test_benchmark_config_cannot_shadow_tracked_weekly_budget(self):
        path = self.output / 'shadow-config.json'
        for field in ('weekly_budget_usd', 'weekly_limit_usd'):
            with self.subTest(field=field):
                ci.dump(path, {field: '999'})
                with self.assertRaisesRegex(ValueError, 'budget.json'):
                    ci.load_config(path, env={})

    def test_unknown_role_creation_is_reconciled_without_a_second_create(self):
        original = self.aws.iam.create_role
        first = True
        def unknown(**params):
            nonlocal first
            result = original(**params)
            if first:
                first = False
                raise TimeoutError('lost CreateRole acknowledgement')
            return result
        with patch.object(self.aws.iam, 'create_role', side_effect=unknown):
            self.allocate()
        self.assertEqual(len(self.aws.iam.created), 6)
        self.assertTrue(self.campaign.cleanup()['cleanup_complete'])

    def test_changed_boundary_is_preserved_not_adopted_or_deleted(self):
        instances, candidate, plan = self.allocate()
        name = ci.planned_profiles(self.cfg, RUN)['storage-0']['name']
        self.aws.iam.roles[name]['PermissionsBoundary']['PermissionsBoundaryArn'] = 'arn:aws:iam::123456789012:policy/foreign'
        with self.assertRaisesRegex(ValueError, 'immutable permissions boundary'):
            self.campaign.finalize_guest_policies(instances, candidate, plan['instance_profiles'])
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertIn(name, self.aws.iam.roles)
        self.assertEqual(len(self.aws.iam.deleted), 5)

    def test_foreign_host_using_run_profile_prevents_iam_cleanup(self):
        self.allocate()
        foreign = copy.deepcopy(self.aws.ec2['us-east-1'].instances[0])
        foreign.update(InstanceId='i-fffffffffffffffff', ClientToken='foreign',
            IamInstanceProfile={'Arn': ci.planned_profiles(self.cfg, RUN)['storage-0']['arn']})
        foreign['Tags'] = []
        self.aws.ec2['us-east-2'].instances.append(foreign)
        receipt = self.campaign.cleanup()
        self.assertFalse(receipt['cleanup_complete'])
        self.assertEqual(self.aws.iam.deleted, [])
        self.assertIn('live instance still references', ' '.join(receipt['errors']))

    def test_final_guest_policies_cannot_overwrite_input_or_other_partition(self):
        instances, candidate, plan = self.allocate()
        self.campaign.finalize_guest_policies(instances, candidate, plan['instance_profiles'])
        def allowed(policy, action, key):
            arn = f"arn:aws:s3:::{self.cfg['bucket']}/{key}"
            for statement in policy['Statement']:
                actions = statement['Action'] if isinstance(statement['Action'], list) else [statement['Action']]
                resources = statement['Resource'] if isinstance(statement['Resource'], list) else [statement['Resource']]
                if statement['Effect'] == 'Allow' and action in actions and any(fnmatch.fnmatchcase(arn, value) for value in resources):
                    return True
            return False
        names = ci.planned_profiles(self.cfg, RUN)
        storage = self.aws.iam.inline[names['storage-2']['name']]['run-scope']
        controller = self.aws.iam.inline[names['controller']['name']]['run-scope']
        self.assertTrue(allowed(storage, 's3:PutObject', RUN + '/results/cells/startup/storage/2/startup.json'))
        self.assertTrue(allowed(storage, 's3:PutObject', RUN + '/results/nodes/case/node-2.tar.gz.receipt.json'))
        for key in (RUN + '/inputs/controller-config.json', RUN + '/results/cells/r01-efa/storage/3/receipt.json',
                    'th-ffffffffffffffff/inputs/build-bundle.tar.gz'):
            self.assertFalse(allowed(storage, 's3:PutObject', key))
        self.assertFalse(allowed(controller, 's3:PutObject', RUN + '/inputs/controller-config.json'))
        self.assertFalse(allowed(controller, 's3:PutObject', RUN + '/inputs/build-bundle.tar.gz'))
        self.assertFalse(allowed(controller, 's3:PutObject', RUN + '/results/cells/r01-efa/storage/2/receipt.json'))
        self.assertTrue(allowed(controller, 's3:PutObject', RUN + '/inputs/kv-storage-node'))
        termination = next(row for row in controller['Statement'] if row['Action'] == 'ec2:TerminateInstances')
        self.assertEqual(set(termination['Resource']), {
            f"arn:aws:ec2:{candidate['region']}:{ACCOUNT}:instance/{item['InstanceId']}" for item in instances})

    def test_atomic_launch_has_no_bootstrap_profile_credentials(self):
        import control
        import sui_checkpoint
        self.allocate()
        self.cfg.update({key: value for key, value in sui_checkpoint.schema()['config'].items()
            if key in ('transports', 'repetitions', 'read_profile_limits', 'write_profile_limits',
                       'native_write_profile_limits', 'kv_suite')})
        inputs = {}
        for key in ('build_bundle', 'fixture_bundle', 'verification_bundle'):
            inputs[key] = dict(key=RUN + '/inputs/' + key, sha256='a' * 64)
        self.campaign.journal.data['uploads'] = inputs
        admission = dict(bindings=sui_checkpoint.schema()['config'],
                         inputs={'runner_bundle': dict(sha256='a' * 64, bytes=1)})
        candidate = self.candidates[0] | dict(ami='ami-approved', subnet='subnet-default',
                                            root_device='/dev/sda1', root_gib=32)
        with patch.object(control, 'user_data', return_value='#!/bin/sh\\nexit 0\\n'):
            request, plan = ci.Campaign.request(self.campaign, candidate, 'sg-owned', admission,
                                                ci.planned_profiles(self.cfg, RUN))
        self.assertNotIn('IamInstanceProfile', request)
        self.assertEqual(len({value['role_name'] for value in plan['instance_profiles'].values()}), 6)

    def recovery_cli(self, run_id, env=None, *, local=False, extra=()):
        environment = dict(GITHUB_ACTIONS='true', GITHUB_REPOSITORY=ci.REPOSITORY,
            GITHUB_REF='refs/heads/' + ci.BRANCH, GITHUB_EVENT_NAME='workflow_dispatch',
            GITHUB_RUN_ID='999999', GITHUB_RUN_ATTEMPT='1')
        if local:
            environment = {}
        if env:
            environment.update(env)
        argv = ['ci_campaign.py', 'cleanup', '--config', 'setup.json', '--output',
                str(self.output / 'recovery'), '--budget-file', str(self.budget_file), '--run-id', run_id]
        argv.extend(extra)
        with patch.dict(ci.os.environ, environment, clear=True), patch.object(ci.sys, 'argv', argv):
            with patch.object(ci, 'load_config', return_value=copy.deepcopy(self.cfg)), \
                    patch.object(ci, 'AWS', return_value=self.aws):
                return ci.main()

    def test_trusted_future_manual_cleanup_can_recover_old_journalled_run(self):
        self.allocate()
        self.assertEqual(self.recovery_cli(RUN), 0)
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])
        self.assertTrue(Ledger.settlements[RUN]['receipt']['cleanup_complete'])

    def test_explicit_old_run_recovery_requires_immutable_account_owned_journal(self):
        self.allocate()
        unknown = 'th-ffffffffffffffff'
        self.assertEqual(self.recovery_cli(unknown), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
        journal = copy.deepcopy(self.campaign.journal.data)
        journal['account'] = '999999999999'
        key = self.campaign.journal.key
        self.aws.s3.objects[key] = ci.encoded(journal)
        self.aws.s3.etags[key] = 'foreign-account-journal'
        self.assertEqual(self.recovery_cli(RUN), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])

    def test_old_run_override_does_not_bypass_trusted_branch_gate(self):
        self.allocate()
        self.assertEqual(self.recovery_cli(RUN, {'GITHUB_REF': 'refs/heads/main'}), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])

    def test_local_recovery_requires_explicit_expected_account_and_never_allocates(self):
        self.allocate()
        self.assertEqual(self.recovery_cli(RUN, local=True), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
        self.assertEqual(self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT)), 0)
        self.assertEqual(len(self.aws.ec2['us-east-1'].terminations), 6)
        self.assertEqual(len(self.aws.ec2['us-east-1'].requests), 1)
        self.assertEqual(self.aws.ec2['us-east-2'].requests, [])

    def test_local_recovery_rejects_wrong_caller_before_any_mutation(self):
        self.allocate()
        identity = SimpleNamespace(get_caller_identity=lambda: {'Account': '999999999999'})
        original = self.aws.client
        with patch.object(self.aws, 'client', side_effect=lambda service, region:
                          identity if service == 'sts' else original(service, region)):
            self.assertEqual(self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT)), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
        self.assertEqual(self.aws.iam.deleted, [])

    def test_local_recovery_rejects_invalid_identity_and_foreign_repository_or_branch_journal(self):
        self.allocate()
        self.assertEqual(self.recovery_cli('th-invalid', local=True, extra=('--expected-account', ACCOUNT)), 1)
        self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
        for field, value in (('Repository', 'foreign/repository'), ('BenchmarkBranch', 'main')):
            with self.subTest(field=field):
                journal = copy.deepcopy(self.campaign.journal.data)
                journal['ownership'][field] = value
                self.aws.s3.objects[self.campaign.journal.key] = ci.encoded(journal)
                self.assertEqual(self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT)), 1)
                self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
                self.assertEqual(self.aws.iam.deleted, [])

    def test_local_run_cannot_use_cleanup_authorization_to_allocate(self):
        argv = ['ci_campaign.py', 'run', '--config', 'setup.json', '--output', str(self.output),
                '--run-id', RUN, '--expected-account', ACCOUNT]
        with patch.dict(ci.os.environ, {}, clear=True), patch.object(ci.sys, 'argv', argv), \
                patch.object(ci, 'load_config', return_value=copy.deepcopy(self.cfg)), patch.object(ci, 'AWS') as aws:
            self.assertEqual(ci.main(), 1)
        aws.assert_not_called()

    def test_stale_reconciliation_digest_aborts_before_any_cleanup_mutation(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            cleanup = self.no_launch_cleanup()
            previous = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True,
                cleanup_evidence=cleanup, usage_evidence=dict(instances=[], elapsed_seconds='0')))
            ledger.settle(RUN, previous['usage_usd'], previous['receipt'])
            self.campaign.journal.data['settlement'] = previous
            self.campaign.journal.save()
            objects, deleted = copy.deepcopy(self.aws.s3.objects), list(self.aws.iam.deleted)
            result = self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT,
                '--reconcile-usage', '--previous-settlement-digest', 'f' * 64))
            self.assertEqual(result, 1)
            self.assertEqual(self.aws.s3.objects, objects)
            self.assertEqual(self.aws.iam.deleted, deleted)
            self.assertTrue(all(not ec2.terminations for ec2 in self.aws.ec2.values()))
            self.assertTrue(all(not ec2.requests for ec2 in self.aws.ec2.values()))

    def test_reconciliation_cannot_shutdown_an_active_unsettled_campaign(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            self.allocate()
            objects = copy.deepcopy(self.aws.s3.objects)
            result = self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT,
                '--reconcile-usage', '--previous-settlement-digest', 'a' * 64))
            self.assertEqual(result, 1)
            self.assertEqual(self.aws.s3.objects, objects)
            self.assertEqual(self.aws.iam.deleted, [])
            self.assertEqual(self.aws.ec2['us-east-1'].terminations, [])
            self.assertTrue(all(item['State']['Name'] == 'running' for item in self.aws.ec2['us-east-1'].instances))

    def test_reconciliation_rejects_live_instances_even_with_completed_settlement(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            self.allocate()
            cleanup = self.campaign.cleanup()
            self.assertTrue(cleanup['cleanup_complete'], cleanup['errors'])
            previous = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True,
                cleanup_evidence=cleanup, usage_evidence=dict(instances=[], elapsed_seconds='0')))
            ledger.settle(RUN, previous['usage_usd'], previous['receipt'])
            self.campaign.journal.data['settlement'] = previous
            self.campaign.journal.save()
            digest = ledger.report(run_id=RUN)['runs'][RUN]['settlement_digest']
            # A later live observation must override the older closed-run assumption.
            self.aws.ec2['us-east-1'].instances[0]['State']['Name'] = 'running'
            objects = copy.deepcopy(self.aws.s3.objects)
            terminated, deleted = list(self.aws.ec2['us-east-1'].terminations), list(self.aws.iam.deleted)
            result = self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT,
                '--reconcile-usage', '--previous-settlement-digest', digest))
            self.assertEqual(result, 1)
            self.assertEqual(self.aws.s3.objects, objects)
            self.assertEqual(self.aws.ec2['us-east-1'].terminations, terminated)
            self.assertEqual(self.aws.iam.deleted, deleted)
            self.assertEqual(self.aws.ec2['us-east-1'].instances[0]['State']['Name'], 'running')

    def test_local_closed_run_reconciliation_cli_uses_authoritative_digest(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            cleanup = self.no_launch_cleanup()
            previous = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True,
                cleanup_evidence=cleanup, usage_evidence=dict(instances=[], elapsed_seconds='0')))
            ledger.settle(RUN, previous['usage_usd'], previous['receipt'])
            self.campaign.journal.data['settlement'] = previous
            self.campaign.journal.save()
            original = ledger.report(run_id=RUN)['runs'][RUN]
            result = self.recovery_cli(RUN, local=True, extra=('--expected-account', ACCOUNT,
                '--reconcile-usage', '--previous-settlement-digest', original['settlement_digest']))
            self.assertEqual(result, 0)
            report = ledger.report(run_id=RUN)
            corrected = report['runs'][RUN]
            self.assertEqual(corrected['usage_accounting_basis'], 'observed-resources-v2')
            self.assertEqual(corrected['usage_reconciliation_history'][0]['receipt'], previous['receipt'])
            self.assertEqual(corrected['reserved_usd'], original['reserved_usd'])
            self.assertEqual(report['weekly_budget_usd'], '60')
            self.assertTrue(all(not ec2.requests for ec2 in self.aws.ec2.values()))

    def test_main_cli_replays_committed_unknown_acknowledgement_and_completed_retry(self):
        with patch('ci_budget.BudgetLedger', ActualBudgetLedger):
            ledger = ActualBudgetLedger(self.aws.s3, self.cfg['bucket'], ACCOUNT, budget_path=self.budget_file)
            ledger.initialize()
            cleanup = self.no_launch_cleanup()
            previous = dict(usage_usd='0.665648825373', receipt=dict(cleanup_complete=True,
                cleanup_evidence=cleanup, usage_evidence=dict(instances=[], elapsed_seconds='0')))
            ledger.settle(RUN, previous['usage_usd'], previous['receipt'])
            self.campaign.journal.data['settlement'] = previous
            self.campaign.journal.save()
            digest = ledger.report(run_id=RUN)['runs'][RUN]['settlement_digest']
            key = RUN + '/inputs/retained-fixture.tar.gz'
            self.aws.s3.put_object(Bucket=self.cfg['bucket'], Key=key, Body=b'actual-retained-input',
                ExpectedBucketOwner=ACCOUNT, ServerSideEncryption='AES256')
            extra = ('--expected-account', ACCOUNT, '--reconcile-usage', '--previous-settlement-digest', digest)
            original_reconcile = ActualBudgetLedger.reconcile_usage
            def committed_but_unknown(owner, *args, **kwargs):
                original_reconcile(owner, *args, **kwargs)
                raise OSError('committed reconciliation acknowledgement unavailable')
            with patch.object(self.aws.s3, 'download_file', wraps=self.aws.s3.download_file) as download, \
                    patch.object(self.aws.s3, 'list_objects_v2', wraps=self.aws.s3.list_objects_v2) as listing:
                with patch.object(ActualBudgetLedger, 'reconcile_usage', new=committed_but_unknown):
                    self.assertEqual(self.recovery_cli(RUN, local=True, extra=extra), 1)
                committed = ledger.report(run_id=RUN)['runs'][RUN]
                pending_journal = ci.Journal(self.aws.s3, self.cfg, RUN, self.output).data
                self.assertEqual(pending_journal['settlement'], previous)
                replacement = pending_journal['usage_reconciliation']['replacement']
                self.assertEqual(replacement['receipt'], committed['receipt'])
                self.assertEqual(len(committed['usage_reconciliation_history']), 1)
                self.assertEqual(download.call_count, 1)
                first_downloads, first_listings = download.call_count, listing.call_count
                manifest = self.output / 'recovery' / 'retrieval-manifest.json'
                manifest_bytes = manifest.read_bytes()
                deleted = list(self.aws.iam.deleted)
                terminations = {region: list(ec2.terminations) for region, ec2 in self.aws.ec2.items()}
                for attempt in range(2):
                    with self.subTest(retry=attempt):
                        self.assertEqual(self.recovery_cli(RUN, local=True, extra=extra), 0)
                        self.assertEqual(ledger.report(run_id=RUN)['runs'][RUN], committed)
                        current_journal = ci.Journal(self.aws.s3, self.cfg, RUN, self.output).data
                        self.assertEqual(current_journal['settlement'], replacement)
                        self.assertEqual(current_journal['usage_reconciliation'], pending_journal['usage_reconciliation'])
                        self.assertEqual(manifest.read_bytes(), manifest_bytes)
                        self.assertEqual(download.call_count, first_downloads)
                        self.assertEqual(listing.call_count, first_listings)
                        self.assertEqual(self.aws.iam.deleted, deleted)
                        self.assertEqual({region: list(ec2.terminations) for region, ec2 in self.aws.ec2.items()}, terminations)
                # A different stale digest still cannot use the exact-retry path.
                extra = ('--expected-account', ACCOUNT, '--reconcile-usage', '--previous-settlement-digest', 'f' * 64)
                self.assertEqual(self.recovery_cli(RUN, local=True, extra=extra), 1)
                self.assertEqual(download.call_count, first_downloads)
                self.assertEqual(listing.call_count, first_listings)
                self.assertEqual(ledger.report(run_id=RUN)['runs'][RUN], committed)

    def test_reconciliation_cli_requires_explicit_cleanup_run_and_digest_pair(self):
        for extra in (('--reconcile-usage',), ('--previous-settlement-digest', 'a' * 64),
                      ('--reconcile-usage', '--previous-settlement-digest', 'invalid')):
            with self.subTest(extra=extra), patch.object(ci, 'AWS') as aws:
                self.assertEqual(self.recovery_cli(RUN, local=True,
                    extra=('--expected-account', ACCOUNT, *extra)), 1)
                aws.assert_not_called()



class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.aws = AWS(self.clock)
        self.cfg = dict(regions=['us-east-1', 'us-east-2'], instance_types=['g6.8xlarge', 'g6e.8xlarge'],
                        bucket_region='us-east-1', account=ACCOUNT)
        self.hardware = [dict(InstanceType=kind, ProcessorInfo={'SupportedArchitectures': ['x86_64']},
            NetworkInfo={'EfaSupported': True}, InstanceStorageSupported=True,
            InstanceStorageInfo={'Disks': [dict(Count=2, Type='ssd')]},
            PlacementGroupInfo={'SupportedStrategies': ['cluster']}) for kind in self.cfg['instance_types']]
        for region, ec2 in self.aws.ec2.items():
            ec2.describe_images = lambda **kwargs: {'Images': [dict(ImageId='ami-reviewed', OwnerId='099720109477',
                State='available', Architecture='x86_64', VirtualizationType='hvm', RootDeviceType='ebs',
                EnaSupport=True, RootDeviceName='/dev/sda1', BlockDeviceMappings=[])]}
            ec2.describe_subnets = lambda _region=region, **kwargs: {'Subnets': [dict(SubnetId='subnet-default',
                VpcId='vpc-default', AvailabilityZone=_region+'a', MapPublicIpOnLaunch=True, AvailableIpAddressCount=12)]}
            ec2.describe_route_tables = lambda **kwargs: {'RouteTables': [{'Routes': [dict(
                DestinationCidrBlock='0.0.0.0/0', GatewayId='igw-public', State='active')]}]}
            ec2.describe_instance_types = lambda **kwargs: {'InstanceTypes': [
                copy.deepcopy(item) for item in self.hardware if item['InstanceType'] in kwargs['InstanceTypes']]}
            ec2.describe_instance_type_offerings = lambda _region=region, **kwargs: {
                'InstanceTypeOfferings': [{'Location': _region+'a'}]}
        original_client = self.aws.client
        self.aws.client = lambda service, region: (
            SimpleNamespace(get_parameter=lambda **kwargs: {'Parameter': {'Value': 'ami-reviewed', 'Version': 1}})
            if service == 'ssm' else object() if service == 'pricing' else original_client(service, region))

    def prices(self, pricing, region, kind):
        return dict(compute_usd_per_hour='1' if region == 'us-east-2' and kind == 'g6e.8xlarge' else '2',
                    ebs_usd_per_gib_month='0.08', ipv4_usd_per_hour='0.005')

    def test_live_prices_select_in_use_ipv4_without_product_family(self):
        def product(sku, service, attributes, unit, amount, family=None):
            value = dict(sku=sku, attributes=dict(regionCode='us-east-1', **attributes))
            if family is not None:
                value['productFamily'] = family
            return dict(serviceCode=service, product=value, terms={'OnDemand': {'term': {
                'priceDimensions': {'dimension': dict(unit=unit, beginRange='0',
                    endRange='Inf', pricePerUnit={'USD': amount})}}}})
        catalog = [
            product('compute', 'AmazonEC2', dict(instanceType='g6.8xlarge',
                operatingSystem='Linux', tenancy='Shared', preInstalledSw='NA',
                capacitystatus='Used'), 'Hrs', '2.0144', 'Compute Instance'),
            product('gp3', 'AmazonEC2', dict(volumeApiName='gp3'), 'GB-Mo', '0.08', 'Storage'),
            product('4GQUNXTFWVSGPUZK', 'AmazonVPC', dict(group='VPCPublicIPv4Address',
                usagetype='USE1-PublicIPv4:InUseAddress'), 'Hrs', '0.005'),
            product('idle', 'AmazonVPC', dict(group='VPCPublicIPv4Address',
                usagetype='USE1-PublicIPv4:IdleAddress'), 'Hrs', '0.019')]
        def get_products(**request):
            def matches(item):
                value = item['product']
                return item['serviceCode'] == request['ServiceCode'] and all(
                    value.get(row['Field'], value['attributes'].get(row['Field'])) == row['Value']
                    for row in request['Filters'])
            return {'PriceList': [json.dumps(item) for item in catalog if matches(item)]}
        pricing = SimpleNamespace(get_products=get_products)
        quoted = ci.live_prices(pricing, 'us-east-1', 'g6.8xlarge')
        self.assertEqual(quoted['compute_usd_per_hour'], '2.0144')
        self.assertEqual(quoted['ebs_usd_per_gib_month'], '0.08')
        self.assertEqual(quoted['ipv4_usd_per_hour'], '0.005')
        self.assertEqual([item['sku'] for item in quoted['evidence']['ipv4']], ['4GQUNXTFWVSGPUZK'])
        catalog[:] = [item for item in catalog if item['product']['sku'] != '4GQUNXTFWVSGPUZK']
        with self.assertRaises(ValueError):
            ci.live_prices(pricing, 'us-east-1', 'g6.8xlarge')

    def test_live_priced_cheapest_compatible_region_wins_not_config_order(self):
        with patch.object(ci, 'live_prices', side_effect=self.prices), \
                patch.object(ci, 'live_ancillary_prices', return_value={'reviewed': True}):
            discovered = ci.discover_candidates(self.aws, self.cfg)
        first = discovered['candidates'][0]
        self.assertEqual((first['region'], first['instance_type']), ('us-east-2', 'g6e.8xlarge'))
        self.assertEqual(first['root_gib'], 32)
        self.assertEqual({item['region'] for item in discovered['candidates']}, set(self.cfg['regions']))

    def discover(self):
        with patch.object(ci, 'live_prices', side_effect=self.prices), \
                patch.object(ci, 'live_ancillary_prices', return_value={'reviewed': True}):
            return ci.discover_candidates(self.aws, self.cfg)

    def test_singleton_type_unavailability_preserves_other_types_and_regions(self):
        ec2 = self.aws.ec2['us-east-1']
        original = ec2.describe_instance_types
        for code in ('InvalidInstanceType', 'InvalidParameterValue'):
            calls = []
            def describe(**kwargs):
                calls.append(kwargs['InstanceTypes'])
                if kwargs['InstanceTypes'] == ['g6e.8xlarge']:
                    error = ServiceError(code)
                    error.response['Error']['Message'] = (
                        'The following supplied instance types do not exist: [g6e.8xlarge]')
                    raise error
                return original(**kwargs)
            ec2.describe_instance_types = describe
            with self.subTest(code=code):
                discovered = self.discover()
                self.assertEqual(calls, [['g6.8xlarge'], ['g6e.8xlarge']])
                self.assertEqual(len(discovered['candidates']), 3)
                self.assertEqual((discovered['candidates'][0]['region'],
                    discovered['candidates'][0]['instance_type']), ('us-east-2', 'g6e.8xlarge'))
                self.assertEqual(discovered['rejected'], [dict(region='us-east-1',
                    instance_type='g6e.8xlarge', reason='describe_instance_types: ' + code)])

    def test_missing_public_image_parameter_rejects_only_that_region(self):
        original = self.aws.client
        calls = []
        def parameter(**kwargs):
            calls.append(kwargs)
            raise ServiceError('ParameterNotFound')
        self.aws.client = lambda service, region: (
            SimpleNamespace(get_parameter=parameter)
            if service == 'ssm' and region == 'us-east-1' else original(service, region))
        discovered = self.discover()
        self.assertEqual(len(calls), 1)
        self.assertEqual({item['region'] for item in discovered['candidates']}, {'us-east-2'})
        self.assertEqual({item['instance_type'] for item in discovered['rejected']}, set(self.cfg['instance_types']))
        self.assertTrue(all(item['region'] == 'us-east-1' and 'ParameterNotFound' in item['reason']
                            for item in discovered['rejected']))

    def test_deregistered_image_rejects_region_but_not_other_regions(self):
        with patch.object(self.aws.ec2['us-east-1'], 'describe_images',
                          side_effect=ServiceError('InvalidAMIID.Unavailable')):
            discovered = self.discover()
        self.assertEqual({item['region'] for item in discovered['candidates']}, {'us-east-2'})
        self.assertEqual(len(discovered['rejected']), 2)
        self.assertTrue(all('InvalidAMIID.Unavailable' in item['reason'] for item in discovered['rejected']))

    def test_discovery_does_not_hide_auth_permission_or_unknown_errors(self):
        original = self.aws.client
        for service, method in (('ssm', 'get_parameter'), ('ec2', 'describe_images'),
                                ('ec2', 'describe_instance_types'), ('ec2', 'describe_instance_type_offerings')):
            for code in ('AccessDeniedException', 'UnauthorizedOperation', 'AuthFailure',
                         'OptInRequired', 'InvalidParameterValue', 'UnsupportedOperation', 'InternalError',
                         'InvalidAMIID.NotFound'):
                error = ServiceError(code)
                with self.subTest(service=service, method=method, code=code):
                    def fail(**kwargs):
                        raise error
                    def client(name, region):
                        if name == 'ssm' and region == 'us-east-1':
                            return SimpleNamespace(get_parameter=fail)
                        return original(name, region)
                    if service == 'ssm':
                        with patch.object(self.aws, 'client', side_effect=client):
                            with self.assertRaises(ServiceError) as raised:
                                self.discover()
                    else:
                        with patch.object(self.aws.ec2['us-east-1'], method, side_effect=error):
                            with self.assertRaises(ServiceError) as raised:
                                self.discover()
                    self.assertIs(raised.exception, error)
        with patch.object(self.aws.ec2['us-east-1'], 'describe_instance_types',
                          side_effect=TimeoutError('unknown outcome')):
            with self.assertRaises(TimeoutError):
                self.discover()

    def test_malformed_type_results_and_invalid_pricing_fail_closed(self):
        for result in ([], self.hardware, [self.hardware[1]], None, [{}]):
            with self.subTest(result=result), patch.object(
                    self.aws.ec2['us-east-1'], 'describe_instance_types', return_value={'InstanceTypes': result}):
                with self.assertRaisesRegex(ValueError, 'exact regional instance type'):
                    self.discover()
        with patch.object(ci, 'live_prices', side_effect=ValueError('no positive price')):
            with self.assertRaisesRegex(ValueError, 'no positive price'):
                ci.discover_candidates(self.aws, self.cfg)

    def test_absent_offerings_and_subnets_are_reported_without_candidates(self):
        self.aws.ec2['us-east-1'].describe_subnets = lambda **kwargs: {'Subnets': []}
        self.aws.ec2['us-east-2'].describe_instance_type_offerings = lambda **kwargs: {'InstanceTypeOfferings': []}
        with self.assertRaisesRegex(ValueError, 'no live compatible') as raised:
            self.discover()
        for region in self.cfg['regions']:
            self.assertIn(region, str(raised.exception))
        for kind in self.cfg['instance_types']:
            self.assertIn(kind, str(raised.exception))
        self.assertIn('no usable public subnet', str(raised.exception))
        self.assertIn('no instance type offering', str(raised.exception))
        self.assertTrue(all(not ec2.requests for ec2 in self.aws.ec2.values()))

    def test_all_types_unsupported_reports_each_rejection_without_allocation(self):
        for ec2 in self.aws.ec2.values():
            ec2.describe_instance_types = lambda **kwargs: (_ for _ in ()).throw(ServiceError('InvalidInstanceType'))
        with self.assertRaisesRegex(ValueError, 'no live compatible') as raised:
            self.discover()
        self.assertEqual(str(raised.exception).count('describe_instance_types: InvalidInstanceType'), 4)
        self.assertTrue(all(not ec2.requests for ec2 in self.aws.ec2.values()))

    def test_four_disk_hardware_requires_its_exact_bootstrap_inventory(self):
        self.cfg['instance_types'] = ['c6id.32xlarge']
        self.hardware[:] = [self.hardware[0]]
        self.hardware[0]['InstanceType'] = 'c6id.32xlarge'
        self.hardware[0]['InstanceStorageInfo']['Disks'][0]['Count'] = 4
        discovered = self.discover()
        self.assertEqual({(item['region'], item['instance_type']) for item in discovered['candidates']},
                         {(region, 'c6id.32xlarge') for region in self.cfg['regions']})
        self.hardware[0]['InstanceStorageInfo']['Disks'][0]['Count'] = 2
        with patch.object(ci, 'live_prices', side_effect=AssertionError('wrong inventory cannot reach paid admission')):
            with self.assertRaisesRegex(ValueError, 'no live compatible'):
                ci.discover_candidates(self.aws, self.cfg)

    def test_missing_cluster_or_efa_capability_is_not_silently_accepted(self):
        for item in self.hardware:
            item['PlacementGroupInfo']['SupportedStrategies'] = ['spread']
        with patch.object(ci, 'live_prices', side_effect=AssertionError('incompatible type must not be priced')):
            with self.assertRaisesRegex(ValueError, 'no live compatible'):
                ci.discover_candidates(self.aws, self.cfg)

    def test_pricing_uses_highest_positive_tier_and_rejects_missing_prices(self):
        product = {'product': {'sku': 'owned-price'}, 'terms': {'OnDemand': {'term': {
            'priceDimensions': {'free': dict(unit='GB', pricePerUnit={'USD': '0'}),
                'paid': dict(unit='GB', pricePerUnit={'USD': '0.09'}),
                'higher': dict(unit='GB', pricePerUnit={'USD': '0.12'})}}}}}
        self.assertEqual(ci.maximum_rate([product], 'GB')[0], '0.12')
        with self.assertRaisesRegex(ValueError, 'no positive'):
            ci.maximum_rate([], 'GB')


class BranchAdmissionTests(unittest.TestCase):
    def environment(self):
        return dict(GITHUB_ACTIONS='true', GITHUB_REPOSITORY=ci.REPOSITORY,
                    GITHUB_REF='refs/heads/' + ci.BRANCH, GITHUB_EVENT_NAME='push',
                    GITHUB_RUN_ID='123', GITHUB_RUN_ATTEMPT='1')

    def test_stable_attempt_identity_and_new_attempt_separation(self):
        env = self.environment()
        self.assertEqual(ci.trusted_run(env), ci.trusted_run(dict(env)))
        self.assertNotEqual(ci.trusted_run(env), ci.trusted_run(env | {'GITHUB_RUN_ATTEMPT': '2'}))
        self.assertEqual(ci.trusted_run(env), ci.trusted_run(env | {'GITHUB_EVENT_NAME': 'workflow_dispatch'}))

    def test_only_exact_repository_branch_push_can_use_live_path(self):
        for changed in ({'GITHUB_ACTIONS': ''}, {'GITHUB_REPOSITORY': 'fork/tidehunter-distributed'},
                        {'GITHUB_REF': 'refs/heads/main'}, {'GITHUB_REF': 'refs/pull/1/merge'},
                        {'GITHUB_EVENT_NAME': 'pull_request'}, {'GITHUB_EVENT_NAME': 'pull_request_target'},
                        {'GITHUB_RUN_ID': ''}, {'GITHUB_RUN_ATTEMPT': '0'}):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, 'trusted repository push'):
                ci.trusted_run(self.environment() | changed)


if __name__ == '__main__':
    unittest.main()
