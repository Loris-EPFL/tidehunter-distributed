#!/usr/bin/env python3
"""Render reviewed KV campaign payloads locally; never contact AWS or launch hosts.

Input JSON contains bootstrap, candidate, security_group_id, package_dir,
diagnostic_proof_dir and diagnostic_ready from ci_prepare.run_tcp_diagnostic.
For a ready activation it also contains controller_config and the six EC2
instance records. AWS calls and durable journal operations remain operator
responsibilities.
"""
import argparse
import hashlib
import json
from pathlib import Path

import ci_campaign
import ci_prepare
import cloud_bootstrap
import cloud_controller
import cloud_package


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def render(data):
    boot = cloud_bootstrap.validate_plan(data['bootstrap'])
    package = Path(data['package_dir']).resolve(strict=True)
    verified = cloud_package.verify_directory(package)
    archive = package / 'bundle.tar.gz'
    archived = cloud_package.verify_archive(archive, boot['bundle_sha256'])
    if (archived != verified or archive.stat().st_size != boot['bundle_bytes']
            or verified['source_sha256'] != boot['source_sha256']
            or verified['binary_sha256'] != boot['binary_sha256']):
        raise ValueError('bundle, staged manifest and bootstrap source/binary bindings differ')
    manifest = json.loads((package / 'manifest.json').read_text())
    diagnostic_ready = data['diagnostic_ready']
    if (diagnostic_ready.get('engine_sha') != manifest['revision']
            or diagnostic_ready.get('binary_sha256') != verified['binary_sha256']
            or diagnostic_ready.get('binary_blake2b512') != verified['binary_blake2b512']):
        raise ValueError('local diagnostic source/binary differs from the frozen package')
    ci_prepare.admit_tcp_diagnostic(data['diagnostic_proof_dir'], diagnostic_ready)
    candidate = data['candidate']
    run = boot['run_id']
    if (candidate['region'] != boot['region'] or candidate['instance_type'] != boot['instance_type']
            or candidate['az'][:-1] != boot['region'] or not candidate['subnet']
            or not candidate['ami'] or not candidate['root_device'] or not data['security_group_id']):
        raise ValueError('candidate must match the immutable bootstrap plan')
    if candidate['root_gib'] != 32:
        raise ValueError('expected reviewed 32 GiB encrypted root volume')
    tags = ci_campaign.core.ownership(run) | cloud_controller.TAGS | {
        'ExpiresAt': boot['deadline_utc'], 'Role': 'bootstrap', 'Name': run}
    request = {
        'ImageId': candidate['ami'], 'InstanceType': candidate['instance_type'],
        'MinCount': 6, 'MaxCount': 6,
        'ClientToken': ci_campaign.core.candidate_token(run, candidate),
        'NetworkInterfaces': [{'DeviceIndex': 0, 'NetworkCardIndex': 0, 'InterfaceType': 'efa',
                               'SubnetId': candidate['subnet'], 'Groups': [data['security_group_id']],
                               'DeleteOnTermination': True, 'AssociatePublicIpAddress': True}],
        'Placement': {'AvailabilityZone': candidate['az'], 'GroupName': run},
        'BlockDeviceMappings': [{'DeviceName': candidate['root_device'], 'Ebs': {
            'VolumeSize': candidate['root_gib'], 'VolumeType': 'gp3',
            'Encrypted': True, 'DeleteOnTermination': True}}],
        'MetadataOptions': {'HttpEndpoint': 'enabled', 'HttpTokens': 'required',
                            'HttpPutResponseHopLimit': 1, 'InstanceMetadataTags': 'enabled'},
        'InstanceInitiatedShutdownBehavior': 'terminate',
        'TagSpecifications': [{'ResourceType': kind, 'Tags': ci_campaign.core.aws_tags(tags)}
                              for kind in ('instance', 'volume', 'network-interface')],
        'UserData': cloud_bootstrap.render_userdata(boot),
    }
    pending = cloud_bootstrap.activation_record(boot)
    pending_bytes = encoded(pending)
    cfg = {'account': boot['account'], 'bucket': boot['bucket']}
    profiles = ci_campaign.core.planned_profiles(cfg, run)
    policies = {role: ci_campaign.guest_policy(cfg, run, role, region=boot['region'],
                                               expires_at=boot['deadline_utc'])
                for role in sorted(profiles)}
    result = {'request': request, 'profiles': profiles, 'initial_guest_policies': policies,
              'package_verification': verified, 'source_revision': manifest['revision'],
              'source_dirty': manifest['dirty'],
              'tcp_diagnostic_sha256': diagnostic_ready['tcp_diagnostic_sha256'],
              'pending': pending, 'pending_sha256': hashlib.sha256(pending_bytes).hexdigest()}
    if 'controller_config' in data or 'instances' in data:
        config, instances = data['controller_config'], data['instances']
        if (config['source_revision'] != manifest['revision']
                or config['binary_blake2b512'] != verified['binary_blake2b512']):
            raise ValueError('controller source revision or binary digest differs from verified package')
        if len(instances) != 6 or {item['AmiLaunchIndex'] for item in instances} != set(range(6)):
            raise ValueError('exact six distinct launch indices required')
        if len({item['InstanceId'] for item in instances}) != 6:
            raise ValueError('duplicate EC2 instance ID')
        by_index = {item['AmiLaunchIndex']: item for item in instances}
        for index, host in [(5, config['fleet']['coordinator']), *enumerate(config['fleet']['nodes'])]:
            instance = by_index[index]
            if (host['instance_id'] != instance['InstanceId']
                    or host['address'] != instance['PrivateIpAddress']):
                raise ValueError('activation fleet roles differ from EC2 launch indices')
        cloud_controller.validate_config(config)
        cloud_controller.verify_instances(config, {'Reservations': [{'OwnerId': boot['account'],
                                                                       'Instances': instances}]}, starting=True)
        ready = cloud_bootstrap.activation_record(boot, config)
        result['ready'] = ready
        result['ready_sha256'] = hashlib.sha256(encoded(ready)).hexdigest()
        result['final_guest_policies'] = {
            role: ci_campaign.guest_policy(cfg, run, role, region=boot['region'],
                                           expires_at=boot['deadline_utc'], instances=instances)
            for role in sorted(profiles)}
        result['profile_assignments'] = {
            item['InstanceId']: profiles['controller' if item['AmiLaunchIndex'] == 5
                                         else f"storage-{item['AmiLaunchIndex']}"]['name']
            for item in instances}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path, help='new file for locally rendered JSON')
    args = parser.parse_args()
    payload = render(json.loads(args.input.read_text()))
    with args.output.open('x') as stream:
        json.dump(payload, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')


if __name__ == '__main__':
    main()
