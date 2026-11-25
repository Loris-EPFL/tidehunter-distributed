#!/usr/bin/env python3
"""Build a reviewable image receipt from two uploaded, SHA-pinned diagnostics.

Upload the two actual guest diagnostic files to GCS first. Supply their exact
object generations and SHA-256 values in the input JSON. This tool writes only a
local receipt; uploading it and pinning it in reviewed configuration is separate.
``--config`` writes the pinned entries after the receipt has been uploaded.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

import gcp_image


def make_receipt(spec, *, qualification_profile='full'):
    if qualification_profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if not isinstance(spec, dict) or set(spec) != {'image', 'runtime_sha256', 'service_account', 'evidence'}:
        raise ValueError('spec requires image, runtime_sha256, service_account and evidence')
    image = spec['image']
    if not isinstance(image, dict) or set(image) != {'path', 'id'} or not isinstance(image['id'], str) or not image['id'].isdigit():
        raise ValueError('captured custom image path and numeric ID required')
    if not isinstance(image['path'], str) or not gcp_image._IMAGE.fullmatch(image['path']):
        raise ValueError('captured custom image path required')
    entries = spec['evidence']
    if not isinstance(entries, list) or len(entries) != 2:
        raise ValueError('two independent evidence files required')
    records, refs = [], []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {'file', 'reference'}:
            raise ValueError('each evidence entry requires file and reference')
        ref = entry['reference']
        if not isinstance(ref, dict) or set(ref) != {'bucket', 'object', 'generation', 'sha256'}:
            raise ValueError('invalid evidence reference')
        if not isinstance(ref['bucket'], str) or not ref['bucket'] or not isinstance(ref['object'], str) or not ref['object'] or not isinstance(ref['generation'], str) or not ref['generation'].isdigit() or int(ref['generation']) <= 0:
            raise ValueError('invalid generation-pinned evidence reference')
        data = Path(entry['file']).read_bytes()
        if len(data) > 16 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != ref['sha256']:
            raise ValueError('evidence bytes do not match uploaded object SHA-256')
        records.append(json.loads(data))
        refs.append(ref)
    if refs[0]['bucket'] != refs[1]['bucket'] or refs[0] == refs[1]:
        raise ValueError('distinct evidence objects in one bucket required')
    gcp_image.validate_pair(records, image, spec['runtime_sha256'], spec['service_account'],
                            qualification_profile=qualification_profile)
    receipt = {'schema_version': 1, 'image': image, 'recipes': gcp_image.recipe_hashes(),
               'runtime_sha256': spec['runtime_sha256'], 'evidence': refs}
    if qualification_profile == 'focused':
        receipt.update(qualification_profile='focused', qualification_proofs=list(gcp_image.FOCUSED_PROOFS))
    return receipt


def apply_config(config_path, receipt, *, bucket, object_name, generation, sha256):
    """Write the three config entries that consume one uploaded receipt.

    The receipt must already carry the current recipe hashes. The reference is
    bound to the exact receipt bytes on disk, so a later edit is caught.
    """
    if not isinstance(receipt, dict) or receipt.get('schema_version') != 1:
        raise ValueError('image receipt schema version 1 required')
    image = receipt.get('image')
    if (not isinstance(image, dict) or not gcp_image._IMAGE.fullmatch(str(image.get('path', '')))
            or not str(image.get('id', '')).isdigit()):
        raise ValueError('receipt does not bind an exact custom image')
    if receipt.get('recipes') != gcp_image.recipe_hashes():
        raise ValueError('receipt recipe hashes do not match the reviewed code')
    if not isinstance(bucket, str) or not bucket or '/' in bucket:
        raise ValueError('invalid receipt bucket')
    if not isinstance(object_name, str) or not object_name or object_name.startswith('/'):
        raise ValueError('invalid receipt object name')
    if not re.fullmatch(r'[1-9][0-9]*', str(generation)):
        raise ValueError('receipt generation must be a positive decimal string')
    if not isinstance(sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', sha256):
        raise ValueError('receipt SHA-256 must be a full lowercase digest')
    profile = receipt.get('qualification_profile', 'full')
    if profile not in ('full', 'focused'):
        raise ValueError('unknown qualification profile')
    if profile == 'focused' and receipt.get('qualification_proofs') != list(gcp_image.FOCUSED_PROOFS):
        raise ValueError('focused receipt proof list mismatch')
    config = json.loads(Path(config_path).read_text())
    if config.get('bucket') != bucket:
        raise ValueError('receipt bucket differs from the configured bucket')
    if config.get('image') not in ('', image['path']):
        raise ValueError('config already pins a different image')
    reference = {'bucket': bucket, 'object': object_name,
                 'generation': str(generation), 'sha256': sha256}
    config.update({'image': image['path'], 'image_qualification': reference,
                   'qualification_profile': profile})
    Path(config_path).write_text(json.dumps(config, indent=2) + '\n')
    return reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--qualification-profile', choices=('focused', 'full'), default='full')
    parser.add_argument('--config', type=Path, help='write image, image_qualification and profile here')
    parser.add_argument('--bucket')
    parser.add_argument('--object', help='uploaded receipt object name')
    parser.add_argument('--generation', help='uploaded receipt generation')
    args = parser.parse_args()
    receipt = make_receipt(json.loads(args.spec.read_text()),
                           qualification_profile=args.qualification_profile)
    with args.output.open('x') as output:
        json.dump(receipt, output, sort_keys=True, indent=2)
        output.write('\n')
    if (args.config, args.bucket, args.object, args.generation) != (None, None, None, None):
        if None in (args.config, args.bucket, args.object, args.generation):
            parser.error('--config requires --bucket, --object and --generation')
        apply_config(args.config, receipt, bucket=args.bucket, object_name=args.object,
                     generation=args.generation,
                     sha256=hashlib.sha256(args.output.read_bytes()).hexdigest())


if __name__ == '__main__':
    main()
