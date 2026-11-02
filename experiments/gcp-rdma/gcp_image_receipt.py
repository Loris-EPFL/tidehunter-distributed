#!/usr/bin/env python3
"""Build a reviewable image receipt from two uploaded, SHA-pinned diagnostics.

Upload the two actual guest diagnostic files to GCS first. Supply their exact
object generations and SHA-256 values in the input JSON. This tool writes only a
local receipt; uploading it and pinning it in reviewed configuration is separate.
"""
import argparse
import hashlib
import json
from pathlib import Path

import gcp_image


def make_receipt(spec):
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
    gcp_image.validate_pair(records, image, spec['runtime_sha256'], spec['service_account'])
    return {'schema_version': 1, 'image': image, 'recipes': gcp_image.recipe_hashes(),
            'runtime_sha256': spec['runtime_sha256'], 'evidence': refs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = make_receipt(json.loads(args.spec.read_text()))
    with args.output.open('x') as output:
        json.dump(receipt, output, sort_keys=True, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
