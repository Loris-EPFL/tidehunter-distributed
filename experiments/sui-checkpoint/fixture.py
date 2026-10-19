#!/usr/bin/env python3
"""Archive the complete closed real producer/genesis with portable relative DB paths."""
import argparse
import fcntl
import io
import json
from pathlib import Path
import tarfile
import yaml
import build


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True,help='new archive directory')
    args=parser.parse_args()
    source=args.fixture.resolve(strict=True)
    previous=json.loads((source/'experiment/results.json').read_text())
    if not previous.get('source_frozen') or previous.get('failure') or not 0<previous['checkpoint_limit']<5000:
        raise ValueError('requires a successful genesis-retaining frozen producer')
    lock=(source/'experiment/.sharded-replay.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    args.output.mkdir(parents=True,exist_ok=False)
    files={}
    archive_path=args.output/'fixture.tar.gz'
    with tarfile.open(archive_path,'w:gz') as archive:
        for path in sorted((source/'producer').rglob('*')):
            if path.is_symlink():
                raise ValueError('producer export rejects symlinks')
            if path.is_file():
                relative=str(path.relative_to(source))
                files[relative]=build.digest(path)
                archive.add(path,arcname='fixture/'+relative)
                if build.digest(path)!=files[relative]:
                    raise ValueError('producer changed during export')
        def add_bytes(name,data):
            import hashlib
            files[name]=hashlib.sha256(data).hexdigest()
            member=tarfile.TarInfo('fixture/'+name)
            member.size=len(data)
            member.mode=0o600
            archive.addfile(member,io.BytesIO(data))
        for mode in ('producer','native','distributed'):
            config=yaml.safe_load((source/f'{mode}.yaml').read_text())
            path=Path(config['db-path'])
            relative=path.relative_to(source/mode) if path.is_absolute() else path.relative_to(mode)
            config['db-path']=str(Path(mode)/relative)
            add_bytes(f'{mode}.yaml',yaml.safe_dump(config).encode())
        add_bytes('experiment/results.json',(source/'experiment/results.json').read_bytes())
        manifest=dict(schema=1,files=files.copy(),checkpoint_limit=previous['checkpoint_limit'],
                      checkpoint_digest=previous['checkpoint_digest'],source_range=previous['verified_source_range'])
        add_bytes('fixture-manifest.json',(json.dumps(manifest,indent=2)+'\n').encode())
    for name,expected in files.items():
        if name.startswith('producer/') and build.digest(source/name)!=expected:
            raise ValueError('producer changed before export completed')
    build.save(args.output/'fixture-receipt.json',dict(archive_sha256=build.digest(archive_path),
        checkpoint_digest=previous['checkpoint_digest'],checkpoint_limit=previous['checkpoint_limit'],files=len(files)))
    print(archive_path)


if __name__=='__main__':
    main()
