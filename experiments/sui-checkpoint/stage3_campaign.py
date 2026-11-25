#!/usr/bin/env python3
"""Stage-3 cell driver with a resumable ledger.

Each cell is one `kv-research` process on a fresh database and a fresh output
directory. Before a rerun, the previous output and database are moved to
`stage3/stale/<case>-attempt-<n>` (never reused). The ledger is saved after
every transition; a cell is `passed` only after every produced artifact's size
and SHA-256 is recorded and checked.

`--resume` skips passed cells whose config hash matches and reruns failed,
pending, invalid and interrupted ones in plan order. `--only` and `--from`
narrow the plan; unit members always run together.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import campaign_ledger as ledger_module
import stage3_specs

STAGE = 'stage3'


def spec_hash(path):
    return ledger_module.sha256_file(path)


def cells_for(root, *, engine_rev, sui_rev, binary_ids, spec_dir=None):
    root = Path(root)
    spec_dir = Path(spec_dir) if spec_dir else root / 'stage3' / 'spec'
    cells = []
    for case in stage3_specs.cells():
        path = spec_dir / f'{case["case_id"]}.json'
        if not path.is_file():
            raise ValueError(f'missing generated spec: {path}')
        cells.append(dict(id=case['case_id'], stage=STAGE,
                          unit=case['case_id'],
                          config_hash=ledger_module.config_hash(
                              spec=spec_hash(path), knobs=dict(case=case),
                              engine_rev=engine_rev, sui_rev=sui_rev,
                              binary_ids=binary_ids),
                          path=str(path)))
    return cells


def artifacts(output):
    rows = []
    for path in sorted(Path(output).rglob('*')):
        if path.is_file() and not path.is_symlink():
            rows.append(dict(path=str(path), size=path.stat().st_size,
                             sha256=ledger_module.sha256_file(path)))
    if not (Path(output) / 'summary.json').is_file():
        raise ValueError('cell produced no summary.json')
    return rows


def archive_previous(path, stale_root, case, attempt):
    if not Path(path).exists():
        return None
    target = Path(stale_root) / f'{case}-attempt-{attempt}'
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(path), str(target))
    return str(target)


def run_cell(cell, *, binary, root, stale_root, attempt, timeout=3600):
    """Run one cell from a fresh database and output; return its artifacts."""
    database = Path(root) / 'stage3' / 'db' / cell['id']
    output = Path(root) / 'stage3' / 'out' / cell['id']
    archive_previous(database, stale_root, cell['id'] + '-db', attempt)
    archive_previous(output, stale_root, cell['id'], attempt)
    command = [str(binary), '--config', cell['path'], '--output', str(output)]
    completed = subprocess.run(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, timeout=timeout)
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout or '')[-4000:]
        raise RuntimeError(f'exit {completed.returncode}: {tail}')
    return artifacts(output)


def run_campaign(cells, *, binary, root, ledger, only=None,
                 from_=None, stop_on_failure=True, timeout=3600):
    root = Path(root)
    ledger.path = Path(ledger.path) if ledger.path else root / 'stage3' / 'run-ledger.json'
    stale_root = root / 'stage3' / 'stale'
    planned = ledger_module.plan(list(cells), ledger, only=only, from_=from_)
    results = []
    for cell in planned:
        entry = ledger.begin(stage=cell['stage'], cell_id=cell['id'],
                             config_hash=cell['config_hash'], unit=cell.get('unit'),
                             start_from='fresh')
        ledger.save()
        try:
            produced = run_cell(cell, binary=binary, root=root,
                                stale_root=stale_root, attempt=entry['attempts'],
                                timeout=timeout)
            ledger.mark_passed(entry, produced)
            ledger.save()
            results.append(dict(id=cell['id'], status='passed'))
        except BaseException as error:
            ledger.fail(entry, error)
            ledger.save()
            results.append(dict(id=cell['id'], status='failed', failure=str(error)))
            if stop_on_failure:
                break
    return results, planned


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True,
                        help='campaign root with stage3/{spec,db,out,stale}')
    parser.add_argument('--binary', type=Path, required=True, help='kv-research')
    parser.add_argument('--ledger', type=Path)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--only', action='append', default=[])
    parser.add_argument('--from', dest='from_')
    parser.add_argument('--continue-on-failure', action='store_true')
    parser.add_argument('--engine-rev', default='unknown')
    parser.add_argument('--sui-rev', default='unknown')
    parser.add_argument('--timeout', type=int, default=3600)
    args = parser.parse_args()
    root = args.root.resolve()
    spec_dir = root / 'stage3' / 'spec'
    if not spec_dir.exists():
        stage3_specs.write(root)
    binary_ids = {name: ledger_module.sha256_file(Path(args.binary))
                  for name in ('kv-research',)}
    cells = cells_for(root, engine_rev=args.engine_rev, sui_rev=args.sui_rev,
                      binary_ids=binary_ids)
    path = args.ledger or root / 'stage3' / 'run-ledger.json'
    if Path(path).exists() and not args.resume:
        raise SystemExit(f'ledger exists; use --resume or a new --ledger: {path}')
    ledger = ledger_module.Ledger(path)
    if args.resume:
        ledger.load()
    results, planned = run_campaign(
        cells, binary=args.binary, root=root, ledger=ledger,
        only=args.only or None, from_=args.from_,
        stop_on_failure=not args.continue_on_failure, timeout=args.timeout)
    print(json.dumps(dict(planned=[cell['id'] for cell in planned], results=results)))


if __name__ == '__main__':
    main()
