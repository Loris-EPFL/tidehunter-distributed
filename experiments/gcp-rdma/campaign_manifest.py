#!/usr/bin/env python3
"""Campaign manifest and initial ledger for `campaign-hybrid-sui-2026-11-25`.

`campaign-manifest.json` binds one campaign to the owner decision, the budget
cap, the three stage windows with their ceilings, the frozen revisions, the
price quote and the case list. `campaign-ledger.json` starts empty and records
one entry per cell. Both files are written locally only. The operator uploads
them to the campaign prefix with `ObjectStoreLedger` (see `--print-upload`).

The values come from `checkpoint-work/research/CLUSTER-CAMPAIGN-PLAN-2026-11-25.md`
(sections 0, 2, 7.1, 8.3, 9.3, 10, 11) and
`checkpoint-work/research/CLUSTER-LAUNCH-CHECKLIST-2026-11-26.md` (sections 3, 4,
5). A changed plan value changes this file, and `manual_sui.identity` then marks
every passed cell `stale`.
"""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'sui-checkpoint'))
import campaign_ledger
import stage2_mechanism
import gcp_budget

SCHEMA = 1
CAMPAIGN_ID = 'campaign-hybrid-sui-2026-11-25'
BUCKET = 'mysten-research-bench-artifacts'
PROJECT = 'mysten-research'
ACCOUNT = 'bench-guest@mysten-research.iam.gserviceaccount.com'
# The campaign directory lives in the project tree, not in the engine repository,
# so the default is relative to the operator's working directory: run this module
# from the project root.
DEFAULT_DIRECTORY = Path('checkpoint-work/cluster-campaign-2026-11-25')


def bucket_prefix(campaign_id=CAMPAIGN_ID):
    return f'gs://{BUCKET}/{campaign_id}'


def parse_bucket_prefix(value):
    """Split `gs://bucket/prefix` into (bucket, prefix) and bound the bucket."""
    if not isinstance(value, str) or not value.startswith('gs://'):
        raise ValueError('bucket prefix must start with gs://')
    rest = value[len('gs://'):].rstrip('/')
    bucket, _, prefix = rest.partition('/')
    if bucket != BUCKET:
        raise ValueError(f'campaign artifacts live in gs://{BUCKET}, not gs://{bucket}')
    if not prefix or any(not part for part in prefix.split('/')):
        raise ValueError('bucket prefix needs a nonempty campaign path')
    return bucket, prefix


def ledger_key(prefix):
    return f'{prefix}/campaign-ledger.json'


# Per-sample files expire with the 90-day rule on `<prefix>/<stage>/raw/`; the
# compact summaries stay under `evidence/`. The group is derived from the file
# name so one helper decides the layout for every driver.
RAW_NAMES = ('samples.csv', 'resources.csv', 'disk.csv')
RAW_SUFFIXES = ('.prom', '.jsonl')


def evidence_group(name):
    """`raw` for a per-sample file, `evidence` for a compact summary."""
    base = name.rsplit('/', 1)[-1]
    if base in RAW_NAMES or base.endswith(RAW_SUFFIXES):
        return 'raw'
    return 'evidence'


def evidence_key(prefix, stage, cell_id, name, attempt=1):
    """One object key; every retry writes its own attempt, never an old case."""
    if not name or name.startswith('/') or '..' in name.split('/'):
        raise ValueError(f'invalid evidence name: {name!r}')
    if type(attempt) is not int or attempt < 1:
        raise ValueError(f'invalid attempt: {attempt!r}')
    return (f'{prefix}/{stage}/{evidence_group(name)}/'
            f'{cell_id}/attempt-{attempt}/{name}')


# One source for the money: budget.json's campaign block, which
# gcp_budget.admit_campaign enforces at create. The manifest never restates a
# cap, a spent total or a fleet ceiling the code does not use.
CAMPAIGN_BUDGET = gcp_budget.load()['campaign']
USD = lambda cents: round(cents / 100, 2)


def fleet_ceiling_usd(hours, hosts=None):
    """The price-based ceiling of one fleet window, as create enforces it."""
    return USD(gcp_budget.fleet_ceiling_cents(
        hours, hosts if hosts is not None else CAMPAIGN_BUDGET['hosts']))

OWNER = dict(
    decision='full option approved',
    decided_utc='2026-11-26',
    approval=('stages 1-3, the full option, parity branch feat/nvmeof-only, '
              'concurrency 1-16 only, stage 3 variant A'),
    grants_usd=[dict(reason='stages 1 and 2 ceiling', amount=21.03),
                dict(reason='stage 3 ceiling', amount=17.09)],
    cap_usd=float(CAMPAIGN_BUDGET['cap_usd']),
    note='the owner writes the two grants and the cap in coordination/LEDGER.md')

BUDGET = dict(
    cap_usd=float(CAMPAIGN_BUDGET['cap_usd']),
    spent_usd=float(CAMPAIGN_BUDGET['spent_usd']),
    remaining_usd=USD(round(CAMPAIGN_BUDGET['cap_usd'] * 100)
                      - round(CAMPAIGN_BUDGET['spent_usd'] * 100)),
    cap_rule=('create refuses a fleet whose spent_usd plus the price-based ceiling '
              'of its window exceeds cap_usd'),
    spent_source=('stage 0 $4.03 plus the failed stage-1 fleet $72.4; the operator '
                  'raises spent_usd after each fleet'),
    resume_admission_usd=6.04,
    resume_min_remaining_usd=7.00,
    fleet_ceilings_usd={str(hours): fleet_ceiling_usd(hours) for hours in (1, 2, 5, 6)})

PRICE = dict(
    machine='z4m-highmem-16',
    region='us-central1',
    zone='us-central1-a',
    usd_per_host_hour=CAMPAIGN_BUDGET['nanodollars_per_host_hour'] / 1e9,
    nanodollars_per_host_hour=CAMPAIGN_BUDGET['nanodollars_per_host_hour'],
    observed_utc='2026-11-02T12:38:36Z',
    includes='mandatory 14000 GiB Local SSD',
    excludes='boot disks, network traffic, retained artifacts, taxes, account discounts',
    ceiling_rule='14500/11834 of compute',
    ceiling_factor=CAMPAIGN_BUDGET['ceiling_numer'] / CAMPAIGN_BUDGET['ceiling_denom'],
    launcher_constant='experiments/gcp-rdma/gcp_budget.py:fleet_ceiling_cents',
    source='https://cloud.google.com/products/compute/pricing/storage-optimized',
    recheck='re-check the price before admission; the 2026-11-02 value is not the invoice')

# The operator alarm sits a fixed margin above the code ceiling; the create
# refusal at ceiling_usd is the hard stop.
STOP_MARGIN_USD = 2.00


def _stage(stage_id, name, driver, window_hours, stop_rule):
    ceiling = fleet_ceiling_usd(window_hours)
    return dict(id=stage_id, name=name, driver=driver, hosts=CAMPAIGN_BUDGET['hosts'],
                window_hours=window_hours,
                host_hours=CAMPAIGN_BUDGET['hosts'] * window_hours,
                compute_usd=USD(round(gcp_budget.fleet_ceiling_cents(
                    window_hours, CAMPAIGN_BUDGET['hosts'])
                    * CAMPAIGN_BUDGET['ceiling_denom'] / CAMPAIGN_BUDGET['ceiling_numer'])),
                ceiling_usd=ceiling, stop_usd=round(ceiling + STOP_MARGIN_USD, 2),
                stop_rule=stop_rule)


STAGES = [
    _stage('stage1', 'correctness', 'manual_sui.py', 5,
           'stop the campaign on the first failed case'),
    _stage('stage2', 'measurement', 'manual_sui.py', 6,
           'no cell starts after 4 h 54 min of the window'),
    _stage('stage3', 'synthetic workload set', 'stage3_campaign.py', 2,
           'no cell starts after 1 h 25 min of the window'),
]
_RUNNING = round(CAMPAIGN_BUDGET['cap_usd'] * 100) - round(CAMPAIGN_BUDGET['spent_usd'] * 100)
for _entry in STAGES:
    _RUNNING -= round(_entry['ceiling_usd'] * 100)
    _entry['entry_remaining_usd'] = USD(_RUNNING + round(_entry['ceiling_usd'] * 100))

REVISIONS = dict(
    engine='2bff0e8a4628aa65363bc0e3b1d24817ee90585e',
    engine_base='df6c3b1daf3c0cd11706951fd3ab5c87d892e0b2',
    engine_branch='origin/feat/nvmeof-only',
    sui='11b95aec95f386b29d9a074c9ad7e642d416f55c',
    sui_branch='hybrid-tidy',
    bundle_sha256='c7b69b8bd942a119bcced33e0e5dbc4222ee47650306f03471c8b42ad0230fa9',
    build_manifest_sha256='450e2b37c38afff1f8fb793a922d213675c17507ef795a00d9b4be97c1a6e609',
    source_archive_sha256='efe40242d235db1d50e3aaa84480f069a1e09aabbca6df576ee55e8a46156fc3',
    schema_digest='03f5401b4b795879c1236b2d3607bd06dfa3de1d6b426597547758518836ff09',
    schema_names=['checkpoints', 'store/epoch_0', 'store/perpetual'],
    binaries={
        'sui-node-native': 'b3ba12b9c44687d3ca6ec41cc88eeedc374b8e57c86f821438da19363238dcf7',
        'sui-node-remote': '488db86eb4e4be6f6b2fe2bfa82cbbb05c001cd7ebbe851a08b2c039e06f8648',
        'rocksdb-audit': 'dd1aa7171ea95561271d84ed715391d7add563b94074fff4ce55a17a37844ff6',
        'storage-node': '215f81e089c49f74b7a8249a5ca580414d310b835f0d197d0fc162b05441f6f8',
        'state-audit': '1384bd9382ccd7d756b4c9bd4921d273688690e57896da003d8ea77de99b4c89',
    },
    build_features=dict(fabric=False, protocol=9,
                        native='typed-store/tidehunter', remote='sui-node/sharded-wal'),
    fixture=dict(path='checkpoint-fixtures/sui-genesis-1775/fixture',
                 checkpoint_limit=1775, checkpoints=1776,
                 digest='EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6',
                 digest_list_sha256='896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f',
                 files=1306, archive_prefix='7eb43b71', seed=1775),
    kernel_module=dict(name='nvmet.ko', sha256_prefix='5ba7b96c3b232e7f',
                       kernel='5.14.0-687.49.1+2.1.el9_8_ciq.x86_64'),
    image=dict(state='blocked', reason='config.json:image is empty and no receipt matches'))

TOPOLOGY = dict(
    hosts=3,
    placement='group placement, COLLOCATED, max_distance 3',
    roles=dict(H0='Sui node, coordinator, producer, checkpoint driver',
               H1='owner 1, NVMe-oF target', H2='owner 2, NVMe-oF target'),
    control_transport='tcp',
    control_security='mTLS, per-host leaf keys plus a public CA',
    data_transport='rdma',
    coordinator_cache='off in every cell',
    owner_page_cache='active in every cell',
    owner_extents_mib=16,
    coordinator_index_fragment_mib=1024)

STAGE1_CASES = [
    dict(id='S1-0', case='admission and qualification', tiers=['native'], points=1,
         est_minutes=30, driver='manual_sui.py'),
    dict(id='S1-1', case='clean lifecycle', tiers=['native', 'synchronous'], points=1,
         est_minutes=20, driver='manual_sui.py',
         pass_criteria=['audit digest equals the producer digest',
                        'a clean reopen creates no new export',
                        'the durable cut does not extend after flush_durable',
                        'the tail replay is idempotent',
                        'all ten RPC index pipelines commit at the frozen tip']),
    dict(id='S1-2', case='coordinator crash', tiers=['native', 'synchronous'], points=2,
         est_minutes=32, driver='manual_sui.py'),
    dict(id='S1-3', case='owner process crash', tiers=['native', 'synchronous'], points=1,
         est_minutes=16, driver='manual_sui.py'),
    dict(id='S1-4', case='W1 resume after controller loss', tiers=['native', 'synchronous'],
         points=3, cells=5, est_minutes=30, driver='manual_sui.py --controller-loss',
         note=('three points x two tiers, less S1-4-during-flush-native: native '
               'forces durable_at_completion, so that point is refused at parse and '
               'is not applicable')),
    dict(id='S1-5', case='leases and fencing', tiers=['native'], points=1,
         est_minutes=30, driver='manual_sui.py'),
    dict(id='S1-6', case='retirement and relocation', tiers=['native', 'synchronous'],
         points=2, est_minutes=16, driver='manual_sui.py'),
    dict(id='S1-7', case='cache correctness', tiers=['native', 'synchronous'], points=1,
         est_minutes=15, driver='manual_sui.py'),
    dict(id='S1-8', case='full Sui audit after faults', tiers=['native', 'synchronous'],
         points=1, est_minutes=20, driver='manual_sui.py'),
]

def _mechanism_cells(family):
    """The stage-2 mechanism rows of one plan family, as the driver builds them."""
    return [cell for cell in stage2_mechanism.cells() if cell.get('family') == family]


def _mechanism_row(family, case, est_minutes):
    """One stage-2 row whose cell count, tiers and widths come from the code."""
    cells = _mechanism_cells(family)
    if not cells:
        raise ValueError('no mechanism cell for ' + family)
    return dict(id=family, case=case, cells=len(cells), synthetic=True,
                tiers=sorted({cell['tier'] for cell in cells}),
                driver='fault_cells.py', est_minutes=est_minutes)


def _m6_row():
    cells = _mechanism_cells('M6')
    widths = sorted({1 if cell['runs'][0]['mix'].get('get_many', 0) == 0
                     else cell['runs'][0]['batch'] for cell in cells})
    concurrency = sorted({cell['runs'][0]['concurrency'] for cell in cells})
    return dict(_mechanism_row('M6', 'read-path lock wait under Sui multi_get', 27),
                keyspaces=['synthetic (one keyspace covers objects and executed_effects)'],
                widths=widths, concurrency=concurrency, tiers=['native'])


STAGE2_CASES = [
    dict(id='M1', case='real Sui replay baseline, catch-up and paced', cells=7,
         driver='manual_sui.py -- --driver-concurrency N [--pacing-ms 245]',
         driver_concurrency=[1, 2, 4, 8, 16], tiers=['native', 'synchronous'],
         est_minutes=58),
    dict(id='M2', case='value cache', cells=4, driver='manual_sui.py',
         arms=['1000 native', '128 native', '0 native', '1000 synchronous'],
         driver_concurrency=[4], est_minutes=20),
    dict(id='M3', case='flush mechanism, forced', cells=2, synthetic=True,
         driver='manual_sui.py -- --max-dirty-keys 16 --max-maps 3 --fragment-mib 64',
         forced=dict(max_dirty_keys=16, max_maps=3, fragment_mib=64),
         tiers=['native', 'synchronous'], est_minutes=10),
    dict(id='M4', case='blob-cache mechanism, forced', cells=2, synthetic=True,
         driver='manual_sui.py -- --index-blob-cache-mib N --index-blob-cache-min-bytes 0',
         forced=dict(index_blob_cache_min_bytes=0, fragment_mib=64,
                     index_blob_cache_mib=[0, 1024]),
         tiers=['native', 'synchronous'], est_minutes=10),
    dict(id='M5', case='coordinator disk use', cells=1, hold_minutes=30,
         driver='manual_sui.py',
         note='one 30 min coordinator hold with disk sampling (M5 hold mode)',
         est_minutes=30),
    _m6_row(),
    dict(id='M7', case='W1 resume after controller loss', cells=5,
         points=3, driver='manual_sui.py --controller-loss',
         tiers=['native', 'synchronous'], driver_concurrency=[4], est_minutes=30,
         note='as S1-4: during-flush at native is not applicable'),
    dict(id='M8', case='fencing latency', cells=8, cases=4, driver='fault_cells.py',
         tiers=['native', 'synchronous'], est_minutes=24),
    dict(_mechanism_row('M9', 'retirement and relocation, forced', 16),
         forced=dict(fragment_mib=64), driver_concurrency=[4]),
    dict(_mechanism_row('M13', '132 KB-batch Synchronous threshold', 5),
         driver_concurrency=[4]),
]

STAGE3_CASES = dict(
    cell_total=72,
    parameters=dict(keys=65536, batch_size=26, seed=1775, queue_capacity=64,
                    request_timeout_ms=30000, measurement_seconds=40,
                    warmup_fraction=0.1, sizes=[256, 8192],
                    distributions=['uniform', 'zipf-1.0'],
                    coordinator_value_cache=0,
                    caller_counts=[2, 4, 8, 16],
                    per_machine_callers=[1, 2, 4, 8]),
    distributed=[
        dict(id='S3-A1', family='point read', mix='read 100', size=8192,
             distribution='uniform', total_c=[2, 4, 8, 16], cells=8),
        dict(id='S3-A2', family='mixed', mix='read 70, put 30', size=8192,
             distribution='uniform', total_c=[2, 4, 8, 16], cells=8),
        dict(id='S3-A3', family='point write', mix='put 100', size=8192,
             distribution='uniform', total_c=[2, 4, 8, 16], cells=8),
        dict(id='S3-A4', family='A1 to A3', size=256, distribution='uniform',
             total_c=[16], cells=6),
        dict(id='S3-A5', family='A1 to A3', size=8192, distribution='zipf-1.0',
             total_c=[16], cells=6),
        dict(id='S3-A6', family='A1 to A3', size=256, distribution='zipf-1.0',
             total_c=[16], cells=6),
        dict(id='S3-A7', family='multi_get 26', mix='get_many 100', size=8192,
             distribution='uniform', total_c=[4, 16], cells=4),
        dict(id='S3-A8', family='put_batch 26', mix='put_batch 100', size=8192,
             distribution='uniform', total_c=[4, 16], cells=4),
        dict(id='S3-A9', family='A7 and A8', size=8192, distribution='zipf-1.0',
             total_c=[16], cells=4)],
    native_reference=[
        dict(id='S3-B1', family='point read', mix='read 100', size=8192,
             distribution='uniform', callers=[1, 2, 4, 8], cells=8),
        dict(id='S3-B2', family='point write', mix='put 100', size=8192,
             distribution='uniform', callers=[1, 2, 4, 8], cells=8)],
    cache_on_reference=[
        dict(id='S3-C1', arm='distributed', family='point read', size=8192,
             tier='native', total_c=16, value_cache=1000, cells=1),
        dict(id='S3-C2', arm='native', family='point read', size=8192,
             tier='native', callers=8, value_cache=1000, cells=1)])


def _stage3_totals(cases):
    """The approved composition: 55 distributed (arm A) and 17 native (arm B)."""
    distributed = sum(case['cells'] for case in cases['distributed'])
    native = sum(case['cells'] for case in cases['native_reference'])
    cache_on = {case['arm']: case['cells'] for case in cases['cache_on_reference']}
    if set(cache_on) != {'distributed', 'native'} or any(cells != 1 for cells in cache_on.values()):
        raise ValueError('stage 3 needs exactly one distributed and one native cache-on cell')
    total = distributed + native + sum(cache_on.values())
    if cases['cell_total'] != total:
        raise ValueError('stage 3 cell rows do not sum to cell_total')
    return {'distributed_cells': distributed + cache_on['distributed'],
            'native_cells': native + cache_on['native'],
            'total_cells': total}


STAGE3_CASES['totals'] = _stage3_totals(STAGE3_CASES)

EVIDENCE = dict(
    files=['campaign-manifest.json', 'campaign-ledger.json', 'run-manifest.json',
           'cases.csv', 'latency.csv', 'resources.csv', 'disk.csv', 'faults.csv',
           'audit/', 'evidence/', 'raw/', 'SHA256SUMS'],
    raw_retention_days=90,
    raw_rule='a bucket lifecycle rule deletes raw/ after 90 days',
    compact_retention='permanent',
    max_log_excerpt='first and last 5 MiB, plus the exit status')


def cases():
    return dict(stage1=STAGE1_CASES, stage2=STAGE2_CASES, stage3=STAGE3_CASES)


def build_revisions(build_root):
    """Rebind the manifest to one finished `build.py` output."""
    root = Path(build_root)
    receipt = json.loads((root / 'build-receipt.json').read_text())
    manifest_value = json.loads((root / 'build-manifest.json').read_text())
    sources = json.loads((root / 'source-manifest.json').read_text())
    return dict(engine=sources['tidehunter-sharded-wal']['revision'],
                sui=sources['sui']['revision'],
                bundle_sha256=receipt['bundle_sha256'],
                build_manifest_sha256=receipt['build_manifest_sha256'],
                source_archive_sha256=receipt['source_archive_sha256'],
                binaries=manifest_value['binaries'])


def manifest(campaign_id=CAMPAIGN_ID, revisions=None):
    """The whole manifest; `revisions` overrides rows of a rebuilt bundle."""
    if campaign_id != CAMPAIGN_ID:
        raise ValueError(f'this manifest binds {CAMPAIGN_ID}, not {campaign_id!r}')
    prefix = f'{CAMPAIGN_ID}'
    bound = dict(REVISIONS)
    bound.update(revisions or {})
    return dict(
        schema=SCHEMA,
        campaign=dict(id=CAMPAIGN_ID, project=PROJECT, branch=bound['engine_branch'],
                      stages=[stage['id'] for stage in STAGES]),
        owner=OWNER,
        budget=BUDGET,
        stages=STAGES,
        price=PRICE,
        revisions=bound,
        topology=TOPOLOGY,
        cases=cases(),
        evidence=EVIDENCE,
        storage=dict(bucket=BUCKET, prefix=prefix, uri=bucket_prefix(),
                     ledger_key=ledger_key(prefix),
                     evidence_layout=(f'{prefix}/<stage>/{{evidence|raw}}/<cell>/'
                                      'attempt-<n>/<name>'),
                     retry_rule='a retry writes attempt-<n+1>; it never overwrites a failed case',
                     account=ACCOUNT,
                     uploader='ObjectStoreLedger: generation precondition and SHA-256 receipt',
                     state='local only; the operator uploads before the first rented host'))


def write(directory=DEFAULT_DIRECTORY, *, campaign_id=CAMPAIGN_ID, revisions=None):
    """Write the manifest and an empty ledger; never clobber a started ledger."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    value = manifest(campaign_id, revisions)
    ledger_path = directory / 'campaign-ledger.json'
    if ledger_path.exists():
        started = campaign_ledger.Ledger(ledger_path).load().entries
        if started:
            raise ValueError(f'ledger already records {len(started)} cell(s): {ledger_path}')
    payload = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    temporary = directory / 'campaign-manifest.json.tmp'
    temporary.write_bytes(payload)
    temporary.replace(directory / 'campaign-manifest.json')
    campaign_ledger.Ledger(ledger_path).save()
    return dict(manifest=str(directory / 'campaign-manifest.json'),
                ledger=str(ledger_path),
                manifest_sha256=campaign_ledger.sha256_bytes(payload),
                ledger_sha256=campaign_ledger.sha256_file(ledger_path),
                storage=value['storage'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument('--campaign-id', default=CAMPAIGN_ID)
    parser.add_argument('--build', type=Path,
                        help='finished build.py output; rebinds every revision and binary hash')
    parser.add_argument('--bundle-sha256')
    parser.add_argument('--build-manifest-sha256')
    parser.add_argument('--source-archive-sha256')
    args = parser.parse_args()
    revisions = build_revisions(args.build) if args.build else {}
    revisions.update({key: value for key, value in (
        ('bundle_sha256', args.bundle_sha256),
        ('build_manifest_sha256', args.build_manifest_sha256),
        ('source_archive_sha256', args.source_archive_sha256)) if value})
    receipt = write(args.directory, campaign_id=args.campaign_id, revisions=revisions)
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
