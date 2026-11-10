#!/usr/bin/env python3
"""Render the primary report from the final CSVs."""
import csv
import datetime as dt
import pathlib
import sys

root=pathlib.Path(sys.argv[1]);pilot=root.parent
def read(path):
    with path.open(newline='') as f:return list(csv.DictReader(f))
def f(v):return f'{float(v):,.2f}'
def date(ms):return dt.datetime.fromtimestamp(int(ms)/1000,dt.timezone.utc).isoformat(timespec='milliseconds')
def metric(table,name):return next(r for r in table if r['window']=='all' and r['metric']==name)
def mixval(table,kind,value):return next((float(r['share']) for r in table if r['window']=='all' and r['metric']==kind and r['value']==value),0)
wins=read(root/'windows.csv'); cp=read(root/'checkpoints.csv'); p=read(root/'percentiles.csv'); old=read(pilot/'percentiles.csv'); mix=read(root/'mix.csv'); variation=read(root/'window_variation.csv');rates=read(root/'component_rate_summary.csv');hours=read(root/'hourly_profile.csv');dig=read(root/'digest_validation.csv');api=read(root/'api_validation.csv');con=read(root/'concentration.csv')
assert len(wins)>=12 and all(int(w['last_checkpoint'])-int(w['first_checkpoint'])+1>=2000 for w in wins)
assert all(int(x['mismatches'])==0 and int(x['transactions_checked'])>=100 for x in dig) and len(dig)==len(wins)
assert len(api)>=3 and all(r['match']=='True' for r in api)
durations=[(int(w['last_timestamp_ms'])-int(w['first_timestamp_ms']))/1000 for w in wins]
weekends=sum(dt.datetime.fromtimestamp(int(w['first_timestamp_ms'])/1000,dt.timezone.utc).weekday()>=5 for w in wins)
total=sum(int(w['transactions']) for w in wins);bytes_=sum(int(c['raw_bytes']) for c in cp)
lines=['# Sui mainnet workload — primary sample','','## Scope and validation','',
 f"The primary sample contains {len(wins)} windows of 2,000 consecutive checkpoints ({len(cp):,} checkpoints, {total:,} transactions), spanning {date(wins[0]['first_timestamp_ms'])} through {date(wins[-1]['last_timestamp_ms'])}. {weekends} windows start on a weekend and {len(wins)-weekends} on weekdays (`windows.csv`). UTC is used throughout. These are short observations, not a representative random sample of mainnet time. The pilot is retained in `../REPORT.md` and its CSVs.",
 f"Compressed primary checkpoint files total {bytes_:,} bytes (`checkpoints.csv`), plus pilot and probes. Recomputed transaction digests matched effects for every transaction in every window (`digest_validation.csv`); counts and timestamps independently matched GraphQL for three checkpoints (`api_validation.csv`, `graphql-validation.json`). The archive retention probe and format are documented in the pilot report. The 18 Python tests and one Rust decoder test passed. Reproduction and code: `tidehunter-sharded-wal/experiments/sui-workload/README.md`.",'',
 '| Window | First–last sequence | First UTC | Last UTC | Duration s | Transactions | Known input versions |','|---|---|---|---|---:|---:|---:|']
for w,d in zip(wins,durations):lines.append(f"| {w['window']} | {w['first_checkpoint']}–{w['last_checkpoint']} | {date(w['first_timestamp_ms'])} | {date(w['last_timestamp_ms'])} | {d:.2f} | {int(w['transactions']):,} | {float(w['fraction_versions_created_in_window'])*100:.2f}% |")
busy=max(wins,key=lambda w:int(w['transactions']))
lines.extend(['',f"Window {busy['window']} is the busiest sampled window by transaction count ({int(busy['transactions']):,}); this is not proof of a network-wide peak (`windows.csv`)."])
lines += ['', '## Mix and sizes','', f"User programmable transactions are {mixval(mix,'kind','ProgrammableTransaction')*100:.2f}% of the sample; the rest are system transactions. Failed transactions are {mixval(mix,'success','0')*100:.2f}%. Among user PTBs, {mixval(mix,'ptb_shared','1')*100:.2f}% have a reconstructed shared input (`mix.csv`). A missing shared input does not prove owned-only execution.", '', 'Overall empirical quantiles (linear interpolation; `percentiles.csv`):', '', '| Metric | p50 | p90 | p99 | p99.9 | max |','|---|---:|---:|---:|---:|---:|']
for name in ('checkpoint.transactions','checkpoint.interval_ms','checkpoint.raw_bytes','checkpoint.events','checkpoint.objects_written','ptb.commands','transaction.input_objects','transaction.output_objects','transaction.transaction_bytes','transaction.effects_bytes','transaction.net_gas_mist','output.bcs_bytes','event.bcs_bytes','input.age_checkpoints','input.age_transactions','checkpoint.component_bcs_bytes'):
 r=metric(p,name);lines.append('| '+name+' | '+' | '.join(f(r[k]) for k in ('p50','p90','p99','p99_9','max'))+' |')
lines.extend(['', 'Net gas is computation plus storage cost minus storage rebate, in MIST; it can be negative (`sui/crates/sui-types/src/gas.rs`, `percentiles.csv`).'])
lines += ['', 'Command mix and classifications by window are in `mix.csv`; the full effects change counts and gas values are in `transactions.csv` and `percentiles.csv`. `move_targets.csv` contains the top 30 Move targets and cumulative shares overall and for every window; `first_move_packages.csv` contains the top 20 first-MoveCall packages per user PTB, shares, and observed module names. PTBs with no MoveCall have their own category. Only user PTB commands are included. `top_object_types.csv` gives the top 20 output object types by count and bytes per window; `object_types.csv` includes all types. Shares use all output-object bytes as the denominator, including any package outputs.','',
 'The class rules are: system first; user PTB function names containing `swap`, then `mint`, then a TransferObjects command, then other. [INFERENCE] Name-based swap and mint categories do not prove DEX swaps or NFT mints; obfuscated and indirect calls are missed, and transfers can occur inside Move. See `mix.csv` and `transactions.csv`.','', '## Variation across windows','', 'The table gives minimum, median and maximum of each **window statistic**, not confidence intervals. Shares are fractions, not percentages. `window_variation.csv` includes all additional percentiles and maximums.','', '| Statistic | Min | Median | Max |','|---|---:|---:|---:|']
for row in variation:
 if row['statistic'] in ('p50','p99','mean','share'):
  lines.append(f"| {row['metric']} {row['statistic']} | {float(row['min']):,.4f} | {float(row['median']):,.4f} | {float(row['max']):,.4f} |")
lines += ['', '## Hourly checkpoint profile','', 'Each observed UTC hour is represented only by the windows listed; missing hours have no observations. Rate is the number of within-window checkpoint intervals divided by their elapsed time, not an all-day throughput estimate (`hourly_profile.csv`).','', '| UTC hour | Windows | Checkpoints | Median transactions/checkpoint | Mean transactions/checkpoint | Checkpoints/s |','|---:|---:|---:|---:|---:|---:|']
for r in hours:lines.append(f"| {r['utc_hour']} | {r['windows']} | {r['checkpoints']} | {f(r['median_tx_per_checkpoint'])} | {f(r['mean_tx_per_checkpoint'])} | {f(r['checkpoint_rate_per_second'])} |")
lines += ['', '## Locality and censoring','']
shares={r['rank_or_metric']:r['value'] for r in con if r['window']=='all'}
lines.append('The pooled top 1/10/100/1,000/10,000 reconstructed input IDs hold '+ '/'.join(f"{float(shares[k])*100:.2f}%" for k in ('1','10','100','1000','10000')) + f" of accesses; log-log least-squares Zipf exponent {float(shares['zipf_exponent']):.3f} uses ranks 10–10,000 (`concentration.csv`, `popularity.csv`). This is a descriptive fit, not proof of a Zipf distribution. `hottest_shared.csv` lists the top 20 shared IDs and types.")
lines.append(f"Age distributions include only input versions written earlier within the same window; older versions are unknown and omitted. The censoring horizon is at most 1,999 checkpoints and the observed window durations are {min(durations):.2f}–{max(durations):.2f} seconds (`windows.csv`, `percentiles.csv`). They do not describe older versions. `working_set.csv` gives per-checkpoint growth; `working_set_1000.csv` gives unique-ID growth after each 1,000 checkpoints. Neither is a cache miss curve.")
lines += ['', '## Sustained component-byte rates','', 'The 1- and 10-second buckets use UTC-aligned complete buckets fully contained in a window; empty buckets count as zero. The full bytes of each checkpoint are assigned to its checkpoint timestamp, not to actual execution or write times. Bytes are transaction BCS without signatures, effects BCS, events BCS, and output-object BCS, divided by bucket seconds (`component_rates.csv`, `component_rate_summary.csv`). They are component estimates, not actual node writes.','', '| Bucket | Component | p50 bytes/s | p99 bytes/s |','|---:|---|---:|---:|']
for r in rates:
 if r['window']=='all':lines.append(f"| {r['bucket_seconds']} s | {r['component']} | {f(r['p50_bytes_s'])} | {f(r['p99_bytes_s'])} |")
lines += ['', '## Comparison with the pilot','', '| Metric | Pilot p50 | Primary p50 | Pilot p99 | Primary p99 |','|---|---:|---:|---:|---:|']
for name in ('checkpoint.transactions','ptb.commands','output.bcs_bytes','input.age_checkpoints','input.age_transactions'):
 a=metric(old,name);b=metric(p,name);lines.append(f"| {name} | {f(a['p50'])} | {f(b['p50'])} | {f(a['p99'])} | {f(b['p99'])} |")
old_windows=read(pilot/'windows.csv')
old_coverage=sum(int(w['versions_created_in_window']) for w in old_windows)/sum(int(w['input_accesses']) for w in old_windows)
new_coverage=sum(int(w['versions_created_in_window']) for w in wins)/sum(int(w['input_accesses']) for w in wins)
lines.extend(['',f"The fraction of reconstructed inputs with a known in-window creation changed from {old_coverage*100:.2f}% in the pilot to {new_coverage*100:.2f}% in the primary; checkpoint-age p99 changed from {f(metric(old,'input.age_checkpoints')['p99'])} to {f(metric(p,'input.age_checkpoints')['p99'])} (`../windows.csv`, `windows.csv`, both `percentiles.csv` files)."])
lines += ['', 'The pilot age distribution used only about 22–25 seconds of history per window, while the primary uses the durations above. These are different samples, so changes cannot be attributed to window length alone. Both age distributions are censored. `workload-model.json` records empirical generator inputs and definitions; its popularity is a rank-frequency summary, not a cache trace.','', '## What checkpoint data does not show','', 'The input set is reconstructed from effects, read-only shared roots, explicit transaction references and gas references. The archive object set is checkpoint-level and deduplicated, not a per-transaction KV read trace. Package and dynamic execution reads, index lookups, cache hits, physical I/O and node-internal storage reads are not established. No authority-store table mapping was established. Component bytes exclude signatures, metadata, indexes, WAL, compression, compaction, replication and write amplification; they must not be interpreted as measured persistent I/O. The private fixture trace is a separate experiment.']
(root/'REPORT.md').write_text('\n'.join(lines)+'\n')
