#!/usr/bin/env python3
"""Generate the pinned mainnet fixture offline; never execute Sui or restore state.

Python stdlib + zstd. The committed compressed source is mandatory because its public
archive retention expires. This module deliberately has no network/download path.
"""
import argparse
import collections
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import struct
import subprocess

from codec import BCS, bcs_payload, one, proto, uleb
import trace_format as tf

HERE = Path(__file__).resolve().parent
MAX_DECOMPRESSED_BYTES = 8_000_000
DIGEST_PREFIX = (32).to_bytes(8, 'big')
SCOPE = tf.SCOPE


def digest(name, raw):
    return hashlib.blake2b(name.encode() + b'::' + raw, digest_size=32).digest()


def object_key(ref):
    return ref[0] + ref[1].to_bytes(8, 'big')


def bcs_digest(raw):
    tf.require(len(raw) == 32, 'digest must have 32 bytes')
    return b'\x20' + raw


def owner(reader):
    start = reader.i
    reader.owner()
    return reader.b[start:reader.i]


def stored_object(raw, ref):
    """Return stored bytes and independently serializable field slices."""
    r = BCS(raw)
    data = r.tag()
    gas, balance = False, None
    if data == 0:
        typ = r.tag()
        if typ == 0:
            r.structtag()
        elif typ in (3, 5):
            r.typetag()
        else:
            tf.require(typ in (1, 2, 4), 'unknown MoveObjectType')
        gas = typ == 1
        tf.require(r.u(1) in (0, 1), 'invalid public-transfer boolean')
        version = r.u()
        contents = r.blob()
        tf.require(len(contents) >= 32 and (contents[:32], version) == ref, 'ObjectKey mismatch')
        if gas:
            tf.require(len(contents) == 40, 'invalid Coin<SUI> contents')
            balance = int.from_bytes(contents[32:], 'little')
    elif data == 1:
        tf.require((r.take(32), r.u()) == ref, 'package ObjectKey mismatch')
        for _ in range(r.tag()):
            r.blob(); r.blob()  # module name and bytecode
        for _ in range(r.tag()):
            r.blob(); r.blob(); r.take(32)  # TypeOrigin
        for _ in range(r.tag()):
            r.take(32); r.take(32); r.take(8)  # linkage and UpgradeInfo
    else:
        raise ValueError(f'unknown Object Data variant {data}')
    data_end = r.i
    own = owner(r)
    previous = r.digest()
    rebate = r.u()
    r.end()
    tail = raw[data_end:]
    value = b'\x00\x00\x03' + balance.to_bytes(8, 'little') + tail if gas else b'\x00\x00' + raw
    fields = dict(data=list(raw[:data_end]), owner=list(own), previous=list(previous),
                  rebate=rebate, gas=gas, balance=balance)
    return value, fields


def input_arguments(raw):
    """Mirror transaction input_objects(): shared initial versions are NOT read refs.

Commands and system payloads are opaque canonical BCS copied from the SHA-pinned
archive. Their complete byte identity is checked against checkpoint transaction digests.
"""
    r = BCS(raw)
    tf.require(r.tag() == 0, 'unknown TransactionData version')
    kind = r.tag()
    tf.require(kind <= 10, 'unknown TransactionKind')
    explicit, receiving, shared = [], [], {}
    if kind not in (0, 10):
        return explicit, receiving, shared
    for _ in range(r.tag()):
        arg = r.tag()
        if arg == 0:
            r.blob()
        elif arg == 1:
            typ, oid, version = r.tag(), r.take(32), r.u()
            if typ in (0, 2):
                object_digest = r.digest()
                if typ == 2:
                    receiving.append((oid, version))
                elif object_digest[-20:] != b'\xac' * 20:
                    explicit.append((oid, version))
            elif typ == 1:
                tf.require(r.tag() in (0, 1, 2), 'unknown shared mutability')
                shared[oid] = version
            else:
                raise ValueError(f'unknown ObjectArg {typ}')
        elif arg == 2:
            tf.require(r.tag() == 0, 'unknown reservation')
            r.take(8)
            tf.require(r.tag() == 0, 'unknown withdrawal type')
            r.typetag()
            source = r.tag()
            if source == 2:
                r.take(64)
            else:
                tf.require(source in (0, 1), 'unknown withdrawal source')
        else:
            raise ValueError(f'unknown CallArg {arg}')
    return explicit, receiving, shared


@dataclass
class Effects:
    transaction: bytes
    inputs: dict
    outputs: list
    tombstones: list
    readonly: list
    unchanged: list
    changes: list
    lamport: int
    events: bytes | None
    accumulators: int


def effects(raw, epoch):
    r = BCS(raw)
    tf.require(r.tag() == 1, 'only effects V2 is supported')
    r.status()
    tf.require(r.u() == epoch, 'effects epoch mismatch')
    r.take(32)
    tx_digest = r.digest()
    r.option(lambda: r.take(4))
    event_digest = r.option(r.digest)
    for _ in range(r.tag()):
        r.digest()
    lamport = r.u()
    inputs, outputs, tombstones, changes = {}, [], [], []
    accumulator_count = 0
    for _ in range(r.tag()):
        oid = r.take(32)
        input_kind = r.tag()
        tf.require(input_kind in (0, 1), 'unknown ObjectIn')
        old_owner = new_owner = None
        if input_kind:
            version = r.u(); r.digest(); old_owner = owner(r)
            tf.require(oid not in inputs, 'duplicate changed object')
            inputs[oid] = (version, old_owner)
        output_kind = r.tag()
        if output_kind == 1:
            r.digest(); new_owner = owner(r); outputs.append((oid, lamport))
        elif output_kind == 2:
            version = r.u(); r.digest(); outputs.append((oid, version))
        elif output_kind == 3:
            accumulator_count += 1
            r.take(32); r.typetag()
            tf.require(r.tag() in (0, 1), 'unknown accumulator operation')
            value = r.tag()
            if value == 0:
                r.take(8)
            elif value == 1:
                r.take(16)
            elif value == 2:
                for _ in range(r.tag()):
                    r.take(8); r.digest()
            else:
                raise ValueError(f'unknown accumulator value {value}')
        else:
            tf.require(output_kind == 0, 'unknown ObjectOut')
        operation = r.tag()
        tf.require(operation in (0, 1, 2), 'unknown IDOperation')
        if output_kind == 0 and operation == 2:
            tombstones.append((oid, lamport, 'deleted'))
        elif output_kind == 0 and input_kind == 1 and operation == 0:
            tombstones.append((oid, lamport, 'wrapped'))
        changes.append((oid, old_owner, new_owner, operation))
    readonly, unchanged = [], []
    for _ in range(r.tag()):
        oid, kind = r.take(32), r.tag()
        tf.require(kind in range(5), 'unknown UnchangedConsensusKind')
        version = None if kind == 4 else r.u()
        if kind == 0:
            r.digest(); readonly.append((oid, version))
        unchanged.append((oid, kind, version))
    r.option(r.digest)
    r.end()
    return Effects(tx_digest, inputs, outputs, tombstones, readonly, unchanged, changes,
                   lamport, event_digest, accumulator_count)


def marker_key(epoch, oid, version, own):
    consensus = own is not None and own[0] in (2, 4)
    start = int.from_bytes(own[1:9], 'little') if consensus else 0
    return ((epoch | (int(consensus) << 63)).to_bytes(8, 'big') + oid
            + start.to_bytes(8, 'big') + version.to_bytes(8, 'big'))


def marker_rows(fx, epoch, receiving, shared):
    rows = []
    for oid, version in receiving:
        if oid in fx.inputs and fx.inputs[oid][0] == version:
            rows.append((marker_key(epoch, oid, version, None), b'\x00'))
    for oid, version, _ in fx.tombstones:
        own = fx.inputs.get(oid, (None, None))[1]
        value = b'\x02' + bcs_digest(fx.transaction) if own and own[0] in (2, 4) else b'\x01'
        rows.append((marker_key(epoch, oid, version, own), value))
    # These loops preserve the node's marker chain order.
    for oid, old, new, op in fx.changes:
        if op == 0 and old and new and old[0] in (0, 1) and new[0] == 4:
            rows.append((marker_key(epoch, oid, fx.lamport, None), b'\x01'))
    for oid, old, new, op in fx.changes:
        if op == 0 and old and new and old[0] == 4 and (
                new[0] in (0, 1, 3) or (new[0] == 4 and old[9:] != new[9:])):
            rows.append((marker_key(epoch, oid, fx.lamport, old), b'\x02' + bcs_digest(fx.transaction)))
    for oid, kind, _ in fx.unchanged:
        if kind == 1:
            own = fx.inputs.get(oid, (None, None))[1]
            if own is None:
                tf.require(oid in shared, 'ended stream lacks initial shared version')
                own = b'\x02' + shared[oid].to_bytes(8, 'little')
            rows.append((marker_key(epoch, oid, fx.lamport, own), b'\x02' + bcs_digest(fx.transaction)))
    return rows


def checkpoint_contents(raw):
    r = BCS(raw)
    version, count = r.tag(), r.tag()
    tf.require(version in (0, 1), 'unknown CheckpointContents version')
    pairs, groups, plain = [], [], []
    if version == 0:
        pairs = [(r.digest(), r.digest()) for _ in range(count)]
        tf.require(r.tag() == count, 'signature group count mismatch')
    for _ in range(count):
        if version == 1:
            pairs.append((r.digest(), r.digest()))
        start = r.i
        signatures = []
        for _ in range(r.tag()):
            signatures.append(r.blob())
            if version == 1:
                r.option(lambda: r.take(8))
        groups.append(raw[start:r.i])
        plain.append(uleb(len(signatures)) + b''.join(uleb(len(s)) + s for s in signatures))
    r.end()
    return version, pairs, groups, plain


def decompress(source):
    raw = source.read_bytes()
    tf.require(len(raw) <= tf.MAX_FILE_BYTES and tf.sha(raw) == tf.SOURCE_SHA256,
               'wrong or oversized compressed mainnet source')
    process = subprocess.Popen(['zstd', '-dc', str(source)], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    try:
        decoded = process.stdout.read(MAX_DECOMPRESSED_BYTES + 1)
        if len(decoded) > MAX_DECOMPRESSED_BYTES:
            raise ValueError('decompressed checkpoint exceeds cap')
        error = process.stderr.read(4097)
        tf.require(process.wait() == 0, f'zstd failed: {error[:4096]!r}')
    finally:
        process.stdout.close(); process.stderr.close()
        if process.poll() is None:
            process.kill(); process.wait()
    return raw, proto(decoded)


def derive(source):
    compressed, cp = decompress(Path(source))
    tf.require(one(cp, 1) == tf.SEQUENCE, 'wrong checkpoint sequence')
    summary = bcs_payload(proto(one(cp, 3)), 'CheckpointSummary')
    sr = BCS(summary)
    epoch, sequence, total_transactions = sr.u(), sr.u(), sr.u()
    tf.require((epoch, sequence) == (tf.EPOCH, tf.SEQUENCE), 'wrong mainnet checkpoint')
    content_digest = sr.digest()
    sr.option(sr.digest); sr.take(32)
    timestamp_ms = sr.u()
    for _ in range(sr.tag()):
        tf.require(sr.tag() in (0, 1), 'unknown checkpoint commitment'); sr.digest()
    tf.require(sr.tag() == 0, 'epoch-ending checkpoints are outside this fixture')
    sr.blob(); sr.end()
    contents = bcs_payload(proto(one(cp, 5)), 'CheckpointContents')
    tf.require(digest('CheckpointContents', contents) == content_digest, 'contents digest mismatch')
    version, pairs, signature_groups, plain_signatures = checkpoint_contents(contents)
    sig = proto(one(cp, 4))
    tf.require(one(sig, 1) == epoch and len(one(sig, 2)) == 48, 'invalid quorum signature fields')
    bitmap = one(sig, 3)
    certified = summary + epoch.to_bytes(8, 'little') + one(sig, 2) + uleb(len(bitmap)) + bitmap
    cp_digest = digest('CheckpointSummary', summary)
    objects, reference_objects = {}, []
    for encoded in proto(one(cp, 7))[1]:
        obj = proto(encoded)
        ref = (bytes.fromhex(one(obj, 2).decode().removeprefix('0x').zfill(64)), one(obj, 3))
        tf.require(len(ref[0]) == 32 and ref not in objects, 'duplicate/invalid archived ObjectKey')
        value, fields = stored_object(bcs_payload(obj, 'Object'), ref)
        objects[ref] = value
        reference_objects.append(dict(key=list(ref[0]), version=ref[1], value=list(value), **fields))
    tf.require(len(pairs) == len(cp[6]), 'transaction count mismatch')
    transactions, staged, preload, demands = [], {}, {}, []
    accumulators, unchanged = 0, collections.Counter()
    for index, encoded in enumerate(cp[6]):
        tx = proto(encoded)
        data = bcs_payload(proto(one(tx, 2)), 'TransactionData')
        ep = proto(one(tx, 4))
        fx_raw = bcs_payload(ep, 'TransactionEffects')
        tx_digest, effect_digest = pairs[index]
        tf.require(digest('TransactionData', data) == tx_digest, 'transaction digest mismatch')
        tf.require(digest('TransactionEffects', fx_raw) == effect_digest, 'effects digest mismatch')
        fx = effects(fx_raw, epoch)
        tf.require(fx.transaction == tx_digest, 'effects transaction mismatch')
        explicit, receiving, shared = input_arguments(data)
        loaded = []
        for encoded_ref in ep.get(15, []):
            ref = proto(encoded_ref)
            oid = bytes.fromhex(one(ref, 1).decode().removeprefix('0x').zfill(64))
            tf.require(len(oid) == 32, 'invalid runtime ObjectID')
            loaded.append((oid, one(ref, 2)))
        refs = sorted(set(explicit + [(oid, v) for oid, (v, _) in fx.inputs.items()]
                          + fx.readonly + loaded), key=object_key)
        for ref in refs:
            tf.require(ref in objects, f'missing archived input at transaction {index}')
            overlay = ref in staged
            if overlay:
                tf.require(staged[ref] == objects[ref], 'overlay value differs from archive')
            else:
                preload[ref] = objects[ref]
            demands.append((index, ref, objects[ref], overlay))
        for ref in fx.outputs:
            tf.require(ref in objects and ref not in staged, 'missing/duplicate live output')
            staged[ref] = objects[ref]
        for oid, v, kind in fx.tombstones:
            staged[oid, v] = b'\x00\x01' if kind == 'deleted' else b'\x00\x02'
        events = None if fx.events is None else bcs_payload(proto(one(tx, 5)), 'TransactionEvents')
        tf.require((5 in tx) == (fx.events is not None), 'events presence mismatch')
        if events is not None:
            tf.require(digest('TransactionEvents', events) == fx.events, 'events digest mismatch')
        trusted = b'\x01\x00\x00\x00' + data + plain_signatures[index]
        transactions.append(dict(digest=tx_digest, effects_digest=effect_digest, trusted=trusted,
                                 raw=data, effects=fx_raw, refs=refs, fx=fx, events=events,
                                 markers=marker_rows(fx, epoch, receiving, shared), loaded=loaded,
                                 signatures=plain_signatures[index]))
        accumulators += fx.accumulators
        unchanged.update(kind for _, kind, _ in fx.unchanged)
    tf.require(not preload.keys() & staged.keys(), 'input precedes its in-window producer')
    full = (bytes([version]) + uleb(len(transactions))
            + b''.join(t['trusted'] + t['effects'] for t in transactions)
            + uleb(len(transactions)) + b''.join(signature_groups))
    return dict(compressed=compressed, objects=objects, references=reference_objects,
                summary=summary, certified=certified, cp_digest=cp_digest, contents=contents,
                content_digest=content_digest, transactions=transactions, full=full, preload=preload,
                demands=demands, epoch=epoch, sequence=sequence, timestamp_ms=timestamp_ms,
                network_total_transactions=total_transactions, accumulators=accumulators,
                unchanged=dict(sorted(unchanged.items())))


def build(model):
    txs, seq, epoch = model['transactions'], model['sequence'], model['epoch']
    counts, values = collections.Counter(), collections.defaultdict(list)
    records = [tf.frame(tf.BEGIN_CHECKPOINT, struct.pack('<QQI', seq, epoch, len(txs)))]
    def batch(phase, rows):
        rows = list(rows)
        records.append(tf.frame(tf.BEGIN_BATCH, struct.pack('<HHI', tf.PHASE_DATABASES[phase],
                                                           phase, len(rows))))
        for table, key, value, tx in rows:
            records.append(tf.put(tf.IDS[table], key, value, tx))
            counts[table] += 1
            values[table].append(len(value))
    def row(table, key, value, tx=tf.CHECKPOINT_ROW):
        return table, key, value, tx
    watermark_value = seq.to_bytes(8, 'little') + bcs_digest(model['cp_digest'])
    def watermark(variant):
        return [row('watermarks', variant.to_bytes(4, 'big'), watermark_value)]
    batch(0, [row('certified_checkpoints', seq.to_bytes(8, 'big'), model['certified']),
              row('checkpoint_by_digest', model['cp_digest'], model['certified'])])
    batch(1, watermark(0))
    batch(2, (r for i, t in enumerate(txs) for r in [
        row('effects', t['effects_digest'], t['effects'], i),
        row('transactions', t['digest'], t['trusted'], i)]))
    batch(3, [row('checkpoint_sequence_by_contents_digest', model['content_digest'], seq.to_bytes(8, 'little')),
              row('full_checkpoint_content_v2', seq.to_bytes(8, 'big'), model['full']),
              row('checkpoint_content', model['content_digest'], model['contents'])])
    batch(4, watermark(1))
    for tx, ref, value, overlay in model['demands']:
        records.append(tf.read(tx, object_key(ref), value, overlay))
    batch(5, (row('epoch_executed_transactions_to_checkpoint', t['digest'], seq.to_bytes(8, 'little'), i)
              for i, t in enumerate(txs)))
    batch(6, (row('executed_transactions_to_checkpoint', t['digest'],
                  epoch.to_bytes(8, 'little') + seq.to_bytes(8, 'little'), i)
              for i, t in enumerate(txs)))
    outputs = []
    for i, t in enumerate(txs):
        fx = t['fx']
        outputs += [row('effects', t['effects_digest'], t['effects'], i),
                    row('executed_effects', t['digest'], bcs_digest(t['effects_digest']), i),
                    row('transactions', t['digest'], t['trusted'], i),
                    row('executed_transaction_digests', epoch.to_bytes(8, 'big') + DIGEST_PREFIX + t['digest'], b'', i)]
        outputs += [row('object_per_epoch_marker_table_v2', key, value, i) for key, value in t['markers']]
        # Node chains deleted rows, then wrapped rows, then its ObjectID-sorted written map.
        for kind in ('deleted', 'wrapped'):
            outputs += [row('objects', object_key((oid, v)), b'\x00\x01' if kind == 'deleted' else b'\x00\x02', i)
                        for oid, v, tomb_kind in fx.tombstones if tomb_kind == kind]
        outputs += [row('objects', object_key(ref), model['objects'][ref], i) for ref in sorted(fx.outputs)]
        if t['events'] is not None:
            outputs.append(row('events_2', t['digest'], t['events'], i))
        if t['loaded']:
            loaded = uleb(len(t['loaded'])) + b''.join(oid + v.to_bytes(8, 'little') for oid, v in t['loaded'])
            outputs.append(row('unchanged_loaded_runtime_objects', t['digest'], loaded, i))
    outputs.append(row('highest_committed_checkpoint', b'', seq.to_bytes(8, 'little')))
    batch(7, outputs)
    batch(8, watermark(2))
    records.append(tf.frame(tf.END_CHECKPOINT))
    preload_records = [tf.put(tf.IDS['objects'], object_key(ref), value)
                       for ref, value in sorted(model['preload'].items(), key=lambda item: object_key(item[0]))]
    trace, preload = tf.encode(records), tf.encode(preload_records, True)
    tf.validate(trace, preload)
    return trace, preload, counts, values


def generate(source, profile_path, output):
    model = derive(source)
    trace, preload, counts, values = build(model)
    profile_raw = Path(profile_path).read_bytes()
    profile = json.loads(profile_raw)
    tf.require(tf.canonical(profile) == profile_raw and profile['selected_checkpoint'] == tf.SEQUENCE
               and profile['archive_sha256'] == tf.SOURCE_SHA256 and profile['scope'] == SCOPE,
               'wrong or noncanonical mainnet profile')
    selected = next(r for r in profile['rows'] if r['sequence'] == tf.SEQUENCE)
    tf.require(selected['put_counts'] == {t['name']: counts[t['name']] for t in tf.TABLES},
               'profile put counts differ from actual trace')
    tf.require(selected['write_value_bytes'] == sum(map(sum, values.values())), 'profile value bytes differ')
    tf.require(selected['store_reads'] == sum(not d[3] for d in model['demands'])
               and selected['overlay_reads'] == sum(d[3] for d in model['demands']), 'profile read counts differ')
    files = {'trace.bin': trace, 'preload.bin': preload, tf.SOURCE_NAME: model['compressed'],
             'profile.json': profile_raw}
    entries = tf.decode(trace)
    batches = []
    for index, entry in enumerate(entries):
        if entry.kind == tf.BEGIN_BATCH:
            members = entries[index + 1:index + 1 + entry.members]
            # CompleteBatch header + exact member key/value payload + conservative per-member framing.
            batches.append(dict(phase=entry.phase, db_id=entry.db, members=entry.members,
                                key_bytes=sum(len(e.key) for e in members),
                                value_bytes=sum(len(e.value) for e in members),
                                owner_encoded_bytes_upper_bound=96 + sum(32 + len(e.key) + len(e.value) for e in members)))
    source_hashes = {name: tf.sha((HERE / name).read_bytes())
                     for name in ('codec.py', 'generate.py', 'trace_format.py', 'schema.json')}
    schema = json.loads((HERE / 'schema.json').read_bytes())
    manifest = dict(schema_version=1, format_version=1, network='mainnet', scope=SCOPE,
                    source=dict(sequence=tf.SEQUENCE, epoch=tf.EPOCH,
                                url=f'https://checkpoints.mainnet.sui.io/{tf.SOURCE_NAME}',
                                sha256=tf.SOURCE_SHA256, compressed_bytes=len(model['compressed']),
                                public_archive_expiration_utc='2026-12-16T01:16:02Z', retained_source_required=True),
                    files={name: dict(bytes=len(data), sha256=tf.sha(data)) for name, data in sorted(files.items())},
                    tables=tf.TABLES, node_databases=[dict(id=0, name='perpetual'),
                        dict(id=1, name='checkpoints'), dict(id=2, name='epoch_1313')],
                    generator_sha256=source_hashes, schema_provenance=schema,
                    checkpoint_window=1, transactions=len(model['transactions']),
                    store_reads=selected['store_reads'], overlay_demands=selected['overlay_reads'],
                    put_members=sum(counts.values()), write_calls=len(batches), batches=batches,
                    preload=dict(objects=len(model['preload']), value_bytes=sum(map(len, model['preload'].values())),
                                 key_bytes=40 * len(model['preload'])),
                    unchanged_consensus_kind_counts={str(k): model['unchanged'].get(k, 0) for k in range(5)},
                    omitted_accumulator_effects=model['accumulators'],
                    coverage=dict(package_read_count=None, package_read_bytes=None,
                                  node_read_caches_modeled=False, repeated_reads_within_transaction=False,
                                  whole_node_trace=False, jsonrpc_indexes=False,
                                  accumulator_materialization=False, epoch_transition=False,
                                  tombstones_in_selected_checkpoint=0),
                    repetition=dict(domain='tidehunter-sui-mainnet-repeat-v1', hash='blake2b-512',
                                    encoding='domain || db_u16le || table_u16le || repeat_u64le || key_len_u32le || key || block_u32le',
                                    truncate_to_original_key_length=True, unchanged_values=True,
                                    unchanged_control_table_ids=[t['id'] for t in tf.TABLES if t['control']],
                                    collision_policy='fail_before_measurement'))
    files['manifest.json'] = tf.canonical(manifest)
    tf.require(sum(map(len, files.values())) < tf.MAX_FILE_BYTES, 'complete fixture exceeds 5 MB cap')
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        path = root / name
        tf.require(not path.is_symlink(), f'output cannot be a symlink: {name}')
        path.write_bytes(data)
    tf.admit(root, tf.sha(files['manifest.json']))
    return {name: dict(bytes=len(data), sha256=tf.sha(data)) for name, data in sorted(files.items())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=HERE / 'fixture' / tf.SOURCE_NAME)
    parser.add_argument('--profile', type=Path, default=HERE / 'fixture/profile.json')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(generate(args.source, args.profile, args.output), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
