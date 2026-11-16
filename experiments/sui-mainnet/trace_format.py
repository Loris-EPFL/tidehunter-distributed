"""Version-1 mainnet replay format. No engine or network dependency.

Put ordinal 65535 denotes a checkpoint-level row. Read origins: 0=store, 1=dirty.
All lengths include the eight-byte record header. Every node DB is a separate domain.
"""
from dataclasses import dataclass
import hashlib
import json
import pathlib
import struct

SCOPE = 'archive-derivable node KV subset'
SEQUENCE = 345683547
EPOCH = 1313
SOURCE_SHA256 = '20c4b1839e5fd44353bc58c0f5a9c458a6a07160ea30b617f5e2648b7302effa'
SOURCE_NAME = f'{SEQUENCE}.binpb.zst'
MAX_FILE_BYTES = 5_000_000
MAX_RECORDS = 100_000
HEADER = struct.Struct('<8sIIIIQQ24s')
RECORD = struct.Struct('<IBBH')
BEGIN_CHECKPOINT, BEGIN_BATCH, PUT, READ, END_CHECKPOINT = range(1, 6)
TRACE_MAGIC, PRELOAD_MAGIC = b'THMNTRC1', b'THMNPRE1'
CHECKPOINT_ROW = 65535
# Stable IDs, sorted by node table name. DB IDs: perpetual=0, checkpoints=1, epoch=2.
_TABLES = [
    ('certified_checkpoints', 1, 8, 'sequence_prefix_48'),
    ('checkpoint_by_digest', 1, 32, 'digest_uniform'),
    ('checkpoint_content', 1, 32, 'digest_uniform'),
    ('checkpoint_sequence_by_contents_digest', 1, 32, 'digest_uniform'),
    ('effects', 0, 32, 'digest_reduction_0_16'),
    ('epoch_executed_transactions_to_checkpoint', 2, 32, 'digest_reduction_0_16'),
    ('events_2', 0, 32, 'digest_uniform'),
    ('executed_effects', 0, 32, 'digest_reduction_0_16'),
    ('executed_transaction_digests', 0, 48, 'epoch_digest_prefix_140'),
    ('executed_transactions_to_checkpoint', 0, 32, 'digest_uniform'),
    ('full_checkpoint_content_v2', 1, 8, 'sequence_prefix_48'),
    ('highest_committed_checkpoint', 0, 0, 'singleton'),
    ('object_per_epoch_marker_table_v2', 0, 56, 'epoch_marker_prefix_76'),
    ('objects', 0, 40, 'object_fixed_40'),
    ('transactions', 0, 32, 'digest_reduction_0_16'),
    ('unchanged_loaded_runtime_objects', 0, 32, 'digest_uniform'),
    ('watermarks', 1, 4, 'control'),
]
TABLES = [dict(id=i, name=name, node_name='executed_transactions_to_checkpoint'
               if name.startswith('epoch_') else name, db_id=db, key_bytes=size,
               key_shape=shape, control=shape in ('singleton', 'control'))
          for i, (name, db, size, shape) in enumerate(_TABLES)]
IDS = {t['name']: t['id'] for t in TABLES}
# One call per phase, in the real fullnode catch-up order.
PHASE_DATABASES = (1, 1, 0, 1, 1, 2, 0, 0, 1)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
                       allow_nan=False) + '\n').encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def frame(kind, payload=b''):
    require(len(payload) + RECORD.size <= MAX_FILE_BYTES, 'record exceeds size cap')
    return RECORD.pack(RECORD.size + len(payload), kind, 0, 0) + payload


def put(table, key, value, tx=CHECKPOINT_ROW):
    return frame(PUT, struct.pack('<HHII', table, tx, len(key), len(value)) + key + value)


def read(tx, key, expected, overlay):
    return frame(READ, struct.pack('<IHBBII', tx, IDS['objects'], int(overlay), 0,
                                  len(key), len(expected)) + key + expected)


def encode(records, preload=False):
    body = b''.join(records)
    require(len(records) <= MAX_RECORDS, 'record count exceeds cap')
    data = HEADER.pack(PRELOAD_MAGIC if preload else TRACE_MAGIC, 1, HEADER.size,
                       0 if preload else 1, len(TABLES), len(records), len(body), bytes(24)) + body
    require(len(data) <= MAX_FILE_BYTES, 'file exceeds size cap')
    return data


@dataclass(frozen=True)
class Entry:
    kind: int
    table: int = -1
    tx: int = CHECKPOINT_ROW
    key: bytes = b''
    value: bytes = b''
    origin: int = 0
    db: int = -1
    phase: int = -1
    members: int = 0
    sequence: int = SEQUENCE
    epoch: int = EPOCH
    transactions: int = 0


def decode(data, preload=False):
    require(HEADER.size <= len(data) <= MAX_FILE_BYTES, 'invalid file size')
    magic, version, size, checkpoints, tables, count, payload, reserved = HEADER.unpack_from(data)
    require(magic == (PRELOAD_MAGIC if preload else TRACE_MAGIC), 'wrong file magic')
    require(version == 1 and size == HEADER.size and tables == len(TABLES), 'unknown format')
    require(checkpoints == (0 if preload else 1) and reserved == bytes(24), 'invalid header')
    require(count <= MAX_RECORDS and payload == len(data) - size, 'header length/count mismatch')
    entries = []
    offset = size
    while offset < len(data):
        require(len(data) - offset >= RECORD.size, 'truncated record header')
        length, kind, flags, pad = RECORD.unpack_from(data, offset)
        require(RECORD.size <= length <= len(data) - offset, 'invalid record length')
        require(flags == 0 and pad == 0, 'nonzero record reserved bits')
        body = data[offset + RECORD.size:offset + length]
        if kind == BEGIN_CHECKPOINT:
            require(len(body) == 20 and not preload, 'invalid checkpoint record')
            seq, epoch, txs = struct.unpack('<QQI', body)
            require(seq == SEQUENCE and epoch == EPOCH and 0 < txs < CHECKPOINT_ROW,
                    'wrong checkpoint or transaction count')
            entry = Entry(kind, sequence=seq, epoch=epoch, transactions=txs)
        elif kind == BEGIN_BATCH:
            require(len(body) == 8 and not preload, 'invalid batch record')
            db, phase, members = struct.unpack('<HHI', body)
            require(phase < len(PHASE_DATABASES) and db == PHASE_DATABASES[phase]
                    and members > 0, 'invalid batch phase/database')
            entry = Entry(kind, db=db, phase=phase, members=members)
        elif kind in (PUT, READ):
            if kind == PUT:
                require(len(body) >= 12, 'truncated put')
                table, tx, klen, vlen = struct.unpack_from('<HHII', body)
                start, origin = 12, 0
            else:
                require(len(body) >= 16 and not preload, 'truncated/unexpected read')
                tx, table, origin, rflags, klen, vlen = struct.unpack_from('<IHBBII', body)
                require(origin in (0, 1) and rflags == 0 and table == IDS['objects'],
                        'invalid read flags/table')
                start = 16
            require(table < len(TABLES) and klen == TABLES[table]['key_bytes'], 'invalid table/key')
            require(klen + vlen == len(body) - start, 'key/value length mismatch')
            entry = Entry(kind, table, tx, body[start:start + klen], body[start + klen:], origin)
        elif kind == END_CHECKPOINT:
            require(not body and not preload, 'invalid end checkpoint')
            entry = Entry(kind)
        else:
            raise ValueError(f'unknown record kind {kind}')
        entries.append(entry)
        offset += length
    require(len(entries) == count, 'record count mismatch')
    return entries


def validate(trace, preload):
    entries, initial = decode(trace), decode(preload, True)
    require(entries and entries[0].kind == BEGIN_CHECKPOINT and entries[-1].kind == END_CHECKPOINT,
            'missing checkpoint boundaries')
    txs = entries[0].transactions
    baseline = {}
    for entry in initial:
        require(entry.kind == PUT and entry.table == IDS['objects']
                and entry.tx == CHECKPOINT_ROW, 'invalid preload row')
        require(entry.key not in baseline, 'duplicate preload key')
        baseline[entry.key] = entry.value
    phases, batches, reads = [], [], []
    i = 1
    while i < len(entries) - 1:
        entry = entries[i]
        if entry.kind == READ:
            require(len(phases) == 5 and entry.tx < txs, 'read outside execution stage')
            reads.append(entry)
            i += 1
            continue
        require(entry.kind == BEGIN_BATCH and entry.phase == len(phases), 'batch order mismatch')
        phases.append(entry.phase)
        members = entries[i + 1:i + 1 + entry.members]
        require(len(members) == entry.members and all(e.kind == PUT for e in members),
                'batch member mismatch')
        require(all(TABLES[e.table]['db_id'] == entry.db for e in members), 'cross-database batch')
        require(all(e.tx == CHECKPOINT_ROW or e.tx < txs for e in members), 'invalid put tx ordinal')
        batches.append(members)
        i += 1 + entry.members
    require(phases == list(range(9)), 'missing/extra phase')
    require(all(reads[j].tx <= reads[j + 1].tx for j in range(len(reads) - 1)), 'read order mismatch')
    produced = {}
    for entry in batches[7]:
        if entry.table == IDS['objects']:
            require(entry.key not in produced, 'duplicate object output version')
            produced[entry.key] = (entry.tx, entry.value)
    require(not baseline.keys() & produced.keys(), 'preloaded an in-window output')
    used_baseline, seen_reads = set(), set()
    for entry in reads:
        require((entry.tx, entry.key) not in seen_reads, 'duplicate input within transaction')
        seen_reads.add((entry.tx, entry.key))
        if entry.origin:
            require(entry.key in produced, 'overlay read missing producer')
            producer, expected = produced[entry.key]
            require(producer < entry.tx, 'forward/cyclic overlay dependency')
        else:
            require(entry.key in baseline, 'store read missing preload')
            used_baseline.add(entry.key)
            expected = baseline[entry.key]
        require(entry.value == expected, 'read expected bytes differ from stored bytes')
    require(used_baseline == baseline.keys(), 'unused preload object')
    watermark = batches[7][-1]
    require(watermark.table == IDS['highest_committed_checkpoint'] and not watermark.key
            and watermark.value == SEQUENCE.to_bytes(8, 'little'), 'missing commit watermark')
    return entries, initial


def admit(directory, manifest_sha256):
    root = pathlib.Path(directory).resolve(strict=True)
    path = root / 'manifest.json'
    require(not path.is_symlink(), 'manifest must be regular')
    raw = path.read_bytes()
    require(len(raw) <= MAX_FILE_BYTES and sha(raw) == manifest_sha256, 'manifest hash mismatch')
    manifest = json.loads(raw)
    require(canonical(manifest) == raw and manifest.get('schema_version') == 1
            and manifest.get('network') == 'mainnet' and manifest.get('scope') == SCOPE,
            'noncanonical or wrong-network manifest')
    require(manifest.get('tables') == TABLES, 'table declarations differ from format')
    expected_names = {'trace.bin', 'preload.bin', 'profile.json', SOURCE_NAME}
    require(set(manifest.get('files', {})) == expected_names, 'unexpected fixture files')
    for name, expected in manifest['files'].items():
        item = root / name
        require(item.is_file() and not item.is_symlink(), f'not a regular fixture file: {name}')
        require(item.stat().st_size == expected['bytes'] <= MAX_FILE_BYTES,
                f'fixture size mismatch: {name}')
        require(sha(item.read_bytes()) == expected['sha256'], f'fixture hash mismatch: {name}')
    require(manifest['files'][SOURCE_NAME]['sha256'] == SOURCE_SHA256, 'wrong mainnet source')
    validate((root / 'trace.bin').read_bytes(), (root / 'preload.bin').read_bytes())
    return manifest
