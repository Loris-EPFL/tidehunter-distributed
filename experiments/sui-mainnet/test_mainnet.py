"""Run with the project Python -B: unittest discover -s experiments/sui-mainnet.

All workload bytes come from the one retained public mainnet checkpoint. Malformed
cases mutate it. Tombstone/marker encodings are serializer unit cases, not workloads.
"""
import collections
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import unittest

import generate as g
from codec import BCS, bcs_payload, one, proto
import trace_format as tf

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / 'fixture'


def records(data):
    result, offset = [], tf.HEADER.size
    while offset < len(data):
        length = struct.unpack_from('<I', data, offset)[0]
        result.append(data[offset:offset + length])
        offset += length
    return result


def changed(record, offset, data):
    copy = bytearray(record)
    copy[offset:offset + len(data)] = data
    return bytes(copy)


class MainnetFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.trace = (FIXTURE / 'trace.bin').read_bytes()
        cls.preload = (FIXTURE / 'preload.bin').read_bytes()
        cls.manifest_raw = (FIXTURE / 'manifest.json').read_bytes()
        cls.manifest = tf.admit(FIXTURE, tf.sha(cls.manifest_raw))
        cls.profile = json.loads((FIXTURE / 'profile.json').read_bytes())
        cls.entries, cls.initial = tf.validate(cls.trace, cls.preload)
        cls.model = g.derive(FIXTURE / tf.SOURCE_NAME)
        cls.raw_records = records(cls.trace)

    def test_golden_determinism(self):
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / 'a', Path(tmp) / 'b'
            identities = g.generate(FIXTURE / tf.SOURCE_NAME, FIXTURE / 'profile.json', first)
            self.assertEqual(identities, g.generate(FIXTURE / tf.SOURCE_NAME, FIXTURE / 'profile.json', second))
            for name in identities:
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes(), name)
                self.assertEqual((first / name).read_bytes(), (FIXTURE / name).read_bytes(), name)
        self.assertLess(sum(p.stat().st_size for p in FIXTURE.iterdir()), 5_000_000)
        self.assertTrue(self.manifest['source']['retained_source_required'])
        self.assertEqual(self.manifest['scope'], tf.SCOPE)

    def test_representative_selection_is_median_medoid(self):
        rows = self.profile['rows']
        self.assertEqual(len(rows), 100)
        metrics = self.profile['selection_metrics']
        def value(row, metric):
            return row['put_counts'][metric[6:]] if metric.startswith('table:') else row[metric]
        scores = []
        for row in rows:
            distance = sum(abs(value(row, m) - self.profile['medians'][m])
                           / self.profile['iqr_scales'][m] for m in metrics) / len(metrics)
            self.assertEqual(distance, row['distance'])
            scores.append((distance, row['sequence']))
        self.assertEqual(min(scores)[1], tf.SEQUENCE)
        self.assertEqual(self.profile['reference_summary']['store_read_value_quantiles'], [343, 745, 2511])

    def test_profile_and_actual_table_accounting(self):
        puts = [entry for entry in self.entries if entry.kind == tf.PUT]
        reads = [entry for entry in self.entries if entry.kind == tf.READ]
        batches = [entry for entry in self.entries if entry.kind == tf.BEGIN_BATCH]
        self.assertEqual(len(puts), 707)
        self.assertEqual(sum(len(entry.value) for entry in puts), 644573)
        self.assertEqual(sum(not entry.origin for entry in reads), 507)
        self.assertEqual(sum(entry.origin for entry in reads), 116)
        self.assertEqual(len(self.initial), 376)
        self.assertEqual(sum(len(entry.value) for entry in self.initial), 191216)
        self.assertEqual([entry.members for entry in batches], [2, 1, 90, 3, 1, 45, 45, 519, 1])
        self.assertEqual(self.manifest['unchanged_consensus_kind_counts'],
                         {'0': 140, '1': 0, '2': 0, '3': 0, '4': 0})
        self.assertEqual(self.manifest['omitted_accumulator_effects'], 29)
        counts = collections.Counter(tf.TABLES[entry.table]['name'] for entry in puts)
        self.assertEqual(counts['transactions'], 90)
        self.assertEqual(counts['effects'], 90)
        self.assertEqual(counts['objects'], 277)
        self.assertEqual(counts['object_per_epoch_marker_table_v2'], 0)
        self.assertEqual((len(self.trace), len(self.preload)), (1017214, 213840))

    def test_reject_truncated_headers_and_records(self):
        for end in (0, 1, tf.HEADER.size - 1, len(self.trace) - 1):
            with self.subTest(end=end), self.assertRaises(ValueError):
                tf.decode(self.trace[:end])
        bad = bytearray(self.trace)
        struct.pack_into('<I', bad, tf.HEADER.size, 0xFFFFFFFF)
        with self.assertRaisesRegex(ValueError, 'record length'):
            tf.decode(bad)

    def test_reject_unknown_format_tags_flags_and_lengths(self):
        for offset, value in [(8, b'\x02'), (40, b'\x01'),
                              (tf.HEADER.size + 4, b'\xff'), (tf.HEADER.size + 5, b'\x01')]:
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                tf.decode(changed(self.trace, offset, value))
        index = next(i for i, record in enumerate(self.raw_records) if record[4] == tf.READ)
        bad = self.raw_records.copy()
        bad[index] = changed(bad[index], 16, struct.pack('<I', 0xFFFFFFFF))
        with self.assertRaisesRegex(ValueError, 'invalid table/key'):
            tf.decode(tf.encode(bad))

    def test_reject_cross_database_and_missing_batch(self):
        index = next(i for i, record in enumerate(self.raw_records)
                     if record[4] == tf.PUT and struct.unpack_from('<H', record, 8)[0] == tf.IDS['effects'])
        bad = self.raw_records.copy()
        bad[index] = changed(bad[index], 8, struct.pack('<H', tf.IDS['checkpoint_content']))
        with self.assertRaisesRegex(ValueError, 'cross-database batch'):
            tf.validate(tf.encode(bad), self.preload)
        with self.assertRaises(ValueError):
            tf.validate(tf.encode(self.raw_records[:-2] + self.raw_records[-1:]), self.preload)

    def test_reject_duplicate_or_missing_preload(self):
        initial = records(self.preload)
        with self.assertRaisesRegex(ValueError, 'duplicate preload'):
            tf.validate(self.trace, tf.encode(initial + initial[:1], True))
        with self.assertRaisesRegex(ValueError, 'missing preload'):
            tf.validate(self.trace, tf.encode(initial[1:], True))

    def test_reject_wrong_read_bytes_and_overlay_origin(self):
        index = next(i for i, record in enumerate(self.raw_records) if record[4] == tf.READ)
        bad = self.raw_records.copy()
        bad[index] = changed(bad[index], len(bad[index]) - 1, bytes([bad[index][-1] ^ 1]))
        with self.assertRaisesRegex(ValueError, 'expected bytes differ'):
            tf.validate(tf.encode(bad), self.preload)
        bad = self.raw_records.copy()
        bad[index] = changed(bad[index], 14, b'\x01')
        with self.assertRaisesRegex(ValueError, 'overlay read missing producer'):
            tf.validate(tf.encode(bad), self.preload)

    def test_reject_forward_dependency_and_bad_tx_ordinal(self):
        for tx in (0, 65535):
            index = next(i for i, record in enumerate(self.raw_records)
                         if record[4] == tf.READ and record[14] == 1)
            bad = self.raw_records.copy()
            bad[index] = changed(bad[index], 8, struct.pack('<I', tx))
            with self.subTest(tx=tx), self.assertRaises(ValueError):
                tf.validate(tf.encode(bad), self.preload)

    def test_reject_wrong_network_hash_and_mutated_file(self):
        with self.assertRaisesRegex(ValueError, 'manifest hash mismatch'):
            tf.admit(FIXTURE, '0' * 64)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for source in FIXTURE.iterdir():
                shutil.copyfile(source, root / source.name)
            manifest = dict(self.manifest)
            manifest['network'] = 'testnet'
            raw = tf.canonical(manifest)
            (root / 'manifest.json').write_bytes(raw)
            with self.assertRaisesRegex(ValueError, 'wrong-network'):
                tf.admit(root, tf.sha(raw))
            (root / 'manifest.json').write_bytes(self.manifest_raw)
            (root / 'trace.bin').write_bytes(changed(self.trace, len(self.trace) - 1, b'\x01'))
            with self.assertRaisesRegex(ValueError, 'fixture hash mismatch'):
                tf.admit(root, tf.sha(self.manifest_raw))

    def test_reject_source_hash_and_unknown_bcs_variant(self):
        with tempfile.TemporaryDirectory() as tmp:
            wrong = Path(tmp) / tf.SOURCE_NAME
            wrong.write_bytes(changed((FIXTURE / tf.SOURCE_NAME).read_bytes(), 0, b'\x00'))
            with self.assertRaisesRegex(ValueError, 'compressed mainnet source'):
                g.derive(wrong)
        with self.assertRaisesRegex(ValueError, 'effects V2'):
            g.effects(b'\x02' + self.model['transactions'][0]['effects'][1:], tf.EPOCH)
        with self.assertRaisesRegex(ValueError, 'noncanonical'):
            BCS(b'\x80\x00').tag()
        with self.assertRaises(ValueError):
            BCS(b'\x05').owner()

    def test_independent_rust_serializer_cross_checks(self):
        _, cp = g.decompress(FIXTURE / tf.SOURCE_NAME)
        sig = proto(one(cp, 4))
        version, _, groups, _ = g.checkpoint_contents(self.model['contents'])
        first = self.model['references'][0]
        txs = self.model['transactions']
        request = dict(objects=self.model['references'], contents_version=version,
                       transactions=[dict(data=list(t['raw']), effects=list(t['effects']),
                                          signatures=list(t['signatures']), signature_group=list(groups[i]))
                                     for i, t in enumerate(txs)],
                       epoch=tf.EPOCH, sequence=tf.SEQUENCE, summary=list(self.model['summary']),
                       signature=list(one(sig, 2)), bitmap=list(one(sig, 3)),
                       transaction_digest=list(txs[0]['digest']), effects_digest=list(txs[0]['effects_digest']),
                       object_id=first['key'], object_version=first['version'], start_version=1)
        env = os.environ.copy()
        env.setdefault('CARGO_TARGET_DIR', '/tmp/tidehunter-mainnet-replay-s1')
        env['CARGO_BUILD_JOBS'] = '4'
        result = subprocess.run(['cargo', 'run', '--quiet', '--locked', '--manifest-path',
                                 str(HERE / 'serializer-reference/Cargo.toml')],
                                input=json.dumps(request), text=True, capture_output=True, env=env,
                                timeout=180)
        self.assertEqual(result.returncode, 0, result.stderr)
        reference = json.loads(result.stdout)
        for source, actual in zip(self.model['references'], reference['objects'], strict=True):
            self.assertEqual(bytes(actual['value']), bytes(source['value']))
            self.assertEqual(bytes(actual['key']), bytes(source['key']) + source['version'].to_bytes(8, 'big'))
        self.assertEqual([bytes(v) for v in reference['trusted_transactions']], [t['trusted'] for t in txs])
        self.assertEqual(bytes(reference['full_contents']), self.model['full'])
        self.assertEqual(bytes(reference['certified_checkpoint']), self.model['certified'])
        self.assertEqual(bytes(reference['digest_key_before_prefix_removal']), g.DIGEST_PREFIX + txs[0]['digest'])
        self.assertEqual(bytes(reference['epoch_digest_key']), tf.EPOCH.to_bytes(8, 'big') + g.DIGEST_PREFIX + txs[0]['digest'])
        self.assertEqual(bytes(reference['effects_digest_value']), g.bcs_digest(txs[0]['effects_digest']))
        self.assertEqual(bytes(reference['sequence_key']), tf.SEQUENCE.to_bytes(8, 'big'))
        self.assertEqual((reference['unit_key'], reference['unit_value']), ([], []))
        self.assertEqual(bytes(reference['fastpath_marker_key']),
                         g.marker_key(tf.EPOCH, bytes(first['key']), first['version'], None))
        self.assertEqual(bytes(reference['consensus_marker_key']),
                         g.marker_key(tf.EPOCH, bytes(first['key']), first['version'], b'\x02' + (1).to_bytes(8, 'little')))
        for name, expected in [('deleted', b'\x00\x01'), ('wrapped', b'\x00\x02'),
                               ('received', b'\x00'), ('fastpath_ended', b'\x01'),
                               ('consensus_ended', b'\x02' + g.bcs_digest(txs[0]['digest']))]:
            self.assertEqual(bytes(reference[name]), expected)


if __name__ == '__main__':
    unittest.main()
