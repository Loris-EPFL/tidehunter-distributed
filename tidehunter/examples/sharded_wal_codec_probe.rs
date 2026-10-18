//! Isolated codec CPU probe: no WAL, network, persistence, or cloud resources.
//! Reports whole calls and per-record normalization separately; digest probes
//! retain every v2 binding byte and verify equivalence before timed loops.
use minibytes::Bytes;
use std::hint::black_box;
use std::time::Instant;
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::types::{
    BatchId, BatchVersion, PhysicalValueAddress, RecordKind, ValueRecordReference,
};
use tidehunter::key_shape::{KeyShape, KeyType};

fn prefix(reference: &ValueRecordReference, packed: bool) -> blake2b_simd::State {
    let mut state = blake2b_simd::Params::new().hash_length(32).to_state();
    let d = reference.descriptor;
    let kind = if d.kind == RecordKind::Put { 1 } else { 3 };
    if packed {
        let mut bytes = [0u8; 101];
        bytes[..27].copy_from_slice(b"tidehunter-batch-member-v2\0");
        bytes[27..43].copy_from_slice(&reference.batch_id.database);
        bytes[43..59].copy_from_slice(&reference.batch_id.operation);
        bytes[59..67].copy_from_slice(&reference.batch_version.epoch.to_be_bytes());
        bytes[67..75].copy_from_slice(&reference.batch_version.sequence.to_be_bytes());
        bytes[75..79].copy_from_slice(&d.operation_index.to_be_bytes());
        bytes[79..83].copy_from_slice(&d.offset.to_be_bytes());
        bytes[83..87].copy_from_slice(&d.len.to_be_bytes());
        bytes[87] = d.keyspace;
        bytes[88] = kind;
        bytes[89..93].copy_from_slice(&d.key_len.to_be_bytes());
        bytes[93..97].copy_from_slice(&d.value_offset.to_be_bytes());
        bytes[97..101].copy_from_slice(&d.value_len.to_be_bytes());
        state.update(&bytes);
    } else {
        state.update(b"tidehunter-batch-member-v2\0");
        state.update(&reference.batch_id.database);
        state.update(&reference.batch_id.operation);
        state.update(&reference.batch_version.epoch.to_be_bytes());
        state.update(&reference.batch_version.sequence.to_be_bytes());
        state.update(&d.operation_index.to_be_bytes());
        state.update(&d.offset.to_be_bytes());
        state.update(&d.len.to_be_bytes());
        state.update(&[d.keyspace]);
        state.update(&[kind]);
        state.update(&d.key_len.to_be_bytes());
        state.update(&d.value_offset.to_be_bytes());
        state.update(&d.value_len.to_be_bytes());
    }
    state
}
fn measure(
    stage: &str,
    value_bytes: usize,
    operations: usize,
    iterations: usize,
    round: usize,
    mut call: impl FnMut(),
) {
    for _ in 0..16 {
        call();
    }
    let started = Instant::now();
    for _ in 0..iterations {
        call();
    }
    let ns = started.elapsed().as_nanos();
    println!(
        "{stage},{value_bytes},{operations},{round},{iterations},{ns},{:.3},{:.3}",
        ns as f64 / iterations as f64,
        ns as f64 / iterations as f64 / operations as f64
    );
}
fn main() {
    let shape = KeyShape::new_single(32, 1, KeyType::uniform(1024));
    let keyspace = shape.iter_ks().next().unwrap().id();
    let limits = BatchLimits {
        max_operations: 128,
        max_encoded_bytes: 2 * 1024 * 1024,
    };
    let id = BatchId {
        database: [1; 16],
        operation: [2; 16],
    };
    let version = BatchVersion {
        epoch: 3,
        sequence: 4,
    };
    eprintln!("blake2b_parallel_degree={}", blake2b_simd::many::degree());
    println!("stage,value_bytes,batch_ops,round,iterations,total_ns,ns_per_call,ns_per_record");
    for value_bytes in [1024usize, 8192] {
        for operations in [1usize, 32] {
            let mutations: Vec<_> = (0..operations)
                .map(|n| Mutation::Put {
                    keyspace,
                    key: vec![n as u8; 32].into(),
                    value: vec![n as u8; value_bytes].into(),
                })
                .collect();
            let batch = CompleteBatch::new(&shape, id, version, mutations.clone(), limits).unwrap();
            let address = PhysicalValueAddress {
                partition: 0,
                segment_generation: 3,
                offset: 4096,
                len: batch.encoded().len() as u32 + 8,
            };
            let refs: Vec<_> = (0..operations)
                .map(|n| batch.record_reference(address, n).unwrap())
                .collect();
            let members: Vec<Bytes> = refs
                .iter()
                .map(|r| {
                    batch.encoded().slice(
                        r.descriptor.offset as usize
                            ..(r.descriptor.offset + r.descriptor.len) as usize,
                    )
                })
                .collect();
            let records: Vec<_> = refs.iter().copied().zip(members.iter().cloned()).collect();
            for (reference, member) in refs.iter().zip(&members) {
                for packed in [false, true] {
                    let mut state = prefix(reference, packed);
                    state.update(&member[36..]);
                    assert_eq!(state.finalize().as_bytes(), &reference.descriptor.digest.0);
                }
                assert_eq!(
                    CompleteBatch::decode_record(&shape, reference, member.clone()).unwrap(),
                    mutations[reference.descriptor.operation_index as usize]
                );
            }
            let iterations = (8 * 1024 * 1024 / (value_bytes * operations)).clamp(32, 8192);
            for round in 0..7 {
                measure(
                    "new_complete_batch",
                    value_bytes,
                    operations,
                    iterations,
                    round,
                    || {
                        black_box(
                            CompleteBatch::new(
                                black_box(&shape),
                                id,
                                version,
                                black_box(mutations.clone()),
                                limits,
                            )
                            .unwrap(),
                        );
                    },
                );
                measure(
                    "decode_owned_batch",
                    value_bytes,
                    operations,
                    iterations,
                    round,
                    || {
                        black_box(
                            CompleteBatch::decode_owned(
                                black_box(&shape),
                                black_box(batch.encoded().to_vec()),
                                limits,
                            )
                            .unwrap(),
                        );
                    },
                );
                measure(
                    "decode_scalar_records",
                    value_bytes,
                    operations,
                    iterations,
                    round,
                    || {
                        for (reference, member) in refs.iter().zip(&members) {
                            black_box(
                                CompleteBatch::decode_record(
                                    black_box(&shape),
                                    black_box(reference),
                                    black_box(member.clone()),
                                )
                                .unwrap(),
                            );
                        }
                    },
                );
                measure(
                    "decode_grouped_records",
                    value_bytes,
                    operations,
                    iterations,
                    round,
                    || {
                        black_box(
                            CompleteBatch::decode_records(black_box(&shape), black_box(&records))
                                .unwrap(),
                        );
                    },
                );
                for packed in [false, true] {
                    measure(
                        if packed {
                            "digest_packed_prefix"
                        } else {
                            "digest_fragmented_prefix"
                        },
                        value_bytes,
                        operations,
                        iterations,
                        round,
                        || {
                            for (reference, member) in refs.iter().zip(&members) {
                                let mut state = prefix(black_box(reference), packed);
                                state.update(black_box(&member[36..]));
                                black_box(state.finalize());
                            }
                        },
                    );
                }
                measure(
                    "digest_parallel",
                    value_bytes,
                    operations,
                    iterations,
                    round,
                    || {
                        let mut states: Vec<_> = refs
                            .iter()
                            .map(|reference| prefix(black_box(reference), true))
                            .collect();
                        blake2b_simd::many::update_many(
                            states
                                .iter_mut()
                                .zip(members.iter().map(|member| black_box(&member[36..]))),
                        );
                        for state in states {
                            black_box(state.finalize());
                        }
                    },
                );
            }
        }
    }
}
