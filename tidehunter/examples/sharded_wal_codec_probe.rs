//! Isolated codec CPU probe: no WAL, network, persistence, or cloud resources.
//! Reports whole codec calls and per-record normalization; integrity timings
//! exercise the production v3 CRC implementation without a duplicate probe hash.
use minibytes::Bytes;
use std::hint::black_box;
use std::time::Instant;
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::types::{BatchId, BatchVersion, PhysicalValueAddress};
use tidehunter::key_shape::{KeyShape, KeyType};

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
    eprintln!("member_integrity=canonical_crc32_v3");
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
            }
        }
    }
}
