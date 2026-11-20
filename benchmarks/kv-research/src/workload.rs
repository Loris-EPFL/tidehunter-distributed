use blake2::{Blake2b512, Digest};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tidehunter::minibytes::Bytes;

use crate::config::{KeyDistribution, OperationMix, PutKeyPolicy};

pub const KEY_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Put,
    Delete,
    MissingRead,
    GetMany,
    PutBatch,
}

impl OperationKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::MissingRead => "missing_read",
            Self::GetMany => "get_many",
            Self::PutBatch => "put_batch",
        }
    }
}
#[derive(Clone, Debug)]
pub struct WorkItem {
    pub operation: OperationKind,
    pub keys: Vec<u64>,
}

/// All key bits are derived from namespace and logical id, preserving a uniform
/// keyspace instead of concentrating keys in a shared-prefix partition.
#[must_use]
pub fn key_bytes(namespace: &[u8; 16], seed: u64, id: u64) -> Bytes {
    let mut hasher = Blake2b512::new();
    hasher.update(b"kv-research-key-v1");
    hasher.update(namespace);
    hasher.update(seed.to_le_bytes());
    hasher.update(id.to_le_bytes());
    let digest = hasher.finalize();
    Bytes::copy_from_slice(&digest[..KEY_BYTES])
}

#[must_use]
pub fn value_bytes(namespace: &[u8; 16], seed: u64, id: u64, size: usize, entropy: f64) -> Bytes {
    Bytes::from(value_vec(namespace, seed, id, size, entropy))
}

/// Index-fixture payload of one value domain: the stock value with `mark`
/// XORed into byte zero before the buffer is frozen, so each domain differs
/// from the build value (mark 0) without extra generation.
#[must_use]
pub fn marked_value_bytes(
    namespace: &[u8; 16],
    seed: u64,
    id: u64,
    size: usize,
    entropy: f64,
    mark: u8,
) -> Bytes {
    let mut value = value_vec(namespace, seed, id, size, entropy);
    if let Some(first) = value.first_mut() {
        *first ^= mark;
    }
    Bytes::from(value)
}

// Admitted values are at most 65,536 bytes and entropy is finite in [0, 1].
#[allow(clippy::cast_precision_loss)]
// Deliberate nearest-byte rounding of the nonnegative, bounded entropy fraction.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn value_vec(namespace: &[u8; 16], seed: u64, id: u64, size: usize, entropy: f64) -> Vec<u8> {
    let mut hasher = Blake2b512::new();
    hasher.update(b"kv-research-value-v1");
    hasher.update(namespace);
    hasher.update(seed.to_le_bytes());
    hasher.update(id.to_le_bytes());
    let digest = hasher.finalize();
    let seed_bytes: [u8; 32] = std::array::from_fn(|index| digest[index]);
    let mut rng = StdRng::from_seed(seed_bytes);
    let mut value = vec![0_u8; size];
    let identity_len = size.min(16);
    value[..identity_len].copy_from_slice(&digest[..identity_len]);
    let random_bytes = ((size - identity_len) as f64 * entropy).round() as usize;
    rng.fill_bytes(&mut value[identity_len..identity_len + random_bytes.min(size - identity_len)]);
    value
}

pub fn fresh_namespace() -> std::io::Result<[u8; 16]> {
    let mut namespace = [0_u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut namespace)?;
    Ok(namespace)
}

pub struct WorkloadGenerator {
    rng: StdRng,
    key_count: u64,
    mix: OperationMix,
    batch_size: usize,
    distribution: KeyDistribution,
    put_key_policy: PutKeyPolicy,
    next_put_index: Arc<AtomicU64>,
}

impl WorkloadGenerator {
    #[must_use]
    pub fn new(
        seed: u64,
        key_count: u64,
        mix: OperationMix,
        batch_size: usize,
        distribution: KeyDistribution,
        put_key_policy: PutKeyPolicy,
        next_put_index: Arc<AtomicU64>,
    ) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            key_count,
            mix,
            batch_size,
            distribution,
            put_key_policy,
            next_put_index,
        }
    }

    /// Reads stay in the prepared range. Each put uses a new logical id above that range.
    pub(crate) fn interference_item(
        &mut self,
        put_index: Option<u64>,
    ) -> crate::backend::BackendResult<WorkItem> {
        let (operation, key) = if let Some(index) = put_index {
            let key = self
                .key_count
                .checked_add(index)
                .ok_or_else(|| std::io::Error::other("interference put key range overflow"))?;
            (OperationKind::Put, key)
        } else {
            (OperationKind::Read, self.next_key()?)
        };
        Ok(WorkItem {
            operation,
            keys: vec![key],
        })
    }

    /// Advance the normal RNG while visiting logical keys in cursor order.
    /// The caller validates a read-only mix; measurement sweeps also bound calls to avoid wrap.
    pub(crate) fn next_sweep_item(
        &mut self,
        cursor: &mut u64,
    ) -> crate::backend::BackendResult<WorkItem> {
        let mut item = self.next_item()?;
        for key in &mut item.keys {
            *key = *cursor;
            *cursor = if *cursor == self.key_count - 1 {
                0
            } else {
                *cursor + 1
            };
        }
        Ok(item)
    }
    /// With K>1 clients, reservations follow thread interleaving: coverage and
    /// uniqueness are exact, but per-operation key assignment is not deterministic.
    /// K=1 uses next_sweep_item and retains its fully deterministic sequence.
    ///
    /// Reserve a disjoint part of one phase's sweep. Relaxed only allocates cursor
    /// positions; it publishes no values or oracle state to other client loops.
    pub(crate) fn next_shared_sweep_item(
        &mut self,
        cursor: &AtomicU64,
    ) -> crate::backend::BackendResult<WorkItem> {
        let mut item = self.next_item()?;
        let first = cursor.fetch_add(item.keys.len() as u64, Ordering::Relaxed);
        for (offset, key) in item.keys.iter_mut().enumerate() {
            *key = (first + offset as u64) % self.key_count;
        }
        Ok(item)
    }

    pub fn next_item(&mut self) -> crate::backend::BackendResult<WorkItem> {
        let choice = self.rng.gen_range(0..self.mix.total_weight());
        let weights = [
            self.mix.read,
            self.mix.put,
            self.mix.delete,
            self.mix.missing_read,
            self.mix.get_many,
            self.mix.put_batch,
        ];
        let mut edge = choice;
        let index = weights
            .iter()
            .position(|weight| {
                if edge < u64::from(*weight) {
                    true
                } else {
                    edge -= u64::from(*weight);
                    false
                }
            })
            .unwrap_or(0);
        let operation = [
            OperationKind::Read,
            OperationKind::Put,
            OperationKind::Delete,
            OperationKind::MissingRead,
            OperationKind::GetMany,
            OperationKind::PutBatch,
        ][index];
        let count = if matches!(operation, OperationKind::GetMany | OperationKind::PutBatch) {
            self.batch_size
        } else {
            1
        };
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            keys.push(if operation == OperationKind::MissingRead {
                self.key_count + self.rng.gen_range(0..self.key_count.max(1))
            } else if self.put_key_policy == PutKeyPolicy::New
                && matches!(operation, OperationKind::Put | OperationKind::PutBatch)
            {
                self.next_put_key()?
            } else {
                self.next_key()?
            });
        }
        Ok(WorkItem { operation, keys })
    }

    fn next_put_key(&self) -> crate::backend::BackendResult<u64> {
        // Relaxed reserves unique ids only; it does not publish any other shared state.
        // With K>1 clients, ID assignment follows thread interleaving; uniqueness
        // and coverage remain exact. K=1 retains its fully deterministic sequence.
        let base = self.key_count;
        let index = self
            .next_put_index
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |index| {
                base.checked_add(index)?;
                index.checked_add(1)
            })
            .map_err(|_| std::io::Error::other("new put key range overflow"))?;
        base.checked_add(index)
            .ok_or_else(|| std::io::Error::other("new put key range overflow").into())
    }

    fn next_key(&mut self) -> crate::backend::BackendResult<u64> {
        match &self.distribution {
            KeyDistribution::Uniform => Ok(self.rng.gen_range(0..self.key_count)),
            KeyDistribution::Zipf { exponent } => {
                zipf_rejection_inversion(&mut self.rng, self.key_count, *exponent)
            }
        }
    }
}

/// Rejection-inversion sampler for discrete Zipf ranks (1..=n), O(1) expected
/// work and O(1) memory independent of dataset cardinality.
// Preserve the conventional rejection-inversion equation names from the reference algorithm.
#[allow(clippy::many_single_char_names)]
// Cardinality is admitted below 10 million, exactly representable in f64.
#[allow(clippy::cast_precision_loss)]
// The floored rank is explicitly clamped to [1, n] before integer conversion.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn zipf_rejection_inversion(
    rng: &mut StdRng,
    n: u64,
    exponent: f64,
) -> crate::backend::BackendResult<u64> {
    if n <= 1 {
        return Ok(0);
    }
    let integral = |x: f64| {
        if (1.0 - exponent).abs() < f64::EPSILON {
            x.ln()
        } else {
            x.powf(1.0 - exponent) / (1.0 - exponent)
        }
    };
    let inverse = |y: f64| {
        if (1.0 - exponent).abs() < f64::EPSILON {
            y.exp()
        } else {
            (y * (1.0 - exponent))
                .max(f64::MIN_POSITIVE)
                .powf(1.0 / (1.0 - exponent))
        }
    };
    let h = |x: f64| x.powf(-exponent);
    let lower = integral(1.5) - 1.0;
    let upper = integral(n as f64 + 0.5);
    let squeeze = 2.0 - inverse(integral(2.5) - h(2.0));
    for _ in 0..1024 {
        let u = lower + rng.gen_range(0.0..1.0) * (upper - lower);
        let x = inverse(u);
        let k = (x + 0.5).floor().clamp(1.0, n as f64) as u64;
        let accept = k as f64 - x <= squeeze || u >= integral(k as f64 + 0.5) - h(k as f64);
        if accept {
            return Ok(k - 1);
        }
    }
    Err(std::io::Error::other("Zipf sampler exceeded its bounded rejection limit").into())
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_sweep_reserves_exact_coverage_across_clients_and_wraps() {
        let cursor = AtomicU64::new(0);
        let mut clients = (0..3)
            .map(|shard| {
                WorkloadGenerator::new(
                    shard,
                    17,
                    OperationMix {
                        read: 1,
                        get_many: 1,
                        ..OperationMix::default()
                    },
                    3,
                    KeyDistribution::Uniform,
                    PutKeyPolicy::Overwrite,
                    Arc::new(AtomicU64::new(0)),
                )
            })
            .collect::<Vec<_>>();
        let mut visited = Vec::new();
        for index in 0..31 {
            let item = clients[index % 3].next_shared_sweep_item(&cursor).unwrap();
            visited.extend(item.keys);
        }
        assert_eq!(cursor.load(Ordering::Relaxed), visited.len() as u64);
        for (index, key) in visited.into_iter().enumerate() {
            assert_eq!(key, index as u64 % 17);
        }
    }

    use super::*;

    #[test]
    fn new_put_keys_share_one_exact_range_across_threads_and_phases() {
        for distribution in [
            KeyDistribution::Uniform,
            KeyDistribution::Zipf { exponent: 1.2 },
        ] {
            let counter = Arc::new(AtomicU64::new(0));
            let mut keys = Vec::new();
            for _ in 0..2 {
                let barrier = std::sync::Barrier::new(4);
                std::thread::scope(|scope| {
                    let mut workers = Vec::new();
                    for worker in 0..4 {
                        let mut generator = WorkloadGenerator::new(
                            worker,
                            100,
                            OperationMix {
                                put: u32::from(worker % 2 == 0),
                                put_batch: u32::from(worker % 2 != 0),
                                ..OperationMix::default()
                            },
                            4,
                            distribution.clone(),
                            PutKeyPolicy::New,
                            Arc::clone(&counter),
                        );
                        let barrier = &barrier;
                        workers.push(scope.spawn(move || {
                            barrier.wait();
                            (0..64)
                                .flat_map(|_| generator.next_item().unwrap().keys)
                                .collect::<Vec<_>>()
                        }));
                    }
                    for worker in workers {
                        keys.extend(worker.join().unwrap());
                    }
                });
            }
            keys.sort_unstable();
            assert_eq!(keys, (100..1380).collect::<Vec<_>>());
        }
    }

    #[test]
    fn new_put_key_overflow_is_an_error_and_never_wraps() {
        for batch in [false, true] {
            let mut generator = WorkloadGenerator::new(
                7,
                u64::MAX - 1,
                OperationMix {
                    put: u32::from(!batch),
                    put_batch: u32::from(batch),
                    ..OperationMix::default()
                },
                2,
                KeyDistribution::Uniform,
                PutKeyPolicy::New,
                Arc::new(AtomicU64::new(0)),
            );
            if batch {
                assert_eq!(
                    generator.next_item().unwrap().keys,
                    [u64::MAX - 1, u64::MAX]
                );
            } else {
                assert_eq!(generator.next_item().unwrap().keys, [u64::MAX - 1]);
                assert_eq!(generator.next_item().unwrap().keys, [u64::MAX]);
            }
            for _ in 0..2 {
                assert_eq!(
                    generator.next_item().unwrap_err().to_string(),
                    "new put key range overflow"
                );
            }
        }
    }

    #[test]
    fn overwrite_preserves_recorded_seeded_operation_and_key_sequence() {
        // Recorded from the old selection path: rand 0.8 StdRng, seed 7, six equal
        // weights, 100 prepared keys and three members per grouped operation.
        let expected = [
            (OperationKind::Read, vec![30]),
            (OperationKind::MissingRead, vec![127]),
            (OperationKind::PutBatch, vec![17, 25, 12]),
            (OperationKind::GetMany, vec![2, 8, 37]),
            (OperationKind::PutBatch, vec![56, 50, 64]),
            (OperationKind::Delete, vec![78]),
            (OperationKind::Put, vec![92]),
            (OperationKind::Delete, vec![46]),
            (OperationKind::PutBatch, vec![56, 1, 67]),
            (OperationKind::MissingRead, vec![128]),
            (OperationKind::Put, vec![66]),
            (OperationKind::Put, vec![40]),
        ];
        let mut generator = WorkloadGenerator::new(
            7,
            100,
            OperationMix {
                read: 1,
                put: 1,
                delete: 1,
                missing_read: 1,
                get_many: 1,
                put_batch: 1,
            },
            3,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        for (operation, keys) in expected {
            let item = generator.next_item().unwrap();
            assert_eq!(item.operation, operation);
            assert_eq!(item.keys, keys);
        }
    }

    #[test]
    fn interference_keys_are_disjoint_and_put_keys_are_new() {
        for distribution in [
            KeyDistribution::Uniform,
            KeyDistribution::Zipf { exponent: 1.2 },
        ] {
            let mut generator = WorkloadGenerator::new(
                7,
                8,
                OperationMix {
                    read: 1,
                    ..OperationMix::default()
                },
                2,
                distribution,
                PutKeyPolicy::Overwrite,
                Arc::new(AtomicU64::new(0)),
            );
            for index in 0..1000 {
                let read = generator.interference_item(None).unwrap();
                let put = generator.interference_item(Some(index)).unwrap();
                assert_eq!(read.operation, OperationKind::Read);
                assert!(read.keys[0] < 8);
                assert_eq!(put.operation, OperationKind::Put);
                assert_eq!(put.keys, [8 + index]);
                assert_ne!(
                    key_bytes(&[0; 16], 7, read.keys[0]),
                    key_bytes(&[0; 16], 7, put.keys[0])
                );
            }
            assert!(generator.interference_item(Some(u64::MAX)).is_err());
        }
    }

    #[test]
    fn deterministic_data_is_namespaced_and_uniform_key_width() {
        let ns = [9; 16];
        assert_eq!(key_bytes(&ns, 7, 42), key_bytes(&ns, 7, 42));
        assert_ne!(key_bytes(&ns, 7, 42), key_bytes(&ns, 7, 43));
        assert_ne!(key_bytes(&ns, 7, 42), key_bytes(&[8; 16], 7, 42));
        assert_eq!(key_bytes(&ns, 7, 42).len(), KEY_BYTES);
    }

    #[test]
    fn generated_values_match_entropy_contract() {
        let ns = [1; 16];
        let compressible = value_bytes(&ns, 3, 4, 256, 0.0);
        assert!(compressible[16..].iter().all(|byte| *byte == 0));
        assert_ne!(compressible, value_bytes(&ns, 3, 5, 256, 0.0));
        let a = value_bytes(&ns, 3, 4, 256, 1.0);
        assert_eq!(a, value_bytes(&ns, 3, 4, 256, 1.0));
        assert_ne!(a, value_bytes(&ns, 3, 5, 256, 1.0));
    }

    #[test]
    fn zipf_sampler_is_bounded_and_skewed() {
        let mut rng = StdRng::seed_from_u64(10);
        let mut counts = [0_u64; 8];
        for _ in 0..20_000 {
            counts
                [usize::try_from(zipf_rejection_inversion(&mut rng, 8, 1.0).unwrap()).unwrap()] +=
                1;
        }
        assert!(counts.iter().all(|count| *count > 0));
        assert!(counts[0] > counts[7] * 2);
    }

    #[test]
    fn mix_is_reproducible_and_batches_keep_ordered_key_vector() {
        let mix = OperationMix {
            get_many: 1,
            ..OperationMix::default()
        };
        let mut a = WorkloadGenerator::new(
            4,
            100,
            mix.clone(),
            32,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        let mut b = WorkloadGenerator::new(
            4,
            100,
            mix,
            32,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        let left = a.next_item().unwrap();
        let right = b.next_item().unwrap();
        assert_eq!(left.operation, OperationKind::GetMany);
        assert_eq!(left.keys.len(), 32);
        assert_eq!(left.keys, right.keys);
    }

    #[test]
    fn bounded_measurement_sweep_has_distinct_full_batch_members() {
        let mix = OperationMix {
            get_many: 1,
            ..OperationMix::default()
        };
        let mut generator = WorkloadGenerator::new(
            7,
            8,
            mix,
            2,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        let mut cursor = 0;
        let keys: Vec<_> = (0..4)
            .flat_map(|_| generator.next_sweep_item(&mut cursor).unwrap().keys)
            .collect();
        assert_eq!(keys, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn warmup_sweep_covers_tail_keys_without_changing_measurement_rng() {
        let mix = OperationMix {
            get_many: 1,
            ..OperationMix::default()
        };
        let mut sweep = WorkloadGenerator::new(
            4,
            7,
            mix.clone(),
            3,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        let mut random = WorkloadGenerator::new(
            4,
            7,
            mix,
            3,
            KeyDistribution::Uniform,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        let mut cursor = 0;
        let mut keys = Vec::new();
        for _ in 0..3 {
            keys.extend(sweep.next_sweep_item(&mut cursor).unwrap().keys);
            random.next_item().unwrap();
        }
        assert_eq!(keys, vec![0, 1, 2, 3, 4, 5, 6, 0, 1]);
        assert_eq!(
            sweep.next_item().unwrap().keys,
            random.next_item().unwrap().keys
        );
    }
}
