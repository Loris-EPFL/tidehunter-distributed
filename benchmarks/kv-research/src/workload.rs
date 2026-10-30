use blake2::{Blake2b512, Digest};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::io::Read;
use tidehunter::minibytes::Bytes;

use crate::config::{KeyDistribution, OperationMix};

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

// Admitted values are at most 65,536 bytes and entropy is finite in [0, 1].
#[allow(clippy::cast_precision_loss)]
// Deliberate nearest-byte rounding of the nonnegative, bounded entropy fraction.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[must_use]
pub fn value_bytes(namespace: &[u8; 16], seed: u64, id: u64, size: usize, entropy: f64) -> Bytes {
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
    Bytes::from(value)
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
}

impl WorkloadGenerator {
    #[must_use]
    pub fn new(
        seed: u64,
        key_count: u64,
        mix: OperationMix,
        batch_size: usize,
        distribution: KeyDistribution,
    ) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            key_count,
            mix,
            batch_size,
            distribution,
        }
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
            } else {
                self.next_key()?
            });
        }
        Ok(WorkItem { operation, keys })
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
    use super::*;

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
        let mut a = WorkloadGenerator::new(4, 100, mix.clone(), 32, KeyDistribution::Uniform);
        let mut b = WorkloadGenerator::new(4, 100, mix, 32, KeyDistribution::Uniform);
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
        let mut generator = WorkloadGenerator::new(7, 8, mix, 2, KeyDistribution::Uniform);
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
        let mut sweep = WorkloadGenerator::new(4, 7, mix.clone(), 3, KeyDistribution::Uniform);
        let mut random = WorkloadGenerator::new(4, 7, mix, 3, KeyDistribution::Uniform);
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
