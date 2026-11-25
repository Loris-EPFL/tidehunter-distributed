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
mod tests;
