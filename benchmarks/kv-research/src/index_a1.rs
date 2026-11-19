//! Roadmap Phase 6 A1 index-fixture hooks shared by the runner and the
//! Tidehunter adapter (`backend.options.index_fixture`, Linux `test-utils`
//! leased fixture only; also the Q1 staged index runner). Stock workloads
//! never construct these types.
//!
//! - `build` writes logical IDs `0..N` exactly once with the stock values;
//! - `overwrite` rewrites uniform IDs below `N` with the stock value whose
//!   byte zero carries the run's [`ValueDomain`] mark, and marks each
//!   successful ID in an `N`-bit touched bitmap, so the final row set is
//!   exactly checkable.

use crate::backend::BackendResult;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexFixtureStage {
    Build,
    Overwrite,
}

impl IndexFixtureStage {
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "build" => Some(Self::Build),
            "overwrite" => Some(Self::Overwrite),
            _ => None,
        }
    }
}

/// One bit per logical dataset ID (100M IDs: 12.5 MiB).
pub struct TouchedBitmap {
    keys: u64,
    words: Box<[AtomicU64]>,
}

impl TouchedBitmap {
    pub fn new(keys: u64) -> BackendResult<Self> {
        let words = usize::try_from(keys.div_ceil(64))?;
        Ok(Self {
            keys,
            words: (0..words).map(|_| AtomicU64::new(0)).collect(),
        })
    }

    #[must_use]
    pub fn keys(&self) -> u64 {
        self.keys
    }

    fn word(&self, id: u64) -> BackendResult<(&AtomicU64, u64)> {
        if id >= self.keys {
            return Err(format!("touched ID {id} is outside the {} dataset IDs", self.keys).into());
        }
        let index = usize::try_from(id / 64)?;
        Ok((&self.words[index], 1 << (id % 64)))
    }

    /// Relaxed: workers only set bits; readers read after the phase drained,
    /// which joins every worker (happens-before through the completion path).
    pub fn set(&self, id: u64) -> BackendResult<()> {
        let (word, bit) = self.word(id)?;
        word.fetch_or(bit, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_set(&self, id: u64) -> BackendResult<bool> {
        let (word, bit) = self.word(id)?;
        Ok(word.load(Ordering::Relaxed) & bit != 0)
    }

    #[must_use]
    pub fn count(&self) -> u64 {
        self.words
            .iter()
            .map(|word| u64::from(word.load(Ordering::Relaxed).count_ones()))
            .sum()
    }

    /// Lowest set ID: the first cold read.
    #[must_use]
    pub fn first(&self) -> Option<u64> {
        self.words.iter().enumerate().find_map(|(index, word)| {
            let bits = word.load(Ordering::Relaxed);
            (bits != 0).then(|| index as u64 * 64 + u64::from(bits.trailing_zeros()))
        })
    }

    /// Little-endian words.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.words
            .iter()
            .flat_map(|word| word.load(Ordering::Relaxed).to_le_bytes())
            .collect()
    }

    pub fn from_bytes(keys: u64, bytes: &[u8]) -> BackendResult<Self> {
        let bitmap = Self::new(keys)?;
        if bytes.len() != bitmap.words.len() * 8 {
            return Err("touched bitmap length does not match the dataset".into());
        }
        for (word, chunk) in bitmap.words.iter().zip(bytes.chunks_exact(8)) {
            let mut le = [0; 8];
            le.copy_from_slice(chunk);
            word.store(u64::from_le_bytes(le), Ordering::Relaxed);
        }
        let tail = keys % 64;
        if tail != 0
            && bitmap
                .words
                .last()
                .is_some_and(|word| word.load(Ordering::Relaxed) >> tail != 0)
        {
            return Err("touched bitmap sets IDs outside the dataset".into());
        }
        Ok(bitmap)
    }

    /// Set every bit of `other` (same dataset size) in `self`.
    pub fn merge(&self, other: &Self) -> BackendResult<()> {
        if other.keys != self.keys {
            return Err("bitmaps of different datasets cannot merge".into());
        }
        for (word, bits) in self.words.iter().zip(other.words.iter()) {
            // Relaxed: both bitmaps are read only after their phases drained.
            word.fetch_or(bits.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Payload family of one fixture run. Each domain is the stock value with a
/// distinct mark XORed into byte zero, so the final value of every ID names
/// the last domain that acknowledged a write to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueDomain {
    /// Stock value (build).
    Build,
    /// Q1 pilot: byte zero XOR 0x40.
    Pilot,
    /// Measured overwrite (A1 overwrite, Q1 timed): byte zero XOR 0x80.
    Timed,
}

impl ValueDomain {
    #[must_use]
    pub const fn mark(self) -> u8 {
        match self {
            Self::Build => 0,
            Self::Pilot => 0x40,
            Self::Timed => 0x80,
        }
    }

    #[must_use]
    pub fn value(
        self,
        namespace: &[u8; 16],
        seed: u64,
        id: u64,
        size: usize,
        entropy: f64,
    ) -> tidehunter::minibytes::Bytes {
        crate::workload::marked_value_bytes(namespace, seed, id, size, entropy, self.mark())
    }
}

/// Per-run fixture state the runner updates on each measured put.
pub struct FixtureRun {
    pub stage: IndexFixtureStage,
    domain: ValueDomain,
    touched: TouchedBitmap,
    /// Dispatched puts without a success outcome: applied or not, unknown.
    uncertain: TouchedBitmap,
    successful_puts: AtomicU64,
}

impl FixtureRun {
    /// A1 run: `build` writes the build domain, `overwrite` the timed one.
    pub fn new(stage: IndexFixtureStage, keys: u64) -> BackendResult<Self> {
        let domain = match stage {
            IndexFixtureStage::Build => ValueDomain::Build,
            IndexFixtureStage::Overwrite => ValueDomain::Timed,
        };
        Self::with_domain(stage, domain, keys)
    }

    pub fn with_domain(
        stage: IndexFixtureStage,
        domain: ValueDomain,
        keys: u64,
    ) -> BackendResult<Self> {
        Ok(Self {
            stage,
            domain,
            touched: TouchedBitmap::new(keys)?,
            uncertain: TouchedBitmap::new(keys)?,
            successful_puts: AtomicU64::new(0),
        })
    }

    #[must_use]
    pub fn value_domain(&self) -> ValueDomain {
        self.domain
    }

    /// One successful measured scalar put of logical `id`.
    pub fn record_put(&self, id: u64) -> BackendResult<()> {
        self.touched.set(id)?;
        // Relaxed: a count read only after the phase drained (see `set`).
        self.successful_puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// One dispatched measured put of `id` that did not succeed: the owner
    /// may or may not hold its value.
    pub fn record_uncertain(&self, id: u64) -> BackendResult<()> {
        self.uncertain.set(id)
    }

    #[must_use]
    pub fn touched(&self) -> &TouchedBitmap {
        &self.touched
    }

    #[must_use]
    pub fn uncertain(&self) -> &TouchedBitmap {
        &self.uncertain
    }

    #[must_use]
    pub fn successful_puts(&self) -> u64 {
        self.successful_puts.load(Ordering::Relaxed)
    }
}

/// Inputs of the stage end, owned so the end runs on a blocking worker.
pub struct FixtureFinish {
    /// Run namespace; must equal the fixture dataset namespace.
    pub namespace: [u8; 16],
    pub case_id: Option<String>,
    pub output: PathBuf,
    /// Measurement drained with no fatal error so far.
    pub measurement_ok: bool,
}
