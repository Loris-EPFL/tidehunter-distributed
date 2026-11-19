//! Post-checkpoint replay suffix that triggers a background checkpoint.
//!
//! WAL replay has no hard suffix bound, as the native engine has none; these
//! limits only decide when a checkpoint starts. The authority manifest
//! records the writer's limits.

use serde::{Deserialize, Serialize};

/// Suffix retained after the last committed checkpoint that triggers the next one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayLimits {
    pub max_batches: usize,
    pub max_encoded_bytes: usize,
}

impl Default for ReplayLimits {
    fn default() -> Self {
        Self {
            max_batches: 4096,
            max_encoded_bytes: 256 * 1024 * 1024,
        }
    }
}

impl ReplayLimits {
    /// Default of an authority manifest that records no limits.
    pub(super) const fn legacy() -> Self {
        Self {
            max_batches: 1024,
            max_encoded_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn validate(self) -> Result<(), &'static str> {
        if !(1..=65_536).contains(&self.max_batches)
            || self.max_encoded_bytes == 0
            || self.max_encoded_bytes as u64 > 8 * 1024 * 1024 * 1024
        {
            return Err("replay limits must be finite: 1..=65536 batches and 1 byte..=8 GiB");
        }
        Ok(())
    }

    /// Start well before the hard bound; a larger window alone does not make a
    /// checkpointer that cannot keep up sustainable. Batches and bytes are
    /// separate soft triggers.
    pub(super) fn batches_due(self, batches: usize) -> bool {
        batches >= (self.max_batches / 4).max(1)
    }

    pub(super) fn bytes_due(self, bytes: usize) -> bool {
        bytes >= (self.max_encoded_bytes / 4).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_is_due_at_a_quarter_of_either_limit() {
        let limits = ReplayLimits {
            max_batches: 16,
            max_encoded_bytes: 400,
        };
        assert!(!limits.batches_due(3) && !limits.bytes_due(99));
        assert!(limits.batches_due(4));
        assert!(limits.bytes_due(100));
        let tiny = ReplayLimits {
            max_batches: 1,
            max_encoded_bytes: 1,
        };
        assert!(!tiny.batches_due(0) && !tiny.bytes_due(0));
        assert!(tiny.batches_due(1));
    }

    #[test]
    fn invalid_unbounded_and_zero_settings_are_rejected() {
        for limits in [
            ReplayLimits {
                max_batches: 0,
                max_encoded_bytes: 1,
            },
            ReplayLimits {
                max_batches: 65_537,
                max_encoded_bytes: 1,
            },
            ReplayLimits {
                max_batches: 1,
                max_encoded_bytes: 0,
            },
        ] {
            assert!(limits.validate().is_err());
        }
        if let Ok(max_encoded_bytes) = usize::try_from(8 * 1024 * 1024 * 1024u64 + 1) {
            assert!(
                ReplayLimits {
                    max_batches: 1,
                    max_encoded_bytes
                }
                .validate()
                .is_err()
            );
        }
    }
}
