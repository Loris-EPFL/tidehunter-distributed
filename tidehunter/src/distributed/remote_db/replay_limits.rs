//! Finite post-checkpoint replay work, shared by admission and recovery.
//!
//! These limits bound recovery work, not acknowledgement durability. A durable
//! authority manifest records the writer's bound; recovery validates against
//! that recorded bound before a larger bound can be activated.

use serde::{Deserialize, Serialize};

/// Maximum complete suffix retained after the last committed checkpoint.
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
    /// Implicit bounds of the format-2 authority manifest.
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
    /// checkpointer that cannot keep up sustainable.
    pub(super) fn checkpoint_due(self, batches: usize, bytes: usize) -> bool {
        batches >= (self.max_batches / 4).max(1) || bytes >= (self.max_encoded_bytes / 4).max(1)
    }

    pub(super) fn requires_flush(self, batches: usize, bytes: usize, incoming: usize) -> bool {
        batches >= self.max_batches
            || bytes
                .checked_add(incoming)
                .is_none_or(|total| total > self.max_encoded_bytes)
    }

    /// A smaller operator setting cannot reinterpret an already acknowledged
    /// suffix as corruption. Shrinking requires an explicit checkpoint cutover.
    pub(super) fn covers(self, recorded: Self) -> bool {
        self.max_batches >= recorded.max_batches
            && self.max_encoded_bytes >= recorded.max_encoded_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_checks_the_next_complete_batch_and_overflow() {
        let limits = ReplayLimits {
            max_batches: 3,
            max_encoded_bytes: 100,
        };
        assert!(!limits.requires_flush(2, 80, 20));
        assert!(limits.requires_flush(3, 0, 1));
        assert!(limits.requires_flush(2, 80, 21));
        assert!(limits.requires_flush(0, usize::MAX, 1));
    }

    #[test]
    fn checkpoint_leaves_headroom_before_either_hard_bound() {
        let limits = ReplayLimits {
            max_batches: 16,
            max_encoded_bytes: 400,
        };
        assert!(!limits.checkpoint_due(3, 99));
        assert!(limits.checkpoint_due(4, 0));
        assert!(limits.checkpoint_due(0, 100));
        assert!(!limits.requires_flush(4, 100, 1));
        let tiny = ReplayLimits {
            max_batches: 1,
            max_encoded_bytes: 1,
        };
        assert!(!tiny.checkpoint_due(0, 0));
        assert!(tiny.checkpoint_due(1, 0));
    }

    #[test]
    fn reopen_cannot_shrink_either_recorded_recovery_dimension() {
        let recorded = ReplayLimits {
            max_batches: 8,
            max_encoded_bytes: 100,
        };
        assert!(recorded.covers(recorded));
        assert!(
            ReplayLimits {
                max_batches: 16,
                max_encoded_bytes: 200
            }
            .covers(recorded)
        );
        assert!(
            !ReplayLimits {
                max_batches: 7,
                max_encoded_bytes: 200
            }
            .covers(recorded)
        );
        assert!(
            !ReplayLimits {
                max_batches: 16,
                max_encoded_bytes: 99
            }
            .covers(recorded)
        );
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
