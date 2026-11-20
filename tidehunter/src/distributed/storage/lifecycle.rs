//! Lifecycle transitions that own live-frame adoption against a stopped
//! frontier.
//!
//! A transition is set before its durability frontier stops and cleared when it
//! ends. It is never inferred from a stopped frontier. Adoption proves appended
//! bytes only; a transition can adopt a frame that an external writer left below
//! the extent end after the frontier stopped, and its own persist is then the
//! covering barrier (paper 3.1: the finalizing sync publishes the written
//! bytes). Outside a transition a stopped frontier cannot cover a new frame, so
//! it stays an error.
use super::{PartitionLog, State, StorageError};
use crate::distributed::durability::frontier::Syncer;
use crate::wal::WalError;

/// The lifecycle transition that owns live-frame adoption.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Lifecycle {
    /// No transition runs. Nothing owns a stopped frontier.
    Running,
    /// The syncer retirement stopped the frontier for its final drain.
    RetiringSyncer,
    /// The seal retired the frontier and now adopts and seals the live extent.
    Sealing,
}

impl Lifecycle {
    /// True while this transition may adopt against a stopped frontier.
    pub(super) fn owns_stopped_frontier(self) -> bool {
        !matches!(self, Self::Running)
    }
}

impl State {
    /// True while the seal transition refuses new writes and adoptions.
    pub(super) fn is_sealing(&self) -> bool {
        self.lifecycle == Lifecycle::Sealing
    }
}

impl PartitionLog {
    /// Stop the syncer, drain it, and catch up the deferred live root.
    ///
    /// The retirement owns every frame its final drain adopts. Mark the log
    /// before the frontier stops: `Syncer::shutdown` drains with one last
    /// `sync_for_frontier`, and that drain adopts frames that external writers
    /// left below the extent end. The drain's own appended persist then covers
    /// them before this call reports success. The frontier stays stopped after
    /// the transition: a later adoption outside a transition is an error.
    pub(crate) fn retire_syncer(&self, syncer: Syncer) -> Result<(), StorageError> {
        {
            let mut state = self.state.lock();
            if state.lifecycle == Lifecycle::Running {
                state.lifecycle = Lifecycle::RetiringSyncer;
            }
        }
        let drained = syncer.shutdown().map_err(|error| {
            StorageError::Unknown(WalError::Io(std::io::Error::other(error.to_string())))
        });
        // A fence and a retirement also catch up a deferred live root.
        let result = drained.and_then(|()| self.persist_live_root());
        {
            let mut state = self.state.lock();
            if state.lifecycle == Lifecycle::RetiringSyncer {
                state.lifecycle = Lifecycle::Running;
            }
        }
        result
    }
}
