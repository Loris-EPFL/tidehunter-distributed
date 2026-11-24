use crate::db::{DbError, DbResult};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

/// Native database worker that first failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackgroundThread {
    Flusher,
    Relocator,
    PendingPromotion,
    FlatPromotion,
    Snapshot,
    WalTracker,
    WalMapper,
    WalSyncer,
    WalUnlinker,
}

/// Original failure, before the worker's panic is resumed.
#[derive(Debug, Eq, PartialEq)]
pub enum BackgroundFailureReason {
    Panic(String),
    Error(String),
}

/// First background failure for one open database. Reopen to recover from its WAL.
#[derive(Debug, Eq, PartialEq)]
pub struct BackgroundFailure {
    pub role: BackgroundThread,
    pub reason: BackgroundFailureReason,
}

#[derive(Default)]
pub(crate) struct BackgroundHealth {
    failed: AtomicBool,
    first: OnceLock<Arc<BackgroundFailure>>,
}

impl BackgroundHealth {
    #[inline]
    pub(crate) fn check(&self) -> DbResult<()> {
        // Acquire pairs with record's Release: the reason is fully installed
        // before any foreground write can observe the failed state.
        if !self.failed.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(DbError::BackgroundFailure(Arc::clone(
            self.first.get().expect("failed flag publishes the reason"),
        )))
    }

    fn record(&self, role: BackgroundThread, reason: BackgroundFailureReason) {
        // The first reason wins; a later worker's failure is not recorded.
        let _ = self.first.set(Arc::new(BackgroundFailure { role, reason }));
        // Release publishes the installed first reason, including when another
        // worker installed it and this worker lost the OnceLock race.
        self.failed.store(true, Ordering::Release);
    }

    pub(crate) fn record_error(&self, role: BackgroundThread, error: &DbError) {
        self.record(role, BackgroundFailureReason::Error(format!("{error:?}")));
    }

    pub(crate) fn run<T>(&self, role: BackgroundThread, job: impl FnOnce() -> T) -> T {
        // The worker is never restarted or allowed to continue after unwind.
        // AssertUnwindSafe only lets us record the failure before resuming it.
        match catch_unwind(AssertUnwindSafe(job)) {
            Ok(result) => result,
            Err(payload) => {
                let message = if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_owned()
                } else {
                    "non-string panic payload".to_owned()
                };
                self.record(role, BackgroundFailureReason::Panic(message));
                resume_unwind(payload)
            }
        }
    }
}
