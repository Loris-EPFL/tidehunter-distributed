use crate::db::{DbError, DbResult};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    additional_failures: AtomicUsize,
    #[cfg(test)]
    pub(crate) on_start: Option<(BackgroundThread, crate::failpoints::FailPoint)>,
    #[cfg(test)]
    pub(crate) failure_recorded: Option<std::sync::mpsc::SyncSender<()>>,
}

impl BackgroundHealth {
    #[cfg(test)]
    pub(crate) fn with_start_hook(
        role: BackgroundThread,
        hook: crate::failpoints::FailPoint,
        recorded: std::sync::mpsc::SyncSender<()>,
    ) -> Self {
        Self {
            on_start: Some((role, hook)),
            failure_recorded: Some(recorded),
            ..Self::default()
        }
    }

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
        if self
            .first
            .set(Arc::new(BackgroundFailure { role, reason }))
            .is_err()
        {
            // Relaxed: this diagnostic counter does not publish any state.
            self.additional_failures.fetch_add(1, Ordering::Relaxed);
        }
        // Release publishes the installed first reason, including when another
        // worker installed it and this worker lost the OnceLock race.
        self.failed.store(true, Ordering::Release);
        #[cfg(test)]
        if let Some(recorded) = &self.failure_recorded {
            let _ = recorded.try_send(());
        }
    }

    pub(crate) fn record_error(&self, role: BackgroundThread, error: &DbError) {
        self.record(role, BackgroundFailureReason::Error(format!("{error:?}")));
    }

    pub(crate) fn run<T>(&self, role: BackgroundThread, job: impl FnOnce() -> T) -> T {
        // The worker is never restarted or allowed to continue after unwind.
        // AssertUnwindSafe only lets us record the failure before resuming it.
        match catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            if let Some((target, hook)) = &self.on_start
                && *target == role
            {
                hook.fp();
            }
            job()
        })) {
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

    #[cfg(test)]
    pub(crate) fn additional_failures(&self) -> usize {
        // Relaxed: read only after the test has joined all recording threads.
        self.additional_failures.load(Ordering::Relaxed)
    }
}
