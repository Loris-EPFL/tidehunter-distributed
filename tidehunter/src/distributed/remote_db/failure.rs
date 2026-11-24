//! The first causal fail-stop record of a database. One writer stores it before
//! any failure flag, so a control wait that observes the flag can report the
//! typed version.
//!
//! Failure-only evidence: no caller data is included, and formatting is capped
//! while writing rather than allocating an unbounded intermediate error string.

use super::super::types::BatchVersion;

/// First causal fail-stop record: the bounded message plus the uncertain
/// version it names. It is written once, before any failure flag is stored,
/// so a control wait that observes the flag can report the typed version.
#[derive(Debug)]
pub(in crate::distributed::remote_db) struct FirstFailure {
    message: String,
    version: Option<BatchVersion>,
}

impl FirstFailure {
    pub(in crate::distributed::remote_db) fn version(&self) -> Option<BatchVersion> {
        self.version
    }
}

impl std::ops::Deref for FirstFailure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for FirstFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Record `error` as the first failure when no other cause is recorded yet.
pub(in crate::distributed::remote_db) fn record_first_failure(
    slot: &std::sync::OnceLock<FirstFailure>,
    partition: Option<u32>,
    version: Option<BatchVersion>,
    stage: &'static str,
    error: &dyn std::fmt::Display,
) {
    struct Bounded(String, bool);
    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let remaining = 4084usize.saturating_sub(self.0.len());
            let mut end = remaining.min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.1 |= end < text.len();
            self.0.push_str(&text[..end]);
            Ok(())
        }
    }
    if slot.get().is_none() {
        use std::fmt::Write;
        let mut message = Bounded(String::new(), false);
        let _ = write!(
            message,
            "owner={partition:?} batch={version:?} stage={stage}: {error}"
        );
        if message.1 {
            message.0.push_str(" [truncated]");
        }
        let _ = slot.set(FirstFailure {
            message: message.0,
            version,
        });
    }
}

/// `message`, followed by the recorded first cause when there is one.
pub(in crate::distributed::remote_db) fn failure_context(
    slot: &std::sync::OnceLock<FirstFailure>,
    message: &str,
) -> String {
    match slot.get() {
        Some(cause) => format!("{message}; first causal failure: {cause}"),
        None => message.to_owned(),
    }
}
