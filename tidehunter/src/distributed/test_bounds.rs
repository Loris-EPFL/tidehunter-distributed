//! Waiting bounds shared by the distributed tests.

use std::time::Duration;

/// Safety cap for a wait that follows another thread's or owner's progress: a
/// handshake, a counter poll or a channel receive. The test passes when the
/// awaited event occurs, never because time passed, so the cap is large enough
/// that only a dead peer, not machine load, can reach it. It is not a timing
/// measurement; floors that prove a negative keep their own short sleeps.
pub(crate) const PROGRESS_CAP: Duration = Duration::from_secs(60);

/// The same cap for a whole child test-binary run, which the crash-model tests
/// spawn and poll with `try_wait`.
pub(crate) const CHILD_RUN_CAP: Duration = Duration::from_secs(600);
