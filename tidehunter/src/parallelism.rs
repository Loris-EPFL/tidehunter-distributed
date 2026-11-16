//! Shared CPU sizing for bounded worker pools.

use std::num::NonZeroUsize;
use std::sync::OnceLock;

/// Return the available CPU parallelism, cached at first use and at least one.
pub fn available() -> usize {
    static AVAILABLE: OnceLock<usize> = OnceLock::new();
    *AVAILABLE.get_or_init(|| std::thread::available_parallelism().map_or(1, NonZeroUsize::get))
}

/// Resolve an automatic count without persisting the current host's CPU count.
pub(crate) fn resolve_count(explicit: Option<usize>, available: usize) -> usize {
    explicit.unwrap_or(available)
}

#[cfg(test)]
mod tests {
    #[test]
    fn automatic_counts_resolve_on_the_runtime_host() {
        for available in [1, 16] {
            assert_eq!(super::resolve_count(None, available), available);
            assert_eq!(super::resolve_count(Some(4), available), 4);
        }
    }

    #[test]
    fn available_is_nonzero_and_cached() {
        let available = super::available();
        assert!(available > 0);
        assert_eq!(super::available(), available);
    }
}
