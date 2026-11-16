//! Shared CPU sizing for bounded worker pools.

use std::num::NonZeroUsize;
use std::sync::OnceLock;

/// Return the available CPU parallelism, cached at first use and at least one.
pub fn available() -> usize {
    static AVAILABLE: OnceLock<usize> = OnceLock::new();
    *AVAILABLE.get_or_init(|| std::thread::available_parallelism().map_or(1, NonZeroUsize::get))
}

#[cfg(test)]
mod tests {
    #[test]
    fn available_is_nonzero_and_cached() {
        let available = super::available();
        assert!(available > 0);
        assert_eq!(super::available(), available);
    }
}
