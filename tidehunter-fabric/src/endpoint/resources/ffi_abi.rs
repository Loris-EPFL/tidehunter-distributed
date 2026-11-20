//! Checked conversion of C ABI constants.

/// Bindgen emits libfabric's status, mode and wait constants as small positive
/// `u32` values, and libc address families are small positive `c_int`; each
/// comparison type always holds them. Name that ABI fact once instead of an
/// unwrap at every call site.
#[allow(
    clippy::expect_used,
    reason = "the libfabric and libc constants here are fixed small values inside every comparison type"
)]
#[track_caller]
pub(super) fn abi<T, R>(value: T) -> R
where
    R: TryFrom<T>,
    <R as TryFrom<T>>::Error: std::fmt::Debug,
{
    R::try_from(value).expect("ABI constant fits its comparison type")
}
