//! Ordered index primitives shared by physical and logical native cell indexes.
//!
//! Keep per-key history until the caller's checkpoint/retention frontier permits
//! collapse. These functions know neither WAL addresses nor persistence policy.

use minibytes::Bytes;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub(crate) type Versions<V> = SmallVec<[V; 1]>;
pub(crate) type Overlay<V> = BTreeMap<Bytes, Versions<V>>;

/// Returns the existing equal-order entry without replacing it. Callers decide
/// whether it is an exact replay duplicate or a conflicting physical binding.
pub(crate) fn insert_unique_by<V: Copy>(
    values: &mut Versions<V>,
    incoming: V,
    mut compare: impl FnMut(&V, &V) -> Ordering,
) -> Option<V> {
    match values.binary_search_by(|existing| compare(existing, &incoming)) {
        Ok(index) => Some(values[index]),
        Err(index) => {
            values.insert(index, incoming);
            None
        }
    }
}

pub(crate) fn matching_prefix<V>(values: &[V], predicate: impl FnMut(&V) -> bool) -> &[V] {
    &values[..values.partition_point(predicate)]
}

/// Binary-search partition point for compact native sorted blocks. The predicate
/// must describe a leading true prefix. No reference width is assumed.
pub(crate) fn lower_bound_by(len: usize, mut before: impl FnMut(usize) -> bool) -> usize {
    let mut low = 0;
    let mut high = len;
    while low < high {
        let mid = low + (high - low) / 2;
        if before(mid) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}
