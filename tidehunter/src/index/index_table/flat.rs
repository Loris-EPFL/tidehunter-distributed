// ---------------------------------------------------------------------------
// Flat binary buffer helpers for IndexTable
//
// Two in-memory formats, selected by `IndexTable::key_size`:
//
//   Variable-length (key_size == None):
//     [count: u32][offsets: u32 * count][key_len: u16][key][position: P::LENGTH]*
//     Offsets enable O(log n) binary search without scanning entries.
//
//   Fixed-length (key_size == Some(n)):
//     [key (n bytes)][position: P::LENGTH]*
//     No per-entry overhead; count = flat.len() / (n + P::LENGTH).
//
// The position bytes are produced and parsed by `IndexPosition::write_flat` /
// `read_flat`. For the native `WalPosition` instantiation this is byte-identical
// to the historical format: `[wal_offset: u64][encoded_len: u32]`, where
// `encoded_len` packs the entry kind into the top 2 bits of the length field.
// WAL frame sizes are always < 1 GiB (2^30), so bits 30-31 are free:
//   bits 31-30 = kind  (0 = Clean, 1 = Modified, 2 = Removed)
//   bits 29-0  = actual WAL frame length
//
// The flat buffer is purely in-memory and never persisted.
// ---------------------------------------------------------------------------

#[cfg(test)]
use super::IndexEntryKind;
use super::{DataOverlay, data_latest_per_key};
use crate::wal::position::IndexPosition;
use bytes::BytesMut;
use minibytes::Bytes;
use std::cmp::Ordering;

// ---- Variable-length helpers (operate on the flat buffer directly) --------

// `#[inline]` on these three: `flat_entry_at::<P>` is instantiated in the
// crate that uses it (the bench and downstream crates), while these are
// non-generic and live in this crate. Without the attribute a native lookup
// calls them across the crate boundary, so the fixed-length path also pays the
// callee-saved register spills of the variable-length path (measured on the
// linked `index_iter_benchmarks`: 7 pushes/pops per `flat_entry_at` call).
#[inline]
fn var_flat_entry_count(flat: &[u8]) -> usize {
    if flat.len() < 4 {
        return 0;
    }
    u32::from_be_bytes(flat[0..4].try_into().unwrap()) as usize
}

#[inline]
fn var_flat_entries_section_start(count: usize) -> usize {
    4 + 4 * count
}

#[inline]
fn var_flat_read_entry_offset(flat: &[u8], idx: usize) -> usize {
    let pos = 4 + 4 * idx;
    u32::from_be_bytes(flat[pos..pos + 4].try_into().unwrap()) as usize
}

/// Encode `kind` into the top 2 bits of `len`. `len` must be < 2^30 (asserted in `Wal::multi_write`).
/// Tombstones read from disk carry `WalPosition::INVALID` whose len is `u32::MAX` —
/// `TaggedWalPosition::from_disk` normalises that to `0` so the encoding here stays lossless.
#[cfg(test)]
#[inline]
pub(super) fn encode_kind_in_len(len: u32, kind: IndexEntryKind) -> u32 {
    assert!(
        len < (1 << 30),
        "encode_kind_in_len: len must be < 2^30 (got 0x{len:08x}) — kind bits collide",
    );
    len | ((kind.to_u8() as u32) << 30)
}

/// Extract `kind` from the top 2 bits and the actual `len` from the bottom 30 bits.
#[cfg(test)]
#[inline]
pub(super) fn decode_kind_from_len(encoded: u32) -> (u32, IndexEntryKind) {
    (
        encoded & 0x3FFF_FFFF,
        IndexEntryKind::from_u8((encoded >> 30) as u8),
    )
}

/// Parse a `(key_slice, tagged position)` from the var-len entries section at
/// `byte_offset`. Layout: `[key_len: u16][key][position: P::LENGTH]`.
fn var_flat_parse_entry<P: IndexPosition>(
    entries_section: &[u8],
    byte_offset: usize,
) -> (&[u8], P::Tagged) {
    let key_len = u16::from_be_bytes(
        entries_section[byte_offset..byte_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let key = &entries_section[byte_offset + 2..byte_offset + 2 + key_len];
    let start = byte_offset + 2 + key_len;
    (key, P::read_flat_tagged_at(entries_section, start))
}

// ---------------------------------------------------------------------------
// Shared flat-buffer helpers (operate on raw `&[u8]`, format-agnostic)
// ---------------------------------------------------------------------------

/// Returns the number of entries encoded in `flat`.
pub(super) fn flat_count<P: IndexPosition>(flat: &[u8], key_size: Option<usize>) -> usize {
    if flat.is_empty() {
        return 0;
    }
    match key_size {
        None => var_flat_entry_count(flat),
        Some(ks) => flat.len() / (ks + P::LENGTH),
    }
}

/// Returns the key slice and tagged position for the entry at position `idx`.
/// The returned key slice borrows from `flat`.
/// Panics if `idx >= flat_count(flat, key_size)`.
pub(super) fn flat_entry_at<P: IndexPosition>(
    flat: &[u8],
    key_size: Option<usize>,
    idx: usize,
) -> (&[u8], P::Tagged) {
    match key_size {
        None => {
            let count = var_flat_entry_count(flat);
            let entries_start = var_flat_entries_section_start(count);
            let entries = &flat[entries_start..];
            let off = var_flat_read_entry_offset(flat, idx);
            var_flat_parse_entry::<P>(entries, off)
        }
        Some(ks) => {
            let elem_size = ks + P::LENGTH;
            let start = idx * elem_size;
            let key = &flat[start..start + ks];
            (key, P::read_flat_tagged_at(flat, start + ks))
        }
    }
}

/// Lower bound: the smallest index `i` such that `flat[i].key >= target`.
/// Returns `flat_count(flat, key_size)` when all keys are less than `target`.
pub(super) fn flat_lower_bound<P: IndexPosition>(
    flat: &[u8],
    key_size: Option<usize>,
    target: &[u8],
) -> usize {
    let count = flat_count::<P>(flat, key_size);
    crate::index::versioned_overlay::lower_bound_by(count, |mid| {
        flat_entry_at::<P>(flat, key_size, mid).0 < target
    })
}

/// Upper bound: the smallest index `i` such that `flat[i].key > target`.
/// Returns `flat_count(flat, key_size)` when all keys are <= `target`.
pub(super) fn flat_upper_bound<P: IndexPosition>(
    flat: &[u8],
    key_size: Option<usize>,
    target: &[u8],
) -> usize {
    let count = flat_count::<P>(flat, key_size);
    crate::index::versioned_overlay::lower_bound_by(count, |mid| {
        flat_entry_at::<P>(flat, key_size, mid).0 <= target
    })
}

/// Build a flat buffer from a sorted list of `(key, tagged position)` pairs.
///
/// Pass `key_size = Some(n)` for fixed-length key spaces; `None` for variable-length.
pub(super) fn build_flat_bytes<P: IndexPosition>(
    entries: Vec<(Bytes, P::Tagged)>,
    key_size: Option<usize>,
) -> Bytes {
    let n = entries.len();
    if n == 0 {
        return Bytes::default();
    }

    if let Some(key_size) = key_size {
        // Fixed-length format: [key (key_size)][position (P::LENGTH)]
        let elem_size = key_size + P::LENGTH;
        let mut result = Vec::with_capacity(n * elem_size);
        for (key, iwp) in &entries {
            debug_assert_eq!(key.len(), key_size, "key length mismatch in fixed flat");
            result.extend_from_slice(key);
            write_tagged::<P>(*iwp, &mut result);
        }
        Bytes::from(result)
    } else {
        // Variable-length format: [count: u32][offsets: u32*n][key_len: u16][key][position]*
        let total_key_bytes: usize = entries.iter().map(|(k, _)| k.len()).sum();
        let total_size = 4 + 4 * n + (2 + P::LENGTH) * n + total_key_bytes;
        let mut out = BytesMut::with_capacity(total_size);
        append_flat_varlen::<P>(&entries, &mut out);
        debug_assert_eq!(out.len(), total_size, "varlen flat size mismatch");
        Bytes::from(out.freeze())
    }
}

/// Append variable-length entries directly into `out`, avoiding a separate allocation.
///
/// Format: `[count: u32][offsets: u32*n][key_len: u16][key][position]`.
///
/// Offsets are relative to the start of the entries section (immediately after the offset table),
/// which is the same convention used by `build_flat_bytes` and the read-side helpers.
pub(super) fn append_flat_varlen<P: IndexPosition>(
    entries: &[(Bytes, P::Tagged)],
    out: &mut BytesMut,
) {
    let n = entries.len();
    if n == 0 {
        return;
    }
    let header_start = out.len();
    let offsets_start = header_start + 4; // after count field
    let data_start = offsets_start + 4 * n; // after offset table

    // Reserve header: [count: u32][offsets: u32 * n] — placeholders filled in below.
    out.extend_from_slice(&(n as u32).to_be_bytes());
    for _ in 0..n {
        out.extend_from_slice(&0u32.to_be_bytes());
    }

    // Write data entries and back-fill each offset as we go.
    for (i, (key, iwp)) in entries.iter().enumerate() {
        let entry_offset: u32 = (out.len() - data_start)
            .try_into()
            .expect("flat buffer exceeds u32 offset range");
        out[offsets_start + 4 * i..offsets_start + 4 * i + 4]
            .copy_from_slice(&entry_offset.to_be_bytes());
        out.extend_from_slice(&(key.len() as u16).to_be_bytes());
        out.extend_from_slice(key);
        write_tagged::<P>(*iwp, out);
    }

    // Back-fill the count field.
    out[header_start..header_start + 4].copy_from_slice(&(n as u32).to_be_bytes());
}

#[inline]
fn write_tagged<P: IndexPosition>(tagged: P::Tagged, out: &mut impl bytes::BufMut) {
    let (position, kind) = P::untag(tagged);
    position.write_flat(kind.to_u8(), out);
}

// ---------------------------------------------------------------------------
// Sequential flat iterator
// ---------------------------------------------------------------------------

pub(super) enum FlatIter<'a, P: IndexPosition> {
    Empty,
    VarLen {
        flat: &'a [u8], // full flat buffer, starts with count field
        entries_section: &'a [u8],
        count: usize,
        index: usize,
    },
    Fixed {
        entries: &'a [u8], // full flat buffer: packed key+position elements
        key_size: usize,
        count: usize,
        index: usize,
        // Ties `P` to the enum without a third variant: a dummy variant
        // would add a match arm to every `next()`.
        position: std::marker::PhantomData<fn() -> P>,
    },
}

impl<'a, P: IndexPosition> FlatIter<'a, P> {
    pub(super) fn new(flat: &'a [u8], key_size: Option<usize>) -> Self {
        if flat.is_empty() {
            return Self::Empty;
        }
        match key_size {
            None => {
                let count = var_flat_entry_count(flat);
                let section_start = var_flat_entries_section_start(count);
                let entries_section = if count > 0 {
                    &flat[section_start..]
                } else {
                    &[]
                };
                Self::VarLen {
                    flat,
                    entries_section,
                    count,
                    index: 0,
                }
            }
            Some(key_size) => {
                let elem_size = key_size + P::LENGTH;
                let count = if elem_size > 0 {
                    flat.len() / elem_size
                } else {
                    0
                };
                Self::Fixed {
                    entries: flat,
                    key_size,
                    count,
                    index: 0,
                    position: std::marker::PhantomData,
                }
            }
        }
    }
}

impl<'a, P: IndexPosition> Iterator for FlatIter<'a, P> {
    type Item = (&'a [u8], P::Tagged);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::VarLen {
                flat,
                entries_section,
                count,
                index,
            } => {
                if *index >= *count {
                    return None;
                }
                let byte_offset = var_flat_read_entry_offset(flat, *index);
                let (key, iwp) = var_flat_parse_entry::<P>(entries_section, byte_offset);
                *index += 1;
                Some((key, iwp))
            }
            Self::Fixed {
                entries,
                key_size,
                count,
                index,
                position: _,
            } => {
                if *index >= *count {
                    return None;
                }
                let elem_size = *key_size + P::LENGTH;
                let start = *index * elem_size;
                let key = &entries[start..start + *key_size];
                *index += 1;
                Some((key, P::read_flat_tagged_at(entries, start + *key_size)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Merge writer (flat + BTreeMap → new flat) used by `promote_to_flat`
// ---------------------------------------------------------------------------

/// Walk the merge of `flat` (decoded via `FlatIter`) with the sorted `data`
/// set, invoking `f` once per emitted entry in sorted-key order. Data
/// entries override flat entries when the keys are equal.
///
/// For each key, `data` may carry multiple positions — only the *latest*
/// participates in the merge. If that latest is not below `cut`, the entire
/// key is skipped (and any matching flat entry is kept as-is).
fn merge_walk<P: IndexPosition>(
    flat: &[u8],
    key_size: Option<usize>,
    data: &DataOverlay<P>,
    cut: P::Cut,
    mut f: impl FnMut(&[u8], P::Tagged),
) {
    let mut flat_iter = FlatIter::<P>::new(flat, key_size);
    let mut data_iter =
        data_latest_per_key::<P>(data).filter(|(_, v)| P::tagged_position(**v).below(cut));
    let mut flat_cur = flat_iter.next();
    let mut data_cur = data_iter.next();

    loop {
        let order = match (&flat_cur, &data_cur) {
            (None, None) => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(fl), Some(bt)) => fl.0.cmp(bt.0.as_ref()),
        };
        match order {
            Ordering::Less => {
                let (fk, fiwp) = flat_cur.take().unwrap();
                f(fk, fiwp);
                flat_cur = flat_iter.next();
            }
            Ordering::Greater => {
                let (bk, biwp) = data_cur.take().unwrap();
                f(bk.as_ref(), *biwp);
                data_cur = data_iter.next();
            }
            Ordering::Equal => {
                // Data entry overrides flat entry on equal keys.
                flat_cur = flat_iter.next();
                let (bk, biwp) = data_cur.take().unwrap();
                f(bk.as_ref(), *biwp);
                data_cur = data_iter.next();
            }
        }
    }
}

/// Merge `flat` and `data` into a new flat buffer with a single allocation.
///
/// `cut` filters the data side: for each key, only the *latest* position
/// participates and only if it is below `cut`. Keys whose latest position is
/// not below `cut` are skipped entirely (the existing flat entry, if any, is
/// kept).
///
/// Two passes over the merge:
///  1. Sizing: count emitted entries (and total key bytes for var-len) and
///     dirty entries to compute the exact output size.
///  2. Fill: write entries directly into a pre-sized `BytesMut`.
///
/// Returns the new flat buffer and the dirty-entry count.
pub(super) fn merge_into_flat<P: IndexPosition>(
    flat: &[u8],
    key_size: Option<usize>,
    data: &DataOverlay<P>,
    cut: P::Cut,
) -> (Bytes, usize) {
    let mut entry_count: usize = 0;
    let mut total_key_bytes: usize = 0;
    let mut dirty_count: usize = 0;
    merge_walk::<P>(flat, key_size, data, cut, |key, iwp| {
        entry_count += 1;
        total_key_bytes += key.len();
        if !P::tagged_is_clean(iwp) {
            dirty_count += 1;
        }
    });

    if entry_count == 0 {
        return (Bytes::default(), 0);
    }

    let new_flat = match key_size {
        Some(ks) => {
            // Fixed-len: [key (ks)][position]*
            let elem_size = ks + P::LENGTH;
            let total_size = entry_count * elem_size;
            let mut out = BytesMut::with_capacity(total_size);
            merge_walk::<P>(flat, key_size, data, cut, |key, iwp| {
                debug_assert_eq!(key.len(), ks, "key length mismatch in fixed flat");
                out.extend_from_slice(key);
                write_tagged::<P>(iwp, &mut out);
            });
            debug_assert_eq!(out.len(), total_size, "fixed flat size mismatch");
            Bytes::from(out.freeze())
        }
        None => {
            // Var-len: [count: u32][offsets: u32 * n][key_len: u16][key][position]*
            let header_size = 4 + 4 * entry_count;
            let entries_size = entry_count * (2 + P::LENGTH) + total_key_bytes;
            let total_size = header_size + entries_size;
            let mut out = BytesMut::with_capacity(total_size);
            out.extend_from_slice(&(entry_count as u32).to_be_bytes());
            let offsets_start = out.len();
            // Reserve the offset table; back-fill each slot as we write entries.
            out.resize(offsets_start + 4 * entry_count, 0);
            let data_start = out.len();
            let mut i: usize = 0;
            merge_walk::<P>(flat, key_size, data, cut, |key, iwp| {
                let entry_offset: u32 = (out.len() - data_start)
                    .try_into()
                    .expect("flat buffer exceeds u32 offset range");
                out[offsets_start + 4 * i..offsets_start + 4 * i + 4]
                    .copy_from_slice(&entry_offset.to_be_bytes());
                out.extend_from_slice(&(key.len() as u16).to_be_bytes());
                out.extend_from_slice(key);
                write_tagged::<P>(iwp, &mut out);
                i += 1;
            });
            debug_assert_eq!(out.len(), total_size, "varlen flat size mismatch");
            Bytes::from(out.freeze())
        }
    };

    (new_flat, dirty_count)
}
