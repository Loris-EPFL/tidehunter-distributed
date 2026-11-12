//! Immutable Patricia slots, following coordinator receipt_history's append-only
//! page writer. One metadata spill syncs the node file once, not once per node.
use super::*;

#[derive(Clone, Copy, Serialize, Deserialize)]
enum Node {
    Leaf {
        key: u128,
        pointer: Pointer,
    },
    Branch {
        bit: u8,
        zero: NodePointer,
        one: NodePointer,
    },
}
fn direction(key: u128, bit: u8) -> bool {
    key & (1u128 << (127 - bit)) != 0
}

fn check_node_pointer(pointer: NodePointer, end: u64) -> Result<(), StorageError> {
    if !pointer.offset.is_multiple_of(SLOT_BYTES as u64)
        || pointer
            .offset
            .checked_add(SLOT_BYTES as u64)
            .is_none_or(|offset| offset > end)
    {
        return Err(StorageError::Invalid("history node exceeds rooted backing"));
    }
    Ok(())
}

fn decode_node(pointer: NodePointer, slot: &[u8; SLOT_BYTES]) -> Result<Node, StorageError> {
    if hash(slot) != pointer.digest {
        return Err(StorageError::Invalid("history node digest differs"));
    }
    decode_slot(slot)
}

fn walk_leaf(
    root: NodePointer,
    key: u128,
    mut node: impl FnMut(NodePointer) -> Result<Node, StorageError>,
) -> Result<(u128, Pointer), StorageError> {
    let mut pointer = root;
    let mut previous = None;
    for _ in 0..=128 {
        match node(pointer)? {
            Node::Leaf { key, pointer } => return Ok((key, pointer)),
            Node::Branch { bit, zero, one } => {
                if bit >= 128 || previous.is_some_and(|previous| bit <= previous) {
                    return Err(StorageError::Invalid("invalid history radix ordering"));
                }
                previous = Some(bit);
                pointer = if direction(key, bit) { one } else { zero };
            }
        }
    }
    Err(StorageError::Invalid("history radix exceeds key width"))
}

impl HistoryReader {
    fn node(&self, pointer: NodePointer, end: u64) -> Result<Node, StorageError> {
        check_node_pointer(pointer, end)?;
        let mut slot = [0; SLOT_BYTES];
        self.files()?
            .nodes
            .read_exact_at(&mut slot, pointer.offset)?;
        decode_node(pointer, &slot)
    }

    fn leaf(
        &self,
        root: NodePointer,
        key: u128,
        end: u64,
    ) -> Result<(u128, Pointer), StorageError> {
        walk_leaf(root, key, |pointer| self.node(pointer, end))
    }

    pub(super) fn index_lookup(
        &self,
        root: Option<NodePointer>,
        key: u128,
    ) -> Result<Option<Pointer>, StorageError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let (found, pointer) = self.leaf(root, key, self.root.nodes_end)?;
        Ok((found == key).then_some(pointer))
    }
}

impl History {
    pub(super) fn flush_nodes(&mut self) -> Result<(), StorageError> {
        if self.node_buffer_len != 0 {
            self.reader.files()?.nodes.write_all_at(
                &self.node_buffer[..self.node_buffer_len],
                self.node_buffer_start,
            )?;
            self.node_buffer_len = 0;
            self.node_buffer_start = self.node_append;
        }
        Ok(())
    }

    fn node(&self, pointer: NodePointer) -> Result<Node, StorageError> {
        check_node_pointer(pointer, self.node_append)?;
        if pointer.offset >= self.node_buffer_start && pointer.offset < self.node_append {
            let offset = usize::try_from(pointer.offset - self.node_buffer_start)
                .map_err(|_| StorageError::Invalid("history buffered node offset overflow"))?;
            let end = offset
                .checked_add(SLOT_BYTES)
                .filter(|end| *end <= self.node_buffer_len)
                .ok_or(StorageError::Invalid(
                    "history node exceeds buffered backing",
                ))?;
            let slot: &[u8; SLOT_BYTES] = self.node_buffer[offset..end]
                .try_into()
                .map_err(|_| StorageError::Invalid("history buffered node bounds differ"))?;
            return decode_node(pointer, slot);
        }
        self.reader.node(pointer, self.node_append)
    }

    fn put_node(&mut self, node: &Node) -> Result<NodePointer, StorageError> {
        let slot = encode_slot::<SLOT_BYTES>(node)?;
        let offset = self.node_append;
        let next = offset
            .checked_add(SLOT_BYTES as u64)
            .ok_or(StorageError::Capacity)?;
        let end = self.node_buffer_len + SLOT_BYTES;
        self.node_buffer[self.node_buffer_len..end].copy_from_slice(&slot);
        self.node_buffer_len = end;
        self.node_append = next;
        let pointer = NodePointer {
            offset,
            digest: hash(&slot),
        };
        if self.node_buffer_len == NODE_BUFFER_BYTES {
            self.flush_nodes()?;
        }
        Ok(pointer)
    }

    pub(super) fn index_insert(
        &mut self,
        root: Option<NodePointer>,
        key: u128,
        pointer: Pointer,
    ) -> Result<Option<NodePointer>, StorageError> {
        let leaf = self.put_node(&Node::Leaf { key, pointer })?;
        let Some(root) = root else {
            return Ok(Some(leaf));
        };
        // History has the sole append capability under the history mutex.
        // Slots are immutable. Keep the nodes verified by this walk instead
        // of reading and checking the same prefix again during reconstruction.
        let mut path = [None; 129];
        let mut depth = 0;
        let (old_key, _) = walk_leaf(root, key, |pointer| {
            let node = self.node(pointer)?;
            *path.get_mut(depth).ok_or(StorageError::Invalid(
                "history radix walk exceeds key width",
            ))? = Some(node);
            depth += 1;
            Ok(node)
        })?;
        if old_key == key {
            return Err(StorageError::Invalid("duplicate history radix authority"));
        }
        let bit = (old_key ^ key).leading_zeros() as u8;
        Ok(Some(self.insert_branch(root, key, bit, leaf, 0, &path)?))
    }

    fn insert_branch(
        &mut self,
        root: NodePointer,
        key: u128,
        split: u8,
        leaf: NodePointer,
        depth: usize,
        path: &[Option<Node>; 129],
    ) -> Result<NodePointer, StorageError> {
        if depth > 128 {
            return Err(StorageError::Invalid(
                "history radix insert exceeds key width",
            ));
        }
        let node = path[depth].ok_or(StorageError::Invalid("missing history radix path"))?;
        match node {
            Node::Branch {
                bit,
                mut zero,
                mut one,
            } if bit < split => {
                if direction(key, bit) {
                    one = self.insert_branch(one, key, split, leaf, depth + 1, path)?;
                } else {
                    zero = self.insert_branch(zero, key, split, leaf, depth + 1, path)?;
                }
                self.put_node(&Node::Branch { bit, zero, one })
            }
            _ => {
                let (zero, one) = if direction(key, split) {
                    (root, leaf)
                } else {
                    (leaf, root)
                };
                self.put_node(&Node::Branch {
                    bit: split,
                    zero,
                    one,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempdir::TempDir, History) {
        let temp = tempdir::TempDir::new("history-buffered-node").unwrap();
        let config = PartitionConfig {
            inventory: StaticInventory {
                database: [1; 16],
                epoch: 1,
                segment_generation: 1,
                partitions: vec![3],
            },
            partition: 3,
            batch_limits: BatchLimits {
                max_operations: 1,
                max_encoded_bytes: 256,
            },
            group_limits: GroupLimits {
                max_batches: 1,
                max_encoded_bytes: 512,
            },
            fragment_bytes: 1024,
            wal_file_bytes: 2048,
            max_batches: 1,
            max_reference_metadata_bytes: 64 * 1024,
        };
        let history = History::create(temp.path(), &config).unwrap();
        (temp, history)
    }

    #[test]
    fn buffered_nodes_check_digest_bounds_and_survive_flush() {
        let (_temp, mut history) = fixture();
        let pointer = history
            .put_node(&Node::Leaf {
                key: 42,
                pointer: Pointer {
                    ordinal: 7,
                    digest: [3; 32],
                },
            })
            .unwrap();
        assert!(matches!(
            history.node(pointer).unwrap(),
            Node::Leaf { key: 42, .. }
        ));
        let mut bad_digest = pointer;
        bad_digest.digest[0] ^= 1;
        assert!(history.node(bad_digest).is_err());
        let mut unaligned = pointer;
        unaligned.offset += 1;
        assert!(history.node(unaligned).is_err());
        let mut past_end = pointer;
        past_end.offset = history.node_append;
        assert!(history.node(past_end).is_err());
        let mut last = pointer;
        for key in 0..NODE_BUFFER_BYTES / SLOT_BYTES {
            last = history
                .put_node(&Node::Leaf {
                    key: key as u128,
                    pointer: Pointer {
                        ordinal: key,
                        digest: [4; 32],
                    },
                })
                .unwrap();
        }
        // The old node has crossed to disk while the last node is still buffered.
        assert!(matches!(
            history.node(pointer).unwrap(),
            Node::Leaf { key: 42, .. }
        ));
        assert!(matches!(history.node(last).unwrap(), Node::Leaf { .. }));
        let mut bad_last = last;
        bad_last.digest[0] ^= 1;
        assert!(history.node(bad_last).is_err());
        history.flush_nodes().unwrap();
        assert!(matches!(
            history.node(pointer).unwrap(),
            Node::Leaf { key: 42, .. }
        ));
        assert!(matches!(history.node(last).unwrap(), Node::Leaf { .. }));
    }

    // The pre-path-reuse algorithm is a compatibility oracle for all appended
    // bytes, including unreachable copy-on-write nodes and failed duplicates.
    fn reference_insert(
        history: &mut History,
        root: Option<NodePointer>,
        key: u128,
        pointer: Pointer,
    ) -> Result<Option<NodePointer>, StorageError> {
        let leaf = history.put_node(&Node::Leaf { key, pointer })?;
        let Some(root) = root else {
            return Ok(Some(leaf));
        };
        let (old_key, _) = walk_leaf(root, key, |pointer| history.node(pointer))?;
        if old_key == key {
            return Err(StorageError::Invalid("duplicate history radix authority"));
        }
        let bit = (old_key ^ key).leading_zeros() as u8;
        Ok(Some(reference_branch(history, root, key, bit, leaf, 0)?))
    }

    fn reference_branch(
        history: &mut History,
        root: NodePointer,
        key: u128,
        split: u8,
        leaf: NodePointer,
        depth: usize,
    ) -> Result<NodePointer, StorageError> {
        if depth > 128 {
            return Err(StorageError::Invalid(
                "history radix insert exceeds key width",
            ));
        }
        match history.node(root)? {
            Node::Branch {
                bit,
                mut zero,
                mut one,
            } if bit < split => {
                if direction(key, bit) {
                    one = reference_branch(history, one, key, split, leaf, depth + 1)?;
                } else {
                    zero = reference_branch(history, zero, key, split, leaf, depth + 1)?;
                }
                history.put_node(&Node::Branch { bit, zero, one })
            }
            _ => {
                let (zero, one) = if direction(key, split) {
                    (root, leaf)
                } else {
                    (leaf, root)
                };
                history.put_node(&Node::Branch {
                    bit: split,
                    zero,
                    one,
                })
            }
        }
    }

    fn assert_same_slots(
        actual: &mut History,
        expected: &mut History,
        actual_root: Option<NodePointer>,
        expected_root: Option<NodePointer>,
    ) {
        actual.flush_nodes().unwrap();
        expected.flush_nodes().unwrap();
        assert_eq!(actual_root, expected_root);
        assert_eq!(actual.node_append, expected.node_append);
        let mut actual_bytes = vec![0; actual.node_append as usize];
        let mut expected_bytes = vec![0; expected.node_append as usize];
        actual
            .reader
            .files()
            .unwrap()
            .nodes
            .read_exact_at(&mut actual_bytes, 0)
            .unwrap();
        expected
            .reader
            .files()
            .unwrap()
            .nodes
            .read_exact_at(&mut expected_bytes, 0)
            .unwrap();
        assert_eq!(actual_bytes, expected_bytes);
    }

    #[test]
    fn radix_path_reuse_preserves_every_slot_and_root_across_spills() {
        let mut random = u128::MAX;
        let random_keys: Vec<_> = (0..512)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 43;
                random
            })
            .collect();
        let mut deep = vec![0];
        deep.extend((0..128).map(|bit| 1u128 << bit));
        deep.extend([0, 1, u128::MAX, u128::MAX]);
        let mut reversed = deep.clone();
        reversed.reverse();
        let shared_prefix = (0..512).map(|key| (u128::MAX << 16) | key).collect();
        for keys in [
            vec![],
            vec![0, 0],
            deep,
            reversed,
            shared_prefix,
            random_keys,
        ] {
            let (_actual_dir, mut actual) = fixture();
            let (_expected_dir, mut expected) = fixture();
            let mut actual_root = None;
            let mut expected_root = None;
            let mut admitted = std::collections::BTreeMap::new();
            for (ordinal, key) in keys.into_iter().enumerate() {
                let pointer = Pointer {
                    ordinal,
                    digest: [7; 32],
                };
                let result = actual.index_insert(actual_root, key, pointer);
                let reference = reference_insert(&mut expected, expected_root, key, pointer);
                match (result, reference) {
                    (Ok(next), Ok(reference)) => {
                        actual_root = next;
                        expected_root = reference;
                        assert!(admitted.insert(key, ordinal).is_none());
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string());
                    }
                    _ => panic!("radix writer differs from reference"),
                }
                assert_eq!(actual_root, expected_root);
                // Force several spill boundaries; later inserts traverse slots
                // on disk and a tail in the existing bounded node buffer.
                if ordinal % 31 == 0 {
                    assert_same_slots(&mut actual, &mut expected, actual_root, expected_root);
                }
            }
            assert_same_slots(&mut actual, &mut expected, actual_root, expected_root);
            actual.reader.root.nodes_end = actual.node_append;
            for (key, ordinal) in admitted {
                let found = actual
                    .reader
                    .index_lookup(actual_root, key)
                    .unwrap()
                    .unwrap();
                assert_eq!(found.ordinal, ordinal);
                assert_eq!(found.digest, [7; 32]);
            }
        }
    }
}
