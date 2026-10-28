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
        self.nodes.read_exact_at(&mut slot, pointer.offset)?;
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
            self.reader.nodes.write_all_at(
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
        let (old_key, _) = walk_leaf(root, key, |pointer| self.node(pointer))?;
        if old_key == key {
            return Err(StorageError::Invalid("duplicate history radix authority"));
        }
        let bit = (old_key ^ key).leading_zeros() as u8;
        Ok(Some(self.insert_branch(root, key, bit, leaf, 0)?))
    }

    fn insert_branch(
        &mut self,
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
        match self.node(root)? {
            Node::Branch {
                bit,
                mut zero,
                mut one,
            } if bit < split => {
                if direction(key, bit) {
                    one = self.insert_branch(one, key, split, leaf, depth + 1)?;
                } else {
                    zero = self.insert_branch(zero, key, split, leaf, depth + 1)?;
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

    #[test]
    fn buffered_nodes_check_digest_bounds_and_survive_flush() {
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
        let mut history = History::create(temp.path(), &config).unwrap();
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
}
