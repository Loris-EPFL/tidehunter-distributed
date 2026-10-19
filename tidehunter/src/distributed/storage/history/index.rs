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

impl HistoryReader {
    fn node(&self, pointer: NodePointer, end: u64) -> Result<Node, StorageError> {
        if !pointer.offset.is_multiple_of(SLOT_BYTES as u64)
            || pointer
                .offset
                .checked_add(SLOT_BYTES as u64)
                .is_none_or(|offset| offset > end)
        {
            return Err(StorageError::Invalid("history node exceeds rooted backing"));
        }
        let mut slot = [0; SLOT_BYTES];
        self.nodes.read_exact_at(&mut slot, pointer.offset)?;
        if hash(&slot) != pointer.digest {
            return Err(StorageError::Invalid("history node digest differs"));
        }
        decode_slot(&slot)
    }

    fn leaf(
        &self,
        root: NodePointer,
        key: u128,
        end: u64,
    ) -> Result<(u128, Pointer), StorageError> {
        let mut pointer = root;
        let mut previous = None;
        for _ in 0..=128 {
            match self.node(pointer, end)? {
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
    fn put_node(&mut self, node: &Node) -> Result<NodePointer, StorageError> {
        let slot = encode_slot::<SLOT_BYTES>(node)?;
        let offset = self.node_append;
        self.node_append = offset
            .checked_add(SLOT_BYTES as u64)
            .ok_or(StorageError::Capacity)?;
        self.reader.nodes.write_all_at(&slot, offset)?;
        Ok(NodePointer {
            offset,
            digest: hash(&slot),
        })
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
        let (old_key, _) = self.reader.leaf(root, key, self.node_append)?;
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
        match self.reader.node(root, self.node_append)? {
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
