//! Implementation of the tree-wide rebalancing and flushing logic.
//!
//! Calling [Tree::rebalance_tree] is not only possible with the root node but may be
//! applied to a variety of nodes given that their parent node is correctly
//! given. Use with caution.
use std::borrow::Borrow;

use super::{
    derivate_ref_nvm::DerivateRefNVM,
    take_child_buffer::{MergeChildResult, TakeChildBufferWrapper},
    FillUpResult, Inner, Node, Tree,
};
use crate::{
    cache::AddSize,
    data_management::{Dml, HasStoragePreference, ObjectReference},
    size::Size,
    tree::{errors::*, MessageAction},
};

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    /// This method performs necessary flushing and rebalancing operations if
    /// too many entries are stored at a node. We use this method immediately
    /// after a new value is inserted in a the specified node to assure that we
    /// will not end up in a state where an overfull node is serialized onto
    /// disk.
    ///
    /// Brief Summary
    /// -------------
    /// This method performs flushes on a path started by the given `node`.  And
    /// continues down until no more nodes can be found which are larger than
    /// they are allowed to. The basic approach is structured like this:
    ///
    /// ```pseudo
    /// Identifiers: node, child
    ///
    /// 1: Check if we have to split the current node. On success, return if new nodes are okay.
    /// 2: Select child with largest messages.
    /// 3: If the child is an internal node and too large, set child as node, goto 1.
    /// 4: If the child is an internal node and has not enough children, merge child with siblings.
    /// 5: Flush down to child.
    /// 6: If child is leaf and too small, merge with siblings.
    /// 7: If child is leaf and too large, split.
    /// 8: If node is still too large, goto 1.
    /// 9: Set child as node, goto 1.
    /// ```
    pub(super) fn rebalance_tree(
        &self,
        mut node: X::CacheValueRefMut,
        mut parent: Option<DerivateRefNVM<X::CacheValueRefMut, TakeChildBufferWrapper<'static, R>>>,
    ) -> Result<(), Error> {
        loop {
            if !node.is_too_large(self.storage_map) {
                return Ok(());
            }
            debug!(
                "{}, {:?}, lvl: {}, size: {}, actual: {:?}",
                node.kind(),
                node.fanout(),
                node.level(),
                node.size(),
                node.actual_size()
            );
            // 1. Select the largest child buffer which can be flushed.
            let mut child_buffer =
                match DerivateRefNVM::try_new(node, |node| node.try_find_flush_candidate()) {
                    // 1.1. If there is none we have to split the node.
                    Err(_node) => match parent {
                        None => {
                            self.split_root_node(_node);
                            return Ok(());
                        }
                        Some(ref mut parent) => {
                            let (next_node, size_delta) = self.split_node(_node, parent)?;
                            node = next_node;
                            parent.add_size(size_delta);
                            continue;
                        }
                    },
                    // 1.2. If successful we flush in the following steps to this node.
                    Ok(selected_child_buffer) => selected_child_buffer,
                };

            let mut child = self.get_mut_node(child_buffer.child_pointer_mut())?;

            // 2. Iterate down to child if too large
            if !child.is_leaf() && child.is_too_large(self.storage_map) {
                warn!("Aborting flush, child is too large already");
                parent = Some(child_buffer);
                node = child;
                continue;
            }
            // 3. If child is internal, small and has not many children -> merge the children of node.
            if child.has_too_low_fanout() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge(&self.dml, self.tree_id());
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    let is_right_sibling = m.is_right_sibling();
                    let MergeChildResult {
                        pivot_key,
                        old_np,
                        size_delta,
                    } = m.merge_children(&self.dml);
                    if is_right_sibling {
                        let size_delta = child.merge(&mut sibling, pivot_key);
                        child.add_size(size_delta);
                    } else {
                        let size_delta = sibling.merge(&mut child, pivot_key);
                        child.add_size(size_delta);
                    }
                    self.dml.remove(old_np);
                    size_delta
                };
                child_buffer.add_size(size_delta);
                node = child_buffer.into_owner();
                continue;
            }
            // 4. Remove messages from the child buffer.
            let (buffer, size_delta) = match &mut *child_buffer {
                TakeChildBufferWrapper::TakeChildBuffer(obj) => obj.take_buffer(),
                TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => {
                    let mut cbuf = self.get_mut_node(obj.buffer_pointer_mut())?;
                    let (bmap, size_delta) = cbuf.assert_buffer_mut().take();
                    obj.add_size(-(size_delta as isize));
                    (bmap, -(size_delta as isize))
                }
            };
            child_buffer.add_size(size_delta);
            self.dml.verify_cache();
            // 5. Insert messages from the child buffer into the child.
            let size_delta_child =
                child.insert_msg_buffer(buffer, self.msg_action(), &self.dml, self.tree_id());
            child.add_size(size_delta_child);

            // 6. Check if minimal leaf size is fulfilled, otherwise merge again.
            if child.is_too_small_leaf() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge(&self.dml, self.tree_id());
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    let left;
                    let right;
                    if m.is_right_sibling() {
                        left = &mut child;
                        right = &mut sibling;
                    } else {
                        left = &mut sibling;
                        right = &mut child;
                    };
                    match left.leaf_rebalance(right) {
                        FillUpResult::Merged { size_delta } => {
                            left.add_size(size_delta);
                            right.add_size(-size_delta);
                            let MergeChildResult {
                                old_np, size_delta, ..
                            } = m.merge_children(&self.dml);
                            self.dml.remove(old_np);
                            size_delta
                        }
                        FillUpResult::Rebalanced {
                            pivot_key,
                            size_delta,
                        } => {
                            left.add_size(size_delta);
                            right.add_size(-size_delta);
                            m.rebalanced(pivot_key, &self.dml)
                        }
                    }
                };
                child_buffer.add_size(size_delta);
            }
            // 7. If the child is too large, split until it is not.
            while child.is_too_large_leaf(self.storage_map) {
                let (next_node, size_delta) = self.split_node(child, &mut child_buffer)?;
                child_buffer.add_size(size_delta);
                child = next_node;
            }

            // 8. After finishing all operations once, see if they have to be repeated.
            if child_buffer.size() > super::MAX_INTERNAL_NODE_SIZE {
                warn!("Node is still too large");
                if child.is_too_large(self.storage_map) {
                    warn!("... but child, too");
                }
                node = child_buffer.into_owner();
                continue;
            }
            // 9. Traverse down to child.
            // Drop old parent here.
            parent = Some(child_buffer);
            node = child;
        }
    }
}
