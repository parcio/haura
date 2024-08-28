//! Iterator over a range of keys in a [Tree].
use super::{
    node::{GetRangeResult, Node},
    Inner, Tree,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{Dml, HasStoragePreference, ObjectReference},
    tree::{errors::*, Key, KeyInfo, MessageAction, Value},
};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, Bound, VecDeque},
    mem::replace,
    ops::RangeBounds,
};

fn increment_pivot_key(v: &mut Vec<u8>) {
    v.push(0);
}

#[derive(Debug, Clone, Copy)]
enum Bounded<T> {
    Included(T),
    Excluded(T),
}

/// The range iterator over (key,value)-tuples of a tree.
///
/// The iterator performs asynchronous prefetching to allow for a better
/// utilization of underlying resources. It is advised to use [RangeIterator]
/// and methods utilizing it in almost all cases.
pub struct RangeIterator<X: Dml, M, I: Borrow<Inner<X::ObjectRef, M>>> {
    buffer: VecDeque<(Key, (KeyInfo, Value))>,
    min_key: Bounded<Vec<u8>>,
    /// Always inclusive
    max_key: Option<Vec<u8>>,
    tree: Tree<X, M, I>,
    finished: bool,
    prefetch_node: Option<X::Prefetch>,
    prefetch_buffer: Option<X::Prefetch>,
}

impl<X, R, M, I> Iterator for RangeIterator<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    type Item = Result<(Key, Value), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((key, (_keyinfo, data))) = self.buffer.pop_front() {
                return Some(Ok((key, data)));
            } else if self.finished {
                return None;
            } else if let Err(e) = self.fill_buffer() {
                self.finished = true;
                return Some(Err(e));
            }
        }
    }
}

impl<X, R, M, I> RangeIterator<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    pub(super) fn new<K, T>(range: T, tree: Tree<X, M, I>) -> Self
    where
        T: RangeBounds<K>,
        K: Borrow<[u8]>,
    {
        let min_key = match range.start_bound() {
            Bound::Unbounded => Bounded::Included(Vec::new()),
            Bound::Included(x) => Bounded::Included(x.borrow().to_vec()),
            Bound::Excluded(x) => Bounded::Excluded(x.borrow().to_vec()),
        };
        let max_key = match range.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(x) => Some(x.borrow().to_vec()),
            Bound::Excluded(x) => {
                let mut v = x.borrow().to_vec();
                increment_pivot_key(&mut v);
                Some(v)
            }
        };

        RangeIterator {
            min_key,
            max_key,
            tree,
            finished: false,
            buffer: VecDeque::new(),
            prefetch_node: None,
            prefetch_buffer: None,
        }
    }

    fn fill_buffer(&mut self) -> Result<(), Error> {
        let next_pivot = {
            let min_key = match self.min_key {
                Bounded::Included(ref x) | Bounded::Excluded(ref x) => x,
            };
            self.tree
                .leaf_range_query(min_key, &mut self.buffer, &mut self.prefetch_node, &mut self.prefetch_buffer)?
        };

        // Strip entries which are out of bounds from the buffer.
        while self
            .buffer
            .front()
            .map(|(key, _)| match self.min_key {
                Bounded::Included(ref min_key) => key < min_key,
                Bounded::Excluded(ref min_key) => key <= min_key,
            })
            .unwrap_or_default()
        {
            self.buffer.pop_front().unwrap();
        }
        if let Some(ref max_key) = self.max_key {
            while self
                .buffer
                .back()
                .map(|(key, _)| key > max_key)
                .unwrap_or_default()
            {
                self.buffer.pop_back().unwrap();
            }
            next_pivot
                .as_ref()
                .map(|pivot| self.finished = pivot >= max_key);
        }

        match next_pivot {
            // If we have not encountered any entry larger than max key, we will
            // update our min key for the next fill_buffer call.
            Some(pivot) if !self.finished => {
                let mut last_key = pivot.to_vec();
                // `last_key` is actually exact.
                // There are no values on this path we have not seen.
                increment_pivot_key(&mut last_key);
                self.min_key = Bounded::Excluded(last_key);
            }
            // If there is no pivot key then this was the right-most leaf in the
            // tree.
            None => {
                self.finished = true;
            }
            _ => {}
        }

        Ok(())
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectReference<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, M>>,
{
    fn leaf_range_query(
        &self,
        key: &[u8],
        data: &mut VecDeque<(CowBytes, (KeyInfo, SlicedCowBytes))>,
        prefetch_node: &mut Option<X::Prefetch>,
        prefetch_buffer: &mut Option<X::Prefetch>,
    ) -> Result<Option<CowBytes>, Error> {
        let result = {
            let mut left_pivot_key = None;
            let mut right_pivot_key = None;
            let mut messages = BTreeMap::new();

            // First, we gather all messages for the given key and its value in the leaf.
            let mut node = self.get_root_node()?;

            loop {
                let next_node = match node.get_range(
                    key,
                    &mut left_pivot_key,
                    &mut right_pivot_key,
                    &mut messages,
                ) {
                    GetRangeResult::NextNode {
                        child_buffer,
                        np,
                        prefetch_option_node,
                        prefetch_option_additional,
                    } => {
                        let previous_prefetch_node = if let Some(prefetch_np) = prefetch_option_node {
                            let f = self.dml.prefetch(&prefetch_np.read())?;
                            replace(prefetch_node, f)
                        } else {
                            prefetch_node.take()
                        };

                        if let Some(previous_prefetch) = previous_prefetch_node {
                            self.dml.finish_prefetch(previous_prefetch)?
                        } else {
                            self.get_node(np)?
                        }
                    }
                    GetRangeResult::Data(leaf_entries) => {
                        self.apply_messages(
                            &left_pivot_key,
                            &right_pivot_key,
                            messages,
                            leaf_entries,
                            data,
                        );
                        break Ok(right_pivot_key);
                    }
                };
                node = next_node;
            }
        };

        if self.evict {
            self.dml.evict()?;
        }
        result
    }

    fn apply_messages<'a, J>(
        &self,
        left_pivot_key: &Option<CowBytes>,
        right_pivot_key: &Option<CowBytes>,
        messages: BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
        leaf_entries: J,
        data: &mut VecDeque<(CowBytes, (KeyInfo, SlicedCowBytes))>,
    ) where
        J: Iterator<Item = (&'a [u8], (KeyInfo, SlicedCowBytes))>,
    {
        // disregard any messages with keys outside of
        // left_pivot_key..right_pivot_key.
        let msgs_iter = messages
            .into_iter()
            .skip_while(|(key, _)| match *left_pivot_key {
                None => false,
                Some(ref min_key) => key < min_key,
            })
            .take_while(|(key, _)| match *right_pivot_key {
                None => true,
                Some(ref max_key) => key <= max_key,
            });
        let leaf_entries = leaf_entries.map(|(k, v)| (CowBytes::from(k), v));

        for (key, msgs, value) in MergeByKeyIterator::new(msgs_iter, leaf_entries) {
            let (mut keyinfo, mut value) = match value {
                Some((keyinfo, value)) => (Some(keyinfo), Some(value)),
                None => (None, None),
            };

            if let Some(msgs) = msgs {
                for (new_keyinfo, msg) in msgs.into_iter().rev() {
                    if let Some(previous_keyinfo) = keyinfo {
                        keyinfo = Some(previous_keyinfo.merge_with_upper(new_keyinfo));
                    } else {
                        keyinfo = Some(new_keyinfo)
                    }

                    self.msg_action().apply(&key, &msg, &mut value);
                }
            }
            if let Some(value) = value {
                // Unwrap is safe here, keyinfo can only be initially None if value
                // is also None. And on every occasion where value can become Some,
                // keyinfo has already been set to Some.
                data.push_back((key, (keyinfo.unwrap(), value)));
            }
        }
    }
}

struct MergeByKeyIterator<I: Iterator, J: Iterator> {
    i: I,
    j: J,
    peeked_i: Option<I::Item>,
    peeked_j: Option<J::Item>,
}

impl<I: Iterator, J: Iterator> MergeByKeyIterator<I, J> {
    pub fn new<A, B>(i: A, j: B) -> Self
    where
        A: IntoIterator<Item = I::Item, IntoIter = I>,
        B: IntoIterator<Item = J::Item, IntoIter = J>,
    {
        MergeByKeyIterator {
            i: i.into_iter(),
            j: j.into_iter(),
            peeked_i: None,
            peeked_j: None,
        }
    }
}

impl<I, J, K, T, U> Iterator for MergeByKeyIterator<I, J>
where
    I: Iterator<Item = (K, T)>,
    J: Iterator<Item = (K, U)>,
    K: Ord,
{
    type Item = (K, Option<T>, Option<U>);

    fn next(&mut self) -> Option<Self::Item> {
        use std::cmp::Ordering;
        let i = self.peeked_i.take().or_else(|| self.i.next());
        let j = self.peeked_j.take().or_else(|| self.j.next());

        let (k_i, v_i, k_j, v_j) = match (i, j) {
            (None, None) => return None,
            (Some((k, v)), None) => return Some((k, Some(v), None)),
            (None, Some((k, v))) => return Some((k, None, Some(v))),
            (Some((k_i, v_i)), Some((k_j, v_j))) => (k_i, v_i, k_j, v_j),
        };

        Some(match k_i.cmp(&k_j) {
            Ordering::Equal => (k_i, Some(v_i), Some(v_j)),
            Ordering::Less => {
                self.peeked_j = Some((k_j, v_j));
                (k_i, Some(v_i), None)
            }
            Ordering::Greater => {
                self.peeked_i = Some((k_i, v_i));
                (k_j, None, Some(v_j))
            }
        })
    }
}
