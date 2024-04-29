//! This module provides `MessageAction` which can be used to override the
//! default message type to build custom message actions.
//! These can have a custom payload and may perform arbitrary
//! computation on message application.

use crate::cow_bytes::SlicedCowBytes;
use std::{fmt::Debug, ops::Deref};

/// Defines the action of a message.
pub trait MessageAction: Clone + Debug + Send + Sync {
    /// Applies the message `msg`. `data` holds the current data.
    fn apply(&self, key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>);

    /// Applies the message `msg` to a leaf entry. `data` holds the current
    /// data.
    fn apply_to_leaf(&self, key: &[u8], msg: SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        self.apply(key, &msg, data)
    }

    /// Merges two messages.
    fn merge(
        &self,
        key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes;
}

impl<T: Deref + Debug + Send + Sync + Clone> MessageAction for T
where
    T::Target: MessageAction,
{
    fn apply(&self, key: &[u8], msg: &SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        (**self).apply(key, msg, data)
    }
    fn apply_to_leaf(&self, key: &[u8], msg: SlicedCowBytes, data: &mut Option<SlicedCowBytes>) {
        (**self).apply_to_leaf(key, msg, data)
    }
    fn merge(
        &self,
        key: &[u8],
        upper_msg: SlicedCowBytes,
        lower_msg: SlicedCowBytes,
    ) -> SlicedCowBytes {
        (**self).merge(key, upper_msg, lower_msg)
    }
}
