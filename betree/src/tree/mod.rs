//! This module provides a B<sup>e</sup>-Tree on top of the Data Management
//! Layer.

mod default_message_action;
mod errors;
mod imp;
mod layer;
mod message_action;

use crate::cow_bytes::{CowBytes, SlicedCowBytes};

pub use self::{
    default_message_action::DefaultMessageAction,
    imp::{Inner, Node, Tree},
    layer::TreeLayer,
    message_action::MessageAction,
};

pub use self::errors::TreeError;

type Key = CowBytes;
type Value = SlicedCowBytes;

use self::imp::KeyInfo;
pub(crate) use self::{imp::MAX_MESSAGE_SIZE, layer::ErasedTreeSync};
