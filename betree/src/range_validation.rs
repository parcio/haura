//! A valid range is inclusively non-empty, an invalid range contains no possible elements.

use std::{borrow::Borrow, ops::RangeBounds};

pub(crate) fn is_inclusive_non_empty<'t, B: RangeBounds<T>, T: Borrow<[u8]>>(bound: &B) -> bool {
    use std::ops::Bound::*;

    match (bound.start_bound(), bound.end_bound()) {
        (Unbounded, _) | (_, Unbounded) => true,
        (Included(a), Included(b)) => a.borrow() <= b.borrow(),
        (Excluded(a), Included(b)) | (Included(a), Excluded(b)) | (Excluded(a), Excluded(b)) => {
            a.borrow() < b.borrow()
        }
    }
}
