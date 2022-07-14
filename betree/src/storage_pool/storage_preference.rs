use serde::{Deserialize, Serialize};
use std::{
    cmp,
    sync::atomic::{AtomicU8, Ordering},
};

/// An allocation preference. If a [StoragePreference] other than [StoragePreference::NONE]
/// is used for an operation, the allocator will try to allocate on that storage class,
/// but success is not guaranteed.
///
/// A value of [StoragePreference::NONE] implies that the caller doesn't care about which storage
/// class is used, and that the database should fall back to a more general preference, e.g.
/// per-dataset, or the global default.
///
/// The different class constants are vaguely named `FASTEST`, `FAST`, `SLOW`, and `SLOWEST`,
/// but a [StoragePreference] can also be created with [StoragePreference::new].
///
/// The exact properties of a storage layer depend on the database administrator, who is assumed
/// to ensure that the vague ordering properties hold for the given deployment.
///
/// This type is not an `Option<u8>`, because it saves one byte per value, and allows the
/// implementation of convenience methods on itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct StoragePreference(u8);
impl StoragePreference {
    /// No preference, any other preference overrides this.
    pub const NONE: Self = Self(u8::MAX - 1);
    /// The fastest storage class (0).
    pub const FASTEST: Self = Self(0);
    /// The second-fastest storage class (1).
    pub const FAST: Self = Self(1);
    /// The third-fastest, or second-slowest, storage class (2).
    pub const SLOW: Self = Self(2);
    /// The slowest storage class (3).
    pub const SLOWEST: Self = Self(3);

    /// Construct a new [StoragePreference], for a given class.
    /// Panics if `class > 3`.
    pub fn new(class: u8) -> Self {
        assert!(class <= 3);
        Self(class)
    }

    /// Similar to [Option::or], chooses the not-NONE preference out of the two given preferences.
    /// Returns NONE if both are NONE.
    pub fn or(self, other: Self) -> Self {
        if self == Self::NONE {
            other
        } else {
            self
        }
    }

    /// Choose the faster preference out of the two given preferences, preferring any specified
    /// class over NONE.
    pub fn choose_faster(a: Self, b: Self) -> Self {
        // Only works if NONE stays larger than any actual class
        Self(a.0.min(b.0))
    }

    /// Convert to an [Option], returns None if self is NONE, otherwise Some.
    pub fn preferred_class(self) -> Option<u8> {
        if self == Self::NONE {
            None
        } else {
            Some(self.0)
        }
    }

    pub(crate) fn as_u8(self) -> u8 {
        self.0
    }
    pub(crate) fn from_u8(u: u8) -> Self {
        debug_assert!(u == u8::MAX - 1 || u <= 3);
        Self(u)
    }

    pub(crate) fn upgrade(&mut self, other: StoragePreference) {
        *self = StoragePreference::choose_faster(*self, other);
    }

    pub(crate) fn lift(self) -> Option<StoragePreference> {
        match self {
            Self::NONE => None,
            _ => Some(Self(self.0.saturating_sub(1))),
        }
    }

    pub(crate) fn lower(self) -> Option<StoragePreference> {
        match self {
            Self::NONE => None,
            Self::SLOWEST => Some(Self::SLOWEST),
            _ => Some(Self(self.0.saturating_add(1))),
        }
    }
}

// Ordered by `strictness`, so 0 < 1 < 2 < 3 < None.
// Implemented separately instead of derived, to comment
// and error on some changes to struct items.
impl PartialOrd for StoragePreference {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        // This works as long as NONE.0 is larger than 3
        self.0.partial_cmp(&other.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// An atomic version of [StoragePreference], replacing a RwLock<Option<StoragePreference>> by
/// using the additional variant [AtomicStoragePreference::UNKNOWN] in place of None.
pub struct AtomicStoragePreference(AtomicU8);

#[allow(missing_docs)]
impl AtomicStoragePreference {
    pub const UNKNOWN: Self = Self(AtomicU8::new(u8::MAX));

    pub fn known(class: StoragePreference) -> Self {
        Self(AtomicU8::new(class.0))
    }

    pub fn as_option(&self) -> Option<StoragePreference> {
        let v = self.0.load(Ordering::SeqCst);

        if v == u8::MAX {
            None
        } else {
            Some(StoragePreference(v))
        }
    }

    pub fn unwrap_or_none(&self) -> StoragePreference {
        self.as_option().unwrap_or(StoragePreference::NONE)
    }

    pub fn set(&self, pref: StoragePreference) {
        self.0.store(pref.0, Ordering::SeqCst);
    }

    pub fn invalidate(&self) {
        self.0.store(u8::MAX, Ordering::SeqCst);
    }

    pub fn upgrade(&self, other: StoragePreference) {
        self.0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| {
                if p != u8::MAX {
                    let mut sp = StoragePreference(p);
                    sp.upgrade(other);
                    Some(sp.0)
                } else {
                    Some(p)
                }
            })
            .unwrap();
    }

    pub fn upgrade_atomic(&self, other: &AtomicStoragePreference) {
        self.0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| {
                // only track changes if in a known state
                if p != u8::MAX {
                    let mut sp = StoragePreference(p);

                    let other_p = other.0.load(Ordering::SeqCst);
                    if other_p != u8::MAX {
                        sp.upgrade(StoragePreference(other_p));
                    }
                    Some(sp.0)
                } else {
                    Some(p)
                }
            })
            .unwrap();
    }
}

impl Clone for AtomicStoragePreference {
    fn clone(&self) -> Self {
        AtomicStoragePreference(AtomicU8::new(self.0.load(Ordering::SeqCst)))
    }
}

impl PartialEq for AtomicStoragePreference {
    fn eq(&self, rhs: &AtomicStoragePreference) -> bool {
        self.0.load(Ordering::SeqCst) == rhs.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pref_choose_faster() {
        use super::StoragePreference as S;
        assert_eq!(S::choose_faster(S::SLOWEST, S::FASTEST), S::FASTEST);
        assert_eq!(S::choose_faster(S::FASTEST, S::NONE), S::FASTEST);
        assert_eq!(S::choose_faster(S::NONE, S::SLOWEST), S::SLOWEST);
    }
}
