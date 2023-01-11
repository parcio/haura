use super::{object_ptr::ObjectPointer, HasStoragePreference};
use crate::{
    cache::AddSize, database::Generation, size::StaticSize, storage_pool::DiskOffset,
    StoragePreference, tree::PivotKey,
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{
    de::DeserializeOwned, ser::Error as SerError, Deserialize, Deserializer, Serialize, Serializer,
};
use stable_deref_trait::StableDeref;
use std::{
    mem::{transmute, ManuallyDrop},
    ops::{Deref, DerefMut},
};

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ModifiedObjectId {
    pub(super) id: u64,
    pub(super) pref: StoragePreference,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum ObjectKey<G> {
    Unmodified { offset: DiskOffset, generation: G },
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

// @jwuensche: Removed Copy due to restrictions of CowBytes in the PivotKey.
/// Definitive variant of the states an object in a tree may inhabit.
///
/// TODO: Fix possibility of invalid Unmodified state through proper types. The
/// pivot key may never *actually* be None, only after serializing this field is
/// filled. This actions is only ever needed when we deserialize from disk
/// properly or create a new instance.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum ObjRef<P> {
    Incomplete(P),
    Unmodified(P, PivotKey),
    Modified(ModifiedObjectId, PivotKey),
    InWriteback(ModifiedObjectId, PivotKey),
}

impl<D> super::ObjectReference for ObjRef<ObjectPointer<D>>
where
    D: std::fmt::Debug + 'static,
    ObjectPointer<D>: Serialize + DeserializeOwned + StaticSize + Clone,
{
    type ObjectPointer = ObjectPointer<D>;
    fn get_unmodified(&self) -> Option<&ObjectPointer<D>> {
        if let ObjRef::Unmodified(ref p, ..) = self {
            Some(p)
        } else {
            None
        }
    }

    fn set_index(&mut self, pk: PivotKey) {
        // Transfer from an invalid object reference to a valid one.
        // if let ObjRef::Incomplete(ref p) = self {
        //     *self = ObjRef::Unmodified(p.clone(), pk);
        // }
        match self {
            ObjRef::Incomplete(ref p) => *self = ObjRef::Unmodified(p.clone(), pk),
            ObjRef::Unmodified(_, o_pk) | ObjRef::Modified(_,o_pk ) => *o_pk = pk,
            // NOTE: An object reference may never need to be modified when
            // performing a write back.
            ObjRef::InWriteback(..) => unreachable!(),
        }

    }

    fn index(&self) -> &PivotKey {
        match self {
            ObjRef::Incomplete(p) => unreachable!(),
            ObjRef::Unmodified(_, pk) | ObjRef::Modified(_, pk) | ObjRef::InWriteback(_, pk) => pk,
        }
    }
}

impl<D> ObjRef<ObjectPointer<D>> {
    pub(super) fn as_key(&self) -> ObjectKey<Generation> {
        match *self {
            ObjRef::Unmodified(ref ptr, ..) => ObjectKey::Unmodified {
                offset: ptr.offset(),
                generation: ptr.generation(),
            },
            ObjRef::Modified(mid, ..) => ObjectKey::Modified(mid),
            ObjRef::InWriteback(mid, ..) => ObjectKey::InWriteback(mid),
            ObjRef::Incomplete(..) => unreachable!(),
        }
    }
}

impl<D> From<ObjectPointer<D>> for ObjRef<ObjectPointer<D>> {
    fn from(ptr: ObjectPointer<D>) -> Self {
        ObjRef::Incomplete(ptr)
    }
}

impl<P: HasStoragePreference> HasStoragePreference for ObjRef<P> {
    fn current_preference(&self) -> Option<StoragePreference> {
        Some(self.correct_preference())
    }

    fn recalculate(&self) -> StoragePreference {
        self.correct_preference()
    }

    fn correct_preference(&self) -> StoragePreference {
        match self {
            ObjRef::Unmodified(p, ..) => p.correct_preference(),
            ObjRef::Modified(mid, ..) | ObjRef::InWriteback(mid, ..) => mid.pref,
            ObjRef::Incomplete(..) => unreachable!(),
        }
    }

    // We do not support encoding in [ObjectRef] at the moment.

    fn system_storage_preference(&self) -> StoragePreference {
        unimplemented!()
    }

    fn set_system_storage_preference(&mut self, _pref: StoragePreference) {
        unimplemented!()
    }
}

impl<P: StaticSize> StaticSize for ObjRef<P> {
    fn static_size() -> usize {
        P::static_size()
    }
}

impl<P: Serialize> Serialize for ObjRef<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ObjRef::Modified(..) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef",
            )),
            ObjRef::InWriteback(..) => Err(S::Error::custom(
                "ObjectRef: Tried to serialize a modified ObjectRef which is currently written back",
            )),
            ObjRef::Incomplete(..) => Err(S::Error::custom("ObjRef: Tried to serialize incomple reference.")),
            // NOTE: Ignore the pivot key as this can be generated while reading a node.
            ObjRef::Unmodified(ref ptr, ..) => ptr.serialize(serializer),
        }
    }
}

impl<'de, D> Deserialize<'de> for ObjRef<ObjectPointer<D>>
where
    ObjectPointer<D>: Deserialize<'de>,
{
    fn deserialize<E>(deserializer: E) -> Result<Self, E::Error>
    where
        E: Deserializer<'de>,
    {
        ObjectPointer::<D>::deserialize(deserializer).map(ObjRef::Incomplete)
    }
}

pub struct CacheValueRef<T, U> {
    head: T,
    guard: ManuallyDrop<U>,
}

impl<T, U> Drop for CacheValueRef<T, U> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
    }
}

impl<T: AddSize, U> AddSize for CacheValueRef<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.head.add_size(size_delta)
    }
}

impl<T, U> CacheValueRef<T, RwLockReadGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    pub(super) fn read(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::read(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

impl<T, U> CacheValueRef<T, RwLockWriteGuard<'static, U>>
where
    T: StableDeref<Target = RwLock<U>>,
{
    pub(super) fn write(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::write(&head)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockReadGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockReadGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &self.guard
    }
}

unsafe impl<T, U> StableDeref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {}

impl<T, U> Deref for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    type Target = U;
    fn deref(&self) -> &U {
        &self.guard
    }
}

impl<T, U> DerefMut for CacheValueRef<T, RwLockWriteGuard<'static, U>> {
    fn deref_mut(&mut self) -> &mut U {
        &mut self.guard
    }
}

pub struct TaggedCacheValue<Val, Tag> {
    value: Val,
    tag: Tag,
}

impl<Val: AddSize, Tag> AddSize for TaggedCacheValue<Val, Tag> {
    fn add_size(&self, size_delta: isize) {
        self.value.add_size(size_delta)
    }
}

impl<Val, Tag> Deref for TaggedCacheValue<Val, Tag> {
    type Target = Val;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
