use super::{object_ptr::ObjectPointer, HasStoragePreference};
use crate::{
    cache::AddSize,
    database::Generation,
    size::StaticSize,
    storage_pool::DiskOffset,
    StoragePreference,
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

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ModifiedObjectId {
    pub(super) id: u64,
    pub(super) pref: StoragePreference,
    pub(super) old_location: Option<DiskOffset>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ObjectKey<G> {
    Unmodified { offset: DiskOffset, generation: G },
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ObjRef<P> {
    Unmodified(P),
    Modified(ModifiedObjectId),
    InWriteback(ModifiedObjectId),
}

impl<D> super::ObjectReference for ObjRef<ObjectPointer<D>>
where
    D: std::fmt::Debug + 'static,
    ObjectPointer<D>: Serialize + DeserializeOwned + StaticSize,
{
    type ObjectPointer = ObjectPointer<D>;
    fn get_unmodified(&self) -> Option<&ObjectPointer<D>> {
        if let ObjRef::Unmodified(ref p) = self {
            Some(p)
        } else {
            None
        }
    }
}

impl<D> ObjRef<ObjectPointer<D>> {
    pub(super) fn as_key(&self) -> ObjectKey<Generation> {
        match *self {
            ObjRef::Unmodified(ref ptr) => ObjectKey::Unmodified {
                offset: ptr.offset(),
                generation: ptr.generation(),
            },
            ObjRef::Modified(mid) => ObjectKey::Modified(mid),
            ObjRef::InWriteback(mid) => ObjectKey::InWriteback(mid),
        }
    }
}

impl<D> From<ObjectPointer<D>> for ObjRef<ObjectPointer<D>> {
    fn from(ptr: ObjectPointer<D>) -> Self {
        ObjRef::Unmodified(ptr)
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
            ObjRef::Unmodified(p) => p.correct_preference(),
            ObjRef::Modified(mid) | ObjRef::InWriteback(mid) => mid.pref,
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
            ObjRef::Unmodified(ref ptr) => ptr.serialize(serializer),
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
        ObjectPointer::<D>::deserialize(deserializer).map(ObjRef::Unmodified)
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
