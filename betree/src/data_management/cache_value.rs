use crate::{cache::AddSize, size::SizeMut};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use stable_deref_trait::StableDeref;
use std::{
    mem::{transmute, ManuallyDrop},
    ops::{Deref, DerefMut},
};

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

impl<T, U, I> CacheValueRef<T, RwLockReadGuard<'static, U>>
where
    T: StableDeref<Target = TaggedCacheValue<RwLock<U>, I>>,
{
    pub(super) fn read(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::read(&head.value)) };
        CacheValueRef {
            head,
            guard: ManuallyDrop::new(guard),
        }
    }
}

impl<T, U, I> CacheValueRef<T, RwLockWriteGuard<'static, U>>
where
    T: StableDeref<Target = TaggedCacheValue<RwLock<U>, I>>,
{
    pub(super) fn write(head: T) -> Self {
        let guard = unsafe { transmute(RwLock::write(&head.value)) };
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

impl<Val, Tag> TaggedCacheValue<Val, Tag> {
    pub fn new(value: Val, tag: Tag) -> Self {
        Self { value, tag }
    }

    pub fn value(&self) -> &Val {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut Val {
        &mut self.value
    }

    pub fn into_value(self) -> Val {
        self.value
    }

    pub fn tag(&self) -> &Tag {
        &self.tag
    }
}

impl<Val: AddSize, Tag> AddSize for TaggedCacheValue<Val, Tag> {
    fn add_size(&self, size_delta: isize) {
        self.value.add_size(size_delta)
    }
}

impl<Val: SizeMut, Tag> SizeMut for TaggedCacheValue<Val, Tag> {
    fn size(&mut self) -> usize {
        self.value.size()
    }

    fn cache_size(&mut self) -> usize {
        self.value.cache_size()
    }
}
