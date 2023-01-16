use super::{Dml, DmlError};
use std::ops::{Deref, DerefMut};

impl<T> Dml for T
where
    T: Deref,
    T::Target: Dml,
{
    type ObjectRef = <T::Target as Dml>::ObjectRef;
    type ObjectPointer = <T::Target as Dml>::ObjectPointer;
    type Info = <T::Target as Dml>::Info;
    type Object = <T::Target as Dml>::Object;
    type CacheValueRef = <T::Target as Dml>::CacheValueRef;
    type CacheValueRefMut = <T::Target as Dml>::CacheValueRefMut;
    type CacheStats = <T::Target as Dml>::CacheStats;
    type Spl = <T::Target as Dml>::Spl;

    fn spl(&self) -> &Self::Spl {
        (**self).spl()
    }

    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef> {
        (**self).try_get(or)
    }

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, DmlError> {
        (**self).get(or)
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, DmlError> {
        (**self).get_mut(or, info)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        (**self).try_get_mut(or)
    }

    fn insert(&self, object: Self::Object, info: Self::Info) -> Self::ObjectRef {
        (**self).insert(object, info)
    }

    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        (**self).insert_and_get_mut(object, info)
    }

    fn remove(&self, or: Self::ObjectRef) {
        (**self).remove(or)
    }

    fn get_and_remove(&self, or: Self::ObjectRef) -> Result<Self::Object, DmlError> {
        (**self).get_and_remove(or)
    }

    fn evict(&self) -> Result<(), DmlError> {
        (**self).evict()
    }

    fn verify_cache(&self) {
        (**self).verify_cache()
    }

    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        <T::Target as Dml>::ref_from_ptr(r)
    }

    fn write_back<F, G>(&self, acquire_or_lock: F) -> Result<Self::ObjectPointer, DmlError>
    where
        F: FnMut() -> G,
        G: DerefMut<Target = Self::ObjectRef>,
    {
        (**self).write_back(acquire_or_lock)
    }

    type Prefetch = <T::Target as Dml>::Prefetch;

    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, DmlError> {
        (**self).prefetch(or)
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), DmlError> {
        (**self).finish_prefetch(p)
    }

    fn cache_stats(&self) -> Self::CacheStats {
        (**self).cache_stats()
    }

    fn drop_cache(&self) {
        (**self).drop_cache();
    }
}
