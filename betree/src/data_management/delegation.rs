use crate::{database::DatasetId, tree::PivotKey};

use super::{Dml, Error};
use std::ops::{Deref, DerefMut};

impl<T> Dml for T
where
    T: Deref,
    T::Target: Dml,
{
    type ObjectRef = <T::Target as Dml>::ObjectRef;
    type ObjectPointer = <T::Target as Dml>::ObjectPointer;
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

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error> {
        (**self).get(or)
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: DatasetId,
    ) -> Result<Self::CacheValueRefMut, Error> {
        (**self).get_mut(or, info)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        (**self).try_get_mut(or)
    }

    fn insert(&self, object: Self::Object, info: DatasetId, pk: PivotKey) -> Self::ObjectRef {
        (**self).insert(object, info, pk)
    }

    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: DatasetId,
        pk: PivotKey,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        (**self).insert_and_get_mut(object, info, pk)
    }

    fn remove(&self, or: Self::ObjectRef) {
        (**self).remove(or)
    }

    fn get_and_remove(&self, or: Self::ObjectRef) -> Result<Self::Object, Error> {
        (**self).get_and_remove(or)
    }

    fn evict(&self) -> Result<(), Error> {
        (**self).evict()
    }

    fn verify_cache(&self) {
        (**self).verify_cache()
    }

    fn root_ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        <T::Target as Dml>::root_ref_from_ptr(r)
    }

    fn write_back<F, G>(&self, acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut() -> G,
        G: DerefMut<Target = Self::ObjectRef>,
    {
        (**self).write_back(acquire_or_lock)
    }

    type Prefetch = <T::Target as Dml>::Prefetch;

    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error> {
        (**self).prefetch(or)
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<Self::CacheValueRef, Error> {
        (**self).finish_prefetch(p)
    }

    fn cache_stats(&self) -> Self::CacheStats {
        (**self).cache_stats()
    }

    fn drop_cache(&self) {
        (**self).drop_cache();
    }
}
