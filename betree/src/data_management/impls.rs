use super::{object_ptr::ObjectPointer, HasStoragePreference};
use crate::{
    database::Generation,
    size::{StaticSize},
    storage_pool::DiskOffset,
    tree::PivotKey,
    StoragePreference,
};
use serde::{
    de::DeserializeOwned, ser::Error as SerError,
};

use rkyv::ser::Serializer;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ModifiedObjectId {
    pub(super) id: u64,
    pub(super) pref: StoragePreference,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
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
    ObjectPointer<D>: serde::Serialize + DeserializeOwned + StaticSize + Clone,
{
    type ObjectPointer = ObjectPointer<D>;
    fn get_unmodified(&self) -> Option<&ObjectPointer<D>> {
        if let ObjRef::Unmodified(ref p, ..) | ObjRef::Incomplete(ref p) = self {
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
            ObjRef::Unmodified(_, o_pk) | ObjRef::Modified(_, o_pk) => *o_pk = pk,
            // NOTE: An object reference may never need to be modified when
            // performing a write back.
            ObjRef::InWriteback(..) => unreachable!(),
        }
    }

    fn index(&self) -> &PivotKey {
        match self {
            ObjRef::Incomplete(_) => unreachable!(),
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

    /// Create an `ObjRef` from the given to the `ObjectPointer` under the
    /// assumption that the pointer belongs to a root object.
    pub fn root_ref_from_obj_ptr(ptr: ObjectPointer<D>) -> Self {
        let d_id = ptr.info;
        ObjRef::Unmodified(ptr, PivotKey::Root(d_id))
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

impl<P: serde::Serialize> serde::Serialize for ObjRef<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
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

impl<'de, D> serde::Deserialize<'de> for ObjRef<ObjectPointer<D>>
where
    ObjectPointer<D>: serde::Deserialize<'de>,
{
    fn deserialize<E>(deserializer: E) -> Result<Self, E::Error>
    where
        E: serde::Deserializer<'de>,
    {
        ObjectPointer::<D>::deserialize(deserializer).map(ObjRef::Incomplete)
    }
}
