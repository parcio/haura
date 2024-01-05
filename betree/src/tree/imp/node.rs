//! Implementation of the generic node wrapper.
use self::Inner::*;
use super::{
    child_buffer::ChildBuffer,
    nvm_child_buffer::NVMChildBuffer,
    internal::{InternalNode, TakeChildBuffer, self},
    nvminternal::{NVMInternalNode, NVMTakeChildBuffer, self},
    leaf::LeafNode,
    nvmleaf::{NVMLeafNode, NVMLeafNodeMetaData, NVMLeafNodeData, self},
    packed::PackedMap,
    nvmleaf::NVMFillUpResult,
    FillUpResult, KeyInfo, PivotKey, MAX_INTERNAL_NODE_SIZE, MAX_LEAF_NODE_SIZE, MIN_FANOUT,
    MIN_FLUSH_SIZE, MIN_LEAF_NODE_SIZE,
};
use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{Dml, HasStoragePreference, Object, ObjectReference},
    database::{DatasetId,RootSpu},
    size::{Size, SizeMut, StaticSize},
    storage_pool::{DiskOffset, StoragePoolLayer},
    tree::{pivot_key::LocalPivotKey, MessageAction, imp::{/*leaf::ArchivedNVMLeafNode,*/ nvminternal::{InternalNodeMetaData, ArchivedInternalNodeMetaData, ArchivedInternalNodeData, InternalNodeData}}},
    StoragePreference,
};
use bincode::{deserialize, serialize_into};
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    io::{self, Write},
    mem::replace,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH}
};

use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, ScratchSpace, Serializer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
    Archive, Archived, Deserialize, Fallible, Infallible, Serialize,
};

//pub(crate) type RootSpu = crate::storage_pool::StoragePoolUnit<crate::checksum::XxHash>;

/// The tree node type.
#[derive(Debug)]
pub struct Node<N: 'static>(Inner<N>);

#[derive(Debug)]
pub(super) enum Inner<N: 'static> {
    PackedLeaf(PackedMap),
    Leaf(LeafNode),
    NVMLeaf(NVMLeafNode),
    Internal(InternalNode<N>),
    NVMInternal(NVMInternalNode<N>),
}

pub(super) enum TakeChildBufferWrapper<'a, N: 'a + 'static> {
    TakeChildBuffer(Option<TakeChildBuffer<'a, N>>),
    NVMTakeChildBuffer(Option<NVMTakeChildBuffer<'a, N>>),
}


impl<'a, N: Size + HasStoragePreference> TakeChildBufferWrapper<'a, N> {
    pub fn node_pointer_mut(&mut self) -> &mut TakeChildBufferWrapper<'a, N>  where N: ObjectReference{
        self
        // TODO: Karim... add comments...
        // match self {
        //     TakeChildBufferWrapper::TakeChildBuffer(obj) => {
        //         println!("2...........................................");
        //         obj.as_mut().unwrap().node_pointer_mut()
        //     },
        //     TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => {
        //         //let (a,b) = obj.as_mut().unwrap().node_pointer_mut();
        //         //&mut self.node.data.write().as_mut().unwrap().as_mut().unwrap().children[self.child_idx].as_mut().unwrap().node_pointer
        //         //obj.as_mut().unwrap().node_pointer_mut()
        //         unimplemented!("..")
        //     },
        // }
    }

    pub fn take_buffer(&mut self) -> (BTreeMap<CowBytes, (KeyInfo, SlicedCowBytes)>, isize) where N: ObjectReference{
        match self {
            TakeChildBufferWrapper::TakeChildBuffer(obj) => {
                println!("22...........................................");
                obj.as_mut().unwrap().take_buffer()
            },
            TakeChildBufferWrapper::NVMTakeChildBuffer(obj) => {
                obj.as_mut().unwrap().take_buffer()
            },
        }

    }
}

use std::iter::Map;

/*trait CBIteratorTrait<'a, N> {
    fn get_iterator(&'a mut self) -> Box<dyn Iterator<Item = &'a mut N> + 'a>;
    fn get_iterator2(&'a self) -> Box<dyn Iterator<Item = &'a  N> + 'a>;
    fn get_iterator3(self) -> Box<dyn Iterator<Item = N> + 'a>;
}

impl<'a, N> CBIteratorTrait<'a, ChildBuffer<N>> for Vec<ChildBuffer<N>> {
    fn get_iterator(&'a mut self) -> Box<dyn Iterator<Item = &'a mut ChildBuffer<N>> + 'a> {
        //Box::new(self.iter_mut().map(|child| child.node_pointer.get_mut()))
        Box::new(self.iter_mut())
    }

    fn get_iterator2(&'a self) -> Box<dyn Iterator<Item = &'a ChildBuffer<N>> + 'a> {
        //Box::new(self.iter_mut().map(|child| child.node_pointer.get_mut()))
        Box::new(self.iter())
    }

    fn get_iterator3(self) -> Box<dyn Iterator<Item = ChildBuffer<N>> + 'a> {
        //Box::new(self.iter_mut().map(|child| child.node_pointer.get_mut()))
        Box::new(self.into_iter())
    }

}

impl<'a, N> CBIteratorTrait<'a, Option<NVMChildBuffer<N>>> for Vec<Option<NVMChildBuffer<N>>> {
    fn get_iterator(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Option<NVMChildBuffer<N>>> + 'a> {
        //Box::new(self.iter_mut().flat_map(|x| x.as_mut()).map(|x| x.node_pointer.get_mut()))
        Box::new(self.iter_mut())
    }

    fn get_iterator2(&'a self) -> Box<dyn Iterator<Item = &'a Option<NVMChildBuffer<N>>> + 'a> {
        //Box::new(self.iter_mut().flat_map(|x| x.as_mut()).map(|x| x.node_pointer.get_mut()))
        Box::new(self.iter())
    }

    fn get_iterator3(self) -> Box<dyn Iterator<Item = Option<NVMChildBuffer<N>>> + 'a> {
        //Box::new(self.iter_mut().flat_map(|x| x.as_mut()).map(|x| x.node_pointer.get_mut()))
        Box::new(self.into_iter())
    }

}
*/
pub(super) enum ChildBufferIterator<'a, N: 'a + 'static> {
    ChildBuffer(Option<Box<dyn Iterator<Item = &'a mut N> + 'a>>),
    NVMChildBuffer(&'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>),
}

pub(super) enum ChildBufferIterator3<'a, N> {
    ChildBuffer(Option<Box<dyn Iterator<Item = N> + 'a>>),
    NVMChildBuffer(Option<Box<dyn Iterator<Item = N> + 'a>>),
}

pub(super) enum ChildBufferIterator2<'a, N> {
    ChildBuffer(Option<Box<dyn Iterator<Item = &'a RwLock<N>> + 'a>>),
    NVMChildBuffer(Option<Box<dyn Iterator<Item = &'a RwLock<N>> + 'a>>),
}


/*pub(super) enum ChildBufferIterator<'a, N: 'static> {
    ChildBuffer(Option<Map<impl Iterator<Item = &'a mut ChildBuffer<N>>, fn(&'a mut ChildBuffer<N>) -> &'a mut ChildBuffer<N>>>),
    //NVMChildBuffer(Option<Map<impl Iterator<Item = &'a mut Option<NVMChildBuffer<N>>>, fn(&'a mut Option<NVMChildBuffer<N>>) -> &'a mut Option<NVMChildBuffer<N>>>),

    //ChildBuffer(Option<std::iter::Map<impl Iterator<Item = &mut ChildBuffer<N>>,),
    //NVMChildBuffer(core::slice::IterMut<'a, Option<NVMChildBuffer<N>>>),

    //std::option::Option<std::iter::Map<impl Iterator<Item = &mut ChildBuffer<N>> + '_
//    std::option::Option<std::iter::Map<impl Iterator<Item = &mut std::option::Option<NVMChildBuffer<N>>> + '_
}*/

/*
pub(super) enum ChildBufferWrapper<'a, N: 'static> {
    ChildBuffer(core::slice::IterMut<'a, ChildBuffer<N>>),
    NVMChildBuffer(core::slice::IterMut<'a, NVMChildBuffer<N>>),
}

pub(super) struct ChildBufferWrapperStruct<'a, N: 'static> {
    pub data: ChildBufferWrapper<'a , N>,
}


impl<'a, N> Iterator for ChildBufferWrapperStruct<'a, N> {
    type Item = ChildBufferWrapperStruct<'a, N>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data {
            ChildBufferWrapper::ChildBuffer(_) =>  unimplemented!(""),
            ChildBufferWrapper::NVMChildBuffer(_) => unimplemented!(""),
        }
    }
}
*/
#[derive(Debug)]
enum NodeInnerType {
    Packed = 1,
    Leaf,
    Internal,
    NVMLeaf,
    NVMInternal,
}

impl<R: HasStoragePreference + StaticSize> HasStoragePreference for Node<R> {
    fn current_preference(&self) -> Option<StoragePreference> {
        match self.0 {
            PackedLeaf(_) => None,
            Leaf(ref leaf) => leaf.current_preference(),
            Internal(ref internal) => internal.current_preference(),
            NVMLeaf(ref nvmleaf) =>  nvmleaf.current_preference(),
            NVMInternal(ref nvminternal) => nvminternal.current_preference(),
        }
    }

    fn recalculate(&self) -> StoragePreference {
        match self.0 {
            PackedLeaf(_) => {
                unreachable!("packed leaves are never written back, have no preference")
            }
            Leaf(ref leaf) => leaf.recalculate(),
            Internal(ref internal) => internal.recalculate(),
            NVMLeaf(ref nvmleaf) => nvmleaf.recalculate(),
            NVMInternal(ref nvminternal) => nvminternal.recalculate(),
        }
    }

    fn system_storage_preference(&self) -> StoragePreference {
        match self.0 {
            // A packed leaf does not have a storage preference
            PackedLeaf(_) => unreachable!("packed leaf preference cannot be determined"),
            Leaf(ref leaf) => leaf.system_storage_preference(),
            Internal(ref int) => int.system_storage_preference(),
            NVMLeaf(ref nvmleaf) => nvmleaf.system_storage_preference(),
            NVMInternal(ref nvminternal) => nvminternal.system_storage_preference(),
        }
    }

    fn set_system_storage_preference(&mut self, pref: StoragePreference) {
        // NOTE: This generally has a greater impact as leafs need to be
        // unpacked asap. Another solution as proposed by similar approaches is
        // waiting for the next read operation for this leaf.
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!("packed leaves cannot have their preference updated"),
            Leaf(ref mut leaf) => leaf.set_system_storage_preference(pref),
            Internal(ref mut int) => int.set_system_storage_preference(pref),
            NVMLeaf(ref mut nvmleaf) => nvmleaf.set_system_storage_preference(pref),
            NVMInternal(ref mut nvminternal) => nvminternal.set_system_storage_preference(pref),
        }
    }
}

impl<R: ObjectReference + HasStoragePreference + StaticSize> Object<R> for Node<R> {
    fn pack<W: Write>(&self, mut writer: W, metadata_size: &mut usize) -> Result<(), io::Error> {
        match self.0 {
            PackedLeaf(ref map) => {
                //println!("pack: PackedLeaf ...........................................");

                //writer.write_all((NodeInnerType::Packed as u32).to_be_bytes().as_ref())?;
                writer.write_all(map.inner())
            },
            Leaf(ref leaf) => {
                //println!("pack: Leaf ...........................................");

                writer.write_all((NodeInnerType::Leaf as u32).to_be_bytes().as_ref())?;
                PackedMap::pack(leaf, writer)
            },
            Internal(ref internal) => {
                //println!("pack: Internal ...........................................");

                writer.write_all((NodeInnerType::Internal as u32).to_be_bytes().as_ref())?;
                //writer.write_all(&[0xFFu8, 0xFF, 0xFF, 0xFF] as &[u8])?;
                serialize_into(writer, internal)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            },
            NVMLeaf(ref leaf) => {
                //println!("pack: NVMLeaf ...........................................");

                let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
                serializer_meta_data.serialize_value(&leaf.meta_data).unwrap();
                let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();

                let mut serializer_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
                serializer_data.serialize_value(leaf.data.read().as_ref().unwrap().as_ref().unwrap()).unwrap();
                let bytes_data = serializer_data.into_serializer().into_inner();

                writer.write_all((NodeInnerType::NVMLeaf as u32).to_be_bytes().as_ref())?;
                writer.write_all(bytes_meta_data.len().to_be_bytes().as_ref())?;
                writer.write_all(bytes_data.len().to_be_bytes().as_ref())?;

                writer.write_all(&bytes_meta_data.as_ref())?;
                writer.write_all(&bytes_data.as_ref())?;

                *metadata_size = 4 + 8 + 8 + bytes_meta_data.len(); //TODO: fix this

                Ok(())
            },
            NVMInternal(ref nvminternal) => {
                //println!("pack: NVMInternal ...........................................");

                let mut serializer_meta_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
                serializer_meta_data.serialize_value(&nvminternal.meta_data).unwrap();
                let bytes_meta_data = serializer_meta_data.into_serializer().into_inner();

                let mut serializer_data = rkyv::ser::serializers::AllocSerializer::<0>::default();
                serializer_data.serialize_value(nvminternal.data.read().as_ref().unwrap().as_ref().unwrap()).unwrap();
                let bytes_data = serializer_data.into_serializer().into_inner();

                writer.write_all((NodeInnerType::NVMInternal as u32).to_be_bytes().as_ref())?;
                writer.write_all(bytes_meta_data.len().to_be_bytes().as_ref())?;
                writer.write_all(bytes_data.len().to_be_bytes().as_ref())?;

                writer.write_all(&bytes_meta_data.as_ref())?;
                writer.write_all(&bytes_data.as_ref())?;

                *metadata_size = 4 + 8 + 8 + bytes_meta_data.len();//TODO: fix this


                debug!("NVMInternal node packed successfully"); 

                Ok(())
            },
        }
    }

    fn unpack_at(size: crate::vdev::Block<u32>, checksum: crate::checksum::XxHash, pool: RootSpu, _offset: DiskOffset, d_id: DatasetId, data: Box<[u8]>) -> Result<Self, io::Error> {
        if data[0..4] == (NodeInnerType::Internal as u32).to_be_bytes() {
            //println!("unpack: Internal ...........................................");
                match deserialize::<InternalNode<_>>(&data[4..]) {
                Ok(internal) => Ok(Node(Internal(internal.complete_object_refs(d_id)))),
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        } else if data[0..4] == (NodeInnerType::Leaf as u32).to_be_bytes() {
            //println!("unpack: Leaf ...........................................");

            // storage_preference is not preserved for packed leaves,
            // because they will not be written back to disk until modified,
            // and every modification requires them to be unpacked.
            // The leaf contents are scanned cheaply during unpacking, which
            // recalculates the correct storage_preference for the contained keys.
            Ok(Node(PackedLeaf(PackedMap::new((&data[4..]).to_vec()))))
        } else if data[0..4] == (NodeInnerType::NVMInternal as u32).to_be_bytes() {
            //println!("unpack: NVMInternal ...........................................");

            let meta_data_len: usize = usize::from_be_bytes(data[4..12].try_into().unwrap());
            let data_len: usize = usize::from_be_bytes(data[12..20].try_into().unwrap());

            let meta_data_start = 4 + 8 + 8;
            let meta_data_end = meta_data_start + meta_data_len;   

            let data_start = meta_data_end;
            let data_end = data_start + data_len;   

            let archivedinternalnodemetadata: &ArchivedInternalNodeMetaData = rkyv::check_archived_root::<InternalNodeMetaData>(&data[meta_data_start..meta_data_end]).unwrap();
            //let archivedinternalnode: &ArchivedInternalNode<NVMChildBuffer<_>>  = unsafe { archived_root::<NVMInternalNode<NVMChildBuffer<R>>>(&data[12..len+12]) };
            let meta_data: InternalNodeMetaData = archivedinternalnodemetadata.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let archivedinternalnodedata: &ArchivedInternalNodeData<_> = rkyv::check_archived_root::<InternalNodeData<R>>(&data[data_start..data_end]).unwrap();
            //let archivedinternalnode: &ArchivedInternalNode<NVMChildBuffer<_>>  = unsafe { archived_root::<NVMInternalNode<NVMChildBuffer<R>>>(&data[12..len+12]) };
            let data: InternalNodeData<_> = archivedinternalnodedata.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            
            Ok(Node(NVMInternal (NVMInternalNode {
                pool: Some(pool),
                disk_offset: Some(_offset),
                meta_data : meta_data,
                data: std::sync::Arc::new(std::sync::RwLock::new(Some(InternalNodeData {
                    children: vec![]
                }))), //Some(data),
                meta_data_size: meta_data_len,
                data_size: data_len,
                data_start: data_start,
                data_end: data_end,
                node_size: size,
                checksum: Some(checksum),                
                need_to_load_data_from_nvm: std::sync::RwLock::new(true), //false,
                time_for_nvm_last_fetch: SystemTime::now(),
                nvm_fetch_counter: 0,

            }.complete_object_refs(d_id))))
        } else if data[0..4] == (NodeInnerType::NVMLeaf as u32).to_be_bytes() {
            //println!("unpack: NVMLeaf ...........................................");

            let meta_data_len: usize = usize::from_be_bytes(data[4..12].try_into().unwrap());
            let data_len: usize = usize::from_be_bytes(data[12..20].try_into().unwrap());

            let meta_data_start = 4 + 8 + 8;
            let meta_data_end = meta_data_start + meta_data_len;   

            let data_start = meta_data_end;
            let data_end = data_start + data_len;   

            let archivedleafnodemetadata = rkyv::check_archived_root::<NVMLeafNodeMetaData>(&data[meta_data_start..meta_data_end]).unwrap();
            //let archivedleafnode: &ArchivedNVMLeafNode = unsafe { archived_root::<NVMLeafNode>(&data) };            
            let meta_data:NVMLeafNodeMetaData = archivedleafnodemetadata.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            
            let archivedleafnodedata = rkyv::check_archived_root::<NVMLeafNodeData>(&data[data_start..data_end]).unwrap();
            //let archivedleafnode: &ArchivedNVMLeafNode = unsafe { archived_root::<NVMLeafNode>(&data) };            
            let data:NVMLeafNodeData = archivedleafnodedata.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            

            let mut nvmleaf = NVMLeafNode {
                pool: Some(pool),
                disk_offset: Some(_offset),
                meta_data : meta_data,
                data : std::sync::Arc::new(std::sync::RwLock::new(Some(NVMLeafNodeData { 
                    entries: BTreeMap::new()
                }))),//Some(data),
                meta_data_size: meta_data_len,
                data_size: data_len,
                data_start: data_start,
                data_end: data_end,
                node_size: size,
                checksum: Some(checksum),
                need_to_load_data_from_nvm: std::sync::Arc::new(std::sync::RwLock::new(true)), //false,
                time_for_nvm_last_fetch: SystemTime::now(),
                nvm_fetch_counter: 0,

            };
            //nvmleaf.load_missing_part();

            debug!("NVMLeaf node packed successfully"); 
            Ok(Node(NVMLeaf(nvmleaf)))
        } else {
            panic!("Unkown bytes to unpack. [0..4]: {}", u32::from_be_bytes(data[..4].try_into().unwrap()));
        }
    }

    fn debug_info(&self) -> String {
        format!(
            "{}: {:?}, {}, {:?}",
            self.kind(),
            self.fanout(),
            self.size(),
            self.actual_size()
        )
    }

    fn for_each_child<E, F>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut R) -> Result<(), E>,
    {
        if let Some(iter_type) = self.child_pointer_iter_mut() {
            match iter_type {
                ChildBufferIterator::ChildBuffer(obj) => {
                    if let Some(iter) = obj {
                        for np in iter {
                            f(np)?;
                        }
                    } else {
                        ()
                    }
                },
                ChildBufferIterator::NVMChildBuffer(obj) => {
                    if let Ok(mut data) = obj.write() {
                        let child_itr = data.as_mut().unwrap().children.iter_mut();

                        let itr = child_itr
                            .map(|child| child.as_mut().unwrap().node_pointer.get_mut());

                        for np in itr {
                            f(np)?;
                        }
                    } else {
                        ()
                    }
                },
            }            
        }
        Ok(())
    }
}

impl<N: StaticSize> Size for Node<N> {
    fn size(&self) -> usize {
        match self.0 {
            PackedLeaf(ref map) => map.size(),
            Leaf(ref leaf) => leaf.size(),
            Internal(ref internal) => 4 + internal.size(),
            NVMLeaf(ref nvmleaf) => nvmleaf.size(),
            NVMInternal(ref nvminternal) => 4 + nvminternal.size(),
        }
    }

    fn actual_size(&self) -> Option<usize> {
        match self.0 {
            PackedLeaf(ref map) => map.actual_size(),
            Leaf(ref leaf) => leaf.actual_size(),
            Internal(ref internal) => internal.actual_size().map(|size| 4 + size),
            NVMLeaf(ref nvmleaf) => nvmleaf.actual_size(),
            NVMInternal(ref nvminternal) => nvminternal.actual_size().map(|size| 4 + size),
        }
    }
}

impl<N: StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn try_walk(&mut self, key: &[u8]) -> Option<TakeChildBufferWrapper<N>> where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => {
                if let Some(data) = internal.try_walk(key) {
                    Some(TakeChildBufferWrapper::TakeChildBuffer(Some(data)))
                } else {
                    None
                }
            },
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref mut nvminternal) =>  {
                if let Some(data) = nvminternal.try_walk(key) {
                    Some(TakeChildBufferWrapper::NVMTakeChildBuffer(Some(data)))
                } else {
                    None
                }
            },
        }
    }

/*    pub(super) fn try_find_flush_candidate(&mut self) -> Option<TakeChildBuffer<N>> where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => internal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            ),
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref nvminternal) =>             /*nvminternal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            )*/,
        }
    }
*/
    pub(super) fn try_find_flush_candidate(&mut self) -> Option<TakeChildBufferWrapper<N>> where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => internal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            ),
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref mut nvminternal) => nvminternal.try_find_flush_candidate(
                MIN_FLUSH_SIZE,
                MAX_INTERNAL_NODE_SIZE,
                MIN_FANOUT,
            ),
        }
    }

    pub(super) fn is_too_large(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() > MAX_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() > MAX_LEAF_NODE_SIZE,
            Internal(ref internal) => internal.size() > MAX_INTERNAL_NODE_SIZE,
            NVMLeaf(ref nvmleaf) => nvmleaf.size() > MAX_LEAF_NODE_SIZE,
            NVMInternal(ref nvminternal) => nvminternal.size() > MAX_INTERNAL_NODE_SIZE,
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn kind(&self) -> &str {
        match self.0 {
            PackedLeaf(_) => "packed leaf",
            Leaf(_) => "leaf",
            Internal(_) => "internal",
            NVMLeaf(ref nvmleaf) => "nvmleaf",
            NVMInternal(ref nvminternal) => "nvminternal",
        }
    }
    pub(super) fn fanout(&self) -> Option<usize> where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref internal) => Some(internal.fanout()),
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref nvminternal) => Some(nvminternal.fanout()),
        }
    }

    fn ensure_unpacked(&mut self) -> isize {
        let before = self.size();

        let leaf = if let PackedLeaf(ref mut map) = self.0 {
            map.unpack_leaf()
        } else {
            return 0;
        };

        self.0 = Leaf(leaf);
        let after = self.size();
        after as isize - before as isize
    }

    fn take(&mut self, isnvm: bool) -> Self {
        replace(self, Self::empty_leaf(isnvm))
    }

    pub(super) fn has_too_low_fanout(&self) -> bool where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() < MIN_FANOUT,
            NVMLeaf(ref nvmleaf) => false,
            NVMInternal(ref nvminternal) => nvminternal.fanout() < MIN_FANOUT,
        }
    }

    pub(super) fn is_too_small_leaf(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() < MIN_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() < MIN_LEAF_NODE_SIZE,
            Internal(_) => false,
            NVMLeaf(ref nvmleaf) => nvmleaf.size() < MIN_LEAF_NODE_SIZE,
            NVMInternal(ref nvminternal) => false,
        }
    }

    pub(super) fn is_too_large_leaf(&self) -> bool {
        match self.0 {
            PackedLeaf(ref map) => map.size() > MAX_LEAF_NODE_SIZE,
            Leaf(ref leaf) => leaf.size() > MAX_LEAF_NODE_SIZE,
            Internal(_) => false,
            NVMLeaf(ref nvmleaf) => nvmleaf.size() > MAX_LEAF_NODE_SIZE,
            NVMInternal(ref nvminternal) => false,
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => true,
            Internal(_) => false,
            NVMLeaf(ref nvmleaf) => true,
            NVMInternal(ref nvminternal) => false,
        }
    }

    pub(super) fn empty_leaf(isnvm: bool) -> Self {
        if(isnvm) {
            Node(NVMLeaf(NVMLeafNode::new()))
        } else {
            Node(Leaf(LeafNode::new()))
        }
    }

    pub(super) fn level(&self) -> u32 {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => 0,
            Internal(ref internal) => internal.level(),
            NVMLeaf(ref nvmleaf) => 0,
            NVMInternal(ref nvminternal) => nvminternal.level(),
        }
    }

    pub(super) fn root_needs_merge(&self) -> bool  where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => false,
            Internal(ref internal) => internal.fanout() == 1,
            NVMLeaf(ref nvmleaf) => false,
            NVMInternal(ref nvminternal) => nvminternal.fanout() == 1,
        }
    }
}

impl<N: ObjectReference + StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn split_root_mut<F>(&mut self, allocate_obj: F) -> isize
    where
        F: Fn(Self, LocalPivotKey) -> N,
    {
        let mut isnvm = match self.0 {
            PackedLeaf(_) | Leaf(_) | Internal(_) => false,
            NVMLeaf(_) | NVMInternal(_) => true,
        };

        let size_before = self.size();
        self.ensure_unpacked();
        // FIXME: Update this PivotKey, as the index of the node is changing due to the structural change.
        let mut left_sibling = self.take(isnvm);

        let (right_sibling, pivot_key, cur_level) = match left_sibling.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (right_sibling, pivot_key, _, _pk) =
                    leaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(Leaf(right_sibling)), pivot_key, 0)
            }
            Internal(ref mut internal) => {
                let (right_sibling, pivot_key, _, _pk) = internal.split();
                (Node(Internal(right_sibling)), pivot_key, internal.level())
            },
            NVMLeaf(ref mut nvmleaf) => {
                isnvm = true;
                let (right_sibling, pivot_key, _, _pk) =
                    nvmleaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(NVMLeaf(right_sibling)), pivot_key, 0)
            },
            NVMInternal(ref mut nvminternal) => {
                isnvm = true;
                let (right_sibling, pivot_key, _, _pk) = nvminternal.split();
                (Node(NVMInternal(right_sibling)), pivot_key, nvminternal.level())
            },
        };
        debug!("Root split pivot key: {:?}", pivot_key);


        if(isnvm) {
            *self = Node(NVMInternal(NVMInternalNode::new(    //TODO: NVM?
                NVMChildBuffer::new(allocate_obj(
                    left_sibling,
                    LocalPivotKey::LeftOuter(pivot_key.clone()),
                )),
                NVMChildBuffer::new(allocate_obj(
                    right_sibling,
                    LocalPivotKey::Right(pivot_key.clone()),
                )),
                pivot_key,
                cur_level + 1,
            )));    
        } else {
            *self = Node(Internal(InternalNode::new(    //TODO: NVM?
                ChildBuffer::new(allocate_obj(
                    left_sibling,
                    LocalPivotKey::LeftOuter(pivot_key.clone()),
                )),
                ChildBuffer::new(allocate_obj(
                    right_sibling,
                    LocalPivotKey::Right(pivot_key.clone()),
                )),
                pivot_key,
                cur_level + 1,
            )));
        }

        let size_after = self.size();
        size_after as isize - size_before as isize
    }
}

pub(super) enum GetResult<'a, N: 'a + 'static> {
    Data(Option<(KeyInfo, SlicedCowBytes)>),
    NextNode(&'a RwLock<N>),
    NVMNextNode {
        np: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
        idx: usize,
    },
}

pub(super) enum ApplyResult<'a, N: 'a + 'static> {
    Leaf(Option<KeyInfo>),
    NextNode(&'a mut N),
    NVMNextNode {
        node: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
        idx: usize
    },
    NVMLeaf(Option<KeyInfo>),
}

pub(super) enum PivotGetResult<'a, N: 'a + 'static> {
    Target(Option<&'a RwLock<N>>),
    NVMTarget {
        np: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
        idx: usize
    },
    NextNode(&'a RwLock<N>),
    NVMNextNode {
        np: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
        idx: usize
    },
}

pub(super) enum PivotGetMutResult<'a, N: 'a + 'static> {
    Target(Option<&'a mut N>),
    NVMTarget {
        idx: usize,
        first_bool: bool,
        second_bool: bool,
        np: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
    },
    NextNode(&'a mut N),
    NVMNextNode {
        idx: usize,
        first_bool: bool,
        second_bool: bool,
        np: &'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>,
    },
}

pub(super) enum GetRangeResult<'a, T, N: 'a + 'static> {
    Data(T),
    NVMData {
        np: &'a std::sync::Arc<std::sync::RwLock<Option<NVMLeafNodeData>>>,
    },
    NextNode {
        np: &'a RwLock<N>,
        prefetch_option: Option<&'a RwLock<N>>,
    },
    NVMNextNode {
        np: (&'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>, usize),
        prefetch_option: Option<(&'a std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>, usize)>,
    },
}

impl<N: HasStoragePreference> Node<N> {
    pub(super) fn get(
        &self,
        key: &[u8],
        msgs: &mut Vec<(KeyInfo, SlicedCowBytes)>,
    ) -> GetResult<N> where N: ObjectReference {
        match self.0 {
            PackedLeaf(ref map) => GetResult::Data(map.get(key)),
            Leaf(ref leaf) => GetResult::Data(leaf.get_with_info(key)),
            Internal(ref internal) => {
                let (child_np, msg) = internal.get(key);
                if let Some(msg) = msg {
                    msgs.push(msg);
                }
                GetResult::NextNode(child_np)
            },
            NVMLeaf(ref nvmleaf) => GetResult::Data(nvmleaf.get_with_info(key)),
            NVMInternal(ref nvminternal) => {
                let (np, msg, idx) = nvminternal.get(key);
                if let Some(msg) = msg {
                    msgs.push(msg);
                }
                GetResult::NVMNextNode {
                    np, 
                    idx
                }
            },
        }
    }

    pub(super) fn get_range<'a>(
        &'a self,
        key: &[u8],
        left_pivot_key: &mut Option<CowBytes>,
        right_pivot_key: &mut Option<CowBytes>,
        all_msgs: &mut BTreeMap<CowBytes, Vec<(KeyInfo, SlicedCowBytes)>>,
    ) -> GetRangeResult<Box<dyn Iterator<Item = (&'a [u8], (KeyInfo, SlicedCowBytes))> + 'a>, N>
    where N: ObjectReference
    {
        //println!("..get_range");

        match self.0 {
            PackedLeaf(ref map) => {
                //println!("..PackedLeaf");
                GetRangeResult::Data(Box::new(map.get_all()))
            },
            Leaf(ref leaf) => {
                //println!("..Leaf");
                GetRangeResult::Data(Box::new(
                leaf.entries().iter().map(|(k, v)| (&k[..], v.clone())),
            ))},
            Internal(ref internal) => {
                println!("..Internal");
                let prefetch_option = if internal.level() == 1 {
                    //println!("..Internal................1");
                    internal.get_next_node(key)
                } else {
                    //println!("..Internal................2");
                    None
                };
                let np = internal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);
                GetRangeResult::NextNode {
                    prefetch_option,
                    np,
                }
            },
            NVMLeaf(ref nvmleaf) => {
                //println!("..NVMLeaf");
                let np = nvmleaf.entries();
                GetRangeResult::NVMData {
                    np
                }
            },
            NVMInternal(ref nvminternal) => {
                //println!("..NVMInternal");
                nvminternal.load_all_data();
                
                let prefetch_option = if nvminternal.level() == 1 {
                    //println!("..NVMInternal................1");
                    Some(nvminternal.get_next_node(key))
                } else {
                    //println!("..NVMInternal................2");
                    None
                };

                let np = nvminternal.get_range(key, left_pivot_key, right_pivot_key, all_msgs);
                GetRangeResult::NVMNextNode {
                    np,
                    prefetch_option,
                }
            },
        }
    }

    pub(super) fn pivot_get(&self, pk: &PivotKey) -> Option<PivotGetResult<N>> where N: ObjectReference {
        if pk.is_root() {
            return Some(PivotGetResult::Target(None));
        }
        match self.0 {
            PackedLeaf(_) | Leaf(_) => None,
            Internal(ref internal) => Some(internal.pivot_get(pk)),
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref nvminternal) => Some(nvminternal.pivot_get(pk)),
        }
    }

    pub(super) fn pivot_get_mut(&mut self, pk: &PivotKey) -> Option<PivotGetMutResult<N>> where N: ObjectReference {
        if pk.is_root() {
            return Some(PivotGetMutResult::Target(None));
        }
        match self.0 {
            PackedLeaf(_) | Leaf(_) => None,
            Internal(ref mut internal) => Some(internal.pivot_get_mut(pk)),
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref mut nvminternal) => Some(nvminternal.pivot_get_mut(pk)),
        }
    }
}

impl<N: HasStoragePreference + StaticSize> Node<N> {
    pub(super) fn insert<K, M>(
        &mut self,
        key: K,
        msg: SlicedCowBytes,
        msg_action: M,
        storage_preference: StoragePreference,
    ) -> isize
    where
        K: Borrow<[u8]> + Into<CowBytes>,
        M: MessageAction,
        N: ObjectReference
    {
        let size_delta = self.ensure_unpacked();
        let keyinfo = KeyInfo { storage_preference };
        size_delta
            + (match self.0 {
                PackedLeaf(_) => unreachable!(),
                Leaf(ref mut leaf) => leaf.insert(key, keyinfo, msg, msg_action),
                Internal(ref mut internal) => internal.insert(key, keyinfo, msg, msg_action),
                NVMLeaf(ref mut nvmleaf) => nvmleaf.insert(key, keyinfo, msg, msg_action),
                NVMInternal(ref mut nvminternal) => nvminternal.insert(key, keyinfo, msg, msg_action),
            })
    }

    pub(super) fn insert_msg_buffer<I, M>(&mut self, msg_buffer: I, msg_action: M) -> isize
    where
        I: IntoIterator<Item = (CowBytes, (KeyInfo, SlicedCowBytes))>,
        M: MessageAction,
        N: ObjectReference
    {
        let size_delta = self.ensure_unpacked();
        size_delta
            + (match self.0 {
                PackedLeaf(_) => unreachable!(),
                Leaf(ref mut leaf) => leaf.insert_msg_buffer(msg_buffer, msg_action),
                Internal(ref mut internal) => internal.insert_msg_buffer(msg_buffer, msg_action),
                NVMLeaf(ref mut nvmleaf) => nvmleaf.insert_msg_buffer(msg_buffer, msg_action),
                NVMInternal(ref mut nvminternal) => nvminternal.insert_msg_buffer(msg_buffer, msg_action),
            })
    }

    pub(super) fn apply_with_info(
        &mut self,
        key: &[u8],
        pref: StoragePreference,
    ) -> ApplyResult<N> where N: ObjectReference {
        // FIXME: This is bad for performance, what we want to do here is modify
        // the preference in place determine the new preference and write the
        // PACKED leaf as is again. This violates the restriction that they may
        // never be written again, therefore we need a new interface preparing
        // packed leafs for this exact and only purpose.
        self.ensure_unpacked();
        match self.0 {
            // FIXME: see above
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => ApplyResult::Leaf(leaf.apply(key, pref)),
            Internal(ref mut internal) => {
                ApplyResult::NextNode(internal.apply_with_info(key, pref))
            },
            NVMLeaf(ref mut nvmleaf) => ApplyResult::NVMLeaf(nvmleaf.apply(key, pref)),
            NVMInternal(ref mut nvminternal) => {
                let (node, idx) = nvminternal.apply_with_info(key, pref);

                ApplyResult::NVMNextNode {
                    node, 
                    idx
                }
            },
        }
    }
}

impl<N: HasStoragePreference> Node<N> {
    pub(super) fn child_pointer_iter_mut(&mut self) -> Option<ChildBufferIterator<'_, N>> where N: ObjectReference {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => { 
                let core_value = internal
                    .iter_mut()
                    .map(|child| child.node_pointer.get_mut());

                Some(ChildBufferIterator::ChildBuffer(Some(Box::new(core_value))))
            },
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref mut nvminternal) =>  {

                let core_value = nvminternal
                    .iter_mut();

                Some(ChildBufferIterator::NVMChildBuffer(core_value))
            },
        }
    }

    pub(super) fn child_pointer_iter(&self) -> Option<ChildBufferIterator2<'_, N>>  where N: ObjectReference  {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref internal) => {
                let core_value = internal.iter().map(|child| &child.node_pointer);
                Some(ChildBufferIterator2::ChildBuffer(Some(Box::new(core_value))))
            },
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref nvminternal) => 
            {
                unimplemented!("Could not find any caller for this method! Therefore not fixing it for NVM-related changes..");

                // TODO: return &std::sync::Arc<std::sync::RwLock<Option<InternalNodeData<N>>>>
                //Some(ChildBufferIterator2::ChildBuffer(nvminternal.iter()))
            },
        }
    }

    pub(super) fn drain_children(&mut self) -> Option<ChildBufferIterator3<'_, N>>   where N: ObjectReference  {
        match self.0 {
            Leaf(_) | PackedLeaf(_) => None,
            Internal(ref mut internal) => {
                println!("drain_children internal.....................................................");
                let core_value = internal.drain_children();
                Some(ChildBufferIterator3::ChildBuffer(Some(Box::new(core_value))))
            },
            NVMLeaf(ref nvmleaf) => None,
            NVMInternal(ref mut nvminternal) =>{
                println!("drain_children nvminternal.....................................................");
                let core_value = nvminternal.drain_children();
                Some(ChildBufferIterator3::NVMChildBuffer(Some(Box::new(core_value))))
            },
        }
    }
}

impl<N: ObjectReference + StaticSize + HasStoragePreference> Node<N> {
    pub(super) fn split(&mut self) -> (Self, CowBytes, isize, LocalPivotKey) {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let (node, pivot_key, size_delta, pk) =
                    leaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(Leaf(node)), pivot_key, size_delta, pk)
            }
            Internal(ref mut internal) => {
                debug_assert!(
                    internal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {:?}",
                    internal.fanout(),
                    internal.size(),
                    internal.actual_size()
                );
                let (node, pivot_key, size_delta, pk) = internal.split();
                (Node(Internal(node)), pivot_key, size_delta, pk)
            },
            NVMLeaf(ref mut nvmleaf) => {
                let (node, pivot_key, size_delta, pk) =
                    nvmleaf.split(MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE);
                (Node(NVMLeaf(node)), pivot_key, size_delta, pk)
            },
            NVMInternal(ref mut nvminternal) => {
                debug_assert!(
                    nvminternal.fanout() >= 2 * MIN_FANOUT,
                    "internal split failed due to low fanout: {}, size: {}, actual_size: {:?}",
                    nvminternal.fanout(),
                    nvminternal.size(),
                    nvminternal.actual_size()
                );
                let (node, pivot_key, size_delta, pk) = nvminternal.split();
                (Node(NVMInternal(node)), pivot_key, size_delta, pk)
            },
        }
    }

    pub(super) fn merge(&mut self, right_sibling: &mut Self, pivot_key: CowBytes) -> isize {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => left.merge(right),
            (&mut Internal(ref mut left), &mut Internal(ref mut right)) => {
                left.merge(right, pivot_key)
            },
            (&mut NVMLeaf(ref mut left), &mut NVMLeaf(ref mut right)) => left.merge(right),
            (&mut Internal(ref mut left), &mut Internal(ref mut right)) => {
                left.merge(right, pivot_key)
            },
            _ => unreachable!(),
        }
    }

    pub(super) fn leaf_rebalance(&mut self, right_sibling: &mut Self) -> FillUpResult {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut Leaf(ref mut left), &mut Leaf(ref mut right)) => {
                left.rebalance(right, MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE)
            },
            _ => unreachable!(),
        }
    }

    pub(super) fn nvmleaf_rebalance(&mut self, right_sibling: &mut Self) -> NVMFillUpResult {
        self.ensure_unpacked();
        right_sibling.ensure_unpacked();
        match (&mut self.0, &mut right_sibling.0) {
            (&mut NVMLeaf(ref mut left), &mut NVMLeaf(ref mut right)) => {
                left.rebalance(right, MIN_LEAF_NODE_SIZE, MAX_LEAF_NODE_SIZE)
            },
            _ => unreachable!(),
        }
    }

    /*pub(super) fn range_delete(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> (isize, Option<(&mut N, Option<&mut N>, Vec<N>)>) {
        self.ensure_unpacked();
        match self.0 {
            PackedLeaf(_) => unreachable!(),
            Leaf(ref mut leaf) => {
                let size_delta = leaf.range_delete(start, end);
                (-(size_delta as isize), None)
            }
            Internal(ref mut internal) => {
                let mut dead = Vec::new();
                let (size_delta, l, r) = internal.range_delete(start, end, &mut dead);
                (-(size_delta as isize), Some((l, r, dead)))
            }
        }
    }*/
}

#[derive(serde::Serialize)]
pub struct ChildInfo {
    from: Option<ByteString>,
    to: Option<ByteString>,
    storage: StoragePreference,
    pub pivot_key: PivotKey,
    pub child: NodeInfo,
}

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NodeInfo {
    Internal {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        children: Vec<ChildInfo>,
    },
    Leaf {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        entry_count: usize,
    },
    Packed {
        entry_count: u32,
        range: Vec<ByteString>,
    },
    NVMLeaf {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        entry_count: usize,
    },
    NVMInternal {
        level: u32,
        storage: StoragePreference,
        system_storage: StoragePreference,
        children: Vec<ChildInfo>,
    },
}

pub struct ByteString(Vec<u8>);

impl serde::Serialize for ByteString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        static CHARS: &[u8] = b"0123456789ABCDEF";
        let mut s = String::with_capacity(self.0.len() * 2 + self.0.len() / 4);
        for chunk in self.0.chunks(2) {
            for b in chunk {
                let (upper, lower) = (b >> 4, b & 0x0F);
                s.push(CHARS[upper as usize] as char);
                s.push(CHARS[lower as usize] as char);
            }
            s.push(' ');
        }

        serializer.serialize_str(s.trim_end())
    }
}

impl<N: HasStoragePreference + ObjectReference> Node<N> {
    pub(crate) fn node_info<D>(&self, dml: &D) -> NodeInfo
    where
        D: Dml<Object = Node<N>, ObjectRef = N>,
        N: ObjectReference<ObjectPointer = D::ObjectPointer>,
    {
        match &self.0 {
            Inner::Internal(int) => NodeInfo::Internal {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                children: {
                    int.iter_with_bounds()
                        .map(|(maybe_left, child_buf, maybe_right)| {
                            let (child, storage_preference, pivot_key) = {
                                let mut np = child_buf.node_pointer.write();
                                let pivot_key = np.index().clone();
                                let storage_preference = np.correct_preference();
                                let child = dml.get(&mut np).unwrap();
                                (child, storage_preference, pivot_key)
                            };

                            let node_info = child.node_info(dml);
                            drop(child);

                            dml.evict().unwrap();

                            ChildInfo {
                                from: maybe_left.map(|cow| ByteString(cow.to_vec())),
                                to: maybe_right.map(|cow| ByteString(cow.to_vec())),
                                storage: storage_preference,
                                pivot_key,
                                child: node_info,
                            }
                        })
                        .collect()
                },
            },
            Inner::Leaf(leaf) => NodeInfo::Leaf {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                entry_count: leaf.entries().len(),
            },
            Inner::PackedLeaf(packed) => {
                let len = packed.entry_count();
                NodeInfo::Packed {
                    entry_count: len,
                    range: if len == 0 {
                        Vec::new()
                    } else {
                        [
                            packed.get_full_by_index(0),
                            packed.get_full_by_index(len - 1),
                        ]
                        .iter()
                        .filter_map(|opt| {
                            if let Some((key, _)) = opt {
                                Some(ByteString(key.to_vec()))
                            } else {
                                None
                            }
                        })
                        .collect()
                    },
                }
            },
            Inner::NVMLeaf(ref nvmleaf) => NodeInfo::NVMLeaf {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                entry_count: nvmleaf.entries().read().as_ref().unwrap().as_ref().unwrap().entries.len(),
            },
            NVMInternal(ref nvminternal) => NodeInfo::NVMInternal {
                storage: self.correct_preference(),
                system_storage: self.system_storage_preference(),
                level: self.level(),
                children: {
                    let auto = nvminternal.iter_with_bounds();

                    if let Ok(data) = auto.read() {
                        
                        let itr = data.as_ref().unwrap().children.iter().enumerate().map(move |(idx, child)| {
                            let maybe_left = if idx == 0 {
                                None
                            } else {
                                nvminternal.meta_data.pivot.get(idx - 1)
                            };
                
                            let maybe_right = nvminternal.meta_data.pivot.get(idx);
                
                            (maybe_left, child, maybe_right)
                        });

                        itr
                        .map(|(maybe_left, child_buf, maybe_right)| {
                            let (child, storage_preference, pivot_key) = {
                                let mut np = child_buf.as_ref().unwrap().node_pointer.write();
                                let pivot_key = np.index().clone();
                                let storage_preference = np.correct_preference();
                                let child = dml.get(&mut np).unwrap();
                                (child, storage_preference, pivot_key)
                            };

                            let node_info = child.node_info(dml);
                            drop(child);

                            dml.evict().unwrap();

                            ChildInfo {
                                from: maybe_left.map(|cow| ByteString(cow.to_vec())),
                                to: maybe_right.map(|cow| ByteString(cow.to_vec())),
                                storage: storage_preference,
                                pivot_key,
                                child: node_info,
                            }
                        })
                        .collect()
                    } else {
                        unimplemented!("..")
                    }
                },
            },           
            /*NodeInfo::NVMInternal {
                pool: None,
                disk_offset: None,
                meta_data: InternalNodeMetaData { 
                    storage: self.correct_preference(),
                    system_storage: self.system_storage_preference(),
                    level: self.level(),
                },
                data: Some(InternalNodeData {
                    children: {
                        int.iter_with_bounds()
                            .map(|(maybe_left, child_buf, maybe_right)| {
                                let (child, storage_preference, pivot_key) = {
                                    let mut np = child_buf.node_pointer.write();
                                    let pivot_key = np.index().clone();
                                    let storage_preference = np.correct_preference();
                                    let child = dml.get(&mut np).unwrap();
                                    (child, storage_preference, pivot_key)
                                };
    
                                let node_info = child.node_info(dml);
                                drop(child);
    
                                dml.evict().unwrap();
    
                                ChildInfo {
                                    from: maybe_left.map(|cow| ByteString(cow.to_vec())),
                                    to: maybe_right.map(|cow| ByteString(cow.to_vec())),
                                    storage: storage_preference,
                                    pivot_key,
                                    child: node_info,
                                }
                            })
                            .collect()
                    }
                }),
                meta_data_size: 0,
                data_size: 0,
                data_start: 0,
                data_end: 0,
                node_size: crate::vdev::Block(0),
                checksum: None,
                need_to_load_data_from_nvm: true,
                time_for_nvm_last_fetch: SystemTime::now(),
                nvm_fetch_counter: 0,
            },*/
        }
    }
}
