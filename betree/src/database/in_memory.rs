use crate::{
    atomic_option::AtomicOption,
    cache::ClockCache,
    checksum::XxHashBuilder,
    compression,
    data_management::{self, Dmu},
    database::{
        handler, DatabaseBuilder, DatasetId, ErrorKind, Generation, Handler, Object, ObjectPointer,
        Result, ResultExt, RootTree, ROOT_DATASET_ID,
    },
    storage_pool::{InMemory, StoragePoolLayer},
    tree::{DefaultMessageAction, TreeLayer},
};
use seqlock::SeqLock;

use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    iter::FromIterator,
    sync::{atomic::AtomicU64, Arc},
};

pub struct InMemoryConfiguration {
    pub data_size: u64,
    pub cache_size: u64,
}

impl DatabaseBuilder for InMemoryConfiguration {
    type Spu = InMemory;
    type Dmu = Dmu<
        compression::None,
        ClockCache<data_management::impls::ObjectKey<Generation>, RwLock<Object>>,
        InMemory,
        handler::Handler,
        DatasetId,
        Generation,
    >;

    fn new_spu(&self) -> Result<Self::Spu> {
        InMemory::new(&self.data_size).chain_err(|| ErrorKind::SplConfiguration)
    }

    fn new_handler(&self, spu: &Self::Spu) -> handler::Handler {
        Handler {
            root_tree_inner: AtomicOption::new(),
            root_tree_snapshot: RwLock::new(None),
            current_generation: SeqLock::new(Generation(1)),
            free_space: HashMap::from_iter((0..1).map(|disk_id| (disk_id, AtomicU64::new(0)))),
            delayed_messages: Mutex::new(Vec::new()),
            last_snapshot_generation: RwLock::new(HashMap::new()),
            allocations: AtomicU64::new(0),
            old_root_allocation: SeqLock::new(None),
        }
    }

    fn new_dmu(&self, spu: Self::Spu, handler: handler::Handler) -> Self::Dmu {
        Self::Dmu::new(
            compression::None,
            XxHashBuilder,
            spu,
            ClockCache::new(self.cache_size as usize),
            handler,
        )
    }

    fn select_root_tree(
        &self,
        dmu: Arc<Self::Dmu>,
    ) -> Result<(RootTree<Self::Dmu>, ObjectPointer)> {
        let tree = RootTree::empty_tree(ROOT_DATASET_ID, DefaultMessageAction, dmu.clone());
        tree.dmu()
            .handler()
            .root_tree_inner
            .set(Arc::clone(tree.inner()));

        let root_ptr = tree.sync()?;
        Ok((tree, root_ptr))
    }
}
