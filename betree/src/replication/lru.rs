use pmem_hashmap::{PMap, PMapError};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// Key pointing to an LRU node in a persistent cache.
pub struct LruKey(u64);

impl LruKey {
    /// Fetch and cast a pmem pointer to a [PlruNode].
    ///
    /// Safety
    /// ======
    /// The internals of this method are highly unsafe. Concurrent usage and
    /// removal are urgently discouraged.
    fn fetch(&self, map: &mut PMap) -> Result<&mut PlruNode, PMapError> {
        map.get(self.key()).and_then(|node_raw| unsafe {
            Ok(std::mem::transmute::<&mut [u8; PLRU_NODE_SIZE], _>(
                node_raw.try_into().unwrap(),
            ))
        })
    }

    /// Wrap a hashed value in an [LruKey].
    pub fn from(hash: u64) -> Self {
        LruKey(hash)
    }

    fn key(&self) -> [u8; size_of::<u64>() + 1] {
        let mut key = [0; size_of::<u64>() + 1];
        key[0] = PREFIX_LRU;
        key[1..].copy_from_slice(&self.0.to_le_bytes());
        key
    }
}

/// Persistent LRU
#[repr(C)]
pub struct Plru {
    head: Option<LruKey>,
    tail: Option<LruKey>,
    // in Blocks? Return evicted element when full
    capacity: u64,
    count: u64,
    size: u64,
}

impl Plru {
    pub fn create(map: &mut PMap, capacity: u64) -> Result<Persistent<Self>, PMapError> {
        let this = Self {
            head: None,
            tail: None,
            capacity,
            size: 0,
            count: 0,
        };
        map.insert([super::PREFIX_LRU_ROOT], unsafe {
            &std::mem::transmute::<_, [u8; size_of::<Plru>()]>(this)
        })?;
        Self::open(map)
    }

    pub fn open(map: &mut PMap) -> Result<Persistent<Self>, PMapError> {
        // Fetch from map, check if valid, transmute to type, and reaffirm
        // non-null-ness. The unchecked transition is always correct as we unpack an already ensured
        unsafe {
            Ok(Persistent(NonNull::new_unchecked(std::mem::transmute(
                map.get([super::PREFIX_LRU_ROOT])?.as_mut_ptr(),
            ))))
        }
    }

    pub fn touch(&mut self, map: &mut PMap, node_ptr: LruKey) -> Result<(), PMapError> {
        if self.head == Some(node_ptr) {
            return Ok(());
        }

        let node = node_ptr.fetch(map).unwrap();
        self.cut_node_and_stitch(map, node)?;

        // Fixate new head
        let old_head_ptr = self.head.expect("Invalid State");
        let old_head = old_head_ptr.fetch(map).unwrap();
        old_head.fwd = Some(node_ptr);
        node.back = self.head;
        self.head = Some(node_ptr);

        Ok(())
    }

    /// Add a new entry into the LRU. Will fail if already present.
    pub fn insert(&mut self, map: &mut PMap, node_ptr: LruKey, size: u64) -> Result<(), PMapError> {
        let node = PlruNode {
            fwd: None,
            back: self.head,
            size,
            data: node_ptr.0,
        };

        map.insert(node_ptr.key(), &unsafe {
            std::mem::transmute::<_, [u8; PLRU_NODE_SIZE]>(node)
        })?;
        if let Some(head_ptr) = self.head {
            let head = head_ptr.fetch(map)?;
            head.fwd = Some(node_ptr);
            self.head = Some(node_ptr);
        } else {
            // no head existed yet -> newly initialized list
            self.head = Some(node_ptr);
            self.tail = Some(node_ptr);
        }
        self.size += size;
        self.count += 1;
        Ok(())
    }

    /// Checks whether an eviction is necessary and which entry to evict.
    /// This call does not perform the removal itself.
    pub fn evict(&mut self) -> Option<DataKey> {
        if let (Some(tail), true) = (self.tail, self.size > self.capacity) {
            return Some(DataKey(tail.0));
        }
        None
    }

    fn cut_node_and_stitch(
        &mut self,
        map: &mut PMap,
        node: &mut PlruNode,
    ) -> Result<(), PMapError> {
        let node_ptr = LruKey(node.data);
        if let Some(forward_ptr) = node.fwd {
            let forward = forward_ptr.fetch(map)?;
            forward.back = node.back;
        }
        if self.tail == Some(node_ptr) {
            self.tail = node.fwd;
        }
        if let Some(back_ptr) = node.back {
            let back = back_ptr.fetch(map)?;
            back.fwd = node.fwd;
        }
        if self.head == Some(node_ptr) {
            self.head = node.back;
        }
        node.fwd = None;
        node.back = None;

        Ok(())
    }

    /// Remove a node from cache and deallocate.
    pub fn remove(&mut self, map: &mut PMap, node_ptr: LruKey) -> Result<(), PMapError> {
        let node = node_ptr.fetch(map)?;
        self.cut_node_and_stitch(map, node)?;
        let size = node.size;
        map.remove(node_ptr.key())?;
        self.size -= size;
        self.count -= 1;
        Ok(())
    }
}

use std::{mem::size_of, ptr::NonNull};

use super::{DataKey, Persistent, PREFIX_LRU};

const PLRU_NODE_SIZE: usize = size_of::<PlruNode>();

/// Ephemeral Wrapper around a byte array for sane access code.
///
/// Structure
/// =========
///
///  fwd  ┌───┐
///  <────┤   │ ..
///    .. │   ├────>
///       └───┘ back
///
/// Safety
/// ======
/// Using this wrapper requires transmuting the underlying byte array, which
/// invalidates, when used on references, all borrow checker guarantees. Use
/// with extrem caution, and be sure what you are doing.
#[repr(C)]
pub struct PlruNode {
    fwd: Option<LruKey>,
    back: Option<LruKey>,
    size: u64,
    data: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, process::Command};
    use tempfile::Builder;

    struct TestFile(PathBuf);

    impl TestFile {
        pub fn new() -> Self {
            TestFile(
                Builder::new()
                    .tempfile()
                    .expect("Could not get tmpfile")
                    .path()
                    .to_path_buf(),
            )
        }

        pub fn path(&self) -> &PathBuf {
            &self.0
        }
    }
    impl Drop for TestFile {
        fn drop(&mut self) {
            if !Command::new("rm")
                .arg(self.0.to_str().expect("Could not pass tmpfile"))
                .output()
                .expect("Could not delete")
                .status
                .success()
            {
                eprintln!("Could not delete tmpfile");
            }
        }
    }

    #[test]
    fn new() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let _ = Plru::create(&mut pmap, 32 * 1024 * 1024).unwrap();
    }

    #[test]
    fn insert() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let mut lru = Plru::create(&mut pmap, 32 * 1024 * 1024).unwrap();
        lru.insert(&mut pmap, LruKey(100), 321).unwrap();
        assert_eq!(lru.head, Some(LruKey(100)));
        lru.insert(&mut pmap, LruKey(101), 322).unwrap();
        assert_eq!(lru.head, Some(LruKey(101)));
        lru.insert(&mut pmap, LruKey(102), 323).unwrap();
        assert_eq!(lru.head, Some(LruKey(102)));
    }

    #[test]
    fn touch() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let mut lru = Plru::create(&mut pmap, 32 * 1024 * 1024).unwrap();
        lru.insert(&mut pmap, LruKey(100), 321).unwrap();
        lru.insert(&mut pmap, LruKey(101), 322).unwrap();
        lru.insert(&mut pmap, LruKey(102), 323).unwrap();
    }

    #[test]
    fn evict() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let mut lru = Plru::create(&mut pmap, 32 * 1024 * 1024).unwrap();
        lru.insert(&mut pmap, LruKey(100), 16 * 1024 * 1024)
            .unwrap();
        lru.insert(&mut pmap, LruKey(101), 15 * 1024 * 1024)
            .unwrap();
        lru.insert(&mut pmap, LruKey(102), 8 * 1024 * 1024).unwrap();
        assert_eq!(
            lru.evict().and_then(|opt| Some(LruKey(opt.0))),
            Some(LruKey(100))
        );
        lru.remove(&mut pmap, LruKey(100)).unwrap();
        lru.insert(&mut pmap, LruKey(100), 1 * 1024 * 1024).unwrap();
        assert_eq!(lru.evict().and_then(|opt| Some(LruKey(opt.0))), None);
        lru.insert(&mut pmap, LruKey(103), 9 * 1024 * 1024).unwrap();
        assert_eq!(
            lru.evict().and_then(|opt| Some(LruKey(opt.0))),
            Some(LruKey(101))
        );
    }

    #[test]
    fn remove() {
        let file = TestFile::new();
        let mut pmap = PMap::create(file.path(), 32 * 1024 * 1024).unwrap();
        let mut lru = Plru::create(&mut pmap, 32 * 1024 * 1024).unwrap();
        lru.insert(&mut pmap, LruKey(100), 16 * 1024 * 1024)
            .unwrap();
        lru.remove(&mut pmap, LruKey(100)).unwrap();
        assert_eq!(lru.head, None);
        assert_eq!(lru.tail, None);
    }
}
