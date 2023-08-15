use crossbeam_channel::Receiver;
use pmem_hashmap::allocator::PalPtr;

use super::{lru::PlruNode, PCacheRoot, Persistent};

pub enum Msg<T> {
    Touch(PalPtr<PlruNode<T>>),
    Remove(PalPtr<PlruNode<T>>),
    Insert(PalPtr<PlruNode<T>>, u64, u64, T),
    Close,
}

pub fn main<T>(rx: Receiver<Msg<T>>, mut root: Persistent<PCacheRoot<T>>) {
    // TODO: Error handling with return to valid state in the data section..
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Touch(mut ptr) => {
                let mut lru = root.lru.write();
                let _ = lru.touch(&mut ptr);
            }
            Msg::Remove(mut ptr) => {
                let mut lru = root.lru.write();
                let _ = lru.remove(&mut ptr);
                ptr.free();
            }
            Msg::Insert(ptr, hash, size, baggage) => {
                let mut lru = root.lru.write();
                let _ = lru.insert(ptr, hash, size, baggage);
            }
            Msg::Close => break,
        }
    }
}
