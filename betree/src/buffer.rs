//! This module provides block-aligned buffers.
//!
//! There are three public buffer types, [Buf], [MutBuf], and [BufWrite]. They can be converted into
//! each other without reallocation of the backing buffer.
//!
//! [Buf] and [MutBuf] are shared (Arc) and splittable, for immutable and mutable access, respectively.
//! [BufWrite] is uniquely owned, and thus can allow both immutable and mutable access, as well as
//! a growable buffer.
//!
//! [MutBuf] does not support growing with [io::Write] because the semantics of growing an inner split buffer are unclear.

use crate::vdev::{Block, BLOCK_SIZE};
use std::{
    alloc::{self, Layout},
    cell::UnsafeCell,
    fmt, io,
    mem::ManuallyDrop,
    ops::{Deref, Range},
    ptr::NonNull,
    slice,
    sync::Arc,
};

const MIN_GROWTH_SIZE: Block<u32> = Block(1);
const GROWTH_FACTOR: f32 = 1.0;

fn is_aligned(buf: &[u8]) -> bool {
    buf.as_ptr() as usize % BLOCK_SIZE == 0 && buf.len() % BLOCK_SIZE == 0
}

fn split_range_at(
    range: &Range<Block<u32>>,
    mid: Block<u32>,
) -> (Range<Block<u32>>, Range<Block<u32>>) {
    // NOTE:
    // jwuensche: I've changed the behavior off this function back to the
    // original implementation whereas we take the relative mid from the current
    // view.  In the redesign of some of the modules this was changed to relate
    // to a total mid which we do not calculate in some of the vdev structs.
    // This only became a problem when performing multiple splits as the buffer
    // returned would contain the total position in the original buffer.
    // To keep the performance advantage we simply offset the mid from the known
    // total start.
    //
    // No further implications should be expected as the only sequentially use of
    // this structure is in the [crate::vdev::Parity1] code.
    if range.start + mid < range.end {
        // mid is in range
        (range.start..range.start + mid, range.start + mid..range.end)
    } else {
        // mid is past range
        (range.clone(), range.end..range.end)
    }
}

#[derive(Debug)]
struct AlignedStorage {
    ptr: NonNull<u8>,
    capacity: Block<u32>,
}

// impl Default for AlignedStorage {
//     fn default() -> Self {
//         AlignedStorage {
//             ptr: None,
//             capacity: Block(0),
//         }
//     }
// }

impl AlignedStorage {
    fn zeroed(capacity: Block<u32>) -> Self {
        Self {
            ptr: unsafe {
                let new_layout =
                    Layout::from_size_align_unchecked(capacity.to_bytes() as usize, BLOCK_SIZE);
                NonNull::new(alloc::alloc_zeroed(new_layout)).expect("Allocation failed.")
            },
            capacity,
        }
    }

    fn ensure_capacity(&mut self, requested_capacity: Block<u32>) {
        if requested_capacity <= self.capacity {
            return;
        }

        let wanted_capacity = requested_capacity
            .max(Block::round_up_from_bytes(
                (self.capacity.to_bytes() as f32 * GROWTH_FACTOR) as u32,
            ))
            .max(self.capacity + MIN_GROWTH_SIZE);

        if wanted_capacity.to_bytes() > 8 * 1024 * 1024 {
            log::warn!(
                "Requested allocation of >8MiB: {} byte",
                wanted_capacity.to_bytes()
            );
        }

        unsafe {
            let curr_layout =
                Layout::from_size_align_unchecked(self.capacity.to_bytes() as usize, BLOCK_SIZE);
            let new_layout =
                Layout::from_size_align_unchecked(wanted_capacity.to_bytes() as usize, BLOCK_SIZE);
            // TODO: benchmark uninit
            // NOTE: this might not call calloc as initially thought. The default impl just allocs uninitialised
            // memory, and then writes 0 to it

            let realloc_ptr = alloc::realloc(
                self.ptr.as_ptr(),
                curr_layout,
                wanted_capacity.to_bytes() as usize,
            );

            self.ptr = NonNull::new(realloc_ptr).unwrap_or_else(|| {
                let new_ptr =
                    NonNull::new(alloc::alloc_zeroed(new_layout)).expect("Allocation failed.");
                self.ptr
                    .as_ptr()
                    .copy_to_nonoverlapping(new_ptr.as_ptr(), self.capacity.to_bytes() as usize);
                alloc::dealloc(self.ptr.as_ptr(), curr_layout);
                new_ptr
            });
            self.capacity = wanted_capacity;
        }
    }
}

impl Drop for AlignedStorage {
    fn drop(&mut self) {
        unsafe {
            let layout =
                Layout::from_size_align_unchecked(self.capacity.to_bytes() as usize, BLOCK_SIZE);
            alloc::dealloc(self.ptr.as_ptr(), layout)
        }
    }
}

impl From<Box<[u8]>> for AlignedStorage {
    fn from(b: Box<[u8]>) -> Self {
        // It can be useful to re-enable this line to easily locate places where unnecessary
        // copying takes place, but it's not suited to stay enabled unconditionally.
        // assert!(is_aligned(&b));
        if is_aligned(&b) {
            AlignedStorage {
                capacity: Block::from_bytes(b.len() as u32),
                ptr: unsafe {
                    NonNull::new((*Box::into_raw(b)).as_mut_ptr()).expect("Assume valid pointer.")
                },
            }
        } else {
            assert!(
                b.len() % BLOCK_SIZE == 0,
                "Box length is not a multiple of block size"
            );
            log::warn!("Unaligned buffer, copying {} bytes", b.len());
            let size = Block::round_up_from_bytes(b.len() as u32);
            let storage = AlignedStorage::zeroed(size);
            unsafe {
                storage
                    .ptr
                    .as_ptr()
                    .copy_from_nonoverlapping(b.as_ptr(), b.len());
            }
            storage
        }
    }
}

// Unsafe private buffer
#[derive(Clone)]
struct AlignedBuf {
    buf: Arc<UnsafeCell<AlignedStorage>>,
}

// UnsafeCell is not Send, and Arc<T> is only Send if T: Send.
// Orphan rules forbid `unsafe impl Send for UnsafeCell<Box<[u8]>> {}`, so this
// impl is for AlignedBuf instead.
//
// AlignedBuf is mutated only in the Arc reference counts, which are atomic,
// and in disjoint pieces via MutBufs. Conversion between Buf and MutBuf panics
// if the convertee is not unique, ensuring an AlignedBuf will not be accessed
// mutable and non-mutably at the same time. Since MutBuf can't be cloned, mutable access
// to each partition is unique. No synchronisation should be necessary, even if different
// threads can mutate different disjoint pieces of buf, if there's nobody to observe
// those changes until buf is unique again.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    fn zeroed(capacity: Block<u32>) -> Self {
        let vec = AlignedStorage::zeroed(capacity);
        Self {
            buf: Arc::new(UnsafeCell::new(vec)),
        }
    }

    fn full_range(&self) -> Range<Block<u32>> {
        let buf = unsafe { &*self.buf.get() };
        Block(0)..buf.capacity
    }

    fn unwrap_storage(self) -> AlignedStorage {
        Arc::try_unwrap(self.buf)
            .expect("AlignedBuf was not unique")
            .into_inner()
    }

    fn unwrap_unique(self) -> Self {
        AlignedBuf {
            buf: Arc::new(UnsafeCell::new(self.unwrap_storage())),
        }
    }
}

impl From<Box<[u8]>> for AlignedBuf {
    fn from(b: Box<[u8]>) -> Self {
        let storage = AlignedStorage::from(b);
        AlignedBuf {
            buf: Arc::new(UnsafeCell::new(storage)),
        }
    }
}

/// A shared read-only buffer, internally using block-aligned allocations.
#[derive(Clone)]
pub struct Buf {
    buf: AlignedBuf,
    range: Range<Block<u32>>,
}

impl fmt::Debug for Buf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Buf").field("range", &self.range).finish()
    }
}

/// A fixed-size mutable buffer, which can be split into disjoint pieces
/// to create multiple mutable references to a single shared buffer in a safe manner.
// This is only safe as long as no 2 MutBufs with overlapping ranges share the same Arc.
pub struct MutBuf {
    buf: AlignedBuf,
    range: Range<Block<u32>>,
}

/// An **un**shared mutable buffer, which can be appended to.
/// Out of [Buf], [MutBuf], and [BufWrite], by which an existing block-aligned
/// buffer can be grown.
pub struct BufWrite {
    buf: AlignedStorage,
    size: u32,
}

impl BufWrite {
    /// Create an empty [BufWrite] with the specified capacity.
    /// The backing storage is zeroed.
    pub fn with_capacity(capacity: Block<u32>) -> Self {
        Self {
            buf: AlignedStorage::zeroed(capacity),
            size: 0,
        }
    }

    /// Convert this to a read-only [Buf].
    /// This is always safe because [BufWrite] can't be split,
    /// and therefore no aliasing writable pieces can remain.
    pub fn into_buf(self) -> Buf {
        Buf::from_aligned(AlignedBuf {
            buf: Arc::new(UnsafeCell::new(self.buf)),
        })
    }

    pub fn len(&self) -> usize {
        self.size as usize
    }
}

impl io::Write for BufWrite {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let required_size = self.size + data.len() as u32;
        self.buf
            .ensure_capacity(Block::round_up_from_bytes(required_size));

        unsafe {
            self.buf
                .ptr
                .as_ptr()
                .offset(self.size as isize)
                .copy_from_nonoverlapping(data.as_ptr(), data.len());
            self.size = required_size;
        }

        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

unsafe impl zstd::stream::raw::WriteBuf for BufWrite {
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn capacity(&self) -> usize {
        self.buf.capacity.to_bytes() as usize
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.buf.ptr.as_mut() }
    }

    unsafe fn filled_until(&mut self, n: usize) {
        self.size = n as u32
    }
}

impl io::Seek for BufWrite {
    fn seek(&mut self, seek: io::SeekFrom) -> io::Result<u64> {
        use io::SeekFrom::*;
        let new_size = match seek {
            Start(offset) => offset as i64,
            End(offset) => self.buf.capacity.to_bytes() as i64 + offset,
            Current(offset) => self.size as i64 + offset,
        } as u32;

        if new_size <= self.buf.capacity.to_bytes() {
            self.size = new_size;
            Ok(new_size as u64)
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid seek"))
        }
    }
}

impl AsRef<[u8]> for BufWrite {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            let slice =
                slice::from_raw_parts(self.buf.ptr.as_ptr(), self.buf.capacity.to_bytes() as usize);
            &slice[..self.size as usize]
        }
    }
}

impl AsMut<[u8]> for BufWrite {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe {
            let slice = slice::from_raw_parts_mut(
                self.buf.ptr.as_ptr(),
                self.buf.capacity.to_bytes() as usize,
            );
            &mut slice[..self.size as usize]
        }
    }
}

impl Buf {
    fn from_aligned(aligned: AlignedBuf) -> Self {
        Self {
            range: aligned.full_range(),
            buf: aligned,
        }
    }

    /// Create a [Buf] from a byte vector. If `b.len()` is not a multiple of the block size,
    /// the size will be rounded up to the next multiple and filled with zeroes.
    pub fn from_zero_padded(mut b: Vec<u8>) -> Self {
        let padded_size = Block::round_up_from_bytes(b.len());
        b.resize(padded_size.to_bytes(), 0);
        Self::from(b.into_boxed_slice())
    }

    /// Create a [Buf] filled with the specified amount of zeroes.
    pub fn zeroed(size: Block<u32>) -> Self {
        Self::from_aligned(AlignedBuf::zeroed(size))
    }

    /// Panics if Buf was not unique, to ensure no readable references remain
    pub fn into_full_mut(self) -> MutBuf {
        let range = self.buf.full_range();
        MutBuf {
            buf: self.buf.unwrap_unique(),
            range,
        }
    }

    /// Convert to a mutable [BufWrite], if this is the only [Buf] referencing the backing storage.
    /// Panics if this [Buf] was not unique.
    pub fn into_buf_write(self) -> BufWrite {
        let storage = Arc::try_unwrap(self.buf.buf)
            .expect("AlignedBuf was not unique")
            .into_inner();
        BufWrite {
            buf: storage,
            size: self.range.end.to_bytes(),
        }
    }

    /// If this [Buf] is unique, return its backing buffer without reallocation or copying.
    /// Panics if this [Buf] was not unique.
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        let storage = ManuallyDrop::new(
            Arc::try_unwrap(self.buf.buf)
                .expect("AlignedBuf was not unique")
                .into_inner(),
        );

        unsafe {
            Box::from_raw(slice::from_raw_parts_mut(
                storage.ptr.as_ptr(),
                storage.capacity.to_bytes() as usize,
            ))
        }
    }

    /// Return the block range accessible via this [Buf].
    pub fn range(&self) -> &Range<Block<u32>> {
        &self.range
    }

    /// Return the block size of this [Buf].
    pub fn size(&self) -> Block<u32> {
        Block(self.range.end.as_u32() - self.range.start.as_u32())
    }

    /// Split this [Buf] into two disjoint [Buf]s at `mid`.
    pub fn split_at(self, mid: Block<u32>) -> (Self, Self) {
        let (left, right) = split_range_at(&self.range, mid);

        (
            Self {
                buf: self.buf.clone(),
                range: left,
            },
            Self {
                buf: self.buf,
                range: right,
            },
        )
    }
}

impl MutBuf {
    /// Split this [MutBuf] into two disjoint [MutBuf]s at `mid`.
    pub fn split_at(self, mid: Block<u32>) -> (Self, Self) {
        let (left, right) = split_range_at(&self.range, mid);

        (
            Self {
                buf: self.buf.clone(),
                range: left,
            },
            Self {
                buf: self.buf,
                range: right,
            },
        )
    }

    /// Panics if MutBuf was not unique, to ensure no mutable references remain
    pub fn into_full_buf(self) -> Buf {
        Buf::from_aligned(self.buf.unwrap_unique())
    }

    /// Return the block range accessible via this [MutBuf].
    pub fn range(&self) -> &Range<Block<u32>> {
        &self.range
    }
}

impl Deref for Buf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Buf {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            let start = self.range.start.to_bytes() as usize;
            let end = self.range.end.to_bytes() as usize;
            let buf = &*self.buf.buf.get();
            let slice = slice::from_raw_parts(buf.ptr.as_ptr(), buf.capacity.to_bytes() as usize);
            &slice[start..end]
        }
    }
}

impl AsMut<[u8]> for MutBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        // This can be cast to a pointer of any kind. Ensure that the access is unique (no active references, mutable or not) when casting to &mut T, and ensure that there are no mutations or mutable aliases going on when casting to &T
        // -- UnsafeCell::get

        // Unique access to each element is an invariant of MutBuf, first ensured by Buf::into_mut by
        // ensuring no other Bufs share the same backing data.
        // During MutBuf::split_at, two new MutBufs are created with disjoint ranges.

        unsafe {
            let start = self.range.start.to_bytes() as usize;
            let end = self.range.end.to_bytes() as usize;
            let buf = &*self.buf.buf.get();
            let slice =
                slice::from_raw_parts_mut(buf.ptr.as_ptr(), buf.capacity.to_bytes() as usize);
            &mut slice[start..end]
        }
    }
}

impl From<Box<[u8]>> for Buf {
    fn from(b: Box<[u8]>) -> Self {
        let aligned = AlignedBuf::from(b);
        Buf {
            range: aligned.full_range(),
            buf: aligned,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buf_round_trip() {
        let b = vec![3; BLOCK_SIZE].into_boxed_slice();
        let b2 = b.clone();
        let buf = Buf::from(b);

        assert_eq!(&b2[..], &buf[..]);
    }

    #[test]
    fn sequential_splits() {
        // This tests checks if sequential splits produce a result as expected by some of the functions.
        let mut b = vec![0; 2 * BLOCK_SIZE];
        b[0] = 1;
        b[BLOCK_SIZE] = 2;
        let buf = Buf::from(b.into_boxed_slice());
        let (left, right) = buf.split_at(Block(1));
        assert!(left[0] == 1);
        let (left, right) = right.split_at(Block(1));
        assert!(right.size() == Block(0));
        assert!(left[0] == 2);
    }
}
