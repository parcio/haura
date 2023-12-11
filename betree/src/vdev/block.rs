use crate::size::StaticSize;

use super::BLOCK_SIZE;
use serde::{Deserialize, Serialize};
use std::{
    iter::Sum,
    ops::{Add, AddAssign, Div, Mul, MulAssign, Rem, Sub},
};

/// A unit which represents a number of bytes which are a multiple of
/// `BLOCK_SIZE`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive(check_bytes)]
#[serde(transparent)]
pub struct Block<T: Uint>(pub T);

impl<T: Uint> StaticSize for Block<T> {
    fn static_size() -> usize {
        // Works for standard sizes
        std::mem::size_of::<T>()
    }
}

pub trait Uint:
    Copy
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + Rem<Output = Self>
    + Eq
    + sealed::Sealed
{
    const ZERO: Self;
    const ONE: Self;
    const BLOCK_SIZE: Self;
}
mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for u32 {}
impl sealed::Sealed for u64 {}
impl sealed::Sealed for usize {}

impl Uint for u32 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
    const BLOCK_SIZE: Self = BLOCK_SIZE as Self;
}
impl Uint for u64 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
    const BLOCK_SIZE: Self = BLOCK_SIZE as Self;
}
impl Uint for usize {
    const ZERO: Self = 0;
    const ONE: Self = 1;
    const BLOCK_SIZE: Self = BLOCK_SIZE;
}

impl<T: Uint> Block<T> {
    /// Creates a new [Block] instance.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `bytes` is not a multiple of [BLOCK_SIZE].
    pub fn from_bytes(bytes: T) -> Self {
        debug_assert!(bytes % T::BLOCK_SIZE == T::ZERO);
        Block(bytes / T::BLOCK_SIZE)
    }

    /// In contrast to [Block::from_bytes], this function accepts sizes which
    /// are not multiples of [BLOCK_SIZE], rounding up to full multiples.
    pub fn round_up_from_bytes(bytes: T) -> Self {
        let full_blocks = bytes / T::BLOCK_SIZE;
        if bytes % T::BLOCK_SIZE == T::ZERO {
            Block(full_blocks)
        } else {
            Block(full_blocks + T::ONE)
        }
    }

    /// Returns the number of `Block`s as a number of bytes.
    pub fn to_bytes(self) -> T {
        self.0 * T::BLOCK_SIZE
    }
}

impl<T: Uint + Into<u64>> Block<T> {
    /// Returns the number of blocks as `u64`.
    pub fn as_u64(self) -> u64 {
        self.0.into()
    }
}

impl Block<u32> {
    /// Returns the number of blocks as `u32`.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<Block<u32>> for Block<u64> {
    fn from(block: Block<u32>) -> Self {
        Block(block.0.into())
    }
}

impl<T: Uint> Add for Block<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Block(self.0 + rhs.0)
    }
}

impl<T: Uint> AddAssign for Block<T> {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl<T: Uint> Add<T> for Block<T> {
    type Output = Self;

    fn add(self, rhs: T) -> Self::Output {
        Block(self.0 + rhs)
    }
}

impl<T: Uint> AddAssign<T> for Block<T> {
    fn add_assign(&mut self, rhs: T) {
        *self = *self + rhs;
    }
}

impl<T: Uint> Sub<T> for Block<T> {
    type Output = Self;

    fn sub(self, rhs: T) -> Self::Output {
        Block(self.0 - rhs)
    }
}

impl<T: Uint> Mul<T> for Block<T> {
    type Output = Self;

    fn mul(self, rhs: T) -> Self::Output {
        Block(self.0 * rhs)
    }
}

impl<T: Uint> MulAssign<T> for Block<T> {
    fn mul_assign(&mut self, rhs: T) {
        *self = Block(self.0 * rhs);
    }
}

impl<T: Uint> Div<T> for Block<T> {
    type Output = Self;

    fn div(self, rhs: T) -> Self::Output {
        Block(self.0 / rhs)
    }
}

impl<T: Uint + Sum> Sum for Block<T> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Block(iter.map(|b| b.0).sum())
    }
}
