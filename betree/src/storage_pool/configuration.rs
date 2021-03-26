//! Storage Pool configuration.
use crate::vdev::{self, Dev, Leaf};
use itertools::Itertools;
use libc;
use ref_slice::ref_slice;
use serde::{Deserialize, Serialize};
use std::{
    fmt, fmt::Write, fs::OpenOptions, io, iter::FromIterator, os::unix::io::AsRawFd, path::PathBuf,
};

/// Configuration of a single storage class.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TierConfiguration {
    top_level_vdevs: Vec<Vdev>,
}

/// Configuration for the storage pool unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoragePoolConfiguration {
    /// Storage classes to make use of
    pub tiers: Vec<TierConfiguration>,
    /// The queue length is the product of this factor and the number of disks involved
    pub queue_depth_factor: u32,
    /// Upper limit for concurrent IO operations
    pub thread_pool_size: Option<u32>,
    /// Whether to pin each worker thread to a CPU core
    pub thread_pool_pinned: bool,
}

impl Default for StoragePoolConfiguration {
    fn default() -> Self {
        Self {
            tiers: Vec::new(),
            queue_depth_factor: 20,
            thread_pool_size: None,
            thread_pool_pinned: false,
        }
    }
}

/// Represents a top-level vdev.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Vdev {
    /// This vdev is a leaf vdev.
    Leaf(LeafVdev),
    /// This vdev is a mirror vdev.
    Mirror(Vec<LeafVdev>),
    /// Parity1 aka RAID5.
    Parity1(Vec<LeafVdev>),
}

/// Represents a leaf vdev.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum LeafVdev {
    /// Backed by a file or disk.
    File(PathBuf),
    /// Backed by a memory buffer.
    Memory(usize),
}

error_chain! {
    errors {
        #[allow(missing_docs)]
        InvalidKeyword
    }
}

impl TierConfiguration {
    /// Returns a new `StorageConfiguration` based on the given top-level vdevs.
    pub fn new(top_level_vdevs: Vec<Vdev>) -> Self {
        TierConfiguration { top_level_vdevs }
    }

    /// Opens file and devices and constructs a `Vec<Vdev>`.
    pub fn build(&self) -> io::Result<Vec<Dev>> {
        self.top_level_vdevs
            .iter()
            .enumerate()
            .map(|(n, v)| v.build(n))
            .collect()
    }

    /// Parses the configuration from a ZFS-like representation.
    ///
    /// This representation is a sequence of top-level vdevs.
    /// The keywords `mirror` and `parity1` signal
    /// that all immediate following devices shall be grouped in such a vdev.
    ///
    /// # Example
    /// `/dev/sda mirror /dev/sdb /dev/sdc parity1 /dev/sdd /dev/sde /dev/sdf`
    /// results in three top-level vdevs: `/dev/sda`, a mirror vdev, and a
    /// parity1 vdev. The mirror vdev contains `/dev/sdb` and `/dev/sdc`.
    /// The parity1 vdev contains `/dev/sdd`, `/dev/sde`, and `/dev/sdf`.
    pub fn parse_zfs_like<I, S>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut iter = iter.into_iter().peekable();
        let mut v = Vec::new();
        while let Some(s) = iter.next() {
            let s = s.as_ref();
            if is_path(s) {
                v.push(Vdev::Leaf(LeafVdev::from(s)));
                continue;
            }
            let f = match s {
                "mirror" => Vdev::Mirror,
                "parity" | "parity1" => Vdev::Parity1,
                _ => bail!(ErrorKind::InvalidKeyword),
            };
            let leaves = iter
                .peeking_take_while(is_path)
                .map(|s| LeafVdev::from(s.as_ref()))
                .collect();
            v.push(f(leaves));
        }
        Ok(TierConfiguration { top_level_vdevs: v })
    }

    /// Returns the configuration in a ZFS-like string representation.
    ///
    /// See `parse_zfs_like` for more information.
    pub fn zfs_like(&self) -> String {
        let mut s = String::new();
        for vdev in &self.top_level_vdevs {
            let (keyword, leaves) = match *vdev {
                Vdev::Leaf(ref leaf) => ("", ref_slice(leaf)),
                Vdev::Mirror(ref leaves) => ("mirror ", &leaves[..]),
                Vdev::Parity1(ref leaves) => ("parity1 ", &leaves[..]),
            };
            s.push_str(keyword);
            for leaf in leaves {
                match leaf {
                    LeafVdev::File(path) => write!(s, "{} ", path.display()).unwrap(),
                    LeafVdev::Memory(size) => write!(s, "memory({}) ", size).unwrap(),
                }
            }
        }
        s.pop();
        s
    }
}

fn is_path<S: AsRef<str> + ?Sized>(s: &S) -> bool {
    match s.as_ref().chars().next() {
        Some('.') | Some('/') => true,
        _ => false,
    }
}

impl FromIterator<Vdev> for TierConfiguration {
    fn from_iter<T: IntoIterator<Item = Vdev>>(iter: T) -> Self {
        TierConfiguration {
            top_level_vdevs: iter.into_iter().collect(),
        }
    }
}

impl Vdev {
    /// Opens file and devices and constructs a `Vdev`.
    fn build(&self, n: usize) -> io::Result<Dev> {
        match *self {
            Vdev::Mirror(ref vec) => {
                let leaves: io::Result<Vec<Leaf>> = vec.iter().map(LeafVdev::build).collect();
                let leaves: Box<[Leaf]> = leaves?.into_boxed_slice();
                Ok(Dev::Mirror(vdev::Mirror::new(
                    leaves,
                    format!("mirror-{}", n),
                )))
            }
            Vdev::Parity1(ref vec) => {
                let leaves: io::Result<Vec<_>> = vec.iter().map(LeafVdev::build).collect();
                let leaves = leaves?.into_boxed_slice();
                Ok(Dev::Parity1(vdev::Parity1::new(
                    leaves,
                    format!("parity-{}", n),
                )))
            }
            Vdev::Leaf(ref leaf) => leaf.build().map(Dev::Leaf),
        }
    }
}

impl LeafVdev {
    fn build(&self) -> io::Result<Leaf> {
        use std::os::unix::fs::OpenOptionsExt;

        match *self {
            LeafVdev::File(ref path) => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(&path)?;
                if unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) }
                    != 0
                {
                    return Err(io::Error::last_os_error());
                }

                Ok(Leaf::File(vdev::File::new(
                    file,
                    path.to_string_lossy().into_owned(),
                )?))
            }
            LeafVdev::Memory(size) => Ok(Leaf::Memory(vdev::Memory::new(
                size,
                format!("memory-{}", size),
            )?)),
        }
    }
}

impl<'a> From<&'a str> for LeafVdev {
    fn from(s: &'a str) -> Self {
        LeafVdev::File(PathBuf::from(s))
    }
}

impl fmt::Display for TierConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for vdev in &self.top_level_vdevs {
            vdev.display(0, f)?;
        }
        Ok(())
    }
}

impl Vdev {
    fn display(&self, indent: usize, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Vdev::Leaf(ref leaf) => leaf.display(indent, f),
            Vdev::Mirror(ref mirror) => {
                writeln!(f, "{:indent$}mirror", "", indent = indent)?;
                for vdev in mirror {
                    vdev.display(indent + 4, f)?;
                }
                Ok(())
            }
            Vdev::Parity1(ref parity) => {
                writeln!(f, "{:indent$}parity1", "", indent = indent)?;
                for vdev in parity {
                    vdev.display(indent + 4, f)?;
                }
                Ok(())
            }
        }
    }
}

impl LeafVdev {
    fn display(&self, indent: usize, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LeafVdev::File(path) => {
                writeln!(f, "{:indent$}{}", "", path.display(), indent = indent)
            }
            LeafVdev::Memory(size) => {
                writeln!(f, "{:indent$}memory({})", "", size, indent = indent)
            }
        }
    }
}
