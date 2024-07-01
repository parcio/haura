//! Storage Pool configuration.
#[cfg(feature = "nvm")]
use pmdk;

use crate::{
    tree::StorageKind,
    vdev::{self, Dev, Leaf},
    StoragePreference,
};
use itertools::Itertools;
use libc;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::{
    convert::TryFrom, fmt, fmt::Write, fs::OpenOptions, io, iter::FromIterator,
    os::unix::io::AsRawFd, path::PathBuf, slice,
};

/// Access pattern descriptor to differentiate and optimize drive usage. Useful
/// when working with [crate::object::ObjectStore] with a defined with access pattern. Assignable to
/// [TierConfiguration].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Writable, Readable)]
pub enum PreferredAccessType {
    /// The default access pattern. No assumptions are made.
    Unknown = 0,
    /// Preferred access is a random read pattern, this can be for example a SSD to reduce wear-out.
    RandomRead = 1,
    /// Preferred access is a random write pattern, this can be for example some otherwise cached file or NVM.
    RandomWrite = 2,
    /// Preferred access is a random read and write pattern, this can be for example a SSD.
    RandomReadWrite = 3,
    /// Preferred access is a sequential write pattern, this can be for example tape storage for archival.
    SequentialWrite = 4,
    /// Preferred access is a sequential read pattern, this can be for example a HDD.
    SequentialRead = 5,
    /// Preferred access is a sequential read and write pattern, this can be for example a HDD mirror1 setup.
    SequentialReadWrite = 6,
}

impl PreferredAccessType {
    /// Convert to C-like enum values.
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl TryFrom<u8> for PreferredAccessType {
    type Error = ();

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            v if v == Self::Unknown as u8 => Ok(Self::Unknown),
            v if v == Self::RandomRead as u8 => Ok(Self::RandomRead),
            v if v == Self::RandomWrite as u8 => Ok(Self::RandomWrite),
            v if v == Self::RandomReadWrite as u8 => Ok(Self::RandomReadWrite),
            v if v == Self::SequentialRead as u8 => Ok(Self::SequentialRead),
            v if v == Self::SequentialWrite as u8 => Ok(Self::SequentialWrite),
            v if v == Self::SequentialReadWrite as u8 => Ok(Self::SequentialReadWrite),
            _ => Err(()),
        }
    }
}

impl Default for PreferredAccessType {
    fn default() -> Self {
        PreferredAccessType::Unknown
    }
}

/// Configuration of a single storage class.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierConfiguration {
    /// The collection of virtual devices backing this tier.
    pub top_level_vdevs: Vec<Vdev>,
    /// Which storage access is preferred to be used with this tier. See
    /// [PreferredAccessType] for all variants.
    pub preferred_access_type: PreferredAccessType,
    /// Which medium this layer is made of.
    pub storage_kind: StorageKind,
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

impl StoragePoolConfiguration {
    /// Returns whether the given storage preference is backed by memory.
    pub fn pref_is_memory(&self, pref: StoragePreference) -> bool {
        match self.tiers.get(pref.as_u8() as usize) {
            Some(tier) => tier.is_memory(),
            _ => false,
        }
    }
}

/// Represents a top-level vdev.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields, rename_all = "lowercase")]
pub enum Vdev {
    /// This vdev is a leaf vdev.
    Leaf(LeafVdev),
    /// This vdev is a mirror vdev.
    Mirror {
        /// Constituent vdevs of this mirror
        mirror: Vec<LeafVdev>,
    },
    /// Parity1 aka RAID5.
    Parity1 {
        /// Constituent vdevs of this parity1 aggregation
        parity1: Vec<LeafVdev>,
    },
}

/// Represents a leaf vdev.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields, rename_all = "lowercase")]
pub enum LeafVdev {
    #[cfg(feature = "nvm")]
    /// Backed by NVM with `pmdk`.
    PMemFile {
        /// Path to the underlying file.
        path: PathBuf,
        /// Size of backing file in bytes.
        len: usize,
    },
    /// Backed by a file or disk.
    File(PathBuf),
    /// Customisable file vdev.
    FileWithOpts {
        /// Path to file or block device
        path: PathBuf,
        /// Whether to use direct IO for this file. Defaults to true.
        direct: Option<bool>,
    },
    /// Backed by a memory buffer.
    Memory {
        /// Size of memory vdev in bytes.
        mem: usize,
    },
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
        TierConfiguration {
            top_level_vdevs,
            preferred_access_type: PreferredAccessType::Unknown,
            storage_kind: StorageKind::Hdd,
        }
    }

    /// Opens file and devices and constructs a `Vec<Vdev>`.
    pub(crate) fn build(&self) -> io::Result<Vec<Dev>> {
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
                "mirror" => |leaves| Vdev::Mirror { mirror: leaves },
                "parity" | "parity1" => |leaves| Vdev::Parity1 { parity1: leaves },
                _ => bail!(ErrorKind::InvalidKeyword),
            };
            let leaves = iter
                .peeking_take_while(is_path)
                .map(|s| LeafVdev::from(s.as_ref()))
                .collect();
            v.push(f(leaves));
        }
        Ok(TierConfiguration {
            top_level_vdevs: v,
            preferred_access_type: PreferredAccessType::Unknown,
            storage_kind: StorageKind::Hdd,
        })
    }

    /// Returns the configuration in a ZFS-like string representation.
    ///
    /// See `parse_zfs_like` for more information.
    pub fn zfs_like(&self) -> String {
        let mut s = String::new();
        for vdev in &self.top_level_vdevs {
            let (keyword, leaves) = match *vdev {
                Vdev::Leaf(ref leaf) => ("", slice::from_ref(leaf)),
                Vdev::Mirror { mirror: ref leaves } => ("mirror ", &leaves[..]),
                Vdev::Parity1 {
                    parity1: ref leaves,
                } => ("parity1 ", &leaves[..]),
            };
            s.push_str(keyword);
            for leaf in leaves {
                match leaf {
                    LeafVdev::File(path) => write!(s, "{} ", path.display()).unwrap(),
                    LeafVdev::FileWithOpts { path, direct } => {
                        write!(s, "{} (direct: {:?}) ", path.display(), direct).unwrap()
                    }
                    LeafVdev::Memory { mem } => write!(s, "memory({mem}) ").unwrap(),
                    #[cfg(feature = "nvm")]
                    LeafVdev::PMemFile { path, len } => {
                        write!(s, "{} {}", path.display(), len).unwrap()
                    }
                }
            }
        }
        s.pop();
        s
    }
}

fn is_path<S: AsRef<str> + ?Sized>(s: &S) -> bool {
    matches!(s.as_ref().chars().next(), Some('.') | Some('/'))
}

impl FromIterator<Vdev> for TierConfiguration {
    fn from_iter<T: IntoIterator<Item = Vdev>>(iter: T) -> Self {
        TierConfiguration {
            top_level_vdevs: iter.into_iter().collect(),
            preferred_access_type: PreferredAccessType::Unknown,
            storage_kind: StorageKind::Hdd,
        }
    }
}

impl Vdev {
    /// Opens file and devices and constructs a `Vdev`.
    fn build(&self, n: usize) -> io::Result<Dev> {
        match *self {
            Vdev::Mirror { mirror: ref vec } => {
                let leaves: io::Result<Vec<Leaf>> = vec.iter().map(LeafVdev::build).collect();
                let leaves: Box<[Leaf]> = leaves?.into_boxed_slice();
                Ok(Dev::Mirror(vdev::Mirror::new(
                    leaves,
                    format!("mirror-{n}"),
                )))
            }
            Vdev::Parity1 { parity1: ref vec } => {
                let leaves: io::Result<Vec<_>> = vec.iter().map(LeafVdev::build).collect();
                let leaves = leaves?.into_boxed_slice();
                Ok(Dev::Parity1(vdev::Parity1::new(
                    leaves,
                    format!("parity-{n}"),
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
            LeafVdev::File(_) | LeafVdev::FileWithOpts { .. } => {
                let (path, direct) = match self {
                    LeafVdev::File(path) => (path, true),
                    LeafVdev::FileWithOpts { path, direct } => (path, direct.unwrap_or(true)),
                    LeafVdev::Memory { .. } => unreachable!(),
                    #[cfg(feature = "nvm")]
                    LeafVdev::PMemFile { .. } => unreachable!(),
                };

                let mut file = OpenOptions::new();
                file.read(true).write(true);
                if direct {
                    file.custom_flags(libc::O_DIRECT);
                }
                let file = file.open(path)?;

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
            LeafVdev::Memory { mem } => Ok(Leaf::Memory(vdev::Memory::new(
                mem,
                format!("memory-{mem}"),
            )?)),
            #[cfg(feature = "nvm")]
            LeafVdev::PMemFile { ref path, len } => {
                let file = match pmdk::PMem::open(path) {
                    Ok(handle) => handle,
                    Err(open_err) => match pmdk::PMem::create(path, len) {
                        Ok(handle) => handle,
                        Err(create_err) => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!(
                                    "Failed to create or open handle for pmem file. Path: {} - Open Error {} -Create Error {}",
                                    path.display(),
                                    open_err,
                                    create_err,
                                ),
                            ));
                        }
                    },
                };
                if file.len() != len {
                    return Err(io::Error::new(io::ErrorKind::Other,
                                    format!("The file already exists with a different length. Provided length: {}, File's length: {}",
                                            len, file.len())));
                }

                Ok(Leaf::PMemFile(vdev::PMemFile::new(
                    file,
                    path.to_string_lossy().into_owned(),
                )?))
            }
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
            Vdev::Mirror { ref mirror } => {
                writeln!(f, "{:indent$}mirror", "", indent = indent)?;
                for vdev in mirror {
                    vdev.display(indent + 4, f)?;
                }
                Ok(())
            }
            Vdev::Parity1 { ref parity1 } => {
                writeln!(f, "{:indent$}parity1", "", indent = indent)?;
                for vdev in parity1 {
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
            LeafVdev::FileWithOpts { path, direct } => {
                writeln!(
                    f,
                    "{:indent$}{} (direct: {:?})",
                    "",
                    path.display(),
                    direct,
                    indent = indent
                )
            }
            LeafVdev::Memory { mem } => {
                writeln!(f, "{:indent$}memory({})", "", mem, indent = indent)
            }
            #[cfg(feature = "nvm")]
            LeafVdev::PMemFile { path, len: _ } => {
                writeln!(f, "{:indent$}{}", "", path.display(), indent = indent)
            }
        }
    }
}
