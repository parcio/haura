use fuser::*;
use std::{
    borrow::Cow,
    cell::RefCell,
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;

use betree_storage_stack::{
    database::{self, DatabaseConfiguration},
    object::{self, Object},
    StoragePreference,
};

type Database = database::Database<DatabaseConfiguration>;
type ObjectStore = object::ObjectStore<DatabaseConfiguration>;
type ObjectHandle<'os> = object::ObjectHandle<'os, DatabaseConfiguration>;

const TTL: Duration = Duration::from_secs(60);
// Inos are just object ids, but have to be mapped back and forth because 1 is our root directory,
// so 0 and 1 can't be used for objects.
const INO_OFFSET: u64 = fuser::FUSE_ROOT_ID + 1;

pub struct BetreeFs {
    store: ObjectStore,
    handles: DashMap<u64, Object>,
    storage_preference: StoragePreference,
}

impl BetreeFs {
    pub fn new(store: ObjectStore, storage_preference: StoragePreference) -> Self {
        BetreeFs {
            store,
            handles: DashMap::new(),
            storage_preference,
        }
    }

    fn handle(&self, ino: u64) -> Option<ObjectHandle> {
        self.handles
            .get(&(ino - INO_OFFSET))
            .map(|obj| self.store.handle_from_object((*obj).clone()))
    }

    fn construct_key<'name>(&self, parent: u64, name: &'name OsStr) -> Option<Cow<'name, OsStr>> {
        if parent == fuser::FUSE_ROOT_ID {
            Some(Cow::Borrowed(name))
        } else if let Some(parent_obj) = self.handles.get(&parent) {
            Some(Cow::Owned(
                Path::new(OsStr::from_bytes(&parent_obj.key))
                    .join(name)
                    .into_os_string(),
            ))
        } else {
            None
        }
    }
}

trait ToFileAttr {
    fn to_file_attr(&self) -> fuser::FileAttr;
}

impl ToFileAttr for object::ObjectInfo {
    fn to_file_attr(&self) -> fuser::FileAttr {
        fuser::FileAttr {
            ino: self.object_id.as_u64() + INO_OFFSET,
            size: self.size,
            blocks: self.size / 512,
            atime: UNIX_EPOCH, // 1970-01-01 00:00:00
            mtime: self.mtime,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o666,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
            padding: 0,
        }
    }
}

static ROOT_ATTR: fuser::FileAttr = fuser::FileAttr {
    ino: fuser::FUSE_ROOT_ID,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o777,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
    blksize: 512,
    padding: 0,
};

impl fuser::Filesystem for BetreeFs {
    fn forget(&mut self, _req: &Request<'_>, ino: u64, _nlookup: u64) {
        self.handles.remove(&(ino - INO_OFFSET));
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if let Some(child_key) = self.construct_key(parent, name) {
            let maybe_child = self
                .store
                .open_object(&child_key.as_bytes(), self.storage_preference);

            if let Some((child, info)) = maybe_child.unwrap() {
                let mut attr = info.to_file_attr();
                let oid = info.object_id.as_u64();
                attr.ino = oid + INO_OFFSET;
                self.handles.insert(oid, child.object.clone());
                reply.entry(&TTL, &attr, 0)
            } else {
                reply.error(libc::ENOENT)
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if let Some(child_key) = self.construct_key(parent, name) {
            let maybe_child = self
                .store
                .create_object(&child_key.as_bytes(), self.storage_preference);

            if let Ok((child, info)) = maybe_child {
                let mut attr = info.to_file_attr();
                let oid = info.object_id.as_u64();
                attr.ino = oid + INO_OFFSET;
                self.handles.insert(oid, child.object.clone());
                reply.created(&TTL, &attr, 0, 0, 0)
            } else {
                reply.error(libc::EIO)
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok()
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == fuser::FUSE_ROOT_ID {
            reply.attr(&TTL, &ROOT_ATTR);
            return;
        }

        let info = self.handle(ino).and_then(|obj| obj.info().ok().flatten());

        if let Some(info) = info {
            let attr = info.to_file_attr();
            reply.attr(&TTL, &attr)
        } else {
            reply.error(libc::ENOENT)
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        self.getattr(req, ino, reply);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        thread_local! {
            static READ_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::new());
        }

        READ_BUF.with(|buf| {
            if let Some(obj) = self.handle(ino) {
                let mut vec = buf.borrow_mut();
                vec.resize(size as usize, 0);
                match obj.read_at(&mut vec, offset as u64) {
                    Ok(n) => reply.data(&vec[..n as usize]),
                    Err((_n_read, _err)) => {
                        reply.error(libc::EIO);
                    }
                }
            } else {
                reply.error(libc::ENOENT)
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        assert!(offset >= 0);
        if let Some(obj) = self.handle(ino) {
            match obj.write_at(data, offset as u64) {
                Ok(n) => reply.written(n as u32),
                Err((_n_written, _err)) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // There is only the root directory for now
        if ino != fuser::FUSE_ROOT_ID {
            reply.error(libc::ENOENT);
            return;
        }

        if let Ok(iter) = self.store.list_objects::<_, &[u8]>(..) {
            for (idx, (obj, info)) in iter.enumerate().skip(offset as usize) {
                let ino = info.object_id.as_u64() + INO_OFFSET;
                let full = reply.add(
                    ino,
                    idx as i64 + 1,
                    FileType::RegularFile,
                    OsStr::from_bytes(&obj.object.key),
                );
                if full {
                    break;
                }
            }

            reply.ok();
        } else {
            reply.error(libc::EIO);
        }

        /*let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }*/
    }

    /// Get file system statistics.
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }
}
