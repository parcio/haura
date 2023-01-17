// let Ok(n) | Err((n, _)) = res; is not stable yet, so use if-let for now
#![allow(irrefutable_let_patterns)]

use super::ObjectHandle;
use crate::{
    StoragePreference,
    database::{Error as DbError, ErrorKind as DbErrorKind},
};

use std::io::{self, Read, Seek, SeekFrom, Write};

/// A streaming interface for [ObjectHandle]s, allowing the use of [Read], [Write], and [Seek]
/// to interoperate with other libraries. Additionally, the per-object storage preference can
/// be overridden with [ObjectHandle::cursor_with_pref] and [ObjectCursor::set_storage_preference].
pub struct ObjectCursor<'handle, 'r> {
    handle: &'r ObjectHandle<'handle>,
    pos: u64,
    pref: StoragePreference,
}

impl<'handle> ObjectHandle<'handle> {
    /// Create a cursor with a storage preference override, at position 0.
    pub fn cursor_with_pref<'r>(
        &'handle self,
        pref: StoragePreference,
    ) -> ObjectCursor<'handle, 'r> {
        ObjectCursor {
            handle: self,
            pos: 0,
            pref,
        }
    }

    /// Create a cursor without a storage preference override, at position 0.
    pub fn cursor<'r>(&'handle self) -> ObjectCursor<'handle, 'r> {
        self.cursor_with_pref(StoragePreference::NONE)
    }
}

impl<'handle, 'r> ObjectCursor<'handle, 'r> {
    /// Override the storage preference to use for future operations with this cursor.
    pub fn set_storage_preference(&mut self, pref: StoragePreference) {
        self.pref = pref;
    }
}

fn convert_res(db_res: Result<u64, (u64, DbError)>) -> io::Result<usize> {
    match db_res {
        Ok(n) => Ok(n as usize),
        Err((_n, e)) => Err(convert_err(e)),
    }
}

fn convert_err(DbError(kind, _): DbError) -> io::Error {
    match kind {
        DbErrorKind::Io(io_err) => io_err,
        // FIXME: this eats io::Errors hidden deeper into the result chain
        e => {
            dbg!("Encountered error: {:?}", e);
            io::Error::from(io::ErrorKind::Other)
        }
    }
}

impl<'a, 'b> Read for ObjectCursor<'a, 'b> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let res = self.handle.read_at(buf, self.pos);

        if let Ok(n) | Err((n, _)) = res {
            self.pos += n;
        }

        convert_res(res)
    }
}

impl<'a, 'b> Write for ObjectCursor<'a, 'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.handle.write_at_with_pref(buf, self.pos, self.pref);

        if let Ok(n) | Err((n, _)) = res {
            self.pos += n;
        }

        convert_res(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, 'b> Seek for ObjectCursor<'a, 'b> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        fn add_u64_i64(base: u64, delta: i64) -> Option<u64> {
            if delta >= 0 {
                base.checked_add(delta as u64)
            } else {
                base.checked_sub(delta.wrapping_neg() as u64)
            }
        }

        match target {
            SeekFrom::Start(new_pos) => {
                self.pos = new_pos;
                Ok(new_pos)
            }
            SeekFrom::End(delta) => {
                let info = self.handle.info().map_err(convert_err)?;

                if let Some(info) = info {
                    if let Some(new_pos) = add_u64_i64(info.size, delta) {
                        self.pos = new_pos;
                        Ok(new_pos)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "position under-/overflow",
                        ))
                    }
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "size query failed, possibly because object was deleted",
                    ))
                }
            }
            SeekFrom::Current(delta) => {
                if let Some(new_pos) = add_u64_i64(self.pos, delta) {
                    self.pos = new_pos;
                    Ok(new_pos)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "position under-/overflow",
                    ))
                }
            }
        }
    }
}
