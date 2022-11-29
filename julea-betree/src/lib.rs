use julea_sys::{JTraceFileOperation::*, *};
use std::{
    convert::TryInto,
    ffi::{CStr, CString},
    fmt::Display,
    fs::File,
    pin::Pin,
    ptr, result, slice,
    sync::Arc,
    time::UNIX_EPOCH,
};

use dashmap::DashMap;
use log::error;
use parking_lot::RwLock;

use betree_storage_stack::{
    database::{self, DatabaseConfiguration},
    object::{self, ObjectInfo},
    StoragePreference,
};

mod jtrace;

error_chain::error_chain! {
    foreign_links {
        Json(serde_json::Error);
        Utf8(std::str::Utf8Error);
        Betree(betree_storage_stack::Error);
        Storage(betree_storage_stack::storage_pool::configuration::Error);
        Io(std::io::Error);
        SystemTime(std::time::SystemTimeError);
        ValueRange(std::num::TryFromIntError);
    }
}

type Database = database::Database<DatabaseConfiguration>;
type ObjectStore = object::ObjectStore<DatabaseConfiguration>;
type ObjectStoreRef<'b> = dashmap::mapref::one::Ref<'b, CString, Pin<Box<ObjectStore>>>;
type ObjectHandle<'os> = object::ObjectHandle<'os, DatabaseConfiguration>;

struct Backend {
    database: Arc<RwLock<Database>>,
    namespaces: DashMap<CString, Pin<Box<ObjectStore>>>,
}

impl Backend {
    fn ns<'b>(&'b self, namespace: &CStr) -> ObjectStoreRef<'b> {
        use dashmap::mapref::entry::Entry;
        // fast path, if already exists
        if let Some(os) = self.namespaces.get(namespace) {
            return os;
        }

        // not present, create it
        match self.namespaces.entry(namespace.to_owned()) {
            Entry::Occupied(e) => e.into_ref().downgrade(),
            Entry::Vacant(e) => e
                .insert(Box::pin(
                    self.database
                        .write()
                        .open_named_object_store(namespace.to_bytes(), StoragePreference::NONE)
                        .expect("Unable to open object store"),
                ))
                .downgrade(),
        }
    }
}

unsafe fn return_box<T, E: Display>(
    res: result::Result<T, E>,
    task: &str,
    out_ptr: *mut gpointer,
) -> gboolean {
    match res {
        Ok(val) => {
            out_ptr.cast::<*mut T>().write(Box::into_raw(Box::new(val)));
            TRUE
        }
        Err(err) => {
            out_ptr.cast::<*mut T>().write(ptr::null_mut());
            error!("couldn't {}: {}", task, err);
            FALSE
        }
    }
}

unsafe extern "C" fn backend_init(path: *const gchar, backend_data: *mut gpointer) -> gboolean {
    env_logger::init();

    let backend = || -> Result<_> {
        let path = CStr::from_ptr(path).to_str()?;
        let file = File::open(&path)?;
        let config: DatabaseConfiguration = serde_json::from_reader(&file)?;

        let db = Database::build_threaded(config)?;

        Ok(Backend {
            database: db,
            namespaces: DashMap::new(),
        })
    }();

    return_box(backend, "initialise backend", backend_data)
}

// This runs after exit handlers, so accessing TLS will fail
unsafe extern "C" fn backend_fini(backend_data: gpointer) {
    Box::from_raw(backend_data.cast::<Backend>());
}

unsafe extern "C" fn backend_create(
    backend_data: gpointer,
    namespace: *const gchar,
    path: *const gchar,
    backend_object: *mut gpointer,
) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let ns = backend.ns(CStr::from_ptr(namespace));
    let key = CStr::from_ptr(path);

    let (obj, _) = jtrace::with(J_TRACE_FILE_CREATE, path, || {
        let obj = ns.create_object(key.to_bytes());
        (obj, (0, 0))
    });

    return_box(obj, "create object", backend_object)
}

unsafe extern "C" fn backend_open(
    backend_data: gpointer,
    namespace: *const gchar,
    path: *const gchar,
    backend_object: *mut gpointer,
) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let ns = backend.ns(CStr::from_ptr(namespace));
    let key = CStr::from_ptr(path);

    let (obj, _) = jtrace::with(J_TRACE_FILE_OPEN, path, || {
        let obj = ns.open_object(key.to_bytes());
        (obj, (0, 0))
    });

    let result = match obj {
        Ok(None) => return FALSE,
        Ok(Some(handle)) => Ok(handle),
        Err(err) => Err(err),
    };

    return_box::<ObjectHandle, _>(result, "open object", backend_object)
}

unsafe extern "C" fn backend_delete(_backend_data: gpointer, backend_object: gpointer) -> gboolean {
    let handle = Box::from_raw(backend_object.cast::<ObjectHandle>());
    let key = handle.object.key().to_vec();

    let (res, _) = jtrace::with_once(J_TRACE_FILE_DELETE, &key, || {
        let res = handle.delete();
        (res, (0, 0))
    });

    if let Err(err) = res {
        error!("couldn't delete object: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_close(_backend_data: gpointer, backend_object: gpointer) -> gboolean {
    let handle = Box::from_raw(backend_object.cast::<ObjectHandle>());
    let key = handle.object.key().to_vec();

    let (res, _) = jtrace::with_once(J_TRACE_FILE_CLOSE, &key, || {
        let res = handle.close();
        (res, (0, 0))
    });

    if let Err(err) = res {
        error!("couldn't close object: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_status(
    _backend_data: gpointer,
    backend_object: gpointer,
    modification_time: *mut gint64,
    size: *mut guint64,
) -> gboolean {
    let handle = &*backend_object.cast::<ObjectHandle>();
    let key = handle.object.key().as_ptr().cast::<i8>();

    let (res, _): (Result<()>, _) = jtrace::with(J_TRACE_FILE_STATUS, key, || {
        let res = (|| {
            if let Ok(Some(info)) = handle.info() {
                modification_time.write(
                    info.mtime
                        .duration_since(UNIX_EPOCH)?
                        .as_micros()
                        .try_into()?,
                );
                size.write(info.size);
            }
            Ok(())
        })();
        (res, (0, 0))
    });

    if let Err(err) = res {
        error!("couldn't query object status: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_sync(backend_data: gpointer, backend_object: gpointer) -> gboolean {
    let backend = &mut *backend_data.cast::<Backend>();
    let handle = &*backend_object.cast::<ObjectHandle>();

    let (res, _) = jtrace::with(
        J_TRACE_FILE_SYNC,
        handle.object.key().as_ptr().cast::<i8>(),
        || {
            let res = backend.database.write().sync();
            (res, (0, 0))
        },
    );

    if let Err(err) = res {
        error!("couldn't sync database: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_read(
    _backend_data: gpointer,
    backend_object: gpointer,
    buffer: gpointer,
    length: guint64,
    offset: guint64,
    bytes_read: *mut guint64,
) -> gboolean {
    let handle = &*backend_object.cast::<ObjectHandle>();

    let (res, (n_read, _)) = jtrace::with(
        J_TRACE_FILE_READ,
        handle.object.key().as_ptr().cast::<i8>(),
        || {
            let res = handle.read_at(
                slice::from_raw_parts_mut(buffer.cast::<u8>(), length as usize),
                offset,
            );
            let bytes_read = match &res {
                Ok(n) => *n,
                Err((n, _)) => *n,
            };
            (res, (bytes_read, offset))
        },
    );

    if !bytes_read.is_null() {
        bytes_read.write(n_read);
    }

    match res {
        Ok(_) => TRUE,
        Err((_, err)) => {
            error!("couldn't read object data: {}", err);
            FALSE
        }
    }
}

unsafe extern "C" fn backend_write(
    _backend_data: gpointer,
    backend_object: gpointer,
    buffer: gconstpointer,
    length: guint64,
    offset: guint64,
    bytes_written: *mut guint64,
) -> gboolean {
    let handle = &mut *backend_object.cast::<ObjectHandle>();

    let (res, (n_written, _)) = jtrace::with(
        J_TRACE_FILE_WRITE,
        handle.object.key().as_ptr().cast::<i8>(),
        || {
            let res = handle.write_at(
                slice::from_raw_parts(buffer.cast::<u8>(), length as usize),
                offset,
            );
            let bytes_written = match &res {
                Ok(n) => *n,
                Err((n, _)) => *n,
            };
            (res, (bytes_written, offset))
        },
    );

    if !bytes_written.is_null() {
        bytes_written.write(n_written);
    }

    match res {
        Ok(_) => TRUE,
        Err((_, err)) => {
            error!("couldn't write object data: {}", err);
            FALSE
        }
    }
}

struct Iter<'os> {
    iter: Box<dyn Iterator<Item = (ObjectHandle<'os>, ObjectInfo)> + 'os>,
    name: Vec<u8>,
}

unsafe extern "C" fn backend_get_all(
    backend_data: gpointer,
    namespace: *const gchar,
    backend_iterator: *mut gpointer,
) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let ns = backend.ns(CStr::from_ptr(namespace));

    let iter = ns.list_objects::<_, &[u8]>(..).map(|iter| Iter {
        iter,
        name: Vec::new(),
    });

    return_box(iter, "iterate all objects", backend_iterator)
}

unsafe extern "C" fn backend_get_by_prefix(
    backend_data: gpointer,
    namespace: *const gchar,
    prefix: *const gchar,
    backend_iterator: *mut gpointer,
) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let ns = backend.ns(CStr::from_ptr(namespace));

    // Prefix listing is a special-case of range listing
    // A prefix search for "foo" can be implemented (and is here)
    // by a range search from "foo" to "foo\xFF"
    let mut key = CStr::from_ptr(prefix).to_owned().into_bytes();
    key.push(0xFF);

    let start = &key[..key.len() - 1];
    let end = &key[..];

    let iter = ns.list_objects(start..end).map(|iter| Iter {
        iter,
        name: Vec::new(),
    });

    return_box(iter, "iterate by prefix", backend_iterator)
}

unsafe extern "C" fn backend_iterate(
    _backend_data: gpointer,
    backend_iterator: gpointer,
    name: *mut *const gchar,
) -> gboolean {
    let iter = &mut *backend_iterator.cast::<Iter>();
    if let Some((handle, _info)) = iter.iter.next() {
        iter.name.clear();
        iter.name.extend_from_slice(handle.object.key());
        name.write(iter.name.as_ptr().cast::<i8>());
        TRUE
    } else {
        let _ = Box::from_raw(backend_iterator.cast::<Iter>());
        FALSE
    }
}

static mut BETREE_BACKEND: JBackend = JBackend {
    type_: JBackendType::J_BACKEND_TYPE_OBJECT,
    component: JBackendComponent::J_BACKEND_COMPONENT_SERVER
        | JBackendComponent::J_BACKEND_COMPONENT_CLIENT
        | JBackendComponent::J_BACKEND_COMPONENT_NOT_UNLOADABLE,
    data: ptr::null_mut(),
    anon1: JBackend__bindgen_ty_1 {
        object: JBackend__bindgen_ty_1__bindgen_ty_1 {
            backend_init: Some(backend_init),
            backend_fini: Some(backend_fini),
            backend_create: Some(backend_create),
            backend_open: Some(backend_open),
            backend_delete: Some(backend_delete),
            backend_close: Some(backend_close),
            backend_status: Some(backend_status),
            backend_sync: Some(backend_sync),
            backend_read: Some(backend_read),
            backend_write: Some(backend_write),
            backend_get_all: Some(backend_get_all),
            backend_get_by_prefix: Some(backend_get_by_prefix),
            backend_iterate: Some(backend_iterate),
        },
    },
};

#[no_mangle]
pub unsafe extern "C" fn backend_info() -> *mut JBackend {
    &mut BETREE_BACKEND
}
