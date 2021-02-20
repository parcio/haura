use julea_sys::{JTraceFileOperation::*, *};
use std::{
    convert::TryInto, ffi::CStr, fmt::Display, fs::File, ptr, result, slice, time::UNIX_EPOCH,
};

use log::error;

use betree_storage_stack::{
    database::{self, DatabaseConfiguration},
    object::{self, ObjectStore},
    storage_pool::StorageConfiguration,
};

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
type Object<'os> = object::Object<'os, DatabaseConfiguration>;

#[derive(serde::Deserialize, serde::Serialize)]
struct Configuration {
    storage: Vec<String>,
}

struct Backend {
    database: Database,
    object_store: ObjectStore<DatabaseConfiguration>,
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
            error!("couldn't {}: {}", task, err);
            FALSE
        }
    }
}

unsafe fn with_j_trace<T>(
    op: JTraceFileOperation,
    path: *const i8,
    mut f: impl FnMut() -> (T, (u64, u64)),
) -> (T, (u64, u64)) {
    j_trace_file_begin(path, op);
    let (val, (length, offset)) = f();
    j_trace_file_end(path, op, length, offset);
    (val, (length, offset))
}

unsafe extern "C" fn backend_init(path: *const gchar, backend_data: *mut gpointer) -> gboolean {
    env_logger::init();

    let backend = || -> Result<_> {
        let path = CStr::from_ptr(path).to_str()?;
        let file = File::open(&path)?;
        let config: Configuration = serde_json::from_reader(&file)?;

        let mut db = Database::build(DatabaseConfiguration {
            storage: StorageConfiguration::parse_zfs_like(config.storage)?,
            ..Default::default()
        })?;

        let os = db.open_object_store()?;

        Ok(Backend {
            database: db,
            object_store: os,
        })
    }();

    return_box(backend, "initialise backend", backend_data)
}

unsafe extern "C" fn backend_fini(backend_data: gpointer) {
    let mut backend = Box::from_raw(backend_data.cast::<Backend>());
    if let Err(err) = backend.database.sync() {
        error!("couldn't sync database during fini: {}", err);
    }
}

unsafe extern "C" fn backend_create(
    backend_data: gpointer,
    namespace: *const gchar,
    path: *const gchar,
    backend_object: *mut gpointer,
) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let key = CStr::from_ptr(path);

    let (obj, _) = with_j_trace(J_TRACE_FILE_CREATE, path, || {
        let obj = backend.object_store.create_object(key.to_bytes());
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
    let key = CStr::from_ptr(path);

    let (obj, _) = with_j_trace(J_TRACE_FILE_OPEN, path, || {
        let obj = backend.object_store.open_object(key.to_bytes());
        (obj, (0, 0))
    });

    return_box(obj, "open object", backend_object)
}

unsafe extern "C" fn backend_delete(backend_data: gpointer, backend_object: gpointer) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let obj = Box::from_raw(backend_object.cast::<Object>());
    let key = obj.key.as_ptr().cast::<i8>();

    let (res, _) = with_j_trace(J_TRACE_FILE_DELETE, key, || {
        let res = backend.object_store.delete_object(&obj);
        (res, (0, 0))
    });

    if let Err(err) = res {
        error!("couldn't delete object: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_close(backend_data: gpointer, backend_object: gpointer) -> gboolean {
    let backend = &*backend_data.cast::<Backend>();
    let obj = Box::from_raw(backend_object.cast::<Object>());
    let key = obj.key.as_ptr().cast::<i8>();

    let (res, _) = with_j_trace(J_TRACE_FILE_CLOSE, key, || {
        let res = backend.object_store.close_object(&obj);
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
    backend_data: gpointer,
    backend_object: gpointer,
    modification_time: *mut gint64,
    size: *mut guint64,
) -> gboolean {
    let obj = &*backend_object.cast::<Object>();
    let key = obj.key.as_ptr().cast::<i8>();

    let (res, _): (Result<()>, _) = with_j_trace(J_TRACE_FILE_STATUS, key, || {
        let res = (|| {
            modification_time.write(
                obj.modification_time()
                    .duration_since(UNIX_EPOCH)?
                    .as_micros()
                    .try_into()?,
            );
            size.write(obj.size());
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
    let obj = &*backend_object.cast::<Object>();

    let (res, _) = with_j_trace(J_TRACE_FILE_SYNC, obj.key.as_ptr().cast::<i8>(), || {
        let res = backend.database.sync();
        (res, (0, 0))
    });

    if let Err(err) = res {
        error!("couldn't sync database: {}", err);
        FALSE
    } else {
        TRUE
    }
}

unsafe extern "C" fn backend_read(
    backend_data: gpointer,
    backend_object: gpointer,
    buffer: gpointer,
    length: guint64,
    offset: guint64,
    bytes_read: *mut guint64,
) -> gboolean {
    let obj = &*backend_object.cast::<Object>();

    let (res, (n_read, _)) = with_j_trace(J_TRACE_FILE_READ, obj.key.as_ptr().cast::<i8>(), || {
        let res = obj.read_at(
            slice::from_raw_parts_mut(buffer.cast::<u8>(), length as usize),
            offset,
        );
        let bytes_read = match &res {
            Ok(n) => *n,
            Err((n, _)) => *n,
        };
        (res, (bytes_read, offset))
    });

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
    backend_data: gpointer,
    backend_object: gpointer,
    buffer: gconstpointer,
    length: guint64,
    offset: guint64,
    bytes_written: *mut guint64,
) -> gboolean {
    let obj = &mut *backend_object.cast::<Object>();

    let (res, (n_written, _)) =
        with_j_trace(J_TRACE_FILE_WRITE, obj.key.as_ptr().cast::<i8>(), || {
            let res = obj.write_at(
                slice::from_raw_parts(buffer.cast::<u8>(), length as usize),
                offset,
            );
            let bytes_written = match &res {
                Ok(n) => *n,
                Err((n, _)) => *n,
            };
            (res, (bytes_written, offset))
        });

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

static mut BETREE_BACKEND: JBackend = JBackend {
    type_: JBackendType::J_BACKEND_TYPE_OBJECT,
    component: JBackendComponent::J_BACKEND_COMPONENT_SERVER,
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
        },
    },
};

#[no_mangle]
pub unsafe extern "C" fn backend_info() -> *mut JBackend {
    &mut BETREE_BACKEND
}
