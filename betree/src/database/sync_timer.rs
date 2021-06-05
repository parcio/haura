use super::{Database, DatabaseBuilder};
use parking_lot::RwLock;
use std::{sync::Arc, thread, time::Duration};

pub fn sync_timer<'b, Config: DatabaseBuilder>(timeout_ms: u64, db: Arc<RwLock<Database<Config>>>) {
    let timeout = Duration::from_millis(timeout_ms);

    loop {
        thread::sleep(timeout);

        log::debug!("syncing db");
        if let Err(err) = db.write().sync() {
            log::error!("couldn't sync db: {}", err);
        }
    }
}
