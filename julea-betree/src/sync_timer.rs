use super::Database;
use std::{
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

pub fn sync_timer<'b>(timeout_ms: u64, db: Arc<RwLock<Database>>) {
    let timeout = Duration::from_millis(timeout_ms);

    loop {
        thread::sleep(timeout);

        log::debug!("syncing db");
        if let Err(err) = db.write().unwrap().sync() {
            log::error!("couldn't sync db: {}", err);
        }
    }
}
