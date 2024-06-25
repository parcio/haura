use super::Database;
use parking_lot::RwLock;
use std::{sync::Arc, thread, time::Duration};

pub fn sync_timer(timeout_ms: u64, db: Arc<RwLock<Database>>) {
    let timeout = Duration::from_millis(timeout_ms);

    loop {
        thread::sleep(timeout);
        println!("\nsyncing db");
        log::debug!("syncing db");
        if let Err(err) = db.write().sync() {
            log::error!("couldn't sync db: {}", err);
        }
    }
}
