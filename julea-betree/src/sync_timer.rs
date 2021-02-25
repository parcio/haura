use std::{ thread, time::Duration, sync::mpsc };
use super::ObjectStoreRef;

pub fn sync_timer<'b>(timeout_ms: u64, rx: mpsc::Receiver<ObjectStoreRef>) {
    let timeout = Duration::from_millis(timeout_ms);
    let mut stores = Vec::new();

    loop {
        thread::sleep(timeout);

        stores.extend(rx.try_iter());

        for store in &stores {
            if let Err(err) = store.begin_sync() {
                log::error!("couldn't sync object store: {}", err);
            }
        }
    }
}
