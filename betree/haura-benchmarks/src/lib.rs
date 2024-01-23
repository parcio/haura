use std::{
    env,
    fs::File,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use betree_storage_stack::{
    database::{self, AccessMode},
    env_logger::init_env_logger,
    metrics, object, DatabaseConfiguration, StoragePreference,
};
use figment::providers::Format;
use parking_lot::RwLock;
use procfs::process::Process;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;

pub mod bufreader;

pub type Database = database::Database;
pub type Dataset = database::Dataset;
pub type ObjectStore = object::ObjectStore;

pub struct Control {
    pub database: Arc<RwLock<Database>>,
}

impl Control {
    pub fn with_custom_config(modify_cfg: impl Fn(&mut DatabaseConfiguration)) -> Self {
        init_env_logger();

        let conf_path = env::var("BETREE_CONFIG").expect("Didn't provide a BETREE_CONFIG");

        let mut cfg: DatabaseConfiguration = figment::Figment::new()
            .merge(DatabaseConfiguration::figment_default())
            .merge(figment::providers::Json::file(conf_path))
            .merge(DatabaseConfiguration::figment_env())
            .extract()
            .expect("Failed to extract DatabaseConfiguration");

        cfg.access_mode = AccessMode::AlwaysCreateNew;

        cfg.sync_interval_ms = Some(1000);

        cfg.metrics = Some(metrics::MetricsConfiguration {
            enabled: true,
            interval_ms: 500,
            output_path: PathBuf::from("betree-metrics.jsonl"),
        });

        modify_cfg(&mut cfg);

        log::info!("using {:?}", cfg);

        let database = Database::build_threaded(cfg).expect("Failed to open database");

        Control { database }
    }

    pub fn new() -> Self {
        Self::with_custom_config(|_| {})
    }

    pub fn client(&mut self, id: u32, task: &[u8]) -> Client {
        let mut lock = self.database.write();

        let os = lock
            .open_named_object_store(task, StoragePreference::NONE)
            .expect("Failed to create/open object store");
        Client {
            database: self.database.clone(),
            rng: Xoshiro256Plus::seed_from_u64(id as u64),
            object_store: os,
        }
    }
}

pub struct Client {
    pub database: Arc<RwLock<Database>>,
    pub rng: Xoshiro256Plus,
    pub object_store: ObjectStore,
}

impl Client {
    pub fn sync(&self) -> database::Result<()> {
        self.database.write().sync()
    }
}

pub fn log_process_info(path: impl AsRef<Path>, interval_ms: u64) -> io::Result<()> {
    let file = File::create(path)?;
    let mut output = BufWriter::new(file);
    let interval = Duration::from_millis(interval_ms);

    let ticks = procfs::ticks_per_second().expect("Couldn't query tick frequency") as f64;
    let page_size = procfs::page_size().expect("Couldn't query page size");

    loop {
        let now = Instant::now();

        if let Ok(proc) = Process::myself() {
            let info = serde_json::json!({
                "vsize": proc.stat.vsize,
                "rss": proc.stat.rss * page_size,
                "utime": proc.stat.utime as f64 / ticks,
                "stime": proc.stat.stime as f64 / ticks,
                "cutime": proc.stat.cutime as f64 / ticks,
                "cstime": proc.stat.cstime as f64 / ticks,
                "minflt": proc.stat.minflt,
                "cminflt": proc.stat.cminflt,
                "majflt": proc.stat.majflt,
                "cmajflt": proc.stat.cmajflt
            });

            serde_json::to_writer(
                &mut output,
                &serde_json::json!({
                    "epoch_ms": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(u128::MAX) as u64,
                    "proc": info
                }),
            )?;
            output.write_all(b"\n")?;
            output.flush()?;
        }

        thread::sleep(interval.saturating_sub(now.elapsed()));
    }
}

pub fn with_random_bytes<E>(
    mut rng: impl Rng,
    mut n_bytes: u64,
    buf_size: usize,
    mut callback: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    let mut buf = vec![0; buf_size];
    while n_bytes > 0 {
        rng.fill(&mut buf[..]);
        if let Err(e) = callback(&buf[..buf_size.min(n_bytes as usize)]) {
            return Err(e);
        }
        n_bytes = n_bytes.saturating_sub(buf_size as u64);
    }

    Ok(())
}
