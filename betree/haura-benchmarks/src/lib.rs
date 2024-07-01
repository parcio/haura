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

        let conf_path =
            PathBuf::from(env::var("BETREE_CONFIG").expect("Didn't provide a BETREE_CONFIG"));

        let mut cfg = figment::Figment::new().merge(DatabaseConfiguration::figment_default());

        match conf_path.extension() {
            Some(ext) if ext == "yml" || ext == "yaml" => {
                cfg = cfg.merge(figment::providers::Yaml::file(conf_path.clone()))
            }
            Some(ext) if ext == "json" => {
                cfg = cfg.merge(figment::providers::Json::file(conf_path.clone()))
            }
            _ => todo!(),
        }

        let mut cfg: DatabaseConfiguration = cfg
            .merge(DatabaseConfiguration::figment_env())
            .extract()
            .expect("Failed to extract DatabaseConfiguration");

        cfg.access_mode = AccessMode::AlwaysCreateNew;

        cfg.sync_interval_ms = None;

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

    pub fn kv_client(&mut self, id: u32) -> KvClient {
        KvClient::new(
            self.database.clone(),
            Xoshiro256Plus::seed_from_u64(id as u64),
        )
    }
}

pub struct KvClient {
    pub db: Arc<RwLock<Database>>,
    pub rng: Xoshiro256Plus,
    pub ds: Dataset,
}

impl KvClient {
    pub fn new(db: Arc<RwLock<Database>>, rng: Xoshiro256Plus) -> Self {
        let ds = db.write().open_or_create_dataset(b"FOOBAR").unwrap();
        Self { db, ds, rng }
    }

    pub fn fill_entries(&mut self, entries: u64, entry_size: u32) -> Vec<[u8; 8]> {
        let mut keys = vec![];
        let mut value = vec![0u8; entry_size as usize];
        for idx in 0..entries {
            self.rng.fill(&mut value[..]);
            let k = (idx as u64).to_be_bytes();
            self.ds.insert(&k[..], &value).unwrap();
            keys.push(k);
        }
        self.db.write().sync().unwrap();
        keys
    }

    pub fn rng(&mut self) -> &mut Xoshiro256Plus {
        &mut self.rng
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

    let ticks = procfs::ticks_per_second() as f64;
    let page_size = procfs::page_size();

    loop {
        let now = Instant::now();

        if let Ok(proc) = Process::myself() {
            let stats = proc.stat().map_err(|e| io::Error::other(e))?;
            let info = serde_json::json!({
                "vsize": stats.vsize,
                "rss": stats.rss * page_size,
                "utime": stats.utime as f64 / ticks,
                "stime": stats.stime as f64 / ticks,
                "cutime": stats.cutime as f64 / ticks,
                "cstime": stats.cstime as f64 / ticks,
                "minflt": stats.minflt,
                "cminflt": stats.cminflt,
                "majflt": stats.majflt,
                "cmajflt": stats.cmajflt
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
