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

use std::fs;
use std::io::{BufReader, Read};
//use std::path::Path;

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

    pub fn fill_entries_with_data_type(&mut self, entries: u64, entry_size: u32, data_type: &str) -> Vec<[u8; 8]> {
        let mut keys = vec![];
        
        for idx in 0..entries {
            let value = match data_type {
                "int" => {
                    // Fill with random integers
                    let mut value = vec![0u8; entry_size as usize];
                    let num_ints = entry_size as usize / 4; // 4 bytes per i32
                    let remaining_bytes = entry_size as usize % 4;
                    
                    for i in 0..num_ints {
                        let random_int: i32 = self.rng.gen();
                        let bytes = random_int.to_le_bytes();
                        value[i * 4..(i + 1) * 4].copy_from_slice(&bytes);
                    }
                    
                    // Fill remaining bytes with random data
                    if remaining_bytes > 0 {
                        let start_idx = num_ints * 4;
                        self.rng.fill(&mut value[start_idx..]);
                    }
                    
                    // Sort the value vector
                    value.sort();
                    
                    value
                }
                "float" => {
                    // Fill with random floats
                    let mut value = vec![0u8; entry_size as usize];
                    let num_floats = entry_size as usize / 4; // 4 bytes per f32
                    let remaining_bytes = entry_size as usize % 4;
                    
                    for i in 0..num_floats {
                        let random_float: f32 = self.rng.gen();
                        let bytes = random_float.to_le_bytes();
                        value[i * 4..(i + 1) * 4].copy_from_slice(&bytes);
                    }
                    
                    // Fill remaining bytes with random data
                    if remaining_bytes > 0 {
                        let start_idx = num_floats * 4;
                        self.rng.fill(&mut value[start_idx..]);
                    }
                    
                    // Sort the value vector
                    value.sort();
                    
                    value
                }
                _ => {
                    // Default: fill with random bytes (same as original)
                    let mut value = vec![0u8; entry_size as usize];
                    self.rng.fill(&mut value[..]);
                    
                    // Sort the value vector
                    value.sort();
                    
                    value
                }
            };
            
            let k = (idx as u64).to_be_bytes();
            self.ds.insert(&k[..], &value).unwrap();
            keys.push(k);
        }
        
        self.db.write().sync().unwrap();
        keys
    }

    pub fn fill_entries_from_path<P: AsRef<Path>>(
        &mut self,
        path: P,
        chunk_size: u32, // now explicitly used as variable chunk size
    ) -> Vec<[u8; 8]> {
        let mut keys = Vec::new();
        let mut idx = 0u64;
        println!("fill_entries_from_path");
        for entry in fs::read_dir(path).expect("Failed to read directory") {
            let entry = entry.expect("Invalid directory entry");
            let file_path = entry.path();

            if file_path.is_file() {
                let file = File::open(&file_path).expect("Failed to open file");
                let mut reader = BufReader::new(file);

                loop {
                    let mut buffer = vec![0u8; chunk_size as usize];
                    let bytes_read = reader.read(&mut buffer).expect("Read error");

                    if bytes_read == 0 {
                        break; // end of file
                    }

                    buffer.truncate(bytes_read); // ensure last chunk has correct size
                    let k = idx.to_be_bytes();
                    self.ds.insert(&k[..], &buffer).unwrap();
                    keys.push(k);
                    idx += 1;
                }
            }
        }

        self.db.write().sync().unwrap();
        //self.db.write().flush_().unwrap();
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