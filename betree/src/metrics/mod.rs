//! A naive metrics system, logging newline-delimited JSON to a configurable file.

use crate::{
    data_management::{Dml, DmlWithHandler},
    database::{DatabaseBuilder, StorageInfo},
    storage_pool::{StoragePoolLayer, NUM_STORAGE_CLASSES},
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Configuration bundle of the [crate::metrics] module.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfiguration {
    /// Whether to dump metrics periodically
    pub enabled: bool,
    /// The interval in milliseconds to wait between reports
    pub interval_ms: u32,
    /// The file to write reports to
    pub output_path: PathBuf,
}

pub(crate) fn metrics_init<Config>(
    cfg: &MetricsConfiguration,
    dmu: Arc<Config::Dmu>,
) -> io::Result<thread::JoinHandle<()>>
where
    Config: DatabaseBuilder,
{
    let cfg = cfg.clone();

    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&cfg.output_path)?;

    thread::Builder::new()
        .name(String::from("metrics"))
        .spawn(move || metrics_loop::<Config>(cfg, file, dmu))
}

#[derive(Serialize)]
struct Metrics<Config: DatabaseBuilder> {
    epoch_ms: u128,
    cache: <Config::Dmu as Dml>::CacheStats,
    storage: <<Config::Dmu as Dml>::Spl as StoragePoolLayer>::Metrics,
    usage: Vec<StorageInfo>,
}

fn metrics_loop<Config>(cfg: MetricsConfiguration, output: fs::File, dmu: Arc<Config::Dmu>)
where
    Config: DatabaseBuilder,
{
    let mut output = io::BufWriter::new(output);
    let sleep_duration = Duration::from_millis(cfg.interval_ms as u64);
    loop {
        log::info!("gathering metrics");
        let now = Instant::now();

        let spu = dmu.spl();

        let metrics: Metrics<Config> = Metrics {
            epoch_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(u128::MAX),
            cache: dmu.cache_stats(),
            storage: spu.metrics(),
            // We can be sure that the following is always correct
            usage: (0..NUM_STORAGE_CLASSES as u8)
                .map(|tier| dmu.handler().get_free_space_tier(tier).unwrap())
                .collect(),
        };

        let mut res = || -> io::Result<()> {
            serde_json::to_writer(&mut output, &metrics)?;
            writeln!(&mut output)?;
            output.flush()?;
            Ok(())
        };

        if let Err(e) = res() {
            log::error!("metrics: {}", e);
        }

        thread::sleep(sleep_duration.saturating_sub(now.elapsed()));
    }
}
