//! A naive metrics system, logging newline-delimited JSON to a configurable file.

use crate::{
    data_management::{DmlWithCache, DmlWithSpl},
    database::DatabaseBuilder,
    storage_pool::StoragePoolLayer,
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
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
        .append(true)
        .open(&cfg.output_path)?;

    thread::Builder::new()
        .name(String::from("metrics"))
        .spawn(move || metrics_loop::<Config>(cfg, file, dmu))
}

#[derive(Serialize)]
struct Metrics<Config: DatabaseBuilder> {
    epoch_ms: u128,
    cache: <Config::Dmu as DmlWithCache>::CacheStats,
    storage: <Config::Spu as StoragePoolLayer>::Metrics,
}

fn metrics_loop<Config>(cfg: MetricsConfiguration, output: fs::File, dmu: Arc<Config::Dmu>)
where
    Config: DatabaseBuilder,
{
    let mut output = io::BufWriter::new(output);
    let sleep_duration = Duration::from_millis(cfg.interval_ms as u64);
    loop {
        thread::sleep(sleep_duration);
        log::info!("gathering metrics");

        let spu = dmu.spl();

        let metrics: Metrics<Config> = Metrics {
            epoch_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(u128::MAX),
            cache: dmu.cache_stats(),
            storage: spu.metrics(),
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
    }
}
