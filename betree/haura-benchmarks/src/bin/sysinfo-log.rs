use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use libmedium::{
    hwmon::Hwmons,
    sensors::{Input, Sensor},
};
use serde_json::{Map, Number, Value};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opts {
    #[structopt(long)]
    output: PathBuf,
    #[structopt(long)]
    interval_ms: u64,
}

fn gather() -> Map<String, Value> {
    let mut map = Map::new();

    if let Ok(hwmons) = Hwmons::parse() {
        let mut hwmons_map = Map::new();

        for (idx, name, hwmon) in hwmons.into_iter() {
            let mut hwmon_map = Map::new();
            for (_name, tempsensor) in hwmon.temps() {
                if let Ok(temp) = tempsensor.read_input() {
                    let celsius = Number::from_f64(temp.as_degrees_celsius())
                        .expect("Invalid temperature (NaN/infinity)");
                    hwmon_map.insert(tempsensor.name(), Value::Number(celsius));
                }
            }

            hwmons_map.insert(format!("{}:{}", idx, name), Value::Object(hwmon_map));
        }

        map.insert(String::from("hwmon"), Value::Object(hwmons_map));
    }

    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(u128::MAX);
    map.insert(String::from("epoch_ms"), Value::from(epoch_ms as u64));

    map
}

fn main() -> io::Result<()> {
    let cfg = Opts::from_args();
    let file = File::create(cfg.output)?;
    let mut output = BufWriter::new(file);
    let interval = Duration::from_millis(cfg.interval_ms);

    loop {
        let now = Instant::now();

        serde_json::to_writer(&mut output, &gather())?;
        writeln!(&mut output)?;
        output.flush()?;

        thread::sleep(interval.saturating_sub(now.elapsed()));
    }
}
