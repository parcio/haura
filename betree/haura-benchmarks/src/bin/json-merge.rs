use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde_json::{Deserializer, Map, Value};
use structopt::StructOpt;

/// json-merge merges newline-delimited JSON documents from multiple sources:
///
/// - read one document from primary stream, containing timestamp t
/// - read second document from primary stream, containing timestamp t'
/// - read all documents from secondary streams while their timestamps are between t (inclusive) and t' (exclusive)
/// - shallowly merge all accumulated documents into one
/// - output resulting document to stdout with timestamp t
/// - set t = t'
/// - repeat process from second step
#[derive(StructOpt)]
struct Opts {
    primary: PathBuf,
    secondary: Vec<PathBuf>,
    #[structopt(long)]
    timestamp_key: String,
}

fn open_stream(p: impl AsRef<Path>) -> io::Result<impl Iterator<Item = serde_json::Result<Value>>> {
    let f = File::open(p.as_ref())?;
    let iter = Deserializer::from_reader(f).into_iter::<Value>();
    Ok(iter)
}

fn extract_timestamp(key: &str, v: &Map<String, Value>) -> Option<u64> {
    v.get(key).and_then(|v| v.as_u64())
}

fn merge_into(mut dst: Map<String, Value>, src: Map<String, Value>) -> Map<String, Value> {
    for (k, v) in src {
        dst.insert(k.clone(), v.clone());
    }

    dst
}

fn main() -> io::Result<()> {
    let cfg = Opts::from_args();

    let mut primary = open_stream(&cfg.primary)?;
    let mut secondaries = cfg
        .secondary
        .iter()
        .map(|path| open_stream(path).map(Iterator::peekable))
        .collect::<io::Result<Vec<_>>>()?;

    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let mut lower = match primary.next() {
        Some(Ok(Value::Object(lower))) => lower,
        Some(Ok(_not_object)) => panic!("Value is not an object!"),
        Some(Err(e)) => panic!("Couldn't read first object, {}", e),
        None => return Ok(()), // primary is empty
    };
    let mut acc = Vec::new();

    while let Some(Ok(Value::Object(upper))) = primary.next() {
        let t = extract_timestamp(&cfg.timestamp_key, &lower);
        let t2 = extract_timestamp(&cfg.timestamp_key, &upper);

        acc.clear();

        for secondary in &mut secondaries {
            'process_sec: while let Some(Ok(Value::Object(map))) = secondary.peek() {
                let ts = extract_timestamp(&cfg.timestamp_key, map);
                if ts < t {
                    // secondary is too early, skip this
                    let _ = secondary.next();
                    continue 'process_sec;
                } else if ts < t2 {
                    // between t and t2, accumulate
                    if let Some(Ok(Value::Object(map))) = secondary.next() {
                        acc.push(map);
                    } else {
                        unreachable!()
                    }
                } else {
                    // too far ahead, go to next stream
                    break 'process_sec;
                }
            }
        }

        let merged = acc.drain(..).fold(lower, |acc, mut x| {
            x.remove(&cfg.timestamp_key);
            merge_into(acc, x)
        });

        serde_json::to_writer(&mut stdout, &Value::Object(merged))?;
        writeln!(&mut stdout)?;

        lower = upper;
    }

    Ok(())
}
