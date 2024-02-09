//! json-flatten collapses each newline-delimited JSON document from stdin according to the following rules:
//!
//! - { "foo": { "bar": 42 } } => { "foo_bar": 42 }
//! - { "foo": [ 1, 2, 3 ] } => { "foo_0": 1, "foo_1": 2, "foo_2": 3 }
//! - { "foo": [ { "bar": 1, "quux": 2 }, { "bar": 3 } ] }
//!   => { "foo_0_bar": 1, "foo_0_quux": 2, "foo_1_bar": 3 }

use std::io::{self, Write};

use serde_json::{Deserializer, Map, Value};

fn flatten_into(out: &mut Map<String, Value>, prefix: Option<&str>, value: &Value) {
    match value {
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                let prefix = prefix
                    .map(|p| format!("{}_{}", p, idx))
                    .unwrap_or_else(|| idx.to_string());

                flatten_into(out, Some(&prefix), item);
            }
        }
        Value::Object(map) => {
            for (key, value) in map.iter() {
                let prefix = prefix
                    .map(|p| format!("{}_{}", p, key))
                    .unwrap_or_else(|| key.clone());

                flatten_into(out, Some(&prefix), value);
            }
        }
        val => {
            out.insert(String::from(prefix.unwrap()), val.clone());
        }
    }
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let stdin = stdin.lock();

    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let mut items = Deserializer::from_reader(stdin).into_iter();

    while let Some(Ok(Value::Object(map))) = items.next() {
        let mut flat = Map::new();
        flatten_into(&mut flat, None, &Value::Object(map));

        serde_json::to_writer(&mut stdout, &Value::Object(flat))?;
        writeln!(&mut stdout)?;
    }

    Ok(())
}
