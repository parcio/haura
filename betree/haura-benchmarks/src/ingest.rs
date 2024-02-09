use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{
    error::Error,
    fs::File,
    io::{self, Seek, SeekFrom},
    path::Path,
};

pub fn run(client: &mut Client, path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
    println!("running ingest::run");

    let os = &client.object_store;

    let mut input = File::open(path.as_ref())?;

    let (obj, _info) = os.open_or_create_object_with_pref(b"obj", StoragePreference::FASTEST)?;
    let mut cursor = obj.cursor();

    io::copy(&mut input, &mut cursor)?;

    client.sync().expect("Failed to sync database");

    cursor.seek(SeekFrom::Start(0))?;
    io::copy(&mut cursor, &mut io::sink())?;

    Ok(())
}
