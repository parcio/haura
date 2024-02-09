use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{
    error::Error,
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use rand::{seq::SliceRandom, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;
use zip::*;

pub fn prepare(
    client: &mut Client,
    path: impl AsRef<Path>,
    start_of_eocr: u64,
) -> Result<(), Box<dyn Error>> {
    println!("running zip::prepare");

    let os = &client.object_store;

    let mut zip_archive = File::open(path.as_ref())?;
    let zip_archive_len = zip_archive.metadata()?.len();
    assert!(start_of_eocr < zip_archive_len);

    let (zip, _info) =
        os.open_or_create_object_with_pref(b"archive.zip", StoragePreference::FAST)?;
    let mut cursor = zip.cursor();

    let mut buf = vec![0; 128 * 1024];
    let mut idx = 0;
    while idx < zip_archive_len {
        let max_bytes = buf.len().min(zip_archive_len as usize - idx as usize);
        let n_read = zip_archive.read(&mut buf[..max_bytes])?;

        // let is_metadata = idx < 1024 * 1024 || idx + n_read as u64 + 1024 * 1024 >= start_of_eocr;
        let is_metadata = idx + n_read as u64 >= start_of_eocr;
        cursor.set_storage_preference(if is_metadata {
            StoragePreference::FASTEST
        } else {
            StoragePreference::FAST
        });

        cursor.write_all(&buf[..n_read])?;
        idx += n_read as u64;
    }

    client.sync().expect("Failed to sync database");

    Ok(())
}

pub fn read(
    client: &mut Client,
    n_clients: u32,
    runs_per_client: u32,
    files_per_run: u32,
) -> Result<(), Box<dyn Error>> {
    println!("running zip::read");

    let zip = client
        .object_store
        .open_object(b"archive.zip")?
        .expect("archive.zip doesn't exist");

    let mut file_names = ZipArchive::new(bufreader::BufReaderSeek::new(zip.cursor()))?
        .file_names()
        .map(String::from)
        .collect::<Vec<_>>();

    // file_names gives a non-deterministic order, sort for reproducible file selection
    file_names.sort();

    crossbeam::scope(|s| {
        for client_id in 0..n_clients {
            let mut rng = Xoshiro256Plus::seed_from_u64(client_id as u64);
            let obj = zip.clone();
            let file_names = &file_names[..];

            s.spawn(move |_| {
                for _ in 0..runs_per_client {
                    let cursor = obj.cursor();
                    let input = bufreader::BufReaderSeek::new(cursor);
                    let mut archive = ZipArchive::new(input).expect("Couldn't open zip archive");

                    for _ in 0..files_per_run {
                        let file_name = &file_names.choose(&mut rng).expect("Empty file name list");

                        let mut file = archive
                            .by_name(file_name)
                            .expect("Couldn't access file by name");

                        io::copy(&mut file, &mut io::sink()).expect("Couldn't read file");
                    }
                }
            });
        }
    })
    .expect("Child thread has panicked");

    Ok(())
}
