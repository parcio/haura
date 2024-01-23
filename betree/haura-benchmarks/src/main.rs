use std::{error::Error, path::PathBuf, process, thread, time::Duration};

use betree_perf::Control;
use structopt::StructOpt;

mod checkpoints;
mod filesystem;
mod filesystem_zip;
mod ingest;
mod rewrite;
mod scientific_evaluation;
mod switchover;
mod tiered1;
mod zip;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt)]
enum Mode {
    Tiered1,
    Checkpoints,
    Filesystem,
    FilesystemZip {
        path: PathBuf
    },
    EvaluationRead {
        #[structopt(default_value = "120")]
        runtime: u64,
    },
    EvaluationRW {
        #[structopt(default_value = "120")]
        runtime: u64,
    },
    Zip {
        n_clients: u32,
        runs_per_client: u32,
        files_per_run: u32,
        path: PathBuf,
        start_of_eocr: u64,
    },
    Ingest {
        path: PathBuf,
    },
    Switchover {
        part_count: u64,
        part_size: u64,
    },
    Rewrite {
        object_size: u64,
        rewrite_count: u64,
    },
}

fn run_all(mode: Mode) -> Result<(), Box<dyn Error>> {
    thread::spawn(|| betree_perf::log_process_info("proc.jsonl", 250));

    let root = std::env::var("ROOT").expect("Didn't provide a repository ROOT");
    let mut sysinfo = process::Command::new(format!("{root}/target/release/sysinfo-log"))
        .args(&["--output", "sysinfo.jsonl", "--interval-ms", "250"])
        .spawn()?;

    let mut control = Control::new();

    match mode {
        Mode::Tiered1 => {
            let client = control.client(0, b"tiered1");
            tiered1::run(client)?;
            control.database.write().sync()?;
        }
        Mode::Checkpoints => {
            let client = control.client(0, b"checkpoints");
            checkpoints::run(client)?;
            control.database.write().sync()?;
        }
        Mode::Filesystem => {
            let client = control.client(0, b"filesystem");
            filesystem::run(client)?;
            control.database.write().sync()?;
        }
        Mode::FilesystemZip { path } => {
            let client = control.client(0, b"filesystem_zip");
            filesystem_zip::run(client, path)?;
            control.database.write().sync()?;
        }
        Mode::EvaluationRead { runtime } => {
            let client = control.client(0, b"scientific_evaluation");
            scientific_evaluation::run_read(client, runtime)?;
            control.database.write().sync()?;
        }
        Mode::EvaluationRW { runtime } => {
            let client = control.client(0, b"scientific_evaluation");
            scientific_evaluation::run_read_write(client, runtime)?;
            control.database.write().sync()?;
        }
        Mode::Zip {
            path,
            start_of_eocr,
            n_clients,
            runs_per_client,
            files_per_run,
        } => {
            let mut client = control.client(0, b"zip");

            zip::prepare(&mut client, path, start_of_eocr)?;
            control.database.write().sync()?;

            zip::read(&mut client, n_clients, runs_per_client, files_per_run)?;
        }
        Mode::Ingest { path } => {
            let mut client = control.client(0, b"ingest");
            ingest::run(&mut client, path)?;
        }
        Mode::Switchover {
            part_count,
            part_size,
        } => {
            let mut client = control.client(0, b"switchover");
            switchover::run(&mut client, part_count, part_size)?;
        }
        Mode::Rewrite {
            object_size,
            rewrite_count,
        } => {
            let mut client = control.client(0, b"rewrite");
            rewrite::run(&mut client, object_size, rewrite_count)?;
        }
    }

    thread::sleep(Duration::from_millis(2000));

    sysinfo.kill()?;
    sysinfo.wait()?;

    Ok(())
}

fn main() {
    let mode = Mode::from_args();
    if let Err(e) = run_all(mode) {
        eprintln!("error: {}", e);
        process::exit(1);
    }
}
