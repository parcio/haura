use std::{
    fmt::{self, Display},
    io::{self, Read, Write},
};

use betree_storage_stack::{
    cow_bytes::CowBytes,
    data_management::Handler,
    database::*,
    tree::{DefaultMessageAction, TreeLayer},
    StorageConfiguration,
};
use chrono::{DateTime, Utc};
use error_chain::ChainedError;
use log::info;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt)]
struct Opt {
    /// ZFS-like description of the pool layout. E.g. "mirror /dev/sde3 /dev/sdf3"
    #[structopt(long, short, env = "BETREE_POOL_CONFIG")]
    pool_config: String,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(StructOpt)]
enum Mode {
    DumpSuperblock,
    ListRoot,

    Db {
        #[structopt(subcommand)]
        mode: DbMode,
    },

    Kv {
        dataset: String,
        #[structopt(subcommand)]
        mode: KvMode,
    },

    Obj {
        dataset: String,
        #[structopt(subcommand)]
        mode: ObjMode,
    },
}

#[derive(StructOpt)]
enum DbMode {
    Init,
    ListDatasets,
    Space,
}

#[derive(StructOpt)]
enum KvMode {
    List {
        #[structopt(short = "v", long)]
        with_value: bool,
    },
    Get {
        name: String,
    },
    Put {
        name: String,
        value: Option<String>,
    },
}

#[derive(StructOpt)]
enum ObjMode {
    List,
    Get {
        name: String,
    },
    Put {
        name: String,
        #[structopt(long, default_value = "8192")]
        buf_size: u32,
    },
    Del {
        name: String,
    },
}

struct PseudoAscii<'a>(&'a [u8]);
impl<'a> Display for PseudoAscii<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            let c = *b as char;
            for encoded_char in c.escape_default() {
                write!(f, "{}", encoded_char)?;
            }
        }

        Ok(())
    }
}

fn open_db(cfg: StorageConfiguration) -> Result<Database<DatabaseConfiguration>> {
    Database::open(cfg).chain_err(|| "couldn't open database")
}

fn bectl_main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();

    let toplevel_devices: Vec<&str> = opt.pool_config.split(';').collect();
    let cfg = StorageConfiguration::parse_zfs_like(&toplevel_devices)
        .chain_err(|| "couldn't parse pool configuration")?;
    info!("Pool configuration: {:?}", cfg);

    match opt.mode {
        Mode::DumpSuperblock => unimplemented!(),
        Mode::ListRoot => {
            let db = open_db(cfg)?;
            let root = db.root_tree();

            let range = root.range::<CowBytes, _>(..).unwrap();
            for e in range {
                if let Ok((k, v)) = e {
                    println!("{:?} -> {:?}", &*k, &*v);
                }
                // println!("{} -> {}", PseudoAscii(&k), PseudoAscii(&v));
            }
        }

        Mode::Db { mode } => match mode {
            DbMode::Init => Database::create(cfg)?.sync()?,

            DbMode::ListDatasets => {
                let db = open_db(cfg)?;
                for ds in db.iter_datasets().unwrap() {
                    println!("name: {:?}", ds);
                }
            }

            DbMode::Space => {
                let db = open_db(cfg)?;
                let root = db.root_tree();
                let dmu = root.dmu();
                let handler = dmu.handler();

                let space = handler.get_free_space(0);
                println!("{:?}", space);
            }
        },

        Mode::Kv { dataset, mode } => match mode {
            KvMode::List { with_value } => {
                let mut db = open_db(cfg)?;
                let ds = db.open_dataset(dataset.as_bytes())?;
                let range = ds.range::<_, CowBytes>(..).unwrap();
                for (k, v) in range.filter_map(Result::ok) {
                    if with_value {
                        println!("{} -> {}", PseudoAscii(&k), PseudoAscii(&v));
                    } else {
                        println!("{}", PseudoAscii(&k));
                    }
                }
            }

            KvMode::Get { name } => {
                let mut db = open_db(cfg)?;
                let ds = db.open_or_create_dataset::<DefaultMessageAction>(dataset.as_bytes())?;
                let value = ds.get(name.as_bytes()).unwrap().unwrap();
                println!("{}", PseudoAscii(&value));
            }

            KvMode::Put { name, value } => {
                let mut db = open_db(cfg)?;
                let ds = db.open_or_create_dataset(dataset.as_bytes())?;
                let value = value.expect("No value given");
                ds.insert(name.as_bytes(), value.as_bytes())?;
                db.sync()?;
            }
        },

        Mode::Obj { mode, .. } => match mode {
            ObjMode::List => {
                let mut db = open_db(cfg)?;
                let os = db.open_object_store()?;

                for obj in os.list_objects::<_, &[u8]>(..)? {
                    let mtime = DateTime::<Utc>::from(obj.modification_time());
                    println!(
                        "{} ({} bytes, modified {})",
                        PseudoAscii(&obj.key[..]),
                        obj.size(),
                        mtime.to_rfc3339()
                    );
                }
            }
            ObjMode::Get { name } => {
                let mut db = open_db(cfg)?;
                let os = db.open_object_store()?;
                let stdout = io::stdout();
                let mut stdout_lock = stdout.lock();

                let obj = os.open_object(name.as_bytes())?.unwrap();
                for (_key, value) in obj.read_all_chunks().unwrap().filter_map(Result::ok) {
                    stdout_lock.write_all(&value);
                }

                stdout_lock.flush();
            }

            ObjMode::Put { name, buf_size } => {
                let mut db = open_db(cfg)?;
                let os = db.open_object_store()?;
                let mut obj = os.open_or_create_object(name.as_bytes())?;

                let stdin = io::stdin();
                let mut stdin_lock = stdin.lock();

                let mut buf = vec![0; buf_size as usize];
                let mut curr_pos = 0;

                loop {
                    match stdin_lock.read(&mut buf) {
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => panic!(e), // FIXME: pass outside
                        Ok(0) => break,
                        Ok(n_read) => {
                            obj.write_at(&buf[..n_read], curr_pos)
                                .map_err(|(_, err)| err)?;
                            curr_pos += n_read as u64;
                        }
                    }
                }

                db.sync()?;
            }

            ObjMode::Del { name } => {
                let mut db = open_db(cfg)?;
                let os = db.open_object_store()?;

                if let Some(obj) = os.open_object(name.as_bytes())? {
                    obj.delete()?;
                }

                db.sync()?;
            }
        },
    }

    Ok(())
}

fn main() {
    if let Err(err) = bectl_main() {
        eprintln!("{}", err.display_chain());
    }
}
