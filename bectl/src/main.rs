use std::{
    fmt::{self, Display},
    io::{self, BufReader, BufWriter, Write},
    num,
    str::FromStr,
};

use betree_storage_stack::{
    cow_bytes::CowBytes,
    database::{Database, DatabaseConfiguration, Superblock},
    storage_pool::DiskOffset,
    tree::{DefaultMessageAction, TreeLayer},
    StoragePreference,
};
use chrono::{DateTime, Utc};
use figment::providers::Format;
use log::info;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt)]
struct Opt {
    /// Path to JSON configuration file of database.
    #[structopt(long, short, env = "BETREE_CONFIG")]
    database_config: String,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(StructOpt)]
enum Mode {
    /// Display the currently active database configuration
    Config {
        #[structopt(subcommand)]
        mode: ConfigMode,
    },

    /// Act on the configured database
    Db {
        #[structopt(subcommand)]
        mode: DbMode,
    },

    /// Key-value interface
    Kv {
        dataset: String,
        #[structopt(long, default_value = "")]
        storage_preference: OptStoragePreference,
        #[structopt(subcommand)]
        mode: KvMode,
    },

    /// Object interface
    Obj {
        namespace: String,
        #[structopt(long, default_value = "")]
        storage_preference: OptStoragePreference,
        #[structopt(subcommand)]
        mode: ObjMode,
    },
}

struct OptStoragePreference(StoragePreference);
impl FromStr for OptStoragePreference {
    type Err = num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(OptStoragePreference(StoragePreference::NONE))
        } else {
            Ok(OptStoragePreference(StoragePreference::new(
                s.parse::<u8>()?,
            )))
        }
    }
}

#[derive(StructOpt)]
enum ConfigMode {
    PrintActive,
    PrintDefault,
}

#[derive(StructOpt)]
enum DbMode {
    Init,
    ListDatasets,
    Space,
    DumpSuperblock,
    ListRoot,
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
        value: String,
    },
    TreeDump,
}

#[derive(StructOpt)]
enum ObjMode {
    List {
        #[structopt(short = "c", long)]
        with_custom: bool,
    },
    Get {
        name: String,
    },
    Put {
        name: String,
        #[structopt(long, default_value = "65536")]
        buf_size: u32,
    },
    Del {
        name: String,
    },
    Mv {
        name: String,
        new_name: String,
    },
    Meta {
        obj_name: String,
        #[structopt(subcommand)]
        mode: ObjMetaMode,
    },
}

#[derive(StructOpt)]
enum ObjMetaMode {
    Get { meta_name: String },
    Set { meta_name: String, value: String },
    Del { meta_name: String },
    List,
}

error_chain::error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }

    foreign_links {
        Figment(figment::error::Error);
        Io(std::io::Error);
        Betree(betree_storage_stack::database::Error);
    }
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

fn open_db(cfg: DatabaseConfiguration) -> Result<Database, Error> {
    Database::build(cfg).chain_err(|| "couldn't open database")
}

fn bectl_main() -> Result<(), Error> {
    betree_storage_stack::env_logger::init_env_logger();
    let opt = Opt::from_args();

    let cfg: DatabaseConfiguration = figment::Figment::new()
        .merge(DatabaseConfiguration::figment_default())
        .merge(figment::providers::Json::file(opt.database_config))
        .merge(DatabaseConfiguration::figment_env())
        .extract()?;

    info!("{:#?}", cfg);

    match opt.mode {
        Mode::Config { mode } => match mode {
            ConfigMode::PrintActive => println!("{:#?}", cfg),
            ConfigMode::PrintDefault => println!("{:#?}", DatabaseConfiguration::default()),
        },

        Mode::Db { mode } => match mode {
            DbMode::Init => Database::create(cfg.storage)?.sync()?,

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

                let space = handler.free_space_disk(DiskOffset::construct_disk_id(0, 0));
                println!("{:?}", space);
            }

            DbMode::DumpSuperblock => {
                let spu = cfg.new_spu()?;
                let superblock = Superblock::fetch_superblocks(&spu);
                println!("{:#?}", superblock);
            }

            DbMode::ListRoot => {
                let db = open_db(cfg)?;
                let root = db.root_tree();

                let range = root.range::<CowBytes, _>(..).unwrap();
                for (k, v) in range.flatten() {
                    println!("{:?} -> {:?}", &*k, &*v);
                }
            }
        },

        Mode::Kv {
            dataset,
            mode,
            storage_preference,
            ..
        } => match mode {
            KvMode::List { with_value } => {
                let mut db = open_db(cfg)?;
                let ds = db.open_custom_dataset::<DefaultMessageAction>(
                    dataset.as_bytes(),
                    storage_preference.0,
                )?;
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
                let ds = db.open_or_create_custom_dataset::<DefaultMessageAction>(
                    dataset.as_bytes(),
                    storage_preference.0,
                )?;
                let value = ds.get(name.as_bytes()).unwrap().unwrap();
                println!("{}", PseudoAscii(&value));
            }

            KvMode::Put { name, value } => {
                let mut db = open_db(cfg)?;
                let ds =
                    db.open_or_create_custom_dataset(dataset.as_bytes(), storage_preference.0)?;
                ds.insert(name.as_bytes(), value.as_bytes())?;
                db.sync()?;
            }

            KvMode::TreeDump => {
                let mut db = open_db(cfg)?;
                let ds = db.open_or_create_custom_dataset::<DefaultMessageAction>(
                    dataset.as_bytes(),
                    storage_preference.0,
                )?;

                let stdout = io::stdout();
                let mut stdout_lock = stdout.lock();

                let _ = serde_json::to_writer_pretty(&mut stdout_lock, &ds.tree_dump()?);
            }
        },

        Mode::Obj {
            mode,
            namespace,
            storage_preference,
            ..
        } => match mode {
            ObjMode::List { with_custom } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;

                for (obj, info) in os.list_objects::<_, &[u8]>(..)? {
                    let mtime = DateTime::<Utc>::from(info.mtime);
                    println!(
                        "{} ({} bytes, modified {})",
                        PseudoAscii(obj.object.key()),
                        info.size,
                        mtime.to_rfc3339()
                    );

                    if with_custom {
                        for (k, v) in obj.iter_metadata()?.flatten() {
                            println!("  {} -> {}", PseudoAscii(&k[..]), PseudoAscii(&v[..]))
                        }
                    }
                }
            }
            ObjMode::Get { name } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;
                let stdout = io::stdout();
                let mut stdout_lock = stdout.lock();

                let (obj, _info) = os
                    .open_object_with_pref(name.as_bytes(), storage_preference.0)?
                    .unwrap();

                let mut cursor = BufReader::new(obj.cursor());
                io::copy(&mut cursor, &mut stdout_lock)?;
                stdout_lock.flush()?;
            }

            ObjMode::Put { name, buf_size } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;
                let (obj, _info) =
                    os.open_or_create_object_with_pref(name.as_bytes(), storage_preference.0)?;

                let stdin = io::stdin();
                let mut stdin_lock = stdin.lock();

                let mut cursor = BufWriter::with_capacity(buf_size as usize, obj.cursor());
                io::copy(&mut stdin_lock, &mut cursor)?;
                cursor.flush()?;

                db.sync()?;
            }

            ObjMode::Del { name } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;

                if let Some((obj, _info)) =
                    os.open_object_with_pref(name.as_bytes(), storage_preference.0)?
                {
                    obj.delete()?;
                }

                db.sync()?;
            }

            ObjMode::Mv { name, new_name } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;

                if let Some((mut obj, _info)) =
                    os.open_object_with_pref(name.as_bytes(), storage_preference.0)?
                {
                    obj.rename(new_name.as_bytes())?;
                }

                db.sync()?;
            }

            ObjMode::Meta { obj_name, mode } => {
                let mut db = open_db(cfg)?;
                let os = db.open_named_object_store(namespace.as_bytes(), storage_preference.0)?;
                if let Some((obj, _info)) =
                    os.open_object_with_pref(obj_name.as_bytes(), storage_preference.0)?
                {
                    match mode {
                        ObjMetaMode::Get { meta_name } => {
                            if let Some(value) = obj.get_metadata(meta_name.as_bytes())? {
                                println!("{}", PseudoAscii(&value[..]));
                            }
                        }
                        ObjMetaMode::Set { meta_name, value } => {
                            obj.set_metadata(meta_name.as_bytes(), value.as_bytes())?;
                        }
                        ObjMetaMode::Del { meta_name } => {
                            obj.delete_metadata(meta_name.as_bytes())?;
                        }
                        ObjMetaMode::List => {
                            for (k, v) in obj.iter_metadata()?.flatten() {
                                println!("{} -> {}", PseudoAscii(&k[..]), PseudoAscii(&v[..]))
                            }
                        }
                    }
                }

                db.sync()?;
            }
        },
    }

    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    use std::{
        error::Error,
        fmt::Debug,
        sync::{Arc, Mutex},
    };

    struct ArcError<E>(Arc<Mutex<E>>);
    impl<E: Debug> Debug for ArcError<E> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.lock().unwrap().fmt(f)
        }
    }
    impl<E: Display> Display for ArcError<E> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.lock().unwrap().fmt(f)
        }
    }
    impl<E: Error> Error for ArcError<E> {}
    Ok(bectl_main().map_err(|err| ArcError(Arc::new(Mutex::new(err))))?)
}
