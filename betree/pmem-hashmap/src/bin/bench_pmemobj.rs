use std::{mem::MaybeUninit, process::Command};

use pmem_hashmap::{
    haura_alloc, haura_direct, pmemobj_create, pmemobj_memcpy, pmemobj_memcpy_persist, PMEMoid,
};

const BS: usize = 4 * 1024 * 1024;
const SIZE: usize = 64 * 1024 * 1024 * 1024;
const EFFECTIVE_SIZE: usize = 32 * 1024 * 1024 * 1024;
const ITER: usize = EFFECTIVE_SIZE / BS;
const WORKERS: usize = 2;

struct Taco<T>(T);

unsafe impl<T> Send for Taco<T> {}
unsafe impl<T> Sync for Taco<T> {}

fn main() {
    Command::new("rm")
        .arg("/home/wuensche/pmem/baz")
        .status()
        .unwrap();
    let pobjpool = {
        let path = std::ffi::CString::new("/home/wuensche/pmem/baz")
            .unwrap()
            .into_raw();
        unsafe { pmemobj_create(path, std::ptr::null(), SIZE, 0o666) }
    };

    enum CMD {
        Read(MaybeUninit<PMEMoid>),
        Write(MaybeUninit<PMEMoid>),
        Wait,
    }

    // Initiate workers
    let channels = (0..WORKERS).map(|_| std::sync::mpsc::sync_channel::<CMD>(0));

    let threads: Vec<_> = channels
        .enumerate()
        .map(|(_id, (tx, rx))| {
            let hdl = Taco(pobjpool.clone());
            (
                tx,
                std::thread::spawn(move || {
                    let bar = hdl;
                    let mut buf = vec![42u8; BS];
                    while let Ok(cmd) = rx.recv() {
                        match cmd {
                            CMD::Read(mut oid) => {
                                let foo = unsafe { haura_direct(*oid.as_mut_ptr()) };
                                unsafe {
                                    pmemobj_memcpy(
                                        bar.0,
                                        buf.as_mut_ptr() as *mut std::ffi::c_void,
                                        foo,
                                        BS,
                                        0,
                                    )
                                };
                            }
                            CMD::Write(mut oid) => {
                                let foo = unsafe { haura_direct(*oid.as_mut_ptr()) };
                                unsafe {
                                    pmemobj_memcpy_persist(
                                        bar.0,
                                        foo,
                                        buf.as_ptr() as *const std::ffi::c_void,
                                        BS,
                                    )
                                };
                            }
                            CMD::Wait => {
                                // NO-OP
                            }
                        }
                    }
                }),
            )
        })
        .collect();

    let mut oids = Vec::with_capacity(ITER);
    let start = std::time::Instant::now();
    for id in 0..ITER {
        let mut oid = std::mem::MaybeUninit::<PMEMoid>::uninit();
        if unsafe {
            haura_alloc(
                pobjpool,
                oid.as_mut_ptr(),
                BS,
                0, // BOGUS
                std::ptr::null_mut(),
            ) != 0
        } {
            panic!("Oh no, something went wrong..");
        }
        assert!(unsafe { oid.assume_init_read().off != 0 });
        oids.push(oid.clone());
        threads[id % WORKERS].0.send(CMD::Write(oid)).unwrap();
    }

    for (tx, _) in threads.iter() {
        tx.send(CMD::Wait).unwrap();
    }
    println!(
        "Write: Achieved {} GiB/s",
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32()
    );

    let start = std::time::Instant::now();
    for id in 0..ITER {
        threads[id % WORKERS].0.send(CMD::Read(oids[id])).unwrap();
    }
    for (tx, _) in threads.iter() {
        tx.send(CMD::Wait).unwrap();
    }
    println!(
        "Read: Achieved {} GiB/s",
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32()
    );
    for (s, thread) in threads.into_iter() {
        drop(s);
        thread.join().unwrap();
    }
}
