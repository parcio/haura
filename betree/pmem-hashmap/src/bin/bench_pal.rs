use std::{process::Command, sync::Arc};

use pmem_hashmap::allocator::{Pal, PalPtr};

fn bench_pal_sub(rank: usize, barrier: Arc<std::sync::Barrier>, tx: std::sync::mpsc::Sender<f32>) {
    Command::new("rm")
        .arg(format!("/home/wuensche/pmem/foobar{rank}"))
        .status()
        .unwrap();

    let pal =
        Arc::new(Pal::create(format!("/home/wuensche/pmem/foobar{rank}"), SIZE, 0o666).unwrap());

    enum CMD {
        Read(Vec<PalPtr<u8>>),
        Write(Vec<PalPtr<u8>>),
        Wait,
    }

    // Initiate workers
    let channels = (0..WORKERS).map(|_| std::sync::mpsc::sync_channel::<CMD>(0));

    let mut threads: Vec<_> = channels
        .enumerate()
        .map(|(id, (tx, rx))| {
            let hdl = Arc::clone(&pal);
            (
                vec![],
                tx,
                std::thread::spawn(move || {
                    assert!(core_affinity::set_for_current(core_affinity::CoreId { id }));
                    let bar = hdl;
                    let mut buf = vec![42u8; BS];
                    while let Ok(cmd) = rx.recv() {
                        match cmd {
                            CMD::Write(ptrs) => {
                                for ptr in ptrs.iter() {
                                    ptr.copy_from(&buf, &bar);
                                }
                            }
                            CMD::Read(ptrs) => {
                                for ptr in ptrs.iter() {
                                    ptr.copy_to(&mut buf, &bar);
                                }
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

    // Create all allocations.
    for id in 0..ITER {
        let ptr = pal.allocate(BS).unwrap();
        threads[id % WORKERS].0.push(ptr.clone());
    }
    for id in 0..WORKERS {
        threads[id % WORKERS]
            .1
            .send(CMD::Write(threads[id % WORKERS].0.clone()))
            .unwrap();
    }

    barrier.wait();
    let start = std::time::Instant::now();

    for (_, tx, _) in threads.iter() {
        tx.send(CMD::Wait).unwrap();
    }
    tx.send(
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32(),
    )
    .unwrap();

    barrier.wait();
    let start = std::time::Instant::now();
    for id in 0..WORKERS {
        threads[id % WORKERS]
            .1
            .send(CMD::Read(threads[id].0.clone()))
            .unwrap();
    }
    for (_, tx, _) in threads.iter() {
        tx.send(CMD::Wait).unwrap();
    }
    tx.send(
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32(),
    )
    .unwrap();
    barrier.wait();
    for (_, s, thread) in threads.into_iter() {
        drop(s);
        thread.join().unwrap();
    }
}

const JOBS: usize = 1;
const BS: usize = 4 * 1024 * 1024;
const SIZE: usize = 64 * 1024 * 1024 * 1024;
const EFFECTIVE_SIZE: usize = 32 * 1024 * 1024 * 1024;
const ITER: usize = EFFECTIVE_SIZE / BS;
const WORKERS: usize = 2;

fn main() {
    let (tx, rx) = std::sync::mpsc::channel();
    let barrier = Arc::new(std::sync::Barrier::new(JOBS));
    let jobs: Vec<_> = (0..JOBS)
        .map(|rank| {
            let foo = Arc::clone(&barrier);
            let bar = tx.clone();
            std::thread::spawn(move || bench_pal_sub(rank, foo, bar))
        })
        .collect();
    let mut bw: f32 = 0f32;
    for _ in 0..JOBS {
        bw += rx.recv().unwrap();
    }
    println!("Write: Achieved {} GiB/s", bw,);

    let mut bw: f32 = 0f32;
    for _ in 0..JOBS {
        bw += rx.recv().unwrap();
    }
    println!("Read: Achieved {} GiB/s", bw,);
    for thread in jobs.into_iter() {
        thread.join().unwrap();
    }
}
