use std::sync::Arc;

use pmdk::PMem;

const BUFFER_SIZE: usize = 4 * 1024;
const SIZE: usize = 64 * 1024 * 1024 * 1024;
const ITER: usize = SIZE / BUFFER_SIZE;
const JOBS: usize = 8;
const OPS_PER_JOB: usize = ITER / JOBS;
const REM_OPS: usize = ITER % JOBS;
enum CMD {
    READ,
    WRITE,
    WAIT,
}

fn basic_read_write_test(path: &str) -> Result<(), std::io::Error> {
    let pmem = Arc::new(match PMem::create(path, SIZE) {
        Ok(value) => value,
        Err(_) => PMem::open(path)?,
    });

    let threads: Vec<_> = (0..JOBS)
        .map(|id| {
            let p = Arc::clone(&pmem);
            let (tx, rx) = std::sync::mpsc::sync_channel::<CMD>(0);
            (
                tx,
                std::thread::spawn(move || {
                    assert!(core_affinity::set_for_current(core_affinity::CoreId {
                        id: id
                    }));
                    let mut buf = vec![0u8; BUFFER_SIZE];
                    while let Ok(msg) = rx.recv() {
                        match msg {
                            CMD::READ => {
                                for it in 0..OPS_PER_JOB {
                                    p.read((it * BUFFER_SIZE) + (id * BUFFER_SIZE), &mut buf)
                                }
                                if id < REM_OPS {
                                    p.read(
                                        JOBS * OPS_PER_JOB * BUFFER_SIZE + (id * BUFFER_SIZE),
                                        &mut buf,
                                    )
                                }
                            }
                            CMD::WRITE => unsafe {
                                for it in 0..OPS_PER_JOB {
                                    p.write((it * BUFFER_SIZE) + (id * BUFFER_SIZE), &buf)
                                }
                                if id < REM_OPS {
                                    p.write(
                                        JOBS * OPS_PER_JOB * BUFFER_SIZE + (id * BUFFER_SIZE),
                                        &buf,
                                    )
                                }
                            },
                            CMD::WAIT => {}
                        }
                    }
                }),
            )
        })
        .collect();

    // Write
    let start = std::time::Instant::now();
    for id in 0..JOBS {
        threads[id].0.send(CMD::WRITE).unwrap();
    }
    for id in 0..JOBS {
        threads[id % JOBS].0.send(CMD::WAIT).unwrap();
    }

    println!(
        "Write: Achieved {} GiB/s",
        SIZE as f32 / 1024f32 / 1024f32 / 1024f32 / start.elapsed().as_secs_f32()
    );

    // Read
    let start = std::time::Instant::now();
    for id in 0..JOBS {
        threads[id % JOBS].0.send(CMD::READ).unwrap();
    }
    for id in 0..JOBS {
        threads[id % JOBS].0.send(CMD::WAIT).unwrap();
    }

    println!(
        "Read: Achieved {} GiB/s",
        SIZE as f32 / 1024f32 / 1024f32 / 1024f32 / start.elapsed().as_secs_f32()
    );

    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    basic_read_write_test("PATH_TO_YOUR_PMEM")?;
    Ok(())
}
