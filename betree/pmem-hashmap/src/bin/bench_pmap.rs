use std::process::Command;

use pmem_hashmap::PMap;

const BS: usize = 4 * 1024 * 1024;
const SIZE: usize = 64 * 1024 * 1024 * 1024;
const EFFECTIVE_SIZE: usize = 32 * 1024 * 1024 * 1024;
const ITER: usize = EFFECTIVE_SIZE / BS;
const WORKERS: usize = 2;

fn main() {
    let _ = Command::new("rm").arg("/home/wuensche/pmem/bar").status();
    let mut pmap = PMap::create("/home/wuensche/pmem/bar", SIZE).unwrap();

    enum CMD {
        Read(usize),
        Wait,
    }
    // Initiate workers
    let channels = (0..WORKERS).map(|_| std::sync::mpsc::sync_channel::<CMD>(0));

    let start = std::time::Instant::now();
    let buf = vec![42u8; BS];
    for id in 0..ITER {
        pmap.insert(id, &buf).unwrap();
    }
    println!(
        "Write: Achieved {} GiB/s",
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32()
    );

    let pmap = std::sync::Arc::new(pmap);

    let threads: Vec<_> = channels
        .map(|(tx, rx)| {
            let foo = pmap.clone();
            (
                tx,
                std::thread::spawn(move || {
                    let mut buf = vec![42u8; BS];
                    while let Ok(msg) = rx.recv() {
                        match msg {
                            CMD::Read(id) => buf.copy_from_slice(foo.get(id).unwrap()),
                            CMD::Wait => {}
                        }
                    }
                }),
            )
        })
        .collect();
    let start = std::time::Instant::now();
    for id in 0..ITER {
        threads[id % WORKERS].0.send(CMD::Read(id)).unwrap();
    }
    for id in 0..WORKERS {
        threads[id % WORKERS].0.send(CMD::Wait).unwrap();
    }
    for (s, thread) in threads.into_iter() {
        drop(s);
        thread.join().unwrap();
    }
    println!(
        "Read: Achieved {} GiB/s",
        (ITER as f32 * BS as f32 / 1024f32 / 1024f32 / 1024f32) / start.elapsed().as_secs_f32()
    );
}
