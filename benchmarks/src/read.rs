use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use helixdb::option::ReadOption;
use helixdb::HelixDB;
use rand::{thread_rng, Rng};
use tokio::runtime::Builder;

use crate::panel::Panel;

fn generate_key(key: u64) -> Vec<u8> {
    key.to_le_bytes().to_vec()
}

pub fn read(helixdb: HelixDB, num_thread: usize, max_key: u64, max_ts: i64, repeat_time: usize) {
    let mut panel = Panel::with_amount(repeat_time as u64);
    let found = Arc::new(AtomicU64::new(0));

    let rt = Builder::new_multi_thread()
        .worker_threads(num_thread)
        .build()
        .unwrap();
    let progress = Arc::new(AtomicU64::new(0));
    panel.start();

    // todo: shuffle keys for "random write".
    for _ in 0..repeat_time {
        let helixdb = helixdb.clone();
        let progress = progress.clone();
        let mut rng = thread_rng();
        let key = generate_key(rng.gen_range(0..max_key));
        let ts = rng.gen_range(0..max_ts);
        let found = found.clone();
        rt.spawn(async move {
            if helixdb
                .get(ts, key, ReadOption::default())
                .await
                .unwrap()
                .is_some()
            {
                found.fetch_add(1, Ordering::Relaxed);
            }
            progress.fetch_add(1, Ordering::Relaxed);
        });
    }

    loop {
        let progress = progress.load(Ordering::Relaxed);
        panel.observe(progress);
        if progress >= repeat_time as u64 {
            break;
        }
    }

    println!("found {} / {}", found.load(Ordering::Relaxed), repeat_time);
}
