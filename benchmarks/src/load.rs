use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use helixdb::{Entry, HelixDB};
use tokio::runtime::Builder;

use crate::panel::Panel;

fn generate_entry(timestamp: i64, key: u64, value_size: usize) -> Entry {
    let key = key.to_le_bytes().to_vec();
    let mut value = Vec::with_capacity(value_size);
    value.resize_with(value_size, Default::default);

    Entry {
        timestamp,
        key,
        value,
    }
}

pub fn load(
    helixdb: HelixDB,
    num_thread: usize,
    batch_size: usize,
    num_key: usize,
    num_timestamp: usize,
    value_size: usize,
) {
    let total_entry = num_key * num_timestamp;
    let mut panel = Panel::with_amount(total_entry as u64);

    let rt = Builder::new_multi_thread()
        .worker_threads(num_thread)
        .build()
        .unwrap();
    let progress = Arc::new(AtomicU64::new(0));
    panel.start();

    for ts in 0..num_timestamp as i64 {
        let keys = (0..num_key as u64).collect::<Vec<_>>();
        // todo: shuffle keys for "random write".
        for keys in keys.chunks(batch_size) {
            let keys_len = keys.len() as u64;
            let helixdb = helixdb.clone();
            let progress = progress.clone();
            let write_batch = keys
                .iter()
                .map(|key| generate_entry(ts, *key, value_size))
                .collect();
            rt.spawn(async move {
                helixdb.put(write_batch).await.unwrap();
                progress.fetch_add(keys_len, Ordering::Relaxed);
            });
        }
    }

    loop {
        let progress = progress.load(Ordering::Relaxed);
        panel.observe(progress);
        if progress >= total_entry as u64 {
            break;
        }
    }
}
