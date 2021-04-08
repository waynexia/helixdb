use helixdb::{Entry, HelixDB};
use indicatif::ProgressBar;
use std::path::Path;
use std::sync::mpsc::channel;
use tokio::runtime::Builder;

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

pub fn load<P: AsRef<Path>>(
    dir: P,
    num_shard: usize,
    num_thread: usize,
    num_entry: usize,
    value_size: usize,
) {
    let helixdb = HelixDB::new(dir, num_shard);
    // let (tx, rx) = channel();

    let bar = ProgressBar::new(num_entry as u64);
    let rt = Builder::new_multi_thread()
        .worker_threads(num_thread)
        .build()
        .unwrap();

    for key in 0..num_entry as u64 {
        let helixdb = helixdb.clone();
        // let tx = tx.clone();
        // rt.spawn(async move {
        helixdb.put(vec![generate_entry(0, key, value_size)]);
        bar.inc(1);
        // tx.send(());
        // });
    }

    // for _ in rx.iter().take(num_entry) {
    //     bar.inc(1);
    // }
}
