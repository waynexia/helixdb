use std::sync::mpsc::channel;

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

pub fn load(helixdb: HelixDB, num_thread: usize, num_entry: usize, value_size: usize) {
    let mut panel = Panel::with_amount(num_entry as u64);

    let (tx, rx) = channel();
    let rt = Builder::new_multi_thread()
        .worker_threads(num_thread)
        .build()
        .unwrap();
    // let begin = Instant::now();
    panel.start();

    for key in 0..num_entry as u64 {
        let helixdb = helixdb.clone();
        let tx = tx.clone();
        rt.spawn(async move {
            helixdb
                .put(vec![generate_entry(0, key, value_size)])
                .await
                .unwrap();
            tx.send(()).unwrap();
        });
    }

    for _ in rx.iter().take(num_entry) {
        panel.increase(1);
    }

    // let elapsed_us = begin.elapsed().as_micros();
    // println!("elapsed: {:?} us", elapsed_us);
    // println!(
    //     "average: {:.2} op/sec",
    //     num_entry as f64 / (elapsed_us as f64 / 1_000_000.0)
    // );
}
