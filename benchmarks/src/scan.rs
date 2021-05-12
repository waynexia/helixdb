use std::convert::TryInto;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Instant;

use helixdb::option::{Options, ScanOption};
use helixdb::{FnRegistry, HelixDB, NoOrderComparator, SimpleTimestampReviewer};
use indicatif::ProgressBar;
use tokio::runtime::Builder;

fn open_db<P: AsRef<Path>>(path: P, num_shard: usize) -> HelixDB {
    let simple_tsr = SimpleTimestampReviewer::new(1024, 8192);
    let mut fn_registry = FnRegistry::new_noop();
    fn_registry.register_sharding_key_fn(Arc::new(move |key| {
        u64::from_le_bytes(key.to_owned().try_into().unwrap()) as usize % num_shard
    }));

    let opts = Options::default()
        .shards(num_shard)
        .set_timestamp_reviewer(Box::new(simple_tsr))
        .set_fn_registry(fn_registry);

    HelixDB::open(path, opts)
}

pub fn scan<P: AsRef<Path>>(path: P, num_shard: usize, repeat_time: usize) {
    let helixdb = open_db(path, num_shard);
    let (tx, rx) = channel();

    let bar = ProgressBar::new(repeat_time as u64);
    let rt = Builder::new_multi_thread()
        .worker_threads(repeat_time)
        .build()
        .unwrap();
    let begin = Instant::now();

    for _ in 0..repeat_time as u64 {
        let helixdb = helixdb.clone();
        let tx = tx.clone();
        rt.spawn(async move {
            helixdb
                .scan::<NoOrderComparator>(
                    (0, 4).into(),
                    (
                        0usize.to_le_bytes().to_vec(),
                        1024usize.to_le_bytes().to_vec(),
                    ),
                    ScanOption {
                        prefetch_buf_size: 8,
                    },
                )
                .await
                .unwrap();
            tx.send(()).unwrap();
        });
    }

    for _ in rx.iter().take(repeat_time) {
        bar.inc(1);
    }
    bar.finish();

    let elapsed_us = begin.elapsed().as_micros();
    println!("elapsed: {:?} us", elapsed_us);
    println!(
        "average: {:.2} op/sec",
        repeat_time as f64 / (elapsed_us as f64 / 1_000_000.0)
    );
}
