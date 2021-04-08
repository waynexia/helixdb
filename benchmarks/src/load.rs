use helixdb::option::Options;
use helixdb::{Entry, FnRegistry, HelixDB, SimpleTimestampReviewer};
use indicatif::ProgressBar;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
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

pub fn load<P: AsRef<Path>>(
    path: P,
    num_shard: usize,
    num_thread: usize,
    num_entry: usize,
    value_size: usize,
) {
    let helixdb = open_db(path, num_shard);
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
