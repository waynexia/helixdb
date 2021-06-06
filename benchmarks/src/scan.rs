use std::sync::mpsc::channel;

use helixdb::iterator::Iterator;
use helixdb::option::ScanOption;
use helixdb::{HelixDB, NoOrderComparator};
use tokio::runtime::Builder;

use crate::panel::Panel;

pub fn scan(helixdb: HelixDB, num_thread: usize, repeat_time: usize, prefetch_buf_size: usize) {
    let (tx, rx) = channel();

    let mut panel = Panel::with_amount(repeat_time as u64);
    let rt = Builder::new_multi_thread()
        .worker_threads(num_thread)
        .build()
        .unwrap();

    for _ in 0..repeat_time as u64 {
        let helixdb = helixdb.clone();
        let tx = tx.clone();
        rt.spawn(async move {
            let mut iter = helixdb
                .scan::<NoOrderComparator>(
                    (0, 4).into(),
                    (
                        0usize.to_le_bytes().to_vec(),
                        1024usize.to_le_bytes().to_vec(),
                    ),
                    ScanOption { prefetch_buf_size },
                )
                .await
                .unwrap();
            let mut scan_cnt = 0;
            while iter.next().await.unwrap().is_some() {
                scan_cnt += 1
            }
            println!("scanned {} item", scan_cnt);
            tx.send(()).unwrap();
        });
    }

    for _ in rx.iter().take(repeat_time) {
        panel.increase(1);
    }
}
