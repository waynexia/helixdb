use std::time::Instant;

use indicatif::{ProgressBar, ProgressStyle};
use procfs::{diskstats, DiskStat};

pub struct Panel {
    amount: u64,
    processed: u64,

    bar: ProgressBar,
    timer: Instant,
    disk_monitor: DiskMonitor,
}

impl Panel {
    pub fn with_amount(amount: u64) -> Self {
        let bar = ProgressBar::new(amount);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:.bold.dim} [{bar:60}] ({pos}/{len}) {msg}")
                .progress_chars("=> "),
        );

        Self {
            amount,
            processed: 0,
            bar,
            timer: Instant::now(),
            disk_monitor: DiskMonitor::new(),
        }
    }

    pub fn start(&mut self) {
        self.timer = Instant::now();
    }

    pub fn observe(&mut self, progress: u64) {
        self.bar.set_position(progress);
        self.processed = progress;

        if progress >= self.amount {
            self.finish();
        }
    }

    pub fn increase(&mut self, delta: u64) {
        self.bar.inc(delta);
        self.processed += delta;

        if self.processed >= self.amount {
            self.finish();
        }
    }

    pub fn reset(&mut self, amount: usize) {
        todo!()
    }

    fn finish(&mut self) {
        let elapsed_ms = self.timer.elapsed().as_millis();
        self.bar.finish_with_message("done");

        println!("elapsed: {:?} ms", elapsed_ms);
        println!(
            "average: {:.2} op/sec",
            self.amount as f64 / (elapsed_ms as f64 / 1_000.0)
        );

        self.disk_monitor.finish();
    }

    fn conclude(&mut self) {
        // todo!()
    }
}

impl Drop for Panel {
    fn drop(&mut self) {
        self.conclude()
    }
}

struct DiskMonitor {
    records: Vec<DiskRecord>,
}

impl DiskMonitor {
    pub fn new() -> Self {
        let records = DiskMonitor::stats_iter()
            .map(DiskRecord::from_stat)
            .collect();

        Self { records }
    }

    pub fn finish(&self) {
        let delta = self
            .records
            .iter()
            .zip(DiskMonitor::stats_iter())
            .map(|(record, stat)| record.delta(stat))
            .collect::<Vec<_>>();

        println!("{:?}", delta);
    }

    /// Return a iterator of disk stat. Only stats that minor number equals to 0
    /// will be preserve. This means to only read the root devices' stat.
    fn stats_iter() -> impl Iterator<Item = DiskStat> {
        diskstats()
            .unwrap()
            .into_iter()
            .filter(|stat| stat.minor == 0)
    }
}

#[derive(Debug)]
struct DiskRecord {
    read_req: usize,
    read_sec: usize,
    time_reading_ms: usize,
    write_req: usize,
    write_sec: usize,
    time_writing_ms: usize,
    flush_req: Option<usize>,
    time_flushing_ms: Option<usize>,
}

impl DiskRecord {
    pub fn from_stat(stat: DiskStat) -> Self {
        Self {
            read_req: stat.reads,
            read_sec: stat.sectors_read,
            time_reading_ms: stat.time_reading,
            write_req: stat.writes,
            write_sec: stat.sectors_written,
            time_writing_ms: stat.time_writing,
            flush_req: stat.flushes,
            time_flushing_ms: stat.time_flushing,
        }
    }

    pub fn delta(&self, stat: DiskStat) -> Self {
        Self {
            read_req: stat.reads.wrapping_sub(self.read_req),
            read_sec: stat.sectors_read.wrapping_sub(self.read_sec),
            time_reading_ms: stat.time_reading.wrapping_sub(self.time_reading_ms),
            write_sec: stat.sectors_written.wrapping_sub(self.write_sec),
            write_req: stat.writes.wrapping_sub(self.write_req),
            time_writing_ms: stat.time_writing.wrapping_sub(self.time_writing_ms),
            // todo: option sub
            flush_req: stat.flushes,
            time_flushing_ms: stat.time_flushing,
        }
    }
}
