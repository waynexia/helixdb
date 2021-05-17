use std::convert::TryInto;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use clap::{App, Arg, SubCommand};

mod load;
mod panel;
mod read;
mod scan;

use helixdb::option::Options;
use helixdb::{FnRegistry, HelixDB, SimpleTimestampReviewer};
use load::load;
use read::read;
use scan::scan;
use tracing::Level;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let matches = App::new("db_bench")
        .about("HelixDB benchmark tool")
        .arg(
            Arg::with_name("dir")
                .long("dir")
                .help("Database directory")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("thread")
                .long("thread")
                .help("Working threads number")
                .default_value("8")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("shard")
                .long("shard")
                .help("Shards number")
                .default_value("8")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compact_interval")
                .long("compact_interval")
                .help("Timestamp range (interval) of each compacted level")
                .default_value("1024"),
        )
        .subcommand(
            SubCommand::with_name("fill")
                .about("Write data")
                .arg(
                    Arg::with_name("batch_size")
                        .long("batch_size")
                        .help("batch size of each put request")
                        .default_value("1024"),
                )
                .arg(
                    Arg::with_name("num_key")
                        .long("num_key")
                        .help("Number of different keys to fill")
                        .default_value("1024"),
                )
                .arg(
                    Arg::with_name("num_timestamp")
                        .long("num_timestamp")
                        .help("Number of timestamp per key to fill")
                        .default_value("1024"),
                )
                .arg(
                    Arg::with_name("value_size")
                        .long("value_size")
                        .help("Size of each value in Bytes")
                        .default_value("1024"),
                ),
        )
        .subcommand(
            SubCommand::with_name("read")
                .about("Read data")
                .arg(
                    Arg::with_name("max_key")
                        .long("max_key")
                        .help("The max user key in database. This is used to specify key range.")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("max_timestamp")
                        .long("max_timestamp")
                        .help(
                            "The max timestamp in database. This is used to specify timestamp \
                             range.",
                        )
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("repeat_time")
                        .long("repeat_time")
                        .default_value("1024")
                        .help("Repeat times"),
                ),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("Scan data")
                .arg(
                    Arg::with_name("key_start")
                        .long("key_start")
                        .help("Start key of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("key_end")
                        .long("key_end")
                        .help("End key of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("timestamp_start")
                        .long("timestamp_start")
                        .help("Start timestamp of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("timestamp_end")
                        .long("timestamp_end")
                        .help("End timestamp of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("prefetch_size")
                        .long("prefetch_size")
                        .help("Prefetch buffer size")
                        .default_value("8"),
                )
                .arg(
                    Arg::with_name("repeat_time")
                        .long("repeat_time")
                        .help("Repeat times")
                        .default_value("1024"),
                ),
        )
        .get_matches();

    let dir = matches.value_of("dir").unwrap();
    let num_thread = matches.value_of("thread").unwrap().parse().unwrap();
    let num_shard = matches.value_of("shard").unwrap().parse().unwrap();
    let compact_interval = matches
        .value_of("compact_interval")
        .unwrap()
        .parse()
        .unwrap();
    let db = open_helix(dir, num_shard, compact_interval);
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    match matches.subcommand() {
        ("fill", Some(sub_matches)) => {
            let batch_size = sub_matches.value_of("batch_size").unwrap().parse().unwrap();
            let num_key = sub_matches.value_of("num_key").unwrap().parse().unwrap();
            let num_timestamp = sub_matches
                .value_of("num_timestamp")
                .unwrap()
                .parse()
                .unwrap();
            let value_size = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            load(
                db,
                num_thread,
                batch_size,
                num_key,
                num_timestamp,
                value_size,
            );
        }

        ("read", Some(sub_matches)) => {
            let max_key = sub_matches.value_of("max_key").unwrap().parse().unwrap();
            let max_ts = sub_matches
                .value_of("max_timestamp")
                .unwrap()
                .parse()
                .unwrap();
            let repeat_time = sub_matches
                .value_of("repeat_time")
                .unwrap()
                .parse()
                .unwrap();

            read(db, num_thread, max_key, max_ts, repeat_time);
        }

        ("scan", Some(sub_matches)) => {
            let prefetch_size = sub_matches
                .value_of("prefetch_size")
                .unwrap()
                .parse()
                .unwrap();
            let repeat_time = sub_matches
                .value_of("repeat_time")
                .unwrap()
                .parse()
                .unwrap();

            scan(db, num_thread, repeat_time, prefetch_size)
        }

        _ => unreachable!(),
    }

    // post process
    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
    std::io::stdout().flush().unwrap();
}

fn open_helix<P: AsRef<Path>>(path: P, num_shard: usize, compact_interval: i64) -> HelixDB {
    let simple_tsr = SimpleTimestampReviewer::new(compact_interval, 8192);
    let mut fn_registry = FnRegistry::new_noop();
    fn_registry.register_sharding_key_fn(Arc::new(move |key| {
        u64::from_le_bytes(key.to_owned().try_into().unwrap()) as usize % num_shard
    }));

    let opts = Options::default()
        .shards(num_shard)
        .set_timestamp_reviewer(Box::new(simple_tsr))
        .set_fn_registry(fn_registry)
        .set_task_buffer_size(1024);

    HelixDB::open(path, opts)
}
