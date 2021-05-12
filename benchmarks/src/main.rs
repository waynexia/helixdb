use std::convert::TryInto;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use clap::{App, Arg, SubCommand};

mod load;
mod panel;
mod scan;

use helixdb::option::Options;
use helixdb::{FnRegistry, HelixDB, SimpleTimestampReviewer};
use load::load;
use scan::scan;

fn main() {
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
        .subcommand(
            SubCommand::with_name("fill")
                .about("Write data")
                .arg(
                    Arg::with_name("entries")
                        .long("entries")
                        .help("Entries number to fill")
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
            SubCommand::with_name("scan")
                .about("Scan data")
                .arg(
                    Arg::with_name("key_start")
                        .long("key_start")
                        // .short("ks")
                        .help("Start key of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("key_end")
                        .long("key_end")
                        // .short("ke")
                        .help("End key of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("timestamp_start")
                        .long("timestamp_start")
                        // .short("ts")
                        .help("Start timestamp of this scan (inclusive)"),
                )
                .arg(
                    Arg::with_name("timestamp_end")
                        .long("timestamp_end")
                        // .short("te")
                        .help("End timestamp of this scan (inclusive)"),
                ),
        )
        .get_matches();

    let dir = matches.value_of("dir").unwrap();
    let num_thread = matches.value_of("thread").unwrap().parse().unwrap();
    let num_shard = matches.value_of("shard").unwrap().parse().unwrap();
    let db = open_helix(dir, num_shard);

    match matches.subcommand() {
        ("fill", Some(sub_matches)) => {
            let num_entry = sub_matches.value_of("entries").unwrap().parse().unwrap();
            let value_size = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            load(db, num_thread, num_entry, value_size);
        }

        ("scan", Some(_)) => scan(dir, num_shard, 10000),

        _ => unreachable!(),
    }

    std::io::stdout().flush().unwrap();
}

fn open_helix<P: AsRef<Path>>(path: P, num_shard: usize) -> HelixDB {
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
