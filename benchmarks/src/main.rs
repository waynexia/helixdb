use clap::{App, Arg, SubCommand};

mod load;

use load::load;

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
                        .help("Entries number to fill")
                        .default_value("1024"),
                )
                .arg(
                    Arg::with_name("value_size")
                        .help("Size of each value in Bytes")
                        .default_value("1024"),
                ),
        )
        .get_matches();

    let dir = matches.value_of("dir").unwrap();
    let num_thread = matches.value_of("thread").unwrap().parse().unwrap();
    let num_shard = matches.value_of("shard").unwrap().parse().unwrap();

    match matches.subcommand() {
        ("fill", Some(sub_matches)) => {
            let num_entry = sub_matches.value_of("entries").unwrap().parse().unwrap();
            let value_size = sub_matches.value_of("value_size").unwrap().parse().unwrap();

            load(dir, num_shard, num_thread, num_entry, value_size);
        }

        _ => unreachable!(),
    }
}
