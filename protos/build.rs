use std::process::Command;

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

const IN_DIR: &str = "flatbuffer/helix.fbs";
const OUT_DIR: &str = "src/";

fn main() -> Result<()> {
    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg(OUT_DIR)
        .arg(IN_DIR)
        .status();

    match status {
        Ok(status) if !status.success() => panic!("`flatc` failed to compile the .fbs to Rust"),
        Ok(_status) => Ok(()), // Successfully compiled
        Err(err) => panic!("Could not execute `flatc`: {}", err),
    }
}
