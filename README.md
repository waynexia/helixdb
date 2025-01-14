<img src="docs/image/Helix_DB_Logo_Proposal_Wordart.svg" height="200" style="display: block;  margin-left: auto;  margin-right: auto;">

# HelixDB

HelixDB is a Key-Value store written in Rust. Checkout our [wiki](https://github.com/waynexia/helixdb/wiki) to get more!

# Features
## Time Series
HelixDB is designed to serve time-series data. "Key-Value" definition here is (`User Key`, `Logical Timestamp`) => `Data`

## Time aware
HelixDB organizes data in a time-aware way. This gives HelixDB the ability to efficiently processing time related requests like "Hierarchy" or "Outdate".

## Custom Compression
HelixDB gives users an interface to customize their compression method that best suits their data. 

## Async I/O & Thread-Per-Core
HelixDB use io-uring provided by glommio as IO library. The thread-per-core architecture is also built on top of glommio.

HelixDB provides async interface, which is `Send` and can be spawned into other async runtime like tokio.

# Status
*This project is still in the early stages.* Laking of test coverage, robust functionality, documentation and other things. So

Any discussion / suggestions / pull requests / issues / ... are welcome :heart:
