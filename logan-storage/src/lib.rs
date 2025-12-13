pub mod cleaner;
pub mod config;
pub mod index;
pub mod log;
pub mod manager;
pub mod record;
pub mod record_prop_test;
pub mod segment;

use std::fs::File;

#[derive(Debug)]
pub enum LogReadResult {
    Data(Vec<u8>),
    FileSlice { file: File, offset: u64, len: u64 },
}

mod index_prop_test;

pub use index::Index;
pub use log::Log;
pub use manager::LogManager;
pub use segment::LogSegment;
