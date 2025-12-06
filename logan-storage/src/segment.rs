use crate::index::Index;
use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct LogSegment {
    pub base_offset: u64,
    log_file: File,
    index: Index,
    pub next_offset: u64, // Exposed for Log to know where to append next
    pub size_bytes: u64,
}

impl LogSegment {
    pub fn new(dir: impl AsRef<Path>, base_offset: u64) -> Result<Self> {
        let dir = dir.as_ref();
        let log_path = dir.join(format!("{:020}.log", base_offset));
        let index_path = dir.join(format!("{:020}.index", base_offset));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open log file: {:?}", log_path))?;

        let size_bytes = file.metadata()?.len();
        let index = Index::new(index_path, base_offset)?;

        // Simple recovery: assume next_offset is base_offset + index.len()
        // This assumes 1 append = 1 offset increment, matching our current strategy.
        let next_offset = base_offset + index.entries_len() as u64;

        Ok(Self {
            base_offset,
            log_file: file,
            index,
            next_offset,
            size_bytes,
        })
    }

    pub fn append(&mut self, records: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let position = self.size_bytes; // Current size is the position to write at

        self.log_file.write_all(records)?;
        self.log_file.flush()?; // Ensure data is on disk (or at least in OS cache)

        // Update size
        self.size_bytes += records.len() as u64;

        // For now, index every message/batch
        // Casts are safe because segment size won't exceed u32 (4GB) in typical Kafka usage,
        // but we should probably check or handle larger files.
        // Kafka uses u32 for positions in index, so segments are capped at 4GB.
        if position <= u32::MAX as u64 {
            self.index
                .append((offset - self.base_offset) as u32, position as u32)?;
        }

        self.next_offset += 1;

        Ok(offset)
    }

    pub fn read(&mut self, offset: u64, max_bytes: i32) -> Result<Vec<u8>> {
        if offset < self.base_offset {
            return Ok(Vec::new());
        }

        let relative_offset = (offset - self.base_offset) as u32;
        if let Some(position) = self.index.lookup(relative_offset) {
            self.log_file.seek(SeekFrom::Start(position as u64))?;

            // Read up to max_bytes
            let mut buffer = vec![0u8; max_bytes as usize];
            let n = self.log_file.read(&mut buffer)?;
            buffer.truncate(n);
            Ok(buffer)
        } else {
            Ok(Vec::new())
        }
    }
}
