use crate::index::Index;
use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use std::path::PathBuf;

#[derive(Debug)]
pub struct LogSegment {
    pub base_offset: u64,
    log_file: File,
    log_path: PathBuf,
    index_path: PathBuf,
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
        // Clone index path because Index::new consumes it (or uses it) - actually Index::new takes path: P
        let index = Index::new(index_path.clone(), base_offset)?;

        // Simple recovery: assume next_offset is base_offset + index.len()
        // This assumes 1 append = 1 offset increment, matching our current strategy.
        let next_offset = base_offset + index.entries_len() as u64;

        Ok(Self {
            base_offset,
            log_file: file,
            log_path,
            index_path,
            index,
            next_offset,
            size_bytes,
        })
    }

    pub fn append(&mut self, records: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let position = self.size_bytes; // Current size is the position to write at

        let record = crate::record::Record::new(records.to_vec());
        let mut buffer = Vec::new();
        record.encode(&mut buffer)?;

        self.log_file.write_all(&buffer)?;
        self.log_file.flush()?; // Ensure data is on disk (or at least in OS cache)

        // Update size
        let bytes_written = buffer.len() as u64;
        self.size_bytes += bytes_written;

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

    pub fn read(&mut self, offset: u64, _max_bytes: i32) -> Result<Vec<u8>> {
        if offset < self.base_offset {
            return Ok(Vec::new());
        }

        let relative_offset = (offset - self.base_offset) as u32;
        if let Some(position) = self.index.lookup(relative_offset) {
            self.log_file.seek(SeekFrom::Start(position as u64))?;

            // Read the record using our helper
            let record = crate::record::Record::decode(&mut self.log_file)?;
            Ok(record.payload)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn read_slice(&mut self, offset: u64, _max_bytes: i32) -> Result<Option<(File, u64, u64)>> {
        if offset < self.base_offset {
            return Ok(None);
        }

        let relative_offset = (offset - self.base_offset) as u32;
        if let Some(position) = self.index.lookup(relative_offset) {
            self.log_file.seek(SeekFrom::Start(position as u64))?;

            let mut len_buf = [0u8; 4];
            self.log_file.read_exact(&mut len_buf)?;
            let total_len = u32::from_be_bytes(len_buf);

            // Total record size on disk = 4 (len prefix) + total_len
            // But we want to return ONLY the payload (skipping 4 bytes Len + 4 bytes CRC)
            // total_len includes CRC (4 bytes).
            // So payload len = total_len - 4.
            // Payload offset = position + 4 (Len) + 4 (CRC) = position + 8.

            let payload_len = (total_len as u64).saturating_sub(4);
            let payload_offset = position as u64 + 8;
            let file_clone = self.log_file.try_clone()?;

            Ok(Some((file_clone, payload_offset, payload_len)))
        } else {
            Ok(None)
        }
    }

    pub fn last_modified(&self) -> Result<std::time::SystemTime> {
        self.log_file
            .metadata()?
            .modified()
            .context("Failed to get modification time")
    }

    pub fn delete(&self) -> Result<()> {
        std::fs::remove_file(&self.log_path).context("Failed to delete log file")?;
        std::fs::remove_file(&self.index_path).context("Failed to delete index file")?;
        Ok(())
    }
}
