use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct Index {
    file: File,
    entries: Vec<(u32, u32)>, // Relative offset -> Position
    _base_offset: u64,
}

impl Index {
    pub fn new(path: impl AsRef<Path>, base_offset: u64) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .with_context(|| format!("Failed to open index file: {:?}", path.as_ref()))?;

        let mut entries = Vec::new();
        let file_len = file.metadata()?.len();

        if file_len > 0 {
            let mut reader = BufReader::new(&file);
            let mut buf = [0u8; 8];
            while reader.read_exact(&mut buf).is_ok() {
                let relative_offset = u32::from_be_bytes(buf[0..4].try_into()?);
                let position = u32::from_be_bytes(buf[4..8].try_into()?);
                entries.push((relative_offset, position));
            }
            // Seek to end for writing
            file.seek(SeekFrom::End(0))?;
        }

        Ok(Self {
            file,
            entries,
            _base_offset: base_offset,
        })
    }

    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<()> {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&position.to_be_bytes());

        self.file.write_all(&buf)?;
        // Ideally we should flush periodically, but for now OS cache is fine.
        // self.file.flush()?;

        self.entries.push((relative_offset, position));
        Ok(())
    }

    pub fn lookup(&self, relative_offset: u32) -> Option<u32> {
        // Simple linear scan for now.
        // We want the largest offset less than or equal to the target offset.
        // Since entries are sorted by offset, we can iterate in reverse or use binary search.
        // `entries` stores (relative_offset, position).

        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the first entry with offset > relative_offset
        let idx = self
            .entries
            .partition_point(|&(off, _)| off <= relative_offset);

        if idx == 0 {
            // All entries are greater than relative_offset, or empty
            // If the first entry is exactly what we want (unlikely given <= check above), handle it.
            // Actually partition_point returns index of first element where predicate is false.
            // So elements [0..idx] satisfy off <= relative_offset.
            // The last element satisfying it is at idx - 1.
            None
        } else {
            Some(self.entries[idx - 1].1)
        }
    }

    pub fn entries_len(&self) -> usize {
        self.entries.len()
    }
}
