use crate::segment::LogSegment;
use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Log {
    _dir: PathBuf,
    segments: Vec<LogSegment>,
}

impl Log {
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;

        // Scan directory for .log files to recover segments
        let mut base_offsets = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(offset) = filename.parse::<u64>() {
                        base_offsets.push(offset);
                    }
                }
            }
        }

        base_offsets.sort_unstable();

        let mut segments = Vec::new();
        if base_offsets.is_empty() {
            // New log, start with segment 0
            segments.push(LogSegment::new(&dir, 0)?);
        } else {
            for offset in base_offsets {
                segments.push(LogSegment::new(&dir, offset)?);
            }
        }

        Ok(Self {
            _dir: dir,
            segments,
        })
    }

    pub fn append(&mut self, records: &[u8]) -> Result<u64> {
        let segment = self
            .segments
            .last_mut()
            .expect("Log should always have at least one segment");
        // TODO: Check segment size and roll if needed
        segment.append(records)
    }

    pub fn read(&mut self, offset: u64, max_bytes: i32) -> Result<Vec<u8>> {
        // Find the segment that contains the offset.
        // Segments are sorted by base_offset.
        // We want the last segment such that segment.base_offset <= offset.

        let idx = self.segments.partition_point(|s| s.base_offset <= offset);

        if idx == 0 {
            // Offset is before the first segment (or empty segments)
            return Ok(Vec::new());
        }

        // partition_point returns the first element where predicate is FALSE.
        // So s.base_offset > offset.
        // The element before it is the one we want.
        let segment = &mut self.segments[idx - 1];

        // Check if offset is within the segment logic is handled by segment.read
        segment.read(offset, max_bytes)
    }
}
