use crate::LogReadResult;
use crate::config::LogConfig;
use crate::segment::LogSegment;
use anyhow::Result;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct Log {
    _dir: PathBuf,
    config: LogConfig,
    segments: Vec<LogSegment>,
}

impl Log {
    pub fn new(dir: PathBuf, config: LogConfig) -> Result<Self> {
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
            config,
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

    pub fn read(&mut self, offset: u64, max_bytes: i32) -> Result<LogReadResult> {
        // Find the segment that contains the offset.
        // Segments are sorted by base_offset.
        // We want the last segment such that segment.base_offset <= offset.

        let idx = self.segments.partition_point(|s| s.base_offset <= offset);

        if idx == 0 {
            // Offset is before the first segment (or empty segments)
            return Ok(LogReadResult::Data(Vec::new()));
        }

        // partition_point returns the first element where predicate is FALSE.
        // So s.base_offset > offset.
        // The element before it is the one we want.
        let segment = &mut self.segments[idx - 1];

        // Check if offset is within the segment logic is handled by segment.read
        // We use read_slice for zero-copy
        match segment.read_slice(offset, max_bytes)? {
            Some((file, file_offset, len)) => Ok(LogReadResult::FileSlice {
                file,
                offset: file_offset,
                len,
            }),
            None => Ok(LogReadResult::Data(Vec::new())),
        }
    }

    pub fn cleanup(&mut self) -> Result<()> {
        self.cleanup_by_time()?;
        self.cleanup_by_size()?;
        Ok(())
    }

    fn cleanup_by_time(&mut self) -> Result<()> {
        if let Some(ms) = self.config.retention_ms {
            let retention_duration = Duration::from_millis(ms);
            let now = SystemTime::now();

            // We iterate safely by collecting indices to remove first or just retaining
            // But we need to call delete() on them which might return Result.
            // And we traditionally don't delete the *active* segment even if it's old (Kafka behavior).
            // Let's assume we keep the last segment always.

            let mut split_idx = 0;
            for (i, segment) in self.segments.iter().enumerate() {
                if i == self.segments.len() - 1 {
                    break; // Keep active segment
                }

                let modified = segment.last_modified().unwrap_or(SystemTime::UNIX_EPOCH);
                if let Ok(age) = now.duration_since(modified) {
                    if age > retention_duration {
                        // Mark for deletion
                        // We continue to find the last segment that SHOULD be deleted.
                        // Actually if segment i is old, earlier segments are likely old too but not strictly required (modified time updates on write).
                        // But usually segments are ordered by time.
                        // Let's just check each individually.
                        tracing::info!(
                            "Deleting segment {} due to time retention (age {:?})",
                            segment.base_offset,
                            age
                        );
                        if let Err(e) = segment.delete() {
                            tracing::error!(
                                "Failed to delete segment {}: {}",
                                segment.base_offset,
                                e
                            );
                        } else {
                            split_idx = i + 1;
                        }
                    } else {
                        // If this segment is young enough, subsequent ones likely are too?
                        // Modification time increases. So yes.
                        break;
                    }
                }
            }

            if split_idx > 0 {
                self.segments.drain(0..split_idx);
            }
        }
        Ok(())
    }

    fn cleanup_by_size(&mut self) -> Result<()> {
        if let Some(max_bytes) = self.config.retention_bytes {
            let mut total_size: u64 = self.segments.iter().map(|s| s.size_bytes).sum();

            let mut split_idx = 0;
            for (i, segment) in self.segments.iter().enumerate() {
                if i == self.segments.len() - 1 {
                    break; // Keep active segment
                }

                if total_size > max_bytes {
                    tracing::info!(
                        "Deleting segment {} due to size retention (total {} > max {})",
                        segment.base_offset,
                        total_size,
                        max_bytes
                    );
                    if let Err(e) = segment.delete() {
                        tracing::error!("Failed to delete segment {}: {}", segment.base_offset, e);
                    } else {
                        total_size = total_size.saturating_sub(segment.size_bytes);
                        split_idx = i + 1;
                    }
                } else {
                    break;
                }
            }

            if split_idx > 0 {
                self.segments.drain(0..split_idx);
            }
        }
        Ok(())
    }
}
