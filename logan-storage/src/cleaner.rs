use crate::config::CleanupPolicy;
use crate::log::Log;
use crate::segment::LogSegment;
use anyhow::{Context, Result};
use bytes::BytesMut;
use logan_protocol::batch::RecordBatch;
use logan_protocol::codec::{Decodable, Encodable};
use std::collections::HashMap;

pub struct LogCleaner;

impl LogCleaner {
    /// Builds a map of valid keys to their latest location.
    /// Returns map: Key -> (SegmentBaseOffset, BatchRelativeOffset, RecordIndexInBatch)
    pub fn build_offset_map(log: &mut Log) -> Result<HashMap<Vec<u8>, (u64, u32, usize)>> {
        let mut map = HashMap::new();

        // Return empty map if policy is not Compact
        if log.config.cleanup_policy != CleanupPolicy::Compact {
            return Ok(map);
        }

        let segments_len = log.segments.len();
        if segments_len <= 1 {
            return Ok(map); // Nothing to compact if only 1 (active) segment
        }

        // Iterate all segments including the active one to ensure we find the absolute latest offsets
        for i in 0..segments_len {
            let segment = &mut log.segments[i];
            let base_offset = segment.base_offset;

            // Clone entries to avoid borrowing issues while mutating segment file position
            let entries = segment.index().entries().to_vec();

            for (rel_offset, pos) in entries {
                let record_bytes = segment.read_record_at_position(pos as u64)?;

                let mut buf_slice = &record_bytes[..];
                // Decode RecordBatch
                // We handle decode errors by context
                let batch = RecordBatch::decode(&mut buf_slice).with_context(|| {
                    format!(
                        "Failed to decode batch at offset {}",
                        base_offset + rel_offset as u64
                    )
                })?;

                for (rec_idx, record) in batch.records.iter().enumerate() {
                    if let Some(key) = &record.key {
                        map.insert(key.to_vec(), (base_offset, rel_offset, rec_idx));
                    }
                }
            }
        }
        Ok(map)
    }

    pub fn clean_segments(
        log: &mut Log,
        offset_map: &HashMap<Vec<u8>, (u64, u32, usize)>,
    ) -> Result<()> {
        if log.config.cleanup_policy != CleanupPolicy::Compact {
            return Ok(());
        }

        let segments_len = log.segments.len();
        if segments_len <= 1 {
            return Ok(());
        }

        // Iterate old segments
        for i in 0..segments_len - 1 {
            let segment = &mut log.segments[i];
            let base_offset = segment.base_offset;

            // Create .clean paths
            let clean_log_path = segment.log_path().with_extension("log.clean");
            let clean_index_path = segment.index_path().with_extension("index.clean");

            // We use a temporary scope to ensure files are closed before renaming
            {
                let mut clean_segment =
                    LogSegment::new_custom(&clean_log_path, &clean_index_path, base_offset)?;

                // Iterate original records
                let entries = segment.index().entries().to_vec();
                for (rel_offset, pos) in entries {
                    // Read original record payload (Batch)
                    let payload = segment.read_record_at_position(pos as u64)?;

                    let mut buf_slice = &payload[..];
                    let batch = RecordBatch::decode(&mut buf_slice)?;

                    // Filter records
                    let mut valid_records = Vec::new();
                    for (rec_idx, record) in batch.records.into_iter().enumerate() {
                        if let Some(key) = &record.key {
                            // Check if this is the latest version
                            if let Some(&(latest_base, latest_rel, latest_rec_idx)) =
                                offset_map.get(key.as_ref())
                            {
                                if latest_base == base_offset
                                    && latest_rel == rel_offset
                                    && latest_rec_idx == rec_idx
                                {
                                    valid_records.push(record);
                                }
                            }
                        } else {
                            // Keep records without keys? Kafka defaults to deleting them in compacted topics usually,
                            // but sometimes they are control records. For now, let's KEEP them to be safe.
                            valid_records.push(record);
                        }
                    }

                    if !valid_records.is_empty() {
                        // Create new batch with valid records
                        // We must preserve the original batch properties (timestamps, etc) ideally.
                        // But for now, we construct a new one.
                        // IMPORTANT: BaseOffset handling.
                        // If we are just appending to a new log, the offset inside the file usually monotonically increases.
                        // But for Compaction, we must PRESERVE the original offset of the record.
                        // `logan-storage` currently manages offsets by `next_offset`.
                        // If we append to `clean_segment`, it will assign NEW offsets (starting from base_offset).
                        // This re-assigns offsets! That effectively changes the offset of a message.
                        // In Kafka, offsets are immutable. Gaps are allowed.
                        // `logan-storage` needs to support appending with explicit offset or gaps.
                        // Current `LogSegment::append` uses `self.next_offset`.
                        // We can hack `clean_segment.next_offset` before appending?
                        // But `Index` needs to point `rel_offset` to `position`.

                        // If we change IDs, clients will be confused (offsets shift).
                        // This confirms `logan-storage` is too simple for full Kafka Compaction yet.
                        // But for "MVP Compaction", maybe shifting offsets is acceptable if we claim "offsets are not permanent"?
                        // No, offsets are critical.

                        // Workaround: We must preserve the original offset.
                        // `clean_segment` must allow us to set `next_offset` or append at specific offset.
                        // The `RecordBatch` has `base_offset`.
                        // The `Index` maps `offset - base` -> `pos`.
                        // So we need to:
                        // 1. Write batch bytes.
                        // 2. Add index entry `(original_offset - base) -> new_pos`.

                        // Note: `logan-storage` append calculates `offset = self.next_offset`.
                        // We need `raw_append` that takes explicit offset?
                        // `clean_segment` can use `append_raw`.

                        // BUT `RecordBatch` contains `Records`. We reconstruct `RecordBatch`.
                        // The batch `base_offset` should be the offset of the first record in it?
                        // If we filtered 2 records out of 5.
                        // Original: 100, 101, 102, 103, 104.
                        // Kept: 102, 104.
                        // If we write 102, 104 as a new Batch.
                        // Batch Base Offset = 102?
                        // Records in batch are delta-encoded from base.
                        // `SimpleRecord` has `offset_delta`.
                        // Rec 102: delta=0. Rec 104: delta=2.
                        // If we preserve `offset_delta`, we preserve absolute offset `base + delta`.
                        // So we must set `batch.base_offset` correctly and preserve deltas?
                        // If we drop 100, 101.
                        // We can keep `base_offset=100`, 100, 101 are just missing.
                        // Rec 102 (delta 2) is there.
                        // This is easiest! Just keep the batch as is, but remove records from vector.
                        // `RecordBatch` encode calculates length.
                        // If we remove records, the `records` vec shrinks.
                        // We just need to make sure we don't mess up deltas if the library auto-calculates them.
                        // `logan-protocol` `RecordBatch::new` recalculates deltas?
                        // Let's check `RecordBatch::new` in `batch.rs`.
                        // It calculates `last_offset_delta`.
                        // But it TAKES `records`. `SimpleRecord` has `offset_delta`.
                        // `RecordBatch::encode` uses `record.offset_delta`.
                        // So if we preserve `SimpleRecord` struct, we preserve the delta!
                        // So `base_offset` + `delta` = `original_offset`.
                        // Perfect.

                        // So:
                        // 1. Create new Batch `new_batch` with `valid_records`.
                        // 2. `new_batch.base_offset = batch.base_offset`.
                        // 3. Encoded new batch.
                        // 4. `clean_segment.append_raw(bytes, batch.base_offset)`?
                        //    Or just `append` but manipulate `next_offset`?
                        //    If we assume 1 Batch = 1 Append Index Entry.
                        //    Then the "Offset" in `logan-storage` index is the Batch Offset.
                        //    So we DO need to preserve the Batch Offset.
                        //    `clean_segment.next_offset` must be set to `base_offset + rel_offset`.

                        // I will duplicate `append` logic inline here or add `append_at_offset`.

                        // For now, I'll assume I can mutate `clean_segment.next_offset`?
                        // `next_offset` is public!
                        // We set fields directly since RecordBatch methods are not setters
                        let mut new_batch = RecordBatch::default();
                        new_batch.base_offset = batch.base_offset;
                        new_batch.partition_leader_epoch = batch.partition_leader_epoch;
                        new_batch.magic = batch.magic;
                        new_batch.attributes = batch.attributes; // Preserves compression
                        new_batch.first_timestamp = batch.first_timestamp;
                        new_batch.max_timestamp = batch.max_timestamp;
                        new_batch.producer_id = batch.producer_id;
                        new_batch.producer_epoch = batch.producer_epoch;
                        new_batch.base_sequence = batch.base_sequence;
                        new_batch.records = valid_records;

                        // Fix last_offset_delta to preserve original offset range
                        if let Some(last) = new_batch.records.last() {
                            new_batch.last_offset_delta = last.offset_delta;
                        }

                        clean_segment.next_offset = base_offset + rel_offset as u64;

                        let mut buf = BytesMut::new();
                        new_batch.encode(&mut buf)?;
                        clean_segment.append(&buf)?;
                    }
                }
            } // clean_segment closed (dropped)

            // Swap files
            std::fs::rename(&clean_log_path, segment.log_path())?;
            std::fs::rename(&clean_index_path, segment.index_path())?;

            // Re-open segment
            *segment = LogSegment::new(&log._dir, base_offset)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CleanupPolicy, LogConfig};
    use crate::log::Log;
    use crate::segment::LogSegment;
    use bytes::{Bytes, BytesMut};
    use logan_protocol::batch::{RecordBatch, SimpleRecord};
    use tempfile::tempdir;

    fn create_record(key: &str, val: &str) -> SimpleRecord {
        SimpleRecord {
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(Bytes::from(key.to_string())),
            value: Some(Bytes::from(val.to_string())),
            headers: vec![],
        }
    }

    #[test]
    fn test_compaction() -> Result<()> {
        let dir = tempdir()?;
        let config = LogConfig {
            retention_bytes: None,
            retention_ms: None,
            cleanup_policy: CleanupPolicy::Compact,
        };

        // 1. Create Data
        // Segment 0: Key A=1, Key B=1
        let mut seg0 = LogSegment::new(dir.path(), 0)?;
        let records0 = vec![create_record("A", "1"), create_record("B", "1")];
        let batch0 = RecordBatch::new(
            0,
            logan_protocol::compression::CompressionType::None,
            records0,
        );
        let mut buf0 = BytesMut::new();
        batch0.encode(&mut buf0)?;
        seg0.append(&buf0)?;

        // Segment 10 (Rolled): Key A=2
        let mut seg1 = LogSegment::new(dir.path(), 10)?;
        let records1 = vec![create_record("A", "2")];
        let batch1 = RecordBatch::new(
            10,
            logan_protocol::compression::CompressionType::None,
            records1,
        );
        let mut buf1 = BytesMut::new();
        batch1.encode(&mut buf1)?;
        seg1.append(&buf1)?;

        // Segment 20 (Active): Key C=1
        let seg2 = LogSegment::new(dir.path(), 20)?;

        // Construct Log
        let mut log = Log {
            _dir: dir.path().to_path_buf(),
            config,
            segments: vec![seg0, seg1, seg2],
        };

        // 2. Build Map
        let map = LogCleaner::build_offset_map(&mut log)?;
        // Map should have: A -> Seg1, B -> Seg0
        // A=2 in Seg1 (Offset 10)
        // B=1 in Seg0 (Offset 0)
        assert_eq!(map.get(b"A".as_slice()).unwrap().0, 10);
        assert_eq!(map.get(b"B".as_slice()).unwrap().0, 0);

        // 3. Compact
        LogCleaner::clean_segments(&mut log, &map)?;

        // 4. Verify Content
        // Segment 0 should contain ONLY B=1.

        let seg0 = &mut log.segments[0];
        assert_eq!(seg0.index().entries_len(), 1);

        // Read the one record (B=1)
        let pos = seg0.index().entries()[0].1;
        let payload = seg0.read_record_at_position(pos as u64)?;
        let batch = RecordBatch::decode(&mut &payload[..])?;
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].key.as_ref().unwrap(), &Bytes::from("B"));
        assert_eq!(batch.records[0].value.as_ref().unwrap(), &Bytes::from("1"));

        // Segment 1 should contain A=2.

        let seg1 = &mut log.segments[1];
        assert_eq!(seg1.index().entries_len(), 1);
        let pos1 = seg1.index().entries()[0].1;
        let payload1 = seg1.read_record_at_position(pos1 as u64)?;
        let batch1 = RecordBatch::decode(&mut &payload1[..])?;
        assert_eq!(batch1.records.len(), 1);
        assert_eq!(batch1.records[0].key.as_ref().unwrap(), &Bytes::from("A"));
        assert_eq!(batch1.records[0].value.as_ref().unwrap(), &Bytes::from("2"));

        Ok(())
    }
}
