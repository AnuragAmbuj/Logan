use crate::compression::CompressionType;
use crate::{Decodable, Encodable};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

/// Helper for Varint encoding/decoding (Protobuf style)
pub mod varint {
    use anyhow::Result;
    use bytes::{Buf, BufMut};

    pub fn encode_i8(n: i8, buf: &mut impl BufMut) {
        buf.put_u8(n as u8); // Not strictly varint for attributes in record but i8 is just 1 byte
    }

    pub fn encode_i32(mut n: i32, buf: &mut impl BufMut) {
        // zigzag encoding
        let mut n = ((n << 1) ^ (n >> 31)) as u32;
        while n >= 0x80 {
            buf.put_u8((n as u8) | 0x80);
            n >>= 7;
        }
        buf.put_u8(n as u8);
    }

    pub fn encode_i64(mut n: i64, buf: &mut impl BufMut) {
        // zigzag encoding
        let mut n = ((n << 1) ^ (n >> 63)) as u64;
        while n >= 0x80 {
            buf.put_u8((n as u8) | 0x80);
            n >>= 7;
        }
        buf.put_u8(n as u8);
    }

    pub fn decode_i32(buf: &mut impl Buf) -> Result<i32> {
        let mut result: u32 = 0;
        let mut shift = 0;
        loop {
            if buf.remaining() == 0 {
                return Err(anyhow::anyhow!("Unexpected EOF decoding varint"));
            }
            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u32) << shift;
            if (byte & 0x80) == 0 {
                // zigzag decoding
                let n = (result >> 1) as i32;
                if (result & 1) != 0 {
                    return Ok(!n);
                } else {
                    return Ok(n);
                }
            }
            shift += 7;
            if shift > 35 {
                return Err(anyhow::anyhow!("Varint too long"));
            }
        }
    }

    pub fn decode_i64(buf: &mut impl Buf) -> Result<i64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            if buf.remaining() == 0 {
                return Err(anyhow::anyhow!("Unexpected EOF decoding varint"));
            }
            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 {
                // zigzag decoding
                let n = (result >> 1) as i64;
                if (result & 1) != 0 {
                    return Ok(!n);
                } else {
                    return Ok(n);
                }
            }
            shift += 7;
            if shift > 70 {
                return Err(anyhow::anyhow!("Varint too long"));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
    // Relative fields for reconstruction if needed
    pub offset_delta: i32,
    pub timestamp_delta: i64,
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<SimpleRecord>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

impl RecordBatch {
    pub fn new(base_offset: i64, compression: CompressionType, records: Vec<SimpleRecord>) -> Self {
        let mut attributes = 0i16;
        attributes |= (compression as i16) & 0x07;
        // Assume no timestamp type bit set (CreateTime)

        // Calculate max timestamp/offset delta based on records (simplified)
        let last_offset_delta = if records.is_empty() {
            0
        } else {
            (records.len() - 1) as i32
        };

        Self {
            base_offset,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0, // Calculated on encode
            attributes,
            last_offset_delta,
            first_timestamp: 0, // Needs real time
            max_timestamp: 0,   // Needs real time
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }

    pub fn compression(&self) -> CompressionType {
        CompressionType::from_id(self.attributes & 0x07).unwrap_or(CompressionType::None)
    }
}

impl Encodable for RecordBatch {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        // We need to encode the body to calculate length and CRC
        let mut body_buf = BytesMut::new();

        // 1. Encode fields AFTER BatchLength
        (self.partition_leader_epoch).encode(&mut body_buf)?;
        (self.magic).encode(&mut body_buf)?;

        // CRC covers from Attributes to End
        let mut record_data_buf = BytesMut::new();
        (self.attributes).encode(&mut record_data_buf)?;
        (self.last_offset_delta).encode(&mut record_data_buf)?;
        (self.first_timestamp).encode(&mut record_data_buf)?;
        (self.max_timestamp).encode(&mut record_data_buf)?;
        (self.producer_id).encode(&mut record_data_buf)?;
        (self.producer_epoch).encode(&mut record_data_buf)?;
        (self.base_sequence).encode(&mut record_data_buf)?;

        // Records
        (self.records.len() as i32).encode(&mut record_data_buf)?;

        // Encode records
        let mut records_buf = BytesMut::new();
        for record in &self.records {
            let mut rec_buf = BytesMut::new();
            varint::encode_i8(0, &mut rec_buf); // Attributes (unused)
            varint::encode_i64(record.timestamp_delta, &mut rec_buf);
            varint::encode_i32(record.offset_delta, &mut rec_buf);

            // Key
            match &record.key {
                Some(k) => {
                    varint::encode_i32(k.len() as i32, &mut rec_buf);
                    rec_buf.put_slice(k);
                }
                None => varint::encode_i32(-1, &mut rec_buf),
            }

            // Value
            match &record.value {
                Some(v) => {
                    varint::encode_i32(v.len() as i32, &mut rec_buf);
                    rec_buf.put_slice(v);
                }
                None => varint::encode_i32(-1, &mut rec_buf),
            }

            // Headers
            varint::encode_i32(record.headers.len() as i32, &mut rec_buf);
            for (k, v) in &record.headers {
                varint::encode_i32(k.len() as i32, &mut rec_buf);
                rec_buf.put_slice(k.as_bytes());
                varint::encode_i32(v.len() as i32, &mut rec_buf);
                rec_buf.put_slice(v);
            }

            // Record Length (varint)
            varint::encode_i32(rec_buf.len() as i32, &mut records_buf);
            records_buf.put(rec_buf);
        }

        // Compress records if needed
        let compressed_records = self.compression().compress(&records_buf)?;
        record_data_buf.put(compressed_records);

        // Calculate CRC
        let mut hasher = Hasher::new();
        hasher.update(&record_data_buf);
        let crc = hasher.finalize();

        // Assemble Body
        (crc as u32).encode(&mut body_buf)?;
        body_buf.put(record_data_buf);

        // Write Header
        (self.base_offset).encode(buf)?;
        (body_buf.len() as i32).encode(buf)?; // BatchLength
        buf.put(body_buf);

        Ok(())
    }
}

impl Decodable for RecordBatch {
    fn decode(buf: &mut impl bytes::Buf) -> Result<Self> {
        let base_offset = i64::decode(buf)?;
        let batch_length = i32::decode(buf)?;

        // We must limit reading to batch_length to avoid over-reading if we are in a stream
        // But `buf` might be the whole stream.
        // For safety, we should ideally verify remaining bytes, but let's assume we have enough for now or use a slice.

        let partition_leader_epoch = i32::decode(buf)?;
        let magic = i8::decode(buf)?;

        if magic != 2 {
            return Err(anyhow::anyhow!("Unsupported magic byte: {}", magic));
        }

        let crc = u32::decode(buf)?;

        // TODO: Validate CRC?

        let attributes = i16::decode(buf)?;
        let last_offset_delta = i32::decode(buf)?;
        let first_timestamp = i64::decode(buf)?;
        let max_timestamp = i64::decode(buf)?;
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let base_sequence = i32::decode(buf)?;

        let num_records = i32::decode(buf)?;

        // Compressed records are remaining in the batch_length
        // BatchLength includes everything AFTER BaseOffset and BatchLength fields.
        // Size so far: 4 (Epoch) + 1 (Magic) + 4 (CRC) + 2 (Attrs) + 4 (LOD) + 8 (FT) + 8 (MT) + 8 (PID) + 2 (PE) + 4 (BS) + 4 (NumRecs) = 49 bytes.
        // The rest is records.

        let header_size = 49;
        if batch_length < header_size as i32 {
            return Err(anyhow::anyhow!(
                "Invalid batch length: {} (min 49)",
                batch_length
            ));
        }
        let records_len = (batch_length as usize) - header_size;

        if buf.remaining() < records_len {
            return Err(anyhow::anyhow!("Not enough bytes for records"));
        }

        let mut records_data = vec![0u8; records_len];
        buf.copy_to_slice(&mut records_data);

        let compression =
            CompressionType::from_id(attributes & 0x07).unwrap_or(CompressionType::None);
        let decompressed_data = compression.decompress(&records_data)?;
        let mut records_buf = decompressed_data; // Bytes

        let mut records = Vec::with_capacity(num_records as usize);
        for _ in 0..num_records {
            let _rec_len = varint::decode_i32(&mut records_buf)?; // Consume record length
            let _attr = records_buf.get_u8(); // unused, we manually decode varint i8? no record attributes is just a byte usually but here we encoded 0
            let timestamp_delta = varint::decode_i64(&mut records_buf)?;
            let offset_delta = varint::decode_i32(&mut records_buf)?;

            let key_len = varint::decode_i32(&mut records_buf)?;
            let key = if key_len >= 0 {
                Some(records_buf.copy_to_bytes(key_len as usize))
            } else {
                None
            };

            let value_len = varint::decode_i32(&mut records_buf)?;
            let value = if value_len >= 0 {
                Some(records_buf.copy_to_bytes(value_len as usize))
            } else {
                None
            };

            let header_count = varint::decode_i32(&mut records_buf)?;
            let mut headers = Vec::with_capacity(header_count as usize);
            for _ in 0..header_count {
                let k_len = varint::decode_i32(&mut records_buf)?;
                let k_bytes = records_buf.copy_to_bytes(k_len as usize);
                let k = String::from_utf8(k_bytes.to_vec())?;

                let v_len = varint::decode_i32(&mut records_buf)?;
                let v = records_buf.copy_to_bytes(v_len as usize);
                headers.push((k, v));
            }

            records.push(SimpleRecord {
                key,
                value,
                headers,
                offset_delta,
                timestamp_delta,
            });
        }

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionType;

    #[test]
    fn test_record_batch_roundtrip_no_compression() {
        let records = vec![
            SimpleRecord {
                key: Some(Bytes::from("key1")),
                value: Some(Bytes::from("value1")),
                headers: vec![],
                offset_delta: 0,
                timestamp_delta: 0,
            },
            SimpleRecord {
                key: Some(Bytes::from("key2")),
                value: Some(Bytes::from("value2")),
                headers: vec![("h1".to_string(), Bytes::from("v1"))],
                offset_delta: 1,
                timestamp_delta: 10,
            },
        ];

        let batch = RecordBatch::new(100, CompressionType::None, records.clone());

        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = RecordBatch::decode(&mut read_buf).unwrap();

        assert_eq!(decoded.base_offset, 100);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key1")));
        assert_eq!(decoded.records[1].headers.len(), 1);
        assert_eq!(decoded.compression(), CompressionType::None);
    }

    #[test]
    fn test_record_batch_roundtrip_snappy() {
        let records = vec![SimpleRecord {
            key: Some(Bytes::from("key1")),
            value: Some(Bytes::from("value1".repeat(100))), // repeatable to compress well
            headers: vec![],
            offset_delta: 0,
            timestamp_delta: 0,
        }];

        let batch = RecordBatch::new(200, CompressionType::Snappy, records.clone());

        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = RecordBatch::decode(&mut read_buf).unwrap();

        assert_eq!(decoded.base_offset, 200);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            decoded.records[0].value,
            Some(Bytes::from("value1".repeat(100)))
        );
        assert_eq!(decoded.compression(), CompressionType::Snappy);
    }

    #[test]
    fn test_record_batch_roundtrip_lz4() {
        let records = vec![SimpleRecord {
            key: Some(Bytes::from("key1")),
            value: Some(Bytes::from("value1".repeat(100))),
            headers: vec![],
            offset_delta: 0,
            timestamp_delta: 0,
        }];

        let batch = RecordBatch::new(300, CompressionType::Lz4, records.clone());

        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = RecordBatch::decode(&mut read_buf).unwrap();

        assert_eq!(decoded.base_offset, 300);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            decoded.records[0].value,
            Some(Bytes::from("value1".repeat(100)))
        );
        assert_eq!(decoded.compression(), CompressionType::Lz4);
    }
}
