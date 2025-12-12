use anyhow::{Result, bail, ensure};
use crc32fast::Hasher;
use std::io::{Read, Write};

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub payload: Vec<u8>,
}

impl Record {
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Encodes the record to the writer.
    /// Format: [Length: 4 bytes] [CRC: 4 bytes] [Payload: N bytes]
    pub fn encode<W: Write>(&self, mut writer: W) -> Result<()> {
        let payload_len = self.payload.len() as u32;
        // Total size = CRC (4) + Payload (N)
        let total_len = 4 + payload_len;

        writer.write_all(&total_len.to_be_bytes())?;

        let mut hasher = Hasher::new();
        hasher.update(&self.payload);
        let checksum = hasher.finalize();

        writer.write_all(&checksum.to_be_bytes())?;
        writer.write_all(&self.payload)?;

        Ok(())
    }

    /// Decodes a record from the reader.
    /// Expects the Length header to NOT be read yet.
    pub fn decode<R: Read>(mut reader: R) -> Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let total_len = u32::from_be_bytes(len_buf);

        if total_len < 4 {
            bail!(
                "Corrupt record: length {} is too small to contain CRC",
                total_len
            );
        }

        let payload_len = total_len - 4;

        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let expected_crc = u32::from_be_bytes(crc_buf);

        let mut payload = vec![0u8; payload_len as usize];
        reader.read_exact(&mut payload)?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();

        ensure!(
            actual_crc == expected_crc,
            "CRC mismatch. Expected {}, got {}",
            expected_crc,
            actual_crc
        );

        Ok(Self { payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let data = b"hello world".to_vec();
        let record = Record::new(data.clone());

        let mut buffer = Vec::new();
        record.encode(&mut buffer).unwrap();

        let decoded = Record::decode(&buffer[..]).unwrap();
        assert_eq!(decoded.payload, data);
    }

    #[test]
    fn test_crc_mismatch() {
        let data = b"hello world".to_vec();
        let record = Record::new(data);

        let mut buffer = Vec::new();
        record.encode(&mut buffer).unwrap();

        // Corrupt the payload (last byte)
        let len = buffer.len();
        buffer[len - 1] ^= 0xFF;

        let result = Record::decode(&buffer[..]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn test_truncated_data() {
        let data = b"hello world".to_vec();
        let record = Record::new(data);

        let mut buffer = Vec::new();
        record.encode(&mut buffer).unwrap();

        // 1. Truncate header (less than 4 bytes)
        let result = Record::decode(&buffer[0..2]);
        assert!(result.is_err());

        // 2. Truncate CRC (less than 8 bytes total)
        let len_with_partial_crc = 4 + 2;
        if buffer.len() >= len_with_partial_crc {
            let result = Record::decode(&buffer[0..len_with_partial_crc]);
            assert!(result.is_err());
        }

        // 3. Truncate payload
        let len_with_partial_payload = buffer.len() - 1;
        let result = Record::decode(&buffer[0..len_with_partial_payload]);
        assert!(result.is_err());
    }
}
