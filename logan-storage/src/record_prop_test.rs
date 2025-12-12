#[cfg(test)]
mod tests {
    use crate::record::Record;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_record_roundtrip(payload in prop::collection::vec(any::<u8>(), 0..1024)) {
            let record = Record::new(payload.clone());
            let mut buffer = Vec::new();
            record.encode(&mut buffer).unwrap();

            let decoded = Record::decode(&buffer[..]).unwrap();
            assert_eq!(decoded.payload, payload);
        }

        #[test]
        fn test_record_corruption(
            payload in prop::collection::vec(any::<u8>(), 1..1024), // Must have at least 1 byte to corrupt
            corruption_index in 0usize..1000 // Index to corrupt
        ) {
            let record = Record::new(payload);
            let mut buffer = Vec::new();
            record.encode(&mut buffer).unwrap();

            // Total length is 4 (len) + 4 (crc) + payload.len
            let total_len = buffer.len();

            // Limit corruption index to valid range within the buffer
            // We want to avoid corrupting the Length prefix if possible for THIS specific test
            // to ensure it fails on CRC, but corrupting length is also a valid failure case.
            // Let's just pick a valid index in the buffer.
            let idx = corruption_index % total_len;

            // Flip a bit
            buffer[idx] ^= 0xFF;

            // Attempt decode
            let result = Record::decode(&buffer[..]);

            // It should fail either due to CRC mismatch OR length mismatch (if we corrupted length)
            // OR if we corrupted length to be huge, it might fail on allocation or reading.
            assert!(result.is_err());
        }
    }
}
