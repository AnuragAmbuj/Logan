# Chapter 2: Trust No One - Data Integrity

## Introduction
In the world of distributed systems, paranoia is a virtue. Hardware is fallible. Disks have "bit rot", networks flip bits, and even memory can be corrupted. If Logan says "I stored your message," it must mean the message is stored *exactly* as sent, bit-for-bit.

## The Problem Statement
We faced several threats to data integrity:
1.  **Network Corruption**: TCP checksums are weak (16-bit) and can miss errors.
2.  **Disk corruption**: "Bit rot" where sectors degrade over time.
3.  **Phantom Writes**: Where the disk controller reports a write as successful, but it never hit the platter.

## The Journey to the Solution

### Attempt 1: Trust the Hardware
We could rely on TCP checksums and Disk ECC.
*   **Problem**: This is the "Ostrich Algorithm" (ignoring the problem). History has shown this is insufficient for large-scale reliable storage.

### Attempt 2: Application-Level Checksums
We decided to wrap every record with a checksum.
*   **Choice of Algorithm**:
    *   **MD5/SHA256**: Too slow for high throughput.
    *   **Adler32**: Fast, but weak collision resistance for short messages.
    *   **CRC32 (IEEE)**: The industry standard. Fast, hardware-accelerated on modern CPUs, and good error detection properties for burst errors.

We chose **CRC32** (specifically the Castagnoli polynomial used by Kafka/ISCSI) for its robustness.

## Architecture Breakdown

### The Record Wrapper
We don't just store the payload. We store a "Frame".

```text
[Length: 4B] [CRC: 4B] [Attributes: 1B] [Key Length: 4B] [Key] [Value Length: 4B] [Value]
```

Every time we write this frame:
1.  We calculate the CRC of the content (Attributes + Key + Value).
2.  We append it to the header.

Every time we read this frame:
1.  We read the whole frame.
2.  We recalculate the CRC of the content.
3.  We compare it with the stored CRC.
4.  Mismatch = **FATAL ERROR**.

## The Code Implementation

### Calculating Checksums
We use the `crc32fast` crate, which uses SIMD instructions for blazing speed.

```rust
use crc32fast::Hasher;

pub fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}
```

### Validation on Read
This is where the magic happens. We validate *lazily* or *eagerly* depending on the operation, but always before sending to the client (unless using Zero-Copy, where the Client does the validation!).

**Wait, slight correction**: In Zero-Copy `sendfile`, the broker *cannot* validate the data because it never sees it (it bypasses the CPU).
*   **Crucial Design Point**: This pushes the responsibility of CRC validation to the **Client**.
*   However, for background tasks (Compaction, Replication) where the Broker *does* read the data, we validate.

```rust
// In log.rs

pub fn read_and_validate(&mut self, offset: u64, size: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0; size];
    self.file.read_exact_at(&mut buffer, offset)?;
    
    // Parse the header
    let stored_crc = parse_u32(&buffer[4..8]);
    let payload = &buffer[8..];
    
    // Validate
    let actual_crc = compute_checksum(payload);
    
    if actual_crc != stored_crc {
        error!("Corrupt data detected at offset {}", offset);
        return Err(StorageError::CorruptData);
    }
    
    Ok(buffer)
}
```

## Conclusion
By embedding integrity checks directly into the storage format, Logan can detect corruption at rest. Interestingly, the interaction with Zero-Copy networking means we rely on the implementation of the *Client* to perform the final end-to-end check, creating a robust chain of trust from Producer -> Disk -> Consumer.
