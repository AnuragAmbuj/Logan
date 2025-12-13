# Chapter 4: Compressing Time and Space - Batching & Compression

## Introduction
Sending millions of tiny 100-byte JSON messages individually is a networking disaster. The overhead of TCP headers (40 bytes), IP headers (20 bytes), and Frame overhead dominates the bandwidth. To scale, we must aggregate.

## The Problem Statement
1.  **Network Overhead**: Small packets have poor payload-to-header ratios.
2.  **Syscall Overhead**: Calling `write()` 1 million times a second kills the OS.
3.  **Storage Costs**: Text-based data (JSON, XML, Logs) is highly repetitive and wasteful on disk.

## The Solution: Record Batching
Instead of sending one message at a time, the Producer groups messages into a **RecordBatch**.
*   **Default**: 16KB or 10ms delay.
*   **Result**: One syscall for hundreds of messages.

## The Solution: Batch Compression
Once we have a batch, we can compress it.
*   **Why Batch Compression?**
    *   Compressing a 100-byte message yields negative compression (header overhead).
    *   Compressing a 16KB batch of repetitive JSON yields 80-90% reduction.

**Algorithms Supported**:
*   **Snappy**: Google's creation. Aimed at very high speeds (500MB/s+). Great for real-time.
*   **LZ4**: Even faster decompression than Snappy.
*   **Zstd**: Facebook's modern algo. Higher compression than Gzip, speed close to Snappy.

## Architecture Breakdown

### The Batch Format (Kafka v2)
We adopted the standard Kafka v2 Batch format for compatibility.

```text
[BaseOffset] [BatchLength] ... [Attributes(CompressionType)] ... [Records...]
```

When compression is enabled, the `Records` field acts as a "wrapper".
1.  The outer batch header stays uncompressed (so the Broker can read offsets/metadata).
2.  The *body* of the batch contains the compressed data.
3.  The compressed data, when uncompressed, reveals the inner RecordBatch.

This allows the Broker to route messages without decompressing them (mostly), unless it needs to re -assign offsets or validate deep data.

## The Code Implementation

### Compressing a Batch
We use traits to abstract the compression logic.

```rust
pub enum Compression {
    None,
    Snappy,
    Lz4,
    Zstd,
}

pub fn compress_batch(data: &[u8], algo: Compression) -> Result<Vec<u8>> {
    match algo {
        Compression::None => Ok(data.to_vec()),
        Compression::Snappy => {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            encoder.write_all(data)?;
            Ok(encoder.into_inner()?)
        },
        Compression::Lz4 => {
             // LZ4 implementation
             let mut encoder = lz4::EncoderBuilder::new().build(Vec::new())?;
             encoder.write_all(data)?;
             let (output, result) = encoder.finish();
             result?;
             Ok(output)
        },
        // ...
    }
}
```

### The Trade-off Triangle
*   **GZIP**: Low CPU (encode), High CPU (decode), Best Ratio. Good for archival.
*   **LZ4**: Low CPU (encode), Tiny CPU (decode), OK Ratio. Good for high throughput.
*   **Snappy**: Balanced.

For Logan, we default to **Snappy** as a good middle ground for streaming.

## Conclusion
Batching solves the syscall/network overhead. Compression solves the bandwidth/storage bottleneck. Together, they allow Logan to handle orders of magnitude more throughput than a naive "one-message-at-a-time" system.
