# Logan

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()

**Status: Implementation Phase (Experimental)**

Logan is a high-performance, distributed streaming platform written in Rust, designed to be compatible with the Apache Kafka protocol. It focuses on experimental high-performance architecture (Thread-per-Core, Zero-Copy I/O) while maintaining compatibility with standard Kafka clients.

## Features

### Performance
- **Core-Local Partitioning**: Implements a Shared-Nothing Actor model to eliminate lock contention on hot partitions.
- **Zero-Copy Networking**: Uses `sendfile` (on Linux) for high-throughput `Fetch` responses, bypassing user-space copying.
- **Batching & Compression**: Support for Kafka v2 `RecordBatch` format with `Snappy` and `LZ4` compression.
- **TCP_NODELAY**: Optimized for low-latency delivery.

### Reliability
- **CRC32 Data Integrity**: All log records are protected by CRC32 checksums, validated on read.
- **Configurable Retention**: Policy-based log cleanup by time (hours) or size (bytes).
- **WAL-based Storage**: Durable append-only log segments.
- **Log Compaction**: Basic support for log cleaning and compaction strategies.

### Compatibility
- **Kafka Protocol Support**: Compatible with standard Kafka clients like `kcat` (librdkafka) and `kafka-python`.
- **Supported APIs**: 
    - `ApiVersions` (v0-v3)
    - `Metadata` (v0)
    - `Produce` (v2)
    - `Fetch` (v0)
    - `OffsetCommit` (v2)
    - `OffsetFetch` (v1)
    - `CreateTopics`
    - `DeleteTopics`

## Architecture

Logan uses a **Shard-based Architecture**:
- A **Shard** is an independent Actor responsible for a subset of Topic-Partitions.
- The **ShardManager** routes incoming requests to the specific Shard owning the partition.
- This design ensures that requests for different partitions can proceed in parallel without locking shared resources.

## Usage

### Prerequisites
- Rust (latest stable)
- Linux (for Zero-Copy `sendfile` support)

### Building
```bash
cargo build --release
```

### Running the Server
```bash
# Start server on port 9093
cargo run --release -p logan-bin -- --bind 0.0.0.0:9093 --log-level info
```

### Using with kcat
```bash
# List Metadata
kcat -b localhost:9093 -L

# Produce Messages
echo "hello logan" | kcat -b localhost:9093 -t test-topic -P

# Consume Messages
kcat -b localhost:9093 -t test-topic -C -o beginning
```

### Running Tests
Logan has a comprehensive test suite including property-based tests for storage reliability.
```bash
cargo test
```

## Project Structure
- `logan-bin`: The main server executable.
- `logan-server`: Core server logic, networking, and request dispatch.
- `logan-storage`: Persistent storage engine (Logs, Segments, Indexes).
- `logan-protocol`: Kafka protocol definitions, encoding/decoding, and primitives.
- `logan-client`: Minimal async client client library.
- `logan-bench`: Performance benchmarking tool.
- `logan-common`: Shared utilities (logging, errors).
