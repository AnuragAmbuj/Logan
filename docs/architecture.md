# Architecture Documentation

## System Overview

Logan is a distributed, Kafka-compatible message broker written in Rust. It follows a thread-per-core (or async task-based) architecture using Tokio for asynchronous I/O.

## Core Components

### 1. Network Layer (`logan-server`)
-   **Server**: Manages the TCP listener and accepts incoming connections. spawns a dedicated Tokio task for each connection.
-   **Dispatch**: Decodes Kafka protocol primitives (`RequestHeader`, `ApiKey`) and routes requests to specific handlers. Use `logan-protocol` for serialization/deserialization.

### 2. Protocol Layer (`logan-protocol`)
-   Defines Rust structs for Kafka wire protocol messages (`ProduceRequest`, `FetchResponse`, etc.).
-   Implements `Encodable` and `Decodable` traits for primitives (`KafkaString`, `KafkaArray`, `i32`, etc.).

### 3. Storage Engine (`logan-storage`)
The storage engine provides persistent, append-only logs for topic partitions.

#### **Directory Structure**
```
/log_dir/
  ├── topic-0/
  │   ├── 00000000000000000000.log    # Message data
  │   └── 00000000000000000000.index  # Offset -> Position mapping
  └── topic-1/
```

#### **Components**
-   **`LogManager`**: Singleton that manages all `Log` instances. Handles directory scanning on startup.
-   **`Log`**: Represents a partition. Contains an ordered list of `LogSegment`.
-   **`LogSegment`**: A specific slice of the log (e.g., offsets 0-1000). Rolling is largely TODO but architectural hooks exist.
    -   `.log` file: Append-only storage of raw bytes.
        -   **Format**: Series of `Record` structures.
        -   `Record`: `[Length: 4 bytes (BE)] [CRC32: 4 bytes (BE)] [Payload: N bytes]`
    -   `.index` file: Sparse index mapping logical offsets to physical file headers (~4KB intervals usually, currently 1:1 for prototype).

### 4. Client Library (`logan-client`)
-   A thin async wrapper around the TCP protocol.
-   Provides high-level methods: `produce`, `fetch`, `create_topics`.
-   Manages correlation IDs and request framing.

## Data Flow (Produce)

1.  Client sends `ProduceRequest`.
2.  `Server` task reads length prefix + bytes.
3.  `Dispatch` decodes header -> `ApiKey::Produce`.
4.  `LogManager` locates the `Log` for `(topic, partition)`.
5.  `Log` appends data to the active `LogSegment`.
    -   Writes to `.log` file.
    -   Updates `.index` file.
    -   (Currently) flushes to disk.
6.  `Server` returns `ProduceResponse` with base offset.

## Data Flow (Fetch)

1.  Client sends `FetchRequest` (Topic, Partition, Offset).
2.  `LogManager` locates `Log`.
3.  `Log` performs binary search on `Index` to find physical position.
4.  `LogSegment` identifies file slice (offset, length).
5.  `Server` uses `sendfile` (via `tokio::io::copy`) to stream data directly from disk to socket (Zero-Copy).

