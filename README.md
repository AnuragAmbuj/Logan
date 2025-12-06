# Logan - High-Performance Distributed Message Broker

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://github.com/AnuragAmbuj/logan/actions/workflows/rust.yml/badge.svg)](https://github.com/AnuragAmbuj/logan/actions)
[![Documentation](https://docs.rs/logan/badge.svg)](https://docs.rs/logan)

Logan is a cloud-native, distributed streaming platform implemented in Rust. It mimics the Kafka protocol to provide a high-throughput, low-latency, and durable message broker that is compatible with existing Kafka clients.

**⚠️ Status: Active Development (Pre-Alpha)**
Logan is currently in the early stages of development. While it supports basic production, consumption, and persistence, it is not yet feature-complete for production environments.

👉 **[View Implementation Plan & Roadmap](./PLAN.md)**

## Key Features (Current & Planned)

-   **Kafka Protocol Compatible**: Works with standard Kafka clients.
-   **High Performance**: Thread-per-core architecture using `tokio`.
-   **Durable Storage**: Disk-based persistence with sparse indexing.
-   **Zero Dependencies**: No Zookeeper required (Planned KRaft implementation).
-   **Written in Rust**: Memory safety and performance without garbage collection.

## Project Structure

-   `logan-bin`: The server binary.
-   `logan-server`: Network layer and request dispatching.
-   `logan-storage`: High-performance, append-only storage engine.
-   `logan-protocol`: Kafka wire protocol implementation.
-   `logan-client`: Async Rust client library.

## Getting Started

### Prerequisites
-   Rust (stable)

### Running the Broker

```bash
# Start the broker (defaults to port 9092, logs in /tmp/logan-logs)
cargo run --release -p logan-bin
```

To specify a custom log directory:
```bash
cargo run --release -p logan-bin -- --log-dir ./data/raft-logs
```

### Running Tests

```bash
# Run unit and integration tests
cargo test --workspace
```

## Roadmap

We are currently in **Phase 1: Foundation & Reliability**.

1.  **Prototype Phase** (Completed) ✅
    -   Basic networking, storage, and protocol parsing.
60: 2.  **Phase 1: Foundation** (In Progress) 🚧
61:     -   Data integrity check (CRC32), rigorous testing, and retention policies.
62: 3.  **Phase 2: Performance**
63:     -   Zero-copy networking, batching, and compression.
64: 4.  **Phase 3: Client & Protocol Expansion** (Completed) ✅
65:     -   Expanded protocol (DeleteTopics), implemented CLI client, and enhanced common utilities.
66: 5.  **Phase 4: Compatibility**
67:     -   Consumer groups, offset management, and verification with external clients.
68: 6.  **Phase 5: Clustering**
69:     -   Raft-based consensus and replication.

See [PLAN.md](./PLAN.md) for the detailed checklist.

## 🤝 Contributing

We welcome interest in the project! Please check the [Implementation Plan](./PLAN.md) to see where you can help.
You can also use AI to contribute to the project. Please do not expect to get paid for it.

In case you want to use AI to contribute to the project, please 

## License

MIT
