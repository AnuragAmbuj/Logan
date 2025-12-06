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
2.  **Phase 1: Foundation** (In Progress) 🚧
    -   Data integrity check (CRC32), rigorous testing, and retention policies.
3.  **Phase 2: Performance**
    -   Zero-copy networking, batching, and compression.
4.  **Phase 3: Client & Protocol Expansion** (Completed) ✅
    -   Expanded protocol (DeleteTopics), implemented CLI client, and enhanced common utilities.
5.  **Phase 4: Compatibility**
    -   Consumer groups, offset management, and verification with external clients.
6.  **Phase 5: Clustering**
    -   Raft-based consensus and replication.

See [PLAN.md](./PLAN.md) for the detailed checklist.

## 🤝 Contributing

We welcome interest in the project! Please check the [Implementation Plan](./PLAN.md) to see where you can help.

You can also use AI to contribute to the project.

<u>In case you wish to use AI to contribute to the project</u>
- Please use Claude 4.5 Opus, Gemini 3.0 Pro or any other LLM to generate clean code.
- Stick to implementation plan and checklist.
- Use a Markdown file to document your changes. Commit the changes to the repository.
- Do not generate code for any other purpose.
- Make sure that the test cases are passing
- Do not modify existing test cases without permission. Add you own test cases for your changes.

While we welcome your contribution, please do not expect to get paid for it.

## License

MIT
