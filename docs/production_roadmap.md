# Production Roadmap: Project Logan

This roadmap outlines the path to transforming the current prototype into a production-grade, Kafka-compatible message broker.

## Phase 1: Storage & Performance Hardening
**Goal:** Ensure single-node reliability and high throughput.

### 1.1 Zero-Copy Networking
- **Current:** `rw.read` / `rw.write` with userspace buffers.
- **Target:** Use `sendfile` (via `tokio::fs` or `nix`) for network transfers to bypass userspace overhead.
- **Benefit:** Massive reduction in CPU usage and memory copies for consumers.

### 1.2 Message Batching & Compression
- **Current:** Single message handling (mostly).
- **Target:** Full support for `RecordBatch` consumption and production. Implement compression (Snappy, LZ4, Zstd).
- **Benefit:** Higher throughput, reduced network/disk IO.

### 1.3 Retention & Compaction
- **Current:** Append-only, infinite storage.
- **Target:** Time-based and size-based retention policies. Log compaction for key-value use cases.
- **Benefit:** Disk space management, support for state stores.

### 1.4 Durability Guarantees
- **Current:** `File::flush` on every append (slow) or implicit OS limit.
- **Target:** Configurable `fsync` policies (e.g., `flush_messages=1000` or `flush_ms=500`). CRC32 validation for records.
- **Benefit:** Balance between latency and durability. Data integrity checks.

## Phase 2: Core Kafka Primitives
**Goal:** Implementation of essential Kafka behaviors for client compatibility.

### 2.1 Consumer Groups (The Coordinator)
- **Current:** Simple `fetch` API.
- **Target:** Implement Group Coordinator, `JoinGroup`, `SyncGroup`, `Heartbeat` APIs. Offset management in `__consumer_offsets`.
- **Benefit:** Scalable consumption and load balancing.

### 2.2 Offset Management
- **Current:** Client tracks offsets manually.
- **Target:** Server-side offset commit/fetch APIs.
- **Benefit:** Resuming consumption after restarts/rebalancing.

### 2.3 Transactions
- **Current:** None.
- **Target:** Idempotent producers and transactional messaging (`EOS`).
- **Benefit:** Exactly-once semantics.

## Phase 3: Distributed Reliability (Clustering)
**Goal:** High availability (HA) and horizontal scaling.

### 3.1 Replication Protocol
- **Current:** Single node.
- **Target:** Leader/Follower replication scheme. ISR (In-Sync Replicas) tracking.
- **Benefit:** Fault tolerance.

### 3.2 Consensus (KRaft)
- **Current:** None.
- **Target:** Raft-based consensus for cluster metadata (controller election, topic config, broker registration).
- **Benefit:** Removing Zookeeper dependency (modern Kafka architecture).

## Phase 4: Operational Readiness
**Goal:** Observability and configuration for operators.

- **Metrics:** Expose Prometheus metrics (throughput, latency, lag, disk usage).
- **Configuration:** Hot-reloadable configs, extensive `server.properties` support.
- **Tooling:** Admin CLI tools for topic management, partition reassignment.

## Next Immediate Steps (Proposed)
1.  **Refactor Storage for Performance:** Implement proper RecordBatch layout and CRC validation.
2.  **Testing Infrastructure:** Set up rigorous property-based tests for the storage engine.
