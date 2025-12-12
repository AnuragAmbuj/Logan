# Implementation Plan

This document outlines the detailed plan to evolve Logan from a prototype into a production-grade, distributed message broker.

## ✅ Phase 0: Prototype (Completed)
- [x] **Core Protocol**: Basic `Produce` and `Fetch` request/response handling.
- [x] **Networking**: Async TCP server using Tokio.
- [x] **Storage Engine**:
    - [x] Append-only log segments.
    - [x] Sparse indexing (offset -> position).
    - [x] `LogManager` for multi-partition support.
- [x] **Integration**: Server writes to and reads from disk.
- [x] **Client**: Basic async client for testing.

---

## 🏗 Phase 1: Foundation & Reliability
**Goal:** harden the single-node storage engine and ensure data integrity.

### 1.1 Robust Testing
- [x] **Property-Based Tests**: Verify `Index` and `LogSegment` correctness with randomized inputs (`proptest`).
- [ ] **Fuzz Testing**: Fuzz the protocol decoder to ensure resilience against malformed packets.
- [x] **Integration Tests**: Simulate crash-recovery loops (Partially covered by `persistence_test`).

### 1.2 Data Integrity
- [x] **CRC32 Validation**:
    - [x] Add CRC32 checksum to record headers.
    - [x] Validate checksum on read.
    - [x] Implement a "background scrubber" thread (Included in retention cleanup).
- [ ] **Fsync Policies**:
    - [ ] Implement configurable flush policies (`flush.messages`, `flush.ms`).

### 1.3 Storage Features
- [x] **Retention Policies**:
    - [x] Implement time-based retention (`log.retention.hours`).
    - [x] Implement size-based retention (`log.retention.bytes`).
    - [x] Implement segment deletion scheduler.
- [ ] **Compaction**:
    - [ ] Implement key-based log compaction for state stores.

---

## 🚀 Phase 2: Performance
**Goal:** Optimize throughput and latency for high-volume workloads.

### 2.1 Zero-Copy Networking
- [x] **Sendfile**: Use `tokio::fs` or `nix::sys::sendfile` to transfer data directly from page cache to socket.
- [ ] **Buffer Management**: Minimize allocations during request parsing.

### 2.2 Batching & Compression
- [ ] **Record Batching**: Full support for Kafka's `RecordBatch` format.
- [ ] **Compression**:
    - [ ] Snappy
    - [ ] LZ4
    - [ ] Zstd

### 2.3 Advanced Architecture (NUMA/Thread-per-Core)
**Note**: This involves a significant architectural shift from "Work Stealing" (Tokio) to "Thread-per-Core" (e.g., `glommio`/`monoio`) similar to Redpanda/Seastar.
- [ ] **Architecture Study**: Evaluate `glommio` vs `tokio-uring` vs standard `tokio` with pinning.
- [ ] **Core Sharding**: Shard `LogManager` so partitions are owned by specific cores (Shared-Nothing).
- [ ] **Message Passing**: Implement inter-core channels for cross-partition requests to replace Mutexes.
- [ ] **Thread Pinning**: Pin runtime threads to physical cores to maximize L1/L2 cache hits and reduce context switches.

---

## 🧩 Phase 3: Kafka Compatibility
**Goal:** Support enough of the Kafka protocol to work with standard clients.

### 3.1 Consumer Groups
- [ ] **Group Coordinator**: Manage consumer group state.
- [ ] **Protocols**:
    - [ ] `JoinGroup`
    - [ ] `SyncGroup`
    - [ ] `Heartbeat`
    - [ ] `LeaveGroup`
- [ ] **Offset Storage**: Implement `__consumer_offsets` topic.

### 3.2 Metadata & Admin
- [ ] **Topic Management**: `CreateTopics`, `DeleteTopics` APIs.
- [ ] **Metadata**: dynamic metadata updates (not just static startup config).

---

## 🌐 Phase 4: Distributed Clustering (KRaft)
**Goal:** Horizontal scaling and fault tolerance without Zookeeper.

### 4.1 Consensus
- [ ] **Raft Implementation**: Implement Raft for cluster metadata consensus.
- [ ] **Controller**: Implement the KRaft controller logic.

### 4.2 Replication
- [ ] **Replica Fetcher**: Follower brokers fetching from leaders.
- [ ] **ISR Tracking**: In-Sync Replicas management.
- [ ] **Leader Election**: Automated failover.

---

## 🛠 Operational Readiness
- [ ] **Metrics**: Expose Prometheus metrics endpoint.
- [ ] **Configuration**: Hot-reloadable `server.properties`.
- [ ] **Tracing**: Distributed tracing integration (OpenTelemetry).
