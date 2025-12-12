# Testing Strategy

## 1. Unit Testing
**Scope:** Individual functions and structs (e.g., `Index` lookups, `Protocol` encoding).

-   **Protocol Fuzzing:** Use `proptest` to generate random Kafka protocol frames and ensure `decode(encode(x)) == x`.
-   **Index Logic:** Verify binary search boundary conditions (empty index, single item, exact match, non-exact match).
-   **Record Fuzzing:** Use `proptest` to verify `Record` serialization and resilience to corruption (CRC32).

## 2. Integration Testing
**Scope:** Interaction between `logan-server`, `logan-storage` and `logan-client`.

-   **Persistence Tests:** (Like existing `persistence_test.rs`) Restart server, verify data integrity.
-   **Concurrent Access:** Multiple producers/consumers hitting the same partition.
-   **Error Handling:** Simulate disk full, permission denied, or malformed packets.

## 3. System / Black-box Testing
**Scope:** Running `logan-bin` as a black box.

-   **Kafka Compatibility:** Run the official Kafka client (Java/Python) against `logan-server`. If it breaks, our protocol implementation is wrong.
-   **Cluster Tests:** (Future) Spin up 3 nodes, kill leader, verify consumption continues.

## 4. Performance Testing
**Scope:** Latency and Throughput.

-   **Benchmarking:** Create a `bench` crate. Measure:
    -   Throughput (record/sec) vs. Concurrency.
    -   Tail latency (p99).
-   **Profile:** Use `flamegraph` to identify CPU hotspots (likely memory copying currently).

## 5. Planned Test Suite Improvements

| Priority | Test Type | Description |
| :--- | :--- | :--- |
| Done | Unit | Add `proptest` for `Index` and `LogSegment` logic. |
| High | Integration | Add `concurrent_produce_fetch` test. |
| Medium | System | Verify compatibility with a standard Python `kafka-python` client. |
