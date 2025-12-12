# Benchmark Report: Thread-per-Core Prototype (Glommio)

**Date**: 2025-12-12
**Architecture**: Thread-per-Core (Glommio), 1 Executor/Core
**Comparison Target**: Logan v0.1 (Tokio)

## Experiment Setup
- **Prototype**: `logan-tpc`, a minimal server using `glommio` crate.
- **Port**: 9093
- **Logic**: Accept connection, Read length-prefixed request, Parse `ProduceRequest` (Zero-Copy), Respond with dummy `ProduceResponse`.
- **Hardware**: Localhost (Dev Machine), 12 Cores.

## Results

| Metric | Tokio (Baseline) | Glommio (TPC Prototype) |
| :--- | :--- | :--- |
| **Throughput** | **771,100 msgs/sec** | *N/A (Unstable)* |
| **P99 Latency** | **1 us** | *N/A (Hung)* |
| **Complexity** | Low (Standard Async) | High (Manual Threading/Binding) |

## Observations & Issues
1.  **Complexity**: Setting up a TPC runtime required manual thread management and careful handling of `TcpListener` binding across cores.
2.  **Stability**: The prototype successfully bound to all cores but encountered network stalls/hangs during the high-concurrency benchmark (`logan-bench`). This suggests subtle issues with buffering, `read_exact` semantics, or cooperative scheduling starvation in the simple prototype.
3.  **Tokio Robustness**: The existing Tokio implementation worked out-of-the-box with massive throughput (~770k/s) and minimal latency.

## Conclusion
While TPC offers theoretical advantages for eliminating lock contention and context switching, achieving a stable implementation requires significant engineering effort (custom runtime tuning, specialized I/O handling). Given the exceptional performance of the current Tokio architecture for the current workload, **switching to a full TPC architecture strictly for network handling is premature.**

**Recommendation**:
- Stick with **Tokio** for the networking layer.
- Investigate **Core-Local Partitioning** (logic layer) on top of Tokio (e.g., using `tokio-uring` or sticking to standard Tokio with careful channel sharding) if lock contention becomes a bottleneck in the storage layer.
- The next optimization phase should focus on **Storage Engine efficiency** (Segment management, Indexing) rather than replacing the network runtime.
