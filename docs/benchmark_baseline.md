# Benchmark Baseline (Logan v0.1)

**Date**: 2025-12-12
**Architecture**: Tokio (Work-Stealing), Generic `TcpStream`, `Arc<Mutex<Log>>`

## Configuration
- **Hardware**: Localhost (Dev Machine)
- **Message Size**: 100 Bytes
- **Count**: 100,000 Messages
- **Concurrency**: 16 Threads
- **Persistence**: Enabled (No explicit fsync per message, relies on OS cache)

## Results

```
Elapsed: 129.68ms
Throughput: 771,100.09 msgs/sec
Min Latency: 0 us
Mean Latency: 1.86 us
P50 Latency: 0 us
P99 Latency: 1 us
P99.9 Latency: 4 us
Max Latency: 10,687 us
```

## Analysis
- **Throughput**: Excellent (~770k/s). This indicates that for small messages, the overhead of Tokio task scheduling and simple `Mutex` locking is minimal when running continuously.
- **Latency**: The P99 of 1us is suspicious. It likely represents the time to write to the kernel socket buffer and receive an ack that was also buffered. Since `logan-server` does not force `fsync` on every produce, this measures "network" + "memory copy" latency.
- **Jitter**: The max latency of ~10ms suggests occasional garbage collection or OS scheduler pauses, which is typical for managed runtimes or work-stealing jitter.

## Next Steps
- Compare against `glommio` (Thread-per-Core) to see if P99.9 latency and max latency can be stabilized.
- Implement `fsync` checks to measure "durable" write performance.
