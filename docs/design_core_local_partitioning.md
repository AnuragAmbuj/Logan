# Design: Core-Local Partitioning (Actor Model on Tokio)

**Objective**: Eliminate lock contention on `Log` state by assigning each partition to a dedicated "Shard" (actor) that processes requests sequentially.

## Problem
Currently, `LogManager` holds a `HashMap<TopicPartition, Arc<Mutex<Log>>>`.
- **Contention**: Every produce/fetch operation locks the specific `Log`.
- **Overhead**: Frequent `lock()/unlock()` calls even for uncontended logs slightly pollutes cache.
- **Future Risk**: As we add complex features (tiering, compaction), critical sections grow, increasing contention risk.

## Proposed Solution: Sharded Actor Model
Instead of shared state (`Mutex`), we use **Message Passing**.

### 1. The Shard
A `Shard` is a background Tokio task (Acting as a specialized thread/worker) that strictly owns a subset of partitions.
- **State**: `HashMap<TopicPartition, Log>` (No Mutex!)
- **Input**: `mpsc::Receiver<ShardCommand>`
- **Logic**: A loop that pops commands and executes them against the local logs.

### 2. Shard Assignment
- **Strategy**: Static Hashing. `shard_id = hash(topic, partition) % num_shards`.
- **Num Shards**: Defaults to `num_cpus`.

### 3. Request Flow
1.  **Network Layer**: Accepts connection, parses `ProduceRequest`.
2.  **Dispatcher**:
    *   Calculates `shard_id` for the target partition.
    *   Constructs a `ShardCommand::Append { data, response_tx }`.
    *   Sends command to `Shard[shard_id]`'s channel.
3.  **Shard**:
    *   Receives `Append` command.
    *   Writes to `Log`.
    *   Sends `Result` back via `response_tx`.
4.  **Network Layer**: Awaits `response_tx` and sends response to client.

## Components

### `ShardManager`
Replaces `LogManager`.
- holds `Vec<mpsc::Sender<ShardCommand>>`.
- Method `get_shard(topic, partition) -> Sender`.

### `ShardCommand` Enum
```rust
enum ShardCommand {
    Append {
        topic_partition: TopicPartition,
        record_batch: RecordBatch,
        resp_tx: oneshot::Sender<Result<LogAppendInfo>>,
    },
    Fetch {
        topic_partition: TopicPartition,
        offset: u64,
        resp_tx: oneshot::Sender<Result<FetchResult>>,
    },
    // Management commands...
    CreatePartition { ... }
}
```

## Pros/Cons
- **Pros**: 
    - **Zero Contention**: No mutexes on the hot path (Produce/Fetch).
    - **Batching**: A Shard can easily batch multiple append requests for the same partition from its queue before fsyncing (Group Commit).
    - **Simplicity**: Single-threaded logic per partition is easier to reason about (no deadlocks).
- **Cons**:
    - **Channel Overhead**: `mpsc` operations have cost, but usually less than contended mutex.
    - **Latency**: Hop to another thread (context switch) if the Shard task is not on the same core. (Tokio scheduler handles this well, but "Thread-Local" efficiency requires more advanced pinning which we defer).

## Implementation Roadmap
1.  Define `ShardCommand` and `Shard` struct.
2.  Implement `Shard` loop.
3.  Refactor `LogManager` into `ShardManager`.
4.  Update `Dispatch` to use channels instead of locks.
