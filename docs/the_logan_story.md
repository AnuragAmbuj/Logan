# The Logan Story: A Journey to Production

Logan is not just a message broker; it's a journey into the depths of distributed systems engineering. This document chronicles our architectural evolution, explaining the "Why" and "How" behind every major decision.

We have broken down this journey into detailed steps:

## [Chapter 1: The Foundation - Storage Engine](story/01_storage_engine.md)
How we moved from standard databases to an indexed Append-Only Log architecture to achieve raw sequential throughput.

## [Chapter 2: Trust No One - Data Integrity](story/02_data_integrity.md)
Fighting bit rot and network corruption with CRC32 checksums embedded directly into the disk format.

## [Chapter 3: The Need for Speed - Zero-Copy Networking](story/03_zero_copy_networking.md)
Bypassing the CPU and context switches using `sendfile` to saturate 10Gbps networks.

## [Chapter 4: Compressing Time and Space](story/04_compression.md)
Solving the small-packet overhead problem with Batching and using Snappy/LZ4 to trade CPU for Bandwidth.

## [Chapter 5: Cleaning House - Log Compaction](story/05_log_compaction.md)
Transforming the event log into a persistent Key-Value store by rewriting history to keep only the latest state.
