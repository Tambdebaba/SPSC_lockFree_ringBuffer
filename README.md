# Concurrent WAL Storage Engine

An educational concurrent key-value storage engine built in Go. This project explores low-level database primitives, focusing on write-ahead logging (WAL) for durability, custom TCP wire protocols, and thread-safe concurrency control[cite: 5, 6]. 

The primary engineering goal of this project was to identify and eliminate race conditions (such as connection ID recycling and lock contention) commonly found in highly concurrent network services.

## Architecture

The system is divided into three decoupled phases to isolate network ingress from physical disk I/O[cite: 5, 6]:

### 1. Networking & Routing (TCP)
*   **Custom Binary Protocol:** Clients communicate via a strict 11-byte header (`[MagicBytes 2B][OpCode 1B][Length 4B][CRC32 4B]`) followed by a variable-length payload[cite: 5, 6].
*   **Connection State Machine:** Each TCP connection maintains its own scratchpad buffer to safely parse headers and payloads while preventing overflow (`MaxScratchpadSize = 4MB`)[cite: 6].
*   **Per-Transaction Channels:** To prevent ID recycling races, every `PUT` operation generates a unique transaction object with a dedicated `ackCh` channel to route the disk worker's group commit acknowledgment back to the correct client thread[cite: 5, 6].

### 2. Disk Worker & Group Commit
*   **Write-Ahead Log (WAL):** All `PUT` transactions are sequentially appended to a WAL file (`wal.log`)[cite: 6].
*   **Group Commit Batching:** The disk worker synchronizes physical writes to disk (`file.Sync()`) only when the network ingress queue is empty, or when the batch size reaches 1,000 transactions (`GroupCommitBatchSize`)[cite: 6]. 

### 3. Storage Engine & Indexing
*   **Sharded In-Memory Index:** Key offsets and sizes are stored in a 64-shard map (`ShardedIndex`) hashed via FNV-1a[cite: 5, 6]. Each shard utilizes independent read/write mutexes and 64-byte padding to eliminate false sharing[cite: 6].
*   **Crash Recovery:** On startup, the engine sequentially scans the WAL, verifies CRC32 checksums, and rebuilds the in-memory index[cite: 5, 6].

## Concurrency Fixes Implemented

This iteration of the engine resolved several critical race conditions:
1.  **ID Recycling Race:** Replaced global reverse-routing arrays with per-transaction `chan struct{}` channels, ensuring a new connection reusing an ID never receives a disk ACK meant for a closed connection[cite: 5, 6].
2.  **Goroutine Leaks:** Implemented strict 5-second `ACKTimeout` timeouts using `select` blocks in the network router to prevent stalled disk workers from permanently locking network threads[cite: 6].
3.  **Compaction Lock Contention:** Separated background compaction into phases. Read locks are acquired only to map active offsets, completely decoupling the slow temp-file I/O from the mutexes blocking network reads[cite: 6].

