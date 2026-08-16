# Concurrent WAL Storage Engine

A concurrent, TCP-based key-value storage engine built in Go[cite: 7]. This project implements a custom binary protocol, write-ahead logging (WAL) for durability, and a sharded in-memory index for fast retrievals[cite: 7].

## System Architecture

The engine is decoupled into three primary phases of operation[cite: 7]:

### 1. Storage Engine & Indexing
*   **Sharded Map:** The in-memory index is divided into 64 independent shards (`NumShards`) using FNV-1a hashing to minimize read/write lock contention[cite: 7].
*   **False Sharing Prevention:** Each shard is padded with 64 bytes to ensure CPU cache line isolation[cite: 7].
*   **Crash Recovery:** On bootstrap, the engine reads `wal.log`, verifies the CRC32 checksum of every payload, and rebuilds the in-memory index offsets before accepting connections[cite: 7].

### 2. Networking & Routing
*   **Connection Pooling:** The server listens on TCP port 8080 and supports a hard limit of 10,000 concurrent connections via a pre-allocated ID pool[cite: 7].
*   **Memory Safety:** Each connection maintains a stateful scratchpad buffer that is strictly capped at 4MB (`MaxScratchpadSize`) to prevent greedy clients from causing memory overflows[cite: 7].
*   **Race-Free Routing:** `PUT` requests generate a unique `Transaction` object containing a dedicated `ackCh` channel[cite: 7]. This ensures disk worker acknowledgments are routed back to the exact requesting thread, eliminating connection ID recycling races[cite: 7].

### 3. Disk Worker & Group Commit
*   **Sequential WAL:** All write operations are appended sequentially to a single write-ahead log file[cite: 7].
*   **Group Commit:** To maximize disk throughput, physical `fsync` calls are batched[cite: 7]. The disk worker only flushes to disk when the network ingress queue is completely empty, or when the pending batch reaches 1,000 transactions (`GroupCommitBatchSize`)[cite: 7].

## Wire Protocol

Clients communicate via a strict binary protocol[cite: 7]. Every request requires an 11-byte header followed by the payload[cite: 7].

**Header Layout (11 Bytes):**
*   `[0-1]` Magic Bytes: `0xDEAD`[cite: 7]
*   `[2]` OpCode: `0` (PUT), `1` (GET), `2` (COMPACT)[cite: 7]
*   `[3-6]` Payload Length (Big-Endian `uint32`)[cite: 7]
*   `[7-10]` CRC32 Checksum of the Payload (Big-Endian `uint32`)[cite: 7]

**Payload Constraints:**
*   For `PUT` operations, the first 2 bytes of the payload must specify the Key Length (Big-Endian `uint16`), followed by the Key, and then the Value[cite: 7].
*   Malformed frames (e.g., Key Length exceeding total payload size) result in immediate connection termination[cite: 7].

## Known Limitations & Technical Constraints

Based on the current implementation, this engine has the following hard constraints:

*   **OOM Vulnerability during Compaction:** The background compaction routine (`OpCompact`), which triggers every 60 seconds, reads every active WAL entry into a single dynamically growing in-memory slice (`allEntries []WALEntry`)[cite: 7]. The maximum size of the database is therefore strictly capped by the host's available RAM before an Out-Of-Memory panic occurs[cite: 7].
*   **Garbage Collection Overhead:** The network router utilizes `time.After(ACKTimeout)` within a `select` block to manage transaction timeouts[cite: 7]. Under high throughput, this generates unexpired timer allocations per transaction, which will increase garbage collection pressure[cite: 7]. 

## Running the Server

```bash
# Compile the engine
go build -o wal-engine .

# Execute the binary
./wal-engine
