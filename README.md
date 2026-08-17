# Go WAL Key-Value Engine

A high-performance, persistent, and concurrent key-value storage engine written in Go. Built around a Write-Ahead Log (WAL) architecture, this system features a custom binary TCP protocol, sharded in-memory indexing, and group commit mechanisms to maximize throughput while guaranteeing data durability.

## Core Features

*   **Write-Ahead Log (WAL) Durability**: All mutations are appended sequentially to disk, ensuring strict durability and `O(1)` write performance.
*   **Group Commit**: Automatically batches concurrent writes into a single `fsync` operation to maximize I/O throughput and minimize disk seeks.
*   **Sharded In-Memory Index**: 64-way sharded index utilizing FNV-1a hashing eliminates lock contention during highly concurrent read/write workloads.
*   **Custom Binary Protocol**: Lightweight, 11-byte TCP framing with CRC32 payload checksumming for end-to-end network and disk corruption detection.
*   **Crash Recovery**: Deterministically rebuilds the in-memory index on startup by replaying the WAL.
*   **Lock-Free Compaction**: Background worker reclaims disk space by purging stale records and safely swapping WAL files without blocking active read streams.
*   **Connection State Management**: Pre-allocated connection IDs, bounded scratchpads, and connection pooling minimize GC pressure under peak load.

## Architecture & Data Flow

The engine is divided into three primary subsystems:

1.  **Network & Routing (Phase 2)**
    *   Listens on TCP `:8080`.
    *   Manages connection state using a scratchpad buffer to prevent partial reads from stalling the pipeline.
    *   Parses the 11-byte header and routes operations (`OpPut`, `OpGet`, `OpCompact`).
2.  **Storage Engine & Indexing (Phase 1)**
    *   Maintains the `ShardedIndex` mapping string keys to `(Offset, Size)` tuples.
    *   Reads values directly from the WAL using concurrent `ReadAt` calls (bypassing global locks).
3.  **Disk Worker & Recovery (Phase 3)**
    *   A single, dedicated goroutine handles all `append` operations to the WAL to prevent interleaved writes.
    *   Implements the group commit logic (configurable batch size and flush intervals).
    *   Orchestrates background compaction and file rotation.

## Binary Protocol Specification

All communication occurs over a custom TCP binary framing protocol. 

### Frame Format

| Offset | Length (Bytes) | Field       | Type   | Description                                           |
| ------ | -------------- | ----------- | ------ | ----------------------------------------------------- |
| 0      | 2              | Magic Bytes | uint16 | Must be `0xDEAD`. Detects stream misalignment.        |
| 2      | 1              | OpCode      | byte   | `0` = PUT, `1` = GET, `2` = COMPACT                   |
| 3      | 4              | Length      | uint32 | Total length of the trailing payload.                 |
| 7      | 4              | Checksum    | uint32 | CRC32 IEEE checksum of the payload.                   |
| 11     | `Length`       | Payload     | bytes  | Payload data (structure depends on OpCode).           |

### Payload Structures

*   **PUT (`OpCode: 0`)**: `[Key Length (uint16)] [Key (bytes)] [Value (bytes)]`
*   **GET (`OpCode: 1`)**: `[Key (bytes)]`
*   **COMPACT (`OpCode: 2`)**: Empty payload.

## Getting Started

### Starting the Server

```bash
go run .
```
The server will initialize the `wal.log` file, recover any existing index entries, and bind to `0.0.0.0:8080`.

### Client Interaction Example

Below is a minimal Go client demonstrating how to construct a PUT frame and read the engine's acknowledgment.

```go
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 1. Prepare Payload
	key := "my_key"
	val := []byte("hello_production")
	
	keyLen := uint16(len(key))
	payload := make([]byte, 2+len(key)+len(val))
	binary.BigEndian.PutUint16(payload[0:2], keyLen)
	copy(payload[2:], key)
	copy(payload[2+keyLen:], val)

	// 2. Prepare Header
	header := make([]byte, 11)
	binary.BigEndian.PutUint16(header[0:2], 0xDEAD) 
	header[2] = 0 // OpPut
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[7:11], crc32.ChecksumIEEE(payload))

	// 3. Send Frame
	conn.Write(append(header, payload...))

	// 4. Read Response
	respBuf := make([]byte, 1024)
	n, _ := conn.Read(respBuf)
	fmt.Printf("Server Response: %s", string(respBuf[:n])) // Expected: "200 OK: Disk Commit Verified"
}
```

## Testing & Benchmarks

The project includes an extensive test suite verifying concurrent loads, checksum validation, disk reclamation, and recovery semantics.

Run tests:
```bash
go test -v ./...
```

Run benchmarks:
```bash
go test -bench=. -benchmem
```

*Benchmarks include single-threaded throughput, GET latency, realistic concurrent Put/Get workloads, group commit efficiency, index shard contention, and memory allocation overhead.*
