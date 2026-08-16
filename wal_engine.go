package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	MagicBytes               uint16 = 0xDEAD
	OpPut                    byte   = 0
	OpGet                    byte   = 1
	OpCompact                byte   = 2
	MaxConnections                  = 10000
	StateReadingHeader              = 0
	StateReadingPayload             = 1
	MaxScratchpadSize               = 4 * 1024 * 1024
	NumShards                       = 64
	ShardMask                       = NumShards - 1
	ReadDeadlinePerConn             = 30 * time.Second
	ACKTimeout                      = 5 * time.Second
	GroupCommitBatchSize            = 1000
	GroupCommitFlushInterval        = 100 * time.Millisecond
)

// ============================================================================
// PHASE 1: STORAGE ENGINE & INDEXING
// ============================================================================

type IndexRecord struct {
	Offset uint64
	Size   uint32
}

type IndexShard struct {
	mu sync.RWMutex
	m  map[string]IndexRecord
	_  [64]byte // Padding to prevent false sharing
}

type ShardedIndex struct {
	shards [NumShards]*IndexShard
}

func NewShardedIndex() *ShardedIndex {
	si := &ShardedIndex{}
	for i := 0; i < NumShards; i++ {
		si.shards[i] = &IndexShard{m: make(map[string]IndexRecord)}
	}
	return si
}

func (si *ShardedIndex) getShard(key string) *IndexShard {
	h := fnv.New64a()
	h.Write([]byte(key))
	return si.shards[h.Sum64()&ShardMask]
}

func (si *ShardedIndex) Put(key string, offset uint64, size uint32) {
	shard := si.getShard(key)
	shard.mu.Lock()
	shard.m[key] = IndexRecord{Offset: offset, Size: size}
	shard.mu.Unlock()
}

func (si *ShardedIndex) Get(key string) (IndexRecord, bool) {
	shard := si.getShard(key)
	shard.mu.RLock()
	record, exists := shard.m[key]
	shard.mu.RUnlock()
	return record, exists
}

type StorageEngine struct {
	mu      sync.RWMutex
	walFile *os.File
	index   *ShardedIndex
	dbPath  string
}

func NewStorageEngine(dbPath string, index *ShardedIndex) (*StorageEngine, error) {
	walFile, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &StorageEngine{
		walFile: walFile,
		index:   index,
		dbPath:  dbPath,
	}, nil
}

func (e *StorageEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.walFile != nil {
		return e.walFile.Close()
	}
	return nil
}

func (se *StorageEngine) GetValue(key string) ([]byte, error) {
	record, exists := se.index.Get(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	buf := make([]byte, 11+record.Size)

	se.mu.RLock()
	_, err := se.walFile.ReadAt(buf, int64(record.Offset))
	se.mu.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("failed to read from WAL: %w", err)
	}

	if binary.BigEndian.Uint16(buf[0:2]) != MagicBytes || buf[2] != OpPut {
		return nil, fmt.Errorf("corrupt data: invalid magic bytes or opcode")
	}

	expectedChecksum := binary.BigEndian.Uint32(buf[7:11])
	payload := buf[11:]

	if crc32.ChecksumIEEE(payload) != expectedChecksum {
		return nil, fmt.Errorf("corrupt checksum")
	}

	keyLen := binary.BigEndian.Uint16(payload[0:2])
	if int(keyLen) > len(payload)-2 {
		return nil, fmt.Errorf("corrupt data: key length exceeds payload")
	}

	return payload[2+keyLen:], nil
}

// ============================================================================
// PHASE 2: NETWORKING & ROUTING
// ============================================================================

type Transaction struct {
	ConnID  uint32
	OpCode  byte
	Key     string
	Payload []byte
	ackCh   chan struct{}
}

type ConnectionState struct {
	conn           net.Conn
	connID         uint32
	scratchpad     []byte
	currentState   int
	expectedLength uint32
	_              [64]byte
}

var (
	idPool chan uint32
)

func (c *ConnectionState) HandleConnection(ingressChan chan Transaction, engine *StorageEngine) {
	defer func() {
		c.conn.Close()
		idPool <- c.connID
	}()

	readBuf := make([]byte, 1024)

	// Set deadline once at connection open
	c.conn.SetReadDeadline(time.Now().Add(ReadDeadlinePerConn))

	for {
		n, err := c.conn.Read(readBuf)
		if err != nil {
			return
		}

		if len(c.scratchpad)+n > MaxScratchpadSize {
			fmt.Printf("[Network] Scratchpad overflow on conn %d\n", c.connID)
			return
		}

		c.scratchpad = append(c.scratchpad, readBuf[:n]...)
		c.processScratchpad(ingressChan, engine)
	}
}

func (c *ConnectionState) processScratchpad(ingressChan chan Transaction, engine *StorageEngine) {
	for {
		if c.currentState == StateReadingHeader {
			if len(c.scratchpad) < 11 {
				return
			}
			if binary.BigEndian.Uint16(c.scratchpad[0:2]) != MagicBytes {
				fmt.Printf("[Network] Invalid magic bytes on conn %d\n", c.connID)
				c.conn.Close()
				return
			}
			c.expectedLength = binary.BigEndian.Uint32(c.scratchpad[3:7])
			c.currentState = StateReadingPayload
		}

		if c.currentState == StateReadingPayload {
			totalRequired := 11 + int(c.expectedLength)
			if len(c.scratchpad) < totalRequired {
				return
			}

			opCode := c.scratchpad[2]
			payload := c.scratchpad[11:totalRequired]

			// Handle GET operations
			if opCode == OpGet {
				val, err := engine.GetValue(string(payload))
				if err == nil {
					c.conn.Write(append([]byte("OK: "), val...))
				} else {
					c.conn.Write([]byte("ERR: NOT FOUND\n"))
				}
				c.scratchpad = c.scratchpad[totalRequired:]
				c.currentState = StateReadingHeader
				continue
			}

			// Handle PUT operations
			if opCode == OpPut {
				keyLen := binary.BigEndian.Uint16(payload[0:2])
				if int(keyLen) > len(payload)-2 {
					fmt.Printf("[Network] Corrupt frame: key length exceeds payload on conn %d\n", c.connID)
					c.conn.Close()
					return
				}

				ackCh := make(chan struct{}, 1)
				tx := Transaction{
					ConnID:  c.connID,
					OpCode:  OpPut,
					Key:     string(payload[2 : 2+keyLen]),
					Payload: append([]byte(nil), payload...),
					ackCh:   ackCh,
				}

				ingressChan <- tx

				c.scratchpad = c.scratchpad[totalRequired:]
				c.currentState = StateReadingHeader

				// Wait for ACK with timeout to prevent goroutine leak
				select {
				case <-ackCh:
					c.conn.Write([]byte("200 OK: Disk Commit Verified\n"))
				case <-time.After(ACKTimeout):
					fmt.Printf("[Network] ACK timeout on conn %d\n", c.connID)
					c.conn.Write([]byte("500 TIMEOUT\n"))
					c.conn.Close()
					return
				}
				continue
			}

			// Handle COMPACT operations
			if opCode == OpCompact {
				ingressChan <- Transaction{OpCode: OpCompact}
				c.scratchpad = c.scratchpad[totalRequired:]
				c.currentState = StateReadingHeader
				continue
			}

			fmt.Printf("[Network] Unknown opcode %d on conn %d\n", opCode, c.connID)
			c.conn.Close()
			return
		}
	}
}

// ============================================================================
// PHASE 3: DISK WORKER, RECOVERY, & COMPACTION
// ============================================================================

type TxMeta struct {
	ConnID uint32
	Key    string
	Offset uint64
	Size   uint32
	ackCh  chan struct{}
}

type WALEntry struct {
	Key    string
	Offset uint64
	Size   uint32
	Data   []byte
}

func StartDiskWorker(ingressChan <-chan Transaction, engine *StorageEngine, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	file, err := os.OpenFile(engine.dbPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("disk worker: failed to open WAL file: %v", err))
	}
	defer close(doneCh)
	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	headerBuffer := make([]byte, 11)
	var processedBatch []TxMeta
	var currentOffset uint64

	info, err := file.Stat()
	if err != nil {
		panic(fmt.Sprintf("disk worker: failed to stat file: %v", err))
	}
	currentOffset = uint64(info.Size())

	var groupCommitMu sync.Mutex
	batchSize := 0

	for {
		select {
		case <-stopCh:
			return

		case tx := <-ingressChan:
			// Handle compaction request
			if tx.OpCode == OpCompact {
				if err := file.Sync(); err != nil {
					panic(fmt.Sprintf("disk worker: sync before compaction failed: %v", err))
				}
				
				// Close BEFORE compaction to avoid locking Windows
				file.Close()
				
				currentOffset = compactWAL(engine)
				
				file, err = os.OpenFile(engine.dbPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				if err != nil {
					panic(fmt.Sprintf("disk worker: reopen after compaction failed: %v", err))
				}
				batchSize = 0
				continue
			}

			// Handle regular transaction (PUT)
			payloadLength := uint32(len(tx.Payload))

			binary.BigEndian.PutUint16(headerBuffer[0:2], MagicBytes)
			headerBuffer[2] = OpPut
			binary.BigEndian.PutUint32(headerBuffer[3:7], payloadLength)
			binary.BigEndian.PutUint32(headerBuffer[7:11], crc32.ChecksumIEEE(tx.Payload))

			// Write header
			n, err := file.Write(headerBuffer)
			if err != nil || n != 11 {
				panic(fmt.Sprintf("disk worker: WAL header write failed: %v", err))
			}

			// Write payload
			n, err = file.Write(tx.Payload)
			if err != nil || n != int(payloadLength) {
				panic(fmt.Sprintf("disk worker: WAL payload write failed: %v", err))
			}

			processedBatch = append(processedBatch, TxMeta{
				ConnID: tx.ConnID,
				Key:    tx.Key,
				Offset: currentOffset,
				Size:   payloadLength,
				ackCh:  tx.ackCh,
			})

			currentOffset += 11 + uint64(payloadLength)
			batchSize++

			// Decision to sync: group commit logic
			groupCommitMu.Lock()
			shouldSync := len(ingressChan) == 0 || batchSize >= GroupCommitBatchSize
			groupCommitMu.Unlock()

			if shouldSync {
				if err := file.Sync(); err != nil {
					panic(fmt.Sprintf("disk worker: WAL sync failed: %v", err))
				}

				// Update index and send ACKs
				for _, meta := range processedBatch {
					engine.index.Put(meta.Key, meta.Offset, meta.Size)
					select {
					case meta.ackCh <- struct{}{}:
					default:
						// Receiver closed channel or timed out
					}
				}

				processedBatch = processedBatch[:0]
				batchSize = 0
			}
		}
	}
}

// compactWAL reclaims space from the WAL by rewriting only active entries
func compactWAL(engine *StorageEngine) uint64 {
	fmt.Println("[Compaction] Starting background compaction...")

	tmpPath := engine.dbPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(fmt.Sprintf("compaction: failed to create temp file: %v", err))
	}

	engine.mu.RLock()
	oldFile, err := os.Open(engine.dbPath)
	engine.mu.RUnlock()
	if err != nil {
		panic(fmt.Sprintf("compaction: failed to open old WAL: %v", err))
	}

	// STEP 1: Collect all entries WITHOUT holding locks
	var allEntries []WALEntry

	for i := 0; i < NumShards; i++ {
		shard := engine.index.shards[i]
		shard.mu.RLock()
		for key, record := range shard.m {
			data := make([]byte, record.Size)
			_, err := oldFile.ReadAt(data, int64(record.Offset)+11)
			if err != nil {
				fmt.Printf("[Compaction] Warning: failed to read entry %s: %v\n", key, err)
				continue
			}
			allEntries = append(allEntries, WALEntry{
				Key:    key,
				Offset: record.Offset,
				Size:   record.Size,
				Data:   data,
			})
		}
		shard.mu.RUnlock()
	}
	
	// Release read handle immediately to unblock Windows rename
	oldFile.Close()

	// STEP 2: Write to temp file WITHOUT any locks
	var newOffset uint64 = 0
	headerBuf := make([]byte, 11)
	offsetMap := make(map[string]uint64) // Track new offsets for updating index

	for _, entry := range allEntries {
		binary.BigEndian.PutUint16(headerBuf[0:2], MagicBytes)
		headerBuf[2] = OpPut
		binary.BigEndian.PutUint32(headerBuf[3:7], entry.Size)
		binary.BigEndian.PutUint32(headerBuf[7:11], crc32.ChecksumIEEE(entry.Data))

		if err := func() error {
			n, err := tmpFile.Write(headerBuf)
			if err != nil || n != 11 {
				return fmt.Errorf("header write failed: %w", err)
			}
			n, err = tmpFile.Write(entry.Data)
			if err != nil || n != int(entry.Size) {
				return fmt.Errorf("data write failed: %w", err)
			}
			return nil
		}(); err != nil {
			panic(fmt.Sprintf("compaction: failed to write entry: %v", err))
		}

		offsetMap[entry.Key] = newOffset
		newOffset += 11 + uint64(entry.Size)
	}

	if err := tmpFile.Sync(); err != nil {
		panic(fmt.Sprintf("compaction: temp file sync failed: %v", err))
	}
	
	// Release write handle immediately to unblock Windows rename
	tmpFile.Close()

	// STEP 3 & 4: Atomic file swap and index update
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if err := engine.walFile.Close(); err != nil {
		panic(fmt.Sprintf("compaction: failed to close old WAL: %v", err))
	}

	if err := os.Rename(tmpPath, engine.dbPath); err != nil {
		panic(fmt.Sprintf("compaction: rename failed: %v", err))
	}

	newWALFile, err := os.OpenFile(engine.dbPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("compaction: failed to reopen WAL: %v", err))
	}
	engine.walFile = newWALFile

	// Update index ONLY AFTER file is swapped, holding the same lock
	for i := 0; i < NumShards; i++ {
		shard := engine.index.shards[i]
		shard.mu.Lock()
		for key, record := range shard.m {
			if newOffsetVal, exists := offsetMap[key]; exists {
				shard.m[key] = IndexRecord{Offset: newOffsetVal, Size: record.Size}
			}
		}
		shard.mu.Unlock()
	}

	fmt.Printf("[Compaction] Complete. Entries: %d, New WAL size: %d bytes\n", len(allEntries), newOffset)
	return newOffset
}

// RecoverIndex rebuilds the in-memory index from the WAL on startup
func RecoverIndex(filepath string, index *ShardedIndex) {
	file, err := os.Open(filepath)
	if os.IsNotExist(err) {
		fmt.Println("[Recovery] No WAL file found, starting fresh")
		return
	}
	if err != nil {
		panic(fmt.Sprintf("recovery: failed to open WAL: %v", err))
	}
	defer file.Close()

	headerBuf := make([]byte, 11)
	var currentOffset uint64 = 0
	var recovered int = 0

	for {
		if _, err := io.ReadFull(file, headerBuf); err != nil {
			break
		}

		if binary.BigEndian.Uint16(headerBuf[0:2]) != MagicBytes {
			fmt.Println("[Recovery] Invalid magic bytes, stopping recovery")
			break
		}

		opCode := headerBuf[2]
		payloadLength := binary.BigEndian.Uint32(headerBuf[3:7])
		payload := make([]byte, payloadLength)

		if _, err := io.ReadFull(file, payload); err != nil {
			fmt.Printf("[Recovery] Failed to read payload: %v\n", err)
			break
		}

		// Verify checksum
		expectedChecksum := binary.BigEndian.Uint32(headerBuf[7:11])
		if crc32.ChecksumIEEE(payload) != expectedChecksum {
			fmt.Printf("[Recovery] Checksum mismatch at offset %d, skipping\n", currentOffset)
			currentOffset += 11 + uint64(payloadLength)
			continue
		}

		// Only index PUT operations
		if opCode == OpPut {
			keyLen := binary.BigEndian.Uint16(payload[0:2])
			if int(keyLen) <= len(payload)-2 {
				key := string(payload[2 : 2+keyLen])
				index.Put(key, currentOffset, payloadLength)
				recovered++
			}
		}

		currentOffset += 11 + uint64(payloadLength)
	}

	fmt.Printf("[Recovery] Finished. Recovered %d entries, scanned to offset %d\n", recovered, currentOffset)
}

// ============================================================================
// SYSTEM BOOTSTRAP
// ============================================================================

func main() {
	walFile := "wal.log"

	// Initialize connection ID pool
	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	// Initialize index and recover from WAL
	index := NewShardedIndex()
	fmt.Println("[System] Starting index recovery...")
	RecoverIndex(walFile, index)

	// Initialize storage engine
	engine, err := NewStorageEngine(walFile, index)
	if err != nil {
		panic(fmt.Sprintf("failed to create storage engine: %v", err))
	}
	defer engine.Close()

	// Start disk worker
	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)

	// Start background compaction goroutine
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case ingressChan <- Transaction{OpCode: OpCompact}:
			case <-diskWorkerDone:
				return
			}
		}
	}()

	// Start TCP listener
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(fmt.Sprintf("failed to bind listener: %v", err))
	}
	defer listener.Close()
	fmt.Println("[System] Ready. DB Engine running on TCP port 8080")

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("[System] Failed to accept connection: %v\n", err)
			continue
		}

		select {
		case assignedID := <-idPool:
			client := &ConnectionState{
				conn:         conn,
				connID:       assignedID,
				scratchpad:   make([]byte, 0, 1024),
				currentState: StateReadingHeader,
			}
			go client.HandleConnection(ingressChan, engine)
		default:
			fmt.Println("[System] Connection limit reached, rejecting connection")
			conn.Close()
		}
	}
}