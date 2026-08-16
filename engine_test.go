package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupTestNode(t *testing.T) (string, string) {
	dbPath := filepath.Join(t.TempDir(), "test.wal")

	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index := NewShardedIndex()
	engine, err := NewStorageEngine(dbPath, index)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	t.Cleanup(func() {
		engine.Close()
	})

	ingressChan := make(chan Transaction, 4096)

	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)

	t.Cleanup(func() {
		close(diskWorkerStop)
		select {
		case <-diskWorkerDone:
		case <-time.After(2 * time.Second):
			t.Log("disk worker did not shut down within 2s of stop signal")
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to bind port: %v", err)
	}
	t.Cleanup(func() {
		listener.Close()
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			assignedID := <-idPool
			client := &ConnectionState{
				conn:         conn,
				connID:       assignedID,
				scratchpad:   make([]byte, 0, 1024),
				currentState: StateReadingHeader,
			}
			go client.HandleConnection(ingressChan, engine)
		}
	}()

	return listener.Addr().String(), dbPath
}

func buildPutFrame(key string, val []byte) []byte {
	keyLen := uint16(len(key))
	payload := make([]byte, 2+len(key)+len(val))
	binary.BigEndian.PutUint16(payload[0:2], keyLen)
	copy(payload[2:], key)
	copy(payload[2+keyLen:], val)

	header := make([]byte, 11)
	binary.BigEndian.PutUint16(header[0:2], MagicBytes)
	header[2] = OpPut
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[7:11], crc32.ChecksumIEEE(payload))

	return append(header, payload...)
}

func buildGetFrame(key string) []byte {
	payload := []byte(key)

	header := make([]byte, 11)
	binary.BigEndian.PutUint16(header[0:2], MagicBytes)
	header[2] = OpGet
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[7:11], crc32.ChecksumIEEE(payload))

	return append(header, payload...)
}

func TestEngine_PutAndGet(t *testing.T) {
	addr, _ := setupTestNode(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	conn.Write(buildPutFrame("test_key", []byte("test_value")))
	respBuf := make([]byte, 1024)
	n, err := conn.Read(respBuf)
	if err != nil {
		t.Fatalf("Read error during PUT: %v", err)
	}
	if got := string(respBuf[:n]); got != "200 OK: Disk Commit Verified\n" {
		t.Fatalf("PUT failed. Expected '200 OK: Disk Commit Verified\\n', got: %q", got)
	}

	conn.Write(buildGetFrame("test_key"))
	n, err = conn.Read(respBuf)
	if err != nil {
		t.Fatalf("Read error during GET: %v", err)
	}
	if got := string(respBuf[:n]); got != "OK: test_value" {
		t.Fatalf("GET failed. Got: %q", got)
	}
}

func TestEngine_ConcurrentLoad(t *testing.T) {
	addr, _ := setupTestNode(t)

	var wg sync.WaitGroup
	numWorkers := 50
	requestsPerWorker := 100

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("Worker %d dial failed: %v", workerID, err)
				return
			}
			defer conn.Close()

			respBuf := make([]byte, 1024)

			for j := 0; j < requestsPerWorker; j++ {
				key := fmt.Sprintf("key_%d_%d", workerID, j)
				val := []byte(fmt.Sprintf("val_%d_%d", workerID, j))

				conn.Write(buildPutFrame(key, val))
				n, err := conn.Read(respBuf)
				if err != nil {
					t.Errorf("Worker %d read failed: %v", workerID, err)
					return
				}

				if got := string(respBuf[:n]); got != "200 OK: Disk Commit Verified\n" {
					t.Errorf("Worker %d expected 200 OK, got: %q", workerID, got)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestEngine_ConnectionIDRecycling(t *testing.T) {
	addr, _ := setupTestNode(t)

	// Open and close multiple connections rapidly
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}

		key := fmt.Sprintf("recycled_key_%d", i)
		val := []byte(fmt.Sprintf("recycled_val_%d", i))

		conn.Write(buildPutFrame(key, val))
		respBuf := make([]byte, 1024)
		n, err := conn.Read(respBuf)
		if err != nil {
			t.Fatalf("Read error during PUT: %v", err)
		}
		if got := string(respBuf[:n]); got != "200 OK: Disk Commit Verified\n" {
			t.Fatalf("PUT failed on iteration %d. Got: %q", i, got)
		}

		conn.Close()
	}

	// Verify all keys were written correctly
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("recycled_key_%d", i)
		conn.Write(buildGetFrame(key))
		respBuf := make([]byte, 1024)
		n, err := conn.Read(respBuf)
		if err != nil {
			t.Fatalf("Read error during GET: %v", err)
		}

		expected := fmt.Sprintf("OK: recycled_val_%d", i)
		if got := string(respBuf[:n]); got != expected {
			t.Fatalf("GET failed for iteration %d. Expected %q, got: %q", i, expected, got)
		}
	}
}

func TestEngine_CompactionReclaimsSpace(t *testing.T) {
	addr, dbPath := setupTestNode(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Write 100 entries
	
	for i := 0; i < 100; i++ {
		conn.Write(buildPutFrame("reclaimed_key", []byte("large_value_12345")))
		respBuf := make([]byte, 1024)
		conn.Read(respBuf)
	}

	time.Sleep(200 * time.Millisecond) // Let disk worker catch up

	// Get file size before compaction
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Failed to stat db before compaction: %v", err)
	}
	sizeBefore := info.Size()

	// Trigger compaction
	conn.Write([]byte{0xDE, 0xAD, 0x02, 0, 0, 0, 0, 0, 0, 0, 0}) // OpCompact frame
	time.Sleep(500 * time.Millisecond)

	info, err = os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Failed to stat db after compaction: %v", err)
	}
	sizeAfter := info.Size()

	if sizeAfter >= sizeBefore {
		t.Fatalf("Compaction didn't reduce size: before=%d, after=%d", sizeBefore, sizeAfter)
	}

	// Verify data still there
	conn.Write(buildGetFrame("reclaimed_key"))
	respBuf := make([]byte, 1024)
	n, _ := conn.Read(respBuf)
	if got := string(respBuf[:n]); !strings.Contains(got, "large_value") {
		t.Fatalf("Data lost after compaction: %q", got)
	}
}

func TestEngine_CorruptChecksumDetected(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt.wal")

	// Write valid frame
	file, _ := os.Create(dbPath)
	payload := []byte("test_payload")
	header := make([]byte, 11)
	binary.BigEndian.PutUint16(header[0:2], MagicBytes)
	header[2] = OpPut
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[7:11], crc32.ChecksumIEEE(payload))

	file.Write(header)
	file.Write(payload)
	file.Close()

	// Now corrupt the payload
	file, _ = os.OpenFile(dbPath, os.O_RDWR, 0666)
	file.WriteAt([]byte("CORRUPTED"), 15) // Corrupt payload
	file.Close()

	// Try to read - should fail
	index := NewShardedIndex()
	engine, _ := NewStorageEngine(dbPath, index)
	defer engine.Close()

	RecoverIndex(dbPath, index)

	// Index should be empty due to checksum failure
	record, exists := index.Get("test_key")
	if exists {
		t.Fatalf("Corrupt entry was indexed: %v", record)
	}
}

func TestEngine_IndexRebuildAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart.wal")

	// Phase 1: Write data with first engine
	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index1 := NewShardedIndex()
	engine1, _ := NewStorageEngine(dbPath, index1)

	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine1, diskWorkerStop, diskWorkerDone)

	// Write 10 entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("restart_key_%d", i)
		val := []byte(fmt.Sprintf("restart_val_%d", i))
		
		// Encode payload to match network layer expectations
		keyLen := uint16(len(key))
		payload := make([]byte, 2+len(key)+len(val))
		binary.BigEndian.PutUint16(payload[0:2], keyLen)
		copy(payload[2:], key)
		copy(payload[2+keyLen:], val)

		ackCh := make(chan struct{}, 1)
		tx := Transaction{
			ConnID:  0,
			OpCode:  OpPut,
			Key:     key,
			Payload: payload, // Use formatted payload instead of raw 'val'
			ackCh:   ackCh,
		}
		ingressChan <- tx
		<-ackCh
	}

	close(diskWorkerStop)
	<-diskWorkerDone
	engine1.Close()

	// Phase 2: Create new engine, recover index from WAL
	index2 := NewShardedIndex()
	engine2, _ := NewStorageEngine(dbPath, index2)
	defer engine2.Close()

	RecoverIndex(dbPath, index2)

	// Verify all 10 entries recovered
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("restart_key_%d", i)
		_, exists := index2.Get(key)
		if !exists {
			t.Fatalf("Key %s lost after restart", key)
		}

		val, err := engine2.GetValue(key)
		if err != nil || string(val) != fmt.Sprintf("restart_val_%d", i) {
			t.Fatalf("Value mismatch for %s: %v", key, err)
		}
	}
}