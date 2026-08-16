package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// BenchmarkPutThroughput measures single-threaded PUT performance
// This is the baseline - how fast can we write one at a time?
func BenchmarkPutThroughput(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.wal")

	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index := NewShardedIndex()
	engine, _ := NewStorageEngine(dbPath, index)
	defer engine.Close()

	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)
	defer func() {
		close(diskWorkerStop)
		<-diskWorkerDone
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("value_%d", i))

		ackCh := make(chan struct{}, 1)
		tx := Transaction{
			ConnID:  uint32(i % 100),
			OpCode:  OpPut,
			Key:     key,
			Payload: val,
			ackCh:   ackCh,
		}
		ingressChan <- tx
		<-ackCh
	}

	b.StopTimer()
}

// BenchmarkGetLatency measures index lookup + value read speed
// Should be much faster than PUT (no disk sync)
func BenchmarkGetLatency(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.wal")

	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index := NewShardedIndex()
	engine, _ := NewStorageEngine(dbPath, index)
	defer engine.Close()

	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)
	defer func() {
		close(diskWorkerStop)
		<-diskWorkerDone
	}()

	// Pre-populate with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("preload_%d", i)
		val := []byte(fmt.Sprintf("value_%d", i))
		ackCh := make(chan struct{}, 1)
		tx := Transaction{
			ConnID:  0,
			OpCode:  OpPut,
			Key:     key,
			Payload: val,
			ackCh:   ackCh,
		}
		ingressChan <- tx
		<-ackCh
	}

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("preload_%d", i%1000)
		engine.GetValue(key)
	}

	b.StopTimer()
}

// BenchmarkConcurrentPutGet is the most realistic benchmark
// This is what you should report on resume
// 10 concurrent workers doing mixed reads/writes
func BenchmarkConcurrentPutGet(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.wal")

	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index := NewShardedIndex()
	engine, _ := NewStorageEngine(dbPath, index)
	defer engine.Close()

	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)
	defer func() {
		close(diskWorkerStop)
		<-diskWorkerDone
	}()

	numWorkers := 10
	opsPerWorker := b.N / numWorkers

	b.ResetTimer()

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker_%d_key_%d", workerID, i)
				val := []byte(fmt.Sprintf("value_%d", i))

				// PUT
				ackCh := make(chan struct{}, 1)
				tx := Transaction{
					ConnID:  uint32(workerID),
					OpCode:  OpPut,
					Key:     key,
					Payload: val,
					ackCh:   ackCh,
				}
				ingressChan <- tx
				<-ackCh

				// GET
				engine.GetValue(key)
			}
		}(w)
	}

	wg.Wait()
	b.StopTimer()
}

// BenchmarkGroupCommit shows effect of batching
// Higher number = better batching (more ops per fsync)
func BenchmarkGroupCommit(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.wal")

	idPool = make(chan uint32, MaxConnections)
	for i := uint32(0); i < MaxConnections; i++ {
		idPool <- i
	}

	index := NewShardedIndex()
	engine, _ := NewStorageEngine(dbPath, index)
	defer engine.Close()

	ingressChan := make(chan Transaction, 4096)
	diskWorkerStop := make(chan struct{})
	diskWorkerDone := make(chan struct{})
	go StartDiskWorker(ingressChan, engine, diskWorkerStop, diskWorkerDone)
	defer func() {
		close(diskWorkerStop)
		<-diskWorkerDone
	}()

	ackChannels := make([]chan struct{}, 0, b.N)

	b.ResetTimer()

	// Send all operations without waiting
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("value_%d", i))

		ackCh := make(chan struct{}, 1)
		tx := Transaction{
			ConnID:  uint32(i % 100),
			OpCode:  OpPut,
			Key:     key,
			Payload: val,
			ackCh:   ackCh,
		}
		ingressChan <- tx
		ackChannels = append(ackChannels, ackCh)
	}

	// Wait for all ACKs
	for _, ackCh := range ackChannels {
		<-ackCh
	}

	b.StopTimer()
}

// BenchmarkIndexShardContention measures lock contention
// Higher number = better sharding (less contention)
func BenchmarkIndexShardContention(b *testing.B) {
	index := NewShardedIndex()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		index.Put(key, uint64(i*100), 32)
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	numWorkers := 8
	opsPerWorker := b.N / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key_%d", (workerID*1000+i)%10000)
				index.Get(key)
			}
		}(w)
	}

	wg.Wait()
	b.StopTimer()
}

// BenchmarkMemoryAllocation shows per-operation allocation overhead
// Run with: go test -bench=BenchmarkMemoryAllocation -benchmem
func BenchmarkMemoryAllocation(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ackCh := make(chan struct{}, 1)
		_ = Transaction{
			ConnID:  uint32(i),
			OpCode:  OpPut,
			Key:     fmt.Sprintf("key_%d", i),
			Payload: []byte(fmt.Sprintf("value_%d", i)),
			ackCh:   ackCh,
		}
	}
}