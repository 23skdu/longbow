package store


import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewShardedIndexChannel verifies sharded channel creation
func TestNewShardedIndexChannel(t *testing.T) {
	numShards := 4
	bufferSize := 100

	sic := NewShardedIndexChannel(numShards, bufferSize)
	if sic == nil {
		t.Fatal("NewShardedIndexChannel returned nil")
	}

	if sic.NumShards() != numShards {
		t.Errorf("NumShards() = %d, want %d", sic.NumShards(), numShards)
	}

	for i := 0; i < numShards; i++ {
		ch := sic.GetShardChannel(i)
		if ch == nil {
			t.Errorf("Shard %d channel is nil", i)
		}
		if cap(ch) != bufferSize {
			t.Errorf("Shard %d buffer size = %d, want %d", i, cap(ch), bufferSize)
		}
	}
}

// TestShardedIndexChannelRouting verifies consistent hash-based routing
func TestShardedIndexChannelRouting(t *testing.T) {
	sic := NewShardedIndexChannel(8, 100)

	// Same dataset should always route to same shard
	dataset1Shard := sic.GetShardForDataset("dataset_alpha")
	for i := 0; i < 100; i++ {
		if sic.GetShardForDataset("dataset_alpha") != dataset1Shard {
			t.Fatal("Inconsistent routing for same dataset")
		}
	}

	// Different datasets should distribute across shards
	shardCounts := make(map[int]int)
	datasets := []string{"ds1", "ds2", "ds3", "ds4", "ds5", "ds6", "ds9", "ds10"}
	for _, ds := range datasets {
		shard := sic.GetShardForDataset(ds)
		shardCounts[shard]++
	}

	if len(shardCounts) < 2 {
		t.Errorf("Poor distribution: only %d shards used for %d datasets", len(shardCounts), len(datasets))
	}
}

// TestShardedIndexChannelSend verifies job submission
func TestShardedIndexChannelSend(t *testing.T) {
	sic := NewShardedIndexChannel(4, 10)

	job := IndexJob{
		DatasetName: "test_dataset",
		BatchIdx:    5,
	}

	ok := sic.Send(job)
	if !ok {
		t.Fatal("Send failed unexpectedly")
	}

	expectedShard := sic.GetShardForDataset("test_dataset")
	ch := sic.GetShardChannel(expectedShard)

	select {
	case received := <-ch:
		if received.DatasetName != job.DatasetName {
			t.Errorf("DatasetName = %s, want %s", received.DatasetName, job.DatasetName)
		}
		if received.BatchIdx != job.BatchIdx {
			t.Errorf("BatchIdx = %d, want %d", received.BatchIdx, job.BatchIdx)
		}
	default:
		t.Fatal("Job not found in expected shard channel")
	}
}

// TestShardedIndexChannelTrySend verifies non-blocking send
func TestShardedIndexChannelTrySend(t *testing.T) {
	sic := NewShardedIndexChannel(2, 1)

	job := IndexJob{DatasetName: "test", BatchIdx: 0}

	if !sic.TrySend(job) {
		t.Error("First TrySend should succeed")
	}

	if sic.TrySend(job) {
		t.Error("Second TrySend should fail when buffer full")
	}
}

// TestShardedIndexChannelClose verifies graceful shutdown
func TestShardedIndexChannelClose(t *testing.T) {
	sic := NewShardedIndexChannel(4, 100)

	sic.Close()

	for i := 0; i < 4; i++ {
		ch := sic.GetShardChannel(i)
		_, ok := <-ch
		if ok {
			t.Errorf("Shard %d channel not closed", i)
		}
	}

	if sic.Send(IndexJob{DatasetName: "test"}) {
		t.Error("Send after Close should return false")
	}
}

// TestShardedIndexChannelStats verifies per-shard statistics
func TestShardedIndexChannelStats(t *testing.T) {
	sic := NewShardedIndexChannel(4, 100)

	for i := 0; i < 20; i++ {
		sic.Send(IndexJob{DatasetName: "dataset_" + string(rune('A'+i)), BatchIdx: i})
	}

	stats := sic.Stats()

	if len(stats) != 4 {
		t.Errorf("Stats length = %d, want 4", len(stats))
	}

	var totalSent int64
	for _, s := range stats {
		totalSent += s.JobsSent
	}
	if totalSent != 20 {
		t.Errorf("Total jobs sent = %d, want 20", totalSent)
	}
}

// TestShardedIndexChannelWithWorkers verifies end-to-end with workers
func TestShardedIndexChannelWithWorkers(t *testing.T) {
	sic := NewShardedIndexChannel(4, 100)

	var processed atomic.Int64
	var wg sync.WaitGroup

	for shardID := 0; shardID < sic.NumShards(); shardID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range sic.GetShardChannel(id) {
				processed.Add(1)
			}
		}(shardID)
	}

	for i := 0; i < 100; i++ {
		sic.Send(IndexJob{DatasetName: "ds_" + string(rune('0'+i%10)), BatchIdx: i})
	}

	sic.Close()
	wg.Wait()

	if processed.Load() != 100 {
		t.Errorf("Processed = %d, want 100", processed.Load())
	}
}

// TestDefaultShardCount verifies sensible default
func TestDefaultShardCount(t *testing.T) {
	sic := NewShardedIndexChannelDefault(1000)

	expected := runtime.NumCPU()
	if sic.NumShards() != expected {
		t.Errorf("Default shards = %d, want %d (numCPU)", sic.NumShards(), expected)
	}
}

// TestNoNoisyNeighbor verifies isolation between datasets on different shards
func TestNoNoisyNeighbor(t *testing.T) {
	sic := NewShardedIndexChannel(4, 5)

	// Find two datasets that hash to different shards
	var ds1, ds2 string
	var shard1, shard2 int
	for i := 0; i < 100; i++ {
		ds1 = "dataset_" + string(byte('A'+i))
		shard1 = sic.GetShardForDataset(ds1)
		for j := i + 1; j < 100; j++ {
			ds2 = "dataset_" + string(byte('A'+j))
			shard2 = sic.GetShardForDataset(ds2)
			if shard1 != shard2 {
				goto found
			}
		}
	}
	t.Skip("Could not find two datasets with different shards")
found:

	// Fill up ds1's shard completely
	for i := 0; i < 5; i++ {
		sic.Send(IndexJob{DatasetName: ds1, BatchIdx: i})
	}

	// ds2 should still be able to send (different shard)
	done := make(chan bool, 1)
	go func() {
		ok := sic.Send(IndexJob{DatasetName: ds2, BatchIdx: 0})
		done <- ok
	}()

	select {
	case ok := <-done:
		if !ok {
			t.Error("Send to different shard failed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Send to different shard blocked - noisy neighbor not prevented")
	}
}

// BenchmarkShardedVsSingleChannel compares throughput
func BenchmarkShardedVsSingleChannel(b *testing.B) {
	b.Run("SingleChannel", func(b *testing.B) {
		ch := make(chan IndexJob, 10000)
		go func() {
			for range ch {
			}
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ch <- IndexJob{DatasetName: "ds_" + string(byte('0'+i%10)), BatchIdx: i}
		}
		close(ch)
	})

	b.Run("ShardedChannel_4", func(b *testing.B) {
		sic := NewShardedIndexChannel(4, 2500)
		for i := 0; i < 4; i++ {
			go func(id int) {
				for range sic.GetShardChannel(id) {
				}
			}(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sic.Send(IndexJob{DatasetName: "ds_" + string(byte('0'+i%10)), BatchIdx: i})
		}
		sic.Close()
	})
}
