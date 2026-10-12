package uaerospike

import (
	"sync"
	"testing"
	"time"
)

// TestClient_Close_WaitsForInFlightOp is the regression test for the
// "concurrent map read and map write" crash: Close() closed the shared per-host
// aerospike client while a BatchOperate/BatchDecorate was still running, racing
// the client's partition map. Close() must instead wait for every in-flight
// operation (registered via beginOp/endOp) to finish before closing.
func TestClient_Close_WaitsForInFlightOp(t *testing.T) {
	// nil embedded *aerospike.Client: the drain is pure synchronization and does
	// not touch the underlying client, so no live server is needed.
	c := &Client{}

	// Simulate an operation in flight.
	c.beginOp()

	closed := make(chan struct{})
	started := make(chan struct{})
	go func() {
		// Signal that the goroutine is scheduled and about to call Close, so the
		// block assertion below can't pass merely because Close() hasn't been
		// invoked yet (scheduler delay would otherwise be a false positive).
		close(started)
		c.Close()
		close(closed)
	}()

	<-started

	// Close must block while the operation is still in flight.
	select {
	case <-closed:
		t.Fatal("Close returned while an operation was still in flight (client would be closed out from under it)")
	case <-time.After(100 * time.Millisecond):
	}

	// Operation finishes.
	c.endOp()

	// Close must now be able to complete.
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the in-flight operation drained")
	}
}

// TestClient_Close_ConcurrentOpsNoRace exercises the guard under -race: many
// goroutines registering/deregistering in-flight ops concurrently with Close().
func TestClient_Close_ConcurrentOpsNoRace(t *testing.T) {
	c := &Client{}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				c.beginOp()
				c.endOp()
			}
		}()
	}

	c.Close()
	wg.Wait()
}

// BenchmarkClient_OpGuard measures the per-operation cost of the drain guard
// (beginOp/endOp = RWMutex RLock/RUnlock) under maximum parallelism — the
// worst case for cache-line contention on the shared reader counter, with no
// other work to amortise it. Run with -cpu=1,8,16,... to see how it scales.
func BenchmarkClient_OpGuard(b *testing.B) {
	c := &Client{}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.beginOp()
			c.endOp()
		}
	})
}
