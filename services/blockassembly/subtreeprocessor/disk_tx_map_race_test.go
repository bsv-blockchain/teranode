package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// TestDiskTxMap_StatsRaceWithWriters is a -race regression. Stats() sums each
// disk shard's bytesWritten, and in production it is called (via
// reportDiskMapStats during moveForwardBlock) while the per-disk writerLoop
// goroutines are still incrementing bytesWritten — Clear() only quiesces them
// afterwards. bytesWritten was a plain int64 written by one goroutine and read
// by another with no synchronization, which `go test -race` flags. After the
// fix both sides use sync/atomic.
//
// Run under -race; reverting bytesWritten to plain += / read makes this fail
// with a data-race report.
func TestDiskTxMap_StatsRaceWithWriters(t *testing.T) {
	m := newTestDiskTxMap(t)

	const iterations = 5000

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: drive the per-disk writerLoop increments via Set.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			m.Set(chainhash.HashH([]byte{byte(i), byte(i >> 8)}), makeInpoints(int16(i%30000)))
		}
	}()

	// Reader: read the same counters concurrently via Stats.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = m.Stats()
		}
	}()

	wg.Wait()
}
