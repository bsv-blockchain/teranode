package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// TestDiskTxMap_StatsRaceWithWriters is a -race regression. Stats() sums each
// log segment's bytesWritten, and in production it is called (via
// reportDiskMapStats during moveForwardBlock) while writes are still
// incrementing bytesWritten. Both sides must use sync/atomic.
//
// Run under -race; reverting bytesWritten to plain += / read makes this fail
// with a data-race report.
func TestDiskTxMap_StatsRaceWithWriters(t *testing.T) {
	m := newTestDiskTxMap(t)

	const iterations = 5000

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: drive the bytesWritten increments via Set.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			m.Set(chainhash.HashH([]byte{byte(i), byte(i >> 8)}), makeInpoints(int16(i%30000)))

			if i%100 == 0 {
				_ = m.Flush() // writes to the file, the only thing that moves bytesWritten
			}
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
