// //go:build manual_tests

package bloom

import (
	"crypto/rand"
	"runtime"
	"testing"
	"time"
)

func Test_BloomFilterSharded(t *testing.T) {
	filter := NewShardedBloomFilter()

	// Number of txids to test
	numTxids := 1_000_000
	t.Logf("Number of txids: %v\n", numTxids)
	txids := make([][]byte, numTxids)
	insertionDiv := 5

	// Generate random txids
	for i := 0; i < numTxids; i++ {
		txids[i] = generateRandomTxid()
	}

	// Add 10% of the txids
	start := time.Now()

	for i := 0; i < numTxids/insertionDiv; i++ {
		filter.Add(txids[i])
	}

	t.Logf("Time to add txids: %v\n", time.Since(start))

	// Test the txids and count false positives
	start = time.Now()
	falsePositives := 0

	for i := 0; i < numTxids; i++ {
		if i < numTxids/insertionDiv {
			if !filter.Test(txids[i]) {
				t.Errorf("txid was added to filter but Test() returned false")
			}
		} else {
			if filter.Test(txids[i]) {
				falsePositives++
			}
		}
	}

	t.Logf("\nTime to test txids: %v\n", time.Since(start))
	t.Logf("Number of false positives: %v\n", falsePositives)
}

// This function would need to generate a valid random txid
func generateRandomTxid() []byte {
	// pseudo-code, replace with your actual random txid generation logic
	bytes := make([]byte, 32)
	_, _ = rand.Read(bytes)

	return bytes
}

func getAlloc() uint64 {
	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	return m.Alloc
}
