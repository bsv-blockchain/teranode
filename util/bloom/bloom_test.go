// //go:build manual_tests

package bloom

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/greatroar/blobloom"
)

// This function would need to generate a valid random txid
func generateRandomTxid() []byte {
	// pseudo-code, replace with your actual random txid generation logic
	bytes := make([]byte, 32)
	_, _ = rand.Read(bytes)
	return bytes
}

func TestBloomFilter2(t *testing.T) {
	// Number of txids to test
	numTxids := 10_000_000
	t.Logf("Number of txids: %v\n", numTxids)
	txids := make([][]byte, numTxids)
	insertionDiv := 5

	// Generate random txids
	for i := 0; i < numTxids; i++ {
		txids[i] = generateRandomTxid()
	}

	filter := blobloom.NewOptimized(blobloom.Config{
		Capacity: uint64(numTxids / insertionDiv), // Expected number of keys.
		FPRate:   1e-5,                            // Accept one false positive per 10,000 lookups.
	})

	// Add 10% of the txids
	var n64 uint64
	start := time.Now()
	for i := 0; i < numTxids/insertionDiv; i++ {
		// take the first 8 bytes of the txid as the key
		//binary.BigEndian.PutUint64(txids[i], n64)
		n64 = uint64(txids[i][0])<<56 | uint64(txids[i][1])<<48 | uint64(txids[i][2])<<40 | uint64(txids[i][3])<<32 | uint64(txids[i][4])<<24 | uint64(txids[i][5])<<16 | uint64(txids[i][6])<<8 | uint64(txids[i][7])
		filter.Add(n64)
	}
	t.Logf("Time to add txids: %v\n", time.Since(start))

	// Test the txids and count false positives
	start = time.Now()
	falsePositives := 0
	for i := 0; i < numTxids; i++ {
		//binary.BigEndian.PutUint64(txids[i], n64)
		n64 = uint64(txids[i][0])<<56 | uint64(txids[i][1])<<48 | uint64(txids[i][2])<<40 | uint64(txids[i][3])<<32 | uint64(txids[i][4])<<24 | uint64(txids[i][5])<<16 | uint64(txids[i][6])<<8 | uint64(txids[i][7])
		if i < numTxids/insertionDiv {
			if !filter.Has(n64) {
				t.Errorf("txid was added to filter but Test() returned false")
			}
		} else {
			if filter.Has(n64) {
				falsePositives++
			}
		}
	}
	t.Logf("\nTime to test txids: %v\n", time.Since(start))
	t.Logf("Number of false positives: %v\n", falsePositives)
}

func TestBloomFilter(t *testing.T) {
	filter := NewShardedBloomFilter()

	// Number of txids to test
	numTxids := 10_000_000
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
