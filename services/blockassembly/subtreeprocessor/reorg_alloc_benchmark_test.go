package subtreeprocessor

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// BenchmarkReorgOptimizations runs paired Old/New benchmarks for each optimization
// in the reorgBlocks path, showing the allocation and performance improvements.
//
// Run with: go test -bench BenchmarkReorgOptimizations -benchmem -count=3 -benchtime=50x ./services/blockassembly/subtreeprocessor/...
func BenchmarkReorgOptimizations(b *testing.B) {
	scales := []struct {
		name    string
		txCount int
	}{
		{"10K", 10_000},
		{"100K", 100_000},
	}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("DedupFilterPipeline/Old/%s", scale.name), func(b *testing.B) {
			benchmarkDedupFilterPipelineOld(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("DedupFilterPipeline/New/%s", scale.name), func(b *testing.B) {
			benchmarkDedupFilterPipelineNew(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("AllMarkFalse/Old/%s", scale.name), func(b *testing.B) {
			benchmarkAllMarkFalseOld(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("AllMarkFalse/New/%s", scale.name), func(b *testing.B) {
			benchmarkAllMarkFalseNew(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("HashSlicePool/Old/%s", scale.name), func(b *testing.B) {
			benchmarkHashSlicePoolOld(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("HashSlicePool/New/%s", scale.name), func(b *testing.B) {
			benchmarkHashSlicePoolNew(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("NodeFlags/Old/%s", scale.name), func(b *testing.B) {
			benchmarkNodeFlagsOld(b, scale.txCount)
		})
		b.Run(fmt.Sprintf("NodeFlags/New/%s", scale.name), func(b *testing.B) {
			benchmarkNodeFlagsNew(b, scale.txCount)
		})
	}
}

// ---------------------------------------------------------------------------
// 1. DedupFilterPipeline — the biggest win
// ---------------------------------------------------------------------------

// Old pattern: Keys() intermediate slice, map[Hash]bool, separate filteredMarkOnLongestChain alloc
func benchmarkDedupFilterPipelineOld(b *testing.B, txCount int) {
	winningHashes := generateHashes(txCount * 80 / 100)
	losingHashes := generateHashesWithOffset(txCount*20/100, txCount*95/100)
	movedBackHashes := generateHashesWithOffset(txCount*10/100, 0)

	movedBackSet := make(map[chainhash.Hash]struct{}, len(movedBackHashes))
	for _, h := range movedBackHashes {
		movedBackSet[h] = struct{}{}
	}

	// Simulate Keys() output — the old code called losingTxHashesMap.Keys()
	// which returned a fresh []chainhash.Hash each iteration
	losingKeys := make([]chainhash.Hash, len(losingHashes))
	copy(losingKeys, losingHashes)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		winningTxSet := make(map[chainhash.Hash]struct{}, len(winningHashes))
		markOnLongestChain := make([]chainhash.Hash, 0, len(winningHashes))

		for _, hash := range winningHashes {
			winningTxSet[hash] = struct{}{}
			if _, inMovedBack := movedBackSet[hash]; !inMovedBack {
				markOnLongestChain = append(markOnLongestChain, hash)
			}
		}

		// OLD: Keys() allocates a fresh intermediate slice
		rawLosingTxHashes := make([]chainhash.Hash, len(losingKeys))
		copy(rawLosingTxHashes, losingKeys)

		// OLD: map[chainhash.Hash]bool (1 byte per entry vs 0 for struct{})
		losingTxSet := make(map[chainhash.Hash]bool, len(rawLosingTxHashes))
		allLosingTxHashes := make([]chainhash.Hash, 0, len(rawLosingTxHashes))
		for _, hash := range rawLosingTxHashes {
			if _, isWinning := winningTxSet[hash]; !isWinning {
				if !losingTxSet[hash] {
					losingTxSet[hash] = true
					allLosingTxHashes = append(allLosingTxHashes, hash)
				}
			}
		}

		// OLD: separate filteredMarkOnLongestChain allocation + copy pass
		filteredMarkOnLongestChain := make([]chainhash.Hash, 0, len(markOnLongestChain))
		for _, hash := range markOnLongestChain {
			if !losingTxSet[hash] {
				filteredMarkOnLongestChain = append(filteredMarkOnLongestChain, hash)
			}
		}

		_ = allLosingTxHashes
		_ = filteredMarkOnLongestChain
	}
}

// New pattern: Iter() avoids Keys(), map[Hash]struct{}, pooled slice, in-place filter
func benchmarkDedupFilterPipelineNew(b *testing.B, txCount int) {
	winningHashes := generateHashes(txCount * 80 / 100)
	losingHashes := generateHashesWithOffset(txCount*20/100, txCount*95/100)
	movedBackHashes := generateHashesWithOffset(txCount*10/100, 0)

	movedBackSet := make(map[chainhash.Hash]struct{}, len(movedBackHashes))
	for _, h := range movedBackHashes {
		movedBackSet[h] = struct{}{}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		winningTxSet := make(map[chainhash.Hash]struct{}, len(winningHashes))
		losingTxSet := make(map[chainhash.Hash]struct{}, len(losingHashes))
		markOnLongestChain := make([]chainhash.Hash, 0, len(winningHashes))

		for _, hash := range winningHashes {
			winningTxSet[hash] = struct{}{}
			if _, inMovedBack := movedBackSet[hash]; !inMovedBack {
				markOnLongestChain = append(markOnLongestChain, hash)
			}
		}

		// NEW: iterate directly, no Keys() intermediate
		for _, hash := range losingHashes {
			if _, isWinning := winningTxSet[hash]; !isWinning {
				losingTxSet[hash] = struct{}{}
			}
		}

		// NEW: pooled slice from losingTxSet
		allLosing := getHashSlice(len(losingTxSet))
		for hash := range losingTxSet {
			*allLosing = append(*allLosing, hash)
		}

		// NEW: in-place filter — no second slice allocation
		n := 0
		for _, hash := range markOnLongestChain {
			if _, isLosing := losingTxSet[hash]; !isLosing {
				markOnLongestChain[n] = hash
				n++
			}
		}
		_ = markOnLongestChain[:n]

		putHashSlice(allLosing)
	}
}

// ---------------------------------------------------------------------------
// 2. AllMarkFalse construction — N+1 allocs/calls → 1 pooled alloc/call
// ---------------------------------------------------------------------------

func buildSubtrees(b *testing.B, txCount int) []*subtreepkg.Subtree {
	b.Helper()
	// Use 1023 tx nodes per subtree + 1 coinbase = 1024 leaves (power of two required)
	nodesPerSubtree := 1023
	numSubtrees := txCount / nodesPerSubtree
	if numSubtrees < 1 {
		numSubtrees = 1
		nodesPerSubtree = txCount
	}

	subtrees := make([]*subtreepkg.Subtree, numSubtrees)
	for s := 0; s < numSubtrees; s++ {
		// leafCount must be power of two; nodesPerSubtree+1 = 1024
		leafCount := nextPowerOfTwo(nodesPerSubtree + 1)
		st, err := subtreepkg.NewTreeByLeafCount(leafCount)
		if err != nil {
			b.Fatal(err)
		}
		_ = st.AddCoinbaseNode()
		for i := 0; i < nodesPerSubtree; i++ {
			hash := chainhash.HashH([]byte(fmt.Sprintf("bench-%d-%d", s, i)))
			_ = st.AddSubtreeNode(subtreepkg.Node{Hash: hash, Fee: 100, SizeInBytes: 250})
		}
		subtrees[s] = st
	}
	return subtrees
}

func nextPowerOfTwo(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// Old pattern: per-subtree make() + per-subtree store call
func benchmarkAllMarkFalseOld(b *testing.B, txCount int) {
	subtrees := buildSubtrees(b, txCount)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// OLD: allocate per subtree, simulate per-subtree call
		for _, st := range subtrees {
			notOnLongestChain := make([]chainhash.Hash, 0, len(st.Nodes))
			for _, node := range st.Nodes {
				if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
					notOnLongestChain = append(notOnLongestChain, node.Hash)
				}
			}
			_ = len(notOnLongestChain) // simulate passing to store call
		}
	}
}

// New pattern: single pooled alloc across all subtrees
func benchmarkAllMarkFalseNew(b *testing.B, txCount int) {
	subtrees := buildSubtrees(b, txCount)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		totalLen := 0
		for _, st := range subtrees {
			totalLen += len(st.Nodes)
		}

		allMarkFalse := getHashSlice(totalLen)
		for _, st := range subtrees {
			for _, node := range st.Nodes {
				if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
					*allMarkFalse = append(*allMarkFalse, node.Hash)
				}
			}
		}

		_ = len(*allMarkFalse)
		putHashSlice(allMarkFalse)
	}
}

// ---------------------------------------------------------------------------
// 3. HashSlicePool — fresh make() vs pooled get/put
// ---------------------------------------------------------------------------

// Old pattern: fresh make() every time
func benchmarkHashSlicePoolOld(b *testing.B, txCount int) {
	hashes := generateHashes(txCount)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := make([]chainhash.Hash, 0, txCount)
		s = append(s, hashes...)
		_ = len(s)
	}
}

// New pattern: sync.Pool — 0 allocs after warmup
func benchmarkHashSlicePoolNew(b *testing.B, txCount int) {
	hashes := generateHashes(txCount)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := getHashSlice(txCount)
		*s = append(*s, hashes...)
		putHashSlice(s)
	}
}

// ---------------------------------------------------------------------------
// 4. NodeFlags — 3x bool arrays vs single byte bitset
// ---------------------------------------------------------------------------

// Old pattern: three separate bool arrays (3 allocs, 3N bytes)
func benchmarkNodeFlagsOld(b *testing.B, txCount int) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		added := make([]bool, txCount)
		removed := make([]bool, txCount)
		modified := make([]bool, txCount)
		// Simulate flag access
		added[0] = true
		removed[0] = true
		modified[0] = true
		_ = added[txCount-1]
		_ = removed[txCount-1]
		_ = modified[txCount-1]
	}
}

// New pattern: single byte slice with bit flags (1 alloc, N bytes)
func benchmarkNodeFlagsNew(b *testing.B, txCount int) {
	const (
		flagAdded    byte = 1 << 0
		flagRemoved  byte = 1 << 1
		flagModified byte = 1 << 2
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		flags := make([]byte, txCount)
		// Simulate flag access
		flags[0] |= flagAdded
		flags[0] |= flagRemoved
		flags[0] |= flagModified
		_ = flags[txCount-1] & flagAdded
		_ = flags[txCount-1] & flagRemoved
		_ = flags[txCount-1] & flagModified
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func generateHashes(count int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, count)
	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte(fmt.Sprintf("hash-%d", i)))
	}
	return hashes
}

func generateHashesWithOffset(count, offset int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, count)
	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte(fmt.Sprintf("hash-%d", i+offset)))
	}
	return hashes
}
