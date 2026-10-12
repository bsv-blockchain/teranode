package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// BenchmarkRemainderLookupPhase runs processRemainderTxHashes on subtrees of
// production size (1M leaves) whose txs are all in the block, so only the
// lookup phase and the in-order collection do work. Skipped in short mode.
//
// wake_p99_ms and wake_max_ms report how late a goroutine sleeping 1ms at a
// time wakes up while the pass runs: a stand-in for the ingest goroutines that
// must get a P while a block is applied.
func BenchmarkRemainderLookupPhase(b *testing.B) {
	if testing.Short() {
		b.Skip("large benchmark")
	}

	const (
		subtrees   = 64
		perSubtree = 1 << 20
	)

	b.Run(fmt.Sprintf("%dx%dM", subtrees, perSubtree>>20), func(b *testing.B) {
		stp := newRemainderLookupBenchProcessor(b)

		transactionMap := NewSplitSwissMap(1024, subtrees*perSubtree)
		chained := make([]*subtreepkg.Subtree, 0, subtrees)

		var h chainhash.Hash

		for s := 0; s < subtrees; s++ {
			st, err := subtreepkg.NewTreeByLeafCount(perSubtree)
			require.NoError(b, err)

			for i := 0; i < perSubtree; i++ {
				binary.LittleEndian.PutUint64(h[0:8], uint64(s)<<32|uint64(i)*0x9e3779b97f4a7c15)
				binary.LittleEndian.PutUint64(h[8:16], uint64(i)^0xdeadbeef)
				require.NoError(b, st.AddNode(h, 1, 1))
				require.NoError(b, transactionMap.Put(h))
			}

			chained = append(chained, st)
		}

		transactionMap.Freeze()

		var (
			stop  atomic.Bool
			lates []time.Duration
			done  = make(chan struct{})
		)

		b.ResetTimer()

		go func() {
			defer close(done)

			for !stop.Load() {
				t0 := time.Now()
				time.Sleep(time.Millisecond)
				lates = append(lates, time.Since(t0)-time.Millisecond)
			}
		}()

		for i := 0; i < b.N; i++ {
			require.NoError(b, stp.processRemainderTxHashes(context.Background(), chained, transactionMap, nil, stp.currentTxMap, true))
		}

		b.StopTimer()
		stop.Store(true)
		<-done

		sort.Slice(lates, func(i, j int) bool { return lates[i] < lates[j] })
		b.ReportMetric(float64(lates[len(lates)*99/100].Microseconds())/1000, "wake_p99_ms")
		b.ReportMetric(float64(lates[len(lates)-1].Microseconds())/1000, "wake_max_ms")
	})
}

func newRemainderLookupBenchProcessor(b *testing.B) *SubtreeProcessor {
	b.Helper()
	stp, cleanup := setupSubtreeProcessorForBenchB(b, 1<<20)
	b.Cleanup(cleanup)

	return stp
}
