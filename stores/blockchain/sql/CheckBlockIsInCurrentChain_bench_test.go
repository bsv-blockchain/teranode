package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// BenchmarkCheckBlockIsInCurrentChainSQL measures the per-call latency of the
// on_main_chain fast path at increasing N (number of block IDs per call) and
// compares it to the CTE fallback. It documents the single-query win over the
// previous N-round-trip loop and guards against regressions.
//
// Run: go test -bench BenchmarkCheckBlockIsInCurrentChainSQL -run '^$' ./stores/blockchain/sql/...
func BenchmarkCheckBlockIsInCurrentChainSQL(b *testing.B) {
	tSettings := test.CreateBaseTestSettings(b)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(b, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(b, err)
	defer s.Close()

	// Wait for the startup rebuild goroutine to release its guard so the fast
	// path (mainChainRebuilding == 0) is actually exercised.
	deadline := time.Now().Add(5 * time.Second)
	for s.mainChainRebuilding.Load() > 0 {
		if time.Now().After(deadline) {
			b.Fatal("startup rebuild did not complete in time")
		}
		time.Sleep(time.Millisecond)
	}

	// Seed a 25-block main chain: block1 → block2 → block3 → 22 fork-builder
	// blocks on top. All are on_main_chain = true by construction.
	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(b, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(b, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(b, err)

	chain := []*model.Block{block1, block2, block3}
	const totalBlocks = 25
	for len(chain) < totalBlocks {
		next := createBlock3OnFork(chain[len(chain)-1])
		_, _, err = s.StoreBlock(context.Background(), next, "peer")
		require.NoError(b, err)
		chain = append(chain, next)
	}

	ids := make([]uint32, len(chain))
	for i, blk := range chain {
		var id uint32
		require.NoError(b, s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, blk.Hash().CloneBytes()).Scan(&id))
		ids[i] = id
	}

	for _, n := range []int{1, 5, 20} {
		input := ids[:n]

		b.Run(fmt.Sprintf("FastPath/N=%d", n), func(b *testing.B) {
			require.Zero(b, s.mainChainRebuilding.Load(), "fast path requires guard=0")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.checkBlockIsInCurrentChainSQL(context.Background(), input); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("CTE/N=%d", n), func(b *testing.B) {
			s.mainChainRebuilding.Add(1)
			defer s.mainChainRebuilding.Add(-1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.checkBlockIsInCurrentChainSQL(context.Background(), input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
