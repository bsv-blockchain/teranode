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
	defer s.Close(context.Background())

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

// BenchmarkCheckBlockIsInCurrentChainRoutes measures what the forked-set route
// actually changes: the cost the store pays to answer one chain-membership call.
//
// It exists because the only other benchmark of this comparison,
// BenchmarkCheckOldBlockIDs in services/blockvalidation, drives a mock blockchain
// client. Its CheckBlockIsInCurrentChain returns instantly, so it measures the
// checkOldBlockIDs dedupe loop and the missing local prefetch, and is blind to
// everything the store does. That is why its in-memory-chain-check case reads
// several times the prefetch case and does not move on this branch: nothing it
// measures is on this branch's diff. Reading it as "the forked-set route is
// slower" is reading the wrong instrument.
//
// The three cases here are the decision the soak is for:
//
//	SQL              blockchain_use_in_memory_chain_check = false
//	ForkedSet        the route on, shadow comparison off, which is the win
//	ForkedSetShadow  the route on with the shadow comparison still on, which is
//	                 what a soaking node pays and is deliberately no faster,
//	                 because the authoritative query still runs
//
// Run: go test -bench BenchmarkCheckBlockIsInCurrentChainRoutes -run '^$' -benchmem ./stores/blockchain/sql/
func BenchmarkCheckBlockIsInCurrentChainRoutes(b *testing.B) {
	newSeededStore := func(b *testing.B, useInMemory, shadow bool) (*SQL, []uint32) {
		b.Helper()

		tSettings := test.CreateBaseTestSettings(b)
		tSettings.BlockChain.UseInMemoryChainCheck = useInMemory
		tSettings.BlockChain.ChainCheckShadowCompare = shadow

		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(b, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(b, err)

		deadline := time.Now().Add(5 * time.Second)
		for s.mainChainRebuilding.Load() > 0 {
			if time.Now().After(deadline) {
				b.Fatal("startup rebuild did not complete in time")
			}
			time.Sleep(time.Millisecond)
		}

		chain := []*model.Block{block1, block2, block3}
		for _, blk := range chain {
			_, _, err = s.StoreBlock(context.Background(), blk, "peer")
			require.NoError(b, err)
		}

		const totalBlocks = 25
		for len(chain) < totalBlocks {
			next := createBlock3OnFork(chain[len(chain)-1])
			_, _, err = s.StoreBlock(context.Background(), next, "peer")
			require.NoError(b, err)

			chain = append(chain, next)
		}

		// Refresh the in-memory structures so the forked-set route is answering
		// against a set that covers everything just stored.
		require.NoError(b, s.rebuildOffChainSet(context.Background()))

		ids := make([]uint32, len(chain))
		for i, blk := range chain {
			var id uint32
			require.NoError(b, s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, blk.Hash().CloneBytes()).Scan(&id))

			ids[i] = id
		}

		return s, ids
	}

	routes := []struct {
		name        string
		useInMemory bool
		shadow      bool
	}{
		{"SQL", false, false},
		{"ForkedSet", true, false},
		{"ForkedSetShadow", true, true},
	}

	for _, route := range routes {
		s, ids := newSeededStore(b, route.useInMemory, route.shadow)

		for _, n := range []int{1, 5, 20} {
			input := ids[:n]

			b.Run(fmt.Sprintf("%s/N=%d", route.name, n), func(b *testing.B) {
				// Every id here is on the main chain, which is the production
				// happy path: the about-to-reject path is rare by construction.
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					onChain, err := s.CheckBlockIsInCurrentChain(context.Background(), input)
					if err != nil {
						b.Fatal(err)
					}

					if !onChain {
						b.Fatal("benchmark input must be on the main chain")
					}
				}
			})
		}

		s.Close(context.Background())
	}
}
