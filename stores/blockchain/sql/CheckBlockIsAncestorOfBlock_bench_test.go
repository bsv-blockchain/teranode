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

// BenchmarkCheckBlockIsAncestorOfBlockDepth measures one ancestor lookup for a main-chain block
// at increasing depth, which is what block validation asks for every parent outside its prefetch
// window. The anchor is either the main-chain tip or a fork block one block off the main chain,
// on the tip's parent, as in a one-block race. Four answers are compared:
//
//	FastPath      CheckBlockIsAncestorOfBlock as it is, main-chain anchor: the fork-point probe
//	ForkFastPath  the same call, fork anchor: a one-block walk to the fork point, then the probe
//	Walk          the main-chain call with the rebuild guard raised, so it takes the full walk
//	ForkWalk      the fork-anchored call with the guard raised
//	MainChain     checkBlockIsInCurrentChainSQL, the lookup block validation used before the
//	              ancestor check replaced it
//
// Run: go test -bench BenchmarkCheckBlockIsAncestorOfBlockDepth -run '^$' -benchmem ./stores/blockchain/sql/
func BenchmarkCheckBlockIsAncestorOfBlockDepth(b *testing.B) {
	tSettings := test.CreateBaseTestSettings(b)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(b, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(b, err)
	defer s.Close(context.Background())

	deadline := time.Now().Add(5 * time.Second)
	for s.mainChainRebuilding.Load() > 0 {
		if time.Now().After(deadline) {
			b.Fatal("startup rebuild did not complete in time")
		}
		time.Sleep(time.Millisecond)
	}

	const chainLength = 20_001

	ctx := context.Background()
	chain := []*model.Block{block1}

	_, _, err = s.StoreBlock(ctx, block1, "peer")
	require.NoError(b, err)

	for len(chain) < chainLength {
		next := createBlock3OnFork(chain[len(chain)-1])
		_, _, err = s.StoreBlock(ctx, next, "peer")
		require.NoError(b, err)
		chain = append(chain, next)
	}

	anchor := chain[len(chain)-1].Hash()

	// The fork block sits on the tip's parent. A new nonce keeps it from being the tip itself.
	forkBlock := createBlock3OnFork(chain[len(chain)-2])
	forkBlock.Header.Nonce = 7
	_, _, err = s.StoreBlock(ctx, forkBlock, "peer")
	require.NoError(b, err)

	forkAnchor := forkBlock.Hash()

	for _, depth := range []int{1, 2_000, 20_000} {
		var id uint32
		require.NoError(b, s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, chain[len(chain)-1-depth].Hash().CloneBytes()).Scan(&id))

		input := []uint32{id}

		b.Run(fmt.Sprintf("FastPath/depth=%d", depth), func(b *testing.B) {
			require.Zero(b, s.mainChainRebuilding.Load())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok, err := s.CheckBlockIsAncestorOfBlock(ctx, input, anchor)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})

		b.Run(fmt.Sprintf("Walk/depth=%d", depth), func(b *testing.B) {
			s.mainChainRebuilding.Add(1)
			defer s.mainChainRebuilding.Add(-1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok, err := s.CheckBlockIsAncestorOfBlock(ctx, input, anchor)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})

		b.Run(fmt.Sprintf("ForkFastPath/depth=%d", depth), func(b *testing.B) {
			require.Zero(b, s.mainChainRebuilding.Load())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok, err := s.CheckBlockIsAncestorOfBlock(ctx, input, forkAnchor)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})

		b.Run(fmt.Sprintf("ForkWalk/depth=%d", depth), func(b *testing.B) {
			s.mainChainRebuilding.Add(1)
			defer s.mainChainRebuilding.Add(-1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok, err := s.CheckBlockIsAncestorOfBlock(ctx, input, forkAnchor)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})

		b.Run(fmt.Sprintf("MainChain/depth=%d", depth), func(b *testing.B) {
			require.Zero(b, s.mainChainRebuilding.Load())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ok, err := s.checkBlockIsInCurrentChainSQL(ctx, input)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})
	}
}
