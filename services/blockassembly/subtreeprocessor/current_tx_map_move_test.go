package subtreeprocessor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// getOnlyTxInpointsMap hides every method but the TxInpointsMap interface, so
// the processor cannot take the raw-copy fast path between two arena maps and
// takes the generic Get + insert path instead. That path is what the code did
// for every map before the arena.
type getOnlyTxInpointsMap struct{ TxInpointsMap }

// TestParallelGetAndSetIfNotExists_ArenaMoveMatchesGenericPath moves the same
// nodes with the raw-copy path and with the generic path, over duplicates, nodes
// already in the destination, removeMap hits and the coinbase placeholder, and
// requires the same wasInserted, the same destination contents and the same
// error for a node the source does not have.
func TestParallelGetAndSetIfNotExists_ArenaMoveMatchesGenericPath(t *testing.T) {
	const (
		txs       = 6000
		buckets   = 64
		remainder = 5000 // nodes[0:remainder] are moved
	)

	run := func(t *testing.T, wrap func(TxInpointsMap) TxInpointsMap, missing bool) ([]bool, map[chainhash.Hash]*subtreepkg.TxInpoints, error) {
		stp := newRemainderFanOutProcessor(t)

		src := NewSplitTxInpointsMap(buckets)
		dst := NewSplitTxInpointsMap(buckets)
		stp.currentTxMap = dst

		nodes := make([]subtreepkg.Node, 0, remainder+1)
		nodes = append(nodes, subtreepkg.Node{Hash: *subtreepkg.CoinbasePlaceholderHash})

		for i := uint64(0); i < txs; i++ {
			src.Set(arenaHash(i), buildInpoints(shapeThreeParents, i))
		}

		for i := uint64(0); i < remainder; i++ {
			nodes = append(nodes, subtreepkg.Node{Hash: arenaHash(i), Fee: i})
		}

		// duplicates inside the batch, ones the destination already holds, ones on the removeMap
		nodes = append(nodes, nodes[10], nodes[20])

		dst.Set(arenaHash(30), buildInpoints(shapeOneParent, 999))
		require.NoError(t, stp.removeMap.Put(arenaHash(40), 1))

		if missing {
			nodes = append(nodes, subtreepkg.Node{Hash: arenaHash(1 << 50)})
		}

		wasInserted := make([]bool, len(nodes))
		err := stp.parallelGetAndSetIfNotExists(nodes, wrap(src), stp.removeMap.Length(), 8, wasInserted)

		got := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

		for i := uint64(0); i < txs; i++ {
			if v, ok := dst.Get(arenaHash(i)); ok {
				got[arenaHash(i)] = v
			}
		}

		return wasInserted, got, err
	}

	for _, missing := range []bool{false, true} {
		fastIns, fastDst, fastErr := run(t, func(m TxInpointsMap) TxInpointsMap { return m }, missing)
		slowIns, slowDst, slowErr := run(t, func(m TxInpointsMap) TxInpointsMap { return getOnlyTxInpointsMap{m} }, missing)

		if missing {
			require.Error(t, fastErr)
			require.Error(t, slowErr)
			require.Equal(t, slowErr.Error(), fastErr.Error(), "the same error for a node the source does not have")

			continue
		}

		require.NoError(t, fastErr)
		require.NoError(t, slowErr)
		require.Equal(t, slowIns, fastIns)
		require.Equal(t, slowDst, fastDst)
		require.Equal(t, buildInpoints(shapeOneParent, 999), fastDst[arenaHash(30)], "a node the destination holds keeps its inpoints")
		require.False(t, fastIns[31], "the pre-existing destination entry is reported as not inserted")
	}
}

// TestCapturedHalfIsSafeWhileTheOwnerSwapsAndClears is the service-level
// lifetime proof. The subtree meta builder runs on its own goroutine with the
// currentTxMap that was active when the subtree completed; the processor swaps
// halves and clears the retired one when the next block is applied, without
// waiting for that goroutine. Readers hammer every half the processor hands out
// while blocks are moved forward, and must only ever see the right record or a
// miss. Run with -race.
func TestCapturedHalfIsSafeWhileTheOwnerSwapsAndClears(t *testing.T) {
	stp, block, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	// every tx's inpoints are a pure function of its hash, so a reader can check any record it gets
	inpointsFor := func(h chainhash.Hash) *subtreepkg.TxInpoints {
		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH(h[:]), h}, []uint32{1, 3, 2, 4, 5})
		return &p
	}

	var (
		halves   sync.Map // *SplitTxInpointsMap captured from subtree requests and from the processor
		stop     atomic.Bool
		hits     atomic.Int64
		readerWg sync.WaitGroup
		hashes   []chainhash.Hash
	)

	for i := uint64(0); i < 40; i++ {
		hashes = append(hashes, arenaHash(i))
	}

	// a tx that is in the block, so the block has something to remove
	inBlock, err := chainhash.NewHashFromStr("6affcabb2013261e764a5d4286b463b11127f4fd1de05368351530ddb3f19942")
	require.NoError(t, err)

	hashes = append(hashes, *inBlock)

	// answer subtree requests the way block assembly does, and remember the map each one captured
	go func() {
		for req := range newSubtreeChan {
			if m, ok := req.ParentTxMap.(*SplitTxInpointsMap); ok {
				halves.Store(m, struct{}{})
			}

			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	halves.Store(stp.currentTxMap.(*SplitTxInpointsMap), struct{}{})
	halves.Store(stp.currentTxMapShadow, struct{}{})

	for r := 0; r < 4; r++ {
		readerWg.Add(1)

		go func() {
			defer readerWg.Done()

			for !stop.Load() {
				halves.Range(func(k, _ any) bool {
					m := k.(*SplitTxInpointsMap)

					for _, h := range hashes {
						if got, ok := m.Get(h); ok {
							hits.Add(1)

							if !equalInpoints(inpointsFor(h), got) {
								t.Errorf("Get returned a wrong record for %s", h)
								stop.Store(true)
							}
						}

						if b, found, err := m.AppendMetaInpoints(nil, h); found && err == nil {
							want, _ := inpointsFor(h).Serialize()
							if string(b) != string(want) {
								t.Errorf("AppendMetaInpoints returned a wrong record for %s", h)
								stop.Store(true)
							}
						}
					}

					return !stop.Load()
				})
			}
		}()
	}

	for round := 0; round < 30 && !stop.Load(); round++ {
		nodes := make([]subtreepkg.Node, 0, len(hashes))
		inpoints := make([]*subtreepkg.TxInpoints, 0, len(hashes))

		for _, h := range hashes {
			nodes = append(nodes, subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250})
			inpoints = append(inpoints, inpointsFor(h))
		}

		stp.AddBatch(nodes, inpoints)

		// every round moves a new block, on the tip the previous one left
		header := *block.Header
		header.HashPrevBlock = stp.currentBlockHeader.Load().Hash()
		header.Nonce = uint32(round) //nolint:gosec // small test count
		block.Header = &header

		errChan := make(chan error, 1)
		stp.handleMoveForwardRequest(context.Background(), moveBlockRequest{block: block, errChan: errChan})
		require.NoError(t, <-errChan, "round %d", round)

		halves.Store(stp.currentTxMap.(*SplitTxInpointsMap), struct{}{})
		halves.Store(stp.currentTxMapShadow, struct{}{})
	}

	stop.Store(true)
	readerWg.Wait()

	require.NotZero(t, hits.Load(), "the readers never hit a record, the test proves nothing")
}

// TestMoveForwardBlock_RemainderKeepsInpointsExactly applies a block that
// leaves txs of every inpoints shape behind in the chained subtrees, on the
// sequential and on the parallel remainder path, and requires the new half to
// hold exactly the inpoints each tx had, the retired half to be empty, and every
// tx to be in a subtree.
func TestMoveForwardBlock_RemainderKeepsInpointsExactly(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int
	}{
		{"sequential remainder path", 1 << 30},
		{"parallel remainder path", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stp, block, newSubtreeChan := newDeferredDrainProcessor(t, nil, func(s *settings.Settings) {
				s.BlockAssembly.ParallelSetIfNotExistsThreshold = tc.threshold
			})

			go func() {
				for req := range newSubtreeChan {
					if req.ErrChan != nil {
						req.ErrChan <- nil
					}
				}
			}()

			want := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

			for i := uint64(0); i < 60; i++ {
				shape := inpointsShape(1 + i%uint64(numShapes-1)) // nil pointers are not valid for addNode
				if shape == shapeNilPointer || shape == shapeManyVouts || shape == shapeHugeParentCount || shape == shapeZeroParentsVouts {
					shape = shapeThreeParents
				}

				p := buildInpoints(shape, i)
				p.SubtreeIndex = 0 // the in-memory map does not track it, the processor never sets it

				h := arenaHash(i)
				want[h] = p

				require.NoError(t, stp.addNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250}, p, true))
			}

			require.NotEmpty(t, stp.chainedSubtrees, "the txs must have filled subtrees, or there is no remainder to move")

			errChan := make(chan error, 1)
			stp.handleMoveForwardRequest(context.Background(), moveBlockRequest{block: block, errChan: errChan})
			require.NoError(t, <-errChan)

			inSubtrees := subtreeHashes(stp)

			for h, p := range want {
				got, ok := stp.currentTxMap.Get(h)
				require.True(t, ok, "tx %s must still be in the current map", h)
				require.Equal(t, p, got, "tx %s", h)

				_, ok = inSubtrees[h]
				require.True(t, ok, "tx %s must still be in a subtree", h)
			}

			require.Equal(t, len(want), stp.currentTxMap.Length())
			require.Zero(t, stp.currentTxMapShadow.Length(), "the retired half must be cleared")
		})
	}
}
