package subtreeprocessor

import (
	"context"
	"fmt"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// forkLeaves lays out one block of two subtrees of 64 leaves, the first led by the coinbase
// placeholder. Each slot takes a hash of the shared pool with probability overlap, so blocks that
// draw from one pool overlap, and a block that draws the same hash twice holds a duplicate; the
// other slots get hashes nobody else has.
func forkLeaves(r *rand.Rand, pool []chainhash.Hash, overlap float64, fresh *int) [][]chainhash.Hash {
	leaves := [][]chainhash.Hash{{subtreepkg.CoinbasePlaceholderHashValue}, {}}

	for s := range leaves {
		for len(leaves[s]) < 64 {
			if r.Float64() < overlap {
				leaves[s] = append(leaves[s], pool[r.Intn(len(pool))])
				continue
			}

			*fresh++
			leaves[s] = append(leaves[s], chainhash.HashH([]byte(fmt.Sprintf("fork-fresh-%d", *fresh))))
		}
	}

	return leaves
}

// With the lost block probed against the first winner's set alone, and a mask of that set's hashes in
// the union, the delta against the union is exactly what computeReorgDelta finds by reading the lost
// block again: D1 with its indices, D2 in order, and the hits. With one winner the probe is the answer.
func TestReorgScenario_ForkLostDeltaMatchesComputeReorgDelta(t *testing.T) {
	stp, store := newTxMapTestProcessor(t)
	ctx := context.Background()
	pool := randHashes(150, 7)
	fresh := 0
	seed := int64(0)

	for _, winners := range []int{1, 2} {
		for _, oLost := range []float64{0, 0.3, 0.7, 1} {
			for _, oFirst := range []float64{0, 0.5, 1} {
				for _, oSecond := range []float64{0, 0.5, 1} {
					seed++

					t.Run(fmt.Sprintf("winners=%d/lost=%.1f/first=%.1f/second=%.1f", winners, oLost, oFirst, oSecond), func(t *testing.T) {
						r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
						lost := storeDeltaBlock(t, store, forkLeaves(r, pool, oLost, &fresh))
						blocks := []*model.Block{storeDeltaBlock(t, store, forkLeaves(r, pool, oFirst, &fresh))}

						if winners == 2 {
							blocks = append(blocks, storeDeltaBlock(t, store, forkLeaves(r, pool, oSecond, &fresh)))
						}

						first, _, err := stp.buildUnionSet(ctx, blocks[:1])
						require.NoError(t, err)

						union, _, err := stp.buildUnionSet(ctx, blocks)
						require.NoError(t, err)

						pre, err := stp.computeReorgDelta(ctx, []*model.Block{lost}, first, 4)
						require.NoError(t, err)

						want, err := stp.computeReorgDelta(ctx, []*model.Block{lost}, union, 4)
						require.NoError(t, err)

						got := pre
						if winners == 2 {
							got = deriveForkDelta(pre, markFirstSet(first, union, 4), union, 4)
						}

						require.Equal(t, want.Hits, got.Hits)
						require.True(t, slices.Equal(want.D1, got.D1), "D1: want %d got %d", len(want.D1), len(got.D1))
						require.True(t, slices.Equal(want.D2, got.D2), "D2: want %d got %d", len(want.D2), len(got.D2))
					})
				}
			}
		}
	}
}

// lostBytesRead is how many bytes have been read from the lost block's subtrees.
func (s *keyLogStore) lostBytesRead(sc *reorgScenario) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, st := range sc.lost.Subtrees {
		n += s.read[*st]
	}

	return n
}

// lostFileBytes is the size of the lost block's subtree files.
func lostFileBytes(t testing.TB, sc *reorgScenario) int {
	t.Helper()

	n := 0

	for _, st := range sc.lost.Subtrees {
		b, err := sc.blobStore.Get(context.Background(), st[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)

		n += len(b)
	}

	return n
}

// reorgReadingTheLostBlock runs the fast path after prepare and returns the bytes it read from the lost
// block's subtrees. The reorg must end as the slow path does.
func reorgReadingTheLostBlock(t *testing.T, cfg reorgScenarioConfig, prepare func(*reorgScenario)) (read, fileBytes int) {
	t.Helper()

	requireReorgEquivalent(t, cfg, slowReference, func(t testing.TB, sc *reorgScenario) error {
		prepare(sc)

		log := logSubtreeReads(sc)
		err := fastReorgPath(t, sc)
		read, fileBytes = log.lostBytesRead(sc), lostFileBytes(t, sc)

		return err
	})

	return read, fileBytes
}

// With the lost block probed while it was the tip, the reorg does not read it again. What it still reads
// of it are two reads that exist without the probe: the conflicting nodes of every subtree
// (collectMoveBackConflictingNodes, once each) and the subtrees that hold effective D1
// (readEffectiveD1, once each). computeReorgDelta's read of every subtree is the one that goes.
func TestReorgScenario_ForkLostDeltaSavesTheReadOfTheLostBlock(t *testing.T) {
	for _, forward := range []int{1, 2} {
		for _, inWinners := range []float64{1, 0.9, 0.2} {
			t.Run(fmt.Sprintf("forward=%d/lostInWinners=%.1f", forward, inWinners), func(t *testing.T) {
				cfg := smallScenarioConfig()
				cfg.forwardBlocks, cfg.lostInWinners = forward, inWinners

				if inWinners == 1 {
					// nothing of the lost block is raw D1, pending or not
					cfg.lostAlsoPending, cfg.pendingSpendsLost, cfg.pendingChains = 0, 0, 0
				}

				control, fileBytes := reorgReadingTheLostBlock(t, cfg, func(sc *reorgScenario) { sc.prepareForwardSet(t, forward) })
				withDelta, _ := reorgReadingTheLostBlock(t, cfg, func(sc *reorgScenario) { sc.prepareForwardSetWithLostDelta(t, forward) })

				require.Equal(t, control-fileBytes, withDelta, "the delta's read of every lost subtree is gone")

				if inWinners == 1 {
					require.Equal(t, fileBytes, withDelta, "no effective D1: only the conflicting-node read is left, one pass")
				}
			})
		}
	}
}

// A probe for other blocks is not used: the reorg reads the lost block as without one.
func TestReorgScenario_ForkLostDeltaIgnoredForOtherWinners(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.forwardBlocks = 2

	control, _ := reorgReadingTheLostBlock(t, cfg, func(sc *reorgScenario) { sc.prepareForwardSet(t, 2) })
	other, _ := reorgReadingTheLostBlock(t, cfg, func(sc *reorgScenario) { sc.prepareForwardSetWithLostDelta(t, 1) })

	require.Equal(t, control, other, "the set is of the first winner only: the winners are not taken, nor is the probe")

	// the probe of another lost block, or against another first winner, is not used either
	requireReorgEquivalent(t, cfg, slowReference, func(t testing.TB, sc *reorgScenario) error {
		sc.prepareForwardSetWithLostDelta(t, 2)

		e := forkEntries(sc.stp)[0]
		require.NotNil(t, e.lostDelta(sc.lost, e.txMap, 2))

		l := e.lost
		l.lost = chainhash.Hash{1}
		require.Nil(t, e.lostDelta(sc.lost, e.txMap, 2), "another lost block")

		l.lost, l.first = *sc.lost.Hash(), chainhash.Hash{2}
		require.Nil(t, e.lostDelta(sc.lost, e.txMap, 2), "another first winner")

		l.first, e.firstMask = e.blocks[0], nil
		require.Nil(t, e.lostDelta(sc.lost, e.txMap, 2), "a second winner without the mask")

		return fastReorgPath(t, sc)
	})
}

// The probe is kept only if the lost block is still the tip when it finishes.
func TestReorgScenario_ForkLostDeltaDiscardedWhenTheTipMoves(t *testing.T) {
	requireReorgEquivalent(t, smallScenarioConfig(), slowReference, func(t testing.TB, sc *reorgScenario) error {
		sc.prepareForwardSet(t, 1)

		first := forkEntries(sc.stp)[0]
		first.lost = newForkLostDelta()

		sc.stp.currentBlockHeader.Store(sc.forward[0].Header)
		require.Error(t, sc.stp.probeLostAgainstFirst(context.Background(), first, sc.lost))
		require.Nil(t, first.lost.delta)

		sc.stp.currentBlockHeader.Store(sc.lost.Header)

		return fastReorgPath(t, sc)
	})
}

// waitLostDelta waits until the probe of the lost block has finished.
func waitLostDelta(t *testing.T, l *forkLostDelta) {
	t.Helper()

	select {
	case <-l.done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the probe of the lost block did not finish")
	}
}

// preparedAs prepares the competitor and its child through PrepareBlock while the tip's block is
// served, and returns the child's entry once built.
func (f *forkFixture) preparedAs(t *testing.T, lost *model.Block) (competitor, child *forkPrepared, childBlock *model.Block) {
	t.Helper()

	f.serveTip(lost)
	childBlock = f.child(t, f.block.Hash())

	f.stp.PrepareBlock(f.block)
	competitor = waitForkDone(t, f.stp, 1)[0]
	waitLostDelta(t, competitor.lost)

	f.stp.PrepareBlock(childBlock)
	require.Eventually(t, func() bool { e := forkEntries(f.stp); return len(e) == 1 && len(e[0].blocks) == 2 }, 10*time.Second, 5*time.Millisecond)

	return competitor, waitForkDone(t, f.stp, 1)[0], childBlock
}

// The competitor's set is probed with the tip's block, and its child's entry takes the probe with a
// mask of the competitor's hashes, so a reorg onto both has the delta without reading the tip.
func TestForkLostDelta_CompetitorAndChildCarryTheProbe(t *testing.T) {
	f := newForkFixture(t)
	lost := f.child(t, model.GenesisBlockHeader.Hash())

	competitor, child, childBlock := f.preparedAs(t, lost)

	require.True(t, competitor.lost.ready())
	require.Same(t, competitor.lost, child.lost)
	require.NotNil(t, child.firstMask)

	taken := f.stp.takePreparedFork([]*model.Block{f.block, childBlock})
	require.Same(t, child, taken)

	got := taken.lostDelta(lost, taken.txMap, 2)
	require.NotNil(t, got)

	want, err := f.stp.computeReorgDelta(context.Background(), []*model.Block{lost}, taken.txMap, 2)
	require.NoError(t, err)
	require.Equal(t, want.Hits, got.Hits)
	require.True(t, slices.Equal(want.D1, got.D1))
	require.True(t, slices.Equal(want.D2, got.D2))
}

// A child whose competitor's probe has not finished does not wait for it and does not keep it: the
// probe is stopped and the child is prepared as without one.
func TestForkLostDelta_ChildDoesNotWaitForTheProbe(t *testing.T) {
	f := newForkFixture(t)
	lost := f.child(t, model.GenesisBlockHeader.Hash())

	gate := make(chan struct{})
	client := &blockchain.Mock{}
	client.On("GetBlock", mock.Anything, mock.Anything).Run(func(mock.Arguments) { <-gate }).Return(lost, nil)
	f.stp.blockchainClient = client

	childBlock := f.child(t, f.block.Hash())

	f.stp.PrepareBlock(f.block)
	competitor := waitForkDone(t, f.stp, 1)[0]

	f.stp.PrepareBlock(childBlock)
	require.Eventually(t, func() bool { e := forkEntries(f.stp); return len(e) == 1 && len(e[0].blocks) == 2 }, 10*time.Second, 5*time.Millisecond)

	child := waitForkDone(t, f.stp, 1)[0]
	require.NoError(t, child.err)
	require.Nil(t, child.lost)
	require.Nil(t, child.firstMask)
	require.Error(t, competitor.lost.ctx.Err(), "the probe is stopped")

	close(gate)
	waitLostDelta(t, competitor.lost)
	require.False(t, competitor.lost.ready())
	require.Nil(t, child.lostDelta(lost, child.txMap, 2))
}

// The probe is let go with the slot: when the tip moves and another competitor is hinted, when the
// slot is released, and not while the child's entry still holds it after the competitor's entry is gone.
func TestForkLostDelta_LetGoWithTheSlot(t *testing.T) {
	f := newForkFixture(t)
	lost := f.child(t, model.GenesisBlockHeader.Hash())

	competitor, child, _ := f.preparedAs(t, lost)
	require.NoError(t, competitor.lost.ctx.Err(), "the competitor's entry is gone, its child holds the probe")

	f.stp.releaseForkPrepared()
	require.Error(t, child.lost.ctx.Err())

	// the tip moves: a hint for a competitor of the new tip lets go of the old fork's probe
	f.stp.currentBlockHeader.Store(f.tip)
	f.stp.PrepareBlock(f.block)

	stale := waitForkDone(t, f.stp, 1)[0]
	waitLostDelta(t, stale.lost)

	f.stp.currentBlockHeader.Store(f.block.Header)
	f.stp.PrepareBlock(f.child(t, model.GenesisBlockHeader.Hash()))
	waitForkDone(t, f.stp, 1)
	require.Error(t, stale.lost.ctx.Err())
}

// The probe's room is gated: a set that fits but a probe that does not leaves the competitor prepared
// without one.
func TestForkLostDelta_NotProbedWhenTheMemoryGateFails(t *testing.T) {
	f := newForkFixture(t)
	f.serveTip(f.child(t, model.GenesisBlockHeader.Hash()))

	const txCount = 8

	setNeed := 2*txCount*fastPathSetBytesPerHash + f.stp.TxCount()*fastPathBytesPerPending

	old := reorgMemoryStats
	reorgMemoryStats = func() (uint64, int64) { return 0, int64(setNeed/3*4 + 8) }

	defer func() { reorgMemoryStats = old }()

	f.stp.PrepareBlock(f.block)

	competitor := waitForkDone(t, f.stp, 1)[0]
	require.NoError(t, competitor.err)
	waitLostDelta(t, competitor.lost)
	require.False(t, competitor.lost.ready())
	require.Nil(t, competitor.lostDelta(f.block, competitor.txMap, 2))
}
