package subtreeprocessor

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// forkFixture is the prepare fixture with the fast path on and a tip, B, that the fixture's block
// competes with: both are children of genesis.
type forkFixture struct {
	*prepareFixture
	tip      *model.BlockHeader
	children int
}

func newForkFixture(t *testing.T) *forkFixture {
	t.Helper()

	f := &forkFixture{prepareFixture: newPrepareFixture(t)}
	f.stp.settings.BlockAssembly.ReorgFastPath = true

	f.tip = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  model.GenesisBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{9},
		Bits:           model.GenesisBlockHeader.Bits,
		Nonce:          99,
	}
	f.stp.currentBlockHeader.Store(f.tip)

	return f
}

// child is a block on top of parent whose two subtrees are stored and bound to its header.
func (f *forkFixture) child(t *testing.T, parent *chainhash.Hash) *model.Block {
	t.Helper()

	f.children++

	var subtrees []*subtreepkg.Subtree

	for s := 0; s < 2; s++ {
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)

		for i := 0; i < 4; i++ {
			if s == 0 && i == 0 {
				require.NoError(t, st.AddCoinbaseNode())
				continue
			}

			require.NoError(t, st.AddNode(chainhash.HashH([]byte(fmt.Sprintf("fork-child-%d-%d-%d", f.children, s, i))), 1, 1))
		}

		b, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, f.store.Set(context.Background(), st.RootHash().CloneBytes(), fileformat.FileTypeSubtree, b))

		subtrees = append(subtrees, st)
	}

	root0, err := subtrees[0].RootHashWithReplaceRootNode(f.block.CoinbaseTx.TxIDChainHash(), 0, uint64(f.block.CoinbaseTx.Size()))
	require.NoError(t, err)

	merkleRoot := chainhash.DoubleHashH(append(root0.CloneBytes(), subtrees[1].RootHash().CloneBytes()...))

	block, err := model.NewBlock(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent,
		HashMerkleRoot: &merkleRoot,
		Bits:           model.GenesisBlockHeader.Bits,
	}, f.block.CoinbaseTx, []*chainhash.Hash{subtrees[0].RootHash(), subtrees[1].RootHash()}, 8, 1000, 2, 0)
	require.NoError(t, err)

	return block
}

func forkEntries(stp *SubtreeProcessor) []*forkPrepared {
	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()

	return append([]*forkPrepared(nil), stp.preparedFork...)
}

// waitForkDone waits until the slot holds n entries and each is built or has failed.
func waitForkDone(t *testing.T, stp *SubtreeProcessor, n int) []*forkPrepared {
	t.Helper()

	require.Eventually(t, func() bool {
		entries := forkEntries(stp)
		if len(entries) != n {
			return false
		}

		for _, e := range entries {
			select {
			case <-e.done:
			default:
				return false
			}
		}

		return true
	}, 10*time.Second, 5*time.Millisecond)

	return forkEntries(stp)
}

// A competitor of the tip is prepared, and so is its child: the child's set holds both blocks, so a
// reorg onto the two reads neither again, and the competitor's own set is let go once it is built.
func TestPrepareForkBlock_PreparesCompetitorAndItsChild(t *testing.T) {
	f := newForkFixture(t)
	f2 := f.child(t, f.block.Hash())

	f.stp.PrepareBlock(f.block)

	entries := waitForkDone(t, f.stp, 1)
	require.NoError(t, entries[0].err)
	require.Equal(t, 8, entries[0].txMap.Length())
	require.Equal(t, []chainhash.Hash{*f.block.Hash()}, entries[0].blocks)

	f.stp.PrepareBlock(f2)

	require.Eventually(t, func() bool {
		e := forkEntries(f.stp)
		return len(e) == 1 && len(e[0].blocks) == 2
	}, 10*time.Second, 5*time.Millisecond)

	entries = waitForkDone(t, f.stp, 1)
	require.NoError(t, entries[0].err)
	require.Equal(t, []chainhash.Hash{*f.block.Hash(), *f2.Hash()}, entries[0].blocks)
	require.Equal(t, 15, entries[0].txMap.Length(), "both blocks' hashes, the coinbase placeholder once")

	for _, st := range f.subtrees {
		for _, n := range st.Nodes {
			require.True(t, entries[0].txMap.Exists(n.Hash))
		}
	}
}

// Nothing is prepared for a fork unless the fast path is on, and only a competitor of the tip or a
// child of one is: not the tip itself, not a block somewhere else.
func TestPrepareForkBlock_OnlyCompetitorsOfTheTip(t *testing.T) {
	f := newForkFixture(t)
	f2 := f.child(t, f.block.Hash())

	f.stp.settings.BlockAssembly.ReorgFastPath = false
	f.stp.PrepareBlock(f.block)
	require.Empty(t, forkEntries(f.stp), "the fast path is off")

	f.stp.settings.BlockAssembly.ReorgFastPath = true

	f.stp.PrepareBlock(f2)
	require.Empty(t, forkEntries(f.stp), "a child of a block that is not prepared")

	// a block with the tip's own header is the tip, not a competitor
	tipBlock, err := model.NewBlock(f.tip, f.block.CoinbaseTx, f.block.Subtrees, 8, 1000, 1, 0)
	require.NoError(t, err)
	f.stp.PrepareBlock(tipBlock)
	require.Empty(t, forkEntries(f.stp))

	// a block on top of the tip is the tip's child, not a fork
	f.stp.currentBlockHeader.Store(f.block.Header)
	f.stp.PrepareBlock(f2)
	require.Empty(t, forkEntries(f.stp))
}

// The slot gives its set only for the same blocks, in order, with the same subtree lists.
func TestPrepareForkBlock_TakenByHashAndSubtreeList(t *testing.T) {
	f := newForkFixture(t)
	f2 := f.child(t, f.block.Hash())

	f.stp.PrepareBlock(f.block)
	waitForkDone(t, f.stp, 1)
	f.stp.PrepareBlock(f2)
	require.Eventually(t, func() bool { e := forkEntries(f.stp); return len(e) == 1 && len(e[0].blocks) == 2 }, 10*time.Second, 5*time.Millisecond)
	waitForkDone(t, f.stp, 1)

	otherList := f.withSubtrees(t, f.subtrees[0].RootHash(), f.spare.RootHash())

	for name, blocks := range map[string][]*model.Block{
		"only the first":     {f.block},
		"another list":       {otherList, f2},
		"wrong order":        {f2, f.block},
		"another block":      {f.block, f.child(t, f.block.Hash())},
		"one block too many": {f.block, f2, f2},
	} {
		// every attempt takes the slot, so a fresh one each time
		if len(forkEntries(f.stp)) == 0 {
			f.stp.PrepareBlock(f.block)
			waitForkDone(t, f.stp, 1)
			f.stp.PrepareBlock(f2)
			require.Eventually(t, func() bool { e := forkEntries(f.stp); return len(e) == 1 && len(e[0].blocks) == 2 }, 10*time.Second, 5*time.Millisecond)
			waitForkDone(t, f.stp, 1)
		}

		set, _, ok := f.stp.takePreparedForkSet(blocks)
		require.False(t, ok, name)
		require.Nil(t, set, name)
		require.Empty(t, forkEntries(f.stp), "%s: the slot is let go either way", name)
	}

	f.stp.PrepareBlock(f.block)
	waitForkDone(t, f.stp, 1)
	f.stp.PrepareBlock(f2)
	require.Eventually(t, func() bool { e := forkEntries(f.stp); return len(e) == 1 && len(e[0].blocks) == 2 }, 10*time.Second, 5*time.Millisecond)
	waitForkDone(t, f.stp, 1)

	set, conflicting, ok := f.stp.takePreparedForkSet([]*model.Block{f.block, f2})
	require.True(t, ok)
	require.Equal(t, 15, set.Length())
	require.Empty(t, conflicting)
	require.Empty(t, forkEntries(f.stp))
}

// A set that is still being built is not waited for: a reorg takes the rebuild at full width.
func TestPrepareForkBlock_UnfinishedSetIsNotWaitedFor(t *testing.T) {
	f := newForkFixture(t)

	f.gated.hold()
	defer f.gated.release()

	f.stp.PrepareBlock(f.block)
	require.Len(t, forkEntries(f.stp), 1)

	_, _, ok := f.stp.takePreparedForkSet([]*model.Block{f.block})
	require.False(t, ok)
	require.Empty(t, forkEntries(f.stp))
}

// The sets belong to the tip they were prepared against: when the tip moves they are let go, whether
// the next block is added on top of the tip or a reorg is applied, and one that is asked for after the
// tip moved is not given.
func TestPrepareForkBlock_DroppedWhenTheTipMoves(t *testing.T) {
	f := newForkFixture(t)

	f.stp.PrepareBlock(f.block)
	waitForkDone(t, f.stp, 1)

	// a block added on top of the tip lets go of them
	_, _, err := f.stp.createTransactionMapIfNeeded(context.Background(), f.block, map[chainhash.Hash]int{})
	require.NoError(t, err)
	require.Empty(t, forkEntries(f.stp))

	f.stp.PrepareBlock(f.block)
	waitForkDone(t, f.stp, 1)

	// the tip moved some other way: the set is not given, and a hint for another block lets go of it
	f.stp.currentBlockHeader.Store(f.block.Header)

	set, _, ok := f.stp.takePreparedForkSet([]*model.Block{f.block})
	require.False(t, ok)
	require.Nil(t, set)

	f.stp.currentBlockHeader.Store(f.tip)
	f.stp.PrepareBlock(f.block)
	stale := waitForkDone(t, f.stp, 1)[0]

	f.stp.currentBlockHeader.Store(f.block.Header)

	f.stp.PrepareBlock(f.child(t, model.GenesisBlockHeader.Hash())) // a competitor of the new tip
	entries := waitForkDone(t, f.stp, 1)
	require.NotSame(t, stale, entries[0], "the set prepared against the old tip is gone")
	require.Equal(t, *f.block.Hash(), entries[0].tip)
}

// A set is built only if the live heap, two sets and the pending set's 48 bytes a transaction fit in
// three quarters of the memory limit. Sized from the bound body, not the block's claimed count.
func TestPrepareForkBlock_NotBuiltWhenTheMemoryGateFails(t *testing.T) {
	const txCount = 8 // the fixture block's two subtrees

	need := uint64(2*txCount*fastPathSetBytesPerHash + 5*fastPathBytesPerPending)

	cases := []struct {
		name  string
		live  uint64
		limit int64
		built bool
	}{
		{"exactly at three quarters", 999, int64((999 + need) / 3 * 4), true},
		{"a little short", 999, int64((999+need)/3*4) - 4, false},
		{"limit unset", 1 << 60, 1 << 62, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newForkFixture(t)

			for i := 0; i < 5; i++ {
				_, inserted := f.stp.currentTxMap.SetIfNotExists(chainhash.HashH([]byte{byte(i)}), &subtreepkg.TxInpoints{})
				require.True(t, inserted)
			}

			reads := f.gated.reads.Load()

			old := reorgMemoryStats
			reorgMemoryStats = func() (uint64, int64) { return tc.live, tc.limit }

			defer func() { reorgMemoryStats = old }()

			// the claimed count says a billion: the gate sizes from the bound body
			f.block.TransactionCount = 1 << 30

			f.stp.PrepareBlock(f.block)

			if tc.built {
				entries := waitForkDone(t, f.stp, 1)
				require.NoError(t, entries[0].err)
				require.NotNil(t, entries[0].txMap)

				return
			}

			require.Eventually(t, func() bool { return len(forkEntries(f.stp)) == 0 }, 10*time.Second, 5*time.Millisecond)
			require.Equal(t, int32(2), f.gated.reads.Load()-reads, "only the bind read the subtrees, no set was built")
		})
	}
}

// The set is built on a quarter of the cores at most, so a prepare leaves the rest to intake.
func TestForkPrepareWorkers_IsAQuarterOfTheCores(t *testing.T) {
	require.Equal(t, max(1, runtime.GOMAXPROCS(0)/4), forkPrepareWorkers())

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	require.Equal(t, 1, forkPrepareWorkers())
	runtime.GOMAXPROCS(16)
	require.Equal(t, 4, forkPrepareWorkers())
}

// A subtree that is only in the to-check state is verified before it goes into a set: a body whose
// leaf was changed, under the right root, is not prepared. The bind, which reads the first and last
// subtree, goes by the root at the head of the file and does not see it.
func TestPrepareForkBlock_ToCheckSubtreeWithChangedLeafIsRejected(t *testing.T) {
	f := newForkFixture(t)
	ctx := context.Background()

	key := f.subtrees[1].RootHash().CloneBytes()
	body, err := f.store.Get(ctx, key, fileformat.FileTypeSubtree)
	require.NoError(t, err)

	body[56+48+3] ^= 0xff // the second leaf's hash

	require.NoError(t, f.store.Del(ctx, key, fileformat.FileTypeSubtree))
	require.NoError(t, f.store.Set(ctx, key, fileformat.FileTypeSubtreeToCheck, body))

	f.stp.PrepareBlock(f.block)

	require.Eventually(t, func() bool { return len(forkEntries(f.stp)) == 0 }, 10*time.Second, 5*time.Millisecond)

	// the same body in the subtree store was checked before it was written there
	require.NoError(t, f.store.Del(ctx, key, fileformat.FileTypeSubtreeToCheck))
	require.NoError(t, f.store.Set(ctx, key, fileformat.FileTypeSubtree, body))

	f.stp.PrepareBlock(f.block)
	entries := waitForkDone(t, f.stp, 1)
	require.NoError(t, entries[0].err)
}
