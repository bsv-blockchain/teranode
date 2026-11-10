package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// A failed fork reorg keeps the subtrees of its last completed step, and a step
// that fails part way puts back its own input, so the reset block assembly falls
// back to sweeps the pre-reorg mempool. m1 is in block assembly before the reorg,
// but its record says it was mined in A2, a block block assembly never moved onto,
// so the reload would not find it: only the sweep's mark brings it back. a1 is
// mined in the moved-back block A. The reorg fails inside the first forward step,
// after its reset replaced the subtrees. After block assembly's reset both m1 and
// a1 must be in block assembly and marked unmined.
//
// The subtree processor's own tests cover the other failure points. This one
// cannot fail a step before its reset, as they do, by deleting F's subtree file:
// block assembly's reset reads that file too.
func TestFailedForkReorg_ResetKeepsThePreReorgMempoolAndTheMovedBackTx(t *testing.T) {
	f := newFailedForkReorgBA(t)

	// m1 spends a parent mined in P, so the reload's input check accepts it.
	mParent := newTestTx(t, 0x60)
	_, _, err := f.utxo.SpendAndCreate(t.Context(), mParent, 0, utxo.WithCreateOnly())
	require.NoError(t, err)

	parentMeta := f.meta(t, f.parent)
	_, err = f.utxo.SetMinedMulti(t.Context(), []*chainhash.Hash{mParent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: parentMeta.ID, BlockHeight: parentMeta.Height, OnLongestChain: true})
	require.NoError(t, err)

	m1 := newTestChild(t, mParent, 0x61)
	_, _, err = f.utxo.SpendAndCreate(t.Context(), m1, 2)
	require.NoError(t, err)

	m1Inpoints, err := subtreepkg.NewTxInpointsFromTx(m1)
	require.NoError(t, err)

	f.stp.AddBatch([]subtreepkg.Node{{Hash: *m1.TxIDChainHash(), Fee: 100, SizeInBytes: uint64(m1.Size())}}, []*subtreepkg.TxInpoints{&m1Inpoints})
	require.Eventually(t, func() bool { return f.assemblyHas(t, m1) }, 5*time.Second, 10*time.Millisecond, "m1 must reach block assembly before the reorg")

	// m1's record now says it is mined in A2, which block assembly never
	// moved onto: the reload reads only records still marked unmined.
	a2Meta := f.meta(t, f.childA2)
	_, err = f.utxo.SetMinedMulti(t.Context(), []*chainhash.Hash{m1.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: a2Meta.ID, BlockHeight: a2Meta.Height, OnLongestChain: true})
	require.NoError(t, err)
	require.Zero(t, unminedSinceOfTx(t, f.utxo, m1), "precondition: the reload would not find m1")

	f.hook.Store(func() *func() {
		fn := func() {
			// a1 is first in the remainder, so its missing entry fails the
			// sequential pass after resetSubtreeState replaced the subtrees
			require.True(t, f.stp.GetCurrentTxMap().Delete(*f.a1.TxIDChainHash()))
		}

		return &fn
	}())

	err = f.ba.moveToBlock(t.Context(), f.forkF2, f.meta(t, f.forkF2))
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset, "the failed fork reorg falls back to a reset")

	tip, _ := f.ba.CurrentBlock()
	require.Equal(t, f.forkF2.Hash(), tip.Hash())
	require.Equal(t, f.forkF2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

	require.True(t, f.assemblyHas(t, m1), "the sweep marked m1 unmined, so the reload put it back")
	require.NotZero(t, unminedSinceOfTx(t, f.utxo, m1))
	require.True(t, f.assemblyHas(t, f.a1), "a1 is back in block assembly once A is off the chain")
	require.NotZero(t, unminedSinceOfTx(t, f.utxo, f.a1), "a1 is marked unmined")
	require.False(t, f.assemblyHas(t, f.f1), "f1 is mined by F")
	require.NoError(t, f.stp.CheckSubtreeProcessor())
}
