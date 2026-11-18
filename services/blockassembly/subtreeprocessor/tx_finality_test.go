package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// Regtest: CSV at 576, so next height 600 measures a time lock against the median time past,
// and [576, 1250] shares its transaction rules.
const (
	finalityTestHeight = 600
	finalityTestMTP    = 1_700_000_000
)

// newFinalityTestProcessor returns a processor whose candidate is (600, MTP 1_700_000_000) on
// its current block.
func newFinalityTestProcessor(t *testing.T, ctx context.Context) *SubtreeProcessor {
	t.Helper()

	stp := newTestSubtreeProcessorForDequeueStaleness(t, ctx)
	stp.InitCurrentBlockHeader(blockHeader)
	stp.SetTxEra(finalityTestHeight)
	stp.SetTxCandidate(blockHeader.Hash(), finalityTestHeight, finalityTestMTP)

	return stp
}

func inTemplate(stp *SubtreeProcessor, hash chainhash.Hash) bool {
	_, ok := stp.currentTxMap.Get(hash)
	return ok
}

// nextFinalityHeader is a block on top of blockHeader, for moving the candidate on.
func nextFinalityHeader() *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      blockHeader.Timestamp + 1,
		Bits:           blockHeader.Bits,
		Nonce:          7,
	}
}

// moveFinalityCandidate moves the processor to the next block and the candidate with it, as
// block assembly does after a block.
func moveFinalityCandidate(stp *SubtreeProcessor, height, medianTimePast uint32) {
	next := nextFinalityHeader()
	stp.currentBlockHeader.Store(next)
	stp.SetTxCandidate(next.Hash(), height, medianTimePast)
}

// TestTxFinality_DequeueHoldsNotFinalAndDescendants pins the queue path: a transaction whose
// nLockTime holds it back at the candidate is held with its descendants, whatever height it
// was validated for (one validated ahead comes from a validator ahead of block assembly). Held
// transactions are offered again when the candidate moves, and go in once they are final.
func TestTxFinality_DequeueHoldsNotFinalAndDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newFinalityTestProcessor(t, ctx)

	heightLocked := chainhash.HashH([]byte("finality-height-locked"))
	timeLocked := chainhash.HashH([]byte("finality-time-locked"))
	final := chainhash.HashH([]byte("finality-final"))
	ahead := chainhash.HashH([]byte("finality-validated-ahead"))
	child := chainhash.HashH([]byte("finality-child"))
	last := chainhash.HashH([]byte("finality-last"))

	require.Equal(t, Admitted, stp.AddBatchBounded(
		[]subtreepkg.Node{{Hash: heightLocked, Fee: 1, SizeInBytes: 1}, {Hash: timeLocked, Fee: 1, SizeInBytes: 1}, {Hash: final, Fee: 1, SizeInBytes: 1}, {Hash: ahead, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{{}, {}, {}, {}},
		false,
		[]uint32{finalityTestHeight, finalityTestHeight, finalityTestHeight, finalityTestHeight + 1},
		[]uint32{finalityTestHeight, finalityTestMTP, finalityTestHeight - 1, finalityTestHeight},
	))
	require.Equal(t, Admitted, stp.AddBatchBounded(
		[]subtreepkg.Node{{Hash: child, Fee: 1, SizeInBytes: 1}, {Hash: last, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{inpointsWithParent(heightLocked), {}},
		false,
		[]uint32{finalityTestHeight, finalityTestHeight},
		[]uint32{0, 0},
	))

	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	require.Eventually(t, func() bool { return stp.queue.length() == 0 && inTemplate(stp, last) }, 2*time.Second, 5*time.Millisecond)

	require.False(t, inTemplate(stp, heightLocked), "height lock 600 is not final at height 600")
	require.False(t, inTemplate(stp, timeLocked), "a time lock equal to the median time past is not final")
	require.False(t, inTemplate(stp, child), "a descendant of a transaction that is not final must not enter the template")
	require.False(t, inTemplate(stp, ahead), "a transaction validated ahead is held until block assembly catches up")
	require.True(t, inTemplate(stp, final), "height lock 599 is final at height 600")

	moveFinalityCandidate(stp, finalityTestHeight+1, finalityTestMTP+1)

	for _, hash := range []chainhash.Hash{heightLocked, timeLocked, child, ahead} {
		require.Eventually(t, func() bool { return inTemplate(stp, hash) }, 2*time.Second, 5*time.Millisecond,
			"a held transaction goes in once the candidate makes it final: %s", hash)
	}
}

// TestTxFinality_HeldTxMinedElsewhereIsNotOfferedAgain pins that a held transaction a block
// mines is forgotten: offering it again would put a mined transaction into the template.
func TestTxFinality_HeldTxMinedElsewhereIsNotOfferedAgain(t *testing.T) {
	stp := newFinalityTestProcessor(t, context.Background())

	locked := chainhash.HashH([]byte("finality-held-then-mined"))
	child := chainhash.HashH([]byte("finality-held-then-mined-child"))

	require.Empty(t, stp.FilterUnminedTxEra([]*utxostore.UnminedTransaction{
		{Node: &subtreepkg.Node{Hash: locked}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestHeight},
		{Node: &subtreepkg.Node{Hash: child}, TxInpoints: inpointsWithParent(locked)},
	}, []uint32{0, 0}))
	require.Equal(t, 2, stp.lockHeldTxCount())

	mined := NewSortedHashSet(1)
	require.NoError(t, mined.Put(locked))
	require.NoError(t, mined.Freeze(1))
	stp.forgetMinedDroppedTxs(mined)

	require.Equal(t, 1, stp.lockHeldTxCount(), "the mined transaction is forgotten, its child stays held")
}

// TestTxFinality_StaleCandidateAdmits pins that the check is skipped while the candidate
// describes another block than the processor's: block assembly sets it after the block moves,
// and checking against the previous block's median time past would drop transactions that are
// final at the new one. The reorg check runs once the candidate is set.
func TestTxFinality_StaleCandidateAdmits(t *testing.T) {
	stp := newFinalityTestProcessor(t, context.Background())
	stp.SetTxCandidate(&chainhash.Hash{0x01}, finalityTestHeight, finalityTestMTP)

	locked := chainhash.HashH([]byte("finality-stale-candidate"))

	kept := stp.FilterUnminedTxEra([]*utxostore.UnminedTransaction{
		{Node: &subtreepkg.Node{Hash: locked}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestHeight},
	}, []uint32{0})
	require.Len(t, kept, 1)
}

// TestTxFinality_ReloadHoldsNotFinal pins the reload path: the store's nLockTime is checked
// against the candidate whatever height the transaction is tagged with, and descendants are
// held with it.
func TestTxFinality_ReloadHoldsNotFinal(t *testing.T) {
	stp := newFinalityTestProcessor(t, context.Background())

	locked := chainhash.HashH([]byte("finality-reload-locked"))
	child := chainhash.HashH([]byte("finality-reload-child"))
	final := chainhash.HashH([]byte("finality-reload-final"))

	kept := stp.FilterUnminedTxEra([]*utxostore.UnminedTransaction{
		{Node: &subtreepkg.Node{Hash: locked}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestMTP + 10},
		{Node: &subtreepkg.Node{Hash: child}, TxInpoints: inpointsWithParent(locked)},
		{Node: &subtreepkg.Node{Hash: final}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestMTP - 1},
	}, []uint32{0, 0, 0})

	require.Len(t, kept, 1)
	require.Equal(t, final, kept[0].Hash)
	require.Equal(t, 2, stp.lockHeldTxCount())

	// Tagged above the candidate: after a reorg to a shorter chain, not a validator ahead.
	afterShorterReorg := chainhash.HashH([]byte("finality-reload-after-shorter-reorg"))
	require.Empty(t, stp.FilterUnminedTxEra([]*utxostore.UnminedTransaction{
		{Node: &subtreepkg.Node{Hash: afterShorterReorg}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestHeight},
	}, []uint32{finalityTestHeight + 2}))
}

// TestTxFinality_PreCSVTimeLockIsNotChecked pins that below CSV a time lock is measured against
// the candidate's own timestamp, which a reorg does not lower, so only height locks are checked.
func TestTxFinality_PreCSVTimeLockIsNotChecked(t *testing.T) {
	stp := newTestSubtreeProcessorForDequeueStaleness(t, context.Background())
	stp.InitCurrentBlockHeader(blockHeader)
	stp.SetTxEra(300)
	stp.SetTxCandidate(blockHeader.Hash(), 300, finalityTestMTP)

	timeLocked := chainhash.HashH([]byte("finality-pre-csv-time"))
	heightLocked := chainhash.HashH([]byte("finality-pre-csv-height"))

	kept := stp.FilterUnminedTxEra([]*utxostore.UnminedTransaction{
		{Node: &subtreepkg.Node{Hash: timeLocked}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: finalityTestMTP + 10},
		{Node: &subtreepkg.Node{Hash: heightLocked}, TxInpoints: &subtreepkg.TxInpoints{}, LockTime: 300},
	}, []uint32{0, 0})

	require.Len(t, kept, 1)
	require.Equal(t, timeLocked, kept[0].Hash)
}

// TestTxFinality_NotFinalTemplateTxs pins what block assembly reads after a reorg: the tracked
// transactions still in the template that the new candidate no longer lets in. A tracked
// transaction a block has mined is forgotten.
func TestTxFinality_NotFinalTemplateTxs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newFinalityTestProcessor(t, ctx)

	timeLocked := chainhash.HashH([]byte("finality-template-time-locked"))
	mined := chainhash.HashH([]byte("finality-template-mined"))

	require.Equal(t, Admitted, stp.AddBatchBounded(
		[]subtreepkg.Node{{Hash: timeLocked, Fee: 1, SizeInBytes: 1}, {Hash: mined, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{{}, {}},
		false,
		[]uint32{finalityTestHeight, finalityTestHeight},
		[]uint32{finalityTestMTP - 100, finalityTestMTP - 50},
	))

	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(ctx) })

	require.Eventually(t, func() bool { return inTemplate(stp, timeLocked) && inTemplate(stp, mined) }, 2*time.Second, 5*time.Millisecond)
	require.Zero(t, stp.NotFinalTemplateTxs())

	// A reorg to a branch whose tip has an earlier median time past.
	stp.SetTxCandidate(blockHeader.Hash(), finalityTestHeight, finalityTestMTP-200)
	require.Equal(t, 2, stp.NotFinalTemplateTxs())

	minedSet := NewSortedHashSet(1)
	require.NoError(t, minedSet.Put(mined))
	require.NoError(t, minedSet.Freeze(1))
	stp.forgetMinedDroppedTxs(minedSet)
	require.Equal(t, 1, stp.NotFinalTemplateTxs(), "a mined transaction is no longer tracked")

	// One that left the template some other way (this node's own block, a conflict) is
	// forgotten when the template is checked, so the tracked set stays the template's.
	stp.droppedMu.Lock()
	stp.trackLockTimeLocked(chainhash.HashH([]byte("finality-template-gone")), finalityTestMTP-50)
	stp.droppedMu.Unlock()
	require.Equal(t, int64(2), stp.lockedCount.Load())

	require.Equal(t, 1, stp.NotFinalTemplateTxs())
	require.Equal(t, int64(1), stp.lockedCount.Load(), "a tracked transaction no longer in the template is forgotten")
}
