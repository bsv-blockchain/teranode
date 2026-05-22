package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createSpendableTestTransaction builds a tx whose input has the
// PreviousTxScript / PreviousTxSatoshis fields populated so that
// util.UTXOHashFromInput can compute a hash. createSpendableTestTransaction
// in process_conflicting_test.go leaves those nil, which is fine for the
// existing ProcessConflicting tests (which use mocks for Spend/Unspend) but
// trips ReverseProcessConflicting's spendsForTx helper.
func createSpendableTestTransaction(parentHash chainhash.Hash, vout uint32) *bt.Tx {
	tx := bt.NewTx()

	input := &bt.Input{
		PreviousTxOutIndex: vout,
		PreviousTxSatoshis: 1_000,
		PreviousTxScript:   bscript.NewFromBytes([]byte{0x51}), // OP_1 — anything non-nil works
	}
	_ = input.PreviousTxIDAdd(&parentHash)
	tx.Inputs = append(tx.Inputs, input)

	output := &bt.Output{
		Satoshis:      900,
		LockingScript: bscript.NewFromBytes([]byte{0x51}),
	}
	tx.Outputs = append(tx.Outputs, output)

	return tx
}

// TestReverseProcessConflicting_RestoresOriginalSpender exercises the
// production-incident scenario: a stale block's moveForward invoked
// ProcessConflicting with [demoted] as the promoted winner, swapping
// parent.SpendingDatas to point at demoted and marking the original
// mempool spender (counter) Conflicting=true. When that block is moved
// back, ReverseProcessConflicting must restore the prior state:
// demoted → Conflicting=true (loser), counter → Conflicting=false
// (winner), parent.SpendingDatas[vout] re-spent by counter.
func TestReverseProcessConflicting_RestoresOriginalSpender(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent")
	demotedHash := createTestHash("reverse-demoted")
	counterHash := createTestHash("reverse-counter")

	const vout = uint32(1)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	counterTx := createSpendableTestTransaction(parentHash, vout)

	// Step 1 — load demoted tx; observed state is the "post-ProcessConflicting"
	// inversion: demoted is currently Conflicting=false (was the winner).
	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	// selectCountersForDemotedTx walks demoted's inputs and queries the
	// parent for its ConflictingChildren list.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash, counterHash},
		}, nil).Once()

	// For each non-demoted candidate, the implementation loads the tx and
	// verifies it spends the same (parentHash, vout). Returns Conflicting=true
	// matching the post-stale-block state for the demoted counter.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true}, nil).Once()

	// Step 2 — MarkConflictingRecursively([demoted]) cascades demoted +
	// descendants → Conflicting=true. The mock returns no further children,
	// terminating the BFS after one round.
	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()

	// Step 3 — Unspend the demoted tx's input spends so the parent UTXO
	// no longer claims demoted as its spender.
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	// Step 4 — re-fetch counter tx for the Spend call.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()

	// Re-spend parent UTXO with counter.
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()

	// Step 5 — UnmarkConflictingRecursively([counter]) flips counter +
	// descendants back to Conflicting=false.
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{counterHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.NotNil(t, touched)
	require.Contains(t, touched, demotedHash, "demoted tx must be in touched set")
	require.Contains(t, touched, counterHash, "counter tx must be in touched set")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_SkipsAlreadyReversedDemoted handles
// idempotency: if a moveBack is replayed (e.g. during reset) on a block
// whose ConflictingNodes were already restored, the demoted tx is
// already Conflicting=true and we have nothing to undo.
func TestReverseProcessConflicting_SkipsAlreadyReversedDemoted(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	demotedHash := createTestHash("reverse-already-conflicting")
	demotedTx := createTestTransaction()

	// createTestTransaction has a single input pointing at "prev-tx" vout 0.
	parentHash := createTestHash("prev-tx")
	// A non-D spender already populates parent.SpendingDatas[0] →
	// isReverseFullyApplied returns true → short-circuit holds.
	counterSpenderHash := createTestHash("reverse-counter-spender")

	// Demoted Conflicting=true.
	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: true}, nil).Once()

	// Parent state confirms reverse fully applied (SpendingDatas[0] != D, non-nil).
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{
			{TxID: &counterSpenderHash, Vin: 0},
		}}, nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	assert.Nil(t, touched, "no work done → no touched hashes")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_PartialStateRetryCompletes pins the
// recovery path Simon flagged in PR #845 review: if a previous reverse
// failed between step 1 (Mark) and step 3 (Spend(C)), the demoted tx is
// stuck Conflicting=true with parent.SpendingDatas[vout] empty. A retry
// MUST detect the partial state via parent observable state and re-run
// the steps to completion, not short-circuit on D.Conflicting alone.
func TestReverseProcessConflicting_PartialStateRetryCompletes(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("partial-retry-parent")
	demotedHash := createTestHash("partial-retry-demoted")
	counterHash := createTestHash("partial-retry-counter")

	const vout = uint32(0)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	counterTx := createSpendableTestTransaction(parentHash, vout)

	// Top-of-loop Get: D.Conflicting=true (partial state from earlier failure).
	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: true}, nil).Once()

	// isReverseFullyApplied → Get parent.SpendingDatas. Slot 0 nil (Unspend
	// cleared it, Spend(C) never ran). Returns false → fall through to retry.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{nil}}, nil).Once()

	// selectCountersForDemotedTx → Get parent.ConflictingChildren → finds counter.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil).Once()
	// Counter still Conflicting=true (Unmark never ran on it).
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true, CreatedAt: 1}, nil).Once()

	// Re-run Mark+Unspend idempotently (already-true, already-cleared — no-ops
	// at the store level, but the helper still issues the calls).
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	// Now the step 3 retry: Get counter body, Spend(C) succeeds this time,
	// Unmark counter.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{counterHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	cascade, touched, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{demotedHash})
	require.NoError(t, err)

	assert.Contains(t, cascade, demotedHash, "demoted remains in cascade even on retry — caller still needs eviction")
	assert.Contains(t, touched, demotedHash)
	assert.Contains(t, touched, counterHash, "counter must be touched on the completing retry — confirms Unmark ran")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_DConflictingButParentStillPointsToD —
// pathological case: D.Conflicting=true but parent.SpendingDatas[vout]
// still points to D. Means an earlier MarkConflictingRecursively
// succeeded but Unspend never ran. Retry must NOT short-circuit; it
// must run Unspend (clearing the slot) and Spend(C).
func TestReverseProcessConflicting_DConflictingButParentStillPointsToD(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("d-stuck-parent")
	demotedHash := createTestHash("d-stuck-demoted")
	counterHash := createTestHash("d-stuck-counter")

	const vout = uint32(0)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	counterTx := createSpendableTestTransaction(parentHash, vout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: true}, nil).Once()

	// Parent shows D still as the recorded spender — Unspend never ran.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{
			{TxID: &demotedHash, Vin: 0},
		}}, nil).Once()

	// Fall through to retry — selectCountersForDemotedTx Get on parent.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true, CreatedAt: 1}, nil).Once()

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{counterHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{demotedHash})
	require.NoError(t, err)
	mockStore.AssertExpectations(t)
}

// TestIsReverseFullyApplied unit-tests the parent-state predicate
// directly. The guard semantics are critical — a false positive (claim
// fully reversed when it isn't) traps partial state; a false negative
// triggers an unnecessary re-run (idempotent, but wasteful).
func TestIsReverseFullyApplied(t *testing.T) {
	parentHash := createTestHash("isrev-parent")
	demotedHash := createTestHash("isrev-demoted")
	otherSpenderHash := createTestHash("isrev-other")

	demotedTx := createSpendableTestTransaction(parentHash, 0)

	t.Run("parent.SpendingDatas[vout] points to non-D non-nil spender -> true", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{
				{TxID: &otherSpenderHash, Vin: 0},
			}}, nil).Once()

		got, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.NoError(t, err)
		assert.True(t, got)
		mockStore.AssertExpectations(t)
	})

	t.Run("parent.SpendingDatas[vout] is nil -> false", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{nil}}, nil).Once()

		got, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.NoError(t, err)
		assert.False(t, got)
		mockStore.AssertExpectations(t)
	})

	t.Run("parent.SpendingDatas[vout].TxID == D -> false", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{
				{TxID: &demotedHash, Vin: 0},
			}}, nil).Once()

		got, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.NoError(t, err)
		assert.False(t, got)
		mockStore.AssertExpectations(t)
	})

	t.Run("parent.SpendingDatas slice shorter than vout -> false", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spendpkg.SpendingData{}}, nil).Once()

		got, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.NoError(t, err)
		assert.False(t, got)
		mockStore.AssertExpectations(t)
	})

	t.Run("parent meta nil -> false (parent pruned or missing)", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return((*meta.Data)(nil), nil).Once()

		got, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.NoError(t, err)
		assert.False(t, got, "missing parent must not be treated as fully-reversed evidence")
		mockStore.AssertExpectations(t)
	})

	t.Run("store error propagates", func(t *testing.T) {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
			Return((*meta.Data)(nil), errors.NewProcessingError("parent get failed")).Once()

		_, err := isReverseFullyApplied(context.Background(), mockStore, demotedTx, demotedHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error getting parent")
		mockStore.AssertExpectations(t)
	})
}

// TestReverseProcessConflicting_FiltersCounterWithMismatchedOutput
// covers the same-output check inside selectCountersForDemotedTx: a tx
// listed in parent.ConflictingChildren that does NOT actually spend the
// (parent, vout) the demoted tx spends must be skipped (it conflicts
// with a sibling output, not this one).
func TestReverseProcessConflicting_FiltersCounterWithMismatchedOutput(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent-multi")
	demotedHash := createTestHash("reverse-demoted-multi")
	mismatchedHash := createTestHash("reverse-counter-mismatched-output")

	const demotedVout = uint32(3)
	const otherVout = uint32(7)

	demotedTx := createSpendableTestTransaction(parentHash, demotedVout)
	// mismatchedTx spends parentHash but a DIFFERENT vout — it's a
	// legitimate sibling-output spender, not a counter to our demoted tx.
	mismatchedTx := createSpendableTestTransaction(parentHash, otherVout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash, mismatchedHash},
		}, nil).Once()

	mockStore.On("Get", mock.Anything, &mismatchedHash, mock.Anything).
		Return(&meta.Data{Tx: mismatchedTx, Conflicting: true}, nil).Once()

	// No Spend / Unmark on mismatched: same-output filter rejects it.
	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.Contains(t, touched, demotedHash)
	require.NotContains(t, touched, mismatchedHash,
		"mismatched-output candidate must not be touched")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_NoCounterToPromote covers the case where
// the demoted tx is currently the spender but no candidate in
// parent.ConflictingChildren is Conflicting=true (the counter was
// already promoted by some other process, or never existed). The
// function should still mark demoted Conflicting=true and unspend its
// inputs, but skip the Spend/Unmark steps.
func TestReverseProcessConflicting_NoCounterToPromote(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent-lone")
	demotedHash := createTestHash("reverse-demoted-lone")

	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	// parent has only the demoted tx as a conflicting child — no counter.
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash},
		}, nil).Once()

	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.Contains(t, touched, demotedHash)
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_SkipsCounterAlreadyNonConflicting verifies
// the Conflicting=true filter inside selectCountersForDemotedTx: if the
// candidate counter is already Conflicting=false, it's not part of the
// state ProcessConflicting flipped — we must leave it alone.
func TestReverseProcessConflicting_SkipsCounterAlreadyNonConflicting(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent-stale-counter")
	demotedHash := createTestHash("reverse-demoted-stale-counter")
	counterHash := createTestHash("reverse-counter-stale")

	const vout = uint32(2)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	counterTx := createSpendableTestTransaction(parentHash, vout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash, counterHash},
		}, nil).Once()

	// Counter is Conflicting=false → skip.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: false}, nil).Once()

	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.Contains(t, touched, demotedHash)
	require.NotContains(t, touched, counterHash)
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_CoinbasePlaceholderSkipped mirrors
// ProcessConflicting's frozen-tx guard at the head of its loop.
func TestReverseProcessConflicting_CoinbasePlaceholderSkipped(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	// No expectations set — the placeholder branch should short-circuit
	// without touching the store.
	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{subtree.CoinbasePlaceholderHashValue})

	require.NoError(t, err)
	assert.Nil(t, touched)
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_EmptyInput is a no-op guard for callers
// that pass a zero-length list (subtree had no ConflictingNodes).
func TestReverseProcessConflicting_EmptyInput(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 1, nil)

	require.NoError(t, err)
	assert.Nil(t, touched)
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_PropagatesGetError surfaces store-level
// errors instead of silently skipping the tx — a Get failure during
// reverse is non-recoverable for this block's moveBack.
func TestReverseProcessConflicting_PropagatesGetError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	demotedHash := createTestHash("reverse-get-error")

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return((*meta.Data)(nil), errors.NewProcessingError("aerospike unavailable")).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Nil(t, touched)
	assert.Contains(t, err.Error(), "error getting demoted tx meta")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_PicksOldestCounterByCreatedAt covers the
// multi-counter case the v2 strategy mis-handled: when more than one entry
// in parent.ConflictingChildren is Conflicting=true and spends the same
// (parent, vout), the heuristic picks the one with the lowest CreatedAt
// (first-seen mempool spender — the original canonical spender that an
// earlier ProcessConflicting demoted). Other candidates are left untouched.
func TestReverseProcessConflicting_PicksOldestCounterByCreatedAt(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent-multi-counter")
	demotedHash := createTestHash("reverse-demoted-multi-counter")
	oldestHash := createTestHash("reverse-counter-oldest")
	youngerHash := createTestHash("reverse-counter-younger")

	const vout = uint32(0)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	oldestTx := createSpendableTestTransaction(parentHash, vout)
	youngerTx := createSpendableTestTransaction(parentHash, vout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash, oldestHash, youngerHash},
		}, nil).Once()

	// CreatedAt: oldest=1000, younger=5000. Oldest wins.
	mockStore.On("Get", mock.Anything, &oldestHash, mock.Anything).
		Return(&meta.Data{Tx: oldestTx, Conflicting: true, CreatedAt: 1000}, nil).Once()
	mockStore.On("Get", mock.Anything, &youngerHash, mock.Anything).
		Return(&meta.Data{Tx: youngerTx, Conflicting: true, CreatedAt: 5000}, nil).Once()

	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	// Only the oldest counter gets the Spend + Unmark sequence.
	mockStore.On("Get", mock.Anything, &oldestHash, mock.Anything).
		Return(&meta.Data{Tx: oldestTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, oldestTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{oldestHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.Contains(t, touched, demotedHash)
	require.Contains(t, touched, oldestHash)
	require.NotContains(t, touched, youngerHash,
		"younger counter must remain Conflicting=true, untouched by reverse")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_TiebreakOnEqualCreatedAtByHash asserts the
// hash-lex tiebreak when two candidates share a CreatedAt value (same
// millisecond, two nodes racing into the mempool). Determinism matters
// because two replicas seeing the same UTXO state must converge.
func TestReverseProcessConflicting_TiebreakOnEqualCreatedAtByHash(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-parent-tiebreak")
	demotedHash := createTestHash("reverse-demoted-tiebreak")
	// Lexicographically smaller hash should win on equal CreatedAt.
	lowerHashCounter := chainhash.Hash{0x00, 0x01}
	higherHashCounter := chainhash.Hash{0xff, 0xfe}

	const vout = uint32(0)
	const sameTime = int64(7777)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	lowerCounterTx := createSpendableTestTransaction(parentHash, vout)
	higherCounterTx := createSpendableTestTransaction(parentHash, vout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{demotedHash, lowerHashCounter, higherHashCounter},
		}, nil).Once()

	mockStore.On("Get", mock.Anything, &lowerHashCounter, mock.Anything).
		Return(&meta.Data{Tx: lowerCounterTx, Conflicting: true, CreatedAt: sameTime}, nil).Once()
	mockStore.On("Get", mock.Anything, &higherHashCounter, mock.Anything).
		Return(&meta.Data{Tx: higherCounterTx, Conflicting: true, CreatedAt: sameTime}, nil).Once()

	demotedSpends := []*Spend{{TxID: &demotedHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return(demotedSpends, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	mockStore.On("Get", mock.Anything, &lowerHashCounter, mock.Anything).
		Return(&meta.Data{Tx: lowerCounterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, lowerCounterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{lowerHashCounter}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	_, touched, err := ReverseProcessConflicting(ctx, mockStore, 100,
		[]chainhash.Hash{demotedHash})

	require.NoError(t, err)
	require.Contains(t, touched, lowerHashCounter)
	require.NotContains(t, touched, higherHashCounter,
		"higher-hash counter must not be touched when CreatedAt ties")
	mockStore.AssertExpectations(t)
}

// TestIsOlderCounter_OrdersByTimestampThenHash spot-checks the comparison
// helper directly so the heuristic's tiebreak semantics are pinned without
// having to walk the full ReverseProcessConflicting flow.
func TestIsOlderCounter_OrdersByTimestampThenHash(t *testing.T) {
	lower := chainhash.Hash{0x00}
	higher := chainhash.Hash{0xff}

	// Lower CreatedAt always wins regardless of hash order.
	assert.True(t, isOlderCounter(100, higher, 200, lower))
	assert.False(t, isOlderCounter(200, lower, 100, higher))

	// Equal CreatedAt → hash lex compare.
	assert.True(t, isOlderCounter(500, lower, 500, higher))
	assert.False(t, isOlderCounter(500, higher, 500, lower))

	// Identical pair is not strictly less than itself.
	assert.False(t, isOlderCounter(500, lower, 500, lower))

	// Missing CreatedAt (0) is treated as newer than any timestamped record:
	// we never prefer the unknown-vintage candidate over a known one.
	assert.False(t, isOlderCounter(0, lower, 1, higher),
		"CreatedAt=0 must not beat a real timestamp")
	assert.True(t, isOlderCounter(1, lower, 0, higher),
		"real timestamp must beat CreatedAt=0")

	// Both missing → fall through to hash lex.
	assert.True(t, isOlderCounter(0, lower, 0, higher))
	assert.False(t, isOlderCounter(0, higher, 0, lower))
}

// TestUnmarkConflictingRecursively_BFSCascade verifies that the BFS
// inversely mirrors MarkConflictingRecursively: input set + every
// descendant reached via SpendingDatas have Conflicting=false applied.
func TestUnmarkConflictingRecursively_BFSCascade(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("unmark-root")
	childHash := createTestHash("unmark-child")
	grandchildHash := createTestHash("unmark-grandchild")

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, false).
		Return([]*Spend{}, []chainhash.Hash{childHash}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, false).
		Return([]*Spend{}, []chainhash.Hash{grandchildHash}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{grandchildHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	cleared, err := UnmarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{parentHash})

	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{parentHash, childHash, grandchildHash}, cleared,
		"cleared set must be BFS order: input first, then each descendant level")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_DemotedMetaNilSkipsTx pins the
// nil-or-missing-tx short-circuit. The reverse must not error on a
// demoted hash whose meta returns nil (legitimate for already-pruned
// records mid-reorg) — it skips and continues with the next demoted hash.
func TestReverseProcessConflicting_DemotedMetaNilSkipsTx(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	missingHash := createTestHash("reverse-demoted-missing")

	mockStore.On("Get", mock.Anything, &missingHash, mock.Anything).
		Return((*meta.Data)(nil), nil).Once()

	cascade, touched, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{missingHash})

	require.NoError(t, err)
	assert.Nil(t, cascade)
	assert.Nil(t, touched)
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_SelectCountersErrorPropagates covers the
// error wrap when parent.ConflictingChildren lookup fails — a store
// outage during reverse selection must abort the reverse cleanly so the
// reorg surfaces the failure instead of marking the demoted tx without
// promoting any counter.
func TestReverseProcessConflicting_SelectCountersErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-sel-err-parent")
	demotedHash := createTestHash("reverse-sel-err-demoted")
	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return((*meta.Data)(nil), errors.NewProcessingError("parent lookup failed")).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error getting parent meta")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_MarkConflictingErrorPropagates verifies
// that an underlying SetConflicting failure during the demoted-side BFS
// cascade aborts the reverse with a wrapped error — we cannot continue
// to Unspend / Spend the counter if the demoted tx isn't actually flagged.
func TestReverseProcessConflicting_MarkConflictingErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-mark-err-parent")
	demotedHash := createTestHash("reverse-mark-err-demoted")
	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{}}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, errors.NewProcessingError("setConflicting failed")).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error marking demoted tx + descendants conflicting")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_UnspendErrorPropagates covers the Unspend
// failure right after MarkConflictingRecursively — at this point the
// demoted tx is flagged but parent.SpendingDatas[vout] hasn't been
// cleared, so we must surface the error rather than leave the UTXO state
// half-reversed (flagged demoted + still recorded as spender).
func TestReverseProcessConflicting_UnspendErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-unspend-err-parent")
	demotedHash := createTestHash("reverse-unspend-err-demoted")
	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{}}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(errors.NewProcessingError("unspend failed")).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error unspending demoted tx inputs")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_CounterMetaNilSkipsCounter — when
// selectCountersForDemotedTx returns a counter hash but a subsequent
// fields.Tx Get yields nil meta (e.g. counter was pruned between calls),
// the reverse must continue: demoted stays flagged, Unspend already
// happened, and we just skip the Spend + Unmark for this counter.
func TestReverseProcessConflicting_CounterMetaNilSkipsCounter(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-counter-nil-parent")
	demotedHash := createTestHash("reverse-counter-nil-demoted")
	counterHash := createTestHash("reverse-counter-nil-counter")

	demotedTx := createSpendableTestTransaction(parentHash, 0)
	counterTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil).Once()
	// First counter Get (from selectCountersForDemotedTx) — returns full meta.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true, CreatedAt: 1}, nil).Once()

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	// Second counter Get (from the post-Unspend Spend prep loop) — nil meta.
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return((*meta.Data)(nil), nil).Once()

	cascade, touched, err := ReverseProcessConflicting(ctx, mockStore, 1,
		[]chainhash.Hash{demotedHash})
	require.NoError(t, err)

	assert.Contains(t, cascade, demotedHash, "demoted must still flag even if counter promotion skipped")
	assert.Contains(t, touched, demotedHash)
	assert.NotContains(t, touched, counterHash, "skipped counter must not appear in touched set")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_CounterSpendErrorPropagates covers the
// Spend failure on the counter promotion step. Once we've unspent the
// demoted tx, the parent.SpendingDatas[vout] is empty — failing to
// re-spend with the counter would leave that slot orphaned, so the
// caller must see the error and surface it on the moveBackBlock path.
func TestReverseProcessConflicting_CounterSpendErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-counter-spend-err-parent")
	demotedHash := createTestHash("reverse-counter-spend-err-demoted")
	counterHash := createTestHash("reverse-counter-spend-err-counter")

	demotedTx := createSpendableTestTransaction(parentHash, 0)
	counterTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true, CreatedAt: 1}, nil).Once()

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()

	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, errors.NewProcessingError("spend failed")).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error spending counter")
	mockStore.AssertExpectations(t)
}

// TestReverseProcessConflicting_CounterUnmarkErrorPropagates exercises
// the Unmark cascade failure path. With Spend done, the UTXO is now
// owned by counter — but failing to flip counter Conflicting=false
// leaves it inconsistent (it's the spender but is flagged conflicting,
// so anything reading it via GetSpend will refuse to spend its outputs).
func TestReverseProcessConflicting_CounterUnmarkErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("reverse-counter-unmark-err-parent")
	demotedHash := createTestHash("reverse-counter-unmark-err-demoted")
	counterHash := createTestHash("reverse-counter-unmark-err-counter")

	demotedTx := createSpendableTestTransaction(parentHash, 0)
	counterTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true, CreatedAt: 1}, nil).Once()

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{counterHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, errors.NewProcessingError("unmark failed")).Once()

	_, _, err := ReverseProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{demotedHash})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error un-marking counter")
	mockStore.AssertExpectations(t)
}

// TestSelectCountersForDemotedTx_ParentMetaNilSkips — when a parent has
// no recorded meta (legitimately if the parent is missing from the
// store mid-reorg), the helper skips that input rather than erroring.
// Other inputs of the same demoted tx still get processed.
func TestSelectCountersForDemotedTx_ParentMetaNilSkips(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("select-counter-parent-nil")

	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return((*meta.Data)(nil), nil).Once()

	got, err := selectCountersForDemotedTx(ctx, mockStore, demotedTx, map[chainhash.Hash]struct{}{})
	require.NoError(t, err)
	assert.Empty(t, got, "nil parent meta must not contribute counters but must not error")
	mockStore.AssertExpectations(t)
}

// TestSelectCountersForDemotedTx_CandidateGetErrorPropagates — a Get
// failure on a candidate counter meta must surface, not silently skip,
// or we risk picking a wrong counter (or none) and leaving SpendingDatas
// pointing at a stale value.
func TestSelectCountersForDemotedTx_CandidateGetErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("select-counter-candidate-err-parent")
	candidateHash := createTestHash("select-counter-candidate-err-candidate")

	demotedTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{candidateHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &candidateHash, mock.Anything).
		Return((*meta.Data)(nil), errors.NewProcessingError("candidate lookup failed")).Once()

	_, err := selectCountersForDemotedTx(ctx, mockStore, demotedTx, map[chainhash.Hash]struct{}{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error getting candidate counter")
	mockStore.AssertExpectations(t)
}

// TestSelectCountersForDemotedTx_CandidateMetaNilSkipsCandidate — if a
// candidate's meta returns nil (pruned mid-reorg), the helper skips it
// and continues to other candidates. Distinct from the error case above
// because nil != error: pruned-but-known is legitimate, store-down isn't.
func TestSelectCountersForDemotedTx_CandidateMetaNilSkipsCandidate(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("select-counter-candidate-nil-parent")
	prunedHash := createTestHash("select-counter-candidate-nil-pruned")
	liveHash := createTestHash("select-counter-candidate-nil-live")

	demotedTx := createSpendableTestTransaction(parentHash, 0)
	liveTx := createSpendableTestTransaction(parentHash, 0)

	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{prunedHash, liveHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &prunedHash, mock.Anything).
		Return((*meta.Data)(nil), nil).Once()
	mockStore.On("Get", mock.Anything, &liveHash, mock.Anything).
		Return(&meta.Data{Tx: liveTx, Conflicting: true, CreatedAt: 100}, nil).Once()

	got, err := selectCountersForDemotedTx(ctx, mockStore, demotedTx, map[chainhash.Hash]struct{}{})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{liveHash}, got,
		"pruned candidate must be skipped, live one promoted")
	mockStore.AssertExpectations(t)
}

// TestSpendsForTx covers the helper that builds []*Spend records for
// Unspend / Spend. ReverseProcessConflicting uses it on the demoted tx
// before un-spending its inputs — if the helper silently dropped a bad
// input we'd leave parent.SpendingDatas[vout] still pointing at the
// demoted tx. Test the happy path AND the error propagation so a future
// refactor can't downgrade the error to a skip.
func TestSpendsForTx(t *testing.T) {
	t.Run("single input populated -> single Spend record", func(t *testing.T) {
		parentHash := createTestHash("spends-single-parent")
		tx := createSpendableTestTransaction(parentHash, 7)

		got, err := spendsForTx(tx)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Equal(t, &parentHash, got[0].TxID)
		assert.Equal(t, uint32(7), got[0].Vout)
		assert.NotNil(t, got[0].UTXOHash, "UTXOHash must be derived from input fields")
		require.NotNil(t, got[0].SpendingData, "SpendingData must point at the spending tx")
		assert.Equal(t, tx.TxIDChainHash(), got[0].SpendingData.TxID)
	})

	t.Run("multiple inputs -> records returned in input order with correct vout indices", func(t *testing.T) {
		parentA := createTestHash("spends-multi-A")
		parentB := createTestHash("spends-multi-B")

		// Two inputs, distinct parents + distinct vouts.
		txA := createSpendableTestTransaction(parentA, 0)
		txB := createSpendableTestTransaction(parentB, 3)
		// Merge inputs into a single tx so we exercise the per-input loop.
		merged := bt.NewTx()
		merged.Inputs = append(merged.Inputs, txA.Inputs[0], txB.Inputs[0])
		merged.Outputs = append(merged.Outputs, txA.Outputs[0])

		got, err := spendsForTx(merged)
		require.NoError(t, err)
		require.Len(t, got, 2)

		assert.Equal(t, &parentA, got[0].TxID)
		assert.Equal(t, uint32(0), got[0].Vout)
		assert.Equal(t, 0, got[0].SpendingData.Vin, "SpendingData.Vin must be the input index")

		assert.Equal(t, &parentB, got[1].TxID)
		assert.Equal(t, uint32(3), got[1].Vout)
		assert.Equal(t, 1, got[1].SpendingData.Vin)

		assert.NotEqual(t, got[0].UTXOHash, got[1].UTXOHash, "distinct inputs must yield distinct UTXO hashes")
	})

	t.Run("input with nil PreviousTxScript surfaces UTXOHashFromInput error", func(t *testing.T) {
		// util.UTXOHashFromInput returns "locking script is nil" when the
		// input's PreviousTxScript is nil. Reverse must not silently skip
		// this — it would leave the demoted tx's input un-spent on the
		// parent UTXO. createSpendableTestTransaction sets a stub script;
		// reach in and null it to trigger the error.
		parentHash := createTestHash("spends-bad-input")
		tx := createSpendableTestTransaction(parentHash, 0)
		tx.Inputs[0].PreviousTxScript = nil

		got, err := spendsForTx(tx)
		require.Error(t, err)
		assert.Nil(t, got, "error path must return nil slice — partial results would mask the bad input downstream")
		assert.Contains(t, err.Error(), "locking script is nil")
	})

	t.Run("empty inputs slice -> empty Spends slice, no error", func(t *testing.T) {
		// Idempotent guard. A tx with zero inputs should not happen in
		// production (coinbase has one input), but the helper must not
		// panic and must not over-allocate.
		got, err := spendsForTx(bt.NewTx())
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
