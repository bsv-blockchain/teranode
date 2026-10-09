package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A dangling spender reference is a parent output slot naming a spender whose own
// record was never written — the spend-first ordering in SequentialSpendAndCreate
// records the spend on the parent before it creates the spender (#1214). The
// counter-conflicting walk must not wedge block validation on one, but it must
// still fail closed when the absence could instead be a mined-then-pruned counter.

// danglingCase wires a tx spending one output of one parent, where the parent
// names a spender the store has no record of.
func danglingCase(t *testing.T, parentMeta *meta.Data, walkErr error) (*MockUtxostore, context.Context, [32]byte, [32]byte) {
	t.Helper()

	mockStore := &MockUtxostore{}

	txHash := createTestHash("dangling-test-tx")
	parentTxHash := createTestHash("dangling-parent-tx")
	absentSpender := createTestHash("dangling-absent-spender")

	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	parentMeta.SpendingDatas = []*spend.SpendingData{{TxID: &absentSpender}}

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(parentMeta, nil)
	mockStore.On("Get", mock.Anything, &absentSpender, mock.Anything).
		Return(nil, walkErr)

	return mockStore, context.Background(), txHash, absentSpender
}

// Parent mined inside the retention window: no counter mined on that output slot
// could have been pruned yet, so the absence is provably a never-created loser.
// Tolerate it and leave it out of the counter set.
func TestGetCounterConflictingTxHashes_ToleratesDanglingSpenderInsideRetention(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, absentSpender := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{900}}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.NoError(t, err)
	require.NotContains(t, result, absentSpender)
	require.Equal(t, [][32]byte{txHash}, [][32]byte{result[0]})
	require.Len(t, result, 1)
}

// Parent confirmed below the pruning horizon: a counter could have been mined on
// that slot and since been pruned, so the absence is not provably benign. SVNode
// would reject a block double-spending a confirmed output, so fail closed.
func TestGetCounterConflictingTxHashes_FailsClosedOnDanglingSpenderBelowRetention(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{100}}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.Nil(t, result)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// A parent with no mined height and no unmined marker gives us nothing to prove
// recency with, so the guard must not fire.
func TestGetCounterConflictingTxHashes_FailsClosedWhenParentDepthUnknown(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, _ := danglingCase(t, &meta.Data{}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	_, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// An unmined parent sits at the top of the chain, so nothing spending it can have
// been pruned. Tolerate.
func TestGetCounterConflictingTxHashes_ToleratesDanglingSpenderOfUnminedParent(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, absentSpender := danglingCase(t,
		&meta.Data{UnminedSince: 995}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.NoError(t, err)
	require.NotContains(t, result, absentSpender)
}

// retention 0 disables the guard entirely — every absent record fails closed.
func TestGetCounterConflictingTxHashes_RetentionZeroDisablesTolerance(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{900}}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	_, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 0)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// The guard is scoped to an absent record. Any other failure of the descendant
// walk must still propagate, even for a parent inside the retention window.
func TestGetCounterConflictingTxHashes_DoesNotTolerateNonNotFoundWalkError(t *testing.T) {
	other := errors.NewProcessingError("aerospike unavailable")

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{900}}, other)

	_, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.Contains(t, err.Error(), "aerospike unavailable")
	require.False(t, errors.Is(err, errors.ErrTxNotFound))
	// the tip height must never be read on a non-tolerable path
	mockStore.AssertNotCalled(t, "GetBlockHeight")
}

// Height zero is the unset value of a store that was never given a tip. Recency
// cannot be proven against it, so the guard must fail closed instead of
// tolerating every absent spender.
func TestGetCounterConflictingTxHashes_FailsClosedWhenTipHeightUnset(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{900}}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(0))

	_, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// A present spender with a missing descendant also surfaces ErrTxNotFound from
// the walk. The spender itself may be mined on our chain, so only an absent
// spender record may be tolerated, never an absent descendant.
func TestGetCounterConflictingTxHashes_FailsClosedOnMissingDescendantOfPresentSpender(t *testing.T) {
	mockStore := &MockUtxostore{}

	txHash := createTestHash("descendant-test-tx")
	parentTxHash := createTestHash("descendant-parent-tx")
	presentSpender := createTestHash("descendant-present-spender")
	missingChild := createTestHash("descendant-missing-child")

	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			BlockHeights:  []uint32{900},
			SpendingDatas: []*spend.SpendingData{{TxID: &presentSpender}},
		}, nil)
	mockStore.On("Get", mock.Anything, &presentSpender, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{missingChild}}, nil)
	mockStore.On("Get", mock.Anything, &missingChild, mock.Anything).
		Return(nil, errors.NewTxNotFoundError("no record for descendant"))
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(context.Background(), mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.Nil(t, result)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// demotionCounterStore routes GetCounterConflicting through the real
// GetCounterConflictingTxHashes with retention 0, as the SQL and Aerospike
// stores do on the demotion path, so ProcessConflicting exercises the guard
// end to end.
type demotionCounterStore struct {
	*MockUtxostore
}

func (d demotionCounterStore) GetCounterConflicting(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return GetCounterConflictingTxHashes(ctx, d, hash, 0, 0)
}

// An absent loser has no record, so its spend stays on the parent slot and the
// winner's spend in step 3 would fail UTXO_SPENT after steps 1 and 2 had mutated.
// The demotion path must therefore fail closed on it, whatever the parent depth,
// before any mutation.
func TestProcessConflicting_FailsClosedOnAbsentLoser(t *testing.T) {
	for name, parentHeight := range map[string]uint32{"recent parent": 900, "old parent": 100} {
		t.Run(name, func(t *testing.T) {
			mockStore := &MockUtxostore{}

			winnerHash := createTestHash("demotion-winner-tx")
			parentTxHash := createTestHash("demotion-parent-tx")
			absentLoser := createTestHash("demotion-absent-loser")

			winnerTx := createTestTransactionWithInputs(parentTxHash, 0)

			mockStore.On("Get", mock.Anything, &winnerHash, mock.Anything).
				Return(&meta.Data{Tx: winnerTx, Conflicting: true}, nil)
			mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
				Return(&meta.Data{
					BlockHeights:  []uint32{parentHeight},
					SpendingDatas: []*spend.SpendingData{{TxID: &absentLoser}},
				}, nil)
			mockStore.On("Get", mock.Anything, &absentLoser, mock.Anything).
				Return(nil, errors.NewTxNotFoundError("no record for loser"))
			mockStore.On("GetBlockHeight").Return(uint32(1000))

			store := demotionCounterStore{MockUtxostore: mockStore}

			losers, _, err := ProcessConflicting(context.Background(), store, 1000, chainhash.Hash{},
				[]chainhash.Hash{winnerHash}, map[chainhash.Hash]struct{}{}, NoAncestryGuard)

			require.Error(t, err)
			require.Nil(t, losers)
			require.True(t, errors.Is(err, errors.ErrTxNotFound))
			mockStore.AssertNotCalled(t, "SetConflicting", mock.Anything, mock.Anything, mock.Anything)
			mockStore.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
			mockStore.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// A reorg stamps UnminedSince on a record without clearing its BlockHeights, so a
// buried parent can carry both. The mined height must decide, not the stamp: an
// unmined parent is tolerated at any depth, a parent mined 900 blocks ago is not.
func TestGetCounterConflictingTxHashes_MinedHeightOutranksUnminedSince(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	t.Run("buried parent with a stale unmined stamp fails closed", func(t *testing.T) {
		mockStore, ctx, txHash, _ := danglingCase(t,
			&meta.Data{BlockHeights: []uint32{100}, UnminedSince: 95}, notFound)
		mockStore.On("GetBlockHeight").Return(uint32(1000))

		result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

		require.Error(t, err)
		require.Nil(t, result)
		require.True(t, errors.Is(err, errors.ErrTxNotFound))
	})

	t.Run("recent parent with an unmined stamp is still tolerated", func(t *testing.T) {
		mockStore, ctx, txHash, absentSpender := danglingCase(t,
			&meta.Data{BlockHeights: []uint32{900}, UnminedSince: 95}, notFound)
		mockStore.On("GetBlockHeight").Return(uint32(1000))

		result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

		require.NoError(t, err)
		require.NotContains(t, result, absentSpender)
	})
}

// errors.Is matches a code anywhere in the wrap chain, so a StorageError that
// wraps a not-found, such as an unreadable external blob, must not be taken for an
// absent record. That is a data-availability fault, not a never-created loser.
func TestGetCounterConflictingTxHashes_FailsClosedOnStorageErrorWrappingNotFound(t *testing.T) {
	wrapped := errors.NewStorageError("external blob unreadable", errors.NewTxNotFoundError("blob missing"))

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockHeights: []uint32{900}}, wrapped)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.Nil(t, result)
	require.True(t, errors.Is(err, errors.ErrStorageError))
}

// A mined record from an older node version or a restore can carry BlockIDs but no
// BlockHeights. It was mined at an unknown height, so even with an UnminedSince
// stamp from a later reorg the guard cannot prove recency and must fail closed.
func TestGetCounterConflictingTxHashes_FailsClosedOnMinedParentWithoutHeights(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")

	mockStore, ctx, txHash, _ := danglingCase(t,
		&meta.Data{BlockIDs: []uint32{7}, UnminedSince: 995}, notFound)
	mockStore.On("GetBlockHeight").Return(uint32(1000))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

	require.Error(t, err)
	require.Nil(t, result)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// The comparison counts the tip ten blocks higher, so a parent that the store's
// cached tip still shows inside the window is tolerated only while it would remain
// inside it were the tip ten blocks further on. Parent mined at 900, retention 288:
// tolerated at tip 1177, failed closed at tip 1178. The heights are literal so the
// test pins the margin instead of following the constant.
func TestGetCounterConflictingTxHashes_TipLagMarginNarrowsTheWindow(t *testing.T) {
	notFound := errors.NewTxNotFoundError("no record for spender")
	t.Run("tip 1177 is tolerated", func(t *testing.T) {
		mockStore, ctx, txHash, absentSpender := danglingCase(t,
			&meta.Data{BlockHeights: []uint32{900}}, notFound)
		mockStore.On("GetBlockHeight").Return(uint32(1177))

		result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

		require.NoError(t, err)
		require.NotContains(t, result, absentSpender)
	})

	t.Run("tip 1178 fails closed", func(t *testing.T) {
		mockStore, ctx, txHash, _ := danglingCase(t,
			&meta.Data{BlockHeights: []uint32{900}}, notFound)
		mockStore.On("GetBlockHeight").Return(uint32(1178))

		result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0, 288)

		require.Error(t, err)
		require.Nil(t, result)
		require.True(t, errors.Is(err, errors.ErrTxNotFound))
	})
}
