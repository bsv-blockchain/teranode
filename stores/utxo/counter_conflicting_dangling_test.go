package utxo

import (
	"context"
	"testing"

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
