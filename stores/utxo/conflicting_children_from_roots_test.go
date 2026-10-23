package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The roots of a block's conflicting transactions overlap: in a linear
// conflicting chain every member's cone contains every later member. One walk per
// root re-reads the shared part once per root (issue 2037), so the multi-root
// walk must read each reachable transaction once, however many roots reach it.
func TestGetConflictingChildrenFromRoots_SharesVisitedAcrossRoots(t *testing.T) {
	rootA := createTestHash("roots-a")
	rootB := createTestHash("roots-b")
	shared := createTestHash("roots-shared")
	tip := createTestHash("roots-tip")

	// rootA -> rootB -> shared -> tip, and rootA -> shared directly
	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &rootA, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &rootB}, {TxID: &shared}}}, nil).Once()
	mockStore.On("Get", mock.Anything, &rootB, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{shared}}, nil).Once()
	mockStore.On("Get", mock.Anything, &shared, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &tip}}}, nil).Once()
	mockStore.On("Get", mock.Anything, &tip, mock.Anything).
		Return(&meta.Data{}, nil).Once()

	children, err := GetConflictingChildrenFromRoots(context.Background(), mockStore, [][]chainhash.Hash{{rootA, rootB}}, 0)
	require.NoError(t, err)
	require.Len(t, children, 1)

	// the roots are not their own descendants, even when one reaches the other
	require.ElementsMatch(t, []chainhash.Hash{shared, tip}, children[0])

	mockStore.AssertExpectations(t)
	mockStore.AssertNumberOfCalls(t, "Get", 4)
}

// A later pass shares the visited set of the earlier ones: what an earlier pass
// read is neither read again nor returned, so the caller can tell the passes'
// cones apart.
func TestGetConflictingChildrenFromRoots_PassesShareVisited(t *testing.T) {
	rootA := createTestHash("passes-a")
	rootB := createTestHash("passes-b")
	childA := createTestHash("passes-child-a")
	childB := createTestHash("passes-child-b")

	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &rootA, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childA}}}, nil).Once()
	mockStore.On("Get", mock.Anything, &rootB, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childA}, {TxID: &childB}}}, nil).Once()
	mockStore.On("Get", mock.Anything, &childA, mock.Anything).Return(&meta.Data{}, nil).Once()
	mockStore.On("Get", mock.Anything, &childB, mock.Anything).Return(&meta.Data{}, nil).Once()

	children, err := GetConflictingChildrenFromRoots(context.Background(), mockStore, [][]chainhash.Hash{{rootA}, {rootA, rootB}}, 0)
	require.NoError(t, err)
	require.Equal(t, [][]chainhash.Hash{{childA}, {childB}}, children)

	mockStore.AssertExpectations(t)
}

// maxNodes is a per-root budget: the walk may visit maxNodes times the number of
// distinct roots, which is the aggregate bound of walking each root on its own.
// Many small cones pass however many there are; one cone too big for maxNodes
// still fails closed once it exceeds the whole walk's budget.
func TestGetConflictingChildrenFromRoots_BudgetScalesWithRoots(t *testing.T) {
	rootA := createTestHash("budget-roots-a")
	rootB := createTestHash("budget-roots-b")
	childA := createTestHash("budget-roots-child-a")
	childB := createTestHash("budget-roots-child-b")
	grandchildA := createTestHash("budget-roots-grandchild-a")

	newStore := func(deepA bool) *MockUtxostore {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &rootA, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childA}}}, nil).Maybe()
		mockStore.On("Get", mock.Anything, &rootB, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childB}}}, nil).Maybe()

		childAMeta := &meta.Data{}
		if deepA {
			childAMeta = &meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &grandchildA}}}
		}

		mockStore.On("Get", mock.Anything, &childA, mock.Anything).Return(childAMeta, nil).Maybe()
		mockStore.On("Get", mock.Anything, &childB, mock.Anything).Return(&meta.Data{}, nil).Maybe()
		mockStore.On("Get", mock.Anything, &grandchildA, mock.Anything).Return(&meta.Data{}, nil).Maybe()

		return mockStore
	}

	t.Run("many small cones pass", func(t *testing.T) {
		// 4 nodes, past maxNodes but within 2 roots x 2
		_, err := GetConflictingChildrenFromRoots(context.Background(), newStore(false), [][]chainhash.Hash{{rootA, rootB}}, 2)
		require.NoError(t, err)
	})

	t.Run("a root in several passes counts once", func(t *testing.T) {
		_, err := GetConflictingChildrenFromRoots(context.Background(), newStore(false), [][]chainhash.Hash{{rootA}, {rootA, rootB}}, 2)
		require.NoError(t, err)
	})

	t.Run("one oversized cone fails closed", func(t *testing.T) {
		// rootA -> childA -> grandchildA is 3 nodes against 1 root x 2
		_, err := GetConflictingChildrenFromRoots(context.Background(), newStore(true), [][]chainhash.Hash{{rootA}}, 2)
		require.ErrorIs(t, err, errors.ErrUtxoWalkLimitExceeded)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)
	})

	t.Run("no bound at zero", func(t *testing.T) {
		_, err := GetConflictingChildrenFromRoots(context.Background(), newStore(true), [][]chainhash.Hash{{rootA}}, 0)
		require.NoError(t, err)
	})
}

// A transaction the store no longer holds says nothing about what spent it, but
// it must not hide the rest of the cone: it is a leaf, and its siblings and their
// descendants are still walked. Both not-found shapes are covered: the SQL store
// returns ErrTxNotFound, aerospike returns a nil record.
func TestGetConflictingChildrenFromRoots_MissingRecordIsLeaf(t *testing.T) {
	root := createTestHash("missing-root")
	goneErr := createTestHash("missing-gone-err")
	goneNil := createTestHash("missing-gone-nil")
	sibling := createTestHash("missing-sibling")
	grandchild := createTestHash("missing-grandchild")

	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &root, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &goneErr}, {TxID: &goneNil}, {TxID: &sibling}}}, nil)
	mockStore.On("Get", mock.Anything, &goneErr, mock.Anything).
		Return(nil, errors.NewTxNotFoundError("gone"))
	mockStore.On("Get", mock.Anything, &goneNil, mock.Anything).
		Return(nil, nil)
	mockStore.On("Get", mock.Anything, &sibling, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &grandchild}}}, nil)
	mockStore.On("Get", mock.Anything, &grandchild, mock.Anything).
		Return(&meta.Data{}, nil)

	children, err := GetConflictingChildrenFromRoots(context.Background(), mockStore, [][]chainhash.Hash{{root}}, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []chainhash.Hash{goneErr, goneNil, sibling, grandchild}, children[0])

	t.Run("any other read error aborts", func(t *testing.T) {
		failing := &MockUtxostore{}
		failing.On("Get", mock.Anything, &root, mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &sibling}}}, nil)
		failing.On("Get", mock.Anything, &sibling, mock.Anything).
			Return(nil, errors.NewStorageError("store down"))

		_, err := GetConflictingChildrenFromRoots(context.Background(), failing, [][]chainhash.Hash{{root}}, 0)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})
}

// The frozen sentinel is not a record. As a root it is skipped; reached as a
// child it is returned for the callers' frozen checks but never read.
func TestGetConflictingChildrenFromRoots_FrozenSentinel(t *testing.T) {
	root := createTestHash("frozen-roots-root")

	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &root, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &subtree.FrozenBytesTxHash}}}, nil)

	children, err := GetConflictingChildrenFromRoots(context.Background(), mockStore,
		[][]chainhash.Hash{{subtree.CoinbasePlaceholderHashValue, root}}, 0)
	require.NoError(t, err)
	require.Equal(t, [][]chainhash.Hash{{subtree.FrozenBytesTxHash}}, children)

	mockStore.AssertNumberOfCalls(t, "Get", 1)
	mockStore.AssertNotCalled(t, "Get", mock.Anything, &subtree.FrozenBytesTxHash, mock.Anything)
}

// One call is one walk to the walk histograms, however many passes and roots it
// has: a single sample of every node visited, the levels of every pass, and the
// whole call's duration. Two partial samples per batch would read lower than the
// size the budget is checked against.
func TestGetConflictingChildrenFromRoots_ObservesOnceForTheWholeWalk(t *testing.T) {
	rootA := createTestHash("observe-a")
	rootB := createTestHash("observe-b")
	childA := createTestHash("observe-child-a")
	childB := createTestHash("observe-child-b")

	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &rootA, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childA}}}, nil)
	mockStore.On("Get", mock.Anything, &rootB, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{{TxID: &childB}}}, nil)
	mockStore.On("Get", mock.Anything, &childA, mock.Anything).Return(&meta.Data{}, nil)
	mockStore.On("Get", mock.Anything, &childB, mock.Anything).Return(&meta.Data{}, nil)

	nodesBefore, nodesSumBefore := histogramSample(t, prometheusUtxoConflictingWalkNodes)
	depthBefore, depthSumBefore := histogramSample(t, prometheusUtxoConflictingWalkDepth)
	durationBefore, _ := histogramSample(t, prometheusUtxoConflictingWalkDuration)

	_, err := GetConflictingChildrenFromRoots(context.Background(), mockStore, [][]chainhash.Hash{{rootA}, {rootB}}, 0)
	require.NoError(t, err)

	nodesAfter, nodesSumAfter := histogramSample(t, prometheusUtxoConflictingWalkNodes)
	depthAfter, depthSumAfter := histogramSample(t, prometheusUtxoConflictingWalkDepth)
	durationAfter, _ := histogramSample(t, prometheusUtxoConflictingWalkDuration)

	require.Equal(t, nodesBefore+1, nodesAfter)
	require.Equal(t, depthBefore+1, depthAfter)
	require.Equal(t, durationBefore+1, durationAfter)

	// four nodes visited across both passes, two levels in each pass
	require.InDelta(t, 4, nodesSumAfter-nodesSumBefore, 0)
	require.InDelta(t, 4, depthSumAfter-depthSumBefore, 0)
}
