package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSetConflicting_MustCascadeToChildren verifies that marking a parent tx
// conflicting via MarkConflictingRecursively also cascades to all spending children.
func TestSetConflicting_MustCascadeToChildren(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")

	parentSpends := []*Spend{{TxID: &parentHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return(parentSpends, []chainhash.Hash{childHash}, nil)

	childSpends := []*Spend{{TxID: &childHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return(childSpends, []chainhash.Hash{}, nil)

	allSpends, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{parentHash})
	require.NoError(t, err)

	require.Len(t, allSpends, 2, "parent + child spends")
	require.Equal(t, []chainhash.Hash{parentHash, childHash}, markedHashes,
		"marked set must be returned in BFS order: parent first, then child")
	mockStore.AssertCalled(t, "SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true)
	mockStore.AssertCalled(t, "SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true)
	mockStore.AssertExpectations(t)
}

// TestSetConflicting_CallerMustCascade verifies SetConflicting is called for both
// parent and child when using MarkConflictingRecursively.
func TestSetConflicting_CallerMustCascade(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return([]*Spend{{TxID: &parentHash, Vout: 0}}, []chainhash.Hash{childHash}, nil)

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return([]*Spend{{TxID: &childHash, Vout: 0}}, []chainhash.Hash{}, nil)

	_, _, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{parentHash})
	require.NoError(t, err)

	mockStore.AssertNumberOfCalls(t, "SetConflicting", 2)
}

// TestMarkConflictingRecursively_DoesCascade verifies the BFS cascade works
// through a 3-level chain: parent → child → grandchild.
func TestMarkConflictingRecursively_DoesCascade(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")
	grandchildHash := createTestHash("grandchild-tx")

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return([]*Spend{{TxID: &parentHash, Vout: 0}}, []chainhash.Hash{childHash}, nil)

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return([]*Spend{{TxID: &childHash, Vout: 0}}, []chainhash.Hash{grandchildHash}, nil)

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{grandchildHash}, true).
		Return([]*Spend{{TxID: &grandchildHash, Vout: 0}}, []chainhash.Hash{}, nil)

	allSpends, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{parentHash})
	require.NoError(t, err)

	require.Len(t, allSpends, 3)
	require.Equal(t, []chainhash.Hash{parentHash, childHash, grandchildHash}, markedHashes,
		"marked set must be returned in BFS order: parent, then child, then grandchild")
	mockStore.AssertCalled(t, "SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true)
	mockStore.AssertCalled(t, "SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true)
	mockStore.AssertCalled(t, "SetConflicting", mock.Anything, []chainhash.Hash{grandchildHash}, true)
	mockStore.AssertExpectations(t)
}
