// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
)

// TestMarkAsConflicting_EvictsCascadedDescendants verifies that when a parent is
// marked conflicting via the reload-time validateUnminedTxInputs path, every
// cascaded descendant is also removed from the in-memory subtree processor.
//
// Before the cascade fix (and even with the cascade alone, without eviction),
// a parallel worker could admit a child to the subtree processor's in-memory
// template before the cascade flipped its conflicting flag in the store; the
// stale child would then appear in the next mining candidate even though its
// parent is now marked conflicting.
//
// This test exercises a 3-level chain (parent → child → grandchild) to show that
// the eviction walks the full cascade, not just the initial hash.
func TestMarkAsConflicting_EvictsCascadedDescendants(t *testing.T) {
	ctx := context.Background()

	parentHash := chainhash.HashH([]byte("parent"))
	childHash := chainhash.HashH([]byte("child"))
	grandchildHash := chainhash.HashH([]byte("grandchild"))

	mockStore := new(utxoStore.MockUtxostore)
	// Level 1: parent → child
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return([]*utxoStore.Spend{{TxID: &parentHash, Vout: 0}}, []chainhash.Hash{childHash}, nil)
	// Level 2: child → grandchild
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return([]*utxoStore.Spend{{TxID: &childHash, Vout: 0}}, []chainhash.Hash{grandchildHash}, nil)
	// Level 3: grandchild has no children — BFS terminates
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{grandchildHash}, true).
		Return([]*utxoStore.Spend{{TxID: &grandchildHash, Vout: 0}}, []chainhash.Hash{}, nil)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Remove", mock.Anything, parentHash).Return(nil).Once()
	mockStp.On("Remove", mock.Anything, childHash).Return(nil).Once()
	mockStp.On("Remove", mock.Anything, grandchildHash).Return(nil).Once()

	ba := &BlockAssembler{
		utxoStore:        mockStore,
		subtreeProcessor: mockStp,
		settings:         &settings.Settings{},
		logger:           ulogger.TestLogger{},
	}

	ba.markAsConflicting(ctx, parentHash)

	mockStore.AssertExpectations(t)
	mockStp.AssertExpectations(t)
	mockStp.AssertNumberOfCalls(t, "Remove", 3)
}

// TestMarkAsConflicting_CascadeErrorDoesNotEvict verifies that when the cascade
// itself fails (underlying SetConflicting returns an error), markAsConflicting
// does not attempt any evictions. This avoids partial eviction of children that
// remain non-conflicting in the store — a reload would simply re-admit them.
func TestMarkAsConflicting_CascadeErrorDoesNotEvict(t *testing.T) {
	ctx := context.Background()

	parentHash := chainhash.HashH([]byte("parent-fail"))

	mockStore := new(utxoStore.MockUtxostore)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return([]*utxoStore.Spend(nil), []chainhash.Hash(nil),
			errors.NewProcessingError("aerospike unavailable"))

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	// Deliberately no Remove expectations — must not be called.

	ba := &BlockAssembler{
		utxoStore:        mockStore,
		subtreeProcessor: mockStp,
		settings:         &settings.Settings{},
		logger:           ulogger.TestLogger{},
	}

	ba.markAsConflicting(ctx, parentHash)

	mockStore.AssertExpectations(t)
	mockStp.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
}
