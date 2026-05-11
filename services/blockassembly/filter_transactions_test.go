// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateParentChain_BatchingAndOrdering tests that the validateParentChain
// function correctly handles transaction ordering validation across batch boundaries.
// This test specifically validates the fix for the variable shadowing bug where the loop variable 'i'
// was being shadowed, causing incorrect currentIdx calculations in ordering validation.
func TestValidateParentChain_BatchingAndOrdering(t *testing.T) {
	ctx := context.Background()

	t.Run("Valid ordering across batches - bug regression test", func(t *testing.T) {
		// Setup mock UTXO store
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		// Create BlockAssembler with test settings
		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 50 // Set batch size to trigger batching

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Create test transactions:
		// - Transactions 0-49: First batch, each has a mined parent
		// - Transactions 50-99: Second batch, each depends on a transaction from first batch
		// - Transaction 100: Third batch, depends on transaction 50 from second batch
		//
		// This specifically tests the bug where the variable 'i' was shadowed at line 1644,
		// causing incorrect currentIdx calculation for transactions in later batches.

		unminedTxs := make([]*utxo.UnminedTransaction, 0, 101)
		parentTxHashes := make([]chainhash.Hash, 50)

		// Create first batch (50 transactions)
		for i := 0; i < 50; i++ {
			parentHash := chainhash.Hash{}
			for j := 0; j < len(parentHash); j++ {
				parentHash[j] = byte(i)
			}
			parentTxHashes[i] = parentHash

			tx := &utxo.UnminedTransaction{
				Node: &subtree.Node{
					Hash:        parentHash,
					Fee:         1000,
					SizeInBytes: 250,
				},
				TxInpoints: &subtree.TxInpoints{
					ParentTxHashes: []chainhash.Hash{{}}, // Empty hash means mined parent
					Idxs:           [][]uint32{{0}},
				},
				CreatedAt: i,
			}
			unminedTxs = append(unminedTxs, tx)
		}

		// Create second batch (50 transactions)
		childTxHashes := make([]chainhash.Hash, 50)
		for i := 0; i < 50; i++ {
			childHash := chainhash.Hash{}
			for j := 0; j < len(childHash); j++ {
				childHash[j] = byte(50 + i)
			}
			childTxHashes[i] = childHash

			tx := &utxo.UnminedTransaction{
				Node: &subtree.Node{
					Hash:        childHash,
					Fee:         1000,
					SizeInBytes: 250,
				},
				TxInpoints: &subtree.TxInpoints{
					ParentTxHashes: []chainhash.Hash{parentTxHashes[i]},
					Idxs:           [][]uint32{{0}},
				},
				CreatedAt: 50 + i,
			}
			unminedTxs = append(unminedTxs, tx)
		}

		// Create third batch (1 transaction) - this is where the bug would manifest
		grandchildHash := chainhash.Hash{}
		for j := 0; j < len(grandchildHash); j++ {
			grandchildHash[j] = byte(100)
		}

		grandchildTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        grandchildHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{childTxHashes[0]}, // Depends on tx at index 50
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 100,
		}
		unminedTxs = append(unminedTxs, grandchildTx)

		// Setup mock responses for BatchDecorate
		// The mock needs to respond to BatchDecorate calls for each batch
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					// Check if it's an empty hash (mined parent) or a known unmined parent
					isEmptyHash := true
					for _, b := range unresolved.Hash {
						if b != 0 {
							isEmptyHash = false
							break
						}
					}

					if isEmptyHash {
						// Mined parent - return with BlockIDs
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
							Locked:       false,
						}
					} else {
						// Unmined parent - check if it's in our list
						found := false
						for _, tx := range unminedTxs {
							if tx.Hash.IsEqual(&unresolved.Hash) {
								found = true
								break
							}
						}

						if found {
							// Unmined parent in our list
							unresolved.Data = &meta.Data{
								BlockIDs:     []uint32{},
								UnminedSince: 1,
								Locked:       false,
							}
						} else {
							// Parent not found - this would cause transaction to be skipped
							unresolved.Err = errors.ErrNotFound
						}
					}
				}
			}).
			Return(nil)

		// Create bestBlockHeaderIDsMap
		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		// Call validateParentChain
		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// All 101 transactions should be valid
		// Before the fix, the bug would cause the grandchild transaction (and potentially others)
		// to be incorrectly filtered due to wrong currentIdx calculation
		require.Equal(t, 101, len(validTxs), "All transactions should be valid with correct parent ordering")

		// Verify the grandchild transaction is included
		foundGrandchild := false
		for _, tx := range validTxs {
			if tx.Hash.IsEqual(&grandchildHash) {
				foundGrandchild = true
				break
			}
		}
		require.True(t, foundGrandchild, "Grandchild transaction should be included in valid transactions")

		mockStore.AssertExpectations(t)
	})

	t.Run("Invalid ordering - parent after child", func(t *testing.T) {
		// Setup mock UTXO store
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 50

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Create transactions with INVALID ordering:
		// Transaction at index 0 depends on transaction at index 1 (parent comes after child)

		parentHash := chainhash.Hash{}
		for j := 0; j < len(parentHash); j++ {
			parentHash[j] = byte(1)
		}

		childHash := chainhash.Hash{}
		for j := 0; j < len(childHash); j++ {
			childHash[j] = byte(2)
		}

		// Child transaction (index 0) - depends on parent at index 1
		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        childHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{parentHash},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 0,
		}

		// Parent transaction (index 1) - no unmined parents
		parentTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        parentHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{{}}, // Empty hash = mined parent
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 1,
		}

		unminedTxs := []*utxo.UnminedTransaction{childTx, parentTx}

		// Setup mock responses
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					isEmptyHash := true
					for _, b := range unresolved.Hash {
						if b != 0 {
							isEmptyHash = false
							break
						}
					}

					if isEmptyHash {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
							Locked:       false,
						}
					} else {
						// Check if it's the parent transaction
						if unresolved.Hash.IsEqual(&parentHash) {
							unresolved.Data = &meta.Data{
								BlockIDs:     []uint32{},
								UnminedSince: 1,
								Locked:       false,
							}
						} else {
							unresolved.Err = errors.ErrNotFound
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		// Call validateParentChain
		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// Only the parent should be valid, child should be skipped due to invalid ordering
		require.Equal(t, 1, len(validTxs), "Only parent transaction should be valid")
		require.Equal(t, parentHash.String(), validTxs[0].Hash.String(), "Valid transaction should be the parent")

		mockStore.AssertExpectations(t)
	})
}

// TestValidateParentChain_RejectsChildOfConflictingParent is the regression test for the
// late-cascade hole: if a parent was flipped to Conflicting=true after a child was already
// admitted to the unmined list, validateParentChain must refuse to re-admit the child.
// Without this check, block validation later rejects the template with
// "parent transaction … has no block IDs" (bad-txns-inputs-missingorspent).
func TestValidateParentChain_RejectsChildOfConflictingParent(t *testing.T) {
	ctx := context.Background()

	mockStore := new(utxo.MockUtxostore)
	logger := ulogger.TestLogger{}

	testSettings := &settings.Settings{}
	testSettings.BlockAssembly.ParentValidationBatchSize = 50
	testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

	blockAssembler := &BlockAssembler{
		utxoStore: mockStore,
		settings:  testSettings,
		logger:    logger,
	}

	var parentHash chainhash.Hash
	for j := range parentHash {
		parentHash[j] = 0xAA
	}

	var childHash chainhash.Hash
	for j := range childHash {
		childHash[j] = 0xBB
	}

	// Only the child is in the unmined list. The conflicting parent has already been
	// filtered out by the server-side unmined-iterator filter, so validateParentChain
	// never sees it as an unmined tx — only as a fetched parent metadata record.
	childTx := &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        childHash,
			Fee:         1000,
			SizeInBytes: 250,
		},
		TxInpoints: &subtree.TxInpoints{
			ParentTxHashes: []chainhash.Hash{parentHash},
			Idxs:           [][]uint32{{0}},
		},
		CreatedAt: 1,
	}
	unminedTxs := []*utxo.UnminedTransaction{childTx}

	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				if unresolved.Hash.IsEqual(&parentHash) {
					unresolved.Data = &meta.Data{
						BlockIDs:     []uint32{},
						UnminedSince: 1,
						Conflicting:  true,
					}
				}
			}
		}).
		Return(nil)

	// MarkConflictingRecursively → SetConflicting on the cascaded child.
	// First call: marks the child, returns no further spending children → cascade ends.
	mockStore.On("SetConflicting", mock.Anything, mock.MatchedBy(func(hashes []chainhash.Hash) bool {
		return len(hashes) == 1 && hashes[0].IsEqual(&childHash)
	}), true).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil).Once()

	bestBlockHeaderIDsMap := map[uint32]bool{1: true}

	validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
	require.NoError(t, err)
	require.Empty(t, validTxs,
		"child of a conflicting parent must be filtered with OnRestartRemoveInvalidParentChainTxs=true")

	mockStore.AssertExpectations(t)
}

// TestValidateParentChain_BatchDecorateRequestsConflicting verifies that validateParentChain
// asks the store for the Conflicting field. Without this field in the decorate call, the
// defence-in-depth check would read a zero value and always accept the child.
func TestValidateParentChain_BatchDecorateRequestsConflicting(t *testing.T) {
	ctx := context.Background()

	mockStore := new(utxo.MockUtxostore)
	logger := ulogger.TestLogger{}

	testSettings := &settings.Settings{}
	testSettings.BlockAssembly.ParentValidationBatchSize = 50

	blockAssembler := &BlockAssembler{
		utxoStore: mockStore,
		settings:  testSettings,
		logger:    logger,
	}

	var parentHash chainhash.Hash
	for j := range parentHash {
		parentHash[j] = 0xCC
	}

	var childHash chainhash.Hash
	for j := range childHash {
		childHash[j] = 0xDD
	}

	childTx := &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        childHash,
			Fee:         1000,
			SizeInBytes: 250,
		},
		TxInpoints: &subtree.TxInpoints{
			ParentTxHashes: []chainhash.Hash{parentHash},
			Idxs:           [][]uint32{{0}},
		},
	}

	var capturedFields []fields.FieldName
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			// MockUtxostore.BatchDecorate forwards the variadic fields as a single slice,
			// so args.Get(2) is the whole []fields.FieldName.
			capturedFields = args.Get(2).([]fields.FieldName)
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				if unresolved.Hash.IsEqual(&parentHash) {
					unresolved.Data = &meta.Data{
						BlockIDs:     []uint32{1},
						UnminedSince: 0,
					}
				}
			}
		}).
		Return(nil)

	_, err := blockAssembler.validateParentChain(ctx, []*utxo.UnminedTransaction{childTx}, map[uint32]bool{1: true})
	require.NoError(t, err)

	require.Contains(t, capturedFields, fields.Conflicting,
		"validateParentChain must request fields.Conflicting from the store")
}

// TestValidateParentChain_RecursivelyFiltersConflictingDescendants is the regression test
// for the cascade hole: when a parent is conflicting, only its DIRECT children were
// previously filtered. Grandchildren and deeper descendants leaked into block assembly,
// producing invalid blocks. The fix tracks rejected hashes in-memory across the run so
// that any tx whose ancestor was filtered as conflicting is also filtered AND propagated
// to the UTXO store as conflicting.
func TestValidateParentChain_RecursivelyFiltersConflictingDescendants(t *testing.T) {
	ctx := context.Background()

	mockStore := new(utxo.MockUtxostore)
	logger := ulogger.TestLogger{}

	testSettings := &settings.Settings{}
	testSettings.BlockAssembly.ParentValidationBatchSize = 50
	testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

	blockAssembler := &BlockAssembler{
		utxoStore: mockStore,
		settings:  testSettings,
		logger:    logger,
	}

	// Chain: conflictingParent -> childB -> childC -> childD
	// conflictingParent is NOT in the unmined list (filtered by iterator), but is
	// returned by BatchDecorate with Conflicting=true.
	// childB, childC, childD ARE in the unmined list. All three must be filtered.
	var conflictingParentHash chainhash.Hash
	for j := range conflictingParentHash {
		conflictingParentHash[j] = 0xA0
	}

	var bHash chainhash.Hash
	for j := range bHash {
		bHash[j] = 0xB0
	}

	var cHash chainhash.Hash
	for j := range cHash {
		cHash[j] = 0xC0
	}

	var dHash chainhash.Hash
	for j := range dHash {
		dHash[j] = 0xD0
	}

	childB := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: bHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: &subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{conflictingParentHash}, Idxs: [][]uint32{{0}}},
		CreatedAt:  1,
	}
	childC := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: cHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: &subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{bHash}, Idxs: [][]uint32{{0}}},
		CreatedAt:  2,
	}
	childD := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: dHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: &subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{cHash}, Idxs: [][]uint32{{0}}},
		CreatedAt:  3,
	}

	unminedTxs := []*utxo.UnminedTransaction{childB, childC, childD}

	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				switch {
				case unresolved.Hash.IsEqual(&conflictingParentHash):
					// Real conflicting parent — store says Conflicting=true.
					unresolved.Data = &meta.Data{
						BlockIDs:     []uint32{},
						UnminedSince: 1,
						Conflicting:  true,
					}
				case unresolved.Hash.IsEqual(&bHash), unresolved.Hash.IsEqual(&cHash):
					// Cascade-affected. Store does NOT yet reflect conflicting
					// (that's the bug — the cascade marker hadn't propagated).
					// Without the fix, these would pass the parentMeta.Conflicting
					// check and leak through.
					unresolved.Data = &meta.Data{
						BlockIDs:     []uint32{},
						UnminedSince: 1,
						Conflicting:  false,
					}
				}
			}
		}).
		Return(nil)

	// All three cascaded txs must be marked conflicting in the store. The map order
	// is non-deterministic, so we just assert the call happens with all three hashes
	// in the first batch (childB, childC, childD all reach SetConflicting since the
	// in-memory cascade tracked them up-front).
	mockStore.On("SetConflicting", mock.Anything, mock.MatchedBy(func(hashes []chainhash.Hash) bool {
		if len(hashes) != 3 {
			return false
		}
		seen := map[chainhash.Hash]bool{}
		for _, h := range hashes {
			seen[h] = true
		}
		return seen[bHash] && seen[cHash] && seen[dHash]
	}), true).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil).Once()

	bestBlockHeaderIDsMap := map[uint32]bool{1: true}

	validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
	require.NoError(t, err)
	require.Empty(t, validTxs,
		"all descendants of a conflicting parent must be cascade-filtered, not just direct children")

	mockStore.AssertExpectations(t)
}

// TestValidateParentChain_RecursivelyFiltersOtherInvalidDescendants verifies that the
// in-memory cascade also works for non-conflicting rejection reasons (e.g. parent missing
// from store). These are filtered out but NOT marked conflicting — the parent isn't
// conflicting, just orphaned, and we shouldn't flip a tx to conflicting on weak evidence.
func TestValidateParentChain_RecursivelyFiltersOtherInvalidDescendants(t *testing.T) {
	ctx := context.Background()

	mockStore := new(utxo.MockUtxostore)
	logger := ulogger.TestLogger{}

	testSettings := &settings.Settings{}
	testSettings.BlockAssembly.ParentValidationBatchSize = 50
	testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

	blockAssembler := &BlockAssembler{
		utxoStore: mockStore,
		settings:  testSettings,
		logger:    logger,
	}

	// Chain: missingParent (not in store, not in list) -> childB (in list) -> childC (in list)
	var missingParentHash chainhash.Hash
	for j := range missingParentHash {
		missingParentHash[j] = 0x10
	}

	var bHash chainhash.Hash
	for j := range bHash {
		bHash[j] = 0x20
	}

	var cHash chainhash.Hash
	for j := range cHash {
		cHash[j] = 0x30
	}

	childB := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: bHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: &subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{missingParentHash}, Idxs: [][]uint32{{0}}},
		CreatedAt:  1,
	}
	childC := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: cHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: &subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{bHash}, Idxs: [][]uint32{{0}}},
		CreatedAt:  2,
	}

	unminedTxs := []*utxo.UnminedTransaction{childB, childC}

	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				switch {
				case unresolved.Hash.IsEqual(&missingParentHash):
					// Truly missing — BatchDecorate signals not-found.
					unresolved.Err = errors.ErrNotFound
				case unresolved.Hash.IsEqual(&bHash):
					// Without the cascade fix, B's metadata looks fine and C would slip through.
					unresolved.Data = &meta.Data{
						BlockIDs:     []uint32{},
						UnminedSince: 1,
						Conflicting:  false,
					}
				}
			}
		}).
		Return(nil)

	// SetConflicting must NOT be called — these aren't conflicting, just orphaned.
	// (mockStore.AssertExpectations will fail if any On() expectation is unmet, but
	// no expectation was set for SetConflicting, so calling it would surface as an
	// "unexpected call" panic — exactly what we want.)

	bestBlockHeaderIDsMap := map[uint32]bool{1: true}

	validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
	require.NoError(t, err)
	require.Empty(t, validTxs,
		"descendants of a missing parent must be cascade-filtered too")

	mockStore.AssertExpectations(t)
}
