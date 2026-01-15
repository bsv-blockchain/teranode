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
