package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Simple tests focused on achieving code coverage

func TestPreserveParentsOfOldUnminedTransactions_Coverage(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UnminedTxRetention = 5

	t.Run("early return when block height too low", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 3, tSettings, logger)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		// Should not call any store methods
		mockStore.AssertNotCalled(t, "QueryOldUnminedTransactions")
	})

	t.Run("handles query error", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		mockStore.On("QueryOldUnminedTransactions", mock.Anything, uint32(5)).
			Return([]chainhash.Hash(nil), errors.NewStorageError("query failed"))

		count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 10, tSettings, logger)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Contains(t, err.Error(), "failed to query old unmined transactions")
		mockStore.AssertExpectations(t)
	})

	t.Run("preserves parents successfully", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		hash1 := chainhash.HashH([]byte("test1"))
		// Use a transaction with inputs to trigger PreserveTransactions
		tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

		// Create TxInpoints from the transaction
		txInpoints, _ := subtree.NewTxInpointsFromTx(tx)

		mockStore.On("QueryOldUnminedTransactions", mock.Anything, uint32(5)).
			Return([]chainhash.Hash{hash1}, nil)

		mockStore.On("Get", mock.Anything, &hash1, mock.Anything).
			Return(&meta.Data{TxInpoints: txInpoints}, nil)

		mockStore.On("PreserveTransactions", mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 10, tSettings, logger)

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
		mockStore.AssertExpectations(t)
	})
}

func TestPreserveSingleUnminedTransactionParents_Coverage(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	t.Run("handles Get failure", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		txHash := chainhash.HashH([]byte("test"))

		mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
			Return(nil, errors.NewProcessingError("not found"))

		err := preserveSingleUnminedTransactionParents(ctx, mockStore, &txHash, 1000, &settings.Settings{}, logger)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get transaction")
		mockStore.AssertExpectations(t)
	})

	t.Run("handles nil transaction", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		txHash := chainhash.HashH([]byte("test"))

		// Return empty TxInpoints (no parent hashes)
		mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
			Return(&meta.Data{TxInpoints: subtree.NewTxInpoints()}, nil)

		err := preserveSingleUnminedTransactionParents(ctx, mockStore, &txHash, 1000, &settings.Settings{}, logger)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has no inpoints")
		mockStore.AssertExpectations(t)
	})

	t.Run("continues when preserve fails", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		txHash := chainhash.HashH([]byte("test"))

		// Use a transaction with inputs to trigger PreserveTransactions
		tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

		// Create TxInpoints from the transaction
		txInpoints, _ := subtree.NewTxInpointsFromTx(tx)

		mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
			Return(&meta.Data{TxInpoints: txInpoints}, nil)

		mockStore.On("PreserveTransactions", mock.Anything, mock.Anything, mock.Anything).
			Return(errors.NewProcessingError("preserve failed"))

		cleanupErr := preserveSingleUnminedTransactionParents(ctx, mockStore, &txHash, 1000, &settings.Settings{}, logger)

		assert.Error(t, cleanupErr) // Should return error when preserve fails
		mockStore.AssertExpectations(t)
	})

	t.Run("handles transaction with no inputs", func(t *testing.T) {
		mockStore := new(MockUtxostore)
		txHash := chainhash.HashH([]byte("test"))

		tx := bt.NewTx()
		tx.Version = 1
		// No inputs - should skip PreserveTransactions

		// Create empty TxInpoints (no parent hashes)
		txInpoints := subtree.NewTxInpoints()

		mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
			Return(&meta.Data{TxInpoints: txInpoints}, nil)

		cleanupErr := preserveSingleUnminedTransactionParents(ctx, mockStore, &txHash, 1000, &settings.Settings{}, logger)

		assert.Error(t, cleanupErr)
		assert.Contains(t, cleanupErr.Error(), "has no inpoints")
		mockStore.AssertExpectations(t)
		// Should not call PreserveTransactions for transaction with no inputs
		mockStore.AssertNotCalled(t, "PreserveTransactions")
	})
}
