package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

func setupDoubleSpendTest(t *testing.T, utxoStoreType string, blockOffset ...uint32) (td *daemon.TestDaemon, coinbaseTx1, txOriginal, txDoubleSpend *bt.Tx, block102 *model.Block, tx *bt.Tx) {
	// Default to aerospike if not specified
	if utxoStoreType == "" {
		utxoStoreType = "aerospike"
	}

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	// Set the FSM state to RUNNING...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	// Use different block heights for different tests to avoid UTXO conflicts
	blockHeight := uint32(1)
	if len(blockOffset) > 0 && blockOffset[0] > 0 && blockOffset[0] <= 100 {
		blockHeight = blockOffset[0]
	}

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, blockHeight)
	require.NoError(t, err)

	coinbaseTx1 = block1.CoinbaseTx
	// t.Logf("Coinbase has %d outputs", len(coinbaseTx.Outputs))

	txOriginal = td.CreateTransaction(t, coinbaseTx1, 0)
	txDoubleSpend = td.CreateTransaction(t, coinbaseTx1, 0)

	err1 := td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal)
	require.NoError(t, err1)

	// td.Logger.SkipCancelOnFail(true)

	err2 := td.PropagationClient.ProcessTransaction(td.Ctx, txDoubleSpend)
	require.Error(t, err2) // This should fail as it is a double spend

	// td.Logger.SkipCancelOnFail(false)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block102, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	require.Equal(t, uint64(2), block102.TransactionCount)

	// Create another transaction from a different block
	// Use blockHeight+1 to ensure we're using a different block
	block2Height := blockHeight + 1
	if block2Height > 100 {
		block2Height = 2
	}
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, block2Height)
	require.NoError(t, err)

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx2)
	require.NoError(t, err)

	return td, coinbaseTx1, txOriginal, txDoubleSpend, block102, tx2
}
