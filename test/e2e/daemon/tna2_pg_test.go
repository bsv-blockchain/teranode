// How to run:
// cd test/tna
// go test -v -timeout 30s -run ^TestSingleTransactionPropagationWithUtxoPostgres$
// go test -v -timeout 30s -run ^TestMultipleTransactionsPropagationWithUtxoPostgres$
// go test -v -timeout 30s -run ^TestConcurrentTransactionsPropagationWithUtxoPostgres$

package smoke

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

func TestSingleTransactionPropagationWithUtxoPostgres(t *testing.T) {
	ctx := context.Background()

	td := utils.SetupPostgresTestDaemon(t, ctx, "test-single-txs")

	// Generate initial blocks
	_, err := td.CallRPC("generate", []any{101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx

	newTx := td.CreateTransaction(t, parenTx)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, newTx)
	require.NoError(t, err)

	// Check if the tx is into the UTXOStore
	txRes, errTxRes := td.UtxoStore.Get(ctx, newTx.TxIDChainHash())

	if txRes == nil {
		t.Fatalf("Tx not found: %v", txRes)
	}

	if errTxRes != nil {
		t.Fatalf("Failed to create and send transaction: %v", errTxRes)
	}

	err = td.BlockAssemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if err == nil {
		t.Logf("Test passed, Tx propagation success")
	} else {
		t.Fatalf("Test failed")
	}
}

func TestMultipleTransactionsPropagationWithUtxoPostgres(t *testing.T) {
	ctx := context.Background()

	td := utils.SetupPostgresTestDaemon(t, ctx, "test-multiple-txs")

	// Generate initial blocks
	_, err := td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx
	numTxs := 5

	// send numTxs
	_, sentTxHashes, err := td.CreateAndSendTxs(t, parenTx, numTxs)
	require.NoError(t, err)

	for i, txHash := range sentTxHashes {
		// Check if the tx is into the UTXOStore
		txRes, errTxRes := td.UtxoStore.Get(ctx, txHash)
		require.NoError(t, errTxRes)

		if txRes == nil {
			t.Fatalf("Tx %d not found in UTXOStore: %v", i, txHash)
		}

		err := td.BlockAssemblyClient.RemoveTx(ctx, txHash)
		if err != nil {
			t.Fatalf("Failed to remove tx %d (%v): %v", i, txHash, err)
		} else {
			t.Logf("Tx %d removed successfully (%v)", i, txHash)
		}
	}
}

func TestConcurrentTransactionsPropagationWithUtxoPostgres(t *testing.T) {
	ctx := context.Background()

	td := utils.SetupPostgresTestDaemon(t, ctx, "test-concurrent-txs")

	// Generate initial blocks
	_, err := td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Send transactions concurrently
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, sentTxHashes, err := td.CreateAndSendTxsConcurrently(t, parenTx)

	require.NoError(t, err)

	// Check if the tx is into the UTXOStore
	_, errTxRes := td.UtxoStore.Get(ctx, sentTxHashes[0])

	require.NoError(t, errTxRes)

	err = td.BlockAssemblyClient.RemoveTx(ctx, sentTxHashes[0])

	if err == nil {
		t.Logf("Test passed, concurrent tx propagation success")
	} else {
		t.Fatalf("Test failed")
	}
}
