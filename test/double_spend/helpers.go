//go:build test_full

package doublespendtest

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

func setupDoubleSpendTest(t *testing.T) (dst *DoubleSpendTester, coinbaseTx1, txOriginal, txDoubleSpend *bt.Tx, block102 *model.Block) {
	dst = NewDoubleSpendTester(t)

	// Set the FSM state to RUNNING...
	err := dst.blockchainClient.Run(dst.ctx, "test")
	require.NoError(t, err)

	err = dst.blockAssemblyClient.GenerateBlocks(dst.ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	state := dst.waitForBlockHeight(t, 101, 5*time.Second)
	require.Equal(t, uint32(101), state.CurrentHeight)

	block1, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, 1)
	require.NoError(t, err)

	coinbaseTx1 = block1.CoinbaseTx
	// t.Logf("Coinbase has %d outputs", len(coinbaseTx.Outputs))

	txOriginal = createTransaction(t, coinbaseTx1, dst.privKey, 49e8)
	txDoubleSpend = createTransaction(t, coinbaseTx1, dst.privKey, 48e8)

	err1 := dst.propagationClient.ProcessTransaction(dst.ctx, txOriginal)
	require.NoError(t, err1)

	dst.logger.SkipCancelOnFail(true)

	err2 := dst.propagationClient.ProcessTransaction(dst.ctx, txDoubleSpend)
	require.Error(t, err2) // This should fail as it is a double spend

	dst.logger.SkipCancelOnFail(false)

	err = dst.blockAssemblyClient.GenerateBlocks(dst.ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	state = dst.waitForBlockHeight(t, 102, 5*time.Second)
	require.Equal(t, uint32(102), state.CurrentHeight)

	block102, err = dst.blockchainClient.GetBlockByHeight(dst.ctx, 102)
	require.NoError(t, err)

	require.Equal(t, uint64(2), block102.TransactionCount)

	return dst, coinbaseTx1, txOriginal, txDoubleSpend, block102
}

func createTransaction(t *testing.T, coinbaseTx *bt.Tx, privkey *bec.PrivateKey, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(privkey.PubKey().SerialiseCompressed(), amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privkey})
	require.NoError(t, err)

	return tx
}
