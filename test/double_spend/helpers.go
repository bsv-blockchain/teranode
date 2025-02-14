//go:build test_sequentially

package doublespendtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

func setupDoubleSpendTest(t *testing.T, utxoStoreOverride string) (dst *DoubleSpendTester, coinbaseTx1, txOriginal, txDoubleSpend *bt.Tx, block102 *model.Block) {
	dst = NewDoubleSpendTester(t, utxoStoreOverride)

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

func (dst *DoubleSpendTester) Stop() {
	dst.tracingDeferFn()
	_ = dst.d.Stop()
	dst.ctxCancel()
}

func createTransaction(t *testing.T, parentTx *bt.Tx, privkey *bec.PrivateKey, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(privkey.PubKey().SerialiseCompressed(), amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privkey})
	require.NoError(t, err)

	return tx
}

// TODO should be moved into a test helper package
func initAerospike() (string, func() error, error) {
	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithImage("aerospike:ce-6.4.0.7_2"))
	if err != nil {
		return "", nil, err
	}

	cleanup := func() error {
		return container.Terminate(ctx)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", cleanup, err
	}

	port, err := container.ServicePort(ctx)
	if err != nil {
		return "", cleanup, err
	}

	// raw client to be able to do gets and cleanup
	client, aeroErr := uaerospike.NewClient(host, port)
	if aeroErr != nil {
		return "", cleanup, aeroErr
	}

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%s&externalStore=file://./data/externalStore", host, port, "test", "test", "10m")

	return aerospikeContainerURL, func() error {
		client.Close()
		return cleanup()
	}, nil
}
