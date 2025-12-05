package tnb

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/testcontainers"
	"github.com/stretchr/testify/require"
)

func TestUtxoStore(t *testing.T) {
	_, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		Path:        "..",
		ComposeFile: "docker-compose-host.yml",
		Profiles:    []string{"postgres"},
	})
	require.NoError(t, err)

	settingsNode1 := settings.NewSettings("docker.host.teranode1.daemon")
	settingsNode1.Propagation.GRPCAddresses = []string{"localhost:8084", "localhost:28084", "localhost:38084"}

	utxoStore, err := url.Parse("postgres://miner1:miner1@localhost:15432/teranode1")
	require.NoError(t, err)

	settingsNode1.UtxoStore.UtxoStore = utxoStore
	settingsNode1.Validator.UseLocalValidator = false

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
	})

	t.Cleanup(func() {
		td.Stop(t)
	})

	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	parentTx := block1.CoinbaseTx

	newTx := td.CreateTransaction(t, parentTx)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, newTx)
	require.NoError(t, err)

	// Wait for transaction to be processed
	delay := td.Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(delay)
	}

	utxoMeta, err := td.UtxoStore.Get(td.Ctx, newTx.TxIDChainHash())
	require.NoError(t, err)

	require.Equal(t, newTx.TxID(), utxoMeta.Tx.TxID(), "Transaction ID mismatch")
	require.Len(t, utxoMeta.TxInpoints.ParentTxHashes, 1, "Should have exactly one parent transaction")
	require.Equal(t, parentTx.TxID(), utxoMeta.TxInpoints.ParentTxHashes[0].String(), "Parent transaction hash mismatch")
	require.Empty(t, utxoMeta.BlockIDs, "BlockIDs should be empty for unconfirmed transaction")
	require.Greater(t, utxoMeta.Fee, uint64(0), "Fee should be greater than 0")
	require.Greater(t, utxoMeta.SizeInBytes, uint64(0), "Size should be greater than 0")
	require.False(t, utxoMeta.IsCoinbase, "Should not be a coinbase transaction")
	require.False(t, utxoMeta.Frozen, "Should not be frozen")
	require.False(t, utxoMeta.Conflicting, "Should not be conflicting")
	require.False(t, utxoMeta.Locked, "Should not be locked")
	require.Equal(t, uint32(0), utxoMeta.LockTime, "LockTime should be 0")
}
