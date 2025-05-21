//go:build test_tnc || debug

// How to run:
// go test -v -timeout 30s -tags "test_tnc" -run ^TestCheckPrevBlockHash$
// go test -v -timeout 30s -tags "test_tnc" -run ^TestPrevBlockHashAfterReorg$

package tnc

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestCheckPrevBlockHash(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate blocks
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	//Generate priv and pub key
	inputPrivKey := td.GetPrivateKey(t)

	outPrivKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	outPubKey := outPrivKey.PubKey()

	parenTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	tx := td.CreateTransactionWithOptions(t,
		daemon.WithInput(parenTx, 0, inputPrivKey),
		daemon.WithP2PKHOutputs(1, 10000, outPubKey),
	)

	// Send Alice to Bob transaction
	_, err = td.DistributorClient.SendTransaction(td.Ctx, tx)
	require.NoError(t, err)

	// Mine the block with the new transaction
	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine block")

	// Get the current best block header
	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that the mining candidate's previous hash matches the current best block
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match current best block")
}
