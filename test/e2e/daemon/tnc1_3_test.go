// How to run:
// go test -v -timeout 30s -run ^TestCandidateContainsAllTxs$
// go test -v -timeout 30s -run ^TestCheckHashPrevBlockCandidate$
// go test -v -timeout 30s -run ^TestCoinbaseTXAmount$
// go test -v -timeout 30s -run ^TestCoinbaseTXAmount2$
// $ go test -v -run "^TestCandidateContainsAllTxs$" ./test/smoke/tnc1_3_test.go
// $ go test -v -run "^TestCheckHashPrevBlockCandidate$" ./test/smoke/tnc1_3_test.go
// $ go test -v -run "^TestCoinbaseTXAmount$" ./test/smoke/tnc1_3_test.go
// $ go test -v -run "^TestCoinbaseTXAmount2$" ./test/smoke/tnc1_3_test.go

package smoke

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TODO: TestCandidateContainsAllTxs when EnableP2P it's fixed
func TestCheckHashPrevBlockCandidate(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	_, _, err = td.CreateAndSendTxs(t, block1.CoinbaseTx, 100)
	require.NoError(t, err)

	// Mine 1 block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine blocks")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Get the current best block header
	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	prevHash, errHash := chainhash.NewHash(mc.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockHeader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}

func TestCoinbaseTXAmount(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	coinbaseValueBlock := mc.CoinbaseValue
	td.Logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	// Get the current best block header
	_, bbhmeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	block, errblock := td.BlockchainClient.GetBlockByHeight(ctx, bbhmeta.Height)
	require.NoError(t, errblock)

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	td.Logger.Infof("Amount inside block coinbase tx: %d", amount)

	if amount != coinbaseValueBlock {
		t.Errorf("Error calculating Coinbase Tx amount")
	}
}

// TODO: Still check the Tx fees should add up to the coinbase value
func TestCoinbaseTXAmount2(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block, errblock := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, errblock)

	_, _, err = td.CreateAndSendTxs(t, block.CoinbaseTx, 35)
	require.NoError(t, err)

	// Get mining candidate
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	coinbaseValueBlock := mc.CoinbaseValue

	td.Logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	_, bbMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	bestBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, bbMeta.Height)
	require.NoError(t, err)

	coinbaseTX := bestBlock.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()

	if coinbaseValueBlock < amount {
		t.Errorf("Error calculating fees")
	}
}
