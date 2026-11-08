package smoke

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/lanestamp"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// TestLanePositionsOnNewRecords runs a node with lane positions on and checks
// what the records carry: a transaction validated by the node gets a lane
// position, a second process creating records against the same blockchain
// service gets a different lane, and after a restart the node carries on in
// its lane above every position it used before.
func TestLanePositionsOnNewRecords(t *testing.T) {
	settingsFunc := test.ComposeSettings(
		test.SystemTestSettings(),
		func(s *settings.Settings) {
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.UtxoStore.LanePositions = true
			s.BlockChain.LaneRangeSize = 1000
		},
	)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        "aerospike",
		SkipContainerCleanup: true,
		SettingsOverrideFunc: settingsFunc,
	})

	containerManager := td.GetContainerManager()
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))
	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 4}))

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block4, 10*time.Second, true)

	lanePosition := func(td *daemon.TestDaemon, tx *bt.Tx) uint64 {
		t.Helper()

		md, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)

		return md.LanePosition
	}

	// The node validates tx0 and its validator stamps it.
	tx0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(2, 100_000),
	)

	require.Eventually(t, func() bool {
		// Retried only for the store's readiness just after start, as the
		// other daemon tests do.
		return td.PropagationClient.ProcessTransaction(td.Ctx, tx0) == nil
	}, 10*time.Second, 200*time.Millisecond)

	first := lanePosition(td, tx0)
	require.True(t, lane.Valid(first), "a record the node validated carries a lane position")

	firstLane, firstPos := lane.Unpack(first)
	t.Logf("tx0 %s: lane %d position %d", tx0.TxIDChainHash(), firstLane, firstPos)

	// A second process, against the same blockchain service, gets its own lane.
	other, err := utxofactory.NewStore(td.Ctx, td.Logger, td.Settings, "lane-test", false)
	require.NoError(t, err)

	alloc := lane.NewAllocator(td.BlockchainClient, "second-process@test", filepath.Join(t.TempDir(), "lane"), td.Logger)
	defer alloc.Close()

	startCtx, cancel := context.WithTimeout(td.Ctx, 10*time.Second)
	require.NoError(t, alloc.Start(startCtx))
	cancel()

	otherTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block2.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 100_000),
	)

	_, _, err = lanestamp.New(other, alloc, td.Logger).SpendAndCreate(td.Ctx, otherTx, 5, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	otherLane, otherPos := lane.Unpack(lanePosition(td, otherTx))
	t.Logf("second process tx %s: lane %d position %d", otherTx.TxIDChainHash(), otherLane, otherPos)
	require.NotEqual(t, firstLane, otherLane, "two processes take two lanes")
	require.Equal(t, lane.FirstPosition, otherPos, "a new lane starts at position 1")

	// Restart the node on the same data folder and store.
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,
		SkipRemoveDataDir:    true,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: settingsFunc,
	})
	defer td.Stop(t)

	tx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(tx0, 0),
		transactions.WithP2PKHOutputs(1, 90_000),
	)

	require.Eventually(t, func() bool {
		return td.PropagationClient.ProcessTransaction(td.Ctx, tx1) == nil
	}, 10*time.Second, 200*time.Millisecond)

	afterLane, afterPos := lane.Unpack(lanePosition(td, tx1))
	t.Logf("tx1 after restart %s: lane %d position %d", tx1.TxIDChainHash(), afterLane, afterPos)
	require.Equal(t, firstLane, afterLane, "the restarted node carries on in its saved lane")
	require.Greater(t, afterPos, firstPos, "positions only increase within a lane across a restart")
}
