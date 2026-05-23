package smoke

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestUnifiedLoggingExample demonstrates the unified logging feature.
// When UseUnifiedLogger is enabled, all application logs are routed through t.Logf
// with consistent formatting: [TestName:serviceName] LEVEL: message
//
// This provides:
// - Consistent log format between test code and application code
// - All logs captured by go test and shown on failure
// - Clear identification of which service/component produced each log line
// - Proper line numbers in test output
//
// To see the unified logging output, run this test with -v flag:
//
//	go test -v -run TestUnifiedLoggingExample ./test/e2e/daemon/ready/
func TestUnifiedLoggingExample(t *testing.T) {
	// Skip in CI - this is a demonstration test meant for local development
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping example test in CI - run locally with -v to see unified logging output")
	}

	// Create daemon with unified logging enabled
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		UseUnifiedLogger: true, // Enable unified logging - all logs go through t.Logf
		UTXOStoreType:    "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// Any additional settings overrides
			},
		),
	})

	defer td.Stop(t)

	// Test-level logging using td.Log() - provides consistent prefix
	// Output: [TestUnifiedLoggingExample] Starting unified logging example test
	td.Log("Starting unified logging example test")

	// Mine blocks to get a spendable coinbase
	// Application logs from blockchain service will appear as:
	// [TestUnifiedLoggingExample:blockchain] INFO: Processing block...
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	td.Log("Got spendable coinbase: %s", coinbaseTx.TxID())

	// Create a transaction
	// Application logs from propagation service will appear as:
	// [TestUnifiedLoggingExample:propagation] INFO: Processing transaction...
	newTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)
	td.Log("Created transaction: %s", newTx.TxID())

	// Send transaction through propagation
	err := td.PropagationClient.ProcessTransaction(td.Ctx, newTx)
	require.NoError(t, err)
	td.Log("Transaction sent successfully")

	// Wait for block assembly to process the transaction
	// Logs from blockassembly service will appear as:
	// [TestUnifiedLoggingExample:blockassembly] INFO: Added transaction to candidate...
	td.WaitForBlockAssemblyToProcessTx(t, newTx.TxIDChainHash().String())
	td.Log("Transaction processed by block assembly")

	// Mine a block containing the transaction
	block := td.MineAndWait(t, 1)
	td.Log("Mined block at height %d with hash %s", block.Height, block.Hash().String())

	// You can also use td.Logger directly - it routes through t.Logf too
	// Output: [TestUnifiedLoggingExample] INFO: Direct logger call example
	td.Logger.Infof("Direct logger call example")

	td.Log("Unified logging example test completed successfully")
}
