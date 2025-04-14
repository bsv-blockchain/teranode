//go:build test_tna || debug || test_all

package tna

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	testkafka "github.com/bitcoin-sv/teranode/test/util/kafka"
	"github.com/bitcoin-sv/teranode/test/util/postgres"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestSingleTransactionPropagationWithUtxoPostgres(t *testing.T) {
	ctx := context.Background()

	pg, errPsql := postgres.RunPostgresTestContainer(ctx, "fairtx")
	require.NoError(t, errPsql)

	t.Cleanup(func() {
		if err := pg.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate postgres container: %v", err)
		}
	})

	gocore.Config().Set("POSTGRES_PORT", pg.Port)

	kafkaContainer, err := testkafka.RunTestContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = kafkaContainer.CleanUp()
	})

	gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))

	// Create custom settings with the PostgreSQL container's dynamic port
	customSettings := settings.NewSettings("dev.system.test.postgres")
	// Update the PostgreSQL connection settings with the dynamic port
	customSettings.PostgresCheckAddress = fmt.Sprintf("%s:%s", pg.Host, pg.Port)
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		KillTeranode:     true,
		SettingsOverride: customSettings,
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// set run state
	err = td.BlockchainClient.Run(td.Ctx, "test-tna2")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC("generate", []interface{}{101})
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
