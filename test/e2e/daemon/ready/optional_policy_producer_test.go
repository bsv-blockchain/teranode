package smoke

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestValidatorRejectsLowFeeWithoutPolicyRejectedTopic(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(test.SystemTestSettings(), func(s *settings.Settings) {
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.Policy.MinMiningTxFee = 0.000001 // 100 sat/kB, expressed in BSV/kB.
			s.Kafka.TxPolicyRejectedConfig = nil
		}),
	})
	defer td.Stop(t, true)
	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, coinbaseTx.Outputs[0].Satoshis-1),
	)
	require.Equal(t, uint64(1), coinbaseTx.Outputs[0].Satoshis-tx.Outputs[0].Satoshis)
	require.Greater(t, (uint64(len(tx.Bytes()))*100+999)/1000, uint64(1))

	// Use synchronous validation; propagation may only acknowledge queueing.
	client, err := validator.NewClient(td.Ctx, td.Logger, td.Settings)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Validate(td.Ctx, tx, 0)
	require.ErrorContains(t, err, "transaction fee is too low")
	td.VerifyNotInUtxoStore(t, tx)
	td.VerifyNotInBlockAssembly(t, tx)
}
