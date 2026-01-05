package smoke

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestCheckSpanPropagation(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Validator.UseLocalValidator = true
				s.TracingEnabled = true
				s.TracingSampleRate = 1.0
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	defer td.Stop(t, true)

	var err error

	ctx, _, endSpan := tracing.Tracer("test").Start(context.Background(), "TestCheckSpanPropagation")
	defer endSpan(err)

	tx := bt.NewTx()

	err = td.PropagationClient.ProcessTransaction(ctx, tx)
	require.Error(t, err)
}
