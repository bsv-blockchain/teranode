package smoke

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/services/rpc"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestCheckSpanPropagation(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: false,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			// settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
			settings.TracingEnabled = true
			settings.TracingSampleRate = 1.0
		},
	})

	var err error

	ctx, _, endSpan := tracing.Tracer("test").Start(context.Background(), "TestCheckSpanPropagation")
	defer endSpan(err)

	distributor, err := rpc.NewDistributor(ctx, td.Logger, td.Settings,
		rpc.WithBackoffDuration(1*time.Second),
		rpc.WithRetryAttempts(1),
		rpc.WithFailureTolerance(1),
	)
	require.NoError(t, err)

	tx := bt.NewTx()

	resp, err := distributor.SendTransaction(ctx, tx)
	require.Error(t, err)

	t.Logf("resp: %v", resp)
}
