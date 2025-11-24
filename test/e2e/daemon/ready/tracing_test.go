package smoke

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestCheckSpanPropagation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
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

	defer td.Stop(t, true)

	var err error

	ctx, _, endSpan := tracing.Tracer("test").Start(context.Background(), "TestCheckSpanPropagation")
	defer endSpan(err)

	tx := bt.NewTx()

	err = td.PropagationClient.ProcessTransaction(ctx, tx)
	require.Error(t, err)
}
