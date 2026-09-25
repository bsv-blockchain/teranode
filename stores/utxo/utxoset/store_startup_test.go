package utxoset

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestStoreRefusesToStartWithThePrunerSkippingCatchup: on this store the pruner also runs the
// deep stamp, so pruner_skipDuringCatchup would switch off both the stamp and every window
// drop for the whole of catch-up and fill the disk in silence. The store refuses to open rather
// than run that way, before any write, and the message names the lever an operator who wants
// an archive should use instead.
func TestStoreRefusesToStartWithThePrunerSkippingCatchup(t *testing.T) {
	ctx := context.Background()

	u, err := url.Parse(testDSN(t))
	require.NoError(t, err)

	tSettings := settings.NewSettings()
	tSettings.Pruner.SkipDuringCatchup = true

	s, err := New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.Nil(t, s)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "a configuration refusal, not a storage fault: %v", err)
	require.Contains(t, err.Error(), "pruner_skipDuringCatchup")
	require.Contains(t, err.Error(), "has no off switch")
	require.Contains(t, err.Error(), "utxostore_retainWindowsIndefinitely")

	// The same settings with the flag off open normally, so the refusal is the flag's alone.
	tSettings.Pruner.SkipDuringCatchup = false

	s, err = New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))
}
