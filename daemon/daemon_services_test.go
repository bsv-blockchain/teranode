package daemon

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Subtree validation, block validation, the validator and block assembly each start the lane index
// on their own. A service that hands its transactions to a validator (propagation, legacy, RPC)
// starts it only when that validator runs in-process; a process running none of these does not.
func TestNeedsLaneReader(t *testing.T) {
	require.False(t, needsLaneReader(false, false, false, false, false, false), "none of them")
	require.False(t, needsLaneReader(false, false, false, false, false, true), "a local validator setting alone")
	require.False(t, needsLaneReader(false, false, false, false, true, false), "a validator client with a remote validator")
	require.True(t, needsLaneReader(false, false, false, false, true, true), "a validator client with a local validator")

	for i, name := range []string{"subtree validation", "block validation", "validator", "block assembly"} {
		flags := make([]bool, 4)
		flags[i] = true

		require.True(t, needsLaneReader(flags[0], flags[1], flags[2], flags[3], false, false), name)
	}
}

// At off, starting the lane reader starts nothing and clears a reader left from before.
func TestStartLaneReaderAtOffStartsNothing(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	shadow := test.CreateBaseTestSettings(t)
	shadow.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	store, err := blockchainsql.New(ulogger.TestLogger{}, u, shadow)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NotNil(t, lanereader.Start(ctx, ulogger.TestLogger{}, shadow, store))
	require.NotNil(t, lanereader.Current())

	d := &Daemon{}
	d.startLaneReader(ctx, test.CreateBaseTestSettings(t), func(string) ulogger.Logger { return ulogger.TestLogger{} })

	require.Nil(t, lanereader.Current())
}
