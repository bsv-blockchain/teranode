package daemon

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Subtree validation, block validation, the validator and the pruner each start the lane index on
// their own. A service that hands its transactions to a validator (propagation, legacy, RPC) starts
// it only when that validator runs in-process; a process running none of these does not.
func TestNeedsLaneReader(t *testing.T) {
	require.False(t, needsLaneReader(false, false, false, false, false, false), "none of them")
	require.False(t, needsLaneReader(false, false, false, false, true, false), "a local validator setting alone")
	require.False(t, needsLaneReader(false, false, false, true, false, false), "a validator client with a remote validator")
	require.True(t, needsLaneReader(false, false, false, true, true, false), "a validator client with a local validator")

	for i, name := range []string{"subtree validation", "block validation", "validator", "pruner"} {
		flags := make([]bool, 4)
		flags[i] = true

		require.True(t, needsLaneReader(flags[0], flags[1], flags[2], false, false, flags[3]), name)
	}
}

// A process whose command line switches on only the pruner keeps a lane index; one with none of the
// services that judge does not.
func TestLaneReaderWantedForAPrunerOnlyProcess(t *testing.T) {
	off := func(apps ...string) []string {
		args := make([]string, 0, len(apps)+1)
		for _, app := range apps {
			args = append(args, "-"+strings.ToLower(app)+"=0")
		}

		return args
	}

	judging := []string{serviceSubtreeValidationFormal, serviceBlockValidationFormal, serviceValidatorFormal,
		servicePropagationFormal, serviceLegacyFormal, serviceRPCFormal}

	require.True(t, laneReaderWanted(append(off(judging...), "-"+strings.ToLower(servicePrunerFormal)+"=1"), false))
	require.False(t, laneReaderWanted(off(slices.Concat(judging, []string{servicePrunerFormal})...), false))
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
