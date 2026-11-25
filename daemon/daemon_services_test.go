package daemon

import (
	"context"
	"net/url"
	"strings"
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

// Start decides from the services the command line switches on: block assembly alone keeps a lane
// index, and a process running none of the services that check parents keeps none.
func TestLaneReaderWanted(t *testing.T) {
	services := []string{serviceSubtreeValidationFormal, serviceBlockValidationFormal, serviceValidatorFormal,
		serviceBlockAssemblyFormal, servicePropagationFormal, serviceLegacyFormal, serviceRPCFormal}

	only := func(on string) []string {
		args := make([]string, 0, len(services))

		for _, s := range services {
			flag := "0"
			if s == on {
				flag = "1"
			}

			args = append(args, "-"+strings.ToLower(s)+"="+flag)
		}

		return args
	}

	require.False(t, laneReaderWanted(only(""), true), "none of them")

	for _, s := range []string{serviceSubtreeValidationFormal, serviceBlockValidationFormal, serviceValidatorFormal, serviceBlockAssemblyFormal} {
		require.True(t, laneReaderWanted(only(s), false), s)
	}

	for _, s := range []string{servicePropagationFormal, serviceLegacyFormal, serviceRPCFormal} {
		require.True(t, laneReaderWanted(only(s), true), "%s with a local validator", s)
		require.False(t, laneReaderWanted(only(s), false), "%s with a remote validator", s)
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
