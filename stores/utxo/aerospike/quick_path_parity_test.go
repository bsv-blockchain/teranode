package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestQuickPathParity runs the two store-contract cases the quick-validation
// no-mutation regression depends on against Aerospike (bitcoin-sv/teranode#4838).
//
// It bootstraps STRICTLY, unlike the shared smoke suite: runAerospikeTestContainer
// plus test.SkipIfContainerUnavailable skips only when the container RUNTIME is
// unreachable and fails on anything else — an image that will not pull, a container
// that never becomes ready. The shared suite's initAerospike skips on every startup
// error, which would let this parity case silently stop running on a CI box where
// Docker is present but the image is not.
func TestQuickPathParity(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	container, err := runAerospikeTestContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	// Registered AFTER New, which is itself after the container's terminate cleanup, so
	// LIFO ordering closes the store's client and background workers before the
	// container they are talking to is torn down. Only the container was being cleaned
	// up before this, leaving the store's own resources to the end of the process.
	t.Cleanup(func() {
		require.NoError(t, store.Close(ctx))
	})

	require.NoError(t, store.SetBlockHeight(100))

	t.Run("quick_path_create_spend_mined_semantics", func(t *testing.T) {
		tests.QuickPathCreateSpendMinedSemantics(t, store)
	})

	t.Run("absent_record_observables", func(t *testing.T) {
		tests.AbsentRecordObservables(t, store)
	})
}
