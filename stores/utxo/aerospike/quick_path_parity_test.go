package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestQuickPathParity runs the two store-contract cases the quick-validation
// no-mutation regression depends on against Aerospike (bitcoin-sv/teranode#4838).
//
// It bootstraps STRICTLY: the shared server skips the test only when the container
// RUNTIME is unreachable and fails it on anything else — an image that will not pull,
// a container that never becomes ready — so this parity case cannot silently stop
// running on a CI box where Docker is present but the image is not.
func TestQuickPathParity(t *testing.T) {
	t.Parallel()

	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	store, _ := newSharedTestStore(t, logger, tSettings, "&externalStore=file://./data/external")

	// Closes the store's batchers and background workers; the shared client stays open.
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
