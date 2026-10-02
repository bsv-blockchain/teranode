package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// TestMinedThenSpendAllPrunes_Postgres verifies that after a tx is mined on the
// longest chain and then has every one of its outputs spent via the normal
// Spend path, the pruner is able to reclaim it. Covers both the bulk spend
// path (bulkSpendSQL) and the interaction with SetMinedMulti's DAH branch.
func TestMinedThenSpendAllPrunes_Postgres(t *testing.T) {
	// The pruner service is scoped to the Store instance, so each test's store
	// gets its own — no global reset needed.
	store, _ := setupTestStore(t)

	provider := any(store).(pruner.PrunerServiceProvider)
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc)

	// Deliberately do NOT Start() the background Worker 2 cursor here: this test
	// exercises Prune's own stamp+delete reclaim path synchronously and
	// deterministically (on-demand mode, where Prune runs the inline catch-up sweep
	// itself). When the cursor IS running it becomes the authoritative stamper and
	// Prune skips the inline sweep — that path is covered by the throughput and
	// integration suites, and is async (would race a synchronous assertion here).
	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}
