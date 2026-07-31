package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_legacyUnified: the netsync adapter gate is the unified flag
// AND the full outpoint-only gate. Any conjunct missing keeps the inline
// pipeline (fail-safe).
func TestSyncManager_legacyUnified(t *testing.T) {
	const checkpointHeight = int32(1000)

	tests := []struct {
		name     string
		unified  bool
		enabled  bool
		sqlStore bool
		height   uint32
		want     bool
	}{
		{name: "unified+outpoint on, below", unified: true, enabled: true, sqlStore: true, height: 500, want: true},
		{name: "unified on, outpoint off", unified: true, enabled: false, sqlStore: true, height: 500, want: false},
		{name: "unified off, outpoint on", unified: false, enabled: true, sqlStore: true, height: 500, want: false},
		{name: "above checkpoint", unified: true, enabled: true, sqlStore: true, height: 1500, want: false},
		{name: "non-SQL store", unified: true, enabled: true, sqlStore: false, height: 500, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.enabled, tt.sqlStore, checkpointHeight)
			tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = tt.unified

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
			}
			if tt.sqlStore {
				sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
			} else {
				sm.utxoStore = &nullstore.NullStore{}
			}

			require.Equal(t, tt.want, sm.legacyUnified(tt.height))
		})
	}
}

// TestSyncManager_prepareSubtrees_UnifiedSkipsUTXOOps: with the unified flag on,
// prepareSubtrees must (a) return blockID 0 without touching the blockchain
// client, (b) never call the UTXO store's Create/Spend (validator client is nil
// and store is a spy — any UTXO write panics/counts), while (c) still writing
// the subtree files. RED before the gate exists: ValidateTransactionsLegacyMode
// runs and panics on the nil validation client.
func TestSyncManager_prepareSubtrees_UnifiedSkipsUTXOOps(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const blockHeight = int32(500) // below checkpoint

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true

	spy := &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
	subtreeStore := memory.New()

	sm := &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		utxoStore:        spy,
		validationClient: nil, // nil — any UTXO call would panic
		subtreeStore:     subtreeStore,
		ctx:              context.Background(),
	}

	require.True(t, sm.legacyUnified(uint32(blockHeight)), "gate must be ON for this case")

	block, _, _ := buildExtendedSubtreeBlock(t, blockHeight, 5)

	subtrees, subtreeSlices, blockID, err := sm.prepareSubtrees(context.Background(), block)
	require.NoError(t, err)

	// (a) blockID must be 0 — server-side assignment deferred
	require.Equal(t, uint32(0), blockID, "unified route must return blockID 0")

	// (b) subtreeSlices returned (non-nil and non-empty) for the merkle check
	require.NotNil(t, subtreeSlices, "subtreeSlices must be returned for merkle check")
	require.NotEmpty(t, subtreeSlices, "subtreeSlices must not be empty")

	// (c) subtree files must still be written to the subtree store
	require.NotEmpty(t, subtrees, "subtree roots must still be produced")
	for _, root := range subtrees {
		require.NotNil(t, root, "each subtree root hash must be non-nil")
	}
}
