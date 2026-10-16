package netsync

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// outpointOnlySpyStore wraps NullStore to count BatchPreviousOutputsDecorate calls
// and capture the CreateOptions passed to Create, so tests can assert which legacy
// path engaged without standing up a real SQL store.
type outpointOnlySpyStore struct {
	*nullstore.NullStore
	decorateCalls atomic.Int32
}

func (s *outpointOnlySpyStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	s.decorateCalls.Add(1)
	return s.NullStore.BatchPreviousOutputsDecorate(ctx, txs)
}

// SupportsOutpointOnlySpend overrides the embedded NullStore to model a store that
// honours the fast path, so legacyOutpointOnly can engage in these tests.
func (s *outpointOnlySpyStore) SupportsOutpointOnlySpend() bool { return true }

// newOutpointOnlySettings returns settings configured so legacyOutpointOnly can
// return true: the feature flag on, a SQL-backed (sqlitememory) UTXO store URL,
// and a single hard-coded checkpoint at checkpointHeight on the chain params.
func newOutpointOnlySettings(t *testing.T, enabled bool, sqlStore bool, checkpointHeight int32) (*settings.Settings, *chaincfg.Params) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = enabled

	if sqlStore {
		u, err := url.Parse("sqlitememory://test")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	} else {
		// aerospike scheme = non-SQL
		u, err := url.Parse("aerospike://host:3000/ns/set")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	}

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings, &params
}

// TestSyncManager_legacyOutpointOnly is the full truth table for the gate helper.
// Every conjunct must hold (flag on AND SQL store AND at/below the highest hard-coded
// checkpoint) for the fast path to engage; any one missing keeps it OFF (fail-safe).
func TestSyncManager_legacyOutpointOnly(t *testing.T) {
	const checkpointHeight = int32(1000)
	const below = uint32(500)
	const atCheckpoint = uint32(1000)
	const above = uint32(1500)

	tests := []struct {
		name       string
		enabled    bool
		sqlStore   bool
		nilChain   bool
		noCheckpts bool
		height     uint32
		want       bool
	}{
		{name: "flag off, SQL, below", enabled: false, sqlStore: true, height: below, want: false},
		{name: "flag on, non-SQL (aerospike), below", enabled: true, sqlStore: false, height: below, want: false},
		{name: "flag on, SQL, below checkpoint", enabled: true, sqlStore: true, height: below, want: true},
		{name: "flag on, SQL, at checkpoint", enabled: true, sqlStore: true, height: atCheckpoint, want: true},
		{name: "flag on, SQL, above checkpoint", enabled: true, sqlStore: true, height: above, want: false},
		// Height 0 is fail-closed since the gates collapsed onto
		// model.BelowCheckpoint: genesis carries only a coinbase and never flows
		// through the legacy fast path, so excluding it costs nothing and keeps
		// one boundary definition everywhere.
		{name: "flag on, SQL, height 0 fail-closed", enabled: true, sqlStore: true, height: 0, want: false},
		{name: "flag on, SQL, nil chain params", enabled: true, sqlStore: true, nilChain: true, height: below, want: false},
		{name: "flag on, SQL, no checkpoints", enabled: true, sqlStore: true, noCheckpts: true, height: below, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.enabled, tt.sqlStore, checkpointHeight)

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
			}

			// The gate now asks the store, not the settings URL: a supporting store
			// (spy over NullStore) for the SQL case, a plain NullStore (reports false)
			// otherwise.
			if tt.sqlStore {
				sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
			} else {
				sm.utxoStore = &nullstore.NullStore{}
			}

			if tt.nilChain {
				sm.chainParams = nil
				sm.settings.ChainCfgParams = nil
			}

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				sm.chainParams = &noCp
				sm.settings.ChainCfgParams = &noCp
			}

			require.Equal(t, tt.want, sm.legacyOutpointOnly(headerProven, tt.height),
				"legacyOutpointOnly(%d) enabled=%v sql=%v", tt.height, tt.enabled, tt.sqlStore)
		})
	}
}

// TestSyncManager_needsParentMinedWait verifies the parent-mined wait is skipped
// only on the below-checkpoint outpoint-only fast path. It reuses the same
// store-capability harness as TestSyncManager_legacyOutpointOnly: a supporting
// store (spy over NullStore) for the "SQL" cases, a plain NullStore otherwise.
func TestSyncManager_needsParentMinedWait(t *testing.T) {
	const checkpointHeight = int32(1000)

	tests := []struct {
		name     string
		enabled  bool
		sqlStore bool
		height   uint32
		want     bool
	}{
		{name: "height 0 never waits", enabled: false, sqlStore: true, height: 0, want: false},
		{name: "height 1 never waits", enabled: false, sqlStore: true, height: 1, want: false},
		{name: "flag off, below checkpoint: waits", enabled: false, sqlStore: true, height: 500, want: true},
		{name: "flag on, non-supporting store, below checkpoint: waits", enabled: true, sqlStore: false, height: 500, want: true},
		{name: "flag on, supporting store, below checkpoint: skips", enabled: true, sqlStore: true, height: 500, want: false},
		{name: "flag on, supporting store, at checkpoint: skips", enabled: true, sqlStore: true, height: 1000, want: false},
		{name: "flag on, supporting store, above checkpoint: waits", enabled: true, sqlStore: true, height: 1500, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.enabled, tt.sqlStore, checkpointHeight)

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

			require.Equal(t, tt.want, sm.needsParentMinedWait(headerProven, tt.height),
				"needsParentMinedWait(%d) enabled=%v store=%v", tt.height, tt.enabled, tt.sqlStore)
		})
	}
}
