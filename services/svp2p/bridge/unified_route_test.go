package bridge

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_legacyUnified: svp2p sends no header proof with ProcessBlock,
// so blockvalidation never quick-validates its blocks. The unified gate must stay
// off for every flag combination, or svp2p would skip the UTXO work that full
// validation needs.
func TestSyncManager_legacyUnified(t *testing.T) {
	const checkpointHeight = int32(1000)

	tests := []struct {
		name     string
		unified  bool
		enabled  bool
		sqlStore bool
		height   uint32
	}{
		{name: "unified+outpoint on, below", unified: true, enabled: true, sqlStore: true, height: 500},
		{name: "unified on, outpoint off", unified: true, enabled: false, sqlStore: true, height: 500},
		{name: "unified off, outpoint on", unified: false, enabled: true, sqlStore: true, height: 500},
		{name: "above checkpoint", unified: true, enabled: true, sqlStore: true, height: 1500},
		{name: "non-SQL store", unified: true, enabled: true, sqlStore: false, height: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.enabled, tt.sqlStore, checkpointHeight)
			tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = tt.unified

			sm := &svp2pBridge{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
			}
			if tt.sqlStore {
				sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
			} else {
				sm.utxoStore = &nullstore.NullStore{}
			}

			require.False(t, sm.legacyUnified(tt.height))
		})
	}
}
