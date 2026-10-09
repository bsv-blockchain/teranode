package blockvalidation

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// unifiedRouteStore stubs only SupportsOutpointOnlySpend; nil-embedded interface
// panics on any other use, proving the routing decision touches nothing else.
type unifiedRouteStore struct {
	utxo.Store
	supports bool
}

func (s *unifiedRouteStore) SupportsOutpointOnlySpend() bool { return s.supports }

func newUnifiedRouteSettings(t *testing.T, unified, outpointOnly bool, checkpointHeight int32) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = unified
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = outpointOnly

	u, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings
}

// TestServer_legacyUnifiedRoute: full truth table. The route opens ONLY when the
// unified flag is on AND the source is legacy AND the shared outpoint-only gate
// (flag + store support + hardcoded checkpoint boundary) holds AND the caller has
// proved the block's ancestry. Any conjunct missing keeps every block on the
// normal validation path (fail-safe). wantEligible is legacyUnifiedEligible's
// answer for the same row: the settings-and-height half, which must not read
// the proof, because the parent-missing guard relies on it for an unproven
// legacy orphan.
func TestServer_legacyUnifiedRoute(t *testing.T) {
	const cp = int32(2000)

	tests := []struct {
		name         string
		unified      bool
		outpointOnly bool
		supports     bool
		baseURL      string
		height       uint32
		headerProven bool
		want         bool
		wantEligible bool
	}{
		{name: "all on, legacy, below, proven", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, headerProven: true, want: true, wantEligible: true},
		{name: "all on, legacy, below, unproven", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, headerProven: false, want: false, wantEligible: true},
		{name: "at checkpoint, proven", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 2000, headerProven: true, want: true, wantEligible: true},
		{name: "at checkpoint, unproven", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 2000, headerProven: false, want: false, wantEligible: true},
		{name: "above checkpoint", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 2001, headerProven: true, want: false, wantEligible: false},
		{name: "unified flag off", unified: false, outpointOnly: true, supports: true, baseURL: "legacy", height: 1000, headerProven: true, want: false, wantEligible: false},
		{name: "outpoint-only flag off", unified: true, outpointOnly: false, supports: true, baseURL: "legacy", height: 1000, headerProven: true, want: false, wantEligible: false},
		{name: "store unsupported", unified: true, outpointOnly: true, supports: false, baseURL: "legacy", height: 1000, headerProven: true, want: false, wantEligible: false},
		{name: "non-legacy source", unified: true, outpointOnly: true, supports: true, baseURL: "http://peer:8090", height: 1000, headerProven: true, want: false, wantEligible: false},
		{name: "height 0", unified: true, outpointOnly: true, supports: true, baseURL: "legacy", height: 0, headerProven: true, want: false, wantEligible: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := newUnifiedRouteSettings(t, tt.unified, tt.outpointOnly, cp)

			u := &Server{
				settings:  tSettings,
				utxoStore: &unifiedRouteStore{supports: tt.supports},
			}

			block := &model.Block{Height: tt.height}
			require.Equal(t, tt.want, u.legacyUnifiedRoute(block, tt.baseURL, tt.headerProven))
			require.Equal(t, tt.wantEligible, u.legacyUnifiedEligible(block, tt.baseURL), "eligibility never reads the proof")
		})
	}
}
