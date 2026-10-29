package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// On every network with checkpoints, a catch-up window that starts at a checkpoint on a netting
// store ends exactly at the next one, whatever blockvalidation_max_accumulated_headers says. On
// testnet the largest gap (700,000 to 800,010) is above the 100,000 default, which used to stop
// the window short of the checkpoint.
func TestNettedRoute_WindowEndsAtTheNextCheckpointOnEveryNetwork(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_networks", 65536)
	defer cleanup()

	networks := []*chaincfg.Params{&chaincfg.MainNetParams, &chaincfg.TestNetParams, &chaincfg.TeraTestNetParams, &chaincfg.TeraScalingTestNetParams}

	for _, params := range networks {
		if len(params.Checkpoints) == 0 {
			continue
		}

		p := *params
		bv.settings.ChainCfgParams = &p

		var largest int

		for _, setting := range []int{1, 100_000, 10_000_000} {
			bv.settings.BlockValidation.CatchupMaxAccumulatedHeaders = setting

			prev := uint32(0)

			for {
				next, ok := model.NextCheckpointAbove(p.Checkpoints, prev)
				if !ok {
					break
				}

				window := model.NettedRouteCatchupWindow(bv.settings, bv.utxoStore, prev, prev)
				require.Equal(t, int(next-prev)+1, window, "%s: tip at %d, setting %d: every header from the tip to checkpoint %d", p.Name, prev, setting, next)

				largest = max(largest, window)
				prev = next
			}

			require.Equal(t, setting, model.NettedRouteCatchupWindow(bv.settings, bv.utxoStore, prev, prev), "%s: past the highest checkpoint the setting applies", p.Name)
		}

		t.Logf("%s: largest catch-up window %d headers", p.Name, largest)
	}
}

// The window counts from the height the locator starts at, so a UTXO store that trails the tip
// makes the window longer, never shorter, and it still ends at the next checkpoint above the tip.
func TestNettedRoute_WindowIsTheDistanceToTheNextCheckpoint(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_distance", 65536)
	defer cleanup()

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}

	for _, tc := range []struct {
		tip, start uint32
		want       int
		why        string
	}{
		{0, 0, 401, "from genesis to checkpoint 400"},
		{400, 400, 601, "from checkpoint 400 to checkpoint 1,000"},
		{700, 700, 301, "from the tip between checkpoints"},
		{700, 695, 306, "a locator 5 blocks behind the tip adds those 5 headers"},
		{999, 10, 991, "a long lag still reaches the checkpoint"},
	} {
		for _, setting := range []int{1, 5000} {
			s.BlockValidation.CatchupMaxAccumulatedHeaders = setting
			require.Equal(t, tc.want, model.NettedRouteCatchupWindow(s, bv.utxoStore, tc.tip, tc.start), "%s, setting %d", tc.why, setting)
		}
	}

	// Above the hardcoded checkpoint, and on a store that does not net, the setting applies.
	s.BlockValidation.CatchupMaxAccumulatedHeaders = 5000
	require.Equal(t, 5000, model.NettedRouteCatchupWindow(s, bv.utxoStore, 1000, 1000))
	require.Equal(t, 5000, model.NettedRouteCatchupWindow(s, bv.utxoStore.(nettingRecorder).applyRecorder, 400, 400))
}

// A block below the checkpoint on a netting store, in a window with no verified checkpoint above
// it, is a local fault, not a fault of the peer: an honest peer that is still syncing serves such
// a window.
func TestNettedRoute_WindowWithoutACheckpointIsNotChargedToThePeer(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_no_checkpoint", 65536)
	defer cleanup()

	bv.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}

	u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv}
	block := &model.Block{
		Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		Height: 500,
	}

	tryNormal, err := u.tryQuickValidation(t.Context(), block, &CatchupContext{useQuickValidation: false}, "peer", "http://peer", nil, nil)
	require.False(t, tryNormal)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
	require.True(t, isLocalCatchupFault(err), "the terminal handler must not charge the peer")
	require.False(t, shouldReportConsensusMalicious(err))
}

func TestNettedRoute_NextCheckpointAbove(t *testing.T) {
	checkpoints := []chaincfg.Checkpoint{{Height: 1000}, {Height: 400}, {Height: 1500}}

	for _, tc := range []struct {
		height, want uint32
		ok           bool
	}{{0, 400, true}, {399, 400, true}, {400, 1000, true}, {999, 1000, true}, {1000, 1500, true}, {1500, 0, false}} {
		next, ok := model.NextCheckpointAbove(checkpoints, tc.height)
		require.Equal(t, tc.ok, ok, "above %d", tc.height)
		require.Equal(t, tc.want, next, "above %d, in any list order", tc.height)
	}

	_, ok := model.NextCheckpointAbove(nil, 0)
	require.False(t, ok)
}

// With the store netting below the checkpoint, block validation refuses at startup the catch-up
// checkpoint override, which the route cannot honour. The route settings themselves are not
// refused, because a netting store ignores them.
func TestNettedRoute_CatchupSettingsRefusedAtStartup(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_catchup", 65536)
	defer cleanup()

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}
	s.BlockValidation.CatchupAllowQuickValidation = true
	s.BlockValidation.CatchupMaxAccumulatedHeaders = 1

	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore), "a small window is raised, not refused")

	s.BlockValidation.CatchupCheckpointHash = "000000000000000000000000000000000000000000000000000000000000000a"
	s.BlockValidation.CatchupCheckpointHeight = 700
	err := model.CheckNettedRouteCatchupSettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "a catch-up checkpoint override, got %v", err)

	// The override is refused with the outpoint-only setting off too: the store nets anyway.
	s.BlockValidation.OutpointOnlyBelowCheckpoint = false
	err = model.CheckNettedRouteCatchupSettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "override with outpoint-only off, got %v", err)

	s.BlockValidation.CatchupCheckpointHash = ""
	s.BlockValidation.CatchupCheckpointHeight = 0
	s.BlockValidation.CatchupAllowQuickValidation = false
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore), "quick validation off is ignored, not refused")

	// A store that does not net needs no check.
	s.BlockValidation.CatchupCheckpointHash = "000000000000000000000000000000000000000000000000000000000000000a"
	s.BlockValidation.CatchupCheckpointHeight = 700
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore.(nettingRecorder).applyRecorder))
}

// On a netting store every route setting is ignored, so a service whose copies are all off
// still puts a block below the checkpoint on the netted route, and catch-up still quick
// validates it. This is the split icellan found: legacy's copy of the outpoint-only setting off
// and block validation's on, which used to leave the two services disagreeing on the route.
func TestNettedRoute_RouteSettingsAreIgnoredOnANettingStore(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_ignores_settings", 65536)
	defer cleanup()

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}
	s.BlockValidation.OutpointOnlyBelowCheckpoint = false
	s.BlockValidation.LegacyUnifiedBelowCheckpoint = false

	require.True(t, model.NettedRouteAt(s, bv.utxoStore, 500), "a netting store nets below the checkpoint with the setting off")
	require.True(t, bv.mustStayOnQuickValidationAt(500))
	require.True(t, bv.quickValidateOutpointOnly(&model.Block{Height: 500}))
	require.False(t, model.NettedRouteAt(s, bv.utxoStore, 1001), "above the checkpoint nothing changes")

	// A store that does not net still follows the setting.
	require.False(t, model.NettedRouteAt(s, bv.utxoStore.(nettingRecorder).applyRecorder, 500))
	require.False(t, model.OutpointOnlyEligible(s, bv.utxoStore.(nettingRecorder).applyRecorder, s.ChainCfgParams, 500))
}

// Catch-up quick validates a window on a netting store with
// blockvalidation_catchup_allow_quick_validation off, because the store takes the blocks below
// the checkpoint through no other route.
func TestNettedRoute_CatchupQuickValidatesWithTheSettingOff(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_allow_quick_off", 65536)
	defer cleanup()

	headers := make([]*model.BlockHeader, 400)
	for i := range headers {
		headers[i] = &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Nonce: uint32(i)} //nolint:gosec // test index
	}

	checkpoints := []chaincfg.Checkpoint{{Height: 400, Hash: headers[399].Hash()}}
	bv.settings.ChainCfgParams.Checkpoints = checkpoints
	bv.settings.BlockValidation.CatchupAllowQuickValidation = false

	newCtx := func() *CatchupContext {
		return &CatchupContext{
			blockUpTo:          &model.Block{Header: headers[len(headers)-1], Height: 400},
			commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
			blockHeaders:       headers,
			checkpoints:        checkpoints,
		}
	}

	u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv, utxoStore: bv.utxoStore}
	c := newCtx()
	require.NoError(t, u.verifyCheckpointsInHeaderChain(c))
	require.True(t, c.useQuickValidation, "a netting store quick validates with the setting off")

	u.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	c = newCtx()
	require.NoError(t, u.verifyCheckpointsInHeaderChain(c))
	require.False(t, c.useQuickValidation, "a store that does not net follows the setting")
}
