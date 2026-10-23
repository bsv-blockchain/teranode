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

// On every network with checkpoints, the catch-up window of a netting store holds the largest
// checkpoint gap plus the common ancestor, starting from the shipped default. Mainnet's default
// already does; testnet's largest gap (700,000 to 800,010) is above the default, so its window
// is raised rather than refused.
func TestNettedRoute_DefaultWindowCoversTheLargestGapOnEveryNetwork(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_networks", 65536)
	defer cleanup()

	const defaultWindow = 100_000

	networks := []*chaincfg.Params{&chaincfg.MainNetParams, &chaincfg.TestNetParams, &chaincfg.TeraTestNetParams, &chaincfg.TeraScalingTestNetParams}

	for _, params := range networks {
		if len(params.Checkpoints) == 0 {
			continue
		}

		p := *params
		bv.settings.ChainCfgParams = &p
		bv.settings.BlockValidation.CatchupMaxAccumulatedHeaders = defaultWindow

		gap := int(model.LargestCheckpointGap(p.Checkpoints))
		window := model.NettedRouteCatchupWindow(bv.settings, bv.utxoStore)

		require.GreaterOrEqual(t, window, gap+1, "%s: a window of N headers holds N-1 new blocks", p.Name)
		require.Equal(t, max(defaultWindow, gap+1), window, "%s", p.Name)
		t.Logf("%s: largest checkpoint gap %d, window %d", p.Name, gap, window)
	}

	bv.settings.ChainCfgParams = &chaincfg.MainNetParams
	require.Equal(t, defaultWindow, model.NettedRouteCatchupWindow(bv.settings, bv.utxoStore), "mainnet keeps its default")

	testnet := chaincfg.TestNetParams
	bv.settings.ChainCfgParams = &testnet
	require.Equal(t, 100_011, model.NettedRouteCatchupWindow(bv.settings, bv.utxoStore), "testnet's gap of 100,010 plus the common ancestor")
}

// The window floor is the gap plus one: a window that starts at a checkpoint drops that
// checkpoint as the common ancestor, so it needs gap+1 headers to reach the next one.
func TestNettedRoute_WindowFloorIsTheGapPlusOne(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_floor", 65536)
	defer cleanup()

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}

	for _, tc := range []struct{ set, want int }{{1, 601}, {599, 601}, {600, 601}, {601, 601}, {5000, 5000}} {
		s.BlockValidation.CatchupMaxAccumulatedHeaders = tc.set
		require.Equal(t, tc.want, model.NettedRouteCatchupWindow(s, bv.utxoStore), "setting %d", tc.set)
	}

	// A store that does not net, or the setting off, keeps the setting.
	s.BlockValidation.CatchupMaxAccumulatedHeaders = 1
	require.Equal(t, 1, model.NettedRouteCatchupWindow(s, bv.utxoStore.(nettingRecorder).applyRecorder))

	s.BlockValidation.OutpointOnlyBelowCheckpoint = false
	require.Equal(t, 1, model.NettedRouteCatchupWindow(s, bv.utxoStore))
}

// On the netted route the locator is not capped at a trailing UTXO height, because each header
// of lag would come out of a window sized to hold exactly one checkpoint gap.
func TestNettedRoute_LocatorIsNotCappedAtATrailingUTXOHeight(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_locator", 65536)
	defer cleanup()

	bv.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}
	u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv, utxoStore: bv.utxoStore}

	require.False(t, u.locatorCapApplies(700, 695), "the next block is on the netted route")
	require.False(t, u.locatorCapApplies(999, 10), "the checkpoint block itself is on the netted route")
	require.True(t, u.locatorCapApplies(1000, 995), "above the hardcoded checkpoint the cap applies")
	require.False(t, u.locatorCapApplies(700, 700), "no lag, no cap")

	u.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	require.True(t, u.locatorCapApplies(700, 695), "a store that does not net keeps the cap")
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

func TestNettedRoute_LargestCheckpointGap(t *testing.T) {
	require.Zero(t, model.LargestCheckpointGap(nil))
	require.Equal(t, uint32(600), model.LargestCheckpointGap([]chaincfg.Checkpoint{{Height: 1000}, {Height: 400}, {Height: 1500}}),
		"genesis to 400 is 400, 400 to 1,000 is 600, 1,000 to 1,500 is 500, in any list order")
}

// With the store netting below the checkpoint, block validation refuses at startup the
// settings that keep catch-up from quick validating those blocks.
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

	s.BlockValidation.CatchupCheckpointHash = ""
	s.BlockValidation.CatchupCheckpointHeight = 0
	s.BlockValidation.CatchupAllowQuickValidation = false
	err = model.CheckNettedRouteCatchupSettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "quick validation off, got %v", err)

	// A store that does not net, or the setting off, needs neither.
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore.(nettingRecorder).applyRecorder))

	s.BlockValidation.OutpointOnlyBelowCheckpoint = false
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore))
}

// With the store netting below the checkpoint, the legacy service refuses at startup a legacy
// route that sends those blocks to full validation.
func TestNettedRoute_LegacySettingsRefusedAtStartup(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_legacy", 65536)
	defer cleanup()

	s := bv.settings

	s.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	require.NoError(t, model.CheckNettedRouteLegacySettings(s, bv.utxoStore))

	s.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	err := model.CheckNettedRouteLegacySettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "got %v", err)

	require.NoError(t, model.CheckNettedRouteLegacySettings(s, bv.utxoStore.(nettingRecorder).applyRecorder),
		"a store that does not net takes legacy blocks through full validation")
}

// runCheckpointStep runs catch-up's checkpoint step on a window from height 1 to height top.
// The checkpoints are at heights 400 and 1,000, and the headers in the window carry their hashes.
// It returns the headers the window keeps.
func runCheckpointStep(t *testing.T, bv *BlockValidation, top uint32) []*model.BlockHeader {
	t.Helper()

	headers := make([]*model.BlockHeader, top)
	for i := range headers {
		headers[i] = &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Nonce:          uint32(i), //nolint:gosec // test index
		}
	}

	checkpoints := []chaincfg.Checkpoint{{Height: 400, Hash: &chainhash.Hash{4}}, {Height: 1000, Hash: &chainhash.Hash{10}}}
	for i := range checkpoints {
		if h := uint32(checkpoints[i].Height); h <= top { //nolint:gosec // test heights
			checkpoints[i].Hash = headers[h-1].Hash()
		}
	}

	bv.settings.ChainCfgParams.Checkpoints = checkpoints
	bv.settings.BlockValidation.CatchupAllowQuickValidation = true

	u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv}
	c := &CatchupContext{
		blockUpTo:          &model.Block{Header: headers[len(headers)-1], Height: top},
		commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
		blockHeaders:       headers,
		checkpoints:        checkpoints,
	}

	require.NoError(t, u.verifyCheckpointsInHeaderChain(c))
	require.True(t, c.useQuickValidation)

	return c.blockHeaders
}

// The window ends at its highest verified checkpoint when the blocks after it are still at or
// below the hardcoded checkpoint on a netting store. Each of those blocks would be refused in
// this window, because no verified checkpoint is at or above it.
func TestNettedRoute_WindowEndsAtTheVerifiedCheckpoint(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_trim", 65536)
	defer cleanup()

	require.Len(t, runCheckpointStep(t, bv, 700), 400, "blocks 401 to 700 wait for the window that holds checkpoint 1,000")

	// Past the hardcoded checkpoint normal validation takes the blocks: no trim.
	require.Len(t, runCheckpointStep(t, bv, 1200), 1200)

	// A store that does not net validates the blocks normally: no trim.
	bv.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	require.Len(t, runCheckpointStep(t, bv, 700), 700)
}
