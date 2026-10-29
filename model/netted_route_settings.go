package model

import (
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The netted route is the below-checkpoint write of a UTXO store that nets (utxoset). Such a store
// writes a block below the highest hardcoded checkpoint without spend-journal rows, so that block
// can then only be written through quick validation. The route depends on the store, the
// checkpoint table and the height alone: blockvalidation_outpoint_only_below_checkpoint,
// blockvalidation_legacy_unified_below_checkpoint and blockvalidation_catchup_allow_quick_validation
// are treated as on for such a store (model.OutpointOnlyEligible, netsync's unifiedRoute and
// catch-up's verifyCheckpointsInHeaderChain). Each service then reaches the same answer, even when
// its copy of a setting differs from another service's. The one setting that can still keep a
// block away from quick validation is refused at startup below.

// NettedRouteAt reports whether a block at height may only be written through quick validation:
// the store nets the blocks below the checkpoint and the outpoint-only gate holds at height. It is
// the one definition of the netted route; block validation's runtime refusal and the startup
// checks below both read it.
func NettedRouteAt(tSettings *settings.Settings, store utxo.Store, height uint32) bool {
	if tSettings == nil || store == nil || !utxo.NetsBelowCheckpoint(store) {
		return false
	}

	return OutpointOnlyEligible(tSettings, store, tSettings.ChainCfgParams, height)
}

// nettedRouteOn reports whether the store nets any block below a checkpoint of these settings.
func nettedRouteOn(tSettings *settings.Settings, store utxo.Store) bool {
	if tSettings == nil || tSettings.ChainCfgParams == nil {
		return false
	}

	return NettedRouteAt(tSettings, store, HighestCheckpointHeight(tSettings.ChainCfgParams.Checkpoints))
}

// NextCheckpointAbove is the height of the lowest checkpoint of the list above height, and false
// when no checkpoint is above it.
func NextCheckpointAbove(checkpoints []chaincfg.Checkpoint, height uint32) (uint32, bool) {
	var next uint32

	found := false

	for _, cp := range checkpoints {
		if cp.Height <= 0 || uint32(cp.Height) <= height {
			continue
		}

		if h := uint32(cp.Height); !found || h < next {
			next, found = h, true
		}
	}

	return next, found
}

// NettedRouteCatchupWindow is the number of headers a catch-up window accumulates when the node's
// tip is at tipHeight and the peer serves its headers from startHeight, the height of the block the
// locator starts at.
//
// When the next block is on the netted route, the window ends at the next hardcoded checkpoint
// above the tip, whatever blockvalidation_max_accumulated_headers says. Quick validation needs a
// verified checkpoint at or above each block of the window, and on that route no block may take
// any other path, so a window must hold that checkpoint, and blocks past it would wait for the
// next window anyway. The peer serves every header from startHeight to the checkpoint, the block
// at startHeight included, which is checkpoint - startHeight + 1 headers. Counting from
// startHeight rather than from the tip makes the window hold the checkpoint however far the
// locator trails the tip. A window is one checkpoint gap plus any such lag: about 8 MB for
// testnet's largest gap of 100,010 blocks.
//
// Elsewhere it is the setting.
func NettedRouteCatchupWindow(tSettings *settings.Settings, store utxo.Store, tipHeight, startHeight uint32) int {
	window := tSettings.BlockValidation.CatchupMaxAccumulatedHeaders

	if tSettings.ChainCfgParams == nil || !NettedRouteAt(tSettings, store, tipHeight+1) {
		return window
	}

	next, ok := NextCheckpointAbove(tSettings.ChainCfgParams.Checkpoints, tipHeight)
	if !ok || next < startHeight {
		return window
	}

	return int(next-startHeight) + 1
}

// CheckNettedRouteCatchupSettings refuses the block validation setting under which native
// catch-up cannot quick validate every block below the checkpoint while the store nets there:
// blockvalidation_catchup_checkpoint_hash with blockvalidation_catchup_checkpoint_height replaces
// the whole checkpoint list in catch-up with one checkpoint, so a window below that one holds no
// checkpoint, and NettedRouteCatchupWindow, which ends each window at a hardcoded checkpoint, no
// longer matches the list catch-up verifies. It is an explicit operator choice the route cannot
// honour, so it is refused rather than ignored.
//
// The window size itself is not refused: NettedRouteCatchupWindow sets it on this route.
func CheckNettedRouteCatchupSettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	bv := tSettings.BlockValidation

	if bv.CatchupCheckpointHash != "" && bv.CatchupCheckpointHeight != 0 {
		return errors.NewConfigurationError("the UTXO store nets the blocks below the checkpoint, so each catch-up window must hold a hardcoded checkpoint, but blockvalidation_catchup_checkpoint_hash and blockvalidation_catchup_checkpoint_height replace the hardcoded checkpoints in catch-up")
	}

	return nil
}
