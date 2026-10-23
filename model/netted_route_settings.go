package model

import (
	"sort"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The netted route is the below-checkpoint write of a UTXO store that nets (utxoset), which
// engages when blockvalidation_outpoint_only_below_checkpoint is on. Such a store writes a block
// below the highest hardcoded checkpoint without spend-journal rows, so that block can then only
// be written through quick validation. A setting that keeps a block below the checkpoint away
// from quick validation therefore stops the sync at that block for good. The checks below refuse
// such settings at startup instead.

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

// LargestCheckpointGap is the most blocks between two neighbouring checkpoints of the list, from
// genesis to the highest checkpoint.
func LargestCheckpointGap(checkpoints []chaincfg.Checkpoint) uint32 {
	heights := make([]uint32, 0, len(checkpoints))

	for _, cp := range checkpoints {
		if cp.Height > 0 {
			heights = append(heights, uint32(cp.Height))
		}
	}

	sort.Slice(heights, func(a, b int) bool { return heights[a] < heights[b] })

	var gap, prev uint32

	for _, h := range heights {
		if h-prev > gap {
			gap = h - prev
		}

		prev = h
	}

	return gap
}

// NettedRouteCatchupWindow is the number of headers a catch-up window accumulates. It is
// blockvalidation_max_accumulated_headers, raised to the largest checkpoint gap plus one when the
// store nets the blocks below the checkpoint.
//
// The plus one is the common ancestor: the peer serves it as the first header and catch-up drops
// it, so a window of N headers holds at most N-1 new blocks. A window that starts at checkpoint c
// must reach c+gap to hold the next checkpoint, so it needs gap+1 headers. That count assumes the
// locator starts at the tip, so block validation does not cap it at a trailing UTXO store height
// on this route (blockvalidation's locatorCapApplies). Without a checkpoint in
// the window no block in it is quick validated, and on the netted route no block may take any
// other route.
//
// The floor is a value, not a refusal, because the default window is smaller than one testnet
// gap (100,010 blocks, from 700,000 to 800,010). 100,011 headers take about 8 MB.
func NettedRouteCatchupWindow(tSettings *settings.Settings, store utxo.Store) int {
	window := tSettings.BlockValidation.CatchupMaxAccumulatedHeaders

	if !nettedRouteOn(tSettings, store) {
		return window
	}

	if floor := int(LargestCheckpointGap(tSettings.ChainCfgParams.Checkpoints)) + 1; window < floor {
		return floor
	}

	return window
}

// CheckNettedRouteCatchupSettings refuses block validation settings under which native catch-up
// cannot quick validate every block below the checkpoint while the store nets there:
//
//   - blockvalidation_catchup_allow_quick_validation off sends every block to normal validation;
//   - blockvalidation_catchup_checkpoint_hash with blockvalidation_catchup_checkpoint_height
//     replaces the whole checkpoint list in catch-up with one checkpoint, so a window below that
//     one holds no checkpoint, and the window floor of NettedRouteCatchupWindow, which measures
//     the hardcoded list, no longer applies.
//
// The window size itself is not refused: NettedRouteCatchupWindow raises it.
func CheckNettedRouteCatchupSettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	bv := tSettings.BlockValidation

	if !bv.CatchupAllowQuickValidation {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so every such block must be quick validated, but blockvalidation_catchup_allow_quick_validation is off")
	}

	if bv.CatchupCheckpointHash != "" && bv.CatchupCheckpointHeight != 0 {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so each catch-up window must hold a hardcoded checkpoint, but blockvalidation_catchup_checkpoint_hash and blockvalidation_catchup_checkpoint_height replace the hardcoded checkpoints in catch-up")
	}

	return nil
}

// CheckNettedRouteLegacySettings refuses legacy settings under which netsync sends blocks below
// the checkpoint to full validation while the store nets there:
// blockvalidation_legacy_unified_below_checkpoint off. Block validation does not need its own
// copy of that setting on such a store (see blockvalidation's legacyUnifiedEligible), so this
// check on the legacy service's copy is the one that matters.
func CheckNettedRouteLegacySettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	if !tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so every such block must be quick validated, but blockvalidation_legacy_unified_below_checkpoint is off, which sends each legacy block to full validation")
	}

	return nil
}
