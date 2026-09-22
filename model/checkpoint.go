package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// HighestCheckpointHeight returns the largest non-negative Height in the supplied
// checkpoint list, or 0 if the list is empty. This is the single source of truth
// for the below-checkpoint boundary: the block-validation write paths, the legacy
// netsync write path, the validator height guard, and checkBlockRewardAndFees' read
// path all derive the boundary from this one function, so the fee=0 write side and
// the fee-check skip read side can never disagree (invariant I3).
//
// services/blockchain.HighestCheckpointHeight delegates here; model sits below
// services/blockchain in the import graph, so the shared definition lives here to
// avoid the import cycle that previously forced a hand-copied loop in Block.go.
func HighestCheckpointHeight(checkpoints []chaincfg.Checkpoint) uint32 {
	var highest uint32
	for _, cp := range checkpoints {
		if cp.Height < 0 {
			continue
		}
		if h := uint32(cp.Height); h > highest {
			highest = h
		}
	}

	return highest
}

// BelowCheckpoint reports whether height is inside the hardcoded-checkpoint-certified
// prefix: a highest checkpoint exists (highest > 0), height is not genesis
// (height > 0 — genesis carries only a coinbase, so fast-path skips are pointless
// and every gate treats it as not-fast-pathable), and height <= highest. This is
// the single boundary definition for every below-checkpoint fast-path gate; do not
// re-derive the comparison elsewhere.
func BelowCheckpoint(checkpoints []chaincfg.Checkpoint, height uint32) bool {
	highest := HighestCheckpointHeight(checkpoints)

	return highest > 0 && height > 0 && height <= highest
}

// OutpointOnlyEligible is the single eligibility gate for the below-checkpoint
// outpoint-only fast path (PR 1178): the operator opted in, the UTXO store supports
// outpoint-only spends (Stage A is SQL-only; Aerospike deferred to Stage B), chain
// params exist, and the height is below the hardcoded checkpoint per
// BelowCheckpoint. Every conjunct must hold — any one missing keeps the gate
// closed (fail-safe, invariant I2). Chain params are passed explicitly because
// callers legitimately source them differently (netsync holds *chaincfg.Params
// directly; blockvalidation and model read settings.ChainCfgParams).
func OutpointOnlyEligible(tSettings *settings.Settings, store utxo.Store, params *chaincfg.Params, height uint32) bool {
	if tSettings == nil || !tSettings.BlockValidation.OutpointOnlyBelowCheckpoint {
		return false
	}

	if store == nil || !store.SupportsOutpointOnlySpend() {
		return false
	}

	if params == nil {
		return false
	}

	return BelowCheckpoint(params.Checkpoints, height)
}

// HighestCheckpointHash returns the Hash of the checkpoint at the highest non-negative
// Height in the supplied list, or nil if the list is empty or that entry carries no hash.
// It pairs with HighestCheckpointHeight: to prove a below-checkpoint block is on the
// checkpoint-committed main chain, a caller compares the main-chain block at
// HighestCheckpointHeight against this pinned hash (see BlockValidation.checkpoint-
// ConfirmedAncestor). Kept here alongside HighestCheckpointHeight so both derive from
// the same checkpoint list and cannot drift.
func HighestCheckpointHash(checkpoints []chaincfg.Checkpoint) *chainhash.Hash {
	var (
		highest uint32
		hash    *chainhash.Hash
	)

	for _, cp := range checkpoints {
		if cp.Height < 0 {
			continue
		}

		if h := uint32(cp.Height); h >= highest {
			highest = h
			hash = cp.Hash
		}
	}

	return hash
}
