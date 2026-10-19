package model

import "github.com/bsv-blockchain/go-chaincfg"

// HighestCheckpointHeight returns the highest non-negative checkpoint height.
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

// BelowCheckpoint excludes genesis and networks without checkpoints.
func BelowCheckpoint(checkpoints []chaincfg.Checkpoint, height uint32) bool {
	highest := HighestCheckpointHeight(checkpoints)

	return highest > 0 && height > 0 && height <= highest
}

// SkipExpectedDifficulty permits historical DAA rules only while building the checkpoint prefix.
// Callers must independently prove checkpoint ancestry; height alone grants no trust.
// Once synced, a new low-height fork must satisfy the expected difficulty.
func SkipExpectedDifficulty(checkpoints []chaincfg.Checkpoint, blockHeight, bestHeight uint32) bool {
	if !BelowCheckpoint(checkpoints, blockHeight) {
		return false
	}

	return bestHeight < HighestCheckpointHeight(checkpoints)
}
