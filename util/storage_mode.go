package util

import "github.com/bsv-blockchain/teranode/settings"

// DetermineStorageMode determines whether this node is a full node or pruned node.
// A full node has the block persister running and within the retention window (default: 288 blocks).
// Since data isn't purged until older than the retention period, a node can serve as "full"
// as long as the persister lag is within this window.
// A pruned node either doesn't have block persister running or it's lagging beyond the retention window.
//
// Parameters:
//   - blockPersisterHeight: The height up to which blocks have been persisted (0 if block persister not running)
//   - bestHeight: The current best block height
//   - retentionWindow: The block height retention window (use 0 for default of 288 blocks)
//   - prunerBlockTrigger: The pruner block trigger mode ("OnBlockPersisted" or "OnBlockMined")
//
// Returns "full" or "pruned" - never returns empty string.
func DetermineStorageMode(blockPersisterHeight uint32, bestHeight uint32, retentionWindow uint32, prunerBlockTrigger string) string {
	// If pruner is configured to ignore block persister (OnBlockMined mode),
	// the node cannot be considered "full" because it's pruning before persistence
	if prunerBlockTrigger == settings.PrunerBlockTriggerOnBlockMined {
		return "pruned"
	}

	// If block persister height is 0, it means persister is not running
	if blockPersisterHeight == 0 {
		return "pruned"
	}

	// Calculate lag
	var lag uint32
	if bestHeight > blockPersisterHeight {
		lag = bestHeight - blockPersisterHeight
	} else {
		lag = 0
	}

	// Get lag threshold
	lagThreshold := retentionWindow
	if lagThreshold == 0 {
		lagThreshold = 288 // Default 2 days of blocks (144 blocks/day * 2)
	}

	// Determine mode based on retention window
	// If BlockPersister is within the retention window, node is "full"
	// If BlockPersister lags beyond the retention window, node is "pruned"
	if lag <= lagThreshold {
		return "full"
	}

	return "pruned"
}
