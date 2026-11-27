// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

// Options represents a function type for configuring the SubtreeProcessor.
// This type implements the functional options pattern, allowing for flexible and
// extensible configuration of the SubtreeProcessor with optional parameters.
// Multiple options can be composed together to customize processor behavior.
type Options func(*SubtreeProcessor)

// WithMmapDir enables mmap-backed subtree Nodes stored in the given directory.
// When set, subtree Node arrays are allocated as file-backed mmap regions instead
// of on the Go heap, eliminating GC pressure and enabling OS-managed paging.
// An empty string disables mmap (uses heap allocation).
func WithMmapDir(dir string) Options {
	return func(stp *SubtreeProcessor) {
		stp.mmapDir = dir
	}
}

// WithTxMapDirs enables disk-backed transaction map using the given directories.
// Each directory should ideally be on a separate physical disk for I/O parallelism.
// When set, the currentTxMap is replaced with a DiskTxMap that uses sharded
// cuckoo filters for fast existence checks and BadgerDB for TxInpoints storage.
// Empty or nil keeps the in-memory SplitTxInpointsMap.
func WithTxMapDirs(dirs []string) Options {
	return func(stp *SubtreeProcessor) {
		stp.txMapDirs = dirs
	}
}

// WithTxMetaEvictor makes the processor publish txmeta DELETEs for every tx its
// conflict processing marks conflicting, so subtree validation evicts the stale
// non-conflicting cache entries. A nil evictor disables publishing.
func WithTxMetaEvictor(e *TxMetaEvictor) Options {
	return func(stp *SubtreeProcessor) {
		stp.txMetaEvictor = e
	}
}

// WithStoredSet makes the processor cut every mining template at the first chained subtree that is
// not in set, which the storage side fills as it stores (blockassembly_storedPrefixTemplate). Nil,
// the default, publishes the whole chain as before.
func WithStoredSet(set *StoredSet) Options {
	return func(stp *SubtreeProcessor) {
		stp.storedSet = set
		stp.prefixWatch = newStoredPrefixWatch()
	}
}

// WithLaneBitmapsKeeper makes the processor build the lane bitmaps (util/lane) of each subtree it
// chains, at utxostore_laneMigration shadow, and hand them to k before a mining candidate can list
// the subtree. At off it is ignored.
func WithLaneBitmapsKeeper(k LaneBitmapsKeeper) Options {
	return func(stp *SubtreeProcessor) {
		stp.laneKeeper = k
	}
}
