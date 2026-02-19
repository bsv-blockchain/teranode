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
