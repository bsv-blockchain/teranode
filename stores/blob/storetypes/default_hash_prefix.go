package storetypes

// DefaultHashPrefix returns the hash-prefix directory layout a blob store of
// the given type is written with when its URL carries no hashPrefix query
// parameter. The prefix is baked into every stored file's path, so a writer
// and a deleter that disagree about this table cannot see each other's files
// - the historical failure was the pruner keeping a private copy of this
// table behind a "must match" comment while the real values lived as inline
// literals scattered through the store construction code.
//
// This is now the single source: daemon store construction and the pruner's
// blob-deletion store both resolve through here, and
// TestDefaultHashPrefixPinnedValues pins the numbers so a change is a
// deliberate, test-failing event rather than a silent drift.
func DefaultHashPrefix(t BlobStoreType) int {
	switch t {
	case TXSTORE:
		return 2
	case SUBTREESTORE:
		return 2
	case BLOCKSTORE:
		return -2
	case TEMPSTORE:
		return 0
	case BLOCKPERSISTERSTORE:
		return 2
	case PEERREGISTRYSTORE:
		return 0
	default:
		return 2
	}
}
