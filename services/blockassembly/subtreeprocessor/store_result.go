package subtreeprocessor

// StoreResult is the final outcome of storing one subtree and its meta, delivered once on
// NewSubtreeRequest.Result. ErrChan says only that the first attempt at both writes ended; a
// failed write is retried afterwards and ErrChan is answered with nil either way. The result is
// posted when the store is settled: stored on the first try or on a retry, or failed because
// the retries ran out.
//
// Nothing is posted when block assembly shuts down before the store settles: a worker that sees
// its context end before it starts the store, and requests left in the worker or retry queues. A
// waiter must also watch its context.
// A subtree that existed with no meta file is reported with MetaStored false and Err nil.
type StoreResult struct {
	// SubtreeStored is true when the subtree file is in the store, written by this store or
	// found there (ExistedAlready).
	SubtreeStored bool

	// MetaStored is true when the subtree meta file is in the store. It is false, with
	// MetaNotApplicable true, when no meta is written because blockassembly_storeTxInpointsForSubtreeMeta
	// is off or the request has no ParentTxMap.
	MetaStored        bool
	MetaNotApplicable bool

	// ExistedAlready is true when the subtree file was already in the store.
	ExistedAlready bool

	// Cancelled is true when the store ended because block assembly was shutting down, not
	// because a write failed.
	Cancelled bool

	// Err is why the store failed, when it did.
	Err error
}

// OK reports whether everything the store has to write is in the store.
func (r StoreResult) OK() bool {
	return r.Err == nil && !r.Cancelled && r.SubtreeStored && (r.MetaStored || r.MetaNotApplicable)
}
