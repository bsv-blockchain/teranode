package subtreeprocessor

import "sync/atomic"

// Superseded reports whether reset() retired the state this request was built from after the
// request was made. Its ParentTxMap was then cleared under it, so a meta build that cannot find a
// parent has not failed on its own: the build is cancelled, not failed and not retried. The zero
// value, a request made without a processor, is never superseded.
func (r NewSubtreeRequest) Superseded() bool {
	return r.superseded != nil && r.superseded.Load()
}

// supersedeToken is the flag shared by the store requests of the current state. Made on first use,
// so a processor built without a constructor needs no setup.
func (stp *SubtreeProcessor) supersedeToken() *atomic.Bool {
	if token := stp.supersedeFlag.Load(); token != nil {
		return token
	}

	stp.supersedeFlag.CompareAndSwap(nil, new(atomic.Bool))

	return stp.supersedeFlag.Load()
}

// supersedeStoreRequests marks every store request made so far as superseded and starts a new
// state for the requests made from now on. It sets a flag and waits for nothing, so reset() never
// waits for a store. It runs before the tx map is cleared: a build that misses a parent because of
// the clear then finds the flag set.
func (stp *SubtreeProcessor) supersedeStoreRequests() {
	if old := stp.supersedeFlag.Swap(new(atomic.Bool)); old != nil {
		old.Store(true)
	}
}
