package aerospike

import "sync"

// errChPool reuses chan error (capacity 1) across spend, locked, and create paths.
// Each hot-path operation allocates one of these per item; pooling eliminates
// makechan + GC overhead at high throughput.
var errChPool = sync.Pool{
	New: func() any { return make(chan error, 1) },
}

// acquireErrCh gets a chan error from the pool.
func acquireErrCh() chan error {
	return errChPool.Get().(chan error)
}

// releaseErrCh drains any residual value and returns the channel to the pool.
func releaseErrCh(ch chan error) {
	// Drain to prevent stale errors leaking to the next user.
	select {
	case <-ch:
	default:
	}
	errChPool.Put(ch)
}

// batchGetItemDataChPool reuses chan batchGetItemData (capacity 1) for the get path.
var batchGetItemDataChPool = sync.Pool{
	New: func() any { return make(chan batchGetItemData, 1) },
}

func acquireBatchGetCh() chan batchGetItemData {
	return batchGetItemDataChPool.Get().(chan batchGetItemData)
}

func releaseBatchGetCh(ch chan batchGetItemData) {
	select {
	case <-ch:
	default:
	}
	batchGetItemDataChPool.Put(ch)
}
