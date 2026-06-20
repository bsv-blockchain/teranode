package teraslab

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// batchStoreItem is a single item in the store batcher.
type batchStoreItem struct {
	ctx        context.Context
	createItem teraslab.CreateItem
	done       chan batchStoreResult
}

// batchStoreResult is the result of a store batch item.
type batchStoreResult struct {
	data *meta.Data
	err  error
}

// batchGetItem is a single item in the get batcher.
type batchGetItem struct {
	ctx       context.Context
	hash      chainhash.Hash
	fieldMask uint32
	done      chan batchGetResult
}

// batchGetResult is the result of a get batch item.
type batchGetResult struct {
	data *meta.Data
	err  error
}

// mergeBatchContexts returns a context that is canceled only when *every* input
// context has been canceled (i.e. when no caller is still waiting for a result).
// This lets the batch RPC proceed for as long as at least one caller cares about
// the answer, while still releasing the flusher goroutine if all callers give up.
//
// The returned cancel func must be called to release resources once the flush
// has completed (success or failure).
func mergeBatchContexts(ctxs []context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(context.Background())

	if len(ctxs) == 0 {
		// No item contexts: caller is the batcher itself; never cancel implicitly.
		return merged, cancel
	}

	var wg sync.WaitGroup
	wg.Add(len(ctxs))

	// done is closed to release watcher goroutines once the flush completes.
	done := make(chan struct{})

	for _, c := range ctxs {
		c := c
		go func() {
			defer wg.Done()
			if c == nil {
				// nil context counts as never canceled — wait until flush completes.
				<-done
				return
			}
			select {
			case <-c.Done():
			case <-done:
			}
		}()
	}

	go func() {
		wg.Wait()
		// Either every item context fired or the flush completed.
		// If everyone fired, cancel the merged context to abort in-flight RPC.
		// If the flush completed (done closed) we still cancel the merged ctx,
		// but it is harmless because callers no longer care.
		cancel()
	}()

	wrapped := func() {
		close(done)
		cancel()
	}

	return merged, wrapped
}

// itemContexts extracts the per-item contexts from a batch.
func storeItemContexts(batch []*batchStoreItem) []context.Context {
	out := make([]context.Context, len(batch))
	for i, b := range batch {
		out[i] = b.ctx
	}
	return out
}

func getItemContexts(batch []*batchGetItem) []context.Context {
	out := make([]context.Context, len(batch))
	for i, b := range batch {
		out[i] = b.ctx
	}
	return out
}

// sendStoreBatch flushes a batch of store items to the TeraSlab server.
//
// The flush context is the merged cancellation of every item's context: it
// fires only when *all* callers have given up. Per-caller cancellation is
// observed at the caller side (via select on done/ctx) — abandoned items still
// receive a result on their buffered done channel, so the batcher goroutine
// never blocks on a vanished caller.
func (s *Store) sendStoreBatch(batch []*batchStoreItem) {
	ctx, cancel := mergeBatchContexts(storeItemContexts(batch))
	defer cancel()

	items := make([]teraslab.CreateItem, len(batch))
	for i, b := range batch {
		items[i] = b.createItem
	}

	result, err := s.client.CreateBatch(ctx, items)
	if err != nil {
		// Check for partial errors
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Map partial errors back to individual items
			errMap := make(map[uint32]*teraslab.BatchItemError)
			for i := range pe.Errors {
				errMap[pe.Errors[i].ItemIndex] = &pe.Errors[i]
			}
			for i, b := range batch {
				if itemErr, exists := errMap[uint32(i)]; exists { //nolint:gosec
					b.done <- batchStoreResult{err: mapErrorCode(itemErr.Code)}
				} else {
					b.done <- batchStoreResult{data: &meta.Data{}}
				}
			}
			return
		}

		// Global error - send to all items
		for _, b := range batch {
			b.done <- batchStoreResult{err: err}
		}
		return
	}

	_ = result
	// Success for all items
	for _, b := range batch {
		b.done <- batchStoreResult{data: &meta.Data{}}
	}
}

// sendGetBatch flushes a batch of get items to the TeraSlab server.
//
// Uses the merged context of all in-flight items so the RPC keeps running as
// long as at least one caller is still waiting (see sendStoreBatch).
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	ctx, cancel := mergeBatchContexts(getItemContexts(batch))
	defer cancel()

	// Use the union of all requested field masks so every item gets what it needs.
	var mask uint32
	txids := make([]teraslab.TxID, len(batch))
	for i, b := range batch {
		txids[i] = hashToTxID(&b.hash)
		mask |= b.fieldMask
	}

	records, err := s.client.GetRecordBatch(ctx, mask, txids)
	if err != nil {
		for _, b := range batch {
			b.done <- batchGetResult{err: err}
		}
		return
	}

	for i, b := range batch {
		if i >= len(records) || !records[i].Found {
			b.done <- batchGetResult{err: mapErrorCode(teraslab.ErrCodeTxNotFound)}
			continue
		}

		data, err := recordToMetaData(records[i])
		if err != nil {
			b.done <- batchGetResult{err: err}
			continue
		}

		b.done <- batchGetResult{data: data}
	}
}
