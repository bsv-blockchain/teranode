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

// batchSpendItem is a single item in the spend batcher.
type batchSpendItem struct {
	ctx               context.Context
	item              teraslab.SpendItem
	blockHeight       uint32
	ignoreConflicting bool
	ignoreLocked      bool
	done              chan error
}

// batchLockedItem is a single item in the locked batcher.
type batchLockedItem struct {
	ctx    context.Context
	txHash chainhash.Hash
	value  bool
	done   chan error
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

func spendItemContexts(batch []*batchSpendItem) []context.Context {
	out := make([]context.Context, len(batch))
	for i, b := range batch {
		out[i] = b.ctx
	}
	return out
}

func lockedItemContexts(batch []*batchLockedItem) []context.Context {
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

// sendSpendBatch flushes a batch of spend items to the TeraSlab server.
//
// Uses the merged context of all in-flight items (see sendStoreBatch).
func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := mergeBatchContexts(spendItemContexts(batch))
	defer cancel()

	// Use the flags from the first item (they should all be the same within a batch)
	params := teraslab.SpendBatchParams{
		IgnoreConflicting:    batch[0].ignoreConflicting,
		IgnoreLocked:         batch[0].ignoreLocked,
		CurrentBlockHeight:   s.blockHeight.Load(),
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	items := make([]teraslab.SpendItem, len(batch))
	for i, b := range batch {
		items[i] = b.item
	}

	_, err := s.client.SpendBatch(ctx, params, items)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			errMap := make(map[uint32]*teraslab.BatchItemError)
			for i := range pe.Errors {
				errMap[pe.Errors[i].ItemIndex] = &pe.Errors[i]
			}
			for i, b := range batch {
				if itemErr, exists := errMap[uint32(i)]; exists { //nolint:gosec
					b.done <- mapErrorCode(itemErr.Code)
				} else {
					b.done <- nil
				}
			}
			return
		}

		for _, b := range batch {
			b.done <- err
		}
		return
	}

	for _, b := range batch {
		b.done <- nil
	}
}

// sendLockedBatch flushes a batch of locked items to the TeraSlab server.
//
// Uses the merged context of all in-flight items (see sendStoreBatch).
func (s *Store) sendLockedBatch(batch []*batchLockedItem) {
	ctx, cancel := mergeBatchContexts(lockedItemContexts(batch))
	defer cancel()

	// Group by value (true/false) since SetLockedBatch requires a single value
	trueItems := make([]teraslab.TxID, 0)
	falseItems := make([]teraslab.TxID, 0)
	trueIndices := make([]int, 0)
	falseIndices := make([]int, 0)

	for i, b := range batch {
		txid := hashToTxID(&b.txHash)
		if b.value {
			trueItems = append(trueItems, txid)
			trueIndices = append(trueIndices, i)
		} else {
			falseItems = append(falseItems, txid)
			falseIndices = append(falseIndices, i)
		}
	}

	// Process true items
	if len(trueItems) > 0 {
		_, err := s.client.SetLockedBatch(ctx, true, trueItems)
		if err != nil {
			if pe, ok := err.(*teraslab.PartialError); ok {
				errMap := make(map[uint32]*teraslab.BatchItemError)
				for i := range pe.Errors {
					errMap[pe.Errors[i].ItemIndex] = &pe.Errors[i]
				}
				for subIdx, batchIdx := range trueIndices {
					if itemErr, exists := errMap[uint32(subIdx)]; exists { //nolint:gosec
						batch[batchIdx].done <- mapErrorCode(itemErr.Code)
					} else {
						batch[batchIdx].done <- nil
					}
				}
			} else {
				for _, idx := range trueIndices {
					batch[idx].done <- err
				}
			}
		} else {
			for _, idx := range trueIndices {
				batch[idx].done <- nil
			}
		}
	}

	// Process false items
	if len(falseItems) > 0 {
		_, err := s.client.SetLockedBatch(ctx, false, falseItems)
		if err != nil {
			if pe, ok := err.(*teraslab.PartialError); ok {
				errMap := make(map[uint32]*teraslab.BatchItemError)
				for i := range pe.Errors {
					errMap[pe.Errors[i].ItemIndex] = &pe.Errors[i]
				}
				for subIdx, batchIdx := range falseIndices {
					if itemErr, exists := errMap[uint32(subIdx)]; exists { //nolint:gosec
						batch[batchIdx].done <- mapErrorCode(itemErr.Code)
					} else {
						batch[batchIdx].done <- nil
					}
				}
			} else {
				for _, idx := range falseIndices {
					batch[idx].done <- err
				}
			}
		} else {
			for _, idx := range falseIndices {
				batch[idx].done <- nil
			}
		}
	}
}
