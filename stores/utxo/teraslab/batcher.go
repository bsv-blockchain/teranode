package teraslab

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	teraslab "github.com/icellan/teraslab/client/go"
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

// batchSetLockedCall is one Store.SetLocked() invocation awaiting coalesced
// dispatch. Concurrent SetLocked() calls that share the same `value` bool are
// merged into a single wire SetLockedBatch RPC (the wire op carries one `value`
// per RPC), then per-item errors are mapped back to each call by its index span.
// SetLocked returns only an error to its caller, so a call fails iff any of ITS
// own items errored (conveyed via partialErrorToError, identical to the
// unbatched path).
type batchSetLockedCall struct {
	ctx    context.Context
	value  bool
	hashes []chainhash.Hash
	done   chan error
}

// batchDecorateCall is one Store.BatchDecorate() invocation awaiting coalesced
// dispatch. Concurrent calls are merged into ONE GetRecordBatch RPC over the
// union of their txids and the OR of their field masks; each call then fills its
// OWN unresolved slice from its slice of the shared response (tracked by index
// span, like sendSpendBatch). The caller-visible return contract (nil on
// success; per-item .Data/.Err) is preserved exactly.
type batchDecorateCall struct {
	ctx        context.Context
	fieldMask  uint32
	unresolved []*utxo.UnresolvedMetaData
	done       chan error
}

// batchGetItem is a single item in the get batcher.
type batchGetItem struct {
	ctx       context.Context
	hash      chainhash.Hash
	fieldMask uint32
	// includeTx controls whether the stored inputs/outputs are decoded into
	// data.Tx. Get sets it true; GetMeta sets it false to skip a body decode it
	// would immediately discard.
	includeTx bool
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

	// Only contexts that can actually be canceled are worth watching. A nil
	// context, or one whose Done() channel is nil (e.g. context.Background /
	// context.TODO), can NEVER fire, so a watcher goroutine for it would block
	// forever doing nothing but cost a goroutine + scheduler/GC churn. At high
	// throughput the per-item watcher spawn/teardown (one goroutine per batched
	// item per flush — hundreds per batch, tens of thousands/sec) dominated the
	// client CPU (profiled: mergeBatchContexts.func1 + selectgo + GC storm).
	//
	// Semantics preserved: the merged context auto-cancels (aborting the in-flight
	// RPC) only when EVERY caller has gone away. A non-cancellable caller never
	// goes away, so if ANY item context is non-cancellable the batch must run to
	// completion regardless — watch nothing and rely on explicit cancel (flush
	// completion) / the RPC's own deadline. When ALL item contexts are cancellable
	// we watch exactly those and cancel once all have fired.
	cancellable := make([]context.Context, 0, len(ctxs))
	allCancellable := true
	for _, c := range ctxs {
		if c == nil || c.Done() == nil {
			allCancellable = false
			continue
		}
		cancellable = append(cancellable, c)
	}

	if !allCancellable || len(cancellable) == 0 {
		// At least one caller never goes away → never auto-cancel. No watcher
		// goroutines (release() still cancels `merged`, preserving the contract).
		return merged, cancel
	}

	var wg sync.WaitGroup
	wg.Add(len(cancellable))

	// done is closed to release watcher goroutines once the flush completes.
	done := make(chan struct{})

	for _, c := range cancellable {
		c := c
		go func() {
			defer wg.Done()
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

// sendSetLockedBatch coalesces concurrent SetLocked() calls into wire
// SetLockedBatch RPCs.
//
// The wire op carries ONE `value` bool per RPC, so calls are grouped by `value`;
// each distinct-value group is one RPC. Within a group, every call's hashes are
// concatenated and each call's [offset, offset+len) span is recorded. A global
// (non-partial) error fans out to every call in the group. On a *PartialError,
// only the per-item errors falling inside a call's span are attributed to it —
// a call's result is partialErrorToError over just its own errors (so a caller
// whose hashes all succeeded still gets nil even if a sibling call failed),
// matching the unbatched Store.SetLocked path exactly.
func (s *Store) sendSetLockedBatch(batch []*batchSetLockedCall) {
	groups := make(map[bool][]int)
	for i, c := range batch {
		groups[c.value] = append(groups[c.value], i)
	}

	for value, idxs := range groups {
		var combined []teraslab.TxID
		offsets := make([]int, len(idxs))
		ctxs := make([]context.Context, len(idxs))
		for gi, ci := range idxs {
			offsets[gi] = len(combined)
			ctxs[gi] = batch[ci].ctx
			for hi := range batch[ci].hashes {
				combined = append(combined, hashToTxID(&batch[ci].hashes[hi]))
			}
		}

		ctx, cancel := mergeBatchContexts(ctxs)
		_, err := s.setLockedFn(ctx, value, combined)
		cancel()

		// Non-partial (global) error fans out to every call in the group.
		pe, isPartial := err.(*teraslab.PartialError)
		if err != nil && !isPartial {
			for _, ci := range idxs {
				batch[ci].done <- err
			}
			continue
		}

		// Success or partial: attribute per-item errors to each call by span.
		for gi, ci := range idxs {
			if !isPartial {
				batch[ci].done <- nil
				continue
			}

			lo := offsets[gi]
			hi := lo + len(batch[ci].hashes)
			var local *teraslab.PartialError
			for _, ie := range pe.Errors {
				if int(ie.ItemIndex) >= lo && int(ie.ItemIndex) < hi {
					if local == nil {
						local = &teraslab.PartialError{}
					}
					ie.ItemIndex -= uint32(lo) //nolint:gosec
					local.Errors = append(local.Errors, ie)
				}
			}
			batch[ci].done <- partialErrorToError("SetLocked", local)
		}
	}
}

// sendDecorateBatch coalesces concurrent BatchDecorate() calls into one
// GetRecordBatch RPC.
//
// The union of every call's txids is sent under the OR of every call's field
// mask (so each caller receives at least the fields it asked for). The response
// is positionally aligned with the sent txids; each call's [offset, offset+len)
// span is sliced back and the call fills its OWN unresolved entries — .Data on
// found, .Err on miss/decode error — exactly as the unbatched BatchDecorate did.
// A global RPC error is returned to every call; per-item handling never errors
// the call as a whole (BatchDecorate returns nil and reports per-item via .Err).
func (s *Store) sendDecorateBatch(batch []*batchDecorateCall) {
	var combined []teraslab.TxID
	var mask uint32
	offsets := make([]int, len(batch))
	ctxs := make([]context.Context, len(batch))
	for i, c := range batch {
		offsets[i] = len(combined)
		ctxs[i] = c.ctx
		mask |= c.fieldMask
		for j := range c.unresolved {
			combined = append(combined, hashToTxID(&c.unresolved[j].Hash))
		}
	}

	ctx, cancel := mergeBatchContexts(ctxs)
	records, err := s.getRecordFn(ctx, mask, combined)
	cancel()

	if err != nil {
		for _, c := range batch {
			c.done <- err
		}
		return
	}

	for i, c := range batch {
		lo := offsets[i]
		for j, umd := range c.unresolved {
			ri := lo + j
			if ri >= len(records) || !records[ri].Found {
				// Mark not-found items so callers can distinguish "not found"
				// from "not processed".
				umd.Data = nil
				umd.Err = errors.NewTxNotFoundError("%s not found", umd.Hash.String())
				continue
			}

			data, derr := recordToMetaData(records[ri])
			if derr != nil {
				umd.Err = derr
				continue
			}

			umd.Data = data
		}
		c.done <- nil
	}
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

		data, err := recordToMetaDataMasked(records[i], b.includeTx)
		if err != nil {
			b.done <- batchGetResult{err: err}
			continue
		}

		b.done <- batchGetResult{data: data}
	}
}
