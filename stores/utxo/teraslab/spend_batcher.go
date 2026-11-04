package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslab "github.com/icellan/teraslab/client/go"
)

// maxAggregatedSpendErrs caps how many per-input spend errors are wrapped into
// the returned top-level error (mirrors the Aerospike backend); an uncapped
// chain is O(N^2) to build and to walk with errors.Is. The per-input errors also
// remain available on each Spend.Err.
const maxAggregatedSpendErrs = 10

// batchSpendItem is one transaction's spend, submitted to the spend batcher. The
// whole transaction's inputs (items/results) and its per-call SpendBatch params
// travel together so the flush keeps results — and any atomic rollback — scoped
// to the originating transaction even when many transactions share one RPC.
type batchSpendItem struct {
	ctx          context.Context
	items        []teraslab.SpendItem
	results      []*utxo.Spend
	params       teraslab.SpendBatchParams
	spendingTxID *chainhash.Hash
	done         chan batchSpendResult
}

// batchSpendResult is delivered to a Spend() caller via its done channel.
type batchSpendResult struct {
	spends []*utxo.Spend
	err    error
}

func spendItemContexts(batch []*batchSpendItem) []context.Context {
	out := make([]context.Context, len(batch))
	for i, b := range batch {
		out[i] = b.ctx
	}
	return out
}

// finalizeSpendResults applies a transaction's per-input SpendBatch outcomes to
// its spend results. errs maps a tx-local input index to its server error. It
// reports whether the transaction failed (hasError) and whether its succeeded
// inputs must be rolled back (needsRollback — true only for genuine-invalid
// codes, never transient/internal faults). This is the pre-batcher inline Spend
// logic, now scoped to one transaction within a shared batch.
func finalizeSpendResults(results []*utxo.Spend, errs map[int]*teraslab.BatchItemError) (hasError, needsRollback bool) {
	for idx, be := range errs {
		if idx < 0 || idx >= len(results) {
			continue
		}
		results[idx].Err = mapErrorCode(be.Code)
		// For AlreadySpent the conflicting txid is carried in the error data.
		if be.Code == teraslab.ErrCodeAlreadySpent && len(be.Data) >= 36 {
			if csd := wireToSpendingData(teraslab.SpendingData(be.Data[:36])); csd != nil {
				results[idx].ConflictingTxID = csd.TxID
			}
		}
		if spendCodeNeedsRollback(be.Code) {
			needsRollback = true
		}
		hasError = true
	}
	return hasError, needsRollback
}

// sendSpendBatch flushes accumulated per-transaction spends. A SpendBatch RPC
// carries a single SpendBatchParams for all its items, so spends are grouped by
// params (block height + ignore flags + retention) and each group is sent as one
// RPC. The server performs one redo append + fsync per RPC, so batching many
// transactions into one group amortizes the durability cost — the single biggest
// catchup throughput lever, since every block's transactions share the same
// params and thus coalesce into one (or few) RPCs.
func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	ctx, cancel := mergeBatchContexts(spendItemContexts(batch))
	defer cancel()

	groups := make(map[teraslab.SpendBatchParams][]*batchSpendItem)
	order := make([]teraslab.SpendBatchParams, 0)
	for _, b := range batch {
		if _, ok := groups[b.params]; !ok {
			order = append(order, b.params)
		}
		groups[b.params] = append(groups[b.params], b)
	}

	for _, key := range order {
		s.flushSpendGroup(ctx, key, groups[key])
	}
}

// flushSpendGroup sends one SpendBatch RPC for all transactions sharing params,
// maps the global-indexed response back to each transaction, performs per-tx
// atomic rollback, and signals each caller.
func (s *Store) flushSpendGroup(ctx context.Context, params teraslab.SpendBatchParams, group []*batchSpendItem) {
	// Concatenate every tx's items, tracking each tx's start offset so the
	// global-indexed response can be split back per transaction.
	var items []teraslab.SpendItem
	offsets := make([]int, len(group))
	for i, b := range group {
		offsets[i] = len(items)
		items = append(items, b.items...)
	}

	_, err := s.client.SpendBatch(ctx, params, items)

	globalErrs := make(map[int]*teraslab.BatchItemError)
	var transportErr error
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			for i := range pe.Errors {
				globalErrs[int(pe.Errors[i].ItemIndex)] = &pe.Errors[i]
			}
		} else {
			transportErr = err
		}
	}

	for i, b := range group {
		if transportErr != nil {
			b.done <- batchSpendResult{err: transportErr}
			continue
		}

		start := offsets[i]
		localErrs := make(map[int]*teraslab.BatchItemError)
		for li := range b.items {
			gi := start + li
			if be, ok := globalErrs[gi]; ok {
				localErrs[li] = be
			}
		}

		hasError, needsRollback := finalizeSpendResults(b.results, localErrs)
		if hasError {
			if needsRollback {
				s.rollbackPartialSpend(b)
			}
			// Wrap the per-input causes into the top-level error (mirrors Aerospike
			// spend.go) so errors.Is(err, ErrTxNotFound/ErrSpent/ErrFrozen) matches
			// the returned error, not only the per-input Spend.Err slice.
			failed := make([]error, 0, len(b.results))
			for _, sp := range b.results {
				if sp.Err != nil {
					failed = append(failed, sp.Err)
				}
			}
			b.done <- batchSpendResult{spends: b.results, err: errors.NewUtxoError("teraslab spend failed", errors.JoinCapped(maxAggregatedSpendErrs, failed...))}
			continue
		}
		b.done <- batchSpendResult{spends: b.results}
	}
}

// rollbackPartialSpend unspends the inputs of b that DID spend, so a genuinely
// invalid transaction leaves its parent UTXOs spendable by a later valid tx.
// Uses a fresh context so a cancelled caller cannot leak the partial spend.
func (s *Store) rollbackPartialSpend(b *batchSpendItem) {
	succeeded := make([]*utxo.Spend, 0, len(b.results))
	for _, sp := range b.results {
		if sp.Err == nil {
			succeeded = append(succeeded, sp)
		}
	}
	if len(succeeded) == 0 {
		return
	}
	if rbErr := s.Unspend(context.Background(), succeeded); rbErr != nil {
		s.logger.Errorf("[TeraSlab] failed to roll back partial spend for %s: %v", b.spendingTxID.String(), rbErr)
	}
}
