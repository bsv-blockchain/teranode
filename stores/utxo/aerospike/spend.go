// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to utxostore_utxoBatchSize UTXOs (default 128)
//   - Pagination Records: Additional records for transactions with more outputs than utxostore_utxoBatchSize (default 128)
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records when outputs exceed utxostore_utxoBatchSize
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
)

// maxAggregatedSpendErrs caps how many per-spend errors are wrapped into the
// aggregate error returned by Spend. The failure count scales with the tx's
// input count, and an uncapped chain makes error construction and every
// errors.Is on it quadratic. See errors.JoinCapped.
const maxAggregatedSpendErrs = 10

// Spend operations in the Aerospike UTXO store handle spending UTXOs through
// batched Lua operations with automatic DAH management and error handling.
//
// # Architecture
//
// The spend process uses a multi-layered approach:
//   1. Batch collection of spend requests
//   2. Grouping of spends by transaction
//   3. Atomic Lua scripts for spending
//   4. DAH management for cleanup
//   5. External storage synchronization
//
// # Main Types

// batchSpend represents a single UTXO spend request in a batch
type batchSpend struct {
	spend       *utxo.Spend // UTXO to spend
	blockHeight uint32      // Current block height
	group       *completion.Group
	completed   atomic.Bool // guards exactly-once completion (Done + slot write)
	// published is stored (release) AFTER spend.Err is written, so a caller
	// acquire-loading it on the abort path synchronizes-with that write and can
	// then safely read the slot (see resolveSpendCompletions). completed alone
	// cannot serve this role: it is set by the CAS, i.e. BEFORE the slot write.
	published         atomic.Bool
	ignoreConflicting bool
	ignoreLocked      bool
}

// complete writes err into the item's result slot (spend.Err) and marks the
// shared group's completion counter. Idempotent: only the first call wins the
// CAS, so dispatch paths that sweep an entire batch on panic (which may include
// items an earlier stage already completed) never double-signal or clobber a
// second value into spend.Err.
//
// Ordering: the winner writes spend.Err, then stores published, then calls
// group.Done(). Readers that got a nil group.Wait (the normal path) are safe
// because Done()'s close(done) synchronizes-with the wait. The abort path
// (group.Wait timed out / ctx cancelled) cannot rely on Done, so it instead
// acquire-loads published: observing published==true synchronizes-with the
// store, and since the slot write is sequenced before that store, the slot is
// then safe to read. Gating the abort read on completed (set by the CAS, BEFORE
// the slot write) would NOT establish that happens-before edge.
func (b *batchSpend) complete(err error) {
	if b.completed.CompareAndSwap(false, true) {
		b.spend.Err = err
		b.published.Store(true)
		b.group.Done()
	}
}

// IncrementSpentRecordsMulti performs a single BatchOperate to increment spent-extra-records for many txids.
// This avoids enqueueing each increment through the batcher and waiting per-item.
func (s *Store) IncrementSpentRecordsMulti(txids []*chainhash.Hash, increment int, blockHeight uint32) error {
	if len(txids) == 0 {
		return nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	batchRecordsPtr := getBatchRecordsSlice(len(txids))
	batchRecords := (*batchRecordsPtr)[:0]

	for _, txid := range txids {
		key, err := aerospike.NewKey(s.namespace, s.setName, txid[:])
		if err != nil {
			*batchRecordsPtr = batchRecords
			putBatchRecordsSlice(batchRecordsPtr)
			return errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(batchUDFPolicy, LuaPackage, key, subOpIncrementSpentExtraRec, "incrementSpentExtraRecs",
			increment,
			int(s.effectiveBlockHeight(blockHeight)),
			s.settings.GetUtxoStoreBlockHeightRetention(),
		))
	}

	if err := s.batchOperate(batchPolicy, batchRecords); err != nil {
		*batchRecordsPtr = batchRecords
		putBatchRecordsSlice(batchRecordsPtr)
		return errors.NewStorageError("[IncrementSpentRecordsMulti] error in aerospike batch", err)
	}

	// Inspect per-record errors
	var aggErr error
	for i := range batchRecords {
		if recErr := batchRecords[i].BatchRec().Err; recErr != nil {
			s.demoteNativeOnUnsupported(recErr)
			if aggErr == nil {
				aggErr = recErr
			} else {
				aggErr = errors.Join(aggErr, recErr)
			}
		}

		// Parse the SUCCESS bin via ParseLuaMapResponse rather than a bare
		// map[interface{}]interface{} assertion: subOpIncrementSpentExtraRec is
		// not fenced, so with native ops enabled the response is decoded through
		// msgpack and can be a map[string]interface{} (or a reflection fallback),
		// which a concrete-type assertion would panic on. ParseLuaMapResponse
		// tolerates every map shape both transports produce (see teranode.go).
		response := batchRecords[i].BatchRec().Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			continue
		}

		rawResponse := response.Bins[LuaSuccess.String()]

		parsed, perr := s.ParseLuaMapResponse(rawResponse)
		if perr != nil {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] failed to parse response bin %q (value %s): %s", describeChainHash(txids[i]), LuaSuccess.String(), describeAerospikeValue(rawResponse), perr.Error(), perr))
			continue
		}

		if parsed.Status != LuaStatusOK {
			aggErr = errors.Join(aggErr, errors.NewProcessingError("[IncrementSpentRecordsMulti][%s] incrementSpentExtraRecs returned %s: %s", describeChainHash(txids[i]), parsed.Status, parsed.Message))
		}
	}

	*batchRecordsPtr = batchRecords
	putBatchRecordsSlice(batchRecordsPtr)

	return aggErr
}

// SetDAHForChildRecordsMulti expands childCount per tx and performs a single BatchOperate
// to set/unset DeleteAtHeight across all child pagination records.
func (s *Store) SetDAHForChildRecordsMulti(items []struct {
	TxID           *chainhash.Hash
	ChildCount     int
	DeleteAtHeight uint32
}) error {
	// Expand into individual child records
	total := 0
	for _, it := range items {
		if it.ChildCount > 0 {
			total += it.ChildCount
		}
	}
	if total == 0 {
		return nil
	}

	batchRecords := make([]aerospike.BatchRecordIfc, 0, total)
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	dahBinName := fields.DeleteAtHeight.String()
	// Pre-create the "unset" operation since it's identical for all unset cases
	unsetOp := aerospike.PutOp(aerospike.NewBin(dahBinName, nil))

	for _, it := range items {
		for i := uint32(1); i <= uint32(it.ChildCount); i++ { // nolint: gosec
			keySource := uaerospike.CalculateKeySourceInternal(it.TxID, i) // children start at 1
			key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				return errors.NewProcessingError("[SetDAHForChildRecordsMulti][%s] failed to create key for pagination record %d: %v", it.TxID.String(), i, err)
			}

			if it.DeleteAtHeight > 0 {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(dahBinName, it.DeleteAtHeight))))
			} else {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, unsetOp))
			}
		}
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("[SetDAHForChildRecordsMulti] failed to set DAH", err)
	}

	var aggErr error
	for _, br := range batchRecords {
		if recErr := br.BatchRec().Err; recErr != nil {
			if aggErr == nil {
				aggErr = recErr
			} else {
				aggErr = errors.Join(aggErr, recErr)
			}
		}
	}

	return aggErr
}

// batchIncrement handles record count updates for paginated transactions
type batchIncrement struct {
	txID        *chainhash.Hash   // Transaction hash
	increment   int               // Count adjustment
	blockHeight uint32            // Height of the operation, for DAH = blockHeight + retention
	group       *completion.Group // shared group the producer waits on
	completed   atomic.Bool       // guards exactly-once completion (see complete)
	res         interface{}       // result slot: raw Lua response; written by the CAS winner, after the CAS and before group.Done()
	err         error             // result slot: written by the CAS winner, after the CAS and before group.Done()
}

// complete writes the result (res, err) into the item's slots and marks the
// shared group's completion counter. Idempotent: only the first call has any
// effect (CAS-guarded), so a dispatch path that sweeps the whole batch on
// panic — which may include an item an earlier stage already completed — never
// double-signals or races a second write into the slots.
//
// The slot writes happen inside the CAS-winner branch, after the CAS succeeds
// and before group.Done(); group.Done()'s close(done) synchronizes-with a nil
// group.Wait(), making the slots safe to read only after group.Wait returns
// nil. completed is the exactly-once guard (CAS), not a publication flag by
// itself. group may be nil (defensive; the sole producer IncrementSpentRecords
// always supplies one) in which case Done is skipped.
func (b *batchIncrement) complete(res interface{}, err error) {
	if b.completed.CompareAndSwap(false, true) {
		b.res = res
		b.err = err
		if b.group != nil {
			b.group.Done()
		}
	}
}

// batchDAH represents a single DeleteAtHeight write on one (child) record.
type batchDAH struct {
	txID           *chainhash.Hash   // Transaction hash
	childIdx       uint32            // Child record index
	deleteAtHeight uint32            // DeleteAtHeight (0 = no delete)
	group          *completion.Group // nil for fire-and-forget submitters (see complete)
	completed      atomic.Bool       // guards exactly-once completion (see complete)
	result         error             // written by the CAS winner, after the CAS and before group.Done()
}

// complete writes err into the item's result slot and marks the shared group's
// completion counter. Idempotent (CAS-guarded), so a panic-recovery sweep over
// an item an earlier stage already completed never double-signals or races a
// second write into result.
//
// group may be nil for a fire-and-forget submitter — one that enqueues the DAH
// write but does not wait for it: the write still executes via the dispatcher,
// so Done is simply skipped. This is a defensive capability; every production
// submitter (SetDAHForChildRecords and the counter-drift master-DAH clear in
// handleExtraRecords) wires a real group and waits synchronously.
func (b *batchDAH) complete(err error) {
	if b.completed.CompareAndSwap(false, true) {
		b.result = err
		if b.group != nil {
			b.group.Done()
		}
	}
}

// handleSpendPanic processes a recovered value from Spend's deferred recover
// and propagates it as an error. Without this, a panic during Spend would be
// logged but the caller would observe (nil, nil) — a silent failure that can
// mask UTXO state corruption.
//
// Uses ERR_UNKNOWN rather than ERR_PROCESSING so the block-validation retry
// classifier (services/blockvalidation/BlockValidation.go) does not treat a
// recovered panic as a transient infrastructure error and retry indefinitely
// against a broken path.
func handleSpendPanic(recovered any, err *error, logger ulogger.Logger) {
	if recovered == nil {
		return
	}

	prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
	logger.Errorf("ERROR panic in aerospike Spend: %v\n%s", recovered, debug.Stack())

	if *err == nil {
		*err = errors.NewUnknownError("panic in Spend: %v", recovered)
	}
}

// Spend marks UTXOs as spent in a batch operation.
// The function:
//  1. Validates inputs
//  2. Batches spend requests
//  3. Handles responses
//  4. Manages rollback on failure
//
// Parameters:
//   - ctx: Context for cancellation
//   - tx: tx to spend
//
// Error handling:
//   - Rolls back successful spends on partial failure
//   - Handles panic recovery
//   - Reports metrics for failures
//
// Example return value:
//
//	spends := []*utxo.Spend{
//	    {
//	        TxID: txHash,
//	        Vout: 0,
//	        UTXOHash: utxoHash,
//	        SpendingTxID: spendingTxHash,
//	    },
//	}
//
//	doubleSpendConflicts := []*chainhash.Hash{
//	    &spendingTxHash,
//	}
//
//	err := store.Spend(ctx, tx)
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) (spends []*utxo.Spend, err error) {
	defer func() {
		handleSpendPanic(recover(), &err, s.logger)
	}()

	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	spends, err = utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	items := make([]*batchSpend, len(spends))
	group := completion.NewGroup(int32(len(spends)))

	// Enqueue all inputs of this tx as one ordered group via a single
	// PutBatchCtx, instead of one PutCtx per input — cutting the per-item
	// channel-send and collector-select overhead to a single operation for the
	// whole tx. Circuit-breaker fast-failed items complete inline (decrementing
	// the group) and are excluded from the enqueued set.
	toEnqueue := make([]*batchSpend, 0, len(spends))

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		item := &batchSpend{
			spend:             spend,
			blockHeight:       blockHeight,
			group:             group,
			ignoreConflicting: useIgnoreConflicting,
			ignoreLocked:      useIgnoreLocked,
		}
		items[idx] = item

		// Fast-fail check: if circuit breaker is already open, reject immediately.
		if s.spendCircuitBreaker != nil && !s.spendCircuitBreaker.Allow() {
			item.complete(errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request"))
			continue
		}

		toEnqueue = append(toEnqueue, item)
	}

	// PutBatchCtx is a no-op on an empty slice (e.g. every input fast-failed).
	// Guard the enqueue: Store.Close may close the spend batcher while an
	// external caller is still spending. One send carries the whole group, so a
	// rejected send rejects every item — complete all of them and let the
	// resolveSpendCompletions path below report it, instead of parking on
	// group.Wait for the full spendTimeout and reporting a bogus timeout.
	if enqueueErr := safeBatcherPutBatchCtx(s.spendBatcher, ctx, toEnqueue, "spend"); enqueueErr != nil {
		for _, item := range toEnqueue {
			item.complete(enqueueErr)
		}
	}

	// One shared wait for the whole batch, instead of one goroutine + timer
	// per input. spendTimeout mirrors the previous per-item fallback.
	spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
	if spendTimeout <= 0 {
		spendTimeout = 30 * time.Second
	}

	if waitErr := group.Wait(ctx, spendTimeout); waitErr != nil {
		// The group did not complete within budget (context canceled, or the
		// dispatcher took longer than spendTimeout). Some items may already
		// have finished before the abort — resolveSpendCompletions(onlyCompleted=true)
		// safely identifies exactly those via each item's own CAS-guarded
		// completed flag and leaves any still-in-flight item's slot
		// untouched, since the dispatcher may still be writing to it.
		result := s.resolveSpendCompletions(ctx, tx, items, true)

		// Roll back the spends we know completed. This is the WEAKEST case for
		// the atomicity contract, not the strictest: onlyCompleted=true above
		// excludes every item whose published flag is not yet set, but the
		// dispatcher runs on s.ctx (not this now-dead caller ctx) and keeps
		// writing to those items after we return here, so a straggler that later
		// succeeds is never rolled back by this call. prometheusUtxoSpendAbortInFlight
		// counts those skipped items so this residual window is measurable
		// rather than invisible; closing it needs attempt identity or
		// create-before-spend ordering, tracked in #1291.
		s.rollbackPartialSpends(tx, result, "batched mode, after wait error")

		// Do NOT return the live spends slice on the abort path: items still
		// in-flight are owned by the dispatcher goroutine, which will keep
		// writing spend.Err after we return here (it runs on s.ctx, not the
		// caller ctx that was just cancelled). Handing that slice back would let
		// a caller read a slot the dispatcher is concurrently writing — a data
		// race, and an in-flight item still reads Err==nil (misclassified as a
		// successful spend). A Wait error means the whole operation failed;
		// callers must treat it as such and not inspect per-item slots.
		if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
			return nil, errors.NewContextCanceledError("[SPEND] context canceled while waiting for batch response: %s", waitErr)
		}

		if prometheusUtxoMapErrors != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "BatchTimeout").Inc()
		}

		return nil, errors.NewServiceUnavailableError("[SPEND] batch operation timed out after %s: %s", spendTimeout, waitErr)
	}

	// group.Wait returned nil: every item has completed, so every slot is
	// safe to read regardless of the CAS flag (onlyCompleted=false).
	result := s.resolveSpendCompletions(ctx, tx, items, false)

	if len(spends) != len(result.spentSpends) { // there must have been failures
		s.rollbackPartialSpends(tx, result, "batched mode")

		// Aggregate with a hard cap. The failure count scales with the tx's
		// input count (a mass DEVICE_OVERLOAD on a 50k-input consolidation tx
		// fails every spend); an uncapped chain is O(N²) to build via pairwise
		// Join and makes every subsequent errors.Is on it walk the full chain.
		// The per-spend errors stay available to the caller via spends[i].Err.
		failedSpends := make([]error, 0, len(spends))

		for _, spend := range spends {
			if spend.Err != nil {
				failedSpends = append(failedSpends, spend.Err)
			}
		}

		// return the errors found
		return spends, errors.NewUtxoError("error in aerospike spend (batched mode) - errors", errors.JoinCapped(maxAggregatedSpendErrs, failedSpends...))
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return spends, nil
}

// spendCompletionResult is the outcome of resolveSpendCompletions: which
// spends succeeded, plus a memo of the "does the spending tx already have a
// record?" lookup used only by resolveSpendCompletions' own loop, so a
// determinate answer within one batch is not looked up twice.
// rollbackPartialSpends does NOT read this memo — it always issues its own
// fresh probe (see rollbackPartialSpends for why).
type spendCompletionResult struct {
	spentSpends []*utxo.Spend

	spenderChecked bool
	spenderExists  bool

	// sawTransientLock records that at least one completed spend failed with
	// ErrTxLocked, which suppresses the rollback (see rollbackPartialSpends) —
	// UNLESS sawUnwinnable is also set.
	sawTransientLock bool

	// sawTransientCreating records that at least one completed spend failed
	// with ErrTxCreating — the OTHER two-phase-commit window the store exposes
	// on a parent record (set in create phase 1, cleared in phase 2 by
	// ensureCreatingBin; see create.go). Tracked as a separate signal from
	// sawTransientLock, not folded into it, because the two windows have
	// different owners/fixes (see rollbackPartialSpends and
	// RollbackOutcomeTransientCreating). Suppresses the rollback under the same
	// rule as sawTransientLock — UNLESS sawUnwinnable is also set.
	sawTransientCreating bool

	// sawUnwinnable records that at least one completed spend failed with an
	// error class meaning this txid can never win (ErrSpent, ErrTxConflicting,
	// ErrFrozen, ErrUtxoHashMismatch). When set, it overrides sawTransientLock's
	// suppression: a concurrent attempt at the SAME txid sees the same
	// outpoints and hits the same unwinnable failure, so it can never reach
	// Create either, meaning nobody can be a legitimate owner of the partial
	// spends and the rollback is both safe and required (#1214).
	sawUnwinnable bool
}

// rollbackDecision is what the spender-existence probe implies for the spends
// that succeeded in a failed batch.
type rollbackDecision int

const (
	// rollbackFire: the spender has no record, so the successful spends are
	// dangling refs and must be reverted.
	rollbackFire rollbackDecision = iota
	// rollbackSkipSpenderExists: the spender has a record, so the refs are not
	// dangling and the slots belong to a live tx — leave them alone.
	rollbackSkipSpenderExists
	// rollbackSkipIndeterminate: the probe itself failed, so we do not know.
	// Skip: wrongly clearing a live spender's slot is unrecoverable, while a
	// surviving dangling ref is at least counted here.
	rollbackSkipIndeterminate
	// rollbackSkipTransientLock: at least one spend failed with ErrTxLocked and
	// nothing else in the batch proved the tx unwinnable, so a concurrent
	// attempt at this same txid may be succeeding right now and may
	// legitimately own the slots we would clear. See rollbackPartialSpends'
	// gate (spendCompletionResult.sawTransientLock / sawUnwinnable).
	rollbackSkipTransientLock
	// rollbackSkipTransientCreating: same suppression as
	// rollbackSkipTransientLock, but caused by ErrTxCreating — the store's other
	// two-phase-commit window on a parent record — rather than ErrTxLocked. Kept
	// as a separate decision (and a separate outcome label) so an operator can
	// tell the two windows apart; when a batch shows both, rollbackPartialSpends
	// reports rollbackSkipTransientLock (see its gate).
	rollbackSkipTransientCreating
)

// String doubles as the "outcome" label value on
// prometheusUtxoPartialSpendRollbacks. Returns the utxo package's shared
// RollbackOutcome* constants so this store and the SQL store cannot drift.
func (d rollbackDecision) String() string {
	switch d {
	case rollbackSkipSpenderExists:
		return utxo.RollbackOutcomeSpenderExists
	case rollbackSkipIndeterminate:
		return utxo.RollbackOutcomeIndeterminate
	case rollbackSkipTransientLock:
		return utxo.RollbackOutcomeTransientLock
	case rollbackSkipTransientCreating:
		return utxo.RollbackOutcomeTransientCreating
	default:
		return utxo.RollbackOutcomeFired
	}
}

// decideRollback maps the existence probe's error to a rollback decision. Only
// a definitive "not found" authorises reverting the spends: any other error
// means the answer is unknown, and an unknown answer must not clear a slot that
// a live spender may own. Kept separate from rollbackPartialSpends so the three
// branches are unit-testable without faulting a live store's Get.
func decideRollback(getErr error) rollbackDecision {
	switch {
	case getErr == nil:
		return rollbackSkipSpenderExists
	case errors.Is(getErr, errors.ErrTxNotFound):
		return rollbackFire
	default:
		return rollbackSkipIndeterminate
	}
}

// rollbackPartialSpends undoes the spends that succeeded in a batch that failed
// as a whole, so that no parent is left naming a spender that has no record.
//
// It is deliberately NOT gated on the error class. The old policy only rolled
// back for "genuinely invalid tx" errors (spent/conflicting/frozen/
// hash-mismatch) and kept partial spends for anything that looked retriable, on
// the grounds that a retry would re-apply them idempotently. That assumption
// does not hold: Validate spends the parents BEFORE creating the spending tx's
// own record, and the callers that see ErrTxLocked / ErrTxNotFound park the tx
// without guaranteeing a retry (legacy netsync's orphan pool is an expiring
// map). The surviving spend then names a tx whose record is never created — a
// dangling ref that makes GetCounterConflictingTxHashes fail with TX_NOT_FOUND
// and permanently wedges block validation on any later block that spends the
// same output (#1214).
//
// It IS gated on the spending tx having no record. With a record present the ref
// is not dangling and rolling back would be the harmful move: it would clear a
// slot a live (possibly mined) tx legitimately owns. That case is reachable
// because the record-level locked/conflicting checks run before the per-UTXO
// same-spender idempotency check, so re-validating an existing tx can fail on
// one parent while succeeding on another.
//
// An indeterminate lookup (any error other than "not found") skips the
// rollback: wrongly clearing a live spender's slot is unrecoverable, whereas a
// surviving dangling ref is at least detectable afterwards via the
// "indeterminate" outcome on prometheusUtxoPartialSpendRollbacks below — no
// automated detector for it exists yet.
func (s *Store) rollbackPartialSpends(tx *bt.Tx, result *spendCompletionResult, phase string) {
	if len(result.spentSpends) == 0 {
		return
	}

	// A transient window means a concurrent attempt at this same txid may be
	// the legitimate owner of these slots, and nothing in the stored spending
	// data lets a rollback tell the two apart — so do not touch them. "Transient
	// window" is the store's set of two-phase-commit windows on a parent
	// record, not a single error: ErrTxLocked (set/cleared around the record's
	// own locked bin) and ErrTxCreating (set in create phase 1, cleared in
	// phase 2 by ensureCreatingBin; see create.go, teranode.lua:297) are both
	// windows a legitimate concurrent creator passes through, and once
	// create-first (#1355) is enabled a parent's window becomes a creating
	// window rather than a locked one — so both must gate the rollback the same
	// way.
	//
	// That exclusion only holds while nothing else in the batch proves the tx
	// unwinnable. If the batch ALSO contains ErrSpent, ErrTxConflicting,
	// ErrFrozen or ErrUtxoHashMismatch (sawUnwinnable), this txid can never
	// reach Create no matter who is racing it: a concurrent attempt sees the
	// same outpoints and hits the same unwinnable failure, so nobody can be a
	// legitimate owner of the partial spends and the rollback is both safe and
	// required — leaving them in place here would be the exact #1214 shape
	// (a parent naming a record-less spender).
	//
	// So the transient-only case is a deliberately uncovered flavour of #1214:
	// create-first ordering (#1355) covers it by making the record exist
	// before any spend, and attempt identity in the stored spending data (#1291)
	// would cover it by letting the ownership check tell two attempts apart.
	// Neither belongs in an error-path rollback. Mirrors the sql store's
	// hasTransientLockFailure/hasUnwinnableFailure doc comments — keep in step.
	if (result.sawTransientLock || result.sawTransientCreating) && !result.sawUnwinnable {
		// When both transient signals fire in the same batch, report
		// transient_lock: it is the flavour with an existing dashboard history
		// and the more commonly hit window today (creating only shows up on
		// wide parents beyond utxoBatchSize); both boil down to the same
		// suppression rule above.
		decision := rollbackSkipTransientLock
		if result.sawTransientCreating && !result.sawTransientLock {
			decision = rollbackSkipTransientCreating
		}

		if prometheusUtxoPartialSpendRollbacks != nil {
			prometheusUtxoPartialSpendRollbacks.WithLabelValues(decision.String()).Inc()
		}

		return
	}

	// Always probe fresh here rather than reusing resolveSpendCompletions'
	// memo: that memo answers a different question (the ErrTxNotFound
	// "already blessed" fallback), taken under a different ctx, potentially
	// many round trips earlier. A stale "absent" answer reused here is
	// exactly what would let this rollback clear a live spender's slot.
	//
	// The rollback context is deliberately detached from BOTH the caller's ctx
	// (already canceled/dead on the abort path this is called from) and s.ctx,
	// but bounded so a wedged store cannot pin this goroutine forever.
	//
	// Not s.ctx: it is canceled when the store shuts down, which would make the
	// probe below fail as "indeterminate" and skip the rollback altogether —
	// leaving the dangling refs this function exists to prevent, exactly when a
	// shutdown races in-flight spends. Cleanup of writes we already made has to
	// outlive the store's own context; the timeout is what keeps that bounded.
	//
	// Deliberately SpendRollbackTimeout, not SpendWaitTimeout: SpendWaitTimeout
	// is the end-to-end budget for ONE batched spend, but this rollback is
	// len(spentSpends) sequential single-record Unspend calls plus the probe
	// above — on a wide transaction it needs a multiple of that budget, not the
	// same one. On overrun the loop aborts and every spend past that point
	// stays applied (a bounded truncation, not a full revert), counted by
	// utxo_spend_rollback_failed.
	rbTimeout := s.settings.UtxoStore.SpendRollbackTimeout
	if rbTimeout <= 0 {
		rbTimeout = 120 * time.Second
	}

	rbCtx, cancel := context.WithTimeout(context.Background(), rbTimeout)
	defer cancel()

	_, getErr := s.Get(rbCtx, tx.TxIDChainHash(), fields.Fee)

	decision := decideRollback(getErr)

	if prometheusUtxoPartialSpendRollbacks != nil {
		prometheusUtxoPartialSpendRollbacks.WithLabelValues(decision.String()).Inc()
	}

	switch decision {
	case rollbackSkipSpenderExists:
		return
	case rollbackSkipIndeterminate:
		s.logger.Errorf("[SPEND][%s] cannot determine whether spender exists, skipping rollback of %d partial spend(s) (%s): %v",
			tx.TxID(), len(result.spentSpends), phase, getErr)

		return
	case rollbackFire:
	}

	// Unspend is ownership-checked (teranode.lua only clears spending_data it
	// still owns), which stops this from wiping a *different* tx's spend. It
	// does not answer the question that matters here: SpendingData is just
	// {TxID, Vin} (stores/utxo/spend/spending_data.go), with no attempt
	// identity, so the check cannot distinguish two concurrent attempts that
	// both spend as the SAME txid. A check-then-act window therefore remains
	// between the probe above and this call. Closing it needs attempt
	// identity, or creating the spending tx's record before spending its
	// parents — tracked in #1291; the outcome metrics here make the residual
	// window measurable in the meantime.
	// Call the counting form, not Unspend: the counter must be a ref count so it
	// means the same thing as the sql store's counter of the same name. Aggregating
	// calls on one backend and dangling refs on the other would be exactly the
	// per-backend drift the shared outcome labels exist to prevent.
	unspendFailed, unspendErr := s.unspend(rbCtx, result.spentSpends)
	if unspendErr != nil {
		if prometheusUtxoSpendRollbackFailed != nil {
			prometheusUtxoSpendRollbackFailed.Add(float64(unspendFailed))
		}

		s.logger.Errorf("[SPEND][%s] rolled back %d of %d partial spend(s), %d left dangling (%s): %v",
			tx.TxID(), len(result.spentSpends)-unspendFailed, len(result.spentSpends), unspendFailed, phase, unspendErr)

		return
	}

	s.logger.Warnf("[SPEND][%s] rolled back %d partial spend(s) (%s)", tx.TxID(), len(result.spentSpends), phase)
}

// resolveSpendCompletions applies the ErrTxNotFound "already blessed"
// fallback and conflict-data extraction to each item exactly once, and
// reports which spends succeeded (the set the caller rolls back when the
// batch as a whole failed).
//
// When onlyCompleted is true (the group.Wait abort path), an item whose
// published flag is still false is skipped without touching its slot: the
// dispatcher may still be writing it (or have won the CAS but not yet stored
// published), and reading spend.Err before observing published==true would
// race. Observing published==true synchronizes-with that item's complete()
// store, which is sequenced after the slot write, so the slot is safely
// readable for exactly the items this loop processes. When onlyCompleted is
// false (the normal, non-abort path), group.Wait having returned nil already
// guarantees every item completed, so the flag is a no-op safety check here,
// not the source of truth.
//
// An unpublished item is skipped in its entirety on the abort path: it is
// invisible not just to spentSpends but also to sawTransientLock,
// sawTransientCreating and sawUnwinnable, for the same race reason (reading
// spend.Err before observing published would race). That means a transient
// window (locked or creating) sitting on an unpublished item does NOT
// suppress rollbackPartialSpends here — the rollback can fire and clear a
// slot a concurrent attempt at this same txid legitimately owns. This is an
// accepted residual of the abort path specifically (prometheusUtxoSpendAbortInFlight
// counts it below); closing it needs attempt identity (#1291) or
// create-before-spend ordering (#1355), same as the transient-window gate
// itself.
func (s *Store) resolveSpendCompletions(ctx context.Context, tx *bt.Tx, items []*batchSpend, onlyCompleted bool) *spendCompletionResult {
	result := &spendCompletionResult{spentSpends: make([]*utxo.Spend, 0, len(items))}

	for _, item := range items {
		if onlyCompleted && !item.published.Load() {
			// This item is invisible to sawTransientLock, sawTransientCreating and
			// sawUnwinnable as well as to spentSpends, because reading spend.Err
			// before observing published would race with the dispatcher. Two
			// consequences on the abort path, both accepted rather than fixed:
			// a transient 2PC window among unpublished items does NOT suppress the
			// rollback, so the rollback can clear a slot a concurrent attempt at
			// this same txid legitimately owns; and an unwinnable failure among
			// them cannot re-enable a rollback that a published transient window
			// suppressed. The sql store's guard scans the whole spends slice, so
			// the two backends genuinely take different inputs here. Closing this
			// needs attempt identity (#1291) or create-before-spend ordering
			// (#1355); prometheusUtxoSpendAbortInFlight counts how often the
			// window is entered at all.
			if prometheusUtxoSpendAbortInFlight != nil {
				prometheusUtxoSpendAbortInFlight.Inc()
			}

			continue
		}

		spend := item.spend

		if spend.Err != nil && errors.Is(spend.Err, errors.ErrTxNotFound) {
			// the parent transaction was not found, this can happen when the parent tx has been DAH'd and removed from
			// the utxo store. We can check whether the tx already exists, which means it has been validated and
			// blessed. In this case we can just clear the error.
			//
			// The outcome is memoized on result (both ways) so a determinate
			// answer (found, or definitively not-found) is looked up at most once
			// per batch. A hard lookup failure (indeterminate: neither of those) is
			// NOT memoized — result.spenderChecked stays false — so it is retried
			// on the next ErrTxNotFound item in this batch. rollbackPartialSpends
			// does not read this memo at all; it always probes fresh.
			if !result.spenderChecked {
				if _, getErr := s.Get(ctx, tx.TxIDChainHash(), fields.Fee); getErr == nil {
					s.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", tx.TxID())

					result.spenderExists = true
					result.spenderChecked = true
				} else if errors.Is(getErr, errors.ErrTxNotFound) {
					result.spenderChecked = true
				}
			}

			if result.spenderExists {
				spend.Err = nil
			}
		}

		if spend.Err != nil {
			if errors.Is(spend.Err, errors.ErrTxLocked) {
				result.sawTransientLock = true
			}

			// ErrTxCreating is the store's OTHER two-phase-commit window on a
			// parent: creating is set in create phase 1 and cleared in phase 2
			// (ensureCreatingBin, create.go), so this is transient for the same
			// reason ErrTxLocked is — see rollbackPartialSpends' gate.
			if errors.Is(spend.Err, errors.ErrTxCreating) {
				result.sawTransientCreating = true
			}

			// This txid can never win: every concurrent attempt at it sees the
			// same outpoints and hits the same unwinnable failure, so nobody
			// can reach Create either. See rollbackPartialSpends' gate.
			if errors.Is(spend.Err, errors.ErrSpent) || errors.Is(spend.Err, errors.ErrTxConflicting) ||
				errors.Is(spend.Err, errors.ErrFrozen) || errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
				result.sawUnwinnable = true
			}

			s.logger.Debugf("[SPEND][%s:%d] error in aerospike spend: %+v", spend.TxID.String(), spend.Vout, spend.Err)

			var errSpent *errors.UtxoSpentErrData
			if errors.AsData(spend.Err, &errSpent) {
				spend.ConflictingTxID = errSpent.SpendingData.TxID
			}

			// don't stop processing the rest of the batch, we want to see all errors
			continue
		}

		result.spentSpends = append(result.spentSpends, spend)
	}

	return result
}

type keyIgnoreLocked struct {
	key               *aerospike.Key
	hash              *chainhash.Hash
	blockHeight       uint32
	ignoreConflicting bool
	ignoreLocked      bool
}

// useExpressionSpend returns true when the expression-based spend path is safe for
// the configured store. It is only implemented and validated for single-UTXO records
// (utxoBatchSize == 1); multi-UTXO records (utxoBatchSize > 1) continue to use Lua.
// The expression filter does byte-compare the element at the offset against the
// expected UTXO hash (see buildSpendFilterExpression), so the single-UTXO path
// cannot mutate the wrong slot; extending this to multi-UTXO records is unimplemented,
// not impossible.
func (s *Store) useExpressionSpend() bool {
	return s.settings.Aerospike.EnableSpendFilterExpressions && s.utxoBatchSize == 1
}

// sendSpendBatchLua processes a batch of spend requests via Lua scripts or expressions.
// The function:
//  1. Groups spends by transaction
//  2. Creates batch UDF operations or expression-based operations
//  3. Executes Lua scripts or expressions
//  4. Handles responses and errors
//  5. Manages DAH settings
//  6. Updates external storage
func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	// go-batcher recovers panics in this fn; re-complete every item on panic
	// (e.g. the unchecked .(int) in processSpendBatchResults) so a crash
	// cannot orphan waiting spenders. complete is a no-op for items already
	// completed (CAS-guarded), so this never double-delivers or races.
	defer func() {
		signalBatchPanic(recover(), batch, "sendSpendBatchLua", s.logger, func(it *batchSpend, err error) {
			if it != nil {
				it.complete(err)
			}
		})
	}()

	batch = utxo.FilterConflictingDuplicateSpendClaims(batch,
		func(item *batchSpend) *utxo.Spend {
			if item == nil {
				return nil
			}
			return item.spend
		},
		func(item *batchSpend, err error) {
			item.complete(err)
		},
	)
	if len(batch) == 0 {
		return
	}

	// Use expression-based implementation only when each Aerospike record holds a single
	// UTXO (utxoBatchSize == 1). With multiple UTXOs per record, the expression cannot
	// byte-compare the specific UTXO hash at a list offset, so we fall back to Lua which
	// performs the strict precondition check inside the UDF.
	if s.useExpressionSpend() {
		s.SpendMultiWithExpressions(s.ctx, batch)
		return
	}

	s.executeLuaSpendBatch(batch)
}

// executeLuaSpendBatch runs the Lua UDF spend pipeline for the provided batch. It is the
// shared backend used by sendSpendBatchLua's Lua route and by the expression path's
// retry-through-Lua handler. Callers MUST have already run any duplicate-claim
// filtering, since this method does not re-run it and assumes every item still expects
// exactly one response on its errCh.
func (s *Store) executeLuaSpendBatch(batch []*batchSpend) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()
	stat := gocore.NewStat("sendSpendBatchLua")

	ctx, _, deferFn := tracing.Tracer("aerospike").Start(s.ctx, "sendSpendBatchLua",
		tracing.WithParentStat(stat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)
	s.logSpendBatchStart(batchID, len(batch))

	batchesByKey, err := s.prepareSpendBatches(batch, batchID)
	if err != nil {
		return
	}

	batchRecords, batchRecordKeys := s.createBatchRecords(batchesByKey)

	if err := s.executeSpendBatch(batchRecords, batch, batchID); err != nil {
		return
	}

	s.processSpendBatchResults(ctx, batchRecords, batchRecordKeys, batchesByKey, batch, batchID)
	stat.NewStat("postBatchOperate").AddTime(start)
}

// logSpendBatchStart logs the start of a spend batch if verbose debug is enabled
func (s *Store) logSpendBatchStart(batchID uint64, batchSize int) {
	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchID, batchSize)
	}
}

// prepareSpendBatches groups spends by key and validates them
func (s *Store) prepareSpendBatches(batch []*batchSpend, batchID uint64) (map[keyIgnoreLocked][]aerospike.MapValue, error) {
	aeroKeyMap := make(map[string]*aerospike.Key)
	batchesByKey := make(map[keyIgnoreLocked][]aerospike.MapValue, len(batch))

	for idx, bItem := range batch {
		key, err := s.getOrCreateAerospikeKey(bItem, s.utxoBatchSize, aeroKeyMap)
		if err != nil {
			bItem.complete(err)
			continue
		}

		if err := s.validateSpendItem(bItem); err != nil {
			bItem.complete(err)
			continue
		}

		mapValue := s.createSpendMapValue(idx, bItem)
		useKey := keyIgnoreLocked{
			key:               key,
			hash:              bItem.spend.TxID,
			blockHeight:       bItem.blockHeight,
			ignoreConflicting: bItem.ignoreConflicting,
			ignoreLocked:      bItem.ignoreLocked,
		}

		batchesByKey[useKey] = append(batchesByKey[useKey], mapValue)
	}

	return batchesByKey, nil
}

// getOrCreateAerospikeKey gets or creates an Aerospike key for the spend
func (s *Store) getOrCreateAerospikeKey(bItem *batchSpend, utxoBatchSize int, keyMap map[string]*aerospike.Key) (*aerospike.Key, error) {
	keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout, utxoBatchSize)
	keySourceStr := string(keySource)

	if key, ok := keyMap[keySourceStr]; ok {
		return key, nil
	}

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		return nil, errors.NewProcessingError("[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend", bItem.spend.TxID.String(), err)
	}

	keyMap[keySourceStr] = key
	return key, nil
}

// validateSpendItem validates that the spend item has all required data
func (s *Store) validateSpendItem(bItem *batchSpend) error {
	if bItem.spend.SpendingData == nil {
		return errors.NewProcessingError("[SPEND_BATCH_LUA][%s] spending data is nil", bItem.spend.TxID.String())
	}
	return nil
}

// createSpendMapValue creates the map value for a spend item
func (s *Store) createSpendMapValue(idx int, bItem *batchSpend) aerospike.MapValue {
	return aerospike.NewMapValue(map[any]any{
		"idx":          idx,
		"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
		"vOut":         bItem.spend.Vout,
		"utxoHash":     bItem.spend.UTXOHash[:],
		"spendingData": bItem.spend.SpendingData.Bytes(),
	})
}

// createBatchRecords creates the batch records for Aerospike operations
func (s *Store) createBatchRecords(batchesByKey map[keyIgnoreLocked][]aerospike.MapValue) ([]aerospike.BatchRecordIfc, []keyIgnoreLocked) {
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batchesByKey))
	batchRecordKeys := make([]keyIgnoreLocked, 0, len(batchesByKey))
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for batchKey, batchItems := range batchesByKey {
		useLuaPackage := LuaPackage
		if s.settings.Aerospike.SeparateSpendUDFModuleCount > 0 {
			// determine which lua package to use for spends, based on the first byte of the tx id, there will be N packages (0 to N-1)
			useLuaPackage = s.spendLuaPackages[batchKey.hash[0]%uint8(s.settings.Aerospike.SeparateSpendUDFModuleCount)]
		}

		batchRecords = append(batchRecords, s.teranodeBatchRecord(batchUDFPolicy, useLuaPackage, batchKey.key, subOpSpendMulti, "spendMulti",
			batchItems,
			batchKey.ignoreConflicting,
			batchKey.ignoreLocked,
			batchKey.blockHeight,
			s.settings.GetUtxoStoreBlockHeightRetention(),
		))
		batchRecordKeys = append(batchRecordKeys, batchKey)
	}

	return batchRecords, batchRecordKeys
}

// executeSpendBatch executes the batch operation
func (s *Store) executeSpendBatch(batchRecords []aerospike.BatchRecordIfc, batch []*batchSpend, batchID uint64) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	err := s.batchOperate(batchPolicy, batchRecords)
	if err != nil {
		// complete is CAS-guarded: items already completed in prepareSpendBatches
		// (key-creation/validation failures) are simply no-ops here, so this
		// safely covers every remaining item without double-signalling.
		for idx, bItem := range batch {
			var sendErr error = errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d idx %d", bItem.spend.TxID.String(), batchID, idx, err)
			bItem.complete(sendErr)
		}
		return err
	}
	return nil
}

// processSpendBatchResults processes the results of the batch operation
func (s *Store) processSpendBatchResults(ctx context.Context, batchRecords []aerospike.BatchRecordIfc, batchRecordKeys []keyIgnoreLocked, batchesByKey map[keyIgnoreLocked][]aerospike.MapValue, batch []*batchSpend, batchID uint64) {
	for batchIdx, batchRecord := range batchRecords {
		key := batchRecordKeys[batchIdx]
		batchByKey, ok := batchesByKey[key]
		if !ok {
			s.logger.Errorf("[SPEND_BATCH_LUA] could not find batch key for batchIdx %d", batchIdx)
			continue
		}

		txID := batch[batchByKey[0]["idx"].(int)].spend.TxID
		s.processSingleBatchResult(ctx, batchRecord, batchByKey, batch, txID, key.blockHeight, batchID)
	}

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchID, len(batch))
	}
}

// processSingleBatchResult processes a single batch record result
func (s *Store) processSingleBatchResult(ctx context.Context, batchRecord aerospike.BatchRecordIfc, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	batchErr := batchRecord.BatchRec().Err
	if batchErr != nil {
		s.handleBatchError(batchByKey, batch, thisBlockHeight, batchID, batchErr)
		return
	}

	response := batchRecord.BatchRec().Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		s.handleMissingResponse(batchByKey, batch, txID)
		return
	}

	res, parseErr := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
	if parseErr != nil {
		s.handleParseError(batchByKey, batch, txID, parseErr)
		return
	}

	// Handle signals
	if res.Signal != "" {
		s.handleSpendSignal(ctx, res.Signal, txID, res.ChildCount, thisBlockHeight)
	}

	// Process based on status
	switch res.Status {
	case LuaStatusOK:
		s.handleSuccessfulSpends(batchByKey, batch)
	case LuaStatusError:
		s.handleErrorSpends(res, batchByKey, batch, txID, thisBlockHeight, batchID)
	}
}

// handleBatchError handles errors from batch operations
func (s *Store) handleBatchError(batchByKey []aerospike.MapValue, batch []*batchSpend, thisBlockHeight uint32, batchID uint64, err error) {
	s.demoteNativeOnUnsupported(err)

	// UPDATE_ONLY on the native path surfaces a missing parent record as a
	// per-record KEY_NOT_FOUND instead of the Lua TX_NOT_FOUND status. Map it
	// to the same TxNotFound error the UDF path produces via createGeneralError
	// so callers (catch-up sync, orphan handling) keep their error branching.
	var aErr *aerospike.AerospikeError
	if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
		for _, batchItem := range batchByKey {
			idx := batchItem["idx"].(int)
			batch[idx].complete(errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err))
		}
		return
	}

	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].complete(errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err))
	}
	// Only count infrastructure failures toward the circuit breaker.
	// Per-record data-state errors (e.g. KEY_NOT_FOUND_ERROR from missing
	// parents during catch-up sync) must not trip the breaker — issue #953.
	if s.spendCircuitBreaker != nil && isInfrastructureFailure(err) {
		s.spendCircuitBreaker.RecordFailure()
	}
}

// handleMissingResponse handles missing response from batch operation
func (s *Store) handleMissingResponse(batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].complete(errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String()))
	}
}

// handleParseError handles parse errors from response
func (s *Store) handleParseError(batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, err error) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].complete(errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String(), err))
	}
}

// handleSpendSignal handles signals from spend operations
func (s *Store) handleSpendSignal(ctx context.Context, signal LuaSignal, txID *chainhash.Hash, childCount int, thisBlockHeight uint32) {
	switch signal {
	case LuaSignalAllSpent:
		if err := s.handleExtraRecords(ctx, txID, 1, thisBlockHeight); err != nil {
			s.logger.Errorf("Failed to handle extra records: %v", err)
		}

	case LuaSignalDAHSet:
		// Only set DAH if BlockHeightRetention is configured (> 0)
		// When retention is 0, it means "don't use automatic retention"
		if dahHeight, ok := s.deleteAtHeightFor(thisBlockHeight); ok {
			if err := s.SetDAHForChildRecords(txID, childCount, dahHeight); err != nil {
				s.logger.Errorf("Failed to set DAH for child records: %v", err)
			}
			// External store DAH is disabled - lifecycle managed by pruner service
		}

	case LuaSignalDAHUnset:
		if err := s.SetDAHForChildRecords(txID, childCount, aerospike.TTLDontExpire); err != nil {
			s.logger.Errorf("Failed to unset DAH for child records: %v", err)
		}
		// External store DAH is disabled - lifecycle managed by pruner service
	}
}

// handleSuccessfulSpends handles successful spend operations
func (s *Store) handleSuccessfulSpends(batchByKey []aerospike.MapValue, batch []*batchSpend) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].complete(nil)
	}
	// Record successful batch operation for circuit breaker
	if s.spendCircuitBreaker != nil {
		s.spendCircuitBreaker.RecordSuccess()
	}
}

// handleErrorSpends handles error responses from spend operations
func (s *Store) handleErrorSpends(res *LuaMapResponse, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	if res.Message != "" {
		// General error for all spends
		generalErr := s.createGeneralError(res.ErrorCode, txID, thisBlockHeight, batchID, res.Message)
		for _, batchItem := range batchByKey {
			idx := batchItem["idx"].(int)
			batch[idx].complete(generalErr)
		}
	} else if res.Errors != nil {
		// Individual errors for specific spends
		s.handleIndividualErrors(res.Errors, batchByKey, batch, txID)
	} else {
		// ERROR status but no message or errors
		for _, batchItem := range batchByKey {
			idx := batchItem["idx"].(int)
			batch[idx].complete(errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %v", txID.String(), thisBlockHeight, batchID, res))
		}
	}
}

// createGeneralError creates a general error based on error code.
//
// The four verdict codes below (FROZEN, CONFLICTING, LOCKED, CREATING) are on
// errors.publicCauseCodes, so DeepestPublicCause surfaces their message verbatim
// to external HTTP/gRPC clients. They must therefore carry only data the
// submitter already knows — txid, block height and the fixed Lua message — and
// not batchID, which is this Store's process-lifetime spend-batch counter and
// would leak node uptime and throughput to anyone who submits two transactions.
// batchID is still carried on the arms below that produce non-allowlisted codes,
// where the public boundary collapses the message anyway.
func (s *Store) createGeneralError(errorCode LuaErrorCode, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64, message string) error {
	switch errorCode {
	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d - %s", txID.String(), thisBlockHeight, message)
	case LuaErrorCodeConflicting:
		return errors.NewTxConflictingError("[SPEND_BATCH_LUA][%s] transaction is conflicting, blockHeight %d - %s", txID.String(), thisBlockHeight, message)
	case LuaErrorCodeLocked:
		return errors.NewTxLockedError("[SPEND_BATCH_LUA][%s] transaction is locked, blockHeight %d - %s", txID.String(), thisBlockHeight, message)
	case LuaErrorCodeCreating:
		return errors.NewTxCreatingError("[SPEND_BATCH_LUA][%s] transaction is creating, blockHeight %d - %s", txID.String(), thisBlockHeight, message)
	case LuaErrorCodeCoinbaseImmature:
		return errors.NewTxCoinbaseImmatureError("[SPEND_BATCH_LUA][%s] coinbase is locked, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeTxNotFound:
		return errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	default:
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	}
}

// handleIndividualErrors handles individual errors for specific spends
func (s *Store) handleIndividualErrors(errors map[int]LuaErrorInfo, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)

		if errMsg, hasError := errors[idx]; hasError {
			batch[idx].complete(s.createSpendError(errMsg, batch[idx], txID))
		} else {
			batch[idx].complete(nil)
		}
	}
}

// createSpendError creates an error for a specific spend
func (s *Store) createSpendError(errMsg LuaErrorInfo, batchItem *batchSpend, txID *chainhash.Hash) error {
	// Guard against a nil batch item / spend before the branches below
	// dereference batchItem.spend (and txID). Return an error rather than
	// panicking; see TestCreateSpendErrorHandlesNilBatchItem.
	if batchItem == nil || batchItem.spend == nil {
		return errors.NewStorageError("[SPEND_BATCH_LUA] cannot create spend error for nil batch item: %s", errMsg.Message)
	}

	switch errMsg.ErrorCode {
	case LuaErrorCodeSpent:
		if errMsg.SpendingData != "" {
			spendingData, parseErr := spendpkg.NewSpendingDataFromString(errMsg.SpendingData)
			if parseErr != nil {
				return errors.NewStorageError("[SPEND_BATCH_LUA][%s] invalid spending data in error: %s", txID.String(), errMsg.SpendingData)
			}

			return errors.NewUtxoSpentError(*batchItem.spend.TxID, batchItem.spend.Vout, *batchItem.spend.UTXOHash, spendingData)
		}

		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] UTXO already spent but no spending data provided", txID.String())

	case LuaErrorCodeInvalidSpend:
		return errors.NewUtxoError("[SPEND_BATCH_LUA][%s] invalid spend for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] UTXO is frozen, vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozenUntil:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] UTXO frozen until block, vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoNotFound:
		return errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] UTXO not found for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoHashMismatch:
		return errors.NewUtxoHashMismatchError("[SPEND_BATCH_LUA][%s] UTXO hash mismatch for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoInvalidSize:
		return errors.NewUtxoInvalidSize("[SPEND_BATCH_LUA][%s] UTXO invalid size for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	default:
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] error for vout %d (code: %s): %s", txID.String(), batchItem.spend.Vout, errMsg.ErrorCode, errMsg.Message)
	}
}

// SetDAHForChildRecords sets DAH for all child records of a transaction. Every
// child is enqueued into the setDAH batcher under one shared completion group,
// and a single group.Wait replaces the previous per-child goroutine + timer +
// channel receive.
func (s *Store) SetDAHForChildRecords(txID *chainhash.Hash, childCount int, dah uint32) error {
	if childCount == 0 {
		return nil
	}

	items := make([]*batchDAH, childCount)
	group := completion.NewGroup(int32(childCount))

	for i := uint32(0); i < uint32(childCount); i++ { // nolint: gosec
		item := &batchDAH{
			txID:           txID,
			childIdx:       i + 1, // We want to set DAH for child record i+1
			deleteAtHeight: dah,
			group:          group,
		}
		items[i] = item

		// Per-item send, so only this child is rejected on a shutdown race.
		if enqueueErr := safeBatcherPut(s.setDAHBatcher, item, "setDAH"); enqueueErr != nil {
			item.complete(enqueueErr)
		}
	}

	// s.batcherWait <= 0 means unbounded (ctx-only) — Group.Wait treats a
	// non-positive timeout the same way, mirroring the previous per-child
	// fallback (which waited unbounded on errCh when batcherWait <= 0), while a
	// positive value still bounds the wait so a wedged batcher cannot pin the
	// caller forever.
	if waitErr := group.Wait(s.ctx, s.batcherWait); waitErr != nil {
		return errors.NewServiceUnavailableError("[setDAHForChildRecords][%s] set DAH for child records did not complete within %s: %s", txID.String(), s.batcherWait, waitErr)
	}

	// group.Wait returned nil: every child has completed, so every result slot
	// is safe to read. Report an aggregate failure if any child errored,
	// matching the previous behaviour.
	var errorsFound bool

	for _, item := range items {
		if item.result != nil {
			errorsFound = true

			s.logger.Errorf("[setDAHForChildRecords][%s] failed to set DAH for child record %d: %v", txID.String(), item.childIdx, item.result)
		}
	}

	if errorsFound {
		return errors.NewStorageError("[setDAHForChildRecords][%s] failed to set DAH for one or more child records", txID.String())
	}

	return nil
}

// handleExtraRecords manages the record count for paginated transactions when UTXOs are spent.
// This function is called when spending operations affect transactions with multiple records
// to maintain accurate pagination counts for cleanup operations.
//
// Parameters:
//   - ctx: Context for cancellation
//   - txID: Transaction ID whose record count needs updating
//   - increment: Amount to increment (can be negative for decrement)
//
// Returns:
//   - error: Any error encountered during the record count update
func (s *Store) handleExtraRecords(ctx context.Context, txID *chainhash.Hash, increment int, blockHeight uint32) error {
	res, err := s.IncrementSpentRecords(txID, increment, blockHeight) // This is a batch operation
	if err != nil {
		return err
	}

	// Parse the map response
	ret, err := s.ParseLuaMapResponse(res)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to parse LUA return value: %v", txID.String(), err)
		return err
	}

	if ret.Status == LuaStatusOK {
		if ret.Signal != "" {
			switch ret.Signal {
			case LuaSignalDAHSet:
				if err := s.handleDAHSetSignal(ctx, txID, ret.ChildCount, blockHeight); err != nil {
					return err
				}

			case LuaSignalDAHUnset:
				if err := s.SetDAHForChildRecords(txID, ret.ChildCount, 0); err != nil {
					return err
				}
				// External store DAH is disabled - lifecycle managed by pruner service
			}
		}
	} else if ret.Status == LuaStatusError {
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to handleExtraRecords: %v", txID.String(), ret.Message)
	}

	return nil
}

// handleDAHSetSignal applies a DAH_SET signal from incrementSpentExtraRecs:
// verify the children really are all spent (the spentExtraRecs counter can
// drift after interrupted rollbacks), clear the drifted master DAH when they
// are not, and otherwise propagate the DAH to the child records. Extracted
// verbatim from handleExtraRecords.
func (s *Store) handleDAHSetSignal(ctx context.Context, txID *chainhash.Hash, childCount int, blockHeight uint32) error {
	// Only set DAH if BlockHeightRetention is configured (> 0)
	// When retention is 0, it means "don't use automatic retention"
	dah, ok := s.deleteAtHeightFor(blockHeight)
	if !ok {
		return nil
	}

	// Sanity check: verify all children are actually spent before
	// setting DAH. The spentExtraRecs counter can drift due to
	// interrupted rollbacks, so we don't trust it blindly.
	if childCount > 0 {
		allSpent, verifyErr := s.verifyAllChildrenSpent(ctx, txID, childCount)
		if verifyErr != nil {
			s.logger.Errorf("[handleExtraRecords][%s] failed to verify children: %v", txID.String(), verifyErr)
			return verifyErr
		}

		if !allSpent {
			s.logger.Warnf("[handleExtraRecords][%s] spentExtraRecs triggered DAH but not all children are spent — counter drift detected, clearing master DAH", txID.String())
			// Lua already set DAH on the master record inline.
			// Clear it since children aren't actually all-spent, and
			// WAIT for the clear to be applied before returning — the
			// caller (and its tests) rely on the drifted DAH being
			// gone once handleExtraRecords returns. The error is
			// logged but not propagated (best-effort cleanup), which
			// matches the pre-conversion behaviour that waited on a
			// per-item errCh and logged without returning it.
			group := completion.NewGroup(1)
			item := &batchDAH{
				txID:           txID,
				childIdx:       0, // master record
				deleteAtHeight: 0, // clear DAH
				group:          group,
			}
			if enqueueErr := safeBatcherPutCtx(s.setDAHBatcher, ctx, item, "setDAH"); enqueueErr != nil {
				item.complete(enqueueErr)
			}

			if werr := group.Wait(ctx, s.batcherWait); werr != nil {
				s.logger.Errorf("[handleExtraRecords][%s] failed to clear drifted master DAH: %v", txID.String(), werr)
			} else if item.result != nil {
				s.logger.Errorf("[handleExtraRecords][%s] failed to clear drifted master DAH: %v", txID.String(), item.result)
			}

			return nil
		}
	}

	// External store DAH is disabled - lifecycle managed by pruner service
	return s.SetDAHForChildRecords(txID, childCount, dah)
}

// verifyAllChildrenSpent batch-reads all child records and checks if every
// child has spentUtxos == recordUtxos. Used as a sanity check before setting
// DAH — the spentExtraRecs counter can drift during interrupted rollbacks,
// so we verify the actual child state before trusting it.
func (s *Store) verifyAllChildrenSpent(ctx context.Context, txID *chainhash.Hash, childCount int) (bool, error) {
	if childCount == 0 {
		return true, nil
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	readPolicy := aerospike.NewBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, 0, childCount)

	for i := uint32(1); i <= uint32(childCount); i++ { // nolint: gosec
		keySource := uaerospike.CalculateKeySourceInternal(txID, i)
		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return false, errors.NewProcessingError("[verifyAllChildrenSpent][%s] failed to create key for child %d", txID.String(), i, err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchRead(
			readPolicy,
			key,
			[]string{fields.SpentUtxos.String(), fields.RecordUtxos.String()},
		))
	}

	if err := s.batchOperate(batchPolicy, batchRecords); err != nil {
		return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] batch read failed", txID.String(), err)
	}

	for i, br := range batchRecords {
		rec := br.BatchRec()
		if rec.Err != nil {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] child %d read failed", txID.String(), i+1, rec.Err)
		}
		if rec.Record == nil || rec.Record.Bins == nil {
			return false, nil
		}

		spentUtxos, ok := rec.Record.Bins[fields.SpentUtxos.String()].(int)
		if !ok {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] invalid type for spentUtxos in child %d", txID.String(), i+1)
		}
		recordUtxos, ok := rec.Record.Bins[fields.RecordUtxos.String()].(int)
		if !ok {
			return false, errors.NewStorageError("[verifyAllChildrenSpent][%s] invalid type for recordUtxos in child %d", txID.String(), i+1)
		}

		if spentUtxos != recordUtxos {
			return false, nil
		}
	}

	return true, nil
}

// IncrementSpentRecords updates the record count for paginated transactions.
// Used for cleanup management of large transactions.
func (s *Store) IncrementSpentRecords(txid *chainhash.Hash, increment int, blockHeight uint32) (interface{}, error) {
	group := completion.NewGroup(1)
	item := &batchIncrement{
		txID:        txid,
		increment:   increment,
		blockHeight: blockHeight,
		group:       group,
	}

	// Enqueue inline: Put is a non-blocking channel send, so the previous
	// goroutine wrapper (needed only to avoid blocking on a per-item channel)
	// is unnecessary under the group model.
	if enqueueErr := safeBatcherPut(s.incrementBatcher, item, "increment"); enqueueErr != nil {
		item.complete(nil, enqueueErr)
	}

	spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
	if spendTimeout <= 0 {
		spendTimeout = 30 * time.Second
	}

	// Bounded by spendTimeout (the previous timer budget) and additionally
	// cancellable by store shutdown via s.ctx — the only context available to
	// this ctx-less producer, matching SetDAHForChildRecords. The enqueued item
	// is still drained on Close (setDAH/increment drain after spend), so an early
	// s.ctx cancel does not drop the write.
	if waitErr := group.Wait(s.ctx, spendTimeout); waitErr != nil {
		if prometheusUtxoMapErrors != nil {
			prometheusUtxoMapErrors.WithLabelValues("IncrementSpentRecords", "BatchTimeout").Inc()
		}

		return nil, errors.NewServiceUnavailableError("[IncrementSpentRecords][%s] batch operation timed out after %s: %s", txid.String(), spendTimeout, waitErr)
	}

	// group.Wait returned nil: the single item has completed, so its result
	// slots are safe to read.
	return item.res, item.err
}

func (s *Store) sendIncrementBatch(batch []*batchIncrement) {
	// go-batcher recovers panics in this fn; re-complete every item on panic so a
	// crash cannot orphan the waiting submitters. complete is CAS-guarded, so this
	// never double-signals an item some earlier stage already completed.
	defer func() {
		signalBatchPanic(recover(), batch, "sendIncrementBatch", s.logger, func(it *batchIncrement, err error) {
			it.complete(nil, err)
		})
	}()

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	// Keep batchRecords index-aligned 1:1 with batch. A key-creation failure used
	// to skip the append, after which the result loop indexed batch[idx] with the
	// wrong position — signalling the wrong item and orphaning the tail. Use a
	// NOOP placeholder + handled[] guard instead so alignment is preserved.
	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))
	handled := make([]bool, len(batch))

	for i, item := range batch {
		aeroKey, err := aerospike.NewKey(s.namespace, s.setName, item.txID[:])
		if err != nil {
			item.complete(nil, errors.NewProcessingError("failed to init new aerospike key for txMeta", err))

			handled[i] = true
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		batchRecords[i] = s.teranodeBatchRecord(batchUDFPolicy, LuaPackage, aeroKey, subOpIncrementSpentExtraRec, "incrementSpentExtraRecs",
			item.increment,
			int(s.effectiveBlockHeight(item.blockHeight)),
			s.settings.GetUtxoStoreBlockHeightRetention(),
		)
	}

	// send the batch to aerospike
	if err := s.batchOperate(batchPolicy, batchRecords); err != nil {
		// complete is CAS-guarded: items already completed above (key-creation
		// failures, tracked via handled[]) are simply skipped, so this covers
		// every remaining item without double-signalling.
		for i, item := range batch {
			if handled[i] {
				continue
			}

			item.complete(nil, errors.NewStorageError("error in aerospike increment batch records", err))
		}

		return
	}

	// Process the batch records
	for idx, batchRecordIfc := range batchRecords {
		if handled[idx] {
			continue
		}

		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			s.demoteNativeOnUnsupported(batchRecord.Err)
			batch[idx].complete(nil, errors.NewStorageError("error in aerospike increment batch record", batchRecord.Err))
			continue
		}

		if batchRecord.Record == nil {
			batch[idx].complete(nil, errors.NewProcessingError("no record returned from Lua"))
			continue
		}

		// Get the raw response from Lua
		rawResponse := batchRecord.Record.Bins[LuaSuccess.String()]
		if rawResponse == nil {
			batch[idx].complete(nil, errors.NewProcessingError("no response from Lua"))
			continue
		}

		// Pass through the raw response - let the caller handle parsing
		batch[idx].complete(rawResponse, nil)
	}
}

func (s *Store) sendSetDAHBatch(batch []*batchDAH) {
	// go-batcher recovers panics in this fn; re-complete every item on panic so a
	// crash cannot orphan a waiting submitter. complete is CAS-guarded and
	// nil-group tolerant, so this never double-signals and safely no-ops for
	// fire-and-forget (nil-group) items.
	defer func() {
		signalBatchPanic(recover(), batch, "sendSetDAHBatch", s.logger, func(it *batchDAH, err error) {
			it.complete(err)
		})
	}()

	// Create batch records with individual TTLs
	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))
	handled := make([]bool, len(batch))
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	dahBinName := fields.DeleteAtHeight.String()
	unsetOp := aerospike.PutOp(aerospike.NewBin(dahBinName, nil))

	for i, b := range batch {
		keySource := uaerospike.CalculateKeySourceInternal(b.txID, b.childIdx)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			// Previously this only logged and continued, leaving batchRecords[i]
			// nil (→ nil-deref panic in the result loop) AND never completing the
			// item (→ the submitter blocked forever). Complete it and keep the
			// slot index-aligned with a NOOP placeholder.
			s.logger.Errorf("[SetDAHBatch][%s] failed to create key for pagination record %d: %v", b.txID.String(), b.childIdx, err)

			b.complete(errors.NewProcessingError("[SetDAHBatch] failed to create key", err))

			handled[i] = true
			batchRecords[i] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		if b.deleteAtHeight > 0 {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(dahBinName, b.deleteAtHeight)))
		} else {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, unsetOp)
		}
	}

	// Execute batch operation
	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		// complete is CAS-guarded: items already completed above (key-creation
		// failures, tracked via handled[]) are skipped, so this covers every
		// remaining item without double-signalling.
		for i, bItem := range batch {
			if handled[i] {
				continue
			}

			bItem.complete(errors.NewStorageError("[SetDAHBatch][%s] failed to set DAH", bItem.txID.String(), err))
		}

		return
	}

	// batchOperate may have no errors, but some of the records may have failed
	for batchIdx, batchRecord := range batchRecords {
		if handled[batchIdx] {
			continue
		}

		if recErr := batchRecord.BatchRec().Err; recErr != nil {
			batch[batchIdx].complete(recErr)
			continue
		}

		batch[batchIdx].complete(nil)
	}
}
