package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	particle_type "github.com/bsv-blockchain/aerospike-client-go/v8/types/particle_type"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

const (
	// rawBatchSize is how many transactions a handler collects before sending
	// them to the iterator's result channel. Partial batches are only sent when
	// the query ends, and there is one handler per node command, so this is
	// kept small.
	rawBatchSize = 4 * 1024

	// rawContextCheckPeriod is how often (in records) a handler polls ctx.
	rawContextCheckPeriod = 1024
)

// Bin names, resolved once; the handler compares raw names against them.
var (
	rawBinTxID         = fields.TxID.String()
	rawBinFee          = fields.Fee.String()
	rawBinSizeInBytes  = fields.SizeInBytes.String()
	rawBinCreatedAt    = fields.CreatedAt.String()
	rawBinConflicting  = fields.Conflicting.String()
	rawBinLocked       = fields.Locked.String()
	rawBinBlockIDs     = fields.BlockIDs.String()
	rawBinUnminedSince = fields.UnminedSince.String()
	rawBinIsCoinbase   = fields.IsCoinbase.String()
	rawBinExternal     = fields.External.String()
	rawBinInputs       = fields.Inputs.String()
)

// rawUnminedRecord holds the decoded bins of the record being delivered.
type rawUnminedRecord struct {
	txid         chainhash.Hash
	fee          uint64
	size         uint64
	createdAt    int
	unminedSince int

	// hasCreatedAt is set whenever the bin is present, whatever its type:
	// its absence is what marks a split record.
	hasTxID, hasFee, hasSize, hasCreatedAt bool

	conflicting, locked, coinbase, external bool

	// blockIDs and inputs are decoded with the client's generic decoder: they
	// are lists and are rare on this scan (partly-mined records, and inputs
	// only when inpoints are stored).
	blockIDs any
	inputs   any

	// Errors are kept per bin and reported in processRecord's order, with its
	// messages.
	txidErr, feeErr, createdAtErr, blockIDsErr, inputsErr error
}

// rawUnminedHandler turns records delivered inline by QueryPartitionsRaw into
// unmined transactions for block assembly, in the goroutine of the node command
// that read them, and sends them to the iterator in batches. One handler exists
// per node command.
type rawUnminedHandler struct {
	it  *unminedTxIterator
	set *rawHandlerSet
	ctx context.Context
	cur rawUnminedRecord
	buf []*utxo.UnminedTransaction

	// progress counts records delivered; busy is set while the handler is
	// blocked on something other than Aerospike (handing a batch to the
	// consumer, fetching inpoints from the external store). The stall
	// watchdog reads both.
	progress atomic.Uint64
	busy     atomic.Bool
}

func (it *unminedTxIterator) newRawUnminedHandler(ctx context.Context) *rawUnminedHandler {
	return &rawUnminedHandler{it: it, ctx: ctx}
}

func (h *rawUnminedHandler) BeginRecord(_ []byte, _, _ uint32) error {
	if h.progress.Add(1)%rawContextCheckPeriod == 0 {
		if err := h.ctx.Err(); err != nil {
			return err
		}
	}

	h.cur = rawUnminedRecord{}

	return nil
}

func (h *rawUnminedHandler) Bin(name []byte, particleType int, value []byte) error {
	r := &h.cur

	// switch on string(name) compiles to comparisons without allocating.
	switch string(name) {
	case rawBinTxID:
		r.hasTxID = true

		if particleType != particle_type.BLOB {
			r.txidErr = errors.NewProcessingError("txid not []byte")
			return nil
		}

		r.txidErr = r.txid.SetBytes(value)
	case rawBinFee:
		r.hasFee = true

		var ok bool
		if r.fee, ok = rawUint64(particleType, value); !ok {
			r.feeErr = errors.NewProcessingError("Failed to convert fee")
		}
	case rawBinSizeInBytes:
		// A size that doesn't convert is 0, as in extractTransactionData.
		r.hasSize = true
		r.size, _ = rawUint64(particleType, value)
	case rawBinCreatedAt:
		r.hasCreatedAt = true

		if particleType != particle_type.INTEGER || len(value) != 8 {
			r.createdAtErr = errors.NewProcessingError("%s not int64", rawBinCreatedAt)
			return nil
		}

		r.createdAt = int(int64(binary.BigEndian.Uint64(value))) //nolint:gosec // wire int64
	case rawBinUnminedSince:
		if particleType == particle_type.INTEGER && len(value) == 8 {
			r.unminedSince = int(int64(binary.BigEndian.Uint64(value))) //nolint:gosec // wire int64
		}
	case rawBinConflicting:
		r.conflicting = rawBool(particleType, value)
	case rawBinLocked:
		r.locked = rawBool(particleType, value)
	case rawBinIsCoinbase:
		r.coinbase = rawBool(particleType, value)
	case rawBinExternal:
		r.external = rawBool(particleType, value)
	case rawBinBlockIDs:
		r.blockIDs, r.blockIDsErr = as.DecodeParticle(particleType, value)
	case rawBinInputs:
		r.inputs, r.inputsErr = as.DecodeParticle(particleType, value)
	}

	return nil
}

// rawUint64 decodes an integer bin; other types go through the client decoder
// and toUint64, as the BinMap path does. ok is false when it doesn't convert.
func rawUint64(particleType int, value []byte) (uint64, bool) {
	if particleType == particle_type.INTEGER && len(value) == 8 {
		return binary.BigEndian.Uint64(value), true
	}

	decoded, err := as.DecodeParticle(particleType, value)
	if err != nil {
		return 0, false
	}

	v, convErr := toUint64(decoded)

	return v, convErr == nil
}

// rawBool is true only for a BOOL particle holding true, matching the BinMap
// path's .(bool) assertion.
func rawBool(particleType int, value []byte) bool {
	return particleType == particle_type.BOOL && len(value) > 0 && value[0] != 0
}

func (h *rawUnminedHandler) EndRecord() error {
	r := &h.cur

	// Split (paginated child) records carry no createdAt; their main record is
	// the one block assembly needs.
	if !r.hasCreatedAt {
		return nil
	}

	if r.conflicting {
		return nil
	}

	if r.coinbase {
		return h.add(&utxo.UnminedTransaction{Skip: true})
	}

	tx, err := h.buildTx()
	if err != nil {
		if h.set != nil {
			h.set.recordErr(err)
		}

		return err
	}

	return h.add(tx)
}

// buildTx mirrors processRecord for a raw record.
func (h *rawUnminedHandler) buildTx() (*utxo.UnminedTransaction, error) {
	r := &h.cur

	var dataErr error

	switch {
	case !r.hasTxID:
		dataErr = errors.NewProcessingError("txid not found")
	case r.txidErr != nil:
		dataErr = r.txidErr
	case !r.hasFee:
		dataErr = errors.NewProcessingError("fee not found")
	case r.feeErr != nil:
		dataErr = r.feeErr
	case !r.hasSize:
		dataErr = errors.NewProcessingError("size not found")
	}

	if dataErr != nil {
		return nil, errors.NewProcessingError("invalid transaction data", dataErr)
	}

	blockIDs := []uint32{}

	if r.blockIDsErr != nil {
		return nil, errors.NewProcessingError("invalid block IDs for %s", r.txid.String(), r.blockIDsErr)
	}

	if r.blockIDs != nil {
		var err error
		if blockIDs, err = processBlockIDs(as.BinMap{rawBinBlockIDs: r.blockIDs}); err != nil {
			return nil, errors.NewProcessingError("invalid block IDs for %s", r.txid.String(), err)
		}
	}

	// Inpoints get their own allocation: the subtree processor keeps the
	// pointer in its tx map for as long as the tx is held.
	txInpoints := &subtree.TxInpoints{}

	if h.it.store.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
		var err error

		switch {
		case r.external:
			h.busy.Store(true)
			*txInpoints, err = h.it.processExternalTransactionInpoints(h.ctx, &r.txid)
			h.busy.Store(false)
		case r.inputsErr != nil:
			err = r.inputsErr
		default:
			*txInpoints, err = h.it.processInternalTransactionInpoints(as.BinMap{rawBinInputs: r.inputs})
		}

		if err != nil {
			return nil, errors.NewProcessingError("failed to process transaction inpoints for %s", r.txid.String(), err)
		}
	}

	if r.createdAtErr != nil {
		return nil, errors.NewProcessingError("invalid createdAt for %s", r.txid.String(), r.createdAtErr)
	}

	rec := &unminedRecord{}
	rec.node = subtree.Node{Hash: r.txid, Fee: r.fee, SizeInBytes: r.size}
	rec.tx = utxo.UnminedTransaction{
		Node:         &rec.node,
		UnminedSince: r.unminedSince,
		TxInpoints:   txInpoints,
		CreatedAt:    r.createdAt,
		Locked:       r.locked,
		BlockIDs:     blockIDs,
	}

	return &rec.tx, nil
}

func (h *rawUnminedHandler) add(tx *utxo.UnminedTransaction) error {
	if h.buf == nil {
		h.buf = make([]*utxo.UnminedTransaction, 0, rawBatchSize)
	}

	h.buf = append(h.buf, tx)
	if len(h.buf) >= rawBatchSize {
		return h.flush()
	}

	return nil
}

func (h *rawUnminedHandler) DiscardRecord() {
	h.cur = rawUnminedRecord{}
}

// flush sends the collected transactions to the iterator.
func (h *rawUnminedHandler) flush() error {
	if len(h.buf) == 0 {
		return nil
	}

	batch := h.buf
	h.buf = nil

	h.busy.Store(true)
	defer h.busy.Store(false)

	select {
	case <-h.ctx.Done():
		return h.ctx.Err()
	case h.it.resultChan <- batch:
		return nil
	}
}

// rawHandlerSet creates handlers for QueryPartitionsRaw and flushes every one
// of them once the query returns.
type rawHandlerSet struct {
	it       *unminedTxIterator
	ctx      context.Context
	mu       sync.Mutex
	handlers []*rawUnminedHandler

	// firstErr is the first record-processing error, reported in preference
	// to a stall. stallReported is set once the watchdog has reported to the
	// iterator, after which queryRaw reports nothing more.
	errMu         sync.Mutex
	firstErr      error
	stallReported atomic.Bool
}

func (s *rawHandlerSet) newHandler() as.RawRecordHandler {
	h := s.it.newRawUnminedHandler(s.ctx)
	h.set = s

	s.mu.Lock()
	s.handlers = append(s.handlers, h)
	s.mu.Unlock()

	return h
}

// flushAll sends every handler's partial batch. The lock is not held while
// blocked on the consumer.
func (s *rawHandlerSet) flushAll() error {
	s.mu.Lock()
	handlers := append([]*rawUnminedHandler(nil), s.handlers...)
	s.mu.Unlock()

	for _, h := range handlers {
		if err := h.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (s *rawHandlerSet) recordErr(err error) {
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.errMu.Unlock()
}

func (s *rawHandlerSet) recordedErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()

	return s.firstErr
}

// report sends err to the iterator once, without blocking.
func (s *rawHandlerSet) report(err error) {
	if !s.stallReported.CompareAndSwap(false, true) {
		return
	}

	select {
	case s.it.errorChan <- err:
	default:
	}
}

// startWatch runs the stall watchdog and returns a function that stops it and
// waits for it to exit, after which it reports nothing.
func (s *rawHandlerSet) startWatch(ctx context.Context, cancel context.CancelCauseFunc, idle time.Duration) (stop func()) {
	watchCtx, stopWatch := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		s.watch(watchCtx, cancel, idle)
	}()

	return func() {
		stopWatch()
		<-done
	}
}

// rawScanStalledError is the watchdog's cancellation cause.
type rawScanStalledError struct {
	idle time.Duration
}

func (e *rawScanStalledError) Error() string {
	return fmt.Sprintf("Aerospike partition query stalled: no records received in %v", e.idle)
}

// watch cancels the scan when, for the idle timeout, no handler received a
// record and none was busy (blocked on the consumer or the external store):
// that time is not a dead connection, so it never counts. It reports the stall
// (or an earlier handler error) to the iterator itself before cancelling. It
// returns when ctx is done.
func (s *rawHandlerSet) watch(ctx context.Context, cancel context.CancelCauseFunc, idle time.Duration) {
	tick := idle / 4
	if tick <= 0 {
		tick = idle
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var lastTotal uint64

	lastChange := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			total, blocked := s.progress()
			if total != lastTotal || blocked {
				lastTotal = total
				lastChange = now

				continue
			}

			if now.Sub(lastChange) >= idle {
				if ctx.Err() != nil {
					return
				}

				stalled := &rawScanStalledError{idle: idle}

				// Report before cancelling: the query can stay blocked in a
				// socket read until SocketTimeout, so the iterator must not wait
				// for it to return.
				if err := s.recordedErr(); err != nil {
					s.report(err)
				} else {
					s.it.store.logger.Errorf("[partitionWorker] %v, aborting worker", stalled)
					s.report(errors.NewProcessingError("Aerospike partition query stalled: no records received in %v", idle))
				}

				cancel(stalled)

				return
			}
		}
	}
}

// progress sums the handlers' record counts and reports whether any handler
// is blocked on the consumer.
func (s *rawHandlerSet) progress() (total uint64, blocked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, h := range s.handlers {
		total += h.progress.Load()
		blocked = blocked || h.busy.Load()
	}

	return total, blocked
}
