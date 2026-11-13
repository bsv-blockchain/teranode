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
	"github.com/bsv-blockchain/go-bt/v2"
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

	// fastInputs is set when the inputs bin was read straight into the
	// handler's inpoints buffer. Otherwise inputs holds it decoded by the
	// client's generic decoder, as blockIDs always does.
	fastInputs bool

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

	// inpoints holds the current record's parent hash and output index per
	// input; inputs and inputPtrs carry them into subtree.NewTxInpointsFromInputs.
	// All three are reused from record to record.
	inpoints  []rawInpoint
	inputs    []bt.Input
	inputPtrs []*bt.Input

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
		if particleType == particle_type.LIST {
			if h.inpoints, r.fastInputs = appendRawInpoints(h.inpoints[:0], value); r.fastInputs {
				break
			}
		}

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

	// read in every mode: block assembly keeps each tx's parent links whatever
	// blockassembly_storeTxInpointsForSubtreeMeta says
	var err error

	switch {
	case r.external:
		h.busy.Store(true)
		*txInpoints, err = h.it.processExternalTransactionInpoints(h.ctx, &r.txid)
		h.busy.Store(false)
	case r.fastInputs:
		*txInpoints, err = h.txInpointsFromBuffer()
	case r.inputsErr != nil:
		err = r.inputsErr
	default:
		*txInpoints, err = h.it.processInternalTransactionInpoints(as.BinMap{rawBinInputs: r.inputs})
	}

	if err != nil {
		return nil, errors.NewProcessingError("failed to process transaction inpoints for %s", r.txid.String(), err)
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

// result is the scan's outcome once the query returned queryErr: that error,
// else a failure sending the handlers' partial batches, else a record
// processing error a handler recorded. The client should already have
// returned such an error; checking it here means a dropped one can't silently
// leave a tx out of the reload.
func (s *rawHandlerSet) result(queryErr error) error {
	if queryErr != nil {
		return queryErr
	}

	if err := s.flushAll(); err != nil {
		return err
	}

	return s.recordedErr()
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

// rawInpoint is one input's parent tx hash and the output index it spends.
type rawInpoint struct {
	hash chainhash.Hash
	vout uint32
}

// appendRawInpoints reads the parent hash and output index of every input in
// a LIST particle of extended inputs, the shape the store writes: a msgpack
// array of BLOB-typed byte strings. Nothing else is decoded and nothing is
// allocated once dst has grown. It reports false for any other shape (an
// ordered list's extension header, a non-BLOB element, an input shorter than
// its own lengths say), and the caller then falls back to the generic decoder,
// which reports the error the full parse always did.
func appendRawInpoints(dst []rawInpoint, b []byte) ([]rawInpoint, bool) {
	if len(b) == 0 {
		return dst, false
	}

	var (
		count int
		pos   int
	)

	switch t := b[0]; {
	case t >= 0x90 && t <= 0x9f:
		count, pos = int(t&0x0f), 1
	case t == 0xdc && len(b) >= 3:
		count, pos = int(binary.BigEndian.Uint16(b[1:3])), 3
	case t == 0xdd && len(b) >= 5:
		count, pos = int(binary.BigEndian.Uint32(b[1:5])), 5
	default:
		return dst, false
	}

	for i := 0; i < count; i++ {
		if pos >= len(b) {
			return dst, false
		}

		var size int

		switch t := b[pos]; {
		case t >= 0xa0 && t <= 0xbf:
			size, pos = int(t&0x1f), pos+1
		case t == 0xd9 && pos+2 <= len(b):
			size, pos = int(b[pos+1]), pos+2
		case t == 0xda && pos+3 <= len(b):
			size, pos = int(binary.BigEndian.Uint16(b[pos+1:pos+3])), pos+3
		case t == 0xdb && pos+5 <= len(b):
			size, pos = int(binary.BigEndian.Uint32(b[pos+1:pos+5])), pos+5
		default:
			return dst, false
		}

		if size < 1 || pos+size > len(b) || b[pos] != particle_type.BLOB {
			return dst, false
		}

		input := b[pos+1 : pos+size]
		pos += size

		if !extendedInputFits(input) {
			return dst, false
		}

		var in rawInpoint

		copy(in.hash[:], input[:32])
		in.vout = binary.LittleEndian.Uint32(input[32:36])
		dst = append(dst, in)
	}

	return dst, pos == len(b)
}

// extendedInputFits reports whether input holds a whole extended input:
// previous txid (32), output index (4), unlocking script with its varint
// length, sequence (4), previous satoshis (8) and the previous locking script
// with its varint length. These are the reads bt.Input.ReadFromExtended makes.
func extendedInputFits(input []byte) bool {
	pos := 36

	for _, fixedAfter := range [2]int{4 + 8, 0} {
		scriptLen, n, ok := rawVarInt(input, pos)
		if !ok {
			return false
		}

		pos += n

		if scriptLen > uint64(len(input)-pos) {
			return false
		}

		pos += int(scriptLen) + fixedAfter
		if pos > len(input) {
			return false
		}
	}

	return true
}

// rawVarInt reads the Bitcoin varint at b[pos:], returning its value and width.
func rawVarInt(b []byte, pos int) (uint64, int, bool) {
	if pos >= len(b) {
		return 0, 0, false
	}

	switch b[pos] {
	case 0xfd:
		if pos+3 > len(b) {
			return 0, 0, false
		}

		return uint64(binary.LittleEndian.Uint16(b[pos+1:])), 3, true
	case 0xfe:
		if pos+5 > len(b) {
			return 0, 0, false
		}

		return uint64(binary.LittleEndian.Uint32(b[pos+1:])), 5, true
	case 0xff:
		if pos+9 > len(b) {
			return 0, 0, false
		}

		return binary.LittleEndian.Uint64(b[pos+1:]), 9, true
	default:
		return uint64(b[pos]), 1, true
	}
}

// txInpointsFromBuffer builds the current record's parent links from the
// inpoints buffer through subtree.NewTxInpointsFromInputs, so parents are
// deduplicated exactly as on every other path. The inputs it passes are
// scratch: the constructor copies the hashes out, and allocates only the
// returned slices.
func (h *rawUnminedHandler) txInpointsFromBuffer() (subtree.TxInpoints, error) {
	n := len(h.inpoints)

	if cap(h.inputs) < n {
		h.inputs = make([]bt.Input, n)
		h.inputPtrs = make([]*bt.Input, n)
	}

	inputs := h.inputs[:n]
	ptrs := h.inputPtrs[:n]

	for i := range h.inpoints {
		inputs[i] = bt.Input{PreviousTxOutIndex: h.inpoints[i].vout}
		if err := inputs[i].PreviousTxIDAdd(&h.inpoints[i].hash); err != nil {
			return subtree.TxInpoints{}, err
		}

		ptrs[i] = &inputs[i]
	}

	return subtree.NewTxInpointsFromInputs(ptrs)
}
