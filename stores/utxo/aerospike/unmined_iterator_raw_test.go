package aerospike

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	particle_type "github.com/bsv-blockchain/aerospike-client-go/v8/types/particle_type"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

var _ as.RawRecordHandler = (*rawUnminedHandler)(nil)

func encInt(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))

	return b
}

func encBool(v bool) []byte {
	if v {
		return []byte{1}
	}

	return []byte{0}
}

type rawBin struct {
	name  string
	ptype int
	value []byte
}

func rawMainRecordBins(i int) []rawBin {
	var txid chainhash.Hash
	binary.LittleEndian.PutUint64(txid[:], uint64(i)+1)

	return []rawBin{
		{fields.TxID.String(), particle_type.BLOB, txid[:]},
		{fields.Fee.String(), particle_type.INTEGER, encInt(int64(100 + i))},
		{fields.SizeInBytes.String(), particle_type.INTEGER, encInt(250)},
		{fields.CreatedAt.String(), particle_type.INTEGER, encInt(1_700_000_000_000 + int64(i))},
		{fields.UnminedSince.String(), particle_type.INTEGER, encInt(10)},
	}
}

// feed delivers one record to h the way QueryPartitionsRaw does. The value
// slices are overwritten after each Bin call to prove nothing retains them.
func feed(t *testing.T, h *rawUnminedHandler, bins []rawBin) error {
	t.Helper()

	if err := h.BeginRecord(make([]byte, 20), 1, 0); err != nil {
		return err
	}

	for _, b := range bins {
		name := []byte(b.name)
		value := append([]byte(nil), b.value...)

		if err := h.Bin(name, b.ptype, value); err != nil {
			return err
		}

		for i := range value {
			value[i] = 0xEE
		}

		for i := range name {
			name[i] = 'x'
		}
	}

	return h.EndRecord()
}

func newRawTestHandler(t *testing.T) (*unminedTxIterator, *rawUnminedHandler) {
	t.Helper()

	it := newHotpathTestIterator(64, time.Second)

	return it, it.newRawUnminedHandler(context.Background())
}

func TestRawUnminedHandler_DecodesMainRecord(t *testing.T) {
	it, h := newRawTestHandler(t)

	bins := append(rawMainRecordBins(7), rawBin{fields.Locked.String(), particle_type.BOOL, encBool(true)})
	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())

	txs := collectResults(it)
	require.Len(t, txs, 1)

	tx := txs[0]
	var want chainhash.Hash
	binary.LittleEndian.PutUint64(want[:], 8)

	require.Equal(t, want, tx.Hash)
	require.Equal(t, uint64(107), tx.Fee)
	require.Equal(t, uint64(250), tx.SizeInBytes)
	require.Equal(t, 1_700_000_000_007, tx.CreatedAt)
	require.Equal(t, 10, tx.UnminedSince)
	require.True(t, tx.Locked)
	require.False(t, tx.Skip)
	require.NotNil(t, tx.TxInpoints)
	require.Empty(t, tx.BlockIDs)
}

// Paginated child records carry no createdAt and must not be yielded.
func TestRawUnminedHandler_SkipsSplitRecords(t *testing.T) {
	it, h := newRawTestHandler(t)

	var bins []rawBin
	for _, b := range rawMainRecordBins(1) {
		if b.name != fields.CreatedAt.String() {
			bins = append(bins, b)
		}
	}

	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())
	require.Empty(t, collectResults(it))
}

func TestRawUnminedHandler_SkipsConflicting(t *testing.T) {
	it, h := newRawTestHandler(t)

	bins := append(rawMainRecordBins(1), rawBin{fields.Conflicting.String(), particle_type.BOOL, encBool(true)})
	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())
	require.Empty(t, collectResults(it))
}

func TestRawUnminedHandler_CoinbaseYieldsSkip(t *testing.T) {
	it, h := newRawTestHandler(t)

	bins := append(rawMainRecordBins(1), rawBin{fields.IsCoinbase.String(), particle_type.BOOL, encBool(true)})
	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())

	txs := collectResults(it)
	require.Len(t, txs, 1)
	require.True(t, txs[0].Skip)
}

// Only the BOOL particle counts as true, exactly like the BinMap path's
// .(bool) assertion: an integer-encoded flag is ignored, not coerced.
func TestRawUnminedHandler_BoolParticleOnly(t *testing.T) {
	it, h := newRawTestHandler(t)

	bins := append(rawMainRecordBins(1),
		rawBin{fields.Locked.String(), particle_type.INTEGER, encInt(1)},
		rawBin{fields.Conflicting.String(), particle_type.INTEGER, encInt(1)},
	)
	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())

	txs := collectResults(it)
	require.Len(t, txs, 1)
	require.False(t, txs[0].Locked)
}

func withBin(bins []rawBin, name string, ptype int, value []byte) []rawBin {
	out := make([]rawBin, 0, len(bins))
	for _, b := range bins {
		if b.name == name {
			b.ptype, b.value = ptype, value
		}

		out = append(out, b)
	}

	return out
}

// A createdAt bin of the wrong type marks a main record the old path rejected;
// it must not be mistaken for a split record and silently dropped.
func TestRawUnminedHandler_WrongTypeCreatedAtIsAnError(t *testing.T) {
	_, h := newRawTestHandler(t)

	bins := withBin(rawMainRecordBins(1), fields.CreatedAt.String(), particle_type.BLOB, []byte("x"))
	require.ErrorContains(t, feed(t, h, bins), "not int64")
}

func TestRawUnminedHandler_UnconvertibleFee(t *testing.T) {
	_, h := newRawTestHandler(t)

	bins := withBin(rawMainRecordBins(1), fields.Fee.String(), particle_type.BLOB, []byte("x"))
	require.ErrorContains(t, feed(t, h, bins), "Failed to convert fee")
}

// The old path ignored a size conversion failure and used 0.
func TestRawUnminedHandler_UnconvertibleSizeIsZero(t *testing.T) {
	it, h := newRawTestHandler(t)

	bins := withBin(rawMainRecordBins(1), fields.SizeInBytes.String(), particle_type.BLOB, []byte("x"))
	require.NoError(t, feed(t, h, bins))
	require.NoError(t, h.flush())

	txs := collectResults(it)
	require.Len(t, txs, 1)
	require.Zero(t, txs[0].SizeInBytes)
}

func TestRawUnminedHandler_MissingFeeIsAnError(t *testing.T) {
	_, h := newRawTestHandler(t)

	var bins []rawBin
	for _, b := range rawMainRecordBins(1) {
		if b.name != fields.Fee.String() {
			bins = append(bins, b)
		}
	}

	require.ErrorContains(t, feed(t, h, bins), "fee not found")
}

// A discarded record must not leak its fields into the next one.
func TestRawUnminedHandler_DiscardResetsState(t *testing.T) {
	it, h := newRawTestHandler(t)

	require.NoError(t, h.BeginRecord(make([]byte, 20), 1, 0))
	require.NoError(t, h.Bin([]byte(fields.Locked.String()), particle_type.BOOL, encBool(true)))
	require.NoError(t, h.Bin([]byte(fields.Conflicting.String()), particle_type.BOOL, encBool(true)))
	h.DiscardRecord()

	require.NoError(t, feed(t, h, rawMainRecordBins(2)))
	require.NoError(t, h.flush())

	txs := collectResults(it)
	require.Len(t, txs, 1)
	require.False(t, txs[0].Locked)
}

func TestRawUnminedHandler_FlushesFullBatches(t *testing.T) {
	it := newHotpathTestIterator(8, time.Second)
	h := it.newRawUnminedHandler(context.Background())

	n := rawBatchSize + 10
	for i := 0; i < n; i++ {
		require.NoError(t, feed(t, h, rawMainRecordBins(i)))
	}

	require.Len(t, it.resultChan, 1, "a full batch must be sent without waiting for the end")
	require.NoError(t, h.flush())
	require.Len(t, collectResults(it), n)
}

func TestRawUnminedHandler_StopsOnCancel(t *testing.T) {
	it := newHotpathTestIterator(8, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := it.newRawUnminedHandler(ctx)

	var err error
	for i := 0; i < 2*rawContextCheckPeriod && err == nil; i++ {
		err = feed(t, h, rawMainRecordBins(i))
	}

	require.ErrorIs(t, err, context.Canceled)
}

// The raw path exists to take per-record allocation off the scan: a record
// may cost the transaction+node pair and its inpoints, nothing else.
func TestRawUnminedHandler_TwoAllocationsPerRecord(t *testing.T) {
	it := newHotpathTestIterator(1024, time.Second)
	h := it.newRawUnminedHandler(context.Background())

	bins := rawMainRecordBins(3)
	names := make([][]byte, len(bins))

	for i, b := range bins {
		names[i] = []byte(b.name)
	}

	digest := make([]byte, 20)

	allocs := testing.AllocsPerRun(1000, func() {
		_ = h.BeginRecord(digest, 1, 0)
		for i, b := range bins {
			_ = h.Bin(names[i], b.ptype, b.value)
		}

		_ = h.EndRecord()
		h.buf = h.buf[:0]
	})

	require.LessOrEqual(t, allocs, 2.0)
}

func newWatchdogSet(it *unminedTxIterator, n int) *rawHandlerSet {
	set := &rawHandlerSet{it: it, ctx: context.Background()}
	for i := 0; i < n; i++ {
		set.newHandler()
	}

	return set
}

// No record anywhere for the idle timeout, and nobody waiting on block
// assembly: the scan is stalled and must be cancelled with the stall error.
func TestRawWatchdog_CancelsWhenNothingProgresses(t *testing.T) {
	it := newHotpathTestIterator(8, 100*time.Millisecond)
	set := newWatchdogSet(it, 3)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go set.watch(ctx, cancel, it.queryIdleTimeout)

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not cancel a stalled scan")
	}

	require.ErrorContains(t, context.Cause(ctx), "Aerospike partition query stalled")

	// The stall must reach Next right away: the query itself may stay blocked
	// in a socket read until SocketTimeout.
	select {
	case err := <-it.errorChan:
		require.ErrorContains(t, err, "Aerospike partition query stalled")
	default:
		t.Fatal("stall was not reported to the iterator")
	}

	require.True(t, set.stallReported.Load())
}

// When a handler already failed, that error is the one to report, not a stall
// caused by another node still blocked in a read.
func TestRawWatchdog_ReportsHandlerErrorOverStall(t *testing.T) {
	it := newHotpathTestIterator(8, 100*time.Millisecond)
	set := newWatchdogSet(it, 2)

	realErr := errors.NewProcessingError("invalid transaction data")
	set.recordErr(realErr)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go set.watch(ctx, cancel, it.queryIdleTimeout)

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not fire")
	}

	select {
	case err := <-it.errorChan:
		require.ErrorIs(t, err, realErr)
	default:
		t.Fatal("nothing reported to the iterator")
	}
}

// stopWatch must make sure no stall is reported afterwards, e.g. while the
// final flush waits on the consumer.
func TestRawWatchdog_StopPreventsLateReports(t *testing.T) {
	it := newHotpathTestIterator(8, 50*time.Millisecond)
	set := newWatchdogSet(it, 1)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	stop := set.startWatch(ctx, cancel, it.queryIdleTimeout)
	stop()

	time.Sleep(200 * time.Millisecond)

	require.NoError(t, ctx.Err())
	require.Empty(t, it.errorChan)
}

// A handler blocked handing a batch to block assembly is backpressure, not a
// dead connection: the watchdog must not fire however long that takes.
func TestRawWatchdog_IgnoresTimeBlockedOnConsumer(t *testing.T) {
	it := newHotpathTestIterator(8, 100*time.Millisecond)
	set := newWatchdogSet(it, 2)
	set.handlers[0].busy.Store(true)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go set.watch(ctx, cancel, it.queryIdleTimeout)

	select {
	case <-ctx.Done():
		t.Fatalf("watchdog fired while a handler was blocked on the consumer: %v", context.Cause(ctx))
	case <-time.After(500 * time.Millisecond):
	}
}

func TestRawWatchdog_IgnoresProgressingScan(t *testing.T) {
	it := newHotpathTestIterator(8, 100*time.Millisecond)
	set := newWatchdogSet(it, 2)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go set.watch(ctx, cancel, it.queryIdleTimeout)

	deadline := time.After(500 * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("watchdog fired on a progressing scan: %v", context.Cause(ctx))
		case <-deadline:
			return
		case <-time.After(20 * time.Millisecond):
			set.handlers[1].progress.Add(1)
		}
	}
}

// A worker error must reach the consumer while other workers are still running
// (e.g. the last worker's query blocked in a socket read after its stall was
// reported), not only after every worker has finished.
func TestNext_ReturnsWorkerErrorWhileResultsStillOpen(t *testing.T) {
	it := newHotpathTestIterator(8, time.Second)

	go func() {
		time.Sleep(50 * time.Millisecond)
		it.errorChan <- errors.NewProcessingError("Aerospike partition query stalled: no records received in 1m0s")
	}()

	done := make(chan error, 1)
	go func() {
		_, err := it.Next(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "Aerospike partition query stalled")
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not return the worker error while resultChan was open")
	}
}

// After every worker finished, both channels are closed: Next must end the
// iteration cleanly instead of spinning on the closed error channel.
func TestNext_EndsCleanlyWhenAllChannelsClosed(t *testing.T) {
	for i := 0; i < 100; i++ {
		it := newHotpathTestIterator(1, time.Second)
		close(it.errorChan)
		close(it.resultChan)

		batch, err := it.Next(context.Background())
		require.NoError(t, err)
		require.Nil(t, batch)
	}
}
