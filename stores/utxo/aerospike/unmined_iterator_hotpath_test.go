package aerospike

import (
	"context"
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// unminedTestBins returns the bin set the default (block assembly) scan mode
// reads for one main record.
func unminedTestBins(i int) as.BinMap {
	var txid chainhash.Hash
	binary.LittleEndian.PutUint64(txid[:], uint64(i)+1)

	return as.BinMap{
		fields.TxID.String():         txid[:],
		fields.Fee.String():          100 + i,
		fields.SizeInBytes.String():  250,
		fields.CreatedAt.String():    1_700_000_000_000 + i,
		fields.UnminedSince.String(): 10,
	}
}

func newHotpathTestIterator(resultChanSize int, idle time.Duration) *unminedTxIterator {
	return &unminedTxIterator{
		store: &Store{
			logger:   ulogger.TestLogger{},
			settings: &settings.Settings{},
		},
		resultChan:       make(chan []*utxo.UnminedTransaction, resultChanSize),
		errorChan:        make(chan error, 1),
		queryIdleTimeout: idle,
	}
}

func collectResults(it *unminedTxIterator) []*utxo.UnminedTransaction {
	var out []*utxo.UnminedTransaction

	for {
		select {
		case batch := <-it.resultChan:
			out = append(out, batch...)
		default:
			return out
		}
	}
}

// The partition scan must not inherit the query policy's TotalTimeout: a full
// unmined scan of billions of records runs for hours, and the configured 30m
// cap killed it at exactly 30:00 on every attempt. Stall detection is left to
// SocketTimeout and the iterator's idle watchdog.
func Test_partitionScanPolicy_DropsTotalTimeout(t *testing.T) {
	base := as.NewQueryPolicy()
	base.TotalTimeout = 30 * time.Minute
	base.SocketTimeout = 25 * time.Minute
	base.MaxRetries = 3

	policy := partitionScanPolicy(base)

	require.Equal(t, time.Duration(0), policy.TotalTimeout)
	require.Equal(t, 25*time.Minute, policy.SocketTimeout)
	require.Equal(t, 3, policy.MaxRetries)
	require.True(t, policy.IncludeBinData)
	require.Equal(t, 512, policy.RecordQueueSize)

	// The shared base policy must not be mutated.
	require.Equal(t, 30*time.Minute, base.TotalTimeout)
}

func Test_processRecordset_DrainsBufferedRecords(t *testing.T) {
	const n = 5000

	it := newHotpathTestIterator(64, time.Second)

	results := make(chan *as.Result, n)
	for i := 0; i < n; i++ {
		results <- &as.Result{Record: &as.Record{Bins: unminedTestBins(i)}}
	}

	close(results)

	it.processRecordset(context.Background(), results)

	txs := collectResults(it)
	require.Len(t, txs, n)

	seen := make(map[chainhash.Hash]struct{}, n)
	for _, tx := range txs {
		require.NotNil(t, tx.Node)
		require.NotNil(t, tx.TxInpoints)
		seen[tx.Hash] = struct{}{}
	}

	require.Len(t, seen, n)
	require.Empty(t, it.errorChan)
}

// A connection that delivers records and then goes silent must still trip the
// idle watchdog; the fast path must not disable stall detection.
func Test_processRecordset_IdleTimeoutAfterRecords(t *testing.T) {
	it := newHotpathTestIterator(64, 100*time.Millisecond)

	results := make(chan *as.Result, 10)
	for i := 0; i < 10; i++ {
		results <- &as.Result{Record: &as.Record{Bins: unminedTestBins(i)}}
	}

	done := make(chan struct{})
	go func() {
		it.processRecordset(context.Background(), results)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processRecordset did not exit after the stream went idle")
	}

	select {
	case err := <-it.errorChan:
		require.ErrorContains(t, err, "Aerospike partition query stalled")
	default:
		t.Fatal("expected a stall error on errorChan")
	}

	require.Len(t, collectResults(it), 10)
}

func Test_processRecordset_ExitsOnCancelWhileWaiting(t *testing.T) {
	it := newHotpathTestIterator(64, time.Hour)

	results := make(chan *as.Result)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		it.processRecordset(ctx, results)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processRecordset did not exit after context cancel")
	}
}

// processRecord runs once per unmined record, billions of times on a large
// reload. Two allocations: the transaction+node pair, and the inpoints, which
// must stay separate because the subtree processor's tx map retains them.
func Test_processRecord_TwoAllocations(t *testing.T) {
	it := newHotpathTestIterator(1, time.Second)
	bins := unminedTestBins(1)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(1000, func() {
		tx, err := it.processRecord(ctx, bins)
		if err != nil || tx == nil {
			t.Fatal("processRecord failed")
		}
	})

	require.LessOrEqual(t, allocs, 2.0)
}

func BenchmarkProcessRecordset(b *testing.B) {
	it := newHotpathTestIterator(1024, time.Minute)
	recs := make([]*as.Result, 16*1024)

	for i := range recs {
		recs[i] = &as.Result{Record: &as.Record{Bins: unminedTestBins(i)}}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		results := make(chan *as.Result, len(recs))
		for _, r := range recs {
			results <- r
		}

		close(results)

		it.processRecordset(context.Background(), results)

		for len(it.resultChan) > 0 {
			<-it.resultChan
		}
	}

	b.ReportMetric(float64(b.N*len(recs))/b.Elapsed().Seconds(), "records/s")
}

// BenchmarkProcessRecordsetParallel reproduces the production shape: 32
// partition workers share one context and one result channel, each fed by its
// own producer through a 512-slot channel (the client's RecordQueueSize).
func BenchmarkProcessRecordsetParallel(b *testing.B) {
	const (
		workers   = 32
		perWorker = 64 * 1024
	)

	recs := make([]*as.Result, perWorker)
	for i := range recs {
		recs[i] = &as.Result{Record: &as.Record{Bins: unminedTestBins(i)}}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it := newHotpathTestIterator(workers*2, time.Minute)
		ctx, cancel := context.WithCancel(context.Background())

		drained := make(chan struct{})
		go func() {
			for range it.resultChan {
			}
			close(drained)
		}()

		for w := 0; w < workers; w++ {
			results := make(chan *as.Result, 512)

			go func() {
				for _, r := range recs {
					results <- r
				}
				close(results)
			}()

			it.wg.Add(1)
			go func() {
				defer it.wg.Done()
				it.processRecordset(ctx, results)
			}()
		}

		it.wg.Wait()
		close(it.resultChan)
		<-drained
		cancel()
	}

	b.ReportMetric(float64(b.N*workers*perWorker)/b.Elapsed().Seconds(), "records/s")
}

// The inpoints must not share an allocation with the transaction or its node:
// the subtree processor keeps the inpoints pointer in its tx map, which would
// otherwise pin the whole record for the lifetime of the tx.
func Test_processRecord_InpointsAllocatedSeparately(t *testing.T) {
	it := newHotpathTestIterator(1, time.Second)

	tx, err := it.processRecord(context.Background(), unminedTestBins(1))
	require.NoError(t, err)

	// tx must be the first field for its address to be the allocation's start.
	require.Zero(t, unsafe.Offsetof(unminedRecord{}.tx))

	txStart := uintptr(unsafe.Pointer(tx))
	txEnd := txStart + unsafe.Sizeof(unminedRecord{})
	inpoints := uintptr(unsafe.Pointer(tx.TxInpoints))

	require.False(t, inpoints >= txStart && inpoints < txEnd, "TxInpoints points inside the transaction's allocation")
}

// An idle-timer expiry that fired while the worker was busy on the fast path
// must not abort the next real wait: Reset has to discard it.
func Test_waitForRecord_IgnoresStaleExpiry(t *testing.T) {
	it := newHotpathTestIterator(1, 500*time.Millisecond)

	idleTimer := time.NewTimer(10 * time.Millisecond)
	defer idleTimer.Stop()

	time.Sleep(50 * time.Millisecond) // the timer expires unobserved

	results := make(chan *as.Result, 1)
	want := &as.Result{Record: &as.Record{Bins: unminedTestBins(1)}}

	go func() {
		time.Sleep(50 * time.Millisecond)
		results <- want
	}()

	rec, ok, abort := it.waitForRecord(context.Background(), results, idleTimer)
	require.False(t, abort, "stale expiry aborted the wait")
	require.True(t, ok)
	require.Same(t, want, rec)
	require.Empty(t, it.errorChan)
}
