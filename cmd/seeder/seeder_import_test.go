package seeder

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func testImportOptions() importOptions {
	return importOptions{
		workerCount:            256,
		multiRecordWorkerCount: 16,
		channelSize:            16,
		utxoBatchSize:          128,
	}
}

// Workers receive with a plain range over the channel (no select on
// ctx.Done), so cancellation relies on the reader closing the channel. A
// cancelled import must still return promptly with an error, never hang.
func TestImportUTXOSet_CancelledContextReturnsPromptly(t *testing.T) {
	path := writeCompleteSnapshotFile(t, benchWrappers("cancel", 10_000, 50))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)

	go func() {
		done <- importUTXOSet(ctx, ulogger.TestLogger{}, noopCreateStore{}, path, testImportOptions())
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("importUTXOSet did not return after context cancellation")
	}
}

// cancelOnFirstCreateStore waits until the reader has sent everything (the
// channel buffer holds the whole file), then cancels the import and returns
// success, so the remaining buffered records are only ever dropped.
type cancelOnFirstCreateStore struct {
	utxo.Store
	cancel context.CancelFunc
	once   sync.Once
}

func (s *cancelOnFirstCreateStore) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.once.Do(func() {
		time.Sleep(200 * time.Millisecond) // let the reader finish and close the channel
		s.cancel()
	})

	return nil, nil, nil
}

// A cancellation that arrives after the reader has finished must not be
// reported as success: buffered records were dropped, so the import is
// incomplete and lastProcessed.dat must not be written.
func TestImportUTXOSet_CancelAfterReaderFinishedIsNotSuccess(t *testing.T) {
	path := writeCompleteSnapshotFile(t, benchWrappers("drain", 500, 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := testImportOptions()
	opts.workerCount = 1
	opts.multiRecordWorkerCount = 1
	opts.channelSize = 1000

	err := importUTXOSet(ctx, ulogger.TestLogger{}, &cancelOnFirstCreateStore{cancel: cancel}, path, opts)
	require.ErrorIs(t, err, context.Canceled)
}

// Zero workers would leave the reader blocked forever on a full channel, and an
// absurd count (a typo'd extra digit) would spawn millions of goroutines whose
// stacks the GC then scans on every cycle.
func TestImportUTXOSet_RejectsOutOfRangeWorkerCounts(t *testing.T) {
	path := writeCompleteSnapshotFile(t, benchWrappers("zero", 10, 0))

	for _, mutate := range []func(*importOptions){
		func(o *importOptions) { o.workerCount = 0 },
		func(o *importOptions) { o.multiRecordWorkerCount = 0 },
		func(o *importOptions) { o.workerCount = maxWorkerCount + 1 },
		func(o *importOptions) { o.multiRecordWorkerCount = maxWorkerCount + 1 },
	} {
		opts := testImportOptions()
		mutate(&opts)

		require.Error(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, opts))
	}
}

// txidRecordingStore records every created txid and how often it was created.
type txidRecordingStore struct {
	utxo.Store
	mu   sync.Mutex
	seen map[chainhash.Hash]int
}

func (s *txidRecordingStore) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	o, err := utxo.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	s.seen[*o.TxID]++
	s.mu.Unlock()

	return nil, nil, nil
}

// Both passes together must create every record exactly once: single-record
// txs in one pass, multi-record txs in the other, none dropped or doubled.
func TestImportUTXOSet_ProcessesAllRecordsExactlyOnce(t *testing.T) {
	const n = 20_000

	wrappers := benchWrappers("all", n, 50)
	path := writeCompleteSnapshotFile(t, wrappers)

	store := &txidRecordingStore{seen: make(map[chainhash.Hash]int, n)}

	require.NoError(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, store, path, testImportOptions()))

	require.Len(t, store.seen, n)

	for _, w := range wrappers {
		require.Equal(t, 1, store.seen[w.TxID], "tx %s", w.TxID)
	}
}

// gatedStore holds every multi-record create until all single-record creates
// have been written. With one shared worker pool the workers pile up on the
// held multi-record txs and the single-record txs never finish (deadlock);
// with independent passes the single-record pass completes and releases them.
type gatedStore struct {
	utxo.Store
	singleRemaining atomic.Int64
	release         chan struct{}
	releaseOnce     sync.Once
}

func (s *gatedStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if len(tx.Outputs) > 128 {
		select {
		case <-s.release:
			return nil, nil, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	if s.singleRemaining.Add(-1) == 0 {
		s.releaseOnce.Do(func() { close(s.release) })
	}

	return nil, nil, nil
}

func TestImportUTXOSet_MultiRecordTxsDoNotStallSingleRecordTxs(t *testing.T) {
	const n = 20_000

	// One tx in 10 is multi-record: 2,000 of them, far more than the 256 workers.
	wrappers := benchWrappers("gated", n, 10)
	path := writeCompleteSnapshotFile(t, wrappers)

	store := &gatedStore{release: make(chan struct{})}

	for _, w := range wrappers {
		if !spansMultipleRecords(maxIndexOf(w), 128) {
			store.singleRemaining.Add(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, importUTXOSet(ctx, ulogger.TestLogger{}, store, path, testImportOptions()))
}

// blockSingleFailMultiStore blocks every single-record create until its
// context is cancelled, and fails every multi-record create.
type blockSingleFailMultiStore struct {
	utxo.Store
}

func (blockSingleFailMultiStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if len(tx.Outputs) > 128 {
		return nil, nil, errors.NewStorageError("simulated multi-record failure")
	}

	<-ctx.Done()

	return nil, nil, ctx.Err()
}

// The passes share one errgroup so a failure in either stops the other. Here
// the single-record pass can only finish by being cancelled; if the multi-record
// failure did not reach it, the import would hang.
func TestImportUTXOSet_FailureInOnePassStopsTheOther(t *testing.T) {
	path := writeCompleteSnapshotFile(t, benchWrappers("cross", 5_000, 50))

	done := make(chan error, 1)

	go func() {
		done <- importUTXOSet(context.Background(), ulogger.TestLogger{}, blockSingleFailMultiStore{}, path, testImportOptions())
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated multi-record failure")
	case <-time.After(10 * time.Second):
		t.Fatal("a failing multi-record pass did not stop the single-record pass")
	}
}

// failingMultiRecordStore fails every multi-record create.
type failingMultiRecordStore struct {
	utxo.Store
}

func (failingMultiRecordStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if len(tx.Outputs) > 128 {
		return nil, nil, errors.NewStorageError("simulated multi-record failure")
	}

	return nil, nil, nil
}

// A failure in the background multi-record pass must stop the whole import
// and surface as an error, not be lost behind the other pass succeeding.
func TestImportUTXOSet_MultiRecordFailureStopsImport(t *testing.T) {
	path := writeCompleteSnapshotFile(t, benchWrappers("fail", 20_000, 50))

	err := importUTXOSet(context.Background(), ulogger.TestLogger{}, failingMultiRecordStore{}, path, testImportOptions())
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated multi-record failure")
}

// The Aerospike store splits a tx into multiple records by its padded output
// count (highest unspent index + 1), not by how many outputs are unspent.
func TestSpansMultipleRecords(t *testing.T) {
	require.False(t, spansMultipleRecords(2, 128))
	require.False(t, spansMultipleRecords(127, 128), "index 127 still fits the first record")
	require.True(t, spansMultipleRecords(128, 128), "a single unspent output at index 128 needs a second record")
	require.True(t, spansMultipleRecords(5000, 128))
	require.False(t, spansMultipleRecords(5000, 0), "threshold 0 disables the check")
	require.False(t, spansMultipleRecords(0, 128), "a record without utxos reports max index 0")
}

// maxIndexOf is the highest output index in w, as ReadUTXOWrapperFrame reports it.
func maxIndexOf(w *utxopersister.UTXOWrapper) uint32 {
	var m uint32
	for _, u := range w.UTXOs {
		m = max(m, u.Index)
	}

	return m
}

// capturingStore records the transaction passed to SpendAndCreate.
type capturingStore struct {
	utxo.Store
	tx *bt.Tx
}

func (s *capturingStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.tx = tx

	return nil, nil, nil
}

// processUTXO places each unspent output at its own index and leaves spent
// positions nil, the layout PadUTXOsWithNil defines and the store keys by.
func TestProcessUTXO_OutputLayout(t *testing.T) {
	cases := map[string]struct {
		utxos []*utxopersister.UTXO
		want  []*utxopersister.UTXO // by output index, nil = spent
	}{
		"no utxos gives one nil output": {
			utxos: nil,
			want:  []*utxopersister.UTXO{nil},
		},
		"holes stay nil, order in the record does not matter": {
			utxos: []*utxopersister.UTXO{{Index: 3, Value: 30, Script: []byte{0x53}}, {Index: 0, Value: 10, Script: []byte{0x51}}},
			want:  []*utxopersister.UTXO{{Value: 10, Script: []byte{0x51}}, nil, nil, {Value: 30, Script: []byte{0x53}}},
		},
		"empty script": {
			utxos: []*utxopersister.UTXO{{Index: 1, Value: 5, Script: []byte{}}},
			want:  []*utxopersister.UTXO{nil, {Value: 5, Script: []byte{}}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			store := &capturingStore{}
			w := &utxopersister.UTXOWrapper{TxID: chainhash.HashH([]byte(name)), Height: 9, UTXOs: tc.utxos}

			require.NoError(t, processUTXO(context.Background(), store, w, nil, false))
			require.NotNil(t, store.tx)
			require.Len(t, store.tx.Outputs, len(tc.want))

			for i, want := range tc.want {
				got := store.tx.Outputs[i]
				if want == nil {
					require.Nil(t, got, "output %d", i)
					continue
				}

				require.NotNil(t, got, "output %d", i)
				require.Equal(t, want.Value, got.Satoshis, "output %d", i)
				require.Equal(t, want.Script, []byte(*got.LockingScript), "output %d", i)
			}
		})
	}
}

// An output index no consensus-valid tx can have must fail the import with an
// error, not crash a worker (0xFFFFFFFF wrapped maxIndex+1 to 0 and panicked)
// or attempt a multi-GiB outputs slice.
func TestImportUTXOSet_RejectsImpossibleOutputIndex(t *testing.T) {
	for _, index := range []uint32{utxopersister.MaxOutputIndex + 1, 0xFFFFFFFE, 0xFFFFFFFF} {
		wrappers := benchWrappers("bad-index", 10, 0)
		wrappers[5].UTXOs[0].Index = index

		path := writeCompleteSnapshotFile(t, wrappers)

		err := importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, testImportOptions())
		require.Error(t, err, "index %d", index)
		require.Contains(t, err.Error(), "output index", "index %d", index)
	}
}

// The largest index a consensus-valid tx can have is still accepted. Checked on
// the bound directly: importing it would build an ~888 MB outputs slice.
func TestCheckOutputIndex(t *testing.T) {
	require.NoError(t, checkOutputIndex(0))
	require.NoError(t, checkOutputIndex(utxopersister.MaxOutputIndex))
	require.Error(t, checkOutputIndex(utxopersister.MaxOutputIndex+1))
	require.Error(t, checkOutputIndex(0xFFFFFFFF))
}

// processUTXO sizes the outputs from the highest index itself, so it must
// reject an impossible index on its own, not rely on the reader having checked.
func TestProcessUTXO_RejectsImpossibleOutputIndex(t *testing.T) {
	w := &utxopersister.UTXOWrapper{
		TxID:   chainhash.HashH([]byte("impossible-index")),
		Height: 1,
		UTXOs:  []*utxopersister.UTXO{{Index: 0xFFFFFFFF, Value: 1, Script: []byte{0x51}}},
	}

	store := &capturingStore{}

	err := processUTXO(context.Background(), store, w, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output index")
	require.Nil(t, store.tx, "nothing must reach the store")
}
