package propagation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// dummyTx builds a positionally-distinct, recognisable *bt.Tx for tests.
// The hash bytes are derived from seed so test assertions can identify
// which tx came back.
func dummyTx(t testing.TB, seed byte) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	in := &bt.Input{}
	h := chainhash.Hash{seed, seed, seed, seed}
	require.NoError(t, in.PreviousTxIDAdd(&h))
	tx.Inputs = append(tx.Inputs, in)
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
	return tx
}

// newCoalescerWithStubFlush constructs a TxCoalescer where the flush
// function is provided by the test directly, bypassing the real
// validator. Used to exercise Submit/gather/dispatch mechanics in
// isolation.
func newCoalescerWithStubFlush(t testing.TB, maxSize int, maxWait time.Duration, maxConcurrent int, flush func(items []*pendingTx)) *TxCoalescer {
	t.Helper()
	logger := ulogger.TestLogger{}
	c := newTxCoalescerForTest(logger, maxSize, maxWait, maxConcurrent, true, flush)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestTxCoalescer_Submit_SingleTxRoundTrip(t *testing.T) {
	flush := func(items []*pendingTx) {
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 5*time.Millisecond, 0, flush)

	tx := dummyTx(t, 0xAA)
	res, err := c.Submit(context.Background(), tx, 0)
	require.NoError(t, err)
	require.NoError(t, res.Err)
	require.Equal(t, *tx.TxIDChainHash(), res.TxHash)
}

// With drain mode enabled the batcher fires immediately on first
// arrival, so an N-item burst under no concurrency cap can produce
// anywhere from 1 to N small batches depending on goroutine scheduling.
// The correctness invariant is that every item is processed exactly
// once and at least one batch fires — the precise gather size is a
// throughput/latency tradeoff exercised by the bench, not the unit
// test.
func TestTxCoalescer_Submit_NConcurrentAllItemsProcessed(t *testing.T) {
	const N = 16
	var (
		flushedCount atomic.Int32
		seenItems    atomic.Int32
	)
	flush := func(items []*pendingTx) {
		flushedCount.Add(1)
		seenItems.Add(int32(len(items)))
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 100*time.Millisecond, 0, flush)

	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := dummyTx(t, byte(i+1))
			r, err := c.Submit(context.Background(), tx, 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()

	require.Equal(t, int32(N), seenItems.Load(), "every submitted item must reach the flush handler")
	require.GreaterOrEqual(t, flushedCount.Load(), int32(1), "at least one flush must occur")
	require.LessOrEqual(t, flushedCount.Load(), int32(N), "no more flushes than items")
}

func TestTxCoalescer_Submit_SizeThresholdTriggersFlush(t *testing.T) {
	const N = 5
	flushCh := make(chan int, 10)
	flush := func(items []*pendingTx) {
		flushCh <- len(items)
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 2, 50*time.Millisecond, 0, flush)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	close(flushCh)
	total := 0
	maxSeen := 0
	for s := range flushCh {
		total += s
		if s > maxSeen {
			maxSeen = s
		}
	}
	require.Equal(t, N, total)
	// With drain mode each batch can be smaller than maxSize; the
	// invariant is that no batch ever EXCEEDS the size cap.
	require.LessOrEqual(t, maxSeen, 2, "no flush should exceed maxSize=2")
	require.GreaterOrEqual(t, maxSeen, 1, "every flush must contain at least one item")
}

// With drain mode enabled by default the batcher fires immediately on
// first arrival rather than waiting for maxWait. A single Submit should
// therefore complete substantially faster than the maxWait timeout.
func TestTxCoalescer_Submit_DrainModeFiresImmediately(t *testing.T) {
	flushCh := make(chan int, 10)
	flush := func(items []*pendingTx) {
		flushCh <- len(items)
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 500*time.Millisecond, 0, flush)

	start := time.Now()
	_, err := c.Submit(context.Background(), dummyTx(t, 0xCC), 0)
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.Less(t, elapsed, 100*time.Millisecond,
		"with drain mode, Submit should fire well before the 500ms maxWait")
}

func TestTxCoalescer_Submit_CtxCancelReturnsEarly(t *testing.T) {
	flushed := make(chan struct{}, 1)
	flush := func(items []*pendingTx) {
		flushed <- struct{}{}
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash()}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, time.Hour, 0, flush)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Submit(ctx, dummyTx(t, 0xDD), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTxCoalescer_Submit_WholeBatchErrFannedToAllCallers(t *testing.T) {
	errBatch := terrors.NewProcessingError("upstream unreachable")
	flush := func(items []*pendingTx) {
		for _, p := range items {
			p.Done <- validator.ValidationResult{TxHash: *p.Tx.TxIDChainHash(), Err: errBatch}
		}
	}
	c := newCoalescerWithStubFlush(t, 1024, 5*time.Millisecond, 0, flush)

	const N = 4
	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()
	for i, r := range results {
		require.ErrorIs(t, r.Err, errBatch, "index %d", i)
	}
}

// fakeValidator implements only the subset of validator.Interface that
// TxCoalescer calls: ValidateBatch. Other methods panic to surface
// accidental dependencies.
type fakeValidator struct {
	mu        sync.Mutex
	calls     int
	lastSize  int
	wholeErr  error
	perTxErrs map[chainhash.Hash]error
}

func (f *fakeValidator) Health(context.Context, bool) (int, string, error) { return 200, "ok", nil }

// Validate is reachable via the single-tx bypass path inside TxCoalescer
// when no other Submit is in flight. Mirror the per-tx error behaviour
// of ValidateBatch so tests can drive either code path with the same
// fixture (perTxErrs map).
func (f *fakeValidator) Validate(_ context.Context, tx *bt.Tx, _ uint32, _ ...validator.Option) (*meta.Data, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSize = 1
	if f.wholeErr != nil {
		return nil, f.wholeErr
	}
	if err, ok := f.perTxErrs[*tx.TxIDChainHash()]; ok && err != nil {
		return nil, err
	}
	return nil, nil
}

func (f *fakeValidator) ValidateWithOptions(context.Context, *bt.Tx, uint32, *validator.Options) (*meta.Data, error) {
	panic("TxCoalescer must not call ValidateWithOptions; only Validate or ValidateBatch")
}

func (f *fakeValidator) ValidateBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, _ ...validator.Option) ([]validator.ValidationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSize = len(txs)
	if f.wholeErr != nil {
		return nil, f.wholeErr
	}
	results := make([]validator.ValidationResult, len(txs))
	for i, tx := range txs {
		h := *tx.TxIDChainHash()
		results[i] = validator.ValidationResult{TxHash: h, Err: f.perTxErrs[h]}
	}
	return results, nil
}

func (f *fakeValidator) GetBlockHeight() uint32                        { return 0 }
func (f *fakeValidator) GetMedianBlockTime() uint32                    { return 0 }
func (f *fakeValidator) TriggerBatcher()                               {}
func (f *fakeValidator) EnsureMTPLoaded(context.Context, uint32) error { return nil }

func TestTxCoalescer_RealFlush_HappyPath(t *testing.T) {
	fv := &fakeValidator{}
	logger := ulogger.TestLogger{}
	c := NewTxCoalescer(context.Background(), logger, fv, 1024, 5*time.Millisecond, 0, true)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	const N = 8
	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()

	require.GreaterOrEqual(t, fv.calls, 1, "ValidateBatch must be called at least once")
	for i, r := range results {
		require.NoError(t, r.Err, "index %d", i)
	}
}

func TestTxCoalescer_RealFlush_PerTxErrorIsolated(t *testing.T) {
	fv := &fakeValidator{perTxErrs: map[chainhash.Hash]error{}}
	logger := ulogger.TestLogger{}
	c := NewTxCoalescer(context.Background(), logger, fv, 1024, 5*time.Millisecond, 0, true)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	good := dummyTx(t, 0x10)
	bad := dummyTx(t, 0x11)
	fv.perTxErrs[*bad.TxIDChainHash()] = terrors.NewProcessingError("per-tx fail")

	var wg sync.WaitGroup
	results := make([]validator.ValidationResult, 2)
	for i, tx := range []*bt.Tx{good, bad} {
		i, tx := i, tx
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Submit(context.Background(), tx, 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()

	require.NoError(t, results[0].Err)
	require.Error(t, results[1].Err)
}

func TestTxCoalescer_RealFlush_WholeBatchErr(t *testing.T) {
	fv := &fakeValidator{wholeErr: terrors.NewServiceError("aerospike unreachable")}
	logger := ulogger.TestLogger{}
	c := NewTxCoalescer(context.Background(), logger, fv, 1024, 5*time.Millisecond, 0, true)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	const N = 4
	results := make([]validator.ValidationResult, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Submit(context.Background(), dummyTx(t, byte(i+1)), 0)
			require.NoError(t, err)
			results[i] = r
		}()
	}
	wg.Wait()
	for i, r := range results {
		require.Error(t, r.Err, "index %d", i)
	}
}
