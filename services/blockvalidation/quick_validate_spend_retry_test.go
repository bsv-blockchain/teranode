package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spendRetrySpyStore wraps NullStore; spend-phase SpendAndCreate behaviour is
// scripted per tx hash, while create-phase calls fall through to NullStore so
// the create leg of createAndSpendUTXOsForBatch stays exercised.
type spendRetrySpyStore struct {
	*nullstore.NullStore
	mu sync.Mutex
	// failuresLeft[txid] = how many times a spend-phase call should fail with failErr[txid]
	failuresLeft map[chainhash.Hash]int
	failErr      map[chainhash.Hash]error
	spendCalls   atomic.Int64
}

func (s *spendRetrySpyStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options, err := utxo.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	if options.CreateOnly {
		return s.NullStore.SpendAndCreate(ctx, tx, blockHeight, opts...)
	}

	s.spendCalls.Add(1)
	h := *tx.TxIDChainHash()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.failuresLeft[h]; ok && n > 0 {
		s.failuresLeft[h] = n - 1
		return nil, nil, s.failErr[h]
	}
	return nil, nil, nil
}

func newSpendRetryHarness(t *testing.T, spy *spendRetrySpyStore) (*BlockValidation, *model.Block, []*bt.Tx) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	u := &BlockValidation{
		logger:            ulogger.TestLogger{},
		settings:          tSettings,
		utxoStore:         spy,
		spendRetryBackoff: time.Millisecond,
	}

	// three distinct txs (coinbase-style, distinct via locktime)
	txs := make([]*bt.Tx, 3)
	for i := range txs {
		tx := bt.NewTx()
		tx.LockTime = uint32(i + 1)
		txs[i] = tx
	}

	block := &model.Block{Height: 100, Header: model.GenesisBlockHeader}

	return u, block, txs
}

// A hard failure must fail the wave without tearing down the store calls already running
// beside it. errgroup.WithContext cancels its context on the first non-nil return, and that
// context is the one the store call runs under; SpendAndCreate is not atomic on any backend,
// so a cancel landing between its spend and its create is how a transaction ends up with its
// inputs spent and no row of its own. The goroutines therefore record their outcome and return
// nil, and this pins that: a sibling that is mid-call when another item hard-fails still sees a
// live context and still completes, and the wave still returns the hard failure.
func TestApplyTxsWithRetry_HardFailDoesNotCancelSiblingsMidCall(t *testing.T) {
	spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
	u, block, txs := newSpendRetryHarness(t, spy)

	items := make([]txApply, len(txs))
	for i, tx := range txs {
		items[i] = txApply{tx: tx, subtreeIdx: 0}
	}

	failing := *txs[0].TxIDChainHash()

	// Cancellations come back over a buffered channel rather than a shared variable: every
	// sibling would write it at once if the guard ever regressed, which is a data race in the
	// very run that is supposed to report the regression.
	var (
		siblingStarted = make(chan struct{})
		failed         = make(chan struct{})
		siblingCtxErrs = make(chan error, len(txs))
		siblingDone    atomic.Int64
		startOnce      sync.Once
	)

	err := u.applyTxsWithRetry(context.Background(), block, "test", items, 3, nil,
		func(ctx context.Context, item txApply) error {
			if *item.tx.TxIDChainHash() == failing {
				// Wait until a sibling is genuinely inside its call, so the cancel this test
				// is looking for would land mid-call rather than before one starts.
				<-siblingStarted
				close(failed)

				return errors.NewTxInvalidError("hard fail")
			}

			startOnce.Do(func() { close(siblingStarted) })
			<-failed

			// The sibling is mid-call at the instant the hard failure is recorded. Give the
			// cancellation a real chance to arrive rather than racing it: if the failing
			// goroutine returned its error to the errgroup, applyCtx is cancelled within
			// microseconds and Done fires long before this timer.
			select {
			case <-ctx.Done():
				siblingCtxErrs <- ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}

			siblingDone.Add(1)

			return nil
		})

	close(siblingCtxErrs)

	require.Error(t, err, "the wave fails on the hard error")
	require.Empty(t, siblingCtxErrs, "a hard failure must not cancel a sibling that is mid store call")
	require.Equal(t, int64(len(txs)-1), siblingDone.Load(), "every sibling ran to completion")
}

func TestSpendBatchWithRetry(t *testing.T) {
	t.Run("clean spends: one call each, no retries", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		require.NoError(t, u.spendBatchWithRetry(context.Background(), block, txs, false, nil))
		require.Equal(t, int64(3), spy.spendCalls.Load())
	})

	t.Run("transient failure converges: retried tx succeeds on attempt 2", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[1].TxIDChainHash()
		spy.failuresLeft[h] = 1
		spy.failErr[h] = errors.NewStorageError("transient device overload") // retryable class

		require.NoError(t, u.spendBatchWithRetry(context.Background(), block, txs, false, nil))
		// 3 first-attempt + 1 retry
		require.Equal(t, int64(4), spy.spendCalls.Load())
	})

	t.Run("conflicting spend fails hard, never retried", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[0].TxIDChainHash()
		spy.failuresLeft[h] = 999
		spy.failErr[h] = errors.NewTxConflictingError("conflicting")

		err := u.spendBatchWithRetry(context.Background(), block, txs, false, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxConflicting) || errors.Is(err, errors.ErrProcessing), "hard fail must surface the conflict")
		require.Equal(t, int64(3), spy.spendCalls.Load()) // first attempt only — never retried
	})

	t.Run("non-retryable error fails hard on first attempt", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[2].TxIDChainHash()
		spy.failuresLeft[h] = 1
		spy.failErr[h] = errors.NewTxInvalidError("bad tx") // not retryable

		require.Error(t, u.spendBatchWithRetry(context.Background(), block, txs, false, nil))
	})

	t.Run("no progress: permanently-retryable tx gives up with error", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[1].TxIDChainHash()
		spy.failuresLeft[h] = 999
		spy.failErr[h] = errors.NewStorageError("still overloaded")

		err := u.spendBatchWithRetry(context.Background(), block, txs, false, nil)
		require.Error(t, err)
		// gave up on no-progress after attempt 1 (same 1 tx failing), NOT after 10 attempts:
		// 3 first-attempt calls + 1 retry call = 4
		require.Equal(t, int64(4), spy.spendCalls.Load())
	})
}
