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
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spendRetrySpyStore wraps NullStore; Spend behaviour is scripted per tx hash.
type spendRetrySpyStore struct {
	*nullstore.NullStore
	mu sync.Mutex
	// failuresLeft[txid] = how many times Spend should fail with failErr[txid]
	failuresLeft map[chainhash.Hash]int
	failErr      map[chainhash.Hash]error
	spendCalls   atomic.Int64
}

func (s *spendRetrySpyStore) Spend(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	s.spendCalls.Add(1)
	h := *tx.TxIDChainHash()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.failuresLeft[h]; ok && n > 0 {
		s.failuresLeft[h] = n - 1
		return nil, s.failErr[h]
	}
	return nil, nil
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

func TestSpendBatchWithRetry(t *testing.T) {
	t.Run("clean spends: one call each, no retries", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		require.NoError(t, u.spendBatchWithRetry(context.Background(), block, txs, false))
		require.Equal(t, int64(3), spy.spendCalls.Load())
	})

	t.Run("transient failure converges: retried tx succeeds on attempt 2", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[1].TxIDChainHash()
		spy.failuresLeft[h] = 1
		spy.failErr[h] = errors.NewStorageError("transient device overload") // retryable class

		require.NoError(t, u.spendBatchWithRetry(context.Background(), block, txs, false))
		// 3 first-attempt + 1 retry
		require.Equal(t, int64(4), spy.spendCalls.Load())
	})

	t.Run("conflicting spend fails hard, never retried", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[0].TxIDChainHash()
		spy.failuresLeft[h] = 999
		spy.failErr[h] = errors.NewTxConflictingError("conflicting")

		err := u.spendBatchWithRetry(context.Background(), block, txs, false)
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

		require.Error(t, u.spendBatchWithRetry(context.Background(), block, txs, false))
	})

	t.Run("no progress: permanently-retryable tx gives up with error", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
		u, block, txs := newSpendRetryHarness(t, spy)

		h := *txs[1].TxIDChainHash()
		spy.failuresLeft[h] = 999
		spy.failErr[h] = errors.NewStorageError("still overloaded")

		err := u.spendBatchWithRetry(context.Background(), block, txs, false)
		require.Error(t, err)
		// gave up on no-progress after attempt 1 (same 1 tx failing), NOT after 10 attempts:
		// 3 first-attempt calls + 1 retry call = 4
		require.Equal(t, int64(4), spy.spendCalls.Load())
	})
}
