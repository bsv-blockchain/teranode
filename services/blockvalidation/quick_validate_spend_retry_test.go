package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
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
	// consensusFrozen[txid]: the spend is rejected with ErrUtxoConsensusFrozen unless the
	// call carries IgnoreConsensusFreeze — the store's behaviour for a coin whose freeze
	// window covers blockHeight.
	consensusFrozen map[chainhash.Hash]bool
	// lastIgnoreConsensusFreeze records the flag the most recent spend-phase call carried.
	lastIgnoreConsensusFreeze atomic.Bool
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
	s.lastIgnoreConsensusFreeze.Store(options.IgnoreFlags.IgnoreConsensusFreeze)
	h := *tx.TxIDChainHash()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consensusFrozen[h] && !options.IgnoreFlags.IgnoreConsensusFreeze {
		return nil, nil, errors.NewUtxoConsensusFrozenError("utxo is consensus-frozen at height %d", blockHeight)
	}
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

// TestSpendBatchWithRetry_ConsensusFreezeBypassIsCheckpointBound pins that the consensus
// tier of an alert-system freeze is lifted only for a block at or below the highest
// HARDCODED checkpoint. Quick validation also runs above that height under the
// catchup-checkpoint override, and there a spend inside a freeze window must be the
// block-invalid verdict every node derives — not bypassed, and not retried (#1422).
func TestSpendBatchWithRetry_ConsensusFreezeBypassIsCheckpointBound(t *testing.T) {
	const checkpointHeight = 100

	t.Run("at or below the checkpoint the bypass is carried and the spend succeeds", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}, consensusFrozen: map[chainhash.Hash]bool{}}
		u, block, txs := newSpendRetryHarness(t, spy)
		u.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
		block.Height = checkpointHeight

		spy.consensusFrozen[*txs[1].TxIDChainHash()] = true

		require.NoError(t, u.spendBatchWithRetry(context.Background(), block, txs, false))
		require.True(t, spy.lastIgnoreConsensusFreeze.Load(), "a checkpointed block's spends must carry IgnoreConsensusFreeze")
		require.Equal(t, int64(3), spy.spendCalls.Load())
	})

	t.Run("above the checkpoint the window is enforced and the block is invalid", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}, consensusFrozen: map[chainhash.Hash]bool{}}
		u, block, txs := newSpendRetryHarness(t, spy)
		u.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
		block.Height = checkpointHeight + 1

		spy.consensusFrozen[*txs[1].TxIDChainHash()] = true

		err := u.spendBatchWithRetry(context.Background(), block, txs, false)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a consensus-frozen spend above the checkpoint is a block-invalid verdict, got %v", err)
		require.False(t, spy.lastIgnoreConsensusFreeze.Load(), "a block above the checkpoint must not carry IgnoreConsensusFreeze")
		require.Equal(t, int64(3), spy.spendCalls.Load(), "a consensus verdict is never retried")
	})

	t.Run("no checkpoints configured fails closed: the window is enforced", func(t *testing.T) {
		spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}, consensusFrozen: map[chainhash.Hash]bool{}}
		u, block, txs := newSpendRetryHarness(t, spy)
		u.settings.ChainCfgParams.Checkpoints = nil

		spy.consensusFrozen[*txs[0].TxIDChainHash()] = true

		err := u.spendBatchWithRetry(context.Background(), block, txs, false)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got %v", err)
		require.False(t, spy.lastIgnoreConsensusFreeze.Load())
	})
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
