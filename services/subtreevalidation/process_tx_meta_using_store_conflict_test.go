package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The fresh path's conflict check runs once, after every decorate batch. An
// entry processTxMetaUsingStore leaves set is never decorated again, so a
// conflicting tx must not be left set without a verdict, however the call ends:
// a batch tripping the fail-fast missing-tx threshold must not let a collected
// conflict escape, and a check that fails retryably must leave its txs unset for
// the retry.
func Test_processTxMetaUsingStore_conflictCheckFailFast(t *testing.T) {
	ctx := context.Background()
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})

	t.Run("a threshold failure in another batch does not hide a conflict", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		// one tx per batch, run in order: the conflicting tx is collected first,
		// then the second missing tx trips the threshold
		tSettings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1
		tSettings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
		tSettings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 1

		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, 1, false, mined)

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		txHashes := []chainhash.Hash{chain[0], chainhash.HashH([]byte("missing-1")), chainhash.HashH([]byte("missing-2"))}
		txMetaSlice := make([]metaSliceItem, len(txHashes))

		for attempt := 1; attempt <= 2; attempt++ {
			_, err := s.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{77: true}, true, true, true)
			require.ErrorIsf(t, err, errors.ErrTxInvalid, "attempt %d: the counter-spender is mined on the candidate chain", attempt)
		}
	})

	t.Run("a retryable check failure leaves its txs unset", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 2
		tSettings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 2

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 4, false)

		faulty := &faultyGetMetaStore{Store: store, fail: *counter.TxIDChainHash()}
		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: faulty}

		txMetaSlice := make([]metaSliceItem, len(chain))

		_, err := s.processTxMetaUsingStore(ctx, chain, txMetaSlice, map[uint32]bool{77: true}, true, true, true)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.NotErrorIs(t, err, errors.ErrTxInvalid)

		for i := range txMetaSlice {
			require.Falsef(t, txMetaSlice[i].isSet, "chain[%d] was not checked, so the retry must decorate it again", i)
		}
	})
}

// On the fresh path, a record the check needs and the store does not hold fails
// it retryably, as the per-transaction check does and as blessMissingTransaction
// does for the same store state, so the verdict does not depend on which path the
// node took. Here the mined counter-spender has been pruned while its parent
// survives, and the conflicting tx double-spends the parent's output. Treating
// the pruned record as not mined would pass the conflict. The cached path keeps
// its own policy and passes: a trailer can outlive the records it names.
func Test_processTxMetaUsingStore_prunedCounterSpenderFailsClosed(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	store := newSQLUtxoStore(t, ctx, tSettings)
	counter, chain := buildConflictingChain(t, ctx, store, 1, false, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100}))
	require.NoError(t, store.Delete(ctx, counter.TxIDChainHash()))

	blockIds := map[uint32]bool{77: true}
	s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

	require.ErrorIs(t, s.blessConflictCheck(ctx, chain[0], blockIds), errors.ErrTxNotFound,
		"the check blessMissingTransaction runs fails on the pruned record")

	_, err := s.processTxMetaUsingStore(ctx, chain, make([]metaSliceItem, len(chain)), blockIds, true, false, true)
	require.ErrorIs(t, err, errors.ErrTxNotFound)
	require.NotErrorIs(t, err, errors.ErrTxInvalid)

	cached, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})
	require.NoError(t, cached.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
}
