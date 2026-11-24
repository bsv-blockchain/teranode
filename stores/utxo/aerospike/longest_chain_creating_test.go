package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Marking a transaction as mined on the longest chain clears its unminedSince. A record that is
// still creating needs that bin: the creating sweep selects by it, so a creating record without it
// is never found again, and its inputs stay unspent for good. These tests pin that the mark settles
// a creating record first, as the setMined pre-flight does, and leaves one it cannot settle as it was.

func TestMarkOnLongestChain_SettlesACreatingRecordFirst(t *testing.T) {
	t.Run("a creating record is rolled forward and then marked", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)
		require.NoError(t, store.SetBlockHeight(1000))

		parent := conflictingIndexParent(t, ctx, store, 8101, 3)
		child := conflictingIndexTx(t, parent, []uint32{0}, 1)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 900, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 900}))
		require.NoError(t, err)

		require.NotNil(t, recordBins(t, client, store, hash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "precondition: the tentative record carries its height")

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true))

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating, "the record was rolled forward, not left creating without a height")

		resp, err := store.GetSpend(ctx, spendHeightSpendsOf(t, child)[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "its input is spent")
		require.Equal(t, hash.String(), resp.SpendingData.TxID.String())

		require.Nil(t, recordBins(t, client, store, hash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "it is mined on the longest chain now")
	})

	t.Run("a creating record that cannot be rolled forward keeps its height, fails the call and does not stop the others", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)
		require.NoError(t, store.SetBlockHeight(1000))

		parent := conflictingIndexParent(t, ctx, store, 8102, 3)
		stuck := conflictingIndexTx(t, parent, []uint32{0}, 1)
		stuckHash := stuck.TxIDChainHash()

		_, err := store.Create(ctx, stuck, 900, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 900}))
		require.NoError(t, err)

		rival := conflictingIndexTx(t, parent, []uint32{0}, 2)
		_, err = store.Spend(ctx, rival, 901, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err, "precondition: a rival took the input")

		other := conflictingIndexTx(t, parent, []uint32{1}, 1)
		otherHash := other.TxIDChainHash()

		_, err = store.Create(ctx, other, 900)
		require.NoError(t, err)

		require.NotNil(t, recordBins(t, client, store, otherHash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "precondition: an ordinary unmined record carries its height")

		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*stuckHash, *otherHash}, true)
		require.Error(t, err, "a record that could not be settled is reported")
		require.ErrorIs(t, err, errors.ErrSpent, "the reason the roll-forward lost is in the error")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, stuckHash, got))
		require.True(t, got.Creating, "the record was not finalized")
		require.NotNil(t, recordBins(t, client, store, stuckHash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "the record keeps the height the sweep selects by")

		stale, err := store.QueryStaleCreatingTxs(ctx, 5000, 0)
		require.NoError(t, err)
		require.Contains(t, stale, *stuckHash, "the sweep still finds it")

		require.Nil(t, recordBins(t, client, store, otherHash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "the other transaction of the call is marked")
	})

	t.Run("a record that is rolling back keeps its height and fails the call", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)
		require.NoError(t, store.SetBlockHeight(1000))

		parent := conflictingIndexParent(t, ctx, store, 8103, 3)
		child := conflictingIndexTx(t, parent, []uint32{0}, 1)
		hash := child.TxIDChainHash()

		md, err := store.Create(ctx, child, 900, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 900}))
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		require.Error(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true))

		require.NotNil(t, recordBins(t, client, store, hash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "a rollback or its hold is still found by the sweep")
	})

	t.Run("marking off the longest chain settles nothing", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)
		require.NoError(t, store.SetBlockHeight(1000))

		parent := conflictingIndexParent(t, ctx, store, 8104, 3)
		child := conflictingIndexTx(t, parent, []uint32{0}, 1)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 900, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 900}))
		require.NoError(t, err)

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, false))

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating, "nothing rolled the record forward")
	})
}
