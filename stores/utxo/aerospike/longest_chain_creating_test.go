package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
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

// TestMarkOnLongestChain_StampsACreatingRecordSoNoRollbackClaimsIt covers a creating record that the
// mark finds and cannot complete. Its transaction is in a block of the longest chain, and the record
// is left for the sweep, which would roll back an attempt that can never spend its inputs. A
// rollback unspends by txid, so it would free outputs that a mined transaction spent. The pre-flight
// stamps the record before it rolls it forward, and no rollback claims a stamped record.
func TestMarkOnLongestChain_StampsACreatingRecordSoNoRollbackClaimsIt(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)
	require.NoError(t, store.SetBlockHeight(1000))

	// unwinnable leaves the tentative record of a transaction whose only input a rival has spent,
	// with no spend lease on it, as a record whose attempt is gone.
	unwinnable := func(t *testing.T, seed int) (*bt.Tx, *meta.Data) {
		t.Helper()

		parent := conflictingIndexParent(t, ctx, store, seed, 2)
		stuck := conflictingIndexTx(t, parent, []uint32{0}, 1)

		md, err := store.Create(ctx, stuck, 900, utxo.WithCreating(true))
		require.NoError(t, err)

		_, err = store.Spend(ctx, conflictingIndexTx(t, parent, []uint32{0}, 2), 901, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err, "precondition: a rival took the input")

		require.NoError(t, store.ReleaseSpendLease(ctx, stuck.TxIDChainHash(), md.Attempt, md.Attempt))

		return stuck, md
	}

	t.Run("a record the mark cannot complete is stamped and closed to a rollback", func(t *testing.T) {
		stuck, md := unwinnable(t, 8301)
		hash := stuck.TxIDChainHash()

		err := store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true)
		require.ErrorIs(t, err, errors.ErrSpent, "the record could not be completed")

		require.Equal(t, true, recordBins(t, client, store, hash, 0, fields.MarkOwed)[fields.MarkOwed.String()], "the record carries the stamp")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating)
		require.True(t, got.MarkOwed, "a read of the record reports the stamp")

		live, err := store.SpendLeasesLive(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, live, "no spend lease is there to refuse a rollback for another reason")

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.False(t, began, "no rollback may claim the record of a transaction on the longest chain")

		ended, err := utxo.RollBackCreatingEnded(ctx, logger, store, stuck, md.Attempt)
		require.NoError(t, err)
		require.False(t, ended)

		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating, "the record is still there for a roll-forward")
		require.False(t, got.RollingBack)
	})

	t.Run("a mark off the longest chain takes the stamp away, and the record can be rolled back again", func(t *testing.T) {
		stuck, md := unwinnable(t, 8302)
		hash := stuck.TxIDChainHash()

		require.Error(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true))
		require.Contains(t, recordBins(t, client, store, hash, 0, fields.MarkOwed), fields.MarkOwed.String(), "precondition: the record is stamped")

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, false))

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.MarkOwed), fields.MarkOwed.String(), "the block left the longest chain, so the mark is no longer owed")

		ended, err := utxo.RollBackCreatingEnded(ctx, logger, store, stuck, md.Attempt)
		require.NoError(t, err)
		require.True(t, ended, "the attempt is an ordinary tentative one again")
	})

	t.Run("a record whose rollback began first is not stamped", func(t *testing.T) {
		stuck, md := unwinnable(t, 8303)
		hash := stuck.TxIDChainHash()

		began, err := store.BeginCreateFirstRollback(ctx, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began)

		require.Error(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true))

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.MarkOwed), fields.MarkOwed.String(), "a rollback that has begun cannot be stopped, so the stamp would say what is not true")
	})

	t.Run("a mark off the longest chain stamps nothing", func(t *testing.T) {
		stuck, _ := unwinnable(t, 8304)
		hash := stuck.TxIDChainHash()

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, false))

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.MarkOwed), fields.MarkOwed.String())
	})
}

// TestMarkOnLongestChain_RollsForwardAtTheHeightOfTheRecordsBlock covers the height the pre-flight
// spends at. The tip of the store can lag the block whose transactions are being marked. A spend at
// the tip then finds a coinbase parent immature that is mature at the height of the block, and the
// record is not completed. A record that carries the height of its block is rolled forward there.
func TestMarkOnLongestChain_RollsForwardAtTheHeightOfTheRecordsBlock(t *testing.T) {
	fx := newTxFixtures(t)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	const (
		coinbaseHeight = 2000
		laggingTip     = 1990
		blockHeight    = 2005
	)

	_, err := store.Create(ctx, fx.coinbaseTx, coinbaseHeight)
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(laggingTip))

	child := conflictingIndexTx(t, fx.coinbaseTx, []uint32{0}, 1)
	hash := child.TxIDChainHash()

	md, err := store.Create(ctx, child, blockHeight, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 9, BlockHeight: blockHeight}))
	require.NoError(t, err)

	require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt))

	spends, err := store.Spend(ctx, child, laggingTip)
	require.Error(t, err)
	require.ErrorIs(t, spends[0].Err, errors.ErrTxCoinbaseImmature, "precondition: at the tip of the store the parent is not mature")

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*hash}, true), "at the height of its block the record is completed and marked")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating)
	require.Nil(t, recordBins(t, client, store, hash, 0, fields.UnminedSince)[fields.UnminedSince.String()], "it is mined on the longest chain")

	resp, err := store.GetSpend(ctx, spendHeightSpendsOf(t, child)[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "its input is spent")
}

// TestMarkOnLongestChain_ReadsTheCreatingStateInBoundedBatches covers the read that comes before the
// mark. It covers every hash, so it is as wide as the mark: it runs in batches of
// utxostore_maxMinedBatchSize, and a batch whose read fails leaves only its own hashes out of the
// mark. The others are still marked, as the mark's own contract says.
func TestMarkOnLongestChain_ReadsTheCreatingStateInBoundedBatches(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.MaxMinedBatchSize = 2
	tSettings.UtxoStore.MaxMinedRoutines = 1

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)
	require.NoError(t, store.SetBlockHeight(1000))

	// isStateRead reports whether a batch is one of the pre-flight's reads, and of which keys.
	isStateRead := func(records []aerospike.BatchRecordIfc) bool {
		for _, record := range records {
			read, ok := record.(*aerospike.BatchRead)
			if !ok || len(read.BinNames) != 3 || read.BinNames[0] != fields.Creating.String() {
				return false
			}
		}

		return len(records) > 0
	}

	unminedSinceOf := func(t *testing.T, hash *chainhash.Hash) any {
		return recordBins(t, client, store, hash, 0, fields.UnminedSince)[fields.UnminedSince.String()]
	}

	// seven stores seven unmined transactions, the fourth of which is a creating record that the
	// pre-flight completes.
	seven := func(t *testing.T, seed int) []chainhash.Hash {
		t.Helper()

		parent := conflictingIndexParent(t, ctx, store, seed, 7)
		hashes := make([]chainhash.Hash, 0, 7)

		for vout := uint32(0); vout < 7; vout++ {
			tx := conflictingIndexTx(t, parent, []uint32{vout}, 1)

			if vout == 3 {
				_, err := store.Create(ctx, tx, 900, utxo.WithCreating(true))
				require.NoError(t, err)
			} else {
				_, _, err := store.SpendAndCreate(ctx, tx, 900)
				require.NoError(t, err)
			}

			hashes = append(hashes, *tx.TxIDChainHash())
		}

		return hashes
	}

	t.Run("the read is sent in batches, and a creating record in a later batch is still found", func(t *testing.T) {
		hashes := seven(t, 8401)

		var sizes []int

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			if isStateRead(records) {
				sizes = append(sizes, len(records))
			}

			return client.BatchOperate(policy, records)
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, hashes, true))

		store.SetBatchOperateFn(nil)

		require.Equal(t, []int{2, 2, 2, 1}, sizes, "seven hashes are read in batches of two")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, &hashes[3], got))
		require.False(t, got.Creating, "the creating record, read in the second batch through reused records, was completed")

		for i := range hashes {
			require.Nil(t, unminedSinceOf(t, &hashes[i]), "transaction %d is marked", i)
		}
	})

	t.Run("a batch whose read fails leaves only its own hashes out of the mark", func(t *testing.T) {
		hashes := seven(t, 8402)

		batch := 0

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			if isStateRead(records) {
				batch++

				if batch == 3 {
					return aerospike.ErrNetwork
				}
			}

			return client.BatchOperate(policy, records)
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })

		err := store.MarkTransactionsOnLongestChain(ctx, hashes, true)
		require.Error(t, err, "the hashes whose state could not be read are reported")

		store.SetBatchOperateFn(nil)

		for i := range hashes {
			if i == 4 || i == 5 {
				require.NotNil(t, unminedSinceOf(t, &hashes[i]), "transaction %d was in the batch that could not be read: it is not known to be complete, so it is not marked", i)

				continue
			}

			require.Nil(t, unminedSinceOf(t, &hashes[i]), "transaction %d is marked although another batch failed", i)
		}
	})
}
