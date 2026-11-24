package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The block assembly scan decodes records inline from QueryPartitionsRaw. Every
// unmined record must come back exactly once with the values the store holds;
// conflicting and mined records and paginated child records must not.
func TestUnminedIterator_RawScanMatchesStore(t *testing.T) {
	t.Parallel()

	for _, storeInpoints := range []bool{false, true} {
		t.Run(map[bool]string{false: "subtree meta off", true: "subtree meta on"}[storeInpoints], func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = storeInpoints

			_, store, ctx, deferFn := initAerospikeOwnServer(t, tSettings, logger)
			t.Cleanup(deferFn)

			height := store.GetBlockHeight() + 1

			var (
				want   = map[chainhash.Hash]*bt.Tx{}
				locked = map[chainhash.Hash]bool{}
			)

			for i := 0; i < 20; i++ {
				tx := paginatedSpendAndCreateTx(t, tests.Tx, 0, uint64(1000+i), 2)
				lock := i%5 == 0

				_, _, err := store.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly(), utxo.WithLocked(lock))
				require.NoError(t, err)

				want[*tx.TxIDChainHash()] = tx
				locked[*tx.TxIDChainHash()] = lock
			}

			// A paginated record: its main record is yielded once, its child
			// records never.
			paginated := paginatedSpendAndCreateTx(t, tests.Tx, 1, 5000, tSettings.UtxoStore.UtxoBatchSize+5)
			_, _, err := store.SpendAndCreate(ctx, paginated, height, utxo.WithCreateOnly())
			require.NoError(t, err)

			want[*paginated.TxIDChainHash()] = paginated

			conflicting := paginatedSpendAndCreateTx(t, tests.Tx, 2, 6000, 2)
			_, _, err = store.SpendAndCreate(ctx, conflicting, height, utxo.WithCreateOnly(), utxo.WithConflicting(true))
			require.NoError(t, err)

			mined := paginatedSpendAndCreateTx(t, tests.Tx, 3, 7000, 2)
			_, _, err = store.SpendAndCreate(ctx, mined, height, utxo.WithCreateOnly(),
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: height, SubtreeIdx: 0}))
			require.NoError(t, err)

			// The unmined_since index is built asynchronously.
			first := *paginatedSpendAndCreateTx(t, tests.Tx, 0, 1000, 2).TxIDChainHash()
			require.True(t, waitUnminedIteratorHas(t, ctx, store, &first, 30*time.Second))

			it, err := store.GetUnminedTxIterator()
			require.NoError(t, err)

			got := map[chainhash.Hash]*utxo.UnminedTransaction{}

			for {
				batch, err := it.Next(ctx)
				require.NoError(t, err)

				if len(batch) == 0 {
					break
				}

				for _, tx := range batch {
					if tx.Skip {
						continue
					}

					_, dup := got[tx.Hash]
					require.False(t, dup, "%s yielded twice", tx.Hash)
					got[tx.Hash] = tx
				}
			}

			require.NoError(t, it.Close())

			require.NotContains(t, got, *conflicting.TxIDChainHash())
			require.NotContains(t, got, *mined.TxIDChainHash())
			require.Len(t, got, len(want))

			for hash, tx := range want {
				u, ok := got[hash]
				require.True(t, ok, "%s not yielded", hash)

				m, err := store.Get(ctx, &hash, fields.Fee, fields.SizeInBytes)
				require.NoError(t, err)

				require.Equal(t, m.Fee, u.Fee, "fee of %s", hash)
				require.Equal(t, m.SizeInBytes, u.SizeInBytes, "size of %s", hash)
				require.Equal(t, locked[hash], u.Locked, "locked of %s", hash)
				require.Positive(t, u.CreatedAt)
				require.Positive(t, u.UnminedSince)
				require.NotNil(t, u.TxInpoints)

				// parent links come back in both modes: the setting only decides
				// whether subtree meta files are written
				require.Equal(t, []chainhash.Hash{*tests.Tx.TxIDChainHash()}, u.TxInpoints.GetParentTxHashes(), "inpoints of %s", hash)
				require.Equal(t, tx.Inputs[0].PreviousTxOutIndex, u.TxInpoints.GetTxInpoints()[0].Index, "vout of %s", hash)
			}
		})
	}
}

// A record whose external file has gone is left out of the block assembly scan,
// against a real Aerospike and file blob store: the scan must not fail, and
// every other unmined transaction, external ones included, still comes back.
// The plain scan, which the rewind tool uses, still fails on the same record.
func TestUnminedIterator_SkipsExternalRecordWithMissingFile(t *testing.T) {
	t.Parallel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.ExternalizeAllTransactions = true

	_, store, ctx, deferFn := initAerospikeOwnServer(t, tSettings, logger)
	t.Cleanup(deferFn)

	height := store.GetBlockHeight() + 1

	healthy := paginatedSpendAndCreateTx(t, tests.Tx, 0, 1000, 2)
	_, _, err := store.SpendAndCreate(ctx, healthy, height, utxo.WithCreateOnly())
	require.NoError(t, err)

	broken := paginatedSpendAndCreateTx(t, tests.Tx, 1, 2000, 2)
	_, _, err = store.SpendAndCreate(ctx, broken, height, utxo.WithCreateOnly())
	require.NoError(t, err)

	// The unmined_since index is built asynchronously; wait while both files
	// still exist, so a missing record below can only be the skip.
	require.True(t, waitUnminedIteratorHas(t, ctx, store, broken.TxIDChainHash(), 30*time.Second))
	require.True(t, waitUnminedIteratorHas(t, ctx, store, healthy.TxIDChainHash(), 30*time.Second))

	require.NoError(t, store.GetExternalStore().Del(ctx, broken.TxIDChainHash()[:], fileformat.FileTypeTx))

	plain, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	var plainErr error
	for plainErr == nil {
		var batch []*utxo.UnminedTransaction
		if batch, plainErr = plain.Next(ctx); plainErr == nil && len(batch) == 0 {
			break
		}
	}

	require.ErrorIs(t, plainErr, errors.ErrNotFound)
	require.NoError(t, plain.Close())

	it, err := store.GetUnminedTxIteratorSkippingMissingExternal()
	require.NoError(t, err)

	got := map[chainhash.Hash]*utxo.UnminedTransaction{}

	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)

		if len(batch) == 0 {
			break
		}

		for _, tx := range batch {
			if !tx.Skip {
				got[tx.Hash] = tx
			}
		}
	}

	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	require.NotContains(t, got, *broken.TxIDChainHash())
	require.Contains(t, got, *healthy.TxIDChainHash())
	require.Equal(t, []chainhash.Hash{*tests.Tx.TxIDChainHash()}, got[*healthy.TxIDChainHash()].TxInpoints.GetParentTxHashes())
}
