package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// markCreating puts the `creating` bin back on one record of txHash, the state a
// failed clearCreatingFlag leaves behind.
func markCreating(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, txHash *chainhash.Hash, recordIdx uint32) {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(txHash, recordIdx))
	require.NoError(t, err)
	require.NoError(t, client.PutBins(nil, key, aerospike.NewBin(fields.Creating.String(), true)))
}

// recordHasCreatingBin reports whether one record of txHash still has a `creating`
// bin at all. Mining deletes the bin, as clearCreatingFlag does, rather than
// setting it false.
func recordHasCreatingBin(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, txHash *chainhash.Hash, recordIdx uint32) bool {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(txHash, recordIdx))
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, rec, "record %d for %s should exist", recordIdx, txHash.String())

	_, ok := rec.Bins[fields.Creating.String()]

	return ok
}

// spendOutput builds a child that spends one output of parent.
func spendOutput(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	out := parent.Outputs[vout]

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxIDChainHash().String(), vout, out.LockingScript.String(), out.Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", out.Satoshis-100))

	return child
}

// TestSetMinedMultiClearsCreatingOnExtraRecords covers a paginated transaction
// whose clearCreatingFlag failed part-way, leaving `creating` on the master and
// on an extra record. setMined is keyed on the master, so before this fix only
// the master lost `creating`. The extra record kept it, and every output on it
// (vout >= utxoBatchSize) was refused with ErrTxCreating although the
// transaction was mined. Mining must make every output spendable.
//
// The first-mine case forges the stuck state before the first SetMinedMulti.
// The re-mine case forges it on the extra records after a first mine and mines
// again with the same block ID, which sends the expression path down its
// FILTERED_OUT branch. A re-mine clears the state too, but only when something
// mines the transaction again; a transaction already mined and settled with the
// stuck state is not revisited.
func TestSetMinedMultiClearsCreatingOnExtraRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, useExpressions := range []bool{false, true} {
		for _, reMine := range []bool{false, true} {
			name := "lua"
			if useExpressions {
				name = "expressions"
			}

			if reMine {
				name += "/re-mine"
			} else {
				name += "/first-mine"
			}

			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				logger := ulogger.NewErrorTestLogger(t)
				settings := test.CreateBaseTestSettings(t)

				// Batch size 4 and 10 outputs: master holds vouts 0-3, extra
				// record 1 holds 4-7, extra record 2 holds 8-9.
				settings.UtxoStore.UtxoBatchSize = 4
				settings.Aerospike.EnableSetMinedFilterExpressions = useExpressions

				client, store, _, cleanup := initAerospike(t, settings, logger)
				defer cleanup()

				cleanDB(t, client)

				const blockHeight = 604315

				const blockID = 11

				// createTransactionWithOutputs spends the zero hash, so the store
				// treats the parent as a coinbase; spend it well past maturity.
				const spendHeight = blockHeight + 1_000

				parent := createTransactionWithOutputs(10)
				parentHash := parent.TxIDChainHash()

				_, err := store.Create(ctx, parent, blockHeight)
				require.NoError(t, err)

				extras := totalExtraRecs(t, client, store, parentHash)
				require.Equal(t, 2, extras, "10 outputs at batch size 4 must give two extra records")

				mined := utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: blockHeight, OnLongestChain: true}

				if reMine {
					_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, mined)
					require.NoError(t, err)
				} else {
					markCreating(t, client, store, parentHash, 0)
				}

				for i := uint32(1); i <= uint32(extras); i++ { // nolint:gosec
					markCreating(t, client, store, parentHash, i)
				}

				// Precondition: the forged state really blocks a spend of an
				// output on an extra record.
				_, err = store.Spend(ctx, spendOutput(t, parent, 9), spendHeight)
				require.ErrorIs(t, err, errors.ErrTxCreating, "the forged creating bin must block the spend")

				_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, mined)
				require.NoError(t, err)

				for i := uint32(0); i <= uint32(extras); i++ { // nolint:gosec
					require.Falsef(t, recordHasCreatingBin(t, client, store, parentHash, i),
						"record %d must not have a creating bin once the transaction is mined", i)
				}

				// One output on each record: master, extra 1, extra 2.
				for _, vout := range []uint32{0, 5, 9} {
					_, err = store.Spend(ctx, spendOutput(t, parent, vout), spendHeight)
					require.NoErrorf(t, err, "vout %d of a mined transaction must be spendable", vout)
				}
			})
		}
	}
}
