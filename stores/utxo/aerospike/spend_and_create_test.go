package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// pruneBeforeCreate runs the pruner's commit for a child, its marker on the
// parent and then its deletion, in the window between SequentialSpendAndCreate's
// spend and its create.
type pruneBeforeCreate struct {
	*teranodeaerospike.Store
	prune func()
}

func (p *pruneBeforeCreate) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	if p.prune != nil {
		p.prune()
	}

	return p.Store.Create(ctx, tx, blockHeight, opts...)
}

// TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction is the Aerospike
// twin of the SQL test of the same name. A rebroadcast of a mined, fully spent
// transaction C is accepted by the spend as idempotent. When the pruner marks
// the parent and deletes C after that spend and before the create, the create
// writes C again. The recheck after the create must answer the replay as pruned
// and remove the record, leaving the parent's historical spend in place. The
// control row prunes nothing, so the recheck passes and C is created.
func TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction(t *testing.T) {
	s := test.CreateBaseTestSettings(t)

	client, store, ctx, cleanup := initAerospike(t, s, ulogger.New("spend-create-recheck"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	for i, tc := range []struct {
		name         string
		pruned       bool
		parentPruned bool
	}{
		{name: "pruned in the window", pruned: true},
		{name: "idempotent retry, nothing pruned"},
		{name: "parent pruned too", pruned: true, parentPruned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := []string{
				"cccc666666666666666666666666666666666666666666666666666666666666",
				"dddd666666666666666666666666666666666666666666666666666666666666",
				"eeee666666666666666666666666666666666666666666666666666666666666",
			}[i]

			parent := bt.NewTx()
			require.NoError(t, parent.From(seed, 0, "51", 30000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			// The parent's output already records C's spend.
			_, err = store.Spend(ctx, child, 1000)
			require.NoError(t, err)

			wrapped := &pruneBeforeCreate{Store: store}

			if tc.pruned {
				_, err = store.Create(ctx, child, 1000)
				require.NoError(t, err)

				parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
				require.NoError(t, err)

				if tc.parentPruned {
					// The parent is gone before the call, so the spend is
					// blessed on C's record, and the recheck must not bless it
					// on the record this call wrote.
					require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))
				}

				wrapped.prune = func() {
					if !tc.parentPruned {
						require.NoError(t, client.Put(nil, parentKey, aerospike.BinMap{
							fields.DeletedChildren.String(): map[interface{}]interface{}{child.TxIDChainHash().String(): true},
						}))
					}

					require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))
				}
			}

			_, _, err = utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, wrapped, child, 1000)

			_, getErr := store.Get(ctx, child.TxIDChainHash())

			if tc.parentPruned {
				require.ErrorIs(t, err, errors.ErrTxNotFound, "the replay must be answered")
				require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated record must be removed again")

				return
			}

			if tc.pruned {
				require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay must be answered as pruned")
				require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated record must be removed again")
			} else {
				require.NoError(t, err)
				require.NoError(t, getErr, "a recheck that passes keeps the created record")
			}

			hash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
			require.NoError(t, err)

			after, err := store.GetSpend(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: hash})
			require.NoError(t, err)
			require.NotNil(t, after.SpendingData, "the parent keeps C's spend either way")
			require.Equal(t, *child.TxIDChainHash(), *after.SpendingData.TxID)
		})
	}
}
