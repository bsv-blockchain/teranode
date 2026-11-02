package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// pruneBeforeCreate runs the pruner's commit for a child, its marker on the
// parent and then its deletion, in the window between SequentialSpendAndCreate's
// spend and its create.
type pruneBeforeCreate struct {
	*Store
	prune func()
}

func (p *pruneBeforeCreate) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	if p.prune != nil {
		p.prune()
	}

	return p.Store.Create(ctx, tx, blockHeight, opts...)
}

// TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction: a rebroadcast of
// a mined, fully spent transaction C is accepted by the spend as idempotent,
// because its parent's output already names C. When the pruner marks the parent
// and deletes C after that spend and before the create, the create finds no
// record and writes C again, with outputs a confirmed descendant consumed.
// Nothing rejects, so nothing compensates. The recheck after the create must
// answer the replay as pruned and remove the record it wrote, leaving the
// parent's historical spend in place.
//
// The control row has no prune in the window: an earlier attempt spent C's
// input and never created C, so the spend is idempotent, the recheck passes
// and C is created.
func TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pruned bool
	}{
		{name: "pruned in the window", pruned: true},
		{name: "idempotent retry, nothing pruned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, _ := setup(ctx, t)

			parent := bt.NewTx()
			require.NoError(t, parent.From("3333333333333333333333333333333333333333333333333333333333333333", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			// The parent's output already records C's spend: C's own, long ago,
			// or an earlier attempt's.
			_, err = store.Spend(ctx, child, 1000)
			require.NoError(t, err)

			wrapped := &pruneBeforeCreate{Store: store}

			if tc.pruned {
				// C existed and the pruner removes it in the window.
				_, err = store.Create(ctx, child, 1000)
				require.NoError(t, err)

				wrapped.prune = func() {
					_, insertErr := store.db.ExecContext(ctx,
						`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
						parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
					require.NoError(t, insertErr)
					require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))
				}
			}

			_, _, err = utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, wrapped, child, 1000)

			_, getErr := store.Get(ctx, child.TxIDChainHash())

			if tc.pruned {
				require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay must be answered as pruned")
				require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated record must be removed again")
			} else {
				require.NoError(t, err)
				require.NoError(t, getErr, "a recheck that passes keeps the created record")
			}

			require.Equal(t, spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes(), outputSpendingData(t, ctx, store, parent, 0),
				"the parent keeps C's spend either way")
		})
	}
}
