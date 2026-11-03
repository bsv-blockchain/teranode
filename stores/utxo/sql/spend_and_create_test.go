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
// spend and its create. With outcomeUnknown the create runs and then reports
// the outcome as unknown, as a batcher wait that ran out does.
type pruneBeforeCreate struct {
	*Store
	prune          func()
	afterCreate    func()
	outcomeUnknown bool
}

func (p *pruneBeforeCreate) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	if p.prune != nil {
		p.prune()
	}

	md, err := p.Store.Create(ctx, tx, blockHeight, opts...)

	if p.afterCreate != nil {
		p.afterCreate()
	}

	if p.outcomeUnknown {
		return nil, utxo.MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("batch did not complete in time"))
	}

	return md, err
}

// TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction: a rebroadcast of
// a mined, fully spent transaction C is accepted by the spend without a write,
// because its parent's output already names C, or because the parent is gone and
// C's record is there. When the pruner deletes C after that spend and before the
// create, the create finds no record and writes C again, with outputs a
// confirmed descendant consumed. Nothing rejects, so nothing compensates. The
// recheck after the create must answer the replay and remove the record it
// wrote, leaving the parent's historical spend in place.
//
// The rows:
//   - marker: the pruner marks the parent and deletes C in the window.
//   - parent pruned too: the parent is gone before the call, so the spend is
//     blessed on C's record, and the pruner deletes C in the window. The recheck
//     must not bless the parent on the record this call just wrote.
//   - idempotent retry: an earlier attempt spent C's input and never created C;
//     nothing is pruned, the recheck passes and C is created.
//   - other failure: the recheck fails for a reason that is not a replay (the
//     parent turned conflicting after the create), so C is kept and the caller
//     is told the outcome is unknown rather than told it succeeded.
func TestSpendAndCreateRecheckRemovesARecreatedPrunedTransaction(t *testing.T) {
	for _, tc := range []struct {
		name         string
		marker       bool
		parentPruned bool
		conflicting  bool
		wantErr      error
		wantGone     bool
	}{
		{name: "marker", marker: true, wantErr: errors.ErrUtxoSpendingTxPruned, wantGone: true},
		{name: "parent pruned too", parentPruned: true, wantErr: errors.ErrTxNotFound, wantGone: true},
		{name: "idempotent retry"},
		{name: "other failure", conflicting: true, wantErr: errors.ErrServiceUnavailable},
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

			if tc.marker || tc.parentPruned {
				// C existed, and the pruner removes it in the window.
				_, err = store.Create(ctx, child, 1000)
				require.NoError(t, err)

				if tc.parentPruned {
					require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))
				}

				wrapped.prune = func() {
					if tc.marker {
						_, insertErr := store.db.ExecContext(ctx,
							`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
							parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
						require.NoError(t, insertErr)
					}

					require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))
				}
			}

			if tc.conflicting {
				wrapped.afterCreate = func() {
					_, updErr := store.db.ExecContext(ctx, `UPDATE transactions SET conflicting = true WHERE hash = $1`, parent.TxIDChainHash()[:])
					require.NoError(t, updErr)
				}
			}

			_, _, err = utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, wrapped, child, 1000)

			_, getErr := store.Get(ctx, child.TxIDChainHash())

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr, "the replay must be answered")
			} else {
				require.NoError(t, err)
			}

			if tc.wantGone {
				require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated record must be removed again")
			} else {
				require.NoError(t, getErr, "the created record is kept")
			}

			if !tc.parentPruned {
				require.Equal(t, spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes(), outputSpendingData(t, ctx, store, parent, 0),
					"the parent keeps C's spend")
			}
		})
	}
}

// TestSpendAndCreateRecheckAfterAnUnknownCreate: a create whose outcome is
// unknown may still have written a pruned C again, so it is rechecked too, but
// without the proof a successful create gives that C was absent. The record is
// removed only when the marker answers and the record was never mined.
//
// The rows:
//   - marker, unmined record: the pruner marks the parent and deletes C in the
//     window, and the create writes C again. It is removed.
//   - marker, mined record: the pruner marks the parent and holds C back, so C
//     is still the mined original. It is kept.
//   - parent pruned, mined record: the parent is gone and C is live, so the
//     rebroadcast is blessed and the recheck answers the missing parent. C is
//     kept: a missing parent alone does not prove C was pruned.
func TestSpendAndCreateRecheckAfterAnUnknownCreate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deleteChild  bool
		parentPruned bool
		marker       bool
		wantPruned   bool
		wantGone     bool
	}{
		{name: "marker, unmined record", marker: true, deleteChild: true, wantPruned: true, wantGone: true},
		{name: "marker, mined record", marker: true},
		{name: "parent pruned, mined record", parentPruned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, _ := setup(ctx, t)

			parent := bt.NewTx()
			require.NoError(t, parent.From("4444444444444444444444444444444444444444444444444444444444444444", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			// C is mined and its spend of the parent is recorded.
			_, err = store.Spend(ctx, child, 1000)
			require.NoError(t, err)
			_, err = store.Create(ctx, child, 1000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 1000}))
			require.NoError(t, err)

			if tc.parentPruned {
				require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))
			}

			wrapped := &pruneBeforeCreate{Store: store, outcomeUnknown: true}
			wrapped.prune = func() {
				if tc.marker {
					_, insertErr := store.db.ExecContext(ctx,
						`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
						parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
					require.NoError(t, insertErr)
				}

				if tc.deleteChild {
					require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))
				}
			}

			_, spends, err := utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, wrapped, child, 1000)
			require.True(t, utxo.IsCreateOutcomeUnknown(err), "the outcome stays unknown, so the spends are kept")
			require.NotEmpty(t, spends)

			if tc.wantPruned {
				require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
			} else {
				require.NotErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
			}

			_, getErr := store.Get(ctx, child.TxIDChainHash())
			if tc.wantGone {
				require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated record must be removed")
			} else {
				require.NoError(t, getErr, "a mined record is never removed on this path")
			}
		})
	}
}
