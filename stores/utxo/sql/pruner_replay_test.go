package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestPrunedReplayAcceptedAfterUnspend pins the rule both stores share: a
// replay is refused only while the parent output still records the spender.
// Unspend clears spending_data and the spend height, so after an
// Unspend the replay is an ordinary spend of a free output and the pruned child
// is written again. Reaching this on a node needs a reorg deeper than the
// pruning retention; the PR lists it as a limit.
func TestPrunedReplayAcceptedAfterUnspend(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *Store

			if backend == "postgres" {
				store, ctx = setupPostgresStore(t)
			} else {
				store, _ = setup(ctx, t)
			}

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)
			require.NoError(t, store.SetBlockHeight(1000))

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err = store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
			require.NoError(t, err)

			grandchild := bt.NewTx()
			require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
			grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
			_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			svc, err := store.GetPrunerService()
			require.NoError(t, err)
			n, err := svc.Prune(ctx, 1300, "unspend-then-replay")
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			// Roll the parent output's spend back through the real Unspend, the
			// path unspendWithRetry takes after a failed create.
			utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
			require.NoError(t, err)
			require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			}}))

			var stillSpent bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT spending_data IS NOT NULL FROM outputs WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parent.TxIDChainHash()[:]).Scan(&stillSpent))
			require.False(t, stillSpent, "fixture: the unspend must actually have cleared spending_data")

			_, _, err = store.SpendAndCreate(ctx, child, 1200)
			require.NoError(t, err, "with the output no longer recording the child, there is no replay to refuse")

			var exists bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
			require.True(t, exists, "the replay writes the child again, as on Aerospike")
		})
	}
}

// TestPrunedReplaySpentAfterReplacementSpend: the parent output was rolled back
// and then taken by a replacement transaction. The output records the
// replacement, not the pruned child, so the replay is answered as a double
// spend, as the Lua spend answers it.
func TestPrunedReplaySpentAfterReplacementSpend(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *Store

			if backend == "postgres" {
				store, ctx = setupPostgresStore(t)
			} else {
				store, _ = setup(ctx, t)
			}

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)
			require.NoError(t, store.SetBlockHeight(1000))

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err = store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
			require.NoError(t, err)

			grandchild := bt.NewTx()
			require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
			grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
			_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			svc, err := store.GetPrunerService()
			require.NoError(t, err)
			n, err := svc.Prune(ctx, 1300, "replace-then-replay")
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
			require.NoError(t, err)
			require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			}}))

			replacement := bt.NewTx()
			require.NoError(t, replacement.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			replacement.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, replacement.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2999))
			_, _, err = store.SpendAndCreate(ctx, replacement, 1200)
			require.NoError(t, err, "fixture: the replacement takes the output")

			_, _, err = store.SpendAndCreate(ctx, child, 1200)
			require.ErrorIs(t, err, errors.ErrSpent, "the output records the replacement, so the replay is a double spend")
			require.NotErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

			var exists bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
			require.False(t, exists)
		})
	}
}

// twoParentPrunedChild builds P and Q (two outputs each, output 1 unspent so
// both survive), C spending P:0 and Q:0, and G spending C's only output, all
// mined; then prunes C, which writes C's marker on both P and Q.
func twoParentPrunedChild(t *testing.T, ctx context.Context, store *Store) (p, q, child *bt.Tx) {
	t.Helper()

	parents := make([]*bt.Tx, 2)

	for i := range parents {
		parent := bt.NewTx()
		require.NoError(t, parent.From("111111111111111111111111111111111111111111111111111111111111111"+string(rune('1'+i)), 0, "51", 30000))
		parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		parents[i] = parent
	}

	p, q = parents[0], parents[1]

	child = bt.NewTx()
	for _, parent := range parents {
		require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	}

	for i := range child.Inputs {
		child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err := store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{p.TxIDChainHash(), q.TxIDChainHash(), child.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
	require.NoError(t, err)

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	n, err := svc.Prune(ctx, 1300, "two-parent-child")
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "fixture: only the child is pruned")

	return p, q, child
}

func forEachBackend(t *testing.T, body func(t *testing.T, ctx context.Context, store *Store)) {
	t.Helper()

	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var store *Store

			if backend == "postgres" {
				store, ctx = setupPostgresStore(t)
			} else {
				store, _ = setup(ctx, t)
			}

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)
			require.NoError(t, store.SetBlockHeight(1000))

			body(t, ctx, store)
		})
	}
}

func outputSpendingData(t *testing.T, ctx context.Context, store *Store, parent *bt.Tx, vout uint32) []byte {
	t.Helper()

	var sd []byte
	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT spending_data FROM outputs WHERE idx = $2 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
		parent.TxIDChainHash()[:], vout).Scan(&sd))

	return sd
}

// TestPrunedReplayFrozenAfterUnspend: the rolled-back output has been frozen.
// It no longer records the pruned child, so its marker refuses nothing and the
// replay gets the freeze's answer on that input, as on Aerospike. Q:0 still
// records the child, so that input is still refused as a pruned replay.
func TestPrunedReplayFrozenAfterUnspend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		p, q, child := twoParentPrunedChild(t, ctx, store)

		utxoHash, err := util.UTXOHashFromOutput(p.TxIDChainHash(), p.Outputs[0], 0)
		require.NoError(t, err)

		spend := &utxo.Spend{TxID: p.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash, SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0)}
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{spend}))
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: p.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}}, test.CreateBaseTestSettings(t)))

		spends, err := store.Spend(ctx, child, 1200)
		require.Error(t, err)

		byParent := make(map[chainhash.Hash]error, len(spends))
		for _, s := range spends {
			byParent[*s.TxID] = s.Err
		}

		require.Len(t, byParent, 2)
		// Spend chains a transaction's input errors together, so P's error also
		// carries Q's refusal; under the old rule P answered only as pruned.
		require.ErrorIs(t, byParent[*p.TxIDChainHash()], errors.ErrFrozen, "P:0 is frozen and no longer records the pruned child")
		require.ErrorIs(t, byParent[*q.TxIDChainHash()], errors.ErrUtxoSpendingTxPruned, "Q:0 still records the pruned child")
	})
}

// TestSpendDoesNotBlessWithRequireParentOutputs: "parent not found, but the
// spending transaction exists" is treated as a transaction validated before its
// parent was pruned, and the error is cleared. Not with RequireParentOutputs,
// the second wave of a below-checkpoint list, whose first wave may have written
// the record itself.
func TestSpendDoesNotBlessWithRequireParentOutputs(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		parent := bt.NewTx()
		require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
		parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
		_, _, err = store.SpendAndCreate(ctx, child, 1000)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()},
			utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		// The parent is gone; the child's record remains.
		require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))

		_, err = store.Spend(ctx, child, 1200)
		require.NoError(t, err, "control: a pre-existing child with a pruned parent is blessed")

		_, err = store.Spend(ctx, child, 1200, utxo.IgnoreFlags{RequireParentOutputs: true})
		require.ErrorIs(t, err, errors.ErrTxNotFound, "with RequireParentOutputs the record is not proof of prior validation")
	})
}

// TestPrunerMarksOnlyTheChildThatHeldTheSpend: a conflicting loser names the
// same outpoint in its inputs as the winner but never held it. Only the child
// the output records as its spender may be marked, as on Aerospike: a marker
// for the loser allows its first fresh spend after Unspend, but refuses a later
// idempotent spend once the output records the loser.
func TestPrunerMarksOnlyTheChildThatHeldTheSpend(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		t.Run(fmt.Sprintf("defensive=%v", defensive), func(t *testing.T) {
			testPrunerMarksOnlyTheChildThatHeldTheSpend(t, defensive)
		})
	}
}

func testPrunerMarksOnlyTheChildThatHeldTheSpend(t *testing.T, defensive bool) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		store.settings.Pruner.UTXODefensiveEnabled = defensive

		parent := bt.NewTx()
		require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
		parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		// The winner takes P:0 and is later fully spent, so it becomes a prune
		// candidate and legitimately earns a marker.
		winner := bt.NewTx()
		require.NoError(t, winner.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		winner.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, winner.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
		_, _, err = store.SpendAndCreate(ctx, winner, 1000)
		require.NoError(t, err)

		// The loser asks for the same outpoint and never gets it. It is created
		// (as a conflicting record would be) and tombstoned alongside the winner.
		loser := bt.NewTx()
		require.NoError(t, loser.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, loser.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2999))
		_, err = store.Create(ctx, loser, 1000)
		require.NoError(t, err)

		// The loser is mined as well: the pruner keeps an unmined record that is not
		// conflicting, so an unmined loser would not be a candidate and the test
		// would pass whatever the marker query did with it.
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), winner.TxIDChainHash(), loser.TxIDChainHash()},
			utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		grandchild := bt.NewTx()
		require.NoError(t, grandchild.From(winner.TxID(), 0, winner.Outputs[0].LockingScript.String(), winner.Outputs[0].Satoshis))
		grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
		_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
			utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
		require.NoError(t, err)

		// Tombstone the loser too, so both are candidates in the same cycle.
		_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1100 WHERE hash = $1", loser.TxIDChainHash()[:])
		require.NoError(t, err)

		svc, err := store.GetPrunerService()
		require.NoError(t, err)
		pruned, err := svc.Prune(ctx, 1300, "conflicting-loser")
		require.NoError(t, err)
		require.GreaterOrEqual(t, pruned, int64(2), "fixture: the loser and the winner must both be pruned")
		require.False(t, rowExists(ctx, t, store, *loser.TxIDChainHash()), "fixture: the loser must be a candidate")

		var loserMarkers int
		require.NoError(t, store.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM deleted_children WHERE child_hash = $1", loser.TxIDChainHash()[:]).Scan(&loserMarkers))
		require.Equal(t, 0, loserMarkers,
			"the loser never held P:0; a stale marker would reject a later idempotent spend after it wins the output")

		var winnerMarkers int
		require.NoError(t, store.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM deleted_children WHERE child_hash = $1", winner.TxIDChainHash()[:]).Scan(&winnerMarkers))
		require.Equal(t, 1, winnerMarkers, "the child that actually held the spend must still be marked")

		// End state that matters: with the winner's spend released, the loser's
		// fresh spend of the now-free output must be accepted.
		utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
		require.NoError(t, err)
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
			TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash,
			SpendingData: spendpkg.NewSpendingData(winner.TxIDChainHash(), 0),
		}}))

		_, err = store.Spend(ctx, loser, 1400)
		require.NoError(t, err, "a transaction that never held the output must be able to spend it once it is free")
	})
}

// TestOrphanedSpendIsReversedOnTheNextAttempt: the store leaves partial spends
// committed when a call fails on an error that is not rollback-class, so an
// attempt that writes one input and then fails on a MISSING parent leaves that
// input spent by a transaction it never created. On the next attempt that input
// reads as an idempotent match. Holding every idempotent match back from the
// rollback made the orphan permanent: the output stayed spent by a transaction
// the store does not hold, and its next legitimate spender was refused naming a
// txid this node has never seen. Found by oskarszoon.
//
// Only a pruned-replay rejection makes an idempotent match historical, because
// only a transaction this store pruned was mined, fully spent and buried. Every
// other rejection, here a frozen output, leaves it reversible.
func TestOrphanedSpendIsReversedOnTheNextAttempt(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := bt.NewTx()
		require.NoError(t, p.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
		p.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, p.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		// Q is NOT created yet.
		q := bt.NewTx()
		require.NoError(t, q.From("2222222222222222222222222222222222222222222222222222222222222222", 0, "51", 30000))
		q.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, q.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		child := bt.NewTx()
		for _, parent := range []*bt.Tx{p, q} {
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		}

		for i := range child.Inputs {
			child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		}

		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))

		// The orphan: an earlier attempt wrote P:0 and then failed on an error
		// that does not roll back (a transient one). A missing parent used to be
		// such an error; it now rolls back, so the orphan is written directly.
		_, err = store.db.ExecContext(ctx,
			"UPDATE outputs SET spending_data = $2, spending_height = 1000 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
			p.TxIDChainHash()[:], spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes())
		require.NoError(t, err)

		require.Equal(t, spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes(),
			outputSpendingData(t, ctx, store, p, 0),
			"fixture: P:0 is left spent by a transaction the store never created")

		var childExists bool
		require.NoError(t, store.db.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&childExists))
		require.False(t, childExists, "fixture: the spender was never created")

		// Q arrives, with its output frozen, so the next attempt fails on a
		// rollback-class error instead.
		_, err = store.Create(ctx, q, 1000)
		require.NoError(t, err)

		q0Hash, err := util.UTXOHashFromOutput(q.TxIDChainHash(), q.Outputs[0], 0)
		require.NoError(t, err)
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: q.TxIDChainHash(), Vout: 0, UTXOHash: q0Hash}}, test.CreateBaseTestSettings(t)))

		// Attempt 2: P:0 now reads as an idempotent match and Q:0 fails.
		_, err = store.Spend(ctx, child, 1000)
		require.Error(t, err)

		require.Nil(t, outputSpendingData(t, ctx, store, p, 0),
			"the orphaned spend must be reversed, not held back as if it were historical")

		// End state: P:0 is spendable by a real transaction again.
		replacement := bt.NewTx()
		require.NoError(t, replacement.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
		replacement.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, replacement.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3999))
		_, _, err = store.SpendAndCreate(ctx, replacement, 1001)
		require.NoError(t, err, "P:0 was never legitimately consumed and must be spendable")
	})
}

// TestSpendRollbackSetForRequireParentOutputs: a RequireParentOutputs call
// reverses only what it wrote, and only when an input failed with a validation
// error. An idempotent match is that record's own spend and is never reversed,
// and a call that failed only on a transient error leaves its writes for the
// retry to match.
func TestSpendRollbackSetForRequireParentOutputs(t *testing.T) {
	written := &utxo.Spend{Vout: 0}
	idempotent := &utxo.Spend{Vout: 1}
	spent := &utxo.Spend{Vout: 2, Err: errors.NewUtxoSpentError(chainhash.Hash{}, 2, chainhash.Hash{}, nil)}
	timedOut := &utxo.Spend{Vout: 3, Err: errors.NewServiceUnavailableError("batch operation timed out")}

	require.Equal(t, []*utxo.Spend{written},
		spendRollbackSet([]*utxo.Spend{written, idempotent, spent, timedOut}, []*utxo.Spend{written}),
		"a validation failure reverses what the call wrote and nothing else")

	require.Nil(t,
		spendRollbackSet([]*utxo.Spend{written, timedOut}, []*utxo.Spend{written}),
		"a call that failed only on a transient error reverses nothing")
}

// TestAbortedSpendKeepsAnIdempotentMatchWhileAnInputIsUnanswered: tx C spends
// A:0, B:0 and E:0. A's batch never answers, so its wait times out, B:0 already
// records C (an idempotent match) and E:0 answers ErrSpent. A's real answer is
// unknown and may be the marker hit, so B:0 may be a confirmed historical
// spend: the rollback must leave it. The Aerospike store pins the same case in
// TestAbortPathNeverReversesAnIdempotentMatchWhileAnInputIsUnanswered.
//
// The per-input wait ends two ways, each with its own arm that counts the input as
// unresolved: the wait timeout, and the caller's context being cancelled. Both are run.
func TestAbortedSpendKeepsAnIdempotentMatchWhileAnInputIsUnanswered(t *testing.T) {
	for _, byCancel := range []bool{false, true} {
		name := "timeout"
		if byCancel {
			name = "context_cancel"
		}

		t.Run(name, func(t *testing.T) {
			testAbortedSpendKeepsIdempotentMatch(t, byCancel)
		})
	}
}

func TestAbandonedQueuedSpendNeverWrites(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		parent := replayParent(t, ctx, store, strings.Repeat("11", 32))
		child := replaySpender(t, parent)
		store.settings.UtxoStore.SpendWaitTimeout = 100 * time.Millisecond
		entered, release, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		store.spendBatcher.Close()
		store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(batch []*batchSpend) {
			close(entered)
			<-release // The serial callback has not handed these items to the store yet.
			store.sendSpendBatch(batch)
			close(settled)
		}, false)
		callDone := make(chan error, 1)
		go func() {
			_, _, err := store.SpendAndCreate(ctx, child, 1000)
			callDone <- err
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("spend was not queued")
		}
		select {
		case err := <-callDone:
			require.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("queued caller must be able to abandon an item before execution")
		}
		close(release)
		select {
		case <-settled:
		case <-time.After(5 * time.Second):
			t.Fatal("released callback did not settle")
		}
		require.Empty(t, outputSpendingData(t, ctx, store, parent, 0), "a queued spend canceled before execution must never reach the database")
	})
}

func TestAbandonedQueuedSpendDoesNotCancelItsBatchPeer(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		a := replaySpender(t, replayParent(t, ctx, store, strings.Repeat("11", 32)))
		parentB := replayParent(t, ctx, store, strings.Repeat("22", 32))
		b := replaySpender(t, parentB)
		store.settings.UtxoStore.SpendWaitTimeout = time.Minute
		entered, release := make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		store.spendBatcher.Close()
		store.spendBatcher = batcher.NewWithPool(2, time.Minute, func(batch []*batchSpend) {
			close(entered)
			<-release
			store.sendSpendBatch(batch)
		}, false)
		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		aDone, bDone := make(chan error, 1), make(chan error, 1)
		go func() { _, err := store.Spend(callCtx, a, 1000); aDone <- err }()
		go func() { _, err := store.Spend(ctx, b, 1000); bDone <- err }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("both spends must be queued together")
		}
		cancel()
		select {
		case err := <-aDone:
			require.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("queued canceled caller did not return")
		}
		close(release)
		select {
		case err := <-bDone:
			require.NoError(t, err, "discarding an abandoned queued item must not cancel a live peer")
		case <-time.After(5 * time.Second):
			t.Fatal("live batch peer did not return")
		}
		require.NotEmpty(t, outputSpendingData(t, ctx, store, parentB, 0))
	})
}

func testAbortedSpendKeepsIdempotentMatch(t *testing.T, byCancel bool) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		newParent := func(seed string) *bt.Tx {
			parent := bt.NewTx()
			require.NoError(t, parent.From(seed, 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			return parent
		}

		a := newParent("1111111111111111111111111111111111111111111111111111111111111111")
		b := newParent("2222222222222222222222222222222222222222222222222222222222222222")
		e := newParent("3333333333333333333333333333333333333333333333333333333333333333")

		spender := func(parents ...*bt.Tx) *bt.Tx {
			tx := bt.NewTx()
			for _, parent := range parents {
				require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for i := range tx.Inputs {
				tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

			return tx
		}

		other := spender(e)
		_, _, err := store.SpendAndCreate(ctx, other, 1000)
		require.NoError(t, err, "fixture: E:0 is spent by another transaction")

		c := spender(a, b, e)

		recorded := spendpkg.NewSpendingData(c.TxIDChainHash(), 1).Bytes()
		_, err = store.db.ExecContext(ctx,
			"UPDATE outputs SET spending_data = $2, spending_height = 1000 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
			b.TxIDChainHash()[:], recorded)
		require.NoError(t, err)

		// A's spend is never answered. The timeout variant lets the per-input wait run
		// out. The cancel variant waits a minute and cancels the caller's context with A
		// in flight, after B and E have been answered.
		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		store.settings.UtxoStore.SpendWaitTimeout = 500 * time.Millisecond
		if byCancel {
			store.settings.UtxoStore.SpendWaitTimeout = time.Minute
		}

		store.spendBatcher = batcher.NewWithPool(store.settings.UtxoStore.SpendBatcherSize, 10*time.Millisecond, func(batch []*batchSpend) {
			answered := make([]*batchSpend, 0, len(batch))

			for _, item := range batch {
				if !item.spend.TxID.IsEqual(a.TxIDChainHash()) {
					answered = append(answered, item)
				}
			}

			if len(answered) > 0 {
				store.sendSpendBatch(answered)
			}

			if byCancel {
				// Let the workers of B and E take their answers first, so only A's wait
				// ends on the cancel.
				time.Sleep(300 * time.Millisecond)
				cancel()
			}
		}, false)

		_, err = store.Spend(callCtx, c, 1000)
		require.Error(t, err)

		require.Equal(t, recorded, outputSpendingData(t, ctx, store, b, 0),
			"an idempotent match must not be reversed while an input is unanswered")
	})
}

// TestPrunerWritesNoMarkerOnAParentItDeletes: a marker on a parent the same prune deletes
// would be removed again by the cascade before the commit, after a foreign-key lock and
// a heap write for nothing, and it is common because chains mined together fall due in
// the same block. A trigger records every marker row the prune inserts, because the
// cascade hides the doomed ones from the final state. Only the marker on the surviving
// parent may be written.
func TestPrunerWritesNoMarkerOnAParentItDeletes(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		const address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

		newParent := func(prev string, outputs int) *bt.Tx {
			tx := bt.NewTx()
			require.NoError(t, tx.From(prev, 0, "51", 30000))
			tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})

			for i := 0; i < outputs; i++ {
				require.NoError(t, tx.PayToAddress(address, 4000))
			}

			_, err := store.Create(ctx, tx, 1000)
			require.NoError(t, err)

			return tx
		}

		spendOf := func(parent *bt.Tx) *bt.Tx {
			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress(address, 3000))

			_, _, err := store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)

			return child
		}

		// q survives (one output stays unspent) and k leaves a marker on it. g is spent by h,
		// and both fall due in the same prune, so the marker h would leave on g is doomed.
		q := newParent("1111111111111111111111111111111111111111111111111111111111111111", 2)
		g := newParent("2222222222222222222222222222222222222222222222222222222222222222", 1)
		k := spendOf(q)
		h := spendOf(g)

		_, err := store.SetMinedMulti(ctx,
			[]*chainhash.Hash{q.TxIDChainHash(), g.TxIDChainHash(), k.TxIDChainHash(), h.TxIDChainHash()},
			utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		for _, tx := range []*bt.Tx{g, k, h} {
			_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1100 WHERE hash = $1", tx.TxIDChainHash()[:])
			require.NoError(t, err)
		}

		_, err = store.db.ExecContext(ctx, "CREATE TABLE marker_insert_log (parent_id BIGINT, child_hash BYTEA)")
		require.NoError(t, err)

		if store.engine == "postgres" {
			_, err = store.db.ExecContext(ctx, `CREATE FUNCTION log_marker_insert() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN INSERT INTO marker_insert_log VALUES (NEW.parent_id, NEW.child_hash); RETURN NEW; END $$`)
			require.NoError(t, err)
			_, err = store.db.ExecContext(ctx, `CREATE TRIGGER log_marker_insert AFTER INSERT ON deleted_children
				FOR EACH ROW EXECUTE FUNCTION log_marker_insert()`)
		} else {
			_, err = store.db.ExecContext(ctx, `CREATE TRIGGER log_marker_insert AFTER INSERT ON deleted_children
				BEGIN INSERT INTO marker_insert_log VALUES (NEW.parent_id, NEW.child_hash); END`)
		}

		require.NoError(t, err)

		svc, err := store.GetPrunerService()
		require.NoError(t, err)
		n, err := svc.Prune(ctx, 1300, "doomed-markers")
		require.NoError(t, err)
		require.Equal(t, int64(3), n, "fixture: g, k and h are the candidates")

		var qID int64

		require.NoError(t, store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", q.TxIDChainHash()[:]).Scan(&qID))

		rows, err := store.db.QueryContext(ctx, "SELECT parent_id, child_hash FROM marker_insert_log")
		require.NoError(t, err)

		defer rows.Close()

		type marker struct {
			parent int64
			child  string
		}

		var written []marker

		for rows.Next() {
			var m marker

			var child []byte

			require.NoError(t, rows.Scan(&m.parent, &child))
			m.child = string(child)

			written = append(written, m)
		}

		require.NoError(t, rows.Err())
		require.Equal(t, []marker{{parent: qID, child: string(k.TxIDChainHash()[:])}}, written,
			"only k's marker on the surviving parent q may be written, not h's marker on the deleted g")
	})
}

// replayParent stores a parent with one output, unspent.
func replayParent(t *testing.T, ctx context.Context, store *Store, seed string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(seed, 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	return parent
}

// replaySpender builds a transaction that spends output 0 of each parent, in order. It stores
// nothing.
func replaySpender(t *testing.T, parents ...*bt.Tx) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for _, parent := range parents {
		require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	}

	for i := range tx.Inputs {
		tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

// plantHistoricalSpend makes output 0 of parent record spender at vin with no spend height, as a
// build that did not store heights left it. The spender has no record, so a spend of the same
// call is a replay of a pruned transaction.
func plantHistoricalSpend(t *testing.T, ctx context.Context, store *Store, parent, spender *bt.Tx, vin int) {
	t.Helper()

	_, err := store.db.ExecContext(ctx,
		"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
		parent.TxIDChainHash()[:], spendpkg.NewSpendingData(spender.TxIDChainHash(), vin).Bytes())
	require.NoError(t, err)
}

func rollbackOutcomeCount(outcome string) float64 {
	return testutil.ToFloat64(prometheusUtxoPartialSpendRollbacks.WithLabelValues(outcome))
}

// TestSpendBlessesInputsOfParentsThatAreGone: C is already stored, and three of its parents
// have been pruned, so spending them answers TxNotFound. Each such input counts as succeeded
// because C's record proves it was validated, but it wrote nothing and has no row to reverse,
// so it goes into neither rollback list. A fourth input fails on an output another
// transaction spent, so the call fails and takes the rollback path: with the blessed inputs
// left out there is nothing this call wrote, and the rollback must not even start. Spends
// answered one at a time make the later blessed inputs take the memo of the first, the
// other arm of the bless.
func TestSpendBlessesInputsOfParentsThatAreGone(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		parents := make([]*bt.Tx, 0, 3)
		for _, seed := range []string{"4", "5", "6"} {
			parents = append(parents, replayParentNotStored(t, strings.Repeat(seed, 64)))
		}

		f := replayParent(t, ctx, store, "7777777777777777777777777777777777777777777777777777777777777777")

		other := replaySpender(t, f)
		_, _, err := store.SpendAndCreate(ctx, other, 1000)
		require.NoError(t, err, "fixture: F:0 is spent by another transaction")

		c := replaySpender(t, append(parents, f)...)

		_, err = store.Create(ctx, c, 1000)
		require.NoError(t, err, "fixture: C is stored")

		store.settings.UtxoStore.SpendWaitTimeout = time.Minute
		store.spendBatcher = batcher.NewWithPool(store.settings.UtxoStore.SpendBatcherSize, 10*time.Millisecond, func(batch []*batchSpend) {
			for i, item := range batch {
				if i > 0 {
					time.Sleep(150 * time.Millisecond)
				}

				store.sendSpendBatch([]*batchSpend{item})
			}
		}, false)

		before := rollbackOutcomeCount(utxo.RollbackOutcomeSpenderExists)

		spends, err := store.Spend(ctx, c, 1000)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrSpent)

		for i := range parents {
			require.NoError(t, spends[i].Err, "input %d: C's record blesses a spend of a parent that is gone", i)
		}

		require.Equal(t, before, rollbackOutcomeCount(utxo.RollbackOutcomeSpenderExists),
			"a blessed input wrote nothing, so no rollback may be attempted for it")
	})
}

// replayParentNotStored builds a parent that is never stored, whose txid a spender can name.
func replayParentNotStored(t *testing.T, seed string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(seed, 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	return parent
}

// TestRequireParentOutputsRollsBackWhatItWrote: the second wave of DefaultSpendAndCreateMulti
// spends a record its first wave stored, so the existence probe would always find the spender
// and never fire. Such a call takes spendRollbackSet's rule instead: when an input fails, what
// the call wrote is reversed. X:0 is written, Y:0 is spent by another transaction, and X:0
// must be free again.
func TestRequireParentOutputsRollsBackWhatItWrote(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		x := replayParent(t, ctx, store, "1111111111111111111111111111111111111111111111111111111111111111")
		y := replayParent(t, ctx, store, "2222222222222222222222222222222222222222222222222222222222222222")

		_, _, err := store.SpendAndCreate(ctx, replaySpender(t, y), 1000)
		require.NoError(t, err, "fixture: Y:0 is spent by another transaction")

		c := replaySpender(t, x, y)

		_, err = store.Create(ctx, c, 1000)
		require.NoError(t, err, "fixture: the first wave stored C")

		_, err = store.Spend(ctx, c, 1000, utxo.IgnoreFlags{RequireParentOutputs: true})
		require.ErrorIs(t, err, errors.ErrSpent)

		require.Empty(t, outputSpendingData(t, ctx, store, x, 0),
			"the spend of X:0 this call wrote is reversed, although C is stored")
	})
}

// TestTransientLockDoesNotHideAPrunedReplay: C spends L:0, whose parent is locked, M:0, which
// records C and carries C's marker, and F:0, which is free and written by this call. The lock
// is a transient window and would suppress the rollback, but the marker means C can never
// win: a replay of a pruned transaction meets it on every attempt. F:0 is spent by a
// transaction that is never created and must be reversed, or its legitimate spender gets
// ErrSpent naming a txid this node never stored.
func TestTransientLockDoesNotHideAPrunedReplay(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		l := replayParent(t, ctx, store, "1111111111111111111111111111111111111111111111111111111111111111")
		m := replayParent(t, ctx, store, "2222222222222222222222222222222222222222222222222222222222222222")
		f := replayParent(t, ctx, store, "3333333333333333333333333333333333333333333333333333333333333333")

		c := replaySpender(t, l, m, f)

		plantHistoricalSpend(t, ctx, store, m, c, 1)

		require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*l.TxIDChainHash()}, true))

		spends, err := store.Spend(ctx, c, 1000)
		require.Error(t, err)
		require.ErrorIs(t, spends[0].Err, errors.ErrTxLocked, "fixture: L is locked")
		require.ErrorIs(t, spends[1].Err, errors.ErrUtxoSpendingTxPruned, "fixture: M carries the marker")

		require.Empty(t, outputSpendingData(t, ctx, store, f, 0),
			"the spend of F:0 this call wrote is reversed: a pruned replay can never win, whatever else is locked")
		require.NotEmpty(t, outputSpendingData(t, ctx, store, m, 0), "M:0 is a confirmed spend and stays")
	})
}

func TestHasUnwinnableFailure(t *testing.T) {
	require.True(t, hasUnwinnableFailure([]*utxo.Spend{
		{Err: errors.NewTxLockedError("locked")},
		{Err: errors.NewUtxoSpendingTxPrunedError("pruned")},
	}), "a pruned-replay refusal is unwinnable")
	require.False(t, hasUnwinnableFailure([]*utxo.Spend{{Err: errors.NewTxLockedError("locked")}}), "a lock alone is transient")
	require.False(t, hasUnwinnableFailure([]*utxo.Spend{nil, {}}))
}

// TestRefusedReplayLeavesTheParentsStampAlone: an outsider can resubmit a pruned transaction
// whose parent survives. The refusal must come before the idempotent bookkeeping, which runs
// setDAH on the parent and moves its delete_at_height forward: that write is what the caller
// controls, and a refused replay has to leave the parent as it was. Runs the bulk path and the
// per-row path.
func TestRefusedReplayLeavesTheParentsStampAlone(t *testing.T) {
	for _, bulk := range []bool{true, false} {
		t.Run(fmt.Sprintf("bulk=%v", bulk), func(t *testing.T) {
			forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
				store.settings.UtxoStore.BatchSQLOperations = bulk

				p := replayParent(t, ctx, store, "1111111111111111111111111111111111111111111111111111111111111111")
				c := replaySpender(t, p)

				_, err := store.Spend(ctx, c, 1000)
				require.NoError(t, err, "fixture: C spends P:0")

				_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{p.TxIDChainHash()},
					utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
				require.NoError(t, err)

				plantHistoricalSpend(t, ctx, store, p, c, 0)

				stamp := readDAH(ctx, t, store, *p.TxIDChainHash())
				require.NotNil(t, stamp, "fixture: P is fully spent and mined, so it carries a stamp")

				require.NoError(t, store.SetBlockHeight(2000))

				// A replay the marker refuses at the read never reaches the write. The
				// per-row path would otherwise refuse it at the UPDATE's marker re-check.
				writes := 0
				spendBeforeWrite = func() { writes++ }

				t.Cleanup(func() { spendBeforeWrite = nil })

				spends, err := store.Spend(ctx, c, 2000)
				spendBeforeWrite = nil

				require.Error(t, err)
				require.ErrorIs(t, spends[0].Err, errors.ErrUtxoSpendingTxPruned)
				require.Zero(t, writes, "a replay refused at the read must not reach the write")

				require.Equal(t, stamp, readDAH(ctx, t, store, *p.TxIDChainHash()),
					"a refused replay must not move the parent's delete_at_height")
			})
		})
	}
}

// TestSetDAHOfARowThePruneDeletedHasNothingToStamp: a row the pruner deleted before setDAH reads
// it, or while setDAH waited for its lock, has no stamp to recompute. setDAH answers nil and
// setDAHIfPresent says the row is gone, so a caller never fails a whole batch for one parent.
func TestSetDAHOfARowThePruneDeletedHasNothingToStamp(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		floorRetention(t, store)

		_, err := store.Create(ctx, tests.ParentTx, 1000)
		require.NoError(t, err)

		var id int
		require.NoError(t, store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", tests.ParentTx.TxIDChainHash()[:]).Scan(&id))

		inTxn := func(body func(txn *sql.Tx)) {
			txn, beginErr := store.db.BeginTx(ctx, nil)
			require.NoError(t, beginErr)

			defer func() { _ = txn.Rollback() }()

			body(txn)
		}

		inTxn(func(txn *sql.Tx) {
			present, setErr := store.setDAHIfPresent(ctx, txn, id)
			require.NoError(t, setErr)
			require.True(t, present, "a row that is there is present")
		})

		_, err = store.db.ExecContext(ctx, "DELETE FROM transactions WHERE id = $1", id)
		require.NoError(t, err)

		inTxn(func(txn *sql.Tx) {
			present, setErr := store.setDAHIfPresent(ctx, txn, id)
			require.NoError(t, setErr)
			require.False(t, present, "a deleted row is reported gone, not as an error")
		})

		inTxn(func(txn *sql.Tx) {
			require.NoError(t, store.setDAH(ctx, txn, id), "setDAH has nothing to stamp on a deleted row")
		})
	})
}

// countedPruneConn measures client writes without changing the PostgreSQL protocol.
type countedPruneConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c *countedPruneConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// TestPostgresPruneUsesOneWireBatch keeps pruning's network cost independent of
// its six ordered statements. One other write clears stale unmined heights.
func TestPostgresPruneUsesOneWireBatch(t *testing.T) {
	testPostgresPruneWireBatch(t, pgx.QueryExecModeExec, 2, "")
}

func TestPostgresPruneWithCachedStatements(t *testing.T) {
	testPostgresPruneWireBatch(t, pgx.QueryExecModeCacheStatement, 0, "")
}

func TestPostgresPruneBatchSelectsReadCommitted(t *testing.T) {
	for _, isolation := range []string{"repeatable read", "serializable"} {
		t.Run(isolation, func(t *testing.T) {
			testPostgresPruneWireBatch(t, pgx.QueryExecModeExec, 2, isolation)
		})
	}
}

func testPostgresPruneWireBatch(t *testing.T, mode pgx.QueryExecMode, wantWrites int64, defaultIsolation string) {
	t.Helper()
	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false
	want := seedPruneTimingStore(ctx, t, store, 0, 4, 2)
	if defaultIsolation != "" {
		_, err := store.db.ExecContext(ctx, `CREATE FUNCTION require_prune_read_committed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF current_setting('transaction_isolation') <> 'read committed' THEN
		RAISE EXCEPTION 'pruning requires READ COMMITTED';
	END IF;
	RETURN OLD;
END $$`)
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx, `CREATE TRIGGER require_prune_read_committed BEFORE DELETE ON transactions
FOR EACH ROW EXECUTE FUNCTION require_prune_read_committed()`)
		require.NoError(t, err)
	}

	connection, err := store.db.Conn(ctx)
	require.NoError(t, err)
	var config *pgx.ConnConfig
	require.NoError(t, connection.Raw(func(raw any) error {
		driver, ok := raw.(*stdlib.Conn)
		require.True(t, ok)
		config = driver.Conn().Config()
		return nil
	}))
	require.NoError(t, connection.Close())
	config.DefaultQueryExecMode = mode
	if defaultIsolation != "" {
		config.RuntimeParams["default_transaction_isolation"] = defaultIsolation
	}

	var writes atomic.Int64
	dial := config.DialFunc
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &countedPruneConn{Conn: conn, writes: &writes}, nil
	}
	pool := stdlib.OpenDB(*config, stdlib.OptionShouldPing(func(context.Context, stdlib.ShouldPingParams) bool { return false }))
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	pool.SetMaxOpenConns(1)
	require.NoError(t, pool.PingContext(ctx))
	service, err := sqlpruner.NewService(store.settings, sqlpruner.Options{
		Logger: store.logger, DB: usql.WrapDB(pool), Ctx: ctx, Engine: "postgres",
	})
	require.NoError(t, err)

	writes.Store(0)
	deleted, err := service.Prune(ctx, 1300, "wire-batch")
	sent := writes.Load()
	require.NoError(t, err)
	require.EqualValues(t, want, deleted)
	if wantWrites > 0 {
		require.Equal(t, wantWrites, sent, "one cleanup query and one atomic pruning batch")
	} else {
		require.Greater(t, sent, int64(2), "cached statements retain the separate transaction path")
	}
	var markers int
	require.NoError(t, pool.QueryRowContext(ctx, "SELECT count(*) FROM deleted_children").Scan(&markers))
	require.Equal(t, 8, markers, "each of four children leaves markers on its two surviving parents")
	if defaultIsolation != "" {
		var isolation string
		require.NoError(t, pool.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&isolation))
		require.Equal(t, defaultIsolation, isolation, "pruning must not change the session's isolation")
	}
}
