package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetLockedReachesAMinedTransaction: both statements' own comments say the flag must reach
// BOTH homes of a transaction -- "the identity row is what a metadata read shows, the coin row
// is what the spend path reads" -- and both were written when a mined transaction still had an
// identity row. It does not now. minedRow.toMeta reads Locked and Conflicting off
// tx_mined.flags, which the move copied once and nothing updated afterwards, so a flag set
// after the stamp was invisible to Get: a transaction reporting itself unlocked forever, or
// locked forever, silently.
func TestSetLockedReachesAMinedTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, tx), "no identity row to carry the flag")

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "the membership row is what Get reads for a mined transaction")

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false))

	got, err = s.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.False(t, got.Locked, "and the release reaches it too")
}

// TestSetLockedStillReachesAMempoolTransaction pins the arm that already worked, so the added
// membership arm cannot be mistaken for a replacement.
func TestSetLockedStillReachesAMempoolTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100)
	require.NoError(t, err)

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked)
}

// TestSetLockedDoesNotTouchACollidingTransactionsCoins is the packed-key bound the coin UPDATE
// was missing. schema.go says it in its own words: "There is deliberately no index on txid:
// every by-txid access is a ukey range scan with a full-txid heap recheck. Any query filtering
// on txid without a ukey range bound is a review failure." This was that query, on the
// two-phase-commit path, one call per mempool transaction.
//
// The correctness of the answer never depended on the bound -- the full txid was already
// rechecked -- so this test pins the plan's premise rather than a wrong answer: a coin sharing
// the packed prefix under a different txid must be left alone, which it is either way, and the
// bound is what stops the planner reading the whole leaf partition to prove it.
func TestSetLockedDoesNotTouchACollidingTransactionsCoins(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100)
	require.NoError(t, err)

	other := insertCollidingCoin(t, s, ctx, tx, 0, 0)

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))

	var otherFlags int16
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT flags FROM utxo WHERE txid = $1`, other).Scan(&otherFlags))
	require.Equal(t, int16(0), otherFlags, "a 96-bit prefix collision is not the same transaction")
}

// TestSetConflictingReachesAMinedTransaction is the same defect in the other statement:
// setConflictingSQL's ident CTE updates tx_ident only, so marking a mined transaction
// conflicting set the coin bit and not the bit Get reports.
//
// It goes in at runConflictingPlan rather than through SetConflicting so that it pins THIS
// statement on its own. SetConflicting's first step, readConflictingInputs, used to read
// tx_ident alone and report a mined transaction as TxNotFound before the flag statement ever
// ran; that separate defect is fixed too, and
// TestSetConflictingReadsInputsFromAMinedTransaction in conflicting_test.go covers the whole
// path end to end.
func TestSetConflictingReachesAMinedTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, tx))

	named := []chainhash.Hash{*tx.TxIDChainHash()}
	inpoints := map[chainhash.Hash]subtree.TxInpoints{named[0]: subtree.NewTxInpoints()}
	plan := s.planConflicting(named, inpoints)

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	_, _, err = s.runConflictingPlan(ctx, dbTx, plan, true)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "the membership row is what Get reads for a mined transaction")
}
