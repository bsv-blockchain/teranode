package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestGetConflictingChildrenWalksTheNotedCone.
//
// A transaction that loses a double-spend race is recorded on the PARENT whose UTXO it wanted,
// because that is the only route from a contested UTXO back to the transactions competing for
// it. This walks that route.
func TestGetConflictingChildrenWalksTheNotedCone(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	loser := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, loser, 101, utxo.WithConflicting(true))
	require.NoError(t, err)

	got, err := s.GetConflictingChildren(ctx, *parent.TxIDChainHash())
	require.NoError(t, err)

	names := make(map[string]bool, len(got))
	for _, h := range got {
		names[h.String()] = true
	}

	require.True(t, names[loser.TxIDChainHash().String()],
		"the parent must name the transaction contesting its UTXO")
}

// TestGetCounterConflictingNamesTheWinner.
//
// When conflict resolution demotes a loser it has to find the transaction that actually took
// the UTXO, so it can promote it. On this store that answer is only in the journal, because the
// UTXO row was destroyed by the winning spend, and it reaches the walk through the per-output
// spend state on a metadata read.
func TestGetCounterConflictingNamesTheWinner(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	// The winner takes output 0 for real.
	winner := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, winner, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, winner, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// The loser wanted the same output and is stored as conflicting.
	loser := spendOutput(t, parent, 0, 2)
	_, err = s.Create(ctx, loser, 101, utxo.WithConflicting(true))
	require.NoError(t, err)

	require.NotEqual(t, winner.TxIDChainHash().String(), loser.TxIDChainHash().String())

	got, err := s.GetCounterConflicting(ctx, *loser.TxIDChainHash())
	require.NoError(t, err)

	names := make(map[string]bool, len(got))
	for _, h := range got {
		names[h.String()] = true
	}

	require.True(t, names[winner.TxIDChainHash().String()],
		"the transaction that actually took the UTXO must be named, or it can never be promoted")
}

// TestGetCounterConflictingReportsATransactionItDoesNotHold, matching both reference stores,
// whose metadata read raises before the walk starts.
func TestGetCounterConflictingReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	missing := mkTx(t, 1, 9_999)

	_, err := s.GetCounterConflicting(ctx, *missing.TxIDChainHash())
	require.Error(t, err, "a hash the store does not hold must fail loudly")
}

// TestGetCounterConflictingNamesTheWinnerForAMinedLoser is the same walk when the loser has
// been recorded in a block on the fork being abandoned and its identity row has since gone to
// the deep stamp, so it lives in tx_mined alone.
//
// Both reads the walk makes have to survive that: what the loser spends, which record-mined
// copied onto the containment row, and who took each of those UTXOs, which comes off the
// parent's per-output spend state. Neither of the two reads here is identity-only, and this
// pins that -- the walk answered from tx_ident alone would report no counter-spender at all,
// which is the answer that lets the double spend stand.
func TestGetCounterConflictingNamesTheWinnerForAMinedLoser(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_100))

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 700_000)
	require.NoError(t, err)

	winner := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, winner, 700_100)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, winner, 700_100)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	loser := spendOutput(t, parent, 0, 2)
	_, err = s.Create(ctx, loser, 700_100, utxo.WithConflicting(true))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(loser),
		utxo.MinedBlockInfo{BlockID: 51, BlockHeight: 700_100})
	require.NoError(t, err)
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*loser.TxIDChainHash()}, true))
	dropIdentityRow(t, s, ctx, loser)

	got, err := s.GetCounterConflicting(ctx, *loser.TxIDChainHash())
	require.NoError(t, err)

	names := make(map[string]bool, len(got))
	for _, h := range got {
		names[h.String()] = true
	}

	require.True(t, names[winner.TxIDChainHash().String()],
		"the transaction that actually took the UTXO must be named for a mined loser too")
}

// TestGetConflictingChildrenWalksTheNotedConeFromAMinedParent. The contest is noted against the
// parent's txid rather than against whichever row answers for it, so the note survives the
// parent being mined and losing its identity row to the stamp. Without that the cone would empty out the moment the contested
// parent was mined, which is the ordinary case rather than a corner.
func TestGetConflictingChildrenWalksTheNotedConeFromAMinedParent(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_100))

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 700_000)
	require.NoError(t, err)

	loser := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, loser, 700_100, utxo.WithConflicting(true))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent),
		utxo.MinedBlockInfo{BlockID: 52, BlockHeight: 700_100})
	require.NoError(t, err)
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*parent.TxIDChainHash()}, true))
	dropIdentityRow(t, s, ctx, parent)

	got, err := s.GetConflictingChildren(ctx, *parent.TxIDChainHash())
	require.NoError(t, err)

	names := make(map[string]bool, len(got))
	for _, h := range got {
		names[h.String()] = true
	}

	require.True(t, names[loser.TxIDChainHash().String()],
		"a mined parent must still name the transaction contesting its UTXO")
}
