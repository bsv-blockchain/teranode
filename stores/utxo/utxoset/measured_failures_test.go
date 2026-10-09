package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// The five further MEASURED failures of the block-facts design, section 1.5 of
// docs/superpowers/specs/2026-09-21-utxoset-block-facts-spec-rebuilt.md. They were probed
// against real postgres in the design cycle of 2026-09-16 and had no committed test until this
// file. Each is written here as the assertion section 14 gives it.
//
// The containment build closed two and a half of them; the stamp, the unspend repair, the
// preservation source rule and the read order closed the rest. All five are plain assertions.

// pairsOf reads the (mined_height, block_id) pair off every live UTXO of a transaction, in
// output order.
func pairsOf(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) [][2]int32 {
	t.Helper()

	lo, hi := Pack(hashBytes(tx), 0), Pack(hashBytes(tx), ^uint32(0))

	rows, err := s.pool.Query(ctx, `
		SELECT mined_height, block_id FROM utxo
		 WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4 ORDER BY ukey`,
		LeafFor(hashBytes(tx)), lo, hi, hashBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	var out [][2]int32

	for rows.Next() {
		var p [2]int32
		require.NoError(t, rows.Scan(&p[0], &p[1]))
		out = append(out, p)
	}

	require.NoError(t, rows.Err())

	return out
}

// preservedPairOf reads the pair the preservation table holds for a transaction; ok is false
// when it holds none.
func preservedPairOf(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) (pair [2]int32, ok bool) {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM preserved_parent WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	if n == 0 {
		return pair, false
	}

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT mined_height, block_id FROM preserved_parent WHERE txid = $1`, hashBytes(tx)).
		Scan(&pair[0], &pair[1]))

	return pair, true
}

// TestUnMiningAForkBlockLeavesTheMainChainRow is measured failure 1. Today's un-mine deleted
// EVERY containment row of any transaction that had a row for the named block, on purpose, to
// keep a transaction in exactly one table; un-mining a fork block therefore deleted the
// main-chain row too. The containment build makes the un-mine a point delete on the full key,
// so the main-chain row stands.
//
// The marker half of the same failure is pinned the other way. The un-mine ALWAYS sets the
// unmined marker on every listed identity row, which is Stu's decision 9 of 2026-09-22: a
// transaction the main chain still contains is marked unmined by the un-mine of a fork block,
// and block assembly's load fix-up, or the deep stamp, repairs it. Leaving the marker wrongly
// NULL would lose a transaction from block assembly for good; setting it wrongly costs a mined
// transaction reloaded as unmined, which is repaired.
func TestUnMiningAForkBlockLeavesTheMainChainRow(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))
	require.Nil(t, markerOf(t, s, ctx, tx), "on the main chain")

	require.NoError(t, s.SetBlockHeight(200))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs, "the fork block's row goes, the main-chain row stands")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	marker := markerOf(t, s, ctx, tx)
	require.NotNil(t, marker, "the un-mine always sets the marker (decision 9); the load fix-up repairs it")
	require.Equal(t, int32(200), *marker)
}

// TestForkThenMainChainRecordClearsTheMarkerAndIsStamped is measured failure 2, in two halves.
//
// The first half passes at the containment build. Today the move out of the identity table
// happened only when the packed list named exactly the block being recorded, so a transaction
// recorded by a fork block first and the main chain second was stranded with two entries and no
// way out. The containment build records both blocks as rows and clears the marker on the
// main-chain record, and nobody has to decide which block is right.
//
// The second half is the stamp: told that block 8 won, it writes the main-chain pair onto the
// transaction's UTXO, deletes block 7's row as a loser, and deletes the identity row.
func TestForkThenMainChainRecordClearsTheMarkerAndIsStamped(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100})
	require.NoError(t, err)
	require.NotNil(t, markerOf(t, s, ctx, tx), "a fork record leaves it waiting")

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	// The first half.
	require.Nil(t, markerOf(t, s, ctx, tx), "the main-chain record clears the marker however many blocks came first")
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7, 8}, got.BlockIDs)
	require.Zero(t, got.UnminedSince)

	// The second half.
	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 8})

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(8), b, "the main-chain block")
	require.False(t, identExists(t, s, ctx, tx), "and the stamp deletes the identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "block 7's row went as a loser")
}

// TestEveryLiveUTXOOfOneTransactionCarriesTheSamePair is measured failure 3. Today the pair
// was written by different statements at different moments -- the create, the reset, the
// retirement stamp and the unspend restore -- so two UTXOs of one transaction could disagree.
//
// The unspend repair writes onto a restored UTXO exactly what the stamp wrote or would write:
// below the fence, the one surviving row's pair; above it, (0,0), and the stamp reaches the
// restored UTXO through the identity row like any other. Either way the siblings agree.
func TestEveryLiveUTXOOfOneTransactionCarriesTheSamePair(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 1_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)

	require.NoError(t, s.Unspend(ctx, spends, false))

	pairs := pairsOf(t, s, ctx, parent)
	require.Len(t, pairs, 2)
	require.Equal(t, [2]int32{0, 0}, pairs[0], "unstamped: the restore keeps (0,0) and the stamp will reach it")
	require.Equal(t, pairs[1], pairs[0], "every live UTXO of one transaction carries an identical pair")

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.Equal(t, [][2]int32{{100, 7}, {100, 7}}, pairsOf(t, s, ctx, parent), "and after the stamp, both carry the winner")
}

// TestPreservedCopyNamesTheBlockThatWon is measured failure 4, decision site 3 of the design's
// earliest-row rule. The preserve pass used to copy the parent's first containment row in
// (mined_height, block_id) order, which here is the fork block with the lower id rather than
// the block the caller said is on the longest chain. Preservation now reads only windows the
// stamp has completed, where the losing rows are already deleted, and skips the parent until
// then.
func TestPreservedCopyNamesTheBlockThatWon(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	// The fork block has the lower id, so the interim rule picks it.
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	_, ok := preservedPairOf(t, s, ctx, parent)
	require.False(t, ok, "not yet: the window is not completed, so either row could still be the loser")

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 8})

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	pair, ok := preservedPairOf(t, s, ctx, parent)
	require.True(t, ok, "a parent with containment in a completed window is preserved")
	require.Equal(t, [2]int32{100, 8}, pair, "the block on the longest chain, not the loser")
}

// TestPreservedCopyNeverOutranksACorrectUTXO is measured failure 5. The read order used to try
// the identity row and containment, then the preserved copy, then the UTXO, then the undo
// copy. A preserved copy taken under the interim rule could name a loser, and once the window
// was gone it then answered ahead of a UTXO that carries the correct pair. The preserved copy
// is now read LAST, behind the UTXO and the undo copy, which removes the failure even if a
// wrong copy is ever written. The store's own preservation can no longer write one, so the
// wrong copy is planted by raw SQL.
func TestPreservedCopyNeverOutranksACorrectUTXO(t *testing.T) {
	s, ctx := newTestStore(t)

	// Born from the main-chain block below the checkpoint, so the UTXO carries (100, 8) from
	// birth and there is no identity row.
	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, `
		INSERT INTO preserved_parent (txid, mined_height, block_id, subtree_idx, created_height, preserve_until)
		VALUES ($1, 100, 7, 0, 100, 5000)`, hashBytes(parent))
	require.NoError(t, err)

	dropped := retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 8})
	require.Equal(t, 1, dropped, "the window has to be gone for the preserved copy to be consulted at all")

	h, b := utxoFacts(t, s, ctx, parent)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(8), b, "the UTXO itself is right")

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{8}, got.BlockIDs, "the UTXO's pair, not the preserved loser")

	// With the UTXO spent, its undo copy still outranks the preserved row.
	spendOneOutput(t, s, ctx, parent, 0, 3_000)

	got, err = s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{8}, got.BlockIDs, "the undo copy's pair, not the preserved loser")
}
