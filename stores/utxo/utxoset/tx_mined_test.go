package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestCoinRowsCarryTheirParentsBlockFacts pins the two columns the read path relies on once
// a transaction's membership window has been dropped: the height and block of the
// transaction that made the coin. Both fixed width, placed before the variable-length
// script so alignment costs nothing.
func TestCoinRowsCarryTheirParentsBlockFacts(t *testing.T) {
	s, ctx := newTestStore(t)

	var names []string
	rows, err := s.pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		 WHERE table_name = 'utxo' ORDER BY ordinal_position`)
	require.NoError(t, err)

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	rows.Close()

	require.Contains(t, names, "mined_height")
	require.Contains(t, names, "block_id")

	idx := func(n string) int {
		for i, x := range names {
			if x == n {
				return i
			}
		}
		return -1
	}
	require.Less(t, idx("block_id"), idx("script"), "fixed-width columns go before the script so the row needs no padding for them")
}

// TestMembershipTableIsKeyedByTransactionIdFirst pins the key order. PostgreSQL needs the
// partition key inside a partitioned table's primary key but not at its head, and a
// height-leading key cannot be probed by transaction id without a skip scan over every
// height in the partition.
func TestMembershipTableIsKeyedByTransactionIdFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 700_000))

	var def string
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		 WHERE tablename = 'tx_mined' AND indexname = 'tx_mined_pkey'`).Scan(&def))
	require.Contains(t, def, "(txid, mined_height, block_id)")

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'tx_mined'::regclass`).Scan(&n))
	require.Equal(t, 1, n, "one 288-block window for height 700,000")
}

// TestDroppedMembershipWindowsCannotComeBack pins the floor: once a window is dropped, a
// create for a height inside it fails instead of recreating the window, because a block
// re-offered after its window retired would otherwise claim every transaction in it afresh.
func TestDroppedMembershipWindowsCannotComeBack(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	floor, err := s.txMinedFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), floor, "window 0 (heights 0-287) was dropped, so the floor is window 1")

	err = s.ensureTxMinedPartition(ctx, 100)
	require.Error(t, err, "recreating a dropped window would let a stale block double its coins")

	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000), "a live window is still fine")
}

// TestRetiringWindowStampsItsLiveCoins: a mempool-created transaction's coins carry the
// sentinel until its membership window retires, when the surviving coins learn their block
// from the window's own list. Only then can the coin be the answer for an old parent.
func TestRetiringWindowStampsItsLiveCoins(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	h, _ := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "not stamped at mining")

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(7), b)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs, "served from the coin now the window is gone")
}

// TestRetiringWindowStampsFromTheFirstRow: a transaction that ends up with two tx_mined rows
// in the same window -- a longest-chain stamp naming block 7, then a fork stamp naming block 8
// at the same height -- must be stamped with the FIRST row's block after the drop. Since
// Task 10 a transaction with a surviving tx_mined row settled under it, and the first (lowest
// seq) is the earliest stamp: a coin naming the second (fork) row's block would be wrong.
func TestRetiringWindowStampsFromTheFirstRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: false})
	require.NoError(t, err)

	require.Equal(t, 2, minedRows(t, s, ctx, tx), "the fork stamp must append a second row, not replace the first")

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(7), b, "the first row's block, not the second")
}

// TestRetiringWindowDoesNotStampAnotherTransactionsCoin is the coin stamp's half of the rule
// TestUnMineDoesNotResetAnotherTransactionsCoin pins for the reset: a by-key write must recheck
// the full transaction id.
//
// The colliding row is at the SENTINEL, which is exactly the row the stamp is looking for, so
// an UPDATE matching on (leaf, ukey) alone stamps a stranger's coin with a block that does not
// contain it -- and after the window is dropped there is nothing left to correct it from.
func TestRetiringWindowDoesNotStampAnotherTransactionsCoin(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	other := insertCollidingCoin(t, s, ctx, tx, 0, 0)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h, "the window's own transaction is stamped")
	require.Equal(t, int32(7), b)

	oh, ob := coinFactsOf(t, s, ctx, other)
	require.Equal(t, int32(0), oh, "a coin sharing the packed key stays unconfirmed")
	require.Equal(t, int32(0), ob)
}

// TestRetiringWindowStampsFromTheFirstRowAcrossWindows: a transaction's membership rows do NOT
// all live in one window, and the stamp has to resolve its block from all of them.
//
// A window is keyed by mined_height, so a transaction mined at height h and fork-stamped at
// h+/-1 across a 288 boundary has a row in each of two windows. The older window retires first,
// and if it stamped from the row IT happens to hold, the coin would take that row's block --
// and here that row is the FORK stamp, appended later but at the lower height. The coin would
// then name a block that is not on the chain, for good: when the other window retires the
// mined_height = 0 guard skips the coin, and once both windows are gone nothing can correct it.
//
// The earliest row by seq across every live window is the right answer, and under Task 9's
// rules it is the transaction's longest-chain stamp: a fork stamp can only be APPENDED to a
// membership table row that already exists, so it can never be the first.
func TestRetiringWindowStampsFromTheFirstRowAcrossWindows(t *testing.T) {
	s, ctx := newTestStore(t)

	// A MEMPOOL create, so the coin sits at the sentinel and is a candidate for the stamp; a
	// block-path create would already carry its facts and be skipped either way.
	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 287)
	require.NoError(t, err)

	// The main chain block, at height 288: window 1.
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 288, OnLongestChain: true})
	require.NoError(t, err)

	// A fork block naming it at height 287: window 0, appended later so its seq is higher.
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 287})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	// Retire window 0 only, so the row left behind is the main chain's.
	dropped, err := s.dropTxMinedWindowsBelow(ctx, 300)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "window 0 goes, window 1 stays")

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(288), h, "the earliest row across windows, not the retiring window's own")
	require.Equal(t, int32(7), b, "the longest-chain block, not the fork block")
}

// TestRetiringWindowStampsWhenTheEarliestRowIsInTheRetiringWindow is the same shape the other
// way round, and it is the ordinary case: the transaction's first stamp is in the window that
// is retiring, so resolving across all windows must give the same answer reading only this one
// would have.
func TestRetiringWindowStampsWhenTheEarliestRowIsInTheRetiringWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 287)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 287, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 288})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 300)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(287), h)
	require.Equal(t, int32(7), b)
}

// TestDroppingAWindowClearsTheEnsureCache pins the interaction between the two halves of
// window management, which is invisible until they disagree.
//
// ensureTxMinedPartition remembers the last window it created and returns early when the next
// create lands in the same one. That memory can outlive the window itself: a fork stamp at an
// old height caches it, the pruner then drops it, and a later stamp at that height hits the
// cache, skips the floor read, and the INSERT fails with "no partition of relation" instead of
// the loud, explained refusal the floor exists to give.
func TestDroppingAWindowClearsTheEnsureCache(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	require.Error(t, s.ensureTxMinedPartition(ctx, 100),
		"the cached window is gone, so the floor has to be re-read and the create refused")
}
