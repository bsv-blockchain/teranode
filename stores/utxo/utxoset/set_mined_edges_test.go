package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestSetMinedMultiAnswersAnEmptyOrNilListWithNothing: the interface allows nil entries, and a
// list with no transaction in it writes nothing and answers an empty map, not an error.
func TestSetMinedMultiAnswersAnEmptyOrNilListWithNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	info := utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 100, OnLongestChain: true}

	got, err := s.SetMinedMulti(ctx, nil, info)
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = s.SetMinedMulti(ctx, []*chainhash.Hash{nil, nil}, info)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestSetMinedMultiSkipsANilEntryBesideRealOnes: a nil entry is skipped, and the real
// transactions beside it are recorded and answered, on the ordinary path and on the quiet
// path a deep fork block in a dropped partition takes.
func TestSetMinedMultiSkipsANilEntryBesideRealOnes(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 1_000)
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{nil, tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 1_000, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, map[chainhash.Hash][]uint32{*tx.TxIDChainHash(): {5}}, got)

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, nil))

	got, err = s.SetMinedMulti(ctx, []*chainhash.Hash{nil, tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, OnLongestChain: false})
	require.NoError(t, err)
	require.Contains(t, got[*tx.TxIDChainHash()], uint32(9), "the quiet outcome reports the block for the real entry")
}

// TestUnMineOfABlockInADroppedPartitionSettles: un-mining a block whose tx_mined partition has
// already dropped has nothing to delete. It returns success, counted, so a reorg that names a
// very old block does not fail.
func TestUnMineOfABlockInADroppedPartitionSettles(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 1_000)
	require.NoError(t, err)

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, nil))

	skips := testutil.ToFloat64(fenceNoops.WithLabelValues("dropped_skip"))

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err)
	require.Equal(t, skips+1, testutil.ToFloat64(fenceNoops.WithLabelValues("dropped_skip")))
}

// TestUnMineCountsARecordWithNoIdentityRowButNotACoinbase: an un-mine that deletes the only
// containment row of a transaction with no identity row has destroyed that transaction's last
// record, which no reorg should ever do, so it is counted as an alarm. A coinbase has no identity
// row by design, so its un-mine is not counted.
func TestUnMineCountsARecordWithNoIdentityRowButNotACoinbase(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	mined := utxo.MinedBlockInfo{BlockID: 4, BlockHeight: 1_000, OnLongestChain: true}

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 1_000)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), mined)
	require.NoError(t, err)

	cb := mkCoinbase(t, 50_000)
	_, err = s.Create(ctx, cb, 1_000, utxo.WithMinedBlockInfo(mined))
	require.NoError(t, err)

	// The state no reorg should reach: the identity row is gone while the block's containment
	// row is still there. The coinbase loses its identity row too, so only its coinbase flag keeps
	// it out of the count.
	dropIdentityRow(t, s, ctx, tx)
	dropIdentityRow(t, s, ctx, cb)

	before := testutil.ToFloat64(noIdentityReached.WithLabelValues("un_mine"))

	mined.UnsetMined = true
	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash(), cb.TxIDChainHash()}, mined)
	require.NoError(t, err)

	require.Equal(t, before+1, testutil.ToFloat64(noIdentityReached.WithLabelValues("un_mine")),
		"the transaction counts, and the coinbase does not")
}

// TestSetMinedMultiTellsABlockAboutAnOlderBlockOfAHeldTransaction is a block that includes
// again a transaction mined long ago that still holds an unspent output. The deep stamp has
// deleted its identity row and its containment, so the only record of the older block is the
// pair on its UTXO. SetMinedMulti must still return the older block, as aerospike's setMined
// does, so the caller marks the new block invalid; "not found" left its mined status unset and
// block assembly waiting behind it.
func TestSetMinedMultiTellsABlockAboutAnOlderBlockOfAHeldTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 7}))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs, "fixture: only the UTXO pair still records block 7")

	newHeight := s.GetBlockHeight() + 1
	require.NoError(t, s.SetBlockHeight(newHeight))

	ids, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9_999, BlockHeight: newHeight, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{7, 9_999}, ids[*tx.TxIDChainHash()])

	// A transaction the store has never held is still not found.
	_, err = s.SetMinedMulti(ctx, hashes(mkTx(t, 1, 6_000)), utxo.MinedBlockInfo{BlockID: 9_999, BlockHeight: newHeight, OnLongestChain: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)
}

// TestSetMinedMultiTellsABlockAboutAnOlderBlockOfASpentTransaction is the same long-ago answer
// for a transaction whose outputs were all spent after the stamp and drop. Its UTXOs are gone,
// so no pair on them records block 7; the undo copies the spend journalled do, and the answer
// must still name block 7 and then the new block.
func TestSetMinedMultiTellsABlockAboutAnOlderBlockOfASpentTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 7}))

	spendAt := s.GetBlockHeight() + 1
	require.NoError(t, s.SetBlockHeight(spendAt))

	_, err = spendOnly(ctx, s, spendParentTx(t, tx, nil, 1_000, 0, 1), spendAt)
	require.NoError(t, err)
	require.Zero(t, utxoCount(t, s, ctx, tx), "fixture: every output is spent, so no UTXO pair is left")
	require.False(t, identExists(t, s, ctx, tx), "fixture: the stamp deleted the identity row")
	require.Zero(t, minedRows(t, s, ctx, tx), "fixture: the drop removed the containment row")

	newHeight := spendAt + 1
	require.NoError(t, s.SetBlockHeight(newHeight))

	ids, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9_999, BlockHeight: newHeight, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{7, 9_999}, ids[*tx.TxIDChainHash()])
}

// TestSetMinedMultiNamesTheOlderBlockOnceWhenAskedForItAgain is the long-ago answer when the
// block asked for is already in the older list. Its answer is that list as it is, without
// appending the same id a second time.
//
// The call names block 7 at a live height rather than at its own height 100. At 100 the call
// never gets this far: on the longest chain recordMined refuses to recreate the dropped
// partition, and off it the call takes the quiet branch and returns first. So this reaches
// the guard directly, by id, which is the only thing the guard compares.
func TestSetMinedMultiNamesTheOlderBlockOnceWhenAskedForItAgain(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 7}))

	newHeight := s.GetBlockHeight() + 1
	require.NoError(t, s.SetBlockHeight(newHeight))

	ids, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: newHeight, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, ids[*tx.TxIDChainHash()], "block 7 is answered once")
}
