package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetMinedRecordsTheBlockAndStopsWaiting is the ordinary path: a transaction seen before
// its block is mined, so it gains a containment row and its unmined marker clears. Its identity
// row STAYS. Nothing moves between tables any more: containment has one home, and the identity
// row lives until the deep stamp of build step 5 writes the block onto the UTXOs and deletes it.
func TestSetMinedRecordsTheBlockAndStopsWaiting(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true,
	})
	require.NoError(t, err)

	require.Contains(t, got, *h, "every hash asked about must appear in the answer")
	require.Contains(t, got[*h], uint32(77), "and every answer must contain the block just recorded")

	require.True(t, identExists(t, s, ctx, tx), "the identity row stays until the stamp")
	require.Nil(t, markerOf(t, s, ctx, tx), "mined on the longest chain means no longer waiting")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	m, err := s.Get(ctx, h)
	require.NoError(t, err)
	require.Equal(t, []uint32{77}, m.BlockIDs)
	require.Equal(t, []uint32{700_005}, m.BlockHeights)
	require.Equal(t, []int{2}, m.SubtreeIdxs)
	require.Zero(t, m.UnminedSince)

	h2, b2 := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h2, "recording mined touches no UTXO; the stamp writes the pair later")
	require.Equal(t, int32(0), b2)
}

// TestSetMinedOnAReplayedBlockStillAnswers: the insert does nothing on conflict, and the answer
// is read back from the rows, so a replayed block reports every transaction in it with the
// block recorded exactly once.
func TestSetMinedOnAReplayedBlockStillAnswers(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err, "a replayed block must not report its transactions missing")
	require.Contains(t, got, *h)
	require.Contains(t, got[*h], uint32(77))

	require.Equal(t, 1, minedRows(t, s, ctx, tx),
		"and the same block must not be recorded twice")
}

// TestSetMinedReportsATransactionItDoesNotHold. The interface requires an implementation
// that cannot prove the postcondition to return an error rather than a partial map. A
// transaction with neither an identity row nor a containment row has no payload to copy, so
// the insert writes nothing for it and the read-back misses it.
func TestSetMinedReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, known, 700_000)
	require.NoError(t, err)

	missing := mkTx(t, 1, 9_999)

	_, err = s.SetMinedMulti(ctx,
		[]*chainhash.Hash{known.TxIDChainHash(), missing.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, OnLongestChain: true})

	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"a hash the store does not hold must fail loudly, not come back as a silent gap in the map: got %v", err)
}

// TestUnsetMinedGivesTheTransactionAFreshClock covers the reorg path, and pins the fact that
// settled the merge question: a resurrected transaction gets a clock taken from the CURRENT
// tip, not its creation height. That is why the marker cannot be derived from created_height.
//
// The un-mine is a point delete of the one containment row named, and the marker is written on
// the identity row, which was there all along. On a store with no checkpoints, because an
// un-mine at or below the checkpoint is refused.
func TestUnsetMinedGivesTheTransactionAFreshClock(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 5, BlockHeight: 100, SubtreeIdx: 0,
	})
	require.NoError(t, err)
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	require.NoError(t, s.SetBlockHeight(5_000))

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 5, BlockHeight: 100, UnsetMined: true,
	})
	require.NoError(t, err)

	r := readIdent(t, s, ctx, h[:])
	require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the unmined set")
	require.Equal(t, int32(5_000), *r.offChainSince,
		"the clock comes from the current tip, not from created_height, which is why the two are different concepts")
	require.Equal(t, 0, minedRows(t, s, ctx, tx), "and the block it was un-mined from is no longer recorded")
}

// TestUnsetMinedToleratesATransactionItDoesNotHold, which the interface states explicitly.
func TestUnsetMinedToleratesATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	gone := mkTx(t, 1, 1_000)

	_, err := s.SetMinedMulti(ctx, []*chainhash.Hash{gone.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err, "un-mining may no-op for a transaction that no longer exists")
}

// TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable: the retry path records a
// transaction the block path already created; the postcondition must be satisfied from
// tx_mined and the returned ids must include the recorded block.
func TestSetMinedMultiFindsABlockPathTransactionInTheMembershipTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got[*tx.TxIDChainHash()])
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "same block recorded again appends nothing")
}

// TestSetMinedMultiAppendsASecondBlockAtTheSameHeight: a sibling block at the same height
// records the same transaction; containment holds both, and the answer lists them in
// (mined_height, block_id) order.
func TestSetMinedMultiAppendsASecondBlockAtTheSameHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100})
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got[*tx.TxIDChainHash()])
	require.Equal(t, 2, minedRows(t, s, ctx, tx))
}

// TestSetMinedMultiStillFailsForAnUnknownTransaction keeps the postcondition honest.
func TestSetMinedMultiStillFailsForAnUnknownTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true})
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestLongestChainRecordCopiesThePayloadOntoTheContainmentRow: the containment row carries
// everything a lookup needs once the identity row is gone, copied from the identity row at the
// moment of recording, and Get answers with both the payload and the block while both exist.
func TestLongestChainRecordCopiesThePayloadOntoTheContainmentRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, tx))

	// The create path writes fee NULL on purpose, so give the row one. A fee lost in the copy
	// would only surface once block assembly rebuilt a candidate from an un-mined transaction
	// after the stamp had deleted its identity row.
	_, err = s.pool.Exec(ctx, `UPDATE tx_ident SET fee = 1234 WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx), "mined on the main chain: the identity row stays")
	require.Nil(t, markerOf(t, s, ctx, tx), "with its marker clear")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, []int{2}, got.SubtreeIdxs)
	require.Equal(t, uint64(uint32(tx.Size())), got.SizeInBytes)
	require.NotNil(t, got.TxInpoints.ParentTxHashes)
	require.Equal(t, uint64(1_234), got.Fee)

	// And the containment row alone answers the same, which is what the stamp will leave.
	dropIdentityRow(t, s, ctx, tx)

	got, err = s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, uint64(uint32(tx.Size())), got.SizeInBytes, "the payload travels with the row")
	require.NotNil(t, got.TxInpoints.ParentTxHashes)
	require.Equal(t, uint64(1_234), got.Fee, "and so does the fee block assembly would need back")
}

// TestForkRecordInsertsContainmentAndKeepsTheMarker: a block not on the longest chain records
// itself as containment exactly as a main-chain block does, and leaves the transaction in the
// unmined set. There is no second code path for a fork block.
func TestForkRecordInsertsContainmentAndKeepsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "containment is recorded whichever chain the block is on")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.NotZero(t, got.UnminedSince, "still in the unmined set")
}

// TestLongestChainRecordAfterAForkRecordClearsTheMarker: two blocks contain it, one of them
// on the longest chain. Both are recorded, the marker clears, and nobody has to decide which
// block is right; the callers filter against the chain.
func TestLongestChainRecordAfterAForkRecordClearsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got.BlockIDs)
	require.Zero(t, got.UnminedSince)
}

// TestForkRecordTwiceRecordsTheBlockOnce: the insert does nothing on conflict, so a replayed
// fork block leaves one row and one entry in the answer.
func TestForkRecordTwiceRecordsTheBlockOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	info := utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3}

	first, err := s.SetMinedMulti(ctx, hashes(tx), info)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, first[*tx.TxIDChainHash()])

	second, err := s.SetMinedMulti(ctx, hashes(tx), info)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, second[*tx.TxIDChainHash()], "a replayed block is recorded once")

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs, "one row, not two")
	require.Equal(t, []uint32{700_100}, got.BlockHeights)
	require.Equal(t, []int{3}, got.SubtreeIdxs)
}

// TestUnMineDeletesTheOneRowAndSetsTheMarker: the block is taken back; its containment row
// goes, the identity row that was there all along gets the unmined marker at the CURRENT tip,
// and the UTXOs, which were at the sentinel from birth, are still there.
func TestUnMineDeletesTheOneRowAndSetsTheMarker(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, got.BlockIDs)
	require.Equal(t, uint32(700_150), got.UnminedSince, "a fresh clock from the current tip, not the creation height")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h)
	require.Equal(t, int32(0), b)
}

// TestUnMineIsAPointDeleteOnTheFullKey: un-mining ONE of the two blocks that contain a
// transaction removes that block's row and nothing else. The sibling block's row survives,
// because "block 43 contains this transaction" is still true and a chain switch cannot make it
// false. No UTXO is touched.
//
// This reverses the earlier rule that an un-mine deleted every containment row so that the
// transaction lived in exactly one table, and it drops the UTXO reset that went with it.
// Containment has one home now and the identity row stays put through mining, so there is no
// second home to keep clear; and above the checkpoint, which is the only place an un-mine is
// allowed, no UTXO carries a pair a reorg could leave stale, because the store applies the
// checkpoint test to creates itself and a block-carrying create there writes (0,0).
//
// The transaction is created by the block path ABOVE the highest checkpoint, so it has an
// identity row, a containment row for block 42 and UTXOs at (0,0), which is what every
// transaction at the tip looks like.
func TestUnMineIsAPointDeleteOnTheFullKey(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(1_000_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 1_000_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 1_000_100, OnLongestChain: true}))
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, tx), "above the checkpoint a block-carrying create writes an identity row")

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 1_000_100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 1_000_100, UnsetMined: true})
	require.NoError(t, err)

	require.Equal(t, 1, minedRows(t, s, ctx, tx), "the sibling's row survives the un-mine")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{43}, got.BlockIDs, "the un-mined block's row is gone, the sibling's stands")
	require.Equal(t, uint32(1_000_150), got.UnminedSince,
		"the un-mine always sets the marker, even on a transaction a sibling block still contains")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "no UTXO is touched")
	require.Equal(t, int32(0), b)
}

// TestUnMineRefusesABlockAtOrBelowTheCheckpoint: below the highest checkpoint every UTXO is
// born from a block-path create with its pair written at birth, and "final at birth" is only
// true if nothing un-mines a checkpoint-certified block. So the store refuses, and every row is
// unchanged. An invalidation there is a resync, not an un-mine.
func TestUnMineRefusesABlockAtOrBelowTheCheckpoint(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, UnsetMined: true})
	require.True(t, errors.Is(err, errors.ErrProcessing), "refused: %v", err)
	require.Contains(t, err.Error(), "at or below the highest checkpoint")

	require.Equal(t, 1, minedRows(t, s, ctx, tx), "the containment row stands")
	require.False(t, identExists(t, s, ctx, tx), "no identity row appears")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(700_100), h, "and the UTXO keeps its pair")
	require.Equal(t, int32(42), b)

	// The height exactly AT the checkpoint is refused too; one above it is not.
	at := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, at, 945_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 50, BlockHeight: 945_000, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(at),
		utxo.MinedBlockInfo{BlockID: 50, BlockHeight: 945_000, UnsetMined: true})
	require.True(t, errors.Is(err, errors.ErrProcessing), "the checkpoint height itself counts as below: %v", err)

	above := mkTx(t, 1, 7_000)
	_, err = s.Create(ctx, above, 945_001, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 51, BlockHeight: 945_001, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(above),
		utxo.MinedBlockInfo{BlockID: 51, BlockHeight: 945_001, UnsetMined: true})
	require.NoError(t, err, "one block above the checkpoint is a reorg's territory")
	require.Equal(t, 0, minedRows(t, s, ctx, above))
}

// TestUnMineOfABlockTheTransactionDoesNotNameDeletesNothing. An un-mine names a block, and a
// transaction with no containment row for THAT block was never mined into it, so there is no
// row to take back and the interface tolerates the absence. The marker IS still set: the
// un-mine always sets it on every listed identity row, because leaving it wrongly NULL would
// lose the transaction from block assembly for good while setting it wrongly costs a mined
// transaction reloaded as unmined, which the consistency scan repairs.
func TestUnMineOfABlockTheTransactionDoesNotNameDeletesNothing(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(1_000_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 1_000_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 1_000_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 1_000_100, UnsetMined: true})
	require.NoError(t, err)

	require.Equal(t, 1, minedRows(t, s, ctx, tx), "block 42's containment row stays")
	require.Equal(t, uint32(1_000_150), uint32(*markerOf(t, s, ctx, tx)), "and the marker is set all the same") //nolint:gosec // a stored height is never negative

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs, "block 42 still contains it")
}
