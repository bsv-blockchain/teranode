package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// minedRows counts the membership rows a transaction holds, across every live window.
func minedRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_mined WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n
}

// utxoFacts reads the block facts off the transaction's first UTXO.
func utxoFacts(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) (minedHeight, blockID int32) {
	t.Helper()

	lo, hi := Pack(hashBytes(tx), 0), Pack(hashBytes(tx), ^uint32(0))
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT mined_height, block_id FROM utxo
		 WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4 ORDER BY ukey LIMIT 1`,
		LeafFor(hashBytes(tx)), lo, hi, hashBytes(tx)).Scan(&minedHeight, &blockID))

	return minedHeight, blockID
}

// TestBlockPathCreateWritesMembershipAndUTXOFactsAndNoIdentityRow is the design in one test:
// a create carrying mined-block information writes a membership row and UTXOs that know their
// block, and no identity row.
func TestBlockPathCreateWritesMembershipAndUTXOFactsAndNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3, OnLongestChain: true}))
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "a mined transaction has no identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(700_100), h)
	require.Equal(t, int32(42), b)
}

// TestMempoolCreateStillClaimsOnTheIdentityTable pins that stage 1 leaves the tip alone.
func TestMempoolCreateStillClaimsOnTheIdentityTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "unconfirmed sentinel")
	require.Equal(t, int32(0), b)
}

// TestBlockPathCreateIsIdempotentForTheSameBlock: a re-applied block after a crash hits the
// membership key and gets ErrTxExists, writing no second UTXO.
func TestBlockPathCreateIsIdempotentForTheSameBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	info := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})

	_, err := s.Create(ctx, tx, 700_100, info)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, info)
	require.True(t, errors.Is(err, errors.ErrTxExists))

	require.Equal(t, 1, utxoCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesTheSameHeightUnderAnotherBlockId: block-id reuse failed on a
// retry, or a stale sibling block at the same height. The same-partition probe on
// (txid, height) refuses it; the caller's ErrTxExists branch stamps instead.
func TestBlockPathCreateRefusesTheSameHeightUnderAnotherBlockId(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, utxoCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesATransactionThatStillHasAUTXO is SV Node's own duplicate check
// and what catches the two historic duplicate coinbases: a re-offer at any height of a
// transaction with a live UTXO creates nothing.
func TestBlockPathCreateRefusesATransactionThatStillHasAUTXO(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Its window is stamped and dropped; the UTXO stays because nobody spent it.
	require.Equal(t, 1, retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 1}))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	_, err = s.Create(ctx, tx, 5_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 5_000, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists), "a live UTXO proves the transaction exists")
	require.Equal(t, 1, utxoCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesAMempoolStray: the same transaction already claimed on the
// identity table (seen before its block) must answer ErrTxExists to the block path, so the
// caller records the block rather than creating its UTXOs twice. The containment row IS
// written, unconditionally, and it carries the identity row's payload: the caller's follow-up
// record-mined call names the same key and does nothing, so if this row were thin the thin
// row would be the one that survives the stamp.
func TestBlockPathCreateRefusesAMempoolStray(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, utxoCount(t, s, ctx, tx), "no second set of UTXOs")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "but the containment row is recorded")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "and the UTXO the identity route wrote is not rewritten")
	require.Equal(t, int32(0), b)

	var inpoints []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT tx_inpoints FROM tx_mined WHERE txid = $1`, hashBytes(tx)).Scan(&inpoints))
	require.NotEmpty(t, inpoints, "the row copies the identity row's payload rather than the block path's NULL")

	// The replay changes nothing and answers the same.
	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, utxoCount(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))
}

// TestBlockCarryingCreateAboveTheCheckpointTakesTheIdentityRoute pins the store applying the
// checkpoint test itself. Above the highest checkpoint a create that carries a block is not
// trusted to write the pair: it writes an identity row with a NULL marker, a containment row
// for the block, and UTXOs at (0,0) for the deep stamp to fill, exactly as a create that
// carries no block does, and Get answers with the block and not waiting.
func TestBlockCarryingCreateAboveTheCheckpointTakesTheIdentityRoute(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 1_000_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 1_000_000, SubtreeIdx: 4}))
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx), "an identity row, because the pair is not final at birth here")
	require.Nil(t, markerOf(t, s, ctx, tx), "with the marker clear: the call has no longest-chain input to say otherwise")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "one containment row for the block")

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "and the UTXOs wait for the stamp")
	require.Equal(t, int32(0), b)

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{77}, got.BlockIDs)
	require.Equal(t, []uint32{1_000_000}, got.BlockHeights)
	require.Equal(t, []int{4}, got.SubtreeIdxs)
	require.Zero(t, got.UnminedSince)
	require.NotNil(t, got.TxInpoints.ParentTxHashes, "the identity route keeps the inpoints")

	// The replay is refused and writes nothing more.
	_, err = s.Create(ctx, tx, 1_000_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 1_000_000, SubtreeIdx: 4}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 2, utxoCount(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	// A network with no checkpoints has no height at or below one, so every block-carrying
	// create there takes this route, the coinbase at any height included.
	u, uctx := newUncheckpointedStore(t)

	low := mkTx(t, 1, 6_000)
	_, err = u.Create(uctx, low, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)
	require.True(t, identExists(t, u, uctx, low))
	require.Equal(t, 1, minedRows(t, u, uctx, low))

	h, b = utxoFacts(t, u, uctx, low)
	require.Equal(t, int32(0), h)
	require.Equal(t, int32(0), b)
}

// TestBlockPathCreateRefusesATransactionContainedElsewhereAndFullySpent is the case the third
// check of the block-path claim exists for, and the one the schema comment calls money-supply
// inflation. A transaction already contained in another block, with every output spent since,
// has no identity row and no live UTXO. A second block's containment insert succeeds because
// the block id differs, so without the tx_mined check the claim would take and the spent
// outputs would be created again.
func TestBlockPathCreateRefusesATransactionContainedElsewhereAndFullySpent(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_101))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutputInBlock(t, s, ctx, tx, 0, 700_101, 43)
	require.Equal(t, 0, utxoCount(t, s, ctx, tx))
	require.False(t, identExists(t, s, ctx, tx))

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 44, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists), "the containment row in block 42 is what refuses it: %v", err)
	require.Equal(t, 0, utxoCount(t, s, ctx, tx), "no UTXO is recreated")
	require.Equal(t, 2, minedRows(t, s, ctx, tx), "the second block's containment row is recorded all the same")
}

// TestClaimFloorIsTheLowerOfTheFixedReachAndTheOldestUndoPartition pins the two bounds of the
// create claims' containment probe. The fixed reach is 2,016 blocks below the store's height,
// aligned down to a window edge. The oldest attached undo partition lowers it further, because
// a fully spent transaction can be re-offered as itself only while the undo copies of its
// inputs live, and they live in the attached undo partitions, however late the drops run.
func TestClaimFloorIsTheLowerOfTheFixedReachAndTheOldestUndoPartition(t *testing.T) {
	s, ctx := newTestStore(t)

	require.Equal(t, int32(0), s.claimFloor(), "a fresh store at height zero reads every window")

	require.NoError(t, s.SetBlockHeight(10_000))
	require.Equal(t, int32(7_776), s.claimFloor(), "align_down(10,000 - 2,016, 288) with no undo partition attached")

	// A spend at height 1,000 attaches undo partition 3, whose first height is 864.
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 1_000))
	require.Equal(t, int32(864), s.claimFloor(), "the oldest attached undo partition is the lower bound")

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 5_000))
	require.Equal(t, int32(864), s.claimFloor(), "a newer partition does not raise it")

	// Dropping partition 3 leaves partition 17, whose first height is 4,896.
	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 2, dropped, "the journal leaf and its conflict-children twin")
	require.Equal(t, int32(4_896), s.claimFloor())

	dropped, err = s.dropSpendJournalPartitionsBelow(ctx, 9_000)
	require.NoError(t, err)
	require.Equal(t, 2, dropped)
	require.Equal(t, int32(7_776), s.claimFloor(), "with nothing attached the fixed reach alone stands")

	// A store opened over an existing database reads the bound from the catalog once.
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 300))

	reopened, rctx := newTestStoreWith(t, nil)
	_ = rctx

	require.Equal(t, uint32(0), reopened.oldestUndoLeaf.Load(), "newTestStore drops the schema, so a fresh store sees nothing")
	require.NoError(t, s.loadOldestUndoLeaf(ctx))
	require.Equal(t, uint32(0), s.oldestUndoLeaf.Load(), "and the old store's catalog read agrees once its tables are gone")
}

// TestUnminedCreateClaimSeesAContainmentRowTheUndoPartitionKeepsInReach: the second bound in
// action. A transaction mined long ago and fully spent recently is a replay candidate for as
// long as the undo copies of its inputs live. Its containment row is far below the fixed reach,
// and the undo partition that holds the copies is what keeps the row inside the probe.
func TestUnminedCreateClaimSeesAContainmentRowTheUndoPartitionKeepsInReach(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Spent at 150, so undo partition 0 is attached and its first height, 0, is the floor
	// however high the store's height goes.
	spendOneOutputInBlock(t, s, ctx, parent, 0, 150, 8)
	require.Equal(t, 0, utxoCount(t, s, ctx, parent))

	require.NoError(t, s.SetBlockHeight(10_000))
	require.Equal(t, int32(0), s.claimFloor())

	// Re-offered as an unmined create with the spend phase skipped, which is the validator's
	// conflicting route. The identity claim's containment probe sees the row at height 100.
	_, _, err = s.SpendAndCreate(ctx, parent, 10_000, utxo.WithCreateOnly())
	require.True(t, errors.Is(err, errors.ErrTxExists), "refused by the containment row: %v", err)
	require.Equal(t, 0, utxoCount(t, s, ctx, parent), "no output is written a second time")
	require.False(t, identExists(t, s, ctx, parent))
}

// TestMempoolCreateRefusesASettledTransaction is the mirror of
// TestBlockPathCreateRefusesAMempoolStray, and the reason a transaction can live in exactly
// ONE of the two tables.
//
// Without a membership guard on the mempool claim, a create of an already-settled transaction
// takes a fresh identity row -- the transaction then has a home in both tables -- and, because
// the UTXO insert is gated on that claim taking, writes every one of its outputs a SECOND
// time. Duplicate UTXOs are the failure the whole claim mechanism exists to prevent.
func TestMempoolCreateRefusesASettledTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_101)
	require.True(t, errors.Is(err, errors.ErrTxExists), "a settled transaction already exists")

	require.Equal(t, 1, utxoCount(t, s, ctx, tx), "and its UTXOs are not written twice")
	require.False(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))
}

// TestMempoolCreateRefusesATransactionThatStillHasAUTXO is the mempool mirror of
// TestBlockPathCreateRefusesATransactionThatStillHasAUTXO, and it closes the one hole the
// membership guard alone leaves open.
//
// For a transaction mined more than the membership retention ago, both of the mempool claim's
// original guards are empty: the identity row never existed, and the membership window has
// been dropped. Its UTXOs are still live, because window retirement stamped them on the way
// out. So the claim took, and because the UTXO insert is gated on that same claim, every
// output was written a SECOND row -- the UTXO key is a non-unique 96-bit prefix by design, so
// nothing downstream catches it. That is money-supply inflation.
//
// The reachable caller is the validator's CreateConflicting branch
// (services/validator/Validator.go:904): when every input fails as already-spent it calls
// CreateInUtxoStore with markAsConflicting, which is SpendAndCreate + WithCreateOnly and no
// mined-block info, so it lands on the mempool claim with the spend phase skipped. That option
// is on for every subtree-validation entry point, which is the mainline block path at the tip.
func TestMempoolCreateRefusesATransactionThatStillHasAUTXO(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Its window is stamped and dropped; the UTXO stays because nobody spent it.
	require.Equal(t, 1, retireWindows(t, s, ctx, 0, map[uint32]uint32{100: 1}))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
	require.False(t, identExists(t, s, ctx, tx))

	// WithCreateOnly skips the spend phase, so the "the mempool path spends before it creates"
	// argument does not hold here: this create reaches the claim with nothing spent.
	_, _, err = s.SpendAndCreate(ctx, tx, 5_000, utxo.WithCreateOnly())
	require.True(t, errors.Is(err, errors.ErrTxExists), "a live UTXO proves the transaction exists")
	require.Equal(t, 1, utxoCount(t, s, ctx, tx), "and its UTXOs are not written twice")
}
