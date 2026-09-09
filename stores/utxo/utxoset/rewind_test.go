package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func hashOf(b []byte) *chainhash.Hash {
	var h chainhash.Hash

	copy(h[:], b)

	return &h
}

// TestRemoveFromConflictingChildrenDropsOnlyWhatIsNamed.
//
// Two parents in one call, each losing a DIFFERENT child, with a second child left on each.
// An implementation that built one combined drop list and applied it to every named row would
// pass a single-parent test and fail this one, which is the mutation worth catching.
//
// The parents are planted with NO identity row at all, which is the case that matters: a
// contested parent is usually mined, and this bookkeeping is keyed on the txid rather than
// hanging off a mempool row.
func TestRemoveFromConflictingChildrenDropsOnlyWhatIsNamed(t *testing.T) {
	s, ctx := newTestStore(t)

	parentA := idBytes(0x71)
	parentB := idBytes(0x72)
	childA1, childA2 := idBytes(0x81), idBytes(0x82)
	childB1, childB2 := idBytes(0x91), idBytes(0x92)

	plantConflictNote(t, s, ctx, 100, parentA, childA1)
	plantConflictNote(t, s, ctx, 100, parentA, childA2)
	plantConflictNote(t, s, ctx, 100, parentB, childB1)
	plantConflictNote(t, s, ctx, 100, parentB, childB2)

	require.NoError(t, s.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
		{ParentHash: hashOf(parentA), ChildHash: hashOf(childA1)},
		{ParentHash: hashOf(parentB), ChildHash: hashOf(childB2)},
	}))

	require.Equal(t, [][]byte{childA2}, conflictChildrenOf(t, s, ctx, parentA),
		"A loses its FIRST child and keeps the second")
	require.Equal(t, [][]byte{childB1}, conflictChildrenOf(t, s, ctx, parentB),
		"B loses its SECOND child and keeps the first")
}

// TestRemoveFromConflictingChildrenSearchesEveryWindow. The pair can have been noted at any
// height still inside the journal's retention, and the caller has no way to know which, so a
// removal that only looked in the current window would silently leave the route in place.
func TestRemoveFromConflictingChildrenSearchesEveryWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := idBytes(0x75)
	child := idBytes(0x85)

	plantConflictNote(t, s, ctx, 100, parent, child)
	plantConflictNote(t, s, ctx, 5_000, parent, child)
	require.Equal(t, [][]byte{child}, conflictChildrenOf(t, s, ctx, parent),
		"the same pair in two windows reads back once")

	require.NoError(t, s.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
		{ParentHash: hashOf(parent), ChildHash: hashOf(child)},
	}))

	require.Empty(t, conflictChildrenOf(t, s, ctx, parent),
		"both copies must go, or the contest is still findable")
}

// TestRemoveFromConflictingChildrenIsIdempotentAndSilent. A rewind is re-run after a crash, and
// the interface asks for a silent no-op rather than an error when there is nothing to remove.
func TestRemoveFromConflictingChildrenIsIdempotentAndSilent(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := idBytes(0x73)
	child := idBytes(0x83)

	plantConflictNote(t, s, ctx, 100, parent, child)

	rm := []utxo.ConflictingChildRemoval{{ParentHash: hashOf(parent), ChildHash: hashOf(child)}}

	require.NoError(t, s.RemoveFromConflictingChildren(ctx, rm))
	require.NoError(t, s.RemoveFromConflictingChildren(ctx, rm), "removing twice must not fail")

	require.Empty(t, conflictChildrenOf(t, s, ctx, parent))

	// A parent the store does not hold, and a child that was never noted.
	require.NoError(t, s.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
		{ParentHash: hashOf(idBytes(0x74)), ChildHash: hashOf(child)},
		{ParentHash: hashOf(parent), ChildHash: hashOf(idBytes(0x84))},
	}))
}

// TestRemoveBlockIDsStripsEveryNamedBlock.
//
// Two transactions in one call losing different blocks, for the same reason as above, and one
// of them carries the SAME block twice under different subtree indexes, which a first-match
// removal would leave still claiming it.
func TestRemoveBlockIDsStripsEveryNamedBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	txA := idBytes(0xa1)
	txB := idBytes(0xa2)

	plantIdent(t, s, ctx, txA,
		packTriples(t, [3]uint32{5, 500, 0}, [3]uint32{6, 600, 0}, [3]uint32{5, 500, 1}), ptrI32(100))
	plantIdent(t, s, ctx, txB,
		packTriples(t, [3]uint32{7, 700, 0}, [3]uint32{8, 800, 0}), ptrI32(100))

	require.NoError(t, s.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
		{TxHash: hashOf(txA), BlockIDs: []uint32{5}},
		{TxHash: hashOf(txB), BlockIDs: []uint32{8}},
	}))

	require.Equal(t, packTriples(t, [3]uint32{6, 600, 0}), readIdent(t, s, ctx, txA).membership,
		"every entry naming block 5 goes, including the repeat under another subtree index")
	require.Equal(t, packTriples(t, [3]uint32{7, 700, 0}), readIdent(t, s, ctx, txB).membership,
		"and B loses only what B was told to lose")
}

// TestRemoveBlockIDsIsIdempotentAndSilent, for the same crash-replay reason.
func TestRemoveBlockIDsIsIdempotentAndSilent(t *testing.T) {
	s, ctx := newTestStore(t)

	txid := idBytes(0xa3)
	plantIdent(t, s, ctx, txid, packTriples(t, [3]uint32{5, 500, 0}), ptrI32(100))

	rm := []utxo.BlockIDsRemoval{{TxHash: hashOf(txid), BlockIDs: []uint32{5}}}

	require.NoError(t, s.RemoveBlockIDs(ctx, rm))
	require.NoError(t, s.RemoveBlockIDs(ctx, rm), "stripping twice must not fail")

	require.Empty(t, readIdent(t, s, ctx, txid).membership)

	// A transaction the store does not hold, and a block it never claimed.
	require.NoError(t, s.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
		{TxHash: hashOf(idBytes(0xa4)), BlockIDs: []uint32{5}},
		{TxHash: hashOf(txid), BlockIDs: []uint32{99}},
	}))
}

// TestRemoveBlockIDsHandlesAHighBlockID pins the one trap in unpacking the packed form. The
// block id is four big-endian bytes, and shifting the top byte left through a 32-bit signed
// type wraps to a negative number, silently, so a comparison against a positive id would strip
// nothing at all.
func TestRemoveBlockIDsHandlesAHighBlockID(t *testing.T) {
	s, ctx := newTestStore(t)

	txid := idBytes(0xa5)
	const high = uint32(4_000_000_000)

	plantIdent(t, s, ctx, txid, packTriples(t, [3]uint32{high, 900, 0}, [3]uint32{6, 600, 0}), ptrI32(100))

	require.NoError(t, s.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
		{TxHash: hashOf(txid), BlockIDs: []uint32{high}},
	}))

	require.Equal(t, packTriples(t, [3]uint32{6, 600, 0}), readIdent(t, s, ctx, txid).membership,
		"a block id above the signed 32-bit range must still be found and stripped")
}

// TestConflictingTxIteratorListsConflictingNonCoinbaseTransactions.
//
// The predicate is deliberately not "waiting to be mined". A transaction that lost a race can
// have been mined into a block that later lost, so it carries membership and no mempool marker,
// and it is still conflicting. Filtering on the marker would hide exactly what a rewind exists
// to purge.
func TestConflictingTxIteratorListsConflictingNonCoinbaseTransactions(t *testing.T) {
	s, ctx := newTestStore(t)

	conflicting := idBytes(0xb1)
	minedConflicting := idBytes(0xb2)
	ordinary := idBytes(0xb3)
	conflictingCoinbase := idBytes(0xb4)

	plantIdent(t, s, ctx, conflicting, nil, ptrI32(100))
	plantIdent(t, s, ctx, minedConflicting, packTriples(t, [3]uint32{5, 500, 0}), nil)
	plantIdent(t, s, ctx, ordinary, nil, ptrI32(100))
	plantIdent(t, s, ctx, conflictingCoinbase, nil, ptrI32(100))

	for _, id := range [][]byte{conflicting, minedConflicting, conflictingCoinbase} {
		_, err := s.pool.Exec(ctx, `UPDATE tx_ident SET flags = flags | $2 WHERE txid = $1`,
			id, FlagConflicting)
		require.NoError(t, err)
	}

	_, err := s.pool.Exec(ctx, `UPDATE tx_ident SET flags = flags | $2 WHERE txid = $1`,
		conflictingCoinbase, FlagCoinbase)
	require.NoError(t, err)

	it, err := s.GetConflictingTxIterator()
	require.NoError(t, err)

	got := drain(t, it, ctx)

	names := make(map[string]bool, len(got))
	for _, tx := range got {
		names[tx.Node.Hash.String()] = true
	}

	require.True(t, names[hashOf(conflicting).String()], "a conflicting transaction")
	require.True(t, names[hashOf(minedConflicting).String()],
		"and one already mined into a block, which is what a rewind is purging")
	require.False(t, names[hashOf(ordinary).String()], "but not an ordinary one")
	require.False(t, names[hashOf(conflictingCoinbase).String()],
		"and not a coinbase, which spends nothing so can never lose a race")
}

// TestRemoveBlockIDsReachesMembershipRows: a mined transaction's membership lives in tx_mined,
// not in tx_ident, so a rewind that only touched the identity table reached mempool and
// fork-limbo rows and silently missed every settled transaction. The tool exists to recover
// from a bad chain state, and its documented contract is that a miss is a silent no-op, so an
// operator got no signal that the rewind was partial.
func TestRemoveBlockIDsReachesMembershipRows(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "a mined transaction has no identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	require.NoError(t, s.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
		{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{42}},
	}))

	require.Equal(t, 0, minedRows(t, s, ctx, tx))
}

// TestRemoveBlockIDsLeavesAMembershipRowNamingAnotherBlock: the tool removes the blocks the
// caller has stopped believing in and nothing else.
func TestRemoveBlockIDsLeavesAMembershipRowNamingAnotherBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100})
	require.NoError(t, err)
	require.Equal(t, 2, minedRows(t, s, ctx, tx))

	require.NoError(t, s.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
		{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{42}},
	}))

	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	var kept int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT block_id FROM tx_mined WHERE txid = $1`, hashBytes(tx)).Scan(&kept))
	require.Equal(t, int32(43), kept)
}

// TestRemoveBlockIDsIsIdempotentOnMembershipRows, for the same crash-replay reason the identity
// arm is idempotent.
func TestRemoveBlockIDsIsIdempotentOnMembershipRows(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	rm := []utxo.BlockIDsRemoval{{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{42}}}
	require.NoError(t, s.RemoveBlockIDs(ctx, rm))
	require.NoError(t, s.RemoveBlockIDs(ctx, rm), "stripping twice must not fail")
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
}
