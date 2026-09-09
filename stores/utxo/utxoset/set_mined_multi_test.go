package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// mkStoredTxs creates n distinct transactions and stores them. base separates one call's
// transactions from another's: the satoshi value is what makes them distinct, so two calls
// sharing a base would build the same transactions and the second Create would report them
// as already held.
func mkStoredTxs(t *testing.T, s *Store, height uint32, base uint64, n int) []*bt.Tx {
	t.Helper()

	txs := make([]*bt.Tx, 0, n)

	for i := 0; i < n; i++ {
		tx := mkTx(t, 1, base+uint64(i))

		_, err := s.Create(t.Context(), tx, height)
		require.NoError(t, err)

		txs = append(txs, tx)
	}

	return txs
}

func txHashes(txs []*bt.Tx) []*chainhash.Hash {
	out := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		out = append(out, tx.TxIDChainHash())
	}

	return out
}

// TestSetMinedMultiStampsEveryTransactionInOneCall is the case a single statement built
// around array parameters can get wrong where a statement per transaction cannot: each
// transaction must gain the block on its OWN row, and on no other.
//
// The block is on the longest chain, so each named transaction is settled by it and moves to
// the membership table. The bystander proves the other half: it keeps its identity row, is
// stamped by nothing and moved by nothing.
func TestSetMinedMultiStampsEveryTransactionInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 5)

	// Stored but NOT named in the call. A join that matched on the leaf alone, or on the
	// 96-bit key prefix, would stamp this one too and no other assertion here would notice.
	bystander := mkStoredTxs(t, s, 700_000, 8_000, 1)[0]

	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	got, err := s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)
	require.Len(t, got, 5, "every hash asked about must appear in the answer")

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		require.Contains(t, got[*h], uint32(77))

		require.False(t, identExists(t, s, ctx, tx), "settled, so out of the mempool table")
		require.Equal(t, 1, minedRows(t, s, ctx, tx),
			"exactly one block, on this transaction's own row")

		m, err := s.Get(ctx, h)
		require.NoError(t, err)
		require.Equal(t, []uint32{77}, m.BlockIDs)
		require.Equal(t, []uint32{700_005}, m.BlockHeights)
		require.Equal(t, []int{2}, m.SubtreeIdxs)
	}

	require.True(t, identExists(t, s, ctx, bystander),
		"a transaction the call did not name must not be moved")

	bh := bystander.TxIDChainHash()
	br := readIdent(t, s, ctx, bh[:])
	require.Empty(t, br.membership, "nor stamped")
	require.NotNil(t, br.offChainSince, "nor lose its mempool marker")
}

// TestSetMinedMultiReplaysOverAMixedBatch puts already-stamped and never-stamped
// transactions in ONE call, which is the shape a re-offered block actually arrives in.
//
// The transactions already carrying the block must not gain it twice, and the ones that do
// not yet carry it must gain it. A statement that got the per-row guard wrong would fail one
// of those two and pass the other.
//
// After the first call the settled transactions are in the membership table, so the second
// call reaches them through the residue path rather than the stamp, and each half of the batch
// must still end up claiming this block exactly once.
func TestSetMinedMultiReplaysOverAMixedBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	first := mkStoredTxs(t, s, 700_000, 1_000, 3)
	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	_, err := s.SetMinedMulti(ctx, txHashes(first), info)
	require.NoError(t, err)

	later := mkStoredTxs(t, s, 700_001, 9_000, 2)
	all := append(txHashes(first), txHashes(later)...)

	got, err := s.SetMinedMulti(ctx, all, info)
	require.NoError(t, err, "a re-offered block must not report its transactions missing")
	require.Len(t, got, 5)

	stamped := make([]*bt.Tx, 0, len(first)+len(later))
	stamped = append(stamped, first...)
	stamped = append(stamped, later...)

	for _, tx := range stamped {
		h := tx.TxIDChainHash()

		require.Contains(t, got[*h], uint32(77))
		require.False(t, identExists(t, s, ctx, tx))
		require.Equal(t, 1, minedRows(t, s, ctx, tx),
			"the block is recorded exactly once, whether or not this transaction already had it")

		m, err := s.Get(ctx, h)
		require.NoError(t, err)
		require.Equal(t, []uint32{77}, m.BlockIDs)
		require.Equal(t, []int{2}, m.SubtreeIdxs)
	}
}

// TestSetMinedMultiToleratesTheSameHashTwiceInOneCall. A caller may name a transaction twice
// in one block, and the two offers now travel in one statement rather than in two.
func TestSetMinedMultiToleratesTheSameHashTwiceInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h, h}, utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Contains(t, got[*h], uint32(77))

	require.False(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "named twice, recorded once")
}

// TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain pins the rule the stamp shares
// with the create gate: "mined into some block" and "on the main chain" are different facts,
// and only the second clears the marker.
func TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 3)

	_, err := s.SetMinedMulti(ctx, txHashes(txs), utxo.MinedBlockInfo{
		BlockID: 88, BlockHeight: 700_006, SubtreeIdx: 1, OnLongestChain: false,
	})
	require.NoError(t, err)

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.Equal(t, packTriples(t, [3]uint32{88, 700_006, 1}), r.membership)
		require.NotNil(t, r.offChainSince,
			"a fork-only block does not settle a transaction, so the marker stays")
	}
}

// TestUnsetMinedMultiUnstampsEveryTransactionInOneCall is the reorg path at batch width: each
// transaction loses the block and gets a clock from the CURRENT tip.
//
// The stamp is a FORK stamp for the reason TestUnsetMinedGivesTheTransactionAFreshClock gives:
// un-mining is an identity-row operation, and a longest-chain stamp on a row claiming no other
// block moves that row out of the identity table.
func TestUnsetMinedMultiUnstampsEveryTransactionInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 100, 1_000, 4)
	info := utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, SubtreeIdx: 0}

	_, err := s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(5_000))

	info.UnsetMined = true

	_, err = s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.Empty(t, r.membership, "the block it was un-mined from is no longer claimed")
		require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the mempool set")
		require.Equal(t, int32(5_000), *r.offChainSince, "with a clock from the current tip")
	}
}

// mkTriple builds one 12-byte membership entry from raw bytes, for planting a column directly.
func mkTriple(start byte) []byte {
	b := make([]byte, 12)
	for i := range b {
		b[i] = start + byte(i)
	}

	return b
}

// TestStampTestsBlockMembershipOnA12ByteBoundary is a defect test for the statement that
// records a block against a transaction.
//
// The membership column is a concatenation of 12-byte triples of block id, block height and
// subtree index, and the reader unpacks it on that boundary. A plain substring search can match
// bytes STRADDLING two neighbouring triples, read that as already-recorded, and silently skip a
// real append. The transaction then never claims a block that actually contains it.
//
// This column is written on every block of every sync, so the exposure is continuous.
func TestStampTestsBlockMembershipOnA12ByteBoundary(t *testing.T) {
	s, ctx := newTestStore(t)

	var txid [32]byte
	for i := range txid {
		txid[i] = byte(i)
	}

	first := mkTriple(0x01)
	second := mkTriple(0x0d)

	// Exactly the last half of the first triple followed by the first half of the second, so it
	// appears at offset 6 and at no 12-byte boundary.
	straddler := append(append([]byte{}, first[6:]...), second[:6]...)

	_, err := s.pool.Exec(ctx, `
        INSERT INTO tx_ident (leaf, txid, created_height, membership)
        VALUES ($1, $2, 100, $3)`,
		LeafFor(txid[:]), txid[:], append(append([]byte{}, first...), second...))
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, stampSQL, LeafFor(txid[:]), [][]byte{txid[:]}, straddler, true)
	require.NoError(t, err)

	var got []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT membership FROM tx_ident WHERE txid = $1`, txid[:]).Scan(&got))

	require.Len(t, got, 36,
		"a block that only appears straddling two entries is NOT recorded, and must be appended")
	require.Equal(t, straddler, got[24:36], "and appended at the end, on the boundary")
}

// TestUnstampRemovesOnlyAWholeEntry is the same defect on the way out, and it is the worse
// half.
//
// Removing a block splices 12 bytes out of the column. At an unaligned offset that destroys the
// tail of one triple and the head of the next, leaving a length that is still a multiple of 12
// so the constraint does not catch it and the reader cannot tell. A block that is not recorded
// on a 12-byte boundary is not recorded at all, and the right answer is to change nothing.
func TestUnstampRemovesOnlyAWholeEntry(t *testing.T) {
	s, ctx := newTestStore(t)

	var txid [32]byte
	for i := range txid {
		txid[i] = byte(i)
	}

	first := mkTriple(0x01)
	second := mkTriple(0x0d)
	planted := append(append([]byte{}, first...), second...)
	straddler := append(append([]byte{}, first[6:]...), second[:6]...)

	_, err := s.pool.Exec(ctx, `
        INSERT INTO tx_ident (leaf, txid, created_height, membership)
        VALUES ($1, $2, 100, $3)`, LeafFor(txid[:]), txid[:], planted)
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, unstampSQL,
		LeafFor(txid[:]), [][]byte{txid[:]}, straddler, int32(5_000))
	require.NoError(t, err)

	var got []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT membership FROM tx_ident WHERE txid = $1`, txid[:]).Scan(&got))

	require.Equal(t, planted, got,
		"a value present only across a boundary is not an entry, so both real entries must survive intact")
}
