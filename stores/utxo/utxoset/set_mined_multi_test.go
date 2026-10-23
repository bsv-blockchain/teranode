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
// The block is on the longest chain, so each named transaction gains a containment row and
// loses its marker. The bystander proves the other half: it gains no row and keeps its marker.
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

		require.Nil(t, markerOf(t, s, ctx, tx), "on the longest chain, so no longer waiting")
		require.Equal(t, 1, minedRows(t, s, ctx, tx),
			"exactly one block, on this transaction's own row")

		m, err := s.Get(ctx, h)
		require.NoError(t, err)
		require.Equal(t, []uint32{77}, m.BlockIDs)
		require.Equal(t, []uint32{700_005}, m.BlockHeights)
		require.Equal(t, []int{2}, m.SubtreeIdxs)
	}

	require.True(t, identExists(t, s, ctx, bystander),
		"a transaction the call did not name must not be touched")
	require.Equal(t, 0, minedRows(t, s, ctx, bystander), "nor recorded")
	require.NotNil(t, markerOf(t, s, ctx, bystander), "nor lose its unmined marker")
}

// TestSetMinedMultiReplaysOverAMixedBatch puts already-stamped and never-stamped
// transactions in ONE call, which is the shape a re-offered block actually arrives in.
//
// The transactions already carrying the block must not gain it twice, and the ones that do
// not yet carry it must gain it. A statement that got the per-row guard wrong would fail one
// of those two and pass the other.
//
// The insert does nothing on conflict, so the transactions already carrying the block are
// left as they are and the rest gain it, in one statement per leaf group.
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
		require.Nil(t, markerOf(t, s, ctx, tx))
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

	require.Equal(t, 1, minedRows(t, s, ctx, tx), "named twice, recorded once")
}

// TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain pins the rule: "mined into some
// block" and "on the main chain" are different facts, and only the second clears the marker.
// The containment row is written either way.
func TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 3)

	_, err := s.SetMinedMulti(ctx, txHashes(txs), utxo.MinedBlockInfo{
		BlockID: 88, BlockHeight: 700_006, SubtreeIdx: 1, OnLongestChain: false,
	})
	require.NoError(t, err)

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		require.Equal(t, 1, minedRows(t, s, ctx, tx), "the containment row is recorded")

		m, err := s.Get(ctx, h)
		require.NoError(t, err)
		require.Equal(t, []uint32{88}, m.BlockIDs)
		require.Equal(t, []int{1}, m.SubtreeIdxs)
		require.NotNil(t, markerOf(t, s, ctx, tx),
			"a fork-only block does not put a transaction on the chain, so the marker stays")
	}
}

// TestUnsetMinedMultiUnstampsEveryTransactionInOneCall is the reorg path at batch width: each
// transaction loses the block's containment row and gets a clock from the CURRENT tip.
func TestUnsetMinedMultiUnstampsEveryTransactionInOneCall(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

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
		require.Equal(t, 0, minedRows(t, s, ctx, tx), "the block it was un-mined from is no longer recorded")
		require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the unmined set")
		require.Equal(t, int32(5_000), *r.offChainSince, "with a clock from the current tip")
	}
}
