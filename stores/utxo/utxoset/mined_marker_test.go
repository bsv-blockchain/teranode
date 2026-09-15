package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestCreateWithABlockIsNotAlsoWaitingToBeMined.
//
// A transaction cannot be mined and waiting to be mined at the same time. It is created
// BECAUSE a block contains it, so the block information being present is what settles the
// question, and the store must not also mark it as sitting in the mempool.
//
// This store used to require the block information to additionally claim the block was on the
// longest chain. The block-application path never says that, because at create time the block
// is still being validated, so every transaction created by a sync was stored in both states at
// once. On the mainnet box that reached 3.8 million rows, which is 91% of the store, and it
// stalled the reclaim: the pass that walks transactions waiting to be mined had millions to
// walk, and it runs before the reclaim, so nothing was ever reclaimed.
//
// The sql store keys on whether any block information was supplied at all, and this now matches
// it.
// The assertion now reads through Get rather than off tx_ident, because a create carrying
// block information no longer writes an identity row at all: it claims a membership row and
// coins that know their block. The waiting marker is a column on the row it does not have, so
// the state this test was written to forbid is now unreachable by construction, and what is
// worth pinning is that the read path says the same thing -- in a block, not waiting.
func TestCreateWithABlockIsNotAlsoWaitingToBeMined(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)

	// Exactly what block application passes: the block that contains it, with no claim about
	// the chain, because at this moment the block is still being validated.
	_, err := s.Create(ctx, tx, 700_000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_000, SubtreeIdx: 0,
	}))
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx),
		"a mined transaction has no identity row, so it cannot carry the marker either")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	require.NotEmpty(t, got.BlockIDs, "it is in a block")
	require.Zero(t, got.UnminedSince,
		"so it must NOT also be reported as waiting to be mined")
}

// TestCreateWithoutABlockIsWaitingToBeMined is the other half, and the reason the marker
// exists. A transaction arriving from the mempool is in no block, so it waits.
func TestCreateWithoutABlockIsWaitingToBeMined(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 6_000)

	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	r := readIdent(t, s, ctx, h[:])

	require.Empty(t, r.membership, "it is in no block")
	require.NotNil(t, r.offChainSince, "so it waits")
	require.Equal(t, int32(700_000), *r.offChainSince, "from the height it arrived at")
}

// TestCreateOfAnUnMinedBlockStillWaits. An explicit un-mine is the one kind of block
// information that does NOT mean the transaction is in a block, so the marker stays.
func TestCreateOfAnUnMinedBlockStillWaits(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 7_000)

	_, err := s.Create(ctx, tx, 700_000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_000, UnsetMined: true,
	}))
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	require.NotNil(t, readIdent(t, s, ctx, h[:]).offChainSince,
		"an un-mine does not put it in a block, so it still waits")
}

// TestTheWaitingSetExcludesMinedTransactions is the consequence that matters operationally.
// The preservation pass walks this set before the reclaim runs, so a mined transaction left in
// it does not merely look wrong, it holds up every reclaim behind it.
func TestTheWaitingSetExcludesMinedTransactions(t *testing.T) {
	s, ctx := newTestStore(t)

	mined := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, mined, 700_000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_000,
	}))
	require.NoError(t, err)

	waiting := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, waiting, 700_000)
	require.NoError(t, err)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	got := drain(t, it, ctx)

	names := make(map[string]bool, len(got))
	for _, tx := range got {
		names[tx.Node.Hash.String()] = true
	}

	require.True(t, names[waiting.TxIDChainHash().String()], "the mempool transaction is waiting")
	require.False(t, names[mined.TxIDChainHash().String()],
		"the mined one must not be, or it stalls the reclaim behind it")
}
