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

// coinFacts reads the block facts off the transaction's first coin.
func coinFacts(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) (minedHeight, blockID int32) {
	t.Helper()

	lo, hi := Pack(hashBytes(tx), 0), Pack(hashBytes(tx), ^uint32(0))
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT mined_height, block_id FROM utxo
		 WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4 ORDER BY ukey LIMIT 1`,
		LeafFor(hashBytes(tx)), lo, hi, hashBytes(tx)).Scan(&minedHeight, &blockID))

	return minedHeight, blockID
}

// TestBlockPathCreateWritesMembershipAndCoinFactsAndNoIdentityRow is the design in one test:
// a create carrying mined-block information writes a membership row and coins that know their
// block, and no identity row.
func TestBlockPathCreateWritesMembershipAndCoinFactsAndNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3, OnLongestChain: true}))
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "a mined transaction has no identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	h, b := coinFacts(t, s, ctx, tx)
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

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "unconfirmed sentinel")
	require.Equal(t, int32(0), b)
}

// TestBlockPathCreateIsIdempotentForTheSameBlock: a re-applied block after a crash hits the
// membership key and gets ErrTxExists, writing no second coin.
func TestBlockPathCreateIsIdempotentForTheSameBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	info := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})

	_, err := s.Create(ctx, tx, 700_100, info)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, info)
	require.True(t, errors.Is(err, errors.ErrTxExists))

	require.Equal(t, 1, coinCount(t, s, ctx, tx))
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
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesATransactionThatStillHasACoin is SV Node's own duplicate check
// and what catches the two historic duplicate coinbases: a re-offer at any height of a
// transaction with a live coin creates nothing.
func TestBlockPathCreateRefusesATransactionThatStillHasACoin(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Its window retires and is dropped; the coin stays because nobody spent it.
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	_, err = s.Create(ctx, tx, 5_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 5_000, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists), "a live coin proves the transaction exists")
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesAMempoolStray: the same transaction already claimed on the
// identity table (a mempool arrival) must answer ErrTxExists to the block path, so the
// caller stamps it rather than creating its coins twice.
func TestBlockPathCreateRefusesAMempoolStray(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
}

// TestMempoolCreateRefusesASettledTransaction is the mirror of
// TestBlockPathCreateRefusesAMempoolStray, and the reason a transaction can live in exactly
// ONE of the two tables.
//
// Without a membership guard on the mempool claim, a create of an already-settled transaction
// takes a fresh identity row -- the transaction then has a home in both tables -- and, because
// the coin insert is gated on that claim taking, writes every one of its outputs a SECOND
// time. Duplicate coins are the failure the whole claim mechanism exists to prevent.
func TestMempoolCreateRefusesASettledTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_101)
	require.True(t, errors.Is(err, errors.ErrTxExists), "a settled transaction already exists")

	require.Equal(t, 1, coinCount(t, s, ctx, tx), "and its coins are not written twice")
	require.False(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))
}

// TestMempoolCreateRefusesATransactionThatStillHasACoin is the mempool mirror of
// TestBlockPathCreateRefusesATransactionThatStillHasACoin, and it closes the one hole the
// membership guard alone leaves open.
//
// For a transaction mined more than the membership retention ago, both of the mempool claim's
// original guards are empty: the identity row never existed, and the membership window has
// been dropped. Its coins are still live, because window retirement stamped them on the way
// out. So the claim took, and because the coin insert is gated on that same claim, every
// output was written a SECOND row -- the coin key is a non-unique 96-bit prefix by design, so
// nothing downstream catches it. That is money-supply inflation.
//
// The reachable caller is the validator's CreateConflicting branch
// (services/validator/Validator.go:904): when every input fails as already-spent it calls
// CreateInUtxoStore with markAsConflicting, which is SpendAndCreate + WithCreateOnly and no
// mined-block info, so it lands on the mempool claim with the spend phase skipped. That option
// is on for every subtree-validation entry point, which is the mainline block path at the tip.
func TestMempoolCreateRefusesATransactionThatStillHasACoin(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Its window retires and is dropped; the coin stays because nobody spent it.
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
	require.False(t, identExists(t, s, ctx, tx))

	// WithCreateOnly skips the spend phase, so the "the mempool path spends before it creates"
	// argument does not hold here: this create reaches the claim with nothing spent.
	_, _, err = s.SpendAndCreate(ctx, tx, 5_000, utxo.WithCreateOnly())
	require.True(t, errors.Is(err, errors.ErrTxExists), "a live coin proves the transaction exists")
	require.Equal(t, 1, coinCount(t, s, ctx, tx), "and its coins are not written twice")
}
