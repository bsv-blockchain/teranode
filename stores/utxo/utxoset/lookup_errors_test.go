package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// corruptInpoints replaces a transaction's stored inpoints with a single byte, which the
// deserializer cannot read a parent count out of.
//
// A direct UPDATE, because nothing in the store's own write path can produce this. That is the
// point: the failure being modelled is a damaged page, a truncated write or a bug in some
// future writer, and the read path has to survive it without taking its neighbours down.
func corruptInpoints(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) {
	t.Helper()

	res, err := s.pool.Exec(ctx,
		`UPDATE tx_ident SET tx_inpoints = '\x00'::bytea WHERE leaf = $1 AND txid = $2`,
		LeafFor(hashBytes(tx)), hashBytes(tx))
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected(), "the row to corrupt has to exist")
}

// TestBatchDecorateReportsACorruptRowOnItsOwnEntry is the contract BatchDecorate states in its
// own doc comment, applied to a decode fault rather than to a miss.
//
// Subtree validation resolves thousands of transactions in one call. Failing the call because
// ONE of them has an unreadable tx_inpoints would reject every transaction that happened to
// travel with it, which is a few thousand valid transactions rejected over one damaged row.
func TestBatchDecorateReportsACorruptRowOnItsOwnEntry(t *testing.T) {
	s, ctx := newTestStore(t)

	good := mkTx(t, 1, 6_001)
	_, err := s.Create(ctx, good, 700_000)
	require.NoError(t, err)

	bad := mkTx(t, 1, 6_002)
	_, err = s.Create(ctx, bad, 700_000)
	require.NoError(t, err)

	corruptInpoints(t, s, ctx, bad)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *good.TxIDChainHash()}, {Hash: *bad.TxIDChainHash()},
	}
	require.NoError(t, s.BatchDecorate(ctx, items, fields.TxInpoints),
		"one corrupt row must not fail the call")

	require.NoError(t, items[0].Err, "the readable transaction is unaffected")
	require.NotNil(t, items[0].Data)
	require.Equal(t, uint64(good.Size()), items[0].Data.SizeInBytes)

	require.Error(t, items[1].Err, "the corrupt row is reported on its own entry")
	require.False(t, errors.Is(items[1].Err, errors.ErrTxNotFound),
		"a row the store HOLDS but cannot decode is a storage fault, not a missing parent: %v", items[1].Err)
}

// TestGetReportsACorruptRowAsAFaultNotAMiss keeps the two answers apart on the single read.
//
// The distinction is load-bearing downstream. The validator turns a not-found parent into
// TxMissingParent and rejects the child, which is a correct verdict about the child. A decode
// fault is a statement about this node's storage, and dressing it up as a missing parent would
// have the node reject a perfectly valid child and blame the sender.
func TestGetReportsACorruptRowAsAFaultNotAMiss(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 6_003)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	corruptInpoints(t, s, ctx, tx)

	_, err = s.Get(ctx, tx.TxIDChainHash())
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxNotFound),
		"want the decode fault, not a miss: %v", err)
}

// TestGetDoesNotAnswerACorruptIdentityRowFromTheCoin pins the other half of the fix, and it is
// the half that could go wrong silently.
//
// The read order falls through to the next step for a transaction the current step did not
// find. A transaction whose identity row is corrupt WAS found, and it still has live coins, so
// treating the fault as "not found here" would send it to the coin step, which would answer
// happily with the thin coin-derived record. The caller would then get a success carrying no
// inpoints and no size, with nothing anywhere saying the real record was unreadable.
func TestGetDoesNotAnswerACorruptIdentityRowFromTheCoin(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 6_004)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	// The fall-through the fault must not take: the coins are there and would answer.
	require.Equal(t, 2, coinCount(t, s, ctx, tx))

	corruptInpoints(t, s, ctx, tx)

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.Error(t, err, "a corrupt identity row must not be answered from the coin")
	require.Nil(t, got)
}
