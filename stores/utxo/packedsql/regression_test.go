package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// unspendableScript is OP_FALSE OP_RETURN, provably unspendable in every era, so
// ShouldStoreOutputAsUTXO keeps it out of the UTXO set and its packed slot stays zeroed.
func unspendableScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0x02})
}

// newTxWithUnspendableFirstOutput builds a transaction whose vout 0 is not a UTXO and
// whose remaining outputs are. The unspendable output at index 0 shifts every later vout
// away from its "spendable index", which is what makes slot addressing bugs observable.
func newTxWithUnspendableFirstOutput(t testing.TB, spendableOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: unspendableScript()})

	for i := 0; i < spendableOutputs; i++ {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(2000+i)))
	}

	return tx
}

// TestOutpointOnlySpendRejectsNonUTXOOutput pins that an outpoint-only spend cannot spend
// an output that was never stored as a UTXO. Such a slot has a zeroed spend slot (so it
// looks unspent) and a zeroed hash slot, so without the non-zero-hash guard the UPDATE
// would succeed, inflate spent_count past page0_count and tombstone the row early.
func TestOutpointOnlySpendRejectsNonUTXOOutput(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newTxWithUnspendableFirstOutput(t, 2)

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	var (
		page0Count int
		spendsLen  int
	)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT page0_count, octet_length(spends) FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&page0Count, &spendsLen))

	// Three outputs, one slot each, but only two of them are UTXOs.
	require.Equal(t, 2, page0Count)
	require.Equal(t, 3*slotSpendSize, spendsLen)

	outpointOnly := utxo.IgnoreFlags{SkipUTXOHashCheck: true}

	spends, err := store.Spend(ctx, newSpendingTx(t, parent, 0), 101, outpointOnly)
	require.Error(t, err)
	require.Len(t, spends, 1)
	require.Error(t, spends[0].Err)

	// The failed spend must not have been recorded anywhere.
	require.Equal(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))

	var (
		spentCount int
		dah        *int64
	)

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_count, delete_at_height FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&spentCount, &dah))
	require.Zero(t, spentCount)
	require.Nil(t, dah)

	// A real UTXO on the same row is still spendable outpoint-only.
	spends, err = store.Spend(ctx, newSpendingTx(t, parent, 1), 101, outpointOnly)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

// TestSpendRejectsNonUTXOOutputWithHashCheck covers the same slot on the hash-checking
// path, where the zeroed hash slot can never match a real utxo hash.
func TestSpendRejectsNonUTXOOutputWithHashCheck(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newTxWithUnspendableFirstOutput(t, 1)

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, newSpendingTx(t, parent, 0), 101)
	require.Error(t, err)
	require.Error(t, spends[0].Err)
	require.Equal(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))
}

// TestDeleteAtHeightWithEmptyOverflowPage pins that a transaction whose overflow page holds
// no spendable output still becomes prunable. Such a page can never be completed by a
// spend, so unless it is counted as complete at creation time pages_spent can never reach
// pages_total and the row leaks forever.
func TestDeleteAtHeightWithEmptyOverflowPage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	pageSize := int(store.pageSize)

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	// Page 0 is entirely spendable; page 1 is entirely unspendable.
	for i := 0; i < pageSize; i++ {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(3000+i)))
	}

	for i := 0; i < 3; i++ {
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: unspendableScript()})
	}

	_, err := store.Create(ctx, tx, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	var pagesTotal, pagesSpent int

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT pages_total, pages_spent FROM packed_txs WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&pagesTotal, &pagesSpent))
	require.Equal(t, 1, pagesTotal)
	require.Equal(t, 1, pagesSpent, "an overflow page with no spendable output must count as complete")

	vouts := make([]uint32, 0, pageSize)
	for i := 0; i < pageSize; i++ {
		vouts = append(vouts, uint32(i)) //nolint:gosec
	}

	_, err = store.Spend(ctx, newSpendingTx(t, tx, vouts...), 101)
	require.NoError(t, err)

	var dah *int64

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "fully spent transaction must be tombstoned")
}

// TestPreviousOutputsDecorateOutOfRangeVout pins that an input referencing a vout past the
// end of the parent's output list is reported as not found. The offsets are read out of the
// blob header, so an unchecked vout would reinterpret payload bytes as a span.
func TestPreviousOutputsDecorateOutOfRangeVout(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 2, 50_000)

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	var outputsLen int

	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT octet_length(outputs) FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&outputsLen))

	// vout 5 reads bytes 20..27 of the blob, which is payload, not header: an unchecked
	// read would decode those bytes as a span instead of falling off the end of the column.
	require.Greater(t, outputsLen, 28)

	for _, vout := range []uint32{2, 5, 99} {
		child := newSpendingTx(t, parent, 0)
		child.Inputs[0].PreviousTxOutIndex = vout
		child.Inputs[0].PreviousTxScript = nil

		err = store.PreviousOutputsDecorate(ctx, child)
		require.Error(t, err, "vout %d", vout)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Nil(t, child.Inputs[0].PreviousTxScript)
	}
}

// TestSpendAfterCloseIsRejected pins that racing a Spend against Close reports a closed
// store instead of panicking on a send to a closed channel.
func TestSpendAfterCloseIsRejected(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	parent := newExtendedTx(t, 2, 60_000)

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	require.NoError(t, store.Close(ctx))

	require.NotPanics(t, func() {
		_, err = store.Spend(ctx, newSpendingTx(t, parent, 0), 101)
	})
	require.Error(t, err)
}
