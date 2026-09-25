package postgres

import (
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestBlockHeightInt32Guard covers the uint32→INT4 height guard: a height above
// MaxInt32 must be rejected at the write entry points rather than silently wrapping.
func TestBlockHeightInt32Guard(t *testing.T) {
	store, _ := setupTestStore(t)

	require.NoError(t, store.SetBlockHeight(uint32(math.MaxInt32)), "max int32 height is valid")
	require.Error(t, store.SetBlockHeight(uint32(math.MaxInt32)+1), "height beyond int32 must be rejected")
}

// TestSpendOpReturnOutputRejected is the regression test for the out_spendables
// spend-guard: a non-spendable output (OP_RETURN) carries a utxo_hash but must NOT
// be spendable. Presenting its correct hash must be rejected (TxNotFound), matching
// the aerospike and sql stores. Sibling spendable outputs must still spend.
func TestSpendOpReturnOutputRejected(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parent := makeThreeOutputTx(t) // vout0 + vout2 spendable; vout1 OP_RETURN
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	// Spending the OP_RETURN output (vout1) must be rejected.
	opReturnChild := getSpendingTx(t, parent, 1)
	_, err = store.Spend(ctx, opReturnChild, 101)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"spending an OP_RETURN output must be TxNotFound, got: %v", err)

	// A genuine spendable sibling output must still spend.
	spendable := getSpendingTx(t, parent, 0)
	_, err = store.Spend(ctx, spendable, 101)
	require.NoError(t, err, "spendable sibling output must still be spendable")
}

// TestBatchedSpendOpReturnRejected exercises the out_spendables guard on the BULK
// spend path: a child whose inputs include an OP_RETURN output (2 inputs → bulk
// batch) must be rejected as not-found, matching the direct-path behaviour.
func TestBatchedSpendOpReturnRejected(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))
	store.Start(ctx) // enable batchers → multi-input Spend hits the bulk path

	parent := makeThreeOutputTx(t) // vout0 spendable, vout1 OP_RETURN
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	for _, idx := range []uint32{0, 1} { // spendable + OP_RETURN → batch of 2
		require.NoError(t, child.From(
			parent.TxIDChainHash().String(), idx,
			parent.Outputs[idx].LockingScript.String(), parent.Outputs[idx].Satoshis))
	}
	_ = child.PayToAddress(testSpendScript, 1000)
	for _, in := range child.Inputs {
		if in.UnlockingScript == nil || len(*in.UnlockingScript) == 0 {
			in.UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		}
	}

	_, err = store.Spend(ctx, child, 101)
	require.Error(t, err, "a batched spend including an OP_RETURN input must fail")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got: %v", err)
}

// TestUnsetMinedClearsDAHOnPartialReorg verifies the DAH stamp is cleared on ANY
// unset-mined, including a partial reorg where other block_ids remain. Previously
// the stamp was preserved when blocks remained, leaving a stale stamp the pruner
// (which trusts the stamp without re-validation) could act on.
func TestUnsetMinedClearsDAHOnPartialReorg(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	tx := newMinedSingleOutputTx(t, store, 100) // block_ids = [100]
	h := tx.TxIDChainHash()

	// Add a second block so removing one block is a PARTIAL reorg.
	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 101, BlockHeight: 101, OnLongestChain: true,
	})
	require.NoError(t, err)

	// Force a deferred-prune stamp.
	_, err = store.pool.Exec(ctx, `UPDATE txs SET delete_at_height = 5000 WHERE hash = $1`, h[:])
	require.NoError(t, err)

	// Partial reorg: remove only block 100; block 101 remains.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: 100, UnsetMined: true,
	})
	require.NoError(t, err)

	var (
		dah     *int64
		nBlocks int
	)
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height, COALESCE(octet_length(mined_info), 0) / 12 FROM txs WHERE hash = $1`,
		h[:]).Scan(&dah, &nBlocks))
	require.Equal(t, 1, nBlocks, "one block should remain (partial reorg)")
	require.Nil(t, dah, "partial reorg must clear delete_at_height unconditionally")
}

// TestUnsetMinedClearsMinedAtHeightOnFullReorg verifies mined_at_height is cleared
// when the tx is fully reorged out, so the DAH sweep stops re-enumerating it as a
// phantom candidate on every pass over the old mining height.
func TestUnsetMinedClearsMinedAtHeightOnFullReorg(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	mineTx(t, store, tx, 100) // SetMinedMulti(OnLongestChain) sets mined_at_height

	var mahBefore *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT mined_at_height FROM txs WHERE hash = $1`, h[:]).Scan(&mahBefore))
	require.NotNil(t, mahBefore, "mined_at_height must be set after mining")

	// Full reorg: remove the only block.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: 100, UnsetMined: true,
	})
	require.NoError(t, err)

	var mahAfter *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT mined_at_height FROM txs WHERE hash = $1`, h[:]).Scan(&mahAfter))
	require.Nil(t, mahAfter, "full reorg must clear mined_at_height")
}
