package postgres

// Tests for the below-checkpoint outpoint-only fast path:
//   - SkipUTXOHashCheck on Spend (IgnoreFlags)
//   - WithSkipExtendedInputs on Create (CreateOption)
//
// These mirror the tests in stores/utxo/sql/sql_test.go
// (TestOutpointOnlySpend_SkipsHashCheck, TestMinimalCreate_FeeZero_OutputsIntact,
// TestSpend_FlagOff_StillEnforcesHash, TestDoubleSpend).

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// buildMinimalUnextendedParent builds a parent tx whose inputs carry NO
// PreviousTxSatoshis or PreviousTxScript (simulating a below-checkpoint
// un-extended tx). It has one spendable output of the given satoshi value.
func buildMinimalUnextendedParent(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()
	parent := bt.NewTx()
	// Coinbase-style dummy input: zero PreviousTxSatoshis and nil PreviousTxScript
	// (un-extended). PreviousTxIDAdd sets the private previousTxIDHash field.
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)
	require.NoError(t, coinbaseInput.PreviousTxIDAdd(zeroHash))
	parent.Inputs = append(parent.Inputs, coinbaseInput)
	_ = parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis)
	return parent
}

// buildSpendingTxOutpointOnly builds a spending tx that references parentTx:vOut
// but carries no PreviousTxSatoshis or PreviousTxScript (un-extended, fast-path).
// extraSats varies the output amount so callers can produce distinct txids.
func buildSpendingTxOutpointOnly(t *testing.T, parentTx *bt.Tx, vOut uint32, extraSats uint64) *bt.Tx {
	t.Helper()
	spendTx := bt.NewTx()
	emptyUnlocking := bscript.Script{}
	input := &bt.Input{
		PreviousTxOutIndex: vOut,
		// No PreviousTxSatoshis, no PreviousTxScript — un-extended.
		UnlockingScript: &emptyUnlocking,
	}
	require.NoError(t, input.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	spendTx.Inputs = append(spendTx.Inputs, input)
	_ = spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100+extraSats)
	return spendTx
}

// TestFastPath_OutpointOnlySpend_Succeeds verifies that Spend with
// IgnoreFlags{SkipUTXOHashCheck:true} succeeds when the spend carries a zero
// UTXOHash (as produced by GetSpendsOutpointOnly). This is the primary fast-path
// scenario: below-checkpoint sync sends un-extended txs.
func TestFastPath_OutpointOnlySpend_Succeeds(t *testing.T) {
	store, ctx := setupTestStore(t)

	const blockHeight = uint32(100)
	parent := buildMinimalUnextendedParent(t, 50000)

	// Create the parent using WithSkipExtendedInputs so fee=0 does not error.
	_, err := store.Create(ctx, parent, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// Build spending tx (no extended inputs).
	spendTx := buildSpendingTxOutpointOnly(t, parent, 0, 0)

	// Spend with SkipUTXOHashCheck: the zero UTXOHash must be accepted.
	spends, err := store.Spend(ctx, spendTx, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)
	require.Len(t, spends, 1, "one input -> one spend")
}

// TestFastPath_FlagOff_StillEnforcesHash verifies that when SkipUTXOHashCheck is
// NOT set (the default), a mismatched UTXO hash is still rejected with
// ErrUtxoHashMismatch — the flag is strictly default-off.
// Uses a fully-extended spending tx whose PreviousTxScript is corrupted to produce
// a wrong UTXOHash at spend time, while the stored hash is correct.
func TestFastPath_FlagOff_StillEnforcesHash(t *testing.T) {
	store, ctx := setupTestStore(t)

	const blockHeight = uint32(100)
	// Use a real extended parent so the utxo_hash is non-zero and anchored.
	parent := testExtendedTx(t)
	_, err := store.Create(ctx, parent, blockHeight)
	require.NoError(t, err)

	// Build a spending tx that references parent:0 with the CORRECT script (from getSpendingTx),
	// then corrupt it so UTXOHashFromInput computes a different hash than what was stored.
	spendTx := getSpendingTx(t, parent, 0)

	// Corrupt the claimed previous script so the hash will differ from stored.
	badScript, err := bscript.NewP2PKHFromAddress("1CounterpartyXXXXXXXXXXXXXXXUWLpVr")
	require.NoError(t, err)
	spendTx.Inputs[0].PreviousTxScript = badScript

	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUtxoHashMismatch),
		"flag OFF must still enforce hash: got %v", err)
}

// TestFastPath_DoubleSpend_StillRejected verifies that even with SkipUTXOHashCheck=true
// the ON CONFLICT double-spend guard remains active: a second distinct spender of the
// same output is rejected with ErrSpent.
func TestFastPath_DoubleSpend_StillRejected(t *testing.T) {
	store, ctx := setupTestStore(t)

	const blockHeight = uint32(100)
	parent := buildMinimalUnextendedParent(t, 50000)
	_, err := store.Create(ctx, parent, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// Different extraSats give distinct output amounts -> distinct serialisations -> distinct txids.
	spendTxA := buildSpendingTxOutpointOnly(t, parent, 0, 1)
	spendTxB := buildSpendingTxOutpointOnly(t, parent, 0, 2)
	require.NotEqual(t, spendTxA.TxIDChainHash().String(), spendTxB.TxIDChainHash().String(),
		"test invariant: spendTxA and spendTxB must have distinct txids")

	// First spend must succeed.
	_, err = store.Spend(ctx, spendTxA, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)

	// Second spend (different tx, same output) must fail with ErrSpent.
	_, err = store.Spend(ctx, spendTxB, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent),
		"double-spend guard must fire even with SkipUTXOHashCheck: got %v", err)
}

// TestFastPath_CreateSkipExtendedInputs_OutputsIntact verifies that Create with
// WithSkipExtendedInputs(true) succeeds on an un-extended tx (fee=0 is fine) and
// that the outputs are fully persisted (the store can be Spent afterwards via the
// fast-path flag).
func TestFastPath_CreateSkipExtendedInputs_OutputsIntact(t *testing.T) {
	store, ctx := setupTestStore(t)

	const blockHeight = uint32(100)
	parent := buildMinimalUnextendedParent(t, 50000)

	// Create must succeed: fee=0, no extended input data.
	txMeta, err := store.Create(ctx, parent, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	require.NotNil(t, txMeta)
	// The v4 postgres schema stores per-input data as outpoints only (no satoshis/script
	// columns in spends or txs), so extended-input absence is structurally guaranteed.
	require.Equal(t, uint64(0), txMeta.Fee, "fee must be 0 when SkipExtendedInputs")

	// The output UTXOHash was computed correctly at create time. Fast-path Spend must work.
	spendTx := buildSpendingTxOutpointOnly(t, parent, 0, 0)
	_, err = store.Spend(ctx, spendTx, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)
}

// buildMinimalUnextendedParentMulti is buildMinimalUnextendedParent generalised to
// N spendable outputs, one per satoshi value in sats. Each output amount must be
// distinct across calls in a test to keep txids unique (the coinbase-style input
// is identical every time, so only the outputs vary the serialisation).
func buildMinimalUnextendedParentMulti(t *testing.T, sats ...uint64) *bt.Tx {
	t.Helper()
	parent := bt.NewTx()
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)
	require.NoError(t, coinbaseInput.PreviousTxIDAdd(zeroHash))
	parent.Inputs = append(parent.Inputs, coinbaseInput)
	for _, s := range sats {
		_ = parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", s)
	}
	return parent
}

// TestFastPathCrossModeSpend pins the post-IBD boundary of the below-checkpoint
// outpoint-only fast path: a parent created via the MINIMAL path (below
// checkpoint, WithSkipExtendedInputs + mined info) must still be spendable via a
// NORMAL, fully-decorated spend (as block validation would build once caught up
// past the checkpoint) with no SkipUTXOHashCheck flag. This proves the utxo_hash
// computed and stored at minimal-create time (from the output's own script and
// satoshis — never touched by WithSkipExtendedInputs) matches exactly what
// decoration derives from the parent's outputs. It also proves the hash check is
// genuinely live in this cross-mode scenario, not silently bypassed: a decorated
// spend built with a WRONG script must still fail with a hash mismatch.
func TestFastPathCrossModeSpend(t *testing.T) {
	store, ctx := setupTestStore(t)

	const mineHeight = uint32(100)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        mineHeight,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}

	// --- Part A: a correct, fully-decorated spend of a minimal-created parent must succeed.
	parentGood := buildMinimalUnextendedParent(t, 50000)
	_, err := store.Create(ctx, parentGood, mineHeight,
		utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)

	// getSpendingTx copies PreviousTxScript/PreviousTxSatoshis straight from
	// parentGood's own outputs — exactly what block-validation decoration does.
	// No IgnoreFlags: SkipUTXOHashCheck is off (the default), so the stored hash
	// really is compared against the decorated hash.
	spendTx := getSpendingTx(t, parentGood, 0)
	spends, err := store.Spend(ctx, spendTx, mineHeight+1)
	require.NoError(t, err, "decorated spend of a minimal-created parent must succeed: "+
		"stored utxo_hash must match the hash derived from decoration")
	require.Len(t, spends, 1)

	// --- Part B: same cross-mode scenario, but with a wrong script — must be rejected.
	parentBad := buildMinimalUnextendedParent(t, 60000)
	_, err = store.Create(ctx, parentBad, mineHeight,
		utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)

	badSpendTx := getSpendingTx(t, parentBad, 0)
	badScript, err := bscript.NewP2PKHFromAddress("1CounterpartyXXXXXXXXXXXXXXXUWLpVr")
	require.NoError(t, err)
	badSpendTx.Inputs[0].PreviousTxScript = badScript

	_, err = store.Spend(ctx, badSpendTx, mineHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUtxoHashMismatch),
		"decorated spend with a wrong script must still be rejected: got %v", err)
}

// TestFastPathMinimalCreatePruning pins the reclamation boundary of the
// below-checkpoint outpoint-only fast path: a parent created via the MINIMAL
// path must still drive the DAH sweep/pruner correctly once fully spent. This
// proves spendable_count/out_spendables recorded by minimal create (with no
// extended input data at all) are exactly what the sweep's fully-spent
// detection relies on.
func TestFastPathMinimalCreatePruning(t *testing.T) {
	store, ctx := setupTestStore(t)

	const mineHeight = uint32(100)
	const spendHeight = uint32(101)
	const safeTip = int64(105)

	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// Minimal-created parent (below checkpoint) with 2 spendable outputs.
	parent := buildMinimalUnextendedParentMulti(t, 50_000, 60_000)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        mineHeight,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := store.Create(ctx, parent, mineHeight,
		utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)

	// Spend ALL outputs, outpoint-only (fast-path); the hash check is not what
	// this test is pinning, so SkipUTXOHashCheck keeps the spends simple.
	spendTx0 := buildSpendingTxOutpointOnly(t, parent, 0, 0)
	_, err = store.Spend(ctx, spendTx0, spendHeight,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)

	spendTx1 := buildSpendingTxOutpointOnly(t, parent, 1, 1)
	_, err = store.Spend(ctx, spendTx1, spendHeight,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)

	// Self-check: the parent must now be genuinely fully spent, mirroring
	// spendAllOutputs' self-check in dah_sweep_test.go.
	parentHash := parent.TxIDChainHash()[:]
	var spendCount, spendableCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1`, parentHash).Scan(&spendCount))
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spendable_count FROM txs WHERE hash=$1`, parentHash).Scan(&spendableCount))
	require.Equal(t, spendableCount, spendCount,
		"minimal-created parent must be fully spent (count(spends) == spendable_count)")

	// Drive the DAH sweep across all partitions.
	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec // retention is a small positive test constant

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, parentHash).Scan(&dah))
	require.NotNil(t, dah, "fully-spent minimal-created parent must be stamped for pruning")
	require.Equal(t, int64(spendHeight)+1+ret, *dah,
		"DAH must be GREATEST(lastSpendHeight, minedHeight)+1+retention")

	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, parentHash).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "minimal-created parent must be mirrored into pending_deletes for reclamation")
	require.Equal(t, *dah, *pdDAH)
}
