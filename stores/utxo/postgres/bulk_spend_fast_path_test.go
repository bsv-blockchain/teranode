package postgres

// TestBulkSpend_* — tests for the BULK spend path (bulkSpendSQL with the
// skip_hash[] $9 array) that only runs when the spend batcher is armed.
//
// Background
//
// setupTestStore calls New() but NOT Start(), so spendBatcher is nil and every
// Spend call hits spendDirect (the single-item CTE path). Calling
// store.Start(ctx) arms the batcher. Inside trySendSpendBatch, len(batch)==1
// is handled by the direct single-item branch — which also does NOT use
// bulkSpendSQL. To reach the true bulk SQL path, the batcher must accumulate
// ≥2 items in one flush, which is reliably achieved by a spending tx with ≥2
// inputs: Spend() enqueues one batchSpendItem per input, and both arrive before
// the flush timer fires because they are enqueued synchronously in the same
// goroutine.
//
// Mixed-flag batch note (TestBulkSpend_MixedSkipHash)
//
// Spend() accepts a single IgnoreFlags applied uniformly to ALL inputs of one
// tx, so one Spend call cannot produce a batch with mixed skip_hash values.
// Two concurrent Spend calls with different flags CAN coalesce in the batcher.
// This test uses goroutines to race two single-input Spend calls. Whether they
// coalesce or not, the per-Spend semantic assertions remain correct: skip=true
// succeeds, skip=false with a wrong hash returns ErrUtxoHashMismatch.

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// startBatchedStore arms the spend batcher on an existing store.
// SpendBatcherSize is set large so the flush fires on the timer, not the
// size cap, ensuring both items enqueued by a 2-input Spend reach the batcher
// before the callback runs.
func startBatchedStore(store *Store, ctx context.Context) {
	store.settings.UtxoStore.SpendBatcherSize = 500
	store.settings.UtxoStore.SpendBatcherDurationMillis = 10
	store.Start(ctx)
}

// buildTwoInputSpendTx builds a spending tx that references parentA:0 and
// parentB:0 with no extended input data (outpoint-only). A non-zero extraSats
// ensures the output amount varies, producing distinct txids across calls.
func buildTwoInputSpendTx(t *testing.T, parentA, parentB *bt.Tx, extraSats uint64) *bt.Tx {
	t.Helper()
	spendTx := bt.NewTx()

	inputA := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, inputA.PreviousTxIDAdd(parentA.TxIDChainHash()))
	spendTx.Inputs = append(spendTx.Inputs, inputA)

	inputB := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, inputB.PreviousTxIDAdd(parentB.TxIDChainHash()))
	spendTx.Inputs = append(spendTx.Inputs, inputB)

	_ = spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000+extraSats)
	return spendTx
}

// TestBulkSpend_AllSkipHash verifies that bulkSpendSQL correctly handles a
// batch where every row has skip_hash=true (below-checkpoint outpoint-only
// scenario: zero UTXOHash values must be accepted).
//
// Bulk path forced by: Store.Start() + spending tx with 2 inputs.
func TestBulkSpend_AllSkipHash(t *testing.T) {
	store, ctx := setupTestStore(t)
	startBatchedStore(store, ctx)

	const blockHeight = uint32(100)
	require.NoError(t, store.SetBlockHeight(blockHeight))

	// Two un-extended parents (fee=0, SkipExtendedInputs).
	parentA := buildMinimalUnextendedParent(t, 50_000)
	parentB := buildMinimalUnextendedParent(t, 60_000)
	_, err := store.Create(ctx, parentA, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	_, err = store.Create(ctx, parentB, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// 2-input tx: enqueues 2 batchSpendItems → forced into bulkSpendSQL.
	spendTx := buildTwoInputSpendTx(t, parentA, parentB, 0)

	spends, err := store.Spend(ctx, spendTx, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)
	require.Len(t, spends, 2, "2 inputs → 2 successful spends")

	// Verify both outputs recorded in spends table.
	var count int
	err = store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = 0`,
		parentA.TxIDChainHash()[:],
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "parentA:0 must appear in spends")

	err = store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM spends WHERE prev_tx_hash = $1 AND prev_output_idx = 0`,
		parentB.TxIDChainHash()[:],
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "parentB:0 must appear in spends")
}

// TestBulkSpend_MixedSkipHash tests that, when two Spend calls with different
// IgnoreFlags coalesce into the same batcher flush, bulkSpendSQL's per-row
// skip_hash[] correctly routes each item:
//   - skip_hash=true: zero UTXOHash → accepted (parentSkip)
//   - skip_hash=false + wrong hash: hash mismatch → ErrUtxoHashMismatch
//
// True coalescing is timing-dependent (two goroutines racing into the batcher).
// Whether or not they coalesce, the semantic assertions below are always valid.
func TestBulkSpend_MixedSkipHash(t *testing.T) {
	store, ctx := setupTestStore(t)
	startBatchedStore(store, ctx)

	const blockHeight = uint32(100)
	require.NoError(t, store.SetBlockHeight(blockHeight))

	// parentSkip: un-extended — spent with SkipUTXOHashCheck=true (zero UTXOHash).
	parentSkip := buildMinimalUnextendedParent(t, 50_000)
	_, err := store.Create(ctx, parentSkip, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// parentNoSkip: extended — spent with SkipUTXOHashCheck=false, but the
	// spending tx carries a corrupted PreviousTxScript so the computed UTXOHash
	// differs from what was stored at create time.
	parentNoSkip := testExtendedTx(t)
	_, err = store.Create(ctx, parentNoSkip, blockHeight)
	require.NoError(t, err)

	// Outpoint-only spend of parentSkip (skip_hash=true).
	spendSkipTx := buildSpendingTxOutpointOnly(t, parentSkip, 0, 0)

	// Spend of parentNoSkip:0 with a deliberately wrong previous script.
	spendNoSkipTx := getSpendingTx(t, parentNoSkip, 0)
	badScript, badErr := bscript.NewP2PKHFromAddress("1CounterpartyXXXXXXXXXXXXXXXUWLpVr")
	require.NoError(t, badErr)
	spendNoSkipTx.Inputs[0].PreviousTxScript = badScript

	// Race both Spend calls so they may coalesce into one bulk flush.
	var (
		wg        sync.WaitGroup
		errSkip   error
		errNoSkip error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errSkip = store.Spend(ctx, spendSkipTx, blockHeight+1,
			utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	}()
	go func() {
		defer wg.Done()
		_, errNoSkip = store.Spend(ctx, spendNoSkipTx, blockHeight+1,
			utxo.IgnoreFlags{IgnoreLocked: false, SkipUTXOHashCheck: false})
	}()
	wg.Wait()

	// skip=true: zero UTXOHash must be accepted.
	require.NoError(t, errSkip, "SkipUTXOHashCheck=true with zero UTXOHash must succeed")

	// skip=false + wrong hash: must be rejected with a hash mismatch error.
	require.Error(t, errNoSkip, "wrong UTXOHash without SkipUTXOHashCheck must be rejected")
	require.True(t, errors.Is(errNoSkip, errors.ErrUtxoHashMismatch),
		"expected ErrUtxoHashMismatch for wrong hash without skip: got %v", errNoSkip)
}

// TestBulkSpend_DoubleSpend verifies that the ON CONFLICT DO NOTHING guard in
// bulkSpendSQL rejects a second bulk spend of already-spent outpoints by a
// different tx, even when SkipUTXOHashCheck=true.
//
// Bulk path forced by: Store.Start() + 2-input spending txs.
func TestBulkSpend_DoubleSpend(t *testing.T) {
	store, ctx := setupTestStore(t)
	startBatchedStore(store, ctx)

	const blockHeight = uint32(100)
	require.NoError(t, store.SetBlockHeight(blockHeight))

	parentA := buildMinimalUnextendedParent(t, 50_000)
	parentB := buildMinimalUnextendedParent(t, 60_000)
	_, err := store.Create(ctx, parentA, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	_, err = store.Create(ctx, parentB, blockHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// First bulk spend — must succeed.
	spendFirst := buildTwoInputSpendTx(t, parentA, parentB, 1)
	spends, err := store.Spend(ctx, spendFirst, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)
	require.Len(t, spends, 2, "first bulk spend: both inputs succeed")

	// Second bulk spend of the SAME outpoints by a DIFFERENT tx (distinct txid).
	spendSecond := buildTwoInputSpendTx(t, parentA, parentB, 2)
	require.NotEqual(t,
		spendFirst.TxIDChainHash().String(),
		spendSecond.TxIDChainHash().String(),
		"test invariant: first and second spenders must have distinct txids",
	)

	_, err = store.Spend(ctx, spendSecond, blockHeight+1,
		utxo.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.Error(t, err, "double-spend must be rejected even with SkipUTXOHashCheck")
	require.True(t, errors.Is(err, errors.ErrSpent),
		"ON CONFLICT double-spend must return ErrSpent: got %v", err)
}
