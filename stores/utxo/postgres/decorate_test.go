package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

const testSpendScript = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

// buildDecorateFan creates a single parent tx with n outputs (funded by the
// 50-BTC output of the canonical test tx, so there is no value depletion), then
// builds n in-memory children where child i references parent output i. The
// children are NOT created in the store — BatchPreviousOutputsDecorate only
// reads the *parent* outputs, which do exist. Returns the children plus the
// expected previous-output script/satoshis captured while extended.
func buildDecorateFan(t *testing.T, store *Store, ctx context.Context, n int) (children []*bt.Tx, wantScript [][]byte, wantSats []uint64) {
	t.Helper()

	root := testExtendedTx(t)
	_, err := store.Create(ctx, root, 100)
	require.NoError(t, err)

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		root.TxIDChainHash().String(), 0,
		root.Outputs[0].LockingScript.String(), root.Outputs[0].Satoshis,
	))
	for i := 0; i < n; i++ {
		require.NoError(t, parent.PayToAddress(testSpendScript, 1_000_000))
	}
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
	_, err = store.Create(ctx, parent, 101)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash().String()
	for i := 0; i < n; i++ {
		c := bt.NewTx()
		require.NoError(t, c.From(
			parentHash, uint32(i),
			parent.Outputs[i].LockingScript.String(), parent.Outputs[i].Satoshis,
		))
		c.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		require.NotNil(t, c.Inputs[0].PreviousTxScript, "child %d must start extended", i)
		wantScript = append(wantScript, *c.Inputs[0].PreviousTxScript)
		wantSats = append(wantSats, c.Inputs[0].PreviousTxSatoshis)
		children = append(children, c)
	}
	return children, wantScript, wantSats
}

func clearExtension(txs []*bt.Tx) {
	for _, tx := range txs {
		tx.Inputs[0].PreviousTxScript = nil
		tx.Inputs[0].PreviousTxSatoshis = 0
	}
}

// TestBatchPreviousOutputsDecorateConcurrency verifies the parallel chunk
// implementation produces identical results to the serial (concurrency=1) path
// across a range of worker counts, with enough distinct parent outpoints to span
// several chunks. Run with -race to catch data races on the disjoint writes.
func TestBatchPreviousOutputsDecorateConcurrency(t *testing.T) {
	store, ctx := setupTestStore(t)

	// 200 distinct parent outpoints → multiple chunks once concurrency > 1
	// (chunk size floors at minDecorateChunkSize=50).
	const n = 200
	children, wantScript, wantSats := buildDecorateFan(t, store, ctx, n)

	for _, concurrency := range []int{1, 2, 8, 32} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			clearExtension(children)
			store.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency = concurrency

			err := store.BatchPreviousOutputsDecorate(ctx, children)
			require.NoError(t, err)

			for i, tx := range children {
				require.NotNil(t, tx.Inputs[0].PreviousTxScript, "child %d not decorated", i)
				require.Equal(t, wantScript[i], []byte(*tx.Inputs[0].PreviousTxScript), "child %d script", i)
				require.Equal(t, wantSats[i], tx.Inputs[0].PreviousTxSatoshis, "child %d satoshis", i)
			}
		})
	}
}

// TestBatchPreviousOutputsDecorateMissing verifies the parallel path still
// reports an error when a referenced parent output does not exist (here an
// out-of-range output index), regardless of which worker chunk it lands in.
func TestBatchPreviousOutputsDecorateMissing(t *testing.T) {
	store, ctx := setupTestStore(t)

	children, _, _ := buildDecorateFan(t, store, ctx, 60)
	clearExtension(children)

	// Point one child's input at an output index that was never created.
	children[30].Inputs[0].PreviousTxOutIndex = 99999

	store.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency = 8
	err := store.BatchPreviousOutputsDecorate(ctx, children)
	require.Error(t, err, "decorate must error when a parent output is missing")
}
