package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

// TestReorgResetRecoversAssemblyTxPostgres verifies that after a SubtreeProcessor reset,
// a transaction that was in block assembly but had unmined_since=NULL in the UTXO store
// is recovered back into block assembly (not silently lost).
//
// See TestReorgResetRecoversAssemblyTxAerospike for the Aerospike variant.
func TestReorgResetRecoversAssemblyTxPostgres(t *testing.T) {
	t.Run("reset_recovers_assembly_tx_with_mined_utxo_state", func(t *testing.T) {
		testReorgResetRecoversAssemblyTx(t, "postgres")
	})
}

// TestReorgResetRecoversAssemblyTxAerospike is the Aerospike variant of TestReorgResetRecoversAssemblyTxPostgres.
func TestReorgResetRecoversAssemblyTxAerospike(t *testing.T) {
	t.Run("reset_recovers_assembly_tx_with_mined_utxo_state", func(t *testing.T) {
		testReorgResetRecoversAssemblyTx(t, "aerospike")
	})
}

// testReorgResetRecoversAssemblyTx demonstrates the bug where SubtreeProcessor.reset() fails
// to recover a block assembly transaction that has unmined_since=NULL in the UTXO store.
//
// Root cause: reset() clears assembly state (chainedSubtrees, currentSubtree) without first
// marking those transactions as NOT on longest chain. The subsequent loadUnminedTransactions()
// call uses the unmined_since index, so any transaction with unmined_since=NULL (mined) is
// invisible to the scan and permanently dropped from block assembly.
//
// This state (tx in assembly, unmined_since=NULL) can arise when a competing fork's
// BlockValidation processes a block containing the tx, marking it as "mined" even though
// the block has not yet become (or will not remain) the longest chain.
//
// The fix: call markNotOnLongestChain() on all assembly transactions before clearing state,
// mirroring what reorgBlocks() already does at lines 2665-2710 of SubtreeProcessor.go.
//
// Test flow:
//  1. Send txX via propagation → txX lands in block assembly with unmined_since set.
//  2. Manually set unmined_since=NULL for txX in the UTXO store, simulating the competing
//     fork scenario where BlockValidation marked it as mined.
//  3. Call ResetBlockAssembly() to force a SubtreeProcessor reset.
//  4. Assert txX is back in block assembly.
//     WITHOUT FIX: txX has unmined_since=NULL → loadUnminedTransactions misses it → LOST.
//     WITH FIX:    reset marks txX NOT on longest chain → loadUnminedTransactions finds it → RECOVERED.
func testReorgResetRecoversAssemblyTx(t *testing.T, utxoStoreType string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	// Generate 101 blocks so we have coinbase UTXOs to spend.
	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101}))

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create a transaction and propagate it into block assembly.
	txX := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txX))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txX, blockWait),
		"txX should be in block assembly after propagation")

	txXHash := txX.TxIDChainHash()

	// Simulate the scenario: a competing fork's BlockValidation marks txX as mined
	// (unmined_since=NULL) even though txX is still in block assembly waiting to be mined
	// on the main chain.
	require.NoError(t, td.UtxoStore.MarkTransactionsOnLongestChain(td.Ctx, []chainhash.Hash{*txXHash}, true),
		"simulating BlockValidation marking txX as mined on a competing fork")

	// Verify the injected state: txX is mined in the UTXO store (unmined_since=NULL).
	utxoMeta, err := td.UtxoStore.Get(td.Ctx, txXHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), utxoMeta.UnminedSince,
		"precondition: txX must have unmined_since=NULL (mined) before the reset")

	// Force a SubtreeProcessor reset via the block assembly API.
	// This triggers BlockAssembler.reset() → SubtreeProcessor.reset() → loadUnminedTransactions().
	require.NoError(t, td.BlockAssemblyClient.ResetBlockAssembly(td.Ctx),
		"ResetBlockAssembly must not fail")

	// After the reset, txX must be back in block assembly.
	//
	// WITHOUT THE FIX: txX has unmined_since=NULL in the UTXO store, so the unmined_since
	// index scan inside loadUnminedTransactions() skips it. txX is never re-added to block
	// assembly and is permanently lost — even though it was never mined on the actual main chain.
	//
	// WITH THE FIX: reset() calls markNotOnLongestChain() for all assembly txs before clearing
	// state, setting txX's unmined_since to a non-zero value. loadUnminedTransactions() finds
	// txX in the index and adds it back to block assembly.
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txX, blockWait),
		"txX must be recovered into block assembly after reset; "+
			"without the fix, assembly txs with unmined_since=NULL are silently dropped")
}
