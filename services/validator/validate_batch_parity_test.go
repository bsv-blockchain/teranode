package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/stretchr/testify/require"
)

// TestValidateBatch_ParityWithValidate_NonExtendedTx asserts that
// Validate(tx) and ValidateBatch([tx]) produce equivalent outcomes for
// the same input.
//
// Setup:
//   - The parent tx is seeded into the real sqlitememory UTXO store (v.utxoStore)
//     so that the direct Validate path can hydrate input fields from the store.
//   - The validator's batch-store override is a stub whose BatchGetParents reports
//     the parent as present — this causes ValidateBatch to run the six-phase
//     pipeline and exposes the missing-hydration bug.
//   - The child tx is submitted NON-EXTENDED: PreviousTxSatoshis and
//     PreviousTxScript are zero/nil, matching the wire shape.
//
// Expected failure with the current implementation:
//   - Direct path  (UseBatchValidation=false): validateInternal calls
//     getTransactionInputBlockHeightsAndExtendTx, which populates
//     PreviousTxSatoshis=1000 from the store.  checkFees sees
//     inputSats(1000) >= outputSats(500) and accepts the tx.
//     errDirect == nil.
//   - Batch native path (UseBatchValidation=true, stub store): Phase B calls
//     v.txValidator.ValidateTransaction on the still-unhydrated tx.
//     checkFees sees inputSats(0) < outputSats(500) and rejects.
//     errBatchPerTx != nil.
//
// This divergence (one path accepts, the other rejects the same wire tx) is
// the bug: Phase A fetches parent records but does NOT hydrate
// input.PreviousTxSatoshis / input.PreviousTxScript from them.
// The test CURRENTLY FAILS and will continue to fail until Phase A is fixed
// to hydrate input fields from the fetched parent records (spec §4.3).
func TestValidateBatch_ParityWithValidate_NonExtendedTx(t *testing.T) {
	ctx := context.Background()

	// newNativeValidator wires a stubBatchStore so getBatchUtxoStore() returns
	// it and ValidateBatch runs the six-phase pipeline. The validator's
	// v.utxoStore is still the real sqlitememory store, so the direct
	// Validate path can hydrate inputs.
	v, stub := newNativeValidator(t)

	// 1. Build the parent tx and seed it into the REAL UTXO store (sqlitememory).
	//    This is what the direct Validate path will hydrate from.
	parent := buildParentTxForParityTest(t)
	_, err := v.utxoStore.Create(ctx, parent, 0)
	require.NoError(t, err, "seed parent into utxoStore")

	// 2. Configure the stub so Phase A sees the parent as present.
	//    The stub ParentRecord carries the parent's outputs so that Phase A
	//    can hydrate PreviousTxScript and PreviousTxSatoshis in-memory without
	//    any extra store calls — exactly the same way BatchGetParents does in
	//    production after it decodes the Outputs bin at fetch time.
	parentHash := parent.TxIDChainHash()
	var parentKey [32]byte
	copy(parentKey[:], parentHash[:])
	stub.parents = map[[32]byte]*aerospike.ParentRecord{
		parentKey: {BlockHeight: 1, Outputs: parent.Outputs},
	}

	// 3. Build the child tx spending parent.Outputs[0].
	//    The child is NOT pre-extended: PreviousTxSatoshis and PreviousTxScript
	//    are zero/nil, matching what arrives from the wire.
	child := buildChildTxForParityTest(t, parent)
	require.False(t, child.IsExtended(), "child must be non-extended for this test")

	// 4. Validate via the per-tx path (no native batch pipeline).
	v.settings.Validator.UseBatchValidation = false
	_, errDirect := v.Validate(ctx, child, 0)

	// 5. Validate via the batched native path on a DEEP COPY so direct-path
	//    mutations (IsExtended flag, PreviousTxSatoshis set by hydration) do not
	//    bleed into the batch-path tx.
	v.settings.Validator.UseBatchValidation = true
	childBytes := child.Bytes()
	childCopy, err := bt.NewTxFromBytes(childBytes)
	require.NoError(t, err, "deep copy child tx")

	results, errBatch := v.ValidateBatch(ctx, []*bt.Tx{childCopy}, 0)
	require.NoError(t, errBatch, "whole-batch error must be nil; per-tx outcomes live in results[i].Err")
	require.Len(t, results, 1)
	errBatchPerTx := results[0].Err

	// 6. Log both outcomes so the test failure message is informative.
	t.Logf("direct  err: %v", errDirect)
	t.Logf("batched err: %v", errBatchPerTx)

	// 7. Parity assertion: both paths must agree.
	if errDirect == nil && errBatchPerTx == nil {
		// Both accepted — parity holds.  Surprising if seen before the fix; see
		// investigation notes in the task description.
		return
	}
	if errDirect != nil && errBatchPerTx != nil {
		require.Equal(t, errDirect.Error(), errBatchPerTx.Error(),
			"both paths rejected but for different reasons — parity still broken")
		return
	}
	// One accepted, the other rejected: clear parity violation.
	t.Fatalf("parity violation — direct err=%v, batched err=%v", errDirect, errBatchPerTx)
}

// buildParentTxForParityTest builds a minimal coinbase-shaped parent tx with one
// OP_TRUE output worth 1000 satoshis.
//
// We make it a real coinbase tx (all-zeros PreviousTxID, vout=0xFFFFFFFF) so that
// sql.Store.Create accepts it:
//   - util.GetFees for coinbase = sum(outputs) = 1000, no negative check
//   - IsCoinbase() = true → the sql schema marks it correctly
//
// We seed this tx directly into the UTXO store — the validator never
// validates it, so coinbase maturity restrictions do not apply (blockHeight=0
// places the maturity cutoff in the past when testing without a real chain).
func buildParentTxForParityTest(t *testing.T) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()

	// Coinbase input: all-zero PreviousTxID + vout=0xFFFFFFFF satisfies IsCoinbase().
	emptyScript := bscript.Script{}
	in := &bt.Input{
		PreviousTxOutIndex: 0xFFFFFFFF, // marks coinbase
		PreviousTxScript:   &emptyScript,
		PreviousTxSatoshis: 0,
		UnlockingScript:    &emptyScript,
		SequenceNumber:     0xFFFFFFFF,
	}
	// PreviousTxIDAdd with all-zero hash (default) satisfies IsCoinbase().
	zeroHash := chainhash.Hash{}
	require.NoError(t, in.PreviousTxIDAdd(&zeroHash))
	tx.Inputs = append(tx.Inputs, in)

	// One OP_TRUE output worth 1000 satoshis.
	opTrue, err := bscript.NewFromHexString("51") // OP_1 / OP_TRUE
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      1000,
		LockingScript: opTrue,
	})
	return tx
}

// buildChildTxForParityTest constructs a tx that spends parent.Outputs[0].
// The returned tx is NON-EXTENDED: PreviousTxSatoshis and PreviousTxScript
// are zero/nil, exactly as they arrive from the network wire.
// UnlockingScript is set to an empty (non-nil) script so that sql.Store.Create
// can persist the child after the direct Validate path accepts it.
func buildChildTxForParityTest(t *testing.T, parent *bt.Tx) *bt.Tx {
	t.Helper()
	child := bt.NewTx()

	ph := parent.TxIDChainHash()
	emptyScript := bscript.Script{}
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		// Deliberately leave PreviousTxSatoshis and PreviousTxScript unset
		// to simulate a non-extended (wire-format) transaction.
		UnlockingScript: &emptyScript,
	}
	require.NoError(t, in.PreviousTxIDAdd(ph))
	child.Inputs = append(child.Inputs, in)

	// One P2PKH output paying 500 satoshis (less than the parent's 1000,
	// so fee = 500 satoshis — valid when properly hydrated).
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
	return child
}
