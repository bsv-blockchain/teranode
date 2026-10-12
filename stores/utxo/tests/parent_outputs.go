package tests

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// parentOutputsTx builds a transaction with the given output values, whose one
// input names a made-up parent so the txid is unique per seed. It is stored with
// WithCreateOnly, so the parent never has to exist.
func parentOutputsTx(t *testing.T, seed byte, values ...uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	var prev chainhash.Hash
	prev[0] = seed
	prev[1] = 0xa5

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      &prev,
		Vout:          0,
		LockingScript: Tx.Inputs[0].PreviousTxScript,
		Satoshis:      Tx.Inputs[0].PreviousTxSatoshis,
	}))
	tx.Inputs[0].UnlockingScript = dummyUnlockingScript

	for _, v := range values {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", v))
	}

	return tx
}

// RequireAnswered fails the test if any slot is Unknown without an Err, the one
// state no store may ever return.
func RequireAnswered(t *testing.T, answers []utxostore.ParentOutput) {
	t.Helper()

	for i, a := range answers {
		if a.Status == utxostore.ParentOutputUnknown {
			require.Error(t, a.Err, "slot %d is Unknown with no Err", i)
		}
	}
}

// ParentOutputsForValidation pins the contract every store shares: one answer per
// outpoint in order, mined and not-mined parents, a missing transaction, an index
// past the end, the lowest recorded height for a parent in two blocks, identical
// answers for duplicates, and no mutation of anything the caller holds.
func ParentOutputsForValidation(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	mined := parentOutputsTx(t, 1, 1000, 2000, 3000)
	unmined := parentOutputsTx(t, 2, 4000)
	forked := parentOutputsTx(t, 3, 5000)

	// A parent as the seeder rebuilds it from a UTXO snapshot (cmd/seeder
	// processUTXO): no inputs, each unspent output at its index, the outputs
	// spent before the snapshot nil at theirs, and the txid supplied. Spending a
	// gap must read as NoSuchIndex, a verdict, never as a fault the caller would
	// retry for ever.
	seeded := &bt.Tx{Outputs: []*bt.Output{
		{Satoshis: 6000, LockingScript: Tx.Outputs[0].LockingScript},
		nil,
		{Satoshis: 8000, LockingScript: Tx.Outputs[0].LockingScript},
	}}

	var seededID chainhash.Hash
	seededID[0] = 0x5e
	seededID[1] = 0xed
	seededID[31] = 0x04

	var missing chainhash.Hash
	missing[0] = 0xee
	missing[31] = 0x01

	for _, tx := range []*bt.Tx{mined, unmined, forked} {
		_ = db.Delete(ctx, tx.TxIDChainHash())
		_, _, err := db.SpendAndCreate(ctx, tx, 100, utxostore.WithCreateOnly())
		require.NoError(t, err)
	}

	_ = db.Delete(ctx, &seededID)
	_, _, err := db.SpendAndCreate(ctx, seeded, 100, utxostore.WithCreateOnly(), utxostore.WithTXID(&seededID),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 100, SubtreeIdx: 0}))
	require.NoError(t, err)

	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{mined.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 11, BlockHeight: 101, SubtreeIdx: 0})
	require.NoError(t, err)

	// Record the higher height first and under the lower block id, so "first
	// recorded" and "lowest block id" both give 105 and only the minimum gives 103.
	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{forked.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 20, BlockHeight: 105, SubtreeIdx: 0})
	require.NoError(t, err)
	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{forked.TxIDChainHash()}, utxostore.MinedBlockInfo{BlockID: 21, BlockHeight: 103, SubtreeIdx: 0})
	require.NoError(t, err)

	outpoints := []utxostore.Outpoint{
		{TxID: *mined.TxIDChainHash(), Vout: 2},
		{TxID: *unmined.TxIDChainHash(), Vout: 0},
		{TxID: missing, Vout: 0},
		{TxID: *mined.TxIDChainHash(), Vout: 9},
		{TxID: *forked.TxIDChainHash(), Vout: 0},
		{TxID: *mined.TxIDChainHash(), Vout: 0},
		{TxID: *mined.TxIDChainHash(), Vout: 2},
		{TxID: seededID, Vout: 1},
		{TxID: seededID, Vout: 2},
	}
	before := append([]utxostore.Outpoint(nil), outpoints...)

	answers, err := db.ParentOutputsForValidation(ctx, outpoints)
	require.NoError(t, err)
	require.Len(t, answers, len(outpoints))
	require.Equal(t, before, outpoints, "the call must not modify its argument")
	RequireAnswered(t, answers)

	requireOutput := func(i int, status utxostore.ParentOutputStatus, out *bt.Output, height uint32) {
		t.Helper()
		require.NoError(t, answers[i].Err, "slot %d", i)
		require.Equal(t, status, answers[i].Status, "slot %d", i)
		require.Equal(t, out.Satoshis, answers[i].Satoshis, "slot %d", i)
		require.NotNil(t, answers[i].LockingScript, "slot %d", i)
		require.Equal(t, []byte(*out.LockingScript), []byte(*answers[i].LockingScript), "slot %d", i)

		if status == utxostore.ParentOutputMined {
			require.Equal(t, height, answers[i].Height, "slot %d", i)
		}
	}

	requireOutput(0, utxostore.ParentOutputMined, mined.Outputs[2], 101)
	requireOutput(1, utxostore.ParentOutputNotMined, unmined.Outputs[0], 0)

	require.NoError(t, answers[2].Err)
	require.Equal(t, utxostore.ParentOutputTxNotFound, answers[2].Status)

	require.NoError(t, answers[3].Err)
	require.Equal(t, utxostore.ParentOutputNoSuchIndex, answers[3].Status)

	requireOutput(4, utxostore.ParentOutputMined, forked.Outputs[0], 103)
	requireOutput(5, utxostore.ParentOutputMined, mined.Outputs[0], 101)
	require.Equal(t, answers[0], answers[6], "duplicate outpoints get identical answers")

	require.NoError(t, answers[7].Err, "a seeded gap is a verdict, not a fault")
	require.Equal(t, utxostore.ParentOutputNoSuchIndex, answers[7].Status)
	requireOutput(8, utxostore.ParentOutputMined, seeded.Outputs[2], 100)

	empty, err := db.ParentOutputsForValidation(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// ParentOutputsReadsOutputsNotInputs pins that answers come only from a parent's
// own stored outputs. A child whose stored input carries a forged copy of a
// parent's output (here an OP_TRUE script with an inflated value) must never make
// that outpoint resolve: with the parent absent the answer is TxNotFound.
func ParentOutputsReadsOutputsNotInputs(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	var absentParent chainhash.Hash
	absentParent[0] = 0xab
	absentParent[31] = 0x07

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      &absentParent,
		Vout:          0,
		LockingScript: bscript.NewFromBytes([]byte{0x51}),
		Satoshis:      21_000_000_00000000,
	}))
	child.Inputs[0].UnlockingScript = dummyUnlockingScript
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))

	_ = db.Delete(ctx, child.TxIDChainHash())
	_, _, err := db.SpendAndCreate(ctx, child, 100, utxostore.WithCreateOnly())
	require.NoError(t, err)

	answers, err := db.ParentOutputsForValidation(ctx, []utxostore.Outpoint{{TxID: absentParent, Vout: 0}})
	require.NoError(t, err)
	require.Len(t, answers, 1)
	require.NoError(t, answers[0].Err)
	require.Equal(t, utxostore.ParentOutputTxNotFound, answers[0].Status)
	require.Nil(t, answers[0].LockingScript)
	require.Zero(t, answers[0].Satoshis)
}
