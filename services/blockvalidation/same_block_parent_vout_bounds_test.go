package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestExtendBatch_SameBlockParentVoutBounds pins that an input referencing a
// non-existent output of a same-block parent fails the block cleanly.
//
// The same-block extension path indexes parentTx.Outputs by the child's
// PreviousTxOutIndex, which comes from the untrusted block. Unguarded, an
// out-of-range vout panics the validating node with index-out-of-range.
//
// This mattered less while peer-supplied extended transactions skipped the path
// entirely. Re-resolving previous outputs locally (GHSA-v76m-6vc7-g7c7) routes
// every transaction in every block through it, so the guard is a prerequisite of
// that change rather than an unrelated hardening.
func TestExtendBatch_SameBlockParentVoutBounds(t *testing.T) {
	parent := bt.NewTx()
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
	parentHash := *parent.TxIDChainHash()

	// Non-extended child referencing vout 5 of a single-output parent.
	child := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 5}
	require.NoError(t, in.PreviousTxIDAdd(&parentHash))
	child.Inputs = append(child.Inputs, in)
	require.False(t, child.IsExtended(), "child must be non-extended to reach the extend path")

	bv := &BlockValidation{
		logger:   ulogger.NewErrorTestLogger(t),
		settings: settings.NewSettings(),
	}

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	batch := &SubtreeProcessingBatch{
		subtreeData: []*subtreepkg.Data{{Txs: []*bt.Tx{child}}},
		batchStart:  0,
		batchEnd:    1,
		txRanges:    make([][2]int, 1),
	}

	err := bv.extendBatch(context.Background(), block, batch, map[chainhash.Hash]*bt.Tx{parentHash: parent})
	require.Error(t, err, "an out-of-range vout must be a clean block error, not a panic")
	require.Contains(t, err.Error(), "non-existent output")
}

// TestExtendBatch_SameBlockParentVoutBounds_ExtendedChild is the same guard
// reached by a peer-supplied extended transaction, which is the shape that
// actually arrives in subtree data.
//
// A reviewer noted that the non-extended fixture above reached the extension
// path before this change too, so it pins the guard without pinning that a
// peer-supplied transaction gets there. This fixture is the missing half.
//
// Worth being precise about what it does and does not prove: since the
// `if !tx.IsExtended()` gate is gone, extension is now unconditional, so
// reachability here is structural rather than something the discard buys. This
// case documents that an extended child reaches the guard; it would not fail if
// the discard alone were removed, because nothing gates the extension any more.
func TestExtendBatch_SameBlockParentVoutBounds_ExtendedChild(t *testing.T) {
	parent := bt.NewTx()
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
	parentHash := *parent.TxIDChainHash()

	// Extended child, as peer subtree data arrives, naming a vout the parent
	// does not have.
	claimedScript := bscript.Script([]byte{0x51})
	child := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 5,
		PreviousTxScript:   &claimedScript,
		PreviousTxSatoshis: 4244635647,
	}
	require.NoError(t, in.PreviousTxIDAdd(&parentHash))
	child.Inputs = append(child.Inputs, in)
	child.SetExtended(true)
	require.True(t, child.IsExtended(), "fixture must arrive extended")

	bv := &BlockValidation{
		logger:   ulogger.NewErrorTestLogger(t),
		settings: settings.NewSettings(),
	}

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	batch := &SubtreeProcessingBatch{
		subtreeData: []*subtreepkg.Data{{Txs: []*bt.Tx{child}}},
		batchStart:  0,
		batchEnd:    1,
		txRanges:    make([][2]int, 1),
	}

	err := bv.extendBatch(context.Background(), block, batch, map[chainhash.Hash]*bt.Tx{parentHash: parent})
	require.Error(t, err, "an extended child naming an out-of-range vout must also fail cleanly")
	require.Contains(t, err.Error(), "non-existent output")
}
