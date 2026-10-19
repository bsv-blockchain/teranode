package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange is a regression test
// for the validator crash reported on v0.15.2/v0.15.4: a child tx whose input
// references a non-existent output index (vout) of a real parent panicked at
// txMeta.Tx.Outputs[PreviousTxOutIndex] with "index out of range" and killed the
// validator process. The extend path must reject an out-of-range vout with a
// clean processing error, not panic.
//
// The v0.15.4 backport (ea6d8ce76) ported only the unreachable idx>=len half of
// #1243 and omitted this reachable bound, so the deref stayed unguarded.
func Test_getUtxoBlockHeightAndExtendForParentTx_VoutOutOfRange(t *testing.T) {
	ctx := context.Background()

	// Child tx: single validly-indexed input (idx 0) whose PreviousTxOutIndex
	// points past the parent's outputs.
	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 99}}}
	utxoHeights := make([]uint32, len(childTx.Inputs))

	parentHash := chainhash.Hash{}

	// Parent exists and is confirmed, but has only 2 outputs (vouts 0 and 1).
	parentMeta := &meta.Data{
		BlockHeights: []uint32{100},
		Tx:           &bt.Tx{Outputs: []*bt.Output{{}, {}}},
	}

	mockStore := &utxostore.MockUtxostore{}
	mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(parentMeta, nil)

	v := &Validator{settings: settings.NewSettings(), utxoStore: mockStore}

	// extend=true forces the parent-output dereference path. nil validationOptions
	// is handled (the ParentMetadata shortcut is nil-guarded), so the store Get
	// path is taken.
	err := v.getUtxoBlockHeightAndExtendForParentTx(ctx, parentHash, []int{0}, utxoHeights, childTx, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no output for index")
}
