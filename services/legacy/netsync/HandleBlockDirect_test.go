package netsync

import (
	"bytes"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/legacy/testdata"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockDirect(t *testing.T) {

	// Load the block
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	assert.Equal(t, block.Hash().String(), "00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386")

	txMap := make(map[chainhash.Hash]struct{}, len(block.Transactions()))

	var parents int

	for _, wireTx := range block.Transactions() {
		txHash := wireTx.Hash()

		// Serialize the tx
		var txBytes bytes.Buffer
		err = wireTx.MsgTx().Serialize(&txBytes)
		require.NoError(t, err)

		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		require.NoError(t, err)

		for _, input := range tx.Inputs {
			if _, found := txMap[*input.PreviousTxIDChainHash()]; found {
				parents++
			}
		}

		txMap[*txHash] = struct{}{}
	}

	t.Log("Parents:", parents)

	// var sm SyncManager
	// err = sm.HandleBlockDirect(context.Background(), nil, block)

	// require.NoError(t, err)
}
