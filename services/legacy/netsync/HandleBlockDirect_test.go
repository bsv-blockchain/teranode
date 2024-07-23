package netsync

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/testdata"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockDirect(t *testing.T) {

	// Load the block
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	assert.Equal(t, block.Hash().String(), "00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386")

	// txMap := make(map[chainhash.Hash]struct{}, len(block.Transactions()))

	// var parents int

	// for _, wireTx := range block.Transactions() {
	// 	txHash := wireTx.Hash()

	// 	// Serialize the tx
	// 	var txBytes bytes.Buffer
	// 	err = wireTx.MsgTx().Serialize(&txBytes)
	// 	require.NoError(t, err)

	// 	tx, err := bt.NewTxFromBytes(txBytes.Bytes())
	// 	require.NoError(t, err)

	// 	for _, input := range tx.Inputs {
	// 		if _, found := txMap[*input.PreviousTxIDChainHash()]; found {
	// 			parents++
	// 		}
	// 	}

	// 	txMap[*txHash] = struct{}{}
	// }

	// t.Log("Parents:", parents)

	var (
		ctx               context.Context             = context.Background()
		logger            ulogger.Logger              = ulogger.TestLogger{}
		blockchainClient  blockchain.ClientI          = &blockchain.MockBlockchain{}
		validator         validator.Interface         = &validator.MockValidator{}
		utxoStore         utxo.Store                  = &utxo.MockUtxostore{}
		subtreeStore      blob.Store                  = memory.New()
		subtreeValidation subtreevalidation.Interface = &subtreevalidation.MockSubtreeValidation{}
		blockValidation   blockvalidation.Interface   = &blockvalidation.MockBlockValidation{}
		config            *Config                     = &Config{}
	)

	sm, err := New(
		ctx,
		logger,
		blockchainClient,
		validator,
		utxoStore,
		subtreeStore,
		subtreeValidation,
		blockValidation,
		config,
	)
	require.NoError(t, err)

	err = sm.HandleBlockDirect(context.Background(), nil, block)
	require.NoError(t, err)
}
