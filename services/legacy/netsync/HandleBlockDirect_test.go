package netsync

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/bitcoin-sv/teranode/services/legacy/testdata"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockDirect(t *testing.T) {
	// Load the block
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	assert.Equal(t, block.Hash().String(), "00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386")

	var (
		ctx               = context.Background()
		logger            = ulogger.TestLogger{}
		blockchainClient  = &blockchain.MockBlockchain{}
		validator         = &validator.MockValidator{}
		utxoStore         = &utxo.MockUtxostore{}
		subtreeStore      = memory.New()
		subtreeValidation = &subtreevalidation.MockSubtreeValidation{}
		blockValidation   = &blockvalidation.MockBlockValidation{}
		blockAssembly     = blockassembly.Mock{}
		config            = &Config{
			ChainParams: &chaincfg.MainNetParams,
		}
	)

	_ = blockchainClient.AddBlock(ctx, &model.Block{
		Header:           nil,
		CoinbaseTx:       nil,
		TransactionCount: 0,
		SizeInBytes:      0,
		Subtrees:         nil,
		SubtreeSlices:    nil,
		Height:           0,
		ID:               0,
	}, "test")

	blockchainClient.CurrentState = blockchain.FSMStateRUNNING

	blockAssembly.State = &blockassembly_api.StateMessage{
		BlockAssemblyState:    "",
		SubtreeProcessorState: "",
		ResetWaitCount:        0,
		ResetWaitTime:         0,
		SubtreeCount:          0,
		TxCount:               0,
		QueueCount:            0,
		CurrentHeight:         0,
		CurrentHash:           "",
	}

	blockBytes, err := block.Bytes()
	require.NoError(t, err)
	assert.Len(t, blockBytes, 335942)

	err = subtreeStore.Set(ctx,
		block.Hash().CloneBytes(),
		blockBytes,
		options.WithFileExtension("msgBlock"),
		options.WithSubDirectory("blocks"),
	)
	require.NoError(t, err)

	mBlock, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	mBlock.Height = 1

	err = blockchainClient.AddBlock(ctx, mBlock, "test")
	require.NoError(t, err)

	tSettings := &settings.Settings{}

	sm, err := New(
		ctx,
		logger,
		tSettings,
		blockchainClient,
		validator,
		utxoStore,
		subtreeStore,
		subtreeStore, // tempStore
		subtreeValidation,
		blockValidation,
		blockAssembly,
		config,
	)
	require.NoError(t, err)

	err = sm.HandleBlockDirect(context.Background(), &peer.Peer{}, *block.Hash())
	require.NoError(t, err)
}
