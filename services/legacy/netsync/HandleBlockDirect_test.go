package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
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
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_HandleBlockDirect(t *testing.T) {
	// Load the block
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	assert.Equal(t, block.Hash().String(), "00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386")

	var (
		ctx                 = context.Background()
		logger              = ulogger.TestLogger{}
		blockchainClient    = &blockchain.Mock{}
		validatorClient     = &validator.MockValidator{}
		utxoStore           = &utxo.MockUtxostore{}
		subtreeStore        = memory.New()
		subtreeValidation   = &subtreevalidation.MockSubtreeValidation{}
		blockValidation     = &blockvalidation.MockBlockValidation{}
		blockAssemblyClient = blockassembly.NewMock()
		config              = &Config{
			ChainParams: &chaincfg.MainNetParams,
		}
	)

	blockToAdd := model.Block{
		Header:           nil,
		CoinbaseTx:       nil,
		TransactionCount: 0,
		SizeInBytes:      0,
		Subtrees:         nil,
		SubtreeSlices:    nil,
		Height:           0,
		ID:               0,
	}

	timeUint32, err := safeconversion.Int64ToUint32(time.Now().Unix())
	require.NoError(t, err)

	// Create mock return values for GetBestBlockHeader
	mockBlockHeader := blockToAdd.Header
	mockBlockHeaderMeta := &model.BlockHeaderMeta{
		ID:          1,
		Height:      blockToAdd.Height,
		TxCount:     blockToAdd.TransactionCount,
		SizeInBytes: blockToAdd.SizeInBytes,
		Miner:       "test",
		BlockTime:   timeUint32,
		Timestamp:   timeUint32,
		ChainWork:   nil,
	}

	blockchainClient.On("AddBlock", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(mockBlockHeader, mockBlockHeaderMeta, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(make(chan *blockchain_api.Notification), nil)

	_ = blockchainClient.AddBlock(ctx, &blockToAdd, "test")

	blockAssemblyClient.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{
		BlockAssemblyState:    "",
		SubtreeProcessorState: "",
		ResetWaitCount:        0,
		ResetWaitTime:         0,
		SubtreeCount:          0,
		TxCount:               0,
		QueueCount:            0,
		CurrentHeight:         0,
		CurrentHash:           "",
	}, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)
	assert.Len(t, blockBytes, 335942)

	err = subtreeStore.Set(ctx,
		block.Hash().CloneBytes(),
		fileformat.FileTypeMsgBlock,
		blockBytes,
		options.WithSubDirectory("blocks"),
	)
	require.NoError(t, err)

	tSettings := &settings.Settings{}

	mBlock, err := model.NewBlockFromBytes(blockBytes, tSettings)
	require.NoError(t, err)

	mBlock.Height = 1

	err = blockchainClient.AddBlock(ctx, mBlock, "test")
	require.NoError(t, err)

	sm, err := New(
		ctx,
		logger,
		tSettings,
		blockchainClient,
		validatorClient,
		utxoStore,
		subtreeStore,
		subtreeStore, // tempStore
		subtreeValidation,
		blockValidation,
		blockAssemblyClient,
		config,
	)
	require.NoError(t, err)

	err = sm.HandleBlockDirect(context.Background(), &peer.Peer{}, *block.Hash(), nil)
	require.NoError(t, err)
}
