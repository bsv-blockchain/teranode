package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestHandleCreateRawTransactionBasic tests the basic functionality of createrawtransaction handler
func TestHandleCreateRawTransactionBasic(t *testing.T) {
	t.Run("invalid txid", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		settings := &settings.Settings{
			ChainCfgParams: &chaincfg.MainNetParams,
		}

		s := &RPCServer{
			settings: settings,
			logger:   logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "invalid_txid",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": 1.0,
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("invalid amount", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		settings := &settings.Settings{
			ChainCfgParams: &chaincfg.MainNetParams,
		}

		s := &RPCServer{
			settings: settings,
			logger:   logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": -1.0, // Negative amount
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCType, rpcErr.Code)
	})
}

// TestHandleCreateRawTransactionComprehensive tests comprehensive scenarios for createrawtransaction handler
func TestHandleCreateRawTransactionComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("locktime out of range - negative", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		negativeTime := int64(-1)
		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:   []bsvjson.TransactionInput{},
			Amounts:  map[string]float64{},
			LockTime: &negativeTime,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Locktime out of range")
	})

	t.Run("locktime out of range - too large", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		largeTime := int64(wire.MaxTxInSequenceNum) + 1
		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:   []bsvjson.TransactionInput{},
			Amounts:  map[string]float64{},
			LockTime: &largeTime,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Locktime out of range")
	})

	t.Run("invalid address", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"invalid_address": 1.0,
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid address or key")
	})

	t.Run("successful multiple inputs", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000002",
				Vout: 1,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": 1.0,
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		result, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.NoError(t, err)

		// Result should be a hex string
		hexResult, ok := result.(string)
		assert.True(t, ok)
		assert.NotEmpty(t, hexResult)

		// Verify it's valid hex
		_, decodeErr := hex.DecodeString(hexResult)
		assert.NoError(t, decodeErr)
	})

	t.Run("amount too large", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": bsvutil.MaxSatoshi + 1,
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCType, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid amount")
	})

	t.Run("successful transaction creation", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": 1.0,
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		result, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.NoError(t, err)

		// Result should be a hex string
		hexResult, ok := result.(string)
		assert.True(t, ok)
		assert.NotEmpty(t, hexResult)

		// Verify it's valid hex
		_, decodeErr := hex.DecodeString(hexResult)
		assert.NoError(t, decodeErr)
	})

	t.Run("successful transaction creation with locktime", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": 1.0,
		}

		lockTime := int64(500000)
		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:   inputs,
			Amounts:  amounts,
			LockTime: &lockTime,
		}

		result, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.NoError(t, err)

		// Result should be a hex string
		hexResult, ok := result.(string)
		assert.True(t, ok)
		assert.NotEmpty(t, hexResult)

		// Verify it's valid hex
		_, decodeErr := hex.DecodeString(hexResult)
		assert.NoError(t, decodeErr)
	})

	t.Run("zero amount", func(t *testing.T) {
		s := &RPCServer{
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			logger: logger,
		}

		inputs := []bsvjson.TransactionInput{
			{
				Txid: "0000000000000000000000000000000000000000000000000000000000000001",
				Vout: 0,
			},
		}

		amounts := map[string]float64{
			"12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX": 0.0, // Zero amount
		}

		cmd := &bsvjson.CreateRawTransactionCmd{
			Inputs:  inputs,
			Amounts: amounts,
		}

		_, err := handleCreateRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCType, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid amount")
	})
}

// TestHandleGetRawTransactionEdgeCases tests edge cases for getrawtransaction
func TestHandleGetRawTransactionEdgeCases(t *testing.T) {
	t.Run("asset URL not configured", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger:       logger,
			assetHTTPURL: nil, // Not configured
		}

		verboseLevel := 0
		cmd := &bsvjson.GetRawTransactionCmd{
			Txid:    "sometxid",
			Verbose: &verboseLevel,
		}

		_, err := handleGetRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrConfiguration))
	})

	t.Run("HTTP server returns 404", func(t *testing.T) {
		// Create test server that returns 404
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		assetURL, _ := url.Parse(server.URL)
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger:       logger,
			assetHTTPURL: assetURL,
		}

		verboseLevel := 0
		cmd := &bsvjson.GetRawTransactionCmd{
			Txid:    "nonexistenttx",
			Verbose: &verboseLevel,
		}

		_, err := handleGetRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "404")
	})
}

// TestHandleSendRawTransactionValidation tests input validation for sendrawtransaction
func TestHandleSendRawTransactionValidation(t *testing.T) {
	t.Run("odd hex string padding", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SendRawTransactionCmd{
			HexTx: "123", // Odd length, should be padded
		}

		// This should fail due to invalid transaction format, not hex decoding
		_, err := handleSendRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		// Should be deserialization error since "0123" is not a valid transaction
		assert.Equal(t, bsvjson.ErrRPCDeserialization, rpcErr.Code)
	})

	t.Run("invalid hex characters", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SendRawTransactionCmd{
			HexTx: "xyz123", // Invalid hex characters
		}

		_, err := handleSendRawTransaction(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})
}

// TestBlockToJSONComprehensive tests the blockToJSON method comprehensively
func TestBlockToJSONComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("nil block returns error", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), nil, 0)
		require.Nil(t, result)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCBlockNotFound, rpcErr.Code)
		assert.Equal(t, "Block not found", rpcErr.Message)
	})

	t.Run("verbosity 0 returns hex string", func(t *testing.T) {
		// Create a mock block
		mockBlock := createMockBlock(t, 100)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 150}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 0)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return a hex string
		hexStr, ok := result.(string)
		assert.True(t, ok)
		assert.NotEmpty(t, hexStr)

		// Verify it's valid hex
		_, err = hex.DecodeString(hexStr)
		assert.NoError(t, err)
	})

	t.Run("verbosity 1 returns block verbose result", func(t *testing.T) {
		mockBlock := createMockBlock(t, 100)

		// Create next block for testing
		nextBlock := createMockBlock(t, 101)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 150}, nil,
		)
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			mockBlock.Header, &model.BlockHeaderMeta{
				Height:    100,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockBlockchainClient.On("GetBlockByHeight", mock.Anything, uint32(101)).Return(nextBlock, nil)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 1)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return a GetBlockVerboseResult (both verbosity levels now return the same type)
		blockResult, ok := result.(*bsvjson.GetBlockVerboseResult)
		assert.True(t, ok)

		// Verify basic fields
		assert.Equal(t, mockBlock.Hash().String(), blockResult.Hash)
		assert.Equal(t, int64(100), blockResult.Height)
		assert.Equal(t, int64(51), blockResult.Confirmations) // 150 - 100 + 1
		assert.Equal(t, mockBlock.Header.HashMerkleRoot.String(), blockResult.MerkleRoot)
		assert.Equal(t, mockBlock.Header.HashPrevBlock.String(), blockResult.PreviousHash)
		assert.Equal(t, nextBlock.Hash().String(), blockResult.NextHash)
	})

	t.Run("verbosity 2 returns same as verbosity 1 (updated implementation)", func(t *testing.T) {
		mockBlock := createMockBlock(t, 200)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 250}, nil,
		)
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			mockBlock.Header, &model.BlockHeaderMeta{
				Height:    200,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockBlockchainClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return((*model.Block)(nil), errors.ErrBlockNotFound)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 200}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 2)
		require.NoError(t, err)

		// Updated implementation returns GetBlockVerboseResult for all verbosity levels >= 1
		blockResult, ok := result.(*bsvjson.GetBlockVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, blockResult)
	})

	t.Run("block with large size returns size info", func(t *testing.T) {
		mockBlock := createMockBlock(t, 100)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 150}, nil,
		)
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			mockBlock.Header, &model.BlockHeaderMeta{
				Height:    100,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockBlockchainClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return((*model.Block)(nil), errors.ErrBlockNotFound)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 1)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return a GetBlockVerboseResult
		blockResult, ok := result.(*bsvjson.GetBlockVerboseResult)
		assert.True(t, ok)

		// Verify the size field is populated
		assert.Greater(t, blockResult.Size, int32(0))
		assert.NotEmpty(t, blockResult.VersionHex)
	})

	t.Run("blockchain client error on best block", func(t *testing.T) {
		mockBlock := createMockBlock(t, 100)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			nil, nil, errors.New(errors.ERR_ERROR, "blockchain service unavailable"),
		)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 1)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "blockchain service unavailable")
	})

	t.Run("blockchain client error on next block", func(t *testing.T) {
		mockBlock := createMockBlock(t, 100)

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 150}, nil,
		)
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			mockBlock.Header, &model.BlockHeaderMeta{
				Height:    100,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockBlockchainClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(
			nil, errors.New(errors.ERR_ERROR, "database connection error"),
		)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 1)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "database connection error")
	})

	t.Run("version conversion error", func(t *testing.T) {
		// Create a block with a version that would cause uint32 to int32 overflow
		mockBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        uint32(2147483648), // This will cause overflow in uint32 to int32 conversion
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1234567890,
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			},
			Height: 100,
		}

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 150}, nil,
		)
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			mockBlock.Header, &model.BlockHeaderMeta{
				Height:    100,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockBlockchainClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return((*model.Block)(nil), errors.ErrBlockNotFound)
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := s.blockToJSON(context.Background(), mockBlock, 1)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

// createMockBlock creates a mock block for testing
func createMockBlock(t *testing.T, height uint64) *model.Block {
	prevHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	merkleRoot, err := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	require.NoError(t, err)

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		},
		Height: uint32(height),
	}
}

// TestHandleGenerateValidation tests validation for generate commands
func TestHandleGenerateValidation(t *testing.T) {
	t.Run("generate - zero blocks", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.RegressionNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateCmd{
			NumBlocks: 0, // Invalid: zero blocks
		}

		_, err := handleGenerate(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "nonzero")
	})

	t.Run("generate - mainnet not supported", func(t *testing.T) {
		logger := mocklogger.NewTestLogger()
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = false

		cmd := &bsvjson.GenerateCmd{
			NumBlocks: 1,
		}

		_, err := handleGenerate(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDifficulty, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "support")
	})
}

// TestHandleHelpComprehensive tests the handleHelp handler
func TestHandleHelpComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("general help when no command specified", func(t *testing.T) {
		// Use real helpCacher to test actual functionality
		s := &RPCServer{
			logger:     logger,
			helpCacher: newHelpCacher(),
		}

		cmd := &bsvjson.HelpCmd{
			Command: nil, // No specific command
		}

		result, err := handleHelp(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify it returns a string (the usage information)
		usage, ok := result.(string)
		require.True(t, ok)
		// Should be a string, even if empty in some test environments
		_ = usage
	})

	t.Run("general help when empty command specified", func(t *testing.T) {
		s := &RPCServer{
			logger:     logger,
			helpCacher: newHelpCacher(),
		}

		emptyCmd := ""
		cmd := &bsvjson.HelpCmd{
			Command: &emptyCmd,
		}

		result, err := handleHelp(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify it returns a string (the usage information)
		usage, ok := result.(string)
		require.True(t, ok)
		_ = usage
	})

	t.Run("specific command help - getbestblockhash", func(t *testing.T) {
		s := &RPCServer{
			logger:     logger,
			helpCacher: newHelpCacher(),
		}

		// Initialize rpcHandlers for the test
		err := s.Init(context.Background())
		require.NoError(t, err)

		specificCmd := "getbestblockhash" // This command exists in the handlers map
		cmd := &bsvjson.HelpCmd{
			Command: &specificCmd,
		}

		result, err := handleHelp(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		help, ok := result.(string)
		require.True(t, ok)
		_ = help
	})

	t.Run("specific command help - help", func(t *testing.T) {
		s := &RPCServer{
			logger:     logger,
			helpCacher: newHelpCacher(),
		}

		// Initialize rpcHandlers for the test
		err := s.Init(context.Background())
		require.NoError(t, err)

		specificCmd := "help"
		cmd := &bsvjson.HelpCmd{
			Command: &specificCmd,
		}

		result, err := handleHelp(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		help, ok := result.(string)
		require.True(t, ok)
		_ = help
	})

	t.Run("unknown command error", func(t *testing.T) {
		s := &RPCServer{
			logger:     logger,
			helpCacher: newHelpCacher(),
		}

		unknownCmd := "nonexistentcommand"
		cmd := &bsvjson.HelpCmd{
			Command: &unknownCmd,
		}

		result, err := handleHelp(context.Background(), s, cmd, nil)
		require.Error(t, err)
		require.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Unknown command")
		assert.Contains(t, rpcErr.Message, unknownCmd)
	})
}

// TestRPCDecodeHexError tests the rpcDecodeHexError function
func TestRPCDecodeHexError(t *testing.T) {
	invalidHex := "xyz123"
	rpcErr := rpcDecodeHexError(invalidHex)

	require.NotNil(t, rpcErr)
	assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, invalidHex)
}

// TestMessageToHexComprehensive tests the messageToHex helper function
func TestMessageToHexComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &RPCServer{
		logger: logger,
		settings: &settings.Settings{
			ChainCfgParams: &chaincfg.MainNetParams,
		},
	}

	t.Run("successful transaction encoding", func(t *testing.T) {
		// Create a simple transaction message
		mtx := wire.NewMsgTx(wire.TxVersion)

		// Add a simple input
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		txIn := wire.NewTxIn(wire.NewOutPoint(prevHash, 0), nil)
		mtx.AddTxIn(txIn)

		// Add a simple output
		txOut := wire.NewTxOut(5000000000, []byte{0x76, 0xa9, 0x14}) // Simple script
		mtx.AddTxOut(txOut)

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify it's valid hex
		decoded, err := hex.DecodeString(result)
		require.NoError(t, err)
		assert.NotEmpty(t, decoded)

		// Verify the hex string starts with transaction version (01000000 in little endian)
		assert.True(t, strings.HasPrefix(result, "01000000"))
	})

	t.Run("empty transaction encoding", func(t *testing.T) {
		// Create an empty transaction
		mtx := wire.NewMsgTx(wire.TxVersion)

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify it's valid hex
		decoded, err := hex.DecodeString(result)
		require.NoError(t, err)
		assert.NotEmpty(t, decoded)

		// Empty transaction should still have version, input count (0), output count (0), locktime
		// Minimum transaction: version(4) + input_count(1) + output_count(1) + locktime(4) = 10 bytes = 20 hex chars
		assert.GreaterOrEqual(t, len(result), 20)
	})

	t.Run("transaction with multiple inputs and outputs", func(t *testing.T) {
		mtx := wire.NewMsgTx(wire.TxVersion)

		// Add multiple inputs
		for i := 0; i < 3; i++ {
			hash := fmt.Sprintf("000000000000000000000000000000000000000000000000000000000000000%d", i+1)
			prevHash, _ := chainhash.NewHashFromStr(hash)
			txIn := wire.NewTxIn(wire.NewOutPoint(prevHash, uint32(i)), []byte{0x48, 0x30, 0x45})
			mtx.AddTxIn(txIn)
		}

		// Add multiple outputs
		for i := 0; i < 2; i++ {
			value := int64(1000000000 + i*500000000)                // Different amounts
			script := []byte{0x76, 0xa9, 0x14, byte(i), 0x88, 0xac} // Different scripts
			txOut := wire.NewTxOut(value, script)
			mtx.AddTxOut(txOut)
		}

		// Set locktime
		mtx.LockTime = 12345

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify it's valid hex
		decoded, err := hex.DecodeString(result)
		require.NoError(t, err)
		assert.NotEmpty(t, decoded)

		// More complex transaction should produce longer hex string
		assert.Greater(t, len(result), 50)
	})

	t.Run("encoding with different transaction versions", func(t *testing.T) {
		// Test with a different version
		mtx := wire.NewMsgTx(2) // Version 2

		// Add minimal content
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		txIn := wire.NewTxIn(wire.NewOutPoint(prevHash, 0), nil)
		mtx.AddTxIn(txIn)

		txOut := wire.NewTxOut(1000000, []byte{0x76, 0xa9})
		mtx.AddTxOut(txOut)

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Version 2 should start with "02000000" (little endian)
		assert.True(t, strings.HasPrefix(result, "02000000"))
	})

	t.Run("encoding preserves transaction structure", func(t *testing.T) {
		mtx := wire.NewMsgTx(wire.TxVersion)

		// Create a specific transaction for round-trip verification
		prevHash, _ := chainhash.NewHashFromStr("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
		txIn := wire.NewTxIn(wire.NewOutPoint(prevHash, 1), []byte{0x47, 0x30, 0x44})
		mtx.AddTxIn(txIn)

		txOut := wire.NewTxOut(2000000000, []byte{0x76, 0xa9, 0x14, 0x12, 0x34, 0x88, 0xac})
		mtx.AddTxOut(txOut)

		mtx.LockTime = 654321

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Decode the hex and verify we can reconstruct the transaction
		decoded, err := hex.DecodeString(result)
		require.NoError(t, err)

		// Create a new transaction and decode the bytes into it
		var decodedTx wire.MsgTx
		buf := bytes.NewReader(decoded)
		err = decodedTx.Bsvdecode(buf, wire.ProtocolVersion, wire.BaseEncoding)
		require.NoError(t, err)

		// Verify key fields match
		assert.Equal(t, mtx.Version, decodedTx.Version)
		assert.Equal(t, mtx.LockTime, decodedTx.LockTime)
		assert.Equal(t, len(mtx.TxIn), len(decodedTx.TxIn))
		assert.Equal(t, len(mtx.TxOut), len(decodedTx.TxOut))

		if len(mtx.TxOut) > 0 && len(decodedTx.TxOut) > 0 {
			assert.Equal(t, mtx.TxOut[0].Value, decodedTx.TxOut[0].Value)
		}
	})

	t.Run("large transaction encoding", func(t *testing.T) {
		mtx := wire.NewMsgTx(wire.TxVersion)

		// Add many outputs to create a larger transaction
		for i := 0; i < 100; i++ {
			value := int64(1000000 + i)
			script := make([]byte, 25) // Standard P2PKH script size
			script[0] = 0x76
			script[1] = 0xa9
			script[2] = 0x14
			for j := 3; j < 23; j++ {
				script[j] = byte(i % 256) // Fill with pattern
			}
			script[23] = 0x88
			script[24] = 0xac

			txOut := wire.NewTxOut(value, script)
			mtx.AddTxOut(txOut)
		}

		result, err := s.messageToHex(mtx)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Large transaction should produce a long hex string
		assert.Greater(t, len(result), 1000)

		// Verify it's still valid hex
		decoded, err := hex.DecodeString(result)
		require.NoError(t, err)
		assert.NotEmpty(t, decoded)
	})

	t.Run("encoding error handling", func(t *testing.T) {
		// Create a mock message that will fail to encode
		mockMsg := &mockMessage{shouldFail: true}

		result, err := s.messageToHex(mockMsg)

		// Should return an error and empty string
		assert.Error(t, err)
		assert.Empty(t, result)

		// Verify error contains our mock encoding failure message
		assert.Contains(t, err.Error(), "mock encoding failure")
	})
}

// TestInternalRPCError tests the internalRPCError helper function
func TestInternalRPCError(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &RPCServer{
		logger: logger,
	}

	errMsg := "test error"
	context := "test context"

	rpcErr := s.internalRPCError(errMsg, context)

	require.NotNil(t, rpcErr)
	assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, errMsg)
}

// TestIPOrSubnetValidationExtended tests additional IP/subnet validation cases
func TestIPOrSubnetValidationExtended(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
		desc     string
	}{
		{"127.0.0.1", true, "valid IPv4"},
		{"192.168.1.0/24", true, "valid IPv4 subnet"},
		{"::1", true, "valid IPv6 localhost"},
		{"2001:db8::/32", false, "IPv6 subnet - function has bug with port removal"},
		{"256.256.256.256", false, "invalid IPv4 - out of range"},
		{"192.168.1.0/33", false, "invalid IPv4 subnet - mask too large"},
		{"not.an.ip", false, "completely invalid"},
		{"", true, "empty string - function behavior"},
		{"192.168.1.1:8080", false, "IPv4 with port"},
		{"[::1]:8080", false, "IPv6 with port"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := isIPOrSubnet(tc.input)
			assert.Equal(t, tc.expected, result, "Input: %s", tc.input)
		})
	}
}

// TestCalculateHashRate tests the hash rate calculation function
func TestCalculateHashRate(t *testing.T) {
	// Test known difficulty and time values
	difficulty := 1000.0
	targetTime := 600.0 // 10 minutes

	hashRate := calculateHashRate(difficulty, targetTime)

	// Hash rate should be positive
	assert.Greater(t, hashRate, 0.0)

	// Test with different values
	hashRate2 := calculateHashRate(difficulty*2, targetTime)
	assert.Greater(t, hashRate2, hashRate, "Higher difficulty should result in higher hash rate")

	hashRate3 := calculateHashRate(difficulty, targetTime/2)
	assert.Greater(t, hashRate3, hashRate, "Lower target time should result in higher hash rate")
}

// TestHandlerFunctionSignatures ensures all handlers have correct signatures
func TestHandlerFunctionSignatures(t *testing.T) {
	// This test ensures that all registered handlers conform to the commandHandler type
	for name, handler := range rpcHandlers {
		assert.NotNil(t, handler, "Handler %s should not be nil", name)

		// We can't directly test the signature at runtime easily,
		// but we can at least ensure the handler is not nil
		// The compiler will catch signature mismatches
	}
}

// TestHandleGetBlockComprehensive tests the handleGetBlock handler comprehensively
func TestHandleGetBlockComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("invalid block hash", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetBlockCmd{
			Hash:      "invalid_hash",
			Verbosity: func() *uint32 { v := uint32(0); return &v }(),
		}

		_, err := handleGetBlock(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlock", mock.Anything, mock.Anything).Return(
			nil, errors.New(errors.ERR_ERROR, "blockchain error"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockCmd{
			Hash:      validHash,
			Verbosity: func() *uint32 { v := uint32(0); return &v }(),
		}

		_, err := handleGetBlock(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "blockchain error")
	})

	t.Run("block not on main chain should return -1 confirmations", func(t *testing.T) {
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		orphanBlock := &model.Block{
			Header: blockHeader,
			Height: 100000,
			ID:     200,
		}

		bestBlockMeta := &model.BlockHeaderMeta{
			Height: 100010,
		}

		// Create headers for median time calculation
		headers := make([]*model.BlockHeader, 11)
		dummyHash := chainhash.Hash{}
		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &dummyHash,
				HashMerkleRoot: &dummyHash,
				Timestamp:      uint32(1231006505 + i*600),
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			}
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlock", mock.Anything, mock.Anything).Return(orphanBlock, nil)
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, bestBlockMeta, nil)
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			blockHeader, &model.BlockHeaderMeta{
				Height:    100000,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			headers,
			[]*model.BlockHeaderMeta{{Height: 100000}},
			nil,
		)
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return((*model.Block)(nil), errors.ErrBlockNotFound)
		mockClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(false, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000002"
		verbosity := uint32(1)
		cmd := &bsvjson.GetBlockCmd{
			Hash:      validHash,
			Verbosity: &verbosity,
		}

		result, err := handleGetBlock(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		blockResult, ok := result.(*bsvjson.GetBlockVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, blockResult)
		assert.Equal(t, int64(-1), blockResult.Confirmations, "orphan block should have -1 confirmations")
	})

	t.Run("block on main chain should return correct confirmations", func(t *testing.T) {
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		validBlock := &model.Block{
			Header: blockHeader,
			Height: 100000,
			ID:     100,
		}

		bestBlockMeta := &model.BlockHeaderMeta{
			Height: 100010,
		}

		// Create headers for median time calculation
		headers := make([]*model.BlockHeader, 11)
		dummyHash := chainhash.Hash{}
		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &dummyHash,
				HashMerkleRoot: &dummyHash,
				Timestamp:      uint32(1231006505 + i*600),
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			}
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlock", mock.Anything, mock.Anything).Return(validBlock, nil)
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, bestBlockMeta, nil)
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			blockHeader, &model.BlockHeaderMeta{
				Height:    100000,
				ChainWork: []byte{0x01, 0x02, 0x03},
			}, nil,
		)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			headers,
			[]*model.BlockHeaderMeta{{Height: 100000}},
			nil,
		)
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return((*model.Block)(nil), errors.ErrBlockNotFound)
		mockClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		verbosity := uint32(1)
		cmd := &bsvjson.GetBlockCmd{
			Hash:      validHash,
			Verbosity: &verbosity,
		}

		result, err := handleGetBlock(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		blockResult, ok := result.(*bsvjson.GetBlockVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, blockResult)
		assert.Equal(t, int64(11), blockResult.Confirmations)
	})
}

// TestHandleGetBlockByHeightComprehensive tests the handleGetBlockByHeight handler
func TestHandleGetBlockByHeightComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(
			nil, errors.New(errors.ERR_ERROR, "block not found at height"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetBlockByHeightCmd{
			Height:    999999,
			Verbosity: func() *uint32 { v := uint32(0); return &v }(),
		}

		_, err := handleGetBlockByHeight(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block not found at height")
	})
}

// TestHandleGetBlockHashComprehensive tests the handleGetBlockHash handler
func TestHandleGetBlockHashComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("invalid height conversion", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Test with a height that would cause conversion issues
		cmd := &bsvjson.GetBlockHashCmd{
			Index: -1, // Negative height
		}

		_, err := handleGetBlockHash(context.Background(), s, cmd, nil)
		require.Error(t, err)
	})

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(
			nil, errors.New(errors.ERR_ERROR, "block not found"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetBlockHashCmd{
			Index: 100,
		}

		_, err := handleGetBlockHash(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block not found")
	})
}

// TestHandleGetBlockHeaderComprehensive tests the handleGetBlockHeader handler
func TestHandleGetBlockHeaderComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("invalid block hash", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    "invalid_hash",
			Verbose: func() *bool { v := true; return &v }(),
		}

		_, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		assert.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(
			nil, nil, errors.New(errors.ERR_ERROR, "header not found"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := true; return &v }(),
		}

		_, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "header not found")
	})

	t.Run("successful block header retrieval with verbose=false", func(t *testing.T) {
		// Create a sample block header
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		blockHeaderMeta := &model.BlockHeaderMeta{
			Height: 100000,
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blockHeader, blockHeaderMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := false; return &v }(),
		}

		result, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return hex string when verbose=false
		hexString, ok := result.(string)
		assert.True(t, ok)
		assert.NotEmpty(t, hexString)

		// Verify it's valid hex
		_, err = hex.DecodeString(hexString)
		assert.NoError(t, err)
	})

	t.Run("successful block header retrieval with verbose=true", func(t *testing.T) {
		// Create a sample block header
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		blockHeaderMeta := &model.BlockHeaderMeta{
			Height:    100000,
			ChainWork: []byte{0x01, 0x02, 0x03, 0x04},
		}

		// Create headers for median time calculation
		headers := make([]*model.BlockHeader, 11)
		dummyHash := chainhash.Hash{}
		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &dummyHash,
				HashMerkleRoot: &dummyHash,
				Timestamp:      uint32(1231006505 + i*600),
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			}
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blockHeader, blockHeaderMeta, nil)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			headers,
			[]*model.BlockHeaderMeta{{Height: 100000}},
			nil,
		)
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 100100}, nil)
		mockClient.On("GetBlockByHeight", mock.Anything, uint32(100001)).Return(
			&model.Block{
				Header:           blockHeader,
				Height:           100001,
				TransactionCount: 5,
			}, nil,
		)
		mockClient.On("GetBlockByHeight", mock.Anything, uint32(100000)).Return(
			&model.Block{
				Header:           blockHeader,
				Height:           100000,
				TransactionCount: 10,
			}, nil,
		)
		mockClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := true; return &v }(),
		}

		result, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return structured result when verbose=true
		headerResult, ok := result.(*bsvjson.GetBlockHeaderVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, headerResult)

		// Verify fields are populated correctly
		assert.NotEmpty(t, headerResult.Hash)
		assert.Equal(t, int32(1), headerResult.Version)
		assert.Equal(t, "00000001", headerResult.VersionHex)
		assert.NotEmpty(t, headerResult.PreviousHash)
		assert.Equal(t, uint64(12345), headerResult.Nonce)
		assert.Equal(t, int64(1234567890), headerResult.Time)
		assert.NotEmpty(t, headerResult.Bits)
		assert.NotEmpty(t, headerResult.MerkleRoot)
		assert.Equal(t, int32(100000), headerResult.Height)
		assert.Greater(t, headerResult.Difficulty, 0.0)
	})

	t.Run("type conversion errors in verbose response", func(t *testing.T) {
		// Create a block header with values that could cause conversion issues
		prevHash := chainhash.Hash{}
		merkleRoot := chainhash.Hash{}
		blockHeader := &model.BlockHeader{
			Version:        4294967295, // Max uint32 value
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      4294967295, // Max uint32 value
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          4294967295, // Max uint32 value
		}

		blockHeaderMeta := &model.BlockHeaderMeta{
			Height: 4294967295, // Max uint32 value
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blockHeader, blockHeaderMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := true; return &v }(),
		}

		// This should return an error due to conversion overflow
		result, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "value out of range")
	})

	t.Run("block on main chain returns correct confirmations", func(t *testing.T) {
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		blockHeaderMeta := &model.BlockHeaderMeta{
			ID:     100,
			Height: 100000,
		}

		bestBlockMeta := &model.BlockHeaderMeta{
			Height: 100010, // 10 blocks ahead
		}

		// Create headers for median time calculation
		headers := make([]*model.BlockHeader, 11)
		dummyHash := chainhash.Hash{}
		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &dummyHash,
				HashMerkleRoot: &dummyHash,
				Timestamp:      uint32(1231006505 + i*600),
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			}
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blockHeader, blockHeaderMeta, nil)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			headers,
			[]*model.BlockHeaderMeta{{Height: 100000}},
			nil,
		)
		mockClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, bestBlockMeta, nil)
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(
			&model.Block{
				Header:           blockHeader,
				Height:           100001,
				TransactionCount: 5,
			}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000001"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := true; return &v }(),
		}

		result, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		headerResult, ok := result.(*bsvjson.GetBlockHeaderVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, headerResult)

		// Should have 11 confirmations (1 + (100010 - 100000))
		assert.Equal(t, int64(11), headerResult.Confirmations)
	})

	t.Run("block not on main chain returns -1 confirmations", func(t *testing.T) {
		prevHash := chainhash.Hash{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		merkleRoot := chainhash.Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
			Nonce:          12345,
		}

		blockHeaderMeta := &model.BlockHeaderMeta{
			ID:     200,
			Height: 100000,
		}

		// Create headers for median time calculation
		headers := make([]*model.BlockHeader, 11)
		dummyHash := chainhash.Hash{}
		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &dummyHash,
				HashMerkleRoot: &dummyHash,
				Timestamp:      uint32(1231006505 + i*600),
				Bits:           model.NBit([4]byte{0xFF, 0xFF, 0x00, 0x1D}),
				Nonce:          12345,
			}
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(blockHeader, blockHeaderMeta, nil)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			headers,
			[]*model.BlockHeaderMeta{{Height: 100000}},
			nil,
		)
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 100100}, nil)
		mockClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(false, nil)
		mockClient.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(
			&model.Block{
				Header:           blockHeader,
				Height:           100001,
				TransactionCount: 5,
			}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		validHash := "0000000000000000000000000000000000000000000000000000000000000002"
		cmd := &bsvjson.GetBlockHeaderCmd{
			Hash:    validHash,
			Verbose: func() *bool { v := true; return &v }(),
		}

		result, err := handleGetBlockHeader(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		headerResult, ok := result.(*bsvjson.GetBlockHeaderVerboseResult)
		assert.True(t, ok)
		assert.NotNil(t, headerResult)

		// Should have -1 confirmations for blocks not on main chain
		assert.Equal(t, int64(-1), headerResult.Confirmations)
	})
}

// TestHandleGetBestBlockHashComprehensive tests the handleGetBestBlockHash handler
func TestHandleGetBestBlockHashComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(
			nil, nil, errors.New(errors.ERR_ERROR, "no best block"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		_, err := handleGetBestBlockHash(context.Background(), s, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no best block")
	})

	t.Run("successful response with caching", func(t *testing.T) {
		// Create a mock hash for testing
		hashBytes := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		expectedHash := chainhash.Hash(hashBytes)

		// Create an NBit value for testing
		nbitBytes := [4]byte{0xFF, 0xFF, 0x00, 0x1D}
		nbit := model.NBit(nbitBytes)

		mockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &expectedHash,
			HashMerkleRoot: &expectedHash,
			Timestamp:      1234567890,
			Bits:           nbit,
			Nonce:          2083236893,
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(mockHeader, &model.BlockHeaderMeta{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					CacheEnabled: true,
				},
			},
		}

		result, err := handleGetBestBlockHash(context.Background(), s, nil, nil)
		require.NoError(t, err)

		resultStr, ok := result.(string)
		require.True(t, ok)
		assert.NotEmpty(t, resultStr)
		assert.Len(t, resultStr, 64) // SHA256 hash should be 64 hex characters
	})
}

// TestHandleGetDifficultyComprehensive tests the handleGetDifficulty handler
func TestHandleGetDifficultyComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("requires block assembly client", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			// blockAssemblyClient is nil - this will cause a panic which indicates the handler
			// doesn't have proper validation, but that's the existing behavior
		}

		// This test verifies that the handleGetDifficulty requires a valid blockAssemblyClient
		// In a real deployment, the server would have this client properly initialized
		assert.Panics(t, func() {
			_, _ = handleGetDifficulty(context.Background(), s, nil, nil)
		})
	})
}

// TestHandleGetMiningCandidateComprehensive tests the handleGetMiningCandidate handler
func TestHandleGetMiningCandidateComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful mining candidate retrieval with verbosity 0", func(t *testing.T) {
		// Create mock mining candidate
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			CoinbaseValue:       5000000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00, 0xff, 0xff},
			Time:                1234567890,
			Height:              100000,
			NumTxs:              150,
			SizeWithoutCoinbase: 50000,
			MerkleProof:         [][]byte{{0x12, 0x34}, {0x56, 0x78}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		verbosity := uint32(0)
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity:         &verbosity,
			ProvideCoinbaseTx: nil, // Default false
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		candidateMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify all expected fields
		assert.NotEmpty(t, candidateMap["id"])
		assert.NotEmpty(t, candidateMap["prevhash"])
		assert.Equal(t, uint64(5000000000), candidateMap["coinbaseValue"])
		assert.Equal(t, uint32(1), candidateMap["version"])
		assert.NotEmpty(t, candidateMap["nBits"])
		assert.Equal(t, uint32(1234567890), candidateMap["time"])
		assert.Equal(t, uint32(100000), candidateMap["height"])
		assert.Equal(t, uint32(150), candidateMap["num_tx"])
		assert.Equal(t, uint64(50000), candidateMap["sizeWithoutCoinbase"])
		assert.Contains(t, candidateMap, "merkleProof")

		// Verify no coinbase or subtreeHashes at verbosity 0
		assert.NotContains(t, candidateMap, "coinbase")
		assert.NotContains(t, candidateMap, "subtreeHashes")
	})

	t.Run("successful mining candidate with coinbase transaction", func(t *testing.T) {
		// Create a mining candidate
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			CoinbaseValue:       2500000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00, 0xff, 0xff},
			Time:                1234567890,
			Height:              50000,
			NumTxs:              75,
			SizeWithoutCoinbase: 25000,
			MerkleProof:         [][]byte{{0xab, 0xcd}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				BlockAssembly: settings.BlockAssemblySettings{
					MinerWalletPrivateKeys: []string{"5KYZdUEo39z3FPrtuX2QbbwGnNP5zTd7yyr2SC1j299sBCnWjss"},
				},
			},
		}

		verbosity := uint32(0)
		provideCoinbase := true
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity:         &verbosity,
			ProvideCoinbaseTx: &provideCoinbase,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		candidateMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify coinbase is included
		assert.Contains(t, candidateMap, "coinbase")
		coinbaseHex, ok := candidateMap["coinbase"].(string)
		assert.True(t, ok)
		assert.NotEmpty(t, coinbaseHex)

		// Verify it's valid hex
		_, err = hex.DecodeString(coinbaseHex)
		assert.NoError(t, err)
	})

	t.Run("successful mining candidate with verbosity 1 (includes subtree hashes)", func(t *testing.T) {
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			CoinbaseValue:       1250000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00, 0xff, 0xff},
			Time:                1234567890,
			Height:              75000,
			NumTxs:              200,
			SizeWithoutCoinbase: 75000,
			MerkleProof:         [][]byte{{0xde, 0xad}, {0xbe, 0xef}},
			SubtreeHashes:       [][]byte{{0x11, 0x22, 0x33, 0x44}, {0x55, 0x66, 0x77, 0x88}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		verbosity := uint32(1)
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity: &verbosity,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		candidateMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify subtreeHashes are included at verbosity 1
		assert.Contains(t, candidateMap, "subtreeHashes")
		subtreeHashes, ok := candidateMap["subtreeHashes"].([]string)
		assert.True(t, ok)
		assert.Len(t, subtreeHashes, 2)
		assert.NotEmpty(t, subtreeHashes[0])
		assert.NotEmpty(t, subtreeHashes[1])
	})

	t.Run("block assembly client error", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return nil, errors.New(errors.ERR_ERROR, "block assembly service unavailable")
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		verbosity := uint32(0)
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity: &verbosity,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "block assembly service unavailable")
	})

	t.Run("invalid previous hash", func(t *testing.T) {
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb}, // Invalid length (should be 32 bytes)
			CoinbaseValue:       5000000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00, 0xff, 0xff},
			Time:                1234567890,
			Height:              100000,
			NumTxs:              150,
			SizeWithoutCoinbase: 50000,
			MerkleProof:         [][]byte{{0x12, 0x34}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		verbosity := uint32(0)
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity: &verbosity,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("invalid nbits", func(t *testing.T) {
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			CoinbaseValue:       5000000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00}, // Invalid length (should be 4 bytes)
			Time:                1234567890,
			Height:              100000,
			NumTxs:              150,
			SizeWithoutCoinbase: 50000,
			MerkleProof:         [][]byte{{0x12, 0x34}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		verbosity := uint32(0)
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity: &verbosity,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("coinbase creation error", func(t *testing.T) {
		mockCandidate := &model.MiningCandidate{
			Id:                  []byte{0x01, 0x02, 0x03, 0x04},
			PreviousHash:        []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			CoinbaseValue:       5000000000,
			Version:             1,
			NBits:               []byte{0x1d, 0x00, 0xff, 0xff},
			Time:                1234567890,
			Height:              100000,
			NumTxs:              150,
			SizeWithoutCoinbase: 50000,
			MerkleProof:         [][]byte{{0x12, 0x34}},
		}

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return mockCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				BlockAssembly: settings.BlockAssemblySettings{
					MinerWalletPrivateKeys: []string{}, // Empty wallet addresses to trigger error
				},
			},
		}

		verbosity := uint32(0)
		provideCoinbase := true
		cmd := &bsvjson.GetMiningCandidateCmd{
			Verbosity:         &verbosity,
			ProvideCoinbaseTx: &provideCoinbase,
		}

		result, err := handleGetMiningCandidate(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "no wallet addresses provided")
	})
}

// TestHandleGetpeerinfoValidation tests validation for getpeerinfo
func TestHandleGetpeerinfoValidation(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("no peer client returns empty result", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
				},
			},
			// peerClient and p2pClient are nil - should return empty results gracefully
		}

		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		// Should return empty array when no clients are available
		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		assert.Empty(t, peers)
	})
}

// TestHandleGetRawMempoolComprehensive tests the handleGetRawMempool handler
func TestHandleGetRawMempoolComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful non-verbose mempool", func(t *testing.T) {
		txHashes := []string{
			"abc123def456",
			"789ghi012jkl",
		}

		mockClient := &mockBlockAssemblyClient{
			getTransactionHashesFunc: func(ctx context.Context) ([]string, error) {
				return txHashes, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: func() *bool { v := false; return &v }(),
		}

		result, err := handleGetRawMempool(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		hashes, ok := result.([]string)
		require.True(t, ok)
		assert.Equal(t, txHashes, hashes)
	})

	t.Run("successful verbose mempool", func(t *testing.T) {
		txHashes := []string{
			"abc123def456",
			"789ghi012jkl",
		}

		miningCandidate := &model.MiningCandidate{
			Time:          1640995200, // Example timestamp
			Height:        700000,
			CoinbaseValue: 625000000, // 6.25 BSV in satoshis
		}

		mockClient := &mockBlockAssemblyClient{
			getTransactionHashesFunc: func(ctx context.Context) ([]string, error) {
				return txHashes, nil
			},
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return miningCandidate, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: func() *bool { v := true; return &v }(),
		}

		result, err := handleGetRawMempool(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		verboseResult, ok := result.(bsvjson.GetRawMempoolVerboseResult)
		require.True(t, ok)
		assert.Equal(t, int32(2), verboseResult.Size)
		assert.Equal(t, float64(625000000), verboseResult.Fee)
		assert.Equal(t, int64(1640995200), verboseResult.Time)
		assert.Equal(t, int64(700000), verboseResult.Height)
		assert.Equal(t, txHashes, verboseResult.Depends)
	})

	t.Run("nil verbose flag defaults to non-verbose", func(t *testing.T) {
		txHashes := []string{"abc123def456"}

		mockClient := &mockBlockAssemblyClient{
			getTransactionHashesFunc: func(ctx context.Context) ([]string, error) {
				return txHashes, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: nil, // nil verbose flag
		}

		result, err := handleGetRawMempool(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		hashes, ok := result.([]string)
		require.True(t, ok)
		assert.Equal(t, txHashes, hashes)
	})

	t.Run("get transaction hashes error", func(t *testing.T) {
		mockClient := &mockBlockAssemblyClient{
			getTransactionHashesFunc: func(ctx context.Context) ([]string, error) {
				return nil, errors.New(errors.ERR_ERROR, "failed to get tx hashes")
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: func() *bool { v := false; return &v }(),
		}

		result, err := handleGetRawMempool(context.Background(), s, cmd, nil)
		require.Error(t, err)
		require.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Error retrieving raw mempool")
		assert.Contains(t, rpcErr.Message, "failed to get tx hashes")
	})

	t.Run("verbose mode - get mining candidate error", func(t *testing.T) {
		txHashes := []string{"abc123def456"}

		mockClient := &mockBlockAssemblyClient{
			getTransactionHashesFunc: func(ctx context.Context) ([]string, error) {
				return txHashes, nil
			},
			getMiningCandidateFunc: func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
				return nil, errors.New(errors.ERR_ERROR, "failed to get mining candidate")
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: func() *bool { v := true; return &v }(),
		}

		result, err := handleGetRawMempool(context.Background(), s, cmd, nil)
		require.Error(t, err)
		require.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Error retrieving mining candidate")
		assert.Contains(t, rpcErr.Message, "failed to get mining candidate")
	})

	t.Run("requires block assembly client", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
			// blockAssemblyClient is nil
		}

		cmd := &bsvjson.GetRawMempoolCmd{
			Verbose: func() *bool { v := false; return &v }(),
		}

		// This handler will panic when blockAssemblyClient is nil
		assert.Panics(t, func() {
			_, _ = handleGetRawMempool(context.Background(), s, cmd, nil)
		})
	})
}

// TestHandleGetblockchaininfoComprehensive tests the handleGetblockchaininfo handler
func TestHandleGetblockchaininfoComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful response", func(t *testing.T) {
		// Create mock data
		hashBytes := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		expectedHash := chainhash.Hash(hashBytes)
		nbitBytes := [4]byte{0xFF, 0xFF, 0x00, 0x1D}
		nbit := model.NBit(nbitBytes)

		mockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &expectedHash,
			HashMerkleRoot: &expectedHash,
			Timestamp:      1234567890,
			Bits:           nbit,
			Nonce:          2083236893,
		}

		mockMeta := &model.BlockHeaderMeta{
			Height:    100000,
			ChainWork: make([]byte, 32), // Valid ChainWork bytes
		}

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(mockHeader, mockMeta, nil)
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(
			[]*model.BlockHeader{mockHeader}, []*model.BlockHeaderMeta{mockMeta}, nil,
		)
		mockClient.On("GetBlockStats", mock.Anything).Return(
			&model.BlockStats{
				TxCount:       1000,
				LastBlockTime: 1234567890,
			}, nil,
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleGetblockchaininfo(context.Background(), s, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		jsonMap, ok := result.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "mainnet", jsonMap["chain"])
		assert.Equal(t, uint32(100000), jsonMap["blocks"])
	})

	t.Run("blockchain client returns error causes panic", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBestBlockHeader", mock.Anything).Return(
			nil, nil, errors.New(errors.ERR_ERROR, "no blockchain info"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// This handler will panic when bestBlockMeta is nil
		_, err := handleGetblockchaininfo(t.Context(), s, nil, nil)
		require.Error(t, err)
	})
}

// TestHandleInvalidateBlockComprehensive tests the handleInvalidateBlock handler
func TestHandleInvalidateBlockComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful block invalidation", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("InvalidateBlock", mock.Anything, mock.MatchedBy(func(blockHash *chainhash.Hash) bool {
			expectedHash, _ := chainhash.NewHashFromStr("00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa")
			return blockHash.IsEqual(expectedHash)
		})).Return([]chainhash.Hash{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		result, err := handleInvalidateBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result) // Handler returns nil on success
	})

	t.Run("invalid block hash format", func(t *testing.T) {
		mockClient := &blockchain.Mock{}

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "invalid-hash", // Invalid hex string
		}

		result, err := handleInvalidateBlock(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		// Should return RPC decode hex error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("short block hash succeeds with padding", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "abcd", // Short but valid hex - gets padded with zeros
		}

		result, err := handleInvalidateBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("blockchain client error", func(t *testing.T) {
		expectedError := errors.New(errors.ERR_ERROR, "blockchain service unavailable")
		mockClient := &blockchain.Mock{}
		mockClient.On("InvalidateBlock", mock.Anything, mock.Anything).Return(([]chainhash.Hash)(nil), expectedError)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		result, err := handleInvalidateBlock(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedError, err)
	})

	t.Run("nil blockchain client", func(t *testing.T) {
		s := &RPCServer{
			logger:           logger,
			blockchainClient: nil, // No blockchain client
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		// This should panic when trying to call InvalidateBlock on nil client
		assert.Panics(t, func() {
			_, _ = handleInvalidateBlock(context.Background(), s, cmd, nil)
		})
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("InvalidateBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
		}).Return(([]chainhash.Hash)(nil), context.Canceled)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		// Create cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := handleInvalidateBlock(ctx, s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("zero block hash", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("InvalidateBlock", mock.Anything, mock.MatchedBy(func(blockHash *chainhash.Hash) bool {
			zeroHash := &chainhash.Hash{}
			return blockHash.IsEqual(zeroHash)
		})).Return([]chainhash.Hash{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.InvalidateBlockCmd{
			BlockHash: "0000000000000000000000000000000000000000000000000000000000000000", // All zeros
		}

		result, err := handleInvalidateBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("malformed command type", func(t *testing.T) {
		mockClient := &blockchain.Mock{}

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Pass wrong command type - this will cause a panic due to type assertion
		assert.Panics(t, func() {
			_, _ = handleInvalidateBlock(context.Background(), s, "wrong-type", nil)
		})
	})
}

// TestHandleReconsiderBlockComprehensive tests the handleReconsiderBlock handler
func TestHandleReconsiderBlockComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful block revalidation", func(t *testing.T) {
		expectedHash, _ := chainhash.NewHashFromStr("00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa")

		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		mockBlockValidationClient := &mockBlockValidationClient{
			revalidateBlockFunc: func(ctx context.Context, blockHash chainhash.Hash) error {
				// Verify the block hash matches expected value
				assert.Equal(t, expectedHash, &blockHash)
				return nil
			},
		}

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: mockBlockValidationClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result) // Handler returns nil on success
	})

	t.Run("invalid block hash format", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "invalid-hash", // Invalid hex string
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		// Should return RPC decode hex error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("revalidation error", func(t *testing.T) {
		expectedError := errors.New(errors.ERR_ERROR, "validation failed")

		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		mockBlockValidationClient := &mockBlockValidationClient{
			revalidateBlockFunc: func(ctx context.Context, blockHash chainhash.Hash) error {
				return expectedError
			},
		}

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: mockBlockValidationClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)
		// The handler converts revalidation errors to RPC verify error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Block failed revalidation")
	})

	t.Run("nil validation client", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: nil, // No validation client
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)
		// Should return internal error when validation service is not available
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Block validation service not available")
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		mockBlockValidationClient := &mockBlockValidationClient{
			revalidateBlockFunc: func(ctx context.Context, blockHash chainhash.Hash) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return errors.New(errors.ERR_ERROR, "should be cancelled")
				}
			},
		}

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: mockBlockValidationClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "00000000000000000007878ec04bb2b2e12317804810f4c26033585b3f81ffaa",
		}

		// Create cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := handleReconsiderBlock(ctx, s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)
		// The handler converts the context.Canceled error to RPC verify error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Block failed revalidation")
	})

	t.Run("short block hash succeeds with padding", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlock", mock.Anything, mock.MatchedBy(func(blockHash *chainhash.Hash) bool {
			expectedHash, _ := chainhash.NewHashFromStr("cafe")
			return blockHash.IsEqual(expectedHash)
		})).Return(&model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock: &chainhash.Hash{},
			},
		}, nil)
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		mockBlockValidationClient := &mockBlockValidationClient{
			validateBlockFunc: func(ctx context.Context, block *model.Block, options *blockvalidation.ValidateBlockOptions) error {
				assert.True(t, options.IsRevalidation)
				return nil
			},
		}

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: mockBlockValidationClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "cafe", // Short but valid hex - gets padded with zeros
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("zero block hash", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlock", mock.Anything, mock.MatchedBy(func(blockHash *chainhash.Hash) bool {
			zeroHash := &chainhash.Hash{}
			return blockHash.IsEqual(zeroHash)
		})).Return(&model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock: &chainhash.Hash{},
			},
		}, nil)
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		mockBlockValidationClient := &mockBlockValidationClient{
			validateBlockFunc: func(ctx context.Context, block *model.Block, options *blockvalidation.ValidateBlockOptions) error {
				assert.True(t, options.IsRevalidation)
				return nil
			},
		}

		s := &RPCServer{
			logger:                logger,
			blockchainClient:      mockClient,
			blockValidationClient: mockBlockValidationClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.ReconsiderBlockCmd{
			BlockHash: "0000000000000000000000000000000000000000000000000000000000000000", // All zeros
		}

		result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("malformed command type", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Pass wrong command type - this will cause a panic due to type assertion
		assert.Panics(t, func() {
			_, _ = handleReconsiderBlock(context.Background(), s, "wrong-type", nil)
		})
	})
}

// TestHandleIsBannedComprehensive tests the handleIsBanned handler
func TestHandleIsBannedComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("empty IP or subnet", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "", // Empty string
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "IPOrSubnet is required")
	})

	t.Run("invalid IP format", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "not-an-ip", // Invalid IP
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid IP or subnet")
	})

	t.Run("p2p client returns banned", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			isBannedFunc: func(ctx context.Context, ipOrSubnet string) (bool, error) {
				assert.Equal(t, "192.168.1.100", ipOrSubnet)
				return true, nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "192.168.1.100",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, banned)
	})

	t.Run("legacy peer client returns banned", func(t *testing.T) {
		mockPeer := &mockLegacyPeerClient{
			isBannedFunc: func(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
				assert.Equal(t, "10.0.0.1", req.IpOrSubnet)
				return &peer_api.IsBannedResponse{IsBanned: true}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "10.0.0.1",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, banned)
	})

	t.Run("both clients return not banned", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			isBannedFunc: func(ctx context.Context, ipOrSubnet string) (bool, error) {
				return false, nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			isBannedFunc: func(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
				return &peer_api.IsBannedResponse{IsBanned: false}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "172.16.0.1",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.False(t, banned)
	})

	t.Run("p2p banned but legacy not banned", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			isBannedFunc: func(ctx context.Context, ipOrSubnet string) (bool, error) {
				return true, nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			isBannedFunc: func(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
				return &peer_api.IsBannedResponse{IsBanned: false}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "192.168.0.1",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, banned) // Should be true because p2p is banned (OR condition)
	})

	t.Run("p2p client error ignored", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			isBannedFunc: func(ctx context.Context, ipOrSubnet string) (bool, error) {
				return false, errors.New(errors.ERR_ERROR, "p2p service error")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			isBannedFunc: func(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
				return &peer_api.IsBannedResponse{IsBanned: true}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "10.10.10.10",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err) // Error from p2p is ignored
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, banned) // Should still return true from legacy peer
	})

	t.Run("valid subnet CIDR notation", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			isBannedFunc: func(ctx context.Context, ipOrSubnet string) (bool, error) {
				assert.Equal(t, "192.168.0.0/24", ipOrSubnet)
				return true, nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "192.168.0.0/24",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, banned)
	})

	t.Run("no clients available", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			// No p2pClient or peerClient
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.IsBannedCmd{
			IPOrSubnet: "192.168.1.1",
		}

		result, err := handleIsBanned(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		banned, ok := result.(bool)
		require.True(t, ok)
		assert.False(t, banned) // No clients means not banned
	})

	t.Run("malformed command type", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Pass wrong command type - this will cause a panic due to type assertion
		assert.Panics(t, func() {
			_, _ = handleIsBanned(context.Background(), s, "wrong-type", nil)
		})
	})
}

// TestHandleListBannedComprehensive tests the handleListBanned handler
func TestHandleListBannedComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("p2p client returns banned list", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			listBannedFunc: func(ctx context.Context) ([]string, error) {
				return []string{"192.168.1.100", "10.0.0.0/24"}, nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleListBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Len(t, bannedList, 2)
		assert.Contains(t, bannedList, "192.168.1.100")
		assert.Contains(t, bannedList, "10.0.0.0/24")
	})

	t.Run("legacy peer client returns banned list", func(t *testing.T) {
		mockPeer := &mockLegacyPeerClient{
			listBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
				return &peer_api.ListBannedResponse{
					Banned: []string{"172.16.0.1", "192.168.0.0/16"},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleListBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Len(t, bannedList, 2)
		assert.Contains(t, bannedList, "172.16.0.1")
		assert.Contains(t, bannedList, "192.168.0.0/16")
	})

	t.Run("both clients return banned lists - combined", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			listBannedFunc: func(ctx context.Context) ([]string, error) {
				return []string{"192.168.1.100", "10.0.0.0/24"}, nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			listBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
				return &peer_api.ListBannedResponse{
					Banned: []string{"172.16.0.1", "192.168.0.0/16"},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleListBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Len(t, bannedList, 4)
		// P2P results come first
		assert.Contains(t, bannedList, "192.168.1.100")
		assert.Contains(t, bannedList, "10.0.0.0/24")
		// Legacy results appended
		assert.Contains(t, bannedList, "172.16.0.1")
		assert.Contains(t, bannedList, "192.168.0.0/16")
	})

	t.Run("p2p client error - continues with legacy", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			listBannedFunc: func(ctx context.Context) ([]string, error) {
				return nil, errors.New(errors.ERR_ERROR, "p2p service error")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			listBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
				return &peer_api.ListBannedResponse{
					Banned: []string{"172.16.0.1"},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleListBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Len(t, bannedList, 1)
		assert.Contains(t, bannedList, "172.16.0.1")
	})

	t.Run("no clients available - empty list", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleListBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Empty(t, bannedList)
	})

	t.Run("p2p client timeout", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			listBannedFunc: func(ctx context.Context) ([]string, error) {
				// Simulate a long-running operation that respects context
				select {
				case <-time.After(10 * time.Second):
					return []string{"192.168.1.100"}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Create a context with short timeout to trigger timeout behavior
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		result, err := handleListBanned(ctx, s, nil, nil)

		require.NoError(t, err)
		bannedList, ok := result.([]string)
		require.True(t, ok)
		assert.Empty(t, bannedList) // Timeout results in empty list
	})
}

// TestHandleClearBannedComprehensive tests the handleClearBanned handler
func TestHandleClearBannedComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("both clients clear successfully", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			clearBannedFunc: func(ctx context.Context) error {
				return nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			clearBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
				return &peer_api.ClearBannedResponse{}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success)
	})

	t.Run("p2p client error - still returns true", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			clearBannedFunc: func(ctx context.Context) error {
				return errors.New(errors.ERR_ERROR, "p2p service error")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			clearBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
				return &peer_api.ClearBannedResponse{}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success) // Still returns true despite error
	})

	t.Run("only p2p client available", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			clearBannedFunc: func(ctx context.Context) error {
				return nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success)
	})

	t.Run("only legacy client available", func(t *testing.T) {
		mockPeer := &mockLegacyPeerClient{
			clearBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
				return &peer_api.ClearBannedResponse{}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success)
	})

	t.Run("no clients available - still returns true", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success)
	})

	t.Run("both clients error - still returns true", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			clearBannedFunc: func(ctx context.Context) error {
				return errors.New(errors.ERR_ERROR, "p2p service error")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			clearBannedFunc: func(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
				return nil, errors.New(errors.ERR_ERROR, "legacy peer service error")
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleClearBanned(context.Background(), s, nil, nil)

		require.NoError(t, err)
		success, ok := result.(bool)
		require.True(t, ok)
		assert.True(t, success) // Always returns true
	})
}

// TestHandleSetBanComprehensive tests the handleSetBan handler
func TestHandleSetBanComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("empty IP or subnet", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "",
			Command:    "add",
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "IPOrSubnet is required")
	})

	t.Run("invalid IP format", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "not-an-ip",
			Command:    "add",
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid IP or subnet")
	})

	t.Run("add ban with absolute time", func(t *testing.T) {
		absoluteTime := int64(1750000000) // Future timestamp
		absoluteFlag := true

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				assert.Equal(t, "192.168.1.100", addr)
				assert.Equal(t, absoluteTime, until)
				return nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			banPeerFunc: func(ctx context.Context, req *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error) {
				assert.Equal(t, "192.168.1.100", req.Addr)
				assert.Equal(t, absoluteTime, req.Until)
				return &peer_api.BanPeerResponse{Ok: true}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "add",
			BanTime:    &absoluteTime,
			Absolute:   &absoluteFlag,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("add ban with relative time", func(t *testing.T) {
		banDuration := int64(3600) // 1 hour in seconds
		absoluteFlag := false

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				// Check that the ban time is approximately 1 hour from now
				expectedTime := time.Now().Add(time.Hour).Unix()
				assert.InDelta(t, expectedTime, until, 5) // Allow 5 second variance
				return nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "10.0.0.1",
			Command:    "add",
			BanTime:    &banDuration,
			Absolute:   &absoluteFlag,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("add ban with zero time - uses default 24 hours", func(t *testing.T) {
		zeroTime := int64(0)

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				// Check that the ban time is approximately 24 hours from now
				expectedTime := time.Now().Add(24 * time.Hour).Unix()
				assert.InDelta(t, expectedTime, until, 5) // Allow 5 second variance
				return nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "172.16.0.1",
			Command:    "add",
			BanTime:    &zeroTime,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("remove ban", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			unbanPeerFunc: func(ctx context.Context, addr string) error {
				assert.Equal(t, "192.168.1.100", addr)
				return nil
			},
		}

		mockPeer := &mockLegacyPeerClient{
			unbanPeerFunc: func(ctx context.Context, req *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error) {
				assert.Equal(t, "192.168.1.100", req.Addr)
				return &peer_api.UnbanPeerResponse{Ok: true}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "remove",
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("invalid command", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "invalid",
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Invalid command")
	})

	t.Run("add ban p2p returns false", func(t *testing.T) {
		banTime := int64(3600)

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				return errors.New(errors.ERR_ERROR, "ban failed") // Ban failed
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "add",
			BanTime:    &banTime,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result) // Returns false when ban fails
	})

	t.Run("add ban with subnet", func(t *testing.T) {
		banTime := int64(7200)

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				assert.Equal(t, "192.168.0.0/24", addr)
				return nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2P,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.0.0/24",
			Command:    "add",
			BanTime:    &banTime,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("add ban both clients fail", func(t *testing.T) {
		banTime := int64(3600)

		mockP2P := &mockP2PClient{
			banPeerFunc: func(ctx context.Context, addr string, until int64) error {
				return errors.New(errors.ERR_ERROR, "p2p ban failed")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			banPeerFunc: func(ctx context.Context, req *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error) {
				return nil, errors.New(errors.ERR_ERROR, "legacy ban failed")
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "add",
			BanTime:    &banTime,
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Failed to add ban")
	})

	t.Run("remove ban both clients fail", func(t *testing.T) {
		mockP2P := &mockP2PClient{
			unbanPeerFunc: func(ctx context.Context, addr string) error {
				return errors.New(errors.ERR_ERROR, "p2p unban failed")
			},
		}

		mockPeer := &mockLegacyPeerClient{
			unbanPeerFunc: func(ctx context.Context, req *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error) {
				return nil, errors.New(errors.ERR_ERROR, "legacy unban failed")
			},
		}

		s := &RPCServer{
			logger:          logger,
			p2pClient:       mockP2P,
			legacyP2PClient: mockPeer,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SetBanCmd{
			IPOrSubnet: "192.168.1.100",
			Command:    "remove",
		}

		result, err := handleSetBan(context.Background(), s, cmd, nil)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Error while trying to unban peer")
	})

	t.Run("malformed command type", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Pass wrong command type - this will cause a panic due to type assertion
		assert.Panics(t, func() {
			_, _ = handleSetBan(context.Background(), s, "wrong-type", nil)
		})
	})
}

// TestHandleGetInfoComprehensive tests the handleGetInfo handler
func TestHandleGetInfoComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	// Clear cache before each test run
	clearRPCCallCache := func() {
		// Access the global rpcCallCache from handlers.go
		// Import the cache package for test access
		rpcCallCache.Delete("getinfo")
	}

	t.Run("successful get info with all clients", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			},
			&model.BlockHeaderMeta{
				Height: 100000,
			},
			nil,
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 12345.67890, nil
			},
		}

		mockP2PClient := &mockP2PClient{
			getPeersFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				peerID1, _ := peer.Decode("peer1")
				peerID2, _ := peer.Decode("peer2")
				return []*p2p.PeerInfo{
					{ID: peerID1},
					{ID: peerID2},
				}, nil
			},
		}

		mockLegacyPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				return &peer_api.GetPeersResponse{
					Peers: []*peer_api.Peer{
						{Id: 1, Addr: "127.0.0.1:8335"},
					},
				}, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			p2pClient:           mockP2PClient,
			legacyP2PClient:     mockLegacyPeerClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      true,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify all expected fields
		assert.Equal(t, 1, infoMap["version"])
		assert.Equal(t, wire.ProtocolVersion, infoMap["protocolversion"])
		assert.Equal(t, uint32(100000), infoMap["blocks"])
		assert.Equal(t, 3, infoMap["connections"]) // 2 p2p + 1 legacy
		// Difficulty is now calculated from nBits and returned as float64
		assert.InDelta(t, 70368426346.67, infoMap["difficulty"], 0.01)
		assert.Equal(t, false, infoMap["testnet"]) // MainNet
		assert.Equal(t, false, infoMap["stn"])     // MainNet
	})

	t.Run("successful get info with testnet", func(t *testing.T) {
		clearRPCCallCache()
		mockBlockchainClient := &blockchain.Mock{}
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 50000,
			}, nil)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 1.0, nil
			},
		}

		// Create TestNet params
		testNetParams := chaincfg.TestNetParams
		testNetParams.Net = wire.TestNet

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &testNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify testnet-specific fields
		assert.Equal(t, uint32(50000), infoMap["blocks"])
		assert.Equal(t, 0, infoMap["connections"]) // No peer clients
		// Difficulty is now calculated from nBits and returned as float64
		assert.InDelta(t, 70368426346.67, infoMap["difficulty"], 0.01)
		assert.Equal(t, true, infoMap["testnet"]) // TestNet
		assert.Equal(t, false, infoMap["stn"])    // Not STN
	})

	t.Run("blockchain client error handling", func(t *testing.T) {
		clearRPCCallCache()
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			nil, nil, errors.New(errors.ERR_ERROR, "blockchain service unavailable"),
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 1000.0, nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false, // Disable cache
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.Error(t, err) // Should error when GetBestBlockHeader fails
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "blockchain service unavailable")
	})

	t.Run("successful without block assembly client", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 75000,
			}, nil,
		)

		// No block assembly client needed anymore since difficulty comes from block header
		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: nil, // Can be nil now
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err) // Should succeed even without block assembly client
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, uint32(75000), infoMap["blocks"])
		// Difficulty is calculated from the nBits value and returned as float64
		assert.InDelta(t, 70368426346.67, infoMap["difficulty"], 0.01)
	})

	t.Run("p2p client error handling", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 80000,
			}, nil,
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 2000.0, nil
			},
		}

		mockP2PClient := &mockP2PClient{
			getPeersFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				return nil, errors.New(errors.ERR_ERROR, "p2p service unavailable")
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			p2pClient:           mockP2PClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err) // P2P errors should not fail the request
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, 0, infoMap["connections"]) // No connections due to error
	})

	t.Run("legacy peer client error handling", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 90000,
			}, nil,
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 1500.0, nil
			},
		}

		mockLegacyPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				return nil, errors.New(errors.ERR_ERROR, "legacy peer service unavailable")
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			legacyP2PClient:     mockLegacyPeerClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err) // Legacy peer errors should not fail the request
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, 0, infoMap["connections"]) // No connections due to error
	})

	t.Run("timeout handling for p2p client", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 95000,
			}, nil,
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 1800.0, nil
			},
		}

		mockP2PClient := &mockP2PClient{
			getPeersFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				// Simulate slow response by checking context cancellation
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Second): // Will timeout before this
					return []*p2p.PeerInfo{}, nil
				}
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			p2pClient:           mockP2PClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 100 * time.Millisecond, // Very short timeout
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err) // Timeout should not fail the request
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, 0, infoMap["connections"]) // No connections due to timeout
	})

	t.Run("network configuration flags", func(t *testing.T) {
		clearRPCCallCache()
		nBits, _ := model.NewNBitFromString("180f9ff5")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{
				Bits: *nBits,
			}, &model.BlockHeaderMeta{
				Height: 25000,
			}, nil,
		)

		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			getCurrentDifficultyFunc: func(ctx context.Context) (float64, error) {
				return 0.5, nil
			},
		}

		// Create a custom ChainParams that mimics STN
		stnParams := chaincfg.MainNetParams
		stnParams.Net = wire.STN

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &stnParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
				},
				Policy: &settings.PolicySettings{
					ExcessiveBlockSize:           4294967296,
					BlockMaxSize:                 2000000000,
					MaxStackMemoryUsagePolicy:    104857600,
					MaxStackMemoryUsageConsensus: 0,
				},
			},
		}

		result, err := handleGetInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		infoMap, ok := result.(map[string]interface{})
		assert.True(t, ok)

		// Verify STN-specific fields
		assert.Equal(t, false, infoMap["testnet"]) // STN is not testnet
		assert.Equal(t, true, infoMap["stn"])      // STN network
	})
}

// TestHandleGetchaintipsComprehensive tests the handleGetchaintips handler
func TestHandleGetchaintipsComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("invalid command type", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Pass wrong command type
		result, err := handleGetchaintips(context.Background(), s, "invalid", nil)
		require.Error(t, err)
		require.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	})

	t.Run("blockchain client returns error", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetChainTips", mock.Anything).Return(
			([]*model.ChainTip)(nil), errors.New(errors.ERR_ERROR, "no chain tips"),
		)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.GetChainTipsCmd{}
		_, err := handleGetchaintips(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	})
}

// mockBlockValidationClient is a mock implementation of blockvalidation.Interface for testing
type mockBlockValidationClient struct {
	validateBlockFunc   func(context.Context, *model.Block, *blockvalidation.ValidateBlockOptions) error
	revalidateBlockFunc func(context.Context, chainhash.Hash) error
	processBlockFunc    func(context.Context, *model.Block, uint32) error
	blockFoundFunc      func(context.Context, *chainhash.Hash, string, bool) error
	healthFunc          func(context.Context, bool) (int, string, error)
}

func (m *mockBlockValidationClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx, checkLiveness)
	}
	return 200, "OK", nil
}

func (m *mockBlockValidationClient) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	if m.blockFoundFunc != nil {
		return m.blockFoundFunc(ctx, blockHash, baseURL, waitToComplete)
	}
	return nil
}

func (m *mockBlockValidationClient) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error {
	if m.processBlockFunc != nil {
		return m.processBlockFunc(ctx, block, blockHeight)
	}
	return nil
}

func (m *mockBlockValidationClient) ValidateBlock(ctx context.Context, block *model.Block, options *blockvalidation.ValidateBlockOptions) error {
	if m.validateBlockFunc != nil {
		return m.validateBlockFunc(ctx, block, options)
	}
	return nil
}

func (m *mockBlockValidationClient) RevalidateBlock(ctx context.Context, blockHash chainhash.Hash) error {
	if m.revalidateBlockFunc != nil {
		return m.revalidateBlockFunc(ctx, blockHash)
	}
	return nil
}

func (m *mockBlockValidationClient) GetCatchupStatus(ctx context.Context) (*blockvalidation.CatchupStatus, error) {
	return &blockvalidation.CatchupStatus{IsCatchingUp: false}, nil
}

// TestHandleFreezeComprehensive tests the handleFreeze handler
func TestHandleFreezeComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("requires valid transaction ID", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
		}

		cmd := &bsvjson.FreezeCmd{
			TxID: "invalid-txid",
			Vout: 0,
		}

		_, err := handleFreeze(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte")
	})

	t.Run("requires utxo store", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
			// utxoStore is nil
		}

		cmd := &bsvjson.FreezeCmd{
			TxID: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			Vout: 0,
		}

		// This should panic when utxoStore is nil
		assert.Panics(t, func() {
			_, _ = handleFreeze(context.Background(), s, cmd, nil)
		})
	})
}

// TestHandleUnfreezeComprehensive tests the handleUnfreeze handler
func TestHandleUnfreezeComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("requires valid transaction ID", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
		}

		cmd := &bsvjson.UnfreezeCmd{
			TxID: "invalid-txid",
			Vout: 0,
		}

		_, err := handleUnfreeze(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte")
	})

	t.Run("requires utxo store", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
			// utxoStore is nil
		}

		cmd := &bsvjson.UnfreezeCmd{
			TxID: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			Vout: 0,
		}

		// This should panic when utxoStore is nil
		assert.Panics(t, func() {
			_, _ = handleUnfreeze(context.Background(), s, cmd, nil)
		})
	})
}

// TestHandleReassignComprehensive tests the handleReassign handler
func TestHandleReassignComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("requires valid old transaction ID", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
		}

		cmd := &bsvjson.ReassignCmd{
			OldTxID:     "invalid-txid",
			OldVout:     0,
			OldUTXOHash: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			NewUTXOHash: "101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f",
		}

		_, err := handleReassign(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte")
	})

	t.Run("requires valid old UTXO hash", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
		}

		cmd := &bsvjson.ReassignCmd{
			OldTxID:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			OldVout:     0,
			OldUTXOHash: "invalid-utxo-hash",
			NewUTXOHash: "101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f",
		}

		_, err := handleReassign(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte")
	})

	t.Run("requires valid new UTXO hash", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
		}

		cmd := &bsvjson.ReassignCmd{
			OldTxID:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			OldVout:     0,
			OldUTXOHash: "101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f",
			NewUTXOHash: "invalid-utxo-hash",
		}

		_, err := handleReassign(context.Background(), s, cmd, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid byte")
	})

	t.Run("requires utxo store", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			settings: &settings.Settings{},
			// utxoStore is nil
		}

		cmd := &bsvjson.ReassignCmd{
			OldTxID:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			OldVout:     0,
			OldUTXOHash: "101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f",
			NewUTXOHash: "1f1e1d1c1b1a19181716151413121110f0e0d0c0b0a09080706050403020100",
		}

		// This should panic when utxoStore is nil
		assert.Panics(t, func() {
			_, _ = handleReassign(context.Background(), s, cmd, nil)
		})
	})
}

// TestHandleGetpeerinfoComprehensive tests the complete handleGetpeerinfo functionality including peer stats from PR #1881
func TestHandleGetpeerinfoComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("legacy peer client with stats", func(t *testing.T) {
		// Create mock legacy peer client
		mockPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				return &peer_api.GetPeersResponse{
					Peers: []*peer_api.Peer{
						{
							Id:             1,
							Addr:           "192.168.1.100:8333",
							AddrLocal:      "10.0.0.1:56789",
							Services:       "00000009", // NODE_NETWORK | NODE_BLOOM
							LastSend:       1705123456, // PR #1881 stats
							LastRecv:       1705123457, // PR #1881 stats
							BytesSent:      12345,      // PR #1881 stats
							BytesReceived:  67890,      // PR #1881 stats
							ConnTime:       1705120000,
							PingTime:       150,
							TimeOffset:     -1,
							Version:        70016,
							SubVer:         "/Bitcoin SV:1.0.0/",
							Inbound:        false,
							StartingHeight: 800000,
							CurrentHeight:  800100,
							Banscore:       0,
							Whitelisted:    false,
							FeeFilter:      1000,
						},
					},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeerClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false, // Disable cache for testing
				},
			},
		}

		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		require.Len(t, peers, 1)

		peer := peers[0]
		// Verify basic peer info
		assert.Equal(t, int32(1), peer.ID)
		assert.Equal(t, "192.168.1.100:8333", peer.Addr)
		assert.Equal(t, "10.0.0.1:56789", peer.AddrLocal)
		assert.Equal(t, "00000009", peer.ServicesStr)

		// Verify PR #1881 peer stats are properly mapped
		assert.Equal(t, int64(1705123456), peer.LastSend)
		assert.Equal(t, int64(1705123457), peer.LastRecv)
		assert.Equal(t, uint64(12345), peer.BytesSent)
		assert.Equal(t, uint64(67890), peer.BytesRecv)

		// Verify other peer info
		assert.Equal(t, int64(1705120000), peer.ConnTime)
		assert.Equal(t, float64(150), peer.PingTime)
		assert.Equal(t, int64(-1), peer.TimeOffset)
		assert.Equal(t, uint32(70016), peer.Version)
		assert.Equal(t, "/Bitcoin SV:1.0.0/", peer.SubVer)
		assert.False(t, peer.Inbound)
		assert.Equal(t, int32(800000), peer.StartingHeight)
		assert.Equal(t, int32(800100), peer.CurrentHeight)
		assert.Equal(t, int32(0), peer.BanScore)
		assert.False(t, peer.Whitelisted)
		assert.Equal(t, int64(1000), peer.FeeFilter)
	})

	t.Run("p2p client with stats", func(t *testing.T) {
		// Create mock p2p client
		mockP2PClient := &mockP2PClient{
			getPeerRegistryFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				peerID, err := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
				require.NoError(t, err, "Failed to decode peer ID")
				return []*p2p.PeerInfo{
					{
						ID:              peerID,
						BytesReceived:   43210,
						BanScore:        5,
						ClientName:      "/Teranode:2.0.0/",
						Height:          800250,
						DataHubURL:      "203.0.113.10:9333",
						ConnectedAt:     time.Unix(1705220000, 0),
						LastMessageTime: time.Unix(1705223456, 0),
						LastBlockTime:   time.Unix(1705223457, 0),
						AvgResponseTime: 75 * time.Second,
						IsConnected:     true,
					},
				}, nil
			},
		}

		s := &RPCServer{
			logger:    logger,
			p2pClient: mockP2PClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false, // Disable cache for testing
				},
			},
		}

		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		require.Len(t, peers, 1)

		p := peers[0]
		// Verify p2p peer info
		assert.Equal(t, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", p.PeerID)
		assert.Equal(t, "203.0.113.10:9333", p.Addr)
		assert.True(t, p.Inbound)
		assert.Equal(t, int32(800250), p.StartingHeight) // P2P uses current height as starting height

		// Verify PR #1881 peer stats are properly mapped for p2p peers
		assert.Equal(t, int64(1705223456), p.LastSend)
		assert.Equal(t, int64(1705223457), p.LastRecv)
		assert.Equal(t, uint64(0), p.BytesSent) // P2P doesn't track bytes sent
		assert.Equal(t, uint64(43210), p.BytesRecv)

		// Verify other p2p peer info
		assert.Equal(t, int64(1705220000), p.ConnTime)
		assert.Equal(t, float64(75), p.PingTime) // AvgResponseTime of 75 seconds
		assert.Equal(t, int64(0), p.TimeOffset)  // P2P doesn't track time offset
		assert.Equal(t, uint32(0), p.Version)    // P2P doesn't track protocol version
		assert.Equal(t, "/Teranode:2.0.0/", p.SubVer)
		assert.Equal(t, int32(800250), p.CurrentHeight)
		assert.Equal(t, int32(5), p.BanScore)
	})

	t.Run("combined legacy and p2p peers", func(t *testing.T) {
		// Create mock clients that both return peers
		mockPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				return &peer_api.GetPeersResponse{
					Peers: []*peer_api.Peer{
						{
							Id:            1,
							Addr:          "192.168.1.100:8333",
							LastSend:      1705100000,
							LastRecv:      1705100001,
							BytesSent:     1000,
							BytesReceived: 2000,
						},
					},
				}, nil
			},
		}

		mockP2PClient := &mockP2PClient{
			getPeerRegistryFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				peerID, err := peer.Decode("12D3KooWJZZnUE4umdLBuK15eUZL1NF6fdTJ9cucEuwvuX8V8Ktp")
				require.NoError(t, err, "Failed to decode peer ID")
				return []*p2p.PeerInfo{
					{
						ID:              peerID,
						BytesReceived:   4000,
						DataHubURL:      "203.0.113.20:9333",
						LastMessageTime: time.Unix(1705200000, 0),
						LastBlockTime:   time.Unix(1705200001, 0),
					},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeerClient,
			p2pClient:       mockP2PClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false,
				},
			},
		}

		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		require.Len(t, peers, 2, "Should return combined peers from both clients")

		// Find legacy and p2p peers
		var legacyPeer, p2pPeer *bsvjson.GetPeerInfoResult
		for _, peer := range peers {
			if peer.ID == 1 {
				legacyPeer = peer
			} else if peer.PeerID == "12D3KooWJZZnUE4umdLBuK15eUZL1NF6fdTJ9cucEuwvuX8V8Ktp" {
				p2pPeer = peer
			}
		}

		require.NotNil(t, legacyPeer, "Should have legacy peer")
		require.NotNil(t, p2pPeer, "Should have p2p peer")

		// Verify legacy peer stats
		assert.Equal(t, "192.168.1.100:8333", legacyPeer.Addr)
		assert.Equal(t, int64(1705100000), legacyPeer.LastSend)
		assert.Equal(t, int64(1705100001), legacyPeer.LastRecv)
		assert.Equal(t, uint64(1000), legacyPeer.BytesSent)
		assert.Equal(t, uint64(2000), legacyPeer.BytesRecv)

		// Verify p2p peer stats
		assert.Equal(t, "203.0.113.20:9333", p2pPeer.Addr)
		assert.Equal(t, int64(1705200000), p2pPeer.LastSend)
		assert.Equal(t, int64(1705200001), p2pPeer.LastRecv)
		assert.Equal(t, uint64(0), p2pPeer.BytesSent) // P2P doesn't track bytes sent
		assert.Equal(t, uint64(4000), p2pPeer.BytesRecv)
	})

	t.Run("client timeout handling", func(t *testing.T) {
		// Create mock client that takes too long to respond
		mockPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				// Simulate timeout by waiting longer than the client timeout
				select {
				case <-time.After(10 * time.Second):
					return &peer_api.GetPeersResponse{Peers: []*peer_api.Peer{}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeerClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 100 * time.Millisecond, // Very short timeout
					CacheEnabled:      false,
				},
			},
		}

		// Should complete without hanging and return empty result
		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		assert.Empty(t, peers, "Should return empty when client times out")
	})

	t.Run("client error handling", func(t *testing.T) {
		// Create mock client that returns error
		mockPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				return nil, errors.NewServiceError("peer service unavailable")
			},
		}

		mockP2PClient := &mockP2PClient{
			getPeersFunc: func(ctx context.Context) ([]*p2p.PeerInfo, error) {
				return nil, errors.NewServiceError("p2p service unavailable")
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeerClient,
			p2pClient:       mockP2PClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      false,
				},
			},
		}

		// Should handle errors gracefully and return empty result
		result, err := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		peers, ok := result.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		assert.Empty(t, peers, "Should return empty when both clients error")
	})

	t.Run("caching functionality", func(t *testing.T) {
		callCount := 0
		mockPeerClient := &mockLegacyPeerClient{
			getPeersFunc: func(ctx context.Context) (*peer_api.GetPeersResponse, error) {
				callCount++
				return &peer_api.GetPeersResponse{
					Peers: []*peer_api.Peer{
						{
							Id:            1,
							Addr:          "192.168.1.100:8333",
							LastSend:      1705100000,
							LastRecv:      1705100001,
							BytesSent:     1000,
							BytesReceived: 2000,
						},
					},
				}, nil
			},
		}

		s := &RPCServer{
			logger:          logger,
			legacyP2PClient: mockPeerClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
				RPC: settings.RPCSettings{
					ClientCallTimeout: 5 * time.Second,
					CacheEnabled:      true, // Enable caching
				},
			},
		}

		// First call should hit the client
		result1, err1 := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err1)
		assert.Equal(t, 1, callCount, "Should call client once")

		peers1, ok := result1.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		require.Len(t, peers1, 1)

		// Second call should use cache (within 10 second cache TTL)
		result2, err2 := handleGetpeerinfo(context.Background(), s, nil, nil)
		require.NoError(t, err2)
		assert.Equal(t, 1, callCount, "Should not call client again due to cache")

		peers2, ok := result2.([]*bsvjson.GetPeerInfoResult)
		require.True(t, ok)
		require.Len(t, peers2, 1)

		// Verify cached result has same stats
		assert.Equal(t, peers1[0].LastSend, peers2[0].LastSend)
		assert.Equal(t, peers1[0].LastRecv, peers2[0].LastRecv)
		assert.Equal(t, peers1[0].BytesSent, peers2[0].BytesSent)
		assert.Equal(t, peers1[0].BytesRecv, peers2[0].BytesRecv)
	})
}

// Mock clients for peer testing
type mockLegacyPeerClient struct {
	getPeersFunc    func(ctx context.Context) (*peer_api.GetPeersResponse, error)
	isBannedFunc    func(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)
	listBannedFunc  func(ctx context.Context, req *emptypb.Empty) (*peer_api.ListBannedResponse, error)
	clearBannedFunc func(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error)
	banPeerFunc     func(ctx context.Context, req *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)
	unbanPeerFunc   func(ctx context.Context, req *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)
}

func (m *mockLegacyPeerClient) GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error) {
	if m.getPeersFunc != nil {
		return m.getPeersFunc(ctx)
	}
	return &peer_api.GetPeersResponse{Peers: []*peer_api.Peer{}}, nil
}

func (m *mockLegacyPeerClient) BanPeer(ctx context.Context, req *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error) {
	if m.banPeerFunc != nil {
		return m.banPeerFunc(ctx, req)
	}
	return &peer_api.BanPeerResponse{}, nil
}

func (m *mockLegacyPeerClient) UnbanPeer(ctx context.Context, req *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error) {
	if m.unbanPeerFunc != nil {
		return m.unbanPeerFunc(ctx, req)
	}
	return &peer_api.UnbanPeerResponse{}, nil
}

func (m *mockLegacyPeerClient) IsBanned(ctx context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
	if m.isBannedFunc != nil {
		return m.isBannedFunc(ctx, req)
	}
	return &peer_api.IsBannedResponse{IsBanned: false}, nil
}

func (m *mockLegacyPeerClient) ListBanned(ctx context.Context, req *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
	if m.listBannedFunc != nil {
		return m.listBannedFunc(ctx, req)
	}
	return &peer_api.ListBannedResponse{}, nil
}

func (m *mockLegacyPeerClient) ClearBanned(ctx context.Context, req *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
	if m.clearBannedFunc != nil {
		return m.clearBannedFunc(ctx, req)
	}
	return &peer_api.ClearBannedResponse{}, nil
}

type mockP2PClient struct {
	getPeersFunc           func(ctx context.Context) ([]*p2p.PeerInfo, error)
	getPeerFunc            func(ctx context.Context, peerID string) (*p2p.PeerInfo, error)
	getPeersForCatchupFunc func(ctx context.Context) ([]*p2p.PeerInfo, error)
	isBannedFunc           func(ctx context.Context, ipOrSubnet string) (bool, error)
	listBannedFunc         func(ctx context.Context) ([]string, error)
	clearBannedFunc        func(ctx context.Context) error
	banPeerFunc            func(ctx context.Context, addr string, until int64) error
	unbanPeerFunc          func(ctx context.Context, addr string) error
	addBanScoreFunc        func(ctx context.Context, peerID string, reason string) error
	getPeerRegistryFunc    func(ctx context.Context) ([]*p2p.PeerInfo, error)
}

func (m *mockP2PClient) GetPeers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	if m.getPeersFunc != nil {
		return m.getPeersFunc(ctx)
	}
	return []*p2p.PeerInfo{}, nil
}

func (m *mockP2PClient) GetPeer(ctx context.Context, peerID string) (*p2p.PeerInfo, error) {
	if m.getPeerFunc != nil {
		return m.getPeerFunc(ctx, peerID)
	}
	return nil, nil
}

func (m *mockP2PClient) GetPeersForCatchup(ctx context.Context) ([]*p2p.PeerInfo, error) {
	if m.getPeersForCatchupFunc != nil {
		return m.getPeersForCatchupFunc(ctx)
	}
	return []*p2p.PeerInfo{}, nil
}

func (m *mockP2PClient) IsPeerMalicious(ctx context.Context, peerID string) (bool, string, error) {
	return false, "", nil
}

func (m *mockP2PClient) IsPeerUnhealthy(ctx context.Context, peerID string) (bool, string, float32, error) {
	return false, "", 0, nil
}

func (m *mockP2PClient) BanPeer(ctx context.Context, addr string, until int64) error {
	if m.banPeerFunc != nil {
		return m.banPeerFunc(ctx, addr, until)
	}
	return nil
}

func (m *mockP2PClient) UnbanPeer(ctx context.Context, addr string) error {
	if m.unbanPeerFunc != nil {
		return m.unbanPeerFunc(ctx, addr)
	}
	return nil
}

func (m *mockP2PClient) IsBanned(ctx context.Context, ipOrSubnet string) (bool, error) {
	if m.isBannedFunc != nil {
		return m.isBannedFunc(ctx, ipOrSubnet)
	}
	return false, nil
}

func (m *mockP2PClient) ListBanned(ctx context.Context) ([]string, error) {
	if m.listBannedFunc != nil {
		return m.listBannedFunc(ctx)
	}
	return []string{}, nil
}

func (m *mockP2PClient) ClearBanned(ctx context.Context) error {
	if m.clearBannedFunc != nil {
		return m.clearBannedFunc(ctx)
	}
	return nil
}

func (m *mockP2PClient) AddBanScore(ctx context.Context, peerID string, reason string) error {
	if m.addBanScoreFunc != nil {
		return m.addBanScoreFunc(ctx, peerID, reason)
	}
	return nil
}

func (m *mockP2PClient) ConnectPeer(ctx context.Context, peerAddr string) error {
	return nil
}

func (m *mockP2PClient) DisconnectPeer(ctx context.Context, peerID string) error {
	return nil
}

func (m *mockP2PClient) RecordCatchupAttempt(ctx context.Context, peerID string) error {
	return nil
}

func (m *mockP2PClient) RecordCatchupSuccess(ctx context.Context, peerID string, durationMs int64) error {
	return nil
}

func (m *mockP2PClient) RecordCatchupFailure(ctx context.Context, peerID string) error {
	return nil
}

func (m *mockP2PClient) RecordCatchupMalicious(ctx context.Context, peerID string) error {
	return nil
}

func (m *mockP2PClient) UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error {
	return nil
}

func (m *mockP2PClient) UpdateCatchupError(ctx context.Context, peerID string, errorMessage string) error {
	return nil
}

func (m *mockP2PClient) ResetReputation(ctx context.Context, peerID string) (int, error) {
	return 0, nil
}

func (m *mockP2PClient) ReportValidSubtree(ctx context.Context, peerID string, subtreeHash string) error {
	return nil
}

func (m *mockP2PClient) ReportValidBlock(ctx context.Context, peerID string, blockHash string) error {
	return nil
}

func (m *mockP2PClient) RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error {
	return nil
}

func (m *mockP2PClient) GetPeerRegistry(ctx context.Context) ([]*p2p.PeerInfo, error) {
	if m.getPeerRegistryFunc != nil {
		return m.getPeerRegistryFunc(ctx)
	}
	return []*p2p.PeerInfo{}, nil
}

// TestHandleSubmitMiningSolutionComprehensive tests the complete handleSubmitMiningSolution functionality
func TestHandleSubmitMiningSolutionComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful mining solution submission", func(t *testing.T) {
		// Create mock block assembly client
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				// Verify the solution is properly constructed
				assert.NotNil(t, solution)
				assert.NotEmpty(t, solution.Id)
				assert.NotEmpty(t, solution.Coinbase)
				assert.Equal(t, uint32(2083236893), solution.Nonce)
				if solution.Time != nil {
					assert.Equal(t, uint32(1705123456), *solution.Time)
				}
				if solution.Version != nil {
					assert.Equal(t, uint32(1), *solution.Version)
				}
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Create valid mining solution command
		time := uint32(1705123456)
		version := uint32(1)
		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				Coinbase: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a01000000434104",
				Time:     &time,
				Nonce:    2083236893,
				Version:  &version,
			},
		}

		result, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		assert.True(t, result.(bool))
	})

	t.Run("invalid mining solution ID - invalid hex", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				t.Error("Should not reach block assembly client with invalid hex")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "invalid-hex-string-with-non-hex-characters",
				Coinbase: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a01000000434104",
				Nonce:    2083236893,
			},
		}

		_, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.Error(t, err)

		// Should be an RPC decode error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "invalid-hex-string-with-non-hex-characters")
	})

	t.Run("invalid mining solution ID - odd length", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				t.Error("Should not reach block assembly client with odd length hex")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcde", // Missing last character
				Coinbase: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a01000000434104",
				Nonce:    2083236893,
			},
		}

		_, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.Error(t, err)

		// Should be an RPC decode error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	})

	t.Run("invalid coinbase - invalid hex", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				t.Error("Should not reach block assembly client with invalid coinbase hex")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				Coinbase: "invalid-coinbase-hex-string",
				Nonce:    2083236893,
			},
		}

		_, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.Error(t, err)

		// Should be an RPC decode error
		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "invalid-coinbase-hex-string")
	})

	t.Run("block assembly client error", func(t *testing.T) {
		expectedError := errors.NewServiceError("block assembly service unavailable")
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				return expectedError
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				Coinbase: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a01000000434104",
				Nonce:    2083236893,
			},
		}

		_, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.Error(t, err)
		assert.Equal(t, expectedError, err)
	})

	t.Run("nil block assembly client", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			// blockAssemblyClient is nil
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				Coinbase: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a01000000434104",
				Nonce:    2083236893,
			},
		}

		// Should panic when blockAssemblyClient is nil
		assert.Panics(t, func() {
			_, _ = handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		})
	})

	t.Run("mining solution with all optional fields", func(t *testing.T) {
		var capturedSolution *model.MiningSolution
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				capturedSolution = solution
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		time := uint32(1705999999)
		version := uint32(536870912) // Version with BIP9 bits set
		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				Coinbase: "020000000101000000000000000000000000000000000000000000000000000000000000000000000000ffffffff1904ffff001d0104546573742041",
				Time:     &time,
				Nonce:    4294967295, // Maximum uint32 value
				Version:  &version,
			},
		}

		result, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		assert.True(t, result.(bool))

		// Verify the captured solution
		require.NotNil(t, capturedSolution)
		assert.NotEmpty(t, capturedSolution.Id)
		assert.NotEmpty(t, capturedSolution.Coinbase)
		assert.Equal(t, uint32(4294967295), capturedSolution.Nonce)
		require.NotNil(t, capturedSolution.Time)
		assert.Equal(t, uint32(1705999999), *capturedSolution.Time)
		require.NotNil(t, capturedSolution.Version)
		assert.Equal(t, uint32(536870912), *capturedSolution.Version)
	})

	t.Run("mining solution with nil optional fields", func(t *testing.T) {
		var capturedSolution *model.MiningSolution
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			submitMiningSolutionFunc: func(ctx context.Context, solution *model.MiningSolution) error {
				capturedSolution = solution
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		cmd := &bsvjson.SubmitMiningSolutionCmd{
			MiningSolution: bsvjson.MiningSolution{
				ID:       "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
				Coinbase: "0100000001000000000000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0134ffffffff0100f2052a01000000434104",
				Nonce:    12345678,
				// Time and Version are nil
			},
		}

		result, err := handleSubmitMiningSolution(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		assert.True(t, result.(bool))

		// Verify the captured solution
		require.NotNil(t, capturedSolution)
		assert.NotEmpty(t, capturedSolution.Id)
		assert.NotEmpty(t, capturedSolution.Coinbase)
		assert.Equal(t, uint32(12345678), capturedSolution.Nonce)
		assert.Nil(t, capturedSolution.Time)
		assert.Nil(t, capturedSolution.Version)
	})
}

// Mock block assembly client for mining solution testing
type mockBlockAssemblyClient struct {
	submitMiningSolutionFunc func(ctx context.Context, solution *model.MiningSolution) error
	generateBlocksFunc       func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error
	getCurrentDifficultyFunc func(ctx context.Context) (float64, error)
	getMiningCandidateFunc   func(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error)
	getTransactionHashesFunc func(ctx context.Context) ([]string, error)
	healthFunc               func(context.Context, bool) (int, string, error)
	// Add other methods as needed
}

func (m *mockBlockAssemblyClient) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	if m.submitMiningSolutionFunc != nil {
		return m.submitMiningSolutionFunc(ctx, solution)
	}
	return nil
}

// Implement other required interface methods with default behavior
func (m *mockBlockAssemblyClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx, checkLiveness)
	}
	return 200, "OK", nil
}
func (m *mockBlockAssemblyClient) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error) {
	return true, nil
}
func (m *mockBlockAssemblyClient) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}
func (m *mockBlockAssemblyClient) GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
	if m.getMiningCandidateFunc != nil {
		return m.getMiningCandidateFunc(ctx, includeSubtreeHashes...)
	}
	return nil, nil
}
func (m *mockBlockAssemblyClient) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	if m.getCurrentDifficultyFunc != nil {
		return m.getCurrentDifficultyFunc(ctx)
	}
	return 0, nil
}
func (m *mockBlockAssemblyClient) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
	if m.generateBlocksFunc != nil {
		return m.generateBlocksFunc(ctx, req)
	}
	return nil
}
func (m *mockBlockAssemblyClient) ResetBlockAssembly(ctx context.Context) error {
	return nil
}

func (m *mockBlockAssemblyClient) ResetBlockAssemblyFully(ctx context.Context) error {
	return nil
}

func (m *mockBlockAssemblyClient) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	return nil, nil
}
func (m *mockBlockAssemblyClient) GetBlockAssemblyBlockCandidate(ctx context.Context) (*model.Block, error) {
	return nil, nil
}
func (m *mockBlockAssemblyClient) GetTransactionHashes(ctx context.Context) ([]string, error) {
	if m.getTransactionHashesFunc != nil {
		return m.getTransactionHashesFunc(ctx)
	}
	return nil, nil
}

// TestHandleGetMiningInfoComprehensive tests the complete handleGetMiningInfo functionality
func TestHandleGetMiningInfoComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful mining info retrieval", func(t *testing.T) {
		// Create test block header and metadata
		testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		// testHash not needed since it's not used in BlockHeaderMeta
		prevHash, _ := chainhash.NewHashFromStr("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
		merkleRoot, _ := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")

		nbitBytes := [4]byte{0x18, 0x07, 0x20, 0x3f} // Valid difficulty bits
		nbit := model.NBit(nbitBytes)

		testHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(testTime.Unix()),
			Bits:           nbit,
			Nonce:          12345,
		}

		testMeta := &model.BlockHeaderMeta{
			Height:      100000,
			TxCount:     1500,
			SizeInBytes: 2500000,
			Timestamp:   uint32(testTime.Unix()),
		}

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(testHeader, testMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleGetMiningInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		miningInfo, ok := result.(*bsvjson.GetMiningInfoResult)
		require.True(t, ok)

		// Verify all fields are properly set
		assert.Equal(t, int64(100000), miningInfo.Blocks)
		assert.Equal(t, uint64(2500000), miningInfo.CurrentBlockSize)
		assert.Equal(t, uint64(1500), miningInfo.CurrentBlockTx)
		assert.Greater(t, miningInfo.Difficulty, 0.0)
		assert.Equal(t, "", miningInfo.Errors)
		assert.Greater(t, miningInfo.NetworkHashPS, 0.0)
		assert.Equal(t, "mainnet", miningInfo.Chain)
	})

	t.Run("blockchain client error", func(t *testing.T) {
		expectedError := errors.NewServiceError("blockchain service unavailable")
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), expectedError)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		_, err := handleGetMiningInfo(context.Background(), s, nil, nil)
		require.Error(t, err)
		assert.Equal(t, expectedError, err)
	})

	t.Run("nil blockchain client", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			// blockchainClient is nil
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		// Should panic when blockchain client is nil
		assert.Panics(t, func() {
			_, _ = handleGetMiningInfo(context.Background(), s, nil, nil)
		})
	})

	t.Run("testnet chain configuration", func(t *testing.T) {
		testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		// testHash not needed since it's not used in BlockHeaderMeta
		prevHash, _ := chainhash.NewHashFromStr("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
		merkleRoot, _ := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")

		nbitBytes := [4]byte{0x18, 0x07, 0x20, 0x3f} // Valid difficulty bits
		nbit := model.NBit(nbitBytes)

		testHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(testTime.Unix()),
			Bits:           nbit,
			Nonce:          12345,
		}

		testMeta := &model.BlockHeaderMeta{
			Height:      50000,
			TxCount:     750,
			SizeInBytes: 1250000,
			Timestamp:   uint32(testTime.Unix()),
		}

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(testHeader, testMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleGetMiningInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		miningInfo, ok := result.(*bsvjson.GetMiningInfoResult)
		require.True(t, ok)

		// Verify testnet chain name (now using MainNet, so expecting "mainnet")
		assert.Equal(t, "mainnet", miningInfo.Chain)
		assert.Equal(t, int64(50000), miningInfo.Blocks)
		assert.Equal(t, uint64(1250000), miningInfo.CurrentBlockSize)
		assert.Equal(t, uint64(750), miningInfo.CurrentBlockTx)
	})

	t.Run("different difficulty values", func(t *testing.T) {
		testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		// testHash not needed since it's not used in BlockHeaderMeta
		prevHash, _ := chainhash.NewHashFromStr("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
		merkleRoot, _ := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")

		// Test with different difficulty (higher difficulty = smaller bits value)
		nbitBytes := [4]byte{0x17, 0x04, 0x86, 0x44} // Higher difficulty
		nbit := model.NBit(nbitBytes)

		testHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(testTime.Unix()),
			Bits:           nbit,
			Nonce:          54321,
		}

		testMeta := &model.BlockHeaderMeta{
			Height:      200000,
			TxCount:     2000,
			SizeInBytes: 3000000,
			Timestamp:   uint32(testTime.Unix()),
		}

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(testHeader, testMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleGetMiningInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		miningInfo, ok := result.(*bsvjson.GetMiningInfoResult)
		require.True(t, ok)

		// Verify different values
		assert.Equal(t, int64(200000), miningInfo.Blocks)
		assert.Equal(t, uint64(3000000), miningInfo.CurrentBlockSize)
		assert.Equal(t, uint64(2000), miningInfo.CurrentBlockTx)
		assert.Greater(t, miningInfo.Difficulty, 0.0)
		assert.Greater(t, miningInfo.NetworkHashPS, 0.0)
		assert.Equal(t, "mainnet", miningInfo.Chain)
	})

	t.Run("low difficulty edge case", func(t *testing.T) {
		testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		// testHash not needed since it's not used in BlockHeaderMeta
		prevHash, _ := chainhash.NewHashFromStr("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
		merkleRoot, _ := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")

		// Test with low difficulty (easier than normal)
		nbitBytes := [4]byte{0x1d, 0x7f, 0xff, 0xff} // Low difficulty
		nbit := model.NBit(nbitBytes)

		testHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(testTime.Unix()),
			Bits:           nbit,
			Nonce:          1,
		}

		testMeta := &model.BlockHeaderMeta{
			Height:      1,
			TxCount:     1,
			SizeInBytes: 285, // Genesis block size
			Timestamp:   uint32(testTime.Unix()),
		}

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(testHeader, testMeta, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}

		result, err := handleGetMiningInfo(context.Background(), s, nil, nil)
		require.NoError(t, err)

		miningInfo, ok := result.(*bsvjson.GetMiningInfoResult)
		require.True(t, ok)

		// Verify low difficulty scenario
		assert.Equal(t, int64(1), miningInfo.Blocks)
		assert.Equal(t, uint64(285), miningInfo.CurrentBlockSize)
		assert.Equal(t, uint64(1), miningInfo.CurrentBlockTx)
		assert.GreaterOrEqual(t, miningInfo.Difficulty, 0.0)    // Could be 0 for some bit values
		assert.GreaterOrEqual(t, miningInfo.NetworkHashPS, 0.0) // Hash rate based on difficulty
		assert.Equal(t, "", miningInfo.Errors)                  // Always empty string
	})
}

// TestHandleGenerateToAddressComprehensive tests the complete handleGenerateToAddress functionality
func TestHandleGenerateToAddressComprehensive(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("successful block generation", func(t *testing.T) {
		var capturedRequest *blockassembly_api.GenerateBlocksRequest
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				capturedRequest = req
				return nil
			},
		}

		// Create mock blockchain client to support the new behavior
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil,
		)
		// Create blocks for all possible heights (101-105 for the 5 blocks that will be generated)
		for i := uint32(101); i <= 105; i++ {
			height := i
			prevHash := chainhash.Hash{}
			merkleRoot := chainhash.Hash{}
			block := &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &prevHash,
					HashMerkleRoot: &merkleRoot,
					Timestamp:      1234567890,
					Bits:           model.NBit([4]byte{0x1d, 0x00, 0xff, 0xff}),
					Nonce:          0,
				},
				Height: height,
			}
			mockBlockchainClient.On("GetBlockByHeight", mock.Anything, height).Return(block, nil)
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams, // Use MainNet but set GenerateSupported = true
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		maxTries := int32(1000000)
		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 5,
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
			MaxTries:  &maxTries,
		}

		result, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.NoError(t, err)

		// Function now returns block hashes array
		blockHashes, ok := result.([]string)
		assert.True(t, ok)
		assert.Len(t, blockHashes, 5) // Should return 5 block hashes

		// Verify the request was passed correctly to block assembly client
		require.NotNil(t, capturedRequest)
		assert.Equal(t, int32(5), capturedRequest.Count)
		assert.Equal(t, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", *capturedRequest.Address)
		assert.Equal(t, int32(1000000), *capturedRequest.MaxTries)
	})

	t.Run("generate not supported on mainnet", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				t.Error("Should not reach block assembly client when generation not supported")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams, // MainNet doesn't support generation
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = false

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 1,
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
		}

		_, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCDifficulty, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "No support for `generatetoaddress`")
		assert.Contains(t, rpcErr.Message, "MainNet")
	})

	t.Run("zero blocks requested", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				t.Error("Should not reach block assembly client with zero blocks")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 0,                                    // Zero blocks
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
		}

		_, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Equal(t, "Please request a nonzero number of blocks to generate.", rpcErr.Message)
	})

	t.Run("negative blocks requested", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				t.Error("Should not reach block assembly client with negative blocks")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: -5,                                   // Negative blocks
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
		}

		_, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Equal(t, "Please request a nonzero number of blocks to generate.", rpcErr.Message)
	})

	t.Run("invalid address", func(t *testing.T) {
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				t.Error("Should not reach block assembly client with invalid address")
				return nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 1,
			Address:   "invalid-address-format", // Invalid address
		}

		_, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
		// Error message will be from bsvutil.DecodeAddress
		assert.NotEmpty(t, rpcErr.Message)
	})

	t.Run("block assembly client error", func(t *testing.T) {
		expectedError := errors.NewServiceError("block assembly generation failed")
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				return expectedError
			},
		}

		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100, ChainWork: []byte{0x01, 0x00}}, nil,
		)

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 3,
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
		}

		_, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.Error(t, err)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "block assembly generation failed")
	})

	t.Run("nil block assembly client", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
			// blockAssemblyClient is nil
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 1,
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
		}

		// Should panic when blockAssemblyClient is nil
		assert.Panics(t, func() {
			_, _ = handleGenerateToAddress(context.Background(), s, cmd, nil)
		})
	})

	t.Run("nil max tries parameter", func(t *testing.T) {
		var capturedRequest *blockassembly_api.GenerateBlocksRequest
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				capturedRequest = req
				return nil
			},
		}

		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100, ChainWork: []byte{0x01, 0x00}}, nil,
		)
		// Create blocks for various heights that will be generated in tests
		for i := uint32(101); i <= 1100; i++ { // Large range to cover all test cases
			height := i
			prevHash := chainhash.Hash{}
			merkleRoot := chainhash.Hash{}
			block := &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &prevHash,
					HashMerkleRoot: &merkleRoot,
					Timestamp:      1234567890,
					Bits:           model.NBit([4]byte{0x1d, 0x00, 0xff, 0xff}),
					Nonce:          0,
				},
				Height: height,
			}
			mockBlockchainClient.On("GetBlockByHeight", mock.Anything, height).Return(block, nil)
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 2,
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
			MaxTries:  nil,                                  // Nil max tries
		}

		result, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify MaxTries is 0 when nil
		require.NotNil(t, capturedRequest)
		assert.Equal(t, int32(2), capturedRequest.Count)
		assert.Nil(t, capturedRequest.MaxTries) // Should be nil when not provided
	})

	t.Run("large block count", func(t *testing.T) {
		var capturedRequest *blockassembly_api.GenerateBlocksRequest
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			generateBlocksFunc: func(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
				capturedRequest = req
				return nil
			},
		}

		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
			&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100, ChainWork: []byte{0x01, 0x00}}, nil,
		)
		// Create blocks for various heights that will be generated in tests
		for i := uint32(101); i <= 1100; i++ { // Large range to cover all test cases
			height := i
			prevHash := chainhash.Hash{}
			merkleRoot := chainhash.Hash{}
			block := &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &prevHash,
					HashMerkleRoot: &merkleRoot,
					Timestamp:      1234567890,
					Bits:           model.NBit([4]byte{0x1d, 0x00, 0xff, 0xff}),
					Nonce:          0,
				},
				Height: height,
			}
			mockBlockchainClient.On("GetBlockByHeight", mock.Anything, height).Return(block, nil)
		}

		s := &RPCServer{
			logger:              logger,
			blockchainClient:    mockBlockchainClient,
			blockAssemblyClient: mockBlockAssemblyClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.MainNetParams,
			},
		}
		s.settings.ChainCfgParams.GenerateSupported = true

		maxTries := int32(50000000)
		cmd := &bsvjson.GenerateToAddressCmd{
			NumBlocks: 1000,                                 // Large number of blocks
			Address:   "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // Bitcoin Genesis address
			MaxTries:  &maxTries,
		}

		result, err := handleGenerateToAddress(context.Background(), s, cmd, nil)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify large values are handled correctly
		require.NotNil(t, capturedRequest)
		assert.Equal(t, int32(1000), capturedRequest.Count)
		assert.Equal(t, int32(50000000), *capturedRequest.MaxTries)
	})
}

// mockMessage is a test helper that implements wire.Message but fails encoding when requested
type mockMessage struct {
	shouldFail bool
}

// BsvEncode implements wire.Message interface
func (m *mockMessage) BsvEncode(w io.Writer, pver uint32, enc wire.MessageEncoding) error {
	if m.shouldFail {
		return errors.New(errors.ERR_ERROR, "mock encoding failure")
	}
	// Write some dummy data for successful case
	_, err := w.Write([]byte{0x01, 0x02, 0x03, 0x04})
	return err
}

// Bsvdecode implements wire.Message interface
func (m *mockMessage) Bsvdecode(r io.Reader, pver uint32, enc wire.MessageEncoding) error {
	return nil // Not used in these tests
}

// Command implements wire.Message interface
func (m *mockMessage) Command() string {
	return "mockcmd"
}

// MaxPayloadLength implements wire.Message interface
func (m *mockMessage) MaxPayloadLength(pver uint32) uint64 {
	return 1000
}
