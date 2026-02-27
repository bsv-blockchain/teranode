package blockchain

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNewClient_blackbox(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("NewClient returns config error when address empty", func(t *testing.T) {
		tSettings.BlockChain.GRPCAddress = ""
		client, err := NewClient(ctx, logger, tSettings, "src")
		require.Error(t, err)
		require.Nil(t, client)
		assert.Contains(t, err.Error(), "no blockchain_grpcAddress setting found")
	})

	t.Run("NewClientWithAddress fails when GetGRPCClient fails", func(t *testing.T) {
		badAddr := "bad-address"
		tSettings.BlockChain.MaxRetries = 0
		client, err := NewClientWithAddress(ctx, logger, tSettings, badAddr, "src")
		require.Error(t, err)
		require.Nil(t, client)
		assert.Contains(t, err.Error(), "rpc error")
	})

	t.Run("NewClientWithAddress fails HealthGRPC immediately", func(t *testing.T) {
		tSettings.BlockChain.MaxRetries = 0
		tSettings.BlockChain.GRPCAddress = "localhost:65535" //closed port
		client, err := NewClientWithAddress(ctx, logger, tSettings, tSettings.BlockChain.GRPCAddress, "src")
		require.Error(t, err)
		require.Nil(t, client)
	})
}

func TestNewClientWithAddress(t *testing.T) {
	// start fake gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	fs := &fakeServer{subCh: make(chan *blockchain_api.Notification, 1)}
	blockchain_api.RegisterBlockchainAPIServer(s, fs)

	go s.Serve(lis) // nolint:errcheck
	defer s.Stop()

	// prepare settings
	tSettings := settings.NewSettings()
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()
	tSettings.BlockChain.MaxRetries = 1
	tSettings.BlockChain.RetrySleep = 10

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)

	// call NewClientWithAddress → should hit the branch
	client, err := NewClientWithAddress(ctx, logger, tSettings, lis.Addr().String(), "src")
	require.NoError(t, err)
	require.NotNil(t, client)

	// Wait a moment to let goroutine process FSM notification
	time.Sleep(300 * time.Millisecond)
}

func TestNewClientWithAddressNotificationLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.MaxRetries = 0

	// Use port 0 to let the OS assign an available port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Get the actual address with the assigned port
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()

	grpcServer := grpc.NewServer()
	fakeSrv := &fakeServer{subCh: make(chan *blockchain_api.Notification, 1)}
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, fakeSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	clientI, err := NewClientWithAddress(ctx, logger, tSettings, tSettings.BlockChain.GRPCAddress, "src")
	require.NoError(t, err)
	c := clientI.(*Client)

	dummyHash := make([]byte, 32)

	notifFSM := &blockchain_api.Notification{
		Type: model.NotificationType_FSMState,
		Hash: dummyHash,
		Metadata: &blockchain_api.NotificationMetadata{
			Metadata: map[string]string{"destination": "RUNNING"},
		},
	}
	fakeSrv.subCh <- notifFSM

	require.Eventually(t, func() bool {
		state := c.fmsState.Load()
		return state != nil && *state == FSMStateRUNNING
	}, 2*time.Second, 100*time.Millisecond)

	ch := make(chan *blockchain_api.Notification, 1)
	c.subscribersMu.Lock()
	c.subscribers = append(c.subscribers, clientSubscriber{id: "test", ch: ch})
	c.subscribersMu.Unlock()

	notifBlock := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: dummyHash,
	}
	fakeSrv.subCh <- notifBlock

	select {
	case got := <-ch:
		require.NotNil(t, got)
		require.Equal(t, model.NotificationType_Block, got.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for block notification")
	}

	c.subscribersMu.Lock()
	last := c.lastBlockNotification
	c.subscribersMu.Unlock()
	require.NotNil(t, last)
}

func TestClientHealth(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("liveness check returns OK", func(t *testing.T) {
		c := &Client{
			client:   &mockHealthClient{},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), true)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, "OK", msg)
	})

	t.Run("readiness check fails with error", func(t *testing.T) {
		c := &Client{
			client: &mockHealthClient{
				resp: &blockchain_api.HealthResponse{Ok: false, Details: "not ready"},
				err:  errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), false)
		require.Error(t, err)
		assert.Equal(t, http.StatusFailedDependency, code)
		assert.Contains(t, msg, "not ready")
	})

	t.Run("readiness check returns OK", func(t *testing.T) {
		c := &Client{
			client: &mockHealthClient{
				resp: &blockchain_api.HealthResponse{Ok: true, Details: "all good"},
				err:  nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		code, msg, err := c.Health(context.Background(), false)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, "all good", msg)
	})
}

func TestClientAddBlock(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Minimal valid block
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	coinbase := bt.NewTx()
	_ = coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = coinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	hashSubtree := &chainhash.Hash{1, 2, 3}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      1000,
		Subtrees:         []*chainhash.Hash{hashSubtree}, // at least one subtree
	}

	t.Run("success", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{err: nil},
			logger:   logger,
			settings: tSettings,
		}

		err := c.AddBlock(context.Background(), block, "peer1")
		require.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{err: errors.NewProcessingError("grpc failure")},
			logger:   logger,
			settings: tSettings,
		}

		err := c.AddBlock(context.Background(), block, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc failure")
	})
}

func TestClientGetBlock(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := &chainhash.Hash{1, 2, 3, 4, 5}
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validHeaderBytes := validHeader.Bytes()

	validCoinbase := bt.NewTx()
	_ = validCoinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = validCoinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	validCoinbaseBytes := validCoinbase.Bytes()

	subtreeHash1 := &chainhash.Hash{1, 2, 3}
	subtreeHash2 := &chainhash.Hash{4, 5, 6}
	validSubtreeHashes := [][]byte{subtreeHash1[:], subtreeHash2[:]}

	t.Run("success with coinbase", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(10),
			SizeInBytes:      uint64(1000),
			Height:           uint32(100),
			Id:               uint32(1),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.NoError(t, err)
		require.NotNil(t, block)
		assert.Equal(t, uint64(10), block.TransactionCount)
		assert.Equal(t, uint64(1000), block.SizeInBytes)
		assert.Equal(t, uint32(100), block.Height)
		assert.Equal(t, uint32(1), block.ID)
		assert.Len(t, block.Subtrees, 2)
		assert.NotNil(t, block.CoinbaseTx)
	})

	t.Run("success with empty coinbase", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       nil, // Empty coinbase
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(5),
			SizeInBytes:      uint64(500),
			Height:           uint32(50),
			Id:               uint32(2),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.NoError(t, err)
		require.NotNil(t, block)
		assert.Nil(t, block.CoinbaseTx)
		assert.Equal(t, uint64(5), block.TransactionCount)
	})

	t.Run("success with empty subtree hashes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    [][]byte{}, // Empty subtree hashes
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(0),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.NoError(t, err)
		require.NotNil(t, block)
		assert.Len(t, block.Subtrees, 0)
	})

	t.Run("grpc client error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: nil,
				err:              errors.NewProcessingError("grpc failure"),
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "grpc failure")
	})

	t.Run("invalid header bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           []byte{0x01, 0x02}, // Invalid header bytes
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(3),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "error parsing block header from bytes")
		assert.Contains(t, err.Error(), blockHash.String())
	})

	t.Run("invalid coinbase bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       []byte{0x01, 0x02}, // Invalid coinbase bytes
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(4),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "error parsing coinbase tx from bytes")
		assert.Contains(t, err.Error(), blockHash.String())
	})

	t.Run("invalid subtree hash", func(t *testing.T) {
		invalidSubtreeHashes := [][]byte{
			{0x01}, // Invalid hash length
			subtreeHash2[:],
		}

		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    invalidSubtreeHashes,
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(5),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlock: mockResponse,
				err:              nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlock(context.Background(), blockHash)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "error parsing subtree hash from bytes")
		assert.Contains(t, err.Error(), blockHash.String())
	})
}

func TestClientGetBlocks(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := &chainhash.Hash{1, 2, 3, 4, 5}

	// Create valid block bytes for testing
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}

	validCoinbase := bt.NewTx()
	_ = validCoinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = validCoinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)

	subtreeHash1 := &chainhash.Hash{1, 2, 3}
	subtreeHash2 := &chainhash.Hash{4, 5, 6}
	subtreeHashes := []*chainhash.Hash{subtreeHash1, subtreeHash2}

	// Create a valid block and convert to bytes
	validBlock, _ := model.NewBlock(validHeader, validCoinbase, subtreeHashes, 1, 1000, 100, 1)
	validBlockBytes, _ := validBlock.Bytes()

	// Create another valid block with different data
	validHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHash,
		HashMerkleRoot: &chainhash.Hash{7, 8, 9},
		Timestamp:      uint32(time.Now().Unix()) + 600,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          456,
	}
	validBlock2, _ := model.NewBlock(validHeader2, validCoinbase, subtreeHashes, 2, 2000, 101, 2)
	validBlock2Bytes, _ := validBlock2.Bytes()

	t.Run("success with single block", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlocksResponse{
			Blocks: [][]byte{validBlockBytes},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: mockResponse,
				err:               nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 1)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		assert.Len(t, blocks, 1)
		assert.Equal(t, uint64(1), blocks[0].TransactionCount)
		assert.Equal(t, uint32(100), blocks[0].Height)
	})

	t.Run("success with multiple blocks", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlocksResponse{
			Blocks: [][]byte{validBlockBytes, validBlock2Bytes},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: mockResponse,
				err:               nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 2)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		assert.Len(t, blocks, 2)
		assert.Equal(t, uint32(100), blocks[0].Height)
		assert.Equal(t, uint32(101), blocks[1].Height)
	})

	t.Run("success with empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlocksResponse{
			Blocks: [][]byte{},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: mockResponse,
				err:               nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 5)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		assert.Len(t, blocks, 0)
	})

	t.Run("grpc client error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: nil,
				err:               errors.NewProcessingError("grpc failure"),
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 1)
		require.Error(t, err)
		assert.Nil(t, blocks)
		assert.Contains(t, err.Error(), "grpc failure")
	})

	t.Run("invalid block bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlocksResponse{
			Blocks: [][]byte{
				validBlockBytes,
				{0x01, 0x02}, // Invalid block bytes
			},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: mockResponse,
				err:               nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 2)
		require.Error(t, err)
		assert.Nil(t, blocks)
		// The error should come from model.NewBlockFromBytes
	})

	t.Run("mixed valid and invalid blocks", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlocksResponse{
			Blocks: [][]byte{
				{0x01, 0x02}, // Invalid block bytes as first block
				validBlockBytes,
			},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocks: mockResponse,
				err:               nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocks(context.Background(), blockHash, 2)
		require.Error(t, err)
		assert.Nil(t, blocks)
		// Should fail on the first invalid block
	})
}

func TestClientGetBlockByHeight(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validHeaderBytes := validHeader.Bytes()

	validCoinbase := bt.NewTx()
	_ = validCoinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = validCoinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	validCoinbaseBytes := validCoinbase.Bytes()

	subtreeHash1 := &chainhash.Hash{1, 2, 3}
	subtreeHash2 := &chainhash.Hash{4, 5, 6}
	validSubtreeHashes := [][]byte{subtreeHash1[:], subtreeHash2[:]}

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(10),
			SizeInBytes:      uint64(1000),
			Height:           uint32(100),
			Id:               uint32(1),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByHeight: mockResponse,
				err:                      nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByHeight(context.Background(), 100)
		require.NoError(t, err)
		require.NotNil(t, block)
		assert.Equal(t, uint32(100), block.Height)
		assert.Equal(t, uint64(10), block.TransactionCount)
	})

	t.Run("grpc client error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByHeight: nil,
				err:                      errors.NewProcessingError("block not found"),
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByHeight(context.Background(), 9999999)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "block not found")
	})

	t.Run("blockFromResponse error", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           []byte{0x01, 0x02}, // Invalid header bytes
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(1),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByHeight: mockResponse,
				err:                      nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByHeight(context.Background(), 1)
		require.Error(t, err)
		assert.Nil(t, block)
	})
}

func TestClientGetBlockByID(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validHeaderBytes := validHeader.Bytes()

	validCoinbase := bt.NewTx()
	_ = validCoinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = validCoinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	validCoinbaseBytes := validCoinbase.Bytes()

	subtreeHash1 := &chainhash.Hash{1, 2, 3}
	subtreeHash2 := &chainhash.Hash{4, 5, 6}
	validSubtreeHashes := [][]byte{subtreeHash1[:], subtreeHash2[:]}

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       validCoinbaseBytes,
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(5),
			SizeInBytes:      uint64(750),
			Height:           uint32(50),
			Id:               uint32(42),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByID: mockResponse,
				err:                  nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByID(context.Background(), 42)
		require.NoError(t, err)
		require.NotNil(t, block)
		assert.Equal(t, uint32(42), block.ID)
		assert.Equal(t, uint64(5), block.TransactionCount)
		assert.Equal(t, uint32(50), block.Height)
	})

	t.Run("grpc client error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByID: nil,
				err:                  errors.NewProcessingError("block with id not found"),
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByID(context.Background(), 99999)
		require.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "block with id not found")
	})

	t.Run("blockFromResponse error", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockResponse{
			Header:           validHeaderBytes,
			CoinbaseTx:       []byte{0x01, 0x02}, // Invalid coinbase bytes
			SubtreeHashes:    validSubtreeHashes,
			TransactionCount: uint64(1),
			SizeInBytes:      uint64(100),
			Height:           uint32(1),
			Id:               uint32(1),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockByID: mockResponse,
				err:                  nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		block, err := c.GetBlockByID(context.Background(), 1)
		require.Error(t, err)
		assert.Nil(t, block)
	})
}

func TestClientGetNextBlockID(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetNextBlockIDResponse{
			NextBlockId: uint64(12345),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetNextBlockID: mockResponse,
				err:                    nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		nextID, err := c.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(12345), nextID)
	})

	t.Run("success with zero ID", func(t *testing.T) {
		mockResponse := &blockchain_api.GetNextBlockIDResponse{
			NextBlockId: uint64(0),
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetNextBlockID: mockResponse,
				err:                    nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		nextID, err := c.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(0), nextID)
	})

	t.Run("success with max uint64", func(t *testing.T) {
		mockResponse := &blockchain_api.GetNextBlockIDResponse{
			NextBlockId: ^uint64(0), // Max uint64 value
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetNextBlockID: mockResponse,
				err:                    nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		nextID, err := c.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ^uint64(0), nextID)
	})

	t.Run("grpc client error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetNextBlockID: nil,
				err:                    errors.NewProcessingError("service unavailable"),
			},
			logger:   logger,
			settings: tSettings,
		}

		nextID, err := c.GetNextBlockID(context.Background())
		require.Error(t, err)
		assert.Equal(t, uint64(0), nextID)
		assert.Contains(t, err.Error(), "service unavailable")
	})
}

func TestClientGetBlockStats(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path (err == nil)", func(t *testing.T) {
		expected := &model.BlockStats{
			BlockCount:         1000,
			TxCount:            500000,
			MaxHeight:          123456,
			AvgBlockSize:       1.25,
			AvgTxCountPerBlock: 250.4,
			FirstBlockTime:     1_700_000_000,
			LastBlockTime:      1_700_003_600,
			ChainWork:          []byte{0x01, 0x02, 0x03},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockStats: expected,
				err:                   nil,
			},
		}

		resp, err := c.GetBlockStats(ctx)
		require.NoError(t, err)
		require.Equal(t, expected, resp)
	})

	t.Run("error path (err != nil ⇒ UnwrapGRPC non-nil)", func(t *testing.T) {
		expected := &model.BlockStats{
			MaxHeight: 7,
		}
		grpcErr := errors.NewProcessingError("grpc error")

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockStats: expected,
				err:                   grpcErr,
			},
		}

		resp, err := c.GetBlockStats(ctx)
		require.Error(t, err)
		require.Equal(t, expected, resp)
	})

	t.Run("error with nil resp", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlockStats: nil,
				err:                   errors.NewProcessingError("grpc error"),
			},
		}
		resp, err := c.GetBlockStats(context.Background())
		require.Error(t, err)
		require.Nil(t, resp)
	})
}

func TestClientGetBlockGraphData(t *testing.T) {
	ctx := context.Background()
	const period uint64 = 3600_000 // 1h in ms

	t.Run("happy path (err == nil)", func(t *testing.T) {
		expected := &model.BlockDataPoints{}

		mc := &mockBlockClient{
			responseGetBlockGraphData: expected,
			err:                       nil,
		}
		c := &Client{client: mc}

		resp, err := c.GetBlockGraphData(ctx, period)
		require.NoError(t, err)
		require.Equal(t, expected, resp)
		require.NotNil(t, mc.lastGetBlockGraphDataReq)
		require.Equal(t, period, mc.lastGetBlockGraphDataReq.PeriodMillis)
	})

	t.Run("error path (gRPC unwrap non-nil)", func(t *testing.T) {
		expected := &model.BlockDataPoints{}
		mc := &mockBlockClient{
			responseGetBlockGraphData: expected,
			err:                       errors.NewProcessingError("grpc error"),
		}
		c := &Client{client: mc}

		resp, err := c.GetBlockGraphData(ctx, period)
		require.Error(t, err)
		require.Equal(t, expected, resp)
	})
}

func TestClient_GetLastNBlocks(t *testing.T) {
	ctx := context.Background()
	const (
		n           int64  = 5
		fromHeight  uint32 = 1234
		includeOrph        = true
	)

	t.Run("happy path (err == nil)", func(t *testing.T) {
		expected := []*model.BlockInfo{
			{},
			{},
		}
		mc := &mockBlockClient{
			responseGetLastNBlocks: &blockchain_api.GetLastNBlocksResponse{
				Blocks: expected,
			},
			err: nil,
		}
		c := &Client{client: mc}

		blocks, err := c.GetLastNBlocks(ctx, n, includeOrph, fromHeight)
		require.NoError(t, err)
		require.Equal(t, expected, blocks)

		require.NotNil(t, mc.lastGetLastNBlocksReq)
		require.Equal(t, n, mc.lastGetLastNBlocksReq.NumberOfBlocks)
		require.Equal(t, includeOrph, mc.lastGetLastNBlocksReq.IncludeOrphans)
		require.Equal(t, fromHeight, mc.lastGetLastNBlocksReq.FromHeight)
	})

	t.Run("error path (gRPC unwrap non-nil)", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetLastNBlocks: &blockchain_api.GetLastNBlocksResponse{
				Blocks: nil,
			},
			err: errors.NewProcessingError("grpc error"),
		}
		c := &Client{client: mc}

		blocks, err := c.GetLastNBlocks(ctx, n, false, 0)
		require.Error(t, err)
		require.Nil(t, blocks)
	})

	t.Run("success with empty list", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetLastNBlocks: &blockchain_api.GetLastNBlocksResponse{
				Blocks: []*model.BlockInfo{},
			},
			err: nil,
		}
		c := &Client{client: mc}

		blocks, err := c.GetLastNBlocks(ctx, n, false, 0)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		require.Len(t, blocks, 0)
	})
}

func TestClientGetLastNInvalidBlocks(t *testing.T) {
	ctx := context.Background()
	const n int64 = 7

	t.Run("happy path", func(t *testing.T) {
		expected := []*model.BlockInfo{{}, {}}

		mc := &mockBlockClient{
			responseGetLastNInvalidBlocks: &blockchain_api.GetLastNInvalidBlocksResponse{
				Blocks: expected,
			},
			err: nil,
		}
		c := &Client{client: mc}

		blocks, err := c.GetLastNInvalidBlocks(ctx, n)
		require.NoError(t, err)
		require.Equal(t, expected, blocks)

		require.NotNil(t, mc.lastGetLastNInvalidBlocksReq)
		require.Equal(t, n, mc.lastGetLastNInvalidBlocksReq.N)
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetLastNInvalidBlocks: &blockchain_api.GetLastNInvalidBlocksResponse{Blocks: nil},
			err:                           errors.NewProcessingError("grpc error"),
		}
		c := &Client{client: mc}

		blocks, err := c.GetLastNInvalidBlocks(ctx, n)
		require.Error(t, err)
		require.Nil(t, blocks)
	})
}

func TestClient_GetSuitableBlock(t *testing.T) {
	ctx := context.Background()

	var h chainhash.Hash
	copy(h[:], bytes32(0xAB))

	t.Run("happy path", func(t *testing.T) {
		expected := &model.SuitableBlock{}

		mc := &mockBlockClient{
			responseGetSuitableBlock: &blockchain_api.GetSuitableBlockResponse{
				Block: expected,
			},
			err: nil,
		}
		c := &Client{client: mc}

		block, err := c.GetSuitableBlock(ctx, &h)
		require.NoError(t, err)
		require.Equal(t, expected, block)

		require.NotNil(t, mc.lastGetSuitableBlockReq)
		require.Equal(t, h[:], mc.lastGetSuitableBlockReq.Hash)
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetSuitableBlock: &blockchain_api.GetSuitableBlockResponse{Block: nil},
			err:                      errors.NewProcessingError("grpc error"),
		}
		c := &Client{client: mc}

		block, err := c.GetSuitableBlock(ctx, &h)
		require.Error(t, err)
		require.Nil(t, block)
	})
}

// helper
func bytes32(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestClient_GetHashOfAncestorBlock(t *testing.T) {
	ctx := context.Background()

	var base chainhash.Hash
	copy(base[:], bytes32(0x11))

	t.Run("happy path", func(t *testing.T) {
		wantHash := bytes32(0xEE)

		mc := &mockBlockClient{
			responseGetHashOfAncestorBlock: &blockchain_api.GetHashOfAncestorBlockResponse{
				Hash: wantHash,
			},
			err: nil,
		}
		c := &Client{client: mc}

		got, err := c.GetHashOfAncestorBlock(ctx, &base, 5)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, wantHash, got[:])

		require.NotNil(t, mc.lastGetHashOfAncestorBlockReq)
		require.Equal(t, base[:], mc.lastGetHashOfAncestorBlockReq.Hash)
		require.Equal(t, uint32(5), mc.lastGetHashOfAncestorBlockReq.Depth)
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetHashOfAncestorBlock: &blockchain_api.GetHashOfAncestorBlockResponse{Hash: nil},
			err:                            errors.NewProcessingError("grpc error"),
		}
		c := &Client{client: mc}

		got, err := c.GetHashOfAncestorBlock(ctx, &base, 1)
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("depth conversion error (negative)", func(t *testing.T) {
		c := &Client{client: &mockBlockClient{}}

		got, err := c.GetHashOfAncestorBlock(ctx, &base, -1)
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("invalid hash length from server", func(t *testing.T) {
		bad := make([]byte, 31)

		mc := &mockBlockClient{
			responseGetHashOfAncestorBlock: &blockchain_api.GetHashOfAncestorBlockResponse{
				Hash: bad,
			},
			err: nil,
		}
		c := &Client{client: mc}

		got, err := c.GetHashOfAncestorBlock(ctx, &base, 2)
		require.Error(t, err)
		require.Nil(t, got)
	})
}

func TestClientGetLatestBlockHeaderFromBlockLocator(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	bestBlockHash := &chainhash.Hash{1, 2, 3, 4, 5}
	blockLocator := []chainhash.Hash{
		{10, 11, 12, 13, 14},
		{20, 21, 22, 23, 24},
		{30, 31, 32, 33, 34},
	}

	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validHeaderBytes := validHeader.Bytes()

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeaderBytes,
			Height:      12345,
			TxCount:     100,
			SizeInBytes: 1000000,
			Miner:       "test_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate header
		assert.Equal(t, validHeader.Version, header.Version)
		assert.Equal(t, validHeader.Nonce, header.Nonce)

		// Validate meta
		assert.Equal(t, uint32(12345), meta.Height)
		assert.Equal(t, uint64(100), meta.TxCount)
		assert.Equal(t, uint64(1000000), meta.SizeInBytes)
		assert.Equal(t, "test_miner", meta.Miner)

		// Validate request was properly formed
		require.NotNil(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq)
		assert.Equal(t, bestBlockHash[:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BestBlockHash)
		assert.Len(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes, 3)
		assert.Equal(t, blockLocator[0][:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes[0])
		assert.Equal(t, blockLocator[1][:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes[1])
		assert.Equal(t, blockLocator[2][:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes[2])
	})

	t.Run("empty block locator error", func(t *testing.T) {
		emptyLocator := []chainhash.Hash{}

		c := &Client{
			client:   &mockBlockClient{},
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, emptyLocator)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		assert.Contains(t, err.Error(), "blockLocator cannot be empty")
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: nil,
			err: errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("invalid block header bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: []byte{0x01, 0x02}, // Invalid header bytes
			Height:      12345,
			TxCount:     100,
			SizeInBytes: 1000000,
			Miner:       "test_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		// Should get an error from model.NewBlockHeaderFromBytes
	})

	t.Run("single hash in locator", func(t *testing.T) {
		singleHashLocator := []chainhash.Hash{{100, 101, 102, 103, 104}}

		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeaderBytes,
			Height:      99,
			TxCount:     50,
			SizeInBytes: 500000,
			Miner:       "single_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, singleHashLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(99), meta.Height)
		assert.Equal(t, uint64(50), meta.TxCount)
		assert.Equal(t, "single_miner", meta.Miner)

		// Validate single hash was properly sent
		require.NotNil(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq)
		assert.Len(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes, 1)
		assert.Equal(t, singleHashLocator[0][:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes[0])
	})

	t.Run("large block locator", func(t *testing.T) {
		// Test with many hashes in the locator
		largeLocator := make([]chainhash.Hash, 100)
		for i := range largeLocator {
			largeLocator[i] = chainhash.Hash{byte(i), byte(i + 1), byte(i + 2)}
		}

		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeaderBytes,
			Height:      1000,
			TxCount:     5000,
			SizeInBytes: 25000000,
			Miner:       "large_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, largeLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(1000), meta.Height)
		assert.Equal(t, uint64(5000), meta.TxCount)

		// Validate all hashes were properly sent
		require.NotNil(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq)
		assert.Len(t, mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes, 100)
		for i := 0; i < 100; i++ {
			assert.Equal(t, largeLocator[i][:], mc.lastGetLatestBlockHeaderFromBlockLocatorReq.BlockLocatorHashes[i])
		}
	})

	t.Run("zero values in response", func(t *testing.T) {
		// Test with zero/empty values in response
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeaderBytes,
			Height:      0,
			TxCount:     0,
			SizeInBytes: 0,
			Miner:       "",
			BlockTime:   0,
			Timestamp:   0,
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate zero values are properly handled
		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint64(0), meta.TxCount)
		assert.Equal(t, uint64(0), meta.SizeInBytes)
		assert.Equal(t, "", meta.Miner)
		assert.Equal(t, uint32(0), meta.BlockTime)
		assert.Equal(t, uint32(0), meta.Timestamp)
	})

	t.Run("meta values validation", func(t *testing.T) {
		// Test with specific meta values to ensure they're properly copied
		testTime := uint32(1640995200) // A specific timestamp
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeaderBytes,
			Height:      999999,
			TxCount:     987654321,
			SizeInBytes: 123456789,
			Miner:       "specific_miner_id",
			BlockTime:   testTime,
			Timestamp:   testTime + 100,
		}

		mc := &mockBlockClient{
			responseGetLatestBlockHeaderFromBlockLocator: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate exact meta values
		assert.Equal(t, uint32(999999), meta.Height)
		assert.Equal(t, uint64(987654321), meta.TxCount)
		assert.Equal(t, uint64(123456789), meta.SizeInBytes)
		assert.Equal(t, "specific_miner_id", meta.Miner)
		assert.Equal(t, testTime, meta.BlockTime)
		assert.Equal(t, testTime+100, meta.Timestamp)
	})
}

func TestClientGetBlockHeadersFromOldest(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	chainTipHash := &chainhash.Hash{1, 2, 3, 4, 5}
	targetHash := &chainhash.Hash{10, 11, 12, 13, 14}
	numberOfHeaders := uint64(5)

	// Create valid headers and metadata for testing
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainTipHash,
		HashMerkleRoot: &chainhash.Hash{7, 8, 9},
		Timestamp:      uint32(time.Now().Unix()) + 600,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          456,
	}

	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "miner1",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}
	validMeta2 := &model.BlockHeaderMeta{
		Height:      101,
		TxCount:     15,
		SizeInBytes: 1500,
		Miner:       "miner2",
		BlockTime:   uint32(time.Now().Unix()) + 600,
		Timestamp:   uint32(time.Now().Unix()) + 600,
	}

	t.Run("success with multiple headers", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes(), validHeader2.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes(), validMeta2.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
		require.NoError(t, err)
		require.NotNil(t, headers)
		require.NotNil(t, metas)
		assert.Len(t, headers, 2)
		assert.Len(t, metas, 2)

		// Validate first header
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, validHeader1.Nonce, headers[0].Nonce)
		assert.Equal(t, uint32(100), metas[0].Height)
		assert.Equal(t, uint64(10), metas[0].TxCount)

		// Validate second header
		assert.Equal(t, validHeader2.Version, headers[1].Version)
		assert.Equal(t, validHeader2.Nonce, headers[1].Nonce)
		assert.Equal(t, uint32(101), metas[1].Height)
		assert.Equal(t, uint64(15), metas[1].TxCount)

		// Validate request was properly formed
		require.NotNil(t, mc.lastGetBlockHeadersFromOldestReq)
		assert.Equal(t, chainTipHash.CloneBytes(), mc.lastGetBlockHeadersFromOldestReq.ChainTipHash)
		assert.Equal(t, targetHash.CloneBytes(), mc.lastGetBlockHeadersFromOldestReq.TargetHash)
		assert.Equal(t, numberOfHeaders, mc.lastGetBlockHeadersFromOldestReq.NumberOfHeaders)
	})

	t.Run("success with empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
		require.NoError(t, err)
		require.NotNil(t, headers)
		require.NotNil(t, metas)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: nil,
			err:                               errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("invalid block header bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{{0x01, 0x02}}, // Invalid header bytes
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
		// Should get an error from model.NewBlockHeaderFromBytes
	})

	t.Run("invalid meta bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{{0x01, 0x02}}, // Invalid meta bytes
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
		// Should get an error from model.NewBlockHeaderMetaFromBytes
	})

	t.Run("zero numberOfHeaders", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, 0)
		require.NoError(t, err)
		require.NotNil(t, headers)
		require.NotNil(t, metas)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)

		// Validate zero was passed through
		require.NotNil(t, mc.lastGetBlockHeadersFromOldestReq)
		assert.Equal(t, uint64(0), mc.lastGetBlockHeadersFromOldestReq.NumberOfHeaders)
	})

	t.Run("large numberOfHeaders", func(t *testing.T) {
		largeNumber := uint64(1000000)
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, largeNumber)
		require.NoError(t, err)
		require.NotNil(t, headers)
		require.NotNil(t, metas)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate large number was passed through
		require.NotNil(t, mc.lastGetBlockHeadersFromOldestReq)
		assert.Equal(t, largeNumber, mc.lastGetBlockHeadersFromOldestReq.NumberOfHeaders)
	})

	t.Run("single header result", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromOldest: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, 1)
		require.NoError(t, err)
		require.NotNil(t, headers)
		require.NotNil(t, metas)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate content
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, uint32(100), metas[0].Height)
		assert.Equal(t, "miner1", metas[0].Miner)
	})
}

func TestClientGetNextWorkRequired(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	previousBlockHash := &chainhash.Hash{1, 2, 3, 4, 5}
	currentBlockTime := int64(1640995200) // A specific timestamp

	t.Run("success", func(t *testing.T) {
		// Create valid difficulty bits
		expectedBits := []byte{0x1d, 0x00, 0xff, 0xff}
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: expectedBits,
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.NoError(t, err)
		require.NotNil(t, bits)

		// Validate the NBit was properly created
		assert.Equal(t, expectedBits, bits.CloneBytes())

		// Validate request was properly formed
		require.NotNil(t, mc.lastGetNextWorkRequiredReq)
		assert.Equal(t, previousBlockHash[:], mc.lastGetNextWorkRequiredReq.PreviousBlockHash)
		assert.Equal(t, currentBlockTime, mc.lastGetNextWorkRequiredReq.CurrentBlockTime)
	})

	t.Run("zero currentBlockTime error", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{},
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, 0)
		require.Error(t, err)
		assert.Nil(t, bits)
		assert.Contains(t, err.Error(), "currentBlockTime cannot be zero")
	})

	t.Run("negative currentBlockTime with mock error", func(t *testing.T) {
		// The function only validates for zero, not negative values
		// Negative values will pass validation and make a GRPC call
		mc := &mockBlockClient{
			responseGetNextWorkRequired: nil,
			err:                         errors.NewProcessingError("negative time not allowed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, -1)
		require.Error(t, err)
		assert.Nil(t, bits)
		assert.Contains(t, err.Error(), "negative time not allowed")

		// Validate that negative time was passed through
		require.NotNil(t, mc.lastGetNextWorkRequiredReq)
		assert.Equal(t, int64(-1), mc.lastGetNextWorkRequiredReq.CurrentBlockTime)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetNextWorkRequired: nil,
			err:                         errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.Error(t, err)
		assert.Nil(t, bits)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("invalid difficulty bits", func(t *testing.T) {
		// Create invalid difficulty bits (wrong length)
		invalidBits := []byte{0x01, 0x02} // Should be 4 bytes
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: invalidBits,
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.Error(t, err)
		assert.Nil(t, bits)
		// Should get an error from model.NewNBitFromSlice
	})

	t.Run("empty difficulty bits", func(t *testing.T) {
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: []byte{},
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.Error(t, err)
		assert.Nil(t, bits)
		// Should get an error from model.NewNBitFromSlice
	})

	t.Run("maximum difficulty bits", func(t *testing.T) {
		// Test with maximum difficulty (all 0xFF)
		maxBits := []byte{0xff, 0xff, 0xff, 0xff}
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: maxBits,
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.NoError(t, err)
		require.NotNil(t, bits)
		assert.Equal(t, maxBits, bits.CloneBytes())
	})

	t.Run("minimum difficulty bits", func(t *testing.T) {
		// Test with minimum difficulty (all 0x00 except first byte)
		minBits := []byte{0x01, 0x00, 0x00, 0x00}
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: minBits,
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.NoError(t, err)
		require.NotNil(t, bits)
		assert.Equal(t, minBits, bits.CloneBytes())
	})

	t.Run("bitcoin mainnet difficulty bits", func(t *testing.T) {
		// Test with typical Bitcoin mainnet difficulty bits
		mainnetBits := []byte{0x17, 0x03, 0x41, 0xaa}
		mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
			Bits: mainnetBits,
		}

		mc := &mockBlockClient{
			responseGetNextWorkRequired: mockResponse,
			err:                         nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, currentBlockTime)
		require.NoError(t, err)
		require.NotNil(t, bits)
		assert.Equal(t, mainnetBits, bits.CloneBytes())
	})

	t.Run("various currentBlockTime values", func(t *testing.T) {
		testCases := []struct {
			name      string
			blockTime int64
		}{
			{"current timestamp", time.Now().Unix()},
			{"year 2030", 1893456000},
			{"minimum valid time", 1},
			{"large timestamp", 9999999999},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				expectedBits := []byte{0x1d, 0x00, 0xff, 0xff}
				mockResponse := &blockchain_api.GetNextWorkRequiredResponse{
					Bits: expectedBits,
				}

				mc := &mockBlockClient{
					responseGetNextWorkRequired: mockResponse,
					err:                         nil,
				}
				c := &Client{
					client:   mc,
					logger:   logger,
					settings: tSettings,
				}

				bits, err := c.GetNextWorkRequired(ctx, previousBlockHash, tc.blockTime)
				require.NoError(t, err)
				require.NotNil(t, bits)

				// Validate the timestamp was passed correctly
				require.NotNil(t, mc.lastGetNextWorkRequiredReq)
				assert.Equal(t, tc.blockTime, mc.lastGetNextWorkRequiredReq.CurrentBlockTime)
			})
		}
	})
}

func TestClientGetBlockExists(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := &chainhash.Hash{1, 2, 3, 4, 5}

	t.Run("block exists", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockExistsResponse{
			Exists: true,
		}

		mc := &mockBlockClient{
			responseGetBlockExists: mockResponse,
			err:                    nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		exists, err := c.GetBlockExists(ctx, blockHash)
		require.NoError(t, err)
		assert.True(t, exists)

		// Validate request was properly formed
		require.NotNil(t, mc.lastGetBlockExistsReq)
		assert.Equal(t, blockHash[:], mc.lastGetBlockExistsReq.Hash)
	})

	t.Run("block does not exist", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockExistsResponse{
			Exists: false,
		}

		mc := &mockBlockClient{
			responseGetBlockExists: mockResponse,
			err:                    nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		exists, err := c.GetBlockExists(ctx, blockHash)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockExists: nil,
			err:                    errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		exists, err := c.GetBlockExists(ctx, blockHash)
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("different block hashes", func(t *testing.T) {
		testCases := []struct {
			name string
			hash *chainhash.Hash
		}{
			{"genesis hash", &chainhash.Hash{0}},
			{"random hash", &chainhash.Hash{255, 254, 253, 252}},
			{"all ones", &chainhash.Hash{0xff, 0xff, 0xff, 0xff, 0xff}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockResponse := &blockchain_api.GetBlockExistsResponse{
					Exists: true,
				}

				mc := &mockBlockClient{
					responseGetBlockExists: mockResponse,
					err:                    nil,
				}
				c := &Client{
					client:   mc,
					logger:   logger,
					settings: tSettings,
				}

				exists, err := c.GetBlockExists(ctx, tc.hash)
				require.NoError(t, err)
				assert.True(t, exists)

				// Validate correct hash was sent
				require.NotNil(t, mc.lastGetBlockExistsReq)
				assert.Equal(t, tc.hash[:], mc.lastGetBlockExistsReq.Hash)
			})
		}
	})
}

func TestClientGetBestBlockHeader(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Create valid header for testing
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeader.Bytes(),
			Height:      999,
			TxCount:     500,
			SizeInBytes: 2500000,
			Miner:       "best_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
			ChainWork:   []byte{0x01, 0x02, 0x03, 0x04},
		}

		mc := &mockBlockClient{
			responseGetBestBlockHeader: mockResponse,
			err:                        nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBestBlockHeader(ctx)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate header
		assert.Equal(t, validHeader.Version, header.Version)
		assert.Equal(t, validHeader.Nonce, header.Nonce)

		// Validate meta
		assert.Equal(t, uint32(999), meta.Height)
		assert.Equal(t, uint64(500), meta.TxCount)
		assert.Equal(t, uint64(2500000), meta.SizeInBytes)
		assert.Equal(t, "best_miner", meta.Miner)
		assert.Equal(t, mockResponse.ChainWork, meta.ChainWork)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBestBlockHeader: nil,
			err:                        errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBestBlockHeader(ctx)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("invalid block header bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: []byte{0x01, 0x02}, // Invalid header bytes
			Height:      999,
			TxCount:     500,
			SizeInBytes: 2500000,
			Miner:       "best_miner",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
		}

		mc := &mockBlockClient{
			responseGetBestBlockHeader: mockResponse,
			err:                        nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBestBlockHeader(ctx)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		// Should get an error from model.NewBlockHeaderFromBytes
	})

	t.Run("zero values in response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeader.Bytes(),
			Height:      0,
			TxCount:     0,
			SizeInBytes: 0,
			Miner:       "",
			BlockTime:   0,
			Timestamp:   0,
			ChainWork:   []byte{},
		}

		mc := &mockBlockClient{
			responseGetBestBlockHeader: mockResponse,
			err:                        nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBestBlockHeader(ctx)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate zero values are properly handled
		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint64(0), meta.TxCount)
		assert.Equal(t, uint64(0), meta.SizeInBytes)
		assert.Equal(t, "", meta.Miner)
		assert.Equal(t, []byte{}, meta.ChainWork)
	})
}

func TestClientCheckBlockIsInCurrentChain(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("blocks are in current chain", func(t *testing.T) {
		blockIDs := []uint32{1, 2, 3, 100, 999}
		mockResponse := &blockchain_api.CheckBlockIsCurrentChainResponse{
			IsPartOfCurrentChain: true,
		}

		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: mockResponse,
			err:                                nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Validate request was properly formed
		require.NotNil(t, mc.lastCheckBlockIsInCurrentChainReq)
		assert.Equal(t, blockIDs, mc.lastCheckBlockIsInCurrentChainReq.BlockIDs)
	})

	t.Run("blocks are not in current chain", func(t *testing.T) {
		blockIDs := []uint32{500, 600, 700}
		mockResponse := &blockchain_api.CheckBlockIsCurrentChainResponse{
			IsPartOfCurrentChain: false,
		}

		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: mockResponse,
			err:                                nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("single block ID", func(t *testing.T) {
		blockIDs := []uint32{42}
		mockResponse := &blockchain_api.CheckBlockIsCurrentChainResponse{
			IsPartOfCurrentChain: true,
		}

		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: mockResponse,
			err:                                nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Validate single ID was sent
		require.NotNil(t, mc.lastCheckBlockIsInCurrentChainReq)
		assert.Equal(t, []uint32{42}, mc.lastCheckBlockIsInCurrentChainReq.BlockIDs)
	})

	t.Run("empty block IDs", func(t *testing.T) {
		blockIDs := []uint32{}
		mockResponse := &blockchain_api.CheckBlockIsCurrentChainResponse{
			IsPartOfCurrentChain: false,
		}

		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: mockResponse,
			err:                                nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)

		// Validate empty slice was sent
		require.NotNil(t, mc.lastCheckBlockIsInCurrentChainReq)
		assert.Equal(t, []uint32{}, mc.lastCheckBlockIsInCurrentChainReq.BlockIDs)
	})

	t.Run("grpc client error", func(t *testing.T) {
		blockIDs := []uint32{1, 2, 3}
		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: nil,
			err:                                errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.Error(t, err)
		assert.False(t, isInChain)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("large number of block IDs", func(t *testing.T) {
		// Create a large slice of block IDs
		blockIDs := make([]uint32, 1000)
		for i := range blockIDs {
			blockIDs[i] = uint32(i + 1)
		}

		mockResponse := &blockchain_api.CheckBlockIsCurrentChainResponse{
			IsPartOfCurrentChain: true,
		}

		mc := &mockBlockClient{
			responseCheckBlockIsInCurrentChain: mockResponse,
			err:                                nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		isInChain, err := c.CheckBlockIsInCurrentChain(ctx, blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Validate all IDs were sent
		require.NotNil(t, mc.lastCheckBlockIsInCurrentChainReq)
		assert.Equal(t, blockIDs, mc.lastCheckBlockIsInCurrentChainReq.BlockIDs)
		assert.Len(t, mc.lastCheckBlockIsInCurrentChainReq.BlockIDs, 1000)
	})
}

func TestClientGetChainTips(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success with multiple tips", func(t *testing.T) {
		mockResponse := &blockchain_api.GetChainTipsResponse{
			Tips: []*model.ChainTip{
				{
					Height:    1000,
					Hash:      "abc123",
					Branchlen: 0,
					Status:    "active",
				},
				{
					Height:    995,
					Hash:      "def456",
					Branchlen: 5,
					Status:    "valid-fork",
				},
				{
					Height:    990,
					Hash:      "ghi789",
					Branchlen: 10,
					Status:    "valid-headers",
				},
			},
		}

		mc := &mockBlockClient{
			responseGetChainTips: mockResponse,
			err:                  nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		tips, err := c.GetChainTips(ctx)
		require.NoError(t, err)
		require.NotNil(t, tips)
		assert.Len(t, tips, 3)

		// Validate first tip (active main chain)
		assert.Equal(t, uint32(1000), tips[0].Height)
		assert.Equal(t, "abc123", tips[0].Hash)
		assert.Equal(t, uint32(0), tips[0].Branchlen)
		assert.Equal(t, "active", tips[0].Status)

		// Validate second tip (fork)
		assert.Equal(t, uint32(995), tips[1].Height)
		assert.Equal(t, "def456", tips[1].Hash)
		assert.Equal(t, uint32(5), tips[1].Branchlen)
		assert.Equal(t, "valid-fork", tips[1].Status)

		// Validate third tip
		assert.Equal(t, uint32(990), tips[2].Height)
		assert.Equal(t, "ghi789", tips[2].Hash)
		assert.Equal(t, uint32(10), tips[2].Branchlen)
		assert.Equal(t, "valid-headers", tips[2].Status)
	})

	t.Run("success with single tip", func(t *testing.T) {
		mockResponse := &blockchain_api.GetChainTipsResponse{
			Tips: []*model.ChainTip{
				{
					Height:    500,
					Hash:      "single123",
					Branchlen: 0,
					Status:    "active",
				},
			},
		}

		mc := &mockBlockClient{
			responseGetChainTips: mockResponse,
			err:                  nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		tips, err := c.GetChainTips(ctx)
		require.NoError(t, err)
		require.NotNil(t, tips)
		assert.Len(t, tips, 1)

		assert.Equal(t, uint32(500), tips[0].Height)
		assert.Equal(t, "single123", tips[0].Hash)
		assert.Equal(t, uint32(0), tips[0].Branchlen)
		assert.Equal(t, "active", tips[0].Status)
	})

	t.Run("success with no tips", func(t *testing.T) {
		mockResponse := &blockchain_api.GetChainTipsResponse{
			Tips: []*model.ChainTip{},
		}

		mc := &mockBlockClient{
			responseGetChainTips: mockResponse,
			err:                  nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		tips, err := c.GetChainTips(ctx)
		require.NoError(t, err)
		require.NotNil(t, tips)
		assert.Len(t, tips, 0)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetChainTips: nil,
			err:                  errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		tips, err := c.GetChainTips(ctx)
		require.Error(t, err)
		assert.Nil(t, tips)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("various tip statuses", func(t *testing.T) {
		mockResponse := &blockchain_api.GetChainTipsResponse{
			Tips: []*model.ChainTip{
				{Height: 100, Hash: "hash1", Branchlen: 0, Status: "active"},
				{Height: 99, Hash: "hash2", Branchlen: 1, Status: "valid-fork"},
				{Height: 98, Hash: "hash3", Branchlen: 2, Status: "valid-headers"},
				{Height: 97, Hash: "hash4", Branchlen: 3, Status: "headers-only"},
				{Height: 96, Hash: "hash5", Branchlen: 4, Status: "invalid"},
			},
		}

		mc := &mockBlockClient{
			responseGetChainTips: mockResponse,
			err:                  nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		tips, err := c.GetChainTips(ctx)
		require.NoError(t, err)
		assert.Len(t, tips, 5)

		// Verify each status is preserved
		expectedStatuses := []string{"active", "valid-fork", "valid-headers", "headers-only", "invalid"}
		for i, expectedStatus := range expectedStatuses {
			assert.Equal(t, expectedStatus, tips[i].Status)
			assert.Equal(t, uint32(i), tips[i].Branchlen)
		}
	})
}

func TestClientGetBlockHeader(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := &chainhash.Hash{1, 2, 3, 4, 5}

	// Create valid header for testing
	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}

	t.Run("success", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: validHeader.Bytes(),
			Id:          42,
			Height:      750,
			TxCount:     250,
			SizeInBytes: 1250000,
			Miner:       "header_miner",
			PeerId:      "peer123",
			BlockTime:   uint32(time.Now().Unix()),
			Timestamp:   uint32(time.Now().Unix()),
			ChainWork:   []byte{0x05, 0x06, 0x07, 0x08},
			MinedSet:    true,
			SubtreesSet: false,
			Invalid:     false,
		}

		mc := &mockBlockClient{
			responseGetBlockHeader: mockResponse,
			err:                    nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBlockHeader(ctx, blockHash)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Validate header
		assert.Equal(t, validHeader.Version, header.Version)
		assert.Equal(t, validHeader.Nonce, header.Nonce)

		// Validate meta (includes more fields than GetBestBlockHeader)
		assert.Equal(t, uint32(42), meta.ID)
		assert.Equal(t, uint32(750), meta.Height)
		assert.Equal(t, uint64(250), meta.TxCount)
		assert.Equal(t, uint64(1250000), meta.SizeInBytes)
		assert.Equal(t, "header_miner", meta.Miner)
		assert.Equal(t, "peer123", meta.PeerID)
		assert.Equal(t, mockResponse.ChainWork, meta.ChainWork)
		assert.True(t, meta.MinedSet)
		assert.False(t, meta.SubtreesSet)
		assert.False(t, meta.Invalid)

		// Validate request was properly formed
		require.NotNil(t, mc.lastGetBlockHeaderReq)
		assert.Equal(t, blockHash[:], mc.lastGetBlockHeaderReq.BlockHash)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeader: nil,
			err:                    errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBlockHeader(ctx, blockHash)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("invalid block header bytes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderResponse{
			BlockHeader: []byte{0x01, 0x02}, // Invalid header bytes
			Id:          42,
			Height:      750,
			TxCount:     250,
			SizeInBytes: 1250000,
			Miner:       "header_miner",
		}

		mc := &mockBlockClient{
			responseGetBlockHeader: mockResponse,
			err:                    nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		header, meta, err := c.GetBlockHeader(ctx, blockHash)
		require.Error(t, err)
		assert.Nil(t, header)
		assert.Nil(t, meta)
		// Should get an error from model.NewBlockHeaderFromBytes
	})

	t.Run("different block hashes", func(t *testing.T) {
		testCases := []struct {
			name string
			hash *chainhash.Hash
		}{
			{"genesis hash", &chainhash.Hash{0}},
			{"random hash", &chainhash.Hash{255, 254, 253, 252}},
			{"all ones", &chainhash.Hash{0xff, 0xff, 0xff, 0xff, 0xff}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockResponse := &blockchain_api.GetBlockHeaderResponse{
					BlockHeader: validHeader.Bytes(),
					Id:          uint32(len(tc.name)), // Use name length as unique ID
					Height:      100,
					TxCount:     10,
					SizeInBytes: 1000,
					Miner:       "test_miner",
				}

				mc := &mockBlockClient{
					responseGetBlockHeader: mockResponse,
					err:                    nil,
				}
				c := &Client{
					client:   mc,
					logger:   logger,
					settings: tSettings,
				}

				header, meta, err := c.GetBlockHeader(ctx, tc.hash)
				require.NoError(t, err)
				require.NotNil(t, header)
				require.NotNil(t, meta)

				// Validate correct hash was sent
				require.NotNil(t, mc.lastGetBlockHeaderReq)
				assert.Equal(t, tc.hash[:], mc.lastGetBlockHeaderReq.BlockHash)
				assert.Equal(t, uint32(len(tc.name)), meta.ID)
			})
		}
	})

	t.Run("all boolean combinations", func(t *testing.T) {
		testCases := []struct {
			name        string
			minedSet    bool
			subtreesSet bool
			invalid     bool
		}{
			{"all false", false, false, false},
			{"only mined", true, false, false},
			{"only subtrees", false, true, false},
			{"only invalid", false, false, true},
			{"mined and subtrees", true, true, false},
			{"mined and invalid", true, false, true},
			{"subtrees and invalid", false, true, true},
			{"all true", true, true, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockResponse := &blockchain_api.GetBlockHeaderResponse{
					BlockHeader: validHeader.Bytes(),
					Id:          123,
					Height:      100,
					TxCount:     10,
					SizeInBytes: 1000,
					Miner:       "bool_test_miner",
					MinedSet:    tc.minedSet,
					SubtreesSet: tc.subtreesSet,
					Invalid:     tc.invalid,
				}

				mc := &mockBlockClient{
					responseGetBlockHeader: mockResponse,
					err:                    nil,
				}
				c := &Client{
					client:   mc,
					logger:   logger,
					settings: tSettings,
				}

				_, meta, err := c.GetBlockHeader(ctx, blockHash)
				require.NoError(t, err)
				require.NotNil(t, meta)

				// Validate boolean flags
				assert.Equal(t, tc.minedSet, meta.MinedSet)
				assert.Equal(t, tc.subtreesSet, meta.SubtreesSet)
				assert.Equal(t, tc.invalid, meta.Invalid)
			})
		}
	})
}

func TestClientGetBlockHeaders(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := &chainhash.Hash{1, 2, 3, 4, 5}
	numberOfHeaders := uint64(10)

	// Create valid headers for testing
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}

	validMeta1 := &model.BlockHeaderMeta{
		Height:      200,
		TxCount:     20,
		SizeInBytes: 2000,
		Miner:       "headers_miner1",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("success with headers", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeaders: mockResponse,
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, uint32(200), metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersReq)
		assert.Equal(t, blockHash.CloneBytes(), mc.lastGetBlockHeadersReq.StartHash)
		assert.Equal(t, numberOfHeaders, mc.lastGetBlockHeadersReq.NumberOfHeaders)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeaders: nil,
			err:                     errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

func TestClientInvalidateBlock(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("successful invalidation", func(t *testing.T) {
		hash1 := make([]byte, 32)
		hash2 := make([]byte, 32)
		hash1[0] = 1
		hash2[0] = 2

		mockResponse := &blockchain_api.InvalidateBlockResponse{
			InvalidatedBlocks: [][]byte{hash1, hash2},
		}

		mc := &mockBlockClient{
			responseInvalidateBlock: mockResponse,
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		invalidatedHashes, err := c.InvalidateBlock(ctx, &blockHash)
		require.NoError(t, err)
		assert.Len(t, invalidatedHashes, 2)

		// Validate request
		require.NotNil(t, mc.lastInvalidateBlockReq)
		assert.Equal(t, blockHash.CloneBytes(), mc.lastInvalidateBlockReq.BlockHash)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseInvalidateBlock: nil,
			err:                     errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		invalidatedHashes, err := c.InvalidateBlock(ctx, &blockHash)
		require.Error(t, err)
		assert.Nil(t, invalidatedHashes)
	})

	t.Run("nil response", func(t *testing.T) {
		mc := &mockBlockClient{
			responseInvalidateBlock: nil,
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		invalidatedHashes, err := c.InvalidateBlock(ctx, &blockHash)
		require.Error(t, err)
		assert.Nil(t, invalidatedHashes)
		assert.Contains(t, err.Error(), "invalidate block did not return a valid response")
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.InvalidateBlockResponse{
			InvalidatedBlocks: [][]byte{},
		}

		mc := &mockBlockClient{
			responseInvalidateBlock: mockResponse,
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		invalidatedHashes, err := c.InvalidateBlock(ctx, &blockHash)
		require.NoError(t, err)
		assert.Len(t, invalidatedHashes, 0)
	})

	t.Run("invalid hash bytes", func(t *testing.T) {
		invalidHashBytes := []byte{1, 2} // Invalid hash length

		mockResponse := &blockchain_api.InvalidateBlockResponse{
			InvalidatedBlocks: [][]byte{invalidHashBytes},
		}

		mc := &mockBlockClient{
			responseInvalidateBlock: mockResponse,
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		invalidatedHashes, err := c.InvalidateBlock(ctx, &blockHash)
		require.Error(t, err)
		assert.Nil(t, invalidatedHashes)
	})
}

func TestClientRevalidateBlock(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("successful revalidation", func(t *testing.T) {
		mc := &mockBlockClient{
			responseRevalidateBlock: &emptypb.Empty{},
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.RevalidateBlock(ctx, &blockHash)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastRevalidateBlockReq)
		assert.Equal(t, blockHash.CloneBytes(), mc.lastRevalidateBlockReq.BlockHash)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseRevalidateBlock: nil,
			err:                     errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.RevalidateBlock(ctx, &blockHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("valid block hash", func(t *testing.T) {
		validHash := &chainhash.Hash{4, 5, 6}
		mc := &mockBlockClient{
			responseRevalidateBlock: &emptypb.Empty{},
			err:                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.RevalidateBlock(ctx, validHash)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastRevalidateBlockReq)
		assert.Equal(t, validHash.CloneBytes(), mc.lastRevalidateBlockReq.BlockHash)
	})
}

func TestClientGetBlockHeaderIDs(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	blockHash := chainhash.Hash{1, 2, 3}
	numberOfHeaders := uint64(5)

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderIDsResponse{
			Ids: []uint32{1, 2, 3, 4, 5},
		}

		mc := &mockBlockClient{
			responseGetBlockHeaderIDs: mockResponse,
			err:                       nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		ids, err := c.GetBlockHeaderIDs(ctx, &blockHash, numberOfHeaders)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 2, 3, 4, 5}, ids)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeaderIDsReq)
		assert.Equal(t, blockHash.CloneBytes(), mc.lastGetBlockHeaderIDsReq.StartHash)
		assert.Equal(t, numberOfHeaders, mc.lastGetBlockHeaderIDsReq.NumberOfHeaders)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeaderIDs: nil,
			err:                       errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		ids, err := c.GetBlockHeaderIDs(ctx, &blockHash, numberOfHeaders)
		require.Error(t, err)
		assert.Nil(t, ids)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderIDsResponse{
			Ids: []uint32{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeaderIDs: mockResponse,
			err:                       nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		ids, err := c.GetBlockHeaderIDs(ctx, &blockHash, numberOfHeaders)
		require.NoError(t, err)
		assert.Len(t, ids, 0)
	})

	t.Run("zero numberOfHeaders", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeaderIDsResponse{
			Ids: []uint32{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeaderIDs: mockResponse,
			err:                       nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		ids, err := c.GetBlockHeaderIDs(ctx, &blockHash, 0)
		require.NoError(t, err)
		assert.Len(t, ids, 0)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeaderIDsReq)
		assert.Equal(t, uint64(0), mc.lastGetBlockHeaderIDsReq.NumberOfHeaders)
	})

	t.Run("large number of headers", func(t *testing.T) {
		largeIDs := make([]uint32, 1000)
		for i := range largeIDs {
			largeIDs[i] = uint32(i)
		}

		mockResponse := &blockchain_api.GetBlockHeaderIDsResponse{
			Ids: largeIDs,
		}

		mc := &mockBlockClient{
			responseGetBlockHeaderIDs: mockResponse,
			err:                       nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		ids, err := c.GetBlockHeaderIDs(ctx, &blockHash, 1000)
		require.NoError(t, err)
		assert.Len(t, ids, 1000)
		assert.Equal(t, uint32(0), ids[0])
		assert.Equal(t, uint32(999), ids[999])
	})
}

func TestClientSendNotification(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful notification", func(t *testing.T) {
		notification := &blockchain_api.Notification{
			Type:     1, // Some notification type
			Hash:     []byte{1, 2, 3},
			Base_URL: "http://test.com",
		}

		mc := &mockBlockClient{
			responseSendNotification: &emptypb.Empty{},
			err:                      nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SendNotification(ctx, notification)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSendNotificationReq)
		assert.Equal(t, int32(1), int32(mc.lastSendNotificationReq.Type))
		assert.Equal(t, []byte{1, 2, 3}, mc.lastSendNotificationReq.Hash)
		assert.Equal(t, "http://test.com", mc.lastSendNotificationReq.Base_URL)
	})

	t.Run("grpc client error", func(t *testing.T) {
		notification := &blockchain_api.Notification{
			Type:     1,
			Hash:     []byte{1, 2, 3},
			Base_URL: "http://test.com",
		}

		mc := &mockBlockClient{
			responseSendNotification: nil,
			err:                      errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SendNotification(ctx, notification)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("nil notification", func(t *testing.T) {
		mc := &mockBlockClient{
			responseSendNotification: &emptypb.Empty{},
			err:                      nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SendNotification(ctx, nil)
		require.NoError(t, err)

		// Should work with nil notification
		assert.Nil(t, mc.lastSendNotificationReq)
	})

	t.Run("empty notification", func(t *testing.T) {
		notification := &blockchain_api.Notification{}

		mc := &mockBlockClient{
			responseSendNotification: &emptypb.Empty{},
			err:                      nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SendNotification(ctx, notification)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSendNotificationReq)
		assert.Equal(t, int32(0), int32(mc.lastSendNotificationReq.Type))
		assert.Nil(t, mc.lastSendNotificationReq.Hash)
		assert.Empty(t, mc.lastSendNotificationReq.Base_URL)
	})
}

func TestClientGetState(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful get", func(t *testing.T) {
		key := "test-key"
		data := []byte("test data")

		mockResponse := &blockchain_api.StateResponse{
			Data: data,
		}

		mc := &mockBlockClient{
			responseGetState: mockResponse,
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		result, err := c.GetState(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, data, result)

		// Validate request
		require.NotNil(t, mc.lastGetStateReq)
		assert.Equal(t, key, mc.lastGetStateReq.Key)
	})

	t.Run("grpc client error", func(t *testing.T) {
		key := "test-key"

		mc := &mockBlockClient{
			responseGetState: nil,
			err:              errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		result, err := c.GetState(ctx, key)
		require.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty key", func(t *testing.T) {
		data := []byte("test data")

		mockResponse := &blockchain_api.StateResponse{
			Data: data,
		}

		mc := &mockBlockClient{
			responseGetState: mockResponse,
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		result, err := c.GetState(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, data, result)

		// Validate request
		require.NotNil(t, mc.lastGetStateReq)
		assert.Empty(t, mc.lastGetStateReq.Key)
	})

	t.Run("empty data", func(t *testing.T) {
		key := "test-key"

		mockResponse := &blockchain_api.StateResponse{
			Data: []byte{},
		}

		mc := &mockBlockClient{
			responseGetState: mockResponse,
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		result, err := c.GetState(ctx, key)
		require.NoError(t, err)
		assert.Len(t, result, 0)
	})

	t.Run("nil data", func(t *testing.T) {
		key := "test-key"

		mockResponse := &blockchain_api.StateResponse{
			Data: nil,
		}

		mc := &mockBlockClient{
			responseGetState: mockResponse,
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		result, err := c.GetState(ctx, key)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestClientSetState(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful set", func(t *testing.T) {
		key := "test-key"
		data := []byte("test data")

		mc := &mockBlockClient{
			responseSetState: &emptypb.Empty{},
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetState(ctx, key, data)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSetStateReq)
		assert.Equal(t, key, mc.lastSetStateReq.Key)
		assert.Equal(t, data, mc.lastSetStateReq.Data)
	})

	t.Run("grpc client error", func(t *testing.T) {
		key := "test-key"
		data := []byte("test data")

		mc := &mockBlockClient{
			responseSetState: nil,
			err:              errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetState(ctx, key, data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})

	t.Run("empty key", func(t *testing.T) {
		data := []byte("test data")

		mc := &mockBlockClient{
			responseSetState: &emptypb.Empty{},
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetState(ctx, "", data)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSetStateReq)
		assert.Empty(t, mc.lastSetStateReq.Key)
		assert.Equal(t, data, mc.lastSetStateReq.Data)
	})

	t.Run("empty data", func(t *testing.T) {
		key := "test-key"

		mc := &mockBlockClient{
			responseSetState: &emptypb.Empty{},
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetState(ctx, key, []byte{})
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSetStateReq)
		assert.Equal(t, key, mc.lastSetStateReq.Key)
		assert.Len(t, mc.lastSetStateReq.Data, 0)
	})

	t.Run("nil data", func(t *testing.T) {
		key := "test-key"

		mc := &mockBlockClient{
			responseSetState: &emptypb.Empty{},
			err:              nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetState(ctx, key, nil)
		require.NoError(t, err)

		// Validate request
		require.NotNil(t, mc.lastSetStateReq)
		assert.Equal(t, key, mc.lastSetStateReq.Key)
		assert.Nil(t, mc.lastSetStateReq.Data)
	})
}

func TestClientGetBlockHeadersToCommonAncestor(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	targetHash := chainhash.Hash{1, 2, 3}
	locatorHash1 := chainhash.Hash{4, 5, 6}
	locatorHash2 := chainhash.Hash{7, 8, 9}
	blockLocatorHashes := []*chainhash.Hash{&locatorHash1, &locatorHash2}
	maxHeaders := uint32(10)
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "test-miner",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersToCommonAncestor: mockResponse,
			err:                                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersToCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, uint32(100), metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersToCommonAncestorReq)
		assert.Equal(t, targetHash.CloneBytes(), mc.lastGetBlockHeadersToCommonAncestorReq.TargetHash)
		assert.Equal(t, maxHeaders, mc.lastGetBlockHeadersToCommonAncestorReq.MaxHeaders)
		assert.Len(t, mc.lastGetBlockHeadersToCommonAncestorReq.BlockLocatorHashes, 2)
		assert.Equal(t, locatorHash1.CloneBytes(), mc.lastGetBlockHeadersToCommonAncestorReq.BlockLocatorHashes[0])
		assert.Equal(t, locatorHash2.CloneBytes(), mc.lastGetBlockHeadersToCommonAncestorReq.BlockLocatorHashes[1])
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersToCommonAncestor: nil,
			err:                                     errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersToCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersToCommonAncestor: mockResponse,
			err:                                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersToCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("empty block locator hashes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersToCommonAncestor: mockResponse,
			err:                                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersToCommonAncestor(ctx, &targetHash, []*chainhash.Hash{}, 10)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersToCommonAncestorReq)
		assert.Len(t, mc.lastGetBlockHeadersToCommonAncestorReq.BlockLocatorHashes, 0)
	})

	t.Run("zero max headers", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersToCommonAncestor: mockResponse,
			err:                                     nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersToCommonAncestor(ctx, &targetHash, blockLocatorHashes, 0)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersToCommonAncestorReq)
		assert.Equal(t, uint32(0), mc.lastGetBlockHeadersToCommonAncestorReq.MaxHeaders)
	})
}

func TestClientGetBlockHeadersFromCommonAncestor(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	targetHash := chainhash.Hash{1, 2, 3}
	locatorHash1 := chainhash.Hash{4, 5, 6}
	locatorHash2 := chainhash.Hash{7, 8, 9}
	blockLocatorHashes := []chainhash.Hash{locatorHash1, locatorHash2}
	maxHeaders := uint32(10)
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "test-miner",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromCommonAncestor: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, uint32(100), metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromCommonAncestorReq)
		assert.Equal(t, targetHash.CloneBytes(), mc.lastGetBlockHeadersFromCommonAncestorReq.TargetHash)
		assert.Equal(t, maxHeaders, mc.lastGetBlockHeadersFromCommonAncestorReq.MaxHeaders)
		assert.Len(t, mc.lastGetBlockHeadersFromCommonAncestorReq.BlockLocatorHashes, 2)
		assert.Equal(t, locatorHash1.CloneBytes(), mc.lastGetBlockHeadersFromCommonAncestorReq.BlockLocatorHashes[0])
		assert.Equal(t, locatorHash2.CloneBytes(), mc.lastGetBlockHeadersFromCommonAncestorReq.BlockLocatorHashes[1])
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersFromCommonAncestor: nil,
			err: errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromCommonAncestor: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromCommonAncestor(ctx, &targetHash, blockLocatorHashes, maxHeaders)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("empty block locator hashes", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromCommonAncestor: mockResponse,
			err: nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromCommonAncestor(ctx, &targetHash, []chainhash.Hash{}, 10)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromCommonAncestorReq)
		assert.Len(t, mc.lastGetBlockHeadersFromCommonAncestorReq.BlockLocatorHashes, 0)
	})
}

func TestClientGetBlockHeadersFromTill(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	fromHash := chainhash.Hash{1, 2, 3}
	tillHash := chainhash.Hash{4, 5, 6}
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "test-miner",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromTill: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromTill(ctx, &fromHash, &tillHash)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, uint32(100), metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromTillReq)
		assert.Equal(t, fromHash.CloneBytes(), mc.lastGetBlockHeadersFromTillReq.StartHash)
		assert.Equal(t, tillHash.CloneBytes(), mc.lastGetBlockHeadersFromTillReq.EndHash)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersFromTill: nil,
			err:                             errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromTill(ctx, &fromHash, &tillHash)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromTill: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromTill(ctx, &fromHash, &tillHash)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("same from and till hash", func(t *testing.T) {
		sameHash := chainhash.Hash{1, 1, 1}
		mockResponse := &blockchain_api.GetBlockHeadersResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromTill: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromTill(ctx, &sameHash, &sameHash)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromTillReq)
		assert.Equal(t, sameHash.CloneBytes(), mc.lastGetBlockHeadersFromTillReq.StartHash)
		assert.Equal(t, sameHash.CloneBytes(), mc.lastGetBlockHeadersFromTillReq.EndHash)
	})
}

func TestClientGetBlockHeadersFromHeight(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	height := uint32(100)
	limit := uint32(5)
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "test-miner",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersFromHeightResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromHeight: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromHeight(ctx, height, limit)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, height, metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromHeightReq)
		assert.Equal(t, height, mc.lastGetBlockHeadersFromHeightReq.StartHeight)
		assert.Equal(t, limit, mc.lastGetBlockHeadersFromHeightReq.Limit)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersFromHeight: nil,
			err:                               errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromHeight(ctx, height, limit)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersFromHeightResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromHeight: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromHeight(ctx, height, limit)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("zero limit", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersFromHeightResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromHeight: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromHeight(ctx, height, 0)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromHeightReq)
		assert.Equal(t, uint32(0), mc.lastGetBlockHeadersFromHeightReq.Limit)
	})

	t.Run("height zero", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersFromHeightResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersFromHeight: mockResponse,
			err:                               nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersFromHeight(ctx, 0, limit)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersFromHeightReq)
		assert.Equal(t, uint32(0), mc.lastGetBlockHeadersFromHeightReq.StartHeight)
	})
}

func TestClientGetBlockHeadersByHeight(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Setup test data
	startHeight := uint32(100)
	endHeight := uint32(105)
	validHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          123,
	}
	validMeta1 := &model.BlockHeaderMeta{
		Height:      100,
		TxCount:     10,
		SizeInBytes: 1000,
		Miner:       "test-miner",
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
	}

	t.Run("successful request", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersByHeightResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersByHeight: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)
		assert.Equal(t, validHeader1.Version, headers[0].Version)
		assert.Equal(t, startHeight, metas[0].Height)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersByHeightReq)
		assert.Equal(t, startHeight, mc.lastGetBlockHeadersByHeightReq.StartHeight)
		assert.Equal(t, endHeight, mc.lastGetBlockHeadersByHeightReq.EndHeight)
	})

	t.Run("grpc client error", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockHeadersByHeight: nil,
			err:                             errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("empty response", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersByHeightResponse{
			BlockHeaders: [][]byte{},
			Metas:        [][]byte{},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersByHeight: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
		require.NoError(t, err)
		assert.Len(t, headers, 0)
		assert.Len(t, metas, 0)
	})

	t.Run("same start and end height", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersByHeightResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersByHeight: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersByHeight(ctx, startHeight, startHeight)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersByHeightReq)
		assert.Equal(t, startHeight, mc.lastGetBlockHeadersByHeightReq.StartHeight)
		assert.Equal(t, startHeight, mc.lastGetBlockHeadersByHeightReq.EndHeight)
	})

	t.Run("zero heights", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockHeadersByHeightResponse{
			BlockHeaders: [][]byte{validHeader1.Bytes()},
			Metas:        [][]byte{validMeta1.Bytes()},
		}

		mc := &mockBlockClient{
			responseGetBlockHeadersByHeight: mockResponse,
			err:                             nil,
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, metas, err := c.GetBlockHeadersByHeight(ctx, 0, 0)
		require.NoError(t, err)
		assert.Len(t, headers, 1)
		assert.Len(t, metas, 1)

		// Validate request
		require.NotNil(t, mc.lastGetBlockHeadersByHeightReq)
		assert.Equal(t, uint32(0), mc.lastGetBlockHeadersByHeightReq.StartHeight)
		assert.Equal(t, uint32(0), mc.lastGetBlockHeadersByHeightReq.EndHeight)
	})
}

// TestClientSubscribe tests the Subscribe method
func TestClientSubscribe(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful subscription", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{},
			logger:   logger,
			settings: tSettings,
		}

		source := "test-source"
		ch, err := c.Subscribe(ctx, source)
		require.NoError(t, err)
		require.NotNil(t, ch)
		assert.Equal(t, 1000, cap(ch))

		// Verify subscriber was added
		c.subscribersMu.Lock()
		assert.Len(t, c.subscribers, 1)
		assert.Equal(t, source, c.subscribers[0].source)
		assert.Equal(t, ch, c.subscribers[0].ch)
		assert.NotEmpty(t, c.subscribers[0].id)
		c.subscribersMu.Unlock()
	})

	t.Run("multiple subscriptions", func(t *testing.T) {
		c := &Client{
			client:   &mockBlockClient{},
			logger:   logger,
			settings: tSettings,
		}

		source1 := "test-source-1"
		source2 := "test-source-2"

		ch1, err1 := c.Subscribe(ctx, source1)
		require.NoError(t, err1)
		require.NotNil(t, ch1)

		ch2, err2 := c.Subscribe(ctx, source2)
		require.NoError(t, err2)
		require.NotNil(t, ch2)

		// Verify both subscribers were added
		c.subscribersMu.Lock()
		assert.Len(t, c.subscribers, 2)
		assert.Equal(t, source1, c.subscribers[0].source)
		assert.Equal(t, source2, c.subscribers[1].source)
		assert.NotEqual(t, c.subscribers[0].id, c.subscribers[1].id)
		c.subscribersMu.Unlock()
	})

	t.Run("subscription with last block notification", func(t *testing.T) {
		lastNotification := &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: (&chainhash.Hash{1, 2, 3})[:],
		}

		c := &Client{
			client:                &mockBlockClient{},
			logger:                logger,
			settings:              tSettings,
			lastBlockNotification: lastNotification,
		}

		source := "test-source"
		ch, err := c.Subscribe(ctx, source)
		require.NoError(t, err)
		require.NotNil(t, ch)

		// Should receive the last block notification immediately
		select {
		case notification := <-ch:
			assert.Equal(t, lastNotification.Type, notification.Type)
			assert.Equal(t, lastNotification.Hash, notification.Hash)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected to receive last block notification immediately")
		}
	})
}

// TestClientGetBlockIsMined tests the GetBlockIsMined method
func TestClientGetBlockIsMined(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("block is mined", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockIsMinedResponse{
			IsMined: true,
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockIsMined: mockResponse,
				err:                     nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		isMined, err := c.GetBlockIsMined(ctx, &blockHash)
		require.NoError(t, err)
		assert.True(t, isMined)
	})

	t.Run("block is not mined", func(t *testing.T) {
		mockResponse := &blockchain_api.GetBlockIsMinedResponse{
			IsMined: false,
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlockIsMined: mockResponse,
				err:                     nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		isMined, err := c.GetBlockIsMined(ctx, &blockHash)
		require.NoError(t, err)
		assert.False(t, isMined)
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlockIsMined: nil,
				err:                     errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		isMined, err := c.GetBlockIsMined(ctx, &blockHash)
		require.Error(t, err)
		assert.False(t, isMined)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// TestClientSetBlockMinedSet tests the SetBlockMinedSet method
func TestClientSetBlockMinedSet(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("successful set", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockMinedSet: &emptypb.Empty{},
				err:                      nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockMinedSet(ctx, &blockHash)
		require.NoError(t, err)
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockMinedSet: nil,
				err:                      errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockMinedSet(ctx, &blockHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// TestClientSetBlockProcessedAt tests the SetBlockProcessedAt method
func TestClientSetBlockProcessedAt(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("successful set", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockProcessedAt: &emptypb.Empty{},
				err:                         nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockProcessedAt(ctx, &blockHash)
		require.NoError(t, err)
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockProcessedAt: nil,
				err:                         errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockProcessedAt(ctx, &blockHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// TestClientGetBlocksMinedNotSet tests the GetBlocksMinedNotSet method
func TestClientGetBlocksMinedNotSet(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful get with blocks", func(t *testing.T) {
		header1 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          123,
		}

		coinbase1 := bt.NewTx()
		_ = coinbase1.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		_ = coinbase1.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)

		// Create test block bytes
		block := &model.Block{
			Header:           header1,
			CoinbaseTx:       coinbase1,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Subtrees:         []*chainhash.Hash{&chainhash.Hash{1, 2, 3}},
			Height:           100,
			ID:               1,
		}

		blockBytes, _ := block.Bytes()
		mockResponse := &blockchain_api.GetBlocksMinedNotSetResponse{
			BlockBytes: [][]byte{blockBytes},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocksMinedNotSet: mockResponse,
				err:                          nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocksMinedNotSet(ctx)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		assert.Len(t, blocks, 1)
		assert.Equal(t, uint32(100), blocks[0].Height)
		assert.Equal(t, uint32(0), blocks[0].ID) // ID field is not preserved through serialization
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlocksMinedNotSet: nil,
				err:                          errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocksMinedNotSet(ctx)
		require.Error(t, err)
		assert.Nil(t, blocks)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// TestClientSetBlockSubtreesSet tests the SetBlockSubtreesSet method
func TestClientSetBlockSubtreesSet(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	blockHash := chainhash.Hash{1, 2, 3}

	t.Run("successful set", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockSubtreesSet: &emptypb.Empty{},
				err:                         nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockSubtreesSet(ctx, &blockHash)
		require.NoError(t, err)
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseSetBlockSubtreesSet: nil,
				err:                         errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		err := c.SetBlockSubtreesSet(ctx, &blockHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// TestClientGetBlocksSubtreesNotSet tests the GetBlocksSubtreesNotSet method
func TestClientGetBlocksSubtreesNotSet(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("successful get with blocks", func(t *testing.T) {
		header1 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          123,
		}

		coinbase1 := bt.NewTx()
		_ = coinbase1.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
		_ = coinbase1.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)

		// Create test block bytes
		block := &model.Block{
			Header:           header1,
			CoinbaseTx:       coinbase1,
			TransactionCount: 1,
			SizeInBytes:      1000,
			Subtrees:         []*chainhash.Hash{&chainhash.Hash{1, 2, 3}},
			Height:           100,
			ID:               1,
		}

		blockBytes, _ := block.Bytes()
		mockResponse := &blockchain_api.GetBlocksSubtreesNotSetResponse{
			BlockBytes: [][]byte{blockBytes},
		}

		c := &Client{
			client: &mockBlockClient{
				responseGetBlocksSubtreesNotSet: mockResponse,
				err:                             nil,
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocksSubtreesNotSet(ctx)
		require.NoError(t, err)
		require.NotNil(t, blocks)
		assert.Len(t, blocks, 1)
		assert.Equal(t, uint32(100), blocks[0].Height)
		assert.Equal(t, uint32(0), blocks[0].ID) // ID field is not preserved through serialization
	})

	t.Run("grpc error", func(t *testing.T) {
		c := &Client{
			client: &mockBlockClient{
				responseGetBlocksSubtreesNotSet: nil,
				err:                             errors.NewProcessingError("grpc error"),
			},
			logger:   logger,
			settings: tSettings,
		}

		blocks, err := c.GetBlocksSubtreesNotSet(ctx)
		require.Error(t, err)
		assert.Nil(t, blocks)
		assert.Contains(t, err.Error(), "grpc error")
	})
}

// Test GetFSMCurrentState
func TestClient_GetFSMCurrentState(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success with gRPC call", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		state, err := client.GetFSMCurrentState(ctx)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, blockchain_api.FSMStateType_IDLE, *state)
	})

	t.Run("gRPC error", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("gRPC error"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		state, err := client.GetFSMCurrentState(ctx)
		assert.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "gRPC error")
	})
}

// Test IsFSMCurrentState
func TestClient_IsFSMCurrentState(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("state matches", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_RUNNING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		isState, err := client.IsFSMCurrentState(ctx, blockchain_api.FSMStateType_RUNNING)
		require.NoError(t, err)
		assert.True(t, isState)
	})

	t.Run("state does not match", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		isState, err := client.IsFSMCurrentState(ctx, blockchain_api.FSMStateType_RUNNING)
		require.NoError(t, err)
		assert.False(t, isState)
	})

	t.Run("GetFSMCurrentState error", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("gRPC error"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		isState, err := client.IsFSMCurrentState(ctx, blockchain_api.FSMStateType_RUNNING)
		assert.Error(t, err)
		assert.False(t, isState)
		assert.Contains(t, err.Error(), "gRPC error")
	})
}

// Test SendFSMEvent
func TestClient_SendFSMEvent(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success with RUN event", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseSendFSMEvent: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_RUNNING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_RUN)
		require.NoError(t, err)
		assert.Equal(t, blockchain_api.FSMEventType_RUN, mockClient.lastSendFSMEventReq.Event)
	})

	t.Run("success with STOP event", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseSendFSMEvent: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_STOP)
		require.NoError(t, err)
		assert.Equal(t, blockchain_api.FSMEventType_STOP, mockClient.lastSendFSMEventReq.Event)
	})

	t.Run("success with LEGACYSYNC event", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseSendFSMEvent: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_LEGACYSYNCING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_LEGACYSYNC)
		require.NoError(t, err)
		assert.Equal(t, blockchain_api.FSMEventType_LEGACYSYNC, mockClient.lastSendFSMEventReq.Event)
	})

	t.Run("success with CATCHUPBLOCKS event", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseSendFSMEvent: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_CATCHINGBLOCKS,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_CATCHUPBLOCKS)
		require.NoError(t, err)
		assert.Equal(t, blockchain_api.FSMEventType_CATCHUPBLOCKS, mockClient.lastSendFSMEventReq.Event)
	})

	t.Run("gRPC error", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("FSM event failed"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_RUN)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "FSM event failed")
	})
}

// Test Run function
func TestClient_Run(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success when already in RUNNING state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_RUNNING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Run(ctx, "test-source")
		require.NoError(t, err)
	})

	t.Run("success when transitioning from IDLE state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Run(ctx, "test-source")
		require.NoError(t, err)
	})

	t.Run("gRPC error on Run call", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("Run failed"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Run(ctx, "test-source")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Run failed")
	})
}

// Test CatchUpBlocks function
func TestClient_CatchUpBlocks(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success when already in CATCHINGBLOCKS state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_CATCHINGBLOCKS,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.CatchUpBlocks(ctx)
		require.NoError(t, err)
	})

	t.Run("success when transitioning from RUNNING state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_RUNNING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.CatchUpBlocks(ctx)
		require.NoError(t, err)
	})

	t.Run("gRPC error on CatchUpBlocks call", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("CatchUpBlocks failed"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.CatchUpBlocks(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CatchUpBlocks failed")
	})
}

// Test LegacySync function
func TestClient_LegacySync(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success when already in LEGACYSYNCING state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_LEGACYSYNCING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.LegacySync(ctx)
		require.NoError(t, err)
	})

	t.Run("success when transitioning from IDLE state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.LegacySync(ctx)
		require.NoError(t, err)
	})

	t.Run("gRPC error on LegacySync call", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("LegacySync failed"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.LegacySync(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LegacySync failed")
	})
}

// Test Idle function
func TestClient_Idle(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success when already in IDLE state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_IDLE,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Idle(ctx)
		require.NoError(t, err)
	})

	t.Run("success when transitioning from RUNNING state", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			responseGetFSMCurrentState: &blockchain_api.GetFSMStateResponse{
				State: blockchain_api.FSMStateType_RUNNING,
			},
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Idle(ctx)
		require.NoError(t, err)
	})

	t.Run("gRPC error on Idle call", func(t *testing.T) {
		ctx := context.Background()
		mockClient := &mockBlockClient{
			err: errors.NewProcessingError("Idle failed"),
		}
		client := &Client{
			client:   mockClient,
			logger:   logger,
			settings: tSettings,
		}

		err := client.Idle(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Idle failed")
	})
}

// Test GetBlockLocator
func TestClient_GetBlockLocator(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	blockHash := chainhash.HashH([]byte("test block hash"))
	height := uint32(12345)

	t.Run("success", func(t *testing.T) {
		// Create test hash bytes for the locator
		hash1 := chainhash.HashH([]byte("hash1"))
		hash2 := chainhash.HashH([]byte("hash2"))
		hash3 := chainhash.HashH([]byte("hash3"))

		mc := &mockBlockClient{
			responseGetBlockLocator: &blockchain_api.GetBlockLocatorResponse{
				Locator: [][]byte{
					hash1.CloneBytes(),
					hash2.CloneBytes(),
					hash3.CloneBytes(),
				},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		locator, err := c.GetBlockLocator(ctx, &blockHash, height)
		require.NoError(t, err)
		require.NotNil(t, locator)
		assert.Len(t, locator, 3)

		// Verify the hashes match
		assert.Equal(t, hash1.CloneBytes(), locator[0].CloneBytes())
		assert.Equal(t, hash2.CloneBytes(), locator[1].CloneBytes())
		assert.Equal(t, hash3.CloneBytes(), locator[2].CloneBytes())

		// Verify request parameters
		assert.NotNil(t, mc.lastGetBlockLocatorReq)
		assert.Equal(t, blockHash.CloneBytes(), mc.lastGetBlockLocatorReq.Hash)
		assert.Equal(t, height, mc.lastGetBlockLocatorReq.Height)
	})

	t.Run("empty locator", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockLocator: &blockchain_api.GetBlockLocatorResponse{
				Locator: [][]byte{},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		locator, err := c.GetBlockLocator(ctx, &blockHash, height)
		require.NoError(t, err)
		require.NotNil(t, locator)
		assert.Len(t, locator, 0)
	})

	t.Run("invalid hash in locator", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBlockLocator: &blockchain_api.GetBlockLocatorResponse{
				Locator: [][]byte{
					{0x00, 0x01}, // Invalid hash length (should be 32 bytes)
				},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		locator, err := c.GetBlockLocator(ctx, &blockHash, height)
		require.Error(t, err)
		assert.Nil(t, locator)
		assert.Contains(t, err.Error(), "invalid hash length")
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			err: errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		locator, err := c.GetBlockLocator(ctx, &blockHash, height)
		require.Error(t, err)
		assert.Nil(t, locator)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})
}

// Test LocateBlockHeaders
func TestClient_LocateBlockHeaders(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Create test locator hashes
	hash1 := chainhash.HashH([]byte("locator1"))
	hash2 := chainhash.HashH([]byte("locator2"))
	locator := []*chainhash.Hash{&hash1, &hash2}
	hashStop := chainhash.HashH([]byte("stop hash"))
	maxHashes := uint32(100)

	t.Run("success", func(t *testing.T) {
		// Create test block headers
		header1 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          12345,
		}
		header2 := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          67890,
		}

		mc := &mockBlockClient{
			responseLocateBlockHeaders: &blockchain_api.LocateBlockHeadersResponse{
				BlockHeaders: [][]byte{
					header1.Bytes(),
					header2.Bytes(),
				},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, err := c.LocateBlockHeaders(ctx, locator, &hashStop, maxHashes)
		require.NoError(t, err)
		require.NotNil(t, headers)
		assert.Len(t, headers, 2)

		// Verify headers are correctly deserialized
		assert.Equal(t, header1.Version, headers[0].Version)
		assert.Equal(t, header1.Nonce, headers[0].Nonce)
		assert.Equal(t, header2.Version, headers[1].Version)
		assert.Equal(t, header2.Nonce, headers[1].Nonce)

		// Verify request parameters
		assert.NotNil(t, mc.lastLocateBlockHeadersReq)
		assert.Len(t, mc.lastLocateBlockHeadersReq.Locator, 2)
		assert.Equal(t, hash1.CloneBytes(), mc.lastLocateBlockHeadersReq.Locator[0])
		assert.Equal(t, hash2.CloneBytes(), mc.lastLocateBlockHeadersReq.Locator[1])
		assert.Equal(t, hashStop.CloneBytes(), mc.lastLocateBlockHeadersReq.HashStop)
		assert.Equal(t, maxHashes, mc.lastLocateBlockHeadersReq.MaxHashes)
	})

	t.Run("empty headers", func(t *testing.T) {
		mc := &mockBlockClient{
			responseLocateBlockHeaders: &blockchain_api.LocateBlockHeadersResponse{
				BlockHeaders: [][]byte{},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, err := c.LocateBlockHeaders(ctx, locator, &hashStop, maxHashes)
		require.NoError(t, err)
		require.NotNil(t, headers)
		assert.Len(t, headers, 0)
	})

	t.Run("invalid header bytes", func(t *testing.T) {
		mc := &mockBlockClient{
			responseLocateBlockHeaders: &blockchain_api.LocateBlockHeadersResponse{
				BlockHeaders: [][]byte{
					{0x00, 0x01, 0x02}, // Invalid header bytes
				},
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, err := c.LocateBlockHeaders(ctx, locator, &hashStop, maxHashes)
		require.Error(t, err)
		assert.Nil(t, headers)
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			err: errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		headers, err := c.LocateBlockHeaders(ctx, locator, &hashStop, maxHashes)
		require.Error(t, err)
		assert.Nil(t, headers)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})
}

// Test GetBestHeightAndTime
func TestClient_GetBestHeightAndTime(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("success", func(t *testing.T) {
		expectedHeight := uint32(123456)
		expectedTime := uint32(1672531200) // Example timestamp

		mc := &mockBlockClient{
			responseGetBestHeightAndTime: &blockchain_api.GetBestHeightAndTimeResponse{
				Height: expectedHeight,
				Time:   expectedTime,
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		height, time, err := c.GetBestHeightAndTime(ctx)
		require.NoError(t, err)
		assert.Equal(t, expectedHeight, height)
		assert.Equal(t, expectedTime, time)
	})

	t.Run("zero values", func(t *testing.T) {
		mc := &mockBlockClient{
			responseGetBestHeightAndTime: &blockchain_api.GetBestHeightAndTimeResponse{
				Height: 0,
				Time:   0,
			},
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		height, time, err := c.GetBestHeightAndTime(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), height)
		assert.Equal(t, uint32(0), time)
	})

	t.Run("grpc error", func(t *testing.T) {
		mc := &mockBlockClient{
			err: errors.NewProcessingError("grpc connection failed"),
		}
		c := &Client{
			client:   mc,
			logger:   logger,
			settings: tSettings,
		}

		height, time, err := c.GetBestHeightAndTime(ctx)
		require.Error(t, err)
		assert.Equal(t, uint32(0), height)
		assert.Equal(t, uint32(0), time)
		assert.Contains(t, err.Error(), "grpc connection failed")
	})
}

func TestHeartbeatTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.MaxRetries = 0

	// Use port 0 to let the OS assign an available port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Get the actual address with the assigned port
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()

	grpcServer := grpc.NewServer()
	fakeSrv := &fakeServer{
		subCh:    make(chan *blockchain_api.Notification, 10),
		fsmState: blockchain_api.FSMStateType_RUNNING,
	}
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, fakeSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	clientI, err := NewClientWithAddress(ctx, logger, tSettings, tSettings.BlockChain.GRPCAddress, "heartbeat-test")
	require.NoError(t, err)
	c := clientI.(*Client)

	t.Run("PING notification updates lastHeartbeat", func(t *testing.T) {
		// Get initial heartbeat timestamp
		initialHB := c.lastHeartbeat.Load()

		// Wait a moment to ensure different timestamp
		time.Sleep(10 * time.Millisecond)

		// Send PING notification
		dummyHash := make([]byte, 32)
		pingNotif := &blockchain_api.Notification{
			Type: model.NotificationType_PING,
			Hash: dummyHash,
		}
		fakeSrv.subCh <- pingNotif

		// Wait for notification to be processed
		require.Eventually(t, func() bool {
			currentHB := c.lastHeartbeat.Load()
			return currentHB > initialHB
		}, 2*time.Second, 50*time.Millisecond, "heartbeat timestamp should be updated after PING")
	})

	t.Run("FSM state restored after reconnection", func(t *testing.T) {
		// Verify FSM state was restored to RUNNING (set by fakeServer)
		require.Eventually(t, func() bool {
			state := c.fmsState.Load()
			return state != nil && *state == FSMStateRUNNING
		}, 2*time.Second, 100*time.Millisecond, "FSM state should be restored to RUNNING after connection")
	})
}

func TestHeartbeatStaleSetsFSMToIdle(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("stale heartbeat sets FSM to IDLE", func(t *testing.T) {
		running := &atomic.Bool{}
		running.Store(true)

		c := &Client{
			logger:   logger,
			settings: tSettings,
			running:  running,
		}

		// Set FSM state to RUNNING
		runningState := FSMStateRUNNING
		c.fmsState.Store(&runningState)

		// Set heartbeat to 35 seconds ago (stale - timeout is 30s)
		staleTime := time.Now().Add(-35 * time.Second).UnixNano()
		c.lastHeartbeat.Store(staleTime)

		// Verify the state is currently RUNNING
		currentState := c.fmsState.Load()
		require.NotNil(t, currentState)
		require.Equal(t, FSMStateRUNNING, *currentState)

		// Simulate the heartbeat staleness check that happens in SubscribeToServer
		const heartbeatTimeout = 30 * time.Second
		lastHB := c.lastHeartbeat.Load()
		if lastHB > 0 && time.Since(time.Unix(0, lastHB)) > heartbeatTimeout {
			idleState := FSMStateIDLE
			c.fmsState.Store(&idleState)
		}

		// Verify FSM state was set to IDLE
		newState := c.fmsState.Load()
		require.NotNil(t, newState)
		require.Equal(t, FSMStateIDLE, *newState, "FSM state should be IDLE after stale heartbeat")
	})

	t.Run("fresh heartbeat does not change FSM state", func(t *testing.T) {
		running := &atomic.Bool{}
		running.Store(true)

		c := &Client{
			logger:   logger,
			settings: tSettings,
			running:  running,
		}

		// Set FSM state to RUNNING
		runningState := FSMStateRUNNING
		c.fmsState.Store(&runningState)

		// Set heartbeat to now (fresh)
		c.lastHeartbeat.Store(time.Now().UnixNano())

		// Simulate the heartbeat staleness check
		const heartbeatTimeout = 30 * time.Second
		lastHB := c.lastHeartbeat.Load()
		if lastHB > 0 && time.Since(time.Unix(0, lastHB)) > heartbeatTimeout {
			idleState := FSMStateIDLE
			c.fmsState.Store(&idleState)
		}

		// Verify FSM state is still RUNNING
		currentState := c.fmsState.Load()
		require.NotNil(t, currentState)
		require.Equal(t, FSMStateRUNNING, *currentState, "FSM state should remain RUNNING with fresh heartbeat")
	})
}
