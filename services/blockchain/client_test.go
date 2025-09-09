package blockchain

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	tSettings.BlockChain.GRPCAddress = "localhost:50052"
	tSettings.BlockChain.MaxRetries = 0

	lis, err := net.Listen("tcp", tSettings.BlockChain.GRPCAddress)
	require.NoError(t, err)

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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          123,
	}

	validCoinbase := bt.NewTx()
	_ = validCoinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	_ = validCoinbase.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)

	subtreeHash1 := &chainhash.Hash{1, 2, 3}
	subtreeHash2 := &chainhash.Hash{4, 5, 6}
	subtreeHashes := []*chainhash.Hash{subtreeHash1, subtreeHash2}

	// Create a valid block and convert to bytes
	validBlock, _ := model.NewBlock(validHeader, validCoinbase, subtreeHashes, 1, 1000, 100, 1, tSettings)
	validBlockBytes, _ := validBlock.Bytes()

	// Create another valid block with different data
	validHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHash,
		HashMerkleRoot: &chainhash.Hash{7, 8, 9},
		Timestamp:      uint32(time.Now().Unix()) + 600,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          456,
	}
	validBlock2, _ := model.NewBlock(validHeader2, validCoinbase, subtreeHashes, 2, 2000, 101, 2, tSettings)
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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
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
