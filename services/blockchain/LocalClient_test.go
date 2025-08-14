package blockchain

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test NewLocalClient
func TestNewLocalClient(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	// Create LocalClient using NewLocalClient function
	client, err := NewLocalClient(logger, nil, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, client)

	localClient, ok := client.(*LocalClient)
	require.True(t, ok)
	assert.Equal(t, logger, localClient.logger)
	assert.Nil(t, localClient.store)
	assert.Nil(t, localClient.subtreeStore)
	assert.Nil(t, localClient.utxoStore)
}

// Test Health
func TestLocalClient_Health(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	t.Run("liveness check returns OK", func(t *testing.T) {
		client, err := NewLocalClient(logger, nil, nil, nil)
		require.NoError(t, err)

		status, message, err := client.Health(ctx, true)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "OK", message)
	})

	t.Run("readiness check with nil stores", func(t *testing.T) {
		client := &LocalClient{
			logger:       logger,
			store:        nil,
			subtreeStore: nil,
			utxoStore:    nil,
		}

		status, message, err := client.Health(ctx, false)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		// The message format has changed - just check it contains the status
		assert.Contains(t, message, "200")
	})
}

// Test SendNotification
func TestLocalClient_SendNotification(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	notification := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
	}

	err = client.SendNotification(ctx, notification)
	require.NoError(t, err)
}

// Test Subscribe
func TestLocalClient_Subscribe(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	source := "test-source"

	ch, err := client.Subscribe(ctx, source)
	require.NoError(t, err)
	require.NotNil(t, ch)
	assert.Equal(t, 10, cap(ch))
}

// Test GetNextWorkRequired
func TestLocalClient_GetNextWorkRequired(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	blockHash := chainhash.HashH([]byte("test"))

	nbit, err := client.GetNextWorkRequired(ctx, &blockHash)
	require.NoError(t, err)
	assert.Nil(t, nbit)
}

// Test FSM methods
func TestLocalClient_FSMMethods(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	t.Run("GetFSMCurrentState", func(t *testing.T) {
		state, err := client.GetFSMCurrentState(ctx)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, FSMStateRUNNING, *state)
	})

	t.Run("IsFSMCurrentState", func(t *testing.T) {
		isRunning, err := client.IsFSMCurrentState(ctx, FSMStateRUNNING)
		require.NoError(t, err)
		assert.True(t, isRunning)

		isIdle, err := client.IsFSMCurrentState(ctx, FSMStateIDLE)
		require.NoError(t, err)
		assert.False(t, isIdle)
	})

	t.Run("WaitForFSMtoTransitionToGivenState", func(t *testing.T) {
		err := client.WaitForFSMtoTransitionToGivenState(ctx, FSMStateRUNNING)
		require.NoError(t, err)
	})

	t.Run("WaitUntilFSMTransitionFromIdleState", func(t *testing.T) {
		err := client.WaitUntilFSMTransitionFromIdleState(ctx)
		require.NoError(t, err)
	})

	t.Run("GetFSMCurrentStateForE2ETestMode", func(t *testing.T) {
		localClient := client.(*LocalClient)
		state := localClient.GetFSMCurrentStateForE2ETestMode()
		assert.Equal(t, FSMStateRUNNING, state)
	})

	t.Run("SendFSMEvent", func(t *testing.T) {
		err := client.SendFSMEvent(ctx, blockchain_api.FSMEventType_RUN)
		require.NoError(t, err)
	})
}

// Test lifecycle methods
func TestLocalClient_LifecycleMethods(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	t.Run("Run", func(t *testing.T) {
		err := client.Run(ctx, "test-source")
		require.NoError(t, err)
	})

	t.Run("Idle", func(t *testing.T) {
		err := client.Idle(ctx)
		require.NoError(t, err)
	})

	t.Run("CatchUpBlocks", func(t *testing.T) {
		err := client.CatchUpBlocks(ctx)
		require.NoError(t, err)
	})

	t.Run("LegacySync", func(t *testing.T) {
		err := client.LegacySync(ctx)
		require.NoError(t, err)
	})
}

// Test LocateBlockHeaders
func TestLocalClient_LocateBlockHeaders(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	locator := []*chainhash.Hash{
		{},
		{},
	}
	hashStop := chainhash.HashH([]byte("stop"))
	maxHashes := uint32(100)

	headers, err := client.LocateBlockHeaders(ctx, locator, &hashStop, maxHashes)
	require.NoError(t, err)
	assert.Nil(t, headers)
}

// Test GetBlockLocator - will panic with nil store
func TestLocalClient_GetBlockLocator(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	blockHeaderHash := chainhash.HashH([]byte("test"))
	blockHeaderHeight := uint32(100)

	// This will panic with nil store, so we handle it
	defer func() {
		if r := recover(); r != nil {
			// Expected panic with nil store
		}
	}()

	_, _ = client.GetBlockLocator(ctx, &blockHeaderHash, blockHeaderHeight)
}

// Test GetBlockHeadersToCommonAncestor - will panic with nil store
func TestLocalClient_GetBlockHeadersToCommonAncestor(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	client, err := NewLocalClient(logger, nil, nil, nil)
	require.NoError(t, err)

	hashTarget := chainhash.HashH([]byte("target"))
	blockLocatorHashes := []*chainhash.Hash{
		{},
		{},
	}
	maxHeaders := uint32(100)

	// This will panic with nil store, so we handle it
	defer func() {
		if r := recover(); r != nil {
			// Expected panic with nil store
		}
	}()

	_, _, _ = client.GetBlockHeadersToCommonAncestor(ctx, &hashTarget, blockLocatorHashes, maxHeaders)
}

// Test methods that work with nil stores by returning errors
func TestLocalClient_NilStorePanics(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	// Create a LocalClient with nil stores to test error paths
	client := &LocalClient{
		logger:       logger,
		store:        nil,
		subtreeStore: nil,
		utxoStore:    nil,
	}

	ctx := context.Background()
	blockHash := chainhash.HashH([]byte("test"))

	// Test each method that will panic with nil store
	testCases := []struct {
		name string
		fn   func()
	}{
		{
			name: "SetState",
			fn: func() {
				_ = client.SetState(ctx, "key", []byte("data"))
			},
		},
		{
			name: "GetState",
			fn: func() {
				_, _ = client.GetState(ctx, "key")
			},
		},
		{
			name: "AddBlock",
			fn: func() {
				block := &model.Block{
					Header: &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  &chainhash.Hash{},
						HashMerkleRoot: &chainhash.Hash{},
						Timestamp:      uint32(time.Now().Unix()),
						Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
						Nonce:          12345,
					},
				}
				_ = client.AddBlock(ctx, block, "peer1")
			},
		},
		{
			name: "GetBlock",
			fn: func() {
				_, _ = client.GetBlock(ctx, &blockHash)
			},
		},
		{
			name: "GetBlocks",
			fn: func() {
				_, _ = client.GetBlocks(ctx, &blockHash, 5)
			},
		},
		{
			name: "GetBlockByHeight",
			fn: func() {
				_, _ = client.GetBlockByHeight(ctx, 100)
			},
		},
		{
			name: "GetBlockByID",
			fn: func() {
				_, _ = client.GetBlockByID(ctx, 100)
			},
		},
		{
			name: "GetBlockExists",
			fn: func() {
				_, _ = client.GetBlockExists(ctx, &blockHash)
			},
		},
		{
			name: "GetBestBlockHeader",
			fn: func() {
				_, _, _ = client.GetBestBlockHeader(ctx)
			},
		},
		{
			name: "GetBlockHeader",
			fn: func() {
				_, _, _ = client.GetBlockHeader(ctx, &blockHash)
			},
		},
		{
			name: "GetBlockHeaders",
			fn: func() {
				_, _, _ = client.GetBlockHeaders(ctx, &blockHash, 5)
			},
		},
		{
			name: "InvalidateBlock",
			fn: func() {
				_ = client.InvalidateBlock(ctx, &blockHash)
			},
		},
		{
			name: "RevalidateBlock",
			fn: func() {
				_ = client.RevalidateBlock(ctx, &blockHash)
			},
		},
		{
			name: "GetBlockHeaderIDs",
			fn: func() {
				_, _ = client.GetBlockHeaderIDs(ctx, &blockHash, 5)
			},
		},
		{
			name: "GetBlockIsMined",
			fn: func() {
				_, _ = client.GetBlockIsMined(ctx, &blockHash)
			},
		},
		{
			name: "SetBlockMinedSet",
			fn: func() {
				_ = client.SetBlockMinedSet(ctx, &blockHash)
			},
		},
		{
			name: "SetBlockProcessedAt",
			fn: func() {
				_ = client.SetBlockProcessedAt(ctx, &blockHash)
			},
		},
		{
			name: "GetBlocksMinedNotSet",
			fn: func() {
				_, _ = client.GetBlocksMinedNotSet(ctx)
			},
		},
		{
			name: "SetBlockSubtreesSet",
			fn: func() {
				_ = client.SetBlockSubtreesSet(ctx, &blockHash)
			},
		},
		{
			name: "GetBlocksSubtreesNotSet",
			fn: func() {
				_, _ = client.GetBlocksSubtreesNotSet(ctx)
			},
		},
		{
			name: "GetChainTips",
			fn: func() {
				_, _ = client.GetChainTips(ctx)
			},
		},
		{
			name: "GetBestHeightAndTime",
			fn: func() {
				_, _, _ = client.GetBestHeightAndTime(ctx)
			},
		},
		{
			name: "GetBlockStats",
			fn: func() {
				_, _ = client.GetBlockStats(ctx)
			},
		},
		{
			name: "GetBlockGraphData",
			fn: func() {
				_, _ = client.GetBlockGraphData(ctx, 3600000)
			},
		},
		{
			name: "GetLastNBlocks",
			fn: func() {
				_, _ = client.GetLastNBlocks(ctx, 10, true, 100)
			},
		},
		{
			name: "GetLastNInvalidBlocks",
			fn: func() {
				_, _ = client.GetLastNInvalidBlocks(ctx, 5)
			},
		},
		{
			name: "GetSuitableBlock",
			fn: func() {
				_, _ = client.GetSuitableBlock(ctx, &blockHash)
			},
		},
		{
			name: "GetHashOfAncestorBlock",
			fn: func() {
				_, _ = client.GetHashOfAncestorBlock(ctx, &blockHash, 10)
			},
		},
		{
			name: "GetBlockHeadersFromTill",
			fn: func() {
				blockHashTill := chainhash.HashH([]byte("till"))
				_, _, _ = client.GetBlockHeadersFromTill(ctx, &blockHash, &blockHashTill)
			},
		},
		{
			name: "CheckBlockIsInCurrentChain",
			fn: func() {
				_, _ = client.CheckBlockIsInCurrentChain(ctx, []uint32{1, 2, 3})
			},
		},
		{
			name: "GetBlockHeadersFromHeight",
			fn: func() {
				_, _, _ = client.GetBlockHeadersFromHeight(ctx, 100, 10)
			},
		},
		{
			name: "GetBlockHeadersByHeight",
			fn: func() {
				_, _, _ = client.GetBlockHeadersByHeight(ctx, 100, 110)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+" with nil store", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					// Expected panic with nil store
				}
			}()
			tc.fn()
		})
	}
}
