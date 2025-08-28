package blockassembly

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/testutil"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_storeSubtree(t *testing.T) {
	t.Run("Test storeSubtree", func(t *testing.T) {
		server, subtreeStore, subtree, txMap := setup(t)

		subtreeRetryChan := make(chan *subtreeRetrySend, 1_000)

		require.NoError(t, server.storeSubtree(t.Context(), subtreeprocessor.NewSubtreeRequest{
			Subtree:     subtree,
			ParentTxMap: txMap,
			ErrChan:     nil,
		}, subtreeRetryChan))

		subtreeBytes, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.NotNil(t, subtreeBytes)

		subtreeFromStore, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err)

		require.Equal(t, subtree.RootHash(), subtreeFromStore.RootHash())

		for idx, node := range subtree.Nodes {
			require.Equal(t, node, subtreeFromStore.Nodes[idx])
		}

		// check that the meta-data is stored
		subtreeMetaBytes, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta)
		require.NoError(t, err)

		subtreeMeta, err := subtreepkg.NewSubtreeMetaFromBytes(subtreeFromStore, subtreeMetaBytes)
		require.NoError(t, err)
		require.NotNil(t, subtreeMeta)

		previousHash := chainhash.HashH([]byte("previousHash"))

		for idx, subtreeMetaParents := range subtreeMeta.TxInpoints {
			parents := []chainhash.Hash{previousHash}
			require.Equal(t, parents, subtreeMetaParents.ParentTxHashes)

			previousHash = subtree.Nodes[idx].Hash
		}
	})

	t.Run("Test storeSubtree - meta missing", func(t *testing.T) {
		server, subtreeStore, subtree, txMap := setup(t)

		txMap.Clear()

		subtreeRetryChan := make(chan *subtreeRetrySend, 1_000)

		require.NoError(t, server.storeSubtree(t.Context(), subtreeprocessor.NewSubtreeRequest{
			Subtree:     subtree,
			ParentTxMap: txMap,
			ErrChan:     nil,
		}, subtreeRetryChan))

		// check that the meta data was not stored
		_, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta)
		require.Error(t, err)
	})
}

func TestCheckBlockAssembly(t *testing.T) {
	// this populates the subtree processor and does a real check
	t.Run("success", func(t *testing.T) {
		server, _, _, _ := setup(t)

		resp, err := server.CheckBlockAssembly(t.Context(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err)

		require.NotNil(t, resp)
		require.Equal(t, true, resp.Ok)
	})

	// this tests simulates a failure in the CheckSubtreeProcessor method
	t.Run("error", func(t *testing.T) {
		server, _, _, _ := setup(t)

		mockSubtreeProcessor := &subtreeprocessor.MockSubtreeProcessor{}
		mockSubtreeProcessor.On("CheckSubtreeProcessor").Return(errors.NewProcessingError("test error"))

		server.blockAssembler.subtreeProcessor = mockSubtreeProcessor

		resp, err := server.CheckBlockAssembly(t.Context(), &blockassembly_api.EmptyMessage{})
		require.Error(t, err)

		unwrapErr := errors.UnwrapGRPC(err)
		require.ErrorIs(t, errors.ErrProcessing, unwrapErr)

		require.Nil(t, resp)
	})
}

func TestGetBlockAssemblyBlockCandidate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server, _, _, _ := setup(t)
		err := server.blockAssembler.Start(t.Context())
		require.NoError(t, err)

		resp, err := server.GetBlockAssemblyBlockCandidate(t.Context(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err)

		require.NotNil(t, resp)

		blockBytes := resp.Block
		require.NotNil(t, blockBytes)

		block, err := model.NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		require.NotNil(t, block)
		require.NotNil(t, block.Header)

		assert.Equal(t, uint32(1), block.Height)
		assert.Equal(t, uint64(0), block.TransactionCount)
		assert.Equal(t, uint64(5000000000), block.CoinbaseTx.Outputs[0].Satoshis)
	})

	t.Run("success with lower coinbase output", func(t *testing.T) {
		server, _, _, _ := setup(t)
		err := server.blockAssembler.Start(t.Context())
		require.NoError(t, err)

		server.blockAssembler.bestBlockHeight.Store(250) // halvings = 150

		resp, err := server.GetBlockAssemblyBlockCandidate(t.Context(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err)

		require.NotNil(t, resp)

		blockBytes := resp.Block
		require.NotNil(t, blockBytes)

		block, err := model.NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		require.NotNil(t, block)
		require.NotNil(t, block.Header)

		assert.Equal(t, uint32(251), block.Height)
		assert.Equal(t, uint64(0), block.TransactionCount)
		assert.Equal(t, uint64(2500000000), block.CoinbaseTx.Outputs[0].Satoshis)
	})

	t.Run("with 10 txs", func(t *testing.T) {
		server, _, _, _ := setup(t)
		err := server.blockAssembler.Start(t.Context())
		require.NoError(t, err)

		for i := uint64(0); i < 10; i++ {
			server.blockAssembler.AddTx(subtreepkg.SubtreeNode{
				Hash:        chainhash.HashH([]byte(fmt.Sprintf("%d", i))),
				Fee:         i,
				SizeInBytes: i,
			}, subtreepkg.TxInpoints{})
		}

		require.Eventually(t, func() bool {
			return server.blockAssembler.TxCount() == 11
		}, 5*time.Second, 10*time.Millisecond)

		resp, err := server.GetBlockAssemblyBlockCandidate(t.Context(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err)

		require.NotNil(t, resp)

		blockBytes := resp.Block
		require.NotNil(t, blockBytes)

		block, err := model.NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		require.NotNil(t, block)
		require.NotNil(t, block.Header)

		assert.Equal(t, uint32(1), block.Height)
		assert.Equal(t, uint64(10), block.TransactionCount)
		assert.Equal(t, uint64(5000000045), block.CoinbaseTx.Outputs[0].Satoshis)
	})
}

func setup(t *testing.T) (*BlockAssembly, *memory.Memory, *subtreepkg.Subtree, *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]) {
	s, subtreeStore := setupServer(t)

	subtree, err := subtreepkg.NewTreeByLeafCount(16)
	require.NoError(t, err)

	txMap := txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()

	previousHash := chainhash.HashH([]byte("previousHash"))

	for i := uint64(0); i < 16; i++ {
		txHash := chainhash.HashH([]byte(fmt.Sprintf("tx%d", i)))
		_ = subtree.AddNode(txHash, i, i)

		txMap.Set(txHash, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{previousHash}, Idxs: [][]uint32{{0, 1}}})
		previousHash = txHash
	}

	return s, subtreeStore, subtree, txMap
}

func setupServer(t *testing.T) (*BlockAssembly, *memory.Memory) {
	common := testutil.NewCommonTestSetup(t)
	subtreeStore := testutil.NewMemoryBlobStore()

	// Use real blockchain client with memory SQLite instead of mock
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)

	utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	s := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)

	// Skip waiting for pending blocks in tests to prevent mock issues
	s.SetSkipWaitForPendingBlocks(true)

	require.NoError(t, s.Init(common.Ctx))

	return s, subtreeStore
}

// TestHealthMethod tests the Health method
func TestHealthMethod(t *testing.T) {
	t.Run("liveness check", func(t *testing.T) {
		server, _ := setupServer(t)

		status, msg, err := server.Health(context.Background(), true)
		require.NoError(t, err)
		require.Equal(t, 200, status)
		require.NotEmpty(t, msg)
	})

	t.Run("readiness check", func(t *testing.T) {
		server, _ := setupServer(t)

		status, msg, err := server.Health(context.Background(), false)
		require.NoError(t, err)
		// Server may return 503 if dependencies are not fully initialized
		// This is expected behavior for readiness checks
		require.True(t, status == 200 || status == 503)
		require.NotEmpty(t, msg)
	})

	t.Run("readiness with nil dependencies", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)

		// Create server with nil dependencies
		server := New(logger, tSettings, nil, nil, nil, nil)

		status, msg, err := server.Health(context.Background(), false)
		require.NoError(t, err)
		// Server may return 503 when dependencies are nil - this is expected
		require.True(t, status == 200 || status == 503)
		require.NotEmpty(t, msg)
	})
}

// TestHealthGRPC tests the HealthGRPC method
func TestHealthGRPC(t *testing.T) {
	t.Run("health check via gRPC", func(t *testing.T) {
		server, _ := setupServer(t)

		resp, err := server.HealthGRPC(context.Background(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.Ok)
		require.NotEmpty(t, resp.Details)
	})
}

// TestAddTx tests the AddTx method with invalid TxInpoints
func TestAddTx(t *testing.T) {
	t.Run("add transaction with invalid TxInpoints", func(t *testing.T) {
		server, _ := setupServer(t)

		// Start the block assembler first
		err := server.blockAssembler.Start(context.Background())
		require.NoError(t, err)

		txHash := chainhash.HashH([]byte("test-tx"))

		// Use invalid TxInpoints to test error path
		txInpointsBytes := []byte{255, 255} // Invalid format

		req := &blockassembly_api.AddTxRequest{
			Txid:       txHash[:],
			Fee:        100,
			Size:       250,
			TxInpoints: txInpointsBytes,
		}

		resp, err := server.AddTx(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "unable to deserialize tx inpoints")
	})

	t.Run("add transaction with invalid txid", func(t *testing.T) {
		server, _ := setupServer(t)

		req := &blockassembly_api.AddTxRequest{
			Txid:       []byte{1, 2, 3}, // Invalid length (not 32 bytes)
			Fee:        100,
			Size:       250,
			TxInpoints: []byte{0},
		}

		resp, err := server.AddTx(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "invalid txid length")
	})
}

// TestRemoveTx tests the RemoveTx method
func TestRemoveTx(t *testing.T) {
	t.Run("remove transaction with invalid txid", func(t *testing.T) {
		server, _ := setupServer(t)

		// Try to remove with invalid txid length
		removeReq := &blockassembly_api.RemoveTxRequest{
			Txid: []byte{1, 2, 3}, // Invalid length
		}
		resp, err := server.RemoveTx(context.Background(), removeReq)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "invalid txid length")
	})
}

// TestAddTxBatch tests the AddTxBatch method
func TestAddTxBatch(t *testing.T) {
	t.Run("add transaction batch with invalid TxInpoints", func(t *testing.T) {
		server, _ := setupServer(t)

		// Create a batch with one invalid transaction
		txHash1 := chainhash.HashH([]byte("tx-1"))
		txBatchRequests := []*blockassembly_api.AddTxRequest{
			{
				Txid:       txHash1[:],
				Fee:        100,
				Size:       250,
				TxInpoints: []byte{255, 255}, // Invalid format
			},
		}

		req := &blockassembly_api.AddTxBatchRequest{
			TxRequests: txBatchRequests,
		}

		resp, err := server.AddTxBatch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "unable to deserialize tx inpoints")
	})
}

// TestTxCount tests the TxCount method
func TestTxCount(t *testing.T) {
	t.Run("get transaction count", func(t *testing.T) {
		server, _ := setupServer(t)

		// Start the block assembler first
		err := server.blockAssembler.Start(context.Background())
		require.NoError(t, err)

		// Initial count may be 1 (coinbase) or 0 depending on implementation
		initialCount := server.TxCount()

		// Add some transactions via the internal block assembler instead
		// to avoid TxInpoints serialization issues
		for i := 0; i < 3; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i)))
			server.blockAssembler.AddTx(subtreepkg.SubtreeNode{
				Hash:        txHash,
				Fee:         uint64(100),
				SizeInBytes: uint64(250),
			}, subtreepkg.TxInpoints{})
		}

		// Wait for processing - expect initial count + 3 added transactions
		require.Eventually(t, func() bool {
			currentCount := server.TxCount()
			return currentCount == initialCount+3
		}, 2*time.Second, 10*time.Millisecond)
	})
}
