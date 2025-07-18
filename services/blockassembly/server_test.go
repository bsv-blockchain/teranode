package blockassembly

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	subtreeStore := memory.New()
	blockchainClient := &blockchain.Mock{}

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1233321,
		Bits:           model.NBit{},
		Nonce:          12333,
	}

	subcriptionCh := make(chan *blockchain.Notification, 100)

	fsmStateRunning := blockchain.FSMStateRUNNING
	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SendNotification", mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetState", mock.Anything, mock.Anything).Return([]byte{}, errors.ErrNotFound)
	blockchainClient.On("GetFSMCurrentState", mock.Anything, mock.Anything).Return(&fsmStateRunning, nil)
	blockchainClient.On("SetState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything, mock.Anything, mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{}, nil)
	blockchainClient.On("GetNextWorkRequired", mock.Anything, mock.Anything).Return(nbits, nil)
	blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(subcriptionCh, nil)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_ = utxoStore.SetBlockHeight(123)

	s := New(ulogger.TestLogger{}, tSettings, nil, utxoStore, subtreeStore, blockchainClient)

	// Skip waiting for pending blocks in tests to prevent mock issues
	s.SetSkipWaitForPendingBlocks(true)

	require.NoError(t, s.Init(t.Context()))

	return s, subtreeStore
}
