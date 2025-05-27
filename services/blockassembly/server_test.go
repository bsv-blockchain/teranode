package blockassembly

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	utxomemory "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
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

		subtreeBytes, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], options.WithFileExtension(options.SubtreeFileExtension))
		require.NoError(t, err)
		require.NotNil(t, subtreeBytes)

		subtreeFromStore, err := util.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err)

		require.Equal(t, subtree.RootHash(), subtreeFromStore.RootHash())

		for idx, node := range subtree.Nodes {
			require.Equal(t, node, subtreeFromStore.Nodes[idx])
		}

		// check that the meta-data is stored
		subtreeMetaBytes, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], options.WithFileExtension(options.SubtreeMetaFileExtension))
		require.NoError(t, err)

		subtreeMeta, err := util.NewSubtreeMetaFromBytes(subtreeFromStore, subtreeMetaBytes)
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
		_, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], options.WithFileExtension(options.SubtreeMetaFileExtension))
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

func setup(t *testing.T) (*BlockAssembly, *memory.Memory, *util.Subtree, *util.SyncedMap[chainhash.Hash, meta.TxInpoints]) {
	s, subtreeStore := setupServer(t)

	subtree, err := util.NewTreeByLeafCount(16)
	require.NoError(t, err)

	txMap := util.NewSyncedMap[chainhash.Hash, meta.TxInpoints]()

	previousHash := chainhash.HashH([]byte("previousHash"))

	for i := uint64(0); i < 16; i++ {
		txHash := chainhash.HashH([]byte(fmt.Sprintf("tx%d", i)))
		_ = subtree.AddNode(txHash, i, i)

		txMap.Set(txHash, meta.TxInpoints{ParentTxHashes: []chainhash.Hash{previousHash}, Idxs: [][]uint32{{0, 1}}})
		previousHash = txHash
	}

	return s, subtreeStore, subtree, txMap
}

func setupServer(t *testing.T) (*BlockAssembly, *memory.Memory) {
	tSettings := createTestSettings()

	subtreeStore := memory.New()
	blockchainClient := &blockchain.Mock{}

	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SendNotification", mock.Anything, mock.Anything).Return(nil)

	utxoStore := utxomemory.New(ulogger.TestLogger{})
	_ = utxoStore.SetBlockHeight(123)

	s := New(ulogger.TestLogger{}, tSettings, nil, utxoStore, subtreeStore, blockchainClient)

	require.NoError(t, s.Init(t.Context()))

	return s, subtreeStore
}
