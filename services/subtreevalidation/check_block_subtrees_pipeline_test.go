package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildMultiBatchBlock creates n single-tx subtrees, stores each one's
// FileTypeSubtreeToCheck marker and FileTypeSubtreeData locally (so the batch
// loader resolves them without any HTTP fetch), and returns a block whose
// per-subtree tx count forces the configured TxBatchSize to one subtree per
// batch — i.e. n sequential batches through the pipeline.
func buildMultiBatchBlock(t *testing.T, server *Server, n int) *model.Block {
	t.Helper()

	subtreeHashes := make([]*chainhash.Hash, 0, n)

	for i := 0; i < n; i++ {
		// Build a genuinely unique tx per subtree (distinct input outpoint +
		// distinct OP_RETURN) so each subtree has its own root hash.
		tx := bt.NewTx()
		require.NoError(t, tx.From(fmt.Sprintf("%064x", i+1), 0,
			"76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, tx.AddOpReturnOutput([]byte(fmt.Sprintf("pipeline_tx_%d", i))))

		s, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, s.AddCoinbaseNode())
		require.NoError(t, s.AddNode(*tx.TxIDChainHash(), 0, 0))

		serialized, err := s.Serialize()
		require.NoError(t, err)

		// FileTypeSubtreeToCheck present (so findLocalSubtreeFile resolves it)
		// but FileTypeSubtree absent (so the subtree is "missing" and enters the
		// batch loop).
		require.NoError(t, server.subtreeStore.Set(context.Background(),
			s.RootHash()[:], fileformat.FileTypeSubtreeToCheck, serialized))

		// SubtreeData stream omits the coinbase placeholder; first tx is the
		// non-coinbase. Stored locally so loadSubtreeBatch extracts without HTTP.
		subtreeData := bytes.Buffer{}
		subtreeData.Write(tx.Bytes())
		require.NoError(t, server.subtreeStore.Set(context.Background(),
			s.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes()))

		subtreeHashes = append(subtreeHashes, s.RootHash())
	}

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	coinbaseTx := &bt.Tx{Version: 1}
	// TransactionCount == len(subtrees) => txsPerSubtree == 1, so with
	// TxBatchSize == 1 each batch holds exactly one subtree.
	block, err := model.NewBlock(header, coinbaseTx, subtreeHashes, uint64(n), 500, 0, 0)
	require.NoError(t, err)

	return block
}

func wireMultiBatchMocks(server *Server) {
	server.settings.SubtreeValidation.TxBatchSize = 1

	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader", mock.Anything).
		Return(&model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}, &model.BlockHeaderMeta{ID: 100}, nil).Maybe()
	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()
	// CheckBlockSubtrees gates optimistic fetch on GetFSMCurrentState (not
	// IsFSMCurrentState); mock the method actually on the code path.
	runningState := blockchain.FSMStateRUNNING
	server.blockchainClient.(*blockchain.Mock).On("GetFSMCurrentState", mock.Anything).
		Return(&runningState, nil).Maybe()
}

// TestCheckBlockSubtrees_MultiBatch_BalancesArenas drives the real batch
// pipeline across several batches and asserts every per-subtree arena that was
// acquired is returned to the pool — no leak through the pipelined load-ahead /
// process / release path.
func TestCheckBlockSubtrees_MultiBatch_BalancesArenas(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	wireMultiBatchMocks(server)

	mockValidator := server.validatorClient.(*validator.MockValidator)
	mockValidator.UtxoStore = server.utxoStore

	const numSubtrees = 5
	block := buildMultiBatchBlock(t, server, numSubtrees)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	// arenaGets/arenaPuts are package-global; the pre/post delta is only valid
	// because subtreevalidation tests run sequentially (no t.Parallel, per
	// the Testing section in AGENTS.md). A concurrent test touching the pool would
	// perturb these deltas.
	gets0, puts0 := arenaGets.Load(), arenaPuts.Load()

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "http://test.com",
	})
	require.NoError(t, err)

	getsDelta := arenaGets.Load() - gets0
	putsDelta := arenaPuts.Load() - puts0

	require.GreaterOrEqual(t, getsDelta, int64(numSubtrees),
		"the batch loop must have acquired an arena per missing subtree")
	require.Equal(t, getsDelta, putsDelta,
		"every acquired arena must be released (no leak): gets=%d puts=%d", getsDelta, putsDelta)
}

// TestCheckBlockSubtrees_MultiBatch_ProcessErrorBalancesArenas injects a
// validation failure so an early batch's PROCESS errors. The pipeline must
// abort, surface the error, and still release the arenas of any batch the
// producer had already loaded ahead — the central risk of the load-ahead
// design.
func TestCheckBlockSubtrees_MultiBatch_ProcessErrorBalancesArenas(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	wireMultiBatchMocks(server)

	mockValidator := server.validatorClient.(*validator.MockValidator)
	mockValidator.UtxoStore = server.utxoStore
	// Fail the very first transaction validated, forcing an early batch's
	// processTransactionsInLevels to error while later batches are in flight.
	mockValidator.Errors = []error{errors.NewProcessingError("injected validation failure")}

	const numSubtrees = 5
	block := buildMultiBatchBlock(t, server, numSubtrees)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	gets0, puts0 := arenaGets.Load(), arenaPuts.Load()

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "http://test.com",
	})
	require.Error(t, err)

	getsDelta := arenaGets.Load() - gets0
	putsDelta := arenaPuts.Load() - puts0

	require.Equal(t, getsDelta, putsDelta,
		"arenas must balance even when a batch fails mid-pipeline: gets=%d puts=%d", getsDelta, putsDelta)
}
