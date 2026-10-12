package subtreevalidation

import (
	"context"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A stored subtree proves its transactions were validated against SOME chain,
// not against the chain of whatever block later references it. A transaction
// legitimately blessed as conflicting on a side fork — where its counter-spender
// is absent from that fork's ancestry — becomes an ancestor double spend when the
// same subtree is replayed onto a chain that HAS confirmed the counter-spender.
//
// These tests pin that CheckBlockSubtrees re-derives the ancestry-sensitive part
// of the verdict for cached subtrees, and that it costs nothing when there is
// nothing to re-derive.

// buildCachedSubtree stores a blessed subtree containing txHash, optionally
// marked conflicting, and returns the block referencing it.
func buildCachedSubtree(t *testing.T, server *Server, txHash chainhash.Hash, conflicting bool) *model.Block {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(txHash, 1, 1))

	if conflicting {
		require.NoError(t, st.AddConflictingNode(txHash))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	rootHash := st.RootHash()

	require.NoError(t, server.subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, serialized))

	prevHash := chainhash.HashH([]byte("candidate-parent"))

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      12345678,
			Bits:           model.NBit{},
			Nonce:          1,
		},
		Height:   200,
		Subtrees: []*chainhash.Hash{rootHash},
	}
}

// TestCheckBlockSubtrees_ConflictingNodesTriggerAncestryCheck pins the gating
// decision this change introduces: when a cached subtree carries a conflicting
// transaction, the candidate's ancestry is fetched and the counter-spend check
// runs, instead of the block being blessed on file existence alone.
//
// The verdict the check then reaches is exercised end to end against real stores
// by TestCrossForkSubtreeReuse* in test/sequentialtest/double_spend, and by the
// existing checkCounterConflictingOnCurrentChain tests. Here the walk is stubbed
// to terminate immediately, so what is under test is purely whether the gate
// engages.
func TestCheckBlockSubtrees_ConflictingNodesTriggerAncestryCheck(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	conflictingTx := chainhash.HashH([]byte("conflicting-tx"))

	block := buildCachedSubtree(t, server, conflictingTx, true)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{77}, nil)

	// An input-less transaction has no counter-spenders, so the conflicting walk
	// terminates at once and the block is blessed.
	mockUtxo := server.utxoStore.(*utxo.MockUtxostore)
	mockUtxo.On("Get", mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{Tx: bt.NewTx()}, nil)
	// The walk always includes the transaction itself; with no block IDs it is
	// not on any ancestry, so the block stays blessed.
	mockUtxo.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})
	require.NoError(t, err)
	require.True(t, resp.Blessed)

	mockBlockchain.AssertCalled(t, "GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything)
	mockUtxo.AssertCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

// TestCheckBlockSubtrees_NoConflictingNodesCostsNothing pins the performance
// contract that justifies keeping the early return at all: a cached subtree with
// no conflicting transactions — effectively every subtree in normal operation —
// must not trigger an ancestry fetch or any UTXO walk.
func TestCheckBlockSubtrees_NoConflictingNodesCostsNothing(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	ordinaryTx := chainhash.HashH([]byte("ordinary-tx"))

	block := buildCachedSubtree(t, server, ordinaryTx, false)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.NoError(t, err)
	require.True(t, resp.Blessed)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.AssertNotCalled(t, "GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything)

	mockUtxo := server.utxoStore.(*utxo.MockUtxostore)
	mockUtxo.AssertNotCalled(t, "GetCounterConflicting", mock.Anything, mock.Anything)
}

// TestCheckBlockSubtrees_CachedSubtreeRejectedWhenCounterSpendOnAncestry pins
// the REJECTING direction in the default unit suite.
//
// Without this, the only coverage of the actual security property lives in the
// container-backed sequential tests, which do not run in a normal `go test ./...`
// — so a regression that silently re-blessed these blocks would pass CI's fast
// path. The walk is driven through a transaction with one input whose parent
// records a different spender, which is the shape that makes a counter-spender
// exist at all.
func TestCheckBlockSubtrees_CachedSubtreeRejectedWhenCounterSpendOnAncestry(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	const ancestorBlockID = uint32(77)

	// parent:0 is spent by counterSpender, while conflictingTx also spends it.
	parentTx := bt.NewTx()
	require.NoError(t, parentTx.AddOpReturnOutput([]byte("parent")))

	conflictingTx := bt.NewTx()
	require.NoError(t, conflictingTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      0,
	}))

	counterSpender := chainhash.HashH([]byte("counter-spender-tx"))

	block := buildCachedSubtree(t, server, *conflictingTx.TxIDChainHash(), true)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{ancestorBlockID}, nil)

	mockUtxo := server.utxoStore.(*utxo.MockUtxostore)

	mockUtxo.On("Get", mock.Anything, conflictingTx.TxIDChainHash(), mock.Anything).
		Return(&utxometa.Data{Tx: conflictingTx}, nil)

	// The parent records counterSpender as the spender of output 0.
	mockUtxo.On("Get", mock.Anything, parentTx.TxIDChainHash(), mock.Anything).
		Return(&utxometa.Data{
			Tx:            parentTx,
			SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&counterSpender, 0)},
		}, nil)

	// counterSpender has no conflicting children, and is mined in a block on the
	// candidate's own ancestry — which is what must reject the block.
	mockUtxo.On("Get", mock.Anything, &counterSpender, mock.Anything).
		Return(&utxometa.Data{Tx: bt.NewTx()}, nil)
	mockUtxo.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{BlockIDs: []uint32{ancestorBlockID}}, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.Error(t, err, "a cached subtree must not be blessed for a chain that already confirmed the counter-spender")
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrTxInvalid),
		"must surface as tx-invalid so block validation persists the block as invalid, got: %v", err)
}
