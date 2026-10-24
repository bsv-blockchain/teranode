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
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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

// conflictingFlagStore answers BatchDecorate from an explicit set.
//
// This is where the cached path now reads conflicting-ness from. It used to read
// each subtree's ConflictingNodes trailer, which records what was true at blessing
// time and is never rewritten for a loser that was never mined (issue #1742), so
// the trailer no longer decides anything and these tests must drive the store.
type conflictingFlagStore struct {
	utxo.Store

	conflicting map[chainhash.Hash]bool
	notFound    map[chainhash.Hash]bool
}

func (c *conflictingFlagStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	for _, item := range items {
		if c.notFound[item.Hash] {
			item.Data = nil
			item.Err = errors.NewTxNotFoundError("transaction %s not found", item.Hash.String())

			continue
		}

		item.Data = &utxometa.Data{Conflicting: c.conflicting[item.Hash]}
	}

	return nil
}

// flagInStore makes the store report the given transactions as conflicting,
// leaving every other transaction of the block unflagged.
func flagInStore(server *Server, conflicting ...chainhash.Hash) {
	set := make(map[chainhash.Hash]bool, len(conflicting))
	for _, h := range conflicting {
		set[h] = true
	}

	server.utxoStore = &conflictingFlagStore{Store: server.utxoStore, conflicting: set}
}

// missingInStore makes the store report the given transactions as absent, the way
// both production backends do: item.Err set, item.Data nil.
func missingInStore(server *Server, missing ...chainhash.Hash) {
	set := make(map[chainhash.Hash]bool, len(missing))
	for _, h := range missing {
		set[h] = true
	}

	server.utxoStore = &conflictingFlagStore{Store: server.utxoStore, notFound: set}
}

// buildCachedSubtree stores a blessed subtree containing txHash and returns the
// block referencing it. withTrailer names txHash in the subtree's ConflictingNodes
// trailer, which the check must now ignore in both directions.
func buildCachedSubtree(t *testing.T, server *Server, txHash chainhash.Hash, withTrailer bool) *model.Block {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(txHash, 1, 1))

	if withTrailer {
		require.NoError(t, st.AddConflictingNode(txHash))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	rootHash := st.RootHash()

	require.NoError(t, server.subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, serialized))

	return boundBlock(t, st, rootHash)
}

// boundBlock returns a block whose header commits to subtreeHashes, the first
// of which is first, so it passes the body-to-header binding the cached path runs
// before probing.
func boundBlock(t *testing.T, first *subtreepkg.Subtree, subtreeHashes ...*chainhash.Hash) *model.Block {
	t.Helper()

	coinbaseTx := &bt.Tx{Version: 1}

	merkleRoot, err := first.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) // nolint: gosec
	require.NoError(t, err)

	if len(subtreeHashes) > 1 {
		top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeHashes))
		require.NoError(t, err)
		require.NoError(t, top.AddNode(*merkleRoot, 1, 0))

		for _, h := range subtreeHashes[1:] {
			require.NoError(t, top.AddNode(*h, 1, 0))
		}

		merkleRoot = top.RootHash()
	}

	prevHash := chainhash.HashH([]byte("candidate-parent"))

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      12345678,
			Bits:           model.NBit{},
			Nonce:          1,
		},
		CoinbaseTx: coinbaseTx,
		Height:     200,
		Subtrees:   subtreeHashes,
	}
}

// storeBoundCachedBlock stores n distinct blessed two-leaf subtrees and returns
// a block whose header commits to them.
func storeBoundCachedBlock(t *testing.T, server *Server, n int) *model.Block {
	t.Helper()

	subtrees := make([]*subtreepkg.Subtree, n)
	hashes := make([]*chainhash.Hash, n)

	for i := range subtrees {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		} else {
			require.NoError(t, st.AddNode(chainhash.HashH([]byte{byte(i), 0}), 1, 1))
		}

		require.NoError(t, st.AddNode(chainhash.HashH([]byte{byte(i), 1}), 1, 1))

		serialized, err := st.Serialize()
		require.NoError(t, err)

		hashes[i] = st.RootHash()
		subtrees[i] = st

		require.NoError(t, server.subtreeStore.Set(context.Background(), hashes[i][:], fileformat.FileTypeSubtree, serialized))
	}

	return boundBlock(t, subtrees[0], hashes...)
}

// TestCheckBlockSubtrees_StoreConflictingFlagTriggersAncestryCheck pins the gate:
// when the STORE flags a transaction of a cached subtree as conflicting, the
// candidate's ancestry is fetched and the counter-spend check runs, instead of the
// block being blessed on file existence alone.
//
// The subtree's trailer is deliberately empty here. That is the #1742 case: a
// transaction demoted after its subtree was blessed, whose loser was never mined,
// never appears in a trailer, so a trailer-sourced gate would not engage at all.
//
// The verdict the check then reaches is exercised end to end against real stores
// by TestCrossForkSubtreeReuse* in test/sequentialtest/double_spend, and by the
// existing checkCounterConflictingOnCurrentChain tests. Here the walk is stubbed
// to terminate immediately, so what is under test is purely whether the gate
// engages.
func TestCheckBlockSubtrees_StoreConflictingFlagTriggersAncestryCheck(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	conflictingTx := chainhash.HashH([]byte("conflicting-tx"))

	// Empty trailer: only the store knows this transaction is conflicting.
	block := buildCachedSubtree(t, server, conflictingTx, false)

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

	flagInStore(server, conflictingTx)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})
	require.NoError(t, err)
	require.True(t, resp.Blessed)

	mockBlockchain.AssertCalled(t, "GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything)
	mockUtxo.AssertCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

// TestCheckBlockSubtrees_NoConflictsCostsOnlyTheProbe pins what the cached path
// now costs when nothing is conflicting — effectively every subtree in normal
// operation.
//
// The conflicting probe itself is unavoidable: it is what replaced the trailer as
// the source of truth, and skipping it is what let identical block bytes get
// opposite verdicts. What must still NOT happen is the expensive part — the
// ancestry fetch and the counter-spend walk.
func TestCheckBlockSubtrees_NoConflictsCostsOnlyTheProbe(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	ordinaryTx := chainhash.HashH([]byte("ordinary-tx"))

	block := buildCachedSubtree(t, server, ordinaryTx, false)

	mockUtxo := server.utxoStore.(*utxo.MockUtxostore)

	flagInStore(server) // nothing conflicting

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.NoError(t, err)
	require.True(t, resp.Blessed)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.AssertNotCalled(t, "GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything)

	mockUtxo.AssertNotCalled(t, "GetCounterConflicting", mock.Anything, mock.Anything)
}

// TestCheckBlockSubtrees_TrailerIsNotTrusted pins the other half of no longer
// sourcing the verdict from the trailer: a subtree whose trailer names a
// transaction as conflicting, which the store says is NOT conflicting, must not
// drag the block through an ancestry fetch and a counter-spend walk.
//
// A trailer entry is a record of a past blessing, not a current fact — it survives
// the transaction being promoted back to the winner on a reorg.
func TestCheckBlockSubtrees_TrailerIsNotTrusted(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	staleTrailerTx := chainhash.HashH([]byte("stale-trailer-tx"))

	// Trailer says conflicting; the store disagrees, and the store wins.
	block := buildCachedSubtree(t, server, staleTrailerTx, true)

	flagInStore(server) // nothing conflicting

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.NoError(t, err)
	require.True(t, resp.Blessed)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.AssertNotCalled(t, "GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything)
}

// TestCheckBlockSubtrees_MissingTxRecordDoesNotFailTheBlock guards a block whose
// subtree blob outlived its transaction records.
//
// The UTXO and subtree retention windows are configured separately, and
// blockpersister keeps subtree blobs indefinitely, so a node can hold a subtree
// whose transactions have aged out; reorgs and catchup over older blocks reach
// exactly that. Both backends report the miss through item.Err, and block
// validation maps ErrTxNotFound to a transient-incomplete retry — against peers
// that cannot supply a record missing from THIS node's store, so the block would
// never converge.
//
// Skipping is not proof the record was never conflicting; it depends on the
// retention ordering documented on cachedSubtreeConflictingNodes.
func TestCheckBlockSubtrees_MissingTxRecordDoesNotFailTheBlock(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	agedOutTx := chainhash.HashH([]byte("aged-out-tx"))

	block := buildCachedSubtree(t, server, agedOutTx, false)

	missingInStore(server, agedOutTx)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.NoError(t, err,
		"a transaction missing from the UTXO store must not fail the block - it would be retried against peers that cannot supply a record absent from our own store")
	require.True(t, resp.Blessed)
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

	// Empty trailer again: the store is what makes this transaction conflicting.
	block := buildCachedSubtree(t, server, *conflictingTx.TxIDChainHash(), false)

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

	flagInStore(server, *conflictingTx.TxIDChainHash())

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})

	require.Error(t, err, "a cached subtree must not be blessed for a chain that already confirmed the counter-spender")
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrTxInvalid),
		"must surface as tx-invalid so block validation persists the block as invalid, got: %v", err)
}
