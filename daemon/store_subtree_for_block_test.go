package daemon

import (
	"context"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// requireSameTx asserts that got is the same transaction as want. The transaction
// id is a cryptographic commitment to the whole body, so equal ids prove the body
// round-tripped intact; the input/output checks are format-independent spot checks
// on top of that.
func requireSameTx(t *testing.T, want, got *bt.Tx) {
	t.Helper()

	require.NotNil(t, got)
	require.Equal(t, *want.TxIDChainHash(), *got.TxIDChainHash())
	require.Equal(t, len(want.Inputs), len(got.Inputs))
	require.Equal(t, len(want.Outputs), len(got.Outputs))

	for i := range want.Outputs {
		require.Equal(t, want.Outputs[i].Satoshis, got.Outputs[i].Satoshis)
	}
}

// TestStoreSubtreeForBlock exercises TestDaemon.StoreSubtreeForBlock against an
// in-memory subtree store. The helper only touches the exported Ctx and
// SubtreeStore fields, so it can be driven from a minimally-populated TestDaemon
// without standing up a full daemon, its dependencies, or any containers.
func TestStoreSubtreeForBlock(t *testing.T) {
	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	td := TestDaemon{
		privKey:      privKey,
		Ctx:          context.Background(),
		SubtreeStore: memory.New(),
	}

	// A parent transaction with two spendable outputs, then two children that each
	// spend one of them. The children are the non-coinbase transactions placed in
	// the subtree after the coinbase placeholder.
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(2, 1e8),
	)

	tx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)
	tx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	txs := []*bt.Tx{tx1, tx2}
	parentHash := *parentTx.TxIDChainHash()

	const deleteAtHeight = uint32(100)

	rootHash := td.StoreSubtreeForBlock(t, txs, deleteAtHeight)
	require.NotNil(t, rootHash)

	// The subtree blob must round-trip to the expected leaf layout: a coinbase
	// placeholder followed by the two child tx hashes in block order, and the same
	// root hash the helper returned.
	subtreeBytes, err := td.SubtreeStore.Get(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)

	parsedSubtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)
	require.Equal(t, 3, parsedSubtree.Length())
	require.True(t, parsedSubtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue))
	require.Equal(t, *tx1.TxIDChainHash(), parsedSubtree.Nodes[1].Hash)
	require.Equal(t, *tx2.TxIDChainHash(), parsedSubtree.Nodes[2].Hash)
	require.Equal(t, *rootHash, *parsedSubtree.RootHash())

	// The subtree data blob must round-trip to the two child tx bodies in order.
	// Index 0 is the coinbase placeholder slot and carries no body.
	dataBytes, err := td.SubtreeStore.Get(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	parsedData, err := subtreepkg.NewSubtreeDataFromBytes(parsedSubtree, dataBytes)
	require.NoError(t, err)
	require.Len(t, parsedData.Txs, 3)
	require.Nil(t, parsedData.Txs[0])
	requireSameTx(t, tx1, parsedData.Txs[1])
	requireSameTx(t, tx2, parsedData.Txs[2])

	// The subtree meta blob must round-trip to each child's parent inpoints. Both
	// children spend the same parent, so each records that single parent tx hash.
	metaBytes, err := td.SubtreeStore.Get(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)

	parsedMeta, err := subtreepkg.NewSubtreeMetaFromBytes(parsedSubtree, metaBytes)
	require.NoError(t, err)

	parents1, err := parsedMeta.GetParentTxHashes(1)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{parentHash}, parents1)

	parents2, err := parsedMeta.GetParentTxHashes(2)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{parentHash}, parents2)

	// Storing the same transaction set again must succeed (overwrite is allowed) and
	// must yield the same deterministic root hash.
	rootHashAgain := td.StoreSubtreeForBlock(t, txs, deleteAtHeight)
	require.NotNil(t, rootHashAgain)
	require.Equal(t, *rootHash, *rootHashAgain)
}
