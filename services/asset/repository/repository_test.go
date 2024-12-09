package repository_test

import (
	"context"
	"crypto/rand"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getMemoryStore(t *testing.T) blob.Store {
	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)

	txStore, err := blob.NewStore(ulogger.TestLogger{}, memoryURL)
	require.NoError(t, err)

	return txStore
}

func TestTransaction(t *testing.T) {
	var subtreeStore blob.Store

	var blockStore blob.Store

	txStore := getMemoryStore(t)
	utxoStore := memory.New(ulogger.TestLogger{})

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Put a transaction into the transaction store
	tx, err := bt.NewTxFromString("0100000001ec3269622c145e065cac62fb47215583ac20efaed38869b5bef2e51fb76875f2010000006a473044022011fbfc7d09cf2e279fe137a1d37f06a94f41671d879f66db5387764522a8e20002205d4bf825a7c9e04468ceb452400ea1e09c19e70af1cb48a00012cb267423bb8b41210262142850483b6728b8ecd299e4d0c8cf30ea0636f66205166814e52d73b64b4bffffffff0200000000000000000a006a075354554b2e434f7ba23401000000001976a91454cba8da8701174e34aac2bb31d42a88e2c302d088ac00000000")
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	err = txStore.Set(context.Background(), txHash.CloneBytes(), tx.Bytes())
	require.NoError(t, err)

	// Create a new repository
	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, utxoStore, txStore, blockchainClient, subtreeStore, blockStore)
	require.NoError(t, err)

	// Get the transaction from the repository
	b, err := repo.GetTransaction(context.Background(), txHash)
	require.NoError(t, err)

	tx2, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	assert.Equal(t, tx.TxID(), tx2.TxID())
}

func TestSubtree(t *testing.T) {
	txns, key, repo := setupSubtreeData(t)

	// Get the subtree node bytes from the repository
	st, err := repo.GetSubtree(context.Background(), key)
	require.NoError(t, err)

	b, err := st.SerializeNodes()
	require.NoError(t, err)

	subtreeNodes := make([]chainhash.Hash, len(b)/32)
	for i := 0; i < len(b); i += 32 {
		subtreeNodes[i/32] = chainhash.Hash(b[i : i+32])
	}

	subtree2, err := util.NewTreeByLeafCount(len(b) / 32)
	require.NoError(t, err)

	for _, hash := range subtreeNodes {
		err = subtree2.AddNode(hash, 0, 0)
		require.NoError(t, err)
	}

	assert.Equal(t, txns[0], subtree2.Nodes[0].Hash)
	assert.Equal(t, txns[1], subtree2.Nodes[1].Hash)
}

func TestSubtreeReader(t *testing.T) {
	txns, key, repo := setupSubtreeData(t)

	// Get the subtree node bytes from the repository
	reader, err := repo.GetSubtreeReader(context.Background(), key)
	require.NoError(t, err)

	b, err := util.DeserializeNodesFromReader(reader)
	require.NoError(t, err)

	subtreeNodes := make([]chainhash.Hash, len(b)/32)
	for i := 0; i < len(b); i += 32 {
		subtreeNodes[i/32] = chainhash.Hash(b[i : i+32])
	}

	subtree2, err := util.NewTreeByLeafCount(len(b) / 32)
	require.NoError(t, err)

	for _, hash := range subtreeNodes {
		err = subtree2.AddNode(hash, 0, 0)
		require.NoError(t, err)
	}

	assert.Equal(t, txns[0], subtree2.Nodes[0].Hash)
	assert.Equal(t, txns[1], subtree2.Nodes[1].Hash)
}

func setupSubtreeData(t *testing.T) ([]chainhash.Hash, *chainhash.Hash, *repository.Repository) {
	itemsPerSubtree := 2

	subtree, err := util.NewTreeByLeafCount(itemsPerSubtree)
	require.NoError(t, err)

	txns := make([]chainhash.Hash, itemsPerSubtree)

	for i := 0; i < itemsPerSubtree; i++ {
		txid := make([]byte, 32)
		n, err := rand.Read(txid)
		require.NoError(t, err)
		require.Equal(t, 32, n)

		txns[i] = chainhash.HashH(txid)
	}

	for _, hash := range txns {
		err := subtree.AddNode(hash, 1, 0)
		require.NoError(t, err)
	}

	blockStore := getMemoryStore(t)
	subtreeStore := getMemoryStore(t)
	txStore := getMemoryStore(t)
	utxoStore := memory.New(ulogger.TestLogger{})

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Put the subtree into the subtree store
	key := subtree.RootHash()

	value, err := subtree.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), key.CloneBytes(), value, options.WithFileExtension("subtree"))
	require.NoError(t, err)

	// Create a new repository
	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, utxoStore, txStore, blockchainClient, subtreeStore, blockStore)
	require.NoError(t, err)

	return txns, key, repo
}

// func Test_GetFullBlock(t *testing.T) {
// 	// setup
// 	ctx := Setup(t)
// 	block := mockBlock(ctx, t)
// 	_, err := ctx.server.store.StoreBlock(context.Background(), block, "")
// 	require.NoError(t, err)

// 	// test
// 	response, err := ctx.server.GetFullBlock(context.Background(), &blockchain_api.GetBlockRequest{Hash: block.Header.Hash().CloneBytes()})
// 	require.NoError(t, err)
// 	require.NotNil(t, response)

// 	buf := bytes.NewBuffer(response.FullBlockBytes)

// 	// version, 4 bytes
// 	version := binary.LittleEndian.Uint32(buf.Next(4))
// 	assert.Equal(t, block.Header.Version, version)

// 	// hashPrevBlock, 32 bytes
// 	hashPrevBlock, _ := chainhash.NewHash(buf.Next(32))
// 	assert.Equal(t, block.Header.HashPrevBlock, hashPrevBlock)

// 	// hashMerkleRoot, 32 bytes
// 	hashMerkleRoot, _ := chainhash.NewHash(buf.Next(32))
// 	assert.Equal(t, block.Header.HashMerkleRoot, hashMerkleRoot)

// 	// timestamp, 4 bytes
// 	timestamp := binary.LittleEndian.Uint32(buf.Next(4))
// 	assert.Equal(t, block.Header.Timestamp, timestamp)

// 	// difficulty, 4 bytes
// 	difficulty := model.NewNBitFromSlice(buf.Next(4))
// 	assert.Equal(t, block.Header.Bits, difficulty)

// 	// nonce, 4 bytes
// 	nonce := binary.LittleEndian.Uint32(buf.Next(4))
// 	assert.Equal(t, block.Header.Nonce, nonce)

// 	// transaction count, varint
// 	transactionCount, _ := binary.ReadUvarint(buf)
// 	assert.Equal(t, block.TransactionCount, transactionCount)

// 	// check the coinbase transaction
// 	txBytes := buf.Bytes()
// 	coinbaseTx, size, err := bt.NewTxFromStream(txBytes)
// 	require.NoError(t, err)
// 	require.NotNil(t, coinbaseTx)
// 	assert.Equal(t, block.CoinbaseTx.Size(), size)

// 	// check the 2nd tx
// 	tx, size2, err := bt.NewTxFromStream(txBytes[size:])
// 	require.NoError(t, err)
// 	require.NotNil(t, tx)

// 	require.Equal(t, size+size2, len(txBytes))
//}
