package repository_test

import (
	"context"
	"encoding/binary"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Put a transaction into the transaction store
	tx, err := bt.NewTxFromString("0100000001ec3269622c145e065cac62fb47215583ac20efaed38869b5bef2e51fb76875f2010000006a473044022011fbfc7d09cf2e279fe137a1d37f06a94f41671d879f66db5387764522a8e20002205d4bf825a7c9e04468ceb452400ea1e09c19e70af1cb48a00012cb267423bb8b41210262142850483b6728b8ecd299e4d0c8cf30ea0636f66205166814e52d73b64b4bffffffff0200000000000000000a006a075354554b2e434f7ba23401000000001976a91454cba8da8701174e34aac2bb31d42a88e2c302d088ac00000000")
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	err = txStore.Set(context.Background(), txHash.CloneBytes(), fileformat.FileTypeTx, tx.Bytes())
	require.NoError(t, err)

	// Create a new repository
	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	// Get the transaction from the repository
	b, err := repo.GetTransaction(context.Background(), txHash)
	require.NoError(t, err)

	tx2, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	assert.Equal(t, tx.TxID(), tx2.TxID())
}

func TestGetSubtreeTransactions(t *testing.T) {
	t.Run("GetSubtreeTransactions form subtree store", func(t *testing.T) {
		txns, subtreeHash, repo := setupSubtreeData(t)

		// Get the transactions from the repository
		txMap, err := repo.GetSubtreeTransactions(context.Background(), subtreeHash)
		require.NoError(t, err)
		assert.Len(t, txMap, 2)

		for _, txHash := range txns {
			tx, ok := txMap[txHash]
			require.True(t, ok, "transaction %s not found in subtree transactions", txHash.String())

			assert.Equal(t, txHash, *tx.TxIDChainHash(), "transaction hash mismatch for %s", txHash.String())
		}
	})

	// the blockStore has been disabled for now because it the transactions in it are not extended
	// t.Run("GetSubtreeTransactions form block store", func(t *testing.T) {
	// 	txns, subtreeHash, repo := setupSubtreeData(t)
	//
	// 	err := repo.SubtreeStore.Del(t.Context(), subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	// 	require.NoError(t, err)
	//
	// 	// Get the transactions from the repository
	// 	txMap, err := repo.GetSubtreeTransactions(context.Background(), subtreeHash)
	// 	require.NoError(t, err)
	// 	assert.Len(t, txMap, 2)
	//
	// 	for _, txHash := range txns {
	// 		tx, ok := txMap[txHash]
	// 		require.True(t, ok, "transaction %s not found in subtree transactions", txHash.String())
	//
	// 		assert.Equal(t, txHash, *tx.TxIDChainHash(), "transaction hash mismatch for %s", txHash.String())
	// 	}
	// })
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

	subtree2, err := subtree.NewTreeByLeafCount(len(b) / 32)
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
	reader, err := repo.GetSubtreeTxIDsReader(context.Background(), key)
	require.NoError(t, err)
	defer reader.Close()

	b, err := subtree.DeserializeNodesFromReader(reader)
	require.NoError(t, err)

	subtreeNodes := make([]chainhash.Hash, len(b)/32)
	for i := 0; i < len(b); i += 32 {
		subtreeNodes[i/32] = chainhash.Hash(b[i : i+32])
	}

	subtree2, err := subtree.NewTreeByLeafCount(len(b) / 32)
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

	st, err := subtree.NewTreeByLeafCount(itemsPerSubtree)
	require.NoError(t, err)

	subtreeData := subtree.NewSubtreeData(st)

	txns := make([]chainhash.Hash, itemsPerSubtree)

	tx := &bt.Tx{
		Inputs:   []*bt.Input{},
		Outputs:  []*bt.Output{},
		Version:  0,
		LockTime: 0,
	}

	for i := 0; i < itemsPerSubtree; i++ {
		txx := tx.Clone()
		txx.Version = uint32(i)  // nolint:gosec
		txx.LockTime = uint32(i) // nolint:gosec

		txns[i] = *txx.TxIDChainHash()

		err := st.AddNode(txns[i], 1, 0)
		require.NoError(t, err)

		err = subtreeData.AddTx(txx, i)
		require.NoError(t, err)
	}

	blockStore := getMemoryStore(t)
	subtreeStore := getMemoryStore(t)
	txStore := getMemoryStore(t)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Put the subtree into the subtree store
	key := st.RootHash()

	value, err := st.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), key.CloneBytes(), fileformat.FileTypeSubtree, value)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), key.CloneBytes(), fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// write the length of the subtree data as the first 4 bytes of the subtree data file in the block store
	subtreeDataLen := uint32(len(subtreeDataBytes)) //nolint:gosec
	subtreeDataLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(subtreeDataLenBytes, subtreeDataLen)

	blockStoreSubtreeDataBytes := append(subtreeDataLenBytes, subtreeDataBytes...)
	err = blockStore.Set(context.Background(), key.CloneBytes(), fileformat.FileTypeSubtreeData, blockStoreSubtreeDataBytes)
	require.NoError(t, err)

	// Create a new repository
	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
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

//		require.Equal(t, size+size2, len(txBytes))
//	}
func TestRepository_GetBlockByHash(t *testing.T) {
	t.Run("GetBlockByHash returns block", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		blockHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

		// This will return an error as no block exists
		block, err := repo.GetBlockByHash(ctx, blockHash)
		assert.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "BLOCK_NOT_FOUND")
	})
}

func TestRepository_GetLastNBlocks(t *testing.T) {
	t.Run("GetLastNBlocks with various parameters", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		// Test with different parameters
		blocks, err := repo.GetLastNBlocks(ctx, 10, false, 0)
		assert.NoError(t, err)
		assert.NotNil(t, blocks)

		// Test with include orphans
		blocks, err = repo.GetLastNBlocks(ctx, 5, true, 0)
		assert.NoError(t, err)
		assert.NotNil(t, blocks)

		// Test with from height
		blocks, err = repo.GetLastNBlocks(ctx, 3, false, 100)
		assert.NoError(t, err)
		assert.NotNil(t, blocks)
	})
}

func TestRepository_GetBlocks(t *testing.T) {
	t.Run("GetBlocks with hash and count", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		blockHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

		blocks, err := repo.GetBlocks(ctx, blockHash, 5)
		assert.NoError(t, err)
		assert.NotNil(t, blocks)
	})
}

func TestRepository_GetBlockHeaders(t *testing.T) {
	t.Run("GetBlockHeaders with hash and count", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		blockHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

		headers, metas, err := repo.GetBlockHeaders(ctx, blockHash, 10)
		// Might not error, but will return empty results
		if err != nil {
			// If error, headers and metas should be nil
			assert.Nil(t, headers)
			assert.Nil(t, metas)
		} else {
			// If no error, headers and metas should be empty
			assert.Empty(t, headers)
			assert.Empty(t, metas)
		}
	})
}

func TestRepository_GetBlockHeadersToCommonAncestor(t *testing.T) {
	t.Run("GetBlockHeadersToCommonAncestor finds common ancestor", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		blockHash1, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		blockHash2, _ := chainhash.NewHashFromStr("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")

		hashes := []*chainhash.Hash{blockHash1, blockHash2}

		headers, metas, err := repo.GetBlockHeadersToCommonAncestor(ctx, blockHash1, hashes, 10)
		// Will error because no blocks exist
		assert.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

func TestRepository_GetBlockHeadersFromCommonAncestor(t *testing.T) {
	t.Run("GetBlockHeadersFromCommonAncestor returns headers", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		blockHash1, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		blockHash2, _ := chainhash.NewHashFromStr("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")

		hashes := []chainhash.Hash{*blockHash1, *blockHash2}

		headers, metas, err := repo.GetBlockHeadersFromCommonAncestor(ctx, blockHash1, hashes, 10)
		// Will error because no blocks exist
		assert.Error(t, err)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

func TestRepository_GetBlockHeadersFromHeight(t *testing.T) {
	t.Run("GetBlockHeadersFromHeight returns headers starting at height", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		headers, metas, err := repo.GetBlockHeadersFromHeight(ctx, 100, 10)
		assert.NoError(t, err)
		assert.NotNil(t, headers)
		// metas might be empty slice
		assert.True(t, metas == nil || len(metas) == 0)
	})
}

func TestRepository_GetSubtreeData(t *testing.T) {
	t.Run("GetSubtreeData with various scenarios", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		// Create a simple subtree
		st, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)

		tx1 := &bt.Tx{Version: 1, LockTime: 0}
		tx2 := &bt.Tx{Version: 2, LockTime: 0}

		hash1 := tx1.TxIDChainHash()
		hash2 := tx2.TxIDChainHash()

		err = st.AddNode(*hash1, 0, 0)
		require.NoError(t, err)
		err = st.AddNode(*hash2, 0, 0)
		require.NoError(t, err)

		subtreeData := subtree.NewSubtreeData(st)
		err = subtreeData.AddTx(tx1, 0)
		require.NoError(t, err)
		err = subtreeData.AddTx(tx2, 1)
		require.NoError(t, err)

		subtreeHash := st.RootHash()
		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		// Store both subtree and subtree data (GetSubtreeData needs both)
		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)
		err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData, subtreeDataBytes)
		require.NoError(t, err)

		// Get subtree data
		retrievedData, err := repo.GetSubtreeData(ctx, subtreeHash)
		assert.NoError(t, err)
		assert.NotNil(t, retrievedData)
	})
}

func TestRepository_GetSubtreeHead(t *testing.T) {
	t.Run("GetSubtreeHead returns structured data", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		// Create a simple subtree
		st, err := subtree.NewTreeByLeafCount(4)
		require.NoError(t, err)

		for i := 0; i < 4; i++ {
			tx := &bt.Tx{Version: uint32(i), LockTime: uint32(i)}
			hash := tx.TxIDChainHash()
			err = st.AddNode(*hash, 0, 0)
			require.NoError(t, err)
		}

		subtreeHash := st.RootHash()
		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)

		// Store subtree
		err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Get subtree head
		result, numNodes, err := repo.GetSubtreeHead(ctx, subtreeHash)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, numNodes) // We added 4 nodes to the subtree
	})
}

func TestRepository_ErrorHandling(t *testing.T) {
	t.Run("handles missing transaction", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		txStore := getMemoryStore(t)
		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		// Try to get non-existent transaction
		txHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		data, err := repo.GetTransaction(ctx, txHash)
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("handles store errors gracefully", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		settings := test.CreateBaseTestSettings(t)

		// Create mock store that returns errors
		errorStore := &mockErrorStore{}

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
		require.NoError(t, err)

		subtreeStore := getMemoryStore(t)
		blockStore := getMemoryStore(t)

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, blockChainStore, nil, nil)
		require.NoError(t, err)

		repo, err := repository.NewRepository(logger, settings, utxoStore, errorStore, blockchainClient, nil, subtreeStore, blockStore, nil)
		require.NoError(t, err)

		// Try to get transaction from error store
		txHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		data, err := repo.GetTransaction(ctx, txHash)
		assert.Error(t, err)
		assert.Nil(t, data)
	})
}

// mockErrorStore is a blob.Store that always returns errors
type mockErrorStore struct{}

func (m *mockErrorStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("mock error")
}

func (m *mockErrorStore) GetReader(ctx context.Context, key []byte, fileType fileformat.FileType) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("mock error")
}

func (m *mockErrorStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("mock error")
}

func (m *mockErrorStore) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	return 0, errors.NewStorageError("mock error")
}

func (m *mockErrorStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return errors.NewStorageError("mock error")
}

func (m *mockErrorStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return errors.NewStorageError("mock error")
}

func (m *mockErrorStore) KeyExists(ctx context.Context, key []byte, fileType fileformat.FileType) bool {
	return false
}

func (m *mockErrorStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return false, errors.NewStorageError("mock error")
}

func (m *mockErrorStore) Health(ctx context.Context, includeMetrics bool) (int, string, error) {
	return 500, "error", errors.NewStorageError("mock error")
}

func (m *mockErrorStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	return errors.NewStorageError("mock error")
}

func (m *mockErrorStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, height uint32, opts ...options.FileOption) error {
	return errors.NewStorageError("mock error")
}

func (m *mockErrorStore) SetCurrentBlockHeight(height uint32) {
}

func (m *mockErrorStore) Close(ctx context.Context) error {
	return nil
}
