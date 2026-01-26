package repository

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	memory_blob "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	coinbase, _ = bt.NewTxFromString(model.CoinbaseHex)
	tx1, _      = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	params = blockInfo{
		version:           1,
		bits:              "2000ffff",
		previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
		height:            1,
		nonce:             2083236893,
		//nolint:gosec
		timestamp: uint32(time.Now().Unix()),
		txs:       []*bt.Tx{coinbase, tx1},
	}
)

func TestGetLegacyBlockWithSubtreeDataFromStore(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)
	ctx.logger.Debugf("test")

	block, subtree := newBlock(ctx, t, params)

	blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// create the .subtreeData file
	subtreeData := subtreepkg.NewSubtreeData(subtree)

	// SubtreeData should only contain non-coinbase transactions
	for i, tx := range params.txs {
		if i != 0 { // Skip coinbase at index 0
			require.NoError(t, subtreeData.AddTx(tx, i))
		}
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// should be able to get the block from the subtree-store from file
	r, err := ctx.repo.GetLegacyBlockReader(t.Context(), &chainhash.Hash{})
	require.NoError(t, err)

	bytes := make([]byte, 4096)

	// magic, 4 bytes
	n, err := io.ReadFull(r, bytes[:4])
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xf9, 0xbe, 0xb4, 0xd9}, bytes[:n])

	// size, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	size := binary.LittleEndian.Uint32(bytes[:n])
	//nolint:gosec
	assert.Equal(t, uint32(block.SizeInBytes+uint64(model.BlockHeaderSize+1)), size)

	assertBlockFromReader(t, r, bytes, block)
}

func TestGetLegacyBlockWithSubtreeStore(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)
	ctx.logger.Debugf("test")

	block, subtree := newBlock(ctx, t, params)

	blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// Create the txs in the utxo store
	for i, tx := range params.txs {
		if i != 0 {
			_, err := ctx.repo.UtxoStore.Create(context.Background(), tx, params.height)
			require.NoError(t, err)
		}
	}

	// Create the subtree in the subtree store
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	// go get me a legacy block from the subtree-store and utxo-store
	// this should NOT find anything in the block-store
	r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
	require.NoError(t, err)

	bytes := make([]byte, 4096)

	// magic, 4 bytes
	n, err := io.ReadFull(r, bytes[:4])
	assert.NoError(t, err)

	assert.Equal(t, []byte{0xf9, 0xbe, 0xb4, 0xd9}, bytes[:n])

	// size, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	size := binary.LittleEndian.Uint32(bytes[:n])
	//nolint:gosec
	assert.Equal(t, uint32(block.SizeInBytes+uint64(model.BlockHeaderSize)+1), size)

	assertBlockFromReader(t, r, bytes, block)
}

func TestGetLegacyWireBlockWithSubtreeStore(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)
	ctx.logger.Debugf("test")

	block, subtree := newBlock(ctx, t, params)

	blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// Create the txs in the utxo store
	for i, tx := range params.txs {
		if i != 0 {
			_, err := ctx.repo.UtxoStore.Create(context.Background(), tx, params.height)
			require.NoError(t, err)
		}
	}

	// Create the subtree in the subtree store
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	// go get me a legacy block from the subtree-store and utxo-store
	// this should NOT find anything in the block-store
	r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
	require.NoError(t, err)

	bytes := make([]byte, 4096)

	// a wire block does not contain the magic number and size
	assertBlockFromReader(t, r, bytes, block)
}

func assertBlockFromReader(t *testing.T, r *io.PipeReader, bytes []byte, block *model.Block) {
	// version, 4 bytes
	n, err := io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	version := binary.LittleEndian.Uint32(bytes[:n])
	assert.Equal(t, block.Header.Version, version)

	// hashPrevBlock, 32 bytes
	n, err = io.ReadFull(r, bytes[:32])
	require.NoError(t, err)

	hashPrevBlock, _ := chainhash.NewHash(bytes[:n])
	assert.Equal(t, block.Header.HashPrevBlock, hashPrevBlock)

	// hashMerkleRoot, 32 bytes
	n, err = io.ReadFull(r, bytes[:32])
	require.NoError(t, err)

	hashMerkleRoot, _ := chainhash.NewHash(bytes[:n])
	assert.Equal(t, block.Header.HashMerkleRoot, hashMerkleRoot)

	// timestamp, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	timestamp := binary.LittleEndian.Uint32(bytes[:n])
	assert.Equal(t, block.Header.Timestamp, timestamp)

	// difficulty, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	difficulty, _ := model.NewNBitFromSlice(bytes[:n])
	assert.Equal(t, block.Header.Bits, *difficulty)

	// nonce, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
	require.NoError(t, err)

	nonce := binary.LittleEndian.Uint32(bytes[:n])
	assert.Equal(t, block.Header.Nonce, nonce)

	// transaction count, varint
	n, err = r.Read(bytes)
	require.NoError(t, err)

	transactionCount, _ := bt.NewVarIntFromBytes(bytes[:n])
	assert.Equal(t, block.TransactionCount, uint64(transactionCount))

	bytes, err = io.ReadAll(r)
	require.ErrorIs(t, err, io.ErrClosedPipe)

	// check the coinbase transaction
	coinbaseTx, coinbaseSize, err := bt.NewTxFromStream(bytes)
	require.NoError(t, err)
	require.NotNil(t, coinbaseTx)
	t.Logf("First transaction hash: %s (expected coinbase: %s, tx1: %s)",
		coinbaseTx.TxIDChainHash().String(),
		coinbase.TxIDChainHash().String(),
		tx1.TxIDChainHash().String())
	// Note: After the fix, the coinbase comes from subtree data, not block.CoinbaseTx
	// The size should match the actual coinbase transaction size
	assert.Equal(t, coinbase.Size(), coinbaseSize)

	// check the 2nd tx
	tx, txSize, err := bt.NewTxFromStream(bytes[coinbaseSize:])
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, tx1.Size(), txSize)

	// check the end of the stream
	n, err = r.Read(bytes)
	assert.Equal(t, io.ErrClosedPipe, err)
	assert.Equal(t, 0, n)
}

type blockInfo struct {
	version           uint32
	bits              string
	previousBlockHash string
	height            uint32
	nonce             uint32
	timestamp         uint32
	txs               []*bt.Tx
}

type testContext struct {
	repo     *Repository
	logger   ulogger.Logger
	settings *settings.Settings
}

func setup(t *testing.T) *testContext {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	txStore := memory_blob.New()
	blockchainClient := &blockchain.Mock{}
	subtreeStore := memory_blob.New()
	blockStore := memory_blob.New()

	repo, err := NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	return &testContext{
		repo:     repo,
		logger:   logger,
		settings: settings,
	}
}

func newBlock(_ *testContext, t *testing.T, b blockInfo) (*model.Block, *subtreepkg.Subtree) {
	if len(b.txs) == 0 {
		panic("no transactions provided")
	}

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)

	for i, tx := range b.txs {
		if i == 0 {
			require.NoError(t, subtree.AddCoinbaseNode())
		} else {
			require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
			require.NoError(t, err)
		}
	}

	nBits, _ := model.NewNBitFromString(b.bits)
	hashPrevBlock, _ := chainhash.NewHashFromStr(b.previousBlockHash)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())

	blockHeader := &model.BlockHeader{
		Version:        b.version,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: subtree.RootHash(), // doesn't matter, we're only checking the value and not whether it's correct
		Timestamp:      b.timestamp,
		Bits:           *nBits,
		Nonce:          b.nonce,
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       b.txs[0],
		TransactionCount: uint64(len(b.txs)),
		Subtrees:         subtreeHashes,
		Height:           b.height,
	}

	return block, subtree
}

// TestWriteTransactionsViaSubtreeStoreStreaming tests that the fan-in pipeline delivers transactions
// in the correct order even when chunks are fetched in parallel.
func TestWriteTransactionsViaSubtreeStoreStreaming(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("transactions are delivered in correct order", func(t *testing.T) {
		ctx := setup(t)

		// Use a small chunk size to force multiple chunks with fewer transactions
		ctx.settings.Asset.SubtreeDataStreamingChunkSize = 10
		ctx.settings.Asset.SubtreeDataStreamingConcurrency = 4

		// Create 63 transactions (64 total with coinbase = power of 2, will create 7 chunks of varying sizes)
		numTxs := 63
		txs := make([]*bt.Tx, numTxs+1) // +1 for coinbase
		txs[0] = coinbase

		for i := 1; i <= numTxs; i++ {
			// Create unique transactions with incrementing version numbers for easy verification
			tx := &bt.Tx{
				Version:  uint32(i), //nolint:gosec
				LockTime: uint32(i), //nolint:gosec
				Inputs:   []*bt.Input{},
				Outputs:  []*bt.Output{},
			}
			txs[i] = tx
		}

		// Create block and subtree with all transactions
		testParams := blockInfo{
			version:           1,
			bits:              "2000ffff",
			previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
			height:            1,
			nonce:             2083236893,
			timestamp:         uint32(time.Now().Unix()), //nolint:gosec
			txs:               txs,
		}

		block, subtree := newBlockWithCorrectMerkleRoot(ctx, t, testParams)

		// Mock blockchain client to return our block
		blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
		blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		// Add all non-coinbase transactions to the UTXO store
		for i := 1; i < len(txs); i++ {
			_, err := ctx.repo.UtxoStore.Create(context.Background(), txs[i], testParams.height)
			require.NoError(t, err)
		}

		// Store the subtree in the subtree store
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Get the legacy block reader (this triggers writeTransactionsViaSubtreeStoreStreaming)
		r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
		require.NoError(t, err)

		// Read and skip the header
		buf := make([]byte, 4096)

		// magic (4) + size (4)
		_, err = io.ReadFull(r, buf[:8])
		require.NoError(t, err)

		// block header (80 bytes)
		_, err = io.ReadFull(r, buf[:80])
		require.NoError(t, err)

		// transaction count varint
		_, err = r.Read(buf[:10])
		require.NoError(t, err)
		txCount, _ := bt.NewVarIntFromBytes(buf[:10])
		assert.Equal(t, uint64(len(txs)), uint64(txCount))

		// Read all transaction data
		allTxData, err := io.ReadAll(r)
		require.ErrorIs(t, err, io.ErrClosedPipe) // Pipe closes after all data

		// Parse transactions from the stream and verify order
		offset := 0
		for i := 0; i < len(txs); i++ {
			parsedTx, size, parseErr := bt.NewTxFromStream(allTxData[offset:])
			require.NoError(t, parseErr, "failed to parse transaction %d", i)
			require.NotNil(t, parsedTx)

			// Verify this is the correct transaction in sequence
			if i == 0 {
				// Coinbase
				assert.Equal(t, coinbase.TxID(), parsedTx.TxID(), "coinbase mismatch at position 0")
			} else {
				// Regular transaction - verify by version number which we set sequentially
				assert.Equal(t, uint32(i), parsedTx.Version, "transaction at position %d has wrong version (expected %d, got %d)", i, i, parsedTx.Version)
				assert.Equal(t, txs[i].TxID(), parsedTx.TxID(), "transaction %d TxID mismatch", i)
			}

			offset += size
		}

		// Verify we consumed all data
		assert.Equal(t, len(allTxData), offset, "not all transaction data was consumed")
	})

	t.Run("handles single chunk correctly", func(t *testing.T) {
		ctx := setup(t)

		// Use chunk size larger than number of transactions
		ctx.settings.Asset.SubtreeDataStreamingChunkSize = 100
		ctx.settings.Asset.SubtreeDataStreamingConcurrency = 4

		// Create 7 transactions (8 total with coinbase = power of 2, all in one chunk)
		numTxs := 7
		txs := make([]*bt.Tx, numTxs+1)
		txs[0] = coinbase

		for i := 1; i <= numTxs; i++ {
			tx := &bt.Tx{
				Version:  uint32(i), //nolint:gosec
				LockTime: uint32(i), //nolint:gosec
				Inputs:   []*bt.Input{},
				Outputs:  []*bt.Output{},
			}
			txs[i] = tx
		}

		testParams := blockInfo{
			version:           1,
			bits:              "2000ffff",
			previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
			height:            1,
			nonce:             2083236893,
			timestamp:         uint32(time.Now().Unix()), //nolint:gosec
			txs:               txs,
		}

		block, subtree := newBlockWithCorrectMerkleRoot(ctx, t, testParams)

		blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
		blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		for i := 1; i < len(txs); i++ {
			_, err := ctx.repo.UtxoStore.Create(context.Background(), txs[i], testParams.height)
			require.NoError(t, err)
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
		require.NoError(t, err)

		buf := make([]byte, 4096)

		// Skip header
		_, err = io.ReadFull(r, buf[:8])
		require.NoError(t, err)
		_, err = io.ReadFull(r, buf[:80])
		require.NoError(t, err)
		_, err = r.Read(buf[:10])
		require.NoError(t, err)

		allTxData, err := io.ReadAll(r)
		require.ErrorIs(t, err, io.ErrClosedPipe)

		// Verify all transactions are present and in order
		offset := 0
		for i := 0; i < len(txs); i++ {
			parsedTx, size, parseErr := bt.NewTxFromStream(allTxData[offset:])
			require.NoError(t, parseErr)

			if i == 0 {
				assert.Equal(t, coinbase.TxID(), parsedTx.TxID())
			} else {
				assert.Equal(t, uint32(i), parsedTx.Version)
			}

			offset += size
		}
	})

	t.Run("handles exact chunk boundary", func(t *testing.T) {
		ctx := setup(t)

		// Create exactly 2 chunks worth of transactions (16 per chunk = 32 total with coinbase)
		ctx.settings.Asset.SubtreeDataStreamingChunkSize = 16
		ctx.settings.Asset.SubtreeDataStreamingConcurrency = 2

		numTxs := 31 // 32 total with coinbase = power of 2, exactly 2 chunks of 16
		txs := make([]*bt.Tx, numTxs+1)
		txs[0] = coinbase

		for i := 1; i <= numTxs; i++ {
			tx := &bt.Tx{
				Version:  uint32(i), //nolint:gosec
				LockTime: uint32(i), //nolint:gosec
				Inputs:   []*bt.Input{},
				Outputs:  []*bt.Output{},
			}
			txs[i] = tx
		}

		testParams := blockInfo{
			version:           1,
			bits:              "2000ffff",
			previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
			height:            1,
			nonce:             2083236893,
			timestamp:         uint32(time.Now().Unix()), //nolint:gosec
			txs:               txs,
		}

		block, subtree := newBlockWithCorrectMerkleRoot(ctx, t, testParams)

		blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
		blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		for i := 1; i < len(txs); i++ {
			_, err := ctx.repo.UtxoStore.Create(context.Background(), txs[i], testParams.height)
			require.NoError(t, err)
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
		require.NoError(t, err)

		buf := make([]byte, 4096)
		_, err = io.ReadFull(r, buf[:8])
		require.NoError(t, err)
		_, err = io.ReadFull(r, buf[:80])
		require.NoError(t, err)
		_, err = r.Read(buf[:10])
		require.NoError(t, err)

		allTxData, err := io.ReadAll(r)
		require.ErrorIs(t, err, io.ErrClosedPipe)

		offset := 0
		for i := 0; i < len(txs); i++ {
			parsedTx, size, parseErr := bt.NewTxFromStream(allTxData[offset:])
			require.NoError(t, parseErr)

			if i == 0 {
				assert.Equal(t, coinbase.TxID(), parsedTx.TxID())
			} else {
				assert.Equal(t, uint32(i), parsedTx.Version, "transaction %d has wrong version", i)
			}

			offset += size
		}
	})
}

// TestGetLegacyBlockNoDuplication verifies that the coinbase transaction is never
// duplicated in the legacy block response, regardless of the code path taken.
func TestGetLegacyBlockNoDuplication(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	// Test all possible scenarios where duplication could occur
	testCases := []struct {
		name             string
		setupSubtreeData bool
		setupUTXOStore   bool
		multipleSubtrees bool
		description      string
	}{
		{
			name:             "subtree_data_exists",
			setupSubtreeData: true,
			setupUTXOStore:   false,
			multipleSubtrees: false,
			description:      "Subtree data exists - should write coinbase once then data from subtree",
		},
		{
			name:             "fallback_to_utxo_store",
			setupSubtreeData: false,
			setupUTXOStore:   true,
			multipleSubtrees: false,
			description:      "No subtree data - should write coinbase once then fetch from UTXO store",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create block with transactions
			txs := []*bt.Tx{coinbase, tx1}
			blockParams := blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            1,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               txs,
			}

			block, subtree := newBlock(ctx, t, blockParams)
			t.Logf("Test: %s", tc.description)

			// Mock blockchain client
			blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
			blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

			// Setup subtree data if needed
			if tc.setupSubtreeData {
				subtreeData := subtreepkg.NewSubtreeData(subtree)
				// Only add non-coinbase transactions (subtree data doesn't contain coinbase)
				for i := 1; i < len(txs); i++ {
					require.NoError(t, subtreeData.AddTx(txs[i], i))
				}
				subtreeDataBytes, err := subtreeData.Serialize()
				require.NoError(t, err)
				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
				require.NoError(t, err)
			}

			// Setup UTXO store if needed (for fallback path)
			if tc.setupUTXOStore {
				for i := 1; i < len(txs); i++ {
					_, err := ctx.repo.UtxoStore.Create(context.Background(), txs[i], blockParams.height)
					require.NoError(t, err)
				}
				// Also need the subtree itself for fallback
				subtreeBytes, err := subtree.Serialize()
				require.NoError(t, err)
				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
				require.NoError(t, err)
			}

			// Get legacy block reader
			r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
			require.NoError(t, err)

			// Read all block data
			blockData, err := io.ReadAll(r)
			if err != nil && err != io.ErrClosedPipe {
				require.NoError(t, err)
			}

			// Parse and count coinbase occurrences
			reader := bytes.NewReader(blockData)

			// Skip header
			headerBytes := make([]byte, 80)
			_, err = io.ReadFull(reader, headerBytes)
			require.NoError(t, err)

			// Read transaction count
			varIntBytes := make([]byte, 9)
			n, err := reader.Read(varIntBytes)
			require.NoError(t, err)
			txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
			_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
			require.NoError(t, err)

			// Read all transactions and count coinbase occurrences
			coinbaseHash := coinbase.TxIDChainHash().String()
			coinbaseCount := 0
			txHashes := make([]string, 0)

			for i := uint64(0); i < uint64(txCount); i++ {
				tx := &bt.Tx{}
				_, err = tx.ReadFrom(reader)
				require.NoError(t, err)

				hash := tx.TxIDChainHash().String()
				txHashes = append(txHashes, hash)

				if hash == coinbaseHash {
					coinbaseCount++
					t.Logf("  Found coinbase at position %d", i)
				}
			}

			// Verify coinbase appears exactly once
			assert.Equal(t, 1, coinbaseCount,
				"Coinbase should appear exactly once, but appeared %d times. Transactions: %v",
				coinbaseCount, txHashes)

			// Verify coinbase is first
			assert.Equal(t, coinbaseHash, txHashes[0],
				"Coinbase should be the first transaction")

			t.Logf("✓ Coinbase appears exactly once at position 0")
		})
	}
}

// TestGetLegacyBlockWireFormat verifies that GetLegacyBlockReader correctly streams
// blocks in wire format with proper transaction ordering and no duplication.
func TestGetLegacyBlockWireFormat(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	testCases := []struct {
		name           string
		txs            []*bt.Tx
		expectSubtrees bool
		description    string
	}{
		{
			name:           "coinbase_only_block",
			txs:            []*bt.Tx{coinbase},
			expectSubtrees: false,
			description:    "Block with only coinbase transaction",
		},
		{
			name:           "block_with_two_transactions",
			txs:            []*bt.Tx{coinbase, tx1},
			expectSubtrees: true,
			description:    "Block with coinbase and one regular transaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test block
			blockParams := blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            1,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               tc.txs,
			}

			// Create block using existing helper
			block, subtree := newBlock(ctx, t, blockParams)

			// Log expected transaction info
			t.Logf("Test case: %s", tc.description)
			t.Logf("Expected %d transactions:", len(tc.txs))
			for i, tx := range tc.txs {
				t.Logf("  Tx[%d]: hash=%s, size=%d", i, tx.TxIDChainHash().String(), tx.Size())
			}

			// Mock blockchain client
			blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
			blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

			// Store subtree data if needed
			if tc.expectSubtrees && len(block.Subtrees) > 0 {
				subtreeData := subtreepkg.NewSubtreeData(subtree)
				for i, tx := range tc.txs {
					err := subtreeData.AddTx(tx, i)
					require.NoError(t, err)
					t.Logf("Added tx[%d] to subtree data: %s (coinbase=%v)", i, tx.TxIDChainHash().String(), i == 0)
				}
				subtreeDataBytes, err := subtreeData.Serialize()
				require.NoError(t, err)

				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
				require.NoError(t, err)
				t.Logf("Stored subtree data: hash=%s, size=%d bytes", subtree.RootHash().String(), len(subtreeDataBytes))

				// Verify it was stored correctly
				exists, err := ctx.repo.SubtreeStore.Exists(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				require.True(t, exists, "Subtree data should exist after storing")

				// Try to read it back to verify format
				subtreeDataReader, err := ctx.repo.SubtreeStore.GetIoReader(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				defer subtreeDataReader.Close()

				// Try reading transactions from it like GetLegacyBlockReader does
				bufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024)
				txCount := 0
				for {
					tx := &bt.Tx{}
					if _, err = tx.ReadFrom(bufferedReader); err != nil {
						if err == io.EOF {
							break
						}
						t.Errorf("Error reading transaction %d from subtree data: %v", txCount, err)
						break
					}
					t.Logf("  Subtree data contains tx[%d]: %s (size=%d)", txCount, tx.TxIDChainHash().String(), tx.Size())
					txCount++
				}
				t.Logf("  Total transactions in subtree data: %d (expected %d)", txCount, len(tc.txs))
			}

			// Get legacy block reader in wire format
			r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
			require.NoError(t, err)

			// Read all block data
			blockData, err := io.ReadAll(r)
			if err != nil && err != io.ErrClosedPipe {
				require.NoError(t, err)
			}

			t.Logf("Read %d bytes of block data", len(blockData))
			reader := bytes.NewReader(blockData)

			// Parse and verify block header
			headerBytes := make([]byte, 80)
			_, err = io.ReadFull(reader, headerBytes)
			require.NoError(t, err, "Failed to read block header")

			// Parse transaction count
			varIntBytes := make([]byte, 9)
			n, err := reader.Read(varIntBytes)
			require.NoError(t, err)
			txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
			_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
			require.NoError(t, err)

			t.Logf("Block header reports %d transactions", txCount)
			require.Equal(t, uint64(len(tc.txs)), uint64(txCount), "Transaction count mismatch")

			// Read and verify each transaction
			actualTxs := make([]*bt.Tx, 0)
			for i := uint64(0); i < uint64(txCount); i++ {
				tx := &bt.Tx{}
				_, err = tx.ReadFrom(reader)
				require.NoError(t, err, "Failed to read transaction %d", i)
				actualTxs = append(actualTxs, tx)

				txHash := tx.TxIDChainHash().String()
				t.Logf("Read Tx[%d]: hash=%s, size=%d", i, txHash, tx.Size())

				// Verify this matches the expected transaction
				expectedHash := tc.txs[i].TxIDChainHash().String()
				assert.Equal(t, expectedHash, txHash,
					"Transaction %d hash mismatch: expected %s, got %s", i, expectedHash, txHash)
			}

			// Verify no extra data
			remainingBytes := reader.Len()
			assert.Equal(t, 0, remainingBytes, "Unexpected %d bytes remaining after reading all transactions", remainingBytes)

			// Verify no duplicate transactions
			seenHashes := make(map[string]bool)
			for i, tx := range actualTxs {
				hash := tx.TxIDChainHash().String()
				assert.False(t, seenHashes[hash], "Duplicate transaction found at position %d: %s", i, hash)
				seenHashes[hash] = true
			}

			t.Logf("✓ Successfully verified %d transactions in correct order with no duplicates", len(actualTxs))
		})
	}
}

// TestLegacyBlockMerkleRootValidation validates that the merkle root calculated from
// all transactions in a legacy block matches the merkle root in the block header.
// This comprehensive test ensures merkle roots are correctly calculated for various block types.
func TestLegacyBlockMerkleRootValidation(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	testCases := []struct {
		name        string
		txs         []*bt.Tx
		description string
	}{
		{
			name:        "coinbase_only",
			txs:         []*bt.Tx{coinbase},
			description: "Coinbase-only block: merkle root equals coinbase hash",
		},
		{
			name:        "two_transactions",
			txs:         []*bt.Tx{coinbase, tx1},
			description: "Two transactions: merkle root from coinbase and tx1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Test case: %s", tc.description)

			// Create block parameters
			blockParams := blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            100,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               tc.txs,
			}

			// Create block with correct merkle root
			block, subtree := createBlockWithCorrectMerkleRoot(ctx, t, blockParams)

			// Mock blockchain client
			blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
			blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

			// Setup subtree data if block has subtrees
			if len(block.Subtrees) > 0 {
				// Create subtree data (contains only non-coinbase transactions)
				subtreeData := subtreepkg.NewSubtreeData(subtree)
				for i := 1; i < len(tc.txs); i++ {
					require.NoError(t, subtreeData.AddTx(tc.txs[i], i))
				}

				subtreeDataBytes, err := subtreeData.Serialize()
				require.NoError(t, err)

				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
				require.NoError(t, err)
			}

			// Get legacy block reader
			r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
			require.NoError(t, err)

			// Read all block data
			blockData, err := io.ReadAll(r)
			if err != nil && err != io.ErrClosedPipe {
				require.NoError(t, err)
			}

			// Parse the block
			reader := bytes.NewReader(blockData)

			// Read and parse header
			headerBytes := make([]byte, 80)
			_, err = io.ReadFull(reader, headerBytes)
			require.NoError(t, err)

			// Extract merkle root from header (bytes 36-68)
			merkleRootBytes := headerBytes[36:68]
			headerMerkleRoot, err := chainhash.NewHash(merkleRootBytes)
			require.NoError(t, err)
			t.Logf("Header merkle root: %s", headerMerkleRoot.String())

			// Read transaction count
			varIntBytes := make([]byte, 9)
			n, err := reader.Read(varIntBytes)
			require.NoError(t, err)
			txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
			_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
			require.NoError(t, err)

			// Read all transactions
			txHashes := make([]*chainhash.Hash, 0, txCount)
			for i := uint64(0); i < uint64(txCount); i++ {
				tx := &bt.Tx{}
				_, err = tx.ReadFrom(reader)
				require.NoError(t, err)
				txHashes = append(txHashes, tx.TxIDChainHash())
				t.Logf("  Tx[%d]: %s", i, tx.TxIDChainHash().String())
			}

			// Calculate merkle root from transactions
			calculatedRoot := calculateMerkleRoot(txHashes)
			t.Logf("Calculated merkle root: %s", calculatedRoot.String())

			// Validate they match
			assert.Equal(t, headerMerkleRoot.String(), calculatedRoot.String(),
				"Merkle root mismatch for %s", tc.description)

			// Additional validation: transaction count
			assert.Equal(t, len(tc.txs), len(txHashes),
				"Transaction count mismatch: expected %d, got %d", len(tc.txs), len(txHashes))

			t.Logf("✓ Merkle root validation passed for block with %d transactions", len(txHashes))
		})
	}
}

// createBlockWithCorrectMerkleRoot creates a block with the properly calculated merkle root
func createBlockWithCorrectMerkleRoot(_ *testContext, t *testing.T, b blockInfo) (*model.Block, *subtreepkg.Subtree) {
	if len(b.txs) == 0 {
		panic("no transactions provided")
	}

	// Create subtree with power-of-two size
	subtreeSize := 1
	for subtreeSize < len(b.txs) {
		subtreeSize *= 2
	}
	subtree, err := subtreepkg.NewTreeByLeafCount(subtreeSize)
	require.NoError(t, err)

	// Add all transactions to subtree
	txHashes := make([]*chainhash.Hash, len(b.txs))
	for i, tx := range b.txs {
		txHashes[i] = tx.TxIDChainHash()
		if i == 0 {
			require.NoError(t, subtree.AddCoinbaseNode())
		} else {
			require.NoError(t, subtree.AddNode(*txHashes[i], 100, 0))
		}
	}

	// Calculate the correct merkle root
	merkleRoot := calculateMerkleRoot(txHashes)

	nBits, _ := model.NewNBitFromString(b.bits)
	hashPrevBlock, _ := chainhash.NewHashFromStr(b.previousBlockHash)

	// Only add subtrees if we have more than just coinbase
	subtreeHashes := make([]*chainhash.Hash, 0)
	if len(b.txs) > 1 {
		subtreeHashes = append(subtreeHashes, subtree.RootHash())
	}

	blockHeader := &model.BlockHeader{
		Version:        b.version,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      b.timestamp,
		Bits:           *nBits,
		Nonce:          b.nonce,
	}

	// Calculate block size
	var sizeInBytes uint64
	for _, tx := range b.txs {
		sizeInBytes += uint64(tx.Size())
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       b.txs[0],
		TransactionCount: uint64(len(b.txs)),
		Subtrees:         subtreeHashes,
		Height:           b.height,
		SizeInBytes:      sizeInBytes,
	}

	return block, subtree
}

// calculateMerkleRoot calculates the merkle root from transaction hashes
// following the Bitcoin protocol specification
func calculateMerkleRoot(txHashes []*chainhash.Hash) *chainhash.Hash {
	if len(txHashes) == 0 {
		return &chainhash.Hash{}
	}
	if len(txHashes) == 1 {
		return txHashes[0]
	}

	// Build merkle tree level by level
	currentLevel := make([]*chainhash.Hash, len(txHashes))
	copy(currentLevel, txHashes)

	for len(currentLevel) > 1 {
		// If odd number of elements, duplicate the last one
		if len(currentLevel)%2 != 0 {
			currentLevel = append(currentLevel, currentLevel[len(currentLevel)-1])
		}

		// Calculate next level
		nextLevel := make([]*chainhash.Hash, 0, len(currentLevel)/2)
		for i := 0; i < len(currentLevel); i += 2 {
			// Concatenate and double-hash the pair
			combined := append(currentLevel[i][:], currentLevel[i+1][:]...)
			hash := chainhash.DoubleHashH(combined)
			nextLevel = append(nextLevel, &hash)
		}

		currentLevel = nextLevel
	}

	return currentLevel[0]
}

// TestMalformedSubtreeDataWithCoinbase verifies that GetLegacyBlockReader correctly handles
// subtree data that incorrectly contains the coinbase transaction (from older buggy code).
// It should not duplicate the coinbase even when subtree data is malformed.
func TestMalformedSubtreeDataWithCoinbase(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	// Create block with multiple transactions
	blockParams := blockInfo{
		version:           1,
		bits:              "2000ffff",
		previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
		height:            100,
		nonce:             2083236893,
		timestamp:         uint32(time.Now().Unix()),
		txs:               []*bt.Tx{coinbase, tx1},
	}

	block, subtree := createBlockWithCorrectMerkleRoot(ctx, t, blockParams)

	// Mock blockchain client
	blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// Create MALFORMED subtree data that includes the coinbase (simulating older buggy code)
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	// Add coinbase as first transaction (this is the bug we're handling)
	require.NoError(t, subtreeData.AddTx(coinbase, 0))
	// Add regular transaction
	require.NoError(t, subtreeData.AddTx(tx1, 1))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Store the malformed subtree data
	err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// Get legacy block reader
	r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
	require.NoError(t, err)

	// Read all block data
	blockData, err := io.ReadAll(r)
	if err != nil && err != io.ErrClosedPipe {
		require.NoError(t, err)
	}

	// Parse the block to count transactions
	reader := bytes.NewReader(blockData)

	// Skip header (80 bytes)
	headerBytes := make([]byte, 80)
	_, err = io.ReadFull(reader, headerBytes)
	require.NoError(t, err)

	// Read transaction count
	varIntBytes := make([]byte, 9)
	n, err := reader.Read(varIntBytes)
	require.NoError(t, err)
	txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
	_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
	require.NoError(t, err)

	// Read and verify all transactions
	txHashes := make([]string, 0, txCount)
	for i := uint64(0); i < uint64(txCount); i++ {
		tx := &bt.Tx{}
		_, err = tx.ReadFrom(reader)
		require.NoError(t, err)
		txHashes = append(txHashes, tx.TxIDChainHash().String())
		t.Logf("Tx[%d]: %s", i, tx.TxIDChainHash().String())
	}

	// Verify we have exactly 2 transactions (no duplication)
	assert.Equal(t, 2, len(txHashes), "Should have exactly 2 transactions")

	// Verify coinbase is first and only appears once
	coinbaseHash := coinbase.TxIDChainHash().String()
	assert.Equal(t, coinbaseHash, txHashes[0], "First transaction should be coinbase")

	// Verify tx1 is second
	tx1Hash := tx1.TxIDChainHash().String()
	assert.Equal(t, tx1Hash, txHashes[1], "Second transaction should be tx1")

	// Verify no duplicates
	assert.NotEqual(t, txHashes[0], txHashes[1], "Transactions should not be duplicated")

	t.Logf("✓ Malformed subtree data handled correctly - no coinbase duplication")
}

// TestBlockVsLegacyEndpointConsistency validates that the merkle root calculated from
// transactions is identical whether using the /block endpoint (structured data) or
// the /block_legacy endpoint (wire format streaming). This test reproduces and validates
// the fix for the original issue where these endpoints produced different merkle roots.
func TestBlockVsLegacyEndpointConsistency(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)

	testCases := []struct {
		name        string
		txs         []*bt.Tx
		description string
	}{
		{
			name:        "single_transaction_block",
			txs:         []*bt.Tx{coinbase},
			description: "Coinbase-only block",
		},
		{
			name:        "multi_transaction_block",
			txs:         []*bt.Tx{coinbase, tx1},
			description: "Block with multiple transactions (reproduces original bug)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing consistency for: %s", tc.description)

			// Create block
			blockParams := blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            100,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               tc.txs,
			}

			block, subtree := createBlockWithCorrectMerkleRoot(ctx, t, blockParams)
			t.Logf("  Created block with merkle root in header: %s", block.Header.HashMerkleRoot.String())
			t.Logf("  Block.TransactionCount: %d", block.TransactionCount)

			// Mock blockchain client
			blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
			// Only the /block_legacy endpoint calls GetBlock via GetLegacyBlockReader
			blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

			// Setup subtree data if needed
			if len(block.Subtrees) > 0 {
				subtreeData := subtreepkg.NewSubtreeData(subtree)
				for i := 1; i < len(tc.txs); i++ {
					require.NoError(t, subtreeData.AddTx(tc.txs[i], i))
				}
				subtreeDataBytes, err := subtreeData.Serialize()
				require.NoError(t, err)
				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
				require.NoError(t, err)
			}

			// =================================================================
			// SIMULATE /block ENDPOINT (structured JSON response)
			// =================================================================
			t.Logf("\n=== Simulating /block endpoint (structured data) ===")

			// The /block endpoint would return structured data with:
			// - header with merkle root
			// - coinbase transaction
			// - subtree information
			blockEndpointTxs := make([]*chainhash.Hash, 0)

			// In the /block response, we get:
			// 1. The coinbase from block.CoinbaseTx
			blockEndpointTxs = append(blockEndpointTxs, block.CoinbaseTx.TxIDChainHash())
			t.Logf("  /block coinbase: %s", block.CoinbaseTx.TxIDChainHash().String())

			// 2. For each subtree, we would fetch and parse transactions
			if len(block.Subtrees) > 0 {
				// Simulate fetching subtree data
				for _, subtreeHash := range block.Subtrees {
					subtreeDataReader, err := ctx.repo.SubtreeStore.GetIoReader(context.Background(),
						subtreeHash[:], fileformat.FileTypeSubtreeData)
					require.NoError(t, err)
					defer subtreeDataReader.Close()

					// Read transactions from subtree data
					bufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024)
					for {
						tx := &bt.Tx{}
						if _, err = tx.ReadFrom(bufferedReader); err != nil {
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
						}
						blockEndpointTxs = append(blockEndpointTxs, tx.TxIDChainHash())
						t.Logf("  /block subtree tx: %s", tx.TxIDChainHash().String())
					}
				}
			}

			blockEndpointMerkleRoot := calculateMerkleRoot(blockEndpointTxs)
			t.Logf("  /block calculated merkle root: %s", blockEndpointMerkleRoot.String())

			// =================================================================
			// SIMULATE /block_legacy ENDPOINT (wire format streaming)
			// =================================================================
			t.Logf("\n=== Simulating /block_legacy endpoint (wire format) ===")

			// Get legacy block reader (this is what /block_legacy uses)
			r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
			require.NoError(t, err)

			// Read the wire format data
			blockData, err := io.ReadAll(r)
			if err != nil && err != io.ErrClosedPipe {
				require.NoError(t, err)
			}

			// Parse block from wire format
			reader := bytes.NewReader(blockData)

			// Skip header (80 bytes)
			headerBytes := make([]byte, 80)
			_, err = io.ReadFull(reader, headerBytes)
			require.NoError(t, err)

			// Extract merkle root from header
			merkleRootBytes := headerBytes[36:68]
			headerMerkleRoot, err := chainhash.NewHash(merkleRootBytes)
			require.NoError(t, err)

			// Read transaction count
			varIntBytes := make([]byte, 9)
			n, err := reader.Read(varIntBytes)
			require.NoError(t, err)
			txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
			_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
			require.NoError(t, err)

			t.Logf("  /block_legacy reports %d transactions in header", txCount)

			// Read all transactions from wire format
			legacyEndpointTxs := make([]*chainhash.Hash, 0, txCount)
			for i := uint64(0); i < uint64(txCount); i++ {
				tx := &bt.Tx{}
				_, err = tx.ReadFrom(reader)
				if err != nil {
					t.Logf("  /block_legacy: Failed to read tx[%d]: %v", i, err)
					break
				}
				legacyEndpointTxs = append(legacyEndpointTxs, tx.TxIDChainHash())
				t.Logf("  /block_legacy tx[%d]: %s", i, tx.TxIDChainHash().String())
			}

			t.Logf("  /block_legacy collected %d transaction hashes", len(legacyEndpointTxs))
			legacyEndpointMerkleRoot := calculateMerkleRoot(legacyEndpointTxs)
			t.Logf("  /block_legacy calculated merkle root: %s", legacyEndpointMerkleRoot.String())

			// =================================================================
			// VERIFY CONSISTENCY
			// =================================================================
			t.Logf("\n=== Verification ===")
			t.Logf("  Header merkle root:        %s", headerMerkleRoot.String())
			t.Logf("  /block calculated root:    %s", blockEndpointMerkleRoot.String())
			t.Logf("  /block_legacy calculated:  %s", legacyEndpointMerkleRoot.String())

			// All three should match
			assert.Equal(t, headerMerkleRoot.String(), blockEndpointMerkleRoot.String(),
				"/block endpoint merkle root should match header")
			assert.Equal(t, headerMerkleRoot.String(), legacyEndpointMerkleRoot.String(),
				"/block_legacy endpoint merkle root should match header")
			assert.Equal(t, blockEndpointMerkleRoot.String(), legacyEndpointMerkleRoot.String(),
				"Both endpoints should produce identical merkle roots")

			// Verify transaction counts match
			assert.Equal(t, len(tc.txs), len(blockEndpointTxs),
				"/block endpoint should return correct transaction count")
			assert.Equal(t, len(tc.txs), len(legacyEndpointTxs),
				"/block_legacy endpoint should return correct transaction count")

			// Verify transaction order matches
			for i := 0; i < len(tc.txs); i++ {
				if i < len(blockEndpointTxs) && i < len(legacyEndpointTxs) {
					assert.Equal(t, blockEndpointTxs[i].String(), legacyEndpointTxs[i].String(),
						"Transaction %d should match between endpoints", i)
				}
			}

			t.Logf("✓ Both endpoints produce identical merkle roots and transaction sets")
		})
	}
}

// TestGetLegacyBlockMerkleRootValidation validates that the merkle root calculated from
// transactions in a legacy block matches the merkle root in the block header.
// This ensures the coinbase duplication bug is fixed and merkle roots are correct.
func TestGetLegacyBlockMerkleRootValidation(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := setup(t)
	ctx.logger.Debugf("test merkle root validation")

	testCases := []struct {
		name        string
		blockParams blockInfo
		description string
	}{
		{
			name: "block_with_two_transactions",
			blockParams: blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            1,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               []*bt.Tx{coinbase, tx1},
			},
			description: "Block with coinbase and one regular transaction",
		},
		{
			name: "block_with_only_coinbase",
			blockParams: blockInfo{
				version:           1,
				bits:              "2000ffff",
				previousBlockHash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
				height:            1,
				nonce:             2083236893,
				timestamp:         uint32(time.Now().Unix()),
				txs:               []*bt.Tx{coinbase},
			},
			description: "Block with only coinbase transaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create block with proper merkle root calculation
			block, subtree := newBlockWithCorrectMerkleRoot(ctx, t, tc.blockParams)
			t.Logf("Block CoinbaseTx hash: %s", block.CoinbaseTx.TxIDChainHash().String())
			t.Logf("Test coinbase hash: %s", coinbase.TxIDChainHash().String())
			t.Logf("Test tx1 hash: %s", tx1.TxIDChainHash().String())

			// Mock the blockchain client
			blockchainClientMock := ctx.repo.BlockchainClient.(*blockchain.Mock)
			blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

			// Log block subtrees for debugging
			t.Logf("Block has %d subtrees", len(block.Subtrees))
			for i, sh := range block.Subtrees {
				t.Logf("Subtree %d hash: %s", i, sh.String())
			}

			// Store subtree data only if block has subtrees
			if len(block.Subtrees) > 0 {
				subtreeData := subtreepkg.NewSubtreeData(subtree)
				for i, tx := range tc.blockParams.txs {
					require.NoError(t, subtreeData.AddTx(tx, i))
					t.Logf("Added tx %d to subtree data: %s", i, tx.TxIDChainHash().String())
				}
				subtreeDataBytes, err := subtreeData.Serialize()
				require.NoError(t, err)
				t.Logf("Subtree data size: %d bytes", len(subtreeDataBytes))
				err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
				require.NoError(t, err)
				t.Logf("Stored subtree data with hash %s containing %d transactions", subtree.RootHash().String(), len(tc.blockParams.txs))

				// Verify we can retrieve the subtree data
				exists, err := ctx.repo.SubtreeStore.Exists(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				require.True(t, exists, "Subtree data should exist after storing")
			} else {
				t.Logf("No subtrees in block - coinbase only")
			}

			// Get the legacy block reader (wire format)
			r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{}, true)
			require.NoError(t, err)

			// Read all data from the pipe reader first
			blockData, err := io.ReadAll(r)
			// io.ErrClosedPipe is expected when the writer closes after all data is written
			if err != nil && err != io.ErrClosedPipe {
				require.NoError(t, err)
			}

			// Ensure we have data
			require.NotEmpty(t, blockData, "Block data should not be empty")
			t.Logf("Read %d bytes of block data", len(blockData))

			// Log first few bytes after header to see what transaction we're getting
			if len(blockData) > 81 {
				t.Logf("First 20 bytes after header+varint: %x", blockData[81:101])
			}

			// For debugging: Let's just ensure we get the right number of bytes
			// Header (80) + varint (1 for count 2) + coinbase (176) + tx1 (152) = 409
			// But with subtree data, we should get both transactions from the subtree

			// Expected sizes:
			// - Coinbase size from test data is 176 bytes
			// - Tx1 size should be about 233 bytes
			// Total should be: 80 + 1 + 176 + 233 = 490 bytes

			if tc.name == "block_with_two_transactions" {
				// For two transactions, we expect more data
				require.Greater(t, len(blockData), 409, "Should have both transactions, not just one")
			}

			// Parse block header and transactions manually
			reader := bytes.NewReader(blockData)

			// Read block header (80 bytes)
			headerBytes := make([]byte, 80)
			_, err = io.ReadFull(reader, headerBytes)
			require.NoError(t, err)

			// Parse merkle root from header (bytes 36-67)
			merkleRootBytes := headerBytes[36:68]
			expectedMerkleRoot, err := chainhash.NewHash(merkleRootBytes)
			require.NoError(t, err)

			// Read transaction count varint
			varIntBytes := make([]byte, 9) // Max varint size
			n, err := reader.Read(varIntBytes)
			require.NoError(t, err)
			txCount, bytesRead := bt.NewVarIntFromBytes(varIntBytes[:n])
			// Seek back to correct position after reading varint
			_, err = reader.Seek(int64(-n+bytesRead), io.SeekCurrent)
			require.NoError(t, err)
			t.Logf("Block has %d transactions", txCount)

			// Verify transaction count matches expected
			require.Equal(t, uint64(len(tc.blockParams.txs)), uint64(txCount),
				"Transaction count in block doesn't match test data")

			// Read all transactions
			txHashes := make([]*chainhash.Hash, 0)
			for i := uint64(0); i < uint64(txCount); i++ {
				tx := &bt.Tx{}
				bytesBeforeRead := reader.Len()
				_, err = tx.ReadFrom(reader)
				if err == io.EOF && i < uint64(txCount) {
					t.Errorf("Hit EOF trying to read transaction %d of %d (bytes before read: %d)",
						i, txCount, bytesBeforeRead)
					break
				}
				require.NoError(t, err, "Failed to read transaction %d", i)
				txHashes = append(txHashes, tx.TxIDChainHash())
				t.Logf("Transaction %d hash: %s", i, txHashes[i].String())
			}

			// Calculate merkle root from transactions
			calculatedMerkleRoot := buildMerkleRootFromHashes(txHashes)

			// Validate the merkle root matches
			assert.Equal(t, expectedMerkleRoot.String(), calculatedMerkleRoot.String(),
				"%s: Merkle root mismatch. Expected %s, got %s",
				tc.description, expectedMerkleRoot, calculatedMerkleRoot)

			// Also verify transaction count
			assert.Equal(t, len(tc.blockParams.txs), int(txCount),
				"Transaction count mismatch")

			// Verify no duplicate coinbase transactions
			if len(txHashes) > 1 {
				firstTxHash := txHashes[0]
				for i := 1; i < len(txHashes); i++ {
					assert.NotEqual(t, firstTxHash.String(), txHashes[i].String(),
						"Found duplicate coinbase transaction at position %d", i)
				}
			}
		})
	}
}

// newBlockWithCorrectMerkleRoot creates a block with the correct merkle root calculated from transactions
func newBlockWithCorrectMerkleRoot(_ *testContext, t *testing.T, b blockInfo) (*model.Block, *subtreepkg.Subtree) {
	if len(b.txs) == 0 {
		panic("no transactions provided")
	}

	// Create subtree
	subtree, err := subtreepkg.NewTreeByLeafCount(len(b.txs))
	require.NoError(t, err)

	// Add transactions to subtree and collect hashes for merkle root
	txHashes := make([]*chainhash.Hash, len(b.txs))
	for i, tx := range b.txs {
		txHash := tx.TxIDChainHash()
		txHashes[i] = txHash

		if i == 0 {
			require.NoError(t, subtree.AddCoinbaseNode())
		} else {
			require.NoError(t, subtree.AddNode(*txHash, 100, 0))
		}
	}

	// Calculate the correct merkle root from all transactions
	merkleRoot := buildMerkleRootFromHashes(txHashes)

	nBits, _ := model.NewNBitFromString(b.bits)
	hashPrevBlock, _ := chainhash.NewHashFromStr(b.previousBlockHash)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())

	blockHeader := &model.BlockHeader{
		Version:        b.version,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot, // Use the correctly calculated merkle root
		Timestamp:      b.timestamp,
		Bits:           *nBits,
		Nonce:          b.nonce,
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       b.txs[0],
		TransactionCount: uint64(len(b.txs)),
		Subtrees:         subtreeHashes,
		Height:           b.height,
		SizeInBytes:      calculateBlockSize(b.txs),
	}

	return block, subtree
}

// calculateBlockSize calculates the total size of all transactions in bytes
func calculateBlockSize(txs []*bt.Tx) uint64 {
	var size uint64
	for _, tx := range txs {
		size += uint64(tx.Size())
	}
	return size
}

// buildMerkleRootFromHashes builds the merkle root from transaction hashes
func buildMerkleRootFromHashes(txHashes []*chainhash.Hash) *chainhash.Hash {
	if len(txHashes) == 0 {
		return &chainhash.Hash{}
	}
	if len(txHashes) == 1 {
		return txHashes[0]
	}

	// Build merkle tree
	for len(txHashes) > 1 {
		if len(txHashes)%2 != 0 {
			txHashes = append(txHashes, txHashes[len(txHashes)-1])
		}

		nextLevel := make([]*chainhash.Hash, 0, len(txHashes)/2)
		for i := 0; i < len(txHashes); i += 2 {
			hash := chainhash.DoubleHashH(append(txHashes[i][:], txHashes[i+1][:]...))
			nextLevel = append(nextLevel, &hash)
		}
		txHashes = nextLevel
	}

	return txHashes[0]
}
