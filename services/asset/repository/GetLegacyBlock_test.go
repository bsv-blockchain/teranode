package repository

import (
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

	for i, tx := range params.txs {
		require.NoError(t, subtreeData.AddTx(tx, i))
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
	assert.Equal(t, block.CoinbaseTx.Size(), coinbaseSize)

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
