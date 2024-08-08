package repository

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	memory_blob "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	memory_utxo "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//func TestGetLegacyBlockWithBlockStore(t *testing.T) {
//	tracing.SetGlobalMockTracer()
//
//	var (
//		coinbase, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
//		tx1, _      = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
//	)
//
//	ctx := setup(t)
//	ctx.logger.Debugf("test")
//
//	params := blockInfo{
//		version:           1,
//		bits:              "2000ffff",
//		previousBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
//		height:            1,
//		nonce:             2083236893,
//		timestamp:         uint32(time.Now().Unix()),
//		txs:               []*bt.Tx{coinbase, tx1},
//	}
//
//	block, subtree := newBlock(ctx, t, params)
//	require.NoError(t, ctx.repo.BlockchainClient.AddBlock(context.Background(), block, "test_peerID"))
//
//	metaDatas := make([]*meta.Data, 0, len(params.txs))
//	for _, tx := range params.txs {
//		metaDatas = append(metaDatas, &meta.Data{
//			Tx: tx,
//		})
//	}
//
//	// create the block-store .subtree file
//	reader, writer := io.Pipe()
//	go blockpersister.WriteTxs(ctx.logger, writer, metaDatas, nil)
//	err := ctx.repo.BlockStore.SetFromReader(context.Background(), subtree.RootHash()[:], reader, options.WithFileExtension("subtree"))
//	require.NoError(t, err)
//
//	// should be able to get the block from the block-store (should NOT be looking at subtree-store)
//	r, err := ctx.repo.GetLegacyBlockReader(context.Background(), &chainhash.Hash{})
//	require.NoError(t, err)
//
//	bytes := make([]byte, 4096)
//
//	// magic, 4 bytes
//	n, err := io.ReadFull(r, bytes[:4])
//	assert.NoError(t, err)
//	assert.Equal(t, []byte{0xf9, 0xbe, 0xb4, 0xd9}, bytes[:n])
//
//	// size, 4 bytes
//	n, err = io.ReadFull(r, bytes[:4])
//	require.NoError(t, err)
//	size := binary.LittleEndian.Uint32(bytes[:n])
//	assert.Equal(t, uint32(block.SizeInBytes), size)
//
//	// version, 4 bytes
//	n, err = io.ReadFull(r, bytes[:4])
//	require.NoError(t, err)
//	version := binary.LittleEndian.Uint32(bytes[:n])
//	assert.Equal(t, block.Header.Version, version)
//
//	// hashPrevBlock, 32 bytes
//	n, err = io.ReadFull(r, bytes[:32])
//	require.NoError(t, err)
//	hashPrevBlock, _ := chainhash.NewHash(bytes[:n])
//	assert.Equal(t, block.Header.HashPrevBlock, hashPrevBlock)
//
//	// hashMerkleRoot, 32 bytes
//	n, err = io.ReadFull(r, bytes[:32])
//	require.NoError(t, err)
//	hashMerkleRoot, _ := chainhash.NewHash(bytes[:n])
//	assert.Equal(t, block.Header.HashMerkleRoot, hashMerkleRoot)
//
//	// timestamp, 4 bytes
//	n, err = io.ReadFull(r, bytes[:4])
//	require.NoError(t, err)
//	timestamp := binary.LittleEndian.Uint32(bytes[:n])
//	assert.Equal(t, block.Header.Timestamp, timestamp)
//
//	// difficulty, 4 bytes
//	n, err = io.ReadFull(r, bytes[:4])
//	require.NoError(t, err)
//	difficulty := model.NewNBitFromSlice(bytes[:n])
//	assert.Equal(t, block.Header.Bits, difficulty)
//
//	// nonce, 4 bytes
//	n, err = io.ReadFull(r, bytes[:4])
//	require.NoError(t, err)
//	nonce := binary.LittleEndian.Uint32(bytes[:n])
//	assert.Equal(t, block.Header.Nonce, nonce)
//
//	// transaction count, varint
//	n, err = r.Read(bytes[:1])
//	require.NoError(t, err)
//	transactionCount, _ := bt.NewVarIntFromBytes(bytes[:n])
//	assert.Equal(t, block.TransactionCount, uint64(transactionCount))
//
//	// read all tx data from the stream (we don't know how big each tx is)
//	bytes, err = io.ReadAll(r)
//	require.ErrorIs(t, err, io.ErrClosedPipe)
//
//	// check the coinbase transaction
//	coinbaseTx, coinbaseSize, err := bt.NewTxFromStream(bytes)
//	require.NoError(t, err)
//	require.NotNil(t, coinbaseTx)
//	assert.Equal(t, block.CoinbaseTx.Size(), coinbaseSize)
//
//	// check the 2nd tx
//	tx, txSize, err := bt.NewTxFromStream(bytes[coinbaseSize:])
//	require.NoError(t, err)
//	require.NotNil(t, tx)
//	assert.Equal(t, tx1.Size(), txSize)
//
//	// check the end of the stream
//	n, err = r.Read(bytes)
//	assert.Equal(t, io.ErrClosedPipe, err)
//	assert.Equal(t, 0, n)
//
//}

func TestGetLegacyBlockWithSubtreeStore(t *testing.T) {
	tracing.SetGlobalMockTracer()

	var (
		coinbase, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
		tx1, _      = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	)

	ctx := setup(t)
	ctx.logger.Debugf("test")

	params := blockInfo{
		version:           1,
		bits:              "2000ffff",
		previousBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		height:            1,
		nonce:             2083236893,
		timestamp:         uint32(time.Now().Unix()),
		txs:               []*bt.Tx{coinbase, tx1},
	}

	block, subtree := newBlock(ctx, t, params)
	require.NoError(t, ctx.repo.BlockchainClient.AddBlock(context.Background(), block, "test_peerID"))

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
	err = ctx.repo.SubtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
	assert.Equal(t, uint32(block.SizeInBytes), size)

	// version, 4 bytes
	n, err = io.ReadFull(r, bytes[:4])
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
	difficulty := model.NewNBitFromSlice(bytes[:n])
	assert.Equal(t, block.Header.Bits, difficulty)

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

type mockStore struct {
	block *model.Block
}

func (s *mockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	//TODO implement me
	panic("implement me")
}

func (s *mockStore) Health(ctx context.Context) (*blockchain_api.HealthResponse, error) {
	panic("not implemented")
}
func (s *mockStore) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	s.block = block
	return nil
}
func (s *mockStore) SendNotification(ctx context.Context, notification *model.Notification) error {
	panic("not implemented")
}
func (s *mockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	return s.block, nil
}
func (s *mockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	return s.block, nil
}
func (s *mockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic("not implemented")
}
func (s *mockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic("not implemented")
}
func (s *mockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	panic("not implemented")
}
func (s *mockStore) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash) (*model.NBit, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	panic("not implemented")
}
func (s *mockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	panic("not implemented")
}
func (s *mockStore) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	panic("not implemented")
}
func (s *mockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	panic("not implemented")
}
func (s *mockStore) SetState(ctx context.Context, key string, data []byte) error {
	panic("not implemented")
}
func (s *mockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {
	panic("not implemented")
}
func (s *mockStore) SendFSMEvent(ctx context.Context, state blockchain_api.FSMEventType) error {
	panic("not implemented")
}
func (s *mockStore) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) HeightToHashRange(startHeight uint32, endHash *chainhash.Hash, maxResults int) ([]chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) IntervalBlockHashes(endHash *chainhash.Hash, interval int) ([]chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	panic("implement me")
}

type testContext struct {
	repo   *Repository
	logger ulogger.Logger
}

func setup(t *testing.T) *testContext {
	// logger := ulogger.TestLogger{}
	logger := ulogger.NewZeroLogger("test")

	utxoStore := memory_utxo.New(logger)
	txStore := memory_blob.New()
	blockchainClient := &mockStore{}
	subtreeStore := memory_blob.New()
	blockStore := memory_blob.New()
	repo, err := NewRepository(logger, utxoStore, txStore, blockchainClient, subtreeStore, blockStore)
	assert.NoError(t, err)

	return &testContext{
		repo:   repo,
		logger: logger,
	}
}

func newBlock(ctx *testContext, t *testing.T, b blockInfo) (*model.Block, *util.Subtree) {
	if len(b.txs) == 0 {
		panic("no transactions provided")
	}

	subtree, err := util.NewTreeByLeafCount(2)
	require.NoError(t, err)

	for i, tx := range b.txs {
		if i == 0 {
			require.NoError(t, subtree.AddNode(model.CoinbasePlaceholder, 0, 0))
		} else {
			require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
			require.NoError(t, err)
		}
	}

	nBits := model.NewNBitFromString(b.bits)
	hashPrevBlock, _ := chainhash.NewHashFromStr(b.previousBlockHash)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())

	blockHeader := &model.BlockHeader{
		Version:        b.version,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: subtree.RootHash(), // doesn't matter, we're only checking the value and not whether it's correct
		Timestamp:      b.timestamp,
		Bits:           nBits,
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
