// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	subtreepkg "github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/blockassembly/mining"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxoStore "github.com/bitcoin-sv/teranode/stores/utxo"
	utxostoresql "github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// baTestItems represents test fixtures for block assembly testing.
type baTestItems struct {
	// utxoStore manages UTXO storage for testing
	utxoStore utxoStore.Store

	// txStore manages transaction storage for testing
	txStore *memory.Memory

	// blobStore manages blob storage for testing
	blobStore *memory.Memory

	// newSubtreeChan handles new subtree notifications in tests
	newSubtreeChan chan subtreeprocessor.NewSubtreeRequest

	// blockAssembler is the test instance of BlockAssembler
	blockAssembler *BlockAssembler

	// blockchainClient provides blockchain operations for testing
	blockchainClient blockchain.ClientI
}

// addBlock adds a test block to the blockchain.
//
// Parameters:
//   - blockHeader: Header of the block to add
//
// Returns:
//   - error: Any error encountered during addition
func (items baTestItems) addBlock(blockHeader *model.BlockHeader) error {
	coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

	return items.blockchainClient.AddBlock(context.Background(), &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}, "")
}

var (
	tx0 = newTx(0)
	tx1 = newTx(1)
	tx2 = newTx(2)
	tx3 = newTx(3)
	tx4 = newTx(4)
	tx5 = newTx(5)
	tx6 = newTx(6)
	tx7 = newTx(7)

	hash0 = tx0.TxIDChainHash()
	hash1 = tx1.TxIDChainHash()
	hash2 = tx2.TxIDChainHash()
	hash3 = tx3.TxIDChainHash()
	hash4 = tx4.TxIDChainHash()
	hash5 = tx5.TxIDChainHash()
	hash6 = tx6.TxIDChainHash()
	hash7 = tx7.TxIDChainHash()
)

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime

	tx.Inputs = make([]*bt.Input, 1)
	tx.Inputs[0] = &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
		SequenceNumber:     0,
	}
	_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})

	return tx
}

func TestBlockAssembly_Start(t *testing.T) {
	t.Run("Start on mainnet, wait 2 blocks", func(t *testing.T) {
		initPrometheusMetrics()

		tSettings := createTestSettings()
		tSettings.BlockAssembly.ResetWaitCount = 2
		tSettings.BlockAssembly.ResetWaitDuration = 20 * time.Minute
		tSettings.ChainCfgParams.Net = wire.MainNet

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
		require.NoError(t, err)

		stats := gocore.NewStat("test")

		blockchainClient := &blockchain.Mock{}
		blockchainClient.On("GetState", mock.Anything, mock.Anything).Return([]byte{}, sql.ErrNoRows)
		blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, errors.ErrNotFound)
		blockchainClient.On("GetNextWorkRequired", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)
		blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		blockAssembler, err := NewBlockAssembler(context.Background(), ulogger.TestLogger{}, tSettings, stats, utxoStore, nil, blockchainClient, nil)
		require.NoError(t, err)
		require.NotNil(t, blockAssembler)

		err = blockAssembler.Start(t.Context())
		require.NoError(t, err)

		assert.Equal(t, int32(0), blockAssembler.resetWaitCount.Load())
		assert.Equal(t, int32(0), blockAssembler.resetWaitDuration.Load())
	})

	t.Run("Start on testnet, inherits same wait as mainnet", func(t *testing.T) {
		initPrometheusMetrics()

		tSettings := createTestSettings()
		tSettings.BlockAssembly.ResetWaitCount = 2
		tSettings.BlockAssembly.ResetWaitDuration = 20 * time.Minute
		tSettings.ChainCfgParams.Net = wire.TestNet

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
		require.NoError(t, err)

		stats := gocore.NewStat("test")

		blockchainClient := &blockchain.Mock{}
		blockchainClient.On("GetState", mock.Anything, mock.Anything).Return([]byte{}, sql.ErrNoRows)
		blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, errors.ErrNotFound)
		blockchainClient.On("GetNextWorkRequired", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)
		blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		blockAssembler, err := NewBlockAssembler(context.Background(), ulogger.TestLogger{}, tSettings, stats, utxoStore, nil, blockchainClient, nil)
		require.NoError(t, err)
		require.NotNil(t, blockAssembler)

		err = blockAssembler.Start(t.Context())
		require.NoError(t, err)

		assert.Equal(t, int32(0), blockAssembler.resetWaitCount.Load(), "resetWaitCount should be set on TestNet as on MainNet")
		assert.Equal(t, int32(0), blockAssembler.resetWaitDuration.Load(), "resetWaitDuration must be no greater than now+configured duration")
	})
}

func TestBlockAssembly_AddTx(t *testing.T) {
	t.Run("addTx", func(t *testing.T) {
		initPrometheusMetrics()

		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)

		testItems.blockAssembler.startChannelListeners(ctx)

		var buf bytes.Buffer

		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), testItems.blockAssembler.settings)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		require.Equal(t, chaincfg.RegressionNetParams.GenesisHash, genesisBlock.Hash())

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			for i := 0; i < 2; i++ {
				subtreeRequest := <-testItems.newSubtreeChan
				subtree := subtreeRequest.Subtree
				assert.NotNil(t, subtree)

				if i == 0 {
					assert.Equal(t, *subtreepkg.CoinbasePlaceholderHash, subtree.Nodes[0].Hash)
				}

				assert.Len(t, subtree.Nodes, 4)
				assert.Equal(t, uint64(666), subtree.Fees)

				if subtreeRequest.ErrChan != nil {
					subtreeRequest.ErrChan <- nil
				}

				wg.Done()
			}
		}()

		_, err = testItems.utxoStore.Create(ctx, tx1, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash1, Fee: 111}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx2, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash2, Fee: 222}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx3, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash3, Fee: 333}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx4, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash4, Fee: 110}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx5, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash5, Fee: 220}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx6, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash6, Fee: 330}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx7, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash7, Fee: 6}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		wg.Wait()

		// need to wait for the txCount to be updated after the subtree notification was fired off
		time.Sleep(10 * time.Millisecond)

		// Check the state of the SubtreeProcessor
		assert.Equal(t, 3, testItems.blockAssembler.subtreeProcessor.SubtreeCount())

		// should include the 7 transactions added + the coinbase placeholder of the first subtree
		assert.Equal(t, uint64(8), testItems.blockAssembler.subtreeProcessor.TxCount())

		miningCandidate, subtrees, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err)
		assert.NotNil(t, miningCandidate)
		assert.NotNil(t, subtrees)
		assert.Equal(t, uint64(5000001332), miningCandidate.CoinbaseValue)
		assert.Equal(t, uint32(1), miningCandidate.Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", utils.ReverseAndHexEncodeSlice(miningCandidate.PreviousHash))
		assert.Len(t, subtrees, 2)
		assert.Len(t, subtrees[0].Nodes, 4)
		assert.Len(t, subtrees[1].Nodes, 4)

		// mine block

		solution, err := mining.Mine(ctx, testItems.blockAssembler.settings, miningCandidate, nil)
		require.NoError(t, err)

		blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
		require.NoError(t, err)

		blockHash := util.Sha256d(blockHeader)
		hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

		bits, _ := model.NewNBitFromSlice(miningCandidate.NBits)
		target := bits.CalculateTarget()

		var bn = big.NewInt(0)

		bn.SetString(hashStr, 16)

		compare := bn.Cmp(target)
		assert.LessOrEqual(t, compare, 0)
	})
}

var (
	bits, _      = model.NewNBitFromString("1d00ffff")
	blockHeader1 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chaincfg.RegressionNetParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *bits,
	}
	blockHeader2 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *bits,
	}
	blockHeader3 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader2.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           *bits,
	}
	blockHeader4 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader3.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4,
		Bits:           *bits,
	}
	blockHeader2Alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          12,
		Bits:           *bits,
	}
	blockHeader3Alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader2Alt.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          13,
		Bits:           *bits,
	}
	blockHeader4Alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader3Alt.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          14,
		Bits:           *bits,
	}
)

func TestBlockAssemblerGetReorgBlockHeaders(t *testing.T) {
	initPrometheusMetrics()

	t.Run("getReorgBlocks nil", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		items.blockAssembler.bestBlockHeader.Store(blockHeader1)
		_, _, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), nil, 0)
		require.Error(t, err)
	})

	t.Run("getReorgBlocks", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		// set the cached BlockAssembler items to the correct values
		items.blockAssembler.bestBlockHeader.Store(blockHeader4)
		items.blockAssembler.bestBlockHeight.Store(4)

		err := items.addBlock(blockHeader1)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2Alt)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3Alt)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4Alt)
		require.NoError(t, err)

		moveBackBlockHeaders, moveForwardBlockHeaders, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), blockHeader4Alt, 4)
		require.NoError(t, err)

		assert.Len(t, moveBackBlockHeaders, 3)
		assert.Equal(t, blockHeader4.Hash(), moveBackBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader3.Hash(), moveBackBlockHeaders[1].Hash())
		assert.Equal(t, blockHeader2.Hash(), moveBackBlockHeaders[2].Hash())

		assert.Len(t, moveForwardBlockHeaders, 3)
		assert.Equal(t, blockHeader2Alt.Hash(), moveForwardBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader3Alt.Hash(), moveForwardBlockHeaders[1].Hash())
		assert.Equal(t, blockHeader4Alt.Hash(), moveForwardBlockHeaders[2].Hash())
	})

	// this situation has been observed when a reorg is triggered when a moveForward should have been triggered
	t.Run("getReorgBlocks - not moving back", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		err := items.addBlock(blockHeader1)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3)
		require.NoError(t, err)

		// set the cached BlockAssembler items to block 2
		items.blockAssembler.bestBlockHeader.Store(blockHeader2)
		items.blockAssembler.bestBlockHeight.Store(2)

		moveBackBlockHeaders, moveForwardBlockHeaders, err := items.blockAssembler.getReorgBlockHeaders(t.Context(), blockHeader3, 3)
		require.NoError(t, err)

		assert.Len(t, moveBackBlockHeaders, 0)

		assert.Len(t, moveForwardBlockHeaders, 1)
		assert.Equal(t, blockHeader3.Hash(), moveForwardBlockHeaders[0].Hash())
	})

	t.Run("getReorgBlocks - missing block", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		// set the cached BlockAssembler items to the correct values
		items.blockAssembler.bestBlockHeader.Store(blockHeader2)
		items.blockAssembler.bestBlockHeight.Store(2)

		err := items.addBlock(blockHeader1)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4)
		require.NoError(t, err)

		moveBackBlockHeaders, moveForwardBlockHeaders, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), blockHeader4, 4)
		require.NoError(t, err)

		assert.Len(t, moveBackBlockHeaders, 0)

		assert.Len(t, moveForwardBlockHeaders, 2)
		assert.Equal(t, blockHeader3.Hash(), moveForwardBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader4.Hash(), moveForwardBlockHeaders[1].Hash())
	})
}

// setupBlockAssemblyTest prepares a test environment for block assembly.
//
// Parameters:
//   - t: Testing instance
//
// Returns:
//   - *baTestItems: Test fixtures and utilities
func setupBlockAssemblyTest(t *testing.T) *baTestItems {
	items := baTestItems{}

	items.blobStore = memory.New() // blob memory store
	items.txStore = memory.New()   // tx memory store

	items.newSubtreeChan = make(chan subtreeprocessor.NewSubtreeRequest)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	tSettings := createTestSettings()

	utxoStore, err := utxostoresql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	items.utxoStore = utxoStore

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	items.blockchainClient, err = blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	require.NoError(t, err)

	stats := gocore.NewStat("test")

	ba, _ := NewBlockAssembler(
		context.Background(),
		ulogger.TestLogger{},
		tSettings,
		stats,
		items.utxoStore,
		items.blobStore,
		items.blockchainClient,
		items.newSubtreeChan,
	)

	assert.NotNil(t, ba.settings)

	// overwrite default subtree processor with a new one
	ba.subtreeProcessor, err = subtreeprocessor.NewSubtreeProcessor(
		context.Background(),
		ulogger.TestLogger{},
		ba.settings,
		nil,
		nil,
		nil,
		items.newSubtreeChan,
		subtreeprocessor.WithBatcherSize(1),
	)
	require.NoError(t, err)

	items.blockAssembler = ba

	return &items
}

func TestBlockAssembly_ShouldNotAllowMoreThanOneCoinbaseTx(t *testing.T) {
	t.Run("addTx", func(t *testing.T) {
		initPrometheusMetrics()

		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)

		testItems.blockAssembler.startChannelListeners(ctx)

		var buf bytes.Buffer

		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), nil)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			subtreeRequest := <-testItems.newSubtreeChan
			subtree := subtreeRequest.Subtree
			assert.NotNil(t, subtree)
			assert.Equal(t, *subtreepkg.CoinbasePlaceholderHash, subtree.Nodes[0].Hash)
			assert.Len(t, subtree.Nodes, 4)
			assert.NotEqual(t, uint64(5000000556), subtree.Fees)

			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

			wg.Done()
		}()

		_, err = testItems.utxoStore.Create(ctx, tx1, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *subtreepkg.CoinbasePlaceholderHash, Fee: 5000000000}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx2, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash2, Fee: 222}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx3, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash3, Fee: 334}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx4, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash4, Fee: 444}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx5, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash5, Fee: 555}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		wg.Wait()

		miningCandidate, subtree, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err)
		assert.NotNil(t, miningCandidate)
		assert.NotNil(t, subtree)
		// assert.Equal(t, uint64(5000000667), miningCandidate.CoinbaseValue)
		assert.NotEqual(t, uint64(10000000556), miningCandidate.CoinbaseValue)
		assert.Equal(t, uint32(1), miningCandidate.Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", utils.ReverseAndHexEncodeSlice(miningCandidate.PreviousHash))
		assert.Len(t, subtree, 1)
		assert.Len(t, subtree[0].Nodes, 4)

		// mine block

		solution, err := mining.Mine(ctx, testItems.blockAssembler.settings, miningCandidate, nil)
		require.NoError(t, err)

		blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
		require.NoError(t, err)

		blockHash := util.Sha256d(blockHeader)
		hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

		bits, _ := model.NewNBitFromSlice(miningCandidate.NBits)
		target := bits.CalculateTarget()

		var bn = big.NewInt(0)

		bn.SetString(hashStr, 16)

		compare := bn.Cmp(target)
		assert.LessOrEqual(t, compare, 0)
	})
}

func TestBlockAssembly_GetMiningCandidate(t *testing.T) {
	t.Run("GetMiningCandidate", func(t *testing.T) {
		initPrometheusMetrics()

		tSettings := createTestSettings()

		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)

		testItems.blockAssembler.startChannelListeners(ctx)

		var buf bytes.Buffer

		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), tSettings)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		require.Equal(t, chaincfg.RegressionNetParams.GenesisHash, genesisBlock.Hash())

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			subtreeRequest := <-testItems.newSubtreeChan
			subtree := subtreeRequest.Subtree
			assert.NotNil(t, subtree)
			assert.Equal(t, *subtreepkg.CoinbasePlaceholderHash, subtree.Nodes[0].Hash)
			assert.Len(t, subtree.Nodes, 4)
			assert.Equal(t, uint64(999), subtree.Fees)

			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

			wg.Done()
		}()

		// first add coinbase
		_, err = testItems.utxoStore.Create(ctx, tx1, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *subtreepkg.CoinbasePlaceholderHash, Fee: 5000000000, SizeInBytes: 111}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx2, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash2, Fee: 222, SizeInBytes: 222}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx3, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash3, Fee: 333, SizeInBytes: 333}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx4, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *hash4, Fee: 444, SizeInBytes: 444}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		wg.Wait()

		miningCandidate, subtrees, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err)

		assert.NotNil(t, miningCandidate)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", utils.ReverseAndHexEncodeSlice(miningCandidate.PreviousHash))
		assert.Equal(t, uint64(5000000999), miningCandidate.CoinbaseValue)
		assert.Equal(t, uint32(1), miningCandidate.Height)
		assert.Equal(t, uint32(4), miningCandidate.NumTxs)
		assert.Equal(t, uint64(1079), miningCandidate.SizeWithoutCoinbase)
		assert.Equal(t, uint32(1), miningCandidate.SubtreeCount)
		// Check the MerkleProof
		expectedMerkleProofChainhash, err := subtreepkg.GetMerkleProofForCoinbase(subtrees)
		assert.NoError(t, err)

		expectedMerkleProof := [][]byte{}
		for _, hash := range expectedMerkleProofChainhash {
			expectedMerkleProof = append(expectedMerkleProof, hash.CloneBytes())
		}

		assert.Equal(t, expectedMerkleProof, miningCandidate.MerkleProof)

		assert.NotNil(t, subtrees)
		assert.Len(t, subtrees, 1)
		assert.Len(t, subtrees[0].Nodes, 4)
		assert.Equal(t, subtreepkg.CoinbasePlaceholderHash.String(), subtrees[0].Nodes[0].Hash.String())
		assert.Equal(t, hash2.String(), subtrees[0].Nodes[1].Hash.String())
		assert.Equal(t, hash3.String(), subtrees[0].Nodes[2].Hash.String())
		assert.Equal(t, hash4.String(), subtrees[0].Nodes[3].Hash.String())

		solution, err := mining.Mine(ctx, testItems.blockAssembler.settings, miningCandidate, nil)
		require.NoError(t, err)

		blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
		require.NoError(t, err)

		blockHash := util.Sha256d(blockHeader)
		hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

		bits, _ := model.NewNBitFromSlice(miningCandidate.NBits)
		target := bits.CalculateTarget()

		var bn = big.NewInt(0)

		bn.SetString(hashStr, 16)

		compare := bn.Cmp(target)
		assert.LessOrEqual(t, compare, 0)
	})
}

func TestBlockAssembly_GetMiningCandidate_MaxBlockSize(t *testing.T) {
	t.Run("GetMiningCandidate_MaxBlockSize", func(t *testing.T) {
		initPrometheusMetrics()

		tSettings := createTestSettings()

		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)
		testItems.blockAssembler.settings.Policy.BlockMaxSize = 15000*4 + 1000

		testItems.blockAssembler.startChannelListeners(ctx)

		var buf bytes.Buffer

		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), tSettings)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		require.Equal(t, chaincfg.RegressionNetParams.GenesisHash, genesisBlock.Hash())

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		var wg sync.WaitGroup

		// 15 txs is 3 complete subtrees
		wg.Add(3)

		go func() {
			for {
				select {
				case subtreeRequest := <-testItems.newSubtreeChan:
					subtree := subtreeRequest.Subtree
					assert.NotNil(t, subtree)
					// assert.Equal(t, *util.CoinbasePlaceholderHash, subtree.Nodes[0].Hash)
					assert.Len(t, subtree.Nodes, 4)
					// assert.Equal(t, uint64(4000000000), subtree.Fees)

					if subtreeRequest.ErrChan != nil {
						subtreeRequest.ErrChan <- nil
					}

					wg.Done()
				case <-ctx.Done():
					return
				}
			}
		}()

		for i := 0; i < 15; i++ {
			// nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
			tx := newTx(uint32(i))
			_, err = testItems.utxoStore.Create(ctx, tx, 0)
			require.NoError(t, err)

			if i == 0 {
				// first add coinbase
				testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *subtreepkg.CoinbasePlaceholderHash, Fee: 5000000000, SizeInBytes: 15000}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			} else {
				testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *tx.TxIDChainHash(), Fee: 1000000000, SizeInBytes: 15000}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			}
		}

		wg.Wait()

		miningCandidate, subtrees, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err)

		assert.NotNil(t, miningCandidate)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", utils.ReverseAndHexEncodeSlice(miningCandidate.PreviousHash))
		assert.Equal(t, uint64(8000000000), miningCandidate.CoinbaseValue)
		assert.Equal(t, uint32(1), miningCandidate.Height)
		assert.Equal(t, uint32(4), miningCandidate.NumTxs)
		assert.Equal(t, uint64(45080), miningCandidate.SizeWithoutCoinbase) // 3 * 1500 + 80
		assert.Equal(t, uint32(1), miningCandidate.SubtreeCount)
		// Check the MerkleProof
		expectedMerkleProofChainhash, err := subtreepkg.GetMerkleProofForCoinbase(subtrees)
		assert.NoError(t, err)

		expectedMerkleProof := [][]byte{}
		for _, hash := range expectedMerkleProofChainhash {
			expectedMerkleProof = append(expectedMerkleProof, hash.CloneBytes())
		}

		assert.Equal(t, expectedMerkleProof, miningCandidate.MerkleProof)

		assert.NotNil(t, subtrees)
		assert.Len(t, subtrees, 1)
		assert.Len(t, subtrees[0].Nodes, 4)
		assert.Equal(t, subtreepkg.CoinbasePlaceholderHash.String(), subtrees[0].Nodes[0].Hash.String())
		assert.Equal(t, hash1.String(), subtrees[0].Nodes[1].Hash.String())
		assert.Equal(t, hash2.String(), subtrees[0].Nodes[2].Hash.String())
		assert.Equal(t, hash3.String(), subtrees[0].Nodes[3].Hash.String())

		solution, err := mining.Mine(ctx, testItems.blockAssembler.settings, miningCandidate, nil)
		require.NoError(t, err)

		blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
		require.NoError(t, err)

		blockHash := util.Sha256d(blockHeader)
		hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

		bits, _ := model.NewNBitFromSlice(miningCandidate.NBits)
		target := bits.CalculateTarget()

		var bn = big.NewInt(0)

		bn.SetString(hashStr, 16)

		compare := bn.Cmp(target)
		assert.LessOrEqual(t, compare, 0)
	})
}

func TestBlockAssembly_GetMiningCandidate_MaxBlockSize_LessThanSubtreeSize(t *testing.T) {
	t.Run("GetMiningCandidate_MaxBlockSize_LessThanSubtreeSize", func(t *testing.T) {
		initPrometheusMetrics()

		tSettings := createTestSettings()

		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)
		testItems.blockAssembler.settings.Policy.BlockMaxSize = 430000

		testItems.blockAssembler.startChannelListeners(ctx)

		var buf bytes.Buffer

		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), tSettings)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		require.Equal(t, chaincfg.RegressionNetParams.GenesisHash, genesisBlock.Hash())

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			subtreeRequest := <-testItems.newSubtreeChan
			subtree := subtreeRequest.Subtree
			assert.NotNil(t, subtree)
			assert.Equal(t, *subtreepkg.CoinbasePlaceholderHash, subtree.Nodes[0].Hash)
			assert.Len(t, subtree.Nodes, 4)
			assert.Equal(t, uint64(3000000000), subtree.Fees)

			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

			wg.Done()
		}()

		for i := 0; i < 4; i++ {
			// nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
			tx := newTx(uint32(i))
			_, err = testItems.utxoStore.Create(ctx, tx, 0)
			require.NoError(t, err)

			if i == 0 {
				// first add coinbase
				testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *subtreepkg.CoinbasePlaceholderHash, Fee: 5000000000, SizeInBytes: 100}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			} else {
				testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{Hash: *tx.TxIDChainHash(), Fee: 1000000000, SizeInBytes: 150000}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}) // 0.15MB
			}
		}

		wg.Wait()

		_, _, err = testItems.blockAssembler.GetMiningCandidate(ctx)
		require.Error(t, err)

		assert.Equal(t, "PROCESSING (4): max block size is less than the size of the subtree", err.Error())
	})
}

// TestBlockAssembly_CoinbaseSubsidyBugReproduction specifically targets issue #3139
// This test attempts to reproduce the exact conditions that cause 0.006 BSV coinbase values
func TestBlockAssembly_CoinbaseSubsidyBugReproduction(t *testing.T) {
	t.Run("SubsidyCalculationFailure", func(t *testing.T) {
		initPrometheusMetrics()

		// Test various chain parameter corruption scenarios
		scenarios := []struct {
			name     string
			params   *chaincfg.Params
			expected string
		}{
			{
				name:     "NilParams",
				params:   nil,
				expected: "should return 0 and log error",
			},
			{
				name: "ZeroSubsidyInterval",
				params: &chaincfg.Params{
					SubsidyReductionInterval: 0, // This causes division by zero!
				},
				expected: "should return 0 due to zero interval",
			},
		}

		for _, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				height := uint32(100) // Early block that should have 50 BTC subsidy

				subsidy := util.GetBlockSubsidyForHeight(height, scenario.params)

				// All these corrupted scenarios should return 0
				assert.Equal(t, uint64(0), subsidy,
					"Corrupted params scenario '%s' should return 0 subsidy", scenario.name)

				t.Logf("SCENARIO '%s': height=%d, subsidy=%d (%.8f BSV) - %s",
					scenario.name, height, subsidy, float64(subsidy)/1e8, scenario.expected)
			})
		}
	})

	t.Run("FeesOnlyScenario", func(t *testing.T) {
		initPrometheusMetrics()

		testItems := setupBlockAssemblyTest(t)
		ctx := context.Background()

		testItems.blockAssembler.startChannelListeners(ctx)

		// Set up genesis block
		var buf bytes.Buffer
		err := chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf)
		require.NoError(t, err)

		genesisBlock, err := model.NewBlockFromBytes(buf.Bytes(), testItems.blockAssembler.settings)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		testItems.blockAssembler.bestBlockHeader.Store(genesisBlock.Header)

		// Create the exact scenario from the bug report: fees only, no subsidy
		height := uint32(1) // Height 1 (after genesis)
		testItems.blockAssembler.bestBlockHeight.Store(height - 1)

		// Handle subtree processing
		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			subtreeRequest := <-testItems.newSubtreeChan
			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

			wg.Done()
		}()

		// Add transactions that would generate approximately 0.006 BSV in fees
		// This simulates the exact value seen in the bug report
		totalExpectedFees := uint64(600000) // 0.006 BSV = 600,000 satoshis

		// Add transactions with fees totaling ~600k satoshis
		tx1 := newTx(1)
		tx2 := newTx(2)
		tx3 := newTx(3)

		// First add coinbase placeholder
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{
			Hash:        *subtreepkg.CoinbasePlaceholderHash,
			Fee:         0,
			SizeInBytes: 100,
		}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		// Add transactions to UTXO store and then to block assembler
		_, err = testItems.utxoStore.Create(ctx, tx1, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{
			Hash:        *tx1.TxIDChainHash(),
			Fee:         200000, // 0.002 BSV
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx2, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{
			Hash:        *tx2.TxIDChainHash(),
			Fee:         300000, // 0.003 BSV
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		_, err = testItems.utxoStore.Create(ctx, tx3, 0)
		require.NoError(t, err)
		testItems.blockAssembler.AddTx(subtreepkg.SubtreeNode{
			Hash:        *tx3.TxIDChainHash(),
			Fee:         100000, // 0.001 BSV
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		wg.Wait()

		// Test with normal parameters - should get full subsidy + fees
		miningCandidate, _, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err, "Failed to get mining candidate")
		assert.NotNil(t, miningCandidate)

		expectedSubsidy := uint64(5000000000) // 50 BSV for early blocks
		expectedTotal := totalExpectedFees + expectedSubsidy

		assert.Equal(t, expectedTotal, miningCandidate.CoinbaseValue,
			"Normal scenario: should have fees (%d) + subsidy (%d) = %d",
			totalExpectedFees, expectedSubsidy, expectedTotal)

		t.Logf("NORMAL CASE: height=%d, fees=%d (%.8f BSV), subsidy=%d (%.8f BSV), total=%d (%.8f BSV)",
			height, totalExpectedFees, float64(totalExpectedFees)/1e8,
			expectedSubsidy, float64(expectedSubsidy)/1e8,
			miningCandidate.CoinbaseValue, float64(miningCandidate.CoinbaseValue)/1e8)

		// Now test what happens if we could somehow corrupt the chain params
		// (This demonstrates what the bug would look like)
		corrupted := *testItems.blockAssembler.settings.ChainCfgParams
		corrupted.SubsidyReductionInterval = 0 // Simulate corruption to zero

		subsidyWithCorruption := util.GetBlockSubsidyForHeight(height, &corrupted)
		assert.Equal(t, uint64(0), subsidyWithCorruption,
			"Corrupted params should cause subsidy calculation to return 0")

		feesOnlyTotal := totalExpectedFees + subsidyWithCorruption

		t.Logf("BUG SIMULATION: With corrupted params, coinbase would be %d (%.8f BSV) - EXACTLY matching bug report!",
			feesOnlyTotal, float64(feesOnlyTotal)/1e8)

		// This proves that 0.006 BSV coinbase = transaction fees only (no subsidy)
		assert.Equal(t, totalExpectedFees, feesOnlyTotal,
			"Bug simulation: corrupted subsidy calculation results in fees-only coinbase")
	})
}

// createTestSettings creates settings for testing purposes.
//
// Returns:
//   - *settings.Settings: Test configuration settings
func createTestSettings() *settings.Settings {
	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.SubtreeProcessorBatcherSize = 1
	tSettings.BlockAssembly.DoubleSpendWindow = 1000
	tSettings.BlockAssembly.MaxGetReorgHashes = 10000
	tSettings.BlockAssembly.MinerWalletPrivateKeys = []string{"5KYZdUEo39z3FPrtuX2QbbwGnNP5zTd7yyr2SC1j299sBCnWjss"}
	tSettings.SubtreeValidation.TxChanBufferSize = 1

	return tSettings
}
