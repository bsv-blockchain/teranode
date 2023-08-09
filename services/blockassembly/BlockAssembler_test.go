package blockassembly

import (
	"context"
	"math/big"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/miner/cpuminer"
	"github.com/TAAL-GmbH/ubsv/stores/blob/memory"
	blockchainstore "github.com/TAAL-GmbH/ubsv/stores/blockchain"
	txmetastore "github.com/TAAL-GmbH/ubsv/stores/txmeta/memory"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo/memory"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type baTestItems struct {
	utxoStore        *utxostore.Memory
	txMetaStore      *txmetastore.Memory
	txStore          *memory.Memory
	blobStore        *memory.Memory
	newSubtreeChan   chan *util.Subtree
	subtreeProcessor *subtreeprocessor.SubtreeProcessor
	blockAssembler   *BlockAssembler
	blockchainClient blockchain.ClientI
}

func (items baTestItems) addBlock(blockHeader *model.BlockHeader) error {
	return items.blockchainClient.AddBlock(context.Background(), &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       &bt.Tx{},
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	})
}

var (
	tx0, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	tx1, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	tx2, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	tx3, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
	tx4, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000004")
	tx5, _ = chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000005")

	utxo1, _ = chainhash.NewHashFromStr("1000000000000000000000000000000000000000000000000000000000000001")
	utxo2, _ = chainhash.NewHashFromStr("1000000000000000000000000000000000000000000000000000000000000002")
	utxo3, _ = chainhash.NewHashFromStr("1000000000000000000000000000000000000000000000000000000000000003")
	utxo4, _ = chainhash.NewHashFromStr("1000000000000000000000000000000000000000000000000000000000000004")
	utxo5, _ = chainhash.NewHashFromStr("1000000000000000000000000000000000000000000000000000000000000005")
)

func TestBlockAssembly_AddTx(t *testing.T) {
	t.Run("AddTx", func(t *testing.T) {
		ctx := context.Background()
		testItems := setupBlockAssemblyTest(t)
		require.NotNil(t, testItems)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			subtree := <-testItems.newSubtreeChan
			assert.NotNil(t, subtree)
			assert.Equal(t, *model.CoinbasePlaceholderHash, *subtree.Nodes[0].Hash)
			assert.Len(t, subtree.Nodes, 4)
			assert.Equal(t, uint64(666), subtree.Fees)
			wg.Done()
		}()

		require.NoError(t, testItems.txMetaStore.Create(ctx, tx1, 111, []*chainhash.Hash{tx0}, []*chainhash.Hash{utxo1}, 0))
		err := testItems.blockAssembler.AddTx(context.Background(), tx1)
		require.NoError(t, err)

		require.NoError(t, testItems.txMetaStore.Create(ctx, tx2, 222, []*chainhash.Hash{tx1}, []*chainhash.Hash{utxo2}, 0))
		err = testItems.blockAssembler.AddTx(context.Background(), tx2)
		require.NoError(t, err)

		require.NoError(t, testItems.txMetaStore.Create(ctx, tx3, 333, []*chainhash.Hash{tx2}, []*chainhash.Hash{utxo3}, 0))
		err = testItems.blockAssembler.AddTx(context.Background(), tx3)
		require.NoError(t, err)

		require.NoError(t, testItems.txMetaStore.Create(ctx, tx4, 444, []*chainhash.Hash{tx3}, []*chainhash.Hash{utxo4}, 0))
		err = testItems.blockAssembler.AddTx(context.Background(), tx4)
		require.NoError(t, err)

		require.NoError(t, testItems.txMetaStore.Create(ctx, tx5, 555, []*chainhash.Hash{tx4}, []*chainhash.Hash{utxo5}, 0))
		err = testItems.blockAssembler.AddTx(context.Background(), tx5)
		require.NoError(t, err)

		wg.Wait()
		miningCandidate, subtree, err := testItems.blockAssembler.GetMiningCandidate(ctx)
		require.NoError(t, err)
		assert.NotNil(t, miningCandidate)
		assert.NotNil(t, subtree)
		assert.Equal(t, uint64(5000000666), miningCandidate.CoinbaseValue)
		assert.Equal(t, uint32(1), miningCandidate.Height)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", utils.ReverseAndHexEncodeSlice(miningCandidate.PreviousHash))
		assert.Len(t, subtree, 1)
		assert.Len(t, subtree[0].Nodes, 4)

		// mine block

		solution, err := cpuminer.Mine(ctx, miningCandidate)
		require.NoError(t, err)

		blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
		require.NoError(t, err)

		blockHash := util.Sha256d(blockHeader)
		hashStr := utils.ReverseAndHexEncodeSlice(blockHash)

		target := model.NewNBitFromSlice(miningCandidate.NBits).CalculateTarget()

		var bn = big.NewInt(0)
		bn.SetString(hashStr, 16)

		compare := bn.Cmp(target)
		assert.LessOrEqual(t, compare, 0)

	})
}

var (
	hashGenesisBlock, _ = chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	blockHeader1        = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashGenesisBlock,
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader2 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader3 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader2.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader4 = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader3.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader2_alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          12,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader3_alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader2_alt.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          13,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
	blockHeader4_alt = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader3_alt.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          14,
		Bits:           model.NewNBitFromString("1d00ffff"),
	}
)

func TestBlockAssembler_getReorgBlockHeaders(t *testing.T) {
	t.Run("getReorgBlocks nil", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		items.blockAssembler.bestBlockHeader = blockHeader1
		_, _, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), nil)
		require.Error(t, err)
	})

	t.Run("getReorgBlocks", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		// set the cached BlockAssembler items to the correct values
		items.blockAssembler.bestBlockHeader = blockHeader4
		items.blockAssembler.currentChain = []*model.BlockHeader{
			blockHeader4,
			blockHeader3,
			blockHeader2,
			blockHeader1,
		}
		items.blockAssembler.currentChainMap = map[chainhash.Hash]uint32{
			*blockHeader4.Hash(): 4,
			*blockHeader3.Hash(): 3,
			*blockHeader2.Hash(): 2,
			*blockHeader1.Hash(): 1,
		}

		err := items.addBlock(blockHeader1)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2_alt)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3_alt)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4_alt)
		require.NoError(t, err)

		moveDownBlockHeaders, moveUpBlockHeaders, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), blockHeader4_alt)
		require.NoError(t, err)

		assert.Len(t, moveDownBlockHeaders, 3)
		assert.Equal(t, blockHeader4.Hash(), moveDownBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader3.Hash(), moveDownBlockHeaders[1].Hash())
		assert.Equal(t, blockHeader2.Hash(), moveDownBlockHeaders[2].Hash())

		assert.Len(t, moveUpBlockHeaders, 3)
		assert.Equal(t, blockHeader2_alt.Hash(), moveUpBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader3_alt.Hash(), moveUpBlockHeaders[1].Hash())
		assert.Equal(t, blockHeader4_alt.Hash(), moveUpBlockHeaders[2].Hash())
	})

	t.Run("getReorgBlocks - missing block", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		require.NotNil(t, items)

		// set the cached BlockAssembler items to the correct values
		items.blockAssembler.bestBlockHeader = blockHeader2
		items.blockAssembler.currentChain = []*model.BlockHeader{blockHeader2, blockHeader1}
		items.blockAssembler.currentChainMap = map[chainhash.Hash]uint32{
			*blockHeader1.Hash(): 1,
			*blockHeader2.Hash(): 2,
		}

		err := items.addBlock(blockHeader1)
		require.NoError(t, err)
		err = items.addBlock(blockHeader2)
		require.NoError(t, err)
		err = items.addBlock(blockHeader3)
		require.NoError(t, err)
		err = items.addBlock(blockHeader4)
		require.NoError(t, err)

		moveDownBlockHeaders, moveUpBlockHeaders, err := items.blockAssembler.getReorgBlockHeaders(context.Background(), blockHeader4)
		require.NoError(t, err)

		assert.Len(t, moveDownBlockHeaders, 0)

		assert.Len(t, moveUpBlockHeaders, 2)
		assert.Equal(t, blockHeader3.Hash(), moveUpBlockHeaders[0].Hash())
		assert.Equal(t, blockHeader4.Hash(), moveUpBlockHeaders[1].Hash())
	})
}

func setupBlockAssemblyTest(t require.TestingT) *baTestItems {
	items := baTestItems{}

	items.utxoStore = utxostore.New(false) // utxo memory store
	items.txMetaStore = txmetastore.New()  // tx status memory store
	items.blobStore = memory.New()         // blob memory store
	items.txStore = memory.New()           // tx memory store

	_ = os.Setenv("initial_merkle_items_per_subtree", "4")
	items.newSubtreeChan = make(chan *util.Subtree)
	items.subtreeProcessor = subtreeprocessor.NewSubtreeProcessor(p2p.TestLogger{}, nil, items.newSubtreeChan)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(p2p.TestLogger{}, storeURL)
	require.NoError(t, err)

	items.blockchainClient, err = blockchain.NewLocalClient(p2p.TestLogger{}, blockchainStore)
	require.NoError(t, err)

	// we cannot rely on the settings to be set in the test environment
	ba := NewBlockAssembler(
		context.Background(),
		p2p.TestLogger{},
		items.txMetaStore,
		items.utxoStore,
		items.txStore,
		items.blobStore,
		items.blockchainClient,
		items.newSubtreeChan,
	)

	items.blockAssembler = ba

	return &items
}
