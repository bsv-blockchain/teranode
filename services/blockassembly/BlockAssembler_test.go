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
			assert.Equal(t, *model.CoinbasePlaceholderHash, *subtree.Nodes[0])
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

func setupBlockAssemblyTest(t *testing.T) *baTestItems {
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

	blockchainClient, err := blockchain.NewLocalClient(p2p.TestLogger{}, blockchainStore)
	require.NoError(t, err)

	// we cannot rely on the settings to be set in the test environment
	ba := NewBlockAssembler(
		context.Background(),
		p2p.TestLogger{},
		items.txMetaStore,
		items.utxoStore,
		items.txStore,
		items.blobStore,
		blockchainClient,
		items.newSubtreeChan,
	)

	items.blockAssembler = ba

	return &items
}
