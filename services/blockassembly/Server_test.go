package blockassembly

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/stores/blob/memory"
	blockchainstore "github.com/TAAL-GmbH/ubsv/stores/blockchain"
	txstatusstore "github.com/TAAL-GmbH/ubsv/stores/txstatus/memory"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo/memory"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type baTestItems struct {
	utxoStore        *utxostore.Memory
	txStatusStore    *txstatusstore.Memory
	blobStore        *memory.Memory
	newSubtreeChan   chan *util.Subtree
	subtreeProcessor *subtreeprocessor.SubtreeProcessor
	blockAssembly    *BlockAssembly
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
		ba := setupBlockAssemblyTest(t)
		require.NotNil(t, ba)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			subtree := <-ba.newSubtreeChan
			assert.NotNil(t, subtree)
			assert.Equal(t, *model.CoinbasePlaceholderHash, *subtree.Nodes[0])
			assert.Len(t, subtree.Nodes, 4)
			assert.Equal(t, uint64(666), subtree.Fees)
			wg.Done()
		}()

		require.NoError(t, ba.txStatusStore.Create(ctx, tx1, 111, []*chainhash.Hash{tx0}, []*chainhash.Hash{utxo1}))
		_, err := ba.blockAssembly.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
			Txid: tx1.CloneBytes(),
		})
		require.NoError(t, err)

		require.NoError(t, ba.txStatusStore.Create(ctx, tx2, 222, []*chainhash.Hash{tx1}, []*chainhash.Hash{utxo2}))
		_, err = ba.blockAssembly.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
			Txid: tx2.CloneBytes(),
		})
		require.NoError(t, err)

		require.NoError(t, ba.txStatusStore.Create(ctx, tx3, 333, []*chainhash.Hash{tx2}, []*chainhash.Hash{utxo3}))
		_, err = ba.blockAssembly.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
			Txid: tx3.CloneBytes(),
		})
		require.NoError(t, err)

		require.NoError(t, ba.txStatusStore.Create(ctx, tx4, 444, []*chainhash.Hash{tx3}, []*chainhash.Hash{utxo4}))
		_, err = ba.blockAssembly.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
			Txid: tx4.CloneBytes(),
		})
		require.NoError(t, err)

		require.NoError(t, ba.txStatusStore.Create(ctx, tx5, 555, []*chainhash.Hash{tx4}, []*chainhash.Hash{utxo5}))
		_, err = ba.blockAssembly.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
			Txid: tx5.CloneBytes(),
		})
		require.NoError(t, err)

		wg.Wait()
	})
}

func setupBlockAssemblyTest(t *testing.T) *baTestItems {
	items := baTestItems{}

	items.utxoStore = utxostore.New(false)    // utxo memory store
	items.txStatusStore = txstatusstore.New() // tx status memory store
	items.blobStore = memory.New()            // blob memory store

	_ = os.Setenv("initial_merkle_items_per_subtree", "4")
	items.newSubtreeChan = make(chan *util.Subtree)
	items.subtreeProcessor = subtreeprocessor.NewSubtreeProcessor(p2p.TestLogger{}, items.newSubtreeChan)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(p2p.TestLogger{}, storeURL)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(p2p.TestLogger{}, blockchainStore)
	require.NoError(t, err)

	// we cannot rely on the settings to be set in the test environment
	ba := BlockAssembly{
		UnimplementedBlockAssemblyAPIServer: blockassembly_api.UnimplementedBlockAssemblyAPIServer{},
		logger:                              p2p.TestLogger{},
		utxoStore:                           items.utxoStore,
		txStatusClient:                      items.txStatusStore,
		subtreeProcessor:                    items.subtreeProcessor,
		grpcServer:                          nil,
		blockchainClient:                    blockchainClient,
		blockStore:                          items.blobStore,
		jobStoreMutex:                       sync.RWMutex{},
		jobStore:                            make(map[chainhash.Hash]*subtreeprocessor.Job),
	}

	items.blockAssembly = &ba

	return &items
}
