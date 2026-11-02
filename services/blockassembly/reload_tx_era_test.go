package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestReloadValidatedHeights pins the height a reload tags each transaction with for the
// transaction rules. A transaction never mined was validated at its unmined_since. One mined
// off the main chain takes the highest height of its blocks. One a reorg or an invalidation
// moved back has a backdated unmined_since (the tip less the retention, or 1), which is
// unknown: the sequential longest-chain and reorg-reset tests lost every moved-back
// transaction to that tag.
func TestReloadValidatedHeights(t *testing.T) {
	bc := &blockchain.Mock{}
	bc.On("GetBlockByID", mock.Anything, uint64(5)).Return(&model.Block{Height: 120}, nil).Once()
	bc.On("GetBlockByID", mock.Anything, uint64(6)).Return(&model.Block{Height: 150}, nil).Once()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UnminedTxRetention = 144

	b := &BlockAssembler{blockchainClient: bc, settings: tSettings}
	b.bestBlock.Store(&BestBlockInfo{Header: &model.BlockHeader{}, Height: 500})

	tx := func(name string, unminedSince int, blockIDs ...uint32) *utxostore.UnminedTransaction {
		return &utxostore.UnminedTransaction{
			Node:         &subtreepkg.Node{Hash: chainhash.HashH([]byte(name))},
			UnminedSince: unminedSince,
			BlockIDs:     blockIDs,
		}
	}

	heights, err := b.reloadValidatedHeights(context.Background(), []*utxostore.UnminedTransaction{
		tx("mempool", 490),
		tx("untagged", 0),
		tx("mined in blocks off the main chain", 1, 5, 6),
		tx("mined in a block off the main chain", 356, 6),
		tx("moved back, backdated to the tip less the retention", 356),
		tx("moved back on a short chain, backdated to 1", 1),
	}, map[uint32]uint32{})
	require.NoError(t, err)
	require.Equal(t, []uint32{490, 0, 150, 150, 0, 0}, heights)

	bc.AssertExpectations(t)
}
