package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSetMinedStampsDAHAtMinedBlockHeight is a regression test for the early-pruning
// bug where setMined computed delete_at_height from the store's cached chain tip
// (s.blockHeight) rather than the height of the block the transaction was actually
// mined into (minedBlockInfo.BlockHeight).
//
// The store's cached height is maintained by an asynchronous blockchain-notification
// subscription, so during catchup / sync / high load it lags behind the block being
// validated. When a transaction that was spent while still unmined is later marked
// mined, setMined stamps the DAH. Using the lagging cached height stamped the DAH far
// too low, so the pruner (which deletes records with delete_at_height <= currentHeight)
// removed the record long before the retention window had elapsed.
//
// The mined block height is always known to block validation and passed in
// minedBlockInfo.BlockHeight, so the DAH must be computed from it.
func TestSetMinedStampsDAHAtMinedBlockHeight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, parent := setup(ctx, t)

	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

	// The store's cached chain-tip height lags far behind the block actually being
	// validated — exactly the catchup / sync condition that triggered the bug.
	const cachedTip uint32 = 100
	const minedHeight uint32 = 900_000
	require.NoError(t, store.SetBlockHeight(cachedTip))

	// Create the parent UNMINED.
	parentHash := parent.TxIDChainHash()
	_, err := store.Create(ctx, parent, cachedTip)
	require.NoError(t, err)

	// Spend every output while the parent is still UNMINED. The parent becomes fully
	// spent but gets NO delete_at_height (the spend path only stamps a DAH on
	// already-mined txs). The DAH is therefore stamped later, by setMined.
	for i, out := range parent.Outputs {
		child := bt.NewTx()
		require.NoError(t, child.From(parentHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
		_, err = store.Spend(ctx, child, cachedTip)
		require.NoError(t, err, "spending output %d while unmined should succeed", i)
	}

	// Mark the parent mined at a height far above the cached tip — block validation
	// always knows and passes this height in minedBlockInfo.BlockHeight.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    minedHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	var dah *int64
	err = store.db.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", parentHash[:]).Scan(&dah)
	require.NoError(t, err)
	require.NotNil(t, dah, "setMined must stamp a delete_at_height on a fully-spent, mined tx")
	require.Equal(t, int64(minedHeight+retention), *dah,
		"delete_at_height must be minedBlockInfo.BlockHeight + retention, not the lagging cachedTip + 1 + retention")
}
